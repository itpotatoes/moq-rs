//! Experiment-2 P1 FSM (WP1b): P1 = P0 core + this FSM.
//!
//! Governing text: registration `md/20260929_실험2_사전등록_문안.md` §2 (P1
//! row) and spec `e88233c` §6.  The FSM is a **pure decision component**: its
//! only input is the anchor pair-miss result of each planned opportunity, in
//! planned (due) order, together with that event's committed Δ_eff.  Skew is
//! not an input (the observation type has no skew field).  It emits tier-change
//! *requests* only; make-before-break, stale-generation disposal and the wire
//! adapter (`s3_switch`) are WP3.
//!
//! Transitions (states and tiers as in Experiment 1, reused from
//! `s3_controller` by import):
//! - Normal → HC and Recovery → HC: the window holds exactly `window` (60)
//!   completed anchor opportunities, all with Δ_eff = Δ_max, and the
//!   one-sided Clopper–Pearson lower bound of the pair-miss ratio exceeds
//!   `p0` (0.05) — equivalently misses >= k*, with k* derived at construction
//!   (7 for 60 / 0.05 / 95 %).
//! - HC → Recovery: window pair misses 0 and Δ_eff < Δ_max − h, sustained 3 s.
//! - Recovery → Normal: same condition sustained 5 s.  Cooldown 2 s between
//!   transitions.
//!
//! `[INTERPRETATION]` markers are listed in the WP1 report.

use std::collections::VecDeque;

use crate::exp2_playout::{
    Exp2Decision, Exp2Mode, Exp2OpportunityLedger, Exp2Params, Exp2Playout,
    Exp2SchedulerLedger, Modality, PairCompleteRecord,
};
use crate::s3_controller::{HapticMode, S3State};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Exp2FsmParams {
    /// Evaluation unit: exactly this many completed anchor opportunities.
    pub window: usize,
    /// Null miss ratio p0 of the Clopper–Pearson test.
    pub p0: f64,
    /// One-sided confidence (0.95).
    pub confidence: f64,
    pub delta_max_us: u64,
    pub hysteresis_us: u64,
    pub hc_to_recovery_us: u64,
    pub recovery_to_normal_us: u64,
    pub cooldown_us: u64,
}

impl Exp2FsmParams {
    /// Registered values (spec §6) with the Δ_max slot as input.
    pub fn registered(delta_max_us: u64) -> Self {
        Self {
            window: 60,
            p0: 0.05,
            confidence: 0.95,
            delta_max_us,
            hysteresis_us: 30_000,
            hc_to_recovery_us: 3_000_000,
            recovery_to_normal_us: 5_000_000,
            cooldown_us: 2_000_000,
        }
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        if self.window == 0 {
            return Err("window must be > 0");
        }
        if !(self.p0 > 0.0 && self.p0 < 1.0) {
            return Err("p0 must be in (0, 1)");
        }
        if !(self.confidence > 0.0 && self.confidence < 1.0) {
            return Err("confidence must be in (0, 1)");
        }
        if self.hysteresis_us >= self.delta_max_us {
            return Err("hysteresis must be < delta_max");
        }
        Ok(())
    }
}

/// Smallest miss count k whose one-sided Clopper–Pearson lower bound exceeds
/// `p0`, i.e. `P(X >= k | n, p0) < 1 − confidence`.  f64 is used once, at
/// construction; the run-time test is the integer `misses >= k`.  Returns
/// `None` if no k <= n qualifies.
pub fn clopper_pearson_miss_threshold(n: usize, p0: f64, confidence: f64) -> Option<usize> {
    let alpha = 1.0 - confidence;
    // pmf by recurrence: pmf(0) = (1−p)^n, pmf(k+1) = pmf(k)·(n−k)/(k+1)·p/(1−p).
    let mut pmf = vec![0.0f64; n + 1];
    pmf[0] = (1.0 - p0).powi(n as i32);
    for k in 0..n {
        pmf[k + 1] = pmf[k] * ((n - k) as f64) / ((k + 1) as f64) * p0 / (1.0 - p0);
    }
    (1..=n).find(|&k| pmf[k..].iter().sum::<f64>() < alpha)
}

/// One pair result.  Deliberately has no skew field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FsmObservation {
    pub event: u32,
    pub at_us: u64,
    pub pair_miss: bool,
    /// Committed shared Δ_eff of the event (P0: equal for both modalities).
    pub delta_eff_us: Option<u64>,
}

impl FsmObservation {
    /// P0 shares one Δ_eff, so both modalities' committed values must exist
    /// and be equal.  A mismatch or a missing value is an integrity error
    /// (never silently reconciled).
    pub fn from_pair(rec: &PairCompleteRecord) -> Result<Self, &'static str> {
        let d = match rec.delta_eff_us {
            [Some(a), Some(b)] if a == b => a,
            [Some(_), Some(_)] => return Err("P0 shared-Δ invariant violated: Δ_eff differs by modality"),
            _ => return Err("P0 pair result without a committed Δ_eff"),
        };
        Ok(Self {
            event: rec.event,
            at_us: rec.at_us,
            pair_miss: rec.pair_miss(),
            delta_eff_us: Some(d),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exp2FsmCause {
    SaturatedPairMiss,
    StableRecovery,
    StableNormal,
}

impl Exp2FsmCause {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SaturatedPairMiss => "saturated_pair_miss",
            Self::StableRecovery => "stable_recovery",
            Self::StableNormal => "stable_normal",
        }
    }
}

/// A tier-change request (WP3 applies it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TierRequest {
    pub at_us: u64,
    /// Event whose result triggered the request.
    pub event: u32,
    pub from: S3State,
    pub to: S3State,
    pub cause: Exp2FsmCause,
    pub pc_tier: u16,
    pub haptic_mode: HapticMode,
    pub window_misses: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Exp2FsmSnapshot {
    pub state: S3State,
    pub window_len: usize,
    pub window_misses: usize,
    pub window_saturated: bool,
    pub stable_since_us: Option<u64>,
}

pub struct Exp2Fsm {
    params: Exp2FsmParams,
    miss_threshold: usize,
    state: S3State,
    window: VecDeque<(bool, Option<u64>)>,
    next_event: u32,
    stable_since_us: Option<u64>,
    last_transition_us: Option<u64>,
    last_at_us: Option<u64>,
}

impl Exp2Fsm {
    pub fn new(params: Exp2FsmParams) -> Result<Self, &'static str> {
        params.validate()?;
        let miss_threshold =
            clopper_pearson_miss_threshold(params.window, params.p0, params.confidence)
                .ok_or("no miss count satisfies the Clopper–Pearson rule")?;
        Ok(Self {
            params,
            miss_threshold,
            state: S3State::Normal,
            window: VecDeque::new(),
            next_event: 0,
            stable_since_us: None,
            last_transition_us: None,
            last_at_us: None,
        })
    }

    pub fn miss_threshold(&self) -> usize {
        self.miss_threshold
    }

    pub fn state(&self) -> S3State {
        self.state
    }

    pub fn snapshot(&self) -> Exp2FsmSnapshot {
        Exp2FsmSnapshot {
            state: self.state,
            window_len: self.window.len(),
            window_misses: self.misses(),
            window_saturated: self.saturated(),
            stable_since_us: self.stable_since_us,
        }
    }

    fn misses(&self) -> usize {
        self.window.iter().filter(|(m, _)| *m).count()
    }

    fn full(&self) -> bool {
        self.window.len() == self.params.window
    }

    fn saturated(&self) -> bool {
        self.full()
            && self
                .window
                .iter()
                .all(|(_, d)| *d == Some(self.params.delta_max_us))
    }

    fn cooldown_complete(&self, now: u64) -> bool {
        self.last_transition_us
            .is_none_or(|t| now - t >= self.params.cooldown_us)
    }

    /// Consume one pair result.  Results must arrive in planned event order
    /// (0, 1, 2, …) — enforced.
    /// [INTERPRETATION] evaluated on every consumed result; the window is a
    /// sliding window of the last `window` results and is not reset by a
    /// transition (the cooldown is the only re-fire guard).
    pub fn observe(&mut self, obs: FsmObservation) -> Result<Option<TierRequest>, &'static str> {
        if obs.event != self.next_event {
            return Err("pair results must be consumed in planned order");
        }
        if self.last_at_us.is_some_and(|t| obs.at_us < t) {
            return Err("time moved backwards");
        }
        self.next_event += 1;
        self.last_at_us = Some(obs.at_us);
        self.window.push_back((obs.pair_miss, obs.delta_eff_us));
        while self.window.len() > self.params.window {
            self.window.pop_front();
        }
        let now = obs.at_us;
        let degrade = self.saturated() && self.misses() >= self.miss_threshold;
        // [INTERPRETATION] "Δ_eff < Δ_max − h" reads the Δ_eff of the event
        // just consumed; "window pair miss 0" requires a full window.
        let stable = self.full()
            && self.misses() == 0
            && obs
                .delta_eff_us
                .is_some_and(|d| d + self.params.hysteresis_us < self.params.delta_max_us);
        let next = match self.state {
            S3State::Normal => {
                self.stable_since_us = None;
                (degrade && self.cooldown_complete(now))
                    .then_some((S3State::HapticCritical, Exp2FsmCause::SaturatedPairMiss))
            }
            S3State::HapticCritical => self.stable_transition(
                stable,
                now,
                self.params.hc_to_recovery_us,
                S3State::Recovery,
                Exp2FsmCause::StableRecovery,
            ),
            S3State::Recovery => {
                if degrade {
                    self.stable_since_us = None;
                    self.cooldown_complete(now)
                        .then_some((S3State::HapticCritical, Exp2FsmCause::SaturatedPairMiss))
                } else {
                    self.stable_transition(
                        stable,
                        now,
                        self.params.recovery_to_normal_us,
                        S3State::Normal,
                        Exp2FsmCause::StableNormal,
                    )
                }
            }
        };
        Ok(next.map(|(to, cause)| {
            let from = self.state;
            self.state = to;
            self.last_transition_us = Some(now);
            // [INTERPRETATION] the Recovery → Normal 5 s is timed from the
            // HC → Recovery transition instant (the condition holds there);
            // every other transition restarts the stable timer from scratch.
            self.stable_since_us = (cause == Exp2FsmCause::StableRecovery).then_some(now);
            TierRequest {
                at_us: now,
                event: obs.event,
                from,
                to,
                cause,
                pc_tier: to.pc_tier(),
                haptic_mode: to.haptic_mode(),
                window_misses: self.misses(),
            }
        }))
    }

    fn stable_transition(
        &mut self,
        stable: bool,
        now: u64,
        need_us: u64,
        to: S3State,
        cause: Exp2FsmCause,
    ) -> Option<(S3State, Exp2FsmCause)> {
        if !stable {
            self.stable_since_us = None;
            return None;
        }
        let since = *self.stable_since_us.get_or_insert(now);
        (now - since >= need_us && self.cooldown_complete(now)).then_some((to, cause))
    }
}

/// P1 = P0 core + FSM.  The FSM sees only the core's in-order pair results.
pub struct Exp2P1 {
    core: Exp2Playout,
    fsm: Exp2Fsm,
}

impl Exp2P1 {
    pub fn new(
        params: Exp2Params,
        fsm_params: Exp2FsmParams,
        ledger: Exp2OpportunityLedger,
    ) -> Result<Self, &'static str> {
        if fsm_params.delta_max_us != params.delta_max_us {
            return Err("FSM and core must share delta_max");
        }
        Ok(Self {
            core: Exp2Playout::new(Exp2Mode::P0, params, ledger)?,
            fsm: Exp2Fsm::new(fsm_params)?,
        })
    }

    pub fn core(&self) -> &Exp2Playout {
        &self.core
    }

    pub fn fsm(&self) -> &Exp2Fsm {
        &self.fsm
    }

    fn feed(
        &mut self,
        decisions: Vec<Exp2Decision>,
    ) -> Result<(Vec<Exp2Decision>, Vec<TierRequest>), &'static str> {
        let mut requests = Vec::new();
        for d in &decisions {
            if let Exp2Decision::PairComplete(p) = d {
                if let Some(r) = self.fsm.observe(FsmObservation::from_pair(p)?)? {
                    requests.push(r);
                }
            }
        }
        Ok((decisions, requests))
    }

    pub fn arrive(
        &mut self,
        m: Modality,
        index: u32,
        t_recv_us: u64,
    ) -> Result<(Vec<Exp2Decision>, Vec<TierRequest>), &'static str> {
        let d = self.core.arrive(m, index, t_recv_us)?;
        self.feed(d)
    }

    pub fn advance_to(
        &mut self,
        t_us: u64,
    ) -> Result<(Vec<Exp2Decision>, Vec<TierRequest>), &'static str> {
        let d = self.core.advance_to(t_us)?;
        self.feed(d)
    }

    /// WP3 API addition (no behaviour change): pass a wire delivery-timeout
    /// observation to the P0 core exactly as `Exp2Playout::delivery_timeout`
    /// does, so P1 has the same input surface as the other core modes.
    pub fn delivery_timeout(
        &mut self,
        m: Modality,
        index: u32,
        t_us: u64,
    ) -> Result<(Vec<Exp2Decision>, Vec<TierRequest>), &'static str> {
        let d = self.core.delivery_timeout(m, index, t_us)?;
        self.feed(d)
    }

    pub fn finalize(
        &mut self,
        horizon_us: u64,
    ) -> Result<(Vec<Exp2Decision>, Vec<TierRequest>, Exp2SchedulerLedger), &'static str> {
        let (d, ledger) = self.core.finalize(horizon_us)?;
        let (d, r) = self.feed(d)?;
        Ok((d, r, ledger))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exp2_playout::tests::{
        arrivals, commits, deltas, horizon, ledger, loss_trace, params, step_times, Arrival, Lcg,
        DMAX, T0,
    };
    use crate::exp2_playout::DeltaCause;

    fn fsm() -> Exp2Fsm {
        Exp2Fsm::new(Exp2FsmParams::registered(DMAX)).unwrap()
    }

    /// Feed `n` observations 33 ms apart starting at event `start`.
    fn feed(
        f: &mut Exp2Fsm,
        start: u32,
        t_start: u64,
        n: u32,
        miss: impl Fn(u32) -> bool,
        delta: impl Fn(u32) -> u64,
    ) -> Vec<TierRequest> {
        let mut out = Vec::new();
        for k in 0..n {
            let e = start + k;
            if let Some(r) = f
                .observe(FsmObservation {
                    event: e,
                    at_us: t_start + k as u64 * 33_333,
                    pair_miss: miss(e),
                    delta_eff_us: Some(delta(e)),
                })
                .unwrap()
            {
                out.push(r);
            }
        }
        out
    }

    #[test]
    fn exp2_fsm_cp_threshold_is_7_of_60_for_registered_inputs() {
        assert_eq!(clopper_pearson_miss_threshold(60, 0.05, 0.95), Some(7));
        assert_eq!(fsm().miss_threshold(), 7);
    }

    #[test]
    fn exp2_fsm_7_of_60_fires_6_does_not() {
        let mut f = fsm();
        let r = feed(&mut f, 0, T0, 60, |e| e < 6, |_| DMAX);
        assert!(r.is_empty(), "6/60 must not fire");
        let mut f = fsm();
        let r = feed(&mut f, 0, T0, 60, |e| e < 7, |_| DMAX);
        assert_eq!(r.len(), 1, "7/60 fires on the 60th result");
        assert_eq!((r[0].from, r[0].to, r[0].event), (S3State::Normal, S3State::HapticCritical, 59));
        assert_eq!(r[0].pc_tier, S3State::HapticCritical.pc_tier());
        assert_eq!(r[0].haptic_mode, HapticMode::Essential);
        // fewer than 60 results never fire, whatever the misses
        let mut f = fsm();
        assert!(feed(&mut f, 0, T0, 59, |_| true, |_| DMAX).is_empty());
    }

    #[test]
    fn exp2_fsm_unsaturated_window_never_transitions() {
        // 60/60 misses but one event below Δ_max anywhere in the window
        for low in [0u32, 30, 59] {
            let mut f = fsm();
            let r = feed(&mut f, 0, T0, 60, |_| true, |e| if e == low { DMAX - 1 } else { DMAX });
            assert!(r.is_empty(), "unsaturated at {low}");
        }
        // it fires once the unsaturated event has slid out of the window
        let mut f = fsm();
        let r = feed(&mut f, 0, T0, 61, |_| true, |e| if e == 0 { DMAX - 1 } else { DMAX });
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].event, 60);
    }

    #[test]
    fn exp2_fsm_hc_recovery_normal_timing_and_cooldown() {
        let mut f = fsm();
        let t = |k: u32| T0 + k as u64 * 33_333;
        assert_eq!(feed(&mut f, 0, T0, 60, |e| e < 7, |_| DMAX).len(), 1);
        let t_hc = t(59);
        // clean, low-Δ results: HC → Recovery after the window is clean
        // (60 results) and the condition held 3 s.
        let low = DMAX - 30_001; // < Δ_max − h
        let r = feed(&mut f, 60, t(60), 400, |_| false, |_| low);
        assert!(r.len() >= 2, "{r:?}");
        let (rec, norm) = (r[0], r[1]);
        assert_eq!((rec.from, rec.to), (S3State::HapticCritical, S3State::Recovery));
        // misses were events 0..6, so the 60-window is first clean at event 66
        let stable_start = t(66);
        assert!(rec.at_us - stable_start >= 3_000_000 && rec.at_us - stable_start < 3_000_000 + 33_334);
        assert!(rec.at_us - t_hc >= 2_000_000);
        assert_eq!((norm.from, norm.to), (S3State::Recovery, S3State::Normal));
        assert!(norm.at_us - rec.at_us >= 5_000_000 && norm.at_us - rec.at_us < 5_000_000 + 33_334);
        // Δ_eff exactly Δ_max − h is not "< Δ_max − h": no recovery
        let mut f = fsm();
        feed(&mut f, 0, T0, 60, |e| e < 7, |_| DMAX);
        let r = feed(&mut f, 60, t(60), 400, |_| false, |_| DMAX - 30_000);
        assert!(r.is_empty());
        // cooldown: 7/60 persists → Normal→HC fires, and no HC exit/entry
        // happens inside 2 s of it
        let mut f = fsm();
        let r = feed(&mut f, 0, T0, 300, |e| e % 8 == 0, |_| DMAX);
        assert_eq!(r.len(), 1, "HC stays: no stable condition; no repeat request");
    }

    #[test]
    fn exp2_fsm_recovery_to_hc_via_7_of_60() {
        let mut f = fsm();
        let t = |k: u32| T0 + k as u64 * 33_333;
        feed(&mut f, 0, T0, 60, |e| e < 7, |_| DMAX);
        let r = feed(&mut f, 60, t(60), 200, |_| false, |_| DMAX - 50_000);
        assert_eq!(r[0].to, S3State::Recovery);
        let t_rec = r[0].at_us;
        // still in Recovery (Normal needs 5 s more); saturate with 7/60 misses
        assert_eq!(f.state(), S3State::Recovery);
        let start = 260u32;
        let r = feed(&mut f, start, t(start).max(t_rec + 1), 60, |e| e < start + 7, |_| DMAX);
        assert_eq!(r.len(), 1, "Recovery → HC fires: {r:?}");
        assert_eq!((r[0].from, r[0].to), (S3State::Recovery, S3State::HapticCritical));
        assert_eq!(r[0].window_misses, 7);
        // and 6/60 in Recovery does not
        let mut f = fsm();
        feed(&mut f, 0, T0, 60, |e| e < 7, |_| DMAX);
        feed(&mut f, 60, t(60), 200, |_| false, |_| DMAX - 50_000);
        assert_eq!(f.state(), S3State::Recovery);
        let r = feed(&mut f, 260, t(260), 60, |e| e < 266, |_| DMAX);
        assert!(r.is_empty());
        assert_eq!(f.state(), S3State::Recovery);
    }

    #[test]
    fn exp2_fsm_from_pair_enforces_shared_delta() {
        let rec = |a: Option<u64>, b: Option<u64>| PairCompleteRecord {
            event: 0,
            event_id: 1,
            at_us: T0,
            completed_at_us: T0,
            pc_miss: true,
            haptic_miss: false,
            delta_eff_us: [a, b],
        };
        let ok = FsmObservation::from_pair(&rec(Some(DMAX), Some(DMAX))).unwrap();
        assert_eq!((ok.delta_eff_us, ok.pair_miss), (Some(DMAX), true));
        assert!(FsmObservation::from_pair(&rec(Some(DMAX), Some(DMAX - 1))).is_err());
        assert!(FsmObservation::from_pair(&rec(Some(DMAX), None)).is_err());
        assert!(FsmObservation::from_pair(&rec(None, None)).is_err());
    }

    #[test]
    fn exp2_fsm_rejects_out_of_order_results() {
        let mut f = fsm();
        f.observe(FsmObservation { event: 0, at_us: T0, pair_miss: false, delta_eff_us: Some(DMAX) })
            .unwrap();
        assert!(f
            .observe(FsmObservation { event: 2, at_us: T0 + 1, pair_miss: false, delta_eff_us: Some(DMAX) })
            .is_err());
    }

    fn run_p1(l: &Exp2OpportunityLedger, trace: &[Arrival]) -> (Vec<Exp2Decision>, Vec<TierRequest>, Exp2P1) {
        let h = horizon(l);
        let mut p1 = Exp2P1::new(params(), Exp2FsmParams::registered(DMAX), l.clone()).unwrap();
        let (mut ds, mut rs) = (Vec::new(), Vec::new());
        for &(t, m, i) in trace {
            if t > h {
                break;
            }
            let (d, r) = p1.arrive(m, i, t).unwrap();
            ds.extend(d);
            rs.extend(r);
        }
        let (d, r, _) = p1.finalize(h).unwrap();
        ds.extend(d);
        rs.extend(r);
        (ds, rs, p1)
    }

    /// The estimator's miss-window reset does not touch the FSM window.
    #[test]
    fn exp2_fsm_window_unaffected_by_estimator_reset() {
        let l = ledger(1200);
        let trace = loss_trace(&l, |i| (300..700).contains(&i));
        let h = horizon(&l);
        let mut p1 = Exp2P1::new(params(), Exp2FsmParams::registered(DMAX), l.clone()).unwrap();
        let mut checked = 0;
        for &(t, m, i) in &trace {
            if t > h {
                break;
            }
            let before = p1.fsm().snapshot().window_len;
            let (d, _) = p1.arrive(m, i, t).unwrap();
            let pairs = d.iter().filter(|x| matches!(x, Exp2Decision::PairComplete(_))).count();
            let stepped = d.iter().any(|x| matches!(x, Exp2Decision::DeltaUpdate(u) if u.cause == DeltaCause::MissStep));
            if stepped {
                // the estimator window was just consumed (len 0 at the step) ...
                let est = p1.core().estimator_snapshot(0, t).unwrap();
                assert!(est.miss_window_len < 60);
                // ... while the FSM window only grew by this input's results.
                assert_eq!(p1.fsm().snapshot().window_len, (before + pairs).min(60), "FSM window kept across a miss step");
                assert_eq!(p1.fsm().snapshot().window_len, 60);
                checked += 1;
            }
        }
        assert!(checked > 0, "at least one miss step observed");
    }

    #[test]
    fn exp2_p1_without_fsm_requests_equals_p0() {
        // FSM only emits requests; the core decisions of P1 are P0's.
        let l = ledger(1200);
        let trace = loss_trace(&l, |i| (300..700).contains(&i));
        let (d1, _, _) = run_p1(&l, &trace);
        let p0 = crate::exp2_playout::tests::run(Exp2Mode::P0, params(), &l, &trace);
        assert_eq!(d1, p0.decisions);
    }

    /// Skew is not an FSM input (the observation type has no skew field).
    /// Corroboration: same PC loss pattern with Δ pinned at Δ_max, haptic
    /// anchor delay 20 ms vs 300 ms (skew differs by ~280 ms) ⇒ identical
    /// tier requests.
    #[test]
    fn exp2_fsm_skew_is_not_an_input() {
        let l = ledger(1200);
        let mk = |hap: u64| {
            let mut rng = Lcg(77);
            arrivals(&l, move |m, idx, _| {
                let j = rng.below(5_000);
                match m {
                    Modality::Pc if idx % 8 == 3 => None,
                    Modality::Pc => Some(330_000 + j),
                    Modality::Haptic => Some(hap + j),
                }
            })
        };
        let (da, ra, _) = run_p1(&l, &mk(20_000));
        let (db, rb, _) = run_p1(&l, &mk(300_000));
        assert!(!ra.is_empty(), "non-vacuous: the FSM transitions");
        assert_eq!(ra, rb);
        let rel = |d: &[Exp2Decision], m: Modality| -> Vec<u64> {
            d.iter()
                .filter_map(|x| match x {
                    Exp2Decision::Release(r) if r.modality == m && r.index % 3 == 0 => Some(r.t_release_us),
                    _ => None,
                })
                .collect()
        };
        assert_eq!(rel(&da, Modality::Haptic), rel(&db, Modality::Haptic), "released at due either way");
    }

    /// Synthetic D1-like trace (not a measured trace): PC delay grows ~300 ms/s
    /// during the [10, 20) s overload and drains at 200 ms/s after; PC objects
    /// whose delay exceeds T_pc (= Δ_max) are never delivered (delivery-timeout
    /// proxy); haptic stays ~20 ms (priority 0).  Records whether Δ reaches
    /// Δ_max and whether the FSM window reaches 7/60 saturated.  Asserts only
    /// determinism.  No parameter is tuned on this.
    #[test]
    fn exp2_stability_record_d1_like_trace() {
        let l = ledger(1200);
        let mk = || {
            let mut rng = Lcg(2026);
            arrivals(&l, |m, idx, r| {
                let rel = r - T0;
                let j = rng.below(10_000);
                match m {
                    Modality::Haptic => Some(15_000 + j),
                    Modality::Pc => {
                        let _ = idx;
                        let q = if rel < 10_000_000 {
                            0
                        } else if rel < 20_000_000 {
                            (rel - 10_000_000) * 3 / 10
                        } else {
                            (3_000_000u64).saturating_sub((rel - 20_000_000) * 2 / 10)
                        };
                        let d = 30_000 + j + q;
                        (d <= DMAX).then_some(d)
                    }
                }
            })
        };
        let trace = mk();
        let (d, reqs, p1) = run_p1(&l, &trace);
        let (d2, reqs2, _) = run_p1(&l, &mk());
        assert_eq!(d, d2);
        assert_eq!(reqs, reqs2);
        let first_max = deltas(&d).iter().find(|u| u.to_us == DMAX).map(|u| u.at_us - T0);
        let steps = step_times(&d, 0).len();
        let first_eff_max = commits(&d).iter().filter(|c| c.at_us >= T0 + 10_000_000).find(|c| c.pc.delta_eff_us == DMAX).map(|c| c.at_us - T0);
        // replay the FSM window to find the first saturated 7/60 point
        let mut f = fsm();
        let mut first_sat_7 = None;
        let mut max_sat_misses = 0;
        for x in &d {
            if let Exp2Decision::PairComplete(p) = x {
                f.observe(FsmObservation::from_pair(p).unwrap()).unwrap();
                let s = f.snapshot();
                if s.window_saturated {
                    max_sat_misses = max_sat_misses.max(s.window_misses);
                    if s.window_misses >= 7 && first_sat_7.is_none() {
                        first_sat_7 = Some(p.at_us - T0);
                    }
                }
            }
        }
        let _ = p1;
        let reqs_fmt: Vec<(u64, &str, &str)> = reqs.iter().map(|r| (r.at_us - T0, r.from.as_str(), r.to.as_str())).collect();
        eprintln!(
            "[record D1-like] first Δ*=Δ_max at t0+{first_max:?} µs; first post-10s Δ_eff=Δ_max commit at t0+{first_eff_max:?} µs; miss steps={steps}; \
             first saturated window with >=7/60 at t0+{first_sat_7:?} µs; max misses in a saturated window={max_sat_misses}; tier requests={reqs_fmt:?}"
        );
    }
}
