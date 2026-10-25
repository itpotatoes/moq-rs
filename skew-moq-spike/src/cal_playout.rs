//! Registration §7-1 L2 equipment calibration workload: a fixed-delay
//! playout with no experimental method (no epoch search, no controller, no
//! delivery timeout, no tier switching).
//!
//! * Plan: the experiment-2 opportunity plan (contract §1), derived here —
//!   `N_PC` PC slots at `pts = floor(i·10⁶/30)` and `N_HAPTIC` haptic slots
//!   at `pts = floor(k·10⁶/90)`, with their event ids.
//! * Schedule: `due(m, j) = t0 + pts(m, j) + D_cal`, `t0` from the
//!   PhaseControl receipt, `D_cal` a registration slot passed by the runner.
//! * Late rule: an object whose complete reception `t_recv > due` is late —
//!   never submitted, recorded. No tolerance.
//! * One terminal per planned slot: released (held, then released at its
//!   due), late, or missing (no object by the seal).
//! * `H_cal = t0 + max planned pts + D_cal` (the last planned due); the seal
//!   runs at `H_cal + CAL_SEAL_EXECUTION_DELAY_US`, and the L2 stage closes at
//!   the absolute deadline `H_cal + W_cal` (W_cal = the calibration-only
//!   report window, not a slot).
//!
//! The release instant itself is the receiver's actual dispatch (never before
//! the due); this module only decides WHAT is due and WHEN.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};

use bytes::Bytes;

use crate::exp2_playout::Modality;
use crate::exp2_recorder::{haptic_slot_identity, pc_slot_identity, N_HAPTIC, N_PC};
use crate::{Header, TRACK_HAPTIC, TRACK_PC};

/// Seal execution delay after `H_cal`: the exp2 B1/S1 seal delay itself
/// (one definition). Not a scientific parameter: it only lets an object
/// stamped `t_recv <= due` that is still between its receive stamp and this
/// scheduler reach it before the slot is sealed `missing`.
pub const CAL_SEAL_EXECUTION_DELAY_US: u64 = crate::exp2_recorder::PLAIN_SEAL_EXECUTION_DELAY_US;
/// Release order of slots with the same due (PC `i` and haptic `3i` share
/// their pts, so their due): PC first, then by slot index — among the
/// objects the scheduler holds when the release runs. Every ingress message
/// already queued is admitted before a release (Codex 158a); an object still
/// between its drain stamp and the scheduler is outside this order (released
/// at once on arrival if it was on time). The actual order is the order of
/// the `cal_release` rows.
pub const CAL_RELEASE_TIE_RULE: &str = "(due_us, pc before haptic, slot index) among the objects admitted to the scheduler when the release runs (every queued ingress message is admitted first); an object still in the drain-to-scheduler hand-off is outside this order and is released at once on arrival if on time; the actual order is the cal_release row order";
pub const CAL_LATE_RULE: &str = "late iff t_recv_us > due_us (no tolerance); a late object is never submitted";
pub const CAL_PLAN_SOURCE: &str = "exp2 contract §1 plan (pc_slot_identity / haptic_slot_identity, N_PC = 1200, N_HAPTIC = 3600)";
pub const CAL_H_RULE: &str = "H_cal = t0 + max planned pts + D_cal (the last planned due)";

/// The calibration plan: per modality, the planned `(pts, event_id)`.
#[derive(Debug, Clone)]
pub struct CalPlan {
    pts: [Vec<u64>; 2],
    event_id: [Vec<u32>; 2],
    by_pts: [HashMap<u64, u32>; 2],
}

impl CalPlan {
    /// The registered 40 s plan (the experiment-2 opportunities).
    pub fn registered() -> Self {
        Self::from_slots(
            (0..N_PC as u64).map(pc_slot_identity).collect(),
            (0..N_HAPTIC as u64).map(haptic_slot_identity).collect(),
        )
    }

    fn from_slots(pc: Vec<(u64, u32)>, haptic: Vec<(u64, u32)>) -> Self {
        let split = |slots: Vec<(u64, u32)>| {
            let by_pts = slots.iter().enumerate().map(|(j, (p, _))| (*p, j as u32)).collect();
            let (pts, ids) = slots.into_iter().unzip();
            (pts, ids, by_pts)
        };
        let (pc_pts, pc_ids, pc_by) = split(pc);
        let (h_pts, h_ids, h_by) = split(haptic);
        Self { pts: [pc_pts, h_pts], event_id: [pc_ids, h_ids], by_pts: [pc_by, h_by] }
    }

    pub fn len(&self, m: Modality) -> usize {
        self.pts[m.ix()].len()
    }

    pub fn is_empty(&self) -> bool {
        self.pts.iter().all(Vec::is_empty)
    }

    pub fn pts(&self, m: Modality, j: u32) -> u64 {
        self.pts[m.ix()][j as usize]
    }

    pub fn event_id(&self, m: Modality, j: u32) -> u32 {
        self.event_id[m.ix()][j as usize]
    }

    pub fn max_pts(&self) -> u64 {
        self.pts.iter().filter_map(|v| v.iter().max()).max().copied().unwrap_or(0)
    }

    fn locate(&self, m: Modality, pts: u64) -> Option<u32> {
        self.by_pts[m.ix()].get(&pts).copied()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CalConfig {
    pub t0_us: u64,
    pub d_cal_us: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SlotState {
    Open,
    Held,
    Released,
    Late,
    Missing,
}

/// A held object that is due (returned by [`CalScheduler::take_due`]).
#[derive(Debug, Clone)]
pub struct CalObject {
    pub modality: Modality,
    pub index: u32,
    pub header: Header,
    pub t_recv_us: u64,
    pub due_us: u64,
    /// The complete object (header + payload).
    pub bytes: Bytes,
}

/// What an arrival was classified as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CalArrival {
    /// On time (`t_recv <= due`): held until its due.
    Held { modality: Modality, index: u32, due_us: u64 },
    /// `t_recv > due`: the slot's terminal, never submitted.
    Late { modality: Modality, index: u32, due_us: u64 },
    /// Arrived after the seal (its slot is already `missing`): diagnostic.
    AfterSeal { modality: Modality, index: u32, due_us: u64 },
    /// A second copy of a slot that already has an object or a terminal.
    Duplicate { modality: Modality, index: u32 },
    /// The planned slot of this pts has another event id: never submitted.
    IdentityMismatch { modality: Modality, index: u32 },
    /// Not a planned slot (unknown track or pts): never submitted.
    Orphan,
}

/// A planned slot without an object at the seal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CalMissing {
    pub modality: Modality,
    pub index: u32,
    pub due_us: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CalStats {
    pub arrivals: u64,
    pub released: u64,
    pub late: u64,
    pub missing: u64,
    pub after_seal: u64,
    pub duplicate: u64,
    pub identity_mismatch: u64,
    pub orphan: u64,
}

/// The fixed-delay calibration scheduler (pure: every instant is passed in).
#[derive(Debug)]
pub struct CalScheduler {
    cfg: CalConfig,
    plan: CalPlan,
    h_cal_us: u64,
    seal_at_us: u64,
    slots: [Vec<SlotState>; 2],
    /// `(due, modality ix, index)`: the tie rule is the tuple order.
    heap: BinaryHeap<Reverse<(u64, usize, u32)>>,
    held: HashMap<(usize, u32), CalObject>,
    sealed: bool,
    stats: CalStats,
}

fn modality_of(track_id: u8) -> Option<Modality> {
    match track_id {
        TRACK_PC => Some(Modality::Pc),
        TRACK_HAPTIC => Some(Modality::Haptic),
        _ => None,
    }
}

impl CalScheduler {
    pub fn new(cfg: CalConfig, plan: CalPlan) -> Result<Self, String> {
        if cfg.d_cal_us == 0 {
            return Err("D_cal must be positive".into());
        }
        if plan.is_empty() {
            return Err("empty calibration plan".into());
        }
        let h_cal_us = cfg
            .t0_us
            .checked_add(plan.max_pts())
            .and_then(|t| t.checked_add(cfg.d_cal_us))
            .ok_or("H_cal overflows u64")?;
        let seal_at_us = h_cal_us
            .checked_add(CAL_SEAL_EXECUTION_DELAY_US)
            .ok_or("the calibration seal instant overflows u64")?;
        let slots = Modality::ALL.map(|m| vec![SlotState::Open; plan.len(m)]);
        Ok(Self {
            cfg,
            plan,
            h_cal_us,
            seal_at_us,
            slots,
            heap: BinaryHeap::new(),
            held: HashMap::new(),
            sealed: false,
            stats: CalStats::default(),
        })
    }

    pub fn config(&self) -> CalConfig {
        self.cfg
    }

    pub fn plan(&self) -> &CalPlan {
        &self.plan
    }

    /// `t0 + pts + D_cal` (no overflow: bounded by `H_cal`).
    pub fn due_us(&self, m: Modality, index: u32) -> u64 {
        self.cfg.t0_us + self.plan.pts(m, index) + self.cfg.d_cal_us
    }

    pub fn h_cal_us(&self) -> u64 {
        self.h_cal_us
    }

    pub fn seal_at_us(&self) -> u64 {
        self.seal_at_us
    }

    pub fn is_sealed(&self) -> bool {
        self.sealed
    }

    pub fn stats(&self) -> &CalStats {
        &self.stats
    }

    /// Classify one complete arrival received at `t_recv_us`.
    pub fn arrive(&mut self, header: Header, t_recv_us: u64, bytes: Bytes) -> CalArrival {
        self.stats.arrivals += 1;
        let Some(m) = modality_of(header.track_id) else {
            self.stats.orphan += 1;
            return CalArrival::Orphan;
        };
        let Some(index) = self.plan.locate(m, header.pts_us) else {
            self.stats.orphan += 1;
            return CalArrival::Orphan;
        };
        let due_us = self.due_us(m, index);
        if self.plan.event_id(m, index) != header.event_id {
            self.stats.identity_mismatch += 1;
            return CalArrival::IdentityMismatch { modality: m, index };
        }
        let state = &mut self.slots[m.ix()][index as usize];
        match *state {
            SlotState::Open => {}
            SlotState::Missing => {
                self.stats.after_seal += 1;
                return CalArrival::AfterSeal { modality: m, index, due_us };
            }
            SlotState::Held | SlotState::Released | SlotState::Late => {
                self.stats.duplicate += 1;
                return CalArrival::Duplicate { modality: m, index };
            }
        }
        if t_recv_us > due_us {
            *state = SlotState::Late;
            self.stats.late += 1;
            return CalArrival::Late { modality: m, index, due_us };
        }
        *state = SlotState::Held;
        self.heap.push(Reverse((due_us, m.ix(), index)));
        self.held.insert(
            (m.ix(), index),
            CalObject { modality: m, index, header, t_recv_us, due_us, bytes },
        );
        CalArrival::Held { modality: m, index, due_us }
    }

    /// Earliest instant with something to do: the next held due, else the
    /// seal. `None` once sealed.
    pub fn next_wakeup_us(&self) -> Option<u64> {
        if self.sealed {
            return None;
        }
        let next_due = self.heap.peek().map(|Reverse((due, _, _))| *due);
        Some(next_due.map_or(self.seal_at_us, |d| d.min(self.seal_at_us)))
    }

    /// Every held object due at `now_us`, in the tie order, marked released.
    pub fn take_due(&mut self, now_us: u64) -> Vec<CalObject> {
        let mut out = Vec::new();
        while let Some(Reverse((due, mix, index))) = self.heap.peek().copied() {
            if due > now_us {
                break;
            }
            self.heap.pop();
            let object = self.held.remove(&(mix, index)).expect("a heap entry has its object");
            self.slots[mix][index as usize] = SlotState::Released;
            self.stats.released += 1;
            out.push(object);
        }
        out
    }

    /// The seal at `now_us >= seal_at`: release everything due first
    /// ([`Self::take_due`], returned), then every open slot is `missing`.
    pub fn seal(&mut self, now_us: u64) -> Result<(Vec<CalObject>, Vec<CalMissing>), String> {
        if self.sealed {
            return Err("sealed twice".into());
        }
        if now_us < self.seal_at_us {
            return Err(format!("seal at {now_us} before the seal instant {}", self.seal_at_us));
        }
        let released = self.take_due(now_us);
        if !self.heap.is_empty() || !self.held.is_empty() {
            return Err("a held object is due after H_cal".into());
        }
        let mut missing = Vec::new();
        for m in Modality::ALL {
            for index in 0..self.plan.len(m) as u32 {
                let state = &mut self.slots[m.ix()][index as usize];
                if *state == SlotState::Open {
                    *state = SlotState::Missing;
                    self.stats.missing += 1;
                    missing.push(CalMissing { modality: m, index, due_us: self.cfg.t0_us + self.plan.pts(m, index) + self.cfg.d_cal_us });
                }
            }
        }
        self.sealed = true;
        Ok((released, missing))
    }

    /// Every planned slot has exactly one terminal (after the seal).
    pub fn all_terminal(&self) -> bool {
        self.sealed
            && self
                .slots
                .iter()
                .flatten()
                .all(|s| matches!(s, SlotState::Released | SlotState::Late | SlotState::Missing))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const T0: u64 = 7_000_000_000;
    const D: u64 = 250_000;

    fn header(m: Modality, index: u32, plan: &CalPlan) -> Header {
        Header {
            version: crate::VERSION,
            track_id: if m == Modality::Pc { TRACK_PC } else { TRACK_HAPTIC },
            tier: if m == Modality::Pc { 2 } else { 0 },
            seq: index,
            pts_us: plan.pts(m, index),
            event_id: plan.event_id(m, index),
            gen_ts_us: T0 + plan.pts(m, index),
            payload_len: 4,
        }
    }

    fn sched() -> CalScheduler {
        CalScheduler::new(CalConfig { t0_us: T0, d_cal_us: D }, CalPlan::registered()).unwrap()
    }

    /// µs-exact fixed delay for every planned slot, H_cal = the last due.
    #[test]
    fn due_is_t0_plus_pts_plus_d_cal_for_every_slot() {
        let s = sched();
        let plan = CalPlan::registered();
        assert_eq!((plan.len(Modality::Pc), plan.len(Modality::Haptic)), (1_200, 3_600));
        let mut last = 0;
        for m in Modality::ALL {
            for j in 0..plan.len(m) as u32 {
                let due = s.due_us(m, j);
                assert_eq!(due, T0 + plan.pts(m, j) + D);
                last = last.max(due);
            }
        }
        assert_eq!(plan.pts(Modality::Pc, 1_199), 39_966_666);
        assert_eq!(plan.pts(Modality::Haptic, 3_599), 39_988_888);
        assert_eq!(s.h_cal_us(), T0 + 39_988_888 + D);
        assert_eq!(s.h_cal_us(), last);
        assert_eq!(s.seal_at_us(), s.h_cal_us() + CAL_SEAL_EXECUTION_DELAY_US);
        // PC i and haptic 3i share the due (tie rule)
        assert_eq!(s.due_us(Modality::Pc, 7), s.due_us(Modality::Haptic, 21));
        assert!(CalScheduler::new(CalConfig { t0_us: T0, d_cal_us: 0 }, CalPlan::registered()).is_err());
        assert!(CalScheduler::new(CalConfig { t0_us: u64::MAX - 10, d_cal_us: D }, CalPlan::registered()).is_err());
    }

    /// On time is held and released at its due, in the tie order; strictly
    /// after the due is late (no tolerance) and never released.
    #[test]
    fn on_time_held_late_never_released_tie_order() {
        let mut s = sched();
        let plan = CalPlan::registered();
        let due = s.due_us(Modality::Pc, 7);
        // haptic 21 arrives first, PC 7 exactly at its due, haptic 22 1 µs late
        assert!(matches!(s.arrive(header(Modality::Haptic, 21, &plan), due - 5, Bytes::new()), CalArrival::Held { .. }));
        assert!(matches!(s.arrive(header(Modality::Pc, 7, &plan), due, Bytes::new()), CalArrival::Held { .. }));
        let due22 = s.due_us(Modality::Haptic, 22);
        assert_eq!(
            s.arrive(header(Modality::Haptic, 22, &plan), due22 + 1, Bytes::new()),
            CalArrival::Late { modality: Modality::Haptic, index: 22, due_us: due22 }
        );
        assert_eq!(s.next_wakeup_us(), Some(due));
        assert!(s.take_due(due - 1).is_empty());
        let out = s.take_due(due);
        let order: Vec<_> = out.iter().map(|o| (o.modality, o.index, o.due_us)).collect();
        assert_eq!(order, vec![(Modality::Pc, 7, due), (Modality::Haptic, 21, due)]);
        // a duplicate never re-arms a slot
        assert_eq!(
            s.arrive(header(Modality::Pc, 7, &plan), due - 10, Bytes::new()),
            CalArrival::Duplicate { modality: Modality::Pc, index: 7 }
        );
        assert_eq!((s.stats().released, s.stats().late, s.stats().duplicate), (2, 1, 1));
    }

    /// A held object whose due passed before the scheduler ran is released
    /// at once (the receiver's actual dispatch is later than the due).
    #[test]
    fn a_held_object_found_after_its_due_is_released_at_once() {
        let mut s = sched();
        let plan = CalPlan::registered();
        let due = s.due_us(Modality::Pc, 3);
        assert!(matches!(s.arrive(header(Modality::Pc, 3, &plan), due - 1, Bytes::new()), CalArrival::Held { .. }));
        let out = s.take_due(due + 5_000);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].due_us, due);
    }

    /// Orphans and identity mismatches are never held.
    #[test]
    fn unplanned_and_mismatched_objects_are_never_held() {
        let mut s = sched();
        let plan = CalPlan::registered();
        let mut h = header(Modality::Pc, 4, &plan);
        h.pts_us += 1;
        assert_eq!(s.arrive(h, T0, Bytes::new()), CalArrival::Orphan);
        let mut h = header(Modality::Haptic, 4, &plan);
        h.track_id = 9;
        assert_eq!(s.arrive(h, T0, Bytes::new()), CalArrival::Orphan);
        let mut h = header(Modality::Haptic, 3, &plan);
        h.event_id = 77;
        assert_eq!(s.arrive(h, T0, Bytes::new()), CalArrival::IdentityMismatch { modality: Modality::Haptic, index: 3 });
        assert_eq!(s.next_wakeup_us(), Some(s.seal_at_us()));
        assert_eq!((s.stats().orphan, s.stats().identity_mismatch), (2, 1));
    }

    /// The seal releases what is due, marks the rest missing (one terminal
    /// per slot), refuses an early or second seal; a later arrival is a
    /// diagnostic only.
    #[test]
    fn seal_marks_missing_and_every_slot_has_one_terminal() {
        let mut s = sched();
        let plan = CalPlan::registered();
        for m in Modality::ALL {
            for j in 0..plan.len(m) as u32 {
                if (m, j) == (Modality::Haptic, 3_599) || (m, j) == (Modality::Pc, 10) {
                    continue; // never arrives
                }
                let due = s.due_us(m, j);
                let t = if (m, j) == (Modality::Pc, 11) { due + 1 } else { due - 1_000 };
                s.arrive(header(m, j, &plan), t, Bytes::new());
            }
        }
        assert!(s.seal(s.seal_at_us() - 1).is_err());
        let mut released = 0;
        let mut now = 0;
        while let Some(wake) = s.next_wakeup_us() {
            now = wake;
            if wake >= s.seal_at_us() {
                break;
            }
            released += s.take_due(wake).len();
        }
        let (rest, missing) = s.seal(now).unwrap();
        released += rest.len();
        assert_eq!(released, 4_800 - 3);
        let missing: Vec<_> = missing.iter().map(|x| (x.modality, x.index)).collect();
        assert_eq!(missing, vec![(Modality::Pc, 10), (Modality::Haptic, 3_599)]);
        assert!(s.all_terminal());
        assert_eq!((s.stats().released, s.stats().late, s.stats().missing), (4_797, 1, 2));
        assert!(s.seal(now).is_err(), "sealed twice");
        assert_eq!(s.next_wakeup_us(), None);
        let after = s.arrive(header(Modality::Pc, 10, &plan), now + 1, Bytes::new());
        assert!(matches!(after, CalArrival::AfterSeal { modality: Modality::Pc, index: 10, .. }));
        assert_eq!(s.stats().after_seal, 1);
    }
}
