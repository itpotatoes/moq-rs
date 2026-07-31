//! Transport-agnostic Phase-4 S3 subscription switch gate.
//!
//! The controller decides a desired state. This gate separates that decision
//! from wire effect: changed target tracks must receive SUBSCRIBE_OK and the
//! target PC/haptic routes must produce an exact pair before the new state is
//! applied. Until then, target objects are barrier-only and the old routes
//! remain release-eligible. Applying a switch makes replaced route generations
//! stale before their subscription handles are cancelled.

use crate::s3_controller::{HapticMode, S3State, S3Transition, TransitionCause};

pub const PC_NORMAL_TRACK: &str = "pc";
pub const PC_RECOVERY_TRACK: &str = "pc-d7";
pub const PC_HAPTIC_CRITICAL_TRACK: &str = "pc-d6";
pub const HAPTIC_FULL_TRACK: &str = "haptic";
pub const HAPTIC_ESSENTIAL_TRACK: &str = "haptic-essential";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TrackRole {
    Pc,
    Haptic,
}

impl TrackRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pc => "pc",
            Self::Haptic => "haptic",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Route {
    pub name: &'static str,
    pub generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Routes {
    pub pc: Route,
    pub haptic: Route,
}

impl Routes {
    pub fn for_role(self, role: TrackRole) -> Route {
        match role {
            TrackRole::Pc => self.pc,
            TrackRole::Haptic => self.haptic,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectDisposition {
    /// Object belongs to the currently applied route and may enter scheduling.
    CurrentReleaseEligible,
    /// Object belongs to a not-yet-applied target route. It may only establish
    /// the exact-pair first-effect barrier.
    PendingBarrierOnly,
    /// Object belongs to a replaced or unknown generation and must be terminal
    /// dropped without entering release accounting.
    StaleDrop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwitchConfig {
    /// Bound from request to exact-pair first effect. No production default is
    /// provided because this value must be preregistered for G5c.
    pub effect_timeout_us: u64,
}

impl SwitchConfig {
    pub fn validate(self) -> Result<(), SwitchError> {
        if self.effect_timeout_us == 0 {
            return Err(SwitchError::InvalidConfig);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwitchRequest {
    pub decision_at_us: u64,
    pub request_at_us: u64,
    pub from: S3State,
    pub to: S3State,
    pub cause: TransitionCause,
    pub target: Routes,
    pub pc_changed: bool,
    pub haptic_changed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwitchApplied {
    pub decision_at_us: u64,
    pub request_at_us: u64,
    pub effect_at_us: u64,
    pub request_to_effect_us: u64,
    pub from: S3State,
    pub to: S3State,
    pub cause: TransitionCause,
    pub exact_pts_us: u64,
    pub exact_event_id: u32,
    pub active: Routes,
    /// Replaced routes become stale before these handles are cancelled.
    pub cancel_pc: Option<Route>,
    pub cancel_haptic: Option<Route>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwitchError {
    InvalidConfig,
    NonMonotonicTime,
    TransitionFromUnappliedState,
    IllegalTransition,
    DecisionAfterRequest,
    SwitchAlreadyPending,
    NoSwitchPending,
    SubscribeOkForUnchangedRole,
    DuplicateSubscribeOk,
    FirstEffectBeforeSubscribeOk,
    SwitchTimedOut,
    GateFailed,
}

#[derive(Debug, Clone, Copy)]
struct PendingSwitch {
    request: SwitchRequest,
    pc_ok: bool,
    haptic_ok: bool,
}

pub struct S3SwitchGate {
    config: SwitchConfig,
    applied_state: S3State,
    active: Routes,
    next_pc_generation: u64,
    next_haptic_generation: u64,
    pending: Option<PendingSwitch>,
    last_now_us: Option<u64>,
    failed: bool,
}

impl S3SwitchGate {
    pub fn new(config: SwitchConfig) -> Result<Self, SwitchError> {
        config.validate()?;
        Ok(Self {
            config,
            applied_state: S3State::Normal,
            active: Routes {
                pc: Route {
                    name: PC_NORMAL_TRACK,
                    generation: 0,
                },
                haptic: Route {
                    name: HAPTIC_FULL_TRACK,
                    generation: 0,
                },
            },
            next_pc_generation: 1,
            next_haptic_generation: 1,
            pending: None,
            last_now_us: None,
            failed: false,
        })
    }

    pub fn applied_state(&self) -> S3State {
        self.applied_state
    }

    pub fn config(&self) -> SwitchConfig {
        self.config
    }

    pub fn active_routes(&self) -> Routes {
        self.active
    }

    pub fn pending_request(&self) -> Option<SwitchRequest> {
        self.pending.map(|pending| pending.request)
    }

    pub fn is_failed(&self) -> bool {
        self.failed
    }

    pub fn request(
        &mut self,
        transition: S3Transition,
        now_us: u64,
    ) -> Result<SwitchRequest, SwitchError> {
        self.check_ready(now_us)?;
        if self.pending.is_some() {
            return Err(SwitchError::SwitchAlreadyPending);
        }
        if transition.from != self.applied_state {
            return Err(SwitchError::TransitionFromUnappliedState);
        }
        if !is_legal_transition(transition.from, transition.to) {
            return Err(SwitchError::IllegalTransition);
        }
        if transition.at_us > now_us {
            return Err(SwitchError::DecisionAfterRequest);
        }

        let pc_name = pc_track(transition.to);
        let haptic_name = haptic_track(transition.to);
        let pc_changed = pc_name != self.active.pc.name;
        let haptic_changed = haptic_name != self.active.haptic.name;

        let pc = if pc_changed {
            let route = Route {
                name: pc_name,
                generation: self.next_pc_generation,
            };
            self.next_pc_generation += 1;
            route
        } else {
            self.active.pc
        };
        let haptic = if haptic_changed {
            let route = Route {
                name: haptic_name,
                generation: self.next_haptic_generation,
            };
            self.next_haptic_generation += 1;
            route
        } else {
            self.active.haptic
        };

        let request = SwitchRequest {
            decision_at_us: transition.at_us,
            request_at_us: now_us,
            from: transition.from,
            to: transition.to,
            cause: transition.cause,
            target: Routes { pc, haptic },
            pc_changed,
            haptic_changed,
        };
        self.pending = Some(PendingSwitch {
            request,
            pc_ok: !pc_changed,
            haptic_ok: !haptic_changed,
        });
        self.last_now_us = Some(now_us);
        Ok(request)
    }

    pub fn subscribe_ok(&mut self, role: TrackRole, now_us: u64) -> Result<(), SwitchError> {
        self.check_ready(now_us)?;
        self.fail_if_timed_out(now_us)?;
        let pending = self.pending.as_mut().ok_or(SwitchError::NoSwitchPending)?;
        let (changed, ok) = match role {
            TrackRole::Pc => (pending.request.pc_changed, &mut pending.pc_ok),
            TrackRole::Haptic => (pending.request.haptic_changed, &mut pending.haptic_ok),
        };
        if !changed {
            return Err(SwitchError::SubscribeOkForUnchangedRole);
        }
        if *ok {
            return Err(SwitchError::DuplicateSubscribeOk);
        }
        *ok = true;
        self.last_now_us = Some(now_us);
        Ok(())
    }

    /// Apply only when the adapter has observed one exact `(pts_us,event_id)`
    /// pair on the target PC/haptic routes. For an unchanged role, the current
    /// route supplies its side of the pair.
    pub fn exact_pair_first_effect(
        &mut self,
        pts_us: u64,
        event_id: u32,
        now_us: u64,
    ) -> Result<SwitchApplied, SwitchError> {
        self.check_ready(now_us)?;
        self.fail_if_timed_out(now_us)?;
        let pending = self.pending.ok_or(SwitchError::NoSwitchPending)?;
        if !pending.pc_ok || !pending.haptic_ok {
            return Err(SwitchError::FirstEffectBeforeSubscribeOk);
        }

        let old = self.active;
        let request = pending.request;
        self.active = request.target;
        self.applied_state = request.to;
        self.pending = None;
        self.last_now_us = Some(now_us);

        Ok(SwitchApplied {
            decision_at_us: request.decision_at_us,
            request_at_us: request.request_at_us,
            effect_at_us: now_us,
            request_to_effect_us: now_us - request.request_at_us,
            from: request.from,
            to: request.to,
            cause: request.cause,
            exact_pts_us: pts_us,
            exact_event_id: event_id,
            active: self.active,
            cancel_pc: request.pc_changed.then_some(old.pc),
            cancel_haptic: request.haptic_changed.then_some(old.haptic),
        })
    }

    pub fn classify_object(&self, role: TrackRole, generation: u64) -> ObjectDisposition {
        if self.active.for_role(role).generation == generation {
            return ObjectDisposition::CurrentReleaseEligible;
        }
        if self
            .pending
            .map(|pending| {
                let request = pending.request;
                let changed = match role {
                    TrackRole::Pc => request.pc_changed,
                    TrackRole::Haptic => request.haptic_changed,
                };
                changed && request.target.for_role(role).generation == generation
            })
            .unwrap_or(false)
        {
            return ObjectDisposition::PendingBarrierOnly;
        }
        ObjectDisposition::StaleDrop
    }

    /// A timeout poisons the gate. The run must fail rather than silently
    /// continuing with a controller state that was not applied on wire.
    pub fn check_timeout(&mut self, now_us: u64) -> Result<(), SwitchError> {
        self.check_ready(now_us)?;
        self.fail_if_timed_out(now_us)?;
        self.last_now_us = Some(now_us);
        Ok(())
    }

    fn fail_if_timed_out(&mut self, now_us: u64) -> Result<(), SwitchError> {
        if self.pending.is_some_and(|pending| {
            now_us.saturating_sub(pending.request.request_at_us) > self.config.effect_timeout_us
        }) {
            self.failed = true;
            self.last_now_us = Some(now_us);
            return Err(SwitchError::SwitchTimedOut);
        }
        Ok(())
    }

    fn check_ready(&self, now_us: u64) -> Result<(), SwitchError> {
        if self.failed {
            return Err(SwitchError::GateFailed);
        }
        if self.last_now_us.is_some_and(|last| now_us < last) {
            return Err(SwitchError::NonMonotonicTime);
        }
        Ok(())
    }
}

fn pc_track(state: S3State) -> &'static str {
    match state {
        S3State::Normal => PC_NORMAL_TRACK,
        S3State::Recovery => PC_RECOVERY_TRACK,
        S3State::HapticCritical => PC_HAPTIC_CRITICAL_TRACK,
    }
}

fn haptic_track(state: S3State) -> &'static str {
    match state.haptic_mode() {
        HapticMode::Full => HAPTIC_FULL_TRACK,
        HapticMode::Essential => HAPTIC_ESSENTIAL_TRACK,
    }
}

fn is_legal_transition(from: S3State, to: S3State) -> bool {
    matches!(
        (from, to),
        (S3State::Normal, S3State::HapticCritical)
            | (S3State::HapticCritical, S3State::Recovery)
            | (S3State::Recovery, S3State::Normal)
            | (S3State::Recovery, S3State::HapticCritical)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3_controller::TransitionCause;

    fn gate() -> S3SwitchGate {
        S3SwitchGate::new(SwitchConfig {
            effect_timeout_us: 1_000_000,
        })
        .unwrap()
    }

    fn transition(at_us: u64, from: S3State, to: S3State) -> S3Transition {
        S3Transition {
            at_us,
            from,
            to,
            cause: TransitionCause::DeadlineMissStreak,
        }
    }

    #[test]
    fn starts_with_s2_identical_normal_track_names() {
        let gate = gate();
        assert_eq!(gate.applied_state(), S3State::Normal);
        assert_eq!(gate.active_routes().pc.name, "pc");
        assert_eq!(gate.active_routes().haptic.name, "haptic");
    }

    #[test]
    fn normal_to_haptic_critical_waits_for_both_ok_and_exact_pair() {
        let mut gate = gate();
        let request = gate
            .request(transition(10, S3State::Normal, S3State::HapticCritical), 10)
            .unwrap();
        assert_eq!(request.target.pc.name, "pc-d6");
        assert_eq!(request.target.haptic.name, "haptic-essential");
        assert_eq!(
            gate.classify_object(TrackRole::Pc, request.target.pc.generation),
            ObjectDisposition::PendingBarrierOnly
        );
        assert_eq!(
            gate.exact_pair_first_effect(1_000, 1, 20),
            Err(SwitchError::FirstEffectBeforeSubscribeOk)
        );
        gate.subscribe_ok(TrackRole::Pc, 21).unwrap();
        gate.subscribe_ok(TrackRole::Haptic, 22).unwrap();
        let applied = gate.exact_pair_first_effect(1_000, 1, 30).unwrap();
        assert_eq!(applied.request_to_effect_us, 20);
        assert_eq!(gate.applied_state(), S3State::HapticCritical);
        assert_eq!(
            gate.classify_object(TrackRole::Pc, applied.cancel_pc.unwrap().generation),
            ObjectDisposition::StaleDrop
        );
        assert_eq!(
            gate.classify_object(TrackRole::Haptic, applied.cancel_haptic.unwrap().generation),
            ObjectDisposition::StaleDrop
        );
    }

    #[test]
    fn recovery_to_normal_reuses_full_haptic_route() {
        let mut gate = gate();
        let first = gate
            .request(transition(10, S3State::Normal, S3State::HapticCritical), 10)
            .unwrap();
        gate.subscribe_ok(TrackRole::Pc, 11).unwrap();
        gate.subscribe_ok(TrackRole::Haptic, 12).unwrap();
        gate.exact_pair_first_effect(100, 1, 13).unwrap();

        gate.request(
            transition(20, S3State::HapticCritical, S3State::Recovery),
            20,
        )
        .unwrap();
        gate.subscribe_ok(TrackRole::Pc, 21).unwrap();
        gate.subscribe_ok(TrackRole::Haptic, 22).unwrap();
        let recovery = gate.exact_pair_first_effect(200, 2, 23).unwrap();
        assert_ne!(
            recovery.active.haptic.generation,
            first.target.haptic.generation
        );

        let request = gate
            .request(transition(30, S3State::Recovery, S3State::Normal), 30)
            .unwrap();
        assert!(request.pc_changed);
        assert!(!request.haptic_changed);
        assert_eq!(request.target.haptic, recovery.active.haptic);
        assert_eq!(
            gate.subscribe_ok(TrackRole::Haptic, 31),
            Err(SwitchError::SubscribeOkForUnchangedRole)
        );
        gate.subscribe_ok(TrackRole::Pc, 32).unwrap();
        let normal = gate.exact_pair_first_effect(300, 3, 33).unwrap();
        assert!(normal.cancel_pc.is_some());
        assert!(normal.cancel_haptic.is_none());
        assert_eq!(normal.active.haptic, recovery.active.haptic);
    }

    #[test]
    fn old_route_stays_current_until_first_effect() {
        let mut gate = gate();
        let old = gate.active_routes();
        let target = gate
            .request(transition(10, S3State::Normal, S3State::HapticCritical), 10)
            .unwrap()
            .target;
        gate.subscribe_ok(TrackRole::Pc, 11).unwrap();
        gate.subscribe_ok(TrackRole::Haptic, 12).unwrap();
        assert_eq!(
            gate.classify_object(TrackRole::Pc, old.pc.generation),
            ObjectDisposition::CurrentReleaseEligible
        );
        assert_eq!(
            gate.classify_object(TrackRole::Pc, target.pc.generation),
            ObjectDisposition::PendingBarrierOnly
        );
    }

    #[test]
    fn stale_and_unknown_generations_are_dropped() {
        let gate = gate();
        assert_eq!(
            gate.classify_object(TrackRole::Pc, 99),
            ObjectDisposition::StaleDrop
        );
        assert_eq!(
            gate.classify_object(TrackRole::Haptic, 99),
            ObjectDisposition::StaleDrop
        );
    }

    #[test]
    fn timeout_is_fail_loud_and_poisoned() {
        let mut gate = gate();
        gate.request(transition(10, S3State::Normal, S3State::HapticCritical), 10)
            .unwrap();
        assert_eq!(gate.check_timeout(1_000_010), Ok(()));
        assert_eq!(
            gate.check_timeout(1_000_011),
            Err(SwitchError::SwitchTimedOut)
        );
        assert!(gate.is_failed());
        assert_eq!(gate.check_timeout(1_000_012), Err(SwitchError::GateFailed));
    }

    #[test]
    fn late_ok_or_first_effect_cannot_bypass_timeout_polling() {
        let mut late_ok = gate();
        late_ok
            .request(transition(10, S3State::Normal, S3State::HapticCritical), 10)
            .unwrap();
        assert_eq!(
            late_ok.subscribe_ok(TrackRole::Pc, 1_000_011),
            Err(SwitchError::SwitchTimedOut)
        );
        assert!(late_ok.is_failed());

        let mut late_effect = gate();
        late_effect
            .request(transition(10, S3State::Normal, S3State::HapticCritical), 10)
            .unwrap();
        late_effect.subscribe_ok(TrackRole::Pc, 11).unwrap();
        late_effect.subscribe_ok(TrackRole::Haptic, 12).unwrap();
        assert_eq!(
            late_effect.exact_pair_first_effect(1_000, 1, 1_000_011),
            Err(SwitchError::SwitchTimedOut)
        );
        assert!(late_effect.is_failed());
    }

    #[test]
    fn rejects_overlap_illegal_from_and_non_monotonic_time() {
        let mut gate = gate();
        assert_eq!(
            gate.request(transition(2, S3State::Normal, S3State::HapticCritical), 1,),
            Err(SwitchError::DecisionAfterRequest)
        );
        assert_eq!(
            gate.request(transition(1, S3State::Normal, S3State::Recovery), 1),
            Err(SwitchError::IllegalTransition)
        );
        gate.request(transition(2, S3State::Normal, S3State::HapticCritical), 2)
            .unwrap();
        assert_eq!(
            gate.request(transition(3, S3State::Normal, S3State::HapticCritical), 3,),
            Err(SwitchError::SwitchAlreadyPending)
        );
        assert_eq!(
            gate.subscribe_ok(TrackRole::Pc, 1),
            Err(SwitchError::NonMonotonicTime)
        );
    }

    #[test]
    fn wrong_from_state_is_rejected_after_apply() {
        let mut gate = gate();
        gate.request(transition(1, S3State::Normal, S3State::HapticCritical), 1)
            .unwrap();
        gate.subscribe_ok(TrackRole::Pc, 2).unwrap();
        gate.subscribe_ok(TrackRole::Haptic, 3).unwrap();
        gate.exact_pair_first_effect(100, 1, 4).unwrap();
        assert_eq!(
            gate.request(transition(5, S3State::Normal, S3State::HapticCritical), 5,),
            Err(SwitchError::TransitionFromUnappliedState)
        );
    }
}
