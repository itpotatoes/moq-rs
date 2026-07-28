//! Receiver-side S3 route-generation ingress gate.
//!
//! This module is transport agnostic. It turns wire-route-tagged objects into
//! scheduler-eligible objects, switch-barrier terminal drops, stale terminal
//! drops, and one atomic apply event. Subscription creation/cancellation stays
//! in the binary adapter.

use std::collections::{BTreeMap, VecDeque};

use crate::playout::{PlayoutAction, PlayoutObject, PlayoutScheduler};
use crate::s3_controller::{S3Observation, S3Transition};
use crate::s3_switch::{
    ObjectDisposition, Route, S3SwitchGate, SwitchApplied, SwitchError, SwitchRequest, TrackRole,
};
use crate::{TRACK_HAPTIC, TRACK_PC};

pub const DROP_STALE_TIER: &str = "stale_tier";
pub const DROP_SWITCH_BARRIER: &str = "switch_barrier";
pub const DROP_SWITCH_BARRIER_OVERFLOW: &str = "switch_barrier_overflow";

#[derive(Debug, Clone, Copy)]
struct AnchorArrival {
    t_recv: u64,
}

/// Emits only the controller inputs defined by the frozen design:
/// receiver-timeline exact-pair release skew, or an exact-PC deadline miss.
/// A PC that met the deadline but whose pair was not jointly released produces
/// no synthetic observation.
pub struct S3DeadlineTracker {
    max_anchors: usize,
    active: bool,
    pc_arrivals: BTreeMap<PairKey, AnchorArrival>,
    haptic_arrivals: BTreeMap<PairKey, AnchorArrival>,
    pc_releases: BTreeMap<PairKey, u64>,
    haptic_releases: BTreeMap<PairKey, u64>,
}

impl S3DeadlineTracker {
    pub fn new(max_anchors: usize) -> Result<Self, &'static str> {
        if max_anchors == 0 {
            return Err("max_anchors must be > 0");
        }
        Ok(Self {
            max_anchors,
            active: false,
            pc_arrivals: BTreeMap::new(),
            haptic_arrivals: BTreeMap::new(),
            pc_releases: BTreeMap::new(),
            haptic_releases: BTreeMap::new(),
        })
    }

    pub fn activate(&mut self) -> Result<(), &'static str> {
        if self.active {
            return Err("S3 deadline tracker already active");
        }
        self.active = true;
        Ok(())
    }

    pub fn note_received(&mut self, object: &PlayoutObject) -> Result<(), &'static str> {
        let Some(key) = PairKey::from_object(object) else {
            return Ok(());
        };
        let map = match object.header.track_id {
            TRACK_PC => &mut self.pc_arrivals,
            TRACK_HAPTIC => &mut self.haptic_arrivals,
            _ => return Err("deadline tracker received an invalid semantic track"),
        };
        map.entry(key).or_insert(AnchorArrival {
            t_recv: object.t_recv,
        });
        if map.len() > self.max_anchors {
            return Err("S3 deadline tracker anchor bound exceeded");
        }
        Ok(())
    }

    /// Forget a scheduler object terminally evicted by the external route
    /// barrier. Ordinary playout drops remain observations; only cancelled
    /// generation objects use this path.
    pub fn forget_evicted(&mut self, object: &PlayoutObject) -> Result<(), &'static str> {
        let Some(key) = PairKey::from_object(object) else {
            return Ok(());
        };
        match object.header.track_id {
            TRACK_PC => {
                self.pc_arrivals.remove(&key);
                self.pc_releases.remove(&key);
            }
            TRACK_HAPTIC => {
                self.haptic_arrivals.remove(&key);
                self.haptic_releases.remove(&key);
            }
            _ => return Err("deadline tracker forgot an invalid semantic track"),
        }
        Ok(())
    }

    /// Call after scheduler actions have been emitted for `now_us`, before
    /// [`Self::advance`] evaluates anchors due at the same timestamp.
    pub fn note_actions(
        &mut self,
        actions: &[PlayoutAction],
        now_us: u64,
    ) -> Result<(), &'static str> {
        for action in actions {
            let PlayoutAction::Release(object) = action else {
                continue;
            };
            let Some(key) = PairKey::from_object(object) else {
                continue;
            };
            let map = match object.header.track_id {
                TRACK_PC => &mut self.pc_releases,
                TRACK_HAPTIC => &mut self.haptic_releases,
                _ => return Err("deadline tracker saw an invalid release track"),
            };
            map.entry(key).or_insert(now_us);
            if map.len() > self.max_anchors {
                return Err("S3 deadline tracker release bound exceeded");
            }
        }
        Ok(())
    }

    pub fn next_wakeup_us(&self, scheduler: &PlayoutScheduler) -> Option<u64> {
        if !self.active {
            return None;
        }
        self.haptic_arrivals
            .keys()
            .filter_map(|key| scheduler.deadline_us(key.pts_us))
            .min()
    }

    pub fn advance(
        &mut self,
        scheduler: &PlayoutScheduler,
        now_us: u64,
    ) -> Result<Vec<S3Observation>, &'static str> {
        if !self.active {
            return Ok(Vec::new());
        }
        let due: Vec<PairKey> = self
            .haptic_arrivals
            .keys()
            .filter(|key| {
                scheduler
                    .deadline_us(key.pts_us)
                    .is_some_and(|deadline| deadline <= now_us)
            })
            .copied()
            .collect();
        let mut observations = Vec::with_capacity(due.len());
        for key in due {
            let deadline = scheduler
                .deadline_us(key.pts_us)
                .ok_or("S3 deadline tracker lost the scheduler epoch")?;
            let pc_met_deadline = self
                .pc_arrivals
                .get(&key)
                .is_some_and(|arrival| arrival.t_recv <= deadline);
            if !pc_met_deadline {
                observations.push(S3Observation {
                    now_us,
                    deadline_miss: true,
                    abs_skew_us: None,
                });
            } else if let (Some(pc), Some(haptic)) = (
                self.pc_releases.get(&key).copied(),
                self.haptic_releases.get(&key).copied(),
            ) {
                observations.push(S3Observation {
                    now_us,
                    deadline_miss: false,
                    abs_skew_us: Some(pc.abs_diff(haptic)),
                });
            }
            self.pc_arrivals.remove(&key);
            self.haptic_arrivals.remove(&key);
            self.pc_releases.remove(&key);
            self.haptic_releases.remove(&key);
        }
        Ok(observations)
    }
}

#[derive(Debug, Clone)]
pub struct RoutedObject {
    pub role: TrackRole,
    pub route: Route,
    pub object: PlayoutObject,
}

#[derive(Debug, Clone)]
pub enum IngressEvent {
    Scheduler(RoutedObject),
    Drop {
        routed: RoutedObject,
        reason: &'static str,
    },
    Applied(SwitchApplied),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct PairKey {
    pts_us: u64,
    event_id: u32,
}

impl PairKey {
    fn from_object(object: &PlayoutObject) -> Option<Self> {
        (object.header.event_id > 0).then_some(Self {
            pts_us: object.header.pts_us,
            event_id: object.header.event_id,
        })
    }
}

pub struct S3ReceiverIngress {
    gate: S3SwitchGate,
    max_barrier_objects_per_role: usize,
    pending_pc: BTreeMap<PairKey, RoutedObject>,
    pending_haptic: BTreeMap<PairKey, RoutedObject>,
    pending_order_pc: VecDeque<PairKey>,
    pending_order_haptic: VecDeque<PairKey>,
    current_pc_anchors: BTreeMap<PairKey, RoutedObject>,
    current_haptic_anchors: BTreeMap<PairKey, RoutedObject>,
    current_order_pc: VecDeque<PairKey>,
    current_order_haptic: VecDeque<PairKey>,
}

impl S3ReceiverIngress {
    pub fn new(
        gate: S3SwitchGate,
        max_barrier_objects_per_role: usize,
    ) -> Result<Self, &'static str> {
        if max_barrier_objects_per_role == 0 {
            return Err("max_barrier_objects_per_role must be > 0");
        }
        Ok(Self {
            gate,
            max_barrier_objects_per_role,
            pending_pc: BTreeMap::new(),
            pending_haptic: BTreeMap::new(),
            pending_order_pc: VecDeque::new(),
            pending_order_haptic: VecDeque::new(),
            current_pc_anchors: BTreeMap::new(),
            current_haptic_anchors: BTreeMap::new(),
            current_order_pc: VecDeque::new(),
            current_order_haptic: VecDeque::new(),
        })
    }

    pub fn gate(&self) -> &S3SwitchGate {
        &self.gate
    }

    pub fn request(
        &mut self,
        transition: S3Transition,
        now_us: u64,
    ) -> Result<SwitchRequest, SwitchError> {
        debug_assert!(self.pending_pc.is_empty() && self.pending_haptic.is_empty());
        self.gate.request(transition, now_us)
    }

    pub fn subscribe_ok(&mut self, role: TrackRole, now_us: u64) -> Result<(), SwitchError> {
        self.gate.subscribe_ok(role, now_us)
    }

    pub fn check_timeout(&mut self, now_us: u64) -> Result<(), SwitchError> {
        self.gate.check_timeout(now_us)
    }

    /// Terminally account target objects that never crossed the exact-pair
    /// barrier (shutdown, timeout, or subscription failure).
    pub fn finish_pending(&mut self) -> Vec<IngressEvent> {
        let mut events = Vec::with_capacity(self.pending_pc.len() + self.pending_haptic.len());
        for (_, routed) in std::mem::take(&mut self.pending_pc) {
            events.push(IngressEvent::Drop {
                routed,
                reason: DROP_SWITCH_BARRIER,
            });
        }
        for (_, routed) in std::mem::take(&mut self.pending_haptic) {
            events.push(IngressEvent::Drop {
                routed,
                reason: DROP_SWITCH_BARRIER,
            });
        }
        self.pending_order_pc.clear();
        self.pending_order_haptic.clear();
        events
    }

    pub fn push(
        &mut self,
        routed: RoutedObject,
        now_us: u64,
    ) -> Result<Vec<IngressEvent>, &'static str> {
        validate_routed_object(&routed)?;
        match self
            .gate
            .classify_object(routed.role, routed.route.generation)
        {
            ObjectDisposition::StaleDrop => Ok(vec![IngressEvent::Drop {
                routed,
                reason: DROP_STALE_TIER,
            }]),
            ObjectDisposition::CurrentReleaseEligible => {
                let mut events = vec![IngressEvent::Scheduler(routed.clone())];
                self.remember_current(routed);
                events.extend(self.try_apply(now_us)?);
                Ok(events)
            }
            ObjectDisposition::PendingBarrierOnly => {
                let mut events = self.remember_pending(routed);
                events.extend(self.try_apply(now_us)?);
                Ok(events)
            }
        }
    }

    fn remember_current(&mut self, routed: RoutedObject) {
        let Some(key) = PairKey::from_object(&routed.object) else {
            return;
        };
        let (map, order) = match routed.role {
            TrackRole::Pc => (&mut self.current_pc_anchors, &mut self.current_order_pc),
            TrackRole::Haptic => (
                &mut self.current_haptic_anchors,
                &mut self.current_order_haptic,
            ),
        };
        if map.insert(key, routed).is_none() {
            order.push_back(key);
        }
        while order.len() > self.max_barrier_objects_per_role {
            if let Some(oldest) = order.pop_front() {
                map.remove(&oldest);
            }
        }
    }

    fn remember_pending(&mut self, routed: RoutedObject) -> Vec<IngressEvent> {
        let Some(key) = PairKey::from_object(&routed.object) else {
            return vec![IngressEvent::Drop {
                routed,
                reason: DROP_SWITCH_BARRIER,
            }];
        };
        let (map, order) = match routed.role {
            TrackRole::Pc => (&mut self.pending_pc, &mut self.pending_order_pc),
            TrackRole::Haptic => (&mut self.pending_haptic, &mut self.pending_order_haptic),
        };
        let mut events = Vec::new();
        if let Some(replaced) = map.insert(key, routed) {
            events.push(IngressEvent::Drop {
                routed: replaced,
                reason: DROP_SWITCH_BARRIER,
            });
        } else {
            order.push_back(key);
        }
        while order.len() > self.max_barrier_objects_per_role {
            if let Some(oldest) = order.pop_front() {
                if let Some(routed) = map.remove(&oldest) {
                    events.push(IngressEvent::Drop {
                        routed,
                        reason: DROP_SWITCH_BARRIER_OVERFLOW,
                    });
                }
            }
        }
        events
    }

    fn try_apply(&mut self, now_us: u64) -> Result<Vec<IngressEvent>, &'static str> {
        let Some(request) = self.gate.pending_request() else {
            return Ok(Vec::new());
        };
        let keys: Vec<PairKey> = if request.pc_changed {
            self.pending_pc.keys().copied().collect()
        } else {
            self.current_pc_anchors.keys().copied().collect()
        };
        let Some(key) = keys.into_iter().find(|key| {
            let pc = if request.pc_changed {
                self.pending_pc.get(key)
            } else {
                self.current_pc_anchors.get(key)
            };
            let haptic = if request.haptic_changed {
                self.pending_haptic.get(key)
            } else {
                self.current_haptic_anchors.get(key)
            };
            pc.is_some() && haptic.is_some()
        }) else {
            return Ok(Vec::new());
        };

        let applied = self
            .gate
            .exact_pair_first_effect(key.pts_us, key.event_id, now_us)
            .map_err(|_| "S3 exact-pair apply rejected")?;
        let mut events = vec![IngressEvent::Applied(applied)];

        for (_, routed) in std::mem::take(&mut self.pending_pc) {
            events.push(IngressEvent::Drop {
                routed,
                reason: DROP_SWITCH_BARRIER,
            });
        }
        for (_, routed) in std::mem::take(&mut self.pending_haptic) {
            events.push(IngressEvent::Drop {
                routed,
                reason: DROP_SWITCH_BARRIER,
            });
        }
        self.pending_order_pc.clear();
        self.pending_order_haptic.clear();

        // Every target object received before first effect remains
        // barrier-only, including the pair that triggers apply. The old route
        // supplied the release-eligible source slot; forwarding the target
        // copy would duplicate one semantic source identity. The next object
        // on the now-current target route is scheduler-eligible.
        if applied.cancel_pc.is_some() {
            self.current_pc_anchors
                .retain(|_, object| object.route == applied.active.pc);
            self.current_order_pc
                .retain(|key| self.current_pc_anchors.contains_key(key));
        }
        if applied.cancel_haptic.is_some() {
            self.current_haptic_anchors
                .retain(|_, object| object.route == applied.active.haptic);
            self.current_order_haptic
                .retain(|key| self.current_haptic_anchors.contains_key(key));
        }
        Ok(events)
    }
}

pub fn validate_routed_object(routed: &RoutedObject) -> Result<(), &'static str> {
    let header = routed.object.header;
    match routed.role {
        TrackRole::Pc => {
            if header.track_id != TRACK_PC {
                return Err("PC route carried a non-PC header");
            }
            let expected_tier = match routed.route.name {
                "pc" => 2,
                "pc-d7" => 3,
                "pc-d6" => 4,
                _ => return Err("unknown PC wire route"),
            };
            if header.tier != expected_tier {
                return Err("PC wire route/header tier mismatch");
            }
        }
        TrackRole::Haptic => {
            if header.track_id != TRACK_HAPTIC {
                return Err("haptic route carried a non-haptic header");
            }
            if !matches!(routed.route.name, "haptic" | "haptic-essential") {
                return Err("unknown haptic wire route");
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::playout::{LatePolicy, PlayoutConfig};
    use crate::s3_controller::{S3State, TransitionCause};
    use crate::s3_switch::{SwitchConfig, HAPTIC_ESSENTIAL_TRACK, HAPTIC_FULL_TRACK};
    use crate::{Header, VERSION};

    fn transition(from: S3State, to: S3State, at_us: u64) -> S3Transition {
        S3Transition {
            at_us,
            from,
            to,
            cause: TransitionCause::DeadlineMissStreak,
        }
    }

    fn object(role: TrackRole, route: Route, pts_us: u64, event_id: u32) -> RoutedObject {
        let (track_id, tier) = match role {
            TrackRole::Pc => (
                TRACK_PC,
                match route.name {
                    "pc" => 2,
                    "pc-d7" => 3,
                    "pc-d6" => 4,
                    _ => 99,
                },
            ),
            TrackRole::Haptic => (TRACK_HAPTIC, 0),
        };
        RoutedObject {
            role,
            route,
            object: PlayoutObject {
                header: Header {
                    version: VERSION,
                    track_id,
                    tier,
                    seq: event_id,
                    pts_us,
                    event_id,
                    gen_ts_us: 1,
                    payload_len: 0,
                },
                t_recv: 1,
                bytes: Bytes::new(),
            },
        }
    }

    fn ingress(bound: usize) -> S3ReceiverIngress {
        S3ReceiverIngress::new(
            S3SwitchGate::new(SwitchConfig {
                effect_timeout_us: 1_000,
            })
            .unwrap(),
            bound,
        )
        .unwrap()
    }

    fn scheduler() -> PlayoutScheduler {
        PlayoutScheduler::new(PlayoutConfig {
            d_play_us: 50_000,
            startup_timeout_us: 2_000_000,
            startup_rearm_limit: 1,
            late_tolerance_us: 10_000,
            max_objects_per_track: 64,
            max_span_us: 2_000_000,
            late_policy: LatePolicy::DropLate,
        })
        .unwrap()
    }

    #[test]
    fn target_is_barrier_only_until_exact_pair_then_old_generation_is_stale() {
        let mut ingress = ingress(8);
        let old = ingress.gate().active_routes();
        assert!(matches!(
            ingress
                .push(object(TrackRole::Pc, old.pc, 100, 1), 1)
                .unwrap()
                .as_slice(),
            [IngressEvent::Scheduler(_)]
        ));
        let request = ingress
            .request(transition(S3State::Normal, S3State::HapticCritical, 2), 2)
            .unwrap();
        ingress.subscribe_ok(TrackRole::Pc, 3).unwrap();
        ingress.subscribe_ok(TrackRole::Haptic, 4).unwrap();
        assert!(ingress
            .push(object(TrackRole::Pc, request.target.pc, 200, 2), 5)
            .unwrap()
            .is_empty());
        let events = ingress
            .push(object(TrackRole::Haptic, request.target.haptic, 200, 2), 6)
            .unwrap();
        assert!(matches!(events[0], IngressEvent::Applied(_)));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, IngressEvent::Scheduler(_)))
                .count(),
            0
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event,
                    IngressEvent::Drop {
                        reason: DROP_SWITCH_BARRIER,
                        ..
                    }
                ))
                .count(),
            2
        );
        let stale = ingress
            .push(object(TrackRole::Pc, old.pc, 300, 3), 7)
            .unwrap();
        assert!(matches!(
            stale.as_slice(),
            [IngressEvent::Drop {
                reason: DROP_STALE_TIER,
                ..
            }]
        ));
    }

    #[test]
    fn unchanged_haptic_current_route_can_complete_recovery_to_normal_barrier() {
        let mut ingress = ingress(8);
        let first = ingress
            .request(transition(S3State::Normal, S3State::HapticCritical, 1), 1)
            .unwrap();
        ingress.subscribe_ok(TrackRole::Pc, 2).unwrap();
        ingress.subscribe_ok(TrackRole::Haptic, 3).unwrap();
        ingress
            .push(object(TrackRole::Pc, first.target.pc, 100, 1), 4)
            .unwrap();
        ingress
            .push(object(TrackRole::Haptic, first.target.haptic, 100, 1), 5)
            .unwrap();

        let second = ingress
            .request(transition(S3State::HapticCritical, S3State::Recovery, 6), 6)
            .unwrap();
        ingress.subscribe_ok(TrackRole::Pc, 7).unwrap();
        ingress.subscribe_ok(TrackRole::Haptic, 8).unwrap();
        ingress
            .push(object(TrackRole::Pc, second.target.pc, 200, 2), 9)
            .unwrap();
        ingress
            .push(object(TrackRole::Haptic, second.target.haptic, 200, 2), 10)
            .unwrap();

        let recovery = ingress.gate().active_routes();
        assert_eq!(recovery.haptic.name, HAPTIC_FULL_TRACK);
        let third = ingress
            .request(transition(S3State::Recovery, S3State::Normal, 11), 11)
            .unwrap();
        assert!(!third.haptic_changed);
        ingress.subscribe_ok(TrackRole::Pc, 12).unwrap();
        let current_haptic = ingress
            .push(object(TrackRole::Haptic, recovery.haptic, 300, 3), 13)
            .unwrap();
        assert!(matches!(
            current_haptic.as_slice(),
            [IngressEvent::Scheduler(_)]
        ));
        let applied = ingress
            .push(object(TrackRole::Pc, third.target.pc, 300, 3), 14)
            .unwrap();
        assert!(applied
            .iter()
            .any(|event| matches!(event, IngressEvent::Applied(_))));
        assert_eq!(
            ingress.gate().active_routes().haptic.name,
            HAPTIC_FULL_TRACK
        );
        assert_ne!(
            ingress.gate().active_routes().haptic.name,
            HAPTIC_ESSENTIAL_TRACK
        );
    }

    #[test]
    fn barrier_memory_is_bounded_and_overflow_is_terminal() {
        let mut ingress = ingress(1);
        let request = ingress
            .request(transition(S3State::Normal, S3State::HapticCritical, 1), 1)
            .unwrap();
        ingress.subscribe_ok(TrackRole::Pc, 2).unwrap();
        ingress.subscribe_ok(TrackRole::Haptic, 3).unwrap();
        assert!(ingress
            .push(object(TrackRole::Pc, request.target.pc, 100, 1), 4)
            .unwrap()
            .is_empty());
        let overflow = ingress
            .push(object(TrackRole::Pc, request.target.pc, 200, 2), 5)
            .unwrap();
        assert!(matches!(
            overflow.as_slice(),
            [IngressEvent::Drop {
                reason: DROP_SWITCH_BARRIER_OVERFLOW,
                ..
            }]
        ));
    }

    #[test]
    fn semantic_track_and_pc_tier_mismatch_fail_loud() {
        let mut ingress = ingress(2);
        let route = ingress.gate().active_routes().pc;
        let mut wrong = object(TrackRole::Pc, route, 100, 1);
        wrong.object.header.tier = 4;
        assert_eq!(
            ingress.push(wrong, 1).unwrap_err(),
            "PC wire route/header tier mismatch"
        );
    }

    #[test]
    fn deadline_tracker_emits_release_skew_and_exact_pc_miss_only_after_active() {
        let routes = ingress(4).gate().active_routes();
        let pc = object(TrackRole::Pc, routes.pc, 0, 1).object;
        let haptic = object(TrackRole::Haptic, routes.haptic, 0, 1).object;
        let mut scheduler = scheduler();
        let mut tracker = S3DeadlineTracker::new(8).unwrap();
        tracker.note_received(&pc).unwrap();
        tracker.note_received(&haptic).unwrap();
        scheduler.push(pc, 1);
        scheduler.push(haptic, 2);
        assert!(scheduler.is_started());
        assert!(tracker.advance(&scheduler, 50_002).unwrap().is_empty());

        tracker.activate().unwrap();
        let actions = scheduler.advance(50_002);
        tracker.note_actions(&actions, 50_002).unwrap();
        let paired = tracker.advance(&scheduler, 50_002).unwrap();
        assert_eq!(paired.len(), 1);
        assert!(!paired[0].deadline_miss);
        assert_eq!(paired[0].abs_skew_us, Some(0));

        let next_haptic = object(TrackRole::Haptic, routes.haptic, 100_000, 4).object;
        tracker.note_received(&next_haptic).unwrap();
        scheduler.push(next_haptic, 100_002);
        let actions = scheduler.advance(150_002);
        tracker.note_actions(&actions, 150_002).unwrap();
        let missed = tracker.advance(&scheduler, 150_002).unwrap();
        assert_eq!(missed.len(), 1);
        assert!(missed[0].deadline_miss);
        assert_eq!(missed[0].abs_skew_us, None);
    }

    #[test]
    fn pc_that_met_deadline_without_joint_release_does_not_create_fake_pair() {
        let routes = ingress(4).gate().active_routes();
        let pc = object(TrackRole::Pc, routes.pc, 0, 1).object;
        let haptic = object(TrackRole::Haptic, routes.haptic, 0, 1).object;
        let mut scheduler = scheduler();
        scheduler.push(pc.clone(), 1);
        scheduler.push(haptic.clone(), 2);
        let mut tracker = S3DeadlineTracker::new(8).unwrap();
        tracker.note_received(&pc).unwrap();
        tracker.note_received(&haptic).unwrap();
        tracker.activate().unwrap();

        // Deliberately do not report the scheduler release actions. The
        // tracker must not substitute receive time or a zero skew.
        scheduler.advance(50_002);
        assert!(tracker.advance(&scheduler, 50_002).unwrap().is_empty());
    }

    #[test]
    fn route_barrier_evictions_do_not_create_stale_deadline_observations() {
        let routes = ingress(4).gate().active_routes();
        let pc = object(TrackRole::Pc, routes.pc, 0, 1).object;
        let haptic = object(TrackRole::Haptic, routes.haptic, 0, 1).object;
        let mut scheduler = scheduler();
        scheduler.push(pc.clone(), 1);
        scheduler.push(haptic.clone(), 2);
        let mut tracker = S3DeadlineTracker::new(8).unwrap();
        tracker.note_received(&pc).unwrap();
        tracker.note_received(&haptic).unwrap();
        tracker.activate().unwrap();

        tracker.forget_evicted(&pc).unwrap();
        tracker.forget_evicted(&haptic).unwrap();
        assert!(tracker.advance(&scheduler, 50_002).unwrap().is_empty());
        assert_eq!(tracker.next_wakeup_us(&scheduler), None);
    }
}
