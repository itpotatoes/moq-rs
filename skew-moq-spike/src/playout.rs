//! Phase-4 S1 bounded application playout scheduler.
//!
//! This module is deliberately transport-agnostic.  It maps the existing
//! snapped PTS/event timeline onto one receiver-monotonic epoch and emits
//! explicit release/drop actions.  It never consults `t_gen`, network state,
//! priority, delivery timeout, or a quality tier controller, preserving the
//! S1 ablation boundary.

use std::collections::{BTreeMap, HashSet};

use bytes::Bytes;

use crate::{Header, TRACK_HAPTIC, TRACK_PC};

pub const DROP_BUFFER_OBJECT_LIMIT: &str = "buffer_object_limit";
pub const DROP_BUFFER_SPAN_LIMIT: &str = "buffer_span_limit";
pub const DROP_LATE: &str = "late";
pub const DROP_STARTUP_TIMEOUT: &str = "startup_timeout";
pub const DROP_SHUTDOWN_BEFORE_EPOCH: &str = "shutdown_before_epoch";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatePolicy {
    ReleaseLate,
    DropLate,
}

impl LatePolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReleaseLate => "release-late",
            Self::DropLate => "drop-late",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlayoutConfig {
    pub d_play_us: u64,
    pub startup_timeout_us: u64,
    pub late_tolerance_us: u64,
    pub max_objects_per_track: usize,
    pub max_span_us: u64,
    pub late_policy: LatePolicy,
}

impl PlayoutConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.d_play_us == 0 {
            return Err("d_play_us must be > 0");
        }
        if self.startup_timeout_us == 0 {
            return Err("startup_timeout_us must be > 0");
        }
        if self.max_objects_per_track == 0 {
            return Err("max_objects_per_track must be > 0");
        }
        if self.max_span_us == 0 {
            return Err("max_span_us must be > 0");
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct PlayoutObject {
    pub header: Header,
    pub t_recv: u64,
    pub bytes: Bytes,
}

impl PlayoutObject {
    pub fn track_name(&self) -> &'static str {
        crate::track_name(self.header.track_id)
    }

    fn identity(&self) -> Identity {
        Identity {
            track_id: self.header.track_id,
            tier: self.header.tier,
            seq: self.header.seq,
            pts_us: self.header.pts_us,
            event_id: self.header.event_id,
        }
    }
}

#[derive(Debug, Clone)]
pub enum PlayoutAction {
    Release(PlayoutObject),
    Drop {
        object: PlayoutObject,
        reason: &'static str,
    },
}

impl PlayoutAction {
    pub fn object(&self) -> &PlayoutObject {
        match self {
            Self::Release(object) | Self::Drop { object, .. } => object,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct Identity {
    track_id: u8,
    tier: u16,
    seq: u32,
    pts_us: u64,
    event_id: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct QueueKey {
    pts_us: u64,
    event_id: u32,
    seq: u32,
    tier: u16,
}

impl From<&PlayoutObject> for QueueKey {
    fn from(object: &PlayoutObject) -> Self {
        Self {
            pts_us: object.header.pts_us,
            event_id: object.header.event_id,
            seq: object.header.seq,
            tier: object.header.tier,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Epoch {
    pts_us: u64,
    monotonic_us: u64,
}

/// Deterministic S1 state machine.  Callers provide receiver-monotonic `now_us`
/// values, which makes unit tests independent of wall time.
pub struct PlayoutScheduler {
    config: PlayoutConfig,
    pc: BTreeMap<QueueKey, PlayoutObject>,
    haptic: BTreeMap<QueueKey, PlayoutObject>,
    terminal: HashSet<Identity>,
    startup_started_us: Option<u64>,
    epoch: Option<Epoch>,
    startup_failed: bool,
}

impl PlayoutScheduler {
    pub fn new(config: PlayoutConfig) -> Result<Self, &'static str> {
        config.validate()?;
        Ok(Self {
            config,
            pc: BTreeMap::new(),
            haptic: BTreeMap::new(),
            terminal: HashSet::new(),
            startup_started_us: None,
            epoch: None,
            startup_failed: false,
        })
    }

    pub fn config(&self) -> PlayoutConfig {
        self.config
    }

    pub fn is_started(&self) -> bool {
        self.epoch.is_some()
    }

    pub fn is_empty(&self) -> bool {
        self.pc.is_empty() && self.haptic.is_empty()
    }

    pub fn buffered_counts(&self) -> (usize, usize) {
        (self.pc.len(), self.haptic.len())
    }

    /// Insert one received object and return any immediately determined
    /// actions.  Repeated exact identities are ignored so they can never be
    /// released twice; the receive-layer integrity checker remains responsible
    /// for diagnosing duplicate rx records.
    pub fn push(&mut self, object: PlayoutObject, now_us: u64) -> Vec<PlayoutAction> {
        if object.header.track_id != TRACK_PC && object.header.track_id != TRACK_HAPTIC {
            return vec![PlayoutAction::Drop {
                object,
                reason: "invalid_track",
            }];
        }
        let identity = object.identity();
        if self.terminal.contains(&identity)
            || self.pc.values().any(|item| item.identity() == identity)
            || self.haptic.values().any(|item| item.identity() == identity)
        {
            return Vec::new();
        }
        if self.startup_failed {
            self.terminal.insert(identity);
            return vec![PlayoutAction::Drop {
                object,
                reason: DROP_STARTUP_TIMEOUT,
            }];
        }

        self.startup_started_us.get_or_insert(now_us);
        let track = object.header.track_id;
        self.buffer_mut(track).insert(QueueKey::from(&object), object);

        let mut actions = self.enforce_bounds(track);
        self.try_start(now_us);
        actions.extend(self.enforce_timeline_horizon(now_us));
        actions.extend(self.advance(now_us));
        actions
    }

    /// Emit every object whose fixed common-timeline deadline has arrived, or
    /// fail startup after the configured bound.
    pub fn advance(&mut self, now_us: u64) -> Vec<PlayoutAction> {
        if self.epoch.is_none() {
            if let Some(start) = self.startup_started_us {
                if now_us.saturating_sub(start) >= self.config.startup_timeout_us {
                    self.startup_failed = true;
                    return self.drop_all(DROP_STARTUP_TIMEOUT);
                }
            }
            return Vec::new();
        }

        let mut due = Vec::new();
        for track in [TRACK_PC, TRACK_HAPTIC] {
            let keys: Vec<QueueKey> = self
                .buffer(track)
                .iter()
                .filter_map(|(key, _)| (self.due_us(key.pts_us) <= now_us).then_some(*key))
                .collect();
            for key in keys {
                let object = self.buffer_mut(track).remove(&key).expect("key came from buffer");
                due.push((self.due_us(object.header.pts_us), object));
            }
        }
        due.sort_by_key(|(deadline, object)| {
            (*deadline, object.header.pts_us, object.header.track_id, object.header.seq)
        });

        let mut actions = Vec::with_capacity(due.len());
        for (deadline, object) in due {
            let identity = object.identity();
            if !self.terminal.insert(identity) {
                continue;
            }
            let too_late = now_us > deadline.saturating_add(self.config.late_tolerance_us);
            if too_late && self.config.late_policy == LatePolicy::DropLate {
                actions.push(PlayoutAction::Drop {
                    object,
                    reason: DROP_LATE,
                });
            } else {
                actions.push(PlayoutAction::Release(object));
            }
        }
        actions
    }

    /// Next receiver-monotonic wakeup needed for a deadline or startup bound.
    pub fn next_wakeup_us(&self) -> Option<u64> {
        if self.epoch.is_some() {
            self.pc
                .keys()
                .chain(self.haptic.keys())
                .map(|key| self.due_us(key.pts_us))
                .min()
        } else if !self.startup_failed {
            self.startup_started_us
                .map(|start| start.saturating_add(self.config.startup_timeout_us))
        } else {
            None
        }
    }

    /// Explicit terminal accounting for a shutdown that cannot establish a
    /// common epoch.  A started scheduler should normally be drained by
    /// advancing to successive wakeups instead.
    pub fn finish_without_epoch(&mut self) -> Vec<PlayoutAction> {
        self.drop_all(DROP_SHUTDOWN_BEFORE_EPOCH)
    }

    fn buffer(&self, track: u8) -> &BTreeMap<QueueKey, PlayoutObject> {
        if track == TRACK_PC {
            &self.pc
        } else {
            &self.haptic
        }
    }

    fn buffer_mut(&mut self, track: u8) -> &mut BTreeMap<QueueKey, PlayoutObject> {
        if track == TRACK_PC {
            &mut self.pc
        } else {
            &mut self.haptic
        }
    }

    fn try_start(&mut self, now_us: u64) {
        if self.epoch.is_some() || self.startup_failed {
            return;
        }
        let pc_pairs: HashSet<(u64, u32)> = self
            .pc
            .values()
            .filter(|item| item.header.event_id > 0)
            .map(|item| (item.header.pts_us, item.header.event_id))
            .collect();
        let first_pair = self
            .haptic
            .values()
            .filter(|item| item.header.event_id > 0)
            .map(|item| (item.header.pts_us, item.header.event_id))
            .filter(|key| pc_pairs.contains(key))
            .min();
        if let Some((pts_us, _)) = first_pair {
            self.epoch = Some(Epoch {
                pts_us,
                monotonic_us: now_us,
            });
        }
    }

    fn due_us(&self, pts_us: u64) -> u64 {
        let epoch = self.epoch.expect("due_us only after epoch");
        let base = epoch.monotonic_us.saturating_add(self.config.d_play_us);
        if pts_us >= epoch.pts_us {
            base.saturating_add(pts_us - epoch.pts_us)
        } else {
            base.saturating_sub(epoch.pts_us - pts_us)
        }
    }

    fn enforce_bounds(&mut self, track: u8) -> Vec<PlayoutAction> {
        let mut dropped = Vec::new();
        while self.buffer(track).len() > self.config.max_objects_per_track {
            if let Some(object) = self.pop_oldest(track) {
                self.terminal.insert(object.identity());
                dropped.push(PlayoutAction::Drop {
                    object,
                    reason: DROP_BUFFER_OBJECT_LIMIT,
                });
            }
        }
        loop {
            let span = match (self.buffer(track).first_key_value(), self.buffer(track).last_key_value()) {
                (Some((first, _)), Some((last, _))) => last.pts_us.saturating_sub(first.pts_us),
                _ => 0,
            };
            if span <= self.config.max_span_us {
                break;
            }
            if let Some(object) = self.pop_oldest(track) {
                self.terminal.insert(object.identity());
                dropped.push(PlayoutAction::Drop {
                    object,
                    reason: DROP_BUFFER_SPAN_LIMIT,
                });
            }
        }
        dropped
    }

    /// The per-track min/max span alone cannot bound a buffer containing one
    /// corrupt far-future PTS. Once the epoch exists, also cap how far an
    /// object may sit ahead of the current common timeline. This makes the
    /// shutdown drain budget a real bound rather than an assumption.
    fn enforce_timeline_horizon(&mut self, now_us: u64) -> Vec<PlayoutAction> {
        if self.epoch.is_none() {
            return Vec::new();
        }
        let latest_due = now_us
            .saturating_add(self.config.d_play_us)
            .saturating_add(self.config.max_span_us);
        let mut dropped = Vec::new();
        for track in [TRACK_PC, TRACK_HAPTIC] {
            let keys: Vec<QueueKey> = self
                .buffer(track)
                .keys()
                .filter(|key| self.due_us(key.pts_us) > latest_due)
                .copied()
                .collect();
            for key in keys {
                let object = self.buffer_mut(track).remove(&key).expect("key came from buffer");
                self.terminal.insert(object.identity());
                dropped.push(PlayoutAction::Drop {
                    object,
                    reason: DROP_BUFFER_SPAN_LIMIT,
                });
            }
        }
        dropped
    }

    fn pop_oldest(&mut self, track: u8) -> Option<PlayoutObject> {
        let key = self.buffer(track).first_key_value().map(|(key, _)| *key)?;
        self.buffer_mut(track).remove(&key)
    }

    fn drop_all(&mut self, reason: &'static str) -> Vec<PlayoutAction> {
        let mut objects: Vec<PlayoutObject> = std::mem::take(&mut self.pc)
            .into_values()
            .chain(std::mem::take(&mut self.haptic).into_values())
            .collect();
        objects.sort_by_key(|object| {
            (object.header.pts_us, object.header.track_id, object.header.seq)
        });
        objects
            .into_iter()
            .filter_map(|object| {
                self.terminal.insert(object.identity()).then_some(PlayoutAction::Drop {
                    object,
                    reason,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> PlayoutConfig {
        PlayoutConfig {
            d_play_us: 50_000,
            startup_timeout_us: 100_000,
            late_tolerance_us: 5_000,
            max_objects_per_track: 4,
            max_span_us: 100_000,
            late_policy: LatePolicy::DropLate,
        }
    }

    fn object(track_id: u8, seq: u32, pts_us: u64, event_id: u32, t_recv: u64) -> PlayoutObject {
        PlayoutObject {
            header: Header {
                version: 1,
                track_id,
                tier: 0,
                seq,
                pts_us,
                event_id,
                gen_ts_us: 9_999_999_999,
                payload_len: 0,
            },
            t_recv,
            bytes: Bytes::new(),
        }
    }

    fn releases(actions: &[PlayoutAction]) -> usize {
        actions.iter().filter(|a| matches!(a, PlayoutAction::Release(_))).count()
    }

    #[test]
    fn exact_anchor_pair_creates_one_epoch_and_common_deadline() {
        let mut scheduler = PlayoutScheduler::new(config()).unwrap();
        assert!(scheduler.push(object(TRACK_PC, 0, 0, 1, 1_000), 1_000).is_empty());
        assert!(scheduler.push(object(TRACK_HAPTIC, 0, 0, 1, 2_000), 2_000).is_empty());
        assert!(scheduler.is_started());
        assert_eq!(scheduler.next_wakeup_us(), Some(52_000));
        assert!(scheduler.advance(51_999).is_empty());
        let actions = scheduler.advance(52_000);
        assert_eq!(releases(&actions), 2);
        assert!(actions.iter().all(|a| a.object().header.pts_us == 0));
    }

    #[test]
    fn sender_generation_clock_never_defines_epoch() {
        let mut scheduler = PlayoutScheduler::new(config()).unwrap();
        scheduler.push(object(TRACK_PC, 0, 0, 1, 10_000), 10_000);
        scheduler.push(object(TRACK_HAPTIC, 0, 0, 1, 20_000), 20_000);
        assert_eq!(scheduler.next_wakeup_us(), Some(70_000));
    }

    #[test]
    fn filler_uses_same_pts_axis_but_not_startup_pairing() {
        let mut scheduler = PlayoutScheduler::new(config()).unwrap();
        scheduler.push(object(TRACK_HAPTIC, 0, 10_000, 0, 1_000), 1_000);
        assert!(!scheduler.is_started());
        scheduler.push(object(TRACK_PC, 0, 0, 1, 2_000), 2_000);
        scheduler.push(object(TRACK_HAPTIC, 1, 0, 1, 3_000), 3_000);
        assert!(scheduler.is_started());
        assert_eq!(scheduler.advance(53_000).len(), 2);
        assert_eq!(scheduler.advance(63_000).len(), 1);
    }

    #[test]
    fn late_policy_is_explicit_and_bounded() {
        let mut scheduler = PlayoutScheduler::new(config()).unwrap();
        scheduler.push(object(TRACK_PC, 0, 0, 1, 0), 0);
        scheduler.push(object(TRACK_HAPTIC, 0, 0, 1, 0), 0);
        let actions = scheduler.advance(60_000);
        assert_eq!(actions.len(), 2);
        assert!(actions.iter().all(|a| matches!(a, PlayoutAction::Drop { reason: DROP_LATE, .. })));

        let mut release_cfg = config();
        release_cfg.late_policy = LatePolicy::ReleaseLate;
        let mut scheduler = PlayoutScheduler::new(release_cfg).unwrap();
        scheduler.push(object(TRACK_PC, 0, 0, 1, 0), 0);
        scheduler.push(object(TRACK_HAPTIC, 0, 0, 1, 0), 0);
        assert_eq!(releases(&scheduler.advance(60_000)), 2);
    }

    #[test]
    fn object_and_time_bounds_drop_oldest_with_distinct_reasons() {
        let mut cfg = config();
        cfg.max_objects_per_track = 2;
        cfg.max_span_us = 1_000_000;
        let mut scheduler = PlayoutScheduler::new(cfg).unwrap();
        scheduler.push(object(TRACK_PC, 0, 0, 1, 0), 0);
        scheduler.push(object(TRACK_PC, 1, 10_000, 2, 1), 1);
        let actions = scheduler.push(object(TRACK_PC, 2, 20_000, 3, 2), 2);
        assert!(matches!(&actions[0], PlayoutAction::Drop { reason: DROP_BUFFER_OBJECT_LIMIT, .. }));

        let mut cfg = config();
        cfg.max_span_us = 10_000;
        let mut scheduler = PlayoutScheduler::new(cfg).unwrap();
        scheduler.push(object(TRACK_PC, 0, 0, 1, 0), 0);
        let actions = scheduler.push(object(TRACK_PC, 1, 20_000, 2, 1), 1);
        assert!(matches!(&actions[0], PlayoutAction::Drop { reason: DROP_BUFFER_SPAN_LIMIT, .. }));

        let mut scheduler = PlayoutScheduler::new(config()).unwrap();
        scheduler.push(object(TRACK_PC, 0, 0, 1, 0), 0);
        scheduler.push(object(TRACK_HAPTIC, 0, 0, 1, 0), 0);
        let actions = scheduler.push(object(TRACK_PC, 1, 10_000_000, 2, 1), 1);
        assert!(actions.iter().any(|action| matches!(
            action,
            PlayoutAction::Drop { reason: DROP_BUFFER_SPAN_LIMIT, .. }
        )));
    }

    #[test]
    fn startup_timeout_and_shutdown_account_for_every_buffered_object() {
        let mut scheduler = PlayoutScheduler::new(config()).unwrap();
        scheduler.push(object(TRACK_PC, 0, 0, 1, 1_000), 1_000);
        let actions = scheduler.advance(101_000);
        assert_eq!(actions.len(), 1);
        assert!(matches!(&actions[0], PlayoutAction::Drop { reason: DROP_STARTUP_TIMEOUT, .. }));
        let actions = scheduler.push(object(TRACK_HAPTIC, 0, 0, 1, 102_000), 102_000);
        assert_eq!(actions.len(), 1);

        let mut scheduler = PlayoutScheduler::new(config()).unwrap();
        scheduler.push(object(TRACK_PC, 0, 0, 1, 0), 0);
        let actions = scheduler.finish_without_epoch();
        assert_eq!(actions.len(), 1);
        assert!(matches!(&actions[0], PlayoutAction::Drop { reason: DROP_SHUTDOWN_BEFORE_EPOCH, .. }));
    }

    #[test]
    fn duplicate_identity_is_never_released_twice() {
        let mut scheduler = PlayoutScheduler::new(config()).unwrap();
        let pc = object(TRACK_PC, 0, 0, 1, 0);
        scheduler.push(pc.clone(), 0);
        assert!(scheduler.push(pc, 1).is_empty());
        scheduler.push(object(TRACK_HAPTIC, 0, 0, 1, 1), 1);
        assert_eq!(releases(&scheduler.advance(51_000)), 2);
    }
}
