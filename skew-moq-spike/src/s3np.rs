//! Phase-4 / plan 단계 9 **event-pair NON-preserving control** (`s3np`).
//!
//! The plan (`md/20260904_Codex용_실험실행계획과_측정지표.md`, 단계 9, method 6)
//! requires a sixth method that uses "the same point-cloud quality and haptic
//! data rate as S3 but does not preserve the PC–haptic event pairs". The
//! difference S3 − S3NP is therefore attributed to *event-pair preservation*
//! alone, not to load reduction.
//!
//! Two independent pieces live here, deliberately separated from transport:
//!
//! 1. [`TierSchedule`] — the open-loop **replay** of a prior S3 run's tier and
//!    haptic-density schedule. The S3 FSM is not run in `s3np`; the sender
//!    simply applies the recorded `(t_offset_us, pc_tier, haptic_density)`
//!    sequence, so the per-track byte volume and the PC quality tiers match the
//!    paired S3 run without any closed-loop adaptation.
//! 2. [`PerTrackReleaseScheduler`] — the receiver's **pairing-free** release
//!    rule. Each object is released on its own track timeline at
//!    `t_release = t_gen + D_play`; there is no common epoch anchored on an
//!    exact PC/haptic anchor pair, no wait for the counterpart track, and no
//!    deadline-miss coupling between tracks.
//!
//! Measurement semantics are untouched: the sender still stamps the exact v5
//! anchor identities (`pts_us`/`event_id`, PC `i` ↔ haptic `3i`) and the
//! receiver still writes the same `role:"rx"` and `role:"release"` rows, so the
//! analyzer pairs `delta_tau = t_play_pc - t_play_haptic` afterwards exactly as
//! for S1/S2/S3. What `s3np` removes is the *system's* use of the pair, not the
//! measurement's ability to observe it.
//!
//! `t_gen + D_play` is a receiver-side monotonic deadline **only** because this
//! rig is single-host netns with one shared `CLOCK_MONOTONIC` (AGENTS.md /
//! CLAUDE.md: "`D = t_recv - t_gen` is valid because the namespaces share the
//! host monotonic clock"). A multi-host port would need an explicit clock
//! transfer here.

use std::collections::{BTreeMap, HashSet};

use crate::playout::{
    LatePolicy, PlayoutAction, PlayoutObject, DROP_BUFFER_OBJECT_LIMIT, DROP_BUFFER_SPAN_LIMIT,
    DROP_LATE,
};
use crate::{TRACK_HAPTIC, TRACK_PC};

/// Schema of the replay document produced by
/// `scripts/event_pair_tier_schedule.py`. Bumping this string is the only
/// sanctioned way to change the document's meaning.
pub const TIER_SCHEDULE_SCHEMA: &str = "event-pair-s3np-tier-schedule-v1";

/// The release rule recorded in metadata so an `s3np` run can never be read as
/// an S1/S2/S3 common-timeline run (and vice versa).
pub const RELEASE_RULE: &str = "per_track_t_gen_plus_d_play";

/// PC quality tiers, as stamped in the 32-byte header `tier` field. These are
/// the frozen S3 values (`s3_sender::pc_tier`): d8 → 2, d7 → 3, d6 → 4.
pub const PC_TIER_NORMAL: u16 = 2;
pub const PC_TIER_RECOVERY: u16 = 3;
pub const PC_TIER_CRITICAL: u16 = 4;

/// S3's haptic temporal density. `Full` is the v5 90 Hz track; `Essential`
/// keeps only the exact 90 Hz tick anchored to each 30 Hz PC frame (tick
/// `3i`), i.e. 30 Hz, which is exactly what `haptic-essential` produces in S3.
/// The header `tier` of a haptic object is `HAPTIC_TIER_FULL` in both modes —
/// S3 distinguishes density by wire track, not by tier — so `s3np` changes the
/// emitted tick set and nothing else.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HapticDensity {
    Full,
    Essential,
}

impl HapticDensity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Essential => "essential",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "full" => Some(Self::Full),
            "essential" => Some(Self::Essential),
            _ => None,
        }
    }
}

/// One replayed operating point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TierState {
    pub pc_tier: u16,
    pub haptic_density: HapticDensity,
}

/// One replayed switch: at `t_offset_us` after `measurement_start`, the
/// operating point becomes `state`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TierSwitch {
    pub t_offset_us: u64,
    pub state: TierState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleError {
    /// The document is not valid JSON or a required field has the wrong type.
    Malformed(&'static str),
    /// `schema` is not [`TIER_SCHEDULE_SCHEMA`].
    UnknownSchema,
    /// `switches` is empty, or the first switch is not at offset 0. A replay
    /// must state the operating point in force at `measurement_start`.
    MissingInitialState,
    /// Offsets must be strictly increasing.
    NonMonotonicOffset,
    /// An offset is at or beyond the replayed run length.
    OffsetOutsideRun,
    /// `pc_tier` is not 2/3/4 or `haptic_density` is not full/essential.
    UnknownTierState,
    /// Two consecutive entries request the same operating point. A no-op
    /// switch would silently change nothing while claiming a transition.
    RedundantSwitch,
    /// `duration_us` is zero.
    InvalidDuration,
}

/// A validated open-loop tier/density replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierSchedule {
    source_run_id: String,
    duration_us: u64,
    switches: Vec<TierSwitch>,
}

impl TierSchedule {
    /// Parse and fully validate a replay document.
    ///
    /// Validation is intentionally duplicated in
    /// `scripts/event_pair_tier_schedule.validate_tier_schedule`; the two must
    /// accept and reject exactly the same documents (Python/Rust parity), so
    /// the extractor cannot emit something the sender will refuse mid-batch.
    pub fn parse(document: &str) -> Result<Self, ScheduleError> {
        let value: serde_json::Value =
            serde_json::from_str(document).map_err(|_| ScheduleError::Malformed("not JSON"))?;
        let object = value
            .as_object()
            .ok_or(ScheduleError::Malformed("document is not an object"))?;
        if object.get("schema").and_then(|v| v.as_str()) != Some(TIER_SCHEDULE_SCHEMA) {
            return Err(ScheduleError::UnknownSchema);
        }
        let source_run_id = object
            .get("source_run_id")
            .and_then(|v| v.as_str())
            .ok_or(ScheduleError::Malformed("source_run_id"))?
            .to_string();
        let duration_us = object
            .get("duration_us")
            .and_then(|v| v.as_u64())
            .ok_or(ScheduleError::Malformed("duration_us"))?;
        if duration_us == 0 {
            return Err(ScheduleError::InvalidDuration);
        }
        let raw = object
            .get("switches")
            .and_then(|v| v.as_array())
            .ok_or(ScheduleError::Malformed("switches"))?;
        let mut switches: Vec<TierSwitch> = Vec::with_capacity(raw.len());
        for entry in raw {
            let entry = entry
                .as_object()
                .ok_or(ScheduleError::Malformed("switch is not an object"))?;
            let t_offset_us = entry
                .get("t_offset_us")
                .and_then(|v| v.as_u64())
                .ok_or(ScheduleError::Malformed("t_offset_us"))?;
            let pc_tier = entry
                .get("pc_tier")
                .and_then(|v| v.as_u64())
                .ok_or(ScheduleError::Malformed("pc_tier"))?;
            let pc_tier = u16::try_from(pc_tier).map_err(|_| ScheduleError::UnknownTierState)?;
            if !matches!(
                pc_tier,
                PC_TIER_NORMAL | PC_TIER_RECOVERY | PC_TIER_CRITICAL
            ) {
                return Err(ScheduleError::UnknownTierState);
            }
            let haptic_density = entry
                .get("haptic_density")
                .and_then(|v| v.as_str())
                .and_then(HapticDensity::parse)
                .ok_or(ScheduleError::UnknownTierState)?;
            let state = TierState {
                pc_tier,
                haptic_density,
            };
            if let Some(previous) = switches.last() {
                if t_offset_us <= previous.t_offset_us {
                    return Err(ScheduleError::NonMonotonicOffset);
                }
                if previous.state == state {
                    return Err(ScheduleError::RedundantSwitch);
                }
            } else if t_offset_us != 0 {
                return Err(ScheduleError::MissingInitialState);
            }
            if t_offset_us >= duration_us {
                return Err(ScheduleError::OffsetOutsideRun);
            }
            switches.push(TierSwitch {
                t_offset_us,
                state,
            });
        }
        if switches.is_empty() {
            return Err(ScheduleError::MissingInitialState);
        }
        Ok(Self {
            source_run_id,
            duration_us,
            switches,
        })
    }

    /// Operating point in force at `offset_us` after `measurement_start`.
    ///
    /// The lookup is on the **nominal slot offset** (`timestamp_us(slot,
    /// rate)`), not on wall time, so the replay is deterministic: a generation
    /// loop that sleeps until `t0 + nominal_offset` applies the new state on
    /// the first slot at or after the recorded switch time, i.e. within one
    /// source period of it.
    pub fn state_at(&self, offset_us: u64) -> TierState {
        let mut state = self.switches[0].state;
        for switch in &self.switches {
            if switch.t_offset_us > offset_us {
                break;
            }
            state = switch.state;
        }
        state
    }

    pub fn switches(&self) -> &[TierSwitch] {
        &self.switches
    }

    pub fn source_run_id(&self) -> &str {
        &self.source_run_id
    }

    pub fn duration_us(&self) -> u64 {
        self.duration_us
    }
}

/// Parameters of the pairing-free release rule. Deliberately a different type
/// from [`crate::playout::PlayoutConfig`]: `startup_timeout_us` and
/// `startup_rearm_limit` exist only to bound the search for the first exact
/// anchor **pair**, which `s3np` must not do, so they are absent rather than
/// defaulted to a value that would be recorded as if it had been applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReleaseConfig {
    pub d_play_us: u64,
    pub late_tolerance_us: u64,
    pub late_policy: LatePolicy,
    pub max_objects_per_track: usize,
    pub max_span_us: u64,
}

impl ReleaseConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.d_play_us == 0 {
            return Err("d_play_us must be > 0");
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct Identity {
    track_id: u8,
    tier: u16,
    seq: u32,
    pts_us: u64,
    event_id: u32,
}

fn identity(object: &PlayoutObject) -> Identity {
    Identity {
        track_id: object.header.track_id,
        tier: object.header.tier,
        seq: object.header.seq,
        pts_us: object.header.pts_us,
        event_id: object.header.event_id,
    }
}

/// Release order inside one track: by deadline, then by the source identity.
/// `t_gen` rises monotonically along a track's generation loop, so this equals
/// generation order; the extra key components only break ties deterministically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct QueueKey {
    release_us: u64,
    pts_us: u64,
    seq: u32,
    tier: u16,
}

/// Deterministic per-track release state machine.
///
/// Invariants:
/// * no cross-track state exists at all — the two buffers are only ever read
///   or written through [`Self::buffer`]/[`Self::buffer_mut`] for one track, so
///   one track's occupancy, lateness, or absence cannot move the other's
///   release times;
/// * every received object has a deadline immediately (`t_gen + D_play`), so
///   there is no startup window, no epoch, and no pre-epoch drop class;
/// * each exact header identity is released or dropped **once** (`terminal`);
/// * both buffers are bounded by object count and by PTS span, and by a
///   forward horizon so a corrupt far-future `t_gen` cannot pin an object in
///   the buffer past shutdown.
pub struct PerTrackReleaseScheduler {
    config: ReleaseConfig,
    pc: BTreeMap<QueueKey, PlayoutObject>,
    haptic: BTreeMap<QueueKey, PlayoutObject>,
    terminal: HashSet<Identity>,
}

impl PerTrackReleaseScheduler {
    pub fn new(config: ReleaseConfig) -> Result<Self, &'static str> {
        config.validate()?;
        Ok(Self {
            config,
            pc: BTreeMap::new(),
            haptic: BTreeMap::new(),
            terminal: HashSet::new(),
        })
    }

    pub fn config(&self) -> ReleaseConfig {
        self.config
    }

    pub fn is_empty(&self) -> bool {
        self.pc.is_empty() && self.haptic.is_empty()
    }

    pub fn buffered_counts(&self) -> (usize, usize) {
        (self.pc.len(), self.haptic.len())
    }

    /// The whole ablation difference, in one line: a per-object deadline that
    /// consults only this object's own generation time.
    pub fn release_us(&self, gen_ts_us: u64) -> u64 {
        gen_ts_us.saturating_add(self.config.d_play_us)
    }

    pub fn push(&mut self, object: PlayoutObject, now_us: u64) -> Vec<PlayoutAction> {
        if object.header.track_id != TRACK_PC && object.header.track_id != TRACK_HAPTIC {
            return vec![PlayoutAction::Drop {
                object,
                reason: "invalid_track",
                had_epoch: true,
            }];
        }
        let id = identity(&object);
        if self.terminal.contains(&id) || self.knows_buffered(id) {
            return Vec::new();
        }
        let track = object.header.track_id;
        let key = self.queue_key(&object);
        self.buffer_mut(track).insert(key, object);
        let mut actions = self.enforce_bounds(track);
        actions.extend(self.enforce_horizon(track, now_us));
        actions.extend(self.advance(now_us));
        actions
    }

    /// Emit every object whose own deadline has arrived, on either track.
    /// Iterating both tracks here is an I/O batching detail: each object's
    /// deadline was fixed by its own `t_gen`, and the per-track buffers never
    /// consult each other.
    pub fn advance(&mut self, now_us: u64) -> Vec<PlayoutAction> {
        let mut due: Vec<(u64, PlayoutObject)> = Vec::new();
        for track in [TRACK_PC, TRACK_HAPTIC] {
            let keys: Vec<QueueKey> = self
                .buffer(track)
                .keys()
                .filter(|key| key.release_us <= now_us)
                .copied()
                .collect();
            for key in keys {
                let object = self
                    .buffer_mut(track)
                    .remove(&key)
                    .expect("key came from buffer");
                due.push((key.release_us, object));
            }
        }
        due.sort_by_key(|(release_us, object)| {
            (
                *release_us,
                object.header.pts_us,
                object.header.track_id,
                object.header.seq,
            )
        });
        let mut actions = Vec::with_capacity(due.len());
        for (release_us, object) in due {
            if !self.terminal.insert(identity(&object)) {
                continue;
            }
            let too_late = now_us > release_us.saturating_add(self.config.late_tolerance_us);
            if too_late && self.config.late_policy == LatePolicy::DropLate {
                actions.push(PlayoutAction::Drop {
                    object,
                    reason: DROP_LATE,
                    had_epoch: true,
                });
            } else {
                actions.push(PlayoutAction::Release(object));
            }
        }
        actions
    }

    /// Next monotonic wakeup needed by either buffer. There is no startup
    /// timer, so an empty scheduler simply has no deadline.
    pub fn next_wakeup_us(&self) -> Option<u64> {
        self.pc
            .keys()
            .chain(self.haptic.keys())
            .map(|key| key.release_us)
            .min()
    }

    fn knows_buffered(&self, id: Identity) -> bool {
        self.pc.values().any(|item| identity(item) == id)
            || self.haptic.values().any(|item| identity(item) == id)
    }

    fn queue_key(&self, object: &PlayoutObject) -> QueueKey {
        QueueKey {
            release_us: self.release_us(object.header.gen_ts_us),
            pts_us: object.header.pts_us,
            seq: object.header.seq,
            tier: object.header.tier,
        }
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

    /// Same bound vocabulary and same victim choice (oldest first) as the
    /// S1 scheduler, so drop accounting stays comparable across arms.
    fn enforce_bounds(&mut self, track: u8) -> Vec<PlayoutAction> {
        let mut dropped = Vec::new();
        while self.buffer(track).len() > self.config.max_objects_per_track {
            if let Some(object) = self.pop_oldest(track) {
                self.terminal.insert(identity(&object));
                dropped.push(PlayoutAction::Drop {
                    object,
                    reason: DROP_BUFFER_OBJECT_LIMIT,
                    had_epoch: true,
                });
            } else {
                break;
            }
        }
        loop {
            let span = match (
                self.buffer(track).values().map(|o| o.header.pts_us).min(),
                self.buffer(track).values().map(|o| o.header.pts_us).max(),
            ) {
                (Some(first), Some(last)) => last.saturating_sub(first),
                _ => 0,
            };
            if span <= self.config.max_span_us {
                break;
            }
            if let Some(object) = self.pop_oldest(track) {
                self.terminal.insert(identity(&object));
                dropped.push(PlayoutAction::Drop {
                    object,
                    reason: DROP_BUFFER_SPAN_LIMIT,
                    had_epoch: true,
                });
            } else {
                break;
            }
        }
        dropped
    }

    /// One corrupt far-future `t_gen` would otherwise sit in the buffer forever
    /// and make the shutdown drain unbounded. Mirrors the S1 horizon.
    fn enforce_horizon(&mut self, track: u8, now_us: u64) -> Vec<PlayoutAction> {
        let latest = now_us
            .saturating_add(self.config.d_play_us)
            .saturating_add(self.config.max_span_us);
        let keys: Vec<QueueKey> = self
            .buffer(track)
            .keys()
            .filter(|key| key.release_us > latest)
            .copied()
            .collect();
        let mut dropped = Vec::with_capacity(keys.len());
        for key in keys {
            let object = self
                .buffer_mut(track)
                .remove(&key)
                .expect("key came from buffer");
            self.terminal.insert(identity(&object));
            dropped.push(PlayoutAction::Drop {
                object,
                reason: DROP_BUFFER_SPAN_LIMIT,
                had_epoch: true,
            });
        }
        dropped
    }

    fn pop_oldest(&mut self, track: u8) -> Option<PlayoutObject> {
        let key = *self.buffer(track).keys().next()?;
        self.buffer_mut(track).remove(&key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{pack_header, timestamp_us, Header, HAPTIC_TIER_FULL};
    use bytes::Bytes;

    fn document(switches: &str, duration_us: u64) -> String {
        format!(
            "{{\"schema\":\"{TIER_SCHEDULE_SCHEMA}\",\"source_run_id\":\"src\",\
             \"duration_us\":{duration_us},\"switches\":[{switches}]}}"
        )
    }

    fn entry(offset: u64, tier: u16, density: &str) -> String {
        format!(
            "{{\"t_offset_us\":{offset},\"pc_tier\":{tier},\"haptic_density\":\"{density}\"}}"
        )
    }

    fn schedule() -> TierSchedule {
        TierSchedule::parse(&document(
            &[
                entry(0, PC_TIER_NORMAL, "full"),
                entry(10_000_000, PC_TIER_CRITICAL, "essential"),
                entry(20_000_000, PC_TIER_RECOVERY, "full"),
                entry(30_000_000, PC_TIER_NORMAL, "full"),
            ]
            .join(","),
            60_000_000,
        ))
        .unwrap()
    }

    fn object(track: u8, tier: u16, seq: u32, pts_us: u64, event_id: u32, gen: u64) -> PlayoutObject {
        let bytes = pack_header(track, tier, seq, pts_us, event_id, gen, 4);
        let header = Header {
            version: bytes[0],
            track_id: track,
            tier,
            seq,
            pts_us,
            event_id,
            gen_ts_us: gen,
            payload_len: 4,
        };
        PlayoutObject {
            header,
            t_recv: gen + 1_000,
            bytes: Bytes::from_static(b"abcd"),
        }
    }

    fn config() -> ReleaseConfig {
        ReleaseConfig {
            d_play_us: 50_000,
            late_tolerance_us: 5_000,
            late_policy: LatePolicy::ReleaseLate,
            max_objects_per_track: 8,
            max_span_us: 1_000_000,
        }
    }

    // ---- tier schedule replay ----

    #[test]
    fn schedule_replay_holds_the_state_until_the_next_recorded_switch() {
        let schedule = schedule();
        assert_eq!(
            schedule.state_at(0),
            TierState {
                pc_tier: PC_TIER_NORMAL,
                haptic_density: HapticDensity::Full
            }
        );
        assert_eq!(schedule.state_at(9_999_999).pc_tier, PC_TIER_NORMAL);
        // Exactly at the switch time the new state is already in force.
        assert_eq!(schedule.state_at(10_000_000).pc_tier, PC_TIER_CRITICAL);
        assert_eq!(
            schedule.state_at(19_999_999).haptic_density,
            HapticDensity::Essential
        );
        assert_eq!(schedule.state_at(20_000_000).pc_tier, PC_TIER_RECOVERY);
        assert_eq!(
            schedule.state_at(20_000_000).haptic_density,
            HapticDensity::Full
        );
        // Past the last switch the final state persists to the run end.
        assert_eq!(schedule.state_at(59_999_999).pc_tier, PC_TIER_NORMAL);
        assert_eq!(schedule.source_run_id(), "src");
        assert_eq!(schedule.duration_us(), 60_000_000);
        assert_eq!(schedule.switches().len(), 4);
    }

    #[test]
    fn pc_switch_times_are_honoured_within_one_frame_period() {
        let schedule = schedule();
        let pc_rate_hz = 30;
        let period_us = 1_000_000 / pc_rate_hz;
        for (index, switch) in schedule.switches().iter().enumerate().skip(1) {
            let target = switch.t_offset_us;
            // The first PC slot whose nominal PTS is at or after the recorded
            // switch time is the first slot that carries the new tier.
            let boundary_slot = (0u64..)
                .find(|slot| timestamp_us(*slot, pc_rate_hz) >= target)
                .expect("a slot must reach the switch time");
            let first_new = timestamp_us(boundary_slot, pc_rate_hz);
            assert_eq!(
                schedule.state_at(first_new),
                switch.state,
                "the first slot at or after the switch must carry the new state"
            );
            assert!(
                first_new - target < period_us,
                "switch at {target} applied at {first_new}, more than one frame period late"
            );
            let previous = timestamp_us(boundary_slot - 1, pc_rate_hz);
            assert!(previous < target, "boundary slot is not the first one");
            assert_eq!(
                schedule.state_at(previous),
                schedule.switches()[index - 1].state,
                "the slot before the boundary must still carry the previous state"
            );
        }
    }

    #[test]
    fn haptic_essential_keeps_only_the_exact_anchor_ticks() {
        let schedule = schedule();
        let (pc_rate_hz, haptic_rate_hz) = (30u64, 90u64);
        let ratio = haptic_rate_hz / pc_rate_hz;
        let mut emitted = 0u64;
        let mut skipped = 0u64;
        for tick in 0..(haptic_rate_hz * 15) {
            let offset = timestamp_us(tick, haptic_rate_hz);
            let essential = schedule.state_at(offset).haptic_density == HapticDensity::Essential;
            if essential && tick % ratio != 0 {
                skipped += 1;
            } else {
                emitted += 1;
            }
        }
        // Ticks 0..1350 cover 15 s; the 10–15 s stretch is Essential, so the
        // two non-anchor ticks of each of its 150 frames are skipped.
        assert_eq!(skipped, 300);
        assert_eq!(emitted, haptic_rate_hz * 15 - 300);
    }

    /// Python/Rust parity: this is the verbatim document
    /// `scripts/event_pair_tier_schedule.py` produced from a real S3 run's
    /// TX/RX logs (`runs/phase4_v5_s3_loopback_20260731_v1`, epoch patched in
    /// for the fixture). The Rust parser must accept the extractor's exact
    /// output, including its provenance keys, which it does not interpret.
    #[test]
    fn the_python_extractor_output_parses_verbatim() {
        let document = r#"{
  "applied_switch_rows": 3,
  "duration_us": 14000000,
  "haptic_rate_hz": 90,
  "measurement_start_us": 118294901529,
  "pc_rate_hz": 30,
  "post_measurement_applies": 0,
  "pre_measurement_applies": 0,
  "redundant_applies": 0,
  "schema": "event-pair-s3np-tier-schedule-v1",
  "source_arm": "s3",
  "source_run_id": "p4v5_s3_forced_local_rep1",
  "source_rx_log_sha256": "31dbd5964127ef42307d43394accec8654a26ac5a8bee7691489e45fdfcf8fd2",
  "source_tx_log_sha256": "0af20cd3230ba5936a4b6d5ff82ecfce24a0377a0083dc4f2de9bacef2c22b94",
  "switches": [
    { "haptic_density": "full", "pc_tier": 2, "t_offset_us": 0 },
    { "haptic_density": "essential", "pc_tier": 4, "t_offset_us": 573205 },
    { "haptic_density": "full", "pc_tier": 3, "t_offset_us": 4706516 },
    { "haptic_density": "full", "pc_tier": 2, "t_offset_us": 9753871 }
  ]
}"#;
        let schedule = TierSchedule::parse(document).expect("extractor output must parse");
        assert_eq!(schedule.source_run_id(), "p4v5_s3_forced_local_rep1");
        assert_eq!(schedule.duration_us(), 14_000_000);
        assert_eq!(schedule.switches().len(), 4);
        assert_eq!(schedule.state_at(0).pc_tier, PC_TIER_NORMAL);
        assert_eq!(schedule.state_at(573_204).pc_tier, PC_TIER_NORMAL);
        assert_eq!(
            schedule.state_at(573_205),
            TierState {
                pc_tier: PC_TIER_CRITICAL,
                haptic_density: HapticDensity::Essential
            }
        );
        assert_eq!(schedule.state_at(4_706_516).pc_tier, PC_TIER_RECOVERY);
        assert_eq!(schedule.state_at(13_999_999).pc_tier, PC_TIER_NORMAL);
    }

    #[test]
    fn schedule_rejects_documents_that_cannot_be_replayed_faithfully() {
        assert_eq!(
            TierSchedule::parse("not json"),
            Err(ScheduleError::Malformed("not JSON"))
        );
        assert_eq!(
            TierSchedule::parse(
                &document(&entry(0, PC_TIER_NORMAL, "full"), 60_000_000)
                    .replace(TIER_SCHEDULE_SCHEMA, "other-v1")
            ),
            Err(ScheduleError::UnknownSchema)
        );
        assert_eq!(
            TierSchedule::parse(&document("", 60_000_000)),
            Err(ScheduleError::MissingInitialState)
        );
        assert_eq!(
            TierSchedule::parse(&document(&entry(1_000, PC_TIER_NORMAL, "full"), 60_000_000)),
            Err(ScheduleError::MissingInitialState)
        );
        assert_eq!(
            TierSchedule::parse(&document(
                &[
                    entry(0, PC_TIER_NORMAL, "full"),
                    entry(0, PC_TIER_CRITICAL, "essential"),
                ]
                .join(","),
                60_000_000
            )),
            Err(ScheduleError::NonMonotonicOffset)
        );
        assert_eq!(
            TierSchedule::parse(&document(
                &[
                    entry(0, PC_TIER_NORMAL, "full"),
                    entry(1_000, PC_TIER_NORMAL, "full"),
                ]
                .join(","),
                60_000_000
            )),
            Err(ScheduleError::RedundantSwitch)
        );
        assert_eq!(
            TierSchedule::parse(&document(&entry(0, 7, "full"), 60_000_000)),
            Err(ScheduleError::UnknownTierState)
        );
        assert_eq!(
            TierSchedule::parse(&document(&entry(0, PC_TIER_NORMAL, "sparse"), 60_000_000)),
            Err(ScheduleError::UnknownTierState)
        );
        assert_eq!(
            TierSchedule::parse(&document(
                &[
                    entry(0, PC_TIER_NORMAL, "full"),
                    entry(60_000_000, PC_TIER_CRITICAL, "essential"),
                ]
                .join(","),
                60_000_000
            )),
            Err(ScheduleError::OffsetOutsideRun)
        );
        assert_eq!(
            TierSchedule::parse(&document(&entry(0, PC_TIER_NORMAL, "full"), 0)),
            Err(ScheduleError::InvalidDuration)
        );
    }

    // ---- per-track release rule ----

    #[test]
    fn each_object_is_released_at_its_own_t_gen_plus_d_play() {
        let mut scheduler = PerTrackReleaseScheduler::new(config()).unwrap();
        let pc = object(TRACK_PC, PC_TIER_NORMAL, 0, 0, 1, 1_000_000);
        assert!(scheduler.push(pc, 1_001_000).is_empty());
        assert_eq!(scheduler.next_wakeup_us(), Some(1_050_000));
        assert!(scheduler.advance(1_049_999).is_empty());
        let actions = scheduler.advance(1_050_000);
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], PlayoutAction::Release(_)));
        assert!(scheduler.is_empty());
    }

    #[test]
    fn a_pc_object_never_waits_for_its_haptic_anchor() {
        // The S1/S2/S3 scheduler cannot release anything until one exact
        // PC/haptic anchor pair has arrived. The non-preserving control must
        // release a lone PC object on its own timeline.
        let mut scheduler = PerTrackReleaseScheduler::new(config()).unwrap();
        let pc = object(TRACK_PC, PC_TIER_NORMAL, 0, 0, 1, 2_000_000);
        let actions = scheduler.push(pc, 2_060_000);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            PlayoutAction::Release(object) => {
                assert_eq!(object.header.track_id, TRACK_PC);
                assert_eq!(object.header.event_id, 1);
            }
            other => panic!("expected a release, got {other:?}"),
        }
        assert_eq!(scheduler.buffered_counts(), (0, 0));
    }

    #[test]
    fn the_two_tracks_have_independent_timelines_and_no_deadline_coupling() {
        let mut scheduler = PerTrackReleaseScheduler::new(config()).unwrap();
        // Haptic anchor generated 40 ms after its PC frame (an extreme, but it
        // makes the independence visible): each still releases at its own
        // t_gen + D_play, so the arrival of one moves nothing about the other.
        let pc = object(TRACK_PC, PC_TIER_NORMAL, 3, 100_000, 4, 3_000_000);
        let haptic = object(TRACK_HAPTIC, HAPTIC_TIER_FULL, 9, 100_000, 4, 3_040_000);
        assert!(scheduler.push(pc, 3_001_000).is_empty());
        assert!(scheduler.push(haptic, 3_041_000).is_empty());
        let released = scheduler.advance(3_050_000);
        assert_eq!(released.len(), 1);
        assert_eq!(released[0].object().header.track_id, TRACK_PC);
        assert_eq!(scheduler.buffered_counts(), (0, 1));
        let released = scheduler.advance(3_090_000);
        assert_eq!(released.len(), 1);
        assert_eq!(released[0].object().header.track_id, TRACK_HAPTIC);
    }

    #[test]
    fn a_saturated_haptic_buffer_cannot_drop_or_delay_pc_objects() {
        let mut config = config();
        config.max_objects_per_track = 2;
        let mut scheduler = PerTrackReleaseScheduler::new(config).unwrap();
        let pc = object(TRACK_PC, PC_TIER_NORMAL, 0, 0, 1, 10_000_000);
        assert!(scheduler.push(pc, 10_000_100).is_empty());
        let mut drops = 0;
        for tick in 0..6u64 {
            let haptic = object(
                TRACK_HAPTIC,
                HAPTIC_TIER_FULL,
                tick as u32,
                tick * 11_111,
                0,
                10_000_000 + tick * 11_111,
            );
            for action in scheduler.push(haptic, 10_000_200) {
                match action {
                    PlayoutAction::Drop { object, reason, .. } => {
                        assert_eq!(object.header.track_id, TRACK_HAPTIC);
                        assert_eq!(reason, DROP_BUFFER_OBJECT_LIMIT);
                        drops += 1;
                    }
                    PlayoutAction::Release(_) => panic!("nothing is due yet"),
                }
            }
        }
        assert_eq!(drops, 4);
        // The PC object survived untouched and still releases on its own time.
        assert_eq!(scheduler.buffered_counts(), (1, 2));
        let actions = scheduler.advance(10_050_000);
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].object().header.track_id, TRACK_PC);
    }

    #[test]
    fn late_policy_and_duplicate_identities_follow_the_s1_vocabulary() {
        let mut config = config();
        config.late_policy = LatePolicy::DropLate;
        let mut scheduler = PerTrackReleaseScheduler::new(config).unwrap();
        let object_a = object(TRACK_PC, PC_TIER_NORMAL, 0, 0, 1, 1_000_000);
        // Arrives already past deadline + tolerance.
        let actions = scheduler.push(object_a.clone(), 1_060_000);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            PlayoutAction::Drop { reason, .. } => assert_eq!(*reason, DROP_LATE),
            other => panic!("expected a late drop, got {other:?}"),
        }
        // The same exact identity can never be accounted twice.
        assert!(scheduler.push(object_a, 1_070_000).is_empty());
    }

    #[test]
    fn a_corrupt_far_future_generation_time_cannot_pin_the_buffer() {
        let mut scheduler = PerTrackReleaseScheduler::new(config()).unwrap();
        let object = object(TRACK_PC, PC_TIER_NORMAL, 0, 0, 1, u64::MAX / 2);
        let actions = scheduler.push(object, 1_000_000);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            PlayoutAction::Drop { reason, .. } => assert_eq!(*reason, DROP_BUFFER_SPAN_LIMIT),
            other => panic!("expected a horizon drop, got {other:?}"),
        }
        assert!(scheduler.is_empty());
        assert_eq!(scheduler.next_wakeup_us(), None);
    }

    #[test]
    fn config_refuses_unbounded_or_zero_offset_settings() {
        for broken in [
            ReleaseConfig {
                d_play_us: 0,
                ..config()
            },
            ReleaseConfig {
                max_objects_per_track: 0,
                ..config()
            },
            ReleaseConfig {
                max_span_us: 0,
                ..config()
            },
        ] {
            assert!(PerTrackReleaseScheduler::new(broken).is_err());
        }
    }
}
