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
//!    sequence. The document carries the provenance of the S3 run it came from
//!    so an `s3np` run can be tied back to the registered S3 run of its block.
//! 2. [`PerTrackReleaseScheduler`] — the receiver's **pairing-free** release
//!    rule. There is no common epoch anchored on an exact PC/haptic anchor
//!    pair, no wait for the counterpart track, and no deadline-miss coupling
//!    between tracks.
//!
//! Measurement semantics are untouched: the sender still stamps the exact v5
//! anchor identities (`pts_us`/`event_id`, PC `i` ↔ haptic `3i`) and the
//! receiver still writes the same `role:"rx"` and `role:"release"` rows, so the
//! analyzer pairs `delta_tau = t_play_pc - t_play_haptic` afterwards exactly as
//! for S1/S2/S3. What `s3np` removes is the *system's* use of the pair, not the
//! measurement's ability to observe it.

use std::collections::{BTreeMap, HashSet};

use crate::playout::{
    LatePolicy, PlayoutAction, PlayoutObject, DROP_BUFFER_OBJECT_LIMIT, DROP_BUFFER_SPAN_LIMIT,
    DROP_LATE,
};
use crate::{Header, TRACK_HAPTIC, TRACK_PC};

/// Schema of the replay document produced by
/// `scripts/event_pair_tier_schedule.py`. Bumping this string is the only
/// sanctioned way to change the document's meaning.
pub const TIER_SCHEDULE_SCHEMA: &str = "event-pair-s3np-tier-schedule-v1";

/// Evidence generation the replay belongs to. A schedule extracted from a
/// pre-boundary S3 run must not silently feed a new-generation run
/// (`md/20260905_새실험세대_전환및_이전결과_보존등록부.md`).
pub const TIER_SCHEDULE_GENERATION: &str = "event-pair-plan-20260905-v1";

/// The v5 source rates a replay may come from. The pre-boundary generation ran
/// haptic at 100 Hz, where the 1:3 anchor rule does not hold, so its applied
/// trajectory is not replayable under the current contract.
pub const SOURCE_PC_RATE_HZ: u64 = 30;
pub const SOURCE_HAPTIC_RATE_HZ: u64 = 90;

/// PC quality tiers, as stamped in the 32-byte header `tier` field. These are
/// the frozen S3 values (`s3_sender::pc_tier`): d8 → 2, d7 → 3, d6 → 4.
pub const PC_TIER_NORMAL: u16 = 2;
pub const PC_TIER_RECOVERY: u16 = 3;
pub const PC_TIER_CRITICAL: u16 = 4;

/// Which pairing-free release rule the receiver applies.
///
/// There is deliberately **no default**. The two rules answer the same
/// ablation question with different deadline baselines, and a silent default
/// would make an `s3np` run's comparability to S3 unrecoverable from the log.
///
/// * [`ReleaseRule::PerTrackEpoch`] — the registered primary rule. Each track
///   forms its OWN epoch from the first object observed on that track, then
///   releases at `E_k + pts + D_play`. This keeps S1/S2/S3's property that the
///   deadline baseline absorbs the first object's one-way delay, while removing
///   the cross-track anchor pair that S1 needs to form its single epoch. That
///   is the intended single-component difference.
/// * [`ReleaseRule::AbsoluteTGen`] — sensitivity variant: `t_gen + D_play`, an
///   absolute deadline that does NOT absorb the one-way delay, so it is
///   systematically tighter than S1–S3's baseline. Valid only on this
///   single-host netns rig, where both namespaces read the same
///   `CLOCK_MONOTONIC` (AGENTS.md / CLAUDE.md: `D = t_recv - t_gen`). Reported
///   as a sensitivity arm, never as the primary comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseRule {
    PerTrackEpoch,
    AbsoluteTGen,
}

impl ReleaseRule {
    /// Recorded verbatim in rx metadata as `s3np_release_rule`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PerTrackEpoch => "per_track_first_object_epoch_plus_d_play",
            Self::AbsoluteTGen => "absolute_t_gen_plus_d_play",
        }
    }

    /// Which clock the release deadline is expressed on. `receiver_monotonic_us`
    /// is the same value S1 records — the per-track rule differs from S1 in its
    /// ANCHOR, not its clock — while the absolute variant hangs the deadline off
    /// the sender's `t_gen`, which is only meaningful on a shared clock.
    pub fn playout_clock(self) -> &'static str {
        match self {
            Self::PerTrackEpoch => "receiver_monotonic_us",
            Self::AbsoluteTGen => "sender_t_gen_monotonic_us",
        }
    }
}

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
    /// An offset or duration beyond `u64` lands here too, because a replay time
    /// this rig cannot represent must not be silently truncated.
    Malformed(&'static str),
    /// `schema` is not [`TIER_SCHEDULE_SCHEMA`].
    UnknownSchema,
    /// `generation` is not [`TIER_SCHEDULE_GENERATION`].
    UnknownGeneration,
    /// `source_run_id` is empty, or a source-log digest is not 64 lowercase hex
    /// characters. Without them an `s3np` run cannot be tied to the registered
    /// S3 run whose trajectory it claims to replay.
    MissingProvenance(&'static str),
    /// The source run's rates are not the v5 30/90 Hz contract.
    UnsupportedSourceRates,
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

fn hex64(value: Option<&str>, field: &'static str) -> Result<String, ScheduleError> {
    let value = value.ok_or(ScheduleError::MissingProvenance(field))?;
    if value.len() != 64 || !value.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(ScheduleError::MissingProvenance(field));
    }
    Ok(value.to_string())
}

/// A validated open-loop tier/density replay, with the provenance of the S3 run
/// it was extracted from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierSchedule {
    generation: String,
    source_run_id: String,
    source_tx_sha256: String,
    source_rx_sha256: String,
    pc_rate_hz: u64,
    haptic_rate_hz: u64,
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
    /// The shared corpus under `tests/fixtures/tier_schedule/` is checked from
    /// both languages and is the executable form of that contract.
    pub fn parse(document: &str) -> Result<Self, ScheduleError> {
        let value: serde_json::Value =
            serde_json::from_str(document).map_err(|_| ScheduleError::Malformed("not JSON"))?;
        let object = value
            .as_object()
            .ok_or(ScheduleError::Malformed("document is not an object"))?;
        if object.get("schema").and_then(|v| v.as_str()) != Some(TIER_SCHEDULE_SCHEMA) {
            return Err(ScheduleError::UnknownSchema);
        }
        if object.get("generation").and_then(|v| v.as_str()) != Some(TIER_SCHEDULE_GENERATION) {
            return Err(ScheduleError::UnknownGeneration);
        }
        let source_run_id = object
            .get("source_run_id")
            .and_then(|v| v.as_str())
            .ok_or(ScheduleError::MissingProvenance("source_run_id"))?;
        if source_run_id.is_empty() {
            return Err(ScheduleError::MissingProvenance("source_run_id"));
        }
        let source_tx_sha256 = hex64(
            object.get("source_tx_sha256").and_then(|v| v.as_str()),
            "source_tx_sha256",
        )?;
        let source_rx_sha256 = hex64(
            object.get("source_rx_sha256").and_then(|v| v.as_str()),
            "source_rx_sha256",
        )?;
        let pc_rate_hz = object
            .get("pc_rate_hz")
            .and_then(|v| v.as_u64())
            .ok_or(ScheduleError::Malformed("pc_rate_hz"))?;
        let haptic_rate_hz = object
            .get("haptic_rate_hz")
            .and_then(|v| v.as_u64())
            .ok_or(ScheduleError::Malformed("haptic_rate_hz"))?;
        if pc_rate_hz != SOURCE_PC_RATE_HZ || haptic_rate_hz != SOURCE_HAPTIC_RATE_HZ {
            return Err(ScheduleError::UnsupportedSourceRates);
        }
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
            generation: TIER_SCHEDULE_GENERATION.to_string(),
            source_run_id: source_run_id.to_string(),
            source_tx_sha256,
            source_rx_sha256,
            pc_rate_hz,
            haptic_rate_hz,
            duration_us,
            switches,
        })
    }

    /// Operating point in force at `offset_us` after `measurement_start`.
    ///
    /// The lookup is on the **nominal slot offset** (`timestamp_us(slot,
    /// rate)`), not on wall time, so the replay is deterministic: a generation
    /// loop that sleeps until `t0 + nominal_offset` applies the new state on
    /// the first slot at or after the recorded switch time.
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

    pub fn generation(&self) -> &str {
        &self.generation
    }

    pub fn source_run_id(&self) -> &str {
        &self.source_run_id
    }

    pub fn source_tx_sha256(&self) -> &str {
        &self.source_tx_sha256
    }

    pub fn source_rx_sha256(&self) -> &str {
        &self.source_rx_sha256
    }

    pub fn pc_rate_hz(&self) -> u64 {
        self.pc_rate_hz
    }

    pub fn haptic_rate_hz(&self) -> u64 {
        self.haptic_rate_hz
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
    pub rule: ReleaseRule,
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

/// One track's own timeline anchor, emitted once per track when it forms.
///
/// This cannot live on the `role:"meta"` row: metadata is written before the
/// subscription exists, and the anchor is by definition the first object
/// actually observed on that track. It is therefore recorded as its own
/// append-only row, which is additive to every existing schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrackEpoch {
    pub track_id: u8,
    /// `pts_us` of the first object observed on this track.
    pub pts_us: u64,
    /// Scheduler observation time of that object — the same notion of "now"
    /// `PlayoutScheduler` uses when it forms the common epoch from the first
    /// exact pair.
    pub monotonic_us: u64,
    /// `E_k = monotonic_us - pts_us`, the constant added to every `pts` on this
    /// track. Recorded so the applied timeline can be reconstructed from the log
    /// without re-deriving it.
    pub offset_us: u64,
}

impl TrackEpoch {
    pub fn track_name(&self) -> &'static str {
        crate::track_name(self.track_id)
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
/// The extra key components only break ties deterministically.
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
/// * no cross-track state exists at all — the buffers and the epochs are
///   indexed per track, so one track's occupancy, lateness, or absence cannot
///   move the other's release times;
/// * every received object has a deadline by the end of the `push` that
///   admitted it (under [`ReleaseRule::PerTrackEpoch`] the first object of a
///   track establishes that track's anchor in the same call), so there is no
///   startup window and no pre-epoch drop class;
/// * each exact header identity is released or dropped **once** (`terminal`);
/// * both buffers are bounded by object count and by PTS span, and by a
///   forward horizon so a corrupt far-future deadline cannot pin an object in
///   the buffer past shutdown.
pub struct PerTrackReleaseScheduler {
    config: ReleaseConfig,
    pc: BTreeMap<QueueKey, PlayoutObject>,
    haptic: BTreeMap<QueueKey, PlayoutObject>,
    pc_epoch: Option<TrackEpoch>,
    haptic_epoch: Option<TrackEpoch>,
    new_epochs: Vec<TrackEpoch>,
    terminal: HashSet<Identity>,
}

impl PerTrackReleaseScheduler {
    pub fn new(config: ReleaseConfig) -> Result<Self, &'static str> {
        config.validate()?;
        Ok(Self {
            config,
            pc: BTreeMap::new(),
            haptic: BTreeMap::new(),
            pc_epoch: None,
            haptic_epoch: None,
            new_epochs: Vec::new(),
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

    pub fn track_epoch(&self, track: u8) -> Option<TrackEpoch> {
        if track == TRACK_PC {
            self.pc_epoch
        } else {
            self.haptic_epoch
        }
    }

    /// Drain the epochs formed since the last call, for the caller to log.
    pub fn take_new_epochs(&mut self) -> Vec<TrackEpoch> {
        std::mem::take(&mut self.new_epochs)
    }

    /// The whole ablation difference, in one place: a per-object deadline that
    /// consults only this object's own track.
    pub fn release_us(&self, header: &Header) -> Option<u64> {
        match self.config.rule {
            ReleaseRule::AbsoluteTGen => {
                Some(header.gen_ts_us.saturating_add(self.config.d_play_us))
            }
            ReleaseRule::PerTrackEpoch => self.track_epoch(header.track_id).map(|epoch| {
                let base = epoch.monotonic_us.saturating_add(self.config.d_play_us);
                if header.pts_us >= epoch.pts_us {
                    base.saturating_add(header.pts_us - epoch.pts_us)
                } else {
                    base.saturating_sub(epoch.pts_us - header.pts_us)
                }
            }),
        }
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
        // Under the per-track rule the FIRST object observed on this track is
        // its anchor. Establishing it before keying the object is what makes
        // every admitted object have a deadline immediately.
        if self.config.rule == ReleaseRule::PerTrackEpoch && self.track_epoch(track).is_none() {
            let epoch = TrackEpoch {
                track_id: track,
                pts_us: object.header.pts_us,
                monotonic_us: now_us,
                offset_us: now_us.saturating_sub(object.header.pts_us),
            };
            if track == TRACK_PC {
                self.pc_epoch = Some(epoch);
            } else {
                self.haptic_epoch = Some(epoch);
            }
            self.new_epochs.push(epoch);
        }
        let release_us = self
            .release_us(&object.header)
            .expect("every admitted object has a deadline once its track anchor exists");
        let key = QueueKey {
            release_us,
            pts_us: object.header.pts_us,
            seq: object.header.seq,
            tier: object.header.tier,
        };
        self.buffer_mut(track).insert(key, object);
        let mut actions = self.enforce_bounds(track);
        actions.extend(self.enforce_horizon(track, now_us));
        actions.extend(self.advance(now_us));
        actions
    }

    /// Emit every object whose own deadline has arrived, on either track.
    /// Iterating both tracks here is an I/O batching detail: each object's
    /// deadline was fixed by its own track, and the per-track buffers never
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

    /// One corrupt far-future deadline would otherwise sit in the buffer
    /// forever and make the shutdown drain unbounded. Mirrors the S1 horizon.
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

    const TX_SHA: &str = "0af20cd3230ba5936a4b6d5ff82ecfce24a0377a0083dc4f2de9bacef2c22b94";
    const RX_SHA: &str = "31dbd5964127ef42307d43394accec8654a26ac5a8bee7691489e45fdfcf8fd2";

    fn document(switches: &str, duration_us: u64) -> String {
        format!(
            "{{\"schema\":\"{TIER_SCHEDULE_SCHEMA}\",\
             \"generation\":\"{TIER_SCHEDULE_GENERATION}\",\
             \"source_run_id\":\"src\",\
             \"source_tx_sha256\":\"{TX_SHA}\",\
             \"source_rx_sha256\":\"{RX_SHA}\",\
             \"pc_rate_hz\":{SOURCE_PC_RATE_HZ},\
             \"haptic_rate_hz\":{SOURCE_HAPTIC_RATE_HZ},\
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

    /// Registered PRIMARY rule fixture.
    fn config() -> ReleaseConfig {
        ReleaseConfig {
            rule: ReleaseRule::PerTrackEpoch,
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

    // Python/Rust parity is checked against the SHARED CORPUS in
    // `tests/fixtures/tier_schedule/` (Rust: `tests/tier_schedule_corpus.rs`,
    // Python: `scripts/test_event_pair_tier_schedule.py::CorpusTests`), which
    // includes a verbatim extractor output from a real S3 run. The tests here
    // cover the individual rules; the corpus covers the contract.

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
    #[test]
    fn schedule_requires_the_provenance_that_ties_it_to_a_registered_s3_run() {
        let schedule = schedule();
        assert_eq!(schedule.generation(), TIER_SCHEDULE_GENERATION);
        assert_eq!(schedule.source_run_id(), "src");
        assert_eq!(schedule.source_tx_sha256(), TX_SHA);
        assert_eq!(schedule.source_rx_sha256(), RX_SHA);
        assert_eq!(schedule.pc_rate_hz(), SOURCE_PC_RATE_HZ);
        assert_eq!(schedule.haptic_rate_hz(), SOURCE_HAPTIC_RATE_HZ);

        let valid = document(&entry(0, PC_TIER_NORMAL, "full"), 60_000_000);
        // Generation marker: a pre-boundary schedule must not feed a new run.
        assert_eq!(
            TierSchedule::parse(&valid.replace(TIER_SCHEDULE_GENERATION, "older-generation")),
            Err(ScheduleError::UnknownGeneration)
        );
        // Empty run id is refused exactly as the Python validator refuses it.
        assert_eq!(
            TierSchedule::parse(&valid.replace("\"source_run_id\":\"src\"", "\"source_run_id\":\"\"")),
            Err(ScheduleError::MissingProvenance("source_run_id"))
        );
        for (field, replaced) in [
            ("source_tx_sha256", TX_SHA),
            ("source_rx_sha256", RX_SHA),
        ] {
            // Absent.
            let without = valid.replace(&format!("\"{field}\":\"{replaced}\","), "");
            assert_eq!(
                TierSchedule::parse(&without),
                Err(ScheduleError::MissingProvenance(field)),
                "{field} must be required"
            );
            // Not 64 lowercase hex.
            for bad in ["", "deadbeef", &replaced.to_uppercase(), &"z".repeat(64)] {
                assert_eq!(
                    TierSchedule::parse(&valid.replace(replaced, bad)),
                    Err(ScheduleError::MissingProvenance(field)),
                    "{field} must be 64 lowercase hex, rejected {bad:?}"
                );
                break; // the replace above would hit both digests; one case each
            }
        }
        assert_eq!(
            TierSchedule::parse(&valid.replace(TX_SHA, "deadbeef")),
            Err(ScheduleError::MissingProvenance("source_tx_sha256"))
        );
        assert_eq!(
            TierSchedule::parse(&valid.replace(RX_SHA, &RX_SHA.to_uppercase())),
            Err(ScheduleError::MissingProvenance("source_rx_sha256"))
        );
        // Pre-boundary 100 Hz haptic sources are not replayable: the 1:3 anchor
        // rule this arm depends on does not hold there.
        assert_eq!(
            TierSchedule::parse(&valid.replace("\"haptic_rate_hz\":90", "\"haptic_rate_hz\":100")),
            Err(ScheduleError::UnsupportedSourceRates)
        );
        assert_eq!(
            TierSchedule::parse(&valid.replace("\"pc_rate_hz\":30", "\"pc_rate_hz\":25")),
            Err(ScheduleError::UnsupportedSourceRates)
        );
        // A replay time this rig cannot represent is refused, not truncated.
        assert_eq!(
            TierSchedule::parse(&valid.replace("\"t_offset_us\":0", "\"t_offset_us\":18446744073709551616")),
            Err(ScheduleError::Malformed("t_offset_us"))
        );
        assert_eq!(
            TierSchedule::parse(&valid.replace("\"duration_us\":60000000", "\"duration_us\":18446744073709551616")),
            Err(ScheduleError::Malformed("duration_us"))
        );
    }

    // ---- release rules ----

    fn absolute_config() -> ReleaseConfig {
        ReleaseConfig {
            rule: ReleaseRule::AbsoluteTGen,
            ..config()
        }
    }

    #[test]
    fn the_two_rules_have_distinct_recorded_names_and_clocks() {
        assert_eq!(
            ReleaseRule::PerTrackEpoch.as_str(),
            "per_track_first_object_epoch_plus_d_play"
        );
        assert_eq!(
            ReleaseRule::AbsoluteTGen.as_str(),
            "absolute_t_gen_plus_d_play"
        );
        assert_ne!(ReleaseRule::PerTrackEpoch.as_str(), ReleaseRule::AbsoluteTGen.as_str());
        // The per-track rule shares S1's clock (it differs in its ANCHOR, not
        // its clock); the absolute variant hangs off the sender's t_gen.
        assert_eq!(ReleaseRule::PerTrackEpoch.playout_clock(), "receiver_monotonic_us");
        assert_eq!(
            ReleaseRule::AbsoluteTGen.playout_clock(),
            "sender_t_gen_monotonic_us"
        );
    }

    #[test]
    fn per_track_epoch_anchors_on_the_first_object_of_that_track() {
        let mut scheduler = PerTrackReleaseScheduler::new(config()).unwrap();
        // First PC object: pts 0, observed at 1_020_000 (20 ms one-way delay on
        // top of a t_gen of 1_000_000). E_pc = 1_020_000.
        let first = object(TRACK_PC, PC_TIER_NORMAL, 0, 0, 1, 1_000_000);
        assert!(scheduler.push(first, 1_020_000).is_empty());
        let epoch = scheduler.track_epoch(TRACK_PC).unwrap();
        assert_eq!(epoch.pts_us, 0);
        assert_eq!(epoch.monotonic_us, 1_020_000);
        assert_eq!(epoch.offset_us, 1_020_000);
        assert_eq!(epoch.track_name(), "pc");
        // The baseline ABSORBS that 20 ms, exactly as the S1 common epoch does,
        // so the deadline is observation + D_play, not t_gen + D_play.
        assert_eq!(scheduler.next_wakeup_us(), Some(1_070_000));

        // A later frame is due one source period after the first.
        let second = object(TRACK_PC, PC_TIER_NORMAL, 1, 33_333, 2, 1_033_333);
        assert!(scheduler.push(second, 1_053_000).is_empty());
        assert_eq!(scheduler.buffered_counts(), (2, 0));
        let released = scheduler.advance(1_070_000);
        assert_eq!(released.len(), 1);
        assert_eq!(released[0].object().header.seq, 0);
        let released = scheduler.advance(1_103_333);
        assert_eq!(released.len(), 1);
        assert_eq!(released[0].object().header.seq, 1);

        // The haptic track forms its OWN anchor, from its own first object.
        let haptic = object(TRACK_HAPTIC, HAPTIC_TIER_FULL, 9, 100_000, 4, 2_000_000);
        assert!(scheduler.push(haptic, 2_005_000).is_empty());
        let haptic_epoch = scheduler.track_epoch(TRACK_HAPTIC).unwrap();
        assert_eq!(haptic_epoch.monotonic_us, 2_005_000);
        assert_eq!(haptic_epoch.pts_us, 100_000);
        assert_ne!(haptic_epoch.offset_us, epoch.offset_us);
        assert_eq!(scheduler.next_wakeup_us(), Some(2_055_000));
    }

    #[test]
    fn epochs_are_reported_once_per_track_for_the_log() {
        let mut scheduler = PerTrackReleaseScheduler::new(config()).unwrap();
        assert!(scheduler.take_new_epochs().is_empty());
        scheduler.push(object(TRACK_PC, PC_TIER_NORMAL, 0, 0, 1, 1_000_000), 1_010_000);
        let formed = scheduler.take_new_epochs();
        assert_eq!(formed.len(), 1);
        assert_eq!(formed[0].track_id, TRACK_PC);
        // Draining is idempotent, and a second object on the same track does
        // not re-anchor it.
        assert!(scheduler.take_new_epochs().is_empty());
        scheduler.push(
            object(TRACK_PC, PC_TIER_NORMAL, 1, 33_333, 2, 1_033_333),
            1_043_000,
        );
        assert!(scheduler.take_new_epochs().is_empty());
        assert_eq!(scheduler.track_epoch(TRACK_PC).unwrap().monotonic_us, 1_010_000);
        scheduler.push(
            object(TRACK_HAPTIC, HAPTIC_TIER_FULL, 0, 0, 1, 1_000_000),
            1_011_000,
        );
        let formed = scheduler.take_new_epochs();
        assert_eq!(formed.len(), 1);
        assert_eq!(formed[0].track_id, TRACK_HAPTIC);
    }

    #[test]
    fn the_absolute_variant_releases_at_t_gen_plus_d_play_and_forms_no_epoch() {
        let mut scheduler = PerTrackReleaseScheduler::new(absolute_config()).unwrap();
        let pc = object(TRACK_PC, PC_TIER_NORMAL, 0, 0, 1, 1_000_000);
        assert!(scheduler.push(pc, 1_020_000).is_empty());
        assert!(scheduler.track_epoch(TRACK_PC).is_none());
        assert!(scheduler.take_new_epochs().is_empty());
        // The one-way delay is NOT absorbed: the deadline is 50 ms after t_gen.
        assert_eq!(scheduler.next_wakeup_us(), Some(1_050_000));
        assert!(scheduler.advance(1_049_999).is_empty());
        let actions = scheduler.advance(1_050_000);
        assert_eq!(actions.len(), 1);
        assert!(matches!(actions[0], PlayoutAction::Release(_)));
        assert!(scheduler.is_empty());
    }

    #[test]
    fn the_absolute_variant_is_the_tighter_baseline_for_the_same_object() {
        // This is the whole reason it is a sensitivity variant rather than the
        // primary rule: for one and the same object, its deadline is earlier by
        // the observed one-way delay.
        let object = object(TRACK_PC, PC_TIER_NORMAL, 0, 0, 1, 1_000_000);
        let mut epoch_rule = PerTrackReleaseScheduler::new(config()).unwrap();
        let mut absolute_rule = PerTrackReleaseScheduler::new(absolute_config()).unwrap();
        epoch_rule.push(object.clone(), 1_030_000);
        absolute_rule.push(object, 1_030_000);
        let epoch_due = epoch_rule.next_wakeup_us().unwrap();
        let absolute_due = absolute_rule.next_wakeup_us().unwrap();
        assert_eq!(epoch_due - absolute_due, 30_000);
    }

    #[test]
    fn a_pc_object_never_waits_for_its_haptic_anchor_under_either_rule() {
        // The S1/S2/S3 scheduler cannot release anything until one exact
        // PC/haptic anchor pair has arrived. The non-preserving control must
        // release a lone PC object on its own timeline under both rules.
        for rule in [ReleaseRule::PerTrackEpoch, ReleaseRule::AbsoluteTGen] {
            let mut scheduler = PerTrackReleaseScheduler::new(ReleaseConfig {
                rule,
                ..config()
            })
            .unwrap();
            let pc = object(TRACK_PC, PC_TIER_NORMAL, 0, 0, 1, 2_000_000);
            // Observed 60 ms late, so its deadline has passed under both rules.
            let actions = scheduler.push(pc, 2_000_000);
            let actions = if actions.is_empty() {
                scheduler.advance(2_060_000)
            } else {
                actions
            };
            assert_eq!(actions.len(), 1, "{rule:?}");
            match &actions[0] {
                PlayoutAction::Release(object) => {
                    assert_eq!(object.header.track_id, TRACK_PC);
                    assert_eq!(object.header.event_id, 1);
                }
                other => panic!("expected a release under {rule:?}, got {other:?}"),
            }
            assert_eq!(scheduler.buffered_counts(), (0, 0), "{rule:?}");
            assert!(scheduler.track_epoch(TRACK_HAPTIC).is_none(), "{rule:?}");
        }
    }

    #[test]
    fn the_two_tracks_have_independent_timelines_and_no_deadline_coupling() {
        let mut scheduler = PerTrackReleaseScheduler::new(config()).unwrap();
        // Same anchor PTS on both tracks, but the haptic object is observed
        // 40 ms later. Each track anchors on its own first object, so the two
        // deadlines differ by that 40 ms and neither moves the other.
        let pc = object(TRACK_PC, PC_TIER_NORMAL, 3, 100_000, 4, 3_000_000);
        let haptic = object(TRACK_HAPTIC, HAPTIC_TIER_FULL, 9, 100_000, 4, 3_000_000);
        assert!(scheduler.push(pc, 3_001_000).is_empty());
        assert!(scheduler.push(haptic, 3_041_000).is_empty());
        assert_eq!(scheduler.buffered_counts(), (1, 1));
        let released = scheduler.advance(3_051_000);
        assert_eq!(released.len(), 1);
        assert_eq!(released[0].object().header.track_id, TRACK_PC);
        assert_eq!(scheduler.buffered_counts(), (0, 1));
        let released = scheduler.advance(3_091_000);
        assert_eq!(released.len(), 1);
        assert_eq!(released[0].object().header.track_id, TRACK_HAPTIC);
    }

    #[test]
    fn a_saturated_haptic_buffer_cannot_drop_or_delay_pc_objects() {
        for rule in [ReleaseRule::PerTrackEpoch, ReleaseRule::AbsoluteTGen] {
            let mut scheduler = PerTrackReleaseScheduler::new(ReleaseConfig {
                rule,
                max_objects_per_track: 2,
                ..config()
            })
            .unwrap();
            let pc = object(TRACK_PC, PC_TIER_NORMAL, 0, 0, 1, 10_000_000);
            assert!(scheduler.push(pc, 10_000_100).is_empty(), "{rule:?}");
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
                        PlayoutAction::Release(_) => panic!("nothing is due yet under {rule:?}"),
                    }
                }
            }
            assert_eq!(drops, 4, "{rule:?}");
            // The PC object survived untouched and still releases on its own
            // time: under both rules its deadline is ~10_050_000, while the
            // surviving haptic ticks are not due until after 10_094_000.
            assert_eq!(scheduler.buffered_counts(), (1, 2), "{rule:?}");
            let actions = scheduler.advance(10_060_000);
            assert_eq!(actions.len(), 1, "{rule:?}");
            assert_eq!(actions[0].object().header.track_id, TRACK_PC, "{rule:?}");
            assert_eq!(scheduler.buffered_counts(), (0, 2), "{rule:?}");
        }
    }

    #[test]
    fn late_policy_and_duplicate_identities_follow_the_s1_vocabulary() {
        for rule in [ReleaseRule::PerTrackEpoch, ReleaseRule::AbsoluteTGen] {
            let mut scheduler = PerTrackReleaseScheduler::new(ReleaseConfig {
                rule,
                late_policy: LatePolicy::DropLate,
                ..config()
            })
            .unwrap();
            let object_a = object(TRACK_PC, PC_TIER_NORMAL, 0, 0, 1, 1_000_000);
            // Admitted, then advanced well past deadline + tolerance.
            let admitted = scheduler.push(object_a.clone(), 1_000_000);
            assert!(admitted.is_empty(), "{rule:?}");
            let actions = scheduler.advance(1_200_000);
            assert_eq!(actions.len(), 1, "{rule:?}");
            match &actions[0] {
                PlayoutAction::Drop { reason, .. } => assert_eq!(*reason, DROP_LATE),
                other => panic!("expected a late drop under {rule:?}, got {other:?}"),
            }
            // The same exact identity can never be accounted twice.
            assert!(scheduler.push(object_a, 1_300_000).is_empty(), "{rule:?}");
        }
    }

    #[test]
    fn a_corrupt_far_future_deadline_cannot_pin_the_buffer_under_either_rule() {
        // Absolute rule: a corrupt t_gen. Per-track rule: a corrupt PTS far
        // beyond the track's anchor. Both must leave through the horizon bound.
        let mut absolute = PerTrackReleaseScheduler::new(absolute_config()).unwrap();
        let broken = object(TRACK_PC, PC_TIER_NORMAL, 0, 0, 1, u64::MAX / 2);
        let actions = absolute.push(broken, 1_000_000);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            PlayoutAction::Drop { reason, .. } => assert_eq!(*reason, DROP_BUFFER_SPAN_LIMIT),
            other => panic!("expected a horizon drop, got {other:?}"),
        }
        assert!(absolute.is_empty());
        assert_eq!(absolute.next_wakeup_us(), None);

        let mut per_track = PerTrackReleaseScheduler::new(config()).unwrap();
        assert!(per_track
            .push(object(TRACK_PC, PC_TIER_NORMAL, 0, 0, 1, 1_000_000), 1_010_000)
            .is_empty());
        let far = object(TRACK_PC, PC_TIER_NORMAL, 9, u64::MAX / 2, 10, 1_100_000);
        let actions = per_track.push(far, 1_110_000);
        // Both objects leave, with the SAME drop reason S1 emits, and in the
        // same order S1 emits it: `enforce_bounds` runs before the horizon and
        // evicts the OLDEST entry to bring the PTS span back under bound, so the
        // healthy object goes first and the corrupt one leaves through the
        // horizon. That is byte-for-byte the S1 buffer policy — `s3np` must not
        // quietly improve on it, or the drop accounting stops being comparable.
        assert_eq!(actions.len(), 2);
        for action in &actions {
            match action {
                PlayoutAction::Drop { reason, .. } => {
                    assert_eq!(*reason, DROP_BUFFER_SPAN_LIMIT)
                }
                other => panic!("expected two bound drops, got {other:?}"),
            }
        }
        assert!(per_track.is_empty());
        assert_eq!(per_track.next_wakeup_us(), None);
        // The anchor itself is unaffected by the eviction: it is a property of
        // the first object OBSERVED, not of the buffer contents.
        assert_eq!(per_track.track_epoch(TRACK_PC).unwrap().monotonic_us, 1_010_000);
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
