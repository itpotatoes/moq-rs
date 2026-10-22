//! L2 output stage: milestone M1 (device-free ledger), M2 (per-modality
//! sinks, schema v1.2; haptic ALSA) and M3 (PC Wayland sink, schema v1.4).
//!
//! Governing documents: `md/20261022_실험1D_L2사다리_사전등록_문안.md` §3-1,
//! §3-3, §3-3a, §3-5, §3-6, §6, §6-1, §11-7; `md/20261022_L2_계측_설계초안.md`
//! §2, §4, §7, §8; contract `md/20260929_실험2_계약.md` §5–§6 and §9 (row
//! roles `submit` / `output` / `explicit_discard`, `t_submit_us`,
//! `t_output_us`, `t_discard_us`, `t_log_arrival_us` = writer clock
//! immediately before the row is written, `instrumentation_end.max_write_lag_us`,
//! final `instrumentation_end`).
//!
//! Shape (one LANE per modality since v1.2; each lane has its own bounded
//! submission queue and its own sink thread, so one modality's sink can never
//! delay the other's):
//!
//! ```text
//!  release point (scheduler / B1 drain)       lane thread (per modality)        writer thread (owns the file)
//!  L2Submitter::submit() --try_send(Item)--> [bounded queue: pc]     --> sink(pc)     ─┐
//!                        --try_send(Item)--> [bounded queue: haptic] --> sink(haptic) ─┤ Submit / Output / Discard
//!     │ (Full)                                                                         │
//!     └─try_send(Discard before submit)───────────────────────────────────────────>  [bounded row queue] --> L2 JSONL
//!  sink kinds: null (submit only), fake (synthetic output), alsa (haptic, real PCM device; see `l2_haptic`),
//!              wayland (PC: render worker + commit thread, wp_presentation feedback; see `l2_pc`, `pc_lane`)
//!  receiver_fault(t, cause): earliest kept in shared state, written before the sentinel
//! ```
//!
//! * The release side only ever calls `try_send` (no file I/O, no JSON, no
//!   logger mutex). A full submission queue is recorded as
//!   `explicit_discard(output_queue_full)` BEFORE submit; it never becomes an
//!   error of the release path.
//! * Every object row carries the ORIGINAL identity of the released object
//!   (run_id, track, tier, seq, pts_us, event_id and, for S3, wire_track +
//!   route_generation). Nothing is rewritten from a ledger.
//! * Row times (also stated in the meta row, `row_time_semantics`): on object
//!   rows and the sentinel `t_us == t_log_arrival_us` = writer clock
//!   immediately before the write; on the `receiver_fault` row `t_us` = the
//!   moment the receiver recorded its first fatal error and
//!   `t_log_arrival_us` = write time. `t_callback_us` = the sink's receipt of
//!   the terminal callback. `t_us` is therefore NOT monotonic in file order;
//!   use `t_log_arrival_us` for write order.
//! * Integrity failures carry their OCCURRENCE time (registration §6-1;
//!   `failure_time_semantics` in meta): duplicate release -> the second
//!   release's `t_release_us`; queue/row/tracking overflow -> the offending
//!   attempt; unresolved -> the earliest `t_submit_us` among unresolved
//!   submits; orphan terminal -> its own receipt time; sink gone / producers
//!   alive at finish / submit after close -> when detected (= when attempted).
//!   A failure whose time cannot be determined makes
//!   `first_failure_time_determinable` false and `first_integrity_failure_us`
//!   null. Since v1.3 every ALSA device failure is such a failure until the
//!   kernel timestamps are verified on the device (registration §6-1): its
//!   observation interval, detection time and raw kernel trigger are kept
//!   apart in the sentinel's `lanes.haptic.device_failure`.
//! * The close grace (`report_window_us`, meta `report_window_anchor:
//!   "close"`) starts when `finish` begins; the deadline is published in
//!   shared state BEFORE anything else in `finish`, so a terminal is accepted
//!   only if its receipt time is strictly before it in every sink phase. It is
//!   NOT the registration's H-anchored `G_report` (§3-6). Since v1.2 the
//!   sentinel records `finish_start_us` and `close_deadline_us` (= finish
//!   start + grace) so a checker can verify every terminal receipt time
//!   (`t_callback_us`, or `t_discard_us` of an after-submit discard) is
//!   strictly `< close_deadline_us`; the writer cross-checks the same rule.
//! * Order rule (registration §3-3): on every `output` row
//!   `t_release_us <= t_submit_us <= t_output_us <= t_callback_us <=
//!   t_log_arrival_us`. The writer checks it AFTER writing the row (raw values
//!   are never clamped or rewritten); a violation is the integrity failure
//!   `output_order_violation`, occurring at the row's `t_callback_us`.
//! * `instrumentation_end` is written only by [`L2Output::finish`]: after the
//!   submission side is closed, every producer handle is gone (otherwise
//!   `producers_alive_at_finish`), the close grace has run and every lane
//!   thread has been joined (a device sink has closed its device). It is the
//!   last row because the writer exits right after writing it. A run that
//!   ends without `finish` has no sentinel — fail closed.
//! * All times are integer microseconds of `CLOCK_MONOTONIC` ([`crate::now_us`]).
//!
//! Sink kinds (meta `sinks` / `sink_contracts`, one per modality):
//! `null` accepts submissions and never emits a terminal: it exists only to
//! measure the overhead of the submission path; its submissions are
//! unresolved by design (contract `resolves: false`), which is not an
//! integrity failure FOR THAT MODALITY. `fake` emits a deterministic synthetic
//! `output` with `t_output_us = t_submit_us + offset(modality)`; it handles
//! the deferred output at a real clock read `t_callback_us >= t_output_us`.
//! Neither is a device output. `alsa` (haptic only) plays the released PCM
//! on the device (`l2_alsa`, `hw:1,0` by default) or on the simulated stream
//! (`l2_haptic::SimPcm`, tests only, never a device output) through the
//! placement engine (`l2_haptic`); its lane thread owns the stream, opens,
//! prefills and starts it BEFORE the meta row is written (which records what
//! was negotiated), emits an `output` only when a status snapshot shows the
//! hardware passed the tick's first frame, terminates everything it can no
//! longer output after a device failure as `explicit_discard(device_failure)`
//! and reports its counters in the sentinel (`lanes.haptic`). `wayland` (PC
//! only, M3) renders every released frame (Draco decoded first) in a
//! worker thread into a one-frame pending slot and commits it from the
//! compositor thread at the next commit opportunity; the
//! `wp_presentation_feedback` of that commit is the frame's terminal
//! (`presented` -> `output`, `discarded` -> `compositor_discarded`); a
//! pending frame replaced before its commit is `superseded_before_commit`;
//! its counters go to `lanes.pc` (module `pc_lane`). The file-level
//! `device_output` is true only if EVERY modality's sink is a device sink, so
//! a file mixing a device sink with a null/fake one is test-only and fails
//! `--require-device-output`.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Barrier, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::{bail, Context, Result};

use crate::l2_haptic::{
    Admission, AlsaOutputDetail, DeviceFailure, EpochStart, HapticPcm, HapticPlacement, PcmParams,
    PlacementConfig, RawSnapshot, SimPcm, SimPcmConfig, FAIL_HAPTIC_PAYLOAD_INVALID,
};
use crate::now_us;

/// The PC Wayland lane (render worker + commit thread), M3.
mod pc_lane;

/// L2 JSONL schema tag written into the meta row.
/// v1 (Codex 104): writer-side `t_log_arrival_us`, `t_callback_us`,
/// conservation/duplicate/failure fields. v1.1 (Codex 105):
/// `first_integrity_failure_us` is the OCCURRENCE time (plus
/// `_detected_us` and `first_failure_time_determinable`), the close deadline
/// binds every sink phase, the earliest `receiver_fault` is kept, and the
/// meta row states the row/failure time semantics. v1.2 (M2, Codex 108 P2 /
/// 1-D §11-7 M2 work order (1)(2)): one sink per modality (meta `sinks`,
/// `sink_contracts` with `device_output`, `resolves` and the `output_detail`
/// schema of each sink kind), file-level `device_output` = every modality is
/// a device sink, mandatory `counter_semantics`, the order rule checked by
/// the writer, and the sentinel's `finish_start_us` / `close_deadline_us`.
/// v1.3 (Codex 109; registration 3-3a, 6-1, 11-11): fields REMOVED from the
/// L2 JSONL — every raw `*_ns` value, the non-CLOCK_MONOTONIC audio
/// timestamp and the audio-timestamp accuracy (the haptic `output_detail`
/// and meta `alsa.stream_epoch` keep only integer µs CLOCK_MONOTONIC times;
/// the raw values live only in a probe-only diagnostics file) — device
/// failures carry NO occurrence time (undeterminable; observation interval,
/// detection and raw kernel trigger kept apart in `lanes.haptic`), `Q`
/// counts the unwritten remainder of the tick being written, the close waits
/// for the END of the last tick, and meta `alsa.audio_tstamp_type` records
/// requested vs actual. Files written under an older tag are never
/// re-labelled.
/// v1.4 (M3, registration 3-2, 3-3b, 11-8): the PC sink kind `wayland`
/// (contract `device_output` true for the display backend), meta `wayland`
/// (startup presentation, clock id, surface/canvas, buffers, tiers meta
/// digest and scales, the Draco decoder identity, the renderer), PC output
/// rows with `output_detail` {commit_id, buffer, render/commit times, MSC,
/// flags, refresh_us} and, for Draco frames only, top-level
/// `t_decode_start_us`/`t_decode_end_us` on every after-submit terminal row,
/// the extended order rule for PC rows (submit <= decode <= render <= commit
/// <= output), the discard reasons `superseded_before_commit`,
/// `compositor_discarded`, `decode_failed`, PC integrity failures
/// (`pc_*` timed, `wayland_*` device failures untimed in `lanes.pc`) and
/// the sentinel's `lanes.pc`. µs-only time fields as before (raw ns only in
/// the probe's diagnostics file).
pub const L2_SCHEMA: &str = "event-pair-l2-v1.4";
pub const L2_CLOCK: &str = "CLOCK_MONOTONIC";
/// The M1 grace is anchored at `finish`, not at the registration's H.
pub const REPORT_WINDOW_ANCHOR: &str = "close";

pub const DISCARD_OUTPUT_QUEUE_FULL: &str = "output_queue_full";
/// Reserved for the PC renderer (M3): a not-yet-committed frame replaced by a
/// newer one. Never used by the M1 sinks.
pub const DISCARD_SUPERSEDED_BEFORE_COMMIT: &str = "superseded_before_commit";
/// Reserved for the PC renderer (M3): `wp_presentation_feedback.discarded`.
pub const DISCARD_COMPOSITOR_DISCARDED: &str = "compositor_discarded";
/// Haptic ALSA lane, after submit: the device failed (XRUN, suspend, epoch
/// reset, error) — every submission it can no longer output. The failure
/// itself is the integrity failure; these rows keep the ledger complete.
pub const DISCARD_DEVICE_FAILURE: &str = "device_failure";
/// After submit: the payload is not a playable PCM tick (haptic ALSA lane)
/// or not a drawable PC frame of a known tier (PC Wayland lane).
pub const DISCARD_PAYLOAD_INVALID: &str = "payload_invalid";
/// PC Wayland lane, after submit: the Draco decoder refused the payload.
pub use crate::l2_pc::DISCARD_DECODE_FAILED;

/// `first_integrity_failure_reason` values.
pub const FAIL_ROW_QUEUE_OVERFLOW: &str = "row_queue_overflow";
pub const FAIL_SUBMIT_AFTER_CLOSE: &str = "submit_after_close";
pub const FAIL_SINK_GONE: &str = "sink_gone";
pub const FAIL_DUPLICATE_LOGICAL_RELEASE: &str = "duplicate_logical_release";
pub const FAIL_TERMINAL_WITHOUT_OPEN_SUBMIT: &str = "terminal_without_open_submit";
pub const FAIL_TRACKING_CAPACITY: &str = "tracking_capacity_exceeded";
pub const FAIL_FAKE_PENDING_OVERFLOW: &str = "fake_pending_overflow";
pub const FAIL_PRODUCERS_ALIVE_AT_FINISH: &str = "producers_alive_at_finish";
pub const FAIL_SUBMITTED_UNRESOLVED: &str = "submitted_unresolved";
pub const FAIL_CONSERVATION: &str = "submit_conservation_violated";
/// v1.2: an `output` row violating `t_release <= t_submit <= t_output <=
/// t_callback <= t_log_arrival`.
pub const FAIL_OUTPUT_ORDER: &str = "output_order_violation";
/// v1.2: a terminal whose receipt time is not strictly before the published
/// close deadline (writer cross-check of the sinks' own refusal).
pub const FAIL_TERMINAL_AT_OR_AFTER_DEADLINE: &str = "terminal_at_or_after_deadline";

/// `close_deadline_us` states besides a real deadline.
const CLOSE_OPEN: u64 = 0;
/// `finish` has started but has not read its clock yet.
const CLOSE_PENDING: u64 = u64::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum L2SinkKind {
    Null,
    Fake,
    /// Haptic only: PCM playback with ALSA-reported output times
    /// (registration §3-3, §3-3a; `l2_haptic`).
    Alsa,
    /// PC only: rendered frames presented through Wayland with
    /// `wp_presentation` output times (registration §3-2, §3-3b; `l2_pc`).
    Wayland,
}

impl L2SinkKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Fake => "fake",
            Self::Alsa => "alsa",
            Self::Wayland => "wayland",
        }
    }

    /// Whether the sink ever emits a terminal (a null sink never does).
    pub fn resolves(self) -> bool {
        self != Self::Null
    }
}

/// The sink of each modality (v1.2). `pc` and `haptic` are independent:
/// e.g. haptic on the ALSA device while PC stays fake (M3 adds Wayland).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct L2Sinks {
    pub pc: L2SinkKind,
    pub haptic: L2SinkKind,
}

impl L2Sinks {
    /// The same sink kind for both modalities.
    pub fn both(kind: L2SinkKind) -> Self {
        Self { pc: kind, haptic: kind }
    }

    pub fn of(&self, modality: L2Modality) -> L2SinkKind {
        match modality {
            L2Modality::Pc => self.pc,
            L2Modality::Haptic => self.haptic,
        }
    }

    /// `pc=<kind>,haptic=<kind>`, the CLI spelling.
    pub fn spec(&self) -> String {
        format!("pc={},haptic={}", self.pc.as_str(), self.haptic.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum L2Modality {
    Pc,
    Haptic,
}

impl L2Modality {
    /// Both modalities, in lane order (`index`).
    pub const ALL: [L2Modality; 2] = [L2Modality::Pc, L2Modality::Haptic];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pc => "pc",
            Self::Haptic => "haptic",
        }
    }

    /// Lane index: PC 0, haptic 1.
    pub fn index(self) -> usize {
        match self {
            Self::Pc => 0,
            Self::Haptic => 1,
        }
    }

    /// Map the logical track name used in the rx log (`"pc"`/`"haptic"`).
    pub fn from_track(track: &str) -> Option<Self> {
        match track {
            "pc" => Some(Self::Pc),
            "haptic" => Some(Self::Haptic),
            _ => None,
        }
    }
}

/// The original identity of one released object, copied from the release
/// point. `route` is `Some((wire_track, route_generation))` only for S3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct L2Identity {
    pub modality: L2Modality,
    pub tier: u16,
    pub seq: u32,
    pub pts_us: u64,
    pub event_id: u32,
    pub route: Option<(&'static str, u64)>,
}

impl L2Identity {
    /// The logical event, independent of tier and route: two releases with
    /// the same key are two outputs of one planned opportunity.
    fn logical(&self) -> (L2Modality, u32) {
        (self.modality, self.seq)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct L2Submission {
    pub identity: L2Identity,
    pub t_release_us: u64,
    pub t_submit_us: u64,
}

#[derive(Debug, Clone)]
pub struct FakeSinkConfig {
    pub pc_offset_us: u64,
    pub haptic_offset_us: u64,
    /// Negative test: every submission is refused as if the queue were full.
    pub force_queue_full: bool,
    /// Negative test: submissions are accepted (submit row) but no terminal is
    /// ever emitted, so each one is `submitted_unresolved`.
    pub drop_terminals: bool,
    /// Test-only: the sink thread waits on this barrier before consuming its
    /// queue, so a test can fill the REAL bounded queue deterministically.
    /// Every fake lane waits on it, so use it with exactly one fake lane.
    pub start_gate: Option<Arc<Barrier>>,
}

impl FakeSinkConfig {
    pub fn offsets(pc_offset_us: u64, haptic_offset_us: u64) -> Self {
        Self {
            pc_offset_us,
            haptic_offset_us,
            force_queue_full: false,
            drop_terminals: false,
            start_gate: None,
        }
    }

    fn offset_us(&self, modality: L2Modality) -> u64 {
        match modality {
            L2Modality::Pc => self.pc_offset_us,
            L2Modality::Haptic => self.haptic_offset_us,
        }
    }
}

/// Defaults of the haptic ALSA sink (receiver `--l2-alsa-*`, probe). All
/// PROVISIONAL sink parameters (registration 3-3a: fixed in the freeze after
/// the M2 device measurement), never experimental parameters of a method.
/// HDA constrains period/buffer bytes to multiples of 128, i.e. multiples of
/// 32 frames at S16_LE stereo.
pub const ALSA_DEFAULT_DEVICE: &str = "hw:1,0";
pub const ALSA_DEFAULT_PERIOD_FRAMES: u64 = 256;
pub const ALSA_DEFAULT_BUFFER_FRAMES: u64 = 2_048;
pub const ALSA_DEFAULT_TARGET_FILL_FRAMES: u64 = 512;
pub const ALSA_DEFAULT_PREFILL_FRAMES: u64 = 512;
pub const ALSA_DEFAULT_QUEUE_FRAMES: u64 = 48_000;
pub const ALSA_DEFAULT_STATUS_INTERVAL_US: u64 = 1_000;
/// Bound on opening, configuring, prefilling and starting the stream.
pub const ALSA_READY_TIMEOUT_US: u64 = 5_000_000;
/// After a ready timeout: bound on waiting for the lane to exit and close
/// the device before `start` reports the failure.
pub const ALSA_RELEASE_TIMEOUT_US: u64 = 5_000_000;
/// Schema tag of the probe-only ALSA diagnostics file (registration 11-11).
pub const L2_ALSA_DIAGNOSTICS_SCHEMA: &str = "event-pair-l2-alsa-diagnostics-v1";

/// Where the haptic ALSA lane plays.
#[derive(Debug, Clone)]
pub enum AlsaBackend {
    /// The real device through libasound (`l2_alsa`), e.g. `hw:1,0`. Opened
    /// inside the lane thread when the stage starts.
    Device { name: String },
    /// The deterministic simulated stream (`l2_haptic::SimPcm`) driven by
    /// the real clock: tests only, never a device output.
    Simulated(SimPcmConfig),
}

impl AlsaBackend {
    pub fn is_device(&self) -> bool {
        match self {
            Self::Device { .. } => true,
            Self::Simulated(_) => false,
        }
    }
}

/// The haptic ALSA sink (registration §3-3, §3-3a). Every number here is a
/// provisional SINK parameter recorded in meta, fixed in the freeze after
/// the M2 device measurement; none is an experimental parameter of a method.
#[derive(Debug, Clone)]
pub struct AlsaSinkConfig {
    pub backend: AlsaBackend,
    pub placement: PlacementConfig,
    /// Status snapshot cadence; the lane also wakes on every arrival.
    pub status_interval_us: u64,
    /// Bound on opening, configuring, prefilling and starting the stream.
    pub ready_timeout_us: u64,
    /// After a ready timeout, bound on waiting for the lane to exit (and so
    /// to close the device) before `start` returns its error.
    pub release_timeout_us: u64,
    /// PROBE ONLY: a separate diagnostics JSONL (create_new) for the raw
    /// snapshot values the L2 JSONL must not carry (registration 11-11).
    /// The receiver never sets it.
    pub diagnostics: Option<PathBuf>,
}

impl AlsaSinkConfig {
    pub fn validate(&self) -> Result<()> {
        self.placement.validate().map_err(anyhow::Error::msg)?;
        if self.status_interval_us == 0 || self.status_interval_us > 100_000 {
            bail!("ALSA status interval must be in 1..=100000 us");
        }
        if self.ready_timeout_us == 0 || self.release_timeout_us == 0 {
            bail!("ALSA ready and release timeouts must be positive");
        }
        match &self.backend {
            AlsaBackend::Device { name } => {
                if name.is_empty() || name.contains('\0') {
                    bail!("ALSA device name must be a non-empty C string");
                }
            }
            AlsaBackend::Simulated(sim) => {
                if (sim.period_frames, sim.buffer_frames)
                    != (self.placement.period_frames, self.placement.buffer_frames)
                {
                    bail!("simulated stream sizes differ from the placement sizes");
                }
            }
        }
        Ok(())
    }
}

/// Defaults of the PC Wayland sink (receiver `--l2-wayland-*`, probe):
/// provisional SINK parameters recorded in meta, never experimental
/// parameters of a method.
pub const WAYLAND_DEFAULT_BUFFERS: usize = 4;
/// Bound on connecting, configuring and presenting the startup frame.
pub const WAYLAND_READY_TIMEOUT_US: u64 = 5_000_000;
/// After a failed start: bound on waiting for the lane to exit (and so to
/// disconnect) before `start` returns its error.
pub const WAYLAND_RELEASE_TIMEOUT_US: u64 = 5_000_000;
/// Schema tag of the probe-only Wayland diagnostics file.
pub const L2_WAYLAND_DIAGNOSTICS_SCHEMA: &str = "event-pair-l2-wayland-diagnostics-v1";

/// Where the PC Wayland lane presents.
#[derive(Debug, Clone)]
pub enum WaylandBackendKind {
    /// The user's Wayland display (`WAYLAND_DISPLAY`): a device output.
    /// Connected inside the lane thread when the stage starts.
    Display,
    /// The simulated compositor (`l2_pc::SimCompositor`) on the real clock:
    /// tests only, never a device output.
    Simulated(crate::l2_pc::SimCompositorConfig),
}

impl WaylandBackendKind {
    pub fn is_device(&self) -> bool {
        matches!(self, Self::Display)
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Display => "wayland",
            Self::Simulated(_) => "simulated",
        }
    }
}

/// The PC Wayland sink (registration §3-2, §3-3b).
#[derive(Debug, Clone)]
pub struct WaylandSinkConfig {
    pub backend: WaylandBackendKind,
    /// What the PC payload is (`--representation`): `.bin` records as sent,
    /// or a Draco bitstream decoded in the render worker.
    pub representation: crate::l2_pc::PcRepresentation,
    /// Header tier -> point scale, from the tiers meta file.
    pub tiers: crate::l2_pc::TierScales,
    /// `wl_shm` buffers (>= 2; a buffer the compositor holds is never written).
    pub buffers: usize,
    pub ready_timeout_us: u64,
    pub release_timeout_us: u64,
    /// PROBE ONLY: a separate diagnostics JSONL (create_new) for the raw
    /// presentation values (ns) the L2 JSONL must not carry. The receiver
    /// never sets it.
    pub diagnostics: Option<PathBuf>,
}

impl WaylandSinkConfig {
    pub fn validate(&self) -> Result<()> {
        if !(2..=16).contains(&self.buffers) {
            bail!("the Wayland sink needs 2..=16 wl_shm buffers");
        }
        if self.ready_timeout_us == 0 || self.release_timeout_us == 0 {
            bail!("Wayland ready and release timeouts must be positive");
        }
        if self.tiers.scales.is_empty() {
            bail!("the Wayland sink needs the tiers meta depth scales");
        }
        if let WaylandBackendKind::Simulated(sim) = &self.backend {
            if sim.buffers != self.buffers {
                bail!("simulated compositor buffers differ from the sink's");
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct L2Config {
    pub run_id: String,
    /// The receiver arm that released the objects (`b1`, `s1`, `s2`, `s3`, `s3r`).
    pub method: String,
    /// The sink of each modality.
    pub sinks: L2Sinks,
    /// Required iff some modality's sink is `fake`, refused otherwise. Its
    /// negative-test switches apply to the fake lanes only.
    pub fake: Option<FakeSinkConfig>,
    /// Required iff the haptic sink is `alsa`, refused otherwise.
    pub alsa: Option<AlsaSinkConfig>,
    /// Required iff the PC sink is `wayland`, refused otherwise (v1.4).
    pub wayland: Option<WaylandSinkConfig>,
    /// Bound of EACH modality's release -> sink submission queue (one lane
    /// per modality since v1.2; meta `queue_capacity_per_track`).
    pub queue_capacity: usize,
    /// Bound of the row queue into the single writer thread.
    pub row_capacity: usize,
    /// Close grace: after `finish` starts, how long producers may take to
    /// disappear and the sink may still receive terminals (anchor: close).
    pub report_window_us: u64,
    /// Bound of every per-object tracking structure (logical-event map, open
    /// submits, fake pending outputs). Exceeding it is an integrity failure.
    pub max_tracked_objects: usize,
}

impl L2Config {
    pub fn validate(&self) -> Result<()> {
        if self.run_id.is_empty() {
            bail!("L2 run_id must be non-empty");
        }
        if self.queue_capacity == 0 || self.row_capacity == 0 || self.max_tracked_objects == 0 {
            bail!("L2 queue and tracking capacities must be positive");
        }
        let any_fake = L2Modality::ALL.iter().any(|m| self.sinks.of(*m) == L2SinkKind::Fake);
        if any_fake && self.fake.is_none() {
            bail!("the fake L2 sink requires its offsets");
        }
        if !any_fake && self.fake.is_some() {
            bail!("fake-sink options apply only when some modality uses the fake L2 sink");
        }
        if self.sinks.pc == L2SinkKind::Alsa {
            bail!("the ALSA sink is a haptic sink");
        }
        if self.sinks.haptic == L2SinkKind::Wayland {
            bail!("the Wayland sink is a PC sink");
        }
        match (self.sinks.pc == L2SinkKind::Wayland, &self.wayland) {
            (true, Some(wayland)) => wayland.validate()?,
            (true, None) => bail!("the Wayland PC sink requires its configuration"),
            (false, Some(_)) => bail!("Wayland options apply only to the Wayland PC sink"),
            (false, None) => {}
        }
        match (self.sinks.haptic == L2SinkKind::Alsa, &self.alsa) {
            (true, Some(alsa)) => alsa.validate()?,
            (true, None) => bail!("the ALSA haptic sink requires its configuration"),
            (false, Some(_)) => bail!("ALSA options apply only to the ALSA haptic sink"),
            (false, None) => {}
        }
        Ok(())
    }

    /// Whether this modality's sink is a real output device. No null/fake
    /// sink is, nor a simulated ALSA stream or compositor.
    pub fn device_output_of(&self, modality: L2Modality) -> bool {
        match self.sinks.of(modality) {
            L2SinkKind::Null | L2SinkKind::Fake => false,
            L2SinkKind::Alsa => self.alsa.as_ref().is_some_and(|a| a.backend.is_device()),
            L2SinkKind::Wayland => self.wayland.as_ref().is_some_and(|w| w.backend.is_device()),
        }
    }

    /// File-level `device_output`: EVERY modality is a device sink.
    pub fn device_output(&self) -> bool {
        L2Modality::ALL.iter().all(|m| self.device_output_of(*m))
    }

    fn fake_lane(&self, modality: L2Modality) -> Option<FakeSinkConfig> {
        (self.sinks.of(modality) == L2SinkKind::Fake).then(|| self.fake.clone()).flatten()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum L2SubmitOutcome {
    Submitted,
    /// Recorded as `explicit_discard(output_queue_full)` before submit.
    QueueFull,
    /// `finish` already started; an integrity failure.
    AfterClose,
    /// The sink thread is gone; an integrity failure.
    SinkGone,
}

/// Integrity failures with their occurrence and detection times.
#[derive(Debug, Default, Clone, Copy)]
struct FailureLog {
    /// Earliest failure whose occurrence time is known:
    /// `(occurred_us, detected_us, reason)`.
    timed: Option<(u64, u64, &'static str)>,
    /// First-detected failure whose occurrence time is unknown:
    /// `(detected_us, reason)`.
    untimed: Option<(u64, &'static str)>,
}

/// State shared by every handle and both threads.
#[derive(Debug, Default)]
struct Shared {
    /// Live `L2Submitter` handles (the stage's own one included). Quiescence
    /// at `finish` means this reaches zero; it is counted explicitly because
    /// the stage keeps a non-producer control sender on the same channel.
    producers: AtomicUsize,
    closed: AtomicBool,
    /// `CLOSE_OPEN`, `CLOSE_PENDING`, or the published close deadline. Set by
    /// `finish` before anything else, independent of the message queue.
    close_deadline_us: AtomicU64,
    /// `finish`'s clock read that anchors the close grace (0 = not started).
    /// Published before the deadline; reported in the sentinel (v1.2).
    finish_start_us: AtomicU64,
    queue_full: AtomicU64,
    row_overflow: AtomicU64,
    submit_after_close: AtomicU64,
    sink_gone: AtomicU64,
    fake_pending_overflow: AtomicU64,
    producers_alive_at_finish: AtomicBool,
    failures: Mutex<FailureLog>,
    /// Earliest receiver fault `(t_us, cause)`; written before the sentinel.
    receiver_fault: Mutex<Option<(u64, String)>>,
}

impl Shared {
    /// A failure detected at the moment it happens (release side, finish).
    fn fail(&self, reason: &'static str) {
        let now = now_us();
        self.fail_occurred(now, reason);
    }

    /// A failure whose occurrence time is known; detection time is now.
    fn fail_occurred(&self, occurred_us: u64, reason: &'static str) {
        self.fail_detected(occurred_us, now_us(), reason);
    }

    /// A failure with both its occurrence and its detection time.
    fn fail_detected(&self, occurred_us: u64, detected_us: u64, reason: &'static str) {
        let mut log = self.failures.lock().unwrap_or_else(|p| p.into_inner());
        if log.timed.is_none_or(|(t, _, _)| occurred_us < t) {
            log.timed = Some((occurred_us, detected_us, reason));
        }
    }

    /// A failure whose occurrence time cannot be determined.
    fn fail_untimed(&self, reason: &'static str) {
        self.fail_untimed_detected(now_us(), reason);
    }

    /// A failure whose occurrence time cannot be determined, detected at
    /// `detected_us` (the first-detected one is kept).
    fn fail_untimed_detected(&self, detected_us: u64, reason: &'static str) {
        let mut log = self.failures.lock().unwrap_or_else(|p| p.into_inner());
        if log.untimed.is_none_or(|(t, _)| detected_us < t) {
            log.untimed = Some((detected_us, reason));
        }
    }

    fn failures(&self) -> FailureLog {
        *self.failures.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The published close deadline, read AFTER the caller read its receipt
    /// time. `CLOSE_PENDING` lasts from `finish`'s marker to its deadline
    /// store; there is no wall-clock bound on that hand-off (a preempted
    /// `finish` prolongs it), and the grace is anchored at `finish`'s clock
    /// read, not at its entry. A receipt read before the marker is compared
    /// with the deadline once it is published, so the verdict is unaffected.
    fn close_deadline(&self) -> Option<u64> {
        loop {
            match self.close_deadline_us.load(Ordering::SeqCst) {
                CLOSE_OPEN => return None,
                CLOSE_PENDING => std::thread::yield_now(),
                deadline => return Some(deadline),
            }
        }
    }

    /// Earliest fault wins (a cause recorded earlier elsewhere may be
    /// reported later with its own time).
    fn note_receiver_fault(&self, t_us: u64, cause: &str) {
        let mut fault = self.receiver_fault.lock().unwrap_or_else(|p| p.into_inner());
        if fault.as_ref().is_none_or(|(t, _)| t_us < *t) {
            *fault = Some((t_us, cause.to_string()));
        }
    }

    fn receiver_fault_noted(&self) -> bool {
        self.receiver_fault.lock().unwrap_or_else(|p| p.into_inner()).is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct L2Summary {
    pub submitted: u64,
    pub output: u64,
    pub discarded_before_submit: u64,
    pub discarded_after_submit: u64,
    pub submitted_unresolved: u64,
    /// `submitted_unresolved` split by modality, indexed by
    /// [`L2Modality::index`] (v1.2: the null-sink exemption is per modality).
    pub submitted_unresolved_by_track: [u64; 2],
    /// `output` rows violating the order rule (v1.2).
    pub output_order_violation: u64,
    /// Terminals received at or after the close deadline (v1.2 cross-check).
    pub terminal_at_or_after_deadline: u64,
    /// `finish`'s clock read that anchors the close grace (v1.2).
    pub finish_start_us: Option<u64>,
    /// The published close deadline = `finish_start_us` + grace (v1.2).
    pub close_deadline_us: Option<u64>,
    pub row_overflow: u64,
    pub submit_after_close: u64,
    pub sink_gone: u64,
    /// A terminal whose identity has no open submit.
    pub terminal_without_open_submit: u64,
    /// EXTRA releases (submit or pre-submit discard) of one `(track, seq)`:
    /// `Σ_key max(N_key − 1, 0)`, not the number of duplicated keys.
    pub duplicate_logical_release: u64,
    pub tracking_overflow: u64,
    pub fake_pending_overflow: u64,
    pub producers_alive_at_finish: bool,
    /// `submitted == output + discarded_after_submit + submitted_unresolved`.
    pub conservation_ok: bool,
    /// Max of (write completion − `t_log_arrival_us`) over all rows.
    pub max_write_lag_us: u64,
    /// Max of (`t_log_arrival_us` − `t_callback_us`) over terminal callbacks.
    pub max_callback_to_log_us: u64,
    /// Occurrence time of the first integrity failure; `None` if there is
    /// none OR its time cannot be determined (see the flag below).
    pub first_integrity_failure_us: Option<u64>,
    pub first_integrity_failure_reason: Option<String>,
    pub first_integrity_failure_detected_us: Option<u64>,
    /// `false` when some failure's occurrence time is unknown (registration
    /// §6-1: undecidable).
    pub first_failure_time_determinable: bool,
    /// The earliest fatal receiver error, if the receiver reported one.
    pub receiver_fault: Option<(u64, String)>,
}

impl L2Summary {
    /// Instrumentation-level integrity (registration §3-5, §6). A null-sink
    /// modality is never L2 evidence; ITS unresolved submissions are expected
    /// (v1.2: per modality, so a null PC lane exempts nothing on haptic).
    pub fn integrity_ok(&self, sinks: L2Sinks) -> bool {
        self.first_integrity_failure_reason.is_none()
            && self.conservation_ok
            && self.row_overflow == 0
            && self.submit_after_close == 0
            && self.sink_gone == 0
            && self.terminal_without_open_submit == 0
            && self.duplicate_logical_release == 0
            && self.tracking_overflow == 0
            && self.fake_pending_overflow == 0
            && self.output_order_violation == 0
            && self.terminal_at_or_after_deadline == 0
            && !self.producers_alive_at_finish
            && L2Modality::ALL.iter().all(|m| {
                !sinks.of(*m).resolves() || self.submitted_unresolved_by_track[m.index()] == 0
            })
    }
}

enum SinkMsg {
    /// One released object and its payload (the object bytes after the
    /// 32-byte header; a refcounted slice, never copied on the release side).
    /// A sink that does not play the payload drops it on accept.
    Item(L2Submission, Option<bytes::Bytes>),
    /// The last producer handle was dropped (prompt wake-up only; the sink
    /// always re-reads the producer count).
    Wake,
    /// `finish` started (wake-up; the deadline itself is in shared state).
    Closing,
}

/// Modality/device evidence of one `output` row (`output_detail`; its schema
/// per sink kind is declared in the meta row's `sink_contracts`).
#[derive(Debug, Clone)]
enum OutputDetail {
    /// `{"device": "fake", "modality": <track>}` (unchanged from v1.1).
    Fake,
    /// The status snapshot that showed `hw_pos > n_k` (haptic ALSA lane).
    Alsa(Box<AlsaOutputDetail>),
    /// The commit and its `presented` feedback (PC Wayland lane, v1.4).
    Wayland(Box<WaylandOutputDetail>),
}

impl OutputDetail {
    fn to_json(&self, modality: L2Modality) -> serde_json::Value {
        match self {
            Self::Fake => serde_json::json!({ "device": "fake", "modality": modality.as_str() }),
            Self::Alsa(detail) => detail.to_json(),
            Self::Wayland(detail) => detail.to_json(),
        }
    }

    /// Draco decode times of a PC frame (top-level row fields, v1.4).
    fn decode_us(&self) -> Option<(u64, u64)> {
        match self {
            Self::Wayland(detail) => detail.timing.decode_us,
            _ => None,
        }
    }
}

/// The evidence of one PC Wayland `output` (integer µs CLOCK_MONOTONIC
/// times only; the raw presentation ns live in the probe diagnostics file).
#[derive(Debug, Clone)]
pub(crate) struct WaylandOutputDetail {
    backend: &'static str,
    commit_id: u64,
    buffer: usize,
    timing: crate::l2_pc::PcTiming,
    t_commit_us: u64,
    presented: crate::l2_pc::Presented,
    info: crate::l2_pc::FrameInfo,
}

impl WaylandOutputDetail {
    fn to_json(&self) -> serde_json::Value {
        let p = &self.presented;
        serde_json::json!({
            "device": "wayland",
            "backend": self.backend,
            "commit_id": self.commit_id,
            "buffer": self.buffer,
            "t_render_start_us": self.timing.t_render_start_us,
            "t_render_end_us": self.timing.t_render_end_us,
            "t_commit_us": self.t_commit_us,
            "refresh_us": p.refresh_us(),
            "msc": p.seq,
            "flags": p.flags,
            "flag_names": crate::l2_pc::presentation_flag_names(p.flags),
            "sync_outputs": p.sync_outputs,
            "points": self.info.points,
            "covered_pixels": self.info.covered_pixels,
            "depth_scale": self.info.depth_scale,
        })
    }

    fn schema() -> serde_json::Value {
        serde_json::json!({
            "device": "the string \"wayland\"",
            "backend": "wayland (the display) | simulated (tests, never a device output)",
            "commit_id": "the commit whose wp_presentation_feedback this row is (immutable commit_id -> run/track/tier/seq/pts/event_id/route mapping made at the commit)",
            "buffer": "wl_shm buffer index the frame was committed in",
            "t_render_start_us": "clock read right before the render (after the Draco decode for draco)",
            "t_render_end_us": "clock read right after the render",
            "t_commit_us": "clock read right before attach/damage/frame/feedback/commit were issued",
            "refresh_us": "presented.refresh as integer us (floor); 0 = unknown",
            "msc": "presented seq_hi << 32 | seq_lo (refresh counter, not a time)",
            "flags": "presented.flags (wp_presentation_feedback.kind bits)",
            "flag_names": "decoded flags: vsync, hw_clock, hw_completion, zero_copy",
            "sync_outputs": "sync_output events received before presented",
            "points": "points drawn",
            "covered_pixels": "canvas pixels at least one point covers",
            "depth_scale": "the tier's depth_scale from the tiers meta",
        })
    }
}

enum Row {
    Submit(L2Submission),
    Output {
        submission: L2Submission,
        t_output_us: u64,
        /// The sink's real receipt time of the terminal callback.
        t_callback_us: u64,
        detail: OutputDetail,
    },
    Discard {
        identity: L2Identity,
        t_release_us: u64,
        t_submit_us: Option<u64>,
        t_discard_us: u64,
        reason: &'static str,
        /// Draco decode times of a PC frame that was decoded (v1.4).
        decode_us: Option<(u64, u64)>,
    },
    /// A device lane's counters, sent once before its thread exits; written
    /// into the sentinel (`lanes.<track>`), never as a row of its own.
    LaneStats { modality: L2Modality, stats: serde_json::Value },
    End,
}

/// Cloneable, non-blocking submission handle held at the release points.
/// Every live clone is counted; `finish` requires all of them to be dropped
/// within the close grace (`producers_alive_at_finish` otherwise), so a
/// submission after the sentinel is only possible in a run whose sentinel
/// already says `integrity_ok: false`.
pub struct L2Submitter {
    /// One bounded submission queue per modality, indexed by
    /// [`L2Modality::index`].
    lanes: [SyncSender<SinkMsg>; 2],
    rows: SyncSender<Row>,
    shared: Arc<Shared>,
    /// Fake negative test (`force_queue_full`), per lane: only fake lanes.
    force_queue_full: [bool; 2],
}

impl Clone for L2Submitter {
    fn clone(&self) -> Self {
        self.shared.producers.fetch_add(1, Ordering::AcqRel);
        Self {
            lanes: self.lanes.clone(),
            rows: self.rows.clone(),
            shared: self.shared.clone(),
            force_queue_full: self.force_queue_full,
        }
    }
}

impl Drop for L2Submitter {
    fn drop(&mut self) {
        // Every send of this handle happened before this decrement, so a sink
        // that reads zero and then drains its queue sees all of them. Every
        // lane is woken: each one re-reads the producer count itself.
        if self.shared.producers.fetch_sub(1, Ordering::AcqRel) == 1 {
            for lane in &self.lanes {
                let _ = lane.try_send(SinkMsg::Wake);
            }
        }
    }
}

impl L2Submitter {
    /// [`Self::submit_payload`] without a payload (the null/fake sinks never
    /// look at it).
    pub fn submit(&self, identity: L2Identity, t_release_us: u64) -> L2SubmitOutcome {
        self.submit_payload(identity, t_release_us, None)
    }

    /// Hand one RELEASED object (and its payload) to its modality's lane.
    /// Never blocks, never fails the caller. `t_submit_us` is read here and
    /// clamped to `t_release_us` so `t_release <= t_submit` holds by
    /// construction.
    pub fn submit_payload(
        &self,
        identity: L2Identity,
        t_release_us: u64,
        payload: Option<bytes::Bytes>,
    ) -> L2SubmitOutcome {
        if self.shared.closed.load(Ordering::Acquire) {
            self.shared.submit_after_close.fetch_add(1, Ordering::Relaxed);
            self.shared.fail(FAIL_SUBMIT_AFTER_CLOSE);
            return L2SubmitOutcome::AfterClose;
        }
        let t_submit_us = now_us().max(t_release_us);
        let submission = L2Submission { identity, t_release_us, t_submit_us };
        let lane = identity.modality.index();
        let full = if self.force_queue_full[lane] {
            true
        } else {
            match self.lanes[lane].try_send(SinkMsg::Item(submission, payload)) {
                Ok(()) => return L2SubmitOutcome::Submitted,
                Err(TrySendError::Full(_)) => true,
                Err(TrySendError::Disconnected(_)) => false,
            }
        };
        if !full {
            self.shared.sink_gone.fetch_add(1, Ordering::Relaxed);
            self.shared.fail(FAIL_SINK_GONE);
            return L2SubmitOutcome::SinkGone;
        }
        self.shared.queue_full.fetch_add(1, Ordering::Relaxed);
        let t_discard_us = now_us().max(t_release_us);
        let row = Row::Discard {
            identity,
            t_release_us,
            t_submit_us: None,
            t_discard_us,
            reason: DISCARD_OUTPUT_QUEUE_FULL,
            decode_us: None,
        };
        if self.rows.try_send(row).is_err() {
            self.shared.row_overflow.fetch_add(1, Ordering::Relaxed);
            self.shared.fail_occurred(t_discard_us, FAIL_ROW_QUEUE_OVERFLOW);
        }
        L2SubmitOutcome::QueueFull
    }

    /// Record a fatal receiver error at `t_us` (the moment the receiver
    /// recorded it). The earliest is kept. Non-blocking, no row queue.
    pub fn receiver_fault(&self, t_us: u64, cause: &str) {
        self.shared.note_receiver_fault(t_us, cause);
    }

    pub fn receiver_fault_noted(&self) -> bool {
        self.shared.receiver_fault_noted()
    }
}

/// A non-producer handle that only records receiver faults (earliest wins).
/// Held by tasks that can store a fatal cause but never submit (drains); it
/// does not count toward producer quiescence at `finish`.
#[derive(Clone)]
pub struct L2FaultStamp {
    shared: Arc<Shared>,
}

impl L2FaultStamp {
    /// Record a fatal receiver error at `t_us` (the moment it was stored).
    pub fn receiver_fault(&self, t_us: u64, cause: &str) {
        self.shared.note_receiver_fault(t_us, cause);
    }

    pub fn receiver_fault_noted(&self) -> bool {
        self.shared.receiver_fault_noted()
    }
}

/// The running output stage: meta row written, lane + writer threads alive.
pub struct L2Output {
    config: L2Config,
    path: PathBuf,
    file_identity: (u64, u64),
    /// The stage's own producer handle; dropped by `finish` before it waits
    /// for every other producer to disappear.
    submitter: Option<L2Submitter>,
    /// The stage's own row handle (`End`). Not a producer.
    rows: SyncSender<Row>,
    /// Non-producer senders used only to wake each lane with `Closing`.
    controls: [SyncSender<SinkMsg>; 2],
    shared: Arc<Shared>,
    /// One sink thread per modality, in lane order.
    lanes: Vec<JoinHandle<()>>,
    writer: Option<JoinHandle<std::io::Result<L2Summary>>>,
}

/// The declared contract of one modality's sink (meta `sink_contracts`):
/// whether it is a device output, whether it ever resolves a submission, what
/// `t_output_us` means, and the `output_detail` schema of its output rows.
fn sink_contract(config: &L2Config, modality: L2Modality) -> serde_json::Value {
    let device_output = config.device_output_of(modality);
    match config.sinks.of(modality) {
        L2SinkKind::Null => serde_json::json!({
            "kind": "null",
            "device_output": device_output,
            "resolves": false,
            "terminals": "none: every submission stays unresolved by design and is exempt from the unresolved integrity rule for this track only",
            "t_output_us": null,
            "output_detail": null,
        }),
        L2SinkKind::Fake => serde_json::json!({
            "kind": "fake",
            "device_output": device_output,
            "resolves": true,
            "terminals": "one synthetic output per submission (unless a fake negative-test switch is set); never an explicit_discard after submit",
            "t_output_us": "t_submit_us + the track's fake offset_us (synthetic, meta fake)",
            "t_callback_us": "a real clock read at or after t_output_us, before the row is queued",
            "output_detail": {
                "device": "the string \"fake\"",
                "modality": "track name (pc | haptic)",
            },
        }),
        L2SinkKind::Alsa => serde_json::json!({
            "kind": "alsa",
            "backend": config.alsa.as_ref().map(|a| alsa_backend_name(&a.backend)),
            "device_output": device_output,
            "resolves": true,
            "terminals": "output once a status snapshot shows hw_pos > n_k (receipt strictly before close_deadline_us); explicit_discard before submit: output_queue_full (write queue would exceed queue_limit_frames); explicit_discard after submit: payload_invalid, device_failure",
            "t_output_us": "htstamp_us + round_half_away((n_k - hw_pos) * 1e6 / 48000), signed integer arithmetic, from the ONE snapshot that shows hw_pos > n_k (registration 3-3: PCM frame time back-computed from the ALSA-reported position and timestamp; not a physical DAC measurement)",
            "t_callback_us": "clock read immediately after that snd_pcm_status call",
            "output_detail": AlsaOutputDetail::schema(),
        }),
        L2SinkKind::Wayland => serde_json::json!({
            "kind": "wayland",
            "backend": config.wayland.as_ref().map(|w| w.backend.name()),
            "representation": config.wayland.as_ref().map(|w| w.representation.as_str()),
            "device_output": device_output,
            "resolves": true,
            "terminals": "output on wp_presentation_feedback.presented of the frame's commit (receipt strictly before close_deadline_us); explicit_discard before submit: output_queue_full (the bounded submission queue); explicit_discard after submit: superseded_before_commit (the pending frame was replaced before any commit), compositor_discarded (feedback discarded), payload_invalid (no payload, a tier the tiers meta does not know, records not drawable at the tier scale), decode_failed (the Draco decoder refused the payload), device_failure (a frame not yet committed when the PC device failed, or submitted after); a COMMITTED frame ends only through its feedback, otherwise it stays unresolved",
            "t_output_us": "presented tv_sec*1e6 + floor(tv_nsec/1000) on the wp_presentation clock, verified CLOCK_MONOTONIC before the meta row (registration 3-2: the time the compositor reports the content update turned into light; not a physical measurement)",
            "t_callback_us": "clock read when the presented event is handled",
            "t_decode_start_us": "draco only, top-level on every after-submit terminal of a decoded frame: clock read right after the frame left the processing queue, immediately before the decoder call",
            "t_decode_end_us": "draco only: clock read right after the 8-byte records exist (the same records the bin path renders)",
            "output_detail": WaylandOutputDetail::schema(),
        }),
    }
}

fn alsa_backend_name(backend: &AlsaBackend) -> &'static str {
    match backend {
        AlsaBackend::Device { .. } => "device",
        AlsaBackend::Simulated(_) => "simulated",
    }
}

/// The meta `alsa` section: the negotiated, verified stream and the
/// registered-later placement policy as implemented.
fn alsa_meta(alsa: &AlsaSinkConfig, ready: &LaneReady) -> serde_json::Value {
    use crate::l2_haptic::{audio_tstamp_type_name, ns_to_us_floor};
    let p = &ready.params;
    let c = &alsa.placement;
    let first = &ready.epoch.first_status;
    // Integer µs CLOCK_MONOTONIC only (registration 11-11): raw ns values and
    // the audio timestamp go to the probe-only diagnostics file.
    let stream_epoch = serde_json::json!({
        "prefill_frames": ready.prefill_frames,
        "t_start_us": ready.epoch.t_start_us,
        "trigger_htstamp_us": ns_to_us_floor(ready.epoch.trigger_htstamp_ns),
        "first_status_t0_us": ready.epoch.first_status_t0_us,
        "first_status_htstamp_us": ns_to_us_floor(first.htstamp_ns),
        "first_status_delay_frames": first.delay,
        "first_status_avail_frames": first.avail,
        "first_status_driver_delay_frames": first.delay - (p.buffer_frames as i64 - first.avail as i64),
    });
    let audio_tstamp_type = serde_json::json!({
        "requested": audio_tstamp_type_name(p.audio_tstamp_type_requested),
        "actual_first_status": audio_tstamp_type_name(first.audio_tstamp.actual_type),
        "report_valid_first_status": first.audio_tstamp.valid,
        "start_check": "the first snapshot must report valid = 1 and actual_type in 0..=5, otherwise the run is refused: plausibility that the request reached the driver as decoded, NOT proof that LINK was selected; every output row records its actual type",
    });
    serde_json::json!({
        "backend": alsa_backend_name(&alsa.backend),
        "device": p.device,
        "library": p.library,
        "open_mode": "SND_PCM_NONBLOCK",
        "access": p.access,
        "format": p.format,
        "channels": p.channels,
        "rate_hz": p.rate_hz,
        "period_frames": p.period_frames,
        "buffer_frames": p.buffer_frames,
        "tstamp_mode": p.tstamp_mode,
        "tstamp_type": p.tstamp_type,
        "device_details": p.details,
        "audio_tstamp_type": audio_tstamp_type,
        "target_fill_frames": c.target_fill_frames,
        "prefill_frames": c.prefill_frames,
        "queue_limit_frames": c.queue_limit_frames,
        "status_interval_us": alsa.status_interval_us,
        "parameter_status": "provisional sink parameters, not registered experimental parameters; fixed in the freeze after the M2 device measurement (registration 3-3a)",
        "stream_epoch": stream_epoch,
        "diagnostics_file": alsa.diagnostics.as_ref().map(|path| path.display().to_string()),
        "frame_numbering": "n = cumulative 48 kHz frames written since the stream was prepared (prefill included); the lane is the only writer, so n equals the unwrapped kernel appl_ptr of the hw device",
        "upsample": "each 8 kHz S16_LE mono sample repeated 6 times (88/89 samples -> 528/534 frames, never a fixed length)",
        "channel_map": "mono duplicated to L and R",
        "placement_policy": "released ticks are appended in release order at the current write position (n_k = w), written only whole (avail >= frames); while a tick waits nothing else is written; never aligned to a planned slot",
        "silence_policy": "with an empty write queue, one period of silence when buffer_frames - avail < target_fill_frames; silence frames are never the output of any tick",
        "queue_policy": "Q counts every tick frame not yet written to the device (the waiting queue plus the unwritten remainder of the tick being written); a tick that would make it exceed queue_limit_frames is explicit_discard output_queue_full before submit",
        "hw_pos_definition": "from ONE snd_pcm_status snapshot: hw_pos = appl_pos - delay, appl_pos = frames written by this lane at that call; hw_ptr_pos = appl_pos - (buffer_frames - avail) is the DMA pointer (must never decrease); driver_delay_frames = delay - (buffer_frames - avail) is recorded per row",
        "output_rule": "an output row only when hw_pos > n_k in that snapshot (strict); t_output_us = htstamp_us + round_half_away((n_k - hw_pos) * 1e6 / 48000); htstamp_us = sec*1e6 + floor(nsec/1000)",
        "htstamp_semantics": "inferred from the kernel source, to be confirmed on the device: with an audio timestamp of actual type LINK the status htstamp is the system time read next to the position read in that same status call; with DEFAULT it is the time the current position value was first observed",
        "audio_tstamp_policy": "the LINK audio timestamp is requested before every status call; the L2 JSONL records only its requested/actual type and report bits; the audio timestamp value (not CLOCK_MONOTONIC) and every raw ns value exist only in a probe diagnostics file (registration 11-11); comparison only, never the registered formula",
        "failure_policy": "XRUN, SUSPENDED, DISCONNECTED, an unexpected state, a moved trigger timestamp or a decreasing DMA pointer (epoch reset), an inconsistent snapshot, or a status/write error is an integrity failure (L2 invalid whatever its time, registration 6-1); the stream is stopped, never recovered, and every submission that can no longer be output gets explicit_discard device_failure (after submit, receipt before close_deadline_us). No occurrence time is claimed until the M2b verification: first_integrity_failure_us = null, first_failure_time_determinable = false; lanes.haptic.device_failure keeps observation_interval_us [last_good_status_t0_us, detected_us], detected_us and the raw kernel_trigger_us",
        "close_policy": "after finish starts the lane keeps writing (silence) and observing until the END of the last written tick (n_k + frames) passed hw_pos — not just its onset output, never waiting for trailing silence — or the close deadline, then drops the stream; lanes.haptic.tail_played_at_close / unplayed_tick_frames_at_close record which",
    })
}

/// The meta `wayland` section: what the PC lane negotiated and how it works.
fn wayland_meta(wayland: &WaylandSinkConfig, ready: &pc_lane::PcLaneReady) -> serde_json::Value {
    serde_json::json!({
        "backend": wayland.backend.name(),
        "startup": ready.startup.to_json(),
        "canvas_side_px": ready.side,
        "buffers": wayland.buffers,
        "representation": wayland.representation.as_str(),
        "tiers_meta": wayland.tiers.to_json(),
        "decoder": crate::l2_pc::decoder_meta(wayland.representation),
        "renderer": crate::l2_pc::renderer_meta(ready.side),
        "diagnostics_file": wayland.diagnostics.as_ref().map(|path| path.display().to_string()),
        "parameter_status": "provisional sink parameters (buffers, ready timeout), not registered experimental parameters; fixed in the freeze after the M3 display measurement",
        "thread_layout": "render worker thread: takes released frames from the bounded submission queue in release order, decodes (draco) and renders them, puts each into a one-frame pending slot; commit thread: owns the compositor connection and commits the pending frame at each commit opportunity (neither thread has a real-time priority, registration 11-10)",
        "commit_policy": "a commit opportunity exists when the frame callback requested with the previous commit has fired (or nothing was committed since startup) and no PC device failure occurred; one frame per commit; wp_presentation.feedback is requested immediately before every commit and maps its commit_id to the frame immutably",
        "pending_policy": "one pending frame; a frame replaced there before it was committed is explicit_discard superseded_before_commit (after submit)",
        "buffer_policy": "a frame is written only into a wl_shm buffer the compositor does not hold (Held from its commit until wl_buffer.release); with none free the pending frame waits; wl_buffer.release and frame callbacks are never output evidence",
        "feedback_policy": "presented -> output; discarded -> explicit_discard compositor_discarded; a feedback or frame callback for an unknown or already resolved commit is a timed integrity failure at its receipt (wayland_unexpected_feedback / wayland_unexpected_frame_callback); a presented timestamp with tv_nsec >= 1e9 is wayland_bad_timestamp and the frame stays unresolved",
        "failure_policy": "compositor connection loss or protocol error, xdg_toplevel.close, a configure that changes the size or drops fullscreen, an output mode change or removal after ready, and committed frames still without feedback at close are PC device failures: untimed (first_integrity_failure_us null, first_failure_time_determinable false), recorded in lanes.pc.device_failure with observation_interval_us [last_good_us, detected_us]; after one, nothing is committed any more and every frame not yet committed (pending or submitted later) gets explicit_discard device_failure after submit; committed frames keep waiting for their feedback",
        "close_policy": "after finish starts the worker processes every submission of the producers, the commit thread commits the last pending frame and waits until every committed frame has its feedback or the close deadline, then disconnects",
        "start_check": "wp_presentation.clock_id must be CLOCK_MONOTONIC before the meta row, otherwise the run is refused; the startup frame (commit 0, background, not a ledger row) must be presented",
    })
}

/// The meta row (written once, synchronously, before any other row).
fn meta_row(config: &L2Config, alsa_ready: Option<&LaneReady>, pc_ready: Option<&pc_lane::PcLaneReady>) -> serde_json::Value {
    let fake = config.fake.as_ref();
    let sinks = serde_json::json!({
        "pc": config.sinks.pc.as_str(),
        "haptic": config.sinks.haptic.as_str(),
    });
    let contracts = serde_json::json!({
        "pc": sink_contract(config, L2Modality::Pc),
        "haptic": sink_contract(config, L2Modality::Haptic),
    });
    let row_time_semantics = serde_json::json!({
        "object_rows": "t_us == t_log_arrival_us == writer clock immediately before the row write",
        "sentinel_row": "t_us == end_us == t_log_arrival_us == writer clock before the sentinel write",
        "receiver_fault": "t_us = moment the receiver recorded its first fatal error (earliest kept); t_log_arrival_us = write time",
        "t_callback_us": "sink receipt time of the terminal callback",
        "t_output_us": "per sink kind: sink_contracts.<track>.t_output_us",
        "file_order": "t_us is not monotonic in file order; t_log_arrival_us is",
    });
    let failure_time_semantics = serde_json::json!({
        "duplicate_logical_release": "t_release_us of the second release (two earliest kept per (track, seq))",
        "row_queue_overflow": "t_discard_us of the discard whose row could not be queued; from a device lane the event time of the row (t_submit_us of a submit, t_callback_us of an output, t_discard_us of a discard)",
        "haptic_payload_invalid": "t_submit_us of the submission",
        "tracking_capacity_exceeded": "t_submit_us (submit) or t_discard_us (pre-submit discard) of the offending release",
        "fake_pending_overflow": "t_submit_us of the offending submission",
        "terminal_without_open_submit": "t_callback_us (output) or t_discard_us (post-submit discard) of the orphan terminal",
        "submitted_unresolved": "earliest t_submit_us among unresolved submits of tracks whose sink resolves",
        "submit_after_close": "detection = the attempt",
        "sink_gone": "detection (the failed send)",
        "producers_alive_at_finish": "detection (the close deadline)",
        "submit_conservation_violated": "undeterminable unless explained by a timed failure",
        "output_order_violation": "t_callback_us of the offending output row",
        "terminal_at_or_after_deadline": "the receipt time of the offending terminal",
        "undeterminable": "first_integrity_failure_us = null, first_failure_time_determinable = false",
    });
    // Every ALSA device-failure reason, looked up by name like the others.
    let mut failure_time_semantics = failure_time_semantics;
    for (reason, semantics) in [
        (crate::l2_pc::FAIL_PC_PAYLOAD_INVALID, "t_submit_us of the submission"),
        (crate::l2_pc::FAIL_PC_DECODE_FAILED, "t_submit_us of the submission (the decoder's refusal is seen at t_decode_end_us, also on the row)"),
        (crate::l2_pc::FAIL_WAYLAND_UNEXPECTED_FEEDBACK, "receipt time of the offending feedback event"),
        (crate::l2_pc::FAIL_WAYLAND_UNEXPECTED_FRAME_CALLBACK, "receipt time of the offending frame callback"),
        (crate::l2_pc::FAIL_WAYLAND_BAD_TIMESTAMP, "receipt time of the presented event"),
    ] {
        failure_time_semantics[reason] = serde_json::Value::from(semantics);
    }
    for reason in crate::l2_pc::WAYLAND_DEVICE_FAILURE_REASONS {
        failure_time_semantics[reason] = serde_json::Value::from(
            "UNDETERMINABLE (PC device failure): first_integrity_failure_us = null, first_failure_time_determinable = false, first_integrity_failure_detected_us = the detection; sentinel lanes.pc.device_failure keeps observation_interval_us [last_good_us (clock read before the last compositor call that succeeded), detected_us]",
        );
    }
    for reason in crate::l2_haptic::ALSA_DEVICE_FAILURE_REASONS {
        failure_time_semantics[reason] = serde_json::Value::from(
            "UNDETERMINABLE until the kernel timestamps are verified on the device (registration 6-1): first_integrity_failure_us = null, first_failure_time_determinable = false, first_integrity_failure_detected_us = the detection; sentinel lanes.haptic.device_failure keeps observation_interval_us [last_good_status_t0_us (clock read before the last healthy status call), detected_us] and the raw kernel_trigger_us (unverified, not an occurrence time)",
        );
    }
    let counter_semantics = serde_json::json!({
        "duplicate_logical_release": "number of EXTRA releases of one (track, seq): sum over keys of max(N_key - 1, 0); not the number of duplicated keys",
        "submitted_unresolved": "submits without a terminal at the sentinel (all tracks); submitted_unresolved_by_track splits it",
        "discarded_before_submit": "releases refused before submit (explicit_discard with discard_phase before_submit; the bounded submission queue: output_queue_full)",
        "discarded_after_submit": "submits terminated by an explicit_discard (discard_phase after_submit)",
        "output_order_violation": "output rows violating order_rule",
        "terminal_at_or_after_deadline": "terminals whose receipt time is not < close_deadline_us",
        "lanes": "sentinel lanes.<track>: counters of a device lane (null for null/fake lanes); see sink_contracts",
        "lanes.pc.device_failure": "the first PC device failure (wayland lane) or null; the counterpart of lanes.haptic.device_failure",
    });
    serde_json::json!({
        "role": "meta",
        "l2_schema": L2_SCHEMA,
        "run_id": config.run_id,
        "method": config.method,
        "sinks": sinks,
        "sink_contracts": contracts,
        "device_output": config.device_output(),
        "device_output_rule": "true iff the sink of EVERY track is a device sink (sink_contracts.<track>.device_output); a file with any null, fake or simulated track is test-only and is not L2 evidence",
        "clock": L2_CLOCK,
        "time_unit": "us",
        "t_us": now_us(),
        "queue_capacity_per_track": config.queue_capacity,
        "row_capacity": config.row_capacity,
        "max_tracked_objects": config.max_tracked_objects,
        "report_window_us": config.report_window_us,
        "report_window_anchor": REPORT_WINDOW_ANCHOR,
        "report_window_rule": "a terminal is accepted iff its receipt time (t_callback_us of an output, t_discard_us of an after-submit discard) < close_deadline_us = finish_start_us + report_window_us (both in the sentinel); equality is rejected; not the H-anchored G_report",
        "order_rule": "every output row satisfies t_release_us <= t_submit_us <= t_output_us <= t_callback_us <= t_log_arrival_us, and a PC wayland output row also t_submit_us <= t_decode_start_us <= t_decode_end_us (draco) <= output_detail.t_render_start_us <= t_render_end_us <= t_commit_us <= t_output_us; the writer checks it after writing the row (values are never clamped) and a violation is the integrity failure output_order_violation",
        "row_time_semantics": row_time_semantics,
        "failure_time_semantics": failure_time_semantics,
        "counter_semantics": counter_semantics,
        "fake": fake.map(|f| serde_json::json!({
            "pc_offset_us": f.pc_offset_us,
            "haptic_offset_us": f.haptic_offset_us,
            "force_queue_full": f.force_queue_full,
            "drop_terminals": f.drop_terminals,
        })),
        "alsa": config.alsa.as_ref().zip(alsa_ready).map(|(alsa, ready)| alsa_meta(alsa, ready)),
        "wayland": config.wayland.as_ref().zip(pc_ready).map(|(wayland, ready)| wayland_meta(wayland, ready)),
    })
}

/// `(st_dev, st_ino)` of an open file (fstat).
pub fn open_file_identity(file: &File) -> std::io::Result<(u64, u64)> {
    let metadata = file.metadata()?;
    Ok((metadata.dev(), metadata.ino()))
}

impl L2Output {
    pub fn start(path: &Path, config: L2Config) -> Result<Self> {
        Self::start_excluding(path, config, &[])
    }

    /// Create the L2 JSONL, write the meta row synchronously and start the
    /// sink and writer threads.
    ///
    /// The file is created with `O_CREAT | O_EXCL` (`create_new`): an existing
    /// path — including a symlink, dangling or not, and a hard link — is
    /// refused and left byte-identical, so no evidence file can be truncated
    /// or aliased. `exclude` lists `(dev, ino)` of files that are already open
    /// (the rx log); the new file is compared by fstat before anything is
    /// written to it.
    pub fn start_excluding(path: &Path, config: L2Config, exclude: &[(u64, u64)]) -> Result<Self> {
        config.validate()?;
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .with_context(|| format!("create L2 log {} (must not exist)", path.display()))?;
        let file_identity = open_file_identity(&file)?;
        if exclude.contains(&file_identity) {
            bail!("L2 log {} is the same file as an open log", path.display());
        }
        let mut w = BufWriter::new(file);
        let (rows_tx, rows_rx) = mpsc::sync_channel::<Row>(config.row_capacity);
        let shared = Arc::new(Shared::default());
        let mut senders: [Option<SyncSender<SinkMsg>>; 2] = [None, None];
        let mut lane_threads: [Option<JoinHandle<()>>; 2] = [None, None];

        // Device lanes go first, PC then haptic: each opens its device (the
        // PC lane connects to the compositor, goes fullscreen and presents its
        // startup frame; the haptic lane opens, configures, prefills and
        // starts its stream, registration 3-3a: before the run's warm-up) and
        // hands back what it negotiated, which the meta row records. If one
        // cannot, `start` fails here and the receiver refuses to run; a device
        // lane that was already ready is torn down first (its queue closed so
        // it disconnects, joined within its release bound). The file then
        // holds no meta row, so it can never pass as a run's ledger.
        //
        // Probe-only diagnostics files are created first (create_new, never
        // the L2 file or an excluded open log), so a bad path fails before
        // any device opens; each lane writes its own after its device stopped.
        let mut alsa_diagnostics = None;
        if let (L2SinkKind::Alsa, Some(alsa)) = (config.sinks.haptic, config.alsa.as_ref()) {
            alsa_diagnostics = Some(match &alsa.diagnostics {
                None => None,
                Some(diag_path) => {
                    let diag_file = OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(diag_path)
                        .with_context(|| format!("create ALSA diagnostics {} (must not exist)", diag_path.display()))?;
                    let diag_identity = open_file_identity(&diag_file)?;
                    if diag_identity == file_identity || exclude.contains(&diag_identity) {
                        bail!("ALSA diagnostics {} is the same file as an open log", diag_path.display());
                    }
                    Some(DiagLog {
                        file: BufWriter::new(diag_file),
                        run_id: config.run_id.clone(),
                        outputs: Vec::new(),
                        dropped: 0,
                    })
                }
            });
        }
        let mut pc_ready = None;
        if let (L2SinkKind::Wayland, Some(wayland)) = (config.sinks.pc, config.wayland.clone()) {
            let diagnostics = match &wayland.diagnostics {
                None => None,
                Some(diag_path) => {
                    let diag_file = OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(diag_path)
                        .with_context(|| format!("create Wayland diagnostics {} (must not exist)", diag_path.display()))?;
                    let diag_identity = open_file_identity(&diag_file)?;
                    let alsa_identity = match &alsa_diagnostics {
                        Some(Some(diag)) => Some(open_file_identity(diag.file.get_ref())?),
                        _ => None,
                    };
                    if diag_identity == file_identity
                        || exclude.contains(&diag_identity)
                        || alsa_identity == Some(diag_identity)
                    {
                        bail!("Wayland diagnostics {} is the same file as an open log", diag_path.display());
                    }
                    Some(pc_lane::PcDiagLog::new(BufWriter::new(diag_file), config.run_id.clone()))
                }
            };
            let (items_tx, items_rx) = mpsc::sync_channel::<SinkMsg>(config.queue_capacity);
            let (ready_tx, ready_rx) = mpsc::channel();
            let rows = rows_tx.clone();
            let lane_shared = shared.clone();
            let timeout = micros(wayland.ready_timeout_us);
            let release = micros(wayland.release_timeout_us);
            let lane = std::thread::Builder::new()
                .name("l2-sink-pc-wayland".into())
                .spawn(move || pc_lane::wayland_lane_main(wayland, items_rx, rows, lane_shared, ready_tx, diagnostics))
                .context("spawn L2 Wayland lane thread")?;
            let (ready, lane, items_tx) = await_lane_ready("Wayland PC", ready_rx, lane, items_tx, timeout, release)?;
            pc_ready = Some(ready);
            let lane_index = L2Modality::Pc.index();
            senders[lane_index] = Some(items_tx);
            lane_threads[lane_index] = Some(lane);
        }
        let mut alsa_ready = None;
        if let (L2SinkKind::Alsa, Some(alsa)) = (config.sinks.haptic, config.alsa.clone()) {
            let diagnostics = alsa_diagnostics.take().flatten();
            let (items_tx, items_rx) = mpsc::sync_channel::<SinkMsg>(config.queue_capacity);
            let (ready_tx, ready_rx) = mpsc::channel();
            let rows = rows_tx.clone();
            let lane_shared = shared.clone();
            let timeout = micros(alsa.ready_timeout_us);
            let release = micros(alsa.release_timeout_us);
            let lane = std::thread::Builder::new()
                .name("l2-sink-haptic-alsa".into())
                .spawn(move || alsa_lane_main(alsa, items_rx, rows, lane_shared, ready_tx, diagnostics))
                .context("spawn L2 ALSA lane thread")?;
            let (ready, lane, items_tx) = match await_lane_ready("ALSA haptic", ready_rx, lane, items_tx, timeout, release) {
                Ok(ready) => ready,
                Err(error) => {
                    // Close the PC device lane that is already up before
                    // reporting: its queue goes, the lane disconnects.
                    let pc = L2Modality::Pc.index();
                    if let (Some(pc_tx), Some(pc_lane)) = (senders[pc].take(), lane_threads[pc].take()) {
                        drop(pc_tx);
                        let bound = config.wayland.as_ref().map_or(release, |w| micros(w.release_timeout_us));
                        let note = if join_within(pc_lane, bound) {
                            "the PC Wayland lane was closed and has disconnected"
                        } else {
                            "the PC Wayland lane has NOT exited within its release bound"
                        };
                        return Err(error.context(note));
                    }
                    return Err(error);
                }
            };
            alsa_ready = Some(ready);
            let lane_index = L2Modality::Haptic.index();
            senders[lane_index] = Some(items_tx);
            lane_threads[lane_index] = Some(lane);
        }

        writeln!(w, "{}", meta_row(&config, alsa_ready.as_ref(), pc_ready.as_ref()))?;
        w.flush()?;
        let writer = {
            let run_id = config.run_id.clone();
            let sinks = config.sinks;
            let device_output = config.device_output();
            let shared = shared.clone();
            let cap = config.max_tracked_objects;
            std::thread::Builder::new()
                .name("l2-writer".into())
                .spawn(move || writer_loop(w, rows_rx, run_id, sinks, device_output, shared, cap))
                .context("spawn L2 writer thread")?
        };
        for modality in L2Modality::ALL {
            if senders[modality.index()].is_some() {
                continue; // the device lane above
            }
            let (items_tx, items_rx) = mpsc::sync_channel::<SinkMsg>(config.queue_capacity);
            let rows = rows_tx.clone();
            let kind = config.sinks.of(modality);
            let fake = config.fake_lane(modality);
            let shared = shared.clone();
            let cap = config.max_tracked_objects;
            let lane = std::thread::Builder::new()
                .name(format!("l2-sink-{}", modality.as_str()))
                .spawn(move || sink_loop(kind, fake, items_rx, rows, shared, cap))
                .context("spawn L2 sink thread")?;
            senders[modality.index()] = Some(items_tx);
            lane_threads[modality.index()] = Some(lane);
        }
        let senders = senders.map(|s| s.expect("every lane has a queue"));
        let lanes: Vec<JoinHandle<()>> = lane_threads.into_iter().flatten().collect();
        let controls: [SyncSender<SinkMsg>; 2] = [senders[0].clone(), senders[1].clone()];
        let force_queue_full = L2Modality::ALL
            .map(|m| config.fake_lane(m).is_some_and(|f| f.force_queue_full));
        shared.producers.store(1, Ordering::Release);
        let submitter = L2Submitter {
            lanes: [senders[0].clone(), senders[1].clone()],
            rows: rows_tx.clone(),
            shared: shared.clone(),
            force_queue_full,
        };
        drop(senders);
        Ok(Self {
            config,
            path: path.to_path_buf(),
            file_identity,
            submitter: Some(submitter),
            rows: rows_tx,
            controls,
            shared,
            lanes,
            writer: Some(writer),
        })
    }

    pub fn submitter(&self) -> L2Submitter {
        self.submitter.clone().expect("the stage handle lives until finish")
    }

    /// A fault-only handle (not a producer).
    pub fn fault_stamp(&self) -> L2FaultStamp {
        L2FaultStamp { shared: self.shared.clone() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn file_identity(&self) -> (u64, u64) {
        self.file_identity
    }

    /// The sink of each modality.
    pub fn sinks(&self) -> L2Sinks {
        self.config.sinks
    }

    /// Record a fatal receiver error at `t_us`; the earliest is kept and
    /// written as the `receiver_fault` row before the sentinel. Non-blocking.
    pub fn receiver_fault(&self, t_us: u64, cause: &str) {
        self.shared.note_receiver_fault(t_us, cause);
    }

    pub fn receiver_fault_noted(&self) -> bool {
        self.shared.receiver_fault_noted()
    }

    /// Registration §3-5 order: publish the close deadline -> stop
    /// submissions -> every producer handle gone (bounded by the grace) ->
    /// terminals received strictly before the deadline -> every lane joined
    /// (a device sink has closed its device) -> `receiver_fault` row (if any)
    /// -> `instrumentation_end` -> flush/close. Blocking; call from
    /// `spawn_blocking` in async code.
    pub fn finish(mut self) -> Result<L2Summary> {
        // The deadline goes first and is independent of the message queue, so
        // a callback a lane handles before it sees `Closing` is still held to
        // it. `CLOSE_PENDING` covers the instant before the clock read.
        self.shared.close_deadline_us.store(CLOSE_PENDING, Ordering::SeqCst);
        let finish_start_us = now_us();
        let deadline_us = finish_start_us
            .saturating_add(self.config.report_window_us)
            .clamp(1, CLOSE_PENDING - 1);
        self.shared.finish_start_us.store(finish_start_us.max(1), Ordering::SeqCst);
        self.shared.close_deadline_us.store(deadline_us, Ordering::SeqCst);
        self.shared.closed.store(true, Ordering::Release);
        // Our own producer handle goes next, so the count the lanes check can
        // reach zero; then the non-producer control senders wake the lanes.
        // Blocking send: a lane is consuming, so space appears; a dead lane
        // makes this fail immediately instead of hanging.
        drop(self.submitter.take().expect("finish runs once"));
        for control in &self.controls {
            if control.send(SinkMsg::Closing).is_err() {
                self.shared.sink_gone.fetch_add(1, Ordering::Relaxed);
                self.shared.fail(FAIL_SINK_GONE);
            }
        }
        let mut panicked = false;
        for lane in self.lanes.drain(..) {
            panicked |= lane.join().is_err();
        }
        if panicked {
            bail!("L2 sink thread panicked");
        }
        self.rows
            .send(Row::End)
            .map_err(|_| anyhow::anyhow!("L2 writer thread is gone before the sentinel"))?;
        let writer = self.writer.take().expect("writer joined only once");
        match writer.join() {
            Ok(result) => result.context("write L2 log"),
            Err(_) => bail!("L2 writer thread panicked"),
        }
    }
}

/// Fake-sink pending outputs, earliest `t_output_us` first; equal instants
/// resolve in submission order. Bounded by `cap`.
struct FakePending {
    heap: BinaryHeap<Reverse<(u64, u64)>>,
    items: HashMap<u64, L2Submission>,
    order: u64,
    cap: usize,
}

impl FakePending {
    fn new(cap: usize) -> Self {
        Self { heap: BinaryHeap::new(), items: HashMap::new(), order: 0, cap }
    }

    /// `false` when the bound is reached (the output is never scheduled).
    fn push(&mut self, submission: L2Submission, t_output_us: u64) -> bool {
        if self.items.len() >= self.cap {
            return false;
        }
        self.heap.push(Reverse((t_output_us, self.order)));
        self.items.insert(self.order, submission);
        self.order += 1;
        true
    }

    fn next_due(&self) -> Option<u64> {
        self.heap.peek().map(|Reverse((t, _))| *t)
    }

    /// Emit every output whose instant has passed. The callback receipt time
    /// is a real clock read, and it is compared with the published close
    /// deadline read AFTER it: once `finish` has started, a callback is
    /// accepted only if its receipt time is strictly before the deadline, in
    /// whatever phase the sink is. The synthetic `t_output_us` is never what
    /// is compared. Returns `false` once the writer is gone.
    fn emit_due(&mut self, rows: &SyncSender<Row>, shared: &Shared) -> bool {
        while let Some(Reverse((t_output_us, key))) = self.heap.peek().copied() {
            let t_callback_us = now_us();
            if t_output_us > t_callback_us {
                break;
            }
            if shared.close_deadline().is_some_and(|deadline| t_callback_us >= deadline) {
                break;
            }
            self.heap.pop();
            let submission = self.items.remove(&key).expect("heap and map agree");
            let row = Row::Output { submission, t_output_us, t_callback_us, detail: OutputDetail::Fake };
            if rows.send(row).is_err() {
                return false;
            }
        }
        true
    }

    /// The next instant worth waking for: nothing once the deadline passed.
    fn next_wake(&self, shared: &Shared) -> Option<u64> {
        let due = self.next_due()?;
        match shared.close_deadline() {
            Some(deadline) if due >= deadline || now_us() >= deadline => None,
            _ => Some(due),
        }
    }
}

fn micros(us: u64) -> Duration {
    Duration::from_micros(us)
}

/// Wait for a device lane's ready report. On an error report the lane is
/// joined (it has closed its device). On a timeout (or a lane that exits
/// without reporting) its queue is closed — a late ready send then fails and
/// the lane stops and closes its stream; with the queue gone its loop exits —
/// and the lane is joined within `release`, so the device is released before
/// any next attempt; the error says whether that happened.
fn await_lane_ready<T>(
    label: &str,
    ready_rx: Receiver<std::result::Result<T, String>>,
    lane: JoinHandle<()>,
    items_tx: SyncSender<SinkMsg>,
    timeout: Duration,
    release: Duration,
) -> Result<(T, JoinHandle<()>, SyncSender<SinkMsg>)> {
    match ready_rx.recv_timeout(timeout) {
        Ok(Ok(ready)) => Ok((ready, lane, items_tx)),
        Ok(Err(error)) => {
            drop(items_tx);
            let _ = lane.join();
            bail!("L2 {label} sink failed to start: {error} (lane joined, device closed)");
        }
        Err(timeout_or_gone) => {
            drop(items_tx);
            drop(ready_rx);
            let released = join_within(lane, release);
            let what = match timeout_or_gone {
                RecvTimeoutError::Timeout => format!("not ready within {} us", timeout.as_micros()),
                RecvTimeoutError::Disconnected => "lane exited without reporting".to_string(),
            };
            if released {
                bail!("L2 {label} sink {what}; the lane thread has exited and the device is closed");
            }
            bail!(
                "L2 {label} sink {what}; the lane thread has NOT exited within a further {} us, so the \
                 device may stay open until this process exits: do not start another attempt before it has",
                release.as_micros()
            );
        }
    }
}

/// Join `thread` if it finishes within `bound`; `false` if it is still
/// running then (it stays detached).
fn join_within(thread: JoinHandle<()>, bound: Duration) -> bool {
    let start = std::time::Instant::now();
    while !thread.is_finished() {
        if start.elapsed() >= bound {
            return false;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    let _ = thread.join();
    true
}

fn sink_loop(
    kind: L2SinkKind,
    fake: Option<FakeSinkConfig>,
    items: Receiver<SinkMsg>,
    rows: SyncSender<Row>,
    shared: Arc<Shared>,
    cap: usize,
) {
    if let Some(gate) = fake.as_ref().and_then(|f| f.start_gate.clone()) {
        gate.wait();
    }
    let emit_outputs =
        kind == L2SinkKind::Fake && !fake.as_ref().is_some_and(|f| f.drop_terminals);
    let mut pending = FakePending::new(cap);
    // Accept one submission: the submit row first, then (fake) schedule its
    // synthetic output. Returns false once the writer is gone.
    let accept = |pending: &mut FakePending, submission: L2Submission| -> bool {
        if rows.send(Row::Submit(submission)).is_err() {
            return false;
        }
        if emit_outputs {
            let offset = fake
                .as_ref()
                .map(|f| f.offset_us(submission.identity.modality))
                .unwrap_or(0);
            if !pending.push(submission, submission.t_submit_us.saturating_add(offset)) {
                shared.fake_pending_overflow.fetch_add(1, Ordering::Relaxed);
                shared.fail_occurred(submission.t_submit_us, FAIL_FAKE_PENDING_OVERFLOW);
            }
        }
        true
    };
    // Take everything queued right now; false once the writer is gone.
    let drain_queue = |pending: &mut FakePending| -> bool {
        loop {
            match items.try_recv() {
                Ok(SinkMsg::Item(submission, _payload)) => {
                    if !accept(pending, submission) {
                        return false;
                    }
                }
                Ok(_) => {}
                Err(_) => return true,
            }
        }
    };

    // Phase 1: submissions open, until `finish` sends `Closing`. A disconnect
    // means the stage was dropped without `finish`: abandon (the writer then
    // ends without a sentinel — fail closed). The published deadline binds
    // every callback here as well.
    loop {
        let msg = match pending.next_wake(&shared) {
            Some(due) => match items.recv_timeout(micros(due.saturating_sub(now_us()))) {
                Ok(msg) => Some(msg),
                Err(RecvTimeoutError::Timeout) => None,
                Err(RecvTimeoutError::Disconnected) => return,
            },
            None => match items.recv() {
                Ok(msg) => Some(msg),
                Err(_) => return,
            },
        };
        match msg {
            Some(SinkMsg::Item(submission, _payload)) => {
                if !accept(&mut pending, submission) {
                    return;
                }
            }
            Some(SinkMsg::Closing) => break,
            Some(SinkMsg::Wake) | None => {}
        }
        if !pending.emit_due(&rows, &shared) {
            return;
        }
    }
    let deadline_us = shared
        .close_deadline()
        .expect("finish publishes the deadline before sending Closing");

    // Phase 2: closing. Every producer handle must be dropped and every
    // terminal received strictly before the deadline.
    let mut producers_gone = false;
    loop {
        if !producers_gone && shared.producers.load(Ordering::Acquire) == 0 {
            producers_gone = true;
        }
        // After reading zero, this drain sees every submission ever sent.
        if !drain_queue(&mut pending) || !pending.emit_due(&rows, &shared) {
            return;
        }
        let now = now_us();
        if now >= deadline_us {
            break;
        }
        let next = pending.next_due().filter(|due| *due < deadline_us);
        if producers_gone {
            match next {
                Some(due) => std::thread::sleep(micros(due.saturating_sub(now))),
                // Nothing left that can be received before the deadline: the
                // rest stays unresolved (no row).
                None => break,
            }
        } else {
            let until = next.map_or(deadline_us, |due| due.min(deadline_us));
            if let Ok(SinkMsg::Item(submission, _payload)) =
                items.recv_timeout(micros(until.saturating_sub(now)))
            {
                // Raced the close on the release side: still recorded.
                if !accept(&mut pending, submission) {
                    return;
                }
            }
        }
    }
    if !producers_gone {
        if shared.producers.load(Ordering::Acquire) > 0 {
            shared.producers_alive_at_finish.store(true, Ordering::Release);
            shared.fail(FAIL_PRODUCERS_ALIVE_AT_FINISH);
        }
        // Whatever is still queued gets its submit row (and stays unresolved).
        let _ = drain_queue(&mut pending);
    }
}

/// What the haptic device lane reports once its stream epoch has started.
struct LaneReady {
    params: PcmParams,
    epoch: EpochStart,
    prefill_frames: u64,
}

/// Open the configured stream INSIDE the lane thread: a device handle never
/// crosses threads.
fn open_haptic_pcm(alsa: &AlsaSinkConfig) -> std::result::Result<Box<dyn HapticPcm>, String> {
    match &alsa.backend {
        AlsaBackend::Device { name } => crate::l2_alsa::AlsaPcm::open(name, &alsa.placement)
            .map(|pcm| Box::new(pcm) as Box<dyn HapticPcm>),
        AlsaBackend::Simulated(sim) => Ok(Box::new(SimPcm::new(sim.clone()))),
    }
}

/// The haptic ALSA lane thread: open -> prefill -> start -> first snapshot
/// (the epoch) -> ready -> loop. Any failure before ready is reported to
/// `start`, which then refuses to run.
fn alsa_lane_main(
    alsa: AlsaSinkConfig,
    items: Receiver<SinkMsg>,
    rows: SyncSender<Row>,
    shared: Arc<Shared>,
    ready: mpsc::Sender<std::result::Result<LaneReady, String>>,
    diagnostics: Option<DiagLog>,
) {
    let fail = |message: String| {
        let _ = ready.send(Err(message));
    };
    let mut pcm = match open_haptic_pcm(&alsa) {
        Ok(pcm) => pcm,
        Err(error) => return fail(error),
    };
    let mut engine = match HapticPlacement::new(alsa.placement, pcm.params()) {
        Ok(engine) => engine,
        Err(error) => return fail(error),
    };
    let mut clock = now_us;
    if let Err(error) = engine.prefill(pcm.as_mut(), &mut clock) {
        return fail(format!("prefill: {error}"));
    }
    let t_start_us = now_us();
    if let Err(error) = pcm.start(t_start_us) {
        let _ = pcm.stop();
        return fail(format!("start: {error}"));
    }
    let t0 = now_us();
    let first = pcm.status(t0);
    let first = match first {
        Ok(first) => first,
        Err(error) => {
            let _ = pcm.stop();
            return fail(format!("first status: {error}"));
        }
    };
    if let Err(error) = engine.begin_epoch(t_start_us, first, t0) {
        let _ = pcm.stop();
        return fail(error);
    }
    let lane_ready = LaneReady {
        params: pcm.params().clone(),
        epoch: *engine.epoch().expect("the epoch has begun"),
        prefill_frames: engine.stats().prefill_frames,
    };
    if ready.send(Ok(lane_ready)).is_err() {
        // `start` gave up waiting.
        let _ = pcm.stop();
        return;
    }
    let mut lane = AlsaLane::new(pcm, engine, rows, shared);
    lane.diagnostics = diagnostics;
    lane.run(items, alsa.status_interval_us);
}

/// Bound on the records of the probe-only diagnostics file.
const DIAG_MAX_RECORDS: usize = 1_000_000;

/// The probe-only diagnostics file (registration 11-11): the raw snapshot
/// values (nanoseconds, and the audio timestamp in its own domain) that the
/// L2 JSONL must not carry. Buffered in memory by the lane and written only
/// after the stream is stopped, so it never delays the device. Never an
/// analysis time axis.
pub(crate) struct DiagLog {
    file: BufWriter<File>,
    run_id: String,
    outputs: Vec<(u32, u64, i64, RawSnapshot)>,
    dropped: u64,
}

impl DiagLog {
    fn note_output(&mut self, seq: u32, n_k: u64, hw_pos: i64, raw: RawSnapshot) {
        if self.outputs.len() < DIAG_MAX_RECORDS {
            self.outputs.push((seq, n_k, hw_pos, raw));
        } else {
            self.dropped += 1;
        }
    }

    /// Write everything (after the stream was stopped).
    fn write(mut self, epoch: Option<&EpochStart>, failure: Option<&DeviceFailure>) -> std::io::Result<(u64, u64)> {
        let header = serde_json::json!({
            "role": "diagnostics_meta",
            "diagnostics_schema": L2_ALSA_DIAGNOSTICS_SCHEMA,
            "l2_schema": L2_SCHEMA,
            "run_id": self.run_id,
            "analysis_time_axis": false,
            "purpose": "M2b device verification only (registration 11-11): raw ALSA snapshot values the L2 JSONL does not carry; never an analysis time axis, never joined to the ledger",
            "fields": {
                "htstamp_ns": { "unit": "ns", "clock": "CLOCK_MONOTONIC (sw tstamp_type MONOTONIC, read back)", "origin": "snd_pcm_status_get_htstamp" },
                "trigger_htstamp_ns": { "unit": "ns", "clock": "CLOCK_MONOTONIC", "origin": "snd_pcm_status_get_trigger_htstamp" },
                "driver_htstamp_ns": { "unit": "ns", "clock": "CLOCK_MONOTONIC", "origin": "snd_pcm_status_get_driver_htstamp" },
                "audio_htstamp_ns": { "unit": "ns", "clock": null, "domain": "per record audio_htstamp_domain, from the ACTUAL audio timestamp type of that snapshot", "origin": "snd_pcm_status_get_audio_htstamp" },
                "audio_tstamp_accuracy_ns": { "unit": "ns", "kind": "accuracy of the audio timestamp, not a time", "origin": "snd_pcm_audio_tstamp_report_t.accuracy (null unless accuracy_report)" },
                "kernel_trigger_ns": { "unit": "ns", "clock": "CLOCK_MONOTONIC", "origin": "trigger timestamp reported with a failed state; meaning unverified, not an occurrence time" },
            },
        });
        writeln!(self.file, "{header}")?;
        if let Some(epoch) = epoch {
            let row = serde_json::json!({
                "role": "stream_epoch",
                "t_start_us": epoch.t_start_us,
                "first_status_t0_us": epoch.first_status_t0_us,
                "first_status_raw": RawSnapshot::of(&epoch.first_status).to_diag_json(),
            });
            writeln!(self.file, "{row}")?;
        }
        for (seq, n_k, hw_pos, raw) in &self.outputs {
            let row = serde_json::json!({
                "role": "output_diag", "seq": seq, "n_k": n_k, "hw_pos": hw_pos, "raw": raw.to_diag_json(),
            });
            writeln!(self.file, "{row}")?;
        }
        if let Some(failure) = failure {
            let raw: serde_json::Map<String, serde_json::Value> =
                failure.raw_ns.iter().map(|(name, value)| ((*name).to_string(), (*value).into())).collect();
            let row = serde_json::json!({
                "role": "device_failure", "reason": failure.reason, "kernel_trigger_ns": failure.kernel_trigger_ns,
                "raw_ns": raw,
            });
            writeln!(self.file, "{row}")?;
        }
        let records = self.outputs.len() as u64;
        let end = serde_json::json!({ "role": "diagnostics_end", "output_records": records, "dropped_records": self.dropped });
        writeln!(self.file, "{end}")?;
        self.file.flush()?;
        Ok((records, self.dropped))
    }
}

/// The running haptic ALSA lane: the device, the placement engine and the
/// row hand-off. Rows go to the writer with `try_send`: a logging stall must
/// never become a PCM underrun (a full row queue is `row_queue_overflow`).
struct AlsaLane {
    pcm: Box<dyn HapticPcm>,
    engine: HapticPlacement,
    rows: SyncSender<Row>,
    shared: Arc<Shared>,
    /// `avail` of this iteration's good snapshot (consumed by `fill`).
    avail: Option<u64>,
    failed: Option<DeviceFailure>,
    stopped: bool,
    stop_error: Option<String>,
    outputs_after_deadline: u64,
    discards_not_emitted_after_deadline: u64,
    device_failure_discards: u64,
    payload_invalid_discards: u64,
    queue_full_discards: u64,
    row_overflow: u64,
    writer_gone: bool,
    /// Probe-only raw diagnostics (never set by the receiver).
    diagnostics: Option<DiagLog>,
    /// Filled when the lane closes: was the latest tick fully played?
    closed_unplayed_tick_frames: Option<u64>,
    diagnostics_result: Option<std::result::Result<(u64, u64), String>>,
}

impl AlsaLane {
    fn new(pcm: Box<dyn HapticPcm>, engine: HapticPlacement, rows: SyncSender<Row>, shared: Arc<Shared>) -> Self {
        Self {
            pcm,
            engine,
            rows,
            shared,
            avail: None,
            failed: None,
            stopped: false,
            stop_error: None,
            outputs_after_deadline: 0,
            discards_not_emitted_after_deadline: 0,
            device_failure_discards: 0,
            payload_invalid_discards: 0,
            queue_full_discards: 0,
            row_overflow: 0,
            writer_gone: false,
            diagnostics: None,
            closed_unplayed_tick_frames: None,
            diagnostics_result: None,
        }
    }

    fn row(&mut self, row: Row, event_us: u64) {
        match self.rows.try_send(row) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.row_overflow += 1;
                self.shared.row_overflow.fetch_add(1, Ordering::Relaxed);
                self.shared.fail_occurred(event_us, FAIL_ROW_QUEUE_OVERFLOW);
            }
            Err(TrySendError::Disconnected(_)) => self.writer_gone = true,
        }
    }

    /// An after-submit discard, emitted only strictly before the close
    /// deadline (otherwise the submission stays unresolved).
    fn discard_after(&mut self, s: L2Submission, reason: &'static str) {
        let t_discard_us = now_us().max(s.t_submit_us);
        if self.shared.close_deadline().is_some_and(|deadline| t_discard_us >= deadline) {
            self.discards_not_emitted_after_deadline += 1;
            return;
        }
        let row = Row::Discard {
            identity: s.identity,
            t_release_us: s.t_release_us,
            t_submit_us: Some(s.t_submit_us),
            t_discard_us,
            reason,
            decode_us: None,
        };
        self.row(row, t_discard_us);
    }

    fn accept(&mut self, s: L2Submission, payload: Option<bytes::Bytes>) {
        if self.failed.is_some() {
            // The device is gone: still a submission, terminated at once.
            self.row(Row::Submit(s), s.t_submit_us);
            self.device_failure_discards += 1;
            self.discard_after(s, DISCARD_DEVICE_FAILURE);
            return;
        }
        match self.engine.admit(s, payload.as_deref()) {
            Admission::Queued => self.row(Row::Submit(s), s.t_submit_us),
            Admission::QueueFull => {
                let t_discard_us = now_us().max(s.t_release_us);
                self.queue_full_discards += 1;
                let row = Row::Discard {
                    identity: s.identity,
                    t_release_us: s.t_release_us,
                    t_submit_us: None,
                    t_discard_us,
                    reason: DISCARD_OUTPUT_QUEUE_FULL,
                    decode_us: None,
                };
                self.row(row, t_discard_us);
            }
            Admission::PayloadInvalid => {
                self.row(Row::Submit(s), s.t_submit_us);
                self.shared.fail_occurred(s.t_submit_us, FAIL_HAPTIC_PAYLOAD_INVALID);
                self.payload_invalid_discards += 1;
                self.discard_after(s, DISCARD_PAYLOAD_INVALID);
            }
        }
    }

    fn handle(&mut self, msg: SinkMsg, closing: &mut bool) {
        match msg {
            SinkMsg::Item(s, payload) => self.accept(s, payload),
            SinkMsg::Closing => *closing = true,
            SinkMsg::Wake => {}
        }
    }

    /// Take everything queued right now; `false` once the stage is gone.
    fn drain(&mut self, items: &Receiver<SinkMsg>, closing: &mut bool) -> bool {
        loop {
            match items.try_recv() {
                Ok(msg) => self.handle(msg, closing),
                Err(mpsc::TryRecvError::Empty) => return true,
                Err(mpsc::TryRecvError::Disconnected) => return false,
            }
        }
    }

    /// One status snapshot: validate it and emit every output it shows,
    /// each only if received strictly before the close deadline.
    fn poll(&mut self) {
        self.avail = None;
        if self.failed.is_some() {
            return;
        }
        let t0 = now_us();
        let status = self.pcm.status(t0);
        let t1 = now_us();
        let snapshot = match status {
            Ok(snapshot) => snapshot,
            Err(error) => {
                let failure = self.engine.failure_from_error(&error, None, t1, "status");
                return self.fail(failure);
            }
        };
        match self.engine.observe(&snapshot, t0, t1) {
            Err(failure) => self.fail(failure),
            Ok(placed) => {
                let deadline = self.shared.close_deadline();
                for p in placed {
                    if deadline.is_some_and(|deadline| p.t_callback_us >= deadline) {
                        // Not accepted: it stays unresolved.
                        self.outputs_after_deadline += 1;
                        continue;
                    }
                    let t = p.t_callback_us;
                    if let Some(diag) = self.diagnostics.as_mut() {
                        diag.note_output(p.submission.identity.seq, p.detail.n_k, p.detail.hw_pos, p.detail.raw);
                    }
                    let row = Row::Output {
                        submission: p.submission,
                        t_output_us: p.t_output_us,
                        t_callback_us: p.t_callback_us,
                        detail: OutputDetail::Alsa(Box::new(p.detail)),
                    };
                    self.row(row, t);
                }
                self.avail = Some(snapshot.avail);
            }
        }
    }

    /// Write what this iteration's snapshot allows.
    fn fill(&mut self) {
        let Some(avail) = self.avail.take() else { return };
        if self.failed.is_some() {
            return;
        }
        let mut clock = now_us;
        if let Err(error) = self.engine.fill(self.pcm.as_mut(), avail, &mut clock) {
            let detected = now_us();
            // The kernel's view right after the error (an XRUN's trigger time).
            let after = self.pcm.status(detected).ok();
            let failure = self.engine.failure_from_error(&error, after.as_ref(), detected, "writei");
            self.fail(failure);
        }
    }

    /// A device failure: recorded with its occurrence and detection times,
    /// the stream stopped (never recovered), and every submission that can
    /// no longer be output terminated as `device_failure`.
    fn fail(&mut self, failure: DeviceFailure) {
        if self.failed.is_some() {
            return;
        }
        // Registration 6-1 (Codex 109 P1): no occurrence time is claimed
        // for a device failure until the kernel timestamps are verified on
        // the device; the sentinel then has first_integrity_failure_us null
        // and first_failure_time_determinable false. The interval, the
        // detection and the raw trigger stay in lanes.haptic.device_failure.
        self.shared.fail_untimed_detected(failure.detected_us, failure.reason);
        self.stop_stream();
        for s in self.engine.abandon() {
            self.device_failure_discards += 1;
            self.discard_after(s, DISCARD_DEVICE_FAILURE);
        }
        self.failed = Some(failure);
    }

    /// Close statistics: frames of the latest tick that started being written
    /// (its full extent, also when only partly written) not yet past
    /// `hw_pos` in the last healthy snapshot before the stream is dropped —
    /// an upper bound (after a device failure nothing more can be played).
    fn record_close(&mut self) {
        self.closed_unplayed_tick_frames = Some(self.engine.unplayed_tick_frames());
    }

    fn stop_stream(&mut self) {
        if !self.stopped {
            self.stopped = true;
            if let Err(error) = self.pcm.stop() {
                self.stop_error = Some(error.to_string());
            }
        }
    }

    fn stats(&self) -> serde_json::Value {
        let mut stats = self.engine.stats().to_json();
        let map = stats.as_object_mut().expect("lane stats are an object");
        map.insert("kind".into(), "alsa".into());
        map.insert("backend".into(), self.pcm.params().backend.into());
        map.insert("outputs_after_deadline".into(), self.outputs_after_deadline.into());
        map.insert(
            "discards_not_emitted_after_deadline".into(),
            self.discards_not_emitted_after_deadline.into(),
        );
        map.insert("device_failure_discards".into(), self.device_failure_discards.into());
        map.insert("payload_invalid_discards".into(), self.payload_invalid_discards.into());
        map.insert("queue_full_discards".into(), self.queue_full_discards.into());
        map.insert("row_overflow".into(), self.row_overflow.into());
        map.insert(
            "device_failure".into(),
            self.failed.as_ref().map_or(serde_json::Value::Null, DeviceFailure::to_json),
        );
        // Registration 3-3a "종료": was the latest tick played to its end
        // (n_k + frames passed hw_pos) before the stream was dropped?
        map.insert(
            "tail_played_at_close".into(),
            self.closed_unplayed_tick_frames.map(|unplayed| unplayed == 0).into(),
        );
        map.insert("unplayed_tick_frames_at_close".into(), self.closed_unplayed_tick_frames.into());
        map.insert(
            "diagnostics".into(),
            match &self.diagnostics_result {
                None => serde_json::Value::Null,
                Some(Ok((records, dropped))) => {
                    serde_json::json!({ "written": true, "output_records": records, "dropped_records": dropped })
                }
                Some(Err(error)) => serde_json::json!({ "written": false, "error": error }),
            },
        );
        map.insert("stop_error".into(), self.stop_error.clone().into());
        stats
    }

    /// The lane loop. Each iteration: status -> observe/emit -> accept
    /// everything queued -> fill (ticks, silence) -> close checks -> wait for
    /// the next status time or an arrival. A tick written in one iteration
    /// is first observable in the next snapshot. Close protocol as the
    /// null/fake lanes: after `Closing`, every producer must go and every
    /// terminal be received strictly before the deadline; the stream keeps
    /// being fed (silence) meanwhile, then is dropped.
    fn run(mut self, items: Receiver<SinkMsg>, interval_us: u64) {
        let mut closing = false;
        let mut producers_gone = false;
        let mut next_poll = now_us();
        loop {
            self.poll();
            if !self.drain(&items, &mut closing) || self.writer_gone {
                // Dropped without `finish` (or the writer died): fail closed,
                // the writer ends without a sentinel.
                self.stop_stream();
                return;
            }
            self.fill();
            let now = now_us();
            let mut deadline = None;
            if closing {
                let deadline_us = self
                    .shared
                    .close_deadline()
                    .expect("finish publishes the deadline before Closing");
                deadline = Some(deadline_us);
                if !producers_gone && self.shared.producers.load(Ordering::Acquire) == 0 {
                    producers_gone = true;
                    // After reading zero, this drain sees every submission ever sent.
                    if !self.drain(&items, &mut closing) {
                        self.stop_stream();
                        return;
                    }
                }
                if now >= deadline_us {
                    break;
                }
                if producers_gone && (self.failed.is_some() || self.engine.idle()) {
                    break;
                }
            }
            if now >= next_poll {
                next_poll = now.saturating_add(interval_us);
            }
            let until = deadline.map_or(next_poll, |d| next_poll.min(d));
            match items.recv_timeout(micros(until.saturating_sub(now_us()))) {
                Ok(msg) => self.handle(msg, &mut closing),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    self.stop_stream();
                    return;
                }
            }
        }
        if !producers_gone {
            if self.shared.producers.load(Ordering::Acquire) > 0 {
                self.shared.producers_alive_at_finish.store(true, Ordering::Release);
                self.shared.fail(FAIL_PRODUCERS_ALIVE_AT_FINISH);
            }
            // Whatever is still queued gets its submit row (and stays unresolved).
            let mut closing = true;
            let _ = self.drain(&items, &mut closing);
        }
        self.record_close();
        self.stop_stream();
        // The stream is stopped: the diagnostics file and a blocking hand-off
        // of the counters cannot delay the device any more.
        if let Some(diag) = self.diagnostics.take() {
            self.diagnostics_result = Some(
                diag.write(self.engine.epoch(), self.failed.as_ref()).map_err(|e| e.to_string()),
            );
        }
        let stats = self.stats();
        let _ = self.rows.send(Row::LaneStats { modality: L2Modality::Haptic, stats });
    }
}

fn identity_json(run_id: &str, id: &L2Identity) -> serde_json::Map<String, serde_json::Value> {
    let mut m = serde_json::Map::new();
    m.insert("run_id".into(), run_id.into());
    m.insert("track".into(), id.modality.as_str().into());
    m.insert("tier".into(), id.tier.into());
    m.insert("seq".into(), id.seq.into());
    m.insert("pts_us".into(), id.pts_us.into());
    m.insert("event_id".into(), id.event_id.into());
    if let Some((wire_track, generation)) = id.route {
        m.insert("wire_track".into(), wire_track.into());
        m.insert("route_generation".into(), generation.into());
    }
    m
}

/// Per-object bookkeeping of the writer, bounded by `cap`.
struct Tracking {
    /// Logical events ever released (submitted or discarded before submit)
    /// with their two earliest `t_release_us`; the second one is when a
    /// duplicate occurred, whatever order the two paths delivered them in.
    seen: HashMap<(L2Modality, u32), (u64, Option<u64>)>,
    /// Open submits per identity, as their `t_submit_us` (a list, so a
    /// duplicate can neither hide nor lose its time).
    open: HashMap<L2Identity, Vec<u64>>,
    open_stored: usize,
    /// Open submits per modality ([`L2Modality::index`]), stored or not.
    open_total: [u64; 2],
    cap: usize,
}

impl Tracking {
    fn new(cap: usize) -> Self {
        Self { seen: HashMap::new(), open: HashMap::new(), open_stored: 0, open_total: [0, 0], cap }
    }

    /// One release of `identity` at `t_release_us`; `attempt_us` is the
    /// submission/discard attempt time used if the bound is exceeded.
    fn release(
        &mut self,
        identity: &L2Identity,
        t_release_us: u64,
        attempt_us: u64,
        summary: &mut L2Summary,
        shared: &Shared,
    ) {
        let key = identity.logical();
        if let Some((first, second)) = self.seen.get_mut(&key) {
            summary.duplicate_logical_release += 1;
            if t_release_us < *first {
                *second = Some(*first);
                *first = t_release_us;
            } else if second.is_none_or(|s| t_release_us < s) {
                *second = Some(t_release_us);
            }
            let occurred = second.expect("two releases are known");
            shared.fail_occurred(occurred, FAIL_DUPLICATE_LOGICAL_RELEASE);
        } else if self.seen.len() >= self.cap {
            summary.tracking_overflow += 1;
            shared.fail_occurred(attempt_us, FAIL_TRACKING_CAPACITY);
        } else {
            self.seen.insert(key, (t_release_us, None));
        }
    }

    fn open(&mut self, s: &L2Submission, summary: &mut L2Summary, shared: &Shared) {
        self.open_total[s.identity.modality.index()] += 1;
        if self.open_stored >= self.cap {
            summary.tracking_overflow += 1;
            shared.fail_occurred(s.t_submit_us, FAIL_TRACKING_CAPACITY);
            return;
        }
        self.open.entry(s.identity).or_default().push(s.t_submit_us);
        self.open_stored += 1;
    }

    /// A terminal for `identity`, received at `terminal_us`.
    fn close(&mut self, identity: &L2Identity, terminal_us: u64, summary: &mut L2Summary, shared: &Shared) {
        let Some(times) = self.open.get_mut(identity) else {
            summary.terminal_without_open_submit += 1;
            shared.fail_occurred(terminal_us, FAIL_TERMINAL_WITHOUT_OPEN_SUBMIT);
            return;
        };
        let earliest = times
            .iter()
            .enumerate()
            .min_by_key(|(_, t)| **t)
            .map(|(i, _)| i)
            .expect("stored lists are never empty");
        times.swap_remove(earliest);
        if times.is_empty() {
            self.open.remove(identity);
        }
        self.open_stored -= 1;
        self.open_total[identity.modality.index()] -= 1;
    }

    /// Earliest stored unresolved `t_submit_us` among the modalities `keep`
    /// selects (`None` if every such unresolved submit was beyond the bound).
    fn earliest_unresolved_submit(&self, keep: impl Fn(L2Modality) -> bool) -> Option<u64> {
        self.open
            .iter()
            .filter(|(identity, _)| keep(identity.modality))
            .flat_map(|(_, times)| times.iter())
            .min()
            .copied()
    }
}

/// Writer cross-check of the close rule (v1.2): a terminal whose receipt time
/// is not strictly before the published deadline. The sinks refuse such a
/// terminal themselves, so this never fires unless a sink is wrong.
fn check_terminal_receipt(receipt_us: u64, summary: &mut L2Summary, shared: &Shared) {
    if shared.close_deadline().is_some_and(|deadline| receipt_us >= deadline) {
        summary.terminal_at_or_after_deadline += 1;
        shared.fail_occurred(receipt_us, FAIL_TERMINAL_AT_OR_AFTER_DEADLINE);
    }
}

/// Write one row: `t_log_arrival_us` (and `t_us`, unless the row carries its
/// own event time) is the writer clock immediately before the write; the write
/// lag is measured after the flush completes.
fn write_row(
    w: &mut BufWriter<File>,
    mut row: serde_json::Map<String, serde_json::Value>,
    event_t_us: Option<u64>,
    summary: &mut L2Summary,
) -> std::io::Result<u64> {
    let t_log_arrival_us = now_us();
    row.insert("t_us".into(), event_t_us.unwrap_or(t_log_arrival_us).into());
    row.insert("t_log_arrival_us".into(), t_log_arrival_us.into());
    writeln!(w, "{}", serde_json::Value::Object(row))?;
    w.flush()?;
    let lag = now_us().saturating_sub(t_log_arrival_us);
    summary.max_write_lag_us = summary.max_write_lag_us.max(lag);
    Ok(t_log_arrival_us)
}

fn writer_loop(
    mut w: BufWriter<File>,
    rows: Receiver<Row>,
    run_id: String,
    sinks: L2Sinks,
    device_output: bool,
    shared: Arc<Shared>,
    cap: usize,
) -> std::io::Result<L2Summary> {
    let mut summary = L2Summary::default();
    let mut tracking = Tracking::new(cap);
    let mut ended = false;
    // Device lanes' counters, for the sentinel (`lanes.<track>`).
    let mut lane_stats: [Option<serde_json::Value>; 2] = [None, None];
    let mut handle = |row: Row, w: &mut BufWriter<File>, summary: &mut L2Summary| -> std::io::Result<()> {
        match row {
            Row::Submit(s) => {
                summary.submitted += 1;
                tracking.release(&s.identity, s.t_release_us, s.t_submit_us, summary, &shared);
                tracking.open(&s, summary, &shared);
                let mut m = identity_json(&run_id, &s.identity);
                m.insert("role".into(), "submit".into());
                m.insert("t_release_us".into(), s.t_release_us.into());
                m.insert("t_submit_us".into(), s.t_submit_us.into());
                write_row(w, m, None, summary)?;
            }
            Row::Output { submission: s, t_output_us, t_callback_us, detail } => {
                summary.output += 1;
                tracking.close(&s.identity, t_callback_us, summary, &shared);
                let mut m = identity_json(&run_id, &s.identity);
                m.insert("role".into(), "output".into());
                m.insert("t_release_us".into(), s.t_release_us.into());
                m.insert("t_submit_us".into(), s.t_submit_us.into());
                m.insert("t_output_us".into(), t_output_us.into());
                m.insert("t_callback_us".into(), t_callback_us.into());
                // v1.4: Draco decode times on the row itself (draco only).
                if let Some((start, end)) = detail.decode_us() {
                    m.insert("t_decode_start_us".into(), start.into());
                    m.insert("t_decode_end_us".into(), end.into());
                }
                // Modality-specific device evidence; its schema per sink kind
                // is `sink_contracts.<track>.output_detail` in the meta row.
                m.insert("output_detail".into(), detail.to_json(s.identity.modality));
                let t_log = write_row(w, m, None, summary)?;
                summary.max_callback_to_log_us =
                    summary.max_callback_to_log_us.max(t_log.saturating_sub(t_callback_us));
                // Order rule, checked on the values as written (never clamped).
                // A PC Wayland row adds its pipeline: submit <= decode start
                // <= decode end <= render start <= render end <= commit <=
                // output (v1.4).
                let pipeline_ordered = match &detail {
                    OutputDetail::Wayland(d) => {
                        let mut chain = vec![s.t_submit_us];
                        if let Some((start, end)) = d.timing.decode_us {
                            chain.extend([start, end]);
                        }
                        chain.extend([d.timing.t_render_start_us, d.timing.t_render_end_us, d.t_commit_us, t_output_us]);
                        chain.windows(2).all(|w| w[0] <= w[1])
                    }
                    _ => true,
                };
                let ordered = pipeline_ordered
                    && s.t_release_us <= s.t_submit_us
                    && s.t_submit_us <= t_output_us
                    && t_output_us <= t_callback_us
                    && t_callback_us <= t_log;
                if !ordered {
                    summary.output_order_violation += 1;
                    shared.fail_occurred(t_callback_us, FAIL_OUTPUT_ORDER);
                }
                check_terminal_receipt(t_callback_us, summary, &shared);
            }
            Row::Discard { identity, t_release_us, t_submit_us, t_discard_us, reason, decode_us } => {
                match t_submit_us {
                    None => {
                        summary.discarded_before_submit += 1;
                        tracking.release(&identity, t_release_us, t_discard_us, summary, &shared);
                    }
                    Some(_) => {
                        summary.discarded_after_submit += 1;
                        tracking.close(&identity, t_discard_us, summary, &shared);
                        check_terminal_receipt(t_discard_us, summary, &shared);
                    }
                }
                let mut m = identity_json(&run_id, &identity);
                m.insert("role".into(), "explicit_discard".into());
                m.insert("reason".into(), reason.into());
                m.insert(
                    "discard_phase".into(),
                    if t_submit_us.is_some() { "after_submit" } else { "before_submit" }.into(),
                );
                m.insert("t_release_us".into(), t_release_us.into());
                m.insert("t_submit_us".into(), t_submit_us.into());
                m.insert("t_discard_us".into(), t_discard_us.into());
                // v1.4: a decoded (draco) PC frame keeps its decode times.
                if let Some((start, end)) = decode_us {
                    m.insert("t_decode_start_us".into(), start.into());
                    m.insert("t_decode_end_us".into(), end.into());
                }
                write_row(w, m, None, summary)?;
            }
            Row::LaneStats { modality, stats } => lane_stats[modality.index()] = Some(stats),
            Row::End => unreachable!("End is handled by the loop"),
        }
        Ok(())
    };
    while let Ok(row) = rows.recv() {
        if matches!(row, Row::End) {
            ended = true;
            break;
        }
        handle(row, &mut w, &mut summary)?;
    }
    if !ended {
        // Every sender vanished without `finish`: fail closed, no sentinel.
        w.flush()?;
        return Err(std::io::Error::other("L2 writer ended without finish"));
    }
    // Rows sent after `End` can only come from a producer that outlived
    // `finish`, which the sink already recorded as an integrity failure; write
    // what is there before the sentinel.
    while let Ok(row) = rows.try_recv() {
        if !matches!(row, Row::End) {
            handle(row, &mut w, &mut summary)?;
        }
    }
    summary.submitted_unresolved_by_track = tracking.open_total;
    summary.submitted_unresolved = tracking.open_total.iter().sum();
    summary.conservation_ok = summary.submitted
        == summary.output + summary.discarded_after_submit + summary.submitted_unresolved;
    // Only tracks whose sink resolves owe a terminal (a null lane never does).
    let resolving = |m: L2Modality| sinks.of(m).resolves();
    let unresolved_owed: u64 = L2Modality::ALL
        .iter()
        .filter(|m| resolving(**m))
        .map(|m| tracking.open_total[m.index()])
        .sum();
    if unresolved_owed > 0 {
        match tracking.earliest_unresolved_submit(resolving) {
            Some(t_submit_us) => shared.fail_occurred(t_submit_us, FAIL_SUBMITTED_UNRESOLVED),
            // Every such unresolved submit was beyond the tracking bound.
            None => shared.fail_untimed(FAIL_SUBMITTED_UNRESOLVED),
        }
    }
    if !summary.conservation_ok
        && summary.terminal_without_open_submit == 0
        && summary.tracking_overflow == 0
    {
        // Not explained by any timed failure.
        shared.fail_untimed(FAIL_CONSERVATION);
    }
    summary.row_overflow = shared.row_overflow.load(Ordering::Relaxed);
    summary.submit_after_close = shared.submit_after_close.load(Ordering::Relaxed);
    summary.sink_gone = shared.sink_gone.load(Ordering::Relaxed);
    summary.fake_pending_overflow = shared.fake_pending_overflow.load(Ordering::Relaxed);
    summary.producers_alive_at_finish = shared.producers_alive_at_finish.load(Ordering::Acquire);
    let failures = shared.failures();
    match (failures.untimed, failures.timed) {
        (Some((detected, reason)), _) => {
            summary.first_integrity_failure_us = None;
            summary.first_integrity_failure_reason = Some(reason.to_string());
            summary.first_integrity_failure_detected_us = Some(detected);
            summary.first_failure_time_determinable = false;
        }
        (None, Some((occurred, detected, reason))) => {
            summary.first_integrity_failure_us = Some(occurred);
            summary.first_integrity_failure_reason = Some(reason.to_string());
            summary.first_integrity_failure_detected_us = Some(detected);
            summary.first_failure_time_determinable = true;
        }
        (None, None) => summary.first_failure_time_determinable = true,
    }
    summary.receiver_fault = shared.receiver_fault.lock().unwrap_or_else(|p| p.into_inner()).clone();
    if let Some((t_us, cause)) = &summary.receiver_fault {
        let mut m = serde_json::Map::new();
        m.insert("role".into(), "receiver_fault".into());
        m.insert("run_id".into(), run_id.clone().into());
        m.insert("cause".into(), cause.clone().into());
        write_row(&mut w, m, Some(*t_us), &mut summary)?;
    }
    let integrity_ok = summary.integrity_ok(sinks);
    let t_end = now_us();
    let (fault_us, fault_cause) = match &summary.receiver_fault {
        Some((t, c)) => (serde_json::json!(t), serde_json::json!(c)),
        None => (serde_json::Value::Null, serde_json::Value::Null),
    };
    // `finish` published both before it sent `End`; `None` is impossible in
    // a file that reaches this point, but is reported as null, not invented.
    summary.finish_start_us = match shared.finish_start_us.load(Ordering::SeqCst) {
        0 => None,
        t => Some(t),
    };
    summary.close_deadline_us = shared.close_deadline();
    let sentinel = serde_json::json!({
        "role": "instrumentation_end",
        "run_id": run_id,
        "sinks": { "pc": sinks.pc.as_str(), "haptic": sinks.haptic.as_str() },
        "device_output": device_output,
        "t_us": t_end,
        "end_us": t_end,
        "t_log_arrival_us": t_end,
        "submitted": summary.submitted,
        "output": summary.output,
        "discarded_before_submit": summary.discarded_before_submit,
        "discarded_after_submit": summary.discarded_after_submit,
        "submitted_unresolved": summary.submitted_unresolved,
        "submitted_unresolved_by_track": {
            "pc": summary.submitted_unresolved_by_track[L2Modality::Pc.index()],
            "haptic": summary.submitted_unresolved_by_track[L2Modality::Haptic.index()],
        },
        "conservation_ok": summary.conservation_ok,
        "duplicate_logical_release": summary.duplicate_logical_release,
        "terminal_without_open_submit": summary.terminal_without_open_submit,
        "tracking_overflow": summary.tracking_overflow,
        "fake_pending_overflow": summary.fake_pending_overflow,
        "row_overflow": summary.row_overflow,
        "submit_after_close": summary.submit_after_close,
        "sink_gone": summary.sink_gone,
        "producers_alive_at_finish": summary.producers_alive_at_finish,
        "output_order_violation": summary.output_order_violation,
        "terminal_at_or_after_deadline": summary.terminal_at_or_after_deadline,
        "max_write_lag_us": summary.max_write_lag_us,
        "max_callback_to_log_us": summary.max_callback_to_log_us,
        "report_window_anchor": REPORT_WINDOW_ANCHOR,
        "finish_start_us": summary.finish_start_us,
        "close_deadline_us": summary.close_deadline_us,
        "lanes": {
            "pc": lane_stats[L2Modality::Pc.index()].take(),
            "haptic": lane_stats[L2Modality::Haptic.index()].take(),
        },
        "first_integrity_failure_us": summary.first_integrity_failure_us,
        "first_integrity_failure_reason": summary.first_integrity_failure_reason,
        "first_integrity_failure_detected_us": summary.first_integrity_failure_detected_us,
        "first_failure_time_determinable": summary.first_failure_time_determinable,
        "receiver_fault_us": fault_us,
        "receiver_fault_cause": fault_cause,
        "integrity_ok": integrity_ok,
    });
    writeln!(w, "{sentinel}")?;
    w.flush()?;
    drop(w);
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("skew-l2-{}-{}-{name}.jsonl", std::process::id(), now_us()))
    }

    fn config(sink: L2SinkKind, fake: Option<FakeSinkConfig>, capacity: usize) -> L2Config {
        config_sinks(L2Sinks::both(sink), fake, capacity)
    }

    fn config_sinks(sinks: L2Sinks, fake: Option<FakeSinkConfig>, capacity: usize) -> L2Config {
        L2Config {
            run_id: "l2-test".into(),
            method: "s1".into(),
            sinks,
            fake,
            alsa: None,
            wayland: None,
            queue_capacity: capacity,
            row_capacity: 4096,
            report_window_us: 500_000,
            max_tracked_objects: 4096,
        }
    }

    fn id(modality: L2Modality, seq: u32) -> L2Identity {
        L2Identity {
            modality,
            tier: if modality == L2Modality::Pc { 2 } else { 0 },
            seq,
            pts_us: seq as u64 * 11_111,
            event_id: seq,
            route: None,
        }
    }

    fn rows(path: &Path) -> Vec<serde_json::Value> {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    fn key(r: &serde_json::Value) -> String {
        format!("{}/{}/{}/{}/{}", r["track"], r["tier"], r["seq"], r["pts_us"], r["event_id"])
    }

    fn sentinel(rows: &[serde_json::Value]) -> &serde_json::Value {
        let last = rows.last().unwrap();
        assert_eq!(last["role"], "instrumentation_end");
        assert_eq!(rows.iter().filter(|r| r["role"] == "instrumentation_end").count(), 1);
        last
    }

    /// Replay rows straight through the writer (Codex 104 harness shape).
    fn replay(path: &Path, sink: L2SinkKind, input: Vec<Row>, writer_delay: Option<Duration>) -> L2Summary {
        replay_with(path, sink, input, writer_delay, Arc::new(Shared::default()))
    }

    fn replay_with(
        path: &Path,
        sink: L2SinkKind,
        input: Vec<Row>,
        writer_delay: Option<Duration>,
        shared: Arc<Shared>,
    ) -> L2Summary {
        // Room for every row plus `End`: the writer runs after the sends.
        let (tx, rx) = mpsc::sync_channel(input.len() + 1);
        for row in input {
            tx.send(row).unwrap();
        }
        tx.send(Row::End).unwrap();
        drop(tx);
        if let Some(delay) = writer_delay {
            std::thread::sleep(delay);
        }
        let file = OpenOptions::new().write(true).create_new(true).open(path).unwrap();
        writer_loop(BufWriter::new(file), rx, "l2-test".into(), L2Sinks::both(sink), false, shared, 64)
            .unwrap()
    }

    fn submission(identity: L2Identity) -> L2Submission {
        let t = now_us();
        L2Submission { identity, t_release_us: t, t_submit_us: t }
    }

    fn output_row(s: L2Submission) -> Row {
        Row::Output { submission: s, t_output_us: s.t_submit_us, t_callback_us: now_us(), detail: OutputDetail::Fake }
    }

    #[test]
    fn fake_sink_gives_each_submission_one_submit_and_one_output_and_ends_with_the_sentinel() {
        let path = tmp("fake-one-terminal");
        let out = L2Output::start(
            &path,
            config(L2SinkKind::Fake, Some(FakeSinkConfig::offsets(3_000, 1_000)), 64),
        )
        .unwrap();
        let s = out.submitter();
        let mut submitted = Vec::new();
        for seq in 0..20 {
            let modality = if seq % 4 == 0 { L2Modality::Pc } else { L2Modality::Haptic };
            let identity = id(modality, seq);
            assert_eq!(s.submit(identity, now_us()), L2SubmitOutcome::Submitted);
            submitted.push(identity);
        }
        drop(s);
        let summary = out.finish().unwrap();
        assert_eq!(summary.submitted, 20);
        assert_eq!(summary.output, 20);
        assert_eq!(summary.submitted_unresolved, 0);
        assert!(summary.conservation_ok);
        assert!(summary.integrity_ok(L2Sinks::both(L2SinkKind::Fake)), "{summary:?}");
        let rows = rows(&path);
        assert_eq!(rows[0]["role"], "meta");
        assert_eq!(rows[0]["l2_schema"], L2_SCHEMA);
        assert_eq!(rows[0]["clock"], L2_CLOCK);
        assert_eq!(rows[0]["sinks"]["pc"], "fake");
        assert_eq!(rows[0]["sinks"]["haptic"], "fake");
        assert_eq!(rows[0]["device_output"], false);
        assert_eq!(rows[0]["report_window_anchor"], "close");
        let end = sentinel(&rows);
        assert_eq!(end["end_us"], end["t_us"]);
        assert_eq!(end["integrity_ok"], true);
        assert!(end["first_integrity_failure_us"].is_null());
        assert!(end["first_integrity_failure_reason"].is_null());
        assert!(end["receiver_fault_us"].is_null());
        assert!(end["max_write_lag_us"].is_u64());
        for identity in &submitted {
            let k = key(&serde_json::Value::Object(identity_json("x", identity)));
            let submits: Vec<_> =
                rows.iter().filter(|r| r["role"] == "submit" && key(r) == k).collect();
            let outputs: Vec<_> =
                rows.iter().filter(|r| r["role"] == "output" && key(r) == k).collect();
            assert_eq!(submits.len(), 1, "{k}");
            assert_eq!(outputs.len(), 1, "{k}");
            let o = outputs[0];
            let offset = if identity.modality == L2Modality::Pc { 3_000 } else { 1_000 };
            assert_eq!(o["t_output_us"].as_u64().unwrap(), o["t_submit_us"].as_u64().unwrap() + offset);
            assert!(o["t_release_us"].as_u64() <= o["t_submit_us"].as_u64());
            assert!(o["t_output_us"].as_u64() <= o["t_callback_us"].as_u64());
            assert!(o["t_callback_us"].as_u64() <= o["t_log_arrival_us"].as_u64());
            assert_eq!(o["t_us"], o["t_log_arrival_us"]);
            assert_eq!(o["run_id"], "l2-test");
            let submit_pos = rows.iter().position(|r| std::ptr::eq(r, submits[0])).unwrap();
            let output_pos = rows.iter().position(|r| std::ptr::eq(r, o)).unwrap();
            assert!(submit_pos < output_pos, "submit precedes its terminal");
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn forced_queue_full_is_an_explicit_discard_without_submit() {
        let path = tmp("fake-forced-full");
        let mut fake = FakeSinkConfig::offsets(0, 0);
        fake.force_queue_full = true;
        let out = L2Output::start(&path, config(L2SinkKind::Fake, Some(fake), 64)).unwrap();
        let s = out.submitter();
        for seq in 0..5 {
            assert_eq!(s.submit(id(L2Modality::Haptic, seq), now_us()), L2SubmitOutcome::QueueFull);
        }
        drop(s);
        let summary = out.finish().unwrap();
        assert_eq!(summary.submitted, 0);
        assert_eq!(summary.discarded_before_submit, 5);
        assert!(summary.integrity_ok(L2Sinks::both(L2SinkKind::Fake)), "{summary:?}");
        let rows = rows(&path);
        assert!(rows.iter().all(|r| r["role"] != "submit" && r["role"] != "output"));
        let discards: Vec<_> = rows.iter().filter(|r| r["role"] == "explicit_discard").collect();
        assert_eq!(discards.len(), 5);
        for d in discards {
            assert_eq!(d["reason"], DISCARD_OUTPUT_QUEUE_FULL);
            assert_eq!(d["discard_phase"], "before_submit");
            assert!(d["t_submit_us"].is_null());
            assert!(d["t_discard_us"].as_u64() >= d["t_release_us"].as_u64());
        }
        sentinel(&rows);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_really_full_bounded_queue_discards_exactly_the_overflow() {
        let path = tmp("fake-real-full");
        let gate = Arc::new(Barrier::new(2));
        let mut fake = FakeSinkConfig::offsets(0, 0);
        fake.start_gate = Some(gate.clone());
        // One fake lane (PC): the gate has exactly two parties.
        let sinks = L2Sinks { pc: L2SinkKind::Fake, haptic: L2SinkKind::Null };
        let out = L2Output::start(&path, config_sinks(sinks, Some(fake), 2)).unwrap();
        let s = out.submitter();
        let outcomes: Vec<_> =
            (0..5).map(|seq| s.submit(id(L2Modality::Pc, seq), now_us())).collect();
        assert_eq!(
            outcomes,
            vec![
                L2SubmitOutcome::Submitted,
                L2SubmitOutcome::Submitted,
                L2SubmitOutcome::QueueFull,
                L2SubmitOutcome::QueueFull,
                L2SubmitOutcome::QueueFull,
            ]
        );
        gate.wait();
        drop(s);
        let summary = out.finish().unwrap();
        assert_eq!((summary.submitted, summary.output, summary.discarded_before_submit), (2, 2, 3));
        assert!(summary.integrity_ok(L2Sinks::both(L2SinkKind::Fake)), "{summary:?}");
        let rows = rows(&path);
        for seq in 2..5u32 {
            let k = key(&serde_json::Value::Object(identity_json("x", &id(L2Modality::Pc, seq))));
            assert!(rows.iter().all(|r| !(r["role"] == "submit" && key(r) == k)));
            assert_eq!(
                rows.iter().filter(|r| r["role"] == "explicit_discard" && key(r) == k).count(),
                1
            );
        }
        sentinel(&rows);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn dropped_terminals_are_counted_unresolved_and_fail_integrity() {
        let path = tmp("fake-drop-terminals");
        let mut fake = FakeSinkConfig::offsets(0, 0);
        fake.drop_terminals = true;
        let out = L2Output::start(&path, config(L2SinkKind::Fake, Some(fake), 8)).unwrap();
        let s = out.submitter();
        for seq in 0..3 {
            s.submit(id(L2Modality::Pc, seq), now_us());
        }
        drop(s);
        let summary = out.finish().unwrap();
        assert_eq!((summary.submitted, summary.output, summary.submitted_unresolved), (3, 0, 3));
        assert!(summary.conservation_ok, "unresolved submits are conserved, not lost");
        assert!(!summary.integrity_ok(L2Sinks::both(L2SinkKind::Fake)));
        let rows = rows(&path);
        let end = sentinel(&rows);
        assert_eq!(end["submitted_unresolved"], 3);
        assert_eq!(end["integrity_ok"], false);
        assert_eq!(end["first_integrity_failure_reason"], FAIL_SUBMITTED_UNRESOLVED);
        assert!(end["first_integrity_failure_us"].as_u64().unwrap() <= end["t_us"].as_u64().unwrap());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn an_output_beyond_the_close_grace_stays_unresolved() {
        let path = tmp("fake-window");
        let mut cfg = config(L2SinkKind::Fake, Some(FakeSinkConfig::offsets(10_000_000, 0)), 8);
        cfg.report_window_us = 20_000;
        let out = L2Output::start(&path, cfg).unwrap();
        let s = out.submitter();
        s.submit(id(L2Modality::Pc, 0), now_us());
        s.submit(id(L2Modality::Haptic, 1), now_us());
        drop(s);
        let summary = out.finish().unwrap();
        assert_eq!((summary.submitted, summary.output, summary.submitted_unresolved), (2, 1, 1));
        assert!(!summary.integrity_ok(L2Sinks::both(L2SinkKind::Fake)));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn null_sink_submits_but_never_resolves() {
        let path = tmp("null");
        let out = L2Output::start(&path, config(L2SinkKind::Null, None, 8)).unwrap();
        let s = out.submitter();
        s.submit(id(L2Modality::Pc, 0), now_us());
        drop(s);
        let summary = out.finish().unwrap();
        assert_eq!((summary.submitted, summary.output, summary.submitted_unresolved), (1, 0, 1));
        assert!(summary.integrity_ok(L2Sinks::both(L2SinkKind::Null)), "{summary:?}");
        let rows = rows(&path);
        assert_eq!(rows[0]["sink_contracts"]["pc"]["resolves"], false);
        assert_eq!(rows[0]["sink_contracts"]["haptic"]["resolves"], false);
        sentinel(&rows);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn s3_route_identity_is_carried_verbatim() {
        let path = tmp("route");
        let out = L2Output::start(&path, config(L2SinkKind::Fake, Some(FakeSinkConfig::offsets(0, 0)), 8)).unwrap();
        let s = out.submitter();
        let mut identity = id(L2Modality::Pc, 7);
        identity.route = Some(("pc-d6", 3));
        s.submit(identity, now_us());
        drop(s);
        out.finish().unwrap();
        let rows = rows(&path);
        for r in rows.iter().filter(|r| r["role"] == "submit" || r["role"] == "output") {
            assert_eq!(r["wire_track"], "pc-d6");
            assert_eq!(r["route_generation"], 3);
            assert_eq!(r["seq"], 7);
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn config_validation_rejects_inconsistent_sinks() {
        assert!(config(L2SinkKind::Fake, None, 8).validate().is_err());
        assert!(config(L2SinkKind::Null, Some(FakeSinkConfig::offsets(0, 0)), 8).validate().is_err());
        assert!(config(L2SinkKind::Null, None, 0).validate().is_err());
        let mut c = config(L2SinkKind::Null, None, 8);
        c.max_tracked_objects = 0;
        assert!(c.validate().is_err());
    }

    // ---- F1: duplicates and conservation (Codex 104 counterexamples) ------

    /// Codex 104 #1: the same identity twice, 150 ms apart, offset 300 ms,
    /// close grace 225 ms -> only one terminal arrives in the grace. Was green.
    #[test]
    fn codex104_duplicate_identity_with_one_terminal_in_the_grace_fails() {
        let path = tmp("c104-dup-window");
        let mut cfg = config(L2SinkKind::Fake, Some(FakeSinkConfig::offsets(300_000, 0)), 8);
        cfg.method = "b1".into();
        cfg.report_window_us = 225_000;
        let out = L2Output::start(&path, cfg).unwrap();
        let s = out.submitter();
        let identity = L2Identity { modality: L2Modality::Pc, tier: 2, seq: 1, pts_us: 33_333, event_id: 2, route: None };
        assert_eq!(s.submit(identity, now_us()), L2SubmitOutcome::Submitted);
        std::thread::sleep(Duration::from_millis(150));
        assert_eq!(s.submit(identity, now_us()), L2SubmitOutcome::Submitted);
        drop(s);
        let summary = out.finish().unwrap();
        assert_eq!((summary.submitted, summary.output, summary.submitted_unresolved), (2, 1, 1));
        assert_eq!(summary.duplicate_logical_release, 1);
        assert!(!summary.integrity_ok(L2Sinks::both(L2SinkKind::Fake)));
        let rows = rows(&path);
        let end = sentinel(&rows);
        assert_eq!(end["integrity_ok"], false);
        assert_eq!(end["first_integrity_failure_reason"], FAIL_DUPLICATE_LOGICAL_RELEASE);
        let _ = std::fs::remove_file(&path);
    }

    /// Codex 104 #2: offset 0, two submit/output pairs of one identity. Was green.
    #[test]
    fn codex104_two_complete_pairs_of_one_identity_fail() {
        let path = tmp("c104-dup-pairs");
        let mut cfg = config(L2SinkKind::Fake, Some(FakeSinkConfig::offsets(0, 0)), 8);
        cfg.method = "b1".into();
        let out = L2Output::start(&path, cfg).unwrap();
        let s = out.submitter();
        let identity = L2Identity { modality: L2Modality::Pc, tier: 2, seq: 1, pts_us: 33_333, event_id: 2, route: None };
        assert_eq!(s.submit(identity, now_us()), L2SubmitOutcome::Submitted);
        assert_eq!(s.submit(identity, now_us()), L2SubmitOutcome::Submitted);
        drop(s);
        let summary = out.finish().unwrap();
        assert_eq!((summary.submitted, summary.output, summary.submitted_unresolved), (2, 2, 0));
        assert_eq!(summary.duplicate_logical_release, 1);
        assert!(!summary.integrity_ok(L2Sinks::both(L2SinkKind::Fake)));
        let _ = std::fs::remove_file(&path);
    }

    /// Same logical event `(track, seq)` released on two S3 routes (different
    /// tier/route) is a duplicate too; two DIFFERENT events on different
    /// routes are fine.
    #[test]
    fn logical_duplicates_are_route_independent_and_distinct_events_pass() {
        let mut a = id(L2Modality::Pc, 4);
        a.route = Some(("pc", 0));
        let mut b = a;
        b.tier = 4;
        b.route = Some(("pc-d6", 1));
        let path = tmp("dup-routes");
        let summary = replay(
            &path,
            L2SinkKind::Fake,
            vec![Row::Submit(submission(a)), output_row(submission(a)), Row::Submit(submission(b)), output_row(submission(b))],
            None,
        );
        assert_eq!(summary.duplicate_logical_release, 1);
        assert!(!summary.integrity_ok(L2Sinks::both(L2SinkKind::Fake)));
        let _ = std::fs::remove_file(&path);

        let mut c = id(L2Modality::Pc, 5);
        c.tier = 4;
        c.route = Some(("pc-d6", 1));
        let mut h = id(L2Modality::Haptic, 4);
        h.route = Some(("haptic-essential", 1));
        let path = tmp("distinct-routes");
        let summary = replay(
            &path,
            L2SinkKind::Fake,
            vec![
                Row::Submit(submission(a)), output_row(submission(a)),
                Row::Submit(submission(c)), output_row(submission(c)),
                Row::Submit(submission(h)), output_row(submission(h)),
            ],
            None,
        );
        assert_eq!(summary.duplicate_logical_release, 0);
        assert!(summary.conservation_ok);
        assert!(summary.integrity_ok(L2Sinks::both(L2SinkKind::Fake)), "{summary:?}");
        let _ = std::fs::remove_file(&path);
    }

    /// A pre-submit discard and a later submit of the same logical event are
    /// two releases of one opportunity.
    #[test]
    fn a_discard_then_a_submit_of_one_logical_event_is_a_duplicate() {
        let a = id(L2Modality::Haptic, 9);
        let path = tmp("discard-then-submit");
        let summary = replay(
            &path,
            L2SinkKind::Fake,
            vec![
                Row::Discard { identity: a, t_release_us: now_us(), t_submit_us: None, t_discard_us: now_us(), reason: DISCARD_OUTPUT_QUEUE_FULL, decode_us: None },
                Row::Submit(submission(a)),
                output_row(submission(a)),
            ],
            None,
        );
        assert_eq!(summary.duplicate_logical_release, 1);
        assert!(!summary.integrity_ok(L2Sinks::both(L2SinkKind::Fake)));
        let _ = std::fs::remove_file(&path);
    }

    /// A terminal that matches no open submit breaks conservation and the
    /// open-submit check.
    #[test]
    fn a_terminal_without_a_submit_breaks_conservation() {
        let path = tmp("orphan-terminal");
        let summary = replay(&path, L2SinkKind::Fake, vec![output_row(submission(id(L2Modality::Pc, 1)))], None);
        assert_eq!(summary.terminal_without_open_submit, 1);
        assert!(!summary.conservation_ok);
        assert!(!summary.integrity_ok(L2Sinks::both(L2SinkKind::Fake)));
        let rows = rows(&path);
        assert_eq!(sentinel(&rows)["conservation_ok"], false);
        let _ = std::fs::remove_file(&path);
    }

    // ---- F2: no truncation, no aliasing ---------------------------------

    fn digest(path: &Path) -> Vec<u8> {
        use sha2::{Digest, Sha256};
        Sha256::digest(std::fs::read(path).unwrap()).to_vec()
    }

    #[test]
    fn an_existing_file_at_the_l2_path_is_refused_and_left_unchanged() {
        let path = tmp("existing");
        std::fs::write(&path, b"existing evidence\n").unwrap();
        let before = digest(&path);
        assert!(L2Output::start(&path, config(L2SinkKind::Null, None, 8)).is_err());
        assert_eq!(digest(&path), before);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_symlink_to_the_rx_log_is_refused_and_the_rx_bytes_survive() {
        let rx = tmp("rx");
        let alias = tmp("rx-alias");
        std::fs::write(&rx, b"{\"role\":\"meta\"}\n{\"role\":\"rx\"}\n").unwrap();
        let before = std::fs::read(&rx).unwrap();
        std::os::unix::fs::symlink(&rx, &alias).unwrap();
        assert!(L2Output::start(&alias, config(L2SinkKind::Null, None, 8)).is_err());
        assert_eq!(std::fs::read(&rx).unwrap(), before);
        // A dangling symlink is refused too (nothing is created through it).
        let target = tmp("never-created");
        let dangling = tmp("dangling");
        std::os::unix::fs::symlink(&target, &dangling).unwrap();
        assert!(L2Output::start(&dangling, config(L2SinkKind::Null, None, 8)).is_err());
        assert!(!target.exists());
        for p in [&rx, &alias, &dangling] {
            let _ = std::fs::remove_file(p);
        }
    }

    #[test]
    fn a_hard_link_to_the_rx_log_is_refused() {
        let rx = tmp("rx-hard");
        let link = tmp("rx-hardlink");
        std::fs::write(&rx, b"rx bytes\n").unwrap();
        std::fs::hard_link(&rx, &link).unwrap();
        assert!(L2Output::start(&link, config(L2SinkKind::Null, None, 8)).is_err());
        assert_eq!(std::fs::read(&rx).unwrap(), b"rx bytes\n");
        for p in [&rx, &link] {
            let _ = std::fs::remove_file(p);
        }
    }

    #[test]
    fn the_fstat_exclusion_is_checked_and_distinct_files_pass() {
        let other = tmp("other-open");
        let file = File::create(&other).unwrap();
        let other_identity = open_file_identity(&file).unwrap();
        let path = tmp("excluding");
        let out = L2Output::start_excluding(&path, config(L2SinkKind::Null, None, 8), &[other_identity]).unwrap();
        assert_ne!(out.file_identity(), other_identity);
        out.finish().unwrap();
        for p in [&other, &path] {
            let _ = std::fs::remove_file(p);
        }
    }

    // ---- F3: receipt clock semantics --------------------------------------

    #[test]
    fn t_log_arrival_is_the_writer_clock_and_the_write_lag_is_reported() {
        let path = tmp("writer-lag");
        let s = submission(id(L2Modality::Pc, 1));
        let callback = now_us();
        let summary = replay(
            &path,
            L2SinkKind::Fake,
            vec![
                Row::Submit(s),
                Row::Output { submission: s, t_output_us: s.t_submit_us, t_callback_us: callback, detail: OutputDetail::Fake },
            ],
            Some(Duration::from_millis(3)),
        );
        let rows = rows(&path);
        let o = rows.iter().find(|r| r["role"] == "output").unwrap();
        assert_eq!(o["t_callback_us"].as_u64(), Some(callback));
        let t_log = o["t_log_arrival_us"].as_u64().unwrap();
        assert!(t_log >= callback + 3_000, "writer delay is inside t_log_arrival - t_callback");
        assert!(summary.max_callback_to_log_us >= 3_000);
        let end = sentinel(&rows);
        assert_eq!(end["max_write_lag_us"].as_u64(), Some(summary.max_write_lag_us));
        assert_eq!(end["max_callback_to_log_us"].as_u64(), Some(summary.max_callback_to_log_us));
        let _ = std::fs::remove_file(&path);
    }

    /// Codex 104 #3b: a callback received after the grace deadline is not
    /// accepted even when its synthetic `t_output` was before the deadline.
    #[test]
    fn a_callback_received_after_the_deadline_is_not_accepted() {
        let (tx, rx) = mpsc::sync_channel(4);
        let shared = Shared::default();
        let s = submission(id(L2Modality::Pc, 1));
        shared.close_deadline_us.store(now_us(), Ordering::SeqCst);
        let mut pending = FakePending::new(8);
        assert!(pending.push(s, s.t_submit_us));
        std::thread::sleep(Duration::from_millis(2));
        assert!(pending.emit_due(&tx, &shared));
        assert!(rx.try_recv().is_err(), "late receipt must not become an output");
        assert_eq!(pending.next_due(), Some(s.t_submit_us), "it stays pending -> unresolved");
        // Without a deadline (submissions open) it is emitted, receipt >= t_output.
        shared.close_deadline_us.store(CLOSE_OPEN, Ordering::SeqCst);
        assert!(pending.emit_due(&tx, &shared));
        match rx.try_recv().unwrap() {
            Row::Output { t_output_us, t_callback_us, .. } => assert!(t_callback_us >= t_output_us),
            _ => panic!("expected an output"),
        }
    }

    // ---- F4: failure time, receiver faults ---------------------------------

    #[test]
    fn the_first_receiver_fault_is_written_once_with_its_own_time() {
        let path = tmp("receiver-fault");
        let out = L2Output::start(&path, config(L2SinkKind::Fake, Some(FakeSinkConfig::offsets(0, 0)), 8)).unwrap();
        let s = out.submitter();
        s.submit(id(L2Modality::Pc, 0), now_us());
        let t_fault = now_us();
        s.receiver_fault(t_fault, "current S3 route pc generation 0 ended Failed: wrong size");
        assert!(out.receiver_fault_noted());
        out.receiver_fault(t_fault + 5, "a later consequence");
        drop(s);
        let summary = out.finish().unwrap();
        assert!(summary.integrity_ok(L2Sinks::both(L2SinkKind::Fake)), "a receiver fault is not an L2 integrity failure");
        let rows = rows(&path);
        let faults: Vec<_> = rows.iter().filter(|r| r["role"] == "receiver_fault").collect();
        assert_eq!(faults.len(), 1);
        assert_eq!(faults[0]["t_us"].as_u64(), Some(t_fault));
        assert!(faults[0]["t_log_arrival_us"].as_u64() >= Some(t_fault));
        assert!(faults[0]["cause"].as_str().unwrap().contains("wrong size"));
        assert!(faults[0].get("seq").is_none(), "not an object row");
        let end = sentinel(&rows);
        assert_eq!(end["receiver_fault_us"].as_u64(), Some(t_fault));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn the_first_integrity_failure_time_and_reason_are_recorded() {
        let shared = Shared::default();
        shared.fail_occurred(200, FAIL_SINK_GONE);
        shared.fail_occurred(100, FAIL_DUPLICATE_LOGICAL_RELEASE);
        shared.fail_occurred(300, FAIL_CONSERVATION);
        let timed = shared.failures().timed.unwrap();
        assert_eq!((timed.0, timed.2), (100, FAIL_DUPLICATE_LOGICAL_RELEASE));
        assert!(timed.1 >= 100, "the detection time is kept apart");
    }

    // ---- F6: producer quiescence ---------------------------------------------

    /// A producer handle alive at `finish` is an integrity failure recorded
    /// before the sentinel; anything it does after `finish` can only happen
    /// to a run whose sentinel is already false.
    #[test]
    fn a_producer_alive_at_finish_fails_the_sentinel_before_it_is_written() {
        let path = tmp("producer-alive");
        let mut cfg = config(L2SinkKind::Fake, Some(FakeSinkConfig::offsets(0, 0)), 8);
        cfg.report_window_us = 30_000;
        let out = L2Output::start(&path, cfg).unwrap();
        let leaked = out.submitter();
        assert_eq!(leaked.submit(id(L2Modality::Pc, 0), now_us()), L2SubmitOutcome::Submitted);
        let summary = out.finish().unwrap();
        assert!(summary.producers_alive_at_finish);
        assert!(!summary.integrity_ok(L2Sinks::both(L2SinkKind::Fake)));
        let sealed = std::fs::read(&path).unwrap();
        let rows = rows(&path);
        let end = sentinel(&rows);
        assert_eq!(end["integrity_ok"], false);
        assert_eq!(end["producers_alive_at_finish"], true);
        assert_eq!(end["first_integrity_failure_reason"], FAIL_PRODUCERS_ALIVE_AT_FINISH);
        // The leaked handle can no longer submit; the file is unchanged.
        assert_eq!(leaked.submit(id(L2Modality::Pc, 1), now_us()), L2SubmitOutcome::AfterClose);
        assert_eq!(std::fs::read(&path).unwrap(), sealed);
        let _ = std::fs::remove_file(&path);
    }

    /// The stage's own handle is dropped before the deadline is delivered, so
    /// even a zero close grace sees no live producer when there is none.
    #[test]
    fn a_zero_close_grace_without_live_producers_is_not_a_quiescence_failure() {
        let path = tmp("zero-grace");
        let mut cfg = config(L2SinkKind::Null, None, 8);
        cfg.report_window_us = 0;
        let out = L2Output::start(&path, cfg).unwrap();
        drop(out.submitter());
        let summary = out.finish().unwrap();
        assert!(!summary.producers_alive_at_finish);
        assert!(summary.integrity_ok(L2Sinks::both(L2SinkKind::Null)), "{summary:?}");
        let _ = std::fs::remove_file(&path);
    }

    /// A producer that is still alive when `finish` starts but is dropped
    /// inside the close grace is quiescence, not a failure.
    #[test]
    fn a_producer_dropped_inside_the_close_grace_is_fine() {
        let path = tmp("late-drop");
        let mut cfg = config(L2SinkKind::Fake, Some(FakeSinkConfig::offsets(0, 0)), 8);
        cfg.report_window_us = 2_000_000;
        let out = L2Output::start(&path, cfg).unwrap();
        let s = out.submitter();
        assert_eq!(s.submit(id(L2Modality::Pc, 0), now_us()), L2SubmitOutcome::Submitted);
        let dropper = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            drop(s);
        });
        let started = std::time::Instant::now();
        let summary = out.finish().unwrap();
        dropper.join().unwrap();
        assert!(started.elapsed() < Duration::from_millis(1_500), "woken by the last drop");
        assert!(!summary.producers_alive_at_finish);
        assert!(summary.integrity_ok(L2Sinks::both(L2SinkKind::Fake)), "{summary:?}");
        let _ = std::fs::remove_file(&path);
    }

    /// A submission between `closed` and the sentinel is counted in the
    /// sentinel (never silently lost).
    #[test]
    fn a_submission_after_close_is_counted_before_the_sentinel() {
        let path = tmp("after-close");
        let out = L2Output::start(&path, config(L2SinkKind::Fake, Some(FakeSinkConfig::offsets(0, 0)), 8)).unwrap();
        let s = out.submitter();
        out.shared.closed.store(true, Ordering::Release);
        assert_eq!(s.submit(id(L2Modality::Pc, 0), now_us()), L2SubmitOutcome::AfterClose);
        drop(s);
        let summary = out.finish().unwrap();
        assert_eq!(summary.submit_after_close, 1);
        assert!(!summary.producers_alive_at_finish);
        assert!(!summary.integrity_ok(L2Sinks::both(L2SinkKind::Fake)));
        let rows = rows(&path);
        assert_eq!(sentinel(&rows)["first_integrity_failure_reason"], FAIL_SUBMIT_AFTER_CLOSE);
        assert!(rows.iter().all(|r| r["role"] != "submit"));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn dropping_the_stage_without_finish_never_writes_a_sentinel() {
        let path = tmp("early-drop");
        let out = L2Output::start(&path, config(L2SinkKind::Fake, Some(FakeSinkConfig::offsets(0, 0)), 8)).unwrap();
        let s = out.submitter();
        assert_eq!(s.submit(id(L2Modality::Pc, 0), now_us()), L2SubmitOutcome::Submitted);
        drop(s);
        drop(out);
        std::thread::sleep(Duration::from_millis(50));
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(!text.contains("\"role\":\"instrumentation_end\""));
        assert!(!text.contains("instrumentation_end"), "not even as a meta key");
        let _ = std::fs::remove_file(&path);
    }

    // ---- F7: bounded tracking -----------------------------------------------

    #[test]
    fn tracking_and_fake_pending_are_bounded_and_overflow_fails_integrity() {
        let path = tmp("bounded");
        let mut cfg = config(L2SinkKind::Fake, Some(FakeSinkConfig::offsets(0, 0)), 64);
        cfg.max_tracked_objects = 3;
        let out = L2Output::start(&path, cfg).unwrap();
        let s = out.submitter();
        for seq in 0..6 {
            s.submit(id(L2Modality::Pc, seq), now_us());
        }
        drop(s);
        let summary = out.finish().unwrap();
        assert!(summary.tracking_overflow > 0 || summary.fake_pending_overflow > 0, "{summary:?}");
        assert!(!summary.integrity_ok(L2Sinks::both(L2SinkKind::Fake)));

        let mut pending = FakePending::new(2);
        let s = submission(id(L2Modality::Pc, 1));
        assert!(pending.push(s, 1) && pending.push(s, 2));
        assert!(!pending.push(s, 3), "a full pending set refuses instead of growing");
        let _ = std::fs::remove_file(&path);
    }

    // ---- Codex 105 G1: the close deadline binds every sink phase ------------

    /// Codex 105 #1: an item queued before `Closing`, handled by a delayed
    /// sink after the deadline, must not become an accepted output.
    #[test]
    fn codex105_a_callback_after_the_deadline_is_refused_even_before_closing_is_read() {
        let path = tmp("c105-late-before-closing");
        let gate = Arc::new(Barrier::new(2));
        // One fake lane (PC): the gate has exactly two parties.
        let sinks = L2Sinks { pc: L2SinkKind::Fake, haptic: L2SinkKind::Null };
        let mut cfg = config_sinks(sinks, Some(FakeSinkConfig::offsets(0, 0)), 8);
        cfg.report_window_us = 1_000;
        cfg.fake.as_mut().unwrap().start_gate = Some(gate.clone());
        let out = L2Output::start(&path, cfg).unwrap();
        let shared = out.shared.clone();
        let s = out.submitter();
        let t_release = now_us();
        assert_eq!(s.submit(id(L2Modality::Pc, 0), t_release), L2SubmitOutcome::Submitted);
        drop(s);
        let finisher = std::thread::spawn(move || out.finish().unwrap());
        // The count reaches zero only after `finish` published its deadline.
        while shared.producers.load(Ordering::Acquire) != 0 {
            std::thread::yield_now();
        }
        let deadline = shared.close_deadline().expect("published before the producer drop");
        std::thread::sleep(Duration::from_millis(20));
        gate.wait();
        let summary = finisher.join().unwrap();
        assert!(now_us() > deadline);
        assert_eq!((summary.submitted, summary.output, summary.submitted_unresolved), (1, 0, 1));
        assert!(!summary.integrity_ok(L2Sinks::both(L2SinkKind::Fake)));
        assert_eq!(summary.first_integrity_failure_reason.as_deref(), Some(FAIL_SUBMITTED_UNRESOLVED));
        let rows = rows(&path);
        let submit = rows.iter().find(|r| r["role"] == "submit").unwrap();
        // Occurrence = the unresolved submit's own t_submit_us (§6-1).
        assert_eq!(summary.first_integrity_failure_us, submit["t_submit_us"].as_u64());
        assert!(rows.iter().all(|r| r["role"] != "output"));
        assert_eq!(sentinel(&rows)["integrity_ok"], false);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn emit_due_compares_the_receipt_time_with_the_published_deadline() {
        let (tx, rx) = mpsc::sync_channel(4);
        let shared = Shared::default();
        let s = submission(id(L2Modality::Pc, 1));
        let mut pending = FakePending::new(8);
        assert!(pending.push(s, s.t_submit_us));
        // A deadline already in the past: nothing is accepted, it stays pending.
        shared.close_deadline_us.store(now_us(), Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(1));
        assert!(pending.emit_due(&tx, &shared));
        assert!(rx.try_recv().is_err());
        assert_eq!(pending.next_wake(&shared), None, "no wake-up for an unreceivable output");
        // A deadline in the future: accepted, with receipt >= t_output.
        shared.close_deadline_us.store(now_us() + 10_000_000, Ordering::SeqCst);
        assert!(pending.emit_due(&tx, &shared));
        match rx.try_recv().unwrap() {
            Row::Output { t_output_us, t_callback_us, .. } => assert!(t_callback_us >= t_output_us),
            _ => panic!("expected an output"),
        }
    }

    // ---- Codex 105 G2: failure OCCURRENCE times ------------------------------

    /// Codex 105 #2: a duplicate that happened BEFORE a receiver fault but was
    /// detected after it (delayed writer) keeps its occurrence time.
    #[test]
    fn codex105_a_duplicate_before_a_fault_is_timed_before_the_fault() {
        let path = tmp("c105-delayed-failure");
        let shared = Arc::new(Shared::default());
        let identity = id(L2Modality::Pc, 0);
        let first = submission(identity);
        let duplicate_us = now_us();
        let second = L2Submission { identity, t_release_us: duplicate_us, t_submit_us: duplicate_us };
        std::thread::sleep(Duration::from_millis(2));
        let fault_us = now_us();
        shared.note_receiver_fault(fault_us, "later receiver failure");
        let summary = replay_with(
            &path,
            L2SinkKind::Fake,
            vec![Row::Submit(first), output_row(first), Row::Submit(second), output_row(second)],
            Some(Duration::from_millis(5)),
            shared,
        );
        assert_eq!(summary.first_integrity_failure_us, Some(duplicate_us));
        assert!(summary.first_integrity_failure_us.unwrap() < fault_us);
        assert!(summary.first_integrity_failure_detected_us.unwrap() > fault_us, "detected later");
        assert!(summary.first_failure_time_determinable);
        let rows = rows(&path);
        let fault = rows.iter().find(|r| r["role"] == "receiver_fault").unwrap();
        assert_eq!(fault["t_us"].as_u64(), Some(fault_us));
        let end = sentinel(&rows);
        assert_eq!(end["first_integrity_failure_us"].as_u64(), Some(duplicate_us));
        assert_eq!(end["receiver_fault_us"].as_u64(), Some(fault_us));
        let _ = std::fs::remove_file(&path);
    }

    /// The submit path and the pre-submit discard path can reach the writer
    /// in either order; the duplicate occurred at the LATER release.
    #[test]
    fn a_duplicate_is_timed_at_the_later_release_in_either_processing_order() {
        let identity = id(L2Modality::Haptic, 9);
        let early = 1_000_000u64;
        let late = 1_000_500u64;
        let discard = |t: u64| Row::Discard {
            identity,
            t_release_us: t,
            t_submit_us: None,
            t_discard_us: t,
            reason: DISCARD_OUTPUT_QUEUE_FULL,
            decode_us: None,
        };
        // A submit and its output, so nothing is left unresolved.
        let submit = |t: u64| {
            let s = L2Submission { identity, t_release_us: t, t_submit_us: t };
            vec![Row::Submit(s), output_row(s)]
        };
        let then = |mut a: Vec<Row>, b: Vec<Row>| {
            a.extend(b);
            a
        };
        for (name, input) in [
            ("inv-a", then(vec![discard(late)], submit(early))),
            ("inv-b", then(submit(early), vec![discard(late)])),
            ("inv-c", then(submit(late), vec![discard(early)])),
        ] {
            let path = tmp(name);
            let summary = replay(&path, L2SinkKind::Fake, input, None);
            assert_eq!(summary.duplicate_logical_release, 1, "{name}");
            assert_eq!(
                summary.first_integrity_failure_reason.as_deref(),
                Some(FAIL_DUPLICATE_LOGICAL_RELEASE),
                "{name}"
            );
            assert_eq!(summary.first_integrity_failure_us, Some(late), "{name}");
            let _ = std::fs::remove_file(&path);
        }
    }

    #[test]
    fn unresolved_is_timed_at_the_earliest_unresolved_submit() {
        let path = tmp("unresolved-earliest");
        let a = L2Submission { identity: id(L2Modality::Pc, 1), t_release_us: 2_000, t_submit_us: 2_000 };
        let b = L2Submission { identity: id(L2Modality::Pc, 2), t_release_us: 900, t_submit_us: 900 };
        let c = L2Submission { identity: id(L2Modality::Pc, 3), t_release_us: 100, t_submit_us: 100 };
        // c is resolved; a and b are not, and arrive in the "wrong" order.
        let summary = replay(
            &path,
            L2SinkKind::Fake,
            vec![Row::Submit(a), Row::Submit(b), Row::Submit(c), output_row(c)],
            None,
        );
        assert_eq!(summary.submitted_unresolved, 2);
        assert_eq!(summary.first_integrity_failure_reason.as_deref(), Some(FAIL_SUBMITTED_UNRESOLVED));
        assert_eq!(summary.first_integrity_failure_us, Some(900));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn an_orphan_terminal_is_timed_at_its_receipt_and_overflow_at_the_attempt() {
        let path = tmp("orphan-time");
        let s = submission(id(L2Modality::Pc, 1));
        let receipt = s.t_submit_us + 7;
        let summary = replay(
            &path,
            L2SinkKind::Fake,
            vec![Row::Output { submission: s, t_output_us: s.t_submit_us, t_callback_us: receipt, detail: OutputDetail::Fake }],
            None,
        );
        assert_eq!(summary.first_integrity_failure_us, Some(receipt));
        assert_eq!(
            summary.first_integrity_failure_reason.as_deref(),
            Some(FAIL_TERMINAL_WITHOUT_OPEN_SUBMIT)
        );
        let _ = std::fs::remove_file(&path);

        // Tracking bound 64 (replay): the 65th distinct release overflows at
        // its own submission time.
        let path = tmp("overflow-time");
        let mut input = Vec::new();
        for seq in 0..65u32 {
            let t = 10_000 + seq as u64;
            let s = L2Submission { identity: id(L2Modality::Haptic, seq), t_release_us: t, t_submit_us: t };
            input.push(Row::Submit(s));
            input.push(output_row(s));
        }
        let summary = replay(&path, L2SinkKind::Fake, input, None);
        assert!(summary.tracking_overflow >= 1);
        assert_eq!(summary.first_integrity_failure_reason.as_deref(), Some(FAIL_TRACKING_CAPACITY));
        assert_eq!(summary.first_integrity_failure_us, Some(10_064));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn an_undeterminable_failure_time_is_null_and_flagged() {
        let path = tmp("untimed");
        let shared = Arc::new(Shared::default());
        shared.fail_occurred(5, FAIL_SINK_GONE);
        shared.fail_untimed(FAIL_CONSERVATION);
        let summary = replay_with(&path, L2SinkKind::Fake, Vec::new(), None, shared);
        assert_eq!(summary.first_integrity_failure_us, None);
        assert_eq!(summary.first_integrity_failure_reason.as_deref(), Some(FAIL_CONSERVATION));
        assert!(!summary.first_failure_time_determinable);
        assert!(!summary.integrity_ok(L2Sinks::both(L2SinkKind::Fake)));
        let rows = rows(&path);
        let end = sentinel(&rows);
        assert!(end["first_integrity_failure_us"].is_null());
        assert_eq!(end["first_failure_time_determinable"], false);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn the_earliest_receiver_fault_wins_whatever_the_reporting_order() {
        let shared = Shared::default();
        shared.note_receiver_fault(300, "reported first, happened later");
        shared.note_receiver_fault(100, "switch target fault, promoted later");
        shared.note_receiver_fault(200, "another");
        let fault = shared.receiver_fault.lock().unwrap().clone().unwrap();
        assert_eq!(fault, (100, "switch target fault, promoted later".to_string()));
    }

    #[test]
    fn the_meta_row_states_the_row_and_failure_time_semantics() {
        let path = tmp("meta-semantics");
        let out = L2Output::start(&path, config(L2SinkKind::Null, None, 8)).unwrap();
        out.finish().unwrap();
        let rows = rows(&path);
        let meta = &rows[0];
        assert_eq!(meta["l2_schema"], "event-pair-l2-v1.4");
        assert_eq!(meta["report_window_anchor"], "close");
        for key in ["object_rows", "sentinel_row", "receiver_fault", "t_callback_us", "file_order"] {
            assert!(meta["row_time_semantics"][key].is_string(), "{key}");
        }
        for key in [
            FAIL_DUPLICATE_LOGICAL_RELEASE, FAIL_ROW_QUEUE_OVERFLOW, FAIL_TRACKING_CAPACITY,
            FAIL_FAKE_PENDING_OVERFLOW, FAIL_TERMINAL_WITHOUT_OPEN_SUBMIT, FAIL_SUBMITTED_UNRESOLVED,
            FAIL_SUBMIT_AFTER_CLOSE, FAIL_SINK_GONE, FAIL_PRODUCERS_ALIVE_AT_FINISH, FAIL_CONSERVATION,
        ] {
            assert!(meta["failure_time_semantics"][key].is_string(), "{key}");
        }
        assert!(meta["counter_semantics"]["duplicate_logical_release"]
            .as_str()
            .unwrap()
            .contains("EXTRA releases"));
        let end = sentinel(&rows);
        assert_eq!(end["t_us"], end["t_log_arrival_us"]);
        let _ = std::fs::remove_file(&path);
    }

    /// Three releases of one key count TWO extra releases (Σ max(N − 1, 0)),
    /// in every processing order; the failure time is the second earliest.
    #[test]
    fn duplicate_counter_counts_extra_releases_in_every_order() {
        let identity = id(L2Modality::Pc, 3);
        let times = [100u64, 200, 300];
        let orders: [[usize; 3]; 6] = [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]];
        for (n, order) in orders.iter().enumerate() {
            let mut input = Vec::new();
            for &i in order {
                let t = times[i];
                let s = L2Submission { identity, t_release_us: t, t_submit_us: t };
                input.push(Row::Submit(s));
                input.push(output_row(s));
            }
            let path = tmp(&format!("dup3-{n}"));
            let summary = replay(&path, L2SinkKind::Fake, input, None);
            assert_eq!(summary.duplicate_logical_release, 2, "{order:?}");
            assert_eq!(summary.first_integrity_failure_us, Some(200), "{order:?}");
            let _ = std::fs::remove_file(&path);
        }
    }

    #[test]
    fn a_fault_stamp_is_not_a_producer() {
        let path = tmp("fault-stamp");
        let mut cfg = config(L2SinkKind::Null, None, 8);
        cfg.report_window_us = 20_000;
        let out = L2Output::start(&path, cfg).unwrap();
        let stamp = out.fault_stamp();
        stamp.receiver_fault(42, "drain counter");
        let summary = out.finish().unwrap();
        assert!(!summary.producers_alive_at_finish, "a live fault stamp is not a producer");
        assert_eq!(summary.receiver_fault, Some((42, "drain counter".to_string())));
        drop(stamp);
        let _ = std::fs::remove_file(&path);
    }

    // ---- v1.2 (M2): per-modality sinks, meta contract, order/close rules ----

    /// Each modality has its own sink: PC fake resolves, haptic null never
    /// does, and the null exemption covers the haptic track only.
    #[test]
    fn per_modality_sinks_resolve_independently() {
        for (sinks, resolving) in [
            (L2Sinks { pc: L2SinkKind::Fake, haptic: L2SinkKind::Null }, L2Modality::Pc),
            (L2Sinks { pc: L2SinkKind::Null, haptic: L2SinkKind::Fake }, L2Modality::Haptic),
        ] {
            let path = tmp(&format!("per-track-{}", sinks.spec()));
            let out = L2Output::start(&path, config_sinks(sinks, Some(FakeSinkConfig::offsets(100, 50)), 16))
                .unwrap();
            let s = out.submitter();
            for seq in 0..6u32 {
                let modality = if seq % 2 == 0 { L2Modality::Pc } else { L2Modality::Haptic };
                // A payload rides along; null/fake lanes ignore it.
                let payload = bytes::Bytes::from(vec![0u8; 178]);
                assert_eq!(s.submit_payload(id(modality, seq), now_us(), Some(payload)), L2SubmitOutcome::Submitted);
            }
            drop(s);
            let summary = out.finish().unwrap();
            assert_eq!((summary.submitted, summary.output), (6, 3), "{sinks:?}");
            let silent = if resolving == L2Modality::Pc { L2Modality::Haptic } else { L2Modality::Pc };
            assert_eq!(summary.submitted_unresolved_by_track[resolving.index()], 0);
            assert_eq!(summary.submitted_unresolved_by_track[silent.index()], 3);
            assert!(summary.integrity_ok(sinks), "{summary:?}");
            let rows = rows(&path);
            assert!(rows
                .iter()
                .filter(|r| r["role"] == "output")
                .all(|r| r["track"] == resolving.as_str() && r["output_detail"]["device"] == "fake"));
            let end = sentinel(&rows);
            assert_eq!(end["submitted_unresolved_by_track"][silent.as_str()], 3);
            assert_eq!(end["submitted_unresolved_by_track"][resolving.as_str()], 0);
            assert_eq!(end["integrity_ok"], true);
            assert_eq!(end["sinks"][resolving.as_str()], "fake");
            assert_eq!(end["sinks"][silent.as_str()], "null");
            let _ = std::fs::remove_file(&path);
        }
    }

    /// The null exemption is per track: an unresolved PC submission on a
    /// fake PC lane fails integrity even though the haptic lane is null, and
    /// its time is the PC submission's, not an earlier haptic one.
    #[test]
    fn a_null_lane_exempts_only_its_own_track() {
        let path = tmp("null-exempts-own-track");
        let mut fake = FakeSinkConfig::offsets(0, 0);
        fake.drop_terminals = true;
        let sinks = L2Sinks { pc: L2SinkKind::Fake, haptic: L2SinkKind::Null };
        let out = L2Output::start(&path, config_sinks(sinks, Some(fake), 16)).unwrap();
        let s = out.submitter();
        s.submit(id(L2Modality::Haptic, 1), now_us());
        std::thread::sleep(Duration::from_millis(2));
        s.submit(id(L2Modality::Pc, 0), now_us());
        drop(s);
        let summary = out.finish().unwrap();
        assert_eq!(summary.submitted_unresolved_by_track, [1, 1]);
        assert!(!summary.integrity_ok(sinks));
        assert_eq!(summary.first_integrity_failure_reason.as_deref(), Some(FAIL_SUBMITTED_UNRESOLVED));
        let rows = rows(&path);
        let pc_submit = rows.iter().find(|r| r["role"] == "submit" && r["track"] == "pc").unwrap();
        assert_eq!(summary.first_integrity_failure_us, pc_submit["t_submit_us"].as_u64());
        let _ = std::fs::remove_file(&path);
    }

    /// 1-D §11-7 M2 work order (1): the v1.2 meta row declares each track's
    /// sink, its contract (device output, resolves, `output_detail` schema),
    /// the file-level `device_output` rule, the order and close rules, and
    /// the (mandatory) counter semantics.
    #[test]
    fn the_v1_2_meta_declares_sinks_contracts_and_rules() {
        let path = tmp("meta-v1-2");
        let sinks = L2Sinks { pc: L2SinkKind::Fake, haptic: L2SinkKind::Null };
        let out = L2Output::start(&path, config_sinks(sinks, Some(FakeSinkConfig::offsets(7, 9)), 8)).unwrap();
        out.finish().unwrap();
        let rows = rows(&path);
        let meta = &rows[0];
        assert_eq!(meta["role"], "meta");
        assert_eq!(meta["l2_schema"], L2_SCHEMA);
        assert_eq!(meta["l2_schema"], "event-pair-l2-v1.4");
        assert_eq!(meta["sinks"], serde_json::json!({ "pc": "fake", "haptic": "null" }));
        assert_eq!(meta["device_output"], false);
        assert!(meta["device_output_rule"].as_str().unwrap().contains("EVERY track"));
        let pc = &meta["sink_contracts"]["pc"];
        assert_eq!((pc["kind"].as_str(), pc["device_output"].as_bool(), pc["resolves"].as_bool()), (Some("fake"), Some(false), Some(true)));
        assert!(pc["output_detail"]["device"].is_string() && pc["output_detail"]["modality"].is_string());
        assert!(pc["t_output_us"].as_str().unwrap().contains("offset"));
        let haptic = &meta["sink_contracts"]["haptic"];
        assert_eq!((haptic["kind"].as_str(), haptic["resolves"].as_bool()), (Some("null"), Some(false)));
        assert!(haptic["output_detail"].is_null());
        assert_eq!(meta["fake"], serde_json::json!({
            "pc_offset_us": 7, "haptic_offset_us": 9, "force_queue_full": false, "drop_terminals": false,
        }));
        assert_eq!(meta["queue_capacity_per_track"], 8);
        assert!(meta.get("queue_capacity").is_none() && meta.get("sink").is_none());
        assert!(meta["order_rule"].as_str().unwrap().contains("t_output_us <= t_callback_us"));
        assert!(meta["report_window_rule"].as_str().unwrap().contains("close_deadline_us"));
        assert!(meta["report_window_rule"].as_str().unwrap().contains("equality is rejected"));
        for key in [
            "duplicate_logical_release", "submitted_unresolved", "discarded_before_submit",
            "discarded_after_submit", "output_order_violation", "terminal_at_or_after_deadline",
        ] {
            assert!(meta["counter_semantics"][key].is_string(), "counter_semantics.{key}");
        }
        for key in [FAIL_OUTPUT_ORDER, FAIL_TERMINAL_AT_OR_AFTER_DEADLINE] {
            assert!(meta["failure_time_semantics"][key].is_string(), "{key}");
        }
        let _ = std::fs::remove_file(&path);
    }

    /// 1-D §11-7 M2 work order (2): the sentinel records `finish_start_us`
    /// and `close_deadline_us`; every terminal receipt is strictly before it.
    #[test]
    fn the_sentinel_records_the_close_deadline_and_terminals_precede_it() {
        let path = tmp("close-deadline");
        let mut cfg = config(L2SinkKind::Fake, Some(FakeSinkConfig::offsets(2_000, 500)), 16);
        cfg.report_window_us = 300_000;
        let out = L2Output::start(&path, cfg).unwrap();
        let s = out.submitter();
        for seq in 0..4u32 {
            s.submit(id(if seq % 2 == 0 { L2Modality::Pc } else { L2Modality::Haptic }, seq), now_us());
        }
        drop(s);
        let before_finish = now_us();
        let summary = out.finish().unwrap();
        assert!(summary.integrity_ok(L2Sinks::both(L2SinkKind::Fake)), "{summary:?}");
        let rows = rows(&path);
        let end = sentinel(&rows);
        let finish_start = end["finish_start_us"].as_u64().unwrap();
        let deadline = end["close_deadline_us"].as_u64().unwrap();
        assert!(finish_start >= before_finish);
        assert_eq!(deadline, finish_start + 300_000);
        assert_eq!(summary.close_deadline_us, Some(deadline));
        assert_eq!(end["terminal_at_or_after_deadline"], 0);
        for o in rows.iter().filter(|r| r["role"] == "output") {
            assert!(o["t_callback_us"].as_u64().unwrap() < deadline);
        }
        let _ = std::fs::remove_file(&path);
    }

    /// The writer's order check fires on the raw values (never clamped):
    /// `t_output < t_submit` and `t_callback < t_output` are both violations,
    /// occurring at the row's `t_callback_us`.
    #[test]
    fn an_output_row_out_of_order_is_an_integrity_failure() {
        let base = now_us();
        for (name, t_output, t_callback) in [
            ("output-before-submit", base - 10, base + 5),
            ("callback-before-output", base + 20, base + 10),
        ] {
            let s = L2Submission { identity: id(L2Modality::Haptic, 3), t_release_us: base - 50, t_submit_us: base };
            let path = tmp(name);
            let summary = replay(
                &path,
                L2SinkKind::Fake,
                vec![
                    Row::Submit(s),
                    Row::Output { submission: s, t_output_us: t_output, t_callback_us: t_callback, detail: OutputDetail::Fake },
                ],
                None,
            );
            assert_eq!(summary.output_order_violation, 1, "{name}");
            assert_eq!(summary.first_integrity_failure_reason.as_deref(), Some(FAIL_OUTPUT_ORDER), "{name}");
            assert_eq!(summary.first_integrity_failure_us, Some(t_callback), "{name}");
            assert!(!summary.integrity_ok(L2Sinks::both(L2SinkKind::Fake)));
            let rows = rows(&path);
            let o = rows.iter().find(|r| r["role"] == "output").unwrap();
            assert_eq!(o["t_output_us"].as_u64(), Some(t_output), "{name}: written as is");
            assert_eq!(sentinel(&rows)["output_order_violation"], 1);
            let _ = std::fs::remove_file(&path);
        }
    }

    /// The writer's close cross-check rejects a terminal received AT the
    /// deadline (equality) and accepts one a microsecond before it.
    #[test]
    fn a_terminal_received_at_the_deadline_is_flagged_and_one_before_it_is_not() {
        for (name, offset_from_deadline, flagged) in [("at", 0i64, true), ("before", -1, false)] {
            let shared = Arc::new(Shared::default());
            // In the past, so the receipt also precedes the writer clock
            // (otherwise the order rule would fire first).
            let deadline = now_us() - 1_000;
            shared.close_deadline_us.store(deadline, Ordering::SeqCst);
            let t_submit = deadline - 10_000;
            let s = L2Submission { identity: id(L2Modality::Pc, 1), t_release_us: t_submit, t_submit_us: t_submit };
            let receipt = (deadline as i64 + offset_from_deadline) as u64;
            let path = tmp(&format!("deadline-{name}"));
            let summary = replay_with(
                &path,
                L2SinkKind::Fake,
                vec![
                    Row::Submit(s),
                    Row::Output { submission: s, t_output_us: t_submit, t_callback_us: receipt, detail: OutputDetail::Fake },
                ],
                None,
                shared,
            );
            assert_eq!(summary.terminal_at_or_after_deadline, u64::from(flagged), "{name}");
            if flagged {
                assert_eq!(summary.first_integrity_failure_reason.as_deref(), Some(FAIL_TERMINAL_AT_OR_AFTER_DEADLINE));
                assert_eq!(summary.first_integrity_failure_us, Some(receipt));
            }
            assert_eq!(summary.close_deadline_us, Some(deadline));
            let _ = std::fs::remove_file(&path);
        }
    }

    /// Lanes are independent: a PC lane that has not started consuming
    /// (gated) does not hold back the haptic lane's rows.
    #[test]
    fn a_stalled_lane_does_not_hold_back_the_other_track() {
        let path = tmp("lane-independence");
        let gate = Arc::new(Barrier::new(2));
        let mut fake = FakeSinkConfig::offsets(0, 0);
        fake.start_gate = Some(gate.clone());
        let sinks = L2Sinks { pc: L2SinkKind::Fake, haptic: L2SinkKind::Null };
        let out = L2Output::start(&path, config_sinks(sinks, Some(fake), 8)).unwrap();
        let s = out.submitter();
        assert_eq!(s.submit(id(L2Modality::Pc, 0), now_us()), L2SubmitOutcome::Submitted);
        assert_eq!(s.submit(id(L2Modality::Haptic, 1), now_us()), L2SubmitOutcome::Submitted);
        // The haptic submit row is written while the PC lane is still gated.
        let started = std::time::Instant::now();
        loop {
            let text = std::fs::read_to_string(&path).unwrap();
            if text.lines().any(|l| l.contains("\"role\":\"submit\"") && l.contains("\"track\":\"haptic\"")) {
                assert!(!text.contains("\"track\":\"pc\""), "the PC lane has not consumed yet");
                break;
            }
            assert!(started.elapsed() < Duration::from_secs(5), "haptic lane blocked by the PC lane");
            std::thread::sleep(Duration::from_millis(1));
        }
        gate.wait();
        drop(s);
        let summary = out.finish().unwrap();
        assert_eq!((summary.submitted, summary.output), (2, 1));
        assert!(summary.integrity_ok(sinks), "{summary:?}");
        let _ = std::fs::remove_file(&path);
    }

    // ---- M2: the haptic ALSA lane on the simulated stream -------------------

    use crate::l2_haptic::{
        frames_to_us_round_half_away, PcmError, SimFault, FAIL_ALSA_STATUS_ERROR, FAIL_ALSA_XRUN,
        PCM_RATE_HZ,
    };

    /// Generous sizes so that a starved test thread cannot manufacture a
    /// simulated XRUN: F = 100 ms of frames, buffer ~341 ms.
    fn sim_placement() -> PlacementConfig {
        PlacementConfig {
            period_frames: 512,
            buffer_frames: 16_384,
            target_fill_frames: 4_800,
            prefill_frames: 4_800,
            queue_limit_frames: 48_000,
        }
    }

    fn sim_alsa(faults: Vec<SimFault>) -> AlsaSinkConfig {
        let mut sim = SimPcmConfig::new(512, 16_384);
        sim.faults = faults;
        AlsaSinkConfig {
            backend: AlsaBackend::Simulated(sim),
            placement: sim_placement(),
            status_interval_us: 1_000,
            ready_timeout_us: 5_000_000,
            release_timeout_us: 5_000_000,
            diagnostics: None,
        }
    }

    const PC_FAKE_HAPTIC_ALSA: L2Sinks = L2Sinks { pc: L2SinkKind::Fake, haptic: L2SinkKind::Alsa };

    fn alsa_config(faults: Vec<SimFault>) -> L2Config {
        let mut c = config_sinks(PC_FAKE_HAPTIC_ALSA, Some(FakeSinkConfig::offsets(1_000, 0)), 64);
        c.alsa = Some(sim_alsa(faults));
        c.report_window_us = 2_000_000;
        c
    }

    fn haptic(seq: u32) -> L2Identity {
        L2Identity {
            modality: L2Modality::Haptic,
            tier: 0,
            seq,
            pts_us: seq as u64 * 1_000_000 / 90,
            event_id: if seq % 3 == 0 { seq / 3 + 1 } else { 0 },
            route: None,
        }
    }

    /// The registered PCM split of tick `seq` (88/89 samples at 8 kHz / 90 Hz).
    fn tick_payload(seq: u32) -> bytes::Bytes {
        let (start, end) = crate::pcm_sample_bounds(seq as u64, 8_000, 90);
        let samples: Vec<u8> = (start..end).flat_map(|i| ((i % 2_000) as i16 - 1_000).to_le_bytes()).collect();
        bytes::Bytes::from(samples)
    }

    fn submits_with_outputs(rows: &[serde_json::Value], track: &str) -> Vec<(serde_json::Value, serde_json::Value)> {
        rows.iter()
            .filter(|r| r["role"] == "submit" && r["track"] == track)
            .map(|s| {
                let k = key(s);
                let outputs: Vec<_> = rows.iter().filter(|r| r["role"] == "output" && key(r) == k).collect();
                assert_eq!(outputs.len(), 1, "{k}: exactly one output");
                (s.clone(), outputs[0].clone())
            })
            .collect()
    }

    /// Registration 11-11 (v1.3): no key ending in `_ns` and no audio
    /// timestamp value anywhere in an L2 JSONL (the schema descriptions in
    /// meta may NAME the diagnostics file's fields only in prose).
    fn assert_no_raw_ns(rows: &[serde_json::Value]) {
        fn walk(value: &serde_json::Value, path: &str) {
            match value {
                serde_json::Value::Object(map) => {
                    for (key, child) in map {
                        assert!(!key.ends_with("_ns"), "raw ns key {path}.{key}");
                        assert!(!key.contains("audio_htstamp") && !key.starts_with("link_audio"), "audio timestamp key {path}.{key}");
                        walk(child, &format!("{path}.{key}"));
                    }
                }
                serde_json::Value::Array(items) => items.iter().for_each(|v| walk(v, path)),
                _ => {}
            }
        }
        for (i, row) in rows.iter().enumerate() {
            walk(row, &format!("row{i}"));
        }
    }

    /// Codex 110: the given raw values must not appear anywhere in the rows —
    /// neither as numbers nor inside strings (digit runs compared whole).
    fn assert_raw_values_absent(rows: &[serde_json::Value], raw: &[u64]) {
        fn walk(value: &serde_json::Value, raw: &[u64], path: &str) {
            match value {
                serde_json::Value::Number(n) => {
                    assert!(!n.as_u64().is_some_and(|v| raw.contains(&v)), "raw value {n} at {path}");
                }
                serde_json::Value::String(text) => {
                    for token in text.split(|c: char| !c.is_ascii_digit()).filter(|t| !t.is_empty()) {
                        let leaked = token.parse::<u64>().is_ok_and(|v| raw.contains(&v));
                        assert!(!leaked, "raw value {token} inside {path}: {text:?}");
                    }
                }
                serde_json::Value::Object(map) => map.iter().for_each(|(k, v)| walk(v, raw, &format!("{path}.{k}"))),
                serde_json::Value::Array(items) => items.iter().for_each(|v| walk(v, raw, path)),
                _ => {}
            }
        }
        for (i, row) in rows.iter().enumerate() {
            walk(row, raw, &format!("row{i}"));
        }
    }

    /// Every released tick: one submit, one output, the snapshot evidence
    /// of the registered formula, release-order back-to-back placement,
    /// and the order rule; v1.2 meta and sentinel for an ALSA lane.
    #[test]
    fn the_alsa_lane_outputs_each_tick_once_with_its_status_evidence() {
        let path = tmp("alsa-sim-happy");
        let out = L2Output::start(&path, alsa_config(Vec::new())).unwrap();
        let s = out.submitter();
        let t_begin = now_us();
        for seq in 0..18u32 {
            let due = t_begin + seq as u64 * 11_111;
            while now_us() < due {
                std::thread::sleep(Duration::from_micros(200));
            }
            assert_eq!(s.submit_payload(haptic(seq), now_us(), Some(tick_payload(seq))), L2SubmitOutcome::Submitted);
            if seq % 3 == 0 {
                assert_eq!(s.submit(id(L2Modality::Pc, seq / 3), now_us()), L2SubmitOutcome::Submitted);
            }
        }
        drop(s);
        let summary = out.finish().unwrap();
        assert!(summary.integrity_ok(PC_FAKE_HAPTIC_ALSA), "{summary:?}");
        assert_eq!((summary.submitted, summary.output), (24, 24));
        let rows = rows(&path);
        let meta = &rows[0];
        assert_eq!(meta["sinks"], serde_json::json!({ "pc": "fake", "haptic": "alsa" }));
        assert_eq!(meta["device_output"], false, "a simulated stream is never a device output");
        let contract = &meta["sink_contracts"]["haptic"];
        assert_eq!((contract["kind"].as_str(), contract["backend"].as_str()), (Some("alsa"), Some("simulated")));
        assert_eq!((contract["device_output"].as_bool(), contract["resolves"].as_bool()), (Some(false), Some(true)));
        for field in ["n_k", "hw_pos", "appl_pos", "delay_frames", "htstamp_us", "trigger_htstamp_us", "offset_us"] {
            assert!(contract["output_detail"][field].is_string(), "schema of {field}");
        }
        // v1.3 (registration 11-11): no raw ns value and no audio-timestamp
        // value anywhere in the L2 JSONL — schema, meta or rows.
        assert_no_raw_ns(&rows);
        let alsa = &meta["alsa"];
        assert_eq!(alsa["backend"], "simulated");
        assert_eq!((alsa["period_frames"].as_u64(), alsa["buffer_frames"].as_u64()), (Some(512), Some(16_384)));
        assert_eq!((alsa["tstamp_mode"].as_str(), alsa["tstamp_type"].as_str()), (Some("ENABLE"), Some("MONOTONIC")));
        assert_eq!(alsa["audio_tstamp_type"]["requested"], "LINK");
        assert_eq!(alsa["audio_tstamp_type"]["actual_first_status"], "LINK");
        assert_eq!(alsa["audio_tstamp_type"]["report_valid_first_status"], true);
        assert!(alsa["parameter_status"].as_str().unwrap().contains("provisional"));
        // No hard-coded observation of a past run in the meta (Codex 110).
        assert!(alsa.get("driver_delay_observation").is_none());
        assert!(alsa["audio_tstamp_type"].get("observed").is_none());
        let t_start = alsa["stream_epoch"]["t_start_us"].as_u64().unwrap();
        assert!(t_start > 0 && alsa["stream_epoch"]["trigger_htstamp_us"].as_u64().unwrap() == t_start);
        assert_eq!(alsa["stream_epoch"]["prefill_frames"], 4_800);
        assert_eq!(alsa["stream_epoch"]["first_status_driver_delay_frames"], 0);
        assert!(alsa["diagnostics_file"].is_null(), "no diagnostics file unless asked (probe only)");

        let pairs = submits_with_outputs(&rows, "haptic");
        assert_eq!(pairs.len(), 18);
        let mut previous_end = 0u64;
        for (submit, output) in &pairs {
            let d = &output["output_detail"];
            assert_eq!((d["device"].as_str(), d["backend"].as_str()), (Some("alsa"), Some("simulated")));
            let seq = submit["seq"].as_u64().unwrap();
            let (start, end) = crate::pcm_sample_bounds(seq, 8_000, 90);
            assert_eq!(d["samples_8k"].as_u64(), Some(end - start));
            assert_eq!(d["frames"].as_u64(), Some(6 * (end - start)));
            assert!(matches!(d["frames"].as_u64(), Some(528) | Some(534)));
            let (n_k, hw_pos) = (d["n_k"].as_u64().unwrap(), d["hw_pos"].as_i64().unwrap());
            assert!(hw_pos > n_k as i64, "strictly passed");
            assert_eq!(hw_pos, d["appl_pos"].as_i64().unwrap() - d["delay_frames"].as_i64().unwrap());
            let offset = frames_to_us_round_half_away(n_k as i64 - hw_pos, PCM_RATE_HZ);
            assert_eq!(d["offset_us"].as_i64(), Some(offset));
            assert_eq!(output["t_output_us"].as_i64(), Some(d["htstamp_us"].as_i64().unwrap() + offset));
            assert!(n_k >= previous_end, "release order, never overlapping");
            previous_end = n_k + d["frames"].as_u64().unwrap();
            let t = |k: &str| output[k].as_u64().unwrap();
            assert!(t("t_release_us") <= t("t_submit_us") && t("t_submit_us") <= t("t_output_us"));
            assert!(t("t_output_us") <= t("t_callback_us") && t("t_callback_us") <= t("t_log_arrival_us"));
            // The snapshot's timestamp lies within its status call.
            let status_t0 = d["status_t0_us"].as_u64().unwrap();
            let htstamp_us = d["htstamp_us"].as_u64().unwrap();
            assert!(status_t0 <= htstamp_us && htstamp_us <= t("t_callback_us"));
        }
        let end = sentinel(&rows);
        assert_eq!(end["integrity_ok"], true);
        assert_eq!(end["device_output"], false);
        assert!(end["lanes"]["pc"].is_null(), "no device lane on PC");
        let lane = &end["lanes"]["haptic"];
        assert_eq!((lane["kind"].as_str(), lane["backend"].as_str()), (Some("alsa"), Some("simulated")));
        assert_eq!(lane["ticks_output"], 18);
        assert_eq!(lane["tail_played_at_close"], true, "the last tick was played to its end");
        assert_eq!(lane["unplayed_tick_frames_at_close"], 0);
        assert!(lane["last_hw_pos"].as_i64().unwrap() >= lane["last_tick_end_frame"].as_i64().unwrap());
        assert_eq!(lane["ticks_written"], 18);
        assert!(lane["silence_frames_written"].as_u64().unwrap() > 4_800, "low-water silence was written");
        assert!(lane["status_calls"].as_u64().unwrap() > 18);
        assert!(lane["device_failure"].is_null());
        assert_eq!(lane["outputs_after_deadline"], 0);
        let _ = std::fs::remove_file(&path);
    }

    /// An XRUN mid-run: an integrity failure WITHOUT an occurrence time
    /// (registration 6-1, Codex 109 P1: first_integrity_failure_us null,
    /// not determinable), the raw kernel trigger (stream start + 60 ms here)
    /// and the observation interval kept apart in `lanes.haptic`; every
    /// submission still has exactly one terminal (an after-submit
    /// `device_failure` discard) and the order/close checks hold.
    #[test]
    fn an_alsa_xrun_fails_integrity_without_an_occurrence_time_and_terminates_every_submission() {
        let path = tmp("alsa-sim-xrun");
        let out = L2Output::start(&path, alsa_config(vec![SimFault::Xrun { after_start_us: 60_000 }])).unwrap();
        let s = out.submitter();
        for seq in 0..12u32 {
            s.submit_payload(haptic(seq), now_us(), Some(tick_payload(seq)));
            std::thread::sleep(Duration::from_millis(11));
        }
        drop(s);
        let summary = out.finish().unwrap();
        assert!(!summary.integrity_ok(PC_FAKE_HAPTIC_ALSA));
        assert_eq!(summary.first_integrity_failure_reason.as_deref(), Some(FAIL_ALSA_XRUN));
        let rows = rows(&path);
        let t_start = rows[0]["alsa"]["stream_epoch"]["t_start_us"].as_u64().unwrap();
        assert_eq!(summary.first_integrity_failure_us, None, "no occurrence time is claimed");
        assert!(!summary.first_failure_time_determinable);
        let detected = summary.first_integrity_failure_detected_us.unwrap();
        assert!(detected >= t_start + 60_000);
        assert!(summary.conservation_ok);
        assert_eq!(summary.submitted_unresolved, 0, "every submission terminated");
        assert_eq!(summary.output_order_violation + summary.terminal_at_or_after_deadline, 0);
        for submit in rows.iter().filter(|r| r["role"] == "submit") {
            let k = key(submit);
            let terminals: Vec<_> = rows
                .iter()
                .filter(|r| (r["role"] == "output" || r["role"] == "explicit_discard") && key(r) == k)
                .collect();
            assert_eq!(terminals.len(), 1, "{k}");
        }
        let discards: Vec<_> = rows.iter().filter(|r| r["role"] == "explicit_discard").collect();
        assert!(!discards.is_empty());
        for d in discards {
            assert_eq!((d["reason"].as_str(), d["discard_phase"].as_str()), (Some(DISCARD_DEVICE_FAILURE), Some("after_submit")));
            assert!(d["t_discard_us"].as_u64() >= d["t_submit_us"].as_u64());
        }
        let end = sentinel(&rows);
        assert_eq!(end["integrity_ok"], false);
        assert!(end["first_integrity_failure_us"].is_null());
        assert_eq!(end["first_failure_time_determinable"], false);
        assert_eq!(end["first_integrity_failure_detected_us"].as_u64(), Some(detected));
        let failure = &end["lanes"]["haptic"]["device_failure"];
        assert_eq!(failure["reason"], FAIL_ALSA_XRUN);
        assert_eq!(failure["occurrence_time_determinable"], false);
        assert_eq!(failure["kernel_trigger_us"].as_u64(), Some(t_start + 60_000), "raw, apart");
        assert_eq!(failure["detected_us"].as_u64(), Some(detected));
        let interval = failure["observation_interval_us"].as_array().unwrap();
        assert!(interval[0].as_u64().unwrap() <= t_start + 60_000, "a healthy status before the stop");
        assert_eq!(interval[1].as_u64(), Some(detected));
        assert_no_raw_ns(&rows);
        let _ = std::fs::remove_file(&path);
    }

    /// A haptic submission without a playable payload: submit row, an
    /// after-submit `payload_invalid` discard, and the integrity failure at
    /// its `t_submit_us`.
    #[test]
    fn an_unplayable_haptic_payload_is_submit_discard_and_an_integrity_failure() {
        let path = tmp("alsa-sim-payload");
        let out = L2Output::start(&path, alsa_config(Vec::new())).unwrap();
        let s = out.submitter();
        assert_eq!(s.submit(haptic(1), now_us()), L2SubmitOutcome::Submitted);
        std::thread::sleep(Duration::from_millis(2));
        s.submit_payload(haptic(2), now_us(), Some(bytes::Bytes::from(vec![1u8, 2, 3])));
        drop(s);
        let summary = out.finish().unwrap();
        assert!(!summary.integrity_ok(PC_FAKE_HAPTIC_ALSA));
        assert_eq!(summary.first_integrity_failure_reason.as_deref(), Some(FAIL_HAPTIC_PAYLOAD_INVALID));
        let rows = rows(&path);
        let first_submit = rows.iter().find(|r| r["role"] == "submit").unwrap();
        assert_eq!(summary.first_integrity_failure_us, first_submit["t_submit_us"].as_u64());
        let discards: Vec<_> = rows.iter().filter(|r| r["role"] == "explicit_discard").collect();
        assert_eq!(discards.len(), 2);
        assert!(discards.iter().all(|d| d["reason"] == DISCARD_PAYLOAD_INVALID && d["discard_phase"] == "after_submit"));
        assert!(rows.iter().all(|r| r["role"] != "output"));
        assert!(summary.conservation_ok && summary.submitted_unresolved == 0);
        let _ = std::fs::remove_file(&path);
    }

    /// A stream that cannot start (the device is held): `start` fails, the
    /// run is refused, and the file never gets a meta row.
    #[test]
    fn an_alsa_stream_that_cannot_start_refuses_the_run_and_leaves_no_meta() {
        let path = tmp("alsa-sim-start-error");
        let error = L2Output::start(&path, alsa_config(vec![SimFault::StartError])).err().expect("refused");
        assert!(format!("{error:#}").contains("failed to start"), "{error:#}");
        assert_eq!(std::fs::read(&path).unwrap(), b"", "no meta row, so never a valid ledger");
        let _ = std::fs::remove_file(&path);
    }

    /// The close deadline binds the device lane too: ticks whose first
    /// frame cannot reach hw_pos before it stay unresolved (no terminal is
    /// accepted at or after the deadline), which fails integrity.
    #[test]
    fn alsa_outputs_after_the_close_deadline_are_not_accepted() {
        let path = tmp("alsa-sim-deadline");
        let mut c = alsa_config(Vec::new());
        c.report_window_us = 5_000; // far shorter than the 100 ms fill
        let out = L2Output::start(&path, c).unwrap();
        let s = out.submitter();
        for seq in 0..3u32 {
            s.submit_payload(haptic(seq), now_us(), Some(tick_payload(seq)));
        }
        drop(s);
        let summary = out.finish().unwrap();
        assert_eq!(summary.submitted_unresolved_by_track[L2Modality::Haptic.index()], 3);
        assert_eq!(summary.first_integrity_failure_reason.as_deref(), Some(FAIL_SUBMITTED_UNRESOLVED));
        assert_eq!(summary.terminal_at_or_after_deadline, 0, "the lane never emits one");
        let rows = rows(&path);
        assert!(rows.iter().all(|r| r["role"] != "output"));
        let end = sentinel(&rows);
        assert_eq!(end["close_deadline_us"].as_u64().unwrap() - end["finish_start_us"].as_u64().unwrap(), 5_000);
        let _ = std::fs::remove_file(&path);
    }

    /// The lane's row hand-off, driven directly and deterministically:
    /// queued -> submit; write queue over Q -> before-submit
    /// `output_queue_full`; no payload -> submit + `payload_invalid`; after
    /// a device failure every abandoned and every later submission ->
    /// `device_failure` after submit.
    #[test]
    fn the_alsa_lane_turns_admissions_into_ledger_rows() {
        let (rows_tx, rows_rx) = mpsc::sync_channel(64);
        let shared = Arc::new(Shared::default());
        let placement = PlacementConfig { queue_limit_frames: 2 * 534, ..sim_placement() };
        let mut pcm = SimPcm::new(SimPcmConfig::new(512, 16_384));
        let mut engine = HapticPlacement::new(placement, pcm.params()).unwrap();
        let mut clock = now_us;
        engine.prefill(&mut pcm, &mut clock).unwrap();
        let t0 = now_us();
        pcm.start(t0).unwrap();
        let first = pcm.status(t0).unwrap();
        engine.begin_epoch(t0, first, t0).unwrap();
        let mut lane = AlsaLane::new(Box::new(pcm), engine, rows_tx, shared.clone());
        let sub = |seq: u32| {
            let t = now_us();
            L2Submission { identity: haptic(seq), t_release_us: t, t_submit_us: t }
        };
        lane.accept(sub(0), Some(tick_payload(1))); // 89 samples
        lane.accept(sub(1), Some(tick_payload(1)));
        lane.accept(sub(2), Some(tick_payload(0))); // over Q
        lane.accept(sub(3), None); // not playable
        let failure = DeviceFailure {
            reason: FAIL_ALSA_XRUN,
            last_good_status_t0_us: t0,
            detected_us: now_us(),
            kernel_trigger_us: None,
            kernel_trigger_ns: None,
            detail: "test".into(),
            raw_ns: Vec::new(),
        };
        lane.fail(failure);
        lane.accept(sub(4), Some(tick_payload(4)));
        drop(lane);
        let got: Vec<String> = rows_rx
            .try_iter()
            .map(|row| match row {
                Row::Submit(s) => format!("submit {}", s.identity.seq),
                Row::Discard { identity, t_submit_us, reason, .. } => {
                    format!("discard {} {} {reason}", identity.seq, if t_submit_us.is_some() { "after" } else { "before" })
                }
                Row::Output { .. } => "output".into(),
                Row::LaneStats { .. } | Row::End => "other".into(),
            })
            .collect();
        assert_eq!(
            got,
            vec![
                "submit 0", "submit 1", "discard 2 before output_queue_full", "submit 3",
                "discard 3 after payload_invalid", "discard 0 after device_failure",
                "discard 1 after device_failure", "submit 4", "discard 4 after device_failure",
            ]
        );
        let failures = shared.failures();
        assert_eq!(failures.untimed.map(|(_, reason)| reason), Some(FAIL_ALSA_XRUN), "untimed device failure");
        assert_eq!(failures.timed.map(|(_, _, reason)| reason), Some(FAIL_HAPTIC_PAYLOAD_INVALID));
    }

    // ---- Codex 109 fix round ---------------------------------------------------

    /// An ALSA lane on a simulated stream started at virtual `t_start`, its
    /// row receiver and shared state (driven directly, deterministically).
    fn virtual_lane(sim: SimPcmConfig, t_start: u64) -> (AlsaLane, Receiver<Row>, Arc<Shared>) {
        let (rows_tx, rows_rx) = mpsc::sync_channel(256);
        let shared = Arc::new(Shared::default());
        let mut pcm = SimPcm::new(sim);
        let mut engine = HapticPlacement::new(sim_placement(), pcm.params()).unwrap();
        let mut clock = || t_start;
        engine.prefill(&mut pcm, &mut clock).unwrap();
        pcm.start(t_start).unwrap();
        let first = pcm.status(t_start).unwrap();
        engine.begin_epoch(t_start, first, t_start).unwrap();
        (AlsaLane::new(Box::new(pcm), engine, rows_tx, shared.clone()), rows_rx, shared)
    }

    /// Write the lane's rows plus its counters through the real writer.
    fn ledger_of(lane: &AlsaLane, rows_rx: &Receiver<Row>, shared: Arc<Shared>, name: &str) -> (L2Summary, Vec<serde_json::Value>) {
        let mut input: Vec<Row> = rows_rx.try_iter().collect();
        input.push(Row::LaneStats { modality: L2Modality::Haptic, stats: lane.stats() });
        let (tx, rx) = mpsc::sync_channel(input.len() + 1);
        for row in input {
            tx.send(row).unwrap();
        }
        tx.send(Row::End).unwrap();
        drop(tx);
        let path = tmp(name);
        let file = OpenOptions::new().write(true).create_new(true).open(&path).unwrap();
        let sinks = L2Sinks { pc: L2SinkKind::Null, haptic: L2SinkKind::Alsa };
        let summary = writer_loop(BufWriter::new(file), rx, "l2-test".into(), sinks, false, shared, 64).unwrap();
        let rows = rows(&path);
        let _ = std::fs::remove_file(&path);
        (summary, rows)
    }

    /// Codex 109 P1 regression 1 at ledger level: healthy status 3,000 ->
    /// receiver abort 4,000 -> device error 4,500 -> detected 5,000. The
    /// sentinel claims NO occurrence time (null, not determinable), so it
    /// cannot order the failure before or after the abort; the interval
    /// [3,000, 5,000] and the detection are kept apart.
    #[test]
    fn codex109_ledger_good_3000_abort_4000_error_4500_detected_5000_is_undeterminable() {
        let mut sim = SimPcmConfig::new(512, 16_384);
        sim.faults.push(SimFault::StatusError { after_start_us: 3_500 }); // 4,500 absolute
        let (mut lane, rows_rx, shared) = virtual_lane(sim, 1_000);
        let healthy = lane.pcm.status(3_000).unwrap();
        lane.engine.observe(&healthy, 3_000, 3_000).unwrap();
        shared.note_receiver_fault(4_000, "receiver abort");
        let error = lane.pcm.status(5_000).unwrap_err();
        let failure = lane.engine.failure_from_error(&error, None, 5_000, "status");
        lane.fail(failure);
        let (summary, rows) = ledger_of(&lane, &rows_rx, shared, "c109-seq1");
        assert_eq!(summary.first_integrity_failure_reason.as_deref(), Some(FAIL_ALSA_STATUS_ERROR));
        assert_eq!(summary.first_integrity_failure_us, None);
        assert!(!summary.first_failure_time_determinable);
        assert_eq!(summary.first_integrity_failure_detected_us, Some(5_000));
        assert_eq!(summary.receiver_fault.as_ref().map(|(t, _)| *t), Some(4_000));
        let end = sentinel(&rows);
        assert!(end["first_integrity_failure_us"].is_null());
        assert_eq!(end["first_failure_time_determinable"], false);
        assert_eq!(end["receiver_fault_us"], 4_000);
        let failure = &end["lanes"]["haptic"]["device_failure"];
        assert_eq!(failure["observation_interval_us"], serde_json::json!([3_000, 5_000]));
        assert!(failure["kernel_trigger_us"].is_null());
        assert_no_raw_ns(&rows);
    }

    /// Codex 109 P1 regression 2 at ledger level: the healthy snapshot is
    /// taken at 3,000 but its call returns at 5,000; the device fails at
    /// 4,000 and is detected at 6,000. The interval starts at the pre-call
    /// read 3,000 (a true lower bound), never at the return 5,000.
    #[test]
    fn codex109_ledger_snapshot_3000_error_4000_return_5000_starts_the_interval_before_the_call() {
        let (mut lane, rows_rx, shared) = virtual_lane(SimPcmConfig::new(512, 16_384), 1_000);
        let healthy = lane.pcm.status(3_000).unwrap();
        lane.engine.observe(&healthy, 3_000, 5_000).unwrap();
        let error = PcmError::Other { errno: libc::EIO, call: "status" };
        let failure = lane.engine.failure_from_error(&error, None, 6_000, "status");
        lane.fail(failure);
        let (summary, rows) = ledger_of(&lane, &rows_rx, shared, "c109-seq2");
        assert_eq!(summary.first_integrity_failure_us, None);
        assert!(!summary.first_failure_time_determinable);
        let failure = &sentinel(&rows)["lanes"]["haptic"]["device_failure"];
        assert_eq!(failure["observation_interval_us"], serde_json::json!([3_000, 6_000]));
        assert_eq!(failure["last_good_status_t0_us"], 3_000);
    }

    /// Codex 109 P1-2 regression through the real lane: `finish` called
    /// right after the onset output of a single 528- or 534-frame tick. The
    /// lane keeps feeding and observing until the tick's LAST frame passed
    /// (hw_pos >= n_k + frames) before it drops the stream.
    #[test]
    fn closing_right_after_the_onset_output_waits_for_the_end_of_the_tick() {
        // Tick 0 has 88 samples (528 frames), tick 1 has 89 (534 frames).
        for (seq, frames) in [(0u32, 528u64), (1, 534)] {
            let path = tmp(&format!("alsa-sim-tail-{frames}"));
            let out = L2Output::start(&path, alsa_config(Vec::new())).unwrap();
            let s = out.submitter();
            assert_eq!(s.submit_payload(haptic(seq), now_us(), Some(tick_payload(seq))), L2SubmitOutcome::Submitted);
            // Wait for the onset output row (F = 100 ms in this config).
            let started = std::time::Instant::now();
            while !std::fs::read_to_string(&path).unwrap().contains("\"role\":\"output\"") {
                assert!(started.elapsed() < Duration::from_secs(5), "no onset output");
                std::thread::sleep(Duration::from_micros(200));
            }
            drop(s);
            let summary = out.finish().unwrap();
            assert!(summary.integrity_ok(PC_FAKE_HAPTIC_ALSA), "{summary:?}");
            let rows = rows(&path);
            let output = rows.iter().find(|r| r["role"] == "output").unwrap();
            assert_eq!(output["output_detail"]["frames"], frames);
            let n_k = output["output_detail"]["n_k"].as_i64().unwrap();
            let lane = &sentinel(&rows)["lanes"]["haptic"];
            assert_eq!(lane["last_tick_end_frame"].as_i64(), Some(n_k + frames as i64));
            assert_eq!(lane["tail_played_at_close"], true, "{frames}: played to its end");
            assert_eq!(lane["unplayed_tick_frames_at_close"], 0);
            assert!(lane["last_hw_pos"].as_i64().unwrap() >= n_k + frames as i64);
            let _ = std::fs::remove_file(&path);
        }
    }

    /// A stand-in lane for `await_lane_ready`: its behaviour is driven only
    /// by channels, so every branch is deterministic (no sleeps).
    fn stand_in_lane(
        behaviour: impl FnOnce(mpsc::Sender<std::result::Result<u32, String>>, Receiver<SinkMsg>) + Send + 'static,
    ) -> (Receiver<std::result::Result<u32, String>>, JoinHandle<()>, SyncSender<SinkMsg>) {
        let (ready_tx, ready_rx) = mpsc::channel();
        let (items_tx, items_rx) = mpsc::sync_channel::<SinkMsg>(4);
        let lane = std::thread::spawn(move || behaviour(ready_tx, items_rx));
        (ready_rx, lane, items_tx)
    }

    /// Ready timeout, deterministically: the stand-in lane never reports and
    /// only exits (closing its "device") once its queue disconnects — which
    /// happens only when `await_lane_ready` gave up waiting. The error then
    /// says the device is closed, and it IS closed before the return.
    #[test]
    fn a_ready_timeout_closes_the_queue_and_returns_after_the_lane_released_the_device() {
        use std::sync::atomic::AtomicBool;
        let closed = Arc::new(AtomicBool::new(false));
        let flag = closed.clone();
        let (ready_rx, lane, items_tx) = stand_in_lane(move |_ready, items| {
            while items.recv().is_ok() {}
            flag.store(true, Ordering::SeqCst);
        });
        let error = await_lane_ready("ALSA haptic", ready_rx, lane, items_tx, Duration::from_millis(1), Duration::from_secs(60))
            .err()
            .expect("not ready");
        let error = format!("{error:#}");
        assert!(error.contains("not ready within 1000 us") && error.contains("device is closed"), "{error}");
        assert!(closed.load(Ordering::SeqCst), "closed before await_lane_ready returned");
    }

    /// A lane that cannot exit within the release bound (held on a gate the
    /// test controls) is reported as possibly still holding the device.
    #[test]
    fn a_lane_that_does_not_exit_within_the_release_bound_is_reported() {
        use std::sync::atomic::AtomicBool;
        let (gate_tx, gate_rx) = mpsc::channel::<()>();
        let exited = Arc::new(AtomicBool::new(false));
        let flag = exited.clone();
        let (ready_rx, lane, items_tx) = stand_in_lane(move |_ready, _items| {
            let _ = gate_rx.recv();
            flag.store(true, Ordering::SeqCst);
        });
        let error = await_lane_ready("ALSA haptic", ready_rx, lane, items_tx, Duration::from_millis(1), Duration::from_millis(20))
            .err()
            .expect("not ready");
        let error = format!("{error:#}");
        assert!(error.contains("may stay open until this process exits"), "{error}");
        assert!(!exited.load(Ordering::SeqCst), "still held on the gate");
        gate_tx.send(()).unwrap();
        while !exited.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
    }

    /// The other branches: an error report (lane joined), a lane that exits
    /// without reporting (joined, device closed), and a ready report.
    #[test]
    fn await_lane_ready_reports_errors_disconnects_and_ready() {
        let (ready_rx, lane, items_tx) = stand_in_lane(|ready, _items| {
            ready.send(Err("snd_pcm_open: Device or resource busy".into())).unwrap();
        });
        let error = format!("{:#}", await_lane_ready("ALSA haptic", ready_rx, lane, items_tx, Duration::from_secs(60), Duration::from_secs(60)).err().unwrap());
        assert!(error.contains("failed to start: snd_pcm_open: Device or resource busy (lane joined, device closed)"), "{error}");

        let (ready_rx, lane, items_tx) = stand_in_lane(|ready, _items| drop(ready));
        let error = format!("{:#}", await_lane_ready("ALSA haptic", ready_rx, lane, items_tx, Duration::from_secs(60), Duration::from_secs(60)).err().unwrap());
        assert!(error.contains("lane exited without reporting") && error.contains("device is closed"), "{error}");

        let (ready_rx, lane, items_tx) = stand_in_lane(|ready, items| {
            ready.send(Ok(42)).unwrap();
            while items.recv().is_ok() {}
        });
        let (ready, lane, items_tx) =
            await_lane_ready("ALSA haptic", ready_rx, lane, items_tx, Duration::from_secs(60), Duration::from_secs(60)).unwrap();
        assert_eq!(ready, 42);
        drop(items_tx);
        lane.join().unwrap();
    }

    /// Codex 110: a tick only partly written when the stream is dropped is
    /// never reported as played — the close statistics count its full extent
    /// (534 frames, 64 written, hw_pos before n_k: 534 unplayed).
    #[test]
    fn a_partly_written_tick_at_close_is_reported_unplayed() {
        let mut sim = SimPcmConfig::new(512, 16_384);
        sim.max_frames_per_write = Some(64);
        let (mut lane, rows_rx, shared) = virtual_lane(sim, 1_000);
        let t = 1_000;
        let s = L2Submission { identity: haptic(1), t_release_us: t, t_submit_us: t };
        lane.accept(s, Some(tick_payload(1)));
        let snapshot = lane.pcm.status(t).unwrap();
        lane.engine.observe(&snapshot, t, t).unwrap();
        lane.engine.fill(lane.pcm.as_mut(), snapshot.avail, &mut || t).unwrap();
        assert_eq!(lane.engine.unwritten_tick_frames(), 534 - 64, "partly written");
        lane.record_close();
        let (_, rows) = ledger_of(&lane, &rows_rx, shared, "c110-partial-close");
        let stats = &sentinel(&rows)["lanes"]["haptic"];
        assert_eq!(stats["tail_played_at_close"], false);
        assert_eq!(stats["unplayed_tick_frames_at_close"], 534);
        assert_eq!(stats["last_tick_end_frame"], 4_800 + 534);
    }

    /// Codex 110: failure detail strings carry µs values only; the raw ns
    /// behind them (a moved trigger, an htstamp that went back) appear only
    /// in the diagnostics record, never anywhere in the L2 JSONL.
    #[test]
    fn device_failure_details_carry_no_raw_ns_and_the_raw_values_go_to_diagnostics() {
        // A moved trigger: epoch trigger 1,000 us, restart at 3,000 us.
        let mut sim = SimPcmConfig::new(512, 16_384);
        sim.faults.push(SimFault::EpochReset { after_start_us: 2_000, rewind_frames: 0 });
        let (mut lane, rows_rx, shared) = virtual_lane(sim, 1_000);
        let snapshot = lane.pcm.status(4_000).unwrap();
        let failure = lane.engine.observe(&snapshot, 4_000, 4_000).unwrap_err();
        assert_eq!(failure.detail, "trigger moved: 3000 us != epoch trigger 1000 us");
        assert_eq!(failure.raw_ns, vec![("trigger_htstamp_ns", 3_000_000), ("epoch_trigger_htstamp_ns", 1_000_000)]);
        let diag_failure = failure.clone();
        lane.fail(failure);
        let (_, l2_rows) = ledger_of(&lane, &rows_rx, shared, "c110-detail-trigger");
        assert_eq!(sentinel(&l2_rows)["lanes"]["haptic"]["device_failure"]["detail"], "trigger moved: 3000 us != epoch trigger 1000 us");
        assert_no_raw_ns(&l2_rows);
        assert_raw_values_absent(&l2_rows, &[3_000_000, 1_000_000]);
        // ...and the raw values are in the diagnostics record.
        let diag_path = tmp("c110-detail-diag");
        let file = OpenOptions::new().write(true).create_new(true).open(&diag_path).unwrap();
        let diag = DiagLog { file: BufWriter::new(file), run_id: "t".into(), outputs: Vec::new(), dropped: 0 };
        diag.write(None, Some(&diag_failure)).unwrap();
        let diag_rows = rows(&diag_path);
        let record = diag_rows.iter().find(|r| r["role"] == "device_failure").unwrap();
        assert_eq!(record["raw_ns"]["trigger_htstamp_ns"], 3_000_000);
        assert_eq!(record["raw_ns"]["epoch_trigger_htstamp_ns"], 1_000_000);
        let _ = std::fs::remove_file(&diag_path);

        // An htstamp that went back by 13 ns: 3,000,000 -> 2,999,987 ns.
        let (mut lane, rows_rx, shared) = virtual_lane(SimPcmConfig::new(512, 16_384), 1_000);
        let healthy = lane.pcm.status(3_000).unwrap();
        lane.engine.observe(&healthy, 3_000, 3_000).unwrap();
        let mut back = lane.pcm.status(3_500).unwrap();
        back.htstamp_ns = 2_999_987;
        let failure = lane.engine.observe(&back, 3_500, 3_500).unwrap_err();
        assert_eq!(failure.detail, "htstamp went back: 3000 us -> 2999 us");
        assert_eq!(failure.raw_ns, vec![("previous_htstamp_ns", 3_000_000), ("htstamp_ns", 2_999_987)]);
        lane.fail(failure);
        let (_, l2_rows) = ledger_of(&lane, &rows_rx, shared, "c110-detail-htstamp");
        assert_raw_values_absent(&l2_rows, &[3_000_000, 2_999_987]);
    }

    /// Registration 11-11: raw values go ONLY to the probe diagnostics file
    /// (unit, clock/domain per ACTUAL type, origin, analysis_time_axis
    /// false); the L2 JSONL stays integer-µs CLOCK_MONOTONIC.
    #[test]
    fn raw_snapshot_values_go_only_to_the_diagnostics_file() {
        let path = tmp("alsa-sim-diag-l2");
        let diag_path = tmp("alsa-sim-diag");
        let mut config = alsa_config(Vec::new());
        config.alsa.as_mut().unwrap().diagnostics = Some(diag_path.clone());
        let out = L2Output::start(&path, config).unwrap();
        let s = out.submitter();
        for seq in 0..3u32 {
            s.submit_payload(haptic(seq), now_us(), Some(tick_payload(seq)));
            std::thread::sleep(Duration::from_millis(11));
        }
        drop(s);
        let summary = out.finish().unwrap();
        assert!(summary.integrity_ok(PC_FAKE_HAPTIC_ALSA), "{summary:?}");
        let l2 = rows(&path);
        assert_no_raw_ns(&l2);
        assert_eq!(l2[0]["alsa"]["diagnostics_file"], diag_path.display().to_string());
        let lane = &sentinel(&l2)["lanes"]["haptic"];
        assert_eq!(lane["diagnostics"]["written"], true);
        assert_eq!(lane["diagnostics"]["output_records"], 3);
        let diag = rows(&diag_path);
        assert_eq!(diag[0]["role"], "diagnostics_meta");
        assert_eq!(diag[0]["diagnostics_schema"], L2_ALSA_DIAGNOSTICS_SCHEMA);
        assert_eq!(diag[0]["analysis_time_axis"], false);
        for field in ["htstamp_ns", "trigger_htstamp_ns", "driver_htstamp_ns", "audio_htstamp_ns", "audio_tstamp_accuracy_ns"] {
            let f = &diag[0]["fields"][field];
            assert_eq!(f["unit"], "ns", "{field}");
            assert!(f["origin"].is_string(), "{field}");
        }
        assert!(diag[0]["fields"]["audio_htstamp_ns"]["clock"].is_null(), "not CLOCK_MONOTONIC");
        assert!(diag[0]["fields"]["audio_tstamp_accuracy_ns"]["kind"].as_str().unwrap().contains("not a time"));
        assert_eq!(diag[1]["role"], "stream_epoch");
        let outputs: Vec<_> = diag.iter().filter(|r| r["role"] == "output_diag").collect();
        assert_eq!(outputs.len(), 3);
        for o in outputs {
            assert!(o["raw"]["htstamp_ns"].is_u64() && o["raw"]["audio_htstamp_ns"].is_u64());
            assert_eq!(o["raw"]["audio_tstamp_type_actual"], "LINK");
            assert!(o["raw"]["audio_htstamp_domain"].as_str().unwrap().contains("link time"));
        }
        assert_eq!(diag.last().unwrap()["role"], "diagnostics_end");
        for p in [&path, &diag_path] {
            let _ = std::fs::remove_file(p);
        }
    }

    // ---- M3: the PC Wayland lane on the simulated compositor -----------------

    use crate::l2_pc::{
        PcRepresentation, SimCompositorConfig, SimWaylandFault, TierScales, FAIL_PC_DECODE_FAILED,
        FAIL_PC_PAYLOAD_INVALID, FAIL_WAYLAND_CONNECTION_LOST, FAIL_WAYLAND_FEEDBACK_MISSING,
        FAIL_WAYLAND_RECONFIGURED, FAIL_WAYLAND_UNEXPECTED_FEEDBACK,
    };

    fn tiers_meta() -> TierScales {
        let text = r#"{"schema": "phase4_s3_tiers_v1", "tiers": {
            "d8": {"tier_id": 2, "depth_scale": 4}, "d7": {"tier_id": 3, "depth_scale": 8},
            "d6": {"tier_id": 4, "depth_scale": 16}}}"#;
        TierScales::parse(text.as_bytes(), "test-tiers").unwrap()
    }

    /// A small fast simulated display: 64x48 (canvas 48), vblank every 5 ms.
    fn sim_display() -> SimCompositorConfig {
        let mut sim = SimCompositorConfig::new(64, 48);
        sim.refresh_us = 5_000;
        sim
    }

    const PC_WAYLAND_HAPTIC_NULL: L2Sinks = L2Sinks { pc: L2SinkKind::Wayland, haptic: L2SinkKind::Null };

    fn wayland_config(sim: SimCompositorConfig, representation: PcRepresentation) -> L2Config {
        let mut c = config_sinks(PC_WAYLAND_HAPTIC_NULL, None, 64);
        c.wayland = Some(WaylandSinkConfig {
            buffers: sim.buffers,
            backend: WaylandBackendKind::Simulated(sim),
            representation,
            tiers: tiers_meta(),
            ready_timeout_us: 5_000_000,
            release_timeout_us: 5_000_000,
            diagnostics: None,
        });
        c.report_window_us = 1_000_000;
        c
    }

    fn pc_frame(seq: u32) -> L2Identity {
        L2Identity { modality: L2Modality::Pc, tier: 2, seq, pts_us: seq as u64 * 33_333, event_id: seq + 1, route: None }
    }

    /// d8-like synthetic records (coordinates up to 255, scale 4) and their
    /// DracoPy encoding, from the skew-draco-sys fixtures (no dataset bytes).
    fn pc_payload(representation: PcRepresentation) -> bytes::Bytes {
        let name = match representation {
            PcRepresentation::Bin => "synthetic_255.bin",
            PcRepresentation::Draco => "synthetic_255.drc",
        };
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../skew-draco-sys/tests/fixtures").join(name);
        bytes::Bytes::from(std::fs::read(path).unwrap())
    }

    /// Submit `n` PC frames `gap_ms` apart and finish.
    fn run_pc(config: L2Config, n: u32, gap_ms: u64, name: &str) -> (L2Summary, Vec<serde_json::Value>, PathBuf) {
        let representation = config.wayland.as_ref().unwrap().representation;
        let payload = pc_payload(representation);
        let path = tmp(name);
        let out = L2Output::start(&path, config).unwrap();
        let s = out.submitter();
        for seq in 0..n {
            assert_eq!(s.submit_payload(pc_frame(seq), now_us(), Some(payload.clone())), L2SubmitOutcome::Submitted);
            std::thread::sleep(Duration::from_millis(gap_ms));
        }
        drop(s);
        let summary = out.finish().unwrap();
        let rows = rows(&path);
        (summary, rows, path)
    }

    /// Every submission has exactly one terminal (output or discard).
    fn terminals_by_key(rows: &[serde_json::Value]) -> HashMap<String, Vec<serde_json::Value>> {
        let mut map: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
        for r in rows.iter().filter(|r| r["role"] == "output" || (r["role"] == "explicit_discard" && r["discard_phase"] == "after_submit")) {
            map.entry(key(r)).or_default().push(r.clone());
        }
        map
    }

    /// Bin frames, paced: each committed at an opportunity, presented at the
    /// next vblank, one output per submission with its commit evidence; v1.4
    /// meta, contract and sentinel; no decode fields for bin; the simulated
    /// compositor never saw a write into a buffer it held.
    #[test]
    fn the_wayland_lane_presents_each_frame_once_with_its_feedback_evidence() {
        let sim = sim_display();
        let observer = sim.observer.clone();
        let (summary, rows, path) = run_pc(wayland_config(sim, PcRepresentation::Bin), 8, 15, "pc-sim-happy");
        assert!(summary.integrity_ok(PC_WAYLAND_HAPTIC_NULL), "{summary:?}");
        assert_eq!((summary.submitted, summary.output), (8, 8));
        let meta = &rows[0];
        assert_eq!(meta["l2_schema"], "event-pair-l2-v1.4");
        assert_eq!(meta["sinks"]["pc"], "wayland");
        assert_eq!(meta["device_output"], false, "a simulated compositor is never a device output");
        let contract = &meta["sink_contracts"]["pc"];
        assert_eq!((contract["kind"].as_str(), contract["backend"].as_str()), (Some("wayland"), Some("simulated")));
        assert_eq!((contract["device_output"].as_bool(), contract["resolves"].as_bool()), (Some(false), Some(true)));
        for field in ["commit_id", "t_commit_us", "t_render_start_us", "refresh_us", "msc", "flags"] {
            assert!(contract["output_detail"][field].is_string(), "schema of {field}");
        }
        let wayland = &meta["wayland"];
        assert_eq!(wayland["canvas_side_px"], 48);
        assert_eq!(wayland["startup"]["clock"], "CLOCK_MONOTONIC");
        assert_eq!(wayland["representation"], "bin");
        assert!(wayland["decoder"].is_null(), "no decoder for bin");
        assert_eq!(wayland["tiers_meta"]["tiers_by_header_tier"]["2"]["depth_scale"], 4);
        assert_no_raw_ns(&rows);
        let terminals = terminals_by_key(&rows);
        let mut previous_commit = 0u64;
        for submit in rows.iter().filter(|r| r["role"] == "submit") {
            let t = &terminals[&key(submit)];
            assert_eq!(t.len(), 1, "exactly one terminal");
            let o = &t[0];
            assert_eq!(o["role"], "output");
            assert!(o.get("t_decode_start_us").is_none() && o.get("t_decode_end_us").is_none(), "bin: no decode fields");
            let d = &o["output_detail"];
            assert_eq!((d["device"].as_str(), d["backend"].as_str()), (Some("wayland"), Some("simulated")));
            let commit_id = d["commit_id"].as_u64().unwrap();
            assert!(commit_id > previous_commit, "one commit per frame, in release order");
            previous_commit = commit_id;
            let v = |k: &str| o[k].as_u64().unwrap();
            let dv = |k: &str| d[k].as_u64().unwrap();
            assert!(v("t_submit_us") <= dv("t_render_start_us") && dv("t_render_start_us") <= dv("t_render_end_us"));
            assert!(dv("t_render_end_us") <= dv("t_commit_us") && dv("t_commit_us") <= v("t_output_us"));
            assert!(v("t_output_us") <= v("t_callback_us") && v("t_callback_us") <= v("t_log_arrival_us"));
            assert_eq!(d["refresh_us"], 5_000);
            assert_eq!(d["flag_names"], serde_json::json!(["vsync", "hw_clock", "hw_completion"]));
            assert_eq!(d["points"], 1_500);
            assert_eq!(d["depth_scale"], 4);
        }
        let end = sentinel(&rows);
        assert_eq!(end["integrity_ok"], true);
        let lane = &end["lanes"]["pc"];
        assert_eq!((lane["kind"].as_str(), lane["backend"].as_str()), (Some("wayland"), Some("simulated")));
        assert_eq!(lane["frames_rendered"], 8);
        assert_eq!(lane["engine"]["commits"], 8);
        assert_eq!(lane["engine"]["presented"], 8);
        assert_eq!(lane["committed_without_feedback_at_close"], 0);
        assert!(lane["device_failure"].is_null());
        assert_eq!(observer.writes_into_held_buffers.load(Ordering::SeqCst), 0);
        assert!(observer.dropped.load(Ordering::SeqCst), "disconnected at close");
        let _ = std::fs::remove_file(&path);
    }

    /// Draco frames released in bursts: some are replaced in the pending
    /// slot before any commit (`superseded_before_commit`), the rest are
    /// presented; EVERY after-submit terminal of a decoded frame carries the
    /// decode times, which precede the render; bin and draco frames of the
    /// same content draw the same number of points.
    #[test]
    fn draco_terminals_carry_decode_times_and_superseded_frames_are_discarded_before_commit() {
        let mut sim = sim_display();
        sim.refresh_us = 40_000; // long frames: bursts must wait
        let config = wayland_config(sim, PcRepresentation::Draco);
        let payload = pc_payload(PcRepresentation::Draco);
        let path = tmp("pc-sim-draco");
        let out = L2Output::start(&path, config).unwrap();
        let s = out.submitter();
        for seq in 0..12u32 {
            s.submit_payload(pc_frame(seq), now_us(), Some(payload.clone()));
            if seq % 4 == 3 {
                std::thread::sleep(Duration::from_millis(60));
            }
        }
        drop(s);
        let summary = out.finish().unwrap();
        assert!(summary.integrity_ok(PC_WAYLAND_HAPTIC_NULL), "{summary:?}");
        let rows = rows(&path);
        let terminals = terminals_by_key(&rows);
        let (mut outputs, mut superseded) = (0, 0);
        for submit in rows.iter().filter(|r| r["role"] == "submit") {
            let t = &terminals[&key(submit)];
            assert_eq!(t.len(), 1, "exactly one terminal");
            let r = &t[0];
            let start = r["t_decode_start_us"].as_u64().expect("decode start on every draco terminal");
            let end = r["t_decode_end_us"].as_u64().expect("decode end");
            assert!(r["t_submit_us"].as_u64().unwrap() <= start && start <= end);
            match r["role"].as_str().unwrap() {
                "output" => {
                    outputs += 1;
                    assert!(end <= r["output_detail"]["t_render_start_us"].as_u64().unwrap());
                    assert_eq!(r["output_detail"]["points"], 1_500, "same points as the bin frame");
                }
                _ => {
                    assert_eq!(r["reason"], DISCARD_SUPERSEDED_BEFORE_COMMIT);
                    assert!(end <= r["t_discard_us"].as_u64().unwrap());
                    superseded += 1;
                }
            }
        }
        assert_eq!(outputs + superseded, 12);
        assert!(outputs >= 3 && superseded >= 1, "outputs {outputs}, superseded {superseded}");
        let meta = &rows[0];
        assert_eq!(meta["wayland"]["decoder"]["version"], "1.5.7");
        assert_eq!(meta["wayland"]["decoder"]["threads"], 1);
        let lane = &sentinel(&rows)["lanes"]["pc"];
        assert_eq!(lane["superseded_before_commit"], superseded);
        assert!(lane["decode_us_max"].as_u64().is_some());
        let _ = std::fs::remove_file(&path);
    }

    /// `discarded` feedback is the frame's after-submit terminal (not an
    /// integrity failure); a frame callback alone never makes an output.
    #[test]
    fn a_compositor_discard_is_an_after_submit_terminal() {
        let mut sim = sim_display();
        sim.faults.push(SimWaylandFault::DiscardFeedback { commit_id: 2 });
        let (summary, rows, path) = run_pc(wayland_config(sim, PcRepresentation::Bin), 3, 15, "pc-sim-discarded");
        assert!(summary.integrity_ok(PC_WAYLAND_HAPTIC_NULL), "{summary:?}");
        assert_eq!((summary.output, summary.discarded_after_submit), (2, 1));
        let discard = rows.iter().find(|r| r["role"] == "explicit_discard").unwrap();
        assert_eq!((discard["reason"].as_str(), discard["discard_phase"].as_str()), (Some(DISCARD_COMPOSITOR_DISCARDED), Some("after_submit")));
        assert_eq!(discard["seq"], 1);
        assert_eq!(sentinel(&rows)["lanes"]["pc"]["engine"]["compositor_discarded"], 1);
        let _ = std::fs::remove_file(&path);
    }

    /// A lost compositor connection is an untimed PC device failure:
    /// `lanes.pc.device_failure`, first_integrity_failure_us null, later
    /// submissions `device_failure` after submit, and nothing committed after.
    #[test]
    fn a_compositor_disconnect_is_an_untimed_pc_device_failure() {
        let mut sim = sim_display();
        sim.faults.push(SimWaylandFault::Disconnect { after_start_us: 60_000 });
        let observer = sim.observer.clone();
        let (summary, rows, path) = run_pc(wayland_config(sim, PcRepresentation::Bin), 8, 15, "pc-sim-disconnect");
        assert!(!summary.integrity_ok(PC_WAYLAND_HAPTIC_NULL));
        assert_eq!(summary.first_integrity_failure_reason.as_deref(), Some(FAIL_WAYLAND_CONNECTION_LOST));
        assert_eq!(summary.first_integrity_failure_us, None, "no occurrence time is claimed");
        assert!(!summary.first_failure_time_determinable);
        let end = sentinel(&rows);
        let failure = &end["lanes"]["pc"]["device_failure"];
        assert_eq!(failure["reason"], FAIL_WAYLAND_CONNECTION_LOST);
        assert_eq!(failure["occurrence_time_determinable"], false);
        let interval = failure["observation_interval_us"].as_array().unwrap();
        assert!(interval[0].as_u64() <= interval[1].as_u64());
        let discards: Vec<_> = rows.iter().filter(|r| r["role"] == "explicit_discard").collect();
        assert!(!discards.is_empty(), "frames after the failure");
        assert!(discards.iter().all(|d| d["reason"] == DISCARD_DEVICE_FAILURE && d["discard_phase"] == "after_submit"));
        // Every submission has at most one terminal; outputs + discards +
        // unresolved (a commit whose feedback can never come) = submitted.
        assert!(summary.conservation_ok);
        assert_eq!(summary.output + summary.discarded_after_submit + summary.submitted_unresolved, 8);
        assert!(observer.commits.load(Ordering::SeqCst) < 8);
        let _ = std::fs::remove_file(&path);
    }

    /// A commit whose feedback never comes stays unresolved (a committed
    /// frame ends only through its feedback) and is the device failure
    /// `wayland_feedback_missing` at close.
    #[test]
    fn missing_feedback_at_close_leaves_the_frame_unresolved_and_fails_the_pc_device() {
        let mut sim = sim_display();
        sim.faults.push(SimWaylandFault::DropFeedback { commit_id: 2 });
        let mut config = wayland_config(sim, PcRepresentation::Bin);
        config.report_window_us = 200_000;
        let (summary, rows, path) = run_pc(config, 3, 15, "pc-sim-missing");
        assert!(!summary.integrity_ok(PC_WAYLAND_HAPTIC_NULL));
        assert_eq!(summary.output, 2);
        assert_eq!(summary.submitted_unresolved_by_track[L2Modality::Pc.index()], 1);
        assert_eq!(summary.first_integrity_failure_reason.as_deref(), Some(FAIL_WAYLAND_FEEDBACK_MISSING));
        assert!(!summary.first_failure_time_determinable);
        let lane = &sentinel(&rows)["lanes"]["pc"];
        assert_eq!(lane["committed_without_feedback_at_close"], 1);
        assert_eq!(lane["device_failure"]["reason"], FAIL_WAYLAND_FEEDBACK_MISSING);
        assert!(rows.iter().all(|r| !(r["seq"] == 1 && (r["role"] == "output" || r["role"] == "explicit_discard"))));
        let _ = std::fs::remove_file(&path);
    }

    /// A second `presented` for one commit is a timed integrity failure at
    /// its receipt; the frame still has exactly one output.
    #[test]
    fn a_duplicate_feedback_is_a_timed_integrity_failure() {
        let mut sim = sim_display();
        sim.faults.push(SimWaylandFault::DuplicateFeedback { commit_id: 1 });
        let (summary, rows, path) = run_pc(wayland_config(sim, PcRepresentation::Bin), 2, 15, "pc-sim-duplicate");
        assert!(!summary.integrity_ok(PC_WAYLAND_HAPTIC_NULL));
        assert_eq!(summary.output, 2);
        assert_eq!(summary.first_integrity_failure_reason.as_deref(), Some(FAIL_WAYLAND_UNEXPECTED_FEEDBACK));
        assert!(summary.first_failure_time_determinable);
        let first_output = rows.iter().find(|r| r["role"] == "output" && r["seq"] == 0).unwrap();
        let at = summary.first_integrity_failure_us.unwrap();
        assert!(at >= first_output["t_callback_us"].as_u64().unwrap(), "timed at the duplicate's receipt");
        let lane = &sentinel(&rows)["lanes"]["pc"];
        assert_eq!(lane["engine"]["unexpected_feedback"], 1);
        assert!(lane["anomalies"][0].as_str().unwrap().contains("already resolved"));
        let _ = std::fs::remove_file(&path);
    }

    /// A reconfigure after ready is a PC device failure; frames after it are
    /// `device_failure`, already committed ones still get their feedback.
    #[test]
    fn a_reconfigure_after_ready_is_a_pc_device_failure() {
        let mut sim = sim_display();
        sim.faults.push(SimWaylandFault::Reconfigure { after_start_us: 50_000 });
        let (summary, rows, path) = run_pc(wayland_config(sim, PcRepresentation::Bin), 6, 15, "pc-sim-reconfigure");
        assert_eq!(summary.first_integrity_failure_reason.as_deref(), Some(FAIL_WAYLAND_RECONFIGURED));
        assert!(!summary.first_failure_time_determinable);
        assert_eq!(summary.submitted_unresolved, 0, "committed frames still resolved by feedback");
        assert!(rows.iter().any(|r| r["reason"] == DISCARD_DEVICE_FAILURE));
        let _ = std::fs::remove_file(&path);
    }

    /// The compositor never releases a buffer: after the free ones are used
    /// the pending frame waits — a held buffer is never written — and stays
    /// unresolved at close.
    #[test]
    fn held_buffers_are_never_written_and_the_frame_waits() {
        let mut sim = sim_display();
        sim.buffers = 2;
        sim.faults.push(SimWaylandFault::NeverRelease);
        let observer = sim.observer.clone();
        let mut config = wayland_config(sim, PcRepresentation::Bin);
        config.report_window_us = 150_000;
        let (summary, rows, path) = run_pc(config, 3, 15, "pc-sim-held");
        assert_eq!(observer.writes_into_held_buffers.load(Ordering::SeqCst), 0);
        assert_eq!(observer.commits.load(Ordering::SeqCst), 1, "buffer 0 (startup) and 1 stay held");
        assert_eq!(summary.output, 1);
        let lane = &sentinel(&rows)["lanes"]["pc"];
        assert!(lane["engine"]["commit_waits_for_buffer"].as_u64().unwrap() >= 1);
        assert_eq!(lane["pending_at_close"], true);
        assert_eq!(summary.submitted_unresolved, 1, "the pending frame");
        assert_eq!(rows.iter().filter(|r| r["reason"] == DISCARD_SUPERSEDED_BEFORE_COMMIT).count(), 1);
        let _ = std::fs::remove_file(&path);
    }

    /// Registration 3-2: clk_id must be CLOCK_MONOTONIC or the run refuses
    /// to start — no meta row.
    #[test]
    fn a_presentation_clock_other_than_monotonic_refuses_the_run() {
        let mut sim = sim_display();
        sim.clock_id = libc::CLOCK_MONOTONIC_RAW as u32;
        let path = tmp("pc-sim-clock");
        let error = L2Output::start(&path, wayland_config(sim, PcRepresentation::Bin)).err().expect("refused");
        let error = format!("{error:#}");
        assert!(error.contains("not CLOCK_MONOTONIC") && error.contains("refuses to start"), "{error}");
        assert_eq!(std::fs::read(&path).unwrap(), b"", "no meta row");
        let _ = std::fs::remove_file(&path);
    }

    /// An unknown tier and a payload the decoder refuses: submit + an
    /// after-submit discard + a timed integrity failure at t_submit.
    #[test]
    fn unknown_tiers_and_undecodable_payloads_are_terminated_and_fail_integrity() {
        for (representation, payload, tier, reason, failure) in [
            (PcRepresentation::Bin, pc_payload(PcRepresentation::Bin), 9u16, DISCARD_PAYLOAD_INVALID, FAIL_PC_PAYLOAD_INVALID),
            (PcRepresentation::Draco, bytes::Bytes::from_static(b"not draco"), 2, DISCARD_DECODE_FAILED, FAIL_PC_DECODE_FAILED),
        ] {
            let path = tmp(&format!("pc-sim-invalid-{}", representation.as_str()));
            let out = L2Output::start(&path, wayland_config(sim_display(), representation)).unwrap();
            let s = out.submitter();
            let identity = L2Identity { tier, ..pc_frame(0) };
            s.submit_payload(identity, now_us(), Some(payload));
            drop(s);
            let summary = out.finish().unwrap();
            assert!(!summary.integrity_ok(PC_WAYLAND_HAPTIC_NULL));
            assert_eq!(summary.first_integrity_failure_reason.as_deref(), Some(failure));
            let rows = rows(&path);
            let submit = rows.iter().find(|r| r["role"] == "submit").unwrap();
            assert_eq!(summary.first_integrity_failure_us, submit["t_submit_us"].as_u64());
            let discard = rows.iter().find(|r| r["role"] == "explicit_discard").unwrap();
            assert_eq!((discard["reason"].as_str(), discard["discard_phase"].as_str()), (Some(reason), Some("after_submit")));
            assert_eq!(
                discard.get("t_decode_start_us").is_some(),
                representation == PcRepresentation::Draco,
                "decode times only where a decode ran"
            );
            assert!(summary.conservation_ok && summary.submitted_unresolved == 0);
            let _ = std::fs::remove_file(&path);
        }
    }

    /// Two device lanes: the PC lane is ready, then the haptic stream cannot
    /// start — the PC lane is closed and has disconnected before `start`
    /// returns, and the file holds no meta row.
    #[test]
    fn a_failed_haptic_start_tears_down_the_ready_pc_lane() {
        let sim = sim_display();
        let observer = sim.observer.clone();
        let mut config = wayland_config(sim, PcRepresentation::Bin);
        config.sinks = L2Sinks { pc: L2SinkKind::Wayland, haptic: L2SinkKind::Alsa };
        config.alsa = Some(sim_alsa(vec![SimFault::StartError]));
        let path = tmp("pc-sim-teardown");
        let error = format!("{:#}", L2Output::start(&path, config).err().expect("refused"));
        assert!(error.contains("ALSA haptic sink failed to start"), "{error}");
        assert!(error.contains("the PC Wayland lane was closed and has disconnected"), "{error}");
        assert!(observer.dropped.load(Ordering::SeqCst), "the compositor connection is gone");
        assert_eq!(std::fs::read(&path).unwrap(), b"", "no meta row");
        let _ = std::fs::remove_file(&path);
    }

    /// Both tracks on device sinks: the file is a device output only with
    /// the display backend AND the ALSA device (pure config, nothing opened).
    #[test]
    fn device_output_needs_the_display_and_the_alsa_device() {
        let mut config = wayland_config(sim_display(), PcRepresentation::Bin);
        config.sinks = L2Sinks { pc: L2SinkKind::Wayland, haptic: L2SinkKind::Alsa };
        config.alsa = Some(sim_alsa(Vec::new()));
        assert!(config.validate().is_ok());
        assert!(!config.device_output_of(L2Modality::Pc) && !config.device_output());
        config.wayland.as_mut().unwrap().backend = WaylandBackendKind::Display;
        assert!(config.device_output_of(L2Modality::Pc));
        assert!(!config.device_output(), "the haptic stream is simulated");
        config.alsa.as_mut().unwrap().backend = AlsaBackend::Device { name: "hw:1,0".into() };
        assert!(config.device_output(), "display + ALSA device");
        // Wayland is a PC sink only, and its options need it.
        let mut bad = config.clone();
        bad.sinks.haptic = L2SinkKind::Wayland;
        assert!(bad.validate().is_err());
        let mut bad = config.clone();
        bad.sinks.pc = L2SinkKind::Fake;
        bad.fake = Some(FakeSinkConfig::offsets(1, 1));
        assert!(bad.validate().is_err(), "Wayland options without the Wayland sink");
        let mut bad = config;
        bad.wayland.as_mut().unwrap().buffers = 1;
        assert!(bad.validate().is_err());
    }
}
