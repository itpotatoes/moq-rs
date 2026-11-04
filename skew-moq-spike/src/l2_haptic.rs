//! Haptic PCM output for the L2 stage (registration 1-D §3-3, §3-3a;
//! milestone M2): the [`HapticPcm`] device trait, the placement engine
//! ([`HapticPlacement`]) that decides what is written where and when an
//! output is observed, the L2 output-time formula, and a deterministic
//! simulated PCM ([`SimPcm`]) so that every rule here is testable without a
//! device. The real device (`hw:1,0` through libasound) implements the same
//! trait in `l2_alsa`; the lane thread that drives the engine and turns its
//! results into ledger rows lives in `l2_output`.
//!
//! Stream and frame numbering (§3-3a "스트림 epoch"): the lane opens the
//! device, writes `prefill_frames` of silence and starts the stream before
//! the run's warm-up. Frame `n` is the cumulative 48 kHz frame index since
//! that prepare (prefill included) — the lane is the only writer, so this
//! equals the kernel's `appl_ptr` for an `hw:` device written with `writei`.
//! A restart, a moved trigger timestamp or a DMA pointer that goes backwards
//! is an epoch reset: an integrity failure, never recovered.
//!
//! Placement (§3-3a "배치"): released ticks enter a bounded write queue and
//! are appended back to back at the current write position `w` (`n_k = w`),
//! in release order, never aligned to a planned slot. A tick is written only
//! whole (`avail >= frames`): while a tick waits nothing else is written, so
//! waiting and splitting give the same `n_k`. Each 8 kHz mono sample becomes
//! six identical stereo frames (88/89 samples -> 528/534 frames, never a
//! fixed length). With an empty queue the lane writes ONE period of silence
//! whenever the queued fill drops below the target `F`. Silence frames are
//! never an output of any tick. `Q` bounds every frame not yet written to
//! the device: the waiting queue PLUS the unwritten remainder of the tick
//! being written (§3-3a "상한", Codex 109). Without a latency bound
//! (below) a queued tick only ever leaves the write queue by being written
//! (or abandoned after a device failure).
//!
//! Latency bound, sink mode A (experiment 3, optional
//! [`PlacementConfig::latency_bound_frames`] `B`; `None` = off = the
//! placement above, unchanged): `B` bounds the SAME count `Q` bounds
//! ([`HapticPlacement::unwritten_tick_frames`]: the waiting queue plus the
//! unwritten remainder of the tick being written; the frames already in the
//! device buffer are NOT part of it). When a playable tick of `frames` is
//! offered and `unwritten + frames > B`, the OLDEST waiting tick (the front
//! of the write queue) is dropped, repeatedly, until it fits or the waiting
//! queue is empty. The tick being written (its `n_k` is fixed, possibly
//! partly written) and the offered tick itself are never dropped for `B`;
//! if the in-progress remainder alone still leaves `unwritten + frames > B`,
//! the offered tick is queued anyway (`B` bounds only waiting ticks). The
//! `Q` rule then applies to the offered tick exactly as without `B` (it can
//! refuse the offered tick after drops only if the in-progress remainder
//! plus the offered tick exceeds `Q`, i.e. never for regular ticks with
//! `Q >= 1,068`). The configured `B` (or null) is recorded in meta
//! `alsa.latency_bound_frames`. A dropped tick was already submitted: the
//! lane ends it with ONE terminal, `explicit_discard(latency_bound)` after
//! submit, before the offered tick's own row ([`Offered`]; the row-level
//! qualifications are in `l2_output::DISCARD_LATENCY_BOUND`). The drop is
//! by queue position only — the engine never looks at a tick's identity
//! (event id, slot, pts) — so anchor ticks are not protected.
//!
//! What `B` guarantees (provable from the rule; payload acceptance is the
//! same with and without `B`, any playable tick up to `buffer − period`
//! frames): right after an admission of a tick of `f` frames,
//! `unwritten_tick_frames() <= min(Q, max(B, r + f))`, where `r` is the
//! unwritten remainder of the tick being written at that admission (0 if
//! none); between admissions the count only decreases. Hence
//! `LaneStats::max_unwritten_tick_frames <= min(Q, max(B, r_max + f_max))
//! <= min(Q, max(B, 2 × the largest admitted tick))`. It is `<= B` when no
//! admitted tick exceeds `B / 2` frames — the registered 528/534-frame
//! ticks with `B >= 1,068` — but NOT in general: a larger playable tick
//! offered while nothing else waits is queued above `B` (only waiting ticks
//! are ever dropped).
//!
//! Output time (§3-3): from ONE `snd_pcm_status` snapshot, `appl_pos` is the
//! lane's own `w` at that call and `hw_pos = appl_pos − delay`. A written
//! tick is output only when `hw_pos > n_k` in that snapshot, with
//! `t_output_us = htstamp_us + round_half_away((n_k − hw_pos) × 1e6 / 48000)`
//! in signed integer arithmetic (`htstamp_us = sec·1e6 + floor(nsec/1000)`,
//! the truncation [`crate::now_us`] uses). The snapshot's DMA pointer
//! `appl_pos − (buffer − avail)` must never decrease; `delay − (buffer −
//! avail)` (the driver/codec delay folded into `delay`) is recorded, not
//! assumed to be zero.
//!
//! Playback end (§3-3a "종료", Codex 109 P1): the onset `output` of a tick
//! does not mean it has been played. The engine tracks the end of the last
//! written tick, `n_k + frames`, and is idle only once `hw_pos >= n_k +
//! frames` (its last frame passed) — never waiting for trailing silence.
//!
//! Device failures (§6-1, Codex 109 P1): until the meaning of the kernel's
//! timestamps is verified on the device, NO occurrence time is claimed. A
//! [`DeviceFailure`] keeps the observation interval (the clock read just
//! before the last healthy status call — the failure happened after that
//! call's snapshot, so after this read — up to the detection), the
//! detection time and the raw kernel trigger time, as separate fields.
//!
//! Raw device timestamps (§11-11): the ledger detail ([`AlsaOutputDetail::to_json`])
//! carries only integer µs CLOCK_MONOTONIC time fields. The raw nanosecond
//! values and the audio-timestamp domain value (`LINK`: not
//! CLOCK_MONOTONIC) are kept in [`RawSnapshot`] for the probe-only
//! diagnostics file and never enter the L2 JSONL.
//!
//! Placement modes (experiment 4, [`PlacementConfig::mode`]): `None` is the
//! legacy engine above (experiments 2/3, L2 v1.7), `Fifo` is the same engine
//! path with `B` refused (only the L2 meta differs, v1.8), `Aligned` is R2
//! below. Every rule above applies unchanged to `None` and `Fifo`.
//!
//! Aligned placement R2 (design `md/20261104_실험4_설계초안.md` §5, contract
//! `md/20261104_실험4_계약.md` §2; [`HapticPlacement::place`]). The plan is a
//! frame-indexed queue covering `[appl_pos, planned_end)` of segments — a
//! silence run, or a slice of a tick (identified by its placement index and
//! carrying its seq) and its frame offset — with `appl_pos = w` (frames
//! actually written) and `planned_end = w + planned frames >= appl_pos` by
//! construction (writing silence past `planned_end` leaves it at `w`). A
//! placement takes `hw_pos` and `htstamp_us` from the LAST HEALTHY snapshot
//! and `appl_pos = w` at placement time (no status call of its own) and, in
//! signed i128 with [`div_round_half_away`]:
//! `n* = hw_pos + round_half_away((t_release_us + c − htstamp_us) × 48000 / 1e6)`,
//! `floor_us = htstamp_us + round_half_away((appl_pos − hw_pos) × 1e6 / 48000) − t_release_us`
//! (recorded for every attempt, before any cut or discard). With L the
//! tick's frames: (i) gap, `n* >= planned_end`: silence `planned_end..n*`
//! then the tick, refused (plan untouched, `placement_queue_full` after
//! submit) if the plan's waiting frames would exceed `Q`
//! (`n* + L − appl_pos > queue_limit_frames`); (ii) queue_overlap,
//! `appl_pos <= n* < planned_end`: every planned frame at or after `n*` is
//! removed (silence included; one trim per affected tick; a tick whose start
//! is removed — never written — is `occluded`), then the tick goes at `n*`;
//! (iii) device_overlap, `n* < appl_pos < n* + L`: no rewind; the whole plan
//! is removed by the (ii) rule, the head `appl_pos − n*` is cut and the rest
//! goes at `appl_pos` (its `n_k`, `head_cut_frames > 0`); (iv) too_late,
//! `n* + L <= appl_pos`: discarded, plan untouched. A tick trimmed after its
//! start was written shrinks its `frames`, the `tick_extent` end and
//! `last_tick_end_frame` (so `idle` and the close statistics stay true). The
//! write-ahead fill writes one period-sized chunk at a time from the plan
//! front while `w − hw_pos < W` (padding with silence when the plan runs
//! out), never all available space. A tick enters `in_flight` at its first
//! written frame and is output once no planned frame of it remains and a
//! snapshot shows `hw_pos > n_k` (the formula below, unchanged). Waiting
//! silence never exceeds `Q`. The latency bound `B` does not exist here.

use std::collections::{BTreeMap, VecDeque};

use crate::l2_output::L2Submission;

/// Device rate, channels and source rate (§3-3).
pub const PCM_RATE_HZ: u64 = 48_000;
pub const PCM_CHANNELS: usize = 2;
pub const SOURCE_RATE_HZ: u64 = crate::PCM_SAMPLE_RATE_HZ;
/// 8 kHz -> 48 kHz by sample repetition.
pub const UPSAMPLE_FACTOR: u64 = PCM_RATE_HZ / SOURCE_RATE_HZ;
/// The largest regular tick at 8 kHz / 90 Hz (89 samples) in 48 kHz frames;
/// the buffer must hold one of these plus a period.
pub const REGULAR_TICK_MAX_FRAMES: u64 = 89 * UPSAMPLE_FACTOR;
/// Upper bound of a tick payload in 8 kHz samples (one second).
pub const MAX_TICK_SAMPLES: usize = 8_000;

/// `snd_pcm_audio_tstamp_type_t` values (alsa-lib `pcm.h`).
pub const AUDIO_TSTAMP_TYPE_COMPAT: u32 = 0;
pub const AUDIO_TSTAMP_TYPE_DEFAULT: u32 = 1;
pub const AUDIO_TSTAMP_TYPE_LINK: u32 = 2;
pub const AUDIO_TSTAMP_TYPE_LINK_ABSOLUTE: u32 = 3;
pub const AUDIO_TSTAMP_TYPE_LINK_ESTIMATED: u32 = 4;
pub const AUDIO_TSTAMP_TYPE_LINK_SYNCHRONIZED: u32 = 5;

pub fn audio_tstamp_type_name(value: u32) -> &'static str {
    match value {
        AUDIO_TSTAMP_TYPE_COMPAT => "COMPAT",
        AUDIO_TSTAMP_TYPE_DEFAULT => "DEFAULT",
        AUDIO_TSTAMP_TYPE_LINK => "LINK",
        AUDIO_TSTAMP_TYPE_LINK_ABSOLUTE => "LINK_ABSOLUTE",
        AUDIO_TSTAMP_TYPE_LINK_ESTIMATED => "LINK_ESTIMATED",
        AUDIO_TSTAMP_TYPE_LINK_SYNCHRONIZED => "LINK_SYNCHRONIZED",
        _ => "UNKNOWN",
    }
}

/// Integrity-failure reasons of the haptic device lane (`first_integrity_failure_reason`).
pub const FAIL_ALSA_XRUN: &str = "alsa_xrun";
pub const FAIL_ALSA_SUSPENDED: &str = "alsa_suspended";
pub const FAIL_ALSA_DISCONNECTED: &str = "alsa_disconnected";
pub const FAIL_ALSA_EPOCH_RESET: &str = "alsa_epoch_reset";
pub const FAIL_ALSA_STATE_UNEXPECTED: &str = "alsa_state_unexpected";
pub const FAIL_ALSA_STATUS_INCONSISTENT: &str = "alsa_status_inconsistent";
pub const FAIL_ALSA_STATUS_ERROR: &str = "alsa_status_error";
pub const FAIL_ALSA_WRITE_ERROR: &str = "alsa_write_error";
/// A haptic submission that is not a playable PCM tick.
pub const FAIL_HAPTIC_PAYLOAD_INVALID: &str = "haptic_payload_invalid";

/// Aligned placement terminals, `explicit_discard` after submit (contract
/// §2-4; re-exported by `l2_output`). A later placement removed the tick's
/// first frame (`trim.start_removed = true`).
pub const DISCARD_OCCLUDED: &str = "occluded";
/// Case (iv): the whole tick lies before `appl_pos`.
pub const DISCARD_TOO_LATE: &str = "too_late";
/// Case (i): the candidate would make the plan exceed `Q` (unlike the FIFO
/// `output_queue_full`, which is before submit).
pub const DISCARD_PLACEMENT_QUEUE_FULL: &str = "placement_queue_full";
/// Every device-failure reason (their occurrence time is undeterminable).
pub const ALSA_DEVICE_FAILURE_REASONS: [&str; 8] = [
    FAIL_ALSA_XRUN,
    FAIL_ALSA_SUSPENDED,
    FAIL_ALSA_DISCONNECTED,
    FAIL_ALSA_EPOCH_RESET,
    FAIL_ALSA_STATE_UNEXPECTED,
    FAIL_ALSA_STATUS_INCONSISTENT,
    FAIL_ALSA_STATUS_ERROR,
    FAIL_ALSA_WRITE_ERROR,
];


/// `round_half_away(num / den)` for `den > 0`, exact in `i128`.
pub fn div_round_half_away(num: i128, den: i128) -> i128 {
    assert!(den > 0, "positive denominator");
    let q = num / den;
    let r = num % den;
    if 2 * r.abs() >= den {
        q + num.signum()
    } else {
        q
    }
}

/// `round_half_away(frames × 1e6 / rate_hz)` µs.
pub fn frames_to_us_round_half_away(frames: i64, rate_hz: u64) -> i64 {
    div_round_half_away(frames as i128 * 1_000_000, rate_hz as i128) as i64
}

/// Integer µs of a CLOCK_MONOTONIC nanosecond count, truncated exactly like
/// [`crate::now_us`] (`sec·1e6 + floor(nsec/1000)`).
pub fn ns_to_us_floor(ns: u64) -> u64 {
    ns / 1_000
}

/// The registered output time of frame `n_k` from one snapshot, `None` if it
/// would be negative (impossible on a sane clock; treated as inconsistent).
pub fn output_time_us(htstamp_us: u64, n_k: u64, hw_pos: i64) -> Option<u64> {
    let frames = n_k as i128 - hw_pos as i128;
    let offset = div_round_half_away(frames * 1_000_000, PCM_RATE_HZ as i128);
    u64::try_from(htstamp_us as i128 + offset).ok()
}

/// `snd_pcm_state_t`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PcmState {
    Open,
    Setup,
    Prepared,
    Running,
    Xrun,
    Draining,
    Paused,
    Suspended,
    Disconnected,
    Other(u32),
}

impl PcmState {
    pub fn from_raw(value: u32) -> Self {
        match value {
            0 => Self::Open,
            1 => Self::Setup,
            2 => Self::Prepared,
            3 => Self::Running,
            4 => Self::Xrun,
            5 => Self::Draining,
            6 => Self::Paused,
            7 => Self::Suspended,
            8 => Self::Disconnected,
            other => Self::Other(other),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "OPEN",
            Self::Setup => "SETUP",
            Self::Prepared => "PREPARED",
            Self::Running => "RUNNING",
            Self::Xrun => "XRUN",
            Self::Draining => "DRAINING",
            Self::Paused => "PAUSED",
            Self::Suspended => "SUSPENDED",
            Self::Disconnected => "DISCONNECTED",
            Self::Other(_) => "OTHER",
        }
    }
}

/// `snd_pcm_audio_tstamp_report_t`, unpacked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AudioTstampReport {
    pub valid: bool,
    pub actual_type: u32,
    pub accuracy_report: bool,
    /// Only meaningful when `accuracy_report`.
    pub accuracy_ns: u32,
}

/// One `snd_pcm_status` snapshot (every field from the SAME call). The
/// `*_ns` timestamps are integer nanoseconds of the clock the stream was
/// configured with (CLOCK_MONOTONIC, verified at open), except
/// `audio_htstamp_ns`, which is in the audio-timestamp domain (for LINK: the
/// HDA link wall clock since the stream started; NOT CLOCK_MONOTONIC).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PcmStatus {
    pub state: PcmState,
    pub htstamp_ns: u64,
    pub trigger_htstamp_ns: u64,
    pub audio_htstamp_ns: u64,
    pub driver_htstamp_ns: u64,
    pub delay: i64,
    pub avail: u64,
    pub avail_max: u64,
    pub audio_tstamp: AudioTstampReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PcmError {
    /// `-EAGAIN` (non-blocking, no space).
    WouldBlock,
    /// `-EPIPE`.
    Xrun,
    /// `-ESTRPIPE`.
    Suspended,
    /// `-ENODEV`.
    Disconnected,
    Other { errno: i32, call: &'static str },
}

impl std::fmt::Display for PcmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WouldBlock => write!(f, "EAGAIN"),
            Self::Xrun => write!(f, "EPIPE (xrun)"),
            Self::Suspended => write!(f, "ESTRPIPE (suspended)"),
            Self::Disconnected => write!(f, "ENODEV (disconnected)"),
            Self::Other { errno, call } => write!(f, "{call} failed: errno {errno}"),
        }
    }
}

impl PcmError {
    /// Map a negative alsa-lib return code.
    pub fn from_code(code: i64, call: &'static str) -> Self {
        match -code {
            c if c == libc::EAGAIN as i64 => Self::WouldBlock,
            c if c == libc::EPIPE as i64 => Self::Xrun,
            c if c == libc::ESTRPIPE as i64 => Self::Suspended,
            c if c == libc::ENODEV as i64 => Self::Disconnected,
            c => Self::Other { errno: c as i32, call },
        }
    }
}

/// The negotiated, verified device configuration (recorded in meta).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PcmParams {
    /// `"device"` (libasound) or `"simulated"` (never evidence).
    pub backend: &'static str,
    pub device: String,
    pub rate_hz: u64,
    pub channels: u64,
    pub format: &'static str,
    pub access: &'static str,
    pub period_frames: u64,
    pub buffer_frames: u64,
    /// Read back from the sw params after setting them.
    pub tstamp_mode: &'static str,
    pub tstamp_type: &'static str,
    pub audio_tstamp_type_requested: u32,
    /// The library actually loaded (device backend).
    pub library: Option<String>,
    /// Backend-specific installed settings (device: sw thresholds, batch
    /// flag, open mode, status struct size); `null` for the simulation.
    pub details: serde_json::Value,
}

/// A PCM playback stream. Every method takes the caller's clock read so the
/// simulated stream can run on a virtual clock; the real device ignores it
/// (the kernel stamps its own times).
pub trait HapticPcm {
    fn params(&self) -> &PcmParams;
    /// Write interleaved S16 frames (`pcm.len() == 2 × frames`); returns the
    /// number of frames written (non-blocking: possibly fewer).
    fn writei(&mut self, pcm: &[i16], now_us: u64) -> Result<u64, PcmError>;
    fn start(&mut self, now_us: u64) -> Result<(), PcmError>;
    fn status(&mut self, now_us: u64) -> Result<PcmStatus, PcmError>;
    /// Stop immediately (`snd_pcm_drop`).
    fn stop(&mut self) -> Result<(), PcmError>;
}

/// Placement parameters. Provisional SINK parameters, recorded in meta and
/// fixed in the freeze after the M2 device measurement (§3-3a); they are not
/// registered experimental parameters of any method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlacementConfig {
    pub period_frames: u64,
    pub buffer_frames: u64,
    /// `F`: with an empty write queue, one period of silence is written
    /// whenever the queued fill is below this.
    pub target_fill_frames: u64,
    /// Silence written before the stream starts (stream epoch frames 0..).
    pub prefill_frames: u64,
    /// `Q`: a tick that would make the write queue exceed this is discarded
    /// before submit (`output_queue_full`).
    pub queue_limit_frames: u64,
    /// `B` (sink mode A, experiment 3): `None` = off. Otherwise, before a
    /// playable tick is queued, the oldest WAITING ticks are dropped
    /// (`explicit_discard(latency_bound)` after submit) while the `Q` count
    /// plus the offered tick exceeds `B` (module doc "Latency bound").
    /// `REGULAR_TICK_MAX_FRAMES <= B <= queue_limit_frames`.
    pub latency_bound_frames: Option<u64>,
    /// Experiment-4 placement mode ([`PlacementMode`]); `None` = legacy.
    pub mode: PlacementMode,
}

/// Placement mode (experiment 4, contract `md/20261104_실험4_계약.md` §2;
/// design `md/20261104_실험4_설계초안.md` §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlacementMode {
    /// Legacy (experiments 2 and 3, L2 `event-pair-l2-v1.7`): the FIFO
    /// placement of the module doc, and sink mode A when
    /// `latency_bound_frames` is set. Rows and meta are exactly the former.
    None,
    /// Experiment-4 FIFO (L2 `event-pair-l2-v1.8` meta): the legacy FIFO
    /// engine path with `B` off (a bound is refused). No placement or trim
    /// row, no aligned counter, no new terminal.
    Fifo,
    /// Experiment-4 time-aligned placement R2 (module doc "Aligned
    /// placement"): `align_offset_us` = c (µs), `write_ahead_frames` = W.
    Aligned { align_offset_us: i64, write_ahead_frames: u64 },
}

impl PlacementMode {
    /// Meta `alsa.placement` (`None` for legacy, which has no such key).
    pub fn as_str(self) -> Option<&'static str> {
        match self {
            Self::None => None,
            Self::Fifo => Some("fifo"),
            Self::Aligned { .. } => Some("aligned"),
        }
    }

    /// `(c, W)` in aligned mode.
    pub fn aligned(self) -> Option<(i64, u64)> {
        match self {
            Self::Aligned { align_offset_us, write_ahead_frames } => Some((align_offset_us, write_ahead_frames)),
            _ => None,
        }
    }
}

impl PlacementConfig {
    pub fn validate(&self) -> Result<(), String> {
        let c = self;
        if c.period_frames == 0 || c.buffer_frames == 0 || c.target_fill_frames == 0 {
            return Err("ALSA period, buffer and target fill must be positive".into());
        }
        if c.buffer_frames < REGULAR_TICK_MAX_FRAMES + c.period_frames {
            return Err(format!(
                "ALSA buffer {} frames must hold a {REGULAR_TICK_MAX_FRAMES}-frame tick plus a period ({})",
                c.buffer_frames, c.period_frames
            ));
        }
        if c.target_fill_frames + c.period_frames > c.buffer_frames {
            return Err(format!(
                "ALSA target fill {} + period {} exceeds the buffer {}",
                c.target_fill_frames, c.period_frames, c.buffer_frames
            ));
        }
        if c.prefill_frames == 0 || c.prefill_frames > c.buffer_frames {
            return Err(format!("ALSA prefill {} must be in 1..=buffer {}", c.prefill_frames, c.buffer_frames));
        }
        if c.queue_limit_frames < REGULAR_TICK_MAX_FRAMES {
            return Err(format!(
                "ALSA queue limit {} frames cannot hold one {REGULAR_TICK_MAX_FRAMES}-frame tick",
                c.queue_limit_frames
            ));
        }
        if let Some(bound) = c.latency_bound_frames {
            if bound < REGULAR_TICK_MAX_FRAMES || bound > c.queue_limit_frames {
                return Err(format!(
                    "ALSA latency bound {bound} frames must be in {REGULAR_TICK_MAX_FRAMES}..=queue limit {}",
                    c.queue_limit_frames
                ));
            }
        }
        if c.mode != PlacementMode::None && c.latency_bound_frames.is_some() {
            return Err(format!(
                "the {} placement has no latency bound (experiment 4 has no sink mode A)",
                c.mode.as_str().unwrap_or("?")
            ));
        }
        if let Some((align_offset_us, write_ahead_frames)) = c.mode.aligned() {
            if align_offset_us < 0 {
                return Err(format!("aligned placement offset c = {align_offset_us} us must be >= 0"));
            }
            if write_ahead_frames == 0
                || write_ahead_frames.checked_add(c.period_frames).map_or(true, |end| end > c.buffer_frames)
            {
                return Err(format!(
                    "aligned write-ahead W = {write_ahead_frames} frames must be in 1..=buffer {} - period {}",
                    c.buffer_frames, c.period_frames
                ));
            }
        }
        Ok(())
    }

    /// The largest tick that can ever be written whole.
    pub fn max_tick_frames(&self) -> u64 {
        self.buffer_frames - self.period_frames
    }
}

/// The outcome of offering one haptic submission to the write queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// Queued: the lane writes its `submit` row.
    Queued,
    /// The write queue would exceed `Q`: `explicit_discard(output_queue_full)`
    /// BEFORE submit.
    QueueFull,
    /// Not a playable PCM tick: `submit` + `explicit_discard(payload_invalid)`
    /// after submit, and the integrity failure `haptic_payload_invalid`.
    PayloadInvalid,
}

/// What offering one haptic submission did ([`HapticPlacement::offer`]).
#[must_use = "every latency-bound drop needs its explicit_discard(latency_bound) row"]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Offered {
    /// Waiting ticks the latency bound dropped to make room, oldest first
    /// (always empty without a bound). Each was already submitted: the lane
    /// writes ONE `explicit_discard(latency_bound)` after submit for each,
    /// in this order, BEFORE it writes the row(s) of `admission`.
    pub latency_dropped: Vec<L2Submission>,
    /// The offered submission's own outcome (the `Q` rule after the drops).
    pub admission: Admission,
}

/// Latency-bound counters (present only with a bound; [`LaneStats`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LatencyBoundStats {
    /// Waiting ticks dropped for `B` (each: `explicit_discard(latency_bound)`).
    pub ticks_dropped: u64,
    /// Their 48 kHz frames (6 × 8 kHz samples each).
    pub frames_dropped: u64,
}

/// The case of one aligned placement (design §5-3 (i)–(iv)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlacementCase {
    /// (i) `n* >= planned_end`.
    Gap,
    /// (ii) `appl_pos <= n* < planned_end`.
    QueueOverlap,
    /// (iii) `n* < appl_pos < n* + L`.
    DeviceOverlap,
    /// (iv) `n* + L <= appl_pos`.
    TooLate,
}

impl PlacementCase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Gap => "gap",
            Self::QueueOverlap => "queue_overlap",
            Self::DeviceOverlap => "device_overlap",
            Self::TooLate => "too_late",
        }
    }
}

/// The `placement` row of one aligned placement attempt (contract §2-2),
/// computed BEFORE any cut or discard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementRecord {
    pub submission: L2Submission,
    /// `htstamp_us` of the last healthy snapshot.
    pub status_htstamp_us: u64,
    /// `hw_pos` of the last healthy snapshot.
    pub hw_pos: i64,
    /// Frames actually written at placement time (`w`).
    pub appl_pos: u64,
    pub planned_end_before: u64,
    pub n_star: i64,
    pub case: PlacementCase,
    pub floor_us: i64,
    pub head_cut_frames: u64,
    pub silence_inserted_frames: u64,
    pub planned_end_after: u64,
    /// `Some(DISCARD_PLACEMENT_QUEUE_FULL)` when case (i) exceeded `Q`.
    pub rejected: Option<&'static str>,
}

/// One `trim` row (contract §2-3): a later placement removed unwritten
/// frames of an earlier tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrimRecord {
    /// The trimmed tick.
    pub submission: L2Submission,
    /// The tick whose placement trimmed it.
    pub by: L2Submission,
    pub tail_cut_frames: u64,
    /// Its first frame was removed (none of it was written): it ends as
    /// `explicit_discard(occluded)`.
    pub start_removed: bool,
}

/// The offered tick's own outcome in aligned mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlignedAdmission {
    /// Placed (case gap, queue_overlap or device_overlap): its terminal is a
    /// later `output` (or `occluded` / `device_failure`).
    Placed,
    /// Case (iv): `explicit_discard(too_late)` after submit.
    TooLate,
    /// Case (i) over `Q`: `explicit_discard(placement_queue_full)` after submit.
    PlacementQueueFull,
    /// Not a playable PCM tick: `explicit_discard(payload_invalid)` after
    /// submit and the integrity failure `haptic_payload_invalid` (no
    /// placement row).
    PayloadInvalid,
}

/// What one aligned placement did ([`HapticPlacement::place`]). The lane
/// writes, in this order: the submit row, the placement row, every trim row
/// (plan order), one `explicit_discard(occluded)` per `occluded` (plan
/// order), then the offered tick's own discard if `admission` is one.
#[must_use = "every trim and occlusion needs its row"]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlignedOffered {
    /// `None` only for `PayloadInvalid`.
    pub placement: Option<PlacementRecord>,
    pub trims: Vec<TrimRecord>,
    /// The trimmed ticks whose start was removed, in plan order (each one is
    /// a `start_removed` trim).
    pub occluded: Vec<L2Submission>,
    pub admission: AlignedAdmission,
}

/// Aligned-placement counters (contract §2-5; present only in aligned mode).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AlignedStats {
    /// Ticks whose start a later placement removed (`occluded`).
    pub ticks_occluded: u64,
    /// Case (iv) ticks (`too_late`).
    pub ticks_too_late: u64,
    /// Case (iii) placements (head cut > 0).
    pub ticks_head_cut: u64,
    /// Case (i) rejections (`placement_queue_full`).
    pub ticks_placement_queue_full: u64,
    /// Σ `trim.tail_cut_frames` (occluded ticks included).
    pub frames_tail_cut: u64,
    /// Σ `placement.head_cut_frames`.
    pub frames_head_cut: u64,
    /// Σ `placement.silence_inserted_frames`.
    pub frames_silence_inserted: u64,
    /// The largest planned (waiting) silence after any placement (<= Q).
    pub max_waiting_silence_frames: u64,
}

/// A device failure (registration §6-1, Codex 109 P1). No occurrence time
/// is claimed until the meaning of the kernel's timestamps is verified on
/// the device: the ledger records it as an integrity failure whose time is
/// undeterminable, and keeps the observation interval, the detection time
/// and the raw kernel trigger apart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceFailure {
    pub reason: &'static str,
    /// Start of the observation interval: the clock read immediately BEFORE
    /// the last status call that showed a healthy stream (at the latest the
    /// epoch's first snapshot). The kernel took that snapshot after this
    /// read and the failure came after the snapshot, so it came after this.
    pub last_good_status_t0_us: u64,
    /// End of the observation interval: when the lane detected the failure.
    pub detected_us: u64,
    /// The trigger timestamp the kernel reported with the failed state, if
    /// it moved (µs, CLOCK_MONOTONIC). Its relation to the actual underrun
    /// or state change is NOT verified: not an occurrence time.
    pub kernel_trigger_us: Option<u64>,
    /// The same, raw (diagnostics file only).
    pub kernel_trigger_ns: Option<u64>,
    /// Reason text with integer µs CLOCK_MONOTONIC values or frame counts
    /// only (it goes into the L2 JSONL, registration 11-11).
    pub detail: String,
    /// Raw nanosecond values behind `detail` (diagnostics file only).
    pub raw_ns: Vec<(&'static str, u64)>,
}

impl DeviceFailure {
    /// Keep raw nanosecond values for the diagnostics file only.
    pub fn with_raw_ns(mut self, values: &[(&'static str, u64)]) -> Self {
        self.raw_ns.extend_from_slice(values);
        self
    }

    /// The ledger form (sentinel `lanes.haptic.device_failure`): integer µs
    /// CLOCK_MONOTONIC only.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "reason": self.reason,
            "occurrence_time_determinable": false,
            "observation_interval_us": [self.last_good_status_t0_us, self.detected_us],
            "last_good_status_t0_us": self.last_good_status_t0_us,
            "detected_us": self.detected_us,
            "kernel_trigger_us": self.kernel_trigger_us,
            "kernel_trigger_semantics": "trigger timestamp the kernel reported with the failed state (unverified; not the underrun instant); not an occurrence time",
            "detail": self.detail,
        })
    }
}

/// The raw values of one status snapshot. Kept ONLY for the probe's
/// diagnostics file (registration §11-11): nanoseconds, and an
/// audio-timestamp value that is not CLOCK_MONOTONIC, never enter the L2
/// JSONL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawSnapshot {
    pub htstamp_ns: u64,
    pub trigger_htstamp_ns: u64,
    pub driver_htstamp_ns: u64,
    /// In the audio-timestamp domain of `audio_tstamp.actual_type`.
    pub audio_htstamp_ns: u64,
    pub audio_tstamp: AudioTstampReport,
}

impl RawSnapshot {
    pub fn of(s: &PcmStatus) -> Self {
        Self {
            htstamp_ns: s.htstamp_ns,
            trigger_htstamp_ns: s.trigger_htstamp_ns,
            driver_htstamp_ns: s.driver_htstamp_ns,
            audio_htstamp_ns: s.audio_htstamp_ns,
            audio_tstamp: s.audio_tstamp,
        }
    }

    /// The diagnostics-file form: every value with its unit, clock or
    /// domain (the audio timestamp's per its ACTUAL type) and origin.
    pub fn to_diag_json(&self) -> serde_json::Value {
        let report = &self.audio_tstamp;
        serde_json::json!({
            "htstamp_ns": self.htstamp_ns,
            "trigger_htstamp_ns": self.trigger_htstamp_ns,
            "driver_htstamp_ns": self.driver_htstamp_ns,
            "audio_htstamp_ns": self.audio_htstamp_ns,
            "audio_htstamp_domain": audio_tstamp_domain(report.actual_type),
            "audio_tstamp_type_actual": audio_tstamp_type_name(report.actual_type),
            "audio_tstamp_report_valid": report.valid,
            "audio_tstamp_accuracy_report": report.accuracy_report,
            "audio_tstamp_accuracy_ns": report.accuracy_report.then_some(report.accuracy_ns),
        })
    }
}

/// What `snd_pcm_status_get_audio_htstamp` measures for an ACTUAL type
/// (source: the enum comments of the local kernel UAPI
/// `/usr/include/sound/asound.h`, `SNDRV_PCM_AUDIO_TSTAMP_TYPE_*`). None of
/// these is CLOCK_MONOTONIC.
pub fn audio_tstamp_domain(actual_type: u32) -> &'static str {
    match actual_type {
        AUDIO_TSTAMP_TYPE_COMPAT => {
            "COMPAT: backwards-compatibility type, maps to wallclock/link time for HDAudio playback and DEFAULT/DMA time for everything else; not CLOCK_MONOTONIC"
        }
        AUDIO_TSTAMP_TYPE_DEFAULT => "DEFAULT: DMA time, reported as per hw_ptr; not CLOCK_MONOTONIC",
        AUDIO_TSTAMP_TYPE_LINK => {
            "LINK: link time reported by sample or wallclock counter, reset on startup; not CLOCK_MONOTONIC"
        }
        AUDIO_TSTAMP_TYPE_LINK_ABSOLUTE => {
            "LINK_ABSOLUTE: link time reported by sample or wallclock counter, not reset on startup; not CLOCK_MONOTONIC"
        }
        AUDIO_TSTAMP_TYPE_LINK_ESTIMATED => "LINK_ESTIMATED: link time estimated indirectly; not CLOCK_MONOTONIC",
        AUDIO_TSTAMP_TYPE_LINK_SYNCHRONIZED => {
            "LINK_SYNCHRONIZED: link time synchronized with system time (still the audio-timestamp domain, not the status htstamp)"
        }
        _ => "UNKNOWN audio timestamp type: domain not defined by the UAPI",
    }
}

/// Evidence recorded in the `output_detail` of one haptic `output` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlsaOutputDetail {
    pub backend: &'static str,
    pub n_k: u64,
    pub frames: u64,
    pub samples_8k: u64,
    /// Clock read after the `writei` that wrote the tick's first frame.
    pub t_write_us: u64,
    /// Clock read after the `writei` that wrote its last frame.
    pub t_write_done_us: u64,
    /// Clock read immediately before the status call (its end is the row's
    /// `t_callback_us`).
    pub status_t0_us: u64,
    pub htstamp_us: u64,
    pub trigger_htstamp_us: u64,
    pub driver_htstamp_us: u64,
    pub appl_pos: u64,
    pub delay_frames: i64,
    pub avail_frames: u64,
    pub hw_pos: i64,
    pub queued_frames: u64,
    pub hw_ptr_pos: u64,
    pub driver_delay_frames: i64,
    pub n_k_minus_hw_pos: i64,
    pub offset_us: i64,
    pub state: &'static str,
    pub audio_tstamp_type_requested: u32,
    pub audio_tstamp: AudioTstampReport,
    /// Raw snapshot values: diagnostics file only, never the ledger.
    pub raw: RawSnapshot,
    /// Aligned mode only (`Some`; the key is absent otherwise): frames cut
    /// from the tick's head at placement (case device_overlap), 0 for every
    /// other tick.
    pub head_cut_frames: Option<u64>,
}

impl AlsaOutputDetail {
    pub fn to_json(&self) -> serde_json::Value {
        let mut json = self.legacy_json();
        if let Some(head_cut_frames) = self.head_cut_frames {
            let map = json.as_object_mut().expect("output detail is an object");
            map.insert("head_cut_frames".into(), head_cut_frames.into());
        }
        json
    }

    fn legacy_json(&self) -> serde_json::Value {
        let report = &self.audio_tstamp;
        serde_json::json!({
            "device": "alsa",
            "modality": "haptic",
            "backend": self.backend,
            "n_k": self.n_k,
            "frames": self.frames,
            "samples_8k": self.samples_8k,
            "t_write_us": self.t_write_us,
            "t_write_done_us": self.t_write_done_us,
            "status_t0_us": self.status_t0_us,
            "htstamp_us": self.htstamp_us,
            "trigger_htstamp_us": self.trigger_htstamp_us,
            "driver_htstamp_us": self.driver_htstamp_us,
            "appl_pos": self.appl_pos,
            "delay_frames": self.delay_frames,
            "avail_frames": self.avail_frames,
            "hw_pos": self.hw_pos,
            "queued_frames": self.queued_frames,
            "hw_ptr_pos": self.hw_ptr_pos,
            "driver_delay_frames": self.driver_delay_frames,
            "n_k_minus_hw_pos": self.n_k_minus_hw_pos,
            "offset_us": self.offset_us,
            "state": self.state,
            "audio_tstamp_type_requested": audio_tstamp_type_name(self.audio_tstamp_type_requested),
            "audio_tstamp_type_actual": audio_tstamp_type_name(report.actual_type),
            "audio_tstamp_report_valid": report.valid,
            "audio_tstamp_accuracy_report": report.accuracy_report,
        })
    }

    /// The `output_detail` schema of the haptic ALSA sink (meta
    /// `sink_contracts.haptic.output_detail`).
    pub fn schema() -> serde_json::Value {
        serde_json::json!({
            "device": "the string \"alsa\"",
            "modality": "the string \"haptic\"",
            "backend": "device (libasound) | simulated (tests, never evidence)",
            "n_k": "48 kHz frame index of the tick's first frame in this stream epoch (prefill included)",
            "frames": "48 kHz frames of the tick = 6 x samples_8k (528 for 88, 534 for 89)",
            "samples_8k": "8 kHz mono samples in the released payload",
            "t_write_us": "clock read after the writei that wrote frame n_k",
            "t_write_done_us": "clock read after the writei that wrote the tick's last frame",
            "status_t0_us": "clock read immediately before the status call; t_callback_us is the read immediately after it",
            "htstamp_us": "snd_pcm_status_get_htstamp of the snapshot, CLOCK_MONOTONIC (verified tstamp_type), truncated: sec*1e6 + floor(nsec/1000)",
            "trigger_htstamp_us": "snd_pcm_status_get_trigger_htstamp, CLOCK_MONOTONIC, truncated (the stream start of this epoch while running)",
            "driver_htstamp_us": "snd_pcm_status_get_driver_htstamp, CLOCK_MONOTONIC, truncated (taken after the reference timestamp)",
            "appl_pos": "frames written by this lane at the status call (the only writer, so the kernel appl_ptr)",
            "delay_frames": "snd_pcm_status_get_delay of the snapshot",
            "avail_frames": "snd_pcm_status_get_avail of the snapshot",
            "hw_pos": "appl_pos - delay_frames (the registered output position)",
            "queued_frames": "buffer_frames - avail_frames",
            "hw_ptr_pos": "appl_pos - queued_frames (the DMA pointer; must never decrease within the epoch)",
            "driver_delay_frames": "delay_frames - queued_frames (driver/codec delay folded into delay; recorded, not assumed 0)",
            "n_k_minus_hw_pos": "n_k - hw_pos (negative: an output row needs hw_pos > n_k)",
            "offset_us": "round_half_away(n_k_minus_hw_pos * 1e6 / 48000); t_output_us = htstamp_us + offset_us",
            "state": "snd_pcm_state of the snapshot (RUNNING)",
            "audio_tstamp_type_requested": "audio timestamp type requested before every status call (LINK)",
            "audio_tstamp_type_actual": "snd_pcm_status_get_audio_htstamp_report actual_type",
            "audio_tstamp_report_valid": "report valid bit",
            "audio_tstamp_accuracy_report": "report accuracy_report bit (the accuracy value and the audio timestamp itself are in the probe-only diagnostics file, registration 11-11)",
        })
    }
}

/// One observed output of a written tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placed {
    pub submission: L2Submission,
    pub t_output_us: u64,
    /// The clock read right after the status call that showed `hw_pos > n_k`.
    pub t_callback_us: u64,
    pub detail: AlsaOutputDetail,
}

/// Lane counters (reported in the sentinel under `lanes.haptic`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LaneStats {
    pub prefill_frames: u64,
    pub frames_written: u64,
    pub tick_frames_written: u64,
    pub silence_frames_written: u64,
    pub ticks_admitted: u64,
    pub ticks_queue_full: u64,
    pub ticks_payload_invalid: u64,
    pub ticks_written: u64,
    pub ticks_output: u64,
    pub status_calls: u64,
    /// Gap between consecutive status calls (start to start).
    pub status_gap_max_us: u64,
    /// Gap histogram, upper bounds [`STATUS_GAP_BUCKETS_US`] then overflow.
    pub status_gap_hist: [u64; 7],
    pub writei_calls: u64,
    pub writei_short: u64,
    pub writei_wouldblock: u64,
    pub writei_max_us: u64,
    pub queued_max_frames: u64,
    pub queued_min_frames: Option<u64>,
    /// Max of every frame not yet written to the device (waiting queue +
    /// remainder of the tick being written) right after an admission.
    pub max_unwritten_tick_frames: u64,
    /// `n_k + frames` of the latest tick that started being written (one
    /// past its last frame; also while it is only partly written).
    pub last_tick_end_frame: Option<u64>,
    /// `hw_pos` of the last healthy snapshot.
    pub last_hw_pos: Option<i64>,
    /// `Some` iff a latency bound `B` is configured; its keys are emitted
    /// only then, so without `B` the counters are exactly the former ones.
    pub latency_bound: Option<LatencyBoundStats>,
    /// `Some` iff the placement is aligned; its keys are emitted only then.
    pub aligned: Option<AlignedStats>,
}

/// Upper bounds (exclusive, µs) of the status-gap histogram buckets.
pub const STATUS_GAP_BUCKETS_US: [u64; 6] = [250, 500, 1_000, 2_000, 4_000, 8_000];

impl LaneStats {
    fn note_gap(&mut self, gap_us: u64) {
        self.status_gap_max_us = self.status_gap_max_us.max(gap_us);
        let bucket = STATUS_GAP_BUCKETS_US.iter().position(|&b| gap_us < b).unwrap_or(6);
        self.status_gap_hist[bucket] += 1;
    }

    pub fn to_json(&self) -> serde_json::Value {
        let mut json = serde_json::json!({
            "prefill_frames": self.prefill_frames,
            "frames_written": self.frames_written,
            "tick_frames_written": self.tick_frames_written,
            "silence_frames_written": self.silence_frames_written,
            "ticks_admitted": self.ticks_admitted,
            "ticks_queue_full": self.ticks_queue_full,
            "ticks_payload_invalid": self.ticks_payload_invalid,
            "ticks_written": self.ticks_written,
            "ticks_output": self.ticks_output,
            "status_calls": self.status_calls,
            "status_gap_max_us": self.status_gap_max_us,
            "status_gap_hist_upper_us": STATUS_GAP_BUCKETS_US,
            "status_gap_hist": self.status_gap_hist,
            "writei_calls": self.writei_calls,
            "writei_short": self.writei_short,
            "writei_wouldblock": self.writei_wouldblock,
            "writei_max_us": self.writei_max_us,
            "queued_max_frames": self.queued_max_frames,
            "queued_min_frames": self.queued_min_frames,
            "max_unwritten_tick_frames": self.max_unwritten_tick_frames,
            "last_tick_end_frame": self.last_tick_end_frame,
            "last_hw_pos": self.last_hw_pos,
        });
        if let Some(bound) = &self.latency_bound {
            let map = json.as_object_mut().expect("lane stats are an object");
            map.insert("ticks_latency_bound".into(), bound.ticks_dropped.into());
            map.insert("latency_bound_frames_dropped".into(), bound.frames_dropped.into());
        }
        if let Some(a) = &self.aligned {
            let map = json.as_object_mut().expect("lane stats are an object");
            map.insert("ticks_occluded".into(), a.ticks_occluded.into());
            map.insert("ticks_too_late".into(), a.ticks_too_late.into());
            map.insert("ticks_head_cut".into(), a.ticks_head_cut.into());
            map.insert("ticks_placement_queue_full".into(), a.ticks_placement_queue_full.into());
            map.insert("frames_tail_cut".into(), a.frames_tail_cut.into());
            map.insert("frames_head_cut".into(), a.frames_head_cut.into());
            map.insert("frames_silence_inserted".into(), a.frames_silence_inserted.into());
            map.insert("max_waiting_silence_frames".into(), a.max_waiting_silence_frames.into());
        }
        json
    }
}

struct PendingTick {
    submission: L2Submission,
    pcm: Vec<i16>,
    frames: u64,
    samples_8k: u64,
}

struct WrittenTick {
    submission: L2Submission,
    n_k: u64,
    frames: u64,
    samples_8k: u64,
    t_write_us: u64,
    t_write_done_us: u64,
}

enum ChunkKind {
    Tick { submission: L2Submission, n_k: u64, samples_8k: u64, t_write_us: Option<u64> },
    Silence,
}

/// The chunk being written (a short `writei` leaves the rest here).
struct Current {
    kind: ChunkKind,
    /// Interleaved stereo; empty for silence (zeros are generated).
    pcm: Vec<i16>,
    total_frames: u64,
    offset_frames: u64,
}

/// One segment of the aligned plan, a frame-indexed queue covering
/// `[w, w + plan_frames)` (`planned_end = w + plan_frames >= appl_pos = w`
/// by construction).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Segment {
    /// A run of planned silence (case gap).
    Silence { frames: u64 },
    /// `frames` frames of tick `id` from frame `offset` of its full PCM
    /// (`id` = placement index; the tick carries its seq).
    Tick { id: u64, offset: u64, frames: u64 },
}

impl Segment {
    fn frames(&self) -> u64 {
        match self {
            Self::Silence { frames } | Self::Tick { frames, .. } => *frames,
        }
    }
}

/// A placed tick of the aligned plan, from its placement until its output
/// row (or occlusion, or abandonment after a device failure).
struct AlignedTick {
    submission: L2Submission,
    /// The full tick (L frames, interleaved stereo); a head cut skips the
    /// first `head_cut` frames of it.
    pcm: Vec<i16>,
    samples_8k: u64,
    head_cut: u64,
    /// Frame of its first remaining frame (the recorded `n_k`).
    n_k: u64,
    /// Recorded frames: L − head cut − every tail cut so far.
    frames: u64,
    /// Frames of it written to the device.
    written: u64,
    /// Clock read after the writei that wrote frame `n_k`.
    t_write_us: Option<u64>,
    /// Clock read after the latest writei that wrote a frame of it.
    t_write_last_us: Option<u64>,
}

/// The aligned placement's state (module doc "Aligned placement").
struct AlignedState {
    align_offset_us: i64,
    write_ahead_frames: u64,
    plan: VecDeque<Segment>,
    plan_frames: u64,
    /// Planned (waiting) silence frames in `plan`.
    plan_silence_frames: u64,
    /// Every placed tick not yet output, occluded or abandoned, by id
    /// (= placement order = plan order).
    ticks: BTreeMap<u64, AlignedTick>,
    next_id: u64,
    /// Ticks whose first remaining frame was written, in frame order; only
    /// the back can still have planned frames.
    in_flight: VecDeque<u64>,
}

impl AlignedState {
    fn new(align_offset_us: i64, write_ahead_frames: u64) -> Self {
        Self {
            align_offset_us,
            write_ahead_frames,
            plan: VecDeque::new(),
            plan_frames: 0,
            plan_silence_frames: 0,
            ticks: BTreeMap::new(),
            next_id: 0,
            in_flight: VecDeque::new(),
        }
    }

    /// Tick frames of the plan (the aligned counterpart of the `Q` count).
    fn plan_tick_frames(&self) -> u64 {
        self.plan_frames - self.plan_silence_frames
    }
}

/// What the stream epoch looked like right after `start`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochStart {
    pub t_start_us: u64,
    /// Raw (diagnostics); the ledger records it truncated to µs.
    pub trigger_htstamp_ns: u64,
    /// Clock read immediately before the first status call.
    pub first_status_t0_us: u64,
    pub first_status: PcmStatus,
}

/// The placement engine of one stream epoch. Clock-agnostic: every time is
/// passed in, so it is deterministic under a virtual clock.
pub struct HapticPlacement {
    cfg: PlacementConfig,
    backend: &'static str,
    audio_tstamp_type_requested: u32,
    /// Frames written in this epoch (prefill included) = `appl_pos`.
    w: u64,
    pending: VecDeque<PendingTick>,
    pending_frames: u64,
    current: Option<Current>,
    in_flight: VecDeque<WrittenTick>,
    /// `(n_k, n_k + frames)` of the latest tick that started being written
    /// (its FULL extent, also while only partly written; `None` before any).
    tick_extent: Option<(u64, u64)>,
    /// `hw_pos` of the last healthy snapshot.
    last_hw_pos: Option<i64>,
    epoch: Option<EpochStart>,
    /// Clock read just before the last healthy status call.
    last_good_status_t0_us: u64,
    last_hw_ptr_pos: u64,
    last_htstamp_ns: u64,
    last_status_t0_us: Option<u64>,
    stats: LaneStats,
    /// `Some` iff the placement is aligned (the legacy/FIFO state above is
    /// then unused: `pending`, `current` and `in_flight` stay empty).
    aligned: Option<AlignedState>,
}

impl HapticPlacement {
    pub fn new(cfg: PlacementConfig, params: &PcmParams) -> Result<Self, String> {
        cfg.validate()?;
        if params.period_frames != cfg.period_frames || params.buffer_frames != cfg.buffer_frames {
            return Err(format!(
                "device period/buffer {}/{} differ from the requested {}/{}",
                params.period_frames, params.buffer_frames, cfg.period_frames, cfg.buffer_frames
            ));
        }
        Ok(Self {
            cfg,
            backend: params.backend,
            audio_tstamp_type_requested: params.audio_tstamp_type_requested,
            w: 0,
            pending: VecDeque::new(),
            pending_frames: 0,
            current: None,
            in_flight: VecDeque::new(),
            tick_extent: None,
            last_hw_pos: None,
            epoch: None,
            last_good_status_t0_us: 0,
            last_hw_ptr_pos: 0,
            last_htstamp_ns: 0,
            last_status_t0_us: None,
            stats: LaneStats {
                latency_bound: cfg.latency_bound_frames.map(|_| LatencyBoundStats::default()),
                aligned: cfg.mode.aligned().map(|_| AlignedStats::default()),
                ..LaneStats::default()
            },
            aligned: cfg.mode.aligned().map(|(c, w)| AlignedState::new(c, w)),
        })
    }

    pub fn config(&self) -> &PlacementConfig {
        &self.cfg
    }

    pub fn stats(&self) -> &LaneStats {
        &self.stats
    }

    pub fn epoch(&self) -> Option<&EpochStart> {
        self.epoch.as_ref()
    }

    /// Frames written so far (`appl_pos`).
    pub fn written_frames(&self) -> u64 {
        self.w
    }

    /// Frames waiting in the write queue (not yet written). Aligned: the
    /// tick frames of the plan (silence excluded).
    pub fn pending_frames(&self) -> u64 {
        if let Some(st) = &self.aligned {
            return st.plan_tick_frames();
        }
        self.pending_frames
    }

    /// Aligned only: `planned_end` (`w` + every planned frame, `>= w`).
    pub fn planned_end(&self) -> Option<u64> {
        self.aligned.as_ref().map(|st| self.w + st.plan_frames)
    }

    /// Aligned only: the planned (waiting) silence frames.
    pub fn waiting_silence_frames(&self) -> Option<u64> {
        self.aligned.as_ref().map(|st| st.plan_silence_frames)
    }

    /// Aligned only: the write-ahead fill estimate `w − hw_pos` of the last
    /// healthy snapshot (= its `delay` + frames written since it).
    pub fn fill_estimate(&self) -> Option<i64> {
        self.aligned.as_ref()?;
        self.last_hw_pos.map(|hw_pos| self.w as i64 - hw_pos)
    }

    /// Nothing queued, nothing partially written, no onset awaiting its
    /// output, AND the last written tick fully played: `hw_pos >= n_k +
    /// frames`, i.e. its LAST frame passed (registration 3-3a "종료"; the
    /// onset output alone does not mean the tick was played, Codex 109 P1).
    /// Trailing silence is never waited for. Aligned: no placed tick left
    /// (planned, being written or awaiting its output) and the tail played.
    pub fn idle(&self) -> bool {
        if let Some(st) = &self.aligned {
            return st.ticks.is_empty() && self.tick_tail_played();
        }
        self.pending.is_empty()
            && self.in_flight.is_empty()
            && !matches!(self.current, Some(Current { kind: ChunkKind::Tick { .. }, .. }))
            && self.tick_tail_played()
    }

    /// Whether the last frame of the latest tick that started being written
    /// has passed `hw_pos` in the last healthy snapshot (true before any).
    pub fn tick_tail_played(&self) -> bool {
        match self.tick_extent {
            None => true,
            Some((_, end)) => self.last_hw_pos.is_some_and(|hw_pos| hw_pos >= end as i64),
        }
    }

    /// Frames of the latest tick that started being written — its full
    /// extent, written or not — that are not yet past `hw_pos` in the last
    /// healthy snapshot (0 when played). An upper bound for a partly written
    /// tick and for anything that played after that snapshot.
    pub fn unplayed_tick_frames(&self) -> u64 {
        let Some((n_k, end)) = self.tick_extent else { return 0 };
        match self.last_hw_pos {
            Some(hw_pos) if hw_pos >= end as i64 => 0,
            Some(hw_pos) => end - (hw_pos.max(n_k as i64) as u64),
            None => end - n_k,
        }
    }

    /// Every tick frame not yet written to the device: the waiting queue
    /// plus the unwritten remainder of the tick being written (the `Q`
    /// count, registration 3-3a "상한"). Aligned: the tick frames of the plan.
    pub fn unwritten_tick_frames(&self) -> u64 {
        if let Some(st) = &self.aligned {
            return st.plan_tick_frames();
        }
        let current = match &self.current {
            Some(c @ Current { kind: ChunkKind::Tick { .. }, .. }) => c.total_frames - c.offset_frames,
            _ => 0,
        };
        self.pending_frames + current
    }

    /// Open the stream epoch: write the prefill silence (before start).
    pub fn prefill(&mut self, pcm: &mut dyn HapticPcm, clock: &mut dyn FnMut() -> u64) -> Result<(), PcmError> {
        assert_eq!(self.w, 0, "prefill opens the epoch");
        let zeros = vec![0i16; self.cfg.prefill_frames as usize * PCM_CHANNELS];
        let mut written = 0u64;
        while written < self.cfg.prefill_frames {
            let from = written as usize * PCM_CHANNELS;
            let n = pcm.writei(&zeros[from..], clock())?;
            if n == 0 {
                return Err(PcmError::Other { errno: libc::EIO, call: "prefill writei" });
            }
            written += n;
        }
        self.w = written;
        self.stats.prefill_frames = written;
        self.stats.frames_written = written;
        self.stats.silence_frames_written = written;
        Ok(())
    }

    /// Record the epoch right after `start`: the first snapshot's trigger
    /// timestamp is the epoch's identity from now on. The first snapshot is
    /// also the ABI witness of the audio-timestamp bitfields: a non-COMPAT
    /// request (LINK) that reached the driver makes the report `valid`, and
    /// an `actual_type` outside the known range means the report was
    /// decoded with a wrong layout — both refuse the run.
    pub fn begin_epoch(&mut self, t_start_us: u64, first_status: PcmStatus, t0: u64) -> Result<(), String> {
        if first_status.state != PcmState::Running {
            return Err(format!("stream not RUNNING after start: {}", first_status.state.as_str()));
        }
        let report = first_status.audio_tstamp;
        if !report.valid || report.actual_type > AUDIO_TSTAMP_TYPE_LINK_SYNCHRONIZED {
            return Err(format!(
                "audio timestamp report not usable (valid {}, actual_type {}): the {} request did not reach the driver as decoded",
                report.valid,
                report.actual_type,
                audio_tstamp_type_name(self.audio_tstamp_type_requested)
            ));
        }
        self.epoch = Some(EpochStart {
            t_start_us,
            trigger_htstamp_ns: first_status.trigger_htstamp_ns,
            first_status_t0_us: t0,
            first_status,
        });
        self.last_good_status_t0_us = t0;
        self.last_htstamp_ns = first_status.htstamp_ns;
        self.last_hw_pos = Some(self.w as i64 - first_status.delay);
        self.stats.last_hw_pos = self.last_hw_pos;
        Ok(())
    }

    /// Offer one released haptic submission (its payload: 8 kHz S16_LE
    /// mono): payload validation first (an unplayable payload drops
    /// nothing), then — only with a latency bound `B` — the oldest WAITING
    /// ticks are dropped while `unwritten_tick_frames() + frames > B` (never
    /// the tick being written, never the offered tick; by queue position
    /// only, blind to identity), then the unchanged `Q` rule decides the
    /// offered tick. Without `B` this is exactly the former admission.
    pub fn offer(&mut self, submission: L2Submission, payload: Option<&[u8]>) -> Offered {
        assert!(self.aligned.is_none(), "the aligned placement offers through place()");
        let mut latency_dropped = Vec::new();
        let admission = self.admit_after_bound(submission, payload, &mut latency_dropped);
        Offered { latency_dropped, admission }
    }

    /// [`Self::offer`] reduced to the offered submission's own outcome, for
    /// tests of a placement without a latency bound. Panics if the bound
    /// dropped a waiting tick (its terminal would be lost): use `offer`.
    #[cfg(test)]
    pub fn admit(&mut self, submission: L2Submission, payload: Option<&[u8]>) -> Admission {
        let offered = self.offer(submission, payload);
        assert!(offered.latency_dropped.is_empty(), "admit() cannot report latency-bound drops: use offer()");
        offered.admission
    }

    fn admit_after_bound(
        &mut self,
        submission: L2Submission,
        payload: Option<&[u8]>,
        latency_dropped: &mut Vec<L2Submission>,
    ) -> Admission {
        let Some(payload) = payload else {
            self.stats.ticks_payload_invalid += 1;
            return Admission::PayloadInvalid;
        };
        let samples = payload.len() / 2;
        if payload.is_empty() || payload.len() % 2 != 0 || samples > MAX_TICK_SAMPLES {
            self.stats.ticks_payload_invalid += 1;
            return Admission::PayloadInvalid;
        }
        let frames = samples as u64 * UPSAMPLE_FACTOR;
        if frames > self.cfg.max_tick_frames() {
            self.stats.ticks_payload_invalid += 1;
            return Admission::PayloadInvalid;
        }
        if let Some(bound) = self.cfg.latency_bound_frames {
            // Sink mode A: drop the oldest waiting tick (front of the write
            // queue) until the offered tick fits under B or nothing waits.
            // `current` (n_k fixed) and `in_flight` (written) are untouched.
            while self.unwritten_tick_frames() + frames > bound {
                let Some(oldest) = self.pending.pop_front() else { break };
                self.pending_frames -= oldest.frames;
                let counters = self.stats.latency_bound.get_or_insert_with(LatencyBoundStats::default);
                counters.ticks_dropped += 1;
                counters.frames_dropped += oldest.frames;
                latency_dropped.push(oldest.submission);
            }
        }
        if self.unwritten_tick_frames() + frames > self.cfg.queue_limit_frames {
            self.stats.ticks_queue_full += 1;
            return Admission::QueueFull;
        }
        let mut pcm = Vec::with_capacity(frames as usize * PCM_CHANNELS);
        for bytes in payload.chunks_exact(2) {
            let sample = i16::from_le_bytes([bytes[0], bytes[1]]);
            for _ in 0..UPSAMPLE_FACTOR {
                pcm.push(sample);
                pcm.push(sample);
            }
        }
        self.pending.push_back(PendingTick { submission, pcm, frames, samples_8k: samples as u64 });
        self.pending_frames += frames;
        self.stats.ticks_admitted += 1;
        self.stats.max_unwritten_tick_frames =
            self.stats.max_unwritten_tick_frames.max(self.unwritten_tick_frames());
        Admission::Queued
    }

    /// Aligned placement of one released haptic submission (module doc
    /// "Aligned placement"; design §5-3). Payload validation first (the same
    /// three checks as [`Self::offer`]; an unplayable payload makes no
    /// placement row). Then, from the LAST HEALTHY snapshot (`hw_pos`,
    /// `htstamp_us`) and `appl_pos = w` (frames actually written so far, no
    /// fresh status call), `n*` and `floor_us` are computed for EVERY
    /// attempt before any cut or discard, the case is classified, its result
    /// is precomputed, and the plan is mutated only if it is not rejected.
    pub fn place(&mut self, submission: L2Submission, payload: Option<&[u8]>) -> AlignedOffered {
        let Some((align_offset_us, _)) = self.cfg.mode.aligned() else {
            panic!("place() is the aligned placement; the FIFO placement offers through offer()");
        };
        let invalid = AlignedOffered {
            placement: None,
            trims: Vec::new(),
            occluded: Vec::new(),
            admission: AlignedAdmission::PayloadInvalid,
        };
        let Some(payload) = payload else {
            self.stats.ticks_payload_invalid += 1;
            return invalid;
        };
        let samples = payload.len() / 2;
        if payload.is_empty() || payload.len() % 2 != 0 || samples > MAX_TICK_SAMPLES {
            self.stats.ticks_payload_invalid += 1;
            return invalid;
        }
        let frames = samples as u64 * UPSAMPLE_FACTOR;
        if frames > self.cfg.max_tick_frames() {
            self.stats.ticks_payload_invalid += 1;
            return invalid;
        }
        // The lane offers only after its epoch began (`begin_epoch` seeds the
        // healthy snapshot): anything else is a programming error.
        assert!(self.epoch.is_some(), "an aligned placement needs a healthy snapshot (begin_epoch first)");
        let hw_pos = self.last_hw_pos.expect("begin_epoch recorded a healthy snapshot");
        let htstamp_us = ns_to_us_floor(self.last_htstamp_ns);
        let appl_pos = self.w;
        let mut st = self.aligned.take().expect("aligned state");
        let planned_end_before = appl_pos + st.plan_frames;
        debug_assert_eq!(st.align_offset_us, align_offset_us);

        // Signed i128, `div_round_half_away` (the rule of `t_output_us`).
        let rate = PCM_RATE_HZ as i128;
        let t_release = submission.t_release_us as i128;
        let n_star =
            hw_pos as i128 + div_round_half_away((t_release + align_offset_us as i128 - htstamp_us as i128) * rate, 1_000_000);
        let floor_us = htstamp_us as i128 + div_round_half_away((appl_pos as i128 - hw_pos as i128) * 1_000_000, rate) - t_release;
        let l = frames as i128;
        let (a, end) = (appl_pos as i128, planned_end_before as i128);
        let case = if n_star >= end {
            PlacementCase::Gap
        } else if n_star >= a {
            PlacementCase::QueueOverlap
        } else if n_star + l > a {
            PlacementCase::DeviceOverlap
        } else {
            PlacementCase::TooLate
        };
        let mut trims = Vec::new();
        let mut head_cut = 0u64;
        let mut silence = 0u64;
        let mut rejected = None;
        // Where the tick's first remaining frame goes, if it is placed.
        let mut start = None;
        match case {
            PlacementCase::Gap => {
                // The candidate's waiting frames: the whole plan after it,
                // planned silence included ([appl_pos, n* + L)).
                if n_star + l - a > self.cfg.queue_limit_frames as i128 {
                    rejected = Some(DISCARD_PLACEMENT_QUEUE_FULL);
                } else {
                    silence = (n_star - end) as u64;
                    start = Some(n_star as u64);
                }
            }
            PlacementCase::QueueOverlap => {
                trims = self.cut_from(&mut st, n_star as u64, submission);
                start = Some(n_star as u64);
            }
            PlacementCase::DeviceOverlap => {
                // No rewind: every planned frame goes, then the head.
                trims = self.cut_from(&mut st, appl_pos, submission);
                head_cut = (a - n_star) as u64;
                start = Some(appl_pos);
            }
            PlacementCase::TooLate => {}
        }
        let counters = self.stats.aligned.as_mut().expect("aligned counters");
        let admission = match (case, start) {
            (_, Some(start)) => {
                if silence > 0 {
                    st.plan.push_back(Segment::Silence { frames: silence });
                    st.plan_frames += silence;
                    st.plan_silence_frames += silence;
                }
                assert_eq!(appl_pos + st.plan_frames, start, "the tick starts where the plan ends");
                let mut pcm = Vec::with_capacity(frames as usize * PCM_CHANNELS);
                for bytes in payload.chunks_exact(2) {
                    let sample = i16::from_le_bytes([bytes[0], bytes[1]]);
                    for _ in 0..UPSAMPLE_FACTOR {
                        pcm.push(sample);
                        pcm.push(sample);
                    }
                }
                let id = st.next_id;
                st.next_id += 1;
                let remaining = frames - head_cut;
                st.ticks.insert(
                    id,
                    AlignedTick {
                        submission,
                        pcm,
                        samples_8k: samples as u64,
                        head_cut,
                        n_k: start,
                        frames: remaining,
                        written: 0,
                        t_write_us: None,
                        t_write_last_us: None,
                    },
                );
                st.plan.push_back(Segment::Tick { id, offset: head_cut, frames: remaining });
                st.plan_frames += remaining;
                counters.frames_silence_inserted += silence;
                counters.frames_head_cut += head_cut;
                if head_cut > 0 {
                    counters.ticks_head_cut += 1;
                }
                counters.max_waiting_silence_frames = counters.max_waiting_silence_frames.max(st.plan_silence_frames);
                self.stats.ticks_admitted += 1;
                self.stats.max_unwritten_tick_frames = self.stats.max_unwritten_tick_frames.max(st.plan_tick_frames());
                AlignedAdmission::Placed
            }
            (PlacementCase::Gap, None) => {
                counters.ticks_placement_queue_full += 1;
                AlignedAdmission::PlacementQueueFull
            }
            (_, None) => {
                counters.ticks_too_late += 1;
                AlignedAdmission::TooLate
            }
        };
        let planned_end_after = appl_pos + st.plan_frames;
        assert!(planned_end_after >= appl_pos);
        assert!(
            st.plan_silence_frames <= self.cfg.queue_limit_frames,
            "waiting silence {} > Q {}",
            st.plan_silence_frames,
            self.cfg.queue_limit_frames
        );
        self.aligned = Some(st);
        let occluded = trims.iter().filter(|t| t.start_removed).map(|t| t.submission).collect();
        let record = PlacementRecord {
            submission,
            status_htstamp_us: htstamp_us,
            hw_pos,
            appl_pos,
            planned_end_before,
            n_star: i64::try_from(n_star).expect("n* of a CLOCK_MONOTONIC µs time fits i64"),
            case,
            floor_us: i64::try_from(floor_us).expect("floor of a CLOCK_MONOTONIC µs time fits i64"),
            head_cut_frames: head_cut,
            silence_inserted_frames: silence,
            planned_end_after,
            rejected,
        };
        AlignedOffered { placement: Some(record), trims, occluded, admission }
    }

    /// Remove every planned frame at or after `pos` (`w <= pos <=
    /// planned_end`), silence included. Each affected tick gets one trim
    /// (plan order). A tick whose first frame is removed is occluded: none of
    /// it was written (asserted) and it leaves the plan. A tick whose start
    /// was written but whose tail is cut shrinks its recorded `frames`, the
    /// `tick_extent` end and `last_tick_end_frame`.
    fn cut_from(&mut self, st: &mut AlignedState, pos: u64, by: L2Submission) -> Vec<TrimRecord> {
        let mut p = self.w;
        assert!(pos >= p && pos <= p + st.plan_frames, "cut inside the plan");
        let mut kept = VecDeque::with_capacity(st.plan.len());
        let mut removed: Vec<(u64, u64)> = Vec::new();
        for seg in st.plan.drain(..) {
            let f = seg.frames();
            if p + f <= pos {
                kept.push_back(seg);
            } else {
                let keep = pos.saturating_sub(p);
                let cut = f - keep;
                match seg {
                    Segment::Silence { .. } => {
                        if keep > 0 {
                            kept.push_back(Segment::Silence { frames: keep });
                        }
                        st.plan_silence_frames -= cut;
                    }
                    Segment::Tick { id, offset, .. } => {
                        if keep > 0 {
                            kept.push_back(Segment::Tick { id, offset, frames: keep });
                        }
                        match removed.iter_mut().find(|(i, _)| *i == id) {
                            Some(entry) => entry.1 += cut,
                            None => removed.push((id, cut)),
                        }
                    }
                }
            }
            p += f;
        }
        st.plan = kept;
        st.plan_frames = pos - self.w;
        let counters = self.stats.aligned.as_mut().expect("aligned counters");
        let mut trims = Vec::with_capacity(removed.len());
        for (id, cut) in removed {
            let tick = st.ticks.get_mut(&id).expect("a planned slice has its tick");
            counters.frames_tail_cut += cut;
            let start_removed = tick.n_k >= pos;
            if start_removed {
                assert_eq!(tick.written, 0, "start_removed implies that no frame of the tick was written");
                assert_eq!(cut, tick.frames, "an occluded tick loses every frame");
                assert!(!st.in_flight.contains(&id), "an occluded tick never entered in_flight");
                let tick = st.ticks.remove(&id).expect("present");
                counters.ticks_occluded += 1;
                trims.push(TrimRecord { submission: tick.submission, by, tail_cut_frames: cut, start_removed: true });
            } else {
                tick.frames -= cut;
                if tick.written > 0 {
                    // Started being written, so it is the latest such tick.
                    assert_eq!(self.tick_extent.map(|e| e.0), Some(tick.n_k), "the trimmed tick is the latest written");
                    let end = tick.n_k + tick.frames;
                    self.tick_extent = Some((tick.n_k, end));
                    self.stats.last_tick_end_frame = Some(end);
                    if tick.written == tick.frames {
                        self.stats.ticks_written += 1; // its remaining frames are all written now
                    }
                }
                trims.push(TrimRecord { submission: tick.submission, by, tail_cut_frames: cut, start_removed: false });
            }
        }
        trims
    }

    /// Aligned write-ahead fill: `fill_est` = the last healthy snapshot's
    /// `delay` + frames written since it (`w − hw_pos`, the design's
    /// `appl_pos − hw_pos`, NOT `buffer − avail`). While `fill_est < W` and
    /// `avail >= period`, ONE period-sized chunk is written from the plan
    /// front (it may span segments; padded with silence if the plan runs out
    /// mid-chunk; one period of silence if the plan is empty); `fill_est`
    /// and `avail` advance by the frames actually written. A short write
    /// ends the call: the unwritten planned frames stay first in the plan
    /// (the unwritten silence pad lies past `planned_end` and is not kept).
    /// The available space is NOT filled as the FIFO placement does.
    fn fill_aligned(&mut self, pcm: &mut dyn HapticPcm, avail: u64, clock: &mut dyn FnMut() -> u64) -> Result<(), PcmError> {
        let mut st = self.aligned.take().expect("aligned state");
        let result = self.fill_aligned_with(&mut st, pcm, avail, clock);
        self.aligned = Some(st);
        result
    }

    fn fill_aligned_with(
        &mut self,
        st: &mut AlignedState,
        pcm: &mut dyn HapticPcm,
        avail: u64,
        clock: &mut dyn FnMut() -> u64,
    ) -> Result<(), PcmError> {
        let period = self.cfg.period_frames;
        let mut avail = avail.min(self.cfg.buffer_frames);
        let hw_pos = self.last_hw_pos.expect("fill after begin_epoch");
        let mut fill_est = self.w as i128 - hw_pos as i128;
        let mut chunk: Vec<i16> = Vec::with_capacity(period as usize * PCM_CHANNELS);
        while fill_est < st.write_ahead_frames as i128 && avail >= period {
            chunk.clear();
            let mut planned = 0u64;
            for seg in &st.plan {
                if planned == period {
                    break;
                }
                let n = seg.frames().min(period - planned);
                match *seg {
                    Segment::Silence { .. } => chunk.resize(chunk.len() + n as usize * PCM_CHANNELS, 0),
                    Segment::Tick { id, offset, .. } => {
                        let from = offset as usize * PCM_CHANNELS;
                        chunk.extend_from_slice(&st.ticks[&id].pcm[from..from + n as usize * PCM_CHANNELS]);
                    }
                }
                planned += n;
            }
            chunk.resize(period as usize * PCM_CHANNELS, 0);
            let t_before = clock();
            let result = pcm.writei(&chunk, t_before);
            let t_after = clock();
            self.stats.writei_calls += 1;
            self.stats.writei_max_us = self.stats.writei_max_us.max(t_after.saturating_sub(t_before));
            let written = match result {
                Ok(k) => k.min(period),
                Err(PcmError::WouldBlock) => {
                    self.stats.writei_wouldblock += 1;
                    break;
                }
                Err(e) => return Err(e),
            };
            if written == 0 {
                self.stats.writei_wouldblock += 1;
                break;
            }
            if written < period {
                self.stats.writei_short += 1;
            }
            let from_plan = written.min(planned);
            self.consume(st, from_plan, t_after);
            // Silence past planned_end: planned_end = w + plan_frames follows w.
            self.stats.silence_frames_written += written - from_plan;
            self.w += written;
            self.stats.frames_written += written;
            avail -= written;
            fill_est += written as i128;
            if written < period {
                break; // short write: the planned rest goes first next time
            }
        }
        Ok(())
    }

    /// The first `n` planned frames were written (at `w..w + n`, before `w`
    /// advances). A tick enters `in_flight` (its `n_k` and `t_write_us` are
    /// fixed) when its first remaining frame is written.
    fn consume(&mut self, st: &mut AlignedState, n: u64, t_after: u64) {
        let mut rem = n;
        let mut p = self.w;
        while rem > 0 {
            let seg = st.plan.front_mut().expect("written frames come from the plan");
            let m = seg.frames().min(rem);
            match seg {
                Segment::Silence { frames } => {
                    *frames -= m;
                    st.plan_silence_frames -= m;
                    self.stats.silence_frames_written += m;
                }
                Segment::Tick { id, offset, frames } => {
                    let tick = st.ticks.get_mut(id).expect("a planned slice has its tick");
                    if tick.written == 0 {
                        assert_eq!(tick.n_k, p, "a tick's first remaining frame is written at its planned frame");
                        tick.t_write_us = Some(t_after);
                        st.in_flight.push_back(*id);
                        let end = tick.n_k + tick.frames;
                        self.tick_extent = Some((tick.n_k, end));
                        self.stats.last_tick_end_frame = Some(end);
                    }
                    tick.written += m;
                    tick.t_write_last_us = Some(t_after);
                    self.stats.tick_frames_written += m;
                    if tick.written == tick.frames {
                        self.stats.ticks_written += 1;
                    }
                    *offset += m;
                    *frames -= m;
                }
            }
            if seg.frames() == 0 {
                st.plan.pop_front();
            }
            st.plan_frames -= m;
            rem -= m;
            p += m;
        }
    }

    /// Write what the snapshot's `avail` allows: the chunk in progress, then
    /// whole ticks back to back, then — only with an empty queue and a
    /// queued fill below `F` — one period of silence.
    /// Aligned: the write-ahead fill ([`Self::fill_aligned`]).
    pub fn fill(&mut self, pcm: &mut dyn HapticPcm, avail: u64, clock: &mut dyn FnMut() -> u64) -> Result<(), PcmError> {
        if self.aligned.is_some() {
            return self.fill_aligned(pcm, avail, clock);
        }
        let mut avail = avail.min(self.cfg.buffer_frames);
        let mut silence_written = false;
        loop {
            if self.current.is_none() {
                if let Some(head) = self.pending.front() {
                    if head.frames > avail {
                        break; // wait for space; nothing else is written meanwhile
                    }
                    let tick = self.pending.pop_front().expect("head exists");
                    self.pending_frames -= tick.frames;
                    // The tick's full extent counts from the moment n_k is
                    // fixed, so a partly written tick is never "played".
                    let extent = (self.w, self.w + tick.frames);
                    self.tick_extent = Some(extent);
                    self.stats.last_tick_end_frame = Some(extent.1);
                    self.current = Some(Current {
                        kind: ChunkKind::Tick {
                            submission: tick.submission,
                            n_k: self.w,
                            samples_8k: tick.samples_8k,
                            t_write_us: None,
                        },
                        total_frames: tick.frames,
                        pcm: tick.pcm,
                        offset_frames: 0,
                    });
                } else {
                    let queued = self.cfg.buffer_frames - avail;
                    if silence_written || queued >= self.cfg.target_fill_frames || avail < self.cfg.period_frames {
                        break;
                    }
                    self.current = Some(Current {
                        kind: ChunkKind::Silence,
                        pcm: Vec::new(),
                        total_frames: self.cfg.period_frames,
                        offset_frames: 0,
                    });
                    silence_written = true;
                }
            }
            let current = self.current.as_mut().expect("a chunk is selected");
            let remaining = current.total_frames - current.offset_frames;
            let n = remaining.min(avail);
            if n == 0 {
                break;
            }
            let zeros;
            let slice: &[i16] = match current.kind {
                ChunkKind::Silence => {
                    zeros = vec![0i16; n as usize * PCM_CHANNELS];
                    &zeros
                }
                ChunkKind::Tick { .. } => {
                    let from = current.offset_frames as usize * PCM_CHANNELS;
                    &current.pcm[from..from + n as usize * PCM_CHANNELS]
                }
            };
            let t_before = clock();
            let result = pcm.writei(slice, t_before);
            let t_after = clock();
            self.stats.writei_calls += 1;
            self.stats.writei_max_us = self.stats.writei_max_us.max(t_after.saturating_sub(t_before));
            let written = match result {
                Ok(k) => k.min(n),
                Err(PcmError::WouldBlock) => {
                    self.stats.writei_wouldblock += 1;
                    break;
                }
                Err(e) => return Err(e),
            };
            if written == 0 {
                self.stats.writei_wouldblock += 1;
                break;
            }
            if written < n {
                self.stats.writei_short += 1;
            }
            self.w += written;
            avail -= written;
            self.stats.frames_written += written;
            current.offset_frames += written;
            match &mut current.kind {
                ChunkKind::Silence => self.stats.silence_frames_written += written,
                ChunkKind::Tick { t_write_us, .. } => {
                    self.stats.tick_frames_written += written;
                    t_write_us.get_or_insert(t_after);
                }
            }
            if current.offset_frames < current.total_frames {
                break; // short write: the rest goes first next time
            }
            let done = self.current.take().expect("the chunk is complete");
            if let ChunkKind::Tick { submission, n_k, samples_8k, t_write_us } = done.kind {
                self.in_flight.push_back(WrittenTick {
                    submission,
                    n_k,
                    frames: done.total_frames,
                    samples_8k,
                    t_write_us: t_write_us.unwrap_or(t_after),
                    t_write_done_us: t_after,
                });
                self.stats.ticks_written += 1;
            }
        }
        Ok(())
    }

    /// Validate one snapshot and emit every written tick whose first frame
    /// the hardware has passed (`hw_pos > n_k`). `t0` / `t1` are the clock
    /// reads immediately before and after the status call. On a failure no
    /// output is emitted from this snapshot.
    pub fn observe(&mut self, s: &PcmStatus, t0: u64, t1: u64) -> Result<Vec<Placed>, DeviceFailure> {
        let epoch = *self.epoch.as_ref().expect("observe after begin_epoch");
        self.stats.status_calls += 1;
        if let Some(previous) = self.last_status_t0_us {
            self.stats.note_gap(t0.saturating_sub(previous));
        }
        self.last_status_t0_us = Some(t0);
        let reason = match s.state {
            PcmState::Running => None,
            PcmState::Xrun => Some(FAIL_ALSA_XRUN),
            PcmState::Suspended => Some(FAIL_ALSA_SUSPENDED),
            PcmState::Disconnected => Some(FAIL_ALSA_DISCONNECTED),
            _ => Some(FAIL_ALSA_STATE_UNEXPECTED),
        };
        if let Some(reason) = reason {
            return Err(self.failure(reason, Some(s), t1, format!("state {}", s.state.as_str())));
        }
        if s.trigger_htstamp_ns != epoch.trigger_htstamp_ns {
            return Err(self.failure(
                FAIL_ALSA_EPOCH_RESET,
                Some(s),
                t1,
                format!(
                    "trigger moved: {} us != epoch trigger {} us",
                    ns_to_us_floor(s.trigger_htstamp_ns),
                    ns_to_us_floor(epoch.trigger_htstamp_ns)
                ),
            )
            .with_raw_ns(&[
                ("trigger_htstamp_ns", s.trigger_htstamp_ns),
                ("epoch_trigger_htstamp_ns", epoch.trigger_htstamp_ns),
            ]));
        }
        let appl_pos = self.w;
        let buffer = self.cfg.buffer_frames;
        if s.avail > buffer || s.delay < 0 {
            return Err(self.failure(
                FAIL_ALSA_STATUS_INCONSISTENT,
                None,
                t1,
                format!("avail {} delay {} buffer {buffer}", s.avail, s.delay),
            ));
        }
        let queued = buffer - s.avail;
        // The DMA pointer cannot be before frame 0 of the epoch. (`delay` CAN
        // exceed `appl_pos` early on: a driver delay puts the output position
        // `hw_pos` before frame 0, which is not an error.)
        if queued > appl_pos {
            return Err(self.failure(
                FAIL_ALSA_EPOCH_RESET,
                None,
                t1,
                format!("queued {queued} beyond appl_pos {appl_pos}"),
            ));
        }
        let hw_ptr_pos = appl_pos - queued;
        if hw_ptr_pos < self.last_hw_ptr_pos {
            return Err(self.failure(
                FAIL_ALSA_EPOCH_RESET,
                None,
                t1,
                format!("DMA pointer went back {} -> {hw_ptr_pos}", self.last_hw_ptr_pos),
            ));
        }
        if s.htstamp_ns < self.last_htstamp_ns {
            return Err(self.failure(
                FAIL_ALSA_STATUS_INCONSISTENT,
                None,
                t1,
                format!(
                    "htstamp went back: {} us -> {} us",
                    ns_to_us_floor(self.last_htstamp_ns),
                    ns_to_us_floor(s.htstamp_ns)
                ),
            )
            .with_raw_ns(&[("previous_htstamp_ns", self.last_htstamp_ns), ("htstamp_ns", s.htstamp_ns)]));
        }
        let hw_pos = appl_pos as i64 - s.delay;
        let htstamp_us = ns_to_us_floor(s.htstamp_ns);
        let mut placed = Vec::new();
        while let Some(front) = self.in_flight.front() {
            if (front.n_k as i64) >= hw_pos {
                break;
            }
            let Some(t_output_us) = output_time_us(htstamp_us, front.n_k, hw_pos) else {
                return Err(self.failure(
                    FAIL_ALSA_STATUS_INCONSISTENT,
                    None,
                    t1,
                    format!("negative output time for n_k {}", front.n_k),
                ));
            };
            let tick = self.in_flight.pop_front().expect("front exists");
            let n_k_minus_hw_pos = tick.n_k as i64 - hw_pos;
            placed.push(Placed {
                submission: tick.submission,
                t_output_us,
                t_callback_us: t1,
                detail: AlsaOutputDetail {
                    backend: self.backend,
                    n_k: tick.n_k,
                    frames: tick.frames,
                    samples_8k: tick.samples_8k,
                    t_write_us: tick.t_write_us,
                    t_write_done_us: tick.t_write_done_us,
                    status_t0_us: t0,
                    htstamp_us,
                    trigger_htstamp_us: ns_to_us_floor(s.trigger_htstamp_ns),
                    driver_htstamp_us: ns_to_us_floor(s.driver_htstamp_ns),
                    appl_pos,
                    delay_frames: s.delay,
                    avail_frames: s.avail,
                    hw_pos,
                    queued_frames: queued,
                    hw_ptr_pos,
                    driver_delay_frames: s.delay - queued as i64,
                    n_k_minus_hw_pos,
                    offset_us: frames_to_us_round_half_away(n_k_minus_hw_pos, PCM_RATE_HZ),
                    state: s.state.as_str(),
                    audio_tstamp_type_requested: self.audio_tstamp_type_requested,
                    audio_tstamp: s.audio_tstamp,
                    raw: RawSnapshot::of(s),
                    head_cut_frames: None,
                },
            });
        }
        if let Some(mut st) = self.aligned.take() {
            // Aligned: a tick is output once NO planned frame of it remains
            // (fully written, or its tail trimmed to what was written), so
            // its `frames` and `t_write_done_us` are final, and the snapshot
            // shows hw_pos > n_k; the same formula as above.
            while let Some(&id) = st.in_flight.front() {
                let tick = &st.ticks[&id];
                if tick.written < tick.frames || (tick.n_k as i64) >= hw_pos {
                    break;
                }
                let Some(t_output_us) = output_time_us(htstamp_us, tick.n_k, hw_pos) else {
                    let n_k = tick.n_k;
                    self.aligned = Some(st);
                    return Err(self.failure(
                        FAIL_ALSA_STATUS_INCONSISTENT,
                        None,
                        t1,
                        format!("negative output time for n_k {n_k}"),
                    ));
                };
                st.in_flight.pop_front();
                let tick = st.ticks.remove(&id).expect("an in-flight tick is placed");
                let n_k_minus_hw_pos = tick.n_k as i64 - hw_pos;
                placed.push(Placed {
                    submission: tick.submission,
                    t_output_us,
                    t_callback_us: t1,
                    detail: AlsaOutputDetail {
                        backend: self.backend,
                        n_k: tick.n_k,
                        frames: tick.frames,
                        samples_8k: tick.samples_8k,
                        t_write_us: tick.t_write_us.expect("written"),
                        t_write_done_us: tick.t_write_last_us.expect("written"),
                        status_t0_us: t0,
                        htstamp_us,
                        trigger_htstamp_us: ns_to_us_floor(s.trigger_htstamp_ns),
                        driver_htstamp_us: ns_to_us_floor(s.driver_htstamp_ns),
                        appl_pos,
                        delay_frames: s.delay,
                        avail_frames: s.avail,
                        hw_pos,
                        queued_frames: queued,
                        hw_ptr_pos,
                        driver_delay_frames: s.delay - queued as i64,
                        n_k_minus_hw_pos,
                        offset_us: frames_to_us_round_half_away(n_k_minus_hw_pos, PCM_RATE_HZ),
                        state: s.state.as_str(),
                        audio_tstamp_type_requested: self.audio_tstamp_type_requested,
                        audio_tstamp: s.audio_tstamp,
                        raw: RawSnapshot::of(s),
                        head_cut_frames: Some(tick.head_cut),
                    },
                });
            }
            self.aligned = Some(st);
        }
        self.stats.ticks_output += placed.len() as u64;
        self.stats.queued_max_frames = self.stats.queued_max_frames.max(queued);
        self.stats.queued_min_frames = Some(self.stats.queued_min_frames.map_or(queued, |m| m.min(queued)));
        // The device was healthy at this snapshot, taken after `t0`.
        self.last_good_status_t0_us = t0;
        self.last_hw_ptr_pos = hw_ptr_pos;
        self.last_htstamp_ns = s.htstamp_ns;
        self.last_hw_pos = Some(hw_pos);
        self.stats.last_hw_pos = self.last_hw_pos;
        Ok(placed)
    }

    /// A device failure from an error return (`call` failed with `error`),
    /// optionally with a snapshot taken right after it.
    pub fn failure_from_error(
        &self,
        error: &PcmError,
        after: Option<&PcmStatus>,
        detected_us: u64,
        call: &'static str,
    ) -> DeviceFailure {
        let reason = match error {
            PcmError::Xrun => FAIL_ALSA_XRUN,
            PcmError::Suspended => FAIL_ALSA_SUSPENDED,
            PcmError::Disconnected => FAIL_ALSA_DISCONNECTED,
            _ if call == "status" => FAIL_ALSA_STATUS_ERROR,
            _ => FAIL_ALSA_WRITE_ERROR,
        };
        self.failure(reason, after, detected_us, format!("{call}: {error}"))
    }

    /// No occurrence time (registration 6-1, Codex 109 P1): the
    /// observation interval [clock read before the last healthy status
    /// call, detection], the detection and the raw kernel trigger (when the
    /// failed state moved it) are kept apart.
    fn failure(&self, reason: &'static str, s: Option<&PcmStatus>, detected_us: u64, detail: String) -> DeviceFailure {
        let epoch = self.epoch.as_ref();
        let moved_trigger_ns = s.zip(epoch).and_then(|(s, epoch)| {
            (s.trigger_htstamp_ns != epoch.trigger_htstamp_ns).then_some(s.trigger_htstamp_ns)
        });
        DeviceFailure {
            reason,
            last_good_status_t0_us: self.last_good_status_t0_us.min(detected_us),
            detected_us,
            kernel_trigger_us: moved_trigger_ns.map(ns_to_us_floor),
            kernel_trigger_ns: moved_trigger_ns,
            detail,
            raw_ns: Vec::new(),
        }
    }

    /// Everything submitted that can no longer be output (after a device
    /// failure): written-but-unobserved ticks, the tick in progress and the
    /// write queue, in submission order of their placement. Aligned: every
    /// placed tick not yet output, once each (a partly written tick is in
    /// `in_flight` and in the plan), in placement order.
    pub fn abandon(&mut self) -> Vec<L2Submission> {
        if let Some(st) = self.aligned.as_mut() {
            let out = std::mem::take(&mut st.ticks).into_values().map(|t| t.submission).collect();
            st.plan.clear();
            st.plan_frames = 0;
            st.plan_silence_frames = 0;
            st.in_flight.clear();
            return out;
        }
        let mut out: Vec<L2Submission> = self.in_flight.drain(..).map(|t| t.submission).collect();
        if let Some(Current { kind: ChunkKind::Tick { submission, .. }, .. }) = self.current.take() {
            out.push(submission);
        }
        out.extend(self.pending.drain(..).map(|t| t.submission));
        self.pending_frames = 0;
        out
    }
}

/// A fault the simulated stream injects at `after_start_us` past its start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimFault {
    /// The stream stops in XRUN (kernel trigger timestamp moves to it).
    Xrun { after_start_us: u64 },
    Suspend { after_start_us: u64 },
    /// The stream was restarted behind the lane's back: the trigger moves
    /// and the DMA pointer goes back by `rewind_frames`.
    EpochReset { after_start_us: u64, rewind_frames: u64 },
    /// The status call fails (EIO) from then on.
    StatusError { after_start_us: u64 },
    /// `writei` fails (EIO) from then on.
    WriteError { after_start_us: u64 },
    /// `start` itself fails (EBUSY), as a device held by another client would.
    StartError,
}

/// Simulated stream configuration.
#[derive(Debug, Clone)]
pub struct SimPcmConfig {
    pub period_frames: u64,
    pub buffer_frames: u64,
    /// Position reporting granularity (HDA: 8/16 frames at 48 kHz S16 stereo).
    pub granularity_frames: u64,
    /// Driver delay folded into `delay` (the M2b probe observed 32 frames
    /// on hw:1,0; the default here is 0).
    pub driver_delay_frames: i64,
    /// The audio timestamp type the "hardware" reports as actual.
    pub audio_tstamp_actual: u32,
    pub faults: Vec<SimFault>,
    /// Test knob: at most this many frames per `writei` (short writes).
    pub max_frames_per_write: Option<u64>,
}

impl SimPcmConfig {
    pub fn new(period_frames: u64, buffer_frames: u64) -> Self {
        Self {
            period_frames,
            buffer_frames,
            granularity_frames: 16,
            driver_delay_frames: 0,
            audio_tstamp_actual: AUDIO_TSTAMP_TYPE_LINK,
            faults: Vec::new(),
            max_frames_per_write: None,
        }
    }
}

/// A deterministic model of an `hw:` playback stream: a ring of
/// `buffer_frames` consumed at exactly 48 kHz from `start`, positions
/// quantised to the granularity, XRUN when the consumer reaches the written
/// position, and the injected faults. All times come from the caller.
pub struct SimPcm {
    cfg: SimPcmConfig,
    params: PcmParams,
    appl: u64,
    start_us: Option<u64>,
    /// `(state, at_us)` once the stream stopped.
    stopped: Option<(PcmState, u64)>,
    trigger_us: u64,
    rewound_frames: u64,
    dropped: bool,
}

impl SimPcm {
    pub fn new(cfg: SimPcmConfig) -> Self {
        let params = PcmParams {
            backend: "simulated",
            device: "sim".into(),
            rate_hz: PCM_RATE_HZ,
            channels: PCM_CHANNELS as u64,
            format: "S16_LE",
            access: "RW_INTERLEAVED",
            period_frames: cfg.period_frames,
            buffer_frames: cfg.buffer_frames,
            tstamp_mode: "ENABLE",
            tstamp_type: "MONOTONIC",
            audio_tstamp_type_requested: AUDIO_TSTAMP_TYPE_LINK,
            library: None,
            details: serde_json::Value::Null,
        };
        Self { cfg, params, appl: 0, start_us: None, stopped: None, trigger_us: 0, rewound_frames: 0, dropped: false }
    }

    fn fault_due(&self, now_us: u64, pick: impl Fn(&SimFault) -> Option<u64>) -> Option<u64> {
        let start = self.start_us?;
        self.cfg.faults.iter().filter_map(pick).map(|after| start + after).filter(|at| *at <= now_us).min()
    }

    /// Raw consumed frames at `now` (continuous model, before quantisation).
    fn consumed(&self, now_us: u64) -> u64 {
        let Some(start) = self.start_us else { return 0 };
        ((now_us.saturating_sub(start) as u128 * PCM_RATE_HZ as u128) / 1_000_000) as u64
    }

    /// Bring the state up to `now`: XRUN when the consumer reached `appl`,
    /// and the injected faults.
    fn advance(&mut self, now_us: u64) {
        if self.stopped.is_some() || self.start_us.is_none() || self.dropped {
            return;
        }
        let start = self.start_us.expect("started");
        let mut events: Vec<(u64, PcmState)> = Vec::new();
        // The consumer reaches `appl` (raw, continuous) at this instant.
        let empty_at = start + ((self.appl as u128 * 1_000_000).div_ceil(PCM_RATE_HZ as u128)) as u64;
        if self.consumed(now_us) >= self.appl {
            events.push((empty_at, PcmState::Xrun));
        }
        if let Some(at) = self.fault_due(now_us, |f| match f {
            SimFault::Xrun { after_start_us } => Some(*after_start_us),
            _ => None,
        }) {
            events.push((at, PcmState::Xrun));
        }
        if let Some(at) = self.fault_due(now_us, |f| match f {
            SimFault::Suspend { after_start_us } => Some(*after_start_us),
            _ => None,
        }) {
            events.push((at, PcmState::Suspended));
        }
        if let Some((at, state)) = events.into_iter().min_by_key(|(at, _)| *at) {
            self.stopped = Some((state, at));
            self.trigger_us = at;
            return;
        }
        if let Some(at) = self.fault_due(now_us, |f| match f {
            SimFault::EpochReset { after_start_us, .. } => Some(*after_start_us),
            _ => None,
        }) {
            if self.trigger_us != at {
                self.trigger_us = at;
                self.rewound_frames = self
                    .cfg
                    .faults
                    .iter()
                    .find_map(|f| match f {
                        SimFault::EpochReset { rewind_frames, .. } => Some(*rewind_frames),
                        _ => None,
                    })
                    .unwrap_or(0);
            }
        }
    }

    /// The reported DMA position at `now` (quantised, never past `appl`).
    fn hw_ptr(&self, now_us: u64) -> u64 {
        let g = self.cfg.granularity_frames.max(1);
        let consumed = self.consumed(now_us).min(self.appl);
        (consumed / g * g).saturating_sub(self.rewound_frames)
    }
}

impl HapticPcm for SimPcm {
    fn params(&self) -> &PcmParams {
        &self.params
    }

    fn writei(&mut self, pcm: &[i16], now_us: u64) -> Result<u64, PcmError> {
        self.advance(now_us);
        if self.dropped {
            return Err(PcmError::Other { errno: libc::EBADFD, call: "writei" });
        }
        if self.fault_due(now_us, |f| match f {
            SimFault::WriteError { after_start_us } => Some(*after_start_us),
            _ => None,
        })
        .is_some()
        {
            return Err(PcmError::Other { errno: libc::EIO, call: "writei" });
        }
        match self.stopped {
            Some((PcmState::Xrun, _)) => return Err(PcmError::Xrun),
            Some((PcmState::Suspended, _)) => return Err(PcmError::Suspended),
            Some(_) => return Err(PcmError::Other { errno: libc::EBADFD, call: "writei" }),
            None => {}
        }
        assert!(pcm.len() % PCM_CHANNELS == 0, "interleaved stereo");
        let frames = (pcm.len() / PCM_CHANNELS) as u64;
        let queued = self.appl - self.hw_ptr(now_us).min(self.appl);
        let space = self.cfg.buffer_frames.saturating_sub(queued);
        let n = frames.min(space).min(self.cfg.max_frames_per_write.unwrap_or(u64::MAX));
        if n == 0 {
            return Err(PcmError::WouldBlock);
        }
        self.appl += n;
        Ok(n)
    }

    fn start(&mut self, now_us: u64) -> Result<(), PcmError> {
        if self.cfg.faults.contains(&SimFault::StartError) {
            return Err(PcmError::Other { errno: libc::EBUSY, call: "start" });
        }
        if self.start_us.is_some() {
            return Err(PcmError::Other { errno: libc::EBADFD, call: "start" });
        }
        self.start_us = Some(now_us);
        self.trigger_us = now_us;
        Ok(())
    }

    fn status(&mut self, now_us: u64) -> Result<PcmStatus, PcmError> {
        self.advance(now_us);
        if self
            .fault_due(now_us, |f| match f {
                SimFault::StatusError { after_start_us } => Some(*after_start_us),
                _ => None,
            })
            .is_some()
        {
            return Err(PcmError::Other { errno: libc::EIO, call: "status" });
        }
        let report = AudioTstampReport {
            valid: true,
            actual_type: self.cfg.audio_tstamp_actual,
            accuracy_report: self.cfg.audio_tstamp_actual == AUDIO_TSTAMP_TYPE_LINK,
            accuracy_ns: if self.cfg.audio_tstamp_actual == AUDIO_TSTAMP_TYPE_LINK { 42 } else { 0 },
        };
        let (state, running) = match (self.start_us, self.stopped, self.dropped) {
            (_, _, true) => (PcmState::Setup, false),
            (None, _, _) => (PcmState::Prepared, false),
            (Some(_), Some((state, _)), _) => (state, false),
            (Some(_), None, _) => (PcmState::Running, true),
        };
        let hw = self.hw_ptr(now_us);
        let queued = self.appl.saturating_sub(hw);
        let (delay, avail) = if running {
            (queued as i64 + self.cfg.driver_delay_frames, self.cfg.buffer_frames.saturating_sub(queued))
        } else {
            (0, self.cfg.buffer_frames)
        };
        let since_start_ns = self.start_us.map_or(0, |s| now_us.saturating_sub(s) * 1_000);
        Ok(PcmStatus {
            state,
            htstamp_ns: now_us * 1_000,
            trigger_htstamp_ns: self.trigger_us * 1_000,
            audio_htstamp_ns: since_start_ns,
            driver_htstamp_ns: now_us * 1_000,
            delay,
            avail,
            avail_max: avail,
            audio_tstamp: report,
        })
    }

    fn stop(&mut self) -> Result<(), PcmError> {
        self.dropped = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::l2_output::{L2Identity, L2Modality};

    const PERIOD: u64 = 256;
    const BUFFER: u64 = 2_048;

    fn cfg() -> PlacementConfig {
        PlacementConfig {
            period_frames: PERIOD,
            buffer_frames: BUFFER,
            target_fill_frames: 512,
            prefill_frames: 512,
            queue_limit_frames: 48_000,
            latency_bound_frames: None,
            mode: PlacementMode::None,
        }
    }

    fn submission(seq: u32, t: u64) -> L2Submission {
        L2Submission {
            identity: L2Identity {
                modality: L2Modality::Haptic,
                tier: 0,
                seq,
                pts_us: seq as u64 * 11_111,
                event_id: if seq % 3 == 0 { seq / 3 + 1 } else { 0 },
                route: None,
            },
            t_release_us: t,
            t_submit_us: t,
        }
    }

    /// `samples` 8 kHz samples, sample i = base + i (so the upsampling is checkable).
    fn payload(samples: usize, base: i16) -> Vec<u8> {
        (0..samples).flat_map(|i| (base + i as i16).to_le_bytes()).collect()
    }

    /// A started epoch on a simulated stream at virtual time `t0`.
    fn started(c: PlacementConfig, sim: SimPcmConfig, t0: u64) -> (HapticPlacement, SimPcm) {
        let mut pcm = SimPcm::new(sim);
        let mut engine = HapticPlacement::new(c, pcm.params()).unwrap();
        let mut clock = || t0;
        engine.prefill(&mut pcm, &mut clock).unwrap();
        pcm.start(t0).unwrap();
        let s = pcm.status(t0).unwrap();
        engine.begin_epoch(t0, s, t0).unwrap();
        (engine, pcm)
    }

    /// One lane iteration at virtual time `t`: status -> observe -> (admit) -> fill.
    fn step(engine: &mut HapticPlacement, pcm: &mut SimPcm, t: u64) -> Result<Vec<Placed>, DeviceFailure> {
        let s = pcm.status(t).map_err(|e| engine.failure_from_error(&e, None, t, "status"))?;
        let placed = engine.observe(&s, t, t)?;
        let mut clock = || t;
        engine
            .fill(pcm, s.avail, &mut clock)
            .map_err(|e| engine.failure_from_error(&e, None, t, "writei"))?;
        Ok(placed)
    }

    // ---- formula ---------------------------------------------------------

    /// The registered rounding: half away from zero, ties at ±3 frames
    /// (±62.5 µs), sign preserved, and `t_output = htstamp_us + offset`.
    #[test]
    fn the_output_formula_rounds_half_away_from_zero_in_signed_integers() {
        let cases: [(i64, i64); 10] = [
            (0, 0), (-1, -21), (1, 21), (-3, -63), (3, 63), (-2, -42), (-6, -125), (6, 125),
            (-9, -188), (-48_000, -1_000_000),
        ];
        for (frames, us) in cases {
            assert_eq!(frames_to_us_round_half_away(frames, PCM_RATE_HZ), us, "{frames} frames");
        }
        // -9 frames = -187.5 µs -> -188 (tie, away from zero); +9 -> +188.
        assert_eq!(frames_to_us_round_half_away(9, PCM_RATE_HZ), 188);
        assert_eq!(div_round_half_away(-5, 2), -3);
        assert_eq!(div_round_half_away(5, 2), 3);
        assert_eq!(div_round_half_away(-4, 3), -1);
        // n_k = 1_000, hw_pos = 1_003: three frames ago.
        assert_eq!(output_time_us(5_000_000, 1_000, 1_003), Some(5_000_000 - 63));
        assert_eq!(output_time_us(5_000_000, 1_003, 1_000), Some(5_000_000 + 63));
        assert_eq!(output_time_us(10, 0, 48_000), None, "negative is not a time");
        // htstamp truncation equals now_us's: sec*1e6 + floor(nsec/1000).
        assert_eq!(ns_to_us_floor(7_000_000_999), 7_000_000);
        assert_eq!(ns_to_us_floor(7_000_001_000), 7_000_001);
    }

    // ---- config ------------------------------------------------------------

    #[test]
    fn placement_config_refuses_unsafe_sizes() {
        assert!(cfg().validate().is_ok());
        for bad in [
            PlacementConfig { buffer_frames: 534 + 255, ..cfg() },
            PlacementConfig { target_fill_frames: BUFFER - PERIOD + 1, ..cfg() },
            PlacementConfig { prefill_frames: 0, ..cfg() },
            PlacementConfig { prefill_frames: BUFFER + 1, ..cfg() },
            PlacementConfig { queue_limit_frames: 533, ..cfg() },
            PlacementConfig { period_frames: 0, ..cfg() },
        ] {
            assert!(bad.validate().is_err(), "{bad:?}");
        }
        // The engine refuses a device whose negotiated sizes differ.
        let mut sim = SimPcmConfig::new(PERIOD, BUFFER);
        sim.buffer_frames = 4_096;
        assert!(HapticPlacement::new(cfg(), SimPcm::new(sim).params()).is_err());
    }

    // ---- prefill / epoch ---------------------------------------------------

    #[test]
    fn prefill_opens_the_epoch_with_silence_before_start() {
        let (engine, mut pcm) = started(cfg(), SimPcmConfig::new(PERIOD, BUFFER), 1_000_000);
        assert_eq!(engine.written_frames(), 512);
        assert_eq!(engine.stats().prefill_frames, 512);
        assert_eq!(engine.stats().silence_frames_written, 512);
        let epoch = engine.epoch().unwrap();
        assert_eq!(epoch.trigger_htstamp_ns, 1_000_000_000);
        let s = pcm.status(1_000_000).unwrap();
        assert_eq!((s.state, s.delay, s.avail), (PcmState::Running, 512, BUFFER - 512));
    }

    // ---- placement --------------------------------------------------------

    /// Low water: with an empty queue, exactly one period of silence is
    /// written whenever the queued fill is below F, and none above it.
    #[test]
    fn low_water_writes_one_period_of_silence_only_below_the_target() {
        let t0 = 1_000_000;
        let (mut engine, mut pcm) = started(cfg(), SimPcmConfig::new(PERIOD, BUFFER), t0);
        // At t0 queued = 512 = F: nothing is written.
        step(&mut engine, &mut pcm, t0).unwrap();
        assert_eq!(engine.written_frames(), 512);
        // 2 ms later 96 frames were consumed: queued < F -> one period.
        step(&mut engine, &mut pcm, t0 + 2_000).unwrap();
        assert_eq!(engine.written_frames(), 512 + PERIOD);
        // Above F now: nothing more in the same instant.
        step(&mut engine, &mut pcm, t0 + 2_000).unwrap();
        assert_eq!(engine.written_frames(), 512 + PERIOD);
        assert_eq!(engine.stats().tick_frames_written, 0);
        assert_eq!(engine.stats().silence_frames_written, 512 + PERIOD);
    }

    /// 88/89 samples become 528/534 frames by ×6 repetition, mono -> L=R.
    #[test]
    fn ticks_are_upsampled_by_sample_repetition_to_528_or_534_frames() {
        let t0 = 1_000_000;
        let (mut engine, mut pcm) = started(cfg(), SimPcmConfig::new(PERIOD, BUFFER), t0);
        assert_eq!(engine.admit(submission(0, t0), Some(&payload(88, 100))), Admission::Queued);
        assert_eq!(engine.admit(submission(1, t0), Some(&payload(89, -50))), Admission::Queued);
        assert_eq!(engine.pending_frames(), 528 + 534);
        let tick = engine.pending.front().unwrap();
        assert_eq!(tick.pcm.len(), 528 * 2);
        for (i, frame) in tick.pcm.chunks_exact(2).enumerate() {
            let sample = 100 + (i / 6) as i16;
            assert_eq!(frame, [sample, sample], "frame {i}");
        }
        step(&mut engine, &mut pcm, t0).unwrap();
        let written: Vec<(u64, u64, u64)> =
            engine.in_flight.iter().map(|t| (t.n_k, t.frames, t.samples_8k)).collect();
        assert_eq!(written, vec![(512, 528, 88), (1_040, 534, 89)], "back to back from w");
    }

    /// A burst is appended back to back in release order; while a tick
    /// waits for space nothing else (no silence) is written; the waiting
    /// tick keeps n_k = w.
    #[test]
    fn a_burst_is_placed_back_to_back_and_waits_whole_for_space() {
        let t0 = 1_000_000;
        let (mut engine, mut pcm) = started(cfg(), SimPcmConfig::new(PERIOD, BUFFER), t0);
        for seq in 0..5u32 {
            assert_eq!(engine.admit(submission(seq, t0), Some(&payload(89, 0))), Admission::Queued);
        }
        // avail = 1536 at t0: two whole ticks fit (1068), the third waits.
        step(&mut engine, &mut pcm, t0).unwrap();
        let n_ks: Vec<u64> = engine.in_flight.iter().map(|t| t.n_k).collect();
        assert_eq!(n_ks, vec![512, 1_046]);
        assert_eq!(engine.pending.len(), 3);
        // 1 ms later only 48 frames freed: still waiting, no silence written.
        step(&mut engine, &mut pcm, t0 + 1_000).unwrap();
        assert_eq!(engine.written_frames(), 512 + 2 * 534);
        assert_eq!(engine.stats().silence_frames_written, 512);
        // Enough space later: the rest follow contiguously, in release order.
        let mut t = t0 + 1_000;
        let mut placed: Vec<(u32, u64)> = Vec::new();
        while !engine.pending.is_empty() {
            t += 1_000;
            for p in step(&mut engine, &mut pcm, t).unwrap() {
                placed.push((p.submission.identity.seq, p.detail.n_k));
            }
        }
        placed.extend(engine.in_flight.iter().map(|t| (t.submission.identity.seq, t.n_k)));
        let expected: Vec<(u32, u64)> = (0..5u32).map(|i| (i, 512 + i as u64 * 534)).collect();
        assert_eq!(placed, expected, "contiguous, release order");
        assert_eq!(engine.stats().silence_frames_written, 512, "no silence between burst ticks");
    }

    /// A late tick (the queue is at low water, silence was just written) is
    /// placed right after that silence: n_k = w, never back-dated.
    #[test]
    fn a_late_tick_goes_at_the_current_write_position() {
        let t0 = 1_000_000;
        let (mut engine, mut pcm) = started(cfg(), SimPcmConfig::new(PERIOD, BUFFER), t0);
        step(&mut engine, &mut pcm, t0 + 2_000).unwrap(); // silence period
        let w = engine.written_frames();
        assert_eq!(w, 512 + PERIOD);
        let late = submission(9, t0 + 2_500);
        assert_eq!(engine.admit(late, Some(&payload(88, 1))), Admission::Queued);
        step(&mut engine, &mut pcm, t0 + 2_500).unwrap();
        assert_eq!(engine.in_flight.front().unwrap().n_k, w);
        // It is output when the hardware passes w, about (w − hw)/48 kHz later.
        let mut t = t0 + 2_500;
        let placed = loop {
            t += 500;
            let placed = step(&mut engine, &mut pcm, t).unwrap();
            if !placed.is_empty() {
                break placed;
            }
        };
        let p = &placed[0];
        assert_eq!(p.submission, late);
        assert!(p.detail.hw_pos > p.detail.n_k as i64);
        let ideal = t0 + (w as u128 * 1_000_000 / 48_000) as u64;
        assert!(p.t_output_us.abs_diff(ideal) <= 400, "within the 16-frame granularity: {} vs {ideal}", p.t_output_us);
        assert!(p.t_output_us >= late.t_submit_us);
    }

    /// Q: a tick that would make the write queue exceed Q is refused before
    /// submit; earlier ticks are unaffected.
    #[test]
    fn the_write_queue_limit_refuses_the_overflowing_tick() {
        let c = PlacementConfig { queue_limit_frames: 2 * 534, ..cfg() };
        let (mut engine, _pcm) = started(c, SimPcmConfig::new(PERIOD, BUFFER), 1_000_000);
        assert_eq!(engine.admit(submission(0, 1), Some(&payload(89, 0))), Admission::Queued);
        assert_eq!(engine.admit(submission(1, 2), Some(&payload(89, 0))), Admission::Queued);
        assert_eq!(engine.admit(submission(2, 3), Some(&payload(88, 0))), Admission::QueueFull);
        assert_eq!(engine.pending_frames(), 2 * 534);
        assert_eq!(engine.stats().ticks_queue_full, 1);
    }

    #[test]
    fn a_payload_that_is_not_a_playable_tick_is_refused() {
        let (mut engine, _pcm) = started(cfg(), SimPcmConfig::new(PERIOD, BUFFER), 1_000_000);
        for bad in [None, Some(Vec::new()), Some(vec![1u8, 2, 3]), Some(payload(BUFFER as usize, 0))] {
            assert_eq!(engine.admit(submission(0, 1), bad.as_deref()), Admission::PayloadInvalid);
        }
        assert_eq!(engine.stats().ticks_payload_invalid, 4);
        assert_eq!(engine.pending_frames(), 0);
    }

    // ---- observation -------------------------------------------------------

    /// Silence never produces an output; every tick produces exactly one,
    /// only once hw_pos > n_k (strict), at the registered formula.
    #[test]
    fn only_ticks_are_output_once_the_hardware_passed_their_first_frame() {
        let t0 = 1_000_000;
        let (mut engine, mut pcm) = started(cfg(), SimPcmConfig::new(PERIOD, BUFFER), t0);
        let mut outputs = Vec::new();
        let mut t = t0;
        for tick in 0..6u32 {
            // One tick every ~11.1 ms, polled every 1 ms.
            engine.admit(submission(tick, t), Some(&payload(88 + (tick % 2) as usize, 0)));
            for _ in 0..11 {
                t += 1_000;
                outputs.extend(step(&mut engine, &mut pcm, t).unwrap());
            }
        }
        for _ in 0..40 {
            t += 1_000;
            outputs.extend(step(&mut engine, &mut pcm, t).unwrap());
        }
        assert_eq!(outputs.len(), 6, "one output per tick");
        let stats = engine.stats();
        assert!(stats.silence_frames_written > stats.prefill_frames, "silence was written too");
        assert_eq!(stats.ticks_output, 6);
        assert!(engine.idle());
        let mut previous = 0;
        for p in &outputs {
            let d = &p.detail;
            assert!(d.hw_pos > d.n_k as i64, "strictly passed");
            assert_eq!(d.n_k_minus_hw_pos, d.n_k as i64 - d.hw_pos);
            assert_eq!(d.hw_pos, d.appl_pos as i64 - d.delay_frames);
            assert_eq!(d.hw_ptr_pos, d.appl_pos - d.queued_frames);
            assert_eq!(d.driver_delay_frames, 0);
            assert_eq!(d.offset_us, frames_to_us_round_half_away(d.n_k_minus_hw_pos, 48_000));
            assert_eq!(p.t_output_us as i64, d.htstamp_us as i64 + d.offset_us);
            assert!(d.frames == 528 || d.frames == 534);
            assert_eq!(d.frames, 6 * d.samples_8k);
            // ordering: release <= submit <= output <= callback
            assert!(p.submission.t_release_us <= p.submission.t_submit_us);
            assert!(p.submission.t_submit_us <= p.t_output_us, "{p:?}");
            assert!(p.t_output_us <= p.t_callback_us);
            assert!(d.t_write_us >= p.submission.t_submit_us);
            assert!(p.t_output_us >= previous, "outputs in placement order");
            previous = p.t_output_us;
        }
    }

    /// The driver delay is folded into `delay` and therefore into `hw_pos`,
    /// and it is recorded, not assumed to be zero.
    #[test]
    fn a_driver_delay_is_recorded_and_moves_hw_pos() {
        let t0 = 1_000_000;
        let mut sim = SimPcmConfig::new(PERIOD, BUFFER);
        sim.driver_delay_frames = 48;
        let (mut engine, mut pcm) = started(cfg(), sim, t0);
        engine.admit(submission(0, t0), Some(&payload(88, 0)));
        step(&mut engine, &mut pcm, t0).unwrap();
        let mut t = t0;
        let p = loop {
            t += 1_000;
            if let Some(p) = step(&mut engine, &mut pcm, t).unwrap().pop() {
                break p;
            }
        };
        assert_eq!(p.detail.driver_delay_frames, 48);
        assert_eq!(p.detail.hw_pos, p.detail.hw_ptr_pos as i64 - 48);
        // At the start the output position is before frame 0 — not an error.
        let (mut engine, mut pcm) = started(cfg(), {
            let mut sim = SimPcmConfig::new(PERIOD, BUFFER);
            sim.driver_delay_frames = 600;
            sim
        }, t0);
        let s = pcm.status(t0).unwrap();
        assert!(s.delay as u64 > engine.written_frames());
        engine.observe(&s, t0, t0).unwrap();
    }

    // ---- device failures ---------------------------------------------------

    /// An underrun (nothing written for longer than the buffer lasts) is an
    /// XRUN. No occurrence time is claimed (registration 6-1, Codex 109
    /// P1): the observation interval starts at the clock read before the
    /// last healthy status call, the kernel trigger is kept raw and apart.
    #[test]
    fn an_xrun_is_a_failure_without_an_occurrence_time() {
        let t0 = 1_000_000;
        let (mut engine, mut pcm) = started(cfg(), SimPcmConfig::new(PERIOD, BUFFER), t0);
        engine.admit(submission(0, t0), Some(&payload(88, 0)));
        step(&mut engine, &mut pcm, t0).unwrap();
        // Starved: 1040 frames last 21.67 ms; poll again only at +50 ms.
        let failure = step(&mut engine, &mut pcm, t0 + 50_000).unwrap_err();
        assert_eq!(failure.reason, FAIL_ALSA_XRUN);
        assert_eq!(failure.kernel_trigger_us, Some(t0 + 21_667), "raw, unverified, not an occurrence time");
        assert_eq!(failure.kernel_trigger_ns, Some((t0 + 21_667) * 1_000));
        assert_eq!((failure.last_good_status_t0_us, failure.detected_us), (t0, t0 + 50_000));
        let json = failure.to_json();
        assert_eq!(json["occurrence_time_determinable"], false);
        assert_eq!(json["observation_interval_us"], serde_json::json!([t0, t0 + 50_000]));
        assert!(json.get("occurred_us").is_none() && json.get("kernel_trigger_ns").is_none());
        assert_eq!(engine.abandon().len(), 1, "the written tick is abandoned, not output");
    }

    #[test]
    fn injected_xrun_and_suspend_keep_their_trigger_apart() {
        for (fault, reason) in [
            (SimFault::Xrun { after_start_us: 3_000 }, FAIL_ALSA_XRUN),
            (SimFault::Suspend { after_start_us: 3_000 }, FAIL_ALSA_SUSPENDED),
        ] {
            let t0 = 1_000_000;
            let mut sim = SimPcmConfig::new(PERIOD, BUFFER);
            sim.faults.push(fault);
            let (mut engine, mut pcm) = started(cfg(), sim, t0);
            step(&mut engine, &mut pcm, t0 + 1_000).unwrap();
            let failure = step(&mut engine, &mut pcm, t0 + 4_000).unwrap_err();
            assert_eq!((failure.reason, failure.kernel_trigger_us), (reason, Some(t0 + 3_000)));
            assert_eq!((failure.last_good_status_t0_us, failure.detected_us), (t0 + 1_000, t0 + 4_000));
        }
    }

    /// Codex 109 P1 regression 1: good snapshot 3,000 -> (receiver abort
    /// 4,000, ledger level) -> device error 4,500 -> detection 5,000. The
    /// engine claims no occurrence time; it keeps [3,000, 5,000] and the
    /// detection (the ledger test of the same sequence is in `l2_output`).
    #[test]
    fn codex109_good_3000_error_4500_detection_5000_claims_no_time() {
        let t_start = 1_000;
        let mut sim = SimPcmConfig::new(PERIOD, BUFFER);
        sim.faults.push(SimFault::StatusError { after_start_us: 3_500 }); // 4,500 absolute
        let (mut engine, mut pcm) = started(cfg(), sim, t_start);
        step(&mut engine, &mut pcm, 3_000).unwrap();
        let failure = step(&mut engine, &mut pcm, 5_000).unwrap_err();
        assert_eq!(failure.reason, FAIL_ALSA_STATUS_ERROR);
        assert_eq!((failure.last_good_status_t0_us, failure.detected_us), (3_000, 5_000));
        assert_eq!(failure.kernel_trigger_us, None, "no kernel time for an error return");
    }

    /// Codex 109 P1 regression 2: the snapshot is taken at 3,000 but the
    /// status call only returns at 5,000 (preempted); the device fails at
    /// 4,000 and is detected at 6,000. The interval must start at the
    /// clock read BEFORE the healthy call (3,000), never at its return.
    #[test]
    fn codex109_snapshot_3000_error_4000_return_5000_keeps_the_pre_call_read() {
        let t_start = 1_000;
        let (mut engine, mut pcm) = started(cfg(), SimPcmConfig::new(PERIOD, BUFFER), t_start);
        let healthy = pcm.status(3_000).unwrap();
        engine.observe(&healthy, 3_000, 5_000).unwrap();
        let error = PcmError::Other { errno: libc::EIO, call: "status" };
        let failure = engine.failure_from_error(&error, None, 6_000, "status");
        assert_eq!((failure.last_good_status_t0_us, failure.detected_us), (3_000, 6_000));
        assert!(failure.last_good_status_t0_us <= 4_000, "a true lower bound of the 4,000 failure");
    }

    /// A moved trigger (restart) or a DMA pointer that goes back is an epoch
    /// reset; a status error has no kernel time at all.
    #[test]
    fn epoch_resets_and_status_errors_keep_interval_detection_and_trigger_apart() {
        let t0 = 1_000_000;
        let mut sim = SimPcmConfig::new(PERIOD, BUFFER);
        sim.faults.push(SimFault::EpochReset { after_start_us: 2_500, rewind_frames: 0 });
        let (mut engine, mut pcm) = started(cfg(), sim, t0);
        step(&mut engine, &mut pcm, t0 + 2_000).unwrap();
        let failure = step(&mut engine, &mut pcm, t0 + 3_000).unwrap_err();
        assert_eq!((failure.reason, failure.kernel_trigger_us), (FAIL_ALSA_EPOCH_RESET, Some(t0 + 2_500)));
        assert_eq!(failure.last_good_status_t0_us, t0 + 2_000);
        // µs only in the detail; the raw ns stay apart (Codex 110).
        assert_eq!(failure.detail, format!("trigger moved: {} us != epoch trigger {} us", t0 + 2_500, t0));
        assert_eq!(failure.raw_ns, vec![("trigger_htstamp_ns", (t0 + 2_500) * 1_000), ("epoch_trigger_htstamp_ns", t0 * 1_000)]);

        // Pointer regression with the trigger unchanged (observed directly).
        let (mut engine, mut pcm) = started(cfg(), SimPcmConfig::new(PERIOD, BUFFER), t0);
        step(&mut engine, &mut pcm, t0 + 5_000).unwrap();
        let mut s = pcm.status(t0 + 6_000).unwrap();
        s.avail = BUFFER; // queued 0 -> DMA pointer = appl_pos, then...
        engine.observe(&s, t0 + 6_000, t0 + 6_000).unwrap();
        let mut back = pcm.status(t0 + 7_000).unwrap();
        back.avail = BUFFER - 100; // ...100 queued: the DMA pointer went back 100 frames
        let failure = engine.observe(&back, t0 + 7_000, t0 + 7_001).unwrap_err();
        assert_eq!(failure.reason, FAIL_ALSA_EPOCH_RESET);
        assert_eq!((failure.last_good_status_t0_us, failure.kernel_trigger_us), (t0 + 6_000, None));

        let mut sim = SimPcmConfig::new(PERIOD, BUFFER);
        sim.faults.push(SimFault::StatusError { after_start_us: 4_000 });
        let (mut engine, mut pcm) = started(cfg(), sim, t0);
        step(&mut engine, &mut pcm, t0 + 3_000).unwrap();
        let failure = step(&mut engine, &mut pcm, t0 + 5_000).unwrap_err();
        assert_eq!(failure.reason, FAIL_ALSA_STATUS_ERROR);
        assert_eq!((failure.last_good_status_t0_us, failure.detected_us), (t0 + 3_000, t0 + 5_000));
    }

    #[test]
    fn a_write_error_is_a_failure_and_abandons_everything_submitted() {
        let t0 = 1_000_000;
        let mut sim = SimPcmConfig::new(PERIOD, BUFFER);
        sim.faults.push(SimFault::WriteError { after_start_us: 1_500 });
        let (mut engine, mut pcm) = started(cfg(), sim, t0);
        engine.admit(submission(0, t0), Some(&payload(88, 0)));
        step(&mut engine, &mut pcm, t0 + 1_000).unwrap();
        engine.admit(submission(1, t0 + 1_800), Some(&payload(88, 0)));
        engine.admit(submission(2, t0 + 1_900), Some(&payload(89, 0)));
        let failure = step(&mut engine, &mut pcm, t0 + 2_000).unwrap_err();
        assert_eq!(failure.reason, FAIL_ALSA_WRITE_ERROR);
        assert_eq!(failure.last_good_status_t0_us, t0 + 2_000, "the healthy status of this iteration");
        let abandoned: Vec<u32> = engine.abandon().iter().map(|s| s.identity.seq).collect();
        assert_eq!(abandoned, vec![0, 1, 2], "written, in progress and queued");
        assert_eq!(engine.unwritten_tick_frames(), 0);
        assert!(engine.in_flight.is_empty());
    }

    // ---- playback end (Codex 109 P1-2) and Q (P2) --------------------------

    /// The snapshot with `hw_pos` = `target` on top of the simulated one
    /// (queued and delay consistent, no driver delay).
    fn snapshot_at(pcm: &mut SimPcm, engine: &HapticPlacement, t: u64, target: u64) -> PcmStatus {
        let mut s = pcm.status(t).unwrap();
        let queued = engine.written_frames() - target;
        s.delay = queued as i64;
        s.avail = BUFFER - queued;
        s
    }

    /// Codex 109 P1 regression: n_k 512, 528 frames, hw_pos 528 — the onset
    /// output is emitted but 512 frames are unplayed, so the engine is NOT
    /// idle; it is idle only once hw_pos >= n_k + frames (the last frame
    /// passed). The same for a 534-frame tick.
    #[test]
    fn codex109_the_engine_is_idle_only_after_the_last_tick_frame_passed() {
        for samples in [88usize, 89] {
            let frames = 6 * samples as u64;
            let t0 = 1_000_000;
            let (mut engine, mut pcm) = started(cfg(), SimPcmConfig::new(PERIOD, BUFFER), t0);
            engine.admit(submission(0, t0), Some(&payload(samples, 0)));
            step(&mut engine, &mut pcm, t0).unwrap();
            assert_eq!(engine.in_flight.front().map(|t| t.n_k), Some(512));
            let end = 512 + frames;
            assert_eq!(engine.stats().last_tick_end_frame, Some(end));
            let s = snapshot_at(&mut pcm, &engine, t0 + 1_000, 528);
            let placed = engine.observe(&s, t0 + 1_000, t0 + 1_000).unwrap();
            assert_eq!(placed.len(), 1, "the onset output");
            assert!(engine.in_flight.is_empty());
            assert!(!engine.idle(), "{samples}: onset output does not mean played");
            assert_eq!(engine.unplayed_tick_frames(), end - 528);
            let s = snapshot_at(&mut pcm, &engine, t0 + 2_000, end - 1);
            engine.observe(&s, t0 + 2_000, t0 + 2_000).unwrap();
            assert!(!engine.idle() && engine.unplayed_tick_frames() == 1, "{samples}: one frame left");
            let s = snapshot_at(&mut pcm, &engine, t0 + 3_000, end);
            engine.observe(&s, t0 + 3_000, t0 + 3_000).unwrap();
            assert!(engine.idle() && engine.tick_tail_played(), "{samples}: last frame passed");
            assert_eq!(engine.unplayed_tick_frames(), 0);
        }
    }

    /// Codex 109 P2 regression: Q = 534 with a short write in progress
    /// (64 of 534 written, 470 unwritten) must refuse another 534-frame tick
    /// (1,004 unwritten), and accept again once the remainder is written.
    #[test]
    fn codex109_q_counts_the_unwritten_remainder_of_the_tick_being_written() {
        let t0 = 1_000_000;
        let c = PlacementConfig { queue_limit_frames: 534, ..cfg() };
        let mut sim = SimPcmConfig::new(PERIOD, BUFFER);
        sim.max_frames_per_write = Some(64);
        let (mut engine, mut pcm) = started(c, sim, t0);
        assert_eq!(engine.admit(submission(0, t0), Some(&payload(89, 0))), Admission::Queued);
        step(&mut engine, &mut pcm, t0).unwrap();
        assert_eq!(engine.unwritten_tick_frames(), 534 - 64, "short write in progress");
        assert_eq!(engine.admit(submission(1, t0), Some(&payload(89, 0))), Admission::QueueFull);
        let mut t = t0;
        while engine.unwritten_tick_frames() > 0 {
            t += 1_000;
            step(&mut engine, &mut pcm, t).unwrap();
        }
        assert_eq!(engine.admit(submission(2, t), Some(&payload(89, 0))), Admission::Queued);
        assert!(engine.stats().max_unwritten_tick_frames <= 534);
    }

    /// Conservation inside the engine: admitted = written + pending, and
    /// every written tick is output exactly once over a long simulated run.
    #[test]
    fn a_long_simulated_run_conserves_ticks() {
        let t0 = 1_000_000;
        let (mut engine, mut pcm) = started(cfg(), SimPcmConfig::new(PERIOD, BUFFER), t0);
        let mut outputs = 0u64;
        let mut t = t0;
        let mut admitted = 0u64;
        for k in 0..900u64 {
            // 90 Hz ticks with jittered, sometimes bursty arrival.
            let due = t0 + k * 11_111;
            while t < due {
                t += 1_000;
                outputs += step(&mut engine, &mut pcm, t).unwrap().len() as u64;
            }
            let samples = if k % 9 == 0 { 88 } else { 89 };
            if engine.admit(submission(k as u32, t), Some(&payload(samples, 0))) == Admission::Queued {
                admitted += 1;
            }
        }
        for _ in 0..100 {
            t += 1_000;
            outputs += step(&mut engine, &mut pcm, t).unwrap().len() as u64;
        }
        assert_eq!(admitted, 900);
        assert_eq!(outputs, 900);
        assert_eq!(engine.stats().ticks_written, 900);
        assert!(engine.idle());
        let stats = engine.stats();
        assert_eq!(stats.frames_written, stats.tick_frames_written + stats.silence_frames_written);
    }

    /// Codex 110: the diagnostics domain text follows the ACTUAL type, per
    /// the local UAPI enum comments (`/usr/include/sound/asound.h`).
    #[test]
    fn the_audio_timestamp_domain_follows_the_actual_type() {
        let texts: Vec<&str> = (0..=6).map(audio_tstamp_domain).collect();
        let mut unique = texts.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), 7, "one text per type");
        assert!(texts[AUDIO_TSTAMP_TYPE_COMPAT as usize].contains("HDAudio playback"));
        assert!(texts[AUDIO_TSTAMP_TYPE_DEFAULT as usize].contains("DMA time"));
        assert!(texts[AUDIO_TSTAMP_TYPE_LINK as usize].contains("reset on startup"));
        assert!(texts[AUDIO_TSTAMP_TYPE_LINK_ABSOLUTE as usize].contains("not reset on startup"));
        assert!(texts[AUDIO_TSTAMP_TYPE_LINK_ESTIMATED as usize].contains("estimated indirectly"));
        assert!(texts[AUDIO_TSTAMP_TYPE_LINK_SYNCHRONIZED as usize].contains("synchronized with system time"));
        assert!(texts[6].starts_with("UNKNOWN"));
    }

    // ---- latency bound B (sink mode A, experiment 3) -------------------------

    /// Two regular ticks: the bound the experiment-3 tests use.
    const B2: u64 = 2 * REGULAR_TICK_MAX_FRAMES;

    fn bounded(bound: Option<u64>) -> PlacementConfig {
        PlacementConfig { latency_bound_frames: bound, ..cfg() }
    }

    /// Seqs of the waiting ticks, front (oldest) first.
    fn pending_seqs(engine: &HapticPlacement) -> Vec<u32> {
        engine.pending.iter().map(|t| t.submission.identity.seq).collect()
    }

    /// The tick being written and the written ticks awaiting their output.
    fn placed_seqs(engine: &HapticPlacement) -> Vec<u32> {
        let mut out: Vec<u32> = engine.in_flight.iter().map(|t| t.submission.identity.seq).collect();
        if let Some(Current { kind: ChunkKind::Tick { submission, .. }, .. }) = &engine.current {
            out.push(submission.identity.seq);
        }
        out
    }

    #[test]
    fn the_latency_bound_must_lie_between_one_regular_tick_and_q() {
        let q = PlacementConfig { queue_limit_frames: B2, ..cfg() };
        for bound in [REGULAR_TICK_MAX_FRAMES, B2] {
            PlacementConfig { latency_bound_frames: Some(bound), ..q }.validate().unwrap();
        }
        for bound in [0, REGULAR_TICK_MAX_FRAMES - 1, B2 + 1] {
            let err = PlacementConfig { latency_bound_frames: Some(bound), ..q }.validate().unwrap_err();
            assert!(err.contains("latency bound"), "{bound}: {err}");
        }
        bounded(None).validate().unwrap();
        bounded(Some(48_000)).validate().unwrap();
        assert!(bounded(Some(48_001)).validate().is_err(), "above the default Q");
    }

    /// The outcome of one simulated offering schedule, by OFFER INDEX (never
    /// by identity): which earlier offers each offer dropped, every output
    /// with its lag `t_output_us − t_submit_us`, and the engine's counters.
    struct Run {
        dropped_by: Vec<(usize, Vec<usize>)>,
        outputs: Vec<(usize, u64)>,
        queue_full: Vec<usize>,
        stats: LaneStats,
    }

    /// Offer `n` regular ticks every `gap_us` of virtual time (1 ms lane
    /// steps), each built by `make(index, t)`, then drain until idle. After
    /// every offer it checks the mode-A rules: the drops are exactly the
    /// oldest waiting ticks, never a placed (being written / written) tick,
    /// and with a bound `unwritten <= B` unless nothing else waits.
    fn offer_schedule(c: PlacementConfig, n: usize, gap_us: u64, make: impl Fn(usize, u64) -> L2Submission) -> Run {
        let t0 = 1_000_000;
        let (mut engine, mut pcm) = started(c, SimPcmConfig::new(PERIOD, BUFFER), t0);
        let mut offered: Vec<L2Submission> = Vec::new();
        let index_of = |offered: &[L2Submission], s: &L2Submission| offered.iter().position(|o| o == s).expect("offered");
        let mut run = Run { dropped_by: Vec::new(), outputs: Vec::new(), queue_full: Vec::new(), stats: LaneStats::default() };
        let mut t = t0;
        let collect = |placed: Vec<Placed>, offered: &[L2Submission], run: &mut Run| {
            for p in placed {
                run.outputs.push((index_of(offered, &p.submission), p.t_output_us - p.submission.t_submit_us));
            }
        };
        for k in 0..n {
            let due = t0 + k as u64 * gap_us;
            while t + 1_000 <= due {
                t += 1_000;
                let placed = step(&mut engine, &mut pcm, t).unwrap();
                collect(placed, &offered, &mut run);
            }
            let s = make(k, t);
            offered.push(s);
            let before = pending_seqs(&engine);
            let placed_before = placed_seqs(&engine);
            let samples = if k % 9 == 0 { 88 } else { 89 };
            let Offered { latency_dropped, admission } = engine.offer(s, Some(&payload(samples, 0)));
            let dropped: Vec<u32> = latency_dropped.iter().map(|d| d.identity.seq).collect();
            assert_eq!(dropped[..], before[..dropped.len()], "offer {k}: the oldest waiting ticks, in order");
            assert!(dropped.iter().all(|seq| !placed_before.contains(seq)), "offer {k}: a placed tick was dropped");
            assert_eq!(placed_seqs(&engine), placed_before, "offer {k}: placed ticks untouched");
            match admission {
                Admission::Queued => {}
                Admission::QueueFull => run.queue_full.push(k),
                Admission::PayloadInvalid => panic!("offer {k}: a regular tick is playable"),
            }
            if let Some(bound) = c.latency_bound_frames {
                assert!(
                    engine.unwritten_tick_frames() <= bound || engine.pending.len() <= 1,
                    "offer {k}: {} unwritten > B {bound} with other ticks still waiting",
                    engine.unwritten_tick_frames()
                );
            } else {
                assert!(dropped.is_empty(), "no bound, no drop");
            }
            if !latency_dropped.is_empty() {
                run.dropped_by.push((k, latency_dropped.iter().map(|d| index_of(&offered, d)).collect()));
            }
        }
        for _ in 0..20_000 {
            if engine.idle() {
                break;
            }
            t += 1_000;
            let placed = step(&mut engine, &mut pcm, t).unwrap();
            collect(placed, &offered, &mut run);
        }
        assert!(engine.idle(), "drained");
        run.stats = engine.stats().clone();
        run
    }

    fn regular(k: usize, t: u64) -> L2Submission {
        submission(k as u32, t)
    }

    /// Compression-like burst (two ticks per 90 Hz slot, twice real time):
    /// without B the write queue grows and so does output − submit (the
    /// experiment-2 sink); with B = 2 ticks the oldest waiting ticks are
    /// dropped, every tick ends exactly once (output or drop), and the lag
    /// stays within (buffer + B) frames.
    #[test]
    fn a_compression_burst_drops_the_oldest_waiting_ticks_and_bounds_the_lag() {
        // 150 ticks at twice real time: the unbounded backlog (~40,000
        // frames) stays under Q = 48,000, so no tick is refused before submit.
        let n = 150;
        let gap = 11_111 / 2;
        let unbounded = offer_schedule(bounded(None), n, gap, regular);
        assert!(unbounded.dropped_by.is_empty() && unbounded.queue_full.is_empty());
        assert_eq!(unbounded.outputs.len(), n, "every tick is output without a bound");
        let first = unbounded.outputs[..20].iter().map(|o| o.1).max().unwrap();
        let last = unbounded.outputs[n - 20..].iter().map(|o| o.1).min().unwrap();
        assert!(last > 500_000 && last > first + 400_000, "the FIFO lag grows: {first} -> {last} us");
        assert!(unbounded.stats.latency_bound.is_none());

        let bound = offer_schedule(bounded(Some(B2)), n, gap, regular);
        assert!(bound.queue_full.is_empty());
        let dropped: Vec<usize> = bound.dropped_by.iter().flat_map(|(_, d)| d.iter().copied()).collect();
        let output: Vec<usize> = bound.outputs.iter().map(|o| o.0).collect();
        assert!(dropped.len() > n / 4, "a 2x burst must drop ticks: {}", dropped.len());
        let mut all: Vec<usize> = dropped.iter().chain(output.iter()).copied().collect();
        all.sort_unstable();
        assert_eq!(all, (0..n).collect::<Vec<_>>(), "exactly one end per offered tick");
        assert!(output.windows(2).all(|w| w[0] < w[1]), "outputs stay in release order");
        for (k, d) in &bound.dropped_by {
            assert!(d.iter().all(|i| i < k), "offer {k} dropped only earlier offers {d:?}");
        }
        let max_lag = bound.outputs.iter().map(|o| o.1).max().unwrap();
        let ceiling = ((BUFFER + B2) * 1_000_000 / PCM_RATE_HZ) + 2_000;
        assert!(max_lag <= ceiling, "lag {max_lag} us above the (buffer + B) ceiling {ceiling} us");
        eprintln!(
            "2x burst of {n}: unbounded lag {first} -> {last} us; B = {B2}: {} dropped, max lag {max_lag} us (ceiling {ceiling})",
            dropped.len()
        );
        let counters = bound.stats.latency_bound.expect("bound counters");
        assert_eq!(counters.ticks_dropped as usize, dropped.len());
        let frames: u64 = dropped.iter().map(|&k| if k % 9 == 0 { 528 } else { 534 }).sum();
        assert_eq!(counters.frames_dropped, frames);
        assert_eq!(bound.stats.ticks_written as usize, output.len());
        assert_eq!(bound.stats.ticks_admitted as usize, n, "a dropped tick was admitted first");
        assert!(bound.stats.max_unwritten_tick_frames <= B2, "two regular ticks never exceed B = 2 ticks");
    }

    /// Regular 90 Hz offering (no compression) never reaches B = 1,068.
    #[test]
    fn regular_90_hz_offering_with_b_1068_drops_nothing() {
        let run = offer_schedule(bounded(Some(B2)), 900, 11_111, regular);
        assert!(run.dropped_by.is_empty(), "dropped {:?}", run.dropped_by);
        assert_eq!(run.outputs.len(), 900);
        assert_eq!(run.stats.latency_bound, Some(LatencyBoundStats::default()));
        let baseline = offer_schedule(bounded(None), 900, 11_111, regular);
        assert_eq!(run.outputs, baseline.outputs, "same outputs, same lags as without the bound");
    }

    /// The drop is blind to identity: other seqs, pts and event ids (every
    /// tick an anchor, or none) give the same drop and output positions.
    #[test]
    fn latency_drops_depend_only_on_queue_position_not_on_identity() {
        let n = 120;
        let gap = 11_111 / 3;
        let reference = offer_schedule(bounded(Some(B2)), n, gap, regular);
        assert!(!reference.dropped_by.is_empty());
        let events: [fn(usize) -> u32; 3] = [|k| k as u32 + 1, |_| 0, |k| if k % 3 == 1 { 9 } else { 0 }];
        for event in events {
            let relabelled = move |k: usize, t: u64| {
                let mut s = submission(1_000 + k as u32, t);
                s.identity.pts_us = 777 + k as u64 * 3_333;
                s.identity.event_id = event(k);
                s
            };
            let other = offer_schedule(bounded(Some(B2)), n, gap, relabelled);
            assert_eq!(other.dropped_by, reference.dropped_by);
            assert_eq!(other.outputs, reference.outputs);
        }
    }

    /// The tick being written (partly written, n_k fixed) is never dropped:
    /// with B = one tick the offered tick is queued behind it anyway, and a
    /// later offer drops that waiting tick, not the one in progress.
    #[test]
    fn the_tick_in_progress_is_never_dropped_for_the_bound() {
        let t0 = 1_000_000;
        let mut sim = SimPcmConfig::new(PERIOD, BUFFER);
        sim.max_frames_per_write = Some(64);
        let (mut engine, mut pcm) = started(bounded(Some(REGULAR_TICK_MAX_FRAMES)), sim, t0);
        assert_eq!(engine.admit(submission(0, t0), Some(&payload(89, 0))), Admission::Queued);
        step(&mut engine, &mut pcm, t0).unwrap();
        assert_eq!(engine.unwritten_tick_frames(), 534 - 64, "tick 0 partly written");
        let one = engine.offer(submission(1, t0), Some(&payload(89, 0)));
        assert_eq!(one, Offered { latency_dropped: Vec::new(), admission: Admission::Queued });
        assert_eq!(engine.unwritten_tick_frames(), 470 + 534, "queued above B: nothing else waits");
        let two = engine.offer(submission(2, t0), Some(&payload(89, 0)));
        assert_eq!(two.latency_dropped, vec![submission(1, t0)], "the waiting tick, not the tick in progress");
        assert_eq!(two.admission, Admission::Queued);
        assert_eq!(placed_seqs(&engine), vec![0]);
        assert_eq!(pending_seqs(&engine), vec![2]);
        // An unplayable offer drops nothing.
        let bad = engine.offer(submission(3, t0), None);
        assert_eq!(bad, Offered { latency_dropped: Vec::new(), admission: Admission::PayloadInvalid });
        assert_eq!(pending_seqs(&engine), vec![2]);
        let mut t = t0;
        let mut out = Vec::new();
        while !engine.idle() {
            t += 1_000;
            out.extend(step(&mut engine, &mut pcm, t).unwrap().into_iter().map(|p| p.submission.identity.seq));
        }
        assert_eq!(out, vec![0, 2]);
        let counters = engine.stats().latency_bound.unwrap();
        assert_eq!((counters.ticks_dropped, counters.frames_dropped), (1, 534));
    }

    /// The Q rule applies after the drops exactly as without B: when the
    /// in-progress remainder plus the offered tick exceeds Q, the offered
    /// tick is refused even though a waiting tick was just dropped (both
    /// get their terminal). Only possible with Q < remainder + tick.
    #[test]
    fn q_still_decides_the_offered_tick_after_the_drops() {
        let t0 = 1_000_000;
        let mut sim = SimPcmConfig::new(PERIOD, BUFFER);
        sim.max_frames_per_write = Some(64);
        let c = PlacementConfig { queue_limit_frames: 600, latency_bound_frames: Some(600), ..cfg() };
        let (mut engine, mut pcm) = started(c, sim, t0);
        assert_eq!(engine.admit(submission(0, t0), Some(&payload(89, 0))), Admission::Queued);
        step(&mut engine, &mut pcm, t0).unwrap();
        assert_eq!(engine.unwritten_tick_frames(), 470);
        assert_eq!(engine.admit(submission(1, t0), Some(&payload(10, 0))), Admission::Queued, "60 frames fit");
        let offered = engine.offer(submission(2, t0), Some(&payload(89, 0)));
        assert_eq!(offered.latency_dropped, vec![submission(1, t0)]);
        assert_eq!(offered.admission, Admission::QueueFull, "470 + 534 > Q = 600");
        assert_eq!(engine.stats().ticks_queue_full, 1);
        assert_eq!(pending_seqs(&engine), Vec::<u32>::new());
    }

    /// Codex 190a P2-2: B bounds only WAITING ticks, so the numerical bound
    /// is `min(Q, max(B, r + f))` after each admission (module doc), not
    /// `B`: a playable 200-sample (1,200-frame) tick is accepted exactly as
    /// without B (acceptance unchanged) and queued above B = 1,068 when
    /// nothing else waits, and a regular tick offered behind its unwritten
    /// remainder lands at `r + f`.
    #[test]
    fn an_oversized_playable_tick_exceeds_b_only_as_the_documented_bound_allows() {
        let t0 = 1_000_000;
        for bound in [None, Some(B2)] {
            let mut sim = SimPcmConfig::new(PERIOD, BUFFER);
            sim.max_frames_per_write = Some(64);
            let (mut engine, mut pcm) = started(bounded(bound), sim, t0);
            assert!(1_200 <= engine.config().max_tick_frames(), "playable");
            let big = engine.offer(submission(0, t0), Some(&payload(200, 0)));
            assert_eq!(big, Offered { latency_dropped: Vec::new(), admission: Admission::Queued }, "{bound:?}");
            assert_eq!(engine.unwritten_tick_frames(), 1_200, "r = 0, f = 1,200: max(B, 1,200)");
            step(&mut engine, &mut pcm, t0).unwrap();
            let r = engine.unwritten_tick_frames();
            assert_eq!(r, 1_200 - 64, "the oversized tick is being written");
            let second = engine.offer(submission(1, t0), Some(&payload(89, 0)));
            assert_eq!(second, Offered { latency_dropped: Vec::new(), admission: Admission::Queued });
            assert_eq!(engine.unwritten_tick_frames(), r + 534, "nothing waits: r + f");
            let third = engine.offer(submission(2, t0), Some(&payload(89, 0)));
            assert_eq!(third.admission, Admission::Queued);
            let max_unwritten = engine.stats().max_unwritten_tick_frames;
            match bound {
                None => {
                    assert!(third.latency_dropped.is_empty());
                    assert_eq!(engine.unwritten_tick_frames(), r + 2 * 534);
                    assert_eq!(max_unwritten, r + 2 * 534);
                }
                Some(b) => {
                    assert_eq!(third.latency_dropped, vec![submission(1, t0)], "the waiting tick, never the one in progress");
                    assert_eq!(engine.unwritten_tick_frames(), r + 534);
                    assert_eq!(max_unwritten, r + 534);
                    assert!(max_unwritten > b, "the documented exception: above B");
                    assert!(max_unwritten <= b.max(r + 534).min(engine.config().queue_limit_frames));
                    assert!(max_unwritten <= b.max(2 * 1_200), "max(B, 2 x the largest admitted tick)");
                }
            }
        }
    }

    /// Without B the counters JSON is exactly the former key set; with B it
    /// gains the two latency-bound keys.
    #[test]
    fn latency_bound_counters_appear_only_with_a_bound() {
        let (off, _) = started(bounded(None), SimPcmConfig::new(PERIOD, BUFFER), 1_000_000);
        let json = off.stats().to_json();
        assert!(json.get("ticks_latency_bound").is_none() && json.get("latency_bound_frames_dropped").is_none());
        let (on, _) = started(bounded(Some(B2)), SimPcmConfig::new(PERIOD, BUFFER), 1_000_000);
        let json = on.stats().to_json();
        assert_eq!(json["ticks_latency_bound"], 0);
        assert_eq!(json["latency_bound_frames_dropped"], 0);
        let mut keys_on: Vec<String> = json.as_object().unwrap().keys().cloned().collect();
        let mut keys_off: Vec<String> = off.stats().to_json().as_object().unwrap().keys().cloned().collect();
        keys_on.sort();
        keys_off.extend(["latency_bound_frames_dropped".to_string(), "ticks_latency_bound".to_string()]);
        keys_off.sort();
        assert_eq!(keys_on, keys_off);
    }
    // ---- aligned placement R2 (experiment 4, design §5) ----------------------
    //
    // Common start (`started` at t0 = 1,000,000): prefill 512, first
    // snapshot hw_pos = 0, htstamp_us = t0, w = appl_pos = 512, empty plan.
    // At 1 ms steps the simulated stream has consumed exactly 48 frames per
    // ms (a multiple of the 16-frame granularity), so hw_pos is exact and
    // frame n plays at t0 + n / 48 kHz.

    const W: u64 = 512;
    const T0: u64 = 1_000_000;

    fn aligned_cfg(c: i64) -> PlacementConfig {
        PlacementConfig { mode: PlacementMode::Aligned { align_offset_us: c, write_ahead_frames: W }, ..cfg() }
    }

    fn released(seq: u32, t_release_us: u64, t_submit_us: u64) -> L2Submission {
        let mut s = submission(seq, t_submit_us);
        s.t_release_us = t_release_us;
        s
    }

    /// Distinct, never-zero content per tick (so device frames identify it).
    fn tick_payload(seq: u32, samples: usize) -> Vec<u8> {
        (0..samples).flat_map(|i| (1 + ((seq as i64 * 977 + i as i64) % 30_000) as i16).to_le_bytes()).collect()
    }

    /// The tick's left-channel 48 kHz frames (×6 repetition).
    fn tick_frames(seq: u32, samples: usize) -> Vec<i16> {
        let bytes = tick_payload(seq, samples);
        bytes.chunks_exact(2).flat_map(|b| std::iter::repeat_n(i16::from_le_bytes([b[0], b[1]]), 6)).collect()
    }

    fn place(engine: &mut HapticPlacement, s: L2Submission, samples: usize) -> AlignedOffered {
        engine.place(s, Some(&tick_payload(s.identity.seq, samples)))
    }

    /// Step at 1 ms until idle, collecting outputs.
    fn drain_until_idle(engine: &mut HapticPlacement, pcm: &mut dyn HapticPcm, t: &mut u64) -> Vec<Placed> {
        let mut out = Vec::new();
        for _ in 0..10_000 {
            if engine.idle() {
                break;
            }
            *t += 1_000;
            out.extend(step_dyn(engine, pcm, *t).unwrap());
        }
        assert!(engine.idle(), "drained");
        out
    }

    fn step_dyn(engine: &mut HapticPlacement, pcm: &mut dyn HapticPcm, t: u64) -> Result<Vec<Placed>, DeviceFailure> {
        let s = pcm.status(t).map_err(|e| engine.failure_from_error(&e, None, t, "status"))?;
        let placed = engine.observe(&s, t, t)?;
        let mut clock = || t;
        engine.fill(pcm, s.avail, &mut clock).map_err(|e| engine.failure_from_error(&e, None, t, "writei"))?;
        Ok(placed)
    }

    /// A simulated stream that also records every frame written (left channel).
    struct Recording {
        sim: SimPcm,
        frames: Vec<i16>,
    }

    impl HapticPcm for Recording {
        fn params(&self) -> &PcmParams {
            self.sim.params()
        }
        fn writei(&mut self, pcm: &[i16], now_us: u64) -> Result<u64, PcmError> {
            let n = self.sim.writei(pcm, now_us)?;
            self.frames.extend(pcm[..n as usize * PCM_CHANNELS].iter().step_by(PCM_CHANNELS));
            Ok(n)
        }
        fn start(&mut self, now_us: u64) -> Result<(), PcmError> {
            self.sim.start(now_us)
        }
        fn status(&mut self, now_us: u64) -> Result<PcmStatus, PcmError> {
            self.sim.status(now_us)
        }
        fn stop(&mut self) -> Result<(), PcmError> {
            self.sim.stop()
        }
    }

    fn started_recording(c: PlacementConfig, sim: SimPcmConfig) -> (HapticPlacement, Recording) {
        let mut pcm = Recording { sim: SimPcm::new(sim), frames: Vec::new() };
        let mut engine = HapticPlacement::new(c, pcm.params()).unwrap();
        let mut clock = || T0;
        engine.prefill(&mut pcm, &mut clock).unwrap();
        pcm.start(T0).unwrap();
        let s = pcm.status(T0).unwrap();
        engine.begin_epoch(T0, s, T0).unwrap();
        (engine, pcm)
    }

    /// The device frames of an output equal the tick's frames after its head cut.
    fn assert_output_content(pcm: &Recording, p: &Placed, samples: usize) {
        let d = &p.detail;
        let head = d.head_cut_frames.expect("aligned output") as usize;
        let want = &tick_frames(p.submission.identity.seq, samples)[head..head + d.frames as usize];
        let got = &pcm.frames[d.n_k as usize..(d.n_k + d.frames) as usize];
        assert_eq!(got, want, "seq {}: the frames at n_k are the tick's", p.submission.identity.seq);
    }

    #[test]
    fn the_placement_mode_refuses_a_latency_bound_and_bad_aligned_parameters() {
        for mode in [PlacementMode::Fifo, PlacementMode::Aligned { align_offset_us: 40_000, write_ahead_frames: W }] {
            PlacementConfig { mode, ..cfg() }.validate().unwrap();
            let err = PlacementConfig { mode, latency_bound_frames: Some(1_068), ..cfg() }.validate().unwrap_err();
            assert!(err.contains("no latency bound"), "{mode:?}: {err}");
        }
        PlacementConfig { latency_bound_frames: Some(1_068), ..cfg() }.validate().unwrap();
        for (c, w) in [(-1, W), (40_000, 0), (40_000, BUFFER - PERIOD + 1), (40_000, u64::MAX)] {
            let bad = PlacementConfig { mode: PlacementMode::Aligned { align_offset_us: c, write_ahead_frames: w }, ..cfg() };
            assert!(bad.validate().is_err(), "{bad:?}");
        }
        aligned_cfg(0).validate().unwrap();
        PlacementConfig { mode: PlacementMode::Aligned { align_offset_us: 0, write_ahead_frames: BUFFER - PERIOD }, ..cfg() }
            .validate()
            .unwrap();
        assert_eq!(PlacementMode::None.as_str(), None);
        assert_eq!(PlacementMode::Fifo.as_str(), Some("fifo"));
        assert_eq!(aligned_cfg(1).mode.as_str(), Some("aligned"));
    }

    /// Fifo = the legacy engine path with B off: the same outputs and counters.
    #[test]
    fn the_fifo_mode_engine_equals_the_legacy_engine() {
        let run = |c: PlacementConfig| {
            let (mut engine, mut pcm) = started(c, SimPcmConfig::new(PERIOD, BUFFER), T0);
            let mut t = T0;
            let mut outputs = Vec::new();
            let mut admissions = Vec::new();
            for k in 0..300u32 {
                let due = T0 + k as u64 * 7_000;
                while t + 1_000 <= due {
                    t += 1_000;
                    outputs.extend(step(&mut engine, &mut pcm, t).unwrap().into_iter().map(|p| (p.t_output_us, p.detail.to_json())));
                }
                let offered = engine.offer(submission(k, t), Some(&payload(88 + (k % 9 != 0) as usize, 1)));
                assert!(offered.latency_dropped.is_empty());
                admissions.push(offered.admission);
            }
            for _ in 0..4_000 {
                t += 1_000;
                outputs.extend(step(&mut engine, &mut pcm, t).unwrap().into_iter().map(|p| (p.t_output_us, p.detail.to_json())));
            }
            assert!(engine.idle());
            (outputs, admissions, engine.stats().to_json())
        };
        let legacy = run(cfg());
        let fifo = run(PlacementConfig { mode: PlacementMode::Fifo, ..cfg() });
        let queued = legacy.1.iter().filter(|a| **a == Admission::Queued).count();
        assert!(queued > 200 && queued < 300, "a compressed schedule also reaches Q: {queued}");
        assert_eq!(legacy.0.len(), queued);
        assert!(legacy.0.iter().all(|(_, d)| d.get("head_cut_frames").is_none()), "no aligned key in FIFO rows");
        assert_eq!(legacy, fifo);
    }

    #[test]
    #[should_panic(expected = "offers through place()")]
    fn the_aligned_engine_refuses_the_fifo_offer() {
        let (mut engine, _pcm) = started(aligned_cfg(40_000), SimPcmConfig::new(PERIOD, BUFFER), T0);
        let _ = engine.offer(submission(0, T0), Some(&payload(89, 1)));
    }

    #[test]
    #[should_panic(expected = "healthy snapshot")]
    fn an_aligned_placement_without_a_healthy_snapshot_is_a_programming_error() {
        let mut pcm = SimPcm::new(SimPcmConfig::new(PERIOD, BUFFER));
        let mut engine = HapticPlacement::new(aligned_cfg(40_000), pcm.params()).unwrap();
        let mut clock = || T0;
        engine.prefill(&mut pcm, &mut clock).unwrap();
        let _ = place(&mut engine, submission(0, T0), 89);
    }

    /// Case (i) gap: n* = 0 + round(40,000 µs × 48 kHz) = 1,920 >= planned_end
    /// 512 -> 1,408 frames of planned silence, then the tick; floor = t0 +
    /// round(512 × 1e6 / 48,000) − t0 = 10,667 µs. A second tick released
    /// 20 ms later: n* = 2,880 >= 2,454 -> 426 frames of silence. Both are
    /// output at exactly t0 + 40 ms and t0 + 60 ms.
    #[test]
    fn aligned_case_i_gap_inserts_planned_silence_up_to_n_star() {
        let (mut engine, mut pcm) = started_recording(aligned_cfg(40_000), SimPcmConfig::new(PERIOD, BUFFER));
        let first = place(&mut engine, released(0, T0, T0), 89);
        assert_eq!(first.admission, AlignedAdmission::Placed);
        assert!(first.trims.is_empty() && first.occluded.is_empty());
        let r = first.placement.unwrap();
        assert_eq!(
            (r.case, r.status_htstamp_us, r.hw_pos, r.appl_pos, r.planned_end_before, r.n_star),
            (PlacementCase::Gap, T0, 0, 512, 512, 1_920)
        );
        assert_eq!((r.floor_us, r.head_cut_frames, r.silence_inserted_frames, r.planned_end_after, r.rejected), (10_667, 0, 1_408, 2_454, None));
        let second = place(&mut engine, released(1, T0 + 20_000, T0 + 20_000), 89).placement.unwrap();
        assert_eq!((second.case, second.n_star, second.silence_inserted_frames, second.planned_end_after), (PlacementCase::Gap, 2_880, 426, 3_414));
        assert_eq!(second.floor_us, 10_667 - 20_000, "floor is signed");
        assert_eq!(engine.planned_end(), Some(3_414));
        assert_eq!(engine.waiting_silence_frames(), Some(1_408 + 426));
        let mut t = T0;
        let outputs = drain_until_idle(&mut engine, &mut pcm, &mut t);
        let got: Vec<(u32, u64, u64, u64, Option<u64>)> = outputs
            .iter()
            .map(|p| (p.submission.identity.seq, p.detail.n_k, p.detail.frames, p.t_output_us, p.detail.head_cut_frames))
            .collect();
        assert_eq!(got, vec![(0, 1_920, 534, T0 + 40_000, Some(0)), (1, 2_880, 534, T0 + 60_000, Some(0))]);
        for p in &outputs {
            assert_output_content(&pcm, p, 89);
            assert!(p.detail.hw_pos > p.detail.n_k as i64);
        }
        assert!(pcm.frames[512..1_920].iter().all(|f| *f == 0), "the gap is silence");
        let a = engine.stats().aligned.unwrap();
        assert_eq!((a.frames_silence_inserted, a.ticks_head_cut, a.ticks_occluded, a.frames_tail_cut), (1_834, 0, 0, 0));
        assert_eq!(a.max_waiting_silence_frames, 1_834);
        assert_eq!(engine.stats().last_tick_end_frame, Some(3_414));
    }

    /// Case (i) over Q: Q = 2,000; the first tick's candidate waits 1,942
    /// frames (accepted); a tick released 12 ms later has n* = 2,496 >= 2,454
    /// and would wait 2,518 > Q: rejected with the plan and planned_end
    /// unchanged; its placement row is still made (floor −1,333 µs).
    #[test]
    fn aligned_case_i_over_q_is_refused_and_leaves_the_plan_unchanged() {
        let c = PlacementConfig { queue_limit_frames: 2_000, ..aligned_cfg(40_000) };
        let (mut engine, _pcm) = started(c, SimPcmConfig::new(PERIOD, BUFFER), T0);
        assert_eq!(place(&mut engine, released(0, T0, T0), 89).admission, AlignedAdmission::Placed);
        let before = (engine.planned_end(), engine.waiting_silence_frames(), engine.pending_frames());
        assert_eq!(before, (Some(2_454), Some(1_408), 534));
        let refused = place(&mut engine, released(1, T0 + 12_000, T0 + 12_000), 89);
        assert_eq!(refused.admission, AlignedAdmission::PlacementQueueFull);
        let r = refused.placement.unwrap();
        assert_eq!((r.case, r.n_star, r.planned_end_before, r.planned_end_after), (PlacementCase::Gap, 2_496, 2_454, 2_454));
        assert_eq!((r.silence_inserted_frames, r.head_cut_frames, r.floor_us), (0, 0, -1_333));
        assert_eq!(r.rejected, Some(DISCARD_PLACEMENT_QUEUE_FULL));
        assert_eq!((engine.planned_end(), engine.waiting_silence_frames(), engine.pending_frames()), before);
        let a = engine.stats().aligned.unwrap();
        assert_eq!((a.ticks_placement_queue_full, a.frames_silence_inserted), (1, 1_408));
        assert_eq!(engine.stats().ticks_admitted, 1);
    }

    /// Case (ii) on WAITING ticks: tick 0 at 1,920 (after 1,408 silence);
    /// tick 1 released 10 ms before t0 -> n* = 1,440 in [512, 2,454): the
    /// silence from 1,440 and the whole tick 0 are removed (trim: tail 534,
    /// start_removed -> occluded), tick 1 goes at 1,440. Tick 2 released
    /// 4 ms before t0 -> n* = 1,728 cuts tick 1's tail at 1,728 (trim: tail
    /// 246, start kept, frames 534 -> 288).
    #[test]
    fn aligned_case_ii_trims_waiting_ticks_and_occludes_a_removed_start() {
        let (mut engine, mut pcm) = started_recording(aligned_cfg(40_000), SimPcmConfig::new(PERIOD, BUFFER));
        let s0 = released(0, T0, T0);
        let s1 = released(1, T0 - 10_000, T0);
        let s2 = released(2, T0 - 4_000, T0);
        assert_eq!(place(&mut engine, s0, 89).admission, AlignedAdmission::Placed);
        let one = place(&mut engine, s1, 89);
        assert_eq!(one.admission, AlignedAdmission::Placed);
        let r = one.placement.unwrap();
        assert_eq!((r.case, r.n_star, r.planned_end_before, r.planned_end_after, r.floor_us), (PlacementCase::QueueOverlap, 1_440, 2_454, 1_974, 20_667));
        assert_eq!(one.trims, vec![TrimRecord { submission: s0, by: s1, tail_cut_frames: 534, start_removed: true }]);
        assert_eq!(one.occluded, vec![s0]);
        assert_eq!(engine.waiting_silence_frames(), Some(1_440 - 512), "the silence after n* went too");
        let two = place(&mut engine, s2, 89);
        let r = two.placement.unwrap();
        assert_eq!((r.case, r.n_star, r.planned_end_before, r.planned_end_after), (PlacementCase::QueueOverlap, 1_728, 1_974, 2_262));
        assert_eq!(two.trims, vec![TrimRecord { submission: s1, by: s2, tail_cut_frames: 246, start_removed: false }]);
        assert!(two.occluded.is_empty());
        let mut t = T0;
        let outputs = drain_until_idle(&mut engine, &mut pcm, &mut t);
        let got: Vec<(u32, u64, u64, u64)> =
            outputs.iter().map(|p| (p.submission.identity.seq, p.detail.n_k, p.detail.frames, p.t_output_us)).collect();
        assert_eq!(got, vec![(1, 1_440, 288, T0 + 30_000), (2, 1_728, 534, T0 + 36_000)]);
        for p in &outputs {
            assert_output_content(&pcm, p, 89);
        }
        assert!(pcm.frames[512..1_440].iter().all(|f| *f == 0));
        let a = engine.stats().aligned.unwrap();
        assert_eq!((a.ticks_occluded, a.frames_tail_cut, a.frames_silence_inserted), (1, 534 + 246, 1_408));
        assert_eq!(engine.stats().ticks_output, 2);
        assert_eq!(engine.stats().last_tick_end_frame, Some(2_262));
    }

    /// Case (ii) on a PARTLY WRITTEN tick: c = 10,667 µs puts tick 0 at
    /// n* = round(10,667 × 0.048) = 512 = appl_pos (gap, no silence). At
    /// t0 + 1 ms (hw_pos 48, fill estimate 464 < W) one 256-frame chunk of it
    /// is written: frames 512..768, it enters in_flight with n_k 512. Tick 1
    /// released at t0 + 8,083 µs: n* = 48 + round(17,750 × 0.048) = 900 in
    /// [768, 1,046): its tail 900..1,046 (146 frames) is cut, start kept;
    /// frames 534 -> 388, tick_extent end and last_tick_end_frame 1,046 -> 900.
    /// Its output row carries the shrunk frames.
    #[test]
    fn aligned_case_ii_cuts_the_tail_of_a_partly_written_tick() {
        let (mut engine, mut pcm) = started_recording(aligned_cfg(10_667), SimPcmConfig::new(PERIOD, BUFFER));
        let s0 = released(0, T0, T0);
        let r = place(&mut engine, s0, 89).placement.unwrap();
        assert_eq!((r.case, r.n_star, r.silence_inserted_frames, r.planned_end_after), (PlacementCase::Gap, 512, 0, 1_046));
        assert!(step_dyn(&mut engine, &mut pcm, T0 + 1_000).unwrap().is_empty());
        assert_eq!(engine.written_frames(), 768);
        let id = *engine.aligned.as_ref().unwrap().in_flight.front().expect("tick 0 entered in_flight at its first frame");
        assert_eq!(engine.aligned.as_ref().unwrap().ticks[&id].written, 256);
        assert_eq!(engine.stats().last_tick_end_frame, Some(1_046));
        assert_eq!(engine.tick_extent, Some((512, 1_046)));
        let s1 = released(1, T0 + 8_083, T0 + 8_083);
        let one = place(&mut engine, s1, 89);
        let r = one.placement.unwrap();
        assert_eq!((r.case, r.hw_pos, r.status_htstamp_us, r.appl_pos, r.n_star), (PlacementCase::QueueOverlap, 48, T0 + 1_000, 768, 900));
        assert_eq!(r.floor_us, 1_000 + 15_000 - 8_083);
        assert_eq!(one.trims, vec![TrimRecord { submission: s0, by: s1, tail_cut_frames: 146, start_removed: false }]);
        assert!(one.occluded.is_empty(), "its start was written: never occluded");
        assert_eq!(engine.tick_extent, Some((512, 900)));
        assert_eq!(engine.stats().last_tick_end_frame, Some(900));
        assert_eq!(engine.planned_end(), Some(1_434));
        let mut t = T0 + 1_000;
        let outputs = drain_until_idle(&mut engine, &mut pcm, &mut t);
        let got: Vec<(u32, u64, u64)> = outputs.iter().map(|p| (p.submission.identity.seq, p.detail.n_k, p.detail.frames)).collect();
        assert_eq!(got, vec![(0, 512, 388), (1, 900, 534)]);
        for p in &outputs {
            assert_output_content(&pcm, p, 89);
        }
        assert_eq!(outputs[0].detail.t_write_us, T0 + 1_000);
        assert!(outputs[0].t_output_us.abs_diff(T0 + 10_667) <= 1, "{}", outputs[0].t_output_us);
        assert_eq!(engine.stats().ticks_written, 2);
        assert_eq!(engine.stats().aligned.unwrap().frames_tail_cut, 146);
    }

    /// Case (iii): at t0 + 1 ms one period of silence was written (w 768,
    /// hw_pos 48). c = 12 ms: n* = 48 + 576 = 624 < 768 < 1,158 -> no rewind,
    /// head 768 − 624 = 144 frames cut, the remaining 390 go at 768 = n_k;
    /// t_output = t0 + 768 / 48 kHz = t0 + 16 ms; floor = 15,000 µs > c.
    #[test]
    fn aligned_case_iii_cuts_the_head_and_records_n_k_at_appl_pos() {
        let (mut engine, mut pcm) = started_recording(aligned_cfg(12_000), SimPcmConfig::new(PERIOD, BUFFER));
        assert!(step_dyn(&mut engine, &mut pcm, T0 + 1_000).unwrap().is_empty());
        assert_eq!(engine.written_frames(), 768, "an empty plan writes one period of silence");
        let three = place(&mut engine, released(0, T0 + 1_000, T0 + 1_000), 89);
        assert_eq!(three.admission, AlignedAdmission::Placed);
        let r = three.placement.unwrap();
        assert_eq!((r.case, r.n_star, r.appl_pos, r.head_cut_frames, r.planned_end_after), (PlacementCase::DeviceOverlap, 624, 768, 144, 1_158));
        assert_eq!(r.floor_us, 15_000);
        let mut t = T0 + 1_000;
        let outputs = drain_until_idle(&mut engine, &mut pcm, &mut t);
        assert_eq!(outputs.len(), 1);
        let p = &outputs[0];
        assert_eq!((p.detail.n_k, p.detail.frames, p.detail.samples_8k, p.detail.head_cut_frames), (768, 390, 89, Some(144)));
        assert_eq!(p.t_output_us, T0 + 16_000);
        assert_eq!(p.t_output_us, output_time_us(p.detail.htstamp_us, p.detail.n_k, p.detail.hw_pos).unwrap(), "the unchanged formula");
        assert_eq!(p.detail.to_json()["head_cut_frames"], 144);
        assert_output_content(&pcm, p, 89);
        let a = engine.stats().aligned.unwrap();
        assert_eq!((a.ticks_head_cut, a.frames_head_cut), (1, 144));
        assert_eq!(engine.stats().last_tick_end_frame, Some(1_158));
    }

    /// Case (iv): released 20 ms before t0, seen at t0 + 1 ms (w 768): n* =
    /// 48 + round(−9,000 × 0.048) = −384 (signed), n* + 534 = 150 <= 768.
    #[test]
    fn aligned_case_iv_discards_a_tick_wholly_before_appl_pos() {
        let (mut engine, mut pcm) = started(aligned_cfg(12_000), SimPcmConfig::new(PERIOD, BUFFER), T0);
        step(&mut engine, &mut pcm, T0 + 1_000).unwrap();
        let late = place(&mut engine, released(0, T0 - 20_000, T0 + 1_000), 89);
        assert_eq!(late.admission, AlignedAdmission::TooLate);
        let r = late.placement.unwrap();
        assert_eq!((r.case, r.n_star, r.appl_pos, r.planned_end_before, r.planned_end_after), (PlacementCase::TooLate, -384, 768, 768, 768));
        assert_eq!((r.floor_us, r.head_cut_frames, r.silence_inserted_frames, r.rejected), (36_000, 0, 0, None));
        assert_eq!(engine.stats().aligned.unwrap().ticks_too_late, 1);
        assert!(engine.idle(), "nothing was placed");
        // An unplayable payload: no placement row at all.
        let bad = engine.place(released(1, T0, T0 + 1_000), None);
        assert_eq!(bad, AlignedOffered { placement: None, trims: Vec::new(), occluded: Vec::new(), admission: AlignedAdmission::PayloadInvalid });
        assert_eq!(engine.stats().ticks_payload_invalid, 1);
    }

    /// Write-ahead: a write happens only while fill_est < W (and avail >=
    /// period), and after every fill fill_est < W + period.
    #[test]
    fn aligned_write_ahead_keeps_the_fill_estimate_below_w_plus_a_period() {
        let (mut engine, mut pcm) = started(aligned_cfg(40_000), SimPcmConfig::new(PERIOD, BUFFER), T0);
        let mut t = T0;
        let mut writes = 0;
        for k in 0..400u32 {
            let due = T0 + k as u64 * 11_111;
            while t < due {
                t += 1_000;
                let s = pcm.status(t).unwrap();
                engine.observe(&s, t, t).unwrap();
                let before = engine.fill_estimate().unwrap();
                let w0 = engine.written_frames();
                let mut clock = || t;
                engine.fill(&mut pcm, s.avail, &mut clock).unwrap();
                let after = engine.fill_estimate().unwrap();
                if before >= W as i64 {
                    assert_eq!(engine.written_frames(), w0, "no write while fill_est >= W");
                } else {
                    assert!(engine.written_frames() > w0, "fill_est {before} < W: a chunk is written");
                    writes += 1;
                }
                assert!(after < (W + PERIOD) as i64, "fill_est {after} after a fill");
                assert!(after >= W as i64 || engine.written_frames() == w0);
                assert_eq!((engine.written_frames() - w0) % PERIOD, 0, "whole chunks");
            }
            let samples = if k % 9 == 0 { 88 } else { 89 };
            let r = place(&mut engine, released(k, t, t), samples);
            assert_ne!(r.admission, AlignedAdmission::PlacementQueueFull);
        }
        assert!(writes > 100);
        let mut t2 = t;
        let outputs = drain_until_idle(&mut engine, &mut pcm, &mut t2);
        assert!(!outputs.is_empty());
    }

    /// A partly written tick sits in in_flight AND the plan: abandoned once.
    #[test]
    fn aligned_abandon_ends_a_partly_written_tick_once() {
        let (mut engine, mut pcm) = started(aligned_cfg(10_667), SimPcmConfig::new(PERIOD, BUFFER), T0);
        let _ = place(&mut engine, released(0, T0, T0), 89);
        let _ = place(&mut engine, released(1, T0 + 11_111, T0), 89);
        step(&mut engine, &mut pcm, T0 + 1_000).unwrap();
        assert_eq!(engine.aligned.as_ref().unwrap().in_flight.len(), 1);
        let abandoned: Vec<u32> = engine.abandon().iter().map(|s| s.identity.seq).collect();
        assert_eq!(abandoned, vec![0, 1]);
        assert!(engine.unwritten_tick_frames() == 0 && engine.planned_end() == Some(engine.written_frames()));
    }

    /// The seeded randomized check of design §5-5 over many ticks with
    /// jitter, out-of-order bursts and late ticks (all four cases, trims and
    /// occlusions), for two Q (the second small enough to refuse some).
    #[test]
    fn aligned_randomized_run_keeps_the_section_5_5_invariants() {
        for (seed, q) in [(0x5eed_0001u64, 48_000u64), (0x5eed_0002, 1_900)] {
            let c = 40_000i64;
            let config = PlacementConfig { queue_limit_frames: q, ..aligned_cfg(c) };
            let (mut engine, mut pcm) = started_recording(config, SimPcmConfig::new(PERIOD, BUFFER));
            let mut rng = seed;
            let mut next = move |n: u64| {
                rng ^= rng >> 12;
                rng ^= rng << 25;
                rng ^= rng >> 27;
                rng.wrapping_mul(0x2545_F491_4F6C_DD1D) % n
            };
            // (arrival, release, seq): mostly prompt, some bursts delayed
            // together (reordering), some very late.
            let n = 1_500u32;
            let mut arrivals: Vec<(u64, u64, u32)> = Vec::new();
            let mut k = 0u32;
            while k < n {
                let release = |k: u32, j: u64| T0 + 20_000 + k as u64 * 11_111 + j;
                match next(20) {
                    0 => {
                        // A burst: up to 6 ticks arrive together, late.
                        let len = 1 + next(6) as u32;
                        let last = (k + len).min(n);
                        let at = release(last - 1, 0) + 5_000 + next(40_000);
                        for j in k..last {
                            arrivals.push((at - next(300), release(j, next(300)), j));
                        }
                        k = last;
                    }
                    1 => {
                        let r = release(k, next(300));
                        arrivals.push((r + 30_000 + next(60_000), r, k));
                        k += 1;
                    }
                    _ => {
                        let r = release(k, next(300));
                        arrivals.push((r + next(2_000), r, k));
                        k += 1;
                    }
                }
            }
            arrivals.sort_by_key(|a| a.0);
            let samples_of = |k: u32| {
                let (a, b) = crate::pcm_sample_bounds(k as u64, 8_000, 90);
                (b - a) as usize
            };
            let mut terminal: std::collections::HashMap<u32, &'static str> = std::collections::HashMap::new();
            let end = |seq: u32, what: &'static str, terminal: &mut std::collections::HashMap<u32, &'static str>| {
                assert!(terminal.insert(seq, what).is_none(), "seq {seq}: a second terminal ({what})");
            };
            let (mut records, mut trims, mut outputs) = (Vec::new(), Vec::new(), Vec::new());
            let mut t = T0;
            let mut last_w = engine.written_frames();
            let mut i = 0;
            let mut invalid = 0;
            while i < arrivals.len() || !engine.idle() {
                t += 1_000;
                assert!(t < T0 + 60_000_000, "the run ends");
                let s = pcm.status(t).unwrap();
                for p in engine.observe(&s, t, t).unwrap() {
                    end(p.submission.identity.seq, "output", &mut terminal);
                    outputs.push(p);
                }
                while i < arrivals.len() && arrivals[i].0 <= t {
                    let (_, release, seq) = arrivals[i];
                    i += 1;
                    let sub = released(seq, release, t.max(release));
                    if seq % 211 == 7 {
                        invalid += 1;
                        let bad = engine.place(sub, None);
                        assert_eq!(bad.admission, AlignedAdmission::PayloadInvalid);
                        assert!(bad.placement.is_none(), "no placement row for payload_invalid");
                        end(seq, "payload_invalid", &mut terminal);
                        continue;
                    }
                    let offered = place(&mut engine, sub, samples_of(seq));
                    let r = offered.placement.clone().expect("a placement row per playable submission");
                    // Recompute n* and floor independently of the engine.
                    let rate = 48_000i128;
                    let n_star = r.hw_pos as i128
                        + div_round_half_away((release as i128 + c as i128 - r.status_htstamp_us as i128) * rate, 1_000_000);
                    let floor = r.status_htstamp_us as i128
                        + div_round_half_away((r.appl_pos as i128 - r.hw_pos as i128) * 1_000_000, rate)
                        - release as i128;
                    assert_eq!((r.n_star as i128, r.floor_us as i128), (n_star, floor));
                    assert!(r.planned_end_after >= r.appl_pos && r.planned_end_before >= r.appl_pos);
                    assert_eq!(r.appl_pos, engine.written_frames());
                    assert!(engine.waiting_silence_frames().unwrap() <= q, "waiting silence <= Q");
                    let l = 6 * samples_of(seq) as i64;
                    let want_case = if r.n_star >= r.planned_end_before as i64 {
                        PlacementCase::Gap
                    } else if r.n_star >= r.appl_pos as i64 {
                        PlacementCase::QueueOverlap
                    } else if r.n_star + l > r.appl_pos as i64 {
                        PlacementCase::DeviceOverlap
                    } else {
                        PlacementCase::TooLate
                    };
                    assert_eq!(r.case, want_case);
                    let occluded: Vec<L2Submission> = offered.trims.iter().filter(|t| t.start_removed).map(|t| t.submission).collect();
                    assert_eq!(offered.occluded, occluded);
                    for o in &offered.occluded {
                        end(o.identity.seq, "occluded", &mut terminal);
                    }
                    match offered.admission {
                        AlignedAdmission::Placed => {}
                        AlignedAdmission::TooLate => end(seq, "too_late", &mut terminal),
                        AlignedAdmission::PlacementQueueFull => end(seq, "placement_queue_full", &mut terminal),
                        AlignedAdmission::PayloadInvalid => panic!("playable"),
                    }
                    trims.extend(offered.trims);
                    records.push(r);
                }
                let mut clock = || t;
                engine.fill(&mut pcm, s.avail, &mut clock).unwrap();
                assert!(engine.written_frames() >= last_w, "written frames are monotonic");
                last_w = engine.written_frames();
                assert!(engine.fill_estimate().unwrap() < (W + PERIOD) as i64);
                assert!(engine.planned_end().unwrap() >= engine.written_frames());
            }
            // Terminals = submissions, exactly one each.
            assert_eq!(terminal.len(), n as usize);
            let count = |what: &str| terminal.values().filter(|v| **v == what).count() as u64;
            let a = engine.stats().aligned.unwrap();
            let case_count = |c: PlacementCase| records.iter().filter(|r| r.case == c).count() as u64;
            eprintln!(
                "seed {seed:#x} Q {q}: outputs {} occluded {} too_late {} queue_full {} gap {} queue_overlap {} device_overlap {} trims {}",
                count("output"), count("occluded"), count("too_late"), count("placement_queue_full"),
                case_count(PlacementCase::Gap), case_count(PlacementCase::QueueOverlap), case_count(PlacementCase::DeviceOverlap), trims.len()
            );
            // Every case and kind of terminal is exercised.
            for c in [PlacementCase::Gap, PlacementCase::QueueOverlap, PlacementCase::DeviceOverlap, PlacementCase::TooLate] {
                assert!(case_count(c) > 0, "{c:?} exercised");
            }
            assert!(count("occluded") > 0 && trims.iter().any(|t| !t.start_removed));
            if q < 48_000 {
                assert!(count("placement_queue_full") > 0);
            }
            // Counters = row counts.
            assert_eq!(records.len() as u64 + invalid, n as u64, "one placement row per playable submission");
            assert_eq!(a.ticks_occluded, count("occluded"));
            assert_eq!(a.ticks_occluded, trims.iter().filter(|t| t.start_removed).count() as u64, "occluded = start_removed trims");
            assert_eq!(a.ticks_too_late, count("too_late"));
            assert_eq!(a.ticks_placement_queue_full, count("placement_queue_full"));
            assert_eq!(a.ticks_placement_queue_full, records.iter().filter(|r| r.rejected.is_some()).count() as u64);
            assert_eq!(a.ticks_head_cut, case_count(PlacementCase::DeviceOverlap));
            assert_eq!(a.frames_head_cut, records.iter().map(|r| r.head_cut_frames).sum::<u64>());
            assert_eq!(a.frames_silence_inserted, records.iter().map(|r| r.silence_inserted_frames).sum::<u64>());
            assert_eq!(a.frames_tail_cut, trims.iter().map(|t| t.tail_cut_frames).sum::<u64>());
            assert!(a.max_waiting_silence_frames <= q);
            let stats = engine.stats();
            assert_eq!(stats.ticks_output, count("output"));
            assert_eq!(stats.ticks_payload_invalid, invalid);
            assert_eq!(stats.ticks_admitted, records.iter().filter(|r| r.rejected.is_none() && r.case != PlacementCase::TooLate).count() as u64);
            assert_eq!(stats.frames_written, stats.tick_frames_written + stats.silence_frames_written);
            assert_eq!(stats.frames_written, pcm.frames.len() as u64);
            // Each frame written once: the output extents are disjoint, hold
            // exactly their tick's frames, and every other frame is silence.
            let mut covered = vec![false; pcm.frames.len()];
            for p in &outputs {
                let d = &p.detail;
                assert_output_content(&pcm, p, samples_of(p.submission.identity.seq));
                assert_eq!(d.frames + d.head_cut_frames.unwrap() <= 6 * d.samples_8k, true);
                for f in d.n_k..d.n_k + d.frames {
                    assert!(!covered[f as usize], "frame {f} belongs to two outputs");
                    covered[f as usize] = true;
                }
                assert_eq!(p.t_output_us, output_time_us(d.htstamp_us, d.n_k, d.hw_pos).unwrap());
                assert!(d.hw_pos > d.n_k as i64);
            }
            assert_eq!(covered.iter().filter(|c| **c).count() as u64, stats.tick_frames_written, "every tick frame is in one output");
            assert!(pcm.frames.iter().zip(&covered).all(|(f, c)| *c || *f == 0), "no stray tick frame");
            assert!(outputs.windows(2).all(|w| w[0].detail.n_k + w[0].detail.frames <= w[1].detail.n_k), "outputs in frame order");
        }
    }
}
