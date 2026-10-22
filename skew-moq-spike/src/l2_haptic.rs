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
//! never an output of any tick.
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

use std::collections::VecDeque;

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

/// `occurred_basis` of a device failure.
pub const OCCURRED_KERNEL_TRIGGER: &str = "kernel_trigger_htstamp";
pub const OCCURRED_LAST_GOOD_STATUS: &str = "last_good_status_lower_bound";

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

/// A device failure with its occurrence time (registration §6-1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceFailure {
    pub reason: &'static str,
    /// The kernel's trigger timestamp when the stop moved it, otherwise the
    /// end of the last good status snapshot (a documented LOWER bound).
    pub occurred_us: u64,
    pub occurred_basis: &'static str,
    pub detected_us: u64,
    pub detail: String,
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
    pub htstamp_ns: u64,
    pub htstamp_us: u64,
    pub trigger_htstamp_ns: u64,
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
    pub link_audio_tstamp_ns: u64,
    pub driver_htstamp_ns: u64,
}

impl AlsaOutputDetail {
    pub fn to_json(&self) -> serde_json::Value {
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
            "htstamp_ns": self.htstamp_ns,
            "htstamp_us": self.htstamp_us,
            "trigger_htstamp_ns": self.trigger_htstamp_ns,
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
            "audio_tstamp_accuracy_ns": report.accuracy_report.then_some(report.accuracy_ns),
            "link_audio_tstamp_ns": self.link_audio_tstamp_ns,
            "driver_htstamp_ns": self.driver_htstamp_ns,
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
            "htstamp_ns": "snd_pcm_status_get_htstamp of the snapshot, ns, CLOCK_MONOTONIC (verified tstamp_type)",
            "htstamp_us": "htstamp_ns truncated: sec*1e6 + floor(nsec/1000)",
            "trigger_htstamp_ns": "snd_pcm_status_get_trigger_htstamp (stream start of this epoch while running)",
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
            "audio_tstamp_accuracy_report": "report accuracy_report bit",
            "audio_tstamp_accuracy_ns": "report accuracy in ns, null unless accuracy_report",
            "link_audio_tstamp_ns": "snd_pcm_status_get_audio_htstamp, ns, AUDIO-TIMESTAMP DOMAIN (LINK: HDA link wall clock since stream start), NOT CLOCK_MONOTONIC; comparison only, not the registered formula",
            "driver_htstamp_ns": "snd_pcm_status_get_driver_htstamp, ns, CLOCK_MONOTONIC (taken after the reference timestamp)",
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
    pub max_write_queue_frames: u64,
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
        serde_json::json!({
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
            "max_write_queue_frames": self.max_write_queue_frames,
        })
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

/// What the stream epoch looked like right after `start`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochStart {
    pub t_start_us: u64,
    pub trigger_htstamp_ns: u64,
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
    epoch: Option<EpochStart>,
    last_good_status_us: u64,
    last_hw_ptr_pos: u64,
    last_htstamp_ns: u64,
    last_status_t0_us: Option<u64>,
    stats: LaneStats,
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
            epoch: None,
            last_good_status_us: 0,
            last_hw_ptr_pos: 0,
            last_htstamp_ns: 0,
            last_status_t0_us: None,
            stats: LaneStats::default(),
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

    /// Frames waiting in the write queue (not yet written).
    pub fn pending_frames(&self) -> u64 {
        self.pending_frames
    }

    /// Nothing queued, nothing partially written, nothing awaiting output.
    pub fn idle(&self) -> bool {
        self.pending.is_empty()
            && self.in_flight.is_empty()
            && !matches!(self.current, Some(Current { kind: ChunkKind::Tick { .. }, .. }))
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
    /// timestamp is the epoch's identity from now on.
    pub fn begin_epoch(&mut self, t_start_us: u64, first_status: PcmStatus, t_status_end_us: u64) -> Result<(), String> {
        if first_status.state != PcmState::Running {
            return Err(format!("stream not RUNNING after start: {}", first_status.state.as_str()));
        }
        self.epoch = Some(EpochStart {
            t_start_us,
            trigger_htstamp_ns: first_status.trigger_htstamp_ns,
            first_status,
        });
        self.last_good_status_us = t_status_end_us;
        self.last_htstamp_ns = first_status.htstamp_ns;
        Ok(())
    }

    /// Offer one released haptic submission (its payload: 8 kHz S16_LE mono).
    pub fn admit(&mut self, submission: L2Submission, payload: Option<&[u8]>) -> Admission {
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
        if self.pending_frames + frames > self.cfg.queue_limit_frames {
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
        self.stats.max_write_queue_frames = self.stats.max_write_queue_frames.max(self.pending_frames);
        Admission::Queued
    }

    /// Write what the snapshot's `avail` allows: the chunk in progress, then
    /// whole ticks back to back, then — only with an empty queue and a
    /// queued fill below `F` — one period of silence.
    pub fn fill(&mut self, pcm: &mut dyn HapticPcm, avail: u64, clock: &mut dyn FnMut() -> u64) -> Result<(), PcmError> {
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
                format!("trigger {} != epoch trigger {}", s.trigger_htstamp_ns, epoch.trigger_htstamp_ns),
            ));
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
                format!("htstamp went back {} -> {}", self.last_htstamp_ns, s.htstamp_ns),
            ));
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
                    htstamp_ns: s.htstamp_ns,
                    htstamp_us,
                    trigger_htstamp_ns: s.trigger_htstamp_ns,
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
                    link_audio_tstamp_ns: s.audio_htstamp_ns,
                    driver_htstamp_ns: s.driver_htstamp_ns,
                },
            });
        }
        self.stats.ticks_output += placed.len() as u64;
        self.stats.queued_max_frames = self.stats.queued_max_frames.max(queued);
        self.stats.queued_min_frames = Some(self.stats.queued_min_frames.map_or(queued, |m| m.min(queued)));
        self.last_good_status_us = t1;
        self.last_hw_ptr_pos = hw_ptr_pos;
        self.last_htstamp_ns = s.htstamp_ns;
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

    /// Occurrence: the kernel trigger timestamp when the stop moved it (and
    /// it is inside the epoch), otherwise the end of the last good snapshot,
    /// a LOWER bound. Never after the detection.
    fn failure(&self, reason: &'static str, s: Option<&PcmStatus>, detected_us: u64, detail: String) -> DeviceFailure {
        let epoch = self.epoch.as_ref();
        let moved_trigger = s.zip(epoch).and_then(|(s, epoch)| {
            let moved = s.trigger_htstamp_ns != epoch.trigger_htstamp_ns
                && s.trigger_htstamp_ns >= epoch.trigger_htstamp_ns;
            moved.then(|| ns_to_us_floor(s.trigger_htstamp_ns))
        });
        let (occurred_us, basis) = match moved_trigger {
            Some(t) => (t.min(detected_us), OCCURRED_KERNEL_TRIGGER),
            None => (self.last_good_status_us.min(detected_us), OCCURRED_LAST_GOOD_STATUS),
        };
        DeviceFailure { reason, occurred_us, occurred_basis: basis, detected_us, detail }
    }

    /// Everything submitted that can no longer be output (after a device
    /// failure): written-but-unobserved ticks, the tick in progress and the
    /// write queue, in submission order of their placement.
    pub fn abandon(&mut self) -> Vec<L2Submission> {
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimPcmConfig {
    pub period_frames: u64,
    pub buffer_frames: u64,
    /// Position reporting granularity (HDA: 8/16 frames at 48 kHz S16 stereo).
    pub granularity_frames: u64,
    /// Driver delay folded into `delay` (Realtek on this host: 0).
    pub driver_delay_frames: i64,
    /// The audio timestamp type the "hardware" reports as actual.
    pub audio_tstamp_actual: u32,
    pub faults: Vec<SimFault>,
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
        let n = frames.min(space);
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
    /// XRUN at the kernel trigger time; no output from that snapshot.
    #[test]
    fn an_xrun_is_a_failure_at_the_kernel_trigger_time() {
        let t0 = 1_000_000;
        let (mut engine, mut pcm) = started(cfg(), SimPcmConfig::new(PERIOD, BUFFER), t0);
        engine.admit(submission(0, t0), Some(&payload(88, 0)));
        step(&mut engine, &mut pcm, t0).unwrap();
        // Starved: 1040 frames last 21.67 ms; poll again only at +50 ms.
        let failure = step(&mut engine, &mut pcm, t0 + 50_000).unwrap_err();
        assert_eq!(failure.reason, FAIL_ALSA_XRUN);
        assert_eq!(failure.occurred_basis, OCCURRED_KERNEL_TRIGGER);
        assert_eq!(failure.occurred_us, t0 + 21_667, "1040 frames after the start");
        assert_eq!(failure.detected_us, t0 + 50_000);
        assert_eq!(engine.abandon().len(), 1, "the written tick is abandoned, not output");
    }

    #[test]
    fn injected_xrun_and_suspend_use_their_trigger_time() {
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
            assert_eq!((failure.reason, failure.occurred_us), (reason, t0 + 3_000));
            assert_eq!(failure.occurred_basis, OCCURRED_KERNEL_TRIGGER);
        }
    }

    /// A moved trigger (restart) or a DMA pointer that goes back is an epoch
    /// reset; a status error has no kernel time, so the last good snapshot
    /// is the documented lower bound.
    #[test]
    fn epoch_resets_and_status_errors_are_failures_with_their_time_basis() {
        let t0 = 1_000_000;
        let mut sim = SimPcmConfig::new(PERIOD, BUFFER);
        sim.faults.push(SimFault::EpochReset { after_start_us: 2_500, rewind_frames: 0 });
        let (mut engine, mut pcm) = started(cfg(), sim, t0);
        step(&mut engine, &mut pcm, t0 + 2_000).unwrap();
        let failure = step(&mut engine, &mut pcm, t0 + 3_000).unwrap_err();
        assert_eq!((failure.reason, failure.occurred_us), (FAIL_ALSA_EPOCH_RESET, t0 + 2_500));
        assert_eq!(failure.occurred_basis, OCCURRED_KERNEL_TRIGGER);

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
        assert_eq!((failure.occurred_us, failure.occurred_basis), (t0 + 6_000, OCCURRED_LAST_GOOD_STATUS));

        let mut sim = SimPcmConfig::new(PERIOD, BUFFER);
        sim.faults.push(SimFault::StatusError { after_start_us: 4_000 });
        let (mut engine, mut pcm) = started(cfg(), sim, t0);
        step(&mut engine, &mut pcm, t0 + 3_000).unwrap();
        let failure = step(&mut engine, &mut pcm, t0 + 5_000).unwrap_err();
        assert_eq!(failure.reason, FAIL_ALSA_STATUS_ERROR);
        assert_eq!((failure.occurred_us, failure.detected_us), (t0 + 3_000, t0 + 5_000));
        assert_eq!(failure.occurred_basis, OCCURRED_LAST_GOOD_STATUS);
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
        assert_eq!(failure.occurred_us, t0 + 2_000, "the last good status of this iteration");
        assert_eq!(failure.occurred_basis, OCCURRED_LAST_GOOD_STATUS);
        let abandoned: Vec<u32> = engine.abandon().iter().map(|s| s.identity.seq).collect();
        assert_eq!(abandoned, vec![0, 1, 2], "written, in progress and queued");
        assert!(engine.idle());
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
}
