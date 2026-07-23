// Shared instrumentation for the MoQ B1 test bed — byte-identical to the
// Python `skew_logging.py` spec so `analyze_skew.py` consumes the JSONL
// unchanged.  Header "<BBHIQIQI" (32B LE), CLOCK_MONOTONIC µs clock, the snap
// pairing rule, the PC-tier / haptic-PCM workload loaders, and the JSONL logger.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

pub mod playout;

pub const HDR: usize = 32;
pub const VERSION: u8 = 1;
pub const TRACK_PC: u8 = 0;
pub const TRACK_HAPTIC: u8 = 1;
pub const HAPTIC_TICK_US: u64 = 10_000; // 100 Hz
pub const HAPTIC_SAMPLES_PER_TICK: usize = 80; // 8kHz * 10ms
pub const HAPTIC_TIER_FULL: u16 = 0;
/// 종료 프로토콜 세대. meta의 `term_protocol`로 기록되어, 분석기가 TX/RX
/// shutdown 정확히 1개를 무조건 요구하는 근거가 된다. 프로토콜 의미가
/// 바뀌면 올린다(1 = shutdown 레코드 각 1개 + exit_code/ending 기록).
pub const TERM_PROTOCOL_V: u32 = 1;

pub fn track_name(track_id: u8) -> &'static str {
    match track_id {
        TRACK_PC => "pc",
        TRACK_HAPTIC => "haptic",
        _ => "unknown",
    }
}

/// System CLOCK_MONOTONIC in µs — matches Python `time.monotonic_ns() // 1000`.
/// System-wide (identical across network namespaces on one kernel), so the
/// one-way delay D = t_recv − t_gen is valid across the tx/rx processes.
pub fn now_us() -> u64 {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    unsafe {
        libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts);
    }
    (ts.tv_sec as u64) * 1_000_000 + (ts.tv_nsec as u64) / 1000
}

/// PC frame index -> pts(µs). Integer math, drift-free (== skew_logging).
pub fn frame_pts_us(frame_idx: u64, fps: u64) -> u64 {
    frame_idx * 1_000_000 / fps
}

/// Snap rule: haptic tick index that frame `k` belongs to = pts_k // 10ms.
pub fn snap_tick(frame_idx: u64, fps: u64) -> u64 {
    frame_pts_us(frame_idx, fps) / HAPTIC_TICK_US
}

/// Pack the 32B header: version, track_id, tier, seq, pts_us, event_id,
/// gen_ts_us, payload_len (little-endian, no padding).
pub fn pack_header(
    track_id: u8,
    tier: u16,
    seq: u32,
    pts_us: u64,
    event_id: u32,
    gen_ts_us: u64,
    payload_len: u32,
) -> [u8; HDR] {
    let mut b = [0u8; HDR];
    b[0] = VERSION;
    b[1] = track_id;
    b[2..4].copy_from_slice(&tier.to_le_bytes());
    b[4..8].copy_from_slice(&seq.to_le_bytes());
    b[8..16].copy_from_slice(&pts_us.to_le_bytes());
    b[16..20].copy_from_slice(&event_id.to_le_bytes());
    b[20..28].copy_from_slice(&gen_ts_us.to_le_bytes());
    b[28..32].copy_from_slice(&payload_len.to_le_bytes());
    b
}

#[derive(Debug, Clone, Copy)]
pub struct Header {
    pub version: u8,
    pub track_id: u8,
    pub tier: u16,
    pub seq: u32,
    pub pts_us: u64,
    pub event_id: u32,
    pub gen_ts_us: u64,
    pub payload_len: u32,
}

pub fn unpack_header(buf: &[u8]) -> Option<Header> {
    if buf.len() < HDR {
        return None;
    }
    Some(Header {
        version: buf[0],
        track_id: buf[1],
        tier: u16::from_le_bytes(buf[2..4].try_into().unwrap()),
        seq: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
        pts_us: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
        event_id: u32::from_le_bytes(buf[16..20].try_into().unwrap()),
        gen_ts_us: u64::from_le_bytes(buf[20..28].try_into().unwrap()),
        payload_len: u32::from_le_bytes(buf[28..32].try_into().unwrap()),
    })
}

// ---- Workload loaders ----

/// Load PC frame payloads: every *.bin (or *.drc) in `dir`, sorted by name.
/// Payload is opaque to transport — uncompressed 8B/point .bin or Draco .drc.
pub fn load_frames(dir: &str) -> anyhow::Result<Vec<Vec<u8>>> {
    let mut paths: Vec<_> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|x| x == "bin" || x == "drc").unwrap_or(false))
        .collect();
    paths.sort();
    if paths.is_empty() {
        anyhow::bail!("no *.bin/*.drc frames in {dir}");
    }
    let frames = paths
        .iter()
        .map(std::fs::read)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(frames)
}

/// Read raw PCM bytes from a WAV file, asserting 8kHz / mono / 16-bit.
pub fn load_haptic_pcm(path: &str) -> anyhow::Result<Vec<u8>> {
    let bytes = std::fs::read(path)?;
    anyhow::ensure!(bytes.len() > 44 && &bytes[0..4] == b"RIFF" && &bytes[8..12] == b"WAVE", "not a WAV: {path}");
    // Walk sub-chunks: [id:4][size:4][data:size], starting at offset 12.
    let mut off = 12usize;
    let mut fmt_ok = false;
    while off + 8 <= bytes.len() {
        let id = &bytes[off..off + 4];
        let size = u32::from_le_bytes(bytes[off + 4..off + 8].try_into().unwrap()) as usize;
        let body = off + 8;
        if id == b"fmt " && body + 16 <= bytes.len() {
            let channels = u16::from_le_bytes(bytes[body + 2..body + 4].try_into().unwrap());
            let rate = u32::from_le_bytes(bytes[body + 4..body + 8].try_into().unwrap());
            let bits = u16::from_le_bytes(bytes[body + 14..body + 16].try_into().unwrap());
            anyhow::ensure!(rate == 8000 && channels == 1 && bits == 16, "haptic WAV must be 8kHz/mono/16bit (got {rate}Hz/{channels}ch/{bits}bit)");
            fmt_ok = true;
        }
        if id == b"data" {
            anyhow::ensure!(fmt_ok, "WAV data before fmt");
            let end = (body + size).min(bytes.len());
            return Ok(bytes[body..end].to_vec());
        }
        off = body + size + (size & 1); // chunks are word-aligned
    }
    anyhow::bail!("no data chunk in WAV: {path}")
}

// ---- JSONL logger (matches skew_logging.JsonlLogger / make_meta) ----

pub struct JsonlLogger {
    w: BufWriter<File>,
}

fn esc(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

impl JsonlLogger {
    /// Open and write the meta line. `c_mbps` is None for the unconstrained run.
    pub fn new(
        path: &Path,
        run_id: &str,
        stack: &str,
        side: &str,
        c_mbps: Option<f64>,
        rtt_ms: f64,
        jitter_ms: f64,
        loss_pct: f64,
        s_bytes: u64,
        fps: u64,
        haptic_hz: u64,
        seed: u64,
        duration_s: Option<f64>,
        haptic_src: Option<&str>,
        // C3: which tracks this side handles ("both" | "pc" | "haptic").
        // The receiver now also records this (the runner passes --tracks),
        // so a C3 rx log stays classifiable even if the tx log is lost.
        tracks: Option<&str>,
        // 종료 프로토콜 세대 마커. meta는 1행이라 후방 절단에도 생존하며,
        // 분석기는 이 값이 있으면 TX/RX shutdown 정확히 1개를 무조건 요구한다.
        // 휴리스틱(tx meta의 tracks 존재)은 tx 로그까지 잃으면 우회되므로
        // rx meta 자체에 명시해야 한다(Codex 7차 P0).
        term_protocol: Option<u32>,
        // Phase 4 S1 only. None preserves existing B1 metadata; Some appends
        // every parameter that can affect a scheduler release/drop decision.
        playout: Option<playout::PlayoutConfig>,
    ) -> anyhow::Result<Self> {
        let f = File::create(path)?;
        let mut w = BufWriter::new(f);
        let cmb = match c_mbps {
            Some(v) => format!("{v}"),
            None => "null".to_string(),
        };
        // 설계값(측정값 아님): duration_s 와 expected_pc_frames = round(duration_s*fps).
        // skew_logging.make_meta 와 동일한 키·위치(clock 뒤, extra 앞)·값 규칙.
        // `{:?}` 는 f64 를 항상 소수점 포함으로 찍어 Python float repr 과 맞춘다.
        let design = match duration_s {
            Some(d) => format!(
                ",\"duration_s\":{:?},\"expected_pc_frames\":{}",
                d,
                (d * fps as f64).round() as i64
            ),
            None => String::new(),
        };
        let extra = match haptic_src {
            Some(h) => format!(",\"haptic_src\":\"{}\"", esc(h)),
            None => String::new(),
        };
        let tracks = match tracks {
            Some(t) => format!(",\"tracks\":\"{}\"", esc(t)),
            None => String::new(),
        };
        let term = match term_protocol {
            Some(v) => format!(",\"term_protocol\":{v}"),
            None => String::new(),
        };
        let playout = match playout {
            Some(p) => format!(
                ",\"arm\":\"s1\",\"playout_clock\":\"receiver_monotonic_us\",\"d_play_us\":{},\"startup_timeout_us\":{},\"late_tolerance_us\":{},\"late_policy\":\"{}\",\"buffer_max_objects_per_track\":{},\"buffer_max_span_us\":{}",
                p.d_play_us,
                p.startup_timeout_us,
                p.late_tolerance_us,
                p.late_policy.as_str(),
                p.max_objects_per_track,
                p.max_span_us,
            ),
            None => String::new(),
        };
        writeln!(
            w,
            "{{\"role\":\"meta\",\"run_id\":\"{}\",\"stack\":\"{}\",\"side\":\"{}\",\"cond\":{{\"C_mbps\":{},\"rtt_ms\":{},\"jitter_ms\":{},\"loss_pct\":{}}},\"S_bytes\":{},\"fps\":{},\"haptic_hz\":{},\"seed\":{},\"clock\":\"monotonic_ns/1000\"{}{}{}{}{}}}",
            esc(run_id), esc(stack), esc(side), cmb, rtt_ms, jitter_ms, loss_pct,
            s_bytes, fps, haptic_hz, seed, design, extra, tracks, term, playout
        )?;
        w.flush()?;
        Ok(Self { w })
    }

    /// tx record. `obj` is the MoQ object identity `(group_id, subgroup_id,
    /// object_id)` this payload was written as — A2 join key for the
    /// `role:"accept"` records. When `None` the line is byte-identical to the
    /// pre-A2 schema and to `skew_logging.py`; when `Some`, three keys are
    /// appended after `t_send`. No existing key changes name, order or meaning.
    #[allow(clippy::too_many_arguments)]
    pub fn log_tx(&mut self, track: &str, tier: u16, seq: u32, pts_us: u64, event_id: u32, size: usize, t_gen: u64, t_send: u64, obj: Option<(u64, u64, u64)>) {
        let objf = match obj {
            Some((g, sg, o)) => format!(",\"group_id\":{g},\"subgroup_id\":{sg},\"object_id\":{o}"),
            None => String::new(),
        };
        let _ = writeln!(
            self.w,
            "{{\"role\":\"tx\",\"track\":\"{track}\",\"tier\":{tier},\"seq\":{seq},\"pts_us\":{pts_us},\"event_id\":{event_id},\"size\":{size},\"t_gen\":{t_gen},\"t_send\":{t_send}{objf}}}"
        );
        let _ = self.w.flush();
    }

    #[allow(clippy::too_many_arguments)]
    pub fn log_rx(&mut self, track: &str, tier: u16, seq: u32, pts_us: u64, event_id: u32, size: u32, t_recv: u64, t_play: u64, t_gen: u64) {
        let _ = writeln!(
            self.w,
            "{{\"role\":\"rx\",\"track\":\"{track}\",\"tier\":{tier},\"seq\":{seq},\"pts_us\":{pts_us},\"event_id\":{event_id},\"size\":{size},\"t_recv\":{t_recv},\"t_play\":{t_play},\"t_gen\":{t_gen}}}"
        );
        let _ = self.w.flush();
    }

    /// Phase-4 L1-R application-release record. This is not L2 `t_play`.
    #[allow(clippy::too_many_arguments)]
    pub fn try_log_release(&mut self, track: &str, tier: u16, seq: u32, pts_us: u64, event_id: u32, t_release: u64) -> std::io::Result<()> {
        writeln!(
            self.w,
            "{{\"role\":\"release\",\"track\":\"{track}\",\"tier\":{tier},\"seq\":{seq},\"pts_us\":{pts_us},\"event_id\":{event_id},\"t_release\":{t_release}}}"
        )?;
        self.w.flush()
    }

    /// Phase-4 L1-R terminal drop record with the same exact identity as rx.
    #[allow(clippy::too_many_arguments)]
    pub fn try_log_drop(&mut self, track: &str, tier: u16, seq: u32, pts_us: u64, event_id: u32, t_drop: u64, drop_reason: &str) -> std::io::Result<()> {
        writeln!(
            self.w,
            "{{\"role\":\"drop\",\"track\":\"{track}\",\"tier\":{tier},\"seq\":{seq},\"pts_us\":{pts_us},\"event_id\":{event_id},\"t_drop\":{t_drop},\"drop_reason\":\"{}\"}}",
            esc(drop_reason)
        )?;
        self.w.flush()
    }

    /// Transport-accept record — A2 instrumentation, additive to the tx/rx schema.
    ///
    /// `t_accept` is the monotonic µs at which the QUIC stack **accepted** the
    /// last payload byte of this object (see `moq_transport::accept_trace`).
    /// This is a transport-accept time, NOT an on-the-wire time: the bytes have
    /// been taken into the QUIC send path, not necessarily transmitted, and
    /// certainly not acknowledged. Under congestion, flow-control credit runs
    /// out and this follows send backpressure closely; outside congestion it is
    /// close to a pure application handoff time and says little.
    ///
    /// Join key to the `role:"tx"` line is (`track`, `group_id`,
    /// `subgroup_id`, `object_id`); the tx line carries the same triple.
    ///
    /// NOTE `size` here is the MoQ **object** payload, i.e. HDR(32) + the app
    /// payload, so it is 32 larger than `size` on the matching tx line. This is
    /// deliberate: it is the byte count actually pushed into the QUIC stream.
    ///
    /// Unlike the other loggers this one **propagates I/O errors**: the accept
    /// integrity counters must reflect records that actually reached the file,
    /// not records we merely attempted (A2-c R3).
    #[allow(clippy::too_many_arguments)]
    pub fn log_accept(&mut self, track: &str, group_id: u64, subgroup_id: u64, object_id: u64, t_accept: u64, size: usize) -> std::io::Result<()> {
        writeln!(
            self.w,
            "{{\"role\":\"accept\",\"track\":\"{track}\",\"group_id\":{group_id},\"subgroup_id\":{subgroup_id},\"object_id\":{object_id},\"t_accept\":{t_accept},\"size\":{size}}}"
        )
    }

    /// Flush, propagating the error. `flush()` remains the ignore-error form
    /// used by paths that have no counter to correct.
    pub fn try_flush(&mut self) -> std::io::Result<()> {
        self.w.flush()
    }

    /// Free-form info record (e.g. {"role":"info", ...}). `body` is inner JSON without braces.
    pub fn log_info(&mut self, body: &str) {
        let _ = writeln!(self.w, "{{\"role\":\"info\",{body}}}");
        let _ = self.w.flush();
    }

    /// Like `log_info`, but propagates I/O errors.
    ///
    /// The finalizer must use this: a run whose stats record silently failed to
    /// reach disk and then exited 0 is indistinguishable from a run that was
    /// never instrumented, which is precisely the ambiguity the stats record
    /// exists to remove (A2-c R2).
    pub fn try_log_info(&mut self, body: &str) -> std::io::Result<()> {
        writeln!(self.w, "{{\"role\":\"info\",{body}}}")?;
        self.w.flush()
    }

    pub fn flush(&mut self) {
        let _ = self.w.flush();
    }
}

#[cfg(test)]
mod phase4_jsonl_tests {
    use super::*;
    use crate::playout::{LatePolicy, PlayoutConfig};

    fn path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "skew-{name}-{}-{}.jsonl",
            std::process::id(),
            now_us()
        ))
    }

    #[test]
    fn b1_meta_is_unchanged_and_s1_fields_are_append_only() {
        let b1 = path("b1-meta");
        {
            JsonlLogger::new(
                &b1, "run", "moq", "rx", None, 0.0, 0.0, 0.0,
                10, 30, 100, 1, None, None, Some("both"),
                Some(TERM_PROTOCOL_V), None,
            ).unwrap();
        }
        let b1_line = std::fs::read_to_string(&b1).unwrap();
        assert_eq!(
            b1_line,
            "{\"role\":\"meta\",\"run_id\":\"run\",\"stack\":\"moq\",\"side\":\"rx\",\"cond\":{\"C_mbps\":null,\"rtt_ms\":0,\"jitter_ms\":0,\"loss_pct\":0},\"S_bytes\":10,\"fps\":30,\"haptic_hz\":100,\"seed\":1,\"clock\":\"monotonic_ns/1000\",\"tracks\":\"both\",\"term_protocol\":1}\n"
        );
        std::fs::remove_file(&b1).unwrap();

        let s1 = path("s1-meta");
        let config = PlayoutConfig {
            d_play_us: 50_000,
            startup_timeout_us: 100_000,
            late_tolerance_us: 5_000,
            max_objects_per_track: 64,
            max_span_us: 250_000,
            late_policy: LatePolicy::DropLate,
        };
        {
            JsonlLogger::new(
                &s1, "run", "moq", "rx", None, 0.0, 0.0, 0.0,
                10, 30, 100, 1, None, None, Some("both"),
                Some(TERM_PROTOCOL_V), Some(config),
            ).unwrap();
        }
        let s1_line = std::fs::read_to_string(&s1).unwrap();
        assert!(s1_line.starts_with(b1_line.trim_end_matches("}\n")));
        for field in [
            "\"arm\":\"s1\"",
            "\"d_play_us\":50000",
            "\"startup_timeout_us\":100000",
            "\"late_tolerance_us\":5000",
            "\"late_policy\":\"drop-late\"",
            "\"buffer_max_objects_per_track\":64",
            "\"buffer_max_span_us\":250000",
        ] {
            assert!(s1_line.contains(field), "missing {field}: {s1_line}");
        }
        std::fs::remove_file(&s1).unwrap();
    }

    #[test]
    fn rust_release_and_drop_rows_match_g1_contract() {
        let path = path("release-schema");
        {
            let mut log = JsonlLogger::new(
                &path, "run", "moq", "rx", None, 0.0, 0.0, 0.0,
                10, 30, 100, 1, None, None, Some("both"),
                Some(TERM_PROTOCOL_V), None,
            ).unwrap();
            log.try_log_release("pc", 2, 7, 123_000, 8, 456_000).unwrap();
            log.try_log_drop("haptic", 0, 9, 223_000, 0, 556_000, "late").unwrap();
        }
        let lines: Vec<String> = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect();
        assert_eq!(
            lines[1],
            "{\"role\":\"release\",\"track\":\"pc\",\"tier\":2,\"seq\":7,\"pts_us\":123000,\"event_id\":8,\"t_release\":456000}"
        );
        assert_eq!(
            lines[2],
            "{\"role\":\"drop\",\"track\":\"haptic\",\"tier\":0,\"seq\":9,\"pts_us\":223000,\"event_id\":0,\"t_drop\":556000,\"drop_reason\":\"late\"}"
        );
        std::fs::remove_file(&path).unwrap();
    }
}

// ---- A2: transport-accept tap ----------------------------------------------
//
// `moq_transport::accept_trace` invokes the observer on the publisher
// forwarding task, i.e. on the transport hot path. The observer therefore does
// exactly two things: read the monotonic clock, and `try_send` into a bounded
// channel. It never blocks, never locks, never touches the filesystem. If the
// channel is full the record is dropped and counted; the count is written to
// the log at shutdown so a run with lossy instrumentation is identifiable
// rather than silently short.
//
// What is captured is a transport-accept time, not an on-the-wire time. Under
// congestion it follows QUIC send backpressure closely because flow-control
// credit is the binding constraint; outside congestion it degenerates to an
// application handoff time.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Track identity for an accept record without allocating for the common tracks.
#[derive(Debug, Clone)]
pub enum TrackTag {
    Pc,
    Haptic,
    Other(String),
}

impl TrackTag {
    fn from_name(name: &str) -> Self {
        match name {
            "pc" => TrackTag::Pc,
            "haptic" => TrackTag::Haptic,
            other => TrackTag::Other(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            TrackTag::Pc => "pc",
            TrackTag::Haptic => "haptic",
            TrackTag::Other(s) => s.as_str(),
        }
    }
}

/// One object's transport-accept observation, moved off the transport path.
#[derive(Debug, Clone)]
pub struct AcceptRec {
    pub track: TrackTag,
    pub group_id: u64,
    pub subgroup_id: u64,
    pub object_id: u64,
    /// Transport-accept time (µs, monotonic). Not an on-the-wire time.
    pub t_accept: u64,
    pub size: usize,
}

/// Instrumentation-integrity counters, shared between the observer (transport
/// task), the drain task, and the shutdown path.
///
/// These exist to separate two situations the analyzer must never conflate:
///
/// * **Instrumentation defect** — the hook fired but the record was lost
///   (`dropped_full`, `dropped_closed`, or `written < enqueued`). Any accept
///   coverage shortfall is then an artefact and the run's A2 component is
///   unusable.
/// * **Transport fact** — the hook never fired for an object because QUIC
///   never accepted it (it was still in the send path at shutdown). The
///   instrumentation is intact and a coverage shortfall is a *finding*, not a
///   defect.
///
/// The instrumentation is intact iff
/// `callbacks == written && dropped_full == 0 && dropped_closed == 0 && drain_complete`.
#[derive(Debug, Default)]
pub struct AcceptCounters {
    /// Times `on_object_accepted` was invoked by the transport.
    callbacks: AtomicU64,
    /// Records successfully placed on the channel.
    enqueued: AtomicU64,
    /// Records actually written to the JSONL file.
    written: AtomicU64,
    /// Records discarded because the bounded channel was full.
    dropped_full: AtomicU64,
    /// Records discarded because the tap was closed (shutdown) or the
    /// receiver was gone.
    dropped_closed: AtomicU64,
    /// Writes or flushes that returned an I/O error.
    io_errors: AtomicU64,
    /// Set at shutdown: stop enqueuing so the in-flight set becomes finite.
    closed: AtomicBool,
    /// Incremented on entry to and exit from the observer body, so the
    /// shutdown path can tell whether any callback is still mid-flight.
    in_flight: AtomicU64,
}

/// Immutable snapshot of [`AcceptCounters`] plus the drain outcome.
#[derive(Debug, Clone, Copy)]
pub struct AcceptSnapshot {
    pub callbacks: u64,
    pub enqueued: u64,
    pub written: u64,
    pub dropped_full: u64,
    pub dropped_closed: u64,
    /// Records whose write or flush returned an I/O error. Never counted as
    /// `written`, so an I/O failure can only ever make `intact` false.
    pub io_errors: u64,
    /// Callbacks that were still inside the observer body when the snapshot was
    /// taken. Such a callback has already incremented `callbacks` but has not
    /// yet reached its exit counter, so it is the exact reconciliation term for
    /// conservation leg 1. In a correct shutdown this is 0, because producers
    /// are aborted and joined first.
    pub in_flight: u64,
    pub drain_complete: bool,
    /// Producers were observed to have stopped calling the hook before the
    /// snapshot was taken (the callback counter held steady with nothing
    /// mid-body). This is a *confirmation on top of* a successful join, never a
    /// substitute for one — see `producers_joined`.
    pub producers_quiesced: bool,
    /// Every producer task was provably joined (or never started). False means
    /// a handle was detached after a join timeout, or a task panicked, so no
    /// quiescence observation can be trusted.
    pub producers_joined: bool,
    /// Per-producer join outcome, for diagnosing which one failed.
    pub session_join: JoinOutcome,
    pub ns_join: JoinOutcome,
    pub capacity: usize,
}

impl AcceptSnapshot {
    /// Records that were enqueued but neither confirmed written nor charged to
    /// an I/O error — i.e. still sitting in the queue when the drain deadline
    /// expired. Non-zero means the accept population is short.
    pub fn unwritten(&self) -> u64 {
        self.enqueued
            .saturating_sub(self.written)
            .saturating_sub(self.io_errors)
    }

    /// A2-c R4 conservation law, in two independent legs:
    ///
    /// Sum of the three exits a callback can take.
    pub fn exits(&self) -> u64 {
        self.enqueued + self.dropped_full + self.dropped_closed
    }

    /// A2-c R4 conservation law, in three real legs:
    ///
    /// 1. `callbacks >= exits` — always required. The snapshot read order makes
    ///    this hold for any honest counter set, so a violation means genuine
    ///    double counting or corruption.
    /// 2. `callbacks == exits` — required **when the run claims to be settled**
    ///    (`in_flight == 0` and producers quiesced). This is the strict form and
    ///    the one that matters for a run we intend to use.
    /// 3. `written + io_errors <= enqueued` — the drain cannot have accounted
    ///    for more records than were ever enqueued.
    ///
    /// None is satisfied by construction. A mid-race snapshot legitimately has
    /// `callbacks > exits`; it is not called a conservation violation, because
    /// it is already disqualified by `producers_quiesced == false`.
    pub fn conservation_ok(&self) -> bool {
        if self.callbacks < self.exits() {
            return false;
        }
        if self.in_flight == 0 && self.producers_joined && self.producers_quiesced && self.callbacks != self.exits() {
            return false;
        }
        self.written + self.io_errors <= self.enqueued
    }

    /// True iff every hook invocation provably reached the file.
    ///
    /// Deliberately conservative: any unexplained counter, any lost record, any
    /// I/O error, an incomplete drain, or producers that were still running at
    /// snapshot time all force this to false. A false-positive here would let a
    /// defective run masquerade as a finding, which is the failure mode this
    /// whole structure exists to prevent.
    pub fn intact(&self) -> bool {
        self.callbacks == self.written
            && self.dropped_full == 0
            && self.dropped_closed == 0
            && self.io_errors == 0
            && self.unwritten() == 0
            && self.in_flight == 0
            && self.drain_complete
            && self.producers_joined
            && self.producers_quiesced
            && self.conservation_ok()
    }

    /// Inner JSON for `JsonlLogger::log_info` (no braces). Every exit path —
    /// normal, signal, and error — emits exactly this shape, so the analyzer
    /// never has to branch on how the run ended.
    pub fn info_body(&self) -> String {
        format!(
            "\"event\":\"accept_trace\",\"accept_callbacks\":{},\"accept_enqueued\":{},\"accept_written\":{},\"accept_unwritten\":{},\"dropped_full\":{},\"dropped_closed\":{},\"io_errors\":{},\"in_flight\":{},\"drain_complete\":{},\"producers_joined\":{},\"producers_quiesced\":{},\"session_join\":\"{}\",\"ns_join\":\"{}\",\"conservation_ok\":{},\"accept_intact\":{},\"accept_capacity\":{}",
            self.callbacks, self.enqueued, self.written, self.unwritten(),
            self.dropped_full, self.dropped_closed, self.io_errors, self.in_flight,
            self.drain_complete, self.producers_joined, self.producers_quiesced,
            self.session_join.as_str(), self.ns_join.as_str(), self.conservation_ok(),
            self.intact(), self.capacity
        )
    }
}

/// Bounded, non-blocking observer installed into `moq_transport::accept_trace`.
pub struct AcceptTap {
    tx: tokio::sync::mpsc::Sender<AcceptRec>,
    c: Arc<AcceptCounters>,
}

impl moq_transport::accept_trace::AcceptObserver for AcceptTap {
    fn on_object_accepted(&self, ev: &moq_transport::accept_trace::ObjectAccepted<'_>) {
        // Runs on the transport forwarding task: clock read, one try_send, a
        // few relaxed atomics. Never blocks, never locks, never does I/O.
        //
        // `in_flight` brackets the whole body so the shutdown path can wait for
        // stragglers that entered before `closed` was set (A2-c R2/R5).
        self.c.in_flight.fetch_add(1, Ordering::SeqCst);
        self.c.callbacks.fetch_add(1, Ordering::SeqCst);

        // Once closed, stop enqueuing so the shutdown path has a finite set to
        // drain. Objects accepted after this point are counted, not recorded.
        if self.c.closed.load(Ordering::Acquire) {
            self.c.dropped_closed.fetch_add(1, Ordering::SeqCst);
            self.c.in_flight.fetch_sub(1, Ordering::SeqCst);
            return;
        }

        // Timestamp with the same clock and epoch as the app's t_gen / t_send,
        // so the analyzer joins without any anchor correction.
        let t_accept = now_us();
        let rec = AcceptRec {
            track: TrackTag::from_name(ev.track_name),
            group_id: ev.group_id,
            subgroup_id: ev.subgroup_id,
            object_id: ev.object_id,
            t_accept,
            size: ev.payload_bytes,
        };
        match self.tx.try_send(rec) {
            Ok(()) => {
                self.c.enqueued.fetch_add(1, Ordering::SeqCst);
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                self.c.dropped_full.fetch_add(1, Ordering::SeqCst);
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                self.c.dropped_closed.fetch_add(1, Ordering::SeqCst);
            }
        }
        self.c.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Outcome of joining one producer task.
///
/// `producers_quiesced` (the callback counter holding steady) is a *check on
/// top of* a successful join, never a substitute for one: a detached task that
/// happens to be idle for a few milliseconds looks identical to a joined one.
/// So the join result is recorded explicitly and gates `accept_intact`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinOutcome {
    /// Task was never spawned (setup failed earlier). Nothing to join.
    NotStarted,
    /// Task future ran to completion and was joined.
    Completed,
    /// Task was aborted and the abort was joined — the normal shutdown path.
    Cancelled,
    /// Task panicked. It is finished, but this is a defect.
    Panicked,
    /// Join budget expired. The handle is detached and the task may still be
    /// running, so nothing downstream may be trusted.
    TimedOut,
    /// Already consumed elsewhere (e.g. the error branch observed it finish).
    /// Recorded so the finalizer never re-polls a completed `JoinHandle`,
    /// which panics.
    AlreadyJoined,
}

impl JoinOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            JoinOutcome::NotStarted => "not_started",
            JoinOutcome::Completed => "completed",
            JoinOutcome::Cancelled => "cancelled",
            JoinOutcome::Panicked => "panicked",
            JoinOutcome::TimedOut => "timed_out",
            JoinOutcome::AlreadyJoined => "already_joined",
        }
    }

    /// Whether the task is provably finished and its future dropped, so it can
    /// no longer invoke an accept callback.
    pub fn is_complete(self) -> bool {
        matches!(
            self,
            JoinOutcome::NotStarted
                | JoinOutcome::Completed
                | JoinOutcome::Cancelled
                | JoinOutcome::AlreadyJoined
        )
    }

    /// Classify a `JoinHandle` result.
    pub fn from_join_result<T>(r: &std::result::Result<T, tokio::task::JoinError>) -> Self {
        match r {
            Ok(_) => JoinOutcome::Completed,
            Err(e) if e.is_panic() => JoinOutcome::Panicked,
            Err(_) => JoinOutcome::Cancelled,
        }
    }
}

/// Join outcomes for every producer that could invoke an accept callback.
#[derive(Debug, Clone, Copy)]
pub struct ProducerJoins {
    pub session: JoinOutcome,
    pub ns: JoinOutcome,
}

impl ProducerJoins {
    /// No producers were ever started — nothing can call back.
    pub fn none_started() -> Self {
        Self { session: JoinOutcome::NotStarted, ns: JoinOutcome::NotStarted }
    }

    pub fn all_complete(self) -> bool {
        self.session.is_complete() && self.ns.is_complete()
    }
}

/// Abort and join one producer within `budget`, never re-polling a handle whose
/// result was already observed.
///
/// `already` carries the outcome if some earlier `select!` arm already drove
/// this handle to completion; a completed `tokio::task::JoinHandle` panics when
/// polled again, so this must not be bypassed.
pub async fn join_producer<T>(
    handle: Option<tokio::task::JoinHandle<T>>,
    already: Option<JoinOutcome>,
    budget: Duration,
) -> JoinOutcome {
    if let Some(o) = already {
        return o;
    }
    let Some(h) = handle else {
        return JoinOutcome::NotStarted;
    };
    h.abort();
    match tokio::time::timeout(budget, h).await {
        Ok(r) => JoinOutcome::from_join_result(&r),
        Err(_) => JoinOutcome::TimedOut,
    }
}

/// Arm the last-resort finalizer watchdog.
///
/// If the finalizer wedges — e.g. inside a synchronous sink write, which
/// `abort` cannot preempt — this exits the process with
/// [`EXIT_WATCHDOG`] so a matrix runner's `pkill` + `wait` cannot hang forever.
/// It deliberately does not touch the logger mutex, which may be the thing that
/// is stuck; every tx record was already flushed at write time, so the A3
/// no-truncation guarantee still holds.
///
/// Cancel by aborting the returned handle.
pub fn spawn_finalize_watchdog(budget: Duration) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        tokio::time::sleep(budget).await;
        eprintln!("[tx] FATAL: finalizer exceeded {budget:?}; exiting {EXIT_WATCHDOG}");
        std::process::exit(EXIT_WATCHDOG);
    })
}

/// Exit code: the finalizer could not complete honestly — a producer join
/// timed out or panicked, or the stats/shutdown record could not be written.
/// The run's instrumentation must not be trusted.
pub const EXIT_FINALIZE_FAILED: i32 = 2;

/// Exit code: the finalizer watchdog fired.
pub const EXIT_WATCHDOG: i32 = 3;

/// Exit code: a receiver drain task failed or panicked. Reception is incomplete.
pub const EXIT_RX_DRAIN_ERROR: i32 = 4;

/// Exit code: the receiver's session ended before both tracks were fully
/// drained. Distinct from a clean end-of-track FIN.
pub const EXIT_RX_SESSION_ENDED: i32 = 5;

/// Exit code: the receiver hit `--max-duration`. This is **incomplete
/// reception**, not a normal completion: the publisher never closed the tracks.
pub const EXIT_RX_TIMEOUT: i32 = 6;

/// Exit code: a track was cancelled and completeness could not be established.
/// `ServeError::Cancel` is the library's generic teardown state, so it cannot
/// be read as a clean end-of-track on its own.
pub const EXIT_RX_CANCELLED_INCOMPLETE: i32 = 7;

/// Exit code: a track that was expected to carry objects received exactly
/// zero. No loss rate explains this, so unlike a partial shortfall it is an
/// unambiguous failure of that subscription.
pub const EXIT_RX_NO_OBJECTS: i32 = 9;

/// Exit code: a received header failed validation (version, track identity, or
/// declared length). The rx log's measurement integrity is compromised.
pub const EXIT_RX_HEADER_INVALID: i32 = 8;

/// Destination for drained accept records.
///
/// Exists so the drain path is generic over the sink: production uses
/// `JsonlLogger`, tests inject a writer that fails, which is the only way to
/// exercise the I/O-error branch deterministically (A2-c R3/R5). Generic, not
/// `dyn`, so there is no dynamic dispatch and the tx logging path is untouched.
pub trait AcceptSink: Send + 'static {
    fn write_accept(&mut self, rec: &AcceptRec) -> std::io::Result<()>;
    fn flush_accept(&mut self) -> std::io::Result<()>;
}

impl AcceptSink for JsonlLogger {
    fn write_accept(&mut self, rec: &AcceptRec) -> std::io::Result<()> {
        self.log_accept(rec.track.as_str(), rec.group_id, rec.subgroup_id, rec.object_id, rec.t_accept, rec.size)
    }
    fn flush_accept(&mut self) -> std::io::Result<()> {
        self.try_flush()
    }
}

/// Owns the installed tap and its drain task, and provides the **bounded**
/// finalizer that every exit path in the sender goes through.
///
/// Shutdown order (A2-c R2 — producers first, then the tap):
///   1. the caller has already aborted **and joined** the session / namespace
///      tasks, so no forwarding future exists that could start a new callback;
///   2. wait for any callback still mid-body to leave (`in_flight` → 0) and for
///      the callback counter to hold steady — recorded as `producers_quiesced`;
///   3. close the tap, so the in-flight record set is finite;
///   4. tell the drain task to flush the remainder and join it;
///   5. return a snapshot including the conservation checks.
///
/// Every wait is deadline-bounded: a matrix run must never be able to wedge here.
pub struct AcceptTrace {
    counters: Arc<AcceptCounters>,
    capacity: usize,
    stop: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    handle: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    done: AtomicBool,
}

impl AcceptTrace {
    /// Install the process-global observer and spawn the drain task.
    ///
    /// Must be called before the MoQ session exists, so that no object can be
    /// forwarded before the observer is live.
    pub fn install<S: AcceptSink>(
        capacity: usize,
        sink: Arc<std::sync::Mutex<S>>,
    ) -> anyhow::Result<Arc<Self>> {
        let (tx, rx) = tokio::sync::mpsc::channel::<AcceptRec>(capacity);
        let counters = Arc::new(AcceptCounters::default());
        let tap = Arc::new(AcceptTap { tx, c: counters.clone() });
        moq_transport::accept_trace::set_observer(tap)
            .map_err(|_| anyhow::anyhow!("accept_trace observer already installed"))?;

        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(drain_task(rx, stop_rx, sink, counters.clone()));

        Ok(Arc::new(Self {
            counters,
            capacity,
            stop: std::sync::Mutex::new(Some(stop_tx)),
            handle: std::sync::Mutex::new(Some(handle)),
            done: AtomicBool::new(false),
        }))
    }

    /// Test-only constructor: same machinery, no global observer. Lets the unit
    /// tests drive the tap directly and run several instances in one process.
    #[cfg(test)]
    fn install_local<S: AcceptSink>(
        capacity: usize,
        sink: Arc<std::sync::Mutex<S>>,
    ) -> (Arc<Self>, Arc<AcceptTap>) {
        let (tx, rx) = tokio::sync::mpsc::channel::<AcceptRec>(capacity);
        let counters = Arc::new(AcceptCounters::default());
        let tap = Arc::new(AcceptTap { tx, c: counters.clone() });
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(drain_task(rx, stop_rx, sink, counters.clone()));
        (
            Arc::new(Self {
                counters,
                capacity,
                stop: std::sync::Mutex::new(Some(stop_tx)),
                handle: std::sync::Mutex::new(Some(handle)),
                done: AtomicBool::new(false),
            }),
            tap,
        )
    }

    /// Read the counters into a snapshot.
    ///
    /// **Read order is load-bearing.** The observer increments `callbacks`
    /// first and its exit counter last, so the snapshot must read the exit
    /// counters *first* and `callbacks` *last*. That guarantees
    /// `callbacks >= enqueued + dropped_full + dropped_closed` even while
    /// callbacks are still arriving: any callback included in an exit counter
    /// necessarily incremented `callbacks` earlier, hence before this read.
    ///
    /// Reading in the opposite order lets the snapshot see an exit without its
    /// callback and report a conservation violation that never happened — which
    /// is exactly what the straggler test caught.
    fn snapshot(&self, drain_complete: bool, producers_quiesced: bool, joins: ProducerJoins) -> AcceptSnapshot {
        let enqueued = self.counters.enqueued.load(Ordering::SeqCst);
        let dropped_full = self.counters.dropped_full.load(Ordering::SeqCst);
        let dropped_closed = self.counters.dropped_closed.load(Ordering::SeqCst);
        let written = self.counters.written.load(Ordering::SeqCst);
        let io_errors = self.counters.io_errors.load(Ordering::SeqCst);
        let in_flight = self.counters.in_flight.load(Ordering::SeqCst);
        // Last, for the ordering argument above.
        let callbacks = self.counters.callbacks.load(Ordering::SeqCst);
        AcceptSnapshot {
            callbacks,
            enqueued,
            written,
            dropped_full,
            dropped_closed,
            io_errors,
            in_flight,
            drain_complete,
            producers_quiesced,
            producers_joined: joins.all_complete(),
            session_join: joins.session,
            ns_join: joins.ns,
            capacity: self.capacity,
        }
    }

    /// Wait until no callback is mid-body and the callback count has stopped
    /// moving, or until `deadline`. Returns whether quiescence was observed.
    ///
    /// The caller must already have aborted and joined the producer tasks; this
    /// only confirms it, so that `producers_quiesced` is an observation rather
    /// than an assumption.
    async fn await_quiescence(&self, deadline: tokio::time::Instant) -> bool {
        const STABLE_SAMPLES: u32 = 3;
        let mut stable = 0u32;
        let mut last = self.counters.callbacks.load(Ordering::SeqCst);
        loop {
            tokio::time::sleep(Duration::from_millis(2)).await;
            let now = self.counters.callbacks.load(Ordering::SeqCst);
            let idle = self.counters.in_flight.load(Ordering::SeqCst) == 0;
            if idle && now == last {
                stable += 1;
                if stable >= STABLE_SAMPLES {
                    return true;
                }
            } else {
                stable = 0;
            }
            last = now;
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
        }
    }

    /// Quiesce producers, close the tap, drain, and join — all within `budget`.
    ///
    /// Idempotent: only the first caller gets `Some(snapshot)`. In the sender
    /// there is exactly one caller (the single finalizer in `main`), so this
    /// guard is defence in depth rather than the primary mechanism.
    pub async fn shutdown(&self, budget: Duration, joins: ProducerJoins) -> Option<AcceptSnapshot> {
        if self.done.swap(true, Ordering::SeqCst) {
            return None;
        }
        let deadline = tokio::time::Instant::now() + budget;

        // 2. Confirm producers have stopped. Bounded by a third of the budget:
        //    if they have not stopped, say so rather than waiting them out.
        let quiesce_deadline =
            std::cmp::min(deadline, tokio::time::Instant::now() + budget / 3);
        let producers_quiesced = self.await_quiescence(quiesce_deadline).await;

        // 3. No further enqueues: the record set is now finite.
        self.counters.closed.store(true, Ordering::SeqCst);

        // Any callback that passed the `closed` check before step 3 is still
        // bracketed by `in_flight`; let it finish its try_send so `enqueued`
        // is final before the drain task decides it is done.
        let straggler_deadline = std::cmp::min(
            deadline,
            tokio::time::Instant::now() + Duration::from_millis(50),
        );
        while self.counters.in_flight.load(Ordering::SeqCst) != 0 {
            if tokio::time::Instant::now() >= straggler_deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        // 4. Tell the drain task to flush what is left and exit.
        if let Some(tx) = self.stop.lock().ok().and_then(|mut g| g.take()) {
            let _ = tx.send(());
        }

        // Join under a hard guard. If the task is wedged inside the sink's
        // synchronous I/O, abort cannot preempt it, so the guard expiring means
        // "unknown", which `drain_complete=false` correctly reports.
        let handle = self.handle.lock().ok().and_then(|mut g| g.take());
        let mut joined = false;
        if let Some(mut h) = handle {
            let remaining = deadline
                .saturating_duration_since(tokio::time::Instant::now())
                + Duration::from_millis(500);
            match tokio::time::timeout(remaining, &mut h).await {
                Ok(Ok(())) => joined = true,
                Ok(Err(_)) => {} // drain task panicked
                Err(_) => {
                    h.abort();
                    let _ = tokio::time::timeout(Duration::from_millis(200), h).await;
                }
            }
        }

        // `drain_complete` means the drain task finished and *accounted for*
        // every enqueued record — written or failed. It must not be conflated
        // with "no I/O errors", which `io_errors` reports separately; folding
        // them together would hide which defect actually occurred.
        let written = self.counters.written.load(Ordering::SeqCst);
        let io_errors = self.counters.io_errors.load(Ordering::SeqCst);
        let enqueued = self.counters.enqueued.load(Ordering::SeqCst);
        let accounted = written + io_errors >= enqueued;
        Some(self.snapshot(joined && accounted, producers_quiesced, joins))
    }
}

/// Writes accept records to the JSONL file, off the transport path.
///
/// Batches so that the logger mutex — shared with the pc/haptic send loops — is
/// taken once per burst rather than once per record. The lock is never held
/// across an await.
async fn drain_task<S: AcceptSink>(
    mut rx: tokio::sync::mpsc::Receiver<AcceptRec>,
    stop_rx: tokio::sync::oneshot::Receiver<()>,
    sink: Arc<std::sync::Mutex<S>>,
    counters: Arc<AcceptCounters>,
) {
    const BATCH: usize = 256;
    let mut batch: Vec<AcceptRec> = Vec::with_capacity(BATCH);
    let mut stop_rx = stop_rx;

    // Phase 1: normal operation.
    let budget = loop {
        tokio::select! {
            biased;
            _ = &mut stop_rx => break Duration::from_secs(2),
            got = rx.recv() => match got {
                Some(first) => {
                    batch.push(first);
                    while batch.len() < BATCH {
                        match rx.try_recv() {
                            Ok(r) => batch.push(r),
                            Err(_) => break,
                        }
                    }
                    write_batch(&sink, &counters, &mut batch);
                }
                // Sender side gone entirely (tap dropped): nothing more can arrive.
                None => return,
            },
        }
    };

    // Phase 2: bounded final drain. `closed` and the straggler wait in
    // `shutdown` have already made `enqueued` final, so this only has to let
    // `written` (plus `io_errors`) account for it — never past the deadline.
    let deadline = tokio::time::Instant::now() + budget;
    let mut stable = 0u32;
    loop {
        while batch.len() < BATCH {
            match rx.try_recv() {
                Ok(r) => batch.push(r),
                Err(_) => break,
            }
        }
        if !batch.is_empty() {
            write_batch(&sink, &counters, &mut batch);
            continue;
        }
        // Accounted for iff every enqueued record was either written or failed.
        let accounted = counters.written.load(Ordering::SeqCst)
            + counters.io_errors.load(Ordering::SeqCst)
            >= counters.enqueued.load(Ordering::SeqCst);
        if accounted && counters.in_flight.load(Ordering::SeqCst) == 0 {
            // Require the queue to stay empty across consecutive polls rather
            // than returning on a single instantaneous observation, which was
            // the A2-c straggler hole.
            stable += 1;
            if stable >= 3 {
                return;
            }
        } else {
            stable = 0;
        }
        if tokio::time::Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Writes one batch, counting only records that the sink actually accepted.
///
/// A record is `written` only if both its write and the subsequent flush
/// succeeded. If the flush fails we cannot tell which records reached the file,
/// so the whole batch is charged to `io_errors` — deliberately conservative:
/// an I/O failure must never be able to make a run look intact.
fn write_batch<S: AcceptSink>(
    sink: &Arc<std::sync::Mutex<S>>,
    counters: &Arc<AcceptCounters>,
    batch: &mut Vec<AcceptRec>,
) {
    if batch.is_empty() {
        return;
    }
    // Tolerate a poisoned mutex: the records still belong in the file.
    let mut lg = match sink.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    let mut ok = 0u64;
    let mut failed = 0u64;
    for r in batch.drain(..) {
        match lg.write_accept(&r) {
            Ok(()) => ok += 1,
            Err(_) => failed += 1,
        }
    }
    let flush_failed = lg.flush_accept().is_err();
    drop(lg);

    if flush_failed {
        counters.io_errors.fetch_add(ok + failed, Ordering::SeqCst);
    } else {
        counters.written.fetch_add(ok, Ordering::SeqCst);
        counters.io_errors.fetch_add(failed, Ordering::SeqCst);
    }
}

// ---- A2-c R5: deterministic shutdown tests ---------------------------------
//
// These exist because the straggler and concurrent-shutdown branches were
// previously reasoned-correct but never executed. Each test *forces* its branch
// instead of hoping to observe it.

#[cfg(test)]
mod accept_shutdown_tests {
    use super::*;
    use moq_transport::accept_trace::{AcceptObserver, ObjectAccepted};
    use std::sync::atomic::AtomicUsize;

    /// Sink that records everything in memory and can be switched to failing.
    struct MemSink {
        lines: Vec<String>,
        fail_writes: bool,
        fail_flush: bool,
    }

    impl MemSink {
        fn new() -> Self {
            Self { lines: Vec::new(), fail_writes: false, fail_flush: false }
        }
    }

    impl AcceptSink for MemSink {
        fn write_accept(&mut self, rec: &AcceptRec) -> std::io::Result<()> {
            if self.fail_writes {
                return Err(std::io::Error::new(std::io::ErrorKind::Other, "injected write failure"));
            }
            self.lines.push(format!("{}:{}", rec.track.as_str(), rec.object_id));
            Ok(())
        }
        fn flush_accept(&mut self) -> std::io::Result<()> {
            if self.fail_flush {
                return Err(std::io::Error::new(std::io::ErrorKind::Other, "injected flush failure"));
            }
            Ok(())
        }
    }

    fn fire(tap: &Arc<AcceptTap>, object_id: u64) {
        tap.on_object_accepted(&ObjectAccepted::new("pc", 0, 0, 0, object_id, 100));
    }

    /// Baseline: everything written, all invariants hold, `intact` is true.
    #[tokio::test]
    async fn clean_shutdown_is_intact() {
        let sink = Arc::new(std::sync::Mutex::new(MemSink::new()));
        let (trace, tap) = AcceptTrace::install_local(1024, sink.clone());
        for i in 0..500 {
            fire(&tap, i);
        }
        let s = trace.shutdown(Duration::from_secs(2), ProducerJoins::none_started()).await.expect("first caller");
        assert_eq!(s.callbacks, 500);
        assert_eq!(s.written, 500, "every callback must reach the sink");
        assert_eq!(s.unwritten(), 0);
        assert_eq!(s.io_errors, 0);
        assert!(s.conservation_ok(), "R4 conservation: {s:?}");
        assert!(s.producers_quiesced);
        assert!(s.drain_complete);
        assert!(s.intact());
        assert_eq!(sink.lock().unwrap().lines.len(), 500);
    }

    /// R5 branch 1 — stragglers. A producer keeps firing callbacks *through*
    /// the shutdown, so callbacks land on both sides of the `closed` store.
    /// The point is not that nothing is dropped (drops are expected and
    /// correct here) but that the counters stay conserved and that a run with
    /// stragglers is never reported as intact.
    #[tokio::test]
    async fn straggler_callbacks_during_shutdown_are_accounted() {
        let sink = Arc::new(std::sync::Mutex::new(MemSink::new()));
        let (trace, tap) = AcceptTrace::install_local(4096, sink.clone());

        let stop = Arc::new(AtomicBool::new(false));
        let fired = Arc::new(AtomicUsize::new(0));
        // Producer on a separate blocking thread: it races the shutdown for
        // real rather than cooperatively yielding to it.
        let producer = {
            let tap = tap.clone();
            let stop = stop.clone();
            let fired = fired.clone();
            std::thread::spawn(move || {
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    fire(&tap, i);
                    fired.fetch_add(1, Ordering::Relaxed);
                    i += 1;
                    std::thread::yield_now();
                }
            })
        };

        // Let the producer get going, then shut down underneath it.
        tokio::time::sleep(Duration::from_millis(30)).await;
        let s = trace.shutdown(Duration::from_secs(2), ProducerJoins::none_started()).await.expect("first caller");
        stop.store(true, Ordering::Relaxed);
        producer.join().unwrap();

        assert!(s.callbacks > 0, "producer must have fired");
        // The conservation law is the real assertion: every callback took
        // exactly one exit, even under a live race.
        assert!(s.conservation_ok(), "R4 conservation under straggler race: {s:?}");
        assert!(
            s.callbacks >= s.exits(),
            "read order must guarantee callbacks >= exits even mid-race: {s:?}"
        );
        // Records the sink accepted must match the written counter exactly.
        assert_eq!(sink.lock().unwrap().lines.len() as u64, s.written);
        // Producers were demonstrably NOT quiesced, so the run must not claim
        // to be intact. This is the false-positive path Codex flagged.
        assert!(
            !s.intact(),
            "a run with callbacks still arriving must never report intact: {s:?}"
        );
    }

    /// R5 branch 1b — a callback that passes the `closed` check must still have
    /// its record drained. Forced by holding the observer mid-body via the
    /// in_flight bracket while shutdown runs.
    #[tokio::test]
    async fn callback_enqueued_just_before_close_is_still_written() {
        let sink = Arc::new(std::sync::Mutex::new(MemSink::new()));
        let (trace, tap) = AcceptTrace::install_local(1024, sink.clone());

        // Fire a burst, then immediately shut down without yielding, so records
        // are still sitting in the channel when `closed` is stored.
        for i in 0..300 {
            fire(&tap, i);
        }
        let s = trace.shutdown(Duration::from_secs(2), ProducerJoins::none_started()).await.expect("first caller");
        assert_eq!(s.enqueued, 300);
        assert_eq!(s.written, 300, "queued-at-close records must still be drained");
        assert_eq!(s.unwritten(), 0);
        assert!(s.intact(), "{s:?}");
        assert_eq!(sink.lock().unwrap().lines.len(), 300);
    }

    /// R5 branch 2 — concurrent shutdown. Exactly one caller may own the
    /// snapshot; the other must get `None` and must not be able to claim the
    /// stats record. In the sender there is only one caller by construction,
    /// but the guard must hold regardless.
    #[tokio::test]
    async fn concurrent_shutdown_yields_exactly_one_snapshot() {
        let sink = Arc::new(std::sync::Mutex::new(MemSink::new()));
        let (trace, tap) = AcceptTrace::install_local(1024, sink.clone());
        for i in 0..200 {
            fire(&tap, i);
        }

        let a = { let t = trace.clone(); tokio::spawn(async move { t.shutdown(Duration::from_secs(2), ProducerJoins::none_started()).await }) };
        let b = { let t = trace.clone(); tokio::spawn(async move { t.shutdown(Duration::from_secs(2), ProducerJoins::none_started()).await }) };
        let c = { let t = trace.clone(); tokio::spawn(async move { t.shutdown(Duration::from_secs(2), ProducerJoins::none_started()).await }) };
        let rs = vec![a.await.unwrap(), b.await.unwrap(), c.await.unwrap()];
        let some: Vec<_> = rs.into_iter().flatten().collect();
        assert_eq!(some.len(), 1, "exactly one caller may own the stats record");
        assert_eq!(some[0].written, 200);
        assert!(some[0].intact(), "{:?}", some[0]);
    }

    /// R3/R5 — injected write failure. `written` must not count records the
    /// sink rejected, and `intact` must be false.
    #[tokio::test]
    async fn write_errors_are_counted_and_break_intact() {
        let sink = Arc::new(std::sync::Mutex::new(MemSink::new()));
        sink.lock().unwrap().fail_writes = true;
        let (trace, tap) = AcceptTrace::install_local(1024, sink.clone());
        for i in 0..120 {
            fire(&tap, i);
        }
        let s = trace.shutdown(Duration::from_secs(2), ProducerJoins::none_started()).await.expect("first caller");
        assert_eq!(s.callbacks, 120);
        assert_eq!(s.enqueued, 120);
        assert_eq!(s.written, 0, "failed writes must never count as written");
        assert_eq!(s.io_errors, 120);
        // Accounted for as errors, so not "unwritten" (which means unaccounted).
        assert_eq!(s.unwritten(), 0);
        assert!(!s.intact(), "I/O failure must break intact: {s:?}");
        assert!(sink.lock().unwrap().lines.is_empty());
    }

    /// R3/R5 — injected flush failure. Writes "succeeded" into the buffer but
    /// the flush failed, so we cannot know what reached the file: the whole
    /// batch is charged to io_errors, never to written.
    #[tokio::test]
    async fn flush_errors_are_counted_and_break_intact() {
        let sink = Arc::new(std::sync::Mutex::new(MemSink::new()));
        sink.lock().unwrap().fail_flush = true;
        let (trace, tap) = AcceptTrace::install_local(1024, sink.clone());
        for i in 0..80 {
            fire(&tap, i);
        }
        let s = trace.shutdown(Duration::from_secs(2), ProducerJoins::none_started()).await.expect("first caller");
        assert_eq!(s.callbacks, 80);
        assert_eq!(s.written, 0, "unflushed records must not count as written");
        assert_eq!(s.io_errors, 80);
        assert!(!s.intact(), "flush failure must break intact: {s:?}");
    }

    /// Channel saturation must be attributed to `dropped_full`, keep the
    /// conservation law, and break `intact` — the instrumentation-defect case,
    /// as distinct from "QUIC never accepted the object".
    #[tokio::test]
    async fn channel_saturation_is_a_defect_not_a_finding() {
        let sink = Arc::new(std::sync::Mutex::new(MemSink::new()));
        let (trace, tap) = AcceptTrace::install_local(4, sink.clone());
        for i in 0..500 {
            fire(&tap, i);
        }
        let s = trace.shutdown(Duration::from_secs(2), ProducerJoins::none_started()).await.expect("first caller");
        assert_eq!(s.callbacks, 500);
        assert!(s.dropped_full > 0, "a 4-slot channel must overflow: {s:?}");
        assert!(s.conservation_ok(), "R4 conservation under overflow: {s:?}");
        assert!(!s.intact());
    }

    /// The snapshot must be stable: reading it twice cannot change the verdict,
    /// and a second shutdown cannot produce a second record.
    #[tokio::test]
    async fn shutdown_is_idempotent() {
        let sink = Arc::new(std::sync::Mutex::new(MemSink::new()));
        let (trace, tap) = AcceptTrace::install_local(1024, sink.clone());
        for i in 0..50 {
            fire(&tap, i);
        }
        assert!(trace.shutdown(Duration::from_secs(2), ProducerJoins::none_started()).await.is_some());
        assert!(trace.shutdown(Duration::from_secs(2), ProducerJoins::none_started()).await.is_none());
        assert!(trace.shutdown(Duration::from_secs(2), ProducerJoins::none_started()).await.is_none());
    }

    /// Callbacks arriving after the tap is closed are counted as
    /// `dropped_closed` — the case (b) accounting must remain exact.
    #[tokio::test]
    async fn post_close_callbacks_are_dropped_closed() {
        let sink = Arc::new(std::sync::Mutex::new(MemSink::new()));
        let (trace, tap) = AcceptTrace::install_local(1024, sink.clone());
        for i in 0..10 {
            fire(&tap, i);
        }
        let _ = trace.shutdown(Duration::from_secs(2), ProducerJoins::none_started()).await.expect("first caller");
        // Post-shutdown callbacks: the tap is closed, so these are counted.
        for i in 10..25 {
            fire(&tap, i);
        }
        let c = &trace.counters;
        assert_eq!(c.callbacks.load(Ordering::SeqCst), 25);
        assert_eq!(c.dropped_closed.load(Ordering::SeqCst), 15);
        assert_eq!(
            c.callbacks.load(Ordering::SeqCst),
            c.enqueued.load(Ordering::SeqCst)
                + c.dropped_full.load(Ordering::SeqCst)
                + c.dropped_closed.load(Ordering::SeqCst)
        );
    }

    /// The emitted stats body must contain every key the analyzer keys off, and
    /// must be identical in shape regardless of exit path.
    #[test]
    fn info_body_contains_the_documented_keys() {
        // Consistent-but-lossy: 10 callbacks = 8 enqueued + 1 full + 1 closed,
        // of which 7 written and 1 errored. Conservation holds, intact does not.
        let s = AcceptSnapshot {
            callbacks: 10, enqueued: 8, written: 7, dropped_full: 1,
            dropped_closed: 1, io_errors: 1, in_flight: 0, drain_complete: true,
            producers_quiesced: true, producers_joined: true,
            session_join: JoinOutcome::Cancelled, ns_join: JoinOutcome::Cancelled,
            capacity: 64,
        };
        let body = s.info_body();
        for k in [
            "\"event\":\"accept_trace\"", "accept_callbacks", "accept_enqueued",
            "accept_written", "accept_unwritten", "dropped_full", "dropped_closed",
            "io_errors", "drain_complete", "producers_quiesced", "conservation_ok",
            "accept_intact", "accept_capacity",
        ] {
            assert!(body.contains(k), "missing {k} in {body}");
        }
        assert!(s.conservation_ok(), "counters are self-consistent here");
        assert!(!s.intact(), "but records were lost, so not intact");
        assert_eq!(s.unwritten(), 0);
        assert!(body.contains("\"conservation_ok\":true"));
        assert!(body.contains("\"accept_intact\":false"));
    }

    /// Leg 1 of the conservation law must actually be able to fail: callbacks
    /// that do not decompose into the three exits are reported as inconsistent.
    #[test]
    fn conservation_leg1_detects_unaccounted_callbacks() {
        let s = AcceptSnapshot {
            callbacks: 10, enqueued: 8, written: 8, dropped_full: 0,
            dropped_closed: 0, io_errors: 0, in_flight: 0, drain_complete: true,
            producers_quiesced: true, producers_joined: true,
            session_join: JoinOutcome::Cancelled, ns_join: JoinOutcome::Cancelled,
            capacity: 64,
        };
        // 10 != 8 + 0 + 0 — two callbacks vanished without an exit.
        assert!(!s.conservation_ok());
        assert!(!s.intact());
        assert!(s.info_body().contains("\"conservation_ok\":false"));
    }

    /// Leg 2 must be able to fail: the drain cannot account for more records
    /// than were enqueued.
    #[test]
    fn conservation_leg2_detects_double_counting() {
        let s = AcceptSnapshot {
            callbacks: 8, enqueued: 8, written: 7, dropped_full: 0,
            dropped_closed: 0, io_errors: 3, in_flight: 0, drain_complete: true,
            producers_quiesced: true, producers_joined: true,
            session_join: JoinOutcome::Cancelled, ns_join: JoinOutcome::Cancelled,
            capacity: 64,
        };
        // 7 + 3 > 8 — a record was counted twice.
        assert!(!s.conservation_ok());
        assert!(!s.intact());
    }

    /// Producers still running at snapshot time must break `intact` even when
    /// every other counter looks perfect. This is Codex finding 1/2: the
    /// false-positive path where a clean-looking snapshot is taken too early.
    #[test]
    fn non_quiesced_producers_break_intact() {
        let s = AcceptSnapshot {
            callbacks: 100, enqueued: 100, written: 100, dropped_full: 0,
            dropped_closed: 0, io_errors: 0, in_flight: 0, drain_complete: true,
            producers_quiesced: false, producers_joined: true,
            session_join: JoinOutcome::Cancelled, ns_join: JoinOutcome::Cancelled,
            capacity: 64,
        };
        assert!(s.conservation_ok());
        assert!(!s.intact(), "unquiesced producers must not report intact");
    }

    /// An incomplete drain must break `intact` even with clean counters.
    #[test]
    fn incomplete_drain_breaks_intact() {
        let s = AcceptSnapshot {
            callbacks: 100, enqueued: 100, written: 100, dropped_full: 0,
            dropped_closed: 0, io_errors: 0, in_flight: 0, drain_complete: false,
            producers_quiesced: true, producers_joined: true,
            session_join: JoinOutcome::Cancelled, ns_join: JoinOutcome::Cancelled,
            capacity: 64,
        };
        assert!(!s.intact());
    }
}

// ---- A2-c R3: deterministic fault injection for the failure paths ----------
//
// The producer-join-timeout, drain-abort and watchdog branches were previously
// reasoned-correct but never executed, which is exactly where the remaining
// false-positive `accept_intact=true` risk lived. Each test below *forces* its
// branch. All injection lives in `#[cfg(test)]`; the production path is
// untouched.

#[cfg(test)]
mod fault_injection_tests {
    use super::*;
    use moq_transport::accept_trace::{AcceptObserver, ObjectAccepted};

    /// Sink whose `write_accept` blocks the calling thread, so the drain task
    /// cannot finish within its budget and `abort` cannot preempt it (abort
    /// only takes effect at an await point).
    struct BlockingSink {
        block: Duration,
    }

    impl AcceptSink for BlockingSink {
        fn write_accept(&mut self, _rec: &AcceptRec) -> std::io::Result<()> {
            std::thread::sleep(self.block);
            Ok(())
        }
        fn flush_accept(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn fire(tap: &Arc<AcceptTap>, object_id: u64) {
        tap.on_object_accepted(&ObjectAccepted::new("pc", 0, 0, 0, object_id, 100));
    }

    /// R3(a) — producer join timeout.
    ///
    /// The "producer" blocks its thread, so `abort()` cannot preempt it and the
    /// join budget expires. The handle is then detached, which is precisely the
    /// state where a quiescence observation must NOT be accepted as proof.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn producer_join_timeout_is_recorded_and_breaks_intact() {
        let stuck: tokio::task::JoinHandle<()> = tokio::spawn(async {
            std::thread::sleep(Duration::from_millis(1500));
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        let outcome = join_producer(Some(stuck), None, Duration::from_millis(100)).await;
        assert_eq!(outcome, JoinOutcome::TimedOut, "join must time out");
        assert!(!outcome.is_complete(), "a timed-out join is not complete");

        // A perfectly clean accept run must still be rejected, because the
        // producer could not be proven to have stopped.
        let sink = Arc::new(std::sync::Mutex::new(MemSinkLite::new()));
        let (trace, tap) = AcceptTrace::install_local(1024, sink.clone());
        for i in 0..50 {
            fire(&tap, i);
        }
        let joins = ProducerJoins { session: JoinOutcome::TimedOut, ns: JoinOutcome::Cancelled };
        let s = trace.shutdown(Duration::from_secs(2), joins).await.expect("first caller");

        assert_eq!(s.written, 50, "the accept side itself is clean");
        assert!(s.conservation_ok());
        assert!(s.producers_quiesced, "nothing is calling back, so it looks quiet");
        assert!(!s.producers_joined, "but the join failed");
        assert!(
            !s.intact(),
            "quiescence must not substitute for a successful join: {s:?}"
        );
        let body = s.info_body();
        assert!(body.contains("\"producers_joined\":false"));
        assert!(body.contains("\"session_join\":\"timed_out\""));
        assert!(body.contains("\"accept_intact\":false"));
    }

    /// A panicking producer is finished, but it is a defect and must be visible.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn producer_panic_is_recorded_and_breaks_intact() {
        let boom: tokio::task::JoinHandle<()> = tokio::spawn(async {
            panic!("injected producer panic");
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        let outcome = join_producer(Some(boom), None, Duration::from_millis(500)).await;
        assert_eq!(outcome, JoinOutcome::Panicked);
        assert!(!outcome.is_complete());

        let joins = ProducerJoins { session: JoinOutcome::Panicked, ns: JoinOutcome::Cancelled };
        assert!(!joins.all_complete());
    }

    /// The normal shutdown path aborts its producers; an aborted-and-joined
    /// task is a *successful* join, not a defect.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn aborted_producer_joins_cleanly() {
        let looper: tokio::task::JoinHandle<()> = tokio::spawn(async {
            loop {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });
        let outcome = join_producer(Some(looper), None, Duration::from_secs(2)).await;
        assert_eq!(outcome, JoinOutcome::Cancelled);
        assert!(outcome.is_complete(), "abort+join is the normal, successful path");
    }

    /// A handle already driven to completion elsewhere must never be re-polled
    /// (tokio panics on that). `join_producer` must return the recorded outcome
    /// without touching the handle.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn already_joined_handle_is_not_repolled() {
        let done: tokio::task::JoinHandle<u32> = tokio::spawn(async { 7 });
        let r = (&mut { done }).await; // consume it, as the error branch does
        let seen = JoinOutcome::from_join_result(&r);
        assert_eq!(seen, JoinOutcome::Completed);
        // Handle is gone; the finalizer must rely on the recorded outcome.
        let outcome = join_producer::<u32>(None, Some(seen), Duration::from_millis(50)).await;
        assert_eq!(outcome, JoinOutcome::Completed);
        assert!(outcome.is_complete());
    }

    /// R3(b) — drain abort.
    ///
    /// The sink blocks far longer than the drain budget, so the join guard
    /// expires, `abort()` cannot preempt the synchronous sleep, and the
    /// finalizer proceeds with `drain_complete=false`. The run must be
    /// rejected, and — critically — the finalizer must still return promptly.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn drain_abort_is_recorded_and_breaks_intact() {
        // 5 ms per record x a full 256-record batch = ~1.3 s of synchronous
        // work inside one `write_batch`, which has no await point. The drain
        // budget below is 100 ms, so the join guard expires while the batch is
        // still running and `abort()` cannot preempt it. Sized so the test
        // itself still finishes in ~1.3 s.
        let sink = Arc::new(std::sync::Mutex::new(BlockingSink {
            block: Duration::from_millis(5),
        }));
        let (trace, tap) = AcceptTrace::install_local(1024, sink.clone());
        for i in 0..600 {
            fire(&tap, i);
        }

        let t0 = std::time::Instant::now();
        let s = trace
            .shutdown(Duration::from_millis(100), ProducerJoins::none_started())
            .await
            .expect("first caller");
        let elapsed = t0.elapsed();

        assert!(
            !s.drain_complete,
            "a drain that overran its budget must not claim completion: {s:?}"
        );
        assert!(s.written < s.enqueued, "records were left unwritten: {s:?}");
        assert!(s.unwritten() > 0);
        assert!(!s.intact(), "an aborted drain must never report intact: {s:?}");
        assert!(
            elapsed < Duration::from_millis(1200),
            "shutdown must stay bounded, took {elapsed:?}"
        );
        assert!(s.info_body().contains("\"drain_complete\":false"));
    }

    /// R3(c) — watchdog.
    ///
    /// `spawn_finalize_watchdog` really calls `process::exit`, so it cannot be
    /// observed in-process. This test re-executes the test binary as a child,
    /// which arms the real watchdog and then wedges, and asserts the child's
    /// exit status is `EXIT_WATCHDOG`.
    #[test]
    fn watchdog_exits_with_code_3() {
        const TRIGGER: &str = "SKEW_TEST_WATCHDOG_CHILD";

        // Child role: arm the real watchdog, then block forever.
        if std::env::var(TRIGGER).is_ok() {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let _wd = spawn_finalize_watchdog(Duration::from_millis(200));
                // Wedge the finalizer exactly as a stuck synchronous sink would.
                std::thread::sleep(Duration::from_secs(30));
            });
            unreachable!("watchdog should have exited the process");
        }

        let exe = std::env::current_exe().expect("test binary path");
        let out = std::process::Command::new(exe)
            .args(["--exact", "fault_injection_tests::watchdog_exits_with_code_3"])
            .arg("--nocapture")
            .env(TRIGGER, "1")
            .output()
            .expect("spawn child");

        assert_eq!(
            out.status.code(),
            Some(EXIT_WATCHDOG),
            "watchdog must exit {EXIT_WATCHDOG}; stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("finalizer exceeded"),
            "watchdog must say why it fired"
        );
    }

    /// Minimal in-memory sink for the join tests.
    struct MemSinkLite {
        n: usize,
    }
    impl MemSinkLite {
        fn new() -> Self {
            Self { n: 0 }
        }
    }
    impl AcceptSink for MemSinkLite {
        fn write_accept(&mut self, _rec: &AcceptRec) -> std::io::Result<()> {
            self.n += 1;
            Ok(())
        }
        fn flush_accept(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
}
