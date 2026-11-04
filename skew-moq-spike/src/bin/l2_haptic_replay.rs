//! `l2_haptic_replay` — Experiment 4 stage 1 haptic L2 replay (NON-EVIDENCE).
//!
//! Governing documents: `md/20261104_실험4_계약.md` §7 (replay ledger
//! `exp4-replay-l2-v1`, trace schema `exp4-trace-v1`, stage-1 freeze, device
//! rule) and `md/20261104_실험4_설계초안.md` §3 (stage 1: V1 submit timing,
//! V2 R0 reproduction, V3 R1 cross-check). The trace comes from
//! `scripts/exp4_extract_traces.py`.
//!
//! What it does: replays the haptic submissions of ONE recorded Exp 2/3 L2
//! run (one trace file) into the EXISTING haptic sink and writes a replay
//! ledger. Nothing of the sink is re-implemented: the binary starts the same
//! output stage the receiver and `l2_alsa_probe` use
//! (`skew_moq::l2_output::L2Output`, PC lane `null`, haptic lane `alsa`), so
//! the lane thread, the placement engine (`skew_moq::l2_haptic`: admission,
//! fill, silence, accounting, sink mode A's `B`) and the row writer run
//! unchanged. Each tick goes through `L2Submitter::submit_payload` exactly
//! like the Exp 2/3 recorder's L2 link (`exp2_recorder.rs`): identity
//! (`tier = HAPTIC_TIER_FULL`, `route = None`), `t_release_us`, payload.
//!
//! Renderers (design §3-3):
//! * `R0` — the Exp 2 FIFO sink, no latency bound (`latency_bound_frames`
//!   null). Purpose `V2`.
//! * `R1` — sink mode A with `B` = 1,068 frames (the Exp 3 value,
//!   `2 × REGULAR_TICK_MAX_FRAMES`). Purpose `V3`.
//! * `R2` — the time-aligned placement (design §5, `PlacementMode::Aligned`,
//!   L2 `event-pair-l2-v1.8`) with `--align-offset-us <c>` (required) and
//!   `--write-ahead-frames <W>` (default 512, design D11). Purposes `V4a`
//!   (c₀ calibration) and `V4b` (fixed c). R2 reads each submission's
//!   `t_release_us`, which the replay passes as `replay_t0_us +
//!   t_release_rel_us` (the `scheduled_release_us` of the sidecar). Both
//!   flags are refused with R0 / R1, which stay the legacy engine (v1.7,
//!   byte-identical rows).
//!
//! Sink parameters are the library defaults (`ALSA_DEFAULT_*`: period 256,
//! buffer 2,048, target fill 512, prefill 512, queue limit 48,000, status
//! interval 1,000 µs; 48 kHz, ×6 upsample). The trace's recorded ALSA block
//! must equal them (refused otherwise). The output-stage capacities (queue
//! 256 per track, row queue 65,536, tracking 65,536) restate the receiver's
//! bin-private constants, which every Exp 2/3 L2 meta recorded.
//!
//! Inputs and refusals (all checked BEFORE anything is written; exit 2):
//! the trace schema tag and every field the replay uses (types exact, the
//! submit order, `t_release ≤ t_submit`, non-decreasing submit times, the
//! sender's identity rule `pts_us = floor(k·1e6/90)`, `event_id`, the
//! 88/89-sample length rule, `samples_8k_ledger` when present, counts,
//! `stream_epoch − t0 = stream_epoch_rel_t0_us`); the WAV's sha256 against
//! the trace's `wav_sha256`; the payloads regenerated with the sender rule
//! (`lib.rs` `load_haptic_pcm` + `pcm_tick_payload`, k = trace k) and their
//! concatenation sha256 in submit order against
//! `integrity.payload_concat_sha256`; the renderer ↔ purpose binding (V2 =
//! R0, V3 = R1); an existing output directory; `--device` without
//! `--i-have-approval` (the device is opened only after the stage-1 run
//! approval, contract §7), `--sim` together with `--device` (clap).
//!
//! Timing (integer µs, CLOCK_MONOTONIC, `skew_moq::now_us`): the stage is
//! started (the lane opens, prefills 512 frames, starts the stream and only
//! then the meta row is written), the replay's own
//! `alsa.stream_epoch.t_start_us` is read back from that meta row, and
//! `replay_t0_us = t_start_us − trace.stream_epoch_rel_t0_us.t_start_us`:
//! the stream starts at exactly the source's offset from t0 by
//! construction (the other epoch fields' residuals are recorded). Until the
//! first submit the lane writes silence by its own rule. Tick `i` (trace
//! submit order; ties keep that order) is due at `replay_t0_us +
//! t_submit_rel_us`: the main thread sleeps with
//! `clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME)` until then (never early),
//! reads `t_call_us`, and calls `submit_payload` with `t_release_us =
//! replay_t0_us + t_release_rel_us` (carried, unused by R0/R1, the alignment
//! target of R2); the engine
//! reads its own `t_submit_us` inside that call. Submit lateness (stage-1 V1)
//! = ledger `t_submit_us − due`; `t_call_us − due` is kept too.
//!
//! Close: after the last submit the producer handle is dropped and
//! `L2Output::finish` runs the 1-D close (deadline = finish start +
//! `CLOSE_GRACE_US`); the lane ends as soon as the last tick's tail played
//! (idle). The grace exceeds a full queue plus the device buffer, so every
//! admitted tick can play out. The source run's close deadline is NOT in the
//! trace, so it is not reproduced.
//!
//! Output directory `--out` (created here, must not exist; parent must):
//! * `l2.jsonl` — the engine's L2 ledger exactly as the writer wrote it
//!   (meta row first, `instrumentation_end` last; schema tag
//!   [`L2_SCHEMA`], FIFO rows: submit / output / explicit_discard with the
//!   existing `output_detail`).
//! * `replay_submits.jsonl` (`exp4-replay-submits-v1`) — one header row and
//!   one row per trace tick: schedule, `t_call_us`, outcome, the ledger's
//!   `t_submit_us` and both latenesses.
//! * `replay.jsonl` — THE replay ledger `exp4-replay-l2-v1`: one head row
//!   `replay_meta`, then the bytes of `l2.jsonl` verbatim. It is composed
//!   after the run (the engine creates its file with `create_new` and writes
//!   its meta row first), so `replay_meta` also carries the sha256 of both
//!   other files and the outcome.
//!
//! Exit: 0 valid; 3 run recorded but invalid (XRUN or any other integrity
//! failure — e.g. `FAIL_ALSA_*` — a tick not submitted, terminals ≠
//! submits, submit order broken, schedule unreachable); 2 refused (nothing
//! written, or for an existing directory nothing touched); 1 I/O or stage
//! error.
//!
//! Known limits (reported, not hidden): the replay ledger's L2 meta is the
//! engine's tag for the renderer (R0/R1: v1.7 rows, the `placement` fields
//! are given in `replay_meta` only; R2: v1.8 with `alsa.placement` aligned,
//! placement and trim rows); the trace has no tier / route, so rows
//! carry `tier = 0` and no route; the source's pre-submit discards cannot
//! become engine rows and are carried in `replay_meta`; the simulated PCM
//! runs on the real clock (a replay takes the trace's real duration).

// The replay_meta row is one large `json!` literal.
#![recursion_limit = "256"]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::{ArgGroup, Parser, ValueEnum};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use skew_moq::l2_haptic::{
    PlacementConfig, PlacementMode, SimFault, SimPcmConfig, PCM_RATE_HZ, REGULAR_TICK_MAX_FRAMES, UPSAMPLE_FACTOR,
};
use skew_moq::l2_output::{
    AlsaBackend, AlsaSinkConfig, L2Config, L2Identity, L2Modality, L2Output, L2SinkKind, L2Sinks, L2SubmitOutcome,
    ALSA_DEFAULT_BUFFER_FRAMES, ALSA_DEFAULT_PERIOD_FRAMES, ALSA_DEFAULT_PREFILL_FRAMES, ALSA_DEFAULT_QUEUE_FRAMES,
    ALSA_DEFAULT_STATUS_INTERVAL_US, ALSA_DEFAULT_TARGET_FILL_FRAMES, ALSA_READY_TIMEOUT_US, ALSA_RELEASE_TIMEOUT_US,
    DISCARD_OCCLUDED, L2_SCHEMA,
};
use skew_moq::{
    load_haptic_pcm, now_us, pcm_sample_bounds, pcm_tick_payload, timestamp_us, HAPTIC_RATE_HZ_V5, HAPTIC_TIER_FULL,
    PCM_BYTES_PER_SAMPLE, PCM_SAMPLE_RATE_HZ, PC_RATE_HZ_V5,
};

const TRACE_SCHEMA: &str = "exp4-trace-v1";
const REPLAY_SCHEMA: &str = "exp4-replay-l2-v1";
const SUBMITS_SCHEMA: &str = "exp4-replay-submits-v1";
const CONTRACT: &str = "exp4-contract-v1";
/// Contract §1: replay run ids start with `exp4r-`.
const RUN_ID_PREFIX: &str = "exp4r-";
/// L2 meta `method`: not an arm (cf. `l2_alsa_probe`'s `probe`).
const L2_METHOD: &str = "replay";
const HAPTIC_RATE_HZ: u64 = HAPTIC_RATE_HZ_V5;
const PC_RATE_HZ: u64 = PC_RATE_HZ_V5;
/// Registered haptic opportunities per run (40 s × 90 Hz; extractor `N_HAPTIC`).
const N_HAPTIC: u64 = 3_600;
/// Sink mode A's `B` for R1: the Exp 3 value (`md/20261103_실험3_계약.md`
/// freeze `l2_alsa_sink`; receiver `--l2-alsa-latency-bound-frames 1068`).
const R1_LATENCY_BOUND_FRAMES: u64 = 1_068;
const _: () = assert!(R1_LATENCY_BOUND_FRAMES == 2 * REGULAR_TICK_MAX_FRAMES);
/// R2's registered write-ahead W (design §5-3 D11: W = F = 512 frames).
const R2_DEFAULT_WRITE_AHEAD_FRAMES: u64 = 512;
/// The receiver's output-stage capacities (`moq_receiver.rs`
/// `L2_DEFAULT_QUEUE_CAPACITY`, `L2_ROW_CAPACITY`, `L2_MAX_TRACKED_OBJECTS`;
/// bin-private there, recorded in every Exp 2/3 L2 meta).
const L2_QUEUE_CAPACITY: usize = 256;
const L2_ROW_CAPACITY: usize = 65_536;
const L2_MAX_TRACKED_OBJECTS: usize = 65_536;
/// Close grace of the replay (an upper bound: the lane ends once idle).
const CLOSE_GRACE_US: u64 = 3_000_000;
const _: () = assert!(
    CLOSE_GRACE_US
        > (ALSA_DEFAULT_QUEUE_FRAMES + ALSA_DEFAULT_BUFFER_FRAMES) * 1_000_000 / PCM_RATE_HZ
            + 10 * ALSA_DEFAULT_STATUS_INTERVAL_US
);
/// The `*_us` fields of `alsa.stream_epoch` (the extractor's rel block).
const EPOCH_US_FIELDS: [&str; 4] = ["t_start_us", "trigger_htstamp_us", "first_status_t0_us", "first_status_htstamp_us"];
const ANCHOR_FIELD: &str = "t_start_us";
const L2_FILE: &str = "l2.jsonl";
const SUBMITS_FILE: &str = "replay_submits.jsonl";
const REPLAY_FILE: &str = "replay.jsonl";

// ---------------------------------------------------------------- refusals

/// A refusal: the replay does not run (exit 2).
#[derive(Debug)]
struct Refusal(String);

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Refusal {}

fn refusal(msg: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(Refusal(msg.into()))
}

macro_rules! need {
    ($cond:expr, $($arg:tt)*) => {
        if !$cond {
            return Err(refusal(format!($($arg)*)));
        }
    };
}

fn is_refusal(error: &anyhow::Error) -> bool {
    error.downcast_ref::<Refusal>().is_some()
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

// ---------------------------------------------------------------- strict JSON access

fn as_obj<'a>(v: &'a Value, what: &str) -> Result<&'a Map<String, Value>> {
    v.as_object().ok_or_else(|| refusal(format!("{what} must be an object")))
}

fn get<'a>(m: &'a Map<String, Value>, key: &str, what: &str) -> Result<&'a Value> {
    m.get(key).ok_or_else(|| refusal(format!("{what}: field {key:?} is missing")))
}

fn get_obj<'a>(m: &'a Map<String, Value>, key: &str, what: &str) -> Result<&'a Map<String, Value>> {
    as_obj(get(m, key, what)?, &format!("{what}.{key}"))
}

fn get_arr<'a>(m: &'a Map<String, Value>, key: &str, what: &str) -> Result<&'a Vec<Value>> {
    get(m, key, what)?.as_array().ok_or_else(|| refusal(format!("{what}.{key} must be an array")))
}

/// A JSON integer >= 0 (never a bool, never a float).
fn get_u64(m: &Map<String, Value>, key: &str, what: &str) -> Result<u64> {
    match get(m, key, what)? {
        Value::Number(n) if n.is_u64() => Ok(n.as_u64().expect("is_u64")),
        other => Err(refusal(format!("{what}.{key} must be a non-negative integer, got {other}"))),
    }
}

/// A JSON integer that fits i64 (never a bool, never a float).
fn get_i64(m: &Map<String, Value>, key: &str, what: &str) -> Result<i64> {
    match get(m, key, what)? {
        Value::Number(n) if (n.is_i64() || n.is_u64()) && n.as_i64().is_some() => Ok(n.as_i64().expect("checked")),
        other => Err(refusal(format!("{what}.{key} must be an integer, got {other}"))),
    }
}

/// Present, and null or a non-negative integer.
fn get_opt_u64(m: &Map<String, Value>, key: &str, what: &str) -> Result<Option<u64>> {
    match get(m, key, what)? {
        Value::Null => Ok(None),
        _ => get_u64(m, key, what).map(Some),
    }
}

fn get_str<'a>(m: &'a Map<String, Value>, key: &str, what: &str) -> Result<&'a str> {
    get(m, key, what)?.as_str().ok_or_else(|| refusal(format!("{what}.{key} must be a string")))
}

/// Present, and null or a string.
fn get_opt_str(m: &Map<String, Value>, key: &str, what: &str) -> Result<Option<String>> {
    match get(m, key, what)? {
        Value::Null => Ok(None),
        Value::String(s) => Ok(Some(s.clone())),
        other => Err(refusal(format!("{what}.{key} must be null or a string, got {other}"))),
    }
}

fn get_sha(m: &Map<String, Value>, key: &str, what: &str) -> Result<String> {
    let s = get_str(m, key, what)?;
    need!(
        s.len() == 64 && s.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "{what}.{key} must be a 64-hex lowercase sha256, got {s:?}"
    );
    Ok(s.to_string())
}

// ---------------------------------------------------------------- the trace (exp4-trace-v1)

#[derive(Debug, Clone, PartialEq, Eq)]
struct TraceTick {
    k: u64,
    pts_us: u64,
    event_id: u32,
    t_release_rel_us: i64,
    t_submit_rel_us: i64,
    submit_order: u64,
    samples_8k_expected: u64,
    samples_8k_ledger: Option<u64>,
    ledger_terminal: Option<String>,
}

#[derive(Debug, Clone)]
struct Trace {
    path: PathBuf,
    sha256: String,
    set: String,
    experiment: String,
    run_id: String,
    method: String,
    source_input_sha256: String,
    source_ledger_sha256: String,
    t0_us: u64,
    /// `stream_epoch_rel_t0_us` (the four `*_us` fields).
    rel_epoch: BTreeMap<&'static str, i64>,
    wav_sha256: String,
    wav_total_samples: u64,
    source_latency_bound_frames: Option<u64>,
    ticks: Vec<TraceTick>,
    pre_submit_discards: Vec<Value>,
    payload_concat_sha256: String,
}

/// 8 kHz samples of tick `k` by the sender's boundary rule.
fn samples_8k_rule(k: u64) -> u64 {
    let (start, end) = pcm_sample_bounds(k, PCM_SAMPLE_RATE_HZ, HAPTIC_RATE_HZ);
    end - start
}

/// The sender's identity of haptic tick `k` (`moq_sender.rs`: anchor ticks
/// carry the PC frame's event id `frame + 1`, the others 0).
fn identity_rule(k: u64) -> (u64, u32) {
    let ratio = HAPTIC_RATE_HZ / PC_RATE_HZ;
    let event_id = if k % ratio == 0 { (k / ratio + 1) as u32 } else { 0 };
    (timestamp_us(k, HAPTIC_RATE_HZ), event_id)
}

fn load_trace(path: &Path) -> Result<Trace> {
    let bytes = fs::read(path).map_err(|e| refusal(format!("trace {}: cannot read ({e})", path.display())))?;
    let sha256 = sha256_hex(&bytes);
    let doc: Value =
        serde_json::from_slice(&bytes).map_err(|e| refusal(format!("trace {}: not JSON ({e})", path.display())))?;
    let root = as_obj(&doc, "trace")?;
    let schema = get_str(root, "schema", "trace")?;
    need!(schema == TRACE_SCHEMA, "trace schema {schema:?} != {TRACE_SCHEMA:?}");
    let run_id = get_str(root, "run_id", "trace")?.to_string();
    need!(
        !run_id.is_empty() && run_id.bytes().all(|b| b.is_ascii_graphic()),
        "trace run_id {run_id:?} must be non-empty printable ASCII"
    );
    let set = get_str(root, "set", "trace")?.to_string();
    let experiment = get_str(root, "experiment", "trace")?.to_string();
    let method = get_str(root, "method", "trace")?.to_string();
    let source_input_sha256 = get_sha(root, "source_input_sha256", "trace")?;
    let source_ledger_sha256 = get_sha(root, "source_ledger_sha256", "trace")?;
    let t0_us = get_u64(root, "t0_us", "trace")?;

    // Stream epoch: absolute values and their offsets from t0 must agree.
    let epoch = get_obj(root, "stream_epoch", "trace")?;
    let rel = get_obj(root, "stream_epoch_rel_t0_us", "trace")?;
    let mut rel_epoch = BTreeMap::new();
    for field in EPOCH_US_FIELDS {
        let r = get_i64(rel, field, "trace.stream_epoch_rel_t0_us")?;
        let abs = get_u64(epoch, field, "trace.stream_epoch")?;
        need!(
            abs as i128 - t0_us as i128 == r as i128,
            "trace stream_epoch.{field} {abs} - t0_us {t0_us} != stream_epoch_rel_t0_us.{field} {r}"
        );
        rel_epoch.insert(field, r);
    }
    let prefill = get_u64(epoch, "prefill_frames", "trace.stream_epoch")?;
    need!(
        prefill == ALSA_DEFAULT_PREFILL_FRAMES,
        "trace stream_epoch.prefill_frames {prefill} != the replay's {ALSA_DEFAULT_PREFILL_FRAMES}"
    );

    // WAV binding and payload rule.
    let wav_sha256 = get_sha(root, "wav_sha256", "trace")?;
    let wav = get_obj(root, "wav", "trace")?;
    let wav_block_sha = get_sha(wav, "sha256", "trace.wav")?;
    need!(wav_block_sha == wav_sha256, "trace wav.sha256 {wav_block_sha} != wav_sha256 {wav_sha256}");
    let wav_total_samples = get_u64(wav, "total_samples", "trace.wav")?;
    need!(wav_total_samples > 0, "trace wav.total_samples is 0");
    let rate = get_u64(wav, "sample_rate_hz", "trace.wav")?;
    let hz = get_u64(wav, "haptic_rate_hz", "trace.wav")?;
    need!(
        rate == PCM_SAMPLE_RATE_HZ && hz == HAPTIC_RATE_HZ,
        "trace wav rule {rate} Hz / {hz} Hz != the sender's {PCM_SAMPLE_RATE_HZ} / {HAPTIC_RATE_HZ}"
    );

    // The source sink must be the one the replay runs (Exp 2/3 parameters).
    let alsa = get_obj(root, "alsa", "trace")?;
    for (key, want) in [
        ("period_frames", ALSA_DEFAULT_PERIOD_FRAMES),
        ("buffer_frames", ALSA_DEFAULT_BUFFER_FRAMES),
        ("target_fill_frames", ALSA_DEFAULT_TARGET_FILL_FRAMES),
        ("prefill_frames", ALSA_DEFAULT_PREFILL_FRAMES),
        ("queue_limit_frames", ALSA_DEFAULT_QUEUE_FRAMES),
        ("rate_hz", PCM_RATE_HZ),
    ] {
        let got = get_u64(alsa, key, "trace.alsa")?;
        need!(got == want, "trace alsa.{key} = {got} != the replay sink's {want} (Exp 2/3 sink parameters)");
    }
    let source_latency_bound_frames = get_opt_u64(alsa, "latency_bound_frames", "trace.alsa")?;

    // Ticks, in submit order.
    let raw_ticks = get_arr(root, "ticks", "trace")?;
    need!(!raw_ticks.is_empty(), "trace has no tick");
    let mut ticks = Vec::with_capacity(raw_ticks.len());
    let mut seen = HashSet::new();
    let mut prev_submit: Option<i64> = None;
    for (i, raw) in raw_ticks.iter().enumerate() {
        let what = format!("trace.ticks[{i}]");
        let m = as_obj(raw, &what)?;
        let k = get_u64(m, "k", &what)?;
        let pts_us = get_u64(m, "pts_us", &what)?;
        let event_id = get_u64(m, "event_id", &what)?;
        let t_release_rel_us = get_i64(m, "t_release_rel_us", &what)?;
        let t_submit_rel_us = get_i64(m, "t_submit_rel_us", &what)?;
        let submit_order = get_u64(m, "submit_order", &what)?;
        let samples_8k_expected = get_u64(m, "samples_8k_expected", &what)?;
        let samples_8k_ledger = get_opt_u64(m, "samples_8k_ledger", &what)?;
        let ledger_terminal = get_opt_str(m, "ledger_terminal", &what)?;
        need!(submit_order == i as u64, "{what}.submit_order {submit_order} != its position {i}");
        need!(k < N_HAPTIC, "{what}.k {k} outside [0, {N_HAPTIC})");
        need!(seen.insert(k), "{what}: k {k} appears twice");
        let (want_pts, want_event) = identity_rule(k);
        need!(
            pts_us == want_pts && event_id == u64::from(want_event),
            "{what}: (pts_us, event_id) ({pts_us}, {event_id}) != the sender rule ({want_pts}, {want_event}) for k {k}"
        );
        need!(
            t_release_rel_us <= t_submit_rel_us,
            "{what}: t_release_rel_us {t_release_rel_us} > t_submit_rel_us {t_submit_rel_us}"
        );
        if let Some(prev) = prev_submit {
            need!(t_submit_rel_us >= prev, "{what}: t_submit_rel_us {t_submit_rel_us} < the previous {prev}");
        }
        prev_submit = Some(t_submit_rel_us);
        need!(
            samples_8k_expected == samples_8k_rule(k),
            "{what}: samples_8k_expected {samples_8k_expected} != the boundary rule {}",
            samples_8k_rule(k)
        );
        if let Some(ledger) = samples_8k_ledger {
            need!(ledger == samples_8k_expected, "{what}: samples_8k_ledger {ledger} != expected {samples_8k_expected}");
        }
        ticks.push(TraceTick {
            k,
            pts_us,
            event_id: event_id as u32,
            t_release_rel_us,
            t_submit_rel_us,
            submit_order,
            samples_8k_expected,
            samples_8k_ledger,
            ledger_terminal,
        });
    }
    let start_rel = rel_epoch[ANCHOR_FIELD];
    need!(
        ticks[0].t_submit_rel_us > start_rel,
        "trace: the first submit ({} us from t0) is not after the stream start ({start_rel} us from t0)",
        ticks[0].t_submit_rel_us
    );

    let pre_submit_discards = get_arr(root, "pre_submit_discards", "trace")?.clone();
    let integrity = get_obj(root, "integrity", "trace")?;
    let haptic_submits = get_u64(integrity, "haptic_submits", "trace.integrity")?;
    need!(
        haptic_submits == ticks.len() as u64,
        "trace integrity.haptic_submits {haptic_submits} != {} ticks",
        ticks.len()
    );
    let n_pre = get_u64(integrity, "pre_submit_discards", "trace.integrity")?;
    need!(
        n_pre == pre_submit_discards.len() as u64,
        "trace integrity.pre_submit_discards {n_pre} != {} listed",
        pre_submit_discards.len()
    );
    let payload_concat_sha256 = get_sha(integrity, "payload_concat_sha256", "trace.integrity")?;
    Ok(Trace {
        path: path.to_path_buf(),
        sha256,
        set,
        experiment,
        run_id,
        method,
        source_input_sha256,
        source_ledger_sha256,
        t0_us,
        rel_epoch,
        wav_sha256,
        wav_total_samples,
        source_latency_bound_frames,
        ticks,
        pre_submit_discards,
        payload_concat_sha256,
    })
}

// ---------------------------------------------------------------- WAV and payloads

/// The WAV's PCM (the sender's loader), bound to `expected_sha256`. The file
/// is hashed before and after the loader read it.
fn load_wav(path: &Path, expected_sha256: &str, total_samples: u64) -> Result<Vec<u8>> {
    let read_sha = |stage: &str| -> Result<String> {
        let bytes = fs::read(path).map_err(|e| refusal(format!("haptic WAV {}: cannot read ({e})", path.display())))?;
        let sha = sha256_hex(&bytes);
        need!(
            sha == expected_sha256,
            "haptic WAV {} sha256 {sha} != the trace's wav_sha256 {expected_sha256} ({stage})",
            path.display()
        );
        Ok(sha)
    };
    read_sha("before loading")?;
    let text = path.to_str().ok_or_else(|| refusal(format!("haptic WAV path {} is not UTF-8", path.display())))?;
    let pcm = load_haptic_pcm(text).map_err(|e| refusal(format!("haptic WAV {}: {e:#}", path.display())))?;
    read_sha("after loading")?;
    need!(
        pcm.len() % PCM_BYTES_PER_SAMPLE == 0 && (pcm.len() / PCM_BYTES_PER_SAMPLE) as u64 == total_samples,
        "haptic WAV {}: {} PCM bytes != the trace's wav.total_samples {total_samples}",
        path.display(),
        pcm.len()
    );
    Ok(pcm)
}

/// Each tick's payload by the sender rule, and the sha256 of their
/// concatenation in submit order.
fn regenerate_payloads(ticks: &[TraceTick], pcm: &[u8]) -> Result<(Vec<Bytes>, String)> {
    let mut hasher = Sha256::new();
    let mut payloads = Vec::with_capacity(ticks.len());
    for t in ticks {
        let payload = pcm_tick_payload(pcm, t.k, PCM_SAMPLE_RATE_HZ, HAPTIC_RATE_HZ)
            .map_err(|e| refusal(format!("payload of k {}: {e:#}", t.k)))?;
        need!(
            payload.len() as u64 == t.samples_8k_expected * PCM_BYTES_PER_SAMPLE as u64,
            "payload of k {}: {} bytes != 2 x samples_8k_expected {}",
            t.k,
            payload.len(),
            t.samples_8k_expected
        );
        hasher.update(&payload);
        payloads.push(Bytes::from(payload));
    }
    Ok((payloads, format!("{:x}", hasher.finalize())))
}

// ---------------------------------------------------------------- request

/// `--renderer` (the R2 parameters come from their own flags).
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum RendererArg {
    /// The Exp 2 FIFO sink (no latency bound).
    #[value(name = "R0")]
    R0,
    /// Sink mode A, B = 1,068 frames (Exp 3).
    #[value(name = "R1")]
    R1,
    /// The time-aligned placement (design §5): needs --align-offset-us.
    #[value(name = "R2")]
    R2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Renderer {
    /// The Exp 2 FIFO sink (no latency bound).
    R0,
    /// Sink mode A, B = 1,068 frames (Exp 3).
    R1,
    /// The time-aligned placement: c (µs) and W (frames).
    R2 { align_offset_us: i64, write_ahead_frames: u64 },
}

impl Renderer {
    fn as_str(self) -> &'static str {
        match self {
            Self::R0 => "R0",
            Self::R1 => "R1",
            Self::R2 { .. } => "R2",
        }
    }

    fn latency_bound_frames(self) -> Option<u64> {
        match self {
            Self::R0 | Self::R2 { .. } => None,
            Self::R1 => Some(R1_LATENCY_BOUND_FRAMES),
        }
    }

    /// R0 / R1: the legacy engine (v1.7); R2: aligned (v1.8).
    fn placement_mode(self) -> PlacementMode {
        match self {
            Self::R0 | Self::R1 => PlacementMode::None,
            Self::R2 { align_offset_us, write_ahead_frames } => PlacementMode::Aligned { align_offset_us, write_ahead_frames },
        }
    }

    fn aligned(self) -> Option<(i64, u64)> {
        self.placement_mode().aligned()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Purpose {
    /// R0 reproduction (design §3-4 V2).
    #[value(name = "V2")]
    V2,
    /// R1 cross-check against live P0-A (design §3-4 V3).
    #[value(name = "V3")]
    V3,
    /// R2 floor calibration at c₀ (design §3-4 V4a, §5-4).
    #[value(name = "V4a")]
    V4a,
    /// R2 at the fixed c (design §3-4 V4b).
    #[value(name = "V4b")]
    V4b,
}

impl Purpose {
    fn as_str(self) -> &'static str {
        match self {
            Self::V2 => "V2",
            Self::V3 => "V3",
            Self::V4a => "V4a",
            Self::V4b => "V4b",
        }
    }

    /// The renderer this purpose replays with (contract §7).
    fn renderer_name(self) -> &'static str {
        match self {
            Self::V2 => "R0",
            Self::V3 => "R1",
            Self::V4a | Self::V4b => "R2",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Backend {
    /// The real device (opened only with --i-have-approval).
    Device(String),
    Sim,
}

#[derive(Debug, Clone)]
struct ReplayRequest {
    trace: PathBuf,
    wav: PathBuf,
    renderer: Renderer,
    purpose: Purpose,
    out: PathBuf,
    backend: Backend,
    /// Unit-test knob for the simulated stream (e.g. an injected XRUN). The
    /// CLI never sets it; it is recorded in `replay_meta.sim_faults`.
    sim_faults: Vec<SimFault>,
}

#[derive(Parser, Debug)]
#[command(
    name = "l2_haptic_replay",
    about = "Experiment 4 stage 1 haptic L2 replay (non-evidence) through the haptic ALSA sink (R0/R1 legacy, R2 aligned)"
)]
#[command(group(ArgGroup::new("pcm").required(true).args(["device", "sim"])))]
struct Cli {
    /// The `exp4-trace-v1` trace file.
    #[arg(long)]
    trace: PathBuf,
    #[arg(long, value_enum)]
    renderer: RendererArg,
    /// Stage-1 purpose: V2 (R0 reproduction), V3 (R1 cross-check), V4a / V4b (R2).
    #[arg(long, value_enum)]
    purpose: Purpose,
    /// R2 only, required: the aligned placement offset c (µs, >= 0).
    #[arg(long)]
    align_offset_us: Option<i64>,
    /// R2 only: the write-ahead W (frames; default 512).
    #[arg(long)]
    write_ahead_frames: Option<u64>,
    /// Output directory to create (must not exist; its parent must).
    #[arg(long)]
    out: PathBuf,
    /// The haptic WAV (the sender's `--haptic-wav`); its sha256 must equal the trace's `wav_sha256`.
    #[arg(long)]
    haptic_wav: PathBuf,
    /// Live run on this ALSA device (e.g. `hw:1,0`); needs --i-have-approval.
    #[arg(long, conflicts_with = "sim")]
    device: Option<String>,
    /// Simulated PCM (never a device output).
    #[arg(long)]
    sim: bool,
    /// Required with --device (refused without one): the stage-1 run was
    /// approved and the card is reserved for this process
    /// (`pw-reserve -n Audio1 -r`, as for `l2_alsa_probe`).
    #[arg(long)]
    i_have_approval: bool,
}

impl Cli {
    fn request(&self) -> Result<ReplayRequest> {
        let backend = match (&self.device, self.sim) {
            (Some(name), false) => {
                need!(
                    self.i_have_approval,
                    "--device {name} opens the real PCM device: only after the stage-1 run approval (contract §7), \
                     with the card reserved for this process (`pw-reserve -n Audio1 -r &` ... `kill $!`), and with \
                     --i-have-approval"
                );
                need!(!name.is_empty() && !name.contains('\0'), "--device must be a non-empty ALSA name");
                Backend::Device(name.clone())
            }
            (None, true) => {
                need!(!self.i_have_approval, "--i-have-approval applies only to --device");
                Backend::Sim
            }
            _ => return Err(refusal("exactly one of --device and --sim")),
        };
        let renderer = match self.renderer {
            RendererArg::R0 | RendererArg::R1 => {
                need!(
                    self.align_offset_us.is_none() && self.write_ahead_frames.is_none(),
                    "--align-offset-us and --write-ahead-frames apply only to --renderer R2"
                );
                if self.renderer == RendererArg::R0 {
                    Renderer::R0
                } else {
                    Renderer::R1
                }
            }
            RendererArg::R2 => {
                let Some(align_offset_us) = self.align_offset_us else {
                    return Err(refusal("--renderer R2 requires --align-offset-us <c in us>"));
                };
                Renderer::R2 {
                    align_offset_us,
                    write_ahead_frames: self.write_ahead_frames.unwrap_or(R2_DEFAULT_WRITE_AHEAD_FRAMES),
                }
            }
        };
        Ok(ReplayRequest {
            trace: self.trace.clone(),
            wav: self.haptic_wav.clone(),
            renderer,
            purpose: self.purpose,
            out: self.out.clone(),
            backend,
            sim_faults: Vec::new(),
        })
    }
}

// ---------------------------------------------------------------- preparation (refusals only)

struct Prepared {
    request: ReplayRequest,
    trace: Trace,
    payloads: Vec<Bytes>,
    payload_concat_sha256: String,
    run_id: String,
    binary: Value,
}

fn replay_run_id(renderer: Renderer, source_run_id: &str) -> String {
    format!("{RUN_ID_PREFIX}{}-{source_run_id}", renderer.as_str())
}

/// Everything that can refuse, before anything is written.
fn prepare(request: &ReplayRequest) -> Result<Prepared> {
    need!(
        request.purpose.renderer_name() == request.renderer.as_str(),
        "stage-1 purpose {} replays with {}, not {}",
        request.purpose.as_str(),
        request.purpose.renderer_name(),
        request.renderer.as_str()
    );
    placement_config(request.renderer)
        .validate()
        .map_err(|e| refusal(format!("renderer {} placement: {e}", request.renderer.as_str())))?;
    let trace = load_trace(&request.trace)?;
    let pcm = load_wav(&request.wav, &trace.wav_sha256, trace.wav_total_samples)?;
    let (payloads, payload_concat_sha256) = regenerate_payloads(&trace.ticks, &pcm)?;
    need!(
        payload_concat_sha256 == trace.payload_concat_sha256,
        "regenerated payload_concat_sha256 {payload_concat_sha256} != the trace's integrity.payload_concat_sha256 {}",
        trace.payload_concat_sha256
    );
    let run_id = replay_run_id(request.renderer, &trace.run_id);
    need!(run_id.starts_with(RUN_ID_PREFIX), "replay run id {run_id:?} lacks {RUN_ID_PREFIX:?}");
    Ok(Prepared { request: request.clone(), trace, payloads, payload_concat_sha256, run_id, binary: binary_info() })
}

/// What the binary can say about itself. The 40-hex source commit is not
/// embedded (the crate has no build-info step): the stage-1 freeze binds
/// it together with `exe_sha256` (contract §7).
fn binary_info() -> Value {
    let exe_path = std::env::current_exe().map(|p| p.display().to_string()).ok();
    // /proc/self/exe is the mapped image, even if the path was replaced since.
    let exe_sha256 = fs::read("/proc/self/exe").map(|b| sha256_hex(&b));
    json!({
        "name": "l2_haptic_replay",
        "crate": env!("CARGO_PKG_NAME"),
        "crate_version": env!("CARGO_PKG_VERSION"),
        "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
        "exe_path": exe_path,
        "exe_sha256": exe_sha256.as_ref().ok(),
        "exe_sha256_error": exe_sha256.as_ref().err().map(|e| e.to_string()),
        "exe_sha256_of": "/proc/self/exe (the running image)",
        "source_commit": null,
        "source_commit_binding": "not embedded: the stage-1 freeze binds the last source commit (40-hex) and this exe_sha256 (contract §7)",
    })
}

// ---------------------------------------------------------------- execution

/// The sink placement of a renderer (library defaults; R2 adds its mode).
fn placement_config(renderer: Renderer) -> PlacementConfig {
    PlacementConfig {
        period_frames: ALSA_DEFAULT_PERIOD_FRAMES,
        buffer_frames: ALSA_DEFAULT_BUFFER_FRAMES,
        target_fill_frames: ALSA_DEFAULT_TARGET_FILL_FRAMES,
        prefill_frames: ALSA_DEFAULT_PREFILL_FRAMES,
        queue_limit_frames: ALSA_DEFAULT_QUEUE_FRAMES,
        latency_bound_frames: renderer.latency_bound_frames(),
        mode: renderer.placement_mode(),
    }
}

fn l2_config(p: &Prepared) -> L2Config {
    let backend = match &p.request.backend {
        Backend::Device(name) => AlsaBackend::Device { name: name.clone() },
        Backend::Sim => {
            let mut sim = SimPcmConfig::new(ALSA_DEFAULT_PERIOD_FRAMES, ALSA_DEFAULT_BUFFER_FRAMES);
            sim.faults = p.request.sim_faults.clone();
            AlsaBackend::Simulated(sim)
        }
    };
    L2Config {
        run_id: p.run_id.clone(),
        method: L2_METHOD.into(),
        sinks: L2Sinks { pc: L2SinkKind::Null, haptic: L2SinkKind::Alsa },
        fake: None,
        alsa: Some(AlsaSinkConfig {
            backend,
            placement: placement_config(p.request.renderer),
            status_interval_us: ALSA_DEFAULT_STATUS_INTERVAL_US,
            ready_timeout_us: ALSA_READY_TIMEOUT_US,
            release_timeout_us: ALSA_RELEASE_TIMEOUT_US,
            diagnostics: None,
        }),
        wayland: None,
        queue_capacity: L2_QUEUE_CAPACITY,
        row_capacity: L2_ROW_CAPACITY,
        report_window_us: CLOSE_GRACE_US,
        max_tracked_objects: L2_MAX_TRACKED_OBJECTS,
    }
}

/// Sleep until CLOCK_MONOTONIC reaches `target_us` (absolute; never early:
/// `now_us()` afterwards is >= `target_us`).
fn sleep_until_us(target_us: u64) {
    let ts = libc::timespec {
        tv_sec: (target_us / 1_000_000) as libc::time_t,
        tv_nsec: ((target_us % 1_000_000) * 1_000) as libc::c_long,
    };
    loop {
        // SAFETY: a valid timespec; the remainder pointer may be null with TIMER_ABSTIME.
        let rc = unsafe { libc::clock_nanosleep(libc::CLOCK_MONOTONIC, libc::TIMER_ABSTIME, &ts, std::ptr::null_mut()) };
        match rc {
            0 => break,
            libc::EINTR => continue,
            _ => {
                // Not expected for a valid absolute request; fall back to polling.
                while now_us() < target_us {
                    std::thread::yield_now();
                }
                break;
            }
        }
    }
}

/// The replay's time origin, from its own stream epoch.
#[derive(Debug, Clone)]
struct Anchor {
    replay_t0_us: i64,
    replay_rel: BTreeMap<&'static str, i64>,
    residual: BTreeMap<&'static str, i64>,
}

fn read_first_row(path: &Path) -> Result<Value> {
    let mut reader = BufReader::new(File::open(path).with_context(|| format!("open {}", path.display()))?);
    let mut line = String::new();
    reader.read_line(&mut line).with_context(|| format!("read {}", path.display()))?;
    anyhow::ensure!(line.ends_with('\n'), "{}: the first row is not complete", path.display());
    serde_json::from_str(line.trim_end_matches('\n')).with_context(|| format!("{}: first row is not JSON", path.display()))
}

fn anchor_from_meta(meta: &Value, trace: &Trace) -> Result<Anchor> {
    anyhow::ensure!(meta["role"] == "meta", "the L2 file's first row is not the meta row");
    let epoch = &meta["alsa"]["stream_epoch"];
    let field = |f: &str| -> Result<i64> {
        epoch[f].as_u64().and_then(|v| i64::try_from(v).ok()).with_context(|| format!("L2 meta alsa.stream_epoch.{f}"))
    };
    let replay_t0_us = field(ANCHOR_FIELD)?
        .checked_sub(trace.rel_epoch[ANCHOR_FIELD])
        .filter(|t| *t > 0)
        .context("replay t0 is not a positive CLOCK_MONOTONIC instant")?;
    let mut replay_rel = BTreeMap::new();
    let mut residual = BTreeMap::new();
    for f in EPOCH_US_FIELDS {
        let rel = field(f)? - replay_t0_us;
        replay_rel.insert(f, rel);
        residual.insert(f, rel - trace.rel_epoch[f]);
    }
    Ok(Anchor { replay_t0_us, replay_rel, residual })
}

fn outcome_str(o: L2SubmitOutcome) -> &'static str {
    match o {
        L2SubmitOutcome::Submitted => "submitted",
        L2SubmitOutcome::QueueFull => "queue_full",
        L2SubmitOutcome::AfterClose => "after_close",
        L2SubmitOutcome::SinkGone => "sink_gone",
    }
}

#[derive(Debug, Clone, Copy)]
struct SubmitRecord {
    due_us: u64,
    release_us: u64,
    t_call_us: u64,
    outcome: L2SubmitOutcome,
}

/// The engine ledger as the replay checks it.
#[derive(Debug, Default)]
struct LedgerView {
    lines: u64,
    sentinel: Value,
    /// `(seq, t_submit_us)` of every haptic submit row, in file order.
    haptic_submits: Vec<(u32, u64)>,
    haptic_outputs: u64,
    haptic_discards_after: BTreeMap<String, u64>,
    haptic_discards_before: BTreeMap<String, u64>,
    pc_rows: u64,
    /// R2 (v1.8): `(seq, appl_pos, planned_end_after)` of every placement row.
    haptic_placements: Vec<(u32, u64, u64)>,
    /// R2 (v1.8): trim rows, and those with `start_removed`.
    haptic_trims: u64,
    haptic_trims_start_removed: u64,
}

impl LedgerView {
    fn haptic_terminals(&self) -> u64 {
        self.haptic_outputs + self.haptic_discards_after.values().sum::<u64>()
    }
}

/// `l2_schema`: the tag the replay's configuration writes; the aligned rows
/// (`placement`, `trim`) are accepted only under v1.8.
fn read_ledger(bytes: &[u8], run_id: &str, l2_schema: &str) -> Result<LedgerView> {
    let text = std::str::from_utf8(bytes).context("L2 ledger is not UTF-8")?;
    anyhow::ensure!(text.ends_with('\n'), "L2 ledger is not newline-terminated");
    let rows: Vec<Value> = text[..text.len() - 1]
        .split('\n')
        .enumerate()
        .map(|(n, line)| serde_json::from_str(line).with_context(|| format!("L2 ledger line {}: not JSON", n + 1)))
        .collect::<Result<_>>()?;
    anyhow::ensure!(rows.len() >= 2, "L2 ledger has fewer than 2 rows");
    let meta = &rows[0];
    anyhow::ensure!(meta["role"] == "meta" && meta["l2_schema"] == l2_schema, "L2 ledger line 1 is not a {l2_schema} meta row");
    let aligned_rows = l2_schema != L2_SCHEMA;
    let sentinel = rows.last().expect("non-empty").clone();
    anyhow::ensure!(sentinel["role"] == "instrumentation_end", "L2 ledger's last row is not instrumentation_end");
    let mut view = LedgerView { lines: rows.len() as u64, sentinel, ..LedgerView::default() };
    for (n, r) in rows.iter().enumerate().take(rows.len() - 1).skip(1) {
        anyhow::ensure!(r["run_id"] == run_id, "L2 ledger line {}: run_id {} != {run_id}", n + 1, r["run_id"]);
        match (r["track"].as_str(), r["role"].as_str()) {
            (Some("pc"), _) => view.pc_rows += 1,
            (Some("haptic"), Some("submit")) => {
                let seq = r["seq"].as_u64().and_then(|s| u32::try_from(s).ok()).context("haptic submit seq")?;
                let t = r["t_submit_us"].as_u64().context("haptic submit t_submit_us")?;
                view.haptic_submits.push((seq, t));
            }
            (Some("haptic"), Some("output")) => view.haptic_outputs += 1,
            (Some("haptic"), Some("placement")) if aligned_rows => {
                let field = |k: &str| r[k].as_u64().with_context(|| format!("L2 ledger line {}: placement {k}", n + 1));
                let seq = u32::try_from(field("seq")?).context("placement seq")?;
                view.haptic_placements.push((seq, field("appl_pos")?, field("planned_end_after")?));
            }
            (Some("haptic"), Some("trim")) if aligned_rows => {
                view.haptic_trims += 1;
                if r["start_removed"].as_bool().context("trim start_removed")? {
                    view.haptic_trims_start_removed += 1;
                }
            }
            (Some("haptic"), Some("explicit_discard")) => {
                let reason = r["reason"].as_str().unwrap_or("?").to_string();
                match r["discard_phase"].as_str() {
                    Some("after_submit") => *view.haptic_discards_after.entry(reason).or_default() += 1,
                    Some("before_submit") => *view.haptic_discards_before.entry(reason).or_default() += 1,
                    other => anyhow::bail!("L2 ledger line {}: discard_phase {other:?}", n + 1),
                }
            }
            (track, role) => anyhow::bail!("L2 ledger line {}: unexpected row (track {track:?}, role {role:?})", n + 1),
        }
    }
    Ok(view)
}

/// Nearest-rank quantile `num/den` of sorted values (index ceil(q·n) − 1).
fn nearest_rank(sorted: &[i64], num: u64, den: u64) -> Option<i64> {
    let n = sorted.len() as u64;
    if n == 0 {
        return None;
    }
    let rank = (num * n).div_ceil(den).max(1);
    Some(sorted[(rank - 1) as usize])
}

fn lateness_stats(values: &[i64]) -> Value {
    let mut signed = values.to_vec();
    signed.sort_unstable();
    let mut abs: Vec<i64> = values.iter().map(|v| v.abs()).collect();
    abs.sort_unstable();
    json!({
        "n": values.len(),
        "min": signed.first(),
        "median_signed": nearest_rank(&signed, 1, 2),
        "max": signed.last(),
        "abs_p99": nearest_rank(&abs, 99, 100),
        "abs_max": abs.last(),
    })
}

/// What a finished replay reports (stdout and tests).
#[derive(Debug)]
struct Report {
    valid: bool,
    invalid_reasons: Vec<String>,
    replay_path: PathBuf,
    summary: Value,
}

fn create_new(path: &Path) -> Result<File> {
    OpenOptions::new().write(true).create_new(true).open(path).with_context(|| format!("create {} (must not exist)", path.display()))
}

/// Run the prepared replay into `p.request.out` (created here).
fn execute(p: &Prepared) -> Result<Report> {
    let out = &p.request.out;
    match fs::create_dir(out) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(refusal(format!("output directory {} already exists; nothing written", out.display())))
        }
        Err(e) => return Err(refusal(format!("cannot create output directory {} ({e}); nothing written", out.display()))),
    }
    let wall_start_us = now_us();
    let l2_path = out.join(L2_FILE);
    let config = l2_config(p);
    config.validate()?;
    let sinks = config.sinks;
    let l2_schema = config.l2_schema();
    // Opens, prefills and starts the stream; the meta row is written before it returns.
    let output = L2Output::start(&l2_path, config)?;
    let mut invalid: Vec<String> = Vec::new();
    let anchor = match read_first_row(&l2_path).and_then(|meta| anchor_from_meta(&meta, &p.trace)) {
        Ok(anchor) => Some(anchor),
        Err(e) => {
            invalid.push(format!("replay_anchor_unavailable: {e:#}"));
            None
        }
    };
    let submitter = output.submitter();
    let mut records: Vec<SubmitRecord> = Vec::with_capacity(p.trace.ticks.len());
    if let Some(anchor) = &anchor {
        let first_due = anchor.replay_t0_us + p.trace.ticks[0].t_submit_rel_us;
        let ready_us = now_us() as i64;
        if ready_us >= first_due {
            invalid.push(format!(
                "schedule_unreachable: the stage was ready {} us after the first submit's due time",
                ready_us - first_due
            ));
        }
        for (tick, payload) in p.trace.ticks.iter().zip(&p.payloads) {
            let due_us = (anchor.replay_t0_us + tick.t_submit_rel_us) as u64;
            let release_us = (anchor.replay_t0_us + tick.t_release_rel_us) as u64;
            sleep_until_us(due_us);
            let identity = L2Identity {
                modality: L2Modality::Haptic,
                tier: HAPTIC_TIER_FULL,
                seq: tick.k as u32,
                pts_us: tick.pts_us,
                event_id: tick.event_id,
                route: None,
            };
            let t_call_us = now_us();
            let outcome = submitter.submit_payload(identity, release_us, Some(payload.clone()));
            records.push(SubmitRecord { due_us, release_us, t_call_us, outcome });
        }
    }
    // Every producer handle must be gone before `finish` (or the lane waits
    // out the grace and records producers_alive_at_finish).
    drop(submitter);
    let summary = output.finish()?;

    // ---- read back and judge
    let raw = fs::read(&l2_path).with_context(|| format!("read {}", l2_path.display()))?;
    let l2_sha256 = sha256_hex(&raw);
    let ledger = read_ledger(&raw, &p.run_id, l2_schema)?;
    let t_submit_by_seq: HashMap<u32, u64> = ledger.haptic_submits.iter().copied().collect();
    let order_of: HashMap<u64, u64> = p.trace.ticks.iter().map(|t| (t.k, t.submit_order)).collect();
    let ledger_orders: Vec<u64> = ledger
        .haptic_submits
        .iter()
        .map(|(seq, _)| order_of.get(&u64::from(*seq)).copied().with_context(|| format!("ledger submit of seq {seq} not in the trace")))
        .collect::<Result<_>>()?;
    let order_violations = ledger_orders.windows(2).filter(|w| w[1] <= w[0]).count() as u64;
    let n_ticks = p.trace.ticks.len() as u64;
    let submitted = ledger.haptic_submits.len() as u64;
    if !summary.integrity_ok(sinks) {
        invalid.push(format!(
            "l2_integrity_failed: first_integrity_failure_reason {:?}, conservation_ok {}, submitted_unresolved {}",
            summary.first_integrity_failure_reason, summary.conservation_ok, summary.submitted_unresolved
        ));
    } else if let Some(reason) = &summary.first_integrity_failure_reason {
        invalid.push(format!("l2_integrity_failure: {reason}"));
    }
    if !ledger.sentinel["lanes"]["haptic"]["device_failure"].is_null() {
        invalid.push(format!("device_failure: {}", ledger.sentinel["lanes"]["haptic"]["device_failure"]));
    }
    if submitted != n_ticks || records.len() as u64 != n_ticks {
        invalid.push(format!("not_all_submitted: {submitted} submit rows for {n_ticks} trace ticks"));
    }
    if ledger.haptic_terminals() != submitted {
        invalid.push(format!("terminals_ne_submits: {} terminals for {submitted} submits", ledger.haptic_terminals()));
    }
    if order_violations > 0 {
        invalid.push(format!("submit_order_violation: {order_violations} inversions in the ledger's submit order"));
    }
    if ledger.pc_rows > 0 {
        invalid.push(format!("pc_rows: {} PC rows in a haptic-only replay", ledger.pc_rows));
    }
    if p.request.renderer.aligned().is_some() {
        // Contract §2-6 (aligned): one placement row per submission (except
        // payload_invalid and after a device failure, which is invalid
        // already), planned_end_after >= appl_pos, occluded = start_removed.
        let payload_invalid = ledger.haptic_discards_after.get("payload_invalid").copied().unwrap_or(0);
        let placed: HashSet<u32> = ledger.haptic_placements.iter().map(|(seq, _, _)| *seq).collect();
        let submitted_seqs: HashSet<u32> = ledger.haptic_submits.iter().map(|(seq, _)| *seq).collect();
        if ledger.sentinel["lanes"]["haptic"]["device_failure"].is_null()
            && (ledger.haptic_placements.len() as u64 != submitted - payload_invalid || !placed.is_subset(&submitted_seqs))
        {
            invalid.push(format!(
                "placement_rows_ne_submits: {} placement rows for {submitted} submits ({payload_invalid} payload_invalid)",
                ledger.haptic_placements.len()
            ));
        }
        let behind = ledger.haptic_placements.iter().filter(|(_, appl, end)| end < appl).count();
        if behind > 0 {
            invalid.push(format!("planned_end_before_appl_pos: {behind} placement rows"));
        }
        let occluded = ledger.haptic_discards_after.get(DISCARD_OCCLUDED).copied().unwrap_or(0);
        if occluded != ledger.haptic_trims_start_removed {
            invalid.push(format!(
                "occluded_ne_start_removed_trims: {occluded} occluded for {} start_removed trims",
                ledger.haptic_trims_start_removed
            ));
        }
    }

    // ---- per-tick submit records (V1)
    let submits_path = out.join(SUBMITS_FILE);
    let mut submits_file = create_new(&submits_path)?;
    let mut submits_hash = Sha256::new();
    let mut put = |file: &mut File, row: &Value| -> Result<()> {
        let mut line = serde_json::to_vec(row)?;
        line.push(b'\n');
        submits_hash.update(&line);
        file.write_all(&line)?;
        Ok(())
    };
    let header = json!({
        "role": "replay_submits_meta",
        "schema": SUBMITS_SCHEMA,
        "replay_schema": REPLAY_SCHEMA,
        "run_id": p.run_id,
        "source_run_id": p.trace.run_id,
        "trace_sha256": p.trace.sha256,
        "replay_t0_us": anchor.as_ref().map(|a| a.replay_t0_us),
        "clock": "CLOCK_MONOTONIC",
        "time_unit": "us",
        "fields": {
            "scheduled_submit_us": "replay_t0_us + t_submit_rel_us (the tick's due time)",
            "scheduled_release_us": "replay_t0_us + t_release_rel_us (passed to submit_payload as t_release_us; unused by R0/R1)",
            "t_call_us": "now_us() immediately before submit_payload, after clock_nanosleep(TIMER_ABSTIME) to the due time",
            "call_lateness_us": "t_call_us - scheduled_submit_us",
            "outcome": "L2SubmitOutcome of the call: submitted | queue_full (explicit_discard output_queue_full before submit) | after_close | sink_gone",
            "t_submit_us": "the L2 ledger's submit row t_submit_us (read inside submit_payload), null without a submit row",
            "submit_lateness_us": "t_submit_us - scheduled_submit_us (stage-1 V1), null without a submit row",
            "source_ledger_terminal": "the trace's ledger_terminal of the source run",
        },
    });
    put(&mut submits_file, &header)?;
    let mut submit_lateness = Vec::with_capacity(records.len());
    let mut call_lateness = Vec::with_capacity(records.len());
    for (tick, rec) in p.trace.ticks.iter().zip(&records) {
        let t_submit = t_submit_by_seq.get(&(tick.k as u32)).copied();
        let lateness = t_submit.map(|t| t as i64 - rec.due_us as i64);
        let call = rec.t_call_us as i64 - rec.due_us as i64;
        call_lateness.push(call);
        if let Some(l) = lateness {
            submit_lateness.push(l);
        }
        put(
            &mut submits_file,
            &json!({
                "role": "replay_submit",
                "run_id": p.run_id,
                "submit_order": tick.submit_order,
                "k": tick.k,
                "seq": tick.k,
                "pts_us": tick.pts_us,
                "event_id": tick.event_id,
                "t_release_rel_us": tick.t_release_rel_us,
                "t_submit_rel_us": tick.t_submit_rel_us,
                "scheduled_release_us": rec.release_us,
                "scheduled_submit_us": rec.due_us,
                "t_call_us": rec.t_call_us,
                "call_lateness_us": call,
                "outcome": outcome_str(rec.outcome),
                "t_submit_us": t_submit,
                "submit_lateness_us": lateness,
                "source_ledger_terminal": tick.ledger_terminal,
            }),
        )?;
    }
    submits_file.sync_all()?;
    drop(submits_file);
    let submits_sha256 = format!("{:x}", submits_hash.finalize());
    if submit_lateness.iter().chain(&call_lateness).any(|l| *l < 0) {
        invalid.push("early_submit: a submit before its due time".into());
    }

    // ---- the replay ledger: replay_meta + the engine rows verbatim
    let valid = invalid.is_empty();
    let lateness_v1 = lateness_stats(&submit_lateness);
    let lane = &ledger.sentinel["lanes"]["haptic"];
    let outcome = json!({
        "valid": valid,
        "invalid_reasons": invalid,
        "integrity_ok": summary.integrity_ok(sinks),
        "first_integrity_failure_reason": summary.first_integrity_failure_reason,
        "device_failure": lane["device_failure"],
        "haptic_submits": submitted,
        "haptic_terminals": ledger.haptic_terminals(),
        "haptic_outputs": ledger.haptic_outputs,
        "haptic_discards_after_submit": ledger.haptic_discards_after,
        "haptic_discards_before_submit": ledger.haptic_discards_before,
        "submitted_unresolved": summary.submitted_unresolved,
        "max_unwritten_tick_frames": lane["max_unwritten_tick_frames"],
        "finish_start_us": summary.finish_start_us,
        "close_deadline_us": summary.close_deadline_us,
    });
    let mut outcome = outcome;
    if p.request.renderer.aligned().is_some() {
        let map = outcome.as_object_mut().expect("outcome is an object");
        map.insert("placement_rows".into(), ledger.haptic_placements.len().into());
        map.insert("trim_rows".into(), ledger.haptic_trims.into());
        map.insert("start_removed_trims".into(), ledger.haptic_trims_start_removed.into());
        for key in ["ticks_occluded", "ticks_too_late", "ticks_head_cut", "ticks_placement_queue_full", "frames_tail_cut", "frames_head_cut", "frames_silence_inserted", "max_waiting_silence_frames", "xrun"] {
            map.insert(key.into(), lane[key].clone());
        }
    }
    let v1 = json!({
        "ticks": n_ticks,
        "submitted": submitted,
        "not_submitted": n_ticks - submitted.min(n_ticks),
        "order_violations": order_violations,
        "submit_lateness_us": lateness_v1,
        "call_lateness_us": lateness_stats(&call_lateness),
        "rule": "lateness = actual - scheduled (signed, µs); median_signed and abs_p99 are nearest-rank (index ceil(q*n) - 1 of the sorted values); order_violations = inversions of the trace submit_order in the ledger's haptic submit-row order",
    });
    let renderer = p.request.renderer;
    let (backend, device) = match &p.request.backend {
        Backend::Device(name) => ("device", Some(name.clone())),
        Backend::Sim => ("simulated", None),
    };
    let sink = json!({
        "engine": "skew_moq::l2_output::L2Output (pc null, haptic alsa) with the unchanged l2_haptic placement engine",
        "period_frames": ALSA_DEFAULT_PERIOD_FRAMES,
        "buffer_frames": ALSA_DEFAULT_BUFFER_FRAMES,
        "target_fill_frames": ALSA_DEFAULT_TARGET_FILL_FRAMES,
        "prefill_frames": ALSA_DEFAULT_PREFILL_FRAMES,
        "queue_limit_frames": ALSA_DEFAULT_QUEUE_FRAMES,
        "latency_bound_frames": renderer.latency_bound_frames(),
        "status_interval_us": ALSA_DEFAULT_STATUS_INTERVAL_US,
        "rate_hz": PCM_RATE_HZ,
        "upsample": UPSAMPLE_FACTOR,
        "queue_capacity_per_track": L2_QUEUE_CAPACITY,
        "row_capacity": L2_ROW_CAPACITY,
        "max_tracked_objects": L2_MAX_TRACKED_OBJECTS,
    });
    let mut sink = sink;
    if let Some((align_offset_us, write_ahead_frames)) = renderer.aligned() {
        let map = sink.as_object_mut().expect("sink is an object");
        map.insert(
            "engine".into(),
            "skew_moq::l2_output::L2Output (pc null, haptic alsa) with the l2_haptic aligned placement (PlacementMode::Aligned, R2)".into(),
        );
        map.insert("placement".into(), "aligned".into());
        map.insert("align_offset_us".into(), align_offset_us.into());
        map.insert("write_ahead_frames".into(), write_ahead_frames.into());
    }
    let close = json!({
        "rule": "after the last submit the producer handle is dropped and L2Output::finish runs the 1-D close: deadline = finish_start_us + report_window_us; the lane ends once the last tick's tail played (idle); the source run's close deadline is not in the trace and is not reproduced",
        "report_window_us": CLOSE_GRACE_US,
    });
    let payload = json!({
        "rule": "lib.rs load_haptic_pcm + pcm_tick_payload(pcm, k, 8000, 90) per trace tick (k = trace k): samples [floor(8000k/90), floor(8000(k+1)/90)) mod total_samples; submitted as the payload of submit_payload",
        "payload_concat_sha256": p.payload_concat_sha256,
        "equals_trace": p.payload_concat_sha256 == p.trace.payload_concat_sha256,
        "identity": "L2Identity { haptic, tier HAPTIC_TIER_FULL (0), seq = k, pts_us, event_id, route None }",
    });
    let timing = json!({
        "anchor_field": ANCHOR_FIELD,
        "rule": "replay_t0_us = (this replay's L2 meta alsa.stream_epoch.t_start_us) - trace.stream_epoch_rel_t0_us.t_start_us; tick i is due at replay_t0_us + t_submit_rel_us (trace submit order, ties keep it), slept to with clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME); t_release_us = replay_t0_us + t_release_rel_us",
        "replay_t0_us": anchor.as_ref().map(|a| a.replay_t0_us),
        "source_stream_epoch_rel_t0_us": p.trace.rel_epoch,
        "replay_stream_epoch_rel_t0_us": anchor.as_ref().map(|a| a.replay_rel.clone()),
        "residual_us": anchor.as_ref().map(|a| a.residual.clone()),
        "residual_rule": "replay rel - source rel per stream_epoch field (0 for the anchor by construction)",
    });
    let files = json!({
        "l2_rows": { "file": L2_FILE, "sha256": l2_sha256, "lines": ledger.lines },
        "submits": { "file": SUBMITS_FILE, "sha256": submits_sha256, "rows": records.len() },
    });
    let meta = json!({
        "role": "replay_meta",
        "replay_schema": REPLAY_SCHEMA,
        "contract": CONTRACT,
        "evidence": false,
        "run_id": p.run_id,
        "run_id_rule": "exp4r-<renderer>-<source_run_id>",
        "t_us": now_us(),
        "clock": "CLOCK_MONOTONIC",
        "time_unit": "us",
        "composition": "this row, then the engine's L2 ledger (l2.jsonl, meta row first, instrumentation_end last) byte for byte; written after the replay ended, so l2_rows.sha256 binds the two",
        "source_run_id": p.trace.run_id,
        "source_experiment": p.trace.experiment,
        "source_set": p.trace.set,
        "source_method": p.trace.method,
        "source_ledger_sha256": p.trace.source_ledger_sha256,
        "source_input_sha256": p.trace.source_input_sha256,
        "source_t0_us": p.trace.t0_us,
        "source_latency_bound_frames": p.trace.source_latency_bound_frames,
        "trace_path": p.trace.path.display().to_string(),
        "trace_sha256": p.trace.sha256,
        "trace_schema": TRACE_SCHEMA,
        "renderer": renderer.as_str(),
        "placement": if renderer.aligned().is_some() { "aligned" } else { "fifo" },
        "align_offset_us": renderer.aligned().map(|(c, _)| c),
        "write_ahead_frames": renderer.aligned().map(|(_, w)| w),
        "latency_bound_frames": renderer.latency_bound_frames(),
        "stage1_purpose": p.request.purpose.as_str(),
        "wav_path": p.request.wav.display().to_string(),
        "wav_sha256": p.trace.wav_sha256,
        "backend": backend,
        "device": device,
        "sim_faults": p.request.sim_faults.iter().map(|f| format!("{f:?}")).collect::<Vec<_>>(),
        "l2_schema": l2_schema,
        "l2_method": L2_METHOD,
        "sink": sink,
        "close": close,
        "payload": payload,
        "timing": timing,
        "v1": v1,
        "carried_pre_submit_discards": p.trace.pre_submit_discards,
        "carried_pre_submit_discards_rule": "the source's pre-submit discards are not replayed (no engine row can be made for them); they are carried here from the trace with their terminal (design §3-2 item 4)",
        "files": files,
        "outcome": outcome,
        "replay_binary": p.binary,
        "wall_us": now_us() - wall_start_us,
    });
    let replay_path = out.join(REPLAY_FILE);
    let mut replay_file = create_new(&replay_path)?;
    let mut head = serde_json::to_vec(&meta)?;
    head.push(b'\n');
    replay_file.write_all(&head)?;
    replay_file.write_all(&raw)?;
    replay_file.sync_all()?;
    drop(replay_file);
    // The composed file must hold exactly the head row and the engine bytes.
    let composed = fs::read(&replay_path)?;
    anyhow::ensure!(
        composed.len() == head.len() + raw.len() && composed[..head.len()] == head[..] && sha256_hex(&composed[head.len()..]) == l2_sha256,
        "composed replay ledger does not reproduce the engine ledger"
    );
    let summary_json = json!({
        "run_id": p.run_id,
        "valid": valid,
        "invalid_reasons": meta["outcome"]["invalid_reasons"],
        "out": out.display().to_string(),
        "renderer": renderer.as_str(),
        "stage1_purpose": p.request.purpose.as_str(),
        "backend": backend,
        "ticks": n_ticks,
        "haptic_submits": submitted,
        "haptic_terminals": ledger.haptic_terminals(),
        "haptic_outputs": ledger.haptic_outputs,
        "haptic_discards_after_submit": meta["outcome"]["haptic_discards_after_submit"],
        "submit_lateness_us": meta["v1"]["submit_lateness_us"],
        "wall_us": meta["wall_us"],
    });
    Ok(Report { valid, invalid_reasons: invalid_reasons(&meta), replay_path, summary: summary_json })
}

fn invalid_reasons(meta: &Value) -> Vec<String> {
    meta["outcome"]["invalid_reasons"]
        .as_array()
        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default()
}

fn replay(request: &ReplayRequest) -> Result<Report> {
    let prepared = prepare(request)?;
    execute(&prepared)
}

/// 0 valid, 3 recorded but invalid, 2 refused, 1 I/O or stage error.
fn exit_status(result: &Result<Report>) -> u8 {
    match result {
        Ok(report) if report.valid => 0,
        Ok(_) => 3,
        Err(error) if is_refusal(error) => 2,
        Err(_) => 1,
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = cli.request().and_then(|request| replay(&request));
    let status = exit_status(&result);
    match &result {
        Ok(report) => {
            println!("{}", report.summary);
            if !report.valid {
                eprintln!(
                    "INVALID (recorded in {}): {}",
                    report.replay_path.display(),
                    report.invalid_reasons.join("; ")
                );
            }
        }
        Err(error) if is_refusal(error) => eprintln!("REFUSED: {error}"),
        Err(error) => eprintln!("ERROR: {error:#}"),
    }
    ExitCode::from(status)
}

#[cfg(test)]
mod tests {
    //! Simulated PCM only: no test constructs `Backend::Device` and runs it.
    use super::*;
    use skew_moq::l2_output::DISCARD_LATENCY_BOUND;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir().join(format!("skew-exp4r-{}-{}-{name}", std::process::id(), now_us()));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    /// RIFF/WAVE, PCM 8 kHz mono 16 bit, with a LIST chunk before `data`.
    fn wav_bytes(samples: &[i16]) -> Vec<u8> {
        let list: &[u8] = b"INFOISFT\x04\x00\x00\x00test";
        let data_len = (samples.len() * 2) as u32;
        let mut b = Vec::new();
        b.extend_from_slice(b"RIFF");
        b.extend_from_slice(&(4 + 24 + 8 + list.len() as u32 + 8 + data_len).to_le_bytes());
        b.extend_from_slice(b"WAVE");
        b.extend_from_slice(b"fmt ");
        b.extend_from_slice(&16u32.to_le_bytes());
        b.extend_from_slice(&1u16.to_le_bytes());
        b.extend_from_slice(&1u16.to_le_bytes());
        b.extend_from_slice(&8_000u32.to_le_bytes());
        b.extend_from_slice(&16_000u32.to_le_bytes());
        b.extend_from_slice(&2u16.to_le_bytes());
        b.extend_from_slice(&16u16.to_le_bytes());
        b.extend_from_slice(b"LIST");
        b.extend_from_slice(&(list.len() as u32).to_le_bytes());
        b.extend_from_slice(list);
        b.extend_from_slice(b"data");
        b.extend_from_slice(&data_len.to_le_bytes());
        for s in samples {
            b.extend_from_slice(&s.to_le_bytes());
        }
        b
    }

    /// Deterministic, pairwise-distinct, non-zero samples.
    fn samples(n: usize) -> Vec<i16> {
        (0..n).map(|i| (((i as i64 * 7_919) % 60_000) - 30_000 + 1) as i16).collect()
    }

    /// Written out WITHOUT lib.rs (`pcm_sample_bounds` / `pcm_tick_payload`):
    /// samples [floor(8000k/90), floor(8000(k+1)/90)) read cyclically.
    fn independent_concat(samples: &[i16], ks: &[u64]) -> String {
        let total = samples.len() as u64;
        let mut h = Sha256::new();
        for &k in ks {
            for s in (8_000 * k / 90)..(8_000 * (k + 1) / 90) {
                h.update(samples[(s % total) as usize].to_le_bytes());
            }
        }
        format!("{:x}", h.finalize())
    }

    const T0: u64 = 5_000_000_000;

    struct Spec {
        ks: Vec<u64>,
        submit_rel: Vec<i64>,
        start_rel_us: i64,
        set: &'static str,
        latency_bound: Option<u64>,
        n_samples: usize,
    }

    impl Spec {
        /// `n` consecutive ticks from k = 0, submitted `spacing_us` apart.
        fn regular(n: u64, spacing_us: i64) -> Self {
            Self {
                ks: (0..n).collect(),
                submit_rel: (0..n as i64).map(|i| 30_000 + i * spacing_us).collect(),
                start_rel_us: -200_000,
                set: "main",
                latency_bound: None,
                n_samples: 4_000,
            }
        }
    }

    fn trace_doc(samples: &[i16], wav: &[u8], spec: &Spec) -> Value {
        let ticks: Vec<Value> = spec
            .ks
            .iter()
            .zip(&spec.submit_rel)
            .enumerate()
            .map(|(i, (&k, &sub))| {
                json!({
                    "k": k, "pts_us": k * 1_000_000 / 90, "event_id": if k % 3 == 0 { k / 3 + 1 } else { 0 },
                    "t_release_rel_us": sub - 40, "t_submit_rel_us": sub, "submit_order": i,
                    "samples_8k_expected": 8_000 * (k + 1) / 90 - 8_000 * k / 90,
                    "samples_8k_ledger": 8_000 * (k + 1) / 90 - 8_000 * k / 90, "ledger_terminal": "output",
                })
            })
            .collect();
        let sha = sha256_hex(wav);
        let s = spec.start_rel_us;
        let abs = |d: i64| (T0 as i64 + d) as u64;
        json!({
            "schema": "exp4-trace-v1", "set": spec.set, "experiment": "exp3",
            "run_id": "exp3-L2-direct-D1-bin-k0-a1-p0-P0A", "method": "P0-A",
            "source_input_sha256": "a".repeat(64), "source_ledger_sha256": "b".repeat(64), "t0_us": T0,
            "stream_epoch": {
                "prefill_frames": 512, "t_start_us": abs(s), "trigger_htstamp_us": abs(s + 18),
                "first_status_t0_us": abs(s + 19), "first_status_htstamp_us": abs(s + 22),
                "first_status_delay_frames": 512, "first_status_avail_frames": 1536, "first_status_driver_delay_frames": 0,
            },
            "stream_epoch_rel_t0_us": {
                "t_start_us": s, "trigger_htstamp_us": s + 18, "first_status_t0_us": s + 19, "first_status_htstamp_us": s + 22,
            },
            "wav_sha256": sha,
            "wav": {"path": "synthetic.wav", "sha256": sha, "total_samples": samples.len(), "sample_rate_hz": 8_000, "haptic_rate_hz": 90},
            "alsa": {
                "prefill_frames": 512, "period_frames": 256, "buffer_frames": 2_048, "target_fill_frames": 512,
                "queue_limit_frames": 48_000, "rate_hz": 48_000, "latency_bound_frames": spec.latency_bound,
                "latency_bound_frames_present": true,
            },
            "ticks": ticks,
            "pre_submit_discards": [],
            "integrity": {
                "haptic_submits": spec.ks.len(), "pre_submit_discards": 0,
                "payload_concat_sha256": independent_concat(samples, &spec.ks),
            },
        })
    }

    struct Fixture {
        dir: TempDir,
        trace: PathBuf,
        wav: PathBuf,
        samples: Vec<i16>,
        doc: Value,
    }

    fn fixture(name: &str, spec: &Spec) -> Fixture {
        let dir = TempDir::new(name);
        let samples = samples(spec.n_samples);
        let wav_data = wav_bytes(&samples);
        let wav = dir.0.join("synthetic.wav");
        fs::write(&wav, &wav_data).unwrap();
        let doc = trace_doc(&samples, &wav_data, spec);
        let trace = dir.0.join("synthetic.trace.json");
        fs::write(&trace, serde_json::to_vec(&doc).unwrap()).unwrap();
        Fixture { dir, trace, wav, samples, doc }
    }

    impl Fixture {
        fn rewrite(&self, edit: impl FnOnce(&mut Value)) {
            let mut doc = self.doc.clone();
            edit(&mut doc);
            fs::write(&self.trace, serde_json::to_vec(&doc).unwrap()).unwrap();
        }

        fn request(&self, renderer: Renderer, purpose: Purpose, out: &str) -> ReplayRequest {
            ReplayRequest {
                trace: self.trace.clone(),
                wav: self.wav.clone(),
                renderer,
                purpose,
                out: self.dir.0.join(out),
                backend: Backend::Sim,
                sim_faults: Vec::new(),
            }
        }
    }

    fn rows(path: &Path) -> Vec<Value> {
        fs::read_to_string(path).unwrap().lines().map(|l| serde_json::from_str(l).unwrap()).collect()
    }

    fn haptic<'a>(rows: &'a [Value], role: &str) -> Vec<&'a Value> {
        rows.iter().filter(|r| r["track"] == "haptic" && r["role"] == role).collect()
    }

    fn refused<T>(result: Result<T>, needle: &str) {
        let Err(error) = result else { panic!("must be refused ({needle})") };
        assert!(is_refusal(&error), "not a refusal: {error:#}");
        assert!(error.to_string().contains(needle), "{needle:?} not in {error}");
    }

    /// The replay ledger = replay_meta + the engine ledger verbatim; every
    /// submit has exactly one terminal; outputs are FIFO (non-overlapping,
    /// in submit order). Returns (replay rows, sidecar rows).
    fn check_common(out: &Path, ks: &[u64]) -> (Vec<Value>, Vec<Value>) {
        let replay = fs::read(out.join(REPLAY_FILE)).unwrap();
        let engine = fs::read(out.join(L2_FILE)).unwrap();
        let cut = replay.iter().position(|b| *b == b'\n').unwrap() + 1;
        assert_eq!(&replay[cut..], &engine[..], "engine rows verbatim after the head row");
        let rows = rows(&out.join(REPLAY_FILE));
        let meta = &rows[0];
        assert_eq!(meta["role"], "replay_meta");
        assert_eq!(meta["replay_schema"], REPLAY_SCHEMA);
        assert_eq!(meta["files"]["l2_rows"]["sha256"], sha256_hex(&engine));
        assert_eq!(rows[1]["role"], "meta");
        assert_eq!(rows[1]["l2_schema"], L2_SCHEMA);
        assert_eq!(rows[1]["sinks"], json!({"pc": "null", "haptic": "alsa"}));
        assert_eq!(rows[1]["alsa"]["backend"], "simulated");
        assert_eq!(rows[1]["device_output"], false);
        assert_eq!(rows.last().unwrap()["role"], "instrumentation_end");
        let submits = haptic(&rows, "submit");
        let seqs: Vec<u64> = submits.iter().map(|r| r["seq"].as_u64().unwrap()).collect();
        assert_eq!(seqs, ks, "every tick submitted, in trace submit order");
        let mut terminals: HashMap<u64, Vec<&Value>> = HashMap::new();
        for r in rows.iter().filter(|r| r["track"] == "haptic" && (r["role"] == "output" || r["role"] == "explicit_discard")) {
            assert_ne!(r["discard_phase"], "before_submit");
            terminals.entry(r["seq"].as_u64().unwrap()).or_default().push(r);
        }
        assert!(ks.iter().all(|k| terminals.get(k).map(Vec::len) == Some(1)), "exactly one terminal per submit");
        assert_eq!(terminals.len(), ks.len());
        let outputs: Vec<(u64, u64)> = ks
            .iter()
            .filter_map(|k| {
                let t = terminals[k][0];
                (t["role"] == "output")
                    .then(|| (t["output_detail"]["n_k"].as_u64().unwrap(), t["output_detail"]["frames"].as_u64().unwrap()))
            })
            .collect();
        assert!(outputs.windows(2).all(|w| w[1].0 >= w[0].0 + w[0].1), "FIFO: in submit order, never overlapping");
        let end = rows.last().unwrap();
        assert_eq!(end["integrity_ok"], true);
        assert!(end["lanes"]["haptic"]["device_failure"].is_null());
        assert_eq!(end["first_integrity_failure_reason"], Value::Null);
        assert_eq!(meta["outcome"]["valid"], true);
        assert_eq!(meta["outcome"]["haptic_terminals"], ks.len());
        let sidecar = rows_of(&out.join(SUBMITS_FILE));
        assert_eq!(meta["files"]["submits"]["sha256"], sha256_hex(&fs::read(out.join(SUBMITS_FILE)).unwrap()));
        (rows, sidecar)
    }

    fn rows_of(path: &Path) -> Vec<Value> {
        rows(path)
    }

    // (a)
    #[test]
    fn the_regenerated_payload_concat_equals_an_independent_regeneration() {
        // 300 samples: the ticks wrap around the WAV several times; gaps in k.
        let mut spec = Spec::regular(0, 11_111);
        spec.ks = vec![0, 1, 2, 4, 5, 9, 10, 11, 30, 31, 3_599];
        spec.submit_rel = (0..spec.ks.len() as i64).map(|i| 30_000 + i * 11_111).collect();
        spec.n_samples = 300;
        let fx = fixture("concat", &spec);
        let prepared = prepare(&fx.request(Renderer::R0, Purpose::V2, "out")).unwrap();
        let want = independent_concat(&fx.samples, &spec.ks);
        assert_eq!(prepared.payload_concat_sha256, want);
        assert_eq!(prepared.trace.payload_concat_sha256, want);
        for (t, p) in prepared.trace.ticks.iter().zip(&prepared.payloads) {
            assert_eq!(p.len() as u64, 2 * (8_000 * (t.k + 1) / 90 - 8_000 * t.k / 90));
        }
        assert!(!fx.dir.0.join("out").exists(), "prepare writes nothing");
        // A trace whose recorded concat differs is refused before anything is written.
        fx.rewrite(|d| d["integrity"]["payload_concat_sha256"] = json!("c".repeat(64)));
        refused(replay(&fx.request(Renderer::R0, Purpose::V2, "out")), "payload_concat_sha256");
        assert!(!fx.dir.0.join("out").exists());
    }

    // (b)
    #[test]
    fn an_r0_sim_replay_terminates_every_submit_in_fifo_order_without_xrun() {
        let spec = Spec::regular(30, 11_111);
        let fx = fixture("r0", &spec);
        let report = replay(&fx.request(Renderer::R0, Purpose::V2, "out")).unwrap();
        assert!(report.valid, "{:?}", report.invalid_reasons);
        let (rows, _) = check_common(&fx.dir.0.join("out"), &spec.ks);
        let meta = &rows[0];
        assert_eq!(meta["renderer"], "R0");
        assert_eq!(meta["stage1_purpose"], "V2");
        assert_eq!(meta["placement"], "fifo");
        assert_eq!(meta["latency_bound_frames"], Value::Null);
        assert_eq!(meta["align_offset_us"], Value::Null);
        assert_eq!(meta["write_ahead_frames"], Value::Null);
        assert_eq!(meta["run_id"], "exp4r-R0-exp3-L2-direct-D1-bin-k0-a1-p0-P0A");
        assert_eq!(rows[1]["alsa"]["latency_bound_frames"], Value::Null);
        assert_eq!(haptic(&rows, "output").len(), 30, "R0 at real-time rate: everything is output");
        let lane = &rows.last().unwrap()["lanes"]["haptic"];
        assert!(lane.get("ticks_latency_bound").is_none(), "no B counters without a bound");
        assert_eq!(meta["timing"]["residual_us"]["t_start_us"], 0, "the stream start offset is exact");
    }

    // (c)
    #[test]
    fn an_r1_sim_replay_of_a_compressed_schedule_drops_waiting_ticks_within_b() {
        // 60 ticks 1 ms apart: ~11x faster than they play.
        let mut spec = Spec::regular(60, 1_000);
        spec.set = "validation";
        spec.latency_bound = Some(1_068);
        let fx = fixture("r1", &spec);
        let report = replay(&fx.request(Renderer::R1, Purpose::V3, "out")).unwrap();
        assert!(report.valid, "{:?}", report.invalid_reasons);
        let (rows, _) = check_common(&fx.dir.0.join("out"), &spec.ks);
        assert_eq!(rows[0]["latency_bound_frames"], 1_068);
        assert_eq!(rows[1]["alsa"]["latency_bound_frames"], 1_068);
        let dropped: Vec<&Value> =
            haptic(&rows, "explicit_discard").into_iter().filter(|r| r["reason"] == DISCARD_LATENCY_BOUND).collect();
        assert!(!dropped.is_empty(), "the compressed schedule must exceed B");
        assert!(dropped.iter().all(|r| r["discard_phase"] == "after_submit"));
        let lane = &rows.last().unwrap()["lanes"]["haptic"];
        assert_eq!(lane["ticks_latency_bound"], dropped.len());
        assert!(lane["max_unwritten_tick_frames"].as_u64().unwrap() <= R1_LATENCY_BOUND_FRAMES);
        assert_eq!(haptic(&rows, "output").len() + dropped.len(), 60);

        // The same schedule through R0: no drop, the waiting queue exceeds B.
        let report = replay(&fx.request(Renderer::R0, Purpose::V2, "out_r0")).unwrap();
        assert!(report.valid, "{:?}", report.invalid_reasons);
        let (rows, _) = check_common(&fx.dir.0.join("out_r0"), &spec.ks);
        assert!(haptic(&rows, "explicit_discard").is_empty());
        assert!(rows.last().unwrap()["lanes"]["haptic"]["max_unwritten_tick_frames"].as_u64().unwrap() > R1_LATENCY_BOUND_FRAMES);
    }

    // (d)
    #[test]
    fn the_submit_lateness_of_every_tick_is_recorded_and_ties_keep_the_submit_order() {
        let mut spec = Spec::regular(8, 11_111);
        // Ticks 3 and 4 share one submit time (a tie), as do 6 and 7.
        spec.submit_rel[4] = spec.submit_rel[3];
        spec.submit_rel[7] = spec.submit_rel[6];
        let fx = fixture("lateness", &spec);
        let report = replay(&fx.request(Renderer::R0, Purpose::V2, "out")).unwrap();
        assert!(report.valid, "{:?}", report.invalid_reasons);
        let (rows, sidecar) = check_common(&fx.dir.0.join("out"), &spec.ks);
        let t0 = rows[0]["timing"]["replay_t0_us"].as_i64().unwrap();
        assert_eq!(sidecar[0]["role"], "replay_submits_meta");
        assert_eq!(sidecar[0]["replay_t0_us"], t0);
        let ledger: HashMap<u64, u64> = haptic(&rows, "submit")
            .iter()
            .map(|r| (r["seq"].as_u64().unwrap(), r["t_submit_us"].as_u64().unwrap()))
            .collect();
        let ticks = &sidecar[1..];
        assert_eq!(ticks.len(), 8);
        for (i, r) in ticks.iter().enumerate() {
            assert_eq!(r["role"], "replay_submit");
            assert_eq!(r["submit_order"], i);
            assert_eq!(r["outcome"], "submitted");
            let due = r["scheduled_submit_us"].as_i64().unwrap();
            assert_eq!(due, t0 + spec.submit_rel[i]);
            assert_eq!(r["scheduled_release_us"].as_i64().unwrap(), t0 + spec.submit_rel[i] - 40);
            let t_submit = r["t_submit_us"].as_u64().unwrap();
            assert_eq!(t_submit, ledger[&spec.ks[i]], "joined with the ledger's submit row");
            let late = r["submit_lateness_us"].as_i64().unwrap();
            assert_eq!(late, t_submit as i64 - due);
            assert!(late >= 0 && r["call_lateness_us"].as_i64().unwrap() >= 0, "never early");
            assert!(r["t_call_us"].as_u64().unwrap() <= t_submit);
        }
        let v1 = &rows[0]["v1"];
        assert_eq!(v1["submitted"], 8);
        assert_eq!(v1["not_submitted"], 0);
        assert_eq!(v1["order_violations"], 0);
        assert_eq!(v1["submit_lateness_us"]["n"], 8);
        // The tie: both ticks submitted back to back in trace order.
        let seqs: Vec<u64> = haptic(&rows, "submit").iter().map(|r| r["seq"].as_u64().unwrap()).collect();
        assert_eq!(seqs, (0..8).collect::<Vec<_>>());
    }

    /// An XRUN (or any other device failure) is recorded in a complete
    /// replay ledger, the run is invalid and the exit status is non-zero.
    #[test]
    fn an_xrun_is_recorded_and_makes_the_replay_invalid() {
        let spec = Spec::regular(20, 11_111);
        let fx = fixture("xrun", &spec);
        let mut req = fx.request(Renderer::R0, Purpose::V2, "out");
        // Stream start + 330 ms = t0 + 130 ms: after ~9 of the 20 ticks.
        req.sim_faults = vec![SimFault::Xrun { after_start_us: 330_000 }];
        let result = replay(&req);
        assert_eq!(exit_status(&result), 3);
        let report = result.unwrap();
        assert!(!report.valid);
        assert!(report.invalid_reasons.iter().any(|r| r.contains("alsa_xrun")), "{:?}", report.invalid_reasons);
        let rows = rows(&report.replay_path);
        let meta = &rows[0];
        assert_eq!(meta["outcome"]["valid"], false);
        assert_eq!(meta["outcome"]["first_integrity_failure_reason"], "alsa_xrun");
        assert_eq!(meta["outcome"]["device_failure"]["reason"], "alsa_xrun");
        assert_eq!(meta["sim_faults"][0], "Xrun { after_start_us: 330000 }");
        let end = rows.last().unwrap();
        assert_eq!(end["role"], "instrumentation_end");
        assert_eq!(end["integrity_ok"], false);
        // Still one terminal per submit: the rest end as device_failure.
        let submits = haptic(&rows, "submit").len();
        let terminals = haptic(&rows, "output").len() + haptic(&rows, "explicit_discard").len();
        assert_eq!((submits, terminals), (20, 20));
        assert!(haptic(&rows, "explicit_discard").iter().any(|r| r["reason"] == "device_failure"));
        // The other exit statuses.
        assert_eq!(exit_status(&Err(refusal("x"))), 2);
        assert_eq!(exit_status(&Err(anyhow::anyhow!("io"))), 1);
    }

    #[test]
    fn nearest_rank_statistics() {
        let v: Vec<i64> = (1..=100).collect();
        assert_eq!(nearest_rank(&v, 1, 2), Some(50));
        assert_eq!(nearest_rank(&v, 99, 100), Some(99));
        assert_eq!(nearest_rank(&[7], 99, 100), Some(7));
        assert_eq!(nearest_rank(&[], 1, 2), None);
        let s = lateness_stats(&[-3, 5, 1]);
        assert_eq!(s["median_signed"], 1);
        assert_eq!(s["abs_max"], 5);
        assert_eq!(s["min"], -3);
    }

    // (e)
    #[test]
    fn a_wav_other_than_the_traces_is_refused() {
        let fx = fixture("wav", &Spec::regular(5, 11_111));
        let mut bytes = fs::read(&fx.wav).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01;
        fs::write(&fx.wav, bytes).unwrap();
        refused(replay(&fx.request(Renderer::R0, Purpose::V2, "out")), "wav_sha256");
        assert!(!fx.dir.0.join("out").exists());
    }

    #[test]
    fn an_existing_output_directory_is_refused_and_left_untouched() {
        let fx = fixture("exists", &Spec::regular(5, 11_111));
        let out = fx.dir.0.join("out");
        fs::create_dir(&out).unwrap();
        refused(replay(&fx.request(Renderer::R0, Purpose::V2, "out")), "already exists");
        assert_eq!(fs::read_dir(&out).unwrap().count(), 0);
        // A missing parent is refused too (no create_dir_all).
        refused(replay(&fx.request(Renderer::R0, Purpose::V2, "no/such/parent")), "cannot create output directory");
    }

    #[test]
    fn the_cli_needs_exactly_one_backend_and_approval_for_a_device() {
        let base = [
            "l2_haptic_replay", "--trace", "t.json", "--renderer", "R1", "--purpose", "V3", "--out", "o", "--haptic-wav", "w.wav",
        ];
        let with = |extra: &[&str]| Cli::try_parse_from(base.iter().copied().chain(extra.iter().copied()));
        let both = with(&["--sim", "--device", "hw:9,9"]).unwrap_err();
        assert_eq!(both.kind(), clap::error::ErrorKind::ArgumentConflict);
        let neither = with(&[]).unwrap_err();
        assert_eq!(neither.kind(), clap::error::ErrorKind::MissingRequiredArgument);
        let error = with(&["--sim", "--i-have-approval"]).unwrap().request().unwrap_err();
        assert!(is_refusal(&error) && error.to_string().contains("only to --device"), "{error}");
        // R2 is here (Task C): it parses, needs --align-offset-us, defaults W to 512.
        let r2 = |extra: &[&str]| {
            Cli::try_parse_from(base.iter().map(|a| if *a == "R1" { "R2" } else { a }).chain(extra.iter().copied()))
        };
        let error = r2(&["--sim"]).unwrap().request().unwrap_err();
        assert!(is_refusal(&error) && error.to_string().contains("--align-offset-us"), "{error}");
        let request = r2(&["--sim", "--align-offset-us", "40000"]).unwrap().request().unwrap();
        assert_eq!(request.renderer, Renderer::R2 { align_offset_us: 40_000, write_ahead_frames: 512 });
        // A device without approval is refused before anything is read.
        let cli = with(&["--device", "hw:9,9"]).unwrap();
        let error = cli.request().unwrap_err();
        assert!(is_refusal(&error) && error.to_string().contains("--i-have-approval"), "{error}");
        // With approval the request names the device; it is NOT run here.
        let cli = with(&["--device", "hw:9,9", "--i-have-approval"]).unwrap();
        assert_eq!(cli.request().unwrap().backend, Backend::Device("hw:9,9".into()));
        assert!(cli.request().unwrap().sim_faults.is_empty());
        assert_eq!(with(&["--sim"]).unwrap().request().unwrap().backend, Backend::Sim);
    }

    #[test]
    fn trace_schema_type_and_binding_mismatches_are_refused() {
        let fx = fixture("schema", &Spec::regular(5, 11_111));
        let req = fx.request(Renderer::R0, Purpose::V2, "out");
        refused(prepare(&fx.request(Renderer::R1, Purpose::V2, "out")), "stage-1 purpose V2 replays with R0");
        type Edit = Box<dyn Fn(&mut Value)>;
        let cases: Vec<(Edit, &str)> = vec![
            (Box::new(|d| d["schema"] = json!("exp4-trace-v2")), "trace schema"),
            (Box::new(|d| d["ticks"][1]["k"] = json!(1.0)), "non-negative integer"),
            (Box::new(|d| d["ticks"][1]["event_id"] = json!(true)), "non-negative integer"),
            (Box::new(|d| d["ticks"][2]["t_submit_rel_us"] = json!(0)), "> t_submit_rel_us"),
            (Box::new(|d| {
                d["ticks"][2]["t_release_rel_us"] = json!(0);
                d["ticks"][2]["t_submit_rel_us"] = json!(0);
            }), "< the previous"),
            (Box::new(|d| d["ticks"][2]["submit_order"] = json!(7)), "submit_order"),
            (Box::new(|d| d["ticks"][2]["pts_us"] = json!(1)), "sender rule"),
            (Box::new(|d| d["ticks"][2]["samples_8k_ledger"] = json!(88)), "samples_8k_ledger"),
            (Box::new(|d| d["ticks"][0].as_object_mut().unwrap().remove("samples_8k_ledger").map(drop).unwrap()), "is missing"),
            (Box::new(|d| d["alsa"]["period_frames"] = json!(512)), "alsa.period_frames"),
            (Box::new(|d| d["integrity"]["haptic_submits"] = json!(4)), "haptic_submits"),
            (Box::new(|d| d["stream_epoch_rel_t0_us"]["t_start_us"] = json!(-1)), "stream_epoch_rel_t0_us.t_start_us"),
            (Box::new(|d| d["wav_sha256"] = json!("d".repeat(64))), "wav.sha256"),
        ];
        for (edit, needle) in cases {
            fx.rewrite(edit);
            refused(prepare(&req), needle);
        }
        assert!(!fx.dir.0.join("out").exists());
    }
    // ---- R2 (Task C) --------------------------------------------------------

    const R2_C0: Renderer = Renderer::R2 { align_offset_us: 40_000, write_ahead_frames: 512 };

    /// The R2 flags: refused with R0 / R1, required (c) for R2, W defaults to
    /// 512; the purpose binding V4a / V4b <-> R2; an out-of-range W or a
    /// negative c is refused before anything is written.
    #[test]
    fn the_r2_flags_and_purposes_bind_to_the_aligned_renderer() {
        let cli = |args: &[&str]| {
            let base = ["l2_haptic_replay", "--trace", "t.json", "--out", "o", "--haptic-wav", "w.wav", "--sim"];
            Cli::try_parse_from(base.iter().copied().chain(args.iter().copied())).unwrap().request()
        };
        for (renderer, purpose) in [("R0", "V2"), ("R1", "V3")] {
            for flag in [["--align-offset-us", "40000"], ["--write-ahead-frames", "512"]] {
                let args = ["--renderer", renderer, "--purpose", purpose, flag[0], flag[1]];
                let error = cli(&args).unwrap_err();
                assert!(is_refusal(&error) && error.to_string().contains("only to --renderer R2"), "{error}");
            }
        }
        let r = cli(&["--renderer", "R2", "--purpose", "V4b", "--align-offset-us", "23456", "--write-ahead-frames", "768"]).unwrap();
        assert_eq!(r.renderer, Renderer::R2 { align_offset_us: 23_456, write_ahead_frames: 768 });
        assert_eq!(r.purpose, Purpose::V4b);
        assert_eq!(cli(&["--renderer", "R2", "--purpose", "V4a", "--align-offset-us", "40000"]).unwrap().renderer, R2_C0);
        let fx = fixture("r2-binding", &Spec::regular(5, 11_111));
        refused(prepare(&fx.request(R2_C0, Purpose::V2, "out")), "stage-1 purpose V2 replays with R0, not R2");
        refused(prepare(&fx.request(Renderer::R0, Purpose::V4a, "out")), "stage-1 purpose V4a replays with R2, not R0");
        refused(prepare(&fx.request(Renderer::R1, Purpose::V4b, "out")), "replays with R2, not R1");
        for bad in [
            Renderer::R2 { align_offset_us: -1, write_ahead_frames: 512 },
            Renderer::R2 { align_offset_us: 40_000, write_ahead_frames: 0 },
            Renderer::R2 { align_offset_us: 40_000, write_ahead_frames: ALSA_DEFAULT_BUFFER_FRAMES },
        ] {
            refused(prepare(&fx.request(bad, Purpose::V4a, "out")), "renderer R2 placement");
        }
        prepare(&fx.request(R2_C0, Purpose::V4a, "out")).unwrap();
        assert!(!fx.dir.0.join("out").exists());
    }

    /// Acceptance (Task C 5): R2 with c = 40,000 µs and W = 512 on the
    /// simulated stream: an `event-pair-l2-v1.8` ledger with alsa.placement
    /// aligned, one placement row right after each submit, one terminal per
    /// submit, and every existing replay integrity check passes (exit 0).
    /// At real-time pace floor (~11-16 ms) < c, so uncut ticks are output at
    /// t_release + c within the snapshot quantisation. (t_release is the
    /// SCHEDULED release, so a submit thread that wakes > ~24 ms late under
    /// load makes that tick device_overlap or too_late without invalidating
    /// the replay: the per-tick alignment is asserted only for uncut ticks,
    /// and most ticks must be uncut.)
    #[test]
    fn an_r2_sim_replay_writes_a_v1_8_aligned_ledger_that_passes_the_replay_checks() {
        let spec = Spec::regular(30, 11_111);
        let fx = fixture("r2", &spec);
        let result = replay(&fx.request(R2_C0, Purpose::V4a, "out"));
        assert_eq!(exit_status(&result), 0, "{:?}", result.as_ref().map(|r| r.invalid_reasons.clone()));
        let out = fx.dir.0.join("out");
        let replay_bytes = fs::read(out.join(REPLAY_FILE)).unwrap();
        let engine = fs::read(out.join(L2_FILE)).unwrap();
        let cut = replay_bytes.iter().position(|b| *b == b'\n').unwrap() + 1;
        assert_eq!(&replay_bytes[cut..], &engine[..], "engine rows verbatim after the head row");
        let rows = rows(&out.join(REPLAY_FILE));
        let meta = &rows[0];
        assert_eq!(meta["role"], "replay_meta");
        assert_eq!(meta["files"]["l2_rows"]["sha256"], sha256_hex(&engine));
        assert_eq!((meta["renderer"].as_str(), meta["placement"].as_str(), meta["stage1_purpose"].as_str()), (Some("R2"), Some("aligned"), Some("V4a")));
        assert_eq!((meta["align_offset_us"].as_i64(), meta["write_ahead_frames"].as_u64()), (Some(40_000), Some(512)));
        assert_eq!(meta["latency_bound_frames"], Value::Null);
        assert_eq!(meta["l2_schema"], "event-pair-l2-v1.8");
        assert_eq!(meta["run_id"], "exp4r-R2-exp3-L2-direct-D1-bin-k0-a1-p0-P0A");
        assert_eq!(meta["sink"]["placement"], "aligned");
        assert_eq!(meta["outcome"]["valid"], true);
        assert_eq!(meta["outcome"]["placement_rows"], 30);
        let l2_meta = &rows[1];
        assert_eq!(l2_meta["l2_schema"], "event-pair-l2-v1.8");
        assert_eq!(l2_meta["method"], "replay");
        assert_eq!((l2_meta["alsa"]["placement"].as_str(), l2_meta["alsa"]["align_offset_us"].as_i64()), (Some("aligned"), Some(40_000)));
        assert_eq!(l2_meta["alsa"]["write_ahead_frames"], 512);
        assert_eq!(l2_meta["alsa"]["placement_policy"], skew_moq::l2_output::ALIGNED_PLACEMENT_POLICY);
        let end = rows.last().unwrap();
        assert_eq!(end["role"], "instrumentation_end");
        assert_eq!(end["integrity_ok"], true);
        assert!(end["lanes"]["haptic"]["device_failure"].is_null());
        let body = &rows[2..rows.len() - 1];
        let submits: Vec<usize> = (0..body.len()).filter(|&i| body[i]["role"] == "submit").collect();
        let seqs: Vec<u64> = submits.iter().map(|&i| body[i]["seq"].as_u64().unwrap()).collect();
        assert_eq!(seqs, spec.ks, "every tick submitted, in trace submit order");
        for &i in &submits {
            assert_eq!((body[i + 1]["role"].as_str(), &body[i + 1]["seq"]), (Some("placement"), &body[i]["seq"]), "placement right after its submit");
        }
        let placements = haptic(&rows, "placement");
        assert_eq!(placements.len(), 30, "one placement row per submission");
        let sidecar = rows_of(&out.join(SUBMITS_FILE));
        for p in &placements {
            assert!(p["planned_end_after"].as_u64() >= p["appl_pos"].as_u64());
            // R2 aligns to the replay's scheduled release (sidecar).
            let side = sidecar.iter().find(|r| r["seq"] == p["seq"]).unwrap();
            assert_eq!(p["t_release_us"], side["scheduled_release_us"]);
        }
        let mut terminals: HashMap<u64, usize> = HashMap::new();
        for r in rows.iter().filter(|r| r["track"] == "haptic" && (r["role"] == "output" || r["role"] == "explicit_discard")) {
            *terminals.entry(r["seq"].as_u64().unwrap()).or_default() += 1;
        }
        assert!(spec.ks.iter().all(|k| terminals.get(k) == Some(&1)), "exactly one terminal per submit");
        let outputs = haptic(&rows, "output");
        let uncut: Vec<&&Value> = outputs.iter().filter(|o| o["output_detail"]["head_cut_frames"] == 0).collect();
        assert!(uncut.len() >= 25, "at real-time pace (nearly) every tick is placed uncut: {} of 30", uncut.len());
        for o in &uncut {
            let lag = o["t_output_us"].as_i64().unwrap() - o["t_release_us"].as_i64().unwrap();
            assert!((lag - 40_000).abs() <= 1_000, "seq {}: output - release {lag} us", o["seq"]);
        }
        let lane = &end["lanes"]["haptic"];
        assert_eq!(lane["placement_rows"], 30);
        assert_eq!(lane["occluded_discards"], lane["ticks_occluded"]);
        assert_eq!(lane["xrun"], 0);
        assert_eq!(meta["outcome"]["trim_rows"], haptic(&rows, "trim").len());
    }

    /// The R0 ledger is still the legacy v1.7 one (no aligned row or key).
    #[test]
    fn r0_and_r1_replays_stay_legacy_v1_7() {
        let spec = Spec::regular(10, 11_111);
        let fx = fixture("r0-legacy", &spec);
        let report = replay(&fx.request(Renderer::R0, Purpose::V2, "out")).unwrap();
        assert!(report.valid, "{:?}", report.invalid_reasons);
        let (rows, _) = check_common(&fx.dir.0.join("out"), &spec.ks);
        assert_eq!(rows[0]["l2_schema"], L2_SCHEMA);
        assert_eq!(rows[0]["placement"], "fifo");
        assert!(rows[0]["outcome"].get("placement_rows").is_none() && rows[0]["sink"].get("placement").is_none());
        assert!(rows[1]["alsa"].get("placement").is_none());
        assert!(haptic(&rows, "placement").is_empty() && haptic(&rows, "trim").is_empty());
        assert!(rows.last().unwrap()["lanes"]["haptic"].get("ticks_occluded").is_none());
    }
}
