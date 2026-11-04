//! Legacy guard for the haptic placement engine (experiment 4, Task C).
//!
//! The experiment-2/3 sink (FIFO, and sink mode A with a latency bound `B`)
//! must stay byte-identical when the experiment-4 placement modes are added
//! (`md/20261104_실험4_계약.md` §6 "FIFO 분기의 diff가 0행"). Lane rows carry
//! real clock reads, so the guard drives the ENGINE on a virtual clock with
//! the simulated PCM and serialises everything the lane would turn into
//! ledger content: every offer's outcome (latency drops, admission), every
//! observed output (`t_output_us`, `t_callback_us`, the `output_detail`
//! JSON), device failures with their abandoned submissions, and the final
//! counters JSON (`lanes.haptic` engine part). The JSONL's sha256 was taken
//! from the same scenarios run against moq-rs `7a2acd1` (before Task C); the
//! only source difference between the two harness copies is the
//! `PlacementConfig` literal (`mode: PlacementMode::None` exists only after).
//!
//! Set `SKEW_L2_LEGACY_GUARD_OUT=<path>` to write the JSONL for a byte
//! comparison (`cmp`).

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use skew_moq::l2_haptic::{
    HapticPcm, HapticPlacement, PlacementConfig, PlacementMode, SimFault, SimPcm, SimPcmConfig,
};
use skew_moq::l2_output::{
    AlsaBackend, AlsaSinkConfig, FakeSinkConfig, L2Config, L2Identity, L2Modality, L2Output, L2SinkKind, L2Sinks,
    L2Submission,
};

/// sha256 of the guard JSONL produced by moq-rs `7a2acd1` (before Task C).
const HEAD_7A2ACD1_SHA256: &str = "24bb0004a144ffb668277556eee2236f085967419993f6686ac507ccb029f602";

const PERIOD: u64 = 256;
const BUFFER: u64 = 2_048;

fn cfg(queue_limit_frames: u64, latency_bound_frames: Option<u64>) -> PlacementConfig {
    PlacementConfig {
        period_frames: PERIOD,
        buffer_frames: BUFFER,
        target_fill_frames: 512,
        prefill_frames: 512,
        queue_limit_frames,
        latency_bound_frames,
        mode: PlacementMode::None,
    }
}

fn submission(seq: u32, t_release_us: u64, t_submit_us: u64) -> L2Submission {
    L2Submission {
        identity: L2Identity {
            modality: L2Modality::Haptic,
            tier: 0,
            seq,
            pts_us: seq as u64 * 1_000_000 / 90,
            event_id: if seq % 3 == 0 { seq / 3 + 1 } else { 0 },
            route: None,
        },
        t_release_us,
        t_submit_us,
    }
}

/// The registered 88/89-sample split of tick `seq`, deterministic content.
fn payload(seq: u32) -> Vec<u8> {
    let (start, end) = skew_moq::pcm_sample_bounds(seq as u64, 8_000, 90);
    (start..end).flat_map(|i| (((i * 7_919) % 20_000) as i16 - 10_000).to_le_bytes()).collect()
}

/// A tiny deterministic generator (xorshift64*), no extra dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

struct Scenario {
    name: &'static str,
    config: PlacementConfig,
    sim: SimPcmConfig,
    seed: u64,
    ticks: u32,
    /// Mean spacing of the offers (µs); 11,111 = real time.
    spacing_us: u64,
    /// Every this many ticks, a burst of `burst_len` offers at one instant.
    burst_every: u32,
    burst_len: u32,
    /// Offer one tick in this many with an unplayable payload (0 = never).
    invalid_every: u32,
}

fn run(s: &Scenario, out: &mut Vec<Value>) {
    let t0 = 1_000_000u64;
    let mut pcm = SimPcm::new(s.sim.clone());
    let mut engine = HapticPlacement::new(s.config, pcm.params()).expect("valid config");
    let mut clock = || t0;
    engine.prefill(&mut pcm, &mut clock).expect("prefill");
    pcm.start(t0).expect("start");
    let first = pcm.status(t0).expect("first status");
    engine.begin_epoch(t0, first, t0).expect("epoch");
    out.push(json!({"scenario": s.name, "ev": "begin", "written": engine.written_frames()}));
    let mut rng = Rng(s.seed);
    let mut t = t0;
    let mut failed = false;
    let step = |engine: &mut HapticPlacement, pcm: &mut SimPcm, t: u64, out: &mut Vec<Value>| -> bool {
        let status = match pcm.status(t) {
            Ok(status) => status,
            Err(error) => {
                let f = engine.failure_from_error(&error, None, t, "status");
                out.push(json!({"scenario": s.name, "ev": "failure", "t": t, "failure": f.to_json()}));
                return false;
            }
        };
        match engine.observe(&status, t, t + 3) {
            Err(f) => {
                out.push(json!({"scenario": s.name, "ev": "failure", "t": t, "failure": f.to_json()}));
                return false;
            }
            Ok(placed) => {
                for p in placed {
                    out.push(json!({
                        "scenario": s.name, "ev": "output", "seq": p.submission.identity.seq,
                        "t_release_us": p.submission.t_release_us, "t_submit_us": p.submission.t_submit_us,
                        "t_output_us": p.t_output_us, "t_callback_us": p.t_callback_us,
                        "output_detail": p.detail.to_json(),
                    }));
                }
            }
        }
        let mut c = t + 5;
        let mut clock = || {
            c += 7;
            c
        };
        if let Err(error) = engine.fill(pcm, status.avail, &mut clock) {
            let after = pcm.status(t + 1).ok();
            let f = engine.failure_from_error(&error, after.as_ref(), t + 1, "writei");
            out.push(json!({"scenario": s.name, "ev": "failure", "t": t, "failure": f.to_json()}));
            return false;
        }
        true
    };
    let mut k = 0u32;
    while k < s.ticks && !failed {
        let jitter = rng.below(s.spacing_us / 2 + 1);
        let due = t0 + 20_000 + k as u64 * s.spacing_us + jitter;
        while t + 1_000 <= due {
            t += 1_000;
            if !step(&mut engine, &mut pcm, t, out) {
                failed = true;
                break;
            }
        }
        if failed {
            break;
        }
        let n = if s.burst_every > 0 && k % s.burst_every == s.burst_every - 1 { s.burst_len } else { 1 };
        for _ in 0..n {
            if k >= s.ticks {
                break;
            }
            let release = t.saturating_sub(rng.below(3_000));
            let sub = submission(k, release, t);
            let invalid = s.invalid_every > 0 && k % s.invalid_every == s.invalid_every - 1;
            let bytes = payload(k);
            let offered = engine.offer(sub, if invalid { None } else { Some(&bytes) });
            out.push(json!({
                "scenario": s.name, "ev": "offer", "seq": k, "t": t,
                "latency_dropped": offered.latency_dropped.iter().map(|d| d.identity.seq).collect::<Vec<_>>(),
                "admission": format!("{:?}", offered.admission),
                "pending_frames": engine.pending_frames(),
                "unwritten_tick_frames": engine.unwritten_tick_frames(),
            }));
            k += 1;
        }
    }
    let mut drained = 0;
    while !failed && !engine.idle() && drained < 5_000 {
        t += 1_000;
        drained += 1;
        if !step(&mut engine, &mut pcm, t, out) {
            failed = true;
        }
    }
    if failed {
        let abandoned: Vec<u32> = engine.abandon().iter().map(|a| a.identity.seq).collect();
        out.push(json!({"scenario": s.name, "ev": "abandon", "seqs": abandoned}));
    }
    out.push(json!({
        "scenario": s.name, "ev": "end", "t": t, "idle": engine.idle(),
        "tick_tail_played": engine.tick_tail_played(), "unplayed_tick_frames": engine.unplayed_tick_frames(),
        "written": engine.written_frames(), "pending_frames": engine.pending_frames(),
        "stats": engine.stats().to_json(),
    }));
}

fn scenarios() -> Vec<Scenario> {
    let sim = SimPcmConfig::new(PERIOD, BUFFER);
    let mut short = SimPcmConfig::new(PERIOD, BUFFER);
    short.max_frames_per_write = Some(64);
    short.driver_delay_frames = 32;
    let mut xrun = SimPcmConfig::new(PERIOD, BUFFER);
    xrun.faults.push(SimFault::Xrun { after_start_us: 2_500_000 });
    vec![
        Scenario { name: "fifo_realtime_bursts", config: cfg(48_000, None), sim: sim.clone(), seed: 1, ticks: 900, spacing_us: 11_111, burst_every: 37, burst_len: 6, invalid_every: 0 },
        Scenario { name: "fifo_compressed", config: cfg(48_000, None), sim: sim.clone(), seed: 2, ticks: 300, spacing_us: 5_555, burst_every: 50, burst_len: 10, invalid_every: 97 },
        Scenario { name: "fifo_short_writes_driver_delay", config: cfg(48_000, None), sim: short.clone(), seed: 3, ticks: 400, spacing_us: 11_111, burst_every: 23, burst_len: 4, invalid_every: 0 },
        Scenario { name: "fifo_small_q", config: cfg(2 * 534, None), sim: sim.clone(), seed: 4, ticks: 200, spacing_us: 4_000, burst_every: 10, burst_len: 5, invalid_every: 0 },
        Scenario { name: "sink_a_b1068_compressed", config: cfg(48_000, Some(1_068)), sim: sim.clone(), seed: 5, ticks: 400, spacing_us: 5_555, burst_every: 30, burst_len: 8, invalid_every: 61 },
        Scenario { name: "sink_a_b1068_short_writes", config: cfg(48_000, Some(1_068)), sim: short, seed: 6, ticks: 300, spacing_us: 7_000, burst_every: 17, burst_len: 5, invalid_every: 0 },
        Scenario { name: "sink_a_b600_q600", config: cfg(600, Some(600)), sim: sim.clone(), seed: 7, ticks: 150, spacing_us: 6_000, burst_every: 9, burst_len: 4, invalid_every: 0 },
        Scenario { name: "fifo_xrun", config: cfg(48_000, None), sim: xrun.clone(), seed: 8, ticks: 600, spacing_us: 11_111, burst_every: 40, burst_len: 3, invalid_every: 0 },
        Scenario { name: "sink_a_xrun", config: cfg(48_000, Some(1_068)), sim: xrun, seed: 9, ticks: 600, spacing_us: 9_000, burst_every: 40, burst_len: 6, invalid_every: 0 },
    ]
}

/// The L2 meta row of a legacy ALSA configuration (simulated stream, real
/// clock), with the clock-dependent values masked: `t_us` and the
/// `alsa.stream_epoch` times and first-snapshot fill (which depend on when
/// the lane thread ran). Everything else — every policy text, contract,
/// schema tag and parameter — must be byte-identical.
fn masked_meta(name: &str, pc: L2SinkKind, latency_bound_frames: Option<u64>) -> Value {
    let dir = std::env::temp_dir().join(format!("skew-l2-legacy-guard-{}-{}", std::process::id(), skew_moq::now_us()));
    std::fs::create_dir(&dir).expect("temp dir");
    let path = dir.join(format!("{name}.jsonl"));
    let config = L2Config {
        run_id: format!("legacy-guard-{name}"),
        method: "s1".into(),
        sinks: L2Sinks { pc, haptic: L2SinkKind::Alsa },
        fake: (pc == L2SinkKind::Fake).then(|| FakeSinkConfig::offsets(1_000, 0)),
        alsa: Some(AlsaSinkConfig {
            backend: AlsaBackend::Simulated(SimPcmConfig::new(PERIOD, BUFFER)),
            placement: cfg(48_000, latency_bound_frames),
            status_interval_us: 1_000,
            ready_timeout_us: 5_000_000,
            release_timeout_us: 5_000_000,
            diagnostics: None,
        }),
        wayland: None,
        queue_capacity: 256,
        row_capacity: 65_536,
        report_window_us: 200_000,
        max_tracked_objects: 65_536,
    };
    let out = L2Output::start(&path, config).expect("start the legacy stage");
    out.finish().expect("finish");
    let text = std::fs::read_to_string(&path).expect("read the L2 file");
    let _ = std::fs::remove_dir_all(&dir);
    let mut meta: Value = serde_json::from_str(text.lines().next().expect("meta row")).expect("meta JSON");
    meta["t_us"] = json!("masked");
    let epoch = meta["alsa"]["stream_epoch"].as_object_mut().expect("stream_epoch");
    for key in ["t_start_us", "trigger_htstamp_us", "first_status_t0_us", "first_status_htstamp_us", "first_status_delay_frames", "first_status_avail_frames"] {
        assert!(epoch.contains_key(key), "{key}");
        epoch.insert(key.into(), json!("masked"));
    }
    json!({"scenario": name, "ev": "meta", "meta": meta})
}

#[test]
fn legacy_fifo_and_sink_a_engine_rows_are_byte_identical_to_7a2acd1() {
    let mut rows = Vec::new();
    for s in scenarios() {
        run(&s, &mut rows);
    }
    rows.push(masked_meta("meta_fifo_pc_null", L2SinkKind::Null, None));
    rows.push(masked_meta("meta_sink_a_pc_null", L2SinkKind::Null, Some(1_068)));
    rows.push(masked_meta("meta_fifo_pc_fake", L2SinkKind::Fake, None));
    let mut bytes = Vec::new();
    for r in &rows {
        bytes.extend_from_slice(serde_json::to_string(r).unwrap().as_bytes());
        bytes.push(b'\n');
    }
    if let Ok(path) = std::env::var("SKEW_L2_LEGACY_GUARD_OUT") {
        std::fs::write(&path, &bytes).expect("write the guard JSONL");
    }
    let digest = format!("{:x}", Sha256::digest(&bytes));
    eprintln!("legacy guard: {} rows, sha256 {digest}", rows.len());
    assert!(rows.len() > 5_000, "the scenarios must exercise the engine ({} rows)", rows.len());
    assert_eq!(digest, HEAD_7A2ACD1_SHA256, "legacy engine output changed");
}
