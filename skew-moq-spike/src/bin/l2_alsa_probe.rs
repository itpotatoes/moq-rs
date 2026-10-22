//! `l2_alsa_probe` — L2 haptic ALSA output probe (experiment 1-D, milestone
//! M2; registration §3-3, §3-3a, §3-6).
//!
//! Plays a deterministic tick pattern through the SAME haptic ALSA sink the
//! receiver uses (`skew_moq::l2_output`: haptic lane `alsa`, PC lane `null`),
//! writes a schema v1.2 L2 JSONL and prints ONE JSON report: the measured
//! status cadence, the writer's max write lag, and the `report_lag =
//! t_log_arrival_us - t_output_us` distribution that §3-6 needs to fix
//! `G_report` (and §3-3a its F/Q/period/buffer values). It reports no
//! experimental effect of any method.
//!
//! IT OPENS THE REAL PCM DEVICE (default `hw:1,0`). Running it needs the
//! user's explicit approval, and it refuses to start without
//! `--i-have-approval`. It must run while PipeWire has released card1: hold
//! the ReserveDevice1 lock of card1 (`Audio1`) for the probe's WHOLE
//! lifetime, e.g.
//!
//! ```sh
//! pw-reserve -n Audio1 -r &      # ask the holder (PipeWire/WirePlumber) to release card1; keep it
//! RESERVE=$!
//! sleep 1                         # let the release complete
//! target/debug/l2_alsa_probe --i-have-approval --l2-log /fresh/path/probe-l2.jsonl
//! kill "$RESERVE"; wait "$RESERVE"   # give card1 back
//! ```
//!
//! A device that is still held makes the probe fail at open
//! (`snd_pcm_open` `-EBUSY`, `SND_PCM_NONBLOCK`) without touching anything
//! else; the L2 file is then left without a meta row.
//!
//! Tick pattern (deterministic, `--ticks` at 90 Hz from the start): tick `k`
//! is released at its slot `t0 + floor(k * 1e6 / 90)` µs with the
//! registered 8 kHz split (88/89 samples) of a 200 Hz sine table, except in
//! every 90-tick cycle: ticks 60..=64 are held and released TOGETHER at the
//! slot of tick 64 (a 5-tick burst, placed back to back), and ticks 80..=82
//! are never released (the stream plays silence there). Exit code: 0 when
//! the L2 sentinel says `integrity_ok`, 1 otherwise, 2 for a usage error or
//! a missing approval.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::Parser;

use skew_moq::l2_haptic::PlacementConfig;
use skew_moq::l2_output::{
    AlsaBackend, AlsaSinkConfig, L2Config, L2Identity, L2Modality, L2Output, L2SinkKind, L2Sinks,
    ALSA_DEFAULT_BUFFER_FRAMES, ALSA_DEFAULT_DEVICE, ALSA_DEFAULT_PERIOD_FRAMES, ALSA_DEFAULT_PREFILL_FRAMES,
    ALSA_DEFAULT_QUEUE_FRAMES, ALSA_DEFAULT_STATUS_INTERVAL_US, ALSA_DEFAULT_TARGET_FILL_FRAMES,
    ALSA_READY_TIMEOUT_US,
};
use skew_moq::{now_us, pcm_tick_payload, timestamp_us, PCM_SAMPLE_RATE_HZ};

const HAPTIC_RATE_HZ: u64 = 90;
const CYCLE: u64 = 90;
const BURST: std::ops::RangeInclusive<u64> = 60..=64;
const GAP: std::ops::RangeInclusive<u64> = 80..=82;

#[derive(Parser, Debug)]
#[command(about = "L2 haptic ALSA output probe (opens the real PCM device; needs --i-have-approval)")]
struct Args {
    /// Required: the user approved running against the real audio device
    /// AND card1 is reserved (`pw-reserve -n Audio1 -r`) for this run.
    #[arg(long)]
    i_have_approval: bool,
    /// The L2 JSONL to create (must not exist).
    #[arg(long)]
    l2_log: Option<PathBuf>,
    #[arg(long, default_value = "l2-alsa-probe")]
    run_id: String,
    #[arg(long, default_value = ALSA_DEFAULT_DEVICE)]
    device: String,
    #[arg(long, default_value_t = ALSA_DEFAULT_PERIOD_FRAMES)]
    period_frames: u64,
    #[arg(long, default_value_t = ALSA_DEFAULT_BUFFER_FRAMES)]
    buffer_frames: u64,
    #[arg(long, default_value_t = ALSA_DEFAULT_TARGET_FILL_FRAMES)]
    target_fill_frames: u64,
    #[arg(long, default_value_t = ALSA_DEFAULT_PREFILL_FRAMES)]
    prefill_frames: u64,
    #[arg(long, default_value_t = ALSA_DEFAULT_QUEUE_FRAMES)]
    queue_frames: u64,
    #[arg(long, default_value_t = ALSA_DEFAULT_STATUS_INTERVAL_US)]
    status_interval_us: u64,
    /// Ticks of the pattern (900 = 10 s at 90 Hz).
    #[arg(long, default_value_t = 900)]
    ticks: u64,
    /// Close grace of the output stage (anchored at its close).
    #[arg(long, default_value_t = 1_000)]
    report_window_ms: u64,
}

/// The approval gate: nothing is opened unless the flag is given.
fn approval(args: &Args) -> Result<()> {
    if !args.i_have_approval {
        bail!(
            "refusing to start: l2_alsa_probe opens the real PCM device ({}). Run it only with the \
             user's explicit approval, while card1 is reserved for the probe's whole lifetime \
             (`pw-reserve -n Audio1 -r &` ... `kill $!`), and pass --i-have-approval.",
            args.device
        );
    }
    Ok(())
}

/// `(release slot k, ticks released at that slot)` in release order.
fn release_plan(ticks: u64) -> Vec<(u64, Vec<u64>)> {
    let mut plan = Vec::new();
    let mut held = Vec::new();
    for k in 0..ticks {
        let phase = k % CYCLE;
        if GAP.contains(&phase) {
            continue;
        }
        if BURST.contains(&phase) {
            held.push(k);
            if phase == *BURST.end() {
                plan.push((k, std::mem::take(&mut held)));
            }
            continue;
        }
        plan.push((k, vec![k]));
    }
    if !held.is_empty() {
        // A pattern cut inside a burst releases the held ticks at the end.
        let last = *held.last().expect("non-empty");
        plan.push((last, held));
    }
    plan
}

/// One second of a 200 Hz sine at 8 kHz, S16_LE mono (the tick source).
fn sine_table() -> Vec<u8> {
    (0..PCM_SAMPLE_RATE_HZ)
        .flat_map(|i| {
            let phase = 2.0 * std::f64::consts::PI * 200.0 * i as f64 / PCM_SAMPLE_RATE_HZ as f64;
            ((phase.sin() * 6_000.0).round() as i16).to_le_bytes()
        })
        .collect()
}

/// Linear-interpolated quantile (numpy `method='linear'`) of sorted values.
fn quantile(sorted: &[i64], q: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let position = q * (sorted.len() - 1) as f64;
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    let t = position - lower as f64;
    let (a, b) = (sorted[lower] as f64, sorted[upper] as f64);
    // numpy's `_lerp`: the form that is exact at each end.
    Some(if t >= 0.5 { b - (b - a) * (1.0 - t) } else { a + (b - a) * t })
}

fn distribution(mut values: Vec<i64>) -> serde_json::Value {
    values.sort_unstable();
    serde_json::json!({
        "n": values.len(),
        "min": values.first(),
        "p50": quantile(&values, 0.50),
        "p95": quantile(&values, 0.95),
        "p99": quantile(&values, 0.99),
        "max": values.last(),
    })
}

fn read_rows(path: &Path) -> Result<Vec<serde_json::Value>> {
    let text = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    text.lines()
        .map(|line| serde_json::from_str(line).with_context(|| format!("bad L2 row: {line}")))
        .collect()
}

/// The probe report from the written L2 file (and the plan).
fn summarize(rows: &[serde_json::Value], plan: &[(u64, Vec<u64>)], ticks: u64) -> serde_json::Value {
    let meta = rows.first().cloned().unwrap_or(serde_json::Value::Null);
    let end = rows
        .last()
        .filter(|r| r["role"] == "instrumentation_end")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let outputs: Vec<&serde_json::Value> = rows.iter().filter(|r| r["role"] == "output").collect();
    let field = |r: &serde_json::Value, k: &str| r[k].as_i64().unwrap_or(0);
    let mut discard_reasons: BTreeMap<String, u64> = BTreeMap::new();
    for d in rows.iter().filter(|r| r["role"] == "explicit_discard") {
        *discard_reasons.entry(d["reason"].as_str().unwrap_or("?").to_string()).or_default() += 1;
    }
    let mut actual_types: BTreeMap<String, u64> = BTreeMap::new();
    for o in &outputs {
        let t = o["output_detail"]["audio_tstamp_type_actual"].as_str().unwrap_or("?").to_string();
        *actual_types.entry(t).or_default() += 1;
    }
    let driver_delay: Vec<i64> = outputs.iter().map(|o| o["output_detail"]["driver_delay_frames"].as_i64().unwrap_or(0)).collect();
    let lane = &end["lanes"]["haptic"];
    serde_json::json!({
        "probe": "l2_alsa_probe",
        "l2_schema": meta["l2_schema"],
        "run_id": meta["run_id"],
        "alsa": meta["alsa"],
        "ticks_planned": ticks,
        "ticks_released": plan.iter().map(|(_, t)| t.len()).sum::<usize>(),
        "release_slots": plan.len(),
        "bursts": plan.iter().filter(|(_, t)| t.len() > 1).count(),
        "sentinel_present": !end.is_null(),
        "integrity_ok": end["integrity_ok"],
        "first_integrity_failure_reason": end["first_integrity_failure_reason"],
        "first_integrity_failure_us": end["first_integrity_failure_us"],
        "submitted": end["submitted"],
        "output": end["output"],
        "discarded_before_submit": end["discarded_before_submit"],
        "discarded_after_submit": end["discarded_after_submit"],
        "submitted_unresolved": end["submitted_unresolved"],
        "discard_reasons": discard_reasons,
        "status_cadence": {
            "status_calls": lane["status_calls"],
            "status_gap_max_us": lane["status_gap_max_us"],
            "status_gap_hist_upper_us": lane["status_gap_hist_upper_us"],
            "status_gap_hist": lane["status_gap_hist"],
        },
        "max_write_lag_us": end["max_write_lag_us"],
        "max_callback_to_log_us": end["max_callback_to_log_us"],
        "report_lag_us": distribution(outputs.iter().map(|o| field(o, "t_log_arrival_us") - field(o, "t_output_us")).collect()),
        "callback_lag_us": distribution(outputs.iter().map(|o| field(o, "t_callback_us") - field(o, "t_output_us")).collect()),
        "output_minus_submit_us": distribution(outputs.iter().map(|o| field(o, "t_output_us") - field(o, "t_submit_us")).collect()),
        "driver_delay_frames": { "min": driver_delay.iter().min(), "max": driver_delay.iter().max() },
        "audio_tstamp_type_actual": actual_types,
        "lane": lane,
    })
}

fn run(args: &Args) -> Result<bool> {
    let path = args.l2_log.clone().context("--l2-log is required")?;
    let sinks = L2Sinks { pc: L2SinkKind::Null, haptic: L2SinkKind::Alsa };
    let config = L2Config {
        run_id: args.run_id.clone(),
        method: "probe".into(),
        sinks,
        fake: None,
        alsa: Some(AlsaSinkConfig {
            backend: AlsaBackend::Device { name: args.device.clone() },
            placement: PlacementConfig {
                period_frames: args.period_frames,
                buffer_frames: args.buffer_frames,
                target_fill_frames: args.target_fill_frames,
                prefill_frames: args.prefill_frames,
                queue_limit_frames: args.queue_frames,
            },
            status_interval_us: args.status_interval_us,
            ready_timeout_us: ALSA_READY_TIMEOUT_US,
        }),
        queue_capacity: 256,
        row_capacity: 65_536,
        report_window_us: args.report_window_ms.saturating_mul(1_000),
        max_tracked_objects: 65_536,
    };
    config.validate()?;
    let table = sine_table();
    let plan = release_plan(args.ticks);
    // Opens, prefills and starts the stream (fails fast if the card is held).
    let output = L2Output::start(&path, config)?;
    let submitter = output.submitter();
    let t0 = now_us();
    for (slot, ticks) in &plan {
        let due = t0 + timestamp_us(*slot, HAPTIC_RATE_HZ);
        let now = now_us();
        if due > now {
            std::thread::sleep(Duration::from_micros(due - now));
        }
        for &k in ticks {
            let payload = pcm_tick_payload(&table, k, PCM_SAMPLE_RATE_HZ, HAPTIC_RATE_HZ)?;
            let identity = L2Identity {
                modality: L2Modality::Haptic,
                tier: 0,
                seq: k as u32,
                pts_us: timestamp_us(k, HAPTIC_RATE_HZ),
                event_id: if k % 3 == 0 { (k / 3 + 1) as u32 } else { 0 },
                route: None,
            };
            submitter.submit_payload(identity, now_us(), Some(bytes::Bytes::from(payload)));
        }
    }
    drop(submitter);
    let summary = output.finish()?;
    let rows = read_rows(&path)?;
    let report = summarize(&rows, &plan, args.ticks);
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(summary.integrity_ok(sinks))
}

fn main() {
    let args = match Args::try_parse() {
        Ok(args) => args,
        Err(error) => {
            let _ = error.print();
            std::process::exit(2);
        }
    };
    if let Err(error) = approval(&args) {
        eprintln!("{error:#}");
        std::process::exit(2);
    }
    match run(&args) {
        Ok(true) => {}
        Ok(false) => std::process::exit(1),
        Err(error) => {
            eprintln!("l2_alsa_probe: {error:#}");
            std::process::exit(2);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Without the flag nothing is opened: the gate refuses and names the
    /// reservation wrapper. (Nothing here ever calls `run`.)
    #[test]
    fn the_probe_refuses_to_start_without_explicit_approval() {
        let args = Args::try_parse_from(["l2_alsa_probe", "--l2-log", "/nonexistent/x.jsonl"]).unwrap();
        let error = approval(&args).unwrap_err().to_string();
        assert!(error.contains("--i-have-approval") && error.contains("pw-reserve -n Audio1 -r"), "{error}");
        let args = Args::try_parse_from(["l2_alsa_probe", "--i-have-approval"]).unwrap();
        assert!(approval(&args).is_ok());
        assert_eq!(args.device, "hw:1,0");
    }

    #[test]
    fn the_release_plan_is_deterministic_with_bursts_and_gaps() {
        let plan = release_plan(900);
        assert_eq!(plan, release_plan(900));
        let released: Vec<u64> = plan.iter().flat_map(|(_, t)| t.iter().copied()).collect();
        assert_eq!(released.len(), 900 - 10 * 3, "three gap ticks per cycle");
        assert!(released.windows(2).all(|w| w[0] < w[1]), "release order = tick order");
        assert!(released.iter().all(|k| !GAP.contains(&(k % CYCLE))));
        let bursts: Vec<&(u64, Vec<u64>)> = plan.iter().filter(|(_, t)| t.len() > 1).collect();
        assert_eq!(bursts.len(), 10);
        for (slot, ticks) in bursts {
            assert_eq!(ticks.len(), 5);
            assert_eq!(*slot, *ticks.last().unwrap(), "released at the last held tick's slot");
        }
        assert!(plan.windows(2).all(|w| w[0].0 < w[1].0), "slots strictly increase");
        // Cut inside a burst: the held ticks are still released.
        let cut = release_plan(62);
        assert_eq!(cut.last().unwrap().1, vec![60, 61]);
    }

    #[test]
    fn the_tick_source_has_the_registered_split() {
        let table = sine_table();
        assert_eq!(table.len(), 16_000);
        let sizes: Vec<usize> = (0..90).map(|k| pcm_tick_payload(&table, k, 8_000, 90).unwrap().len() / 2).collect();
        assert_eq!(sizes.iter().filter(|&&n| n == 89).count(), 80);
        assert_eq!(sizes.iter().filter(|&&n| n == 88).count(), 10);
    }

    #[test]
    fn quantiles_are_linear_like_numpy() {
        let v = [1i64, 2, 3, 4];
        assert_eq!(quantile(&v, 0.5), Some(2.5));
        assert!((quantile(&v, 0.95).unwrap() - 3.85).abs() < 1e-9);
        assert_eq!(quantile(&v, 0.0), Some(1.0));
        assert_eq!(quantile(&v, 1.0), Some(4.0));
        assert_eq!(quantile(&[7], 0.99), Some(7.0));
        assert_eq!(quantile(&[], 0.5), None);
    }

    /// The report is computed from the file alone (synthetic rows).
    #[test]
    fn the_report_is_computed_from_the_l2_rows() {
        let rows = vec![
            serde_json::json!({"role": "meta", "l2_schema": "event-pair-l2-v1.2", "run_id": "p", "alsa": {"device": "hw:1,0"}}),
            serde_json::json!({"role": "submit", "t_submit_us": 100}),
            serde_json::json!({"role": "output", "t_submit_us": 100, "t_output_us": 10_100, "t_callback_us": 10_400, "t_log_arrival_us": 10_900,
                "output_detail": {"audio_tstamp_type_actual": "LINK", "driver_delay_frames": 0}}),
            serde_json::json!({"role": "explicit_discard", "reason": "output_queue_full"}),
            serde_json::json!({"role": "instrumentation_end", "integrity_ok": true, "max_write_lag_us": 30,
                "lanes": {"haptic": {"status_calls": 10, "status_gap_max_us": 1_100}}}),
        ];
        let report = summarize(&rows, &release_plan(3), 3);
        assert_eq!(report["report_lag_us"]["max"], 800);
        assert_eq!(report["callback_lag_us"]["p50"], 300.0);
        assert_eq!(report["output_minus_submit_us"]["min"], 10_000);
        assert_eq!(report["discard_reasons"]["output_queue_full"], 1);
        assert_eq!(report["audio_tstamp_type_actual"]["LINK"], 1);
        assert_eq!(report["status_cadence"]["status_calls"], 10);
        assert_eq!(report["integrity_ok"], true);
        assert_eq!(report["ticks_released"], 3);
    }
}
