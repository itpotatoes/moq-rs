//! `l2_wayland_probe` — L2 PC Wayland output probe (experiment 1-D,
//! milestone M3; registration §3-2, §3-3b, §3-6).
//!
//! Shows a deterministic sequence of point-cloud frames through the SAME PC
//! sink the receiver uses (`skew_moq::l2_output`: PC lane `wayland` on the
//! display backend, haptic lane `null`), once per representation given —
//! `.bin` frames as sent and/or Draco frames decoded by the vendored Draco
//! 1.5.7 — writes one schema v1.4 L2 JSONL per representation and prints ONE
//! JSON report: presentation flags, refresh, MSC steps (missed refreshes),
//! the latency distributions `t_output - t_release`, `t_output - t_commit`,
//! `t_callback - t_output`, `report_lag = t_log_arrival - t_output` (§3-6
//! needs it for `G_report`), decode and render times. It reports no
//! experimental effect of any method.
//!
//! IT OPENS A FULLSCREEN WINDOW ON THE USER'S WAYLAND DISPLAY
//! (`WAYLAND_DISPLAY`). Running it needs the user's explicit approval and the
//! GUI rules of the registration (fullscreen, no other window, screen lock
//! and blanking off); it refuses to start without `--i-have-approval`.
//!
//! ```sh
//! target/debug/l2_wayland_probe --i-have-approval \
//!     --tiers-meta datasets/tiers_s3/tiers_meta.json \
//!     --bin-frames-dir datasets/tiers_s3/loot/d8 --bin-l2-log /fresh/probe-bin-l2.jsonl \
//!     --draco-frames-dir datasets/tiers_draco_s3/loot/d8 --draco-l2-log /fresh/probe-draco-l2.jsonl \
//!     --diagnostics-prefix /fresh/probe-wayland-diag
//! ```
//!
//! Release plan (deterministic, `--frames` at 30 fps from the start of each
//! phase): frame `k` is released at its slot `t0 + floor(k * 1e6 / 30)` µs
//! with the payload of file `k mod files` (sorted names) and header tier
//! `--tier`, except in every 30-frame cycle: frames 20..=22 are held and
//! released TOGETHER at the slot of frame 22 (a burst: the pending slot must
//! supersede), and frames 26..=27 are never released. Exit code: 0 when
//! every phase's sentinel says `integrity_ok`, 1 otherwise, 2 for a usage
//! error or a missing approval.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::Parser;

use skew_moq::l2_output::{
    L2Config, L2Identity, L2Modality, L2Output, L2SinkKind, L2Sinks, WaylandBackendKind, WaylandSinkConfig,
    WAYLAND_DEFAULT_BUFFERS, WAYLAND_READY_TIMEOUT_US, WAYLAND_RELEASE_TIMEOUT_US,
};
use skew_moq::l2_pc::{PcRepresentation, TierScales};
use skew_moq::{load_frames_checked, now_us, timestamp_us, Representation};

const PC_RATE_HZ: u64 = 30;
const CYCLE: u64 = 30;
const BURST: std::ops::RangeInclusive<u64> = 20..=22;
const GAP: std::ops::RangeInclusive<u64> = 26..=27;

#[derive(Parser, Debug)]
#[command(about = "L2 PC Wayland output probe (opens a fullscreen window on the user's display; needs --i-have-approval)")]
struct Args {
    /// Required: the user approved opening a fullscreen window on the
    /// Wayland display for this probe (GUI rules: no other window, screen
    /// lock and blanking off).
    #[arg(long)]
    i_have_approval: bool,
    /// Tiers meta (schema phase4_s3_tiers_v1) with the depth scales.
    #[arg(long)]
    tiers_meta: Option<PathBuf>,
    /// `.bin` frames of one tier (e.g. datasets/tiers_s3/loot/d8).
    #[arg(long)]
    bin_frames_dir: Option<PathBuf>,
    /// The L2 JSONL of the bin phase (must not exist).
    #[arg(long)]
    bin_l2_log: Option<PathBuf>,
    /// Draco frames of the same tier (e.g. datasets/tiers_draco_s3/loot/d8).
    #[arg(long)]
    draco_frames_dir: Option<PathBuf>,
    /// The L2 JSONL of the draco phase (must not exist).
    #[arg(long)]
    draco_l2_log: Option<PathBuf>,
    /// Optional probe-only diagnostics: `<prefix>-<representation>.jsonl`
    /// (must not exist) with the raw presentation values (ns) the L2 JSONL
    /// does not carry. Never an analysis time axis.
    #[arg(long)]
    diagnostics_prefix: Option<PathBuf>,
    #[arg(long, default_value = "l2-wayland-probe")]
    run_id: String,
    /// Header tier of every frame (2 = d8 in the S3 tiers meta).
    #[arg(long, default_value_t = 2)]
    tier: u16,
    /// Frames per phase (300 = 10 s at 30 fps).
    #[arg(long, default_value_t = 300)]
    frames: u64,
    #[arg(long, default_value_t = WAYLAND_DEFAULT_BUFFERS)]
    buffers: usize,
    /// Close grace of the output stage (anchored at its close).
    #[arg(long, default_value_t = 1_000)]
    report_window_ms: u64,
}

/// The approval gate: nothing is opened unless the flag is given.
fn approval(args: &Args) -> Result<()> {
    if !args.i_have_approval {
        bail!(
            "refusing to start: l2_wayland_probe opens a FULLSCREEN window on the user's Wayland display \
             ({}). Run it only with the user's explicit approval, with no other window in use and screen \
             lock/blanking off, and pass --i-have-approval.",
            std::env::var("WAYLAND_DISPLAY").unwrap_or_else(|_| "WAYLAND_DISPLAY unset".into())
        );
    }
    Ok(())
}

/// `(release slot k, frames released at that slot)` in release order.
fn release_plan(frames: u64) -> Vec<(u64, Vec<u64>)> {
    let mut plan = Vec::new();
    let mut held = Vec::new();
    for k in 0..frames {
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
    if let Some(&last) = held.last() {
        plan.push((last, held));
    }
    plan
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

/// One phase's report, from the written L2 file alone.
fn summarize(rows: &[serde_json::Value], plan: &[(u64, Vec<u64>)]) -> serde_json::Value {
    let meta = rows.first().cloned().unwrap_or(serde_json::Value::Null);
    let end = rows.last().filter(|r| r["role"] == "instrumentation_end").cloned().unwrap_or(serde_json::Value::Null);
    let outputs: Vec<&serde_json::Value> = rows.iter().filter(|r| r["role"] == "output").collect();
    let num = |r: &serde_json::Value, k: &str| r[k].as_i64().unwrap_or(0);
    let detail = |r: &serde_json::Value, k: &str| r["output_detail"][k].as_i64().unwrap_or(0);
    let mut discard_reasons: BTreeMap<String, u64> = BTreeMap::new();
    for d in rows.iter().filter(|r| r["role"] == "explicit_discard") {
        *discard_reasons.entry(d["reason"].as_str().unwrap_or("?").to_string()).or_default() += 1;
    }
    let mut flags: BTreeMap<String, u64> = BTreeMap::new();
    let mut refresh: BTreeMap<i64, u64> = BTreeMap::new();
    for o in &outputs {
        let names = o["output_detail"]["flag_names"].as_array().map(|a| {
            a.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>().join("|")
        });
        *flags.entry(names.unwrap_or_default()).or_default() += 1;
        *refresh.entry(detail(o, "refresh_us")).or_default() += 1;
    }
    // MSC steps between consecutive outputs (in commit order): at 30 fps on
    // a 60 Hz output an undisturbed step is 2; larger steps are missed
    // refreshes or held frames (the plan has bursts and gaps).
    let mut by_commit: Vec<(i64, i64)> = outputs.iter().map(|o| (detail(o, "commit_id"), detail(o, "msc"))).collect();
    by_commit.sort_unstable();
    let mut msc_steps: BTreeMap<i64, u64> = BTreeMap::new();
    for w in by_commit.windows(2) {
        *msc_steps.entry(w[1].1 - w[0].1).or_default() += 1;
    }
    let decode: Vec<i64> = rows
        .iter()
        .filter(|r| r.get("t_decode_end_us").is_some())
        .map(|r| num(r, "t_decode_end_us") - num(r, "t_decode_start_us"))
        .collect();
    serde_json::json!({
        "l2_schema": meta["l2_schema"],
        "run_id": meta["run_id"],
        "representation": meta["wayland"]["representation"],
        "startup": meta["wayland"]["startup"],
        "canvas_side_px": meta["wayland"]["canvas_side_px"],
        "decoder": meta["wayland"]["decoder"],
        "frames_released": plan.iter().map(|(_, f)| f.len()).sum::<usize>(),
        "release_slots": plan.len(),
        "sentinel_present": !end.is_null(),
        "integrity_ok": end["integrity_ok"],
        "first_integrity_failure_reason": end["first_integrity_failure_reason"],
        "first_integrity_failure_us": end["first_integrity_failure_us"],
        "submitted": end["submitted"],
        "output": end["output"],
        "discarded_after_submit": end["discarded_after_submit"],
        "discarded_before_submit": end["discarded_before_submit"],
        "submitted_unresolved": end["submitted_unresolved"],
        "discard_reasons": discard_reasons,
        "presented_flags": flags,
        "refresh_us": refresh,
        "msc_steps": msc_steps,
        "release_to_output_us": distribution(outputs.iter().map(|o| num(o, "t_output_us") - num(o, "t_release_us")).collect()),
        "commit_to_output_us": distribution(outputs.iter().map(|o| num(o, "t_output_us") - detail(o, "t_commit_us")).collect()),
        "output_to_callback_us": distribution(outputs.iter().map(|o| num(o, "t_callback_us") - num(o, "t_output_us")).collect()),
        "report_lag_us": distribution(outputs.iter().map(|o| num(o, "t_log_arrival_us") - num(o, "t_output_us")).collect()),
        "render_us": distribution(outputs.iter().map(|o| detail(o, "t_render_end_us") - detail(o, "t_render_start_us")).collect()),
        "decode_us": distribution(decode),
        "max_write_lag_us": end["max_write_lag_us"],
        "max_callback_to_log_us": end["max_callback_to_log_us"],
        "lane": end["lanes"]["pc"],
    })
}

/// One phase: open the sink (connect, fullscreen, startup frame), release
/// the plan, close, summarize.
fn run_phase(
    args: &Args,
    tiers: &TierScales,
    representation: PcRepresentation,
    frames_dir: &Path,
    l2_log: &Path,
) -> Result<(bool, serde_json::Value)> {
    let declared = match representation {
        PcRepresentation::Bin => Representation::Bin,
        PcRepresentation::Draco => Representation::Draco,
    };
    let frames_dir_s = frames_dir.to_str().context("frames dir must be UTF-8")?;
    let frames: Vec<bytes::Bytes> =
        load_frames_checked(frames_dir_s, declared)?.into_iter().map(bytes::Bytes::from).collect();
    let diagnostics = args
        .diagnostics_prefix
        .as_ref()
        .map(|prefix| PathBuf::from(format!("{}-{}.jsonl", prefix.display(), representation.as_str())));
    let sinks = L2Sinks { pc: L2SinkKind::Wayland, haptic: L2SinkKind::Null };
    let config = L2Config {
        run_id: format!("{}-{}", args.run_id, representation.as_str()),
        method: "probe".into(),
        sinks,
        fake: None,
        alsa: None,
        wayland: Some(WaylandSinkConfig {
            backend: WaylandBackendKind::Display,
            representation,
            tiers: tiers.clone(),
            buffers: args.buffers,
            ready_timeout_us: WAYLAND_READY_TIMEOUT_US,
            release_timeout_us: WAYLAND_RELEASE_TIMEOUT_US,
            diagnostics,
        }),
        queue_capacity: 256,
        row_capacity: 65_536,
        report_window_us: args.report_window_ms.saturating_mul(1_000),
        max_tracked_objects: 65_536,
    };
    config.validate()?;
    let plan = release_plan(args.frames);
    // Connects, goes fullscreen and presents the startup frame.
    let output = L2Output::start(l2_log, config)?;
    let submitter = output.submitter();
    let t0 = now_us();
    for (slot, released) in &plan {
        let due = t0 + timestamp_us(*slot, PC_RATE_HZ);
        let now = now_us();
        if due > now {
            std::thread::sleep(Duration::from_micros(due - now));
        }
        for &k in released {
            let identity = L2Identity {
                modality: L2Modality::Pc,
                tier: args.tier,
                seq: k as u32,
                pts_us: timestamp_us(k, PC_RATE_HZ),
                event_id: (k + 1) as u32,
                route: None,
            };
            let payload = frames[(k % frames.len() as u64) as usize].clone();
            submitter.submit_payload(identity, now_us(), Some(payload));
        }
    }
    drop(submitter);
    let summary = output.finish()?;
    let rows = read_rows(l2_log)?;
    Ok((summary.integrity_ok(sinks), summarize(&rows, &plan)))
}

fn run(args: &Args) -> Result<bool> {
    let tiers_path = args.tiers_meta.as_ref().context("--tiers-meta is required")?;
    let tiers = TierScales::load(tiers_path).map_err(anyhow::Error::msg)?;
    if tiers.depth_scale(args.tier).is_none() {
        bail!("tier {} is not in {}", args.tier, tiers_path.display());
    }
    let mut phases = Vec::new();
    match (&args.bin_frames_dir, &args.bin_l2_log) {
        (Some(dir), Some(log)) => phases.push((PcRepresentation::Bin, dir.clone(), log.clone())),
        (None, None) => {}
        _ => bail!("--bin-frames-dir and --bin-l2-log go together"),
    }
    match (&args.draco_frames_dir, &args.draco_l2_log) {
        (Some(dir), Some(log)) => phases.push((PcRepresentation::Draco, dir.clone(), log.clone())),
        (None, None) => {}
        _ => bail!("--draco-frames-dir and --draco-l2-log go together"),
    }
    if phases.is_empty() {
        bail!("give a bin phase, a draco phase, or both");
    }
    for (_, _, log) in &phases {
        if std::fs::symlink_metadata(log).is_ok() {
            bail!("{} already exists; an L2 log is never overwritten", log.display());
        }
    }
    let mut all_ok = true;
    let mut reports = serde_json::Map::new();
    for (representation, dir, log) in &phases {
        let (ok, report) = run_phase(args, &tiers, *representation, dir, log)?;
        all_ok &= ok;
        reports.insert(representation.as_str().into(), report);
    }
    let report = serde_json::json!({
        "probe": "l2_wayland_probe",
        "frames_per_phase": args.frames,
        "tier": args.tier,
        "tiers_meta": tiers.to_json(),
        "phases": reports,
    });
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(all_ok)
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
            eprintln!("l2_wayland_probe: {error:#}");
            std::process::exit(2);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Without the flag nothing is opened: the gate refuses and names the
    /// GUI rules. (Nothing here ever calls `run`.)
    #[test]
    fn the_probe_refuses_to_start_without_explicit_approval() {
        let args = Args::try_parse_from(["l2_wayland_probe", "--bin-l2-log", "/nonexistent/x.jsonl"]).unwrap();
        let error = approval(&args).unwrap_err().to_string();
        assert!(error.contains("--i-have-approval") && error.contains("FULLSCREEN"), "{error}");
        let args = Args::try_parse_from(["l2_wayland_probe", "--i-have-approval"]).unwrap();
        assert!(approval(&args).is_ok());
        assert_eq!((args.tier, args.frames, args.buffers), (2, 300, 4));
    }

    #[test]
    fn the_release_plan_is_deterministic_with_bursts_and_gaps() {
        let plan = release_plan(300);
        assert_eq!(plan, release_plan(300));
        let released: Vec<u64> = plan.iter().flat_map(|(_, f)| f.iter().copied()).collect();
        assert_eq!(released.len(), 300 - 10 * 2, "two gap frames per cycle");
        assert!(released.windows(2).all(|w| w[0] < w[1]), "release order = frame order");
        let bursts: Vec<&(u64, Vec<u64>)> = plan.iter().filter(|(_, f)| f.len() > 1).collect();
        assert_eq!(bursts.len(), 10);
        assert!(bursts.iter().all(|(slot, f)| f.len() == 3 && *slot == *f.last().unwrap()));
        assert!(plan.windows(2).all(|w| w[0].0 < w[1].0), "slots strictly increase");
        assert_eq!(release_plan(21).last().unwrap().1, vec![20], "a plan cut inside a burst still releases");
    }

    #[test]
    fn quantiles_are_linear_like_numpy() {
        let v = [1i64, 2, 3, 4];
        assert_eq!(quantile(&v, 0.5), Some(2.5));
        assert!((quantile(&v, 0.95).unwrap() - 3.85).abs() < 1e-9);
        assert_eq!(quantile(&[], 0.5), None);
    }

    /// The report is computed from the file alone (synthetic rows).
    #[test]
    fn the_report_is_computed_from_the_l2_rows() {
        let rows = vec![
            serde_json::json!({"role": "meta", "l2_schema": "event-pair-l2-v1.4", "run_id": "p",
                "wayland": {"representation": "draco", "canvas_side_px": 1080}}),
            serde_json::json!({"role": "output", "t_release_us": 100, "t_output_us": 20_100, "t_callback_us": 20_400,
                "t_log_arrival_us": 20_900, "t_decode_start_us": 150, "t_decode_end_us": 6_150,
                "output_detail": {"commit_id": 1, "msc": 10, "t_commit_us": 9_000, "t_render_start_us": 6_200,
                    "t_render_end_us": 8_200, "refresh_us": 16_666, "flag_names": ["vsync", "hw_clock"]}}),
            serde_json::json!({"role": "output", "t_release_us": 33_433, "t_output_us": 53_433, "t_callback_us": 53_533,
                "t_log_arrival_us": 53_633, "t_decode_start_us": 33_500, "t_decode_end_us": 39_000,
                "output_detail": {"commit_id": 2, "msc": 12, "t_commit_us": 41_000, "t_render_start_us": 39_100,
                    "t_render_end_us": 40_100, "refresh_us": 16_666, "flag_names": ["vsync", "hw_clock"]}}),
            serde_json::json!({"role": "explicit_discard", "reason": "superseded_before_commit", "t_decode_start_us": 1, "t_decode_end_us": 5_001}),
            serde_json::json!({"role": "instrumentation_end", "integrity_ok": true, "output": 2, "lanes": {"pc": {"kind": "wayland"}}}),
        ];
        let report = summarize(&rows, &release_plan(3));
        assert_eq!(report["representation"], "draco");
        assert_eq!(report["report_lag_us"]["max"], 800);
        assert_eq!(report["commit_to_output_us"]["min"], 11_100);
        assert_eq!(report["output_to_callback_us"]["p50"], 200.0);
        assert_eq!(report["msc_steps"]["2"], 1);
        assert_eq!(report["presented_flags"]["vsync|hw_clock"], 2);
        assert_eq!(report["refresh_us"]["16666"], 2);
        assert_eq!(report["decode_us"]["n"], 3, "decode times of every decoded terminal");
        assert_eq!(report["discard_reasons"]["superseded_before_commit"], 1);
        assert_eq!(report["lane"]["kind"], "wayland");
    }
}
