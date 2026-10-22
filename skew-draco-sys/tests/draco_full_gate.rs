//! Opt-in freeze gate (registration 1-D §3-2): all 900 frames (d8/d7/d6 x
//! 300) decoded by the vendored Draco 1.5.7 must give, for every frame,
//!
//! 1. the same input bytes the reference digests were made from (`.drc` and
//!    `.bin` sha256 — the gate is about these exact files),
//! 2. DracoPy 2.0.0's decode bit for bit: point count, positions as float32
//!    bits in Draco point order, colours (sha256 of each, from
//!    `tests/fixtures/dracopy_digests.json`),
//! 3. the source `.bin` as a record multiset, byte-exact, after the
//!    round-half-even record conversion.
//!
//! `harness = false`: without `DRACO_GATE=1` it prints that it was skipped
//! and exits 0 WITHOUT counting as a passed test. With it, it prints one JSON
//! report and exits 1 on any mismatch. Reads only the local datasets
//! (`DRACO_GATE_DATASETS`, default `<repo>/datasets`).
//!
//!     DRACO_GATE=1 cargo test -p skew-draco-sys --test draco_full_gate

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use sha2::{Digest, Sha256};
use skew_draco_sys::{decode_point_cloud, decoder_info, records_from_cloud, sorted_records};

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn sha256(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

/// numpy `quantile(method='linear')` of sorted values.
fn quantile(sorted: &[f64], q: f64) -> f64 {
    let position = q * (sorted.len() - 1) as f64;
    let (lo, hi) = (position.floor() as usize, position.ceil() as usize);
    let t = position - lo as f64;
    let (a, b) = (sorted[lo], sorted[hi]);
    if t >= 0.5 {
        b - (b - a) * (1.0 - t)
    } else {
        a + (b - a) * t
    }
}

#[derive(Default)]
struct TierStats {
    frames: u64,
    pass: u64,
    points: u64,
    max_abs_rounding: f32,
    decode_ms: Vec<f64>,
    records_ms: Vec<f64>,
    multiset_digests: Vec<String>,
}

fn main() {
    if std::env::var("DRACO_GATE").as_deref() != Ok("1") {
        println!("draco_full_gate: SKIPPED (opt-in: DRACO_GATE=1 cargo test -p skew-draco-sys --test draco_full_gate)");
        return;
    }
    let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let datasets = std::env::var_os("DRACO_GATE_DATASETS")
        .map(PathBuf::from)
        .unwrap_or_else(|| crate_dir.join("../../../datasets"));
    let reference: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(crate_dir.join("tests/fixtures/dracopy_digests.json")).expect("reference digests"),
    )
    .expect("reference JSON");
    let frames = reference["frames"].as_array().expect("frames");
    let mut tiers: BTreeMap<String, TierStats> = BTreeMap::new();
    let mut failures: Vec<String> = Vec::new();
    for f in frames {
        let tier = f["tier"].as_str().expect("tier").to_string();
        let i = f["frame"].as_u64().expect("frame");
        let drc_path = datasets.join(format!("tiers_draco_s3/loot/{tier}/loot_{tier}_{i:04}.drc"));
        let bin_path = datasets.join(format!("tiers_s3/loot/{tier}/loot_{tier}_{i:04}.bin"));
        let stats = tiers.entry(tier.clone()).or_default();
        stats.frames += 1;
        let (drc, bin) = match (std::fs::read(&drc_path), std::fs::read(&bin_path)) {
            (Ok(d), Ok(b)) => (d, b),
            (d, b) => {
                failures.push(format!("{tier} {i}: read failed ({:?}, {:?})", d.err(), b.err()));
                continue;
            }
        };
        // Every problem of this frame; the frame passes iff there is none.
        let mut problems: Vec<String> = Vec::new();
        if sha256(&drc) != f["drc_sha256"].as_str().unwrap() {
            problems.push("drc bytes differ from the reference input".into());
        }
        if sha256(&bin) != f["bin_sha256"].as_str().unwrap() {
            problems.push("bin bytes differ from the reference input".into());
        }
        let t0 = Instant::now();
        let decoded = decode_point_cloud(&drc);
        let t1 = Instant::now();
        let cloud = match decoded {
            Ok(cloud) => cloud,
            Err(error) => {
                failures.push(format!("{tier} {i}: decode failed: {error}"));
                continue;
            }
        };
        let converted = records_from_cloud(&cloud);
        let t2 = Instant::now();
        let records = match converted {
            Ok(records) => records,
            Err(error) => {
                failures.push(format!("{tier} {i}: records failed: {error}"));
                continue;
            }
        };
        stats.decode_ms.push((t1 - t0).as_secs_f64() * 1e3);
        stats.records_ms.push((t2 - t1).as_secs_f64() * 1e3);
        if cloud.num_points() as u64 != f["points"].as_u64().unwrap() {
            problems.push(format!("points {} != DracoPy {}", cloud.num_points(), f["points"]));
        }
        let pos_bytes: Vec<u8> = cloud.positions.iter().flat_map(|v| v.to_le_bytes()).collect();
        if sha256(&pos_bytes) != f["pos_f32_sha256"].as_str().unwrap() {
            problems.push("float32 position bits differ from DracoPy's".into());
        }
        if sha256(&cloud.colors) != f["rgb_u8_sha256"].as_str().unwrap() {
            problems.push("colours differ from DracoPy's".into());
        }
        let got = sorted_records(&records.bytes);
        let want = if bin.len() % 8 == 0 { sorted_records(&bin) } else { Vec::new() };
        if got != want {
            problems.push(format!("record multiset differs from the .bin ({} vs {} records)", got.len(), bin.len() / 8));
        }
        let multiset: Vec<u8> = got.iter().flat_map(|k| k.to_le_bytes()).collect();
        stats.multiset_digests.push(sha256(&multiset));
        stats.max_abs_rounding = stats.max_abs_rounding.max(records.max_abs_rounding);
        stats.points += cloud.num_points() as u64;
        if problems.is_empty() {
            stats.pass += 1;
        }
        failures.extend(problems.into_iter().map(|p| format!("{tier} {i}: {p}")));
    }
    let mut per_tier = serde_json::Map::new();
    let mut total = (0u64, 0u64);
    for (tier, s) in &mut tiers {
        total.0 += s.frames;
        total.1 += s.pass;
        let mut d = s.decode_ms.clone();
        d.sort_by(f64::total_cmp);
        let mut r = s.records_ms.clone();
        r.sort_by(f64::total_cmp);
        let timing = |v: &[f64]| {
            if v.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::json!({ "median": quantile(v, 0.5), "p95": quantile(v, 0.95), "max": v[v.len() - 1] })
            }
        };
        per_tier.insert(
            tier.clone(),
            serde_json::json!({
                "frames": s.frames,
                "byte_exact": s.pass,
                "points": s.points,
                "max_abs_rounding": s.max_abs_rounding,
                "multiset_digest": sha256(s.multiset_digests.join("\n").as_bytes()),
                "decode_ms_informational": timing(&d),
                "records_ms_informational": timing(&r),
            }),
        );
    }
    let info = decoder_info();
    let report = serde_json::json!({
        "gate": "draco_full_gate",
        "pass": failures.is_empty() && total.0 == 900 && total.1 == 900,
        "frames": total.0,
        "byte_exact_frames": total.1,
        "checks": ["input sha256 = reference", "DracoPy 2.0.0 point count, float32 position bits and colours (point order)", "record multiset = source .bin (byte-exact, sorted u64 LE)"],
        "datasets": datasets.display().to_string(),
        "reference": reference["decoder"],
        "tiers": per_tier,
        "decoder": {
            "id": info.id,
            "version": info.version,
            "source_tarball_sha256": info.source_tarball_sha256,
            "vendored_sources_sha256": info.vendored_sources_sha256,
            "compiler": info.compiler,
            "compiler_version": info.compiler_version,
            "fixed_flags": info.fixed_flags,
            "compiler_args": info.compiler_args,
            "rust_opt_level": info.rust_opt_level,
            "threads": info.threads,
        },
        "failures": failures.iter().take(50).collect::<Vec<_>>(),
        "failure_count": failures.len(),
        "timing_note": "decode times are informational (this host, idle, gate binary); not a campaign measurement",
    });
    println!("{}", serde_json::to_string_pretty(&report).expect("JSON"));
    if !(failures.is_empty() && total.0 == 900 && total.1 == 900) {
        std::process::exit(1);
    }
}
