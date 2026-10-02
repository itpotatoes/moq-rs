//! Experiment-2 WP3 recorder: the receiver-side producer of the L1-R output
//! ledger (contract `md/20260929_실험2_계약.md`, exp2-contract-v3).
//!
//! The recorder owns the run's exp2 JSONL file and, for the core methods, the
//! WP1 core (`Exp2Playout` for S3NPA'/P0-NP/P0, `Exp2P1` for P1).  B1 and S1
//! keep their existing release behaviour; the recorder only *logs* their
//! terminals and seals the ledger at H (contract §5 coordinator decision).
//! It is transport-free: the binary feeds it received objects, wall-clock
//! ticks and (S1) scheduler actions; tests drive it with an injected clock and
//! an in-memory sink.
//!
//! Serialisation rules (implementation plan WP3, Codex 84):
//! * core `Released` / `Dropped` become JSONL roles `release` / `drop` (the
//!   role is never `SchedulerTerminal::as_str()`);
//! * `PendingAtHorizon` / `NoObject` are produced by walking the sealed core
//!   ledger at H and are written with `t_us == H`;
//! * a `Dropped(delivery_timeout)` exists only in the sealed ledger, so its
//!   `drop` row has `t_us == H`; the observation instant was written earlier
//!   as a `wire` row;
//! * a later copy of an already-admitted slot (any route generation) is a
//!   `drop` with reason `route_copy_discard` (not a terminal);
//! * `instrumentation_end` is the last row (`end_us == t_us`); after it the
//!   recorder refuses every row.
//!
//! Time-feeding rule (the WP1 core is strictly time-monotone): the time fed to
//! the core for an arrival is `max(t_recv, last core input, drained + 1)`;
//! the core is never advanced past H before `finalize(H)`, and inputs whose
//! fed time would exceed H are not fed (post-horizon, counted).

use std::collections::HashMap;
use std::io::Write;

use anyhow::{anyhow, bail, ensure, Context, Result};
use serde_json::{json, Map, Value};

use crate::exp2_fsm::{Exp2FsmParams, Exp2P1, TierRequest};
use crate::exp2_playout::{
    CommittedDue, Exp2Decision, Exp2Mode, Exp2OpportunityLedger, Exp2Params, Exp2Playout,
    Exp2SchedulerLedger, Modality, SchedulerTerminal,
};
use crate::s3_controller::HapticMode;
use crate::{is_warmup_seq, Header, TRACK_HAPTIC, TRACK_PC};

pub const EXP2_CONTRACT_VERSION: &str = "exp2-contract-v4";
pub const EXP2_METRIC_SCHEMA_VERSION: &str = "exp2-v2";
/// Registration §1: run length, B_play, ε_ref, G_report,R (integer µs).
pub const RUN_DURATION_US: u64 = 40_000_000;
pub const B_PLAY_US: u64 = 400_000;
pub const EPS_REF_US: u64 = 5_000;
pub const G_REPORT_R_US: u64 = 1_000_000;
pub const N_PC: usize = 1_200;
pub const N_HAPTIC: usize = 3_600;
pub const PC_RATE_HZ: u64 = 30;
pub const HAPTIC_RATE_HZ: u64 = 90;
pub const ROUTE_COPY_DISCARD: &str = "route_copy_discard";
/// Contract v4 §9 / registration §2-2: an object that arrived but was
/// discarded by the P1 switch gate on every route (never admitted) is a
/// terminal drop with this reason (P1's switching cost), not `no_object`.
pub const SWITCH_BARRIER: &str = "switch_barrier";
/// Contract v4 §9 `wire` counter kind (sender/relay delivery timeouts).
pub const WIRE_COUNTER_KIND: &str = "delivery_timeout_counter";
/// S1 slot that was admitted but neither released nor dropped by H and whose
/// due is not after H (or unknown): none of the contract §4 states applies, so
/// it is an explicit discard with this reason (WP3 interpretation, reported).
pub const S1_UNRELEASED_AT_HORIZON: &str = "unreleased_at_horizon";
/// B1/S1 (no core) seal execution delay after H.  Not a scientific parameter:
/// every sealing row still carries `t_us == H` and the ledger is the state as
/// of H; the delay only lets objects stamped `t_recv <= H` that are still
/// between their receive stamp and the recorder land before the seal.
pub const PLAIN_SEAL_EXECUTION_DELAY_US: u64 = 100_000;

/// `H = t0 + 40 s + B_play` (registration §1).
pub fn horizon_us(t0_us: u64) -> u64 {
    t0_us + RUN_DURATION_US + B_PLAY_US
}

/// Contract §1: PC opportunity `i` has `pts = floor(i·10⁶/30)`, `event_id = i+1`.
pub fn pc_slot_identity(i: u64) -> (u64, u32) {
    (crate::timestamp_us(i, PC_RATE_HZ), (i + 1) as u32)
}

/// Contract §1: haptic slot `k` has `pts = floor(k·10⁶/90)`; the anchor
/// `k = 3i` carries `event_id = i+1`, fillers `0`.
pub fn haptic_slot_identity(k: u64) -> (u64, u32) {
    let event_id = if k % 3 == 0 { (k / 3 + 1) as u32 } else { 0 };
    (crate::timestamp_us(k, HAPTIC_RATE_HZ), event_id)
}

/// The six registered methods (registration §2) and their two spellings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exp2Method {
    B1,
    S1,
    S3npaPrime,
    P0np,
    P0,
    P1,
}

impl Exp2Method {
    /// Analyzer / schedule spelling (`analysis/exp2_analysis.py` METHODS).
    pub fn label(self) -> &'static str {
        match self {
            Self::B1 => "B1",
            Self::S1 => "S1",
            Self::S3npaPrime => "S3NPA'",
            Self::P0np => "P0-NP",
            Self::P0 => "P0",
            Self::P1 => "P1",
        }
    }

    /// Core mode, `None` for B1/S1 (no WP1 core).  P1 runs the P0 core.
    pub fn core_mode(self) -> Option<Exp2Mode> {
        match self {
            Self::B1 | Self::S1 => None,
            Self::S3npaPrime => Some(Exp2Mode::S3npaPrime),
            Self::P0np => Some(Exp2Mode::P0Np),
            Self::P0 | Self::P1 => Some(Exp2Mode::P0),
        }
    }
}

fn modality_of(track_id: u8) -> Option<Modality> {
    match track_id {
        TRACK_PC => Some(Modality::Pc),
        TRACK_HAPTIC => Some(Modality::Haptic),
        _ => None,
    }
}

fn haptic_mode_str(mode: HapticMode) -> &'static str {
    match mode {
        HapticMode::Full => "full",
        HapticMode::Essential => "essential",
    }
}

// --------------------------------------------------------------------------
// Opportunity ledger file (contract §3) and meta (contract §6)
// --------------------------------------------------------------------------

/// Rebuild every object with its keys in sorted order (independent of
/// whether serde_json's `preserve_order` feature is on in this build).
fn canonicalize(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let mut out = Map::new();
            for key in keys {
                out.insert(key.clone(), canonicalize(&map[key]));
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(canonicalize).collect()),
        other => other.clone(),
    }
}

/// Contract §3 canonical bytes: sorted keys, `(",", ":")` separators, UTF-8.
pub fn canonical_json_bytes(value: &Value) -> Vec<u8> {
    serde_json::to_vec(&canonicalize(value)).expect("a JSON value serialises")
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(bytes))
}

fn get_u64(map: &Map<String, Value>, key: &str, at: &str) -> Result<u64> {
    map.get(key)
        .and_then(Value::as_u64)
        .with_context(|| format!("{at}.{key} must be a non-negative integer"))
}

/// Parse and validate the sealed opportunity ledger file (contract §3) for
/// this run.  Fails closed unless the file bytes are exactly the canonical
/// form (so the file SHA-256 equals the canonical digest the analyzer
/// recomputes) and every slot matches the registered integer PTS rule.
/// Returns the core ledger and the file SHA-256 (hex).
pub fn load_opportunities(
    bytes: &[u8],
    run_id: &str,
    t0_us: u64,
) -> Result<(Exp2OpportunityLedger, String)> {
    let value: Value = serde_json::from_slice(bytes).context("parse opportunities JSON")?;
    ensure!(
        canonical_json_bytes(&value) == bytes,
        "opportunities file is not in canonical form (sorted keys, compact, no trailing newline)"
    );
    let map = value.as_object().context("opportunities must be a JSON object")?;
    ensure!(
        map.get("contract").and_then(Value::as_str) == Some(EXP2_CONTRACT_VERSION),
        "opportunities contract is not {EXP2_CONTRACT_VERSION}"
    );
    ensure!(
        map.get("run_id").and_then(Value::as_str) == Some(run_id),
        "opportunities run_id differs from --run-id"
    );
    ensure!(
        get_u64(map, "t0_us", "opportunities")? == t0_us,
        "opportunities t0_us differs from the phase-control t0"
    );
    let pc = map.get("pc").and_then(Value::as_array).context("opportunities.pc")?;
    let haptic = map
        .get("haptic")
        .and_then(Value::as_array)
        .context("opportunities.haptic")?;
    ensure!(
        pc.len() == N_PC && haptic.len() == N_HAPTIC,
        "opportunities must hold {N_PC} PC and {N_HAPTIC} haptic slots, got {}/{}",
        pc.len(),
        haptic.len()
    );
    let mut pc_pts = Vec::with_capacity(N_PC);
    for (i, slot) in pc.iter().enumerate() {
        let slot = slot.as_object().context("opportunities.pc slot")?;
        let at = format!("opportunities.pc[{i}]");
        let (pts, event_id) = pc_slot_identity(i as u64);
        ensure!(
            get_u64(slot, "i", &at)? == i as u64
                && get_u64(slot, "pts_us", &at)? == pts
                && get_u64(slot, "event_id", &at)? == event_id as u64
                && get_u64(slot, "ref_us", &at)? == t0_us + pts,
            "{at} does not match the registered slot rule"
        );
        pc_pts.push(pts);
    }
    let mut haptic_pts = Vec::with_capacity(N_HAPTIC);
    for (k, slot) in haptic.iter().enumerate() {
        let slot = slot.as_object().context("opportunities.haptic slot")?;
        let at = format!("opportunities.haptic[{k}]");
        let (pts, event_id) = haptic_slot_identity(k as u64);
        let anchor = slot.get("anchor_i").context("anchor_i missing")?;
        let anchor_ok = if k % 3 == 0 {
            anchor.as_u64() == Some((k / 3) as u64)
        } else {
            anchor.is_null()
        };
        ensure!(
            get_u64(slot, "k", &at)? == k as u64
                && get_u64(slot, "pts_us", &at)? == pts
                && get_u64(slot, "event_id", &at)? == event_id as u64
                && anchor_ok,
            "{at} does not match the registered slot rule"
        );
        haptic_pts.push(pts);
    }
    let ledger = Exp2OpportunityLedger {
        t0_us,
        pc_pts_us: pc_pts,
        haptic_pts_us: haptic_pts,
    };
    ledger.validate().map_err(|e| anyhow!(e))?;
    Ok((ledger, sha256_hex(bytes)))
}

/// Runner-owned meta keys (contract §6) the receiver cannot know itself.
pub const RUNNER_META_KEYS: &[&str] = &[
    "batch_id",
    "campaign",
    "condition",
    "block_id",
    "trajectory_seed",
    "attempt",
    "planned_predecessor",
    "actual_predecessor",
    "epsilon_output_us",
    "g_report_us",
    "threshold_profile",
    "thresh_pos_ms",
    "thresh_neg_ms",
    "threshold_citation_id",
    "threshold_verified",
];

/// Receiver-owned meta keys; a runner meta document must not carry them.
pub const RECEIVER_META_KEYS: &[&str] = &[
    "role",
    "contract",
    "metric_schema_version",
    "run_id",
    "method",
    "topology",
    "representation",
    "t0_us",
    "opportunities_sha256",
    "delta_max_us",
    "t_pc_ms",
    "C_mbps",
    "S_bytes",
    "t_us",
    "scientific_eligible",
    "exp2_test_mode",
    "exp2_test_force_hc_at_ms",
    "p1_switch_settings",
];

pub struct ReceiverMeta<'a> {
    pub run_id: &'a str,
    pub method: Exp2Method,
    pub topology: &'a str,
    pub representation: &'a str,
    pub t0_us: u64,
    pub opportunities_sha256: &'a str,
    pub delta_max_us: u64,
    /// `None` (JSON null) for B1/S1, which apply no delivery timeout
    /// (contract v4 §9).
    pub t_pc_ms: Option<u64>,
    pub c_mbps: f64,
    pub s_bytes: u64,
    /// False only in the hidden receiver test mode (contract v4 §9).
    pub scientific_eligible: bool,
    /// Receiver-owned additions (P1 switch settings, test-mode fields).
    pub extra: Map<String, Value>,
}

/// Merge the runner meta document with the receiver's own fields.  Returns
/// the meta map and `g_report_us`.
pub fn build_meta(runner: &Value, own: &ReceiverMeta<'_>) -> Result<(Map<String, Value>, u64)> {
    let runner = runner.as_object().context("exp2 meta must be a JSON object")?;
    for key in RECEIVER_META_KEYS {
        ensure!(
            !runner.contains_key(*key),
            "exp2 meta must not carry the receiver-owned key {key:?}"
        );
    }
    for key in RUNNER_META_KEYS {
        ensure!(runner.contains_key(*key), "exp2 meta lacks {key:?}");
    }
    let campaign = runner["campaign"].as_str().context("campaign must be a string")?;
    ensure!(
        matches!(campaign, "L1-R" | "L2" | "pilot" | "calibration"),
        "unknown campaign {campaign:?}"
    );
    for key in ["trajectory_seed", "attempt"] {
        ensure!(
            runner[key].is_i64() || runner[key].is_u64(),
            "{key} must be an integer"
        );
    }
    ensure!(
        runner["epsilon_output_us"].is_u64(),
        "epsilon_output_us must be a non-negative integer"
    );
    let g_report_us = runner["g_report_us"]
        .as_u64()
        .filter(|g| *g > 0)
        .context("g_report_us must be a positive integer")?;
    if campaign == "L1-R" {
        ensure!(
            g_report_us == G_REPORT_R_US,
            "L1-R runs use G_report,R = {G_REPORT_R_US} us"
        );
    }
    if campaign == "pilot" {
        ensure!(runner.contains_key("layer"), "a pilot run's meta needs layer");
    }
    ensure!(own.c_mbps.is_finite(), "C_mbps must be finite");
    let mut meta = Map::new();
    meta.insert("role".into(), json!("meta"));
    meta.insert("contract".into(), json!(EXP2_CONTRACT_VERSION));
    meta.insert("metric_schema_version".into(), json!(EXP2_METRIC_SCHEMA_VERSION));
    meta.insert("run_id".into(), json!(own.run_id));
    meta.insert("method".into(), json!(own.method.label()));
    meta.insert("topology".into(), json!(own.topology));
    meta.insert("representation".into(), json!(own.representation));
    meta.insert("t0_us".into(), json!(own.t0_us));
    meta.insert("opportunities_sha256".into(), json!(own.opportunities_sha256));
    meta.insert("delta_max_us".into(), json!(own.delta_max_us));
    meta.insert("t_pc_ms".into(), json!(own.t_pc_ms));
    meta.insert("C_mbps".into(), json!(own.c_mbps));
    meta.insert("S_bytes".into(), json!(own.s_bytes));
    meta.insert("scientific_eligible".into(), json!(own.scientific_eligible));
    for (key, value) in &own.extra {
        ensure!(
            RECEIVER_META_KEYS.contains(&key.as_str()),
            "receiver meta extra {key:?} is not a receiver-owned key"
        );
        meta.insert(key.clone(), value.clone());
    }
    for (key, value) in runner {
        meta.insert(key.clone(), value.clone());
    }
    Ok((meta, g_report_us))
}

// --------------------------------------------------------------------------
// Recorder
// --------------------------------------------------------------------------

pub struct Exp2RecorderConfig {
    pub method: Exp2Method,
    pub run_id: String,
    pub ledger: Exp2OpportunityLedger,
    pub delta_max_us: u64,
    pub g_report_us: u64,
}

enum Engine {
    Plain,
    Core(Exp2Playout),
    P1(Exp2P1),
}

type CoreOut = (Vec<Exp2Decision>, Vec<TierRequest>);

impl Engine {
    fn arrive(&mut self, m: Modality, index: u32, t: u64) -> Result<CoreOut> {
        match self {
            Engine::Plain => Ok((Vec::new(), Vec::new())),
            Engine::Core(c) => Ok((c.arrive(m, index, t).map_err(|e| anyhow!(e))?, Vec::new())),
            Engine::P1(p) => p.arrive(m, index, t).map_err(|e| anyhow!(e)),
        }
    }

    fn delivery_timeout(&mut self, m: Modality, index: u32, t: u64) -> Result<CoreOut> {
        match self {
            Engine::Plain => Ok((Vec::new(), Vec::new())),
            Engine::Core(c) => Ok((
                c.delivery_timeout(m, index, t).map_err(|e| anyhow!(e))?,
                Vec::new(),
            )),
            Engine::P1(p) => p.delivery_timeout(m, index, t).map_err(|e| anyhow!(e)),
        }
    }

    fn advance_to(&mut self, t: u64) -> Result<CoreOut> {
        match self {
            Engine::Plain => Ok((Vec::new(), Vec::new())),
            Engine::Core(c) => Ok((c.advance_to(t).map_err(|e| anyhow!(e))?, Vec::new())),
            Engine::P1(p) => p.advance_to(t).map_err(|e| anyhow!(e)),
        }
    }

    fn finalize(&mut self, h: u64) -> Result<(CoreOut, Option<Exp2SchedulerLedger>)> {
        match self {
            Engine::Plain => Ok(((Vec::new(), Vec::new()), None)),
            Engine::Core(c) => {
                let (d, l) = c.finalize(h).map_err(|e| anyhow!(e))?;
                Ok(((d, Vec::new()), Some(l)))
            }
            Engine::P1(p) => {
                let (d, r, l) = p.finalize(h).map_err(|e| anyhow!(e))?;
                Ok(((d, r), Some(l)))
            }
        }
    }

    fn next_wakeup_us(&self) -> Option<u64> {
        match self {
            Engine::Plain => None,
            Engine::Core(c) => c.next_wakeup_us(),
            Engine::P1(p) => p.core().next_wakeup_us(),
        }
    }

    fn core(&self) -> Option<&Exp2Playout> {
        match self {
            Engine::Plain => None,
            Engine::Core(c) => Some(c),
            Engine::P1(p) => Some(p.core()),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct Slot {
    /// Route generation of the admitted copy (first admitted wins).
    admitted_gen: Option<u64>,
    /// A terminal row (release/drop/sealing) has been written.
    terminal: bool,
    seq: Option<u32>,
    gen_ts_us: Option<u64>,
    t_recv_us: Option<u64>,
    /// Core input time of the admitted copy.
    t_in_us: Option<u64>,
    payload_len: u32,
    /// S1 due learned from an S1 action (incl. a suppressed post-H one).
    s1_due_us: Option<u64>,
    /// Admitted for the core but not fed because its input time is after H.
    held_post_horizon: bool,
    /// Wall time of the first wire delivery-timeout observation.
    timeout_observed_us: Option<u64>,
    /// Route copies of this slot discarded without admission (P1 switch
    /// barrier / stale generation); written on a sealing row as a diagnostic.
    copies_discarded: u32,
    /// First transport (switch-gate) discard of a never-admitted copy:
    /// `(t_recv, reason)`.
    first_gate_discard: Option<(u64, &'static str)>,
}

/// Counters written into the closing `wire` and `instrumentation_end` rows.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Exp2RecorderStats {
    pub rx_rows: u64,
    pub warmup_ignored: u64,
    pub orphan_rx: u64,
    pub event_id_mismatch: u64,
    pub route_copy_discards: u64,
    pub ingress_drops: u64,
    pub post_horizon_arrivals: u64,
    pub post_horizon_terminals_suppressed: u64,
    pub rows_refused_after_end: u64,
    pub integrity_warnings: u64,
    pub clamped_core_inputs: u64,
    pub service_after_due_objects: u64,
    pub service_after_due_bytes: u64,
    pub service_after_due_us: u64,
    pub delivery_timeout_observations: u64,
    pub core_misses: u64,
    pub delta_updates_miss_step: u64,
    pub delta_updates_q_increase: u64,
    pub delta_updates_q_decrease: u64,
    pub core_diagnostics: u64,
    pub tier_requests: u64,
    pub tier_switches: u64,
}

/// An object as received (header + route), independent of transport.
#[derive(Debug, Clone, Copy)]
pub struct Exp2Rx {
    pub header: Header,
    pub route_generation: u64,
    pub t_recv_us: u64,
}

/// What `admit` decided for one received object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// First admitted copy of an opportunity slot.
    Admitted(Modality, u32),
    /// A later copy of an already-admitted slot: `route_copy_discard` written.
    CopyDiscarded,
    /// Warm-up, orphan PTS, event-id mismatch, or recorder closed.
    NotAdmitted,
}

pub struct Exp2Recorder<W: Write> {
    out: W,
    clock: Box<dyn FnMut() -> u64 + Send>,
    run_id: String,
    method: Exp2Method,
    t0: u64,
    h: u64,
    end_at: u64,
    pts: [Vec<u64>; 2],
    by_pts: [HashMap<u64, u32>; 2],
    engine: Engine,
    slots: [Vec<Slot>; 2],
    meta_written: bool,
    workload_started: bool,
    sealed: bool,
    ended: bool,
    write_failed: bool,
    core_last_in: Option<u64>,
    core_drained: Option<u64>,
    s1_finished: bool,
    stats: Exp2RecorderStats,
    side_rows: Vec<String>,
    max_write_lag_us: u64,
    /// Last committed Δ_eff per modality (controller rows on change).
    last_delta_eff: [Option<u64>; 2],
}

impl<W: Write> Exp2Recorder<W> {
    pub fn new(
        out: W,
        clock: Box<dyn FnMut() -> u64 + Send>,
        config: Exp2RecorderConfig,
    ) -> Result<Self> {
        let ledger = config.ledger;
        ledger.validate().map_err(|e| anyhow!(e))?;
        ensure!(
            ledger.pc_pts_us.len() == N_PC && ledger.haptic_pts_us.len() == N_HAPTIC,
            "exp2 ledger must hold {N_PC}/{N_HAPTIC} slots"
        );
        ensure!(config.g_report_us > 0, "g_report_us must be positive");
        let params = Exp2Params::registered(config.delta_max_us);
        let engine = match config.method {
            Exp2Method::B1 | Exp2Method::S1 => Engine::Plain,
            Exp2Method::P1 => Engine::P1(
                Exp2P1::new(
                    params,
                    Exp2FsmParams::registered(config.delta_max_us),
                    ledger.clone(),
                )
                .map_err(|e| anyhow!(e))?,
            ),
            method => Engine::Core(
                Exp2Playout::new(
                    method.core_mode().expect("core method"),
                    params,
                    ledger.clone(),
                )
                .map_err(|e| anyhow!(e))?,
            ),
        };
        let index = |pts: &[u64]| -> HashMap<u64, u32> {
            pts.iter().enumerate().map(|(i, p)| (*p, i as u32)).collect()
        };
        let t0 = ledger.t0_us;
        let h = horizon_us(t0);
        Ok(Self {
            out,
            clock,
            run_id: config.run_id,
            method: config.method,
            t0,
            h,
            end_at: h + config.g_report_us,
            by_pts: [index(&ledger.pc_pts_us), index(&ledger.haptic_pts_us)],
            pts: [ledger.pc_pts_us.clone(), ledger.haptic_pts_us.clone()],
            engine,
            slots: [vec![Slot::default(); N_PC], vec![Slot::default(); N_HAPTIC]],
            meta_written: false,
            workload_started: false,
            sealed: false,
            ended: false,
            write_failed: false,
            core_last_in: None,
            core_drained: None,
            s1_finished: false,
            stats: Exp2RecorderStats::default(),
            side_rows: Vec::new(),
            max_write_lag_us: 0,
            last_delta_eff: [None, None],
        })
    }

    pub fn method(&self) -> Exp2Method {
        self.method
    }

    /// t0 is fixed at construction and nothing changes it (gate 6).
    pub fn t0_us(&self) -> u64 {
        self.t0
    }

    pub fn horizon_us(&self) -> u64 {
        self.h
    }

    pub fn end_at_us(&self) -> u64 {
        self.end_at
    }

    pub fn is_sealed(&self) -> bool {
        self.sealed
    }

    pub fn is_ended(&self) -> bool {
        self.ended
    }

    pub fn write_failed(&self) -> bool {
        self.write_failed
    }

    pub fn stats(&self) -> &Exp2RecorderStats {
        &self.stats
    }

    pub fn core(&self) -> Option<&Exp2Playout> {
        self.engine.core()
    }

    /// P1 only: the FSM state (the desired tier state WP3 applies).
    pub fn p1_fsm_snapshot(&self) -> Option<crate::exp2_fsm::Exp2FsmSnapshot> {
        match &self.engine {
            Engine::P1(p) => Some(p.fsm().snapshot()),
            _ => None,
        }
    }

    /// Legacy-log diagnostics (controller updates, misses, core diagnostics)
    /// that have no exp2 contract role; the binary writes them as `info` rows
    /// of the legacy receiver log.
    pub fn take_side_rows(&mut self) -> Vec<String> {
        std::mem::take(&mut self.side_rows)
    }

    // ---------------------------------------------------------------- output

    fn write_value(&mut self, mut row: Map<String, Value>, log_arrival: bool) -> Result<()> {
        if self.ended {
            self.stats.rows_refused_after_end += 1;
            return Ok(());
        }
        ensure!(self.meta_written, "exp2 row before the meta line");
        row.insert("run_id".into(), json!(self.run_id));
        // Contract v4 §9: `t_log_arrival_us` is the pre-write stamp (a row
        // cannot contain its own write-completion instant); the write lag
        // (completion − this stamp, serialisation included) is measured for
        // every row and its maximum goes on `instrumentation_end`.
        let t_before = (self.clock)();
        if log_arrival {
            row.insert("t_log_arrival_us".into(), json!(t_before));
        }
        self.write_line(Value::Object(row), t_before)
    }

    fn write_line(&mut self, row: Value, t_before: u64) -> Result<()> {
        let mut line = serde_json::to_vec(&row)?;
        line.push(b'\n');
        if let Err(error) = self.out.write_all(&line).and_then(|_| self.out.flush()) {
            self.write_failed = true;
            return Err(error).context("write exp2 JSONL row");
        }
        let lag = (self.clock)().saturating_sub(t_before);
        self.max_write_lag_us = self.max_write_lag_us.max(lag);
        Ok(())
    }

    /// The first line (contract §6).  `meta` comes from [`build_meta`].
    pub fn write_meta(&mut self, mut meta: Map<String, Value>) -> Result<()> {
        ensure!(!self.meta_written, "meta written twice");
        ensure!(
            meta.get("t0_us").and_then(Value::as_u64) == Some(self.t0),
            "meta t0_us differs from the recorder t0"
        );
        ensure!(
            meta.get("run_id").and_then(Value::as_str) == Some(self.run_id.as_str()),
            "meta run_id differs"
        );
        ensure!(
            meta.get("method").and_then(Value::as_str) == Some(self.method.label()),
            "meta method differs"
        );
        let t_before = (self.clock)();
        meta.insert("t_us".into(), json!(t_before));
        self.meta_written = true;
        self.write_line(Value::Object(meta), t_before)
    }

    fn slot_fields(&self, m: Modality, index: u32) -> Map<String, Value> {
        let mut row = Map::new();
        row.insert("track".into(), json!(m.as_str()));
        let pts = self.pts[m.ix()][index as usize];
        row.insert("pts_us".into(), json!(pts));
        match m {
            Modality::Pc => {
                row.insert("event_id".into(), json!(index + 1));
                row.insert("i".into(), json!(index));
            }
            Modality::Haptic => {
                let event_id = if index % 3 == 0 { index / 3 + 1 } else { 0 };
                row.insert("event_id".into(), json!(event_id));
                row.insert("i".into(), json!(index / 3));
                row.insert("k".into(), json!(index));
            }
        }
        let slot = &self.slots[m.ix()][index as usize];
        if let Some(seq) = slot.seq {
            row.insert("seq".into(), json!(seq));
        }
        if let Some(gen) = slot.gen_ts_us {
            row.insert("gen_ts_us".into(), json!(gen));
        }
        if let Some(route) = slot.admitted_gen {
            row.insert("route_generation".into(), json!(route));
        }
        row
    }

    fn integrity_warning(&mut self, kind: &str, mut fields: Map<String, Value>) -> Result<()> {
        self.stats.integrity_warnings += 1;
        fields.insert("role".into(), json!("integrity_warning"));
        fields.insert("kind".into(), json!(kind));
        fields.insert("t_us".into(), json!((self.clock)()));
        self.write_value(fields, false)
    }

    // ---------------------------------------------------------------- inputs

    fn rx_row(&mut self, rx: &Exp2Rx, slot: Option<(Modality, u32)>) -> Result<()> {
        let h = rx.header;
        let mut row = Map::new();
        row.insert("role".into(), json!("rx"));
        row.insert("t_us".into(), json!((self.clock)()));
        row.insert(
            "track".into(),
            json!(if h.track_id == TRACK_PC { "pc" } else { "haptic" }),
        );
        row.insert("seq".into(), json!(h.seq));
        row.insert("tier".into(), json!(h.tier));
        row.insert("pts_us".into(), json!(h.pts_us));
        row.insert("event_id".into(), json!(h.event_id));
        if let Some((m, index)) = slot {
            row.insert(
                "i".into(),
                json!(if m == Modality::Pc { index } else { index / 3 }),
            );
            if m == Modality::Haptic {
                row.insert("k".into(), json!(index));
            }
        }
        row.insert("route_generation".into(), json!(rx.route_generation));
        row.insert("t_recv_us".into(), json!(rx.t_recv_us));
        row.insert("gen_ts_us".into(), json!(h.gen_ts_us));
        row.insert("payload_len".into(), json!(h.payload_len));
        self.stats.rx_rows += 1;
        self.write_value(row, false)
    }

    /// Map a header to its planned slot by EXACT pts and event-id agreement
    /// (contract §2: no approximate pairing).  Writes the rx row and any
    /// integrity warning.  `None` for warm-up, orphan or mismatched objects.
    fn locate_and_log_rx(&mut self, rx: &Exp2Rx) -> Result<Option<(Modality, u32)>> {
        let h = rx.header;
        if is_warmup_seq(h.seq) {
            self.stats.warmup_ignored += 1;
            return Ok(None);
        }
        let Some(m) = modality_of(h.track_id) else {
            bail!("exp2 recorder received an unknown track id {}", h.track_id);
        };
        let slot = self.by_pts[m.ix()].get(&h.pts_us).copied();
        let slot = match slot {
            Some(index) => {
                let expected = match m {
                    Modality::Pc => index + 1,
                    Modality::Haptic if index % 3 == 0 => index / 3 + 1,
                    Modality::Haptic => 0,
                };
                if h.event_id == expected {
                    Some((m, index))
                } else {
                    self.stats.event_id_mismatch += 1;
                    None
                }
            }
            None => {
                self.stats.orphan_rx += 1;
                None
            }
        };
        self.rx_row(rx, slot)?;
        if slot.is_none() {
            let mut f = Map::new();
            f.insert("track".into(), json!(m.as_str()));
            f.insert("pts_us".into(), json!(h.pts_us));
            f.insert("event_id".into(), json!(h.event_id));
            f.insert("seq".into(), json!(h.seq));
            self.integrity_warning("unplanned_slot_or_event_id_mismatch", f)?;
            return Ok(None);
        }
        // Contract §2 diagnostic: t_gen − pts − t0 > ε_ref (warning, not invalid).
        let excess = h.gen_ts_us as i128 - h.pts_us as i128 - self.t0 as i128;
        if excess > EPS_REF_US as i128 {
            let mut f = Map::new();
            f.insert("track".into(), json!(m.as_str()));
            f.insert("pts_us".into(), json!(h.pts_us));
            f.insert("event_id".into(), json!(h.event_id));
            f.insert("seq".into(), json!(h.seq));
            f.insert("tgen_minus_ref_us".into(), json!(excess as i64));
            self.integrity_warning("tgen_ref_exceeds_eps_ref", f)?;
        }
        Ok(slot)
    }

    fn copy_discard(
        &mut self,
        m: Modality,
        index: u32,
        rx: &Exp2Rx,
        legacy_reason: Option<&str>,
    ) -> Result<()> {
        self.stats.route_copy_discards += 1;
        let copies = &mut self.slots[m.ix()][index as usize].copies_discarded;
        *copies = copies.saturating_add(1);
        let h = rx.header;
        let mut row = Map::new();
        row.insert("role".into(), json!("drop"));
        row.insert("reason".into(), json!(ROUTE_COPY_DISCARD));
        row.insert("t_us".into(), json!((self.clock)()));
        row.insert("track".into(), json!(m.as_str()));
        row.insert("seq".into(), json!(h.seq));
        row.insert("pts_us".into(), json!(h.pts_us));
        row.insert("event_id".into(), json!(h.event_id));
        row.insert("route_generation".into(), json!(rx.route_generation));
        if let Some(first) = self.slots[m.ix()][index as usize].admitted_gen {
            row.insert("admitted_route_generation".into(), json!(first));
        }
        if let Some(reason) = legacy_reason {
            row.insert("transport_reason".into(), json!(reason));
        }
        row.insert("t_recv_us".into(), json!(rx.t_recv_us));
        self.write_value(row, true)
    }

    /// A received object offered for admission.  Writes the rx row; the first
    /// admitted copy of a slot wins, every later copy (any route generation)
    /// is a `route_copy_discard`.  B1 releases the admitted copy at `t_recv`
    /// if `t_recv <= H` and the ledger is not sealed.  For the core methods
    /// the caller must then call [`Self::feed_arrival`] (static path: from the
    /// scheduler task; P1: immediately).
    pub fn admit(&mut self, rx: Exp2Rx) -> Result<Admission> {
        if self.ended {
            self.stats.rows_refused_after_end += 1;
            return Ok(Admission::NotAdmitted);
        }
        let Some((m, index)) = self.locate_and_log_rx(&rx)? else {
            return Ok(Admission::NotAdmitted);
        };
        if self.slots[m.ix()][index as usize].admitted_gen.is_some() {
            self.copy_discard(m, index, &rx, None)?;
            return Ok(Admission::CopyDiscarded);
        }
        {
            let slot = &mut self.slots[m.ix()][index as usize];
            slot.admitted_gen = Some(rx.route_generation);
            slot.seq = Some(rx.header.seq);
            slot.gen_ts_us = Some(rx.header.gen_ts_us);
            slot.t_recv_us = Some(rx.t_recv_us);
            slot.payload_len = rx.header.payload_len;
        }
        if self.method == Exp2Method::B1 {
            if self.sealed || rx.t_recv_us > self.h {
                self.stats.post_horizon_arrivals += 1;
            } else {
                // Registration §2 / contract §4: B1 release := t_recv.
                self.write_release(m, index, rx.t_recv_us, None, None)?;
            }
        }
        Ok(Admission::Admitted(m, index))
    }

    /// A transport-level discard of a route copy that was never admitted
    /// (P1 switch barrier / stale generation): rx row + `route_copy_discard`.
    pub fn discard_route_copy(&mut self, rx: Exp2Rx, transport_reason: &'static str) -> Result<()> {
        if self.ended {
            self.stats.rows_refused_after_end += 1;
            return Ok(());
        }
        let Some((m, index)) = self.locate_and_log_rx(&rx)? else {
            return Ok(());
        };
        let slot = &mut self.slots[m.ix()][index as usize];
        if slot.first_gate_discard.is_none() && rx.t_recv_us <= self.h {
            slot.first_gate_discard = Some((rx.t_recv_us, transport_reason));
        }
        self.copy_discard(m, index, &rx, Some(transport_reason))
    }

    /// The admitted copy could not be handed to the scheduler (bounded
    /// ingress queue full / closed): a receiver drop terminal.
    pub fn ingress_drop(&mut self, m: Modality, index: u32, at_us: u64, reason: &str) -> Result<()> {
        if self.ended {
            self.stats.rows_refused_after_end += 1;
            return Ok(());
        }
        self.stats.ingress_drops += 1;
        if self.sealed || at_us > self.h {
            self.stats.post_horizon_terminals_suppressed += 1;
            return Ok(());
        }
        self.write_drop(m, index, at_us, reason, None, None, None)
    }

    /// Feed an admitted arrival to the core (core methods only).
    pub fn feed_arrival(&mut self, m: Modality, index: u32, t_recv_us: u64) -> Result<Vec<TierRequest>> {
        ensure!(
            matches!(self.engine, Engine::Core(_) | Engine::P1(_)),
            "feed_arrival on a method without a core"
        );
        ensure!(
            self.slots[m.ix()][index as usize].admitted_gen.is_some(),
            "feed_arrival for a slot that was never admitted"
        );
        if self.sealed || self.ended {
            self.stats.post_horizon_arrivals += 1;
            return Ok(Vec::new());
        }
        let t_in = self.clamp_input(t_recv_us);
        if t_in > self.h {
            self.stats.post_horizon_arrivals += 1;
            self.slots[m.ix()][index as usize].held_post_horizon = true;
            return Ok(Vec::new());
        }
        if t_in != t_recv_us {
            self.stats.clamped_core_inputs += 1;
        }
        self.wait_clock_reaches(t_in)?;
        self.slots[m.ix()][index as usize].t_in_us = Some(t_in);
        self.core_last_in = Some(t_in);
        // registration §2 wire metric: bytes/time served after the committed due.
        if let Some(c) = self.engine.core().and_then(|core| core.committed(m, index)) {
            if t_in > c.due_us {
                self.stats.service_after_due_objects += 1;
                self.stats.service_after_due_bytes +=
                    self.slots[m.ix()][index as usize].payload_len as u64;
                self.stats.service_after_due_us += t_in - c.due_us;
            }
        }
        let out = self.engine.arrive(m, index, t_in)?;
        self.handle_core(out)
    }

    /// A wire delivery-timeout observation for `(m, index)` (core methods).
    /// The observation is written immediately as a `wire` row; the core keeps
    /// it as a non-terminal label resolved only at `finalize`.
    pub fn delivery_timeout(&mut self, m: Modality, index: u32, now_us: u64) -> Result<Vec<TierRequest>> {
        if self.sealed || self.ended {
            return Ok(Vec::new());
        }
        self.stats.delivery_timeout_observations += 1;
        let first = &mut self.slots[m.ix()][index as usize].timeout_observed_us;
        if first.is_none() {
            *first = Some(now_us);
        }
        let mut row = self.slot_fields(m, index);
        row.insert("role".into(), json!("wire"));
        row.insert("source".into(), json!("receiver"));
        row.insert("kind".into(), json!("delivery_timeout_observed"));
        row.insert("t_observed_us".into(), json!(now_us));
        row.insert("t_us".into(), json!((self.clock)()));
        self.write_value(row, false)?;
        if !matches!(self.engine, Engine::Core(_) | Engine::P1(_)) {
            return Ok(Vec::new());
        }
        let t_in = self.clamp_input(now_us);
        if t_in > self.h {
            return Ok(Vec::new());
        }
        self.wait_clock_reaches(t_in)?;
        self.core_last_in = Some(t_in);
        let out = self.engine.delivery_timeout(m, index, t_in)?;
        self.handle_core(out)
    }

    /// A clamped core time is at most 1 µs past the wall clock (`drained +
    /// 1` with `drained == now`).  Never act in the future: wait until the
    /// clock reaches it, so every decision time is <= its log receipt.
    fn wait_clock_reaches(&mut self, t: u64) -> Result<()> {
        for _ in 0..10_000_000u32 {
            if (self.clock)() >= t {
                return Ok(());
            }
            std::hint::spin_loop();
        }
        bail!("monotonic clock did not reach the core time {t}")
    }

    fn clamp_input(&self, t: u64) -> u64 {
        let mut t_in = t;
        if let Some(last) = self.core_last_in {
            t_in = t_in.max(last);
        }
        if let Some(drained) = self.core_drained {
            t_in = t_in.max(drained + 1);
        }
        t_in
    }

    /// Earliest instant at which [`Self::advance`] has something to do.
    pub fn next_wakeup_us(&self) -> Option<u64> {
        if self.ended {
            return None;
        }
        let mut wake = Vec::new();
        if !self.workload_started {
            wake.push(self.t0);
        }
        if !self.sealed {
            if let Some(core) = self.engine.next_wakeup_us() {
                wake.push(core.min(self.h));
            }
            wake.push(match self.engine {
                Engine::Plain => self.h + PLAIN_SEAL_EXECUTION_DELAY_US,
                _ => self.h,
            });
        } else {
            wake.push(self.end_at);
        }
        wake.into_iter().min()
    }

    /// Wall-clock tick: `workload_started` at t0, core timers up to
    /// `min(now, H)`, the H seal for the core methods (at `now >= H`), and
    /// for B1/S1 (when the S1 task has finished) at `now >= H + delay`.
    pub fn advance(&mut self, now_us: u64) -> Result<Vec<TierRequest>> {
        if self.ended {
            return Ok(Vec::new());
        }
        if !self.workload_started && now_us >= self.t0 {
            self.workload_started = true;
            let mut row = Map::new();
            row.insert("role".into(), json!("workload_started"));
            row.insert("t_us".into(), json!(now_us));
            row.insert("t0_us".into(), json!(self.t0));
            self.write_value(row, false)?;
        }
        let mut requests = Vec::new();
        if self.sealed {
            return Ok(requests);
        }
        match self.engine {
            Engine::Plain => {
                let s1_ready = self.method == Exp2Method::B1 || self.s1_finished;
                if s1_ready && now_us >= self.h + PLAIN_SEAL_EXECUTION_DELAY_US {
                    self.seal_plain(&|_| None)?;
                }
            }
            Engine::Core(_) | Engine::P1(_) => {
                let mut target = now_us.min(self.h);
                if let Some(last) = self.core_last_in {
                    target = target.max(last);
                }
                if self.core_drained.is_none_or(|d| target > d) {
                    self.wait_clock_reaches(target)?;
                    let out = self.engine.advance_to(target)?;
                    self.core_drained = Some(target);
                    self.core_last_in = Some(target);
                    requests.extend(self.handle_core(out)?);
                }
                if now_us >= self.h {
                    requests.extend(self.seal_core()?);
                }
            }
        }
        Ok(requests)
    }

    // ------------------------------------------------------------ core rows

    fn handle_core(&mut self, (decisions, requests): CoreOut) -> Result<Vec<TierRequest>> {
        for decision in decisions {
            match decision {
                Exp2Decision::Commit(rec) => {
                    let targets = [
                        (Modality::Pc, rec.event, rec.pc, rec.hold_g_us[0], rec.compression_us[0]),
                        (Modality::Haptic, 3 * rec.event, rec.haptic[0], rec.hold_g_us[1], rec.compression_us[1]),
                        (Modality::Haptic, 3 * rec.event + 1, rec.haptic[1], rec.hold_g_us[1], rec.compression_us[1]),
                        (Modality::Haptic, 3 * rec.event + 2, rec.haptic[2], rec.hold_g_us[1], rec.compression_us[1]),
                    ];
                    for (m, index, due, g, compression) in targets {
                        let mut row = self.slot_fields(m, index);
                        row.remove("seq");
                        row.remove("gen_ts_us");
                        row.remove("route_generation");
                        row.insert("role".into(), json!("decision"));
                        row.insert("t_us".into(), json!((self.clock)()));
                        row.insert("t_commit_us".into(), json!(rec.at_us));
                        row.insert("due_us".into(), json!(due.due_us));
                        row.insert("delta_eff_us".into(), json!(due.delta_eff_us));
                        row.insert("delta_generation".into(), json!(due.delta_generation));
                        row.insert("hold_g_us".into(), json!(g));
                        row.insert("compression_us".into(), json!(compression));
                        self.write_value(row, false)?;
                    }
                    // Contract v4 §9 `controller`: Δ_eff changes per modality.
                    // P0-NP has one Δ per modality; P0/P1 share one Δ and
                    // S3NPA' a fixed one, so those are written once as "shared".
                    let per_modality = self.method == Exp2Method::P0np;
                    for (m, due, g, compression) in [
                        (Modality::Pc, rec.pc, rec.hold_g_us[0], rec.compression_us[0]),
                        (Modality::Haptic, rec.haptic[0], rec.hold_g_us[1], rec.compression_us[1]),
                    ] {
                        if !per_modality && m == Modality::Haptic {
                            continue;
                        }
                        let modality = if per_modality { m.as_str() } else { "shared" };
                        let previous = self.last_delta_eff[m.ix()];
                        if previous == Some(due.delta_eff_us) {
                            continue;
                        }
                        self.last_delta_eff[m.ix()] = Some(due.delta_eff_us);
                        let mut row = self.controller_row("delta_eff_commit", modality, due.delta_eff_us);
                        row.insert("i".into(), json!(rec.event));
                        row.insert("at_us".into(), json!(rec.at_us));
                        row.insert("from_us".into(), json!(previous));
                        row.insert("delta_generation".into(), json!(due.delta_generation));
                        row.insert("hold_g_us".into(), json!(g));
                        row.insert("compression_us".into(), json!(compression));
                        self.write_value(row, false)?;
                    }
                }
                Exp2Decision::Release(r) => {
                    if self.slots[r.modality.ix()][r.index as usize].terminal {
                        bail!("core released an opportunity that already has a terminal row");
                    }
                    // Contract v4 §9: the release time is the ACTUAL dispatch
                    // instant (the scheduled due stays in the decision row).
                    let actual = (self.clock)().max(r.t_release_us);
                    let t_in = self.slots[r.modality.ix()][r.index as usize].t_in_us;
                    self.write_release(r.modality, r.index, actual, Some(r.grace), t_in)?;
                }
                Exp2Decision::Drop(r) => {
                    if self.slots[r.modality.ix()][r.index as usize].terminal {
                        bail!("core dropped an opportunity that already has a terminal row");
                    }
                    let detail = match r.reason {
                        crate::exp2_playout::DropReason::BufferBound(b) => Some(b.as_str()),
                        _ => None,
                    };
                    self.write_drop(
                        r.modality,
                        r.index,
                        r.at_us,
                        r.reason.as_str(),
                        detail,
                        r.committed,
                        None,
                    )?;
                }
                Exp2Decision::Miss(r) => {
                    self.stats.core_misses += 1;
                    self.side_rows.push(format!(
                        "\"event\":\"exp2_controller_terminal_miss\",\"track\":\"{}\",\"index\":{},\"at_us\":{},\"due_us\":{}",
                        r.modality.as_str(),
                        r.index,
                        r.at_us,
                        r.committed.due_us
                    ));
                }
                Exp2Decision::DeltaUpdate(u) => {
                    match u.cause {
                        crate::exp2_playout::DeltaCause::MissStep => {
                            self.stats.delta_updates_miss_step += 1
                        }
                        crate::exp2_playout::DeltaCause::QIncrease => {
                            self.stats.delta_updates_q_increase += 1
                        }
                        crate::exp2_playout::DeltaCause::QDecrease => {
                            self.stats.delta_updates_q_decrease += 1
                        }
                    }
                    let modality = match self.method {
                        Exp2Method::P0np if u.controller == 0 => "pc",
                        Exp2Method::P0np => "haptic",
                        _ => "shared",
                    };
                    // Contract v4 §9 controller rows: the cause row
                    // (miss_step: value = Δ* after the step; q_update: value =
                    // the nearest-rank Q that moved Δ*), then the Δ* row.
                    let mut cause = match u.cause {
                        crate::exp2_playout::DeltaCause::MissStep => {
                            let mut r = self.controller_row("miss_step", modality, u.to_us);
                            r.insert("window_misses".into(), json!(u.window_misses));
                            r
                        }
                        _ => {
                            let q = u.q_us.map(|q| q.max(0) as u64);
                            let mut r = self.controller_row("q_update", modality, q.unwrap_or(0));
                            r.insert("value_us".into(), json!(q));
                            r.insert("direction".into(), json!(u.cause.as_str()));
                            r
                        }
                    };
                    cause.insert("at_us".into(), json!(u.at_us));
                    cause.insert("controller".into(), json!(u.controller));
                    self.write_value(cause, false)?;
                    let mut star = self.controller_row("delta_star", modality, u.to_us);
                    star.insert("at_us".into(), json!(u.at_us));
                    star.insert("controller".into(), json!(u.controller));
                    star.insert("from_us".into(), json!(u.from_us));
                    star.insert("cause".into(), json!(u.cause.as_str()));
                    star.insert("delta_generation".into(), json!(u.delta_generation));
                    self.write_value(star, false)?;
                    self.side_rows.push(format!(
                        "\"event\":\"exp2_delta_update\",\"controller\":{},\"at_us\":{},\"cause\":\"{}\",\"from_us\":{},\"to_us\":{},\"delta_generation\":{},\"window_misses\":{},\"q_us\":{}",
                        u.controller,
                        u.at_us,
                        u.cause.as_str(),
                        u.from_us,
                        u.to_us,
                        u.delta_generation,
                        u.window_misses.map(|v| v.to_string()).unwrap_or_else(|| "null".into()),
                        u.q_us.map(|v| v.to_string()).unwrap_or_else(|| "null".into()),
                    ));
                }
                Exp2Decision::PairComplete(_) | Exp2Decision::DeliveryTimeoutObserved(_) => {}
                Exp2Decision::Diagnostic(d) => {
                    self.stats.core_diagnostics += 1;
                    self.side_rows.push(format!(
                        "\"event\":\"exp2_core_diagnostic\",\"kind\":\"{:?}\",\"track\":\"{}\",\"index\":{},\"at_us\":{}",
                        d.kind,
                        d.modality.as_str(),
                        d.index,
                        d.at_us
                    ));
                }
            }
        }
        Ok(requests)
    }

    fn controller_row(&mut self, event: &str, modality: &str, value_us: u64) -> Map<String, Value> {
        let mut row = Map::new();
        row.insert("role".into(), json!("controller"));
        row.insert("t_us".into(), json!((self.clock)()));
        row.insert("modality".into(), json!(modality));
        row.insert("event".into(), json!(event));
        row.insert("value_us".into(), json!(value_us));
        row
    }

    fn write_release(
        &mut self,
        m: Modality,
        index: u32,
        t_release_us: u64,
        grace: Option<bool>,
        core_t_in: Option<u64>,
    ) -> Result<()> {
        let mut row = self.slot_fields(m, index);
        let slot = &self.slots[m.ix()][index as usize];
        let t_recv = slot.t_recv_us;
        row.insert("role".into(), json!("release"));
        row.insert("t_us".into(), json!(t_release_us));
        row.insert("t_release_us".into(), json!(t_release_us));
        if let Some(t) = t_recv {
            row.insert("t_recv_us".into(), json!(t));
        }
        if let Some(t) = core_t_in {
            row.insert("t_core_in_us".into(), json!(t));
        }
        if let Some(g) = grace {
            row.insert("grace".into(), json!(g));
        }
        self.slots[m.ix()][index as usize].terminal = true;
        self.write_value(row, true)
    }

    #[allow(clippy::too_many_arguments)]
    fn write_drop(
        &mut self,
        m: Modality,
        index: u32,
        at_us: u64,
        reason: &str,
        detail: Option<&str>,
        committed: Option<CommittedDue>,
        t_observed_us: Option<u64>,
    ) -> Result<()> {
        let mut row = self.slot_fields(m, index);
        row.insert("role".into(), json!("drop"));
        row.insert("reason".into(), json!(reason));
        if let Some(d) = detail {
            row.insert("detail".into(), json!(d));
        }
        row.insert("t_us".into(), json!(at_us));
        if let Some(t) = self.slots[m.ix()][index as usize].t_recv_us {
            row.insert("t_recv_us".into(), json!(t));
        }
        if let Some(c) = committed {
            row.insert("due_us".into(), json!(c.due_us));
            row.insert("delta_eff_us".into(), json!(c.delta_eff_us));
        }
        if let Some(t) = t_observed_us {
            row.insert("t_observed_us".into(), json!(t));
        }
        self.slots[m.ix()][index as usize].terminal = true;
        self.write_value(row, true)
    }

    /// A sealing row (contract v4 §9): `t_us` is the ACTUAL write instant
    /// (>= H) and `seal_horizon_us = H` names the horizon it seals.
    fn write_sealing(&mut self, m: Modality, index: u32, role: &str, due: Option<u64>) -> Result<()> {
        let mut row = self.slot_fields(m, index);
        row.insert("role".into(), json!(role));
        row.insert("t_us".into(), json!((self.clock)()));
        row.insert("seal_horizon_us".into(), json!(self.h));
        if let Some(d) = due {
            row.insert("due_us".into(), json!(d));
        }
        self.slots[m.ix()][index as usize].terminal = true;
        self.write_value(row, false)
    }

    /// A drop decided by the seal (delivery timeout label, S1 unreleased,
    /// P1 switch barrier): `t_us` = actual write instant, `seal_horizon_us = H`.
    fn write_seal_drop(
        &mut self,
        m: Modality,
        index: u32,
        reason: &str,
        due: Option<u64>,
        extra: Map<String, Value>,
    ) -> Result<()> {
        let mut row = self.slot_fields(m, index);
        row.insert("role".into(), json!("drop"));
        row.insert("reason".into(), json!(reason));
        row.insert("t_us".into(), json!((self.clock)()));
        row.insert("seal_horizon_us".into(), json!(self.h));
        if let Some(t) = self.slots[m.ix()][index as usize].t_recv_us {
            row.insert("t_recv_us".into(), json!(t));
        }
        if let Some(d) = due {
            row.insert("due_us".into(), json!(d));
        }
        for (k, v) in extra {
            row.insert(k, v);
        }
        self.slots[m.ix()][index as usize].terminal = true;
        self.write_value(row, true)
    }

    /// Contract v4 §9 / registration §2-2: a slot never admitted but with an
    /// arrived copy discarded by the switch gate on every route is a terminal
    /// `switch_barrier` drop.  Returns whether it wrote one.
    fn seal_switch_barrier(&mut self, m: Modality, index: u32) -> Result<bool> {
        let slot = &self.slots[m.ix()][index as usize];
        if slot.terminal || slot.admitted_gen.is_some() {
            return Ok(false);
        }
        let Some((t_first, transport_reason)) = slot.first_gate_discard else {
            return Ok(false);
        };
        let mut extra = Map::new();
        extra.insert("t_first_discard_us".into(), json!(t_first));
        extra.insert("transport_reason".into(), json!(transport_reason));
        extra.insert("route_copies_discarded".into(), json!(slot.copies_discarded));
        self.write_seal_drop(m, index, SWITCH_BARRIER, None, extra)?;
        Ok(true)
    }

    fn seal_core(&mut self) -> Result<Vec<TierRequest>> {
        let ((decisions, mut requests), ledger) = self.engine.finalize(self.h)?;
        self.core_drained = Some(self.h);
        requests.extend(self.handle_core((decisions, Vec::new()))?);
        let ledger = ledger.context("core finalize returned no ledger")?;
        ensure!(ledger.horizon_us == self.h, "sealed ledger horizon mismatch");
        self.sealed = true;
        for m in Modality::ALL {
            let entries: Vec<_> = ledger.entries(m).to_vec();
            ensure!(entries.len() == self.slots[m.ix()].len(), "sealed ledger size");
            for (index, entry) in entries.into_iter().enumerate() {
                let index = index as u32;
                if self.seal_switch_barrier(m, index)? {
                    continue;
                }
                let written = self.slots[m.ix()][index as usize].terminal;
                match entry.terminal {
                    SchedulerTerminal::Released { .. } => {
                        ensure!(written, "core released {}[{index}] without a release row", m.as_str());
                    }
                    SchedulerTerminal::Dropped {
                        reason: crate::exp2_playout::DropReason::DeliveryTimeout,
                        at_us,
                    } => {
                        if !written {
                            // Seal-decided drop (contract v4 §9); the
                            // observation instant is in the earlier wire row
                            // (the core's label time can be 1 µs later when
                            // the input was clamped).
                            let observed = self.slots[m.ix()][index as usize]
                                .timeout_observed_us
                                .unwrap_or(at_us);
                            let mut extra = Map::new();
                            extra.insert("t_observed_us".into(), json!(observed));
                            self.write_seal_drop(
                                m,
                                index,
                                "delivery_timeout",
                                entry.committed.map(|c| c.due_us),
                                extra,
                            )?;
                        }
                    }
                    SchedulerTerminal::Dropped { .. } => {
                        ensure!(written, "core dropped {}[{index}] without a drop row", m.as_str());
                    }
                    SchedulerTerminal::PendingAtHorizon => {
                        if !written {
                            let due = entry.committed.map(|c| c.due_us);
                            ensure!(
                                due.is_some_and(|d| d > self.h),
                                "pending_at_horizon without a committed due after H"
                            );
                            self.write_sealing(m, index, "pending_at_horizon", due)?;
                        }
                    }
                    SchedulerTerminal::NoObject => {
                        if !written {
                            self.write_sealing(m, index, "no_object", None)?;
                        }
                    }
                }
            }
        }
        Ok(requests)
    }

    /// B1/S1 seal: every slot without a terminal row gets one with
    /// `t_us == H`.  An admitted S1 slot is `pending_at_horizon` iff its due
    /// (learned from an S1 action or `deadline`) is after H; otherwise it is
    /// an explicit `unreleased_at_horizon` drop.  Unadmitted slots are
    /// `no_object`.
    fn seal_plain(&mut self, deadline: &dyn Fn(u64) -> Option<u64>) -> Result<()> {
        ensure!(!self.sealed, "sealed twice");
        self.sealed = true;
        for m in Modality::ALL {
            for index in 0..self.slots[m.ix()].len() as u32 {
                if self.seal_switch_barrier(m, index)? {
                    continue;
                }
                let slot = self.slots[m.ix()][index as usize].clone();
                if slot.terminal {
                    continue;
                }
                let admitted = slot.admitted_gen.is_some()
                    && slot.t_recv_us.is_some_and(|t| t <= self.h);
                if self.method == Exp2Method::S1 && admitted {
                    let due = slot
                        .s1_due_us
                        .or_else(|| deadline(self.pts[m.ix()][index as usize]));
                    if due.is_some_and(|d| d > self.h) {
                        self.write_sealing(m, index, "pending_at_horizon", due)?;
                    } else {
                        self.write_seal_drop(m, index, S1_UNRELEASED_AT_HORIZON, due, Map::new())?;
                    }
                } else {
                    self.write_sealing(m, index, "no_object", None)?;
                }
            }
        }
        Ok(())
    }

    // ------------------------------------------------------------ S1 hooks

    fn s1_slot(&mut self, header: &Header) -> Option<(Modality, u32)> {
        let m = modality_of(header.track_id)?;
        let index = *self.by_pts[m.ix()].get(&header.pts_us)?;
        let slot = &self.slots[m.ix()][index as usize];
        (slot.admitted_gen.is_some() && slot.seq == Some(header.seq)).then_some((m, index))
    }

    /// An S1 release action (time = the action time the S1 path logged).
    pub fn s1_release(&mut self, header: &Header, t_release_us: u64, due_us: Option<u64>) -> Result<()> {
        self.s1_terminal(header, t_release_us, due_us, None)
    }

    /// An S1 drop action with its S1 reason (passed through verbatim).
    pub fn s1_drop(&mut self, header: &Header, at_us: u64, reason: &str, due_us: Option<u64>) -> Result<()> {
        self.s1_terminal(header, at_us, due_us, Some(reason))
    }

    fn s1_terminal(&mut self, header: &Header, at_us: u64, due_us: Option<u64>, drop_reason: Option<&str>) -> Result<()> {
        if self.ended || is_warmup_seq(header.seq) {
            return Ok(());
        }
        let Some((m, index)) = self.s1_slot(header) else {
            // Orphan / unadmitted copies already carry their rx diagnostics.
            return Ok(());
        };
        if self.sealed || at_us > self.h {
            // The ledger is the state as of H: a later S1 action is a
            // diagnostic only (its due still informs the seal if unsealed).
            if !self.sealed && due_us.is_some() {
                self.slots[m.ix()][index as usize].s1_due_us = due_us;
            }
            self.stats.post_horizon_terminals_suppressed += 1;
            return Ok(());
        }
        if due_us.is_some() {
            self.slots[m.ix()][index as usize].s1_due_us = due_us;
        }
        if self.slots[m.ix()][index as usize].terminal {
            return Ok(());
        }
        match drop_reason {
            None => {
                let mut row = self.slot_fields(m, index);
                row.insert("role".into(), json!("release"));
                row.insert("t_us".into(), json!(at_us));
                row.insert("t_release_us".into(), json!(at_us));
                if let Some(t) = self.slots[m.ix()][index as usize].t_recv_us {
                    row.insert("t_recv_us".into(), json!(t));
                }
                self.slots[m.ix()][index as usize].terminal = true;
                self.write_value(row, true)
            }
            Some(reason) => {
                // S1 has no Δ_eff: only the due is recorded (write_drop's
                // committed carries due_us; the S1 drop row omits Δ).
                let mut row = self.slot_fields(m, index);
                row.insert("role".into(), json!("drop"));
                row.insert("reason".into(), json!(reason));
                row.insert("t_us".into(), json!(at_us));
                if let Some(t) = self.slots[m.ix()][index as usize].t_recv_us {
                    row.insert("t_recv_us".into(), json!(t));
                }
                if let Some(d) = due_us {
                    row.insert("due_us".into(), json!(d));
                }
                self.slots[m.ix()][index as usize].terminal = true;
                self.write_value(row, true)
            }
        }
    }

    /// When the S1 scheduler task seals: `H + PLAIN_SEAL_EXECUTION_DELAY_US`
    /// (Codex 85), the same execution delay as B1, so an object stamped
    /// `t_recv <= H` that reaches the recorder just after H is still admitted
    /// before the seal. S1 actions with `at_us > H` stay suppressed.
    pub fn s1_seal_at_us(&self) -> u64 {
        self.h + PLAIN_SEAL_EXECUTION_DELAY_US
    }

    /// Called by the S1 scheduler task (which owns every admitted object's
    /// due) at each wake, before it dispatches anything; seals once
    /// `now >= s1_seal_at_us()`.  Returns whether this call sealed.
    pub fn seal_s1_if_due(&mut self, now_us: u64, deadline: &dyn Fn(u64) -> Option<u64>) -> Result<bool> {
        if self.sealed || self.ended || now_us < self.s1_seal_at_us() {
            return Ok(false);
        }
        self.seal_s1(deadline)?;
        Ok(true)
    }

    /// Seal unconditionally (tests and `seal_s1_if_due`).
    pub fn seal_s1(&mut self, deadline: &dyn Fn(u64) -> Option<u64>) -> Result<()> {
        if self.sealed || self.ended {
            return Ok(());
        }
        ensure!(self.method == Exp2Method::S1, "seal_s1 on a non-S1 recorder");
        self.seal_plain(deadline)
    }

    /// The S1 task has exited; the housekeeping tick may now seal.
    pub fn s1_finished(&mut self) {
        self.s1_finished = true;
    }

    // ------------------------------------------------------------ P1 rows

    /// `tier_request` row for one FSM request and what the WP3 adapter did.
    pub fn log_tier_request(&mut self, request: &TierRequest, outcome: &str, detail: Option<&str>) -> Result<()> {
        self.stats.tier_requests += 1;
        let mut row = Map::new();
        row.insert("role".into(), json!("tier_request"));
        row.insert("t_us".into(), json!((self.clock)()));
        row.insert("decision_at_us".into(), json!(request.at_us));
        row.insert("event".into(), json!(request.event));
        row.insert("from".into(), json!(request.from.as_str()));
        row.insert("to".into(), json!(request.to.as_str()));
        row.insert("cause".into(), json!(request.cause.as_str()));
        row.insert("pc_tier".into(), json!(request.pc_tier));
        row.insert("haptic_mode".into(), json!(haptic_mode_str(request.haptic_mode)));
        row.insert("window_misses".into(), json!(request.window_misses));
        row.insert("outcome".into(), json!(outcome));
        if let Some(d) = detail {
            row.insert("detail".into(), json!(d));
        }
        self.write_value(row, false)
    }

    /// A public `integrity_warning` row (diagnostic, never invalidating).
    pub fn warn(&mut self, kind: &str, fields: Value) -> Result<()> {
        let fields = match fields {
            Value::Object(map) => map,
            _ => Map::new(),
        };
        self.integrity_warning(kind, fields)
    }

    /// Import a sender/relay cumulative timeout counter sample (contract v4
    /// §9 `wire` row) into this run's JSONL. The sample must be exactly a
    /// counter row: role `wire`, source `sender` | `relay`, kind
    /// `delivery_timeout_counter`, this run's `run_id`, an integer
    /// `delivery_timeout_count` (Codex 85). A malformed sample is NOT
    /// imported: an `integrity_warning` names it and the counter stays absent
    /// (analyzer: null). `t_us` is this write; the producer's sample time is
    /// kept as `t_sample_us`. Returns whether the sample was imported.
    pub fn import_wire_counter(&mut self, sample: &Map<String, Value>, imported_from: &str) -> Result<bool> {
        let source = sample.get("source").and_then(Value::as_str).unwrap_or_default();
        let count = sample.get("delivery_timeout_count").and_then(Value::as_u64);
        let problem = if sample.get("role").and_then(Value::as_str) != Some("wire") {
            Some("role is not wire")
        } else if !matches!(source, "sender" | "relay") {
            Some("source is not sender or relay")
        } else if sample.get("kind").and_then(Value::as_str) != Some(WIRE_COUNTER_KIND) {
            Some("kind is not delivery_timeout_counter")
        } else if sample.get("run_id").and_then(Value::as_str) != Some(self.run_id.as_str()) {
            Some("run_id differs from this run")
        } else if count.is_none() {
            Some("delivery_timeout_count is not a non-negative integer")
        } else {
            None
        };
        if let Some(problem) = problem {
            self.warn(
                "wire_counter_malformed",
                json!({"imported_from": imported_from, "problem": problem}),
            )?;
            return Ok(false);
        }
        let mut row = Map::new();
        for (key, value) in sample {
            if !matches!(key.as_str(), "run_id" | "t_us") {
                row.insert(key.clone(), value.clone());
            }
        }
        row.insert("t_sample_us".into(), sample.get("t_us").cloned().unwrap_or(Value::Null));
        row.insert("t_us".into(), json!((self.clock)()));
        row.insert("imported_from".into(), json!(imported_from));
        self.write_value(row, false)?;
        Ok(true)
    }

    /// `tier_switch` row: a requested switch took effect (make-before-break
    /// exact-pair first effect) or was abandoned.
    pub fn log_tier_switch(&mut self, fields: Map<String, Value>) -> Result<()> {
        self.stats.tier_switches += 1;
        let mut row = fields;
        row.insert("role".into(), json!("tier_switch"));
        row.insert("t_us".into(), json!((self.clock)()));
        self.write_value(row, false)
    }

    // ------------------------------------------------------------ end

    /// Write the closing `wire` summary and the `instrumentation_end`
    /// sentinel, then refuse every further row.  Only at `now >= H +
    /// G_report`, after the seal, and only if no exp2 write failed (a failed
    /// write leaves the run without a sentinel, i.e. instrumentation-invalid).
    /// Returns whether the sentinel was written.
    pub fn finish(&mut self, now_us: u64, rss_bytes: Option<u64>) -> Result<bool> {
        if self.ended {
            return Ok(true);
        }
        if !self.sealed || now_us < self.end_at || self.write_failed {
            return Ok(false);
        }
        let s = self.stats.clone();
        let occupancy = self
            .engine
            .core()
            .map(|c| [c.max_occupancy(Modality::Pc), c.max_occupancy(Modality::Haptic)]);
        let mut wire = Map::new();
        wire.insert("role".into(), json!("wire"));
        wire.insert("source".into(), json!("receiver"));
        wire.insert("kind".into(), json!("receiver_summary"));
        wire.insert("t_us".into(), json!((self.clock)()));
        // Receiver-side service after the committed due: defined only where a
        // WP1 core commits dues (B1 has no due; S1 dues are not committed
        // before arrival), null otherwise.
        let has_core = self.engine.core().is_some();
        let core_value = |v: u64| if has_core { json!(v) } else { Value::Null };
        wire.insert("service_after_due_bytes".into(), core_value(s.service_after_due_bytes));
        wire.insert("service_after_due_us".into(), core_value(s.service_after_due_us));
        wire.insert("service_after_due_objects".into(), core_value(s.service_after_due_objects));
        wire.insert("delivery_timeout_count".into(), json!(s.delivery_timeout_observations));
        wire.insert("delivery_timeout_source".into(), json!("receiver_observed"));
        wire.insert(
            "max_buffer_occupancy".into(),
            match occupancy {
                Some([pc, hap]) => json!({"pc": pc, "haptic": hap}),
                None => Value::Null,
            },
        );
        wire.insert("receiver_rss_bytes".into(), json!(rss_bytes));
        self.write_value(wire, false)?;
        if self.write_failed {
            return Ok(false);
        }
        let end = (self.clock)().max(now_us);
        let mut row = Map::new();
        row.insert("role".into(), json!("instrumentation_end"));
        row.insert("t_us".into(), json!(end));
        row.insert("end_us".into(), json!(end));
        // Contract v4 §9: max write lag over every row written before this one.
        row.insert("max_write_lag_us".into(), json!(self.max_write_lag_us));
        row.insert(
            "counters".into(),
            json!({
                "rx_rows": s.rx_rows,
                "warmup_ignored": s.warmup_ignored,
                "orphan_rx": s.orphan_rx,
                "event_id_mismatch": s.event_id_mismatch,
                "route_copy_discards": s.route_copy_discards,
                "ingress_drops": s.ingress_drops,
                "post_horizon_arrivals": s.post_horizon_arrivals,
                "post_horizon_terminals_suppressed": s.post_horizon_terminals_suppressed,
                "integrity_warnings": s.integrity_warnings,
                "clamped_core_inputs": s.clamped_core_inputs,
                "core_misses": s.core_misses,
                "delta_updates_miss_step": s.delta_updates_miss_step,
                "delta_updates_q_increase": s.delta_updates_q_increase,
                "delta_updates_q_decrease": s.delta_updates_q_decrease,
                "core_diagnostics": s.core_diagnostics,
                "tier_requests": s.tier_requests,
                "tier_switches": s.tier_switches,
            }),
        );
        self.write_value(row, false)?;
        self.ended = true;
        Ok(true)
    }
}

// --------------------------------------------------------------------------
// Contract v4 §9: sender-side delivery-timeout counter (`wire`, source sender)
// --------------------------------------------------------------------------

/// Contract v4 §9 measurement-scope rule for delivery-timeout counts
/// (Codex 85): an expiry counts iff the expired object belongs to the 40 s
/// measurement, i.e. it was CREATED at or after t0. Warm-up objects are all
/// generated before t0 (the registered warm-up window is [t0 − 3 s, t0)), so
/// a warm-up object that times out after t0 (e.g. created t0 − 30 ms,
/// expiring t0 + 315 ms) is excluded. The rule is about the object, never
/// about when the timeout fired.
///
/// At the SENDER the hop's `received_at` is the object's creation instant,
/// so this predicate is exact. At a RELAY `received_at` is the relay's
/// receipt, which can be after t0 for a backlogged warm-up object: the WP4
/// relay aggregation must decide scope by the object identity instead
/// (warm-up seq flag bit 31 of the sender's object, joined through the relay
/// trace's group/object identity), with the same "warm-up objects never
/// count" outcome.
pub fn delivery_timeout_in_measurement_scope(object_created_us: u64, t0_us: u64) -> bool {
    object_created_us >= t0_us
}

/// Counts DELIVERY_TIMEOUT expiries at this process's forwarding hop through
/// moq-transport's object-boundary hook (`ForwardTimeout` is emitted on both
/// timeout paths of `Subscribed::serve_subgroup*`: the stream that never
/// opened and the stream reset mid-object). Lock-free, allocation-free.
/// Nothing is counted until t0 is known (`set_t0`); expiries of objects
/// created before t0 (warm-up) are counted separately as excluded.
#[derive(Debug)]
pub struct DeliveryTimeoutCounter {
    t0_us: std::sync::atomic::AtomicU64,
    pc: std::sync::atomic::AtomicU64,
    haptic: std::sync::atomic::AtomicU64,
    other: std::sync::atomic::AtomicU64,
    excluded_warmup: std::sync::atomic::AtomicU64,
}

impl Default for DeliveryTimeoutCounter {
    fn default() -> Self {
        Self {
            t0_us: std::sync::atomic::AtomicU64::new(u64::MAX),
            pc: Default::default(),
            haptic: Default::default(),
            other: Default::default(),
            excluded_warmup: Default::default(),
        }
    }
}

impl DeliveryTimeoutCounter {
    pub fn set_t0(&self, t0_us: u64) {
        self.t0_us.store(t0_us, std::sync::atomic::Ordering::Release);
    }

    /// Count one expiry of an object created at `created_us` on a wire track
    /// (`pc*` / `haptic*` route names), subject to the measurement scope.
    pub fn note(&self, track_name: &[u8], created_us: u64) {
        use std::sync::atomic::Ordering::{Acquire, Relaxed};
        if !delivery_timeout_in_measurement_scope(created_us, self.t0_us.load(Acquire)) {
            self.excluded_warmup.fetch_add(1, Relaxed);
            return;
        }
        if track_name.starts_with(b"pc") {
            self.pc.fetch_add(1, Relaxed);
        } else if track_name.starts_with(b"haptic") {
            self.haptic.fetch_add(1, Relaxed);
        } else {
            self.other.fetch_add(1, Relaxed);
        }
    }

    /// `(total, pc, haptic, other)` of in-scope expiries.
    pub fn snapshot(&self) -> (u64, u64, u64, u64) {
        use std::sync::atomic::Ordering::Relaxed;
        let (pc, haptic, other) = (self.pc.load(Relaxed), self.haptic.load(Relaxed), self.other.load(Relaxed));
        (pc + haptic + other, pc, haptic, other)
    }

    /// Expiries of objects created before t0 (warm-up), not counted.
    pub fn excluded_warmup(&self) -> u64 {
        self.excluded_warmup.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl moq_transport::object_trace::Observer for DeliveryTimeoutCounter {
    fn now_us(&self) -> u64 {
        crate::now_us()
    }

    fn record(
        &self,
        boundary: moq_transport::object_trace::Boundary,
        object: &std::sync::Arc<moq_transport::serve::SubgroupObject>,
        _track_alias: u64,
        timestamp_us: u64,
    ) {
        if boundary == moq_transport::object_trace::Boundary::ForwardTimeout {
            // The object's creation instant on this host's monotonic µs
            // clock: the observation time minus the object's age.
            let age_us = tokio::time::Instant::now()
                .saturating_duration_since(object.received_at)
                .as_micros() as u64;
            self.note(object.group.track.name.as_bytes(), timestamp_us.saturating_sub(age_us));
        }
    }
}

/// The sender's exp2 `wire` sidecar: cumulative `delivery_timeout_count`
/// samples in the contract v4 row format (role wire, source sender, kind
/// delivery_timeout_counter). The receiver copies the latest sample into
/// the run's exp2 JSONL before its sentinel. Create-only.
pub struct SenderWireSidecar {
    file: std::sync::Mutex<std::fs::File>,
    run_id: String,
    counter: Option<std::sync::Arc<DeliveryTimeoutCounter>>,
}

impl SenderWireSidecar {
    /// `counter` is `None` when the arm applies no delivery timeout (B1/S1):
    /// every sample is then 0 with `delivery_timeout_configured: false`.
    pub fn create(
        path: &std::path::Path,
        run_id: &str,
        counter: Option<std::sync::Arc<DeliveryTimeoutCounter>>,
    ) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .with_context(|| format!("create sender wire sidecar {} (create-only)", path.display()))?;
        Ok(Self { file: std::sync::Mutex::new(file), run_id: run_id.to_string(), counter })
    }

    /// Start the measurement scope (objects created at or after t0 count).
    pub fn set_t0(&self, t0_us: u64) {
        if let Some(counter) = &self.counter {
            counter.set_t0(t0_us);
        }
    }

    /// One cumulative sample, labelled with when it was taken
    /// (`t0` / `horizon` / `shutdown`).
    pub fn sample(&self, at: &str, t_us: u64) -> Result<()> {
        let (total, pc, haptic, other) = self
            .counter
            .as_ref()
            .map(|c| c.snapshot())
            .unwrap_or((0, 0, 0, 0));
        let row = json!({
            "role": "wire", "source": "sender", "kind": WIRE_COUNTER_KIND,
            "run_id": self.run_id, "t_us": t_us, "sample": at,
            "delivery_timeout_count": total,
            "by_track": {"pc": pc, "haptic": haptic, "other": other},
            "delivery_timeout_configured": self.counter.is_some(),
            "warmup_timeouts_excluded": self.counter.as_ref().map(|c| c.excluded_warmup()).unwrap_or(0),
            "scope_rule": "object created at or after t0 (warm-up objects excluded)",
        });
        let mut line = serde_json::to_vec(&row)?;
        line.push(b'\n');
        let mut file = self.file.lock().map_err(|_| anyhow!("sender wire sidecar poisoned"))?;
        file.write_all(&line).and_then(|_| file.flush()).context("write sender wire sample")
    }
}

/// Resident set size of this process (bytes), from `/proc/self/statm`.
pub fn current_rss_bytes() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages: u64 = text.split_whitespace().nth(1)?.parse().ok()?;
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    (page > 0).then(|| pages * page as u64)
}

// --------------------------------------------------------------------------
// Tests: WP3 serialisation rules and integration gates that are deterministic
// at the recorder boundary (gate 6, gate 7, single terminal, H boundary,
// post-H, same-µs order, route copies, B1/S1 sealing).
// --------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    const T0: u64 = 50_000_000;
    const G_report_R_US_FOR_TEST: u64 = G_REPORT_R_US;
    const DMAX: u64 = 345_000; // provisional slot value, test input only

    #[derive(Clone, Default)]
    struct Sink(Arc<Mutex<Vec<u8>>>);

    impl Write for Sink {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl Sink {
        fn rows(&self) -> Vec<Value> {
            let bytes = self.0.lock().unwrap().clone();
            String::from_utf8(bytes)
                .unwrap()
                .lines()
                .filter(|l| !l.trim().is_empty())
                .map(|l| serde_json::from_str(l).unwrap())
                .collect()
        }
    }

    struct Rig {
        rec: Exp2Recorder<Sink>,
        sink: Sink,
        clock: Arc<AtomicU64>,
    }

    impl Rig {
        /// The injected clock is monotone like the real one.
        fn set(&self, t: u64) {
            self.clock.fetch_max(t, Ordering::SeqCst);
        }
    }

    fn ledger(t0: u64) -> Exp2OpportunityLedger {
        Exp2OpportunityLedger {
            t0_us: t0,
            pc_pts_us: (0..N_PC as u64).map(|i| pc_slot_identity(i).0).collect(),
            haptic_pts_us: (0..N_HAPTIC as u64).map(|k| haptic_slot_identity(k).0).collect(),
        }
    }

    fn rig(method: Exp2Method) -> Rig {
        let sink = Sink::default();
        let clock = Arc::new(AtomicU64::new(T0 - 1_000_000));
        let c = clock.clone();
        let mut rec = Exp2Recorder::new(
            sink.clone(),
            // monotone and ticking 1 µs per read, like the real clock
            Box::new(move || c.fetch_add(1, Ordering::SeqCst)),
            Exp2RecorderConfig {
                method,
                run_id: "run-1".into(),
                ledger: ledger(T0),
                delta_max_us: DMAX,
                g_report_us: G_REPORT_R_US,
            },
        )
        .unwrap();
        let mut meta = Map::new();
        meta.insert("role".into(), json!("meta"));
        meta.insert("run_id".into(), json!("run-1"));
        meta.insert("method".into(), json!(method.label()));
        meta.insert("t0_us".into(), json!(T0));
        rec.write_meta(meta).unwrap();
        Rig { rec, sink, clock }
    }

    fn header(m: Modality, index: u32, t_gen: u64) -> Header {
        let (pts, event_id) = match m {
            Modality::Pc => pc_slot_identity(index as u64),
            Modality::Haptic => haptic_slot_identity(index as u64),
        };
        Header {
            version: crate::VERSION,
            track_id: if m == Modality::Pc { TRACK_PC } else { TRACK_HAPTIC },
            tier: if m == Modality::Pc { 2 } else { 0 },
            seq: index,
            pts_us: pts,
            event_id,
            gen_ts_us: t_gen,
            payload_len: 100,
        }
    }

    fn slot_ref(m: Modality, index: u32) -> u64 {
        T0 + match m {
            Modality::Pc => pc_slot_identity(index as u64).0,
            Modality::Haptic => haptic_slot_identity(index as u64).0,
        }
    }

    /// Drive a full run: every slot (unless `lost`) arrives at
    /// `ref + delay(m, index)` on route generation `gen(m, index)`, the
    /// recorder is ticked at every arrival and every core wakeup, and the run
    /// is closed at `H + G_report`.  Returns all written rows.
    fn drive(
        rig: &mut Rig,
        delay: impl Fn(Modality, u32) -> Option<u64>,
        gen: impl Fn(Modality, u32) -> u64,
    ) -> Vec<Value> {
        let mut arrivals: Vec<(u64, Modality, u32)> = Vec::new();
        for m in Modality::ALL {
            let n = if m == Modality::Pc { N_PC } else { N_HAPTIC };
            for index in 0..n as u32 {
                if let Some(d) = delay(m, index) {
                    arrivals.push((slot_ref(m, index) + d, m, index));
                }
            }
        }
        arrivals.sort();
        let mut next = 0;
        let end = rig.rec.end_at_us();
        loop {
            let wake = rig.rec.next_wakeup_us();
            let arrival_t = arrivals.get(next).map(|a| a.0);
            let t = match (wake, arrival_t) {
                (Some(w), Some(a)) => w.min(a),
                (Some(w), None) => w,
                (None, Some(a)) => a,
                (None, None) => break,
            };
            rig.set(t);
            // arrivals at t first (tie rule), then the tick
            while next < arrivals.len() && arrivals[next].0 == t {
                let (ta, m, index) = arrivals[next];
                next += 1;
                let rx = Exp2Rx {
                    header: header(m, index, slot_ref(m, index)),
                    route_generation: gen(m, index),
                    t_recv_us: ta,
                };
                if let Admission::Admitted(mm, ii) = rig.rec.admit(rx).unwrap() {
                    if rig.rec.method().core_mode().is_some() {
                        rig.rec.feed_arrival(mm, ii, ta).unwrap();
                    }
                }
            }
            rig.rec.advance(t).unwrap();
            if rig.rec.is_sealed() && t >= end {
                assert!(rig.rec.finish(t, Some(1)).unwrap());
                break;
            }
        }
        rig.sink.rows()
    }

    const TERMINAL_ROLES: [&str; 4] = ["release", "drop", "pending_at_horizon", "no_object"];

    /// Every planned slot carries exactly one terminal row (contract §5
    /// ledger completeness); route copies are not terminals.
    fn assert_single_terminal(rows: &[Value]) -> HashMap<(String, u64), Value> {
        let mut terminals: HashMap<(String, u64), Value> = HashMap::new();
        for row in rows {
            let role = row["role"].as_str().unwrap();
            if !TERMINAL_ROLES.contains(&role) || row["reason"] == json!(ROUTE_COPY_DISCARD) {
                continue;
            }
            let key = (
                row["track"].as_str().unwrap().to_string(),
                row["pts_us"].as_u64().unwrap(),
            );
            assert!(terminals.insert(key.clone(), row.clone()).is_none(), "two terminals for {key:?}");
        }
        assert_eq!(terminals.len(), N_PC + N_HAPTIC, "every planned slot has a terminal");
        terminals
    }

    fn assert_contract_shape(rows: &[Value]) {
        let h = horizon_us(T0);
        assert_eq!(rows[0]["role"], json!("meta"));
        let last = rows.last().unwrap();
        assert_eq!(last["role"], json!("instrumentation_end"));
        assert_eq!(last["end_us"], last["t_us"]);
        assert!(last["end_us"].as_u64().unwrap() >= h + G_REPORT_R_US);
        assert!(last["max_write_lag_us"].is_u64(), "v4 write lag on the sentinel");
        for row in rows {
            assert!(row["t_us"].is_u64(), "integer µs t_us: {row}");
            assert_eq!(row["run_id"], json!("run-1"));
            let role = row["role"].as_str().unwrap();
            if matches!(role, "pending_at_horizon" | "no_object") || row.get("seal_horizon_us").is_some() {
                // contract v4 §9: actual write time inside [H, H + G_report)
                assert_eq!(row["seal_horizon_us"].as_u64().unwrap(), h, "seal_horizon_us == H");
                let t = row["t_us"].as_u64().unwrap();
                assert!(t >= h && t < h + G_report_R_US_FOR_TEST, "sealing row written in the window");
            }
            if role == "release" {
                assert!(row.get("due_us").is_none(), "due stays in decision rows only");
                assert_eq!(row["t_us"], row["t_release_us"]);
                assert!(row["t_log_arrival_us"].as_u64().unwrap() >= row["t_release_us"].as_u64().unwrap());
            }
            if role == "wire" {
                assert!(row["source"].is_string(), "v4 wire rows carry source");
            }
            if role == "controller" {
                assert!(matches!(row["modality"].as_str(), Some("pc" | "haptic" | "shared")));
                assert!(matches!(
                    row["event"].as_str(),
                    Some("q_update" | "miss_step" | "delta_star" | "delta_eff_commit")
                ));
                assert!(row["value_us"].is_u64() || row["value_us"].is_null());
            }
            if role == "drop" {
                assert!(row["reason"].is_string());
                assert!(row["t_log_arrival_us"].is_u64());
            }
            // the role is the JSONL vocabulary, never the core's terminal name
            assert!(!matches!(role, "released" | "dropped"));
        }
        assert_eq!(rows.iter().filter(|r| r["role"] == json!("instrumentation_end")).count(), 1);
        assert_eq!(rows.iter().filter(|r| r["role"] == json!("workload_started")).count(), 1);
    }

    fn lossy(seed: u64) -> impl Fn(Modality, u32) -> Option<u64> {
        move |m, index| {
            let x = (index as u64 + 1)
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .wrapping_add(seed + m.ix() as u64 * 7);
            let r = (x >> 33) % 100;
            if r < 5 {
                None // lost
            } else if r < 10 {
                Some(600_000) // after due + ε: late
            } else {
                Some(20_000 + (r * 1_500))
            }
        }
    }

    #[test]
    fn exp2_recorder_single_terminal_and_contract_shape_every_method() {
        for method in [
            Exp2Method::B1,
            Exp2Method::S3npaPrime,
            Exp2Method::P0np,
            Exp2Method::P0,
            Exp2Method::P1,
        ] {
            let mut r = rig(method);
            let rows = drive(&mut r, lossy(3), |_, _| 0);
            assert_contract_shape(&rows);
            let terminals = assert_single_terminal(&rows);
            if method.core_mode().is_some() {
                // decision rows for all 4800 slots, immutable (one per slot)
                let decisions = rows.iter().filter(|r| r["role"] == json!("decision")).count();
                assert_eq!(decisions, N_PC + N_HAPTIC, "{method:?}");
                assert!(terminals.values().any(|r| r["reason"] == json!("late")));
            } else {
                assert!(!rows.iter().any(|r| r["role"] == json!("decision")));
                // B1 release := t_recv
                for row in terminals.values().filter(|r| r["role"] == json!("release")) {
                    assert_eq!(row["t_release_us"], row["t_recv_us"]);
                }
            }
            assert!(terminals.values().any(|r| r["role"] == json!("no_object")), "{method:?}");
        }
    }

    #[test]
    fn exp2_recorder_delivery_timeout_then_arrival_keeps_one_terminal() {
        let mut r = rig(Exp2Method::P0);
        // Slot pc[10]: timeout observed, then the object still arrives in time
        // -> released, no delivery_timeout drop.  Slot pc[20]: timeout and no
        // object -> drop(delivery_timeout) at t_us == H with the observation
        // time, plus the wire row written at observation.
        let t10 = slot_ref(Modality::Pc, 10);
        let t20 = slot_ref(Modality::Pc, 20);
        r.set(t10 + 5_000);
        r.rec.advance(t10 + 5_000).unwrap();
        r.rec.delivery_timeout(Modality::Pc, 10, t10 + 5_000).unwrap();
        r.set(t20 + 1_000);
        r.rec.advance(t20 + 1_000).unwrap();
        r.rec.delivery_timeout(Modality::Pc, 20, t20 + 1_000).unwrap();
        let rows = drive(&mut r, |m, i| (!(m == Modality::Pc && i == 20)).then_some(30_000), |_, _| 0);
        assert_contract_shape(&rows);
        let terminals = assert_single_terminal(&rows);
        let pc10 = &terminals[&("pc".to_string(), pc_slot_identity(10).0)];
        assert_eq!(pc10["role"], json!("release"));
        let pc20 = &terminals[&("pc".to_string(), pc_slot_identity(20).0)];
        assert_eq!(pc20["role"], json!("drop"));
        assert_eq!(pc20["reason"], json!("delivery_timeout"));
        assert_eq!(pc20["seal_horizon_us"].as_u64().unwrap(), horizon_us(T0));
        assert!(pc20["t_us"].as_u64().unwrap() >= horizon_us(T0));
        assert_eq!(pc20["t_observed_us"].as_u64().unwrap(), t20 + 1_000);
        let wire: Vec<_> = rows
            .iter()
            .filter(|r| r["role"] == json!("wire") && r["kind"] == json!("delivery_timeout_observed"))
            .collect();
        assert_eq!(wire.len(), 2);
        assert_eq!(wire[1]["t_observed_us"].as_u64().unwrap(), t20 + 1_000);
    }

    #[test]
    fn exp2_recorder_same_us_arrivals_and_out_of_order_inputs() {
        let mut r = rig(Exp2Method::P0);
        let t = slot_ref(Modality::Pc, 5) + 40_000;
        r.set(t);
        r.rec.advance(t - 1).unwrap();
        // PC and haptic anchor of event 5 in the same µs: both fed unclamped.
        for (m, index) in [(Modality::Pc, 5), (Modality::Haptic, 15)] {
            let rx = Exp2Rx { header: header(m, index, slot_ref(m, index)), route_generation: 0, t_recv_us: t };
            let Admission::Admitted(mm, ii) = r.rec.admit(rx).unwrap() else { panic!() };
            r.rec.feed_arrival(mm, ii, t).unwrap();
        }
        assert_eq!(r.rec.stats().clamped_core_inputs, 0);
        // An arrival stamped before the last advance is fed at drained + 1.
        r.rec.advance(t + 10).unwrap();
        let late_stamp = t + 3;
        let rx = Exp2Rx { header: header(Modality::Haptic, 16, slot_ref(Modality::Haptic, 16)), route_generation: 0, t_recv_us: late_stamp };
        let Admission::Admitted(mm, ii) = r.rec.admit(rx).unwrap() else { panic!() };
        r.rec.feed_arrival(mm, ii, late_stamp).unwrap();
        assert_eq!(r.rec.stats().clamped_core_inputs, 1);
        let rows = drive(&mut r, |m, i| (!matches!((m, i), (Modality::Pc, 5) | (Modality::Haptic, 15) | (Modality::Haptic, 16))).then_some(30_000), |_, _| 0);
        let terminals = assert_single_terminal(&rows);
        let h16 = &terminals[&("haptic".to_string(), haptic_slot_identity(16).0)];
        assert_eq!(h16["role"], json!("release"));
        assert_eq!(h16["t_recv_us"].as_u64().unwrap(), late_stamp);
        assert_eq!(h16["t_core_in_us"].as_u64().unwrap(), t + 11);
    }

    #[test]
    fn exp2_recorder_h_boundary_and_post_h_reports_are_not_terminals() {
        let h = horizon_us(T0);
        let mut r = rig(Exp2Method::S3npaPrime);
        // Last PC slot: due = ref + Δmax.  Arrive at exactly H (late: after
        // due + ε) and the second-to-last at H + 1 (post-H, never fed).
        let last = (N_PC - 1) as u32;
        let rows = drive(
            &mut r,
            |m, i| match (m, i) {
                (Modality::Pc, x) if x == last => Some(h - slot_ref(Modality::Pc, last)),
                (Modality::Pc, x) if x == last - 1 => Some(h + 1 - slot_ref(Modality::Pc, last - 1)),
                _ => Some(10_000),
            },
            |_, _| 0,
        );
        assert_contract_shape(&rows);
        let terminals = assert_single_terminal(&rows);
        let at_h = &terminals[&("pc".to_string(), pc_slot_identity(last as u64).0)];
        assert_eq!(at_h["role"], json!("drop"));
        assert_eq!(at_h["reason"], json!("late"));
        assert_eq!(at_h["t_us"].as_u64().unwrap(), h);
        let after_h = &terminals[&("pc".to_string(), pc_slot_identity(last as u64 - 1).0)];
        assert_eq!(after_h["role"], json!("no_object"));
        assert_eq!(after_h["seal_horizon_us"].as_u64().unwrap(), h);
        // the post-H object still has its rx row (reception), not a terminal
        assert!(rows.iter().any(|r| r["role"] == json!("rx") && r["pts_us"] == json!(pc_slot_identity(last as u64 - 1).0)));
        assert_eq!(r.rec.stats().post_horizon_arrivals, 1);
        // after the sentinel every row is refused
        let n = r.sink.rows().len();
        let rx = Exp2Rx { header: header(Modality::Pc, 0, T0), route_generation: 0, t_recv_us: h + 2_000_000 };
        assert_eq!(r.rec.admit(rx).unwrap(), Admission::NotAdmitted);
        assert_eq!(r.sink.rows().len(), n);
        assert_eq!(r.sink.rows().last().unwrap()["role"], json!("instrumentation_end"));
    }

    #[test]
    fn exp2_recorder_pending_at_horizon_from_the_core_seal() {
        // With registered constants Δ_max < B_play, so pending_at_horizon
        // cannot arise; a non-registered TEST input Δ = 450 ms (> B_play)
        // makes the last opportunities' due exceed H while their objects sit
        // buffered.  S3NPA' keeps Δ fixed, so exactly these slots qualify:
        // pc[1199] (ref 39.967 s) and haptic[3596..=3599] (event 1198/1199
        // anchors+fillers with pts > 39.95 s).
        let sink = Sink::default();
        let clock = Arc::new(AtomicU64::new(T0 - 1_000_000));
        let c = clock.clone();
        let mut rec = Exp2Recorder::new(
            sink.clone(),
            Box::new(move || c.fetch_add(1, Ordering::SeqCst)),
            Exp2RecorderConfig {
                method: Exp2Method::S3npaPrime,
                run_id: "run-1".into(),
                ledger: ledger(T0),
                delta_max_us: 450_000,
                g_report_us: G_REPORT_R_US,
            },
        )
        .unwrap();
        let mut meta = Map::new();
        meta.insert("role".into(), json!("meta"));
        meta.insert("run_id".into(), json!("run-1"));
        meta.insert("method".into(), json!("S3NPA'"));
        meta.insert("t0_us".into(), json!(T0));
        rec.write_meta(meta).unwrap();
        let mut r = Rig { rec, sink, clock };
        let rows = drive(&mut r, |_, _| Some(10_000), |_, _| 0);
        assert_contract_shape(&rows);
        let terminals = assert_single_terminal(&rows);
        let h = horizon_us(T0);
        let pending: Vec<_> = terminals
            .values()
            .filter(|r| r["role"] == json!("pending_at_horizon"))
            .collect();
        let mut keys: Vec<(String, u64)> = pending
            .iter()
            .map(|r| (r["track"].as_str().unwrap().to_string(), r["pts_us"].as_u64().unwrap()))
            .collect();
        keys.sort();
        let mut expected: Vec<(String, u64)> = vec![("pc".into(), pc_slot_identity(1199).0)];
        expected.extend((3596..3600).map(|k| ("haptic".to_string(), haptic_slot_identity(k).0)));
        expected.sort();
        assert_eq!(keys, expected);
        for row in pending {
            assert_eq!(row["seal_horizon_us"].as_u64().unwrap(), h, "seals H");
            assert!(row["due_us"].as_u64().unwrap() > h, "due after H");
            assert!(row["seq"].is_u64(), "the buffered object is identified");
        }
        // everything else was released at its due (<= H); nothing else sealed
        assert!(!terminals.values().any(|r| r["role"] == json!("no_object")));
    }

    #[test]
    fn exp2_recorder_route_copies_across_generations() {
        let mut r = rig(Exp2Method::P1);
        let t = slot_ref(Modality::Pc, 3) + 30_000;
        r.set(t);
        r.rec.advance(t - 1).unwrap();
        let rx0 = Exp2Rx { header: header(Modality::Pc, 3, slot_ref(Modality::Pc, 3)), route_generation: 0, t_recv_us: t };
        let Admission::Admitted(m, i) = r.rec.admit(rx0).unwrap() else { panic!() };
        r.rec.feed_arrival(m, i, t).unwrap();
        // the same slot on a newer route generation: a copy, not a terminal
        let rx1 = Exp2Rx { route_generation: 1, t_recv_us: t + 5, ..rx0 };
        assert_eq!(r.rec.admit(rx1).unwrap(), Admission::CopyDiscarded);
        // a transport barrier/stale discard of another slot's copy
        let rx2 = Exp2Rx { header: header(Modality::Haptic, 9, slot_ref(Modality::Haptic, 9)), route_generation: 1, t_recv_us: t + 6 };
        r.rec.discard_route_copy(rx2, "switch_barrier").unwrap();
        let rows = drive(&mut r, |m, i| (!(m == Modality::Pc && i == 3)).then_some(30_000), |m, i| if i > 600 && m == Modality::Pc { 1 } else { 0 });
        assert_contract_shape(&rows);
        let terminals = assert_single_terminal(&rows);
        let copies: Vec<_> = rows.iter().filter(|r| r["reason"] == json!(ROUTE_COPY_DISCARD)).collect();
        assert_eq!(copies.len(), 2);
        assert_eq!(copies[0]["route_generation"], json!(1));
        assert_eq!(copies[0]["admitted_route_generation"], json!(0));
        assert_eq!(copies[1]["transport_reason"], json!("switch_barrier"));
        // the admitted gen-0 copy is the terminal
        let pc3 = &terminals[&("pc".to_string(), pc_slot_identity(3).0)];
        assert_eq!(pc3["role"], json!("release"));
        assert_eq!(pc3["route_generation"], json!(0));
    }

    #[test]
    fn exp2_recorder_gate6_t0_constant_across_route_generations() {
        let mut r = rig(Exp2Method::P1);
        let t0 = r.rec.t0_us();
        // PC from generation 0 for the first half, generation 2 afterwards.
        let rows = drive(&mut r, |_, _| Some(25_000), |m, i| if m == Modality::Pc && i >= 600 { 2 } else { 0 });
        assert_eq!(r.rec.t0_us(), t0);
        for row in rows.iter().filter(|r| r["role"] == json!("decision")) {
            let pts = row["pts_us"].as_u64().unwrap();
            let due = row["due_us"].as_u64().unwrap();
            let delta = row["delta_eff_us"].as_u64().unwrap();
            assert_eq!(due, t0 + pts + delta, "due anchored on the single t0");
        }
        for row in rows.iter().filter(|r| r["role"] == json!("release")) {
            assert!(row["route_generation"].as_u64().unwrap() <= 2);
        }
        assert_single_terminal(&rows);
    }

    #[test]
    fn exp2_recorder_gate7_buffer_bound_and_shutdown() {
        let mut r = rig(Exp2Method::P0);
        // Haptic objects of the first 3.2 s all arrive at t0 (before their
        // commits): the 3 s span bound must drop the oldest buffered ones.
        r.set(T0);
        let span_slots: Vec<u32> = (0..(3_200_000 * 90 / 1_000_000) as u32).collect();
        for &k in &span_slots {
            let rx = Exp2Rx { header: header(Modality::Haptic, k, T0), route_generation: 0, t_recv_us: T0 };
            let Admission::Admitted(m, i) = r.rec.admit(rx).unwrap() else { panic!() };
            r.rec.feed_arrival(m, i, T0).unwrap();
        }
        let occupancy = r.rec.core().unwrap().max_occupancy(Modality::Haptic);
        assert!(occupancy <= 4_096);
        // shutdown before H + G_report writes nothing and reports false
        assert!(!r.rec.finish(T0 + 1, None).unwrap());
        let rows = drive(&mut r, |m, k| (!(m == Modality::Haptic && span_slots.contains(&k))).then_some(20_000), |_, _| 0);
        assert_contract_shape(&rows);
        let terminals = assert_single_terminal(&rows);
        let bound: Vec<_> = terminals.values().filter(|r| r["reason"] == json!("buffer_bound")).collect();
        assert!(!bound.is_empty(), "span bound drops");
        assert!(bound.iter().all(|r| r["detail"] == json!("span_limit")));
    }

    #[test]
    fn exp2_recorder_s1_hooks_and_seal() {
        let h = horizon_us(T0);
        let mut r = rig(Exp2Method::S1);
        r.set(T0 + 100);
        r.rec.advance(T0 + 100).unwrap();
        let admit = |r: &mut Rig, m, i, t| {
            let rx = Exp2Rx { header: header(m, i, slot_ref(m, i)), route_generation: 0, t_recv_us: t };
            assert!(matches!(r.rec.admit(rx).unwrap(), Admission::Admitted(..)));
        };
        admit(&mut r, Modality::Pc, 0, T0 + 1_000);
        admit(&mut r, Modality::Pc, 1, T0 + 40_000);
        admit(&mut r, Modality::Pc, 2, T0 + 70_000);
        admit(&mut r, Modality::Pc, 3, T0 + 100_000);
        r.set(T0 + 140_000);
        r.rec.s1_release(&header(Modality::Pc, 0, T0), T0 + 101_000, Some(T0 + 101_000)).unwrap();
        r.rec.s1_drop(&header(Modality::Pc, 1, T0), T0 + 140_000, "late", Some(T0 + 134_000)).unwrap();
        // pc[2] still buffered at the seal with due after H -> pending;
        // pc[3] buffered with a due before H -> explicit unreleased drop.
        r.set(h);
        r.rec.advance(h).unwrap();
        assert!(!r.rec.is_sealed(), "S1 is sealed by its scheduler task");
        let pts2 = pc_slot_identity(2).0;
        r.rec.seal_s1(&|pts| Some(if pts == pts2 { h + 5 } else { h - 5 })).unwrap();
        r.rec.s1_release(&header(Modality::Pc, 3, T0), h + 1, Some(h - 5)).unwrap();
        r.set(h + G_REPORT_R_US);
        r.rec.advance(h + G_REPORT_R_US).unwrap();
        assert!(r.rec.finish(h + G_REPORT_R_US, None).unwrap());
        let rows = r.sink.rows();
        assert_contract_shape(&rows);
        let terminals = assert_single_terminal(&rows);
        let get = |i: u64| &terminals[&("pc".to_string(), pc_slot_identity(i).0)];
        assert_eq!(get(0)["role"], json!("release"));
        assert_eq!(get(0)["t_release_us"].as_u64().unwrap(), T0 + 101_000);
        assert_eq!(get(1)["reason"], json!("late"));
        assert_eq!(get(2)["role"], json!("pending_at_horizon"));
        assert_eq!(get(2)["due_us"].as_u64().unwrap(), h + 5);
        assert_eq!(get(3)["reason"], json!(S1_UNRELEASED_AT_HORIZON));
        assert_eq!(get(4)["role"], json!("no_object"));
        assert_eq!(r.rec.stats().post_horizon_terminals_suppressed, 1);
    }

    #[test]
    fn exp2_recorder_s1_finished_before_h_is_sealed_by_housekeeping() {
        let h = horizon_us(T0);
        let mut r = rig(Exp2Method::S1);
        r.rec.s1_finished();
        r.set(h);
        r.rec.advance(h).unwrap();
        assert!(!r.rec.is_sealed());
        r.set(h + PLAIN_SEAL_EXECUTION_DELAY_US);
        r.rec.advance(h + PLAIN_SEAL_EXECUTION_DELAY_US).unwrap();
        assert!(r.rec.is_sealed());
        r.set(h + G_REPORT_R_US);
        assert!(r.rec.finish(h + G_REPORT_R_US, None).unwrap());
        let rows = r.sink.rows();
        assert_contract_shape(&rows);
        assert_single_terminal(&rows);
    }

    #[test]
    fn exp2_recorder_orphans_mismatches_and_warmup_are_not_slots() {
        let mut r = rig(Exp2Method::P0);
        r.set(T0 + 10);
        let mut bad = header(Modality::Pc, 4, T0);
        bad.event_id = 99;
        assert_eq!(r.rec.admit(Exp2Rx { header: bad, route_generation: 0, t_recv_us: T0 + 10 }).unwrap(), Admission::NotAdmitted);
        let mut orphan = header(Modality::Pc, 4, T0);
        orphan.pts_us += 1;
        assert_eq!(r.rec.admit(Exp2Rx { header: orphan, route_generation: 0, t_recv_us: T0 + 10 }).unwrap(), Admission::NotAdmitted);
        let mut warm = header(Modality::Pc, 4, T0);
        warm.seq |= crate::WARMUP_SEQ_FLAG;
        assert_eq!(r.rec.admit(Exp2Rx { header: warm, route_generation: 0, t_recv_us: T0 + 10 }).unwrap(), Admission::NotAdmitted);
        let s = r.rec.stats();
        assert_eq!((s.event_id_mismatch, s.orphan_rx, s.warmup_ignored), (1, 1, 1));
        let rows = r.sink.rows();
        assert_eq!(rows.iter().filter(|r| r["role"] == json!("rx")).count(), 2, "warm-up has no rx row");
        assert_eq!(rows.iter().filter(|r| r["role"] == json!("integrity_warning")).count(), 2);
    }

    #[test]
    fn exp2_recorder_tgen_ref_warning() {
        let mut r = rig(Exp2Method::B1);
        r.set(T0 + 20_000);
        let rx = Exp2Rx { header: header(Modality::Pc, 0, T0 + EPS_REF_US + 1), route_generation: 0, t_recv_us: T0 + 20_000 };
        r.rec.admit(rx).unwrap();
        let rows = r.sink.rows();
        let w: Vec<_> = rows.iter().filter(|r| r["kind"] == json!("tgen_ref_exceeds_eps_ref")).collect();
        assert_eq!(w.len(), 1);
        assert_eq!(w[0]["tgen_minus_ref_us"].as_u64().unwrap(), EPS_REF_US + 1);
    }

    fn ledger_file(run_id: &str, t0: u64) -> Vec<u8> {
        let pc: Vec<Value> = (0..N_PC as u64)
            .map(|i| {
                let (pts, e) = pc_slot_identity(i);
                json!({"i": i, "pts_us": pts, "event_id": e, "ref_us": t0 + pts})
            })
            .collect();
        let haptic: Vec<Value> = (0..N_HAPTIC as u64)
            .map(|k| {
                let (pts, e) = haptic_slot_identity(k);
                json!({"k": k, "pts_us": pts, "event_id": e,
                       "anchor_i": if k % 3 == 0 { json!(k / 3) } else { Value::Null }})
            })
            .collect();
        canonical_json_bytes(&json!({"contract": EXP2_CONTRACT_VERSION, "run_id": run_id,
                                     "t0_us": t0, "pc": pc, "haptic": haptic}))
    }

    #[test]
    fn exp2_recorder_opportunity_ledger_file_is_checked() {
        let bytes = ledger_file("run-1", T0);
        let (l, sha) = load_opportunities(&bytes, "run-1", T0).unwrap();
        assert_eq!(l, ledger(T0));
        assert_eq!(sha, sha256_hex(&bytes));
        let mut newline = bytes.clone();
        newline.push(b'\n');
        assert!(load_opportunities(&newline, "run-1", T0).is_err(), "non-canonical bytes");
        assert!(load_opportunities(&bytes, "run-2", T0).is_err(), "run_id");
        assert!(load_opportunities(&bytes, "run-1", T0 + 1).is_err(), "t0");
        let mut v: Value = serde_json::from_slice(&bytes).unwrap();
        v["pc"][7]["pts_us"] = json!(233_334);
        assert!(load_opportunities(&canonical_json_bytes(&v), "run-1", T0).is_err(), "pts rule");
        // Python json.dumps(sort_keys=True, separators=(",", ":")) agreement
        // on a small object (ints, strings, null).
        assert_eq!(
            canonical_json_bytes(&json!({"b": 1, "a": [null, "x"], "c": {"z": 2, "y": 3}})),
            br#"{"a":[null,"x"],"b":1,"c":{"y":3,"z":2}}"#
        );
    }

    fn runner_meta() -> Value {
        json!({"batch_id": "b", "campaign": "L1-R", "condition": "loopback", "block_id": "blk",
               "trajectory_seed": 1, "attempt": 1, "planned_predecessor": null,
               "actual_predecessor": null, "epsilon_output_us": 40000, "g_report_us": 1000000,
               "threshold_profile": "middle", "thresh_pos_ms": 77, "thresh_neg_ms": -118,
               "threshold_citation_id": "x", "threshold_verified": true})
    }

    fn own() -> ReceiverMeta<'static> {
        ReceiverMeta { run_id: "run-1", method: Exp2Method::P0np, topology: "direct",
                       representation: "bin", t0_us: T0, opportunities_sha256: "ab",
                       delta_max_us: DMAX, t_pc_ms: Some(345), c_mbps: 131.9, s_bytes: 1,
                       scientific_eligible: true, extra: Map::new() }
    }

    #[test]
    fn exp2_recorder_meta_merge_rules() {
        let (meta, g) = build_meta(&runner_meta(), &own()).unwrap();
        assert_eq!(g, G_REPORT_R_US);
        assert_eq!(meta["method"], json!("P0-NP"));
        assert_eq!(meta["contract"], json!(EXP2_CONTRACT_VERSION));
        assert_eq!(meta["metric_schema_version"], json!(EXP2_METRIC_SCHEMA_VERSION));
        assert_eq!(meta["t_pc_ms"], json!(345));
        assert_eq!(meta["scientific_eligible"], json!(true));
        let mut b1 = own();
        b1.method = Exp2Method::B1;
        b1.t_pc_ms = None;
        b1.scientific_eligible = false;
        b1.extra.insert("exp2_test_mode".into(), json!(true));
        let (meta_b1, _) = build_meta(&runner_meta(), &b1).unwrap();
        assert!(meta_b1["t_pc_ms"].is_null(), "B1/S1 t_pc_ms is null");
        assert_eq!(meta_b1["scientific_eligible"], json!(false));
        let mut foreign = own();
        foreign.extra.insert("campaign".into(), json!("x"));
        assert!(build_meta(&runner_meta(), &foreign).is_err(), "extra must be receiver-owned");
        let mut eligible = runner_meta();
        eligible["scientific_eligible"] = json!(true);
        assert!(build_meta(&eligible, &own()).is_err(), "runner cannot set eligibility");
        let mut owned = runner_meta();
        owned["t0_us"] = json!(1);
        assert!(build_meta(&owned, &own()).is_err(), "receiver-owned key");
        let mut g = runner_meta();
        g["g_report_us"] = json!(2_000_000);
        assert!(build_meta(&g, &own()).is_err(), "L1-R G_report,R");
        let mut missing = runner_meta();
        missing.as_object_mut().unwrap().remove("attempt");
        assert!(build_meta(&missing, &own()).is_err());
    }

    #[test]
    fn exp2_header_identity_matches_the_ledger_and_wire_format() {
        // 32-byte little-endian header, integer µs, exact i <-> 3i.
        for i in 0..N_PC as u64 {
            let (pts, e) = pc_slot_identity(i);
            assert_eq!(pts, i * 1_000_000 / 30);
            let (hpts, he) = haptic_slot_identity(3 * i);
            assert_eq!((hpts, he), (pts, e), "anchor 3i shares pts and event_id with PC i");
            for j in 1..3 {
                assert_eq!(haptic_slot_identity(3 * i + j).1, 0, "fillers carry event_id 0");
            }
            let packed = crate::pack_header(TRACK_PC, 2, i as u32, pts, e, 123, 456);
            assert_eq!(packed.len(), crate::HDR);
            assert_eq!(crate::HDR, 32);
            let h = crate::unpack_header(&packed).unwrap();
            assert_eq!((h.pts_us, h.event_id, h.seq, h.gen_ts_us, h.payload_len), (pts, e, i as u32, 123, 456));
        }
        assert!(pc_slot_identity(N_PC as u64 - 1).0 < RUN_DURATION_US);
        assert!(haptic_slot_identity(N_HAPTIC as u64 - 1).0 < RUN_DURATION_US);
    }

    #[test]
    fn exp2_recorder_v4_switch_barrier_is_a_terminal_drop() {
        // An arrived object discarded by the switch gate on every route and
        // never admitted: terminal drop(switch_barrier) at the seal, not
        // no_object.  A slot whose copy was discarded but that was admitted
        // from another route keeps its release.
        let mut r = rig(Exp2Method::P1);
        let t = slot_ref(Modality::Pc, 100) + 20_000;
        r.set(t);
        r.rec.advance(t - 1).unwrap();
        for (index, gen) in [(100u32, 1u64), (101, 1)] {
            let rx = Exp2Rx { header: header(Modality::Pc, index, slot_ref(Modality::Pc, index)), route_generation: gen, t_recv_us: t };
            r.rec.discard_route_copy(rx, "switch_barrier").unwrap();
        }
        let rows = drive(&mut r, |m, i| (!(m == Modality::Pc && i == 100)).then_some(30_000), |_, _| 0);
        assert_contract_shape(&rows);
        let terminals = assert_single_terminal(&rows);
        let barrier = &terminals[&("pc".to_string(), pc_slot_identity(100).0)];
        assert_eq!(barrier["role"], json!("drop"));
        assert_eq!(barrier["reason"], json!(SWITCH_BARRIER));
        assert_eq!(barrier["seal_horizon_us"].as_u64().unwrap(), horizon_us(T0));
        assert_eq!(barrier["t_first_discard_us"].as_u64().unwrap(), t);
        assert!(barrier["t_log_arrival_us"].is_u64());
        let admitted = &terminals[&("pc".to_string(), pc_slot_identity(101).0)];
        assert_eq!(admitted["role"], json!("release"));
    }

    #[test]
    fn exp2_recorder_v4_release_is_actual_dispatch_time() {
        // A buffered object released at its due is logged at the instant the
        // recorder actually dispatches it (later than the due when the tick
        // is late); the due is only in the decision row.
        let mut r = rig(Exp2Method::S3npaPrime);
        let t_arr = slot_ref(Modality::Pc, 0) + 10_000;
        r.set(t_arr);
        let rx = Exp2Rx { header: header(Modality::Pc, 0, T0), route_generation: 0, t_recv_us: t_arr };
        let Admission::Admitted(m, i) = r.rec.admit(rx).unwrap() else { panic!() };
        r.rec.feed_arrival(m, i, t_arr).unwrap();
        let due = T0 + DMAX;
        let late_tick = due + 700; // the loop woke 700 µs after the due
        r.set(late_tick);
        r.rec.advance(late_tick).unwrap();
        let rows = r.sink.rows();
        let release = rows.iter().find(|r| r["role"] == json!("release")).unwrap();
        assert!(release["t_release_us"].as_u64().unwrap() >= late_tick);
        let decision = rows.iter().find(|r| r["role"] == json!("decision") && r["track"] == json!("pc")).unwrap();
        assert_eq!(decision["due_us"].as_u64().unwrap(), due);
    }

    #[test]
    fn exp2_recorder_v4_controller_rows_and_write_lag() {
        for method in [Exp2Method::P0, Exp2Method::P0np, Exp2Method::S3npaPrime] {
            let mut r = rig(method);
            // sustained loss drives miss steps / Q updates in the adaptive modes
            let rows = drive(&mut r, lossy(11), |_, _| 0);
            assert_contract_shape(&rows);
            let controller: Vec<_> = rows.iter().filter(|r| r["role"] == json!("controller")).collect();
            let commits: Vec<_> = controller.iter().filter(|r| r["event"] == json!("delta_eff_commit")).collect();
            assert!(!commits.is_empty(), "{method:?}: initial Δ_eff commit row");
            assert_eq!(commits[0]["i"], json!(0));
            assert_eq!(commits[0]["value_us"].as_u64().unwrap(), DMAX);
            if method == Exp2Method::P0np {
                assert!(commits.iter().any(|r| r["modality"] == json!("pc")));
                assert!(commits.iter().any(|r| r["modality"] == json!("haptic")));
            } else {
                assert!(commits.iter().all(|r| r["modality"] == json!("shared")));
            }
            if method == Exp2Method::S3npaPrime {
                assert_eq!(controller.len(), 1, "fixed Δ: only the initial commit");
            } else {
                assert!(controller.iter().any(|r| r["event"] == json!("delta_star")), "{method:?}");
            }
        }
    }

    #[test]
    fn exp2_recorder_v4_imports_sender_wire_counter() {
        let mut r = rig(Exp2Method::P0);
        let mut sample = Map::new();
        sample.insert("role".into(), json!("wire"));
        sample.insert("source".into(), json!("sender"));
        sample.insert("kind".into(), json!("delivery_timeout_counter"));
        sample.insert("run_id".into(), json!("run-1"));
        sample.insert("t_us".into(), json!(T0 + 41_000_000));
        sample.insert("delivery_timeout_count".into(), json!(3));
        assert!(r.rec.import_wire_counter(&sample, "/x/sender_wire.jsonl").unwrap());
        let rows = r.sink.rows();
        let wire = rows.iter().find(|r| r["role"] == json!("wire")).unwrap();
        assert_eq!(wire["source"], json!("sender"));
        assert_eq!(wire["delivery_timeout_count"], json!(3));
        assert_eq!(wire["t_sample_us"], json!(T0 + 41_000_000));
        // Codex 85: malformed counter rows are not imported -> warning only
        let mut bad = Vec::new();
        for (key, value) in [
            ("run_id", json!("run-2")),
            ("source", json!("receiver")),
            ("kind", json!("receiver_summary")),
            ("role", json!("info")),
            ("delivery_timeout_count", json!(-1)),
            ("delivery_timeout_count", json!("3")),
        ] {
            let mut m = sample.clone();
            m.insert(key.into(), value);
            bad.push(m);
        }
        let mut missing_kind = sample.clone();
        missing_kind.remove("kind");
        bad.push(missing_kind);
        for m in &bad {
            assert!(!r.rec.import_wire_counter(m, "x").unwrap(), "{m:?}");
        }
        let rows = r.sink.rows();
        assert_eq!(rows.iter().filter(|r| r["role"] == json!("wire")).count(), 1, "nothing else imported");
        assert_eq!(
            rows.iter().filter(|r| r["kind"] == json!("wire_counter_malformed")).count(),
            bad.len()
        );
    }


    #[test]
    fn exp2_sender_timeout_counter_and_sidecar_rows() {
        let counter = Arc::new(DeliveryTimeoutCounter::default());
        counter.set_t0(100);
        counter.note(b"pc", 100);
        counter.note(b"pc-d7", 150);
        counter.note(b"haptic", 200);
        counter.note(b"pc", 99); // created before t0: warm-up, excluded
        assert_eq!(counter.excluded_warmup(), 1);
        assert_eq!(counter.snapshot(), (3, 2, 1, 0));
        let path = std::env::temp_dir().join(format!("skew-exp2-sidecar-{}-{}", std::process::id(), crate::now_us()));
        let side = SenderWireSidecar::create(&path, "run-1", Some(counter.clone())).unwrap();
        side.sample("t0", 10).unwrap();
        counter.note(b"pc", 300);
        side.sample("horizon", 20).unwrap();
        assert!(SenderWireSidecar::create(&path, "run-1", None).is_err(), "create-only");
        let rows: Vec<Value> = std::fs::read_to_string(&path).unwrap().lines().map(|l| serde_json::from_str(l).unwrap()).collect();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1]["source"], json!("sender"));
        assert_eq!(rows[1]["kind"], json!("delivery_timeout_counter"));
        assert_eq!(rows[1]["delivery_timeout_count"], json!(4));
        assert_eq!(rows[1]["by_track"]["pc"], json!(3));
        // the receiver imports the latest sample as-is
        let mut r = rig(Exp2Method::P0);
        assert!(r.rec.import_wire_counter(rows[1].as_object().unwrap(), "x").unwrap());
        let imported = r.sink.rows().into_iter().find(|r| r["role"] == json!("wire")).unwrap();
        assert_eq!(imported["delivery_timeout_count"], json!(4));
        assert_eq!(imported["sample"], json!("horizon"));
        let _ = std::fs::remove_file(&path);
        // B1/S1: no timeout configured -> 0
        let path2 = std::env::temp_dir().join(format!("skew-exp2-sidecar0-{}-{}", std::process::id(), crate::now_us()));
        let none = SenderWireSidecar::create(&path2, "run-1", None).unwrap();
        none.sample("shutdown", 30).unwrap();
        let row: Value = serde_json::from_str(std::fs::read_to_string(&path2).unwrap().lines().next().unwrap()).unwrap();
        assert_eq!(row["delivery_timeout_count"], json!(0));
        assert_eq!(row["delivery_timeout_configured"], json!(false));
        let _ = std::fs::remove_file(&path2);
    }


    #[test]
    fn exp2_sender_counter_observes_only_forward_timeouts() {
        use moq_transport::object_trace::{Boundary, Observer};
        use moq_transport::{coding::TrackNamespace, serve::{SubgroupInfo, Track}};
        let object = |name: &str| {
            let (mut writer, _reader) = SubgroupInfo {
                track: Arc::new(Track::new(TrackNamespace::from_utf8_path("run"), name)),
                group_id: 0,
                subgroup_id: 0,
                priority: 0,
            }
            .produce();
            writer.create(32, None).unwrap().info.clone()
        };
        let counter = DeliveryTimeoutCounter::default();
        // Codex 85: a warm-up PC object created at t0 − 30 ms that times out
        // at t0 + 315 ms is NOT counted; scope follows the object's creation.
        let warmup = object("pc");
        let t0 = crate::now_us() + 30_000;
        counter.set_t0(t0);
        std::thread::sleep(std::time::Duration::from_micros(t0 + 315_000 - crate::now_us()));
        counter.record(Boundary::ForwardTimeout, &warmup, 1, crate::now_us());
        assert_eq!(counter.snapshot().0, 0, "warm-up expiry after t0 is not counted");
        assert_eq!(counter.excluded_warmup(), 1);
        // only ForwardTimeout counts; measurement objects (created >= t0) do
        for boundary in [Boundary::ForwardStart, Boundary::ForwardAccepted, Boundary::ReceiveTimeout] {
            counter.record(boundary, &object("pc"), 1, crate::now_us());
        }
        assert_eq!(counter.snapshot().0, 0);
        counter.record(Boundary::ForwardTimeout, &object("pc-d6"), 1, crate::now_us());
        counter.record(Boundary::ForwardTimeout, &object("haptic-essential"), 2, crate::now_us());
        assert_eq!(counter.snapshot(), (2, 1, 1, 0));
        // nothing is counted before t0 is known
        let fresh = DeliveryTimeoutCounter::default();
        fresh.record(Boundary::ForwardTimeout, &object("pc"), 1, crate::now_us());
        assert_eq!((fresh.snapshot().0, fresh.excluded_warmup()), (0, 1));
        assert!(delivery_timeout_in_measurement_scope(t0, t0));
        assert!(!delivery_timeout_in_measurement_scope(t0 - 30_000, t0));
    }


    #[test]
    fn exp2_recorder_s1_seal_waits_for_late_admission_codex85() {
        // An S1 object stamped t_recv = H − 1 µs is admitted by the receive
        // task only at H + ε.  The S1 seal must not run at H (it would mark
        // the slot no_object); it runs at H + delay and sees the admission.
        let h = horizon_us(T0);
        let mut r = rig(Exp2Method::S1);
        r.set(T0 + 1);
        r.rec.advance(T0 + 1).unwrap();
        let last = (N_PC - 1) as u32;
        r.set(h);
        assert!(!r.rec.seal_s1_if_due(h, &|_| None).unwrap(), "no seal at H");
        let eps = 300;
        r.set(h + eps);
        let rx = Exp2Rx { header: header(Modality::Pc, last, slot_ref(Modality::Pc, last)), route_generation: 0, t_recv_us: h - 1 };
        assert!(matches!(r.rec.admit(rx).unwrap(), Admission::Admitted(..)));
        // S1 dispatches it after H: suppressed, its due is learned
        r.rec.s1_release(&header(Modality::Pc, last, 0), h + eps + 10, Some(h - 50)).unwrap();
        assert!(!r.rec.seal_s1_if_due(h + PLAIN_SEAL_EXECUTION_DELAY_US - 1, &|_| None).unwrap());
        r.set(h + PLAIN_SEAL_EXECUTION_DELAY_US);
        assert!(r.rec.seal_s1_if_due(h + PLAIN_SEAL_EXECUTION_DELAY_US, &|_| None).unwrap());
        r.set(h + G_REPORT_R_US);
        assert!(r.rec.finish(h + G_REPORT_R_US, None).unwrap());
        let rows = r.sink.rows();
        assert_contract_shape(&rows);
        let terminals = assert_single_terminal(&rows);
        let slot = &terminals[&("pc".to_string(), pc_slot_identity(last as u64).0)];
        assert_ne!(slot["role"], json!("no_object"), "an arrived object is not no_object");
        assert_eq!(slot["role"], json!("drop"));
        assert_eq!(slot["reason"], json!(S1_UNRELEASED_AT_HORIZON));
        assert_eq!(r.rec.stats().post_horizon_terminals_suppressed, 1);
    }

}
