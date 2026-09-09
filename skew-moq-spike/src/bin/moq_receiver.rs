// moq_receiver — MoQ naive B1 subscriber (L1: t_play = t_recv).
//
// Subscribes to both tracks (pc, haptic) on namespace == run_id via the relay,
// reads each object, parses the 32B header, and logs an rx record with
// t_recv = t_play (arrival) and t_gen (from the header) for the D metrics.
// Each MoQ object is one complete message, so no byte reassembly is needed.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use clap::Parser;
use moq_native_ietf::{quic, tls};
use moq_transport::{
    coding::{KeyValuePairs, TrackNamespace},
    message::SubscriptionFilter,
    serve::{Track, TrackReader, TrackReaderMode, Tracks},
    session::{
        DataPriorityMapping, PublishedNamespace, Session, SessionConfig, Subscribe, Subscriber,
    },
};
use std::process::Stdio;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::sync::mpsc;
use url::Url;

use skew_moq::playout::{
    LatePolicy, PlayoutAction, PlayoutConfig, PlayoutObject, PlayoutScheduler,
};
use skew_moq::s3_controller::{S3Config, S3Controller, S3Observation, S3Update};
use skew_moq::s3_receiver::{
    validate_routed_object, IngressEvent, RetirementCause, RoutedObject, S3DeadlineTracker,
    S3ReceiverIngress, S3RetirementQueue, DROP_DUPLICATE_IDENTITY, DROP_STALE_TIER,
};
use skew_moq::s3_switch::{Route, S3SwitchGate, SwitchApplied, SwitchConfig, TrackRole};
use skew_moq::*;

#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum Arm {
    B1,
    S1,
    M1,
    S2,
    #[value(name = "s2eq")]
    S2Eq,
    S3,
}

impl Arm {
    fn as_str(self) -> &'static str {
        match self {
            Self::B1 => "b1",
            Self::S1 => "s1",
            Self::M1 => "m1",
            Self::S2 => "s2",
            Self::S2Eq => "s2eq",
            Self::S3 => "s3",
        }
    }
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum CliLatePolicy {
    ReleaseLate,
    DropLate,
}

impl From<CliLatePolicy> for LatePolicy {
    fn from(value: CliLatePolicy) -> Self {
        match value {
            CliLatePolicy::ReleaseLate => LatePolicy::ReleaseLate,
            CliLatePolicy::DropLate => LatePolicy::DropLate,
        }
    }
}

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "https://10.0.0.2:4443")]
    relay: Url,
    /// Direct (relay-free) topology: bind here and ACCEPT one inbound session
    /// instead of connecting out to a relay. The sender still uses `--relay`,
    /// pointed at this address.
    ///
    /// Registered as the `Md` arm of the topology axis
    /// (md/20260813_실험1_6팔_토폴로지축_설계개정.md). MoQT is an
    /// endpoint-to-endpoint protocol — the relay is an optional fan-out
    /// element, and `moq_transport::Session` exposes `accept` alongside
    /// `connect`. Nothing here changes the wire format.
    #[arg(long)]
    listen: Option<SocketAddr>,
    /// TLS certificate chain for `--listen`. Required with `--listen`.
    #[arg(long)]
    tls_cert: Option<PathBuf>,
    /// TLS private key for `--listen`. Required with `--listen`.
    #[arg(long)]
    tls_key: Option<PathBuf>,
    #[arg(long)]
    run_id: String,
    #[arg(long)]
    out: PathBuf,
    /// Optional bounded raw receive-object trace. B1/frame verification only.
    #[arg(long)]
    receive_trace: Option<PathBuf>,
    /// Preallocated receive-trace record bound (registered + complete per
    /// object, plus timeout/interrupt boundaries). The runner sizes it from
    /// the planned object count; overflow is counted and fails the seal.
    #[arg(long, default_value_t = 4096)]
    receive_trace_capacity: usize,
    #[arg(long)]
    s_bytes: u64,
    #[arg(long)]
    c_mbps: Option<f64>,
    #[arg(long, default_value_t = 0.0)]
    rtt_ms: f64,
    #[arg(long, default_value_t = 0.0)]
    jitter_ms: f64,
    #[arg(long, default_value_t = 0.0)]
    loss_pct: f64,
    #[arg(long, default_value_t = 0)]
    seed: u64,
    /// Local translation from MoQT publisher priority to quinn stream
    /// priority. Explicit so v5 bridge runs cannot silently mix v1/v2.
    #[arg(long, default_value = "legacy-v1")]
    data_priority_mapping: DataPriorityMapping,
    #[arg(long)]
    pc_rate_hz: u64,
    #[arg(long)]
    haptic_rate_hz: u64,
    #[arg(long, value_enum)]
    payload_mode: PayloadMode,
    /// PC payload encoding axis of the 4-arm re-run. Required, not defaulted:
    /// a silent default would label a draco run as bin in the meta line.
    #[arg(long, value_enum)]
    representation: Representation,
    /// Forwarding structure of the arm (log schema 4). Required, not defaulted.
    /// `--listen` implies `direct` and its absence implies `relay`, but the
    /// value is declared rather than derived so that a runner wiring bug shows
    /// up as a startup failure instead of a mislabelled log; the two are
    /// cross-checked below.
    #[arg(long, value_enum)]
    topology: Topology,
    #[arg(long)]
    chunk_bytes: usize,
    #[arg(long)]
    reassembly_max_pending_frames: usize,
    #[arg(long)]
    reassembly_max_pending_bytes: usize,
    #[arg(long)]
    reassembly_max_age_ms: u64,
    #[arg(long, default_value_t = 180.0)]
    max_duration: f64,
    /// Stage A: render received pc frames via the Python bridge (same pipeline as B0).
    #[arg(long)]
    render: bool,
    /// Stage A: play received haptic PCM to the DAC via the Python bridge.
    #[arg(long)]
    audio: bool,
    /// Draco arm: decode received pc payload (.drc) before rendering (bridge).
    #[arg(long)]
    draco: bool,
    /// Python interpreter for the Stage A bridge (cwd is the repo root).
    #[arg(long, default_value = ".venv/bin/python")]
    python: String,
    /// C3: which tracks the publisher was told to generate. Used only to know
    /// which tracks are *expected to be empty*, so a deliberately silent track
    /// is not mistaken for a lost one. Does not change what is subscribed.
    #[arg(long, value_enum, default_value_t = RxTrackSel::Both)]
    tracks: RxTrackSel,
    /// Design run length (seconds). Supplying it lets the receiver resolve an
    /// ambiguous `Cancel` by checking whether the design quantity arrived.
    /// Without it a cancelled track cannot be proven complete and the run exits
    /// `EXIT_RX_CANCELLED_INCOMPLETE`.
    #[arg(long)]
    duration_s: Option<f64>,
    /// Budget for establishing the subscription, in seconds.
    ///
    /// The publisher's namespace announce can arrive after the subscriber is
    /// up, so a `Track not found` is retried within this budget instead of
    /// failing the run. Only "not found" is retried; any other error fails
    /// immediately so a real fault is not hidden by polling.
    #[arg(long, default_value_t = 10.0)]
    subscribe_timeout: f64,
    /// Poll interval for the subscribe retry, in milliseconds.
    #[arg(long, default_value_t = 100)]
    subscribe_retry_ms: u64,
    /// Phase-4 arm. B1 remains the default and preserves the historical path.
    #[arg(long, value_enum, default_value_t = Arm::B1)]
    arm: Arm,
    /// Stage-5 queue policy. `separate` (default) subscribes pc+haptic;
    /// `shared_fifo` (B1/frame only) subscribes the single "mixed" track and
    /// demultiplexes by the 32-byte header track_id into unchanged rx rows.
    #[arg(long, value_enum, default_value_t = QueuePolicy::Separate)]
    queue_policy: QueuePolicy,
    /// Fixed S1 playout offset. The governing design permits only 50/100 ms
    /// before the pilot selects one; S1 requires an explicit choice.
    #[arg(long)]
    d_play_ms: Option<u64>,
    /// Bound for finding the first exact PC/haptic anchor pair.
    #[arg(long)]
    startup_timeout_ms: Option<u64>,
    /// Number of identical startup windows allowed after the first timeout.
    ///
    /// Phase-4 v5 requires exactly one common re-arm for S1/M1/S2.
    #[arg(long)]
    startup_rearm_limit: Option<u8>,
    /// Grace after a fixed timeline deadline before the selected late policy.
    #[arg(long)]
    late_tolerance_ms: Option<u64>,
    /// Per-track object-count bound for the S1 playout buffer.
    #[arg(long)]
    buffer_max_objects_per_track: Option<usize>,
    /// Per-track PTS-span bound for the S1 playout buffer.
    #[arg(long)]
    buffer_max_span_ms: Option<u64>,
    /// Fixed late policy for the entire S1 run.
    #[arg(long, value_enum)]
    late_policy: Option<CliLatePolicy>,
    /// S2 hop-local PC object forwarding budget in integer milliseconds.
    #[arg(long)]
    pc_delivery_timeout_ms: Option<u64>,
    /// S3 controller window. Required explicitly by --arm s3.
    #[arg(long)]
    s3_window_ms: Option<u64>,
    #[arg(long)]
    s3_ewma_alpha: Option<f64>,
    #[arg(long)]
    s3_miss_streak_threshold: Option<u32>,
    #[arg(long)]
    s3_violation_ratio_threshold: Option<f64>,
    #[arg(long)]
    s3_target_skew_ms: Option<u64>,
    #[arg(long)]
    s3_recovery_fraction: Option<f64>,
    #[arg(long)]
    s3_haptic_critical_stable_ms: Option<u64>,
    #[arg(long)]
    s3_recovery_stable_ms: Option<u64>,
    #[arg(long)]
    s3_cooldown_ms: Option<u64>,
    #[arg(long)]
    s3_min_paired_samples: Option<usize>,
    #[arg(long)]
    s3_max_window_samples: Option<usize>,
    /// Occupancy bound for the S3 deadline tracker, in anchors.
    ///
    /// S3 FSM 구현계약 §2.1 규칙 E: PC-only anchor expiry stays unchanged
    /// (an anchor whose haptic counterpart never arrives is never expired),
    /// so this bound must NOT be derived from the scheduler's per-track
    /// buffer bound the way it used to be. It is a separately recorded
    /// parameter, sized from the registered generated anchor population.
    /// No implicit S3 default.
    #[arg(long)]
    s3_deadline_max_anchors: Option<usize>,
    /// Request-to-exact-pair first-effect bound. No implicit S3 default.
    #[arg(long)]
    s3_effect_timeout_ms: Option<u64>,
    /// Maximum retry attempts while establishing the initial Normal routes.
    /// Explicitly bounded so a missing publisher cannot exhaust request IDs.
    #[arg(long)]
    s3_initial_retry_limit: Option<u32>,
    /// Maximum retry attempts after a failed target subscription.
    #[arg(long)]
    s3_switch_retry_limit: Option<u32>,
    /// Hidden mechanism-test gate. Any run using this is ineligible for
    /// performance or scientific claims.
    #[arg(long, hide = true)]
    s3_test_mode: bool,
    /// Inject the frozen three-miss trigger after controller activation.
    #[arg(long, hide = true)]
    s3_test_force_misses_after_ms: Option<u64>,
}

fn ms_to_us(value: u64, name: &str) -> Result<u64> {
    value
        .checked_mul(1_000)
        .with_context(|| format!("{name} is too large"))
}

/// Validate the ablation boundary before opening the output log. No S1 value
/// is implicit: parameters that affect release/drop decisions are required and
/// then written into rx metadata.
fn playout_config(args: &Args) -> Result<Option<PlayoutConfig>> {
    let supplied = args.d_play_ms.is_some()
        || args.startup_timeout_ms.is_some()
        || args.startup_rearm_limit.is_some()
        || args.late_tolerance_ms.is_some()
        || args.buffer_max_objects_per_track.is_some()
        || args.buffer_max_span_ms.is_some()
        || args.late_policy.is_some()
        || args.pc_delivery_timeout_ms.is_some();
    if args.arm == Arm::B1 {
        if supplied {
            bail!("S1 scheduler options require --arm s1; B1 must remain uncontrolled");
        }
        return Ok(None);
    }

    match args.arm {
        Arm::B1 => unreachable!("handled above"),
        Arm::S1 | Arm::M1 => {
            if args.pc_delivery_timeout_ms.is_some() {
                bail!("PC delivery timeout requires --arm s2");
            }
        }
        Arm::S2 | Arm::S2Eq | Arm::S3 => {
            let timeout = args.pc_delivery_timeout_ms.with_context(|| {
                format!(
                    "--arm {} requires --pc-delivery-timeout-ms",
                    args.arm.as_str()
                )
            })?;
            if timeout == 0 {
                bail!("--pc-delivery-timeout-ms must be greater than zero");
            }
            if args.arm == Arm::S3 && timeout != 67 {
                bail!("--arm s3 inherits the frozen 67ms PC delivery timeout");
            }
        }
    }

    let arm = args.arm.as_str();
    let d_play_ms = args
        .d_play_ms
        .with_context(|| format!("--arm {arm} requires --d-play-ms"))?;
    if !matches!(d_play_ms, 50 | 100) {
        bail!("--d-play-ms must be a governing-design candidate: 50 or 100");
    }
    let config = PlayoutConfig {
        d_play_us: ms_to_us(d_play_ms, "d-play-ms")?,
        startup_timeout_us: ms_to_us(
            args.startup_timeout_ms
                .with_context(|| format!("--arm {arm} requires --startup-timeout-ms"))?,
            "startup-timeout-ms",
        )?,
        startup_rearm_limit: args
            .startup_rearm_limit
            .with_context(|| format!("--arm {arm} requires --startup-rearm-limit"))?,
        late_tolerance_us: ms_to_us(
            args.late_tolerance_ms
                .with_context(|| format!("--arm {arm} requires --late-tolerance-ms"))?,
            "late-tolerance-ms",
        )?,
        max_objects_per_track: args
            .buffer_max_objects_per_track
            .with_context(|| format!("--arm {arm} requires --buffer-max-objects-per-track"))?,
        max_span_us: ms_to_us(
            args.buffer_max_span_ms
                .with_context(|| format!("--arm {arm} requires --buffer-max-span-ms"))?,
            "buffer-max-span-ms",
        )?,
        late_policy: args
            .late_policy
            .with_context(|| format!("--arm {arm} requires --late-policy"))?
            .into(),
    };
    if config.startup_rearm_limit != 1 {
        bail!("--startup-rearm-limit must be the governing-design value 1");
    }
    config.validate().map_err(anyhow::Error::msg)?;
    Ok(Some(config))
}

struct S3RuntimeConfig {
    controller: S3Config,
    switch: SwitchConfig,
    initial_retry_limit: u32,
    switch_retry_limit: u32,
    deadline_max_anchors: usize,
    test_force_misses_after_us: Option<u64>,
}

fn s3_runtime_config(args: &Args) -> Result<Option<S3RuntimeConfig>> {
    let supplied = args.s3_window_ms.is_some()
        || args.s3_ewma_alpha.is_some()
        || args.s3_miss_streak_threshold.is_some()
        || args.s3_violation_ratio_threshold.is_some()
        || args.s3_target_skew_ms.is_some()
        || args.s3_recovery_fraction.is_some()
        || args.s3_haptic_critical_stable_ms.is_some()
        || args.s3_recovery_stable_ms.is_some()
        || args.s3_cooldown_ms.is_some()
        || args.s3_min_paired_samples.is_some()
        || args.s3_max_window_samples.is_some()
        || args.s3_deadline_max_anchors.is_some()
        || args.s3_effect_timeout_ms.is_some()
        || args.s3_initial_retry_limit.is_some()
        || args.s3_switch_retry_limit.is_some()
        || args.s3_test_mode
        || args.s3_test_force_misses_after_ms.is_some();
    if args.arm != Arm::S3 {
        if supplied {
            bail!("S3 controller/switch options require --arm s3");
        }
        return Ok(None);
    }
    if args.tracks != RxTrackSel::Both {
        bail!("--arm s3 requires --tracks both");
    }
    let controller = S3Config {
        window_us: ms_to_us(
            args.s3_window_ms
                .context("--arm s3 requires --s3-window-ms")?,
            "s3-window-ms",
        )?,
        ewma_alpha: args
            .s3_ewma_alpha
            .context("--arm s3 requires --s3-ewma-alpha")?,
        miss_streak_threshold: args
            .s3_miss_streak_threshold
            .context("--arm s3 requires --s3-miss-streak-threshold")?,
        violation_ratio_threshold: args
            .s3_violation_ratio_threshold
            .context("--arm s3 requires --s3-violation-ratio-threshold")?,
        target_skew_us: ms_to_us(
            args.s3_target_skew_ms
                .context("--arm s3 requires --s3-target-skew-ms")?,
            "s3-target-skew-ms",
        )?,
        recovery_fraction: args
            .s3_recovery_fraction
            .context("--arm s3 requires --s3-recovery-fraction")?,
        haptic_critical_stable_us: ms_to_us(
            args.s3_haptic_critical_stable_ms
                .context("--arm s3 requires --s3-haptic-critical-stable-ms")?,
            "s3-haptic-critical-stable-ms",
        )?,
        recovery_stable_us: ms_to_us(
            args.s3_recovery_stable_ms
                .context("--arm s3 requires --s3-recovery-stable-ms")?,
            "s3-recovery-stable-ms",
        )?,
        cooldown_us: ms_to_us(
            args.s3_cooldown_ms
                .context("--arm s3 requires --s3-cooldown-ms")?,
            "s3-cooldown-ms",
        )?,
        min_paired_samples: args
            .s3_min_paired_samples
            .context("--arm s3 requires --s3-min-paired-samples")?,
        max_window_samples: args
            .s3_max_window_samples
            .context("--arm s3 requires --s3-max-window-samples")?,
    };
    if controller.window_us != 1_000_000
        || controller.ewma_alpha != 0.2
        || controller.miss_streak_threshold != 3
        || controller.violation_ratio_threshold != 0.2
        || controller.recovery_fraction != 0.5
        || controller.haptic_critical_stable_us != 3_000_000
        || controller.recovery_stable_us != 5_000_000
        || controller.cooldown_us != 2_000_000
    {
        bail!(
            "--arm s3 must preserve window=1000ms, alpha=0.2, streak=3, \
             ratio=0.2, recovery-fraction=0.5, HC=3000ms, Recovery=5000ms, \
             cooldown=2000ms"
        );
    }
    if !matches!(
        controller.target_skew_us,
        15_000 | 25_000 | 50_000 | 100_000
    ) {
        bail!("--s3-target-skew-ms must be a governing-design candidate: 15, 25, 50, or 100");
    }
    controller.validate().map_err(anyhow::Error::msg)?;
    let switch = SwitchConfig {
        effect_timeout_us: ms_to_us(
            args.s3_effect_timeout_ms
                .context("--arm s3 requires --s3-effect-timeout-ms")?,
            "s3-effect-timeout-ms",
        )?,
    };
    switch
        .validate()
        .map_err(|error| anyhow::anyhow!("invalid S3 switch config: {error:?}"))?;
    let initial_retry_limit = args
        .s3_initial_retry_limit
        .context("--arm s3 requires --s3-initial-retry-limit")?;
    let switch_retry_limit = args
        .s3_switch_retry_limit
        .context("--arm s3 requires --s3-switch-retry-limit")?;
    if args.s3_test_force_misses_after_ms.is_some() && !args.s3_test_mode {
        bail!("--s3-test-force-misses-after-ms requires --s3-test-mode");
    }
    let test_force_misses_after_us = args
        .s3_test_force_misses_after_ms
        .map(|value| ms_to_us(value, "s3-test-force-misses-after-ms"))
        .transpose()?;
    // 계약 §2.1 규칙 E. The old code reused the scheduler's per-track buffer
    // bound, which is only defensible if every registered anchor is expired —
    // and PC-only anchors are not. Sizing this from the generated anchor
    // population instead makes a violation mean "more anchors than the run can
    // generate", i.e. a real defect, rather than ordinary PC-only residue.
    let deadline_max_anchors = args
        .s3_deadline_max_anchors
        .context("--arm s3 requires --s3-deadline-max-anchors")?;
    if deadline_max_anchors == 0 {
        bail!("--s3-deadline-max-anchors must be > 0");
    }
    Ok(Some(S3RuntimeConfig {
        controller,
        switch,
        initial_retry_limit,
        switch_retry_limit,
        deadline_max_anchors,
        test_force_misses_after_us,
    }))
}

fn phase4_transport(args: &Args) -> Option<Phase4TransportMeta> {
    match args.arm {
        Arm::B1 | Arm::S1 => None,
        Arm::M1 => Some(Phase4TransportMeta {
            arm: "m1",
            pc_subgroup_mapping: "frame-per-subgroup",
            pc_publisher_priority: 128,
            haptic_publisher_priority: 128,
            publisher_priority_profile: "equal-128",
            data_priority_mapping: args.data_priority_mapping.as_str(),
            pc_delivery_timeout_ms: None,
        }),
        Arm::S2 | Arm::S2Eq | Arm::S3 => Some(Phase4TransportMeta {
            arm: args.arm.as_str(),
            pc_subgroup_mapping: "frame-per-subgroup",
            pc_publisher_priority: if args.arm == Arm::S2Eq { 128 } else { 1 },
            haptic_publisher_priority: if args.arm == Arm::S2Eq { 128 } else { 0 },
            publisher_priority_profile: if args.arm == Arm::S2Eq {
                "equal-128"
            } else {
                "relative-haptic0-pc1"
            },
            data_priority_mapping: args.data_priority_mapping.as_str(),
            pc_delivery_timeout_ms: args.pc_delivery_timeout_ms,
        }),
    }
}

/// The declared topology must match the wiring this process actually builds.
///
/// `--listen` accepts an inbound session from the sender with no forwarding
/// element in between (`Md`); its absence dials a relay (`M`). Declaring one and
/// building the other would write a log whose `topology` is a lie, and the whole
/// point of the axis is that `Md − M` is read off that field.
fn validate_topology(topology: Topology, listening: bool) -> Result<()> {
    match (topology, listening) {
        (Topology::Direct, false) => bail!(
            "--topology direct requires --listen: without it this receiver dials \
             a relay, which is the relay topology"
        ),
        (Topology::Relay, true) => bail!(
            "--topology relay must not be combined with --listen: --listen is the \
             direct (relay-free) path"
        ),
        _ => Ok(()),
    }
}

/// Stage-5 boundary: shared_fifo is a B1/frame-only control configuration.
/// The raw receive trace (`--receive-trace`) stays available: it records
/// transport boundaries per WIRE track (`track`/`track_hex` = "mixed",
/// group/subgroup/object_id) without decoding the header, so P1 is traced
/// exactly like P2/P3 and joins to TX rows through `transport_track`.
fn validate_queue_policy(
    queue_policy: QueuePolicy,
    arm: Arm,
    payload_mode: PayloadMode,
) -> Result<()> {
    if queue_policy != QueuePolicy::SharedFifo {
        return Ok(());
    }
    if arm != Arm::B1 {
        bail!("--queue-policy shared_fifo requires --arm b1");
    }
    if payload_mode != PayloadMode::Frame {
        bail!("--queue-policy shared_fifo requires --payload-mode frame");
    }
    Ok(())
}

fn validate_phase4_v5_args(args: &Args) -> Result<()> {
    if args.arm != Arm::B1 && args.payload_mode != PayloadMode::Frame {
        bail!(
            "--arm {} requires --payload-mode frame; equal-chunk is a B1 negative ablation only",
            args.arm.as_str()
        );
    }
    if matches!(args.arm, Arm::S2 | Arm::S2Eq | Arm::S3)
        && args.data_priority_mapping != DataPriorityMapping::MoqtV2
    {
        bail!(
            "--arm {} requires --data-priority-mapping moqt-v2 in the v5 generation",
            args.arm.as_str()
        );
    }
    Ok(())
}

#[derive(Debug, Default)]
struct PlayoutStats {
    released: u64,
    dropped: u64,
    bridge_observer_dropped: u64,
}

fn dispatch_playout_actions(
    actions: Vec<PlayoutAction>,
    logger: &Arc<Mutex<JsonlLogger>>,
    bridge: Option<&mpsc::Sender<Bytes>>,
    render: bool,
    audio: bool,
    stats: &mut PlayoutStats,
) -> std::io::Result<()> {
    for action in actions {
        let object = action.object();
        let h = object.header;
        let action_time = now_us().max(object.t_recv);
        match action {
            PlayoutAction::Release(object) => {
                logger.lock().unwrap().try_log_release(
                    object.track_name(),
                    h.tier,
                    h.seq,
                    h.pts_us,
                    h.event_id,
                    action_time,
                )?;
                stats.released += 1;
                let observe =
                    (h.track_id == TRACK_PC && render) || (h.track_id == TRACK_HAPTIC && audio);
                if observe {
                    if let Some(tx) = bridge {
                        if tx.try_send(object.bytes).is_err() {
                            // Stage-A forwarding remains a best-effort observer
                            // of the headless L1-R sink. Count its loss without
                            // rewriting the already valid release decision.
                            stats.bridge_observer_dropped += 1;
                        }
                    }
                }
            }
            PlayoutAction::Drop { object, reason, .. } => {
                logger.lock().unwrap().try_log_drop(
                    object.track_name(),
                    h.tier,
                    h.seq,
                    h.pts_us,
                    h.event_id,
                    action_time,
                    reason,
                )?;
                stats.dropped += 1;
            }
        }
    }
    Ok(())
}

async fn run_playout_scheduler(
    config: PlayoutConfig,
    mut input: mpsc::Receiver<PlayoutObject>,
    logger: Arc<Mutex<JsonlLogger>>,
    bridge: Option<mpsc::Sender<Bytes>>,
    render: bool,
    audio: bool,
) -> Result<PlayoutStats> {
    let mut scheduler = PlayoutScheduler::new(config).map_err(anyhow::Error::msg)?;
    let mut stats = PlayoutStats::default();

    loop {
        let actions = if let Some(wakeup) = scheduler.next_wakeup_us() {
            let wait = Duration::from_micros(wakeup.saturating_sub(now_us()));
            tokio::select! {
                item = input.recv() => match item {
                    Some(item) => scheduler.push(item, now_us()),
                    None => break,
                },
                _ = tokio::time::sleep(wait) => scheduler.advance(now_us()),
            }
        } else {
            match input.recv().await {
                Some(item) => scheduler.push(item, now_us()),
                None => break,
            }
        };
        dispatch_playout_actions(actions, &logger, bridge.as_ref(), render, audio, &mut stats)?;
    }

    if !scheduler.is_started() {
        let actions = scheduler.finish_without_epoch();
        dispatch_playout_actions(actions, &logger, bridge.as_ref(), render, audio, &mut stats)?;
    } else {
        // Producer ended: deterministically drain the bounded timeline, then
        // return so logger finalization cannot race a detached scheduler task.
        while !scheduler.is_empty() {
            let Some(wakeup) = scheduler.next_wakeup_us() else {
                break;
            };
            tokio::time::sleep(Duration::from_micros(wakeup.saturating_sub(now_us()))).await;
            let actions = scheduler.advance(now_us());
            dispatch_playout_actions(actions, &logger, bridge.as_ref(), render, audio, &mut stats)?;
        }
    }
    Ok(stats)
}

/// `RequestErrorCode::DoesNotExist` (draft-ietf-moq-transport §13.1). A remote
/// "the track does not exist" arrives as `ServeError::Closed(0x10)`, because
/// the subscriber passes the wire error code through unchanged.
const REQUEST_ERROR_DOES_NOT_EXIST: u64 = 0x10;

/// Whether a failed subscribe should be retried.
///
/// Retry **only** "the track does not exist yet": that is the announce/subscribe
/// ordering race and it resolves on its own once the publisher announces. Every
/// other error — auth, timeout, not-supported, duplicate, malformed — is a real
/// fault and must surface immediately; blanket retrying (the previous
/// behaviour) buries the real cause until the budget expires and then reports
/// the wrong one.
///
/// Two representations mean the same thing here:
///   * `NotFound` / `NotFoundWithId` — raised locally;
///   * `Closed(0x10)` — the same condition arriving from the peer. This is the
///     one that actually occurs against the relay; an earlier revision matched
///     only the local variants and so failed instantly instead of recovering.
///
/// `Closed(0x4)` is deliberately NOT retried. `ServeError::code()` maps
/// `NotFound` to `0x4`, but in the `RequestErrorCode` registry `0x4` is
/// `MalformedAuthToken` — an upstream inconsistency. Retrying it would poll on
/// an auth failure, so the ambiguous code is treated as fatal.
fn is_retryable_subscribe_error(e: &moq_transport::serve::ServeError) -> bool {
    use moq_transport::serve::ServeError;
    matches!(
        e,
        ServeError::NotFound
            | ServeError::NotFoundWithId(..)
            | ServeError::Closed(REQUEST_ERROR_DOES_NOT_EXIST)
    )
}

/// Outcome of establishing the subscription, for the shutdown record.
#[derive(Debug, Clone, Copy, Default)]
struct SubscribeStats {
    /// Retry attempts after the first (0 == succeeded first try).
    retries: u64,
    /// Wall time spent waiting for the subscription to establish.
    waited_ms: u64,
}

/// Mirror of the sender's `--tracks`, used only for expectations.
/// Stage-5 queue policy (mirror of the sender flag).
#[derive(Clone, Copy, PartialEq, Eq, Debug, clap::ValueEnum)]
enum QueuePolicy {
    Separate,
    #[value(name = "shared_fifo")]
    SharedFifo,
}

impl QueuePolicy {
    fn as_str(self) -> &'static str {
        match self {
            QueuePolicy::Separate => "separate",
            QueuePolicy::SharedFifo => "shared_fifo",
        }
    }

    /// Wire tracks to subscribe. `mixed` carries both logical tracks.
    fn wire_tracks(self) -> &'static [&'static str] {
        match self {
            QueuePolicy::Separate => &["pc", "haptic"],
            QueuePolicy::SharedFifo => &[SHARED_FIFO_TRACK],
        }
    }
}

const SHARED_FIFO_TRACK: &str = "mixed";

/// Logical-track slot (0 == pc, 1 == haptic) used for counters/wire stats.
fn track_slot(track: &str) -> usize {
    if track == "pc" {
        0
    } else {
        1
    }
}

/// Header-level demultiplex. On a pc/haptic subscription the header must name
/// that same track (unchanged R3b rule). On the shared "mixed" subscription
/// either logical track is admitted and the row is logged under the header's
/// track, so rx rows are identical to the separate policy. Anything else is a
/// header failure, never a row.
fn demux_track(subscribed: &'static str, track_id: u8) -> Option<&'static str> {
    let track = track_name(track_id);
    if subscribed == SHARED_FIFO_TRACK {
        matches!(track, "pc" | "haptic").then_some(track)
    } else {
        (track == subscribed).then_some(subscribed)
    }
}

/// Per-logical-track reports from the per-wire-track drain ends. A "mixed"
/// end is one subgroup stream carrying both tracks, so its FIN/cancel/fail
/// verdict applies to pc and haptic alike, each against its own design count.
fn expand_reports(
    ends: Vec<(&'static str, TrackEnd, String)>,
    n_pc: u64,
    n_hap: u64,
    args: &Args,
) -> Vec<DrainReport> {
    let mut reports = Vec::new();
    for (name, end, detail) in ends {
        let logical: &[&'static str] = if name == SHARED_FIFO_TRACK {
            &["pc", "haptic"]
        } else {
            &[name]
        };
        for track in logical {
            reports.push(DrainReport {
                name: track,
                end,
                received: if *track == "pc" { n_pc } else { n_hap },
                expected: expected_for(track, args),
                detail: detail.clone(),
            });
        }
    }
    reports
}

#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum RxTrackSel {
    Both,
    Pc,
    Haptic,
}

impl RxTrackSel {
    fn enabled(self, name: &str) -> bool {
        match self {
            RxTrackSel::Both => true,
            RxTrackSel::Pc => name == "pc",
            RxTrackSel::Haptic => name == "haptic",
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            RxTrackSel::Both => "both",
            RxTrackSel::Pc => "pc",
            RxTrackSel::Haptic => "haptic",
        }
    }
}

/// How the drain phase ended, before classification.
enum Drained {
    Ends(Vec<(&'static str, TrackEnd, String)>),
    SessionArm(String),
    Timeout,
}

/// Design expectation for a track, if `--duration-s` was supplied.
///
/// A track disabled by C3 expects exactly 0, so a silent track is provably
/// complete rather than merely unproven.
fn expected_for(name: &str, args: &Args) -> Option<u64> {
    let d = args.duration_s?;
    if !args.tracks.enabled(name) {
        return Some(0);
    }
    let rate = if name == "pc" {
        args.pc_rate_hz
    } else {
        args.haptic_rate_hz
    };
    Some((d * rate as f64).round() as u64)
}

/// How reception ended. Only `Normal` — the publisher closed both tracks with a
/// FIN — may exit 0. Everything else means the rx log is incomplete and a batch
/// runner must be able to see that from the exit code alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RxEnding {
    /// Both tracks drained to end-of-track. The only success.
    Normal,
    /// A drain task returned an error or panicked.
    DrainError,
    /// The session ended before both tracks were drained.
    SessionEnded,
    /// `--max-duration` expired. Reception is incomplete by definition: the
    /// publisher never closed the tracks.
    Timeout,
    /// A track was cancelled and completeness could not be established — either
    /// fewer objects arrived than the design expects, or no expectation was
    /// supplied so completeness is unprovable.
    CancelledIncomplete,
    /// A header failed validation (version, track identity, or length).
    HeaderInvalid,
    /// A track that was expected to carry objects received exactly zero.
    /// Unambiguous total failure of that subscription.
    NoObjects,
}

impl RxEnding {
    fn as_str(self) -> &'static str {
        match self {
            RxEnding::Normal => "normal",
            RxEnding::DrainError => "drain_error",
            RxEnding::SessionEnded => "session_ended",
            RxEnding::Timeout => "timeout",
            RxEnding::CancelledIncomplete => "cancelled_incomplete",
            RxEnding::HeaderInvalid => "header_invalid",
            RxEnding::NoObjects => "no_objects",
        }
    }
    fn exit_code(self) -> i32 {
        match self {
            RxEnding::Normal => 0,
            RxEnding::DrainError => EXIT_RX_DRAIN_ERROR,
            RxEnding::SessionEnded => EXIT_RX_SESSION_ENDED,
            RxEnding::Timeout => EXIT_RX_TIMEOUT,
            RxEnding::CancelledIncomplete => EXIT_RX_CANCELLED_INCOMPLETE,
            RxEnding::HeaderInvalid => EXIT_RX_HEADER_INVALID,
            RxEnding::NoObjects => EXIT_RX_NO_OBJECTS,
        }
    }
}

/// Drain failure, kept typed so `ServeError` can be classified before it is
/// erased into an `anyhow::Error`.
enum DrainFail {
    Serve(moq_transport::serve::ServeError),
    NonSubgroup,
}

impl From<moq_transport::serve::ServeError> for DrainFail {
    fn from(e: moq_transport::serve::ServeError) -> Self {
        DrainFail::Serve(e)
    }
}

/// How one track's drain ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TrackEnd {
    /// End-of-track: the reader ran out cleanly (`Ok(None)`) or the publisher
    /// signalled `Done`. This is the only unambiguous "publisher finished".
    Fin,
    /// `Cancel` / `Closed(_)`. **Ambiguous by itself.** `ServeError::Cancel` is
    /// the library's generic teardown state — `subscribed.rs:746`,
    /// `published.rs:261` and the `serve/track.rs` lock failures all use it — so
    /// a relay or session collapse surfaces here just as a deliberate
    /// unsubscribe would. It must never be treated as success on its own; the
    /// caller resolves it with the session state and the expected counts.
    Cancelled,
    /// A genuine failure.
    Failed,
}

/// Classify a drain result into a track ending.
///
/// Only `Done` is accepted as a FIN. Mapping `Cancel` to "normal" was a real
/// defect: on relay death both drains observe `Cancel` and can win the
/// `select!` against the `session_run` arm, so a failed run reported
/// `ending=normal, rc=0`.
fn classify_track_end(e: &moq_transport::serve::ServeError) -> TrackEnd {
    use moq_transport::serve::ServeError;
    match e {
        ServeError::Done => TrackEnd::Fin,
        ServeError::Cancel | ServeError::Closed(_) => TrackEnd::Cancelled,
        _ => TrackEnd::Failed,
    }
}

/// Per-track drain result, carried to the ending decision.
#[derive(Debug, Clone)]
struct DrainReport {
    name: &'static str,
    end: TrackEnd,
    received: u64,
    /// Design expectation for this track, when the caller supplied one.
    expected: Option<u64>,
    detail: String,
}

impl DrainReport {
    /// Whether this track received everything the design says it should.
    /// A track disabled by C3 expects 0 and is trivially complete.
    fn complete(&self) -> Option<bool> {
        self.expected.map(|e| self.received >= e)
    }

    fn end_str(&self) -> &'static str {
        match self.end {
            TrackEnd::Fin => "fin",
            TrackEnd::Cancelled => "cancelled",
            TrackEnd::Failed => "failed",
        }
    }

    /// JSON object for the shutdown record.
    fn to_json(&self) -> String {
        let expected = match self.expected {
            Some(e) => e.to_string(),
            None => "null".to_string(),
        };
        let complete = match self.complete() {
            Some(c) => c.to_string(),
            None => "null".to_string(),
        };
        format!(
            "{{\"name\":\"{}\",\"end\":\"{}\",\"received\":{},\"expected\":{},\"complete\":{}}}",
            self.name,
            self.end_str(),
            self.received,
            expected,
            complete
        )
    }
}

/// Render the per-track reports as a JSON array for the shutdown record.
///
/// This is deliberately *data*, not a verdict. A FIN carrying fewer objects
/// than the design expects is ambiguous at this layer: a lossy link and a
/// publisher that died early both look identical from the receiver, because the
/// relay turns a publisher disconnect into a clean end-of-track. Only the
/// sender's log says how many objects were actually generated, so the
/// completeness verdict belongs to the analyzer, which has both logs.
fn tracks_json(reports: &[DrainReport]) -> String {
    let items: Vec<String> = reports.iter().map(|r| r.to_json()).collect();
    format!("[{}]", items.join(","))
}

/// Decide the run's ending from the per-track reports and the session state.
///
/// Rule order matters, most-severe first:
///   1. any genuine drain failure -> `DrainError`;
///   2. session already finished -> `SessionEnded` (this is the case that used
///      to be masked by a `Cancelled` drain winning the race);
///   3. any `Cancelled` track -> resolved by completeness. Complete against the
///      design expectation means the publisher really was done, so `Normal`;
///      otherwise `CancelledIncomplete`. With no expectation supplied we cannot
///      prove completeness, so we must not claim `Normal`;
///   4. otherwise `Normal`.
///
/// The chosen rule is written into `detail` so a run can be re-classified after
/// the fact without rerunning it.
fn classify_ending(reports: &[DrainReport], session_finished: bool) -> (RxEnding, String) {
    classify_ending_with_timeout(reports, session_finished, false)
}

fn classify_ending_with_timeout(
    reports: &[DrainReport],
    session_finished: bool,
    allow_empty_pc_timeout: bool,
) -> (RxEnding, String) {
    if let Some(r) = reports.iter().find(|r| r.end == TrackEnd::Failed) {
        return (
            RxEnding::DrainError,
            format!("rule=drain_failed track={} detail={}", r.name, r.detail),
        );
    }
    if session_finished {
        return (RxEnding::SessionEnded, "rule=session_finished".to_string());
    }
    // Zero objects on a track that was expected to carry some is a total
    // failure of that subscription — no loss rate explains it — so unlike a
    // partial shortfall it is NOT ambiguous and must not pass. This is the
    // backstop for the announce/subscribe race, where the publisher accepts the
    // subscribe and then fails to serve it, which no subscribe-side retry can
    // observe.
    //
    // Boundaries, all deliberate:
    //   * expected == 0 (a C3 disabled track) never trips this;
    //   * expected unknown (no --duration-s) never trips this;
    //   * 0 < received < expected is left to the analyzer, because a lossy link
    //     and a publisher that died early are genuinely indistinguishable here
    //     and failing it would break every LOSS condition in the matrix.
    let dead: Vec<String> = reports
        .iter()
        .filter(|r| {
            r.received == 0
                && r.expected.unwrap_or(0) > 0
                && !(allow_empty_pc_timeout && r.name == "pc")
        })
        .map(|r| format!("{}=0/{}", r.name, r.expected.unwrap_or(0)))
        .collect();
    if !dead.is_empty() {
        return (
            RxEnding::NoObjects,
            format!("rule=zero_objects_expected {}", dead.join(",")),
        );
    }
    let cancelled: Vec<&DrainReport> = reports
        .iter()
        .filter(|r| r.end == TrackEnd::Cancelled)
        .collect();
    if !cancelled.is_empty() {
        let names: Vec<&str> = cancelled.iter().map(|r| r.name).collect();
        let unknown: Vec<&str> = cancelled
            .iter()
            .filter(|r| r.complete().is_none())
            .map(|r| r.name)
            .collect();
        if !unknown.is_empty() {
            return (
                RxEnding::CancelledIncomplete,
                format!(
                    "rule=cancel_without_expectation tracks={} (pass --duration-s to resolve)",
                    unknown.join("+")
                ),
            );
        }
        let short: Vec<String> = cancelled
            .iter()
            .filter(|r| r.complete() == Some(false))
            .map(|r| format!("{}={}/{}", r.name, r.received, r.expected.unwrap_or(0)))
            .collect();
        if !short.is_empty() {
            return (
                RxEnding::CancelledIncomplete,
                format!("rule=cancel_incomplete {}", short.join(",")),
            );
        }
        return (
            RxEnding::Normal,
            format!("rule=cancel_but_complete tracks={}", names.join("+")),
        );
    }
    if allow_empty_pc_timeout
        && reports
            .iter()
            .any(|r| r.name == "pc" && r.received == 0 && r.expected.unwrap_or(0) > 0)
    {
        return (
            RxEnding::Normal,
            "rule=fin pc_empty_requires_timeout_accounting".to_string(),
        );
    }
    (RxEnding::Normal, "rule=fin".to_string())
}

/// Minimal escaping so a free-form error string stays valid inside the JSON
/// `detail` field.
fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

#[derive(Debug)]
enum S3WireEvent {
    Object(RoutedObject),
    Ended {
        role: TrackRole,
        route: Route,
        end: TrackEnd,
        detail: String,
    },
}

struct S3LiveSubscription {
    handle: Subscribe,
    drain: tokio::task::JoinHandle<()>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct S3ObjectKey {
    track_id: u8,
    tier: u16,
    seq: u32,
    pts_us: u64,
    event_id: u32,
}

impl From<&PlayoutObject> for S3ObjectKey {
    fn from(object: &PlayoutObject) -> Self {
        Self {
            track_id: object.header.track_id,
            tier: object.header.tier,
            seq: object.header.seq,
            pts_us: object.header.pts_us,
            event_id: object.header.event_id,
        }
    }
}

async fn drain_s3_track(
    role: TrackRole,
    route: Route,
    received_track: TrackReader,
    logger: Arc<Mutex<JsonlLogger>>,
    events: mpsc::Sender<S3WireEvent>,
    bad_headers: Arc<AtomicU64>,
    ingress_drops: Arc<AtomicU64>,
    log_failed: Arc<AtomicU64>,
) {
    let result = async {
        let mut subgroups = match received_track.mode().await? {
            TrackReaderMode::Subgroups(subgroups) => subgroups,
            _ => return Err(DrainFail::NonSubgroup),
        };
        while let Some(mut subgroup) = subgroups.next().await? {
            while let Some(bytes) = subgroup.read_next().await? {
                let t_recv = now_us();
                let Some(header) = unpack_header(&bytes) else {
                    bad_headers.fetch_add(1, Ordering::Relaxed);
                    continue;
                };
                if header.version != VERSION || bytes.len() != HDR + header.payload_len as usize {
                    bad_headers.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                let routed = RoutedObject {
                    role,
                    route,
                    object: PlayoutObject {
                        header,
                        t_recv,
                        bytes,
                    },
                };
                if validate_routed_object(&routed).is_err() {
                    bad_headers.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                if is_warmup_seq(header.seq) {
                    if logger
                        .lock()
                        .map_err(|_| DrainFail::NonSubgroup)?
                        .try_log_warmup_rx(
                            role.as_str(),
                            header.tier,
                            header.seq,
                            header.pts_us,
                            header.event_id,
                            header.payload_len,
                            header.gen_ts_us,
                            t_recv,
                        )
                        .is_err()
                    {
                        log_failed.fetch_add(1, Ordering::Relaxed);
                    }
                    continue;
                }
                if logger
                    .lock()
                    .map_err(|_| DrainFail::NonSubgroup)?
                    .try_log_rx_s3(
                        role,
                        route,
                        header.tier,
                        header.seq,
                        header.pts_us,
                        header.event_id,
                        header.payload_len,
                        t_recv,
                        t_recv,
                        header.gen_ts_us,
                    )
                    .is_err()
                {
                    log_failed.fetch_add(1, Ordering::Relaxed);
                }
                match events.try_send(S3WireEvent::Object(routed)) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(S3WireEvent::Object(routed))) => {
                        ingress_drops.fetch_add(1, Ordering::Relaxed);
                        let header = routed.object.header;
                        if logger
                            .lock()
                            .map_err(|_| DrainFail::NonSubgroup)?
                            .try_log_drop_s3(
                                role,
                                route,
                                header.tier,
                                header.seq,
                                header.pts_us,
                                header.event_id,
                                now_us().max(routed.object.t_recv),
                                "ingress_queue_full",
                            )
                            .is_err()
                        {
                            log_failed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => return Ok(()),
                    Err(mpsc::error::TrySendError::Full(S3WireEvent::Ended { .. })) => {
                        unreachable!("object send returned a non-object")
                    }
                }
            }
        }
        Ok::<(), DrainFail>(())
    }
    .await;

    let (end, detail) = match result {
        Ok(()) => (TrackEnd::Fin, String::new()),
        Err(DrainFail::Serve(error)) => (classify_track_end(&error), error.to_string()),
        Err(DrainFail::NonSubgroup) => (
            TrackEnd::Failed,
            "invalid S3 subgroup/log state".to_string(),
        ),
    };
    let _ = events
        .send(S3WireEvent::Ended {
            role,
            route,
            end,
            detail,
        })
        .await;
}

#[allow(clippy::too_many_arguments)]
async fn open_s3_subscription(
    subscriber: &mut Subscriber,
    namespace: &TrackNamespace,
    role: TrackRole,
    route: Route,
    initial: bool,
    retry_limit: u32,
    args: &Args,
    logger: Arc<Mutex<JsonlLogger>>,
    events: mpsc::Sender<S3WireEvent>,
    bad_headers: Arc<AtomicU64>,
    ingress_drops: Arc<AtomicU64>,
    log_failed: Arc<AtomicU64>,
) -> Result<(S3LiveSubscription, u64, u32)> {
    let started = tokio::time::Instant::now();
    let deadline = started + Duration::from_secs_f64(args.subscribe_timeout);
    let mut retries = 0u32;
    loop {
        let (writer, reader) = Track::new(namespace.clone(), route.name).produce();
        let mut params = KeyValuePairs::default();
        if role == TrackRole::Pc {
            params.set_delivery_timeout(
                args.pc_delivery_timeout_ms
                    .expect("validated S3 PC delivery timeout"),
            );
        }
        if !initial {
            params
                .set_subscription_filter(&SubscriptionFilter::next_group_start())
                .context("set S3 NextGroupStart filter")?;
        }
        match subscriber.subscribe_open_with_params(writer, params).await {
            Ok(handle) => {
                let t_ok = now_us();
                let drain = tokio::spawn(drain_s3_track(
                    role,
                    route,
                    reader,
                    logger,
                    events,
                    bad_headers,
                    ingress_drops,
                    log_failed,
                ));
                return Ok((S3LiveSubscription { handle, drain }, t_ok, retries));
            }
            Err(error)
                if is_retryable_subscribe_error(&error)
                    && retries < retry_limit
                    && tokio::time::Instant::now() < deadline =>
            {
                retries += 1;
                tokio::time::sleep(Duration::from_millis(args.subscribe_retry_ms)).await;
            }
            Err(error) => {
                bail!(
                    "S3 subscribe {} generation {} failed after {} retries: {}",
                    route.name,
                    route.generation,
                    retries,
                    error
                );
            }
        }
    }
}

/// Reconcile the deadline tracker with one batch of terminal scheduler
/// actions: record releases, then forget every object that was terminally
/// dropped while no epoch existed.
///
/// S3 FSM 구현계약 §2.1 규칙 B·C. Such an object never had a valid deadline and
/// can never be re-pushed or released, so the only observation it could still
/// produce is a retroactively fabricated deadline miss — the forced first
/// `Normal -> Haptic-Critical` transition. The drop reason is deliberately NOT
/// consulted: the scheduler tags the epoch state at emission time
/// (`had_epoch`) because `push` can emit a pre-epoch buffer-limit drop in the
/// same call that forms the epoch.
///
/// Extracted so the tests drive this exact code rather than a copy of it.
fn settle_tracker_batch(
    tracker: &mut S3DeadlineTracker,
    actions: &[PlayoutAction],
    now: u64,
) -> Result<(), &'static str> {
    tracker.note_actions(actions, now)?;
    for action in actions {
        if action.is_pre_epoch_drop() {
            tracker.forget_evicted(action.object())?;
        }
    }
    Ok(())
}

/// Which stage of [`advance_tracker_checked`] failed.
///
/// The two stages fail for unrelated reasons — a lost scheduler epoch is an
/// `advance` invariant break, an occupancy overflow is a bound violation — and
/// only the latter warrants an `s3_tracker_bound` record. Merging them into one
/// `&'static str` would misattribute the cause in the run's own log.
#[derive(Debug)]
enum TrackerStepError {
    Advance(&'static str),
    Bound(&'static str),
}

impl TrackerStepError {
    fn message(&self) -> &'static str {
        match self {
            Self::Advance(message) | Self::Bound(message) => message,
        }
    }
}

/// Expire every due anchor and then check the occupancy bound (P2).
///
/// The order is the point: the bound is evaluated on settled occupancy, after
/// this iteration's [`settle_tracker_batch`] and after expiry, never on a
/// mid-batch transient. Extracted for the same reason as above.
fn advance_tracker_checked(
    tracker: &mut S3DeadlineTracker,
    scheduler: &PlayoutScheduler,
    now: u64,
) -> Result<Vec<S3Observation>, TrackerStepError> {
    let observations = tracker
        .advance(scheduler, now)
        .map_err(TrackerStepError::Advance)?;
    tracker.check_bounds().map_err(TrackerStepError::Bound)?;
    Ok(observations)
}

fn dispatch_s3_playout_actions(
    actions: Vec<PlayoutAction>,
    routes: &mut HashMap<S3ObjectKey, (TrackRole, Route)>,
    tracker: &mut S3DeadlineTracker,
    logger: &Arc<Mutex<JsonlLogger>>,
    bridge: Option<&mpsc::Sender<Bytes>>,
    render: bool,
    audio: bool,
    stats: &mut PlayoutStats,
    now: u64,
) -> Result<()> {
    settle_tracker_batch(tracker, &actions, now).map_err(anyhow::Error::msg)?;
    for action in actions {
        let object = action.object();
        let header = object.header;
        let key = S3ObjectKey::from(object);
        let (role, route) = routes
            .remove(&key)
            .context("missing S3 route for terminal scheduler action")?;
        match action {
            PlayoutAction::Release(object) => {
                logger
                    .lock()
                    .map_err(|_| anyhow::anyhow!("RX logger poisoned"))?
                    .try_log_release_s3(
                        role,
                        route,
                        header.tier,
                        header.seq,
                        header.pts_us,
                        header.event_id,
                        now.max(object.t_recv),
                    )?;
                stats.released += 1;
                let observe = (header.track_id == TRACK_PC && render)
                    || (header.track_id == TRACK_HAPTIC && audio);
                if observe {
                    if let Some(bridge) = bridge {
                        if bridge.try_send(object.bytes).is_err() {
                            stats.bridge_observer_dropped += 1;
                        }
                    }
                }
            }
            PlayoutAction::Drop { object, reason, .. } => {
                logger
                    .lock()
                    .map_err(|_| anyhow::anyhow!("RX logger poisoned"))?
                    .try_log_drop_s3(
                        role,
                        route,
                        header.tier,
                        header.seq,
                        header.pts_us,
                        header.event_id,
                        now.max(object.t_recv),
                        reason,
                    )?;
                stats.dropped += 1;
            }
        }
    }
    Ok(())
}

/// At atomic S3 apply, terminally evict cancelled-generation objects with
/// `pts_us` beyond the exact-pair barrier from BOTH places they can exist
/// within one ingress batch:
///
/// 1. already staged in `scheduler_actions` — the ingress emits
///    `Scheduler(routed)` before `Applied`, and `scheduler.push` for that
///    earlier event internally advances the timeline, so an ALREADY-OVERDUE
///    old-route object can be emitted as a `Release` before the apply is
///    handled. Such staged releases are converted in place into terminal
///    `stale_tier` drops, marked terminal in the scheduler, and forgotten by
///    the deadline tracker exactly like buffered evictions;
/// 2. still buffered in the scheduler — evicted via `drop_matching` as
///    before.
///
/// Staged drops are already terminal and stay untouched, which also makes a
/// repeat of the same cancellation idempotent (a converted object cannot be
/// converted or counted again). Objects at or below the barrier PTS and
/// objects of non-cancelled routes pass through unchanged. Every evicted
/// object therefore yields exactly one terminal drop record, zero release
/// records, and no retained deadline-tracker observation.
fn apply_s3_route_barrier(
    applied: &SwitchApplied,
    scheduler: &mut PlayoutScheduler,
    scheduler_actions: &mut Vec<PlayoutAction>,
    object_routes: &HashMap<S3ObjectKey, (TrackRole, Route)>,
    tracker: &mut S3DeadlineTracker,
) -> Result<(), &'static str> {
    for (role, route) in [
        (TrackRole::Pc, applied.cancel_pc),
        (TrackRole::Haptic, applied.cancel_haptic),
    ] {
        let Some(route) = route else { continue };
        let cancelled = |object: &PlayoutObject| {
            object.header.pts_us > applied.exact_pts_us
                && object_routes
                    .get(&S3ObjectKey::from(object))
                    .is_some_and(|stored| *stored == (role, route))
        };
        for action in scheduler_actions.iter_mut() {
            let convert = match &*action {
                PlayoutAction::Release(object) => cancelled(object),
                PlayoutAction::Drop { .. } => false,
            };
            if !convert {
                continue;
            }
            let PlayoutAction::Release(object) = action else {
                unreachable!("convert selects only staged releases");
            };
            let object = object.clone();
            scheduler.mark_terminal(&object);
            tracker.forget_evicted(&object)?;
            *action = PlayoutAction::Drop {
                object,
                reason: DROP_STALE_TIER,
                // A staged Release implies the epoch already exists; read the
                // scheduler rather than hard-coding it.
                had_epoch: scheduler.is_started(),
            };
        }
        let evicted = scheduler.drop_matching(DROP_STALE_TIER, cancelled);
        for action in &evicted {
            tracker.forget_evicted(action.object())?;
        }
        scheduler_actions.extend(evicted);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn request_s3_switch(
    update: S3Update,
    ingress: &mut S3ReceiverIngress,
    subscriber: &mut Subscriber,
    namespace: &TrackNamespace,
    live: &mut HashMap<(TrackRole, u64), S3LiveSubscription>,
    config: &S3RuntimeConfig,
    args: &Args,
    logger: Arc<Mutex<JsonlLogger>>,
    events: mpsc::Sender<S3WireEvent>,
    bad_headers: Arc<AtomicU64>,
    ingress_drops: Arc<AtomicU64>,
    log_failed: Arc<AtomicU64>,
) -> Result<bool> {
    let Some(transition) = update.transition else {
        return Ok(false);
    };
    let request_at = now_us().max(transition.at_us);
    {
        let mut logger = logger
            .lock()
            .map_err(|_| anyhow::anyhow!("RX logger poisoned"))?;
        logger.try_log_s3_transition(transition, update.snapshot)?;
    }
    let request = ingress
        .request(transition, request_at)
        .map_err(|error| anyhow::anyhow!("request S3 switch: {error:?}"))?;
    logger
        .lock()
        .map_err(|_| anyhow::anyhow!("RX logger poisoned"))?
        .try_log_s3_switch_request(request)?;

    for role in [TrackRole::Pc, TrackRole::Haptic] {
        let changed = match role {
            TrackRole::Pc => request.pc_changed,
            TrackRole::Haptic => request.haptic_changed,
        };
        if !changed {
            continue;
        }
        let route = request.target.for_role(role);
        let (subscription, t_ok, retries) = open_s3_subscription(
            subscriber,
            namespace,
            role,
            route,
            false,
            config.switch_retry_limit,
            args,
            logger.clone(),
            events.clone(),
            bad_headers.clone(),
            ingress_drops.clone(),
            log_failed.clone(),
        )
        .await?;
        if let Err(error) = ingress.check_timeout(t_ok) {
            drop(subscription.handle);
            subscription.drain.abort();
            let _ = subscription.drain.await;
            return Err(anyhow::anyhow!(
                "S3 switch timed out while subscribing: {error:?}"
            ));
        }
        if live.contains_key(&(role, route.generation)) {
            drop(subscription.handle);
            subscription.drain.abort();
            let _ = subscription.drain.await;
            return Err(anyhow::anyhow!(
                "duplicate live S3 subscription {} generation {}",
                route.name,
                route.generation
            ));
        }
        live.insert((role, route.generation), subscription);
        ingress
            .subscribe_ok(role, t_ok)
            .map_err(|error| anyhow::anyhow!("record S3 SUBSCRIBE_OK: {error:?}"))?;
        {
            let mut logger = logger
                .lock()
                .map_err(|_| anyhow::anyhow!("RX logger poisoned"))?;
            logger.try_log_s3_subscribe_ok(request, role, t_ok)?;
            logger.try_log_info(&format!(
                "\"event\":\"s3_subscribe\",\"track\":\"{}\",\"route_generation\":{},\"retries\":{}",
                role.as_str(),
                route.generation,
                retries
            ))?;
        }
    }
    Ok(true)
}

fn min_wakeup(values: impl IntoIterator<Item = Option<u64>>) -> Option<u64> {
    values.into_iter().flatten().min()
}

/// Lifecycle defects while moving a cancelled route into retirement. Both are
/// fail-loud; `Duplicate` returns the payload so the caller can release it.
#[derive(Debug)]
enum RetireError<T> {
    Missing,
    Duplicate(T),
}

/// At atomic S3 apply, move the cancelled route's live wire subscription into
/// the bounded retirement queue instead of unsubscribing immediately. The
/// route is already stale for scheduling (every later object of it terminally
/// drops as `stale_tier`), so deferral changes no release decision; it only
/// keeps the delivery/timeout accounting path open for in-flight
/// pre-/at-barrier objects.
fn retire_cancelled_s3_route<T>(
    live: &mut HashMap<(TrackRole, u64), T>,
    retiring: &mut S3RetirementQueue<T>,
    role: TrackRole,
    route: Route,
    now: u64,
) -> Result<(), RetireError<T>> {
    let Some(old) = live.remove(&(role, route.generation)) else {
        return Err(RetireError::Missing);
    };
    retiring
        .admit(role, route, old, now)
        .map_err(|(_reason, old)| RetireError::Duplicate(old))
}

/// Release one retiring old-route subscription: dropping the wire handle sends
/// the deferred UNSUBSCRIBE, and the drain task joins the existing retired
/// pool for shutdown. The registered cancel record was already written at
/// apply time; this only records when and why the wire release happened.
fn release_retired_s3_subscription(
    role: TrackRole,
    route: Route,
    subscription: S3LiveSubscription,
    cause: RetirementCause,
    now: u64,
    logger: &Arc<Mutex<JsonlLogger>>,
    retired_drains: &mut Vec<tokio::task::JoinHandle<()>>,
) -> Result<()> {
    drop(subscription.handle);
    retired_drains.push(subscription.drain);
    logger
        .lock()
        .map_err(|_| anyhow::anyhow!("RX logger poisoned"))?
        .try_log_info(&format!(
            "\"event\":\"s3_retired_unsubscribe\",\"track\":\"{}\",\"wire_track\":\"{}\",\"generation\":{},\"cause\":\"{}\",\"t_unsubscribe\":{now}",
            role.as_str(),
            route.name,
            route.generation,
            cause.as_str(),
        ))?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_s3_receiver(
    args: &Args,
    playout: PlayoutConfig,
    runtime: S3RuntimeConfig,
    logger: Arc<Mutex<JsonlLogger>>,
) -> Result<()> {
    let mut controller = S3Controller::new(runtime.controller).map_err(anyhow::Error::msg)?;
    let gate = S3SwitchGate::new(runtime.switch)
        .map_err(|error| anyhow::anyhow!("create S3 switch gate: {error:?}"))?;
    logger
        .lock()
        .map_err(|_| anyhow::anyhow!("RX logger poisoned"))?
        .try_log_s3_config(&controller, &gate)?;
    logger
        .lock()
        .map_err(|_| anyhow::anyhow!("RX logger poisoned"))?
        .try_log_info(&format!(
            "\"event\":\"s3_runtime\",\"initial_retry_limit\":{},\"switch_retry_limit\":{},\"barrier_max_objects_per_role\":{},\"deadline_max_anchors\":{},\"test_mode\":{},\"test_force_misses_after_us\":{}",
            runtime.initial_retry_limit,
            runtime.switch_retry_limit,
            playout.max_objects_per_track,
            runtime.deadline_max_anchors,
            runtime.test_force_misses_after_us.is_some(),
            runtime
                .test_force_misses_after_us
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string()),
        ))?;
    let mut ingress =
        S3ReceiverIngress::new(gate, playout.max_objects_per_track).map_err(anyhow::Error::msg)?;
    let mut tracker =
        S3DeadlineTracker::new(runtime.deadline_max_anchors).map_err(anyhow::Error::msg)?;
    let mut scheduler = PlayoutScheduler::new(playout).map_err(anyhow::Error::msg)?;
    let mut stats = PlayoutStats::default();
    let mut object_routes: HashMap<S3ObjectKey, (TrackRole, Route)> = HashMap::new();

    let (session, mut subscriber) = {
        let (webtransport, transport) = establish(&args).await.context("establish S3 session")?;
        session_handshake(&args, webtransport, transport)
            .await
            .context("S3 SETUP")?
    };
    let mut session_run = tokio::spawn(session.run());
    let namespace = TrackNamespace::from_utf8_path(&args.run_id);

    // 직결 토폴로지에서는 이 수신자가 송신자의 피어다 — subscribe 하기 전에
    // 송신자의 PUBLISH_NAMESPACE 에 먼저 응답해야 한다.
    // 핸들은 런이 끝날 때까지 살려 둔다(drop = PUBLISH_NAMESPACE_CANCEL).
    let _announce_guard: Option<PublishedNamespace> = if args.listen.is_some() {
        Some(
            ack_published_namespace(&mut subscriber, &namespace, args.subscribe_timeout)
                .await
                .context("직결 토폴로지 announce 응답")?,
        )
    } else {
        None
    };

    let event_capacity = playout
        .max_objects_per_track
        .checked_mul(4)
        .context("S3 ingress capacity overflow")?;
    let (event_tx, mut event_rx) = mpsc::channel::<S3WireEvent>(event_capacity);
    let bad_headers = Arc::new(AtomicU64::new(0));
    let ingress_drops = Arc::new(AtomicU64::new(0));
    let log_failed = Arc::new(AtomicU64::new(0));
    let recv_pc = Arc::new(AtomicU64::new(0));
    let recv_haptic = Arc::new(AtomicU64::new(0));

    let mut bridge_child = None;
    let bridge_tx: Option<mpsc::Sender<Bytes>> = if args.render || args.audio {
        let mut command = Command::new(&args.python);
        command.arg("tools/stage_a_bridge.py");
        if args.render {
            command.arg("--render");
        }
        if args.audio {
            command.arg("--audio");
        }
        if args.draco {
            command.arg("--draco");
        }
        command
            .arg("--title")
            .arg(format!("skew live — {}", args.run_id))
            .stdin(Stdio::piped());
        let mut child = command.spawn().context("spawn S3 stage_a_bridge")?;
        let mut stdin = child.stdin.take().context("open S3 bridge stdin")?;
        let (tx, mut rx) = mpsc::channel::<Bytes>(64);
        tokio::spawn(async move {
            while let Some(bytes) = rx.recv().await {
                if stdin.write_all(&bytes).await.is_err() {
                    break;
                }
            }
            let _ = stdin.flush().await;
        });
        bridge_child = Some(child);
        Some(tx)
    } else {
        None
    };

    let mut live: HashMap<(TrackRole, u64), S3LiveSubscription> = HashMap::new();
    let mut retired_drains = Vec::new();
    // Registered §v8 barrier semantics make the old generation stale at apply,
    // but the wire UNSUBSCRIBE is deferred by the frozen 67ms PC delivery
    // timeout so in-flight pre-/at-barrier objects resolve through the normal
    // delivery/timeout contract instead of being orphaned by a relay
    // hard-stop (v11 single-frame unaccounted defect).
    let mut retiring: S3RetirementQueue<S3LiveSubscription> = S3RetirementQueue::new(ms_to_us(
        args.pc_delivery_timeout_ms
            .context("S3 route retirement requires --pc-delivery-timeout-ms")?,
        "pc-delivery-timeout-ms",
    )?)
    .map_err(anyhow::Error::msg)?;
    let initial = ingress.gate().active_routes();
    for (role, route) in [
        (TrackRole::Pc, initial.pc),
        (TrackRole::Haptic, initial.haptic),
    ] {
        let (subscription, t_ok, retries) = open_s3_subscription(
            &mut subscriber,
            &namespace,
            role,
            route,
            true,
            runtime.initial_retry_limit,
            args,
            logger.clone(),
            event_tx.clone(),
            bad_headers.clone(),
            ingress_drops.clone(),
            log_failed.clone(),
        )
        .await?;
        live.insert((role, route.generation), subscription);
        logger
            .lock()
            .map_err(|_| anyhow::anyhow!("RX logger poisoned"))?
            .try_log_info(&format!(
                "\"event\":\"s3_initial_subscribe\",\"track\":\"{}\",\"route_generation\":0,\"t_subscribe_ok\":{},\"retries\":{}",
                role.as_str(),
                t_ok,
                retries
            ))?;
    }
    println!("[rx] S3 subscribed Normal pc+haptic on {}", args.run_id);
    let readiness_components: &[&str] = match args.topology {
        Topology::Relay => &[
            "sender_relay_session",
            "relay_receiver_session",
            "pc_subscription",
            "haptic_subscription",
        ],
        Topology::Direct => &[
            "sender_receiver_session",
            "pc_subscription",
            "haptic_subscription",
        ],
    };
    logger
        .lock()
        .map_err(|_| anyhow::anyhow!("RX logger poisoned"))?
        .log_readiness(now_us(), readiness_components)
        .context("failed to record S3 readiness components")?;

    let max_end_us = now_us()
        .checked_add(Duration::from_secs_f64(args.max_duration).as_micros() as u64)
        .context("S3 receiver max-duration overflow")?;
    let mut current_finished = [false, false];
    let mut normal_end = false;
    let mut outcome_error: Option<anyhow::Error> = None;
    let mut controller_active_at_us: Option<u64> = None;
    let mut forced_misses_injected = false;

    while !normal_end && outcome_error.is_none() {
        let now = now_us();
        let pending_at_start = ingress.gate().pending_request().is_some();
        let switch_deadline = ingress.gate().pending_request().map(|request| {
            request
                .request_at_us
                .saturating_add(runtime.switch.effect_timeout_us)
                .saturating_add(1)
        });
        let forced_miss_wakeup = match (
            forced_misses_injected,
            controller_active_at_us,
            runtime.test_force_misses_after_us,
        ) {
            (false, Some(active_at), Some(delay)) => Some(active_at.saturating_add(delay)),
            _ => None,
        };
        let wakeup = min_wakeup([
            scheduler.next_wakeup_us(),
            tracker.next_wakeup_us(&scheduler),
            switch_deadline,
            forced_miss_wakeup,
            retiring.next_deadline_us(),
            Some(max_end_us),
        ])
        .unwrap_or(max_end_us);
        let wait = Duration::from_micros(wakeup.saturating_sub(now));

        let event = tokio::select! {
            event = event_rx.recv() => event,
            result = &mut session_run => {
                outcome_error = Some(anyhow::anyhow!("S3 session ended early: {result:?}"));
                None
            }
            _ = tokio::time::sleep(wait) => None,
        };
        let now = now_us();
        if now >= max_end_us {
            outcome_error = Some(anyhow::anyhow!("S3 receiver max-duration reached"));
            break;
        }

        // Complete route retirements whose bounded drain window has passed.
        for (role, route, subscription) in retiring.take_due(now) {
            if let Err(error) = release_retired_s3_subscription(
                role,
                route,
                subscription,
                RetirementCause::Deadline,
                now,
                &logger,
                &mut retired_drains,
            ) {
                outcome_error = Some(error);
            }
        }
        if outcome_error.is_some() {
            continue;
        }

        let mut scheduler_actions = Vec::new();
        if let Some(event) = event {
            match event {
                S3WireEvent::Object(routed) => {
                    match routed.role {
                        TrackRole::Pc => {
                            recv_pc.fetch_add(1, Ordering::Relaxed);
                        }
                        TrackRole::Haptic => {
                            recv_haptic.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    let ingress_events = match ingress.push(routed, now) {
                        Ok(events) => events,
                        Err(error) => {
                            outcome_error =
                                Some(anyhow::anyhow!("S3 ingress validation failed: {error}"));
                            continue;
                        }
                    };
                    for ingress_event in ingress_events {
                        match ingress_event {
                            IngressEvent::Scheduler(routed) => {
                                // A duplicate wire copy of an identity the
                                // scheduler already knows (terminal, or still
                                // buffered from another route) would be
                                // silently ignored by `scheduler.push`, so it
                                // would never produce a terminal action and
                                // its route/tracker registrations would leak
                                // into shutdown residue. Terminally drop it
                                // here BEFORE any registration.
                                if scheduler.knows_identity(&routed.object) {
                                    let header = routed.object.header;
                                    if let Err(error) = logger
                                        .lock()
                                        .map_err(|_| anyhow::anyhow!("RX logger poisoned"))
                                        .and_then(|mut logger| {
                                            logger
                                                .try_log_drop_s3(
                                                    routed.role,
                                                    routed.route,
                                                    header.tier,
                                                    header.seq,
                                                    header.pts_us,
                                                    header.event_id,
                                                    now.max(routed.object.t_recv),
                                                    DROP_DUPLICATE_IDENTITY,
                                                )
                                                .map_err(anyhow::Error::from)
                                        })
                                    {
                                        outcome_error = Some(error);
                                        break;
                                    }
                                    stats.dropped += 1;
                                    continue;
                                }
                                if let Err(error) = tracker.note_received(&routed.object) {
                                    outcome_error = Some(anyhow::anyhow!(error));
                                    break;
                                }
                                let key = S3ObjectKey::from(&routed.object);
                                if object_routes
                                    .insert(key, (routed.role, routed.route))
                                    .is_some()
                                {
                                    outcome_error =
                                        Some(anyhow::anyhow!("duplicate S3 scheduler identity"));
                                    break;
                                }
                                scheduler_actions.extend(scheduler.push(routed.object, now));
                            }
                            IngressEvent::Drop { routed, reason } => {
                                let header = routed.object.header;
                                if let Err(error) = logger
                                    .lock()
                                    .map_err(|_| anyhow::anyhow!("RX logger poisoned"))
                                    .and_then(|mut logger| {
                                        logger
                                            .try_log_drop_s3(
                                                routed.role,
                                                routed.route,
                                                header.tier,
                                                header.seq,
                                                header.pts_us,
                                                header.event_id,
                                                now.max(routed.object.t_recv),
                                                reason,
                                            )
                                            .map_err(anyhow::Error::from)
                                    })
                                {
                                    outcome_error = Some(error);
                                    break;
                                }
                                stats.dropped += 1;
                            }
                            IngressEvent::Applied(applied) => {
                                // The ingress gate prevents *new* stale objects,
                                // but old-route objects may already be in the
                                // common playout scheduler, or already staged
                                // as actions by an earlier `scheduler.push` in
                                // this same ingress batch. At atomic apply,
                                // terminally evict only cancelled-generation
                                // objects beyond the exact-pair barrier from
                                // both places. Older slots may finish on their
                                // frozen deadlines.
                                if let Err(error) = apply_s3_route_barrier(
                                    &applied,
                                    &mut scheduler,
                                    &mut scheduler_actions,
                                    &object_routes,
                                    &mut tracker,
                                ) {
                                    outcome_error = Some(anyhow::anyhow!(error));
                                    break;
                                }
                                let log_result = logger
                                    .lock()
                                    .map_err(|_| anyhow::anyhow!("RX logger poisoned"))
                                    .and_then(|mut logger| {
                                        logger.try_log_s3_first_effect(applied)?;
                                        logger.try_log_s3_apply(applied)?;
                                        Ok::<(), anyhow::Error>(())
                                    });
                                if let Err(error) = log_result {
                                    outcome_error = Some(error);
                                    break;
                                }
                                for (role, route) in [
                                    (TrackRole::Pc, applied.cancel_pc),
                                    (TrackRole::Haptic, applied.cancel_haptic),
                                ] {
                                    let Some(route) = route else { continue };
                                    current_finished[match role {
                                        TrackRole::Pc => 0,
                                        TrackRole::Haptic => 1,
                                    }] = false;
                                    if let Err(error) = logger
                                        .lock()
                                        .map_err(|_| anyhow::anyhow!("RX logger poisoned"))
                                        .and_then(|mut logger| {
                                            logger
                                                .try_log_s3_cancel(role, route, now)
                                                .map_err(anyhow::Error::from)
                                        })
                                    {
                                        outcome_error = Some(error);
                                        break;
                                    }
                                    // Defer the wire UNSUBSCRIBE for one
                                    // registered PC delivery-timeout window:
                                    // the old generation is already stale for
                                    // scheduling, but in-flight
                                    // pre-/at-barrier objects must still
                                    // resolve as delivery or a relay
                                    // delivery_timeout event, not vanish in a
                                    // relay hard-stop.
                                    match retire_cancelled_s3_route(
                                        &mut live,
                                        &mut retiring,
                                        role,
                                        route,
                                        now,
                                    ) {
                                        Ok(()) => {}
                                        Err(RetireError::Missing) => {
                                            outcome_error = Some(anyhow::anyhow!(
                                                "missing old S3 subscription {} generation {}",
                                                route.name,
                                                route.generation
                                            ));
                                            break;
                                        }
                                        Err(RetireError::Duplicate(old)) => {
                                            drop(old.handle);
                                            old.drain.abort();
                                            retired_drains.push(old.drain);
                                            outcome_error = Some(anyhow::anyhow!(
                                                "duplicate retiring S3 route {} generation {}",
                                                route.name,
                                                route.generation
                                            ));
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                S3WireEvent::Ended {
                    role,
                    route,
                    end,
                    detail,
                } => {
                    // A retiring route that ends (relay FIN/close) has nothing
                    // left in flight: complete its deferred unsubscribe now.
                    // A retiring route is never the current route, so the
                    // current-route end handling below stays unreachable for
                    // it by construction.
                    if let Some(subscription) = retiring.take_ended(role, route) {
                        if let Err(error) = release_retired_s3_subscription(
                            role,
                            route,
                            subscription,
                            RetirementCause::TrackEnd,
                            now,
                            &logger,
                            &mut retired_drains,
                        ) {
                            outcome_error = Some(error);
                        }
                    }
                    let current = ingress.gate().active_routes().for_role(role);
                    if current == route {
                        if end != TrackEnd::Fin {
                            outcome_error = Some(anyhow::anyhow!(
                                "current S3 route {} generation {} ended {:?}: {}",
                                route.name,
                                route.generation,
                                end,
                                detail
                            ));
                        } else {
                            current_finished[match role {
                                TrackRole::Pc => 0,
                                TrackRole::Haptic => 1,
                            }] = true;
                            normal_end = current_finished.iter().all(|finished| *finished)
                                && ingress.gate().pending_request().is_none();
                        }
                    }
                }
            }
        }

        scheduler_actions.extend(scheduler.advance(now));
        if let Err(error) = dispatch_s3_playout_actions(
            scheduler_actions,
            &mut object_routes,
            &mut tracker,
            &logger,
            bridge_tx.as_ref(),
            args.render,
            args.audio,
            &mut stats,
            now,
        ) {
            outcome_error = Some(error);
            continue;
        }
        if scheduler.is_started() && !controller.is_active() {
            if let Err(error) = controller.activate(now) {
                outcome_error = Some(anyhow::anyhow!(error));
                continue;
            }
            if let Err(error) = tracker.activate() {
                outcome_error = Some(anyhow::anyhow!(error));
                continue;
            }
            controller_active_at_us = Some(now);
        }
        if let Err(error) = ingress.check_timeout(now) {
            outcome_error = Some(anyhow::anyhow!("S3 switch timeout: {error:?}"));
            continue;
        }
        if !forced_misses_injected
            && ingress.gate().pending_request().is_none()
            && controller_active_at_us
                .zip(runtime.test_force_misses_after_us)
                .is_some_and(|(active_at, delay)| now >= active_at.saturating_add(delay))
        {
            forced_misses_injected = true;
            let mut final_update = None;
            for _ in 0..3 {
                match controller.observe(S3Observation {
                    now_us: now,
                    deadline_miss: true,
                    abs_skew_us: None,
                }) {
                    Ok(update) => final_update = Some(update),
                    Err(error) => {
                        outcome_error = Some(anyhow::anyhow!(error));
                        break;
                    }
                }
            }
            if outcome_error.is_none() {
                if let Err(error) = logger
                    .lock()
                    .map_err(|_| anyhow::anyhow!("RX logger poisoned"))
                    .and_then(|mut logger| {
                        logger
                            .try_log_info(
                                "\"event\":\"s3_test_forced_deadline_misses\",\"count\":3,\"scientific_eligible\":false",
                            )
                            .map_err(anyhow::Error::from)
                    })
                {
                    outcome_error = Some(error);
                    continue;
                }
                if let Some(update) = final_update {
                    if let Err(error) = request_s3_switch(
                        update,
                        &mut ingress,
                        &mut subscriber,
                        &namespace,
                        &mut live,
                        &runtime,
                        args,
                        logger.clone(),
                        event_tx.clone(),
                        bad_headers.clone(),
                        ingress_drops.clone(),
                        log_failed.clone(),
                    )
                    .await
                    {
                        outcome_error = Some(error);
                        continue;
                    }
                }
            }
        }
        let observations = match advance_tracker_checked(&mut tracker, &scheduler, now) {
            Ok(observations) => observations,
            Err(error) => {
                // Only a bound violation gets the diagnostic record; an
                // `advance` invariant break is a different failure and must not
                // be filed under it. Either way the run ends fail-loud, and the
                // original error survives even if the logging itself fails.
                if let TrackerStepError::Bound(message) = &error {
                    let (pc_arr, haptic_arr, pc_rel, haptic_rel) = tracker.occupancy();
                    let (pc_buf, haptic_buf) = scheduler.buffered_counts();
                    if let Err(log_error) = logger
                        .lock()
                        .map_err(|_| anyhow::anyhow!("RX logger poisoned"))
                        .and_then(|mut logger| {
                            logger
                                .try_log_info(&format!(
                                    "\"event\":\"s3_tracker_bound\",\"error\":\"{message}\",\
                                     \"max_anchors\":{},\"pc_arrivals\":{pc_arr},\
                                     \"haptic_arrivals\":{haptic_arr},\"pc_releases\":{pc_rel},\
                                     \"haptic_releases\":{haptic_rel},\"pc_buffered\":{pc_buf},\
                                     \"haptic_buffered\":{haptic_buf},\"epoch\":{}",
                                    tracker.max_anchors(),
                                    scheduler.is_started()
                                ))
                                .map_err(anyhow::Error::from)
                        })
                    {
                        outcome_error =
                            Some(log_error.context(format!("S3 tracker bound: {message}")));
                        continue;
                    }
                }
                outcome_error = Some(anyhow::anyhow!(error.message()));
                continue;
            }
        };
        if pending_at_start || ingress.gate().pending_request().is_some() {
            if !observations.is_empty() {
                if let Err(error) = logger
                    .lock()
                    .map_err(|_| anyhow::anyhow!("RX logger poisoned"))
                    .and_then(|mut logger| {
                        logger
                            .try_log_info(&format!(
                                "\"event\":\"s3_observations_suppressed_during_switch\",\"count\":{}",
                                observations.len()
                            ))
                            .map_err(anyhow::Error::from)
                    })
                {
                    outcome_error = Some(error);
                }
            }
        } else {
            for observation in observations {
                let update = match controller.observe(observation) {
                    Ok(update) => update,
                    Err(error) => {
                        outcome_error = Some(anyhow::anyhow!(error));
                        break;
                    }
                };
                match request_s3_switch(
                    update,
                    &mut ingress,
                    &mut subscriber,
                    &namespace,
                    &mut live,
                    &runtime,
                    args,
                    logger.clone(),
                    event_tx.clone(),
                    bad_headers.clone(),
                    ingress_drops.clone(),
                    log_failed.clone(),
                )
                .await
                {
                    Ok(true) => break,
                    Ok(false) => {}
                    Err(error) => {
                        outcome_error = Some(error);
                        break;
                    }
                }
            }
        }
    }

    for event in ingress.finish_pending() {
        if let IngressEvent::Drop { routed, reason } = event {
            let header = routed.object.header;
            logger
                .lock()
                .map_err(|_| anyhow::anyhow!("RX logger poisoned"))?
                .try_log_drop_s3(
                    routed.role,
                    routed.route,
                    header.tier,
                    header.seq,
                    header.pts_us,
                    header.event_id,
                    now_us().max(routed.object.t_recv),
                    reason,
                )?;
            stats.dropped += 1;
        }
    }

    // Stop every subscription before joining/aborting its drain, then stop the
    // session. No detached reader is allowed to write after shutdown logging.
    // Retiring routes lose their remaining drain window at shutdown; this is
    // identical to the pre-existing treatment of live routes.
    for (_role, _route, subscription) in retiring.drain_all() {
        drop(subscription.handle);
        subscription.drain.abort();
        let _ = subscription.drain.await;
    }
    for (_, subscription) in live.drain() {
        drop(subscription.handle);
        subscription.drain.abort();
        let _ = subscription.drain.await;
    }
    for drain in retired_drains {
        if !drain.is_finished() {
            drain.abort();
        }
        let _ = drain.await;
    }
    session_run.abort();
    let _ = session_run.await;
    drop(event_tx);
    drop(bridge_tx);
    if let Some(mut child) = bridge_child {
        let _ = child.start_kill();
        let _ = child.wait().await;
    }

    if scheduler.is_started() {
        let budget_end = now_us()
            .saturating_add(playout.d_play_us)
            .saturating_add(playout.max_span_us)
            .saturating_add(playout.late_tolerance_us)
            .saturating_add(250_000);
        while !scheduler.is_empty() && now_us() <= budget_end {
            let Some(wakeup) = scheduler.next_wakeup_us() else {
                break;
            };
            tokio::time::sleep(Duration::from_micros(wakeup.saturating_sub(now_us()))).await;
            let now = now_us();
            let actions = scheduler.advance(now);
            dispatch_s3_playout_actions(
                actions,
                &mut object_routes,
                &mut tracker,
                &logger,
                None,
                false,
                false,
                &mut stats,
                now,
            )?;
        }
    } else {
        let now = now_us();
        let actions = scheduler.finish_without_epoch();
        dispatch_s3_playout_actions(
            actions,
            &mut object_routes,
            &mut tracker,
            &logger,
            None,
            false,
            false,
            &mut stats,
            now,
        )?;
    }

    let n_pc = recv_pc.load(Ordering::Relaxed);
    let n_haptic = recv_haptic.load(Ordering::Relaxed);
    let n_bad = bad_headers.load(Ordering::Relaxed);
    let n_ingress_drop = ingress_drops.load(Ordering::Relaxed);
    let failed = outcome_error.is_some()
        || n_bad > 0
        || n_ingress_drop > 0
        || log_failed.load(Ordering::Relaxed) > 0
        || !object_routes.is_empty();
    {
        let mut logger = logger
            .lock()
            .map_err(|_| anyhow::anyhow!("RX logger poisoned"))?;
        logger.try_log_info(&format!(
            "\"recv_pc\":{n_pc},\"recv_haptic\":{n_haptic},\"bad_headers\":{n_bad},\"s1_released\":{},\"s1_dropped\":{},\"s1_ingress_dropped\":{n_ingress_drop},\"s1_bridge_observer_dropped\":{}",
            stats.released,
            stats.dropped,
            stats.bridge_observer_dropped,
        ))?;
        logger.try_log_info(&format!(
            "\"event\":\"shutdown\",\"ending\":\"{}\",\"exit_code\":{},\"bad_headers\":{},\"subscribe_retries\":0,\"subscribe_wait_ms\":0,\"tracks\":[],\"detail\":\"{}\"",
            if failed { "error" } else { "normal" },
            if failed { 1 } else { 0 },
            n_bad,
            json_escape(
                &outcome_error
                    .as_ref()
                    .map(|error| format!("{error:#}"))
                    .unwrap_or_else(|| "rule=s3_current_routes_fin".to_string())
            )
        ))?;
        logger.try_flush()?;
    }
    if let Some(error) = outcome_error {
        return Err(error);
    }
    if failed {
        bail!(
            "S3 final integrity failure: bad_headers={n_bad} ingress_drops={n_ingress_drop} route_residue={}",
            object_routes.len()
        );
    }
    println!(
        "[rx] done (normal S3): pc={n_pc} haptic={n_haptic} -> {}",
        args.out.display()
    );
    Ok(())
}

async fn connect(
    relay: &Url,
) -> Result<(web_transport::Session, moq_transport::session::Transport)> {
    let tls_args = tls::Args {
        disable_verify: true,
        ..Default::default()
    };
    let tls = tls_args.load()?;
    let bind: SocketAddr = "[::]:0".parse().unwrap();
    let quic = quic::Endpoint::new(quic::Config::new(bind, None, tls)?)?;
    let (session, _cid, transport) = quic.client.connect(relay, None).await?;
    Ok((session, transport))
}

/// Direct topology: bind and accept exactly one inbound QUIC/WebTransport
/// session. Mirror image of `connect` — the only difference is which side
/// opens the connection.
async fn accept_direct(
    listen: SocketAddr,
    cert: &PathBuf,
    key: &PathBuf,
) -> Result<(web_transport::Session, moq_transport::session::Transport)> {
    let tls_args = tls::Args {
        cert: vec![cert.clone()],
        key: vec![key.clone()],
        disable_verify: true,
        ..Default::default()
    };
    let tls = tls_args.load()?;
    anyhow::ensure!(
        tls.server.is_some(),
        "--listen requires a usable server TLS config (check --tls-cert/--tls-key)"
    );
    let mut quic = quic::Endpoint::new(quic::Config::new(listen, None, tls)?)?;
    let server = quic
        .server
        .as_mut()
        .context("QUIC endpoint has no server side despite server TLS config")?;
    let (session, _cid, transport) = server
        .accept()
        .await
        .context("no inbound session accepted on --listen")?;
    Ok((session, transport))
}

/// Establish the transport session for whichever topology was selected.
/// `relay` = connect out to a relay; `direct` = bind and accept.
async fn establish(
    args: &Args,
) -> Result<(web_transport::Session, moq_transport::session::Transport)> {
    match args.listen {
        Some(listen) => {
            let cert = args
                .tls_cert
                .as_ref()
                .context("--listen requires --tls-cert")?;
            let key = args
                .tls_key
                .as_ref()
                .context("--listen requires --tls-key")?;
            accept_direct(listen, cert, key)
                .await
                .context("accept direct session")
        }
        None => connect(&args.relay).await.context("connect relay"),
    }
}

/// MoQ SETUP for whichever topology was selected.
///
/// The QUIC layer is not the whole story: the side that ACCEPTS the connection
/// must also accept the MoQ session (decode CLIENT_SETUP, reply SERVER_SETUP).
/// Calling `connect` on both sides makes both send CLIENT_SETUP and the
/// handshake stalls until the connection times out.
///
/// `connect_with_config` yields `(Session, Publisher, Subscriber)` while
/// `accept_with_config` yields `Option`s (roles are negotiated by the peer).
/// This normalizes both to the subscriber the receiver actually needs.
async fn session_handshake(
    args: &Args,
    sess: web_transport::Session,
    tp: moq_transport::session::Transport,
) -> Result<(Session, Subscriber)> {
    let config = SessionConfig {
        data_priority_mapping: args.data_priority_mapping,
        ..SessionConfig::default()
    };
    if args.listen.is_some() {
        let (session, _pub, sub) = Session::accept_with_config(sess, None, tp, config)
            .await
            .context("SETUP (accept, direct topology)")?;
        let sub =
            sub.context("peer did not negotiate a publisher role; no subscriber available")?;
        Ok((session, sub))
    } else {
        let (session, _pub, sub) = Session::connect_with_config(sess, None, tp, config)
            .await
            .context("SETUP (connect, relay topology)")?;
        Ok((session, sub))
    }
}

/// 직결(`--listen`) 토폴로지 전용: 송신자의 PUBLISH_NAMESPACE 에 응답한다.
///
/// **릴레이 토폴로지에서는 릴레이가 이 일을 한다.** sender 는 relay 에
/// PUBLISH_NAMESPACE 를 보내고 relay 가 OK 로 답하며, 이 수신자는 subscribe 만
/// 한다. 직결에서는 중간 상자가 없고 **이 수신자가 곧 송신자의 피어**이므로,
/// inbound announce 큐(`Subscriber::published_namespace`)를 직접 배수해
/// draft-16의 `REQUEST_OK` 를 보내야 한다.
///
/// 그러지 않으면 송신자의 요청이 만료되고
/// (`moq-transport/src/session/publisher.rs:622` "PublishNamespace response
/// timed out") 세션이 런 도중에 끊긴다. 수신자의 SUBSCRIBE 도 송신자가 아직
/// 네임스페이스를 등록하기 전에 도착하면 `unknown_subscribed` 로 빠져 응답을
/// 받지 못한다 — 여기서 announce 를 먼저 기다리므로 그 경쟁도 함께 닫힌다.
///
/// **게이트 6 에서 실측으로 드러났다.** 20 s 런과 6 s 루프백은 제어 타임아웃이
/// 발화하기 전에 끝나 통과했고, 60 s 런에서만 3/3 실패했다. 즉 커밋 bc4cd69 의
/// 스모크와 게이트 5 는 이 결함을 가릴 수밖에 없었다.
async fn ack_published_namespace(
    subscriber: &mut Subscriber,
    namespace: &TrackNamespace,
    timeout_s: f64,
) -> Result<PublishedNamespace> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs_f64(timeout_s);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            bail!("직결 토폴로지: 송신자가 {timeout_s}s 안에 네임스페이스를 announce 하지 않았다");
        }
        match tokio::time::timeout(remaining, subscriber.published_namespace()).await {
            Ok(Some(mut ns)) => {
                if &ns.info.namespace != namespace {
                    // 다른 네임스페이스는 우리 런의 것이 아니다. 응답하지 않고
                    // drop 하면 REQUEST_ERROR 로 거절되며, 계속 기다린다.
                    tracing::warn!(
                        got = ?ns.info.namespace,
                        want = ?namespace,
                        "직결: 예상과 다른 네임스페이스 announce — 거절하고 대기"
                    );
                    continue;
                }
                ns.ok().context("PUBLISH_NAMESPACE REQUEST_OK 전송")?;
                // **핸들을 돌려준다.** `PublishedNamespace` 는 drop 시
                // PUBLISH_NAMESPACE_CANCEL 을 보내므로, 여기서 떨어뜨리면
                // 송신자가 곧바로 `cancelled` 로 죽는다(루프백 실측).
                // 호출자가 런이 끝날 때까지 붙들고 있어야 한다.
                return Ok(ns);
            }
            // 큐가 닫혔다 = 세션이 끝났다. 폴링으로 숨기지 않는다.
            Ok(None) => bail!("직결 토폴로지: announce 를 받기 전에 세션이 종료됐다"),
            Err(_) => continue, // remaining 만료 → 다음 반복이 deadline 으로 종료
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();
    let args = Args::parse();
    validate_v5_rates(args.pc_rate_hz, args.haptic_rate_hz)?;
    anyhow::ensure!(args.chunk_bytes > 0, "--chunk-bytes must be positive");
    anyhow::ensure!(
        args.reassembly_max_pending_frames > 0
            && args.reassembly_max_pending_bytes > 0
            && args.reassembly_max_age_ms > 0,
        "all reassembly bounds must be positive"
    );
    validate_topology(args.topology, args.listen.is_some())?;
    validate_phase4_v5_args(&args)?;
    if args.receive_trace.is_some() {
        anyhow::ensure!(args.arm == Arm::B1 && args.payload_mode == PayloadMode::Frame,
            "--receive-trace supports only B1/frame verification");
    }
    validate_queue_policy(args.queue_policy, args.arm, args.payload_mode)?;
    let s1_config = playout_config(&args)?;
    let s3_runtime = s3_runtime_config(&args)?;
    let phase4_transport = phase4_transport(&args);

    let logger = Arc::new(Mutex::new(JsonlLogger::new(
        &args.out,
        &args.run_id,
        "moq",
        "rx",
        args.c_mbps,
        args.rtt_ms,
        args.jitter_ms,
        args.loss_pct,
        args.s_bytes,
        args.pc_rate_hz,
        args.haptic_rate_hz,
        args.seed,
        // duration_s = None **유지**: rx meta에 duration_s를 넣으면 분석기의
        // 설계 분모 출처(`_design`이 rx_meta.duration_s도 읽음)로 흡수되어
        // 기대 프레임 분모의 provenance가 바뀐다. 분모는 tx meta/CLI 주입만
        // 쓰는 현 계약을 유지한다.
        // tracks: 러너가 --tracks를 전달하므로 이제 수신자도 안다. rx meta에
        // 기록해 두면 tx 로그를 잃은 C3 rx 로그도 단독 트랙으로 분류된다.
        // term_protocol: 종료 프로토콜 세대 마커(Codex 7차 P0 — tx 소실 +
        // shutdown 결손 조합이 구세대로 오인되는 우회를 rx meta 자체로 차단).
        None,
        None,
        Some(args.tracks.as_str()),
        Some(TERM_PROTOCOL_V),
        s1_config,
        phase4_transport,
        Some(V5Meta {
            payload_mode: args.payload_mode,
            representation: args.representation,
            topology: args.topology,
            chunk_bytes: args.chunk_bytes,
            queue_policy: Some(args.queue_policy.as_str()),
        }),
    )?));

    if args.arm == Arm::S3 {
        return run_s3_receiver(
            &args,
            s1_config.expect("S3 requires the common playout scheduler"),
            s3_runtime.expect("validated S3 runtime config"),
            logger,
        )
        .await;
    }

    let receive_trace = args.receive_trace.as_ref()
        .map(|path| receive_trace::Trace::start(path, &args.run_id, args.receive_trace_capacity)).transpose()?;
    let (sess, tp) = establish(&args).await?;
    let (session, mut subscriber) = session_handshake(&args, sess, tp).await?;
    let mut session_run = tokio::spawn(session.run());

    let namespace = TrackNamespace::from_utf8_path(&args.run_id);
    let names = args.queue_policy.wire_tracks();

    // 직결 토폴로지에서는 이 수신자가 송신자의 피어다 — subscribe 하기 전에
    // 송신자의 PUBLISH_NAMESPACE 에 먼저 응답해야 한다.
    // 핸들은 런이 끝날 때까지 살려 둔다(drop = PUBLISH_NAMESPACE_CANCEL).
    let _announce_guard: Option<PublishedNamespace> = if args.listen.is_some() {
        Some(
            ack_published_namespace(&mut subscriber, &namespace, args.subscribe_timeout)
                .await
                .context("직결 토폴로지 announce 응답")?,
        )
    } else {
        None
    };

    // Subscribe to both tracks, retrying until the publisher has announced.
    // A fresh Tracks producer per attempt avoids duplicate-name residue.
    let mut received = Vec::new();
    let mut sub_handles = Vec::new();
    let mut _keep_reader = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs_f64(args.max_duration);
    // Dedicated, tighter budget for establishing the subscription. Previously
    // this shared `max_duration` and retried on *any* error every 300 ms, which
    // both hid genuine faults and left a wide race window.
    let sub_started = tokio::time::Instant::now();
    let sub_deadline = sub_started + Duration::from_secs_f64(args.subscribe_timeout);
    let mut sub_stats = SubscribeStats::default();
    'outer: loop {
        let (mut sub_tracks, _req, mut sub_reader) = Tracks::new(namespace.clone()).produce();
        let mut this_recv = Vec::new();
        let mut this_handles = Vec::new();
        // None == the local producer is not ready yet, which is the same
        // announce-ordering race and is retryable.
        let mut fatal: Option<anyhow::Error> = None;
        let mut ok = true;
        for &name in names {
            let tw = match sub_tracks.create(name) {
                Some(tw) => tw,
                None => {
                    ok = false;
                    break;
                }
            };
            let rr = match sub_reader.get_track_reader(&namespace, name) {
                Some(rr) => rr,
                None => {
                    ok = false;
                    break;
                }
            };
            let mut params = KeyValuePairs::default();
            if name == "pc" && matches!(args.arm, Arm::S2 | Arm::S2Eq) {
                params.set_delivery_timeout(
                    args.pc_delivery_timeout_ms
                        .expect("S2 timeout validated before connecting"),
                );
            }
            match subscriber.subscribe_open_with_params(tw, params).await {
                Ok(h) => {
                    this_handles.push(h);
                    this_recv.push((name, rr));
                }
                Err(e) if is_retryable_subscribe_error(&e) => {
                    ok = false;
                    break;
                }
                // Anything else is a real fault: fail now, do not poll on it.
                Err(e) => {
                    fatal = Some(anyhow::anyhow!("subscribe {name} failed: {e}"));
                    ok = false;
                    break;
                }
            }
        }
        if let Some(e) = fatal {
            return Err(e);
        }
        if ok {
            received = this_recv;
            sub_handles = this_handles;
            _keep_reader = Some(sub_reader);
            sub_stats.waited_ms = sub_started.elapsed().as_millis() as u64;
            break 'outer;
        }
        if tokio::time::Instant::now() >= sub_deadline {
            bail!(
                "could not subscribe within {}s ({} retries; publisher never announced?)",
                args.subscribe_timeout,
                sub_stats.retries
            );
        }
        // Check the session is still alive before retrying.
        if session_run.is_finished() {
            bail!("session ended before subscribe succeeded");
        }
        sub_stats.retries += 1;
        tokio::time::sleep(Duration::from_millis(args.subscribe_retry_ms)).await;
    }
    if sub_stats.retries > 0 {
        println!(
            "[rx] subscribe recovered after {} retries ({} ms)",
            sub_stats.retries, sub_stats.waited_ms
        );
    }
    println!("[rx] subscribed {} on {}", names.join("+"), args.run_id);
    // rev7 §7.1 readiness is the complete application path, not the UDP
    // listener used by the launcher. A successful two-track SUBSCRIBE through
    // the relay proves both MoQ sessions; direct mode proves the accepted
    // sender/receiver session. Keep the exact component set in the JSONL so
    // the production driver can derive latency from its paired start anchor.
    // shared_fifo has one subscription, so its component is "mixed_subscription".
    let mut readiness_components: Vec<&str> = match args.topology {
        Topology::Relay => vec!["sender_relay_session", "relay_receiver_session"],
        Topology::Direct => vec!["sender_receiver_session"],
    };
    match args.queue_policy {
        QueuePolicy::Separate => {
            readiness_components.push("pc_subscription");
            readiness_components.push("haptic_subscription");
        }
        QueuePolicy::SharedFifo => readiness_components.push("mixed_subscription"),
    }
    logger
        .lock()
        .unwrap()
        .log_readiness(now_us(), &readiness_components)
        .context("failed to record readiness components")?;

    // Stage A bridge (optional): spawn the Python renderer/audio helper and forward
    // received objects (header+payload) over a non-blocking, drop-on-full channel so
    // the receive loop never stalls — t_play stays == t_recv (L1), same as B0.
    let mut bridge_child = None;
    let ftx: Option<mpsc::Sender<Bytes>> = if args.render || args.audio {
        let mut cmd = Command::new(&args.python);
        cmd.arg("tools/stage_a_bridge.py");
        if args.render {
            cmd.arg("--render");
        }
        if args.audio {
            cmd.arg("--audio");
        }
        if args.draco {
            cmd.arg("--draco");
        }
        cmd.arg("--title")
            .arg(format!("skew live — {}", args.run_id));
        cmd.stdin(Stdio::piped());
        let mut child = cmd.spawn().context("spawn stage_a_bridge")?;
        let mut stdin = child.stdin.take().unwrap();
        let (tx, mut rx) = mpsc::channel::<Bytes>(64);
        tokio::spawn(async move {
            while let Some(b) = rx.recv().await {
                if stdin.write_all(&b).await.is_err() {
                    break;
                }
            }
            let _ = stdin.flush().await; // EOF on drop -> bridge exits
        });
        bridge_child = Some(child);
        println!(
            "[rx] Stage A bridge: render={} audio={}",
            args.render, args.audio
        );
        Some(tx)
    } else {
        None
    };

    // S1 owns a bounded ingress queue in addition to its per-track bounded
    // buffers. B1 never creates this task and keeps its original direct path.
    let ingress_drops = Arc::new(AtomicU64::new(0));
    let ingress_log_failed = Arc::new(AtomicU64::new(0));
    let (s1_tx, mut s1_task) = if let Some(config) = s1_config {
        let capacity = config
            .max_objects_per_track
            .checked_mul(2)
            .context("S1 ingress capacity overflow")?;
        let (tx, rx) = mpsc::channel::<PlayoutObject>(capacity);
        let task = tokio::spawn(run_playout_scheduler(
            config,
            rx,
            logger.clone(),
            ftx.clone(),
            args.render,
            args.audio,
        ));
        println!(
            "[rx] S1 scheduler: D_play={}ms startup={}ms late={}ms policy={} objects/track={} span={}ms",
            config.d_play_us / 1_000,
            config.startup_timeout_us / 1_000,
            config.late_tolerance_us / 1_000,
            config.late_policy.as_str(),
            config.max_objects_per_track,
            config.max_span_us / 1_000,
        );
        (Some(tx), Some(task))
    } else {
        (None, None)
    };

    // Drain each wire track: parse header, log rx, (optionally) forward for
    // render/audio. Counters and wire stats are per LOGICAL track (0 == pc,
    // 1 == haptic); a "mixed" drain feeds both slots by header track_id.
    let counts: Vec<Arc<AtomicU64>> = (0..2).map(|_| Arc::new(AtomicU64::new(0))).collect();
    let wire_stats: Vec<Arc<Mutex<ReassemblyStats>>> = (0..2)
        .map(|_| Arc::new(Mutex::new(ReassemblyStats::default())))
        .collect();
    // Header-integrity counters (R3b). A mismatch means the rx log cannot be
    // trusted as a measurement, so it is counted, recorded, and exits non-zero.
    let bad_headers = Arc::new(AtomicU64::new(0));
    let mut drains = Vec::new();
    for (name, received_track) in received.into_iter() {
        let logger = logger.clone();
        let counts = counts.clone();
        let bad = bad_headers.clone();
        let ftx = ftx.clone();
        let s1_tx = s1_tx.clone();
        let ingress_drops = ingress_drops.clone();
        let ingress_log_failed = ingress_log_failed.clone();
        let wire_stats_out = wire_stats.clone();
        // Logical slots this drain owns: its own for pc/haptic, both for mixed.
        let owned_slots: Vec<usize> = if name == SHARED_FIFO_TRACK {
            vec![0, 1]
        } else {
            vec![track_slot(name)]
        };
        let payload_mode = args.payload_mode;
        let chunk_bytes = args.chunk_bytes;
        let reassembly_max_pending_frames = args.reassembly_max_pending_frames;
        let reassembly_max_pending_bytes = args.reassembly_max_pending_bytes;
        let reassembly_max_age_us = args.reassembly_max_age_ms.saturating_mul(1_000);
        let (render, audio) = (args.render, args.audio);
        drains.push(tokio::spawn(async move {
            let mut reassembler = if payload_mode == PayloadMode::EqualChunk {
                match LogicalReassembler::new(
                    chunk_bytes,
                    reassembly_max_pending_frames,
                    reassembly_max_pending_bytes,
                    reassembly_max_age_us,
                ) {
                    Ok(value) => Some(value),
                    Err(e) => return (name, TrackEnd::Failed, format!("{e:#}")),
                }
            } else {
                None
            };
            // Warmup equal-chunk objects have their own bounded assembly
            // state. Mixing them into the measurement reassembler would make
            // lifecycle traffic change frames_completed/incomplete_frames.
            let mut warmup_reassembler = if payload_mode == PayloadMode::EqualChunk {
                match LogicalReassembler::new(
                    chunk_bytes,
                    reassembly_max_pending_frames,
                    reassembly_max_pending_bytes,
                    reassembly_max_age_us,
                ) {
                    Ok(value) => Some(value),
                    Err(e) => return (name, TrackEnd::Failed, format!("{e:#}")),
                }
            } else {
                None
            };
            let mut frame_stats = [ReassemblyStats::default(), ReassemblyStats::default()];
            // Inner future yields the raw ServeError so the ending can be
            // classified before the error is erased.
            let inner = async {
            let mut subgroups = match received_track.mode().await? {
                TrackReaderMode::Subgroups(s) => s,
                _ => return Err(DrainFail::NonSubgroup),
            };
            while let Some(mut sg) = subgroups.next().await? {
                while let Some(obj) = sg.read_next().await? {
                    let t = now_us(); // arrival = t_recv = t_play (L1)
                    // R3b: validate before logging. Anything that fails these
                    // checks is not a measurement, so it must not enter the
                    // rx log as if it were one.
                    let Some(h) = unpack_header(&obj) else {
                        eprintln!("[rx] {name}: object shorter than the {HDR}B header ({}B)", obj.len());
                        bad.fetch_add(1, Ordering::Relaxed);
                        continue;
                    };
                    if h.version != VERSION {
                        eprintln!("[rx] {name}: header version {} != {VERSION}", h.version);
                        bad.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    let Some(track) = demux_track(name, h.track_id) else {
                        eprintln!(
                            "[rx] {name}: header track '{}' does not match the subscribed track",
                            track_name(h.track_id)
                        );
                        bad.fetch_add(1, Ordering::Relaxed);
                        continue;
                    };
                    let slot = track_slot(track);
                    let fwd = (track == "pc" && render) || (track == "haptic" && audio);
                    if obj.len() != HDR + h.payload_len as usize {
                        eprintln!(
                            "[rx] {name}: object length {} != {HDR} + declared payload_len {}",
                            obj.len(), h.payload_len
                        );
                        bad.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    let warmup = is_warmup_seq(h.seq);
                    let selected_reassembler = if warmup {
                        warmup_reassembler.as_mut()
                    } else {
                        reassembler.as_mut()
                    };
                    let (h, logical_obj) = if let Some(r) = selected_reassembler {
                        let complete = match r.feed(h, &obj[HDR..], t) {
                            Ok(value) => value,
                            Err(e) => {
                                eprintln!("[rx] {name}: invalid equal-chunk object: {e:#}");
                                bad.fetch_add(1, Ordering::Relaxed);
                                continue;
                            }
                        };
                        let Some(complete) = complete else {
                            continue;
                        };
                        let h = complete.header;
                        let packed = pack_header(
                            h.track_id, h.tier, h.seq, h.pts_us, h.event_id,
                            h.gen_ts_us, h.payload_len,
                        );
                        let mut logical = Vec::with_capacity(HDR + complete.payload.len());
                        logical.extend_from_slice(&packed);
                        logical.extend_from_slice(&complete.payload);
                        (h, Bytes::from(logical))
                    } else {
                        if !warmup {
                            frame_stats[slot].chunks_received += 1;
                            frame_stats[slot].frames_completed += 1;
                        }
                        (h, obj.clone())
                    };
                    if is_warmup_seq(h.seq) {
                        if logger.lock().unwrap().try_log_warmup_rx(
                            track, h.tier, h.seq, h.pts_us, h.event_id,
                            h.payload_len, h.gen_ts_us, t,
                        ).is_err() {
                            ingress_log_failed.fetch_add(1, Ordering::Relaxed);
                        }
                        continue;
                    }
                    logger.lock().unwrap().log_rx(
                        track, h.tier, h.seq, h.pts_us, h.event_id, h.payload_len, t, t, h.gen_ts_us,
                    );
                    counts[slot].fetch_add(1, Ordering::Relaxed);
                    if let Some(tx) = &s1_tx {
                        let scheduled = PlayoutObject {
                            header: h,
                            t_recv: t,
                            bytes: logical_obj.clone(),
                        };
                        match tx.try_send(scheduled) {
                            Ok(()) => {}
                            Err(mpsc::error::TrySendError::Full(scheduled)) => {
                                ingress_drops.fetch_add(1, Ordering::Relaxed);
                                let h = scheduled.header;
                                let logged = logger.lock().unwrap().try_log_drop(
                                    scheduled.track_name(), h.tier, h.seq, h.pts_us, h.event_id,
                                    now_us().max(scheduled.t_recv), "ingress_queue_full",
                                );
                                if logged.is_err() {
                                    ingress_log_failed.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            Err(mpsc::error::TrySendError::Closed(scheduled)) => {
                                // A closed scheduler while receive producers are
                                // live is an internal failure, not an expected
                                // capacity drop. Account the object and force a
                                // non-zero finalization result.
                                ingress_drops.fetch_add(1, Ordering::Relaxed);
                                let h = scheduled.header;
                                let _ = logger.lock().unwrap().try_log_drop(
                                    scheduled.track_name(), h.tier, h.seq, h.pts_us, h.event_id,
                                    now_us().max(scheduled.t_recv), "scheduler_closed",
                                );
                                ingress_log_failed.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    } else if fwd {
                        if let Some(tx) = &ftx {
                            let _ = tx.try_send(logical_obj); // drop-on-full (latest-wins-ish)
                        }
                    }
                }
            }
            Ok::<(), DrainFail>(())
            };
            // Reader exhausted cleanly == FIN. Otherwise classify the error;
            // only `Done` is a FIN, `Cancel`/`Closed` stay ambiguous.
            let result = inner.await;
            if let Some(r) = reassembler.as_mut() {
                // equal_chunk is separate-policy only: exactly one owned slot.
                r.finish();
                *wire_stats_out[owned_slots[0]].lock().unwrap() = r.stats.clone();
            } else {
                for &slot in &owned_slots {
                    *wire_stats_out[slot].lock().unwrap() = frame_stats[slot].clone();
                }
            }
            match result {
                Ok(()) => (name, TrackEnd::Fin, String::new()),
                Err(DrainFail::Serve(e)) => (name, classify_track_end(&e), format!("{e}")),
                Err(DrainFail::NonSubgroup) => (name, TrackEnd::Failed, "non-subgroup delivery".into()),
            }
        }));
    }
    drop(ftx); // only the drain clones keep the channel open now

    // Wait for both drains to finish (tracks closed by publisher) or session
    // end / timeout.
    //
    // Errors here used to be discarded, so the receiver exited 0 no matter what
    // happened and a batch runner's "abort on non-zero exit" never fired on the
    // rx side. Each outcome is now classified and reported.
    let all_drains = async {
        let mut ends: Vec<(&'static str, TrackEnd, String)> = Vec::new();
        for d in &mut drains {
            match d.await {
                Ok(t) => ends.push(t),
                Err(e) if e.is_panic() => {
                    ends.push(("?", TrackEnd::Failed, format!("drain task panicked: {e}")))
                }
                Err(e) => ends.push(("?", TrackEnd::Failed, format!("drain task join error: {e}"))),
            }
        }
        ends
    };
    let drained = tokio::select! {
        ends = all_drains => Drained::Ends(ends),
        r = &mut session_run => {
            eprintln!("[rx] session ended before tracks closed: {r:?}");
            Drained::SessionArm(format!("{r:?}"))
        }
        _ = tokio::time::sleep_until(deadline) => {
            eprintln!("[rx] max-duration reached — reception is INCOMPLETE");
            Drained::Timeout
        }
    };

    // A cancelled select branch must not detach receiver tasks. Abort and join
    // any outstanding producer before closing the scheduler input.
    for drain in &drains {
        if !drain.is_finished() {
            drain.abort();
        }
    }
    for drain in &mut drains {
        if !drain.is_finished() {
            let _ = drain.await;
        }
    }
    drop(s1_tx);

    let mut scheduler_finalize_failed = false;
    let mut s1_stats = PlayoutStats::default();
    if let Some(mut task) = s1_task.take() {
        let config = s1_config.expect("S1 task has config");
        let budget_us = config
            .d_play_us
            .saturating_add(config.max_span_us)
            .saturating_add(config.late_tolerance_us)
            .saturating_add(250_000);
        match tokio::time::timeout(Duration::from_micros(budget_us), &mut task).await {
            Ok(Ok(Ok(stats))) => s1_stats = stats,
            Ok(Ok(Err(e))) => {
                eprintln!("[rx] S1 scheduler failed: {e:#}");
                scheduler_finalize_failed = true;
            }
            Ok(Err(e)) => {
                eprintln!("[rx] S1 scheduler task join failed: {e}");
                scheduler_finalize_failed = true;
            }
            Err(_) => {
                eprintln!("[rx] S1 scheduler shutdown exceeded {budget_us}us");
                task.abort();
                let _ = task.await;
                scheduler_finalize_failed = true;
            }
        }
    }

    let n_pc = counts[0].load(Ordering::Relaxed);
    let n_hap = counts[1].load(Ordering::Relaxed);
    let pc_wire = wire_stats[0].lock().unwrap().clone();
    let hap_wire = wire_stats[1].lock().unwrap().clone();

    let session_already_joined = matches!(&drained, Drained::SessionArm(_));
    let mut reports_out: Vec<DrainReport> = Vec::new();
    let (ending, detail) = match drained {
        Drained::Timeout => (
            RxEnding::Timeout,
            format!("rule=max_duration max_duration={}", args.max_duration),
        ),
        Drained::SessionArm(d) => (RxEnding::SessionEnded, format!("rule=session_arm {d}")),
        Drained::Ends(ends) => {
            // A `Cancelled` drain can finish fractionally before the session
            // task resolves, which is exactly how a relay death used to be
            // reported as normal. Give the session a bounded moment to settle
            // before deciding, then consult it explicitly.
            let ambiguous = ends.iter().any(|(_, e, _)| *e == TrackEnd::Cancelled);
            if ambiguous {
                let grace = tokio::time::Instant::now() + Duration::from_millis(500);
                while !session_run.is_finished() && tokio::time::Instant::now() < grace {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
            let reports = expand_reports(ends, n_pc, n_hap, &args);
            for r in &reports {
                println!(
                    "[rx] track {}: end={:?} received={} expected={:?} {}",
                    r.name, r.end, r.received, r.expected, r.detail
                );
            }
            // A FIN carrying less than the design quantity cannot be
            // adjudicated here (lossy link vs. publisher that died early look
            // identical), but it must not pass silently either.
            for r in &reports {
                if r.end == TrackEnd::Fin && r.complete() == Some(false) {
                    eprintln!(
                        "[rx] WARNING: {} FINed with {}/{} objects — check the tx log before using this run",
                        r.name, r.received, r.expected.unwrap_or(0)
                    );
                }
            }
            let verdict = classify_ending_with_timeout(
                &reports,
                session_run.is_finished(),
                matches!(args.arm, Arm::S2 | Arm::S2Eq) && args.pc_delivery_timeout_ms.is_some(),
            );
            reports_out = reports;
            verdict
        }
    };
    if ending == RxEnding::Normal {
        println!("[rx] tracks closed");
    } else {
        eprintln!("[rx] abnormal ending: {} ({detail})", ending.as_str());
    }

    let _ = &sub_handles; // keep subscriptions alive until here
    let n_bad = bad_headers.load(Ordering::Relaxed);
    if n_bad > 0 {
        eprintln!("[rx] {n_bad} object(s) failed header validation and were NOT logged");
    }

    // Same record convention as the sender, so the analyzer can classify a run
    // from the rx log alone. Write errors are propagated: a run whose ending
    // never reached disk must not exit 0.
    let mut record_io_failed =
        scheduler_finalize_failed || ingress_log_failed.load(Ordering::Relaxed) > 0;
    {
        let mut lg = match logger.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        if lg
            .try_log_info(&format!(
                "\"recv_pc\":{n_pc},\"recv_haptic\":{n_hap},\"bad_headers\":{n_bad},\"pc_chunks_received\":{},\"haptic_chunks_received\":{},\"frames_completed\":{},\"incomplete_frames\":{},\"duplicate_chunks\":{},\"invalid_chunks\":{},\"reassembly_peak_frames\":{},\"reassembly_peak_bytes\":{},\"s1_released\":{},\"s1_dropped\":{},\"s1_ingress_dropped\":{},\"s1_bridge_observer_dropped\":{}",
                pc_wire.chunks_received,
                hap_wire.chunks_received,
                pc_wire.frames_completed + hap_wire.frames_completed,
                pc_wire.incomplete_frames + hap_wire.incomplete_frames,
                pc_wire.duplicate_chunks + hap_wire.duplicate_chunks,
                pc_wire.invalid_chunks + hap_wire.invalid_chunks,
                pc_wire.peak_frames.max(hap_wire.peak_frames),
                pc_wire.peak_bytes.max(hap_wire.peak_bytes),
                s1_stats.released,
                s1_stats.dropped,
                ingress_drops.load(Ordering::Relaxed),
                s1_stats.bridge_observer_dropped,
            ))
            .is_err()
        {
            record_io_failed = true;
        }
        if lg
            .try_log_info(&format!(
                "\"event\":\"shutdown\",\"ending\":\"{}\",\"exit_code\":{},\"bad_headers\":{},\"subscribe_retries\":{},\"subscribe_wait_ms\":{},\"tracks\":{},\"detail\":\"{}\"",
                ending.as_str(),
                ending.exit_code(),
                n_bad,
                sub_stats.retries,
                sub_stats.waited_ms,
                tracks_json(&reports_out),
                json_escape(&detail)
            ))
            .is_err()
        {
            record_io_failed = true;
        }
        if lg.try_flush().is_err() {
            record_io_failed = true;
        }
    }

    session_run.abort();
    if let Some(trace) = receive_trace {
        // Trace-only teardown: let receive scopes emit interruption on cancellation.
        // A consumed select result must never be polled twice. The sealed trace
        // remains a callback interval, not a claim of whole-runtime quiescence.
        if !session_already_joined {
            match tokio::time::timeout(Duration::from_secs(2), &mut session_run).await {
                Ok(Ok(_)) => {},
                Ok(Err(error)) if error.is_cancelled() => {},
                _ => record_io_failed = true,
            }
        }
        if let Err(error) = trace.finish(ending.as_str()) {
            eprintln!("[rx] receive trace finalize failed: {error:#}");
            record_io_failed = true;
        }
    }
    // Let the Stage A bridge flush its final frame counts, then reap it.
    if let Some(mut child) = bridge_child {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let _ = child.start_kill();
        let _ = child.wait().await;
    }
    println!(
        "[rx] done ({}): pc={n_pc} haptic={n_hap} -> {}",
        ending.as_str(),
        args.out.display()
    );

    if record_io_failed {
        eprintln!(
            "[rx] FATAL: could not write the shutdown record; exiting {EXIT_FINALIZE_FAILED}"
        );
        std::process::exit(EXIT_FINALIZE_FAILED);
    }
    if ending != RxEnding::Normal {
        std::process::exit(ending.exit_code());
    }
    // Header integrity outranks a nominally normal ending: a run that carried
    // malformed objects is not a valid measurement even if the tracks FINed.
    if n_bad > 0 {
        eprintln!("[rx] FATAL: {n_bad} malformed header(s); exiting {EXIT_RX_HEADER_INVALID}");
        std::process::exit(EXIT_RX_HEADER_INVALID);
    }
    Ok(())
}

// ---- R2: receiver ending-classification tests ------------------------------
//
// The absence of these was itself a blocking finding, combined with the real
// `Cancel -> normal` ambiguity. Each test forces one of the four paths.

#[cfg(test)]
mod rx_ending_tests {
    use super::*;

    fn rep(name: &'static str, end: TrackEnd, received: u64, expected: Option<u64>) -> DrainReport {
        DrainReport {
            name,
            end,
            received,
            expected,
            detail: String::new(),
        }
    }

    fn stage5_cli(extra: &[&str]) -> Vec<String> {
        let mut base: Vec<String> = [
            "moq_receiver", "--run-id", "t", "--out", "/dev/null", "--s-bytes", "1",
            "--pc-rate-hz", "30", "--haptic-rate-hz", "90", "--payload-mode", "frame",
            "--representation", "bin", "--chunk-bytes", "178",
            "--reassembly-max-pending-frames", "64", "--reassembly-max-pending-bytes",
            "67108864", "--reassembly-max-age-ms", "2000", "--topology", "relay",
            "--duration-s", "1",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        base.extend(extra.iter().map(|s| s.to_string()));
        base
    }

    /// Stage-5 P1: a mixed object sequence (1 PC : 3 haptic) is demultiplexed
    /// by header track_id into the same logical pc/haptic rows the separate
    /// policy writes, and the counters/report expansion follow the same split.
    #[test]
    fn shared_fifo_demux_splits_a_mixed_sequence_into_pc_and_haptic_rows() {
        let sequence = [TRACK_PC, TRACK_HAPTIC, TRACK_HAPTIC, TRACK_HAPTIC, TRACK_PC, TRACK_HAPTIC];
        let demuxed: Vec<&str> = sequence
            .iter()
            .map(|&id| demux_track(SHARED_FIFO_TRACK, id).expect("mixed admits both tracks"))
            .collect();
        assert_eq!(demuxed, ["pc", "haptic", "haptic", "haptic", "pc", "haptic"]);
        let mut counts = [0u64; 2];
        for track in &demuxed {
            counts[track_slot(track)] += 1;
        }
        assert_eq!(counts, [2, 4]);
        // Unknown track ids are header failures on every subscription.
        assert_eq!(demux_track(SHARED_FIFO_TRACK, 9), None);
        // The separate policy keeps the unchanged R3b rule: a header naming
        // the other track is rejected, not re-routed.
        assert_eq!(demux_track("pc", TRACK_PC), Some("pc"));
        assert_eq!(demux_track("pc", TRACK_HAPTIC), None);
        assert_eq!(demux_track("haptic", TRACK_HAPTIC), Some("haptic"));
        assert_eq!(demux_track("haptic", TRACK_PC), None);

        let args = Args::try_parse_from(stage5_cli(&["--queue-policy", "shared_fifo"]))
            .expect("shared_fifo parses");
        assert_eq!(args.queue_policy, QueuePolicy::SharedFifo);
        assert_eq!(args.queue_policy.wire_tracks(), ["mixed"]);
        let reports = expand_reports(
            vec![(SHARED_FIFO_TRACK, TrackEnd::Fin, String::new())],
            counts[0],
            counts[1],
            &args,
        );
        let summary: Vec<(&str, TrackEnd, u64, Option<u64>)> = reports
            .iter()
            .map(|r| (r.name, r.end, r.received, r.expected))
            .collect();
        // One mixed FIN yields one report per logical track against its own
        // design count (1 s at 30/90 Hz), so completeness stays per track.
        assert_eq!(
            summary,
            [("pc", TrackEnd::Fin, 2, Some(30)), ("haptic", TrackEnd::Fin, 4, Some(90))]
        );
        let (ending, _) = classify_ending(&reports, false);
        assert_eq!(ending, RxEnding::Normal);
        let full = expand_reports(
            vec![(SHARED_FIFO_TRACK, TrackEnd::Cancelled, "x".into())],
            30,
            90,
            &args,
        );
        assert_eq!(classify_ending(&full, false).0, RxEnding::Normal);
        let short = expand_reports(
            vec![(SHARED_FIFO_TRACK, TrackEnd::Cancelled, "x".into())],
            30,
            89,
            &args,
        );
        assert_eq!(classify_ending(&short, false).0, RxEnding::CancelledIncomplete);

        // The separate policy is untouched: default flag, two wire tracks,
        // one report per drain.
        let args = Args::try_parse_from(stage5_cli(&[])).expect("default parses");
        assert_eq!(args.queue_policy, QueuePolicy::Separate);
        assert_eq!(args.queue_policy.wire_tracks(), ["pc", "haptic"]);
        let reports = expand_reports(
            vec![
                ("pc", TrackEnd::Fin, String::new()),
                ("haptic", TrackEnd::Fin, String::new()),
            ],
            30,
            90,
            &args,
        );
        assert_eq!(reports.len(), 2);
        assert_eq!((reports[0].name, reports[0].received), ("pc", 30));
        assert_eq!((reports[1].name, reports[1].received), ("haptic", 90));
    }

    #[test]
    fn shared_fifo_is_rejected_outside_b1_frame_but_accepts_the_receive_trace() {
        use QueuePolicy::{Separate, SharedFifo};
        validate_queue_policy(SharedFifo, Arm::B1, PayloadMode::Frame).unwrap();
        for arm in [Arm::S1, Arm::M1, Arm::S2, Arm::S2Eq, Arm::S3] {
            let err = validate_queue_policy(SharedFifo, arm, PayloadMode::Frame)
                .unwrap_err()
                .to_string();
            assert!(err.contains("requires --arm b1"), "{err}");
            // separate never rejects anything.
            validate_queue_policy(Separate, arm, PayloadMode::EqualChunk).unwrap();
        }
        let err = validate_queue_policy(SharedFifo, Arm::B1, PayloadMode::EqualChunk)
            .unwrap_err()
            .to_string();
        assert!(err.contains("--payload-mode frame"), "{err}");
        assert!(Args::try_parse_from(stage5_cli(&["--queue-policy", "mixed"])).is_err());

        // Instrumentation parity (registered policy: all traces on for every
        // policy): a shared_fifo configuration WITH the raw receive trace must
        // pass the same validation gates main() applies before opening logs.
        let args = Args::try_parse_from(stage5_cli(&[
            "--queue-policy", "shared_fifo", "--receive-trace", "/dev/null/trace.jsonl",
            "--receive-trace-capacity", "512",
        ]))
        .expect("shared_fifo with receive trace parses");
        assert_eq!(args.queue_policy, QueuePolicy::SharedFifo);
        assert!(args.receive_trace.is_some());
        assert!(args.arm == Arm::B1 && args.payload_mode == PayloadMode::Frame);
        validate_phase4_v5_args(&args).unwrap();
        validate_queue_policy(args.queue_policy, args.arm, args.payload_mode).unwrap();
    }

    /// Path 1 — normal FIN, complete reception. The only rc=0 case.
    #[test]
    fn fin_on_both_tracks_is_normal() {
        let reports = vec![
            rep("pc", TrackEnd::Fin, 180, Some(180)),
            rep("haptic", TrackEnd::Fin, 600, Some(600)),
        ];
        let (e, d) = classify_ending(&reports, false);
        assert_eq!(e, RxEnding::Normal, "{d}");
        assert_eq!(e.exit_code(), 0);
        assert!(d.contains("rule=fin"));
    }

    /// A FIN is still normal without expectations: `Done` is unambiguous, so a
    /// lossy-but-cleanly-finished run must not be failed. This guards the loss
    /// conditions in the matrix, where fewer objects legitimately arrive.
    #[test]
    fn fin_without_expectations_is_normal_even_if_lossy() {
        let reports = vec![
            rep("pc", TrackEnd::Fin, 54, None),
            rep("haptic", TrackEnd::Fin, 600, None),
        ];
        let (e, _) = classify_ending(&reports, false);
        assert_eq!(
            e,
            RxEnding::Normal,
            "a clean FIN is authoritative regardless of count"
        );
    }

    /// Path 2 — publisher cancel with partial reception. Must be non-zero and
    /// distinguishable. This is the case that previously reported rc=0.
    #[test]
    fn cancel_with_partial_reception_is_non_zero() {
        let reports = vec![
            rep("pc", TrackEnd::Cancelled, 90, Some(180)),
            rep("haptic", TrackEnd::Fin, 600, Some(600)),
        ];
        let (e, d) = classify_ending(&reports, false);
        assert_eq!(e, RxEnding::CancelledIncomplete, "{d}");
        assert_eq!(e.exit_code(), EXIT_RX_CANCELLED_INCOMPLETE);
        assert_ne!(e.exit_code(), 0);
        assert!(
            d.contains("rule=cancel_incomplete"),
            "reason must be recorded: {d}"
        );
        assert!(d.contains("pc=90/180"), "shortfall must be recorded: {d}");
    }

    /// Cancel with no expectation supplied cannot be proven complete, so it
    /// must not claim normal.
    #[test]
    fn cancel_without_expectation_is_non_zero() {
        let reports = vec![
            rep("pc", TrackEnd::Cancelled, 180, None),
            rep("haptic", TrackEnd::Fin, 600, None),
        ];
        let (e, d) = classify_ending(&reports, false);
        assert_eq!(e, RxEnding::CancelledIncomplete);
        assert!(d.contains("rule=cancel_without_expectation"), "{d}");
        assert!(
            d.contains("--duration-s"),
            "must say how to resolve it: {d}"
        );
    }

    /// Cancel that provably delivered the design quantity is a real completion.
    #[test]
    fn cancel_with_complete_reception_is_normal() {
        let reports = vec![
            rep("pc", TrackEnd::Cancelled, 180, Some(180)),
            rep("haptic", TrackEnd::Cancelled, 601, Some(600)),
        ];
        let (e, d) = classify_ending(&reports, false);
        assert_eq!(e, RxEnding::Normal, "{d}");
        assert!(d.contains("rule=cancel_but_complete"), "{d}");
    }

    /// Path 3 — session ended. Must outrank a cancelled drain, which is exactly
    /// the race that used to mask relay death as `normal`.
    #[test]
    fn session_finished_outranks_cancelled_drains() {
        let reports = vec![
            rep("pc", TrackEnd::Cancelled, 90, Some(180)),
            rep("haptic", TrackEnd::Cancelled, 300, Some(600)),
        ];
        let (e, d) = classify_ending(&reports, true);
        assert_eq!(e, RxEnding::SessionEnded, "{d}");
        assert_eq!(e.exit_code(), EXIT_RX_SESSION_ENDED);
        assert!(d.contains("rule=session_finished"));
    }

    /// Even a fully complete cancel must not report normal if the session died.
    #[test]
    fn session_finished_outranks_complete_cancel() {
        let reports = vec![
            rep("pc", TrackEnd::Cancelled, 180, Some(180)),
            rep("haptic", TrackEnd::Cancelled, 600, Some(600)),
        ];
        let (e, _) = classify_ending(&reports, true);
        assert_eq!(e, RxEnding::SessionEnded);
    }

    /// A genuine drain failure outranks everything.
    #[test]
    fn drain_failure_outranks_session_end() {
        let reports = vec![
            rep("pc", TrackEnd::Failed, 0, Some(180)),
            rep("haptic", TrackEnd::Fin, 600, Some(600)),
        ];
        let (e, d) = classify_ending(&reports, true);
        assert_eq!(e, RxEnding::DrainError, "{d}");
        assert_eq!(e.exit_code(), EXIT_RX_DRAIN_ERROR);
        assert!(d.contains("rule=drain_failed"));
    }

    /// Path 4 — C3 single-track. The unused track is closed with zero objects
    /// and MUST NOT be treated as a loss, whether it FINs or is cancelled.
    /// A false positive here stops the matrix on its first run under STRICT=1,
    /// which is exactly what an earlier revision did.
    #[test]
    fn c3_unused_track_closed_empty_is_normal() {
        for unused_end in [TrackEnd::Fin, TrackEnd::Cancelled] {
            let reports = vec![
                rep("pc", TrackEnd::Fin, 180, Some(180)),
                // haptic disabled by --tracks pc: expected 0, received 0.
                rep("haptic", unused_end, 0, Some(0)),
            ];
            let (e, d) = classify_ending(&reports, false);
            assert_eq!(
                e,
                RxEnding::Normal,
                "unused C3 track {unused_end:?} must be normal: {d}"
            );
            assert_eq!(e.exit_code(), 0);
        }
    }

    /// The same, mirrored: pc disabled.
    #[test]
    fn c3_haptic_only_is_normal() {
        let reports = vec![
            rep("pc", TrackEnd::Cancelled, 0, Some(0)),
            rep("haptic", TrackEnd::Fin, 600, Some(600)),
        ];
        let (e, d) = classify_ending(&reports, false);
        assert_eq!(e, RxEnding::Normal, "{d}");
    }

    /// `Cancel` must never classify as a FIN — the defect this all came from.
    #[test]
    fn cancel_is_not_a_fin() {
        use moq_transport::serve::ServeError;
        assert_eq!(classify_track_end(&ServeError::Done), TrackEnd::Fin);
        assert_eq!(classify_track_end(&ServeError::Cancel), TrackEnd::Cancelled);
        assert_eq!(
            classify_track_end(&ServeError::Closed(0)),
            TrackEnd::Cancelled
        );
        assert_eq!(
            classify_track_end(&ServeError::Closed(1)),
            TrackEnd::Cancelled
        );
        assert_eq!(classify_track_end(&ServeError::NotFound), TrackEnd::Failed);
        assert_eq!(classify_track_end(&ServeError::Duplicate), TrackEnd::Failed);
        assert_eq!(
            classify_track_end(&ServeError::Internal("x".into())),
            TrackEnd::Failed
        );
    }

    /// The declared topology and the wiring actually built must agree, in both
    /// directions. A mislabelled log would silently move a run between the `Md`
    /// and `M` arms, which is the contrast the batch measures.
    #[test]
    fn topology_must_match_listen_wiring() {
        assert!(validate_topology(Topology::Direct, true).is_ok());
        assert!(validate_topology(Topology::Relay, false).is_ok());

        let err = validate_topology(Topology::Direct, false)
            .unwrap_err()
            .to_string();
        assert!(err.contains("--topology direct requires --listen"), "{err}");
        let err = validate_topology(Topology::Relay, true)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("--topology relay must not be combined"),
            "{err}"
        );
    }

    /// `--topology` has no default and no unregistered value, at the CLI layer
    /// rather than only at `validate_topology`.  A defaulted topology would
    /// label an `Md` run as `M` in a syntactically valid log.
    #[test]
    fn topology_cli_is_required_and_closed() {
        use clap::Parser as _;
        let base: Vec<String> = [
            "moq_receiver",
            "--run-id",
            "t",
            "--out",
            "/dev/null",
            "--s-bytes",
            "1",
            "--pc-rate-hz",
            "30",
            "--haptic-rate-hz",
            "90",
            "--payload-mode",
            "frame",
            "--representation",
            "bin",
            "--chunk-bytes",
            "178",
            "--reassembly-max-pending-frames",
            "64",
            "--reassembly-max-pending-bytes",
            "67108864",
            "--reassembly-max-age-ms",
            "2000",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();

        assert!(
            Args::try_parse_from(&base).is_err(),
            "missing --topology parsed"
        );

        let mut bad = base.clone();
        bad.extend(["--topology".to_string(), "sfu".to_string()]);
        assert!(
            Args::try_parse_from(&bad).is_err(),
            "unregistered topology parsed"
        );

        for (value, expected) in [("direct", Topology::Direct), ("relay", Topology::Relay)] {
            let mut good = base.clone();
            good.extend(["--topology".to_string(), value.to_string()]);
            let args = Args::try_parse_from(&good).expect("registered topology");
            assert_eq!(args.topology, expected);
        }
    }

    /// Expectations must follow C3: a disabled track expects exactly 0, and an
    /// absent `--duration-s` leaves every expectation unknown.
    #[test]
    fn expectations_follow_tracks_and_duration() {
        let base = |tracks, duration_s| Args {
            relay: Url::parse("https://127.0.0.1:1").unwrap(),
            // 직결(Md) 토폴로지 필드. 이 픽스처들은 릴레이 경로를 검사하므로
            // None 이 등록된 기본이다. 필드를 추가할 때 픽스처를 함께 고치지
            // 않으면 `verify.sh quick` 이 컴파일 단계에서 멈춘다.
            listen: None,
            tls_cert: None,
            tls_key: None,
            run_id: "t".into(),
            out: PathBuf::from("/dev/null"),
            receive_trace: None,
            receive_trace_capacity: 4096,
            s_bytes: 1,
            c_mbps: None,
            rtt_ms: 0.0,
            jitter_ms: 0.0,
            loss_pct: 0.0,
            seed: 0,
            data_priority_mapping: DataPriorityMapping::LegacyV1,
            pc_rate_hz: 30,
            haptic_rate_hz: 90,
            payload_mode: PayloadMode::Frame,
            representation: Representation::Bin,
            // 릴레이 경로 픽스처이므로 선언 토폴로지도 relay 다.
            // (`listen: None` 과의 정합성은 validate_topology 가 강제한다.)
            topology: Topology::Relay,
            chunk_bytes: 178,
            reassembly_max_pending_frames: 64,
            reassembly_max_pending_bytes: 64 * 1024 * 1024,
            reassembly_max_age_ms: 2_000,
            max_duration: 10.0,
            render: false,
            audio: false,
            draco: false,
            python: String::new(),
            tracks,
            duration_s,
            subscribe_timeout: 10.0,
            subscribe_retry_ms: 100,
            arm: Arm::B1,
            queue_policy: QueuePolicy::Separate,
            d_play_ms: None,
            startup_timeout_ms: None,
            startup_rearm_limit: None,
            late_tolerance_ms: None,
            buffer_max_objects_per_track: None,
            buffer_max_span_ms: None,
            late_policy: None,
            pc_delivery_timeout_ms: None,
            s3_window_ms: None,
            s3_ewma_alpha: None,
            s3_miss_streak_threshold: None,
            s3_violation_ratio_threshold: None,
            s3_target_skew_ms: None,
            s3_recovery_fraction: None,
            s3_haptic_critical_stable_ms: None,
            s3_recovery_stable_ms: None,
            s3_cooldown_ms: None,
            s3_min_paired_samples: None,
            s3_max_window_samples: None,
            s3_deadline_max_anchors: None,
            s3_effect_timeout_ms: None,
            s3_initial_retry_limit: None,
            s3_switch_retry_limit: None,
            s3_test_mode: false,
            s3_test_force_misses_after_ms: None,
        };

        let a = base(RxTrackSel::Both, Some(60.0));
        assert_eq!(expected_for("pc", &a), Some(1800));
        assert_eq!(expected_for("haptic", &a), Some(5400));

        let a = base(RxTrackSel::Pc, Some(60.0));
        assert_eq!(expected_for("pc", &a), Some(1800));
        assert_eq!(
            expected_for("haptic", &a),
            Some(0),
            "disabled track expects 0"
        );

        let a = base(RxTrackSel::Haptic, Some(60.0));
        assert_eq!(expected_for("pc", &a), Some(0));
        assert_eq!(expected_for("haptic", &a), Some(5400));

        let a = base(RxTrackSel::Both, None);
        assert_eq!(expected_for("pc", &a), None, "no duration => unprovable");
        assert_eq!(expected_for("haptic", &a), None);
    }

    /// Every abnormal ending must map to a distinct non-zero code, and only
    /// `Normal` may be zero.
    #[test]
    fn exit_codes_are_distinct_and_only_normal_is_zero() {
        let all = [
            RxEnding::Normal,
            RxEnding::DrainError,
            RxEnding::SessionEnded,
            RxEnding::Timeout,
            RxEnding::CancelledIncomplete,
            RxEnding::HeaderInvalid,
            RxEnding::NoObjects,
        ];
        let mut codes: Vec<i32> = all.iter().map(|e| e.exit_code()).collect();
        let n = codes.len();
        codes.sort_unstable();
        codes.dedup();
        assert_eq!(codes.len(), n, "exit codes must be distinct");
        for e in all {
            assert_eq!(e == RxEnding::Normal, e.exit_code() == 0, "{e:?}");
        }
    }

    /// R2 — a track expected to carry objects that received exactly zero is an
    /// unambiguous failure and must exit non-zero.
    #[test]
    fn zero_objects_with_expectation_is_non_zero() {
        let reports = vec![
            rep("pc", TrackEnd::Fin, 0, Some(800)),
            rep("haptic", TrackEnd::Fin, 0, Some(800)),
        ];
        let (e, d) = classify_ending(&reports, false);
        assert_eq!(e, RxEnding::NoObjects, "{d}");
        assert_eq!(e.exit_code(), EXIT_RX_NO_OBJECTS);
        assert_ne!(e.exit_code(), 0);
        assert!(d.contains("rule=zero_objects_expected"), "{d}");
        assert!(d.contains("pc=0/800"), "shortfall must be recorded: {d}");
    }

    #[test]
    fn s2_pc_zero_can_finish_only_for_external_timeout_accounting() {
        let reports = vec![
            rep("pc", TrackEnd::Fin, 0, Some(180)),
            rep("haptic", TrackEnd::Fin, 600, Some(600)),
        ];
        let (normal, detail) = classify_ending_with_timeout(&reports, false, true);
        assert_eq!(normal, RxEnding::Normal, "{detail}");
        assert!(detail.contains("requires_timeout_accounting"), "{detail}");

        let (blocked, _) = classify_ending(&reports, false);
        assert_eq!(blocked, RxEnding::NoObjects);
    }

    /// One dead track is enough, even if the other is complete.
    #[test]
    fn one_zero_track_is_enough_to_fail() {
        let reports = vec![
            rep("pc", TrackEnd::Fin, 180, Some(180)),
            rep("haptic", TrackEnd::Fin, 0, Some(600)),
        ];
        let (e, d) = classify_ending(&reports, false);
        assert_eq!(e, RxEnding::NoObjects, "{d}");
        assert!(d.contains("haptic=0/600"), "{d}");
    }

    /// R2 boundary — partial reception stays rc=0. Failing it would break every
    /// LOSS condition in the matrix, where a shortfall is the expected result.
    #[test]
    fn partial_reception_is_still_normal() {
        for received in [1u64, 5, 90, 179] {
            let reports = vec![
                rep("pc", TrackEnd::Fin, received, Some(180)),
                rep("haptic", TrackEnd::Fin, 600, Some(600)),
            ];
            let (e, d) = classify_ending(&reports, false);
            assert_eq!(
                e,
                RxEnding::Normal,
                "received={received} must stay normal: {d}"
            );
            assert_eq!(e.exit_code(), 0);
        }
    }

    /// R2 boundary — a C3 disabled track expects 0, so receiving 0 is correct
    /// and must not trip the zero-object rule. This is the false positive that
    /// would stop a STRICT=1 batch on its first single-track run.
    #[test]
    fn c3_disabled_track_expecting_zero_does_not_trip_no_objects() {
        // --tracks pc: haptic disabled, expects 0, receives 0.
        let reports = vec![
            rep("pc", TrackEnd::Fin, 180, Some(180)),
            rep("haptic", TrackEnd::Fin, 0, Some(0)),
        ];
        let (e, d) = classify_ending(&reports, false);
        assert_eq!(e, RxEnding::Normal, "{d}");
        assert_eq!(e.exit_code(), 0);

        // --tracks haptic: mirrored.
        let reports = vec![
            rep("pc", TrackEnd::Fin, 0, Some(0)),
            rep("haptic", TrackEnd::Fin, 600, Some(600)),
        ];
        let (e, d) = classify_ending(&reports, false);
        assert_eq!(e, RxEnding::Normal, "{d}");
    }

    /// R2 boundary — without `--duration-s` there is no expectation, so the
    /// rule cannot fire. Zero objects then stays rc=0, matching the existing
    /// harness invocation that passes no design length.
    #[test]
    fn zero_objects_without_expectation_does_not_trip() {
        let reports = vec![
            rep("pc", TrackEnd::Fin, 0, None),
            rep("haptic", TrackEnd::Fin, 0, None),
        ];
        let (e, d) = classify_ending(&reports, false);
        assert_eq!(
            e,
            RxEnding::Normal,
            "unprovable, so not claimed as failure: {d}"
        );
    }

    /// Severity order: a drain failure and a dead session both outrank the
    /// zero-object rule, so the most specific cause is reported.
    #[test]
    fn zero_objects_ranks_below_failure_and_session_end() {
        let reports = vec![rep("pc", TrackEnd::Fin, 0, Some(180))];
        assert_eq!(classify_ending(&reports, true).0, RxEnding::SessionEnded);

        let reports = vec![
            rep("pc", TrackEnd::Failed, 0, Some(180)),
            rep("haptic", TrackEnd::Fin, 0, Some(600)),
        ];
        assert_eq!(classify_ending(&reports, false).0, RxEnding::DrainError);
    }

    /// R1 — only "track not found" is retryable. Retrying anything else would
    /// bury a genuine fault behind the polling budget.
    #[test]
    fn only_track_not_found_is_retryable() {
        use moq_transport::serve::ServeError;
        assert!(is_retryable_subscribe_error(&ServeError::NotFound));
        assert!(is_retryable_subscribe_error(&ServeError::NotFoundWithId(
            "Track not found".into(),
            uuid::Uuid::nil()
        )));
        // The representation that actually occurs on the wire against the
        // relay: RequestErrorCode::DoesNotExist. Matching only the local
        // variants made the receiver fail instantly instead of recovering.
        assert!(
            is_retryable_subscribe_error(&ServeError::Closed(0x10)),
            "wire DoesNotExist must be retryable"
        );
        for e in [
            ServeError::Duplicate,
            ServeError::Cancel,
            ServeError::Done,
            ServeError::Closed(0x0),  // InternalError
            ServeError::Closed(0x1),  // Unauthorized
            ServeError::Closed(0x2),  // Timeout
            ServeError::Closed(0x3),  // NotSupported
            ServeError::Closed(0x4),  // MalformedAuthToken (ambiguous -> fatal)
            ServeError::Closed(0x11), // InvalidRange
            ServeError::Closed(0x12), // MalformedTrack
            ServeError::Closed(0x19), // DuplicateSubscription
            ServeError::Mode,
            ServeError::Size,
            ServeError::Internal("x".into()),
            ServeError::NotImplemented("x".into()),
        ] {
            assert!(
                !is_retryable_subscribe_error(&e),
                "{e:?} must not be retried"
            );
        }
    }

    #[test]
    fn json_escape_keeps_detail_parseable() {
        let s = json_escape("a\"b\\c\nd\te");
        assert_eq!(s, "a\\\"b\\\\c\\nd\\te");
        assert!(!json_escape("x\u{1}y").contains('\u{1}'));
    }

    #[test]
    fn s1_cli_requires_explicit_recorded_parameters() {
        let base = Args {
            relay: Url::parse("https://127.0.0.1:1").unwrap(),
            // 직결(Md) 토폴로지 필드. 이 픽스처들은 릴레이 경로를 검사하므로
            // None 이 등록된 기본이다. 필드를 추가할 때 픽스처를 함께 고치지
            // 않으면 `verify.sh quick` 이 컴파일 단계에서 멈춘다.
            listen: None,
            tls_cert: None,
            tls_key: None,
            run_id: "t".into(),
            out: PathBuf::from("/dev/null"),
            receive_trace: None,
            receive_trace_capacity: 4096,
            s_bytes: 1,
            c_mbps: None,
            rtt_ms: 0.0,
            jitter_ms: 0.0,
            loss_pct: 0.0,
            seed: 0,
            data_priority_mapping: DataPriorityMapping::LegacyV1,
            pc_rate_hz: 30,
            haptic_rate_hz: 90,
            payload_mode: PayloadMode::Frame,
            representation: Representation::Bin,
            // 릴레이 경로 픽스처이므로 선언 토폴로지도 relay 다.
            // (`listen: None` 과의 정합성은 validate_topology 가 강제한다.)
            topology: Topology::Relay,
            chunk_bytes: 178,
            reassembly_max_pending_frames: 64,
            reassembly_max_pending_bytes: 64 * 1024 * 1024,
            reassembly_max_age_ms: 2_000,
            max_duration: 10.0,
            render: false,
            audio: false,
            draco: false,
            python: String::new(),
            tracks: RxTrackSel::Both,
            duration_s: Some(1.0),
            subscribe_timeout: 10.0,
            subscribe_retry_ms: 100,
            arm: Arm::S1,
            queue_policy: QueuePolicy::Separate,
            d_play_ms: Some(50),
            startup_timeout_ms: Some(100),
            startup_rearm_limit: Some(1),
            late_tolerance_ms: Some(5),
            buffer_max_objects_per_track: Some(64),
            buffer_max_span_ms: Some(250),
            late_policy: Some(CliLatePolicy::DropLate),
            pc_delivery_timeout_ms: None,
            s3_window_ms: None,
            s3_ewma_alpha: None,
            s3_miss_streak_threshold: None,
            s3_violation_ratio_threshold: None,
            s3_target_skew_ms: None,
            s3_recovery_fraction: None,
            s3_haptic_critical_stable_ms: None,
            s3_recovery_stable_ms: None,
            s3_cooldown_ms: None,
            s3_min_paired_samples: None,
            s3_max_window_samples: None,
            s3_deadline_max_anchors: None,
            s3_effect_timeout_ms: None,
            s3_initial_retry_limit: None,
            s3_switch_retry_limit: None,
            s3_test_mode: false,
            s3_test_force_misses_after_ms: None,
        };
        let cfg = playout_config(&base).unwrap().unwrap();
        assert_eq!(cfg.d_play_us, 50_000);
        assert_eq!(cfg.startup_rearm_limit, 1);
        assert_eq!(cfg.max_objects_per_track, 64);

        let mut bad = base;
        bad.d_play_ms = Some(75);
        assert!(playout_config(&bad)
            .unwrap_err()
            .to_string()
            .contains("50 or 100"));
        bad.d_play_ms = Some(50);
        bad.startup_rearm_limit = Some(0);
        assert!(playout_config(&bad)
            .unwrap_err()
            .to_string()
            .contains("startup-rearm-limit"));
    }

    #[test]
    fn m1_and_s2_cli_preserve_the_ablation_boundary() {
        let mut args = Args {
            relay: Url::parse("https://127.0.0.1:1").unwrap(),
            // 직결(Md) 토폴로지 필드. 이 픽스처들은 릴레이 경로를 검사하므로
            // None 이 등록된 기본이다. 필드를 추가할 때 픽스처를 함께 고치지
            // 않으면 `verify.sh quick` 이 컴파일 단계에서 멈춘다.
            listen: None,
            tls_cert: None,
            tls_key: None,
            run_id: "t".into(),
            out: PathBuf::from("/dev/null"),
            receive_trace: None,
            receive_trace_capacity: 4096,
            s_bytes: 1,
            c_mbps: None,
            rtt_ms: 0.0,
            jitter_ms: 0.0,
            loss_pct: 0.0,
            seed: 0,
            data_priority_mapping: DataPriorityMapping::LegacyV1,
            pc_rate_hz: 30,
            haptic_rate_hz: 90,
            payload_mode: PayloadMode::Frame,
            representation: Representation::Bin,
            // 릴레이 경로 픽스처이므로 선언 토폴로지도 relay 다.
            // (`listen: None` 과의 정합성은 validate_topology 가 강제한다.)
            topology: Topology::Relay,
            chunk_bytes: 178,
            reassembly_max_pending_frames: 64,
            reassembly_max_pending_bytes: 16 * 1024 * 1024,
            reassembly_max_age_ms: 500,
            max_duration: 10.0,
            render: false,
            audio: false,
            draco: false,
            python: String::new(),
            tracks: RxTrackSel::Both,
            duration_s: Some(1.0),
            subscribe_timeout: 10.0,
            subscribe_retry_ms: 100,
            arm: Arm::M1,
            queue_policy: QueuePolicy::Separate,
            d_play_ms: Some(50),
            startup_timeout_ms: Some(100),
            startup_rearm_limit: Some(1),
            late_tolerance_ms: Some(5),
            buffer_max_objects_per_track: Some(64),
            buffer_max_span_ms: Some(250),
            late_policy: Some(CliLatePolicy::DropLate),
            pc_delivery_timeout_ms: None,
            s3_window_ms: None,
            s3_ewma_alpha: None,
            s3_miss_streak_threshold: None,
            s3_violation_ratio_threshold: None,
            s3_target_skew_ms: None,
            s3_recovery_fraction: None,
            s3_haptic_critical_stable_ms: None,
            s3_recovery_stable_ms: None,
            s3_cooldown_ms: None,
            s3_min_paired_samples: None,
            s3_max_window_samples: None,
            s3_deadline_max_anchors: None,
            s3_effect_timeout_ms: None,
            s3_initial_retry_limit: None,
            s3_switch_retry_limit: None,
            s3_test_mode: false,
            s3_test_force_misses_after_ms: None,
        };

        assert!(playout_config(&args).is_ok());
        assert!(validate_phase4_v5_args(&args).is_ok());
        let m1 = phase4_transport(&args).unwrap();
        assert_eq!(m1.arm, "m1");
        assert_eq!(m1.pc_publisher_priority, 128);
        assert_eq!(m1.pc_delivery_timeout_ms, None);

        args.pc_delivery_timeout_ms = Some(67);
        assert!(playout_config(&args).is_err(), "M1 must reject timeout");

        args.arm = Arm::S2;
        assert!(playout_config(&args).is_ok());
        assert!(validate_phase4_v5_args(&args).is_err());
        args.data_priority_mapping = DataPriorityMapping::MoqtV2;
        assert!(validate_phase4_v5_args(&args).is_ok());
        let s2 = phase4_transport(&args).unwrap();
        assert_eq!(s2.pc_publisher_priority, 1);
        assert_eq!(s2.haptic_publisher_priority, 0);
        assert_eq!(s2.pc_delivery_timeout_ms, Some(67));

        args.arm = Arm::S2Eq;
        let s2eq = phase4_transport(&args).unwrap();
        assert!(playout_config(&args).is_ok());
        assert_eq!(s2eq.arm, "s2eq");
        assert_eq!(s2eq.pc_publisher_priority, 128);
        assert_eq!(s2eq.haptic_publisher_priority, 128);
        assert_eq!(s2eq.publisher_priority_profile, "equal-128");
        assert_eq!(s2eq.pc_delivery_timeout_ms, Some(67));

        args.pc_delivery_timeout_ms = Some(0);
        assert!(playout_config(&args).is_err(), "timeout zero is invalid");

        args.arm = Arm::S3;
        args.pc_delivery_timeout_ms = Some(67);
        args.startup_timeout_ms = Some(2_000);
        args.late_tolerance_ms = Some(10);
        args.s3_window_ms = Some(1_000);
        args.s3_ewma_alpha = Some(0.2);
        args.s3_miss_streak_threshold = Some(3);
        args.s3_violation_ratio_threshold = Some(0.2);
        args.s3_target_skew_ms = Some(25);
        args.s3_recovery_fraction = Some(0.5);
        args.s3_haptic_critical_stable_ms = Some(3_000);
        args.s3_recovery_stable_ms = Some(5_000);
        args.s3_cooldown_ms = Some(2_000);
        args.s3_min_paired_samples = Some(5);
        args.s3_max_window_samples = Some(128);
        args.s3_effect_timeout_ms = Some(1_000);
        args.s3_initial_retry_limit = Some(20);
        args.s3_switch_retry_limit = Some(2);
        assert!(playout_config(&args).is_ok());
        // 계약 §2.1 규칙 E: the deadline-tracker bound is a separate recorded
        // parameter, never derived from the scheduler buffer bound, and like
        // every other S3 parameter it has no implicit default.
        assert!(
            s3_runtime_config(&args).is_err(),
            "S3 must reject an implicit deadline-tracker bound"
        );
        args.s3_deadline_max_anchors = Some(900);
        let runtime = s3_runtime_config(&args).unwrap().unwrap();
        assert_eq!(runtime.controller.target_skew_us, 25_000);
        assert_eq!(runtime.switch.effect_timeout_us, 1_000_000);
        assert_eq!(runtime.initial_retry_limit, 20);
        assert_eq!(runtime.switch_retry_limit, 2);
        assert_eq!(runtime.deadline_max_anchors, 900);
        assert_ne!(
            runtime.deadline_max_anchors,
            playout_config(&args)
                .unwrap()
                .unwrap()
                .max_objects_per_track,
            "the bound must not be coupled to the scheduler buffer bound"
        );

        args.s3_target_skew_ms = Some(30);
        assert!(s3_runtime_config(&args).is_err());
    }
}

// ---- S3 switch-barrier ordering-race tests ---------------------------------
//
// `S3ReceiverIngress::push` emits `Scheduler(routed)` before `Applied`, and
// `PlayoutScheduler::push` internally advances the timeline. An overdue
// cancelled-route object above the barrier PTS can therefore already be
// staged as a `Release` in the same batch before the `Applied` arm runs.
// These tests drive the same ingress/scheduler/tracker composition as the
// `run_s3_receiver` event loop, without a network.

#[cfg(test)]
mod s3_barrier_race_tests {
    use super::*;
    use skew_moq::playout::{DROP_BUFFER_SPAN_LIMIT, DROP_LATE, DROP_STARTUP_TIMEOUT};
    use skew_moq::s3_controller::{S3State, S3Transition, TransitionCause};
    use skew_moq::s3_switch::Routes;

    fn routed(
        role: TrackRole,
        route: Route,
        pts_us: u64,
        event_id: u32,
        t_recv: u64,
    ) -> RoutedObject {
        let (track_id, tier) = match role {
            TrackRole::Pc => (
                TRACK_PC,
                match route.name {
                    "pc" => 2,
                    "pc-d7" => 3,
                    "pc-d6" => 4,
                    _ => 99,
                },
            ),
            TrackRole::Haptic => (TRACK_HAPTIC, 0),
        };
        RoutedObject {
            role,
            route,
            object: PlayoutObject {
                header: Header {
                    version: VERSION,
                    track_id,
                    tier,
                    seq: event_id,
                    pts_us,
                    event_id,
                    gen_ts_us: 1,
                    payload_len: 0,
                },
                t_recv,
                bytes: Bytes::new(),
            },
        }
    }

    fn transition(from: S3State, to: S3State, at_us: u64) -> S3Transition {
        S3Transition {
            at_us,
            from,
            to,
            cause: TransitionCause::DeadlineMissStreak,
        }
    }

    fn test_playout() -> PlayoutConfig {
        PlayoutConfig {
            d_play_us: 50_000,
            startup_timeout_us: 2_000_000,
            startup_rearm_limit: 1,
            late_tolerance_us: 50_000,
            max_objects_per_track: 64,
            max_span_us: 2_000_000,
            late_policy: LatePolicy::DropLate,
        }
    }

    /// Mirrors the per-event wiring of `run_s3_receiver`: ingress events feed
    /// the tracker, the route map, the scheduler, and the apply barrier in the
    /// same order, then the trailing `advance` and the dispatch accounting.
    struct Harness {
        ingress: S3ReceiverIngress,
        scheduler: PlayoutScheduler,
        tracker: S3DeadlineTracker,
        object_routes: HashMap<S3ObjectKey, (TrackRole, Route)>,
        released: Vec<S3ObjectKey>,
        dropped: Vec<(S3ObjectKey, &'static str)>,
        barrier_dropped: usize,
        pushed_to_scheduler: usize,
        duplicate_dropped: Vec<(S3ObjectKey, &'static str)>,
    }

    impl Harness {
        fn new() -> Self {
            let gate = S3SwitchGate::new(SwitchConfig {
                effect_timeout_us: 10_000_000,
            })
            .unwrap();
            Self {
                ingress: S3ReceiverIngress::new(gate, 64).unwrap(),
                scheduler: PlayoutScheduler::new(test_playout()).unwrap(),
                tracker: S3DeadlineTracker::new(64).unwrap(),
                object_routes: HashMap::new(),
                released: Vec::new(),
                dropped: Vec::new(),
                barrier_dropped: 0,
                pushed_to_scheduler: 0,
                duplicate_dropped: Vec::new(),
            }
        }

        fn push(&mut self, routed: RoutedObject, now: u64) -> Vec<PlayoutAction> {
            let mut scheduler_actions = Vec::new();
            for event in self.ingress.push(routed, now).unwrap() {
                match event {
                    IngressEvent::Scheduler(routed) => {
                        // Mirrors the receiver's duplicate-identity guard:
                        // an identity the scheduler already knows is
                        // terminally dropped BEFORE any tracker/route/push
                        // registration.
                        if self.scheduler.knows_identity(&routed.object) {
                            self.duplicate_dropped
                                .push((S3ObjectKey::from(&routed.object), DROP_DUPLICATE_IDENTITY));
                            continue;
                        }
                        self.tracker.note_received(&routed.object).unwrap();
                        let key = S3ObjectKey::from(&routed.object);
                        assert!(
                            self.object_routes
                                .insert(key, (routed.role, routed.route))
                                .is_none(),
                            "duplicate S3 scheduler identity"
                        );
                        self.pushed_to_scheduler += 1;
                        scheduler_actions.extend(self.scheduler.push(routed.object, now));
                    }
                    IngressEvent::Drop { .. } => {
                        self.barrier_dropped += 1;
                    }
                    IngressEvent::Applied(applied) => {
                        apply_s3_route_barrier(
                            &applied,
                            &mut self.scheduler,
                            &mut scheduler_actions,
                            &self.object_routes,
                            &mut self.tracker,
                        )
                        .unwrap();
                    }
                }
            }
            scheduler_actions.extend(self.scheduler.advance(now));
            self.dispatch(&scheduler_actions, now);
            scheduler_actions
        }

        /// The receiver's end-of-iteration expiry + bound check (P2), calling
        /// the production function so the ordering is fixed by the test.
        fn advance_tracker(&mut self, now: u64) -> Vec<S3Observation> {
            advance_tracker_checked(&mut self.tracker, &self.scheduler, now)
                .map_err(|error| error.message())
                .unwrap()
        }

        /// Mirrors `dispatch_s3_playout_actions` accounting. The route-map
        /// removal is the conservation check: exactly one terminal action per
        /// scheduler-entered object, never two.
        fn dispatch(&mut self, actions: &[PlayoutAction], now: u64) {
            // The production settlement function itself, not a copy of it.
            settle_tracker_batch(&mut self.tracker, actions, now).unwrap();
            for action in actions {
                let key = S3ObjectKey::from(action.object());
                assert!(
                    self.object_routes.remove(&key).is_some(),
                    "missing S3 route for terminal scheduler action"
                );
                match action {
                    PlayoutAction::Release(_) => self.released.push(key),
                    PlayoutAction::Drop { reason, .. } => self.dropped.push((key, reason)),
                }
            }
        }
    }

    /// Required test 1 — the ordering race itself. Recovery -> Normal where
    /// the unchanged-route haptic anchor arrives last, `now` is beyond
    /// several deadlines, and the cancelled PC route has objects above the
    /// barrier PTS already overdue in the buffer. `scheduler.push(anchor)`
    /// stages them as releases before the `Applied` event is handled; every
    /// such object must still end as exactly one `stale_tier` terminal drop,
    /// zero releases, and no retained tracker observation.
    #[test]
    fn overdue_cancelled_route_releases_staged_before_apply_become_stale_drops() {
        let mut h = Harness::new();
        let gen0 = h.ingress.gate().active_routes();

        // Exact startup pair on generation-0 routes: epoch at now=2_000, so
        // due(pts) = 52_000 + pts.
        h.push(routed(TrackRole::Pc, gen0.pc, 0, 1, 1_000), 1_000);
        h.push(routed(TrackRole::Haptic, gen0.haptic, 0, 1, 2_000), 2_000);
        assert!(h.scheduler.is_started());
        h.tracker.activate().unwrap();

        // Normal -> HapticCritical (both routes change), applied at pts 10_000.
        let first = h
            .ingress
            .request(
                transition(S3State::Normal, S3State::HapticCritical, 3_000),
                3_000,
            )
            .unwrap();
        h.ingress.subscribe_ok(TrackRole::Pc, 3_100).unwrap();
        h.ingress.subscribe_ok(TrackRole::Haptic, 3_200).unwrap();
        h.push(
            routed(TrackRole::Pc, first.target.pc, 10_000, 2, 3_300),
            3_300,
        );
        h.push(
            routed(TrackRole::Haptic, first.target.haptic, 10_000, 2, 3_400),
            3_400,
        );

        // HapticCritical -> Recovery (both change), applied at pts 20_000.
        let second = h
            .ingress
            .request(
                transition(S3State::HapticCritical, S3State::Recovery, 4_000),
                4_000,
            )
            .unwrap();
        h.ingress.subscribe_ok(TrackRole::Pc, 4_100).unwrap();
        h.ingress.subscribe_ok(TrackRole::Haptic, 4_200).unwrap();
        h.push(
            routed(TrackRole::Pc, second.target.pc, 20_000, 3, 4_300),
            4_300,
        );
        h.push(
            routed(TrackRole::Haptic, second.target.haptic, 20_000, 3, 4_400),
            4_400,
        );
        let recovery = h.ingress.gate().active_routes();
        assert_eq!(recovery.pc.name, "pc-d7");

        // Old-route PC objects above the coming barrier (pts 30_000), plus a
        // current-route haptic sibling for pts 60_000 that must survive.
        h.push(routed(TrackRole::Pc, recovery.pc, 60_000, 6, 5_000), 5_000);
        h.push(routed(TrackRole::Pc, recovery.pc, 70_000, 7, 5_100), 5_100);
        h.push(
            routed(TrackRole::Haptic, recovery.haptic, 60_000, 6, 5_200),
            5_200,
        );

        // Recovery -> Normal: only PC changes; haptic stays current.
        let third = h
            .ingress
            .request(transition(S3State::Recovery, S3State::Normal, 6_000), 6_000)
            .unwrap();
        assert!(third.pc_changed && !third.haptic_changed);
        h.ingress.subscribe_ok(TrackRole::Pc, 6_100).unwrap();
        assert!(h
            .push(
                routed(TrackRole::Pc, third.target.pc, 30_000, 4, 7_000),
                7_000
            )
            .is_empty());

        // The unchanged-route haptic anchor arrives LAST, with `now` beyond
        // the deadlines of pts 30_000/60_000/70_000 (due 82k/112k/122k, late
        // tolerance 50k keeps 60k/70k releasable).
        let actions = h.push(
            routed(TrackRole::Haptic, recovery.haptic, 30_000, 4, 130_000),
            130_000,
        );

        let stale: Vec<&PlayoutAction> = actions
            .iter()
            .filter(|action| {
                matches!(
                    action,
                    PlayoutAction::Drop {
                        reason: DROP_STALE_TIER,
                        ..
                    }
                )
            })
            .collect();
        assert_eq!(stale.len(), 2, "both overdue cancelled-route objects");
        for action in &stale {
            let header = action.object().header;
            assert_eq!(header.track_id, TRACK_PC);
            assert_eq!(header.tier, 3, "cancelled pc-d7 route");
            assert!(header.pts_us > 30_000);
        }
        // Zero releases escaped for the cancelled route above the barrier.
        for action in &actions {
            if let PlayoutAction::Release(object) = action {
                assert!(
                    object.header.track_id == TRACK_HAPTIC,
                    "cancelled-route PC release escaped the barrier: {:?}",
                    object.header
                );
            }
        }
        assert_eq!(
            actions
                .iter()
                .filter(|action| matches!(action, PlayoutAction::Release(_)))
                .count(),
            2,
            "haptic pts 30_000 anchor and haptic pts 60_000 still release"
        );

        // Scheduler terminal set holds the converted identities: re-pushing an
        // overdue duplicate must be ignored instead of being emitted again.
        let duplicate = routed(TrackRole::Pc, recovery.pc, 60_000, 6, 131_000).object;
        assert!(h.scheduler.push(duplicate, 131_000).is_empty());

        // The tracker retained no observation for the evicted objects: the
        // matching haptic anchors now report a plain PC deadline miss, never a
        // fabricated exact-pair release skew.
        let observations = h.tracker.advance(&h.scheduler, 130_000).unwrap();
        assert_eq!(observations.len(), 2, "pts 30_000 and pts 60_000 anchors");
        assert!(observations.iter().all(|obs| obs.deadline_miss));
        assert!(observations.iter().all(|obs| obs.abs_skew_us.is_none()));
        assert_eq!(h.tracker.next_wakeup_us(&h.scheduler), None);

        // Accounting conservation: every scheduler-entered object produced
        // exactly one terminal action (dispatch panics on double emission).
        assert_eq!(h.pushed_to_scheduler, 6);
        assert_eq!(h.released.len() + h.dropped.len(), 6);
        assert!(h.object_routes.is_empty());
        assert_eq!(
            h.dropped
                .iter()
                .filter(|(_, reason)| *reason == DROP_STALE_TIER)
                .count(),
            2
        );
        // Barrier-only target objects (2 + 2 + 1 across the three applies)
        // were terminally dropped by the ingress, never by the scheduler.
        assert_eq!(h.barrier_dropped, 5);
    }

    /// Required test 2 — applying the same cancellation twice must not double
    /// count: converted actions stay converted, the buffered pass finds
    /// nothing, and release+drop actions still conserve the pushed objects.
    #[test]
    fn repeated_apply_filtering_is_idempotent_and_conserves_accounting() {
        let route = Route {
            name: "pc-d7",
            generation: 2,
        };
        let mut scheduler = PlayoutScheduler::new(test_playout()).unwrap();
        let mut tracker = S3DeadlineTracker::new(64).unwrap();
        let mut object_routes: HashMap<S3ObjectKey, (TrackRole, Route)> = HashMap::new();

        let gen0 = Routes {
            pc: Route {
                name: "pc",
                generation: 0,
            },
            haptic: Route {
                name: "haptic",
                generation: 0,
            },
        };
        let mut pushed = 0usize;
        for routed_object in [
            routed(TrackRole::Pc, gen0.pc, 0, 1, 1_000),
            routed(TrackRole::Haptic, gen0.haptic, 0, 1, 2_000),
            routed(TrackRole::Pc, route, 60_000, 6, 3_000),
            routed(TrackRole::Pc, route, 70_000, 7, 3_100),
        ] {
            tracker.note_received(&routed_object.object).unwrap();
            object_routes.insert(
                S3ObjectKey::from(&routed_object.object),
                (routed_object.role, routed_object.route),
            );
            pushed += 1;
            let now = routed_object.object.t_recv;
            assert!(scheduler.push(routed_object.object, now).is_empty());
        }

        // Everything is overdue: pts 0 is beyond tolerance (late drops), the
        // pts 60k/70k pc-d7 objects are staged as releases.
        let mut actions = scheduler.advance(130_000);
        assert_eq!(actions.len(), pushed);

        let applied = SwitchApplied {
            decision_at_us: 6_000,
            request_at_us: 6_000,
            effect_at_us: 130_000,
            request_to_effect_us: 124_000,
            from: S3State::Recovery,
            to: S3State::Normal,
            cause: TransitionCause::DeadlineMissStreak,
            exact_pts_us: 30_000,
            exact_event_id: 4,
            active: Routes {
                pc: Route {
                    name: "pc",
                    generation: 3,
                },
                haptic: Route {
                    name: "haptic",
                    generation: 0,
                },
            },
            cancel_pc: Some(route),
            cancel_haptic: None,
        };

        let snapshot = |actions: &[PlayoutAction]| -> Vec<(S3ObjectKey, Option<&'static str>)> {
            actions
                .iter()
                .map(|action| match action {
                    PlayoutAction::Release(object) => (S3ObjectKey::from(object), None),
                    PlayoutAction::Drop { object, reason, .. } => {
                        (S3ObjectKey::from(object), Some(*reason))
                    }
                })
                .collect()
        };

        apply_s3_route_barrier(
            &applied,
            &mut scheduler,
            &mut actions,
            &object_routes,
            &mut tracker,
        )
        .unwrap();
        let first_pass = snapshot(&actions);
        assert_eq!(actions.len(), pushed, "no action added or lost");
        assert_eq!(
            first_pass
                .iter()
                .filter(|(_, reason)| *reason == Some(DROP_STALE_TIER))
                .count(),
            2
        );
        assert_eq!(
            first_pass
                .iter()
                .filter(|(_, reason)| reason.is_none())
                .count(),
            0,
            "no release survives for the cancelled route above the barrier"
        );

        // Second application of the identical cancellation: byte-for-byte the
        // same staged actions, an empty buffered eviction, no double drops.
        apply_s3_route_barrier(
            &applied,
            &mut scheduler,
            &mut actions,
            &object_routes,
            &mut tracker,
        )
        .unwrap();
        assert_eq!(snapshot(&actions), first_pass);
        assert_eq!(scheduler.buffered_counts(), (0, 0));

        // Terminal-set consistency: the converted identities can never be
        // scheduled again.
        for (pts_us, event_id) in [(60_000, 6), (70_000, 7)] {
            let duplicate = routed(TrackRole::Pc, route, pts_us, event_id, 131_000).object;
            assert!(scheduler.push(duplicate, 131_000).is_empty());
        }
    }

    /// Required test 3 — one-role transition: with only the PC route
    /// cancelled, staged haptic releases (and PC objects at the barrier PTS)
    /// must remain releases; only cancelled-route PC objects above the
    /// barrier convert.
    #[test]
    fn one_role_cancellation_leaves_haptic_and_at_barrier_releases_untouched() {
        let pc_route = Route {
            name: "pc-d7",
            generation: 2,
        };
        let haptic_route = Route {
            name: "haptic",
            generation: 2,
        };
        let mut scheduler = PlayoutScheduler::new(test_playout()).unwrap();
        let mut tracker = S3DeadlineTracker::new(64).unwrap();
        let mut object_routes: HashMap<S3ObjectKey, (TrackRole, Route)> = HashMap::new();

        for routed_object in [
            routed(TrackRole::Pc, pc_route, 0, 1, 1_000),
            routed(TrackRole::Haptic, haptic_route, 0, 1, 2_000),
            // At the barrier PTS: must stay a release.
            routed(TrackRole::Pc, pc_route, 30_000, 4, 3_000),
            // Above the barrier on the cancelled route: must convert.
            routed(TrackRole::Pc, pc_route, 60_000, 6, 3_100),
            // Above the barrier on the unchanged haptic route: must stay.
            routed(TrackRole::Haptic, haptic_route, 60_000, 6, 3_200),
        ] {
            tracker.note_received(&routed_object.object).unwrap();
            object_routes.insert(
                S3ObjectKey::from(&routed_object.object),
                (routed_object.role, routed_object.route),
            );
            let now = routed_object.object.t_recv;
            scheduler.push(routed_object.object, now);
        }

        // now=130_000: pts 0 is late-dropped, pts 30_000/60_000 stage releases.
        let mut actions = scheduler.advance(130_000);
        let applied = SwitchApplied {
            decision_at_us: 6_000,
            request_at_us: 6_000,
            effect_at_us: 130_000,
            request_to_effect_us: 124_000,
            from: S3State::Recovery,
            to: S3State::Normal,
            cause: TransitionCause::DeadlineMissStreak,
            exact_pts_us: 30_000,
            exact_event_id: 4,
            active: Routes {
                pc: Route {
                    name: "pc",
                    generation: 3,
                },
                haptic: haptic_route,
            },
            cancel_pc: Some(pc_route),
            cancel_haptic: None,
        };
        apply_s3_route_barrier(
            &applied,
            &mut scheduler,
            &mut actions,
            &object_routes,
            &mut tracker,
        )
        .unwrap();

        let mut releases = Vec::new();
        let mut stale = Vec::new();
        for action in &actions {
            match action {
                PlayoutAction::Release(object) => {
                    releases.push((object.header.track_id, object.header.pts_us));
                }
                PlayoutAction::Drop {
                    object,
                    reason: DROP_STALE_TIER,
                    ..
                } => stale.push((object.header.track_id, object.header.pts_us)),
                PlayoutAction::Drop { .. } => {}
            }
        }
        releases.sort_unstable();
        assert_eq!(
            releases,
            vec![(TRACK_PC, 30_000), (TRACK_HAPTIC, 60_000)],
            "at-barrier PC and unchanged-route haptic stay releases"
        );
        assert_eq!(stale, vec![(TRACK_PC, 60_000)]);
    }

    /// Confirmed-leak regression (run p4g5cdyn_..._s3, route_residue=1): the
    /// first copy of one header identity arrives on the current route and is
    /// terminally evicted by the switch barrier; the SAME identity then
    /// arrives on the NEW current route. Before the guard, that second copy
    /// registered a tracker arrival and an `object_routes` entry while
    /// `scheduler.push` silently ignored it (terminal-set dedup), so no
    /// terminal action ever removed the entry and shutdown failed with route
    /// residue. It must instead become exactly one `duplicate_identity`
    /// terminal drop with no registration at all.
    #[test]
    fn duplicate_identity_after_terminal_first_copy_is_dropped_without_residue() {
        let mut h = Harness::new();
        let gen0 = h.ingress.gate().active_routes();

        // Exact startup pair on generation 0: epoch at now=2_000, so
        // due(pts) = 52_000 + pts.
        h.push(routed(TrackRole::Pc, gen0.pc, 0, 1, 1_000), 1_000);
        h.push(routed(TrackRole::Haptic, gen0.haptic, 0, 1, 2_000), 2_000);
        assert!(h.scheduler.is_started());
        h.tracker.activate().unwrap();

        // First copy on the soon-cancelled current haptic route, above the
        // coming barrier: enters the scheduler buffer.
        h.push(
            routed(TrackRole::Haptic, gen0.haptic, 30_000, 4, 3_000),
            3_000,
        );
        assert_eq!(h.pushed_to_scheduler, 3);

        // Normal -> HapticCritical (both routes change); the exact pair at
        // pts 10_000 applies the switch and the barrier terminally evicts the
        // buffered generation-0 haptic pts 30_000 as `stale_tier`.
        let request = h
            .ingress
            .request(
                transition(S3State::Normal, S3State::HapticCritical, 4_000),
                4_000,
            )
            .unwrap();
        h.ingress.subscribe_ok(TrackRole::Pc, 4_100).unwrap();
        h.ingress.subscribe_ok(TrackRole::Haptic, 4_200).unwrap();
        h.push(
            routed(TrackRole::Pc, request.target.pc, 10_000, 2, 4_300),
            4_300,
        );
        h.push(
            routed(TrackRole::Haptic, request.target.haptic, 10_000, 2, 4_400),
            4_400,
        );
        assert_eq!(
            h.dropped
                .iter()
                .filter(|(key, reason)| *reason == DROP_STALE_TIER && key.pts_us == 30_000)
                .count(),
            1,
            "first copy is terminal via the barrier eviction"
        );
        let current = h.ingress.gate().active_routes();
        assert_ne!(current.haptic, gen0.haptic, "haptic route switched");

        // The SAME header identity arrives moments later on the new current
        // route (CurrentReleaseEligible at the ingress).
        let before = h.scheduler.buffered_counts();
        let actions = h.push(
            routed(TrackRole::Haptic, current.haptic, 30_000, 4, 5_000),
            5_000,
        );
        assert!(actions.is_empty(), "duplicate stages no scheduler action");
        assert_eq!(h.scheduler.buffered_counts(), before, "scheduler untouched");
        assert_eq!(h.duplicate_dropped.len(), 1, "exactly one duplicate drop");
        let (dup_key, dup_reason) = h.duplicate_dropped[0];
        assert_eq!(dup_reason, DROP_DUPLICATE_IDENTITY);
        assert_eq!(
            (dup_key.track_id, dup_key.pts_us, dup_key.event_id),
            (TRACK_HAPTIC, 30_000, 4)
        );

        // The tracker holds no observation for the duplicate: its arrival was
        // never re-registered after `forget_evicted`, so only pair pts 0
        // remains pending and it produces nothing without releases.
        let observations = h.tracker.advance(&h.scheduler, 130_000).unwrap();
        assert!(
            observations.is_empty(),
            "no fabricated observation from the duplicate: {observations:?}"
        );

        // Drain everything and check conservation: every scheduler-entered
        // object yields exactly one terminal record and no route residue.
        let actions = h.scheduler.advance(130_000);
        h.dispatch(&actions, 130_000);
        assert!(h.object_routes.is_empty(), "no route residue at shutdown");
        assert_eq!(
            h.released.len() + h.dropped.len(),
            h.pushed_to_scheduler,
            "terminal records == pushed objects"
        );
        assert_eq!(
            h.dropped
                .iter()
                .filter(|(key, _)| key.pts_us == 30_000)
                .count(),
            1,
            "the identity has exactly one scheduler-side terminal record"
        );
    }

    /// Buffered variant of the same leak: the first copy is still buffered on
    /// the cancelled route (pts at/below the barrier survives on its frozen
    /// deadline) when the duplicate arrives on the new current route. The
    /// duplicate gets one `duplicate_identity` drop and the original still
    /// releases exactly once at its frozen deadline.
    #[test]
    fn duplicate_identity_while_original_buffered_drops_duplicate_and_releases_original_once() {
        let mut h = Harness::new();
        let gen0 = h.ingress.gate().active_routes();

        h.push(routed(TrackRole::Pc, gen0.pc, 0, 1, 1_000), 1_000);
        h.push(routed(TrackRole::Haptic, gen0.haptic, 0, 1, 2_000), 2_000);
        assert!(h.scheduler.is_started());
        h.tracker.activate().unwrap();

        // First copy below the coming barrier: survives the switch buffered.
        h.push(
            routed(TrackRole::Haptic, gen0.haptic, 5_000, 2, 3_000),
            3_000,
        );

        let request = h
            .ingress
            .request(
                transition(S3State::Normal, S3State::HapticCritical, 4_000),
                4_000,
            )
            .unwrap();
        h.ingress.subscribe_ok(TrackRole::Pc, 4_100).unwrap();
        h.ingress.subscribe_ok(TrackRole::Haptic, 4_200).unwrap();
        h.push(
            routed(TrackRole::Pc, request.target.pc, 10_000, 3, 4_300),
            4_300,
        );
        h.push(
            routed(TrackRole::Haptic, request.target.haptic, 10_000, 3, 4_400),
            4_400,
        );
        assert!(
            h.dropped.is_empty(),
            "pts 5_000 <= barrier 10_000 stays buffered on its frozen deadline"
        );
        let current = h.ingress.gate().active_routes();
        assert_ne!(current.haptic, gen0.haptic);

        // Duplicate identity on the new current route while the original is
        // still buffered: one duplicate drop, nothing else changes.
        let before = h.scheduler.buffered_counts();
        let actions = h.push(
            routed(TrackRole::Haptic, current.haptic, 5_000, 2, 4_500),
            4_500,
        );
        assert!(actions.is_empty());
        assert_eq!(h.scheduler.buffered_counts(), before);
        assert_eq!(h.duplicate_dropped.len(), 1);
        assert_eq!(h.duplicate_dropped[0].1, DROP_DUPLICATE_IDENTITY);
        assert_eq!(h.duplicate_dropped[0].0.pts_us, 5_000);

        // The original still releases exactly once, on time at its frozen
        // deadline due(5_000) = 57_000 (pts 0 is within the late tolerance).
        let actions = h.scheduler.advance(57_000);
        h.dispatch(&actions, 57_000);
        assert_eq!(
            h.released
                .iter()
                .filter(|key| key.track_id == TRACK_HAPTIC && key.pts_us == 5_000)
                .count(),
            1
        );
        assert!(h.dropped.is_empty(), "no scheduler-side drop in this run");
        assert!(h.object_routes.is_empty());
        assert_eq!(h.released.len() + h.dropped.len(), h.pushed_to_scheduler);
    }

    /// Non-duplicate control: DISTINCT identities delivered across the two
    /// routes flow unchanged — no `duplicate_identity` drop, every object
    /// releases, and conservation holds.
    #[test]
    fn distinct_identities_on_two_routes_flow_unchanged() {
        let mut h = Harness::new();
        let gen0 = h.ingress.gate().active_routes();

        h.push(routed(TrackRole::Pc, gen0.pc, 0, 1, 1_000), 1_000);
        h.push(routed(TrackRole::Haptic, gen0.haptic, 0, 1, 2_000), 2_000);
        assert!(h.scheduler.is_started());
        h.tracker.activate().unwrap();

        // Old-route identity below the barrier survives the switch.
        h.push(
            routed(TrackRole::Haptic, gen0.haptic, 5_000, 2, 3_000),
            3_000,
        );

        let request = h
            .ingress
            .request(
                transition(S3State::Normal, S3State::HapticCritical, 4_000),
                4_000,
            )
            .unwrap();
        h.ingress.subscribe_ok(TrackRole::Pc, 4_100).unwrap();
        h.ingress.subscribe_ok(TrackRole::Haptic, 4_200).unwrap();
        h.push(
            routed(TrackRole::Pc, request.target.pc, 10_000, 3, 4_300),
            4_300,
        );
        h.push(
            routed(TrackRole::Haptic, request.target.haptic, 10_000, 3, 4_400),
            4_400,
        );
        let current = h.ingress.gate().active_routes();

        // A DISTINCT identity on the new current route.
        h.push(
            routed(TrackRole::Haptic, current.haptic, 30_000, 4, 4_500),
            4_500,
        );
        assert!(
            h.duplicate_dropped.is_empty(),
            "no duplicate drop for distinct identities"
        );
        assert_eq!(h.pushed_to_scheduler, 4);

        // Drain at due(30_000) = 82_000: pts 0 and pts 5_000 are within the
        // late tolerance, so all four objects release.
        let actions = h.scheduler.advance(82_000);
        h.dispatch(&actions, 82_000);
        assert_eq!(h.released.len(), 4);
        assert!(h.dropped.is_empty());
        assert!(h.duplicate_dropped.is_empty());
        assert!(h.object_routes.is_empty());
        assert_eq!(h.released.len() + h.dropped.len(), h.pushed_to_scheduler);
    }

    // ---- S3 FSM 구현계약 §2.1 (2026-08-10 개정) 회귀검사 ----
    //
    // Before this amendment the deadline tracker registered every arriving
    // anchor but only released entries via post-epoch haptic-keyed expiry or
    // route-barrier eviction. Objects the scheduler terminally dropped stayed
    // behind, which produced two observed failures: the receiver died with
    // "anchor bound exceeded" on a run that never formed an epoch, and every
    // S3 run that did form one opened with a forced
    // `Normal -> Haptic-Critical` transition built from pre-epoch anchors that
    // never had a valid deadline.

    /// 회귀 1 — 증상 1. A run whose epoch never forms must not grow the
    /// tracker without bound. The pre-amendment code stored one entry per
    /// arriving anchor for the whole run and aborted at `max_anchors + 1`.
    #[test]
    fn pre_epoch_anchors_do_not_accumulate_when_the_epoch_never_forms() {
        let mut h = Harness::new();
        let gen0 = h.ingress.gate().active_routes();
        let max = h.tracker.max_anchors();

        // Haptic only, so no exact pair can ever start the epoch. Far more
        // anchors than the bound, spread past both startup windows so the
        // 2s timeout and its single re-arm both fire.
        let mut now = 1_000;
        for event_id in 1..=(max as u32 * 4) {
            let pts_us = u64::from(event_id - 1) * 33_333;
            h.push(
                routed(TrackRole::Haptic, gen0.haptic, pts_us, event_id, now),
                now,
            );
            now += 33_333;
        }

        assert!(!h.scheduler.is_started(), "epoch must never form");
        h.tracker
            .check_bounds()
            .expect("pre-epoch cleanup must keep the tracker bounded");
        let (pc_arrivals, haptic_arrivals, ..) = h.tracker.occupancy();
        assert_eq!(pc_arrivals, 0);
        assert!(
            haptic_arrivals <= max,
            "haptic arrivals {haptic_arrivals} exceeded bound {max}"
        );
    }

    /// 회귀 2 — 증상 2, 계약 §2.1 규칙 B. Anchors terminally dropped before the
    /// epoch existed must not produce controller observations once it does.
    /// This is the forced first transition: the pre-amendment tracker replayed
    /// them all as `deadline_miss` at the instant of activation.
    #[test]
    fn pre_epoch_startup_timeout_drops_produce_no_deadline_miss_after_the_epoch() {
        let mut h = Harness::new();
        let gen0 = h.ingress.gate().active_routes();

        // Haptic-only anchors, then let both startup windows expire so they
        // are terminally dropped as `startup_timeout`.
        for event_id in 1..=3u32 {
            let pts_us = u64::from(event_id - 1) * 33_333;
            h.push(
                routed(TrackRole::Haptic, gen0.haptic, pts_us, event_id, 1_000),
                1_000,
            );
        }
        let actions = h.push(
            routed(TrackRole::Haptic, gen0.haptic, 100_000, 4, 2_100_000),
            2_100_000,
        );
        assert!(
            actions.iter().any(|action| matches!(
                action,
                PlayoutAction::Drop {
                    reason: DROP_STARTUP_TIMEOUT,
                    ..
                }
            )),
            "the first startup window must expire"
        );
        assert!(!h.scheduler.is_started());
        // `push` buffers the new object before running `advance`, so the
        // expiring window terminally drops it too and nothing is left
        // registered.
        let (_, haptic_arrivals, ..) = h.tracker.occupancy();
        assert_eq!(haptic_arrivals, 0, "the expired window drained the tracker");

        // A real exact pair now forms the epoch, and the tracker activates
        // exactly as the receiver does.
        h.push(
            routed(TrackRole::Pc, gen0.pc, 200_000, 7, 2_200_000),
            2_200_000,
        );
        h.push(
            routed(TrackRole::Haptic, gen0.haptic, 200_000, 7, 2_200_100),
            2_200_100,
        );
        assert!(h.scheduler.is_started(), "exact pair must start the epoch");
        h.tracker.activate().unwrap();

        // Every dropped pre-epoch anchor has a deadline in the past now, so
        // the pre-amendment tracker emitted a `deadline_miss` for each.
        let observations = h.advance_tracker(3_000_000);
        assert!(
            observations
                .iter()
                .all(|observation| !observation.deadline_miss),
            "pre-epoch losses must not reach the controller: {observations:?}"
        );
    }

    /// 회귀 3 — 계약 §2.1 규칙 C. `push` runs `enforce_bounds` before
    /// `try_start`, so a buffer-limit drop and epoch formation can share one
    /// call. Classifying by drop reason, or by the scheduler state observed
    /// after the batch, would leave that object registered.
    #[test]
    fn buffer_limit_drop_in_the_same_push_that_forms_the_epoch_is_forgotten() {
        let mut config = test_playout();
        // Small enough that the span rule fires while the epoch is still
        // absent, using anchors the tracker actually keys on.
        config.max_span_us = 100_000;
        let mut h = Harness::new();
        h.scheduler = PlayoutScheduler::new(config).unwrap();
        let gen0 = h.ingress.gate().active_routes();

        // Old haptic anchor, then a PC/haptic exact pair far enough ahead that
        // inserting it violates the span bound and evicts the old anchor in
        // the same `push` that starts the epoch.
        h.push(routed(TrackRole::Haptic, gen0.haptic, 0, 1, 1_000), 1_000);
        h.push(routed(TrackRole::Pc, gen0.pc, 900_000, 28, 1_100), 1_100);
        let actions = h.push(
            routed(TrackRole::Haptic, gen0.haptic, 900_000, 28, 1_200),
            1_200,
        );

        assert!(h.scheduler.is_started(), "the exact pair starts the epoch");
        let evicted: Vec<_> = actions
            .iter()
            .filter(|action| {
                matches!(
                    action,
                    PlayoutAction::Drop {
                        reason: DROP_BUFFER_SPAN_LIMIT,
                        ..
                    }
                )
            })
            .collect();
        assert_eq!(
            evicted.len(),
            1,
            "the old anchor is span-evicted: {actions:?}"
        );
        assert!(
            evicted[0].is_pre_epoch_drop(),
            "the drop was emitted before try_start, so it is pre-epoch"
        );

        h.tracker.activate().unwrap();
        let observations = h.advance_tracker(3_000_000);
        assert!(
            observations
                .iter()
                .all(|observation| !observation.deadline_miss),
            "the span-evicted pre-epoch anchor must not become a miss: {observations:?}"
        );
    }

    /// 회귀 4 — P2. The bound is checked once per iteration, after this
    /// batch's terminal cleanup and `advance`. Checking on insert aborted at
    /// the boundary before the scheduler's own eviction could run.
    #[test]
    fn arrival_at_the_buffer_bound_is_evicted_before_the_bound_check() {
        let mut config = test_playout();
        config.max_objects_per_track = 4;
        let mut h = Harness::new();
        h.scheduler = PlayoutScheduler::new(config).unwrap();
        h.tracker = S3DeadlineTracker::new(4).unwrap();
        let gen0 = h.ingress.gate().active_routes();

        // Fill the haptic buffer to its bound and then keep going. Each new
        // arrival evicts the oldest, so settled occupancy never exceeds it.
        for event_id in 1..=12u32 {
            let pts_us = u64::from(event_id - 1) * 33_333;
            h.push(
                routed(TrackRole::Haptic, gen0.haptic, pts_us, event_id, 1_000),
                1_000,
            );
            h.tracker
                .check_bounds()
                .expect("settled occupancy must stay within the bound");
        }
        let (_, haptic_arrivals, ..) = h.tracker.occupancy();
        assert!(haptic_arrivals <= 4, "haptic arrivals {haptic_arrivals}");
    }

    /// 회귀 5 — 계약 §2.1 규칙 E, the counterexample that killed the withdrawn
    /// PC-only expiry proposal. A PC released at its deadline whose haptic
    /// counterpart lands within `late_tolerance` still yields a real pair
    /// observation, NOT a deadline miss. Any future PC-only expiry rule must
    /// keep this test passing.
    #[test]
    fn pc_first_with_a_late_but_tolerated_haptic_still_yields_a_pair_observation() {
        let mut h = Harness::new();
        let gen0 = h.ingress.gate().active_routes();

        // Epoch from an exact pair at pts 0: due(pts) = 52_000 + pts.
        h.push(routed(TrackRole::Pc, gen0.pc, 0, 1, 1_000), 1_000);
        h.push(routed(TrackRole::Haptic, gen0.haptic, 0, 1, 2_000), 2_000);
        assert!(h.scheduler.is_started());
        h.tracker.activate().unwrap();
        let actions = h.scheduler.advance(52_000);
        h.dispatch(&actions, 52_000);
        h.advance_tracker(52_000);

        // Next anchor: PC arrives and is released at its deadline while the
        // haptic counterpart has not arrived at all.
        let pc_deadline = 52_000 + 100_000;
        h.push(routed(TrackRole::Pc, gen0.pc, 100_000, 4, 60_000), 60_000);
        let actions = h.scheduler.advance(pc_deadline);
        h.dispatch(&actions, pc_deadline);
        assert!(
            actions
                .iter()
                .any(|action| matches!(action, PlayoutAction::Release(object)
                    if object.header.pts_us == 100_000)),
            "PC is released at its own deadline: {actions:?}"
        );
        let observations = h.advance_tracker(pc_deadline);
        assert!(
            observations.is_empty(),
            "a PC-only anchor produces nothing yet: {observations:?}"
        );

        // The haptic counterpart lands 5ms late — inside the 50ms tolerance of
        // `test_playout`, so the scheduler still releases it.
        let haptic_arrival = pc_deadline + 5_000;
        let actions = h.push(
            routed(TrackRole::Haptic, gen0.haptic, 100_000, 4, haptic_arrival),
            haptic_arrival,
        );
        assert!(
            actions
                .iter()
                .any(|action| matches!(action, PlayoutAction::Release(object)
                    if object.header.pts_us == 100_000
                        && object.header.track_id == TRACK_HAPTIC)),
            "a tolerated late haptic is released, not dropped: {actions:?}"
        );

        let observations = h.advance_tracker(haptic_arrival);
        let paired: Vec<_> = observations
            .iter()
            .filter(|observation| observation.abs_skew_us.is_some())
            .collect();
        assert_eq!(
            paired.len(),
            1,
            "the pair must be observed: {observations:?}"
        );
        assert!(!paired[0].deadline_miss, "PC met its deadline");
        assert_eq!(paired[0].abs_skew_us, Some(5_000));
    }

    /// 회귀 6 — 계약 §2.1 규칙 D. Post-epoch drops keep their existing
    /// observation behaviour; only pre-epoch losses are withheld. A PC dropped
    /// as `late` after the epoch must still register as a deadline miss for
    /// its anchor.
    #[test]
    fn post_epoch_late_drop_still_reports_a_deadline_miss() {
        let mut h = Harness::new();
        let gen0 = h.ingress.gate().active_routes();

        h.push(routed(TrackRole::Pc, gen0.pc, 0, 1, 1_000), 1_000);
        h.push(routed(TrackRole::Haptic, gen0.haptic, 0, 1, 2_000), 2_000);
        assert!(h.scheduler.is_started());
        h.tracker.activate().unwrap();
        // Drain the epoch-forming pair at its own deadline so it cannot be
        // swept up by the late sweep below.
        let actions = h.scheduler.advance(52_000);
        h.dispatch(&actions, 52_000);
        h.advance_tracker(52_000);

        // PC for the next anchor arrives long after its deadline plus the
        // tolerance, so the scheduler drops it as `late` post-epoch.
        let arrival = 52_000 + 100_000 + 500_000;
        let actions = h.push(routed(TrackRole::Pc, gen0.pc, 100_000, 4, arrival), arrival);
        let late: Vec<_> = actions
            .iter()
            .filter(|action| {
                matches!(
                    action,
                    PlayoutAction::Drop {
                        reason: DROP_LATE,
                        ..
                    }
                )
            })
            .collect();
        assert_eq!(late.len(), 1, "PC is dropped as late: {actions:?}");
        assert!(
            !late[0].is_pre_epoch_drop(),
            "a post-epoch drop must not be treated as pre-epoch"
        );

        h.push(
            routed(TrackRole::Haptic, gen0.haptic, 100_000, 4, arrival + 1_000),
            arrival + 1_000,
        );
        let observations = h.advance_tracker(arrival + 1_000);
        assert!(
            observations
                .iter()
                .any(|observation| observation.deadline_miss),
            "the anchor's PC missed its deadline and must still be reported: {observations:?}"
        );
    }

    /// 회귀 9 — 계약 §2.1 규칙 B, the path P1 alone does NOT cover.
    ///
    /// Measured in the 20260810f netns pilot: an anchor can arrive before any
    /// epoch exists and simply sit in the scheduler buffer — never terminally
    /// dropped, so `had_epoch` tagging never sees it. When the epoch finally
    /// forms from a LATER exact pair, every such anchor is instantly overdue
    /// (`due_us` is in the past) and, with no PC, becomes a `deadline_miss`.
    /// Three of them trip `miss_streak_threshold` at the exact microsecond of
    /// activation — the forced `Normal -> Haptic-Critical` transition.
    ///
    /// The epoch-forming pair itself (pts == epoch pts) must survive.
    #[test]
    fn haptic_anchors_older_than_the_epoch_produce_no_deadline_miss() {
        let mut h = Harness::new();
        let gen0 = h.ingress.gate().active_routes();

        // Haptic-only anchors that stay BUFFERED (well inside the 2s startup
        // window, so no startup timeout drops them).
        for event_id in 1..=3u32 {
            let pts_us = u64::from(event_id - 1) * 33_333;
            h.push(
                routed(TrackRole::Haptic, gen0.haptic, pts_us, event_id, 1_000),
                1_000,
            );
        }
        assert!(!h.scheduler.is_started(), "no exact pair yet");
        assert!(
            h.dropped.is_empty(),
            "these anchors are alive in the buffer, not terminal: {:?}",
            h.dropped
        );

        // A later exact pair forms the epoch at pts 100_000.
        h.push(
            routed(TrackRole::Pc, gen0.pc, 100_000, 4, 1_500_000),
            1_500_000,
        );
        h.push(
            routed(TrackRole::Haptic, gen0.haptic, 100_000, 4, 1_500_100),
            1_500_100,
        );
        assert!(h.scheduler.is_started());
        h.tracker.activate().unwrap();

        // Release the epoch-forming pair at its own deadline
        // (due = 1_500_100 + 50_000 + 0).
        let actions = h.scheduler.advance(1_550_100);
        h.dispatch(&actions, 1_550_100);

        // Every pts < 100_000 anchor is instantly overdue with no PC, and the
        // epoch-forming anchor is due with both sides released.
        let observations = h.advance_tracker(1_550_100);
        assert!(
            observations
                .iter()
                .all(|observation| !observation.deadline_miss),
            "anchors older than the epoch must not reach the controller: {observations:?}"
        );
        // ...while the epoch-forming pair still yields its real observation:
        // the rule must evict the pre-epoch stretch, not silence the tracker.
        assert_eq!(
            observations.len(),
            1,
            "the epoch-forming pair must still be observed: {observations:?}"
        );
        assert!(observations[0].abs_skew_us.is_some(), "{observations:?}");
    }

    /// 회귀 8 — the two failure stages of `advance_tracker_checked` keep
    /// distinct provenance. Only a bound violation may be filed as
    /// `s3_tracker_bound`; an `advance` invariant break is a different failure
    /// and merging them would misattribute the cause in the run's own log.
    #[test]
    fn tracker_step_errors_keep_advance_and_bound_provenance_apart() {
        let mut h = Harness::new();
        h.tracker = S3DeadlineTracker::new(1).unwrap();
        let gen0 = h.ingress.gate().active_routes();

        // Two anchors past a bound of 1, with an epoch so `advance` succeeds
        // and only the bound check can fail.
        h.push(routed(TrackRole::Pc, gen0.pc, 0, 1, 1_000), 1_000);
        h.push(routed(TrackRole::Haptic, gen0.haptic, 0, 1, 2_000), 2_000);
        assert!(h.scheduler.is_started());
        h.tracker.activate().unwrap();
        h.push(routed(TrackRole::Pc, gen0.pc, 100_000, 4, 3_000), 3_000);
        h.push(routed(TrackRole::Pc, gen0.pc, 133_333, 5, 3_100), 3_100);

        match advance_tracker_checked(&mut h.tracker, &h.scheduler, 3_100) {
            Err(TrackerStepError::Bound(message)) => {
                assert!(message.contains("bound exceeded"), "{message}");
            }
            other => panic!("expected a Bound error, got {other:?}"),
        }

        // An inactive tracker cannot fail either stage, so a clean run must not
        // manufacture a bound error.
        let mut fresh = Harness::new();
        fresh.push(routed(TrackRole::Pc, gen0.pc, 0, 1, 1_000), 1_000);
        assert!(
            advance_tracker_checked(&mut fresh.tracker, &fresh.scheduler, 1_000)
                .unwrap()
                .is_empty()
        );
    }

    /// 회귀 7 — 계약 §2.1 규칙 A, the amendment's safety claim. Withholding
    /// pre-epoch losses from the controller must not remove them from
    /// delivery/terminal accounting: every object that entered the scheduler
    /// still yields exactly one terminal record.
    #[test]
    fn pre_epoch_losses_remain_in_terminal_accounting() {
        let mut h = Harness::new();
        let gen0 = h.ingress.gate().active_routes();

        for event_id in 1..=5u32 {
            let pts_us = u64::from(event_id - 1) * 33_333;
            h.push(
                routed(TrackRole::Haptic, gen0.haptic, pts_us, event_id, 1_000),
                1_000,
            );
        }
        h.push(
            routed(TrackRole::Haptic, gen0.haptic, 200_000, 7, 2_100_000),
            2_100_000,
        );

        let startup_dropped = h
            .dropped
            .iter()
            .filter(|(_, reason)| *reason == DROP_STARTUP_TIMEOUT)
            .count();
        assert_eq!(
            startup_dropped, 6,
            "every pre-epoch loss stays in terminal accounting: {:?}",
            h.dropped
        );
        assert_eq!(
            h.released.len() + h.dropped.len(),
            h.pushed_to_scheduler,
            "exactly one terminal record per scheduler-entered object"
        );
        // ...while the controller side is empty.
        let (_, haptic_arrivals, ..) = h.tracker.occupancy();
        assert_eq!(haptic_arrivals, 0, "no pre-epoch loss remains registered");
    }
}

// ---- S3 switch-barrier retirement tests ------------------------------------
//
// v11 defect: at apply, the receiver immediately dropped the old-route
// subscription handle. The relay hard-stopped on the UNSUBSCRIBE, so an
// in-flight pre-/at-barrier frame neither arrived nor produced a relay
// delivery_timeout event — exactly one unaccounted frame per affected run.
// The fix keeps the old-route wire path open for one registered PC
// delivery-timeout window (S3RetirementQueue), during which such a frame
// either arrives (rx row + terminal stale drop) or hits the relay timeout.
// These tests drive the same functions the `run_s3_receiver` loop uses.

#[cfg(test)]
mod s3_retirement_tests {
    use super::*;
    use skew_moq::s3_controller::{S3State, S3Transition, TransitionCause};

    /// Frozen S3 PC delivery timeout (67ms), the registered bound the
    /// retirement window reuses.
    const BUDGET_US: u64 = 67_000;

    fn transition(from: S3State, to: S3State, at_us: u64) -> S3Transition {
        S3Transition {
            at_us,
            from,
            to,
            cause: TransitionCause::DeadlineMissStreak,
        }
    }

    #[test]
    fn cancelled_route_is_retired_bounded_not_unsubscribed_immediately() {
        let mut live: HashMap<(TrackRole, u64), u32> = HashMap::new();
        let mut retiring: S3RetirementQueue<u32> = S3RetirementQueue::new(BUDGET_US).unwrap();
        let old = Route {
            name: "pc",
            generation: 0,
        };
        live.insert((TrackRole::Pc, 0), 7);

        retire_cancelled_s3_route(&mut live, &mut retiring, TrackRole::Pc, old, 1_000).unwrap();
        assert!(live.is_empty());
        assert!(retiring.contains(TrackRole::Pc, old));
        assert_eq!(retiring.next_deadline_us(), Some(1_000 + BUDGET_US));
        // The subscription is NOT released before the registered window ends…
        assert!(retiring.take_due(1_000 + BUDGET_US - 1).is_empty());
        // …and is released exactly once at the deadline.
        let due = retiring.take_due(1_000 + BUDGET_US);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].0, TrackRole::Pc);
        assert_eq!(due[0].1, old);
        assert_eq!(due[0].2, 7);
        assert!(retiring.is_empty());

        // Lifecycle defects fail loud instead of leaking or double-freeing.
        assert!(matches!(
            retire_cancelled_s3_route(&mut live, &mut retiring, TrackRole::Pc, old, 70_000),
            Err(RetireError::Missing)
        ));
        let next = Route {
            name: "pc",
            generation: 1,
        };
        live.insert((TrackRole::Pc, 1), 8);
        retire_cancelled_s3_route(&mut live, &mut retiring, TrackRole::Pc, next, 70_100).unwrap();
        live.insert((TrackRole::Pc, 1), 9);
        assert!(matches!(
            retire_cancelled_s3_route(&mut live, &mut retiring, TrackRole::Pc, next, 70_200),
            Err(RetireError::Duplicate(9))
        ));
    }

    fn wire_object(tier: u16, seq: u32, pts_us: u64, event_id: u32) -> Bytes {
        let payload = [0u8; 4];
        let header = pack_header(
            TRACK_PC,
            tier,
            seq,
            pts_us,
            event_id,
            1,
            payload.len() as u32,
        );
        let mut bytes = Vec::with_capacity(HDR + payload.len());
        bytes.extend_from_slice(&header);
        bytes.extend_from_slice(&payload);
        Bytes::from(bytes)
    }

    fn barrier_object(role: TrackRole, route: Route, pts_us: u64, event_id: u32) -> RoutedObject {
        let (track_id, tier) = match role {
            TrackRole::Pc => (
                TRACK_PC,
                match route.name {
                    "pc" => 2,
                    "pc-d7" => 3,
                    "pc-d6" => 4,
                    _ => 99,
                },
            ),
            TrackRole::Haptic => (TRACK_HAPTIC, 0),
        };
        RoutedObject {
            role,
            route,
            object: PlayoutObject {
                header: Header {
                    version: VERSION,
                    track_id,
                    tier,
                    seq: event_id,
                    pts_us,
                    event_id,
                    gen_ts_us: 1,
                    payload_len: 0,
                },
                t_recv: 1,
                bytes: Bytes::new(),
            },
        }
    }

    fn test_log_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "skew-{name}-{}-{}.jsonl",
            std::process::id(),
            now_us()
        ))
    }

    /// Regression for the v11 single-frame loss: after apply, the old route's
    /// wire path stays open, so an in-flight at-barrier frame is DELIVERED
    /// (rx-logged by the still-running drain) and terminally stale-dropped by
    /// the ingress — zero unaccounted. A post-barrier old-route frame is
    /// equally barrier-dropped and never released. Track end completes the
    /// retirement before the deadline.
    #[tokio::test]
    async fn in_flight_pre_barrier_frame_delivers_and_accounts_during_retirement() {
        // Receiver-side atomic apply with barrier (pts 3_500_000, event 106),
        // mirroring the v11 evidence run D50/T50.
        let gate = S3SwitchGate::new(SwitchConfig {
            effect_timeout_us: 10_000_000,
        })
        .unwrap();
        let mut ingress = S3ReceiverIngress::new(gate, 64).unwrap();
        let old = ingress.gate().active_routes();
        let request = ingress
            .request(
                transition(S3State::Normal, S3State::HapticCritical, 1_000),
                1_000,
            )
            .unwrap();
        ingress.subscribe_ok(TrackRole::Pc, 1_100).unwrap();
        ingress.subscribe_ok(TrackRole::Haptic, 1_200).unwrap();
        assert!(ingress
            .push(
                barrier_object(TrackRole::Pc, request.target.pc, 3_500_000, 106),
                2_000
            )
            .unwrap()
            .is_empty());
        let events = ingress
            .push(
                barrier_object(TrackRole::Haptic, request.target.haptic, 3_500_000, 106),
                2_100,
            )
            .unwrap();
        let applied = events
            .iter()
            .find_map(|event| match event {
                IngressEvent::Applied(applied) => Some(*applied),
                _ => None,
            })
            .expect("exact-pair apply");
        let cancel_pc = applied.cancel_pc.expect("old PC route cancelled");
        assert_eq!(cancel_pc, old.pc);

        // The fix: the cancelled route is retired (wire subscription kept),
        // not unsubscribed at apply.
        let mut live: HashMap<(TrackRole, u64), &'static str> = HashMap::new();
        live.insert((TrackRole::Pc, cancel_pc.generation), "old-pc-subscription");
        let mut retiring: S3RetirementQueue<&'static str> =
            S3RetirementQueue::new(BUDGET_US).unwrap();
        retire_cancelled_s3_route(
            &mut live,
            &mut retiring,
            TrackRole::Pc,
            cancel_pc,
            applied.effect_at_us,
        )
        .unwrap();

        // Wire: the old-route drain keeps reading during the window, exactly
        // as in `run_s3_receiver` (the drain task is untouched at apply).
        let out = test_log_path("s3-retire-rx");
        let logger = Arc::new(Mutex::new(
            JsonlLogger::new(
                &out,
                "run",
                "moq",
                "rx",
                None,
                0.0,
                0.0,
                0.0,
                10,
                30,
                90,
                1,
                None,
                None,
                Some("both"),
                Some(TERM_PROTOCOL_V),
                None,
                None,
                Some(V5Meta {
                    payload_mode: PayloadMode::Frame,
                    representation: Representation::Bin,
                    topology: Topology::Relay,
                    chunk_bytes: 178,
                    queue_policy: None,
                }),
            )
            .unwrap(),
        ));
        let (event_tx, mut event_rx) = mpsc::channel::<S3WireEvent>(16);
        let bad_headers = Arc::new(AtomicU64::new(0));
        let ingress_drops = Arc::new(AtomicU64::new(0));
        let log_failed = Arc::new(AtomicU64::new(0));
        let (writer, reader) =
            Track::new(TrackNamespace::from_utf8_path("/retire"), "pc").produce();
        let drain = tokio::spawn(drain_s3_track(
            TrackRole::Pc,
            cancel_pc,
            reader,
            logger.clone(),
            event_tx,
            bad_headers.clone(),
            ingress_drops.clone(),
            log_failed.clone(),
        ));

        // The relay serves the in-flight at-barrier copy plus one
        // post-barrier frame on the still-open old route.
        let mut subgroups = writer.subgroups().unwrap();
        let mut subgroup = subgroups.append(1).unwrap();
        subgroup.write(wire_object(2, 105, 3_500_000, 106)).unwrap();
        subgroup.write(wire_object(2, 106, 3_533_333, 107)).unwrap();

        let mut received = Vec::new();
        for _ in 0..2 {
            let event = tokio::time::timeout(Duration::from_secs(5), event_rx.recv())
                .await
                .expect("in-flight frame must arrive during the drain window")
                .expect("drain alive");
            match event {
                S3WireEvent::Object(routed) => received.push(routed),
                other => panic!("expected object, got end: {other:?}"),
            }
        }
        assert_eq!(received[0].object.header.pts_us, 3_500_000);
        assert_eq!(received[0].object.header.event_id, 106);
        assert_eq!(received[0].route, cancel_pc);

        // Both frames are terminal barrier drops — never scheduler releases.
        for routed in received {
            let events = ingress.push(routed, applied.effect_at_us + 10_000).unwrap();
            assert!(
                matches!(
                    events.as_slice(),
                    [IngressEvent::Drop {
                        reason: DROP_STALE_TIER,
                        ..
                    }]
                ),
                "cancelled-route frame must be a stale terminal drop"
            );
        }

        // Track end (publisher FIN) completes retirement before the deadline.
        drop(subgroup);
        drop(subgroups);
        let ended = tokio::time::timeout(Duration::from_secs(5), event_rx.recv())
            .await
            .expect("track end must be observed")
            .expect("drain alive");
        match ended {
            S3WireEvent::Ended {
                role, route, end, ..
            } => {
                assert_eq!(role, TrackRole::Pc);
                assert_eq!(route, cancel_pc);
                assert_eq!(end, TrackEnd::Fin);
            }
            other => panic!("expected end, got {other:?}"),
        }
        assert_eq!(
            retiring.take_ended(TrackRole::Pc, cancel_pc),
            Some("old-pc-subscription")
        );
        assert!(retiring.is_empty());
        drain.await.unwrap();

        // Delivery accounting: the at-barrier identity has an rx row for its
        // (route, identity) — it is received, not unaccounted.
        assert_eq!(bad_headers.load(Ordering::Relaxed), 0);
        assert_eq!(log_failed.load(Ordering::Relaxed), 0);
        let log = std::fs::read_to_string(&out).unwrap();
        assert!(
            log.lines().any(|line| line.contains("\"role\":\"rx\"")
                && line.contains("\"pts_us\":3500000")
                && line.contains("\"event_id\":106")
                && line.contains("\"wire_track\":\"pc\"")
                && line.contains("\"route_generation\":0")),
            "at-barrier frame must be rx-accounted on the old route: {log}"
        );
        std::fs::remove_file(&out).unwrap();
    }
}
