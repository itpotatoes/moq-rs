// moq_receiver — MoQ naive B1 subscriber (L1: t_play = t_recv).
//
// Subscribes to both tracks (pc, haptic) on namespace == run_id via the relay,
// reads each object, parses the 32B header, and logs an rx record with
// t_recv = t_play (arrival) and t_gen (from the header) for the D metrics.
// Each MoQ object is one complete message, so no byte reassembly is needed.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
use skew_moq::s3_switch::{Route, S3SwitchGate, SwitchApplied, SwitchConfig, SwitchRequest, TrackRole};
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
    /// Plan 단계 9 method 6: the event-pair NON-preserving control. Same
    /// subscriptions, priorities and PC DELIVERY_TIMEOUT as S2, but the
    /// receiver releases each track on its OWN timeline
    /// (`t_release = t_gen + D_play`) instead of on a common timeline anchored
    /// on the first exact PC/haptic anchor pair. No FSM, no tier switching, no
    /// cross-track deadline coupling.
    #[value(name = "s3np")]
    S3np,
    /// Plan 단계 9 / user decision 9-7(b): "S2 + replay of a registered tier
    /// trajectory, event pairs PRESERVED". The receiver is the UNCHANGED S2
    /// receiver — the S1 common-timeline scheduler whose epoch is the first
    /// exact PC/haptic anchor pair, plus the PC DELIVERY_TIMEOUT — and the
    /// sender replays the tier trajectory on the single static PC track.
    ///
    /// It differs from closed-loop `s3` in receiver mechanics as well as in
    /// adaptation: there is NO controller, NO switch gate, and therefore no
    /// barrier objects and no route retirement. `S3 - S3R` consequently measures
    /// adaptation PLUS the cost of multi-route switching, while `S3R - S3NP`
    /// isolates the pair-preserving scheduler at an identical sender stream.
    #[value(name = "s3r")]
    S3r,
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
            Self::S3np => "s3np",
            Self::S3r => "s3r",
        }
    }

    /// Arms that generate from a recorded tier trajectory and therefore carry
    /// the schedule provenance in metadata.
    fn replays_tier_schedule(self) -> bool {
        matches!(self, Self::S3np | Self::S3r)
    }
}

/// CLI mirror of `skew_moq::s3np::ReleaseRule`. A separate type so clap's value
/// names are part of the CLI contract rather than of the library.
#[derive(Clone, Copy, PartialEq, Eq, Debug, clap::ValueEnum)]
enum CliReleaseRule {
    /// Registered PRIMARY rule: each track anchors on its own first observed
    /// object, then releases at `E_k + pts + D_play`.
    #[value(name = "per_track_epoch")]
    PerTrackEpoch,
    /// Sensitivity variant: absolute `t_gen + D_play`, which does not absorb the
    /// one-way delay and is therefore a tighter baseline than S1-S3's.
    #[value(name = "absolute_t_gen")]
    AbsoluteTGen,
}

impl From<CliReleaseRule> for skew_moq::s3np::ReleaseRule {
    fn from(value: CliReleaseRule) -> Self {
        match value {
            CliReleaseRule::PerTrackEpoch => Self::PerTrackEpoch,
            CliReleaseRule::AbsoluteTGen => Self::AbsoluteTGen,
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
    /// `--arm s3np` only: which pairing-free release rule to apply. There is
    /// deliberately NO default — the two rules use different deadline baselines,
    /// and a silent default would make an s3np run's comparability to S3
    /// unrecoverable from the log.
    #[arg(long, value_enum)]
    s3np_release_rule: Option<CliReleaseRule>,
    /// `--arm s3np` only: the same tier-schedule document the sender replays.
    /// The receiver does not use its contents — it applies no tier policy — but
    /// it parses and digests it so BOTH endpoints' metadata name the schedule
    /// that was in force, and so a wiring mistake fails at startup rather than
    /// producing an unattributable run.
    #[arg(long)]
    tier_schedule: Option<PathBuf>,
}

/// `--duration-s` as exact integer microseconds. A fractional microsecond would
/// make the schedule/run window comparison depend on float rounding, so it is
/// refused rather than rounded.
fn duration_us_exact(duration_s: f64) -> Result<u64> {
    if !duration_s.is_finite() || duration_s <= 0.0 {
        bail!("--duration-s must be finite and positive");
    }
    let micros = duration_s * 1_000_000.0;
    if (micros - micros.round()).abs() > 1e-6 {
        bail!("--duration-s {duration_s} is not an exact microsecond count");
    }
    Ok(micros.round() as u64)
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
        Arm::S2 | Arm::S2Eq | Arm::S3 | Arm::S3np | Arm::S3r => {
            let timeout = args.pc_delivery_timeout_ms.with_context(|| {
                format!(
                    "--arm {} requires --pc-delivery-timeout-ms",
                    args.arm.as_str()
                )
            })?;
            if timeout == 0 {
                bail!("--pc-delivery-timeout-ms must be greater than zero");
            }
            if matches!(args.arm, Arm::S3 | Arm::S3np | Arm::S3r) && timeout != 67 {
                bail!(
                    "--arm {} inherits the frozen 67ms PC delivery timeout",
                    args.arm.as_str()
                );
            }
        }
    }
    // s3np owns a DIFFERENT release rule, so it must not build a
    // `PlayoutConfig`: that type carries the exact-pair startup window, which
    // is the one thing the non-preserving control is defined not to use.
    // `s3np_release_config` validates and returns its own parameters.
    //
    // s3r deliberately falls THROUGH to the unchanged S1/S2 configuration: its
    // whole definition is "the S2 receiver, fed a replayed tier trajectory", so
    // the exact-pair epoch, startup window, late policy and buffer bounds are
    // the S2 ones, with no new parameter.
    if args.arm == Arm::S3np {
        return Ok(None);
    }
    if args.arm == Arm::S3r {
        if args.tracks != RxTrackSel::Both {
            bail!("--arm s3r preserves PC/haptic event pairs and requires --tracks both");
        }
        if args.queue_policy != QueuePolicy::Separate {
            bail!("--arm s3r requires --queue-policy separate");
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

/// Validate and build the `s3np` pairing-free release configuration.
///
/// Refused for every other arm, and — for `s3np` — the two *pairing-only*
/// scheduler options are refused rather than accepted and ignored:
/// `--startup-timeout-ms` and `--startup-rearm-limit` bound the search for the
/// first exact PC/haptic anchor pair, which this arm never performs. Accepting
/// them would let a run record a parameter it did not apply.
fn s3np_release_config(args: &Args) -> Result<Option<skew_moq::s3np::ReleaseConfig>> {
    if args.arm != Arm::S3np {
        if args.s3np_release_rule.is_some() {
            // s3r is the likely mistake: it also replays a schedule, but it has
            // exactly ONE release rule (the unchanged S1/S2 common timeline), so
            // there is nothing to select and a selection must not be recorded.
            bail!(
                "--s3np-release-rule requires --arm s3np; --arm s3r always uses the \
                 unchanged S1/S2 common timeline"
            );
        }
        return Ok(None);
    }
    if args.tracks != RxTrackSel::Both {
        bail!("--arm s3np requires --tracks both");
    }
    if args.queue_policy != QueuePolicy::Separate {
        bail!("--arm s3np requires --queue-policy separate");
    }
    if args.startup_timeout_ms.is_some() || args.startup_rearm_limit.is_some() {
        bail!(
            "--startup-timeout-ms/--startup-rearm-limit bound the exact anchor-PAIR \
             startup window, which --arm s3np never performs; omit them"
        );
    }
    let d_play_ms = args
        .d_play_ms
        .context("--arm s3np requires --d-play-ms")?;
    if !matches!(d_play_ms, 50 | 100) {
        bail!("--d-play-ms must be a governing-design candidate: 50 or 100");
    }
    let config = skew_moq::s3np::ReleaseConfig {
        // Fail closed. Every other s3np parameter is explicit for the same
        // reason; the release rule is the one that decides what the arm MEANS.
        rule: args
            .s3np_release_rule
            .context(
                "--arm s3np requires --s3np-release-rule {per_track_epoch|absolute_t_gen}; \
                 there is no default because the two rules are not interchangeable",
            )?
            .into(),
        d_play_us: ms_to_us(d_play_ms, "d-play-ms")?,
        late_tolerance_us: ms_to_us(
            args.late_tolerance_ms
                .context("--arm s3np requires --late-tolerance-ms")?,
            "late-tolerance-ms",
        )?,
        late_policy: args
            .late_policy
            .context("--arm s3np requires --late-policy")?
            .into(),
        max_objects_per_track: args
            .buffer_max_objects_per_track
            .context("--arm s3np requires --buffer-max-objects-per-track")?,
        max_span_us: ms_to_us(
            args.buffer_max_span_ms
                .context("--arm s3np requires --buffer-max-span-ms")?,
            "buffer-max-span-ms",
        )?,
    };
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
        Arm::S2 | Arm::S2Eq | Arm::S3 | Arm::S3np | Arm::S3r => Some(Phase4TransportMeta {
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
    if matches!(args.arm, Arm::S2 | Arm::S2Eq | Arm::S3 | Arm::S3np | Arm::S3r)
        && args.data_priority_mapping != DataPriorityMapping::MoqtV2
    {
        bail!(
            "--arm {} requires --data-priority-mapping moqt-v2 in the v5 generation",
            args.arm.as_str()
        );
    }
    Ok(())
}

/// Released objects whose measured lateness is more negative than this are a
/// clock or scheduler defect, not a fast release: an object cannot legitimately
/// be handed out materially before its own deadline. Counted and reported so the
/// analyzer can reject such a run instead of averaging the impossible values in.
const MAX_NEGATIVE_LATENESS_US: i64 = 1_000;

#[derive(Debug, Default)]
struct PlayoutStats {
    released: u64,
    dropped: u64,
    bridge_observer_dropped: u64,
    /// Releases with `t_release - t_due < -MAX_NEGATIVE_LATENESS_US`.
    negative_lateness: u64,
}

#[allow(clippy::too_many_arguments)]
fn dispatch_playout_actions(
    actions: Vec<PlayoutAction>,
    logger: &Arc<Mutex<JsonlLogger>>,
    bridge: Option<&mpsc::Sender<Bytes>>,
    render: bool,
    audio: bool,
    stats: &mut PlayoutStats,
    // The scheduled release instant of each action, from the scheduler that
    // emitted it (stage-9 decision 9-6 `t_due`).
    due_us: &dyn Fn(&PlayoutAction) -> Option<u64>,
) -> std::io::Result<()> {
    for action in actions {
        let t_due = due_us(&action);
        let object = action.object();
        let h = object.header;
        let action_time = now_us().max(object.t_recv);
        match action {
            PlayoutAction::Release(object) => {
                if let Some(t_due) = t_due {
                    let lateness = action_time as i64 - t_due as i64;
                    if lateness < -MAX_NEGATIVE_LATENESS_US {
                        stats.negative_lateness += 1;
                        debug_assert!(
                            false,
                            "released {}us before its deadline: t_release={action_time} \
                             t_due={t_due} track={} seq={}",
                            -lateness,
                            object.track_name(),
                            h.seq
                        );
                    }
                }
                logger.lock().unwrap().try_log_release(
                    object.track_name(),
                    h.tier,
                    h.seq,
                    h.pts_us,
                    h.event_id,
                    action_time,
                    t_due,
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
                    t_due,
                )?;
                stats.dropped += 1;
            }
        }
    }
    Ok(())
}

/// Record each armed common timeline (stage-9 decision 9-6 evidence). Shared by
/// the static S1/M1/S2/S3R loop and the S3 receiver, which use the same
/// scheduler, so one timeline can never be logged under two different shapes.
fn log_common_epochs(
    scheduler: &mut PlayoutScheduler,
    logger: &Arc<Mutex<JsonlLogger>>,
    config: PlayoutConfig,
) -> Result<()> {
    for epoch in scheduler.take_new_epochs() {
        logger
            .lock()
            .map_err(|_| anyhow::anyhow!("RX logger poisoned"))?
            .try_log_timeline_epoch(
                "common",
                epoch.monotonic_us,
                epoch.anchor_pts_us,
                epoch.anchor_event_id,
                config.d_play_us,
                epoch.rearm_index,
            )
            .context("failed to record the common playout timeline epoch")?;
    }
    Ok(())
}

/// `dispatch_playout_actions` with the common-timeline scheduler supplying each
/// action's `t_due`.
#[allow(clippy::too_many_arguments)]
fn dispatch_common_actions(
    actions: Vec<PlayoutAction>,
    scheduler: &PlayoutScheduler,
    logger: &Arc<Mutex<JsonlLogger>>,
    bridge: &Option<mpsc::Sender<Bytes>>,
    render: bool,
    audio: bool,
    stats: &mut PlayoutStats,
) -> std::io::Result<()> {
    dispatch_playout_actions(
        actions,
        logger,
        bridge.as_ref(),
        render,
        audio,
        stats,
        &|action| scheduler.action_due_us(action),
    )
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
        log_common_epochs(&mut scheduler, &logger, config)?;
        dispatch_common_actions(actions, &scheduler, &logger, &bridge, render, audio, &mut stats)?;
    }

    if !scheduler.is_started() {
        let actions = scheduler.finish_without_epoch();
        dispatch_common_actions(actions, &scheduler, &logger, &bridge, render, audio, &mut stats)?;
    } else {
        // Producer ended: deterministically drain the bounded timeline, then
        // return so logger finalization cannot race a detached scheduler task.
        while !scheduler.is_empty() {
            let Some(wakeup) = scheduler.next_wakeup_us() else {
                break;
            };
            tokio::time::sleep(Duration::from_micros(wakeup.saturating_sub(now_us()))).await;
            let actions = scheduler.advance(now_us());
            dispatch_common_actions(actions, &scheduler, &logger, &bridge, render, audio, &mut stats)?;
        }
    }
    Ok(stats)
}

/// Plan 단계 9 method 6 release loop: the same ingress queue, the same
/// `role:"release"`/`role:"drop"` rows and the same bounded-buffer vocabulary as
/// [`run_playout_scheduler`], with exactly ONE difference — the deadline of an
/// object is `t_gen + D_play` on its own track, so nothing waits for an exact
/// PC/haptic anchor pair and no deadline miss on one track can affect the other.
///
/// There is no startup window to fail, so there is also no
/// `finish_without_epoch` path: on producer end every buffered object already
/// has a deadline and the loop below drains them deterministically.
async fn run_s3np_release_scheduler(
    config: skew_moq::s3np::ReleaseConfig,
    mut input: mpsc::Receiver<PlayoutObject>,
    logger: Arc<Mutex<JsonlLogger>>,
    bridge: Option<mpsc::Sender<Bytes>>,
    render: bool,
    audio: bool,
) -> Result<PlayoutStats> {
    let mut scheduler =
        skew_moq::s3np::PerTrackReleaseScheduler::new(config).map_err(anyhow::Error::msg)?;
    let mut stats = PlayoutStats::default();

    // Each track's anchor is recorded the moment it forms. It cannot live on the
    // `role:"meta"` row (written before any object exists), so it is its own
    // append-only row; without it the applied timeline is not reconstructible
    // from the log.
    fn log_epochs(
        scheduler: &mut skew_moq::s3np::PerTrackReleaseScheduler,
        logger: &Arc<Mutex<JsonlLogger>>,
        config: skew_moq::s3np::ReleaseConfig,
    ) -> Result<()> {
        for epoch in scheduler.take_new_epochs() {
            logger
                .lock()
                .map_err(|_| anyhow::anyhow!("RX logger poisoned"))?
                // Same row shape as the common timeline, scoped per track: the
                // analyzer reads one `timeline_epoch` vocabulary for every arm.
                // `rearm_index` is always 0 — a per-track anchor is the first
                // object observed on that track and is never re-armed.
                .try_log_timeline_epoch(
                    epoch.track_name(),
                    epoch.monotonic_us,
                    epoch.pts_us,
                    epoch.event_id,
                    config.d_play_us,
                    0,
                )
                .context("failed to record an s3np track timeline epoch")?;
        }
        Ok(())
    }

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
        log_epochs(&mut scheduler, &logger, config)?;
        dispatch_playout_actions(
            actions,
            &logger,
            bridge.as_ref(),
            render,
            audio,
            &mut stats,
            &|action| scheduler.action_due_us(action),
        )?;
    }

    while !scheduler.is_empty() {
        let Some(wakeup) = scheduler.next_wakeup_us() else {
            break;
        };
        tokio::time::sleep(Duration::from_micros(wakeup.saturating_sub(now_us()))).await;
        let actions = scheduler.advance(now_us());
        dispatch_playout_actions(
            actions,
            &logger,
            bridge.as_ref(),
            render,
            audio,
            &mut stats,
            &|action| scheduler.action_due_us(action),
        )?;
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
///
/// `Closed(S3_RUN_ENDED_REQUEST_ERROR_CODE)` is NOT retryable and has its
/// own arm below: the end of the sender's run is final, so retrying would only
/// burn the switch's effect budget (2 x `--subscribe-retry-ms`) before
/// reaching the same answer. The arm is written out rather than left to fall
/// through, so the decision is visible at the site that makes it.
fn is_retryable_subscribe_error(e: &moq_transport::serve::ServeError) -> bool {
    use moq_transport::serve::ServeError;
    match e {
        // Final: the sender's run has ended (produced out, or draining).
        ServeError::Closed(code) if *code == S3_RUN_ENDED_REQUEST_ERROR_CODE => false,
        ServeError::NotFound | ServeError::NotFoundWithId(..) => true,
        ServeError::Closed(code) => *code == REQUEST_ERROR_DOES_NOT_EXIST,
        _ => false,
    }
}

/// A SUBSCRIBE that gave up: the wire/local error is kept typed so the caller
/// can classify it by `RequestErrorCode` instead of by its text. `Display` is
/// byte-identical to the message the previous `bail!` produced, so the
/// shutdown `detail` of every still-fatal failure is unchanged.
#[derive(Debug)]
struct S3SubscribeFailure {
    role: TrackRole,
    route: Route,
    retries: u32,
    error: moq_transport::serve::ServeError,
    /// Monotonic instant at which THIS role observed the final error, captured
    /// inside `open_s3_subscription` before any retry sleep. The post-`join!`
    /// instant is a different quantity (it is the settle time of the whole
    /// request) and must not be substituted for it: two roles can be refused
    /// tens of milliseconds apart.
    t_failed_us: u64,
}

impl std::fmt::Display for S3SubscribeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "S3 subscribe {} generation {} failed after {} retries: {}",
            self.route.name, self.route.generation, self.retries, self.error
        )
    }
}

impl std::error::Error for S3SubscribeFailure {}

impl S3SubscribeFailure {
    /// The peer answered REQUEST_ERROR with the registered run-ended code —
    /// the ONE meaning the S3 sender signals that way: its run is over, so
    /// this subscription can never be served. Both sender sites produce it
    /// through `skew_moq::s3_sender::run_ended_refusal`: the
    /// `current_routes_completed()` refusal and the
    /// "namespace is draining after run end" refusal. They are
    /// indistinguishable here, which is correct — the receiver's response is
    /// the same for both.
    ///
    /// It does NOT assert that the sender's run SUCCEEDED; the sender's own
    /// verdict and exit code decide that.
    ///
    /// This is deliberately NOT `DoesNotExist` (0x10): the sender still answers
    /// 0x10 for an unsupported S3 track name and for an invalid route
    /// allocation, and moq-transport's publisher answers 0x10 for a track it
    /// cannot find. All of those are faults. Time cannot discriminate either —
    /// the sender finishes at its last slot, so a real run-ended refusal is
    /// routinely observed BEFORE the receiver's window end. Only the typed
    /// code is sound, and the match is on the code alone, never on the reason
    /// text (`ServeError::Closed` has none).
    fn is_run_ended_refusal(&self) -> bool {
        matches!(
            self.error,
            moq_transport::serve::ServeError::Closed(code)
                if code == S3_RUN_ENDED_REQUEST_ERROR_CODE
        )
    }
}

/// Whether a controller transition may still be requested on wire.
///
/// The compared value is the **request instant**, `request_at_us =
/// max(now, t_decision)` — not the decision instant. A transition decided just
/// before the window end whose request would only be issued at or after it is
/// therefore suppressed as well. That is intentional: what cannot be sent
/// inside the window cannot take effect inside it. The
/// `s3_switch`/`suppressed_after_end` row carries both `t_decision` and
/// `t_request`, so which of the two crossed the boundary stays reconstructible.
///
/// `window_end_us` is the registered measurement window end (`t0 + duration`)
/// if the receiver could derive it. Without a known window end the receiver
/// behaves exactly as before (ungated).
fn switch_allowed_at(request_at_us: u64, window_end_us: Option<u64>) -> bool {
    window_end_us.map_or(true, |end| request_at_us < end)
}

/// One role refused by the sender's run-ended signal:
/// `(role, route, wire error code, that role's observation instant)`.
type RefusedRole = (TrackRole, Route, u64, u64);

/// Why the target subscriptions of a requested switch could not be opened.
#[derive(Debug)]
enum SwitchOpenFailure {
    /// EVERY failed role was refused with the registered run-ended
    /// REQUEST_ERROR code: the sender's run was already over when the
    /// SUBSCRIBE arrived. Carries the refused roles with their wire error code
    /// and per-role observation instant for the additive
    /// `refused_after_run_end` rows.
    RefusedAfterRunEnd(Vec<RefusedRole>),
    /// Anything else: fatal, exactly as before.
    Fatal,
}

/// Classify the settled failures of one switch request.
///
/// A refusal after run end requires ALL failures to be typed subscribe
/// failures carrying `S3_RUN_ENDED_REQUEST_ERROR_CODE`. A timeout, a bare
/// `DoesNotExist` (0x10), a locally raised `NotFound`, an untyped error, or a
/// mix on the two roles all stay fatal.
///
/// Time is NOT part of the rule. Whether the sender's run has ended is the
/// authority and it is signalled by the code; the sender finishes at its last
/// slot, so a genuine run-ended refusal is routinely observed before the
/// receiver's `t0 + duration`. Observation instants are still carried, for the
/// log rows only.
fn classify_switch_open_failure<'a>(
    errors: impl IntoIterator<Item = &'a anyhow::Error>,
) -> SwitchOpenFailure {
    let mut refused = Vec::new();
    for error in errors {
        match error.downcast_ref::<S3SubscribeFailure>() {
            Some(failure) if failure.is_run_ended_refusal() => {
                refused.push((
                    failure.role,
                    failure.route,
                    failure.error.code(),
                    failure.t_failed_us,
                ));
            }
            _ => return SwitchOpenFailure::Fatal,
        }
    }
    if refused.is_empty() {
        return SwitchOpenFailure::Fatal;
    }
    SwitchOpenFailure::RefusedAfterRunEnd(refused)
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

/// PROVENANCE of one track terminal (16th rework, P1-A).
///
/// A code-less `ServeError::Cancel` is produced BOTH by a peer/relay collapse
/// and by this receiver's own release of the subscription (dropping the
/// handle sends UNSUBSCRIBE, `session/subscribe.rs:278`), and
/// `classify_track_end` cannot tell them apart. The 15th rework worked around
/// that by faulting a torn-down target only when a close CODE happened to be
/// present, which silently dropped every code-less REMOTE failure — including
/// the `Failed`/`None` the drain raises for a malformed subgroup or a failed
/// log write.
///
/// The tag is therefore decided by provenance, not by the shape of the error:
/// the drain task itself records whether the receiver had already begun
/// releasing this subscription at the instant it classified the terminal. The
/// read happens in the same poll that observes the error and BEFORE the event
/// is enqueued, so a terminal that was already queued when the release started
/// is necessarily `Remote` — which is exactly the arrival order the relay-late
/// refusal path produces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TerminalSource {
    /// The receiver had not begun releasing this subscription. The terminal is
    /// the peer's (or a local protocol/log fault), never our own teardown.
    Remote,
    /// The receiver had already set `local_release` — dropped, or is about to
    /// drop, the handle — so this terminal may be its own UNSUBSCRIBE.
    LocalRelease,
}

#[derive(Debug)]
enum S3WireEvent {
    Object(RoutedObject),
    Ended {
        role: TrackRole,
        route: Route,
        end: TrackEnd,
        detail: String,
        /// The application error code of a `ServeError::Closed(code)` ending,
        /// kept TYPED. `classify_track_end` deliberately collapses every
        /// `Cancel`/`Closed(_)` into `Cancelled`, which is right for the
        /// ending decision but loses the one thing that distinguishes the
        /// sender's run-ended signal from a generic teardown. The detail text
        /// must never be parsed for it; this field is the only authority.
        /// `None` for every non-`Closed` ending.
        close_code: Option<u64>,
        /// Whether this terminal can be the receiver's OWN teardown. See
        /// [`TerminalSource`]. A `Closed(code)` is always the peer's whatever
        /// this says; the tag is what makes a CODE-LESS remote failure
        /// distinguishable from our own UNSUBSCRIBE.
        source: TerminalSource,
    },
}

/// The application code of a `Closed(code)` ending, or `None`.
fn track_end_close_code(e: &moq_transport::serve::ServeError) -> Option<u64> {
    match e {
        moq_transport::serve::ServeError::Closed(code) => Some(*code),
        _ => None,
    }
}

/// One live wire subscription and the task draining it.
///
/// `H` is the subscription handle. Production is `Subscribe`; dropping it
/// sends UNSUBSCRIBE. The handle is only ever dropped, never called, which is
/// why it can be a parameter without changing any production behaviour.
struct S3LiveSubscription<H> {
    handle: H,
    drain: tokio::task::JoinHandle<()>,
    /// Set by [`S3LiveSubscription::release_handle`] immediately BEFORE the
    /// handle is dropped, and read by the drain task when it classifies its
    /// terminal. This is the provenance channel of [`TerminalSource`]; it is
    /// never cleared, because a released subscription is never reopened.
    local_release: Arc<AtomicBool>,
}

impl<H> S3LiveSubscription<H> {
    /// Release the wire handle (drop == UNSUBSCRIBE) through the ONE site that
    /// also records the release for provenance, and hand back the drain task.
    ///
    /// Every receiver-initiated release goes through here. The store happens
    /// before the drop, so any terminal the drop itself causes is classified
    /// `LocalRelease`, while anything the drain had already classified keeps
    /// the `Remote` tag it was built with.
    fn release_handle(self) -> tokio::task::JoinHandle<()> {
        self.local_release.store(true, Ordering::Release);
        drop(self.handle);
        self.drain
    }
}

/// The single seam of the S3 receiver: opening one target subscription.
///
/// Production is `SubscriberSeam`, a transparent wrapper that forwards to
/// `Subscriber::subscribe_open_with_params` with the same arguments in the
/// same order — no added branch, no added state, no behaviour change.
///
/// It exists because `run_s3_control` — the real control loop, including the
/// switch request path, the exit drain and the shutdown accounting — otherwise
/// could not be driven without a live QUIC/WebTransport session. The workspace
/// has no offline way to stand one up in a unit test (no self-signed
/// certificate generator among the dependencies, and `dev/spike.*` is
/// untracked and expired), so the loop is made parametric at exactly this one
/// call instead.
trait S3SubscribeSeam: Clone + Send + 'static {
    /// Subscription handle; dropped to release the subscription.
    type Handle: Send + 'static;

    fn subscribe_open(
        &mut self,
        writer: moq_transport::serve::TrackWriter,
        params: KeyValuePairs,
    ) -> impl std::future::Future<
        Output = std::result::Result<Self::Handle, moq_transport::serve::ServeError>,
    > + Send;
}

/// Production seam: `Subscriber::subscribe_open_with_params`, unchanged.
#[derive(Clone)]
struct SubscriberSeam(Subscriber);

impl S3SubscribeSeam for SubscriberSeam {
    type Handle = Subscribe;

    async fn subscribe_open(
        &mut self,
        writer: moq_transport::serve::TrackWriter,
        params: KeyValuePairs,
    ) -> std::result::Result<Subscribe, moq_transport::serve::ServeError> {
        self.0.subscribe_open_with_params(writer, params).await
    }
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
    local_release: Arc<AtomicBool>,
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
                                // Never admitted to the scheduler, so no
                                // deadline was ever evaluated for it.
                                None,
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

    let (end, detail, close_code, source) = match result {
        // End-of-track is the publisher's, and it is never faulted anyway.
        Ok(()) => (TrackEnd::Fin, String::new(), None, TerminalSource::Remote),
        // The ONLY shape our own release can take. The flag is read here, in
        // the same poll that observed the error and before the event is
        // enqueued, so the answer describes the state at classification time.
        Err(DrainFail::Serve(error)) => (
            classify_track_end(&error),
            error.to_string(),
            track_end_close_code(&error),
            if local_release.load(Ordering::Acquire) {
                TerminalSource::LocalRelease
            } else {
                TerminalSource::Remote
            },
        ),
        // A malformed subgroup mode or a failed log write. Dropping the
        // subscription handle cannot produce either, so this is remote (or a
        // local defect) by construction and never our teardown.
        Err(DrainFail::NonSubgroup) => (
            TrackEnd::Failed,
            "invalid S3 subgroup/log state".to_string(),
            None,
            TerminalSource::Remote,
        ),
    };
    let _ = events
        .send(S3WireEvent::Ended {
            role,
            route,
            end,
            detail,
            close_code,
            source,
        })
        .await;
}

#[allow(clippy::too_many_arguments)]
async fn open_s3_subscription<S: S3SubscribeSeam>(
    subscriber: &mut S,
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
) -> Result<(S3LiveSubscription<S::Handle>, u64, u32)> {
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
        match subscriber.subscribe_open(writer, params).await {
            Ok(handle) => {
                let t_ok = now_us();
                let local_release = Arc::new(AtomicBool::new(false));
                let drain = tokio::spawn(drain_s3_track(
                    role,
                    route,
                    reader,
                    logger,
                    events,
                    bad_headers,
                    ingress_drops,
                    log_failed,
                    local_release.clone(),
                ));
                return Ok((
                    S3LiveSubscription {
                        handle,
                        drain,
                        local_release,
                    },
                    t_ok,
                    retries,
                ));
            }
            Err(error) => {
                // Per-role observation instant, taken where the error is seen
                // and BEFORE any retry sleep, so the failure carries when this
                // role was actually refused rather than when the whole request
                // settled after `join!`.
                let t_failed_us = now_us();
                if is_retryable_subscribe_error(&error)
                    && retries < retry_limit
                    && tokio::time::Instant::now() < deadline
                {
                    retries += 1;
                    tokio::time::sleep(Duration::from_millis(args.subscribe_retry_ms)).await;
                    continue;
                }
                return Err(anyhow::Error::new(S3SubscribeFailure {
                    role,
                    route,
                    retries,
                    error,
                    t_failed_us,
                }));
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

#[allow(clippy::too_many_arguments)]
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
    // Stage-9 decision 9-6 `t_due`, from the scheduler that emitted the action.
    due_us: &dyn Fn(&PlayoutAction) -> Option<u64>,
) -> Result<()> {
    settle_tracker_batch(tracker, &actions, now).map_err(anyhow::Error::msg)?;
    for action in actions {
        let t_due = due_us(&action);
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
                        t_due,
                    )?;
                if let Some(t_due) = t_due {
                    if (now.max(object.t_recv) as i64 - t_due as i64) < -MAX_NEGATIVE_LATENESS_US {
                        stats.negative_lateness += 1;
                        debug_assert!(false, "S3 released an object before its deadline");
                    }
                }
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
                        t_due,
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
async fn request_s3_switch<S: S3SubscribeSeam>(
    update: S3Update,
    ingress: &mut S3ReceiverIngress,
    subscriber: &mut S,
    namespace: &TrackNamespace,
    live: &mut HashMap<(TrackRole, u64), S3LiveSubscription<S::Handle>>,
    config: &S3RuntimeConfig,
    args: &Args,
    logger: Arc<Mutex<JsonlLogger>>,
    events: mpsc::Sender<S3WireEvent>,
    bad_headers: Arc<AtomicU64>,
    ingress_drops: Arc<AtomicU64>,
    log_failed: Arc<AtomicU64>,
    window_end_us: Option<u64>,
    stats: &mut PlayoutStats,
    faults: &mut S3SwitchTargetFaults,
) -> Result<SwitchOutcome> {
    let Some(transition) = update.transition else {
        return Ok(SwitchOutcome::NoTransition);
    };
    let request_at = now_us().max(transition.at_us);
    {
        let mut logger = logger
            .lock()
            .map_err(|_| anyhow::anyhow!("RX logger poisoned"))?;
        logger.try_log_s3_transition(transition, update.snapshot)?;
        if !switch_allowed_at(request_at, window_end_us) {
            let window_end_us = window_end_us.expect("a disallowed request has a window end");
            logger.try_log_s3_switch_suppressed_after_end(transition, request_at, window_end_us)?;
            return Ok(SwitchOutcome::SuppressedAfterEnd);
        }
    }
    let request = ingress
        .request(transition, request_at)
        .map_err(|error| anyhow::anyhow!("request S3 switch: {error:?}"))?;
    logger
        .lock()
        .map_err(|_| anyhow::anyhow!("RX logger poisoned"))?
        .try_log_s3_switch_request(request)?;

    // Open the changed roles' target subscriptions CONCURRENTLY. Each SUBSCRIBE
    // costs several RTT; opening them one after the other doubled the time to
    // the haptic SUBSCRIBE_OK and pushed the switch past its effect timeout
    // under RTT/jitter/loss shaping. `Subscriber` is `Clone` and
    // `subscribe_open_with_params` only holds its bookkeeping mutexes
    // synchronously (its single await is the SUBSCRIBE_OK), so the two
    // requests are queued back-to-back and their round trips overlap.
    //
    // Both opens are bounded by the switch's effect timeout (request_at +
    // effect_timeout_us, from the SwitchConfig the gate already holds). A role
    // that has not answered by then is abandoned; whatever did succeed is torn
    // down and the gate is poisoned exactly as a late SUBSCRIBE_OK would
    // poison it, so a stalled SUBSCRIBE cannot hold the control loop for the
    // transport's request timeout.
    let effect_deadline_us = request
        .request_at_us
        .saturating_add(ingress.gate().config().effect_timeout_us);
    let effect_remaining = Duration::from_micros(effect_deadline_us.saturating_sub(now_us()));
    let timed_out = AtomicBool::new(false);
    let mut haptic_subscriber = subscriber.clone();
    let pc_open = async {
        if !request.pc_changed {
            return None;
        }
        let route = request.target.for_role(TrackRole::Pc);
        let open = open_s3_subscription(
            subscriber,
            namespace,
            TrackRole::Pc,
            route,
            false,
            config.switch_retry_limit,
            args,
            logger.clone(),
            events.clone(),
            bad_headers.clone(),
            ingress_drops.clone(),
            log_failed.clone(),
        );
        let result = tokio::select! {
            result = open => result,
            _ = tokio::time::sleep(effect_remaining) => {
                timed_out.store(true, Ordering::Relaxed);
                Err(anyhow::anyhow!(
                    "S3 switch pc SUBSCRIBE did not complete before the effect timeout"
                ))
            }
        };
        Some((TrackRole::Pc, route, result))
    };
    let haptic_open = async {
        if !request.haptic_changed {
            return None;
        }
        let route = request.target.for_role(TrackRole::Haptic);
        let open = open_s3_subscription(
            &mut haptic_subscriber,
            namespace,
            TrackRole::Haptic,
            route,
            false,
            config.switch_retry_limit,
            args,
            logger.clone(),
            events.clone(),
            bad_headers.clone(),
            ingress_drops.clone(),
            log_failed.clone(),
        );
        let result = tokio::select! {
            result = open => result,
            _ = tokio::time::sleep(effect_remaining) => {
                timed_out.store(true, Ordering::Relaxed);
                Err(anyhow::anyhow!(
                    "S3 switch haptic SUBSCRIBE did not complete before the effect timeout"
                ))
            }
        };
        Some((TrackRole::Haptic, route, result))
    };
    let (pc, haptic) = tokio::join!(pc_open, haptic_open);
    let ready = match settle_switch_subscribes(pc.into_iter().chain(haptic).collect()) {
        SwitchSubscribeOutcome::Ready(ready) => ready,
        SwitchSubscribeOutcome::Failed {
            error,
            more_errors,
            cleanup,
        } => {
            let teardown = SwitchTeardown {
                subscriptions: cleanup,
                live_keys: Vec::new(),
            };
            if timed_out.load(Ordering::Relaxed) {
                // Fail through the gate so it is poisoned like a late
                // SUBSCRIBE_OK; the run then ends loudly either way.
                let gate = ingress.check_timeout(now_us());
                let error = anyhow::anyhow!("{error}; gate: {gate:?}");
                return fail_s3_switch(teardown, live, error).await;
            }
            // Settle instant of the WHOLE request (post-`join!`), shared by
            // every row of this request. Each refused role keeps its own
            // observation instant, captured inside `open_s3_subscription`.
            let t_settled = now_us();
            let refused = match classify_switch_open_failure(
                std::iter::once(&error).chain(more_errors.iter()),
            ) {
                SwitchOpenFailure::RefusedAfterRunEnd(refused) => refused,
                SwitchOpenFailure::Fatal => {
                    return fail_s3_switch(teardown, live, error).await;
                }
            };
            // Which changed roles OPENED and were therefore torn down, as
            // opposed to refused. A role whose `subscribe_open` failed has no
            // drain task and can never produce an `S3WireEvent::Ended`.
            let refused_roles: Vec<TrackRole> =
                refused.iter().map(|(role, _, _, _)| *role).collect();
            let torn_down: Vec<(TrackRole, Route)> = [TrackRole::Pc, TrackRole::Haptic]
                .into_iter()
                .filter(|role| match role {
                    TrackRole::Pc => request.pc_changed,
                    TrackRole::Haptic => request.haptic_changed,
                })
                .filter(|role| !refused_roles.contains(role))
                .map(|role| (role, request.target.for_role(role)))
                .collect();
            // The sender's run had already ended when this SUBSCRIBE arrived
            // — either its current routes finished producing or its namespace
            // was draining after the run end; both are signalled by the one
            // registered REQUEST_ERROR code. The switch never took effect:
            // tear down whatever did open exactly as a fatal failure would —
            // joining each aborted drain task so no further RX row or event can
            // appear after this point — abandon the request in the gate so the
            // current routes' FIN can still end the run normally, and account
            // any target object that reached the barrier.
            teardown_s3_switch(teardown, live).await?;
            // The real instant the teardown completed, so the additive row
            // below is not back-dated to the request's settle instant.
            let t_torn_down = now_us();
            let (_, barrier_drops) = ingress
                .abandon_pending(now_us())
                .map_err(|error| anyhow::anyhow!("abandon refused S3 switch: {error:?}"))?;
            {
                let mut logger = logger
                    .lock()
                    .map_err(|_| anyhow::anyhow!("RX logger poisoned"))?;
                for (role, route, code, t_refused) in refused {
                    logger.try_log_s3_switch_refused_after_run_end(
                        role,
                        route,
                        t_refused,
                        t_settled,
                        window_end_us,
                        code,
                    )?;
                }
                // 16th rework, P1-B. The relay path registers every torn-down
                // target in the fault store's watch list; the DIRECT path did
                // not, so a target whose open SUCCEEDED and whose drain had
                // already queued a failure (`Closed(0x10)`, a peer `Cancel`,
                // ...) matched nothing once the gate abandoned the request —
                // not pending, not watched, not current — and was dropped.
                // Same registration, same additive row, same `cause` label.
                for (role, route) in torn_down {
                    logger.try_log_info(&format!(
                        "\"event\":\"s3_switch_target_torn_down\",\"track\":\"{}\",{},\"t_torn_down\":{t_torn_down},\"cause\":\"refused_after_run_end\"",
                        role.as_str(),
                        s3_route_fields(route),
                    ))?;
                    faults.watch_abandoned(role, route);
                }
                log_s3_barrier_drops(&mut logger, barrier_drops, stats)?;
            }
            return Ok(SwitchOutcome::RefusedAfterRunEnd);
        }
    };

    // Gate calls in ascending t_ok order: the gate refuses non-monotonic time,
    // and each role keeps its own SUBSCRIBE_OK timestamp. EVERY failure in this
    // loop (gate timeout, duplicate, SUBSCRIBE_OK record, logger) tears down
    // every target subscription of this request, including those already
    // inserted into `live`, so no drain task leaks and no switch is
    // half-applied.
    let mut apply = SwitchApplyState::new(ready);
    while let Some((role, route, subscription, t_ok, retries)) = apply.take_next() {
        let key = (role, route.generation);
        let checks: Result<()> = if let Err(error) = ingress.check_timeout(t_ok) {
            Err(anyhow::anyhow!(
                "S3 switch timed out while subscribing: {error:?}"
            ))
        } else if live.contains_key(&key) {
            Err(anyhow::anyhow!(
                "duplicate live S3 subscription {} generation {}",
                route.name,
                route.generation
            ))
        } else {
            Ok(())
        };
        if let Err(error) = checks {
            return fail_s3_switch(apply.fail(Some(subscription)), live, error).await;
        }
        live.insert(key, subscription);
        apply.inserted(key);
        let applied: Result<()> = (|| {
            ingress
                .subscribe_ok(role, t_ok)
                .map_err(|error| anyhow::anyhow!("record S3 SUBSCRIBE_OK: {error:?}"))?;
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
            Ok(())
        })();
        if let Err(error) = applied {
            return fail_s3_switch(apply.fail(None), live, error).await;
        }
    }
    Ok(SwitchOutcome::Requested)
}

/// Terminally account target objects the ingress returned from behind the
/// exact-pair barrier (`finish_pending`/`abandon_pending`): the same
/// `role:"drop"` row and the same `dropped` count as every other terminal
/// drop, so no object is unaccounted.
fn log_s3_barrier_drops(
    logger: &mut JsonlLogger,
    events: Vec<IngressEvent>,
    stats: &mut PlayoutStats,
) -> Result<()> {
    for event in events {
        if let IngressEvent::Drop { routed, reason } = event {
            let header = routed.object.header;
            logger.try_log_drop_s3(
                routed.role,
                routed.route,
                header.tier,
                header.seq,
                header.pts_us,
                header.event_id,
                now_us().max(routed.object.t_recv),
                reason,
                // Route/tier integrity, not a scheduling decision.
                None,
            )?;
            stats.dropped += 1;
        }
    }
    Ok(())
}

/// Everything that must be torn down when a switch request fails part-way.
struct SwitchTeardown<S> {
    /// Target subscriptions that were opened but never inserted into `live`.
    subscriptions: Vec<S>,
    /// Keys inserted into `live` during this request; they are removed and
    /// torn down too.
    live_keys: Vec<(TrackRole, u64)>,
}

/// Bookkeeping for the apply loop of one switch request: which settled
/// subscriptions are still pending and which were already inserted into
/// `live`, so that a failure at any step yields the complete teardown set.
struct SwitchApplyState<S> {
    pending: std::collections::VecDeque<(TrackRole, Route, S, u64, u32)>,
    inserted: Vec<(TrackRole, u64)>,
}

impl<S> SwitchApplyState<S> {
    fn new(ready: Vec<(TrackRole, Route, S, u64, u32)>) -> Self {
        Self {
            pending: ready.into_iter().collect(),
            inserted: Vec::new(),
        }
    }

    fn take_next(&mut self) -> Option<(TrackRole, Route, S, u64, u32)> {
        self.pending.pop_front()
    }

    fn inserted(&mut self, key: (TrackRole, u64)) {
        self.inserted.push(key);
    }

    /// Fail at the current step. `current` is the subscription taken by
    /// `take_next` that was NOT inserted into `live` (None once inserted).
    fn fail(self, current: Option<S>) -> SwitchTeardown<S> {
        let mut subscriptions = Vec::with_capacity(self.pending.len() + 1);
        subscriptions.extend(current);
        subscriptions.extend(
            self.pending
                .into_iter()
                .map(|(_, _, subscription, _, _)| subscription),
        );
        SwitchTeardown {
            subscriptions,
            live_keys: self.inserted,
        }
    }
}

/// Empty the exact-pair barrier and terminally account everything it held.
///
/// Extracted so the shutdown path can route a failure here through
/// `first-cause-wins` instead of returning: a failed logger lock or write
/// must not skip drain pass 2, the scheduler flush or the shutdown row.
fn flush_s3_barrier_drops(
    ingress: &mut S3ReceiverIngress,
    logger: &Arc<Mutex<JsonlLogger>>,
    stats: &mut PlayoutStats,
) -> Result<()> {
    let barrier_drops = ingress.finish_pending();
    let mut logger = logger
        .lock()
        .map_err(|_| anyhow::anyhow!("RX logger poisoned"))?;
    log_s3_barrier_drops(&mut logger, barrier_drops, stats)
}

/// First-cause-wins: keep the error that ended the run, record a later one
/// only if there is none yet. Everything on the shutdown path uses this, so
/// no failure after the loop can ever replace the recorded cause — and none
/// of them can skip the rest of the shutdown sequence either.
///
/// SCOPE, stated exactly (15th rework). "First cause" is guaranteed for: the
/// whole shutdown sequence; the control loop's session-end arm, max-duration
/// check, retirement releases and switch-effect timeout, which can fire in the
/// SAME iteration and where a later one is a consequence of the earlier; and
/// the switch-target fault store. It is NOT guaranteed for the remaining
/// in-iteration sites (`log_common_epochs`, `dispatch_s3_playout_actions`,
/// the controller/tracker activation and the observation path) or for the
/// assignments inside `handle_s3_wire_event`: those keep their original
/// last-writer-wins behaviour, so a logging or accounting failure raised after
/// a cause in the same iteration still replaces the reported `detail`. Every
/// such case is itself a fatal fault, and the earlier cause is still in the
/// log as its own row, but the shutdown `detail` is then the later one.
fn keep_first_cause(outcome_error: &mut Option<anyhow::Error>, error: anyhow::Error) {
    if outcome_error.is_none() {
        *outcome_error = Some(error);
    }
}

/// One additive `info` row from the shutdown sequence. Returns the error
/// instead of propagating it so the caller can fold it into `outcome_error`
/// with `keep_first_cause` and still finish the shutdown sequence.
fn log_s3_exit_note(logger: &Arc<Mutex<JsonlLogger>>, body: &str) -> Result<()> {
    logger
        .lock()
        .map_err(|_| anyhow::anyhow!("RX logger poisoned"))?
        .try_log_info(body)
        .map_err(anyhow::Error::from)
}

/// Fold ONE drain-join outcome into the shutdown accounting (16th rework,
/// P1-C).
///
/// * `Joined` — our own abort, or a task that had already finished: normal.
/// * `Panicked` — a run FAULT. The additive `s3_drain_join_panic` row is kept,
///   and the cause is recorded through `keep_first_cause`, so a strictly
///   earlier cause still wins and the panic is still in the log as its own
///   row. It does NOT touch `drains_unjoined`, which counts only tasks that
///   may still be running.
/// * `Unjoined` — still running past the bound: counted, reported in the
///   shutdown row, and fatal through the existing integrity check.
fn note_s3_drain_join(
    join: DrainJoin,
    logger: &Arc<Mutex<JsonlLogger>>,
    drains_unjoined: &mut u64,
    outcome_error: &mut Option<anyhow::Error>,
) {
    match join {
        DrainJoin::Joined => {}
        DrainJoin::Panicked(detail) => {
            // The panic happened BEFORE this row is written, so it is the
            // earlier cause; a failing row write below can only be secondary.
            keep_first_cause(
                outcome_error,
                anyhow::anyhow!("S3 drain task panicked: {detail}"),
            );
            if let Err(error) = log_s3_exit_note(
                logger,
                &format!(
                    "\"event\":\"s3_drain_join_panic\",\"detail\":\"{}\"",
                    json_escape(&detail)
                ),
            ) {
                keep_first_cause(outcome_error, error);
            }
        }
        DrainJoin::Unjoined => *drains_unjoined += 1,
    }
}

/// Bound on joining ONE aborted drain task. An aborted `drain_s3_track` stops
/// at its next await point, which it reaches immediately, so this bound is a
/// liveness guard and never a normal outcome.
const S3_DRAIN_JOIN_BOUND: Duration = Duration::from_secs(1);

/// Tear down every subscription of a switch request that will not be applied.
/// Subscriptions already inserted into `live` are removed first so the
/// shutdown path never sees a half-applied switch.
///
/// Every aborted drain task is JOINED before this returns. That is the
/// property the partial-success path depends on: once teardown returns, the
/// torn-down route can no longer log an `rx` row or enqueue an
/// `S3WireEvent`, so the set of events that still need a terminal is finite
/// and is exactly what is sitting in `event_rx`. A task that does not stop
/// within `S3_DRAIN_JOIN_BOUND` breaks that property, so it is reported as an
/// error rather than silently detached.
async fn teardown_s3_switch<H>(
    teardown: SwitchTeardown<S3LiveSubscription<H>>,
    live: &mut HashMap<(TrackRole, u64), S3LiveSubscription<H>>,
) -> Result<()> {
    let mut unjoined = 0usize;
    let mut panics: Vec<String> = Vec::new();
    let mut note = |join: DrainJoin| match join {
        DrainJoin::Joined => {}
        DrainJoin::Panicked(detail) => panics.push(detail),
        DrainJoin::Unjoined => unjoined += 1,
    };
    for subscription in teardown.subscriptions {
        note(discard_s3_subscription(subscription).await);
    }
    for key in teardown.live_keys {
        if let Some(subscription) = live.remove(&key) {
            note(discard_s3_subscription(subscription).await);
        }
    }
    // A panicking drain is as fatal here as one that never stopped: it stopped
    // mid-track, so nothing guarantees the route was fully accounted.
    if !panics.is_empty() {
        bail!(
            "S3 switch teardown: drain task(s) panicked: {}",
            panics.join("; ")
        );
    }
    if unjoined > 0 {
        bail!(
            "S3 switch teardown: {unjoined} drain task(s) still running after {} ms",
            S3_DRAIN_JOIN_BOUND.as_millis()
        );
    }
    Ok(())
}

/// Tear down every subscription of a failed switch request, then return the
/// error.
///
/// A teardown join timeout never replaces the original failure — the run is
/// already ending with `error` — but it is attached as context so the
/// shutdown `detail` still shows it.
async fn fail_s3_switch<H>(
    teardown: SwitchTeardown<S3LiveSubscription<H>>,
    live: &mut HashMap<(TrackRole, u64), S3LiveSubscription<H>>,
    error: anyhow::Error,
) -> Result<SwitchOutcome> {
    match teardown_s3_switch(teardown, live).await {
        Ok(()) => Err(error),
        Err(teardown_error) => Err(error.context(teardown_error.to_string())),
    }
}

/// What `request_s3_switch` did with one controller update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SwitchOutcome {
    /// The update carried no transition.
    NoTransition,
    /// The switch was requested and is pending in the gate.
    Requested,
    /// The transition was decided at/after the registered window end: logged,
    /// never requested (no gate request, no subscription).
    SuppressedAfterEnd,
    /// The request was issued but every changed role was refused with the
    /// registered run-ended REQUEST_ERROR code (never `DoesNotExist`, and
    /// never by time): torn down and abandoned in the gate without failing
    /// the run. The same outcome is reached from the event handler when a
    /// target route that was already `SUBSCRIBE_OK`ed ends with that code
    /// (`refuse_pending_s3_switch`), which is how the relay delivers it.
    RefusedAfterRunEnd,
}

/// One changed role's target subscription attempt: `(subscription, t_ok, retries)`.
type SwitchSubscribeResult<S> = (TrackRole, Route, Result<(S, u64, u32)>);

/// Settled outcome of the concurrent target subscriptions of one switch request.
enum SwitchSubscribeOutcome<S> {
    /// Every changed role subscribed. Entries are in ascending `t_ok` order
    /// (PC first on a tie) so the gate's monotonic-time contract holds
    /// whichever role's SUBSCRIBE_OK arrived first.
    Ready(Vec<(TrackRole, Route, S, u64, u32)>),
    /// At least one role failed. `cleanup` holds every subscription that did
    /// succeed and must be torn down; `error` is the first failure in
    /// PC-then-haptic order and `more_errors` the remaining ones in that
    /// order, so a refusal classification can inspect every failed role.
    Failed {
        error: anyhow::Error,
        more_errors: Vec<anyhow::Error>,
        cleanup: Vec<S>,
    },
}

fn settle_switch_subscribes<S>(
    results: Vec<SwitchSubscribeResult<S>>,
) -> SwitchSubscribeOutcome<S> {
    let mut ready = Vec::with_capacity(results.len());
    let mut error = None;
    let mut more_errors = Vec::new();
    for (role, route, result) in results {
        match result {
            Ok((subscription, t_ok, retries)) => ready.push((role, route, subscription, t_ok, retries)),
            Err(failure) => {
                if error.is_none() {
                    error = Some(failure);
                } else {
                    more_errors.push(failure);
                }
            }
        }
    }
    if let Some(error) = error {
        return SwitchSubscribeOutcome::Failed {
            error,
            more_errors,
            cleanup: ready
                .into_iter()
                .map(|(_, _, subscription, _, _)| subscription)
                .collect(),
        };
    }
    ready.sort_by_key(|(role, _, _, t_ok, _)| (*t_ok, *role as u8));
    SwitchSubscribeOutcome::Ready(ready)
}

/// Outcome of joining ONE aborted drain task (16th rework, P1-C).
///
/// The 15th rework collapsed this into a bool, which hid a PANICKING drain:
/// `timeout(..).await` answers `Ok(Err(JoinError))` for a panic, so a panic
/// counted as "joined within the bound" and changed nothing. A drain that
/// panics between two objects leaves no unterminated object, drops its sender
/// clone (so the queue still reports `Disconnected`) and, with the current
/// routes FIN'd, trips no other integrity check — it must be reported.
#[derive(Debug)]
enum DrainJoin {
    /// Stopped within the bound: cancelled by our abort, or already finished.
    Joined,
    /// Stopped within the bound, but the task had PANICKED.
    Panicked(String),
    /// Still running after `S3_DRAIN_JOIN_BOUND`.
    Unjoined,
}

/// Join one aborted drain task, bounded, distinguishing a panic from our own
/// cancellation.
async fn join_s3_drain(drain: tokio::task::JoinHandle<()>) -> DrainJoin {
    match tokio::time::timeout(S3_DRAIN_JOIN_BOUND, drain).await {
        // Our own abort is the NORMAL outcome here and is not a fault.
        Ok(Err(error)) if error.is_panic() => DrainJoin::Panicked(error.to_string()),
        Ok(_) => DrainJoin::Joined,
        Err(_) => DrainJoin::Unjoined,
    }
}

/// Tear down a target subscription that will not be applied: release the
/// handle (UNSUBSCRIBE), abort its drain task, and join it.
///
/// The release goes through `S3LiveSubscription::release_handle`, so every
/// terminal the drain classifies from this point on is tagged
/// `TerminalSource::LocalRelease` and is never mistaken for a peer failure.
/// The join result distinguishes a task that did not stop within
/// `S3_DRAIN_JOIN_BOUND` (the route can no longer be assumed silent) from one
/// that stopped by PANICKING (a defect that must fail the run).
async fn discard_s3_subscription<H>(subscription: S3LiveSubscription<H>) -> DrainJoin {
    let drain = subscription.release_handle();
    drain.abort();
    join_s3_drain(drain).await
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
fn release_retired_s3_subscription<H>(
    role: TrackRole,
    route: Route,
    subscription: S3LiveSubscription<H>,
    cause: RetirementCause,
    now: u64,
    logger: &Arc<Mutex<JsonlLogger>>,
    retired_drains: &mut Vec<tokio::task::JoinHandle<()>>,
) -> Result<()> {
    retired_drains.push(subscription.release_handle());
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
/// Registered window end from the first measurement object: t0 is recovered
/// as `gen_ts_us - pts_us` (the sender stamps `t_gen` after waking at slot
/// `t0 + pts_us`), so the result is never earlier than the true window end.
/// `None` only on arithmetic overflow/underflow of a malformed header.
fn derive_window_end_us(header: Header, duration_us: u64) -> Option<(u64, u64)> {
    let t0_rx_us = header.gen_ts_us.checked_sub(header.pts_us)?;
    let end_us = t0_rx_us.checked_add(duration_us)?;
    Some((t0_rx_us, end_us))
}

/// Body of the additive `role:"info"` row that records the applied window end
/// and its derivation (which object, `t0_rx_us = gen_ts_us - pts_us`).
fn s3_window_end_row_body(
    role: TrackRole,
    route: Route,
    header: Header,
    t0_rx_us: u64,
    duration_us: u64,
    window_end_us: u64,
) -> String {
    format!(
        "\"event\":\"s3_window_end\",\"window_end_us\":{window_end_us},\"t0_rx_us\":{t0_rx_us},\"duration_us\":{duration_us},\"t0_source\":\"first_object_gen_ts_minus_pts\",\"track\":\"{}\",{},\"seq\":{},\"pts_us\":{},\"gen_ts_us\":{}",
        role.as_str(),
        s3_route_fields(route),
        header.seq,
        header.pts_us,
        header.gen_ts_us,
    )
}

/// Body of the S3 shutdown row. The vocabulary up to `detail` is unchanged;
/// the S3 fields after it are additive. 12th rework: how many controller
/// transitions were never requested because their request instant fell at or
/// after the registered window end, how many requested switches the sender
/// refused because the sender's run had ended, and the window end applied (`null` if
/// unknown). 13th rework: `s3_events_drained_at_exit`, how many queued wire
/// events were still routed through the ingress after the control loop
/// exited (both drain passes summed). A non-zero value is normal — it is the
/// count of objects/FINs that would previously have been discarded with the
/// channel — and it is reported so the terminal accounting is auditable from
/// the log alone.
///
/// 14th rework (additive): `s3_event_queue_closed`, the verdict of drain pass
/// 2 — `true` means `try_recv` reported `Disconnected` after every drain task
/// was joined and the loop's own sender was dropped, i.e. the wire-event
/// queue is provably empty forever. It is the log-visible witness that pass 2
/// ran at all, which the counter above cannot show (a run with nothing left
/// to drain also reports 0). `s3_drains_unjoined`, the number of aborted
/// drain tasks that did NOT stop within `S3_DRAIN_JOIN_BOUND` at shutdown;
/// any non-zero value means a task could still write after this row, so the
/// run is failed.
fn s3_shutdown_row_body(
    failed: bool,
    bad_headers: u64,
    detail: &str,
    switch_suppressed_after_end: u64,
    switch_refused_after_run_end: u64,
    window_end_us: Option<u64>,
    events_drained_at_exit: u64,
    event_queue_closed: bool,
    drains_unjoined: u64,
) -> String {
    format!(
        "\"event\":\"shutdown\",\"ending\":\"{}\",\"exit_code\":{},\"bad_headers\":{},\"subscribe_retries\":0,\"subscribe_wait_ms\":0,\"tracks\":[],\"detail\":\"{}\",\"s3_switch_suppressed_after_end\":{switch_suppressed_after_end},\"s3_switch_refused_after_run_end\":{switch_refused_after_run_end},\"s3_window_end_us\":{},\"s3_events_drained_at_exit\":{events_drained_at_exit},\"s3_event_queue_closed\":{},\"s3_drains_unjoined\":{drains_unjoined}",
        if failed { "error" } else { "normal" },
        if failed { 1 } else { 0 },
        bad_headers,
        json_escape(detail),
        window_end_us
            .map(|value| value.to_string())
            .unwrap_or_else(|| "null".to_string()),
        if event_queue_closed { "true" } else { "false" },
    )
}

fn count_switch_outcome(outcome: SwitchOutcome, suppressed: &mut u64, refused: &mut u64) {
    match outcome {
        SwitchOutcome::SuppressedAfterEnd => *suppressed += 1,
        SwitchOutcome::RefusedAfterRunEnd => *refused += 1,
        SwitchOutcome::NoTransition | SwitchOutcome::Requested => {}
    }
}

/// `wire_track`/`route_generation` fields in the S3 row vocabulary.
fn s3_route_fields(route: Route) -> String {
    format!(
        "\"wire_track\":\"{}\",\"route_generation\":{}",
        json_escape(route.name),
        route.generation
    )
}

/// What the control loop must do after one wire event was handled.
///
/// The two variants are exactly what the pre-extraction inline code did, so
/// moving the block into a function changed no control flow:
///   * `Continue` — fall through to this iteration's `scheduler.advance`,
///     epoch logging and action dispatch (this includes the inline `break`s
///     out of the ingress-event loop, which fell through too);
///   * `SkipIteration` — the inline `continue`, skipping the rest of the
///     iteration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EventFlow {
    Continue,
    SkipIteration,
}

/// Does this track ending mean "the sender refused the PENDING switch"?
///
/// In relay topology the downstream SUBSCRIBE is answered before the upstream
/// one is (moq-relay-ietf `local.rs` -> `producer.rs` -> moq-transport
/// `subscribed.rs`), so the sender's `REQUEST_ERROR(S3_RUN_ENDED…)` can arrive
/// AFTER `SUBSCRIBE_OK`, as `PUBLISH_DONE(S3_RUN_ENDED…)` on the already-open
/// target track. It carries the same code and means exactly what a direct
/// refusal means, so it must take the same non-fatal path.
///
/// The condition is deliberately narrow — ALL of:
///   * a switch request is PENDING (it has not been applied: applying it
///     clears the pending request);
///   * this role is one the request CHANGES (for an unchanged role the target
///     route IS the current route, and a current route that ends non-FIN must
///     stay fatal);
///   * the ended route IS that role's target route of the pending request;
///   * the ending is `Cancelled` — `Closed(_)` never classifies as anything
///     else — and
///   * the close code is exactly `S3_RUN_ENDED_REQUEST_ERROR_CODE`.
/// Any other terminal on a pending target route keeps the previous behaviour.
fn is_pending_target_run_ended_refusal(
    ingress: &S3ReceiverIngress,
    role: TrackRole,
    route: Route,
    end: TrackEnd,
    close_code: Option<u64>,
) -> bool {
    let Some(request) = ingress.gate().pending_request() else {
        return false;
    };
    let changed = match role {
        TrackRole::Pc => request.pc_changed,
        TrackRole::Haptic => request.haptic_changed,
    };
    changed
        && request.target.for_role(role) == route
        && end == TrackEnd::Cancelled
        && close_code == Some(S3_RUN_ENDED_REQUEST_ERROR_CODE)
}

/// Terminals observed on the target routes of a switch request that never
/// applied.
///
/// Why this exists (15th rework, P1). The relay-late refusal path is
/// non-fatal by design: the sender's run ended, nothing changed on wire, so
/// the run may still end by its normal FIN rule. But a pending switch has TWO
/// targets, and a target route is never a current route, so a GENERAL
/// terminal on one of them (`Closed(0x10)`, `Failed`, ...) was observed by the
/// event handler and then dropped — the current-route branch did not match it
/// and nothing else looked at it. A run-ended refusal on the SIBLING role then
/// abandoned the whole request and `recompute_s3_normal_end` let the run end
/// normally, so a real transport failure was normalised away.
///
/// The direct path never had that hole: `classify_switch_open_failure`
/// inspects EVERY failed role and is `Fatal` on any mix. This store is what
/// makes the relay path obey the same rule — it is the only state that
/// survives `abandon_pending`, the sibling's refusal and
/// `recompute_s3_normal_end`.
///
/// ORDERING. The cause is persisted here the moment the handler sees it and
/// is promoted into `outcome_error` at the first of: the sibling's refusal
/// (`handle_s3_wire_event`) or the exit merge (`run_s3_control`). Promotion
/// always goes through `keep_first_cause`, so a STRICTLY EARLIER cause still
/// wins; in that case the terminal is still in the log as its own
/// `s3_switch_target_terminal_fault` row. It is never erased, only ordered.
///
/// It is deliberately NOT written straight into `outcome_error` at
/// observation: that would end the control loop at the first dead target and
/// replace the registered 2 s switch effect timeout as the recorded cause of
/// a switch that failed with its sibling still alive.
#[derive(Default)]
struct S3SwitchTargetFaults {
    /// Target routes of a switch that the relay-late refusal path tore down.
    /// Their drain tasks were aborted, but an `Ended` they had ALREADY
    /// enqueued still arrives, and by then the gate has no pending request to
    /// recognise them by.
    watched: Vec<(TrackRole, Route)>,
    /// Target routes on which a non-run-ended terminal was observed.
    faulted: Vec<(TrackRole, Route)>,
    /// The first such terminal, kept as the run's failure cause.
    cause: Option<anyhow::Error>,
}

impl S3SwitchTargetFaults {
    /// Remember a target route that was torn down with a pending request that
    /// was abandoned, so a straggling terminal on it is still classified.
    fn watch_abandoned(&mut self, role: TrackRole, route: Route) {
        if !self.watched.contains(&(role, route)) {
            self.watched.push((role, route));
        }
    }

    fn is_watched(&self, role: TrackRole, route: Route) -> bool {
        self.watched.contains(&(role, route))
    }

    fn forget(&mut self, role: TrackRole, route: Route) {
        self.watched.retain(|entry| *entry != (role, route));
    }

    /// First-cause-wins inside the store as well: a second faulted target is
    /// still recorded as faulted (so the refusal rule sees it), but the cause
    /// text stays the first one.
    fn record(&mut self, role: TrackRole, route: Route, error: anyhow::Error) {
        if !self.faulted.contains(&(role, route)) {
            self.faulted.push((role, route));
        }
        if self.cause.is_none() {
            self.cause = Some(error);
        }
    }

    /// Does THIS pending request have a faulted target? Routes carry their
    /// generation, so this can never match a different switch's target.
    fn request_has_fault(&self, request: SwitchRequest) -> bool {
        [TrackRole::Pc, TrackRole::Haptic].into_iter().any(|role| {
            let changed = match role {
                TrackRole::Pc => request.pc_changed,
                TrackRole::Haptic => request.haptic_changed,
            };
            changed && self.faulted.contains(&(role, request.target.for_role(role)))
        })
    }

    fn take_cause(&mut self) -> Option<anyhow::Error> {
        self.cause.take()
    }
}

/// Is this terminal of an UNAPPLIED switch target a run fault?
///
/// `pending` is what `unapplied_switch_target` answers: `true` while the gate
/// still holds the request (the subscription is live), `false` once the
/// relay-late refusal path abandoned it and only the watch list remembers the
/// route.
///
/// The rule, stated once (16th rework, P1-A):
///   * the sender's run-ended code is NEVER a fault, on either side;
///   * a FIN is never a fault (it is the publisher's clean end of track);
///   * while PENDING, any other terminal is the peer's, because the receiver
///     has not released anything yet;
///   * once WATCHED, the receiver has dropped the handle, so a terminal is a
///     fault when it cannot be that release: either it carries a close CODE
///     (our UNSUBSCRIBE never produces one) or the drain classified it before
///     the release began (`TerminalSource::Remote`).
///
/// The 15th rework used `close_code.is_some()` alone for the watched case,
/// which dropped every code-less remote failure — a peer `Cancel` and the
/// `Failed`/`None` of a malformed subgroup or a failed log write.
fn is_switch_target_fault(
    pending: bool,
    end: TrackEnd,
    close_code: Option<u64>,
    source: TerminalSource,
) -> bool {
    if close_code == Some(S3_RUN_ENDED_REQUEST_ERROR_CODE) || end == TrackEnd::Fin {
        return false;
    }
    pending || close_code.is_some() || source == TerminalSource::Remote
}

/// Is this ended route a target of a switch that never applied?
///
/// Two ways to be one, and they are treated differently on purpose:
///   * the gate still has the PENDING request and this is a changed role's
///     target — the subscription is still live, so any terminal on it comes
///     from the peer;
///   * the request was already abandoned by a refusal/teardown path (relay or
///     direct) and the route is `watched` — here the receiver itself released
///     the handle, so a code-less `Cancelled` may be its OWN teardown. The
///     caller resolves that by PROVENANCE, not by the presence of a code; see
///     `is_switch_target_fault`.
fn unapplied_switch_target(
    ingress: &S3ReceiverIngress,
    faults: &S3SwitchTargetFaults,
    role: TrackRole,
    route: Route,
) -> Option<bool> {
    if let Some(request) = ingress.gate().pending_request() {
        let changed = match role {
            TrackRole::Pc => request.pc_changed,
            TrackRole::Haptic => request.haptic_changed,
        };
        if changed && request.target.for_role(role) == route {
            return Some(true);
        }
    }
    faults.is_watched(role, route).then_some(false)
}

/// The run ends by its normal FIN rule as soon as both current routes have
/// FINished and nothing is pending. Re-evaluated whenever a pending request
/// is abandoned, because the FINs may have been observed WHILE it was
/// pending — in which case nothing else would ever re-check the rule and the
/// run would sit until `--max-duration`.
fn recompute_s3_normal_end(
    ingress: &S3ReceiverIngress,
    current_finished: &[bool; 2],
    normal_end: &mut bool,
) {
    *normal_end = current_finished.iter().all(|finished| *finished)
        && ingress.gate().pending_request().is_none();
}

/// Take the NON-FATAL refusal path for a pending switch whose target route
/// was refused after `SUBSCRIBE_OK` (see
/// `is_pending_target_run_ended_refusal`).
///
/// Identical in effect to the direct-refusal path in `request_s3_switch`:
/// tear down every target subscription this request opened, abandon the
/// pending gate request (the applied state and the active routes are
/// untouched — nothing changed on wire), terminally account whatever reached
/// the exact-pair barrier, record the refusal, and let the run end by its
/// normal FIN rule.
///
/// The one difference is the join: this runs in the synchronous event
/// handler, so each torn-down drain task is aborted and pushed onto
/// `retired_drains` — the same deferral `release_retired_s3_subscription`
/// already uses — and joined (bounded) in the shutdown sequence BEFORE drain
/// pass 2, which is what keeps "every rx object has exactly one terminal"
/// true for anything those tasks wrote before stopping.
///
/// The torn-down sibling targets are handed to `faults.watch_abandoned` on
/// the way out. The abort stops the drain task, but an `Ended` it had already
/// enqueued still arrives after the gate has forgotten the request, and that
/// terminal must still be classified (15th rework, P1).
///
/// PRECONDITION: the caller has already established that no target of this
/// request faulted (`S3SwitchTargetFaults::request_has_fault`). A mixed
/// request is fatal, exactly as `classify_switch_open_failure` makes it fatal
/// on the direct path.
#[allow(clippy::too_many_arguments)]
fn refuse_pending_s3_switch<H>(
    observed_role: TrackRole,
    observed_route: Route,
    now: u64,
    window_end_us: Option<u64>,
    ingress: &mut S3ReceiverIngress,
    live: &mut HashMap<(TrackRole, u64), S3LiveSubscription<H>>,
    retired_drains: &mut Vec<tokio::task::JoinHandle<()>>,
    logger: &Arc<Mutex<JsonlLogger>>,
    stats: &mut PlayoutStats,
    refused_after_run_end: &mut u64,
    faults: &mut S3SwitchTargetFaults,
) -> Result<()> {
    let request = ingress
        .gate()
        .pending_request()
        .context("refused S3 switch has no pending request")?;
    let mut torn_down: Vec<(TrackRole, Route)> = Vec::new();
    for role in [TrackRole::Pc, TrackRole::Haptic] {
        let changed = match role {
            TrackRole::Pc => request.pc_changed,
            TrackRole::Haptic => request.haptic_changed,
        };
        if !changed {
            continue;
        }
        let target = request.target.for_role(role);
        let Some(subscription) = live.remove(&(role, target.generation)) else {
            continue;
        };
        let drain = subscription.release_handle();
        drain.abort();
        retired_drains.push(drain);
        if role != observed_role {
            torn_down.push((role, target));
        }
    }
    let (_, barrier_drops) = ingress
        .abandon_pending(now)
        .map_err(|error| anyhow::anyhow!("abandon refused S3 switch: {error:?}"))?;
    {
        let mut logger = logger
            .lock()
            .map_err(|_| anyhow::anyhow!("RX logger poisoned"))?;
        // The same row a direct refusal writes, with this role's own
        // observation instant. `t_settled` is the same instant: the terminal
        // IS the settlement here, there is no later `join!` to wait for.
        logger.try_log_s3_switch_refused_after_run_end(
            observed_role,
            observed_route,
            now,
            now,
            window_end_us,
            S3_RUN_ENDED_REQUEST_ERROR_CODE,
        )?;
        // Additive, and the only thing that distinguishes this path from a
        // direct refusal in the log: the refusal arrived AFTER SUBSCRIBE_OK.
        // The refusal row above keeps its registered shape.
        logger.try_log_info(&format!(
            "\"event\":\"s3_switch_refused_after_subscribe_ok\",\"track\":\"{}\",{},\"t_refused\":{now},\"error_code\":{}",
            observed_role.as_str(),
            s3_route_fields(observed_route),
            S3_RUN_ENDED_REQUEST_ERROR_CODE,
        ))?;
        // The sibling target role was never refused itself, so it gets no
        // refusal row (that would claim an observation that never happened);
        // it is recorded as what it is — torn down with the switch.
        for (role, route) in torn_down {
            logger.try_log_info(&format!(
                "\"event\":\"s3_switch_target_torn_down\",\"track\":\"{}\",{},\"t_torn_down\":{now},\"cause\":\"refused_after_run_end\"",
                role.as_str(),
                s3_route_fields(route),
            ))?;
            faults.watch_abandoned(role, route);
        }
        log_s3_barrier_drops(&mut logger, barrier_drops, stats)?;
    }
    *refused_after_run_end += 1;
    Ok(())
}

/// Route ONE `S3WireEvent` through the receiver's ingress path.
///
/// Extracted verbatim from the control loop so that the loop and the
/// drain-at-exit run the same code rather than two copies of it. Every
/// terminal decision an object can get — `stale_tier`, `switch_barrier`,
/// `duplicate_identity`, admission to the common scheduler — is made here and
/// nowhere else, which is what makes the exit drain able to close an object
/// the loop never dequeued.
///
/// `outcome_error` is written exactly where the inline code wrote it,
/// including its last-writer-wins behaviour, so an error path reports the same
/// `detail` as before. The switch-target fault sites added in the 15th rework
/// are the exception and use `keep_first_cause`, because their whole purpose
/// is that the cause they record survives what the sibling role does next.
#[allow(clippy::too_many_arguments)]
fn handle_s3_wire_event<H>(
    event: S3WireEvent,
    now: u64,
    window_duration_us: Option<u64>,
    window_end_us: &mut Option<u64>,
    ingress: &mut S3ReceiverIngress,
    scheduler: &mut PlayoutScheduler,
    tracker: &mut S3DeadlineTracker,
    object_routes: &mut HashMap<S3ObjectKey, (TrackRole, Route)>,
    live: &mut HashMap<(TrackRole, u64), S3LiveSubscription<H>>,
    retiring: &mut S3RetirementQueue<S3LiveSubscription<H>>,
    retired_drains: &mut Vec<tokio::task::JoinHandle<()>>,
    current_finished: &mut [bool; 2],
    normal_end: &mut bool,
    scheduler_actions: &mut Vec<PlayoutAction>,
    recv_pc: &AtomicU64,
    recv_haptic: &AtomicU64,
    logger: &Arc<Mutex<JsonlLogger>>,
    stats: &mut PlayoutStats,
    refused_after_run_end: &mut u64,
    faults: &mut S3SwitchTargetFaults,
    outcome_error: &mut Option<anyhow::Error>,
) -> EventFlow {
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
            if window_end_us.is_none() {
                if let Some(duration_us) = window_duration_us {
                    let header = routed.object.header;
                    match derive_window_end_us(header, duration_us) {
                        Some((t0_rx_us, end_us)) => {
                            *window_end_us = Some(end_us);
                            if let Err(error) = logger
                                .lock()
                                .map_err(|_| anyhow::anyhow!("RX logger poisoned"))
                                .and_then(|mut logger| {
                                    logger
                                        .try_log_info(&s3_window_end_row_body(
                                            routed.role,
                                            routed.route,
                                            header,
                                            t0_rx_us,
                                            duration_us,
                                            end_us,
                                        ))
                                        .map_err(anyhow::Error::from)
                                })
                            {
                                *outcome_error = Some(error);
                                return EventFlow::SkipIteration;
                            }
                        }
                        None => {
                            *outcome_error = Some(anyhow::anyhow!(
                                "S3 window end overflow: gen_ts_us={} pts_us={} duration_us={duration_us}",
                                header.gen_ts_us,
                                header.pts_us
                            ));
                            return EventFlow::SkipIteration;
                        }
                    }
                }
            }
            let ingress_events = match ingress.push(routed, now) {
                Ok(events) => events,
                Err(error) => {
                    *outcome_error =
                        Some(anyhow::anyhow!("S3 ingress validation failed: {error}"));
                    return EventFlow::SkipIteration;
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
                                            // Identity integrity, not a
                                            // scheduling decision.
                                            None,
                                        )
                                        .map_err(anyhow::Error::from)
                                })
                            {
                                *outcome_error = Some(error);
                                break;
                            }
                            stats.dropped += 1;
                            continue;
                        }
                        if let Err(error) = tracker.note_received(&routed.object) {
                            *outcome_error = Some(anyhow::anyhow!(error));
                            break;
                        }
                        let key = S3ObjectKey::from(&routed.object);
                        if object_routes
                            .insert(key, (routed.role, routed.route))
                            .is_some()
                        {
                            *outcome_error =
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
                                        // Route/tier integrity, not a
                                        // scheduling decision.
                                        None,
                                    )
                                    .map_err(anyhow::Error::from)
                            })
                        {
                            *outcome_error = Some(error);
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
                            scheduler,
                            scheduler_actions,
                            object_routes,
                            tracker,
                        ) {
                            *outcome_error = Some(anyhow::anyhow!(error));
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
                            *outcome_error = Some(error);
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
                                *outcome_error = Some(error);
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
                                live,
                                retiring,
                                role,
                                route,
                                now,
                            ) {
                                Ok(()) => {}
                                Err(RetireError::Missing) => {
                                    *outcome_error = Some(anyhow::anyhow!(
                                        "missing old S3 subscription {} generation {}",
                                        route.name,
                                        route.generation
                                    ));
                                    break;
                                }
                                Err(RetireError::Duplicate(old)) => {
                                    let drain = old.release_handle();
                                    drain.abort();
                                    retired_drains.push(drain);
                                    *outcome_error = Some(anyhow::anyhow!(
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
            close_code,
            source,
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
                    logger,
                    retired_drains,
                ) {
                    *outcome_error = Some(error);
                }
            }
            // A target route of a switch that never applied, ending with
            // anything OTHER than the sender's run-ended code, is a run
            // failure that must survive whatever the sibling role does next
            // (15th rework, P1). It is persisted in `faults` here and never
            // erased; `abandon_pending`, the sibling's refusal and
            // `recompute_s3_normal_end` cannot reach it.
            if let Some(pending) = unapplied_switch_target(ingress, faults, role, route) {
                // The whole rule lives in `is_switch_target_fault`; a watched
                // (torn-down) target is separated from our own teardown by the
                // terminal's PROVENANCE, not by whether a close code happens
                // to be present.
                let general_terminal = is_switch_target_fault(pending, end, close_code, source);
                if general_terminal {
                    if let Err(error) = logger
                        .lock()
                        .map_err(|_| anyhow::anyhow!("RX logger poisoned"))
                        .and_then(|mut logger| {
                            logger
                                .try_log_info(&format!(
                                    "\"event\":\"s3_switch_target_terminal_fault\",\"track\":\"{}\",{},\"t_ended\":{now},\"end\":\"{:?}\",\"error_code\":{},\"pending\":{pending},\"terminal_source\":\"{:?}\"",
                                    role.as_str(),
                                    s3_route_fields(route),
                                    end,
                                    close_code
                                        .map(|code| code.to_string())
                                        .unwrap_or_else(|| "null".to_string()),
                                    source,
                                ))
                                .map_err(anyhow::Error::from)
                        })
                    {
                        keep_first_cause(outcome_error, error);
                    }
                    faults.record(
                        role,
                        route,
                        anyhow::anyhow!(
                            "S3 switch target {} generation {} ended {:?} (error_code={}): {}",
                            route.name,
                            route.generation,
                            end,
                            close_code
                                .map(|code| code.to_string())
                                .unwrap_or_else(|| "none".to_string()),
                            detail
                        ),
                    );
                }
                if !pending {
                    faults.forget(role, route);
                }
            }
            // The relay form of a run-ended refusal: a target route of the
            // PENDING switch, accepted downstream before the upstream answer
            // was known, now ends with the sender's run-ended code. Same
            // meaning as a direct refusal, so the same non-fatal path.
            if is_pending_target_run_ended_refusal(ingress, role, route, end, close_code) {
                // ... but ONLY when every observed terminal of this request's
                // targets was the run-ended code. This is the same rule
                // `classify_switch_open_failure` applies to every failed role
                // on the direct path, so the two contracts now agree: a mix of
                // a run-ended refusal and a general error is fatal on both.
                let request = ingress
                    .gate()
                    .pending_request()
                    .expect("a pending-target refusal has a pending request");
                if faults.request_has_fault(request) {
                    // The sibling's fault is promoted to the run's cause here,
                    // so the refusal cannot bury it. If an EARLIER cause is
                    // already recorded, first-cause-wins keeps that one and
                    // the terminal stays in the log as its own
                    // `s3_switch_target_terminal_fault` row.
                    if let Some(cause) = faults.take_cause() {
                        keep_first_cause(outcome_error, cause);
                    }
                    if let Err(error) = logger
                        .lock()
                        .map_err(|_| anyhow::anyhow!("RX logger poisoned"))
                        .and_then(|mut logger| {
                            logger
                                .try_log_info(&format!(
                                    "\"event\":\"s3_switch_refusal_not_taken\",\"track\":\"{}\",{},\"t_refused\":{now},\"error_code\":{},\"cause\":\"switch_target_terminal_fault\"",
                                    role.as_str(),
                                    s3_route_fields(route),
                                    S3_RUN_ENDED_REQUEST_ERROR_CODE,
                                ))
                                .map_err(anyhow::Error::from)
                        })
                    {
                        keep_first_cause(outcome_error, error);
                    }
                    return EventFlow::Continue;
                }
                match refuse_pending_s3_switch(
                    role,
                    route,
                    now,
                    *window_end_us,
                    ingress,
                    live,
                    retired_drains,
                    logger,
                    stats,
                    refused_after_run_end,
                    faults,
                ) {
                    Ok(()) => {
                        recompute_s3_normal_end(ingress, current_finished, normal_end);
                    }
                    Err(error) => *outcome_error = Some(error),
                }
                return EventFlow::Continue;
            }
            let current = ingress.gate().active_routes().for_role(role);
            if current == route {
                if end != TrackEnd::Fin {
                    *outcome_error = Some(anyhow::anyhow!(
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
                    recompute_s3_normal_end(ingress, current_finished, normal_end);
                }
            }
        }
    }
    EventFlow::Continue
}

/// Result of one drain-at-exit pass over `event_rx`.
struct ExitDrain {
    /// Events routed through the ingress path by this pass.
    drained: u64,
    /// `try_recv` reported `Disconnected`: the queue is empty AND every
    /// sender clone is gone, so no further event can ever appear. `false`
    /// means the pass merely found the queue momentarily empty.
    disconnected: bool,
}

/// Drain whatever is still queued in `event_rx` and route it through the SAME
/// ingress path the control loop uses (`handle_s3_wire_event`).
///
/// Why this exists (13th rework, P1-2). A switch can succeed for one role and
/// be refused for the other. The successful role's drain task starts
/// immediately: it logs `rx` rows and enqueues `S3WireEvent::Object`s for the
/// target route. The refusal then tears that route down and abandons the
/// pending request, and the two current routes' FINs — already queued ahead of
/// those objects — end the run. The target objects were never dequeued, so
/// they were never pushed into the ingress and never got a terminal:
///   * `S3ReceiverIngress::abandon_pending` only returns objects that already
///     reached the exact-pair barrier, which these never did;
///   * the `object_routes` residue check cannot see them either, because
///     `object_routes` is only written when an object is admitted to the
///     scheduler — an object that never left the channel was never registered,
///     so an empty `object_routes` proves nothing about it.
/// Draining them here is what closes that gap: each one is pushed into the
/// ingress, which assigns it the terminal it would have received in the loop
/// (`stale_tier` for a retired/abandoned generation, `switch_barrier`,
/// `duplicate_identity`, or admission to the scheduler, whose shutdown flush
/// then releases or drops it).
///
/// Errors follow FIRST-cause-wins across the loop/drain boundary: an error
/// raised while draining never overwrites the error that ended the loop,
/// because the drain runs after that error and is a consequence of it. The
/// pass keeps going after a failure so that the remaining events still reach a
/// terminal.
#[allow(clippy::too_many_arguments)]
fn drain_s3_events_at_exit<H>(
    event_rx: &mut mpsc::Receiver<S3WireEvent>,
    window_duration_us: Option<u64>,
    window_end_us: &mut Option<u64>,
    ingress: &mut S3ReceiverIngress,
    scheduler: &mut PlayoutScheduler,
    tracker: &mut S3DeadlineTracker,
    object_routes: &mut HashMap<S3ObjectKey, (TrackRole, Route)>,
    live: &mut HashMap<(TrackRole, u64), S3LiveSubscription<H>>,
    retiring: &mut S3RetirementQueue<S3LiveSubscription<H>>,
    retired_drains: &mut Vec<tokio::task::JoinHandle<()>>,
    current_finished: &mut [bool; 2],
    normal_end: &mut bool,
    recv_pc: &AtomicU64,
    recv_haptic: &AtomicU64,
    logger: &Arc<Mutex<JsonlLogger>>,
    stats: &mut PlayoutStats,
    refused_after_run_end: &mut u64,
    faults: &mut S3SwitchTargetFaults,
    outcome_error: &mut Option<anyhow::Error>,
) -> ExitDrain {
    let mut drained = 0u64;
    let disconnected = loop {
        let event = match event_rx.try_recv() {
            Ok(event) => event,
            Err(mpsc::error::TryRecvError::Empty) => break false,
            Err(mpsc::error::TryRecvError::Disconnected) => break true,
        };
        drained += 1;
        let now = now_us();
        let mut actions = Vec::new();
        let mut failure: Option<anyhow::Error> = None;
        // The flow verdict is for the control loop; here there is no rest of
        // the iteration to skip, and on `SkipIteration` no action was staged.
        let _ = handle_s3_wire_event(
            event,
            now,
            window_duration_us,
            window_end_us,
            ingress,
            scheduler,
            tracker,
            object_routes,
            live,
            retiring,
            retired_drains,
            current_finished,
            normal_end,
            &mut actions,
            recv_pc,
            recv_haptic,
            logger,
            stats,
            refused_after_run_end,
            faults,
            &mut failure,
        );
        // Same terminal accounting as the shutdown flush that follows: no
        // bridge, no render, no audio — this path is teardown, not playback.
        let dispatched = dispatch_s3_playout_actions(
            actions,
            object_routes,
            tracker,
            logger,
            None,
            false,
            false,
            stats,
            now,
            &|action| scheduler.action_due_us(action),
        );
        for error in [failure, dispatched.err()].into_iter().flatten() {
            if outcome_error.is_none() {
                *outcome_error = Some(error);
            }
        }
    };
    ExitDrain {
        drained,
        disconnected,
    }
}

/// Establish the transport session, answer the direct-topology announce, and
/// hand the real control loop a production seam.
///
/// Everything below the session handshake lives in `run_s3_control`; this
/// function is the I/O prologue and nothing else. The split exists so that
/// `run_s3_control` — the loop, the switch path, the exit drain and the
/// shutdown accounting — is the code under test, instead of a copy of it.
async fn run_s3_receiver(
    args: &Args,
    playout: PlayoutConfig,
    runtime: S3RuntimeConfig,
    logger: Arc<Mutex<JsonlLogger>>,
) -> Result<()> {
    let (session, mut subscriber) = {
        let (webtransport, transport) = establish(&args).await.context("establish S3 session")?;
        session_handshake(&args, webtransport, transport)
            .await
            .context("S3 SETUP")?
    };
    let session_run = tokio::spawn(session.run());
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

    run_s3_control(
        args,
        playout,
        runtime,
        logger,
        namespace,
        SubscriberSeam(subscriber),
        session_run,
    )
    .await
}

/// The S3 receiver control loop, parametric only in the subscribe seam and in
/// the session task's output type. Production instantiates it with
/// `SubscriberSeam` and `session.run()`.
async fn run_s3_control<S, T>(
    args: &Args,
    playout: PlayoutConfig,
    runtime: S3RuntimeConfig,
    logger: Arc<Mutex<JsonlLogger>>,
    namespace: TrackNamespace,
    mut subscriber: S,
    mut session_run: tokio::task::JoinHandle<T>,
) -> Result<()>
where
    S: S3SubscribeSeam,
    T: std::fmt::Debug,
{
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

    let mut live: HashMap<(TrackRole, u64), S3LiveSubscription<S::Handle>> = HashMap::new();
    let mut retired_drains = Vec::new();
    // Registered §v8 barrier semantics make the old generation stale at apply,
    // but the wire UNSUBSCRIBE is deferred by the frozen 67ms PC delivery
    // timeout so in-flight pre-/at-barrier objects resolve through the normal
    // delivery/timeout contract instead of being orphaned by a relay
    // hard-stop (v11 single-frame unaccounted defect).
    let mut retiring: S3RetirementQueue<S3LiveSubscription<S::Handle>> =
        S3RetirementQueue::new(ms_to_us(
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
    // Whether the `session_run` arm of the select below has already CONSUMED
    // the join handle. A `JoinHandle` that returned `Ready` panics when it is
    // polled again ("polled after completion"), so an early session end (a
    // normal return, an error return, or a panicking session task) would kill
    // the run at the unconditional `session_run.await` in the exit path —
    // before drain pass 2, the scheduler flush and the shutdown row. The arm
    // is therefore disabled once it has fired, and the exit path only
    // aborts/joins a handle that was never consumed.
    let mut session_consumed = false;
    let mut controller_active_at_us: Option<u64> = None;
    let mut forced_misses_injected = false;

    // Registered measurement window end, `t0 + duration`. The receiver has no
    // wire copy of the sender's `measurement_start`; it recovers t0 from the
    // first measurement object it observes as `gen_ts_us - pts_us`, because
    // every S3 producer stamps `t_gen` after waking at slot `t0 + pts_us` on
    // the host monotonic clock the two namespaces share. The estimate is
    // therefore never earlier than t0 and late only by the sender's wake
    // latency (sub-millisecond); it is fixed at first observation and logged
    // so the window is reconstructible from the RX log alone. Without
    // `--duration-s` the window is unknown and the controller is ungated.
    let window_duration_us = args.duration_s.map(duration_us_exact).transpose()?;
    let mut window_end_us: Option<u64> = None;
    let mut switch_suppressed_after_end: u64 = 0;
    let mut switch_refused_after_run_end: u64 = 0;
    // Terminals observed on switch targets that never applied. Persisted the
    // moment they are seen and merged into `outcome_error` on the exit path
    // below, so nothing on the non-fatal refusal path can erase them.
    let mut switch_faults = S3SwitchTargetFaults::default();
    if window_duration_us.is_none() {
        logger
            .lock()
            .map_err(|_| anyhow::anyhow!("RX logger poisoned"))?
            .try_log_info(
                "\"event\":\"s3_window_end\",\"window_end_us\":null,\"reason\":\"no_duration_s\"",
            )?;
    }

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
            result = &mut session_run, if !session_consumed => {
                session_consumed = true;
                keep_first_cause(
                    &mut outcome_error,
                    anyhow::anyhow!("S3 session ended early: {result:?}"),
                );
                None
            }
            _ = tokio::time::sleep(wait) => None,
        };
        let now = now_us();
        // First-cause-wins from here to the end of the iteration: the session
        // arm above, the max-duration check and the retirement releases can
        // all fire in the SAME iteration, and the cause that ended the run
        // must not be replaced by a consequence of it. (The loop condition
        // guarantees `outcome_error` was `None` at the top of the iteration,
        // so this only orders causes WITHIN one iteration.)
        if now >= max_end_us {
            keep_first_cause(
                &mut outcome_error,
                anyhow::anyhow!("S3 receiver max-duration reached"),
            );
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
                keep_first_cause(&mut outcome_error, error);
            }
        }
        if outcome_error.is_some() {
            continue;
        }

        let mut scheduler_actions = Vec::new();
        if let Some(event) = event {
            match handle_s3_wire_event(
                event,
                now,
                window_duration_us,
                &mut window_end_us,
                &mut ingress,
                &mut scheduler,
                &mut tracker,
                &mut object_routes,
                &mut live,
                &mut retiring,
                &mut retired_drains,
                &mut current_finished,
                &mut normal_end,
                &mut scheduler_actions,
                &recv_pc,
                &recv_haptic,
                &logger,
                &mut stats,
                &mut switch_refused_after_run_end,
                &mut switch_faults,
                &mut outcome_error,
            ) {
                EventFlow::Continue => {}
                EventFlow::SkipIteration => continue,
            }
        }

        scheduler_actions.extend(scheduler.advance(now));
        // Same common-timeline epoch row as S1/M1/S2/S3R: S3 uses the same
        // scheduler, so its `t_due` is verifiable the same way.
        if let Err(error) = log_common_epochs(&mut scheduler, &logger, playout) {
            outcome_error = Some(error);
            continue;
        }
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
            &|action| scheduler.action_due_us(action),
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
            // First-cause-wins: the effect timeout of the SAME pending switch
            // can expire in the very iteration that recorded why its target
            // died, and the timeout is then a consequence of that cause, not
            // the cause. (With no earlier cause this is exactly as before —
            // the registered timeout is what ends a switch that simply never
            // took effect.)
            keep_first_cause(
                &mut outcome_error,
                anyhow::anyhow!("S3 switch timeout: {error:?}"),
            );
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
                        window_end_us,
                        &mut stats,
                        &mut switch_faults,
                    )
                    .await
                    {
                        Ok(outcome) => {
                            count_switch_outcome(
                                outcome,
                                &mut switch_suppressed_after_end,
                                &mut switch_refused_after_run_end,
                            );
                            if outcome == SwitchOutcome::RefusedAfterRunEnd {
                                // Defensive symmetry with the event-driven
                                // refusal path. No event is processed inside
                                // `request_s3_switch`, so on THIS path the
                                // current routes' FINs cannot already have
                                // been seen; the call is a no-op today and
                                // exists so the rule lives at every place a
                                // pending request is abandoned.
                                recompute_s3_normal_end(
                                    &ingress,
                                    &current_finished,
                                    &mut normal_end,
                                );
                            }
                        }
                        Err(error) => {
                            outcome_error = Some(error);
                            continue;
                        }
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
                    window_end_us,
                    &mut stats,
                    &mut switch_faults,
                )
                .await
                {
                    Ok(outcome) => {
                        count_switch_outcome(
                            outcome,
                            &mut switch_suppressed_after_end,
                            &mut switch_refused_after_run_end,
                        );
                        if outcome == SwitchOutcome::RefusedAfterRunEnd {
                            // See the forced-miss path above: defensive, and
                            // a no-op on this path.
                            recompute_s3_normal_end(
                                &ingress,
                                &current_finished,
                                &mut normal_end,
                            );
                        }
                        // A pending request suppresses the rest of this
                        // batch (as before). A suppressed or refused
                        // transition leaves nothing pending, so the
                        // controller keeps observing.
                        if outcome == SwitchOutcome::Requested {
                            break;
                        }
                    }
                    Err(error) => {
                        outcome_error = Some(error);
                        break;
                    }
                }
            }
        }
    }

    // Drain-at-exit, pass 1 — BEFORE `finish_pending`, on the normal end and
    // on every error exit alike. Anything still queued (typically the objects
    // of a target route whose sibling role was refused) is routed through the
    // ingress so it gets its terminal instead of vanishing with the channel.
    //
    // SCOPE, stated exactly: "every exit" means every exit OF THIS LOOP,
    // including the early session end, and every failure below is folded into
    // `outcome_error` instead of returning, so the shutdown row is always
    // written. It does NOT cover the process being killed: the receiver
    // installs no SIGINT/SIGTERM handler, so a signal ends the process
    // wherever it is, with no drain, no flush and no shutdown row. A run
    // terminated that way is incomplete by inspection of the log (no
    // `shutdown` row) and must be treated as such.
    let mut s3_events_drained_at_exit: u64 = 0;
    let pass_one = drain_s3_events_at_exit(
        &mut event_rx,
        window_duration_us,
        &mut window_end_us,
        &mut ingress,
        &mut scheduler,
        &mut tracker,
        &mut object_routes,
        &mut live,
        &mut retiring,
        &mut retired_drains,
        &mut current_finished,
        &mut normal_end,
        &recv_pc,
        &recv_haptic,
        &logger,
        &mut stats,
        &mut switch_refused_after_run_end,
        &mut switch_faults,
        &mut outcome_error,
    );
    s3_events_drained_at_exit += pass_one.drained;

    // First-cause-wins, and NEVER an early return: a failure here would
    // otherwise skip drain pass 2, the scheduler flush and the shutdown row,
    // which is exactly the property this section claims.
    if let Err(error) = flush_s3_barrier_drops(&mut ingress, &logger, &mut stats) {
        keep_first_cause(&mut outcome_error, error);
    }

    // Stop every subscription before joining/aborting its drain, then stop the
    // session. No detached reader is allowed to write after shutdown logging.
    // Retiring routes lose their remaining drain window at shutdown; this is
    // identical to the pre-existing treatment of live routes.
    //
    // Every join below is bounded by `S3_DRAIN_JOIN_BOUND`, exactly as the
    // switch teardown is. An unbounded await here was not "correct because
    // the task always stops": if one ever did not, the receiver would hang
    // with no log and no verdict instead of reporting it. A task that misses
    // the bound is counted, reported in the shutdown row and fails the run,
    // because it could still write a row after the shutdown row — which is
    // precisely the invariant this section exists to guarantee.
    let mut drains_unjoined: u64 = 0;
    // A join that COMPLETES can still carry a `JoinError`. Every task here was
    // aborted first, so `is_cancelled()` is the NORMAL outcome and is not a
    // fault; a PANIC is (16th rework, P1-C). The 15th rework recorded a panic
    // additively and left the verdict alone, arguing that it "shows up in the
    // object accounting". It does not: a drain that panics BETWEEN objects
    // leaves no unterminated rx object, drops its sender clone so the queue
    // still reports `Disconnected`, and with the current routes FIN'd every
    // other `failed` condition can be false. The row stays additive; the cause
    // is recorded through `keep_first_cause` so an EARLIER cause still wins.
    for (_role, _route, subscription) in retiring.drain_all() {
        let join = discard_s3_subscription(subscription).await;
        note_s3_drain_join(join, &logger, &mut drains_unjoined, &mut outcome_error);
    }
    for (_, subscription) in live.drain() {
        let join = discard_s3_subscription(subscription).await;
        note_s3_drain_join(join, &logger, &mut drains_unjoined, &mut outcome_error);
    }
    for drain in retired_drains.drain(..) {
        if !drain.is_finished() {
            drain.abort();
        }
        let join = join_s3_drain(drain).await;
        note_s3_drain_join(join, &logger, &mut drains_unjoined, &mut outcome_error);
    }
    // Only a handle the select never consumed may be polled here; re-polling
    // a completed one panics and would skip everything below, including the
    // shutdown row. The bound matches the drain joins: an aborted task stops
    // at its next await point, so it is a liveness guard, never a normal
    // outcome.
    if !session_consumed {
        session_run.abort();
        // The join RESULT is recorded, not discarded: a session task that has
        // not stopped within the bound is the one case where something other
        // than this function could still touch the transport after the
        // shutdown row. It is additive and does NOT change the verdict — the
        // session writes no JSONL row, so it cannot break the "exactly one
        // terminal per rx object" invariant that `drains_unjoined` guards.
        if tokio::time::timeout(S3_DRAIN_JOIN_BOUND, &mut session_run)
            .await
            .is_err()
        {
            if let Err(error) = log_s3_exit_note(
                &logger,
                &format!(
                    "\"event\":\"s3_session_join_timeout\",\"bound_ms\":{}",
                    S3_DRAIN_JOIN_BOUND.as_millis()
                ),
            ) {
                keep_first_cause(&mut outcome_error, error);
            }
        }
    }
    // Every drain task has now been joined — or, for the `drains_unjoined`
    // ones that missed the bound, is still alive and still holds an
    // `S3WireEvent` sender clone, which is why pass 2 below then reports
    // `Empty` instead of `Disconnected` and the run is failed either way.
    // Dropping the loop's own clone is what makes the channel observably
    // closed in the normal case.
    drop(event_tx);
    // Drain-at-exit, pass 2 — this is the pass that CLOSES the window. Pass 1
    // ran while the current/retiring routes were still readable, so an object
    // could still be enqueued behind it. Here no sender exists any more, so a
    // `Disconnected` verdict proves the queue is permanently empty and every
    // `rx` row a drain task ever wrote has been routed through the ingress.
    // Objects admitted to the scheduler here are still terminated by the
    // shutdown flush below, which has not run yet.
    let pass_two = drain_s3_events_at_exit(
        &mut event_rx,
        window_duration_us,
        &mut window_end_us,
        &mut ingress,
        &mut scheduler,
        &mut tracker,
        &mut object_routes,
        &mut live,
        &mut retiring,
        &mut retired_drains,
        &mut current_finished,
        &mut normal_end,
        &recv_pc,
        &recv_haptic,
        &logger,
        &mut stats,
        &mut switch_refused_after_run_end,
        &mut switch_faults,
        &mut outcome_error,
    );
    s3_events_drained_at_exit += pass_two.drained;
    let event_queue_closed = pass_two.disconnected;
    // `finish_pending` empties the exact-pair barrier but does NOT clear the
    // gate's pending request, so on an ERROR exit that still had a switch
    // pending, an object drained in pass 2 can be classified
    // `PendingBarrierOnly` and parked in the barrier again. Emptying it a
    // second time is the only way those objects reach a terminal. In every
    // other case this returns nothing.
    if let Err(error) = flush_s3_barrier_drops(&mut ingress, &logger, &mut stats) {
        keep_first_cause(&mut outcome_error, error);
    }
    // Promote a switch-target terminal fault observed by EITHER drain pass or
    // by the control loop. This is the merge point that makes the fault
    // outlive `abandon_pending` and the FIN rule: `normal_end` may well be
    // true here, but `failed` below is computed from `outcome_error`, so the
    // run ends in error with the terminal's own cause unless a strictly
    // earlier cause was already recorded.
    if let Some(cause) = switch_faults.take_cause() {
        keep_first_cause(&mut outcome_error, cause);
    }
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
            // Same rule as above: record and stop flushing, but still write
            // the shutdown row.
            if let Err(error) = dispatch_s3_playout_actions(
                actions,
                &mut object_routes,
                &mut tracker,
                &logger,
                None,
                false,
                false,
                &mut stats,
                now,
                &|action| scheduler.action_due_us(action),
            ) {
                keep_first_cause(&mut outcome_error, error);
                break;
            }
        }
    } else {
        let now = now_us();
        let actions = scheduler.finish_without_epoch();
        if let Err(error) = dispatch_s3_playout_actions(
            actions,
            &mut object_routes,
            &mut tracker,
            &logger,
            None,
            false,
            false,
            &mut stats,
            now,
            // No epoch was ever armed, so no object had a deadline.
            &|_| None,
        ) {
            keep_first_cause(&mut outcome_error, error);
        }
    }

    let n_pc = recv_pc.load(Ordering::Relaxed);
    let n_haptic = recv_haptic.load(Ordering::Relaxed);
    let n_bad = bad_headers.load(Ordering::Relaxed);
    let n_ingress_drop = ingress_drops.load(Ordering::Relaxed);
    // Final integrity, stated as one rule: EVERY object a drain task logged an
    // `rx` row for must end with exactly one release or drop terminal.
    //
    // A drain task writes the `rx` row and then either (a) enqueues the object
    // on `event_rx` or (b) fails `try_send` and writes its own
    // `ingress_queue_full` drop terminal, counted in `n_ingress_drop`. Path (b)
    // is already terminal. Path (a) is terminal only if the object was routed
    // through `handle_s3_wire_event`, which either drops it (barrier/stale/
    // duplicate row) or registers it in `object_routes` and hands it to the
    // scheduler, whose flush above emits release/drop and removes the entry.
    // So the two residue conditions together are exhaustive:
    //   * `object_routes` empty — nothing admitted to the scheduler is unclosed;
    //   * `event_queue_closed` — `try_recv` reported `Disconnected` after every
    //     drain task was joined and the loop's own sender was dropped, i.e. the
    //     queue is permanently empty, so nothing enqueued is unclosed either.
    // The second condition is what `object_routes` alone can never see: an
    // object that never left the channel was never registered anywhere.
    let failed = outcome_error.is_some()
        || n_bad > 0
        || n_ingress_drop > 0
        || log_failed.load(Ordering::Relaxed) > 0
        || !object_routes.is_empty()
        || !event_queue_closed
        || drains_unjoined > 0;
    {
        let mut logger = logger
            .lock()
            .map_err(|_| anyhow::anyhow!("RX logger poisoned"))?;
        logger.try_log_info(&format!(
            "\"recv_pc\":{n_pc},\"recv_haptic\":{n_haptic},\"bad_headers\":{n_bad},\"s1_released\":{},\"s1_dropped\":{},\"s1_ingress_dropped\":{n_ingress_drop},\"s1_bridge_observer_dropped\":{},\"s1_negative_lateness\":{}",
            stats.released,
            stats.dropped,
            stats.bridge_observer_dropped,
            stats.negative_lateness,
        ))?;
        logger.try_log_info(&s3_shutdown_row_body(
            failed,
            n_bad,
            &outcome_error
                .as_ref()
                .map(|error| format!("{error:#}"))
                .unwrap_or_else(|| "rule=s3_current_routes_fin".to_string()),
            switch_suppressed_after_end,
            switch_refused_after_run_end,
            window_end_us,
            s3_events_drained_at_exit,
            event_queue_closed,
            drains_unjoined,
        ))?;
        logger.try_flush()?;
    }
    if let Some(error) = outcome_error {
        return Err(error);
    }
    if failed {
        bail!(
            "S3 final integrity failure: bad_headers={n_bad} ingress_drops={n_ingress_drop} route_residue={} event_queue_closed={event_queue_closed} drains_unjoined={drains_unjoined}",
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
    let s3np_config = s3np_release_config(&args)?;
    let s3_runtime = s3_runtime_config(&args)?;
    let phase4_transport = phase4_transport(&args);
    let replay_meta = match (args.arm.replays_tier_schedule(), &args.tier_schedule) {
        (true, Some(path)) => {
            let document = std::fs::read_to_string(path)
                .with_context(|| format!("read tier schedule {}", path.display()))?;
            let schedule = skew_moq::s3np::TierSchedule::parse(&document)
                .map_err(|error| anyhow::anyhow!("invalid tier schedule: {error:?}"))?;
            // The receiver applies no tier policy, but it must refuse a schedule
            // that does not describe THIS run: the sender makes the same check,
            // and a one-sided check would let a mismatched pair start.
            if schedule.pc_rate_hz() != args.pc_rate_hz
                || schedule.haptic_rate_hz() != args.haptic_rate_hz
            {
                bail!(
                    "tier schedule was extracted from a {}/{} Hz run but this run is {}/{} Hz",
                    schedule.pc_rate_hz(),
                    schedule.haptic_rate_hz(),
                    args.pc_rate_hz,
                    args.haptic_rate_hz
                );
            }
            let duration_s = args.duration_s.with_context(|| {
                format!(
                    "--arm {} requires --duration-s so the replayed window can be checked \
                     against the schedule",
                    args.arm.as_str()
                )
            })?;
            let duration_us = duration_us_exact(duration_s)?;
            if schedule.duration_us() != duration_us {
                bail!(
                    "tier schedule covers {}us but this run is {}us; the replay must come \
                     from a source S3 run of the same registered duration",
                    schedule.duration_us(),
                    duration_us
                );
            }
            let sha256: [u8; 32] = {
                use sha2::{Digest, Sha256};
                Sha256::digest(document.as_bytes()).into()
            };
            // s3np records its own release parameters here; s3r's are already on
            // the S1 common-timeline block, and duplicating a JSON key would
            // make the meta row ambiguous, so it records only the rule name.
            let (release_rule_key, release_rule, d_play_us, release) = match &s3np_config {
                Some(config) => (
                    skew_moq::s3np::S3NP_RELEASE_RULE_KEY,
                    config.rule.as_str(),
                    Some(config.d_play_us),
                    Some(ReplayReleaseMeta {
                        playout_clock: config.rule.playout_clock(),
                        late_tolerance_us: config.late_tolerance_us,
                        late_policy: config.late_policy.as_str(),
                        max_objects_per_track: config.max_objects_per_track,
                        max_span_us: config.max_span_us,
                    }),
                ),
                None => (
                    skew_moq::s3np::S3R_RELEASE_RULE_KEY,
                    skew_moq::s3np::S3R_RELEASE_RULE,
                    None,
                    None,
                ),
            };
            Some(TierReplayMeta {
                tier_schedule_sha256: sha256,
                tier_schedule_generation: schedule.generation().to_string(),
                tier_schedule_source_run_id: schedule.source_run_id().to_string(),
                tier_schedule_source_tx_sha256: schedule.source_tx_sha256().to_string(),
                tier_schedule_source_rx_sha256: schedule.source_rx_sha256().to_string(),
                tier_schedule_switches: schedule.switches().len(),
                release_rule_key,
                release_rule,
                d_play_us,
                release,
            })
        }
        (true, None) => bail!(
            "--arm {} requires --tier-schedule",
            args.arm.as_str()
        ),
        (false, Some(_)) => bail!("--tier-schedule requires --arm s3np or --arm s3r"),
        (false, None) => None,
    };

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
            replay: replay_meta,
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
            // s3np keeps S2's PC DELIVERY_TIMEOUT: the control differs from S3
            // by event-pair preservation, not by transport policy.
            if name == "pc" && matches!(args.arm, Arm::S2 | Arm::S2Eq | Arm::S3np | Arm::S3r) {
                params.set_delivery_timeout(
                    args.pc_delivery_timeout_ms
                        .expect("S2/s3np timeout validated before connecting"),
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
    // Exactly one scheduler task exists: the common-timeline one for
    // S1/M1/S2/S2Eq, or the per-track pairing-free one for s3np. B1 creates
    // neither and keeps its original direct path. `scheduler_budget_us` is the
    // bounded shutdown drain, derived from whichever parameter set applies.
    let (s1_tx, mut s1_task, scheduler_budget_us) = if let Some(config) = s1_config {
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
        let budget_us = config
            .d_play_us
            .saturating_add(config.max_span_us)
            .saturating_add(config.late_tolerance_us)
            .saturating_add(250_000);
        (Some(tx), Some(task), Some(budget_us))
    } else if let Some(config) = s3np_config {
        let capacity = config
            .max_objects_per_track
            .checked_mul(2)
            .context("s3np ingress capacity overflow")?;
        let (tx, rx) = mpsc::channel::<PlayoutObject>(capacity);
        let task = tokio::spawn(run_s3np_release_scheduler(
            config,
            rx,
            logger.clone(),
            ftx.clone(),
            args.render,
            args.audio,
        ));
        println!(
            "[rx] s3np per-track release: rule={} D_play={}ms late={}ms policy={} objects/track={} span={}ms",
            config.rule.as_str(),
            config.d_play_us / 1_000,
            config.late_tolerance_us / 1_000,
            config.late_policy.as_str(),
            config.max_objects_per_track,
            config.max_span_us / 1_000,
        );
        let budget_us = config
            .d_play_us
            .saturating_add(config.max_span_us)
            .saturating_add(config.late_tolerance_us)
            .saturating_add(250_000);
        (Some(tx), Some(task), Some(budget_us))
    } else {
        (None, None, None)
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
                                    // Never admitted to the scheduler, so no
                                    // deadline was ever evaluated for it.
                                    None,
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
                                    None,
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
        let budget_us = scheduler_budget_us.expect("a scheduler task has a shutdown budget");
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
                matches!(args.arm, Arm::S2 | Arm::S2Eq | Arm::S3np | Arm::S3r)
                    && args.pc_delivery_timeout_ms.is_some(),
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
                "\"recv_pc\":{n_pc},\"recv_haptic\":{n_hap},\"bad_headers\":{n_bad},\"pc_chunks_received\":{},\"haptic_chunks_received\":{},\"frames_completed\":{},\"incomplete_frames\":{},\"duplicate_chunks\":{},\"invalid_chunks\":{},\"reassembly_peak_frames\":{},\"reassembly_peak_bytes\":{},\"s1_released\":{},\"s1_dropped\":{},\"s1_ingress_dropped\":{},\"s1_bridge_observer_dropped\":{},\"s1_negative_lateness\":{}",
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
                s1_stats.negative_lateness,
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
            s3np_release_rule: None,
            tier_schedule: None,
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
            s3np_release_rule: None,
            tier_schedule: None,
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
            s3np_release_rule: None,
            tier_schedule: None,
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

        // ---- s3np: the event-pair NON-preserving control ------------------
        //
        // Same transport policy as S2, a different RELEASE rule, and the two
        // pairing-only scheduler options refused rather than recorded.
        let mut np = args;
        np.arm = Arm::S3np;
        np.pc_delivery_timeout_ms = Some(67);
        np.d_play_ms = Some(50);
        np.late_tolerance_ms = Some(5);
        np.buffer_max_objects_per_track = Some(64);
        np.buffer_max_span_ms = Some(250);
        np.late_policy = Some(CliLatePolicy::DropLate);
        np.startup_timeout_ms = None;
        np.startup_rearm_limit = None;
        // Fail closed: the rule must be stated. Nothing about the two rules is
        // interchangeable, so an omitted flag is an error, not a default.
        assert!(s3np_release_config(&np)
            .unwrap_err()
            .to_string()
            .contains("--s3np-release-rule"));
        np.s3np_release_rule = Some(CliReleaseRule::PerTrackEpoch);
        // Every S3 controller option is refused: s3np runs no FSM.
        assert!(
            s3_runtime_config(&np).is_err(),
            "s3np must refuse S3 controller options"
        );
        let s3_only = [
            "s3_window_ms",
            "s3_ewma_alpha",
            "s3_miss_streak_threshold",
            "s3_violation_ratio_threshold",
            "s3_target_skew_ms",
            "s3_recovery_fraction",
            "s3_haptic_critical_stable_ms",
            "s3_recovery_stable_ms",
            "s3_cooldown_ms",
            "s3_min_paired_samples",
            "s3_max_window_samples",
            "s3_deadline_max_anchors",
            "s3_effect_timeout_ms",
            "s3_initial_retry_limit",
            "s3_switch_retry_limit",
        ];
        assert_eq!(s3_only.len(), 15, "every S3 option stays s3-exclusive");
        np.s3_window_ms = None;
        np.s3_ewma_alpha = None;
        np.s3_miss_streak_threshold = None;
        np.s3_violation_ratio_threshold = None;
        np.s3_target_skew_ms = None;
        np.s3_recovery_fraction = None;
        np.s3_haptic_critical_stable_ms = None;
        np.s3_recovery_stable_ms = None;
        np.s3_cooldown_ms = None;
        np.s3_min_paired_samples = None;
        np.s3_max_window_samples = None;
        np.s3_deadline_max_anchors = None;
        np.s3_effect_timeout_ms = None;
        np.s3_initial_retry_limit = None;
        np.s3_switch_retry_limit = None;
        assert!(s3_runtime_config(&np).unwrap().is_none());

        assert!(validate_phase4_v5_args(&np).is_ok());
        let meta = phase4_transport(&np).unwrap();
        assert_eq!(meta.arm, "s3np");
        assert_eq!(meta.pc_subgroup_mapping, "frame-per-subgroup");
        assert_eq!(meta.pc_publisher_priority, 1);
        assert_eq!(meta.haptic_publisher_priority, 0);
        assert_eq!(meta.publisher_priority_profile, "relative-haptic0-pc1");
        assert_eq!(meta.pc_delivery_timeout_ms, Some(67));

        // No common-timeline config is built, so no startup window can be
        // recorded as if it had been applied.
        assert!(playout_config(&np).unwrap().is_none());
        let release = s3np_release_config(&np).unwrap().unwrap();
        assert_eq!(release.rule, skew_moq::s3np::ReleaseRule::PerTrackEpoch);
        assert_eq!(
            release.rule.as_str(),
            "per_track_first_object_epoch_plus_d_play"
        );
        assert_eq!(release.rule.playout_clock(), "receiver_monotonic_us");
        assert_eq!(release.d_play_us, 50_000);
        assert_eq!(release.late_tolerance_us, 5_000);
        assert_eq!(release.max_objects_per_track, 64);
        assert_eq!(release.max_span_us, 250_000);

        // The sensitivity variant is selectable and records a DIFFERENT rule
        // name and clock, so no analysis can conflate the two.
        np.s3np_release_rule = Some(CliReleaseRule::AbsoluteTGen);
        let variant = s3np_release_config(&np).unwrap().unwrap();
        assert_eq!(variant.rule, skew_moq::s3np::ReleaseRule::AbsoluteTGen);
        assert_eq!(variant.rule.as_str(), "absolute_t_gen_plus_d_play");
        assert_eq!(variant.rule.playout_clock(), "sender_t_gen_monotonic_us");
        assert_ne!(variant.rule.as_str(), release.rule.as_str());
        np.s3np_release_rule = Some(CliReleaseRule::PerTrackEpoch);

        // Each refusal is checked in place and then undone: `Args` is not
        // `Clone`, and a per-case fixture copy would be the only reason to make
        // it so.
        let cases: [(&str, fn(&mut Args), fn(&mut Args)); 11] = [
            (
                "startup window",
                |a| a.startup_timeout_ms = Some(2_000),
                |a| a.startup_timeout_ms = None,
            ),
            (
                "startup rearm",
                |a| a.startup_rearm_limit = Some(1),
                |a| a.startup_rearm_limit = None,
            ),
            (
                "non-candidate D_play",
                |a| a.d_play_ms = Some(75),
                |a| a.d_play_ms = Some(50),
            ),
            (
                "missing D_play",
                |a| a.d_play_ms = None,
                |a| a.d_play_ms = Some(50),
            ),
            (
                "missing late tolerance",
                |a| a.late_tolerance_ms = None,
                |a| a.late_tolerance_ms = Some(5),
            ),
            (
                "missing late policy",
                |a| a.late_policy = None,
                |a| a.late_policy = Some(CliLatePolicy::DropLate),
            ),
            (
                "missing buffer object bound",
                |a| a.buffer_max_objects_per_track = None,
                |a| a.buffer_max_objects_per_track = Some(64),
            ),
            (
                "missing buffer span bound",
                |a| a.buffer_max_span_ms = None,
                |a| a.buffer_max_span_ms = Some(250),
            ),
            (
                "single track",
                |a| a.tracks = RxTrackSel::Pc,
                |a| a.tracks = RxTrackSel::Both,
            ),
            (
                "shared FIFO",
                |a| a.queue_policy = QueuePolicy::SharedFifo,
                |a| a.queue_policy = QueuePolicy::Separate,
            ),
            (
                "missing release rule",
                |a| a.s3np_release_rule = None,
                |a| a.s3np_release_rule = Some(CliReleaseRule::PerTrackEpoch),
            ),
        ];
        for (label, break_it, restore) in cases {
            break_it(&mut np);
            assert!(
                s3np_release_config(&np).is_err(),
                "s3np must refuse {label}"
            );
            restore(&mut np);
            assert!(s3np_release_config(&np).is_ok(), "restore failed: {label}");
        }

        // The control inherits S3's frozen PC delivery timeout; a different
        // value would silently turn the comparison into a timeout ablation.
        np.pc_delivery_timeout_ms = Some(100);
        assert!(playout_config(&np)
            .unwrap_err()
            .to_string()
            .contains("67ms"));
        np.pc_delivery_timeout_ms = Some(67);

        // ---- s3r: S2 receiver + replayed tier trajectory, pairs PRESERVED --
        //
        // The point of s3r is that NOTHING about the receiver changes: it builds
        // the unchanged S1/S2 common-timeline config (exact-pair epoch, startup
        // window, S2 late policy and buffer bounds) and no pairing-free config.
        let mut sr = np;
        sr.arm = Arm::S3r;
        sr.s3np_release_rule = None;
        sr.startup_timeout_ms = Some(2_000);
        sr.startup_rearm_limit = Some(1);
        sr.pc_delivery_timeout_ms = Some(67);
        sr.duration_s = Some(60.0);

        assert!(validate_phase4_v5_args(&sr).is_ok());
        let meta = phase4_transport(&sr).unwrap();
        assert_eq!(meta.arm, "s3r");
        assert_eq!(meta.pc_subgroup_mapping, "frame-per-subgroup");
        assert_eq!((meta.pc_publisher_priority, meta.haptic_publisher_priority), (1, 0));
        assert_eq!(meta.publisher_priority_profile, "relative-haptic0-pc1");
        assert_eq!(meta.pc_delivery_timeout_ms, Some(67));

        // The pairing scheduler, with S2's parameters and nothing new.
        let common = playout_config(&sr).unwrap().expect("s3r uses the S1 scheduler");
        assert_eq!(common.d_play_us, 50_000);
        assert_eq!(common.startup_timeout_us, 2_000_000);
        assert_eq!(common.startup_rearm_limit, 1);
        assert_eq!(common.late_tolerance_us, 5_000);
        assert_eq!(common.max_objects_per_track, 64);
        assert_eq!(common.max_span_us, 250_000);
        assert!(
            s3np_release_config(&sr).unwrap().is_none(),
            "s3r must not build the pairing-free release config"
        );
        // An S2 fixture with the same flags yields the SAME scheduler config:
        // s3r changes the sender's stream, not the receiver's release policy.
        let mut s2_like = sr;
        s2_like.arm = Arm::S2;
        assert_eq!(playout_config(&s2_like).unwrap().unwrap(), common);
        s2_like.arm = Arm::S3r;
        let mut sr = s2_like;

        // s3r inherits the frozen timeout and still needs the pairing options.
        sr.pc_delivery_timeout_ms = Some(100);
        assert!(playout_config(&sr).unwrap_err().to_string().contains("67ms"));
        sr.pc_delivery_timeout_ms = Some(67);
        sr.startup_timeout_ms = None;
        assert!(playout_config(&sr)
            .unwrap_err()
            .to_string()
            .contains("--startup-timeout-ms"));
        sr.startup_timeout_ms = Some(2_000);
        sr.tracks = RxTrackSel::Pc;
        assert!(playout_config(&sr)
            .unwrap_err()
            .to_string()
            .contains("--tracks both"));
        sr.tracks = RxTrackSel::Both;
        sr.queue_policy = QueuePolicy::SharedFifo;
        assert!(playout_config(&sr).is_err());
        sr.queue_policy = QueuePolicy::Separate;
        // The s3np rule selector is refused: s3r has exactly one release rule.
        sr.s3np_release_rule = Some(CliReleaseRule::AbsoluteTGen);
        assert!(s3np_release_config(&sr)
            .unwrap_err()
            .to_string()
            .contains("requires --arm s3np"));
        sr.s3np_release_rule = None;
        // No S3 controller options either.
        sr.s3_window_ms = Some(1_000);
        assert!(s3_runtime_config(&sr).is_err());
        sr.s3_window_ms = None;
        assert!(s3_runtime_config(&sr).unwrap().is_none());
        assert!(playout_config(&sr).is_ok());

        let mut np = sr;
        np.arm = Arm::S3np;
        np.startup_timeout_ms = None;
        np.startup_rearm_limit = None;
        np.s3np_release_rule = Some(CliReleaseRule::PerTrackEpoch);
        assert!(playout_config(&np).unwrap().is_none());
        assert!(s3np_release_config(&np).unwrap().is_some());

        // And no other arm builds the s3np release config — nor may it claim
        // the release rule.
        np.arm = Arm::S2;
        np.startup_timeout_ms = Some(2_000);
        np.startup_rearm_limit = Some(1);
        assert!(s3np_release_config(&np)
            .unwrap_err()
            .to_string()
            .contains("requires --arm s3np"));
        np.s3np_release_rule = None;
        assert!(s3np_release_config(&np).unwrap().is_none());
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

    pub(crate) fn test_log_path(name: &str) -> PathBuf {
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
                    replay: None,
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
            Arc::new(AtomicBool::new(false)),
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

#[cfg(test)]
mod s3_switch_subscribe_tests {
    use super::*;

    fn pc_route() -> Route {
        Route {
            name: skew_moq::s3_switch::PC_RECOVERY_TRACK,
            generation: 1,
        }
    }

    fn haptic_route() -> Route {
        Route {
            name: skew_moq::s3_switch::HAPTIC_ESSENTIAL_TRACK,
            generation: 1,
        }
    }

    fn ready(
        outcome: SwitchSubscribeOutcome<&'static str>,
    ) -> Vec<(TrackRole, Route, &'static str, u64, u32)> {
        match outcome {
            SwitchSubscribeOutcome::Ready(ready) => ready,
            SwitchSubscribeOutcome::Failed { error, .. } => panic!("unexpected failure: {error}"),
        }
    }

    #[test]
    fn both_ok_are_ordered_by_t_ok_whichever_role_answered_first() {
        // Haptic SUBSCRIBE_OK arrived first: it must be fed to the gate first.
        let out = ready(settle_switch_subscribes(vec![
            (TrackRole::Pc, pc_route(), Ok(("pc", 5_000, 0))),
            (TrackRole::Haptic, haptic_route(), Ok(("haptic", 4_000, 1))),
        ]));
        assert_eq!(
            out.iter().map(|(role, _, s, t, r)| (*role, *s, *t, *r)).collect::<Vec<_>>(),
            vec![
                (TrackRole::Haptic, "haptic", 4_000, 1),
                (TrackRole::Pc, "pc", 5_000, 0)
            ]
        );
        // PC first when it answered first.
        let out = ready(settle_switch_subscribes(vec![
            (TrackRole::Pc, pc_route(), Ok(("pc", 4_000, 0))),
            (TrackRole::Haptic, haptic_route(), Ok(("haptic", 5_000, 0))),
        ]));
        assert_eq!(out[0].0, TrackRole::Pc);
        assert_eq!(out[1].0, TrackRole::Haptic);
        // Tie: PC first (deterministic), timestamps untouched.
        let out = ready(settle_switch_subscribes(vec![
            (TrackRole::Haptic, haptic_route(), Ok(("haptic", 4_000, 0))),
            (TrackRole::Pc, pc_route(), Ok(("pc", 4_000, 0))),
        ]));
        assert_eq!(out[0].0, TrackRole::Pc);
        assert_eq!(out[1].0, TrackRole::Haptic);
        assert_eq!((out[0].3, out[1].3), (4_000, 4_000));
    }

    #[test]
    fn single_changed_role_is_passed_through() {
        let out = ready(settle_switch_subscribes(vec![(
            TrackRole::Haptic,
            haptic_route(),
            Ok(("haptic", 4_000, 2)),
        )]));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, TrackRole::Haptic);
        assert_eq!(out[0].4, 2);
        let out = ready(settle_switch_subscribes::<&'static str>(Vec::new()));
        assert!(out.is_empty());
    }

    #[test]
    fn one_failure_returns_the_other_subscription_for_cleanup() {
        match settle_switch_subscribes(vec![
            (TrackRole::Pc, pc_route(), Ok(("pc", 5_000, 0))),
            (TrackRole::Haptic, haptic_route(), Err(anyhow::anyhow!("haptic failed"))),
        ]) {
            SwitchSubscribeOutcome::Failed {
                error,
                more_errors,
                cleanup,
            } => {
                assert_eq!(error.to_string(), "haptic failed");
                assert!(more_errors.is_empty());
                assert_eq!(cleanup, vec!["pc"]);
            }
            SwitchSubscribeOutcome::Ready(_) => panic!("must fail"),
        }
        match settle_switch_subscribes(vec![
            (TrackRole::Pc, pc_route(), Err(anyhow::anyhow!("pc failed"))),
            (TrackRole::Haptic, haptic_route(), Ok(("haptic", 4_000, 0))),
        ]) {
            SwitchSubscribeOutcome::Failed {
                error,
                more_errors,
                cleanup,
            } => {
                assert_eq!(error.to_string(), "pc failed");
                assert!(more_errors.is_empty());
                assert_eq!(cleanup, vec!["haptic"]);
            }
            SwitchSubscribeOutcome::Ready(_) => panic!("must fail"),
        }
    }

    fn entry(role: TrackRole, route: Route, s: &'static str) -> (TrackRole, Route, &'static str, u64, u32) {
        (role, route, s, 4_000, 0)
    }

    #[test]
    fn apply_state_returns_the_full_teardown_set_at_every_step() {
        // Failure before the first insertion: current + remaining pending.
        let mut apply = SwitchApplyState::new(vec![
            entry(TrackRole::Pc, pc_route(), "pc"),
            entry(TrackRole::Haptic, haptic_route(), "haptic"),
        ]);
        let (_, _, first, _, _) = apply.take_next().unwrap();
        let teardown = apply.fail(Some(first));
        assert_eq!(teardown.subscriptions, vec!["pc", "haptic"]);
        assert!(teardown.live_keys.is_empty());

        // Failure after the first insertion (e.g. subscribe_ok / log write):
        // the inserted key is removed and the second is torn down.
        let mut apply = SwitchApplyState::new(vec![
            entry(TrackRole::Pc, pc_route(), "pc"),
            entry(TrackRole::Haptic, haptic_route(), "haptic"),
        ]);
        let (role, route, _, _, _) = apply.take_next().unwrap();
        apply.inserted((role, route.generation));
        let teardown = apply.fail(None);
        assert_eq!(teardown.subscriptions, vec!["haptic"]);
        assert_eq!(teardown.live_keys, vec![(TrackRole::Pc, 1)]);

        // Failure at the second entry's checks: first inserted, second current.
        let mut apply = SwitchApplyState::new(vec![
            entry(TrackRole::Pc, pc_route(), "pc"),
            entry(TrackRole::Haptic, haptic_route(), "haptic"),
        ]);
        let (role, route, _, _, _) = apply.take_next().unwrap();
        apply.inserted((role, route.generation));
        let (_, _, second, _, _) = apply.take_next().unwrap();
        let teardown = apply.fail(Some(second));
        assert_eq!(teardown.subscriptions, vec!["haptic"]);
        assert_eq!(teardown.live_keys, vec![(TrackRole::Pc, 1)]);

        // Failure after both inserted: nothing pending, both keys removed.
        let mut apply = SwitchApplyState::new(vec![
            entry(TrackRole::Pc, pc_route(), "pc"),
            entry(TrackRole::Haptic, haptic_route(), "haptic"),
        ]);
        while let Some((role, route, _, _, _)) = apply.take_next() {
            apply.inserted((role, route.generation));
        }
        let teardown = apply.fail(None);
        assert!(teardown.subscriptions.is_empty());
        assert_eq!(
            teardown.live_keys,
            vec![(TrackRole::Pc, 1), (TrackRole::Haptic, 1)]
        );

        // Single-role switch, failure before insertion.
        let mut apply = SwitchApplyState::new(vec![entry(TrackRole::Haptic, haptic_route(), "haptic")]);
        let (_, _, only, _, _) = apply.take_next().unwrap();
        let teardown = apply.fail(Some(only));
        assert_eq!(teardown.subscriptions, vec!["haptic"]);
        assert!(teardown.live_keys.is_empty());
    }

    #[test]
    fn both_failures_report_pc_first_and_nothing_to_clean() {
        match settle_switch_subscribes::<&'static str>(vec![
            (TrackRole::Pc, pc_route(), Err(anyhow::anyhow!("pc failed"))),
            (TrackRole::Haptic, haptic_route(), Err(anyhow::anyhow!("haptic failed"))),
        ]) {
            SwitchSubscribeOutcome::Failed {
                error,
                more_errors,
                cleanup,
            } => {
                assert_eq!(error.to_string(), "pc failed");
                // The second failure is kept, in PC-then-haptic order, so a
                // refusal classification can inspect every failed role.
                assert_eq!(more_errors.len(), 1);
                assert_eq!(more_errors[0].to_string(), "haptic failed");
                assert!(cleanup.is_empty());
            }
            SwitchSubscribeOutcome::Ready(_) => panic!("must fail"),
        }
    }

    // ---- window-end gating and run-ended refusal (helper-level) ----
    //
    // These cover the two pure decisions `request_s3_switch` makes:
    // `switch_allowed_at` (requirement A) and `classify_switch_open_failure`
    // (requirement B). The end-to-end behaviour of both, driven through the
    // REAL `run_s3_receiver` control loop, is in `s3_exit_drain_tests`.

    use moq_transport::message::RequestErrorCode;
    use moq_transport::serve::ServeError;

    const WINDOW_END: u64 = 30_000_000;

    fn subscribe_failure_at(
        role: TrackRole,
        route: Route,
        error: ServeError,
        t_failed_us: u64,
    ) -> anyhow::Error {
        anyhow::Error::new(S3SubscribeFailure {
            role,
            route,
            retries: 2,
            error,
            t_failed_us,
        })
    }

    fn subscribe_failure(role: TrackRole, route: Route, error: ServeError) -> anyhow::Error {
        subscribe_failure_at(role, route, error, WINDOW_END + 50_000)
    }

    fn does_not_exist() -> ServeError {
        ServeError::Closed(u64::from(RequestErrorCode::DoesNotExist))
    }

    /// Exactly what BOTH sender sites put on the wire: built by the sender's
    /// own constructor (`current_routes_completed()` refusal and
    /// "namespace is draining after run end" refusal call it), so a drift
    /// there fails these cases instead of silently making the receiver's
    /// non-fatal path unreachable.
    fn run_ended() -> ServeError {
        skew_moq::s3_sender::run_ended_refusal("test: sender run ended")
    }

    #[test]
    fn subscribe_failure_display_matches_the_previous_bail_text() {
        // The shutdown `detail` of every still-fatal failure must not change.
        let error = subscribe_failure(TrackRole::Pc, pc_route(), does_not_exist());
        assert_eq!(
            format!("{error:#}"),
            "S3 subscribe pc-d7 generation 1 failed after 2 retries: closed, code=16"
        );
        assert_eq!(error.to_string(), format!("{error:#}"));
        // 13th rework: 0x10 alone is NOT the completion signal. The sender also
        // answers 0x10 for an unsupported track name, an invalid route and a
        // draining namespace, and moq-transport answers it for "track not
        // found"; every one of those must stay fatal.
        let failure = error.downcast_ref::<S3SubscribeFailure>().unwrap();
        assert!(!failure.is_run_ended_refusal());
        assert_eq!(u64::from(RequestErrorCode::DoesNotExist), 0x10);

        // Only the registered run-ended code is the signal.
        let completed = subscribe_failure(TrackRole::Pc, pc_route(), run_ended());
        assert!(completed
            .downcast_ref::<S3SubscribeFailure>()
            .unwrap()
            .is_run_ended_refusal());
        for other in [
            ServeError::Closed(u64::from(RequestErrorCode::Timeout)),
            ServeError::Closed(u64::from(RequestErrorCode::InternalError)),
            ServeError::NotFound,
            ServeError::NotFoundWithId("x".to_string(), uuid::Uuid::nil()),
            ServeError::Cancel,
        ] {
            assert!(
                !S3SubscribeFailure {
                    role: TrackRole::Pc,
                    route: pc_route(),
                    retries: 0,
                    error: other.clone(),
                    t_failed_us: 0,
                }
                .is_run_ended_refusal(),
                "{other:?} must not be read as the end of the sender's run"
            );
        }
    }

    #[test]
    fn the_completion_code_is_final_and_never_retried() {
        // Retrying a run-ended refusal only burns the switch's effect budget.
        assert!(!is_retryable_subscribe_error(&run_ended()));
        // Every other retry decision is unchanged.
        assert!(is_retryable_subscribe_error(&ServeError::NotFound));
        assert!(is_retryable_subscribe_error(&ServeError::Closed(
            REQUEST_ERROR_DOES_NOT_EXIST
        )));
        assert!(!is_retryable_subscribe_error(&ServeError::Closed(0x4)));
        assert!(!is_retryable_subscribe_error(&ServeError::Cancel));
    }

    #[test]
    fn switch_allowed_only_before_a_known_window_end() {
        // (2) before the window end: requested as before.
        assert!(switch_allowed_at(WINDOW_END - 1, Some(WINDOW_END)));
        assert!(switch_allowed_at(0, Some(WINDOW_END)));
        // (1) at or after the window end: never requested. The compared value
        // is the REQUEST instant, so a decision taken before the end whose
        // request would be issued after it is suppressed too.
        assert!(!switch_allowed_at(WINDOW_END, Some(WINDOW_END)));
        assert!(!switch_allowed_at(WINDOW_END + 24_100, Some(WINDOW_END)));
        // Unknown window end (no --duration-s): ungated, as before.
        assert!(switch_allowed_at(WINDOW_END + 24_100, None));
    }

    #[test]
    fn only_the_completion_code_on_every_failed_role_is_a_refusal() {
        // (3) both roles refused with the run-ended code: both rows, each
        // keeping its OWN observation instant.
        let pc = subscribe_failure_at(TrackRole::Pc, pc_route(), run_ended(), 29_969_000);
        let haptic = subscribe_failure_at(
            TrackRole::Haptic,
            haptic_route(),
            run_ended(),
            29_992_000,
        );
        match classify_switch_open_failure([&pc, &haptic]) {
            SwitchOpenFailure::RefusedAfterRunEnd(refused) => {
                assert_eq!(
                    refused,
                    vec![
                        (
                            TrackRole::Pc,
                            pc_route(),
                            S3_RUN_ENDED_REQUEST_ERROR_CODE,
                            29_969_000
                        ),
                        (
                            TrackRole::Haptic,
                            haptic_route(),
                            S3_RUN_ENDED_REQUEST_ERROR_CODE,
                            29_992_000
                        ),
                    ]
                );
            }
            SwitchOpenFailure::Fatal => panic!("must be a refusal"),
        }
        // (f) The sender finishes at its LAST SLOT (measured: PC ~31 ms and
        // haptic ~8 ms before the window end), so a run-ended refusal
        // observed BEFORE the end is still a refusal. Time is not part of the
        // rule any more, at any instant.
        for t_failed in [0, 1, WINDOW_END - 1, WINDOW_END, u64::MAX] {
            let early =
                subscribe_failure_at(TrackRole::Haptic, haptic_route(), run_ended(), t_failed);
            assert!(matches!(
                classify_switch_open_failure([&early]),
                SwitchOpenFailure::RefusedAfterRunEnd(refused)
                    if refused == vec![(
                        TrackRole::Haptic,
                        haptic_route(),
                        S3_RUN_ENDED_REQUEST_ERROR_CODE,
                        t_failed
                    )]
            ));
        }
    }

    /// The sender has TWO run-ended refusal sites and the second one — a
    /// SUBSCRIBE that arrived while the namespace was draining after the run
    /// end — is genuinely reachable: under the registered RTT/jitter the
    /// receiver can legitimately issue a switch SUBSCRIBE just before its
    /// window end and have it land after the sender's stop. Before the 13th
    /// rework follow-up that site answered `DoesNotExist` (0x10) and aborted
    /// the receiver's run.
    ///
    /// Both errors are built by the sender's own constructor here, so this
    /// asserts the real wire values, not a restatement of them.
    #[test]
    fn a_drain_window_refusal_is_classified_exactly_like_a_finished_run_refusal() {
        let drain = skew_moq::s3_sender::run_ended_refusal("S3 namespace is draining after run end");
        assert_eq!(drain, run_ended(), "both sender sites must be identical");
        assert_eq!(drain.code(), S3_RUN_ENDED_REQUEST_ERROR_CODE);
        assert_ne!(
            drain.code(),
            u64::from(RequestErrorCode::DoesNotExist),
            "the drain window must no longer be indistinguishable from a fault"
        );
        // Non-retryable: the run is over, polling cannot change that.
        assert!(!is_retryable_subscribe_error(&drain));
        // Non-fatal on both roles, at any observation instant.
        let pc = subscribe_failure_at(TrackRole::Pc, pc_route(), drain.clone(), 1);
        let haptic = subscribe_failure_at(TrackRole::Haptic, haptic_route(), drain, u64::MAX);
        assert!(pc
            .downcast_ref::<S3SubscribeFailure>()
            .unwrap()
            .is_run_ended_refusal());
        assert!(matches!(
            classify_switch_open_failure([&pc, &haptic]),
            SwitchOpenFailure::RefusedAfterRunEnd(refused) if refused.len() == 2
        ));
        // Mixing the two sender sites is still one refusal, not a fault.
        let finished = subscribe_failure_at(TrackRole::Haptic, haptic_route(), run_ended(), 7);
        assert!(matches!(
            classify_switch_open_failure([&pc, &finished]),
            SwitchOpenFailure::RefusedAfterRunEnd(refused) if refused.len() == 2
        ));
    }

    #[test]
    fn any_other_failure_shape_stays_fatal() {
        let completed = subscribe_failure(TrackRole::Pc, pc_route(), run_ended());
        // (e) A plain DoesNotExist (0x10) is NOT the completion signal: it is
        // also what an unsupported track, an invalid route, a draining
        // namespace and moq-transport's "track not found" produce.
        let does_not_exist = subscribe_failure(TrackRole::Pc, pc_route(), does_not_exist());
        assert!(matches!(
            classify_switch_open_failure([&does_not_exist]),
            SwitchOpenFailure::Fatal
        ));
        // A different request error code: fatal.
        let timeout = subscribe_failure(
            TrackRole::Pc,
            pc_route(),
            ServeError::Closed(u64::from(RequestErrorCode::Timeout)),
        );
        assert!(matches!(
            classify_switch_open_failure([&timeout]),
            SwitchOpenFailure::Fatal
        ));
        // Local NotFound after retries exhausted is not the wire refusal.
        let local = subscribe_failure(TrackRole::Pc, pc_route(), ServeError::NotFound);
        assert!(matches!(
            classify_switch_open_failure([&local]),
            SwitchOpenFailure::Fatal
        ));
        // An untyped failure (e.g. the effect-timeout message).
        let untyped =
            anyhow::anyhow!("S3 switch pc SUBSCRIBE did not complete before the effect timeout");
        assert!(matches!(
            classify_switch_open_failure([&untyped]),
            SwitchOpenFailure::Fatal
        ));
        // Mixed: one refused with the run-ended code, the other something
        // else — no partial refusal, in either order.
        assert!(matches!(
            classify_switch_open_failure([&completed, &timeout]),
            SwitchOpenFailure::Fatal
        ));
        assert!(matches!(
            classify_switch_open_failure([&timeout, &completed]),
            SwitchOpenFailure::Fatal
        ));
        assert!(matches!(
            classify_switch_open_failure([&completed, &does_not_exist]),
            SwitchOpenFailure::Fatal
        ));
        // No failures at all cannot be classified as a refusal.
        assert!(matches!(
            classify_switch_open_failure(std::iter::empty()),
            SwitchOpenFailure::Fatal
        ));
    }

    #[test]
    fn window_end_is_t0_from_gen_ts_minus_pts_plus_duration() {
        let header = |gen_ts_us: u64, pts_us: u64| Header {
            version: VERSION,
            track_id: TRACK_HAPTIC,
            tier: 0,
            seq: 0,
            pts_us,
            event_id: 1,
            gen_ts_us,
            payload_len: 0,
        };
        // First object is pts 0 at t0 (+ wake latency): t0 == gen_ts.
        assert_eq!(
            derive_window_end_us(header(4_000_123, 0), 30_000_000),
            Some((4_000_123, 34_000_123))
        );
        // First observed object is a later slot (earlier ones lost): t0 is
        // still recovered from the slot arithmetic, not from arrival.
        assert_eq!(
            derive_window_end_us(header(4_033_400, 33_333), 30_000_000),
            Some((4_000_067, 34_000_067))
        );
        // Malformed relation or overflow yields no window instead of a wrong one.
        assert_eq!(derive_window_end_us(header(10, 11), 30_000_000), None);
        assert_eq!(derive_window_end_us(header(u64::MAX, 0), 1), None);
    }

    #[test]
    fn window_end_and_shutdown_info_rows_are_valid_json_with_additive_fields() {
        let header = Header {
            version: VERSION,
            track_id: TRACK_HAPTIC,
            tier: 0,
            seq: 0,
            pts_us: 0,
            event_id: 1,
            gen_ts_us: 4_000_123,
            payload_len: 0,
        };
        let haptic = Route {
            name: skew_moq::s3_switch::HAPTIC_FULL_TRACK,
            generation: 0,
        };
        let row = format!(
            "{{\"role\":\"info\",{}}}",
            s3_window_end_row_body(TrackRole::Haptic, haptic, header, 4_000_123, 30_000_000, 34_000_123)
        );
        let value: serde_json::Value = serde_json::from_str(&row).expect("valid JSON");
        assert_eq!(value["event"], "s3_window_end");
        assert_eq!(value["window_end_us"], 34_000_123_u64);
        assert_eq!(value["t0_rx_us"], 4_000_123_u64);
        assert_eq!(value["duration_us"], 30_000_000_u64);
        assert_eq!(value["t0_source"], "first_object_gen_ts_minus_pts");
        assert_eq!(value["track"], "haptic");
        assert_eq!(value["wire_track"], "haptic");
        assert_eq!(value["route_generation"], 0);
        assert_eq!(value["gen_ts_us"], 4_000_123_u64);

        // Shutdown row: the pre-existing vocabulary is untouched and the three
        // S3 fields are appended after `detail`.
        let normal = format!(
            "{{\"role\":\"info\",{}}}",
            s3_shutdown_row_body(
                false,
                0,
                "rule=s3_current_routes_fin",
                1,
                1,
                Some(34_000_123),
                4,
                true,
                0
            )
        );
        assert!(normal.starts_with(
            "{\"role\":\"info\",\"event\":\"shutdown\",\"ending\":\"normal\",\"exit_code\":0,\"bad_headers\":0,\"subscribe_retries\":0,\"subscribe_wait_ms\":0,\"tracks\":[],\"detail\":\"rule=s3_current_routes_fin\","
        ));
        let value: serde_json::Value = serde_json::from_str(&normal).expect("valid JSON");
        assert_eq!(value["s3_switch_suppressed_after_end"], 1);
        assert_eq!(value["s3_switch_refused_after_run_end"], 1);
        assert_eq!(value["s3_window_end_us"], 34_000_123_u64);
        // 13th rework: appended after the 12th rework's three fields.
        assert_eq!(value["s3_events_drained_at_exit"], 4);
        // 14th rework: appended after that, never in front of it.
        assert_eq!(value["s3_event_queue_closed"], true);
        assert_eq!(value["s3_drains_unjoined"], 0);
        let error = format!(
            "{{\"role\":\"info\",{}}}",
            s3_shutdown_row_body(
                true,
                0,
                "S3 subscribe pc-d6 generation 7 failed after 2 retries: closed, code=16",
                0,
                0,
                None,
                0,
                false,
                2
            )
        );
        let value: serde_json::Value = serde_json::from_str(&error).expect("valid JSON");
        assert_eq!(value["ending"], "error");
        assert_eq!(value["exit_code"], 1);
        assert_eq!(
            value["detail"],
            "S3 subscribe pc-d6 generation 7 failed after 2 retries: closed, code=16"
        );
        assert!(value["s3_window_end_us"].is_null());
        assert_eq!(value["s3_events_drained_at_exit"], 0);
        assert_eq!(value["s3_event_queue_closed"], false);
        assert_eq!(value["s3_drains_unjoined"], 2);
    }

    /// The shutdown path records the FIRST cause and never replaces it, which
    /// is what lets every step after the loop fold its failure in instead of
    /// returning early and skipping drain pass 2 and the shutdown row.
    /// The fault store is what makes the RELAY path obey
    /// `classify_switch_open_failure`'s "every failed role" rule. Helper-level:
    /// it pins the store's own contract (first cause, per-generation matching,
    /// role symmetry) that the two control-loop cases then exercise on wire.
    #[test]
    fn a_faulted_target_is_matched_per_role_and_per_generation() {
        use skew_moq::s3_controller::{S3State, TransitionCause};
        use skew_moq::s3_switch::Routes;

        let target = Routes {
            pc: Route {
                name: "pc-d6",
                generation: 3,
            },
            haptic: Route {
                name: "haptic-essential",
                generation: 3,
            },
        };
        let request = SwitchRequest {
            decision_at_us: 0,
            request_at_us: 0,
            from: S3State::Normal,
            to: S3State::HapticCritical,
            cause: TransitionCause::DeadlineMissStreak,
            target,
            pc_changed: true,
            haptic_changed: true,
        };
        let mut faults = S3SwitchTargetFaults::default();
        assert!(!faults.request_has_fault(request));

        // The HAPTIC role faults — the P1's own role assignment; the rule is
        // symmetric, so the PC role behaves identically.
        faults.record(
            TrackRole::Haptic,
            target.haptic,
            anyhow::anyhow!("first cause"),
        );
        assert!(faults.request_has_fault(request));
        // First-cause-wins inside the store as well.
        faults.record(TrackRole::Pc, target.pc, anyhow::anyhow!("second cause"));
        assert_eq!(
            format!("{:#}", faults.take_cause().expect("a cause was recorded")),
            "first cause"
        );
        assert!(faults.take_cause().is_none());
        // Still faulted for the rule even after the cause was promoted.
        assert!(faults.request_has_fault(request));

        // A different generation of the SAME track name is a different route,
        // so it can never be mistaken for this request's target.
        let mut other = S3SwitchTargetFaults::default();
        other.record(
            TrackRole::Haptic,
            Route {
                name: "haptic-essential",
                generation: 2,
            },
            anyhow::anyhow!("older generation"),
        );
        assert!(!other.request_has_fault(request));
        // Neither is the same route under the other role.
        let mut swapped = S3SwitchTargetFaults::default();
        swapped.record(TrackRole::Pc, target.haptic, anyhow::anyhow!("wrong role"));
        assert!(!swapped.request_has_fault(request));

        // A torn-down target stays watched until its terminal arrives.
        let mut watched = S3SwitchTargetFaults::default();
        assert!(!watched.is_watched(TrackRole::Haptic, target.haptic));
        watched.watch_abandoned(TrackRole::Haptic, target.haptic);
        watched.watch_abandoned(TrackRole::Haptic, target.haptic);
        assert!(watched.is_watched(TrackRole::Haptic, target.haptic));
        assert!(!watched.is_watched(TrackRole::Pc, target.haptic));
        watched.forget(TrackRole::Haptic, target.haptic);
        assert!(!watched.is_watched(TrackRole::Haptic, target.haptic));
    }

    #[test]
    fn the_first_cause_wins_on_the_shutdown_path() {
        let mut outcome_error: Option<anyhow::Error> = None;
        keep_first_cause(&mut outcome_error, anyhow::anyhow!("first"));
        keep_first_cause(&mut outcome_error, anyhow::anyhow!("second"));
        assert_eq!(
            format!("{:#}", outcome_error.expect("an error was recorded")),
            "first"
        );
        // Nothing to record: the slot stays empty.
        let mut none: Option<anyhow::Error> = None;
        assert!(none.is_none());
        keep_first_cause(&mut none, anyhow::anyhow!("only"));
        assert_eq!(format!("{:#}", none.expect("recorded")), "only");
    }

    #[test]
    fn switch_outcomes_count_only_the_two_additive_shutdown_fields() {
        let mut suppressed = 0;
        let mut refused = 0;
        for outcome in [
            SwitchOutcome::NoTransition,
            SwitchOutcome::Requested,
            SwitchOutcome::SuppressedAfterEnd,
            SwitchOutcome::RefusedAfterRunEnd,
            SwitchOutcome::SuppressedAfterEnd,
        ] {
            count_switch_outcome(outcome, &mut suppressed, &mut refused);
        }
        assert_eq!((suppressed, refused), (2, 1));
    }
}

// ---- 16th rework: terminal PROVENANCE and the drain-join fault -------------
//
// Defect A of the 26th review: a watched (torn-down) switch target whose
// failure carried no close code was ignored, because the 15th rework used the
// PRESENCE of a code as its only way to tell a peer failure from the
// receiver's own UNSUBSCRIBE. These cases pin the replacement — a provenance
// tag the drain task itself records — at three levels: the rule as a pure
// function, the drain task that produces the tag, and (in the control-loop
// module below) the real loop.
//
// Defect C: a panicking drain join must fail the run.
#[cfg(test)]
mod s3_terminal_provenance_tests {
    use super::*;
    use crate::s3_retirement_tests::test_log_path;
    use moq_transport::coding::TrackNamespace;
    use moq_transport::serve::ServeError;

    fn route() -> Route {
        Route {
            name: skew_moq::s3_switch::PC_HAPTIC_CRITICAL_TRACK,
            generation: 2,
        }
    }

    /// The whole watched/pending fault rule, as a truth table.
    ///
    /// The row that used to be WRONG is the last `Remote` pair: a torn-down
    /// target that ends `Cancelled`/`Failed` with NO code. The 15th rework
    /// answered `false` for both of them, which is how a real target failure
    /// was normalised away; only the `LocalRelease` form may answer `false`.
    #[test]
    fn a_code_less_remote_terminal_faults_and_our_own_release_does_not() {
        use TerminalSource::{LocalRelease, Remote};
        let run_ended = Some(S3_RUN_ENDED_REQUEST_ERROR_CODE);
        let does_not_exist = Some(u64::from(
            moq_transport::message::RequestErrorCode::DoesNotExist,
        ));

        // The sender's run-ended code is never a fault, pending or watched,
        // whatever the provenance says.
        for pending in [true, false] {
            for source in [Remote, LocalRelease] {
                assert!(!is_switch_target_fault(
                    pending,
                    TrackEnd::Cancelled,
                    run_ended,
                    source
                ));
            }
        }
        // A FIN is never a fault either.
        for pending in [true, false] {
            for source in [Remote, LocalRelease] {
                assert!(!is_switch_target_fault(pending, TrackEnd::Fin, None, source));
            }
        }
        // PENDING: the subscription is still live, so every non-FIN terminal
        // is the peer's — including a code-less one. Unchanged.
        for source in [Remote, LocalRelease] {
            assert!(is_switch_target_fault(
                true,
                TrackEnd::Cancelled,
                None,
                source
            ));
            assert!(is_switch_target_fault(true, TrackEnd::Failed, None, source));
            assert!(is_switch_target_fault(
                true,
                TrackEnd::Cancelled,
                does_not_exist,
                source
            ));
        }
        // WATCHED with a close CODE: our UNSUBSCRIBE never produces one, so it
        // is a fault regardless of provenance. Unchanged, and NOT weakened.
        for source in [Remote, LocalRelease] {
            assert!(is_switch_target_fault(
                false,
                TrackEnd::Cancelled,
                does_not_exist,
                source
            ));
        }
        // WATCHED, no code — THE DEFECT. `Remote` faults; only our own
        // release is excused.
        assert!(is_switch_target_fault(
            false,
            TrackEnd::Cancelled,
            None,
            Remote
        ));
        assert!(is_switch_target_fault(false, TrackEnd::Failed, None, Remote));
        assert!(!is_switch_target_fault(
            false,
            TrackEnd::Cancelled,
            None,
            LocalRelease
        ));
        assert!(!is_switch_target_fault(
            false,
            TrackEnd::Failed,
            None,
            LocalRelease
        ));
    }

    async fn drain_once(
        local_release: Arc<AtomicBool>,
        act: impl FnOnce(moq_transport::serve::TrackWriter),
    ) -> S3WireEvent {
        let out = test_log_path("s3-provenance");
        let logger = Arc::new(Mutex::new(
            JsonlLogger::new(
                &out, "run", "moq", "rx", None, 0.0, 0.0, 0.0, 10, 30, 90, 1, None, None,
                Some("both"), Some(TERM_PROTOCOL_V), None, None, None,
            )
            .unwrap(),
        ));
        let (event_tx, mut event_rx) = mpsc::channel::<S3WireEvent>(4);
        let (writer, reader) =
            Track::new(TrackNamespace::from_utf8_path("/provenance"), "pc").produce();
        let drain = tokio::spawn(drain_s3_track(
            TrackRole::Pc,
            route(),
            reader,
            logger,
            event_tx,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            local_release,
        ));
        act(writer);
        let event = tokio::time::timeout(Duration::from_secs(5), event_rx.recv())
            .await
            .expect("the drain must terminate")
            .expect("drain alive");
        drain.await.unwrap();
        std::fs::remove_file(&out).ok();
        event
    }

    /// The REAL `drain_s3_track` produces the tag, and it reads the flag at
    /// the instant it classifies the terminal.
    #[tokio::test]
    async fn the_drain_task_tags_a_code_less_cancel_by_the_release_flag() {
        // Nobody released anything: a bare `Cancel` is the peer's.
        let event = drain_once(Arc::new(AtomicBool::new(false)), |writer| {
            writer.close(ServeError::Cancel).unwrap();
        })
        .await;
        match event {
            S3WireEvent::Ended {
                end,
                close_code,
                source,
                ..
            } => {
                assert_eq!(end, TrackEnd::Cancelled);
                assert_eq!(close_code, None);
                assert_eq!(source, TerminalSource::Remote);
                // ... and that is exactly what the watched rule now faults.
                assert!(is_switch_target_fault(false, end, close_code, source));
            }
            other => panic!("expected an end, got {other:?}"),
        }

        // The receiver had already begun releasing: the same wire terminal is
        // OUR teardown and must not fault.
        let event = drain_once(Arc::new(AtomicBool::new(true)), |writer| {
            writer.close(ServeError::Cancel).unwrap();
        })
        .await;
        match event {
            S3WireEvent::Ended {
                end,
                close_code,
                source,
                ..
            } => {
                assert_eq!(end, TrackEnd::Cancelled);
                assert_eq!(close_code, None);
                assert_eq!(source, TerminalSource::LocalRelease);
                assert!(!is_switch_target_fault(false, end, close_code, source));
            }
            other => panic!("expected an end, got {other:?}"),
        }
    }

    /// The `Failed`/`None` variant the reviewer named: a NON-SUBGROUP track.
    /// Dropping the handle cannot cause it, so it is `Remote` by construction
    /// — even with the release flag already set — and it faults a watched
    /// target.
    #[tokio::test]
    async fn a_non_subgroup_track_is_a_code_less_remote_failure() {
        for released in [false, true] {
            let event = drain_once(Arc::new(AtomicBool::new(released)), |writer| {
                // Datagram mode, not subgroups: `DrainFail::NonSubgroup`.
                let datagrams = writer.datagrams().expect("datagram mode");
                drop(datagrams);
            })
            .await;
            match event {
                S3WireEvent::Ended {
                    end,
                    close_code,
                    source,
                    ..
                } => {
                    assert_eq!(end, TrackEnd::Failed);
                    assert_eq!(close_code, None);
                    assert_eq!(
                        source,
                        TerminalSource::Remote,
                        "a malformed subgroup/log state is never our teardown"
                    );
                    assert!(is_switch_target_fault(false, end, close_code, source));
                }
                other => panic!("expected an end, got {other:?}"),
            }
        }
    }

    /// Defect C. The shutdown path joins every aborted drain through
    /// `join_s3_drain` + `note_s3_drain_join`; these are exactly those two
    /// calls. A panicking task fails the run (`outcome_error` set, additive
    /// row written, `drains_unjoined` untouched); our own cancellation does
    /// not.
    ///
    /// Helper-level on purpose: `drain_s3_track` has no reachable panic, so a
    /// real-loop case cannot produce one without adding a production failure
    /// injection point.
    #[tokio::test]
    async fn a_panicking_drain_join_fails_the_run_and_a_cancelled_one_does_not() {
        let out = test_log_path("s3-drain-panic");
        let logger = Arc::new(Mutex::new(
            JsonlLogger::new(
                &out, "run", "moq", "rx", None, 0.0, 0.0, 0.0, 10, 30, 90, 1, None, None,
                Some("both"), Some(TERM_PROTOCOL_V), None, None, None,
            )
            .unwrap(),
        ));
        let mut drains_unjoined = 0u64;
        let mut outcome_error: Option<anyhow::Error> = None;

        // Our own abort of a healthy task: the NORMAL outcome.
        let cancelled = tokio::spawn(async { std::future::pending::<()>().await });
        cancelled.abort();
        let join = join_s3_drain(cancelled).await;
        assert!(matches!(join, DrainJoin::Joined), "{join:?}");
        note_s3_drain_join(join, &logger, &mut drains_unjoined, &mut outcome_error);
        assert!(outcome_error.is_none());
        assert_eq!(drains_unjoined, 0);

        // A drain that PANICKED between objects: no unterminated object, no
        // route residue, the queue still closes — and yet the run must fail.
        let panicking = tokio::spawn(async { panic!("scripted drain panic") });
        let join = join_s3_drain(panicking).await;
        assert!(matches!(join, DrainJoin::Panicked(_)), "{join:?}");
        note_s3_drain_join(join, &logger, &mut drains_unjoined, &mut outcome_error);
        let cause = format!(
            "{:#}",
            outcome_error.expect("a panicking drain join is a run fault")
        );
        assert!(cause.contains("S3 drain task panicked"), "{cause}");
        assert_eq!(drains_unjoined, 0, "a panic is not an unjoined task");

        // The additive row is KEPT, not replaced by the verdict.
        drop(logger);
        let text = std::fs::read_to_string(&out).expect("log written");
        assert_eq!(
            text.lines()
                .filter(|line| line.contains("\"event\":\"s3_drain_join_panic\""))
                .count(),
            1,
            "{text}"
        );
        std::fs::remove_file(&out).ok();
    }
}

// ---- 13th rework: the REAL S3 control loop under test ----------------------
//
// Every test in this module drives `run_s3_control`, i.e. the production
// control loop, switch request path, exit drain and shutdown accounting, with
// exactly one substitution: `S3SubscribeSeam`. The seam's production
// implementation (`SubscriberSeam`) forwards to
// `Subscriber::subscribe_open_with_params` unchanged, so no production
// behaviour is altered by its existence.
//
// A full in-process QUIC/WebTransport session is NOT available offline: the
// workspace has no certificate generator among its dependencies and
// `dev/spike.{crt,key}` is untracked and expired. Below the seam everything is
// real — real `Track::produce()` writers, the real `drain_s3_track` task, real
// `rx`/`drop`/`release` JSONL rows, the real ingress, scheduler, deadline
// tracker and switch gate.
#[cfg(test)]
mod s3_control_loop_tests {
    use super::*;
    use moq_transport::message::RequestErrorCode;
    use moq_transport::serve::{ServeError, SubgroupsWriter, TrackWriter};
    use crate::s3_retirement_tests::test_log_path;
    use moq_transport::coding::TrackNamespace;
    use std::collections::VecDeque;

    const PC_NORMAL: &str = "pc";
    const HAPTIC_FULL: &str = "haptic";
    const PC_CRITICAL: &str = "pc-d6";
    const HAPTIC_ESSENTIAL: &str = "haptic-essential";

    /// One step the script performs on the already-open tracks.
    #[derive(Debug, Clone)]
    enum Step {
        /// Publish these `(tier, seq, pts_us, event_id)` objects, one subgroup
        /// each, on an open track.
        Publish(&'static str, Vec<(u16, u32, u64, u32)>),
        /// Drop the track's writer: the drain task observes end-of-track and
        /// emits `S3WireEvent::Ended { end: Fin }`.
        Fin(&'static str),
        /// Close an ALREADY-OPEN track with an application error code, i.e.
        /// what a relay does when the upstream answer arrives after the
        /// downstream `SUBSCRIBE_OK`: moq-transport turns the upstream
        /// `REQUEST_ERROR(code)` into `PUBLISH_DONE(code)`, which reaches the
        /// receiver's drain as `ServeError::Closed(code)` from
        /// `SubgroupsReader::next`.
        CloseError(&'static str, u64),
        /// Close an already-open track with `ServeError::Cancel`: a terminal
        /// with NO application code, which is what a generic teardown or a
        /// session collapse looks like on the wire side.
        Cancel(&'static str),
        /// Hand the runtime `n` scheduling turns so the spawned drain tasks
        /// actually read what was just published and enqueue it. This is what
        /// makes the queue ORDER deterministic.
        Yield(usize),
        /// Like `Yield` but with a real delay, for the points where a drain
        /// task has to complete a multi-step read before the next scripted
        /// action is allowed to happen.
        Sleep(u64),
    }

    /// The scripted answer to one `subscribe_open` call.
    #[derive(Debug, Clone)]
    enum Answer {
        /// Accept; the steps run after this track's writer is registered but
        /// before the handle is returned (so before its drain task exists).
        Accept(Vec<Step>),
        /// Refuse with this wire error, as a REQUEST_ERROR would arrive.
        Refuse(ServeError),
        /// Never answer: exercises the registered switch effect timeout.
        Hang,
    }

    #[derive(Debug, Clone)]
    struct Call {
        track: &'static str,
        /// Performed before the answer is produced.
        before: Vec<Step>,
        answer: Answer,
    }

    #[derive(Default)]
    struct SeamState {
        script: VecDeque<Call>,
        open: HashMap<String, SubgroupsWriter>,
        /// Track names actually asked for, in call order.
        calls: Vec<String>,
    }

    /// Scripted subscribe seam. Only `subscribe_open` is scripted; everything
    /// it returns is a real `Track` writer/reader pair.
    #[derive(Clone)]
    struct ScriptedSeam {
        state: Arc<Mutex<SeamState>>,
    }

    impl ScriptedSeam {
        fn new(script: Vec<Call>) -> Self {
            Self {
                state: Arc::new(Mutex::new(SeamState {
                    script: script.into(),
                    ..SeamState::default()
                })),
            }
        }

        fn calls(&self) -> Vec<String> {
            self.state.lock().unwrap().calls.clone()
        }

        /// Steps only touch the shared map, never await, so the lock is never
        /// held across a suspension point.
        fn apply_sync(&self, step: &Step) {
            let mut state = self.state.lock().unwrap();
            match step {
                Step::Publish(track, objects) => {
                    let writer = state
                        .open
                        .get_mut(*track)
                        .unwrap_or_else(|| panic!("publish on unopened track {track}"));
                    for (tier, seq, pts_us, event_id) in objects {
                        let mut subgroup = writer.append(1).expect("append subgroup");
                        subgroup
                            .write(wire_bytes(*track, *tier, *seq, *pts_us, *event_id))
                            .expect("write object");
                    }
                }
                Step::Fin(track) => {
                    state
                        .open
                        .remove(*track)
                        .unwrap_or_else(|| panic!("FIN on unopened track {track}"));
                }
                Step::CloseError(track, code) => {
                    state
                        .open
                        .remove(*track)
                        .unwrap_or_else(|| panic!("close on unopened track {track}"))
                        .close(ServeError::Closed(*code))
                        .expect("close the track with an error");
                }
                Step::Cancel(track) => {
                    state
                        .open
                        .remove(*track)
                        .unwrap_or_else(|| panic!("cancel on unopened track {track}"))
                        .close(ServeError::Cancel)
                        .expect("cancel the track");
                }
                Step::Yield(_) | Step::Sleep(_) => {}
            }
        }

        async fn run(&self, steps: &[Step]) {
            for step in steps {
                match step {
                    Step::Yield(turns) => {
                        for _ in 0..*turns {
                            tokio::task::yield_now().await;
                        }
                    }
                    Step::Sleep(ms) => tokio::time::sleep(Duration::from_millis(*ms)).await,
                    other => self.apply_sync(other),
                }
            }
        }
    }

    impl S3SubscribeSeam for ScriptedSeam {
        /// No wire, so the handle is inert. Production drops a `Subscribe`
        /// here, which is the only thing the receiver ever does with it.
        type Handle = &'static str;

        async fn subscribe_open(
            &mut self,
            writer: TrackWriter,
            _params: KeyValuePairs,
        ) -> std::result::Result<Self::Handle, ServeError> {
            let name = writer.info.name.to_string_lossy().into_owned();
            let call = {
                let mut state = self.state.lock().unwrap();
                state.calls.push(name.clone());
                state
                    .script
                    .pop_front()
                    .unwrap_or_else(|| panic!("unscripted subscribe for {name}"))
            };
            assert_eq!(call.track, name, "script/track order mismatch");
            self.run(&call.before).await;
            match call.answer {
                Answer::Accept(steps) => {
                    {
                        let mut state = self.state.lock().unwrap();
                        state
                            .open
                            .insert(name.clone(), writer.subgroups().expect("subgroups"));
                    }
                    self.run(&steps).await;
                    Ok("scripted-subscription")
                }
                Answer::Refuse(error) => Err(error),
                Answer::Hang => std::future::pending().await,
            }
        }
    }

    fn wire_bytes(track: &str, tier: u16, seq: u32, pts_us: u64, event_id: u32) -> Bytes {
        let track_id = match track {
            PC_NORMAL | PC_CRITICAL | "pc-d7" => TRACK_PC,
            _ => TRACK_HAPTIC,
        };
        let payload = [0u8; 4];
        let header = pack_header(
            track_id,
            tier,
            seq,
            pts_us,
            event_id,
            // `t0_rx = gen_ts_us - pts_us`, so a non-zero stamp keeps the
            // derived window end in the same monotonic frame as the run.
            now_us(),
            payload.len() as u32,
        );
        let mut bytes = Vec::with_capacity(HDR + payload.len());
        bytes.extend_from_slice(&header);
        bytes.extend_from_slice(&payload);
        Bytes::from(bytes)
    }

    /// Registered S3 receiver CLI (`scripts/run_phase4_v5_s3_loopback.sh`) with
    /// the registered switch effect timeout of 2000 ms. Only `--duration-s`,
    /// `--max-duration` and the test-mode forcing delay vary per case, and none
    /// of those is a registered control parameter.
    fn s3_args(
        out: &std::path::Path,
        run_id: &str,
        duration_s: Option<&str>,
        force_ms: &str,
    ) -> Args {
        let out = out.display().to_string();
        let mut cli = vec![
            "moq_receiver",
            "--relay",
            "https://127.0.0.1:4443",
            "--run-id",
            run_id,
            "--out",
            &out,
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
            "--topology",
            "relay",
            "--chunk-bytes",
            "178",
            "--reassembly-max-pending-frames",
            "64",
            "--reassembly-max-pending-bytes",
            "67108864",
            "--reassembly-max-age-ms",
            "2000",
            "--data-priority-mapping",
            "moqt-v2",
            "--tracks",
            "both",
            "--max-duration",
            "6",
            "--seed",
            "1",
            "--arm",
            "s3",
            "--d-play-ms",
            "100",
            "--startup-timeout-ms",
            "2000",
            "--startup-rearm-limit",
            "1",
            "--late-tolerance-ms",
            "10",
            "--buffer-max-objects-per-track",
            "4096",
            "--buffer-max-span-ms",
            "3000",
            "--late-policy",
            "drop-late",
            "--pc-delivery-timeout-ms",
            "67",
            "--s3-window-ms",
            "1000",
            "--s3-ewma-alpha",
            "0.2",
            "--s3-miss-streak-threshold",
            "3",
            "--s3-violation-ratio-threshold",
            "0.2",
            "--s3-target-skew-ms",
            "25",
            "--s3-recovery-fraction",
            "0.5",
            "--s3-haptic-critical-stable-ms",
            "3000",
            "--s3-recovery-stable-ms",
            "5000",
            "--s3-cooldown-ms",
            "2000",
            "--s3-min-paired-samples",
            "5",
            "--s3-max-window-samples",
            "128",
            "--s3-effect-timeout-ms",
            "2000",
            "--s3-initial-retry-limit",
            "20",
            "--s3-switch-retry-limit",
            "2",
            "--s3-deadline-max-anchors",
            "900",
            "--s3-test-mode",
            "--s3-test-force-misses-after-ms",
            force_ms,
        ];
        if let Some(duration_s) = duration_s {
            cli.push("--duration-s");
            cli.push(duration_s);
        }
        Args::try_parse_from(cli).expect("registered S3 receiver CLI must parse")
    }

    struct CaseOutcome {
        result: Result<()>,
        rows: Vec<serde_json::Value>,
        calls: Vec<String>,
    }

    impl CaseOutcome {
        fn find(&self, role: &str, event: &str) -> Vec<&serde_json::Value> {
            self.rows
                .iter()
                .filter(|row| row["role"] == role && row["event"] == event)
                .collect()
        }

        fn shutdown(&self) -> &serde_json::Value {
            let rows = self.find("info", "shutdown");
            assert_eq!(rows.len(), 1, "exactly one shutdown row");
            rows[0]
        }

        /// Identity of one object row: the wire route plus the header identity.
        fn keys(&self, role: &str) -> Vec<String> {
            self.rows
                .iter()
                .filter(|row| row["role"] == role)
                .map(|row| {
                    format!(
                        "{}/{}/{}/{}/{}",
                        row["wire_track"], row["route_generation"], row["seq"], row["pts_us"],
                        row["event_id"]
                    )
                })
                .collect()
        }

        /// The invariant the `object_routes` residue check cannot see on its
        /// own: EVERY object a drain task rx-logged has exactly one terminal.
        fn assert_every_rx_object_is_terminated(&self) {
            let rx = self.keys("rx");
            let mut terminals = self.keys("release");
            terminals.extend(self.keys("drop"));
            for key in &rx {
                let n = terminals.iter().filter(|t| *t == key).count();
                assert_eq!(
                    n, 1,
                    "rx object {key} must have exactly one release/drop terminal, got {n}"
                );
            }
            for key in &terminals {
                assert!(
                    rx.contains(key),
                    "terminal {key} has no rx row: {terminals:?}"
                );
            }
        }
    }

    /// How the session task ends, for the cases that exercise the
    /// `session_run` arm of the control loop's `select!`.
    ///
    /// The production task is `session.run()`, whose output is a `Result`.
    /// All variants share ONE output type so the loop's `T: Debug` parameter
    /// is instantiated exactly once per case.
    #[derive(Debug, Clone, Copy)]
    enum SessionEnding {
        /// Healthy: never ends on its own, aborted at shutdown (the default
        /// every pre-existing case uses).
        Never,
        /// Returns `Ok(())` after this many milliseconds.
        NormalAfterMs(u64),
        /// Returns `Err(..)` after this many milliseconds.
        ErrorAfterMs(u64),
        /// PANICS after this many milliseconds, so the join handle yields
        /// `Err(JoinError)`.
        PanicAfterMs(u64),
    }

    impl SessionEnding {
        fn spawn(self) -> tokio::task::JoinHandle<std::result::Result<(), String>> {
            tokio::spawn(async move {
                match self {
                    SessionEnding::Never => std::future::pending().await,
                    SessionEnding::NormalAfterMs(ms) => {
                        tokio::time::sleep(Duration::from_millis(ms)).await;
                        Ok(())
                    }
                    SessionEnding::ErrorAfterMs(ms) => {
                        tokio::time::sleep(Duration::from_millis(ms)).await;
                        Err("scripted session failure".to_string())
                    }
                    SessionEnding::PanicAfterMs(ms) => {
                        tokio::time::sleep(Duration::from_millis(ms)).await;
                        panic!("scripted session panic");
                    }
                }
            })
        }
    }

    async fn run_case(
        name: &str,
        duration_s: &str,
        force_ms: &str,
        script: Vec<Call>,
    ) -> CaseOutcome {
        run_case_inner(
            name,
            Some(duration_s),
            force_ms,
            script,
            Vec::new(),
            SessionEnding::Never,
        )
        .await
    }

    /// A case whose SESSION ends early, while the loop is running.
    async fn run_case_with_session(
        name: &str,
        force_ms: &str,
        script: Vec<Call>,
        session: SessionEnding,
    ) -> CaseOutcome {
        run_case_inner(name, Some("30"), force_ms, script, Vec::new(), session).await
    }

    /// No `--duration-s`: the receiver can derive no window end at all.
    async fn run_case_without_duration(
        name: &str,
        force_ms: &str,
        script: Vec<Call>,
    ) -> CaseOutcome {
        run_case_inner(name, None, force_ms, script, Vec::new(), SessionEnding::Never).await
    }

    /// `delayed` steps run from a side task `after_ms` after the loop starts —
    /// the only way to act on the tracks at a time when the seam is not being
    /// called (e.g. to end a run in which no switch is ever requested).
    async fn run_case_delayed(
        name: &str,
        duration_s: Option<&str>,
        force_ms: &str,
        script: Vec<Call>,
        delayed: Vec<(u64, Vec<Step>)>,
    ) -> CaseOutcome {
        run_case_inner(name, duration_s, force_ms, script, delayed, SessionEnding::Never).await
    }

    async fn run_case_inner(
        name: &str,
        duration_s: Option<&str>,
        force_ms: &str,
        script: Vec<Call>,
        delayed: Vec<(u64, Vec<Step>)>,
        session: SessionEnding,
    ) -> CaseOutcome {
        let out = test_log_path(name);
        let args = s3_args(&out, "loop-test", duration_s, force_ms);
        let playout = playout_config(&args)
            .expect("playout config")
            .expect("S3 uses the common scheduler");
        let runtime = s3_runtime_config(&args)
            .expect("runtime config")
            .expect("S3 runtime");
        let logger = Arc::new(Mutex::new(
            JsonlLogger::new(
                &out,
                &args.run_id,
                "moq",
                "rx",
                None,
                0.0,
                0.0,
                0.0,
                1,
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
                    replay: None,
                }),
            )
            .unwrap(),
        ));
        let seam = ScriptedSeam::new(script);
        let side = {
            let seam = seam.clone();
            tokio::spawn(async move {
                for (after_ms, steps) in delayed {
                    tokio::time::sleep(Duration::from_millis(after_ms)).await;
                    seam.run(&steps).await;
                }
            })
        };
        // By default the session task never ends on its own, exactly like a
        // healthy `session.run()`; the loop aborts it at shutdown. A case may
        // instead script a normal return, an error return or a panic, which
        // is what makes the loop CONSUME the join handle.
        let session_run = session.spawn();
        let result = run_s3_control(
            &args,
            playout,
            runtime,
            logger.clone(),
            TrackNamespace::from_utf8_path(&args.run_id),
            seam.clone(),
            session_run,
        )
        .await;
        side.abort();
        let _ = side.await;
        let text = std::fs::read_to_string(&out).expect("log written");
        if std::env::var_os("SKEW_TEST_DUMP").is_some() {
            eprintln!("---- {name} ----\n{text}\n---- result {:?} ----", result);
        }
        std::fs::remove_file(&out).ok();
        let rows = text
            .lines()
            .map(|line| {
                serde_json::from_str(line).unwrap_or_else(|e| panic!("bad JSONL row {line}: {e}"))
            })
            .collect();
        CaseOutcome {
            result,
            rows,
            calls: seam.calls(),
        }
    }

    /// Initial Normal subscribes: PC first, then haptic, which publishes the
    /// exact pair that arms the common timeline (and, being the first observed
    /// object, fixes the derived window end).
    fn initial_calls(after_epoch: Vec<Step>) -> Vec<Call> {
        let mut haptic_steps = vec![
            Step::Publish(PC_NORMAL, vec![(2, 1, 0, 1)]),
            Step::Publish(HAPTIC_FULL, vec![(0, 1, 0, 1)]),
        ];
        haptic_steps.extend(after_epoch);
        vec![
            Call {
                track: PC_NORMAL,
                before: Vec::new(),
                answer: Answer::Accept(Vec::new()),
            },
            Call {
                track: HAPTIC_FULL,
                before: Vec::new(),
                answer: Answer::Accept(haptic_steps),
            },
        ]
    }

    /// The sender's "my current routes finished producing" refusal, built by
    /// the sender's OWN constructor so a drift there fails these cases instead
    /// of silently making the receiver's non-fatal path unreachable.
    fn run_ended() -> ServeError {
        skew_moq::s3_sender::run_ended_refusal(
            "S3 subscription 'pc-d6' arrived after run completion",
        )
    }

    /// The sender's OTHER run-ended refusal: the SUBSCRIBE arrived while the
    /// namespace was draining after the run end. Same wire code by
    /// construction; the receiver cannot and need not tell them apart.
    fn drain_window_refusal() -> ServeError {
        skew_moq::s3_sender::run_ended_refusal("S3 namespace is draining after run end")
    }

    /// (a) A transition whose request instant is before the window end is
    /// requested on wire and applied at the target exact pair.
    #[tokio::test]
    async fn transition_before_the_window_end_is_requested_and_applied() {
        let mut script = initial_calls(Vec::new());
        script.push(Call {
            track: PC_CRITICAL,
            before: Vec::new(),
            answer: Answer::Accept(Vec::new()),
        });
        script.push(Call {
            track: HAPTIC_ESSENTIAL,
            before: Vec::new(),
            answer: Answer::Accept(vec![
                // Target exact pair: the switch takes effect here.
                Step::Publish(PC_CRITICAL, vec![(4, 2, 33_333, 2)]),
                Step::Publish(HAPTIC_ESSENTIAL, vec![(0, 2, 33_333, 2)]),
                Step::Yield(50),
            ]),
        });
        let outcome = run_case_delayed(
            "s3-loop-a",
            Some("30"),
            "20",
            script,
            // AFTER the apply: a FIN observed before the switch takes effect
            // names a route that is not current yet and is ignored, which is
            // pre-existing behaviour and not what this case is about.
            vec![(
                300,
                vec![
                    Step::Fin(PC_CRITICAL),
                    Step::Fin(HAPTIC_ESSENTIAL),
                    Step::Fin(PC_NORMAL),
                    Step::Fin(HAPTIC_FULL),
                ],
            )],
        )
        .await;
        assert!(outcome.result.is_ok(), "{:?}", outcome.result);
        assert_eq!(
            outcome.calls,
            vec![PC_NORMAL, HAPTIC_FULL, PC_CRITICAL, HAPTIC_ESSENTIAL]
        );
        assert_eq!(outcome.find("s3_switch", "request").len(), 1);
        assert_eq!(outcome.find("s3_switch", "apply").len(), 1);
        assert!(outcome.find("s3_switch", "suppressed_after_end").is_empty());
        assert!(outcome.find("s3_switch", "refused_after_run_end").is_empty());
        let shutdown = outcome.shutdown();
        assert_eq!(shutdown["ending"], "normal");
        assert_eq!(shutdown["detail"], "rule=s3_current_routes_fin");
        assert_eq!(shutdown["s3_switch_suppressed_after_end"], 0);
        assert_eq!(shutdown["s3_switch_refused_after_run_end"], 0);
        outcome.assert_every_rx_object_is_terminated();
    }

    /// (b) A transition whose request instant falls at/after the window end is
    /// logged and never requested: no third subscribe call is made at all.
    #[tokio::test]
    async fn transition_at_or_after_the_window_end_is_suppressed_without_a_request() {
        // `--duration-s 0.001` puts the derived window end 1 ms after the
        // first observed object, i.e. before the forced transition.
        let script = initial_calls(Vec::new());
        let outcome = run_case_delayed(
            "s3-loop-b",
            Some("0.001"),
            "20",
            script,
            // The run must outlive the forced transition, and no switch
            // subscribe is expected, so the FIN comes from a side task.
            vec![(120, vec![Step::Fin(PC_NORMAL), Step::Fin(HAPTIC_FULL)])],
        )
        .await;
        assert!(outcome.result.is_ok(), "{:?}", outcome.result);
        // Only the two initial subscribes were ever opened.
        assert_eq!(outcome.calls, vec![PC_NORMAL, HAPTIC_FULL]);
        let suppressed = outcome.find("s3_switch", "suppressed_after_end");
        assert_eq!(suppressed.len(), 1);
        let row = suppressed[0];
        assert!(row["t_request"].as_u64().unwrap() >= row["window_end_us"].as_u64().unwrap());
        assert_eq!(row["from_state"], "Normal");
        assert_eq!(row["to_state"], "Haptic-Critical");
        assert!(outcome.find("s3_switch", "request").is_empty());
        let shutdown = outcome.shutdown();
        assert_eq!(shutdown["ending"], "normal");
        assert_eq!(shutdown["s3_switch_suppressed_after_end"], 1);
        outcome.assert_every_rx_object_is_terminated();
    }

    /// (c) Both roles refused with the run-ended code: non-fatal, one row per
    /// role, the run still ends by the normal FIN rule.
    #[tokio::test]
    async fn both_roles_refused_with_the_completion_code_is_not_fatal() {
        let mut script = initial_calls(Vec::new());
        script.push(Call {
            track: PC_CRITICAL,
            before: Vec::new(),
            answer: Answer::Refuse(run_ended()),
        });
        script.push(Call {
            track: HAPTIC_ESSENTIAL,
            before: vec![
                Step::Fin(PC_NORMAL),
                Step::Fin(HAPTIC_FULL),
                Step::Yield(50),
            ],
            answer: Answer::Refuse(run_ended()),
        });
        let outcome = run_case("s3-loop-c", "30", "20", script).await;
        assert!(outcome.result.is_ok(), "{:?}", outcome.result);
        let refused = outcome.find("s3_switch", "refused_after_run_end");
        assert_eq!(refused.len(), 2);
        for row in &refused {
            assert_eq!(
                row["error_code"].as_u64().unwrap(),
                S3_RUN_ENDED_REQUEST_ERROR_CODE
            );
            assert!(row["t_settled"].as_u64().unwrap() >= row["t_refused"].as_u64().unwrap());
        }
        assert_eq!(refused[0]["track"], "pc");
        assert_eq!(refused[1]["track"], "haptic");
        let shutdown = outcome.shutdown();
        assert_eq!(shutdown["ending"], "normal");
        assert_eq!(shutdown["detail"], "rule=s3_current_routes_fin");
        assert_eq!(shutdown["s3_switch_refused_after_run_end"], 1);
        assert_eq!(shutdown["s3_switch_suppressed_after_end"], 0);
        outcome.assert_every_rx_object_is_terminated();
    }

    /// (d) THE P1-2 REGRESSION. One target role succeeds and starts draining
    /// (rx rows + queued objects); the other is refused; the current routes'
    /// FINs are already queued AHEAD of those objects, so the loop ends before
    /// ever dequeuing them. Every one of them must still be terminated.
    #[tokio::test]
    async fn partial_success_with_fin_first_terminates_every_target_object() {
        let mut script = initial_calls(Vec::new());
        script.push(Call {
            track: PC_CRITICAL,
            // The current routes FIN first and their drain tasks enqueue the
            // two `Ended` events BEFORE any target object exists.
            before: vec![
                Step::Fin(PC_NORMAL),
                Step::Fin(HAPTIC_FULL),
                Step::Yield(50),
            ],
            answer: Answer::Accept(vec![Step::Publish(
                PC_CRITICAL,
                vec![(4, 2, 33_333, 2), (4, 3, 66_666, 3)],
            )]),
        });
        script.push(Call {
            track: HAPTIC_ESSENTIAL,
            // Let the pc-d6 drain task actually read and enqueue the two
            // objects before the refusal tears it down. This is well inside
            // the registered 2 s effect timeout.
            before: vec![Step::Sleep(40), Step::Yield(50)],
            answer: Answer::Refuse(run_ended()),
        });
        let outcome = run_case("s3-loop-d", "30", "20", script).await;
        assert!(outcome.result.is_ok(), "{:?}", outcome.result);
        // The target route really did deliver: without these rows the case
        // would be vacuous.
        let target_rx: Vec<_> = outcome
            .rows
            .iter()
            .filter(|row| row["role"] == "rx" && row["wire_track"] == PC_CRITICAL)
            .collect();
        assert_eq!(target_rx.len(), 2, "target objects must be rx-logged");
        // ... and every one of them is terminally dropped, not lost.
        let target_drops: Vec<_> = outcome
            .rows
            .iter()
            .filter(|row| row["role"] == "drop" && row["wire_track"] == PC_CRITICAL)
            .collect();
        assert_eq!(target_drops.len(), 2, "every target object needs a terminal");
        outcome.assert_every_rx_object_is_terminated();
        let shutdown = outcome.shutdown();
        assert_eq!(shutdown["ending"], "normal");
        assert_eq!(shutdown["detail"], "rule=s3_current_routes_fin");
        assert_eq!(shutdown["s3_switch_refused_after_run_end"], 1);
        assert!(
            shutdown["s3_events_drained_at_exit"].as_u64().unwrap() >= 2,
            "the two target objects were drained at exit: {shutdown}"
        );
    }

    /// (d2) 16th rework, P1-B — the DIRECT partial-success teardown.
    ///
    /// Same shape as (d): the PC target opens and drains, the haptic target is
    /// refused with the run-ended code, so `request_s3_switch` tears the PC
    /// target down and abandons the request WITHOUT failing the run. The
    /// difference is that the PC target really failed first (`Closed(0x10)`),
    /// and its `Ended` was already queued when the teardown ran.
    ///
    /// The 15th rework registered the torn-down target in the fault store's
    /// watch list only on the RELAY path, so on this path the queued terminal
    /// matched nothing — not pending (abandoned), not watched (never
    /// registered), not current — and was dropped; the run ended `normal`.
    /// The direct path now registers the same watch and writes the same
    /// additive `s3_switch_target_torn_down` row as the relay path.
    #[tokio::test]
    async fn a_directly_torn_down_target_that_already_failed_fails_the_run() {
        let mut script = initial_calls(Vec::new());
        script.push(Call {
            track: PC_CRITICAL,
            before: vec![
                Step::Fin(PC_NORMAL),
                Step::Fin(HAPTIC_FULL),
                Step::Yield(50),
            ],
            answer: Answer::Accept(vec![Step::Publish(
                PC_CRITICAL,
                vec![(4, 2, 33_333, 2), (4, 3, 66_666, 3)],
            )]),
        });
        script.push(Call {
            track: HAPTIC_ESSENTIAL,
            before: vec![
                // Let the pc-d6 drain read and enqueue the two objects ...
                Step::Sleep(40),
                Step::Yield(50),
                // ... then the successfully opened target FAILS, and its
                // terminal is enqueued BEFORE the sibling's refusal is even
                // produced.
                Step::CloseError(PC_CRITICAL, u64::from(RequestErrorCode::DoesNotExist)),
                Step::Yield(50),
            ],
            answer: Answer::Refuse(run_ended()),
        });
        let outcome = run_case("s3-loop-d2", "30", "20", script).await;
        let error = outcome
            .result
            .as_ref()
            .expect_err("a failure of the successfully opened target must fail the run");
        let text = format!("{error:#}");
        assert!(
            text.contains(PC_CRITICAL) && text.contains("error_code=16"),
            "the 0x10 terminal of the torn-down target must be the cause: {text}"
        );
        // The refusal of the OTHER role really happened and stays recorded.
        let refused = outcome.find("s3_switch", "refused_after_run_end");
        assert_eq!(refused.len(), 1);
        assert_eq!(refused[0]["track"], "haptic");
        // The direct path now records the torn-down target exactly as the
        // relay path does — this row is what the watch registration rides on.
        let torn = outcome.find("info", "s3_switch_target_torn_down");
        assert_eq!(torn.len(), 1, "the opened-then-torn-down target is recorded");
        assert_eq!(torn[0]["track"], "pc");
        assert_eq!(torn[0]["wire_track"], PC_CRITICAL);
        let fault = outcome.find("info", "s3_switch_target_terminal_fault");
        assert_eq!(fault.len(), 1);
        assert_eq!(fault[0]["track"], "pc");
        assert_eq!(fault[0]["wire_track"], PC_CRITICAL);
        assert_eq!(fault[0]["error_code"].as_u64().unwrap(), 16);
        assert_eq!(fault[0]["pending"], false);
        // Still no delivery loss: (d)'s invariant is unchanged by the verdict.
        let target_rx: Vec<_> = outcome
            .rows
            .iter()
            .filter(|row| row["role"] == "rx" && row["wire_track"] == PC_CRITICAL)
            .collect();
        assert_eq!(target_rx.len(), 2, "target objects must be rx-logged");
        outcome.assert_every_rx_object_is_terminated();
        let shutdown = outcome.shutdown();
        assert_eq!(shutdown["ending"], "error");
        assert_eq!(
            shutdown["detail"].as_str().expect("detail is a string"),
            text
        );
        assert_eq!(shutdown["s3_switch_refused_after_run_end"], 1);
        assert_eq!(shutdown["s3_event_queue_closed"], true);
        assert_eq!(shutdown["s3_drains_unjoined"], 0);
    }

    /// (h) The sender's OTHER run-ended site: the SUBSCRIBE landed while the
    /// namespace was draining after the run end. Identical wire code, so the
    /// real loop must treat it exactly like (c) — non-fatal, rows written, run
    /// ends by `s3_current_routes_fin`. This is the case that used to abort
    /// the receiver with `DoesNotExist`.
    #[tokio::test]
    async fn a_drain_window_refusal_is_not_fatal_in_the_real_loop() {
        let mut script = initial_calls(Vec::new());
        script.push(Call {
            track: PC_CRITICAL,
            before: Vec::new(),
            answer: Answer::Refuse(drain_window_refusal()),
        });
        script.push(Call {
            track: HAPTIC_ESSENTIAL,
            before: vec![
                Step::Fin(PC_NORMAL),
                Step::Fin(HAPTIC_FULL),
                Step::Yield(50),
            ],
            answer: Answer::Refuse(drain_window_refusal()),
        });
        let outcome = run_case("s3-loop-h", "30", "20", script).await;
        assert!(outcome.result.is_ok(), "{:?}", outcome.result);
        let refused = outcome.find("s3_switch", "refused_after_run_end");
        assert_eq!(refused.len(), 2);
        for row in &refused {
            assert_eq!(
                row["error_code"].as_u64().unwrap(),
                S3_RUN_ENDED_REQUEST_ERROR_CODE
            );
        }
        let shutdown = outcome.shutdown();
        assert_eq!(shutdown["ending"], "normal");
        assert_eq!(shutdown["detail"], "rule=s3_current_routes_fin");
        assert_eq!(shutdown["s3_switch_refused_after_run_end"], 1);
        outcome.assert_every_rx_object_is_terminated();
    }

    /// (e) A plain `DoesNotExist` (0x10) is NOT the run-ended signal and stays
    /// fatal, because the sender still answers 0x10 for real faults.
    #[tokio::test]
    async fn a_plain_does_not_exist_refusal_is_still_fatal() {
        let mut script = initial_calls(Vec::new());
        script.push(Call {
            track: PC_CRITICAL,
            before: Vec::new(),
            answer: Answer::Refuse(run_ended()),
        });
        // 0x10 IS retryable (announce/subscribe race), so the registered
        // `--s3-switch-retry-limit 2` makes three attempts before it settles.
        for _ in 0..3 {
            script.push(Call {
                track: HAPTIC_ESSENTIAL,
                before: Vec::new(),
                answer: Answer::Refuse(ServeError::Closed(u64::from(
                    RequestErrorCode::DoesNotExist,
                ))),
            });
        }
        let outcome = run_case("s3-loop-e", "30", "20", script).await;
        let error = outcome
            .result
            .as_ref()
            .expect_err("mixed refusal must stay fatal");
        assert!(
            format!("{error:#}").contains("S3 subscribe"),
            "unexpected error: {error:#}"
        );
        assert!(outcome.find("s3_switch", "refused_after_run_end").is_empty());
        let shutdown = outcome.shutdown();
        assert_eq!(shutdown["ending"], "error");
        assert_eq!(shutdown["s3_switch_refused_after_run_end"], 0);
        outcome.assert_every_rx_object_is_terminated();
    }

    /// (f) The sender completes at its LAST SLOT, so a run-ended refusal can
    /// legitimately be observed BEFORE the receiver's window end. With the
    /// window end 1 ms after the first object, the refusal here is observed
    /// long after it — so the case that actually matters is the one where the
    /// window end is not even derivable. Time is no longer part of the rule.
    #[tokio::test]
    async fn a_completion_refusal_without_a_derivable_window_end_is_not_fatal() {
        let mut script = initial_calls(Vec::new());
        script.push(Call {
            track: PC_CRITICAL,
            before: Vec::new(),
            answer: Answer::Refuse(run_ended()),
        });
        script.push(Call {
            track: HAPTIC_ESSENTIAL,
            before: vec![
                Step::Fin(PC_NORMAL),
                Step::Fin(HAPTIC_FULL),
                Step::Yield(50),
            ],
            answer: Answer::Refuse(run_ended()),
        });
        // No `--duration-s`: there is no window end at all, and the 12th
        // rework's time rule would have made this fatal.
        let outcome = run_case_without_duration("s3-loop-f", "20", script).await;
        assert!(outcome.result.is_ok(), "{:?}", outcome.result);
        let refused = outcome.find("s3_switch", "refused_after_run_end");
        assert_eq!(refused.len(), 2);
        for row in &refused {
            assert!(row["window_end_us"].is_null(), "no window end: {row}");
            assert_eq!(
                row["error_code"].as_u64().unwrap(),
                S3_RUN_ENDED_REQUEST_ERROR_CODE
            );
        }
        let shutdown = outcome.shutdown();
        assert_eq!(shutdown["ending"], "normal");
        assert!(shutdown["s3_window_end_us"].is_null());
        outcome.assert_every_rx_object_is_terminated();
    }

    /// (i) THE 14TH-REWORK P1-A REGRESSION, one case per session ending.
    ///
    /// The control loop's `select!` CONSUMES the session join handle. Before
    /// the fix the exit path then called `session_run.abort(); session_run
    /// .await;` unconditionally, and polling a completed `JoinHandle` panics
    /// ("polled after completion") — so an early session end killed the run
    /// BEFORE drain pass 2, the scheduler flush and the shutdown row. The
    /// pre-existing cases could never catch it, because they install a
    /// session task that never ends.
    ///
    /// Each case asserts the same three things:
    ///   * drain pass 2 RAN — `s3_event_queue_closed` is written only from
    ///     pass 2's `Disconnected` verdict, and the shutdown row that carries
    ///     it is emitted after the flush that follows pass 2;
    ///   * the shutdown row exists and its `detail` is the FIRST cause (the
    ///     session ending), not a later one;
    ///   * every rx object still has exactly one release/drop terminal.
    async fn session_ending_case(name: &str, session: SessionEnding) -> CaseOutcome {
        // The two initial subscribes deliver the exact pair that arms the
        // timeline; the forcing delay is far beyond the session end, so no
        // switch is ever requested and the ONLY thing that ends the loop is
        // the session arm.
        let script = initial_calls(vec![Step::Yield(50)]);
        let outcome = run_case_with_session(name, "5000", script, session).await;
        let error = outcome
            .result
            .as_ref()
            .expect_err("an early session end fails the run");
        let text = format!("{error:#}");
        assert!(
            text.contains("S3 session ended early"),
            "the first cause must be the session ending: {text}"
        );
        let shutdown = outcome.shutdown();
        assert_eq!(shutdown["ending"], "error");
        assert_eq!(
            shutdown["detail"].as_str().unwrap(),
            text,
            "the shutdown row must carry the first cause"
        );
        assert_eq!(
            shutdown["s3_event_queue_closed"], true,
            "drain pass 2 must have run and found the queue permanently empty: {shutdown}"
        );
        assert_eq!(shutdown["s3_drains_unjoined"], 0);
        outcome.assert_every_rx_object_is_terminated();
        outcome
    }

    #[tokio::test]
    async fn a_session_that_returns_normally_still_drains_and_writes_the_shutdown_row() {
        let outcome =
            session_ending_case("s3-loop-i1", SessionEnding::NormalAfterMs(120)).await;
        // `Ok(Ok(()))`: the loop reports the join result verbatim.
        assert!(
            outcome.shutdown()["detail"]
                .as_str()
                .unwrap()
                .contains("Ok(Ok(()))"),
            "{}",
            outcome.shutdown()
        );
    }

    #[tokio::test]
    async fn a_session_that_returns_an_error_still_drains_and_keeps_the_error() {
        let outcome = session_ending_case("s3-loop-i2", SessionEnding::ErrorAfterMs(120)).await;
        assert!(
            outcome.shutdown()["detail"]
                .as_str()
                .unwrap()
                .contains("scripted session failure"),
            "the session's own error text must survive: {}",
            outcome.shutdown()
        );
    }

    #[tokio::test]
    async fn a_panicking_session_task_still_drains_and_keeps_the_join_error() {
        let outcome = session_ending_case("s3-loop-i3", SessionEnding::PanicAfterMs(120)).await;
        assert!(
            outcome.shutdown()["detail"]
                .as_str()
                .unwrap()
                .contains("panic"),
            "the JoinError must survive: {}",
            outcome.shutdown()
        );
    }

    /// (j) THE 14TH-REWORK P1-D REGRESSION. In relay topology the downstream
    /// `Subscribed` is accepted before the upstream result is known, so the
    /// sender's run-ended `REQUEST_ERROR` can arrive AFTER `SUBSCRIBE_OK` as
    /// `PUBLISH_DONE(0x5343)`. The receiver used to see only a `Cancelled`
    /// terminal on a route that is not current, ignore it, and let the
    /// pending switch die on the 2 s effect timeout — a failed run for what
    /// is a normal end-of-run refusal. It must take the same non-fatal path a
    /// direct refusal takes.
    #[tokio::test]
    async fn a_pending_target_closed_with_the_run_ended_code_is_not_fatal() {
        let mut script = initial_calls(Vec::new());
        script.push(Call {
            track: PC_CRITICAL,
            before: Vec::new(),
            answer: Answer::Accept(Vec::new()),
        });
        script.push(Call {
            track: HAPTIC_ESSENTIAL,
            before: Vec::new(),
            // These run while the haptic SUBSCRIBE is still being answered,
            // so the events are queued and the control loop only sees them
            // once BOTH target roles are live and the request is pending —
            // exactly the state the relay race produces.
            answer: Answer::Accept(vec![
                Step::CloseError(PC_CRITICAL, S3_RUN_ENDED_REQUEST_ERROR_CODE),
                Step::Fin(PC_NORMAL),
                Step::Fin(HAPTIC_FULL),
                Step::Yield(50),
            ]),
        });
        let outcome = run_case("s3-loop-j", "30", "20", script).await;
        assert!(outcome.result.is_ok(), "{:?}", outcome.result);
        // Both target roles really were subscribed before the refusal.
        assert_eq!(
            outcome.calls,
            vec![PC_NORMAL, HAPTIC_FULL, PC_CRITICAL, HAPTIC_ESSENTIAL]
        );
        assert_eq!(outcome.find("s3_switch", "request").len(), 1);
        assert_eq!(
            outcome.find("s3_switch", "subscribe_ok").len(),
            2,
            "the relay case is only reachable after SUBSCRIBE_OK"
        );
        // ... and the switch never took effect.
        assert!(outcome.find("s3_switch", "apply").is_empty());
        let refused = outcome.find("s3_switch", "refused_after_run_end");
        assert_eq!(refused.len(), 1, "one row for the role that was refused");
        assert_eq!(refused[0]["track"], "pc");
        assert_eq!(refused[0]["wire_track"], PC_CRITICAL);
        assert_eq!(
            refused[0]["error_code"].as_u64().unwrap(),
            S3_RUN_ENDED_REQUEST_ERROR_CODE
        );
        assert_eq!(
            refused[0]["t_settled"].as_u64().unwrap(),
            refused[0]["t_refused"].as_u64().unwrap()
        );
        // The distinguishing additive row: this refusal arrived after the OK.
        let late = outcome.find("info", "s3_switch_refused_after_subscribe_ok");
        assert_eq!(late.len(), 1);
        assert_eq!(late[0]["wire_track"], PC_CRITICAL);
        // The sibling target was torn down, not claimed to have been refused.
        let torn = outcome.find("info", "s3_switch_target_torn_down");
        assert_eq!(torn.len(), 1);
        assert_eq!(torn[0]["track"], "haptic");
        assert_eq!(torn[0]["wire_track"], HAPTIC_ESSENTIAL);
        let shutdown = outcome.shutdown();
        assert_eq!(shutdown["ending"], "normal");
        assert_eq!(shutdown["detail"], "rule=s3_current_routes_fin");
        assert_eq!(shutdown["s3_switch_refused_after_run_end"], 1);
        assert_eq!(shutdown["s3_event_queue_closed"], true);
        assert_eq!(shutdown["s3_drains_unjoined"], 0);
        outcome.assert_every_rx_object_is_terminated();
    }

    /// (l) The same relay refusal, with the current routes' FINs already
    /// PROCESSED when it arrives. Nothing else ever re-checks the FIN rule,
    /// so without re-evaluating it at the moment the pending request is
    /// abandoned the run would sit idle until `--max-duration` and fail. The
    /// ordering is forced: the FINs are queued (and drained) while the target
    /// SUBSCRIBEs are still being answered.
    #[tokio::test]
    async fn a_relay_refusal_after_both_fins_still_ends_by_the_fin_rule() {
        let mut script = initial_calls(Vec::new());
        script.push(Call {
            track: PC_CRITICAL,
            before: Vec::new(),
            // No awaiting step before the writer is registered: the two
            // target opens run inside one `join!`, so an await here would
            // hand control to the haptic open before `pc-d6` exists.
            answer: Answer::Accept(Vec::new()),
        });
        script.push(Call {
            track: HAPTIC_ESSENTIAL,
            // The current routes end FIRST and their drain tasks enqueue both
            // `Ended` events during the sleep, ahead of the refusal below.
            before: vec![
                Step::Fin(PC_NORMAL),
                Step::Fin(HAPTIC_FULL),
                Step::Sleep(20),
                Step::Yield(50),
            ],
            answer: Answer::Accept(vec![
                Step::CloseError(PC_CRITICAL, S3_RUN_ENDED_REQUEST_ERROR_CODE),
                Step::Yield(50),
            ]),
        });
        let outcome = run_case("s3-loop-l", "30", "20", script).await;
        assert!(outcome.result.is_ok(), "{:?}", outcome.result);
        assert_eq!(outcome.find("s3_switch", "refused_after_run_end").len(), 1);
        let shutdown = outcome.shutdown();
        assert_eq!(shutdown["ending"], "normal");
        assert_eq!(
            shutdown["detail"], "rule=s3_current_routes_fin",
            "the run must end by the FIN rule, not by --max-duration"
        );
        assert_eq!(shutdown["s3_switch_refused_after_run_end"], 1);
        assert_eq!(shutdown["s3_event_queue_closed"], true);
        outcome.assert_every_rx_object_is_terminated();
    }

    /// (k) The condition is NARROW: any other terminal on a pending target
    /// route keeps the previous behaviour. A `DoesNotExist` (0x10) close is
    /// not the run-ended signal, so the pending switch still dies on the
    /// registered 2 s effect timeout and the run fails loudly.
    #[tokio::test]
    async fn a_pending_target_closed_with_another_code_still_fails_the_run() {
        let mut script = initial_calls(Vec::new());
        script.push(Call {
            track: PC_CRITICAL,
            before: Vec::new(),
            answer: Answer::Accept(Vec::new()),
        });
        script.push(Call {
            track: HAPTIC_ESSENTIAL,
            before: Vec::new(),
            answer: Answer::Accept(vec![
                Step::CloseError(
                    PC_CRITICAL,
                    u64::from(RequestErrorCode::DoesNotExist),
                ),
                Step::Fin(PC_NORMAL),
                Step::Fin(HAPTIC_FULL),
                Step::Yield(50),
            ]),
        });
        let outcome = run_case("s3-loop-k", "30", "20", script).await;
        let error = outcome
            .result
            .as_ref()
            .expect_err("a non-run-ended terminal must stay fatal");
        let text = format!("{error:#}");
        assert!(
            text.contains("S3 switch timeout"),
            "unexpected error: {text}"
        );
        assert!(outcome.find("s3_switch", "refused_after_run_end").is_empty());
        assert!(outcome
            .find("info", "s3_switch_refused_after_subscribe_ok")
            .is_empty());
        let shutdown = outcome.shutdown();
        assert_eq!(shutdown["ending"], "error");
        assert_eq!(shutdown["s3_switch_refused_after_run_end"], 0);
        assert_eq!(shutdown["s3_event_queue_closed"], true);
        outcome.assert_every_rx_object_is_terminated();
    }

    /// Both target subscriptions of one switch accepted with NOTHING scripted
    /// on them, so their terminals can be delivered later by the delayed task.
    fn accepted_switch_targets() -> Vec<Call> {
        vec![
            Call {
                track: PC_CRITICAL,
                before: Vec::new(),
                answer: Answer::Accept(Vec::new()),
            },
            Call {
                track: HAPTIC_ESSENTIAL,
                before: Vec::new(),
                answer: Answer::Accept(Vec::new()),
            },
        ]
    }

    /// When the delayed task closes the two target tracks.
    ///
    /// Why a delayed step and not a scripted one: a step inside an `Answer`
    /// runs while the control loop is blocked inside `request_s3_switch`, so
    /// the SECOND target's drain task does not exist yet and can never enqueue
    /// anything ahead of the loop's next poll. A delayed step runs on the
    /// current-thread test runtime only while the loop is PARKED and both
    /// drain tasks are alive, so closing the two tracks back-to-back (no
    /// `Yield` between them) wakes both drains in script order, both `Ended`
    /// events are enqueued before the loop is polled again, and the ARRIVAL
    /// ORDER under test is exactly the script order.
    ///
    /// 500 ms is after the switch request (tens of ms: the epoch is armed by
    /// the initial exact pair and the forced misses fire 20 ms after the
    /// controller activates) and well inside the registered 2 s effect
    /// timeout. A close that arrived before the request would panic on an
    /// unopened track, never pass silently.
    const TARGET_TERMINALS_AT_MS: u64 = 500;

    /// (m) 15th rework, P1 — the P1 sequence in its own role assignment: the
    /// HAPTIC target of a pending switch ends with a general error
    /// (`Closed(0x10)`) and the PC target is then closed with the sender's
    /// run-ended code. The run-ended close must NOT take the non-fatal path,
    /// because not every observed terminal of this request's targets was the
    /// run-ended code — the same rule `classify_switch_open_failure` applies
    /// to every failed role on the direct path. The run fails and the 0x10 is
    /// the recorded cause.
    #[tokio::test]
    async fn a_general_target_terminal_before_the_siblings_refusal_fails_the_run() {
        let mut script = initial_calls(Vec::new());
        script.extend(accepted_switch_targets());
        let outcome = run_case_delayed(
            "s3-loop-m",
            Some("30"),
            "20",
            script,
            vec![(
                TARGET_TERMINALS_AT_MS,
                vec![
                    Step::CloseError(
                        HAPTIC_ESSENTIAL,
                        u64::from(RequestErrorCode::DoesNotExist),
                    ),
                    Step::CloseError(PC_CRITICAL, S3_RUN_ENDED_REQUEST_ERROR_CODE),
                    Step::Fin(PC_NORMAL),
                    Step::Fin(HAPTIC_FULL),
                    Step::Yield(50),
                ],
            )],
        )
        .await;
        assert_eq!(
            outcome.calls,
            vec![PC_NORMAL, HAPTIC_FULL, PC_CRITICAL, HAPTIC_ESSENTIAL],
            "both targets must really have been subscribed"
        );
        assert_eq!(outcome.find("s3_switch", "subscribe_ok").len(), 2);
        assert!(outcome.find("s3_switch", "apply").is_empty());
        let error = outcome
            .result
            .as_ref()
            .expect_err("a general terminal on a pending target must fail the run");
        let text = format!("{error:#}");
        assert!(
            text.contains(HAPTIC_ESSENTIAL) && text.contains("error_code=16"),
            "the 0x10 terminal must be the recorded cause: {text}"
        );
        let fault = outcome.find("info", "s3_switch_target_terminal_fault");
        assert_eq!(fault.len(), 1, "one faulted target");
        assert_eq!(fault[0]["track"], "haptic");
        assert_eq!(fault[0]["wire_track"], HAPTIC_ESSENTIAL);
        assert_eq!(fault[0]["error_code"].as_u64().unwrap(), 16);
        assert_eq!(fault[0]["pending"], true);
        // The sibling's run-ended close was observed and REFUSED the non-fatal
        // path, so no refusal row and no counter.
        assert!(outcome.find("s3_switch", "refused_after_run_end").is_empty());
        assert!(outcome
            .find("info", "s3_switch_refused_after_subscribe_ok")
            .is_empty());
        let not_taken = outcome.find("info", "s3_switch_refusal_not_taken");
        assert_eq!(not_taken.len(), 1);
        assert_eq!(not_taken[0]["track"], "pc");
        assert_eq!(not_taken[0]["wire_track"], PC_CRITICAL);
        let shutdown = outcome.shutdown();
        assert_eq!(shutdown["ending"], "error");
        assert_eq!(
            shutdown["detail"].as_str().expect("detail is a string"),
            text,
            "the shutdown row must carry the 0x10 cause, not the FIN rule"
        );
        assert_eq!(shutdown["s3_switch_refused_after_run_end"], 0);
        assert_eq!(shutdown["s3_event_queue_closed"], true);
        assert_eq!(shutdown["s3_drains_unjoined"], 0);
        outcome.assert_every_rx_object_is_terminated();
    }

    /// (n) 15th rework, P1 — the REVERSE arrival order. The PC target's
    /// run-ended close arrives first, so the non-fatal refusal legitimately
    /// runs: it tears the haptic target down and abandons the request. The
    /// haptic target's `Closed(0x10)`, already enqueued by then, arrives with
    /// no pending request left to recognise it by. It must still fail the run
    /// and must still name the 0x10.
    ///
    /// Implemented as "the refusal is recorded first": the refusal row and its
    /// counter describe an observation that really happened (the sender did
    /// refuse that role with that code) and the root verifier pairs every such
    /// row with the sender's own refused row, so suppressing it afterwards
    /// would falsify the log. The VERDICT is what the fault changes.
    #[tokio::test]
    async fn a_general_target_terminal_after_the_siblings_refusal_still_fails_the_run() {
        let mut script = initial_calls(Vec::new());
        script.extend(accepted_switch_targets());
        let outcome = run_case_delayed(
            "s3-loop-n",
            Some("30"),
            "20",
            script,
            vec![(
                TARGET_TERMINALS_AT_MS,
                vec![
                    Step::CloseError(PC_CRITICAL, S3_RUN_ENDED_REQUEST_ERROR_CODE),
                    Step::CloseError(
                        HAPTIC_ESSENTIAL,
                        u64::from(RequestErrorCode::DoesNotExist),
                    ),
                    Step::Fin(PC_NORMAL),
                    Step::Fin(HAPTIC_FULL),
                    Step::Yield(50),
                ],
            )],
        )
        .await;
        let error = outcome
            .result
            .as_ref()
            .expect_err("a general terminal after the refusal must still fail the run");
        let text = format!("{error:#}");
        assert!(
            text.contains(HAPTIC_ESSENTIAL) && text.contains("error_code=16"),
            "the 0x10 terminal must be the recorded cause: {text}"
        );
        // The refusal itself really happened and is recorded as such.
        let refused = outcome.find("s3_switch", "refused_after_run_end");
        assert_eq!(refused.len(), 1);
        assert_eq!(refused[0]["track"], "pc");
        let torn = outcome.find("info", "s3_switch_target_torn_down");
        assert_eq!(torn.len(), 1);
        assert_eq!(torn[0]["wire_track"], HAPTIC_ESSENTIAL);
        // ... and the straggling terminal of the torn-down target is still
        // classified, with `pending` false because the gate had already
        // forgotten the request.
        let fault = outcome.find("info", "s3_switch_target_terminal_fault");
        assert_eq!(fault.len(), 1);
        assert_eq!(fault[0]["track"], "haptic");
        assert_eq!(fault[0]["error_code"].as_u64().unwrap(), 16);
        assert_eq!(fault[0]["pending"], false);
        let shutdown = outcome.shutdown();
        assert_eq!(
            shutdown["ending"], "error",
            "the FIN rule must not normalise a refused switch whose sibling failed"
        );
        assert_eq!(
            shutdown["detail"].as_str().expect("detail is a string"),
            text
        );
        assert_eq!(shutdown["s3_switch_refused_after_run_end"], 1);
        assert_eq!(shutdown["s3_event_queue_closed"], true);
        assert_eq!(shutdown["s3_drains_unjoined"], 0);
        outcome.assert_every_rx_object_is_terminated();
    }

    /// (q) 16th rework, P1-A — the SAME arrival order as (n), but the straggling
    /// terminal carries NO application code: a bare `ServeError::Cancel`, which
    /// is what a relay or session collapse looks like on the wire.
    ///
    /// The 15th rework faulted a torn-down ("watched") target only when a close
    /// CODE was present, because our own `discard_s3_subscription` surfaces as a
    /// code-less `Cancel` too. That guard silently dropped every code-less
    /// REMOTE failure and this run ended `normal`. The terminal is now separated
    /// from our own teardown by PROVENANCE: the drain task records whether the
    /// receiver had begun releasing the subscription at the instant it
    /// classified the terminal, and here it had not.
    #[tokio::test]
    async fn a_code_less_target_terminal_after_the_siblings_refusal_still_fails_the_run() {
        let mut script = initial_calls(Vec::new());
        script.extend(accepted_switch_targets());
        let outcome = run_case_delayed(
            "s3-loop-q",
            Some("30"),
            "20",
            script,
            vec![(
                TARGET_TERMINALS_AT_MS,
                vec![
                    Step::CloseError(PC_CRITICAL, S3_RUN_ENDED_REQUEST_ERROR_CODE),
                    // No code at all — the case the 15th rework dropped.
                    Step::Cancel(HAPTIC_ESSENTIAL),
                    Step::Fin(PC_NORMAL),
                    Step::Fin(HAPTIC_FULL),
                    Step::Yield(50),
                ],
            )],
        )
        .await;
        let error = outcome
            .result
            .as_ref()
            .expect_err("a code-less remote terminal must still fail the run");
        let text = format!("{error:#}");
        assert!(
            text.contains(HAPTIC_ESSENTIAL) && text.contains("error_code=none"),
            "the code-less terminal must be the recorded cause: {text}"
        );
        // The refusal itself really happened and stays recorded as an
        // observation; only the VERDICT changes.
        let refused = outcome.find("s3_switch", "refused_after_run_end");
        assert_eq!(refused.len(), 1);
        assert_eq!(refused[0]["track"], "pc");
        let torn = outcome.find("info", "s3_switch_target_torn_down");
        assert_eq!(torn.len(), 1);
        assert_eq!(torn[0]["wire_track"], HAPTIC_ESSENTIAL);
        let fault = outcome.find("info", "s3_switch_target_terminal_fault");
        assert_eq!(fault.len(), 1, "the code-less terminal must be faulted");
        assert_eq!(fault[0]["track"], "haptic");
        assert!(
            fault[0]["error_code"].is_null(),
            "no close code: {}",
            fault[0]
        );
        assert_eq!(fault[0]["pending"], false);
        // The additive field that records WHY it was faulted despite having no
        // code: the drain classified it before any release began.
        assert_eq!(fault[0]["terminal_source"], "Remote");
        let shutdown = outcome.shutdown();
        assert_eq!(
            shutdown["ending"], "error",
            "the FIN rule must not normalise a code-less target failure"
        );
        assert_eq!(
            shutdown["detail"].as_str().expect("detail is a string"),
            text
        );
        assert_eq!(shutdown["s3_switch_refused_after_run_end"], 1);
        assert_eq!(shutdown["s3_event_queue_closed"], true);
        assert_eq!(shutdown["s3_drains_unjoined"], 0);
        outcome.assert_every_rx_object_is_terminated();
    }

    /// (o) The non-fatal path is unchanged when EVERY observed terminal of the
    /// request's targets is the run-ended code — including the second one,
    /// which arrives after the request was abandoned and is therefore matched
    /// by the watch list rather than by the gate. Case (j) covers the
    /// one-terminal form; this is the two-terminal form.
    #[tokio::test]
    async fn both_target_terminals_with_the_run_ended_code_stay_non_fatal() {
        let mut script = initial_calls(Vec::new());
        script.extend(accepted_switch_targets());
        let outcome = run_case_delayed(
            "s3-loop-o",
            Some("30"),
            "20",
            script,
            vec![(
                TARGET_TERMINALS_AT_MS,
                vec![
                    Step::CloseError(PC_CRITICAL, S3_RUN_ENDED_REQUEST_ERROR_CODE),
                    Step::CloseError(HAPTIC_ESSENTIAL, S3_RUN_ENDED_REQUEST_ERROR_CODE),
                    Step::Fin(PC_NORMAL),
                    Step::Fin(HAPTIC_FULL),
                    Step::Yield(50),
                ],
            )],
        )
        .await;
        assert!(outcome.result.is_ok(), "{:?}", outcome.result);
        assert!(
            outcome
                .find("info", "s3_switch_target_terminal_fault")
                .is_empty(),
            "the run-ended code is never a fault"
        );
        assert_eq!(outcome.find("s3_switch", "refused_after_run_end").len(), 1);
        let shutdown = outcome.shutdown();
        assert_eq!(shutdown["ending"], "normal");
        assert_eq!(shutdown["detail"], "rule=s3_current_routes_fin");
        assert_eq!(shutdown["s3_switch_refused_after_run_end"], 1);
        assert_eq!(shutdown["s3_event_queue_closed"], true);
        outcome.assert_every_rx_object_is_terminated();
    }

    /// (p) A terminal with NO application code on a pending target — a generic
    /// `Cancel`, i.e. a teardown or a session collapse rather than an answer
    /// from the sender. It is faulted (the subscription was live, so the
    /// terminal is the peer's) but, with no sibling refusal to bury it, the
    /// run still dies exactly as before on the registered 2 s effect timeout,
    /// and that remains the recorded first cause.
    #[tokio::test]
    async fn a_pending_target_cancelled_without_a_code_still_dies_on_the_effect_timeout() {
        let mut script = initial_calls(Vec::new());
        script.push(Call {
            track: PC_CRITICAL,
            before: Vec::new(),
            answer: Answer::Accept(Vec::new()),
        });
        script.push(Call {
            track: HAPTIC_ESSENTIAL,
            before: Vec::new(),
            answer: Answer::Accept(vec![
                Step::Cancel(PC_CRITICAL),
                Step::Fin(PC_NORMAL),
                Step::Fin(HAPTIC_FULL),
                Step::Yield(50),
            ]),
        });
        let outcome = run_case("s3-loop-p", "30", "20", script).await;
        let error = outcome
            .result
            .as_ref()
            .expect_err("a code-less terminal on a pending target must stay fatal");
        let text = format!("{error:#}");
        assert!(
            text.contains("S3 switch timeout"),
            "the registered effect timeout must still be the cause: {text}"
        );
        let fault = outcome.find("info", "s3_switch_target_terminal_fault");
        assert_eq!(fault.len(), 1, "the terminal is still recorded");
        assert_eq!(fault[0]["track"], "pc");
        assert_eq!(fault[0]["end"], "Cancelled");
        assert!(
            fault[0]["error_code"].is_null(),
            "a `Cancel` carries no application code"
        );
        assert_eq!(fault[0]["pending"], true);
        assert!(outcome.find("s3_switch", "refused_after_run_end").is_empty());
        let shutdown = outcome.shutdown();
        assert_eq!(shutdown["ending"], "error");
        assert_eq!(shutdown["s3_switch_refused_after_run_end"], 0);
        assert_eq!(shutdown["s3_event_queue_closed"], true);
        outcome.assert_every_rx_object_is_terminated();
    }

    /// (g) The registered 2 s switch effect timeout still poisons the gate: a
    /// role that never answers fails the run loudly.
    #[tokio::test]
    async fn the_registered_effect_timeout_still_poisons_the_gate() {
        let mut script = initial_calls(Vec::new());
        script.push(Call {
            track: PC_CRITICAL,
            before: Vec::new(),
            answer: Answer::Hang,
        });
        script.push(Call {
            track: HAPTIC_ESSENTIAL,
            before: Vec::new(),
            answer: Answer::Accept(Vec::new()),
        });
        let outcome = run_case("s3-loop-g", "30", "20", script).await;
        let error = outcome
            .result
            .as_ref()
            .expect_err("effect timeout must fail the run");
        let text = format!("{error:#}");
        assert!(
            text.contains("did not complete before the effect timeout") && text.contains("gate:"),
            "unexpected error: {text}"
        );
        assert!(outcome.find("s3_switch", "refused_after_run_end").is_empty());
        assert_eq!(outcome.shutdown()["ending"], "error");
        outcome.assert_every_rx_object_is_terminated();
    }
}
