// moq_sender — Phase-4 MoQ publisher.
//
// One QUIC connection to a relay carries TWO tracks (pc, haptic) as independent
// tracks. B1/S1 preserve the long-lived equal-priority mapping.
// M1 changes only PC to frame-per-subgroup. S2 keeps that mapping and adds
// static publisher priorities; its PC DELIVERY_TIMEOUT is requested by the
// receiver and repeated here only for frozen metadata agreement.
// The v5 generation uses PC 30 Hz and haptic 90 Hz with exact 1:3 rational
// anchors. `frame` sends one object per logical item; B1 `equal-chunk` is a
// negative ablation with fixed-size objects and logical JSONL rows.
//
// Namespace == run_id, so concurrent/repeat runs never collide on the relay.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::borrow::Cow;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
use moq_native_ietf::{quic, tls};
use moq_transport::{
    coding::TrackNamespace,
    serve::Tracks,
    session::{DataPriorityMapping, Session, SessionConfig},
};
use tokio::signal::unix::{signal, SignalKind};
use tokio::time::{sleep_until, Instant};
use url::Url;

use skew_moq::s3_producer::SubscriptionProducerRegistry;
use skew_moq::s3_sender::{
    run_namespace as run_s3_namespace, AcceptRouteMap, SenderContext as S3SenderContext,
};
use skew_moq::*;

/// Hard budget for the accept finalizer: quiesce producers, close the tap,
/// drain, join. Bounded on purpose — a wedged drain must never stall a matrix
/// run. `AcceptTrace::shutdown` adds a small join guard on top.
const ACCEPT_DRAIN_BUDGET: Duration = Duration::from_secs(2);

/// Budget for joining the aborted session / namespace tasks. These must be
/// gone before the tap closes (A2-c R2), but a stuck one must not wedge us.
const PRODUCER_JOIN_BUDGET: Duration = Duration::from_secs(2);

/// Last-resort watchdog. If the finalizer itself wedges — e.g. inside a
/// synchronous sink write, which `abort` cannot preempt — exit anyway so the
/// runner's `pkill` + `wait` cannot hang forever. Deliberately much larger than
/// the sum of the budgets above, so it only fires on a real defect. It does not
/// touch the logger mutex (it may be the thing that is stuck); every tx record
/// was already flushed at write time, so the A3 no-truncation guarantee holds.
const FINALIZE_WATCHDOG: Duration = Duration::from_secs(20);

/// How this run ended. Every variant goes through the same finalizer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ending {
    Normal,
    Signal,
    Error,
}

impl Ending {
    fn as_str(self) -> &'static str {
        match self {
            Ending::Normal => "normal",
            Ending::Signal => "signal",
            Ending::Error => "error",
        }
    }
}

/// The long-lived transport tasks that can invoke accept callbacks. They must
/// be aborted **and joined** before the accept tap is closed.
///
/// Each handle is an `Option` so that a `select!` arm which already drove one to
/// completion can `take()` it and hand the outcome to the finalizer. Re-polling
/// a finished `tokio::task::JoinHandle` panics, so the finalizer must never
/// await a handle whose result was already observed.
type SessionJoinHandle =
    tokio::task::JoinHandle<std::result::Result<(), moq_transport::session::SessionError>>;
type NamespaceJoinHandle = tokio::task::JoinHandle<anyhow::Result<()>>;

struct Producers {
    session_run: Option<SessionJoinHandle>,
    ns_task: Option<NamespaceJoinHandle>,
    /// Set when a `select!` arm consumed the corresponding handle.
    session_seen: Option<JoinOutcome>,
    ns_seen: Option<JoinOutcome>,
}

/// Re-armable wait for the shutdown signal, usable in more than one `select!`.
async fn wait_signal(rx: &mut tokio::sync::watch::Receiver<bool>) {
    loop {
        if *rx.borrow_and_update() {
            return;
        }
        if rx.changed().await.is_err() {
            // Sender gone: never fires again.
            std::future::pending::<()>().await;
        }
    }
}

/// C3 single-track baseline selector. Identical surface to
/// `webrtc_sender.py --tracks {both,pc,haptic}`.
///
/// Both tracks are always created and announced, so the namespace/track
/// structure — and therefore the subscriber — is unchanged in every mode. Only
/// the generation loop is gated: a disabled track has its subgroups writer
/// dropped immediately, so it puts **zero** objects (and zero subgroup streams)
/// on the wire and the subscriber sees a clean end-of-track instead of waiting.
#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum TrackSel {
    Both,
    Pc,
    Haptic,
}

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

    fn pc_frame_subgroups(self) -> bool {
        matches!(self, Self::M1 | Self::S2 | Self::S2Eq | Self::S3)
    }

    fn pc_priority(self) -> u8 {
        if matches!(self, Self::S2 | Self::S3) {
            1
        } else {
            128
        }
    }

    fn haptic_priority(self) -> u8 {
        if matches!(self, Self::S2 | Self::S3) {
            0
        } else {
            128
        }
    }
}

impl TrackSel {
    fn as_str(self) -> &'static str {
        match self {
            TrackSel::Both => "both",
            TrackSel::Pc => "pc",
            TrackSel::Haptic => "haptic",
        }
    }
    fn pc_on(self) -> bool {
        matches!(self, TrackSel::Both | TrackSel::Pc)
    }
    fn haptic_on(self) -> bool {
        matches!(self, TrackSel::Both | TrackSel::Haptic)
    }
}

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "https://10.0.0.2:4443")]
    relay: Url,
    #[arg(long)]
    run_id: String,
    #[arg(long)]
    out: PathBuf,
    #[arg(long)]
    duration: f64,
    /// PC tier frames dir (*.bin). Mutually exclusive with --dummy-size.
    #[arg(long)]
    frames_dir: Option<String>,
    #[arg(long)]
    dummy_size: Option<usize>,
    #[arg(long, default_value_t = 2)]
    tier: u16,
    #[arg(long)]
    haptic_wav: String,
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
    /// Forwarding structure of the arm (log schema 4). Required, not defaulted
    /// and not inferable here: the sender dials one address either way, so only
    /// the runner knows whether that address is a relay or the receiver itself.
    #[arg(long, value_enum)]
    topology: Topology,
    #[arg(long)]
    chunk_bytes: usize,
    /// Read workloads, report object-rate/overhead estimates, and exit without
    /// changing external network state.
    #[arg(long)]
    preflight_only: bool,
    /// Equal-chunk debug only: permit per-object accept records. Disabled in
    /// formal runs because tens of thousands of rows/s can dominate timing.
    #[arg(long)]
    chunk_trace: bool,
    /// Delay before workload starts so the subscriber can attach.
    #[arg(long, default_value_t = 1.0)]
    warmup: f64,
    /// Extra time to keep the session up after sending, to drain backlog.
    #[arg(long, default_value_t = 10.0)]
    drain_timeout: f64,
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
    /// priority. legacy-v1 reproduces existing evidence; moqt-v2 implements
    /// MoQT's lower-number-first semantics.
    #[arg(long, default_value = "legacy-v1")]
    data_priority_mapping: DataPriorityMapping,
    /// A2 instrumentation: emit role:"accept" records. These carry the time at
    /// which the QUIC stack accepted the object's last payload byte — a
    /// transport-accept time, not an on-the-wire time.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    accept_trace: bool,
    /// Bounded queue for accept records; overflow is dropped and counted.
    #[arg(long, default_value_t = 65536)]
    accept_trace_capacity: usize,
    /// C3: which tracks to generate. `both` is byte-identical to the pre-C3
    /// behaviour; `pc`/`haptic` are the single-track combination-penalty
    /// baselines (the other loop never runs).
    #[arg(long, value_enum, default_value_t = TrackSel::Both)]
    tracks: TrackSel,
    /// Phase-4 transport arm. B1/S1 preserve the historical wire mapping.
    #[arg(long, value_enum, default_value_t = Arm::B1)]
    arm: Arm,
    /// S2 PC DELIVERY_TIMEOUT, repeated on TX for frozen metadata agreement.
    /// The receiver places the actual parameter on the PC SUBSCRIBE.
    #[arg(long)]
    pc_delivery_timeout_ms: Option<u64>,
    /// S3 Recovery/d7 frame directory. Required only by --arm s3.
    #[arg(long)]
    s3_recovery_frames_dir: Option<String>,
    /// S3 Haptic-Critical/d6 frame directory. Required only by --arm s3.
    #[arg(long)]
    s3_critical_frames_dir: Option<String>,
    /// Positive bound for closing a subscription producer after its source
    /// reaches run end. Explicit because no production S3 default is frozen.
    #[arg(long)]
    s3_producer_shutdown_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default)]
struct TrackRunStats {
    logical_generated: u64,
    chunks_sent: u64,
    source_payload_bytes: u64,
    padding_bytes: u64,
    period: PeriodStats,
}

#[derive(Debug, Clone, Copy)]
struct Preflight {
    frame_min: usize,
    /// Exact integer inputs to the registered `S_bytes` rule. `frame_mean`
    /// stays for the human-readable preflight line only.
    frame_sum: usize,
    frame_count: usize,
    frame_mean: f64,
    frame_max: usize,
    chunks_min: usize,
    chunks_mean: f64,
    chunks_max: usize,
    pc_objects_per_s: f64,
    haptic_objects_per_s: f64,
    source_payload_mbps: f64,
    application_object_mbps: f64,
    header_bytes_per_s: f64,
    header_overhead_ratio: f64,
}

fn preflight(frames: &[Vec<u8>], args: &Args) -> Preflight {
    let mut sizes: Vec<usize> = frames.iter().map(Vec::len).collect();
    sizes.sort_unstable();
    let frame_sum: usize = sizes.iter().sum();
    let frame_count = sizes.len();
    let frame_mean = frame_sum as f64 / frame_count as f64;
    let chunks: Vec<usize> = sizes
        .iter()
        .map(|&size| match args.payload_mode {
            PayloadMode::Frame => 1,
            PayloadMode::EqualChunk => size.div_ceil(args.chunk_bytes).max(1),
        })
        .collect();
    let chunks_sum: usize = chunks.iter().sum();
    let chunks_mean = chunks_sum as f64 / chunks.len() as f64;
    let pc_objects_per_s = chunks_mean * args.pc_rate_hz as f64;
    let haptic_objects_per_s = args.haptic_rate_hz as f64;
    let source_haptic_bytes_per_s =
        PCM_SAMPLE_RATE_HZ as f64 * PCM_BYTES_PER_SAMPLE as f64;
    let source_payload_bytes_per_s =
        frame_mean * args.pc_rate_hz as f64 + source_haptic_bytes_per_s;
    let application_bytes_per_s = match args.payload_mode {
        PayloadMode::Frame => {
            source_payload_bytes_per_s
                + (pc_objects_per_s + haptic_objects_per_s) * HDR as f64
        }
        PayloadMode::EqualChunk => {
            (pc_objects_per_s + haptic_objects_per_s)
                * (HDR + CHUNK_HDR + args.chunk_bytes) as f64
        }
    };
    let header_bytes_per_s = match args.payload_mode {
        PayloadMode::Frame => (pc_objects_per_s + haptic_objects_per_s) * HDR as f64,
        PayloadMode::EqualChunk => {
            (pc_objects_per_s + haptic_objects_per_s) * (HDR + CHUNK_HDR) as f64
        }
    };
    Preflight {
        frame_min: *sizes.first().unwrap(),
        frame_sum,
        frame_count,
        frame_mean,
        frame_max: *sizes.last().unwrap(),
        chunks_min: *chunks.iter().min().unwrap(),
        chunks_mean,
        chunks_max: *chunks.iter().max().unwrap(),
        pc_objects_per_s,
        haptic_objects_per_s,
        source_payload_mbps: source_payload_bytes_per_s * 8.0 / 1e6,
        application_object_mbps: application_bytes_per_s * 8.0 / 1e6,
        header_bytes_per_s,
        header_overhead_ratio: header_bytes_per_s / application_bytes_per_s,
    }
}

fn preflight_info(p: Preflight) -> String {
    format!(
        "\"event\":\"preflight\",\"frame_bytes_min\":{},\"frame_bytes_mean\":{:.3},\"frame_bytes_max\":{},\"chunks_per_frame_min\":{},\"chunks_per_frame_mean\":{:.3},\"chunks_per_frame_max\":{},\"pc_objects_per_s\":{:.3},\"haptic_objects_per_s\":{:.3},\"total_objects_per_s\":{:.3},\"source_payload_mbps\":{:.6},\"application_object_mbps\":{:.6},\"header_bytes_per_s\":{:.3},\"header_overhead_ratio\":{:.9},\"throughput_warning\":{}",
        p.frame_min,
        p.frame_mean,
        p.frame_max,
        p.chunks_min,
        p.chunks_mean,
        p.chunks_max,
        p.pc_objects_per_s,
        p.haptic_objects_per_s,
        p.pc_objects_per_s + p.haptic_objects_per_s,
        p.source_payload_mbps,
        p.application_object_mbps,
        p.header_bytes_per_s,
        p.header_overhead_ratio,
        p.pc_objects_per_s >= 50_000.0,
    )
}

/// The registered `S_bytes` rule, in exact integer arithmetic.
///
/// `S_bytes = floor(sum/count + 1/2) = (2*sum + count) / (2*count)`.
///
/// Computed from the integer byte sum and frame count rather than from the
/// `f64` mean: the runners derive the same number independently, and a float
/// intermediate makes the two agree only up to `f64` precision and rounding
/// mode. They must agree exactly, because the analyzer rejects a run whose TX
/// and RX metadata disagree on `S_bytes` -- that is what blocked every gate-6
/// pair. Same rule as `scripts/registered_s_bytes.py`.
fn registered_s_bytes(frame_sum: usize, frame_count: usize) -> u64 {
    assert!(frame_count > 0, "registered_s_bytes needs at least one frame");
    ((2 * frame_sum as u128 + frame_count as u128) / (2 * frame_count as u128)) as u64
}

/// Turn the two clock readings that bracket `Instant::now()` into the recorded
/// epoch and its capture span.
///
/// The scheduling base is an `Instant`, which carries no microsecond value, so
/// the run's epoch has to be read from the log clock either side of it. The
/// true monotonic microsecond of the base lies somewhere in between. Recording
/// the **earlier** reading keeps the epoch at or before the base every deadline
/// `t0 + pts_k + W_obs` is hung off, so a deadline can only be placed early --
/// conservative -- never late. The span is the width of that bracket, and the
/// analyser refuses a run whose span exceeds the registered bound
/// (`MAX_T0_CAPTURE_SPAN_US`, 설계개정 §11.4); a preemption between the two
/// reads is a scheduling quantum, not a syscall (Codex 88차 P1-3).
///
/// A backwards pair is refused rather than saturated. `saturating_sub` turned a
/// violated monotonic-clock invariant into `0` -- the value that means "perfect
/// capture" -- so the one reading that proves the clock cannot be trusted would
/// have produced the strongest possible claim about it (Codex 89차 P1). Failing
/// here costs one run; recording it costs a run that looks ideal and is not.
fn epoch_record(before_us: u64, after_us: u64) -> Result<(u64, u64)> {
    let span = after_us.checked_sub(before_us).with_context(|| {
        format!(
            "the log clock went backwards between the two epoch reads \
             ({before_us} then {after_us}); the monotonic clock invariant \
             every recorded time depends on does not hold"
        )
    })?;
    Ok((before_us, span))
}

fn json_f64(value: f64) -> String {
    if value.is_finite() {
        format!("{value:.9}")
    } else {
        "null".to_string()
    }
}

fn phase4_transport(args: &Args) -> Result<Option<Phase4TransportMeta>> {
    if args.arm != Arm::B1 && args.payload_mode != PayloadMode::Frame {
        anyhow::bail!(
            "--arm {} requires --payload-mode frame; equal-chunk is a B1 negative ablation only",
            args.arm.as_str()
        );
    }
    if matches!(args.arm, Arm::S2 | Arm::S2Eq | Arm::S3)
        && args.data_priority_mapping != DataPriorityMapping::MoqtV2
    {
        anyhow::bail!(
            "--arm {} requires --data-priority-mapping moqt-v2 in the v5 generation",
            args.arm.as_str()
        );
    }
    match args.arm {
        Arm::B1 | Arm::S1 => {
            if args.pc_delivery_timeout_ms.is_some() {
                anyhow::bail!("PC delivery timeout requires --arm s2");
            }
            Ok(None)
        }
        Arm::M1 => {
            if args.pc_delivery_timeout_ms.is_some() {
                anyhow::bail!("M1 is mapping-only and forbids PC delivery timeout");
            }
            Ok(Some(Phase4TransportMeta {
                arm: "m1",
                pc_subgroup_mapping: "frame-per-subgroup",
                pc_publisher_priority: 128,
                haptic_publisher_priority: 128,
                publisher_priority_profile: "equal-128",
                data_priority_mapping: args.data_priority_mapping.as_str(),
                pc_delivery_timeout_ms: None,
            }))
        }
        Arm::S2 | Arm::S2Eq | Arm::S3 => {
            let timeout = args.pc_delivery_timeout_ms.with_context(|| {
                format!(
                    "--arm {} requires --pc-delivery-timeout-ms",
                    args.arm.as_str()
                )
            })?;
            if timeout == 0 {
                anyhow::bail!("--pc-delivery-timeout-ms must be greater than zero");
            }
            if args.arm == Arm::S3 && timeout != 67 {
                anyhow::bail!("--arm s3 inherits the frozen 67ms PC delivery timeout");
            }
            Ok(Some(Phase4TransportMeta {
                arm: args.arm.as_str(),
                pc_subgroup_mapping: "frame-per-subgroup",
                pc_publisher_priority: args.arm.pc_priority(),
                haptic_publisher_priority: args.arm.haptic_priority(),
                publisher_priority_profile: if args.arm == Arm::S2Eq {
                    "equal-128"
                } else {
                    "relative-haptic0-pc1"
                },
                data_priority_mapping: args.data_priority_mapping.as_str(),
                pc_delivery_timeout_ms: Some(timeout),
            }))
        }
    }
}

fn validate_s3_args(args: &Args) -> Result<()> {
    let supplied = args.s3_recovery_frames_dir.is_some()
        || args.s3_critical_frames_dir.is_some()
        || args.s3_producer_shutdown_timeout_ms.is_some();
    if args.arm != Arm::S3 {
        if supplied {
            anyhow::bail!("S3 frame/lifecycle options require --arm s3");
        }
        return Ok(());
    }
    if args.tracks != TrackSel::Both {
        anyhow::bail!("--arm s3 requires --tracks both");
    }
    if args.tier != 2 {
        anyhow::bail!("--arm s3 Normal must preserve header tier 2");
    }
    if args.frames_dir.is_none() || args.dummy_size.is_some() {
        anyhow::bail!("--arm s3 requires --frames-dir d8 and forbids --dummy-size");
    }
    args.s3_recovery_frames_dir
        .as_ref()
        .context("--arm s3 requires --s3-recovery-frames-dir d7")?;
    args.s3_critical_frames_dir
        .as_ref()
        .context("--arm s3 requires --s3-critical-frames-dir d6")?;
    let timeout = args
        .s3_producer_shutdown_timeout_ms
        .context("--arm s3 requires --s3-producer-shutdown-timeout-ms")?;
    if timeout == 0 {
        anyhow::bail!("--s3-producer-shutdown-timeout-ms must be greater than zero");
    }
    Ok(())
}

struct S3AcceptSink {
    logger: Arc<Mutex<JsonlLogger>>,
    routes: AcceptRouteMap,
}

impl AcceptSink for S3AcceptSink {
    fn write_accept(&mut self, rec: &AcceptRec) -> std::io::Result<()> {
        let route = self
            .routes
            .lock()
            .map_err(|_| std::io::Error::other("S3 accept route map poisoned"))?
            .get(&rec.track_alias)
            .copied()
            .ok_or_else(|| std::io::Error::other("unknown S3 accept track alias"))?;
        if rec.track.as_str() != route.route.name {
            return Err(std::io::Error::other(
                "S3 accept track name/alias route mismatch",
            ));
        }
        self.logger
            .lock()
            .map_err(|_| std::io::Error::other("TX logger poisoned"))?
            .try_log_accept_s3(
                route.role,
                route.route,
                rec.group_id,
                rec.subgroup_id,
                rec.object_id,
                rec.t_accept,
                rec.size,
            )
    }

    fn flush_accept(&mut self) -> std::io::Result<()> {
        self.logger
            .lock()
            .map_err(|_| std::io::Error::other("TX logger poisoned"))?
            .try_flush()
    }
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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();
    let args = Args::parse();
    let ratio = validate_v5_rates(args.pc_rate_hz, args.haptic_rate_hz)?;
    anyhow::ensure!(args.chunk_bytes > 0, "--chunk-bytes must be positive");
    anyhow::ensure!(
        args.chunk_bytes <= u32::MAX as usize,
        "--chunk-bytes exceeds the wire field"
    );
    let phase4_transport = phase4_transport(&args)?;
    validate_s3_args(&args)?;

    // ---- Workload ----
    let frames: Vec<Vec<u8>> = match (&args.frames_dir, args.dummy_size) {
        (Some(dir), _) => load_frames_checked(dir, args.representation)?,
        (None, Some(n)) => vec![vec![0u8; n]],
        (None, None) => anyhow::bail!("need --frames-dir or --dummy-size"),
    };
    let pcm = load_haptic_pcm(&args.haptic_wav)?;
    let pf = preflight(&frames, &args);
    let s_bytes = registered_s_bytes(pf.frame_sum, pf.frame_count);
    let frames = Arc::new(frames);
    let pcm = Arc::new(pcm);
    let s3_frames = if args.arm == Arm::S3 {
        Some((
            Arc::new(load_frames_checked(
                args.s3_recovery_frames_dir
                    .as_deref()
                    .expect("validated S3 recovery frames"),
                args.representation,
            )?),
            Arc::new(load_frames_checked(
                args.s3_critical_frames_dir
                    .as_deref()
                    .expect("validated S3 critical frames"),
                args.representation,
            )?),
        ))
    } else {
        None
    };
    let haptic_src = std::path::Path::new(&args.haptic_wav)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_string();

    println!(
        "[tx] frames={} S={}B haptic={}B pc={}Hz haptic={}Hz mode={} chunk={}B tracks={} arm={} -> {}",
        frames.len(), s_bytes, pcm.len(), args.pc_rate_hz, args.haptic_rate_hz,
        args.payload_mode.as_str(), args.chunk_bytes, args.tracks.as_str(), args.arm.as_str(),
        args.out.display(),
    );
    println!(
        "[tx] preflight mean_frame={:.1}B mean_chunks={:.1} pc_objects/s={:.1} app={:.3}Mbps warning={}",
        pf.frame_mean, pf.chunks_mean, pf.pc_objects_per_s,
        pf.application_object_mbps, pf.pc_objects_per_s >= 50_000.0
    );

    let logger = Arc::new(Mutex::new(JsonlLogger::new(
        &args.out, &args.run_id, "moq", "tx", args.c_mbps, args.rtt_ms,
        args.jitter_ms, args.loss_pct, s_bytes, args.pc_rate_hz,
        args.haptic_rate_hz, args.seed,
        Some(args.duration), Some(&haptic_src), Some(args.tracks.as_str()),
        Some(TERM_PROTOCOL_V), None, phase4_transport,
        Some(V5Meta {
            payload_mode: args.payload_mode,
            representation: args.representation,
            topology: args.topology,
            chunk_bytes: args.chunk_bytes,
        }),
    )?));
    logger.lock().unwrap().log_info(&preflight_info(pf));
    if args.preflight_only {
        logger.lock().unwrap().try_log_info(
            "\"event\":\"shutdown\",\"ending\":\"preflight_only\",\"exit_code\":0"
        )?;
        println!("[tx] preflight-only complete -> {}", args.out.display());
        return Ok(());
    }

    // ---- A2 transport-accept tap ----
    // Installed before the session exists so no object can be forwarded before
    // the observer is live. The drain task owns all file I/O; the observer
    // itself only timestamps and try_sends (see skew_moq::AcceptTap).
    //
    // The captured time is when QUIC accepted the object's last payload byte —
    // a transport-accept time, not an on-the-wire time. It follows send
    // backpressure under congestion and degenerates to a handoff time
    // otherwise.
    let accept_trace_enabled = args.accept_trace
        && (args.payload_mode == PayloadMode::Frame || args.chunk_trace);
    let s3_accept_routes: AcceptRouteMap = Arc::new(Mutex::new(HashMap::new()));
    let accept_trace = if accept_trace_enabled {
        if args.arm == Arm::S3 {
            Some(AcceptTrace::install(
                args.accept_trace_capacity,
                Arc::new(Mutex::new(S3AcceptSink {
                    logger: logger.clone(),
                    routes: s3_accept_routes.clone(),
                })),
            )?)
        } else {
            Some(AcceptTrace::install(
                args.accept_trace_capacity,
                logger.clone(),
            )?)
        }
    } else {
        None
    };

    // 종료 내구성 (A2-c R1): 시그널은 여기서 프로세스를 끝내지 않는다. main 으로
    // 전달만 하고, 정상·시그널·오류 종료가 **모두 같은 finalizer** 를 통과한다.
    // 예전 구조는 시그널 태스크가 곧바로 process::exit(0) 을 불러서, 정상 경로가
    // stats 를 쓰기 전에 프로세스를 끝낼 수 있는 창이 있었다.
    let (sig_tx, mut sig_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(_) => return,
        };
        let mut intr = match signal(SignalKind::interrupt()) {
            Ok(s) => s,
            Err(_) => return,
        };
        tokio::select! {
            _ = term.recv() => {}
            _ = intr.recv() => {}
        }
        let _ = sig_tx.send(true);
    });

    // Everything below feeds the single finalizer at the end of `main`.
    let mut producers: Option<Producers> = None;
    let mut pc_stats = TrackRunStats::default();
    let mut hap_stats = TrackRunStats::default();
    let mut ending = Ending::Normal;

    // Fallible section. It must not use `?` to leave `main` — errors are
    // captured so the finalizer still runs (A2-c R1, Codex finding 5).
    let outcome: Result<()> = async {
        // ---- MoQ session ----
        let (sess, tp) = connect(&args.relay).await.context("connect relay")?;
        let config = SessionConfig {
            data_priority_mapping: args.data_priority_mapping,
            ..SessionConfig::default()
        };
        let (session, mut publisher, _sub) = Session::connect_with_config(sess, None, tp, config)
            .await
            .context("SETUP")?;

        // Register each task with the finalizer *immediately* after spawning it.
        // Registering both only after the track setup left a window in which a
        // `?` from `tracks_w.create(...)` would leave the session task spawned
        // but unknown to the finalizer, so it was neither joined nor accounted.
        producers = Some(Producers {
            session_run: Some(tokio::spawn(session.run())),
            ns_task: None,
            session_seen: None,
            ns_seen: None,
        });

        let namespace = TrackNamespace::from_utf8_path(&args.run_id);

        if args.arm == Arm::S3 {
            let warmup_us = Duration::from_secs_f64(args.warmup).as_micros() as u64;
            let duration_us = Duration::from_secs_f64(args.duration).as_micros() as u64;
            let anchor_us = now_us()
                .checked_add(warmup_us)
                .context("S3 run anchor overflow")?;
            let end_us = anchor_us
                .checked_add(duration_us)
                .context("S3 run end overflow")?;
            let (recovery_frames, critical_frames) =
                s3_frames.as_ref().expect("validated S3 frames");
            let registry = Arc::new(Mutex::new(SubscriptionProducerRegistry::new()));
            let context = Arc::new(S3SenderContext {
                clock: skew_moq::s3_producer::RunSlotClock::new(anchor_us),
                end_us,
                pc_rate_hz: args.pc_rate_hz,
                haptic_rate_hz: args.haptic_rate_hz,
                normal_frames: frames.clone(),
                recovery_frames: recovery_frames.clone(),
                critical_frames: critical_frames.clone(),
                haptic_pcm: pcm.clone(),
                logger: logger.clone(),
                shutdown_timeout: Duration::from_millis(
                    args.s3_producer_shutdown_timeout_ms
                        .expect("validated S3 shutdown timeout"),
                ),
            });
            let ns_publisher = publisher.clone();
            let ns_registry = registry.clone();
            let ns_routes = s3_accept_routes.clone();
            producers.as_mut().expect("registered above").ns_task = Some(tokio::spawn(
                run_s3_namespace(ns_publisher, namespace, context, ns_registry, ns_routes),
            ));

            let run_wait = Duration::from_micros(warmup_us.saturating_add(duration_us));
            let step: Result<()> = {
                let p = producers.as_mut().expect("producers set above");
                let sr = p.session_run.as_mut().expect("session handle present");
                let nt = p.ns_task.as_mut().expect("namespace handle present");
                let mut session_done = None;
                let mut ns_done = None;
                let outcome = tokio::select! {
                    _ = tokio::time::sleep(run_wait) => Ok(()),
                    _ = wait_signal(&mut sig_rx) => {
                        ending = Ending::Signal;
                        Ok(())
                    }
                    r = sr => {
                        session_done = Some(JoinOutcome::from_join_result(&r));
                        Err(anyhow::anyhow!("session ended during S3 send: {:?}", r))
                    }
                    r = nt => {
                        ns_done = Some(JoinOutcome::from_join_result(&r));
                        Err(anyhow::anyhow!("S3 namespace ended during send: {:?}", r))
                    }
                };
                if let Some(outcome) = session_done {
                    p.session_run = None;
                    p.session_seen = Some(outcome);
                }
                if let Some(outcome) = ns_done {
                    p.ns_task = None;
                    p.ns_seen = Some(outcome);
                }
                outcome
            };
            step?;

            if ending != Ending::Signal {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs_f64(args.drain_timeout)) => {}
                    _ = wait_signal(&mut sig_rx) => { ending = Ending::Signal; }
                }
            }
            let stats = registry
                .lock()
                .map_err(|_| anyhow::anyhow!("S3 producer registry poisoned"))?
                .stats()
                .map_err(|error| anyhow::anyhow!("read S3 producer stats: {error:?}"))?;
            pc_stats.logical_generated = stats
                .iter()
                .filter(|stats| stats.role == skew_moq::s3_switch::TrackRole::Pc)
                .map(|stats| stats.objects)
                .sum();
            pc_stats.chunks_sent = pc_stats.logical_generated;
            hap_stats.logical_generated = stats
                .iter()
                .filter(|stats| stats.role == skew_moq::s3_switch::TrackRole::Haptic)
                .map(|stats| stats.objects)
                .sum();
            hap_stats.chunks_sent = hap_stats.logical_generated;
            println!(
                "[tx] S3 generated pc={} haptic={} across {} subscription generations",
                pc_stats.logical_generated,
                hap_stats.logical_generated,
                stats.len()
            );
            return Ok(());
        }

        let (mut tracks_w, _req, tracks_r) = Tracks::new(namespace.clone()).produce();
        let pc_tw = tracks_w.create("pc").context("create pc track")?;
        let hap_tw = tracks_w.create("haptic").context("create haptic track")?;

        // Announce the namespace (serves subscribes on demand).
        let mut ns_pub = publisher.clone();
        producers.as_mut().expect("registered above").ns_task = Some(tokio::spawn(async move {
            ns_pub
                .publish_namespace(tracks_r)
                .await
                .map_err(anyhow::Error::from)
        }));
        // publisher no longer needed after cloning for the namespace.
        let _ = &mut publisher;

        // Warmup so the subscriber is attached before objects flow.
        tokio::time::sleep(Duration::from_secs_f64(args.warmup)).await;
        // A-4: the schedule's base and the log clock are two readings, so the
        // true monotonic microsecond of `anchor` lies between them. Record the
        // *earlier* one -- the recorded epoch is then never later than the base
        // every deadline is hung off, so deadlines can only be conservative --
        // and record the span so the analyser can refuse a capture that was
        // preempted (Codex 88차 P1-3). The earlier claim that the two differ by
        // "one syscall" was wrong: a preemption here is a scheduling quantum.
        let before_us = now_us();
        let anchor = Instant::now();
        let after_us = now_us();
        let (t0_us, capture_span_us) = epoch_record(before_us, after_us)?;
        logger
            .lock()
            .unwrap()
            .log_measurement_start(t0_us, capture_span_us)
            .context("failed to record the measurement epoch")?;
        let end = anchor + Duration::from_secs_f64(args.duration);

        // ---- PC loop: rational PC Hz, with the arm-specific subgroup map ----
        let pc_task = if args.tracks.pc_on() {
            let frames = frames.clone();
            let logger = logger.clone();
            let pc_rate_hz = args.pc_rate_hz;
            let tier = args.tier;
            let payload_mode = args.payload_mode;
            let chunk_bytes = args.chunk_bytes;
            let frame_subgroups = args.arm.pc_frame_subgroups();
            let priority = args.arm.pc_priority();
            let mut pc_sub = pc_tw.subgroups().context("pc subgroups")?;
            let mut long_sg = if frame_subgroups {
                None
            } else {
                Some(pc_sub.append(priority).context("pc append")?)
            };
            Some(tokio::spawn(async move {
                let mut i: u64 = 0;
                let mut stats = TrackRunStats::default();
                while Instant::now() < end {
                    sleep_until(anchor + Duration::from_nanos(deadline_ns(i, pc_rate_hz))).await;
                    if Instant::now() >= end {
                        break;
                    }
                    let pts = timestamp_us(i, pc_rate_hz);
                    let payload = &frames[(i as usize) % frames.len()];
                    let t_gen = now_us();
                    stats.period.observe(t_gen);
                    let object_payloads: Vec<Cow<'_, [u8]>> = match payload_mode {
                        PayloadMode::Frame => vec![Cow::Borrowed(payload.as_slice())],
                        PayloadMode::EqualChunk => equal_chunks(
                            payload,
                            i as u32,
                            chunk_bytes,
                        )?
                        .into_iter()
                        .map(Cow::Owned)
                        .collect(),
                    };
                    let mut frame_sg = if frame_subgroups {
                        Some(pc_sub.append(priority).context("pc frame append")?)
                    } else {
                        None
                    };
                    let mut frame_obj = None;
                    for object_payload in &object_payloads {
                        let sg = match frame_sg.as_mut() {
                            Some(sg) => sg,
                            None => long_sg.as_mut().expect("long subgroup exists"),
                        };
                        let hdr = pack_header(
                            TRACK_PC,
                            tier,
                            i as u32,
                            pts,
                            (i + 1) as u32,
                            t_gen,
                            object_payload.len() as u32,
                        );
                        let mut buf = Vec::with_capacity(HDR + object_payload.len());
                        buf.extend_from_slice(&hdr);
                        buf.extend_from_slice(object_payload.as_ref());
                        let (gid, sgid) = (sg.group_id, sg.subgroup_id);
                        let mut obj = sg.create(buf.len(), None).context("pc create")?;
                        let oid = obj.object_id;
                        obj.write(Bytes::from(buf)).context("pc write")?;
                        drop(obj);
                        if payload_mode == PayloadMode::Frame {
                            frame_obj = Some((gid, sgid, oid));
                        }
                    }
                    drop(frame_sg);
                    logger.lock().unwrap().log_tx(
                        "pc",
                        tier,
                        i as u32,
                        pts,
                        (i + 1) as u32,
                        payload.len(),
                        t_gen,
                        now_us(),
                        frame_obj,
                    );
                    stats.logical_generated += 1;
                    stats.chunks_sent += object_payloads.len() as u64;
                    stats.source_payload_bytes += payload.len() as u64;
                    if payload_mode == PayloadMode::EqualChunk {
                        stats.padding_bytes +=
                            (object_payloads.len() * chunk_bytes - payload.len()) as u64;
                    }
                    i += 1;
                }
                drop(long_sg);
                drop(pc_sub);
                Ok::<TrackRunStats, anyhow::Error>(stats)
            }))
        } else {
            drop(pc_tw.subgroups().context("pc subgroups")?);
            None
        };

        // ---- Haptic loop: rational 90 Hz with exact 3:1 anchors ----
        let hap_task = if args.tracks.haptic_on() {
            let pcm = pcm.clone();
            let logger = logger.clone();
            let pc_rate_hz = args.pc_rate_hz;
            let haptic_rate_hz = args.haptic_rate_hz;
            let payload_mode = args.payload_mode;
            let chunk_bytes = args.chunk_bytes;
            let priority = args.arm.haptic_priority();
            let mut hap_sub = hap_tw.subgroups().context("haptic subgroups")?;
            let mut sg = hap_sub.append(priority).context("haptic append")?;
            Some(tokio::spawn(async move {
                let mut k: u64 = 0;
                let mut stats = TrackRunStats::default();
                while Instant::now() < end {
                    sleep_until(
                        anchor + Duration::from_nanos(deadline_ns(k, haptic_rate_hz)),
                    )
                    .await;
                    if Instant::now() >= end {
                        break;
                    }
                    let (pts, event_id) = if k % ratio == 0 {
                        let fi = k / ratio;
                        (timestamp_us(fi, pc_rate_hz), (fi + 1) as u32)
                    } else {
                        (timestamp_us(k, haptic_rate_hz), 0u32)
                    };
                    let payload =
                        pcm_tick_payload(&pcm, k, PCM_SAMPLE_RATE_HZ, haptic_rate_hz)?;
                    let t_gen = now_us();
                    stats.period.observe(t_gen);
                    let object_payloads: Vec<Cow<'_, [u8]>> = match payload_mode {
                        PayloadMode::Frame => vec![Cow::Borrowed(payload.as_slice())],
                        PayloadMode::EqualChunk => equal_chunks(&payload, k as u32, chunk_bytes)?
                            .into_iter()
                            .map(Cow::Owned)
                            .collect(),
                    };
                    let mut frame_obj = None;
                    for object_payload in &object_payloads {
                        let hdr = pack_header(
                            TRACK_HAPTIC,
                            HAPTIC_TIER_FULL,
                            k as u32,
                            pts,
                            event_id,
                            t_gen,
                            object_payload.len() as u32,
                        );
                        let mut buf = Vec::with_capacity(HDR + object_payload.len());
                        buf.extend_from_slice(&hdr);
                        buf.extend_from_slice(object_payload.as_ref());
                        let (gid, sgid) = (sg.group_id, sg.subgroup_id);
                        let mut obj = sg.create(buf.len(), None).context("haptic create")?;
                        let oid = obj.object_id;
                        obj.write(Bytes::from(buf)).context("haptic write")?;
                        drop(obj);
                        if payload_mode == PayloadMode::Frame {
                            frame_obj = Some((gid, sgid, oid));
                        }
                    }
                    logger.lock().unwrap().log_tx(
                        "haptic",
                        HAPTIC_TIER_FULL,
                        k as u32,
                        pts,
                        event_id,
                        payload.len(),
                        t_gen,
                        now_us(),
                        frame_obj,
                    );
                    stats.logical_generated += 1;
                    stats.chunks_sent += object_payloads.len() as u64;
                    stats.source_payload_bytes += payload.len() as u64;
                    if payload_mode == PayloadMode::EqualChunk {
                        stats.padding_bytes +=
                            (object_payloads.len() * chunk_bytes - payload.len()) as u64;
                    }
                    k += 1;
                }
                drop(sg);
                drop(hap_sub);
                Ok::<TrackRunStats, anyhow::Error>(stats)
            }))
        } else {
            drop(hap_tw.subgroups().context("haptic subgroups")?);
            None
        };

        // Wait for both loops, a session failure, or the shutdown signal.
        // A disabled track contributes 0.
        //
        // A producer arm firing means that handle has been polled to
        // completion. Record the outcome and mark the handle consumed *before*
        // returning the error, so the finalizer joins the survivor only and
        // never re-polls a finished handle.
        let step: Result<()> = {
            let p = producers.as_mut().expect("producers set above");
            let sr = p.session_run.as_mut().expect("session handle present");
            let nt = p.ns_task.as_mut().expect("ns handle present");
            let mut session_done: Option<JoinOutcome> = None;
            let mut ns_done: Option<JoinOutcome> = None;
            let outcome = tokio::select! {
                r = async {
                    let pc = match pc_task {
                        Some(h) => h.await??,
                        None => TrackRunStats::default(),
                    };
                    let haptic = match hap_task {
                        Some(h) => h.await??,
                        None => TrackRunStats::default(),
                    };
                    Ok::<_, anyhow::Error>((pc, haptic))
                } => match r {
                    Ok((a, b)) => { pc_stats = a; hap_stats = b; Ok(()) }
                    Err(e) => Err(e),
                },
                _ = wait_signal(&mut sig_rx) => { ending = Ending::Signal; Ok(()) }
                r = sr => {
                    session_done = Some(JoinOutcome::from_join_result(&r));
                    Err(anyhow::anyhow!("session ended during send: {:?}", r))
                }
                r = nt => {
                    ns_done = Some(JoinOutcome::from_join_result(&r));
                    Err(anyhow::anyhow!("publish_namespace ended during send: {:?}", r))
                }
            };
            if let Some(o) = session_done {
                p.session_run = None;
                p.session_seen = Some(o);
            }
            if let Some(o) = ns_done {
                p.ns_task = None;
                p.ns_seen = Some(o);
            }
            outcome
        };
        step?;

        if ending == Ending::Signal {
            return Ok(());
        }
        println!(
            "[tx] sent pc={} haptic={} chunks={}/{}; draining {}s",
            pc_stats.logical_generated, hap_stats.logical_generated,
            pc_stats.chunks_sent, hap_stats.chunks_sent, args.drain_timeout
        );

        // Keep the session up so the relay forwards the backlog to the
        // subscriber — but let a signal cut the wait short, which is exactly
        // what the matrix runner's `pkill` does.
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs_f64(args.drain_timeout)) => {}
            _ = wait_signal(&mut sig_rx) => { ending = Ending::Signal; }
        }
        Ok(())
    }
    .await;

    if outcome.is_err() {
        ending = Ending::Error;
    }

    // ================= SINGLE FINALIZER (A2-c R1/R2) =================
    // Normal, signal, and error endings all arrive here, in this order:
    //   1. abort AND JOIN the producer tasks, so no forwarding future survives
    //      that could start a new accept callback;
    //   2. AcceptTrace::shutdown — confirm quiescence, close the tap, drain,
    //      join, and compute the conservation checks;
    //   3. write the stats record, the run counts, and the ending marker;
    //   4. flush, then return.
    // Producers are stopped *before* the tap closes; the previous order was the
    // reverse, which allowed callbacks after the snapshot.

    // Last-resort watchdog: bounds the finalizer even against a synchronous
    // wedge that `abort` cannot preempt.
    let watchdog = spawn_finalize_watchdog(FINALIZE_WATCHDOG);

    // 1. Producers first: abort, join, and RECORD the outcome. A detached
    //    handle after a join timeout would leave a task that can still invoke
    //    accept callbacks, so quiescence alone must never be treated as proof.
    let joins = match producers {
        Some(p) => {
            let session = join_producer(p.session_run, p.session_seen, PRODUCER_JOIN_BUDGET).await;
            let ns = join_producer(p.ns_task, p.ns_seen, PRODUCER_JOIN_BUDGET).await;
            ProducerJoins { session, ns }
        }
        // Setup failed before anything was spawned: nothing can call back.
        None => ProducerJoins::none_started(),
    };

    // 2. Accept instrumentation. The join outcomes gate `accept_intact`.
    let snap = match &accept_trace {
        Some(at) => at.shutdown(ACCEPT_DRAIN_BUDGET, joins).await,
        None => None,
    };

    // 3./4. One record shape for every ending. Write errors are NOT ignored:
    //    a run whose stats never reached disk must not exit 0, or it becomes
    //    indistinguishable from an uninstrumented run.
    let mut record_io_failed = false;
    {
        let mut lg = match logger.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(s) = &snap {
            if lg.try_log_info(&s.info_body()).is_err() {
                record_io_failed = true;
            }
        }
        if lg
            .try_log_info(&format!(
                "\"sent_pc\":{},\"sent_haptic\":{},\"pc_frames_generated\":{},\"pc_chunks_sent\":{},\"haptic_ticks_generated\":{},\"haptic_chunks_sent\":{},\"chunk_payload_bytes\":{},\"chunk_padding_bytes\":{},\"chunk_objects_per_s\":{:.6},\"pc_achieved_rate_hz\":{},\"haptic_achieved_rate_hz\":{},\"pc_period_mean_ms\":{},\"pc_period_std_ms\":{},\"haptic_period_mean_ms\":{},\"haptic_period_std_ms\":{},\"accept_trace_requested\":{},\"accept_trace_enabled\":{},\"chunk_trace\":{}",
                pc_stats.logical_generated,
                hap_stats.logical_generated,
                pc_stats.logical_generated,
                pc_stats.chunks_sent,
                hap_stats.logical_generated,
                hap_stats.chunks_sent,
                pc_stats.source_payload_bytes + hap_stats.source_payload_bytes,
                pc_stats.padding_bytes + hap_stats.padding_bytes,
                (pc_stats.chunks_sent + hap_stats.chunks_sent) as f64 / args.duration,
                json_f64(pc_stats.period.achieved_rate_hz()),
                json_f64(hap_stats.period.achieved_rate_hz()),
                json_f64(pc_stats.period.mean_ms()),
                json_f64(pc_stats.period.std_ms()),
                json_f64(hap_stats.period.mean_ms()),
                json_f64(hap_stats.period.std_ms()),
                args.accept_trace,
                accept_trace_enabled,
                args.chunk_trace,
            ))
            .is_err()
        {
            record_io_failed = true;
        }
        if lg
            .try_log_info(&format!(
                "\"event\":\"shutdown\",\"ending\":\"{}\",\"session_join\":\"{}\",\"ns_join\":\"{}\"",
                ending.as_str(),
                joins.session.as_str(),
                joins.ns.as_str()
            ))
            .is_err()
        {
            record_io_failed = true;
        }
        if lg.try_flush().is_err() {
            record_io_failed = true;
        }
    }
    watchdog.abort();

    if let Some(s) = &snap {
        println!(
            "[tx] accept callbacks={} enqueued={} written={} unwritten={} dropped_full={} dropped_closed={} io_errors={} in_flight={} drain_complete={} joined={} quiesced={} conserved={} intact={}",
            s.callbacks, s.enqueued, s.written, s.unwritten(), s.dropped_full,
            s.dropped_closed, s.io_errors, s.in_flight, s.drain_complete,
            s.producers_joined, s.producers_quiesced, s.conservation_ok(), s.intact()
        );
    }
    println!(
        "[tx] done ({}) session_join={} ns_join={} -> {}",
        ending.as_str(),
        joins.session.as_str(),
        joins.ns.as_str(),
        args.out.display()
    );

    // Exit-code contract:
    //   0  clean run
    //   1  workload/session error (anyhow propagated from `main`)
    //   2  finalization could not be trusted — a producer join timed out or
    //      panicked, or a shutdown record could not be written
    //   3  finalizer watchdog fired
    // 2 outranks 1: an untrustworthy finalization is the more dangerous state,
    // because it is the one that could otherwise masquerade as valid evidence.
    if record_io_failed || !joins.all_complete() {
        eprintln!(
            "[tx] FATAL: finalization not trustworthy (record_io_failed={record_io_failed}, session_join={}, ns_join={}); exiting {EXIT_FINALIZE_FAILED}",
            joins.session.as_str(),
            joins.ns.as_str()
        );
        if let Err(e) = &outcome {
            eprintln!("[tx] (run also ended with error: {e:#})");
        }
        std::process::exit(EXIT_FINALIZE_FAILED);
    }
    outcome
}

#[cfg(test)]
mod tests {
    use super::{epoch_record, now_us, registered_s_bytes, Duration};

    #[test]
    fn registered_s_bytes_is_rounded_workload_mean() {
        // datasets/tiers/loot/d8: 137,188,376 B over 300 frames = 457,294.5867
        assert_eq!(registered_s_bytes(137_188_376, 300), 457_295);
        // Half-way inputs go up, not to even: this is the rule the runners
        // implement in scripts/registered_s_bytes.py.
        assert_eq!(registered_s_bytes(5, 2), 3);
        assert_eq!(registered_s_bytes(3, 2), 2);
        assert_eq!(registered_s_bytes(1, 2), 1);
        assert_eq!(registered_s_bytes(0, 7), 0);
        // Exact past the f64 mantissa, where the old `mean.round()` path could
        // not be trusted to agree with integer arithmetic.
        assert_eq!(registered_s_bytes(9_007_199_254_740_993, 1), 9_007_199_254_740_993);
    }

    #[test]
    #[should_panic(expected = "at least one frame")]
    fn registered_s_bytes_rejects_an_empty_frame_set() {
        registered_s_bytes(0, 0);
    }

    /// 88차 P3-12 fixture: a forced delay between the two clock reads.
    ///
    /// The recorded epoch must be the earlier reading and the span must be the
    /// full observed width -- not the midpoint, and not silently clamped. A
    /// midpoint would place deadlines *after* the scheduling base for half the
    /// bracket, and a clamped span would hide the very preemption the analyser
    /// bound exists to catch.
    #[test]
    fn a_preempted_capture_records_the_earlier_read_and_the_full_span() {
        assert_eq!(epoch_record(1_000, 1_007).unwrap(), (1_000, 7));
        // 50 ms: a scheduling quantum, far past the registered 1 ms bound.
        assert_eq!(epoch_record(1_000, 51_000).unwrap(), (1_000, 50_000));
        // Identical reads are a real outcome on a coarse clock, not an error.
        assert_eq!(epoch_record(1_000, 1_000).unwrap(), (1_000, 0));
        // A backwards pair is refused, not saturated to the value that means
        // "perfect capture" (89차 P1).
        let err = epoch_record(1_000, 999).unwrap_err().to_string();
        assert!(err.contains("went backwards"), "{err}");
    }

    /// The same rule over a real forced delay rather than hand-picked numbers,
    /// so the helper is pinned against the clock it actually reads.
    #[test]
    fn a_real_forced_delay_is_measured_not_assumed_away() {
        let before = now_us();
        std::thread::sleep(Duration::from_millis(5));
        let after = now_us();
        let (t0_us, span) = epoch_record(before, after).unwrap();
        assert_eq!(t0_us, before, "the epoch must be the earlier read");
        // Lower bound only: the sleep is a floor, and the scheduler may add to
        // it. Asserting an upper bound here would make the test flaky on a
        // loaded machine for no gain.
        assert!(span >= 5_000, "span {span} did not cover the forced delay");
        // MAX_T0_CAPTURE_SPAN_US is 1000 in analyze_skew_v5; a capture this
        // wide is refused there rather than quietly scored.
        assert!(span > 1_000);
    }
}
