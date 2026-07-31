// moq_sender — MoQ naive B1 publisher (the "divergence extreme").
//
// One QUIC connection to a relay carries TWO tracks (pc, haptic) as independent
// subgroup streams. NO priority, NO delivery timeout, NO playout timeline — the
// naive baseline. The v5 generation uses PC 30 Hz and haptic 90 Hz with exact
// 1:3 rational-time anchors. `frame` sends one MoQ object per logical item;
// `equal-chunk` sends fixed-size objects and keeps logical JSONL rows.
//
// Namespace == run_id, so concurrent/repeat runs never collide on the relay.

use std::net::SocketAddr;
use std::borrow::Cow;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
use moq_native_ietf::{quic, tls};
use moq_transport::{coding::TrackNamespace, serve::Tracks, session::Session};
use tokio::signal::unix::{signal, SignalKind};
use tokio::time::{sleep_until, Instant};
use url::Url;

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

struct Producers {
    session_run: Option<SessionJoinHandle>,
    ns_task: Option<SessionJoinHandle>,
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
    let frame_mean = frame_sum as f64 / sizes.len() as f64;
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

fn json_f64(value: f64) -> String {
    if value.is_finite() {
        format!("{value:.9}")
    } else {
        "null".to_string()
    }
}

async fn connect(relay: &Url) -> Result<(web_transport::Session, moq_transport::session::Transport)> {
    let tls_args = tls::Args { disable_verify: true, ..Default::default() };
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

    // ---- Workload ----
    let frames: Vec<Vec<u8>> = match (&args.frames_dir, args.dummy_size) {
        (Some(dir), _) => load_frames(dir)?,
        (None, Some(n)) => vec![vec![0u8; n]],
        (None, None) => anyhow::bail!("need --frames-dir or --dummy-size"),
    };
    let pcm = load_haptic_pcm(&args.haptic_wav)?;
    let s_bytes = frames[0].len() as u64;
    let pf = preflight(&frames, &args);
    let frames = Arc::new(frames);
    let pcm = Arc::new(pcm);
    let haptic_src = std::path::Path::new(&args.haptic_wav)
        .file_name().and_then(|s| s.to_str()).unwrap_or("").to_string();

    println!(
        "[tx] frames={} S={}B haptic={}B pc={}Hz haptic={}Hz mode={} chunk={}B tracks={} -> {}",
        frames.len(), s_bytes, pcm.len(), args.pc_rate_hz, args.haptic_rate_hz,
        args.payload_mode.as_str(), args.chunk_bytes, args.tracks.as_str(),
        args.out.display()
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
        Some(TERM_PROTOCOL_V), None,
        Some(V5Meta {
            log_schema_version: 2,
            payload_mode: args.payload_mode,
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
    let accept_trace = if accept_trace_enabled {
        Some(AcceptTrace::install(args.accept_trace_capacity, logger.clone())?)
    } else {
        None
    };

    // 종료 내구성 (A2-c R1): 시그널은 여기서 프로세스를 끝내지 않는다. main 으로
    // 전달만 하고, 정상·시그널·오류 종료가 **모두 같은 finalizer** 를 통과한다.
    // 예전 구조는 시그널 태스크가 곧바로 process::exit(0) 을 불러서, 정상 경로가
    // stats 를 쓰기 전에 프로세스를 끝낼 수 있는 창이 있었다.
    let (sig_tx, mut sig_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        let mut term = match signal(SignalKind::terminate()) { Ok(s) => s, Err(_) => return };
        let mut intr = match signal(SignalKind::interrupt()) { Ok(s) => s, Err(_) => return };
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
        let (session, mut publisher, _sub) = Session::connect(sess, None, tp).await.context("SETUP")?;

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
        let (mut tracks_w, _req, tracks_r) = Tracks::new(namespace.clone()).produce();
        let pc_tw = tracks_w.create("pc").context("create pc track")?;
        let hap_tw = tracks_w.create("haptic").context("create haptic track")?;

        // Announce the namespace (serves subscribes on demand).
        let mut ns_pub = publisher.clone();
        producers.as_mut().expect("registered above").ns_task =
            Some(tokio::spawn(async move { ns_pub.publish_namespace(tracks_r).await }));
        // publisher no longer needed after cloning for the namespace.
        let _ = &mut publisher;

        // Warmup so the subscriber is attached before objects flow.
        tokio::time::sleep(Duration::from_secs_f64(args.warmup)).await;
        let anchor = Instant::now();
        let end = anchor + Duration::from_secs_f64(args.duration);

    // ---- PC loop: rational PC Hz ----
    // The whole subgroups chain lives in the task, so it fully drops when the
    // loop ends — closing the track so the subscriber sees end-of-track.
    let pc_task = if args.tracks.pc_on() {
        let frames = frames.clone();
        let logger = logger.clone();
        let pc_rate_hz = args.pc_rate_hz;
        let tier = args.tier;
        let payload_mode = args.payload_mode;
        let chunk_bytes = args.chunk_bytes;
        let mut pc_sub = pc_tw.subgroups().context("pc subgroups")?;
        let mut sg = pc_sub.append(128).context("pc append")?;
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
                    PayloadMode::EqualChunk => {
                        equal_chunks(payload, i as u32, chunk_bytes)?
                            .into_iter()
                            .map(Cow::Owned)
                            .collect()
                    }
                };
                let mut frame_obj = None;
                for object_payload in &object_payloads {
                    let hdr = pack_header(
                        TRACK_PC, tier, i as u32, pts, (i + 1) as u32, t_gen,
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
                let t_send = now_us();
                logger.lock().unwrap().log_tx(
                    "pc", tier, i as u32, pts, (i + 1) as u32,
                    payload.len(), t_gen, t_send, frame_obj,
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
            drop(sg);
            drop(pc_sub); // close pc track -> subscriber end-of-track
            Ok::<TrackRunStats, anyhow::Error>(stats)
        }))
    } else {
        // C3 haptic-only: never call `append`, so no subgroup stream and no
        // object is ever put on the wire for pc. Dropping the writer closes
        // the track, so the subscriber's pc drain ends immediately instead of
        // blocking until --max-duration.
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
        let mut hap_sub = hap_tw.subgroups().context("haptic subgroups")?;
        let mut sg = hap_sub.append(128).context("haptic append")?;
        Some(tokio::spawn(async move {
            let mut k: u64 = 0;
            let mut stats = TrackRunStats::default();
            while Instant::now() < end {
                sleep_until(
                    anchor + Duration::from_nanos(deadline_ns(k, haptic_rate_hz))
                ).await;
                if Instant::now() >= end {
                    break;
                }
                let (pts, event_id) = if k % ratio == 0 {
                    let fi = k / ratio;
                    let pts = timestamp_us(fi, pc_rate_hz);
                    (pts, (fi + 1) as u32)
                } else {
                    (timestamp_us(k, haptic_rate_hz), 0u32)
                };
                let payload = pcm_tick_payload(
                    &pcm, k, PCM_SAMPLE_RATE_HZ, haptic_rate_hz
                )?;
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
                        TRACK_HAPTIC, HAPTIC_TIER_FULL, k as u32, pts, event_id,
                        t_gen, object_payload.len() as u32,
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
                    "haptic", HAPTIC_TIER_FULL, k as u32, pts, event_id,
                    payload.len(), t_gen, now_us(), frame_obj,
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
            drop(hap_sub); // close haptic track
            Ok::<TrackRunStats, anyhow::Error>(stats)
        }))
    } else {
        // C3 pc-only: see the pc branch above — zero haptic objects on the
        // wire, track closed immediately so the subscriber does not wait.
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
        ending.as_str(), joins.session.as_str(), joins.ns.as_str(), args.out.display()
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
