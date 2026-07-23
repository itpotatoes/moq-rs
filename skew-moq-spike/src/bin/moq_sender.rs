// moq_sender — MoQ naive B1 publisher (the "divergence extreme").
//
// One QUIC connection to a relay carries TWO tracks (pc, haptic) as independent
// subgroup streams. NO priority, NO delivery timeout, NO playout timeline — the
// naive baseline. PC runs a 30fps loop over pre-loaded tier .bin frames; haptic
// runs a 100Hz loop over MPEG-I test-signal PCM with the snap pairing rule.
// tx JSONL is byte-schema-identical to webrtc_sender.py.
//
// Namespace == run_id, so concurrent/repeat runs never collide on the relay.

use std::collections::HashMap;
use std::net::SocketAddr;
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
    #[arg(long, default_value_t = 30)]
    fps: u64,
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

    // ---- Workload ----
    let frames: Vec<Vec<u8>> = match (&args.frames_dir, args.dummy_size) {
        (Some(dir), _) => load_frames(dir)?,
        (None, Some(n)) => vec![vec![0u8; n]],
        (None, None) => anyhow::bail!("need --frames-dir or --dummy-size"),
    };
    let pcm = load_haptic_pcm(&args.haptic_wav)?;
    let tick_bytes = HAPTIC_SAMPLES_PER_TICK * 2; // 160B
    let n_ticks_in_pcm = (pcm.len() / tick_bytes).max(1) as u64;
    let s_bytes = frames[0].len() as u64;
    let frames = Arc::new(frames);
    let pcm = Arc::new(pcm);
    let haptic_src = std::path::Path::new(&args.haptic_wav)
        .file_name().and_then(|s| s.to_str()).unwrap_or("").to_string();

    println!("[tx] frames={} S={}B haptic={}B ({} ticks) tracks={} -> {}",
        frames.len(), s_bytes, pcm.len(), n_ticks_in_pcm, args.tracks.as_str(),
        args.out.display());

    let logger = Arc::new(Mutex::new(JsonlLogger::new(
        &args.out, &args.run_id, "moq", "tx", args.c_mbps, args.rtt_ms,
        args.jitter_ms, args.loss_pct, s_bytes, args.fps, 100, args.seed,
        Some(args.duration), Some(&haptic_src), Some(args.tracks.as_str()),
        Some(TERM_PROTOCOL_V), None,
    )?));

    // ---- A2 transport-accept tap ----
    // Installed before the session exists so no object can be forwarded before
    // the observer is live. The drain task owns all file I/O; the observer
    // itself only timestamps and try_sends (see skew_moq::AcceptTap).
    //
    // The captured time is when QUIC accepted the object's last payload byte —
    // a transport-accept time, not an on-the-wire time. It follows send
    // backpressure under congestion and degenerates to a handoff time
    // otherwise.
    let accept_trace = if args.accept_trace {
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
    let mut n_pc: u64 = 0;
    let mut n_hap: u64 = 0;
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

    // ---- PC loop: 30 fps ----
    // The whole subgroups chain lives in the task, so it fully drops when the
    // loop ends — closing the track so the subscriber sees end-of-track.
    let pc_task = if args.tracks.pc_on() {
        let frames = frames.clone();
        let logger = logger.clone();
        let fps = args.fps;
        let tier = args.tier;
        let mut pc_sub = pc_tw.subgroups().context("pc subgroups")?;
        let mut sg = pc_sub.append(128).context("pc append")?;
        Some(tokio::spawn(async move {
            let mut i: u64 = 0;
            while Instant::now() < end {
                let pts = frame_pts_us(i, fps);
                let payload = &frames[(i as usize) % frames.len()];
                let t_gen = now_us();
                let hdr = pack_header(TRACK_PC, tier, i as u32, pts, (i + 1) as u32, t_gen, payload.len() as u32);
                let mut buf = Vec::with_capacity(HDR + payload.len());
                buf.extend_from_slice(&hdr);
                buf.extend_from_slice(payload);
                // Same work as SubgroupWriter::write, but the object identity is
                // observable so the tx record can carry the wire join key.
                let (gid, sgid) = (sg.group_id, sg.subgroup_id);
                let mut obj = sg.create(buf.len(), None).context("pc create")?;
                let oid = obj.object_id;
                obj.write(Bytes::from(buf)).context("pc write")?;
                drop(obj);
                let t_send = now_us();
                logger.lock().unwrap().log_tx("pc", tier, i as u32, pts, (i + 1) as u32, payload.len(), t_gen, t_send, Some((gid, sgid, oid)));
                i += 1;
                sleep_until(anchor + Duration::from_secs_f64(i as f64 / fps as f64)).await;
            }
            drop(sg);
            drop(pc_sub); // close pc track -> subscriber end-of-track
            Ok::<u64, anyhow::Error>(i)
        }))
    } else {
        // C3 haptic-only: never call `append`, so no subgroup stream and no
        // object is ever put on the wire for pc. Dropping the writer closes
        // the track, so the subscriber's pc drain ends immediately instead of
        // blocking until --max-duration.
        drop(pc_tw.subgroups().context("pc subgroups")?);
        None
    };

    // ---- Haptic loop: 100 Hz with snap pairing ----
    let hap_task = if args.tracks.haptic_on() {
        let pcm = pcm.clone();
        let logger = logger.clone();
        let fps = args.fps;
        let duration = args.duration;
        let mut hap_sub = hap_tw.subgroups().context("haptic subgroups")?;
        let mut sg = hap_sub.append(128).context("haptic append")?;
        // Precompute snap map: tick index -> frame index (period 33.3ms > 10ms tick).
        let n_frames_max = (duration * fps as f64) as u64 + fps;
        let mut snap_map: HashMap<u64, u64> = HashMap::new();
        for fi in 0..n_frames_max {
            snap_map.insert(snap_tick(fi, fps), fi);
        }
        Some(tokio::spawn(async move {
            let mut k: u64 = 0;
            while Instant::now() < end {
                let (pts, event_id) = if let Some(&fi) = snap_map.get(&k) {
                    // Align send to the pts instant (removes structural offset).
                    let pts = frame_pts_us(fi, fps);
                    sleep_until(anchor + Duration::from_secs_f64(pts as f64 / 1e6)).await;
                    (pts, (fi + 1) as u32)
                } else {
                    (k * HAPTIC_TICK_US, 0u32)
                };
                let off = ((k % n_ticks_in_pcm) as usize) * tick_bytes;
                let payload = &pcm[off..(off + tick_bytes).min(pcm.len())];
                let t_gen = now_us();
                let hdr = pack_header(TRACK_HAPTIC, HAPTIC_TIER_FULL, k as u32, pts, event_id, t_gen, payload.len() as u32);
                let mut buf = Vec::with_capacity(HDR + payload.len());
                buf.extend_from_slice(&hdr);
                buf.extend_from_slice(payload);
                let (gid, sgid) = (sg.group_id, sg.subgroup_id);
                let mut obj = sg.create(buf.len(), None).context("haptic create")?;
                let oid = obj.object_id;
                obj.write(Bytes::from(buf)).context("haptic write")?;
                drop(obj);
                logger.lock().unwrap().log_tx("haptic", HAPTIC_TIER_FULL, k as u32, pts, event_id, payload.len(), t_gen, now_us(), Some((gid, sgid, oid)));
                k += 1;
                sleep_until(anchor + Duration::from_secs_f64(k as f64 * 0.01)).await;
            }
            drop(sg);
            drop(hap_sub); // close haptic track
            Ok::<u64, anyhow::Error>(k)
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
                    let n_pc = match pc_task { Some(h) => h.await??, None => 0 };
                    let n_hap = match hap_task { Some(h) => h.await??, None => 0 };
                    Ok::<_, anyhow::Error>((n_pc, n_hap))
                } => match r {
                    Ok((a, b)) => { n_pc = a; n_hap = b; Ok(()) }
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
        println!("[tx] sent pc={n_pc} haptic={n_hap}; draining {}s", args.drain_timeout);

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
            .try_log_info(&format!("\"sent_pc\":{n_pc},\"sent_haptic\":{n_hap}"))
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
