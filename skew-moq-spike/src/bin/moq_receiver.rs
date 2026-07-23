// moq_receiver — MoQ naive B1 subscriber (L1: t_play = t_recv).
//
// Subscribes to both tracks (pc, haptic) on namespace == run_id via the relay,
// reads each object, parses the 32B header, and logs an rx record with
// t_recv = t_play (arrival) and t_gen (from the header) for the D metrics.
// Each MoQ object is one complete message, so no byte reassembly is needed.

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
    serve::{TrackReaderMode, Tracks},
    session::Session,
};
use std::process::Stdio;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::sync::mpsc;
use url::Url;

use skew_moq::*;
use skew_moq::playout::{LatePolicy, PlayoutAction, PlayoutConfig, PlayoutObject, PlayoutScheduler};

#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum Arm {
    B1,
    S1,
    M1,
    S2,
}

impl Arm {
    fn as_str(self) -> &'static str {
        match self {
            Self::B1 => "b1",
            Self::S1 => "s1",
            Self::M1 => "m1",
            Self::S2 => "s2",
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
    #[arg(long)]
    run_id: String,
    #[arg(long)]
    out: PathBuf,
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
    #[arg(long, default_value_t = 30)]
    fps: u64,
    #[arg(long, default_value_t = 100)]
    haptic_hz: u64,
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
    /// Fixed S1 playout offset. The governing design permits only 50/100 ms
    /// before the pilot selects one; S1 requires an explicit choice.
    #[arg(long)]
    d_play_ms: Option<u64>,
    /// Bound for finding the first exact PC/haptic anchor pair.
    #[arg(long)]
    startup_timeout_ms: Option<u64>,
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
        Arm::S2 => {
            let timeout = args
                .pc_delivery_timeout_ms
                .context("--arm s2 requires --pc-delivery-timeout-ms")?;
            if timeout == 0 {
                bail!("--pc-delivery-timeout-ms must be greater than zero");
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
        late_tolerance_us: ms_to_us(
            args.late_tolerance_ms
                .with_context(|| format!("--arm {arm} requires --late-tolerance-ms"))?,
            "late-tolerance-ms",
        )?,
        max_objects_per_track: args
            .buffer_max_objects_per_track
            .with_context(|| {
                format!("--arm {arm} requires --buffer-max-objects-per-track")
            })?,
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
    config.validate().map_err(anyhow::Error::msg)?;
    Ok(Some(config))
}

fn phase4_transport(args: &Args) -> Option<Phase4TransportMeta> {
    match args.arm {
        Arm::B1 | Arm::S1 => None,
        Arm::M1 => Some(Phase4TransportMeta {
            arm: "m1",
            pc_subgroup_mapping: "frame-per-subgroup",
            pc_publisher_priority: 128,
            haptic_publisher_priority: 128,
            pc_delivery_timeout_ms: None,
        }),
        Arm::S2 => Some(Phase4TransportMeta {
            arm: "s2",
            pc_subgroup_mapping: "frame-per-subgroup",
            pc_publisher_priority: 1,
            haptic_publisher_priority: 0,
            pc_delivery_timeout_ms: args.pc_delivery_timeout_ms,
        }),
    }
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
                    object.track_name(), h.tier, h.seq, h.pts_us, h.event_id, action_time,
                )?;
                stats.released += 1;
                let observe = (h.track_id == TRACK_PC && render)
                    || (h.track_id == TRACK_HAPTIC && audio);
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
            PlayoutAction::Drop { object, reason } => {
                logger.lock().unwrap().try_log_drop(
                    object.track_name(), h.tier, h.seq, h.pts_us, h.event_id,
                    action_time, reason,
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
            let Some(wakeup) = scheduler.next_wakeup_us() else { break };
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
    let rate = if name == "pc" { args.fps } else { args.haptic_hz };
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
            self.name, self.end_str(), self.received, expected, complete
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
        return (
            RxEnding::SessionEnded,
            "rule=session_finished".to_string(),
        );
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
    let cancelled: Vec<&DrainReport> =
        reports.iter().filter(|r| r.end == TrackEnd::Cancelled).collect();
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
    let s1_config = playout_config(&args)?;
    let phase4_transport = phase4_transport(&args);

    let logger = Arc::new(Mutex::new(JsonlLogger::new(
        &args.out, &args.run_id, "moq", "rx", args.c_mbps, args.rtt_ms,
        args.jitter_ms, args.loss_pct, args.s_bytes, args.fps, args.haptic_hz, args.seed,
        // duration_s = None **유지**: rx meta에 duration_s를 넣으면 분석기의
        // 설계 분모 출처(`_design`이 rx_meta.duration_s도 읽음)로 흡수되어
        // 기대 프레임 분모의 provenance가 바뀐다. 분모는 tx meta/CLI 주입만
        // 쓰는 현 계약을 유지한다.
        // tracks: 러너가 --tracks를 전달하므로 이제 수신자도 안다. rx meta에
        // 기록해 두면 tx 로그를 잃은 C3 rx 로그도 단독 트랙으로 분류된다.
        // term_protocol: 종료 프로토콜 세대 마커(Codex 7차 P0 — tx 소실 +
        // shutdown 결손 조합이 구세대로 오인되는 우회를 rx meta 자체로 차단).
        None, None, Some(args.tracks.as_str()), Some(TERM_PROTOCOL_V), s1_config,
        phase4_transport,
    )?));

    let (sess, tp) = connect(&args.relay).await.context("connect relay")?;
    let (session, _pub, mut subscriber) = Session::connect(sess, None, tp).await.context("SETUP")?;
    let mut session_run = tokio::spawn(session.run());

    let namespace = TrackNamespace::from_utf8_path(&args.run_id);
    let names = ["pc", "haptic"];

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
        for name in names {
            let tw = match sub_tracks.create(name) {
                Some(tw) => tw,
                None => { ok = false; break; }
            };
            let rr = match sub_reader.get_track_reader(&namespace, name) {
                Some(rr) => rr,
                None => { ok = false; break; }
            };
            let mut params = KeyValuePairs::default();
            if name == "pc" && args.arm == Arm::S2 {
                params.set_delivery_timeout(
                    args.pc_delivery_timeout_ms
                        .expect("S2 timeout validated before connecting"),
                );
            }
            match subscriber.subscribe_open_with_params(tw, params).await {
                Ok(h) => { this_handles.push(h); this_recv.push((name, rr)); }
                Err(e) if is_retryable_subscribe_error(&e) => { ok = false; break; }
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
                args.subscribe_timeout, sub_stats.retries
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
    println!("[rx] subscribed pc+haptic on {}", args.run_id);

    // Stage A bridge (optional): spawn the Python renderer/audio helper and forward
    // received objects (header+payload) over a non-blocking, drop-on-full channel so
    // the receive loop never stalls — t_play stays == t_recv (L1), same as B0.
    let mut bridge_child = None;
    let ftx: Option<mpsc::Sender<Bytes>> = if args.render || args.audio {
        let mut cmd = Command::new(&args.python);
        cmd.arg("tools/stage_a_bridge.py");
        if args.render { cmd.arg("--render"); }
        if args.audio { cmd.arg("--audio"); }
        if args.draco { cmd.arg("--draco"); }
        cmd.arg("--title").arg(format!("skew live — {}", args.run_id));
        cmd.stdin(Stdio::piped());
        let mut child = cmd.spawn().context("spawn stage_a_bridge")?;
        let mut stdin = child.stdin.take().unwrap();
        let (tx, mut rx) = mpsc::channel::<Bytes>(64);
        tokio::spawn(async move {
            while let Some(b) = rx.recv().await {
                if stdin.write_all(&b).await.is_err() { break; }
            }
            let _ = stdin.flush().await; // EOF on drop -> bridge exits
        });
        bridge_child = Some(child);
        println!("[rx] Stage A bridge: render={} audio={}", args.render, args.audio);
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
            config, rx, logger.clone(), ftx.clone(), args.render, args.audio,
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

    // Drain each track: parse header, log rx, (optionally) forward for render/audio.
    let counts: Vec<Arc<AtomicU64>> = names.iter().map(|_| Arc::new(AtomicU64::new(0))).collect();
    // Header-integrity counters (R3b). A mismatch means the rx log cannot be
    // trusted as a measurement, so it is counted, recorded, and exits non-zero.
    let bad_headers = Arc::new(AtomicU64::new(0));
    let mut drains = Vec::new();
    for (idx, (name, received_track)) in received.into_iter().enumerate() {
        let logger = logger.clone();
        let count = counts[idx].clone();
        let bad = bad_headers.clone();
        let ftx = ftx.clone();
        let s1_tx = s1_tx.clone();
        let ingress_drops = ingress_drops.clone();
        let ingress_log_failed = ingress_log_failed.clone();
        let fwd = (name == "pc" && args.render) || (name == "haptic" && args.audio);
        drains.push(tokio::spawn(async move {
            // Inner future yields the raw ServeError so the ending can be
            // classified before the error is erased.
            let inner = async move {
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
                    let track = track_name(h.track_id);
                    if h.version != VERSION {
                        eprintln!("[rx] {name}: header version {} != {VERSION}", h.version);
                        bad.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    if track != name {
                        eprintln!("[rx] {name}: header track '{track}' does not match the subscribed track");
                        bad.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    if obj.len() != HDR + h.payload_len as usize {
                        eprintln!(
                            "[rx] {name}: object length {} != {HDR} + declared payload_len {}",
                            obj.len(), h.payload_len
                        );
                        bad.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    logger.lock().unwrap().log_rx(
                        track, h.tier, h.seq, h.pts_us, h.event_id, h.payload_len, t, t, h.gen_ts_us,
                    );
                    count.fetch_add(1, Ordering::Relaxed);
                    if let Some(tx) = &s1_tx {
                        let scheduled = PlayoutObject {
                            header: h,
                            t_recv: t,
                            bytes: obj.clone(),
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
                            let _ = tx.try_send(obj.clone()); // drop-on-full (latest-wins-ish)
                        }
                    }
                }
            }
            Ok::<(), DrainFail>(())
            };
            // Reader exhausted cleanly == FIN. Otherwise classify the error;
            // only `Done` is a FIN, `Cancel`/`Closed` stay ambiguous.
            match inner.await {
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
                Err(e) if e.is_panic() => ends.push(("?", TrackEnd::Failed, format!("drain task panicked: {e}"))),
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
            let reports: Vec<DrainReport> = ends
                .into_iter()
                .map(|(name, end, detail)| {
                    let received = if name == "pc" { n_pc } else { n_hap };
                    DrainReport { name, end, received, expected: expected_for(name, &args), detail }
                })
                .collect();
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
                args.arm == Arm::S2 && args.pc_delivery_timeout_ms.is_some(),
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
                "\"recv_pc\":{n_pc},\"recv_haptic\":{n_hap},\"bad_headers\":{n_bad},\"s1_released\":{},\"s1_dropped\":{},\"s1_ingress_dropped\":{},\"s1_bridge_observer_dropped\":{}",
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
        eprintln!("[rx] FATAL: could not write the shutdown record; exiting {EXIT_FINALIZE_FAILED}");
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
        DrainReport { name, end, received, expected, detail: String::new() }
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
        assert_eq!(e, RxEnding::Normal, "a clean FIN is authoritative regardless of count");
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
        assert!(d.contains("rule=cancel_incomplete"), "reason must be recorded: {d}");
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
        assert!(d.contains("--duration-s"), "must say how to resolve it: {d}");
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
            assert_eq!(e, RxEnding::Normal, "unused C3 track {unused_end:?} must be normal: {d}");
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
        assert_eq!(classify_track_end(&ServeError::Closed(0)), TrackEnd::Cancelled);
        assert_eq!(classify_track_end(&ServeError::Closed(1)), TrackEnd::Cancelled);
        assert_eq!(classify_track_end(&ServeError::NotFound), TrackEnd::Failed);
        assert_eq!(classify_track_end(&ServeError::Duplicate), TrackEnd::Failed);
        assert_eq!(
            classify_track_end(&ServeError::Internal("x".into())),
            TrackEnd::Failed
        );
    }

    /// Expectations must follow C3: a disabled track expects exactly 0, and an
    /// absent `--duration-s` leaves every expectation unknown.
    #[test]
    fn expectations_follow_tracks_and_duration() {
        let base = |tracks, duration_s| Args {
            relay: Url::parse("https://127.0.0.1:1").unwrap(),
            run_id: "t".into(),
            out: PathBuf::from("/dev/null"),
            s_bytes: 1,
            c_mbps: None,
            rtt_ms: 0.0,
            jitter_ms: 0.0,
            loss_pct: 0.0,
            seed: 0,
            fps: 30,
            haptic_hz: 100,
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
            d_play_ms: None,
            startup_timeout_ms: None,
            late_tolerance_ms: None,
            buffer_max_objects_per_track: None,
            buffer_max_span_ms: None,
            late_policy: None,
            pc_delivery_timeout_ms: None,
        };

        let a = base(RxTrackSel::Both, Some(60.0));
        assert_eq!(expected_for("pc", &a), Some(1800));
        assert_eq!(expected_for("haptic", &a), Some(6000));

        let a = base(RxTrackSel::Pc, Some(60.0));
        assert_eq!(expected_for("pc", &a), Some(1800));
        assert_eq!(expected_for("haptic", &a), Some(0), "disabled track expects 0");

        let a = base(RxTrackSel::Haptic, Some(60.0));
        assert_eq!(expected_for("pc", &a), Some(0));
        assert_eq!(expected_for("haptic", &a), Some(6000));

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
            assert_eq!(e, RxEnding::Normal, "received={received} must stay normal: {d}");
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
        assert_eq!(e, RxEnding::Normal, "unprovable, so not claimed as failure: {d}");
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
            assert!(!is_retryable_subscribe_error(&e), "{e:?} must not be retried");
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
            run_id: "t".into(),
            out: PathBuf::from("/dev/null"),
            s_bytes: 1,
            c_mbps: None,
            rtt_ms: 0.0,
            jitter_ms: 0.0,
            loss_pct: 0.0,
            seed: 0,
            fps: 30,
            haptic_hz: 100,
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
            d_play_ms: Some(50),
            startup_timeout_ms: Some(100),
            late_tolerance_ms: Some(5),
            buffer_max_objects_per_track: Some(64),
            buffer_max_span_ms: Some(250),
            late_policy: Some(CliLatePolicy::DropLate),
            pc_delivery_timeout_ms: None,
        };
        let cfg = playout_config(&base).unwrap().unwrap();
        assert_eq!(cfg.d_play_us, 50_000);
        assert_eq!(cfg.max_objects_per_track, 64);

        let mut bad = base;
        bad.d_play_ms = Some(75);
        assert!(playout_config(&bad).unwrap_err().to_string().contains("50 or 100"));
    }

    #[test]
    fn m1_and_s2_cli_preserve_the_ablation_boundary() {
        let mut args = Args {
            relay: Url::parse("https://127.0.0.1:1").unwrap(),
            run_id: "t".into(),
            out: PathBuf::from("/dev/null"),
            s_bytes: 1,
            c_mbps: None,
            rtt_ms: 0.0,
            jitter_ms: 0.0,
            loss_pct: 0.0,
            seed: 0,
            fps: 30,
            haptic_hz: 100,
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
            d_play_ms: Some(50),
            startup_timeout_ms: Some(100),
            late_tolerance_ms: Some(5),
            buffer_max_objects_per_track: Some(64),
            buffer_max_span_ms: Some(250),
            late_policy: Some(CliLatePolicy::DropLate),
            pc_delivery_timeout_ms: None,
        };

        assert!(playout_config(&args).is_ok());
        let m1 = phase4_transport(&args).unwrap();
        assert_eq!(m1.arm, "m1");
        assert_eq!(m1.pc_publisher_priority, 128);
        assert_eq!(m1.pc_delivery_timeout_ms, None);

        args.pc_delivery_timeout_ms = Some(67);
        assert!(playout_config(&args).is_err(), "M1 must reject timeout");

        args.arm = Arm::S2;
        assert!(playout_config(&args).is_ok());
        let s2 = phase4_transport(&args).unwrap();
        assert_eq!(s2.pc_publisher_priority, 1);
        assert_eq!(s2.haptic_publisher_priority, 0);
        assert_eq!(s2.pc_delivery_timeout_ms, Some(67));

        args.pc_delivery_timeout_ms = Some(0);
        assert!(playout_config(&args).is_err(), "timeout zero is invalid");
    }
}
