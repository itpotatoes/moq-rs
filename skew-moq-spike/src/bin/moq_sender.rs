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

use std::borrow::Cow;
use std::collections::HashMap;
use std::net::SocketAddr;
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
use url::Url;

use skew_moq::s3_producer::{CompletionState, SubscriptionProducerRegistry};
use skew_moq::s3_sender::{
    run_namespace as run_s3_namespace, AcceptRouteMap, SenderContext as S3SenderContext,
    SourceSchedule,
};
use skew_moq::*;

/// Hard budget for the accept finalizer: quiesce producers, close the tap,
/// drain, join. Bounded on purpose — a wedged drain must never stall a matrix
/// run. `AcceptTrace::shutdown` adds a small join guard on top.
const ACCEPT_DRAIN_BUDGET: Duration = Duration::from_secs(2);

/// Budget for joining the aborted session / namespace tasks. These must be
/// gone before the tap closes (A2-c R2), but a stuck one must not wedge us.
const PRODUCER_JOIN_BUDGET: Duration = Duration::from_secs(2);

/// After the registered drain edge a direct publisher must leave its session
/// alive long enough for the receiver to consume the track FINs and close the
/// peer session.  The receiver already has a 60 s post-drain safety margin;
/// using the same bound here keeps the handoff finite without extending the
/// 60 s measurement or 90 s drain. Relay runs retain their existing teardown:
/// the relay, rather than this sender session, owns the downstream FIN handoff.
const REGISTERED_DIRECT_FIN_HANDOFF_BUDGET: Duration = Duration::from_secs(60);

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

/// Keep the static-track writers alive until the registered drain edge.
///
/// Dropping the final subgroup/track writer emits the track FIN.  The receiver
/// treats a complete FIN as its normal process-exit authority, so retaining
/// only the MoQ session is not enough: on an uncongested link it exits at the
/// measurement edge while the sender is still in its fixed drain.  Ownership
/// of both states lives in this future for a phase-controlled run, making the
/// FIN edge coincide with the drain deadline (or an explicit shutdown signal)
/// without changing subgroup mapping, priority, or object order. Legacy runs
/// release the state before their session-only drain, preserving their existing
/// early-FIN behavior.
async fn hold_static_track_state_through_drain<P, H>(
    duration: Duration,
    mut signal: tokio::sync::watch::Receiver<bool>,
    pc_state: P,
    haptic_state: H,
    hold_tracks: bool,
) -> bool {
    let _held_track_state = if hold_tracks {
        Some((pc_state, haptic_state))
    } else {
        drop(pc_state);
        drop(haptic_state);
        None
    };
    tokio::select! {
        _ = tokio::time::sleep(duration) => false,
        _ = wait_signal(&mut signal) => true,
    }
}

#[derive(Debug, PartialEq, Eq)]
enum DirectFinHandoff<T> {
    PeerClosed(T),
    Signal,
    TimedOut,
}

/// Wait for the direct receiver to consume the released track FINs and close
/// its peer session. The caller supplies the session future so this race stays
/// unit-testable without opening a socket. A signal remains authoritative and
/// a missing peer close never becomes an unbounded matrix hang.
async fn wait_registered_direct_fin_handoff<F, T>(
    budget: Duration,
    mut signal: tokio::sync::watch::Receiver<bool>,
    peer_close: F,
) -> DirectFinHandoff<T>
where
    F: std::future::Future<Output = T>,
{
    tokio::select! {
        result = peer_close => DirectFinHandoff::PeerClosed(result),
        _ = wait_signal(&mut signal) => DirectFinHandoff::Signal,
        _ = tokio::time::sleep(budget) => DirectFinHandoff::TimedOut,
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
    /// Plan 단계 9 method 6: the event-pair NON-preserving control. Same wire
    /// configuration as S2 (frame-per-subgroup PC, haptic 0 / PC 1, PC
    /// DELIVERY_TIMEOUT) and the same PC tiers and haptic density as the
    /// paired S3 run, replayed OPEN-LOOP from `--tier-schedule`. The S3 FSM is
    /// not run, and the receiver releases each track on its own timeline.
    #[value(name = "s3np")]
    S3np,
    /// Plan 단계 9 / user decision 9-7(b): "S2 + replay of a registered tier
    /// trajectory, event pairs PRESERVED". On the SENDER this is byte-identical
    /// to `s3np` — same single-track replay of `--tier-schedule`, same wire
    /// policy — and the only difference is the recorded arm name. The two arms
    /// differ purely in receiver mechanics, which is what makes
    /// `S3R - S3NP` a clean scheduler comparison at an identical sender stream.
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

    fn pc_frame_subgroups(self) -> bool {
        matches!(
            self,
            Self::M1 | Self::S2 | Self::S2Eq | Self::S3 | Self::S3np | Self::S3r
        )
    }

    fn pc_priority(self) -> u8 {
        if matches!(self, Self::S2 | Self::S3 | Self::S3np | Self::S3r) {
            1
        } else {
            128
        }
    }

    fn haptic_priority(self) -> u8 {
        if matches!(self, Self::S2 | Self::S3 | Self::S3np | Self::S3r) {
            0
        } else {
            128
        }
    }

    /// Whether this arm generates from a recorded tier trajectory instead of
    /// from a fixed tier (`s3np`) or the live S3 FSM (`s3`). Both replay arms go
    /// through ONE code path — `TierReplay` — so their sender-side tier and
    /// haptic-density decisions are identical by construction, not by
    /// coincidence.
    fn replays_tier_schedule(self) -> bool {
        matches!(self, Self::S3np | Self::S3r)
    }

    /// Arms that need the three S3 PC tier directories.
    fn needs_pc_tier_dirs(self) -> bool {
        matches!(self, Self::S3 | Self::S3np | Self::S3r)
    }
}

/// Stage-5 cause-separation queue policy (plan 단계 5, decision 2026-09-08).
/// `separate` is the historical two-track wiring (P2 with equal-128, P3 with
/// haptic-first). `shared_fifo` (P1) sends both tracks' objects through ONE
/// track ("mixed") and ONE long-lived subgroup — a single QUIC stream — in
/// generation order; the receiver demultiplexes by the header `track_id`.
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
}

/// B1 static publisher-priority profile. `equal-128` is the historical B1
/// (P2); `haptic-first` (P3) is priority only — haptic 0 / PC 1, the S2
/// numbers — on B1's long-lived subgroups, with no PC delivery timeout.
#[derive(Clone, Copy, PartialEq, Eq, Debug, clap::ValueEnum)]
enum PublisherPriorityProfile {
    #[value(name = "equal-128")]
    Equal128,
    #[value(name = "haptic-first")]
    HapticFirst,
}

impl PublisherPriorityProfile {
    /// Meta vocabulary shared with S2/S2Eq so the same numbers carry the same
    /// `publisher_priority_profile` string across arms.
    fn meta_str(self) -> &'static str {
        match self {
            PublisherPriorityProfile::Equal128 => "equal-128",
            PublisherPriorityProfile::HapticFirst => "relative-haptic0-pc1",
        }
    }
}

/// Publisher priority of the single shared-FIFO subgroup. One stream has one
/// priority; the historical B1 value keeps P1 comparable with P2.
const SHARED_FIFO_PRIORITY: u8 = 128;

/// Wire track name that carries both logical tracks under `shared_fifo`.
const SHARED_FIFO_TRACK: &str = "mixed";

/// Static subgroup priorities `(pc, haptic)` for this run. Non-B1 arms keep
/// their frozen `Arm` mapping; B1 selects P2 (128/128) or P3 (1/0).
fn resolved_priorities(arm: Arm, profile: PublisherPriorityProfile) -> (u8, u8) {
    match (arm, profile) {
        (Arm::B1, PublisherPriorityProfile::HapticFirst) => (1, 0),
        _ => (arm.pc_priority(), arm.haptic_priority()),
    }
}

/// Stage-5 CLI boundary. Both options are opt-in and B1-only so no other arm
/// (S1/M1/S2/S2Eq/S3) can change behaviour through them. `profile` is the
/// explicit CLI value (`None` == default equal-128).
fn validate_stage5_policy(
    arm: Arm,
    payload_mode: PayloadMode,
    tracks: TrackSel,
    queue_policy: QueuePolicy,
    profile: Option<PublisherPriorityProfile>,
    mapping: DataPriorityMapping,
) -> Result<()> {
    if queue_policy == QueuePolicy::SharedFifo {
        if arm != Arm::B1 {
            anyhow::bail!("--queue-policy shared_fifo requires --arm b1");
        }
        if payload_mode != PayloadMode::Frame {
            anyhow::bail!("--queue-policy shared_fifo requires --payload-mode frame");
        }
        if tracks != TrackSel::Both {
            anyhow::bail!("--queue-policy shared_fifo requires --tracks both");
        }
    }
    if let Some(profile) = profile {
        if arm != Arm::B1 {
            anyhow::bail!("--publisher-priority-profile requires --arm b1");
        }
        if profile == PublisherPriorityProfile::HapticFirst {
            if queue_policy == QueuePolicy::SharedFifo {
                anyhow::bail!(
                    "--publisher-priority-profile haptic-first cannot combine with \
                     --queue-policy shared_fifo: a single stream has one priority"
                );
            }
            if mapping != DataPriorityMapping::MoqtV2 {
                anyhow::bail!(
                    "--publisher-priority-profile haptic-first requires \
                     --data-priority-mapping moqt-v2 (lower number first)"
                );
            }
        }
    }
    Ok(())
}

/// B1 transport metadata. The historical B1 (separate, equal-128) records
/// nothing here so its meta line keeps the pre-stage-5 shape; either stage-5
/// deviation records the mapping and the priorities actually applied.
fn b1_transport_meta(
    queue_policy: QueuePolicy,
    profile: PublisherPriorityProfile,
    mapping: DataPriorityMapping,
) -> Option<Phase4TransportMeta> {
    if queue_policy == QueuePolicy::Separate && profile == PublisherPriorityProfile::Equal128 {
        return None;
    }
    let (pc, haptic) = resolved_priorities(Arm::B1, profile);
    Some(Phase4TransportMeta {
        arm: "b1",
        pc_subgroup_mapping: match queue_policy {
            QueuePolicy::Separate => "long-lived-subgroup",
            QueuePolicy::SharedFifo => "shared-single-subgroup",
        },
        pc_publisher_priority: pc,
        haptic_publisher_priority: haptic,
        publisher_priority_profile: profile.meta_str(),
        data_priority_mapping: mapping.as_str(),
        pc_delivery_timeout_ms: None,
    })
}

/// Append target of the shared FIFO. Abstracted so the ordering rule can be
/// unit-tested without a MoQ session.
trait SharedFifoWriter {
    fn identity(&self) -> (u64, u64);
    fn append(&mut self, bytes: Vec<u8>) -> Result<u64>;
}

impl SharedFifoWriter for moq_transport::serve::SubgroupWriter {
    fn identity(&self) -> (u64, u64) {
        (self.group_id, self.subgroup_id)
    }

    fn append(&mut self, bytes: Vec<u8>) -> Result<u64> {
        let mut object = self.create(bytes.len(), None).context("mixed create")?;
        let object_id = object.object_id;
        object.write(Bytes::from(bytes)).context("mixed write")?;
        Ok(object_id)
    }
}

type SharedSubgroup = Arc<Mutex<moq_transport::serve::SubgroupWriter>>;

/// Timing of one shared-FIFO append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SharedFifoAppend {
    /// Monotonic µs immediately before the lock attempt: payload bytes are
    /// already copied into the object buffer, only the header is missing.
    t_ready: u64,
    /// Monotonic µs sampled under the lock; the header's `gen_ts_us`.
    t_gen: u64,
    /// MoQ identity `(group_id, subgroup_id, object_id)` of the appended object.
    identity: (u64, u64, u64),
}

/// Shared-FIFO ordering rule and exact lock boundary.
///
/// Outside the lock (before `t_ready`): the caller has allocated the object
/// buffer and copied the full payload after a 32-byte header placeholder
/// (`object_buffer`), so the payload copy — up to one PC frame, ~460 kB —
/// never happens while the other producer waits.
///
/// Under the lock: `t_gen = now_us()`, pack the 32-byte header into the
/// placeholder, then `create` + `write` on the single subgroup. Nothing else.
/// Because `t_gen` and the append share one critical section, `object_id`
/// order equals `t_gen` order across the PC and haptic producers.
///
/// `t_gen - t_ready` is therefore the lock wait, which `t_recv - t_gen` does
/// not contain; it is exported on the tx row as `t_ready`. The guard never
/// spans an await; the QUIC send happens later in the session task.
fn shared_fifo_append<W: SharedFifoWriter>(
    shared: &Mutex<W>,
    mut bytes: Vec<u8>,
    header: impl FnOnce(u64) -> [u8; HDR],
) -> Result<SharedFifoAppend> {
    anyhow::ensure!(bytes.len() >= HDR, "shared FIFO object buffer lacks the header placeholder");
    let t_ready = now_us();
    let mut writer = shared
        .lock()
        .map_err(|_| anyhow::anyhow!("shared FIFO subgroup poisoned"))?;
    let t_gen = now_us();
    bytes[..HDR].copy_from_slice(&header(t_gen));
    let (group_id, subgroup_id) = writer.identity();
    let object_id = writer.append(bytes)?;
    Ok(SharedFifoAppend {
        t_ready,
        t_gen,
        identity: (group_id, subgroup_id, object_id),
    })
}

/// Object buffer for `shared_fifo_append`: a zeroed 32-byte header
/// placeholder followed by the payload copy. Built OUTSIDE the FIFO lock.
fn object_buffer(payload: &[u8]) -> Vec<u8> {
    let mut bytes = vec![0u8; HDR];
    bytes.reserve_exact(payload.len());
    bytes.extend_from_slice(payload);
    bytes
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
    /// Confirmatory batch identity. These three phase options are all-or-none.
    #[arg(long)]
    batch_id: Option<String>,
    #[arg(long)]
    phase_control: Option<PathBuf>,
    #[arg(long)]
    warmup_pass: Option<PathBuf>,
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
    /// Stage-5 queue policy. `separate` (default) is the current behaviour;
    /// `shared_fifo` needs --arm b1, --payload-mode frame, --tracks both.
    #[arg(long, value_enum, default_value_t = QueuePolicy::Separate)]
    queue_policy: QueuePolicy,
    /// Stage-5 B1 priority profile. Absent == `equal-128` (current B1).
    /// `haptic-first` is priority only (haptic 0 / PC 1, moqt-v2), no timeout.
    #[arg(long, value_enum)]
    publisher_priority_profile: Option<PublisherPriorityProfile>,
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
    /// `--arm s3np` only: the tier/haptic-density schedule extracted from the
    /// paired S3 run by `scripts/event_pair_tier_schedule.py`. Applied
    /// open-loop, so `s3np` matches that run's PC quality and haptic data rate
    /// without running the S3 FSM.
    #[arg(long)]
    tier_schedule: Option<PathBuf>,
    /// `--arm s3np` only: the receiver's fixed playout offset, repeated here
    /// for frozen metadata agreement exactly as `--pc-delivery-timeout-ms` is
    /// on S2. The sender does not schedule playout; recording the value lets
    /// accounting attest that both endpoints ran the same policy.
    #[arg(long)]
    d_play_ms: Option<u64>,
    /// `--arm s3np` only: the receiver's release rule, echoed here for the same
    /// reason as `--d-play-ms`. The sender applies no release policy at all; the
    /// echo exists so a single-endpoint audit cannot confuse the registered
    /// primary rule with its sensitivity variant. No default: the receiver has
    /// none either.
    #[arg(long, value_enum)]
    s3np_release_rule: Option<CliReleaseRule>,
}

/// CLI mirror of `skew_moq::s3np::ReleaseRule`. Kept a separate type so clap's
/// value names are part of the CLI contract rather than of the library.
#[derive(Clone, Copy, PartialEq, Eq, Debug, clap::ValueEnum)]
enum CliReleaseRule {
    #[value(name = "per_track_epoch")]
    PerTrackEpoch,
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

/// Open-loop tier/haptic-density replay for `--arm s3np` (plan 단계 9 method 6).
///
/// The paired S3 run's *applied* schedule is replayed verbatim, so the two arms
/// send the same PC tiers and the same haptic tick density at the same offsets
/// from `measurement_start`. Nothing here observes the network, and no S3
/// controller, switch gate, or subscription producer exists in this arm: that is
/// the point of the control.
///
/// What the replay CANNOT reproduce exactly is documented in
/// `md/20260912_짝비보존_대조_구현.md`: S3 switches tier by make-before-break
/// subscription (the target route emits barrier-only objects while the old route
/// is still current), so S3 transiently sends more PC bytes around a switch than
/// this single-track replay does.
struct TierReplay {
    schedule: skew_moq::s3np::TierSchedule,
    sha256: [u8; 32],
    /// Replayed `pc_tier` → frame source. Frozen S3 mapping: 2 → d8
    /// (`--frames-dir`), 3 → d7 (Recovery), 4 → d6 (Haptic-Critical).
    normal: Arc<Vec<Vec<u8>>>,
    recovery: Arc<Vec<Vec<u8>>>,
    critical: Arc<Vec<Vec<u8>>>,
}

impl TierReplay {
    fn frames(&self, pc_tier: u16) -> &Vec<Vec<u8>> {
        match pc_tier {
            skew_moq::s3np::PC_TIER_RECOVERY => &self.recovery,
            skew_moq::s3np::PC_TIER_CRITICAL => &self.critical,
            // `TierSchedule::parse` admits only 2/3/4, so this is Normal.
            _ => &self.normal,
        }
    }

    /// Replayed PC tier for the PC slot whose nominal PTS is `pts_us`.
    ///
    /// The lookup key is the NOMINAL slot offset, never wall time, so the
    /// decision is deterministic and identical for every replay arm. Both
    /// `s3np` and `s3r` call exactly this.
    fn pc_tier_at(&self, pts_us: u64) -> u16 {
        self.schedule.state_at(pts_us).pc_tier
    }

    /// Whether the replayed density skips this haptic tick.
    ///
    /// `Essential` keeps only the exact anchor tick `3i`, which is precisely the
    /// tick set S3's `haptic-essential` route produces; `Full` keeps every tick.
    /// Identity, PCM slice and header of a kept tick are unchanged, so the only
    /// effect is the haptic data rate.
    fn skips_haptic_tick(&self, tick: u64, haptic_rate_hz: u64, ratio: u64) -> bool {
        let offset_us = timestamp_us(tick, haptic_rate_hz);
        self.schedule.state_at(offset_us).haptic_density
            == skew_moq::s3np::HapticDensity::Essential
            && tick % ratio != 0
    }
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
    let source_haptic_bytes_per_s = PCM_SAMPLE_RATE_HZ as f64 * PCM_BYTES_PER_SAMPLE as f64;
    let source_payload_bytes_per_s =
        frame_mean * args.pc_rate_hz as f64 + source_haptic_bytes_per_s;
    let application_bytes_per_s = match args.payload_mode {
        PayloadMode::Frame => {
            source_payload_bytes_per_s + (pc_objects_per_s + haptic_objects_per_s) * HDR as f64
        }
        PayloadMode::EqualChunk => {
            (pc_objects_per_s + haptic_objects_per_s) * (HDR + CHUNK_HDR + args.chunk_bytes) as f64
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
    assert!(
        frame_count > 0,
        "registered_s_bytes needs at least one frame"
    );
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
#[cfg(test)]
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
    if matches!(args.arm, Arm::S2 | Arm::S2Eq | Arm::S3 | Arm::S3np | Arm::S3r)
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
            if args.arm == Arm::B1 {
                return Ok(b1_transport_meta(
                    args.queue_policy,
                    priority_profile(args),
                    args.data_priority_mapping,
                ));
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
        Arm::S2 | Arm::S2Eq | Arm::S3 | Arm::S3np | Arm::S3r => {
            let timeout = args.pc_delivery_timeout_ms.with_context(|| {
                format!(
                    "--arm {} requires --pc-delivery-timeout-ms",
                    args.arm.as_str()
                )
            })?;
            if timeout == 0 {
                anyhow::bail!("--pc-delivery-timeout-ms must be greater than zero");
            }
            // The replay arms are S3 controls, so they inherit the same frozen
            // timeout: a different value would make the comparison a timeout
            // ablation instead of an adaptation/pairing one.
            if matches!(args.arm, Arm::S3 | Arm::S3np | Arm::S3r) && timeout != 67 {
                anyhow::bail!(
                    "--arm {} inherits the frozen 67ms PC delivery timeout",
                    args.arm.as_str()
                );
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

/// Meta key under which this arm's release rule is recorded. Arm-specific so a
/// metadata row can never be read as the other replay arm.
fn replay_release_rule_key(arm: Arm) -> &'static str {
    match arm {
        Arm::S3r => skew_moq::s3np::S3R_RELEASE_RULE_KEY,
        _ => skew_moq::s3np::S3NP_RELEASE_RULE_KEY,
    }
}

/// The release rule this run's RECEIVER applies, echoed in sender metadata for
/// the same reason S2 echoes `--pc-delivery-timeout-ms`: a single-endpoint audit
/// must not be able to confuse the arms, or the two `s3np` rule variants.
fn replay_release_rule(arm: Arm, s3np_rule: Option<CliReleaseRule>) -> &'static str {
    match arm {
        Arm::S3r => skew_moq::s3np::S3R_RELEASE_RULE,
        _ => skew_moq::s3np::ReleaseRule::from(
            s3np_rule.expect("validated s3np release rule"),
        )
        .as_str(),
    }
}

fn priority_profile(args: &Args) -> PublisherPriorityProfile {
    args.publisher_priority_profile
        .unwrap_or(PublisherPriorityProfile::Equal128)
}

/// S3 and both replay arms need the three PC tier directories, tier 2 on the
/// Normal header, and both tracks. Only S3 owns subscription producers; only the
/// replay arms own `--tier-schedule`/`--d-play-ms`; and only `s3np` owns
/// `--s3np-release-rule`, because `s3r` has exactly one release rule. Every
/// arm's exclusive flags stay refused everywhere else.
fn validate_s3_args(args: &Args) -> Result<()> {
    let tier_dirs = args.s3_recovery_frames_dir.is_some() || args.s3_critical_frames_dir.is_some();
    let s3_only = args.s3_producer_shutdown_timeout_ms.is_some();
    let replay_only = args.tier_schedule.is_some() || args.d_play_ms.is_some();
    let s3np_only = args.s3np_release_rule.is_some();
    if !args.arm.needs_pc_tier_dirs() {
        if tier_dirs {
            anyhow::bail!(
                "S3 PC tier frame directories require --arm s3, --arm s3np or --arm s3r"
            );
        }
        if s3_only {
            anyhow::bail!("S3 lifecycle options require --arm s3");
        }
        if replay_only {
            anyhow::bail!("--tier-schedule and --d-play-ms require --arm s3np or --arm s3r");
        }
        if s3np_only {
            anyhow::bail!("--s3np-release-rule requires --arm s3np");
        }
        return Ok(());
    }
    let arm = args.arm.as_str();
    if args.arm == Arm::S3 && (replay_only || s3np_only) {
        anyhow::bail!(
            "--tier-schedule, --d-play-ms and --s3np-release-rule require a replay arm; \
             S3 runs its own FSM and must never replay a recorded schedule"
        );
    }
    if args.arm.replays_tier_schedule() && s3_only {
        anyhow::bail!(
            "--s3-producer-shutdown-timeout-ms requires --arm s3; the replay arms publish \
             the static two-track mapping and own no subscription producers"
        );
    }
    if args.arm == Arm::S3r && s3np_only {
        anyhow::bail!(
            "--s3np-release-rule requires --arm s3np; s3r has exactly one release rule \
             (the unchanged S1/S2 common timeline), so there is nothing to select"
        );
    }
    if args.tracks != TrackSel::Both {
        anyhow::bail!("--arm {arm} requires --tracks both");
    }
    if args.tier != 2 {
        anyhow::bail!("--arm {arm} Normal must preserve header tier 2");
    }
    if args.frames_dir.is_none() || args.dummy_size.is_some() {
        anyhow::bail!("--arm {arm} requires --frames-dir d8 and forbids --dummy-size");
    }
    args.s3_recovery_frames_dir
        .as_ref()
        .with_context(|| format!("--arm {arm} requires --s3-recovery-frames-dir d7"))?;
    args.s3_critical_frames_dir
        .as_ref()
        .with_context(|| format!("--arm {arm} requires --s3-critical-frames-dir d6"))?;
    if args.arm == Arm::S3 {
        let timeout = args
            .s3_producer_shutdown_timeout_ms
            .context("--arm s3 requires --s3-producer-shutdown-timeout-ms")?;
        if timeout == 0 {
            anyhow::bail!("--s3-producer-shutdown-timeout-ms must be greater than zero");
        }
        return Ok(());
    }
    args.tier_schedule
        .as_ref()
        .with_context(|| format!("--arm {arm} requires --tier-schedule"))?;
    let d_play_ms = args.d_play_ms.with_context(|| {
        format!("--arm {arm} requires --d-play-ms (the receiver value, for metadata agreement)")
    })?;
    if !matches!(d_play_ms, 50 | 100) {
        anyhow::bail!("--d-play-ms must be a governing-design candidate: 50 or 100");
    }
    // Fail closed: s3np has no default release rule. s3r has exactly one, which
    // is why it must NOT accept the flag (refused above).
    if args.arm == Arm::S3np {
        args.s3np_release_rule
            .context("--arm s3np requires --s3np-release-rule {per_track_epoch|absolute_t_gen}")?;
    }
    if args.queue_policy != QueuePolicy::Separate {
        anyhow::bail!("--arm {arm} requires --queue-policy separate");
    }
    Ok(())
}

fn validate_phase_args(args: &Args) -> Result<()> {
    let supplied = [
        args.batch_id.is_some(),
        args.phase_control.is_some(),
        args.warmup_pass.is_some(),
    ];
    if supplied.iter().any(|value| *value) && !supplied.iter().all(|value| *value) {
        anyhow::bail!("--batch-id, --phase-control, and --warmup-pass are all-or-none");
    }
    if supplied.iter().all(|value| *value) && args.tracks != TrackSel::Both {
        anyhow::bail!("registered warmup phase control requires --tracks both");
    }
    Ok(())
}

fn topology_phase_arm(topology: Topology) -> &'static str {
    match topology {
        Topology::Relay => "M",
        Topology::Direct => "Md",
    }
}

/// Resolve once the latest-generation PC and haptic producers have both
/// completed: producer loop finished at the run end AND forwarder closed
/// cleanly with the produced-count check passed.
///
/// The registry bumps `terminal_rx` under its own lock on every terminal or
/// forwarder transition, and the check below holds that same lock after
/// marking the current version seen, so a transition can never fall between
/// the check and the `changed()` wait.
async fn wait_s3_current_routes_completed(
    registry: &Arc<Mutex<SubscriptionProducerRegistry>>,
    terminal_rx: &mut tokio::sync::watch::Receiver<u64>,
) -> Result<()> {
    loop {
        terminal_rx.borrow_and_update();
        let completed = registry
            .lock()
            .map_err(|_| anyhow::anyhow!("S3 producer registry poisoned"))?
            .current_routes_completed();
        if completed {
            return Ok(());
        }
        terminal_rx
            .changed()
            .await
            .map_err(|_| anyhow::anyhow!("S3 producer terminal watch closed"))?;
    }
}

/// Verdict for a session/namespace end observed by the S3 send loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransportEndVerdict {
    /// Both current routes completed the run: the transport end is the
    /// peer's normal post-FIN close.
    Normal,
    /// Both producer loops reached the run end but a forwarder outcome is not
    /// recorded yet; wait (bounded) for the producer tasks to record it.
    AwaitForwarders,
    /// A role is running, remote-closed, errored, or its forwarder failed:
    /// the transport end is a mid-run fault.
    Error,
}

/// Pure decision for a transport end observed at a given completion state.
fn session_end_after_completion(state: CompletionState) -> TransportEndVerdict {
    match state {
        CompletionState::Completed => TransportEndVerdict::Normal,
        CompletionState::Pending => TransportEndVerdict::AwaitForwarders,
        CompletionState::Incomplete => TransportEndVerdict::Error,
    }
}

/// Re-check completion when the session or namespace task ends. Returns
/// `true` when the run is complete (normal end), `false` otherwise.
///
/// `PUBLISH_DONE` is emitted by `Drop for Subscribed` inside
/// `Subscribed::serve_accepted`, which runs before the producer task can
/// record `forward_closed`; a zero-latency peer close can therefore observe
/// `Pending` here. In that state both producer loops have already reached the
/// run end, and their tasks will record `Closed` or `Failed` within the
/// producer shutdown timeout regardless of the session state, so the wait is
/// bounded by `settle_bound` and never widens the error window.
async fn settle_s3_transport_end(
    registry: &Arc<Mutex<SubscriptionProducerRegistry>>,
    settle_bound: Duration,
) -> Result<bool> {
    let mut terminal_rx = registry
        .lock()
        .map_err(|_| anyhow::anyhow!("S3 producer registry poisoned"))?
        .terminal_watch();
    let deadline = tokio::time::Instant::now() + settle_bound;
    loop {
        terminal_rx.borrow_and_update();
        let state = registry
            .lock()
            .map_err(|_| anyhow::anyhow!("S3 producer registry poisoned"))?
            .completion_state();
        match session_end_after_completion(state) {
            TransportEndVerdict::Normal => return Ok(true),
            TransportEndVerdict::Error => return Ok(false),
            TransportEndVerdict::AwaitForwarders => {}
        }
        match tokio::time::timeout_at(deadline, terminal_rx.changed()).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => return Err(anyhow::anyhow!("S3 producer terminal watch closed")),
            Err(_) => return Ok(false),
        }
    }
}

fn json_text(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

async fn sleep_monotonic_until(target_us: u64) {
    let wait = target_us.saturating_sub(now_us());
    if wait > 0 {
        tokio::time::sleep(Duration::from_micros(wait)).await;
    }
}

#[allow(clippy::too_many_arguments)]
/// Where registered warmup objects are written. They must take the exact
/// route the measurement objects take, so the t0 edge changes nothing.
enum WarmupRoute<'a> {
    Separate {
        pc_sub: &'a mut moq_transport::serve::SubgroupsWriter,
        pc_long_sg: &'a mut Option<moq_transport::serve::SubgroupWriter>,
        haptic_sg: &'a mut moq_transport::serve::SubgroupWriter,
    },
    Shared(&'a SharedSubgroup),
}

async fn send_static_registered_warmup(
    args: &Args,
    phase: &skew_moq::phase::PhaseControl,
    frames: &Arc<Vec<Vec<u8>>>,
    pcm: &Arc<Vec<u8>>,
    logger: &Arc<Mutex<JsonlLogger>>,
    route: WarmupRoute<'_>,
    ratio: u64,
) -> Result<()> {
    let pc_count = args
        .pc_rate_hz
        .checked_mul(3)
        .context("PC warmup count overflow")?;
    let haptic_count = args
        .haptic_rate_hz
        .checked_mul(3)
        .context("haptic warmup count overflow")?;
    let frame_subgroups = args.arm.pc_frame_subgroups();
    let (pc_priority, _) = resolved_priorities(args.arm, priority_profile(args));
    let (mut pc_sub, mut pc_long_sg, mut haptic_sg, shared) = match route {
        WarmupRoute::Separate {
            pc_sub,
            pc_long_sg,
            haptic_sg,
        } => (Some(pc_sub), Some(pc_long_sg), Some(haptic_sg), None),
        WarmupRoute::Shared(shared) => (None, None, None, Some(shared)),
    };
    let transport_track = shared.map(|_| SHARED_FIFO_TRACK);

    let pc = async {
        for index in 0..pc_count {
            sleep_monotonic_until(
                phase
                    .warmup_start_us
                    .saturating_add(timestamp_us(index, args.pc_rate_hz)),
            )
            .await;
            let pts_us = timestamp_us(index, args.pc_rate_hz);
            let seq = warmup_seq(index)?;
            let payload = &frames[(index as usize) % frames.len()];
            let (t_gen, frame_obj) = if let Some(shared) = shared {
                let appended = shared_fifo_append(shared, object_buffer(payload), |t_gen| {
                    pack_header(TRACK_PC, args.tier, seq, pts_us, (index + 1) as u32, t_gen, payload.len() as u32)
                })?;
                (appended.t_gen, Some(appended.identity))
            } else {
            let pc_sub = pc_sub.as_deref_mut().context("PC warmup subgroups missing")?;
            let pc_long_sg = pc_long_sg
                .as_deref_mut()
                .context("PC warmup long subgroup slot missing")?;
            let t_gen = now_us();
            let object_payloads: Vec<Cow<'_, [u8]>> = match args.payload_mode {
                PayloadMode::Frame => vec![Cow::Borrowed(payload.as_slice())],
                PayloadMode::EqualChunk => equal_chunks(payload, seq, args.chunk_bytes)?
                    .into_iter()
                    .map(Cow::Owned)
                    .collect(),
            };
            let mut frame_sg = if frame_subgroups {
                Some(
                    pc_sub
                        .append(pc_priority)
                        .context("PC warmup frame append")?,
                )
            } else {
                None
            };
            let mut frame_obj = None;
            for object_payload in &object_payloads {
                let subgroup = match frame_sg.as_mut() {
                    Some(value) => value,
                    None => pc_long_sg
                        .as_mut()
                        .context("PC warmup long subgroup missing")?,
                };
                let header = pack_header(
                    TRACK_PC,
                    args.tier,
                    seq,
                    pts_us,
                    (index + 1) as u32,
                    t_gen,
                    object_payload.len() as u32,
                );
                let mut bytes = Vec::with_capacity(HDR + object_payload.len());
                bytes.extend_from_slice(&header);
                bytes.extend_from_slice(object_payload.as_ref());
                let identity = (subgroup.group_id, subgroup.subgroup_id);
                let mut object = subgroup
                    .create(bytes.len(), None)
                    .context("PC warmup create")?;
                let object_id = object.object_id;
                object
                    .write(Bytes::from(bytes))
                    .context("PC warmup write")?;
                drop(object);
                if args.payload_mode == PayloadMode::Frame {
                    frame_obj = Some((identity.0, identity.1, object_id));
                }
            }
            drop(frame_sg);
            (t_gen, frame_obj)
            };
            logger
                .lock()
                .map_err(|_| anyhow::anyhow!("TX logger poisoned"))?
                .try_log_warmup_tx_transport(
                    "pc",
                    args.tier,
                    seq,
                    pts_us,
                    (index + 1) as u32,
                    payload.len(),
                    t_gen,
                    now_us(),
                    frame_obj,
                    transport_track,
                )?;
        }
        Ok::<(), anyhow::Error>(())
    };

    let haptic = async {
        for tick in 0..haptic_count {
            sleep_monotonic_until(
                phase
                    .warmup_start_us
                    .saturating_add(timestamp_us(tick, args.haptic_rate_hz)),
            )
            .await;
            let (pts_us, event_id) = if tick % ratio == 0 {
                let frame = tick / ratio;
                (timestamp_us(frame, args.pc_rate_hz), (frame + 1) as u32)
            } else {
                (timestamp_us(tick, args.haptic_rate_hz), 0)
            };
            let seq = warmup_seq(tick)?;
            let payload = pcm_tick_payload(pcm, tick, PCM_SAMPLE_RATE_HZ, args.haptic_rate_hz)?;
            let (t_gen, frame_obj) = if let Some(shared) = shared {
                let appended = shared_fifo_append(shared, object_buffer(&payload), |t_gen| {
                    pack_header(TRACK_HAPTIC, HAPTIC_TIER_FULL, seq, pts_us, event_id, t_gen, payload.len() as u32)
                })?;
                (appended.t_gen, Some(appended.identity))
            } else {
            let haptic_sg = haptic_sg
                .as_deref_mut()
                .context("haptic warmup subgroup missing")?;
            let t_gen = now_us();
            let object_payloads: Vec<Cow<'_, [u8]>> = match args.payload_mode {
                PayloadMode::Frame => vec![Cow::Borrowed(payload.as_slice())],
                PayloadMode::EqualChunk => equal_chunks(&payload, seq, args.chunk_bytes)?
                    .into_iter()
                    .map(Cow::Owned)
                    .collect(),
            };
            let mut frame_obj = None;
            for object_payload in &object_payloads {
                let header = pack_header(
                    TRACK_HAPTIC,
                    HAPTIC_TIER_FULL,
                    seq,
                    pts_us,
                    event_id,
                    t_gen,
                    object_payload.len() as u32,
                );
                let mut bytes = Vec::with_capacity(HDR + object_payload.len());
                bytes.extend_from_slice(&header);
                bytes.extend_from_slice(object_payload.as_ref());
                let identity = (haptic_sg.group_id, haptic_sg.subgroup_id);
                let mut object = haptic_sg
                    .create(bytes.len(), None)
                    .context("haptic warmup create")?;
                let object_id = object.object_id;
                object
                    .write(Bytes::from(bytes))
                    .context("haptic warmup write")?;
                drop(object);
                if args.payload_mode == PayloadMode::Frame {
                    frame_obj = Some((identity.0, identity.1, object_id));
                }
            }
            (t_gen, frame_obj)
            };
            logger
                .lock()
                .map_err(|_| anyhow::anyhow!("TX logger poisoned"))?
                .try_log_warmup_tx_transport(
                    "haptic",
                    HAPTIC_TIER_FULL,
                    seq,
                    pts_us,
                    event_id,
                    payload.len(),
                    t_gen,
                    now_us(),
                    frame_obj,
                    transport_track,
                )?;
        }
        Ok::<(), anyhow::Error>(())
    };

    tokio::try_join!(pc, haptic)?;
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
    validate_stage5_policy(
        args.arm,
        args.payload_mode,
        args.tracks,
        args.queue_policy,
        args.publisher_priority_profile,
        args.data_priority_mapping,
    )?;
    let phase4_transport = phase4_transport(&args)?;
    validate_s3_args(&args)?;
    validate_phase_args(&args)?;
    let (pc_priority, haptic_priority) = resolved_priorities(args.arm, priority_profile(&args));
    let shared_fifo = args.queue_policy == QueuePolicy::SharedFifo;

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
    // S3 and its s3np control read the same three tier directories. S3 hands
    // them to its subscription producers; s3np selects between them per slot.
    let s3_frames = if args.arm.needs_pc_tier_dirs() {
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
    // ONE replay path for both replay arms: identical tier and haptic-density
    // decisions for the same schedule, by construction.
    let tier_replay: Option<Arc<TierReplay>> = if args.arm.replays_tier_schedule() {
        let path = args
            .tier_schedule
            .as_ref()
            .expect("validated s3np tier schedule");
        let document = std::fs::read_to_string(path)
            .with_context(|| format!("read tier schedule {}", path.display()))?;
        let schedule = skew_moq::s3np::TierSchedule::parse(&document)
            .map_err(|error| anyhow::anyhow!("invalid tier schedule: {error:?}"))?;
        // The replay window must cover the run being generated, or the tail of
        // the run would silently hold the last recorded state beyond anything
        // the source S3 run observed.
        let duration_us = Duration::from_secs_f64(args.duration).as_micros() as u64;
        if schedule.duration_us() != duration_us {
            anyhow::bail!(
                "tier schedule covers {}us but this run is {}us; the replay must \
                 come from a source S3 run of the same registered duration",
                schedule.duration_us(),
                duration_us
            );
        }
        // The replay's source rates are already pinned to the v5 contract by
        // the parser; cross-check them against THIS run so a rate mismatch
        // cannot silently change the anchor rule under a valid schedule.
        if schedule.pc_rate_hz() != args.pc_rate_hz
            || schedule.haptic_rate_hz() != args.haptic_rate_hz
        {
            anyhow::bail!(
                "tier schedule was extracted from a {}/{} Hz run but this run is {}/{} Hz",
                schedule.pc_rate_hz(),
                schedule.haptic_rate_hz(),
                args.pc_rate_hz,
                args.haptic_rate_hz
            );
        }
        let sha256: [u8; 32] = {
            use sha2::{Digest, Sha256};
            Sha256::digest(document.as_bytes()).into()
        };
        let (recovery, critical) = s3_frames.as_ref().expect("validated S3 tier frames");
        Some(Arc::new(TierReplay {
            schedule,
            sha256,
            normal: frames.clone(),
            recovery: recovery.clone(),
            critical: critical.clone(),
        }))
    } else {
        None
    };
    let haptic_src = std::path::Path::new(&args.haptic_wav)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_string();

    println!(
        "[tx] frames={} S={}B haptic={}B pc={}Hz haptic={}Hz mode={} chunk={}B tracks={} arm={} queue_policy={} priority pc={} haptic={} -> {}",
        frames.len(), s_bytes, pcm.len(), args.pc_rate_hz, args.haptic_rate_hz,
        args.payload_mode.as_str(), args.chunk_bytes, args.tracks.as_str(), args.arm.as_str(),
        args.queue_policy.as_str(), pc_priority, haptic_priority,
        args.out.display(),
    );
    println!(
        "[tx] preflight mean_frame={:.1}B mean_chunks={:.1} pc_objects/s={:.1} app={:.3}Mbps warning={}",
        pf.frame_mean, pf.chunks_mean, pf.pc_objects_per_s,
        pf.application_object_mbps, pf.pc_objects_per_s >= 50_000.0
    );

    let logger = Arc::new(Mutex::new(JsonlLogger::new(
        &args.out,
        &args.run_id,
        "moq",
        "tx",
        args.c_mbps,
        args.rtt_ms,
        args.jitter_ms,
        args.loss_pct,
        s_bytes,
        args.pc_rate_hz,
        args.haptic_rate_hz,
        args.seed,
        Some(args.duration),
        Some(&haptic_src),
        Some(args.tracks.as_str()),
        Some(TERM_PROTOCOL_V),
        None,
        phase4_transport,
        Some(V5Meta {
            payload_mode: args.payload_mode,
            representation: args.representation,
            topology: args.topology,
            chunk_bytes: args.chunk_bytes,
            queue_policy: Some(args.queue_policy.as_str()),
            replay: tier_replay.as_ref().map(|replay| TierReplayMeta {
                tier_schedule_sha256: replay.sha256,
                tier_schedule_generation: replay.schedule.generation().to_string(),
                tier_schedule_source_run_id: replay.schedule.source_run_id().to_string(),
                tier_schedule_source_tx_sha256: replay
                    .schedule
                    .source_tx_sha256()
                    .to_string(),
                tier_schedule_source_rx_sha256: replay
                    .schedule
                    .source_rx_sha256()
                    .to_string(),
                tier_schedule_switches: replay.schedule.switches().len(),
                // The sender never runs the S1 block, so it always carries the
                // echoed offset here.
                d_play_us: Some(
                    args.d_play_ms
                        .expect("validated replay d-play echo")
                        .saturating_mul(1_000),
                ),
                release_rule_key: replay_release_rule_key(args.arm),
                release_rule: replay_release_rule(args.arm, args.s3np_release_rule),
                // The sender schedules no playout; only the receiver records
                // the release parameters it actually applied.
                release: None,
            }),
        }),
    )?));
    logger.lock().unwrap().log_info(&preflight_info(pf));
    if args.preflight_only {
        logger
            .lock()
            .unwrap()
            .try_log_info("\"event\":\"shutdown\",\"ending\":\"preflight_only\",\"exit_code\":0")?;
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
    let accept_trace_enabled =
        args.accept_trace && (args.payload_mode == PayloadMode::Frame || args.chunk_trace);
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
    // S3 only: how the transport ended when the session/namespace task
    // finished after both current routes had completed the run.
    let mut s3_transport_end: Option<String> = None;

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
            let duration_us = Duration::from_secs_f64(args.duration).as_micros() as u64;
            let (recovery_frames, critical_frames) =
                s3_frames.as_ref().expect("validated S3 frames");
            let registry = Arc::new(Mutex::new(SubscriptionProducerRegistry::new()));
            // The initial subscriptions must exist before the runner can
            // observe readiness and publish phase-control. Producers therefore
            // wait on this one-shot schedule authority instead of inventing an
            // anchor at subscription time.
            let (schedule_tx, schedule_rx) = tokio::sync::watch::channel(None);
            let (measurement_tx, measurement_rx) = tokio::sync::watch::channel(false);
            let context = Arc::new(S3SenderContext {
                schedule: schedule_rx,
                measurement_gate: measurement_rx,
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
            // Producer tasks record the forwarder outcome within their
            // shutdown timeout; allow that plus a margin before a Pending
            // completion is treated as a fault.
            let settle_bound = context
                .shutdown_timeout
                .saturating_add(Duration::from_secs(1));
            let ns_publisher = publisher.clone();
            let ns_registry = registry.clone();
            let ns_routes = s3_accept_routes.clone();
            producers.as_mut().expect("registered above").ns_task = Some(tokio::spawn(
                run_s3_namespace(ns_publisher, namespace, context, ns_registry, ns_routes),
            ));

            let mut terminal_rx = registry
                .lock()
                .map_err(|_| anyhow::anyhow!("S3 producer registry poisoned"))?
                .terminal_watch();
            let run_sequence = async {
                let (schedule, phase) = if let (Some(batch_id), Some(phase_path)) =
                    (args.batch_id.as_deref(), args.phase_control.as_ref())
                {
                    let phase = skew_moq::phase::wait_phase_control(
                        phase_path.clone(),
                        batch_id,
                        &args.run_id,
                        topology_phase_arm(args.topology),
                        Duration::from_secs(30),
                    )
                    .await?;
                    let end_us = phase
                        .t0_us
                        .checked_add(duration_us)
                        .context("S3 registered run end overflow")?;
                    (
                        SourceSchedule {
                            warmup_start_us: Some(phase.warmup_start_us),
                            measurement_start_us: phase.t0_us,
                            end_us,
                        },
                        Some(phase),
                    )
                } else {
                    let warmup_us = Duration::from_secs_f64(args.warmup).as_micros() as u64;
                    let measurement_start_us = now_us()
                        .checked_add(warmup_us)
                        .context("S3 legacy run anchor overflow")?;
                    let end_us = measurement_start_us
                        .checked_add(duration_us)
                        .context("S3 legacy run end overflow")?;
                    (
                        SourceSchedule {
                            warmup_start_us: None,
                            measurement_start_us,
                            end_us,
                        },
                        None,
                    )
                };
                schedule_tx
                    .send(Some(schedule))
                    .map_err(|_| anyhow::anyhow!("S3 schedule consumers closed"))?;
                if let Some(phase) = &phase {
                    skew_moq::phase::require_warmup_pass(
                        args.warmup_pass
                            .clone()
                            .expect("validated warmup pass path"),
                        phase,
                    )
                    .await?;
                }
                sleep_monotonic_until(schedule.measurement_start_us).await;
                logger
                    .lock()
                    .map_err(|_| anyhow::anyhow!("TX logger poisoned"))?
                    .log_measurement_start(schedule.measurement_start_us, 0)
                    .context("failed to record the S3 measurement epoch")?;
                measurement_tx
                    .send(true)
                    .map_err(|_| anyhow::anyhow!("S3 measurement gate consumers closed"))?;
                // The run is complete as soon as both current-route producers
                // have reached end_us and their forwarders closed cleanly, or
                // at the wall-clock end if a producer never reports. Waiting
                // only on the wall clock let the receiver's post-FIN close
                // race ahead of `end_us` under zero shaping and surface as a
                // session error.
                tokio::select! {
                    biased;
                    completed = wait_s3_current_routes_completed(&registry, &mut terminal_rx) => completed?,
                    _ = sleep_monotonic_until(schedule.end_us) => {}
                }
                Ok::<(), anyhow::Error>(())
            };
            let step: Result<()> = {
                let p = producers.as_mut().expect("producers set above");
                let sr = p.session_run.as_mut().expect("session handle present");
                let nt = p.ns_task.as_mut().expect("namespace handle present");
                let mut session_done = None;
                let mut ns_done = None;
                // `biased`: a completed run sequence must win over a session
                // or namespace end that becomes ready in the same poll.
                let outcome = tokio::select! {
                    biased;
                    result = run_sequence => result,
                    _ = wait_signal(&mut sig_rx) => {
                        ending = Ending::Signal;
                        Ok(())
                    }
                    r = sr => {
                        session_done = Some(JoinOutcome::from_join_result(&r));
                        // Re-check completion at this moment: a session end
                        // after both current routes completed the run is the
                        // peer's normal close; anything else stays an error.
                        if settle_s3_transport_end(&registry, settle_bound).await? {
                            s3_transport_end = Some(format!("session_after_completion:{:?}", r));
                            Ok(())
                        } else {
                            Err(anyhow::anyhow!("session ended during S3 send: {:?}", r))
                        }
                    }
                    r = nt => {
                        ns_done = Some(JoinOutcome::from_join_result(&r));
                        if settle_s3_transport_end(&registry, settle_bound).await? {
                            s3_transport_end = Some(format!("namespace_after_completion:{:?}", r));
                            Ok(())
                        } else {
                            Err(anyhow::anyhow!("S3 namespace ended during send: {:?}", r))
                        }
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
        // shared_fifo publishes ONLY the "mixed" track; separate keeps pc+haptic.
        let (pc_tw, hap_tw, mixed_tw) = if shared_fifo {
            (
                None,
                None,
                Some(
                    tracks_w
                        .create(SHARED_FIFO_TRACK)
                        .context("create mixed track")?,
                ),
            )
        } else {
            (
                Some(tracks_w.create("pc").context("create pc track")?),
                Some(tracks_w.create("haptic").context("create haptic track")?),
                None,
            )
        };

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

        // Establish the real subgroup mapping before readiness. In registered
        // mode these exact writers span warmup and measurement, so the t0 edge
        // neither drains backlog nor creates a different transport route.
        let mut pc_state = match pc_tw {
            Some(pc_tw) if args.tracks.pc_on() => {
                let mut subgroups = pc_tw.subgroups().context("pc subgroups")?;
                let long = if args.arm.pc_frame_subgroups() {
                    None
                } else {
                    Some(subgroups.append(pc_priority).context("pc append")?)
                };
                Some((subgroups, long))
            }
            Some(pc_tw) => {
                drop(pc_tw.subgroups().context("pc subgroups")?);
                None
            }
            None => None,
        };
        let mut haptic_state = match hap_tw {
            Some(hap_tw) if args.tracks.haptic_on() => {
                let mut subgroups = hap_tw.subgroups().context("haptic subgroups")?;
                let subgroup = subgroups
                    .append(haptic_priority)
                    .context("haptic append")?;
                Some((subgroups, subgroup))
            }
            Some(hap_tw) => {
                drop(hap_tw.subgroups().context("haptic subgroups")?);
                None
            }
            None => None,
        };
        // shared_fifo: ONE long-lived subgroup, shared by both producer loops
        // behind a lock; both loops hold a clone and the state below is what
        // the drain edge releases (last owner drop == FIN).
        let mut shared_state: Option<(moq_transport::serve::SubgroupsWriter, SharedSubgroup)> =
            match mixed_tw {
                Some(mixed_tw) => {
                    let mut subgroups = mixed_tw.subgroups().context("mixed subgroups")?;
                    let subgroup = subgroups
                        .append(SHARED_FIFO_PRIORITY)
                        .context("mixed append")?;
                    Some((subgroups, Arc::new(Mutex::new(subgroup))))
                }
                None => None,
            };
        let shared_subgroup: Option<SharedSubgroup> =
            shared_state.as_ref().map(|(_, shared)| shared.clone());

        // Sender-side readiness edge (plan §3.1 start timing; ledger
        // `readiness_us`). The sender can prove its own MoQ SETUP and that the
        // track writers exist; it cannot observe the receiver's SUBSCRIBE, so
        // the subscription components stay on the receiver's readiness row.
        // Recorded before any warmup or phase wait so the edge is the same in
        // smoke and registered runs. No data-path change.
        {
            let mut components: Vec<&str> = vec![match args.topology {
                Topology::Relay => "sender_relay_session",
                Topology::Direct => "sender_receiver_session",
            }];
            if pc_state.is_some() {
                components.push("pc_track_writer");
            }
            if haptic_state.is_some() {
                components.push("haptic_track_writer");
            }
            if shared_state.is_some() {
                components.push("mixed_track_writer");
            }
            logger
                .lock()
                .unwrap()
                .log_readiness(now_us(), &components)
                .context("failed to record sender readiness components")?;
        }

        let phase = if let (Some(batch_id), Some(path)) =
            (args.batch_id.as_deref(), args.phase_control.as_ref())
        {
            Some(
                skew_moq::phase::wait_phase_control(
                    path.clone(),
                    batch_id,
                    &args.run_id,
                    topology_phase_arm(args.topology),
                    Duration::from_secs(30),
                )
                .await?,
            )
        } else {
            None
        };
        let t0_us = if let Some(phase) = &phase {
            let route = match shared_subgroup.as_ref() {
                Some(shared) => WarmupRoute::Shared(shared),
                None => {
                    let (pc_sub, pc_long_sg) = pc_state
                        .as_mut()
                        .context("registered warmup requires the PC track")?;
                    let (_, haptic_sg) = haptic_state
                        .as_mut()
                        .context("registered warmup requires the haptic track")?;
                    WarmupRoute::Separate {
                        pc_sub,
                        pc_long_sg,
                        haptic_sg,
                    }
                }
            };
            send_static_registered_warmup(&args, phase, &frames, &pcm, &logger, route, ratio)
                .await?;
            skew_moq::phase::require_warmup_pass(
                args.warmup_pass
                    .clone()
                    .expect("validated warmup pass path"),
                phase,
            )
            .await?;
            sleep_monotonic_until(phase.t0_us).await;
            phase.t0_us
        } else {
            tokio::time::sleep(Duration::from_secs_f64(args.warmup)).await;
            now_us()
        };
        logger
            .lock()
            .unwrap()
            .log_measurement_start(t0_us, 0)
            .context("failed to record the measurement epoch")?;
        let duration_us = Duration::from_secs_f64(args.duration).as_micros() as u64;
        let end_us = t0_us
            .checked_add(duration_us)
            .context("static run end overflow")?;

        // ---- PC loop: rational PC Hz, with the arm-specific subgroup map ----
        let pc_task = if args.tracks.pc_on() {
            let frames = frames.clone();
            let logger = logger.clone();
            let pc_rate_hz = args.pc_rate_hz;
            let tier = args.tier;
            let payload_mode = args.payload_mode;
            let chunk_bytes = args.chunk_bytes;
            let frame_subgroups = args.arm.pc_frame_subgroups();
            let priority = pc_priority;
            let mut state = pc_state.take();
            let shared = shared_subgroup.clone();
            let replay = tier_replay.clone();
            if state.is_none() && shared.is_none() {
                anyhow::bail!("PC state validated");
            }
            Some(tokio::spawn(async move {
                let mut i: u64 = 0;
                let mut stats = TrackRunStats::default();
                while now_us() < end_us {
                    sleep_monotonic_until(t0_us.saturating_add(timestamp_us(i, pc_rate_hz))).await;
                    if now_us() >= end_us {
                        break;
                    }
                    let pts = timestamp_us(i, pc_rate_hz);
                    // s3np: the replayed tier for this slot, looked up on the
                    // NOMINAL slot offset (== pts) so the switch instant is
                    // deterministic and within one frame period of the recorded
                    // S3 switch time. Every other arm keeps its fixed tier.
                    let (tier, frame_set): (u16, &Vec<Vec<u8>>) = match &replay {
                        Some(replay) => {
                            let pc_tier = replay.pc_tier_at(pts);
                            (pc_tier, replay.frames(pc_tier))
                        }
                        None => (tier, &frames),
                    };
                    let payload = &frame_set[(i as usize) % frame_set.len()];
                    // (t_gen, MoQ identity, objects written, padding bytes)
                    let (t_gen, t_ready, frame_obj, objects, padding) = if let Some(shared) = &shared {
                        // shared_fifo (frame mode only): payload copy outside,
                        // t_gen + header + append under the FIFO lock.
                        let buffer = object_buffer(payload);
                        let appended = shared_fifo_append(shared, buffer, |t_gen| {
                            pack_header(TRACK_PC, tier, i as u32, pts, (i + 1) as u32, t_gen, payload.len() as u32)
                        })?;
                        (appended.t_gen, Some(appended.t_ready), Some(appended.identity), 1u64, 0u64)
                    } else {
                        let (pc_sub, long_sg) = state.as_mut().expect("separate PC state");
                        let t_gen = now_us();
                        let object_payloads: Vec<Cow<'_, [u8]>> = match payload_mode {
                            PayloadMode::Frame => vec![Cow::Borrowed(payload.as_slice())],
                            PayloadMode::EqualChunk => equal_chunks(payload, i as u32, chunk_bytes)?
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
                        let padding = if payload_mode == PayloadMode::EqualChunk {
                            (object_payloads.len() * chunk_bytes - payload.len()) as u64
                        } else {
                            0
                        };
                        (t_gen, None, frame_obj, object_payloads.len() as u64, padding)
                    };
                    stats.period.observe(t_gen);
                    logger.lock().unwrap().log_tx_transport(
                        "pc",
                        tier,
                        i as u32,
                        pts,
                        (i + 1) as u32,
                        payload.len(),
                        t_gen,
                        now_us(),
                        frame_obj,
                        shared.as_ref().map(|_| SHARED_FIFO_TRACK),
                        t_ready,
                    );
                    stats.logical_generated += 1;
                    stats.chunks_sent += objects;
                    stats.source_payload_bytes += payload.len() as u64;
                    stats.padding_bytes += padding;
                    i += 1;
                }
                Ok::<_, anyhow::Error>((stats, state))
            }))
        } else {
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
            let mut state = haptic_state.take();
            let shared = shared_subgroup.clone();
            let replay = tier_replay.clone();
            if state.is_none() && shared.is_none() {
                anyhow::bail!("haptic state validated");
            }
            Some(tokio::spawn(async move {
                let mut k: u64 = 0;
                let mut stats = TrackRunStats::default();
                while now_us() < end_us {
                    sleep_monotonic_until(t0_us.saturating_add(timestamp_us(k, haptic_rate_hz)))
                        .await;
                    if now_us() >= end_us {
                        break;
                    }
                    // Replayed haptic density (see `TierReplay::skips_haptic_tick`).
                    if replay
                        .as_ref()
                        .is_some_and(|replay| replay.skips_haptic_tick(k, haptic_rate_hz, ratio))
                    {
                        k += 1;
                        continue;
                    }
                    let (pts, event_id) = if k % ratio == 0 {
                        let fi = k / ratio;
                        (timestamp_us(fi, pc_rate_hz), (fi + 1) as u32)
                    } else {
                        (timestamp_us(k, haptic_rate_hz), 0u32)
                    };
                    let payload = pcm_tick_payload(&pcm, k, PCM_SAMPLE_RATE_HZ, haptic_rate_hz)?;
                    let (t_gen, t_ready, frame_obj, objects, padding) = if let Some(shared) = &shared {
                        let buffer = object_buffer(&payload);
                        let appended = shared_fifo_append(shared, buffer, |t_gen| {
                            pack_header(
                                TRACK_HAPTIC, HAPTIC_TIER_FULL, k as u32, pts, event_id, t_gen,
                                payload.len() as u32,
                            )
                        })?;
                        (appended.t_gen, Some(appended.t_ready), Some(appended.identity), 1u64, 0u64)
                    } else {
                        let (_, sg) = state.as_mut().expect("separate haptic state");
                        let t_gen = now_us();
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
                        let padding = if payload_mode == PayloadMode::EqualChunk {
                            (object_payloads.len() * chunk_bytes - payload.len()) as u64
                        } else {
                            0
                        };
                        (t_gen, None, frame_obj, object_payloads.len() as u64, padding)
                    };
                    stats.period.observe(t_gen);
                    logger.lock().unwrap().log_tx_transport(
                        "haptic",
                        HAPTIC_TIER_FULL,
                        k as u32,
                        pts,
                        event_id,
                        payload.len(),
                        t_gen,
                        now_us(),
                        frame_obj,
                        shared.as_ref().map(|_| SHARED_FIFO_TRACK),
                        t_ready,
                    );
                    stats.logical_generated += 1;
                    stats.chunks_sent += objects;
                    stats.source_payload_bytes += payload.len() as u64;
                    stats.padding_bytes += padding;
                    k += 1;
                }
                Ok::<_, anyhow::Error>((stats, state))
            }))
        } else {
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
                        Some(h) => {
                            let (stats, state) = h.await??;
                            pc_state = state;
                            stats
                        }
                        None => TrackRunStats::default(),
                    };
                    let haptic = match hap_task {
                        Some(h) => {
                            let (stats, state) = h.await??;
                            haptic_state = state;
                            stats
                        }
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
            pc_stats.logical_generated,
            hap_stats.logical_generated,
            pc_stats.chunks_sent,
            hap_stats.chunks_sent,
            args.drain_timeout
        );

        // A registered run keeps both the session and track writers up while
        // the relay forwards backlog. Releasing the writers emits FIN and gives
        // a complete receiver permission to exit. Legacy runs keep their old
        // session-only drain; a signal cuts either wait short for cleanup.
        // The producer loops' clones of the shared subgroup are gone once
        // they were joined above, so `shared_state` is the last owner and
        // releasing it at the drain edge emits the "mixed" FIN.
        drop(shared_subgroup);
        let drain_signalled = hold_static_track_state_through_drain(
            Duration::from_secs_f64(args.drain_timeout),
            sig_rx.clone(),
            (pc_state.take(), shared_state.take()),
            haptic_state.take(),
            phase.is_some(),
        )
        .await;
        if drain_signalled {
            ending = Ending::Signal;
        } else if phase.is_some() && args.topology == Topology::Direct {
            // Dropping the writer state above emits FIN. In a direct arm the
            // finalizer must not immediately abort `session.run()`: that can
            // close QUIC before the receiver observes the FIN and converts a
            // complete 1800/5400 delivery into `session_ended` rc=5. The peer
            // closes after draining both tracks, which is the positive handoff
            // acknowledgement. Relay arms intentionally keep their existing
            // teardown because their sender session is not the downstream
            // receiver session.
            let p = producers.as_mut().expect("producers set above");
            let sr = p
                .session_run
                .as_mut()
                .context("direct session handle missing")?;
            match wait_registered_direct_fin_handoff(
                REGISTERED_DIRECT_FIN_HANDOFF_BUDGET,
                sig_rx.clone(),
                &mut *sr,
            )
            .await
            {
                DirectFinHandoff::PeerClosed(result) => {
                    p.session_seen = Some(JoinOutcome::from_join_result(&result));
                    p.session_run = None;
                }
                DirectFinHandoff::Signal => ending = Ending::Signal,
                DirectFinHandoff::TimedOut => {
                    anyhow::bail!(
                        "direct receiver did not close after registered track FIN handoff"
                    );
                }
            }
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
        let s3_transport_end_field = s3_transport_end
            .as_deref()
            .map(|text| format!(",\"s3_transport_end\":\"{}\"", json_text(text)))
            .unwrap_or_default();
        if lg
            .try_log_info(&format!(
                "\"event\":\"shutdown\",\"ending\":\"{}\",\"session_join\":\"{}\",\"ns_join\":\"{}\"{}",
                ending.as_str(),
                joins.session.as_str(),
                joins.ns.as_str(),
                s3_transport_end_field
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::{
        b1_transport_meta, epoch_record, hold_static_track_state_through_drain, now_us,
        object_buffer, phase4_transport, registered_s_bytes, resolved_priorities,
        json_text, replay_release_rule, replay_release_rule_key, session_end_after_completion,
        shared_fifo_append, timestamp_us, validate_s3_args, validate_stage5_policy, Args, Arm,
        CliReleaseRule, CompletionState, DataPriorityMapping, DirectFinHandoff, Duration,
        PayloadMode, Phase4TransportMeta, PublisherPriorityProfile, QueuePolicy, Representation,
        SharedFifoWriter, TierReplay, Topology, TrackSel, TransportEndVerdict, Url, HDR,
        wait_registered_direct_fin_handoff,
    };
    use std::path::PathBuf;
    use std::sync::Mutex;

    /// In-memory stand-in for the mixed subgroup: records every appended
    /// object in append order and hands out sequential object ids.
    struct RecordingWriter {
        objects: Vec<Vec<u8>>,
    }

    impl SharedFifoWriter for RecordingWriter {
        fn identity(&self) -> (u64, u64) {
            (7, 0)
        }
        fn append(&mut self, bytes: Vec<u8>) -> Result<u64, anyhow::Error> {
            self.objects.push(bytes);
            Ok(self.objects.len() as u64 - 1)
        }
    }

    /// Stage-5 P1 ordering rule: object_id order == t_gen order across two
    /// concurrent producers, because t_gen is sampled under the same lock
    /// that serialises the append.
    #[test]
    fn shared_fifo_append_orders_object_ids_by_t_gen_across_producers() {
        const PER_PRODUCER: u64 = 400;
        let shared = Arc::new(Mutex::new(RecordingWriter { objects: Vec::new() }));
        let producers: Vec<_> = [0u8, 1u8]
            .into_iter()
            .map(|track| {
                let shared = Arc::clone(&shared);
                std::thread::spawn(move || {
                    let mut ids = Vec::new();
                    for _ in 0..PER_PRODUCER {
                        // Payload copied outside the lock; header carries t_gen.
                        let appended = shared_fifo_append(&shared, object_buffer(&[track]), |t_gen| {
                            let mut header = [0u8; HDR];
                            header[..8].copy_from_slice(&t_gen.to_le_bytes());
                            header
                        })
                        .unwrap();
                        let (gid, sgid, oid) = appended.identity;
                        assert_eq!((gid, sgid), (7, 0));
                        assert!(appended.t_ready <= appended.t_gen);
                        ids.push((oid, appended.t_gen));
                        std::thread::yield_now();
                    }
                    ids
                })
            })
            .collect();
        let mut returned: Vec<(u64, u64)> = producers
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        returned.sort_unstable();
        let writer = shared.lock().unwrap();
        assert_eq!(writer.objects.len() as u64, 2 * PER_PRODUCER);
        let mut previous = 0u64;
        let mut per_track = [0u64; 2];
        for (oid, bytes) in writer.objects.iter().enumerate() {
            let t_gen = u64::from_le_bytes(bytes[..8].try_into().unwrap());
            assert!(t_gen >= previous, "object {oid}: t_gen {t_gen} < previous {previous}");
            previous = t_gen;
            assert_eq!(bytes.len(), HDR + 1, "header placeholder + 1-byte payload");
            per_track[bytes[HDR] as usize] += 1;
            // The identity handed back to the caller is the one recorded.
            assert_eq!(returned[oid], (oid as u64, t_gen));
        }
        assert_eq!(per_track, [PER_PRODUCER, PER_PRODUCER]);
    }

    /// Lock-wait boundary: `t_ready` precedes the lock attempt and `t_gen`
    /// is taken under the lock, so a contender that has to wait for a holder
    /// shows `t_gen - t_ready` at least as long as the hold.
    #[test]
    fn shared_fifo_t_ready_precedes_t_gen_and_exposes_the_lock_wait() {
        const HOLD: Duration = Duration::from_millis(30);
        let shared = Arc::new(Mutex::new(RecordingWriter { objects: Vec::new() }));
        // Uncontended: t_ready <= t_gen and both are real monotonic readings.
        let before = now_us();
        let free = shared_fifo_append(&shared, object_buffer(&[9, 9, 9]), |_| [0u8; HDR]).unwrap();
        assert!(before <= free.t_ready && free.t_ready <= free.t_gen && free.t_gen <= now_us());
        assert_eq!(free.identity, (7, 0, 0));
        assert_eq!(shared.lock().unwrap().objects[0][HDR..], [9, 9, 9]);

        // Contended: hold the lock for HOLD while the contender is already
        // past its t_ready sample.
        let holder = shared.lock().unwrap();
        let contender = {
            let shared = Arc::clone(&shared);
            std::thread::spawn(move || {
                shared_fifo_append(&shared, object_buffer(&[1]), |_| [0u8; HDR]).unwrap()
            })
        };
        std::thread::sleep(HOLD);
        drop(holder);
        let waited = contender.join().unwrap();
        let lock_wait_us = waited.t_gen - waited.t_ready;
        // Lower bound only: the contender may have started its wait a little
        // after the hold began, but it cannot have acquired the lock before
        // the hold ended, and thread start-up only shortens the measured wait
        // if it exceeds HOLD — which a 30 ms hold rules out on a sane host.
        assert!(
            lock_wait_us >= (HOLD.as_micros() as u64) / 2,
            "lock wait {lock_wait_us}us did not reflect a {HOLD:?} hold"
        );
        assert_eq!(waited.identity, (7, 0, 1));
    }

    #[test]
    fn stage5_policy_is_b1_frame_both_only_and_rejects_mixed_priority() {
        use DataPriorityMapping::{LegacyV1, MoqtV2};
        use PayloadMode::{EqualChunk, Frame};
        use PublisherPriorityProfile::{Equal128, HapticFirst};
        use QueuePolicy::{Separate, SharedFifo};
        let ok = |arm, mode, tracks, q, p, m| validate_stage5_policy(arm, mode, tracks, q, p, m);
        // Defaults never reject any arm.
        for arm in [Arm::B1, Arm::S1, Arm::M1, Arm::S2, Arm::S2Eq, Arm::S3] {
            ok(arm, Frame, TrackSel::Both, Separate, None, MoqtV2).unwrap();
        }
        ok(Arm::B1, Frame, TrackSel::Both, SharedFifo, None, LegacyV1).unwrap();
        ok(Arm::B1, Frame, TrackSel::Both, Separate, Some(HapticFirst), MoqtV2).unwrap();
        ok(Arm::B1, Frame, TrackSel::Both, Separate, Some(Equal128), LegacyV1).unwrap();
        let err = |r: Result<(), anyhow::Error>| r.unwrap_err().to_string();
        assert!(err(ok(Arm::S1, Frame, TrackSel::Both, SharedFifo, None, MoqtV2))
            .contains("requires --arm b1"));
        assert!(err(ok(Arm::B1, EqualChunk, TrackSel::Both, SharedFifo, None, MoqtV2))
            .contains("--payload-mode frame"));
        assert!(err(ok(Arm::B1, Frame, TrackSel::Pc, SharedFifo, None, MoqtV2))
            .contains("--tracks both"));
        assert!(err(ok(Arm::S2, Frame, TrackSel::Both, Separate, Some(Equal128), MoqtV2))
            .contains("requires --arm b1"));
        assert!(err(ok(Arm::S2, Frame, TrackSel::Both, Separate, Some(HapticFirst), MoqtV2))
            .contains("requires --arm b1"));
        assert!(err(ok(Arm::B1, Frame, TrackSel::Both, SharedFifo, Some(HapticFirst), MoqtV2))
            .contains("one priority"));
        assert!(err(ok(Arm::B1, Frame, TrackSel::Both, Separate, Some(HapticFirst), LegacyV1))
            .contains("moqt-v2"));
    }

    #[test]
    fn b1_priorities_and_meta_follow_the_profile_without_touching_other_arms() {
        use PublisherPriorityProfile::{Equal128, HapticFirst};
        assert_eq!(resolved_priorities(Arm::B1, Equal128), (128, 128));
        assert_eq!(resolved_priorities(Arm::B1, HapticFirst), (1, 0));
        // Non-B1 arms ignore the profile entirely.
        assert_eq!(resolved_priorities(Arm::S2, HapticFirst), (1, 0));
        assert_eq!(resolved_priorities(Arm::S2Eq, HapticFirst), (128, 128));
        assert_eq!(resolved_priorities(Arm::S1, HapticFirst), (128, 128));
        assert_eq!(resolved_priorities(Arm::M1, HapticFirst), (128, 128));

        // Historical B1 records no phase4 transport block at all.
        assert!(b1_transport_meta(QueuePolicy::Separate, Equal128, DataPriorityMapping::MoqtV2)
            .is_none());
        let p3 = b1_transport_meta(QueuePolicy::Separate, HapticFirst, DataPriorityMapping::MoqtV2)
            .unwrap();
        assert_eq!(p3.arm, "b1");
        assert_eq!(p3.pc_subgroup_mapping, "long-lived-subgroup");
        assert_eq!((p3.pc_publisher_priority, p3.haptic_publisher_priority), (1, 0));
        assert_eq!(p3.publisher_priority_profile, "relative-haptic0-pc1");
        assert_eq!(p3.data_priority_mapping, "moqt-v2");
        assert_eq!(p3.pc_delivery_timeout_ms, None);
        let p1 = b1_transport_meta(QueuePolicy::SharedFifo, Equal128, DataPriorityMapping::MoqtV2)
            .unwrap();
        assert_eq!(p1.pc_subgroup_mapping, "shared-single-subgroup");
        assert_eq!((p1.pc_publisher_priority, p1.haptic_publisher_priority), (128, 128));
        assert_eq!(p1.publisher_priority_profile, "equal-128");
        assert_eq!(p1.pc_delivery_timeout_ms, None);
    }

    /// Minimal `Args` fixture for the arm-boundary validators. Every field is
    /// explicit so that adding a CLI option fails here instead of silently
    /// defaulting inside a boundary test.
    fn arm_args(arm: Arm) -> Args {
        Args {
            relay: Url::parse("https://127.0.0.1:1").unwrap(),
            run_id: "t".into(),
            out: PathBuf::from("/dev/null"),
            duration: 60.0,
            frames_dir: Some("datasets/d8".into()),
            dummy_size: None,
            tier: 2,
            haptic_wav: "datasets/haptic.wav".into(),
            pc_rate_hz: 30,
            haptic_rate_hz: 90,
            payload_mode: PayloadMode::Frame,
            representation: Representation::Bin,
            topology: Topology::Relay,
            chunk_bytes: 178,
            preflight_only: false,
            chunk_trace: false,
            warmup: 1.0,
            batch_id: None,
            phase_control: None,
            warmup_pass: None,
            drain_timeout: 10.0,
            c_mbps: None,
            rtt_ms: 0.0,
            jitter_ms: 0.0,
            loss_pct: 0.0,
            seed: 0,
            data_priority_mapping: DataPriorityMapping::MoqtV2,
            accept_trace: true,
            accept_trace_capacity: 65536,
            tracks: TrackSel::Both,
            arm,
            queue_policy: QueuePolicy::Separate,
            publisher_priority_profile: None,
            pc_delivery_timeout_ms: None,
            s3_recovery_frames_dir: None,
            s3_critical_frames_dir: None,
            s3_producer_shutdown_timeout_ms: None,
            tier_schedule: None,
            d_play_ms: None,
            s3np_release_rule: None,
        }
    }

    #[test]
    fn s3np_shares_s2_transport_policy_and_stays_exclusive_from_s3() {
        // Wire policy: identical to S2, so S3 − S3NP isolates event-pair
        // preservation rather than priority, mapping, or timeout.
        assert!(Arm::S3np.pc_frame_subgroups());
        assert_eq!(
            (Arm::S3np.pc_priority(), Arm::S3np.haptic_priority()),
            (Arm::S2.pc_priority(), Arm::S2.haptic_priority())
        );
        assert_eq!(Arm::S3np.as_str(), "s3np");
        assert_eq!(
            resolved_priorities(Arm::S3np, PublisherPriorityProfile::HapticFirst),
            (1, 0),
            "the B1-only profile must not move the s3np priorities"
        );

        let mut args = arm_args(Arm::S3np);
        args.s3_recovery_frames_dir = Some("datasets/d7".into());
        args.s3_critical_frames_dir = Some("datasets/d6".into());
        args.tier_schedule = Some(PathBuf::from("schedule.json"));
        args.d_play_ms = Some(50);
        args.s3np_release_rule = Some(CliReleaseRule::PerTrackEpoch);
        // The frozen 67 ms PC delivery timeout is inherited, not chosen.
        assert!(phase4_transport(&args).is_err(), "timeout is required");
        args.pc_delivery_timeout_ms = Some(100);
        assert!(phase4_transport(&args)
            .unwrap_err()
            .to_string()
            .contains("67ms"));
        args.pc_delivery_timeout_ms = Some(67);
        let meta = phase4_transport(&args).unwrap().unwrap();
        assert_eq!(meta.arm, "s3np");
        assert_eq!(meta.pc_subgroup_mapping, "frame-per-subgroup");
        assert_eq!((meta.pc_publisher_priority, meta.haptic_publisher_priority), (1, 0));
        assert_eq!(meta.publisher_priority_profile, "relative-haptic0-pc1");
        assert_eq!(meta.pc_delivery_timeout_ms, Some(67));
        // v5 requires the MoQT priority mapping for every prioritised arm.
        args.data_priority_mapping = DataPriorityMapping::LegacyV1;
        assert!(phase4_transport(&args).is_err());
        args.data_priority_mapping = DataPriorityMapping::MoqtV2;
        assert!(validate_s3_args(&args).is_ok());

        // Exclusivity both ways: S3 never replays a schedule, s3np never owns
        // subscription producers, and neither set leaks onto another arm.
        args.s3_producer_shutdown_timeout_ms = Some(2_000);
        assert!(validate_s3_args(&args)
            .unwrap_err()
            .to_string()
            .contains("requires --arm s3"));
        args.s3_producer_shutdown_timeout_ms = None;

        let mut s3 = arm_args(Arm::S3);
        s3.pc_delivery_timeout_ms = Some(67);
        s3.s3_recovery_frames_dir = Some("datasets/d7".into());
        s3.s3_critical_frames_dir = Some("datasets/d6".into());
        s3.s3_producer_shutdown_timeout_ms = Some(2_000);
        assert!(validate_s3_args(&s3).is_ok());
        s3.tier_schedule = Some(PathBuf::from("schedule.json"));
        assert!(validate_s3_args(&s3)
            .unwrap_err()
            .to_string()
            .contains("require a replay arm"));
        s3.tier_schedule = None;
        s3.d_play_ms = Some(50);
        assert!(validate_s3_args(&s3).is_err());
        s3.d_play_ms = None;
        s3.s3np_release_rule = Some(CliReleaseRule::PerTrackEpoch);
        assert!(validate_s3_args(&s3).is_err());

        let mut s2 = arm_args(Arm::S2);
        s2.pc_delivery_timeout_ms = Some(67);
        s2.tier_schedule = Some(PathBuf::from("schedule.json"));
        assert!(validate_s3_args(&s2).is_err());
        s2.tier_schedule = None;
        s2.d_play_ms = Some(50);
        assert!(validate_s3_args(&s2).is_err());
        s2.d_play_ms = None;
        s2.s3np_release_rule = Some(CliReleaseRule::PerTrackEpoch);
        assert!(validate_s3_args(&s2).is_err());
        s2.s3np_release_rule = None;
        s2.s3_recovery_frames_dir = Some("datasets/d7".into());
        assert!(validate_s3_args(&s2).is_err());

        // Required s3np inputs, each individually.
        let mut missing = arm_args(Arm::S3np);
        missing.pc_delivery_timeout_ms = Some(67);
        missing.s3_recovery_frames_dir = Some("datasets/d7".into());
        missing.s3_critical_frames_dir = Some("datasets/d6".into());
        missing.d_play_ms = Some(50);
        missing.s3np_release_rule = Some(CliReleaseRule::AbsoluteTGen);
        assert!(validate_s3_args(&missing)
            .unwrap_err()
            .to_string()
            .contains("--tier-schedule"));
        missing.tier_schedule = Some(PathBuf::from("schedule.json"));
        // Fail closed: neither endpoint has a default release rule.
        missing.s3np_release_rule = None;
        assert!(validate_s3_args(&missing)
            .unwrap_err()
            .to_string()
            .contains("--s3np-release-rule"));
        missing.s3np_release_rule = Some(CliReleaseRule::PerTrackEpoch);
        missing.d_play_ms = None;
        assert!(validate_s3_args(&missing).is_err());
        missing.d_play_ms = Some(75);
        assert!(validate_s3_args(&missing)
            .unwrap_err()
            .to_string()
            .contains("50 or 100"));
        missing.d_play_ms = Some(50);
        missing.s3_critical_frames_dir = None;
        assert!(validate_s3_args(&missing)
            .unwrap_err()
            .to_string()
            .contains("d6"));
        missing.s3_critical_frames_dir = Some("datasets/d6".into());
        missing.tier = 3;
        assert!(validate_s3_args(&missing)
            .unwrap_err()
            .to_string()
            .contains("header tier 2"));
        missing.tier = 2;
        missing.tracks = TrackSel::Pc;
        assert!(validate_s3_args(&missing)
            .unwrap_err()
            .to_string()
            .contains("--tracks both"));
        missing.tracks = TrackSel::Both;
        missing.queue_policy = QueuePolicy::SharedFifo;
        assert!(validate_s3_args(&missing).is_err());
    }

    fn replay_schedule() -> skew_moq::s3np::TierSchedule {
        let document = format!(
            "{{\"schema\":\"{}\",\"generation\":\"{}\",\"source_run_id\":\"src\",\
             \"source_tx_sha256\":\"{}\",\"source_rx_sha256\":\"{}\",\
             \"pc_rate_hz\":30,\"haptic_rate_hz\":90,\"duration_us\":60000000,\"switches\":[\
             {{\"t_offset_us\":0,\"pc_tier\":2,\"haptic_density\":\"full\"}},\
             {{\"t_offset_us\":1000000,\"pc_tier\":4,\"haptic_density\":\"essential\"}},\
             {{\"t_offset_us\":2000000,\"pc_tier\":3,\"haptic_density\":\"full\"}}]}}",
            skew_moq::s3np::TIER_SCHEDULE_SCHEMA,
            skew_moq::s3np::TIER_SCHEDULE_GENERATION,
            "0a".repeat(32),
            "1b".repeat(32),
        );
        skew_moq::s3np::TierSchedule::parse(&document).expect("fixture schedule must parse")
    }

    fn replay(schedule: skew_moq::s3np::TierSchedule) -> TierReplay {
        TierReplay {
            schedule,
            sha256: [7u8; 32],
            // Distinct frame counts per tier so a wrong tier selection would
            // also show up as a wrong frame set.
            normal: Arc::new(vec![vec![0u8; 100], vec![0u8; 101]]),
            recovery: Arc::new(vec![vec![0u8; 50], vec![0u8; 51], vec![0u8; 52]]),
            critical: Arc::new(vec![vec![0u8; 10]]),
        }
    }

    #[test]
    fn both_replay_arms_share_one_sender_path_and_decide_identically() {
        // The sender difference between s3np and s3r is the recorded arm name and
        // nothing else: they must select the same tier, the same frame set and
        // the same haptic tick set for the same schedule. That is guaranteed
        // structurally — `TierReplay` takes no arm — and pinned here.
        assert!(Arm::S3np.replays_tier_schedule());
        assert!(Arm::S3r.replays_tier_schedule());
        for other in [Arm::B1, Arm::S1, Arm::M1, Arm::S2, Arm::S2Eq, Arm::S3] {
            assert!(
                !other.replays_tier_schedule(),
                "{} must not replay a schedule",
                other.as_str()
            );
        }
        assert!(Arm::S3.needs_pc_tier_dirs());
        assert!(Arm::S3np.needs_pc_tier_dirs());
        assert!(Arm::S3r.needs_pc_tier_dirs());
        assert!(!Arm::S2.needs_pc_tier_dirs());

        // Identical wire policy, so S3R - S3NP cannot be a transport difference.
        assert_eq!(
            Arm::S3r.pc_frame_subgroups(),
            Arm::S3np.pc_frame_subgroups()
        );
        assert_eq!(
            (Arm::S3r.pc_priority(), Arm::S3r.haptic_priority()),
            (Arm::S3np.pc_priority(), Arm::S3np.haptic_priority())
        );

        let replay = replay(replay_schedule());
        // 30 Hz PC slots across both switch boundaries (1.0 s and 2.0 s).
        let decisions: Vec<(u16, usize)> = (0..90u64)
            .map(|slot| {
                let pts = timestamp_us(slot, 30);
                let tier = replay.pc_tier_at(pts);
                (tier, replay.frames(tier).len())
            })
            .collect();
        assert_eq!(decisions[0], (2, 2));
        assert_eq!(decisions[29], (2, 2), "slot 29 pts=966666 is still Normal");
        assert_eq!(decisions[30], (4, 1), "slot 30 pts=1000000 is the boundary");
        assert_eq!(decisions[59], (4, 1));
        assert_eq!(decisions[60], (3, 3), "slot 60 pts=2000000 is the boundary");
        assert_eq!(decisions[89], (3, 3));

        // Haptic: every tick kept under Full, only anchor ticks under Essential.
        let kept: Vec<u64> = (0..270u64)
            .filter(|tick| !replay.skips_haptic_tick(*tick, 90, 3))
            .collect();
        assert_eq!(kept.len(), 90 + 30 + 90);
        assert!((0..90).all(|tick| kept.contains(&tick)), "Full keeps all");
        for tick in 90..180u64 {
            assert_eq!(
                kept.contains(&tick),
                tick % 3 == 0,
                "Essential keeps only the anchor tick {tick}"
            );
        }
        assert!((180..270).all(|tick| kept.contains(&tick)), "Full keeps all");
    }

    #[test]
    fn s3r_records_its_own_release_rule_under_its_own_key() {
        // Distinct KEY and distinct VALUE, so no metadata row can be read as the
        // other replay arm even if a value were copied.
        assert_eq!(
            replay_release_rule_key(Arm::S3r),
            skew_moq::s3np::S3R_RELEASE_RULE_KEY
        );
        assert_eq!(
            replay_release_rule_key(Arm::S3np),
            skew_moq::s3np::S3NP_RELEASE_RULE_KEY
        );
        assert_ne!(
            replay_release_rule_key(Arm::S3r),
            replay_release_rule_key(Arm::S3np)
        );
        assert_eq!(
            replay_release_rule(Arm::S3r, None),
            "common_timeline_first_exact_pair_epoch_plus_d_play"
        );
        assert_eq!(
            replay_release_rule(Arm::S3np, Some(CliReleaseRule::PerTrackEpoch)),
            "per_track_first_object_epoch_plus_d_play"
        );
        assert_eq!(
            replay_release_rule(Arm::S3np, Some(CliReleaseRule::AbsoluteTGen)),
            "absolute_t_gen_plus_d_play"
        );
    }

    #[test]
    fn s3r_transport_meta_differs_from_s3np_only_in_the_arm_name() {
        let mut np = arm_args(Arm::S3np);
        np.pc_delivery_timeout_ms = Some(67);
        np.s3_recovery_frames_dir = Some("datasets/d7".into());
        np.s3_critical_frames_dir = Some("datasets/d6".into());
        np.tier_schedule = Some(PathBuf::from("schedule.json"));
        np.d_play_ms = Some(50);
        np.s3np_release_rule = Some(CliReleaseRule::PerTrackEpoch);
        assert!(validate_s3_args(&np).is_ok());
        let np_meta = phase4_transport(&np).unwrap().unwrap();

        let mut r = arm_args(Arm::S3r);
        r.pc_delivery_timeout_ms = Some(67);
        r.s3_recovery_frames_dir = Some("datasets/d7".into());
        r.s3_critical_frames_dir = Some("datasets/d6".into());
        r.tier_schedule = Some(PathBuf::from("schedule.json"));
        r.d_play_ms = Some(50);
        assert!(validate_s3_args(&r).is_ok());
        let r_meta = phase4_transport(&r).unwrap().unwrap();

        assert_eq!(r_meta.arm, "s3r");
        assert_eq!(np_meta.arm, "s3np");
        assert_eq!(
            Phase4TransportMeta { arm: "s3np", ..r_meta },
            np_meta,
            "the two replay arms must differ in nothing but the arm name"
        );

        // v5 requires the MoQT priority mapping for every prioritised arm.
        r.data_priority_mapping = DataPriorityMapping::LegacyV1;
        assert!(phase4_transport(&r)
            .unwrap_err()
            .to_string()
            .contains("moqt-v2"));
        r.data_priority_mapping = DataPriorityMapping::MoqtV2;

        // s3r must refuse the s3np-only rule selector: it has exactly one rule.
        r.s3np_release_rule = Some(CliReleaseRule::AbsoluteTGen);
        assert!(validate_s3_args(&r)
            .unwrap_err()
            .to_string()
            .contains("requires --arm s3np"));
        r.s3np_release_rule = None;

        // Required replay inputs, individually.
        r.tier_schedule = None;
        assert!(validate_s3_args(&r)
            .unwrap_err()
            .to_string()
            .contains("--tier-schedule"));
        r.tier_schedule = Some(PathBuf::from("schedule.json"));
        r.d_play_ms = None;
        assert!(validate_s3_args(&r)
            .unwrap_err()
            .to_string()
            .contains("--d-play-ms"));
        r.d_play_ms = Some(75);
        assert!(validate_s3_args(&r)
            .unwrap_err()
            .to_string()
            .contains("50 or 100"));
        r.d_play_ms = Some(50);
        // The frozen 67 ms timeout is inherited, like S3 and s3np.
        r.pc_delivery_timeout_ms = Some(100);
        assert!(phase4_transport(&r)
            .unwrap_err()
            .to_string()
            .contains("67ms"));
        r.pc_delivery_timeout_ms = Some(67);
        // No subscription producers: s3r publishes the static mapping.
        r.s3_producer_shutdown_timeout_ms = Some(2_000);
        assert!(validate_s3_args(&r)
            .unwrap_err()
            .to_string()
            .contains("requires --arm s3"));
        r.s3_producer_shutdown_timeout_ms = None;
        r.tier = 3;
        assert!(validate_s3_args(&r)
            .unwrap_err()
            .to_string()
            .contains("header tier 2"));
        r.tier = 2;
        r.tracks = TrackSel::Pc;
        assert!(validate_s3_args(&r)
            .unwrap_err()
            .to_string()
            .contains("--tracks both"));
    }

    struct DropProbe(Arc<AtomicUsize>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn static_track_state_is_not_dropped_before_the_registered_drain_edge() {
        let dropped = Arc::new(AtomicUsize::new(0));
        let pc = DropProbe(Arc::clone(&dropped));
        let haptic = DropProbe(Arc::clone(&dropped));
        let (signal_tx, signal_rx) = tokio::sync::watch::channel(false);

        let drain = tokio::spawn(hold_static_track_state_through_drain(
            Duration::from_secs(60),
            signal_rx,
            pc,
            haptic,
            true,
        ));
        tokio::task::yield_now().await;
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            0,
            "track writers must remain alive throughout the registered drain",
        );

        signal_tx.send(true).unwrap();
        assert!(drain.await.unwrap(), "the signal must cut the drain short");
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            2,
            "track writers are released only after the drain wait returns",
        );
    }

    #[tokio::test]
    async fn legacy_static_track_state_keeps_the_existing_early_fin_behavior() {
        let dropped = Arc::new(AtomicUsize::new(0));
        let pc = DropProbe(Arc::clone(&dropped));
        let haptic = DropProbe(Arc::clone(&dropped));
        let (signal_tx, signal_rx) = tokio::sync::watch::channel(false);

        let drain = tokio::spawn(hold_static_track_state_through_drain(
            Duration::from_secs(60),
            signal_rx,
            pc,
            haptic,
            false,
        ));
        tokio::task::yield_now().await;
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            2,
            "legacy runs must still publish FIN before their session-only drain",
        );

        signal_tx.send(true).unwrap();
        assert!(drain.await.unwrap(), "the signal must cut the drain short");
    }

    #[tokio::test]
    async fn registered_direct_fin_handoff_observes_peer_close() {
        let (_signal_tx, signal_rx) = tokio::sync::watch::channel(false);
        let outcome =
            wait_registered_direct_fin_handoff(Duration::from_secs(1), signal_rx, async { 7u8 })
                .await;
        assert!(matches!(outcome, DirectFinHandoff::PeerClosed(7)));
    }

    #[tokio::test]
    async fn registered_direct_fin_handoff_remains_bounded_and_signal_aware() {
        let (signal_tx, signal_rx) = tokio::sync::watch::channel(false);
        signal_tx.send(true).unwrap();
        let signalled = wait_registered_direct_fin_handoff(
            Duration::from_secs(1),
            signal_rx,
            std::future::pending::<u8>(),
        )
        .await;
        assert!(matches!(signalled, DirectFinHandoff::Signal));

        let (_signal_tx, signal_rx) = tokio::sync::watch::channel(false);
        let timed_out = wait_registered_direct_fin_handoff(
            Duration::from_millis(1),
            signal_rx,
            std::future::pending::<u8>(),
        )
        .await;
        assert!(matches!(timed_out, DirectFinHandoff::TimedOut));
    }

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
        assert_eq!(
            registered_s_bytes(9_007_199_254_740_993, 1),
            9_007_199_254_740_993
        );
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


    #[test]
    fn transport_end_verdict_follows_the_completion_state() {
        assert_eq!(
            session_end_after_completion(CompletionState::Completed),
            TransportEndVerdict::Normal
        );
        assert_eq!(
            session_end_after_completion(CompletionState::Pending),
            TransportEndVerdict::AwaitForwarders
        );
        assert_eq!(
            session_end_after_completion(CompletionState::Incomplete),
            TransportEndVerdict::Error
        );
    }

    #[test]
    fn shutdown_row_transport_end_text_is_json_safe() {
        assert_eq!(
            json_text("session_after_completion:Ok(Err(Decode(More(1))))"),
            "session_after_completion:Ok(Err(Decode(More(1))))"
        );
        assert_eq!(json_text("a\"b\\c"), "a\\\"b\\\\c");
    }
}
