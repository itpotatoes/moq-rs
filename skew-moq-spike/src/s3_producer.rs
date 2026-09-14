//! Subscription-scoped producer leases for Phase-4 S3.
//!
//! A route may produce objects only while its remote subscription lease is
//! active. Cancelling a lease flips the shared active bit before notifying the
//! producer, so a producer that consults [`ProducerLease::record_object`] for
//! every object cannot account or emit work after unsubscribe. The later wire
//! adapter owns the corresponding `Subscribed::serve` future and must drop the
//! producer future when that subscription closes.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Context};
use moq_transport::serve::{ServeError, Track, TrackWriter};
use moq_transport::session::{SessionError, Subscribed};
use tokio::sync::watch;

use crate::s3_switch::{
    Route, TrackRole, HAPTIC_ESSENTIAL_TRACK, HAPTIC_FULL_TRACK, PC_HAPTIC_CRITICAL_TRACK,
    PC_NORMAL_TRACK, PC_RECOVERY_TRACK,
};
use crate::timestamp_us;

/// During make-before-break, each role may have one current and one target
/// producer. A fifth active producer is therefore always a lifecycle defect.
pub const MAX_ACTIVE_PRODUCERS: usize = 4;
const MICROS_PER_SECOND: u128 = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotError {
    ZeroRate,
    InvalidRateRatio,
    ArithmeticOverflow,
    SequenceOverflow,
}

/// Common sender-monotonic source clock shared by every S3 tier.
///
/// A newly subscribed producer starts at the first source slot whose nominal
/// PTS is not before `now_us`; it never creates a new local epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunSlotClock {
    pub anchor_us: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceIdentity {
    pub slot: u64,
    pub seq: u32,
    pub pts_us: u64,
    pub event_id: u32,
}

impl RunSlotClock {
    pub fn new(anchor_us: u64) -> Self {
        Self { anchor_us }
    }

    pub fn next_slot(self, now_us: u64, rate_hz: u64) -> Result<u64, SlotError> {
        if rate_hz == 0 {
            return Err(SlotError::ZeroRate);
        }
        let elapsed = now_us.saturating_sub(self.anchor_us) as u128;
        let scaled = elapsed
            .checked_mul(rate_hz as u128)
            .ok_or(SlotError::ArithmeticOverflow)?;
        let slot = scaled
            .checked_add(MICROS_PER_SECOND - 1)
            .ok_or(SlotError::ArithmeticOverflow)?
            / MICROS_PER_SECOND;
        u64::try_from(slot).map_err(|_| SlotError::ArithmeticOverflow)
    }

    pub fn pc_identity(self, now_us: u64, pc_rate_hz: u64) -> Result<SourceIdentity, SlotError> {
        let slot = self.next_slot(now_us, pc_rate_hz)?;
        source_identity(slot, slot, timestamp_us(slot, pc_rate_hz))
    }

    /// Essential haptic carries only the exact 90-Hz tick anchored to the next
    /// 30-Hz PC frame. Its sequence remains the global haptic source tick
    /// rather than a new local sequence.
    pub fn essential_haptic_identity(
        self,
        now_us: u64,
        pc_rate_hz: u64,
        haptic_rate_hz: u64,
    ) -> Result<SourceIdentity, SlotError> {
        if pc_rate_hz == 0 || haptic_rate_hz == 0 {
            return Err(SlotError::ZeroRate);
        }
        if haptic_rate_hz % pc_rate_hz != 0 || haptic_rate_hz / pc_rate_hz != 3 {
            return Err(SlotError::InvalidRateRatio);
        }
        let pc_slot = self.next_slot(now_us, pc_rate_hz)?;
        let haptic_tick = pc_slot
            .checked_mul(haptic_rate_hz / pc_rate_hz)
            .ok_or(SlotError::ArithmeticOverflow)?;
        source_identity(haptic_tick, pc_slot, timestamp_us(pc_slot, pc_rate_hz))
    }

    pub fn full_haptic_slot(self, now_us: u64, haptic_rate_hz: u64) -> Result<u64, SlotError> {
        self.next_slot(now_us, haptic_rate_hz)
    }
}

fn source_identity(
    sequence_slot: u64,
    event_slot: u64,
    pts_us: u64,
) -> Result<SourceIdentity, SlotError> {
    Ok(SourceIdentity {
        slot: sequence_slot,
        seq: u32::try_from(sequence_slot).map_err(|_| SlotError::SequenceOverflow)?,
        pts_us,
        event_id: u32::try_from(
            event_slot
                .checked_add(1)
                .ok_or(SlotError::SequenceOverflow)?,
        )
        .map_err(|_| SlotError::SequenceOverflow)?,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerError {
    InvalidTrackForRole,
    DuplicateOrReusedGeneration,
    TooManyActiveProducers,
    UnknownOrInactiveRoute,
    LeaseCancelled,
    StatePoisoned,
    /// Both current routes already completed the run; no new producer may
    /// move the latest generation after that snapshot.
    RunCompleted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerStats {
    pub role: TrackRole,
    pub route: Route,
    pub active: bool,
    pub starts: u64,
    pub stops: u64,
    pub objects: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionTaskEnd {
    RemoteClosed,
    ProducerFinished,
}

/// Terminal outcome of one producer loop, recorded by
/// `serve_subscription_producer` from the same branch that decides the logged
/// stop reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerTerminal {
    /// The producer loop returned normally (it reached the run end). Recorded
    /// synchronously before the forwarder is polled again, i.e. before the
    /// track close / PUBLISH_DONE for this route can reach the peer.
    Finished,
    /// The remote unsubscribed (or the forwarder ended) first.
    RemoteClosed,
    /// The producer returned an error, or its task was abandoned; the reason
    /// is a short stable tag (`producer`, `aborted`, ...).
    Error(&'static str),
}

/// Outcome of the forwarder of a `Finished` producer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForwardOutcome {
    /// The forwarder closed within the shutdown timeout, ended cleanly
    /// (`Ok`/`Done`, never a remote cancel), and the produced count matched
    /// the recorded count.
    Closed,
    /// `shutdown_timeout`, `remote_cancel`, `serve_error`, `count_mismatch`,
    /// or `aborted`.
    Failed(&'static str),
}

/// A fault recorded for one producer task of this run. Any fault, including
/// one on a retired generation, makes the run verdict an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordedFault {
    pub role: TrackRole,
    pub generation: u64,
    pub reason: &'static str,
}

/// Registry state the run verdict is computed from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistrySnapshot {
    pub pc: Option<RoleTerminal>,
    pub haptic: Option<RoleTerminal>,
    pub faults: Vec<RecordedFault>,
    /// Reserved or activated producers without a final recorded outcome.
    pub outstanding: usize,
}

/// A session or namespace task end observed by the send loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportEnd {
    pub kind: &'static str,
    pub text: String,
    pub at_us: u64,
    pub before_production_end: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunVerdict {
    Normal,
    Error(Vec<String>),
}

impl RunVerdict {
    pub fn as_str(&self) -> &'static str {
        match self {
            RunVerdict::Normal => "normal",
            RunVerdict::Error(_) => "error",
        }
    }

    pub fn reasons(&self) -> &[String] {
        match self {
            RunVerdict::Normal => &[],
            RunVerdict::Error(reasons) => reasons,
        }
    }
}

fn role_verdict_reason(role: TrackRole, terminal: Option<RoleTerminal>) -> Option<String> {
    let name = role.as_str();
    match terminal {
        None => Some(format!("{name}: latest-generation producer never terminated")),
        Some(RoleTerminal {
            terminal: ProducerTerminal::RemoteClosed,
            ..
        }) => Some(format!("{name}: latest-generation producer was remote-closed")),
        Some(RoleTerminal {
            terminal: ProducerTerminal::Error(reason),
            ..
        }) => Some(format!("{name}: latest-generation producer error ({reason})")),
        Some(RoleTerminal {
            terminal: ProducerTerminal::Finished,
            forward: None,
        }) => Some(format!("{name}: forwarder outcome never recorded")),
        Some(RoleTerminal {
            terminal: ProducerTerminal::Finished,
            forward: Some(ForwardOutcome::Failed(reason)),
        }) => Some(format!("{name}: forwarder failed ({reason})")),
        Some(RoleTerminal {
            terminal: ProducerTerminal::Finished,
            forward: Some(ForwardOutcome::Closed),
        }) => None,
    }
}

/// The single run verdict, computed once after the drain from the registry
/// snapshot and the observed transport ends.
///
/// Normal ONLY if: both latest-generation producers are `Finished` with
/// forwarder `Closed`; no fault of any kind was recorded (including on
/// retired generations; a retired generation's `RemoteClosed` from a normal
/// switch cancel is not a fault); no producer task is outstanding; and no
/// session/namespace end was observed before production ended.
pub fn s3_run_verdict(snapshot: &RegistrySnapshot, transport_ends: &[TransportEnd]) -> RunVerdict {
    let mut reasons = Vec::new();
    reasons.extend(role_verdict_reason(TrackRole::Pc, snapshot.pc));
    reasons.extend(role_verdict_reason(TrackRole::Haptic, snapshot.haptic));
    for fault in &snapshot.faults {
        reasons.push(format!(
            "fault {} generation {}: {}",
            fault.role.as_str(),
            fault.generation,
            fault.reason
        ));
    }
    if snapshot.outstanding > 0 {
        reasons.push(format!(
            "{} producer task(s) without a recorded outcome",
            snapshot.outstanding
        ));
    }
    for end in transport_ends {
        if end.before_production_end {
            reasons.push(format!(
                "{} ended before production end at {} us: {}",
                end.kind, end.at_us, end.text
            ));
        }
    }
    if reasons.is_empty() {
        RunVerdict::Normal
    } else {
        RunVerdict::Error(reasons)
    }
}

/// Held by every producer task for its whole lifetime. If the task ends (or
/// is aborted) without a final recorded outcome, the drop records
/// `Error("aborted")` / `Failed("aborted")` / a released reservation with an
/// `aborted` fault, so the registry never has a silent gap.
pub struct ProducerTaskGuard {
    registry: Arc<Mutex<SubscriptionProducerRegistry>>,
    role: TrackRole,
    route: Route,
}

impl ProducerTaskGuard {
    pub fn new(registry: Arc<Mutex<SubscriptionProducerRegistry>>, role: TrackRole, route: Route) -> Self {
        Self {
            registry,
            role,
            route,
        }
    }
}

impl Drop for ProducerTaskGuard {
    fn drop(&mut self) {
        if let Ok(mut registry) = self.registry.lock() {
            registry.record_abandoned(self.role, self.route);
        }
    }
}

/// Terminal record of one role's latest-generation producer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoleTerminal {
    pub terminal: ProducerTerminal,
    /// Only ever `Some` when `terminal == Finished`.
    pub forward: Option<ForwardOutcome>,
}

/// Whether the completion question for the current routes is settled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionState {
    /// Both latest-generation producers reached the run end AND their
    /// forwarders closed cleanly with the count check passed.
    Completed,
    /// Every latest-generation producer reached the run end, but at least one
    /// forwarder outcome is not recorded yet. A bounded wait resolves it.
    Pending,
    /// A role is still running, was remote-closed, errored, or its forwarder
    /// failed: the run is not complete.
    Incomplete,
}

/// One role is complete only when its latest-generation producer finished
/// normally and its forwarder closed cleanly.
pub fn role_completed(role: Option<RoleTerminal>) -> bool {
    matches!(
        role,
        Some(RoleTerminal {
            terminal: ProducerTerminal::Finished,
            forward: Some(ForwardOutcome::Closed),
        })
    )
}

/// True only when BOTH roles are complete (see [`role_completed`]). Retired
/// generations do not count; a remote unsubscribe, an error, or a forwarder
/// failure on the latest generation is never "completed".
pub fn current_routes_completed(pc: Option<RoleTerminal>, haptic: Option<RoleTerminal>) -> bool {
    role_completed(pc) && role_completed(haptic)
}

pub fn completion_state(pc: Option<RoleTerminal>, haptic: Option<RoleTerminal>) -> CompletionState {
    if current_routes_completed(pc, haptic) {
        return CompletionState::Completed;
    }
    let awaiting_forwarder = |role: Option<RoleTerminal>| {
        role_completed(role)
            || matches!(
                role,
                Some(RoleTerminal {
                    terminal: ProducerTerminal::Finished,
                    forward: None,
                })
            )
    };
    if awaiting_forwarder(pc) && awaiting_forwarder(haptic) {
        CompletionState::Pending
    } else {
        CompletionState::Incomplete
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubscriptionTaskResult {
    pub end: SubscriptionTaskEnd,
    pub role: TrackRole,
    pub route: Route,
    pub objects: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ProducerKey {
    role: TrackRole,
    generation: u64,
}

#[derive(Default)]
struct RegistryState {
    stats: HashMap<ProducerKey, ProducerStats>,
}

pub struct ProducerLease {
    key: ProducerKey,
    route: Route,
    state: Arc<Mutex<RegistryState>>,
    cancelled: watch::Receiver<bool>,
}

impl ProducerLease {
    pub fn role(&self) -> TrackRole {
        self.key.role
    }

    pub fn route(&self) -> Route {
        self.route
    }

    pub fn is_cancelled(&self) -> bool {
        *self.cancelled.borrow()
    }

    pub async fn cancelled(&mut self) {
        if self.is_cancelled() {
            return;
        }
        while self.cancelled.changed().await.is_ok() {
            if self.is_cancelled() {
                return;
            }
        }
    }

    /// Authorize and account one object immediately before its synchronous
    /// track write. A cancelled lease fails closed.
    pub fn record_object(&self) -> Result<u64, ProducerError> {
        self.authorize_object(true)
    }

    /// Authorize a pre-t0 object without adding it to measurement producer
    /// counts. It still uses the active subscription lease and therefore
    /// cannot write after cancellation.
    pub fn authorize_warmup_object(&self) -> Result<u64, ProducerError> {
        self.authorize_object(false)
    }

    fn authorize_object(&self, measurement: bool) -> Result<u64, ProducerError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ProducerError::StatePoisoned)?;
        let stats = state
            .stats
            .get_mut(&self.key)
            .ok_or(ProducerError::UnknownOrInactiveRoute)?;
        if !stats.active || self.is_cancelled() {
            return Err(ProducerError::LeaseCancelled);
        }
        if measurement {
            stats.objects = stats
                .objects
                .checked_add(1)
                .ok_or(ProducerError::StatePoisoned)?;
        }
        Ok(stats.objects)
    }
}

pub struct SubscriptionProducerRegistry {
    state: Arc<Mutex<RegistryState>>,
    active: HashMap<ProducerKey, watch::Sender<bool>>,
    seen: HashSet<ProducerKey>,
    /// Reserved (allocated under the completion check) but not yet activated.
    reserved: HashSet<ProducerKey>,
    /// Reservations that could never be activated; excluded from "latest".
    released: HashSet<ProducerKey>,
    terminal: HashMap<ProducerKey, ProducerTerminal>,
    forward: HashMap<ProducerKey, ForwardOutcome>,
    faults: Vec<RecordedFault>,
    /// Bumped on every terminal transition so a waiter can re-check
    /// `current_routes_finished` without polling.
    terminal_tx: watch::Sender<u64>,
}

impl SubscriptionProducerRegistry {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(RegistryState::default())),
            active: HashMap::new(),
            seen: HashSet::new(),
            reserved: HashSet::new(),
            released: HashSet::new(),
            terminal: HashMap::new(),
            forward: HashMap::new(),
            faults: Vec::new(),
            terminal_tx: watch::channel(0).0,
        }
    }

    /// Record how the producer of `route` terminated. The lease must already
    /// be inactive (cancelled) and each generation records exactly once.
    pub fn record_terminal(
        &mut self,
        role: TrackRole,
        route: Route,
        terminal: ProducerTerminal,
    ) -> Result<(), ProducerError> {
        let key = ProducerKey {
            role,
            generation: route.generation,
        };
        if !self.seen.contains(&key) || self.active.contains_key(&key) {
            return Err(ProducerError::UnknownOrInactiveRoute);
        }
        if self.terminal.contains_key(&key) {
            return Err(ProducerError::DuplicateOrReusedGeneration);
        }
        self.terminal.insert(key, terminal);
        if let ProducerTerminal::Error(reason) = terminal {
            self.faults.push(RecordedFault {
                role,
                generation: route.generation,
                reason,
            });
        }
        self.terminal_tx.send_modify(|version| *version += 1);
        Ok(())
    }

    /// Reserve a generation under the completion check. Callers allocate the
    /// generation and reserve it in ONE registry critical section so a
    /// subscription can never consume a generation after completion. A
    /// reservation counts toward "latest" until activated or released.
    pub fn reserve(&mut self, role: TrackRole, route: Route) -> Result<(), ProducerError> {
        if !valid_track(role, route.name) {
            return Err(ProducerError::InvalidTrackForRole);
        }
        if self.current_routes_completed() {
            return Err(ProducerError::RunCompleted);
        }
        let key = ProducerKey {
            role,
            generation: route.generation,
        };
        if !self.seen.insert(key) {
            return Err(ProducerError::DuplicateOrReusedGeneration);
        }
        self.reserved.insert(key);
        self.terminal_tx.send_modify(|version| *version += 1);
        Ok(())
    }

    /// A reserved generation that can never be activated. It no longer counts
    /// toward "latest" or outstanding work; `fault` records why, if the cause
    /// is a defect (a post-completion refusal passes `None`).
    pub fn release_reservation(
        &mut self,
        role: TrackRole,
        route: Route,
        fault: Option<&'static str>,
    ) -> Result<(), ProducerError> {
        let key = ProducerKey {
            role,
            generation: route.generation,
        };
        if !self.reserved.remove(&key) {
            return Err(ProducerError::UnknownOrInactiveRoute);
        }
        self.released.insert(key);
        if let Some(reason) = fault {
            self.faults.push(RecordedFault {
                role,
                generation: route.generation,
                reason,
            });
        }
        self.terminal_tx.send_modify(|version| *version += 1);
        Ok(())
    }

    /// Record a fault that is not a terminal (e.g. the producer-stop log
    /// write failed after the outcome was recorded).
    pub fn record_fault(
        &mut self,
        role: TrackRole,
        route: Route,
        reason: &'static str,
    ) -> Result<(), ProducerError> {
        let key = ProducerKey {
            role,
            generation: route.generation,
        };
        if !self.seen.contains(&key) {
            return Err(ProducerError::UnknownOrInactiveRoute);
        }
        self.faults.push(RecordedFault {
            role,
            generation: route.generation,
            reason,
        });
        self.terminal_tx.send_modify(|version| *version += 1);
        Ok(())
    }

    /// Drop-guard entry: close every gap a task can leave when it ends
    /// without recording a final outcome. No-op when the outcome is complete.
    pub fn record_abandoned(&mut self, role: TrackRole, route: Route) {
        let key = ProducerKey {
            role,
            generation: route.generation,
        };
        if self.released.contains(&key) || !self.seen.contains(&key) {
            return;
        }
        if self.reserved.contains(&key) {
            let _ = self.release_reservation(role, route, Some("aborted"));
            return;
        }
        if self.active.contains_key(&key) {
            let _ = self.cancel(role, route);
        }
        match self.terminal.get(&key).copied() {
            None => {
                let _ = self.record_terminal(role, route, ProducerTerminal::Error("aborted"));
            }
            Some(ProducerTerminal::Finished) if !self.forward.contains_key(&key) => {
                let _ = self.mark_forward(role, route, ForwardOutcome::Failed("aborted"));
            }
            _ => {}
        }
    }

    /// Both latest-generation producers have finished producing (their loops
    /// reached the run end), whatever their forwarders did afterwards.
    pub fn production_ended(&self) -> bool {
        [TrackRole::Pc, TrackRole::Haptic].into_iter().all(|role| {
            matches!(
                self.latest_role_terminal(role),
                Some(RoleTerminal {
                    terminal: ProducerTerminal::Finished,
                    ..
                })
            )
        })
    }

    /// Reserved or activated producers without a final recorded outcome.
    pub fn outstanding(&self) -> usize {
        self.seen
            .iter()
            .filter(|key| !self.released.contains(key))
            .filter(|key| match self.terminal.get(key) {
                None => true,
                Some(ProducerTerminal::Finished) => !self.forward.contains_key(key),
                Some(_) => false,
            })
            .count()
    }

    pub fn faults(&self) -> &[RecordedFault] {
        &self.faults
    }

    pub fn snapshot(&self) -> RegistrySnapshot {
        RegistrySnapshot {
            pc: self.latest_role_terminal(TrackRole::Pc),
            haptic: self.latest_role_terminal(TrackRole::Haptic),
            faults: self.faults.clone(),
            outstanding: self.outstanding(),
        }
    }

    fn latest_generation(&self, role: TrackRole) -> Option<u64> {
        self.seen
            .iter()
            .filter(|key| key.role == role && !self.released.contains(key))
            .map(|key| key.generation)
            .max()
    }

    /// Record that the forwarder of a `Finished` producer closed cleanly and
    /// its produced count matched. This is the second half of completion.
    pub fn mark_forward_closed(&mut self, role: TrackRole, route: Route) -> Result<(), ProducerError> {
        self.mark_forward(role, route, ForwardOutcome::Closed)
    }

    /// Record that the forwarder of a `Finished` producer timed out, ended with
    /// an error, or failed the produced-count check. The role is then never
    /// complete.
    pub fn mark_forward_failed(
        &mut self,
        role: TrackRole,
        route: Route,
        reason: &'static str,
    ) -> Result<(), ProducerError> {
        self.mark_forward(role, route, ForwardOutcome::Failed(reason))
    }

    fn mark_forward(
        &mut self,
        role: TrackRole,
        route: Route,
        outcome: ForwardOutcome,
    ) -> Result<(), ProducerError> {
        let key = ProducerKey {
            role,
            generation: route.generation,
        };
        if self.terminal.get(&key) != Some(&ProducerTerminal::Finished) {
            return Err(ProducerError::UnknownOrInactiveRoute);
        }
        if self.forward.contains_key(&key) {
            return Err(ProducerError::DuplicateOrReusedGeneration);
        }
        self.forward.insert(key, outcome);
        if let ForwardOutcome::Failed(reason) = outcome {
            self.faults.push(RecordedFault {
                role,
                generation: route.generation,
                reason,
            });
        }
        self.terminal_tx.send_modify(|version| *version += 1);
        Ok(())
    }

    pub fn forward_closed(&self, role: TrackRole, route: Route) -> bool {
        self.forward.get(&ProducerKey {
            role,
            generation: route.generation,
        }) == Some(&ForwardOutcome::Closed)
    }

    /// Terminal outcome of the latest activated generation for `role`, or
    /// `None` when no producer was activated or it is still running.
    pub fn latest_terminal(&self, role: TrackRole) -> Option<ProducerTerminal> {
        self.latest_role_terminal(role).map(|role| role.terminal)
    }

    pub fn latest_role_terminal(&self, role: TrackRole) -> Option<RoleTerminal> {
        let generation = self.latest_generation(role)?;
        let key = ProducerKey { role, generation };
        let terminal = *self.terminal.get(&key)?;
        Some(RoleTerminal {
            terminal,
            forward: self.forward.get(&key).copied(),
        })
    }

    pub fn current_routes_completed(&self) -> bool {
        current_routes_completed(
            self.latest_role_terminal(TrackRole::Pc),
            self.latest_role_terminal(TrackRole::Haptic),
        )
    }

    pub fn completion_state(&self) -> CompletionState {
        completion_state(
            self.latest_role_terminal(TrackRole::Pc),
            self.latest_role_terminal(TrackRole::Haptic),
        )
    }

    /// Receiver that changes on every terminal transition; pair it with
    /// [`Self::current_routes_finished`].
    pub fn terminal_watch(&self) -> watch::Receiver<u64> {
        self.terminal_tx.subscribe()
    }

    pub fn activate(
        &mut self,
        role: TrackRole,
        route: Route,
    ) -> Result<ProducerLease, ProducerError> {
        if !valid_track(role, route.name) {
            return Err(ProducerError::InvalidTrackForRole);
        }
        if self.active.len() >= MAX_ACTIVE_PRODUCERS {
            return Err(ProducerError::TooManyActiveProducers);
        }
        let key = ProducerKey {
            role,
            generation: route.generation,
        };
        if self.reserved.remove(&key) {
            // Reserved under the completion check in the same critical
            // section as its allocation; it already counts toward "latest".
        } else {
            // Switch/end boundary for an unreserved activation: once both
            // current routes completed the run, no later subscription may
            // move `latest` past that snapshot.
            if self.current_routes_completed() {
                return Err(ProducerError::RunCompleted);
            }
            if !self.seen.insert(key) {
                return Err(ProducerError::DuplicateOrReusedGeneration);
            }
        }

        let (cancel, cancelled) = watch::channel(false);
        let stats = ProducerStats {
            role,
            route,
            active: true,
            starts: 1,
            stops: 0,
            objects: 0,
        };
        self.state
            .lock()
            .map_err(|_| ProducerError::StatePoisoned)?
            .stats
            .insert(key, stats);
        self.active.insert(key, cancel);

        Ok(ProducerLease {
            key,
            route,
            state: self.state.clone(),
            cancelled,
        })
    }

    /// Mark inactive before notifying the producer. The caller must then await
    /// or abort-and-join the producer task before final accounting.
    pub fn cancel(&mut self, role: TrackRole, route: Route) -> Result<(), ProducerError> {
        let key = ProducerKey {
            role,
            generation: route.generation,
        };
        let cancel = self
            .active
            .remove(&key)
            .ok_or(ProducerError::UnknownOrInactiveRoute)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| ProducerError::StatePoisoned)?;
        let stats = state
            .stats
            .get_mut(&key)
            .ok_or(ProducerError::UnknownOrInactiveRoute)?;
        if stats.route != route || !stats.active {
            return Err(ProducerError::UnknownOrInactiveRoute);
        }
        stats.active = false;
        stats.stops += 1;
        cancel.send_replace(true);
        Ok(())
    }

    pub fn active_count(&self) -> usize {
        self.active.len()
    }

    pub fn stats(&self) -> Result<Vec<ProducerStats>, ProducerError> {
        let state = self
            .state
            .lock()
            .map_err(|_| ProducerError::StatePoisoned)?;
        let mut stats: Vec<_> = state.stats.values().copied().collect();
        stats.sort_by_key(|stats| (stats.role as u8, stats.route.generation));
        Ok(stats)
    }

    pub fn objects_for_track(&self, name: &str) -> Result<u64, ProducerError> {
        Ok(self
            .stats()?
            .iter()
            .filter(|stats| stats.route.name == name)
            .map(|stats| stats.objects)
            .sum())
    }
}

impl Default for SubscriptionProducerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for SubscriptionProducerRegistry {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.lock() {
            for (key, cancel) in self.active.drain() {
                if let Some(stats) = state.stats.get_mut(&key) {
                    stats.active = false;
                    stats.stops = stats.stops.saturating_add(1);
                }
                cancel.send_replace(true);
            }
        }
    }
}

/// Accept one inbound subscription, then run exactly one producer for its
/// lifetime.
///
/// The producer does not start until SUBSCRIBE_OK has been sent. If the remote
/// subscription closes first, the registry marks the lease inactive before
/// this function drops the producer future and its TrackWriter. If the producer
/// reaches the run end first, its writer closes the track and the forwarding
/// future must finish within `shutdown_timeout`.
///
/// Every exit path records its outcome in the registry (terminal, forwarder
/// outcome, released reservation with or without a fault). `Ok(None)` is the
/// normal outcome for a subscription refused because the run had already
/// completed.
pub async fn serve_subscription_producer<F, Fut>(
    mut subscribed: Subscribed,
    role: TrackRole,
    route: Route,
    registry: Arc<Mutex<SubscriptionProducerRegistry>>,
    shutdown_timeout: Duration,
    producer: F,
) -> anyhow::Result<Option<SubscriptionTaskResult>>
where
    F: FnOnce(TrackWriter, ProducerLease) -> Fut,
    Fut: Future<Output = anyhow::Result<u64>>,
{
    if shutdown_timeout.is_zero() {
        return Err(anyhow!(
            "subscription producer shutdown timeout must be > 0"
        ));
    }
    if subscribed.info.track_name.as_bytes() != route.name.as_bytes() {
        return Err(anyhow!(
            "subscription track '{}' does not match route '{}'",
            subscribed.info.track_name,
            route.name
        ));
    }

    let (writer, reader) = Track::new(
        subscribed.info.track_namespace.clone(),
        subscribed.info.track_name.clone(),
    )
    .produce();

    // The producer must not generate retained history before the subscription
    // has a valid alias on the peer.
    if let Err(error) = subscribed.accept(&reader).await {
        release_reservation_shared(&registry, role, route, Some("subscription_setup"))?;
        return Err(error).context("accept subscription before producer start");
    }

    let activation = registry
        .lock()
        .map_err(|_| anyhow!("producer registry poisoned"))?
        .activate(role, route);
    let lease = match activation {
        Ok(lease) => lease,
        Err(ProducerError::RunCompleted) => {
            // The run already completed on both current routes: refuse the
            // late subscription as a NORMAL outcome, without a producer, a
            // log row, or a fault; the reservation no longer counts.
            let _ = subscribed.close(ServeError::not_found_ctx(format!(
                "S3 subscription '{}' generation {} arrived after run completion",
                route.name, route.generation
            )));
            release_reservation_shared(&registry, role, route, None)?;
            return Ok(None);
        }
        Err(error) => {
            release_reservation_shared(&registry, role, route, Some("activation_failed"))?;
            return Err(anyhow!("activate producer lease: {error:?}"));
        }
    };

    let serve = subscribed.serve_accepted(reader);
    let produce = producer(writer, lease);
    tokio::pin!(serve);
    tokio::pin!(produce);

    tokio::select! {
        biased;
        serve_result = &mut serve => {
            cancel_shared(&registry, role, route)?;
            record_terminal_shared(&registry, role, route, ProducerTerminal::RemoteClosed)?;
            if let Err(error) = normalize_serve_end(serve_result) {
                record_fault_shared(&registry, role, route, "serve_error")?;
                return Err(error);
            }
            Ok(Some(SubscriptionTaskResult {
                end: SubscriptionTaskEnd::RemoteClosed,
                role,
                route,
                objects: route_objects(&registry, role, route)?,
            }))
        }
        producer_result = &mut produce => {
            let produced = match producer_result {
                Ok(produced) => produced,
                Err(error) => {
                    cancel_shared(&registry, role, route)?;
                    record_terminal_shared(&registry, role, route, ProducerTerminal::Error("producer"))?;
                    return Err(error).context("subscription producer");
                }
            };
            cancel_shared(&registry, role, route)?;
            // Ordering: `serve` is pinned in this task and was polled (Pending)
            // before `produce` in this same `biased` poll; it is not polled
            // again until the timeout below. The producer's dropped writers
            // therefore cannot be observed, and no track close / PUBLISH_DONE
            // can be forwarded, before this record lands.
            record_terminal_shared(&registry, role, route, ProducerTerminal::Finished)?;
            // Completion needs the forwarder to close cleanly as well. Every
            // failure below records `ForwardOutcome::Failed(reason)` (a fault)
            // so the role can never be reported complete. A remote cancel is
            // never "closed": a produced-count match is not evidence that
            // forwarding completed.
            let forwarded: Result<u64, (&'static str, anyhow::Error)> = async {
                let serve_result = match tokio::time::timeout(shutdown_timeout, &mut serve).await {
                    Ok(result) => result,
                    Err(_) => {
                        return Err((
                            "shutdown_timeout",
                            anyhow!("forwarder did not close after producer end"),
                        ))
                    }
                };
                match serve_result {
                    Ok(()) | Err(SessionError::Serve(ServeError::Done)) => {}
                    Err(SessionError::Serve(ServeError::Cancel)) => {
                        return Err((
                            "remote_cancel",
                            anyhow!("forwarder was cancelled by the remote after producer end"),
                        ))
                    }
                    Err(error) => {
                        return Err(("serve_error", anyhow::Error::from(error).context("serve subscription")))
                    }
                }
                let recorded = match route_objects(&registry, role, route) {
                    Ok(recorded) => recorded,
                    Err(error) => return Err(("count_mismatch", error)),
                };
                if produced != recorded {
                    return Err((
                        "count_mismatch",
                        anyhow!(
                            "producer count mismatch for {} generation {}: returned {}, recorded {}",
                            route.name,
                            route.generation,
                            produced,
                            recorded
                        ),
                    ));
                }
                Ok(recorded)
            }
            .await;
            let recorded = match forwarded {
                Ok(recorded) => recorded,
                Err((reason, error)) => {
                    mark_forward_shared(&registry, role, route, ForwardOutcome::Failed(reason))?;
                    return Err(error);
                }
            };
            mark_forward_shared(&registry, role, route, ForwardOutcome::Closed)?;
            Ok(Some(SubscriptionTaskResult {
                end: SubscriptionTaskEnd::ProducerFinished,
                role,
                route,
                objects: recorded,
            }))
        }
    }
}

fn cancel_shared(
    registry: &Arc<Mutex<SubscriptionProducerRegistry>>,
    role: TrackRole,
    route: Route,
) -> anyhow::Result<()> {
    registry
        .lock()
        .map_err(|_| anyhow!("producer registry poisoned"))?
        .cancel(role, route)
        .map_err(|error| anyhow!("cancel producer lease: {error:?}"))
}

fn record_terminal_shared(
    registry: &Arc<Mutex<SubscriptionProducerRegistry>>,
    role: TrackRole,
    route: Route,
    terminal: ProducerTerminal,
) -> anyhow::Result<()> {
    registry
        .lock()
        .map_err(|_| anyhow!("producer registry poisoned"))?
        .record_terminal(role, route, terminal)
        .map_err(|error| anyhow!("record producer terminal: {error:?}"))
}

fn mark_forward_shared(
    registry: &Arc<Mutex<SubscriptionProducerRegistry>>,
    role: TrackRole,
    route: Route,
    outcome: ForwardOutcome,
) -> anyhow::Result<()> {
    let mut registry = registry
        .lock()
        .map_err(|_| anyhow!("producer registry poisoned"))?;
    match outcome {
        ForwardOutcome::Closed => registry.mark_forward_closed(role, route),
        ForwardOutcome::Failed(reason) => registry.mark_forward_failed(role, route, reason),
    }
    .map_err(|error| anyhow!("record producer forwarder outcome: {error:?}"))
}

fn record_fault_shared(
    registry: &Arc<Mutex<SubscriptionProducerRegistry>>,
    role: TrackRole,
    route: Route,
    reason: &'static str,
) -> anyhow::Result<()> {
    registry
        .lock()
        .map_err(|_| anyhow!("producer registry poisoned"))?
        .record_fault(role, route, reason)
        .map_err(|error| anyhow!("record producer fault: {error:?}"))
}

/// Release a reservation if one exists; an unreserved (directly activated)
/// route is not an error here.
fn release_reservation_shared(
    registry: &Arc<Mutex<SubscriptionProducerRegistry>>,
    role: TrackRole,
    route: Route,
    fault: Option<&'static str>,
) -> anyhow::Result<()> {
    let mut registry = registry
        .lock()
        .map_err(|_| anyhow!("producer registry poisoned"))?;
    match registry.release_reservation(role, route, fault) {
        Ok(()) | Err(ProducerError::UnknownOrInactiveRoute) => Ok(()),
        Err(error) => Err(anyhow!("release producer reservation: {error:?}")),
    }
}

fn route_objects(
    registry: &Arc<Mutex<SubscriptionProducerRegistry>>,
    role: TrackRole,
    route: Route,
) -> anyhow::Result<u64> {
    let stats = registry
        .lock()
        .map_err(|_| anyhow!("producer registry poisoned"))?
        .stats()
        .map_err(|error| anyhow!("read producer stats: {error:?}"))?;
    stats
        .into_iter()
        .find(|stats| stats.role == role && stats.route == route)
        .map(|stats| stats.objects)
        .ok_or_else(|| {
            anyhow!(
                "missing producer stats for {} generation {}",
                route.name,
                route.generation
            )
        })
}

fn normalize_serve_end(result: Result<(), SessionError>) -> anyhow::Result<()> {
    match result {
        Ok(())
        | Err(SessionError::Serve(ServeError::Cancel))
        | Err(SessionError::Serve(ServeError::Done)) => Ok(()),
        Err(error) => Err(error).context("serve subscription"),
    }
}

fn valid_track(role: TrackRole, name: &str) -> bool {
    match role {
        TrackRole::Pc => matches!(
            name,
            PC_NORMAL_TRACK | PC_RECOVERY_TRACK | PC_HAPTIC_CRITICAL_TRACK
        ),
        TrackRole::Haptic => matches!(name, HAPTIC_FULL_TRACK | HAPTIC_ESSENTIAL_TRACK),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(name: &'static str, generation: u64) -> Route {
        Route { name, generation }
    }

    #[test]
    fn common_clock_uses_next_global_slot_without_reset() {
        let clock = RunSlotClock::new(1_000_000);
        assert_eq!(clock.next_slot(1_000_000, 30).unwrap(), 0);
        assert_eq!(clock.next_slot(1_000_001, 30).unwrap(), 1);
        assert_eq!(clock.next_slot(1_033_333, 30).unwrap(), 1);
        assert_eq!(clock.next_slot(1_033_334, 30).unwrap(), 2);

        let switched = clock.pc_identity(6_100_001, 30).unwrap();
        assert_eq!(switched.slot, 154);
        assert_eq!(switched.seq, 154);
        assert_eq!(switched.event_id, 155);
        assert_eq!(switched.pts_us, timestamp_us(154, 30));
        assert_ne!(switched.seq, 0);
    }

    #[test]
    fn essential_haptic_keeps_global_tick_and_exact_pc_identity() {
        let clock = RunSlotClock::new(1_000_000);
        let pc = clock.pc_identity(3_345_678, 30).unwrap();
        let haptic = clock.essential_haptic_identity(3_345_678, 30, 90).unwrap();
        assert_eq!(haptic.pts_us, pc.pts_us);
        assert_eq!(haptic.event_id, pc.event_id);
        assert_eq!(haptic.slot, pc.slot * 3);
        assert_eq!(haptic.seq as u64, haptic.slot);
        assert_eq!(clock.full_haptic_slot(3_345_678, 90).unwrap(), 212);
        assert_eq!(
            clock.essential_haptic_identity(3_345_678, 30, 100),
            Err(SlotError::InvalidRateRatio)
        );
    }

    #[test]
    fn slot_clock_fails_loud_on_invalid_or_unrepresentable_input() {
        let clock = RunSlotClock::new(0);
        assert_eq!(clock.next_slot(1, 0), Err(SlotError::ZeroRate));
        assert_eq!(
            clock.pc_identity(u64::MAX, u64::MAX),
            Err(SlotError::ArithmeticOverflow)
        );
    }

    #[test]
    fn inactive_tiers_stay_at_zero_objects() {
        let mut registry = SubscriptionProducerRegistry::new();
        let pc = registry
            .activate(TrackRole::Pc, route(PC_NORMAL_TRACK, 0))
            .unwrap();
        let haptic = registry
            .activate(TrackRole::Haptic, route(HAPTIC_FULL_TRACK, 0))
            .unwrap();

        for _ in 0..3 {
            pc.record_object().unwrap();
        }
        for _ in 0..10 {
            haptic.record_object().unwrap();
        }

        assert_eq!(registry.objects_for_track(PC_NORMAL_TRACK).unwrap(), 3);
        assert_eq!(registry.objects_for_track(HAPTIC_FULL_TRACK).unwrap(), 10);
        assert_eq!(registry.objects_for_track(PC_RECOVERY_TRACK).unwrap(), 0);
        assert_eq!(
            registry
                .objects_for_track(PC_HAPTIC_CRITICAL_TRACK)
                .unwrap(),
            0
        );
        assert_eq!(
            registry.objects_for_track(HAPTIC_ESSENTIAL_TRACK).unwrap(),
            0
        );
    }

    #[test]
    fn cancel_marks_inactive_before_notifying_and_blocks_more_objects() {
        let mut registry = SubscriptionProducerRegistry::new();
        let route = route(PC_NORMAL_TRACK, 0);
        let lease = registry.activate(TrackRole::Pc, route).unwrap();
        lease.record_object().unwrap();
        registry.cancel(TrackRole::Pc, route).unwrap();

        assert!(lease.is_cancelled());
        assert_eq!(lease.record_object(), Err(ProducerError::LeaseCancelled));
        let stats = registry.stats().unwrap();
        assert_eq!(stats[0].objects, 1);
        assert_eq!(stats[0].stops, 1);
        assert!(!stats[0].active);
    }

    #[test]
    fn make_before_break_allows_only_current_and_target_overlap() {
        let mut registry = SubscriptionProducerRegistry::new();
        let pc_old = route(PC_NORMAL_TRACK, 0);
        let hap_old = route(HAPTIC_FULL_TRACK, 0);
        let pc_new = route(PC_HAPTIC_CRITICAL_TRACK, 1);
        let hap_new = route(HAPTIC_ESSENTIAL_TRACK, 1);

        let old_pc = registry.activate(TrackRole::Pc, pc_old).unwrap();
        let old_hap = registry.activate(TrackRole::Haptic, hap_old).unwrap();
        let new_pc = registry.activate(TrackRole::Pc, pc_new).unwrap();
        let new_hap = registry.activate(TrackRole::Haptic, hap_new).unwrap();
        assert_eq!(registry.active_count(), 4);

        assert!(matches!(
            registry.activate(TrackRole::Pc, route(PC_RECOVERY_TRACK, 2)),
            Err(ProducerError::TooManyActiveProducers)
        ));
        old_pc.record_object().unwrap();
        old_hap.record_object().unwrap();
        new_pc.record_object().unwrap();
        new_hap.record_object().unwrap();

        registry.cancel(TrackRole::Pc, pc_old).unwrap();
        registry.cancel(TrackRole::Haptic, hap_old).unwrap();
        assert_eq!(registry.active_count(), 2);
        assert_eq!(old_pc.record_object(), Err(ProducerError::LeaseCancelled));
        assert_eq!(old_hap.record_object(), Err(ProducerError::LeaseCancelled));
        new_pc.record_object().unwrap();
        new_hap.record_object().unwrap();
    }

    #[test]
    fn generation_cannot_be_reused_after_cancel() {
        let mut registry = SubscriptionProducerRegistry::new();
        let route = route(PC_NORMAL_TRACK, 0);
        registry.activate(TrackRole::Pc, route).unwrap();
        registry.cancel(TrackRole::Pc, route).unwrap();
        assert!(matches!(
            registry.activate(TrackRole::Pc, route),
            Err(ProducerError::DuplicateOrReusedGeneration)
        ));
    }

    #[test]
    fn track_name_must_match_role() {
        let mut registry = SubscriptionProducerRegistry::new();
        assert!(matches!(
            registry.activate(TrackRole::Haptic, route(PC_NORMAL_TRACK, 0)),
            Err(ProducerError::InvalidTrackForRole)
        ));
        assert!(matches!(
            registry.activate(TrackRole::Pc, route(HAPTIC_FULL_TRACK, 0)),
            Err(ProducerError::InvalidTrackForRole)
        ));
    }

    #[tokio::test]
    async fn registry_drop_notifies_outstanding_lease() {
        let mut registry = SubscriptionProducerRegistry::new();
        let mut lease = registry
            .activate(TrackRole::Pc, route(PC_NORMAL_TRACK, 0))
            .unwrap();
        drop(registry);
        lease.cancelled().await;
        assert!(lease.is_cancelled());
        assert_eq!(lease.record_object(), Err(ProducerError::LeaseCancelled));
    }

    fn finished(forward: Option<ForwardOutcome>) -> Option<RoleTerminal> {
        Some(RoleTerminal {
            terminal: ProducerTerminal::Finished,
            forward,
        })
    }

    fn ended(terminal: ProducerTerminal) -> Option<RoleTerminal> {
        Some(RoleTerminal {
            terminal,
            forward: None,
        })
    }

    #[test]
    fn completion_requires_finished_and_forwarder_closed_on_both_latest_generations() {
        use ForwardOutcome::*;
        use ProducerTerminal::*;
        let complete = finished(Some(Closed));
        assert!(current_routes_completed(complete, complete));
        // Finished without forward_closed -> not complete.
        assert!(!current_routes_completed(finished(None), complete));
        assert!(!current_routes_completed(complete, finished(None)));
        // Forwarder failure -> not complete.
        assert!(!current_routes_completed(finished(Some(Failed("serve_error"))), complete));
        assert!(!current_routes_completed(complete, finished(Some(Failed("serve_error")))));
        assert!(!current_routes_completed(None, None));
        assert!(!current_routes_completed(complete, None));
        assert!(!current_routes_completed(ended(RemoteClosed), complete));
        assert!(!current_routes_completed(complete, ended(RemoteClosed)));
        assert!(!current_routes_completed(ended(Error("producer")), complete));
        assert!(!current_routes_completed(complete, ended(Error("producer"))));
    }

    #[test]
    fn completion_state_is_pending_only_while_forwarders_of_finished_producers_are_open() {
        use CompletionState::*;
        use ForwardOutcome::*;
        use ProducerTerminal::*;
        let complete = finished(Some(Closed));
        assert_eq!(completion_state(complete, complete), Completed);
        assert_eq!(completion_state(finished(None), complete), Pending);
        assert_eq!(completion_state(complete, finished(None)), Pending);
        assert_eq!(completion_state(finished(None), finished(None)), Pending);
        assert_eq!(completion_state(finished(Some(Failed("aborted"))), complete), Incomplete);
        assert_eq!(completion_state(complete, finished(Some(Failed("aborted")))), Incomplete);
        assert_eq!(completion_state(None, complete), Incomplete);
        assert_eq!(completion_state(complete, None), Incomplete);
        assert_eq!(completion_state(ended(RemoteClosed), finished(None)), Incomplete);
        assert_eq!(completion_state(ended(Error("producer")), complete), Incomplete);
        assert_eq!(completion_state(None, None), Incomplete);
    }

    #[test]
    fn registry_tracks_latest_generation_terminal_and_notifies() {
        let mut registry = SubscriptionProducerRegistry::new();
        let mut watch = registry.terminal_watch();
        assert!(!registry.current_routes_completed());
        assert_eq!(registry.completion_state(), CompletionState::Incomplete);
        assert_eq!(registry.latest_terminal(TrackRole::Pc), None);

        let pc0 = route(PC_NORMAL_TRACK, 0);
        let hap0 = route(HAPTIC_FULL_TRACK, 0);
        let _pc_lease = registry.activate(TrackRole::Pc, pc0).unwrap();
        let _hap_lease = registry.activate(TrackRole::Haptic, hap0).unwrap();
        // Still active: cannot record, nothing finished.
        assert_eq!(
            registry.record_terminal(TrackRole::Pc, pc0, ProducerTerminal::Finished),
            Err(ProducerError::UnknownOrInactiveRoute)
        );
        assert!(!registry.current_routes_completed());

        // One role finished, the other still active -> Incomplete.
        registry.cancel(TrackRole::Pc, pc0).unwrap();
        registry
            .record_terminal(TrackRole::Pc, pc0, ProducerTerminal::Finished)
            .unwrap();
        assert!(watch.has_changed().unwrap());
        watch.borrow_and_update();
        assert_eq!(
            registry.latest_terminal(TrackRole::Pc),
            Some(ProducerTerminal::Finished)
        );
        assert_eq!(registry.completion_state(), CompletionState::Incomplete);

        // A generation records exactly once.
        assert_eq!(
            registry.record_terminal(TrackRole::Pc, pc0, ProducerTerminal::Finished),
            Err(ProducerError::DuplicateOrReusedGeneration)
        );

        // Both finished but forwarders open -> Pending, not completed.
        registry.cancel(TrackRole::Haptic, hap0).unwrap();
        registry
            .record_terminal(TrackRole::Haptic, hap0, ProducerTerminal::Finished)
            .unwrap();
        assert!(watch.has_changed().unwrap());
        watch.borrow_and_update();
        assert_eq!(registry.completion_state(), CompletionState::Pending);
        assert!(!registry.current_routes_completed());

        // Forwarder close is recorded once, only after Finished, and notifies.
        assert!(!registry.forward_closed(TrackRole::Pc, pc0));
        registry.mark_forward_closed(TrackRole::Pc, pc0).unwrap();
        assert!(watch.has_changed().unwrap());
        watch.borrow_and_update();
        assert!(registry.forward_closed(TrackRole::Pc, pc0));
        assert_eq!(
            registry.mark_forward_failed(TrackRole::Pc, pc0, "serve_error"),
            Err(ProducerError::DuplicateOrReusedGeneration)
        );
        assert_eq!(registry.completion_state(), CompletionState::Pending);
        registry.mark_forward_closed(TrackRole::Haptic, hap0).unwrap();
        assert!(watch.has_changed().unwrap());
        assert!(registry.current_routes_completed());
        assert_eq!(registry.completion_state(), CompletionState::Completed);

        // Switch/end boundary: no activation after completion.
        assert_eq!(
            registry
                .activate(TrackRole::Pc, route(PC_RECOVERY_TRACK, 1))
                .err(),
            Some(ProducerError::RunCompleted)
        );
        assert_eq!(registry.latest_terminal(TrackRole::Pc), Some(ProducerTerminal::Finished));
    }

    #[test]
    fn forwarder_failure_on_a_finished_producer_makes_the_run_incomplete() {
        let mut registry = SubscriptionProducerRegistry::new();
        let pc0 = route(PC_NORMAL_TRACK, 0);
        let hap0 = route(HAPTIC_FULL_TRACK, 0);
        let _pc_lease = registry.activate(TrackRole::Pc, pc0).unwrap();
        let _hap_lease = registry.activate(TrackRole::Haptic, hap0).unwrap();
        registry.cancel(TrackRole::Pc, pc0).unwrap();
        registry
            .record_terminal(TrackRole::Pc, pc0, ProducerTerminal::Finished)
            .unwrap();
        registry.mark_forward_closed(TrackRole::Pc, pc0).unwrap();
        registry.cancel(TrackRole::Haptic, hap0).unwrap();
        registry
            .record_terminal(TrackRole::Haptic, hap0, ProducerTerminal::Finished)
            .unwrap();
        assert_eq!(registry.completion_state(), CompletionState::Pending);
        registry
            .mark_forward_failed(TrackRole::Haptic, hap0, "shutdown_timeout")
            .unwrap();
        assert_eq!(registry.completion_state(), CompletionState::Incomplete);
        assert!(!registry.current_routes_completed());
        assert_eq!(
            registry.faults(),
            &[RecordedFault {
                role: TrackRole::Haptic,
                generation: 0,
                reason: "shutdown_timeout",
            }]
        );
        // Forward outcome is only valid for a Finished producer.
        let mut registry = SubscriptionProducerRegistry::new();
        let _pc_lease = registry.activate(TrackRole::Pc, pc0).unwrap();
        registry.cancel(TrackRole::Pc, pc0).unwrap();
        registry
            .record_terminal(TrackRole::Pc, pc0, ProducerTerminal::RemoteClosed)
            .unwrap();
        assert_eq!(
            registry.mark_forward_closed(TrackRole::Pc, pc0),
            Err(ProducerError::UnknownOrInactiveRoute)
        );
    }

    #[test]
    fn retired_generation_does_not_count_and_latest_remote_close_or_error_is_not_complete() {
        fn complete(registry: &mut SubscriptionProducerRegistry, role: TrackRole, route: Route) {
            registry.cancel(role, route).unwrap();
            registry
                .record_terminal(role, route, ProducerTerminal::Finished)
                .unwrap();
            registry.mark_forward_closed(role, route).unwrap();
        }
        // Old generation cancelled by a switch (RemoteClosed), new generation
        // completed -> true.
        let mut registry = SubscriptionProducerRegistry::new();
        let pc0 = route(PC_NORMAL_TRACK, 0);
        let pc1 = route(PC_RECOVERY_TRACK, 1);
        let hap0 = route(HAPTIC_FULL_TRACK, 0);
        let _pc0_lease = registry.activate(TrackRole::Pc, pc0).unwrap();
        let _hap0_lease = registry.activate(TrackRole::Haptic, hap0).unwrap();
        let _pc1_lease = registry.activate(TrackRole::Pc, pc1).unwrap();
        registry.cancel(TrackRole::Pc, pc0).unwrap();
        registry
            .record_terminal(TrackRole::Pc, pc0, ProducerTerminal::RemoteClosed)
            .unwrap();
        assert_eq!(registry.latest_terminal(TrackRole::Pc), None);
        complete(&mut registry, TrackRole::Haptic, hap0);
        assert!(!registry.current_routes_completed());
        complete(&mut registry, TrackRole::Pc, pc1);
        assert!(registry.current_routes_completed());

        // Latest generation remote-unsubscribed -> false even though the old
        // generation completed.
        let mut registry = SubscriptionProducerRegistry::new();
        let _pc0_lease = registry.activate(TrackRole::Pc, pc0).unwrap();
        let _hap0_lease = registry.activate(TrackRole::Haptic, hap0).unwrap();
        let _pc1_lease = registry.activate(TrackRole::Pc, pc1).unwrap();
        complete(&mut registry, TrackRole::Pc, pc0);
        complete(&mut registry, TrackRole::Haptic, hap0);
        assert!(!registry.current_routes_completed());
        registry.cancel(TrackRole::Pc, pc1).unwrap();
        registry
            .record_terminal(TrackRole::Pc, pc1, ProducerTerminal::RemoteClosed)
            .unwrap();
        assert!(!registry.current_routes_completed());
        assert_eq!(registry.completion_state(), CompletionState::Incomplete);

        // Latest generation error -> false.
        let mut registry = SubscriptionProducerRegistry::new();
        let _pc0_lease = registry.activate(TrackRole::Pc, pc0).unwrap();
        let _hap0_lease = registry.activate(TrackRole::Haptic, hap0).unwrap();
        registry.cancel(TrackRole::Pc, pc0).unwrap();
        registry
            .record_terminal(TrackRole::Pc, pc0, ProducerTerminal::Error("producer"))
            .unwrap();
        complete(&mut registry, TrackRole::Haptic, hap0);
        assert!(!registry.current_routes_completed());
        assert_eq!(registry.completion_state(), CompletionState::Incomplete);
        assert_eq!(registry.faults().len(), 1);
    }

    fn complete_role() -> Option<RoleTerminal> {
        Some(RoleTerminal {
            terminal: ProducerTerminal::Finished,
            forward: Some(ForwardOutcome::Closed),
        })
    }

    fn clean_snapshot() -> RegistrySnapshot {
        RegistrySnapshot {
            pc: complete_role(),
            haptic: complete_role(),
            faults: Vec::new(),
            outstanding: 0,
        }
    }

    fn session_end(before_production_end: bool) -> TransportEnd {
        TransportEnd {
            kind: "session",
            text: "Ok(Err(Decode(More(1))))".to_string(),
            at_us: 33_990_000,
            before_production_end,
        }
    }

    #[test]
    fn run_verdict_is_normal_only_for_a_clean_completed_registry() {
        // 1. both complete, no transport end -> normal
        assert_eq!(s3_run_verdict(&clean_snapshot(), &[]), RunVerdict::Normal);
        // 7. session end after completion with Decode(More(1)) -> normal
        assert_eq!(
            s3_run_verdict(&clean_snapshot(), &[session_end(false)]),
            RunVerdict::Normal
        );
        // 5. retired generation RemoteClosed after a normal switch: the
        // snapshot only carries the latest generation and no fault -> normal
        let mut registry = SubscriptionProducerRegistry::new();
        let pc0 = route(PC_NORMAL_TRACK, 0);
        let pc1 = route(PC_RECOVERY_TRACK, 1);
        let hap0 = route(HAPTIC_FULL_TRACK, 0);
        let _pc0 = registry.activate(TrackRole::Pc, pc0).unwrap();
        let _hap0 = registry.activate(TrackRole::Haptic, hap0).unwrap();
        let _pc1 = registry.activate(TrackRole::Pc, pc1).unwrap();
        registry.cancel(TrackRole::Pc, pc0).unwrap();
        registry
            .record_terminal(TrackRole::Pc, pc0, ProducerTerminal::RemoteClosed)
            .unwrap();
        for (role, route) in [(TrackRole::Pc, pc1), (TrackRole::Haptic, hap0)] {
            registry.cancel(role, route).unwrap();
            registry
                .record_terminal(role, route, ProducerTerminal::Finished)
                .unwrap();
            registry.mark_forward_closed(role, route).unwrap();
        }
        assert!(registry.production_ended());
        assert_eq!(registry.outstanding(), 0);
        assert_eq!(
            s3_run_verdict(&registry.snapshot(), &[session_end(false)]),
            RunVerdict::Normal
        );
    }

    #[test]
    fn run_verdict_reports_every_fault_class() {
        // 2. forwarder failed -> error
        let mut snapshot = clean_snapshot();
        snapshot.haptic = Some(RoleTerminal {
            terminal: ProducerTerminal::Finished,
            forward: Some(ForwardOutcome::Failed("remote_cancel")),
        });
        snapshot.faults.push(RecordedFault {
            role: TrackRole::Haptic,
            generation: 0,
            reason: "remote_cancel",
        });
        let RunVerdict::Error(reasons) = s3_run_verdict(&snapshot, &[]) else {
            panic!("forwarder failure must be an error");
        };
        assert!(reasons.iter().any(|r| r.contains("haptic: forwarder failed (remote_cancel)")));
        assert!(reasons.iter().any(|r| r.contains("fault haptic generation 0: remote_cancel")));

        // 3. log_write fault after completion -> error
        let mut snapshot = clean_snapshot();
        snapshot.faults.push(RecordedFault {
            role: TrackRole::Pc,
            generation: 0,
            reason: "log_write",
        });
        assert_eq!(
            s3_run_verdict(&snapshot, &[]),
            RunVerdict::Error(vec!["fault pc generation 0: log_write".to_string()])
        );

        // 4. aborted producer (retired generation) -> error
        let mut snapshot = clean_snapshot();
        snapshot.faults.push(RecordedFault {
            role: TrackRole::Pc,
            generation: 0,
            reason: "aborted",
        });
        assert!(matches!(s3_run_verdict(&snapshot, &[]), RunVerdict::Error(_)));

        // 6. session end before production end -> error
        let RunVerdict::Error(reasons) = s3_run_verdict(&clean_snapshot(), &[session_end(true)]) else {
            panic!("early session end must be an error");
        };
        assert_eq!(reasons.len(), 1);
        assert!(reasons[0].starts_with("session ended before production end at 33990000 us"));

        // 8. Pending never resolved -> error
        let mut snapshot = clean_snapshot();
        snapshot.pc = Some(RoleTerminal {
            terminal: ProducerTerminal::Finished,
            forward: None,
        });
        snapshot.outstanding = 1;
        let RunVerdict::Error(reasons) = s3_run_verdict(&snapshot, &[]) else {
            panic!("pending forwarder must be an error");
        };
        assert!(reasons.iter().any(|r| r == "pc: forwarder outcome never recorded"));
        assert!(reasons.iter().any(|r| r == "1 producer task(s) without a recorded outcome"));

        // 9. no producers at all -> error; latest remote-closed / error -> error
        let empty = RegistrySnapshot {
            pc: None,
            haptic: None,
            faults: Vec::new(),
            outstanding: 0,
        };
        assert!(matches!(s3_run_verdict(&empty, &[]), RunVerdict::Error(_)));
        let mut snapshot = clean_snapshot();
        snapshot.pc = Some(RoleTerminal {
            terminal: ProducerTerminal::RemoteClosed,
            forward: None,
        });
        assert!(matches!(s3_run_verdict(&snapshot, &[]), RunVerdict::Error(_)));
        let mut snapshot = clean_snapshot();
        snapshot.haptic = Some(RoleTerminal {
            terminal: ProducerTerminal::Error("producer"),
            forward: None,
        });
        assert!(matches!(s3_run_verdict(&snapshot, &[]), RunVerdict::Error(_)));
    }

    #[test]
    fn reservations_count_toward_latest_until_activated_or_released() {
        let mut registry = SubscriptionProducerRegistry::new();
        let pc0 = route(PC_NORMAL_TRACK, 0);
        let pc1 = route(PC_RECOVERY_TRACK, 1);
        let hap0 = route(HAPTIC_FULL_TRACK, 0);
        registry.reserve(TrackRole::Pc, pc0).unwrap();
        registry.reserve(TrackRole::Haptic, hap0).unwrap();
        assert_eq!(registry.outstanding(), 2);
        assert!(!registry.production_ended());
        // Reserved generation cannot be re-reserved or activated twice.
        assert_eq!(
            registry.reserve(TrackRole::Pc, pc0),
            Err(ProducerError::DuplicateOrReusedGeneration)
        );
        let _pc0 = registry.activate(TrackRole::Pc, pc0).unwrap();
        let _hap0 = registry.activate(TrackRole::Haptic, hap0).unwrap();
        assert_eq!(
            registry.activate(TrackRole::Pc, pc0).err(),
            Some(ProducerError::DuplicateOrReusedGeneration)
        );
        for (role, route) in [(TrackRole::Pc, pc0), (TrackRole::Haptic, hap0)] {
            registry.cancel(role, route).unwrap();
            registry
                .record_terminal(role, route, ProducerTerminal::Finished)
                .unwrap();
        }
        assert!(registry.production_ended());
        assert_eq!(registry.outstanding(), 2);
        registry.mark_forward_closed(TrackRole::Pc, pc0).unwrap();
        // A reservation made before completion moves "latest": production is
        // no longer ended and completion waits for it.
        registry.reserve(TrackRole::Pc, pc1).unwrap();
        assert!(!registry.production_ended());
        registry.mark_forward_closed(TrackRole::Haptic, hap0).unwrap();
        assert!(!registry.current_routes_completed());
        assert_eq!(registry.outstanding(), 1);
        // Released without fault (post-completion refusal): latest falls back.
        registry
            .release_reservation(TrackRole::Pc, pc1, None)
            .unwrap();
        assert!(registry.current_routes_completed());
        assert_eq!(registry.outstanding(), 0);
        assert!(registry.faults().is_empty());
        // Now completed: reservation refused.
        assert_eq!(
            registry.reserve(TrackRole::Pc, route(PC_HAPTIC_CRITICAL_TRACK, 2)),
            Err(ProducerError::RunCompleted)
        );
        assert_eq!(s3_run_verdict(&registry.snapshot(), &[]), RunVerdict::Normal);
        // Released with a fault is an error verdict.
        let mut registry = SubscriptionProducerRegistry::new();
        registry.reserve(TrackRole::Pc, pc0).unwrap();
        registry
            .release_reservation(TrackRole::Pc, pc0, Some("subscription_setup"))
            .unwrap();
        assert_eq!(registry.outstanding(), 0);
        assert_eq!(registry.faults().len(), 1);
        assert_eq!(
            registry.release_reservation(TrackRole::Pc, pc0, None),
            Err(ProducerError::UnknownOrInactiveRoute)
        );
    }

    #[test]
    fn abandoned_tasks_record_a_fault_at_every_lifecycle_stage() {
        let pc0 = route(PC_NORMAL_TRACK, 0);
        // Reserved, never activated.
        let mut registry = SubscriptionProducerRegistry::new();
        registry.reserve(TrackRole::Pc, pc0).unwrap();
        registry.record_abandoned(TrackRole::Pc, pc0);
        assert_eq!(registry.faults()[0].reason, "aborted");
        assert_eq!(registry.outstanding(), 0);
        assert_eq!(registry.latest_role_terminal(TrackRole::Pc), None);
        // Active lease, aborted mid-production.
        let mut registry = SubscriptionProducerRegistry::new();
        let _lease = registry.activate(TrackRole::Pc, pc0).unwrap();
        registry.record_abandoned(TrackRole::Pc, pc0);
        assert_eq!(registry.active_count(), 0);
        assert_eq!(
            registry.latest_terminal(TrackRole::Pc),
            Some(ProducerTerminal::Error("aborted"))
        );
        assert_eq!(registry.faults()[0].reason, "aborted");
        // Finished, forwarder outcome missing.
        let mut registry = SubscriptionProducerRegistry::new();
        let _lease = registry.activate(TrackRole::Pc, pc0).unwrap();
        registry.cancel(TrackRole::Pc, pc0).unwrap();
        registry
            .record_terminal(TrackRole::Pc, pc0, ProducerTerminal::Finished)
            .unwrap();
        registry.record_abandoned(TrackRole::Pc, pc0);
        assert_eq!(
            registry.latest_role_terminal(TrackRole::Pc).unwrap().forward,
            Some(ForwardOutcome::Failed("aborted"))
        );
        // Complete outcome: no-op.
        let mut registry = SubscriptionProducerRegistry::new();
        let _lease = registry.activate(TrackRole::Pc, pc0).unwrap();
        registry.cancel(TrackRole::Pc, pc0).unwrap();
        registry
            .record_terminal(TrackRole::Pc, pc0, ProducerTerminal::Finished)
            .unwrap();
        registry.mark_forward_closed(TrackRole::Pc, pc0).unwrap();
        registry.record_abandoned(TrackRole::Pc, pc0);
        assert!(registry.faults().is_empty());
        // Unknown key: no-op.
        registry.record_abandoned(TrackRole::Haptic, route(HAPTIC_FULL_TRACK, 7));
        assert!(registry.faults().is_empty());
        // Drop guard drives the same path.
        let registry = Arc::new(Mutex::new(SubscriptionProducerRegistry::new()));
        registry.lock().unwrap().reserve(TrackRole::Pc, pc0).unwrap();
        drop(ProducerTaskGuard::new(registry.clone(), TrackRole::Pc, pc0));
        assert_eq!(registry.lock().unwrap().faults()[0].reason, "aborted");
        // record_fault after a clean terminal (log_write).
        let mut registry = SubscriptionProducerRegistry::new();
        let _lease = registry.activate(TrackRole::Pc, pc0).unwrap();
        registry.cancel(TrackRole::Pc, pc0).unwrap();
        registry
            .record_terminal(TrackRole::Pc, pc0, ProducerTerminal::Finished)
            .unwrap();
        registry.mark_forward_closed(TrackRole::Pc, pc0).unwrap();
        registry.record_fault(TrackRole::Pc, pc0, "log_write").unwrap();
        assert_eq!(registry.faults()[0].reason, "log_write");
        assert_eq!(
            registry.record_fault(TrackRole::Haptic, route(HAPTIC_FULL_TRACK, 0), "log_write"),
            Err(ProducerError::UnknownOrInactiveRoute)
        );
    }
}
