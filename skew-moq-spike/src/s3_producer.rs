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
    /// The producer returned an error.
    Error,
}

/// Outcome of the forwarder of a `Finished` producer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForwardOutcome {
    /// The forwarder closed within the shutdown timeout, ended cleanly, and
    /// the produced count matched the recorded count.
    Closed,
    /// Shutdown timeout, serve error, or produced-count mismatch.
    Failed,
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
    /// Highest activated generation per role: the "current" route.
    latest: HashMap<TrackRole, u64>,
    terminal: HashMap<ProducerKey, ProducerTerminal>,
    forward: HashMap<ProducerKey, ForwardOutcome>,
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
            latest: HashMap::new(),
            terminal: HashMap::new(),
            forward: HashMap::new(),
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
        self.terminal_tx.send_modify(|version| *version += 1);
        Ok(())
    }

    /// Record that the forwarder of a `Finished` producer closed cleanly and
    /// its produced count matched. This is the second half of completion.
    pub fn mark_forward_closed(&mut self, role: TrackRole, route: Route) -> Result<(), ProducerError> {
        self.mark_forward(role, route, ForwardOutcome::Closed)
    }

    /// Record that the forwarder of a `Finished` producer timed out, ended with
    /// an error, or failed the produced-count check. The role is then never
    /// complete.
    pub fn mark_forward_failed(&mut self, role: TrackRole, route: Route) -> Result<(), ProducerError> {
        self.mark_forward(role, route, ForwardOutcome::Failed)
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
        let generation = *self.latest.get(&role)?;
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
        // Switch/end boundary: once both current routes completed the run,
        // no later subscription may move `latest` past that snapshot.
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
        self.latest
            .entry(role)
            .and_modify(|latest| *latest = (*latest).max(route.generation))
            .or_insert(route.generation);

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
pub async fn serve_subscription_producer<F, Fut>(
    mut subscribed: Subscribed,
    role: TrackRole,
    route: Route,
    registry: Arc<Mutex<SubscriptionProducerRegistry>>,
    shutdown_timeout: Duration,
    producer: F,
) -> anyhow::Result<SubscriptionTaskResult>
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
    subscribed
        .accept(&reader)
        .await
        .context("accept subscription before producer start")?;

    let activation = registry
        .lock()
        .map_err(|_| anyhow!("producer registry poisoned"))?
        .activate(role, route);
    let lease = match activation {
        Ok(lease) => lease,
        Err(ProducerError::RunCompleted) => {
            // The run already completed on both current routes: reject the
            // late subscription without starting a producer or logging a
            // producer start/stop row.
            let _ = subscribed.close(ServeError::not_found_ctx(format!(
                "S3 subscription '{}' generation {} arrived after run completion",
                route.name, route.generation
            )));
            return Err(anyhow!(
                "S3 subscription '{}' generation {} arrived after run completion",
                route.name,
                route.generation
            ));
        }
        Err(error) => return Err(anyhow!("activate producer lease: {error:?}")),
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
            normalize_serve_end(serve_result)?;
            Ok(SubscriptionTaskResult {
                end: SubscriptionTaskEnd::RemoteClosed,
                role,
                route,
                objects: route_objects(&registry, role, route)?,
            })
        }
        producer_result = &mut produce => {
            let produced = match producer_result {
                Ok(produced) => produced,
                Err(error) => {
                    cancel_shared(&registry, role, route)?;
                    record_terminal_shared(&registry, role, route, ProducerTerminal::Error)?;
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
            // failure below records `ForwardOutcome::Failed` so the role can
            // never be reported complete, and a completion waiter is released.
            let forwarded: anyhow::Result<u64> = async {
                let serve_result = tokio::time::timeout(shutdown_timeout, &mut serve)
                    .await
                    .context("forwarder did not close after producer end")?;
                normalize_serve_end(serve_result)?;
                let recorded = route_objects(&registry, role, route)?;
                if produced != recorded {
                    return Err(anyhow!(
                        "producer count mismatch for {} generation {}: returned {}, recorded {}",
                        route.name,
                        route.generation,
                        produced,
                        recorded
                    ));
                }
                Ok(recorded)
            }
            .await;
            let recorded = match forwarded {
                Ok(recorded) => recorded,
                Err(error) => {
                    mark_forward_shared(&registry, role, route, ForwardOutcome::Failed)?;
                    return Err(error);
                }
            };
            mark_forward_shared(&registry, role, route, ForwardOutcome::Closed)?;
            Ok(SubscriptionTaskResult {
                end: SubscriptionTaskEnd::ProducerFinished,
                role,
                route,
                objects: recorded,
            })
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
        ForwardOutcome::Failed => registry.mark_forward_failed(role, route),
    }
    .map_err(|error| anyhow!("record producer forwarder outcome: {error:?}"))
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
        assert!(!current_routes_completed(finished(Some(Failed)), complete));
        assert!(!current_routes_completed(complete, finished(Some(Failed))));
        assert!(!current_routes_completed(None, None));
        assert!(!current_routes_completed(complete, None));
        assert!(!current_routes_completed(ended(RemoteClosed), complete));
        assert!(!current_routes_completed(complete, ended(RemoteClosed)));
        assert!(!current_routes_completed(ended(Error), complete));
        assert!(!current_routes_completed(complete, ended(Error)));
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
        assert_eq!(completion_state(finished(Some(Failed)), complete), Incomplete);
        assert_eq!(completion_state(complete, finished(Some(Failed))), Incomplete);
        assert_eq!(completion_state(None, complete), Incomplete);
        assert_eq!(completion_state(complete, None), Incomplete);
        assert_eq!(completion_state(ended(RemoteClosed), finished(None)), Incomplete);
        assert_eq!(completion_state(ended(Error), complete), Incomplete);
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
            registry.mark_forward_failed(TrackRole::Pc, pc0),
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
        registry.mark_forward_failed(TrackRole::Haptic, hap0).unwrap();
        assert_eq!(registry.completion_state(), CompletionState::Incomplete);
        assert!(!registry.current_routes_completed());
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
            .record_terminal(TrackRole::Pc, pc0, ProducerTerminal::Error)
            .unwrap();
        complete(&mut registry, TrackRole::Haptic, hap0);
        assert!(!registry.current_routes_completed());
        assert_eq!(registry.completion_state(), CompletionState::Incomplete);
    }
}
