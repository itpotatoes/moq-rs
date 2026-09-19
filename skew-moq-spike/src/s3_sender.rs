//! Actual subscription-scoped S3 publisher path.
//!
//! Only a remotely subscribed route owns a producer. Every producer derives
//! identity from one future run anchor, so make-before-break never restarts
//! seq/PTS/event numbering and inactive tiers generate no retained objects.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use bytes::Bytes;
use moq_transport::coding::TrackNamespace;
use moq_transport::serve::TrackWriter;
use moq_transport::session::Publisher;
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::s3_logging::S3RefusalCause;
use crate::s3_producer::{
    serve_subscription_producer, ProducerLease, ProducerTaskGuard, RunSlotClock, SlotError,
    SubscriptionProducerRegistry,
};
use crate::s3_switch::{
    Route, TrackRole, HAPTIC_ESSENTIAL_TRACK, HAPTIC_FULL_TRACK, PC_HAPTIC_CRITICAL_TRACK,
    PC_NORMAL_TRACK, PC_RECOVERY_TRACK,
};
use crate::{
    anchor_tick, now_us, pack_header, pcm_tick_payload, timestamp_us, validate_v5_rates,
    JsonlLogger, HAPTIC_TIER_FULL, HDR, PCM_SAMPLE_RATE_HZ, TRACK_HAPTIC, TRACK_PC,
};

pub const S3_PC_PRIORITY: u8 = 1;
pub const S3_HAPTIC_PRIORITY: u8 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcceptRoute {
    pub role: TrackRole,
    pub route: Route,
}

pub type AcceptRouteMap = Arc<Mutex<HashMap<u64, AcceptRoute>>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceSchedule {
    pub warmup_start_us: Option<u64>,
    pub measurement_start_us: u64,
    pub end_us: u64,
}

impl SourceSchedule {
    fn validate(self) -> anyhow::Result<()> {
        if let Some(start) = self.warmup_start_us {
            if self.measurement_start_us.checked_sub(start)
                != Some(crate::phase::WARMUP_DURATION_US)
            {
                bail!("S3 registered warmup must be exactly 3,000,000 us");
            }
        }
        if self.end_us <= self.measurement_start_us {
            bail!("S3 end must be after the common measurement anchor");
        }
        Ok(())
    }

    fn measurement_clock(self) -> RunSlotClock {
        RunSlotClock::new(self.measurement_start_us)
    }
}

fn initial_measurement_slot(
    schedule: SourceSchedule,
    route_generation: u64,
    observed_at_us: u64,
    rate_hz: u64,
) -> Result<u64, SlotError> {
    if route_generation == 0 {
        // The initial Normal/full routes (generation 0) own the common t0 and
        // must emit measurement identity zero even when the gate wakes a few
        // us late. This holds for both the registered (warmup pass) schedule
        // and the legacy `now + warmup` schedule, matching the static S1/S2
        // path which always emits slot 0. Switched routes (generation >= 1)
        // retain catch-up semantics.
        Ok(0)
    } else {
        schedule
            .measurement_clock()
            .next_slot(observed_at_us, rate_hz)
    }
}

pub struct SenderContext {
    pub schedule: watch::Receiver<Option<SourceSchedule>>,
    pub measurement_gate: watch::Receiver<bool>,
    pub pc_rate_hz: u64,
    pub haptic_rate_hz: u64,
    pub normal_frames: Arc<Vec<Vec<u8>>>,
    pub recovery_frames: Arc<Vec<Vec<u8>>>,
    pub critical_frames: Arc<Vec<Vec<u8>>>,
    pub haptic_pcm: Arc<Vec<u8>>,
    pub logger: Arc<Mutex<JsonlLogger>>,
    pub shutdown_timeout: Duration,
}

impl SenderContext {
    pub fn validate(&self) -> anyhow::Result<()> {
        validate_v5_rates(self.pc_rate_hz, self.haptic_rate_hz)
            .context("S3 requires the v5 30:90 Hz rate contract")?;
        if self.shutdown_timeout.is_zero() {
            bail!("S3 producer shutdown timeout must be greater than zero");
        }
        if self.normal_frames.is_empty()
            || self.recovery_frames.is_empty()
            || self.critical_frames.is_empty()
        {
            bail!("every S3 PC tier must contain at least one frame");
        }
        if self.haptic_pcm.is_empty() {
            bail!("S3 haptic PCM must not be empty");
        }
        Ok(())
    }

    async fn await_schedule(&self) -> anyhow::Result<SourceSchedule> {
        let mut schedule = self.schedule.clone();
        loop {
            if let Some(value) = *schedule.borrow_and_update() {
                value.validate()?;
                return Ok(value);
            }
            schedule
                .changed()
                .await
                .context("S3 source schedule authority closed")?;
        }
    }

    async fn await_measurement_gate(&self) -> anyhow::Result<()> {
        let mut gate = self.measurement_gate.clone();
        loop {
            if *gate.borrow_and_update() {
                return Ok(());
            }
            gate.changed()
                .await
                .context("S3 measurement gate authority closed")?;
        }
    }
}

#[derive(Default)]
struct RouteAllocator {
    next_pc: u64,
    next_haptic: u64,
}

impl RouteAllocator {
    fn allocate(&mut self, name: &str) -> anyhow::Result<(TrackRole, Route)> {
        let role = role_for_track(name)
            .ok_or_else(|| anyhow!("unsupported S3 subscription track '{name}'"))?;
        let next = match role {
            TrackRole::Pc => &mut self.next_pc,
            TrackRole::Haptic => &mut self.next_haptic,
        };
        if *next == 0 {
            let expected = match role {
                TrackRole::Pc => PC_NORMAL_TRACK,
                TrackRole::Haptic => HAPTIC_FULL_TRACK,
            };
            if name != expected {
                bail!(
                    "first S3 {} subscription must be '{}', got '{}'",
                    role.as_str(),
                    expected,
                    name
                );
            }
        }
        let route = Route {
            name: canonical_track(name).expect("validated S3 track"),
            generation: *next,
        };
        *next = next
            .checked_add(1)
            .context("S3 route generation overflow")?;
        Ok((role, route))
    }
}

fn canonical_track(name: &str) -> Option<&'static str> {
    match name {
        PC_NORMAL_TRACK => Some(PC_NORMAL_TRACK),
        PC_RECOVERY_TRACK => Some(PC_RECOVERY_TRACK),
        PC_HAPTIC_CRITICAL_TRACK => Some(PC_HAPTIC_CRITICAL_TRACK),
        HAPTIC_FULL_TRACK => Some(HAPTIC_FULL_TRACK),
        HAPTIC_ESSENTIAL_TRACK => Some(HAPTIC_ESSENTIAL_TRACK),
        _ => None,
    }
}

fn role_for_track(name: &str) -> Option<TrackRole> {
    match name {
        PC_NORMAL_TRACK | PC_RECOVERY_TRACK | PC_HAPTIC_CRITICAL_TRACK => Some(TrackRole::Pc),
        HAPTIC_FULL_TRACK | HAPTIC_ESSENTIAL_TRACK => Some(TrackRole::Haptic),
        _ => None,
    }
}

/// The REQUEST-VALIDITY verdict of one arriving subscription, independent of
/// whether the run is still producing.
///
/// It exists because the post-run drain window must apply the SAME checks the
/// accept loop applies. Without it the drain window answered every arriving
/// subscription with the run-ended code, so an unsupported track name or an
/// invalid parameter set — fatal faults the receiver must never absorb —
/// silently became the receiver's non-fatal path for as long as the drain
/// window lasted.
///
/// Only the STATELESS checks live here: the track name and the
/// DELIVERY_TIMEOUT parameter. `RouteAllocator::allocate`'s ordering rule
/// ("the first PC subscription must be `PC_NORMAL_TRACK`, i.e. `pc`") is deliberately NOT part of
/// it — it is a property of the allocator's state machine, not of the
/// request, and after the run has ended no generation may be allocated at
/// all. A request that is valid here and would still have failed allocation
/// is therefore refused with the run-ended code in the drain window; that is
/// correct, because allocation is exactly what a finished run cannot do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestValidity {
    /// A supported S3 track with an acceptable parameter set.
    Valid(TrackRole),
    /// The track name is not an S3 track: `not_found` (0x10).
    UnsupportedTrack,
    /// The DELIVERY_TIMEOUT parameter is not the registered one for this
    /// role: internal error, carrying the accept loop's own message.
    BadDeliveryTimeout(&'static str),
}

impl RequestValidity {
    /// The close error for an invalid request — byte-identical to what the
    /// accept loop has always sent, so both sites cannot drift apart.
    fn refusal(self, name: &str) -> Option<moq_transport::serve::ServeError> {
        match self {
            RequestValidity::Valid(_) => None,
            RequestValidity::UnsupportedTrack => {
                Some(moq_transport::serve::ServeError::not_found_ctx(format!(
                    "unsupported S3 subscription track '{name}'"
                )))
            }
            RequestValidity::BadDeliveryTimeout(message) => {
                Some(moq_transport::serve::ServeError::internal_ctx(message))
            }
        }
    }
}

/// DELIVERY_TIMEOUT is hop-local. The receiver's registered 67 ms request is
/// enforced by the relay on relay->receiver forwarding and need not be
/// repeated on relay->publisher. If a direct peer does send it here, only the
/// frozen value is accepted; haptic must not carry one at all.
fn classify_subscription_request(name: &str, delivery_timeout_ms: Option<u64>) -> RequestValidity {
    let Some(role) = role_for_track(name) else {
        return RequestValidity::UnsupportedTrack;
    };
    match role {
        TrackRole::Pc if delivery_timeout_ms.is_some() && delivery_timeout_ms != Some(67) => {
            RequestValidity::BadDeliveryTimeout(
                "S3 PC subscription carried a non-67ms DELIVERY_TIMEOUT",
            )
        }
        TrackRole::Haptic if delivery_timeout_ms.is_some() => RequestValidity::BadDeliveryTimeout(
            "S3 haptic subscription must not carry DELIVERY_TIMEOUT",
        ),
        _ => RequestValidity::Valid(role),
    }
}

fn pc_tier(name: &str) -> Option<u16> {
    match name {
        PC_NORMAL_TRACK => Some(2),
        PC_RECOVERY_TRACK => Some(3),
        PC_HAPTIC_CRITICAL_TRACK => Some(4),
        _ => None,
    }
}

fn frames_for_route<'a>(context: &'a SenderContext, name: &str) -> Option<&'a Arc<Vec<Vec<u8>>>> {
    match name {
        PC_NORMAL_TRACK => Some(&context.normal_frames),
        PC_RECOVERY_TRACK => Some(&context.recovery_frames),
        PC_HAPTIC_CRITICAL_TRACK => Some(&context.critical_frames),
        _ => None,
    }
}

async fn sleep_until_us(target_us: u64) {
    let wait = target_us.saturating_sub(now_us());
    if wait > 0 {
        tokio::time::sleep(Duration::from_micros(wait)).await;
    }
}

#[allow(clippy::too_many_arguments)]
fn write_pc_object(
    subgroups: &mut moq_transport::serve::SubgroupsWriter,
    lease: &ProducerLease,
    context: &SenderContext,
    route: Route,
    tier: u16,
    frames: &Arc<Vec<Vec<u8>>>,
    slot: u64,
    seq: u32,
    pts_us: u64,
    event_id: u32,
    warmup: bool,
) -> anyhow::Result<()> {
    let payload = &frames[(slot as usize) % frames.len()];
    let t_gen = now_us();
    let header = pack_header(
        TRACK_PC,
        tier,
        seq,
        pts_us,
        event_id,
        t_gen,
        payload.len() as u32,
    );
    let mut bytes = Vec::with_capacity(HDR + payload.len());
    bytes.extend_from_slice(&header);
    bytes.extend_from_slice(payload);
    if warmup {
        lease
            .authorize_warmup_object()
            .map_err(|error| anyhow!("authorize S3 PC warmup object: {error:?}"))?;
    } else {
        lease
            .record_object()
            .map_err(|error| anyhow!("authorize S3 PC object: {error:?}"))?;
    }
    let mut subgroup = subgroups.append(S3_PC_PRIORITY).context("S3 PC append")?;
    let identity = (subgroup.group_id, subgroup.subgroup_id);
    let mut object = subgroup.create(bytes.len(), None).context("S3 PC create")?;
    let object_id = object.object_id;
    object.write(Bytes::from(bytes)).context("S3 PC write")?;
    drop(object);
    drop(subgroup);
    let t_send = now_us();
    let mut logger = context
        .logger
        .lock()
        .map_err(|_| anyhow!("TX logger poisoned"))?;
    if warmup {
        logger.try_log_warmup_tx_s3(
            TrackRole::Pc,
            route,
            tier,
            seq,
            pts_us,
            event_id,
            payload.len(),
            t_gen,
            t_send,
            Some((identity.0, identity.1, object_id)),
        )?;
    } else {
        logger.try_log_tx_s3(
            TrackRole::Pc,
            route,
            tier,
            seq,
            pts_us,
            event_id,
            payload.len(),
            t_gen,
            t_send,
            Some((identity.0, identity.1, object_id)),
        )?;
    }
    Ok(())
}

fn haptic_payload(context: &SenderContext, tick: u64) -> anyhow::Result<Vec<u8>> {
    pcm_tick_payload(
        &context.haptic_pcm,
        tick,
        PCM_SAMPLE_RATE_HZ,
        context.haptic_rate_hz,
    )
    .context("slice S3 haptic PCM tick")
}

async fn produce_pc(
    writer: TrackWriter,
    lease: ProducerLease,
    context: Arc<SenderContext>,
) -> anyhow::Result<u64> {
    let route = lease.route();
    let tier = pc_tier(route.name).context("PC producer received a non-PC route")?;
    let frames = frames_for_route(&context, route.name)
        .context("missing S3 PC frames")?
        .clone();
    let schedule = context.await_schedule().await?;
    let mut subgroups = writer.subgroups().context("S3 PC subgroups")?;
    let mut count = 0u64;

    if route.name == PC_NORMAL_TRACK {
        if let Some(warmup_start_us) = schedule.warmup_start_us {
            let warmup_clock = RunSlotClock::new(warmup_start_us);
            let mut slot = warmup_clock
                .next_slot(now_us(), context.pc_rate_hz)
                .map_err(|error| anyhow!("derive initial PC warmup slot: {error:?}"))?;
            loop {
                let pts_us = timestamp_us(slot, context.pc_rate_hz);
                let target_us = warmup_start_us.saturating_add(pts_us);
                if target_us >= schedule.measurement_start_us {
                    break;
                }
                sleep_until_us(target_us).await;
                if lease.is_cancelled() {
                    return Ok(count);
                }
                write_pc_object(
                    &mut subgroups,
                    &lease,
                    &context,
                    route,
                    tier,
                    &frames,
                    slot,
                    crate::warmup_seq(slot)?,
                    pts_us,
                    u32::try_from(slot.checked_add(1).context("S3 PC warmup event overflow")?)
                        .context("S3 PC warmup event overflow")?,
                    true,
                )?;
                slot = slot.checked_add(1).context("S3 PC warmup slot overflow")?;
            }
        }
    }
    context.await_measurement_gate().await?;
    if lease.is_cancelled() {
        return Ok(count);
    }
    let mut slot =
        initial_measurement_slot(schedule, route.generation, now_us(), context.pc_rate_hz)
            .map_err(|error| anyhow!("derive initial PC slot: {error:?}"))?;

    loop {
        let pts_us = timestamp_us(slot, context.pc_rate_hz);
        let target_us = schedule.measurement_start_us.saturating_add(pts_us);
        if target_us >= schedule.end_us {
            break;
        }
        sleep_until_us(target_us).await;
        if now_us() >= schedule.end_us || lease.is_cancelled() {
            break;
        }
        let seq = u32::try_from(slot).context("S3 PC sequence overflow")?;
        let event_id = u32::try_from(slot.checked_add(1).context("S3 PC event overflow")?)
            .context("S3 PC event overflow")?;
        write_pc_object(
            &mut subgroups,
            &lease,
            &context,
            route,
            tier,
            &frames,
            slot,
            seq,
            pts_us,
            event_id,
            false,
        )?;
        count += 1;
        slot = slot.checked_add(1).context("S3 PC slot overflow")?;
    }
    Ok(count)
}

fn exact_frame_for_tick(tick: u64, pc_rate_hz: u64, haptic_rate_hz: u64) -> Option<u64> {
    let ratio = validate_v5_rates(pc_rate_hz, haptic_rate_hz).ok()?;
    (tick % ratio == 0).then_some(tick / ratio)
}

async fn write_haptic_object(
    subgroup: &mut moq_transport::serve::SubgroupWriter,
    lease: &ProducerLease,
    context: &SenderContext,
    seq: u32,
    tick: u64,
    pts_us: u64,
    event_id: u32,
    warmup: bool,
) -> anyhow::Result<()> {
    let payload = haptic_payload(context, tick)?;
    let t_gen = now_us();
    let header = pack_header(
        TRACK_HAPTIC,
        HAPTIC_TIER_FULL,
        seq,
        pts_us,
        event_id,
        t_gen,
        payload.len() as u32,
    );
    let mut bytes = Vec::with_capacity(HDR + payload.len());
    bytes.extend_from_slice(&header);
    bytes.extend_from_slice(&payload);

    if warmup {
        lease
            .authorize_warmup_object()
            .map_err(|error| anyhow!("authorize S3 haptic warmup object: {error:?}"))?;
    } else {
        lease
            .record_object()
            .map_err(|error| anyhow!("authorize S3 haptic object: {error:?}"))?;
    }
    let identity = (subgroup.group_id, subgroup.subgroup_id);
    let mut object = subgroup
        .create(bytes.len(), None)
        .context("S3 haptic create")?;
    let object_id = object.object_id;
    object
        .write(Bytes::from(bytes))
        .context("S3 haptic write")?;
    drop(object);
    let t_send = now_us();
    let mut logger = context
        .logger
        .lock()
        .map_err(|_| anyhow!("TX logger poisoned"))?;
    if warmup {
        logger.try_log_warmup_tx_s3(
            TrackRole::Haptic,
            lease.route(),
            HAPTIC_TIER_FULL,
            seq,
            pts_us,
            event_id,
            payload.len(),
            t_gen,
            t_send,
            Some((identity.0, identity.1, object_id)),
        )?;
    } else {
        logger.try_log_tx_s3(
            TrackRole::Haptic,
            lease.route(),
            HAPTIC_TIER_FULL,
            seq,
            pts_us,
            event_id,
            payload.len(),
            t_gen,
            t_send,
            Some((identity.0, identity.1, object_id)),
        )?;
    }
    Ok(())
}

async fn produce_haptic(
    writer: TrackWriter,
    lease: ProducerLease,
    context: Arc<SenderContext>,
) -> anyhow::Result<u64> {
    let essential = lease.route().name == HAPTIC_ESSENTIAL_TRACK;
    let mut subgroups = writer.subgroups().context("S3 haptic subgroups")?;
    let mut subgroup = subgroups
        .append(S3_HAPTIC_PRIORITY)
        .context("S3 haptic append")?;
    let mut count = 0u64;
    let schedule = context.await_schedule().await?;

    if !essential {
        if let Some(warmup_start_us) = schedule.warmup_start_us {
            let warmup_clock = RunSlotClock::new(warmup_start_us);
            let mut tick = warmup_clock
                .full_haptic_slot(now_us(), context.haptic_rate_hz)
                .map_err(|error| anyhow!("derive initial haptic warmup slot: {error:?}"))?;
            loop {
                let nominal_pts = timestamp_us(tick, context.haptic_rate_hz);
                let target_us = warmup_start_us.saturating_add(nominal_pts);
                if target_us >= schedule.measurement_start_us {
                    break;
                }
                sleep_until_us(target_us).await;
                if lease.is_cancelled() {
                    return Ok(count);
                }
                let (pts_us, event_id) =
                    match exact_frame_for_tick(tick, context.pc_rate_hz, context.haptic_rate_hz) {
                        Some(frame) => (
                            timestamp_us(frame, context.pc_rate_hz),
                            u32::try_from(
                                frame
                                    .checked_add(1)
                                    .context("full haptic warmup event overflow")?,
                            )
                            .context("full haptic warmup event overflow")?,
                        ),
                        None => (nominal_pts, 0),
                    };
                write_haptic_object(
                    &mut subgroup,
                    &lease,
                    &context,
                    crate::warmup_seq(tick)?,
                    tick,
                    pts_us,
                    event_id,
                    true,
                )
                .await?;
                tick = tick.checked_add(1).context("haptic warmup slot overflow")?;
            }
        }
    }
    context.await_measurement_gate().await?;
    if lease.is_cancelled() {
        return Ok(count);
    }
    if essential {
        let mut frame = initial_measurement_slot(
            schedule,
            lease.route().generation,
            now_us(),
            context.pc_rate_hz,
        )
        .map_err(|error| anyhow!("derive essential PC slot: {error:?}"))?;
        loop {
            let pts_us = timestamp_us(frame, context.pc_rate_hz);
            let target_us = schedule.measurement_start_us.saturating_add(pts_us);
            if target_us >= schedule.end_us {
                break;
            }
            sleep_until_us(target_us).await;
            if now_us() >= schedule.end_us || lease.is_cancelled() {
                break;
            }
            let tick = anchor_tick(frame, context.pc_rate_hz, context.haptic_rate_hz)
                .context("derive essential haptic anchor tick")?;
            let seq = u32::try_from(tick).context("essential haptic sequence overflow")?;
            let event_id = u32::try_from(
                frame
                    .checked_add(1)
                    .context("essential haptic event overflow")?,
            )
            .context("essential haptic event overflow")?;
            write_haptic_object(
                &mut subgroup,
                &lease,
                &context,
                seq,
                tick,
                pts_us,
                event_id,
                false,
            )
            .await?;
            count += 1;
            frame = frame.checked_add(1).context("essential PC slot overflow")?;
        }
    } else {
        let mut tick = initial_measurement_slot(
            schedule,
            lease.route().generation,
            now_us(),
            context.haptic_rate_hz,
        )
        .map_err(|error| anyhow!("derive full haptic slot: {error:?}"))?;
        loop {
            let nominal_pts = timestamp_us(tick, context.haptic_rate_hz);
            let target_us = schedule.measurement_start_us.saturating_add(nominal_pts);
            if target_us >= schedule.end_us {
                break;
            }
            sleep_until_us(target_us).await;
            if now_us() >= schedule.end_us || lease.is_cancelled() {
                break;
            }
            let (pts_us, event_id) =
                match exact_frame_for_tick(tick, context.pc_rate_hz, context.haptic_rate_hz) {
                    Some(frame) => (
                        timestamp_us(frame, context.pc_rate_hz),
                        u32::try_from(frame.checked_add(1).context("full haptic event overflow")?)
                            .context("full haptic event overflow")?,
                    ),
                    None => (nominal_pts, 0),
                };
            let seq = u32::try_from(tick).context("full haptic sequence overflow")?;
            write_haptic_object(
                &mut subgroup,
                &lease,
                &context,
                seq,
                tick,
                pts_us,
                event_id,
                false,
            )
            .await?;
            count += 1;
            tick = tick.checked_add(1).context("full haptic slot overflow")?;
        }
    }
    Ok(count)
}

/// Bound for the abort-and-join fallback inside `drain_children`.
pub const DRAIN_ABANDON_BOUND: Duration = Duration::from_secs(2);
/// Margin main adds on top of the namespace task's own bounds.
pub const NAMESPACE_JOIN_MARGIN: Duration = Duration::from_secs(1);

/// The bound main must allow the namespace task after the stop signal so the
/// task's own drain (`shutdown_timeout + 1 s`) and its abandon fallback
/// (`DRAIN_ABANDON_BOUND`) can both complete and write `children_unsettled`
/// before main gives up on it.
pub fn namespace_join_bound(shutdown_timeout: Duration) -> Duration {
    shutdown_timeout
        .saturating_add(Duration::from_secs(1))
        .saturating_add(DRAIN_ABANDON_BOUND)
        .saturating_add(NAMESPACE_JOIN_MARGIN)
}

/// What `run_namespace` returns: the end it observed and WHEN it observed
/// it (source time), independent of when main collects the join.
#[derive(Debug)]
pub struct NamespaceExit {
    pub observed_at_us: u64,
    pub end: anyhow::Result<NamespaceEnd>,
}

/// How `run_namespace` ended without an error, observed by the namespace
/// task itself at the moment its future completed (the close REASON); main
/// never re-derives it from its own state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NamespaceEnd {
    /// Exited via our stop watch; child cleanup was attempted within its bound.
    Drained {
        /// If the subscription stream ended or errored while draining, its
        /// text is preserved here for audit.
        subscribe_end: Option<String>,
    },
    /// The namespace watch peer was dropped: `closed()` returned `Ok(())`
    /// or `subscribed()` returned `Ok(None)`. Child cleanup is bounded; the
    /// settled registry decides local completeness, not remote delivery.
    StateDropped {
        source: &'static str,
        subscribe_end: Option<String>,
    },
}

enum LoopExit {
    Drained,
    StateDropped(&'static str),
}

type ChildOutcome = (TrackRole, Route, anyhow::Result<()>);

/// Record one joined child: an `Err` with no fault yet recorded for its key
/// becomes `task_error`; a panicked join is a namespace fault; a cancelled
/// join was already recorded as `aborted` by the child's guard.
fn record_child_outcome(
    registry: &Arc<Mutex<SubscriptionProducerRegistry>>,
    joined: Result<ChildOutcome, tokio::task::JoinError>,
) -> anyhow::Result<()> {
    let mut registry = registry
        .lock()
        .map_err(|_| anyhow!("S3 producer registry poisoned"))?;
    match joined {
        Ok((_, _, Ok(()))) => {}
        Ok((role, route, Err(error))) => {
            if !registry.has_fault(role, route) {
                let _ = registry.record_fault(role, route, "task_error");
            }
            tracing::warn!(track = role.as_str(), generation = route.generation, error = %format!("{error:#}"), "S3 subscription task error");
        }
        Err(error) if error.is_panic() => registry.record_namespace_fault("task_panic"),
        Err(_) => {}
    }
    Ok(())
}

/// Common cleanup for every error exit of `run_namespace`: abort the remaining
/// children and join them (bounded) so each guard has recorded its outcome
/// before main snapshots the registry. If the bound expires the registry is
/// flagged `children_unsettled` so the verdict can never claim completeness.
async fn abandon_children(
    tasks: &mut JoinSet<ChildOutcome>,
    registry: &Arc<Mutex<SubscriptionProducerRegistry>>,
    bound: Duration,
) {
    tasks.abort_all();
    let deadline = tokio::time::Instant::now() + bound;
    while !tasks.is_empty() {
        match tokio::time::timeout_at(deadline, tasks.join_next()).await {
            Ok(Some(joined)) => {
                // Keep joining even if recording fails (e.g. poisoned registry).
                let _ = record_child_outcome(registry, joined);
            }
            Ok(None) => break,
            Err(_) => {
                if let Ok(mut registry) = registry.lock() {
                    registry.record_namespace_fault("child_join_timeout");
                    registry.mark_children_unsettled();
                }
                break;
            }
        }
    }
}

/// Join every child without aborting (they finish on their own once the run
/// ended or the session went away), refusing new subscriptions meanwhile.
/// On bound expiry fall back to `abandon_children`. A peer error observed on
/// the subscription stream while draining is recorded as a namespace fault
/// (as soon as it is observed) so the verdict fails; its text is
/// kept too. A registry failure while recording routes through
/// `abandon_children` before returning.
/// The ONE refusal every "this run has ended on the sender side" close uses.
///
/// Both call sites mean the same thing to the receiver — the run is over, so
/// this subscription can never be served — and both must therefore leave the
/// same typed code on the wire. Building it here instead of at each site is
/// what makes that checkable: `run_ended_refusal_carries_the_registered_code`
/// asserts the code once and both sites inherit it.
///
/// `ServeError::Closed` carries no reason string (the wire ReasonPhrase is
/// moq-transport's own rendering of the code), so the diagnostic text is
/// emitted locally here. The code, not the text, is the contract.
pub fn run_ended_refusal(context: &str) -> moq_transport::serve::ServeError {
    tracing::warn!(
        context = %context,
        code = crate::S3_RUN_ENDED_REQUEST_ERROR_CODE,
        "S3 subscription refused: the run has already ended"
    );
    moq_transport::serve::ServeError::Closed(crate::S3_RUN_ENDED_REQUEST_ERROR_CODE)
}

/// Write the TX row for ONE run-ended refusal, at the instant of the refusal.
///
/// Both refusal sites call this, so the two are indistinguishable in the log
/// exactly as they are on the wire, except for `cause`.
///
/// A refusal that cannot be logged is never dropped silently: it is recorded
/// as a namespace fault, which `s3_run_verdict` turns into a failed run. That
/// is the convention this file already uses for a fault observed while
/// draining (`drain_subscribe_error`); it does NOT abort the children,
/// because they are completing normally and killing them would destroy
/// evidence of the very run whose log just failed.
fn log_run_ended_refusal(
    logger: &Arc<Mutex<JsonlLogger>>,
    registry: &Arc<Mutex<SubscriptionProducerRegistry>>,
    t_refused: u64,
    name: &str,
    cause: S3RefusalCause,
) {
    let logged = logger
        .lock()
        .map_err(|_| anyhow!("TX logger poisoned"))
        .and_then(|mut logger| {
            logger
                .try_log_s3_producer_refused(t_refused, name, cause)
                .map_err(anyhow::Error::from)
        });
    if let Err(error) = logged {
        let text = format!("refusal_log_write: {error}");
        tracing::error!(error = %format!("{error:#}"), "S3 run-ended refusal could not be logged");
        if let Ok(mut registry) = registry.lock() {
            registry.record_namespace_fault(text);
        }
    }
}

async fn drain_children(
    tasks: &mut JoinSet<ChildOutcome>,
    publish: &moq_transport::session::PublishNamespace,
    registry: &Arc<Mutex<SubscriptionProducerRegistry>>,
    logger: &Arc<Mutex<JsonlLogger>>,
    bound: Duration,
) -> anyhow::Result<Option<String>> {
    drain_children_with(tasks, || publish.subscribed(), registry, logger, bound).await
}

// Injectable subscription source; production uses PublishNamespace::subscribed.
async fn drain_children_with<F, Fut>(
    tasks: &mut JoinSet<ChildOutcome>,
    mut subscribed: F,
    registry: &Arc<Mutex<SubscriptionProducerRegistry>>,
    logger: &Arc<Mutex<JsonlLogger>>,
    bound: Duration,
) -> anyhow::Result<Option<String>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Option<moq_transport::session::Subscribed>, moq_transport::serve::ServeError>>,
{
    let deadline = tokio::time::Instant::now() + bound;
    let mut accepting = true;
    let mut subscribe_end: Option<String> = None;
    let outcome = async {
        loop {
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline) => {
                    if let Ok(mut registry) = registry.lock() {
                        registry.record_namespace_fault("child_drain_timeout");
                        registry.mark_children_unsettled();
                    }
                    abandon_children(tasks, registry, DRAIN_ABANDON_BOUND).await;
                    break;
                }
                subscribed = subscribed(), if accepting => {
                    match subscribed {
                        Ok(Some(subscribed)) => {
                            // Reaching this drain already means the run ended:
                            // it runs only for `LoopExit::Drained` (stop watch)
                            // and `LoopExit::StateDropped` (namespace watch
                            // peer gone). Every error exit goes to
                            // `abandon_children`, which accepts nothing. So the
                            // contract here is identical to the
                            // `current_routes_completed` refusal, and a
                            // SUBSCRIBE that legitimately left the receiver
                            // just before its window end and arrived inside
                            // this drain window must NOT abort the receiver's
                            // run.
                            //
                            // But only for a VALID S3 request. The request
                            // checks of the accept loop are applied first and
                            // unchanged: an unsupported track name still gets
                            // `not_found` (0x10) and an invalid DELIVERY_TIMEOUT
                            // still gets the internal error, both fatal for the
                            // receiver. Otherwise every malformed request that
                            // happened to land inside the drain window would be
                            // absorbed by the receiver's non-fatal path.
                            let name = subscribed.info.track_name.to_string_lossy().into_owned();
                            let validity = classify_subscription_request(
                                &name,
                                subscribed.info.delivery_timeout_ms,
                            );
                            if let Some(refusal) = validity.refusal(&name) {
                                // No `refused` row: the row's `error_code` is
                                // contractually the run-ended code, the accept
                                // loop writes no row for the identical refusal
                                // either, and this refusal is fatal for the
                                // receiver, so it can never be silently
                                // absorbed the way a run-ended refusal can.
                                tracing::warn!(
                                    track = %name,
                                    code = refusal.code(),
                                    "S3 subscription refused as invalid inside the post-run drain window"
                                );
                                let _ = subscribed.close(refusal);
                                continue;
                            }
                            let t_refused = now_us();
                            let _ = subscribed.close(run_ended_refusal(
                                "S3 namespace is draining after run end",
                            ));
                            log_run_ended_refusal(
                                logger,
                                registry,
                                t_refused,
                                &name,
                                S3RefusalCause::NamespaceDrain,
                            );
                        }
                        Ok(None) => {
                            accepting = false;
                            subscribe_end = Some("subscription stream ended".to_string());
                        }
                        Err(error) => {
                            accepting = false;
                            let text = format!("subscription stream error: {error}");
                            registry.lock()
                                .map_err(|_| anyhow!("S3 producer registry poisoned"))?
                                .record_namespace_fault(format!("drain_subscribe_error: {text}"));
                            subscribe_end = Some(text);
                        }
                    }
                }
                joined = tasks.join_next(), if !tasks.is_empty() => {
                    if let Some(joined) = joined {
                        if let Err(error) = record_child_outcome(registry, joined) {
                            return Err(error);
                        }
                    }
                }
                _ = std::future::ready(()), if tasks.is_empty() => break,
            }
        }
        Ok(subscribe_end)
    }
    .await;
    if outcome.is_err() {
        abandon_children(tasks, registry, DRAIN_ABANDON_BOUND).await;
    }
    outcome
}

/// Publish one S3 namespace and own every subscription producer.
///
/// Switch/end boundary: while running, subscriptions are allocated and
/// reserved in one registry critical section. The loop exits are classified
/// HERE, at the moment the future completes, from the close reason
/// moq-transport gives: stop watch -> `Drained`; `closed()`/`subscribed()`
/// `Ok(())`/`Ok(None)` -> `StateDropped` (the namespace watch peer is gone); `Err(ServeError)` from them (REQUEST_ERROR ->
/// `Closed(code)`, PUBLISH_NAMESPACE_CANCEL -> `Cancel`, others) -> `Err`
/// with the original text; child failure -> `Err`. Every exit joins the
/// children first (`drain_children` for the non-error exits,
/// `abandon_children` for every error exit, both bounded).
pub async fn run_namespace(
    publisher: Publisher,
    namespace: TrackNamespace,
    context: Arc<SenderContext>,
    registry: Arc<Mutex<SubscriptionProducerRegistry>>,
    accept_routes: AcceptRouteMap,
    stop: watch::Receiver<bool>,
) -> NamespaceExit {
    let mut observed_at_us = None;
    let end = run_namespace_inner(
        publisher, namespace, context, registry, accept_routes, stop, &mut observed_at_us,
    )
    .await;
    NamespaceExit {
        observed_at_us: observed_at_us.unwrap_or_else(now_us),
        end,
    }
}

async fn run_namespace_inner(
    mut publisher: Publisher,
    namespace: TrackNamespace,
    context: Arc<SenderContext>,
    registry: Arc<Mutex<SubscriptionProducerRegistry>>,
    accept_routes: AcceptRouteMap,
    mut stop: watch::Receiver<bool>,
    observed_at_us: &mut Option<u64>,
) -> anyhow::Result<NamespaceEnd> {
    context.validate()?;
    let publish = publisher
        .publish_namespace_open(namespace)
        .context("open S3 namespace")?;
    publish.ok().await.context("S3 namespace rejected")?;

    let mut allocator = RouteAllocator::default();
    let mut tasks: JoinSet<ChildOutcome> = JoinSet::new();
    let child_join_bound = context.shutdown_timeout.saturating_add(Duration::from_secs(1));
    let loop_exit: anyhow::Result<LoopExit> = async {
        loop {
            tokio::select! {
                subscribed = publish.subscribed() => {
                    let subscribed = match subscribed {
                        Ok(Some(subscribed)) => subscribed,
                        // The peer side of the namespace watch was dropped.
                        Ok(None) => return Ok(LoopExit::StateDropped("subscribed")),
                        // Peer namespace error; ALWAYS a fault downstream.
                        Err(error) => {
                            return Err(anyhow::Error::from(error)
                                .context("S3 namespace peer error (subscribed)"));
                        }
                    };
                let name = subscribed.info.track_name.to_string_lossy().into_owned();
                    // Request validity FIRST, through the same stateless check
                    // the post-run drain window applies. Both refusals keep
                    // the codes they have always had (0x10 / internal), which
                    // stay fatal for the receiver.
                    if let Some(refusal) = classify_subscription_request(
                        &name,
                        subscribed.info.delivery_timeout_ms,
                    )
                    .refusal(&name)
                    {
                        let _ = subscribed.close(refusal);
                        continue;
                    }
                    // Switch/end boundary, ONE registry critical section: the
                    // completion check, the generation allocation, and the
                    // reservation happen under the same lock, so a subscription
                    // can never consume a generation after completion, and a
                    // reserved generation counts toward "latest" until it is
                    // activated or released. The wire code is
                    // `S3_RUN_ENDED_REQUEST_ERROR_CODE` when the refusal is
                    // "this run has ended" and `not_found` (0x10) for every
                    // other refusal in this block (invalid route allocation),
                    // which stays fatal. A run-ended refusal ALSO writes its
                    // own `s3_producer`/`refused` TX row (14th rework); the
                    // fatal ones still write none, exactly like the
                    // request-validity refusals above.
                    let (role, route) = {
                        let mut reg = registry
                            .lock()
                            .map_err(|_| anyhow!("S3 producer registry poisoned"))?;
                        if reg.current_routes_completed() {
                            drop(reg);
                            // "The run has ended" is signalled by its OWN wire
                            // code, not by `DoesNotExist`: 0x10 is also what an
                            // unsupported track name, an invalid route
                            // allocation and moq-transport's own "track not
                            // found" produce, and those must stay fatal for the
                            // receiver. The receiver's non-fatal path keys on
                            // this code alone; it never keys on time, because
                            // the sender finishes at its last slot and so can
                            // legitimately refuse BEFORE the receiver's window
                            // end.
                            // The sender records its OWN refusal, stamped at
                            // the instant it refuses, so the receiver's
                            // non-fatal path is auditable against this row
                            // instead of being inferred from the order of
                            // `producer`/`stop` rows (which is not a sound
                            // inference: the run sequence also ends on the
                            // wall clock).
                            let t_refused = now_us();
                            let _ = subscribed.close(run_ended_refusal(&format!(
                                "S3 subscription '{name}' arrived after run completion"
                            )));
                            log_run_ended_refusal(
                                &context.logger,
                                &registry,
                                t_refused,
                                &name,
                                S3RefusalCause::RunCompleted,
                            );
                            continue;
                        }
                        let (role, route) = match allocator.allocate(&name) {
                            Ok(route) => route,
                            Err(error) => {
                                drop(reg);
                                let _ = subscribed.close(moq_transport::serve::ServeError::not_found_ctx(
                                    format!("invalid S3 subscription: {error}")
                                ));
                                continue;
                            }
                        };
                        reg.reserve(role, route)
                            .map_err(|error| anyhow!("reserve S3 producer generation: {error:?}"))?;
                        (role, route)
                    };
                    // Created synchronously, before the spawn, and moved into the
                    // future: a task that is never polled still records its
                    // abandonment when the future is dropped. Between here and the
                    // spawn there is no fallible statement except the route-map
                    // insert below, which releases the reservation on failure.
                    let guard = ProducerTaskGuard::new(registry.clone(), role, route);

                    if let Err(error) = accept_routes
                        .lock()
                        .map_err(|_| anyhow!("S3 accept route map poisoned"))
                        .map(|mut routes| {
                            routes.insert(subscribed.info.id, AcceptRoute { role, route });
                        })
                    {
                        drop(guard); // records `aborted` fault and Settled
                        // Propagates to the outer wrapper, which runs the common
                        // abort-and-join cleanup before returning.
                        return Err(error);
                    }
                    let context = context.clone();
                    let registry = registry.clone();
                    tasks.spawn(async move {
                        let guard = guard;
                        let logger = context.logger.clone();
                        let producer_context = context.clone();
                        let outcome: anyhow::Result<()> = async {
                            let result = serve_subscription_producer(
                                subscribed,
                                role,
                                route,
                                registry.clone(),
                                context.shutdown_timeout,
                                move |writer, lease| {
                                    let context = producer_context.clone();
                                    async move {
                                        context.logger.lock()
                                            .map_err(|_| anyhow!("TX logger poisoned"))?
                                            .try_log_s3_producer_start(role, route, now_us())?;
                                        match role {
                                            TrackRole::Pc => produce_pc(writer, lease, context).await,
                                            TrackRole::Haptic => produce_haptic(writer, lease, context).await,
                                        }
                                    }
                                },
                            ).await?;
                            // `None`: refused after run completion, a normal outcome.
                            let Some(result) = result else {
                                return Ok(());
                            };
                            let logged = logger
                                .lock()
                                .map_err(|_| anyhow!("TX logger poisoned"))
                                .and_then(|mut logger| {
                                    logger
                                        .try_log_s3_producer_stop(result, now_us())
                                        .map_err(anyhow::Error::from)
                                });
                            if let Err(error) = logged {
                                // Record BEFORE Settled so the verdict sees it.
                                if let Ok(mut reg) = registry.lock() {
                                    let _ = reg.record_fault(role, route, "log_write");
                                }
                                return Err(error);
                            }
                            Ok(())
                        }
                        .await;
                        // LAST statement of the task.
                        guard.settle();
                        (role, route, outcome)
                    });
                }
            joined = tasks.join_next(), if !tasks.is_empty() => {
                    let Some(joined) = joined else { continue };
                    let failed = !matches!(joined, Ok((_, _, Ok(()))));
                    record_child_outcome(&registry, joined)?;
                    if failed {
                        // Fail loud mid-run; the outer wrapper aborts and
                        // joins the remaining children.
                        bail!("S3 subscription task failed");
                    }
                }
                closed = publish.closed() => {
                    match closed {
                        Ok(()) => return Ok(LoopExit::StateDropped("closed")),
                        Err(error) => {
                            return Err(anyhow::Error::from(error)
                                .context("S3 namespace peer error (closed)"));
                        }
                    }
                }
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() {
                        return Ok(LoopExit::Drained);
                    }
                }
            }
        }
    }
    .await;

    // Capture the source observation BEFORE child cleanup can delay return.
    *observed_at_us = Some(now_us());

    // Every exit joins the children before returning, so main's snapshot
    // after joining this task sees every child settled (or the
    // `children_unsettled` flag).
    match loop_exit {
        Ok(LoopExit::Drained) => {
            let subscribe_end =
                drain_children(&mut tasks, &publish, &registry, &context.logger, child_join_bound)
                    .await?;
            Ok(NamespaceEnd::Drained { subscribe_end })
        }
        Ok(LoopExit::StateDropped(source)) => {
            let subscribe_end =
                drain_children(&mut tasks, &publish, &registry, &context.logger, child_join_bound)
                    .await?;
            Ok(NamespaceEnd::StateDropped {
                source,
                subscribe_end,
            })
        }
        Err(error) => {
            abandon_children(&mut tasks, &registry, child_join_bound).await;
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tx_log_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "skew-s3-sender-{name}-{}-{}.jsonl",
            std::process::id(),
            now_us()
        ))
    }

    /// Minimal TX logger, same constructor the production sender uses.
    fn tx_logger(path: &std::path::Path) -> Arc<Mutex<JsonlLogger>> {
        Arc::new(Mutex::new(
            JsonlLogger::new(
                path, "run", "moq", "tx", None, 0.0, 0.0, 0.0, 1, 30, 90, 1, None, None,
                Some("both"), Some(crate::TERM_PROTOCOL_V), None, None, None,
            )
            .unwrap(),
        ))
    }

    /// Rows written by a logger, minus the `meta` first line.
    fn rows(path: &std::path::Path) -> Vec<serde_json::Value> {
        let text = std::fs::read_to_string(path).expect("TX log written");
        text.lines()
            .map(|line| serde_json::from_str(line).unwrap_or_else(|e| panic!("bad row {line}: {e}")))
            .filter(|row: &serde_json::Value| row["role"] != "meta")
            .collect()
    }

    /// Both sender sites that mean "this run has ended" must leave the SAME
    /// typed code on the wire, because the receiver classifies on the code
    /// alone. `drain_children_with`'s refusal arm needs a
    /// `moq_transport::session::Subscribed`, which cannot be constructed
    /// outside moq-transport, so the assertion is made on the single
    /// constructor both arms call.
    #[test]
    fn run_ended_refusal_carries_the_registered_code() {
        let refusal = run_ended_refusal("S3 namespace is draining after run end");
        assert_eq!(
            refusal,
            moq_transport::serve::ServeError::Closed(crate::S3_RUN_ENDED_REQUEST_ERROR_CODE)
        );
        // What actually reaches REQUEST_ERROR.error_code
        // (moq-transport subscribed.rs `request_error_code`).
        assert_eq!(refusal.code(), crate::S3_RUN_ENDED_REQUEST_ERROR_CODE);
        assert_eq!(refusal.code(), 0x5343);
        // NOT DoesNotExist: the 0x10 refusals in this file are real faults.
        assert_ne!(
            refusal.code(),
            u64::from(moq_transport::message::RequestErrorCode::DoesNotExist)
        );
        // The run-completion site builds the identical error.
        assert_eq!(
            run_ended_refusal("S3 subscription 'pc-d6' arrived after run completion"),
            refusal,
            "both run-ended sites must be indistinguishable on the wire"
        );
        // A wire refusal from either site stays non-retryable and typed the
        // same way the receiver's `is_run_ended_refusal` expects.
        assert!(matches!(
            refusal,
            moq_transport::serve::ServeError::Closed(code)
                if code == crate::S3_RUN_ENDED_REQUEST_ERROR_CODE
        ));
    }

    /// The drain window applies the SAME request-validity rule as the accept
    /// loop, because both call this one function. An unsupported name and an
    /// invalid DELIVERY_TIMEOUT keep their own fatal codes; only a valid S3
    /// track can reach the run-ended refusal.
    #[test]
    fn request_validity_is_one_rule_for_the_accept_loop_and_the_drain_window() {
        assert_eq!(
            classify_subscription_request(PC_NORMAL_TRACK, None),
            RequestValidity::Valid(TrackRole::Pc)
        );
        assert_eq!(
            classify_subscription_request(PC_NORMAL_TRACK, Some(67)),
            RequestValidity::Valid(TrackRole::Pc)
        );
        assert_eq!(
            classify_subscription_request(HAPTIC_FULL_TRACK, None),
            RequestValidity::Valid(TrackRole::Haptic)
        );
        // Every S3 track name is valid as a REQUEST; the ordering rule that
        // makes `pc-d7` illegal as a FIRST subscription belongs to the
        // allocator's state, not to the request.
        assert_eq!(
            classify_subscription_request(PC_RECOVERY_TRACK, None),
            RequestValidity::Valid(TrackRole::Pc)
        );
        assert_eq!(
            classify_subscription_request(HAPTIC_ESSENTIAL_TRACK, None),
            RequestValidity::Valid(TrackRole::Haptic)
        );
        assert_eq!(
            classify_subscription_request("pc-d99", None),
            RequestValidity::UnsupportedTrack
        );
        assert!(matches!(
            classify_subscription_request(PC_NORMAL_TRACK, Some(100)),
            RequestValidity::BadDeliveryTimeout(_)
        ));
        assert!(matches!(
            classify_subscription_request(HAPTIC_FULL_TRACK, Some(67)),
            RequestValidity::BadDeliveryTimeout(_)
        ));
    }

    /// An invalid request keeps the accept loop's fatal answers — the
    /// `NotFound` family for an unsupported name, the `Internal` family for a
    /// bad parameter set — and NEVER the run-ended code, wherever it is
    /// refused.
    ///
    /// The assertion is on the VARIANT, because that is what decides the wire
    /// code: `Subscribed::close` runs the error through
    /// `Subscribed::request_error_code`
    /// (`moq-transport/src/session/subscribed.rs:470`), which maps
    /// `NotFound`/`NotFoundWithId` to `DoesNotExist` (0x10) and
    /// `Internal*` to `InternalError` (0x0), and passes only `Closed(code)`
    /// through unchanged. `ServeError::code()` is a different, local mapping
    /// (it answers 0x4 for `NotFound`) and is not what reaches the peer.
    #[test]
    fn an_invalid_request_never_gets_the_run_ended_code() {
        use moq_transport::serve::ServeError;
        let unsupported = classify_subscription_request("pc-d99", None)
            .refusal("pc-d99")
            .expect("an unsupported track is refused");
        assert!(
            matches!(
                unsupported,
                ServeError::NotFound | ServeError::NotFoundWithId(..)
            ),
            "unsupported track must stay in the NotFound family: {unsupported:?}"
        );
        assert!(
            !matches!(unsupported, ServeError::Closed(_)),
            "a validity refusal must never be a pass-through Closed(code)"
        );
        let bad_timeout = classify_subscription_request(PC_NORMAL_TRACK, Some(100))
            .refusal(PC_NORMAL_TRACK)
            .expect("a bad DELIVERY_TIMEOUT is refused");
        assert!(
            matches!(
                bad_timeout,
                ServeError::Internal(_) | ServeError::InternalWithId(..)
            ),
            "a bad parameter set must stay in the Internal family: {bad_timeout:?}"
        );
        assert!(!matches!(bad_timeout, ServeError::Closed(_)));
        // The run-ended refusal, by contrast, IS a pass-through `Closed`
        // carrying the registered code — the one answer the receiver treats
        // as non-fatal.
        assert_eq!(
            run_ended_refusal("t"),
            ServeError::Closed(crate::S3_RUN_ENDED_REQUEST_ERROR_CODE)
        );
        // A valid request has no validity refusal at all: only then may the
        // run-ended code be used.
        assert!(classify_subscription_request(PC_NORMAL_TRACK, None)
            .refusal(PC_NORMAL_TRACK)
            .is_none());
    }

    /// The additive TX row both refusal sites write. The root validator is
    /// written against this exact shape, so it is asserted field by field.
    #[test]
    fn a_run_ended_refusal_writes_its_own_tx_row() {
        let path = tx_log_path("refused-row");
        let logger = tx_logger(&path);
        let registry = Arc::new(Mutex::new(SubscriptionProducerRegistry::new()));
        log_run_ended_refusal(&logger, &registry, 1234, "pc-d6", S3RefusalCause::RunCompleted);
        log_run_ended_refusal(
            &logger,
            &registry,
            5678,
            "pc-d99",
            S3RefusalCause::NamespaceDrain,
        );
        let rows = rows(&path);
        if std::env::var_os("SKEW_TEST_DUMP").is_some() {
            eprintln!("{}", std::fs::read_to_string(&path).unwrap());
        }
        std::fs::remove_file(&path).ok();
        assert_eq!(rows.len(), 2, "{rows:?}");
        assert_eq!(rows[0]["role"], "s3_producer");
        assert_eq!(rows[0]["event"], "refused");
        assert_eq!(rows[0]["t_refused"], 1234);
        // The RAW requested name, not a normalized role.
        assert_eq!(rows[0]["track"], "pc-d6");
        assert_eq!(rows[0]["cause"], "run_completed");
        assert_eq!(rows[0]["error_code"], 21315);
        assert_eq!(
            rows[0]["error_code"].as_u64().unwrap(),
            crate::S3_RUN_ENDED_REQUEST_ERROR_CODE
        );
        assert_eq!(rows[1]["cause"], "namespace_drain");
        assert_eq!(rows[1]["t_refused"], 5678);
        assert_eq!(rows[1]["track"], "pc-d99");
        assert_eq!(rows[1]["error_code"], 21315);
        // Logging a refusal is not itself a fault.
        assert!(registry
            .lock()
            .unwrap()
            .snapshot()
            .namespace_faults
            .is_empty());
    }

    /// A peer-supplied name is escaped and bounded, so one absurd request can
    /// neither corrupt the JSONL nor write an unbounded row.
    #[test]
    fn a_hostile_track_name_still_produces_one_parseable_row() {
        let path = tx_log_path("refused-hostile");
        let logger = tx_logger(&path);
        let registry = Arc::new(Mutex::new(SubscriptionProducerRegistry::new()));
        let hostile = format!("pc\"-\\-\n-\u{7f}-{}", "x".repeat(1024));
        log_run_ended_refusal(
            &logger,
            &registry,
            7,
            &hostile,
            S3RefusalCause::NamespaceDrain,
        );
        let rows = rows(&path);
        std::fs::remove_file(&path).ok();
        assert_eq!(rows.len(), 1, "one row, still parseable: {rows:?}");
        let track = rows[0]["track"].as_str().expect("a string track");
        assert!(track.starts_with("pc\"-\\-\n-\u{7f}-x"), "{track}");
        assert_eq!(track.chars().count(), 256, "bounded at 256 characters");
        assert_eq!(rows[0]["cause"], "namespace_drain");
    }

    /// A refusal whose row cannot be written is never dropped silently: it
    /// becomes a namespace fault, and `s3_run_verdict` fails on that.
    #[test]
    fn a_refusal_that_cannot_be_logged_becomes_a_namespace_fault() {
        let path = tx_log_path("refused-poisoned");
        let logger = tx_logger(&path);
        let registry = Arc::new(Mutex::new(SubscriptionProducerRegistry::new()));
        // Poison the logger mutex the same way a panicking writer would.
        let poisoner = Arc::clone(&logger);
        std::thread::spawn(move || {
            let _guard = poisoner.lock().unwrap();
            panic!("poison the TX logger");
        })
        .join()
        .expect_err("the poisoning thread panics");
        log_run_ended_refusal(&logger, &registry, 9, "pc-d6", S3RefusalCause::RunCompleted);
        std::fs::remove_file(&path).ok();
        let faults = registry.lock().unwrap().snapshot().namespace_faults;
        assert_eq!(faults.len(), 1, "{faults:?}");
        assert!(faults[0].starts_with("refusal_log_write:"), "{faults:?}");
    }

    #[tokio::test]
    async fn drain_records_peer_error_even_when_last_child_is_already_done() {
        let registry = Arc::new(Mutex::new(SubscriptionProducerRegistry::new()));
        let mut tasks = JoinSet::new();
        // No children remain: an already-ready namespace error must still be
        // observed before returning Drained.
        let path = tx_log_path("drain-peer-error");
        let logger = tx_logger(&path);
        let text = drain_children_with(&mut tasks,
            || std::future::ready(Err(moq_transport::serve::ServeError::Closed(4))),
            &registry, &logger, Duration::from_secs(1)).await.unwrap();
        std::fs::remove_file(&path).ok();
        assert!(text.unwrap().contains("subscription stream error"));
        assert!(registry.lock().unwrap().snapshot().namespace_faults.iter()
            .any(|s| s.contains("drain_subscribe_error")));
    }

    #[tokio::test]
    async fn drain_timeout_joins_aborted_children_and_keeps_timeout_fault() {
        let registry = Arc::new(Mutex::new(SubscriptionProducerRegistry::new()));
        let mut tasks = JoinSet::new();
        tasks.spawn(std::future::pending::<ChildOutcome>());
        let path = tx_log_path("drain-timeout");
        let logger = tx_logger(&path);
        drain_children_with(&mut tasks, std::future::pending,
            &registry, &logger, Duration::ZERO).await.unwrap();
        std::fs::remove_file(&path).ok();
        assert!(tasks.is_empty(), "aborted children must be joined");
        let snapshot = registry.lock().unwrap().snapshot();
        assert!(snapshot.children_unsettled);
        assert!(snapshot.namespace_faults.iter().any(|s| s == "child_drain_timeout"));
    }

    #[test]
    fn route_allocator_requires_s2_identical_normal_start_and_never_reuses_generation() {
        let mut allocator = RouteAllocator::default();
        assert!(allocator.allocate(PC_RECOVERY_TRACK).is_err());
        assert_eq!(
            allocator.allocate(PC_NORMAL_TRACK).unwrap(),
            (
                TrackRole::Pc,
                Route {
                    name: PC_NORMAL_TRACK,
                    generation: 0,
                }
            )
        );
        assert_eq!(
            allocator
                .allocate(PC_HAPTIC_CRITICAL_TRACK)
                .unwrap()
                .1
                .generation,
            1
        );
        assert!(allocator.allocate(HAPTIC_ESSENTIAL_TRACK).is_err());
        assert_eq!(
            allocator.allocate(HAPTIC_FULL_TRACK).unwrap().1.generation,
            0
        );
        assert_eq!(
            allocator
                .allocate(HAPTIC_ESSENTIAL_TRACK)
                .unwrap()
                .1
                .generation,
            1
        );
    }

    #[test]
    fn initial_routes_start_at_zero_but_switches_catch_up() {
        let registered = SourceSchedule {
            warmup_start_us: Some(1_000_000),
            measurement_start_us: 4_000_000,
            end_us: 14_000_000,
        };
        assert_eq!(
            initial_measurement_slot(registered, 0, 4_000_001, 30).unwrap(),
            0
        );
        assert_eq!(
            initial_measurement_slot(registered, 0, 4_000_001, 90).unwrap(),
            0
        );
        assert_eq!(
            initial_measurement_slot(registered, 1, 4_000_001, 30).unwrap(),
            1
        );

        let legacy = SourceSchedule {
            warmup_start_us: None,
            measurement_start_us: 4_000_000,
            end_us: 14_000_000,
        };
        assert_eq!(
            initial_measurement_slot(legacy, 0, 4_000_001, 30).unwrap(),
            0
        );
        assert_eq!(
            initial_measurement_slot(legacy, 1, 4_000_001, 30).unwrap(),
            1
        );
        assert_eq!(
            initial_measurement_slot(legacy, 1, 4_000_001, 90).unwrap(),
            1
        );
    }

    #[test]
    fn legacy_generation_zero_observed_late_still_starts_at_zero() {
        // The legacy schedule (no registered warmup pass) is the path the
        // stage-9 harness uses (`--warmup 1`, no `--warmup-pass`). The producer
        // wakes a few us after the measurement anchor; it must still emit
        // measurement slot 0 on both tracks, like the static S1/S2 sender.
        let legacy = SourceSchedule {
            warmup_start_us: None,
            measurement_start_us: 4_000_000,
            end_us: 34_000_000,
        };
        for late_us in [1, 5, 50, 500] {
            let observed = legacy.measurement_start_us + late_us;
            assert_eq!(initial_measurement_slot(legacy, 0, observed, 30).unwrap(), 0);
            assert_eq!(initial_measurement_slot(legacy, 0, observed, 90).unwrap(), 0);
        }
        // Exactly on the anchor is also slot 0.
        assert_eq!(
            initial_measurement_slot(legacy, 0, legacy.measurement_start_us, 30).unwrap(),
            0
        );
    }

    #[test]
    fn full_haptic_exact_anchors_use_the_v5_one_to_three_rule() {
        for frame in 0..300 {
            let tick = frame * 3;
            assert_eq!(exact_frame_for_tick(tick, 30, 90), Some(frame));
        }
        assert_eq!(exact_frame_for_tick(1, 30, 90), None);
        assert_eq!(exact_frame_for_tick(3, 30, 100), None);
    }

    #[test]
    fn route_specs_preserve_frozen_names_tiers_and_roles() {
        for (name, tier) in [
            (PC_NORMAL_TRACK, 2),
            (PC_RECOVERY_TRACK, 3),
            (PC_HAPTIC_CRITICAL_TRACK, 4),
        ] {
            assert_eq!(role_for_track(name), Some(TrackRole::Pc));
            assert_eq!(pc_tier(name), Some(tier));
        }
        assert_eq!(role_for_track(HAPTIC_FULL_TRACK), Some(TrackRole::Haptic));
        assert_eq!(
            role_for_track(HAPTIC_ESSENTIAL_TRACK),
            Some(TrackRole::Haptic)
        );
    }
}
