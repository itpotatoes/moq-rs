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

use crate::s3_producer::{
    serve_subscription_producer, ProducerLease, RunSlotClock, SlotError,
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

/// Publish one S3 namespace and own every subscription producer until the
/// namespace task is aborted/joined by the sender finalizer.
pub async fn run_namespace(
    mut publisher: Publisher,
    namespace: TrackNamespace,
    context: Arc<SenderContext>,
    registry: Arc<Mutex<SubscriptionProducerRegistry>>,
    accept_routes: AcceptRouteMap,
) -> anyhow::Result<()> {
    context.validate()?;
    let publish = publisher
        .publish_namespace_open(namespace)
        .context("open S3 namespace")?;
    publish.ok().await.context("S3 namespace rejected")?;

    let mut allocator = RouteAllocator::default();
    let mut tasks = JoinSet::new();
    loop {
        tokio::select! {
            subscribed = publish.subscribed() => {
                let Some(subscribed) = subscribed.context("receive S3 subscription")? else {
                    bail!("S3 namespace closed while waiting for subscriptions");
                };
                let name = subscribed.info.track_name.to_string_lossy().into_owned();
                let Some(role) = role_for_track(&name) else {
                    let _ = subscribed.close(moq_transport::serve::ServeError::not_found_ctx(
                        format!("unsupported S3 subscription track '{name}'"),
                    ));
                    continue;
                };
                // DELIVERY_TIMEOUT is hop-local. The receiver's 67ms request
                // is enforced by the relay on relay→receiver forwarding and
                // need not be repeated on relay→publisher. If a direct peer
                // does send it here, only the frozen value is accepted.
                if role == TrackRole::Pc
                    && subscribed.info.delivery_timeout_ms.is_some()
                    && subscribed.info.delivery_timeout_ms != Some(67)
                {
                    let _ = subscribed.close(moq_transport::serve::ServeError::internal_ctx(
                        "S3 PC subscription carried a non-67ms DELIVERY_TIMEOUT",
                    ));
                    continue;
                }
                if role == TrackRole::Haptic && subscribed.info.delivery_timeout_ms.is_some() {
                    let _ = subscribed.close(moq_transport::serve::ServeError::internal_ctx(
                        "S3 haptic subscription must not carry DELIVERY_TIMEOUT",
                    ));
                    continue;
                }
                // Switch/end boundary: once both current routes completed the
                // run, a late subscription is refused here without consuming a
                // generation, a producer, or a log row (the registry would also
                // refuse activation with `RunCompleted`).
                let completed = registry
                    .lock()
                    .map_err(|_| anyhow!("S3 producer registry poisoned"))?
                    .current_routes_completed();
                if completed {
                    let _ = subscribed.close(moq_transport::serve::ServeError::not_found_ctx(
                        format!("S3 subscription '{name}' arrived after run completion"),
                    ));
                    continue;
                }
                let (role, route) = match allocator.allocate(&name) {
                    Ok(route) => route,
                    Err(error) => {
                        let _ = subscribed.close(moq_transport::serve::ServeError::not_found_ctx(
                            format!("invalid S3 subscription: {error}")
                        ));
                        continue;
                    }
                };

                accept_routes
                    .lock()
                    .map_err(|_| anyhow!("S3 accept route map poisoned"))?
                    .insert(subscribed.info.id, AcceptRoute { role, route });
                let context = context.clone();
                let registry = registry.clone();
                tasks.spawn(async move {
                    let logger = context.logger.clone();
                    let producer_context = context.clone();
                    let result = serve_subscription_producer(
                        subscribed,
                        role,
                        route,
                        registry,
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
                    logger.lock()
                        .map_err(|_| anyhow!("TX logger poisoned"))?
                        .try_log_s3_producer_stop(result, now_us())?;
                    Ok::<(), anyhow::Error>(())
                });
            }
            joined = tasks.join_next(), if !tasks.is_empty() => {
                match joined {
                    Some(Ok(Ok(()))) => {}
                    Some(Ok(Err(error))) => return Err(error).context("S3 subscription task"),
                    Some(Err(error)) => return Err(error).context("join S3 subscription task"),
                    None => {}
                }
            }
            closed = publish.closed() => {
                closed.context("S3 namespace closed")?;
                bail!("S3 namespace ended unexpectedly");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
