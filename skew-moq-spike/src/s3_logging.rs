//! Append-only JSONL instrumentation for Phase-4 S3.
//!
//! The stable `track` key always denotes the semantic modality (`pc` or
//! `haptic`). The actual MoQ subscription name and its incarnation are carried
//! separately as `wire_track` and `route_generation`. Existing B1/S1/M1/S2
//! logger methods are deliberately untouched.

use std::io::{Error, ErrorKind, Result, Write};

use crate::s3_controller::{S3Controller, S3Snapshot, S3Transition};
use crate::s3_producer::{SubscriptionTaskEnd, SubscriptionTaskResult};
use crate::s3_switch::{
    Route, Routes, S3SwitchGate, SwitchApplied, SwitchRequest, TrackRole, HAPTIC_ESSENTIAL_TRACK,
    HAPTIC_FULL_TRACK, PC_HAPTIC_CRITICAL_TRACK, PC_NORMAL_TRACK, PC_RECOVERY_TRACK,
};
use crate::{esc, JsonlLogger};

fn invalid(message: &'static str) -> Error {
    Error::new(ErrorKind::InvalidInput, message)
}

fn validate_route(role: TrackRole, route: Route) -> Result<()> {
    let valid = match role {
        TrackRole::Pc => matches!(
            route.name,
            PC_NORMAL_TRACK | PC_RECOVERY_TRACK | PC_HAPTIC_CRITICAL_TRACK
        ),
        TrackRole::Haptic => {
            matches!(route.name, HAPTIC_FULL_TRACK | HAPTIC_ESSENTIAL_TRACK)
        }
    };
    if valid {
        Ok(())
    } else {
        Err(invalid("wire track does not match semantic track"))
    }
}

fn object_fields(object: Option<(u64, u64, u64)>) -> String {
    match object {
        Some((group, subgroup, object)) => {
            format!(",\"group_id\":{group},\"subgroup_id\":{subgroup},\"object_id\":{object}")
        }
        None => String::new(),
    }
}

fn route_fields(route: Route) -> String {
    format!(
        "\"wire_track\":\"{}\",\"route_generation\":{}",
        esc(route.name),
        route.generation
    )
}

fn routes_fields(routes: Routes) -> String {
    format!(
        "\"pc_wire_track\":\"{}\",\"pc_route_generation\":{},\"haptic_wire_track\":\"{}\",\"haptic_route_generation\":{}",
        esc(routes.pc.name),
        routes.pc.generation,
        esc(routes.haptic.name),
        routes.haptic.generation
    )
}

fn bool_json(value: bool) -> &'static str {
    if value {
        "true"
    } else {
        "false"
    }
}

fn producer_end(end: SubscriptionTaskEnd) -> &'static str {
    match end {
        SubscriptionTaskEnd::RemoteClosed => "remote_closed",
        SubscriptionTaskEnd::ProducerFinished => "producer_finished",
    }
}

impl JsonlLogger {
    /// Record the complete S3 controller and switch configuration once per run.
    ///
    /// This is a separate role rather than a change to `role:"meta"`, so old
    /// metadata and constructor call sites remain byte-identical.
    pub fn try_log_s3_config(
        &mut self,
        controller: &S3Controller,
        gate: &S3SwitchGate,
    ) -> Result<()> {
        let controller = controller.config();
        let switch = gate.config();
        controller
            .validate()
            .map_err(|_| invalid("invalid S3 controller config"))?;
        switch
            .validate()
            .map_err(|_| invalid("invalid S3 switch config"))?;
        if gate.applied_state().as_str() != "Normal" {
            return Err(invalid("S3 must log its configuration before a switch"));
        }
        let initial = gate.active_routes();
        validate_route(TrackRole::Pc, initial.pc)?;
        validate_route(TrackRole::Haptic, initial.haptic)?;
        writeln!(
            self.w,
            "{{\"role\":\"s3_config\",\"window_us\":{},\"ewma_alpha\":{},\"miss_streak_threshold\":{},\"violation_ratio_threshold\":{},\"target_skew_us\":{},\"recovery_fraction\":{},\"haptic_critical_stable_us\":{},\"recovery_stable_us\":{},\"cooldown_us\":{},\"min_paired_samples\":{},\"max_window_samples\":{},\"effect_timeout_us\":{},\"initial_state\":\"{}\",{}}}",
            controller.window_us,
            controller.ewma_alpha,
            controller.miss_streak_threshold,
            controller.violation_ratio_threshold,
            controller.target_skew_us,
            controller.recovery_fraction,
            controller.haptic_critical_stable_us,
            controller.recovery_stable_us,
            controller.cooldown_us,
            controller.min_paired_samples,
            controller.max_window_samples,
            switch.effect_timeout_us,
            gate.applied_state().as_str(),
            routes_fields(initial),
        )?;
        self.w.flush()
    }

    /// S3 tx row. Every historical tx field keeps its name, order and meaning;
    /// the route identity is appended after the optional MoQ object key.
    #[allow(clippy::too_many_arguments)]
    pub fn try_log_tx_s3(
        &mut self,
        role: TrackRole,
        route: Route,
        tier: u16,
        seq: u32,
        pts_us: u64,
        event_id: u32,
        size: usize,
        t_gen: u64,
        t_send: u64,
        object: Option<(u64, u64, u64)>,
    ) -> Result<()> {
        validate_route(role, route)?;
        let object = object_fields(object);
        writeln!(
            self.w,
            "{{\"role\":\"tx\",\"track\":\"{}\",\"tier\":{tier},\"seq\":{seq},\"pts_us\":{pts_us},\"event_id\":{event_id},\"size\":{size},\"t_gen\":{t_gen},\"t_send\":{t_send}{object},{}}}",
            role.as_str(),
            route_fields(route),
        )?;
        self.w.flush()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_log_rx_s3(
        &mut self,
        role: TrackRole,
        route: Route,
        tier: u16,
        seq: u32,
        pts_us: u64,
        event_id: u32,
        size: u32,
        t_recv: u64,
        t_play: u64,
        t_gen: u64,
    ) -> Result<()> {
        validate_route(role, route)?;
        writeln!(
            self.w,
            "{{\"role\":\"rx\",\"track\":\"{}\",\"tier\":{tier},\"seq\":{seq},\"pts_us\":{pts_us},\"event_id\":{event_id},\"size\":{size},\"t_recv\":{t_recv},\"t_play\":{t_play},\"t_gen\":{t_gen},{}}}",
            role.as_str(),
            route_fields(route),
        )?;
        self.w.flush()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_log_release_s3(
        &mut self,
        role: TrackRole,
        route: Route,
        tier: u16,
        seq: u32,
        pts_us: u64,
        event_id: u32,
        t_release: u64,
    ) -> Result<()> {
        validate_route(role, route)?;
        writeln!(
            self.w,
            "{{\"role\":\"release\",\"track\":\"{}\",\"tier\":{tier},\"seq\":{seq},\"pts_us\":{pts_us},\"event_id\":{event_id},\"t_release\":{t_release},{}}}",
            role.as_str(),
            route_fields(route),
        )?;
        self.w.flush()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_log_drop_s3(
        &mut self,
        role: TrackRole,
        route: Route,
        tier: u16,
        seq: u32,
        pts_us: u64,
        event_id: u32,
        t_drop: u64,
        drop_reason: &str,
    ) -> Result<()> {
        validate_route(role, route)?;
        writeln!(
            self.w,
            "{{\"role\":\"drop\",\"track\":\"{}\",\"tier\":{tier},\"seq\":{seq},\"pts_us\":{pts_us},\"event_id\":{event_id},\"t_drop\":{t_drop},\"drop_reason\":\"{}\",{}}}",
            role.as_str(),
            esc(drop_reason),
            route_fields(route),
        )?;
        self.w.flush()
    }

    /// S3 accept identity is delivery-specific. Keeping semantic `track` while
    /// adding the route generation prevents a resubscription from aliasing a
    /// previous delivery with the same MoQ object triple.
    #[allow(clippy::too_many_arguments)]
    pub fn try_log_accept_s3(
        &mut self,
        role: TrackRole,
        route: Route,
        group_id: u64,
        subgroup_id: u64,
        object_id: u64,
        t_accept: u64,
        size: usize,
    ) -> Result<()> {
        validate_route(role, route)?;
        writeln!(
            self.w,
            "{{\"role\":\"accept\",\"track\":\"{}\",\"group_id\":{group_id},\"subgroup_id\":{subgroup_id},\"object_id\":{object_id},\"t_accept\":{t_accept},\"size\":{size},{}}}",
            role.as_str(),
            route_fields(route),
        )
    }

    /// Controller transition with the trigger snapshot needed to audit the
    /// decision independently from its later on-wire effect.
    pub fn try_log_s3_transition(
        &mut self,
        transition: S3Transition,
        snapshot: S3Snapshot,
    ) -> Result<()> {
        if !snapshot.active {
            return Err(invalid("transition snapshot is not active"));
        }
        if snapshot.state != transition.to {
            return Err(invalid("transition snapshot is not the destination state"));
        }
        if snapshot
            .violation_ratio
            .is_some_and(|value| !value.is_finite() || !(0.0..=1.0).contains(&value))
            || snapshot
                .ewma_abs_skew_us
                .is_some_and(|value| !value.is_finite() || value < 0.0)
        {
            return Err(invalid("transition snapshot contains invalid statistics"));
        }
        let violation_ratio = snapshot
            .violation_ratio
            .map(|value| value.to_string())
            .unwrap_or_else(|| "null".to_string());
        let p95 = snapshot
            .p95_abs_skew_us
            .map(|value| value.to_string())
            .unwrap_or_else(|| "null".to_string());
        let ewma = snapshot
            .ewma_abs_skew_us
            .map(|value| value.to_string())
            .unwrap_or_else(|| "null".to_string());
        writeln!(
            self.w,
            "{{\"role\":\"s3_controller\",\"event\":\"transition\",\"t_decision\":{},\"from_state\":\"{}\",\"to_state\":\"{}\",\"cause\":\"{}\",\"window_samples\":{},\"paired_samples\":{},\"deadline_misses\":{},\"miss_streak\":{},\"violation_ratio\":{violation_ratio},\"p95_abs_skew_us\":{p95},\"ewma_abs_skew_us\":{ewma}}}",
            transition.at_us,
            transition.from.as_str(),
            transition.to.as_str(),
            transition.cause.as_str(),
            snapshot.window_samples,
            snapshot.paired_samples,
            snapshot.deadline_misses,
            snapshot.miss_streak,
        )?;
        self.w.flush()
    }

    pub fn try_log_s3_switch_request(&mut self, request: SwitchRequest) -> Result<()> {
        validate_route(TrackRole::Pc, request.target.pc)?;
        validate_route(TrackRole::Haptic, request.target.haptic)?;
        if request.decision_at_us > request.request_at_us {
            return Err(invalid("switch decision occurs after request"));
        }
        writeln!(
            self.w,
            "{{\"role\":\"s3_switch\",\"event\":\"request\",\"t_decision\":{},\"t_request\":{},\"from_state\":\"{}\",\"to_state\":\"{}\",\"cause\":\"{}\",{},\"pc_changed\":{},\"haptic_changed\":{}}}",
            request.decision_at_us,
            request.request_at_us,
            request.from.as_str(),
            request.to.as_str(),
            request.cause.as_str(),
            routes_fields(request.target),
            bool_json(request.pc_changed),
            bool_json(request.haptic_changed),
        )?;
        self.w.flush()
    }

    pub fn try_log_s3_subscribe_ok(
        &mut self,
        request: SwitchRequest,
        role: TrackRole,
        t_subscribe_ok: u64,
    ) -> Result<()> {
        let changed = match role {
            TrackRole::Pc => request.pc_changed,
            TrackRole::Haptic => request.haptic_changed,
        };
        if !changed {
            return Err(invalid("SUBSCRIBE_OK recorded for an unchanged route"));
        }
        if t_subscribe_ok < request.request_at_us {
            return Err(invalid("SUBSCRIBE_OK occurs before switch request"));
        }
        let route = request.target.for_role(role);
        validate_route(role, route)?;
        writeln!(
            self.w,
            "{{\"role\":\"s3_switch\",\"event\":\"subscribe_ok\",\"t_subscribe_ok\":{t_subscribe_ok},\"track\":\"{}\",{},\"to_state\":\"{}\"}}",
            role.as_str(),
            route_fields(route),
            request.to.as_str(),
        )?;
        self.w.flush()
    }

    /// `first_effect` and `apply` are separate records even though the gate
    /// intentionally makes them share one exact-pair timestamp.
    pub fn try_log_s3_first_effect(&mut self, applied: SwitchApplied) -> Result<()> {
        validate_applied(applied)?;
        writeln!(
            self.w,
            "{{\"role\":\"s3_switch\",\"event\":\"first_effect\",\"t_first_effect\":{},\"from_state\":\"{}\",\"to_state\":\"{}\",\"cause\":\"{}\",\"pts_us\":{},\"event_id\":{},{}}}",
            applied.effect_at_us,
            applied.from.as_str(),
            applied.to.as_str(),
            applied.cause.as_str(),
            applied.exact_pts_us,
            applied.exact_event_id,
            routes_fields(applied.active),
        )?;
        self.w.flush()
    }

    pub fn try_log_s3_apply(&mut self, applied: SwitchApplied) -> Result<()> {
        validate_applied(applied)?;
        writeln!(
            self.w,
            "{{\"role\":\"s3_switch\",\"event\":\"apply\",\"t_apply\":{},\"t_decision\":{},\"t_request\":{},\"request_to_effect_us\":{},\"from_state\":\"{}\",\"to_state\":\"{}\",\"cause\":\"{}\",\"pts_us\":{},\"event_id\":{},{}}}",
            applied.effect_at_us,
            applied.decision_at_us,
            applied.request_at_us,
            applied.request_to_effect_us,
            applied.from.as_str(),
            applied.to.as_str(),
            applied.cause.as_str(),
            applied.exact_pts_us,
            applied.exact_event_id,
            routes_fields(applied.active),
        )?;
        self.w.flush()
    }

    /// Call only after the gate has made this generation stale and immediately
    /// before cancelling the corresponding subscription handle.
    pub fn try_log_s3_cancel(
        &mut self,
        role: TrackRole,
        route: Route,
        t_cancel: u64,
    ) -> Result<()> {
        validate_route(role, route)?;
        writeln!(
            self.w,
            "{{\"role\":\"s3_switch\",\"event\":\"cancel\",\"t_cancel\":{t_cancel},\"track\":\"{}\",{}}}",
            role.as_str(),
            route_fields(route),
        )?;
        self.w.flush()
    }

    pub fn try_log_s3_producer_start(
        &mut self,
        role: TrackRole,
        route: Route,
        t_start: u64,
    ) -> Result<()> {
        validate_route(role, route)?;
        writeln!(
            self.w,
            "{{\"role\":\"s3_producer\",\"event\":\"start\",\"t_start\":{t_start},\"track\":\"{}\",{}}}",
            role.as_str(),
            route_fields(route),
        )?;
        self.w.flush()
    }

    pub fn try_log_s3_producer_stop(
        &mut self,
        result: SubscriptionTaskResult,
        t_stop: u64,
    ) -> Result<()> {
        validate_route(result.role, result.route)?;
        writeln!(
            self.w,
            "{{\"role\":\"s3_producer\",\"event\":\"stop\",\"t_stop\":{t_stop},\"track\":\"{}\",{},\"objects\":{},\"reason\":\"{}\"}}",
            result.role.as_str(),
            route_fields(result.route),
            result.objects,
            producer_end(result.end),
        )?;
        self.w.flush()
    }
}

fn validate_applied(applied: SwitchApplied) -> Result<()> {
    validate_route(TrackRole::Pc, applied.active.pc)?;
    validate_route(TrackRole::Haptic, applied.active.haptic)?;
    if applied.exact_event_id == 0 {
        return Err(invalid("first effect is not an exact anchor pair"));
    }
    if applied.decision_at_us > applied.request_at_us
        || applied.request_at_us > applied.effect_at_us
        || applied.request_to_effect_us != applied.effect_at_us - applied.request_at_us
    {
        return Err(invalid("inconsistent switch timing"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::playout::{LatePolicy, PlayoutConfig};
    use crate::s3_controller::{S3Config, S3Controller, S3Observation, TransitionCause};
    use crate::s3_switch::{ObjectDisposition, S3SwitchGate, SwitchConfig};
    use crate::{now_us, Phase4TransportMeta, TERM_PROTOCOL_V};

    fn path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "skew-s3-log-{name}-{}-{}.jsonl",
            std::process::id(),
            now_us()
        ))
    }

    fn logger(path: &std::path::Path) -> JsonlLogger {
        JsonlLogger::new(
            path,
            "run",
            "moq",
            "tx",
            None,
            0.0,
            0.0,
            0.0,
            10,
            30,
            100,
            1,
            None,
            None,
            Some("both"),
            Some(TERM_PROTOCOL_V),
            Some(PlayoutConfig {
                d_play_us: 50_000,
                startup_timeout_us: 100_000,
                startup_rearm_limit: 1,
                late_tolerance_us: 10_000,
                max_objects_per_track: 256,
                max_span_us: 2_000_000,
                late_policy: LatePolicy::DropLate,
            }),
            Some(Phase4TransportMeta {
                arm: "s3",
                pc_subgroup_mapping: "frame-per-subgroup",
                pc_publisher_priority: 128,
                haptic_publisher_priority: 128,
                publisher_priority_profile: "equal-128",
                data_priority_mapping: "legacy-v1",
                pc_delivery_timeout_ms: Some(67),
            }),
        )
        .unwrap()
    }

    fn controller_config() -> S3Config {
        S3Config {
            window_us: 1_000_000,
            ewma_alpha: 0.2,
            miss_streak_threshold: 3,
            violation_ratio_threshold: 0.2,
            target_skew_us: 25_000,
            recovery_fraction: 0.5,
            haptic_critical_stable_us: 200_000,
            recovery_stable_us: 300_000,
            cooldown_us: 500_000,
            min_paired_samples: 2,
            max_window_samples: 64,
        }
    }

    #[test]
    fn s3_object_rows_keep_semantic_track_and_append_route_identity() {
        let path = path("objects");
        {
            let mut log = logger(&path);
            let pc = Route {
                name: PC_HAPTIC_CRITICAL_TRACK,
                generation: 7,
            };
            log.try_log_tx_s3(
                TrackRole::Pc,
                pc,
                4,
                8,
                9,
                10,
                11,
                12,
                13,
                Some((14, 15, 16)),
            )
            .unwrap();
            log.try_log_release_s3(TrackRole::Pc, pc, 4, 8, 9, 10, 17)
                .unwrap();
            log.try_log_accept_s3(TrackRole::Pc, pc, 14, 15, 16, 18, 43)
                .unwrap();
            log.try_flush().unwrap();
        }
        let lines: Vec<_> = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect();
        assert_eq!(
            lines[1],
            "{\"role\":\"tx\",\"track\":\"pc\",\"tier\":4,\"seq\":8,\"pts_us\":9,\"event_id\":10,\"size\":11,\"t_gen\":12,\"t_send\":13,\"group_id\":14,\"subgroup_id\":15,\"object_id\":16,\"wire_track\":\"pc-d6\",\"route_generation\":7}"
        );
        assert!(lines[2].contains(
            "\"role\":\"release\",\"track\":\"pc\",\"tier\":4,\"seq\":8,\"pts_us\":9,\"event_id\":10"
        ));
        assert!(lines[2].ends_with("\"wire_track\":\"pc-d6\",\"route_generation\":7}"));
        assert!(lines[3].contains("\"role\":\"accept\",\"track\":\"pc\""));
        assert!(lines[3].ends_with("\"wire_track\":\"pc-d6\",\"route_generation\":7}"));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn switch_lifecycle_is_explicit_and_timing_consistent() {
        let path = path("switch");
        {
            let config = controller_config();
            let mut controller = S3Controller::new(config).unwrap();
            controller.activate(0).unwrap();
            controller
                .observe(S3Observation {
                    now_us: 1,
                    deadline_miss: true,
                    abs_skew_us: None,
                })
                .unwrap();
            controller
                .observe(S3Observation {
                    now_us: 2,
                    deadline_miss: true,
                    abs_skew_us: None,
                })
                .unwrap();
            let update = controller
                .observe(S3Observation {
                    now_us: 3,
                    deadline_miss: true,
                    abs_skew_us: None,
                })
                .unwrap();
            let transition = update.transition.unwrap();
            assert_eq!(transition.cause, TransitionCause::DeadlineMissStreak);

            let switch_config = SwitchConfig {
                effect_timeout_us: 1_000_000,
            };
            let mut gate = S3SwitchGate::new(switch_config).unwrap();
            let mut log = logger(&path);
            log.try_log_s3_config(&controller, &gate).unwrap();
            let mut inactive = update.snapshot;
            inactive.active = false;
            assert_eq!(
                log.try_log_s3_transition(transition, inactive)
                    .unwrap_err()
                    .kind(),
                ErrorKind::InvalidInput
            );
            log.try_log_s3_transition(transition, update.snapshot)
                .unwrap();
            let request = gate.request(transition, 4).unwrap();
            log.try_log_s3_switch_request(request).unwrap();
            gate.subscribe_ok(TrackRole::Pc, 5).unwrap();
            log.try_log_s3_subscribe_ok(request, TrackRole::Pc, 5)
                .unwrap();
            gate.subscribe_ok(TrackRole::Haptic, 6).unwrap();
            log.try_log_s3_subscribe_ok(request, TrackRole::Haptic, 6)
                .unwrap();
            let applied = gate.exact_pair_first_effect(100, 1, 7).unwrap();
            log.try_log_s3_first_effect(applied).unwrap();
            log.try_log_s3_apply(applied).unwrap();
            assert_eq!(
                gate.classify_object(
                    TrackRole::Pc,
                    applied.cancel_pc.expect("replaced PC").generation
                ),
                ObjectDisposition::StaleDrop
            );
            log.try_log_s3_cancel(TrackRole::Pc, applied.cancel_pc.unwrap(), 8)
                .unwrap();
        }
        let text = std::fs::read_to_string(&path).unwrap();
        for event in [
            "\"role\":\"s3_config\"",
            "\"role\":\"s3_controller\",\"event\":\"transition\"",
            "\"event\":\"request\"",
            "\"event\":\"subscribe_ok\"",
            "\"event\":\"first_effect\"",
            "\"event\":\"apply\"",
            "\"event\":\"cancel\"",
        ] {
            assert!(text.contains(event), "missing {event}: {text}");
        }
        assert!(text.contains("\"request_to_effect_us\":3"));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn rejects_wire_track_semantic_aliasing() {
        let path = path("invalid-route");
        let mut log = logger(&path);
        let wrong = Route {
            name: HAPTIC_FULL_TRACK,
            generation: 1,
        };
        let error = log
            .try_log_s3_producer_start(TrackRole::Pc, wrong, 1)
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        drop(log);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn producer_stop_records_reason_and_object_count() {
        let path = path("producer");
        {
            let mut log = logger(&path);
            let route = Route {
                name: HAPTIC_ESSENTIAL_TRACK,
                generation: 3,
            };
            log.try_log_s3_producer_start(TrackRole::Haptic, route, 10)
                .unwrap();
            log.try_log_s3_producer_stop(
                SubscriptionTaskResult {
                    end: SubscriptionTaskEnd::RemoteClosed,
                    role: TrackRole::Haptic,
                    route,
                    objects: 27,
                },
                20,
            )
            .unwrap();
        }
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains(
            "\"event\":\"stop\",\"t_stop\":20,\"track\":\"haptic\",\"wire_track\":\"haptic-essential\",\"route_generation\":3,\"objects\":27,\"reason\":\"remote_closed\""
        ));
        std::fs::remove_file(path).unwrap();
    }
}
