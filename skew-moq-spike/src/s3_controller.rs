//! Transport-agnostic Phase-4 S3 controller.
//!
//! The primary S3 arm is constructed in Normal/d8/full and remains inactive
//! until the playout scheduler has an exact-pair receiver-monotonic epoch.
//! This module only decides desired state transitions; subscription
//! request/apply/effect handling and stale-object disposal belong to the later
//! wire adapter.

use std::collections::VecDeque;

pub const PC_TIER_NORMAL: u16 = 2; // d8
pub const PC_TIER_RECOVERY: u16 = 3; // d7
pub const PC_TIER_HAPTIC_CRITICAL: u16 = 4; // d6

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HapticMode {
    Full,
    Essential,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3State {
    Normal,
    HapticCritical,
    Recovery,
}

impl S3State {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "Normal",
            Self::HapticCritical => "Haptic-Critical",
            Self::Recovery => "Recovery",
        }
    }

    pub fn pc_tier(self) -> u16 {
        match self {
            Self::Normal => PC_TIER_NORMAL,
            Self::HapticCritical => PC_TIER_HAPTIC_CRITICAL,
            Self::Recovery => PC_TIER_RECOVERY,
        }
    }

    pub fn haptic_mode(self) -> HapticMode {
        match self {
            Self::Normal | Self::Recovery => HapticMode::Full,
            Self::HapticCritical => HapticMode::Essential,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct S3Config {
    pub window_us: u64,
    pub ewma_alpha: f64,
    pub miss_streak_threshold: u32,
    pub violation_ratio_threshold: f64,
    pub target_skew_us: u64,
    pub recovery_fraction: f64,
    pub haptic_critical_stable_us: u64,
    pub recovery_stable_us: u64,
    pub cooldown_us: u64,
    pub min_paired_samples: usize,
    pub max_window_samples: usize,
}

impl S3Config {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.window_us == 0 {
            return Err("window_us must be > 0");
        }
        if !self.ewma_alpha.is_finite() || self.ewma_alpha <= 0.0 || self.ewma_alpha > 1.0 {
            return Err("ewma_alpha must be in (0,1]");
        }
        if self.miss_streak_threshold == 0 {
            return Err("miss_streak_threshold must be > 0");
        }
        if !self.violation_ratio_threshold.is_finite()
            || self.violation_ratio_threshold < 0.0
            || self.violation_ratio_threshold >= 1.0
        {
            return Err("violation_ratio_threshold must be in [0,1)");
        }
        if self.target_skew_us == 0 {
            return Err("target_skew_us must be > 0");
        }
        if !self.recovery_fraction.is_finite()
            || self.recovery_fraction <= 0.0
            || self.recovery_fraction >= 1.0
        {
            return Err("recovery_fraction must be in (0,1)");
        }
        if self.haptic_critical_stable_us == 0 || self.recovery_stable_us == 0 {
            return Err("stable durations must be > 0");
        }
        if self.cooldown_us == 0 {
            return Err("cooldown_us must be > 0");
        }
        if self.min_paired_samples == 0 {
            return Err("min_paired_samples must be > 0");
        }
        if self.max_window_samples < self.min_paired_samples {
            return Err("max_window_samples must cover min_paired_samples");
        }
        Ok(())
    }

    fn recovery_limit_us(&self) -> u64 {
        ((self.target_skew_us as f64) * self.recovery_fraction).floor() as u64
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct S3Observation {
    pub now_us: u64,
    pub deadline_miss: bool,
    /// Exact-pair `|delta_tau_release|`; absent only for a deadline miss.
    pub abs_skew_us: Option<u64>,
}

impl S3Observation {
    fn validate(&self) -> Result<(), &'static str> {
        match (self.deadline_miss, self.abs_skew_us) {
            (true, None) | (false, Some(_)) => Ok(()),
            (true, Some(_)) => Err("deadline miss cannot carry paired skew"),
            (false, None) => Err("paired observation requires abs_skew_us"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct WindowSample {
    now_us: u64,
    deadline_miss: bool,
    abs_skew_us: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransitionCause {
    DeadlineMissStreak,
    ViolationRatio,
    StableRecovery,
    StableNormal,
}

impl TransitionCause {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DeadlineMissStreak => "deadline_miss_streak",
            Self::ViolationRatio => "violation_ratio",
            Self::StableRecovery => "stable_recovery",
            Self::StableNormal => "stable_normal",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct S3Transition {
    pub at_us: u64,
    pub from: S3State,
    pub to: S3State,
    pub cause: TransitionCause,
}

#[derive(Debug, Clone, Copy)]
pub struct S3Snapshot {
    pub active: bool,
    pub state: S3State,
    pub pc_tier: u16,
    pub haptic_mode: HapticMode,
    pub window_samples: usize,
    pub paired_samples: usize,
    pub deadline_misses: usize,
    pub miss_streak: u32,
    pub violation_ratio: Option<f64>,
    pub p95_abs_skew_us: Option<u64>,
    pub ewma_abs_skew_us: Option<f64>,
}

#[derive(Debug, Clone, Copy)]
pub struct S3Update {
    pub transition: Option<S3Transition>,
    pub snapshot: S3Snapshot,
}

pub struct S3Controller {
    config: S3Config,
    active: bool,
    state: S3State,
    window: VecDeque<WindowSample>,
    miss_streak: u32,
    ewma_abs_skew_us: Option<f64>,
    stable_since_us: Option<u64>,
    last_now_us: Option<u64>,
    last_transition_us: Option<u64>,
}

impl S3Controller {
    pub fn new(config: S3Config) -> Result<Self, &'static str> {
        config.validate()?;
        Ok(Self {
            config,
            active: false,
            state: S3State::Normal,
            window: VecDeque::new(),
            miss_streak: 0,
            ewma_abs_skew_us: None,
            stable_since_us: None,
            last_now_us: None,
            last_transition_us: None,
        })
    }

    pub fn config(&self) -> S3Config {
        self.config
    }

    pub fn state(&self) -> S3State {
        self.state
    }

    pub fn is_active(&self) -> bool {
        self.active
    }

    /// Activate once the scheduler has established its exact-pair epoch.
    /// Activation deliberately does not create a transition event.
    pub fn activate(&mut self, now_us: u64) -> Result<S3Snapshot, &'static str> {
        if self.active {
            return Err("controller already active");
        }
        self.check_monotonic(now_us)?;
        self.active = true;
        self.last_now_us = Some(now_us);
        Ok(self.snapshot())
    }

    /// Pre-ACTIVE observations are ignored by design and cannot mutate the
    /// window, miss streak, EWMA, or state.
    pub fn observe(&mut self, observation: S3Observation) -> Result<S3Update, &'static str> {
        observation.validate()?;
        if !self.active {
            return Ok(S3Update {
                transition: None,
                snapshot: self.snapshot(),
            });
        }
        self.check_monotonic(observation.now_us)?;
        self.last_now_us = Some(observation.now_us);
        self.purge_window(observation.now_us);
        self.window.push_back(WindowSample {
            now_us: observation.now_us,
            deadline_miss: observation.deadline_miss,
            abs_skew_us: observation.abs_skew_us,
        });
        while self.window.len() > self.config.max_window_samples {
            self.window.pop_front();
        }

        if observation.deadline_miss {
            self.miss_streak = self.miss_streak.saturating_add(1);
        } else {
            self.miss_streak = 0;
            let sample = observation
                .abs_skew_us
                .expect("validated paired observation") as f64;
            self.ewma_abs_skew_us = Some(match self.ewma_abs_skew_us {
                Some(previous) => {
                    self.config.ewma_alpha * sample + (1.0 - self.config.ewma_alpha) * previous
                }
                None => sample,
            });
        }

        let transition = self.evaluate(observation.now_us);
        Ok(S3Update {
            transition,
            snapshot: self.snapshot(),
        })
    }

    pub fn snapshot(&self) -> S3Snapshot {
        let paired: Vec<u64> = self
            .window
            .iter()
            .filter_map(|sample| sample.abs_skew_us)
            .collect();
        let violations = paired
            .iter()
            .filter(|&&skew| skew > self.config.target_skew_us)
            .count();
        S3Snapshot {
            active: self.active,
            state: self.state,
            pc_tier: self.state.pc_tier(),
            haptic_mode: self.state.haptic_mode(),
            window_samples: self.window.len(),
            paired_samples: paired.len(),
            deadline_misses: self
                .window
                .iter()
                .filter(|sample| sample.deadline_miss)
                .count(),
            miss_streak: self.miss_streak,
            violation_ratio: (!paired.is_empty())
                .then_some(violations as f64 / paired.len() as f64),
            p95_abs_skew_us: percentile_95(paired),
            ewma_abs_skew_us: self.ewma_abs_skew_us,
        }
    }

    fn check_monotonic(&self, now_us: u64) -> Result<(), &'static str> {
        if self.last_now_us.is_some_and(|last| now_us < last) {
            return Err("receiver monotonic time moved backwards");
        }
        Ok(())
    }

    fn purge_window(&mut self, now_us: u64) {
        let cutoff = now_us.saturating_sub(self.config.window_us);
        while self
            .window
            .front()
            .is_some_and(|sample| sample.now_us < cutoff)
        {
            self.window.pop_front();
        }
    }

    fn cooldown_complete(&self, now_us: u64) -> bool {
        self.last_transition_us
            .is_none_or(|last| now_us.saturating_sub(last) >= self.config.cooldown_us)
    }

    fn degradation_cause(&self, snapshot: &S3Snapshot) -> Option<TransitionCause> {
        if snapshot.miss_streak >= self.config.miss_streak_threshold {
            return Some(TransitionCause::DeadlineMissStreak);
        }
        if snapshot.paired_samples >= self.config.min_paired_samples
            && snapshot
                .violation_ratio
                .is_some_and(|ratio| ratio > self.config.violation_ratio_threshold)
        {
            return Some(TransitionCause::ViolationRatio);
        }
        None
    }

    fn is_stable(&self, snapshot: &S3Snapshot) -> bool {
        snapshot.deadline_misses == 0
            && snapshot.paired_samples >= self.config.min_paired_samples
            && snapshot
                .p95_abs_skew_us
                .is_some_and(|p95| p95 < self.config.recovery_limit_us())
    }

    fn evaluate(&mut self, now_us: u64) -> Option<S3Transition> {
        let snapshot = self.snapshot();
        match self.state {
            S3State::Normal => {
                self.stable_since_us = None;
                let cause = self.degradation_cause(&snapshot)?;
                self.cooldown_complete(now_us)
                    .then(|| self.transition(now_us, S3State::HapticCritical, cause))
            }
            S3State::HapticCritical => {
                if !self.is_stable(&snapshot) {
                    self.stable_since_us = None;
                    return None;
                }
                let stable_since = *self.stable_since_us.get_or_insert(now_us);
                (now_us.saturating_sub(stable_since) >= self.config.haptic_critical_stable_us
                    && self.cooldown_complete(now_us))
                .then(|| {
                    self.transition(now_us, S3State::Recovery, TransitionCause::StableRecovery)
                })
            }
            S3State::Recovery => {
                if let Some(cause) = self.degradation_cause(&snapshot) {
                    self.stable_since_us = None;
                    return self
                        .cooldown_complete(now_us)
                        .then(|| self.transition(now_us, S3State::HapticCritical, cause));
                }
                if !self.is_stable(&snapshot) {
                    self.stable_since_us = None;
                    return None;
                }
                let stable_since = *self.stable_since_us.get_or_insert(now_us);
                (now_us.saturating_sub(stable_since) >= self.config.recovery_stable_us
                    && self.cooldown_complete(now_us))
                .then(|| self.transition(now_us, S3State::Normal, TransitionCause::StableNormal))
            }
        }
    }

    fn transition(&mut self, at_us: u64, to: S3State, cause: TransitionCause) -> S3Transition {
        let from = self.state;
        debug_assert_ne!(from, to);
        self.state = to;
        self.stable_since_us = None;
        self.last_transition_us = Some(at_us);
        S3Transition {
            at_us,
            from,
            to,
            cause,
        }
    }
}

fn percentile_95(mut values: Vec<u64>) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    let rank = (95usize.saturating_mul(values.len()).saturating_add(99)) / 100;
    Some(values[rank.saturating_sub(1).min(values.len() - 1)])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> S3Config {
        S3Config {
            window_us: 1_000,
            ewma_alpha: 0.2,
            miss_streak_threshold: 3,
            violation_ratio_threshold: 0.2,
            target_skew_us: 100,
            recovery_fraction: 0.5,
            haptic_critical_stable_us: 300,
            recovery_stable_us: 500,
            cooldown_us: 200,
            min_paired_samples: 5,
            max_window_samples: 8,
        }
    }

    fn paired(now_us: u64, abs_skew_us: u64) -> S3Observation {
        S3Observation {
            now_us,
            deadline_miss: false,
            abs_skew_us: Some(abs_skew_us),
        }
    }

    fn missed(now_us: u64) -> S3Observation {
        S3Observation {
            now_us,
            deadline_miss: true,
            abs_skew_us: None,
        }
    }

    fn active() -> S3Controller {
        let mut controller = S3Controller::new(config()).unwrap();
        controller.activate(0).unwrap();
        controller
    }

    fn enter_haptic_critical(controller: &mut S3Controller, start: u64) -> u64 {
        assert!(controller
            .observe(missed(start))
            .unwrap()
            .transition
            .is_none());
        assert!(controller
            .observe(missed(start + 1))
            .unwrap()
            .transition
            .is_none());
        let update = controller.observe(missed(start + 2)).unwrap();
        assert_eq!(
            update.transition.unwrap().cause,
            TransitionCause::DeadlineMissStreak
        );
        assert_eq!(controller.state(), S3State::HapticCritical);
        start + 2
    }

    fn fill_stable(controller: &mut S3Controller, start: u64) -> u64 {
        let mut now = start;
        for _ in 0..5 {
            controller.observe(paired(now, 10)).unwrap();
            now += 1;
        }
        now - 1
    }

    #[test]
    fn starts_normal_d8_full_but_inactive() {
        let controller = S3Controller::new(config()).unwrap();
        let snapshot = controller.snapshot();
        assert!(!snapshot.active);
        assert_eq!(snapshot.state, S3State::Normal);
        assert_eq!(snapshot.pc_tier, 2);
        assert_eq!(snapshot.haptic_mode, HapticMode::Full);
    }

    #[test]
    fn pre_active_observations_cannot_change_state_or_window() {
        let mut controller = S3Controller::new(config()).unwrap();
        for now in 0..10 {
            assert!(controller
                .observe(missed(now))
                .unwrap()
                .transition
                .is_none());
        }
        let snapshot = controller.snapshot();
        assert_eq!(snapshot.state, S3State::Normal);
        assert_eq!(snapshot.window_samples, 0);
        assert_eq!(snapshot.miss_streak, 0);
    }

    #[test]
    fn activation_preserves_normal_and_is_single_use() {
        let mut controller = S3Controller::new(config()).unwrap();
        let snapshot = controller.activate(10).unwrap();
        assert!(snapshot.active);
        assert_eq!(snapshot.state, S3State::Normal);
        assert!(controller.activate(11).is_err());
    }

    #[test]
    fn third_consecutive_miss_enters_haptic_critical() {
        let mut controller = active();
        enter_haptic_critical(&mut controller, 1);
        assert_eq!(controller.snapshot().pc_tier, 4);
        assert_eq!(controller.snapshot().haptic_mode, HapticMode::Essential);
    }

    #[test]
    fn exact_pair_resets_miss_streak() {
        let mut controller = active();
        controller.observe(missed(1)).unwrap();
        controller.observe(missed(2)).unwrap();
        controller.observe(paired(3, 10)).unwrap();
        controller.observe(missed(4)).unwrap();
        controller.observe(missed(5)).unwrap();
        assert_eq!(controller.state(), S3State::Normal);
        assert_eq!(controller.snapshot().miss_streak, 2);
    }

    #[test]
    fn ratio_is_strictly_greater_than_twenty_percent() {
        let mut controller = active();
        for (now, skew) in [10, 10, 10, 10, 101].into_iter().enumerate() {
            controller.observe(paired(now as u64 + 1, skew)).unwrap();
        }
        assert_eq!(controller.snapshot().violation_ratio, Some(0.2));
        assert_eq!(controller.state(), S3State::Normal);
        let update = controller.observe(paired(6, 101)).unwrap();
        assert_eq!(
            update.transition.unwrap().cause,
            TransitionCause::ViolationRatio
        );
    }

    #[test]
    fn ratio_trigger_requires_minimum_paired_samples() {
        let mut controller = active();
        for now in 1..5 {
            controller.observe(paired(now, 1_000)).unwrap();
        }
        assert_eq!(controller.state(), S3State::Normal);
    }

    #[test]
    fn cooldown_blocks_early_recovery_transition() {
        let mut cfg = config();
        cfg.window_us = 100;
        cfg.haptic_critical_stable_us = 50;
        let mut controller = S3Controller::new(cfg).unwrap();
        controller.activate(0).unwrap();
        let entered = enter_haptic_critical(&mut controller, 1);
        let stable = fill_stable(&mut controller, entered + 101);
        controller.observe(paired(stable + 50, 10)).unwrap();
        assert_eq!(controller.state(), S3State::HapticCritical);
        controller.observe(paired(entered + 200, 10)).unwrap();
        assert_eq!(controller.state(), S3State::Recovery);
    }

    #[test]
    fn stable_haptic_critical_reaches_recovery_after_duration() {
        let mut controller = active();
        let entered = enter_haptic_critical(&mut controller, 1);
        let stable = fill_stable(&mut controller, entered + 1_001);
        assert_eq!(controller.state(), S3State::HapticCritical);
        controller.observe(paired(stable + 299, 10)).unwrap();
        assert_eq!(controller.state(), S3State::HapticCritical);
        controller.observe(paired(stable + 300, 10)).unwrap();
        assert_eq!(controller.state(), S3State::Recovery);
        assert_eq!(controller.snapshot().pc_tier, 3);
        assert_eq!(controller.snapshot().haptic_mode, HapticMode::Full);
    }

    #[test]
    fn miss_resets_stability_timer() {
        let mut controller = active();
        let entered = enter_haptic_critical(&mut controller, 1);
        let stable = fill_stable(&mut controller, entered + 1_001);
        controller.observe(paired(stable + 200, 10)).unwrap();
        controller.observe(missed(stable + 201)).unwrap();
        fill_stable(&mut controller, stable + 1_202);
        controller.observe(paired(stable + 1_400, 10)).unwrap();
        assert_eq!(controller.state(), S3State::HapticCritical);
    }

    #[test]
    fn insufficient_pairs_cannot_recover() {
        let mut controller = active();
        let entered = enter_haptic_critical(&mut controller, 1);
        for now in entered + 1..=entered + 1_100 {
            if now % 400 == 0 {
                controller.observe(paired(now, 10)).unwrap();
            }
        }
        assert_eq!(controller.state(), S3State::HapticCritical);
    }

    #[test]
    fn recovery_reaches_normal_after_configured_stable_duration() {
        let mut cfg = config();
        cfg.window_us = 100;
        cfg.haptic_critical_stable_us = 30;
        cfg.recovery_stable_us = 50;
        cfg.cooldown_us = 20;
        cfg.min_paired_samples = 3;
        let mut controller = S3Controller::new(cfg).unwrap();
        controller.activate(0).unwrap();
        let entered = enter_haptic_critical(&mut controller, 1);
        controller.observe(paired(entered + 101, 10)).unwrap();
        controller.observe(paired(entered + 102, 10)).unwrap();
        controller.observe(paired(entered + 103, 10)).unwrap();
        controller.observe(paired(entered + 133, 10)).unwrap();
        assert_eq!(controller.state(), S3State::Recovery);
        controller.observe(paired(entered + 134, 10)).unwrap();
        controller.observe(paired(entered + 135, 10)).unwrap();
        controller.observe(paired(entered + 184, 10)).unwrap();
        assert_eq!(controller.state(), S3State::Normal);
    }

    #[test]
    fn recovery_degradation_returns_to_haptic_critical() {
        let mut cfg = config();
        cfg.haptic_critical_stable_us = 10;
        cfg.cooldown_us = 5;
        let mut controller = S3Controller::new(cfg).unwrap();
        controller.activate(0).unwrap();
        let entered = enter_haptic_critical(&mut controller, 1);
        let stable = fill_stable(&mut controller, entered + 1_001);
        controller.observe(paired(stable + 10, 10)).unwrap();
        assert_eq!(controller.state(), S3State::Recovery);
        let start = stable + 20;
        controller.observe(missed(start)).unwrap();
        controller.observe(missed(start + 1)).unwrap();
        controller.observe(missed(start + 2)).unwrap();
        assert_eq!(controller.state(), S3State::HapticCritical);
    }

    #[test]
    fn backward_time_is_a_fail_loud_error() {
        let mut controller = active();
        controller.observe(paired(10, 10)).unwrap();
        assert!(controller.observe(paired(9, 10)).is_err());
    }

    #[test]
    fn window_is_bounded_by_sample_count() {
        let mut controller = active();
        for now in 1..=20 {
            controller.observe(paired(now, 10)).unwrap();
        }
        assert_eq!(controller.snapshot().window_samples, 8);
    }

    #[test]
    fn invalid_observation_and_config_fail_loud() {
        let mut controller = active();
        assert!(controller
            .observe(S3Observation {
                now_us: 1,
                deadline_miss: true,
                abs_skew_us: Some(10),
            })
            .is_err());
        let mut bad = config();
        bad.min_paired_samples = 0;
        assert!(S3Controller::new(bad).is_err());
    }
}
