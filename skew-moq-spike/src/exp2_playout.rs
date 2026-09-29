//! Experiment-2 playout scheduler core (WP1): S3NPA', P0-NP and P0.
//!
//! Governing text: `md/20260929_실험2_사전등록_문안.md` (registration; wins on
//! conflict), the controller internals of `md/20260928_제안스케줄러_B_세부사양.md`
//! at commit `e88233c` (§3 estimator, miss step, tick order; §4 initialisation,
//! opportunity/terminal, tie rule, increase caps, commit-at-ref(i), Δ_eff,
//! compression, filler due, logging; §5 release rule), and the WP0 contract
//! `md/20260929_실험2_계약.md` §4 (names of the scheduler-level facts).
//!
//! This module is a **pure state machine**: no I/O, no clock reads, no tasks.
//! It is driven by a trace of timestamped inputs (`arrive`,
//! `delivery_timeout`, `advance_to`, `finalize`) and returns decisions.
//! Every time value is integer microseconds on the shared monotonic clock.
//! Structure follows spec §4 "구현 구조": an event ledger plus one ordered
//! timer set (a single minimum-deadline queue; `next_wakeup_us` is the single
//! wakeup) — there are no per-object timers.
//!
//! Scope boundary (coordinator WP1 adjustment, contract §4/§5): the core emits
//! only scheduler-level facts — immutable `due_us`/`delta_eff_us`/
//! `delta_generation` per opportunity, release/drop actions, the scheduler
//! terminal (`released`, `dropped(reason)`, `pending_at_horizon`) and hold
//! increments `g_m(i)`.  The registration's output-ledger states
//! (`output`, `explicit_discard`, `submitted_unresolved`, `not_submitted`) are
//! produced by WP3/WP6 and judged by WP2; they are not modelled here.
//!
//! Deterministic same-µs order (spec §3 tick order / §4 tie rule):
//!   (0) arrivals with `t_recv <= now`  →  (1) terminals (release at due,
//!   timeout at `c = due + ε_release`)  →  (2) `ref(i)` commit (uses the Δ*
//!   in force before this µs' tick)  →  (3) miss step  →  (4) Q update.
//! The API encodes this: `arrive(t)` drains internal timers strictly before
//! `t`; `advance_to(t)` drains timers `<= t`; an arrival at a µs whose timers
//! were already drained is rejected.
//!
//! Interpretations of points the documents leave open are marked
//! `[INTERPRETATION]` and listed in the WP1 report.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Modality {
    Pc,
    Haptic,
}

impl Modality {
    pub const ALL: [Modality; 2] = [Modality::Pc, Modality::Haptic];

    pub fn ix(self) -> usize {
        match self {
            Self::Pc => 0,
            Self::Haptic => 1,
        }
    }

    /// Contract §1 track names.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pc => "pc",
            Self::Haptic => "haptic",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exp2Mode {
    /// `due = ref + Δ_max`, no adaptation (registration §2).
    S3npaPrime,
    /// Independent per-modality adaptive Δ.
    P0Np,
    /// Shared adaptive Δ.
    P0,
}

impl Exp2Mode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::S3npaPrime => "s3npa-prime",
            Self::P0Np => "p0np",
            Self::P0 => "p0",
        }
    }
}

/// Exact rational used for q, r_up and the compression fraction so the time
/// path stays integer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ratio {
    pub num: u64,
    pub den: u64,
}

impl Ratio {
    pub const fn new(num: u64, den: u64) -> Self {
        Self { num, den }
    }
}

/// All controller parameters are constructor inputs (registration §2: a
/// priori, untuned).  `delta_max_us` is a registration slot and has no default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Exp2Params {
    pub delta_max_us: u64,
    pub d_min_us: u64,
    pub eps_release_us: u64,
    /// Estimator sample window W (indexed by event `ref`).
    pub window_us: u64,
    /// Quantile q (nearest-rank).
    pub quantile: Ratio,
    /// Update period U.
    pub update_period_us: u64,
    /// Margin m.
    pub margin_us: u64,
    /// Decrease hysteresis h.
    pub hysteresis_us: u64,
    /// Decrease sustain time T_dec.
    pub t_dec_us: u64,
    /// Miss evidence window: exactly this many completed anchor opportunities.
    pub miss_window: usize,
    /// Miss step fires when `misses / miss_window > r_up`.
    pub r_up: Ratio,
    /// Miss step size s_up.
    pub step_up_us: u64,
    /// Maximum compression per event as a fraction of the event interval.
    pub compression: Ratio,
    /// Receiver buffer bounds (per modality), inherited from Experiment 1-B.
    pub max_objects_per_modality: usize,
    pub max_span_us: u64,
}

impl Exp2Params {
    /// The registered constants (registration §1–§2) with the Δ_max slot as an
    /// explicit input.  Nothing here is tuned.
    pub fn registered(delta_max_us: u64) -> Self {
        Self {
            delta_max_us,
            d_min_us: 100_000,
            eps_release_us: 10_000,
            window_us: 2_000_000,
            quantile: Ratio::new(95, 100),
            update_period_us: 500_000,
            margin_us: 20_000,
            hysteresis_us: 30_000,
            t_dec_us: 3_000_000,
            miss_window: 60,
            r_up: Ratio::new(5, 100),
            step_up_us: 50_000,
            compression: Ratio::new(10, 100),
            max_objects_per_modality: 4096,
            max_span_us: 3_000_000,
        }
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        if self.d_min_us == 0 {
            return Err("d_min_us must be > 0");
        }
        if self.delta_max_us < self.d_min_us {
            return Err("delta_max_us must be >= d_min_us");
        }
        if self.window_us == 0 || self.update_period_us == 0 {
            return Err("window_us and update_period_us must be > 0");
        }
        if self.quantile.den == 0 || self.quantile.num == 0 || self.quantile.num > self.quantile.den
        {
            return Err("quantile must be in (0, 1]");
        }
        if self.miss_window == 0 {
            return Err("miss_window must be > 0");
        }
        if self.r_up.den == 0 || self.r_up.num >= self.r_up.den {
            return Err("r_up must be in [0, 1)");
        }
        if self.step_up_us == 0 {
            return Err("step_up_us must be > 0");
        }
        if self.compression.den == 0 || self.compression.num >= self.compression.den {
            return Err("compression must be in [0, 1)");
        }
        if self.max_objects_per_modality == 0 || self.max_span_us == 0 {
            return Err("buffer bounds must be > 0");
        }
        Ok(())
    }
}

/// The sealed pre-run opportunity ledger (contract §3): `t0` and the integer
/// pts of every PC opportunity and haptic slot, used verbatim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Exp2OpportunityLedger {
    pub t0_us: u64,
    /// PC opportunity `i` (event_id = i + 1).
    pub pc_pts_us: Vec<u64>,
    /// Haptic slot `k` belongs to event `k / 3`; `k = 3i` is the anchor
    /// (event_id = i + 1), `3i+1`, `3i+2` are fillers (event_id = 0).
    pub haptic_pts_us: Vec<u64>,
}

impl Exp2OpportunityLedger {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.pc_pts_us.is_empty() {
            return Err("ledger has no PC opportunities");
        }
        if self.pc_pts_us.len() > (u32::MAX / 3) as usize {
            return Err("ledger too large");
        }
        if self.haptic_pts_us.len() != 3 * self.pc_pts_us.len() {
            return Err("haptic slot count must be 3 x PC opportunity count");
        }
        for pts in [&self.pc_pts_us, &self.haptic_pts_us] {
            if pts.windows(2).any(|w| w[1] <= w[0]) {
                return Err("pts must be strictly increasing");
            }
            if pts
                .last()
                .is_some_and(|last| self.t0_us.checked_add(*last).is_none())
            {
                return Err("t0 + pts overflows");
            }
        }
        Ok(())
    }

    pub fn event_count(&self) -> usize {
        self.pc_pts_us.len()
    }
}

/// Contract §2: anchor `event_id = i + 1`.
pub fn event_id_of(event: u32) -> u32 {
    event + 1
}

// --------------------------------------------------------------------------
// Decisions (the log contract for WP3)
// --------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferBound {
    ObjectLimit,
    SpanLimit,
}

impl BufferBound {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ObjectLimit => "object_limit",
            Self::SpanLimit => "span_limit",
        }
    }
}

/// Contract §4 drop reasons.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    /// Arrived after `due + ε_release`.
    Late,
    /// Receiver buffer bound (which bound is kept as detail).
    BufferBound(BufferBound),
    /// Known from the wire (label only; see `delivery_timeout`).
    DeliveryTimeout,
}

impl DropReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Late => "late",
            Self::BufferBound(_) => "buffer_bound",
            Self::DeliveryTimeout => "delivery_timeout",
        }
    }
}

/// Immutable per-opportunity values fixed at `ref(i)` (contract §4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommittedDue {
    pub due_us: u64,
    pub delta_eff_us: u64,
    pub delta_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRecord {
    /// Event index i (0-based).
    pub event: u32,
    /// Contract anchor id = i + 1.
    pub event_id: u32,
    /// = ref(i).
    pub at_us: u64,
    /// PC opportunity i.
    pub pc: CommittedDue,
    /// Haptic slots 3i (anchor), 3i+1, 3i+2 (fillers).
    pub haptic: [CommittedDue; 3],
    /// Hold increment `g_m(i) = max(0, Δ_eff,m(i) − Δ_eff,m(i−1))`, `[pc, haptic]`,
    /// 0 for i = 0.
    pub hold_g_us: [u64; 2],
    /// `Δ_eff,m(i−1) − Δ_eff,m(i)` when compressing, else 0.
    pub compression_us: [u64; 2],
}

impl CommitRecord {
    pub fn delta_eff_us(&self, m: Modality) -> u64 {
        match m {
            Modality::Pc => self.pc.delta_eff_us,
            Modality::Haptic => self.haptic[0].delta_eff_us,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseRecord {
    pub modality: Modality,
    pub index: u32,
    pub t_release_us: u64,
    pub t_recv_us: u64,
    pub committed: CommittedDue,
    /// Released on arrival inside `(due, due + ε_release]`.
    pub grace: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropRecord {
    pub modality: Modality,
    pub index: u32,
    pub at_us: u64,
    /// `None` for a wire delivery-timeout label with no object.
    pub t_recv_us: Option<u64>,
    pub reason: DropReason,
    /// `None` only for a buffer-bound drop before `ref(i)` commit.
    pub committed: Option<CommittedDue>,
}

/// Controller terminal of an opportunity not released by `c = due + ε`
/// (spec §4 "terminal"; enters the miss windows as a miss).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissRecord {
    pub modality: Modality,
    pub index: u32,
    /// = `due + ε_release`.
    pub at_us: u64,
    pub committed: CommittedDue,
}

/// Anchor pair result, emitted strictly in planned event order (the spec §6
/// "traverse the planned ledger in due order").  It is the only input of the
/// P1 FSM and, in P0, of the shared miss window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PairCompleteRecord {
    pub event: u32,
    pub event_id: u32,
    /// Time the result was consumed (= now when the in-order cursor passed it).
    pub at_us: u64,
    /// Pair completion = later of the two anchor terminals.
    pub completed_at_us: u64,
    pub pc_miss: bool,
    pub haptic_miss: bool,
    /// Committed Δ_eff per modality `[pc, haptic]`; `None` only if an anchor
    /// was buffer-dropped before its event committed.
    pub delta_eff_us: [Option<u64>; 2],
}

impl PairCompleteRecord {
    pub fn pair_miss(&self) -> bool {
        self.pc_miss || self.haptic_miss
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaCause {
    MissStep,
    QIncrease,
    QDecrease,
}

impl DeltaCause {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MissStep => "miss_step",
            Self::QIncrease => "q_increase",
            Self::QDecrease => "q_decrease",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaUpdateRecord {
    /// Controller index: P0 → 0 (shared); P0-NP → 0 = PC, 1 = haptic.
    pub controller: usize,
    pub at_us: u64,
    pub cause: DeltaCause,
    pub from_us: u64,
    pub to_us: u64,
    pub delta_generation: u64,
    /// For `MissStep`: misses in the consumed window.
    pub window_misses: Option<usize>,
    /// For Q paths: the nearest-rank quantile used.
    pub q_us: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticKind {
    DuplicateArrival,
    InputAfterFinalize,
    DeliveryTimeoutIgnored,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiagnosticRecord {
    pub kind: DiagnosticKind,
    pub modality: Modality,
    pub index: u32,
    pub at_us: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Exp2Decision {
    Commit(CommitRecord),
    Release(ReleaseRecord),
    Drop(DropRecord),
    Miss(MissRecord),
    PairComplete(PairCompleteRecord),
    DeltaUpdate(DeltaUpdateRecord),
    Diagnostic(DiagnosticRecord),
}

impl Exp2Decision {
    pub fn at_us(&self) -> u64 {
        match self {
            Self::Commit(r) => r.at_us,
            Self::Release(r) => r.t_release_us,
            Self::Drop(r) => r.at_us,
            Self::Miss(r) => r.at_us,
            Self::PairComplete(r) => r.at_us,
            Self::DeltaUpdate(r) => r.at_us,
            Self::Diagnostic(r) => r.at_us,
        }
    }
}

// --------------------------------------------------------------------------
// Scheduler terminal ledger (contract §4)
// --------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulerTerminal {
    Released { t_release_us: u64, grace: bool },
    Dropped { at_us: u64, reason: DropReason },
    /// Not released by H and scheduler due > H (cancelled at H).
    PendingAtHorizon,
    /// [CONTRACT GAP] No object arrived by H and due <= H: none of the three
    /// contract §4 states applies.  The controller terminal is the timeout at
    /// `c`; how WP3/WP2 name it in the output ledger is theirs
    /// (`not_submitted` in registration §4-5).
    NoObject,
}

impl SchedulerTerminal {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Released { .. } => "released",
            Self::Dropped { .. } => "dropped",
            Self::PendingAtHorizon => "pending_at_horizon",
            Self::NoObject => "no_object",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerLedgerEntry {
    pub terminal: SchedulerTerminal,
    /// `None` only if the event never committed (ref > H).
    pub committed: Option<CommittedDue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Exp2SchedulerLedger {
    pub horizon_us: u64,
    pub pc: Vec<SchedulerLedgerEntry>,
    pub haptic: Vec<SchedulerLedgerEntry>,
}

impl Exp2SchedulerLedger {
    pub fn entries(&self, m: Modality) -> &[SchedulerLedgerEntry] {
        match m {
            Modality::Pc => &self.pc,
            Modality::Haptic => &self.haptic,
        }
    }

    pub fn count(&self, m: Modality, state: &str) -> usize {
        self.entries(m)
            .iter()
            .filter(|e| e.terminal.as_str() == state)
            .count()
    }
}

/// Hold metrics per modality (registration §4-6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HoldSummary {
    pub hold_step_count: u64,
    pub total_added_hold_us: u64,
    pub hold_episode_count: u64,
    pub compression_event_count: u64,
    pub total_compression_us: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EstimatorSnapshot {
    pub delta_star_us: u64,
    pub delta_generation: u64,
    pub q_us: Option<i64>,
    pub sample_count: usize,
    pub miss_window_len: usize,
    pub miss_window_misses: usize,
    pub decrease_since_us: Option<u64>,
}

// --------------------------------------------------------------------------
// Internal state
// --------------------------------------------------------------------------

const RANK_TERMINAL: u8 = 0;
const RANK_COMMIT: u8 = 1;
const RANK_TICK: u8 = 2;

const KIND_DUE: u8 = 0;
const KIND_TIMEOUT: u8 = 1;
const KIND_COMMIT: u8 = 2;
const KIND_TICK: u8 = 3;

/// `(time, rank, modality, index, kind)` — the tuple order is the
/// deterministic same-µs order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct TimerKey(u64, u8, u8, u32, u8);

#[derive(Debug, Clone, Default)]
struct Slot {
    committed: Option<CommittedDue>,
    arrival_us: Option<u64>,
    buffered: bool,
    /// Controller terminal `(at, missed)`.
    terminal: Option<(u64, bool)>,
    released: Option<(u64, bool)>,
    dropped: Option<(u64, DropReason)>,
    wire_timeout_us: Option<u64>,
}

#[derive(Debug, Clone, Default)]
struct EventState {
    delta_eff_us: [Option<u64>; 2],
}

#[derive(Debug, Clone)]
struct Controller {
    delta_star_us: u64,
    delta_generation: u64,
    miss_window: VecDeque<bool>,
    decrease_since_us: Option<u64>,
    /// event → (event ref, arrival-delay sample).
    samples: BTreeMap<u32, (u64, i64)>,
}

impl Controller {
    fn new(delta_max_us: u64) -> Self {
        Self {
            delta_star_us: delta_max_us,
            delta_generation: 0,
            miss_window: VecDeque::new(),
            decrease_since_us: None,
            samples: BTreeMap::new(),
        }
    }

    fn push_completion(&mut self, missed: bool, cap: usize) {
        self.miss_window.push_back(missed);
        while self.miss_window.len() > cap {
            self.miss_window.pop_front();
        }
    }

    fn misses(&self) -> usize {
        self.miss_window.iter().filter(|m| **m).count()
    }

    /// Nearest-rank quantile (`⌈q·n⌉`-th smallest) over samples with
    /// `ref ∈ (at − W, at]`.  [INTERPRETATION] half-open window by event ref.
    fn quantile(&self, at_us: u64, window_us: u64, q: Ratio) -> (Option<i64>, usize) {
        let mut values: Vec<i64> = self
            .samples
            .values()
            .filter(|(ref_us, _)| {
                *ref_us <= at_us && (at_us < window_us || *ref_us > at_us - window_us)
            })
            .map(|(_, v)| *v)
            .collect();
        let n = values.len();
        if n == 0 {
            return (None, 0);
        }
        values.sort_unstable();
        let rank = ((q.num as u128 * n as u128 + q.den as u128 - 1) / q.den as u128) as usize;
        (Some(values[rank.max(1) - 1]), n)
    }
}

pub struct Exp2Playout {
    mode: Exp2Mode,
    params: Exp2Params,
    ledger: Exp2OpportunityLedger,
    slots: [Vec<Slot>; 2],
    events: Vec<EventState>,
    controllers: Vec<Controller>,
    timers: BTreeSet<TimerKey>,
    buffers: [BTreeSet<(u64, u32)>; 2],
    max_occupancy: [usize; 2],
    /// Next event whose anchor pair result is consumed (planned order).
    pair_cursor: u32,
    /// P0-NP: next anchor per modality consumed into that modality's window.
    modal_cursor: [u32; 2],
    last_input_us: Option<u64>,
    drained_through_us: Option<u64>,
    finalized: Option<Exp2SchedulerLedger>,
}

impl Exp2Playout {
    pub fn new(
        mode: Exp2Mode,
        params: Exp2Params,
        ledger: Exp2OpportunityLedger,
    ) -> Result<Self, &'static str> {
        params.validate()?;
        ledger.validate()?;
        let n = ledger.event_count();
        let controllers = match mode {
            Exp2Mode::S3npaPrime => Vec::new(),
            Exp2Mode::P0 => vec![Controller::new(params.delta_max_us)],
            Exp2Mode::P0Np => vec![
                Controller::new(params.delta_max_us),
                Controller::new(params.delta_max_us),
            ],
        };
        let mut timers = BTreeSet::new();
        for (i, pts) in ledger.pc_pts_us.iter().enumerate() {
            timers.insert(TimerKey(
                ledger.t0_us + pts,
                RANK_COMMIT,
                0,
                i as u32,
                KIND_COMMIT,
            ));
        }
        // [INTERPRETATION] tick grid t0 + k·U, k >= 1 (anchored to t0 so the
        // schedule is invariant under t0 translation).
        timers.insert(TimerKey(
            ledger.t0_us + params.update_period_us,
            RANK_TICK,
            0,
            0,
            KIND_TICK,
        ));
        Ok(Self {
            mode,
            params,
            slots: [vec![Slot::default(); n], vec![Slot::default(); 3 * n]],
            events: vec![EventState::default(); n],
            ledger,
            controllers,
            timers,
            buffers: [BTreeSet::new(), BTreeSet::new()],
            max_occupancy: [0, 0],
            pair_cursor: 0,
            modal_cursor: [0, 0],
            last_input_us: None,
            drained_through_us: None,
            finalized: None,
        })
    }

    pub fn mode(&self) -> Exp2Mode {
        self.mode
    }

    pub fn params(&self) -> &Exp2Params {
        &self.params
    }

    /// t0 is fixed at construction; no API changes it.
    pub fn t0_us(&self) -> u64 {
        self.ledger.t0_us
    }

    pub fn event_count(&self) -> usize {
        self.ledger.event_count()
    }

    /// `ref(i) = t0 + pts_pc(i)`.
    pub fn event_ref_us(&self, event: u32) -> u64 {
        self.ledger.t0_us + self.ledger.pc_pts_us[event as usize]
    }

    fn slot_ref_us(&self, m: Modality, index: u32) -> u64 {
        self.ledger.t0_us
            + match m {
                Modality::Pc => self.ledger.pc_pts_us[index as usize],
                Modality::Haptic => self.ledger.haptic_pts_us[index as usize],
            }
    }

    /// The single wakeup for WP3: the earliest pending internal timer.
    pub fn next_wakeup_us(&self) -> Option<u64> {
        if self.finalized.is_some() {
            return None;
        }
        self.timers.first().map(|k| k.0)
    }

    /// Maximum buffer occupancy after bound enforcement (registration §4-6).
    pub fn max_occupancy(&self, m: Modality) -> usize {
        self.max_occupancy[m.ix()]
    }

    pub fn is_finalized(&self) -> bool {
        self.finalized.is_some()
    }

    pub fn committed(&self, m: Modality, index: u32) -> Option<CommittedDue> {
        self.slots[m.ix()][index as usize].committed
    }

    fn controller_for(&self, m: Modality) -> Option<usize> {
        match self.mode {
            Exp2Mode::S3npaPrime => None,
            Exp2Mode::P0 => Some(0),
            Exp2Mode::P0Np => Some(m.ix()),
        }
    }

    pub fn controller_count(&self) -> usize {
        self.controllers.len()
    }

    /// Read-only estimator state evaluated as a Q update at `at_us` would see
    /// it (sample set as of the last processed input).
    /// `None` for S3NPA' (no controller) or an out-of-range index.
    pub fn estimator_snapshot(&self, controller: usize, at_us: u64) -> Option<EstimatorSnapshot> {
        let c = self.controllers.get(controller)?;
        let (q_us, sample_count) =
            c.quantile(at_us, self.params.window_us, self.params.quantile);
        Some(EstimatorSnapshot {
            delta_star_us: c.delta_star_us,
            delta_generation: c.delta_generation,
            q_us,
            sample_count,
            miss_window_len: c.miss_window.len(),
            miss_window_misses: c.misses(),
            decrease_since_us: c.decrease_since_us,
        })
    }

    fn begin_input(
        &mut self,
        m: Modality,
        index: u32,
        t_us: u64,
        out: &mut Vec<Exp2Decision>,
    ) -> Result<bool, &'static str> {
        if index as usize >= self.slots[m.ix()].len() {
            return Err("opportunity index out of range");
        }
        if self.finalized.is_some() {
            out.push(Exp2Decision::Diagnostic(DiagnosticRecord {
                kind: DiagnosticKind::InputAfterFinalize,
                modality: m,
                index,
                at_us: t_us,
            }));
            return Ok(false);
        }
        if self.last_input_us.is_some_and(|last| t_us < last) {
            return Err("input time moved backwards");
        }
        if self.drained_through_us.is_some_and(|d| t_us <= d) {
            return Err("input at a µs whose timers were already processed");
        }
        self.drain(t_us, false, out);
        self.last_input_us = Some(t_us);
        Ok(true)
    }

    /// Object `(m, index)` arrived at `t_recv_us`.  Internal timers strictly
    /// before `t_recv_us` are processed first; timers at `t_recv_us` stay
    /// pending, so the arrival precedes any same-µs terminal (tie rule).
    pub fn arrive(
        &mut self,
        m: Modality,
        index: u32,
        t_recv_us: u64,
    ) -> Result<Vec<Exp2Decision>, &'static str> {
        let mut out = Vec::new();
        if self.begin_input(m, index, t_recv_us, &mut out)? {
            self.on_arrival(m, index, t_recv_us, &mut out);
        }
        Ok(out)
    }

    /// The wire reported that `(m, index)` hit its delivery timeout.
    ///
    /// [INTERPRETATION] label only: it does not create an early controller
    /// terminal (spec §4 terminal = receiver release/drop, or `c`), so the
    /// controller dynamics do not depend on wire notification timing.  It
    /// becomes the scheduler terminal `dropped(delivery_timeout)` only if the
    /// object is never released or receiver-dropped.  Ignored (diagnostic) if
    /// the object already arrived.
    pub fn delivery_timeout(
        &mut self,
        m: Modality,
        index: u32,
        t_us: u64,
    ) -> Result<Vec<Exp2Decision>, &'static str> {
        let mut out = Vec::new();
        if !self.begin_input(m, index, t_us, &mut out)? {
            return Ok(out);
        }
        let slot = &mut self.slots[m.ix()][index as usize];
        if slot.arrival_us.is_some() || slot.wire_timeout_us.is_some() {
            out.push(Exp2Decision::Diagnostic(DiagnosticRecord {
                kind: DiagnosticKind::DeliveryTimeoutIgnored,
                modality: m,
                index,
                at_us: t_us,
            }));
            return Ok(out);
        }
        slot.wire_timeout_us = Some(t_us);
        out.push(Exp2Decision::Drop(DropRecord {
            modality: m,
            index,
            at_us: t_us,
            t_recv_us: None,
            reason: DropReason::DeliveryTimeout,
            committed: slot.committed,
        }));
        Ok(out)
    }

    /// Process every internal timer with time `<= t_us`.
    pub fn advance_to(&mut self, t_us: u64) -> Result<Vec<Exp2Decision>, &'static str> {
        if self.finalized.is_some() {
            return Err("already finalized");
        }
        if self.last_input_us.is_some_and(|last| t_us < last) {
            return Err("input time moved backwards");
        }
        let mut out = Vec::new();
        self.drain(t_us, true, &mut out);
        self.last_input_us = Some(t_us);
        self.drained_through_us = Some(t_us);
        Ok(out)
    }

    /// Observation horizon H: process timers `<= H`, then seal the scheduler
    /// terminal of every opportunity.  Buffered objects whose due exceeds H are
    /// cancelled and recorded as `pending_at_horizon`.
    pub fn finalize(
        &mut self,
        horizon_us: u64,
    ) -> Result<(Vec<Exp2Decision>, Exp2SchedulerLedger), &'static str> {
        let out = self.advance_to(horizon_us)?;
        let mut sealed = Exp2SchedulerLedger {
            horizon_us,
            pc: Vec::with_capacity(self.slots[0].len()),
            haptic: Vec::with_capacity(self.slots[1].len()),
        };
        for m in Modality::ALL {
            for (index, slot) in self.slots[m.ix()].iter().enumerate() {
                // Uncommitted at H means ref > H, hence due > H.
                let due = slot.committed.map(|c| c.due_us).unwrap_or_else(|| {
                    self.slot_ref_us(m, index as u32) + self.params.d_min_us
                });
                // [INTERPRETATION] precedence: released > receiver drop >
                // wire delivery_timeout > pending_at_horizon > no_object.
                let terminal = if let Some((t_release_us, grace)) = slot.released {
                    SchedulerTerminal::Released {
                        t_release_us,
                        grace,
                    }
                } else if let Some((at_us, reason)) = slot.dropped {
                    SchedulerTerminal::Dropped { at_us, reason }
                } else if let Some(at_us) = slot.wire_timeout_us {
                    SchedulerTerminal::Dropped {
                        at_us,
                        reason: DropReason::DeliveryTimeout,
                    }
                } else if due > horizon_us {
                    SchedulerTerminal::PendingAtHorizon
                } else {
                    SchedulerTerminal::NoObject
                };
                let entry = SchedulerLedgerEntry {
                    terminal,
                    committed: slot.committed,
                };
                match m {
                    Modality::Pc => sealed.pc.push(entry),
                    Modality::Haptic => sealed.haptic.push(entry),
                }
            }
            self.buffers[m.ix()].clear();
        }
        self.timers.clear();
        self.finalized = Some(sealed.clone());
        Ok((out, sealed))
    }

    /// Hold metrics over committed events (registration §4-6):
    /// `hold_episode_count` = maximal runs of positive increments not broken
    /// by a decrease (unchanged events do not break a run).
    pub fn hold_summary(&self, m: Modality) -> HoldSummary {
        let mut s = HoldSummary::default();
        let mut prev: Option<u64> = None;
        let mut in_episode = false;
        for e in &self.events {
            let Some(d) = e.delta_eff_us[m.ix()] else {
                break;
            };
            if let Some(p) = prev {
                if d > p {
                    s.hold_step_count += 1;
                    s.total_added_hold_us += d - p;
                    if !in_episode {
                        s.hold_episode_count += 1;
                        in_episode = true;
                    }
                } else if d < p {
                    in_episode = false;
                    s.compression_event_count += 1;
                    s.total_compression_us += p - d;
                }
            }
            prev = Some(d);
        }
        s
    }

    // ----------------------------------------------------------------------

    fn drain(&mut self, limit_us: u64, inclusive: bool, out: &mut Vec<Exp2Decision>) {
        while let Some(&key) = self.timers.first() {
            if key.0 > limit_us || (!inclusive && key.0 == limit_us) {
                break;
            }
            self.timers.remove(&key);
            let TimerKey(at, _rank, m, index, kind) = key;
            let modality = if m == 0 { Modality::Pc } else { Modality::Haptic };
            match kind {
                KIND_DUE => self.on_due(modality, index, at, out),
                KIND_TIMEOUT => self.on_timeout(modality, index, at, out),
                KIND_COMMIT => self.on_commit(index, at, out),
                KIND_TICK => self.on_tick(at, out),
                _ => unreachable!("unknown timer kind"),
            }
        }
    }

    fn schedule_terminal(&mut self, at: u64, m: Modality, index: u32, kind: u8) {
        self.timers
            .insert(TimerKey(at, RANK_TERMINAL, m.ix() as u8, index, kind));
    }

    fn on_arrival(&mut self, m: Modality, index: u32, t: u64, out: &mut Vec<Exp2Decision>) {
        if self.slots[m.ix()][index as usize].arrival_us.is_some() {
            out.push(Exp2Decision::Diagnostic(DiagnosticRecord {
                kind: DiagnosticKind::DuplicateArrival,
                modality: m,
                index,
                at_us: t,
            }));
            return;
        }
        self.slots[m.ix()][index as usize].arrival_us = Some(t);
        self.add_sample(m, index);

        let slot = self.slots[m.ix()][index as usize].clone();
        if slot.terminal.is_some() {
            // Timed out at c < t: late drop (the miss was recorded at c).
            self.discard(m, index, t, DropReason::Late, out);
            return;
        }
        match slot.committed {
            Some(c) if t <= c.due_us => {
                self.buffer_insert(m, index);
                self.schedule_terminal(c.due_us, m, index, KIND_DUE);
                self.enforce_buffer_bounds(m, t, out);
            }
            Some(c) => {
                // (due, due + ε]: the timeout at c has not fired (timers < t
                // were drained, and a fired timeout would have set `terminal`).
                debug_assert!(t <= c.due_us + self.params.eps_release_us);
                self.release(m, index, t, true, out);
            }
            None => {
                // Arrived before ref(i) commit.
                self.buffer_insert(m, index);
                self.enforce_buffer_bounds(m, t, out);
            }
        }
    }

    /// Estimator samples: anchors only; P0 uses `max(a_pc, a_hap)` of events
    /// whose two anchors both arrived; P0-NP uses the modality's own `a_m`.
    /// Missing arrivals are never imputed.  `a_m = t_recv − (t0 + pts_m)`;
    /// the window index is the event ref `ref(i)` for both modalities.
    fn add_sample(&mut self, m: Modality, index: u32) {
        let Some(ci) = self.controller_for(m) else {
            return;
        };
        let event = match m {
            Modality::Pc => index,
            Modality::Haptic if index % 3 == 0 => index / 3,
            Modality::Haptic => return, // fillers never feed the estimator
        };
        let delay = |this: &Self, mm: Modality, idx: u32| -> Option<i64> {
            this.slots[mm.ix()][idx as usize]
                .arrival_us
                .map(|t| t as i64 - this.slot_ref_us(mm, idx) as i64)
        };
        let value = match self.mode {
            Exp2Mode::S3npaPrime => None,
            Exp2Mode::P0Np => delay(self, m, index),
            Exp2Mode::P0 => match (
                delay(self, Modality::Pc, event),
                delay(self, Modality::Haptic, 3 * event),
            ) {
                (Some(a), Some(b)) => Some(a.max(b)),
                _ => None,
            },
        };
        if let Some(v) = value {
            let event_ref = self.event_ref_us(event);
            self.controllers[ci]
                .samples
                .entry(event)
                .or_insert((event_ref, v));
        }
    }

    fn buffer_insert(&mut self, m: Modality, index: u32) {
        let pts = self.slot_ref_us(m, index) - self.ledger.t0_us;
        self.buffers[m.ix()].insert((pts, index));
        self.slots[m.ix()][index as usize].buffered = true;
    }

    fn enforce_buffer_bounds(&mut self, m: Modality, now: u64, out: &mut Vec<Exp2Decision>) {
        while self.buffers[m.ix()].len() > self.params.max_objects_per_modality {
            let (_, index) = *self.buffers[m.ix()].first().expect("non-empty");
            self.discard(
                m,
                index,
                now,
                DropReason::BufferBound(BufferBound::ObjectLimit),
                out,
            );
        }
        loop {
            let buf = &self.buffers[m.ix()];
            let span = match (buf.first(), buf.last()) {
                (Some(a), Some(b)) => b.0 - a.0,
                _ => 0,
            };
            if span <= self.params.max_span_us {
                break;
            }
            let (_, index) = *buf.first().expect("non-empty");
            self.discard(
                m,
                index,
                now,
                DropReason::BufferBound(BufferBound::SpanLimit),
                out,
            );
        }
        let occ = self.buffers[m.ix()].len();
        self.max_occupancy[m.ix()] = self.max_occupancy[m.ix()].max(occ);
    }

    fn remove_from_buffer(&mut self, m: Modality, index: u32) {
        let pts = self.slot_ref_us(m, index) - self.ledger.t0_us;
        let slot = &mut self.slots[m.ix()][index as usize];
        if slot.buffered {
            slot.buffered = false;
            self.buffers[m.ix()].remove(&(pts, index));
        }
    }

    fn discard(
        &mut self,
        m: Modality,
        index: u32,
        at: u64,
        reason: DropReason,
        out: &mut Vec<Exp2Decision>,
    ) {
        self.remove_from_buffer(m, index);
        let slot = &mut self.slots[m.ix()][index as usize];
        debug_assert!(slot.dropped.is_none() && slot.released.is_none());
        slot.dropped = Some((at, reason));
        out.push(Exp2Decision::Drop(DropRecord {
            modality: m,
            index,
            at_us: at,
            t_recv_us: slot.arrival_us,
            reason,
            committed: slot.committed,
        }));
        if slot.terminal.is_none() {
            self.on_terminal(m, index, at, true, out);
        }
    }

    fn release(
        &mut self,
        m: Modality,
        index: u32,
        at: u64,
        grace: bool,
        out: &mut Vec<Exp2Decision>,
    ) {
        self.remove_from_buffer(m, index);
        let slot = &mut self.slots[m.ix()][index as usize];
        slot.released = Some((at, grace));
        out.push(Exp2Decision::Release(ReleaseRecord {
            modality: m,
            index,
            t_release_us: at,
            t_recv_us: slot.arrival_us.expect("release requires an object"),
            committed: slot.committed.expect("release requires a committed due"),
            grace,
        }));
        self.on_terminal(m, index, at, false, out);
    }

    fn on_due(&mut self, m: Modality, index: u32, at: u64, out: &mut Vec<Exp2Decision>) {
        let slot = &self.slots[m.ix()][index as usize];
        if slot.buffered
            && slot.terminal.is_none()
            && slot.committed.map(|c| c.due_us) == Some(at)
        {
            // No pair gate: each modality releases at its own due.
            self.release(m, index, at, false, out);
        }
    }

    fn on_timeout(&mut self, m: Modality, index: u32, at: u64, out: &mut Vec<Exp2Decision>) {
        let slot = &self.slots[m.ix()][index as usize];
        if slot.terminal.is_some() {
            return; // stale: released or dropped earlier
        }
        debug_assert!(!slot.buffered, "a buffered object is released at due < c");
        out.push(Exp2Decision::Miss(MissRecord {
            modality: m,
            index,
            at_us: at,
            committed: slot.committed.expect("timeout requires a due"),
        }));
        self.on_terminal(m, index, at, true, out);
    }

    fn is_anchor(m: Modality, index: u32) -> bool {
        m == Modality::Pc || index % 3 == 0
    }

    fn anchor_terminal(&self, m: Modality, event: u32) -> Option<(u64, bool)> {
        let idx = match m {
            Modality::Pc => event,
            Modality::Haptic => 3 * event,
        };
        self.slots[m.ix()][idx as usize].terminal
    }

    fn on_terminal(
        &mut self,
        m: Modality,
        index: u32,
        at: u64,
        missed: bool,
        out: &mut Vec<Exp2Decision>,
    ) {
        self.slots[m.ix()][index as usize].terminal = Some((at, missed));
        if !Self::is_anchor(m, index) {
            return; // fillers enter no window
        }
        let n = self.events.len() as u32;
        let cap = self.params.miss_window;
        if self.mode == Exp2Mode::P0Np {
            // P0-NP: the modality's own anchor terminals, consumed in planned
            // order (independent of the other modality).
            while self.modal_cursor[m.ix()] < n {
                let e = self.modal_cursor[m.ix()];
                let Some((_, missed)) = self.anchor_terminal(m, e) else {
                    break;
                };
                self.controllers[m.ix()].push_completion(missed, cap);
                self.modal_cursor[m.ix()] += 1;
            }
        }
        // Pair results in planned event order (every mode; P0 window, P1 FSM).
        while self.pair_cursor < n {
            let e = self.pair_cursor;
            let (Some((tp, pm)), Some((th, hm))) = (
                self.anchor_terminal(Modality::Pc, e),
                self.anchor_terminal(Modality::Haptic, e),
            ) else {
                break;
            };
            let rec = PairCompleteRecord {
                event: e,
                event_id: event_id_of(e),
                at_us: at,
                completed_at_us: tp.max(th),
                pc_miss: pm,
                haptic_miss: hm,
                delta_eff_us: self.events[e as usize].delta_eff_us,
            };
            if self.mode == Exp2Mode::P0 {
                self.controllers[0].push_completion(rec.pair_miss(), cap);
            }
            out.push(Exp2Decision::PairComplete(rec));
            self.pair_cursor += 1;
        }
    }

    fn on_commit(&mut self, event: u32, at: u64, out: &mut Vec<Exp2Decision>) {
        let i = event as usize;
        let interval = if i == 0 {
            0
        } else {
            self.ledger.pc_pts_us[i] - self.ledger.pc_pts_us[i - 1]
        };
        // floor(interval · compression) keeps the reduction at most 10 %.
        let max_reduction = ((interval as u128 * self.params.compression.num as u128)
            / self.params.compression.den as u128) as u64;
        let mut delta_eff = [0u64; 2];
        let mut generation = [0u64; 2];
        let mut hold = [0u64; 2];
        let mut compression = [0u64; 2];
        for m in Modality::ALL {
            let (star, gen) = match self.controller_for(m) {
                Some(ci) => (
                    self.controllers[ci].delta_star_us,
                    self.controllers[ci].delta_generation,
                ),
                None => (self.params.delta_max_us, 0),
            };
            let d = if i == 0 {
                // Initialisation: due(0) = ref(0) + Δ_max.
                self.params.delta_max_us
            } else {
                let prev = self.events[i - 1].delta_eff_us[m.ix()].expect("prior commit");
                if star >= prev {
                    hold[m.ix()] = star - prev; // increase: immediate hold
                    star
                } else {
                    // [INTERPRETATION] compression uses the event interval
                    // ref(i) − ref(i−1) for both modalities.
                    let d = star.max(prev.saturating_sub(max_reduction));
                    compression[m.ix()] = prev - d;
                    d
                }
            };
            debug_assert!(d <= self.params.delta_max_us);
            delta_eff[m.ix()] = d;
            generation[m.ix()] = gen;
        }
        let pc = CommittedDue {
            due_us: at + delta_eff[0],
            delta_eff_us: delta_eff[0],
            delta_generation: generation[0],
        };
        let mut haptic = [pc; 3];
        for (j, h) in haptic.iter_mut().enumerate() {
            // due = t0 + pts_h + Δ_eff,h(i), pts_h verbatim from the ledger.
            *h = CommittedDue {
                due_us: self.slot_ref_us(Modality::Haptic, 3 * event + j as u32) + delta_eff[1],
                delta_eff_us: delta_eff[1],
                delta_generation: generation[1],
            };
        }
        self.events[i].delta_eff_us = [Some(delta_eff[0]), Some(delta_eff[1])];
        out.push(Exp2Decision::Commit(CommitRecord {
            event,
            event_id: event_id_of(event),
            at_us: at,
            pc,
            haptic,
            hold_g_us: hold,
            compression_us: compression,
        }));
        // One transaction: all four opportunities of event i.
        let mut targets = vec![(Modality::Pc, event, pc)];
        for (j, h) in haptic.iter().enumerate() {
            targets.push((Modality::Haptic, 3 * event + j as u32, *h));
        }
        for (m, index, c) in targets {
            let slot = &mut self.slots[m.ix()][index as usize];
            debug_assert!(slot.committed.is_none(), "due is committed once");
            slot.committed = Some(c);
            if slot.terminal.is_some() {
                continue; // buffer-dropped before commit
            }
            let buffered = slot.buffered;
            self.schedule_terminal(c.due_us + self.params.eps_release_us, m, index, KIND_TIMEOUT);
            if buffered {
                self.schedule_terminal(c.due_us, m, index, KIND_DUE);
            }
        }
    }

    fn on_tick(&mut self, at: u64, out: &mut Vec<Exp2Decision>) {
        self.timers.insert(TimerKey(
            at + self.params.update_period_us,
            RANK_TICK,
            0,
            0,
            KIND_TICK,
        ));
        let p = self.params;
        for ci in 0..self.controllers.len() {
            let c = &mut self.controllers[ci];
            // Samples are only read through `(at − W, at]` with `at` monotone.
            if at >= p.window_us {
                let cutoff = at - p.window_us;
                c.samples.retain(|_, (r, _)| *r > cutoff);
            }
            // (3) miss step — exactly `miss_window` completed anchors.
            if c.miss_window.len() == p.miss_window {
                let misses = c.misses();
                if misses as u128 * p.r_up.den as u128
                    > p.r_up.num as u128 * p.miss_window as u128
                {
                    // [INTERPRETATION] Δ_current = controller target Δ*.
                    let from = c.delta_star_us;
                    let inc = p.step_up_us.min(p.delta_max_us - from);
                    c.delta_star_us = from + inc;
                    if inc > 0 {
                        c.delta_generation += 1;
                    }
                    // Consume the whole evidence window; keep the Q sample
                    // window.  [INTERPRETATION] also fires (and consumes) at
                    // Δ* = Δ_max with a zero increment, pre-empting the Q
                    // update this tick; resets the decrease timer.
                    c.miss_window.clear();
                    c.decrease_since_us = None;
                    out.push(Exp2Decision::DeltaUpdate(DeltaUpdateRecord {
                        controller: ci,
                        at_us: at,
                        cause: DeltaCause::MissStep,
                        from_us: from,
                        to_us: c.delta_star_us,
                        delta_generation: c.delta_generation,
                        window_misses: Some(misses),
                        q_us: None,
                    }));
                    continue;
                }
            }
            // (4) Q update.
            let (q, _) = c.quantile(at, p.window_us, p.quantile);
            let Some(q) = q else {
                // No samples: hold Δ*.  [INTERPRETATION] breaks "sustained".
                c.decrease_since_us = None;
                continue;
            };
            let target = (q.saturating_add(p.margin_us as i64))
                .clamp(p.d_min_us as i64, p.delta_max_us as i64) as u64;
            let from = c.delta_star_us;
            if target > from {
                c.delta_star_us = target;
                c.delta_generation += 1;
                c.decrease_since_us = None;
                out.push(Exp2Decision::DeltaUpdate(DeltaUpdateRecord {
                    controller: ci,
                    at_us: at,
                    cause: DeltaCause::QIncrease,
                    from_us: from,
                    to_us: target,
                    delta_generation: c.delta_generation,
                    window_misses: None,
                    q_us: Some(q),
                }));
            } else if target + p.hysteresis_us <= from {
                // [INTERPRETATION] sustained = every tick since `since` had
                // target <= Δ* − h; decrease to the current tick's target.
                let since = *c.decrease_since_us.get_or_insert(at);
                if at - since >= p.t_dec_us {
                    c.delta_star_us = target;
                    c.delta_generation += 1;
                    c.decrease_since_us = None;
                    out.push(Exp2Decision::DeltaUpdate(DeltaUpdateRecord {
                        controller: ci,
                        at_us: at,
                        cause: DeltaCause::QDecrease,
                        from_us: from,
                        to_us: target,
                        delta_generation: c.delta_generation,
                        window_misses: None,
                        q_us: Some(q),
                    }));
                }
            } else {
                c.decrease_since_us = None;
            }
        }
    }
}

// --------------------------------------------------------------------------
// Tests: spec §8 gates (core-side), §3 stability assertions, and the
// individual core tests of the WP1 adjustment.
// --------------------------------------------------------------------------

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub const T0: u64 = 10_000_000;
    pub const DMAX: u64 = 345_000; // provisional slot value, test input only
    pub const B_PLAY: u64 = 400_000;

    /// Contract §1 pts: pc `floor(i·10⁶/30)`, haptic `floor(k·10⁶/90)`.
    pub fn ledger(n: usize) -> Exp2OpportunityLedger {
        ledger_at(T0, n)
    }

    pub fn ledger_at(t0: u64, n: usize) -> Exp2OpportunityLedger {
        Exp2OpportunityLedger {
            t0_us: t0,
            pc_pts_us: (0..n as u64).map(|i| i * 1_000_000 / 30).collect(),
            haptic_pts_us: (0..3 * n as u64).map(|k| k * 1_000_000 / 90).collect(),
        }
    }

    /// H = t0 + run length + B_play.
    pub fn horizon(l: &Exp2OpportunityLedger) -> u64 {
        l.t0_us + (l.pc_pts_us.len() as u64) * 1_000_000 / 30 + B_PLAY
    }

    pub type Arrival = (u64, Modality, u32);

    /// Build arrivals from a delay function `(modality, index, ref) → delay`.
    pub fn arrivals(
        l: &Exp2OpportunityLedger,
        mut f: impl FnMut(Modality, u32, u64) -> Option<u64>,
    ) -> Vec<Arrival> {
        let mut v = Vec::new();
        for (i, pts) in l.pc_pts_us.iter().enumerate() {
            let r = l.t0_us + pts;
            if let Some(d) = f(Modality::Pc, i as u32, r) {
                v.push((r + d, Modality::Pc, i as u32));
            }
        }
        for (k, pts) in l.haptic_pts_us.iter().enumerate() {
            let r = l.t0_us + pts;
            if let Some(d) = f(Modality::Haptic, k as u32, r) {
                v.push((r + d, Modality::Haptic, k as u32));
            }
        }
        v.sort();
        v
    }

    pub struct Run {
        pub decisions: Vec<Exp2Decision>,
        pub ledger: Exp2SchedulerLedger,
        pub core: Exp2Playout,
    }

    pub fn run(
        mode: Exp2Mode,
        params: Exp2Params,
        l: &Exp2OpportunityLedger,
        trace: &[Arrival],
    ) -> Run {
        let h = horizon(l);
        let mut core = Exp2Playout::new(mode, params, l.clone()).unwrap();
        let mut decisions = Vec::new();
        for &(t, m, i) in trace {
            if t > h {
                break;
            }
            decisions.extend(core.arrive(m, i, t).unwrap());
        }
        let (tail, sealed) = core.finalize(h).unwrap();
        decisions.extend(tail);
        Run {
            decisions,
            ledger: sealed,
            core,
        }
    }

    /// Deterministic LCG.
    pub struct Lcg(pub u64);
    impl Lcg {
        pub fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.0 >> 33
        }
        pub fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    /// A mixed trace: low base delay (decreases and compression), a delay
    /// spike (Q increase / hold), a burst of anchor losses (miss step), and a
    /// late-arrival segment.  PC i and haptic 3i get the *same* delay
    /// (gate-1 precondition); fillers get independent jitter.
    pub fn mixed_delay(seed: u64) -> impl FnMut(Modality, u32, u64) -> Option<u64> {
        let mut per_event: BTreeMap<u32, Option<u64>> = BTreeMap::new();
        let mut rng = Lcg(seed);
        move |m, idx, r| {
            let event = match m {
                Modality::Pc => idx,
                Modality::Haptic => idx / 3,
            };
            let rel = r - T0;
            let base = |rng: &mut Lcg| -> Option<u64> {
                let j = rng.below(15_000);
                if rel < 8_000_000 {
                    Some(40_000 + j)
                } else if rel < 10_000_000 {
                    Some(230_000 + j) // spike: Q increase
                } else if rel < 12_000_000 {
                    if event % 9 == 0 {
                        None // anchor losses, 1 in 9 events
                    } else {
                        Some(50_000 + j)
                    }
                } else if rel < 13_000_000 {
                    if event % 5 == 0 {
                        Some(420_000 + j) // late beyond any due + ε
                    } else {
                        Some(60_000 + j)
                    }
                } else {
                    Some(45_000 + j)
                }
            };
            match m {
                Modality::Haptic if idx % 3 != 0 => Some(30_000 + rng.below(20_000)),
                _ => *per_event.entry(event).or_insert_with(|| base(&mut rng)),
            }
        }
    }

    pub fn params() -> Exp2Params {
        Exp2Params::registered(DMAX)
    }

    /// Per-modality projection of controller-visible decisions.
    #[derive(Debug, PartialEq, Eq)]
    enum Proj {
        Commit(u32, CommittedDue, u64, u64),
        Release(u32, u64, CommittedDue, bool),
        Drop(u32, u64, DropReason, Option<CommittedDue>),
        Miss(u32, u64, CommittedDue),
        Delta(u64, DeltaCause, u64, u64),
    }

    fn project(d: &[Exp2Decision], m: Modality, controller: Option<usize>) -> Vec<Proj> {
        let mut v = Vec::new();
        for x in d {
            match x {
                Exp2Decision::Commit(c) => {
                    let due = match m {
                        Modality::Pc => c.pc,
                        Modality::Haptic => c.haptic[0],
                    };
                    v.push(Proj::Commit(
                        c.event,
                        due,
                        c.hold_g_us[m.ix()],
                        c.compression_us[m.ix()],
                    ));
                }
                Exp2Decision::Release(r) if r.modality == m => {
                    v.push(Proj::Release(r.index, r.t_release_us, r.committed, r.grace))
                }
                Exp2Decision::Drop(r) if r.modality == m => {
                    v.push(Proj::Drop(r.index, r.at_us, r.reason, r.committed))
                }
                Exp2Decision::Miss(r) if r.modality == m => {
                    v.push(Proj::Miss(r.index, r.at_us, r.committed))
                }
                Exp2Decision::DeltaUpdate(u) if Some(u.controller) == controller => {
                    v.push(Proj::Delta(u.at_us, u.cause, u.from_us, u.to_us))
                }
                _ => {}
            }
        }
        v
    }

    fn release_drop_miss(d: &[Exp2Decision]) -> Vec<Exp2Decision> {
        d.iter()
            .filter(|x| {
                matches!(
                    x,
                    Exp2Decision::Release(_) | Exp2Decision::Drop(_) | Exp2Decision::Miss(_)
                )
            })
            .cloned()
            .collect()
    }

    pub fn commits(d: &[Exp2Decision]) -> Vec<CommitRecord> {
        d.iter()
            .filter_map(|x| match x {
                Exp2Decision::Commit(c) => Some(c.clone()),
                _ => None,
            })
            .collect()
    }

    pub fn deltas(d: &[Exp2Decision]) -> Vec<DeltaUpdateRecord> {
        d.iter()
            .filter_map(|x| match x {
                Exp2Decision::DeltaUpdate(u) => Some(u.clone()),
                _ => None,
            })
            .collect()
    }

    fn release_of(d: &[Exp2Decision], m: Modality, index: u32) -> Option<ReleaseRecord> {
        d.iter().find_map(|x| match x {
            Exp2Decision::Release(v) if v.modality == m && v.index == index => Some(v.clone()),
            _ => None,
        })
    }

    // ---------------------------------------------------------------- basics

    #[test]
    fn exp2_rejects_bad_inputs() {
        let mut p = params();
        p.delta_max_us = 50_000;
        assert!(Exp2Playout::new(Exp2Mode::P0, p, ledger(10)).is_err());
        let mut l = ledger(10);
        l.haptic_pts_us.pop();
        assert!(Exp2Playout::new(Exp2Mode::P0, params(), l).is_err());
        let mut core = Exp2Playout::new(Exp2Mode::P0, params(), ledger(10)).unwrap();
        assert!(core.arrive(Modality::Pc, 10, T0).is_err());
        core.arrive(Modality::Pc, 0, T0 + 5).unwrap();
        assert!(core.arrive(Modality::Pc, 1, T0 + 4).is_err(), "time backwards");
        core.advance_to(T0 + 10).unwrap();
        assert!(
            core.arrive(Modality::Pc, 1, T0 + 10).is_err(),
            "arrival at an already-drained µs would break the tie rule"
        );
    }

    #[test]
    fn exp2_initial_commit_uses_delta_max() {
        let l = ledger(30);
        let trace = arrivals(&l, |_, _, _| Some(40_000));
        let r = run(Exp2Mode::P0, params(), &l, &trace);
        let c0 = &commits(&r.decisions)[0];
        assert_eq!((c0.at_us, c0.event_id), (T0, 1));
        assert_eq!(c0.pc.due_us, T0 + DMAX);
        assert!(c0.haptic.iter().all(|h| h.delta_eff_us == DMAX));
    }

    // ------------------------------------------- individual core tests (WP1)

    /// Filler due = t0 + pts_h + Δ_eff,h(i) with the ledger's integer pts,
    /// never recomputed from 11.11 ms: perturb filler pts and check.
    #[test]
    fn exp2_filler_due_uses_ledger_pts_verbatim() {
        let mut l = ledger(60);
        for k in 0..l.haptic_pts_us.len() {
            if k % 3 != 0 {
                l.haptic_pts_us[k] += 7 * (k as u64 % 5) + 1;
            }
        }
        let trace = arrivals(&l, |_, _, _| Some(40_000));
        let r = run(Exp2Mode::P0Np, params(), &l, &trace);
        for c in commits(&r.decisions) {
            for j in 0..3u32 {
                let k = (3 * c.event + j) as usize;
                assert_eq!(
                    c.haptic[j as usize].due_us,
                    T0 + l.haptic_pts_us[k] + c.haptic[j as usize].delta_eff_us
                );
                assert_eq!(c.haptic[j as usize].delta_eff_us, c.haptic[0].delta_eff_us);
            }
        }
        let c0 = &commits(&r.decisions)[0];
        assert!(c0.haptic[0].due_us < c0.haptic[1].due_us && c0.haptic[1].due_us < c0.haptic[2].due_us);
    }

    /// Fillers never enter Q: wildly different filler delays leave every
    /// controller decision and anchor outcome unchanged.
    #[test]
    fn exp2_estimator_samples_are_anchor_only() {
        let l = ledger(600);
        let mk = |filler_delay: u64| {
            let mut inner = mixed_delay(31);
            arrivals(&l, move |m, idx, r| {
                let v = inner(m, idx, r);
                if m == Modality::Haptic && idx % 3 != 0 {
                    Some(filler_delay)
                } else {
                    v
                }
            })
        };
        for mode in [Exp2Mode::P0, Exp2Mode::P0Np] {
            let a = run(mode, params(), &l, &mk(20_000));
            let b = run(mode, params(), &l, &mk(330_000));
            assert_eq!(deltas(&a.decisions), deltas(&b.decisions), "{mode:?}");
            assert_eq!(commits(&a.decisions), commits(&b.decisions));
            assert_eq!(project(&a.decisions, Modality::Pc, None), project(&b.decisions, Modality::Pc, None));
        }
        // sample count equals anchor events only
        let mut core = Exp2Playout::new(Exp2Mode::P0Np, params(), l.clone()).unwrap();
        for k in 0..30u32 {
            core.arrive(Modality::Haptic, k, T0 + l.haptic_pts_us[k as usize] + 1_000).unwrap();
        }
        let at = T0 + 400_000;
        assert_eq!(core.estimator_snapshot(1, at).unwrap().sample_count, 10);
    }

    /// P0: A_i = max(a_pc, a_hap) only for events where both anchors arrived;
    /// no imputation for a missing modality.  P0-NP: own-modality samples.
    #[test]
    fn exp2_p0_complete_pair_max_and_no_imputation() {
        let l = ledger(60);
        let mut p0 = Exp2Playout::new(Exp2Mode::P0, params(), l.clone()).unwrap();
        let mut np = Exp2Playout::new(Exp2Mode::P0Np, params(), l.clone()).unwrap();
        // events 0..10: PC delay 30 ms, haptic 90 ms  → A = 90 ms
        // events 10..15: PC only (haptic anchor missing)
        let mut tr = Vec::new();
        for i in 0..15u32 {
            let r = T0 + l.pc_pts_us[i as usize];
            tr.push((r + 30_000, Modality::Pc, i));
            if i < 10 {
                tr.push((r + 90_000, Modality::Haptic, 3 * i));
            }
        }
        tr.sort();
        for &(t, m, i) in &tr {
            p0.arrive(m, i, t).unwrap();
            np.arrive(m, i, t).unwrap();
        }
        let at = T0 + 499_999;
        p0.advance_to(at).unwrap();
        np.advance_to(at).unwrap();
        let s = p0.estimator_snapshot(0, at).unwrap();
        assert_eq!(s.sample_count, 10, "incomplete events contribute nothing");
        assert_eq!(s.q_us, Some(90_000));
        let spc = np.estimator_snapshot(0, at).unwrap();
        let shp = np.estimator_snapshot(1, at).unwrap();
        assert_eq!((spc.sample_count, spc.q_us), (15, Some(30_000)));
        assert_eq!((shp.sample_count, shp.q_us), (10, Some(90_000)));
        // a late partner completes the P0 sample later (not imputed earlier)
        let r10 = T0 + l.pc_pts_us[10];
        p0.arrive(Modality::Haptic, 30, r10 + 600_000).unwrap();
        let s = p0.estimator_snapshot(0, r10 + 600_000).unwrap();
        assert_eq!(s.sample_count, 11);
        assert_eq!(s.q_us, Some(600_000), "max(a_pc, a_hap) of the completed pair");
    }

    /// Nearest-rank ⌈q·n⌉ over the exact window.
    #[test]
    fn exp2_nearest_rank_quantile() {
        let c = Controller {
            delta_star_us: 0,
            delta_generation: 0,
            miss_window: VecDeque::new(),
            decrease_since_us: None,
            samples: (0..20u32).map(|i| (i, (1_000 + i as u64, (i as i64 + 1) * 10))).collect(),
        };
        // n = 20, ⌈0.95·20⌉ = 19 → 190
        assert_eq!(c.quantile(10_000, 2_000_000, Ratio::new(95, 100)), (Some(190), 20));
        // window (at − W, at]: at = 1_010 + W excludes refs <= 1_010 (i <= 10)
        let (q, n) = c.quantile(1_010 + 2_000_000, 2_000_000, Ratio::new(95, 100));
        assert_eq!(n, 9);
        assert_eq!(q, Some(200)); // ⌈0.95·9⌉ = 9 → max
    }

    /// No pair-release gate: a modality releases at its own due even when
    /// the other modality's anchor is absent.
    #[test]
    fn exp2_no_pair_release_gate() {
        let l = ledger(30);
        let trace = arrivals(&l, |m, idx, _| match (m, idx) {
            (Modality::Pc, 5) => None,
            (Modality::Haptic, 21) => None, // anchor of event 7
            _ => Some(40_000),
        });
        for mode in [Exp2Mode::P0, Exp2Mode::P0Np, Exp2Mode::S3npaPrime] {
            let r = run(mode, params(), &l, &trace);
            let h = release_of(&r.decisions, Modality::Haptic, 15).expect("haptic anchor 15 released");
            assert_eq!(h.t_release_us, h.committed.due_us);
            let p = release_of(&r.decisions, Modality::Pc, 7).expect("pc 7 released");
            assert_eq!(p.t_release_us, p.committed.due_us);
        }
    }

    /// U = 500 ms tick coinciding with ref(30) = t0 + 1 s: the commit uses the
    /// Δ* from before the tick; the next event uses the updated Δ*.
    /// (T_dec = 500 ms here is a mechanism-test input, not a registered value.)
    #[test]
    fn exp2_u_tick_and_ref_same_us_order() {
        let l = ledger(120);
        assert_eq!(l.pc_pts_us[30], 1_000_000);
        let mut p = params();
        p.t_dec_us = 500_000;
        let trace = arrivals(&l, |_, _, _| Some(40_000));
        let r = run(Exp2Mode::P0, p, &l, &trace);
        let ds = deltas(&r.decisions);
        assert_eq!(ds[0].at_us, T0 + 1_000_000);
        assert_eq!((ds[0].cause, ds[0].from_us, ds[0].to_us), (DeltaCause::QDecrease, DMAX, 100_000));
        let cs = commits(&r.decisions);
        assert_eq!(cs[30].pc.delta_eff_us, DMAX, "ref(30) commit precedes the same-µs tick");
        assert_eq!(cs[30].pc.delta_generation, 0);
        assert_eq!(cs[31].pc.delta_eff_us, DMAX - 33_333 / 10, "compression from event 31");
        assert_eq!(cs[31].pc.delta_generation, 1);
        let pos_c = r.decisions.iter().position(|x| matches!(x, Exp2Decision::Commit(c) if c.event == 30)).unwrap();
        let pos_u = r.decisions.iter().position(|x| matches!(x, Exp2Decision::DeltaUpdate(_))).unwrap();
        assert!(pos_c < pos_u);
        // An arrival at exactly the tick µs is visible to that tick.
        let tick = T0 + 500_000;
        let mut a = Exp2Playout::new(Exp2Mode::P0, params(), l.clone()).unwrap();
        let mut b = Exp2Playout::new(Exp2Mode::P0, params(), l.clone()).unwrap();
        for core in [&mut a, &mut b] {
            core.arrive(Modality::Haptic, 42, T0 + 470_000).unwrap(); // anchor of event 14
        }
        a.arrive(Modality::Pc, 14, tick).unwrap();
        a.advance_to(tick).unwrap();
        b.advance_to(tick).unwrap();
        b.arrive(Modality::Pc, 14, tick + 1).unwrap();
        assert_eq!(a.estimator_snapshot(0, tick).unwrap().decrease_since_us, Some(tick));
        assert_eq!(b.estimator_snapshot(0, tick).unwrap().decrease_since_us, None);
    }

    // ---------------------------------------------------------------- gate 1

    #[test]
    fn exp2_gate1_p0_equals_p0np_on_identical_modal_traces() {
        let l = ledger(1200);
        let trace = arrivals(&l, mixed_delay(7));
        let a = run(Exp2Mode::P0, params(), &l, &trace);
        let b = run(Exp2Mode::P0Np, params(), &l, &trace);
        let causes: BTreeSet<_> = deltas(&a.decisions).iter().map(|u| u.cause.as_str()).collect();
        assert!(
            causes.contains("miss_step") && causes.contains("q_increase") && causes.contains("q_decrease"),
            "non-vacuous: {causes:?}"
        );
        assert_eq!(project(&a.decisions, Modality::Pc, Some(0)), project(&b.decisions, Modality::Pc, Some(0)));
        assert_eq!(
            project(&a.decisions, Modality::Haptic, Some(0)),
            project(&b.decisions, Modality::Haptic, Some(1))
        );
        assert_eq!(a.ledger, b.ledger);
        for m in Modality::ALL {
            assert_eq!(a.core.hold_summary(m), b.core.hold_summary(m));
        }
    }

    // ---------------------------------------------------------------- gate 2

    #[test]
    fn exp2_gate2_causality_future_arrivals_do_not_change_past_decisions() {
        let l = ledger(600);
        let trace = arrivals(&l, mixed_delay(11));
        for mode in [Exp2Mode::P0, Exp2Mode::P0Np, Exp2Mode::S3npaPrime] {
            let a = run(mode, params(), &l, &trace);
            for cut_rel in [3_000_000u64, 9_100_000, 10_500_001, 12_345_678] {
                let t_star = T0 + cut_rel;
                let mut rng = Lcg(cut_rel);
                let mut altered: Vec<Arrival> = trace
                    .iter()
                    .filter_map(|&(t, m, i)| {
                        if t < t_star {
                            Some((t, m, i))
                        } else if rng.below(3) == 0 {
                            None
                        } else {
                            Some((t + rng.below(300_000), m, i))
                        }
                    })
                    .collect();
                altered.sort();
                let b = run(mode, params(), &l, &altered);
                let before = |d: &[Exp2Decision]| -> Vec<Exp2Decision> {
                    d.iter().filter(|x| x.at_us() < t_star).cloned().collect()
                };
                assert_eq!(before(&a.decisions), before(&b.decisions), "{mode:?} t*={cut_rel}");
                assert!(a.decisions != b.decisions, "alteration must matter somewhere");
            }
        }
    }

    // ---------------------------------------------------------------- gate 3

    #[test]
    fn exp2_gate3_committed_due_immutable_and_p0_atomic() {
        let l = ledger(1200);
        let trace = arrivals(&l, mixed_delay(3));
        for mode in [Exp2Mode::P0, Exp2Mode::P0Np, Exp2Mode::S3npaPrime] {
            let r = run(mode, params(), &l, &trace);
            let cs = commits(&r.decisions);
            assert_eq!(cs.len(), 1200);
            let mut due: BTreeMap<(Modality, u32), CommittedDue> = BTreeMap::new();
            for (k, c) in cs.iter().enumerate() {
                assert_eq!(c.event as usize, k, "exactly one commit per event, in order");
                assert_eq!(c.event_id as usize, k + 1);
                assert_eq!(c.at_us, T0 + l.pc_pts_us[k]);
                if mode != Exp2Mode::P0Np {
                    assert_eq!(c.pc.delta_eff_us, c.haptic[0].delta_eff_us, "P0 atomic shared Δ");
                    assert_eq!(c.pc.delta_generation, c.haptic[0].delta_generation);
                }
                assert_eq!(c.pc.due_us, c.at_us + c.pc.delta_eff_us);
                due.insert((Modality::Pc, c.event), c.pc);
                for j in 0..3u32 {
                    due.insert((Modality::Haptic, 3 * c.event + j), c.haptic[j as usize]);
                }
            }
            for x in &r.decisions {
                let (key, cd) = match x {
                    Exp2Decision::Release(v) => ((v.modality, v.index), v.committed),
                    Exp2Decision::Miss(v) => ((v.modality, v.index), v.committed),
                    Exp2Decision::Drop(v) => match v.committed {
                        Some(cd) => ((v.modality, v.index), cd),
                        None => continue,
                    },
                    _ => continue,
                };
                assert_eq!(due[&key], cd, "decision used a due other than the committed one");
                if let Exp2Decision::Release(v) = x {
                    assert!(v.t_release_us <= v.committed.due_us + params().eps_release_us);
                    assert!(v.t_release_us == v.committed.due_us || v.grace);
                }
            }
            for m in Modality::ALL {
                for (i, e) in r.ledger.entries(m).iter().enumerate() {
                    assert_eq!(e.committed, Some(due[&(m, i as u32)]));
                }
            }
        }
    }

    // ---------------------------------------------------------------- gate 4

    #[test]
    fn exp2_gate4_compression_bounds_due_monotone_delta_bounds() {
        let l = ledger(1200);
        let trace = arrivals(&l, mixed_delay(5));
        for mode in [Exp2Mode::P0, Exp2Mode::P0Np] {
            let r = run(mode, params(), &l, &trace);
            let cs = commits(&r.decisions);
            let p = params();
            let mut saw_compression = false;
            let mut saw_hold = false;
            let mut hap_dues = Vec::new();
            for (k, c) in cs.iter().enumerate() {
                for m in Modality::ALL {
                    let d = c.delta_eff_us(m);
                    assert!(d <= p.delta_max_us && d >= p.d_min_us);
                    if k > 0 {
                        let prev = cs[k - 1].delta_eff_us(m);
                        let interval = l.pc_pts_us[k] - l.pc_pts_us[k - 1];
                        assert!(d + interval / 10 >= prev, "compression > 10 %");
                        assert_eq!(c.hold_g_us[m.ix()], d.saturating_sub(prev));
                        saw_compression |= d < prev;
                        saw_hold |= d > prev;
                    }
                }
                if k > 0 {
                    assert!(c.pc.due_us > cs[k - 1].pc.due_us);
                }
                hap_dues.extend(c.haptic.iter().map(|h| h.due_us));
            }
            assert!(hap_dues.windows(2).all(|w| w[1] > w[0]), "all 3600 haptic dues strictly increasing");
            assert!(saw_compression && saw_hold, "non-vacuous");
            let hs = r.core.hold_summary(Modality::Pc);
            assert!(hs.hold_step_count > 0 && hs.hold_episode_count > 0);
            assert!(hs.hold_episode_count <= hs.hold_step_count);
        }
    }

    #[test]
    fn exp2_hold_metrics_match_definition() {
        let l = ledger(1200);
        let r = run(Exp2Mode::P0, params(), &l, &arrivals(&l, mixed_delay(5)));
        let cs = commits(&r.decisions);
        let seq: Vec<u64> = cs.iter().map(|c| c.pc.delta_eff_us).collect();
        let (mut episodes, mut in_ep) = (0, false);
        for w in seq.windows(2) {
            if w[1] > w[0] {
                if !in_ep {
                    episodes += 1;
                }
                in_ep = true;
            } else if w[1] < w[0] {
                in_ep = false;
            }
        }
        let hs = r.core.hold_summary(Modality::Pc);
        assert_eq!(hs.hold_episode_count, episodes);
        assert_eq!(hs.total_added_hold_us, cs.iter().map(|c| c.hold_g_us[0]).sum::<u64>());
        assert_eq!(hs.hold_step_count, cs.iter().filter(|c| c.hold_g_us[0] > 0).count() as u64);
    }

    // ---------------------------------------------------------------- gate 5

    #[test]
    fn exp2_gate5_p0_pinned_at_delta_max_equals_s3npa_prime() {
        let l = ledger(1200);
        // (a) pinned by D_min = Δ_max on the mixed trace
        let trace = arrivals(&l, mixed_delay(9));
        let mut pinned = params();
        pinned.d_min_us = DMAX;
        let a = run(Exp2Mode::P0, pinned, &l, &trace);
        let s = run(Exp2Mode::S3npaPrime, params(), &l, &trace);
        let strip_gen = |d: &[Exp2Decision]| -> Vec<Exp2Decision> { release_drop_miss(d) };
        assert_eq!(strip_gen(&a.decisions), strip_gen(&s.decisions));
        assert_eq!(commits(&a.decisions), commits(&s.decisions));
        assert_eq!(a.ledger, s.ledger);
        // (b) pinned by the trace under registered params: every anchor delay
        // keeps Q + m >= Δ_max; some anchors are lost so zero-increment steps fire.
        let mut rng = Lcg(99);
        let trace_b = arrivals(&l, |m, idx, _| match m {
            Modality::Haptic if idx % 3 != 0 => Some(20_000 + rng.below(10_000)),
            _ if idx % 12 == 7 => None, // 5 per 60 anchors: steps fire
            _ => Some(330_000 + rng.below(10_000)),
        });
        let b = run(Exp2Mode::P0, params(), &l, &trace_b);
        let sb = run(Exp2Mode::S3npaPrime, params(), &l, &trace_b);
        assert!(deltas(&b.decisions).iter().all(|u| u.to_us == DMAX));
        assert!(deltas(&b.decisions).iter().any(|u| u.cause == DeltaCause::MissStep));
        assert_eq!(release_drop_miss(&b.decisions), release_drop_miss(&sb.decisions));
        assert_eq!(commits(&b.decisions), commits(&sb.decisions));
        assert_eq!(b.ledger, sb.ledger);
    }

    // ------------------------------------------- gate 6 (core-side only)

    /// Core-side part of gate 6 only: t0 is a constructor input with no
    /// setter, and translating t0 and every input by a constant translates
    /// every decision by that constant.  "t0 unchanged across route
    /// generation" is a WP3 integration test.
    #[test]
    fn exp2_gate6_core_t0_translation_invariance() {
        let shift = 123_457u64;
        let l = ledger(600);
        let l2 = ledger_at(T0 + shift, 600);
        let trace = arrivals(&l, mixed_delay(13));
        let trace2: Vec<Arrival> = trace.iter().map(|&(t, m, i)| (t + shift, m, i)).collect();
        for mode in [Exp2Mode::P0, Exp2Mode::P0Np, Exp2Mode::S3npaPrime] {
            let a = run(mode, params(), &l, &trace);
            let b = run(mode, params(), &l2, &trace2);
            assert_eq!(b.core.t0_us(), T0 + shift);
            assert_eq!(a.decisions.len(), b.decisions.len());
            for (x, y) in a.decisions.iter().zip(b.decisions.iter()) {
                assert_eq!(x.at_us() + shift, y.at_us());
                match (x, y) {
                    (Exp2Decision::Commit(p), Exp2Decision::Commit(q)) => {
                        assert_eq!(p.pc.delta_eff_us, q.pc.delta_eff_us);
                        assert_eq!(p.pc.due_us + shift, q.pc.due_us);
                    }
                    _ => assert_eq!(std::mem::discriminant(x), std::mem::discriminant(y)),
                }
            }
        }
    }

    // ------------------------------------------- gate 7 (core-side only)

    #[test]
    fn exp2_gate7_core_buffer_bounds() {
        let l = ledger(300);
        let trace = arrivals(&l, |_, _, _| Some(5_000)); // everything early
        let mut p = params();
        p.max_objects_per_modality = 4;
        let r = run(Exp2Mode::S3npaPrime, p, &l, &trace);
        assert!(r.core.max_occupancy(Modality::Pc) <= 4);
        assert!(r.core.max_occupancy(Modality::Haptic) <= 4);
        assert!(r.ledger.count(Modality::Pc, "dropped") > 0);
        assert!(r.ledger.pc.iter().all(|e| match e.terminal {
            SchedulerTerminal::Dropped { reason, .. } =>
                reason == DropReason::BufferBound(BufferBound::ObjectLimit),
            _ => true,
        }));
        let mut p = params();
        p.max_span_us = 100_000;
        let r = run(Exp2Mode::S3npaPrime, p, &l, &trace);
        assert!(r.decisions.iter().any(|x| matches!(x, Exp2Decision::Drop(d)
            if d.reason == DropReason::BufferBound(BufferBound::SpanLimit))));
        // registered bounds never bind on a normal trace
        let r = run(Exp2Mode::P0, params(), &l, &trace);
        assert!(!r.decisions.iter().any(|x| matches!(x, Exp2Decision::Drop(_))));
        assert!(r.core.max_occupancy(Modality::Pc) > 0);
    }

    #[test]
    fn exp2_gate7_core_every_opportunity_exactly_one_scheduler_terminal() {
        let l = ledger(1200);
        let trace = arrivals(&l, mixed_delay(17));
        for mode in [Exp2Mode::P0, Exp2Mode::P0Np, Exp2Mode::S3npaPrime] {
            let r = run(mode, params(), &l, &trace);
            assert_eq!(r.ledger.pc.len(), 1200);
            assert_eq!(r.ledger.haptic.len(), 3600);
            for m in Modality::ALL {
                let total: usize = ["released", "dropped", "pending_at_horizon", "no_object"]
                    .iter()
                    .map(|s| r.ledger.count(m, s))
                    .sum();
                assert_eq!(total, r.ledger.entries(m).len());
                // Δ_max < B_play ⇒ due < H: pending_at_horizon structurally empty
                assert_eq!(r.ledger.count(m, "pending_at_horizon"), 0);
                assert!(r.core.slots[m.ix()].iter().all(|s| s.terminal.is_some()));
            }
            let pairs: Vec<u32> = r.decisions.iter().filter_map(|x| match x {
                Exp2Decision::PairComplete(p) => Some(p.event),
                _ => None,
            }).collect();
            assert_eq!(pairs, (0..1200).collect::<Vec<_>>(), "pair results in planned order");
            assert!(r.core.next_wakeup_us().is_none());
            assert!(r.ledger.count(Modality::Pc, "no_object") > 0);
            assert!(r.ledger.count(Modality::Pc, "dropped") > 0);
        }
    }

    #[test]
    fn exp2_gate7_core_metamorphic_fillers_and_finalize() {
        let l = ledger(600);
        let base = arrivals(&l, mixed_delay(21));
        let a = run(Exp2Mode::P0, params(), &l, &base);
        let due_of = |m: Modality, i: u32| a.ledger.entries(m)[i as usize].committed.unwrap().due_us;
        // (i) jitter every on-time filler within [ref, due]: nothing changes
        // except that filler's t_recv.
        let mut rng = Lcg(5);
        let mut jittered: Vec<Arrival> = base
            .iter()
            .map(|&(t, m, i)| {
                if m == Modality::Haptic && i % 3 != 0 && t <= due_of(m, i) {
                    let r = T0 + l.haptic_pts_us[i as usize];
                    (r + rng.below(due_of(m, i) - r + 1), m, i)
                } else {
                    (t, m, i)
                }
            })
            .collect();
        jittered.sort();
        let b = run(Exp2Mode::P0, params(), &l, &jittered);
        assert_eq!(a.ledger, b.ledger);
        let norm = |d: &[Exp2Decision]| -> Vec<String> {
            let mut v: Vec<String> = d
                .iter()
                .cloned()
                .map(|x| match x {
                    Exp2Decision::Release(mut r) => {
                        r.t_recv_us = 0;
                        format!("{:?}", Exp2Decision::Release(r))
                    }
                    other => format!("{other:?}"),
                })
                .collect();
            v.sort();
            v
        };
        assert_eq!(norm(&a.decisions), norm(&b.decisions));
        // (ii) one filler delivered after its c: only its own terminal flips
        let victim = 3 * 100 + 1;
        let c = due_of(Modality::Haptic, victim) + params().eps_release_us;
        let mut late: Vec<Arrival> = base
            .iter()
            .map(|&(t, m, i)| if m == Modality::Haptic && i == victim { (c + 1, m, i) } else { (t, m, i) })
            .collect();
        late.sort();
        let cc = run(Exp2Mode::P0, params(), &l, &late);
        for m in Modality::ALL {
            for (i, (x, y)) in a.ledger.entries(m).iter().zip(cc.ledger.entries(m)).enumerate() {
                if m == Modality::Haptic && i as u32 == victim {
                    assert!(matches!(y.terminal, SchedulerTerminal::Dropped { reason: DropReason::Late, .. }));
                } else {
                    assert_eq!(x, y);
                }
            }
        }
        assert_eq!(project(&a.decisions, Modality::Pc, Some(0)), project(&cc.decisions, Modality::Pc, Some(0)));
        // (iii) inputs after finalize are ignored
        let mut core = Exp2Playout::new(Exp2Mode::P0, params(), l.clone()).unwrap();
        let (_, sealed) = core.finalize(horizon(&l)).unwrap();
        let d = core.arrive(Modality::Pc, 0, horizon(&l) + 1).unwrap();
        assert!(matches!(d[0], Exp2Decision::Diagnostic(DiagnosticRecord { kind: DiagnosticKind::InputAfterFinalize, .. })));
        assert_eq!(core.finalized.as_ref().unwrap(), &sealed);
        assert!(core.advance_to(horizon(&l) + 2).is_err());
        // (iv) duplicate arrival is a diagnostic only
        let mut core = Exp2Playout::new(Exp2Mode::P0, params(), l.clone()).unwrap();
        core.arrive(Modality::Pc, 0, T0 + 1_000).unwrap();
        let d = core.arrive(Modality::Pc, 0, T0 + 2_000).unwrap();
        assert!(matches!(d[0], Exp2Decision::Diagnostic(DiagnosticRecord { kind: DiagnosticKind::DuplicateArrival, .. })));
    }

    #[test]
    fn exp2_pending_at_horizon_with_oversized_delta_max() {
        // Δ_max deliberately > B_play (not a registered value) to exercise the
        // state; under registered constants it is structurally empty.
        let l = ledger(30);
        let trace = arrivals(&l, |m, i, _| if m == Modality::Pc && i == 29 { None } else { Some(10_000) });
        let r = run(Exp2Mode::S3npaPrime, Exp2Params::registered(600_000), &l, &trace);
        let h = horizon(&l);
        assert!(r.ledger.count(Modality::Pc, "pending_at_horizon") > 0);
        for e in &r.ledger.pc {
            match e.terminal {
                SchedulerTerminal::PendingAtHorizon => assert!(e.committed.unwrap().due_us > h),
                SchedulerTerminal::Released { t_release_us, .. } => assert!(t_release_us <= h),
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(r.ledger.pc[29].terminal, SchedulerTerminal::PendingAtHorizon, "not arrived, due > H");
    }

    #[test]
    fn exp2_delivery_timeout_is_label_only() {
        let l = ledger(30);
        let trace = arrivals(&l, |m, i, _| if m == Modality::Pc && i == 5 { None } else { Some(20_000) });
        let base = run(Exp2Mode::P0, params(), &l, &trace);
        let mut core = Exp2Playout::new(Exp2Mode::P0, params(), l.clone()).unwrap();
        let mut dec = Vec::new();
        let t_wire = T0 + l.pc_pts_us[5] + 50_000;
        let mut injected = false;
        for &(t, m, i) in &trace {
            if !injected && t > t_wire {
                dec.extend(core.delivery_timeout(Modality::Pc, 5, t_wire).unwrap());
                injected = true;
            }
            dec.extend(core.arrive(m, i, t).unwrap());
        }
        let (tail, sealed) = core.finalize(horizon(&l)).unwrap();
        dec.extend(tail);
        assert!(matches!(sealed.pc[5].terminal, SchedulerTerminal::Dropped { reason: DropReason::DeliveryTimeout, .. }));
        assert_eq!(base.ledger.pc[5].terminal, SchedulerTerminal::NoObject);
        // controller-visible stream unchanged (miss still at c)
        let strip = |d: &[Exp2Decision]| -> Vec<Exp2Decision> {
            d.iter().filter(|x| !matches!(x, Exp2Decision::Drop(r) if r.reason == DropReason::DeliveryTimeout)).cloned().collect()
        };
        assert_eq!(strip(&dec), strip(&base.decisions));
    }

    // ------------------------------------------------ §3 stability assertions

    pub fn step_times(d: &[Exp2Decision], controller: usize) -> Vec<u64> {
        deltas(d)
            .iter()
            .filter(|u| u.cause == DeltaCause::MissStep && u.controller == controller)
            .map(|u| u.at_us)
            .collect()
    }

    fn pair_consumptions(d: &[Exp2Decision]) -> Vec<u64> {
        d.iter()
            .filter_map(|x| match x {
                Exp2Decision::PairComplete(p) => Some(p.at_us),
                _ => None,
            })
            .collect()
    }

    /// Steady ~50 ms delays with the PC anchors of `lost` events never arriving.
    pub fn loss_trace(l: &Exp2OpportunityLedger, lost: impl Fn(u32) -> bool) -> Vec<Arrival> {
        let mut rng = Lcg(1);
        arrivals(l, |m, idx, _| {
            let j = rng.below(10_000);
            match m {
                Modality::Pc if lost(idx) => None,
                _ => Some(45_000 + j),
            }
        })
    }

    #[test]
    fn exp2_stability_q_unchanged_by_step() {
        let l = ledger(1200);
        let trace = loss_trace(&l, |i| (300..304).contains(&i));
        let r = run(Exp2Mode::P0, params(), &l, &trace);
        let steps = step_times(&r.decisions, 0);
        assert_eq!(steps.len(), 1);
        let t_step = steps[0];
        let mut core = Exp2Playout::new(Exp2Mode::P0, params(), l.clone()).unwrap();
        for &(t, m, i) in trace.iter().filter(|a| a.0 < t_step) {
            core.arrive(m, i, t).unwrap();
        }
        core.advance_to(t_step - 1).unwrap();
        let before = core.estimator_snapshot(0, t_step).unwrap();
        let out = core.advance_to(t_step).unwrap();
        assert!(out.iter().any(|x| matches!(x, Exp2Decision::DeltaUpdate(u) if u.cause == DeltaCause::MissStep)));
        let after = core.estimator_snapshot(0, t_step).unwrap();
        assert_eq!(before.q_us, after.q_us, "Q value unchanged by the step");
        assert_eq!(before.sample_count, after.sample_count, "Q sample window kept");
        assert_eq!((before.miss_window_len, before.miss_window_misses), (60, 4));
        assert_eq!(after.miss_window_len, 0, "evidence window consumed");
        assert_eq!(after.delta_star_us, before.delta_star_us + 50_000.min(DMAX - before.delta_star_us));
    }

    #[test]
    fn exp2_stability_four_miss_burst_consumed_once_impulse_and_three_ignored() {
        let l = ledger(1200);
        let r = run(Exp2Mode::P0, params(), &l, &loss_trace(&l, |i| (300..304).contains(&i)));
        assert_eq!(step_times(&r.decisions, 0).len(), 1, "4-miss burst consumed exactly once");
        let r = run(Exp2Mode::P0, params(), &l, &loss_trace(&l, |i| i == 300));
        assert!(step_times(&r.decisions, 0).is_empty(), "impulse miss never steps");
        let r = run(Exp2Mode::P0, params(), &l, &loss_trace(&l, |i| (300..303).contains(&i)));
        assert!(step_times(&r.decisions, 0).is_empty(), "3/60 is not > 0.05");
    }

    #[test]
    fn exp2_stability_no_refire_before_60_new_completions_and_recovery() {
        let l = ledger(1200);
        let r = run(Exp2Mode::P0, params(), &l, &loss_trace(&l, |i| (300..700).contains(&i)));
        let steps = step_times(&r.decisions, 0);
        assert!(steps.len() >= 2, "sustained loss re-fires: {steps:?}");
        let comps = pair_consumptions(&r.decisions);
        for w in steps.windows(2) {
            let n = comps.iter().filter(|&&t| t > w[0] && t <= w[1]).count();
            assert!(n >= 60, "re-fired after only {n} new completions");
        }
        let reached_max = deltas(&r.decisions).iter().any(|u| u.to_us == DMAX);
        let last = commits(&r.decisions).last().unwrap().pc.delta_eff_us;
        eprintln!(
            "[record] sustained PC loss events 300..700: miss steps={}, Δ*→Δ_max={}, final Δ_eff={} µs",
            steps.len(),
            reached_max,
            last
        );
        assert!(last < DMAX, "Δ leaves Δ_max after the loss ends (not stuck)");
    }

    #[test]
    fn exp2_stability_alternating_loss_around_threshold() {
        let l = ledger(1200);
        let r3 = run(Exp2Mode::P0, params(), &l, &loss_trace(&l, |i| i % 20 == 10));
        assert!(step_times(&r3.decisions, 0).is_empty(), "3 per 60 never steps");
        let r4 = run(Exp2Mode::P0, params(), &l, &loss_trace(&l, |i| i % 15 == 10));
        let s = step_times(&r4.decisions, 0);
        let traj: Vec<(u64, &str, u64)> = deltas(&r4.decisions)
            .iter()
            .map(|u| ((u.at_us - T0) / 1000, u.cause.as_str(), u.to_us))
            .collect();
        eprintln!("[record] 4-per-60 periodic PC loss: {} miss steps; Δ* trajectory (ms, cause, µs) = {traj:?}", s.len());
        assert!(!s.is_empty());
    }

    /// Truly alternating loss around the threshold: 60-event blocks with 3
    /// then 5 PC-anchor losses.  Records the trajectory; asserts determinism
    /// only (outcome is reported, parameters are not tuned).
    #[test]
    fn exp2_stability_record_alternating_3_and_5_per_60_blocks() {
        let l = ledger(1200);
        let lost = |i: u32| {
            let (block, pos) = (i / 60, i % 60);
            let k = if block % 2 == 0 { 3 } else { 5 };
            pos % 12 == 5 && pos / 12 < k
        };
        let a = run(Exp2Mode::P0, params(), &l, &loss_trace(&l, lost));
        let b = run(Exp2Mode::P0, params(), &l, &loss_trace(&l, lost));
        assert_eq!(a.decisions, b.decisions);
        let traj: Vec<(u64, &str, u64)> = deltas(&a.decisions)
            .iter()
            .map(|u| ((u.at_us - T0) / 1000, u.cause.as_str(), u.to_us))
            .collect();
        let at_max = commits(&a.decisions).iter().filter(|c| c.pc.delta_eff_us == DMAX).count();
        eprintln!(
            "[record] alternating 3/60,5/60 blocks: {} miss steps; events committed at Δ_max = {at_max}/1200; Δ* trajectory (ms, cause, µs) = {traj:?}",
            step_times(&a.decisions, 0).len()
        );
    }

    #[test]
    fn exp2_stability_p0np_windows_independent() {
        let l = ledger(1200);
        let lossy = run(Exp2Mode::P0Np, params(), &l, &loss_trace(&l, |i| (300..400).contains(&i)));
        let clean = run(Exp2Mode::P0Np, params(), &l, &loss_trace(&l, |_| false));
        assert!(!step_times(&lossy.decisions, 0).is_empty(), "PC controller stepped");
        assert!(step_times(&lossy.decisions, 1).is_empty(), "haptic controller did not");
        assert_eq!(
            project(&lossy.decisions, Modality::Haptic, Some(1)),
            project(&clean.decisions, Modality::Haptic, Some(1)),
            "haptic stream identical to the no-PC-loss run"
        );
    }

    #[test]
    fn exp2_stability_deterministic_replay() {
        let l = ledger(1200);
        let trace = arrivals(&l, mixed_delay(7));
        for mode in [Exp2Mode::P0, Exp2Mode::P0Np, Exp2Mode::S3npaPrime] {
            let a = run(mode, params(), &l, &trace);
            let b = run(mode, params(), &l, &trace);
            assert_eq!(a.decisions, b.decisions);
            assert_eq!(a.ledger, b.ledger);
        }
    }

    #[test]
    fn exp2_stability_grace_arrival_consistency() {
        let l = ledger(30);
        let eps = params().eps_release_us;
        let due5 = T0 + l.pc_pts_us[5] + DMAX; // early events keep Δ_max
        for (offset, released, grace) in
            [(0u64, true, false), (1, true, true), (eps, true, true), (eps + 1, false, false)]
        {
            let mut trace = arrivals(&l, |m, i, _| if m == Modality::Pc && i == 5 { None } else { Some(20_000) });
            trace.push((due5 + offset, Modality::Pc, 5));
            trace.sort();
            let r = run(Exp2Mode::P0, params(), &l, &trace);
            let rel = release_of(&r.decisions, Modality::Pc, 5);
            let pair = r.decisions.iter().find_map(|x| match x {
                Exp2Decision::PairComplete(p) if p.event == 5 => Some(p.clone()),
                _ => None,
            }).unwrap();
            assert_eq!(rel.is_some(), released, "offset {offset}");
            assert_eq!(pair.pc_miss, !released, "grace release is not a miss");
            if let Some(v) = rel {
                assert_eq!(v.grace, grace);
                assert_eq!(v.t_release_us, due5 + offset);
            } else {
                let miss = r.decisions.iter().any(|x| matches!(x, Exp2Decision::Miss(m)
                    if m.index == 5 && m.modality == Modality::Pc && m.at_us == due5 + eps));
                let drop = r.decisions.iter().any(|x| matches!(x, Exp2Decision::Drop(d)
                    if d.index == 5 && d.modality == Modality::Pc && d.reason == DropReason::Late));
                assert!(miss && drop, "miss at c then late drop on arrival");
                assert!(matches!(r.ledger.pc[5].terminal, SchedulerTerminal::Dropped { reason: DropReason::Late, .. }));
            }
        }
    }
}
