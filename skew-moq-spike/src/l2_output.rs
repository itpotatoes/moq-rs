//! L2 output stage, milestone M1 (device-free).
//!
//! Governing documents: `md/20261022_실험1D_L2사다리_사전등록_문안.md` §3-1,
//! §3-5, §3-6, §6, §6-1; `md/20261022_L2_계측_설계초안.md` §2, §7; contract
//! `md/20260929_실험2_계약.md` §5–§6 and §9 (row roles `submit` / `output` /
//! `explicit_discard`, `t_submit_us`, `t_output_us`, `t_discard_us`,
//! `t_log_arrival_us` = writer clock immediately before the row is written,
//! `instrumentation_end.max_write_lag_us`, final `instrumentation_end`).
//!
//! Shape:
//!
//! ```text
//!  release point (scheduler / B1 drain)            sink thread                 writer thread (owns the file)
//!  L2Submitter::submit() --try_send(Item)--> [bounded submission queue] --> Submit row ─┐
//!     │ (Full)                                     NullSink: nothing more              │
//!     └─try_send(Discard before submit)─────────────────────────────────────────────>  [bounded row queue] --> L2 JSONL
//!  receiver_fault() (first fatal receiver error) ──────────────────────────────────>  │
//!                                                  FakeSink: Output (t_callback_us) ──┘
//! ```
//!
//! * The release side only ever calls `try_send` (no file I/O, no JSON, no
//!   logger mutex). A full submission queue is recorded as
//!   `explicit_discard(output_queue_full)` BEFORE submit; it never becomes an
//!   error of the release path.
//! * Every object row carries the ORIGINAL identity of the released object
//!   (run_id, track, tier, seq, pts_us, event_id and, for S3, wire_track +
//!   route_generation). Nothing is rewritten from a ledger.
//! * The writer stamps `t_log_arrival_us` (and `t_us`) immediately before it
//!   writes each row and measures the write lag (`max_write_lag_us`, contract
//!   §9). The sink's own receipt time of a terminal callback is kept apart as
//!   `t_callback_us`.
//! * Integrity is checked by construction and recorded in the sentinel:
//!   duplicate logical releases (same `(track, seq)`, any route), terminals
//!   without an open submit, conservation `submitted == output +
//!   discarded_after_submit + submitted_unresolved`, unresolved submits
//!   (non-null sinks), bounded-tracking overflow, row-queue overflow,
//!   submissions after close, producers still alive at finish. The first
//!   failure's time and reason are kept (`first_integrity_failure_*`).
//! * `instrumentation_end` is written only by [`L2Output::finish`]: after the
//!   submission side is closed, every producer handle is gone (channel
//!   disconnected; otherwise `producers_alive_at_finish`), the close grace has
//!   run, and the sink thread has been joined. It is the last row because the
//!   writer exits right after writing it. A run that ends without `finish`
//!   has no sentinel — fail closed.
//! * All times are integer microseconds of `CLOCK_MONOTONIC` ([`crate::now_us`]).
//!
//! `NullSink` accepts submissions and never emits a terminal: it exists only
//! to measure the overhead of the submission path; its submissions are
//! unresolved by design (meta `null_sink_never_resolves`), which is not an
//! integrity failure. `FakeSink` emits a deterministic synthetic `output` with
//! `t_output_us = t_submit_us + offset(modality)`; it handles the deferred
//! output at a real clock read `t_callback_us >= t_output_us`. Neither is a
//! device output (meta `device_output: false`).
//!
//! The close grace (`report_window_us`, meta `report_window_anchor: "close"`)
//! starts when `finish` begins. It is NOT the registration's H-anchored
//! `G_report` (§3-6); during the grace a terminal is accepted only if its real
//! receipt time `t_callback_us` is strictly before the grace deadline.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Barrier, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::{bail, Context, Result};

use crate::now_us;

/// L2 JSONL schema tag written into the meta row. v1 (Codex 104 fixes):
/// `t_log_arrival_us` is the writer's pre-write clock, `t_callback_us` is new,
/// the sentinel carries the conservation/duplicate/failure-time fields.
pub const L2_SCHEMA: &str = "event-pair-l2-v1";
pub const L2_CLOCK: &str = "CLOCK_MONOTONIC";
/// The M1 grace is anchored at `finish`, not at the registration's H.
pub const REPORT_WINDOW_ANCHOR: &str = "close";

pub const DISCARD_OUTPUT_QUEUE_FULL: &str = "output_queue_full";
/// Reserved for the PC renderer (M3): a not-yet-committed frame replaced by a
/// newer one. Never used by the M1 sinks.
pub const DISCARD_SUPERSEDED_BEFORE_COMMIT: &str = "superseded_before_commit";
/// Reserved for the PC renderer (M3): `wp_presentation_feedback.discarded`.
pub const DISCARD_COMPOSITOR_DISCARDED: &str = "compositor_discarded";

/// `first_integrity_failure_reason` values.
pub const FAIL_ROW_QUEUE_OVERFLOW: &str = "row_queue_overflow";
pub const FAIL_SUBMIT_AFTER_CLOSE: &str = "submit_after_close";
pub const FAIL_SINK_GONE: &str = "sink_gone";
pub const FAIL_DUPLICATE_LOGICAL_RELEASE: &str = "duplicate_logical_release";
pub const FAIL_TERMINAL_WITHOUT_OPEN_SUBMIT: &str = "terminal_without_open_submit";
pub const FAIL_TRACKING_CAPACITY: &str = "tracking_capacity_exceeded";
pub const FAIL_FAKE_PENDING_OVERFLOW: &str = "fake_pending_overflow";
pub const FAIL_PRODUCERS_ALIVE_AT_FINISH: &str = "producers_alive_at_finish";
pub const FAIL_SUBMITTED_UNRESOLVED: &str = "submitted_unresolved";
pub const FAIL_CONSERVATION: &str = "submit_conservation_violated";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum L2SinkKind {
    Null,
    Fake,
}

impl L2SinkKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Fake => "fake",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum L2Modality {
    Pc,
    Haptic,
}

impl L2Modality {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pc => "pc",
            Self::Haptic => "haptic",
        }
    }

    /// Map the logical track name used in the rx log (`"pc"`/`"haptic"`).
    pub fn from_track(track: &str) -> Option<Self> {
        match track {
            "pc" => Some(Self::Pc),
            "haptic" => Some(Self::Haptic),
            _ => None,
        }
    }
}

/// The original identity of one released object, copied from the release
/// point. `route` is `Some((wire_track, route_generation))` only for S3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct L2Identity {
    pub modality: L2Modality,
    pub tier: u16,
    pub seq: u32,
    pub pts_us: u64,
    pub event_id: u32,
    pub route: Option<(&'static str, u64)>,
}

impl L2Identity {
    /// The logical event, independent of tier and route: two releases with
    /// the same key are two outputs of one planned opportunity.
    fn logical(&self) -> (L2Modality, u32) {
        (self.modality, self.seq)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct L2Submission {
    pub identity: L2Identity,
    pub t_release_us: u64,
    pub t_submit_us: u64,
}

#[derive(Debug, Clone)]
pub struct FakeSinkConfig {
    pub pc_offset_us: u64,
    pub haptic_offset_us: u64,
    /// Negative test: every submission is refused as if the queue were full.
    pub force_queue_full: bool,
    /// Negative test: submissions are accepted (submit row) but no terminal is
    /// ever emitted, so each one is `submitted_unresolved`.
    pub drop_terminals: bool,
    /// Test-only: the sink thread waits on this barrier before consuming its
    /// queue, so a test can fill the REAL bounded queue deterministically.
    pub start_gate: Option<Arc<Barrier>>,
}

impl FakeSinkConfig {
    pub fn offsets(pc_offset_us: u64, haptic_offset_us: u64) -> Self {
        Self {
            pc_offset_us,
            haptic_offset_us,
            force_queue_full: false,
            drop_terminals: false,
            start_gate: None,
        }
    }

    fn offset_us(&self, modality: L2Modality) -> u64 {
        match modality {
            L2Modality::Pc => self.pc_offset_us,
            L2Modality::Haptic => self.haptic_offset_us,
        }
    }
}

#[derive(Debug, Clone)]
pub struct L2Config {
    pub run_id: String,
    /// The receiver arm that released the objects (`b1`, `s1`, `s2`, `s3`, `s3r`).
    pub method: String,
    pub sink: L2SinkKind,
    /// Required for `L2SinkKind::Fake`, refused otherwise.
    pub fake: Option<FakeSinkConfig>,
    /// Bound of the release -> sink submission queue.
    pub queue_capacity: usize,
    /// Bound of the row queue into the single writer thread.
    pub row_capacity: usize,
    /// Close grace: after `finish` starts, how long producers may take to
    /// disappear and the sink may still receive terminals (anchor: close).
    pub report_window_us: u64,
    /// Bound of every per-object tracking structure (logical-event set, open
    /// submits, fake pending outputs). Exceeding it is an integrity failure.
    pub max_tracked_objects: usize,
}

impl L2Config {
    pub fn validate(&self) -> Result<()> {
        if self.run_id.is_empty() {
            bail!("L2 run_id must be non-empty");
        }
        if self.queue_capacity == 0 || self.row_capacity == 0 || self.max_tracked_objects == 0 {
            bail!("L2 queue and tracking capacities must be positive");
        }
        if self.sink == L2SinkKind::Fake && self.fake.is_none() {
            bail!("the fake L2 sink requires its offsets");
        }
        if self.sink == L2SinkKind::Null && self.fake.is_some() {
            bail!("fake-sink options do not apply to the null L2 sink");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum L2SubmitOutcome {
    Submitted,
    /// Recorded as `explicit_discard(output_queue_full)` before submit.
    QueueFull,
    /// `finish` already started; an integrity failure.
    AfterClose,
    /// The sink thread is gone; an integrity failure.
    SinkGone,
}

/// State shared by every handle and both threads.
#[derive(Debug, Default)]
struct Shared {
    /// Live `L2Submitter` handles (the stage's own one included). Quiescence
    /// at `finish` means this reaches zero; it is counted explicitly because
    /// the stage keeps a non-producer control sender on the same channel.
    producers: AtomicUsize,
    closed: AtomicBool,
    queue_full: AtomicU64,
    row_overflow: AtomicU64,
    submit_after_close: AtomicU64,
    sink_gone: AtomicU64,
    fake_pending_overflow: AtomicU64,
    producers_alive_at_finish: AtomicBool,
    receiver_fault_noted: AtomicBool,
    /// Earliest integrity failure `(t_us, reason)`.
    first_failure: Mutex<Option<(u64, &'static str)>>,
}

impl Shared {
    fn fail(&self, reason: &'static str) {
        self.fail_at(now_us(), reason);
    }

    fn fail_at(&self, t_us: u64, reason: &'static str) {
        let mut first = self.first_failure.lock().unwrap_or_else(|p| p.into_inner());
        if first.is_none_or(|(t, _)| t_us < t) {
            *first = Some((t_us, reason));
        }
    }

    fn first_failure(&self) -> Option<(u64, &'static str)> {
        *self.first_failure.lock().unwrap_or_else(|p| p.into_inner())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct L2Summary {
    pub submitted: u64,
    pub output: u64,
    pub discarded_before_submit: u64,
    pub discarded_after_submit: u64,
    pub submitted_unresolved: u64,
    pub row_overflow: u64,
    pub submit_after_close: u64,
    pub sink_gone: u64,
    /// A terminal whose identity has no open submit.
    pub terminal_without_open_submit: u64,
    /// A second release (submit or pre-submit discard) of one `(track, seq)`.
    pub duplicate_logical_release: u64,
    pub tracking_overflow: u64,
    pub fake_pending_overflow: u64,
    pub producers_alive_at_finish: bool,
    /// `submitted == output + discarded_after_submit + submitted_unresolved`.
    pub conservation_ok: bool,
    /// Max of (write completion − `t_log_arrival_us`) over all rows.
    pub max_write_lag_us: u64,
    /// Max of (`t_log_arrival_us` − `t_callback_us`) over terminal callbacks.
    pub max_callback_to_log_us: u64,
    pub first_integrity_failure: Option<(u64, String)>,
    /// The first fatal receiver error, if the receiver reported one.
    pub receiver_fault: Option<(u64, String)>,
}

impl L2Summary {
    /// Instrumentation-level integrity (registration §3-5, §6). `NullSink`
    /// runs are never L2 evidence; their unresolved submissions are expected.
    pub fn integrity_ok(&self, sink: L2SinkKind) -> bool {
        self.first_integrity_failure.is_none()
            && self.conservation_ok
            && self.row_overflow == 0
            && self.submit_after_close == 0
            && self.sink_gone == 0
            && self.terminal_without_open_submit == 0
            && self.duplicate_logical_release == 0
            && self.tracking_overflow == 0
            && self.fake_pending_overflow == 0
            && !self.producers_alive_at_finish
            && (sink == L2SinkKind::Null || self.submitted_unresolved == 0)
    }
}

enum SinkMsg {
    Item(L2Submission),
    /// The last producer handle was dropped (prompt wake-up only; the sink
    /// always re-reads the producer count).
    Wake,
    /// `finish` started: producers must be gone and terminals must be
    /// received strictly before `deadline_us`.
    Closing { deadline_us: u64 },
}

enum Row {
    Submit(L2Submission),
    Output {
        submission: L2Submission,
        t_output_us: u64,
        /// The sink's real receipt time of the terminal callback.
        t_callback_us: u64,
        device: &'static str,
    },
    Discard {
        identity: L2Identity,
        t_release_us: u64,
        t_submit_us: Option<u64>,
        t_discard_us: u64,
        reason: &'static str,
    },
    /// The receiver's first fatal error (registration §6-1 needs its time).
    ReceiverFault { t_us: u64, cause: String },
    End,
}

fn note_receiver_fault(rows: &SyncSender<Row>, shared: &Shared, t_us: u64, cause: &str) {
    if shared.receiver_fault_noted.swap(true, Ordering::AcqRel) {
        return;
    }
    let row = Row::ReceiverFault { t_us, cause: cause.to_string() };
    if rows.try_send(row).is_err() {
        shared.row_overflow.fetch_add(1, Ordering::Relaxed);
        shared.fail(FAIL_ROW_QUEUE_OVERFLOW);
    }
}

/// Cloneable, non-blocking submission handle held at the release points.
/// Every live clone is counted; `finish` requires all of them to be dropped
/// within the close grace (`producers_alive_at_finish` otherwise), so a
/// submission after the sentinel is only possible in a run whose sentinel
/// already says `integrity_ok: false`.
pub struct L2Submitter {
    items: SyncSender<SinkMsg>,
    rows: SyncSender<Row>,
    shared: Arc<Shared>,
    force_queue_full: bool,
}

impl Clone for L2Submitter {
    fn clone(&self) -> Self {
        self.shared.producers.fetch_add(1, Ordering::AcqRel);
        Self {
            items: self.items.clone(),
            rows: self.rows.clone(),
            shared: self.shared.clone(),
            force_queue_full: self.force_queue_full,
        }
    }
}

impl Drop for L2Submitter {
    fn drop(&mut self) {
        // Every send of this handle happened before this decrement, so a sink
        // that reads zero and then drains the queue sees all of them.
        if self.shared.producers.fetch_sub(1, Ordering::AcqRel) == 1 {
            let _ = self.items.try_send(SinkMsg::Wake);
        }
    }
}

impl L2Submitter {
    /// Hand one RELEASED object to the output stage. Never blocks, never
    /// fails the caller. `t_submit_us` is read here and clamped to
    /// `t_release_us` so `t_release <= t_submit` holds by construction.
    pub fn submit(&self, identity: L2Identity, t_release_us: u64) -> L2SubmitOutcome {
        if self.shared.closed.load(Ordering::Acquire) {
            self.shared.submit_after_close.fetch_add(1, Ordering::Relaxed);
            self.shared.fail(FAIL_SUBMIT_AFTER_CLOSE);
            return L2SubmitOutcome::AfterClose;
        }
        let t_submit_us = now_us().max(t_release_us);
        let submission = L2Submission { identity, t_release_us, t_submit_us };
        let full = if self.force_queue_full {
            true
        } else {
            match self.items.try_send(SinkMsg::Item(submission)) {
                Ok(()) => return L2SubmitOutcome::Submitted,
                Err(TrySendError::Full(_)) => true,
                Err(TrySendError::Disconnected(_)) => false,
            }
        };
        if !full {
            self.shared.sink_gone.fetch_add(1, Ordering::Relaxed);
            self.shared.fail(FAIL_SINK_GONE);
            return L2SubmitOutcome::SinkGone;
        }
        self.shared.queue_full.fetch_add(1, Ordering::Relaxed);
        let row = Row::Discard {
            identity,
            t_release_us,
            t_submit_us: None,
            t_discard_us: now_us().max(t_release_us),
            reason: DISCARD_OUTPUT_QUEUE_FULL,
        };
        if self.rows.try_send(row).is_err() {
            self.shared.row_overflow.fetch_add(1, Ordering::Relaxed);
            self.shared.fail(FAIL_ROW_QUEUE_OVERFLOW);
        }
        L2SubmitOutcome::QueueFull
    }

    /// Record the receiver's FIRST fatal error at `t_us` (the moment the
    /// receiver recorded it). Later calls are ignored. Non-blocking.
    pub fn receiver_fault(&self, t_us: u64, cause: &str) {
        note_receiver_fault(&self.rows, &self.shared, t_us, cause);
    }

    pub fn receiver_fault_noted(&self) -> bool {
        self.shared.receiver_fault_noted.load(Ordering::Acquire)
    }
}

/// The running output stage: meta row written, sink + writer threads alive.
pub struct L2Output {
    config: L2Config,
    path: PathBuf,
    file_identity: (u64, u64),
    /// The stage's own producer handle; dropped by `finish` before it waits
    /// for every other producer to disappear.
    submitter: Option<L2Submitter>,
    /// The stage's own row handle (`End`, receiver faults). Not a producer.
    rows: SyncSender<Row>,
    /// Non-producer sender used only to deliver `Closing`.
    control: SyncSender<SinkMsg>,
    shared: Arc<Shared>,
    sink: Option<JoinHandle<()>>,
    writer: Option<JoinHandle<std::io::Result<L2Summary>>>,
}

/// `(st_dev, st_ino)` of an open file (fstat).
pub fn open_file_identity(file: &File) -> std::io::Result<(u64, u64)> {
    let metadata = file.metadata()?;
    Ok((metadata.dev(), metadata.ino()))
}

impl L2Output {
    pub fn start(path: &Path, config: L2Config) -> Result<Self> {
        Self::start_excluding(path, config, &[])
    }

    /// Create the L2 JSONL, write the meta row synchronously and start the
    /// sink and writer threads.
    ///
    /// The file is created with `O_CREAT | O_EXCL` (`create_new`): an existing
    /// path — including a symlink, dangling or not, and a hard link — is
    /// refused and left byte-identical, so no evidence file can be truncated
    /// or aliased. `exclude` lists `(dev, ino)` of files that are already open
    /// (the rx log); the new file is compared by fstat before anything is
    /// written to it.
    pub fn start_excluding(path: &Path, config: L2Config, exclude: &[(u64, u64)]) -> Result<Self> {
        config.validate()?;
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .with_context(|| format!("create L2 log {} (must not exist)", path.display()))?;
        let file_identity = open_file_identity(&file)?;
        if exclude.contains(&file_identity) {
            bail!("L2 log {} is the same file as an open log", path.display());
        }
        let mut w = BufWriter::new(file);
        let fake = config.fake.as_ref();
        let meta = serde_json::json!({
            "role": "meta",
            "l2_schema": L2_SCHEMA,
            "run_id": config.run_id,
            "method": config.method,
            "sink": config.sink.as_str(),
            "clock": L2_CLOCK,
            "time_unit": "us",
            "t_us": now_us(),
            "queue_capacity": config.queue_capacity,
            "row_capacity": config.row_capacity,
            "max_tracked_objects": config.max_tracked_objects,
            "report_window_us": config.report_window_us,
            "report_window_anchor": REPORT_WINDOW_ANCHOR,
            "t_log_arrival": "writer_clock_immediately_before_row_write",
            "fake_pc_offset_us": fake.map(|f| f.pc_offset_us),
            "fake_haptic_offset_us": fake.map(|f| f.haptic_offset_us),
            "fake_force_queue_full": fake.map(|f| f.force_queue_full),
            "fake_drop_terminals": fake.map(|f| f.drop_terminals),
            "device_output": false,
            "null_sink_never_resolves": config.sink == L2SinkKind::Null,
        });
        writeln!(w, "{meta}")?;
        w.flush()?;

        let (items_tx, items_rx) = mpsc::sync_channel::<SinkMsg>(config.queue_capacity);
        let (rows_tx, rows_rx) = mpsc::sync_channel::<Row>(config.row_capacity);
        let shared = Arc::new(Shared::default());
        let writer = {
            let run_id = config.run_id.clone();
            let sink = config.sink;
            let shared = shared.clone();
            let cap = config.max_tracked_objects;
            std::thread::Builder::new()
                .name("l2-writer".into())
                .spawn(move || writer_loop(w, rows_rx, run_id, sink, shared, cap))
                .context("spawn L2 writer thread")?
        };
        let sink = {
            let rows = rows_tx.clone();
            let kind = config.sink;
            let fake = config.fake.clone();
            let shared = shared.clone();
            let cap = config.max_tracked_objects;
            std::thread::Builder::new()
                .name("l2-sink".into())
                .spawn(move || sink_loop(kind, fake, items_rx, rows, shared, cap))
                .context("spawn L2 sink thread")?
        };
        shared.producers.store(1, Ordering::Release);
        let submitter = L2Submitter {
            items: items_tx.clone(),
            rows: rows_tx.clone(),
            shared: shared.clone(),
            force_queue_full: config.fake.as_ref().is_some_and(|f| f.force_queue_full),
        };
        Ok(Self {
            config,
            path: path.to_path_buf(),
            file_identity,
            submitter: Some(submitter),
            rows: rows_tx,
            control: items_tx,
            shared,
            sink: Some(sink),
            writer: Some(writer),
        })
    }

    pub fn submitter(&self) -> L2Submitter {
        self.submitter.clone().expect("the stage handle lives until finish")
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn file_identity(&self) -> (u64, u64) {
        self.file_identity
    }

    pub fn sink_kind(&self) -> L2SinkKind {
        self.config.sink
    }

    /// Record the receiver's FIRST fatal error at `t_us`; later calls are
    /// ignored. Non-blocking. Not a producer handle.
    pub fn receiver_fault(&self, t_us: u64, cause: &str) {
        note_receiver_fault(&self.rows, &self.shared, t_us, cause);
    }

    pub fn receiver_fault_noted(&self) -> bool {
        self.shared.receiver_fault_noted.load(Ordering::Acquire)
    }

    /// Registration §3-5 order: stop submissions -> every producer handle
    /// gone (bounded by the close grace) -> terminals received before the
    /// grace deadline -> sink joined -> `instrumentation_end` -> flush/close.
    /// Blocking; call from `spawn_blocking` in async code.
    pub fn finish(mut self) -> Result<L2Summary> {
        self.shared.closed.store(true, Ordering::Release);
        let deadline_us = now_us().saturating_add(self.config.report_window_us);
        // Our own producer handle goes first, so the count the sink checks
        // can reach zero; then the deadline is delivered on the non-producer
        // control sender. Blocking send: the sink is consuming, so space
        // appears; a dead sink makes this fail immediately instead of hanging.
        drop(self.submitter.take().expect("finish runs once"));
        if self.control.send(SinkMsg::Closing { deadline_us }).is_err() {
            self.shared.sink_gone.fetch_add(1, Ordering::Relaxed);
            self.shared.fail(FAIL_SINK_GONE);
        }
        if let Some(sink) = self.sink.take() {
            if sink.join().is_err() {
                bail!("L2 sink thread panicked");
            }
        }
        self.rows
            .send(Row::End)
            .map_err(|_| anyhow::anyhow!("L2 writer thread is gone before the sentinel"))?;
        let writer = self.writer.take().expect("writer joined only once");
        match writer.join() {
            Ok(result) => result.context("write L2 log"),
            Err(_) => bail!("L2 writer thread panicked"),
        }
    }
}

/// Fake-sink pending outputs, earliest `t_output_us` first; equal instants
/// resolve in submission order. Bounded by `cap`.
struct FakePending {
    heap: BinaryHeap<Reverse<(u64, u64)>>,
    items: HashMap<u64, L2Submission>,
    order: u64,
    cap: usize,
}

impl FakePending {
    fn new(cap: usize) -> Self {
        Self { heap: BinaryHeap::new(), items: HashMap::new(), order: 0, cap }
    }

    /// `false` when the bound is reached (the output is never scheduled).
    fn push(&mut self, submission: L2Submission, t_output_us: u64) -> bool {
        if self.items.len() >= self.cap {
            return false;
        }
        self.heap.push(Reverse((t_output_us, self.order)));
        self.items.insert(self.order, submission);
        self.order += 1;
        true
    }

    fn next_due(&self) -> Option<u64> {
        self.heap.peek().map(|Reverse((t, _))| *t)
    }

    /// Emit every output whose instant has passed. The callback receipt time
    /// is a real clock read; with `deadline` set (close grace) a callback is
    /// accepted only if that receipt time is strictly before the deadline —
    /// the synthetic `t_output_us` is never what is compared. Returns `false`
    /// once the writer is gone.
    fn emit_due(&mut self, rows: &SyncSender<Row>, deadline: Option<u64>) -> bool {
        while let Some(Reverse((t_output_us, key))) = self.heap.peek().copied() {
            let t_callback_us = now_us();
            if t_output_us > t_callback_us || deadline.is_some_and(|d| t_callback_us >= d) {
                break;
            }
            self.heap.pop();
            let submission = self.items.remove(&key).expect("heap and map agree");
            let row = Row::Output { submission, t_output_us, t_callback_us, device: "fake" };
            if rows.send(row).is_err() {
                return false;
            }
        }
        true
    }
}

fn micros(us: u64) -> Duration {
    Duration::from_micros(us)
}

fn sink_loop(
    kind: L2SinkKind,
    fake: Option<FakeSinkConfig>,
    items: Receiver<SinkMsg>,
    rows: SyncSender<Row>,
    shared: Arc<Shared>,
    cap: usize,
) {
    if let Some(gate) = fake.as_ref().and_then(|f| f.start_gate.clone()) {
        gate.wait();
    }
    let emit_outputs =
        kind == L2SinkKind::Fake && !fake.as_ref().is_some_and(|f| f.drop_terminals);
    let mut pending = FakePending::new(cap);
    // Accept one submission: the submit row first, then (fake) schedule its
    // synthetic output. Returns false once the writer is gone.
    let accept = |pending: &mut FakePending, submission: L2Submission| -> bool {
        if rows.send(Row::Submit(submission)).is_err() {
            return false;
        }
        if emit_outputs {
            let offset = fake
                .as_ref()
                .map(|f| f.offset_us(submission.identity.modality))
                .unwrap_or(0);
            if !pending.push(submission, submission.t_submit_us.saturating_add(offset)) {
                shared.fake_pending_overflow.fetch_add(1, Ordering::Relaxed);
                shared.fail(FAIL_FAKE_PENDING_OVERFLOW);
            }
        }
        true
    };
    // Take everything queued right now; false once the writer is gone.
    let drain_queue = |pending: &mut FakePending| -> bool {
        loop {
            match items.try_recv() {
                Ok(SinkMsg::Item(submission)) => {
                    if !accept(pending, submission) {
                        return false;
                    }
                }
                Ok(_) => {}
                Err(_) => return true,
            }
        }
    };

    // Phase 1: submissions open, until `finish` sends `Closing`. A disconnect
    // means the stage was dropped without `finish`: abandon (the writer then
    // ends without a sentinel — fail closed).
    let deadline_us = loop {
        let msg = match pending.next_due() {
            Some(due) => match items.recv_timeout(micros(due.saturating_sub(now_us()))) {
                Ok(msg) => Some(msg),
                Err(RecvTimeoutError::Timeout) => None,
                Err(RecvTimeoutError::Disconnected) => return,
            },
            None => match items.recv() {
                Ok(msg) => Some(msg),
                Err(_) => return,
            },
        };
        match msg {
            Some(SinkMsg::Item(submission)) => {
                if !accept(&mut pending, submission) {
                    return;
                }
            }
            Some(SinkMsg::Closing { deadline_us }) => break deadline_us,
            Some(SinkMsg::Wake) | None => {}
        }
        if !pending.emit_due(&rows, None) {
            return;
        }
    };

    // Phase 2: closing. Every producer handle must be dropped and every
    // terminal received strictly before the deadline.
    let mut producers_gone = false;
    loop {
        if !producers_gone && shared.producers.load(Ordering::Acquire) == 0 {
            producers_gone = true;
        }
        // After reading zero, this drain sees every submission ever sent.
        if !drain_queue(&mut pending) || !pending.emit_due(&rows, Some(deadline_us)) {
            return;
        }
        let now = now_us();
        if now >= deadline_us {
            break;
        }
        let next = pending.next_due().filter(|due| *due < deadline_us);
        if producers_gone {
            match next {
                Some(due) => std::thread::sleep(micros(due.saturating_sub(now))),
                // Nothing left that can be received before the deadline: the
                // rest stays unresolved (no row).
                None => break,
            }
        } else {
            let until = next.map_or(deadline_us, |due| due.min(deadline_us));
            match items.recv_timeout(micros(until.saturating_sub(now))) {
                Ok(SinkMsg::Item(submission)) => {
                    // Raced the close on the release side: still recorded.
                    if !accept(&mut pending, submission) {
                        return;
                    }
                }
                Ok(_) | Err(_) => {}
            }
        }
    }
    if !producers_gone {
        if shared.producers.load(Ordering::Acquire) > 0 {
            shared.producers_alive_at_finish.store(true, Ordering::Release);
            shared.fail(FAIL_PRODUCERS_ALIVE_AT_FINISH);
        }
        // Whatever is still queued gets its submit row (and stays unresolved).
        let _ = drain_queue(&mut pending);
    }
}

fn identity_json(run_id: &str, id: &L2Identity) -> serde_json::Map<String, serde_json::Value> {
    let mut m = serde_json::Map::new();
    m.insert("run_id".into(), run_id.into());
    m.insert("track".into(), id.modality.as_str().into());
    m.insert("tier".into(), id.tier.into());
    m.insert("seq".into(), id.seq.into());
    m.insert("pts_us".into(), id.pts_us.into());
    m.insert("event_id".into(), id.event_id.into());
    if let Some((wire_track, generation)) = id.route {
        m.insert("wire_track".into(), wire_track.into());
        m.insert("route_generation".into(), generation.into());
    }
    m
}

/// Per-object bookkeeping of the writer, bounded by `cap`.
struct Tracking {
    /// Logical events ever released (submitted or discarded before submit).
    seen: HashSet<(L2Modality, u32)>,
    /// Open submits per identity (a count, so duplicates cannot hide).
    open: HashMap<L2Identity, u32>,
    open_total: u64,
    cap: usize,
}

impl Tracking {
    fn release(&mut self, identity: &L2Identity, summary: &mut L2Summary, shared: &Shared) {
        let key = identity.logical();
        if self.seen.contains(&key) {
            summary.duplicate_logical_release += 1;
            shared.fail(FAIL_DUPLICATE_LOGICAL_RELEASE);
        } else if self.seen.len() >= self.cap {
            summary.tracking_overflow += 1;
            shared.fail(FAIL_TRACKING_CAPACITY);
        } else {
            self.seen.insert(key);
        }
    }

    fn open(&mut self, identity: L2Identity, summary: &mut L2Summary, shared: &Shared) {
        self.open_total += 1;
        if let Some(count) = self.open.get_mut(&identity) {
            *count += 1;
        } else if self.open.len() >= self.cap {
            summary.tracking_overflow += 1;
            shared.fail(FAIL_TRACKING_CAPACITY);
        } else {
            self.open.insert(identity, 1);
        }
    }

    fn close(&mut self, identity: &L2Identity, summary: &mut L2Summary, shared: &Shared) {
        match self.open.get_mut(identity) {
            Some(count) => {
                *count -= 1;
                if *count == 0 {
                    self.open.remove(identity);
                }
                self.open_total -= 1;
            }
            None => {
                summary.terminal_without_open_submit += 1;
                shared.fail(FAIL_TERMINAL_WITHOUT_OPEN_SUBMIT);
            }
        }
    }
}

/// Write one row: `t_log_arrival_us` (and `t_us`, unless the row carries its
/// own event time) is the writer clock immediately before the write; the write
/// lag is measured after the flush completes.
fn write_row(
    w: &mut BufWriter<File>,
    mut row: serde_json::Map<String, serde_json::Value>,
    event_t_us: Option<u64>,
    summary: &mut L2Summary,
) -> std::io::Result<u64> {
    let t_log_arrival_us = now_us();
    row.insert("t_us".into(), event_t_us.unwrap_or(t_log_arrival_us).into());
    row.insert("t_log_arrival_us".into(), t_log_arrival_us.into());
    writeln!(w, "{}", serde_json::Value::Object(row))?;
    w.flush()?;
    let lag = now_us().saturating_sub(t_log_arrival_us);
    summary.max_write_lag_us = summary.max_write_lag_us.max(lag);
    Ok(t_log_arrival_us)
}

fn writer_loop(
    mut w: BufWriter<File>,
    rows: Receiver<Row>,
    run_id: String,
    sink: L2SinkKind,
    shared: Arc<Shared>,
    cap: usize,
) -> std::io::Result<L2Summary> {
    let mut summary = L2Summary::default();
    let mut tracking = Tracking { seen: HashSet::new(), open: HashMap::new(), open_total: 0, cap };
    let mut ended = false;
    let mut handle = |row: Row, w: &mut BufWriter<File>, summary: &mut L2Summary| -> std::io::Result<()> {
        match row {
            Row::Submit(s) => {
                summary.submitted += 1;
                tracking.release(&s.identity, summary, &shared);
                tracking.open(s.identity, summary, &shared);
                let mut m = identity_json(&run_id, &s.identity);
                m.insert("role".into(), "submit".into());
                m.insert("t_release_us".into(), s.t_release_us.into());
                m.insert("t_submit_us".into(), s.t_submit_us.into());
                write_row(w, m, None, summary)?;
            }
            Row::Output { submission: s, t_output_us, t_callback_us, device } => {
                summary.output += 1;
                tracking.close(&s.identity, summary, &shared);
                let mut m = identity_json(&run_id, &s.identity);
                m.insert("role".into(), "output".into());
                m.insert("t_release_us".into(), s.t_release_us.into());
                m.insert("t_submit_us".into(), s.t_submit_us.into());
                m.insert("t_output_us".into(), t_output_us.into());
                m.insert("t_callback_us".into(), t_callback_us.into());
                // Modality-specific device evidence (PC presentation feedback,
                // ALSA status) is filled by the M2/M3 sinks.
                m.insert(
                    "output_detail".into(),
                    serde_json::json!({ "device": device, "modality": s.identity.modality.as_str() }),
                );
                let t_log = write_row(w, m, None, summary)?;
                summary.max_callback_to_log_us =
                    summary.max_callback_to_log_us.max(t_log.saturating_sub(t_callback_us));
            }
            Row::Discard { identity, t_release_us, t_submit_us, t_discard_us, reason } => {
                match t_submit_us {
                    None => {
                        summary.discarded_before_submit += 1;
                        tracking.release(&identity, summary, &shared);
                    }
                    Some(_) => {
                        summary.discarded_after_submit += 1;
                        tracking.close(&identity, summary, &shared);
                    }
                }
                let mut m = identity_json(&run_id, &identity);
                m.insert("role".into(), "explicit_discard".into());
                m.insert("reason".into(), reason.into());
                m.insert(
                    "discard_phase".into(),
                    if t_submit_us.is_some() { "after_submit" } else { "before_submit" }.into(),
                );
                m.insert("t_release_us".into(), t_release_us.into());
                m.insert("t_submit_us".into(), t_submit_us.into());
                m.insert("t_discard_us".into(), t_discard_us.into());
                write_row(w, m, None, summary)?;
            }
            Row::ReceiverFault { t_us, cause } => {
                let mut m = serde_json::Map::new();
                m.insert("role".into(), "receiver_fault".into());
                m.insert("run_id".into(), run_id.clone().into());
                m.insert("cause".into(), cause.clone().into());
                write_row(w, m, Some(t_us), summary)?;
                summary.receiver_fault.get_or_insert((t_us, cause));
            }
            Row::End => unreachable!("End is handled by the loop"),
        }
        Ok(())
    };
    while let Ok(row) = rows.recv() {
        if matches!(row, Row::End) {
            ended = true;
            break;
        }
        handle(row, &mut w, &mut summary)?;
    }
    if !ended {
        // Every sender vanished without `finish`: fail closed, no sentinel.
        w.flush()?;
        return Err(std::io::Error::other("L2 writer ended without finish"));
    }
    // Rows sent after `End` can only come from a producer that outlived
    // `finish`, which the sink already recorded as an integrity failure; write
    // what is there before the sentinel.
    while let Ok(row) = rows.try_recv() {
        if !matches!(row, Row::End) {
            handle(row, &mut w, &mut summary)?;
        }
    }
    summary.submitted_unresolved = tracking.open_total;
    summary.conservation_ok = summary.submitted
        == summary.output + summary.discarded_after_submit + summary.submitted_unresolved;
    if sink != L2SinkKind::Null && summary.submitted_unresolved > 0 {
        shared.fail(FAIL_SUBMITTED_UNRESOLVED);
    }
    if !summary.conservation_ok {
        shared.fail(FAIL_CONSERVATION);
    }
    summary.row_overflow = shared.row_overflow.load(Ordering::Relaxed);
    summary.submit_after_close = shared.submit_after_close.load(Ordering::Relaxed);
    summary.sink_gone = shared.sink_gone.load(Ordering::Relaxed);
    summary.fake_pending_overflow = shared.fake_pending_overflow.load(Ordering::Relaxed);
    summary.producers_alive_at_finish = shared.producers_alive_at_finish.load(Ordering::Acquire);
    summary.first_integrity_failure = shared.first_failure().map(|(t, r)| (t, r.to_string()));
    let integrity_ok = summary.integrity_ok(sink);
    let t_end = now_us();
    let (failure_us, failure_reason) = match &summary.first_integrity_failure {
        Some((t, r)) => (serde_json::json!(t), serde_json::json!(r)),
        None => (serde_json::Value::Null, serde_json::Value::Null),
    };
    let (fault_us, fault_cause) = match &summary.receiver_fault {
        Some((t, c)) => (serde_json::json!(t), serde_json::json!(c)),
        None => (serde_json::Value::Null, serde_json::Value::Null),
    };
    let sentinel = serde_json::json!({
        "role": "instrumentation_end",
        "run_id": run_id,
        "sink": sink.as_str(),
        "t_us": t_end,
        "end_us": t_end,
        "submitted": summary.submitted,
        "output": summary.output,
        "discarded_before_submit": summary.discarded_before_submit,
        "discarded_after_submit": summary.discarded_after_submit,
        "submitted_unresolved": summary.submitted_unresolved,
        "conservation_ok": summary.conservation_ok,
        "duplicate_logical_release": summary.duplicate_logical_release,
        "terminal_without_open_submit": summary.terminal_without_open_submit,
        "tracking_overflow": summary.tracking_overflow,
        "fake_pending_overflow": summary.fake_pending_overflow,
        "row_overflow": summary.row_overflow,
        "submit_after_close": summary.submit_after_close,
        "sink_gone": summary.sink_gone,
        "producers_alive_at_finish": summary.producers_alive_at_finish,
        "max_write_lag_us": summary.max_write_lag_us,
        "max_callback_to_log_us": summary.max_callback_to_log_us,
        "report_window_anchor": REPORT_WINDOW_ANCHOR,
        "first_integrity_failure_us": failure_us,
        "first_integrity_failure_reason": failure_reason,
        "receiver_fault_us": fault_us,
        "receiver_fault_cause": fault_cause,
        "integrity_ok": integrity_ok,
    });
    writeln!(w, "{sentinel}")?;
    w.flush()?;
    drop(w);
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("skew-l2-{}-{}-{name}.jsonl", std::process::id(), now_us()))
    }

    fn config(sink: L2SinkKind, fake: Option<FakeSinkConfig>, capacity: usize) -> L2Config {
        L2Config {
            run_id: "l2-test".into(),
            method: "s1".into(),
            sink,
            fake,
            queue_capacity: capacity,
            row_capacity: 4096,
            report_window_us: 500_000,
            max_tracked_objects: 4096,
        }
    }

    fn id(modality: L2Modality, seq: u32) -> L2Identity {
        L2Identity {
            modality,
            tier: if modality == L2Modality::Pc { 2 } else { 0 },
            seq,
            pts_us: seq as u64 * 11_111,
            event_id: seq,
            route: None,
        }
    }

    fn rows(path: &Path) -> Vec<serde_json::Value> {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    fn key(r: &serde_json::Value) -> String {
        format!("{}/{}/{}/{}/{}", r["track"], r["tier"], r["seq"], r["pts_us"], r["event_id"])
    }

    fn sentinel(rows: &[serde_json::Value]) -> &serde_json::Value {
        let last = rows.last().unwrap();
        assert_eq!(last["role"], "instrumentation_end");
        assert_eq!(rows.iter().filter(|r| r["role"] == "instrumentation_end").count(), 1);
        last
    }

    /// Replay rows straight through the writer (Codex 104 harness shape).
    fn replay(path: &Path, sink: L2SinkKind, input: Vec<Row>, writer_delay: Option<Duration>) -> L2Summary {
        let (tx, rx) = mpsc::sync_channel(64);
        for row in input {
            tx.send(row).unwrap();
        }
        tx.send(Row::End).unwrap();
        drop(tx);
        if let Some(delay) = writer_delay {
            std::thread::sleep(delay);
        }
        let file = OpenOptions::new().write(true).create_new(true).open(path).unwrap();
        writer_loop(BufWriter::new(file), rx, "l2-test".into(), sink, Arc::new(Shared::default()), 64).unwrap()
    }

    fn submission(identity: L2Identity) -> L2Submission {
        let t = now_us();
        L2Submission { identity, t_release_us: t, t_submit_us: t }
    }

    fn output_row(s: L2Submission) -> Row {
        Row::Output { submission: s, t_output_us: s.t_submit_us, t_callback_us: now_us(), device: "fake" }
    }

    #[test]
    fn fake_sink_gives_each_submission_one_submit_and_one_output_and_ends_with_the_sentinel() {
        let path = tmp("fake-one-terminal");
        let out = L2Output::start(
            &path,
            config(L2SinkKind::Fake, Some(FakeSinkConfig::offsets(3_000, 1_000)), 64),
        )
        .unwrap();
        let s = out.submitter();
        let mut submitted = Vec::new();
        for seq in 0..20 {
            let modality = if seq % 4 == 0 { L2Modality::Pc } else { L2Modality::Haptic };
            let identity = id(modality, seq);
            assert_eq!(s.submit(identity, now_us()), L2SubmitOutcome::Submitted);
            submitted.push(identity);
        }
        drop(s);
        let summary = out.finish().unwrap();
        assert_eq!(summary.submitted, 20);
        assert_eq!(summary.output, 20);
        assert_eq!(summary.submitted_unresolved, 0);
        assert!(summary.conservation_ok);
        assert!(summary.integrity_ok(L2SinkKind::Fake), "{summary:?}");
        let rows = rows(&path);
        assert_eq!(rows[0]["role"], "meta");
        assert_eq!(rows[0]["l2_schema"], L2_SCHEMA);
        assert_eq!(rows[0]["clock"], L2_CLOCK);
        assert_eq!(rows[0]["sink"], "fake");
        assert_eq!(rows[0]["report_window_anchor"], "close");
        let end = sentinel(&rows);
        assert_eq!(end["end_us"], end["t_us"]);
        assert_eq!(end["integrity_ok"], true);
        assert!(end["first_integrity_failure_us"].is_null());
        assert!(end["first_integrity_failure_reason"].is_null());
        assert!(end["receiver_fault_us"].is_null());
        assert!(end["max_write_lag_us"].is_u64());
        for identity in &submitted {
            let k = key(&serde_json::Value::Object(identity_json("x", identity)));
            let submits: Vec<_> =
                rows.iter().filter(|r| r["role"] == "submit" && key(r) == k).collect();
            let outputs: Vec<_> =
                rows.iter().filter(|r| r["role"] == "output" && key(r) == k).collect();
            assert_eq!(submits.len(), 1, "{k}");
            assert_eq!(outputs.len(), 1, "{k}");
            let o = outputs[0];
            let offset = if identity.modality == L2Modality::Pc { 3_000 } else { 1_000 };
            assert_eq!(o["t_output_us"].as_u64().unwrap(), o["t_submit_us"].as_u64().unwrap() + offset);
            assert!(o["t_release_us"].as_u64() <= o["t_submit_us"].as_u64());
            assert!(o["t_output_us"].as_u64() <= o["t_callback_us"].as_u64());
            assert!(o["t_callback_us"].as_u64() <= o["t_log_arrival_us"].as_u64());
            assert_eq!(o["t_us"], o["t_log_arrival_us"]);
            assert_eq!(o["run_id"], "l2-test");
            let submit_pos = rows.iter().position(|r| std::ptr::eq(r, submits[0])).unwrap();
            let output_pos = rows.iter().position(|r| std::ptr::eq(r, o)).unwrap();
            assert!(submit_pos < output_pos, "submit precedes its terminal");
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn forced_queue_full_is_an_explicit_discard_without_submit() {
        let path = tmp("fake-forced-full");
        let mut fake = FakeSinkConfig::offsets(0, 0);
        fake.force_queue_full = true;
        let out = L2Output::start(&path, config(L2SinkKind::Fake, Some(fake), 64)).unwrap();
        let s = out.submitter();
        for seq in 0..5 {
            assert_eq!(s.submit(id(L2Modality::Haptic, seq), now_us()), L2SubmitOutcome::QueueFull);
        }
        drop(s);
        let summary = out.finish().unwrap();
        assert_eq!(summary.submitted, 0);
        assert_eq!(summary.discarded_before_submit, 5);
        assert!(summary.integrity_ok(L2SinkKind::Fake), "{summary:?}");
        let rows = rows(&path);
        assert!(rows.iter().all(|r| r["role"] != "submit" && r["role"] != "output"));
        let discards: Vec<_> = rows.iter().filter(|r| r["role"] == "explicit_discard").collect();
        assert_eq!(discards.len(), 5);
        for d in discards {
            assert_eq!(d["reason"], DISCARD_OUTPUT_QUEUE_FULL);
            assert_eq!(d["discard_phase"], "before_submit");
            assert!(d["t_submit_us"].is_null());
            assert!(d["t_discard_us"].as_u64() >= d["t_release_us"].as_u64());
        }
        sentinel(&rows);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_really_full_bounded_queue_discards_exactly_the_overflow() {
        let path = tmp("fake-real-full");
        let gate = Arc::new(Barrier::new(2));
        let mut fake = FakeSinkConfig::offsets(0, 0);
        fake.start_gate = Some(gate.clone());
        let out = L2Output::start(&path, config(L2SinkKind::Fake, Some(fake), 2)).unwrap();
        let s = out.submitter();
        let outcomes: Vec<_> =
            (0..5).map(|seq| s.submit(id(L2Modality::Pc, seq), now_us())).collect();
        assert_eq!(
            outcomes,
            vec![
                L2SubmitOutcome::Submitted,
                L2SubmitOutcome::Submitted,
                L2SubmitOutcome::QueueFull,
                L2SubmitOutcome::QueueFull,
                L2SubmitOutcome::QueueFull,
            ]
        );
        gate.wait();
        drop(s);
        let summary = out.finish().unwrap();
        assert_eq!((summary.submitted, summary.output, summary.discarded_before_submit), (2, 2, 3));
        assert!(summary.integrity_ok(L2SinkKind::Fake), "{summary:?}");
        let rows = rows(&path);
        for seq in 2..5u32 {
            let k = key(&serde_json::Value::Object(identity_json("x", &id(L2Modality::Pc, seq))));
            assert!(rows.iter().all(|r| !(r["role"] == "submit" && key(r) == k)));
            assert_eq!(
                rows.iter().filter(|r| r["role"] == "explicit_discard" && key(r) == k).count(),
                1
            );
        }
        sentinel(&rows);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn dropped_terminals_are_counted_unresolved_and_fail_integrity() {
        let path = tmp("fake-drop-terminals");
        let mut fake = FakeSinkConfig::offsets(0, 0);
        fake.drop_terminals = true;
        let out = L2Output::start(&path, config(L2SinkKind::Fake, Some(fake), 8)).unwrap();
        let s = out.submitter();
        for seq in 0..3 {
            s.submit(id(L2Modality::Pc, seq), now_us());
        }
        drop(s);
        let summary = out.finish().unwrap();
        assert_eq!((summary.submitted, summary.output, summary.submitted_unresolved), (3, 0, 3));
        assert!(summary.conservation_ok, "unresolved submits are conserved, not lost");
        assert!(!summary.integrity_ok(L2SinkKind::Fake));
        let rows = rows(&path);
        let end = sentinel(&rows);
        assert_eq!(end["submitted_unresolved"], 3);
        assert_eq!(end["integrity_ok"], false);
        assert_eq!(end["first_integrity_failure_reason"], FAIL_SUBMITTED_UNRESOLVED);
        assert!(end["first_integrity_failure_us"].as_u64().unwrap() <= end["t_us"].as_u64().unwrap());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn an_output_beyond_the_close_grace_stays_unresolved() {
        let path = tmp("fake-window");
        let mut cfg = config(L2SinkKind::Fake, Some(FakeSinkConfig::offsets(10_000_000, 0)), 8);
        cfg.report_window_us = 20_000;
        let out = L2Output::start(&path, cfg).unwrap();
        let s = out.submitter();
        s.submit(id(L2Modality::Pc, 0), now_us());
        s.submit(id(L2Modality::Haptic, 1), now_us());
        drop(s);
        let summary = out.finish().unwrap();
        assert_eq!((summary.submitted, summary.output, summary.submitted_unresolved), (2, 1, 1));
        assert!(!summary.integrity_ok(L2SinkKind::Fake));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn null_sink_submits_but_never_resolves() {
        let path = tmp("null");
        let out = L2Output::start(&path, config(L2SinkKind::Null, None, 8)).unwrap();
        let s = out.submitter();
        s.submit(id(L2Modality::Pc, 0), now_us());
        drop(s);
        let summary = out.finish().unwrap();
        assert_eq!((summary.submitted, summary.output, summary.submitted_unresolved), (1, 0, 1));
        assert!(summary.integrity_ok(L2SinkKind::Null), "{summary:?}");
        let rows = rows(&path);
        assert_eq!(rows[0]["null_sink_never_resolves"], true);
        sentinel(&rows);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn s3_route_identity_is_carried_verbatim() {
        let path = tmp("route");
        let out = L2Output::start(&path, config(L2SinkKind::Fake, Some(FakeSinkConfig::offsets(0, 0)), 8)).unwrap();
        let s = out.submitter();
        let mut identity = id(L2Modality::Pc, 7);
        identity.route = Some(("pc-d6", 3));
        s.submit(identity, now_us());
        drop(s);
        out.finish().unwrap();
        let rows = rows(&path);
        for r in rows.iter().filter(|r| r["role"] == "submit" || r["role"] == "output") {
            assert_eq!(r["wire_track"], "pc-d6");
            assert_eq!(r["route_generation"], 3);
            assert_eq!(r["seq"], 7);
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn config_validation_rejects_inconsistent_sinks() {
        assert!(config(L2SinkKind::Fake, None, 8).validate().is_err());
        assert!(config(L2SinkKind::Null, Some(FakeSinkConfig::offsets(0, 0)), 8).validate().is_err());
        assert!(config(L2SinkKind::Null, None, 0).validate().is_err());
        let mut c = config(L2SinkKind::Null, None, 8);
        c.max_tracked_objects = 0;
        assert!(c.validate().is_err());
    }

    // ---- F1: duplicates and conservation (Codex 104 counterexamples) ------

    /// Codex 104 #1: the same identity twice, 150 ms apart, offset 300 ms,
    /// close grace 225 ms -> only one terminal arrives in the grace. Was green.
    #[test]
    fn codex104_duplicate_identity_with_one_terminal_in_the_grace_fails() {
        let path = tmp("c104-dup-window");
        let mut cfg = config(L2SinkKind::Fake, Some(FakeSinkConfig::offsets(300_000, 0)), 8);
        cfg.method = "b1".into();
        cfg.report_window_us = 225_000;
        let out = L2Output::start(&path, cfg).unwrap();
        let s = out.submitter();
        let identity = L2Identity { modality: L2Modality::Pc, tier: 2, seq: 1, pts_us: 33_333, event_id: 2, route: None };
        assert_eq!(s.submit(identity, now_us()), L2SubmitOutcome::Submitted);
        std::thread::sleep(Duration::from_millis(150));
        assert_eq!(s.submit(identity, now_us()), L2SubmitOutcome::Submitted);
        drop(s);
        let summary = out.finish().unwrap();
        assert_eq!((summary.submitted, summary.output, summary.submitted_unresolved), (2, 1, 1));
        assert_eq!(summary.duplicate_logical_release, 1);
        assert!(!summary.integrity_ok(L2SinkKind::Fake));
        let rows = rows(&path);
        let end = sentinel(&rows);
        assert_eq!(end["integrity_ok"], false);
        assert_eq!(end["first_integrity_failure_reason"], FAIL_DUPLICATE_LOGICAL_RELEASE);
        let _ = std::fs::remove_file(&path);
    }

    /// Codex 104 #2: offset 0, two submit/output pairs of one identity. Was green.
    #[test]
    fn codex104_two_complete_pairs_of_one_identity_fail() {
        let path = tmp("c104-dup-pairs");
        let mut cfg = config(L2SinkKind::Fake, Some(FakeSinkConfig::offsets(0, 0)), 8);
        cfg.method = "b1".into();
        let out = L2Output::start(&path, cfg).unwrap();
        let s = out.submitter();
        let identity = L2Identity { modality: L2Modality::Pc, tier: 2, seq: 1, pts_us: 33_333, event_id: 2, route: None };
        assert_eq!(s.submit(identity, now_us()), L2SubmitOutcome::Submitted);
        assert_eq!(s.submit(identity, now_us()), L2SubmitOutcome::Submitted);
        drop(s);
        let summary = out.finish().unwrap();
        assert_eq!((summary.submitted, summary.output, summary.submitted_unresolved), (2, 2, 0));
        assert_eq!(summary.duplicate_logical_release, 1);
        assert!(!summary.integrity_ok(L2SinkKind::Fake));
        let _ = std::fs::remove_file(&path);
    }

    /// Same logical event `(track, seq)` released on two S3 routes (different
    /// tier/route) is a duplicate too; two DIFFERENT events on different
    /// routes are fine.
    #[test]
    fn logical_duplicates_are_route_independent_and_distinct_events_pass() {
        let mut a = id(L2Modality::Pc, 4);
        a.route = Some(("pc", 0));
        let mut b = a;
        b.tier = 4;
        b.route = Some(("pc-d6", 1));
        let path = tmp("dup-routes");
        let summary = replay(
            &path,
            L2SinkKind::Fake,
            vec![Row::Submit(submission(a)), output_row(submission(a)), Row::Submit(submission(b)), output_row(submission(b))],
            None,
        );
        assert_eq!(summary.duplicate_logical_release, 1);
        assert!(!summary.integrity_ok(L2SinkKind::Fake));
        let _ = std::fs::remove_file(&path);

        let mut c = id(L2Modality::Pc, 5);
        c.tier = 4;
        c.route = Some(("pc-d6", 1));
        let mut h = id(L2Modality::Haptic, 4);
        h.route = Some(("haptic-essential", 1));
        let path = tmp("distinct-routes");
        let summary = replay(
            &path,
            L2SinkKind::Fake,
            vec![
                Row::Submit(submission(a)), output_row(submission(a)),
                Row::Submit(submission(c)), output_row(submission(c)),
                Row::Submit(submission(h)), output_row(submission(h)),
            ],
            None,
        );
        assert_eq!(summary.duplicate_logical_release, 0);
        assert!(summary.conservation_ok);
        assert!(summary.integrity_ok(L2SinkKind::Fake), "{summary:?}");
        let _ = std::fs::remove_file(&path);
    }

    /// A pre-submit discard and a later submit of the same logical event are
    /// two releases of one opportunity.
    #[test]
    fn a_discard_then_a_submit_of_one_logical_event_is_a_duplicate() {
        let a = id(L2Modality::Haptic, 9);
        let path = tmp("discard-then-submit");
        let summary = replay(
            &path,
            L2SinkKind::Fake,
            vec![
                Row::Discard { identity: a, t_release_us: now_us(), t_submit_us: None, t_discard_us: now_us(), reason: DISCARD_OUTPUT_QUEUE_FULL },
                Row::Submit(submission(a)),
                output_row(submission(a)),
            ],
            None,
        );
        assert_eq!(summary.duplicate_logical_release, 1);
        assert!(!summary.integrity_ok(L2SinkKind::Fake));
        let _ = std::fs::remove_file(&path);
    }

    /// A terminal that matches no open submit breaks conservation and the
    /// open-submit check.
    #[test]
    fn a_terminal_without_a_submit_breaks_conservation() {
        let path = tmp("orphan-terminal");
        let summary = replay(&path, L2SinkKind::Fake, vec![output_row(submission(id(L2Modality::Pc, 1)))], None);
        assert_eq!(summary.terminal_without_open_submit, 1);
        assert!(!summary.conservation_ok);
        assert!(!summary.integrity_ok(L2SinkKind::Fake));
        let rows = rows(&path);
        assert_eq!(sentinel(&rows)["conservation_ok"], false);
        let _ = std::fs::remove_file(&path);
    }

    // ---- F2: no truncation, no aliasing ---------------------------------

    fn digest(path: &Path) -> Vec<u8> {
        use sha2::{Digest, Sha256};
        Sha256::digest(std::fs::read(path).unwrap()).to_vec()
    }

    #[test]
    fn an_existing_file_at_the_l2_path_is_refused_and_left_unchanged() {
        let path = tmp("existing");
        std::fs::write(&path, b"existing evidence\n").unwrap();
        let before = digest(&path);
        assert!(L2Output::start(&path, config(L2SinkKind::Null, None, 8)).is_err());
        assert_eq!(digest(&path), before);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_symlink_to_the_rx_log_is_refused_and_the_rx_bytes_survive() {
        let rx = tmp("rx");
        let alias = tmp("rx-alias");
        std::fs::write(&rx, b"{\"role\":\"meta\"}\n{\"role\":\"rx\"}\n").unwrap();
        let before = std::fs::read(&rx).unwrap();
        std::os::unix::fs::symlink(&rx, &alias).unwrap();
        assert!(L2Output::start(&alias, config(L2SinkKind::Null, None, 8)).is_err());
        assert_eq!(std::fs::read(&rx).unwrap(), before);
        // A dangling symlink is refused too (nothing is created through it).
        let target = tmp("never-created");
        let dangling = tmp("dangling");
        std::os::unix::fs::symlink(&target, &dangling).unwrap();
        assert!(L2Output::start(&dangling, config(L2SinkKind::Null, None, 8)).is_err());
        assert!(!target.exists());
        for p in [&rx, &alias, &dangling] {
            let _ = std::fs::remove_file(p);
        }
    }

    #[test]
    fn a_hard_link_to_the_rx_log_is_refused() {
        let rx = tmp("rx-hard");
        let link = tmp("rx-hardlink");
        std::fs::write(&rx, b"rx bytes\n").unwrap();
        std::fs::hard_link(&rx, &link).unwrap();
        assert!(L2Output::start(&link, config(L2SinkKind::Null, None, 8)).is_err());
        assert_eq!(std::fs::read(&rx).unwrap(), b"rx bytes\n");
        for p in [&rx, &link] {
            let _ = std::fs::remove_file(p);
        }
    }

    #[test]
    fn the_fstat_exclusion_is_checked_and_distinct_files_pass() {
        let other = tmp("other-open");
        let file = File::create(&other).unwrap();
        let other_identity = open_file_identity(&file).unwrap();
        let path = tmp("excluding");
        let out = L2Output::start_excluding(&path, config(L2SinkKind::Null, None, 8), &[other_identity]).unwrap();
        assert_ne!(out.file_identity(), other_identity);
        out.finish().unwrap();
        for p in [&other, &path] {
            let _ = std::fs::remove_file(p);
        }
    }

    // ---- F3: receipt clock semantics --------------------------------------

    #[test]
    fn t_log_arrival_is_the_writer_clock_and_the_write_lag_is_reported() {
        let path = tmp("writer-lag");
        let s = submission(id(L2Modality::Pc, 1));
        let callback = now_us();
        let summary = replay(
            &path,
            L2SinkKind::Fake,
            vec![
                Row::Submit(s),
                Row::Output { submission: s, t_output_us: s.t_submit_us, t_callback_us: callback, device: "fake" },
            ],
            Some(Duration::from_millis(3)),
        );
        let rows = rows(&path);
        let o = rows.iter().find(|r| r["role"] == "output").unwrap();
        assert_eq!(o["t_callback_us"].as_u64(), Some(callback));
        let t_log = o["t_log_arrival_us"].as_u64().unwrap();
        assert!(t_log >= callback + 3_000, "writer delay is inside t_log_arrival - t_callback");
        assert!(summary.max_callback_to_log_us >= 3_000);
        let end = sentinel(&rows);
        assert_eq!(end["max_write_lag_us"].as_u64(), Some(summary.max_write_lag_us));
        assert_eq!(end["max_callback_to_log_us"].as_u64(), Some(summary.max_callback_to_log_us));
        let _ = std::fs::remove_file(&path);
    }

    /// Codex 104 #3b: a callback received after the grace deadline is not
    /// accepted even when its synthetic `t_output` was before the deadline.
    #[test]
    fn a_callback_received_after_the_deadline_is_not_accepted() {
        let (tx, rx) = mpsc::sync_channel(4);
        let s = submission(id(L2Modality::Pc, 1));
        let deadline = now_us();
        let mut pending = FakePending::new(8);
        assert!(pending.push(s, s.t_submit_us));
        std::thread::sleep(Duration::from_millis(2));
        assert!(pending.emit_due(&tx, Some(deadline)));
        assert!(rx.try_recv().is_err(), "late receipt must not become an output");
        assert_eq!(pending.next_due(), Some(s.t_submit_us), "it stays pending -> unresolved");
        // Without a deadline (submissions open) it is emitted, receipt >= t_output.
        assert!(pending.emit_due(&tx, None));
        match rx.try_recv().unwrap() {
            Row::Output { t_output_us, t_callback_us, .. } => assert!(t_callback_us >= t_output_us),
            _ => panic!("expected an output"),
        }
    }

    // ---- F4: failure time, receiver faults ---------------------------------

    #[test]
    fn the_first_receiver_fault_is_written_once_with_its_own_time() {
        let path = tmp("receiver-fault");
        let out = L2Output::start(&path, config(L2SinkKind::Fake, Some(FakeSinkConfig::offsets(0, 0)), 8)).unwrap();
        let s = out.submitter();
        s.submit(id(L2Modality::Pc, 0), now_us());
        let t_fault = now_us();
        s.receiver_fault(t_fault, "current S3 route pc generation 0 ended Failed: wrong size");
        assert!(out.receiver_fault_noted());
        out.receiver_fault(t_fault + 5, "a later consequence");
        drop(s);
        let summary = out.finish().unwrap();
        assert!(summary.integrity_ok(L2SinkKind::Fake), "a receiver fault is not an L2 integrity failure");
        let rows = rows(&path);
        let faults: Vec<_> = rows.iter().filter(|r| r["role"] == "receiver_fault").collect();
        assert_eq!(faults.len(), 1);
        assert_eq!(faults[0]["t_us"].as_u64(), Some(t_fault));
        assert!(faults[0]["t_log_arrival_us"].as_u64() >= Some(t_fault));
        assert!(faults[0]["cause"].as_str().unwrap().contains("wrong size"));
        assert!(faults[0].get("seq").is_none(), "not an object row");
        let end = sentinel(&rows);
        assert_eq!(end["receiver_fault_us"].as_u64(), Some(t_fault));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn the_first_integrity_failure_time_and_reason_are_recorded() {
        let shared = Shared::default();
        shared.fail_at(200, FAIL_SINK_GONE);
        shared.fail_at(100, FAIL_DUPLICATE_LOGICAL_RELEASE);
        shared.fail_at(300, FAIL_CONSERVATION);
        assert_eq!(shared.first_failure(), Some((100, FAIL_DUPLICATE_LOGICAL_RELEASE)));
    }

    // ---- F6: producer quiescence ---------------------------------------------

    /// A producer handle alive at `finish` is an integrity failure recorded
    /// before the sentinel; anything it does after `finish` can only happen
    /// to a run whose sentinel is already false.
    #[test]
    fn a_producer_alive_at_finish_fails_the_sentinel_before_it_is_written() {
        let path = tmp("producer-alive");
        let mut cfg = config(L2SinkKind::Fake, Some(FakeSinkConfig::offsets(0, 0)), 8);
        cfg.report_window_us = 30_000;
        let out = L2Output::start(&path, cfg).unwrap();
        let leaked = out.submitter();
        assert_eq!(leaked.submit(id(L2Modality::Pc, 0), now_us()), L2SubmitOutcome::Submitted);
        let summary = out.finish().unwrap();
        assert!(summary.producers_alive_at_finish);
        assert!(!summary.integrity_ok(L2SinkKind::Fake));
        let sealed = std::fs::read(&path).unwrap();
        let rows = rows(&path);
        let end = sentinel(&rows);
        assert_eq!(end["integrity_ok"], false);
        assert_eq!(end["producers_alive_at_finish"], true);
        assert_eq!(end["first_integrity_failure_reason"], FAIL_PRODUCERS_ALIVE_AT_FINISH);
        // The leaked handle can no longer submit; the file is unchanged.
        assert_eq!(leaked.submit(id(L2Modality::Pc, 1), now_us()), L2SubmitOutcome::AfterClose);
        assert_eq!(std::fs::read(&path).unwrap(), sealed);
        let _ = std::fs::remove_file(&path);
    }

    /// The stage's own handle is dropped before the deadline is delivered, so
    /// even a zero close grace sees no live producer when there is none.
    #[test]
    fn a_zero_close_grace_without_live_producers_is_not_a_quiescence_failure() {
        let path = tmp("zero-grace");
        let mut cfg = config(L2SinkKind::Null, None, 8);
        cfg.report_window_us = 0;
        let out = L2Output::start(&path, cfg).unwrap();
        drop(out.submitter());
        let summary = out.finish().unwrap();
        assert!(!summary.producers_alive_at_finish);
        assert!(summary.integrity_ok(L2SinkKind::Null), "{summary:?}");
        let _ = std::fs::remove_file(&path);
    }

    /// A producer that is still alive when `finish` starts but is dropped
    /// inside the close grace is quiescence, not a failure.
    #[test]
    fn a_producer_dropped_inside_the_close_grace_is_fine() {
        let path = tmp("late-drop");
        let mut cfg = config(L2SinkKind::Fake, Some(FakeSinkConfig::offsets(0, 0)), 8);
        cfg.report_window_us = 2_000_000;
        let out = L2Output::start(&path, cfg).unwrap();
        let s = out.submitter();
        assert_eq!(s.submit(id(L2Modality::Pc, 0), now_us()), L2SubmitOutcome::Submitted);
        let dropper = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            drop(s);
        });
        let started = std::time::Instant::now();
        let summary = out.finish().unwrap();
        dropper.join().unwrap();
        assert!(started.elapsed() < Duration::from_millis(1_500), "woken by the last drop");
        assert!(!summary.producers_alive_at_finish);
        assert!(summary.integrity_ok(L2SinkKind::Fake), "{summary:?}");
        let _ = std::fs::remove_file(&path);
    }

    /// A submission between `closed` and the sentinel is counted in the
    /// sentinel (never silently lost).
    #[test]
    fn a_submission_after_close_is_counted_before_the_sentinel() {
        let path = tmp("after-close");
        let out = L2Output::start(&path, config(L2SinkKind::Fake, Some(FakeSinkConfig::offsets(0, 0)), 8)).unwrap();
        let s = out.submitter();
        out.shared.closed.store(true, Ordering::Release);
        assert_eq!(s.submit(id(L2Modality::Pc, 0), now_us()), L2SubmitOutcome::AfterClose);
        drop(s);
        let summary = out.finish().unwrap();
        assert_eq!(summary.submit_after_close, 1);
        assert!(!summary.producers_alive_at_finish);
        assert!(!summary.integrity_ok(L2SinkKind::Fake));
        let rows = rows(&path);
        assert_eq!(sentinel(&rows)["first_integrity_failure_reason"], FAIL_SUBMIT_AFTER_CLOSE);
        assert!(rows.iter().all(|r| r["role"] != "submit"));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn dropping_the_stage_without_finish_never_writes_a_sentinel() {
        let path = tmp("early-drop");
        let out = L2Output::start(&path, config(L2SinkKind::Fake, Some(FakeSinkConfig::offsets(0, 0)), 8)).unwrap();
        let s = out.submitter();
        assert_eq!(s.submit(id(L2Modality::Pc, 0), now_us()), L2SubmitOutcome::Submitted);
        drop(s);
        drop(out);
        std::thread::sleep(Duration::from_millis(50));
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(!text.contains("instrumentation_end"));
        let _ = std::fs::remove_file(&path);
    }

    // ---- F7: bounded tracking -----------------------------------------------

    #[test]
    fn tracking_and_fake_pending_are_bounded_and_overflow_fails_integrity() {
        let path = tmp("bounded");
        let mut cfg = config(L2SinkKind::Fake, Some(FakeSinkConfig::offsets(0, 0)), 64);
        cfg.max_tracked_objects = 3;
        let out = L2Output::start(&path, cfg).unwrap();
        let s = out.submitter();
        for seq in 0..6 {
            s.submit(id(L2Modality::Pc, seq), now_us());
        }
        drop(s);
        let summary = out.finish().unwrap();
        assert!(summary.tracking_overflow > 0 || summary.fake_pending_overflow > 0, "{summary:?}");
        assert!(!summary.integrity_ok(L2SinkKind::Fake));

        let mut pending = FakePending::new(2);
        let s = submission(id(L2Modality::Pc, 1));
        assert!(pending.push(s, 1) && pending.push(s, 2));
        assert!(!pending.push(s, 3), "a full pending set refuses instead of growing");
        let _ = std::fs::remove_file(&path);
    }
}
