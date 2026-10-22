//! L2 output stage, milestone M1 (device-free).
//!
//! Governing documents: `md/20261022_실험1D_L2사다리_사전등록_문안.md` §3-1,
//! §3-5, §6; `md/20261022_L2_계측_설계초안.md` §2, §7; contract
//! `md/20260929_실험2_계약.md` §5–§6 (row roles `submit` / `output` /
//! `explicit_discard`, `t_submit_us`, `t_output_us`, `t_discard_us`,
//! `t_log_arrival_us`, final `instrumentation_end`).
//!
//! Shape:
//!
//! ```text
//!  release point (scheduler / B1 drain)                 sink thread             writer thread
//!  ─────────────────────────────────────                ───────────             ─────────────
//!  L2Submitter::submit()  --try_send(Item)-->  [bounded submission queue]  --> submit row ──┐
//!      │ (Full)                                     NullSink: nothing more                  │
//!      └-try_send(Discard before submit)------------------------------------------------>  [bounded row queue] --> L2 JSONL
//!                                                   FakeSink: output / discard  ──────────┘
//! ```
//!
//! * The release side only ever calls `try_send` (no file I/O, no JSON
//!   formatting, no logger mutex). A full submission queue is recorded as
//!   `explicit_discard(output_queue_full)` BEFORE submit; it never becomes an
//!   error of the release path.
//! * Every row carries the ORIGINAL identity of the released object (run_id,
//!   track, tier, seq, pts_us, event_id and, for S3, wire_track +
//!   route_generation). Nothing is ever rewritten from a ledger.
//! * The `instrumentation_end` sentinel is written only by
//!   [`L2Output::finish`], after the submission side is closed, the sink has
//!   drained its queue and its report window, and the sink thread has been
//!   joined; it is the last row by construction because the writer thread
//!   exits right after writing it. A run that ends without `finish` (early
//!   error return) has no sentinel — fail closed.
//! * All times are integer microseconds of `CLOCK_MONOTONIC` ([`crate::now_us`]).
//!
//! `NullSink` accepts submissions and never emits a terminal: it exists only
//! to measure the overhead of the submission path, and every one of its
//! submissions is `submitted_unresolved` by design (recorded in the meta row).
//! `FakeSink` emits a deterministic synthetic `output` with
//! `t_output_us = t_submit_us + offset(modality)`; it waits until that instant
//! and stamps `t_log_arrival_us` with a real clock read when it handles the
//! deferred output. It is NOT a device output.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Barrier};
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::{bail, Context, Result};

use crate::now_us;

/// L2 JSONL schema tag written into the meta row.
pub const L2_SCHEMA: &str = "event-pair-l2-v0";
pub const L2_CLOCK: &str = "CLOCK_MONOTONIC";

pub const DISCARD_OUTPUT_QUEUE_FULL: &str = "output_queue_full";
/// Reserved for the PC renderer (M3): a not-yet-committed frame replaced by a
/// newer one. Never used by the M1 sinks.
pub const DISCARD_SUPERSEDED_BEFORE_COMMIT: &str = "superseded_before_commit";
/// Reserved for the PC renderer (M3): `wp_presentation_feedback.discarded`.
pub const DISCARD_COMPOSITOR_DISCARDED: &str = "compositor_discarded";

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
    /// Required for `L2SinkKind::Fake`, ignored otherwise.
    pub fake: Option<FakeSinkConfig>,
    /// Bound of the release -> sink submission queue.
    pub queue_capacity: usize,
    /// Bound of the row queue into the single writer thread.
    pub row_capacity: usize,
    /// After submissions are closed, how long the sink may still resolve
    /// already-submitted objects before it stops (the report window).
    pub report_window_us: u64,
}

impl L2Config {
    pub fn validate(&self) -> Result<()> {
        if self.run_id.is_empty() {
            bail!("L2 run_id must be non-empty");
        }
        if self.queue_capacity == 0 || self.row_capacity == 0 {
            bail!("L2 queue capacities must be positive");
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
    /// `finish` already started; counted as an integrity failure.
    AfterClose,
    /// The sink thread is gone; counted as an integrity failure.
    SinkGone,
}

/// Counters that the release side increments (the writer reads them for the
/// sentinel). Any non-zero value other than `queue_full` is an integrity
/// failure of the L2 instrumentation.
#[derive(Debug, Default)]
struct SharedCounters {
    queue_full: AtomicU64,
    row_overflow: AtomicU64,
    submit_after_close: AtomicU64,
    sink_gone: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct L2Summary {
    pub submitted: u64,
    pub output: u64,
    pub discarded_before_submit: u64,
    pub discarded_after_submit: u64,
    pub submitted_unresolved: u64,
    pub row_overflow: u64,
    pub submit_after_close: u64,
    pub sink_gone: u64,
    /// A terminal whose identity was never submitted or already resolved.
    pub terminal_without_open_submit: u64,
}

impl L2Summary {
    /// Instrumentation-level integrity (registration §6). `NullSink` runs are
    /// never L2 evidence; their unresolved submissions are expected.
    pub fn integrity_ok(&self, sink: L2SinkKind) -> bool {
        self.row_overflow == 0
            && self.submit_after_close == 0
            && self.sink_gone == 0
            && self.terminal_without_open_submit == 0
            && (sink == L2SinkKind::Null || self.submitted_unresolved == 0)
    }
}

enum SinkMsg {
    Item(L2Submission),
    Close,
}

enum Row {
    Submit(L2Submission),
    Output {
        submission: L2Submission,
        t_output_us: u64,
        t_log_arrival_us: u64,
        device: &'static str,
    },
    Discard {
        identity: L2Identity,
        t_release_us: u64,
        t_submit_us: Option<u64>,
        t_discard_us: u64,
        reason: &'static str,
    },
    End,
}

/// Cloneable, non-blocking submission handle held at the release points.
#[derive(Clone)]
pub struct L2Submitter {
    items: SyncSender<SinkMsg>,
    rows: SyncSender<Row>,
    closed: Arc<AtomicBool>,
    counters: Arc<SharedCounters>,
    force_queue_full: bool,
}

impl L2Submitter {
    /// Hand one RELEASED object to the output stage. Never blocks, never
    /// fails the caller. `t_submit_us` is read here and clamped to
    /// `t_release_us` so `t_release <= t_submit` holds by construction.
    pub fn submit(&self, identity: L2Identity, t_release_us: u64) -> L2SubmitOutcome {
        if self.closed.load(Ordering::Acquire) {
            self.counters.submit_after_close.fetch_add(1, Ordering::Relaxed);
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
            self.counters.sink_gone.fetch_add(1, Ordering::Relaxed);
            return L2SubmitOutcome::SinkGone;
        }
        self.counters.queue_full.fetch_add(1, Ordering::Relaxed);
        let row = Row::Discard {
            identity,
            t_release_us,
            t_submit_us: None,
            t_discard_us: now_us().max(t_release_us),
            reason: DISCARD_OUTPUT_QUEUE_FULL,
        };
        if self.rows.try_send(row).is_err() {
            self.counters.row_overflow.fetch_add(1, Ordering::Relaxed);
        }
        L2SubmitOutcome::QueueFull
    }
}

/// The running output stage: meta row written, sink + writer threads alive.
pub struct L2Output {
    config: L2Config,
    path: PathBuf,
    submitter: L2Submitter,
    sink: Option<JoinHandle<()>>,
    writer: Option<JoinHandle<std::io::Result<L2Summary>>>,
}

impl L2Output {
    /// Create the L2 JSONL (truncating), write the meta row synchronously and
    /// start the sink and writer threads.
    pub fn start(path: &Path, config: L2Config) -> Result<Self> {
        config.validate()?;
        let file = File::create(path)
            .with_context(|| format!("create L2 log {}", path.display()))?;
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
            "report_window_us": config.report_window_us,
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
        let counters = Arc::new(SharedCounters::default());
        let writer = {
            let run_id = config.run_id.clone();
            let sink = config.sink;
            let counters = counters.clone();
            std::thread::Builder::new()
                .name("l2-writer".into())
                .spawn(move || writer_loop(w, rows_rx, run_id, sink, counters))
                .context("spawn L2 writer thread")?
        };
        let sink = {
            let rows = rows_tx.clone();
            let kind = config.sink;
            let fake = config.fake.clone();
            let report_window_us = config.report_window_us;
            std::thread::Builder::new()
                .name("l2-sink".into())
                .spawn(move || sink_loop(kind, fake, items_rx, rows, report_window_us))
                .context("spawn L2 sink thread")?
        };
        let submitter = L2Submitter {
            items: items_tx,
            rows: rows_tx,
            closed: Arc::new(AtomicBool::new(false)),
            counters,
            force_queue_full: config.fake.as_ref().is_some_and(|f| f.force_queue_full),
        };
        Ok(Self {
            config,
            path: path.to_path_buf(),
            submitter,
            sink: Some(sink),
            writer: Some(writer),
        })
    }

    pub fn submitter(&self) -> L2Submitter {
        self.submitter.clone()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn sink_kind(&self) -> L2SinkKind {
        self.config.sink
    }

    /// Registration §3-5 order: stop submissions -> sink drains its queue and
    /// its report window -> sink thread joined -> `instrumentation_end` ->
    /// flush/close. Blocking; call from `spawn_blocking` in async code.
    pub fn finish(mut self) -> Result<L2Summary> {
        self.submitter.closed.store(true, Ordering::Release);
        // Blocking send: the sink is consuming, so space appears; a dead sink
        // makes this fail immediately instead of hanging.
        let _ = self.submitter.items.send(SinkMsg::Close);
        if let Some(sink) = self.sink.take() {
            if sink.join().is_err() {
                bail!("L2 sink thread panicked");
            }
        }
        self.submitter
            .rows
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
/// resolve in submission order.
struct FakePending {
    heap: BinaryHeap<Reverse<(u64, u64)>>,
    items: HashMap<u64, L2Submission>,
    order: u64,
}

impl FakePending {
    fn new() -> Self {
        Self { heap: BinaryHeap::new(), items: HashMap::new(), order: 0 }
    }

    fn push(&mut self, submission: L2Submission, t_output_us: u64) {
        self.heap.push(Reverse((t_output_us, self.order)));
        self.items.insert(self.order, submission);
        self.order += 1;
    }

    fn next_due(&self) -> Option<u64> {
        self.heap.peek().map(|Reverse((t, _))| *t)
    }

    /// Emit every output whose instant has passed (and, during the report
    /// window, is not beyond `limit`). `t_log_arrival_us` is a real clock read
    /// taken when the deferred output is handled.
    fn emit_due(&mut self, rows: &SyncSender<Row>, limit: Option<u64>) -> bool {
        while let Some(Reverse((t_output_us, key))) = self.heap.peek().copied() {
            let now = now_us();
            if t_output_us > now || limit.is_some_and(|l| t_output_us > l) {
                break;
            }
            self.heap.pop();
            let submission = self.items.remove(&key).expect("heap and map agree");
            let row = Row::Output {
                submission,
                t_output_us,
                t_log_arrival_us: now_us(),
                device: "fake",
            };
            if rows.send(row).is_err() {
                return false;
            }
        }
        true
    }
}

fn sink_loop(
    kind: L2SinkKind,
    fake: Option<FakeSinkConfig>,
    items: Receiver<SinkMsg>,
    rows: SyncSender<Row>,
    report_window_us: u64,
) {
    if let Some(gate) = fake.as_ref().and_then(|f| f.start_gate.clone()) {
        gate.wait();
    }
    let emit_outputs =
        kind == L2SinkKind::Fake && !fake.as_ref().is_some_and(|f| f.drop_terminals);
    let mut pending = FakePending::new();
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
            pending.push(submission, submission.t_submit_us.saturating_add(offset));
        }
        true
    };

    // Phase 1: submissions open.
    loop {
        let msg = match pending.next_due() {
            Some(due) => {
                let wait = Duration::from_micros(due.saturating_sub(now_us()));
                match items.recv_timeout(wait) {
                    Ok(msg) => Some(msg),
                    Err(RecvTimeoutError::Timeout) => None,
                    Err(RecvTimeoutError::Disconnected) => Some(SinkMsg::Close),
                }
            }
            None => Some(items.recv().unwrap_or(SinkMsg::Close)),
        };
        match msg {
            Some(SinkMsg::Item(submission)) => {
                if !accept(&mut pending, submission) {
                    return;
                }
            }
            Some(SinkMsg::Close) => break,
            None => {}
        }
        if !pending.emit_due(&rows, None) {
            return;
        }
    }

    // Phase 2: submissions closed. Whatever was queued before the close gets
    // its submit row; then the report window runs.
    while let Ok(SinkMsg::Item(submission)) = items.try_recv() {
        if !accept(&mut pending, submission) {
            return;
        }
    }
    let deadline = now_us().saturating_add(report_window_us);
    loop {
        if !pending.emit_due(&rows, Some(deadline)) {
            return;
        }
        match pending.next_due() {
            Some(due) if due <= deadline => {
                std::thread::sleep(Duration::from_micros(due.saturating_sub(now_us())));
            }
            // Nothing left, or only outputs beyond the window: those stay
            // unresolved (no row) and the sentinel counts them.
            _ => break,
        }
    }
    // A submission that raced the close is still given its submit row so it
    // cannot vanish; it is unresolved by construction.
    while let Ok(SinkMsg::Item(submission)) = items.try_recv() {
        if rows.send(Row::Submit(submission)).is_err() {
            return;
        }
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

fn writer_loop(
    mut w: BufWriter<File>,
    rows: Receiver<Row>,
    run_id: String,
    sink: L2SinkKind,
    counters: Arc<SharedCounters>,
) -> std::io::Result<L2Summary> {
    let mut summary = L2Summary::default();
    let mut open: HashSet<L2Identity> = HashSet::new();
    let mut ended = false;
    while let Ok(row) = rows.recv() {
        let value = match row {
            Row::Submit(s) => {
                summary.submitted += 1;
                open.insert(s.identity);
                let mut m = identity_json(&run_id, &s.identity);
                m.insert("role".into(), "submit".into());
                m.insert("t_release_us".into(), s.t_release_us.into());
                m.insert("t_submit_us".into(), s.t_submit_us.into());
                m.insert("t_us".into(), now_us().into());
                m
            }
            Row::Output { submission: s, t_output_us, t_log_arrival_us, device } => {
                summary.output += 1;
                if !open.remove(&s.identity) {
                    summary.terminal_without_open_submit += 1;
                }
                let mut m = identity_json(&run_id, &s.identity);
                m.insert("role".into(), "output".into());
                m.insert("t_release_us".into(), s.t_release_us.into());
                m.insert("t_submit_us".into(), s.t_submit_us.into());
                m.insert("t_output_us".into(), t_output_us.into());
                m.insert("t_log_arrival_us".into(), t_log_arrival_us.into());
                // Modality-specific device evidence (PC presentation feedback,
                // ALSA status) is filled by the M2/M3 sinks.
                m.insert(
                    "output_detail".into(),
                    serde_json::json!({ "device": device, "modality": s.identity.modality.as_str() }),
                );
                m.insert("t_us".into(), now_us().into());
                m
            }
            Row::Discard { identity, t_release_us, t_submit_us, t_discard_us, reason } => {
                match t_submit_us {
                    None => summary.discarded_before_submit += 1,
                    Some(_) => {
                        summary.discarded_after_submit += 1;
                        if !open.remove(&identity) {
                            summary.terminal_without_open_submit += 1;
                        }
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
                m.insert("t_us".into(), now_us().into());
                m
            }
            Row::End => {
                ended = true;
                break;
            }
        };
        writeln!(w, "{}", serde_json::Value::Object(value))?;
    }
    if !ended {
        // Every sender vanished without `finish`: fail closed, no sentinel.
        w.flush()?;
        return Err(std::io::Error::other("L2 writer ended without finish"));
    }
    // Rows that raced the close on the release side (queue-full discards
    // only) are still in the channel; they precede the sentinel.
    while let Ok(row) = rows.try_recv() {
        if let Row::Discard { identity, t_release_us, t_discard_us, reason, t_submit_us: None } = row {
            summary.discarded_before_submit += 1;
            let mut m = identity_json(&run_id, &identity);
            m.insert("role".into(), "explicit_discard".into());
            m.insert("reason".into(), reason.into());
            m.insert("discard_phase".into(), "before_submit".into());
            m.insert("t_release_us".into(), t_release_us.into());
            m.insert("t_submit_us".into(), serde_json::Value::Null);
            m.insert("t_discard_us".into(), t_discard_us.into());
            m.insert("t_us".into(), now_us().into());
            writeln!(w, "{}", serde_json::Value::Object(m))?;
        }
    }
    summary.submitted_unresolved = open.len() as u64;
    summary.row_overflow = counters.row_overflow.load(Ordering::Relaxed);
    summary.submit_after_close = counters.submit_after_close.load(Ordering::Relaxed);
    summary.sink_gone = counters.sink_gone.load(Ordering::Relaxed);
    let t_end = now_us();
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
        "row_overflow": summary.row_overflow,
        "submit_after_close": summary.submit_after_close,
        "sink_gone": summary.sink_gone,
        "terminal_without_open_submit": summary.terminal_without_open_submit,
        "integrity_ok": summary.integrity_ok(sink),
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
        std::env::temp_dir().join(format!("skew-l2-{}-{name}.jsonl", std::process::id()))
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
            let t_release = now_us();
            assert_eq!(s.submit(identity, t_release), L2SubmitOutcome::Submitted);
            submitted.push(identity);
        }
        drop(s);
        let summary = out.finish().unwrap();
        assert_eq!(summary.submitted, 20);
        assert_eq!(summary.output, 20);
        assert_eq!(summary.submitted_unresolved, 0);
        assert!(summary.integrity_ok(L2SinkKind::Fake));
        let rows = rows(&path);
        assert_eq!(rows[0]["role"], "meta");
        assert_eq!(rows[0]["l2_schema"], L2_SCHEMA);
        assert_eq!(rows[0]["clock"], L2_CLOCK);
        assert_eq!(rows[0]["sink"], "fake");
        assert_eq!(rows.last().unwrap()["role"], "instrumentation_end");
        assert_eq!(rows.last().unwrap()["end_us"], rows.last().unwrap()["t_us"]);
        assert_eq!(rows.iter().filter(|r| r["role"] == "instrumentation_end").count(), 1);
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
            assert!(o["t_output_us"].as_u64() <= o["t_log_arrival_us"].as_u64());
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
        assert_eq!(rows.last().unwrap()["role"], "instrumentation_end");
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
        let rows = rows(&path);
        for seq in 2..5u32 {
            let k = key(&serde_json::Value::Object(identity_json("x", &id(L2Modality::Pc, seq))));
            assert!(rows.iter().all(|r| !(r["role"] == "submit" && key(r) == k)));
            assert_eq!(
                rows.iter().filter(|r| r["role"] == "explicit_discard" && key(r) == k).count(),
                1
            );
        }
        assert_eq!(rows.last().unwrap()["role"], "instrumentation_end");
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
        assert!(!summary.integrity_ok(L2SinkKind::Fake));
        let rows = rows(&path);
        let end = rows.last().unwrap();
        assert_eq!(end["role"], "instrumentation_end");
        assert_eq!(end["submitted_unresolved"], 3);
        assert_eq!(end["integrity_ok"], false);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn an_output_beyond_the_report_window_stays_unresolved() {
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
        assert!(summary.integrity_ok(L2SinkKind::Null));
        let rows = rows(&path);
        assert_eq!(rows[0]["null_sink_never_resolves"], true);
        assert_eq!(rows.last().unwrap()["role"], "instrumentation_end");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_submission_after_finish_started_is_refused_and_counted() {
        let path = tmp("after-close");
        let out = L2Output::start(&path, config(L2SinkKind::Fake, Some(FakeSinkConfig::offsets(0, 0)), 8)).unwrap();
        let leaked = out.submitter();
        let closed = out.submitter.closed.clone();
        closed.store(true, Ordering::Release);
        assert_eq!(leaked.submit(id(L2Modality::Pc, 0), now_us()), L2SubmitOutcome::AfterClose);
        let summary = out.finish().unwrap();
        assert_eq!(summary.submit_after_close, 1);
        assert!(!summary.integrity_ok(L2SinkKind::Fake));
        // The leaked handle outlives finish; it still cannot write anything.
        assert_eq!(leaked.submit(id(L2Modality::Pc, 1), now_us()), L2SubmitOutcome::AfterClose);
        let rows = rows(&path);
        assert_eq!(rows.last().unwrap()["role"], "instrumentation_end");
        assert!(rows.iter().all(|r| r["role"] != "submit"));
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
    }
}
