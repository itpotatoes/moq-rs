//! The PC Wayland lane (registration 1-D §3-2, §3-3b; milestone M3): two
//! threads around the pure state of `crate::l2_pc`.
//!
//! ```text
//!  [bounded submission queue: pc] --> render worker ----offer----> [pending slot (1 frame)]
//!        submit row, decode (draco), render;           |  replaced -> superseded_before_commit
//!        payload_invalid / decode_failed rows          v
//!                                              commit thread (owns the compositor connection)
//!                         commit opportunity + free buffer -> write, commit (feedback requested first)
//!                         presented -> output, discarded -> compositor_discarded, device failures
//! ```
//!
//! Rows go to the writer with `try_send` from both threads (a full row queue
//! is `row_queue_overflow`, never a stall of the compositor thread). Close:
//! `finish` sends `Closing` into the submission queue; the worker wakes the
//! commit thread, processes every submission of the producers (bounded by
//! the close deadline) and reports done; the commit thread commits the last
//! pending frame and waits until every committed frame has its feedback or
//! the deadline passed, records committed frames still without feedback as
//! the device failure `wayland_feedback_missing` (they stay unresolved: a
//! committed frame ends only through its feedback), sends its counters
//! (`lanes.pc`) and disconnects. A stage dropped without `finish` abandons:
//! no counters, no sentinel (fail closed).

use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::Arc;

use super::{
    micros, L2Modality, L2Submission, OutputDetail, Row, Shared, SinkMsg, WaylandBackendKind, WaylandOutputDetail,
    WaylandSinkConfig, DISCARD_COMPOSITOR_DISCARDED, DISCARD_DEVICE_FAILURE, DISCARD_PAYLOAD_INVALID,
    DISCARD_SUPERSEDED_BEFORE_COMMIT, FAIL_PRODUCERS_ALIVE_AT_FINISH, FAIL_ROW_QUEUE_OVERFLOW, L2_SCHEMA,
    L2_WAYLAND_DIAGNOSTICS_SCHEMA,
};
use crate::l2_pc::{
    presentation_flag_names, BackendEvent, EngineOutcome, FramePreparer, Offer, PendingSlot, PixelPool, PrepareError,
    PresentBackend, PresentEngine, Presented, SimCompositor, StartupInfo, Waker, CLOCK_MONOTONIC_ID,
    DISCARD_DECODE_FAILED, FAIL_PC_DECODE_FAILED, FAIL_PC_PAYLOAD_INVALID, FAIL_WAYLAND_CONNECTION_LOST,
    FAIL_WAYLAND_FEEDBACK_MISSING,
};
use crate::l2_wayland::{canvas_layout, WaylandBackend};
use crate::now_us;

/// What the lane hands back once the compositor is ready (meta `wayland`).
pub(crate) struct PcLaneReady {
    pub startup: StartupInfo,
    pub side: u32,
}

/// Bound on the anomaly details kept for the sentinel.
const MAX_ANOMALY_DETAILS: usize = 16;
/// Bound on the records of the probe-only diagnostics file.
const DIAG_MAX_RECORDS: usize = 1_000_000;

/// The probe-only diagnostics file: the raw `presented` values (ns) the L2
/// JSONL must not carry (registration 11-11 rule, M3). Buffered in memory
/// and written after the lane stopped presenting. Never an analysis time
/// axis.
pub(crate) struct PcDiagLog {
    file: BufWriter<File>,
    run_id: String,
    records: Vec<(u64, Presented, u64)>,
    dropped: u64,
}

impl PcDiagLog {
    pub(crate) fn new(file: BufWriter<File>, run_id: String) -> Self {
        Self { file, run_id, records: Vec::new(), dropped: 0 }
    }

    fn note(&mut self, commit_id: u64, presented: Presented, t_callback_us: u64) {
        if self.records.len() < DIAG_MAX_RECORDS {
            self.records.push((commit_id, presented, t_callback_us));
        } else {
            self.dropped += 1;
        }
    }

    fn write(mut self, startup: &StartupInfo) -> std::io::Result<(u64, u64)> {
        let raw = |p: &Presented| {
            serde_json::json!({
                "tv_sec": p.tv_sec, "tv_nsec": p.tv_nsec, "refresh_ns": p.refresh_ns,
                "seq": p.seq, "flags": p.flags, "flag_names": presentation_flag_names(p.flags),
                "sync_outputs": p.sync_outputs,
            })
        };
        let header = serde_json::json!({
            "role": "diagnostics_meta",
            "diagnostics_schema": L2_WAYLAND_DIAGNOSTICS_SCHEMA,
            "l2_schema": L2_SCHEMA,
            "run_id": self.run_id,
            "analysis_time_axis": false,
            "purpose": "M3 display verification only: the raw wp_presentation_feedback.presented values the L2 JSONL does not carry; never an analysis time axis, never joined to the ledger",
            "fields": {
                "tv_sec": { "unit": "s", "clock": "the wp_presentation clock_id (verified CLOCK_MONOTONIC at start)", "origin": "presented tv_sec_hi << 32 | tv_sec_lo" },
                "tv_nsec": { "unit": "ns", "clock": "same", "origin": "presented tv_nsec" },
                "refresh_ns": { "unit": "ns", "kind": "duration predicted to the next refresh, 0 = unknown", "origin": "presented refresh" },
                "seq": { "unit": "count", "kind": "MSC refresh counter, not a time", "origin": "presented seq_hi << 32 | seq_lo" },
                "flags": { "unit": "bits", "origin": "presented flags (kind)" },
                "t_callback_us": { "unit": "us", "clock": "CLOCK_MONOTONIC", "origin": "clock read when the event was handled (as in the L2 row)" },
            },
        });
        writeln!(self.file, "{header}")?;
        if let Some(p) = &startup.startup_presented {
            let row = serde_json::json!({ "role": "startup_presented", "commit_id": 0, "raw": raw(p) });
            writeln!(self.file, "{row}")?;
        }
        for (commit_id, p, t_callback_us) in &self.records {
            let row = serde_json::json!({ "role": "presented", "commit_id": commit_id, "t_callback_us": t_callback_us, "raw": raw(p) });
            writeln!(self.file, "{row}")?;
        }
        let records = self.records.len() as u64;
        let end = serde_json::json!({ "role": "diagnostics_end", "presented_records": records, "dropped_records": self.dropped });
        writeln!(self.file, "{end}")?;
        self.file.flush()?;
        Ok((records, self.dropped))
    }
}

/// Row hand-off of one lane thread.
struct RowSink {
    rows: SyncSender<Row>,
    shared: Arc<Shared>,
    row_overflow: u64,
    writer_gone: bool,
    discards_not_emitted_after_deadline: u64,
}

impl RowSink {
    fn new(rows: SyncSender<Row>, shared: Arc<Shared>) -> Self {
        Self { rows, shared, row_overflow: 0, writer_gone: false, discards_not_emitted_after_deadline: 0 }
    }

    fn row(&mut self, row: Row, event_us: u64) {
        match self.rows.try_send(row) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.row_overflow += 1;
                self.shared.row_overflow.fetch_add(1, Ordering::Relaxed);
                self.shared.fail_occurred(event_us, FAIL_ROW_QUEUE_OVERFLOW);
            }
            Err(TrySendError::Disconnected(_)) => self.writer_gone = true,
        }
    }

    /// An after-submit discard at `t_discard_us`, emitted only strictly
    /// before the close deadline (otherwise the submission stays unresolved).
    fn discard_after(&mut self, s: L2Submission, reason: &'static str, decode_us: Option<(u64, u64)>, t_discard_us: u64) {
        let t_discard_us = t_discard_us.max(s.t_submit_us);
        if self.shared.close_deadline().is_some_and(|deadline| t_discard_us >= deadline) {
            self.discards_not_emitted_after_deadline += 1;
            return;
        }
        let row = Row::Discard {
            identity: s.identity,
            t_release_us: s.t_release_us,
            t_submit_us: Some(s.t_submit_us),
            t_discard_us,
            reason,
            decode_us,
        };
        self.row(row, t_discard_us);
    }
}

/// What the worker and the commit thread share.
struct PcLink {
    pending: PendingSlot,
    waker: Waker,
    pool: PixelPool,
    worker_done: AtomicBool,
    abandoned: AtomicBool,
}

#[derive(Default)]
struct WorkerStats {
    frames_received: u64,
    frames_rendered: u64,
    superseded_before_commit: u64,
    payload_invalid: u64,
    decode_failed: u64,
    device_failure_discards: u64,
    not_rendered_after_deadline: u64,
    decode_us_max: Option<u64>,
    render_us_max: Option<u64>,
    first_frame_error: Option<String>,
    row_overflow: u64,
    discards_not_emitted_after_deadline: u64,
}

struct Worker {
    preparer: FramePreparer,
    out: RowSink,
    link: Arc<PcLink>,
    stats: WorkerStats,
}

impl Worker {
    fn handle(&mut self, s: L2Submission, payload: Option<bytes::Bytes>) {
        self.out.row(Row::Submit(s), s.t_submit_us);
        self.stats.frames_received += 1;
        if self.out.shared.close_deadline().is_some_and(|deadline| now_us() >= deadline) {
            // Nothing can be presented any more: it stays unresolved.
            self.stats.not_rendered_after_deadline += 1;
            return;
        }
        if self.link.pending.failed() {
            self.stats.device_failure_discards += 1;
            self.out.discard_after(s, DISCARD_DEVICE_FAILURE, None, now_us());
            return;
        }
        let pixels = self.link.pool.take();
        let mut clock = now_us;
        match self.preparer.prepare(s, payload.as_deref(), pixels, &mut clock) {
            Ok(frame) => {
                self.stats.frames_rendered += 1;
                if let Some((start, end)) = frame.timing.decode_us {
                    let d = end - start;
                    self.stats.decode_us_max = Some(self.stats.decode_us_max.map_or(d, |m| m.max(d)));
                }
                let r = frame.timing.t_render_end_us - frame.timing.t_render_start_us;
                self.stats.render_us_max = Some(self.stats.render_us_max.map_or(r, |m| m.max(r)));
                match self.link.pending.offer(frame) {
                    Offer::Accepted { superseded: None } => {}
                    Offer::Accepted { superseded: Some(old) } => {
                        self.stats.superseded_before_commit += 1;
                        let (submission, decode_us) = (old.submission, old.timing.decode_us);
                        self.link.pool.put(old.pixels);
                        self.out.discard_after(submission, DISCARD_SUPERSEDED_BEFORE_COMMIT, decode_us, now_us());
                    }
                    Offer::Refused(frame) => {
                        self.stats.device_failure_discards += 1;
                        let (submission, decode_us) = (frame.submission, frame.timing.decode_us);
                        self.link.pool.put(frame.pixels);
                        self.out.discard_after(submission, DISCARD_DEVICE_FAILURE, decode_us, now_us());
                    }
                }
                self.link.waker.wake();
            }
            Err((error, pixels)) => {
                self.link.pool.put(pixels);
                if self.stats.first_frame_error.is_none() {
                    self.stats.first_frame_error = Some(format!("seq {}: {}", s.identity.seq, error.detail()));
                }
                let decode_us = error.decode_us();
                match error {
                    PrepareError::PayloadInvalid { .. } => {
                        self.stats.payload_invalid += 1;
                        self.out.shared.fail_occurred(s.t_submit_us, FAIL_PC_PAYLOAD_INVALID);
                        self.out.discard_after(s, DISCARD_PAYLOAD_INVALID, decode_us, now_us());
                    }
                    PrepareError::DecodeFailed { .. } => {
                        // Registration 3-2: a measurement integrity failure.
                        self.stats.decode_failed += 1;
                        self.out.shared.fail_occurred(s.t_submit_us, FAIL_PC_DECODE_FAILED);
                        self.out.discard_after(s, DISCARD_DECODE_FAILED, decode_us, now_us());
                    }
                }
            }
        }
    }

    fn abandon(&mut self) {
        self.link.abandoned.store(true, Ordering::SeqCst);
        self.link.waker.wake();
    }
}

/// The render worker: release order in, frames into the pending slot.
fn worker_main(
    preparer: FramePreparer,
    items: Receiver<SinkMsg>,
    rows: SyncSender<Row>,
    shared: Arc<Shared>,
    link: Arc<PcLink>,
) -> WorkerStats {
    let mut w = Worker { preparer, out: RowSink::new(rows, shared.clone()), link, stats: WorkerStats::default() };
    // Phase 1: submissions open, until `finish` sends `Closing`. A
    // disconnect means the stage was dropped without `finish`: abandon.
    loop {
        match items.recv() {
            Ok(SinkMsg::Item(s, payload)) => w.handle(s, payload),
            Ok(SinkMsg::Closing) => break,
            Ok(SinkMsg::Wake) => {}
            Err(_) => {
                w.abandon();
                return w.stats;
            }
        }
        if w.out.writer_gone {
            w.abandon();
            return w.stats;
        }
    }
    // The commit thread learns the deadline now.
    w.link.waker.wake();
    let deadline = shared.close_deadline().expect("finish publishes the deadline before Closing");
    // Phase 2: every producer handle must go; every submission is processed
    // (rendered while there is time left before the deadline).
    let mut producers_gone = false;
    loop {
        if !producers_gone && shared.producers.load(Ordering::Acquire) == 0 {
            producers_gone = true;
        }
        // After reading zero, this drain sees every submission ever sent.
        while let Ok(msg) = items.try_recv() {
            if let SinkMsg::Item(s, payload) = msg {
                w.handle(s, payload);
            }
        }
        let now = now_us();
        if producers_gone || now >= deadline {
            break;
        }
        match items.recv_timeout(micros(deadline.saturating_sub(now))) {
            Ok(SinkMsg::Item(s, payload)) => w.handle(s, payload),
            Ok(_) | Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    if !producers_gone {
        if shared.producers.load(Ordering::Acquire) > 0 {
            shared.producers_alive_at_finish.store(true, Ordering::Release);
            shared.fail(FAIL_PRODUCERS_ALIVE_AT_FINISH);
        }
        // Whatever is still queued gets its submit row (and stays unresolved).
        while let Ok(msg) = items.try_recv() {
            if let SinkMsg::Item(s, _) = msg {
                w.out.row(Row::Submit(s), s.t_submit_us);
                w.stats.frames_received += 1;
                w.stats.not_rendered_after_deadline += 1;
            }
        }
    }
    w.stats.row_overflow = w.out.row_overflow;
    w.stats.discards_not_emitted_after_deadline = w.out.discards_not_emitted_after_deadline;
    w.link.worker_done.store(true, Ordering::SeqCst);
    w.link.waker.wake();
    w.stats
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Exit {
    Closed,
    Abandoned,
}

/// The commit thread's lane state.
struct CommitLane {
    backend: Box<dyn PresentBackend>,
    backend_name: &'static str,
    engine: PresentEngine,
    link: Arc<PcLink>,
    out: RowSink,
    side: u32,
    backend_lost: bool,
    /// The clock read before the last compositor call that succeeded.
    last_good_us: u64,
    waiting_for_buffer: bool,
    outputs_after_deadline: u64,
    device_failure_discards: u64,
    anomalies: Vec<String>,
    committed_without_feedback_at_close: u64,
    pending_at_close: bool,
    diagnostics: Option<PcDiagLog>,
}

impl CommitLane {
    fn try_commit(&mut self) {
        if self.backend_lost || !self.engine.commit_opportunity() || !self.link.pending.has_frame() {
            return;
        }
        let Some(buffer) = self.engine.free_buffer() else {
            if !self.waiting_for_buffer {
                self.waiting_for_buffer = true;
                self.engine.note_no_free_buffer();
            }
            return;
        };
        self.waiting_for_buffer = false;
        let Some(frame) = self.link.pending.take() else { return };
        let t_write_us = now_us();
        if let Err(error) = self.backend.write_frame(buffer, self.side, &frame.pixels) {
            self.backend_lost = true;
            self.device_failure(FAIL_WAYLAND_CONNECTION_LOST, now_us(), error.detail);
            // Never committed: its terminal is device_failure.
            self.device_failure_discards += 1;
            let (submission, decode_us) = (frame.submission, frame.timing.decode_us);
            self.link.pool.put(frame.pixels);
            self.out.discard_after(submission, DISCARD_DEVICE_FAILURE, decode_us, now_us());
            return;
        }
        self.last_good_us = t_write_us;
        let t_commit_us = now_us();
        let (commit_id, pixels) = self.engine.commit(frame, buffer, t_commit_us);
        self.link.pool.put(pixels);
        match self.backend.commit(buffer, commit_id) {
            Ok(()) => self.last_good_us = t_commit_us,
            Err(error) => {
                // The commit may have reached the compositor: the frame keeps
                // waiting for a feedback that cannot come (unresolved).
                self.backend_lost = true;
                self.device_failure(FAIL_WAYLAND_CONNECTION_LOST, now_us(), error.detail);
            }
        }
    }

    fn device_failure(&mut self, reason: &'static str, detected_us: u64, detail: String) {
        if self.engine.fail(reason, self.last_good_us, detected_us, detail) {
            self.out.shared.fail_untimed_detected(detected_us, reason);
        }
        if let Some(frame) = self.link.pending.fail() {
            self.device_failure_discards += 1;
            let (submission, decode_us) = (frame.submission, frame.timing.decode_us);
            self.link.pool.put(frame.pixels);
            self.out.discard_after(submission, DISCARD_DEVICE_FAILURE, decode_us, now_us());
        }
    }

    fn apply(&mut self, event: BackendEvent, t_callback_us: u64) {
        let deadline = self.out.shared.close_deadline();
        match self.engine.on_event(event, t_callback_us) {
            None => {}
            Some(EngineOutcome::Output { frame, presented, t_output_us, t_callback_us }) => {
                if deadline.is_some_and(|d| t_callback_us >= d) {
                    // Not accepted: it stays unresolved.
                    self.outputs_after_deadline += 1;
                    return;
                }
                if let Some(diag) = self.diagnostics.as_mut() {
                    diag.note(frame.commit_id, presented, t_callback_us);
                }
                let detail = WaylandOutputDetail {
                    backend: self.backend_name,
                    commit_id: frame.commit_id,
                    buffer: frame.buffer,
                    timing: frame.timing,
                    t_commit_us: frame.t_commit_us,
                    presented,
                    info: frame.info,
                };
                let row = Row::Output {
                    submission: frame.submission,
                    t_output_us,
                    t_callback_us,
                    detail: OutputDetail::Wayland(Box::new(detail)),
                };
                self.out.row(row, t_callback_us);
            }
            Some(EngineOutcome::Discarded { frame, t_callback_us }) => {
                self.out.discard_after(frame.submission, DISCARD_COMPOSITOR_DISCARDED, frame.timing.decode_us, t_callback_us);
            }
            Some(EngineOutcome::Anomaly { reason, t_us, detail }) => {
                self.out.shared.fail_occurred(t_us, reason);
                if self.anomalies.len() < MAX_ANOMALY_DETAILS {
                    self.anomalies.push(format!("{reason}: {detail}"));
                }
            }
            Some(EngineOutcome::DeviceFailure { reason, detail }) => self.device_failure(reason, t_callback_us, detail),
        }
    }

    fn run(&mut self) -> Exit {
        loop {
            if self.link.abandoned.load(Ordering::SeqCst) || self.out.writer_gone {
                return Exit::Abandoned;
            }
            self.try_commit();
            let deadline = self.out.shared.close_deadline();
            if deadline.is_some_and(|d| now_us() >= d) {
                return Exit::Closed;
            }
            if self.link.worker_done.load(Ordering::SeqCst) {
                let more_to_commit =
                    self.link.pending.has_frame() && !self.backend_lost && self.engine.failure().is_none();
                let more_feedback = self.engine.awaiting_feedback() > 0 && !self.backend_lost;
                if !more_to_commit && !more_feedback {
                    return Exit::Closed;
                }
            }
            if self.backend_lost {
                // Nothing can arrive from the compositor any more.
                self.link.waker.wait(deadline);
                continue;
            }
            let t_before = now_us();
            match self.backend.wait(deadline, &self.link.waker) {
                Ok(events) => {
                    self.last_good_us = t_before;
                    for event in events {
                        let t_callback_us = now_us();
                        self.apply(event, t_callback_us);
                    }
                }
                Err(error) => {
                    self.backend_lost = true;
                    self.device_failure(FAIL_WAYLAND_CONNECTION_LOST, now_us(), error.detail);
                }
            }
        }
    }

    /// Close statistics; the worker has been joined.
    fn finish(mut self, startup: &StartupInfo, representation: &'static str, worker: WorkerStats) {
        if let Some(frame) = self.link.pending.take() {
            // Rendered, never committed before the close ended: unresolved.
            self.pending_at_close = true;
            self.link.pool.put(frame.pixels);
        }
        let unresolved = self.engine.unresolved();
        if !unresolved.is_empty() {
            let detected = now_us();
            let detail = format!(
                "{} committed frame(s) without feedback at close (commits {}..={})",
                unresolved.len(),
                unresolved[0].commit_id,
                unresolved[unresolved.len() - 1].commit_id
            );
            self.committed_without_feedback_at_close = unresolved.len() as u64;
            // Recorded even after an earlier failure (the first one stays in
            // lanes.pc.device_failure; the count is in the lane counters).
            self.device_failure(FAIL_WAYLAND_FEEDBACK_MISSING, detected, detail);
        }
        let diagnostics = self.diagnostics.take().map(|diag| match diag.write(startup) {
            Ok((records, dropped)) => serde_json::json!({ "written": true, "presented_records": records, "dropped_records": dropped }),
            Err(error) => serde_json::json!({ "written": false, "error": error.to_string() }),
        });
        let stats = serde_json::json!({
            "kind": "wayland",
            "backend": self.backend_name,
            "representation": representation,
            "canvas_side_px": self.side,
            "frames_received": worker.frames_received,
            "frames_rendered": worker.frames_rendered,
            "superseded_before_commit": worker.superseded_before_commit,
            "payload_invalid": worker.payload_invalid,
            "decode_failed": worker.decode_failed,
            "device_failure_discards": worker.device_failure_discards + self.device_failure_discards,
            "not_rendered_after_deadline": worker.not_rendered_after_deadline,
            "decode_us_max": worker.decode_us_max,
            "render_us_max": worker.render_us_max,
            "first_frame_error": worker.first_frame_error,
            "engine": self.engine.stats().to_json(),
            "outputs_after_deadline": self.outputs_after_deadline,
            "discards_not_emitted_after_deadline": worker.discards_not_emitted_after_deadline + self.out.discards_not_emitted_after_deadline,
            "committed_without_feedback_at_close": self.committed_without_feedback_at_close,
            "pending_at_close": self.pending_at_close,
            "anomalies": self.anomalies,
            "backend_lost": self.backend_lost,
            "device_failure": self.engine.failure().map_or(serde_json::Value::Null, |f| f.to_json()),
            "row_overflow": worker.row_overflow + self.out.row_overflow,
            "diagnostics": diagnostics,
        });
        // The compositor is done with: a blocking hand-off is harmless now.
        let _ = self.out.rows.send(Row::LaneStats { modality: L2Modality::Pc, stats });
    }
}

/// The PC Wayland lane thread: connect (or simulate) -> check the clock ->
/// spawn the render worker -> ready -> commit loop -> close.
pub(crate) fn wayland_lane_main(
    cfg: WaylandSinkConfig,
    items: Receiver<SinkMsg>,
    rows: SyncSender<Row>,
    shared: Arc<Shared>,
    ready: mpsc::Sender<std::result::Result<PcLaneReady, String>>,
    diagnostics: Option<PcDiagLog>,
) {
    let fail = |message: String| {
        let _ = ready.send(Err(message));
    };
    let backend: Box<dyn PresentBackend> = match &cfg.backend {
        WaylandBackendKind::Display => match WaylandBackend::connect(cfg.buffers, cfg.ready_timeout_us) {
            Ok(backend) => Box::new(backend),
            Err(error) => return fail(error),
        },
        WaylandBackendKind::Simulated(sim) => Box::new(SimCompositor::new(sim.clone())),
    };
    let startup = backend.startup().clone();
    // Registration 3-2: clk_id must be CLOCK_MONOTONIC before the first
    // sample, otherwise the run refuses to start (no meta row).
    if startup.clock_id != CLOCK_MONOTONIC_ID {
        return fail(format!(
            "wp_presentation clock_id {} is not CLOCK_MONOTONIC ({CLOCK_MONOTONIC_ID}): the run refuses to start",
            startup.clock_id
        ));
    }
    if startup.buffers != cfg.buffers {
        return fail(format!("{} buffers instead of {}", startup.buffers, cfg.buffers));
    }
    let (side, _, _) = canvas_layout(startup.width, startup.height);
    if side == 0 {
        return fail("the surface has no pixels".into());
    }
    let waker = match Waker::new() {
        Ok(waker) => waker,
        Err(error) => return fail(format!("eventfd: {error}")),
    };
    let link = Arc::new(PcLink {
        pending: PendingSlot::new(),
        waker,
        pool: PixelPool::new(4),
        worker_done: AtomicBool::new(false),
        abandoned: AtomicBool::new(false),
    });
    let preparer = FramePreparer::new(cfg.representation, cfg.tiers.clone(), side);
    let worker = {
        let (link, rows, shared) = (link.clone(), rows.clone(), shared.clone());
        std::thread::Builder::new()
            .name("l2-sink-pc-render".into())
            .spawn(move || worker_main(preparer, items, rows, shared, link))
    };
    let worker = match worker {
        Ok(worker) => worker,
        Err(error) => return fail(format!("spawn the PC render worker: {error}")),
    };
    if ready.send(Ok(PcLaneReady { startup: startup.clone(), side })).is_err() {
        // `start` gave up and closed the queue: the worker abandons.
        let _ = worker.join();
        return;
    }
    let held = startup.held.clone();
    let mut lane = CommitLane {
        backend,
        backend_name: cfg.backend.name(),
        engine: PresentEngine::new(startup.buffers, &held),
        link: link.clone(),
        out: RowSink::new(rows, shared),
        side,
        backend_lost: false,
        last_good_us: now_us(),
        waiting_for_buffer: false,
        outputs_after_deadline: 0,
        device_failure_discards: 0,
        anomalies: Vec::new(),
        committed_without_feedback_at_close: 0,
        pending_at_close: false,
        diagnostics,
    };
    let exit = lane.run();
    if exit == Exit::Abandoned {
        // Make sure the worker does not wait for a commit thread that is gone.
        link.abandoned.store(true, Ordering::SeqCst);
    }
    let stats = match worker.join() {
        Ok(stats) => stats,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    if exit == Exit::Abandoned {
        return; // no counters, no sentinel: fail closed
    }
    lane.finish(&startup, cfg.representation.as_str(), stats);
}
