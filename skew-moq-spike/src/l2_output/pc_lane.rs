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
//! (`lanes.pc`) and disconnects. No commit is started at or after the close
//! deadline (v1.6). A rendered frame still pending at close (only possible
//! when the close deadline ended the commit loop) keeps no terminal and is
//! classified by what kept it from its commit at the close deadline
//! (registration 3-3b, v1.6; a frame callback or buffer release received
//! at or after the deadline does not change the branch): (i) the frame
//! callback of the last commit is missing -> `wayland_frame_callback_missing`;
//! (ii) the callback fired but the compositor holds every buffer ->
//! `wayland_buffer_release_missing` (`buffer_release_missing_at_close`) —
//! both untimed PC device failures —; (iii) neither: the frame reached the
//! slot too late for a commit to start before the deadline -> a timed
//! unresolved submit, counted in `committable_pending_at_close`. A stage
//! dropped without `finish` abandons: no counters, no sentinel (fail
//! closed).

use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::Arc;

use super::{
    micros, L2Modality, L2Submission, LaneStartError, OutputDetail, Row, Shared, SinkMsg, WaylandBackendKind,
    WaylandOutputDetail, WaylandSinkConfig, DISCARD_COMPOSITOR_DISCARDED, DISCARD_DEVICE_FAILURE,
    DISCARD_PAYLOAD_INVALID, DISCARD_SUPERSEDED_BEFORE_COMMIT, FAIL_PRODUCERS_ALIVE_AT_FINISH,
    FAIL_ROW_QUEUE_OVERFLOW, L2_SCHEMA, L2_WAYLAND_DIAGNOSTICS_SCHEMA,
};
use crate::l2_pc::{
    presentation_flag_names, BackendEvent, BufferState, EngineOutcome, FramePreparer, Offer, PendingSlot, PixelPool,
    PrepareError, PresentBackend, PresentEngine, Presented, SimCompositor, StartupInfo, Waker, CLOCK_MONOTONIC_ID,
    DISCARD_DECODE_FAILED, FAIL_PC_DECODE_FAILED, FAIL_PC_PAYLOAD_INVALID, FAIL_WAYLAND_BUFFER_RELEASE_MISSING,
    FAIL_WAYLAND_CONNECTION_LOST, FAIL_WAYLAND_FEEDBACK_MISSING, FAIL_WAYLAND_FRAME_CALLBACK_MISSING,
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
    /// Anomalies with the raw presentation behind them (Codex 112 P2-5:
    /// the L2 JSONL keeps only the reason and the commit id).
    anomalies: Vec<(&'static str, u64, Presented)>,
    dropped: u64,
}

impl PcDiagLog {
    pub(crate) fn new(file: BufWriter<File>, run_id: String) -> Self {
        Self { file, run_id, records: Vec::new(), anomalies: Vec::new(), dropped: 0 }
    }

    fn note(&mut self, commit_id: u64, presented: Presented, t_callback_us: u64) {
        if self.records.len() < DIAG_MAX_RECORDS {
            self.records.push((commit_id, presented, t_callback_us));
        } else {
            self.dropped += 1;
        }
    }

    fn note_anomaly(&mut self, reason: &'static str, t_us: u64, raw: Presented) {
        if self.anomalies.len() < DIAG_MAX_RECORDS {
            self.anomalies.push((reason, t_us, raw));
        } else {
            self.dropped += 1;
        }
    }

    fn write(mut self, startup: &StartupInfo) -> std::io::Result<(u64, u64)> {
        let raw = |p: &Presented| {
            serde_json::json!({
                "tv_sec": p.tv_sec, "tv_nsec": p.tv_nsec, "refresh_ns": p.refresh_ns,
                "seq": p.seq, "flags": p.flags, "flag_names": presentation_flag_names(p.flags),
                "sync_outputs": p.sync_outputs, "sync_output": p.sync_output,
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
                "seq": { "unit": "count", "kind": "the compositor-reported sequence; not assumed to count vblanks, not a time", "origin": "presented seq_hi << 32 | seq_lo" },
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
        for (reason, t_us, p) in &self.anomalies {
            let row = serde_json::json!({ "role": "anomaly", "reason": reason, "t_us": t_us, "raw": raw(p) });
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

/// Registration 3-3b (v1.6) close branch (ii): the state that kept the
/// pending frame from its commit — the frame callback had fired, but the
/// compositor held every buffer at the close deadline. Sentinel
/// `lanes.pc.buffer_release_missing_at_close` (null when branch (ii) did not
/// happen):
/// `{"pending_seq": u32, "buffers": n, "buffers_held": n,
///   "holding_commit_ids": [commit id holding buffer 0, .., buffer n-1]}`
/// (`buffers_held == buffers`; commit id 0 = the startup commit).
#[derive(Debug, Clone, PartialEq, Eq)]
struct BufferReleaseMissing {
    /// `seq` of the rendered frame that could not be committed.
    pending_seq: u32,
    /// The wl_shm pool size.
    buffers: usize,
    /// Buffers the compositor held at the close deadline.
    buffers_held: usize,
    /// Index = buffer: the commit that holds it.
    holding_commit_ids: Vec<u64>,
}

impl BufferReleaseMissing {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "pending_seq": self.pending_seq,
            "buffers": self.buffers,
            "buffers_held": self.buffers_held,
            "holding_commit_ids": self.holding_commit_ids,
        })
    }
}

/// v1.6: what the close classification of a pending frame reads — the
/// engine's outstanding frame callback and buffer states AT THE CLOSE
/// DEADLINE. Taken just before the first compositor event received at or
/// after the deadline is applied, so that a late frame callback or buffer
/// release (still applied and counted in `lanes.pc.engine`) cannot turn
/// branch (i) or (ii) into (iii). No snapshot = no event was applied after
/// the deadline; since no commit is started then either, the engine state at
/// `finish` IS the state at the deadline.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DeadlineState {
    outstanding_frame_callback: Option<u64>,
    buffers: Vec<BufferState>,
}

impl DeadlineState {
    fn of(engine: &PresentEngine) -> Self {
        Self { outstanding_frame_callback: engine.outstanding_frame_callback(), buffers: engine.buffer_states().to_vec() }
    }
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
    /// Feedback received at or after the close deadline: not accepted, the
    /// commit stays outstanding (Codex 112 P1).
    feedback_after_deadline: u64,
    device_failure_discards: u64,
    anomalies: Vec<String>,
    committed_without_feedback_at_close: u64,
    pending_at_close: bool,
    /// The commit whose missing frame callback kept the pending frame from
    /// being committed before the close deadline (registration 3-3b, Codex
    /// 114; branch (i)).
    frame_callback_missing_at_close: Option<u64>,
    /// Branch (ii) (v1.6): every buffer held by the compositor at the close
    /// deadline.
    buffer_release_missing_at_close: Option<BufferReleaseMissing>,
    /// Branch (iii) (v1.6): a pending frame that was committable at the
    /// close deadline (callback fired, a buffer free) — it reached the slot
    /// after the commit loop's deadline exit, or too shortly before the
    /// deadline for a commit to start. 0 or 1.
    committable_pending_at_close: u64,
    /// The close classification's input at the deadline ([`DeadlineState`]).
    at_deadline: Option<DeadlineState>,
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
        // v1.6: no commit is STARTED at or after the close deadline (the
        // same boundary as the terminal acceptance `< close_deadline_us`):
        // its feedback could never be accepted, and the commit would turn a
        // frame the deadline cut off (iii) or a stall (i)/(ii) into a
        // spurious wayland_feedback_missing. Read immediately before the
        // pending frame is taken; the frame then stays pending for `finish`.
        if self.out.shared.close_deadline().is_some_and(|deadline| now_us() >= deadline) {
            return;
        }
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
        // v1.6: freeze the close classification's input before the first
        // event received at or after the deadline changes the engine (the
        // event itself is still applied and counted below).
        if self.at_deadline.is_none() && deadline.is_some_and(|d| t_callback_us >= d) {
            self.at_deadline = Some(DeadlineState::of(&self.engine));
        }
        // Codex 112 P1: a feedback received at or after the close deadline is
        // not accepted AND its commit must stay outstanding, so that the
        // close check records it as the PC device failure
        // wayland_feedback_missing. Checked BEFORE the engine resolves
        // (removes) the commit. Feedback for an unknown or resolved commit
        // still goes to the engine (an anomaly at its receipt).
        if let BackendEvent::Presented { commit_id, .. } | BackendEvent::Discarded { commit_id } = &event {
            if deadline.is_some_and(|d| t_callback_us >= d) && self.engine.is_awaiting(*commit_id) {
                self.feedback_after_deadline += 1;
                return;
            }
        }
        for outcome in self.engine.on_event(event, t_callback_us) {
            match outcome {
                EngineOutcome::Output { frame, presented, timing, t_output_us, t_callback_us } => {
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
                        presented_timing: timing,
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
                EngineOutcome::Discarded { frame, t_callback_us } => {
                    self.out.discard_after(frame.submission, DISCARD_COMPOSITOR_DISCARDED, frame.timing.decode_us, t_callback_us);
                }
                EngineOutcome::Anomaly { reason, t_us, detail, raw } => {
                    self.out.shared.fail_occurred(t_us, reason);
                    if self.anomalies.len() < MAX_ANOMALY_DETAILS {
                        self.anomalies.push(format!("{reason}: {detail}"));
                    }
                    if let (Some(diag), Some(raw)) = (self.diagnostics.as_mut(), raw) {
                        diag.note_anomaly(reason, t_us, raw);
                    }
                }
                EngineOutcome::DeviceFailure { reason, detail } => self.device_failure(reason, t_callback_us, detail),
            }
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
        let pending = self.link.pending.take();
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
        if let Some(frame) = pending {
            // Rendered, never committed before the close ended: it keeps no
            // terminal and stays unresolved (it was taken from the slot
            // above, so a device failure below discards nothing; a discard
            // now would not precede the deadline anyway). A pending frame
            // can only be left here when the close deadline ended `run`:
            // its other exit needs an empty slot or a device failure, which
            // empties the slot. Registration 3-3b (v1.6), by the engine
            // state AT THE CLOSE DEADLINE that kept the frame from its
            // commit ([`DeadlineState`]: a frame callback or buffer release
            // received at or after the deadline does not change the branch;
            // never `commit_opportunity()`, which a feedback-missing failure
            // recorded above already turned false):
            // (i)   the frame callback of the last commit is still missing:
            //       untimed PC device failure wayland_frame_callback_missing
            //       (Codex 114);
            // (ii)  the callback fired but the compositor holds every buffer
            //       (no wl_buffer.release): untimed PC device failure
            //       wayland_buffer_release_missing, the block recorded in
            //       buffer_release_missing_at_close;
            // (iii) the callback fired and a buffer is free: the frame reached
            //       the slot after the commit loop's deadline exit (the worker
            //       checks the deadline BEFORE it decodes and renders, and
            //       offers afterwards) or too shortly before the deadline for
            //       a commit to start (`try_commit` starts none at or after
            //       it) — deliberately a timed unresolved submit at its
            //       t_submit_us, like a frame not rendered before the
            //       deadline; counted in committable_pending_at_close.
            // With no pending frame neither (i) nor (ii) is a failure: a last
            // frame presented without its callback, or buffers still held
            // with nothing waiting, are not.
            self.pending_at_close = true;
            let seq = frame.submission.identity.seq;
            let at_deadline = self.at_deadline.take().unwrap_or_else(|| DeadlineState::of(&self.engine));
            if let Some(blocking) = at_deadline.outstanding_frame_callback {
                self.frame_callback_missing_at_close = Some(blocking);
                let detail = format!(
                    "the frame callback of commit {blocking} was still missing at the close deadline, so the rendered frame seq {seq} was never committed"
                );
                self.device_failure(FAIL_WAYLAND_FRAME_CALLBACK_MISSING, now_us(), detail);
            } else if !at_deadline.buffers.contains(&BufferState::Free) {
                let holding_commit_ids: Vec<u64> = at_deadline
                    .buffers
                    .iter()
                    .filter_map(|state| match state {
                        BufferState::Held { commit_id } => Some(*commit_id),
                        BufferState::Free => None,
                    })
                    .collect();
                let blocked = BufferReleaseMissing {
                    pending_seq: seq,
                    buffers: at_deadline.buffers.len(),
                    buffers_held: holding_commit_ids.len(),
                    holding_commit_ids,
                };
                let detail = format!(
                    "the frame callback had fired but the compositor still held all {} buffers at the close deadline (held by commits {:?}, no wl_buffer.release), so the rendered frame seq {seq} was never committed",
                    blocked.buffers, blocked.holding_commit_ids
                );
                self.buffer_release_missing_at_close = Some(blocked);
                self.device_failure(FAIL_WAYLAND_BUFFER_RELEASE_MISSING, now_us(), detail);
            } else {
                self.committable_pending_at_close += 1;
            }
            self.link.pool.put(frame.pixels);
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
            "feedback_after_deadline": self.feedback_after_deadline,
            "discards_not_emitted_after_deadline": worker.discards_not_emitted_after_deadline + self.out.discards_not_emitted_after_deadline,
            "committed_without_feedback_at_close": self.committed_without_feedback_at_close,
            "pending_at_close": self.pending_at_close,
            "frame_callback_missing_at_close": self.frame_callback_missing_at_close,
            "buffer_release_missing_at_close": self.buffer_release_missing_at_close.as_ref().map(BufferReleaseMissing::to_json),
            "committable_pending_at_close": self.committable_pending_at_close,
            "anomalies": self.anomalies,
            "backend_lost": self.backend_lost,
            "device_failure": self.engine.failure().map_or(serde_json::Value::Null, |f| f.to_json()),
            "row_overflow": worker.row_overflow + self.out.row_overflow,
            "diagnostics": diagnostics,
            "compositor_log": self.backend.run_log(),
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
    ready: mpsc::Sender<std::result::Result<PcLaneReady, LaneStartError>>,
    diagnostics: Option<PcDiagLog>,
) {
    let fail = |error: LaneStartError| {
        let _ = ready.send(Err(error));
    };
    let backend: Box<dyn PresentBackend> = match &cfg.backend {
        // The lane's own startup deadline is shorter than the stage's
        // outer ready timeout, so its stage-specific cause arrives first.
        WaylandBackendKind::Display => {
            match WaylandBackend::connect(cfg.buffers, cfg.ready_timeout_us - cfg.ready_timeout_us / 5) {
                Ok(backend) => Box::new(backend),
                Err(error) => return fail(error),
            }
        }
        WaylandBackendKind::Simulated(sim) => Box::new(SimCompositor::new(sim.clone())),
    };
    let startup = backend.startup().clone();
    // Registration 3-2: clk_id must be CLOCK_MONOTONIC before the first
    // sample, otherwise the run refuses to start (no meta row).
    if startup.clock_id != CLOCK_MONOTONIC_ID {
        return fail(LaneStartError::new(
            "clock",
            false,
            format!(
                "wp_presentation clock_id {} is not CLOCK_MONOTONIC ({CLOCK_MONOTONIC_ID}): the run refuses to start",
                startup.clock_id
            ),
        ));
    }
    if startup.buffers != cfg.buffers {
        return fail(LaneStartError::new("buffers", false, format!("{} buffers instead of {}", startup.buffers, cfg.buffers)));
    }
    let (side, _, _) = canvas_layout(startup.width, startup.height);
    if side == 0 {
        return fail(LaneStartError::new("configure", false, "the surface has no pixels"));
    }
    let waker = match Waker::new() {
        Ok(waker) => waker,
        Err(error) => return fail(LaneStartError::new("worker", false, format!("eventfd: {error}"))),
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
        Err(error) => return fail(LaneStartError::new("worker", false, format!("spawn the PC render worker: {error}"))),
    };
    if ready.send(Ok(PcLaneReady { startup: startup.clone(), side })).is_err() {
        // `start` gave up and closed the queue: the worker abandons.
        let _ = worker.join();
        return;
    }
    let mut lane = CommitLane {
        backend,
        backend_name: cfg.backend.name(),
        engine: PresentEngine::from_startup(&startup),
        link: link.clone(),
        out: RowSink::new(rows, shared),
        side,
        backend_lost: false,
        last_good_us: now_us(),
        waiting_for_buffer: false,
        feedback_after_deadline: 0,
        device_failure_discards: 0,
        anomalies: Vec::new(),
        committed_without_feedback_at_close: 0,
        pending_at_close: false,
        frame_callback_missing_at_close: None,
        buffer_release_missing_at_close: None,
        committable_pending_at_close: 0,
        at_deadline: None,
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

#[cfg(test)]
mod tests {
    use super::super::{writer_loop, L2Identity, L2SinkKind, L2Sinks, L2Summary, FAIL_SUBMITTED_UNRESOLVED};
    use super::*;
    use crate::l2_pc::{FrameInfo, Offer, PcTiming, PendingFrame, SIM_OUTPUT};
    use std::io::BufWriter as Buf;

    const SINKS: L2Sinks = L2Sinks { pc: L2SinkKind::Wayland, haptic: L2SinkKind::Null };

    /// A backend that does nothing (the tests drive `apply` directly).
    struct ScriptBackend {
        startup: StartupInfo,
    }

    impl PresentBackend for ScriptBackend {
        fn startup(&self) -> &StartupInfo {
            &self.startup
        }
        fn write_frame(&mut self, _: usize, _: u32, _: &[u32]) -> Result<(), crate::l2_pc::BackendError> {
            Ok(())
        }
        fn commit(&mut self, _: usize, _: u64) -> Result<(), crate::l2_pc::BackendError> {
            Ok(())
        }
        fn wait(&mut self, _: Option<u64>, _: &Waker) -> Result<Vec<BackendEvent>, crate::l2_pc::BackendError> {
            Ok(Vec::new())
        }
    }

    fn startup() -> StartupInfo {
        StartupInfo {
            backend: "simulated",
            clock_id: CLOCK_MONOTONIC_ID,
            width: 64,
            height: 48,
            buffers: 4,
            held: Vec::new(),
            startup_presented: None,
            startup_commit_to_presented_us: None,
            output: Some(SIM_OUTPUT),
            details: serde_json::Value::Null,
        }
    }

    fn lane(rows: SyncSender<Row>, shared: Arc<Shared>, diagnostics: Option<PcDiagLog>) -> CommitLane {
        let link = Arc::new(PcLink {
            pending: PendingSlot::new(),
            waker: Waker::new().unwrap(),
            pool: PixelPool::new(1),
            worker_done: AtomicBool::new(true),
            abandoned: AtomicBool::new(false),
        });
        CommitLane {
            backend: Box::new(ScriptBackend { startup: startup() }),
            backend_name: "simulated",
            engine: PresentEngine::from_startup(&startup()),
            link,
            out: RowSink::new(rows, shared),
            side: 48,
            backend_lost: false,
            last_good_us: 245,
            waiting_for_buffer: false,
            feedback_after_deadline: 0,
            device_failure_discards: 0,
            anomalies: Vec::new(),
            committed_without_feedback_at_close: 0,
            pending_at_close: false,
            frame_callback_missing_at_close: None,
            buffer_release_missing_at_close: None,
            committable_pending_at_close: 0,
            at_deadline: None,
            diagnostics,
        }
    }

    fn presented(t_us: u64) -> Presented {
        Presented {
            tv_sec: t_us / 1_000_000,
            tv_nsec: ((t_us % 1_000_000) * 1_000) as u32,
            refresh_ns: 16_666_667,
            seq: 1,
            flags: 0x7,
            sync_outputs: 1,
            sync_output: Some(SIM_OUTPUT),
        }
    }

    /// The lane's rows plus `End`, through the real writer.
    fn ledger(rows_rx: Receiver<Row>, shared: Arc<Shared>, name: &str) -> (L2Summary, Vec<serde_json::Value>) {
        let input: Vec<Row> = rows_rx.try_iter().collect();
        let (tx, rx) = mpsc::sync_channel(input.len() + 1);
        for row in input {
            tx.send(row).unwrap();
        }
        tx.send(Row::End).unwrap();
        drop(tx);
        let path = std::env::temp_dir().join(format!("skew-pc-lane-{}-{}-{name}.jsonl", std::process::id(), now_us()));
        let file = std::fs::OpenOptions::new().write(true).create_new(true).open(&path).unwrap();
        let summary = writer_loop(Buf::new(file), rx, "l2-test".into(), SINKS, false, shared, 64).unwrap();
        let rows = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        let _ = std::fs::remove_file(&path);
        (summary, rows)
    }

    /// Codex 112 P1 regression, deterministic: submit 210, commit 240,
    /// close deadline 1,000; the feedback (`presented` or `discarded`) is
    /// handled at deadline-1, deadline, deadline+1, with and without a
    /// receiver abort at 100. Before the deadline it is the frame's terminal;
    /// at or after it the feedback is NOT accepted, the commit stays
    /// outstanding and the close records the PC device failure
    /// `wayland_feedback_missing` (untimed) — never a timed
    /// `submitted_unresolved` at 210 that an abort exception could rescue.
    #[test]
    fn codex112_feedback_at_or_after_the_deadline_leaves_the_commit_to_the_feedback_missing_check() {
        for presented_kind in [true, false] {
            for offset in [-1i64, 0, 1] {
                for abort in [false, true] {
                    let case = format!("presented={presented_kind} offset={offset} abort={abort}");
                    let (rows_tx, rows_rx) = mpsc::sync_channel(64);
                    let shared = Arc::new(Shared::default());
                    if abort {
                        shared.note_receiver_fault(100, "receiver abort");
                    }
                    let mut lane = lane(rows_tx.clone(), shared.clone(), None);
                    let identity = L2Identity {
                        modality: L2Modality::Pc,
                        tier: 2,
                        seq: 3,
                        pts_us: 100_000,
                        event_id: 4,
                        route: None,
                    };
                    let s = L2Submission { identity, t_release_us: 200, t_submit_us: 210 };
                    rows_tx.send(Row::Submit(s)).unwrap();
                    let frame = PendingFrame {
                        submission: s,
                        timing: PcTiming { decode_us: None, t_render_start_us: 220, t_render_end_us: 230 },
                        info: FrameInfo { points: 1, covered_pixels: 1, depth_scale: 4 },
                        pixels: Vec::new(),
                    };
                    let (commit_id, _) = lane.engine.commit(frame, 1, 240);
                    shared.finish_start_us.store(500, Ordering::SeqCst);
                    shared.close_deadline_us.store(1_000, Ordering::SeqCst);
                    let t_callback = (1_000 + offset) as u64;
                    let event = if presented_kind {
                        BackendEvent::Presented { commit_id, presented: presented(300) }
                    } else {
                        BackendEvent::Discarded { commit_id }
                    };
                    lane.apply(event, t_callback);
                    lane.finish(&startup(), "bin", WorkerStats::default());
                    drop(rows_tx);
                    let (summary, rows) = ledger(rows_rx, shared, "c112-p1");
                    let end = rows.last().unwrap();
                    let pc = &end["lanes"]["pc"];
                    if offset < 0 {
                        // Accepted: exactly one terminal, nothing missing.
                        let terminals = rows
                            .iter()
                            .filter(|r| r["role"] == "output" || r["role"] == "explicit_discard")
                            .count();
                        assert_eq!(terminals, 1, "{case}");
                        assert_eq!(summary.submitted_unresolved, 0, "{case}");
                        assert!(pc["device_failure"].is_null(), "{case}");
                        assert_eq!(pc["feedback_after_deadline"], 0, "{case}");
                        assert!(summary.integrity_ok(SINKS), "{case}: {summary:?}");
                    } else {
                        // Not accepted: no terminal, the commit was outstanding
                        // at close -> PC device failure, untimed.
                        assert!(rows.iter().all(|r| r["role"] != "output" && r["role"] != "explicit_discard"), "{case}");
                        assert_eq!(summary.submitted_unresolved, 1, "{case}");
                        assert_eq!(summary.first_integrity_failure_reason.as_deref(), Some(FAIL_WAYLAND_FEEDBACK_MISSING), "{case}");
                        assert_eq!(summary.first_integrity_failure_us, None, "{case}");
                        assert!(!summary.first_failure_time_determinable, "{case}");
                        assert_eq!(pc["device_failure"]["reason"], FAIL_WAYLAND_FEEDBACK_MISSING, "{case}");
                        assert_eq!(pc["committed_without_feedback_at_close"], 1, "{case}");
                        assert_eq!(pc["feedback_after_deadline"], 1, "{case}");
                        assert!(!summary.integrity_ok(SINKS), "{case}");
                    }
                    assert_eq!(summary.receiver_fault.as_ref().map(|(t, _)| *t), abort.then_some(100), "{case}");
                }
            }
        }
    }

    /// Codex 114 / registration 3-3b, deterministic. The last commit (A) was
    /// presented but its frame callback never came; a rendered frame (B,
    /// submit 210) therefore waited for its commit until the close deadline
    /// (1,000), with and without a receiver abort at 100. With B waiting this
    /// is the untimed PC device failure `wayland_frame_callback_missing` (first
    /// failure time null, not determinable) — never a timed unresolved
    /// submit at 210 that an abort exception could rescue. With nothing
    /// waiting, A's missing callback is no failure.
    #[test]
    fn codex114_a_missing_frame_callback_that_blocks_a_pending_frame_is_an_untimed_device_failure() {
        for pending in [true, false] {
            for abort in [true, false] {
                let case = format!("pending={pending} abort={abort}");
                let (rows_tx, rows_rx) = mpsc::sync_channel(64);
                let shared = Arc::new(Shared::default());
                if abort {
                    shared.note_receiver_fault(100, "receiver abort");
                }
                let mut lane = lane(rows_tx.clone(), shared.clone(), None);
                let pc = |seq: u32, t_submit_us: u64| L2Submission {
                    identity: L2Identity { modality: L2Modality::Pc, tier: 2, seq, pts_us: seq as u64 * 33_333, event_id: seq + 1, route: None },
                    t_release_us: t_submit_us - 1,
                    t_submit_us,
                };
                let frame = |s: L2Submission| PendingFrame {
                    submission: s,
                    timing: PcTiming { decode_us: None, t_render_start_us: s.t_submit_us + 1, t_render_end_us: s.t_submit_us + 2 },
                    info: FrameInfo { points: 1, covered_pixels: 1, depth_scale: 4 },
                    pixels: Vec::new(),
                };
                let a = pc(1, 150);
                rows_tx.send(Row::Submit(a)).unwrap();
                let (commit_a, _) = lane.engine.commit(frame(a), 1, 160);
                lane.apply(BackendEvent::Presented { commit_id: commit_a, presented: presented(170) }, 180);
                if pending {
                    let b = pc(2, 210);
                    rows_tx.send(Row::Submit(b)).unwrap();
                    assert!(matches!(lane.link.pending.offer(frame(b)), Offer::Accepted { superseded: None }));
                }
                shared.finish_start_us.store(500, Ordering::SeqCst);
                shared.close_deadline_us.store(1_000, Ordering::SeqCst);
                lane.finish(&startup(), "bin", WorkerStats::default());
                drop(rows_tx);
                let (summary, rows) = ledger(rows_rx, shared, "c114-callback");
                let pc_lane = &rows.last().unwrap()["lanes"]["pc"];
                assert_eq!(rows.iter().filter(|r| r["role"] == "output").count(), 1, "{case}: A keeps its output");
                if pending {
                    assert_eq!(summary.submitted_unresolved, 1, "{case}: B has no terminal");
                    assert_eq!(summary.first_integrity_failure_reason.as_deref(), Some(FAIL_WAYLAND_FRAME_CALLBACK_MISSING), "{case}");
                    assert_eq!(summary.first_integrity_failure_us, None, "{case}");
                    assert!(!summary.first_failure_time_determinable, "{case}");
                    assert_eq!(pc_lane["device_failure"]["reason"], FAIL_WAYLAND_FRAME_CALLBACK_MISSING, "{case}");
                    assert_eq!(pc_lane["device_failure"]["occurrence_time_determinable"], false, "{case}");
                    assert_eq!(pc_lane["frame_callback_missing_at_close"], commit_a, "{case}");
                    assert_eq!(pc_lane["pending_at_close"], true, "{case}");
                    assert!(!summary.integrity_ok(SINKS), "{case}");
                } else {
                    assert!(summary.integrity_ok(SINKS), "{case}: {summary:?}");
                    assert!(pc_lane["device_failure"].is_null(), "{case}");
                    assert!(pc_lane["frame_callback_missing_at_close"].is_null(), "{case}");
                    assert_eq!(pc_lane["pending_at_close"], false, "{case}");
                }
                assert_eq!(summary.receiver_fault.as_ref().map(|(t, _)| *t), abort.then_some(100), "{case}");
            }
        }
    }

    fn pc_submission(seq: u32, t_submit_us: u64) -> L2Submission {
        L2Submission {
            identity: L2Identity { modality: L2Modality::Pc, tier: 2, seq, pts_us: seq as u64 * 33_333, event_id: seq + 1, route: None },
            t_release_us: t_submit_us - 1,
            t_submit_us,
        }
    }

    fn rendered(s: L2Submission) -> PendingFrame {
        PendingFrame {
            submission: s,
            timing: PcTiming { decode_us: None, t_render_start_us: s.t_submit_us + 1, t_render_end_us: s.t_submit_us + 2 },
            info: FrameInfo { points: 1, covered_pixels: 1, depth_scale: 4 },
            pixels: Vec::new(),
        }
    }

    /// The engine state at close (registration 3-3b, v1.6).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum CloseState {
        /// (i) the frame callback of the last commit has not fired.
        CallbackMissing,
        /// (ii) the callback fired, the compositor holds every buffer.
        AllBuffersHeld,
        /// (iii) the callback fired and a buffer is free.
        Committable,
    }

    /// Registration 3-3b (v1.6), deterministic: the close-time three-way
    /// decision for a rendered frame still pending at close. The last commit
    /// A (submit 150, commit 160) was presented; at close (i) A's frame
    /// callback is missing, (ii) it fired but the compositor holds both
    /// buffers (startup commit 0 holds buffer 0, A buffer 1, no release), or
    /// (iii) it fired and buffers are free; a rendered frame B (submit 210)
    /// waits or not; with and without a receiver abort at 100; deadline
    /// 1,000. (i)/(ii) with B waiting: the untimed PC device failure of the
    /// branch (first failure time null, not determinable; B keeps no
    /// terminal and is counted unresolved) — never a timed unresolved at 210
    /// that an abort exception could rescue. (iii) with B waiting: the timed
    /// unresolved submit at 210, no device failure, counted in
    /// `committable_pending_at_close`. Without B no state is a failure.
    /// Exactly one branch marker is set iff a frame was pending.
    #[test]
    fn v1_6_a_frame_pending_at_close_is_classified_by_what_kept_it_from_its_commit() {
        for state in [CloseState::CallbackMissing, CloseState::AllBuffersHeld, CloseState::Committable] {
            for pending in [true, false] {
                for abort in [true, false] {
                    let case = format!("{state:?} pending={pending} abort={abort}");
                    let (rows_tx, rows_rx) = mpsc::sync_channel(64);
                    let shared = Arc::new(Shared::default());
                    if abort {
                        shared.note_receiver_fault(100, "receiver abort");
                    }
                    let mut lane = lane(rows_tx.clone(), shared.clone(), None);
                    if state == CloseState::AllBuffersHeld {
                        // Two buffers; the startup frame (commit 0) holds buffer 0.
                        lane.engine = PresentEngine::new(2, &[(0, 0)]);
                    }
                    let a = pc_submission(1, 150);
                    rows_tx.send(Row::Submit(a)).unwrap();
                    let buffer = lane.engine.free_buffer().unwrap();
                    let (commit_a, _) = lane.engine.commit(rendered(a), buffer, 160);
                    lane.apply(BackendEvent::Presented { commit_id: commit_a, presented: presented(170) }, 180);
                    if state != CloseState::CallbackMissing {
                        lane.apply(BackendEvent::FrameDone { commit_id: commit_a }, 190);
                    }
                    match state {
                        CloseState::CallbackMissing => assert_eq!(lane.engine.outstanding_frame_callback(), Some(commit_a), "{case}"),
                        CloseState::AllBuffersHeld => {
                            assert!(lane.engine.commit_opportunity() && lane.engine.free_buffer().is_none(), "{case}")
                        }
                        CloseState::Committable => {
                            assert!(lane.engine.commit_opportunity() && lane.engine.free_buffer().is_some(), "{case}")
                        }
                    }
                    if pending {
                        let b = pc_submission(2, 210);
                        rows_tx.send(Row::Submit(b)).unwrap();
                        assert!(matches!(lane.link.pending.offer(rendered(b)), Offer::Accepted { superseded: None }));
                    }
                    shared.finish_start_us.store(500, Ordering::SeqCst);
                    shared.close_deadline_us.store(1_000, Ordering::SeqCst);
                    lane.finish(&startup(), "bin", WorkerStats::default());
                    drop(rows_tx);
                    let (summary, rows) = ledger(rows_rx, shared, "v16-close");
                    let pc_lane = &rows.last().unwrap()["lanes"]["pc"];
                    assert_eq!(rows.iter().filter(|r| r["role"] == "output").count(), 1, "{case}: A keeps its output");
                    assert!(rows.iter().all(|r| r["role"] != "explicit_discard"), "{case}: B gets no terminal");
                    assert_eq!(summary.receiver_fault.as_ref().map(|(t, _)| *t), abort.then_some(100), "{case}");
                    assert_eq!(pc_lane["pending_at_close"], pending, "{case}");
                    let markers = [
                        !pc_lane["frame_callback_missing_at_close"].is_null(),
                        !pc_lane["buffer_release_missing_at_close"].is_null(),
                        pc_lane["committable_pending_at_close"] == 1,
                    ];
                    assert_eq!(markers.iter().filter(|m| **m).count(), usize::from(pending), "{case}: one branch iff a frame waits");
                    if !pending {
                        assert!(summary.integrity_ok(SINKS), "{case}: {summary:?}");
                        assert!(pc_lane["device_failure"].is_null(), "{case}");
                        assert_eq!(pc_lane["committable_pending_at_close"], 0, "{case}");
                        continue;
                    }
                    assert!(!summary.integrity_ok(SINKS), "{case}");
                    assert_eq!(summary.submitted_unresolved, 1, "{case}: B has no terminal");
                    match state {
                        CloseState::CallbackMissing | CloseState::AllBuffersHeld => {
                            let reason = if state == CloseState::CallbackMissing {
                                FAIL_WAYLAND_FRAME_CALLBACK_MISSING
                            } else {
                                FAIL_WAYLAND_BUFFER_RELEASE_MISSING
                            };
                            assert_eq!(summary.first_integrity_failure_reason.as_deref(), Some(reason), "{case}");
                            assert_eq!(summary.first_integrity_failure_us, None, "{case}");
                            assert!(!summary.first_failure_time_determinable, "{case}");
                            assert_eq!(pc_lane["device_failure"]["reason"], reason, "{case}");
                            assert_eq!(pc_lane["device_failure"]["occurrence_time_determinable"], false, "{case}");
                            assert_eq!(pc_lane["committable_pending_at_close"], 0, "{case}");
                        }
                        CloseState::Committable => {
                            assert_eq!(summary.first_integrity_failure_reason.as_deref(), Some(FAIL_SUBMITTED_UNRESOLVED), "{case}");
                            assert_eq!(summary.first_integrity_failure_us, Some(210), "{case}: timed at B's submit");
                            assert!(summary.first_failure_time_determinable, "{case}");
                            assert!(pc_lane["device_failure"].is_null(), "{case}");
                            assert_eq!(pc_lane["committable_pending_at_close"], 1, "{case}");
                        }
                    }
                    match state {
                        CloseState::CallbackMissing => assert_eq!(pc_lane["frame_callback_missing_at_close"], commit_a, "{case}"),
                        _ => assert!(pc_lane["frame_callback_missing_at_close"].is_null(), "{case}"),
                    }
                    match state {
                        CloseState::AllBuffersHeld => assert_eq!(
                            pc_lane["buffer_release_missing_at_close"],
                            serde_json::json!({ "pending_seq": 2, "buffers": 2, "buffers_held": 2, "holding_commit_ids": [0, commit_a] }),
                            "{case}"
                        ),
                        _ => assert!(pc_lane["buffer_release_missing_at_close"].is_null(), "{case}"),
                    }
                }
            }
        }
    }

    /// Registration 3-3b recording precedence (Codex 116 P2): the last
    /// commit A (submit 150) never gets its feedback, so at close it is a
    /// committed frame without feedback, recorded FIRST as the untimed
    /// wayland_feedback_missing; a rendered frame B (submit 210) is pending
    /// in branch (i) (A's callback missing), (ii) (callback fired, both
    /// buffers held) or (iii) (callback fired, buffers free); with and
    /// without a receiver abort at 100. lanes.pc.device_failure and the
    /// run's first failure are wayland_feedback_missing (untimed) in every
    /// case — for (iii) not the timed unresolved at 210 — while the branch
    /// marker is still set (`committable_pending_at_close == 1` for (iii)).
    #[test]
    fn v1_6_committed_frames_without_feedback_are_recorded_before_the_pending_branch() {
        for state in [CloseState::CallbackMissing, CloseState::AllBuffersHeld, CloseState::Committable] {
            for abort in [true, false] {
                let case = format!("{state:?} abort={abort}");
                let (rows_tx, rows_rx) = mpsc::sync_channel(64);
                let shared = Arc::new(Shared::default());
                if abort {
                    shared.note_receiver_fault(100, "receiver abort");
                }
                let mut lane = lane(rows_tx.clone(), shared.clone(), None);
                if state == CloseState::AllBuffersHeld {
                    lane.engine = PresentEngine::new(2, &[(0, 0)]);
                }
                let a = pc_submission(1, 150);
                rows_tx.send(Row::Submit(a)).unwrap();
                let buffer = lane.engine.free_buffer().unwrap();
                let (commit_a, _) = lane.engine.commit(rendered(a), buffer, 160);
                // No presented / discarded for A, ever.
                if state != CloseState::CallbackMissing {
                    lane.apply(BackendEvent::FrameDone { commit_id: commit_a }, 190);
                }
                let b = pc_submission(2, 210);
                rows_tx.send(Row::Submit(b)).unwrap();
                assert!(matches!(lane.link.pending.offer(rendered(b)), Offer::Accepted { superseded: None }));
                shared.finish_start_us.store(500, Ordering::SeqCst);
                shared.close_deadline_us.store(1_000, Ordering::SeqCst);
                lane.finish(&startup(), "bin", WorkerStats::default());
                drop(rows_tx);
                let (summary, rows) = ledger(rows_rx, shared, "v16-precedence");
                let pc_lane = &rows.last().unwrap()["lanes"]["pc"];
                assert!(rows.iter().all(|r| r["role"] != "output" && r["role"] != "explicit_discard"), "{case}: no terminal");
                assert_eq!(summary.submitted_unresolved, 2, "{case}: A and B");
                assert!(!summary.integrity_ok(SINKS), "{case}");
                assert_eq!(summary.first_integrity_failure_reason.as_deref(), Some(FAIL_WAYLAND_FEEDBACK_MISSING), "{case}");
                assert_eq!(summary.first_integrity_failure_us, None, "{case}: untimed, not B's submit at 210");
                assert!(!summary.first_failure_time_determinable, "{case}");
                assert_eq!(summary.receiver_fault.as_ref().map(|(t, _)| *t), abort.then_some(100), "{case}");
                assert_eq!(pc_lane["device_failure"]["reason"], FAIL_WAYLAND_FEEDBACK_MISSING, "{case}: the first PC device failure");
                assert_eq!(pc_lane["committed_without_feedback_at_close"], 1, "{case}");
                assert_eq!(pc_lane["pending_at_close"], true, "{case}");
                match state {
                    CloseState::CallbackMissing => assert_eq!(pc_lane["frame_callback_missing_at_close"], commit_a, "{case}"),
                    _ => assert!(pc_lane["frame_callback_missing_at_close"].is_null(), "{case}"),
                }
                match state {
                    CloseState::AllBuffersHeld => assert_eq!(
                        pc_lane["buffer_release_missing_at_close"],
                        serde_json::json!({ "pending_seq": 2, "buffers": 2, "buffers_held": 2, "holding_commit_ids": [0, commit_a] }),
                        "{case}"
                    ),
                    _ => assert!(pc_lane["buffer_release_missing_at_close"].is_null(), "{case}"),
                }
                assert_eq!(pc_lane["committable_pending_at_close"], u64::from(state == CloseState::Committable), "{case}: the marker stays");
            }
        }
    }

    /// v1.6 branch (iii) is reachable, replayed deterministically: the
    /// commit loop stops at the close deadline (`run`: `try_commit` finds no
    /// frame, then the deadline exit) and the render worker offers its frame
    /// only afterwards — `Worker::handle` checks the deadline BEFORE it
    /// decodes and renders and offers afterwards, so a frame whose render
    /// started before the deadline can reach the slot after the loop
    /// stopped. Committable at close (no callback outstanding, buffers
    /// free): a timed unresolved submit at its t_submit_us, no device
    /// failure, `committable_pending_at_close` 1.
    #[test]
    fn v1_6_a_frame_offered_after_the_commit_loop_stopped_at_the_deadline_stays_timed_unresolved() {
        let (rows_tx, rows_rx) = mpsc::sync_channel(64);
        let shared = Arc::new(Shared::default());
        let mut lane = lane(rows_tx.clone(), shared.clone(), None);
        shared.finish_start_us.store(500, Ordering::SeqCst);
        shared.close_deadline_us.store(1_000, Ordering::SeqCst);
        assert_eq!(lane.run(), Exit::Closed, "the deadline has passed");
        assert_eq!(lane.engine.stats().commits, 0, "nothing to commit before the deadline exit");
        let b = pc_submission(2, 210);
        rows_tx.send(Row::Submit(b)).unwrap();
        assert!(matches!(lane.link.pending.offer(rendered(b)), Offer::Accepted { superseded: None }));
        assert!(lane.engine.commit_opportunity() && lane.engine.free_buffer().is_some());
        lane.finish(&startup(), "bin", WorkerStats::default());
        drop(rows_tx);
        let (summary, rows) = ledger(rows_rx, shared, "v16-late-offer");
        let pc_lane = &rows.last().unwrap()["lanes"]["pc"];
        assert_eq!(summary.submitted_unresolved, 1);
        assert_eq!(summary.first_integrity_failure_reason.as_deref(), Some(FAIL_SUBMITTED_UNRESOLVED));
        assert_eq!(summary.first_integrity_failure_us, Some(210));
        assert!(summary.first_failure_time_determinable);
        assert!(pc_lane["device_failure"].is_null());
        assert_eq!(pc_lane["pending_at_close"], true);
        assert_eq!(pc_lane["committable_pending_at_close"], 1);
        assert!(pc_lane["frame_callback_missing_at_close"].is_null());
        assert!(pc_lane["buffer_release_missing_at_close"].is_null());
    }

    /// The LAST wait of a close that ran into its deadline: `wait` returns
    /// its scripted events only once the deadline it is given has passed,
    /// so `run` applies them with a receipt time at or after the deadline.
    struct LateBackend {
        startup: StartupInfo,
        late: Vec<BackendEvent>,
    }

    impl PresentBackend for LateBackend {
        fn startup(&self) -> &StartupInfo {
            &self.startup
        }
        fn write_frame(&mut self, _: usize, _: u32, _: &[u32]) -> Result<(), crate::l2_pc::BackendError> {
            Ok(())
        }
        fn commit(&mut self, _: usize, _: u64) -> Result<(), crate::l2_pc::BackendError> {
            Ok(())
        }
        fn wait(&mut self, until_us: Option<u64>, _: &Waker) -> Result<Vec<BackendEvent>, crate::l2_pc::BackendError> {
            let until = until_us.expect("the close deadline is published before the last wait");
            while now_us() < until {
                std::thread::sleep(std::time::Duration::from_micros(100));
            }
            Ok(std::mem::take(&mut self.late))
        }
    }

    /// v1.6 (coordinator decision on the e23ae17 report), through the real
    /// commit loop: the close runs into its deadline with a rendered frame B
    /// (submit 210) waiting, and the last wait returns, at or after the
    /// deadline, the event that would have unblocked B — (i) A's late frame
    /// callback, or (ii) a late wl_buffer.release with both buffers held.
    /// No commit is started (`engine.commits` stays 1, no
    /// wayland_feedback_missing), the late event IS applied and counted, and
    /// the branch is the one of the engine state AT THE DEADLINE: (i) or (ii),
    /// untimed, never (iii) (which an abort could excuse). Before v1.6 B was
    /// committed after the deadline and closed as wayland_feedback_missing.
    #[test]
    fn v1_6_a_late_callback_or_release_in_the_last_wait_neither_commits_nor_changes_the_branch() {
        for late_release in [false, true] {
            let case = if late_release { "(ii) + late wl_buffer.release" } else { "(i) + late frame callback" };
            let (rows_tx, rows_rx) = mpsc::sync_channel(64);
            let shared = Arc::new(Shared::default());
            let mut lane = lane(rows_tx.clone(), shared.clone(), None);
            if late_release {
                // Two buffers; the startup frame (commit 0) holds buffer 0.
                lane.engine = PresentEngine::new(2, &[(0, 0)]);
            }
            let a = pc_submission(1, 150);
            rows_tx.send(Row::Submit(a)).unwrap();
            let buffer = lane.engine.free_buffer().unwrap();
            let (commit_a, _) = lane.engine.commit(rendered(a), buffer, 160);
            lane.apply(BackendEvent::Presented { commit_id: commit_a, presented: presented(170) }, 180);
            if late_release {
                lane.apply(BackendEvent::FrameDone { commit_id: commit_a }, 190);
            }
            let b = pc_submission(2, 210);
            rows_tx.send(Row::Submit(b)).unwrap();
            assert!(matches!(lane.link.pending.offer(rendered(b)), Offer::Accepted { superseded: None }));
            let late = if late_release {
                BackendEvent::BufferReleased { buffer: 0 }
            } else {
                BackendEvent::FrameDone { commit_id: commit_a }
            };
            lane.backend = Box::new(LateBackend { startup: startup(), late: vec![late] });
            // A deadline 100 ms ahead: `run` reaches its wait well before it.
            let t_finish = now_us();
            shared.finish_start_us.store(t_finish, Ordering::SeqCst);
            shared.close_deadline_us.store(t_finish + 100_000, Ordering::SeqCst);
            assert_eq!(lane.run(), Exit::Closed, "{case}");
            assert!(now_us() >= t_finish + 100_000, "{case}: the loop ended at the deadline");
            assert_eq!(lane.engine.stats().commits, 1, "{case}: no commit at or after the deadline");
            assert!(lane.link.pending.has_frame(), "{case}: B still waits");
            // The late event was applied: B became committable — after the deadline.
            assert!(lane.engine.commit_opportunity() && lane.engine.free_buffer().is_some(), "{case}: late event applied");
            assert!(lane.at_deadline.is_some(), "{case}: the deadline state was frozen before it");
            lane.finish(&startup(), "bin", WorkerStats::default());
            drop(rows_tx);
            let (summary, rows) = ledger(rows_rx, shared, "v16-late-event");
            let pc_lane = &rows.last().unwrap()["lanes"]["pc"];
            let reason = if late_release { FAIL_WAYLAND_BUFFER_RELEASE_MISSING } else { FAIL_WAYLAND_FRAME_CALLBACK_MISSING };
            assert_eq!(rows.iter().filter(|r| r["role"] == "output").count(), 1, "{case}: A keeps its output");
            assert_eq!(summary.submitted_unresolved, 1, "{case}: B has no terminal");
            assert_eq!(summary.first_integrity_failure_reason.as_deref(), Some(reason), "{case}");
            assert_eq!(summary.first_integrity_failure_us, None, "{case}");
            assert!(!summary.first_failure_time_determinable, "{case}");
            assert_eq!(pc_lane["device_failure"]["reason"], reason, "{case}: not wayland_feedback_missing");
            assert_eq!(pc_lane["committed_without_feedback_at_close"], 0, "{case}");
            assert_eq!(pc_lane["committable_pending_at_close"], 0, "{case}: not (iii)");
            assert_eq!(pc_lane["pending_at_close"], true, "{case}");
            assert_eq!(pc_lane["engine"]["commits"], 1, "{case}");
            assert_eq!(pc_lane["engine"]["frame_callbacks"], 1, "{case}: the late callback is counted");
            assert_eq!(pc_lane["engine"]["buffer_releases"], u64::from(late_release), "{case}: the late release is counted");
            if late_release {
                assert!(pc_lane["frame_callback_missing_at_close"].is_null(), "{case}");
                assert_eq!(
                    pc_lane["buffer_release_missing_at_close"],
                    serde_json::json!({ "pending_seq": 2, "buffers": 2, "buffers_held": 2, "holding_commit_ids": [0, commit_a] }),
                    "{case}: the buffers as they were at the deadline"
                );
            } else {
                assert_eq!(pc_lane["frame_callback_missing_at_close"], commit_a, "{case}");
                assert!(pc_lane["buffer_release_missing_at_close"].is_null(), "{case}");
            }
        }
    }

    /// v1.6 guard, deterministic and without sleeping: `try_commit` starts a
    /// commit with no deadline published or strictly before the deadline
    /// (no change inside the window), and none at the deadline (the clock
    /// read at the deadline value itself; the boundary of the terminal
    /// acceptance `< close_deadline_us`) or after it — the frame then stays
    /// pending.
    #[test]
    fn v1_6_no_commit_is_started_at_or_after_the_close_deadline() {
        for (case, deadline, committed) in [
            ("no deadline", None, true),
            ("strictly before", Some(now_us() + 60_000_000), true),
            ("at", Some(0), false), // replaced by a clock read just before `try_commit`
            ("after", Some(1_000), false),
        ] {
            let (rows_tx, _rows_rx) = mpsc::sync_channel(64);
            let shared = Arc::new(Shared::default());
            let mut lane = lane(rows_tx, shared.clone(), None);
            assert!(matches!(lane.link.pending.offer(rendered(pc_submission(2, 210))), Offer::Accepted { superseded: None }));
            assert!(lane.engine.commit_opportunity() && lane.engine.free_buffer().is_some());
            if let Some(deadline) = deadline {
                let deadline = if case == "at" { now_us() } else { deadline };
                shared.close_deadline_us.store(deadline, Ordering::SeqCst);
            }
            lane.try_commit();
            assert_eq!(lane.engine.stats().commits, u64::from(committed), "{case}");
            assert_eq!(lane.link.pending.has_frame(), !committed, "{case}");
            if committed {
                let commit = lane.engine.unresolved()[0];
                assert_eq!(commit.submission.identity.seq, 2, "{case}");
                if let Some(deadline) = deadline {
                    assert!(commit.t_commit_us < deadline, "{case}");
                }
            }
        }
    }

    /// Codex 112 P2-5: an anomaly's L2 detail carries the reason and the
    /// commit id only; the raw presentation goes to the probe diagnostics.
    #[test]
    fn a_bad_timestamp_keeps_raw_values_out_of_the_l2_jsonl() {
        let (rows_tx, rows_rx) = mpsc::sync_channel(64);
        let shared = Arc::new(Shared::default());
        let diag_path = std::env::temp_dir().join(format!("skew-pc-lane-diag-{}-{}.jsonl", std::process::id(), now_us()));
        let diag_file = std::fs::OpenOptions::new().write(true).create_new(true).open(&diag_path).unwrap();
        let diag = PcDiagLog::new(BufWriter::new(diag_file), "t".into());
        let mut lane = lane(rows_tx.clone(), shared.clone(), Some(diag));
        let identity = L2Identity { modality: L2Modality::Pc, tier: 2, seq: 1, pts_us: 0, event_id: 1, route: None };
        let s = L2Submission { identity, t_release_us: 10, t_submit_us: 11 };
        rows_tx.send(Row::Submit(s)).unwrap();
        let frame = PendingFrame {
            submission: s,
            timing: PcTiming { decode_us: None, t_render_start_us: 12, t_render_end_us: 13 },
            info: FrameInfo { points: 1, covered_pixels: 1, depth_scale: 4 },
            pixels: Vec::new(),
        };
        let (commit_id, _) = lane.engine.commit(frame, 1, 14);
        let mut bad = presented(20);
        bad.tv_nsec = 1_987_654_321;
        lane.apply(BackendEvent::Presented { commit_id, presented: bad }, 30);
        lane.finish(&startup(), "bin", WorkerStats::default());
        drop(rows_tx);
        let (summary, rows) = ledger(rows_rx, shared, "c112-raw");
        // The frame has no terminal (its earlier submit is the first timed
        // failure); the bad timestamp itself is counted and listed.
        assert!(!summary.integrity_ok(SINKS));
        assert_eq!(summary.submitted_unresolved, 1);
        let pc = &rows.last().unwrap()["lanes"]["pc"];
        assert_eq!(pc["engine"]["bad_timestamps"], 1);
        assert!(pc["anomalies"][0].as_str().unwrap().starts_with(crate::l2_pc::FAIL_WAYLAND_BAD_TIMESTAMP));
        let text = rows.iter().map(|r| r.to_string()).collect::<String>();
        assert!(!text.contains("1987654321"), "no raw tv_nsec anywhere in the L2 JSONL");
        assert!(rows.last().unwrap()["lanes"]["pc"]["anomalies"][0].as_str().unwrap().contains(&format!("commit {commit_id}")));
        let diag: Vec<serde_json::Value> = std::fs::read_to_string(&diag_path)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        let anomaly = diag.iter().find(|r| r["role"] == "anomaly").expect("raw in the diagnostics");
        assert_eq!(anomaly["raw"]["tv_nsec"], 1_987_654_321u64);
        let _ = std::fs::remove_file(&diag_path);
    }
}
