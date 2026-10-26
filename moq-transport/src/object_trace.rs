//! Optional object-boundary observations. No wire-time or queue-only claim.
//!
//! Callbacks must not block, perform I/O or allocate unboundedly. The observer
//! supplies its clock so transport does not invent another epoch. Off by default.
//! Keep the Arc alive in a recorder to disambiguate object incarnations offline.
//!
//! Control-stream observations (SUBSCRIBE, SUBSCRIBE_OK, REQUEST_ERROR) are a
//! separate opt-in: an observer that does not override `observes_control` is
//! never called for them and no clock is read on their behalf.
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, OnceLock,
};

use crate::message;
use crate::serve::SubgroupObject;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Boundary {
    ReceiveRegistered,
    ReceiveComplete,
    ForwardStart,
    ForwardAccepted,
    ReceiveTimeout,
    ReceiveInterrupted,
    ForwardTimeout,
    ForwardInterrupted,
}

impl Boundary {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReceiveRegistered => "receive_registered",
            Self::ReceiveComplete => "receive_complete",
            Self::ForwardStart => "forward_start",
            Self::ForwardAccepted => "forward_accepted",
            Self::ReceiveTimeout => "receive_timeout",
            Self::ReceiveInterrupted => "receive_interrupted",
            Self::ForwardTimeout => "forward_timeout",
            Self::ForwardInterrupted => "forward_interrupted",
        }
    }
}

/// Where on the session's control stream a control message was observed.
///
/// `Sent`: after the encoded message was written into the control-stream
/// writer (handed to the QUIC stack), not wire departure. `Received`: right
/// after the message was decoded from the control stream, before dispatch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControlDirection {
    Sent,
    Received,
}

/// Borrowed view of the control messages observed for subscription propagation.
#[derive(Clone, Copy, Debug)]
pub enum ControlMessage<'a> {
    Subscribe(&'a message::Subscribe),
    SubscribeOk(&'a message::SubscribeOk),
    RequestError(&'a message::RequestError),
}

impl ControlMessage<'_> {
    /// Stable boundary name of this message at `direction`.
    pub fn boundary(&self, direction: ControlDirection) -> &'static str {
        match (self, direction) {
            (Self::Subscribe(_), ControlDirection::Sent) => "subscribe_sent",
            (Self::Subscribe(_), ControlDirection::Received) => "subscribe_received",
            (Self::SubscribeOk(_), ControlDirection::Sent) => "subscribe_ok_sent",
            (Self::SubscribeOk(_), ControlDirection::Received) => "subscribe_ok_received",
            (Self::RequestError(_), ControlDirection::Sent) => "request_error_sent",
            (Self::RequestError(_), ControlDirection::Received) => "request_error_received",
        }
    }
}

pub trait Observer: Send + Sync + 'static {
    /// Must use the same monotonic microsecond epoch as the endpoints.
    fn now_us(&self) -> u64;
    fn record(&self, boundary: Boundary, object: &Arc<SubgroupObject>,
              track_alias: u64, timestamp_us: u64);
    /// Object payload bytes read at this receive observation, if known.
    /// None is unknown, never zero. This does not count QUIC/wire bytes.
    fn record_progress(&self, boundary: Boundary, object: &Arc<SubgroupObject>,
                       track_alias: u64, timestamp_us: u64, _received_bytes: Option<usize>) {
        self.record(boundary, object, track_alias, timestamp_us);
    }
    /// Opt in to control-stream observations. Default false: `record_control`
    /// is never called and no clock is read for control messages.
    fn observes_control(&self) -> bool {
        false
    }
    /// One control message at one session boundary. `session` is a
    /// process-local ordinal of the MoQT session (see `next_session_ordinal`),
    /// not a wire identifier. Must not block or perform I/O.
    fn record_control(&self, _direction: ControlDirection, _message: ControlMessage<'_>,
                      _session: u64, _timestamp_us: u64) {}
}

static OBSERVER: OnceLock<Arc<dyn Observer>> = OnceLock::new();
static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);

/// Process-local session ordinal (1, 2, ...), assigned once per MoQT session.
pub fn next_session_ordinal() -> u64 {
    NEXT_SESSION.fetch_add(1, Ordering::Relaxed)
}

/// Observe one control message of `session` if an observer opted in. Only
/// SUBSCRIBE, SUBSCRIBE_OK and REQUEST_ERROR are reported.
pub fn control(direction: ControlDirection, msg: &message::Message, session: u64) {
    if let Some(observer) = OBSERVER.get() {
        control_with(observer.as_ref(), direction, msg, session);
    }
}

fn control_with(observer: &dyn Observer, direction: ControlDirection,
                msg: &message::Message, session: u64) {
    if !observer.observes_control() {
        return;
    }
    let view = match msg {
        message::Message::Subscribe(m) => ControlMessage::Subscribe(m),
        message::Message::SubscribeOk(m) => ControlMessage::SubscribeOk(m),
        message::Message::RequestError(m) => ControlMessage::RequestError(m),
        _ => return,
    };
    observer.record_control(direction, view, session, observer.now_us());
}

pub fn install(observer: Arc<dyn Observer>) -> Result<(), Arc<dyn Observer>> {
    OBSERVER.set(observer)
}

/// Capture before publishing a writer, then attach its identity after creation.
pub fn timestamp() -> Option<u64> {
    OBSERVER.get().map(|observer| observer.now_us())
}

pub fn emit_at(boundary: Boundary, object: &Arc<SubgroupObject>,
               track_alias: u64, timestamp_us: Option<u64>) {
    if let (Some(observer), Some(timestamp_us)) = (OBSERVER.get(), timestamp_us) {
        observer.record(boundary, object, track_alias, timestamp_us);
    }
}

pub fn emit(boundary: Boundary, object: &Arc<SubgroupObject>, track_alias: u64) {
    if let Some(observer) = OBSERVER.get() {
        observer.record(boundary, object, track_alias, observer.now_us());
    }
}

/// Record interrupted receive scopes even when the future is dropped.
/// Interruption is NOT proof of user cancellation: it may be I/O failure,
/// truncated FIN or teardown. Explicit delivery reset has its own outcome.
pub struct ReceiveScope {
    observer: Option<Arc<dyn Observer>>,
    object: Option<Arc<SubgroupObject>>,
    track_alias: u64,
    received: usize,
}

impl ReceiveScope {
    pub fn new(object: &Arc<SubgroupObject>, track_alias: u64) -> Self {
        let observer = OBSERVER.get().cloned();
        let object = observer.as_ref().map(|_| object.clone());
        Self { observer, object, track_alias, received: 0 }
    }

    pub fn received(&mut self, bytes: usize) {
        self.received += bytes;
    }

    /// Exactly one terminal event; subsequent Drop is silent.
    pub fn finish(&mut self, boundary: Boundary) {
        if self.object.is_none() { return; }
        let timestamp = self.observer.as_ref().map(|observer| observer.now_us());
        self.finish_at(boundary, timestamp);
    }

    pub fn finish_at(&mut self, boundary: Boundary, timestamp: Option<u64>) {
        if let (Some(observer), Some(object), Some(timestamp)) =
            (&self.observer, self.object.take(), timestamp) {
            observer.record_progress(boundary, &object, self.track_alias,
                                     timestamp, Some(self.received));
        }
    }
}

impl Drop for ReceiveScope {
    fn drop(&mut self) {
        self.finish(Boundary::ReceiveInterrupted);
    }
}

/// A started forwarding scope must not vanish on error or task cancellation.
/// The amount accepted before interruption is unknown here, not zero.
pub struct ForwardScope {
    observer: Option<Arc<dyn Observer>>,
    object: Option<Arc<SubgroupObject>>,
    track_alias: u64,
}

impl ForwardScope {
    pub fn new(object: &Arc<SubgroupObject>, track_alias: u64) -> Self {
        let observer = OBSERVER.get().cloned();
        let object = observer.as_ref().map(|_| object.clone());
        Self { observer, object, track_alias }
    }
    pub fn disarm(&mut self) { self.object = None; }
}

impl Drop for ForwardScope {
    fn drop(&mut self) {
        if let (Some(observer), Some(object)) = (&self.observer, self.object.take()) {
            observer.record(Boundary::ForwardInterrupted, &object, self.track_alias,
                            observer.now_us());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{coding::TrackNamespace, serve::{SubgroupInfo, Track}};
    use std::sync::Mutex;

    #[derive(Default)]
    struct Sink(Mutex<Vec<(Boundary, Option<usize>)>>);
    impl Observer for Sink {
        fn now_us(&self) -> u64 { 1 } // synthetic, not a network measurement
        fn record(&self, boundary: Boundary, _: &Arc<SubgroupObject>, _: u64, _: u64) {
            self.0.lock().unwrap().push((boundary, None));
        }
        fn record_progress(&self, boundary: Boundary, _: &Arc<SubgroupObject>, _: u64,
                           _: u64, bytes: Option<usize>) {
            self.0.lock().unwrap().push((boundary, bytes));
        }
    }
    /// Control-stream sink: opted in or not; counts clock reads.
    struct ControlSink {
        opted_in: bool,
        clock_reads: std::sync::atomic::AtomicU64,
        rows: Mutex<Vec<(&'static str, u64, u64, u64)>>,
    }
    impl ControlSink {
        fn new(opted_in: bool) -> Self {
            Self { opted_in, clock_reads: Default::default(), rows: Mutex::new(Vec::new()) }
        }
    }
    impl Observer for ControlSink {
        fn now_us(&self) -> u64 {
            self.clock_reads.fetch_add(1, Ordering::SeqCst) + 100 // synthetic
        }
        fn record(&self, _: Boundary, _: &Arc<SubgroupObject>, _: u64, _: u64) {}
        fn observes_control(&self) -> bool { self.opted_in }
        fn record_control(&self, direction: ControlDirection, message: ControlMessage<'_>,
                          session: u64, timestamp_us: u64) {
            let id = match message {
                ControlMessage::Subscribe(m) => m.id,
                ControlMessage::SubscribeOk(m) => m.id,
                ControlMessage::RequestError(m) => m.id,
            };
            self.rows.lock().unwrap().push((message.boundary(direction), session, id, timestamp_us));
        }
    }
    fn control_messages() -> Vec<message::Message> {
        use crate::coding::{KeyValuePairs, ReasonPhrase};
        vec![
            message::Message::Subscribe(message::Subscribe {
                id: 2, track_namespace: TrackNamespace::from_utf8_path("probe"),
                track_name: "pc".into(), params: KeyValuePairs::default(),
            }),
            message::Message::SubscribeOk(message::SubscribeOk {
                id: 2, track_alias: 2, params: KeyValuePairs::default(),
                track_extensions: Default::default(),
            }),
            message::Message::RequestError(message::RequestError {
                id: 4, error_code: 16, retry_interval: 0,
                reason: ReasonPhrase("not found".to_string()),
            }),
            message::Message::Unsubscribe(message::Unsubscribe { id: 2 }),
        ]
    }
    #[test]
    fn control_observation_is_opt_in_and_reads_no_clock_by_default() {
        let sink = ControlSink::new(false);
        for msg in control_messages() {
            control_with(&sink, ControlDirection::Received, &msg, 1);
            control_with(&sink, ControlDirection::Sent, &msg, 1);
        }
        assert!(sink.rows.lock().unwrap().is_empty());
        assert_eq!(sink.clock_reads.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn control_observation_reports_subscribe_ok_and_error_only() {
        let sink = ControlSink::new(true);
        for msg in control_messages() {
            control_with(&sink, ControlDirection::Received, &msg, 7);
        }
        for msg in control_messages() {
            control_with(&sink, ControlDirection::Sent, &msg, 8);
        }
        assert_eq!(*sink.rows.lock().unwrap(), vec![
            ("subscribe_received", 7, 2, 100), ("subscribe_ok_received", 7, 2, 101),
            ("request_error_received", 7, 4, 102),
            ("subscribe_sent", 8, 2, 103), ("subscribe_ok_sent", 8, 2, 104),
            ("request_error_sent", 8, 4, 105),
        ]);
        // UNSUBSCRIBE is not reported and reads no clock.
        assert_eq!(sink.clock_reads.load(Ordering::SeqCst), 6);
    }
    #[test]
    fn session_ordinals_are_unique_and_increasing() {
        let first = next_session_ordinal();
        let second = next_session_ordinal();
        assert!(first >= 1 && second > first);
    }
    fn scope(sink: Arc<Sink>) -> ReceiveScope {
        let (mut writer, _reader) = SubgroupInfo {
            track: Arc::new(Track::new(TrackNamespace::from_utf8_path("probe"), "pc")),
            group_id: 0, subgroup_id: 0, priority: 0,
        }.produce();
        let object = writer.create(8, None).unwrap().info.clone();
        ReceiveScope { observer: Some(sink), object: Some(object), track_alias: 1, received: 0 }
    }
    #[test]
    fn receive_scope_terminal_outcomes_preserve_partial_counts_exactly_once() {
        let sink = Arc::new(Sink::default());
        let mut complete = scope(sink.clone());
        complete.received(8);
        complete.finish(Boundary::ReceiveComplete);
        drop(complete);
        let mut expired = scope(sink.clone());
        expired.received(4);
        expired.finish(Boundary::ReceiveTimeout);
        drop(expired);
        let mut interrupted = scope(sink.clone());
        interrupted.received(3);
        drop(interrupted);
        assert_eq!(*sink.0.lock().unwrap(), vec![
            (Boundary::ReceiveComplete, Some(8)),
            (Boundary::ReceiveTimeout, Some(4)),
            (Boundary::ReceiveInterrupted, Some(3)),
        ]);
    }
    #[tokio::test]
    async fn cancelling_receive_future_emits_one_interrupted_terminal() {
        let sink = Arc::new(Sink::default());
        let mut receive = scope(sink.clone());
        receive.received(4);
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _scope = receive;
            ready_tx.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        ready_rx.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(*sink.0.lock().unwrap(), vec![(Boundary::ReceiveInterrupted, Some(4))]);
    }
    #[test]
    fn forwarding_interruption_keeps_unknown_bytes_and_disarm_prevents_duplicates() {
        let sink = Arc::new(Sink::default());
        let receive = scope(sink.clone());
        let object = receive.object.as_ref().unwrap().clone();
        let mut complete = ForwardScope {
            observer: Some(sink.clone()), object: Some(object.clone()), track_alias: 1,
        };
        complete.disarm();
        drop(complete);
        drop(ForwardScope { observer: Some(sink.clone()), object: Some(object), track_alias: 1 });
        assert_eq!(*sink.0.lock().unwrap(), vec![(Boundary::ForwardInterrupted, None)]);
    }
}
