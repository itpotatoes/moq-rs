//! Optional object-boundary observations. No wire-time or queue-only claim.
//!
//! Callbacks must not block, perform I/O or allocate unboundedly. The observer
//! supplies its clock so transport does not invent another epoch. Off by default.
//! Keep the Arc alive in a recorder to disambiguate object incarnations offline.
use std::sync::{Arc, OnceLock};

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
}

static OBSERVER: OnceLock<Arc<dyn Observer>> = OnceLock::new();

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
