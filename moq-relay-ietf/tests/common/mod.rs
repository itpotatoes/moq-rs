//! In-process harness for relay subscribe-path integration tests: a real
//! `Relay` on 127.0.0.1 (UDP port chosen by the OS), real moq-transport
//! publisher and subscriber sessions over QUIC, and a process-global
//! `object_trace` observer that records the control and binding observations
//! of EVERY session in this process (relay and clients).
//!
//! Each `tests/*.rs` file is its own process and contains ONE test, because
//! the observer is a process-global `OnceLock`.
//!
//! TLS: `tests/fixtures/localhost-test-only.{crt,key}` is a self-signed,
//! test-only certificate (no secret value; never used outside these tests).
//! The clients pin it as their only root and also disable verification, so
//! the platform trust store is never read.
#![allow(dead_code)]

use std::{
    net::{SocketAddr, UdpSocket},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::Duration,
};

use async_trait::async_trait;
use moq_native_ietf::{quic, tls};
use moq_relay_ietf::{
    Coordinator, CoordinatorError, CoordinatorResult, DataPriorityMapping, NamespaceOrigin,
    NamespaceRegistration, Relay, RelayConfig, SessionConfig,
};
use moq_transport::{
    coding::TrackNamespace,
    object_trace::{self, Boundary, ControlDirection, ControlMessage, Observer, TrackBinding},
    serve::{SubgroupObject, Track},
    session::{Publisher, Session, Subscriber},
};

pub const STEP: Duration = Duration::from_secs(10);

/// A single relay: every namespace is local, nothing is remote.
pub struct LocalOnlyCoordinator;

#[async_trait]
impl Coordinator for LocalOnlyCoordinator {
    async fn register_namespace(
        &self,
        _scope: Option<&str>,
        _namespace: &TrackNamespace,
    ) -> CoordinatorResult<NamespaceRegistration> {
        Ok(NamespaceRegistration::new(()))
    }

    async fn unregister_namespace(
        &self,
        _scope: Option<&str>,
        _namespace: &TrackNamespace,
    ) -> CoordinatorResult<()> {
        Ok(())
    }

    async fn lookup(
        &self,
        _scope: Option<&str>,
        _namespace: &TrackNamespace,
    ) -> CoordinatorResult<(NamespaceOrigin, Option<quic::Client>)> {
        Err(CoordinatorError::NamespaceNotFound)
    }
}

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name)
}

fn tls_config(server: bool) -> tls::Config {
    let mut args = tls::Args {
        root: vec![fixture("localhost-test-only.crt")],
        disable_verify: true,
        ..Default::default()
    };
    if server {
        args.cert = vec![fixture("localhost-test-only.crt")];
        args.key = vec![fixture("localhost-test-only.key")];
    }
    args.load().expect("test TLS config")
}

/// A running relay. `Relay::run` is not `Send`, so the relay runs on its own
/// thread with a current-thread runtime, like the binary's `main`.
pub struct RelayHandle {
    pub url: url::Url,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<anyhow::Result<()>>>,
}

impl RelayHandle {
    /// Stop the relay (drops its endpoint and sessions) and join its thread.
    pub fn stop(mut self) {
        self.shutdown();
    }

    fn shutdown(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for RelayHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Start a relay on 127.0.0.1 with an OS-chosen UDP port.
pub fn start_relay() -> RelayHandle {
    let socket = UdpSocket::bind("127.0.0.1:0").expect("bind relay socket");
    let addr: SocketAddr = socket.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let thread = std::thread::spawn(move || -> anyhow::Result<()> {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        runtime.block_on(async move {
            let tls = tls_config(true);
            let endpoint = quic::Endpoint::new(quic::Config::with_socket(socket, None, tls.clone()))?;
            let relay = Relay::new(RelayConfig {
                bind: None,
                endpoints: vec![endpoint],
                tls,
                qlog_dir: None,
                mlog_dir: None,
                announce: None,
                node: None,
                coordinator: Arc::new(LocalOnlyCoordinator),
                session: SessionConfig {
                    max_request_id: 100,
                    data_priority_mapping: DataPriorityMapping::MoqtV2,
                },
            })?;
            tokio::select! {
                result = relay.run() => result,
                _ = stopped => Ok(()),
            }
        })
    });
    let url = url::Url::parse(&format!("https://{addr}")).unwrap();
    RelayHandle { url, stop: Some(stop), thread: Some(thread) }
}

/// A client session to the relay; the session runs on its own task.
pub struct Client {
    pub publisher: Publisher,
    pub subscriber: Subscriber,
    pub session: tokio::task::JoinHandle<Result<(), moq_transport::session::SessionError>>,
    _endpoint: quic::Endpoint,
}

pub async fn connect(url: &url::Url) -> Client {
    let endpoint = quic::Endpoint::new(
        quic::Config::new("127.0.0.1:0".parse().unwrap(), None, tls_config(false)).unwrap(),
    )
    .expect("client endpoint");
    let (session, _cid, transport) =
        tokio::time::timeout(STEP, endpoint.client.connect(url, None))
            .await
            .expect("connect in time")
            .expect("connect");
    let config = SessionConfig {
        max_request_id: 100,
        data_priority_mapping: DataPriorityMapping::MoqtV2,
    };
    let (session, publisher, subscriber) =
        tokio::time::timeout(STEP, Session::connect_with_config(session, None, transport, config))
            .await
            .expect("setup in time")
            .expect("setup");
    Client { publisher, subscriber, session: tokio::spawn(session.run()), _endpoint: endpoint }
}

/// One control observation.
#[derive(Debug, Clone)]
pub struct ControlRow {
    pub seq: u64,
    pub boundary: &'static str,
    pub session: u64,
    pub request_id: u64,
    pub track: Option<String>,
    pub error_code: Option<u64>,
    pub status_code: Option<u64>,
    pub send_begin: Option<u64>,
}

/// One subscription-to-track binding.
#[derive(Debug, Clone)]
pub struct LinkRow {
    pub seq: u64,
    pub binding: TrackBinding,
    pub session: u64,
    pub request_id: u64,
    pub track: Arc<Track>,
}

/// One relay-or-client forward observation, with its track incarnation.
#[derive(Debug, Clone)]
pub struct ForwardRow {
    pub boundary: Boundary,
    pub track: Arc<Track>,
    pub group_id: u64,
    pub object_id: u64,
}

/// Records every observation. Its clock is a process-wide sequence number,
/// so `seq` totally orders the observations as they were made.
#[derive(Default)]
pub struct Probe {
    clock: AtomicU64,
    pub control: Mutex<Vec<ControlRow>>,
    pub links: Mutex<Vec<LinkRow>>,
    pub forwards: Mutex<Vec<ForwardRow>>,
}

/// Hold the application's waiter: poll once; a Ready result is returned AS
/// IS (never re-polled — Codex 176a); only a Pending future is held `delay`
/// and then awaited.
pub async fn hold_waiter<F: std::future::Future>(future: F, delay: Duration) -> F::Output {
    let mut future = Box::pin(future);
    match futures::poll!(future.as_mut()) {
        std::task::Poll::Ready(output) => output,
        std::task::Poll::Pending => {
            tokio::time::sleep(delay).await;
            future.await
        }
    }
}

impl Observer for Probe {
    fn now_us(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn record(&self, boundary: Boundary, object: &Arc<SubgroupObject>, _alias: u64, _t: u64) {
        if matches!(boundary, Boundary::ForwardStart | Boundary::ForwardAccepted) {
            self.forwards.lock().unwrap().push(ForwardRow {
                boundary,
                track: object.group.track.clone(),
                group_id: object.group_id,
                object_id: object.object_id,
            });
        }
    }

    fn observes_control(&self) -> bool {
        true
    }

    fn record_control(
        &self,
        direction: ControlDirection,
        message: ControlMessage<'_>,
        session: u64,
        timestamp_us: u64,
        send_begin_us: Option<u64>,
    ) {
        let (request_id, track, error_code, status_code) = match message {
            ControlMessage::Subscribe(m) => (m.id, Some(m.track_name.to_string_lossy().into_owned()), None, None),
            ControlMessage::SubscribeOk(m) => (m.id, None, None, None),
            ControlMessage::RequestError(m) => (m.id, None, Some(m.error_code), None),
            ControlMessage::PublishDone(m) => (m.id, None, None, Some(m.status_code)),
        };
        self.control.lock().unwrap().push(ControlRow {
            seq: timestamp_us,
            boundary: message.boundary(direction),
            session,
            request_id,
            track,
            error_code,
            status_code,
            send_begin: send_begin_us,
        });
    }

    fn record_link(
        &self,
        binding: TrackBinding,
        session: u64,
        request_id: u64,
        track: &Arc<Track>,
        timestamp_us: u64,
    ) {
        self.links.lock().unwrap().push(LinkRow {
            seq: timestamp_us,
            binding,
            session,
            request_id,
            track: track.clone(),
        });
    }
}

static PROBE: OnceLock<Arc<Probe>> = OnceLock::new();

/// Install the probe (once per test process) and return it.
pub fn probe() -> Arc<Probe> {
    PROBE
        .get_or_init(|| {
            let probe = Arc::new(Probe::default());
            object_trace::install(probe.clone()).ok().expect("first observer in this process");
            probe
        })
        .clone()
}

impl Probe {
    pub fn controls(&self) -> Vec<ControlRow> {
        self.control.lock().unwrap().clone()
    }

    pub fn links(&self) -> Vec<LinkRow> {
        self.links.lock().unwrap().clone()
    }

    /// The relay's downstream bindings of `track` (relay producer only).
    pub fn downstream_links(&self, track: &str) -> Vec<LinkRow> {
        self.links()
            .into_iter()
            .filter(|l| l.binding.is_downstream() && l.track.name.to_string_lossy() == track)
            .collect()
    }

    /// The upstream SUBSCRIBE bindings into `incarnation` (identity, not name).
    pub fn upstream_links_into(&self, incarnation: &Arc<Track>) -> Vec<LinkRow> {
        self.links()
            .into_iter()
            .filter(|l| !l.binding.is_downstream() && Arc::ptr_eq(&l.track, incarnation))
            .collect()
    }

    /// The control rows of one request: `boundary` on `session` for `request_id`.
    pub fn row(&self, boundary: &str, session: u64, request_id: u64) -> Option<ControlRow> {
        self.controls()
            .into_iter()
            .find(|r| r.boundary == boundary && r.session == session && r.request_id == request_id)
    }
}

/// Wait (bounded) until `cond` holds on the probe.
pub async fn eventually(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + STEP;
    while !cond() {
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// How the scripted publisher answers one SUBSCRIBE that reaches it.
#[derive(Debug, Clone)]
pub enum Answer {
    /// REQUEST_ERROR `DoesNotExist` (0x10) at once, the code a sender's
    /// unsupported or unknown track produces.
    Refuse,
    /// REQUEST_ERROR `code`, but only after `gate` is released: the test
    /// holds the upstream answer until the subscriber has OBSERVED the relay's
    /// local SUBSCRIBE_OK, so the "OK, then refusal" shape is deterministic
    /// at the application, not only on the wire (Codex 174a P2).
    RefuseAfter { code: u64, gate: Arc<tokio::sync::Notify> },
    /// SUBSCRIBE_OK, then `objects` one-object subgroups, `gap` apart; the
    /// track stays open until the publisher task ends.
    Accept { objects: u64, gap: Duration },
    /// SUBSCRIBE_OK, `objects` one-object subgroups, then — only after `gate`
    /// is released, i.e. after the application observed the OK — the
    /// publisher ENDS the track (writer dropped): a normal end, PUBLISH_DONE
    /// TrackEnded (Codex 175a: without the gate a held open waiter sees Done).
    AcceptThenEnd { objects: u64, gate: Arc<tokio::sync::Notify> },
}

/// The scripted publisher's tasks. `stop` aborts them and JOINS every child
/// (serve and producer tasks included); a child that panicked fails the test
/// (Codex 176a P3: an unjoined child's panic used to be lost).
pub struct PublisherTask {
    main: tokio::task::JoinHandle<()>,
    children: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl PublisherTask {
    pub fn from_parts(
        main: tokio::task::JoinHandle<()>,
        children: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    ) -> Self {
        Self { main, children }
    }

    /// Abort the publisher and ALL its children first, then join each within
    /// `STEP`; a panic or a join past the bound fails the test. (The bound is
    /// the helper's own; a worker blocked inside a poll can still delay the
    /// runtime's shutdown, so the cargo record applies its own per-process
    /// deadline.)
    pub async fn stop(self) {
        if let Err(error) = self.stop_within(STEP).await {
            panic!("{error}");
        }
    }

    pub async fn stop_within(self, bound: Duration) -> Result<(), String> {
        let children = std::mem::take(&mut *self.children.lock().unwrap());
        self.main.abort();
        for child in &children {
            child.abort();
        }
        let tasks = std::iter::once(("the publisher task", self.main))
            .chain(children.into_iter().map(|child| ("a publisher child task", child)));
        for (what, task) in tasks {
            match tokio::time::timeout(bound, task).await {
                Err(_) => return Err(format!("{what} did not finish within {bound:?} after abort")),
                Ok(joined) => {
                    if let Err(error) = joined {
                        if error.is_panic() {
                            return Err(format!("{what} panicked: {error}"));
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// A joined task either finished or was cancelled; a panic fails the test.
pub fn expect_no_panic<T>(what: &str, joined: Result<T, tokio::task::JoinError>) {
    if let Err(error) = joined {
        assert!(!error.is_panic(), "{what} panicked: {error}");
    }
}

/// A scripted publisher on `client`: publishes `namespace`, waits for the
/// relay's REQUEST_OK, then answers each arriving SUBSCRIBE with the next
/// `answers` entry. Returns its tasks and the number of SUBSCRIBEs it saw.
/// Producers stop quietly when the track is gone (the subscriber or the
/// relay ended it); anything else is a panic that `stop` reports.
pub async fn publish(
    client: &mut Client,
    namespace: &str,
    answers: Vec<Answer>,
) -> (PublisherTask, Arc<AtomicU64>) {
    use moq_transport::serve::ServeError;
    let ns = TrackNamespace::from_utf8_path(namespace);
    let publish = client.publisher.publish_namespace_open(ns.clone()).expect("PUBLISH_NAMESPACE");
    tokio::time::timeout(STEP, publish.ok()).await.expect("namespace OK in time").expect("namespace OK");
    let seen = Arc::new(AtomicU64::new(0));
    let counter = seen.clone();
    let children: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>> = Arc::default();
    let spawned = children.clone();
    let spawn = move |task: futures::future::BoxFuture<'static, ()>| {
        spawned.lock().unwrap().push(tokio::spawn(task));
    };
    let main = tokio::spawn(async move {
        use futures::FutureExt;
        let mut answers: std::collections::VecDeque<Answer> = answers.into();
        while let Ok(Some(subscribed)) = publish.subscribed().await {
            counter.fetch_add(1, Ordering::SeqCst);
            let name = subscribed.track_name.clone();
            match answers.pop_front().expect("an answer for every SUBSCRIBE") {
                Answer::Refuse => {
                    let _ = subscribed.close(ServeError::not_found_ctx("test publisher refuses"));
                }
                Answer::RefuseAfter { code, gate } => spawn(async move {
                    gate.notified().await;
                    let _ = subscribed.close(ServeError::Closed(code));
                }.boxed()),
                Answer::AcceptThenEnd { objects, gate } => {
                    let (writer, reader) = Track::new(ns.clone(), name).produce();
                    spawn(async move { let _ = subscribed.serve(reader).await; }.boxed());
                    let mut subgroups = writer.subgroups().expect("subgroups");
                    spawn(async move {
                        for i in 0..objects {
                            let Ok(mut subgroup) = subgroups.append(1) else { return };
                            if subgroup.write(i.to_le_bytes().to_vec().into()).is_err() {
                                return;
                            }
                        }
                        gate.notified().await;
                        drop(subgroups); // the publisher ends the track normally
                    }.boxed());
                }
                Answer::Accept { objects, gap } => {
                    let (writer, reader) = Track::new(ns.clone(), name).produce();
                    spawn(async move { let _ = subscribed.serve(reader).await; }.boxed());
                    let mut subgroups = writer.subgroups().expect("subgroups");
                    spawn(async move {
                        for i in 0..objects {
                            let Ok(mut subgroup) = subgroups.append(1) else { return };
                            if subgroup.write(i.to_le_bytes().to_vec().into()).is_err() {
                                return;
                            }
                            tokio::time::sleep(gap).await;
                        }
                        // Keep the track open until the publisher is stopped.
                        std::future::pending::<()>().await;
                        drop(subgroups);
                    }.boxed());
                }
            }
        }
    });
    (PublisherTask::from_parts(main, children), seen)
}

/// Subscribe `track` of `namespace` through `client`; returns the SUBSCRIBE
/// handle (Ok only after SUBSCRIBE_OK) and the track reader.
pub async fn subscribe(
    client: &mut Client,
    namespace: &str,
    track: &str,
) -> (
    Result<moq_transport::session::Subscribe, moq_transport::serve::ServeError>,
    moq_transport::serve::TrackReader,
) {
    let (writer, reader) = Track::new(TrackNamespace::from_utf8_path(namespace), track).produce();
    let result = tokio::time::timeout(STEP, client.subscriber.subscribe_open(writer)).await;
    (result.expect("SUBSCRIBE answered in time"), reader)
}

/// Wait (bounded) for a subscription's terminal.
pub async fn closed(subscribe: &moq_transport::session::Subscribe) -> Result<(), moq_transport::serve::ServeError> {
    tokio::time::timeout(STEP, subscribe.closed()).await.expect("a terminal in time")
}

/// [`subscribe`] with the APPLICATION's open waiter held for `delay` after
/// the SUBSCRIBE went out (the transport keeps running): the condition under
/// which Codex 174a made the ungated refusal tests fail. With the publisher
/// gate the open must still return Ok.
pub async fn subscribe_with_held_waiter(
    client: &mut Client,
    namespace: &str,
    track: &str,
    delay: Duration,
) -> (
    Result<moq_transport::session::Subscribe, moq_transport::serve::ServeError>,
    moq_transport::serve::TrackReader,
) {
    let (writer, reader) = Track::new(TrackNamespace::from_utf8_path(namespace), track).produce();
    let open = client.subscriber.subscribe_open(writer);
    let result = tokio::time::timeout(STEP, hold_waiter(open, delay)).await.expect("SUBSCRIBE answered in time");
    (result, reader)
}

/// Read `n` objects from a subscribed track (bounded). Reads through a CLONE:
/// the caller's reader stays alive, so the subscription is not torn down by
/// dropping its last reader (Codex 176a P2).
pub async fn read_objects(reader: &moq_transport::serve::TrackReader, n: usize) -> Vec<Vec<u8>> {
    use moq_transport::serve::TrackReaderMode;
    let reader = reader.clone();
    tokio::time::timeout(STEP, async move {
        let mut out = Vec::new();
        let TrackReaderMode::Subgroups(mut subgroups) = reader.mode().await.expect("mode") else {
            panic!("expected subgroups");
        };
        while out.len() < n {
            let mut subgroup = subgroups.next().await.expect("subgroup").expect("open track");
            while let Some(object) = subgroup.read_next().await.expect("object") {
                out.push(object.to_vec());
            }
        }
        out
    })
    .await
    .expect("objects in time")
}
