//! Spec item (10), relay hop, the case the normal-end test does not cover
//! (Codex 174a): the upstream publisher's session ends while an object is
//! still being forwarded (1,024 bytes declared, 8 written). The relay's child
//! forward fails, so the downstream route ends with an ERROR — PUBLISH_DONE
//! status 0 (InternalError), `closed() == Err(Closed(0))` — not TrackEnded.
//! Together with `relay_upstream_end_cleans_up_routes`: the terminal of an
//! upstream end depends on what was in flight, so neither terminal tells a
//! sender crash from a normal end; run validity needs the sender's exit
//! status. Real relay, real QUIC.
mod common;

use std::sync::Arc;

use common::*;
use moq_transport::{
    coding::TrackNamespace,
    object_trace::Boundary,
    serve::{ServeError, Track},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_upstream_end_during_a_partial_object_ends_the_downstream_route_with_an_error() {
    let probe = probe();
    let relay = start_relay();
    let url = relay.url.clone();
    let mut sender = connect(&url).await;
    let ns = TrackNamespace::from_utf8_path("run-p");
    let announced = sender.publisher.publish_namespace_open(ns.clone()).expect("PUBLISH_NAMESPACE");
    tokio::time::timeout(STEP, announced.ok()).await.expect("in time").expect("namespace OK");
    let publisher = tokio::spawn(async move {
        let subscribed = announced.subscribed().await.expect("subscribed").expect("a SUBSCRIBE");
        let (writer, reader) = Track::new(ns, "pc").produce();
        let serving = tokio::spawn(async move { let _ = subscribed.serve(reader).await; });
        let mut subgroups = writer.subgroups().expect("subgroups");
        let mut subgroup = subgroups.append(1).expect("subgroup");
        let mut object = subgroup.create(1024, None).expect("object");
        object.write(vec![1u8; 8].into()).expect("first 8 bytes");
        let _hold = (announced, subgroups, subgroup, object);
        // Joined here; aborting this task cancels the serve task with it.
        struct AbortOnDrop(tokio::task::JoinHandle<()>);
        impl Drop for AbortOnDrop {
            fn drop(&mut self) { self.0.abort(); }
        }
        let _serving = AbortOnDrop(serving);
        std::future::pending::<()>().await;
    });
    let mut receiver = connect(&url).await;
    let (live, _reader) = subscribe(&mut receiver, "run-p", "pc").await;
    let live = live.expect("accepted");
    eventually("the relay forwarding the partial object", || {
        let down = probe.downstream_links("pc");
        !down.is_empty()
            && probe.forwards.lock().unwrap().iter().any(|f| {
                f.boundary == Boundary::ForwardStart && Arc::ptr_eq(&f.track, &down[0].track)
            })
    })
    .await;

    // The publisher's SESSION ends; its task (still holding the partial
    // object's writer) is stopped and joined only after the terminal.
    sender.session.abort();
    drop(sender);

    let terminal = closed(&live).await;
    assert_eq!(terminal, Err(ServeError::Closed(0)), "an error terminal, not TrackEnded");
    let down = probe.downstream_links("pc");
    eventually("the relay's PUBLISH_DONE", || {
        probe.row("publish_done_sent", down[0].session, down[0].request_id).is_some()
    })
    .await;
    let done = probe.row("publish_done_sent", down[0].session, down[0].request_id).unwrap();
    assert_eq!(done.status_code, Some(0x0), "PUBLISH_DONE InternalError");
    publisher.abort();
    expect_no_panic("the partial-object publisher", publisher.await);
    relay.stop();
}
