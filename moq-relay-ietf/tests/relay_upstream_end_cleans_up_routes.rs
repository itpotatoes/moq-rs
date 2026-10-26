//! Spec item (10), relay hop: when the upstream publisher's session ends
//! while a downstream subscription is live, the relay ends that downstream
//! route (a terminal reaches the subscriber in bounded time) and removes the
//! namespace route, so a later SUBSCRIBE for the namespace is refused instead
//! of being bound to a dead incarnation. Real relay, real QUIC sessions.
//!
//! Pinned observation: the downstream terminal of an upstream session END is
//! a NORMAL track end — PUBLISH_DONE status TrackEnded (0x2) and `closed()`
//! == Ok — indistinguishable at the subscriber from a publisher's own normal
//! end. A receiver therefore cannot fail closed on it; run validity has to
//! come from the sender's own exit status.
mod common;

use std::time::Duration;

use common::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_end_of_the_upstream_session_ends_downstream_routes_and_the_namespace_route() {
    let probe = probe();
    let relay = start_relay();
    let url = relay.url.clone();
    let mut sender = connect(&url).await;
    let stream = Answer::Accept { objects: 10_000, gap: Duration::from_millis(5) };
    let (publisher, _seen) = publish(&mut sender, "run-d", vec![stream]).await;
    let mut receiver = connect(&url).await;
    let (live, reader) = subscribe(&mut receiver, "run-d", "pc").await;
    let live = live.expect("accepted");
    read_objects(reader, 2).await;

    // The publisher's session ends (its task and endpoint go away).
    publisher.abort();
    sender.session.abort();
    drop(sender);

    let terminal = tokio::time::timeout(STEP, live.closed()).await.expect("a terminal in bounded time");
    assert_eq!(terminal, Ok(()), "observed: a normal end at the subscriber");
    let down = probe.downstream_links("pc");
    assert_eq!(down.len(), 1);
    eventually("the relay's PUBLISH_DONE for the live route", || {
        probe.row("publish_done_sent", down[0].session, down[0].request_id).is_some()
    })
    .await;
    let done = probe.row("publish_done_sent", down[0].session, down[0].request_id).unwrap();
    assert_eq!(done.status_code, Some(0x2), "PUBLISH_DONE TrackEnded");

    // The namespace route is gone: a new SUBSCRIBE is refused, never bound.
    let mut refused = None;
    for _ in 0..200 {
        let (result, _r) = subscribe(&mut receiver, "run-d", "haptic").await;
        match result {
            Err(error) => {
                refused = Some(error);
                break;
            }
            Ok(handle) => {
                // Route not yet removed: the relay bound it; wait and retry.
                drop(handle);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    let refused = refused.expect("a SUBSCRIBE after the upstream end is refused");
    assert_eq!(refused, moq_transport::serve::ServeError::Closed(0x10), "not found");
    assert!(probe.downstream_links("haptic").iter().all(|l| l.binding.as_str() != "reused"));

    relay.stop();
}
