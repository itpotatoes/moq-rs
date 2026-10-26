//! Codex 173c F2: `D1 SUB -> D1 OK(local) -> D2 SUB -> D2 OK -> U SUB` cannot
//! be paired by time. With the binding observations it is exact: the second
//! downstream request on the same track REUSES the first one's incarnation,
//! only the first created it, and there is exactly one upstream SUBSCRIBE
//! into it. Real relay, two real subscriber sessions, one process.
mod common;

use std::{sync::{atomic::Ordering, Arc}, time::Duration};

use common::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_downstream_subscribe_reuses_the_first_incarnation_without_an_upstream_request() {
    let probe = probe();
    let relay = start_relay();
    let url = relay.url.clone();
    let mut sender = connect(&url).await;
    let stream = Answer::Accept { objects: 400, gap: Duration::from_millis(5) };
    let (publisher, seen) = publish(&mut sender, "run-c", vec![stream]).await;

    let mut first = connect(&url).await;
    let (a, reader_a) = subscribe(&mut first, "run-c", "pc").await;
    let _a = a.expect("first subscriber accepted");
    read_objects(&reader_a, 1).await; // the first lease is live and flowing
    let mut second = connect(&url).await;
    let (b, reader_b) = subscribe(&mut second, "run-c", "pc").await;
    let _b = b.expect("second subscriber accepted");
    read_objects(&reader_b, 1).await;
    assert_eq!(seen.load(Ordering::SeqCst), 1, "the publisher saw ONE SUBSCRIBE");

    let down = probe.downstream_links("pc");
    assert_eq!(down.len(), 2, "{down:?}");
    assert_eq!(down[0].binding.as_str(), "created");
    assert_eq!(down[1].binding.as_str(), "reused");
    assert!(Arc::ptr_eq(&down[0].track, &down[1].track), "one incarnation");
    assert_ne!(down[0].session, down[1].session, "two downstream sessions");
    let up = probe.upstream_links_into(&down[0].track);
    assert_eq!(up.len(), 1, "exactly one upstream SUBSCRIBE: {up:?}");
    assert!(probe.row("subscribe_sent", up[0].session, up[0].request_id).is_some());
    // Both downstream requests got their own local OK.
    for d in &down {
        assert!(probe.row("subscribe_ok_sent", d.session, d.request_id).is_some(), "{d:?}");
    }

    publisher.stop().await;
    relay.stop();
}
