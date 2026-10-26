//! Spec item (9), relay hop: a subscriber's retry after an upstream refusal
//! is a FRESH upstream SUBSCRIBE into a NEW track incarnation (the refused
//! one is never reused), so the publisher sees exactly one SUBSCRIBE per
//! subscriber attempt and every object the subscriber then receives belongs
//! to the retry's incarnation. Real relay, real QUIC sessions, one process.
//! (The first attempt's OK-then-refusal order is built by the harness gate.)
mod common;

use std::{sync::{atomic::Ordering, Arc}, time::Duration};

use common::*;
use moq_transport::serve::ServeError;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retry_after_an_upstream_refusal_reaches_the_publisher_as_a_fresh_subscribe() {
    let probe = probe();
    let relay = start_relay();
    let url = relay.url.clone();
    let mut sender = connect(&url).await;
    let accept = Answer::Accept { objects: 3, gap: Duration::from_millis(5) };
    let gate = Arc::new(tokio::sync::Notify::new());
    let refuse = Answer::RefuseAfter { code: 0x10, gate: gate.clone() };
    let (publisher, seen) = publish(&mut sender, "run-b", vec![refuse, accept]).await;
    let mut receiver = connect(&url).await;

    // Attempt 1: local OK, then the upstream refusal.
    let (first, _reader) =
        subscribe_with_held_waiter(&mut receiver, "run-b", "pc-d6", Duration::from_millis(50)).await;
    let first = first.expect("local OK");
    gate.notify_one(); // the refusal is held until the local OK was observed
    let terminal = tokio::time::timeout(STEP, first.closed()).await.expect("terminal in time");
    assert_eq!(terminal, Err(ServeError::Closed(0x10)));
    drop(first);
    // Attempt 2 (the retry): accepted end to end, objects flow.
    let (second, reader) = subscribe(&mut receiver, "run-b", "pc-d6").await;
    let _second = second.expect("the retry is accepted");
    let objects = read_objects(reader, 3).await;
    assert_eq!(objects.len(), 3);
    assert_eq!(seen.load(Ordering::SeqCst), 2, "one publisher SUBSCRIBE per subscriber attempt");

    let down = probe.downstream_links("pc-d6");
    assert_eq!(down.len(), 2, "{down:?}");
    assert!(down.iter().all(|l| l.binding.as_str() == "created"), "the refused incarnation is not reused: {down:?}");
    assert!(!Arc::ptr_eq(&down[0].track, &down[1].track), "two incarnations");
    let ups: Vec<_> = down.iter().map(|d| probe.upstream_links_into(&d.track)).collect();
    assert!(ups.iter().all(|u| u.len() == 1), "one upstream SUBSCRIBE per incarnation: {ups:?}");
    let (u1, u2) = (&ups[0][0], &ups[1][0]);
    assert_ne!(u1.request_id, u2.request_id, "a new upstream request id");
    assert!(probe.row("request_error_received", u1.session, u1.request_id).is_some());
    assert!(probe.row("subscribe_ok_received", u2.session, u2.request_id).is_some());
    assert!(probe.row("subscribe_ok_received", u1.session, u1.request_id).is_none());
    // The retry's downstream request is new, too.
    assert_ne!(down[0].request_id, down[1].request_id);
    // Every object the relay forwarded on this track belongs to incarnation 2.
    let forwarded: Vec<_> = probe
        .forwards
        .lock()
        .unwrap()
        .iter()
        .filter(|f| Arc::ptr_eq(&f.track, &down[0].track) || Arc::ptr_eq(&f.track, &down[1].track))
        .cloned()
        .collect();
    assert!(!forwarded.is_empty());
    assert!(forwarded.iter().all(|f| Arc::ptr_eq(&f.track, &down[1].track)), "{forwarded:?}");

    publisher.abort();
    relay.stop();
}
