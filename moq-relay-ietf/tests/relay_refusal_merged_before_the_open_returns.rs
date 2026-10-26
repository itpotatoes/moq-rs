//! Spec item (2), the other legal application-side shape (Codex 174a): the
//! relay's local SUBSCRIBE_OK and the propagated refusal (PUBLISH_DONE 0x10)
//! are BOTH processed by the subscriber's transport before the application's
//! `subscribe_open` waiter runs again. The open then returns the terminal
//! itself, `Err(Closed(0x10))` (an open-error, which a P1 receiver treats as a
//! retryable 0x10 refusal). Real relay, real QUIC; the application waiter is
//! held, never the transport.
//!
//! "Processed" means DISPATCHED: the control hook runs before dispatch, so the
//! test fences on a LATER response on the same incoming control stream (a
//! TRACK_STATUS refusal), and a 50 ms stall injected right after the DONE's
//! hook (Codex 175a) must not change the result. Order: asserted are
//! `OK < DONE` and `refusal < DONE` (causal); `OK < refusal` is only printed —
//! observed in these runs, not a protocol guarantee.
mod common;

use std::task::Poll;

use common::*;
use moq_transport::{coding::TrackNamespace, serve::{ServeError, Track}};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ok_and_refusal_processed_before_the_waiter_runs_make_the_open_return_the_refusal() {
    let probe = probe();
    let relay = start_relay();
    let url = relay.url.clone();
    let mut sender = connect(&url).await;
    let (publisher, _seen) = publish(&mut sender, "run-m", vec![Answer::Refuse]).await;
    let mut receiver = connect(&url).await;
    let mut fence = receiver.subscriber.clone();
    *probe.hold_after.lock().unwrap() =
        Some(("publish_done_received", std::time::Duration::from_millis(50)));
    let (writer, _reader) = Track::new(TrackNamespace::from_utf8_path("run-m"), "pc-d6").produce();
    let mut open = Box::pin(receiver.subscriber.subscribe_open(writer));
    assert!(matches!(futures::poll!(open.as_mut()), Poll::Pending), "the SUBSCRIBE is in flight");
    // Hold only the application waiter until the subscriber's transport has
    // processed both the SUBSCRIBE_OK and the PUBLISH_DONE.
    eventually("the subscriber received OK and the propagated refusal", || {
        let rows = probe.controls();
        rows.iter().any(|r| r.boundary == "subscribe_ok_received")
            && rows.iter().any(|r| r.boundary == "publish_done_received" && r.status_code == Some(0x10))
    })
    .await;
    // Fence: a later response on the subscriber's same control stream is
    // decoded only after the DONE before it was dispatched.
    let client = probe
        .controls()
        .into_iter()
        .find(|r| r.boundary == "publish_done_received" && r.status_code == Some(0x10))
        .expect("the subscriber's DONE")
        .session;
    fence.track_status(&TrackNamespace::from_utf8_path("run-m"), "dispatch-fence");
    eventually("a later response on the subscriber's control stream", || {
        probe.controls().iter().any(|r| r.boundary == "request_error_received" && r.session == client)
    })
    .await;
    *probe.hold_after.lock().unwrap() = None;
    let result = tokio::time::timeout(STEP, open).await.expect("open resolves");
    assert!(matches!(result, Err(ServeError::Closed(0x10))), "the open returns the refusal");
    // The relay side: the upstream refusal and the local OK both precede the
    // PUBLISH_DONE that carries the refusal's code.
    let down = probe.downstream_links("pc-d6");
    assert_eq!(down.len(), 1);
    assert_eq!(down[0].binding.as_str(), "created");
    let up = probe.upstream_links_into(&down[0].track);
    assert_eq!(up.len(), 1);
    let ok = probe.row("subscribe_ok_sent", down[0].session, down[0].request_id).expect("local OK");
    let refusal = probe.row("request_error_received", up[0].session, up[0].request_id).expect("refusal");
    let done = probe.row("publish_done_sent", down[0].session, down[0].request_id).expect("DONE");
    assert_eq!((refusal.error_code, done.status_code), (Some(0x10), Some(0x10)));
    assert!(refusal.seq < done.seq);
    assert!(ok.seq < done.seq, "OK left before the DONE on the wire");
    eprintln!("observed (not guaranteed): OK seq {} / upstream refusal seq {} / DONE seq {}",
              ok.seq, refusal.seq, done.seq);
    publisher.abort();
    relay.stop();
}
