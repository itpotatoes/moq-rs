//! Spec item (2): after the relay's local SUBSCRIBE_OK, an upstream REFUSAL
//! reaches the downstream subscriber as a terminal carrying the upstream code.
//! The OK-before-refusal order is a PRECONDITION the harness builds (the
//! publisher's answer is held until the subscriber observed the OK); it shows
//! the relay CAN accept before the upstream answers, not that it always does.
//! What the test proves is the propagation after it. Real relay, real QUIC.
//!
//! Observed shape (what the receiver's scripted `Step::CloseError(track,
//! 0x10)` reproduces): `subscribe_open` returns Ok (SUBSCRIBE_OK), then
//! `closed()` returns `Closed(0x10)`; on the wire the relay sends PUBLISH_DONE
//! with status 0x10, not a REQUEST_ERROR, because the request was already
//! accepted.
//!
//! The upstream refusal is HELD (publisher gate) until the application has
//! observed `subscribe_open` == Ok; without the gate the transport may process
//! OK and DONE before the application waiter wakes, and `subscribe_open` then
//! returns the terminal itself (Codex 174a); how a P1 receiver treats that
//! open-error is covered by `exp2_p1_a_retried_target_subscribe_keeps_the_route_generation`.
mod common;

use common::*;
use moq_transport::serve::ServeError;

const DOES_NOT_EXIST: u64 = 0x10;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_upstream_refusal_after_the_relays_local_subscribe_ok_reaches_the_subscriber() {
    let probe = probe();
    let relay = start_relay();
    let url = relay.url.clone();
    let mut sender = connect(&url).await;
    let gate = std::sync::Arc::new(tokio::sync::Notify::new());
    let refuse = Answer::RefuseAfter { code: DOES_NOT_EXIST, gate: gate.clone() };
    let (publisher, seen) = publish(&mut sender, "run-a", vec![refuse]).await;
    let mut receiver = connect(&url).await;

    // The relay's local SUBSCRIBE_OK: subscribe_open returns Ok ...
    // The application waiter is also held 50 ms (Codex 174a's injection):
    // the gate keeps the open's answer Ok regardless.
    let (subscribe, _reader) =
        subscribe_with_held_waiter(&mut receiver, "run-a", "pc-d6", std::time::Duration::from_millis(50)).await;
    let subscribe = subscribe.expect("the relay accepts locally before the upstream answer");
    gate.notify_one(); // only now may the publisher refuse
    // ... then the upstream refusal arrives as the subscription's terminal.
    let terminal = closed(&subscribe).await;
    assert_eq!(terminal, Err(ServeError::Closed(DOES_NOT_EXIST)), "{terminal:?}");
    assert_eq!(seen.load(std::sync::atomic::Ordering::SeqCst), 1, "one SUBSCRIBE reached the publisher");

    // The relay's own observations, joined by binding identity.
    eventually("the relay's PUBLISH_DONE observation", || {
        probe.controls().iter().any(|r| r.boundary == "publish_done_sent" && r.status_code == Some(DOES_NOT_EXIST))
    })
    .await;
    let down = probe.downstream_links("pc-d6");
    assert_eq!(down.len(), 1, "{down:?}");
    let down = &down[0];
    assert_eq!(down.binding.as_str(), "created", "this request created the incarnation");
    let up = probe.upstream_links_into(&down.track);
    assert_eq!(up.len(), 1, "exactly one upstream SUBSCRIBE into that incarnation: {up:?}");
    let up = &up[0];
    assert_ne!(up.session, down.session, "upstream and downstream are different sessions");

    let d_received = probe.row("subscribe_received", down.session, down.request_id).expect("downstream SUBSCRIBE");
    let d_ok = probe.row("subscribe_ok_sent", down.session, down.request_id).expect("downstream SUBSCRIBE_OK");
    let u_sent = probe.row("subscribe_sent", up.session, up.request_id).expect("upstream SUBSCRIBE");
    let u_refused = probe.row("request_error_received", up.session, up.request_id).expect("upstream REQUEST_ERROR");
    let d_done = probe.row("publish_done_sent", down.session, down.request_id).expect("downstream PUBLISH_DONE");
    assert_eq!(u_refused.error_code, Some(DOES_NOT_EXIST));
    assert_eq!(d_done.status_code, Some(DOES_NOT_EXIST), "the upstream code is propagated");
    assert!(probe.row("request_error_sent", down.session, down.request_id).is_none(),
            "an accepted request is never refused with REQUEST_ERROR");
    // Causal order (guaranteed): request -> upstream SUBSCRIBE -> refusal -> PUBLISH_DONE.
    assert!(d_received.seq < down.seq && down.seq < d_ok.seq, "bound before the local OK");
    assert!(up.seq < u_sent.seq && u_sent.seq < u_refused.seq && u_refused.seq < d_done.seq);
    // Sent rows carry the writer start before the write end.
    for row in [&d_ok, &u_sent, &d_done] {
        assert!(row.send_begin.is_some_and(|begin| begin < row.seq), "{row:?}");
    }
    assert!(d_received.send_begin.is_none() && u_refused.send_begin.is_none());
    // Observed, not guaranteed by the protocol: the local OK left first.
    eprintln!(
        "downstream OK seq {} / upstream SUBSCRIBE seq {} / upstream REQUEST_ERROR seq {}",
        d_ok.seq, u_sent.seq, u_refused.seq
    );

    publisher.stop().await;
    relay.stop();
}
