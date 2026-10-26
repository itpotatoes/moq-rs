//! Codex 174a P3: the code spaces collide. An upstream REQUEST_ERROR with
//! code 2 (Timeout) after the relay's local OK reaches the subscriber as
//! PUBLISH_DONE status 2, which is TrackEnded, so `closed() == Ok` exactly as
//! for a publisher's normal end of the track. The two are distinguishable
//! ONLY through the relay trace: the refused incarnation has an upstream
//! `request_error_received` (code 2); the normally ended one has an upstream
//! `subscribe_ok_received` and `publish_done_received` (2) and no refusal.
//! (The collision itself is upstream moq-transport design; not changed.)
mod common;

use std::sync::Arc;

use common::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_timeout_refusal_and_a_normal_track_end_look_the_same_downstream_but_not_in_the_relay_trace() {
    let probe = probe();
    let relay = start_relay();
    let url = relay.url.clone();
    let mut sender = connect(&url).await;
    let gate = Arc::new(tokio::sync::Notify::new());
    let end_gate = Arc::new(tokio::sync::Notify::new());
    let answers = vec![
        Answer::RefuseAfter { code: 0x2, gate: gate.clone() },
        Answer::AcceptThenEnd { objects: 2, gate: end_gate.clone() },
    ];
    // Both opens are taken with the application waiter held 50 ms (Codex
    // 175a's injection); both publisher answers are gated on the observed OK.
    let held = std::time::Duration::from_millis(50);
    let (publisher, _seen) = publish(&mut sender, "run-t", answers).await;
    let mut receiver = connect(&url).await;

    let (refused, _r1) = subscribe_with_held_waiter(&mut receiver, "run-t", "pc-d6", held).await;
    let refused = refused.expect("local OK");
    gate.notify_one();
    let refused_end = closed(&refused).await;
    let (ended, _r2) = subscribe_with_held_waiter(&mut receiver, "run-t", "pc-d7", held).await;
    let ended = ended.expect("accepted");
    end_gate.notify_one();
    let normal_end = closed(&ended).await;
    // Downstream: the SAME terminal for both.
    assert_eq!(refused_end, Ok(()), "a code-2 refusal reads as TrackEnded at the subscriber");
    assert_eq!(normal_end, Ok(()));

    let chain = |track: &str| {
        let down = probe.downstream_links(track);
        assert_eq!(down.len(), 1, "{track}: {down:?}");
        let up = probe.upstream_links_into(&down[0].track);
        assert_eq!(up.len(), 1, "{track}: {up:?}");
        (down[0].clone(), up[0].clone())
    };
    let (d_ref, u_ref) = chain("pc-d6");
    let (d_end, u_end) = chain("pc-d7");
    eventually("both downstream PUBLISH_DONE rows", || {
        probe.row("publish_done_sent", d_ref.session, d_ref.request_id).is_some()
            && probe.row("publish_done_sent", d_end.session, d_end.request_id).is_some()
    })
    .await;
    for d in [&d_ref, &d_end] {
        let done = probe.row("publish_done_sent", d.session, d.request_id).unwrap();
        assert_eq!(done.status_code, Some(0x2), "the same downstream code: {done:?}");
    }
    // The relay trace tells them apart, by the upstream rows of each incarnation.
    let refusal = probe.row("request_error_received", u_ref.session, u_ref.request_id);
    assert_eq!(refusal.map(|r| r.error_code), Some(Some(0x2)), "the refused chain has the upstream refusal");
    assert!(probe.row("subscribe_ok_received", u_ref.session, u_ref.request_id).is_none());
    assert!(probe.row("request_error_received", u_end.session, u_end.request_id).is_none(), "no refusal");
    assert!(probe.row("subscribe_ok_received", u_end.session, u_end.request_id).is_some());
    let upstream_done = probe.row("publish_done_received", u_end.session, u_end.request_id)
        .expect("the publisher's own normal end");
    assert_eq!(upstream_done.status_code, Some(0x2));
    publisher.stop().await;
    relay.stop();
}
