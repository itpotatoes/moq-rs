//! Codex 174a P3 (null incarnation contract): bindings exist for SUBSCRIBE
//! chains only. (a) A downstream SUBSCRIBE served from an inbound PUBLISH is
//! bound `published` and has NO upstream SUBSCRIBE; the inbound PUBLISH's own
//! PUBLISH_DONE has no binding. (b) A REQUEST_ERROR answering a non-SUBSCRIBE
//! request (TRACK_STATUS for an unknown track) has no binding. In the relay
//! recorder those rows serialize with `track_incarnation` / `track_binding`
//! null — which is normal, not a loss. Real relay, real QUIC.
mod common;

use std::{sync::Arc, time::Duration};

use common::*;
use moq_transport::{
    coding::{KeyValuePairs, TrackNamespace},
    serve::Track,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn published_tracks_and_non_subscribe_errors_have_no_upstream_subscribe_chain() {
    let probe = probe();
    let relay = start_relay();
    let url = relay.url.clone();
    let mut sender = connect(&url).await;
    let ns = TrackNamespace::from_utf8_path("run-pub");

    // (a) An inbound PUBLISH of one exact track.
    let (writer, reader) = Track::new(ns.clone(), "pc").produce();
    let mut published = tokio::time::timeout(STEP, sender.publisher.publish(reader, KeyValuePairs::default()))
        .await
        .expect("PUBLISH answered in time")
        .expect("PUBLISH accepted");
    tokio::time::timeout(STEP, published.ok()).await.expect("PUBLISH_OK in time").expect("PUBLISH_OK");
    let serving = tokio::spawn(published.serve());
    let mut subgroups = writer.subgroups().expect("subgroups");
    let producer = tokio::spawn(async move {
        for i in 0..200u64 {
            let mut subgroup = subgroups.append(1).expect("subgroup");
            subgroup.write(i.to_le_bytes().to_vec().into()).expect("object");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        subgroups
    });

    let mut receiver = connect(&url).await;
    let (sub, reader) = subscribe(&mut receiver, "run-pub", "pc").await;
    let _sub = sub.expect("served from the published track");
    read_objects(reader, 1).await;
    let down = probe.downstream_links("pc");
    assert_eq!(down.len(), 1, "{down:?}");
    assert_eq!(down[0].binding.as_str(), "published");
    assert!(probe.upstream_links_into(&down[0].track).is_empty(), "no upstream SUBSCRIBE for a published track");
    assert!(probe.row("subscribe_ok_sent", down[0].session, down[0].request_id).is_some());

    // (b) A non-SUBSCRIBE request refused by the relay.
    receiver.subscriber.track_status(&ns, "nope");
    eventually("the relay's REQUEST_ERROR for the TRACK_STATUS", || {
        probe.controls().iter().any(|r| r.boundary == "request_error_sent" && r.session == down[0].session)
    })
    .await;
    let refusal = probe
        .controls()
        .into_iter()
        .find(|r| r.boundary == "request_error_sent" && r.session == down[0].session)
        .unwrap();
    assert!(
        !probe.links().iter().any(|l| l.session == refusal.session && l.request_id == refusal.request_id
            && l.binding.is_downstream()),
        "no binding for a non-SUBSCRIBE request: {refusal:?}"
    );

    // The inbound PUBLISH ends: its PUBLISH_DONE reaches the relay unbound.
    producer.abort();
    serving.abort();
    drop(sender);
    eventually("the relay's PUBLISH_DONE for the inbound PUBLISH", || {
        probe.controls().iter().any(|r| r.boundary == "publish_done_received" && r.session != down[0].session)
    })
    .await;
    let done = probe
        .controls()
        .into_iter()
        .find(|r| r.boundary == "publish_done_received" && r.session != down[0].session)
        .unwrap();
    assert!(
        !probe.links().iter().any(|l| l.session == done.session && l.request_id == done.request_id),
        "the inbound PUBLISH's DONE has no binding: {done:?}"
    );
    let _ = Arc::strong_count(&down[0].track);
    relay.stop();
}
