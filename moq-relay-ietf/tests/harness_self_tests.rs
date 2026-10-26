//! Self-tests of the relay integration harness (`tests/common`), no relay:
//! the held waiter returns a first-poll Ready result as is (Codex 176a P2:
//! re-polling it panicked with "async fn resumed after completion"), and a
//! stopped publisher joins its children and fails on a child panic (176a P3).
mod common;

use std::{sync::{Arc, Mutex}, time::{Duration, Instant}};

use common::*;

#[tokio::test]
async fn the_held_waiter_returns_a_ready_first_poll_without_polling_again() {
    // Both complete on their first poll; a second poll would panic.
    assert_eq!(hold_waiter(std::future::ready(7), Duration::from_millis(50)).await, 7);
    assert_eq!(hold_waiter(async { 8 }, Duration::from_millis(50)).await, 8);
}

#[tokio::test]
async fn the_held_waiter_holds_a_pending_future_and_then_awaits_it() {
    let start = Instant::now();
    let value = hold_waiter(
        async {
            tokio::time::sleep(Duration::from_millis(1)).await;
            9
        },
        Duration::from_millis(50),
    )
    .await;
    assert_eq!(value, 9);
    assert!(start.elapsed() >= Duration::from_millis(50), "held for the delay");
}

#[tokio::test]
#[should_panic(expected = "a publisher child task panicked")]
async fn stopping_a_publisher_fails_on_a_child_panic() {
    let child = tokio::spawn(async { panic!("child failure") });
    while !child.is_finished() {
        tokio::task::yield_now().await;
    }
    let children = Arc::new(Mutex::new(vec![child]));
    let main = tokio::spawn(std::future::pending::<()>());
    PublisherTask::from_parts(main, children).stop().await;
}

#[tokio::test]
async fn stopping_a_publisher_cancels_and_joins_running_children() {
    let children = Arc::new(Mutex::new(vec![tokio::spawn(std::future::pending::<()>())]));
    let main = tokio::spawn(std::future::pending::<()>());
    PublisherTask::from_parts(main, children.clone()).stop().await;
    assert!(children.lock().unwrap().is_empty(), "every child was taken and joined");
}
