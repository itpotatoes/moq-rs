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

/// Codex 177a P2: `stop()` aborts EVERY child before joining any, and a join
/// is bounded: a child stuck inside a poll makes `stop_within` fail at the
/// bound instead of hanging, and the children after it were still aborted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stopping_a_publisher_is_bounded_and_aborts_every_child_first() {
    use std::sync::atomic::{AtomicBool, Ordering};
    struct SetOnDrop(Arc<AtomicBool>);
    impl Drop for SetOnDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let started = Arc::new(AtomicBool::new(false));
    let flag = started.clone();
    // Blocks its worker inside one poll for 1 s: abort cannot interrupt it.
    let stuck = tokio::spawn(async move {
        flag.store(true, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(1_000));
    });
    while !started.load(Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    let dropped = Arc::new(AtomicBool::new(false));
    let guard = SetOnDrop(dropped.clone());
    let later = tokio::spawn(async move {
        let _guard = guard;
        std::future::pending::<()>().await;
    });
    let children = Arc::new(Mutex::new(vec![stuck, later]));
    let main = tokio::spawn(std::future::pending::<()>());
    let begin = Instant::now();
    let result = PublisherTask::from_parts(main, children).stop_within(Duration::from_millis(200)).await;
    let error = result.expect_err("a stuck child fails the stop");
    assert!(error.contains("did not finish within"), "{error}");
    assert!(begin.elapsed() < Duration::from_millis(900), "bounded, not waiting for the stuck child");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(dropped.load(Ordering::SeqCst), "the later child was aborted before the stuck one was joined");
}

/// Codex 178a (its reproduction, moved here): the parent's poll that is
/// running when `stop` aborts it registers one more child; `stop` must reap
/// that late child too (abort and join it) instead of returning Ok with the
/// child still running. An independent thread releases the parent's poll
/// only after `stop` has taken the first child list.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stopping_a_publisher_reaps_children_registered_by_the_parents_last_poll() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let children = Arc::new(Mutex::new(vec![tokio::spawn(std::future::pending::<()>())]));
    let main_started = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicBool::new(false));
    struct OnDrop(Arc<AtomicBool>);
    impl Drop for OnDrop {
        fn drop(&mut self) { self.0.store(true, Ordering::SeqCst); }
    }
    let main = tokio::spawn({
        let children = children.clone();
        let main_started = main_started.clone();
        let release = release.clone();
        let guard = OnDrop(dropped.clone());
        async move {
            main_started.store(true, Ordering::SeqCst);
            while !release.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(1));
            }
            // This is still the same poll. abort() cannot stop it here.
            children.lock().unwrap().push(tokio::spawn(async move {
                let _guard = guard;
                std::future::pending::<()>().await;
            }));
        }
    });
    while !main_started.load(Ordering::SeqCst) { tokio::task::yield_now().await; }
    let releaser = std::thread::spawn({
        let children = children.clone();
        let release = release.clone();
        move || {
            let begin = Instant::now();
            // New implementation drains before the parent settles. The
            // deadline also lets the original 177a implementation progress.
            while !children.lock().unwrap().is_empty() && begin.elapsed() < Duration::from_millis(300) {
                std::thread::sleep(Duration::from_millis(1));
            }
            release.store(true, Ordering::SeqCst);
        }
    });
    let result = PublisherTask::from_parts(main, children.clone()).stop_within(Duration::from_secs(2)).await;
    releaser.join().unwrap();
    let leftovers = std::mem::take(&mut *children.lock().unwrap());
    let missed = leftovers.len();
    let was_dropped = dropped.load(Ordering::SeqCst);
    eprintln!("stop result={result:?} missed_children={missed} late_child_dropped={was_dropped}");
    // Clean up the rig even when the helper failed to do so.
    for child in leftovers { child.abort(); let _ = child.await; }
    assert!(result.is_ok());
    assert_eq!(missed, 0, "stop returned Ok but skipped the parent's last child");
    assert!(was_dropped, "the late child must be cancelled and joined");
}
