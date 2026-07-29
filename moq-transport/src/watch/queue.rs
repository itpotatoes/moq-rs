// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use super::State;
use futures::channel::oneshot;
use std::collections::VecDeque;

pub struct Queue<T> {
    state: State<VecDeque<(T, Option<oneshot::Sender<()>>)>>, // store optional notifier per item
}

impl<T> Queue<T> {
    /// Push an item onto the queue. Returns Err(item) if the queue has been closed.
    pub fn push(&mut self, item: T) -> Result<(), T> {
        match self.state.lock_mut() {
            Some(mut state) => state.push_back((item, None)),
            None => return Err(item),
        };

        Ok(())
    }

    /// Push an item without panicking if the queue lock is poisoned.
    pub fn try_push(&mut self, item: T) -> Result<(), T> {
        match self.state.try_lock_mut() {
            Ok(Some(mut state)) => state.push_back((item, None)),
            Ok(None) => return Err(item),
            Err(_) => {
                tracing::error!("queue lock poisoned while pushing item");
                return Err(item);
            }
        };

        Ok(())
    }

    /// Pop an item from the queue, waiting if necessary.
    pub async fn pop(&mut self) -> Option<T> {
        loop {
            // Scope 1: try to pop an item
            {
                let queue = self.state.lock();
                if !queue.is_empty() {
                    // Take mutable access only in a block
                    if let Some((item, notifier)) = {
                        let mut state_mut = queue.into_mut()?;
                        state_mut.pop_front()
                    } {
                        if let Some(tx) = notifier {
                            let _ = tx.send(()); // notify waiter
                        }
                        return Some(item);
                    }
                }
            }

            // Scope 2: wait for modifications.
            //
            // Re-check emptiness under the SAME lock whose epoch `modified()`
            // snapshots. A push can land between scope 1 releasing the lock
            // and this acquisition; that push's `notify()` already bumped the
            // epoch, so the snapshot would include it and awaiting
            // `modified()` would sleep until the *next* push while the item
            // sits in the queue (lost wakeup).
            //
            // The guard is consumed inside the block so only the `Send`
            // `StateChanged` future is held across the await point.
            let modified = {
                let queue = self.state.lock();
                if !queue.is_empty() {
                    continue;
                }
                queue.modified()?
            };
            modified.await;
        }
    }

    /// Drop the state
    pub fn close(self) -> Vec<T> {
        // Drain the queue of any remaining entries
        let res = match self.state.lock_mut() {
            Some(mut queue) => queue.drain(..).map(|(item, _)| item).collect(),
            _ => Vec::new(),
        };

        // Prevent any new entries from being added
        drop(self.state);

        res
    }

    /// Push an item and wait until it is popped.
    /// Returns Ok(()) if the item was successfully popped.
    /// Returns Err(()) if the queue was closed before the item could be confirmed popped.
    pub async fn push_and_wait_until_popped(&mut self, item: T) -> Result<(), ()> {
        // Create a oneshot channel
        let (tx, rx) = oneshot::channel();

        // Push the item along with the sender
        match self.state.lock_mut() {
            Some(mut state) => state.push_back((item, Some(tx))),
            None => return Err(()), // Queue already closed before push
        }

        // Wait until the item is popped.
        // If we receive Canceled, it means the sender was dropped without sending,
        // which indicates the queue was closed while we were waiting.
        rx.await.map_err(|_| ())
    }

    /// Split the queue into two handles that share the same underlying state.
    pub fn split(self) -> (Self, Self) {
        let state = self.state.split();
        (Self { state: state.0 }, Self { state: state.1 })
    }
}

impl<T> Clone for Queue<T> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

impl<T> Default for Queue<T> {
    fn default() -> Self {
        Self {
            state: State::new(Default::default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Regression test for the `pop()` lost-wakeup race.
    ///
    /// Before the fix, a push landing between `pop()`'s scope-1 unlock and its
    /// scope-2 lock had its `notify()` folded into the epoch snapshot taken by
    /// `modified()`, so `pop()` slept until the *next* push while the item sat
    /// in the queue. With no subsequent push, the item was stranded
    /// indefinitely (observed in the field as a 30 s control-message stall).
    ///
    /// An OS thread pushes small bursts with randomized spin micro-delays so
    /// pushes sweep across every phase of the popper's unlock->lock gap, while
    /// an async popper (driven on a second thread) forwards each popped item
    /// through a channel. Every pushed item must arrive within a bounded
    /// timeout; the last item of each burst has no rescue push, so any lost
    /// wakeup trips the timeout instead of self-healing.
    #[test]
    fn pop_does_not_lose_wakeup_when_push_races_lock_gap() {
        const ROUNDS: usize = 30_000;
        const BURST: usize = 2;
        const TOTAL: usize = ROUNDS * BURST;
        const ITEM_TIMEOUT: Duration = Duration::from_secs(2);

        let queue: Queue<usize> = Queue::default();
        let mut popper = queue.clone();
        let mut pusher = queue;

        let (tx, rx) = std::sync::mpsc::channel::<usize>();

        let popper_thread = std::thread::spawn(move || {
            futures::executor::block_on(async move {
                for _ in 0..TOTAL {
                    match popper.pop().await {
                        // Receiver may be gone if the main thread already
                        // panicked on a timeout; just stop.
                        Some(item) => {
                            if tx.send(item).is_err() {
                                return;
                            }
                        }
                        None => return,
                    }
                }
            });
        });

        // xorshift64* keeps the delay sweep deterministic across runs.
        let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut rand = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };

        let mut next_push = 0usize;
        let mut next_pop = 0usize;
        for _ in 0..ROUNDS {
            for _ in 0..BURST {
                pusher.push(next_push).expect("queue closed during push");
                next_push += 1;
                // 0..8192 spin-loop iterations gives roughly 0..~10us of
                // fine-grained phase offset against the popper's re-entry.
                for _ in 0..(rand() % 8192) {
                    std::hint::spin_loop();
                }
            }
            for _ in 0..BURST {
                let got = rx.recv_timeout(ITEM_TIMEOUT).unwrap_or_else(|_| {
                    panic!("lost wakeup: pushed item {next_pop} not popped within {ITEM_TIMEOUT:?}")
                });
                assert_eq!(got, next_pop, "FIFO order violated");
                next_pop += 1;
            }
        }

        popper_thread.join().expect("popper thread panicked");
    }
}
