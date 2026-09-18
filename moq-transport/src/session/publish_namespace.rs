// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{collections::VecDeque, ops};

use crate::coding::TrackNamespace;
use crate::watch::State;
use crate::{message, serve::ServeError};

use super::{Publisher, Subscribed, TrackStatusRequested};

/// Information about an outbound PUBLISH_NAMESPACE request.
#[derive(Debug, Clone)]
pub struct PublishNamespaceInfo {
    pub request_id: u64,
    pub namespace: TrackNamespace,
}

struct PublishNamespaceState {
    subscribers: VecDeque<Subscribed>,
    track_statuses_requested: VecDeque<TrackStatusRequested>,
    ok: bool,
    closed: Result<(), ServeError>,
}

impl Default for PublishNamespaceState {
    fn default() -> Self {
        Self {
            subscribers: Default::default(),
            track_statuses_requested: Default::default(),
            ok: false,
            closed: Ok(()),
        }
    }
}

impl Drop for PublishNamespaceState {
    fn drop(&mut self) {
        for subscriber in self.subscribers.drain(..) {
            subscriber
                .close(ServeError::not_found_ctx(
                    "publish_namespace dropped before subscription handled",
                ))
                .ok();
        }
    }
}

/// One step of [`PublishNamespace::subscribed`]: a stored peer error wins over
/// a queued subscriber; `Ok(true)` means a subscriber is queued, `Ok(false)`
/// means wait (or end when the state is dropped).
fn subscribed_step(closed: &Result<(), ServeError>, queued: usize) -> Result<bool, ServeError> {
    closed.clone()?;
    Ok(queued > 0)
}

/// Represents an outbound PUBLISH_NAMESPACE sent by a publisher.
///
/// Dropped with PUBLISH_NAMESPACE_DONE unless already closed with an error.
#[must_use = "send PUBLISH_NAMESPACE_DONE on drop"]
pub struct PublishNamespace {
    publisher: Publisher,
    state: State<PublishNamespaceState>,

    pub info: PublishNamespaceInfo,
}

impl PublishNamespace {
    pub(super) fn new(
        publisher: Publisher,
        request_id: u64,
        namespace: TrackNamespace,
    ) -> (PublishNamespace, PublishNamespaceRecv) {
        let info = PublishNamespaceInfo {
            request_id,
            namespace: namespace.clone(),
        };

        let (send, recv) = State::default().split();

        let send = Self {
            publisher,
            info,
            state: send,
        };
        let recv = PublishNamespaceRecv {
            state: recv,
            request_id,
        };

        (send, recv)
    }

    pub(super) fn send_request(&mut self) {
        self.publisher.send_message(message::PublishNamespace {
            id: self.info.request_id,
            track_namespace: self.info.namespace.clone(),
            params: Default::default(),
        });
    }

    /// Wait until the namespace publish is closed (error or peer disconnect).
    pub async fn closed(&self) -> Result<(), ServeError> {
        loop {
            {
                let state = self.state.lock();
                state.closed.clone()?;

                match state.modified() {
                    Some(notified) => notified,
                    None => return Ok(()),
                }
            }
            .await;
        }
    }

    /// Wait until a subscriber arrives for this namespace.
    ///
    /// A stored peer error (REQUEST_ERROR -> `Closed(code)`,
    /// PUBLISH_NAMESPACE_CANCEL -> `Cancel`) is surfaced FIRST, even when a
    /// subscription is still queued: once the receiving side has recorded the
    /// error and dropped its handle, the queued subscriber can no longer be
    /// handed out (`into_mut` yields `None`) and the error would otherwise be
    /// lost behind an `Ok(None)`.
    pub async fn subscribed(&self) -> Result<Option<Subscribed>, ServeError> {
        Self::subscribed_state(&self.state).await
    }

    async fn subscribed_state(state: &State<PublishNamespaceState>) -> Result<Option<Subscribed>, ServeError> {
        loop {
            {
                let state = state.lock();
                if subscribed_step(&state.closed, state.subscribers.len())? {
                    return Ok(state
                        .into_mut()
                        .and_then(|mut state| state.subscribers.pop_front()));
                }

                match state.modified() {
                    Some(notified) => notified,
                    None => return Ok(None),
                }
            }
            .await;
        }
    }

    /// Wait until a TRACK_STATUS request arrives for this namespace.
    pub async fn track_status_requested(&self) -> Result<Option<TrackStatusRequested>, ServeError> {
        loop {
            {
                let state = self.state.lock();
                if !state.track_statuses_requested.is_empty() {
                    return Ok(state
                        .into_mut()
                        .and_then(|mut state| state.track_statuses_requested.pop_front()));
                }

                state.closed.clone()?;
                match state.modified() {
                    Some(notified) => notified,
                    None => return Ok(None),
                }
            }
            .await;
        }
    }

    /// Wait until the peer has sent REQUEST_OK for this namespace.
    pub async fn ok(&self) -> Result<(), ServeError> {
        loop {
            {
                let state = self.state.lock();
                if state.ok {
                    return Ok(());
                }
                state.closed.clone()?;

                match state.modified() {
                    Some(notified) => notified,
                    None => return Ok(()),
                }
            }
            .await;
        }
    }
}

impl Drop for PublishNamespace {
    fn drop(&mut self) {
        if self.state.lock().closed.is_err() {
            return;
        }

        // Draft-16 §9.22: PUBLISH_NAMESPACE_DONE carries the Request ID,
        // not the namespace.
        self.publisher.send_message(message::PublishNamespaceDone {
            id: self.info.request_id,
        });
    }
}

impl ops::Deref for PublishNamespace {
    type Target = PublishNamespaceInfo;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

/// Peer-facing handle for tracking a PUBLISH_NAMESPACE request.
pub(super) struct PublishNamespaceRecv {
    state: State<PublishNamespaceState>,
    /// Request ID of the outbound PUBLISH_NAMESPACE.
    // Namespace lookup alone is insufficient: both request_id and namespace
    // are needed, so Publisher holds a second index by request_id.
    pub request_id: u64,
}

impl PublishNamespaceRecv {
    pub fn recv_ok(&mut self) -> Result<(), ServeError> {
        if let Some(mut state) = self.state.lock_mut() {
            if state.ok {
                return Err(ServeError::Duplicate);
            }

            state.ok = true;
        }

        Ok(())
    }

    pub fn recv_error(self, err: ServeError) -> Result<(), ServeError> {
        let state = self.state.lock();
        state.closed.clone()?;

        let mut state = state.into_mut().ok_or(ServeError::Done)?;
        state.closed = Err(err);

        Ok(())
    }

    pub fn recv_subscribe(&mut self, subscriber: Subscribed) -> Result<(), ServeError> {
        let mut state = self.state.lock_mut().ok_or(ServeError::Done)?;
        state.subscribers.push_back(subscriber);

        Ok(())
    }

    pub fn recv_track_status_requested(
        &mut self,
        track_status_requested: TrackStatusRequested,
    ) -> Result<(), ServeError> {
        let mut state = self.state.lock_mut().ok_or(ServeError::Done)?;
        state
            .track_statuses_requested
            .push_back(track_status_requested);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribed_surfaces_a_stored_peer_error_before_a_queued_subscription() {
        // REQUEST_ERROR recorded (Closed(code)) while a subscription is still
        // queued and the recv handle was dropped: the error must be observed,
        // never an `Ok(None)`.
        assert!(matches!(
            subscribed_step(&Err(ServeError::Closed(4)), 1),
            Err(ServeError::Closed(4))
        ));
        assert!(matches!(
            subscribed_step(&Err(ServeError::Cancel), 1),
            Err(ServeError::Cancel)
        ));
        assert!(matches!(
            subscribed_step(&Err(ServeError::Closed(4)), 0),
            Err(ServeError::Closed(4))
        ));
        // No error: a queued subscription is handed out, otherwise wait.
        assert!(subscribed_step(&Ok(()), 1).unwrap());
        assert!(!subscribed_step(&Ok(()), 0).unwrap());
    }

    #[tokio::test]
    async fn subscribed_wait_observes_recv_error_after_recv_handle_is_dropped() {
        for error in [ServeError::Closed(4), ServeError::Cancel] {
            let (send, recv) = State::<PublishNamespaceState>::default().split();
            let recv = PublishNamespaceRecv { state: recv, request_id: 7 };
            let wait = PublishNamespace::subscribed_state(&send);
            tokio::pin!(wait);
            assert!(futures::poll!(&mut wait).is_pending());
            // recv_error consumes the receiver, records the error and drops
            // the peer handle. The actual async wait must return that error.
            recv.recv_error(error.clone()).unwrap();
            assert_eq!(wait.await.err(), Some(error));
        }
    }

    #[test]
    fn recv_error_is_stored_once_and_read_first() {
        let (send, recv) = State::<PublishNamespaceState>::default().split();
        let recv = PublishNamespaceRecv {
            state: recv,
            request_id: 7,
        };
        recv.recv_error(ServeError::Closed(4)).unwrap();
        let state = send.lock();
        assert!(matches!(state.closed, Err(ServeError::Closed(4))));
        assert!(matches!(
            subscribed_step(&state.closed, state.subscribers.len()),
            Err(ServeError::Closed(4))
        ));
    }
}
