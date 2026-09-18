// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::ops;
use std::sync::{Arc, Mutex};

use futures::stream::FuturesUnordered;
use futures::StreamExt;

use crate::coding::{Encode, KeyValuePairs, Location, ReasonPhrase};
use crate::message::RequestErrorCode;
use crate::mlog;
use crate::serve::{ServeError, TrackReaderMode};
use crate::watch::State;
use crate::{data, message, serve};

use super::{DeliveryFilter, Publisher, SessionError, SubscribeInfo, Writer};

// This file defines Publisher handling of inbound Subscriptions

/// Draft-16 §10.4.3 RESET_STREAM code for an object delivery timeout.
const DELIVERY_TIMEOUT_RESET_CODE: u32 = 0x2;

#[derive(Debug)]
struct ObjectForwarderState {
    largest_location: Option<Location>,
    stream_count: u64,
    delivery_timeouts: u64,
    delivery_timeout_resets: u64,
    /// Set to true when UNSUBSCRIBE is received.  When true, Drop skips sending
    /// PUBLISH_DONE or REQUEST_ERROR because the subscriber already terminated.
    unsubscribed: bool,
    closed: Result<(), ServeError>,
}

impl ObjectForwarderState {
    fn record_stream_opened(&mut self) {
        self.stream_count = self.stream_count.saturating_add(1);
    }

    fn update_largest_location(&mut self, group_id: u64, object_id: u64) -> Result<(), ServeError> {
        if let Some(current_largest_location) = self.largest_location {
            let update_largest_location = Location::new(group_id, object_id);
            if update_largest_location > current_largest_location {
                self.largest_location = Some(update_largest_location);
            }
        }

        Ok(())
    }

    fn record_delivery_timeout(&mut self, reset: bool) {
        self.delivery_timeouts = self.delivery_timeouts.saturating_add(1);
        if reset {
            self.delivery_timeout_resets = self.delivery_timeout_resets.saturating_add(1);
        }
    }
}

impl Default for ObjectForwarderState {
    fn default() -> Self {
        Self {
            largest_location: None,
            stream_count: 0,
            delivery_timeouts: 0,
            delivery_timeout_resets: 0,
            unsubscribed: false,
            closed: Ok(()),
        }
    }
}

pub struct Subscribed {
    /// The tracknamespace and trackname for the subscription.
    pub info: SubscribeInfo,

    forwarder: ObjectForwarder,

    /// Tracks if SubscribeOk has been sent yet or not. Used to send
    /// PUBLISH_DONE vs REQUEST_ERROR on drop.
    ok: bool,

    /// Largest location captured when SUBSCRIBE_OK was sent. The outer Option
    /// distinguishes "not accepted" from an accepted empty track.
    accepted_largest: Option<Option<Location>>,
}

pub(super) struct ObjectForwarder {
    /// The sessions Publisher manager, used to create streams and datagrams.
    publisher: Publisher,
    state: State<ObjectForwarderState>,
    track_alias: u64,
    mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
}

/// The forwarder's stored terminal, if any: `Cancel` after a peer
/// UNSUBSCRIBE (`ObjectForwarderRecv::recv_unsubscribe`) or a local `close`,
/// otherwise the error passed to `close`. `None` when nothing is stored, which
/// says nothing about whether the watch peer is still alive. Reads with
/// `lock()`, which keeps working after the peer dropped the state.
fn stored_terminal(state: &State<ObjectForwarderState>) -> Option<ServeError> {
    state.lock().closed.clone().err()
}

/// Error for a serve/subgroup path that found the forwarder state vanished
/// (`lock_mut()`/`into_mut()` returned `None`). `recv_unsubscribe` stores
/// `Cancel` and only then drops the recv half, so a child racing that drop
/// must surface the peer's stored terminal (track-level `Cancel`) instead of
/// a synthetic `Done`; a pure watch loss with nothing stored stays `Done`.
fn terminal_or_done(state: &State<ObjectForwarderState>) -> ServeError {
    stored_terminal(state).unwrap_or(ServeError::Done)
}

/// Store `err` as the forwarder's terminal state. Fails with the error that
/// is already stored (a `Cancel` from UNSUBSCRIBE) or `Done` when the watch
/// peer is gone.
fn close_forwarder_state(
    state: &State<ObjectForwarderState>,
    err: ServeError,
) -> Result<(), ServeError> {
    let state = state.lock();
    state.closed.clone()?;

    let mut state = state.into_mut().ok_or(ServeError::Done)?;
    state.closed = Err(err);

    Ok(())
}

/// Cleanup after `serve` returned: store the error, but ALWAYS return the
/// FIRST observed serve result. A racing UNSUBSCRIBE (stored `Cancel`) or a
/// vanished watch peer (`Done`) must not replace the transport error that
/// actually ended forwarding; the stored value is only logged.
fn finish_serve(
    state: &State<ObjectForwarderState>,
    res: Result<(), SessionError>,
) -> Result<(), SessionError> {
    if let Err(err) = &res {
        if let Err(stored) = close_forwarder_state(state, err.clone().into()) {
            tracing::debug!(
                first = ?err,
                stored = ?stored,
                "forwarder already closed; keeping the first serve error"
            );
        }
    }
    res
}

impl ObjectForwarder {
    pub(super) fn new(
        publisher: Publisher,
        track_alias: u64,
        mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
    ) -> (Self, ObjectForwarderRecv) {
        let (send, recv) = State::default().split();
        let send = Self {
            publisher,
            state: send,
            track_alias,
            mlog,
        };
        let recv = ObjectForwarderRecv { state: recv };
        (send, recv)
    }

    pub(super) fn set_largest_location(
        &self,
        largest_location: Option<Location>,
    ) -> Result<(), ServeError> {
        self.state
            .lock_mut()
            .ok_or(ServeError::Cancel)?
            .largest_location = largest_location;
        Ok(())
    }

    fn terminal_state(&self) -> (ServeError, u64, bool) {
        let state = self.state.lock();
        let err = state
            .closed
            .as_ref()
            .err()
            .cloned()
            .unwrap_or(ServeError::Done);
        (err, state.stream_count, state.unsubscribed)
    }

    fn close(&self, err: ServeError) -> Result<(), ServeError> {
        close_forwarder_state(&self.state, err)
    }

    async fn closed(&self) -> Result<(), ServeError> {
        loop {
            {
                let state = self.state.lock();
                state.closed.clone()?;

                match state.modified() {
                    Some(notify) => notify,
                    None => return Ok(()),
                }
            }
            .await;
        }
    }

    pub(super) async fn serve(
        &mut self,
        track: serve::TrackReader,
        delivery_filter: DeliveryFilter,
        delivery_timeout: Option<std::time::Duration>,
    ) -> Result<(), SessionError> {
        match track.mode().await? {
            TrackReaderMode::Stream(_stream) => Err(SessionError::Serve(
                ServeError::not_implemented_ctx("stream track reader mode"),
            )),
            TrackReaderMode::Subgroups(subgroups) => {
                self.serve_subgroups(subgroups, delivery_filter, delivery_timeout)
                    .await
            }
            TrackReaderMode::Datagrams(datagrams) => {
                self.serve_datagrams(datagrams, delivery_filter).await
            }
        }
    }
}

enum SubgroupOutput {
    Stream(Writer),
    #[cfg(test)]
    Buffer(bytes::BytesMut),
}

impl SubgroupOutput {
    async fn encode<T: Encode>(&mut self, msg: &T) -> Result<(), SessionError> {
        match self {
            Self::Stream(writer) => writer.encode(msg).await,
            #[cfg(test)]
            Self::Buffer(buffer) => {
                msg.encode(buffer)?;
                Ok(())
            }
        }
    }

    async fn write(&mut self, buf: &[u8]) -> Result<(), SessionError> {
        match self {
            Self::Stream(writer) => writer.write(buf).await,
            #[cfg(test)]
            Self::Buffer(buffer) => {
                buffer.extend_from_slice(buf);
                Ok(())
            }
        }
    }

    fn reset(&mut self, code: u32) -> bool {
        match self {
            Self::Stream(writer) => {
                writer.reset(code);
                true
            }
            #[cfg(test)]
            Self::Buffer(_) => false,
        }
    }

    #[cfg(test)]
    fn into_buffer(self) -> bytes::BytesMut {
        match self {
            Self::Buffer(buffer) => buffer,
            Self::Stream(_) => unreachable!("test output should use a buffer"),
        }
    }
}

impl Subscribed {
    pub(super) fn new(
        publisher: Publisher,
        msg: message::Subscribe,
        mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
    ) -> Result<(Self, ObjectForwarderRecv), SessionError> {
        let info = SubscribeInfo::new_from_subscribe(&msg)?;
        let track_alias = info.id;
        let (forwarder, recv) = ObjectForwarder::new(publisher, track_alias, mlog);
        let send = Self {
            info,
            forwarder,
            ok: false,
            accepted_largest: None,
        };

        Ok((send, recv))
    }

    pub async fn serve(mut self, track: serve::TrackReader) -> Result<(), SessionError> {
        let res = async {
            self.accept(&track).await?;
            self.serve_accepted_inner(track).await
        }
        .await;
        finish_serve(&self.forwarder.state, res)
    }

    /// Send SUBSCRIBE_OK without starting object forwarding.
    ///
    /// Subscription-scoped producers use this barrier to ensure they do not
    /// generate objects before the peer has received an accepted track alias.
    pub async fn accept(&mut self, track: &serve::TrackReader) -> Result<(), SessionError> {
        if self.ok {
            return Err(SessionError::Duplicate);
        }
        if track.namespace != self.info.track_namespace || track.name != self.info.track_name {
            return Err(SessionError::Internal);
        }

        // Update largest location before sending SubscribeOk
        let largest_location = track.largest_location();
        self.forwarder.set_largest_location(largest_location)?;

        // Send SubscribeOk using send_message_and_wait to ensure it is sent at least to the QUIC stack before
        // we start serving the track.  If a subscriber gets the stream before SubscribeOk
        // then they won't recognize the track_alias in the stream header.
        let mut params = KeyValuePairs::default();
        if let Some(largest) = largest_location {
            params
                .set_largest_object(largest)
                .map_err(|_| SessionError::Internal)?;
        }
        if let Some(timeout_ms) = self.info.delivery_timeout_ms {
            params.set_delivery_timeout(timeout_ms);
        }

        self.forwarder
            .publisher
            .send_message_and_wait(message::SubscribeOk {
                id: self.info.id,
                track_alias: self.info.id,
                params,
                track_extensions: Default::default(),
            })
            .await;

        self.ok = true; // So we send PUBLISH_DONE on drop
        self.accepted_largest = Some(largest_location);
        Ok(())
    }

    /// Forward an already accepted subscription. [`Self::accept`] must have
    /// completed for this same track before calling this method.
    pub async fn serve_accepted(mut self, track: serve::TrackReader) -> Result<(), SessionError> {
        let res = self.serve_accepted_inner(track).await;
        finish_serve(&self.forwarder.state, res)
    }

    async fn serve_accepted_inner(
        &mut self,
        track: serve::TrackReader,
    ) -> Result<(), SessionError> {
        if track.namespace != self.info.track_namespace || track.name != self.info.track_name {
            return Err(SessionError::Internal);
        }
        let largest_location = self.accepted_largest.ok_or(SessionError::Internal)?;
        let delivery_filter = self.info.delivery_filter(largest_location);
        let delivery_timeout = self
            .info
            .delivery_timeout_ms
            .map(std::time::Duration::from_millis);

        self.forwarder
            .serve(track, delivery_filter, delivery_timeout)
            .await
    }

    pub fn close(self, err: ServeError) -> Result<(), ServeError> {
        self.forwarder.close(err)
    }

    pub async fn closed(&self) -> Result<(), ServeError> {
        self.forwarder.closed().await
    }
}

impl ops::Deref for Subscribed {
    type Target = SubscribeInfo;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

impl Drop for Subscribed {
    fn drop(&mut self) {
        let (err, stream_count, unsubscribed) = self.forwarder.terminal_state();

        // Subscriber already sent UNSUBSCRIBE — no terminal message needed.
        if unsubscribed {
            return;
        }

        if self.ok {
            self.forwarder.publisher.send_message(message::PublishDone {
                id: self.info.id,
                status_code: Self::publish_done_code(&err),
                stream_count,
                reason: ReasonPhrase(err.to_string()),
            });
        } else {
            // Draft-16 §9.8: subscription rejection uses REQUEST_ERROR, not the
            // legacy SUBSCRIBE_ERROR.
            self.forwarder.publisher.send_request_error(
                "subscribe",
                message::RequestError {
                    id: self.info.id,
                    error_code: Self::request_error_code(&err),
                    retry_interval: 0,
                    reason: ReasonPhrase(err.to_string()),
                },
            );
            self.forwarder.publisher.drop_subscribe(self.info.id);
        };
    }
}

impl Subscribed {
    fn publish_done_code(err: &ServeError) -> u64 {
        match err {
            ServeError::Done => message::PublishDoneCode::TrackEnded as u64,
            ServeError::Closed(code) => *code,
            _ => message::PublishDoneCode::InternalError as u64,
        }
    }

    fn request_error_code(err: &ServeError) -> u64 {
        match err {
            ServeError::Closed(code) => *code,
            ServeError::NotFound | ServeError::NotFoundWithId(_, _) => {
                RequestErrorCode::DoesNotExist as u64
            }
            ServeError::Duplicate => RequestErrorCode::DuplicateSubscription as u64,
            ServeError::Cancel | ServeError::Done => RequestErrorCode::Uninterested as u64,
            ServeError::Mode
            | ServeError::Size
            | ServeError::NotImplemented(_)
            | ServeError::NotImplementedWithId(_, _) => RequestErrorCode::NotSupported as u64,
            ServeError::Internal(_) | ServeError::InternalWithId(_, _) => {
                RequestErrorCode::InternalError as u64
            }
        }
    }

    fn is_expected_serve_shutdown(err: &SessionError) -> bool {
        matches!(
            err,
            SessionError::Serve(ServeError::Cancel | ServeError::Done)
        )
    }
}

impl ObjectForwarder {
    async fn serve_subgroups(
        &mut self,
        subgroups: serve::SubgroupsReader,
        delivery_filter: DeliveryFilter,
        delivery_timeout: Option<std::time::Duration>,
    ) -> Result<(), SessionError> {
        let publisher = self.publisher.clone();
        let state = self.state.clone();
        let mlog = self.mlog.clone();
        let track_alias = self.track_alias;
        let terminal_state = self.state.clone();
        Self::forward_subgroups(subgroups, self.closed(), move || stored_terminal(&terminal_state),
            move |subgroup| {
            let header = data::SubgroupHeader {
                header_type: data::StreamHeaderType::SubgroupIdExt,
                track_alias,
                group_id: subgroup.group_id,
                subgroup_id: Some(subgroup.subgroup_id),
                publisher_priority: subgroup.priority,
            };
            Self::serve_subgroup(header, subgroup, publisher.clone(), state.clone(),
                mlog.clone(), delivery_filter, delivery_timeout)
        }).await
    }

    /// Drive the real subgroup reader and all forwarders together. A child
    /// failure cancels the other in-task futures before returning the error.
    /// Keep watching remote cancellation after the track producer has ended:
    /// outstanding subgroup writes can still fail during this drain.
    ///
    /// `stored_terminal` reads the forwarder's stored terminal (see
    /// [`stored_terminal`]); it decides whether a child `Done`/`Cancel` is the
    /// peer's track-level end seen from inside a child (the `closed` arm lost
    /// the poll-order race) or a genuine subgroup-internal failure.
    async fn forward_subgroups<F, Fut>(
        mut subgroups: serve::SubgroupsReader,
        closed: impl std::future::Future<Output = Result<(), ServeError>>,
        stored_terminal: impl Fn() -> Option<ServeError>,
        mut forward: F,
    ) -> Result<(), SessionError>
    where
        F: FnMut(serve::SubgroupReader) -> Fut,
        Fut: std::future::Future<Output = Result<(), SessionError>>,
    {
        let mut tasks: FuturesUnordered<Fut> = FuturesUnordered::new();
        let mut done = false;
        let mut close_seen = false;
        tokio::pin!(closed);
        loop {
            tokio::select! {
                // A stored remote error wins over final success.
                biased;
                result = &mut closed, if !close_seen => {
                    close_seen = true;
                    result?;
                    // A dropped watch peer is not proof that queued groups
                    // were forwarded. Drain the reader and child futures;
                    // their own outcomes decide completion.
                }
                result = tasks.next(), if !tasks.is_empty() => {
                    if let Some(result) = result {
                        // Done from inside a subgroup means its state vanished;
                        // Cancel from inside a subgroup is an aborted object.
                        // Ordinary end-of-subgroup and DELIVERY_TIMEOUT return
                        // Ok. Neither may be confused with the track-level
                        // Done/Cancel (watch gone / peer UNSUBSCRIBE) that the
                        // caller normalizes.
                        //
                        // Exception: the biased `closed` arm was already polled
                        // Pending in this iteration when another thread stored
                        // the forwarder terminal (UNSUBSCRIBE) and dropped the
                        // watch peer, and the child then observed the vanished
                        // state. The stored terminal is exactly what `closed`
                        // returns on the next poll, so surface it now instead
                        // of a synthetic internal error. Nothing stored means
                        // the child's Done/Cancel is its own (state truly
                        // vanished / object aborted) and stays a fault.
                        result.map_err(|error| match error {
                            SessionError::Serve(inner @ (ServeError::Done | ServeError::Cancel)) => {
                                if let Some(stored) = stored_terminal() {
                                    tracing::debug!(
                                        child = ?inner,
                                        stored = ?stored,
                                        "subgroup ended after the forwarder terminal was stored; surfacing the stored terminal"
                                    );
                                    return SessionError::Serve(stored);
                                }
                                match inner {
                                    ServeError::Done => SessionError::Serve(ServeError::internal_ctx(
                                        "subgroup state ended before forwarding completed")),
                                    _ => SessionError::Serve(ServeError::internal_ctx(
                                        "subgroup object aborted")),
                                }
                            }
                            error => error,
                        })?;
                    }
                }
                result = subgroups.next(), if !done => match result? {
                    Some(subgroup) => tasks.push(forward(subgroup)),
                    None => done = true,
                },
                _ = std::future::ready(()), if done && tasks.is_empty() => return Ok(()),
            }
        }
    }

    async fn serve_subgroup(
        header: data::SubgroupHeader,
        mut subgroup_reader: serve::SubgroupReader,
        mut publisher: Publisher,
        state: State<ObjectForwarderState>,
        mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
        delivery_filter: DeliveryFilter,
        delivery_timeout: Option<std::time::Duration>,
    ) -> Result<(), SessionError> {
        tracing::trace!(
            "[PUBLISHER] serve_subgroup: starting - group_id={}, subgroup_id={:?}, priority={}",
            subgroup_reader.group_id,
            subgroup_reader.subgroup_id,
            subgroup_reader.priority
        );

        let Some(first_object) =
            Self::next_allowed_object(&mut subgroup_reader, delivery_filter).await?
        else {
            return Ok(());
        };

        let first_deadline = delivery_timeout.map(|timeout| first_object.received_at + timeout);
        let mut send_stream = match first_deadline {
            Some(deadline) => match tokio::time::timeout_at(deadline, publisher.open_uni()).await {
                Ok(result) => result?,
                Err(_) => {
                    Self::record_delivery_timeout(
                        &state,
                        &mlog,
                        &header,
                        &first_object,
                        delivery_timeout.expect("deadline implies timeout"),
                        false,
                    );
                    return Ok(());
                }
            },
            None => publisher.open_uni().await?,
        };
        tracing::trace!("[PUBLISHER] serve_subgroup: opened unidirectional stream");

        state
            .lock_mut()
            .ok_or_else(|| terminal_or_done(&state))?
            .record_stream_opened();

        let mapping = publisher.data_priority_mapping();
        let quinn_priority = mapping.to_quinn(subgroup_reader.priority);
        tracing::trace!(
            publisher_priority = subgroup_reader.priority,
            quinn_priority,
            data_priority_mapping = mapping.as_str(),
            "mapped MoQT publisher priority to quinn send-stream priority"
        );
        send_stream.set_priority(quinn_priority);

        let mut output = SubgroupOutput::Stream(Writer::new(send_stream));
        Self::serve_subgroup_objects(
            header,
            subgroup_reader,
            first_object,
            &mut output,
            state,
            mlog,
            delivery_filter,
            delivery_timeout,
        )
        .await
    }

    async fn next_allowed_object(
        subgroup_reader: &mut serve::SubgroupReader,
        delivery_filter: DeliveryFilter,
    ) -> Result<Option<serve::SubgroupObjectReader>, ServeError> {
        while let Some(subgroup_object_reader) = subgroup_reader.next().await? {
            if delivery_filter.allows(subgroup_reader.group_id, subgroup_object_reader.object_id) {
                return Ok(Some(subgroup_object_reader));
            }

            tracing::trace!(
                "[PUBLISHER] serve_subgroup: filtered object group_id={}, object_id={}",
                subgroup_reader.group_id,
                subgroup_object_reader.object_id
            );
        }

        Ok(None)
    }

    fn record_delivery_timeout(
        state: &State<ObjectForwarderState>,
        mlog: &Option<Arc<Mutex<mlog::MlogWriter>>>,
        header: &data::SubgroupHeader,
        object: &serve::SubgroupObjectReader,
        timeout: std::time::Duration,
        reset: bool,
    ) {
        crate::object_trace::emit(
            crate::object_trace::Boundary::ForwardTimeout,
            &object.info, header.track_alias,
        );
        if let Some(mut locked) = state.lock_mut() {
            locked.record_delivery_timeout(reset);
        }

        let age_ms = tokio::time::Instant::now()
            .saturating_duration_since(object.received_at)
            .as_millis();
        tracing::info!(
            track_alias = header.track_alias,
            group_id = header.group_id,
            subgroup_id = object.subgroup_id,
            object_id = object.object_id,
            timeout_ms = timeout.as_millis(),
            age_ms,
            reset_code = DELIVERY_TIMEOUT_RESET_CODE,
            stream_reset = reset,
            "delivery timeout"
        );

        if let Some(mlog) = mlog {
            if let Ok(mut guard) = mlog.lock() {
                let event = mlog::loglevel_event(
                    guard.elapsed_ms(),
                    mlog::LogLevel::Info,
                    format!(
                        "delivery_timeout: track_alias={} group_id={} subgroup_id={} object_id={} timeout_ms={} age_ms={} reset_code={} stream_reset={}",
                        header.track_alias,
                        header.group_id,
                        object.subgroup_id,
                        object.object_id,
                        timeout.as_millis(),
                        age_ms,
                        DELIVERY_TIMEOUT_RESET_CODE,
                        reset,
                    ),
                );
                let _ = guard.add_event(event);
            }
        }
    }

    async fn serve_subgroup_objects(
        header: data::SubgroupHeader,
        mut subgroup_reader: serve::SubgroupReader,
        first_object: serve::SubgroupObjectReader,
        output: &mut SubgroupOutput,
        state: State<ObjectForwarderState>,
        mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
        delivery_filter: DeliveryFilter,
        delivery_timeout: Option<std::time::Duration>,
    ) -> Result<(), SessionError> {
        let mut object_count = 0;
        let mut next_object = Some(first_object);
        loop {
            let mut subgroup_object_reader = match next_object.take() {
                Some(reader) => reader,
                None => {
                    match Self::next_allowed_object(&mut subgroup_reader, delivery_filter).await? {
                        Some(reader) => reader,
                        None => break,
                    }
                }
            };

            let subgroup_object = data::SubgroupObjectExt {
                // TODO(itzmanish): compute real delta when the receive side uses object IDs
                // for ordering. Both sender and receiver must agree on the same prev tracking
                // semantics before this is meaningful.
                object_id_delta: 0,
                extension_headers: subgroup_object_reader.extension_headers.clone(), // Pass through extension headers
                payload_length: subgroup_object_reader.size,
                status: if subgroup_object_reader.size == 0 {
                    // Only set status if payload length is zero
                    Some(subgroup_object_reader.status)
                } else {
                    None
                },
            };

            tracing::trace!(
                "[PUBLISHER] serve_subgroup: sending object #{} - object_id={}, object_id_delta={}, payload_length={}, status={:?}, extension_headers={:?}",
                object_count + 1,
                subgroup_object_reader.object_id,
                subgroup_object.object_id_delta,
                subgroup_object.payload_length,
                subgroup_object.status,
                subgroup_object.extension_headers
            );

            let mut forward_trace = crate::object_trace::ForwardScope::new(
                &subgroup_object_reader.info, header.track_alias,
            );
            let deadline =
                delivery_timeout.map(|timeout| subgroup_object_reader.received_at + timeout);
            let send_object = async {
                crate::object_trace::emit(
                    crate::object_trace::Boundary::ForwardStart,
                    &subgroup_object_reader.info, header.track_alias,
                );
                if object_count == 0 {
                    tracing::trace!(
                        "[PUBLISHER] serve_subgroup: sending header - track_alias={}, group_id={}, subgroup_id={:?}, priority={}, header_type={:?}",
                        header.track_alias,
                        header.group_id,
                        header.subgroup_id,
                        header.publisher_priority,
                        header.header_type
                    );
                    output.encode(&header).await?;

                    if let Some(ref mlog) = mlog {
                        if let Ok(mut mlog_guard) = mlog.lock() {
                            let time = mlog_guard.elapsed_ms();
                            let stream_id = 0;
                            let event = mlog::subgroup_header_created(time, stream_id, &header);
                            let _ = mlog_guard.add_event(event);
                        }
                    }
                }

                output.encode(&subgroup_object).await?;

                if let Some(ref mlog) = mlog {
                    if let Ok(mut mlog_guard) = mlog.lock() {
                        let time = mlog_guard.elapsed_ms();
                        let stream_id = 0;
                        let event = mlog::subgroup_object_ext_created(
                            time,
                            stream_id,
                            subgroup_reader.group_id,
                            subgroup_reader.subgroup_id,
                            subgroup_object_reader.object_id,
                            &subgroup_object,
                        );
                        let _ = mlog_guard.add_event(event);
                    }
                }

                let mut chunks_sent = 0;
                let mut bytes_sent = 0;
                while let Some(chunk) = subgroup_object_reader.read().await? {
                    tracing::trace!(
                        "[PUBLISHER] serve_subgroup: sending payload chunk #{} for object #{} ({} bytes)",
                        chunks_sent + 1,
                        object_count + 1,
                        chunk.len()
                    );
                    bytes_sent += chunk.len();
                    output.write(&chunk).await?;
                    chunks_sent += 1;
                }

                Ok::<(usize, usize), SessionError>((bytes_sent, chunks_sent))
            };

            let sent = match delivery_timeout {
                Some(timeout) => {
                    match tokio::time::timeout_at(
                        deadline.expect("timeout implies deadline"),
                        send_object,
                    )
                    .await
                    {
                        Ok(result) => result?,
                        Err(_) => {
                            let reset = output.reset(DELIVERY_TIMEOUT_RESET_CODE);
                            Self::record_delivery_timeout(
                                &state,
                                &mlog,
                                &header,
                                &subgroup_object_reader,
                                timeout,
                                reset,
                            );
                            forward_trace.disarm();
                            // A timed-out subgroup is never reopened. A later
                            // frame can proceed only if it has its own subgroup.
                            return Ok(());
                        }
                    }
                }
                None => send_object.await?,
            };
            let (bytes_sent, chunks_sent) = sent;
            crate::object_trace::emit(
                crate::object_trace::Boundary::ForwardAccepted,
                &subgroup_object_reader.info, header.track_alias,
            );
            forward_trace.disarm();

            state
                .lock_mut()
                .ok_or_else(|| terminal_or_done(&state))?
                .update_largest_location(
                    subgroup_reader.group_id,
                    subgroup_object_reader.object_id,
                )?;

            // Instrumentation only (see crate::accept_trace). The payload write
            // loop above awaits QUIC flow control, so this is the last point in
            // this crate that is still subject to send backpressure.
            //
            // This is a transport-accept time, NOT an on-the-wire time: the
            // QUIC stack has accepted the bytes, it has not necessarily sent
            // them and has certainly not had them acknowledged. Under
            // congestion, flow-control credit runs out and this tracks
            // backpressure well; outside congestion it is close to a pure
            // application handoff time.
            //
            // Must not block: when no observer is installed this is one
            // OnceLock load.
            if let Some(obs) = crate::accept_trace::observer() {
                obs.on_object_accepted(&crate::accept_trace::ObjectAccepted {
                    track_name: &subgroup_reader.name.to_string_lossy(),
                    track_alias: header.track_alias,
                    group_id: subgroup_reader.group_id,
                    subgroup_id: subgroup_reader.subgroup_id,
                    object_id: subgroup_object_reader.object_id,
                    payload_bytes: bytes_sent,
                });
            }

            tracing::trace!(
                "[PUBLISHER] serve_subgroup: completed object #{} ({} chunks, {} bytes total)",
                object_count + 1,
                chunks_sent,
                bytes_sent
            );
            object_count += 1;
        }

        tracing::trace!(
            "[PUBLISHER] serve_subgroup: completed subgroup (group_id={}, subgroup_id={:?}, {} objects sent)",
            subgroup_reader.group_id,
            subgroup_reader.subgroup_id,
            object_count
        );

        Ok(())
    }

    #[cfg(test)]
    async fn serve_subgroup_to_buffer(
        header: data::SubgroupHeader,
        mut subgroup_reader: serve::SubgroupReader,
        state: State<ObjectForwarderState>,
        delivery_filter: DeliveryFilter,
        delivery_timeout: Option<std::time::Duration>,
    ) -> Result<bytes::BytesMut, SessionError> {
        let Some(first_object) =
            Self::next_allowed_object(&mut subgroup_reader, delivery_filter).await?
        else {
            return Ok(bytes::BytesMut::new());
        };

        state
            .lock_mut()
            .ok_or_else(|| terminal_or_done(&state))?
            .record_stream_opened();

        let mut output = SubgroupOutput::Buffer(bytes::BytesMut::new());
        Self::serve_subgroup_objects(
            header,
            subgroup_reader,
            first_object,
            &mut output,
            state,
            None,
            delivery_filter,
            delivery_timeout,
        )
        .await?;

        Ok(output.into_buffer())
    }

    async fn serve_datagrams(
        &mut self,
        mut datagrams: serve::DatagramsReader,
        delivery_filter: DeliveryFilter,
    ) -> Result<(), SessionError> {
        tracing::debug!("[PUBLISHER] serve_datagrams: starting");

        let mut datagram_count = 0;
        while let Some(datagram) = datagrams.read().await? {
            if !delivery_filter.allows(datagram.group_id, datagram.object_id) {
                tracing::trace!(
                    "[PUBLISHER] serve_datagrams: filtered datagram group_id={}, object_id={}",
                    datagram.group_id,
                    datagram.object_id
                );
                continue;
            }

            // Determine datagram type based on extension headers presence
            let has_extension_headers = !datagram.extension_headers.is_empty();
            let datagram_type = if has_extension_headers {
                data::DatagramType::ObjectIdPayloadExt
            } else {
                data::DatagramType::ObjectIdPayload
            };

            let encoded_datagram = data::Datagram {
                datagram_type,
                track_alias: self.track_alias,
                group_id: datagram.group_id,
                object_id: Some(datagram.object_id),
                publisher_priority: datagram.priority,
                extension_headers: if has_extension_headers {
                    Some(datagram.extension_headers.clone())
                } else {
                    None
                },
                status: None,
                payload: Some(datagram.payload),
            };

            let payload_len = encoded_datagram
                .payload
                .as_ref()
                .map(|p| p.len())
                .unwrap_or(0);
            let mut buffer = bytes::BytesMut::with_capacity(payload_len + 100);
            encoded_datagram.encode(&mut buffer)?;

            tracing::trace!(
                "[PUBLISHER] serve_datagrams: sending datagram #{} - track_alias={}, group_id={}, object_id={}, priority={}, payload_len={}, extension_headers={:?}, total_encoded_len={}",
                datagram_count + 1,
                encoded_datagram.track_alias,
                encoded_datagram.group_id,
                encoded_datagram.object_id.unwrap(),
                encoded_datagram.publisher_priority,
                payload_len,
                encoded_datagram.extension_headers,
                buffer.len()
            );

            // Create mlog event for datagram created
            if let Some(ref mlog) = self.mlog {
                if let Ok(mut mlog_guard) = mlog.lock() {
                    let time = mlog_guard.elapsed_ms();
                    let stream_id = 0; // TODO: Placeholder, need actual QUIC stream ID
                    let _ = mlog_guard.add_event(mlog::object_datagram_created(
                        time,
                        stream_id,
                        &encoded_datagram,
                    ));
                }
            }

            self.publisher.send_datagram(buffer.into()).await?;

            self.state
                .lock_mut()
                .ok_or_else(|| terminal_or_done(&self.state))?
                .update_largest_location(
                    encoded_datagram.group_id,
                    encoded_datagram.object_id.unwrap(),
                )?;

            datagram_count += 1;
        }

        tracing::trace!(
            "[PUBLISHER] serve_datagrams: completed ({} datagrams sent)",
            datagram_count
        );

        Ok(())
    }
}

pub(super) struct ObjectForwarderRecv {
    state: State<ObjectForwarderState>,
}

impl ObjectForwarderRecv {
    pub fn recv_unsubscribe(&mut self) -> Result<(), ServeError> {
        let state = self.state.lock();
        state.closed.clone()?;

        if let Some(mut state) = state.into_mut() {
            state.unsubscribed = true;
            state.closed = Err(ServeError::Cancel);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribed_state_counts_opened_streams() {
        let mut state = ObjectForwarderState::default();
        assert_eq!(state.stream_count, 0);

        state.record_stream_opened();
        assert_eq!(state.stream_count, 1);

        state.record_stream_opened();
        assert_eq!(state.stream_count, 2);
    }

    #[test]
    fn recv_unsubscribe_marks_unsubscribed_and_closes() {
        let state = State::<ObjectForwarderState>::default();
        let (_send, recv_state) = state.split();
        let mut recv = ObjectForwarderRecv { state: recv_state };

        assert!(!recv.state.lock().unsubscribed);

        recv.recv_unsubscribe().unwrap();

        let locked = recv.state.lock();
        assert!(locked.unsubscribed);
        assert!(matches!(locked.closed, Err(ServeError::Cancel)));
    }

    #[tokio::test]
    async fn subgroup_failure_after_track_end_reaches_parent_and_cancels_siblings() {
        use crate::coding::TrackNamespace;
        use bytes::Bytes;
        for cause in [ServeError::Cancel, ServeError::Size, ServeError::Done] {
            let (writer, reader) = serve::Track::new(
                TrackNamespace::from_utf8_path("test"), "video").produce();
            let mut groups = writer.subgroups().unwrap();
            let mut failed = groups.append(1).unwrap();
            let mut object = failed.create(5, None).unwrap();
            object.write(Bytes::from_static(b"hi")).unwrap();
            let mut pending = groups.append(1).unwrap();
            let pending_object = pending.create(5, None).unwrap();
            drop(failed);
            drop(pending);
            drop(groups); // producer FIN before the residual object fails
            let TrackReaderMode::Subgroups(subgroups) = reader.mode().await.unwrap() else {
                panic!("subgroups expected");
            };
            let state = State::<ObjectForwarderState>::default();
            let forward = ObjectForwarder::forward_subgroups(
                subgroups, std::future::ready(Ok(())), || None, |subgroup| {
                    let header = data::SubgroupHeader {
                        header_type: data::StreamHeaderType::SubgroupIdExt,
                        track_alias: 42, group_id: subgroup.group_id,
                        subgroup_id: Some(subgroup.subgroup_id),
                        publisher_priority: subgroup.priority,
                    };
                    let state = state.clone();
                    async move {
                        ObjectForwarder::serve_subgroup_to_buffer(header, subgroup, state,
                            DeliveryFilter { forward: true, start_location: None, end_group_id: None },
                            None).await.map(|_| ())
                    }
                });
            tokio::pin!(forward);
            // Poll the actual reader/writer path until blocked on payload.
            assert!(futures::poll!(&mut forward).is_pending());
            let aborted_with_cancel = cause == ServeError::Cancel;
            object.abort(cause).unwrap();
            let result = tokio::time::timeout(std::time::Duration::from_secs(1), forward)
                .await.expect("failure must not wait for the unfinished sibling");
            assert!(result.is_err(), "residual object error was swallowed");
            assert!(!matches!(result, Err(SessionError::Serve(ServeError::Done))));
            // An aborted object must never look like a peer UNSUBSCRIBE.
            assert!(!matches!(result, Err(SessionError::Serve(ServeError::Cancel))));
            if aborted_with_cancel {
                // `internal_ctx` keeps the context in the log only; the typed
                // variant is what separates it from a track-level Cancel.
                assert!(
                    matches!(&result, Err(SessionError::Serve(
                        ServeError::InternalWithId(_, _) | ServeError::Internal(_)))),
                    "unexpected mapping for subgroup-internal Cancel: {result:?}"
                );
            }
            drop(pending_object);
        }
    }

    #[tokio::test]
    async fn parent_forwarder_accepts_normal_end_and_delivery_timeout_drop() {
        use crate::coding::TrackNamespace;
        for timeout_drop in [false, true] {
            let (writer, reader) = serve::Track::new(
                TrackNamespace::from_utf8_path("test"), "video").produce();
            let mut groups = writer.subgroups().unwrap();
            let mut group = groups.append(1).unwrap();
            let held_object = if timeout_drop {
                Some(group.create(5, None).unwrap())
            } else {
                group.write(bytes::Bytes::from_static(b"hello")).unwrap();
                None
            };
            drop(group);
            drop(groups);
            let TrackReaderMode::Subgroups(subgroups) = reader.mode().await.unwrap() else {
                panic!("subgroups expected");
            };
            let state = State::<ObjectForwarderState>::default();
            let result = ObjectForwarder::forward_subgroups(subgroups,
                std::future::pending(), || None, |subgroup| {
                    let header = data::SubgroupHeader {
                        header_type: data::StreamHeaderType::SubgroupIdExt,
                        track_alias: 42, group_id: subgroup.group_id,
                        subgroup_id: Some(subgroup.subgroup_id), publisher_priority: subgroup.priority,
                    };
                    let state = state.clone();
                    async move {
                        ObjectForwarder::serve_subgroup_to_buffer(header, subgroup, state,
                            DeliveryFilter { forward: true, start_location: None, end_group_id: None },
                            Some(std::time::Duration::from_millis(1))).await.map(|_| ())
                    }
                });
            tokio::time::timeout(std::time::Duration::from_secs(1), result).await.unwrap().unwrap();
            assert_eq!(state.lock().delivery_timeouts, u64::from(timeout_drop));
            drop(held_object);
        }
    }

    #[tokio::test]
    async fn remote_cancel_during_subgroup_drain_is_not_hidden_by_track_fin() {
        use crate::coding::TrackNamespace;
        let (writer, reader) = serve::Track::new(
            TrackNamespace::from_utf8_path("test"), "video").produce();
        let mut groups = writer.subgroups().unwrap();
        let _group = groups.append(1).unwrap();
        drop(groups);
        let TrackReaderMode::Subgroups(subgroups) = reader.mode().await.unwrap() else {
            panic!("subgroups expected");
        };
        let (tx, rx) = tokio::sync::oneshot::channel();
        let forward = ObjectForwarder::forward_subgroups(subgroups,
            async { rx.await.unwrap() }, || None, |_| std::future::pending());
        tokio::pin!(forward);
        assert!(futures::poll!(&mut forward).is_pending());
        tx.send(Err(ServeError::Cancel)).unwrap();
        assert!(matches!(forward.await, Err(SessionError::Serve(ServeError::Cancel))));
    }

    #[tokio::test]
    async fn object_forwarder_forwards_subgroup_object_to_output() {
        use bytes::{Buf, Bytes};

        use crate::{coding::Decode, coding::TrackNamespace};

        let (track_writer, track_reader) =
            serve::Track::new(TrackNamespace::from_utf8_path("test"), "video").produce();
        let mut subgroups_writer = track_writer.subgroups().unwrap();
        let mut subgroup_writer = subgroups_writer
            .create(serve::Subgroup {
                group_id: 7,
                subgroup_id: 2,
                priority: 9,
            })
            .unwrap();
        subgroup_writer.write(Bytes::from_static(b"hello")).unwrap();
        drop(subgroup_writer);
        drop(subgroups_writer);

        let mut subgroups = match track_reader.mode().await.unwrap() {
            TrackReaderMode::Subgroups(subgroups) => subgroups,
            _ => panic!("expected subgroups mode"),
        };
        let subgroup = subgroups
            .next()
            .await
            .unwrap()
            .expect("subgroup should be available");
        let state = State::<ObjectForwarderState>::default();
        let header = data::SubgroupHeader {
            header_type: data::StreamHeaderType::SubgroupIdExt,
            track_alias: 42,
            group_id: subgroup.group_id,
            subgroup_id: Some(subgroup.subgroup_id),
            publisher_priority: subgroup.priority,
        };

        let output = ObjectForwarder::serve_subgroup_to_buffer(
            header.clone(),
            subgroup,
            state.clone(),
            DeliveryFilter {
                forward: true,
                start_location: None,
                end_group_id: None,
            },
            None,
        )
        .await
        .unwrap();

        assert_eq!(state.lock().stream_count, 1);

        let mut output = output.freeze();
        let header_type = data::StreamHeaderType::decode(&mut output).unwrap();
        let decoded_header = data::SubgroupHeader::decode(header_type, &mut output).unwrap();
        assert_eq!(decoded_header, header);

        let object = data::SubgroupObjectExt::decode(&mut output).unwrap();
        assert_eq!(object.object_id_delta, 0);
        assert!(object.extension_headers.is_empty());
        assert_eq!(object.payload_length, 5);
        assert_eq!(object.status, None);

        let payload = output.copy_to_bytes(object.payload_length);
        assert_eq!(&payload[..], b"hello");
        assert!(!output.has_remaining());
    }

    #[tokio::test]
    async fn delivery_timeout_stops_one_subgroup_without_reopening_it() {
        use crate::coding::TrackNamespace;

        let (track_writer, track_reader) =
            serve::Track::new(TrackNamespace::from_utf8_path("test"), "video").produce();
        let mut subgroups_writer = track_writer.subgroups().unwrap();
        let mut subgroup_writer = subgroups_writer
            .create(serve::Subgroup {
                group_id: 11,
                subgroup_id: 0,
                priority: 1,
            })
            .unwrap();
        // Keep an incomplete object open. The forwarder can encode its header
        // but blocks waiting for payload until the hop-local timeout fires.
        let _object_writer = subgroup_writer.create(5, None).unwrap();

        let mut subgroups = match track_reader.mode().await.unwrap() {
            TrackReaderMode::Subgroups(subgroups) => subgroups,
            _ => panic!("expected subgroups mode"),
        };
        let subgroup = subgroups
            .next()
            .await
            .unwrap()
            .expect("subgroup should be available");
        let state = State::<ObjectForwarderState>::default();
        let header = data::SubgroupHeader {
            header_type: data::StreamHeaderType::SubgroupIdExt,
            track_alias: 42,
            group_id: subgroup.group_id,
            subgroup_id: Some(subgroup.subgroup_id),
            publisher_priority: subgroup.priority,
        };

        let _ = ObjectForwarder::serve_subgroup_to_buffer(
            header,
            subgroup,
            state.clone(),
            DeliveryFilter {
                forward: true,
                start_location: None,
                end_group_id: None,
            },
            Some(std::time::Duration::from_millis(1)),
        )
        .await
        .unwrap();

        let state = state.lock();
        assert_eq!(state.delivery_timeouts, 1);
        // Buffer output has no QUIC stream to reset. Production stream output
        // records this as a reset and calls Writer::reset(0x2).
        assert_eq!(state.delivery_timeout_resets, 0);
        assert_eq!(state.stream_count, 1);
    }

    #[test]
    fn publish_done_code_maps_done_to_track_ended() {
        assert_eq!(
            Subscribed::publish_done_code(&ServeError::Done),
            message::PublishDoneCode::TrackEnded as u64
        );
    }

    #[test]
    fn publish_done_code_passes_through_closed_code() {
        assert_eq!(
            Subscribed::publish_done_code(&ServeError::Closed(0x12)),
            0x12
        );
    }

    #[test]
    fn publish_done_code_maps_other_errors_to_internal() {
        assert_eq!(
            Subscribed::publish_done_code(&ServeError::internal_ctx("test")),
            message::PublishDoneCode::InternalError as u64
        );
    }

    #[test]
    fn request_error_code_maps_rejection_reasons() {
        assert_eq!(
            Subscribed::request_error_code(&ServeError::NotFound),
            RequestErrorCode::DoesNotExist as u64
        );
        assert_eq!(
            Subscribed::request_error_code(&ServeError::Duplicate),
            RequestErrorCode::DuplicateSubscription as u64
        );
        assert_eq!(
            Subscribed::request_error_code(&ServeError::NotImplemented("fetch".to_string())),
            RequestErrorCode::NotSupported as u64
        );
        assert_eq!(
            Subscribed::request_error_code(&ServeError::Cancel),
            RequestErrorCode::Uninterested as u64
        );
        assert_eq!(
            Subscribed::request_error_code(&ServeError::Closed(0x42)),
            0x42
        );
    }

    #[test]
    fn expected_serve_shutdown_is_only_cancel_or_done() {
        assert!(Subscribed::is_expected_serve_shutdown(
            &SessionError::Serve(ServeError::Cancel)
        ));
        assert!(Subscribed::is_expected_serve_shutdown(
            &SessionError::Serve(ServeError::Done)
        ));
        assert!(!Subscribed::is_expected_serve_shutdown(
            &SessionError::Serve(ServeError::NotFound)
        ));
        assert!(!Subscribed::is_expected_serve_shutdown(
            &SessionError::Internal
        ));
    }
    #[tokio::test]
    async fn first_serve_error_is_preserved_over_a_racing_unsubscribe_cancel() {
        use crate::coding::TrackNamespace;
        use bytes::Bytes;
        // Real reader/forwarder path: a payload error (object aborted with
        // Size after producer FIN) ends forwarding first.
        let (writer, reader) = serve::Track::new(
            TrackNamespace::from_utf8_path("test"), "video").produce();
        let mut groups = writer.subgroups().unwrap();
        let mut group = groups.append(1).unwrap();
        let mut object = group.create(5, None).unwrap();
        object.write(Bytes::from_static(b"hi")).unwrap();
        drop(group);
        drop(groups);
        let TrackReaderMode::Subgroups(subgroups) = reader.mode().await.unwrap() else {
            panic!("subgroups expected");
        };
        let (send_state, recv_state) = State::<ObjectForwarderState>::default().split();
        let mut recv = ObjectForwarderRecv { state: recv_state };
        let forward = ObjectForwarder::forward_subgroups(
            subgroups, std::future::pending(), || None, |subgroup| {
                let header = data::SubgroupHeader {
                    header_type: data::StreamHeaderType::SubgroupIdExt,
                    track_alias: 42, group_id: subgroup.group_id,
                    subgroup_id: Some(subgroup.subgroup_id),
                    publisher_priority: subgroup.priority,
                };
                let state = send_state.clone();
                async move {
                    ObjectForwarder::serve_subgroup_to_buffer(header, subgroup, state,
                        DeliveryFilter { forward: true, start_location: None, end_group_id: None },
                        None).await.map(|_| ())
                }
            });
        tokio::pin!(forward);
        assert!(futures::poll!(&mut forward).is_pending());
        object.abort(ServeError::Size).unwrap();
        let res = tokio::time::timeout(std::time::Duration::from_secs(1), forward)
            .await.expect("payload error must end forwarding");
        assert!(matches!(res, Err(SessionError::Serve(ServeError::Size))), "{res:?}");
        // UNSUBSCRIBE lands before the cleanup close: the stored Cancel must
        // not replace the payload error.
        recv.recv_unsubscribe().unwrap();
        let finished = finish_serve(&send_state, res);
        assert!(matches!(finished, Err(SessionError::Serve(ServeError::Size))), "{finished:?}");
        assert!(matches!(send_state.lock().closed, Err(ServeError::Cancel)));
        assert!(send_state.lock().unsubscribed);
    }

    #[test]
    fn finish_serve_stores_the_error_when_nothing_is_stored_and_keeps_it_when_the_watch_is_gone() {
        // Nothing stored: the error is stored and returned.
        let (send_state, recv_state) = State::<ObjectForwarderState>::default().split();
        let res = finish_serve(&send_state, Err(SessionError::Serve(ServeError::Size)));
        assert!(matches!(res, Err(SessionError::Serve(ServeError::Size))));
        assert!(matches!(send_state.lock().closed, Err(ServeError::Size)));
        // Success passes through untouched.
        assert!(finish_serve(&send_state, Ok(())).is_ok());
        drop(recv_state);
        // Watch peer gone (`close` would fail with Done): the first error is
        // still the one returned.
        let (send_state, recv_state) = State::<ObjectForwarderState>::default().split();
        drop(recv_state);
        let res = finish_serve(&send_state, Err(SessionError::Internal));
        assert!(matches!(res, Err(SessionError::Internal)), "{res:?}");
    }

    /// Helper for the race tests: one subgroup with a single complete object,
    /// producer FIN, returned in subgroups mode.
    async fn one_object_track() -> serve::SubgroupsReader {
        use crate::coding::TrackNamespace;
        let (writer, reader) = serve::Track::new(
            TrackNamespace::from_utf8_path("test"), "video").produce();
        let mut groups = writer.subgroups().unwrap();
        let mut group = groups.append(1).unwrap();
        group.write(bytes::Bytes::from_static(b"hello")).unwrap();
        drop(group);
        drop(groups);
        match reader.mode().await.unwrap() {
            TrackReaderMode::Subgroups(subgroups) => subgroups,
            _ => panic!("subgroups expected"),
        }
    }

    fn buffer_child(
        state: &State<ObjectForwarderState>,
    ) -> impl FnMut(serve::SubgroupReader) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), SessionError>> + Send>> + '_ {
        move |subgroup| {
            let header = data::SubgroupHeader {
                header_type: data::StreamHeaderType::SubgroupIdExt,
                track_alias: 42, group_id: subgroup.group_id,
                subgroup_id: Some(subgroup.subgroup_id),
                publisher_priority: subgroup.priority,
            };
            let state = state.clone();
            Box::pin(async move {
                ObjectForwarder::serve_subgroup_to_buffer(header, subgroup, state,
                    DeliveryFilter { forward: true, start_location: None, end_group_id: None },
                    None).await.map(|_| ())
            })
        }
    }

    #[tokio::test]
    async fn unsubscribe_then_dropped_recv_half_is_seen_by_a_child_as_track_level_cancel() {
        // Stage-9 race: UNSUBSCRIBE handling stores Cancel + unsubscribed and
        // drops the recv half BEFORE the child's record_stream_opened path
        // runs (its lock_mut() returns None). The child must resolve to the
        // peer's stored terminal, not to Done / the internal error.
        let mut subgroups = one_object_track().await;
        let subgroup = subgroups.next().await.unwrap().expect("subgroup");
        let (send_state, recv_state) = State::<ObjectForwarderState>::default().split();
        let mut recv = ObjectForwarderRecv { state: recv_state };
        recv.recv_unsubscribe().unwrap();
        drop(recv);
        assert!(send_state.lock_mut().is_none(), "recv half drop must vanish the state");
        assert!(send_state.lock().unsubscribed);
        assert!(matches!(stored_terminal(&send_state), Some(ServeError::Cancel)));
        assert!(matches!(terminal_or_done(&send_state), ServeError::Cancel));

        let header = data::SubgroupHeader {
            header_type: data::StreamHeaderType::SubgroupIdExt,
            track_alias: 42, group_id: subgroup.group_id,
            subgroup_id: Some(subgroup.subgroup_id), publisher_priority: subgroup.priority,
        };
        let res = ObjectForwarder::serve_subgroup_to_buffer(header, subgroup, send_state.clone(),
            DeliveryFilter { forward: true, start_location: None, end_group_id: None }, None).await;
        assert!(matches!(res, Err(SessionError::Serve(ServeError::Cancel))), "{res:?}");
        assert_eq!(send_state.lock().stream_count, 0);

        // Same race through the parent with the `closed` arm still Pending
        // (it lost the poll order): the run must end RemoteClosed (Cancel).
        let subgroups = one_object_track().await;
        let terminal_state = send_state.clone();
        let res = tokio::time::timeout(std::time::Duration::from_secs(1),
            ObjectForwarder::forward_subgroups(subgroups, std::future::pending(),
                move || stored_terminal(&terminal_state), buffer_child(&send_state)))
            .await.expect("must not hang");
        assert!(matches!(res, Err(SessionError::Serve(ServeError::Cancel))), "{res:?}");
    }

    #[tokio::test]
    async fn pure_watch_loss_with_nothing_stored_stays_done_for_a_child() {
        // Recv half dropped without UNSUBSCRIBE: nothing stored, Done as before.
        let mut subgroups = one_object_track().await;
        let subgroup = subgroups.next().await.unwrap().expect("subgroup");
        let (send_state, recv_state) = State::<ObjectForwarderState>::default().split();
        drop(recv_state);
        assert!(stored_terminal(&send_state).is_none());
        assert!(matches!(terminal_or_done(&send_state), ServeError::Done));
        let header = data::SubgroupHeader {
            header_type: data::StreamHeaderType::SubgroupIdExt,
            track_alias: 42, group_id: subgroup.group_id,
            subgroup_id: Some(subgroup.subgroup_id), publisher_priority: subgroup.priority,
        };
        let res = ObjectForwarder::serve_subgroup_to_buffer(header, subgroup, send_state,
            DeliveryFilter { forward: true, start_location: None, end_group_id: None }, None).await;
        assert!(matches!(res, Err(SessionError::Serve(ServeError::Done))), "{res:?}");
    }

    #[tokio::test]
    async fn child_done_with_stored_unsubscribe_cancel_is_track_level_cancel() {
        let subgroups = one_object_track().await;
        let (send_state, recv_state) = State::<ObjectForwarderState>::default().split();
        let mut recv = ObjectForwarderRecv { state: recv_state };
        recv.recv_unsubscribe().unwrap();
        assert!(send_state.lock().unsubscribed);
        let res = ObjectForwarder::forward_subgroups(subgroups, std::future::pending(),
            || stored_terminal(&send_state),
            |_| std::future::ready(Err(SessionError::Serve(ServeError::Done)))).await;
        assert!(matches!(res, Err(SessionError::Serve(ServeError::Cancel))), "{res:?}");
        // Any other stored terminal is surfaced as-is.
        let subgroups = one_object_track().await;
        let res = ObjectForwarder::forward_subgroups(subgroups, std::future::pending(),
            || Some(ServeError::Closed(0x12)),
            |_| std::future::ready(Err(SessionError::Serve(ServeError::Cancel)))).await;
        assert!(matches!(res, Err(SessionError::Serve(ServeError::Closed(0x12)))), "{res:?}");
    }

    #[tokio::test]
    async fn child_done_with_nothing_stored_is_still_the_internal_error() {
        let subgroups = one_object_track().await;
        let res = ObjectForwarder::forward_subgroups(subgroups, std::future::pending(), || None,
            |_| std::future::ready(Err(SessionError::Serve(ServeError::Done)))).await;
        // `internal_ctx` keeps "subgroup state ended before forwarding
        // completed" in the log only; the typed variant is the contract.
        assert!(
            matches!(&res, Err(SessionError::Serve(
                ServeError::InternalWithId(_, _) | ServeError::Internal(_)))),
            "unexpected mapping for child Done: {res:?}"
        );
    }

    #[tokio::test]
    async fn child_cancel_with_nothing_stored_is_still_the_object_abort_error() {
        let subgroups = one_object_track().await;
        let res = ObjectForwarder::forward_subgroups(subgroups, std::future::pending(), || None,
            |_| std::future::ready(Err(SessionError::Serve(ServeError::Cancel)))).await;
        // `internal_ctx` keeps "subgroup object aborted" in the log only; the
        // typed variant is what separates it from a track-level Cancel.
        assert!(
            matches!(&res, Err(SessionError::Serve(
                ServeError::InternalWithId(_, _) | ServeError::Internal(_)))),
            "unexpected mapping for child Cancel: {res:?}"
        );
        // Other child errors pass through untouched even with a stored terminal.
        let subgroups = one_object_track().await;
        let res = ObjectForwarder::forward_subgroups(subgroups, std::future::pending(),
            || Some(ServeError::Cancel),
            |_| std::future::ready(Err(SessionError::Serve(ServeError::Size)))).await;
        assert!(matches!(res, Err(SessionError::Serve(ServeError::Size))), "{res:?}");
    }
}
