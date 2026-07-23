// SPDX-License-Identifier: MIT OR Apache-2.0
//
//! Optional, opt-in instrumentation hook for "object accepted by the QUIC stack".
//!
//! Motivation (Skew research project, TODO item A2): an application that writes
//! into a [`crate::serve::SubgroupWriter`] only learns when the object was
//! *appended to the in-process serve buffer*, because `SubgroupWriter::write`
//! is synchronous. Everything after that — serve-buffer residency, the
//! per-subgroup forwarding task, QUIC stream/connection flow control, pacing —
//! is invisible to the application and therefore gets attributed to "network".
//!
//! This module lets an application install a process-global observer that is
//! invoked immediately after the payload write loop for one object has
//! completed in `ObjectForwarder::serve_subgroup_objects`. That loop awaits
//! `web_transport::SendStream::write_buf`, so its completion is the last point
//! inside this crate that is still subject to QUIC send backpressure.
//!
//! # What this is, and what it is not
//!
//! This is a **transport-accept time, not an on-the-wire time.** The callback
//! fires when the QUIC stack has *accepted* the last payload byte of the
//! object into its send path. It does not mean the bytes were placed on the
//! wire, and it certainly does not mean they were acknowledged.
//!
//! Acceptance is bounded by stream and connection flow-control credit.
//! Therefore:
//!
//! * **Under congestion** the sender runs out of credit and the write loop
//!   parks, so the accept time tracks send backpressure closely. This is the
//!   regime the instrumentation exists for.
//! * **Outside congestion** credit is plentiful and the write loop returns
//!   almost immediately, so the accept time is close to a pure application
//!   handoff time and carries little information.
//!
//! Do not present an accept timestamp as evidence of transmission or delivery.
//!
//! # Other constraints
//!
//! * No timestamp is produced here on purpose. The observer takes its own
//!   timestamp so that the value shares an epoch with the application clock and
//!   no anchor correction is needed at analysis time.
//! * The observer runs **on the forwarding task**. It must be non-blocking:
//!   no locks held across I/O, no channel that can block. Cost when no observer
//!   is installed is a single `OnceLock` load.
//!
//! This is instrumentation only; it must not change transmission behaviour.

use std::sync::{Arc, OnceLock};

/// One object's payload has been fully accepted by the QUIC stack.
///
/// This is an accept event, not a transmission event; see the module docs.
///
/// `track_name` borrows from the subgroup info and is only valid for the
/// duration of the call; an observer that needs to keep it must copy it.
#[derive(Debug)]
#[non_exhaustive]
pub struct ObjectAccepted<'a> {
    /// Track name as published (e.g. `pc`, `haptic`). Lossy UTF-8.
    pub track_name: &'a str,
    /// Track alias negotiated for this subscription.
    pub track_alias: u64,
    pub group_id: u64,
    pub subgroup_id: u64,
    pub object_id: u64,
    /// Payload bytes written for this object (excludes the object header).
    pub payload_bytes: usize,
}

impl<'a> ObjectAccepted<'a> {
    /// Construct an event. The struct is `#[non_exhaustive]` so that adding a
    /// field later is not a breaking change; this constructor exists so that an
    /// embedder's tests can exercise their observer without a live session.
    pub fn new(
        track_name: &'a str,
        track_alias: u64,
        group_id: u64,
        subgroup_id: u64,
        object_id: u64,
        payload_bytes: usize,
    ) -> Self {
        Self { track_name, track_alias, group_id, subgroup_id, object_id, payload_bytes }
    }
}

/// Receives [`ObjectAccepted`] notifications from the publisher forwarding task.
///
/// Implementations MUST NOT block, allocate unboundedly, or perform I/O.
pub trait AcceptObserver: Send + Sync + 'static {
    fn on_object_accepted(&self, ev: &ObjectAccepted<'_>);
}

static OBSERVER: OnceLock<Arc<dyn AcceptObserver>> = OnceLock::new();

/// Install the process-global observer. Returns `Err` if one is already set.
pub fn set_observer(observer: Arc<dyn AcceptObserver>) -> Result<(), Arc<dyn AcceptObserver>> {
    OBSERVER.set(observer)
}

/// The installed observer, if any. Cheap enough to call per object.
#[inline]
pub fn observer() -> Option<&'static Arc<dyn AcceptObserver>> {
    OBSERVER.get()
}
