// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::hash_map;
use std::collections::HashMap;
use std::ops;
use std::sync::{Arc, Mutex};

use moq_transport::{
    coding::TrackNamespace,
    serve::{FullTrackName, ServeError, Track, TrackReader, TrackWriter},
};
use tokio::sync::{mpsc, watch};

use crate::metrics::GaugeGuard;

/// Scope key for the outer level of the two-level registry.
///
/// An empty string (`""`) represents the global/unscoped bucket. All unscoped
/// connections share this bucket — any publisher without a scope can be reached
/// by any subscriber without a scope. This is the default behavior for backward
/// compatibility with pre-scope deployments.
type ScopeKey = String;

/// The scope key used for unscoped (global) registrations.
const UNSCOPED: &str = "";

const NAMESPACE_REQUEST_CHANNEL_CAPACITY: usize = 1024;

#[derive(Clone)]
struct NamespaceSource {
    requests: mpsc::Sender<NamespaceTrackRequest>,
}

enum TrackEntry {
    Published(TrackReader),
    Namespace {
        reader: TrackReader,
        leases: usize,
        cancel: watch::Sender<bool>,
        identity: Arc<()>,
    },
}

impl TrackEntry {
    fn reader(&self) -> &TrackReader {
        match self {
            Self::Published(reader) | Self::Namespace { reader, .. } => reader,
        }
    }

    fn cancel_namespace(self) {
        if let Self::Namespace { cancel, .. } = self {
            cancel.send_replace(true);
        }
    }
}

pub(crate) struct NamespaceTrackRequest {
    pub writer: TrackWriter,
    pub cancelled: watch::Receiver<bool>,
}

impl ops::Deref for NamespaceTrackRequest {
    type Target = TrackWriter;

    fn deref(&self) -> &Self::Target {
        &self.writer
    }
}

pub(crate) struct LocalTrack {
    pub reader: TrackReader,
    _lease: Option<LocalTrackLease>,
}

impl ops::Deref for LocalTrack {
    type Target = TrackReader;

    fn deref(&self) -> &Self::Target {
        &self.reader
    }
}

struct LocalTrackLease {
    locals: Locals,
    scope_key: ScopeKey,
    full_name: FullTrackName,
    identity: Arc<()>,
}

/// Relay-local registry.
///
/// Actual media tracks are always stored by exact Full Track Name. Namespace
/// entries are only routing metadata from PUBLISH_NAMESPACE: they tell the
/// relay which upstream publisher can be asked for a missing track.
#[derive(Clone)]
pub struct Locals {
    /// Actual media tracks, indexed by (scope, full track name).
    tracks: Arc<Mutex<HashMap<ScopeKey, HashMap<FullTrackName, TrackEntry>>>>,

    /// Namespace route sources from PUBLISH_NAMESPACE, indexed by (scope,
    /// namespace) and matched by prefix.
    namespaces: Arc<Mutex<HashMap<ScopeKey, HashMap<TrackNamespace, NamespaceSource>>>>,
}

impl Default for Locals {
    fn default() -> Self {
        Self::new()
    }
}

impl Locals {
    pub fn new() -> Self {
        Self {
            tracks: Default::default(),
            namespaces: Default::default(),
        }
    }

    /// Register namespace routing metadata from PUBLISH_NAMESPACE.
    ///
    /// This does not register any media tracks. It only creates a request queue
    /// used when a downstream SUBSCRIBE asks for a missing track under this
    /// namespace.
    pub(crate) async fn register_namespace(
        &mut self,
        scope: Option<&str>,
        namespace: TrackNamespace,
    ) -> anyhow::Result<(
        LocalNamespaceRegistration,
        mpsc::Receiver<NamespaceTrackRequest>,
    )> {
        let scope_key = scope.unwrap_or(UNSCOPED).to_string();
        let (tx, rx) = mpsc::channel(NAMESPACE_REQUEST_CHANNEL_CAPACITY);

        let mut namespaces = self
            .namespaces
            .lock()
            .map_err(|_| ServeError::internal_ctx("locals namespace registry lock poisoned"))?;
        let bucket = namespaces.entry(scope_key.clone()).or_default();
        match bucket.entry(namespace.clone()) {
            hash_map::Entry::Vacant(entry) => {
                entry.insert(NamespaceSource { requests: tx });
            }
            hash_map::Entry::Occupied(_) => return Err(ServeError::Duplicate.into()),
        }

        let registration = LocalNamespaceRegistration {
            locals: self.clone(),
            scope_key,
            namespace,
            _gauge_guard: GaugeGuard::new("moq_relay_announced_namespaces"),
        };

        Ok((registration, rx))
    }

    /// Register one exact track received via PUBLISH.
    pub async fn register_track(
        &mut self,
        scope: Option<&str>,
        track: TrackReader,
    ) -> anyhow::Result<LocalTrackRegistration> {
        let full_name = FullTrackName {
            namespace: track.namespace.clone(),
            name: track.name.clone(),
        };
        self.insert_track_with_registration(scope, full_name, track)
            .await
    }

    async fn insert_track_with_registration(
        &mut self,
        scope: Option<&str>,
        full_name: FullTrackName,
        track: TrackReader,
    ) -> anyhow::Result<LocalTrackRegistration> {
        let scope_key = scope.unwrap_or(UNSCOPED).to_string();

        let mut tracks = self
            .tracks
            .lock()
            .map_err(|_| ServeError::internal_ctx("locals track registry lock poisoned"))?;
        let bucket = tracks.entry(scope_key.clone()).or_default();
        match bucket.entry(full_name.clone()) {
            hash_map::Entry::Vacant(entry) => entry.insert(TrackEntry::Published(track)),
            hash_map::Entry::Occupied(_) => return Err(ServeError::Duplicate.into()),
        };

        Ok(LocalTrackRegistration {
            locals: self.clone(),
            scope_key,
            full_name,
            _gauge_guard: GaugeGuard::new("moq_relay_active_published_tracks"),
        })
    }

    /// Retrieve one actual media track by exact Full Track Name.
    pub fn retrieve_track(
        &self,
        scope: Option<&str>,
        full_name: &FullTrackName,
    ) -> Option<TrackReader> {
        let mut tracks = self.tracks.lock().ok()?;
        let bucket = tracks.get_mut(scope.unwrap_or(UNSCOPED))?;
        if bucket
            .get(full_name)
            .is_some_and(|track| track.reader().is_closed())
        {
            if let Some(entry) = bucket.remove(full_name) {
                entry.cancel_namespace();
            }
            return None;
        }
        bucket.get(full_name).map(|entry| entry.reader().clone())
    }

    /// Return the best namespace route source for a requested namespace.
    fn route_namespace(
        &self,
        scope: Option<&str>,
        namespace: &TrackNamespace,
    ) -> Option<NamespaceSource> {
        let namespaces = self.namespaces.lock().ok()?;
        let bucket = namespaces.get(scope.unwrap_or(UNSCOPED))?;

        let mut best_match: Option<NamespaceSource> = None;
        let mut best_len = 0;

        for (registered_ns, source) in bucket.iter() {
            if namespace.fields.len() >= registered_ns.fields.len() {
                let is_prefix = registered_ns
                    .fields
                    .iter()
                    .zip(namespace.fields.iter())
                    .all(|(a, b)| a == b);

                if is_prefix && registered_ns.fields.len() > best_len {
                    best_match = Some(source.clone());
                    best_len = registered_ns.fields.len();
                }
            }
        }

        best_match
    }

    /// Get an existing exact track or request it from a matching namespace source.
    ///
    /// This replaces the old `TracksReader::subscribe` relay registry behavior:
    /// the actual track reader is stored in `tracks`, while PUBLISH_NAMESPACE is
    /// only a source to ask when a track is missing.
    pub(crate) async fn get_or_request_track(
        &mut self,
        scope: Option<&str>,
        namespace: TrackNamespace,
        track_name: impl Into<moq_transport::coding::TrackName>,
    ) -> Option<LocalTrack> {
        let track_name = track_name.into();
        let full_name = FullTrackName {
            namespace: namespace.clone(),
            name: track_name.clone(),
        };
        let scope_key = scope.unwrap_or(UNSCOPED).to_string();

        if let Some(track) = self.acquire_track(&scope_key, &full_name) {
            return Some(track);
        }

        let source = self.route_namespace(scope, &namespace)?;

        let (writer, reader) = Track::new(namespace.clone(), track_name.clone()).produce();
        let (track, cancelled) = {
            let mut tracks = self.tracks.lock().ok()?;
            let bucket = tracks.entry(scope_key.clone()).or_default();
            match bucket.entry(full_name.clone()) {
                hash_map::Entry::Vacant(entry) => {
                    let (cancel, cancelled) = watch::channel(false);
                    let identity = Arc::new(());
                    entry.insert(TrackEntry::Namespace {
                        reader: reader.clone(),
                        leases: 1,
                        cancel,
                        identity: identity.clone(),
                    });
                    (
                        LocalTrack {
                            reader,
                            _lease: Some(LocalTrackLease {
                                locals: self.clone(),
                                scope_key: scope_key.clone(),
                                full_name: full_name.clone(),
                                identity,
                            }),
                        },
                        Some(cancelled),
                    )
                }
                hash_map::Entry::Occupied(mut entry) => {
                    if !entry.get().reader().is_closed() {
                        return acquire_entry(self.clone(), scope_key, full_name, entry.get_mut());
                    }
                    let (cancel, cancelled) = watch::channel(false);
                    let identity = Arc::new(());
                    entry
                        .insert(TrackEntry::Namespace {
                            reader: reader.clone(),
                            leases: 1,
                            cancel,
                            identity: identity.clone(),
                        })
                        .cancel_namespace();
                    (
                        LocalTrack {
                            reader,
                            _lease: Some(LocalTrackLease {
                                locals: self.clone(),
                                scope_key: scope_key.clone(),
                                full_name: full_name.clone(),
                                identity,
                            }),
                        },
                        Some(cancelled),
                    )
                }
            }
        };

        let request = NamespaceTrackRequest {
            writer,
            cancelled: cancelled.expect("new namespace track has cancellation"),
        };
        if source.requests.send(request).await.is_err() {
            if let Ok(mut tracks) = self.tracks.lock() {
                if let Some(bucket) = tracks.get_mut(&scope_key) {
                    if let Some(entry) = bucket.remove(&full_name) {
                        entry.cancel_namespace();
                    }
                }
            }
            return None;
        }

        Some(track)
    }

    fn acquire_track(&self, scope_key: &str, full_name: &FullTrackName) -> Option<LocalTrack> {
        let mut tracks = self.tracks.lock().ok()?;
        let bucket = tracks.get_mut(scope_key)?;
        let entry = bucket.get_mut(full_name)?;
        if entry.reader().is_closed() {
            if let Some(entry) = bucket.remove(full_name) {
                entry.cancel_namespace();
            }
            return None;
        }
        acquire_entry(
            self.clone(),
            scope_key.to_string(),
            full_name.clone(),
            entry,
        )
    }
}

fn acquire_entry(
    locals: Locals,
    scope_key: ScopeKey,
    full_name: FullTrackName,
    entry: &mut TrackEntry,
) -> Option<LocalTrack> {
    match entry {
        TrackEntry::Published(reader) => Some(LocalTrack {
            reader: reader.clone(),
            _lease: None,
        }),
        TrackEntry::Namespace {
            reader,
            leases,
            identity,
            ..
        } => {
            *leases = leases.checked_add(1)?;
            Some(LocalTrack {
                reader: reader.clone(),
                _lease: Some(LocalTrackLease {
                    locals,
                    scope_key,
                    full_name,
                    identity: identity.clone(),
                }),
            })
        }
    }
}

impl Drop for LocalTrackLease {
    fn drop(&mut self) {
        let Ok(mut tracks) = self.locals.tracks.lock() else {
            return;
        };
        let Some(bucket) = tracks.get_mut(&self.scope_key) else {
            return;
        };
        let remove = match bucket.get_mut(&self.full_name) {
            Some(TrackEntry::Namespace {
                leases, identity, ..
            }) if Arc::ptr_eq(identity, &self.identity) => {
                *leases = leases.saturating_sub(1);
                *leases == 0
            }
            _ => false,
        };
        if remove {
            if let Some(entry) = bucket.remove(&self.full_name) {
                entry.cancel_namespace();
            }
            if bucket.is_empty() {
                tracks.remove(&self.scope_key);
            }
        }
    }
}

pub struct LocalNamespaceRegistration {
    locals: Locals,
    scope_key: ScopeKey,
    namespace: TrackNamespace,
    _gauge_guard: GaugeGuard,
}

impl Drop for LocalNamespaceRegistration {
    fn drop(&mut self) {
        let ns = self.namespace.to_utf8_path();
        let scope = if self.scope_key.is_empty() {
            "<unscoped>"
        } else {
            &self.scope_key
        };
        tracing::debug!(namespace = %ns, scope = %scope, "deregistering namespace route source from locals");

        if let Ok(mut namespaces) = self.locals.namespaces.lock() {
            if let Some(bucket) = namespaces.get_mut(self.scope_key.as_str()) {
                bucket.remove(&self.namespace);
                if bucket.is_empty() {
                    namespaces.remove(self.scope_key.as_str());
                }
            }
        }
    }
}

pub struct LocalTrackRegistration {
    locals: Locals,
    scope_key: ScopeKey,
    full_name: FullTrackName,
    _gauge_guard: GaugeGuard,
}

impl Drop for LocalTrackRegistration {
    fn drop(&mut self) {
        let namespace = self.full_name.namespace.to_utf8_path();
        let track = self.full_name.name.to_string();
        let scope = if self.scope_key.is_empty() {
            "<unscoped>"
        } else {
            &self.scope_key
        };
        tracing::debug!(namespace = %namespace, track = %track, scope = %scope, "deregistering track from locals");

        if let Ok(mut tracks) = self.locals.tracks.lock() {
            if let Some(bucket) = tracks.get_mut(self.scope_key.as_str()) {
                bucket.remove(&self.full_name);
                if bucket.is_empty() {
                    tracks.remove(self.scope_key.as_str());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use moq_transport::coding::TrackName;

    fn ns(path: &str) -> TrackNamespace {
        TrackNamespace::from_utf8_path(path)
    }

    fn full(namespace: &TrackNamespace, name: &str) -> FullTrackName {
        FullTrackName {
            namespace: namespace.clone(),
            name: TrackName::from(name),
        }
    }

    #[tokio::test]
    async fn register_track_makes_exact_track_retrievable_until_drop() {
        let mut locals = Locals::new();
        let namespace = ns("room/123");
        let (writer, reader) = Track::new(namespace.clone(), "audio").produce();
        let key = full(&namespace, "audio");

        let registration = locals
            .register_track(None, reader.clone())
            .await
            .expect("track registration should succeed");

        assert!(locals.retrieve_track(None, &key).is_some());

        drop(registration);
        assert!(locals.retrieve_track(None, &key).is_none());

        drop(writer);
    }

    #[tokio::test]
    async fn get_or_request_track_uses_namespace_source_and_caches_reader() {
        let mut locals = Locals::new();
        let namespace = ns("room/123");
        let (_registration, mut requests) = locals
            .register_namespace(None, namespace.clone())
            .await
            .expect("namespace source should register");

        let reader = locals
            .get_or_request_track(None, namespace.clone(), "video")
            .await
            .expect("missing track should be requested from namespace source");

        let requested = requests
            .recv()
            .await
            .expect("source should get TrackWriter");
        assert_eq!(requested.namespace, namespace);
        assert_eq!(requested.name, TrackName::from("video"));

        let key = full(&namespace, "video");
        let cached = locals
            .retrieve_track(None, &key)
            .expect("requested track should be cached");
        assert_eq!(cached.namespace, reader.namespace);
        assert_eq!(cached.name, reader.name);

        let reader_again = locals
            .get_or_request_track(None, namespace.clone(), "video")
            .await
            .expect("cached track should be returned");
        assert_eq!(reader_again.namespace, namespace);
        assert_eq!(reader_again.name, TrackName::from("video"));

        let no_second_request =
            tokio::time::timeout(std::time::Duration::from_millis(50), requests.recv()).await;
        assert!(
            no_second_request.is_err(),
            "cache hit should not request again"
        );
    }

    #[tokio::test]
    async fn concurrent_get_or_request_track_deduplicates_request() {
        let mut locals = Locals::new();
        let namespace = ns("room/123");
        let (_registration, mut requests) = locals
            .register_namespace(None, namespace.clone())
            .await
            .expect("namespace source should register");

        let mut first = locals.clone();
        let mut second = locals.clone();
        let (first_reader, second_reader) = tokio::join!(
            first.get_or_request_track(None, namespace.clone(), "video"),
            second.get_or_request_track(None, namespace.clone(), "video"),
        );

        let first_reader = first_reader.expect("first request should get a reader");
        let second_reader = second_reader.expect("second request should get cached reader");
        assert_eq!(first_reader.namespace, namespace);
        assert_eq!(second_reader.namespace, namespace);
        assert_eq!(first_reader.name, TrackName::from("video"));
        assert_eq!(second_reader.name, TrackName::from("video"));

        requests
            .recv()
            .await
            .expect("source should receive one TrackWriter");
        let no_second_request =
            tokio::time::timeout(std::time::Duration::from_millis(50), requests.recv()).await;
        assert!(
            no_second_request.is_err(),
            "concurrent misses should be deduplicated"
        );
    }

    #[tokio::test]
    async fn namespace_track_cancels_upstream_after_last_downstream_lease() {
        let mut locals = Locals::new();
        let namespace = ns("room/123");
        let (_registration, mut requests) = locals
            .register_namespace(None, namespace.clone())
            .await
            .expect("namespace source should register");

        let first = locals
            .get_or_request_track(None, namespace.clone(), "video")
            .await
            .expect("first downstream should create the upstream track");
        let second = locals
            .get_or_request_track(None, namespace.clone(), "video")
            .await
            .expect("second downstream should share the upstream track");
        let mut request = requests
            .recv()
            .await
            .expect("source should receive one upstream request");

        drop(first);
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(20),
                request.cancelled.changed()
            )
            .await
            .is_err(),
            "one remaining downstream must preserve the upstream subscription"
        );

        drop(second);
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            request.cancelled.changed(),
        )
        .await
        .expect("last downstream should cancel promptly")
        .expect("cancellation sender should remain valid");
        assert!(*request.cancelled.borrow());

        let key = full(&namespace, "video");
        assert!(
            locals.retrieve_track(None, &key).is_none(),
            "zero-lease namespace track must leave the cache"
        );

        let _third = locals
            .get_or_request_track(None, namespace.clone(), "video")
            .await
            .expect("a later downstream should create a fresh upstream track");
        requests
            .recv()
            .await
            .expect("fresh downstream should issue a fresh upstream request");
    }

    #[tokio::test]
    async fn closed_namespace_track_is_replaced_without_stale_lease_removal() {
        let mut locals = Locals::new();
        let namespace = ns("room/123");
        let (_registration, mut requests) = locals
            .register_namespace(None, namespace.clone())
            .await
            .expect("namespace source should register");

        let stale_lease = locals
            .get_or_request_track(None, namespace.clone(), "video")
            .await
            .expect("first request should create a namespace track");
        let first_request = requests.recv().await.expect("first upstream request");
        drop(first_request.writer);

        let current_lease = locals
            .get_or_request_track(None, namespace.clone(), "video")
            .await
            .expect("closed cached track should be replaced in one call");
        let _current_request = requests.recv().await.expect("replacement upstream request");
        drop(stale_lease);

        let key = full(&namespace, "video");
        assert!(
            locals.retrieve_track(None, &key).is_some(),
            "dropping a stale incarnation lease must not remove the replacement"
        );
        drop(current_lease);
    }

    #[tokio::test]
    async fn get_or_request_track_uses_longest_namespace_prefix() {
        let mut locals = Locals::new();
        let (_short_registration, mut short_requests) = locals
            .register_namespace(None, ns("room"))
            .await
            .expect("short prefix should register");
        let (_long_registration, mut long_requests) = locals
            .register_namespace(None, ns("room/123"))
            .await
            .expect("long prefix should register");

        let requested_ns = ns("room/123/camera");
        let reader = locals
            .get_or_request_track(None, requested_ns.clone(), "video")
            .await
            .expect("track should be requested from longest prefix");
        assert_eq!(reader.namespace, requested_ns);

        let long_request = long_requests
            .recv()
            .await
            .expect("longest prefix should receive the request");
        assert_eq!(long_request.namespace, ns("room/123/camera"));
        assert_eq!(long_request.name, TrackName::from("video"));

        let no_short_request =
            tokio::time::timeout(std::time::Duration::from_millis(50), short_requests.recv()).await;
        assert!(
            no_short_request.is_err(),
            "shorter prefix should not receive request"
        );
    }

    #[tokio::test]
    async fn get_or_request_track_returns_none_without_track_or_namespace_source() {
        let mut locals = Locals::new();
        let result = locals
            .get_or_request_track(None, ns("unknown"), "video")
            .await;
        assert!(result.is_none());
    }
}
