// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! A stream is a stream of objects with a header, split into a [Writer] and [Reader] handle.
//!
//! A [Writer] writes an ordered stream of objects.
//! Each object can have a sequence number, allowing the reader to detect gaps objects.
//!
//! A [Reader] reads an ordered stream of objects.
//! The reader can be cloned, in which case each reader receives a copy of each object. (fanout)
//!
//! The stream is closed with [ServeError::Closed] when all writers or readers are dropped.
use std::{collections::VecDeque, ops::Deref, sync::Arc};

use bytes::Bytes;

use crate::data::ObjectStatus;
use crate::watch::State;

use super::{ServeError, Track};

const DELIVERY_TIMEOUT_RESET_CODE: u64 = 0x2;

/// Maximum number of subgroup readers retained for late or lagging consumers.
///
/// A 60-second Phase-4 PC run creates 1,800 frame-per-subgroup entries at
/// 30 Hz, so this keeps the complete registered run while bounding longer
/// live sessions. MoQ tracks are allowed to omit old streams; a consumer that
/// falls behind this window resumes from the oldest retained subgroup.
const MAX_SUBGROUP_HISTORY: usize = 2_048;

pub struct Subgroups {
    pub track: Arc<Track>,
}

impl Subgroups {
    pub fn produce(self) -> (SubgroupsWriter, SubgroupsReader) {
        let (writer, reader) = State::default().split();

        let writer = SubgroupsWriter::new(writer, self.track.clone());
        let reader = SubgroupsReader::new(reader, self.track);

        (writer, reader)
    }
}

impl Deref for Subgroups {
    type Target = Track;

    fn deref(&self) -> &Self::Target {
        &self.track
    }
}

// State shared between the writer and reader.
struct SubgroupsState {
    // Preserve every announced subgroup in creation order. Keeping only the
    // latest reader silently skipped intermediate frame-per-subgroup groups
    // when several appends occurred before a reader was polled.
    subgroups: VecDeque<SubgroupReader>,
    first_index: u64,
    // The subgroup with the numerically largest (group_id, subgroup_id).
    // Groups may arrive out of order over independent QUIC streams, so the
    // back of the arrival-ordered deque is not necessarily the largest; this
    // watermark never regresses when a late subgroup arrives.
    latest: Option<SubgroupReader>,
    closed: Result<(), ServeError>,
}

impl Default for SubgroupsState {
    fn default() -> Self {
        Self {
            subgroups: VecDeque::new(),
            first_index: 0,
            latest: None,
            closed: Ok(()),
        }
    }
}

pub struct SubgroupsWriter {
    pub info: Arc<Track>,
    state: State<SubgroupsState>,
    next_subgroup_id: u64, // Not in the state to avoid a lock
    next_group_id: u64,    // Not in the state to avoid a lock
    last_group_id: u64,    // Not in the state to avoid a lock
}

impl SubgroupsWriter {
    fn new(state: State<SubgroupsState>, track: Arc<Track>) -> Self {
        Self {
            info: track,
            state,
            next_subgroup_id: 0,
            next_group_id: 0,
            last_group_id: 0,
        }
    }

    // Helper to increment the group by one.
    pub fn append(&mut self, priority: u8) -> Result<SubgroupWriter, ServeError> {
        let group_id;
        let subgroup_id;

        // TODO: refactor here... For now, every subgroup is mapped to a new group...
        let start_new_group = true;

        if start_new_group {
            group_id = self.next_group_id;
            subgroup_id = 0;
        } else {
            group_id = self.last_group_id;
            subgroup_id = self.next_subgroup_id;
        }

        self.create(Subgroup {
            group_id,
            subgroup_id,
            priority,
        })
    }

    /// Create a new subgroup with the given parameters, inserting it into the track.
    pub fn create(&mut self, subgroup: Subgroup) -> Result<SubgroupWriter, ServeError> {
        let subgroup = SubgroupInfo {
            track: self.info.clone(),
            group_id: subgroup.group_id,
            subgroup_id: subgroup.subgroup_id,
            priority: subgroup.priority,
        };
        let (writer, reader) = subgroup.produce();

        let mut state = self.state.lock_mut().ok_or(ServeError::Cancel)?;

        // Groups and subgroups may arrive out of order (independent QUIC
        // streams), so a re-created identity must be detected against the
        // full retained history, not only the most recent entry.
        if state
            .subgroups
            .iter()
            .chain(state.latest.as_ref())
            .any(|prior| prior.group_id == writer.group_id && prior.subgroup_id == writer.subgroup_id)
        {
            return Err(ServeError::Duplicate);
        }

        // Enqueue every unique subgroup in arrival order. A late-but-unique
        // subgroup must keep its reader: returning a writer whose reader was
        // discarded cancels the still-unread network stream. Consumers (and
        // any downstream delivery-timeout policy) decide a late group's fate.
        if state
            .latest
            .as_ref()
            .is_none_or(|latest| (writer.group_id, writer.subgroup_id) > (latest.group_id, latest.subgroup_id))
        {
            state.latest = Some(reader.clone());
        }
        state.subgroups.push_back(reader);

        while state.subgroups.len() > MAX_SUBGROUP_HISTORY {
            state.subgroups.pop_front();
            state.first_index = state.first_index.saturating_add(1);
        }

        let latest = state.latest.as_ref().expect("just inserted subgroup");
        self.next_subgroup_id = latest.subgroup_id + 1;
        self.next_group_id = latest.group_id + 1;
        self.last_group_id = latest.group_id;

        Ok(writer)
    }

    /// Close the segment with an error.
    pub fn close(self, err: ServeError) -> Result<(), ServeError> {
        let state = self.state.lock();
        state.closed.clone()?;

        let mut state = state.into_mut().ok_or(ServeError::Cancel)?;
        state.closed = Err(err);

        Ok(())
    }
}

impl Deref for SubgroupsWriter {
    type Target = Track;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

#[derive(Clone)]
pub struct SubgroupsReader {
    pub info: Arc<Track>,
    state: State<SubgroupsState>,
    read_index: u64,
}

impl SubgroupsReader {
    fn new(state: State<SubgroupsState>, track_info: Arc<Track>) -> Self {
        Self {
            info: track_info,
            state,
            read_index: 0,
        }
    }

    pub async fn next(&mut self) -> Result<Option<SubgroupReader>, ServeError> {
        loop {
            {
                let state = self.state.lock();

                if self.read_index < state.first_index {
                    tracing::debug!(
                        skipped = state.first_index - self.read_index,
                        "subgroup reader fell behind retained history"
                    );
                    self.read_index = state.first_index;
                }

                let offset = self.read_index.saturating_sub(state.first_index);
                if offset < state.subgroups.len() as u64 {
                    let subgroup = state.subgroups[offset as usize].clone();
                    self.read_index += 1;
                    return Ok(Some(subgroup));
                }

                state.closed.clone()?;
                match state.modified() {
                    Some(notify) => notify,
                    None => return Ok(None),
                }
            }
            .await; // Try again when the state changes
        }
    }

    // Returns the largest group/sequence
    pub fn latest(&self) -> Option<(u64, u64)> {
        let state = self.state.lock();
        state
            .latest
            .as_ref()
            .and_then(|group| group.latest().map(|object_id| (group.group_id, object_id)))
    }

    /// Check if the subgroups writer has been closed or dropped.
    pub fn is_closed(&self) -> bool {
        let state = self.state.lock();
        state.closed.is_err() || state.modified().is_none()
    }
}

impl Deref for SubgroupsReader {
    type Target = Track;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

/// Parameters that can be specified by the user
#[derive(Debug, Clone, PartialEq)]
pub struct Subgroup {
    // The sequence number of the group within the track.
    // NOTE: These may be received out of order or with gaps.
    pub group_id: u64,

    // The sequence number of the subgroup within the group.
    // NOTE: These may be received out of order or with gaps.
    pub subgroup_id: u64,

    // The priority of the group within the track.
    pub priority: u8,
}

/// Static information about the group
#[derive(Debug, Clone, PartialEq)]
pub struct SubgroupInfo {
    pub track: Arc<Track>,

    // The sequence number of the group within the track.
    // NOTE: These may be received out of order or with gaps.
    pub group_id: u64,

    // The sequence number of the subgroup within the group.
    // NOTE: These may be received out of order or with gaps.
    pub subgroup_id: u64,

    // The priority of the group within the track.
    pub priority: u8,
}

impl SubgroupInfo {
    pub fn produce(self) -> (SubgroupWriter, SubgroupReader) {
        let (writer, reader) = State::default().split();
        let info = Arc::new(self);

        let writer = SubgroupWriter::new(writer, info.clone());
        let reader = SubgroupReader::new(reader, info);

        (writer, reader)
    }
}

impl Deref for SubgroupInfo {
    type Target = Track;

    fn deref(&self) -> &Self::Target {
        &self.track
    }
}

struct SubgroupState {
    // The data that has been received thus far.
    objects: Vec<SubgroupObjectReader>,

    // Set when the writer or all readers are dropped.
    closed: Result<(), ServeError>,
}

impl Default for SubgroupState {
    fn default() -> Self {
        Self {
            objects: Vec::new(),
            closed: Ok(()),
        }
    }
}

/// Used to write data to a stream and notify readers.
pub struct SubgroupWriter {
    // Mutable stream state.
    state: State<SubgroupState>,

    // Immutable stream state.
    pub info: Arc<SubgroupInfo>,

    // The next object sequence number to use.
    next_object_id: u64,
}

impl SubgroupWriter {
    fn new(state: State<SubgroupState>, group: Arc<SubgroupInfo>) -> Self {
        Self {
            state,
            info: group,
            next_object_id: 0,
        }
    }

    /// Create the next object ID with the given payload.
    pub fn write(&mut self, payload: bytes::Bytes) -> Result<(), ServeError> {
        let mut object = self.create(payload.len(), None)?;
        object.write(payload)?;
        Ok(())
    }

    /// Write an object over multiple writes.
    ///
    /// BAD STUFF will happen if the size is wrong; this is an advanced feature.
    pub fn create(
        &mut self,
        size: usize,
        extension_headers: Option<crate::data::ExtensionHeaders>,
    ) -> Result<SubgroupObjectWriter, ServeError> {
        self.create_at(size, extension_headers, tokio::time::Instant::now())
    }

    /// Create the next object and preserve when its header became available to
    /// this forwarding hop.
    ///
    /// Locally produced objects use [`create`](Self::create), whose timestamp
    /// is the object creation instant. A relay receive path calls this method
    /// immediately after decoding the object header, which is the draft-16
    /// DELIVERY_TIMEOUT origin. The timestamp is process-local monotonic state;
    /// it is never serialized or compared across hosts.
    pub fn create_at(
        &mut self,
        size: usize,
        extension_headers: Option<crate::data::ExtensionHeaders>,
        received_at: tokio::time::Instant,
    ) -> Result<SubgroupObjectWriter, ServeError> {
        let (writer, reader) = SubgroupObject {
            group: self.info.clone(),
            object_id: self.next_object_id,
            status: ObjectStatus::NormalObject,
            size,
            extension_headers: extension_headers.unwrap_or_default(),
            received_at,
        }
        .produce();

        self.next_object_id += 1;

        let mut state = self.state.lock_mut().ok_or(ServeError::Cancel)?;
        state.objects.push(reader);

        Ok(writer)
    }

    /// Close the stream with an error.
    pub fn close(self, err: ServeError) -> Result<(), ServeError> {
        let state = self.state.lock();
        state.closed.clone()?;

        let mut state = state.into_mut().ok_or(ServeError::Cancel)?;
        state.closed = Err(err);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.state.lock().objects.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Deref for SubgroupWriter {
    type Target = SubgroupInfo;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

/// Notified when a stream has new data available.
#[derive(Clone)]
pub struct SubgroupReader {
    // Modify the stream state.
    state: State<SubgroupState>,

    // Immutable stream state.
    pub info: Arc<SubgroupInfo>,

    // The number of chunks that we've read.
    // NOTE: Cloned readers inherit this index, but then run in parallel.
    read_index: usize,
}

impl SubgroupReader {
    fn new(state: State<SubgroupState>, subgroup: Arc<SubgroupInfo>) -> Self {
        Self {
            state,
            info: subgroup,
            read_index: 0,
        }
    }

    pub fn latest(&self) -> Option<u64> {
        let state = self.state.lock();
        state.objects.last().map(|o| o.object_id)
    }

    pub async fn read_next(&mut self) -> Result<Option<Bytes>, ServeError> {
        let object = self.next().await?;
        match object {
            Some(mut object) => match object.read_all().await {
                Ok(bytes) => Ok(Some(bytes)),
                Err(ServeError::Closed(DELIVERY_TIMEOUT_RESET_CODE)) => Ok(None),
                Err(err) => Err(err),
            },
            None => Ok(None),
        }
    }

    pub async fn next(&mut self) -> Result<Option<SubgroupObjectReader>, ServeError> {
        loop {
            {
                let state = self.state.lock();

                if self.read_index < state.objects.len() {
                    let object = state.objects[self.read_index].clone();
                    self.read_index += 1;
                    return Ok(Some(object));
                }

                state.closed.clone()?;
                match state.modified() {
                    Some(notify) => notify,
                    None => return Ok(None),
                }
            }
            .await; // Try again when the state changes
        }
    }

    pub fn pos(&self) -> usize {
        self.read_index
    }

    pub fn len(&self) -> usize {
        self.state.lock().objects.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Deref for SubgroupReader {
    type Target = SubgroupInfo;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

/// A subset of Object, since we use the group's info.
#[derive(Clone, PartialEq, Debug)]
pub struct SubgroupObject {
    pub group: Arc<SubgroupInfo>,

    pub object_id: u64,

    // The size of the object.
    pub size: usize,

    // Object status
    pub status: ObjectStatus,

    // Extension headers (for draft-14 compliance, particularly immutable extensions)
    pub extension_headers: crate::data::ExtensionHeaders,

    /// Monotonic instant at which this forwarding hop received/created the
    /// object header. Used only for hop-local DELIVERY_TIMEOUT enforcement.
    pub received_at: tokio::time::Instant,
}

impl SubgroupObject {
    pub fn produce(self) -> (SubgroupObjectWriter, SubgroupObjectReader) {
        let (writer, reader) = State::default().split();
        let info = Arc::new(self);

        let writer = SubgroupObjectWriter::new(writer, info.clone());
        let reader = SubgroupObjectReader::new(reader, info);

        (writer, reader)
    }
}

impl Deref for SubgroupObject {
    type Target = SubgroupInfo;

    fn deref(&self) -> &Self::Target {
        &self.group
    }
}

struct SubgroupObjectState {
    // The data that has been received thus far.
    chunks: Vec<Bytes>,

    // Set when the writer is dropped.
    closed: Result<(), ServeError>,
}

impl Default for SubgroupObjectState {
    fn default() -> Self {
        Self {
            chunks: Vec::new(),
            closed: Ok(()),
        }
    }
}

/// Used to write data to a segment and notify readers.
pub struct SubgroupObjectWriter {
    // Mutable segment state.
    state: State<SubgroupObjectState>,

    // Immutable segment state.
    pub info: Arc<SubgroupObject>,

    // The amount of promised data that has yet to be written.
    remain: usize,
}

impl SubgroupObjectWriter {
    /// Create a new segment with the given info.
    fn new(state: State<SubgroupObjectState>, object: Arc<SubgroupObject>) -> Self {
        Self {
            state,
            remain: object.size,
            info: object,
        }
    }

    /// Write a new chunk of bytes.
    pub fn write(&mut self, chunk: Bytes) -> Result<(), ServeError> {
        if chunk.len() > self.remain {
            return Err(ServeError::Size);
        }
        self.remain -= chunk.len();

        let mut state = self.state.lock_mut().ok_or(ServeError::Cancel)?;
        state.chunks.push(chunk);

        Ok(())
    }

    /// Close the segment with an error.
    pub fn close(self, err: ServeError) -> Result<(), ServeError> {
        if self.remain != 0 {
            return Err(ServeError::Size);
        }

        let state = self.state.lock();
        state.closed.clone()?;

        let mut state = state.into_mut().ok_or(ServeError::Cancel)?;
        state.closed = Err(err);

        Ok(())
    }

    /// Abort an incomplete object without exposing its partial chunks.
    ///
    /// This is used when an inbound subgroup stream is deliberately reset by
    /// DELIVERY_TIMEOUT. Unlike [`close`](Self::close), an abort is valid while
    /// bytes remain because the object is explicitly being discarded.
    pub fn abort(mut self, err: ServeError) -> Result<(), ServeError> {
        let state = self.state.lock();
        state.closed.clone()?;

        let mut state = state.into_mut().ok_or(ServeError::Cancel)?;
        state.closed = Err(err);
        self.remain = 0;
        Ok(())
    }
}

impl Drop for SubgroupObjectWriter {
    fn drop(&mut self) {
        if self.remain == 0 {
            return;
        }

        if let Some(mut state) = self.state.lock_mut() {
            state.closed = Err(ServeError::Size);
        }
    }
}

impl Deref for SubgroupObjectWriter {
    type Target = SubgroupObject;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

/// Notified when a segment has new data available.
#[derive(Clone)]
pub struct SubgroupObjectReader {
    // Modify the segment state.
    state: State<SubgroupObjectState>,

    // Immutable segment state.
    pub info: Arc<SubgroupObject>,

    // The number of chunks that we've read.
    // NOTE: Cloned readers inherit this index, but then run in parallel.
    index: usize,
}

impl SubgroupObjectReader {
    fn new(state: State<SubgroupObjectState>, object: Arc<SubgroupObject>) -> Self {
        Self {
            state,
            info: object,
            index: 0,
        }
    }

    /// Block until the next chunk of bytes is available.
    pub async fn read(&mut self) -> Result<Option<Bytes>, ServeError> {
        loop {
            {
                let state = self.state.lock();

                if self.index < state.chunks.len() {
                    let chunk = state.chunks[self.index].clone();
                    self.index += 1;
                    return Ok(Some(chunk));
                }

                state.closed.clone()?;
                match state.modified() {
                    Some(notify) => notify,
                    None => return Ok(None), // No more changes will come
                }
            }
            .await; // Try again when the state changes
        }
    }

    pub async fn read_all(&mut self) -> Result<Bytes, ServeError> {
        let mut chunks = Vec::new();
        while let Some(chunk) = self.read().await? {
            chunks.push(chunk);
        }

        Ok(Bytes::from(chunks.concat()))
    }
}

impl Deref for SubgroupObjectReader {
    type Target = SubgroupObject;

    fn deref(&self) -> &Self::Target {
        &self.info
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coding::TrackNamespace;
    use crate::serve::{Track, TrackReaderMode};

    #[tokio::test]
    async fn reader_preserves_every_rapidly_appended_subgroup() {
        let (track_writer, track_reader) =
            Track::new(TrackNamespace::from_utf8_path("test"), "pc").produce();
        let mut writer = track_writer.subgroups().unwrap();

        for expected_group in 0..3u64 {
            let mut subgroup = writer.append(128).unwrap();
            assert_eq!(subgroup.group_id, expected_group);
            subgroup.write(Bytes::from(vec![expected_group as u8])).unwrap();
            drop(subgroup);
        }
        drop(writer);

        let mut reader = match track_reader.mode().await.unwrap() {
            TrackReaderMode::Subgroups(reader) => reader,
            _ => panic!("expected subgroup mode"),
        };
        for expected_group in 0..3u64 {
            let mut subgroup = reader
                .next()
                .await
                .unwrap()
                .expect("missing appended subgroup");
            assert_eq!(subgroup.group_id, expected_group);
            assert_eq!(
                subgroup.read_next().await.unwrap().unwrap(),
                Bytes::from(vec![expected_group as u8])
            );
        }
        assert!(reader.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn history_is_bounded_and_a_lagging_reader_fast_forwards() {
        let (track_writer, track_reader) =
            Track::new(TrackNamespace::from_utf8_path("test"), "pc").produce();
        let mut writer = track_writer.subgroups().unwrap();
        let mut reader = match track_reader.mode().await.unwrap() {
            TrackReaderMode::Subgroups(reader) => reader,
            _ => panic!("expected subgroup mode"),
        };

        for expected_group in 0..(MAX_SUBGROUP_HISTORY as u64 + 3) {
            let mut subgroup = writer.append(128).unwrap();
            subgroup
                .write(Bytes::from(vec![(expected_group % 256) as u8]))
                .unwrap();
            drop(subgroup);
        }

        {
            let state = reader.state.lock();
            assert_eq!(state.subgroups.len(), MAX_SUBGROUP_HISTORY);
            assert_eq!(state.first_index, 3);
        }

        let first_retained = reader
            .next()
            .await
            .unwrap()
            .expect("oldest retained subgroup missing");
        assert_eq!(first_retained.group_id, 3);
    }

    #[tokio::test]
    async fn late_subgroup_keeps_its_reader_and_the_track_proceeds() {
        let (track_writer, track_reader) =
            Track::new(TrackNamespace::from_utf8_path("test"), "pc").produce();
        let mut writer = track_writer.subgroups().unwrap();

        // Upstream QUIC may deliver independent subgroup streams out of
        // order: group 570 arrives after 571..=580, then 581 follows.
        let arrival: Vec<u64> = std::iter::once(569)
            .chain(571..=580)
            .chain([570, 581])
            .collect();

        for &group_id in &arrival {
            let mut subgroup = writer
                .create(Subgroup {
                    group_id,
                    subgroup_id: 0,
                    priority: 128,
                })
                .unwrap_or_else(|err| panic!("group {} rejected: {:?}", group_id, err));
            // A late group's writer must not be cancelled by a discarded
            // reader; its payload stays receivable.
            subgroup
                .write(Bytes::from(group_id.to_be_bytes().to_vec()))
                .unwrap_or_else(|err| panic!("group {} write failed: {:?}", group_id, err));
        }
        drop(writer);

        let mut reader = match track_reader.mode().await.unwrap() {
            TrackReaderMode::Subgroups(reader) => reader,
            _ => panic!("expected subgroup mode"),
        };
        for &group_id in &arrival {
            let mut subgroup = reader
                .next()
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("group {} missing", group_id));
            assert_eq!(subgroup.group_id, group_id);
            assert_eq!(
                subgroup.read_next().await.unwrap().unwrap(),
                Bytes::from(group_id.to_be_bytes().to_vec())
            );
        }
        assert!(reader.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn late_subgroup_does_not_regress_the_latest_watermark() {
        let (track_writer, track_reader) =
            Track::new(TrackNamespace::from_utf8_path("test"), "pc").produce();
        let mut writer = track_writer.subgroups().unwrap();
        let reader = match track_reader.mode().await.unwrap() {
            TrackReaderMode::Subgroups(reader) => reader,
            _ => panic!("expected subgroup mode"),
        };

        for group_id in std::iter::once(569).chain(571..=580) {
            let mut subgroup = writer
                .create(Subgroup {
                    group_id,
                    subgroup_id: 0,
                    priority: 128,
                })
                .unwrap();
            subgroup.write(Bytes::from_static(b"x")).unwrap();
        }
        assert_eq!(reader.latest(), Some((580, 0)));

        // The late arrival of 570 must not move the watermark backwards.
        let mut late = writer
            .create(Subgroup {
                group_id: 570,
                subgroup_id: 0,
                priority: 128,
            })
            .unwrap();
        late.write(Bytes::from_static(b"x")).unwrap();
        assert_eq!(reader.latest(), Some((580, 0)));

        // A subsequent group advances it normally.
        let mut next = writer
            .create(Subgroup {
                group_id: 581,
                subgroup_id: 0,
                priority: 128,
            })
            .unwrap();
        next.write(Bytes::from_static(b"x")).unwrap();
        assert_eq!(reader.latest(), Some((581, 0)));
    }

    #[tokio::test]
    async fn recreated_group_id_is_a_duplicate_below_and_at_the_watermark() {
        let (track_writer, _track_reader) =
            Track::new(TrackNamespace::from_utf8_path("test"), "pc").produce();
        let mut writer = track_writer.subgroups().unwrap();

        for group_id in 569..=571 {
            writer
                .create(Subgroup {
                    group_id,
                    subgroup_id: 0,
                    priority: 128,
                })
                .unwrap();
        }

        for group_id in [571, 570, 569] {
            let result = writer.create(Subgroup {
                group_id,
                subgroup_id: 0,
                priority: 128,
            });
            assert!(
                matches!(result, Err(ServeError::Duplicate)),
                "re-created group {} must be a duplicate",
                group_id
            );
        }
    }

    #[tokio::test]
    async fn delivery_timeout_discards_a_partial_object_without_failing_the_track() {
        let (track_writer, track_reader) =
            Track::new(TrackNamespace::from_utf8_path("test"), "pc").produce();
        let mut writer = track_writer.subgroups().unwrap();
        let mut subgroup = writer.append(128).unwrap();
        let mut object = subgroup.create(8, None).unwrap();
        object.write(Bytes::from_static(b"part")).unwrap();
        object.abort(ServeError::Closed(DELIVERY_TIMEOUT_RESET_CODE)).unwrap();
        drop(subgroup);
        drop(writer);

        let mut reader = match track_reader.mode().await.unwrap() {
            TrackReaderMode::Subgroups(reader) => reader,
            _ => panic!("expected subgroup mode"),
        };
        let mut subgroup = reader.next().await.unwrap().expect("missing subgroup");
        assert!(subgroup.read_next().await.unwrap().is_none());
        assert!(reader.next().await.unwrap().is_none());
    }
}
