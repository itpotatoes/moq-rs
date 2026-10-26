//! Bounded verification-only recorder. No file I/O on transport callbacks.
//!
//! Schema `relay-object-trace-v3` = v1 rows and fields unchanged, plus:
//! * `relay_object`: `publisher_priority`, `quinn_priority_mapped`,
//!   `track_incarnation`;
//! * `relay_control` rows (SUBSCRIBE / SUBSCRIBE_OK / REQUEST_ERROR /
//!   PUBLISH_DONE at each session's control stream) in their own bounded
//!   buffer, with `t_send_begin_us` on sent rows and the request's
//!   `track_incarnation` / `track_binding` joined from the relay's own
//!   subscription-to-track bindings (also bounded and loss-accounted);
//! * relay RSS (`/proc/self/status` VmRSS, sampled on a separate OS thread)
//!   on `trace_meta` (start) and `trace_end` (sampled maximum and end).
//!   Kernel VmHWM is not exported: on this host it read below an earlier
//!   VmRSS sample.
//!
//! v2 -> v3: control rows gained PUBLISH_DONE boundaries and the fields
//! above, `trace_end` gained link and control-clock counters, and `intact`
//! also requires no link loss, no duplicate binding and no control clock
//! failure. `clock_errors` again counts object-boundary clock failures only.
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::Path,
    sync::{atomic::{AtomicBool, AtomicU64, Ordering}, Arc, Mutex},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use moq_relay_ietf::DataPriorityMapping;
use moq_transport::{
    coding::{TrackName, TrackNamespace},
    object_trace::{Boundary, ControlDirection, ControlMessage, Observer, TrackBinding},
    serve::{SubgroupObject, Track},
};
use serde_json::json;

const CAPACITY: usize = 4096;

pub const SCHEMA: &str = "relay-object-trace-v3";
/// Fixed bound of `relay_control` rows, independent of `--object-trace-capacity`.
pub const CONTROL_CAPACITY: usize = 4096;
/// Fixed bound of subscription-to-track bindings (one per SUBSCRIBE served
/// or sent), independent of `--object-trace-capacity`.
pub const LINK_CAPACITY: usize = 4096;
/// Nominal RSS sampling period (separate OS thread; never on a runtime worker).
pub const RSS_SAMPLE_PERIOD: Duration = Duration::from_millis(100);

struct Record {
    boundary: Boundary,
    object: Arc<SubgroupObject>,
    track_alias: u64,
    timestamp_us: u64,
    received_bytes: Option<usize>,
}

/// One control message as observed; owned copies of the identity fields.
struct ControlRecord {
    boundary: &'static str,
    direction: ControlDirection,
    session: u64,
    timestamp_us: u64,
    send_begin_us: Option<u64>,
    request_id: u64,
    track_alias: Option<u64>,
    namespace: Option<TrackNamespace>,
    track_name: Option<TrackName>,
    /// Some(Some(ms)) present, Some(None) absent, None not applicable.
    delivery_timeout_ms: Option<Option<u64>>,
    params_decode_error: Option<bool>,
    error_code: Option<u64>,
    status_code: Option<u64>,
    stream_count: Option<u64>,
    reason: Option<String>,
}

impl ControlRecord {
    fn new(direction: ControlDirection, message: ControlMessage<'_>, session: u64,
           timestamp_us: u64, send_begin_us: Option<u64>) -> Self {
        let mut record = Self {
            boundary: message.boundary(direction), direction, session, timestamp_us,
            send_begin_us, request_id: 0, track_alias: None, namespace: None,
            track_name: None, delivery_timeout_ms: None, params_decode_error: None,
            error_code: None, status_code: None, stream_count: None, reason: None,
        };
        match message {
            ControlMessage::Subscribe(m) => {
                record.request_id = m.id;
                record.namespace = Some(m.track_namespace.clone());
                record.track_name = Some(m.track_name.clone());
                record.set_delivery_timeout(m.params.delivery_timeout());
            }
            ControlMessage::SubscribeOk(m) => {
                record.request_id = m.id;
                record.track_alias = Some(m.track_alias);
                record.set_delivery_timeout(m.params.delivery_timeout());
            }
            ControlMessage::RequestError(m) => {
                record.request_id = m.id;
                record.error_code = Some(m.error_code);
                record.reason = Some(m.reason.0.clone());
            }
            ControlMessage::PublishDone(m) => {
                record.request_id = m.id;
                record.status_code = Some(m.status_code);
                record.stream_count = Some(m.stream_count);
                record.reason = Some(m.reason.0.clone());
            }
        }
        record
    }

    fn set_delivery_timeout<E>(&mut self, value: Result<Option<u64>, E>) {
        match value {
            Ok(ms) => {
                self.delivery_timeout_ms = Some(ms);
                self.params_decode_error = Some(false);
            }
            Err(_) => {
                self.delivery_timeout_ms = Some(None);
                self.params_decode_error = Some(true);
            }
        }
    }

    /// The request-id space of this row: a message SENT in reply to, or a
    /// SUBSCRIBE RECEIVED from, the peer uses the peer-issued id space
    /// (`true`); our own SUBSCRIBE and the replies to it use our own space.
    fn peer_issued(&self) -> bool {
        match self.boundary {
            "subscribe_received" => true,
            "subscribe_sent" => false,
            _ => self.direction == ControlDirection::Sent,
        }
    }
}

/// One subscription request bound to a track incarnation (the Arc is kept so
/// its address cannot be reused while the trace exists).
struct LinkRecord {
    binding: TrackBinding,
    session: u64,
    request_id: u64,
    track: Arc<Track>,
    timestamp_us: u64,
}

/// Same OS clock and epoch as the Skew sender/receiver. Linux only.
fn monotonic_us() -> Option<u64> {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    if rc != 0 || ts.tv_sec < 0 || !(0..1_000_000_000).contains(&ts.tv_nsec) {
        return None;
    }
    Some(ts.tv_sec as u64 * 1_000_000 + ts.tv_nsec as u64 / 1000)
}

pub struct Recorder {
    enabled: AtomicBool,
    active: AtomicU64,
    callbacks: AtomicU64,
    dropped: AtomicU64,
    /// Object-boundary clock failures only (v1 meaning).
    clock_errors: AtomicU64,
    records: Mutex<Vec<Record>>,
    capacity: usize,
    control_callbacks: AtomicU64,
    control_dropped: AtomicU64,
    /// Clock failures of control and binding observations.
    control_clock_errors: AtomicU64,
    control_records: Mutex<Vec<ControlRecord>>,
    control_capacity: usize,
    link_callbacks: AtomicU64,
    link_dropped: AtomicU64,
    link_records: Mutex<Vec<LinkRecord>>,
    link_capacity: usize,
    /// Clock of control and binding observations (injectable in tests).
    control_clock: fn() -> Option<u64>,
}

impl Recorder {
    fn new(capacity: usize) -> Self {
        Self::with_capacities(capacity, CONTROL_CAPACITY, LINK_CAPACITY)
    }

    fn with_capacities(capacity: usize, control_capacity: usize, link_capacity: usize) -> Self {
        Self {
            enabled: AtomicBool::new(true), active: AtomicU64::new(0),
            callbacks: AtomicU64::new(0), dropped: AtomicU64::new(0),
            clock_errors: AtomicU64::new(0),
            records: Mutex::new(Vec::with_capacity(capacity)), capacity,
            control_callbacks: AtomicU64::new(0), control_dropped: AtomicU64::new(0),
            control_clock_errors: AtomicU64::new(0),
            control_records: Mutex::new(Vec::with_capacity(control_capacity)), control_capacity,
            link_callbacks: AtomicU64::new(0), link_dropped: AtomicU64::new(0),
            link_records: Mutex::new(Vec::with_capacity(link_capacity)), link_capacity,
            control_clock: monotonic_us,
        }
    }

    #[allow(clippy::type_complexity)]
    fn seal(&self) -> anyhow::Result<(Vec<Record>, Vec<ControlRecord>, Vec<LinkRecord>)> {
        // Only callbacks admitted before this cutoff belong to the snapshot.
        // This is NOT a proof that the relay's producer tasks were all joined.
        self.enabled.store(false, Ordering::SeqCst);
        let deadline = Instant::now() + Duration::from_millis(100);
        while self.active.load(Ordering::SeqCst) != 0 {
            anyhow::ensure!(Instant::now() < deadline, "object trace seal timeout");
            std::thread::yield_now();
        }
        let mut records = self.records.lock().map_err(|_| anyhow::anyhow!("trace poisoned"))?;
        let mut control = self.control_records.lock()
            .map_err(|_| anyhow::anyhow!("control trace poisoned"))?;
        let mut links = self.link_records.lock()
            .map_err(|_| anyhow::anyhow!("link trace poisoned"))?;
        Ok((std::mem::take(&mut *records), std::mem::take(&mut *control),
            std::mem::take(&mut *links)))
    }

    /// Admit one control-side record: build it outside the lock, then hold
    /// the lock only for the capacity check and the push. Control and binding
    /// observations are rare (tens per run) and never on the object path, so
    /// they WAIT for the lock instead of dropping on contention; only a full
    /// buffer (or a poisoned lock) loses a record, and that is counted.
    fn admit<T>(&self, buffer: &Mutex<Vec<T>>, capacity: usize, callbacks: &AtomicU64,
                dropped: &AtomicU64, make: impl FnOnce() -> T) {
        self.active.fetch_add(1, Ordering::SeqCst);
        if self.enabled.load(Ordering::SeqCst) {
            callbacks.fetch_add(1, Ordering::SeqCst);
            let record = make();
            match buffer.lock() {
                Ok(mut records) if records.len() < capacity => records.push(record),
                _ => { dropped.fetch_add(1, Ordering::SeqCst); }
            }
        }
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Observer for Recorder {
    fn now_us(&self) -> u64 {
        match monotonic_us() {
            Some(t) => t,
            None => {
                self.clock_errors.fetch_add(1, Ordering::SeqCst);
                0
            }
        }
    }

    fn control_now_us(&self) -> u64 {
        match (self.control_clock)() {
            Some(t) => t,
            None => {
                self.control_clock_errors.fetch_add(1, Ordering::SeqCst);
                0
            }
        }
    }

    fn record(&self, boundary: Boundary, object: &Arc<SubgroupObject>,
              track_alias: u64, timestamp_us: u64) {
        self.record_progress(boundary, object, track_alias, timestamp_us, None);
    }

    fn record_progress(&self, boundary: Boundary, object: &Arc<SubgroupObject>,
                       track_alias: u64, timestamp_us: u64, received_bytes: Option<usize>) {
        self.active.fetch_add(1, Ordering::SeqCst);
        if self.enabled.load(Ordering::SeqCst) {
            self.callbacks.fetch_add(1, Ordering::SeqCst);
            // Never wait for another callback. Any contention/overflow/poison
            // invalidates the snapshot rather than hiding missing observations.
            match self.records.try_lock() {
                Ok(mut records) if records.len() < self.capacity => records.push(Record {
                    boundary, object: object.clone(), track_alias, timestamp_us, received_bytes,
                }),
                _ => { self.dropped.fetch_add(1, Ordering::SeqCst); }
            }
        }
        self.active.fetch_sub(1, Ordering::SeqCst);
    }

    fn observes_control(&self) -> bool {
        true
    }

    fn record_control(&self, direction: ControlDirection, message: ControlMessage<'_>,
                      session: u64, timestamp_us: u64, send_begin_us: Option<u64>) {
        self.admit(&self.control_records, self.control_capacity, &self.control_callbacks,
                   &self.control_dropped,
                   || ControlRecord::new(direction, message, session, timestamp_us, send_begin_us));
    }

    fn record_link(&self, binding: TrackBinding, session: u64, request_id: u64,
                   track: &Arc<Track>, timestamp_us: u64) {
        self.admit(&self.link_records, self.link_capacity, &self.link_callbacks,
                   &self.link_dropped,
                   || LinkRecord { binding, session, request_id, track: track.clone(), timestamp_us });
    }
}

/// VmRSS of this process in kB, as `/proc/self/status` reports it (the
/// kernel's approximate per-CPU-batched counter, not an exact page count).
fn read_rss_kb() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let field = |name: &str| status.lines()
        .find_map(|line| line.strip_prefix(name))
        .and_then(|rest| rest.trim().strip_suffix("kB"))
        .and_then(|value| value.trim().parse::<u64>().ok());
    field("VmRSS:")
}

type RssReader = Arc<dyn Fn() -> Option<u64> + Send + Sync>;

struct RssStats {
    samples: AtomicU64,
    errors: AtomicU64,
    max_kb: AtomicU64,
    max_t_us: AtomicU64,
    read: RssReader,
}

/// A sample: (VmRSS kB, t_us).
type RssSample = (u64, u64);

impl RssStats {
    fn new(read: RssReader) -> Self {
        Self { samples: AtomicU64::new(0), errors: AtomicU64::new(0), max_kb: AtomicU64::new(0),
               max_t_us: AtomicU64::new(0), read }
    }

    /// Only one writer at a time: `start`, then the sampler thread, then
    /// `finish` after the thread was joined.
    fn sample(&self) -> Option<RssSample> {
        let sample = monotonic_us().and_then(|t| (self.read)().map(|rss| (rss, t)));
        match sample {
            Some((rss, t)) => {
                if self.samples.fetch_add(1, Ordering::SeqCst) == 0
                    || rss > self.max_kb.load(Ordering::SeqCst) {
                    self.max_kb.store(rss, Ordering::SeqCst);
                    self.max_t_us.store(t, Ordering::SeqCst);
                }
            }
            None => { self.errors.fetch_add(1, Ordering::SeqCst); }
        }
        sample
    }
}

/// Periodic RSS sampler on its own OS thread.
///
/// Spacing: the next periodic sample is due one full `period` after the
/// previous sample COMPLETED, so a slow read is never followed by a
/// catch-up sample. `finish` unparks the thread, but a read already in
/// progress is not interruptible: teardown waits for at most that one read
/// (normally microseconds; as long as the read itself if procfs stalls).
struct RssSampler {
    stats: Arc<RssStats>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
    start: Option<RssSample>,
}

impl RssSampler {
    fn start(period: Duration) -> anyhow::Result<Self> {
        Self::start_with(period, Arc::new(read_rss_kb))
    }

    fn start_with(period: Duration, read: RssReader) -> anyhow::Result<Self> {
        let stats = Arc::new(RssStats::new(read));
        let stop = Arc::new(AtomicBool::new(false));
        let start = stats.sample();
        let thread = {
            let (stats, stop) = (stats.clone(), stop.clone());
            std::thread::Builder::new().name("relay-trace-rss".into()).spawn(move || {
                let mut next = Instant::now() + period;
                while !stop.load(Ordering::SeqCst) {
                    let now = Instant::now();
                    if now < next {
                        std::thread::park_timeout(next - now);
                        continue;
                    }
                    stats.sample();
                    next = Instant::now() + period; // after completion: no catch-up
                }
            })?
        };
        Ok(Self { stats, stop, thread: Some(thread), start })
    }

    /// Stop and join the sampler, then take the end sample on this thread.
    fn finish(&mut self) -> anyhow::Result<Option<RssSample>> {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            thread.thread().unpark();
            thread.join().map_err(|_| anyhow::anyhow!("rss sampler panicked"))?;
        }
        Ok(self.stats.sample())
    }
}

impl Drop for RssSampler {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = &self.thread {
            thread.thread().unpark();
        }
    }
}

fn meta_row(run_id: &str, capacity: usize, mapping: DataPriorityMapping,
            rss_start: Option<RssSample>) -> serde_json::Value {
    json!({"role":"trace_meta", "schema":SCHEMA,
        "run_id":run_id, "pid":std::process::id(),
        "clock":"monotonic_ns/1000", "capacity":capacity,
        "receive_progress":true,
        "scope":"sealed_callback_interval_not_producer_quiescence",
        "control_capacity":CONTROL_CAPACITY,
        "link_capacity":LINK_CAPACITY,
        "data_priority_mapping":mapping.as_str(),
        "rss_source":"/proc/self/status VmRSS kB",
        "rss_sample_period_us":RSS_SAMPLE_PERIOD.as_micros() as u64,
        "rss_start_kb":rss_start.map(|s| s.0),
        "rss_start_t_us":rss_start.map(|s| s.1)})
}

fn is_forward(boundary: Boundary) -> bool {
    matches!(boundary, Boundary::ForwardStart | Boundary::ForwardAccepted
        | Boundary::ForwardTimeout | Boundary::ForwardInterrupted)
}

/// Small opaque ids for track incarnations, first-seen order.
#[derive(Default)]
struct Incarnations(HashMap<usize, usize>);

impl Incarnations {
    fn id(&mut self, track: &Arc<Track>) -> usize {
        let next = self.0.len() + 1;
        *self.0.entry(Arc::as_ptr(track) as usize).or_insert(next)
    }
}

pub struct Trace {
    recorder: Arc<Recorder>,
    writer: BufWriter<File>,
    mapping: DataPriorityMapping,
    rss: RssSampler,
}

impl Trace {
    pub fn start(path: &Path, run_id: &str, capacity: usize,
                 mapping: DataPriorityMapping) -> anyhow::Result<Self> {
        anyhow::ensure!(capacity > 0, "object trace capacity must be positive");
        anyhow::ensure!(cfg!(target_os = "linux"), "object trace requires Linux CLOCK_MONOTONIC");
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        let mut writer = BufWriter::new(file);
        let rss = RssSampler::start(RSS_SAMPLE_PERIOD)?;
        writeln!(writer, "{}", meta_row(run_id, capacity, mapping, rss.start))?;
        writer.flush()?;
        let recorder = Arc::new(Recorder::new(capacity));
        moq_transport::object_trace::install(recorder.clone())
            .map_err(|_| anyhow::anyhow!("object trace already installed"))?;
        Ok(Self { recorder, writer, mapping, rss })
    }

    pub fn finish(mut self, ending: &str) -> anyhow::Result<()> {
        let (records, control, links) = self.recorder.seal()?;
        let rss_end = self.rss.finish()?;
        // Bindings first, so incarnation ids follow binding order.
        let mut incarnations = Incarnations::default();
        let mut by_request: HashMap<(u64, bool, u64), (usize, TrackBinding, u64)> = HashMap::new();
        let mut duplicate_keys = 0u64;
        for link in &links {
            let id = incarnations.id(&link.track);
            let key = (link.session, link.binding.is_downstream(), link.request_id);
            if by_request.contains_key(&key) {
                duplicate_keys += 1; // keep the first binding of the request
            } else {
                by_request.insert(key, (id, link.binding, link.timestamp_us));
            }
        }
        // Arc references in records prevent address reuse. Export small opaque
        // IDs instead of pointers; namespace bytes retain exact tuple identity.
        let mut instances = HashMap::new();
        for record in &records {
            let object = &record.object;
            let next = instances.len() + 1;
            let instance = *instances.entry(Arc::as_ptr(object) as usize).or_insert(next);
            let namespace_hex: Vec<_> = object.namespace.fields.iter()
                .map(|field| hex::encode(&field.value)).collect();
            // The subgroup incarnation's priority: the received subgroup
            // header's publisher priority, which the relay writes unchanged
            // into the forwarded header. The quinn value is this binary's
            // mapping of it (the computation applied at stream open), not a
            // readback of quinn state; receive rows have none.
            let priority = object.group.priority;
            let quinn = is_forward(record.boundary).then(|| self.mapping.to_quinn(priority));
            let incarnation = incarnations.id(&object.group.track);
            writeln!(self.writer, "{}", json!({"role":"relay_object",
                "boundary":record.boundary.as_str(), "t_us":record.timestamp_us,
                "object_instance":instance, "namespace_hex":namespace_hex,
                "track_hex":hex::encode(object.name.as_bytes()),
                "track":object.name.to_string_lossy(), "track_alias":record.track_alias,
                "group_id":object.group_id, "subgroup_id":object.subgroup_id,
                "object_id":object.object_id, "size":object.size,
                "received_payload_bytes":record.received_bytes,
                "publisher_priority":priority, "quinn_priority_mapped":quinn,
                "track_incarnation":incarnation}))?;
        }
        for record in &control {
            let namespace_hex: Option<Vec<_>> = record.namespace.as_ref().map(|namespace|
                namespace.fields.iter().map(|field| hex::encode(&field.value)).collect());
            let link = by_request.get(&(record.session, record.peer_issued(), record.request_id));
            writeln!(self.writer, "{}", json!({"role":"relay_control",
                "boundary":record.boundary, "t_us":record.timestamp_us,
                "t_send_begin_us":record.send_begin_us,
                "session":record.session, "request_id":record.request_id,
                "track_alias":record.track_alias, "namespace_hex":namespace_hex,
                "track_hex":record.track_name.as_ref().map(|name| hex::encode(name.as_bytes())),
                "track":record.track_name.as_ref().map(|name| name.to_string_lossy()),
                "delivery_timeout_ms":record.delivery_timeout_ms.flatten(),
                "params_decode_error":record.params_decode_error,
                "error_code":record.error_code, "status_code":record.status_code,
                "stream_count":record.stream_count, "reason":record.reason,
                "track_incarnation":link.map(|l| l.0),
                "track_binding":link.map(|l| l.1.as_str()),
                "t_bind_us":link.map(|l| l.2)}))?;
        }
        let callbacks = self.recorder.callbacks.load(Ordering::SeqCst);
        let dropped = self.recorder.dropped.load(Ordering::SeqCst);
        let clock_errors = self.recorder.clock_errors.load(Ordering::SeqCst);
        let control_callbacks = self.recorder.control_callbacks.load(Ordering::SeqCst);
        let control_dropped = self.recorder.control_dropped.load(Ordering::SeqCst);
        let control_clock_errors = self.recorder.control_clock_errors.load(Ordering::SeqCst);
        let link_callbacks = self.recorder.link_callbacks.load(Ordering::SeqCst);
        let link_dropped = self.recorder.link_dropped.load(Ordering::SeqCst);
        let control_intact = control_callbacks == control.len() as u64 && control_dropped == 0
            && link_callbacks == links.len() as u64 && link_dropped == 0
            && duplicate_keys == 0 && control_clock_errors == 0;
        let intact = callbacks == records.len() as u64 && dropped == 0 && clock_errors == 0
            && control_intact;
        let stats = &self.rss.stats;
        let samples = stats.samples.load(Ordering::SeqCst);
        writeln!(self.writer, "{}", json!({"role":"trace_end", "ending":ending,
            "sealed":true, "callbacks":callbacks, "written":records.len(),
            "dropped":dropped, "clock_errors":clock_errors, "intact":intact,
            "producer_quiescence_proven":false,
            "control_callbacks":control_callbacks, "control_written":control.len(),
            "control_dropped":control_dropped, "control_clock_errors":control_clock_errors,
            "link_callbacks":link_callbacks, "link_recorded":links.len(),
            "link_dropped":link_dropped, "link_duplicate_keys":duplicate_keys,
            "rss_samples":samples, "rss_sample_errors":stats.errors.load(Ordering::SeqCst),
            "rss_max_kb":(samples > 0).then(|| stats.max_kb.load(Ordering::SeqCst)),
            "rss_max_t_us":(samples > 0).then(|| stats.max_t_us.load(Ordering::SeqCst)),
            "rss_end_kb":rss_end.map(|s| s.0), "rss_end_t_us":rss_end.map(|s| s.1)}))?;
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        anyhow::ensure!(intact, "object trace integrity failure");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use moq_transport::{coding::{KeyValuePairs, ReasonPhrase, TrackNamespace},
                        message, serve::{Track, SubgroupInfo}};

    fn object() -> Arc<SubgroupObject> {
        object_with("pc", 0)
    }

    fn object_with(track: &str, priority: u8) -> Arc<SubgroupObject> {
        let (mut writer, _reader) = SubgroupInfo {
            track: Arc::new(Track::new(TrackNamespace::from_utf8_path("run"), track)),
            group_id: 0, subgroup_id: 0, priority,
        }.produce();
        writer.create(32, None).unwrap().info.clone()
    }

    fn rows(path: &std::path::Path) -> Vec<serde_json::Value> {
        std::fs::read_to_string(path).unwrap()
            .lines().map(|line| serde_json::from_str(line).unwrap()).collect()
    }

    fn subscribe(id: u64, track: &str, timeout_ms: Option<u64>) -> message::Subscribe {
        let mut params = KeyValuePairs::default();
        if let Some(ms) = timeout_ms { params.set_delivery_timeout(ms); }
        message::Subscribe { id, track_namespace: TrackNamespace::from_utf8_path("run"),
                             track_name: track.into(), params }
    }

    fn subscribe_ok(id: u64, alias: u64, timeout_ms: Option<u64>) -> message::SubscribeOk {
        let mut params = KeyValuePairs::default();
        if let Some(ms) = timeout_ms { params.set_delivery_timeout(ms); }
        message::SubscribeOk { id, track_alias: alias, params, track_extensions: Default::default() }
    }

    fn request_error(id: u64, code: u64) -> message::RequestError {
        message::RequestError { id, error_code: code, retry_interval: 0,
                                reason: ReasonPhrase("track not found".to_string()) }
    }

    #[test]
    fn bounded_overflow_is_counted() {
        let recorder = Recorder::new(1);
        let object = object();
        for _ in 0..2 { recorder.record(Boundary::ForwardStart, &object, 0, 1); }
        assert_eq!(recorder.seal().unwrap().0.len(), 1);
        assert_eq!(recorder.callbacks.load(Ordering::SeqCst), 2);
        assert_eq!(recorder.dropped.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn contention_does_not_block() {
        let recorder = Recorder::new(2);
        let guard = recorder.records.lock().unwrap();
        recorder.record(Boundary::ForwardStart, &object(), 0, 1);
        assert_eq!(recorder.dropped.load(Ordering::SeqCst), 1);
        drop(guard);
        assert!(recorder.seal().unwrap().0.is_empty());
    }

    #[test]
    fn seal_excludes_later_callbacks_explicitly() {
        let recorder = Recorder::new(2);
        let object = object();
        recorder.record(Boundary::ReceiveComplete, &object, 0, 1);
        assert_eq!(recorder.seal().unwrap().0.len(), 1);
        recorder.record(Boundary::ForwardStart, &object, 0, 2);
        assert_eq!(recorder.callbacks.load(Ordering::SeqCst), 1);
        assert_eq!(recorder.active.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn timestamp_is_os_monotonic_microseconds() {
        let recorder = Recorder::new(2);
        let first = recorder.now_us();
        assert!(first > 0);
        assert!(recorder.now_us() >= first);
        assert_eq!(recorder.clock_errors.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn concurrent_callbacks_conserve_records_and_drops() {
        let recorder = Arc::new(Recorder::new(4096));
        let object = object();
        let threads: Vec<_> = (0..4).map(|_| {
            let recorder = recorder.clone();
            let object = object.clone();
            std::thread::spawn(move || {
                for _ in 0..256 {
                    recorder.record(Boundary::ForwardStart, &object, 0, 1);
                }
            })
        }).collect();
        for thread in threads { thread.join().unwrap(); }
        let records = recorder.seal().unwrap().0;
        assert_eq!(recorder.callbacks.load(Ordering::SeqCst), 1024);
        assert_eq!(records.len() as u64 + recorder.dropped.load(Ordering::SeqCst), 1024);
    }

    fn test_trace(recorder: Arc<Recorder>, file: File) -> Trace {
        Trace { recorder, writer: BufWriter::new(file), mapping: DataPriorityMapping::MoqtV2,
                rss: RssSampler::start(RSS_SAMPLE_PERIOD).unwrap() }
    }

    fn new_test_file() -> (std::path::PathBuf, File) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!("skew-relay-trace-test-{}-{}-{}",
            std::process::id(), Recorder::new(0).now_us(), NEXT.fetch_add(1, Ordering::SeqCst)));
        let file = OpenOptions::new().create_new(true).write(true).open(&path).unwrap();
        (path, file)
    }

    #[test]
    fn existing_evidence_is_never_overwritten() {
        let (path, mut file) = new_test_file();
        file.write_all(b"preserved").unwrap();
        assert!(Trace::start(&path, "run", 4096, DataPriorityMapping::MoqtV2).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"preserved");
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn finalization_propagates_write_errors() {
        let (path, file) = new_test_file();
        drop(file);
        let trace = test_trace(Arc::new(Recorder::new(4)), File::open(&path).unwrap()); // read-only FD
        assert!(trace.finish("sigterm").is_err());
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn serialization_preserves_incarnations_and_sealed_scope() {
        let (path, file) = new_test_file();
        let recorder = Arc::new(Recorder::new(4));
        let first = object();
        let second = object(); // same logical identity, distinct incarnation
        recorder.record(Boundary::ReceiveComplete, &first, 3, 10);
        recorder.record(Boundary::ForwardStart, &first, 7, 11);
        recorder.record(Boundary::ReceiveComplete, &second, 3, 12);
        test_trace(recorder, file).finish("sigterm").unwrap();
        let rows: Vec<serde_json::Value> = std::fs::read_to_string(&path).unwrap()
            .lines().map(|line| serde_json::from_str(line).unwrap()).collect();
        assert_eq!(rows[0]["object_instance"], rows[1]["object_instance"]);
        assert_ne!(rows[0]["object_instance"], rows[2]["object_instance"]);
        assert_eq!(rows[0]["namespace_hex"], json!(["72756e"]));
        assert_eq!(rows[3]["intact"], true);
        assert_eq!(rows[3]["producer_quiescence_proven"], false);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn terminal_progress_keeps_partial_bytes_distinct_from_unknown() {
        let (path, file) = new_test_file();
        let recorder = Arc::new(Recorder::new(2));
        let object = object();
        recorder.record_progress(Boundary::ReceiveTimeout, &object, 1, 10, Some(4));
        recorder.record(Boundary::ForwardTimeout, &object, 2, 11);
        test_trace(recorder, file).finish("sigterm").unwrap();
        let rows: Vec<serde_json::Value> = std::fs::read_to_string(&path).unwrap()
            .lines().map(|line| serde_json::from_str(line).unwrap()).collect();
        assert_eq!(rows[0]["received_payload_bytes"], 4);
        assert!(rows[1]["received_payload_bytes"].is_null());
        assert_eq!(rows[2]["written"], 2);
        std::fs::remove_file(path).unwrap();
    }

    fn publish_done(id: u64, code: u64) -> message::PublishDone {
        message::PublishDone { id, status_code: code, stream_count: 2,
                               reason: ReasonPhrase("upstream refused".to_string()) }
    }

    fn track(name: &str) -> Arc<Track> {
        Arc::new(Track::new(TrackNamespace::from_utf8_path("run"), name))
    }

    /// An object whose subgroup belongs to the incarnation `track`.
    fn object_of(track: &Arc<Track>, priority: u8) -> Arc<SubgroupObject> {
        let (mut writer, _reader) = SubgroupInfo {
            track: track.clone(), group_id: 0, subgroup_id: 0, priority,
        }.produce();
        writer.create(32, None).unwrap().info.clone()
    }

    #[test]
    fn schema_is_v3_and_meta_adds_control_link_mapping_and_rss_start_only() {
        let meta = meta_row("run", 99_232, DataPriorityMapping::MoqtV2, Some((2048, 77)));
        assert_eq!(SCHEMA, "relay-object-trace-v3");
        assert_eq!(meta["schema"], "relay-object-trace-v3");
        // v1 fields unchanged.
        for (key, value) in [("role", json!("trace_meta")), ("run_id", json!("run")),
                             ("clock", json!("monotonic_ns/1000")), ("capacity", json!(99_232)),
                             ("receive_progress", json!(true)),
                             ("scope", json!("sealed_callback_interval_not_producer_quiescence"))] {
            assert_eq!(meta[key], value, "{key}");
        }
        assert_eq!(meta["control_capacity"], 4096);
        assert_eq!(meta["link_capacity"], 4096);
        assert_eq!(meta["data_priority_mapping"], "moqt-v2");
        assert_eq!(meta["rss_sample_period_us"], 100_000);
        assert_eq!(meta["rss_start_kb"], 2048);
        assert_eq!(meta["rss_start_t_us"], 77);
        assert!(meta_row("run", 1, DataPriorityMapping::LegacyV1, None)["rss_start_kb"].is_null());
        assert_eq!(meta_row("run", 1, DataPriorityMapping::LegacyV1, None)["data_priority_mapping"],
                   "legacy-v1");
    }

    #[test]
    fn object_rows_carry_publisher_priority_and_mapped_quinn_priority_on_forward_only() {
        for (mapping, haptic_q, pc_q) in [(DataPriorityMapping::MoqtV2, 255, 254),
                                          (DataPriorityMapping::LegacyV1, 0, 1)] {
            let (path, file) = new_test_file();
            let recorder = Arc::new(Recorder::new(16));
            let haptic = object_with("haptic", 0);
            let pc = object_with("pc", 1);
            for object in [&haptic, &pc] {
                for boundary in [Boundary::ReceiveRegistered, Boundary::ReceiveComplete,
                                 Boundary::ForwardStart, Boundary::ForwardAccepted,
                                 Boundary::ForwardTimeout, Boundary::ForwardInterrupted] {
                    recorder.record(boundary, object, 3, 10);
                }
            }
            let mut trace = test_trace(recorder, file);
            trace.mapping = mapping;
            trace.finish("sigterm").unwrap();
            let rows = rows(&path);
            let objects: Vec<_> = rows.iter().filter(|r| r["role"] == "relay_object").collect();
            assert_eq!(objects.len(), 12);
            for row in objects {
                let haptic_row = row["track"] == "haptic";
                assert_eq!(row["publisher_priority"], if haptic_row { 0 } else { 1 });
                let forward = row["boundary"].as_str().unwrap().starts_with("forward_");
                if forward {
                    assert_eq!(row["quinn_priority_mapped"], if haptic_row { haptic_q } else { pc_q });
                } else {
                    assert!(row["quinn_priority_mapped"].is_null(), "{row}");
                }
                // v1 fields still present.
                for key in ["boundary", "t_us", "object_instance", "namespace_hex", "track_hex",
                            "track", "track_alias", "group_id", "subgroup_id", "object_id",
                            "size", "received_payload_bytes"] {
                    assert!(row.get(key).is_some(), "{key}");
                }
            }
            std::fs::remove_file(path).unwrap();
        }
    }

    /// The Codex 173c case: D1 SUB -> D1 OK(local) -> D2 SUB -> D2 OK -> U SUB.
    /// With bindings, D1 and D2 point at ONE incarnation, D1 created it (and
    /// so the upstream request), D2 reused it, and the upstream rows carry the
    /// same incarnation. A later, separate incarnation of the same name gets
    /// its own id. Object rows carry the incarnation of their subgroup.
    #[test]
    fn control_rows_join_each_request_to_its_track_incarnation_and_binding() {
        let (path, file) = new_test_file();
        let recorder = Arc::new(Recorder::new(4));
        let first = track("pc-d6");
        let second = track("pc-d6"); // same name, a later incarnation
        recorder.record(Boundary::ReceiveComplete, &object_of(&first, 1), 5, 9);
        // downstream session 2: D1 (id 4) creates, D2 (id 6) reuses
        recorder.record_link(TrackBinding::DownstreamCreated, 2, 4, &first, 99);
        recorder.record_control(ControlDirection::Received,
            ControlMessage::Subscribe(&subscribe(4, "pc-d6", Some(345))), 2, 100, None);
        recorder.record_control(ControlDirection::Sent,
            ControlMessage::SubscribeOk(&subscribe_ok(4, 4, Some(345))), 2, 110, Some(108));
        recorder.record_link(TrackBinding::DownstreamReused, 2, 6, &first, 119);
        recorder.record_control(ControlDirection::Received,
            ControlMessage::Subscribe(&subscribe(6, "pc-d6", Some(345))), 2, 120, None);
        recorder.record_control(ControlDirection::Sent,
            ControlMessage::SubscribeOk(&subscribe_ok(6, 6, Some(345))), 2, 130, Some(129));
        // upstream session 1: U (own id 5) for the first incarnation, refused
        recorder.record_link(TrackBinding::UpstreamSubscribe, 1, 5, &first, 139);
        recorder.record_control(ControlDirection::Sent,
            ControlMessage::Subscribe(&subscribe(5, "pc-d6", None)), 1, 140, Some(138));
        recorder.record_control(ControlDirection::Received,
            ControlMessage::RequestError(&request_error(5, 16)), 1, 150, None);
        // the refusal reaches the accepted downstream requests as PUBLISH_DONE
        recorder.record_control(ControlDirection::Sent,
            ControlMessage::PublishDone(&publish_done(4, 16)), 2, 160, Some(159));
        // a later incarnation of the same name, upstream own id 7 (a retry)
        recorder.record_link(TrackBinding::DownstreamCreated, 2, 8, &second, 169);
        recorder.record_link(TrackBinding::UpstreamSubscribe, 1, 7, &second, 170);
        recorder.record_control(ControlDirection::Sent,
            ControlMessage::Subscribe(&subscribe(7, "pc-d6", None)), 1, 171, Some(170));
        // a downstream refusal before any binding (relay: not found)
        recorder.record_control(ControlDirection::Received,
            ControlMessage::Subscribe(&subscribe(10, "nope", None)), 2, 180, None);
        recorder.record_control(ControlDirection::Sent,
            ControlMessage::RequestError(&request_error(10, 16)), 2, 181, Some(180));
        test_trace(recorder, file).finish("sigterm").unwrap();
        let rows = rows(&path);
        let control: Vec<_> = rows.iter().filter(|r| r["role"] == "relay_control").collect();
        let field = |i: usize, k: &str| control[i][k].clone();
        let ids: Vec<_> = (0..control.len()).map(|i| field(i, "track_incarnation")).collect();
        let bindings: Vec<_> = (0..control.len()).map(|i| field(i, "track_binding")).collect();
        assert_eq!(ids, vec![json!(1), json!(1), json!(1), json!(1), json!(1), json!(1),
                             json!(1), json!(2), json!(null), json!(null)]);
        assert_eq!(bindings, vec![json!("created"), json!("created"), json!("reused"),
                                  json!("reused"), json!("upstream_subscribe"),
                                  json!("upstream_subscribe"), json!("created"),
                                  json!("upstream_subscribe"), json!(null), json!(null)]);
        assert_eq!(field(0, "t_bind_us"), 99);
        assert_eq!(field(4, "t_bind_us"), 139);
        // sent rows carry the writer start; received rows have none
        assert_eq!((field(1, "t_send_begin_us"), field(1, "t_us")), (json!(108), json!(110)));
        assert!(field(0, "t_send_begin_us").is_null());
        assert_eq!(control[6].get("boundary").unwrap(), "publish_done_sent");
        assert_eq!((field(6, "status_code"), field(6, "stream_count"), field(6, "error_code")),
                   (json!(16), json!(2), json!(null)));
        assert_eq!(field(6, "reason"), "upstream refused");
        // the object row carries the incarnation of its subgroup
        let object = rows.iter().find(|r| r["role"] == "relay_object").unwrap();
        assert_eq!(object["track_incarnation"], 1);
        let end = rows.last().unwrap();
        assert_eq!((end["link_callbacks"].clone(), end["link_recorded"].clone(),
                    end["link_dropped"].clone(), end["link_duplicate_keys"].clone()),
                   (json!(5), json!(5), json!(0), json!(0)));
        assert_eq!((end["control_callbacks"].clone(), end["control_written"].clone()),
                   (json!(10), json!(10)));
        assert_eq!(end["intact"], true);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn control_rows_keep_the_v2_fields_and_object_counters_their_v1_meaning() {
        let (path, file) = new_test_file();
        let recorder = Arc::new(Recorder::new(4));
        let object = object();
        recorder.record(Boundary::ReceiveComplete, &object, 3, 10);
        let sub = subscribe(2, "pc-d6", Some(345));
        let err = request_error(3, 16);
        recorder.record_control(ControlDirection::Received, ControlMessage::Subscribe(&sub), 2, 20, None);
        recorder.record_control(ControlDirection::Received, ControlMessage::RequestError(&err), 1, 24, None);
        test_trace(recorder, file).finish("sigterm").unwrap();
        let rows = rows(&path);
        // The test file has no trace_meta row (written by `start` only).
        let roles: Vec<_> = rows.iter().map(|r| r["role"].as_str().unwrap()).collect();
        assert_eq!(roles, ["relay_object", "relay_control", "relay_control", "trace_end"]);
        assert_eq!(rows[1], json!({"role":"relay_control", "boundary":"subscribe_received",
            "t_us":20, "t_send_begin_us":null, "session":2, "request_id":2, "track_alias":null,
            "namespace_hex":["72756e"], "track_hex":hex::encode("pc-d6"), "track":"pc-d6",
            "delivery_timeout_ms":345, "params_decode_error":false,
            "error_code":null, "status_code":null, "stream_count":null, "reason":null,
            "track_incarnation":null, "track_binding":null, "t_bind_us":null}));
        assert_eq!(rows[2]["error_code"], 16);
        let end = &rows[3];
        assert_eq!((end["callbacks"].clone(), end["written"].clone(), end["dropped"].clone()),
                   (json!(1), json!(1), json!(0)));
        assert_eq!((end["control_callbacks"].clone(), end["control_written"].clone(),
                    end["control_dropped"].clone(), end["control_clock_errors"].clone()),
                   (json!(2), json!(2), json!(0), json!(0)));
        assert_eq!(end["clock_errors"], 0);
        assert_eq!(end["intact"], true);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn control_and_link_overflow_are_counted_and_fail_integrity() {
        for (control_cap, link_cap) in [(1, 4), (4, 1)] {
            let (path, file) = new_test_file();
            let recorder = Arc::new(Recorder::with_capacities(4, control_cap, link_cap));
            let sub = subscribe(2, "pc", None);
            let incarnation = track("pc");
            for t in 0..2 {
                recorder.record_control(ControlDirection::Received, ControlMessage::Subscribe(&sub),
                                        1, t, None);
                recorder.record_link(TrackBinding::DownstreamCreated, 1, 2 + 2 * t, &incarnation, t);
            }
            assert!(test_trace(recorder, file).finish("sigterm").is_err());
            let rows = rows(&path);
            let end = rows.last().unwrap();
            let expect_control_drop = u64::from(control_cap == 1);
            assert_eq!(end["control_dropped"], expect_control_drop);
            assert_eq!(end["link_dropped"], 1 - expect_control_drop);
            assert_eq!((end["dropped"].clone(), end["intact"].clone()), (json!(0), json!(false)));
            std::fs::remove_file(path).unwrap();
        }
    }

    #[test]
    fn a_duplicate_binding_of_one_request_fails_integrity_and_keeps_the_first() {
        let (path, file) = new_test_file();
        let recorder = Arc::new(Recorder::new(4));
        let (a, b) = (track("pc"), track("pc"));
        recorder.record_link(TrackBinding::DownstreamCreated, 2, 4, &a, 1);
        recorder.record_link(TrackBinding::DownstreamReused, 2, 4, &b, 2);
        recorder.record_control(ControlDirection::Received,
            ControlMessage::Subscribe(&subscribe(4, "pc", None)), 2, 3, None);
        assert!(test_trace(recorder, file).finish("sigterm").is_err());
        let rows = rows(&path);
        assert_eq!(rows[0]["track_binding"], "created");
        assert_eq!(rows.last().unwrap()["link_duplicate_keys"], 1);
        assert_eq!(rows.last().unwrap()["intact"], false);
        std::fs::remove_file(path).unwrap();
    }

    /// Codex 173a P2: control callbacks from two real threads at once must
    /// never lose a record while capacity remains (they wait for the short
    /// push-only lock); the object path still never waits.
    #[test]
    fn concurrent_control_and_link_callbacks_from_two_threads_lose_nothing() {
        let recorder = Arc::new(Recorder::with_capacities(4, 4096, 4096));
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let threads: Vec<_> = (0..2u64).map(|session| {
            let (recorder, barrier) = (recorder.clone(), barrier.clone());
            std::thread::spawn(move || {
                let incarnation = track("pc");
                barrier.wait();
                for id in 0..1000u64 {
                    let sub = subscribe(id, "pc", Some(345));
                    recorder.record_control(ControlDirection::Received,
                                            ControlMessage::Subscribe(&sub), session, id, None);
                    recorder.record_link(TrackBinding::DownstreamCreated, session, id,
                                         &incarnation, id);
                }
            })
        }).collect();
        for thread in threads { thread.join().unwrap(); }
        assert_eq!(recorder.control_callbacks.load(Ordering::SeqCst), 2000);
        assert_eq!(recorder.control_dropped.load(Ordering::SeqCst), 0);
        assert_eq!(recorder.link_dropped.load(Ordering::SeqCst), 0);
        let (_, control, links) = recorder.seal().unwrap();
        assert_eq!((control.len(), links.len()), (2000, 2000));
        // Holding the CONTROL lock does not make an object callback wait or drop.
        let recorder = Recorder::new(4);
        let guard = recorder.control_records.lock().unwrap();
        recorder.record(Boundary::ForwardStart, &object(), 0, 1);
        assert_eq!(recorder.dropped.load(Ordering::SeqCst), 0);
        drop(guard);
        // ...and the object path keeps its never-wait rule on its own lock.
        let guard = recorder.records.lock().unwrap();
        recorder.record(Boundary::ForwardStart, &object(), 0, 2);
        assert_eq!(recorder.dropped.load(Ordering::SeqCst), 1);
        drop(guard);
    }

    #[test]
    fn a_sealed_recorder_excludes_later_control_and_link_callbacks() {
        let recorder = Recorder::new(4);
        let sub = subscribe(2, "pc", None);
        recorder.record_control(ControlDirection::Received, ControlMessage::Subscribe(&sub), 1, 1, None);
        let (_, control, _) = recorder.seal().unwrap();
        assert_eq!(control.len(), 1);
        recorder.record_control(ControlDirection::Received, ControlMessage::Subscribe(&sub), 1, 2, None);
        recorder.record_link(TrackBinding::UpstreamSubscribe, 1, 3, &track("pc"), 3);
        assert_eq!(recorder.control_callbacks.load(Ordering::SeqCst), 1);
        assert_eq!(recorder.link_callbacks.load(Ordering::SeqCst), 0);
        assert_eq!(recorder.active.load(Ordering::SeqCst), 0);
    }

    /// Codex 173a P3: a control-clock failure is counted apart from the
    /// object clock (whose `clock_errors` keeps the v1 meaning) and fails
    /// integrity.
    #[test]
    fn a_control_clock_failure_is_counted_apart_from_object_clock_errors() {
        let (path, file) = new_test_file();
        let mut recorder = Recorder::new(4);
        recorder.control_clock = || None;
        let recorder = Arc::new(recorder);
        assert!(recorder.now_us() > 0, "the object clock still works");
        assert_eq!(recorder.control_now_us(), 0);
        recorder.record(Boundary::ReceiveComplete, &object(), 3, 10);
        test_trace(recorder.clone(), file).finish("sigterm").unwrap_err();
        let end = rows(&path).pop().unwrap();
        assert_eq!((end["clock_errors"].clone(), end["control_clock_errors"].clone()),
                   (json!(0), json!(1)));
        assert_eq!((end["callbacks"].clone(), end["written"].clone()), (json!(1), json!(1)));
        assert_eq!(end["intact"], false);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn rss_maximum_and_end_are_recorded_from_proc_status() {
        assert!(read_rss_kb().unwrap() > 0);
        let (path, file) = new_test_file();
        let trace = test_trace(Arc::new(Recorder::new(1)), file);
        let start = trace.rss.start.unwrap();
        std::thread::sleep(Duration::from_millis(350));
        trace.finish("sigterm").unwrap();
        let end = rows(&path).pop().unwrap();
        let samples = end["rss_samples"].as_u64().unwrap();
        assert!(samples >= 3, "start + periodic + end samples: {samples}");
        assert_eq!(end["rss_sample_errors"], 0);
        let (max, end_kb) = (end["rss_max_kb"].as_u64().unwrap(),
                             end["rss_end_kb"].as_u64().unwrap());
        assert!(max >= start.0 && max >= end_kb);
        assert!(end.get("rss_hwm_kb").is_none());
        assert!(end["rss_max_t_us"].as_u64().unwrap() >= start.1);
        assert!(end["rss_end_t_us"].as_u64().unwrap() >= end["rss_max_t_us"].as_u64().unwrap());
        std::fs::remove_file(path).unwrap();
    }

    /// Codex 173a P2: after a slow read (250 ms, > 2 periods) the next
    /// periodic sample starts no earlier than one full period after that read
    /// COMPLETED (before: 24.77 us later). Periodic samples really run here.
    #[test]
    fn a_slow_rss_read_is_never_followed_by_a_catch_up_sample() {
        let period = Duration::from_millis(40);
        let calls: Arc<Mutex<Vec<(Instant, Instant)>>> = Arc::new(Mutex::new(Vec::new()));
        let reader: RssReader = {
            let calls = calls.clone();
            Arc::new(move || {
                let begin = Instant::now();
                let n = calls.lock().unwrap().len();
                if n == 1 {
                    std::thread::sleep(Duration::from_millis(250)); // the first periodic read stalls
                }
                calls.lock().unwrap().push((begin, Instant::now()));
                Some(1000 + n as u64)
            })
        };
        let mut sampler = RssSampler::start_with(period, reader).unwrap();
        std::thread::sleep(Duration::from_millis(500));
        sampler.finish().unwrap();
        let calls = calls.lock().unwrap().clone();
        // start, >= 3 periodic (one slow), end
        assert!(calls.len() >= 5, "periodic samples ran: {}", calls.len());
        let periodic = &calls[1..calls.len() - 1];
        assert!(periodic[0].1 - periodic[0].0 >= Duration::from_millis(250), "the injected stall");
        for pair in periodic.windows(2) {
            let gap = pair[1].0.duration_since(pair[0].1);
            assert!(gap >= period, "next sample {gap:?} after the previous completed (< {period:?})");
        }
    }

    #[test]
    fn rss_sampler_stops_promptly_when_idle() {
        let mut sampler = RssSampler::start(Duration::from_secs(10)).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        let begin = Instant::now();
        let end = sampler.finish().unwrap();
        assert!(begin.elapsed() < Duration::from_millis(500), "join waits for unpark, not period");
        assert!(end.is_some());
        // start + end only: the 10 s period never elapsed.
        assert_eq!(sampler.stats.samples.load(Ordering::SeqCst), 2);
    }

    /// Teardown waits for a read already in progress (documented bound: one read).
    #[test]
    fn finish_waits_for_at_most_the_read_in_progress() {
        let calls = Arc::new(AtomicU64::new(0));
        let reader: RssReader = {
            let calls = calls.clone();
            Arc::new(move || {
                if calls.fetch_add(1, Ordering::SeqCst) == 1 {
                    std::thread::sleep(Duration::from_millis(200));
                }
                Some(1)
            })
        };
        let mut sampler = RssSampler::start_with(Duration::from_millis(20), reader).unwrap();
        std::thread::sleep(Duration::from_millis(60)); // the slow periodic read is in progress
        let begin = Instant::now();
        sampler.finish().unwrap();
        let waited = begin.elapsed();
        assert!(waited < Duration::from_millis(400), "bounded by the one read: {waited:?}");
    }
}
