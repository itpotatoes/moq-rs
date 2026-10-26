//! Bounded verification-only recorder. No file I/O on transport callbacks.
//!
//! Schema `relay-object-trace-v2` = v1 rows and fields unchanged, plus:
//! `publisher_priority` / `quinn_priority_mapped` on every `relay_object` row,
//! `relay_control` rows (SUBSCRIBE / SUBSCRIBE_OK / REQUEST_ERROR at each
//! session's control stream) in their own bounded buffer, and relay RSS
//! (`/proc/self/status` VmRSS, sampled on a separate OS thread) on
//! `trace_meta` (start) and `trace_end` (sampled maximum and end). Kernel
//! VmHWM is not exported: on this host it read below an earlier VmRSS sample.
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
    object_trace::{Boundary, ControlDirection, ControlMessage, Observer},
    serve::SubgroupObject,
};
use serde_json::json;

const CAPACITY: usize = 4096;

pub const SCHEMA: &str = "relay-object-trace-v2";
/// Fixed bound of `relay_control` rows, independent of `--object-trace-capacity`.
pub const CONTROL_CAPACITY: usize = 4096;
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
    session: u64,
    timestamp_us: u64,
    request_id: u64,
    track_alias: Option<u64>,
    namespace: Option<TrackNamespace>,
    track_name: Option<TrackName>,
    /// Some(Some(ms)) present, Some(None) absent, None not applicable.
    delivery_timeout_ms: Option<Option<u64>>,
    params_decode_error: Option<bool>,
    error_code: Option<u64>,
    reason: Option<String>,
}

impl ControlRecord {
    fn new(direction: ControlDirection, message: ControlMessage<'_>, session: u64,
           timestamp_us: u64) -> Self {
        let mut record = Self {
            boundary: message.boundary(direction), session, timestamp_us, request_id: 0,
            track_alias: None, namespace: None, track_name: None, delivery_timeout_ms: None,
            params_decode_error: None, error_code: None, reason: None,
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
    clock_errors: AtomicU64,
    records: Mutex<Vec<Record>>,
    capacity: usize,
    control_callbacks: AtomicU64,
    control_dropped: AtomicU64,
    control_records: Mutex<Vec<ControlRecord>>,
    control_capacity: usize,
}

impl Recorder {
    fn new(capacity: usize) -> Self {
        Self::with_control_capacity(capacity, CONTROL_CAPACITY)
    }

    fn with_control_capacity(capacity: usize, control_capacity: usize) -> Self {
        Self {
            enabled: AtomicBool::new(true), active: AtomicU64::new(0),
            callbacks: AtomicU64::new(0), dropped: AtomicU64::new(0),
            clock_errors: AtomicU64::new(0),
            records: Mutex::new(Vec::with_capacity(capacity)), capacity,
            control_callbacks: AtomicU64::new(0), control_dropped: AtomicU64::new(0),
            control_records: Mutex::new(Vec::with_capacity(control_capacity)), control_capacity,
        }
    }

    fn seal(&self) -> anyhow::Result<(Vec<Record>, Vec<ControlRecord>)> {
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
        Ok((std::mem::take(&mut *records), std::mem::take(&mut *control)))
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
                      session: u64, timestamp_us: u64) {
        self.active.fetch_add(1, Ordering::SeqCst);
        if self.enabled.load(Ordering::SeqCst) {
            self.control_callbacks.fetch_add(1, Ordering::SeqCst);
            // Own buffer and lock: a control callback never contends with an
            // object callback. Same rule otherwise: never wait, count losses.
            match self.control_records.try_lock() {
                Ok(mut records) if records.len() < self.control_capacity => records.push(
                    ControlRecord::new(direction, message, session, timestamp_us)),
                _ => { self.control_dropped.fetch_add(1, Ordering::SeqCst); }
            }
        }
        self.active.fetch_sub(1, Ordering::SeqCst);
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

#[derive(Default)]
struct RssStats {
    samples: AtomicU64,
    errors: AtomicU64,
    max_kb: AtomicU64,
    max_t_us: AtomicU64,
}

/// A sample: (VmRSS kB, t_us).
type RssSample = (u64, u64);

impl RssStats {
    /// Only one writer at a time: `start`, then the sampler thread, then
    /// `finish` after the thread was joined.
    fn sample(&self) -> Option<RssSample> {
        let sample = monotonic_us().and_then(|t| read_rss_kb().map(|rss| (rss, t)));
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

struct RssSampler {
    stats: Arc<RssStats>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
    start: Option<RssSample>,
}

impl RssSampler {
    fn start(period: Duration) -> anyhow::Result<Self> {
        let stats = Arc::new(RssStats::default());
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
                    next += period;
                    if next <= now {
                        next = now + period; // never burst to catch up
                    }
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
        let (records, control) = self.recorder.seal()?;
        let rss_end = self.rss.finish()?;
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
            writeln!(self.writer, "{}", json!({"role":"relay_object",
                "boundary":record.boundary.as_str(), "t_us":record.timestamp_us,
                "object_instance":instance, "namespace_hex":namespace_hex,
                "track_hex":hex::encode(object.name.as_bytes()),
                "track":object.name.to_string_lossy(), "track_alias":record.track_alias,
                "group_id":object.group_id, "subgroup_id":object.subgroup_id,
                "object_id":object.object_id, "size":object.size,
                "received_payload_bytes":record.received_bytes,
                "publisher_priority":priority, "quinn_priority_mapped":quinn}))?;
        }
        for record in &control {
            let namespace_hex: Option<Vec<_>> = record.namespace.as_ref().map(|namespace|
                namespace.fields.iter().map(|field| hex::encode(&field.value)).collect());
            writeln!(self.writer, "{}", json!({"role":"relay_control",
                "boundary":record.boundary, "t_us":record.timestamp_us,
                "session":record.session, "request_id":record.request_id,
                "track_alias":record.track_alias, "namespace_hex":namespace_hex,
                "track_hex":record.track_name.as_ref().map(|name| hex::encode(name.as_bytes())),
                "track":record.track_name.as_ref().map(|name| name.to_string_lossy()),
                "delivery_timeout_ms":record.delivery_timeout_ms.flatten(),
                "params_decode_error":record.params_decode_error,
                "error_code":record.error_code, "reason":record.reason}))?;
        }
        let callbacks = self.recorder.callbacks.load(Ordering::SeqCst);
        let dropped = self.recorder.dropped.load(Ordering::SeqCst);
        let clock_errors = self.recorder.clock_errors.load(Ordering::SeqCst);
        let control_callbacks = self.recorder.control_callbacks.load(Ordering::SeqCst);
        let control_dropped = self.recorder.control_dropped.load(Ordering::SeqCst);
        let control_intact = control_callbacks == control.len() as u64 && control_dropped == 0;
        let intact = callbacks == records.len() as u64 && dropped == 0 && clock_errors == 0
            && control_intact;
        let stats = &self.rss.stats;
        let samples = stats.samples.load(Ordering::SeqCst);
        writeln!(self.writer, "{}", json!({"role":"trace_end", "ending":ending,
            "sealed":true, "callbacks":callbacks, "written":records.len(),
            "dropped":dropped, "clock_errors":clock_errors, "intact":intact,
            "producer_quiescence_proven":false,
            "control_callbacks":control_callbacks, "control_written":control.len(),
            "control_dropped":control_dropped,
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

    #[test]
    fn schema_is_v2_and_meta_adds_control_mapping_and_rss_start_only() {
        let meta = meta_row("run", 99_232, DataPriorityMapping::MoqtV2, Some((2048, 77)));
        assert_eq!(SCHEMA, "relay-object-trace-v2");
        assert_eq!(meta["schema"], "relay-object-trace-v2");
        // v1 fields unchanged.
        for (key, value) in [("role", json!("trace_meta")), ("run_id", json!("run")),
                             ("clock", json!("monotonic_ns/1000")), ("capacity", json!(99_232)),
                             ("receive_progress", json!(true)),
                             ("scope", json!("sealed_callback_interval_not_producer_quiescence"))] {
            assert_eq!(meta[key], value, "{key}");
        }
        assert_eq!(meta["control_capacity"], 4096);
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

    #[test]
    fn control_rows_follow_object_rows_with_their_own_counters() {
        let (path, file) = new_test_file();
        let recorder = Arc::new(Recorder::new(4));
        let object = object();
        recorder.record(Boundary::ReceiveComplete, &object, 3, 10);
        let sub = subscribe(2, "pc-d6", Some(345));
        let upstream = subscribe(1, "pc-d6", None);
        let ok = subscribe_ok(1, 1, None);
        let down_ok = subscribe_ok(2, 2, Some(345));
        let err = request_error(3, 16);
        recorder.record_control(ControlDirection::Received, ControlMessage::Subscribe(&sub), 2, 20);
        recorder.record_control(ControlDirection::Sent, ControlMessage::SubscribeOk(&down_ok), 2, 21);
        recorder.record_control(ControlDirection::Sent, ControlMessage::Subscribe(&upstream), 1, 22);
        recorder.record_control(ControlDirection::Received, ControlMessage::SubscribeOk(&ok), 1, 23);
        recorder.record_control(ControlDirection::Received, ControlMessage::RequestError(&err), 1, 24);
        recorder.record_control(ControlDirection::Sent, ControlMessage::RequestError(&err), 2, 25);
        test_trace(recorder, file).finish("sigterm").unwrap();
        let rows = rows(&path);
        let roles: Vec<_> = rows.iter().map(|r| r["role"].as_str().unwrap()).collect();
        // The test file has no trace_meta row (written by `start` only).
        assert_eq!(roles, ["relay_object", "relay_control", "relay_control", "relay_control",
                           "relay_control", "relay_control", "relay_control", "trace_end"]);
        let control = &rows[1..7];
        let boundaries: Vec<_> = control.iter().map(|r| r["boundary"].as_str().unwrap()).collect();
        assert_eq!(boundaries, ["subscribe_received", "subscribe_ok_sent", "subscribe_sent",
                                "subscribe_ok_received", "request_error_received",
                                "request_error_sent"]);
        assert_eq!(control[0], json!({"role":"relay_control", "boundary":"subscribe_received",
            "t_us":20, "session":2, "request_id":2, "track_alias":null,
            "namespace_hex":["72756e"], "track_hex":hex::encode("pc-d6"), "track":"pc-d6",
            "delivery_timeout_ms":345, "params_decode_error":false,
            "error_code":null, "reason":null}));
        assert_eq!(control[1]["track_alias"], 2);
        assert_eq!(control[1]["delivery_timeout_ms"], 345);
        assert!(control[1]["track"].is_null() && control[1]["namespace_hex"].is_null());
        assert!(control[2]["delivery_timeout_ms"].is_null());
        assert_eq!(control[2]["params_decode_error"], false);
        assert_eq!(control[3]["request_id"], 1);
        assert_eq!(control[3]["track_alias"], 1);
        assert_eq!(control[4], json!({"role":"relay_control", "boundary":"request_error_received",
            "t_us":24, "session":1, "request_id":3, "track_alias":null, "namespace_hex":null,
            "track_hex":null, "track":null, "delivery_timeout_ms":null,
            "params_decode_error":null, "error_code":16, "reason":"track not found"}));
        let end = &rows[7];
        // Object counters keep their v1 meaning (object rows only).
        assert_eq!((end["callbacks"].clone(), end["written"].clone(), end["dropped"].clone()),
                   (json!(1), json!(1), json!(0)));
        assert_eq!((end["control_callbacks"].clone(), end["control_written"].clone(),
                    end["control_dropped"].clone()), (json!(6), json!(6), json!(0)));
        assert_eq!(end["intact"], true);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn control_overflow_is_counted_and_fails_integrity() {
        let (path, file) = new_test_file();
        let recorder = Arc::new(Recorder::with_control_capacity(4, 1));
        let sub = subscribe(2, "pc", None);
        for t in 0..2 {
            recorder.record_control(ControlDirection::Received, ControlMessage::Subscribe(&sub), 1, t);
        }
        assert!(test_trace(recorder, file).finish("sigterm").is_err());
        let rows = rows(&path);
        let end = rows.last().unwrap();
        assert_eq!(rows.iter().filter(|r| r["role"] == "relay_control").count(), 1);
        assert_eq!((end["control_callbacks"].clone(), end["control_written"].clone(),
                    end["control_dropped"].clone()), (json!(2), json!(1), json!(1)));
        assert_eq!((end["dropped"].clone(), end["intact"].clone()), (json!(0), json!(false)));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn control_and_object_buffers_never_contend_and_never_block() {
        let recorder = Recorder::new(4);
        let sub = subscribe(2, "pc", None);
        let control_guard = recorder.control_records.lock().unwrap();
        recorder.record(Boundary::ForwardStart, &object(), 0, 1);
        recorder.record_control(ControlDirection::Received, ControlMessage::Subscribe(&sub), 1, 2);
        assert_eq!(recorder.dropped.load(Ordering::SeqCst), 0);
        assert_eq!(recorder.control_dropped.load(Ordering::SeqCst), 1);
        drop(control_guard);
        let object_guard = recorder.records.lock().unwrap();
        recorder.record_control(ControlDirection::Received, ControlMessage::Subscribe(&sub), 1, 3);
        assert_eq!(recorder.control_dropped.load(Ordering::SeqCst), 1);
        drop(object_guard);
        let (records, control) = recorder.seal().unwrap();
        assert_eq!((records.len(), control.len()), (1, 1));
        // Sealed: later control callbacks are excluded, not counted.
        recorder.record_control(ControlDirection::Received, ControlMessage::Subscribe(&sub), 1, 4);
        assert_eq!(recorder.control_callbacks.load(Ordering::SeqCst), 2);
        assert_eq!(recorder.active.load(Ordering::SeqCst), 0);
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

    #[test]
    fn rss_sampler_stops_promptly_and_never_bursts() {
        let mut sampler = RssSampler::start(Duration::from_secs(10)).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        let begin = Instant::now();
        let end = sampler.finish().unwrap();
        assert!(begin.elapsed() < Duration::from_millis(500), "join waits for unpark, not period");
        assert!(end.is_some());
        // start + end only: the 10 s period never elapsed.
        assert_eq!(sampler.stats.samples.load(Ordering::SeqCst), 2);
    }
}
