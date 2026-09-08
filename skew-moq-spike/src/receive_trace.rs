//! Opt-in B1/frame receiver observations, bounded to short verification runs.
//! Derived from the validated relay recorder; separate schema, no hot-path file I/O.
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::Path,
    sync::{atomic::{AtomicBool, AtomicU64, Ordering}, Arc, Mutex},
    time::{Duration, Instant},
};

use moq_transport::{object_trace::{Boundary, Observer}, serve::SubgroupObject};
use serde_json::json;

const CAPACITY: usize = 4096;

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

struct Record {
    boundary: Boundary,
    object: Arc<SubgroupObject>,
    track_alias: u64,
    timestamp_us: u64,
    received_bytes: Option<usize>,
}

pub struct Recorder {
    enabled: AtomicBool,
    active: AtomicU64,
    callbacks: AtomicU64,
    dropped: AtomicU64,
    clock_errors: AtomicU64,
    records: Mutex<Vec<Record>>,
    capacity: usize,
}

impl Recorder {
    fn new(capacity: usize) -> Self {
        Self {
            enabled: AtomicBool::new(true), active: AtomicU64::new(0),
            callbacks: AtomicU64::new(0), dropped: AtomicU64::new(0),
            clock_errors: AtomicU64::new(0),
            records: Mutex::new(Vec::with_capacity(capacity)), capacity,
        }
    }

    fn seal(&self) -> anyhow::Result<Vec<Record>> {
        // Only callbacks admitted before this cutoff belong to the snapshot.
        // This is NOT a proof that the receiver's producer tasks were all joined.
        self.enabled.store(false, Ordering::SeqCst);
        let deadline = Instant::now() + Duration::from_millis(100);
        while self.active.load(Ordering::SeqCst) != 0 {
            anyhow::ensure!(Instant::now() < deadline, "object trace seal timeout");
            std::thread::yield_now();
        }
        let mut records = self.records.lock().map_err(|_| anyhow::anyhow!("trace poisoned"))?;
        Ok(std::mem::take(&mut *records))
    }
}

impl Observer for Recorder {
    fn now_us(&self) -> u64 {
        // Same OS clock and epoch as the Skew sender/receiver. Linux only.
        let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
        let rc = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
        if rc != 0 || ts.tv_sec < 0 || !(0..1_000_000_000).contains(&ts.tv_nsec) {
            self.clock_errors.fetch_add(1, Ordering::SeqCst);
            return 0;
        }
        ts.tv_sec as u64 * 1_000_000 + ts.tv_nsec as u64 / 1000
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
}

pub struct Trace {
    recorder: Arc<Recorder>,
    writer: BufWriter<File>,
}

impl Trace {
    pub fn start(path: &Path, run_id: &str) -> anyhow::Result<Self> {
        anyhow::ensure!(cfg!(target_os = "linux"), "object trace requires Linux CLOCK_MONOTONIC");
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        let mut writer = BufWriter::new(file);
        writeln!(writer, "{}", json!({"role":"trace_meta", "schema":"receiver-object-trace-v1",
            "run_id":run_id, "pid":std::process::id(),
            "clock":"monotonic_ns/1000", "capacity":CAPACITY,
            "receive_progress":true,
            "scope":"sealed_callback_interval_not_producer_quiescence"}))?;
        writer.flush()?;
        let recorder = Arc::new(Recorder::new(CAPACITY));
        moq_transport::object_trace::install(recorder.clone())
            .map_err(|_| anyhow::anyhow!("object trace already installed"))?;
        Ok(Self { recorder, writer })
    }

    pub fn finish(mut self, ending: &str) -> anyhow::Result<()> {
        let records = self.recorder.seal()?;
        // Arc references in records prevent address reuse. Export small opaque
        // IDs instead of pointers; namespace bytes retain exact tuple identity.
        let mut instances = HashMap::new();
        for record in &records {
            let object = &record.object;
            let next = instances.len() + 1;
            let instance = *instances.entry(Arc::as_ptr(object) as usize).or_insert(next);
            let namespace_hex: Vec<_> = object.namespace.fields.iter()
                .map(|field| encode_hex(&field.value)).collect();
            writeln!(self.writer, "{}", json!({"role":"receive_object",
                "boundary":record.boundary.as_str(), "t_us":record.timestamp_us,
                "object_instance":instance, "namespace_hex":namespace_hex,
                "track_hex":encode_hex(object.name.as_bytes()),
                "track":object.name.to_string_lossy(), "track_alias":record.track_alias,
                "group_id":object.group_id, "subgroup_id":object.subgroup_id,
                "object_id":object.object_id, "size":object.size,
                "received_payload_bytes":record.received_bytes}))?;
        }
        let callbacks = self.recorder.callbacks.load(Ordering::SeqCst);
        let dropped = self.recorder.dropped.load(Ordering::SeqCst);
        let clock_errors = self.recorder.clock_errors.load(Ordering::SeqCst);
        let intact = callbacks == records.len() as u64 && dropped == 0 && clock_errors == 0;
        writeln!(self.writer, "{}", json!({"role":"trace_end", "ending":ending,
            "sealed":true, "callbacks":callbacks, "written":records.len(),
            "dropped":dropped, "clock_errors":clock_errors, "intact":intact,
            "producer_quiescence_proven":false}))?;
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        anyhow::ensure!(intact, "object trace integrity failure");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use moq_transport::{coding::TrackNamespace, serve::{Track, SubgroupInfo}};

    fn object() -> Arc<SubgroupObject> {
        let (mut writer, _reader) = SubgroupInfo {
            track: Arc::new(Track::new(TrackNamespace::from_utf8_path("run"), "pc")),
            group_id: 0, subgroup_id: 0, priority: 0,
        }.produce();
        writer.create(32, None).unwrap().info.clone()
    }

    #[test]
    fn bounded_overflow_is_counted() {
        let recorder = Recorder::new(1);
        let object = object();
        for _ in 0..2 { recorder.record(Boundary::ForwardStart, &object, 0, 1); }
        assert_eq!(recorder.seal().unwrap().len(), 1);
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
        assert!(recorder.seal().unwrap().is_empty());
    }

    #[test]
    fn seal_excludes_later_callbacks_explicitly() {
        let recorder = Recorder::new(2);
        let object = object();
        recorder.record(Boundary::ReceiveComplete, &object, 0, 1);
        assert_eq!(recorder.seal().unwrap().len(), 1);
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
        let records = recorder.seal().unwrap();
        assert_eq!(recorder.callbacks.load(Ordering::SeqCst), 1024);
        assert_eq!(records.len() as u64 + recorder.dropped.load(Ordering::SeqCst), 1024);
    }

    fn new_test_file() -> (std::path::PathBuf, File) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!("skew-receiver-trace-test-{}-{}-{}",
            std::process::id(), Recorder::new(0).now_us(), NEXT.fetch_add(1, Ordering::SeqCst)));
        let file = OpenOptions::new().create_new(true).write(true).open(&path).unwrap();
        (path, file)
    }

    #[test]
    fn existing_evidence_is_never_overwritten() {
        let (path, mut file) = new_test_file();
        file.write_all(b"preserved").unwrap();
        assert!(Trace::start(&path, "run").is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"preserved");
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn finalization_propagates_write_errors() {
        let (path, file) = new_test_file();
        drop(file);
        let trace = Trace { recorder: Arc::new(Recorder::new(4)),
            writer: BufWriter::new(File::open(&path).unwrap()) }; // read-only FD
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
        Trace { recorder, writer: BufWriter::new(file) }.finish("sigterm").unwrap();
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
        Trace { recorder, writer: BufWriter::new(file) }.finish("sigterm").unwrap();
        let rows: Vec<serde_json::Value> = std::fs::read_to_string(&path).unwrap()
            .lines().map(|line| serde_json::from_str(line).unwrap()).collect();
        assert_eq!(rows[0]["received_payload_bytes"], 4);
        assert!(rows[1]["received_payload_bytes"].is_null());
        assert_eq!(rows[2]["written"], 2);
        std::fs::remove_file(path).unwrap();
    }
}

