//! Opt-in relay live counters (`--live-counters-out`): cumulative per-track
//! object-boundary counts, written as one JSONL snapshot line per period by a
//! separate OS thread, plus one final line at shutdown.
//!
//! Hot path: an object callback does a lock-free lookup of its track
//! incarnation in a fixed slot array (CAS claim on first sight) and atomic
//! increments; it never waits for the writer or the disk. Counts are taken
//! inside the SAME admission gate as the object trace, so the final line
//! equals the trace's per-boundary row counts per incarnation whenever the
//! trace is intact (trace rows can be lost on `try_lock` contention; these
//! counters cannot).
//!
//! Line (`relay-live-counters-v1`), every line including the final one:
//! `{"schema","seq","t_us","final","period_us","write_errors","clock_errors",
//!   "untracked_events","tracks":[{"namespace_hex","track","track_incarnation",
//!   "registered","receive_complete","receive_timeout","receive_interrupted",
//!   "forward_started","forward_accepted","forward_timeout","forward_interrupted",
//!   "first_registered_t_us","last_registered_t_us"}]}`
//!
//! Reader rules: only `\n`-terminated lines count (an unterminated tail is a
//! write in progress); a terminated line that does not parse is a write fault
//! (skip it — after a failed write the next line starts with `\n`, so a
//! fragment never merges with it); `seq` increases by exactly 1 per line
//! attempted, so a gap is a lost line; per `track_incarnation` every count is
//! non-decreasing; exactly one line has `final: true` and it is the last —
//! without it the relay did not reach its shutdown; `write_errors` and
//! `clock_errors` on a line count failures before that line.
use std::{
    fs::OpenOptions,
    io::Write,
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, OnceLock,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use moq_transport::{object_trace::Boundary, serve::Track};
use serde_json::json;

pub const SCHEMA: &str = "relay-live-counters-v1";
/// One slot per track incarnation (not per object).
pub const REGISTRY_CAPACITY: usize = 4096;

/// Boundary names in count-array order (the object trace's boundary names,
/// with the trace's `receive_registered`/`forward_start` spelled
/// `registered`/`forward_started` as the sidecar contract asks).
pub const COUNT_NAMES: [&str; 8] = [
    "registered", "receive_complete", "receive_timeout", "receive_interrupted",
    "forward_started", "forward_accepted", "forward_timeout", "forward_interrupted",
];

pub fn count_index(boundary: Boundary) -> usize {
    match boundary {
        Boundary::ReceiveRegistered => 0,
        Boundary::ReceiveComplete => 1,
        Boundary::ReceiveTimeout => 2,
        Boundary::ReceiveInterrupted => 3,
        Boundary::ForwardStart => 4,
        Boundary::ForwardAccepted => 5,
        Boundary::ForwardTimeout => 6,
        Boundary::ForwardInterrupted => 7,
    }
}

#[derive(Default)]
struct Slot {
    /// `Arc::as_ptr` of the incarnation; 0 = free. Set once, never cleared.
    key: AtomicUsize,
    /// The incarnation itself, kept so its address cannot be reused.
    track: OnceLock<Arc<Track>>,
    counts: [AtomicU64; 8],
    /// `receive_registered` callback timestamps; 0 = none.
    first_registered: AtomicU64,
    last_registered: AtomicU64,
}

/// Lock-free incarnation registry; ids are slot index + 1 (first observed).
pub struct Registry {
    slots: Box<[Slot]>,
    untracked_events: AtomicU64,
}

/// One track's counts at a snapshot.
#[derive(Debug, Clone)]
pub struct TrackCounts {
    pub id: usize,
    pub track: Arc<Track>,
    pub counts: [u64; 8],
    pub first_registered_t_us: Option<u64>,
    pub last_registered_t_us: Option<u64>,
}

impl Registry {
    pub fn new(capacity: usize) -> Self {
        Self {
            slots: (0..capacity).map(|_| Slot::default()).collect(),
            untracked_events: AtomicU64::new(0),
        }
    }

    /// Find or claim the slot of `track` without locks. Claims fill a prefix
    /// of the array (a claim takes the first free slot it sees), so a scan
    /// stops at the first free slot. None when every slot is taken.
    fn slot(&self, track: &Arc<Track>) -> Option<(usize, &Slot)> {
        let key = Arc::as_ptr(track) as usize;
        for (index, slot) in self.slots.iter().enumerate() {
            match slot.key.load(Ordering::Acquire) {
                k if k == key => return Some((index, slot)),
                0 => match slot.key.compare_exchange(0, key, Ordering::AcqRel, Ordering::Acquire) {
                    Ok(_) => {
                        let _ = slot.track.set(track.clone());
                        return Some((index, slot));
                    }
                    Err(k) if k == key => return Some((index, slot)),
                    Err(_) => continue, // another incarnation took it first
                },
                _ => continue,
            }
        }
        None
    }

    /// Assign `track` its id now (first observation), if there is room.
    pub fn register(&self, track: &Arc<Track>) -> Option<usize> {
        self.slot(track).map(|(index, _)| index + 1)
    }

    /// The id of an already registered incarnation (no claim).
    pub fn id_of(&self, track: &Arc<Track>) -> Option<usize> {
        let key = Arc::as_ptr(track) as usize;
        for (index, slot) in self.slots.iter().enumerate() {
            match slot.key.load(Ordering::Acquire) {
                0 => return None,
                k if k == key => return Some(index + 1),
                _ => {}
            }
        }
        None
    }

    /// Count one object boundary of `track` observed at `timestamp_us`.
    pub fn count(&self, boundary: Boundary, track: &Arc<Track>, timestamp_us: u64) {
        let Some((_, slot)) = self.slot(track) else {
            self.untracked_events.fetch_add(1, Ordering::Relaxed);
            return;
        };
        slot.counts[count_index(boundary)].fetch_add(1, Ordering::Relaxed);
        if boundary == Boundary::ReceiveRegistered && timestamp_us != 0 {
            let _ = slot.first_registered.compare_exchange(0, timestamp_us, Ordering::Relaxed, Ordering::Relaxed);
            slot.last_registered.fetch_max(timestamp_us, Ordering::Relaxed);
        }
    }

    pub fn untracked_events(&self) -> u64 {
        self.untracked_events.load(Ordering::Relaxed)
    }

    /// Current counts of every visible incarnation, in id order.
    pub fn snapshot(&self) -> Vec<TrackCounts> {
        let mut out = Vec::new();
        for (index, slot) in self.slots.iter().enumerate() {
            if slot.key.load(Ordering::Acquire) == 0 {
                break;
            }
            // A claim whose track is not yet published appears next time.
            let Some(track) = slot.track.get() else { continue };
            let nonzero = |v: u64| (v != 0).then_some(v);
            out.push(TrackCounts {
                id: index + 1,
                track: track.clone(),
                counts: std::array::from_fn(|i| slot.counts[i].load(Ordering::Relaxed)),
                first_registered_t_us: nonzero(slot.first_registered.load(Ordering::Relaxed)),
                last_registered_t_us: nonzero(slot.last_registered.load(Ordering::Relaxed)),
            });
        }
        out
    }
}

/// The sidecar file and its line state; touched only by the writer thread
/// and, after it is joined, by `finish`.
struct Sink {
    out: Box<dyn Write + Send>,
    seq: u64,
    write_errors: u64,
    clock_errors: u64,
    /// After a failed write: start the next line with `\n` so a fragment of
    /// the failed line stays on its own (unparseable) line.
    need_newline: bool,
}

fn line(registry: &Registry, seq: u64, t_us: Option<u64>, last: bool, period: Duration,
        write_errors: u64, clock_errors: u64) -> serde_json::Value {
    let tracks: Vec<_> = registry.snapshot().into_iter().map(|t| {
        let mut entry = serde_json::Map::new();
        entry.insert("namespace_hex".into(), json!(t.track.namespace.fields.iter()
            .map(|field| hex::encode(&field.value)).collect::<Vec<_>>()));
        entry.insert("track".into(), json!(t.track.name.to_string_lossy()));
        entry.insert("track_incarnation".into(), json!(t.id));
        for (name, value) in COUNT_NAMES.iter().zip(t.counts) {
            entry.insert((*name).into(), json!(value));
        }
        entry.insert("first_registered_t_us".into(), json!(t.first_registered_t_us));
        entry.insert("last_registered_t_us".into(), json!(t.last_registered_t_us));
        serde_json::Value::Object(entry)
    }).collect();
    json!({"schema": SCHEMA, "seq": seq, "t_us": t_us, "final": last,
           "period_us": period.as_micros() as u64, "write_errors": write_errors,
           "clock_errors": clock_errors, "untracked_events": registry.untracked_events(),
           "tracks": tracks})
}

impl Sink {
    fn write_line(&mut self, registry: &Registry, period: Duration, last: bool) {
        let t_us = crate::object_trace::monotonic_us();
        if t_us.is_none() {
            self.clock_errors += 1;
        }
        let value = line(registry, self.seq, t_us, last, period, self.write_errors, self.clock_errors);
        self.seq += 1;
        let mut bytes = Vec::new();
        if self.need_newline {
            bytes.push(b'\n');
        }
        bytes.extend_from_slice(value.to_string().as_bytes());
        bytes.push(b'\n');
        match self.out.write_all(&bytes).and_then(|()| self.out.flush()) {
            Ok(()) => self.need_newline = false,
            Err(_) => {
                self.write_errors += 1;
                self.need_newline = true;
            }
        }
    }
}

/// The periodic writer. Spacing: the next line is due one `period` after the
/// previous line's write+flush COMPLETED, so a slow disk never causes a
/// catch-up burst.
pub struct LiveWriter {
    registry: Arc<Registry>,
    sink: Arc<Mutex<Sink>>,
    period: Duration,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl LiveWriter {
    /// Create `path` (create-only: an existing file is an error, untouched)
    /// and start the writer thread.
    pub fn start(path: &Path, period: Duration, registry: Arc<Registry>) -> anyhow::Result<Self> {
        anyhow::ensure!(!period.is_zero(), "the live counter period must be positive");
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        Self::start_with(Box::new(file), period, registry)
    }

    pub fn start_with(out: Box<dyn Write + Send>, period: Duration, registry: Arc<Registry>)
        -> anyhow::Result<Self> {
        let sink = Arc::new(Mutex::new(Sink { out, seq: 0, write_errors: 0, clock_errors: 0,
                                              need_newline: false }));
        let stop = Arc::new(AtomicBool::new(false));
        let thread = {
            let (registry, sink, stop) = (registry.clone(), sink.clone(), stop.clone());
            std::thread::Builder::new().name("relay-live-counters".into()).spawn(move || {
                let mut next = Instant::now() + period;
                while !stop.load(Ordering::SeqCst) {
                    let now = Instant::now();
                    if now < next {
                        std::thread::park_timeout(next - now);
                        continue;
                    }
                    if let Ok(mut sink) = sink.lock() {
                        sink.write_line(&registry, period, false);
                    }
                    next = Instant::now() + period; // after completion: no catch-up
                }
            })?
        };
        Ok(Self { registry, sink, period, stop, thread: Some(thread) })
    }

    /// Stop the periodic writer and write the final line. Call only after
    /// the recorder closed admission, so the final counts are complete.
    /// Waits for a write in progress (no absolute wall-time bound).
    pub fn finish(mut self) -> anyhow::Result<LiveSummary> {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            thread.thread().unpark();
            thread.join().map_err(|_| anyhow::anyhow!("live counter writer panicked"))?;
        }
        let mut sink = self.sink.lock().map_err(|_| anyhow::anyhow!("live counter sink poisoned"))?;
        sink.write_line(&self.registry, self.period, true);
        Ok(LiveSummary { lines: sink.seq, write_errors: sink.write_errors,
                         clock_errors: sink.clock_errors })
    }
}

impl Drop for LiveWriter {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = &self.thread {
            thread.thread().unpark();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveSummary {
    pub lines: u64,
    pub write_errors: u64,
    pub clock_errors: u64,
}

/// Parse a sidecar file's bytes by the reader rules: complete lines only,
/// unparseable complete lines counted, the unterminated tail ignored.
#[cfg(test)]
pub fn read_complete_lines(bytes: &[u8]) -> (Vec<serde_json::Value>, usize) {
    let complete = match bytes.iter().rposition(|b| *b == b'\n') {
        Some(end) => &bytes[..=end],
        None => &bytes[..0],
    };
    let mut lines = Vec::new();
    let mut bad = 0;
    for raw in complete.split(|b| *b == b'\n') {
        if raw.is_empty() {
            continue;
        }
        match serde_json::from_slice::<serde_json::Value>(raw) {
            Ok(value) => lines.push(value),
            Err(_) => bad += 1,
        }
    }
    (lines, bad)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use moq_transport::coding::TrackNamespace;

    /// An in-memory sink shared with the test.
    #[derive(Clone, Default)]
    pub(crate) struct SharedSink(pub Arc<Mutex<Vec<u8>>>);

    impl Write for SharedSink {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl SharedSink {
        pub(crate) fn lines(&self) -> (Vec<serde_json::Value>, usize) {
            read_complete_lines(&self.0.lock().unwrap())
        }
    }

    fn track(name: &str) -> Arc<Track> {
        Arc::new(Track::new(TrackNamespace::from_utf8_path("run"), name))
    }

    #[test]
    fn concurrent_counting_from_two_threads_loses_nothing_and_claims_one_slot() {
        let registry = Arc::new(Registry::new(16));
        let shared = track("pc");
        let threads: Vec<_> = (0..2).map(|_| {
            let (registry, shared) = (registry.clone(), shared.clone());
            std::thread::spawn(move || {
                for i in 0..10_000u64 {
                    registry.count(Boundary::ForwardStart, &shared, 1 + i);
                    registry.count(Boundary::ReceiveRegistered, &shared, 1 + i);
                }
            })
        }).collect();
        for thread in threads { thread.join().unwrap(); }
        let snapshot = registry.snapshot();
        assert_eq!(snapshot.len(), 1, "one incarnation, one slot");
        assert_eq!(snapshot[0].counts[count_index(Boundary::ForwardStart)], 20_000);
        assert_eq!(snapshot[0].counts[count_index(Boundary::ReceiveRegistered)], 20_000);
        assert_eq!(snapshot[0].first_registered_t_us, Some(1));
        assert_eq!(snapshot[0].last_registered_t_us, Some(10_000));
        assert_eq!(registry.untracked_events(), 0);
    }

    #[test]
    fn concurrent_first_sight_of_many_incarnations_never_duplicates_a_slot() {
        let registry = Arc::new(Registry::new(64));
        let tracks: Arc<Vec<_>> = Arc::new((0..32).map(|i| track(&format!("t{i}"))).collect());
        let threads: Vec<_> = (0..4).map(|_| {
            let (registry, tracks) = (registry.clone(), tracks.clone());
            std::thread::spawn(move || {
                for track in tracks.iter() {
                    registry.count(Boundary::ReceiveComplete, track, 5);
                }
            })
        }).collect();
        for thread in threads { thread.join().unwrap(); }
        let snapshot = registry.snapshot();
        assert_eq!(snapshot.len(), 32);
        assert!(snapshot.iter().all(|t| t.counts[1] == 4), "each counted once per thread");
        assert_eq!(snapshot.iter().map(|t| t.id).collect::<Vec<_>>(), (1..=32).collect::<Vec<_>>());
    }

    #[test]
    fn a_full_registry_counts_untracked_events_and_says_so_on_every_line() {
        let registry = Arc::new(Registry::new(2));
        let (a, b, c) = (track("a"), track("b"), track("c"));
        registry.count(Boundary::ReceiveRegistered, &a, 1);
        registry.count(Boundary::ReceiveRegistered, &b, 2);
        registry.count(Boundary::ReceiveRegistered, &c, 3);
        registry.count(Boundary::ForwardStart, &c, 4);
        assert_eq!(registry.untracked_events(), 2);
        let sink = SharedSink::default();
        LiveWriter::start_with(Box::new(sink.clone()), Duration::from_secs(60), registry)
            .unwrap().finish().unwrap();
        let (lines, bad) = sink.lines();
        assert_eq!(bad, 0);
        assert_eq!(lines.last().unwrap()["untracked_events"], 2);
        assert_eq!(lines.last().unwrap()["tracks"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn a_zero_timestamp_is_not_a_registration_time() {
        let registry = Registry::new(4);
        let t = track("pc");
        registry.count(Boundary::ReceiveRegistered, &t, 0); // a clock failure
        assert_eq!(registry.snapshot()[0].first_registered_t_us, None);
        registry.count(Boundary::ReceiveRegistered, &t, 7);
        registry.count(Boundary::ReceiveRegistered, &t, 5);
        let s = registry.snapshot();
        assert_eq!((s[0].first_registered_t_us, s[0].last_registered_t_us), (Some(7), Some(7)));
        assert_eq!(s[0].counts[0], 3, "every registration is counted");
    }

    /// A sink whose writes are slow: the hot path must not wait for it.
    struct SlowSink { delay: Duration, writing: Arc<AtomicBool> }

    impl Write for SlowSink {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.writing.store(true, Ordering::SeqCst);
            std::thread::sleep(self.delay);
            self.writing.store(false, Ordering::SeqCst);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn a_slow_disk_never_blocks_the_counting_hot_path() {
        let registry = Arc::new(Registry::new(16));
        let writing = Arc::new(AtomicBool::new(false));
        let sink = SlowSink { delay: Duration::from_millis(300), writing: writing.clone() };
        let writer = LiveWriter::start_with(Box::new(sink), Duration::from_millis(5), registry.clone()).unwrap();
        while !writing.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
        let t = track("pc");
        let begin = Instant::now();
        for i in 0..100_000u64 {
            registry.count(Boundary::ForwardAccepted, &t, i + 1);
        }
        let elapsed = begin.elapsed();
        assert!(writing.load(Ordering::SeqCst), "the writer is still inside its slow write");
        assert!(elapsed < Duration::from_millis(250), "100k callbacks while a write blocks: {elapsed:?}");
        assert_eq!(registry.snapshot()[0].counts[count_index(Boundary::ForwardAccepted)], 100_000);
        writer.finish().unwrap();
    }

    /// Fails its `fail_on`-th write after writing half of the line.
    struct FailingSink { inner: SharedSink, writes: usize, fail_on: usize }

    impl Write for FailingSink {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.inner.write(buf)
        }
        fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
            self.writes += 1;
            if self.writes == self.fail_on {
                self.inner.write(&buf[..buf.len() / 2])?;
                return Err(std::io::Error::other("injected write failure"));
            }
            self.inner.write(buf).map(|_| ())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn a_failed_write_leaves_a_skippable_fragment_a_seq_gap_and_a_counted_error() {
        let registry = Arc::new(Registry::new(4));
        registry.count(Boundary::ReceiveRegistered, &track("pc"), 1);
        let inner = SharedSink::default();
        let sink = FailingSink { inner: inner.clone(), writes: 0, fail_on: 2 };
        let writer = LiveWriter::start_with(Box::new(sink), Duration::from_millis(5), registry).unwrap();
        std::thread::sleep(Duration::from_millis(60));
        let summary = writer.finish().unwrap();
        assert_eq!(summary.write_errors, 1);
        let (lines, bad) = inner.lines();
        assert_eq!(bad, 1, "the half-written line is one unparseable line");
        let seqs: Vec<u64> = lines.iter().map(|l| l["seq"].as_u64().unwrap()).collect();
        assert_eq!(seqs[0], 0);
        assert_eq!(seqs[1], 2, "seq 1 was the lost line: a gap");
        assert!(seqs.windows(2).skip(1).all(|w| w[1] == w[0] + 1), "{seqs:?}");
        assert_eq!(lines[1]["write_errors"], 1, "counted on the next line");
        assert_eq!(lines.last().unwrap()["final"], true);
        assert!(lines[..lines.len() - 1].iter().all(|l| l["final"] == false));
    }

    /// Records each write's start and end; the second write is slow.
    struct TimedSink { calls: Arc<Mutex<Vec<(Instant, Instant)>>> }

    impl Write for TimedSink {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let begin = Instant::now();
            if self.calls.lock().unwrap().len() == 1 {
                std::thread::sleep(Duration::from_millis(150));
            }
            self.calls.lock().unwrap().push((begin, Instant::now()));
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn a_slow_write_is_never_followed_by_a_catch_up_line() {
        let period = Duration::from_millis(40);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let writer = LiveWriter::start_with(Box::new(TimedSink { calls: calls.clone() }), period,
                                            Arc::new(Registry::new(4))).unwrap();
        std::thread::sleep(Duration::from_millis(400));
        writer.finish().unwrap();
        let calls = calls.lock().unwrap().clone();
        let periodic = &calls[..calls.len() - 1]; // the final line is written on demand
        assert!(periodic.len() >= 4, "periodic lines ran: {}", periodic.len());
        assert!(periodic[1].1 - periodic[1].0 >= Duration::from_millis(150), "the injected slow write");
        for pair in periodic.windows(2) {
            let gap = pair[1].0.duration_since(pair[0].1);
            assert!(gap >= period, "next line {gap:?} after the previous completed (< {period:?})");
        }
    }

    #[test]
    fn a_reader_during_the_run_parses_every_complete_line_in_order() {
        let path = std::env::temp_dir().join(format!("skew-live-counters-{}-{}",
            std::process::id(), crate::object_trace::monotonic_us().unwrap()));
        let registry = Arc::new(Registry::new(16));
        let writer = LiveWriter::start(&path, Duration::from_millis(3), registry.clone()).unwrap();
        let t = track("pc");
        let mut previous_last_seq = None;
        for round in 0..20u64 {
            for i in 0..50 {
                registry.count(Boundary::ReceiveRegistered, &t, 1 + round * 50 + i);
            }
            std::thread::sleep(Duration::from_millis(5));
            let bytes = std::fs::read(&path).unwrap();
            let (lines, bad) = read_complete_lines(&bytes);
            assert_eq!(bad, 0);
            let seqs: Vec<u64> = lines.iter().map(|l| l["seq"].as_u64().unwrap()).collect();
            assert!(seqs.iter().enumerate().all(|(i, s)| *s == i as u64), "seq 0,1,2,...: {seqs:?}");
            let counts: Vec<u64> = lines.iter()
                .filter_map(|l| l["tracks"].as_array().and_then(|t| t.first()))
                .map(|t| t["registered"].as_u64().unwrap()).collect();
            assert!(counts.windows(2).all(|w| w[0] <= w[1]), "non-decreasing: {counts:?}");
            assert!(lines.iter().all(|l| l["final"] == false), "no final line before finish");
            if let Some(last) = seqs.last() {
                assert!(previous_last_seq.is_none_or(|p| *last >= p));
                previous_last_seq = Some(*last);
            }
        }
        writer.finish().unwrap();
        let (lines, bad) = read_complete_lines(&std::fs::read(&path).unwrap());
        assert_eq!(bad, 0);
        let last = lines.last().unwrap();
        assert_eq!(last["final"], true);
        assert_eq!(last["tracks"][0]["registered"], 1_000);
        assert_eq!(lines.iter().filter(|l| l["final"] == true).count(), 1);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn an_unterminated_tail_is_ignored_by_the_reader_rule() {
        let (lines, bad) = read_complete_lines(b"{\"seq\":0}\n{\"seq\":1}\n{\"seq\":2");
        assert_eq!((lines.len(), bad), (2, 0));
        let (lines, bad) = read_complete_lines(b"{\"seq\":0}\n{\"se\n{\"seq\":2}\n");
        assert_eq!((lines.len(), bad), (2, 1));
    }

    #[test]
    fn an_existing_sidecar_is_never_overwritten() {
        let path = std::env::temp_dir().join(format!("skew-live-counters-exists-{}-{}",
            std::process::id(), crate::object_trace::monotonic_us().unwrap()));
        std::fs::write(&path, b"preserved").unwrap();
        assert!(LiveWriter::start(&path, Duration::from_millis(100), Arc::new(Registry::new(4))).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"preserved");
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn every_line_has_the_documented_fields() {
        let registry = Arc::new(Registry::new(4));
        registry.count(Boundary::ReceiveRegistered, &track("pc"), 11);
        let sink = SharedSink::default();
        LiveWriter::start_with(Box::new(sink.clone()), Duration::from_secs(60), registry)
            .unwrap().finish().unwrap();
        let (lines, _) = sink.lines();
        let line = &lines[0];
        let mut keys: Vec<_> = line.as_object().unwrap().keys().cloned().collect();
        keys.sort();
        assert_eq!(keys, ["clock_errors", "final", "period_us", "schema", "seq", "t_us", "tracks",
                          "untracked_events", "write_errors"]);
        assert_eq!(line["schema"], "relay-live-counters-v1");
        assert_eq!(line["period_us"], 60_000_000);
        let mut track_keys: Vec<_> = line["tracks"][0].as_object().unwrap().keys().cloned().collect();
        track_keys.sort();
        assert_eq!(track_keys, ["first_registered_t_us", "forward_accepted", "forward_interrupted",
            "forward_started", "forward_timeout", "last_registered_t_us", "namespace_hex",
            "receive_complete", "receive_interrupted", "receive_timeout", "registered", "track",
            "track_incarnation"]);
        assert_eq!(line["tracks"][0]["namespace_hex"], json!(["72756e"]));
        assert!(line["t_us"].as_u64().unwrap() > 0);
    }
}
