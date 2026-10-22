//! L2 PC output (registration `md/20261022_실험1D_L2사다리_사전등록_문안.md`
//! §3-2, §3-3b; L2 design §3, §8-5; milestone M3): everything of the
//! Wayland PC sink that does not need a display, so that every rule is
//! testable with a simulated compositor.
//!
//! * [`TierScales`]: the point scale of each header tier, read from the
//!   tiers meta file (`depth_scale`, schema `phase4_s3_tiers_v1`) — never a
//!   hard-coded tier -> depth map.
//! * [`FramePreparer`]: payload -> 8-byte point records (`.bin` as sent;
//!   Draco decoded by the vendored Draco 1.5.7, `skew_draco_sys`) -> pixels
//!   (`skew_pc_render`, shared by both representations). Times:
//!   `t_decode_start_us` right before the decoder call and
//!   `t_decode_end_us` right after the records exist (Draco only), then
//!   `t_render_start_us` / `t_render_end_us` around the render, all
//!   [`crate::now_us`] (CLOCK_MONOTONIC).
//! * [`PendingSlot`]: the ONE-frame wait between the render worker and the
//!   commit thread. A frame replaced there before it was committed is
//!   handed back to be terminated `superseded_before_commit`; after a device
//!   failure the slot refuses frames (they become `device_failure`).
//! * [`PresentEngine`]: the commit thread's state. A commit opportunity
//!   exists when the frame callback of the previous commit has fired (or
//!   nothing was committed yet) and the stage has not failed; the frame
//!   goes into a buffer the compositor does NOT hold (`wl_buffer.release`
//!   only frees a buffer, it is never output evidence, nor is a frame
//!   callback). Every commit gets an immutable `commit_id -> frame`
//!   mapping; `wp_presentation_feedback.presented` of that id is its
//!   `output`, `discarded` its after-submit `compositor_discarded`. A
//!   feedback or frame callback for an unknown or already resolved id is a
//!   timed integrity failure at its receipt; a compositor that goes away,
//!   closes or reconfigures the window is a PC DEVICE failure (untimed).
//! * [`PresentBackend`]: the compositor interface — the real Wayland client
//!   (`crate::l2_wayland`) or [`SimCompositor`] (tests, never a device
//!   output).

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use sha2::{Digest, Sha256};
use skew_pc_render::Canvas;

use crate::l2_output::L2Submission;
use crate::now_us;

/// `clockid_t` CLOCK_MONOTONIC: the only presentation clock the run accepts.
pub const CLOCK_MONOTONIC_ID: u32 = libc::CLOCK_MONOTONIC as u32;
/// `wp_presentation_feedback.kind` bits.
pub const PRESENTATION_VSYNC: u32 = 0x1;
pub const PRESENTATION_HW_CLOCK: u32 = 0x2;
pub const PRESENTATION_HW_COMPLETION: u32 = 0x4;
pub const PRESENTATION_ZERO_COPY: u32 = 0x8;

/// After-submit discard reason: the Draco decoder refused the payload.
pub const DISCARD_DECODE_FAILED: &str = "decode_failed";
/// Integrity failures of the PC lane (`first_integrity_failure_reason`).
/// Timed at the submission (`t_submit_us`), like the haptic payload rule.
pub const FAIL_PC_PAYLOAD_INVALID: &str = "pc_payload_invalid";
pub const FAIL_PC_DECODE_FAILED: &str = "pc_decode_failed";
/// Timed at their receipt.
pub const FAIL_WAYLAND_UNEXPECTED_FEEDBACK: &str = "wayland_unexpected_feedback";
pub const FAIL_WAYLAND_UNEXPECTED_FRAME_CALLBACK: &str = "wayland_unexpected_frame_callback";
pub const FAIL_WAYLAND_BAD_TIMESTAMP: &str = "wayland_bad_timestamp";
/// PC device failures: untimed (no occurrence time is claimed), kept in the
/// sentinel's `lanes.pc.device_failure`.
pub const FAIL_WAYLAND_CONNECTION_LOST: &str = "wayland_connection_lost";
pub const FAIL_WAYLAND_WINDOW_CLOSED: &str = "wayland_window_closed";
pub const FAIL_WAYLAND_RECONFIGURED: &str = "wayland_reconfigured";
pub const FAIL_WAYLAND_FEEDBACK_MISSING: &str = "wayland_feedback_missing";
pub const WAYLAND_DEVICE_FAILURE_REASONS: [&str; 4] = [
    FAIL_WAYLAND_CONNECTION_LOST,
    FAIL_WAYLAND_WINDOW_CLOSED,
    FAIL_WAYLAND_RECONFIGURED,
    FAIL_WAYLAND_FEEDBACK_MISSING,
];

pub fn presentation_flag_names(flags: u32) -> Vec<&'static str> {
    let mut names = Vec::new();
    for (bit, name) in [
        (PRESENTATION_VSYNC, "vsync"),
        (PRESENTATION_HW_CLOCK, "hw_clock"),
        (PRESENTATION_HW_COMPLETION, "hw_completion"),
        (PRESENTATION_ZERO_COPY, "zero_copy"),
    ] {
        if flags & bit != 0 {
            names.push(name);
        }
    }
    if flags & !0xF != 0 {
        names.push("unknown_bits");
    }
    names
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

// ---- tiers meta -------------------------------------------------------------

pub const TIERS_META_SCHEMA: &str = "phase4_s3_tiers_v1";

/// Header tier id -> (tier name, `depth_scale`) from a tiers meta file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierScales {
    pub path: String,
    pub sha256: String,
    pub scales: BTreeMap<u16, (String, u32)>,
}

impl TierScales {
    pub fn load(path: &Path) -> Result<Self, String> {
        let bytes = std::fs::read(path).map_err(|e| format!("read tiers meta {}: {e}", path.display()))?;
        Self::parse(&bytes, &path.display().to_string())
    }

    /// Strict: schema `phase4_s3_tiers_v1`, every tier with an integer
    /// `tier_id` (u16) and `depth_scale` in 1..=1024, tier ids unique.
    pub fn parse(bytes: &[u8], path: &str) -> Result<Self, String> {
        let doc: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|e| format!("tiers meta {path}: {e}"))?;
        if doc["schema"] != TIERS_META_SCHEMA {
            return Err(format!("tiers meta {path}: schema {} is not {TIERS_META_SCHEMA}", doc["schema"]));
        }
        let tiers = doc["tiers"]
            .as_object()
            .filter(|t| !t.is_empty())
            .ok_or_else(|| format!("tiers meta {path}: no tiers"))?;
        let mut scales = BTreeMap::new();
        for (name, tier) in tiers {
            let id = tier["tier_id"]
                .as_u64()
                .filter(|id| *id <= u16::MAX as u64)
                .ok_or_else(|| format!("tiers meta {path}: {name}.tier_id"))? as u16;
            let scale = tier["depth_scale"]
                .as_u64()
                .filter(|s| (1..=1024).contains(s))
                .ok_or_else(|| format!("tiers meta {path}: {name}.depth_scale"))? as u32;
            if scales.insert(id, (name.clone(), scale)).is_some() {
                return Err(format!("tiers meta {path}: tier id {id} twice"));
            }
        }
        Ok(Self { path: path.to_string(), sha256: hex(&Sha256::digest(bytes)), scales })
    }

    pub fn depth_scale(&self, tier: u16) -> Option<u32> {
        self.scales.get(&tier).map(|(_, scale)| *scale)
    }

    pub fn to_json(&self) -> serde_json::Value {
        let tiers: serde_json::Map<String, serde_json::Value> = self
            .scales
            .iter()
            .map(|(id, (name, scale))| (id.to_string(), serde_json::json!({ "name": name, "depth_scale": scale })))
            .collect();
        serde_json::json!({
            "path": self.path,
            "sha256": self.sha256,
            "schema": TIERS_META_SCHEMA,
            "tiers_by_header_tier": tiers,
        })
    }
}

// ---- representation, decoding, rendering -------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PcRepresentation {
    Bin,
    Draco,
}

impl PcRepresentation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Bin => "bin",
            Self::Draco => "draco",
        }
    }
}

/// The decoder of a representation for the L2 meta (null for `.bin`).
pub fn decoder_meta(representation: PcRepresentation) -> serde_json::Value {
    match representation {
        PcRepresentation::Bin => serde_json::Value::Null,
        PcRepresentation::Draco => {
            let info = skew_draco_sys::decoder_info();
            serde_json::json!({
                "id": info.id,
                "version": info.version,
                "source_url": info.source_url,
                "source_tarball_sha256": info.source_tarball_sha256,
                "vendored_sources_sha256": info.vendored_sources_sha256,
                "vendored_files": info.vendored_files,
                "compiled_sources": info.compiled_sources,
                "compiler": info.compiler,
                "compiler_version": info.compiler_version,
                "fixed_flags": info.fixed_flags,
                "compiler_args": info.compiler_args,
                "record_conversion_rust_opt_level": info.rust_opt_level,
                "threads": info.threads,
                "rounding": info.rounding,
                "placement": "decoded in the PC render worker thread after release (registration 3-2), one frame at a time in release order",
            })
        }
    }
}

/// The renderer for the L2 meta.
pub fn renderer_meta(side: u32) -> serde_json::Value {
    serde_json::json!({
        "id": "skew-pc-render (CPU, integer orthographic voxel splat)",
        "canvas_side_px": side,
        "projection": "front orthographic view of the vox10 cube [0,1024)^3, x right, y up, viewer at +z; a point of tier scale s covers [c*s,(c+1)*s) per axis; vox10 [a,b) -> pixels [floor(a*side/1024), floor(b*side/1024)); the square canvas is centred in the surface",
        "visibility": "per pixel the maximum key ((z+1)<<24 | rgb) over the covering points: nearest wins, then the larger 24-bit RGB; independent of point order, so bin and draco frames of one content render identically",
        "background_xrgb": format!("0x{:08x}", skew_pc_render::BACKGROUND),
        "pixel_format": "XRGB8888",
        "opt_level": "3 in every profile (workspace profile override)",
    })
}

/// Decode / render times of one frame (µs, CLOCK_MONOTONIC).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PcTiming {
    /// `(t_decode_start_us, t_decode_end_us)`, Draco only.
    pub decode_us: Option<(u64, u64)>,
    pub t_render_start_us: u64,
    pub t_render_end_us: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameInfo {
    pub points: u64,
    pub covered_pixels: u64,
    pub depth_scale: u32,
}

/// A rendered frame that has not been committed.
#[derive(Debug)]
pub struct PendingFrame {
    pub submission: L2Submission,
    pub timing: PcTiming,
    pub info: FrameInfo,
    /// `side × side` XRGB8888.
    pub pixels: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrepareError {
    /// No payload, a tier the tiers meta does not know, or records that
    /// cannot be drawn at the tier's scale: after-submit `payload_invalid`.
    PayloadInvalid { detail: String, decode_us: Option<(u64, u64)> },
    /// The Draco decoder refused the payload: after-submit `decode_failed`
    /// (registration 3-2: an integrity failure, never a performance result).
    DecodeFailed { detail: String, decode_us: (u64, u64) },
}

impl PrepareError {
    pub fn decode_us(&self) -> Option<(u64, u64)> {
        match self {
            Self::PayloadInvalid { decode_us, .. } => *decode_us,
            Self::DecodeFailed { decode_us, .. } => Some(*decode_us),
        }
    }

    pub fn detail(&self) -> &str {
        match self {
            Self::PayloadInvalid { detail, .. } | Self::DecodeFailed { detail, .. } => detail,
        }
    }
}

/// Payload -> pixels, owned by the render worker thread.
pub struct FramePreparer {
    representation: PcRepresentation,
    scales: TierScales,
    canvas: Canvas,
}

impl FramePreparer {
    pub fn new(representation: PcRepresentation, scales: TierScales, side: u32) -> Self {
        Self { representation, scales, canvas: Canvas::new(side) }
    }

    pub fn side(&self) -> u32 {
        self.canvas.side()
    }

    pub fn representation(&self) -> PcRepresentation {
        self.representation
    }

    /// Decode (Draco) and render one released frame into `pixels` (reused;
    /// resized to `side × side`). On error the buffer is handed back.
    pub fn prepare(
        &mut self,
        submission: L2Submission,
        payload: Option<&[u8]>,
        mut pixels: Vec<u32>,
        clock: &mut dyn FnMut() -> u64,
    ) -> Result<PendingFrame, (PrepareError, Vec<u32>)> {
        let side = self.canvas.side() as usize;
        pixels.resize(side * side, 0);
        let tier = submission.identity.tier;
        let Some(depth_scale) = self.scales.depth_scale(tier) else {
            let detail = format!("header tier {tier} is not in the tiers meta");
            return Err((PrepareError::PayloadInvalid { detail, decode_us: None }, pixels));
        };
        let Some(payload) = payload else {
            return Err((PrepareError::PayloadInvalid { detail: "no payload".into(), decode_us: None }, pixels));
        };
        let (decoded, decode_us) = match self.representation {
            PcRepresentation::Bin => (None, None),
            PcRepresentation::Draco => {
                let t_decode_start_us = clock();
                let result = skew_draco_sys::decode_to_records(payload);
                let t_decode_end_us = clock();
                let decode_us = (t_decode_start_us, t_decode_end_us);
                match result {
                    Ok(records) => (Some(records.bytes), Some(decode_us)),
                    Err(error) => {
                        let detail = error.to_string();
                        return Err((PrepareError::DecodeFailed { detail, decode_us }, pixels));
                    }
                }
            }
        };
        let records: &[u8] = decoded.as_deref().unwrap_or(payload);
        let t_render_start_us = clock();
        let stats = match skew_pc_render::render(records, depth_scale, &mut self.canvas, &mut pixels) {
            Ok(stats) => stats,
            Err(error) => {
                let detail = format!("{}: {error}", error.reason());
                return Err((PrepareError::PayloadInvalid { detail, decode_us }, pixels));
            }
        };
        let t_render_end_us = clock();
        Ok(PendingFrame {
            submission,
            timing: PcTiming { decode_us, t_render_start_us, t_render_end_us },
            info: FrameInfo { points: stats.points, covered_pixels: stats.covered_pixels, depth_scale },
            pixels,
        })
    }
}

/// Recycled `side × side` pixel buffers (bounded).
pub struct PixelPool {
    free: Mutex<Vec<Vec<u32>>>,
    cap: usize,
}

impl PixelPool {
    pub fn new(cap: usize) -> Self {
        Self { free: Mutex::new(Vec::new()), cap }
    }

    pub fn take(&self) -> Vec<u32> {
        self.free.lock().unwrap_or_else(|p| p.into_inner()).pop().unwrap_or_default()
    }

    pub fn put(&self, pixels: Vec<u32>) {
        let mut free = self.free.lock().unwrap_or_else(|p| p.into_inner());
        if free.len() < self.cap {
            free.push(pixels);
        }
    }
}

// ---- the one-frame pending slot ---------------------------------------------

#[derive(Debug)]
pub enum Offer {
    /// The frame waits; `superseded` is the frame it replaced (never
    /// committed): its after-submit terminal is `superseded_before_commit`.
    Accepted { superseded: Option<PendingFrame> },
    /// The stage failed: the frame's terminal is `device_failure`.
    Refused(PendingFrame),
}

#[derive(Default)]
struct SlotState {
    frame: Option<PendingFrame>,
    failed: bool,
}

/// One frame between the render worker and the commit thread. Every
/// transition happens under one lock, so a frame is either taken by the
/// commit thread, replaced, or refused — exactly once.
#[derive(Default)]
pub struct PendingSlot {
    state: Mutex<SlotState>,
}

impl PendingSlot {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, SlotState> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub fn offer(&self, frame: PendingFrame) -> Offer {
        let mut state = self.lock();
        if state.failed {
            return Offer::Refused(frame);
        }
        Offer::Accepted { superseded: state.frame.replace(frame) }
    }

    pub fn take(&self) -> Option<PendingFrame> {
        self.lock().frame.take()
    }

    pub fn has_frame(&self) -> bool {
        self.lock().frame.is_some()
    }

    pub fn failed(&self) -> bool {
        self.lock().failed
    }

    /// A device failure: every later offer is refused; the waiting frame
    /// (if any) is handed back for its `device_failure` terminal.
    pub fn fail(&self) -> Option<PendingFrame> {
        let mut state = self.lock();
        state.failed = true;
        state.frame.take()
    }
}

// ---- wake-up -----------------------------------------------------------------

/// Block on `fds` until one is readable or `until_us` (CLOCK_MONOTONIC µs)
/// passes; `None` waits indefinitely. Returns the `ppoll` count (0 on
/// timeout or EINTR).
pub fn ppoll_until(fds: &mut [libc::pollfd], until_us: Option<u64>) -> std::io::Result<usize> {
    let timeout = until_us.map(|until| {
        let left = until.saturating_sub(now_us());
        libc::timespec { tv_sec: (left / 1_000_000) as libc::time_t, tv_nsec: ((left % 1_000_000) * 1_000) as libc::c_long }
    });
    let timeout_ptr = timeout.as_ref().map_or(std::ptr::null(), |t| t as *const libc::timespec);
    // SAFETY: valid pollfd slice and timespec (or null); no signal mask.
    let n = unsafe { libc::ppoll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, timeout_ptr, std::ptr::null()) };
    if n < 0 {
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::Interrupted {
            return Ok(0);
        }
        return Err(error);
    }
    Ok(n as usize)
}

/// An eventfd the render worker and the stage use to wake the commit
/// thread (which otherwise blocks on the compositor socket).
pub struct Waker {
    fd: OwnedFd,
}

impl Waker {
    pub fn new() -> std::io::Result<Self> {
        // SAFETY: plain syscall; the fd is owned below.
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: a fresh fd we own.
        Ok(Self { fd: unsafe { OwnedFd::from_raw_fd(fd) } })
    }

    pub fn wake(&self) {
        let one: u64 = 1;
        // SAFETY: 8 bytes from a valid u64; a full counter (EAGAIN) still wakes.
        unsafe { libc::write(self.fd.as_raw_fd(), (&one as *const u64).cast(), 8) };
    }

    pub fn clear(&self) {
        let mut value: u64 = 0;
        // SAFETY: 8 bytes into a valid u64; EAGAIN when not signalled.
        unsafe { libc::read(self.fd.as_raw_fd(), (&mut value as *mut u64).cast(), 8) };
    }

    pub fn raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }

    /// Wait until woken (cleared, `true`) or `until_us` passes (`false`).
    pub fn wait(&self, until_us: Option<u64>) -> bool {
        let mut fds = [libc::pollfd { fd: self.raw_fd(), events: libc::POLLIN, revents: 0 }];
        let woken = matches!(ppoll_until(&mut fds, until_us), Ok(n) if n > 0) && fds[0].revents & libc::POLLIN != 0;
        if woken {
            self.clear();
        }
        woken
    }
}

// ---- presentation feedback ----------------------------------------------------

/// The raw `wp_presentation_feedback.presented` values (+ the number of
/// `sync_output` events before it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Presented {
    pub tv_sec: u64,
    pub tv_nsec: u32,
    /// Nanoseconds; 0 = unknown.
    pub refresh_ns: u32,
    /// The 64-bit MSC (`seq_hi << 32 | seq_lo`).
    pub seq: u64,
    pub flags: u32,
    pub sync_outputs: u32,
}

impl Presented {
    /// The presentation time as integer µs of the presentation clock,
    /// truncated exactly like [`crate::now_us`] (`sec·1e6 + floor(nsec/1000)`);
    /// `None` if the nanoseconds are not below 1e9 (protocol violation).
    pub fn t_us(&self) -> Option<u64> {
        if self.tv_nsec >= 1_000_000_000 {
            return None;
        }
        self.tv_sec.checked_mul(1_000_000)?.checked_add(self.tv_nsec as u64 / 1_000)
    }

    /// The refresh interval in integer µs (floor); 0 = unknown.
    pub fn refresh_us(&self) -> u64 {
        self.refresh_ns as u64 / 1_000
    }
}

// ---- the compositor interface -------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum BackendEvent {
    /// `wl_callback.done` of the frame callback requested with that commit.
    FrameDone { commit_id: u64 },
    Presented { commit_id: u64, presented: Presented },
    Discarded { commit_id: u64 },
    /// `wl_buffer.release`.
    BufferReleased { buffer: usize },
    /// After ready: a configure that changes the size or drops fullscreen,
    /// or an output mode change.
    Reconfigured { detail: String },
    /// `xdg_toplevel.close`.
    Closed,
}

/// The compositor connection failed (I/O error, protocol error, hang-up).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendError {
    pub detail: String,
}

impl std::fmt::Display for BackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.detail)
    }
}

/// What the backend negotiated before the run (recorded in meta).
#[derive(Debug, Clone)]
pub struct StartupInfo {
    /// `"wayland"` (a device output) or `"simulated"` (tests).
    pub backend: &'static str,
    /// `wp_presentation.clock_id`.
    pub clock_id: u32,
    pub width: u32,
    pub height: u32,
    pub buffers: usize,
    /// Buffers the startup commit still holds: `(buffer, startup commit id)`.
    pub held: Vec<(usize, u64)>,
    /// The startup frame's presentation (not a ledger row).
    pub startup_presented: Option<Presented>,
    pub startup_commit_to_presented_us: Option<i64>,
    /// Backend-specific facts (globals, outputs, display name).
    pub details: serde_json::Value,
}

impl StartupInfo {
    pub fn to_json(&self) -> serde_json::Value {
        let presented = self.startup_presented.map(|p| {
            serde_json::json!({
                "t_presented_us": p.t_us(),
                "refresh_us": p.refresh_us(),
                "msc": p.seq,
                "flags": p.flags,
                "flag_names": presentation_flag_names(p.flags),
                "sync_outputs": p.sync_outputs,
                "commit_to_presented_us": self.startup_commit_to_presented_us,
            })
        });
        serde_json::json!({
            "backend": self.backend,
            "clock_id": self.clock_id,
            "clock": if self.clock_id == CLOCK_MONOTONIC_ID { "CLOCK_MONOTONIC" } else { "OTHER" },
            "surface_width": self.width,
            "surface_height": self.height,
            "buffers": self.buffers,
            "startup_presentation": presented,
            "details": self.details,
        })
    }
}

/// The compositor as the commit thread sees it. Opened inside the lane
/// thread (a connection never crosses threads).
pub trait PresentBackend {
    fn startup(&self) -> &StartupInfo;
    /// Copy a `side × side` frame into `buffer` (centred). The caller only
    /// passes a buffer the compositor does not hold.
    fn write_frame(&mut self, buffer: usize, side: u32, pixels: &[u32]) -> Result<(), BackendError>;
    /// Attach `buffer`, damage it, request a frame callback and a
    /// presentation feedback (both carrying `commit_id`), commit, flush.
    fn commit(&mut self, buffer: usize, commit_id: u64) -> Result<(), BackendError>;
    /// Block until compositor events, a wake-up, or `until_us`; return the
    /// events (possibly none).
    fn wait(&mut self, until_us: Option<u64>, waker: &Waker) -> Result<Vec<BackendEvent>, BackendError>;
}

// ---- the commit-side state machine --------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferState {
    Free,
    Held { commit_id: u64 },
}

/// A committed frame: immutable until its feedback.
#[derive(Debug, Clone, Copy)]
pub struct CommittedFrame {
    pub commit_id: u64,
    pub buffer: usize,
    pub t_commit_us: u64,
    pub submission: L2Submission,
    pub timing: PcTiming,
    pub info: FrameInfo,
}

#[derive(Debug, Clone)]
pub enum EngineOutcome {
    /// `presented`: the frame's `output`.
    Output { frame: CommittedFrame, presented: Presented, t_output_us: u64, t_callback_us: u64 },
    /// `discarded`: the frame's after-submit `compositor_discarded`.
    Discarded { frame: CommittedFrame, t_callback_us: u64 },
    /// A timed integrity failure at its receipt.
    Anomaly { reason: &'static str, t_us: u64, detail: String },
    /// A PC device failure (untimed).
    DeviceFailure { reason: &'static str, detail: String },
}

/// The first PC device failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PcDeviceFailure {
    pub reason: &'static str,
    /// The clock read before the last backend call that succeeded.
    pub last_good_us: u64,
    pub detected_us: u64,
    pub detail: String,
}

impl PcDeviceFailure {
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "reason": self.reason,
            "occurrence_time_determinable": false,
            "observation_interval_us": [self.last_good_us, self.detected_us],
            "last_good_us": self.last_good_us,
            "detected_us": self.detected_us,
            "detail": self.detail,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct EngineStats {
    pub commits: u64,
    pub presented: u64,
    pub compositor_discarded: u64,
    pub frame_callbacks: u64,
    pub buffer_releases: u64,
    pub unexpected_buffer_releases: u64,
    pub unexpected_feedback: u64,
    pub unexpected_frame_callbacks: u64,
    pub bad_timestamps: u64,
    pub commit_waits_for_buffer: u64,
    pub max_commit_to_presented_us: Option<i64>,
    pub max_commit_to_frame_callback_us: Option<u64>,
    pub refresh_us_min: Option<u64>,
    pub refresh_us_max: Option<u64>,
    pub flags_counts: BTreeMap<u32, u64>,
}

impl EngineStats {
    pub fn to_json(&self) -> serde_json::Value {
        let flags: serde_json::Map<String, serde_json::Value> = self
            .flags_counts
            .iter()
            .map(|(flags, n)| (format!("{flags}:{}", presentation_flag_names(*flags).join("|")), (*n).into()))
            .collect();
        serde_json::json!({
            "commits": self.commits,
            "presented": self.presented,
            "compositor_discarded": self.compositor_discarded,
            "frame_callbacks": self.frame_callbacks,
            "buffer_releases": self.buffer_releases,
            "unexpected_buffer_releases": self.unexpected_buffer_releases,
            "unexpected_feedback": self.unexpected_feedback,
            "unexpected_frame_callbacks": self.unexpected_frame_callbacks,
            "bad_timestamps": self.bad_timestamps,
            "commit_waits_for_buffer": self.commit_waits_for_buffer,
            "max_commit_to_presented_us": self.max_commit_to_presented_us,
            "max_commit_to_frame_callback_us": self.max_commit_to_frame_callback_us,
            "refresh_us_min": self.refresh_us_min,
            "refresh_us_max": self.refresh_us_max,
            "presented_flags": flags,
        })
    }
}

/// The commit thread's state: buffers, the outstanding frame callback, the
/// committed frames awaiting feedback (immutable `commit_id -> frame`),
/// and the first device failure. Pure: no I/O, no clock.
pub struct PresentEngine {
    buffers: Vec<BufferState>,
    committed: HashMap<u64, CommittedFrame>,
    next_commit_id: u64,
    /// `(commit_id, t_commit_us)` whose frame callback has not fired.
    frame_callback: Option<(u64, u64)>,
    failure: Option<PcDeviceFailure>,
    stats: EngineStats,
}

/// The first commit id of ledger frames (ids below it belong to startup).
pub const FIRST_COMMIT_ID: u64 = 1;

impl PresentEngine {
    /// `held`: buffers the startup commit still holds.
    pub fn new(buffers: usize, held: &[(usize, u64)]) -> Self {
        let mut states = vec![BufferState::Free; buffers];
        for &(buffer, commit_id) in held {
            if let Some(state) = states.get_mut(buffer) {
                *state = BufferState::Held { commit_id };
            }
        }
        Self {
            buffers: states,
            committed: HashMap::new(),
            next_commit_id: FIRST_COMMIT_ID,
            frame_callback: None,
            failure: None,
            stats: EngineStats::default(),
        }
    }

    pub fn stats(&self) -> &EngineStats {
        &self.stats
    }

    pub fn buffer_states(&self) -> &[BufferState] {
        &self.buffers
    }

    pub fn failure(&self) -> Option<&PcDeviceFailure> {
        self.failure.as_ref()
    }

    /// A commit opportunity: the previous commit's frame callback has fired
    /// (or nothing was committed) and the stage has not failed.
    pub fn commit_opportunity(&self) -> bool {
        self.failure.is_none() && self.frame_callback.is_none()
    }

    /// The lowest-index buffer the compositor does not hold.
    pub fn free_buffer(&self) -> Option<usize> {
        self.buffers.iter().position(|b| *b == BufferState::Free)
    }

    /// Count an opportunity lost to "no free buffer" (statistics only).
    pub fn note_no_free_buffer(&mut self) {
        self.stats.commit_waits_for_buffer += 1;
    }

    /// Record the commit of `frame` into `buffer` (already written) and
    /// return its id and the pixel buffer for recycling. Panics unless at a
    /// commit opportunity with a free buffer — callers check both.
    pub fn commit(&mut self, frame: PendingFrame, buffer: usize, t_commit_us: u64) -> (u64, Vec<u32>) {
        assert!(self.commit_opportunity(), "commit only at a commit opportunity");
        assert_eq!(self.buffers[buffer], BufferState::Free, "never write a buffer the compositor holds");
        let commit_id = self.next_commit_id;
        self.next_commit_id += 1;
        self.buffers[buffer] = BufferState::Held { commit_id };
        self.frame_callback = Some((commit_id, t_commit_us));
        let PendingFrame { submission, timing, info, pixels } = frame;
        self.committed.insert(commit_id, CommittedFrame { commit_id, buffer, t_commit_us, submission, timing, info });
        self.stats.commits += 1;
        (commit_id, pixels)
    }

    /// Committed frames still waiting for their feedback.
    pub fn awaiting_feedback(&self) -> usize {
        self.committed.len()
    }

    /// The frames that never got feedback (they stay unresolved), in commit order.
    pub fn unresolved(&self) -> Vec<CommittedFrame> {
        let mut frames: Vec<CommittedFrame> = self.committed.values().copied().collect();
        frames.sort_by_key(|f| f.commit_id);
        frames
    }

    fn unknown_detail(&self, what: &str, commit_id: u64) -> String {
        if commit_id < FIRST_COMMIT_ID {
            format!("{what} for startup commit {commit_id} after ready")
        } else if commit_id < self.next_commit_id {
            format!("{what} for commit {commit_id}, already resolved (duplicate)")
        } else {
            format!("{what} for commit {commit_id}, never committed")
        }
    }

    pub fn on_event(&mut self, event: BackendEvent, t_callback_us: u64) -> Option<EngineOutcome> {
        match event {
            BackendEvent::FrameDone { commit_id } => match self.frame_callback {
                Some((outstanding, t_commit_us)) if outstanding == commit_id => {
                    self.frame_callback = None;
                    self.stats.frame_callbacks += 1;
                    let wait = t_callback_us.saturating_sub(t_commit_us);
                    self.stats.max_commit_to_frame_callback_us =
                        Some(self.stats.max_commit_to_frame_callback_us.map_or(wait, |m| m.max(wait)));
                    None
                }
                _ => {
                    self.stats.unexpected_frame_callbacks += 1;
                    Some(EngineOutcome::Anomaly {
                        reason: FAIL_WAYLAND_UNEXPECTED_FRAME_CALLBACK,
                        t_us: t_callback_us,
                        detail: self.unknown_detail("frame callback", commit_id),
                    })
                }
            },
            BackendEvent::Presented { commit_id, presented } => {
                let Some(frame) = self.committed.remove(&commit_id) else {
                    self.stats.unexpected_feedback += 1;
                    return Some(EngineOutcome::Anomaly {
                        reason: FAIL_WAYLAND_UNEXPECTED_FEEDBACK,
                        t_us: t_callback_us,
                        detail: self.unknown_detail("presented", commit_id),
                    });
                };
                let Some(t_output_us) = presented.t_us() else {
                    // The feedback came but carries no usable time: the
                    // frame gets no terminal (it stays unresolved).
                    self.stats.bad_timestamps += 1;
                    return Some(EngineOutcome::Anomaly {
                        reason: FAIL_WAYLAND_BAD_TIMESTAMP,
                        t_us: t_callback_us,
                        detail: format!("presented tv_nsec {} for commit {commit_id}", presented.tv_nsec),
                    });
                };
                self.stats.presented += 1;
                let latency = t_output_us as i64 - frame.t_commit_us as i64;
                self.stats.max_commit_to_presented_us =
                    Some(self.stats.max_commit_to_presented_us.map_or(latency, |m| m.max(latency)));
                let refresh = presented.refresh_us();
                self.stats.refresh_us_min = Some(self.stats.refresh_us_min.map_or(refresh, |m| m.min(refresh)));
                self.stats.refresh_us_max = Some(self.stats.refresh_us_max.map_or(refresh, |m| m.max(refresh)));
                *self.stats.flags_counts.entry(presented.flags).or_default() += 1;
                Some(EngineOutcome::Output { frame, presented, t_output_us, t_callback_us })
            }
            BackendEvent::Discarded { commit_id } => match self.committed.remove(&commit_id) {
                Some(frame) => {
                    self.stats.compositor_discarded += 1;
                    Some(EngineOutcome::Discarded { frame, t_callback_us })
                }
                None => {
                    self.stats.unexpected_feedback += 1;
                    Some(EngineOutcome::Anomaly {
                        reason: FAIL_WAYLAND_UNEXPECTED_FEEDBACK,
                        t_us: t_callback_us,
                        detail: self.unknown_detail("discarded", commit_id),
                    })
                }
            },
            BackendEvent::BufferReleased { buffer } => {
                match self.buffers.get_mut(buffer) {
                    Some(state @ BufferState::Held { .. }) => {
                        *state = BufferState::Free;
                        self.stats.buffer_releases += 1;
                    }
                    // Not evidence of anything; counted, never acted on.
                    _ => self.stats.unexpected_buffer_releases += 1,
                }
                None
            }
            BackendEvent::Reconfigured { detail } => {
                Some(EngineOutcome::DeviceFailure { reason: FAIL_WAYLAND_RECONFIGURED, detail })
            }
            BackendEvent::Closed => Some(EngineOutcome::DeviceFailure {
                reason: FAIL_WAYLAND_WINDOW_CLOSED,
                detail: "xdg_toplevel.close".into(),
            }),
        }
    }

    /// Record a device failure; `true` for the first one (later ones are
    /// not recorded: the stage stopped at the first).
    pub fn fail(&mut self, reason: &'static str, last_good_us: u64, detected_us: u64, detail: String) -> bool {
        if self.failure.is_some() {
            return false;
        }
        self.failure = Some(PcDeviceFailure { reason, last_good_us, detected_us, detail });
        true
    }
}

// ---- simulated compositor -------------------------------------------------------

/// Injected faults of [`SimCompositor`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SimWaylandFault {
    /// The connection is lost this long after startup.
    Disconnect { after_start_us: u64 },
    /// This commit's feedback is never sent (its frame callback is).
    DropFeedback { commit_id: u64 },
    /// This commit's feedback is `discarded`.
    DiscardFeedback { commit_id: u64 },
    /// This commit's `presented` is sent twice.
    DuplicateFeedback { commit_id: u64 },
    /// The window is reconfigured this long after startup.
    Reconfigure { after_start_us: u64 },
    /// No buffer is ever released.
    NeverRelease,
}

/// What a simulated compositor observed (shared with the test).
#[derive(Debug, Default)]
pub struct SimObserver {
    pub commits: AtomicU64,
    /// Frames written into a buffer the compositor still held (must be 0).
    pub writes_into_held_buffers: AtomicU64,
    pub dropped: AtomicBool,
}

#[derive(Debug, Clone)]
pub struct SimCompositorConfig {
    pub width: u32,
    pub height: u32,
    pub buffers: usize,
    pub clock_id: u32,
    /// Vblank period on the real clock.
    pub refresh_us: u64,
    pub flags: u32,
    pub faults: Vec<SimWaylandFault>,
    pub observer: Arc<SimObserver>,
}

impl SimCompositorConfig {
    pub fn new(width: u32, height: u32) -> Self {
        Self {
            width,
            height,
            buffers: 4,
            clock_id: CLOCK_MONOTONIC_ID,
            refresh_us: 16_667,
            flags: PRESENTATION_VSYNC | PRESENTATION_HW_CLOCK | PRESENTATION_HW_COMPLETION,
            faults: Vec::new(),
            observer: Arc::new(SimObserver::default()),
        }
    }
}

/// A deterministic compositor on the real clock: vblanks every
/// `refresh_us` from startup; at a vblank the LAST commit made since the
/// previous one is presented at the vblank time (earlier ones discarded),
/// every such commit's frame callback fires, and the buffer shown before is
/// released. Startup leaves buffer 0 on screen (startup commit 0), like the
/// Wayland backend's warm-up frame. Tests only: never a device output.
pub struct SimCompositor {
    cfg: SimCompositorConfig,
    startup: StartupInfo,
    t_start_us: u64,
    /// Commits since the last vblank: `(commit_id, buffer, t_commit_us)`.
    queued: Vec<(u64, usize, u64)>,
    displayed: Option<usize>,
    /// The compositor's view: buffers it holds (displayed or queued).
    held: Vec<bool>,
    events: VecDeque<BackendEvent>,
    reconfigure_sent: bool,
}

impl SimCompositor {
    pub fn new(cfg: SimCompositorConfig) -> Self {
        let t_start_us = now_us();
        let mut held = vec![false; cfg.buffers];
        if let Some(first) = held.first_mut() {
            *first = true;
        }
        let startup = StartupInfo {
            backend: "simulated",
            clock_id: cfg.clock_id,
            width: cfg.width,
            height: cfg.height,
            buffers: cfg.buffers,
            held: if cfg.buffers > 0 { vec![(0, 0)] } else { Vec::new() },
            startup_presented: None,
            startup_commit_to_presented_us: None,
            details: serde_json::json!({ "refresh_us": cfg.refresh_us, "flags": cfg.flags }),
        };
        Self {
            cfg,
            startup,
            t_start_us,
            queued: Vec::new(),
            displayed: Some(0),
            held,
            events: VecDeque::new(),
            reconfigure_sent: false,
        }
    }

    fn fault(&self, test: impl Fn(&SimWaylandFault) -> bool) -> bool {
        self.cfg.faults.iter().any(test)
    }

    fn disconnect_at(&self) -> Option<u64> {
        self.cfg.faults.iter().find_map(|f| match f {
            SimWaylandFault::Disconnect { after_start_us } => Some(self.t_start_us + after_start_us),
            _ => None,
        })
    }

    fn reconfigure_at(&self) -> Option<u64> {
        self.cfg.faults.iter().find_map(|f| match f {
            SimWaylandFault::Reconfigure { after_start_us } => Some(self.t_start_us + after_start_us),
            _ => None,
        })
    }

    fn check_connection(&self) -> Result<(), BackendError> {
        match self.disconnect_at() {
            Some(t) if now_us() >= t => Err(BackendError { detail: "simulated compositor hang-up".into() }),
            _ => Ok(()),
        }
    }

    /// The first vblank strictly after `t`.
    fn vblank_after(&self, t: u64) -> u64 {
        let k = (t.saturating_sub(self.t_start_us)) / self.cfg.refresh_us + 1;
        self.t_start_us + k * self.cfg.refresh_us
    }

    fn process_vblanks(&mut self, now: u64) {
        while let Some(&(_, _, t_first)) = self.queued.first() {
            let vblank = self.vblank_after(t_first);
            if vblank > now {
                break;
            }
            let (due, rest): (Vec<_>, Vec<_>) = self.queued.iter().partition(|(_, _, t)| *t < vblank);
            self.queued = rest;
            let last = due.len() - 1;
            for (i, &(commit_id, buffer, _)) in due.iter().enumerate() {
                self.events.push_back(BackendEvent::FrameDone { commit_id });
                let presented = Presented {
                    tv_sec: vblank / 1_000_000,
                    tv_nsec: ((vblank % 1_000_000) * 1_000) as u32,
                    refresh_ns: (self.cfg.refresh_us * 1_000) as u32,
                    seq: (vblank - self.t_start_us) / self.cfg.refresh_us,
                    flags: self.cfg.flags,
                    sync_outputs: 1,
                };
                let shown = i == last;
                if self.fault(|f| *f == SimWaylandFault::DropFeedback { commit_id }) {
                    // no feedback at all
                } else if !shown || self.fault(|f| *f == SimWaylandFault::DiscardFeedback { commit_id }) {
                    self.events.push_back(BackendEvent::Discarded { commit_id });
                } else {
                    self.events.push_back(BackendEvent::Presented { commit_id, presented });
                    if self.fault(|f| *f == SimWaylandFault::DuplicateFeedback { commit_id }) {
                        self.events.push_back(BackendEvent::Presented { commit_id, presented });
                    }
                }
                if !shown {
                    // A discarded commit's buffer goes back at once.
                    self.release(buffer);
                }
            }
            let (_, buffer, _) = due[last];
            if let Some(previous) = self.displayed.replace(buffer) {
                if previous != buffer {
                    self.release(previous);
                }
            }
        }
    }

    fn release(&mut self, buffer: usize) {
        if self.fault(|f| *f == SimWaylandFault::NeverRelease) {
            return;
        }
        if self.held[buffer] {
            self.held[buffer] = false;
            self.events.push_back(BackendEvent::BufferReleased { buffer });
        }
    }

    fn next_due(&self) -> Option<u64> {
        let vblank = self.queued.first().map(|&(_, _, t)| self.vblank_after(t));
        let reconfigure = self.reconfigure_at().filter(|_| !self.reconfigure_sent);
        [vblank, reconfigure, self.disconnect_at()].into_iter().flatten().min()
    }
}

impl Drop for SimCompositor {
    fn drop(&mut self) {
        self.cfg.observer.dropped.store(true, Ordering::SeqCst);
    }
}

impl PresentBackend for SimCompositor {
    fn startup(&self) -> &StartupInfo {
        &self.startup
    }

    fn write_frame(&mut self, buffer: usize, side: u32, pixels: &[u32]) -> Result<(), BackendError> {
        self.check_connection()?;
        assert_eq!(pixels.len(), side as usize * side as usize, "a square frame");
        assert!(side <= self.cfg.width.min(self.cfg.height), "the frame fits the surface");
        if self.held[buffer] {
            self.cfg.observer.writes_into_held_buffers.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }

    fn commit(&mut self, buffer: usize, commit_id: u64) -> Result<(), BackendError> {
        self.check_connection()?;
        self.held[buffer] = true;
        self.queued.push((commit_id, buffer, now_us()));
        self.cfg.observer.commits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn wait(&mut self, until_us: Option<u64>, waker: &Waker) -> Result<Vec<BackendEvent>, BackendError> {
        loop {
            self.check_connection()?;
            let now = now_us();
            self.process_vblanks(now);
            if let Some(t) = self.reconfigure_at() {
                if !self.reconfigure_sent && now >= t {
                    self.reconfigure_sent = true;
                    self.events.push_back(BackendEvent::Reconfigured { detail: "simulated size change".into() });
                }
            }
            if !self.events.is_empty() {
                return Ok(self.events.drain(..).collect());
            }
            if until_us.is_some_and(|until| now >= until) {
                return Ok(Vec::new());
            }
            let deadline = match (self.next_due(), until_us) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            if waker.wait(deadline) {
                return Ok(Vec::new());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::l2_output::{L2Identity, L2Modality};

    fn submission(seq: u32, tier: u16) -> L2Submission {
        L2Submission {
            identity: L2Identity { modality: L2Modality::Pc, tier, seq, pts_us: seq as u64 * 33_333, event_id: seq + 1, route: None },
            t_release_us: 1_000 + seq as u64,
            t_submit_us: 1_001 + seq as u64,
        }
    }

    fn frame(seq: u32) -> PendingFrame {
        PendingFrame {
            submission: submission(seq, 2),
            timing: PcTiming { decode_us: None, t_render_start_us: 2_000, t_render_end_us: 2_100 },
            info: FrameInfo { points: 1, covered_pixels: 1, depth_scale: 4 },
            pixels: vec![0; 16],
        }
    }

    fn presented(t_us: u64) -> Presented {
        Presented {
            tv_sec: t_us / 1_000_000,
            tv_nsec: ((t_us % 1_000_000) * 1_000 + 999) as u32,
            refresh_ns: 16_666_667,
            seq: 42,
            flags: PRESENTATION_VSYNC | PRESENTATION_HW_CLOCK | PRESENTATION_HW_COMPLETION,
            sync_outputs: 1,
        }
    }

    fn tiers_meta() -> TierScales {
        let text = r#"{"schema": "phase4_s3_tiers_v1", "tiers": {
            "d8": {"tier_id": 2, "depth_scale": 4}, "d7": {"tier_id": 3, "depth_scale": 8},
            "d6": {"tier_id": 4, "depth_scale": 16}}}"#;
        TierScales::parse(text.as_bytes(), "test").unwrap()
    }

    #[test]
    fn presentation_time_is_truncated_to_integer_microseconds() {
        let p = Presented { tv_sec: 12, tv_nsec: 345_678_999, refresh_ns: 16_666_667, seq: 1, flags: 0, sync_outputs: 0 };
        assert_eq!(p.t_us(), Some(12_345_678));
        assert_eq!(p.refresh_us(), 16_666);
        assert_eq!(Presented { tv_nsec: 1_000_000_000, ..p }.t_us(), None);
        assert_eq!(Presented { tv_sec: u64::MAX, ..p }.t_us(), None);
        assert_eq!(presentation_flag_names(0x7), vec!["vsync", "hw_clock", "hw_completion"]);
        assert_eq!(presentation_flag_names(0x18), vec!["zero_copy", "unknown_bits"]);
    }

    #[test]
    fn tiers_meta_maps_header_tier_ids_to_depth_scales() {
        let scales = tiers_meta();
        assert_eq!((scales.depth_scale(2), scales.depth_scale(3), scales.depth_scale(4)), (Some(4), Some(8), Some(16)));
        assert_eq!(scales.depth_scale(0), None);
        assert_eq!(scales.sha256.len(), 64);
        for bad in [
            r#"{"schema": "other", "tiers": {"d8": {"tier_id": 2, "depth_scale": 4}}}"#,
            r#"{"schema": "phase4_s3_tiers_v1", "tiers": {}}"#,
            r#"{"schema": "phase4_s3_tiers_v1", "tiers": {"d8": {"tier_id": 2}}}"#,
            r#"{"schema": "phase4_s3_tiers_v1", "tiers": {"d8": {"tier_id": 2, "depth_scale": 0}}}"#,
            r#"{"schema": "phase4_s3_tiers_v1", "tiers": {"a": {"tier_id": 2, "depth_scale": 4}, "b": {"tier_id": 2, "depth_scale": 8}}}"#,
        ] {
            assert!(TierScales::parse(bad.as_bytes(), "bad").is_err(), "{bad}");
        }
    }

    /// The repository's real tiers meta (read only).
    #[test]
    fn the_s3_tiers_meta_of_the_datasets_parses() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../datasets/tiers_s3/tiers_meta.json");
        let scales = TierScales::load(&path).unwrap();
        assert_eq!(scales.scales.get(&2), Some(&("d8".to_string(), 4)));
        assert_eq!(scales.scales.get(&3), Some(&("d7".to_string(), 8)));
        assert_eq!(scales.scales.get(&4), Some(&("d6".to_string(), 16)));
    }

    fn fixture(name: &str) -> Vec<u8> {
        std::fs::read(Path::new(env!("CARGO_MANIFEST_DIR")).join("../skew-draco-sys/tests/fixtures").join(name)).unwrap()
    }

    fn ticking_clock() -> impl FnMut() -> u64 {
        let mut t = 10_000u64;
        move || {
            t += 7;
            t
        }
    }

    /// Decode times exist only for Draco; bin and Draco of the same content
    /// render to identical pixels.
    #[test]
    fn decode_times_only_for_draco_and_both_representations_render_identically() {
        let bin = fixture("synthetic_255.bin");
        let drc = fixture("synthetic_255.drc");
        let mut bin_prep = FramePreparer::new(PcRepresentation::Bin, tiers_meta(), 256);
        let mut drc_prep = FramePreparer::new(PcRepresentation::Draco, tiers_meta(), 256);
        let a = bin_prep.prepare(submission(1, 2), Some(&bin), Vec::new(), &mut ticking_clock()).unwrap();
        let b = drc_prep.prepare(submission(1, 2), Some(&drc), Vec::new(), &mut ticking_clock()).unwrap();
        assert_eq!(a.timing.decode_us, None, "bin: no decode");
        let (start, end) = b.timing.decode_us.expect("draco: decode times");
        assert!(start < end && end < b.timing.t_render_start_us && b.timing.t_render_start_us < b.timing.t_render_end_us);
        assert_eq!(a.pixels, b.pixels, "identical images");
        assert_eq!(a.info, b.info);
        assert_eq!(a.info.points, 1_500);
    }

    #[test]
    fn unknown_tiers_and_bad_payloads_are_payload_invalid_and_decoder_errors_decode_failed() {
        let bin = fixture("synthetic_255.bin");
        let mut prep = FramePreparer::new(PcRepresentation::Bin, tiers_meta(), 64);
        let (error, pixels) = prep.prepare(submission(1, 9), Some(&bin), Vec::new(), &mut ticking_clock()).unwrap_err();
        assert!(matches!(&error, PrepareError::PayloadInvalid { decode_us: None, .. }), "{error:?}");
        assert!(error.detail().contains("tier 9"));
        assert_eq!(pixels.len(), 64 * 64, "the buffer comes back");
        assert!(matches!(prep.prepare(submission(1, 2), None, Vec::new(), &mut ticking_clock()), Err((PrepareError::PayloadInvalid { .. }, _))));
        // d8 data drawn at the d6 scale leaves the cube.
        assert!(matches!(prep.prepare(submission(1, 4), Some(&bin), Vec::new(), &mut ticking_clock()), Err((PrepareError::PayloadInvalid { .. }, _))));
        let mut drc_prep = FramePreparer::new(PcRepresentation::Draco, tiers_meta(), 64);
        let (error, _) = drc_prep.prepare(submission(1, 2), Some(b"garbage"), Vec::new(), &mut ticking_clock()).unwrap_err();
        let PrepareError::DecodeFailed { decode_us: (s, e), .. } = error else { panic!("{error:?}") };
        assert!(s < e);
    }

    #[test]
    fn a_pending_frame_replaced_before_commit_is_handed_back_once() {
        let slot = PendingSlot::new();
        assert!(matches!(slot.offer(frame(1)), Offer::Accepted { superseded: None }));
        let Offer::Accepted { superseded: Some(old) } = slot.offer(frame(2)) else { panic!("superseded") };
        assert_eq!(old.submission.identity.seq, 1);
        assert_eq!(slot.take().unwrap().submission.identity.seq, 2);
        assert!(slot.take().is_none());
        assert!(matches!(slot.offer(frame(3)), Offer::Accepted { superseded: None }));
        assert_eq!(slot.fail().unwrap().submission.identity.seq, 3, "the waiting frame goes to device_failure");
        let Offer::Refused(refused) = slot.offer(frame(4)) else { panic!("refused after a failure") };
        assert_eq!(refused.submission.identity.seq, 4);
        assert!(slot.failed() && !slot.has_frame());
    }

    fn engine() -> PresentEngine {
        PresentEngine::new(3, &[(0, 0)])
    }

    fn output_of(outcome: Option<EngineOutcome>) -> (u32, u64, u64) {
        match outcome {
            Some(EngineOutcome::Output { frame, t_output_us, t_callback_us, .. }) => {
                (frame.submission.identity.seq, t_output_us, t_callback_us)
            }
            other => panic!("expected an output, got {other:?}"),
        }
    }

    /// A commit -> B released into the pending slot -> A presented: A's
    /// output comes from ITS feedback and B waits for the next opportunity;
    /// A's buffer release before its feedback resolves nothing.
    #[test]
    fn a_commit_then_b_release_then_a_presented() {
        let mut e = engine();
        let slot = PendingSlot::new();
        assert!(e.commit_opportunity());
        assert_eq!(e.free_buffer(), Some(1), "buffer 0 is held by the startup frame");
        let (a, _) = e.commit(frame(1), 1, 5_000);
        assert!(matches!(slot.offer(frame(2)), Offer::Accepted { superseded: None }), "B released meanwhile");
        assert!(!e.commit_opportunity(), "A's frame callback has not fired");
        // A's buffer is released before its feedback: no terminal.
        assert!(e.on_event(BackendEvent::BufferReleased { buffer: 1 }, 5_500).is_none());
        assert_eq!(e.awaiting_feedback(), 1);
        assert!(e.on_event(BackendEvent::FrameDone { commit_id: a }, 6_000).is_none(), "a frame callback is no output");
        assert_eq!(e.awaiting_feedback(), 1);
        assert_eq!(output_of(e.on_event(BackendEvent::Presented { commit_id: a, presented: presented(5_900) }, 6_100)), (1, 5_900, 6_100));
        assert!(e.commit_opportunity());
        let b = slot.take().unwrap();
        assert_eq!(b.submission.identity.seq, 2);
        let (b_id, _) = e.commit(b, e.free_buffer().unwrap(), 6_200);
        assert_eq!(b_id, a + 1);
    }

    /// Feedback arriving in reverse commit order resolves each commit by its
    /// own id.
    #[test]
    fn reversed_feedback_order_is_resolved_by_commit_id() {
        let mut e = engine();
        let (a, _) = e.commit(frame(1), 1, 1_000);
        e.on_event(BackendEvent::FrameDone { commit_id: a }, 1_100);
        let (b, _) = e.commit(frame(2), 2, 1_200);
        e.on_event(BackendEvent::FrameDone { commit_id: b }, 1_300);
        assert_eq!(e.awaiting_feedback(), 2);
        assert_eq!(output_of(e.on_event(BackendEvent::Presented { commit_id: b, presented: presented(1_400) }, 1_450)).0, 2);
        assert_eq!(output_of(e.on_event(BackendEvent::Presented { commit_id: a, presented: presented(1_350) }, 1_500)).0, 1);
        assert_eq!(e.awaiting_feedback(), 0);
    }

    #[test]
    fn a_compositor_discard_terminates_that_commit() {
        let mut e = engine();
        let (a, _) = e.commit(frame(7), 1, 1_000);
        match e.on_event(BackendEvent::Discarded { commit_id: a }, 1_234) {
            Some(EngineOutcome::Discarded { frame, t_callback_us }) => {
                assert_eq!((frame.submission.identity.seq, t_callback_us), (7, 1_234));
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(e.stats().compositor_discarded, 1);
    }

    /// Duplicate or unknown feedback and frame callbacks are timed anomalies
    /// at their receipt; they never produce a second terminal.
    #[test]
    fn duplicate_and_unknown_callbacks_are_anomalies() {
        let mut e = engine();
        let (a, _) = e.commit(frame(1), 1, 1_000);
        output_of(e.on_event(BackendEvent::Presented { commit_id: a, presented: presented(1_100) }, 1_200));
        for (event, reason, detail) in [
            (BackendEvent::Presented { commit_id: a, presented: presented(1_100) }, FAIL_WAYLAND_UNEXPECTED_FEEDBACK, "already resolved"),
            (BackendEvent::Discarded { commit_id: 99 }, FAIL_WAYLAND_UNEXPECTED_FEEDBACK, "never committed"),
            (BackendEvent::FrameDone { commit_id: 99 }, FAIL_WAYLAND_UNEXPECTED_FRAME_CALLBACK, "never committed"),
        ] {
            match e.on_event(event, 2_000) {
                Some(EngineOutcome::Anomaly { reason: r, t_us, detail: d }) => {
                    assert_eq!((r, t_us), (reason, 2_000));
                    assert!(d.contains(detail), "{d}");
                }
                other => panic!("{other:?}"),
            }
        }
        // The real callback still clears the outstanding one.
        assert!(e.on_event(BackendEvent::FrameDone { commit_id: a }, 2_100).is_none());
        assert!(e.on_event(BackendEvent::FrameDone { commit_id: a }, 2_200).is_some(), "a second done is unexpected");
        // A presented with an invalid timestamp: anomaly, no terminal.
        let (b, _) = e.commit(frame(2), e.free_buffer().unwrap(), 3_000);
        let mut bad = presented(3_100);
        bad.tv_nsec = 1_000_000_000;
        assert!(matches!(
            e.on_event(BackendEvent::Presented { commit_id: b, presented: bad }, 3_200),
            Some(EngineOutcome::Anomaly { reason: FAIL_WAYLAND_BAD_TIMESTAMP, .. })
        ));
    }

    /// A buffer the compositor holds is never chosen: with every buffer
    /// held the commit waits for a release.
    #[test]
    fn held_buffers_are_never_reused() {
        let mut e = PresentEngine::new(2, &[(0, 0)]);
        let (a, _) = e.commit(frame(1), e.free_buffer().unwrap(), 1_000);
        e.on_event(BackendEvent::FrameDone { commit_id: a }, 1_100);
        assert!(e.commit_opportunity());
        assert_eq!(e.free_buffer(), None, "0 (startup) and 1 (A) are held");
        e.on_event(BackendEvent::Presented { commit_id: a, presented: presented(1_050) }, 1_150);
        assert_eq!(e.free_buffer(), None, "presentation does not free a buffer");
        e.on_event(BackendEvent::BufferReleased { buffer: 0 }, 1_200);
        assert_eq!(e.free_buffer(), Some(0));
        let (b, _) = e.commit(frame(2), 0, 1_300);
        assert_eq!(e.buffer_states(), &[BufferState::Held { commit_id: b }, BufferState::Held { commit_id: a }]);
        // A release of a free buffer is counted, nothing else.
        e.on_event(BackendEvent::BufferReleased { buffer: 1 }, 1_400);
        e.on_event(BackendEvent::BufferReleased { buffer: 1 }, 1_500);
        assert_eq!(e.stats().unexpected_buffer_releases, 1);
    }

    #[test]
    #[should_panic(expected = "never write a buffer the compositor holds")]
    fn committing_into_a_held_buffer_panics() {
        let mut e = engine();
        e.commit(frame(1), 0, 1_000);
    }

    #[test]
    fn reconfigure_and_close_are_device_failures_and_only_the_first_counts() {
        let mut e = engine();
        assert!(matches!(
            e.on_event(BackendEvent::Reconfigured { detail: "size".into() }, 1),
            Some(EngineOutcome::DeviceFailure { reason: FAIL_WAYLAND_RECONFIGURED, .. })
        ));
        assert!(matches!(e.on_event(BackendEvent::Closed, 2), Some(EngineOutcome::DeviceFailure { reason: FAIL_WAYLAND_WINDOW_CLOSED, .. })));
        assert!(e.fail(FAIL_WAYLAND_RECONFIGURED, 100, 200, "size".into()));
        assert!(!e.fail(FAIL_WAYLAND_CONNECTION_LOST, 300, 400, "later".into()));
        assert_eq!(e.failure().unwrap().reason, FAIL_WAYLAND_RECONFIGURED);
        assert!(!e.commit_opportunity(), "no commit after a device failure");
        let json = e.failure().unwrap().to_json();
        assert_eq!(json["observation_interval_us"], serde_json::json!([100, 200]));
        assert_eq!(json["occurrence_time_determinable"], false);
    }

    /// Frames still waiting for feedback at close are reported, in commit
    /// order, and stay unresolved.
    #[test]
    fn committed_frames_without_feedback_are_listed_at_close() {
        let mut e = engine();
        let (a, _) = e.commit(frame(1), 1, 1_000);
        e.on_event(BackendEvent::FrameDone { commit_id: a }, 1_100);
        let (b, _) = e.commit(frame(2), 2, 1_200);
        let missing: Vec<u64> = e.unresolved().iter().map(|f| f.commit_id).collect();
        assert_eq!(missing, vec![a, b]);
    }

    #[test]
    fn the_waker_wakes_and_times_out() {
        let waker = Waker::new().unwrap();
        let t0 = now_us();
        assert!(!waker.wait(Some(t0 + 2_000)), "timeout");
        assert!(now_us() >= t0 + 2_000);
        waker.wake();
        waker.wake();
        assert!(waker.wait(Some(now_us() + 1_000_000)), "woken");
        assert!(!waker.wait(Some(now_us() + 1_000)), "cleared");
    }

    /// The simulated compositor: present the last commit of a vblank,
    /// discard earlier ones, release the previously shown buffer.
    #[test]
    fn the_simulated_compositor_presents_at_vblanks() {
        let mut cfg = SimCompositorConfig::new(64, 48);
        cfg.refresh_us = 5_000;
        let observer = cfg.observer.clone();
        let mut sim = SimCompositor::new(cfg);
        let waker = Waker::new().unwrap();
        sim.write_frame(1, 48, &vec![0; 48 * 48]).unwrap();
        sim.commit(1, 1).unwrap();
        sim.write_frame(2, 48, &vec![0; 48 * 48]).unwrap();
        sim.commit(2, 2).unwrap();
        let mut events = Vec::new();
        let deadline = now_us() + 1_000_000;
        while events.len() < 5 && now_us() < deadline {
            events.extend(sim.wait(Some(now_us() + 50_000), &waker).unwrap());
        }
        // Usually both commits fall before the same vblank; if a vblank
        // separated them, both are presented. Either way: exactly one
        // terminal per commit, frame callbacks for both, buffer 0 released.
        let terminals: Vec<&BackendEvent> = events.iter().filter(|e| matches!(e, BackendEvent::Presented { .. } | BackendEvent::Discarded { .. })).collect();
        assert_eq!(terminals.len(), 2, "{events:?}");
        assert!(events.contains(&BackendEvent::FrameDone { commit_id: 1 }) && events.contains(&BackendEvent::FrameDone { commit_id: 2 }));
        assert!(events.contains(&BackendEvent::BufferReleased { buffer: 0 }));
        assert!(matches!(terminals[1], BackendEvent::Presented { commit_id: 2, .. }));
        assert_eq!(observer.writes_into_held_buffers.load(Ordering::SeqCst), 0);
        sim.write_frame(0, 48, &vec![0; 48 * 48]).unwrap();
        assert_eq!(observer.writes_into_held_buffers.load(Ordering::SeqCst), 0, "0 was released");
        sim.write_frame(2, 48, &vec![0; 48 * 48]).unwrap();
        assert_eq!(observer.writes_into_held_buffers.load(Ordering::SeqCst), 1, "2 is on screen");
    }
}
