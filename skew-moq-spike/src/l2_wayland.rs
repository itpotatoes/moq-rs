//! The real compositor backend of the L2 PC sink (registration 1-D §3-2;
//! milestone M3): a pure-Rust Wayland client (`wayland-client` 0.31 without
//! the `system` feature: no libwayland at build or run time) with a
//! fullscreen `xdg_toplevel`, `wl_shm` XRGB8888 buffers in one memfd pool,
//! and `wp_presentation` feedback. It implements
//! [`crate::l2_pc::PresentBackend`]; every decision (which buffer, when to
//! commit, what a feedback means for the ledger) is the
//! [`crate::l2_pc::PresentEngine`]'s.
//!
//! CONNECTING OPENS A WINDOW ON THE USER'S DISPLAY. Nothing in the test
//! suite calls [`WaylandBackend::connect`]; only the receiver with
//! `--l2-sink pc=wayland,...` and the approval-gated `l2_wayland_probe` do.
//!
//! Startup (before the L2 meta row; every WAIT for the compositor is bounded
//! by one deadline, while `connect_to_env` — a local socket connect — and
//! the synchronous buffer preparation have no internal deadline; any
//! failure is a typed [`LaneStartError`] naming its stage, Codex 112 P2-3,
//! Codex 114): `connect` (`WAYLAND_DISPLAY`); `registry` (the globals and a
//! bounded `wl_display.sync` round trip — no unbounded `roundtrip`; bind
//! `wl_compositor` >= 4, `wl_shm`, `xdg_wm_base`, `wp_presentation`, every
//! `wl_output`); `clock` (`wp_presentation.clock_id` must be
//! CLOCK_MONOTONIC, registration 3-2); `shm_format` (XRGB8888); `configure`
//! (the first configure must be fullscreen with a size, or the size of the
//! one output); `buffers`; `startup_present` and `startup_callback` (ONE
//! background frame, startup commit 0, not a ledger row: its `presented`
//! AND its frame callback); `single_output` (the surface is on exactly one
//! output, which every later presentation must be synced to). Buffer 0 stays
//! held by the startup frame until the compositor releases it.
//!
//! After ready every configure, surface enter/leave, preferred scale or
//! transform and output scale/mode/removal is recorded with its receipt
//! time ([`PresentBackend::run_log`]); a change that moves the window off
//! its fullscreen size, its single output or its scale is reported as
//! `Reconfigured` (a PC device failure).

use std::collections::BTreeMap;
use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd};

use wayland_client::protocol::{
    wl_buffer::{self, WlBuffer},
    wl_callback::{self, WlCallback},
    wl_compositor::WlCompositor,
    wl_output::{self, WlOutput},
    wl_registry::{self, WlRegistry},
    wl_shm::{self, WlShm},
    wl_shm_pool::WlShmPool,
    wl_surface::{self, WlSurface},
};
use wayland_client::{delegate_noop, Connection, Dispatch, EventQueue, Proxy, QueueHandle, WEnum};
use wayland_protocols::wp::presentation_time::client::{
    wp_presentation::{self, WpPresentation},
    wp_presentation_feedback::{self, WpPresentationFeedback},
};
use wayland_protocols::xdg::shell::client::{
    xdg_surface::{self, XdgSurface},
    xdg_toplevel::{self, XdgToplevel},
    xdg_wm_base::{self, XdgWmBase},
};

use crate::l2_output::LaneStartError;
use crate::l2_pc::{
    ppoll_until, BackendError, BackendEvent, PresentBackend, Presented, StartupInfo, Waker, CLOCK_MONOTONIC_ID,
};
use crate::now_us;

/// The startup commit id (the warm-up frame; never a ledger row).
pub const STARTUP_COMMIT_ID: u64 = 0;
/// `xdg_toplevel.state` fullscreen.
const XDG_STATE_FULLSCREEN: u32 = 2;
/// Bound on each recorded event list of the run log.
const MAX_LOG_RECORDS: usize = 256;

/// The square canvas inside a `width × height` surface: its side and the
/// top-left offset that centres it.
pub fn canvas_layout(width: u32, height: u32) -> (u32, u32, u32) {
    let side = width.min(height);
    (side, (width - side) / 2, (height - side) / 2)
}

/// `xdg_toplevel.configure` states (an array of u32 LE).
pub fn toplevel_states(raw: &[u8]) -> Vec<u32> {
    raw.chunks_exact(4).map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]])).collect()
}

/// Whether a configure after ready keeps the run's window: fullscreen and
/// the ready size (`0×0` means "client decides", i.e. unchanged).
pub fn configure_keeps_window(width: i32, height: i32, states: &[u32], ready: (i32, i32)) -> bool {
    states.contains(&XDG_STATE_FULLSCREEN) && ((width == 0 && height == 0) || (width, height) == ready)
}

#[derive(Debug, Clone, Default)]
struct OutputInfo {
    global_name: u32,
    name: Option<String>,
    description: Option<String>,
    make: Option<String>,
    model: Option<String>,
    /// `(width, height, refresh_mhz)` of the current mode.
    mode: Option<(i32, i32, i32)>,
    scale: Option<i32>,
}

impl OutputInfo {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "global_name": self.global_name,
            "name": self.name,
            "description": self.description,
            "make": self.make,
            "model": self.model,
            "mode_width": self.mode.map(|m| m.0),
            "mode_height": self.mode.map(|m| m.1),
            "mode_refresh_mhz": self.mode.map(|m| m.2),
            "scale": self.scale,
        })
    }
}

/// A bounded list of `{t_us, ...}` records plus the total count.
#[derive(Debug, Default)]
struct EventLog {
    records: Vec<serde_json::Value>,
    total: u64,
}

impl EventLog {
    fn push(&mut self, mut record: serde_json::Value) {
        self.total += 1;
        if self.records.len() < MAX_LOG_RECORDS {
            record["t_us"] = now_us().into();
            self.records.push(record);
        }
    }

    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({ "records": self.records, "total": self.total, "kept": self.records.len() })
    }
}

/// The run's window once ready: what later events are compared with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReadyShape {
    width: i32,
    height: i32,
    output: u32,
    output_scale: Option<i32>,
}

/// User data of the bounded `wl_display.sync` round trips.
struct SyncToken(u32);

/// Dispatch state of the connection (lives on the lane thread).
#[derive(Default)]
struct State {
    clock_id: Option<u32>,
    /// `(name, interface, version)` of every advertised global.
    globals: Vec<(u32, String, u32)>,
    syncs_done: Vec<u32>,
    /// The last toplevel configure `(width, height, states)`.
    toplevel_configure: Option<(i32, i32, Vec<u32>)>,
    configured: bool,
    configures_before_ready: EventLog,
    configures: EventLog,
    surface_events: EventLog,
    output_events: EventLog,
    outputs: BTreeMap<u32, OutputInfo>,
    entered_outputs: Vec<u32>,
    preferred_scale: Option<i32>,
    shm_formats: Vec<u32>,
    ready: Option<ReadyShape>,
    /// `(count, first output)` of the sync_output events of unresolved
    /// feedback objects, by commit id.
    sync_outputs: BTreeMap<u64, (u32, Option<u32>)>,
    events: Vec<BackendEvent>,
}

impl State {
    fn reconfigured(&mut self, detail: String) {
        if self.ready.is_some() {
            self.events.push(BackendEvent::Reconfigured { detail });
        }
    }
}

impl Dispatch<WlRegistry, ()> for State {
    fn event(state: &mut Self, _: &WlRegistry, event: wl_registry::Event, _: &(), _: &Connection, _: &QueueHandle<Self>) {
        match event {
            wl_registry::Event::Global { name, interface, version } => state.globals.push((name, interface, version)),
            wl_registry::Event::GlobalRemove { name } => {
                if state.outputs.contains_key(&name) {
                    state.output_events.push(serde_json::json!({ "event": "output_removed", "output": name }));
                    if state.ready.is_some_and(|r| r.output == name) {
                        state.reconfigured(format!("the startup output {name} was removed"));
                    }
                }
            }
            _ => {}
        }
    }
}

delegate_noop!(State: ignore WlCompositor);
delegate_noop!(State: ignore WlShmPool);

impl Dispatch<WlShm, ()> for State {
    fn event(state: &mut Self, _: &WlShm, event: wl_shm::Event, _: &(), _: &Connection, _: &QueueHandle<Self>) {
        if let wl_shm::Event::Format { format } = event {
            state.shm_formats.push(match format {
                WEnum::Value(f) => f as u32,
                WEnum::Unknown(raw) => raw,
            });
        }
    }
}

impl Dispatch<WlBuffer, usize> for State {
    fn event(state: &mut Self, _: &WlBuffer, event: wl_buffer::Event, index: &usize, _: &Connection, _: &QueueHandle<Self>) {
        if let wl_buffer::Event::Release = event {
            state.events.push(BackendEvent::BufferReleased { buffer: *index });
        }
    }
}

/// Frame callbacks carry the commit id.
impl Dispatch<WlCallback, u64> for State {
    fn event(state: &mut Self, _: &WlCallback, event: wl_callback::Event, commit_id: &u64, _: &Connection, _: &QueueHandle<Self>) {
        if let wl_callback::Event::Done { .. } = event {
            state.events.push(BackendEvent::FrameDone { commit_id: *commit_id });
        }
    }
}

/// The bounded round trips' callbacks.
impl Dispatch<WlCallback, SyncToken> for State {
    fn event(state: &mut Self, _: &WlCallback, event: wl_callback::Event, token: &SyncToken, _: &Connection, _: &QueueHandle<Self>) {
        if let wl_callback::Event::Done { .. } = event {
            state.syncs_done.push(token.0);
        }
    }
}

impl Dispatch<WlSurface, ()> for State {
    fn event(state: &mut Self, _: &WlSurface, event: wl_surface::Event, _: &(), _: &Connection, _: &QueueHandle<Self>) {
        match event {
            wl_surface::Event::Enter { output } => {
                let name = output.data::<u32>().copied();
                state.surface_events.push(serde_json::json!({ "event": "enter", "output": name }));
                if let Some(name) = name {
                    state.entered_outputs.push(name);
                    state.reconfigured(format!("the surface entered output {name} after ready"));
                }
            }
            wl_surface::Event::Leave { output } => {
                let name = output.data::<u32>().copied();
                state.surface_events.push(serde_json::json!({ "event": "leave", "output": name }));
                if let Some(name) = name {
                    state.entered_outputs.retain(|n| *n != name);
                    state.reconfigured(format!("the surface left output {name} after ready"));
                }
            }
            wl_surface::Event::PreferredBufferScale { factor } => {
                state.surface_events.push(serde_json::json!({ "event": "preferred_buffer_scale", "factor": factor }));
                let previous = state.preferred_scale.replace(factor);
                if previous.is_some_and(|p| p != factor) {
                    state.reconfigured(format!("preferred buffer scale {previous:?} -> {factor} after ready"));
                }
            }
            wl_surface::Event::PreferredBufferTransform { transform } => {
                let value = match transform {
                    WEnum::Value(t) => t as u32,
                    WEnum::Unknown(raw) => raw,
                };
                state.surface_events.push(serde_json::json!({ "event": "preferred_buffer_transform", "transform": value }));
            }
            _ => {}
        }
    }
}

impl Dispatch<WlOutput, u32> for State {
    fn event(state: &mut Self, _: &WlOutput, event: wl_output::Event, name: &u32, _: &Connection, _: &QueueHandle<Self>) {
        let ready = state.ready;
        let is_run_output = ready.is_some_and(|r| r.output == *name);
        let info = state.outputs.entry(*name).or_insert_with(|| OutputInfo { global_name: *name, ..Default::default() });
        match event {
            wl_output::Event::Geometry { make, model, .. } => {
                info.make = Some(make);
                info.model = Some(model);
            }
            wl_output::Event::Mode { flags, width, height, refresh } => {
                let current = matches!(flags, WEnum::Value(f) if f.contains(wl_output::Mode::Current));
                if current {
                    let previous = info.mode.replace((width, height, refresh));
                    if ready.is_some() {
                        state.output_events.push(serde_json::json!({
                            "event": "mode", "output": name, "width": width, "height": height, "refresh_mhz": refresh,
                        }));
                    }
                    if is_run_output && previous.is_some_and(|p| p != (width, height, refresh)) {
                        let detail = format!("output {name} mode {previous:?} -> {width}x{height}@{refresh}mHz after ready");
                        state.reconfigured(detail);
                    }
                }
            }
            wl_output::Event::Scale { factor } => {
                let previous = info.scale.replace(factor);
                if ready.is_some() {
                    state.output_events.push(serde_json::json!({ "event": "scale", "output": name, "factor": factor }));
                }
                if is_run_output && previous.is_some_and(|p| p != factor) {
                    state.reconfigured(format!("output {name} scale {previous:?} -> {factor} after ready"));
                }
            }
            wl_output::Event::Name { name: output_name } => info.name = Some(output_name),
            wl_output::Event::Description { description } => info.description = Some(description),
            _ => {}
        }
    }
}

impl Dispatch<XdgWmBase, ()> for State {
    fn event(_: &mut Self, wm_base: &XdgWmBase, event: xdg_wm_base::Event, _: &(), _: &Connection, _: &QueueHandle<Self>) {
        if let xdg_wm_base::Event::Ping { serial } = event {
            wm_base.pong(serial);
        }
    }
}

impl Dispatch<XdgSurface, ()> for State {
    fn event(state: &mut Self, surface: &XdgSurface, event: xdg_surface::Event, _: &(), _: &Connection, _: &QueueHandle<Self>) {
        if let xdg_surface::Event::Configure { serial } = event {
            surface.ack_configure(serial);
            state.configured = true;
            let (w, h, states) = state.toplevel_configure.clone().unwrap_or((0, 0, Vec::new()));
            let record = serde_json::json!({ "serial": serial, "width": w, "height": h, "states": states });
            // Codex 112 / M3b C: every configure is recorded, not only the first.
            match state.ready {
                None => state.configures_before_ready.push(record),
                Some(ready) => {
                    state.configures.push(record);
                    if !configure_keeps_window(w, h, &states, (ready.width, ready.height)) {
                        let detail = format!(
                            "configure {w}x{h} states {states:?} after ready ({}x{} fullscreen)",
                            ready.width, ready.height
                        );
                        state.reconfigured(detail);
                    }
                }
            }
        }
    }
}

impl Dispatch<XdgToplevel, ()> for State {
    fn event(state: &mut Self, _: &XdgToplevel, event: xdg_toplevel::Event, _: &(), _: &Connection, _: &QueueHandle<Self>) {
        match event {
            xdg_toplevel::Event::Configure { width, height, states } => {
                state.toplevel_configure = Some((width, height, toplevel_states(&states)));
            }
            xdg_toplevel::Event::Close => state.events.push(BackendEvent::Closed),
            _ => {}
        }
    }
}

impl Dispatch<WpPresentation, ()> for State {
    fn event(state: &mut Self, _: &WpPresentation, event: wp_presentation::Event, _: &(), _: &Connection, _: &QueueHandle<Self>) {
        if let wp_presentation::Event::ClockId { clk_id } = event {
            state.clock_id = Some(clk_id);
        }
    }
}

impl Dispatch<WpPresentationFeedback, u64> for State {
    fn event(state: &mut Self, _: &WpPresentationFeedback, event: wp_presentation_feedback::Event, commit_id: &u64, _: &Connection, _: &QueueHandle<Self>) {
        match event {
            wp_presentation_feedback::Event::SyncOutput { output } => {
                // Codex 112 P2-4: keep WHICH output, not only how many.
                let name = output.data::<u32>().copied();
                let entry = state.sync_outputs.entry(*commit_id).or_insert((0, None));
                entry.0 += 1;
                if entry.1.is_none() {
                    entry.1 = name;
                }
            }
            wp_presentation_feedback::Event::Presented { tv_sec_hi, tv_sec_lo, tv_nsec, refresh, seq_hi, seq_lo, flags } => {
                let flags = match flags {
                    WEnum::Value(kind) => kind.bits(),
                    WEnum::Unknown(raw) => raw,
                };
                let (sync_outputs, sync_output) = state.sync_outputs.remove(commit_id).unwrap_or((0, None));
                let presented = Presented {
                    tv_sec: ((tv_sec_hi as u64) << 32) | tv_sec_lo as u64,
                    tv_nsec,
                    refresh_ns: refresh,
                    seq: ((seq_hi as u64) << 32) | seq_lo as u64,
                    flags,
                    sync_outputs,
                    sync_output,
                };
                state.events.push(BackendEvent::Presented { commit_id: *commit_id, presented });
            }
            wp_presentation_feedback::Event::Discarded => {
                state.sync_outputs.remove(commit_id);
                state.events.push(BackendEvent::Discarded { commit_id: *commit_id });
            }
            _ => {}
        }
    }
}

/// The shared-memory pool: one memfd mapping holding every buffer.
struct ShmPool {
    _fd: OwnedFd,
    ptr: *mut u8,
    len: usize,
}

impl ShmPool {
    fn new(len: usize) -> Result<Self, String> {
        // SAFETY: plain syscalls; the fd is owned below, the mapping unmapped on drop.
        unsafe {
            let fd = libc::memfd_create(c"skew-l2-pc".as_ptr(), libc::MFD_CLOEXEC);
            if fd < 0 {
                return Err(format!("memfd_create: {}", std::io::Error::last_os_error()));
            }
            let fd = OwnedFd::from_raw_fd(fd);
            if libc::ftruncate(fd.as_raw_fd(), len as libc::off_t) != 0 {
                return Err(format!("ftruncate: {}", std::io::Error::last_os_error()));
            }
            let ptr = libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ | libc::PROT_WRITE, libc::MAP_SHARED, fd.as_raw_fd(), 0);
            if ptr == libc::MAP_FAILED {
                return Err(format!("mmap: {}", std::io::Error::last_os_error()));
            }
            Ok(Self { _fd: fd, ptr: ptr.cast(), len })
        }
    }

    fn pixels_mut(&mut self, offset: usize, count: usize) -> &mut [u32] {
        assert!(offset % 4 == 0 && offset + count * 4 <= self.len, "inside the pool");
        // SAFETY: in bounds (checked), 4-aligned (page-aligned mapping), exclusively borrowed.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.add(offset).cast::<u32>(), count) }
    }
}

impl Drop for ShmPool {
    fn drop(&mut self) {
        // SAFETY: the mapping created in `new`.
        unsafe { libc::munmap(self.ptr.cast(), self.len) };
    }
}

/// A connected, fullscreen, ready-to-commit Wayland surface.
pub struct WaylandBackend {
    conn: Connection,
    queue: EventQueue<State>,
    qh: QueueHandle<State>,
    state: State,
    surface: WlSurface,
    presentation: WpPresentation,
    buffers: Vec<WlBuffer>,
    pool: ShmPool,
    width: u32,
    height: u32,
    startup: StartupInfo,
    // Kept alive for the connection's lifetime.
    _objects: (WlCompositor, WlShm, XdgWmBase, XdgSurface, XdgToplevel, WlShmPool, WlRegistry),
}

fn lost(what: &str, error: impl std::fmt::Display) -> BackendError {
    BackendError { detail: format!("{what}: {error}") }
}

/// Why a bounded wait ended without its condition.
struct WaitFailed {
    timed_out: bool,
    detail: String,
}

impl WaitFailed {
    fn at(self, stage: &'static str) -> LaneStartError {
        LaneStartError::new(stage, self.timed_out, self.detail)
    }
}

impl WaylandBackend {
    /// Connect and prepare the fullscreen surface (see the module docs);
    /// every wait for the compositor ends by `timeout_us` from the call
    /// (`connect_to_env` and the buffer preparation have no deadline of their
    /// own). OPENS A WINDOW ON THE USER'S DISPLAY.
    pub fn connect(buffer_count: usize, timeout_us: u64) -> Result<Self, LaneStartError> {
        let fail = |stage: &'static str, detail: String| LaneStartError::new(stage, false, detail);
        if buffer_count < 2 {
            return Err(fail("buffers", "at least 2 wl_shm buffers".into()));
        }
        let deadline = now_us().saturating_add(timeout_us);
        let display_name = std::env::var("WAYLAND_DISPLAY").ok();
        let conn = Connection::connect_to_env().map_err(|e| fail("connect", format!("connect to the Wayland display: {e}")))?;
        let mut queue = conn.new_event_queue::<State>();
        let qh = queue.handle();
        let display = conn.display();
        let registry = display.get_registry(&qh, ());
        let mut state = State::default();
        // Startup waits use their own eventfd (never signalled).
        let waker = Waker::new().map_err(|e| fail("connect", format!("eventfd: {e}")))?;
        // `registry`: the globals arrive before the reply to a sync.
        display.sync(&qh, SyncToken(1));
        Self::dispatch_until(&conn, &mut queue, &mut state, &waker, deadline, |s| s.syncs_done.contains(&1))
            .map_err(|e| e.at("registry"))?;
        let global = |interface: &str, min: u32| -> Result<(u32, u32), LaneStartError> {
            match state.globals.iter().find(|(_, i, _)| i == interface) {
                Some((name, _, version)) if *version >= min => Ok((*name, *version)),
                Some((_, _, version)) => Err(fail("registry", format!("{interface} version {version} < {min}"))),
                None => Err(fail("registry", format!("no {interface} global"))),
            }
        };
        let (name, version) = global("wl_compositor", 4)?;
        let compositor: WlCompositor = registry.bind(name, version.min(6), &qh, ());
        let (name, _) = global("wl_shm", 1)?;
        let shm: WlShm = registry.bind(name, 1, &qh, ());
        let (name, version) = global("xdg_wm_base", 1)?;
        let wm_base: XdgWmBase = registry.bind(name, version.min(6), &qh, ());
        let (name, version) = global("wp_presentation", 1)?;
        let presentation: WpPresentation = registry.bind(name, version.min(2), &qh, ());
        let mut bound = serde_json::Map::new();
        for (interface, proxy_version) in [
            ("wl_compositor", compositor.version()),
            ("wl_shm", shm.version()),
            ("xdg_wm_base", wm_base.version()),
            ("wp_presentation", presentation.version()),
        ] {
            bound.insert(interface.into(), proxy_version.into());
        }
        let outputs: Vec<(u32, u32)> = state
            .globals
            .iter()
            .filter(|(_, interface, _)| interface == "wl_output")
            .map(|(name, _, version)| (*name, *version))
            .collect();
        for (name, version) in outputs {
            let _output: WlOutput = registry.bind(name, version.min(4), &qh, name);
            state.outputs.insert(name, OutputInfo { global_name: name, ..Default::default() });
        }
        // The clock id, the outputs' facts and the shm formats come before
        // the reply to a second sync.
        display.sync(&qh, SyncToken(2));
        Self::dispatch_until(&conn, &mut queue, &mut state, &waker, deadline, |s| s.syncs_done.contains(&2))
            .map_err(|e| e.at("registry"))?;
        let clock_id = state.clock_id.ok_or_else(|| fail("clock", "wp_presentation sent no clock_id".into()))?;
        if clock_id != CLOCK_MONOTONIC_ID {
            return Err(fail(
                "clock",
                format!("wp_presentation clock_id {clock_id} is not CLOCK_MONOTONIC ({CLOCK_MONOTONIC_ID}): the run refuses to start"),
            ));
        }
        if !state.shm_formats.contains(&(wl_shm::Format::Xrgb8888 as u32)) {
            return Err(fail("shm_format", "wl_shm does not offer XRGB8888".into()));
        }
        // `configure`
        let surface = compositor.create_surface(&qh, ());
        let xdg_surface = wm_base.get_xdg_surface(&surface, &qh, ());
        let toplevel = xdg_surface.get_toplevel(&qh, ());
        toplevel.set_title("skew-l2".into());
        toplevel.set_app_id("skew-l2".into());
        toplevel.set_fullscreen(None);
        surface.commit();
        Self::dispatch_until(&conn, &mut queue, &mut state, &waker, deadline, |s| s.configured)
            .map_err(|e| e.at("configure"))?;
        let (w, h, states) = state.toplevel_configure.clone().ok_or_else(|| fail("configure", "no xdg_toplevel configure".into()))?;
        if !states.contains(&XDG_STATE_FULLSCREEN) {
            return Err(fail("configure", format!("the compositor did not grant fullscreen (states {states:?})")));
        }
        let (width, height) = if w > 0 && h > 0 {
            (w as u32, h as u32)
        } else {
            let modes: Vec<(i32, i32, i32)> = state.outputs.values().filter_map(|o| o.mode).collect();
            match modes.as_slice() {
                [(mw, mh, _)] if *mw > 0 && *mh > 0 => (*mw as u32, *mh as u32),
                _ => return Err(fail("configure", format!("fullscreen configure without a size and {} output modes", modes.len()))),
            }
        };
        // `buffers`
        let buffer_bytes = width as usize * height as usize * 4;
        let total = buffer_bytes
            .checked_mul(buffer_count)
            .filter(|t| *t <= i32::MAX as usize)
            .ok_or_else(|| fail("buffers", "pool too large".into()))?;
        let mut pool = ShmPool::new(total).map_err(|e| fail("buffers", e))?;
        pool.pixels_mut(0, total / 4).fill(skew_pc_render::BACKGROUND);
        let shm_pool = shm.create_pool(pool._fd.as_fd(), total as i32, &qh, ());
        let buffers: Vec<WlBuffer> = (0..buffer_count)
            .map(|i| {
                shm_pool.create_buffer(
                    (i * buffer_bytes) as i32,
                    width as i32,
                    height as i32,
                    (width * 4) as i32,
                    wl_shm::Format::Xrgb8888,
                    &qh,
                    i,
                )
            })
            .collect();
        // `startup_present`: the warm-up frame (buffer 0, background), frame
        // callback and presentation feedback with the startup commit id.
        surface.attach(Some(&buffers[0]), 0, 0);
        surface.damage_buffer(0, 0, width as i32, height as i32);
        surface.frame(&qh, STARTUP_COMMIT_ID);
        presentation.feedback(&surface, &qh, STARTUP_COMMIT_ID);
        let t_commit_us = now_us();
        surface.commit();
        let is_startup_feedback = |e: &BackendEvent| {
            matches!(e, BackendEvent::Presented { commit_id: STARTUP_COMMIT_ID, .. } | BackendEvent::Discarded { commit_id: STARTUP_COMMIT_ID })
        };
        let closed = |s: &State| s.events.contains(&BackendEvent::Closed);
        Self::dispatch_until(&conn, &mut queue, &mut state, &waker, deadline, |s| closed(s) || s.events.iter().any(is_startup_feedback))
            .map_err(|e| e.at("startup_present"))?;
        if closed(&state) {
            return Err(fail("startup_present", "the window was closed during startup".into()));
        }
        let presented = state.events.iter().find_map(|e| match e {
            BackendEvent::Presented { commit_id: STARTUP_COMMIT_ID, presented } => Some(*presented),
            _ => None,
        });
        let Some(presented) = presented else {
            return Err(fail("startup_present", "the compositor discarded the startup frame".into()));
        };
        let t_presented = presented
            .t_us()
            .ok_or_else(|| fail("startup_present", "startup presentation with an invalid timestamp".into()))?;
        // `startup_callback`
        Self::dispatch_until(&conn, &mut queue, &mut state, &waker, deadline, |s| {
            s.events.contains(&BackendEvent::FrameDone { commit_id: STARTUP_COMMIT_ID })
        })
        .map_err(|e| e.at("startup_callback"))?;
        // `single_output`: the surface must be on exactly one output.
        Self::dispatch_until(&conn, &mut queue, &mut state, &waker, deadline, |s| !s.entered_outputs.is_empty())
            .map_err(|e| e.at("single_output"))?;
        let output = match state.entered_outputs.as_slice() {
            [one] => *one,
            many => return Err(fail("single_output", format!("the surface is on {} outputs {many:?}", many.len()))),
        };
        // Keep only the events of the run: releases may already have come.
        let mut released_startup_buffer = false;
        state.events.retain(|e| match e {
            BackendEvent::BufferReleased { buffer: 0 } => {
                released_startup_buffer = true;
                false
            }
            BackendEvent::FrameDone { commit_id: STARTUP_COMMIT_ID } => false,
            e if is_startup_feedback(e) => false,
            _ => true,
        });
        let output_scale = state.outputs.get(&output).and_then(|o| o.scale);
        state.ready = Some(ReadyShape { width: width as i32, height: height as i32, output, output_scale });
        let held = if released_startup_buffer { Vec::new() } else { vec![(0, STARTUP_COMMIT_ID)] };
        let details = serde_json::json!({
            "display": display_name,
            "bound_versions": bound,
            "shm_format": "XRGB8888",
            "shm_formats_offered": state.shm_formats.len(),
            "outputs": state.outputs.values().map(OutputInfo::to_json).collect::<Vec<_>>(),
            "surface_entered_outputs": state.entered_outputs,
            "preferred_buffer_scale": state.preferred_scale,
            "first_configure": { "width": w, "height": h, "states": states },
            "configures_before_ready": state.configures_before_ready.to_json(),
            "surface_events_before_ready": state.surface_events.to_json(),
            "startup_commit_id": STARTUP_COMMIT_ID,
            "startup_buffer_released_before_ready": released_startup_buffer,
        });
        // The run log starts at ready.
        state.surface_events = EventLog::default();
        let startup = StartupInfo {
            backend: "wayland",
            clock_id,
            width,
            height,
            buffers: buffer_count,
            held,
            startup_presented: Some(presented),
            startup_commit_to_presented_us: Some(t_presented as i64 - t_commit_us as i64),
            output: Some(output),
            details,
        };
        Ok(Self {
            conn,
            queue,
            qh,
            state,
            surface,
            presentation,
            buffers,
            pool,
            width,
            height,
            startup,
            _objects: (compositor, shm, wm_base, xdg_surface, toplevel, shm_pool, registry),
        })
    }

    /// Read and dispatch until `done(state)` or the deadline.
    fn dispatch_until(
        conn: &Connection,
        queue: &mut EventQueue<State>,
        state: &mut State,
        waker: &Waker,
        deadline_us: u64,
        done: impl Fn(&State) -> bool,
    ) -> Result<(), WaitFailed> {
        loop {
            queue
                .dispatch_pending(state)
                .map_err(|e| WaitFailed { timed_out: false, detail: format!("dispatch: {e}") })?;
            if done(state) {
                return Ok(());
            }
            if now_us() >= deadline_us {
                return Err(WaitFailed { timed_out: true, detail: "timed out".into() });
            }
            Self::read_once(conn, queue, waker, Some(deadline_us))
                .map_err(|e| WaitFailed { timed_out: false, detail: e.detail })?;
        }
    }

    /// Flush, then wait for the socket (or the waker / deadline) and read.
    fn read_once(conn: &Connection, queue: &EventQueue<State>, waker: &Waker, until_us: Option<u64>) -> Result<(), BackendError> {
        match conn.flush() {
            Ok(()) => {}
            Err(wayland_client::backend::WaylandError::Io(e)) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(e) => return Err(lost("flush", Self::describe(conn, e))),
        }
        let Some(guard) = queue.prepare_read() else {
            return Ok(()); // events already queued: dispatch them first
        };
        let mut fds = [
            libc::pollfd { fd: guard.connection_fd().as_raw_fd(), events: libc::POLLIN, revents: 0 },
            libc::pollfd { fd: waker.raw_fd(), events: libc::POLLIN, revents: 0 },
        ];
        ppoll_until(&mut fds, until_us).map_err(|e| lost("ppoll", e))?;
        if fds[0].revents & (libc::POLLIN | libc::POLLERR | libc::POLLHUP) != 0 {
            match guard.read() {
                Ok(_) => {}
                Err(wayland_client::backend::WaylandError::Io(e)) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(lost("read", Self::describe(conn, e))),
            }
        } else {
            drop(guard);
        }
        if fds[1].revents & libc::POLLIN != 0 {
            waker.clear();
        }
        Ok(())
    }

    fn describe(conn: &Connection, error: wayland_client::backend::WaylandError) -> String {
        match conn.protocol_error() {
            Some(p) => format!("{error} (protocol error {} on {}: {})", p.code, p.object_interface, p.message),
            None => error.to_string(),
        }
    }

    fn dispatch_pending(&mut self) -> Result<(), BackendError> {
        self.queue.dispatch_pending(&mut self.state).map_err(|e| lost("dispatch", e))?;
        Ok(())
    }
}

impl PresentBackend for WaylandBackend {
    fn startup(&self) -> &StartupInfo {
        &self.startup
    }

    fn write_frame(&mut self, buffer: usize, side: u32, pixels: &[u32]) -> Result<(), BackendError> {
        let (canvas, x0, y0) = canvas_layout(self.width, self.height);
        assert!(side == canvas && pixels.len() == side as usize * side as usize, "a canvas-sized frame");
        let width = self.width as usize;
        let base = buffer * width * self.height as usize;
        let target = self.pool.pixels_mut(base * 4, width * self.height as usize);
        for row in 0..side as usize {
            let dst = (y0 as usize + row) * width + x0 as usize;
            let src = row * side as usize;
            target[dst..dst + side as usize].copy_from_slice(&pixels[src..src + side as usize]);
        }
        Ok(())
    }

    fn commit(&mut self, buffer: usize, commit_id: u64) -> Result<(), BackendError> {
        self.surface.attach(Some(&self.buffers[buffer]), 0, 0);
        self.surface.damage_buffer(0, 0, self.width as i32, self.height as i32);
        self.surface.frame(&self.qh, commit_id);
        // Requested immediately before the commit (registration 3-2).
        self.presentation.feedback(&self.surface, &self.qh, commit_id);
        self.surface.commit();
        match self.conn.flush() {
            Ok(()) => Ok(()),
            Err(wayland_client::backend::WaylandError::Io(e)) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(()),
            Err(e) => Err(lost("flush after commit", Self::describe(&self.conn, e))),
        }
    }

    fn wait(&mut self, until_us: Option<u64>, waker: &Waker) -> Result<Vec<BackendEvent>, BackendError> {
        self.dispatch_pending()?;
        if self.state.events.is_empty() {
            Self::read_once(&self.conn, &self.queue, waker, until_us)?;
            self.dispatch_pending()?;
        }
        Ok(std::mem::take(&mut self.state.events))
    }

    fn run_log(&self) -> serde_json::Value {
        serde_json::json!({
            "backend": "wayland",
            "configures": self.state.configures.to_json(),
            "surface_events": self.state.surface_events.to_json(),
            "output_events": self.state.output_events.to_json(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Never connects: only the pure helpers are tested here.

    #[test]
    fn the_canvas_is_the_centred_square() {
        assert_eq!(canvas_layout(1920, 1080), (1080, 420, 0));
        assert_eq!(canvas_layout(1080, 1920), (1080, 0, 420));
        assert_eq!(canvas_layout(64, 64), (64, 0, 0));
        assert_eq!(canvas_layout(65, 64), (64, 0, 0));
    }

    #[test]
    fn toplevel_states_are_u32_le() {
        let raw: Vec<u8> = [2u32, 4].iter().flat_map(|s| s.to_le_bytes()).collect();
        assert_eq!(toplevel_states(&raw), vec![XDG_STATE_FULLSCREEN, 4]);
        assert!(toplevel_states(&[1, 2, 3]).is_empty());
    }

    /// A configure after ready keeps the window only when it stays
    /// fullscreen at the ready size (0x0 = client decides). A focus change
    /// (activated added or removed) is no reconfigure, but it is logged.
    #[test]
    fn only_size_or_fullscreen_changes_are_reconfigures() {
        let ready = (1920, 1080);
        assert!(configure_keeps_window(1920, 1080, &[2], ready));
        assert!(configure_keeps_window(1920, 1080, &[2, 4], ready), "activated added");
        assert!(configure_keeps_window(0, 0, &[2], ready));
        assert!(!configure_keeps_window(1920, 1080, &[4], ready), "fullscreen dropped");
        assert!(!configure_keeps_window(1280, 720, &[2], ready), "size changed");
    }

    #[test]
    fn the_event_log_is_bounded_and_counts_everything() {
        let mut log = EventLog::default();
        for i in 0..(MAX_LOG_RECORDS + 10) {
            log.push(serde_json::json!({ "i": i }));
        }
        let json = log.to_json();
        assert_eq!(json["total"], MAX_LOG_RECORDS as u64 + 10);
        assert_eq!(json["kept"], MAX_LOG_RECORDS);
        assert!(json["records"][0]["t_us"].as_u64().is_some());
    }
}
