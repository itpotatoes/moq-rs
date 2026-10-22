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
//! Startup (before the L2 meta row; any failure refuses the run): connect
//! (`WAYLAND_DISPLAY`), bind `wl_compositor` (>= 4, `damage_buffer`),
//! `wl_shm`, `xdg_wm_base`, `wp_presentation`, every `wl_output`; require
//! `wp_presentation.clock_id == CLOCK_MONOTONIC` (registration: otherwise
//! the run refuses to start); create the surface, request fullscreen,
//! wait for the first configure (which must be fullscreen with a size, or
//! the size of the one output), create the buffers, then present ONE
//! background frame (startup commit 0, not a ledger row) and wait for both
//! its frame callback and its `presented` feedback — the presentation path
//! works, and its refresh/flags go into meta. Buffer 0 stays held by that
//! frame until the compositor releases it.

use std::collections::BTreeMap;
use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd};

use wayland_client::globals::{registry_queue_init, GlobalListContents};
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

use crate::l2_pc::{
    ppoll_until, BackendError, BackendEvent, PresentBackend, Presented, StartupInfo, Waker, CLOCK_MONOTONIC_ID,
};
use crate::now_us;

/// The startup commit id (the warm-up frame; never a ledger row).
pub const STARTUP_COMMIT_ID: u64 = 0;
/// `xdg_toplevel.state` fullscreen.
const XDG_STATE_FULLSCREEN: u32 = 2;

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
    done: bool,
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

/// Dispatch state of the connection (lives on the lane thread).
#[derive(Default)]
struct State {
    clock_id: Option<u32>,
    /// The last toplevel configure `(width, height, states)` and whether an
    /// xdg_surface configure confirmed it.
    toplevel_configure: Option<(i32, i32, Vec<u32>)>,
    configured: bool,
    outputs: BTreeMap<u32, OutputInfo>,
    entered_outputs: Vec<u32>,
    shm_formats: Vec<u32>,
    /// Set once the run started: changes become `Reconfigured` events.
    ready: Option<ReadyShape>,
    /// sync_output counts of feedback objects not yet resolved, by commit id.
    sync_outputs: BTreeMap<u64, u32>,
    events: Vec<BackendEvent>,
    closed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReadyShape {
    width: i32,
    height: i32,
}

impl Dispatch<WlRegistry, GlobalListContents> for State {
    fn event(state: &mut Self, _: &WlRegistry, event: wl_registry::Event, _: &GlobalListContents, _: &Connection, _: &QueueHandle<Self>) {
        if let wl_registry::Event::GlobalRemove { name } = event {
            if state.outputs.contains_key(&name) && state.ready.is_some() {
                state.events.push(BackendEvent::Reconfigured { detail: format!("wl_output global {name} removed") });
            }
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

impl Dispatch<WlCallback, u64> for State {
    fn event(state: &mut Self, _: &WlCallback, event: wl_callback::Event, commit_id: &u64, _: &Connection, _: &QueueHandle<Self>) {
        if let wl_callback::Event::Done { .. } = event {
            state.events.push(BackendEvent::FrameDone { commit_id: *commit_id });
        }
    }
}

impl Dispatch<WlSurface, ()> for State {
    fn event(state: &mut Self, _: &WlSurface, event: wl_surface::Event, _: &(), _: &Connection, _: &QueueHandle<Self>) {
        match event {
            wl_surface::Event::Enter { output } => {
                if let Some(name) = output.data::<u32>() {
                    state.entered_outputs.push(*name);
                }
            }
            wl_surface::Event::Leave { output } => {
                if let Some(name) = output.data::<u32>() {
                    state.entered_outputs.retain(|n| n != name);
                }
            }
            _ => {}
        }
    }
}

impl Dispatch<WlOutput, u32> for State {
    fn event(state: &mut Self, _: &WlOutput, event: wl_output::Event, name: &u32, _: &Connection, _: &QueueHandle<Self>) {
        let ready = state.ready.is_some();
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
                    if ready && previous.is_some_and(|p| p != (width, height, refresh)) {
                        let detail = format!(
                            "output {name} mode {:?} -> {width}x{height}@{refresh}mHz",
                            previous.expect("checked")
                        );
                        state.events.push(BackendEvent::Reconfigured { detail });
                    }
                }
            }
            wl_output::Event::Scale { factor } => info.scale = Some(factor),
            wl_output::Event::Name { name } => info.name = Some(name),
            wl_output::Event::Description { description } => info.description = Some(description),
            wl_output::Event::Done => info.done = true,
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
            if let (Some(ready), Some((w, h, states))) = (state.ready, &state.toplevel_configure) {
                let fullscreen = states.contains(&XDG_STATE_FULLSCREEN);
                let same_size = (*w == 0 && *h == 0) || (*w, *h) == (ready.width, ready.height);
                if !fullscreen || !same_size {
                    let detail = format!("configure {w}x{h} fullscreen={fullscreen} after ready ({}x{})", ready.width, ready.height);
                    state.events.push(BackendEvent::Reconfigured { detail });
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
            xdg_toplevel::Event::Close => {
                state.closed = true;
                state.events.push(BackendEvent::Closed);
            }
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
            wp_presentation_feedback::Event::SyncOutput { .. } => {
                *state.sync_outputs.entry(*commit_id).or_default() += 1;
            }
            wp_presentation_feedback::Event::Presented { tv_sec_hi, tv_sec_lo, tv_nsec, refresh, seq_hi, seq_lo, flags } => {
                let flags = match flags {
                    WEnum::Value(kind) => kind.bits(),
                    WEnum::Unknown(raw) => raw,
                };
                let presented = Presented {
                    tv_sec: ((tv_sec_hi as u64) << 32) | tv_sec_lo as u64,
                    tv_nsec,
                    refresh_ns: refresh,
                    seq: ((seq_hi as u64) << 32) | seq_lo as u64,
                    flags,
                    sync_outputs: state.sync_outputs.remove(commit_id).unwrap_or(0),
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
    _objects: (WlCompositor, WlShm, XdgWmBase, XdgSurface, XdgToplevel, WlShmPool),
}

fn lost(what: &str, error: impl std::fmt::Display) -> BackendError {
    BackendError { detail: format!("{what}: {error}") }
}

impl WaylandBackend {
    /// Connect and prepare the fullscreen surface (see the module docs).
    /// OPENS A WINDOW ON THE USER'S DISPLAY.
    pub fn connect(buffer_count: usize, timeout_us: u64) -> Result<Self, String> {
        if buffer_count < 2 {
            return Err("at least 2 wl_shm buffers".into());
        }
        let deadline = now_us().saturating_add(timeout_us);
        let display_name = std::env::var("WAYLAND_DISPLAY").ok();
        let conn = Connection::connect_to_env().map_err(|e| format!("connect to the Wayland display: {e}"))?;
        let (globals, mut queue) = registry_queue_init::<State>(&conn).map_err(|e| format!("registry: {e}"))?;
        let qh = queue.handle();
        let compositor: WlCompositor = globals.bind(&qh, 4..=6, ()).map_err(|e| format!("wl_compositor >= 4: {e}"))?;
        let shm: WlShm = globals.bind(&qh, 1..=1, ()).map_err(|e| format!("wl_shm: {e}"))?;
        let wm_base: XdgWmBase = globals.bind(&qh, 1..=6, ()).map_err(|e| format!("xdg_wm_base: {e}"))?;
        let presentation: WpPresentation = globals.bind(&qh, 1..=2, ()).map_err(|e| format!("wp_presentation: {e}"))?;
        let mut state = State::default();
        let mut bound = serde_json::Map::new();
        for (name, proxy_version) in [
            ("wl_compositor", compositor.version()),
            ("wl_shm", shm.version()),
            ("xdg_wm_base", wm_base.version()),
            ("wp_presentation", presentation.version()),
        ] {
            bound.insert(name.into(), proxy_version.into());
        }
        let advertised = globals.contents().clone_list();
        for global in advertised.iter().filter(|g| g.interface == "wl_output") {
            let output: WlOutput = globals.registry().bind(global.name, global.version.min(4), &qh, global.name);
            state.outputs.insert(global.name, OutputInfo { global_name: global.name, ..Default::default() });
            let _ = output;
        }
        queue.roundtrip(&mut state).map_err(|e| format!("roundtrip: {e}"))?;
        let clock_id = state.clock_id.ok_or("wp_presentation sent no clock_id")?;
        if clock_id != CLOCK_MONOTONIC_ID {
            return Err(format!(
                "wp_presentation clock_id {clock_id} is not CLOCK_MONOTONIC ({CLOCK_MONOTONIC_ID}): the run refuses to start"
            ));
        }
        if !state.shm_formats.contains(&(wl_shm::Format::Xrgb8888 as u32)) {
            return Err("wl_shm does not offer XRGB8888".into());
        }
        let surface = compositor.create_surface(&qh, ());
        let xdg_surface = wm_base.get_xdg_surface(&surface, &qh, ());
        let toplevel = xdg_surface.get_toplevel(&qh, ());
        toplevel.set_title("skew-l2".into());
        toplevel.set_app_id("skew-l2".into());
        toplevel.set_fullscreen(None);
        surface.commit();
        // Startup waits use their own eventfd (never signalled).
        let startup_waker = Waker::new().map_err(|e| format!("eventfd: {e}"))?;
        let mut backend_state = state;
        Self::dispatch_until(&conn, &mut queue, &mut backend_state, &startup_waker, deadline, |s| s.configured)
            .map_err(|e| format!("waiting for the first configure: {e}"))?;
        let (w, h, states) = backend_state.toplevel_configure.clone().ok_or("no xdg_toplevel configure")?;
        if !states.contains(&XDG_STATE_FULLSCREEN) {
            return Err(format!("the compositor did not grant fullscreen (states {states:?})"));
        }
        let (width, height) = if w > 0 && h > 0 {
            (w as u32, h as u32)
        } else {
            let modes: Vec<(i32, i32, i32)> = backend_state.outputs.values().filter_map(|o| o.mode).collect();
            match modes.as_slice() {
                [(mw, mh, _)] if *mw > 0 && *mh > 0 => (*mw as u32, *mh as u32),
                _ => return Err(format!("fullscreen configure without a size and {} output modes", modes.len())),
            }
        };
        let buffer_bytes = width as usize * height as usize * 4;
        let total = buffer_bytes.checked_mul(buffer_count).filter(|t| *t <= i32::MAX as usize).ok_or("pool too large")?;
        let mut pool = ShmPool::new(total)?;
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
        // The warm-up frame: buffer 0 (background), frame callback and
        // presentation feedback with the startup commit id.
        surface.attach(Some(&buffers[0]), 0, 0);
        surface.damage_buffer(0, 0, width as i32, height as i32);
        surface.frame(&qh, STARTUP_COMMIT_ID);
        presentation.feedback(&surface, &qh, STARTUP_COMMIT_ID);
        let t_commit_us = now_us();
        surface.commit();
        let mut warm_presented = None;
        let mut warm_done = false;
        let mut early = Vec::new();
        let result = Self::dispatch_until(&conn, &mut queue, &mut backend_state, &startup_waker, deadline, |s| {
            s.events.iter().any(|e| matches!(e, BackendEvent::FrameDone { commit_id: STARTUP_COMMIT_ID }))
                && s.events.iter().any(|e| matches!(e, BackendEvent::Presented { commit_id: STARTUP_COMMIT_ID, .. } | BackendEvent::Discarded { commit_id: STARTUP_COMMIT_ID }))
        });
        for event in backend_state.events.drain(..) {
            match event {
                BackendEvent::FrameDone { commit_id: STARTUP_COMMIT_ID } => warm_done = true,
                BackendEvent::Presented { commit_id: STARTUP_COMMIT_ID, presented } => warm_presented = Some(presented),
                BackendEvent::Discarded { commit_id: STARTUP_COMMIT_ID } => {
                    return Err("the compositor discarded the startup frame".into());
                }
                BackendEvent::Closed => return Err("the window was closed during startup".into()),
                // Releases before ready stay events of the run.
                other => early.push(other),
            }
        }
        result.map_err(|e| format!("waiting for the startup presentation: {e}"))?;
        let presented = warm_presented.ok_or("no startup presentation")?;
        if !warm_done {
            return Err("no startup frame callback".into());
        }
        let t_presented = presented.t_us().ok_or("startup presentation with an invalid timestamp")?;
        backend_state.events = early;
        backend_state.ready = Some(ReadyShape { width: width as i32, height: height as i32 });
        let held = if backend_state.events.contains(&BackendEvent::BufferReleased { buffer: 0 }) {
            backend_state.events.retain(|e| *e != BackendEvent::BufferReleased { buffer: 0 });
            Vec::new()
        } else {
            vec![(0, STARTUP_COMMIT_ID)]
        };
        let details = serde_json::json!({
            "display": display_name,
            "bound_versions": bound,
            "shm_format": "XRGB8888",
            "shm_formats_offered": backend_state.shm_formats.len(),
            "outputs": backend_state.outputs.values().map(OutputInfo::to_json).collect::<Vec<_>>(),
            "surface_entered_outputs": backend_state.entered_outputs,
            "first_configure": { "width": w, "height": h, "states": states },
            "startup_commit_id": STARTUP_COMMIT_ID,
            "startup_buffer_released_before_ready": held.is_empty(),
        });
        let startup = StartupInfo {
            backend: "wayland",
            clock_id,
            width,
            height,
            buffers: buffer_count,
            held,
            startup_presented: Some(presented),
            startup_commit_to_presented_us: Some(t_presented as i64 - t_commit_us as i64),
            details,
        };
        Ok(Self {
            conn,
            queue,
            qh,
            state: backend_state,
            surface,
            presentation,
            buffers,
            pool,
            width,
            height,
            startup,
            _objects: (compositor, shm, wm_base, xdg_surface, toplevel, shm_pool),
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
    ) -> Result<(), String> {
        loop {
            queue.dispatch_pending(state).map_err(|e| format!("dispatch: {e}"))?;
            if done(state) {
                return Ok(());
            }
            if now_us() >= deadline_us {
                return Err("timed out".into());
            }
            Self::read_once(conn, queue, waker, Some(deadline_us)).map_err(|e| e.detail)?;
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
}
