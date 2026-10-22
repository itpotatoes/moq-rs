//! The real haptic PCM device for the L2 stage (registration 1-D §3-3,
//! milestone M2): `libasound.so.2` loaded at RUNTIME with `libloading`.
//!
//! Binding decision (1-D §11-7 M2a; L2 설계 §7-4). `alsa-sys` was evaluated
//! and NOT used: 0.3.1 ships pre-generated bindgen output (`src/generated.rs`)
//! but its build script panics unless `pkg-config` finds `alsa`, and this
//! host has no `alsa.pc` and no ALSA headers (only the runtime
//! `libasound.so.2`, no `libasound.so` dev symlink), so it would need a local
//! `.pc` plus a `PKG_CONFIG_PATH` or a `links` override in a cargo config for
//! EVERY workspace build; 0.6.1 additionally requires `libc >= 0.2.186`
//! (workspace lock: 0.2.175, a workspace-wide bump) and a `cc` build
//! dependency; and linking either one adds `DT_NEEDED libasound.so.2` to the
//! receiver, so every run of every arm would load it. Here nothing links
//! against ALSA: only a haptic lane with the device backend loads the
//! library, at stream open, inside its own thread; `--l2-sink none` and
//! every other path never touch it. `libloading` 0.8.3 was already in
//! `Cargo.lock` (via `clang-sys`), so no new crate was downloaded.
//!
//! Correctness surface: every declaration below is TRANSCRIBED from the
//! bindgen output of alsa-lib's `asoundlib.h` shipped as `alsa-sys` 0.3.1
//! `src/generated.rs` (crate checksum in the M2 report), with opaque handles
//! as `c_void` pointers and enum parameters at their bindgen widths
//! (`snd_pcm_stream_t`/`access_t`/`tstamp_t`/`tstamp_type_t`/`state_t` =
//! `u32`, `snd_pcm_format_t` = `i32`, `snd_pcm_uframes_t` = `c_ulong`,
//! `snd_pcm_sframes_t` = `c_long`, `snd_htimestamp_t` = `struct timespec`).
//! The only structs passed by value-layout are the two audio-timestamp
//! bitfield structs; their GCC x86-64 packing is taken from the same bindgen
//! output (config: `type_requested` bits 0–3, `report_delay` bit 4, 4 bytes;
//! report: `valid` bit 0, `actual_type` bits 1–4, `accuracy_report` bit 5,
//! then `accuracy: u32` at offset 4, 8 bytes) and witnessed on the device: a
//! LINK request that reached the driver makes the first report `valid`
//! (COMPAT would leave it invalid), and an `actual_type` outside 0..=5 is
//! refused. Every symbol is resolved at open; a missing one is a start
//! failure, never a later panic.
//!
//! Stream setup (fail closed, everything read back): `snd_pcm_open(...,
//! SND_PCM_NONBLOCK)` (a card held by PipeWire returns `-EBUSY` at once);
//! hw: RW_INTERLEAVED, S16_LE, 2 channels, exactly 48000 Hz without
//! resampling, period/buffer `_near` then read back (the placement engine
//! refuses any difference from the requested sizes), batch devices refused
//! (they report positions per period only); sw: `start_threshold = boundary`
//! (the prefill must not auto-start the stream; `snd_pcm_start` is explicit),
//! `stop_threshold = buffer` (XRUN when empty), `avail_min = period`,
//! silence threshold/size 0 (kernel auto-silence would mask an underrun),
//! `tstamp_mode = ENABLE`, `tstamp_type = MONOTONIC`, read back and refused
//! otherwise. Before EVERY `snd_pcm_status` the LINK audio-timestamp config
//! is set again: the STATUS_EXT ioctl overwrites that word with the report.

use std::ffi::{c_char, c_int, c_long, c_uint, c_ulong, c_void, CStr, CString};
use std::ptr::null_mut;

use crate::l2_haptic::{
    AudioTstampReport, HapticPcm, PcmError, PcmParams, PcmState, PcmStatus, PlacementConfig,
    AUDIO_TSTAMP_TYPE_LINK, PCM_CHANNELS, PCM_RATE_HZ,
};

/// The runtime library (no dev symlink needed).
pub const LIBASOUND: &str = "libasound.so.2";

const SND_PCM_STREAM_PLAYBACK: c_uint = 0;
const SND_PCM_NONBLOCK: c_int = 0x1;
const SND_PCM_ACCESS_RW_INTERLEAVED: c_uint = 3;
const SND_PCM_FORMAT_S16_LE: c_int = 2;
const SND_PCM_TSTAMP_ENABLE: c_uint = 1;
const SND_PCM_TSTAMP_TYPE_MONOTONIC: c_uint = 1;

/// `snd_pcm_audio_tstamp_config_t` (bindgen: 1-byte bitfield unit, aligned
/// to 4): `type_requested` bits 0–3, `report_delay` bit 4.
#[repr(C)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RawAudioTstampConfig {
    pub bits: c_uint,
}

impl RawAudioTstampConfig {
    pub fn new(type_requested: u32, report_delay: bool) -> Self {
        Self { bits: (type_requested & 0xF) | (u32::from(report_delay) << 4) }
    }
}

/// `snd_pcm_audio_tstamp_report_t` (bindgen: 1-byte bitfield unit, then
/// `accuracy: c_uint` at offset 4): `valid` bit 0, `actual_type` bits 1–4,
/// `accuracy_report` bit 5.
#[repr(C)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RawAudioTstampReport {
    pub bits: c_uint,
    pub accuracy: c_uint,
}

impl RawAudioTstampReport {
    pub fn decode(&self) -> AudioTstampReport {
        AudioTstampReport {
            valid: self.bits & 1 == 1,
            actual_type: (self.bits >> 1) & 0xF,
            accuracy_report: (self.bits >> 5) & 1 == 1,
            accuracy_ns: self.accuracy,
        }
    }
}

type Pcm = c_void;
type HwParams = c_void;
type SwParams = c_void;
type Status = c_void;

/// Every libasound entry point the device backend uses, resolved at open.
struct Api {
    snd_asoundlib_version: unsafe extern "C" fn() -> *const c_char,
    snd_strerror: unsafe extern "C" fn(c_int) -> *const c_char,
    snd_pcm_open: unsafe extern "C" fn(*mut *mut Pcm, *const c_char, c_uint, c_int) -> c_int,
    snd_pcm_close: unsafe extern "C" fn(*mut Pcm) -> c_int,
    snd_pcm_hw_params_malloc: unsafe extern "C" fn(*mut *mut HwParams) -> c_int,
    snd_pcm_hw_params_free: unsafe extern "C" fn(*mut HwParams),
    snd_pcm_hw_params_any: unsafe extern "C" fn(*mut Pcm, *mut HwParams) -> c_int,
    snd_pcm_hw_params_set_access: unsafe extern "C" fn(*mut Pcm, *mut HwParams, c_uint) -> c_int,
    snd_pcm_hw_params_set_format: unsafe extern "C" fn(*mut Pcm, *mut HwParams, c_int) -> c_int,
    snd_pcm_hw_params_set_channels: unsafe extern "C" fn(*mut Pcm, *mut HwParams, c_uint) -> c_int,
    snd_pcm_hw_params_set_rate_resample: unsafe extern "C" fn(*mut Pcm, *mut HwParams, c_uint) -> c_int,
    snd_pcm_hw_params_set_rate: unsafe extern "C" fn(*mut Pcm, *mut HwParams, c_uint, c_int) -> c_int,
    snd_pcm_hw_params_set_period_size_near:
        unsafe extern "C" fn(*mut Pcm, *mut HwParams, *mut c_ulong, *mut c_int) -> c_int,
    snd_pcm_hw_params_set_buffer_size_near: unsafe extern "C" fn(*mut Pcm, *mut HwParams, *mut c_ulong) -> c_int,
    snd_pcm_hw_params: unsafe extern "C" fn(*mut Pcm, *mut HwParams) -> c_int,
    snd_pcm_hw_params_get_rate: unsafe extern "C" fn(*const HwParams, *mut c_uint, *mut c_int) -> c_int,
    snd_pcm_hw_params_get_channels: unsafe extern "C" fn(*const HwParams, *mut c_uint) -> c_int,
    snd_pcm_hw_params_get_period_size: unsafe extern "C" fn(*const HwParams, *mut c_ulong, *mut c_int) -> c_int,
    snd_pcm_hw_params_get_buffer_size: unsafe extern "C" fn(*const HwParams, *mut c_ulong) -> c_int,
    snd_pcm_hw_params_is_batch: unsafe extern "C" fn(*const HwParams) -> c_int,
    snd_pcm_sw_params_malloc: unsafe extern "C" fn(*mut *mut SwParams) -> c_int,
    snd_pcm_sw_params_free: unsafe extern "C" fn(*mut SwParams),
    snd_pcm_sw_params_current: unsafe extern "C" fn(*mut Pcm, *mut SwParams) -> c_int,
    snd_pcm_sw_params_get_boundary: unsafe extern "C" fn(*const SwParams, *mut c_ulong) -> c_int,
    snd_pcm_sw_params_set_start_threshold: unsafe extern "C" fn(*mut Pcm, *mut SwParams, c_ulong) -> c_int,
    snd_pcm_sw_params_set_stop_threshold: unsafe extern "C" fn(*mut Pcm, *mut SwParams, c_ulong) -> c_int,
    snd_pcm_sw_params_set_avail_min: unsafe extern "C" fn(*mut Pcm, *mut SwParams, c_ulong) -> c_int,
    snd_pcm_sw_params_set_silence_threshold: unsafe extern "C" fn(*mut Pcm, *mut SwParams, c_ulong) -> c_int,
    snd_pcm_sw_params_set_silence_size: unsafe extern "C" fn(*mut Pcm, *mut SwParams, c_ulong) -> c_int,
    snd_pcm_sw_params_set_tstamp_mode: unsafe extern "C" fn(*mut Pcm, *mut SwParams, c_uint) -> c_int,
    snd_pcm_sw_params_set_tstamp_type: unsafe extern "C" fn(*mut Pcm, *mut SwParams, c_uint) -> c_int,
    snd_pcm_sw_params_get_tstamp_mode: unsafe extern "C" fn(*const SwParams, *mut c_uint) -> c_int,
    snd_pcm_sw_params_get_tstamp_type: unsafe extern "C" fn(*const SwParams, *mut c_uint) -> c_int,
    snd_pcm_sw_params_get_start_threshold: unsafe extern "C" fn(*const SwParams, *mut c_ulong) -> c_int,
    snd_pcm_sw_params_get_stop_threshold: unsafe extern "C" fn(*const SwParams, *mut c_ulong) -> c_int,
    snd_pcm_sw_params_get_avail_min: unsafe extern "C" fn(*const SwParams, *mut c_ulong) -> c_int,
    snd_pcm_sw_params: unsafe extern "C" fn(*mut Pcm, *mut SwParams) -> c_int,
    snd_pcm_status_sizeof: unsafe extern "C" fn() -> usize,
    snd_pcm_status_malloc: unsafe extern "C" fn(*mut *mut Status) -> c_int,
    snd_pcm_status_free: unsafe extern "C" fn(*mut Status),
    snd_pcm_status: unsafe extern "C" fn(*mut Pcm, *mut Status) -> c_int,
    snd_pcm_status_get_state: unsafe extern "C" fn(*const Status) -> c_uint,
    snd_pcm_status_get_htstamp: unsafe extern "C" fn(*const Status, *mut libc::timespec),
    snd_pcm_status_get_trigger_htstamp: unsafe extern "C" fn(*const Status, *mut libc::timespec),
    snd_pcm_status_get_audio_htstamp: unsafe extern "C" fn(*const Status, *mut libc::timespec),
    snd_pcm_status_get_driver_htstamp: unsafe extern "C" fn(*const Status, *mut libc::timespec),
    snd_pcm_status_get_audio_htstamp_report: unsafe extern "C" fn(*const Status, *mut RawAudioTstampReport),
    snd_pcm_status_set_audio_htstamp_config: unsafe extern "C" fn(*mut Status, *mut RawAudioTstampConfig),
    snd_pcm_status_get_delay: unsafe extern "C" fn(*const Status) -> c_long,
    snd_pcm_status_get_avail: unsafe extern "C" fn(*const Status) -> c_ulong,
    snd_pcm_status_get_avail_max: unsafe extern "C" fn(*const Status) -> c_ulong,
    snd_pcm_writei: unsafe extern "C" fn(*mut Pcm, *const c_void, c_ulong) -> c_long,
    snd_pcm_start: unsafe extern "C" fn(*mut Pcm) -> c_int,
    snd_pcm_drop: unsafe extern "C" fn(*mut Pcm) -> c_int,
    /// Keeps every function pointer above valid; dropped last.
    _lib: libloading::Library,
}

macro_rules! resolve {
    ($lib:expr, $($name:ident),+ $(,)?) => {
        Api {
            $($name: {
                // SAFETY: the declared type is the bindgen signature of this
                // exported C function (see the module doc); the pointer is
                // only used while `_lib` keeps the library loaded.
                let symbol: libloading::Symbol<'_, _> =
                    unsafe { $lib.get(concat!(stringify!($name), "\0").as_bytes()) }
                        .map_err(|e| format!("{LIBASOUND}: missing symbol {}: {e}", stringify!($name)))?;
                *symbol
            },)+
            _lib: $lib,
        }
    };
}

impl Api {
    fn load() -> Result<Self, String> {
        // SAFETY: loading the system ALSA library runs its ELF initialisers;
        // alsa-lib has no initialiser with observable side effects (its
        // configuration is read on the first `snd_pcm_open`).
        let lib = unsafe { libloading::Library::new(LIBASOUND) }
            .map_err(|e| format!("cannot load {LIBASOUND}: {e}"))?;
        Ok(resolve!(
            lib,
            snd_asoundlib_version, snd_strerror, snd_pcm_open, snd_pcm_close,
            snd_pcm_hw_params_malloc, snd_pcm_hw_params_free, snd_pcm_hw_params_any,
            snd_pcm_hw_params_set_access, snd_pcm_hw_params_set_format, snd_pcm_hw_params_set_channels,
            snd_pcm_hw_params_set_rate_resample, snd_pcm_hw_params_set_rate,
            snd_pcm_hw_params_set_period_size_near, snd_pcm_hw_params_set_buffer_size_near,
            snd_pcm_hw_params, snd_pcm_hw_params_get_rate, snd_pcm_hw_params_get_channels,
            snd_pcm_hw_params_get_period_size, snd_pcm_hw_params_get_buffer_size,
            snd_pcm_hw_params_is_batch, snd_pcm_sw_params_malloc, snd_pcm_sw_params_free,
            snd_pcm_sw_params_current, snd_pcm_sw_params_get_boundary,
            snd_pcm_sw_params_set_start_threshold, snd_pcm_sw_params_set_stop_threshold,
            snd_pcm_sw_params_set_avail_min, snd_pcm_sw_params_set_silence_threshold,
            snd_pcm_sw_params_set_silence_size, snd_pcm_sw_params_set_tstamp_mode,
            snd_pcm_sw_params_set_tstamp_type, snd_pcm_sw_params_get_tstamp_mode,
            snd_pcm_sw_params_get_tstamp_type, snd_pcm_sw_params_get_start_threshold,
            snd_pcm_sw_params_get_stop_threshold, snd_pcm_sw_params_get_avail_min,
            snd_pcm_sw_params, snd_pcm_status_sizeof, snd_pcm_status_malloc, snd_pcm_status_free,
            snd_pcm_status, snd_pcm_status_get_state, snd_pcm_status_get_htstamp,
            snd_pcm_status_get_trigger_htstamp, snd_pcm_status_get_audio_htstamp,
            snd_pcm_status_get_driver_htstamp, snd_pcm_status_get_audio_htstamp_report,
            snd_pcm_status_set_audio_htstamp_config, snd_pcm_status_get_delay,
            snd_pcm_status_get_avail, snd_pcm_status_get_avail_max, snd_pcm_writei,
            snd_pcm_start, snd_pcm_drop,
        ))
    }

    fn strerror(&self, code: c_int) -> String {
        // SAFETY: snd_strerror returns a static NUL-terminated string.
        let text = unsafe { (self.snd_strerror)(code) };
        if text.is_null() {
            return format!("error {code}");
        }
        unsafe { CStr::from_ptr(text) }.to_string_lossy().into_owned()
    }

    fn version(&self) -> String {
        // SAFETY: static NUL-terminated string.
        let text = unsafe { (self.snd_asoundlib_version)() };
        if text.is_null() {
            return "unknown".into();
        }
        unsafe { CStr::from_ptr(text) }.to_string_lossy().into_owned()
    }

    fn check(&self, code: c_int, call: &str) -> Result<(), String> {
        if code < 0 {
            Err(format!("{call}: {} ({code})", self.strerror(code)))
        } else {
            Ok(())
        }
    }
}

/// Which `--l2-alsa-*` sw settings were installed (recorded in meta).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwRecord {
    pub boundary: u64,
    pub start_threshold: u64,
    pub stop_threshold: u64,
    pub avail_min: u64,
}

/// One open `hw:` playback stream. Never leaves the thread that opened it
/// (it holds raw libasound pointers and is deliberately not `Send`).
pub struct AlsaPcm {
    api: Api,
    pcm: *mut Pcm,
    status: *mut Status,
    params: PcmParams,
    sw: SwRecord,
    is_batch: bool,
    status_struct_bytes: usize,
}

fn timespec_ns(ts: &libc::timespec) -> u64 {
    (ts.tv_sec.max(0) as u64) * 1_000_000_000 + ts.tv_nsec.max(0) as u64
}

impl AlsaPcm {
    /// Open and configure `device` for the placement sizes; fail closed on
    /// anything that is not exactly the registered stream.
    pub fn open(device: &str, placement: &PlacementConfig) -> Result<Self, String> {
        let api = Api::load()?;
        let name = CString::new(device).map_err(|_| format!("device name {device:?} contains NUL"))?;
        let mut pcm: *mut Pcm = null_mut();
        // SAFETY: valid out-pointer and NUL-terminated name.
        let rc = unsafe { (api.snd_pcm_open)(&mut pcm, name.as_ptr(), SND_PCM_STREAM_PLAYBACK, SND_PCM_NONBLOCK) };
        api.check(rc, &format!("snd_pcm_open({device}, PLAYBACK, NONBLOCK)"))?;
        let status_struct_bytes = unsafe { (api.snd_pcm_status_sizeof)() };
        let mut this = Self {
            api,
            pcm,
            status: null_mut(),
            params: PcmParams {
                backend: "device",
                device: device.to_string(),
                rate_hz: 0,
                channels: 0,
                format: "S16_LE",
                access: "RW_INTERLEAVED",
                period_frames: 0,
                buffer_frames: 0,
                tstamp_mode: "",
                tstamp_type: "",
                audio_tstamp_type_requested: AUDIO_TSTAMP_TYPE_LINK,
                library: None,
                details: serde_json::Value::Null,
            },
            sw: SwRecord { boundary: 0, start_threshold: 0, stop_threshold: 0, avail_min: 0 },
            is_batch: false,
            status_struct_bytes,
        };
        // From here on `Drop` closes the stream on every error path.
        this.configure_hw(placement)?;
        this.configure_sw()?;
        let mut status = null_mut();
        let rc = unsafe { (this.api.snd_pcm_status_malloc)(&mut status) };
        this.api.check(rc, "snd_pcm_status_malloc")?;
        this.status = status;
        this.params.library = Some(format!("{LIBASOUND} (alsa-lib {})", this.api.version()));
        this.params.details = serde_json::json!({
            "open_mode": "SND_PCM_NONBLOCK",
            "rate_resample": false,
            "is_batch": this.is_batch,
            "sw_boundary": this.sw.boundary,
            "sw_start_threshold": this.sw.start_threshold,
            "sw_stop_threshold": this.sw.stop_threshold,
            "sw_avail_min": this.sw.avail_min,
            "sw_silence_threshold": 0,
            "sw_silence_size": 0,
            "status_struct_bytes": this.status_struct_bytes,
            "binding": "libloading, declarations transcribed from alsa-sys 0.3.1 generated.rs (bindgen of asoundlib.h)",
        });
        Ok(this)
    }

    fn configure_hw(&mut self, placement: &PlacementConfig) -> Result<(), String> {
        let api = &self.api;
        let pcm = self.pcm;
        let mut hw: *mut HwParams = null_mut();
        api.check(unsafe { (api.snd_pcm_hw_params_malloc)(&mut hw) }, "snd_pcm_hw_params_malloc")?;
        struct FreeHw<'a>(&'a Api, *mut HwParams);
        impl Drop for FreeHw<'_> {
            fn drop(&mut self) {
                unsafe { (self.0.snd_pcm_hw_params_free)(self.1) }
            }
        }
        let _free = FreeHw(api, hw);
        unsafe {
            api.check((api.snd_pcm_hw_params_any)(pcm, hw), "snd_pcm_hw_params_any")?;
            api.check(
                (api.snd_pcm_hw_params_set_access)(pcm, hw, SND_PCM_ACCESS_RW_INTERLEAVED),
                "snd_pcm_hw_params_set_access(RW_INTERLEAVED)",
            )?;
            api.check((api.snd_pcm_hw_params_set_format)(pcm, hw, SND_PCM_FORMAT_S16_LE), "snd_pcm_hw_params_set_format(S16_LE)")?;
            api.check((api.snd_pcm_hw_params_set_channels)(pcm, hw, PCM_CHANNELS as c_uint), "snd_pcm_hw_params_set_channels(2)")?;
            api.check((api.snd_pcm_hw_params_set_rate_resample)(pcm, hw, 0), "snd_pcm_hw_params_set_rate_resample(0)")?;
            api.check((api.snd_pcm_hw_params_set_rate)(pcm, hw, PCM_RATE_HZ as c_uint, 0), "snd_pcm_hw_params_set_rate(48000, exact)")?;
            let mut period = placement.period_frames as c_ulong;
            let mut dir: c_int = 0;
            api.check((api.snd_pcm_hw_params_set_period_size_near)(pcm, hw, &mut period, &mut dir), "snd_pcm_hw_params_set_period_size_near")?;
            let mut buffer = placement.buffer_frames as c_ulong;
            api.check((api.snd_pcm_hw_params_set_buffer_size_near)(pcm, hw, &mut buffer), "snd_pcm_hw_params_set_buffer_size_near")?;
            api.check((api.snd_pcm_hw_params)(pcm, hw), "snd_pcm_hw_params (install)")?;
            // Read back what was installed.
            let (mut rate, mut rate_dir, mut channels) = (0 as c_uint, 0 as c_int, 0 as c_uint);
            api.check((api.snd_pcm_hw_params_get_rate)(hw, &mut rate, &mut rate_dir), "snd_pcm_hw_params_get_rate")?;
            api.check((api.snd_pcm_hw_params_get_channels)(hw, &mut channels), "snd_pcm_hw_params_get_channels")?;
            let (mut period_rb, mut period_dir, mut buffer_rb) = (0 as c_ulong, 0 as c_int, 0 as c_ulong);
            api.check((api.snd_pcm_hw_params_get_period_size)(hw, &mut period_rb, &mut period_dir), "snd_pcm_hw_params_get_period_size")?;
            api.check((api.snd_pcm_hw_params_get_buffer_size)(hw, &mut buffer_rb), "snd_pcm_hw_params_get_buffer_size")?;
            let batch = (api.snd_pcm_hw_params_is_batch)(hw);
            if rate as u64 != PCM_RATE_HZ || rate_dir != 0 {
                return Err(format!("device rate {rate} (dir {rate_dir}) is not exactly 48000 Hz"));
            }
            if channels as usize != PCM_CHANNELS {
                return Err(format!("device channels {channels} != 2"));
            }
            if batch != 0 {
                return Err("batch device: positions are reported per period only, refused".into());
            }
            self.params.rate_hz = rate as u64;
            self.params.channels = channels as u64;
            self.params.period_frames = period_rb as u64;
            self.params.buffer_frames = buffer_rb as u64;
            self.is_batch = false;
        }
        Ok(())
    }

    fn configure_sw(&mut self) -> Result<(), String> {
        let api = &self.api;
        let pcm = self.pcm;
        let mut sw: *mut SwParams = null_mut();
        api.check(unsafe { (api.snd_pcm_sw_params_malloc)(&mut sw) }, "snd_pcm_sw_params_malloc")?;
        struct FreeSw<'a>(&'a Api, *mut SwParams);
        impl Drop for FreeSw<'_> {
            fn drop(&mut self) {
                unsafe { (self.0.snd_pcm_sw_params_free)(self.1) }
            }
        }
        let _free = FreeSw(api, sw);
        let buffer = self.params.buffer_frames as c_ulong;
        let period = self.params.period_frames as c_ulong;
        unsafe {
            api.check((api.snd_pcm_sw_params_current)(pcm, sw), "snd_pcm_sw_params_current")?;
            let mut boundary: c_ulong = 0;
            api.check((api.snd_pcm_sw_params_get_boundary)(sw, &mut boundary), "snd_pcm_sw_params_get_boundary")?;
            api.check((api.snd_pcm_sw_params_set_start_threshold)(pcm, sw, boundary), "snd_pcm_sw_params_set_start_threshold(boundary)")?;
            api.check((api.snd_pcm_sw_params_set_stop_threshold)(pcm, sw, buffer), "snd_pcm_sw_params_set_stop_threshold(buffer)")?;
            api.check((api.snd_pcm_sw_params_set_avail_min)(pcm, sw, period), "snd_pcm_sw_params_set_avail_min(period)")?;
            api.check((api.snd_pcm_sw_params_set_silence_threshold)(pcm, sw, 0), "snd_pcm_sw_params_set_silence_threshold(0)")?;
            api.check((api.snd_pcm_sw_params_set_silence_size)(pcm, sw, 0), "snd_pcm_sw_params_set_silence_size(0)")?;
            api.check((api.snd_pcm_sw_params_set_tstamp_mode)(pcm, sw, SND_PCM_TSTAMP_ENABLE), "snd_pcm_sw_params_set_tstamp_mode(ENABLE)")?;
            api.check((api.snd_pcm_sw_params_set_tstamp_type)(pcm, sw, SND_PCM_TSTAMP_TYPE_MONOTONIC), "snd_pcm_sw_params_set_tstamp_type(MONOTONIC)")?;
            api.check((api.snd_pcm_sw_params)(pcm, sw), "snd_pcm_sw_params (install)")?;
            // Read back the INSTALLED values from a fresh snapshot.
            api.check((api.snd_pcm_sw_params_current)(pcm, sw), "snd_pcm_sw_params_current (readback)")?;
            let (mut mode, mut kind) = (0 as c_uint, 0 as c_uint);
            api.check((api.snd_pcm_sw_params_get_tstamp_mode)(sw, &mut mode), "snd_pcm_sw_params_get_tstamp_mode")?;
            api.check((api.snd_pcm_sw_params_get_tstamp_type)(sw, &mut kind), "snd_pcm_sw_params_get_tstamp_type")?;
            if mode != SND_PCM_TSTAMP_ENABLE || kind != SND_PCM_TSTAMP_TYPE_MONOTONIC {
                return Err(format!("timestamps not ENABLE/MONOTONIC after install (mode {mode}, type {kind}): refused"));
            }
            let (mut start, mut stop, mut avail_min) = (0 as c_ulong, 0 as c_ulong, 0 as c_ulong);
            api.check((api.snd_pcm_sw_params_get_start_threshold)(sw, &mut start), "snd_pcm_sw_params_get_start_threshold")?;
            api.check((api.snd_pcm_sw_params_get_stop_threshold)(sw, &mut stop), "snd_pcm_sw_params_get_stop_threshold")?;
            api.check((api.snd_pcm_sw_params_get_avail_min)(sw, &mut avail_min), "snd_pcm_sw_params_get_avail_min")?;
            if start != boundary || stop != buffer {
                return Err(format!("sw thresholds not installed (start {start} != boundary {boundary} or stop {stop} != buffer {buffer})"));
            }
            self.sw = SwRecord {
                boundary: boundary as u64,
                start_threshold: start as u64,
                stop_threshold: stop as u64,
                avail_min: avail_min as u64,
            };
            self.params.tstamp_mode = "ENABLE";
            self.params.tstamp_type = "MONOTONIC";
        }
        Ok(())
    }

    /// The installed sw settings.
    pub fn sw_record(&self) -> SwRecord {
        self.sw
    }

    /// `snd_pcm_status_sizeof()` of the loaded library.
    pub fn status_struct_bytes(&self) -> usize {
        self.status_struct_bytes
    }

    pub fn is_batch(&self) -> bool {
        self.is_batch
    }
}

impl HapticPcm for AlsaPcm {
    fn params(&self) -> &PcmParams {
        &self.params
    }

    fn writei(&mut self, pcm: &[i16], _now_us: u64) -> Result<u64, PcmError> {
        assert!(pcm.len() % PCM_CHANNELS == 0, "interleaved stereo");
        let frames = (pcm.len() / PCM_CHANNELS) as c_ulong;
        // SAFETY: `pcm` holds `frames` interleaved S16 stereo frames.
        let n = unsafe { (self.api.snd_pcm_writei)(self.pcm, pcm.as_ptr().cast(), frames) };
        if n < 0 {
            Err(PcmError::from_code(n as i64, "writei"))
        } else {
            Ok(n as u64)
        }
    }

    fn start(&mut self, _now_us: u64) -> Result<(), PcmError> {
        let rc = unsafe { (self.api.snd_pcm_start)(self.pcm) };
        if rc < 0 {
            Err(PcmError::from_code(rc as i64, "start"))
        } else {
            Ok(())
        }
    }

    fn status(&mut self, _now_us: u64) -> Result<PcmStatus, PcmError> {
        let api = &self.api;
        // Re-request LINK before EVERY call: STATUS_EXT overwrites the word.
        let mut config = RawAudioTstampConfig::new(AUDIO_TSTAMP_TYPE_LINK, false);
        unsafe { (api.snd_pcm_status_set_audio_htstamp_config)(self.status, &mut config) };
        let rc = unsafe { (api.snd_pcm_status)(self.pcm, self.status) };
        if rc < 0 {
            return Err(PcmError::from_code(rc as i64, "status"));
        }
        let status = self.status;
        let stamp = |get: unsafe extern "C" fn(*const Status, *mut libc::timespec)| {
            let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
            // SAFETY: `status` was filled by the call above; `ts` is a valid out-pointer.
            unsafe { get(status, &mut ts) };
            timespec_ns(&ts)
        };
        let htstamp_ns = stamp(api.snd_pcm_status_get_htstamp);
        let trigger_htstamp_ns = stamp(api.snd_pcm_status_get_trigger_htstamp);
        let audio_htstamp_ns = stamp(api.snd_pcm_status_get_audio_htstamp);
        let driver_htstamp_ns = stamp(api.snd_pcm_status_get_driver_htstamp);
        let mut report = RawAudioTstampReport::default();
        unsafe { (api.snd_pcm_status_get_audio_htstamp_report)(self.status, &mut report) };
        Ok(PcmStatus {
            state: PcmState::from_raw(unsafe { (api.snd_pcm_status_get_state)(self.status) }),
            htstamp_ns,
            trigger_htstamp_ns,
            audio_htstamp_ns,
            driver_htstamp_ns,
            delay: unsafe { (api.snd_pcm_status_get_delay)(self.status) } as i64,
            avail: unsafe { (api.snd_pcm_status_get_avail)(self.status) } as u64,
            avail_max: unsafe { (api.snd_pcm_status_get_avail_max)(self.status) } as u64,
            audio_tstamp: report.decode(),
        })
    }

    fn stop(&mut self) -> Result<(), PcmError> {
        let rc = unsafe { (self.api.snd_pcm_drop)(self.pcm) };
        if rc < 0 {
            Err(PcmError::from_code(rc as i64, "drop"))
        } else {
            Ok(())
        }
    }
}

impl Drop for AlsaPcm {
    fn drop(&mut self) {
        // SAFETY: both were obtained from this library and are freed once.
        unsafe {
            if !self.status.is_null() {
                (self.api.snd_pcm_status_free)(self.status);
            }
            if !self.pcm.is_null() {
                (self.api.snd_pcm_close)(self.pcm);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The bitfield packing taken from the bindgen output (pure; no library).
    #[test]
    fn audio_tstamp_bitfields_follow_the_bindgen_layout() {
        assert_eq!(std::mem::size_of::<RawAudioTstampConfig>(), 4);
        assert_eq!(std::mem::size_of::<RawAudioTstampReport>(), 8);
        assert_eq!(std::mem::offset_of!(RawAudioTstampReport, accuracy), 4);
        assert_eq!(RawAudioTstampConfig::new(AUDIO_TSTAMP_TYPE_LINK, false).bits, 0b0_0010);
        assert_eq!(RawAudioTstampConfig::new(AUDIO_TSTAMP_TYPE_LINK, true).bits, 0b1_0010);
        assert_eq!(RawAudioTstampConfig::new(0x1F, false).bits, 0xF, "type is 4 bits");
        // valid=1, actual_type=LINK(2), accuracy_report=1, accuracy 42 ns.
        let report = RawAudioTstampReport { bits: 1 | (2 << 1) | (1 << 5), accuracy: 42 }.decode();
        assert_eq!(report, AudioTstampReport { valid: true, actual_type: 2, accuracy_report: true, accuracy_ns: 42 });
        let report = RawAudioTstampReport { bits: 1 << 1, accuracy: 0 }.decode();
        assert_eq!((report.valid, report.actual_type, report.accuracy_report), (false, 1, false));
    }

    /// The host library exports every entry point the device backend
    /// resolves. Loads `libasound.so.2` only — NO device is opened and no
    /// stream function is called. Skipped where the library is absent.
    #[test]
    fn libasound_exports_every_symbol_the_device_backend_resolves() {
        match Api::load() {
            Ok(api) => {
                assert!(!api.version().is_empty());
                // x86-64 `struct snd_pcm_status` (kernel uapi) is 152 bytes.
                assert_eq!(unsafe { (api.snd_pcm_status_sizeof)() }, 152);
                assert!(api.strerror(-libc::EBUSY).to_lowercase().contains("busy"));
            }
            Err(error) if error.starts_with("cannot load") => {
                eprintln!("skipped: {error}");
            }
            Err(error) => panic!("{error}"),
        }
    }

    #[test]
    fn timespecs_become_integer_nanoseconds() {
        assert_eq!(timespec_ns(&libc::timespec { tv_sec: 7, tv_nsec: 999 }), 7_000_000_999);
    }
}
