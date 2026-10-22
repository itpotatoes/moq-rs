//! `skew-draco-sys` — the Google Draco 1.5.7 point-cloud decoder, vendored as
//! C++ source (see `PROVENANCE`), behind an `extern "C"` shim, plus the
//! conversion of a decoded cloud into the experiment's 8-byte point records.
//!
//! Registration `md/20261022_실험1D_L2사다리_사전등록_문안.md` §3-2:
//! * The decoder is the encoder's own reference implementation (DracoPy
//!   2.0.0 bundles Draco 1.5.7), compiled with fixed flags (`build.rs`:
//!   `-std=c++17 -O3 -DNDEBUG -fPIC -ffp-contract=off`, never
//!   `-march=native` / `-ffast-math`), single-threaded.
//! * Lossless restoration rule: the encoder quantised positions over the
//!   frame's own bounding box (largest side), so a decoded coordinate is a
//!   float NEAR an integer, not an integer. Each coordinate is rounded
//!   half-to-even ([`round_half_even_f32`], implemented explicitly here; C++
//!   `std::round` rounds halves away from zero) and must then be an integer
//!   in `0..=1023`. The record is `u32 LE (x | y << 10 | z << 20)` followed
//!   by `R, G, B, 255` — the same 8 bytes per point the `.bin`
//!   representation carries (`draco/draco_codec.py` `pack_bin`, which uses
//!   numpy `rint`, i.e. half-to-even, and alpha 255). Draco's KD-tree coder
//!   reorders points, so a decoded frame equals the `.bin` frame as a
//!   MULTISET of records, not byte for byte.
//! * Freeze precondition: all 900 frames (d8/d7/d6 x 300) byte-exact as
//!   record multisets — the opt-in gate `tests/draco_full_gate.rs`.

use std::ffi::{c_char, c_int, CStr};

/// The vendored release (checked at run time against the library's own
/// `draco::kDracoVersion`).
pub const DRACO_VERSION: &str = "1.5.7";
/// Where the vendored files come from (PROVENANCE).
pub const DRACO_SOURCE_URL: &str = "https://github.com/google/draco/archive/refs/tags/1.5.7.tar.gz";
pub const DRACO_SOURCE_TARBALL_SHA256: &str =
    "bf6b105b79223eab2b86795363dfe5e5356050006a96521477973aba8f036fe1";
/// Largest coordinate a 10-bit record field holds.
pub const MAX_COORDINATE: u32 = 1_023;
/// Bytes per point record.
pub const RECORD_BYTES: usize = 8;

mod ffi {
    use std::ffi::{c_char, c_int};

    #[repr(C)]
    pub struct SkewDracoCloud {
        _private: [u8; 0],
    }

    extern "C" {
        pub fn skew_draco_decode_point_cloud(
            data: *const u8,
            len: usize,
            out: *mut *mut SkewDracoCloud,
            err: *mut c_char,
            err_len: usize,
        ) -> c_int;
        pub fn skew_draco_cloud_num_points(cloud: *const SkewDracoCloud) -> usize;
        pub fn skew_draco_cloud_copy(
            cloud: *const SkewDracoCloud,
            positions: *mut f32,
            positions_len: usize,
            colors: *mut u8,
            colors_len: usize,
        ) -> c_int;
        pub fn skew_draco_cloud_free(cloud: *mut SkewDracoCloud);
        pub fn skew_draco_version() -> *const c_char;
        pub fn skew_draco_cxx_runtime_build() -> *const c_char;
        pub fn skew_draco_cxx_runtime_loaded() -> *const c_char;
    }
}

/// Shim error codes (`shim/skew_draco_shim.h`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DracoErrorKind {
    NullArgument,
    NotAPointCloud,
    Decode,
    AttributeSet,
    PositionFormat,
    ColorFormat,
    BufferSize,
    Exception,
    UnknownException,
    Other(i32),
}

impl DracoErrorKind {
    fn from_code(code: c_int) -> Self {
        match code {
            1 => Self::NullArgument,
            2 => Self::NotAPointCloud,
            3 => Self::Decode,
            4 => Self::AttributeSet,
            5 => Self::PositionFormat,
            6 => Self::ColorFormat,
            7 => Self::BufferSize,
            8 => Self::Exception,
            9 => Self::UnknownException,
            other => Self::Other(other),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::NullArgument => "null_argument",
            Self::NotAPointCloud => "not_a_point_cloud",
            Self::Decode => "decode",
            Self::AttributeSet => "attribute_set",
            Self::PositionFormat => "position_format",
            Self::ColorFormat => "color_format",
            Self::BufferSize => "buffer_size",
            Self::Exception => "exception",
            Self::UnknownException => "unknown_exception",
            Self::Other(_) => "other",
        }
    }
}

/// A failed decode or a decoded cloud that is not a valid record frame.
#[derive(Debug, Clone, PartialEq)]
pub enum DracoError {
    /// The shim reported an error (the Draco status message, if any).
    Shim { kind: DracoErrorKind, message: String },
    /// A decoded coordinate is not finite or does not round to `0..=1023`.
    Coordinate { point: usize, axis: usize, value: f32 },
}

impl DracoError {
    /// A short stable reason code.
    pub fn reason(&self) -> &'static str {
        match self {
            Self::Shim { kind, .. } => kind.as_str(),
            Self::Coordinate { .. } => "coordinate_out_of_range",
        }
    }
}

impl std::fmt::Display for DracoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Shim { kind, message } => write!(f, "draco {}: {message}", kind.as_str()),
            Self::Coordinate { point, axis, value } => {
                write!(f, "point {point} axis {axis}: {value} does not round to an integer in 0..={MAX_COORDINATE}")
            }
        }
    }
}

impl std::error::Error for DracoError {}

/// One decoded cloud in Draco's point order: 3 floats and 3 colour bytes
/// per point, the float bits exactly as the decoder produced them.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedCloud {
    pub positions: Vec<f32>,
    pub colors: Vec<u8>,
}

impl DecodedCloud {
    pub fn num_points(&self) -> usize {
        self.colors.len() / 3
    }
}

/// Owns a shim handle; frees it on every path.
struct Handle(*mut ffi::SkewDracoCloud);

impl Drop for Handle {
    fn drop(&mut self) {
        // SAFETY: the pointer came from a successful decode and is freed once.
        unsafe { ffi::skew_draco_cloud_free(self.0) }
    }
}

/// Decode one Draco bitstream that must encode a point cloud with exactly
/// POSITION float32 x 3 and COLOR uint8 x 3. Single-threaded, no global
/// state: safe to call from any one thread at a time per call.
pub fn decode_point_cloud(data: &[u8]) -> Result<DecodedCloud, DracoError> {
    let mut out: *mut ffi::SkewDracoCloud = std::ptr::null_mut();
    let mut err = [0 as c_char; 512];
    // SAFETY: `data` is a valid slice; `out` and `err` are valid for writes;
    // the shim never unwinds (it catches every C++ exception).
    let code = unsafe {
        ffi::skew_draco_decode_point_cloud(data.as_ptr(), data.len(), &mut out, err.as_mut_ptr(), err.len())
    };
    if code != 0 {
        // SAFETY: the shim NUL-terminates within `err.len()` bytes; the buffer
        // was zeroed, so it is a C string either way.
        let message = unsafe { CStr::from_ptr(err.as_ptr()) }.to_string_lossy().into_owned();
        return Err(DracoError::Shim { kind: DracoErrorKind::from_code(code), message });
    }
    let handle = Handle(out);
    // SAFETY: a live handle.
    let n = unsafe { ffi::skew_draco_cloud_num_points(handle.0) };
    let mut positions = vec![0f32; 3 * n];
    let mut colors = vec![0u8; 3 * n];
    // SAFETY: both buffers have exactly the sizes the shim checks.
    let code = unsafe {
        ffi::skew_draco_cloud_copy(handle.0, positions.as_mut_ptr(), positions.len(), colors.as_mut_ptr(), colors.len())
    };
    if code != 0 {
        return Err(DracoError::Shim { kind: DracoErrorKind::from_code(code), message: "copy".into() });
    }
    Ok(DecodedCloud { positions, colors })
}

/// Round half to even (banker's rounding), written out explicitly: the
/// registration requires this rule and C/C++ `round` is half-away-from-zero.
/// Exact for every finite `f32` (for `|x| >= 2^23` every `f32` is already an
/// integer); NaN and infinities are returned unchanged; the sign of zero
/// follows the input.
pub fn round_half_even_f32(x: f32) -> f32 {
    if !x.is_finite() {
        return x;
    }
    let a = x.abs();
    let floor = a.floor();
    // Exact: 0 <= a - floor < 1 (Sterbenz for a >= 1, identity for a < 1).
    let frac = a - floor;
    let rounded = if frac > 0.5 {
        floor + 1.0
    } else if frac < 0.5 {
        floor
    } else if floor % 2.0 == 0.0 {
        floor
    } else {
        floor + 1.0
    };
    rounded.copysign(x)
}

/// A decoded coordinate as a record field: round half to even, then it must
/// be an integer in `0..=MAX_COORDINATE`. Equal to
/// `round_half_even_f32(value)` restricted to that range (tested over a
/// dense grid); written without `floorf` because it runs once per
/// coordinate of every frame.
pub fn coordinate_field(value: f32) -> Option<u32> {
    // Only [-0.5, 1023.5] can round into 0..=1023; NaN fails both tests.
    if !(value >= -0.5 && value <= MAX_COORDINATE as f32 + 0.5) {
        return None;
    }
    if value < 0.0 {
        // (-0.5, 0) and -0.5 itself round to -0.0, i.e. field 0.
        return Some(0);
    }
    let floor = value as u32; // truncation = floor for value >= 0
    // Exact: floor <= value < floor + 1 < 2^24.
    let frac = value - floor as f32;
    let rounded = if frac > 0.5 {
        floor + 1
    } else if frac < 0.5 {
        floor
    } else {
        floor + (floor & 1) // a half: to the even neighbour
    };
    (rounded <= MAX_COORDINATE).then_some(rounded)
}

/// What [`records_from_cloud`] produced, with the largest distance between a
/// decoded float and its rounded integer (diagnostics: the quantisation
/// error that the rounding removed).
#[derive(Debug, Clone, PartialEq)]
pub struct Records {
    pub bytes: Vec<u8>,
    pub max_abs_rounding: f32,
}

/// The 8-byte records of a decoded cloud, in Draco's point order.
pub fn records_from_cloud(cloud: &DecodedCloud) -> Result<Records, DracoError> {
    let n = cloud.num_points();
    let mut bytes = Vec::with_capacity(n * RECORD_BYTES);
    let mut max_abs_rounding = 0f32;
    for point in 0..n {
        let mut field = [0u32; 3];
        for axis in 0..3 {
            let value = cloud.positions[3 * point + axis];
            field[axis] = coordinate_field(value).ok_or(DracoError::Coordinate { point, axis, value })?;
            max_abs_rounding = max_abs_rounding.max((value - field[axis] as f32).abs());
        }
        let word = field[0] | (field[1] << 10) | (field[2] << 20);
        bytes.extend_from_slice(&word.to_le_bytes());
        bytes.extend_from_slice(&cloud.colors[3 * point..3 * point + 3]);
        bytes.push(255);
    }
    Ok(Records { bytes, max_abs_rounding })
}

/// Decode a frame straight into its 8-byte records.
pub fn decode_to_records(data: &[u8]) -> Result<Records, DracoError> {
    records_from_cloud(&decode_point_cloud(data)?)
}

/// `records` sorted as little-endian u64 values: the canonical form of a
/// record multiset (two frames hold the same points iff these are equal).
pub fn sorted_records(records: &[u8]) -> Vec<u64> {
    assert_eq!(records.len() % RECORD_BYTES, 0, "whole records");
    let mut keys: Vec<u64> = records
        .chunks_exact(RECORD_BYTES)
        .map(|r| u64::from_le_bytes(r.try_into().expect("8 bytes")))
        .collect();
    keys.sort_unstable();
    keys
}

/// The library's own version string (`draco::kDracoVersion`).
pub fn library_version() -> String {
    // SAFETY: a static NUL-terminated string in the library.
    unsafe { CStr::from_ptr(ffi::skew_draco_version()) }.to_string_lossy().into_owned()
}

/// The C++ runtime the shim was compiled against (header macros).
pub fn cxx_runtime_build() -> String {
    // SAFETY: a static NUL-terminated string literal of the shim.
    unsafe { CStr::from_ptr(ffi::skew_draco_cxx_runtime_build()) }.to_string_lossy().into_owned()
}

/// The C++ runtime loaded in this process: the shared object of
/// `std::terminate` and, if it is a symlink, what it resolves to (the
/// versioned file, e.g. `libstdc++.so.6.0.34`).
pub fn cxx_runtime_loaded() -> String {
    // SAFETY: a NUL-terminated path owned by the dynamic loader (valid while
    // the library is loaded), copied at once.
    let path = unsafe { CStr::from_ptr(ffi::skew_draco_cxx_runtime_loaded()) }.to_string_lossy().into_owned();
    match std::fs::canonicalize(&path) {
        Ok(resolved) if resolved.to_string_lossy() != path => format!("{path} -> {}", resolved.display()),
        _ => path,
    }
}

/// Decoder identity for the L2 meta row and the freeze (registration §3-2:
/// "디코더 id·버전·플래그·threads를 L2 meta에 기록").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecoderInfo {
    pub id: &'static str,
    pub version: String,
    pub source_url: &'static str,
    pub source_tarball_sha256: &'static str,
    /// `sha256sum`-format digest of every file under `vendor/` and `shim/`
    /// (see `build.rs`).
    pub vendored_sources_sha256: &'static str,
    pub vendored_files: &'static str,
    pub compiled_sources: &'static str,
    pub compiler: &'static str,
    pub compiler_version: &'static str,
    /// The registered flags as passed.
    pub fixed_flags: &'static str,
    /// The full compiler argument vector (`<crate>` = this crate's dir).
    pub compiler_args: &'static str,
    /// `OPT_LEVEL` of this crate's Rust code (record packing).
    pub rust_opt_level: &'static str,
    /// The C++ runtime compiled against and the one loaded at run time.
    pub cxx_runtime_build: String,
    pub cxx_runtime_loaded: String,
    pub threads: u32,
    pub rounding: &'static str,
}

pub fn decoder_info() -> DecoderInfo {
    DecoderInfo {
        id: "google-draco-cpp (vendored, skew-draco-sys)",
        version: library_version(),
        source_url: DRACO_SOURCE_URL,
        source_tarball_sha256: DRACO_SOURCE_TARBALL_SHA256,
        vendored_sources_sha256: env!("SKEW_DRACO_SOURCE_SHA256"),
        vendored_files: env!("SKEW_DRACO_SOURCE_FILES"),
        compiled_sources: env!("SKEW_DRACO_COMPILED_SOURCES"),
        compiler: env!("SKEW_DRACO_CXX"),
        compiler_version: env!("SKEW_DRACO_CXX_VERSION"),
        fixed_flags: env!("SKEW_DRACO_FIXED_FLAGS"),
        compiler_args: env!("SKEW_DRACO_CXX_ARGS"),
        rust_opt_level: env!("SKEW_DRACO_RUST_OPT_LEVEL"),
        cxx_runtime_build: cxx_runtime_build(),
        cxx_runtime_loaded: cxx_runtime_loaded(),
        threads: 1,
        rounding: "each dequantized float32 coordinate rounded half-to-even (explicit Rust implementation), then required to be an integer in 0..=1023; record = u32 LE (x | y<<10 | z<<20) + R G B 255",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    /// Explicit half-way cases: C `round` would give 1, 2, 3, -1, 1024.
    #[test]
    fn halves_round_to_even() {
        let cases: &[(f32, f32)] = &[
            (0.5, 0.0),
            (1.5, 2.0),
            (2.5, 2.0),
            (3.5, 4.0),
            (-0.5, -0.0),
            (-1.5, -2.0),
            (-2.5, -2.0),
            (1022.5, 1022.0),
            (1023.5, 1024.0),
            (0.49999997, 0.0),
            (0.50000006, 1.0),
            (254.877, 255.0),
            (255.123, 255.0),
            (8_388_607.5, 8_388_608.0), // 2^23 - 0.5: the last f32 with a .5
            (16_777_216.0, 16_777_216.0),
        ];
        for &(x, want) in cases {
            let got = round_half_even_f32(x);
            assert_eq!(got.to_bits(), want.to_bits(), "{x} -> {got}, want {want}");
            assert_eq!(got.to_bits(), x.round_ties_even().to_bits(), "{x} vs std");
        }
        assert!(round_half_even_f32(f32::NAN).is_nan());
        assert_eq!(round_half_even_f32(f32::INFINITY), f32::INFINITY);
        assert_eq!(round_half_even_f32(-0.0).to_bits(), (-0.0f32).to_bits());
        // C's round would disagree on every one of these.
        for x in [0.5f32, 2.5, -0.5, 1022.5] {
            assert_ne!(round_half_even_f32(x), x.round(), "{x}");
        }
    }

    /// Cross-check with `f32::round_ties_even` over every f32 in [-2048,
    /// 2048) whose low mantissa bits make it a multiple of 2^-10, plus a
    /// deterministic pseudo-random sample of all bit patterns.
    #[test]
    fn explicit_rounding_matches_round_ties_even() {
        let mut k = -2048 * 1024i32;
        while k < 2048 * 1024 {
            let x = k as f32 / 1024.0;
            assert_eq!(round_half_even_f32(x).to_bits(), x.round_ties_even().to_bits(), "{x}");
            k += 1;
        }
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        for _ in 0..2_000_000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let x = f32::from_bits(state as u32);
            let (a, b) = (round_half_even_f32(x), x.round_ties_even());
            assert!(a.to_bits() == b.to_bits() || (a.is_nan() && b.is_nan()), "{x:e}");
        }
    }

    /// The per-coordinate fast path equals the explicit rounding restricted
    /// to the field range: every multiple of 2^-12 in [-2, 1026), every
    /// half-way value, and a pseudo-random sample of all f32 bit patterns.
    #[test]
    fn the_coordinate_fast_path_equals_the_explicit_rounding() {
        let reference = |v: f32| {
            let r = round_half_even_f32(v);
            (r.is_finite() && r >= 0.0 && r <= MAX_COORDINATE as f32).then(|| r as u32)
        };
        let mut k = -2 * 4096i32;
        while k < 1026 * 4096 {
            let v = k as f32 / 4096.0;
            assert_eq!(coordinate_field(v), reference(v), "{v}");
            k += 1;
        }
        for i in -4..2_060i32 {
            let v = i as f32 * 0.5;
            assert_eq!(coordinate_field(v), reference(v), "{v}");
            for v in [v.next_up(), v.next_down()] {
                assert_eq!(coordinate_field(v), reference(v), "{v:e}");
            }
        }
        let mut state = 0xD1B5_4A32_D192_ED03u64;
        for _ in 0..1_000_000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let v = f32::from_bits(state as u32);
            assert_eq!(coordinate_field(v), reference(v), "{v:e}");
        }
    }

    #[test]
    fn coordinate_fields_are_integers_in_range() {
        assert_eq!(coordinate_field(0.0), Some(0));
        assert_eq!(coordinate_field(-0.4), Some(0), "-0 is 0");
        assert_eq!(coordinate_field(-0.5), Some(0), "-0.5 rounds to -0");
        assert_eq!(coordinate_field(-0.6), None);
        assert_eq!(coordinate_field(254.877), Some(255));
        assert_eq!(coordinate_field(1023.4), Some(1023));
        assert_eq!(coordinate_field(1023.5), None, "rounds to 1024");
        assert_eq!(coordinate_field(f32::NAN), None);
        assert_eq!(coordinate_field(f32::INFINITY), None);
    }

    #[test]
    fn records_pack_like_pack_bin() {
        let cloud = DecodedCloud {
            positions: vec![1.0, 2.0, 3.0, 254.877, 0.123, 1022.5],
            colors: vec![10, 20, 30, 255, 0, 7],
        };
        let records = records_from_cloud(&cloud).unwrap();
        let w0 = 1u32 | (2 << 10) | (3 << 20);
        let w1 = 255u32 | (0 << 10) | (1022 << 20);
        let mut want = Vec::new();
        want.extend_from_slice(&w0.to_le_bytes());
        want.extend_from_slice(&[10, 20, 30, 255]);
        want.extend_from_slice(&w1.to_le_bytes());
        want.extend_from_slice(&[255, 0, 7, 255]);
        assert_eq!(records.bytes, want);
        assert!((records.max_abs_rounding - 0.5).abs() < 1e-6);
        let bad = DecodedCloud { positions: vec![1.0, 1024.0, 0.0], colors: vec![0, 0, 0] };
        assert_eq!(
            records_from_cloud(&bad).unwrap_err(),
            DracoError::Coordinate { point: 0, axis: 1, value: 1024.0 }
        );
    }

    #[test]
    fn the_vendored_library_is_draco_1_5_7_with_the_registered_flags() {
        let info = decoder_info();
        assert_eq!(info.version, DRACO_VERSION);
        assert_eq!(info.threads, 1);
        assert_eq!(info.fixed_flags, "-std=c++17 -O3 -DNDEBUG -fPIC -ffp-contract=off");
        for flag in info.fixed_flags.split(' ') {
            assert!(info.compiler_args.split(' ').any(|a| a == flag), "{flag} in {}", info.compiler_args);
        }
        for forbidden in ["-march=native", "-ffast-math", "-Ofast", "-O0", "-O2", "-ffp-contract=fast"] {
            assert!(!info.compiler_args.split(' ').any(|a| a == forbidden), "{forbidden}");
        }
        assert_eq!(info.vendored_sources_sha256.len(), 64);
        assert_eq!(info.compiled_sources, "64", "63 Draco sources + the shim");
        assert_eq!(info.rust_opt_level, "3", "the workspace profile override applies");
        assert!(info.cxx_runtime_build.starts_with("libstdc++ _GLIBCXX_RELEASE="), "{}", info.cxx_runtime_build);
        assert!(info.cxx_runtime_loaded.contains("libstdc++.so"), "{}", info.cxx_runtime_loaded);
    }

    /// Garbage, an empty buffer and a truncated stream are errors, never a
    /// panic or an unwinding C++ exception.
    #[test]
    fn invalid_streams_are_errors() {
        for data in [&b""[..], &b"DRACO"[..], &b"not a draco bitstream at all"[..]] {
            let error = decode_point_cloud(data).unwrap_err();
            assert!(matches!(error, DracoError::Shim { kind: DracoErrorKind::Decode, .. }), "{error}");
        }
        let fixture = std::fs::read(fixture_dir().join("synthetic_37.drc")).unwrap();
        for cut in [12, fixture.len() / 2, fixture.len() - 1] {
            assert!(decode_point_cloud(&fixture[..cut]).is_err(), "truncated at {cut}");
        }
    }

    fn fixture_dir() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
    }

    /// Synthetic fixtures encoded by DracoPy 2.0.0 (quantization_bits 10,
    /// compression_level 10, the experiment's encoder settings; see
    /// tests/fixtures/make_fixtures.py): bounding boxes whose largest side is not a
    /// power of two, so every dequantized coordinate is a non-integer float
    /// that only the rounding restores. Hermetic: no dataset bytes.
    #[test]
    fn synthetic_fixtures_decode_to_their_source_records() {
        let manifest: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(fixture_dir().join("manifest.json")).unwrap()).unwrap();
        let fixtures = manifest["fixtures"].as_array().unwrap();
        assert!(fixtures.len() >= 3);
        for f in fixtures {
            let name = f["name"].as_str().unwrap();
            let drc = std::fs::read(fixture_dir().join(format!("{name}.drc"))).unwrap();
            let bin = std::fs::read(fixture_dir().join(format!("{name}.bin"))).unwrap();
            let cloud = decode_point_cloud(&drc).unwrap();
            assert_eq!(cloud.num_points(), bin.len() / RECORD_BYTES, "{name}");
            let non_integer = cloud.positions.iter().filter(|v| v.fract() != 0.0).count();
            assert!(non_integer > 0, "{name}: the fixture must exercise the rounding");
            let records = records_from_cloud(&cloud).unwrap();
            assert_eq!(sorted_records(&records.bytes), sorted_records(&bin), "{name}: record multiset");
            assert!(records.max_abs_rounding < 0.5, "{name}");
            // DracoPy's own decode of the same bytes (float bits, point order).
            let ref_pos = std::fs::read(fixture_dir().join(format!("{name}.dracopy_pos_f32"))).unwrap();
            let ref_rgb = std::fs::read(fixture_dir().join(format!("{name}.dracopy_rgb_u8"))).unwrap();
            let pos_bytes: Vec<u8> = cloud.positions.iter().flat_map(|v| v.to_le_bytes()).collect();
            assert_eq!(pos_bytes, ref_pos, "{name}: float bits equal DracoPy's, in point order");
            assert_eq!(cloud.colors, ref_rgb, "{name}: colours equal DracoPy's");
        }
    }

    /// Three real frames (one per tier) from the LOCAL datasets (read only;
    /// the dataset bytes are never part of this repository). Fails loudly if
    /// the datasets are absent; the full 900-frame gate is
    /// `tests/draco_full_gate.rs`.
    #[test]
    fn real_frames_match_their_bin_records_as_multisets() {
        let datasets = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../datasets");
        let cases = [("d8", "tiers_draco_s3", 0u32), ("d7", "tiers_draco_s3", 150), ("d6", "tiers_draco_s3", 299)];
        for (tier, draco_root, frame) in cases {
            let drc_path = datasets.join(format!("{draco_root}/loot/{tier}/loot_{tier}_{frame:04}.drc"));
            let bin_path = datasets.join(format!("tiers_s3/loot/{tier}/loot_{tier}_{frame:04}.bin"));
            let drc = std::fs::read(&drc_path)
                .unwrap_or_else(|e| panic!("{} (this test reads the local datasets): {e}", drc_path.display()));
            let bin = std::fs::read(&bin_path).unwrap_or_else(|e| panic!("{}: {e}", bin_path.display()));
            let records = decode_to_records(&drc).unwrap();
            assert_eq!(records.bytes.len(), bin.len(), "{tier} {frame}: point count");
            assert_eq!(sorted_records(&records.bytes), sorted_records(&bin), "{tier} {frame}");
            assert_ne!(records.bytes, bin, "{tier} {frame}: the KD-tree coder reorders points");
            assert!(records.max_abs_rounding > 0.0 && records.max_abs_rounding < 0.5);
        }
    }
}
