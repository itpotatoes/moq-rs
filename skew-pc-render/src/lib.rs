//! Deterministic CPU point splatting for the experiment 1-D L2 PC sink
//! (registration `md/20261022_실험1D_L2사다리_사전등록_문안.md` §3-2: wl_shm
//! buffers, CPU point drawing, point scale from `tiers_meta.json`
//! `depth_scale`; §11-8 M3 note: overlapping pixels must get a deterministic
//! colour whatever the point order).
//!
//! Input: the 8-byte point records both representations deliver (`.bin`
//! frames as sent, Draco frames after `skew_draco_sys` decoding):
//! `u32 LE (x | y << 10 | z << 20)` then `R, G, B, A` (A ignored). A tier's
//! coordinates are in units of `depth_scale` vox10 voxels (d8: 4, d7: 8,
//! d6: 16), so a point covers the vox10 cube `[c·s, (c+1)·s)` per axis.
//!
//! Projection (integer arithmetic only): an orthographic front view of the
//! vox10 cube `[0, 1024)^3` onto a square canvas of `side` pixels, x to the
//! right, y up, the viewer on the +z side. The vox10 interval `[a, b)` maps to
//! the pixel interval `[⌊a·side/1024⌋, ⌊b·side/1024⌋)`, so adjacent voxels tile
//! the canvas without gaps or overlaps (a voxel always covers >= 1 pixel when
//! `side >= 1024 / s`).
//!
//! Visibility, independent of point order: every pixel keeps the MAXIMUM of
//! the keys `((z + 1) << 24) | rgb` of the points covering it — the nearest
//! point (largest z) wins, and among equally near points the larger 24-bit
//! RGB value wins. `max` is commutative and associative, so the image is a
//! function of the record MULTISET: a `.bin` frame and the Draco frame of
//! the same content render to identical pixels although the Draco KD-tree
//! coder reorders points. A pixel no point covers gets [`BACKGROUND`].

/// Bytes per point record.
pub const RECORD_BYTES: usize = 8;
/// The vox10 cube side.
pub const VOX10: u64 = 1024;
/// XRGB8888 background (`0x00RRGGBB`).
pub const BACKGROUND: u32 = 0x0014_1419;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RenderError {
    /// No record at all.
    Empty,
    /// The payload is not a whole number of 8-byte records.
    NotWholeRecords { len: usize },
    /// `depth_scale` is 0 or larger than the vox10 cube.
    BadDepthScale { depth_scale: u32 },
    /// The two top bits of a coordinate word are set.
    ReservedBits { point: usize },
    /// A coordinate whose voxel leaves the vox10 cube at this depth scale
    /// (`(c + 1) · s > 1024`): wrong tier or a corrupt frame.
    CoordinateOutOfRange { point: usize, axis: usize, value: u32 },
    /// The output pixel buffer is not `side × side`.
    OutputSize { len: usize, side: u32 },
}

impl RenderError {
    pub fn reason(&self) -> &'static str {
        match self {
            Self::Empty => "empty_frame",
            Self::NotWholeRecords { .. } => "not_whole_records",
            Self::BadDepthScale { .. } => "bad_depth_scale",
            Self::ReservedBits { .. } => "reserved_bits_set",
            Self::CoordinateOutOfRange { .. } => "coordinate_out_of_range",
            Self::OutputSize { .. } => "output_size",
        }
    }
}

impl std::fmt::Display for RenderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "empty point frame"),
            Self::NotWholeRecords { len } => write!(f, "{len} bytes is not a whole number of 8-byte records"),
            Self::BadDepthScale { depth_scale } => write!(f, "depth_scale {depth_scale} not in 1..=1024"),
            Self::ReservedBits { point } => write!(f, "point {point}: reserved coordinate bits set"),
            Self::CoordinateOutOfRange { point, axis, value } => {
                write!(f, "point {point} axis {axis}: coordinate {value} leaves the vox10 cube")
            }
            Self::OutputSize { len, side } => write!(f, "output has {len} pixels, not {side}x{side}"),
        }
    }
}

impl std::error::Error for RenderError {}

/// What one render did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RenderStats {
    pub points: u64,
    /// Pixels at least one point covers.
    pub covered_pixels: u64,
}

/// The reusable per-pixel key buffer of one canvas size (owned by the render
/// thread, never shared).
pub struct Canvas {
    side: u32,
    keys: Vec<u64>,
}

impl Canvas {
    pub fn new(side: u32) -> Self {
        assert!(side > 0, "a canvas has pixels");
        Self { side, keys: vec![0; side as usize * side as usize] }
    }

    pub fn side(&self) -> u32 {
        self.side
    }
}

/// Check a record payload against a depth scale without drawing anything:
/// the number of points, or why it cannot be drawn.
pub fn validate(records: &[u8], depth_scale: u32) -> Result<usize, RenderError> {
    if depth_scale == 0 || depth_scale as u64 > VOX10 {
        return Err(RenderError::BadDepthScale { depth_scale });
    }
    if records.is_empty() {
        return Err(RenderError::Empty);
    }
    if records.len() % RECORD_BYTES != 0 {
        return Err(RenderError::NotWholeRecords { len: records.len() });
    }
    let limit = VOX10 / depth_scale as u64; // c + 1 <= limit
    for (point, record) in records.chunks_exact(RECORD_BYTES).enumerate() {
        let word = u32::from_le_bytes(record[..4].try_into().expect("4 bytes"));
        if word >> 30 != 0 {
            return Err(RenderError::ReservedBits { point });
        }
        for axis in 0..3 {
            let value = (word >> (10 * axis)) & 0x3FF;
            if value as u64 + 1 > limit {
                return Err(RenderError::CoordinateOutOfRange { point, axis, value });
            }
        }
    }
    Ok(records.len() / RECORD_BYTES)
}

/// `[⌊a·side/1024⌋, ⌊b·side/1024⌋)` for the vox10 interval `[a, b)`.
#[inline]
fn span(a: u64, b: u64, side: u64) -> (usize, usize) {
    ((a * side / VOX10) as usize, (b * side / VOX10) as usize)
}

/// Render one frame into `out` (`side × side` XRGB8888 pixels, row 0 at the
/// top). Validates first, so a frame is either drawn completely or not at
/// all (`out` untouched on error).
pub fn render(records: &[u8], depth_scale: u32, canvas: &mut Canvas, out: &mut [u32]) -> Result<RenderStats, RenderError> {
    let side = canvas.side;
    if out.len() != side as usize * side as usize {
        return Err(RenderError::OutputSize { len: out.len(), side });
    }
    let points = validate(records, depth_scale)?;
    let s = depth_scale as u64;
    let n = side as u64;
    let width = side as usize;
    canvas.keys.fill(0);
    for record in records.chunks_exact(RECORD_BYTES) {
        let word = u32::from_le_bytes([record[0], record[1], record[2], record[3]]) as u64;
        let (x, y, z) = (word & 0x3FF, (word >> 10) & 0x3FF, (word >> 20) & 0x3FF);
        let rgb = ((record[4] as u64) << 16) | ((record[5] as u64) << 8) | record[6] as u64;
        let key = ((z + 1) << 24) | rgb;
        let (c0, c1) = span(x * s, (x + 1) * s, n);
        let (v0, v1) = span(y * s, (y + 1) * s, n);
        // y up: vox10 rows [v0, v1) are canvas rows [side - v1, side - v0).
        for row in (width - v1)..(width - v0) {
            let line = &mut canvas.keys[row * width + c0..row * width + c1];
            for slot in line {
                if key > *slot {
                    *slot = key;
                }
            }
        }
    }
    let mut covered = 0u64;
    for (pixel, &key) in out.iter_mut().zip(canvas.keys.iter()) {
        if key == 0 {
            *pixel = BACKGROUND;
        } else {
            covered += 1;
            *pixel = (key & 0x00FF_FFFF) as u32;
        }
    }
    Ok(RenderStats { points: points as u64, covered_pixels: covered })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(x: u32, y: u32, z: u32, rgb: [u8; 3]) -> [u8; 8] {
        let w = x | (y << 10) | (z << 20);
        let b = w.to_le_bytes();
        [b[0], b[1], b[2], b[3], rgb[0], rgb[1], rgb[2], 255]
    }

    fn frame(points: &[[u8; 8]]) -> Vec<u8> {
        points.iter().flatten().copied().collect()
    }

    fn draw(records: &[u8], scale: u32, side: u32) -> Vec<u32> {
        let mut canvas = Canvas::new(side);
        let mut out = vec![0u32; side as usize * side as usize];
        render(records, scale, &mut canvas, &mut out).unwrap();
        out
    }

    /// A deterministic pseudo-random frame with overlaps: many points share
    /// pixels at the small canvas, some with equal z and different colours.
    fn busy_frame(n: usize, seed: u64, range: u32) -> Vec<[u8; 8]> {
        let mut state = seed;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        (0..n)
            .map(|_| {
                let r = next();
                let rgb = [(r >> 32) as u8, (r >> 40) as u8, (r >> 48) as u8];
                record((r % range as u64) as u32, ((r >> 10) % range as u64) as u32, ((r >> 20) % 4) as u32, rgb)
            })
            .collect()
    }

    #[test]
    fn the_image_does_not_depend_on_point_order() {
        let points = busy_frame(5_000, 0x1234_5678_9ABC_DEF0, 256);
        let forward = draw(&frame(&points), 4, 300);
        let mut reversed = points.clone();
        reversed.reverse();
        assert_eq!(draw(&frame(&reversed), 4, 300), forward);
        // A fixed shuffle (rotate + interleave).
        let mut shuffled: Vec<[u8; 8]> = points.iter().step_by(2).chain(points.iter().skip(1).step_by(2)).copied().collect();
        shuffled.rotate_left(1_234);
        assert_eq!(draw(&frame(&shuffled), 4, 300), forward);
    }

    #[test]
    fn nearest_point_wins_then_the_larger_colour() {
        // Same pixel footprint (same x, y), different z / colours.
        let far = record(10, 10, 3, [0, 0, 255]);
        let near = record(10, 10, 9, [1, 0, 0]);
        let tie_low = record(20, 20, 5, [0, 255, 0]);
        let tie_high = record(20, 20, 5, [0, 255, 1]);
        for order in [vec![far, near, tie_low, tie_high], vec![tie_high, near, tie_low, far]] {
            let out = draw(&frame(&order), 16, 1024);
            // Voxel (10, 10) at scale 16 and side 1024 covers columns
            // 160..176, rows 1024-176..1024-160.
            assert_eq!(out[(1024 - 170) * 1024 + 165], 0x0001_0000, "near wins over far");
            assert_eq!(out[(1024 - 330) * 1024 + 325], 0x0000_FF01, "equal z: larger RGB");
        }
    }

    #[test]
    fn voxels_tile_the_canvas_without_gaps() {
        // A full 64x64 face of d6 voxels (scale 16) covers the whole canvas
        // at side 1080, every pixel exactly once in effect.
        let mut points = Vec::new();
        for x in 0..64 {
            for y in 0..64 {
                points.push(record(x, y, 0, [1, 2, 3]));
            }
        }
        let mut canvas = Canvas::new(1080);
        let mut out = vec![0u32; 1080 * 1080];
        let stats = render(&frame(&points), 16, &mut canvas, &mut out).unwrap();
        assert_eq!(stats.points, 4_096);
        assert_eq!(stats.covered_pixels, 1080 * 1080, "no gap anywhere");
        assert!(out.iter().all(|&p| p == 0x0001_0203));
        // One voxel in the corner of a d8 frame: x = 0, y = 0 is bottom-left.
        let out = draw(&frame(&[record(0, 0, 0, [9, 9, 9])]), 4, 1080);
        let covered: Vec<usize> = (0..out.len()).filter(|&i| out[i] != BACKGROUND).collect();
        // [0, 4) vox10 -> [0, 4) px at 1080 (4 * 1080 / 1024 = 4.2 -> 4).
        assert_eq!(covered.len(), 16);
        assert!(covered.iter().all(|&i| i / 1080 >= 1076 && i % 1080 < 4), "bottom-left");
    }

    #[test]
    fn invalid_frames_are_refused_and_leave_the_output_untouched() {
        let mut canvas = Canvas::new(64);
        let mut out = vec![7u32; 64 * 64];
        assert_eq!(render(&[], 4, &mut canvas, &mut out), Err(RenderError::Empty));
        assert_eq!(render(&[0; 12], 4, &mut canvas, &mut out), Err(RenderError::NotWholeRecords { len: 12 }));
        assert_eq!(render(&frame(&[record(1, 1, 1, [0; 3])]), 0, &mut canvas, &mut out), Err(RenderError::BadDepthScale { depth_scale: 0 }));
        // d8 data (x = 255) is fine at scale 4, out of the cube at scale 8.
        let d8 = frame(&[record(255, 3, 3, [0; 3])]);
        assert!(render(&d8, 4, &mut canvas, &mut out).is_ok());
        out.fill(7);
        assert_eq!(
            render(&d8, 8, &mut canvas, &mut out),
            Err(RenderError::CoordinateOutOfRange { point: 0, axis: 0, value: 255 })
        );
        let mut reserved = record(1, 1, 1, [0; 3]);
        reserved[3] |= 0x40;
        assert_eq!(render(&reserved, 4, &mut canvas, &mut out), Err(RenderError::ReservedBits { point: 0 }));
        assert!(out.iter().all(|&p| p == 7), "untouched on error");
        let mut small = vec![0u32; 10];
        assert_eq!(render(&d8, 4, &mut canvas, &mut small), Err(RenderError::OutputSize { len: 10, side: 64 }));
    }

    #[test]
    fn alpha_is_ignored() {
        let mut a = record(5, 5, 5, [1, 2, 3]);
        let b = a;
        a[7] = 0;
        assert_eq!(draw(&a, 4, 256), draw(&b, 4, 256));
    }
}
