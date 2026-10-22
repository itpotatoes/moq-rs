"""Regenerate the synthetic Draco fixtures of skew-draco-sys (no dataset bytes).

Run with the Skew repository's .venv (DracoPy 2.0.0 = Draco 1.5.7, the
experiment's encoder):  .venv/bin/python make_fixtures.py <out_dir>

Each fixture is a deterministic synthetic cloud of distinct integer
coordinates with random RGB colours, packed exactly like
draco/draco_codec.py `pack_bin` (u32 LE x | y<<10 | z<<20, then R G B 255)
and encoded with the experiment's settings (quantization_bits=10,
compression_level=10, point cloud). The largest bounding-box side is not a
power of two, so the dequantized floats are not integers. For each fixture
the files are: <name>.bin (source records), <name>.drc (DracoPy encode),
<name>.dracopy_pos_f32 / <name>.dracopy_rgb_u8 (DracoPy's decode of the .drc:
float32 LE bits and colours, Draco point order).
"""
import hashlib
import importlib.metadata
import json
import sys
from pathlib import Path

import numpy as np
import DracoPy

QBITS = 10
LEVEL = 10
# name, point count, per-axis extents (largest side = the quantization range)
SPECS = [
    ("synthetic_37", 400, (37, 20, 30), 11),
    ("synthetic_255", 1500, (255, 200, 180), 22),
    ("synthetic_1000", 1200, (1000, 600, 400), 33),
]


def pack(coords, colors):
    rec = np.empty(len(coords), dtype=[("coord", "<u4"), ("rgba", "u1", 4)])
    c = coords.astype(np.uint32)
    rec["coord"] = c[:, 0] | (c[:, 1] << np.uint32(10)) | (c[:, 2] << np.uint32(20))
    rgba = np.empty((len(coords), 4), dtype=np.uint8)
    rgba[:, :3] = colors
    rgba[:, 3] = 255
    rec["rgba"] = rgba
    return rec.tobytes()


def main(out):
    out.mkdir(parents=True, exist_ok=True)
    manifest = {
        "generator": "make_fixtures.py",
        "encoder": f"DracoPy {importlib.metadata.version('DracoPy')}",
        "quantization_bits": QBITS,
        "compression_level": LEVEL,
        "fixtures": [],
    }
    for name, n, extent, seed in SPECS:
        rng = np.random.default_rng(seed)
        seen, coords = set(), []
        while len(coords) < n:
            p = tuple(int(rng.integers(0, e + 1)) for e in extent)
            if p not in seen:
                seen.add(p)
                coords.append(p)
        coords = np.array(coords, dtype=np.int64)
        # Pin both ends of the largest axis so the range is exactly extent[0].
        coords[0, 0], coords[1, 0] = 0, extent[0]
        assert len({tuple(c) for c in coords}) == n
        colors = rng.integers(0, 256, size=(n, 3), dtype=np.uint8)
        bin_bytes = pack(coords, colors)
        drc = DracoPy.encode(coords.astype(np.float32), colors=colors,
                             quantization_bits=QBITS, compression_level=LEVEL)
        dec = DracoPy.decode(drc)
        pts = np.asarray(dec.points)
        pts32 = pts.astype("<f4")
        assert np.array_equal(pts32.astype(pts.dtype), pts), "DracoPy floats are float32"
        col = np.ascontiguousarray(np.asarray(dec.colors), dtype=np.uint8)
        # Lossless after round-half-even (numpy rint), as a multiset.
        back = pack(np.rint(pts32).astype(np.int64), col)
        a = np.sort(np.frombuffer(bin_bytes, "<u8"))
        b = np.sort(np.frombuffer(back, "<u8"))
        assert np.array_equal(a, b), f"{name}: not lossless"
        non_integer = int(np.count_nonzero(pts32 != np.rint(pts32)))
        assert non_integer > 0, f"{name}: no non-integer coordinate"
        files = {
            f"{name}.bin": bin_bytes,
            f"{name}.drc": drc,
            f"{name}.dracopy_pos_f32": pts32.tobytes(),
            f"{name}.dracopy_rgb_u8": col.tobytes(),
        }
        for fname, data in files.items():
            (out / fname).write_bytes(data)
        manifest["fixtures"].append({
            "name": name,
            "points": n,
            "extent": list(extent),
            "seed": seed,
            "non_integer_coordinates": non_integer,
            "max_abs_rounding": float(np.max(np.abs(pts32 - np.rint(pts32)))),
            "sha256": {f: hashlib.sha256(d).hexdigest() for f, d in files.items()},
        })
    (out / "manifest.json").write_text(json.dumps(manifest, indent=1) + "\n")
    for f in manifest["fixtures"]:
        print(f["name"], f["points"], "non-integer", f["non_integer_coordinates"],
              "max |x - rint x|", round(f["max_abs_rounding"], 4))


if __name__ == "__main__":
    main(Path(sys.argv[1]))
