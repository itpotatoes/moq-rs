"""DracoPy 2.0.0 reference digests for the 900-frame gate (no dataset bytes).

Run with the Skew repository's .venv from the repository root:
  .venv/bin/python tools/moq-rs/skew-draco-sys/tests/fixtures/make_dracopy_digests.py \
      datasets tools/moq-rs/skew-draco-sys/tests/fixtures/dracopy_digests.json

For every frame of d8/d7/d6 (300 each): sha256 of the .drc input, DracoPy's
decoded point count, sha256 of its positions as float32 LE bits in Draco
point order, sha256 of its colours (u8 x 3, point order), and sha256 of the
source .bin. The gate (tests/draco_full_gate.rs) recomputes the same digests
from the vendored decoder, so equality means bit-identical decoder output.
"""
import hashlib
import importlib.metadata
import json
import sys
from pathlib import Path

import numpy as np
import DracoPy

TIERS = ("d8", "d7", "d6")


def main(datasets: Path, out: Path):
    frames = []
    for tier in TIERS:
        for i in range(300):
            drc = (datasets / f"tiers_draco_s3/loot/{tier}/loot_{tier}_{i:04d}.drc").read_bytes()
            src = (datasets / f"tiers_s3/loot/{tier}/loot_{tier}_{i:04d}.bin").read_bytes()
            dec = DracoPy.decode(drc)
            pts = np.asarray(dec.points)
            pts32 = pts.astype("<f4")
            assert np.array_equal(pts32.astype(pts.dtype), pts), "float32 values"
            col = np.ascontiguousarray(np.asarray(dec.colors), dtype=np.uint8)
            frames.append({
                "tier": tier,
                "frame": i,
                "drc_sha256": hashlib.sha256(drc).hexdigest(),
                "bin_sha256": hashlib.sha256(src).hexdigest(),
                "points": int(len(pts32)),
                "pos_f32_sha256": hashlib.sha256(pts32.tobytes()).hexdigest(),
                "rgb_u8_sha256": hashlib.sha256(col.tobytes()).hexdigest(),
            })
    doc = {
        "generator": "make_dracopy_digests.py",
        "decoder": f"DracoPy {importlib.metadata.version('DracoPy')}",
        "inputs": "datasets/tiers_draco_s3/loot/<tier>/loot_<tier>_<i>.drc, datasets/tiers_s3/loot/<tier>/loot_<tier>_<i>.bin",
        "frames": frames,
    }
    out.write_text(json.dumps(doc, indent=0) + "\n")
    print(len(frames), "frames")


if __name__ == "__main__":
    main(Path(sys.argv[1]), Path(sys.argv[2]))
