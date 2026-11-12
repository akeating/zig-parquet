"""Writes snappy inputs for bench-snappy: NAME.raw (uncompressed) and NAME.snappy (a raw
snappy block compressed by PyArrow) in OUT_DIR. Usage: snappy_inputs.py OUT_DIR"""
import json
import sys

import numpy as np
import pyarrow as pa

out = sys.argv[1]
rng = np.random.default_rng(1)
n = 1_000_000


def plain_strings(strs):
    """PLAIN BYTE_ARRAY page values: a 4-byte length before each string."""
    parts = []
    for s in strs:
        b = s.encode()
        parts.append(len(b).to_bytes(4, "little") + b)
    return b"".join(parts)


inputs = {
    # PLAIN int64 ids 0..n: what the flat benchmark's id pages hold
    "ids": np.arange(n, dtype=np.int64).tobytes(),
    # PLAIN int64 nanosecond timestamps with irregular gaps
    "timestamps": np.cumsum(rng.integers(100_000, 3_000_000_000, n)).astype(np.int64).tobytes(),
    # PLAIN random doubles: little to compress
    "doubles": (rng.random(n) * 100.0).tobytes(),
    # PLAIN strings from 1,000 distinct values (a category column without dictionary)
    "strings": plain_strings([f"category_{i:04d}" for i in rng.integers(0, 1000, 600_000)]),
    # PLAIN JSON-like text records
    "json": plain_strings(
        [
            json.dumps({"id": i, "name": f"user_{i % 5000}", "score": round(float(s), 3), "tags": ["a", "b", "c"][: i % 4]})
            for i, s in zip(range(150_000), rng.random(150_000))
        ]
    ),
}
for name, raw in inputs.items():
    comp = pa.compress(raw, codec="snappy", asbytes=True)
    open(f"{out}/{name}.raw", "wb").write(raw)
    open(f"{out}/{name}.snappy", "wb").write(comp)
    print(f"{name:<11} raw {len(raw) / 1e6:6.2f} MB  snappy {len(comp) / 1e6:6.2f} MB  ratio {len(raw) / len(comp):5.2f}")
