"""Generate a column-read benchmark file.

Usage: gen_bench.py OUT.parquet [ROWS] [--nested]

Flat file (default, 10M rows): id int64 required, score float64 (10% null), category string
with 1,000 distinct values.

Nested file (--nested, default 2M rows):
  ints   list<int64>: 0-5 elements per row, 10% of rows null
  point  struct<x: float64, label: string>: 10% of rows null; label has 100 distinct values
  attrs  map<string, int64>: 0-3 entries per row, keys from 100 distinct values

Written with PyArrow defaults: snappy, dictionary encoding on, 1Mi rows per row group.
"""
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

args = [a for a in sys.argv[1:] if not a.startswith("--")]
nested = "--nested" in sys.argv
out = args[0]
n = int(args[1]) if len(args) > 1 else (2_000_000 if nested else 10_000_000)
rng = np.random.default_rng(42)


def flat_table():
    ids = pa.array(np.arange(n, dtype=np.int64))
    scores = rng.random(n) * 100.0
    score = pa.array(scores, mask=rng.random(n) < 0.10)
    categories = np.array([f"category_{i:04d}" for i in range(1000)], dtype=object)
    category = pa.array(categories[rng.integers(0, 1000, n)], type=pa.string())
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("score", pa.float64(), nullable=True),
            pa.field("category", pa.string(), nullable=True),
        ]
    )
    return pa.Table.from_arrays([ids, score, category], schema=schema)


def offsets_for(lengths):
    offsets = np.zeros(len(lengths) + 1, dtype=np.int32)
    np.cumsum(lengths, out=offsets[1:])
    return offsets


def nested_table():
    # ints: list<int64>
    list_lens = rng.integers(0, 6, n)
    list_null = rng.random(n) < 0.10
    list_lens[list_null] = 0
    values = pa.array(rng.integers(0, 1_000_000, int(list_lens.sum())), type=pa.int64())
    ints = pa.ListArray.from_arrays(pa.array(offsets_for(list_lens)), values, mask=pa.array(list_null))

    # point: struct<x: float64, label: string>
    labels = np.array([f"label_{i:03d}" for i in range(100)], dtype=object)
    x = pa.array(rng.random(n) * 1000.0)
    label = pa.array(labels[rng.integers(0, 100, n)], type=pa.string())
    point = pa.StructArray.from_arrays([x, label], names=["x", "label"], mask=pa.array(rng.random(n) < 0.10))

    # attrs: map<string, int64>
    keys_pool = np.array([f"key_{i:03d}" for i in range(100)], dtype=object)
    map_lens = rng.integers(0, 4, n)
    total = int(map_lens.sum())
    keys = pa.array(keys_pool[rng.integers(0, 100, total)], type=pa.string())
    items = pa.array(rng.integers(0, 1_000_000, total), type=pa.int64())
    attrs = pa.MapArray.from_arrays(pa.array(offsets_for(map_lens)), keys, items)

    return pa.Table.from_arrays([ints, point, attrs], names=["ints", "point", "attrs"])


table = nested_table() if nested else flat_table()
pq.write_table(table, out)

md = pq.ParquetFile(out).metadata
print(f"rows={md.num_rows} row_groups={md.num_row_groups} created_by={md.created_by}")
for c in range(md.num_columns):
    col = md.row_group(0).column(c)
    print(
        f"  {col.path_in_schema}: codec={col.compression} encodings={col.encodings} "
        f"rg0 compressed={col.total_compressed_size} uncompressed={col.total_uncompressed_size}"
    )
