"""Generate the column-read benchmark file. Usage: gen_bench.py OUT.parquet [ROWS]"""
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

out = sys.argv[1]
n = int(sys.argv[2]) if len(sys.argv) > 2 else 10_000_000
rng = np.random.default_rng(42)

ids = pa.array(np.arange(n, dtype=np.int64))
scores = rng.random(n) * 100.0
score_null = rng.random(n) < 0.10
score = pa.array(scores, mask=score_null)
categories = np.array([f"category_{i:04d}" for i in range(1000)], dtype=object)
category = pa.array(categories[rng.integers(0, 1000, n)], type=pa.string())

schema = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("score", pa.float64(), nullable=True),
        pa.field("category", pa.string(), nullable=True),
    ]
)
table = pa.Table.from_arrays([ids, score, category], schema=schema)
# PyArrow defaults: snappy, dictionary encoding on, 1Mi rows per row group
pq.write_table(table, out)

md = pq.ParquetFile(out).metadata
print(f"rows={md.num_rows} row_groups={md.num_row_groups} created_by={md.created_by}")
for c in range(md.num_columns):
    col = md.row_group(0).column(c)
    print(
        f"  {col.path_in_schema}: codec={col.compression} encodings={col.encodings} "
        f"rg0 compressed={col.total_compressed_size} uncompressed={col.total_uncompressed_size}"
    )
