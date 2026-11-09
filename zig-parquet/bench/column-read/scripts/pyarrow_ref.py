"""PyArrow reference timings for the column-read benchmark. Usage: pyarrow_ref.py FILE [RUNS]

Reads every row group, one column at a time and all columns, single-threaded (use_threads=False)
to match the Zig benchmark. Reports median and minimum wall time over RUNS after one warm-up run,
and the Arrow memory pool's peak allocation."""
import statistics
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq

path = sys.argv[1]
runs = int(sys.argv[2]) if len(sys.argv) > 2 else 5
pf = pq.ParquetFile(path)
names = pf.schema_arrow.names
pool = pa.default_memory_pool()
print(f"pyarrow {pa.__version__}, pool {pool.backend_name}, use_threads=False")
print(f"{'column':<10} {'median ms':>10} {'min ms':>10} {'peak MB':>10}")

for cols in [[n] for n in names] + [None]:
    times = []
    for run in range(runs + 1):
        pf = pq.ParquetFile(path)
        start = time.perf_counter()
        for rg in range(pf.num_row_groups):
            t = pf.read_row_group(rg, columns=cols, use_threads=False)
            del t
        elapsed = time.perf_counter() - start
        if run > 0:
            times.append(elapsed * 1000)
    label = cols[0] if cols else "all"
    print(f"{label:<10} {statistics.median(times):>10.1f} {min(times):>10.1f} {pool.max_memory() / 2**20:>10.1f}")
