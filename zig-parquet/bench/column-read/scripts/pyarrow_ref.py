"""PyArrow reference timings for the column-read benchmark. Usage: pyarrow_ref.py FILE [RUNS]

Reads the file one column at a time and all columns, single-threaded (use_threads=False) to
match the Zig benchmark, in two ways: each row group whole (read_row_group), and in batches of
65,536 rows (iter_batches). Reports median and minimum wall time over RUNS after one warm-up
run, and the Arrow memory pool's peak allocation.

The pool's peak cannot be reset, so each case runs in its own process."""
import statistics
import subprocess
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq

BATCH_ROWS = 65536


def run_case(path, runs, mode, column):
    cols = None if column == "all" else [column]
    times = []
    for run in range(runs + 1):
        pf = pq.ParquetFile(path)
        start = time.perf_counter()
        if mode == "rg":
            for rg in range(pf.num_row_groups):
                t = pf.read_row_group(rg, columns=cols, use_threads=False)
                del t
        else:
            for b in pf.iter_batches(batch_size=BATCH_ROWS, columns=cols, use_threads=False):
                del b
        elapsed = time.perf_counter() - start
        if run > 0:
            times.append(elapsed * 1000)
    peak = pa.default_memory_pool().max_memory() / 2**20
    print(f"{mode:<8} {column:<10} {statistics.median(times):>10.1f} {min(times):>10.1f} {peak:>10.1f}", flush=True)


path = sys.argv[1]
runs = int(sys.argv[2]) if len(sys.argv) > 2 else 5
if len(sys.argv) > 4:
    run_case(path, runs, sys.argv[3], sys.argv[4])
    sys.exit(0)

pool = pa.default_memory_pool()
print(f"pyarrow {pa.__version__}, pool {pool.backend_name}, use_threads=False, batches of {BATCH_ROWS} rows")
print(f"{'mode':<8} {'column':<10} {'median ms':>10} {'min ms':>10} {'peak MB':>10}", flush=True)
names = pq.ParquetFile(path).schema_arrow.names
for mode in ["rg", "batches"]:
    for column in names + ["all"]:
        subprocess.run([sys.executable, "-I", __file__, path, str(runs), mode, column], check=True)
