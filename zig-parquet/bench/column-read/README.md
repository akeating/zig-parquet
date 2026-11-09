# Column read benchmark

Measures reading every row group of a Parquet file through `readRowGroupAsArrow` (Arrow path) and `readRowsProjected` / `readAllRows` (row path), one column at a time and all columns together. Reports wall time and allocator activity, and compares with PyArrow on the same file.

## Run

From the repo root:

```bash
# 1. Generate the test file (10M rows, 10 row groups, snappy; ~125 MB)
cd test-files-arrow
uv run python -I ../zig-parquet/bench/column-read/scripts/gen_bench.py /tmp/bench_10m.parquet

# 2. zig-parquet (builds ReleaseFast by default)
cd ../zig-parquet/bench/column-read
zig build run -- /tmp/bench_10m.parquet 5            # all cases, 5 measured runs
zig build run -- /tmp/bench_10m.parquet 5 arrow:id   # one case: PATH:COLUMN, PATH is arrow or rows, COLUMN a column name or all
zig build -Dcodecs=c-only run -- /tmp/bench_10m.parquet 5   # with the C codec libraries (rebuilds zig-out)

# 3. PyArrow reference (single-threaded)
cd ../../../test-files-arrow
uv run python -I ../zig-parquet/bench/column-read/scripts/pyarrow_ref.py /tmp/bench_10m.parquet 5
```

## Output columns

| Column | Meaning |
|---|---|
| median ms, min ms | Wall time over the measured runs, after one warm-up run. The file is read from the OS page cache. |
| allocs, resizes | Allocator calls in one run. |
| alloc MB | Total bytes allocated in one run. |
| peak MB | Maximum bytes live during a run, above those live when it started. |
| retained MB | Bytes still live when a run ends. Non-zero means memory held after the result is freed. |
| check | Checksum of lengths and null counts; equal across paths when they read the same data. |

## Baseline (2026-11-08, Apple M2, Zig 0.17.1, ReleaseFast, pure-Zig codecs)

| Case | median ms | allocs | alloc MB | peak MB | PyArrow ms | PyArrow peak MB |
|---|---|---|---|---|---|---|
| arrow id | 239 | 3,951 | 1,287 | 49.9 | 39.4 | 14.3 |
| arrow score | 164 | 4,974 | 1,367 | 55.0 | 39.0 | 19.7 |
| arrow category | 356 | 10,014,397 | 1,569 | 58.9 | 91.8 | 23.6 |
| arrow all | 750 | 10,022,942 | 4,223 | 75.1 | 178.8 | 57.6 |
| rows id | 452 | 10,003,811 | 1,820 | 88.0 | | |
| rows score | 397 | 10,004,913 | 2,020 | 92.0 | | |
| rows category | 555 | 10,014,327 | 2,652 | 732.0 | | |
| rows all | 1,228 | 10,022,881 | 6,529 | 1,632.1 | | |

PyArrow peak is its memory pool's process-wide maximum, which only increases as cases run, so it is an upper bound for each case after the first. The row path's string cases retain memory between runs (`DynamicReader.bytes_arena` is freed only in `deinit`), which inflates their peak.

## Flat-column decoder (2026-11-09, same machine and file)

Arrow path after flat columns were moved to `flat_column.zig`, with block copies for byte arrays and u64 bit-unpacking. The row path is unchanged.

| Case | median ms, Zig codecs | median ms, C codecs (`-Dcodecs=c-only`) | allocs | alloc MB | peak MB | PyArrow ms |
|---|---|---|---|---|---|---|
| arrow id | 114 | 63 | 2,800 | 208 | 14.4 | 39.4 |
| arrow score | 51 | 50 | 2,855 | 235 | 17.7 | 39.0 |
| arrow category | 72 | 71 | 11,897 | 453 | 30.2 | 91.8 |
| arrow all | 237 | 180 | 17,172 | 896 | 46.5 | 178.8 |

For `id`, about 84% of samples are in snappy decompression with the pure-Zig codec (macOS `sample`, 5 s).
