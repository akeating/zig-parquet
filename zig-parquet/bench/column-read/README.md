# Column read benchmark

Measures reading every row group of a Parquet file through `readRowGroupAsArrow` (Arrow path) and `readRowsProjected` / `readAllRows` (row path), one top-level column at a time and all columns together. Reports wall time and allocator activity, and compares with PyArrow on the same file.

## Run

From the repo root:

```bash
# 1. Generate the test files (snappy, 1Mi rows per row group)
cd test-files-arrow
uv run python -I ../zig-parquet/bench/column-read/scripts/gen_bench.py /tmp/bench_10m.parquet             # flat, 10M rows, ~125 MB
uv run python -I ../zig-parquet/bench/column-read/scripts/gen_bench.py /tmp/bench_nested_2m.parquet --nested  # list/struct/map, 2M rows, ~59 MB

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

## Current results (2026-11-09, Apple M2, Zig 0.17.1, ReleaseFast)

Arrow path rows measured at commit 32643e1; row path rows at the commit that last changed them in this file. Median of 5 runs. "Zig codecs" is the default build (pure-Zig decompressors); "C codecs" is `-Dcodecs=c-only`. PyArrow 23.0.1 with `use_threads=False`. Medians vary by about 5% between runs.

Flat file (`bench_10m.parquet`, 10M rows):

| Case | ms, Zig codecs | ms, C codecs | allocs | alloc MB | peak MB | PyArrow ms | PyArrow peak MB |
|---|---|---|---|---|---|---|---|
| arrow id | 59 | 65 | 2,810 | 208 | 14.4 | 39.6 | 14.3 |
| arrow score | 48 | 51 | 2,865 | 235 | 17.7 | 36.6 | 19.7 |
| arrow category | 64 | 64 | 11,907 | 453 | 30.2 | 92.1 | 23.6 |
| arrow all | 171 | 175 | 17,182 | 896 | 46.5 | 174.6 | 57.6 |
| rows id | 150 | | 2,630 | 933 | 76.0 | | |
| rows score | 145 | | 2,685 | 960 | 76.0 | | |
| rows category | 167 | | 11,737 | 1,216 | 89.0 | | |
| rows all | 332 | | 16,982 | 2,346 | 161.0 | | |

Nested file (`bench_nested_2m.parquet`, 2M rows):

| Case | ms, Zig codecs | ms, C codecs | allocs | alloc MB | peak MB | PyArrow ms | PyArrow peak MB |
|---|---|---|---|---|---|---|---|
| arrow ints | 97 | 100 | 691 | 295 | 70.8 | 54.1 | 62.7 |
| arrow point | 32 | 33 | 1,222 | 156 | 37.4 | 29.5 | 62.7 |
| arrow attrs | 138 | 146 | 1,275 | 410 | 93.1 | 71.9 | 91.3 |
| arrow all | 268 | 291 | 2,988 | 861 | 136.0 | 155.6 | 134.7 |
| rows ints | 120 | | 591 | 708 | 239.4 | | |
| rows point | 83 | | 1,128 | 778 | 266.5 | | |
| rows attrs | 150 | | 1,161 | 994 | 365.1 | | |
| rows all | 333 | | 2,856 | 1,893 | 526.1 | | |

The row path writes a `Value` per entry and a `Row` per row. The values of a read are in one block of storage shared by its rows, and string values reference the decoded byte buffers.

Arrow path reading all columns, first measurement → now: flat file 750 → 171 ms (first measured at commit b64724f, before the column-read changes); nested file 479 → 268 ms (first measured at commit 2eee218, when list, struct and map assembly still built a `Value` per entry). Row path reading all columns: flat file 996 → 332 ms, nested file 1,167 → 333 ms (before at commit 32643e1). The commit messages on the branch record each change's effect.
