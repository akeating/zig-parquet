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

## Nested file baseline (2026-11-09, same machine)

`bench_nested_2m.parquet`: `ints` list<int64> (0-5 elements, 10% null), `point` struct<x: double, label: string> (10% null), `attrs` map<string, int64> (0-3 entries). Leaf columns are decoded by `leaf_decoder`; list, struct and map assembly in `arrow_batch.zig` still builds a `Value` per entry.

| Case | median ms | allocs | alloc MB | peak MB | PyArrow ms | PyArrow peak MB |
|---|---|---|---|---|---|---|
| arrow ints | 154 | 693 | 512 | 156.3 | 54.7 | 62.7 |
| arrow point | 92 | 1,801,120 | 325 | 65.4 | 29.6 | 62.7 |
| arrow attrs | 228 | 3,001,107 | 730 | 227.7 | 72.3 | 91.3 |
| arrow all | 479 | 4,802,720 | 1,567 | 270.6 | 156.2 | 134.7 |
| rows ints | 334 | 6,500,587 | 946 | 275.9 | | |
| rows point | 401 | 14,600,352 | 1,058 | 331.7 | | |
| rows attrs | 491 | 6,001,427 | 1,155 | 376.9 | | |
| rows all | 1,237 | 23,102,284 | 3,025 | 913.2 | | |

In a profile of `arrow ints` and `arrow attrs` (macOS `sample`, 5 s), about 60% of samples are in `Value` handling (`leaf_decoder.toValues`, `valuesToArrowArray`, `Value.deinit`, the list/map assembly, and the copies they make), and 20-25% in decompression and decoding.


## Nested assembly from buffers (2026-11-09, same machine and file)

After list, map and leaf assembly in `arrow_batch.zig` stopped building `Value`s: list and map children are taken from the decoded buffers by definition level, and every leaf (flat or not) is decoded directly into an Arrow array. The row path is unchanged.

| Case | median ms | allocs | alloc MB | peak MB | PyArrow ms | PyArrow peak MB |
|---|---|---|---|---|---|---|
| arrow ints | 121 | 689 | 295 | 70.8 | 54.7 | 62.7 |
| arrow point | 28 | 1,188 | 106 | 28.9 | 29.6 | 62.7 |
| arrow attrs | 154 | 1,273 | 410 | 93.1 | 72.3 | 91.3 |
| arrow all | 306 | 2,950 | 811 | 136.0 | 156.2 | 134.7 |

Medians varied by about 5% between runs (arrow all: 288-306 ms). For `ints`, about 40% of profile samples are in snappy decompression.

## Snappy decoder with block copies (2026-11-09, same machine and files)

The pure-Zig snappy decoder copies literals of up to 16 bytes and back-references in 8- or 16-byte blocks instead of `@memcpy` calls and byte loops, byte by byte only in the last 16 bytes of output.

| Case | median ms | PyArrow ms |
|---|---|---|
| arrow id | 52 | 39.4 |
| arrow score | 46 | 39.0 |
| arrow category | 64 | 91.8 |
| arrow all (flat) | 163 | 178.8 |
| arrow ints | 99 | 54.7 |
| arrow point | 32 | 29.6 |
| arrow attrs | 141 | 72.3 |
| arrow all (nested) | 272 | 156.2 |

Decompression alone (PyArrow-compressed inputs, GB/s of output, Zig before → after, C snappy): int64 ids 0.86 → 2.2 (2.0), int64 timestamps 0.90 → 2.2 (2.1), strings 2.5 → 4.3 (5.3), JSON-like text 1.5 → 2.7 (4.6), random doubles 20.7 → 20.4 (20.4).
