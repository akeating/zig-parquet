# Column read benchmark

Measures reading every row group of a Parquet file through `readRowGroupAsArrow` (Arrow path, one row group at a time), `ArrowBatchReader` (groups path, one batch per row group; batches path, 65,536 rows at a time) and `readRowsProjected` / `readAllRows` (row path), one top-level column at a time and all columns together. Reports wall time and allocator activity, and compares with PyArrow on the same file.

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
zig build run -- /tmp/bench_10m.parquet 5 arrow:id   # one case: PATH:COLUMN, PATH is arrow, groups, batches, rows, cgroups or cbatches, COLUMN a column name or all
zig build run -- /tmp/bench_10m.parquet 5 threads=8 ahead=1   # decode on up to 8 threads, 1 row group ahead (groups and batches paths)
zig build -Dcodecs=c-only run -- /tmp/bench_10m.parquet 5   # with the C codec libraries (rebuilds zig-out)

# 3. PyArrow reference (single-threaded; read_row_group and iter_batches, each case in its own process)
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

## Current results (2026-11-11, Apple M2, Zig 0.17.1, ReleaseFast)

Arrow and batches rows and the PyArrow columns measured in one run at commit e4a0b1a; row path rows at commit c733328 (2026-11-09), the last change to the row path. Median of 5 runs. "Zig codecs" is the default build (pure-Zig decompressors); "C codecs" is `-Dcodecs=c-only`. PyArrow 23.0.1 with `use_threads=False`: `read_row_group` for the arrow rows, `iter_batches(batch_size=65536)` for the batches rows. Medians vary by about 5% between runs.

Flat file (`bench_10m.parquet`, 10M rows):

| Case | ms, Zig codecs | ms, C codecs | allocs | alloc MB | peak MB | PyArrow ms | PyArrow peak MB |
|---|---|---|---|---|---|---|---|
| arrow id | 59 | 61 | 2,720 | 208 | 14.4 | 40.2 | 14.3 |
| arrow score | 49 | 47 | 2,775 | 235 | 17.7 | 40.4 | 19.7 |
| arrow category | 68 | 65 | 11,827 | 453 | 30.2 | 94.6 | 23.6 |
| arrow all | 177 | 176 | 17,182 | 896 | 46.5 | 182.8 | 57.6 |
| batches id | 63 | 68 | 5,993 | 292 | 3.3 | 41.1 | 11.5 |
| batches score | 53 | 50 | 6,058 | 293 | 3.9 | 43.8 | 18.5 |
| batches category | 63 | 59 | 15,517 | 442 | 3.2 | 95.3 | 4.2 |
| batches all | 176 | 177 | 25,402 | 1,027 | 8.2 | 187.6 | 28.4 |
| rows id | 150 | | 2,630 | 933 | 76.0 | | |
| rows score | 145 | | 2,685 | 960 | 76.0 | | |
| rows category | 167 | | 11,737 | 1,216 | 89.0 | | |
| rows all | 332 | | 16,982 | 2,346 | 161.0 | | |

Nested file (`bench_nested_2m.parquet`, 2M rows):

| Case | ms, Zig codecs | ms, C codecs | allocs | alloc MB | peak MB | PyArrow ms | PyArrow peak MB |
|---|---|---|---|---|---|---|---|
| arrow ints | 101 | 101 | 625 | 295 | 70.8 | 58.0 | 62.7 |
| arrow point | 33 | 34 | 1,166 | 156 | 37.4 | 31.3 | 51.9 |
| arrow attrs | 145 | 142 | 1,229 | 410 | 93.1 | 75.8 | 91.3 |
| arrow all | 278 | 280 | 2,988 | 861 | 136.0 | 160.3 | 134.7 |
| batches ints | 108 | 106 | 1,799 | 277 | 9.3 | 55.6 | 28.2 |
| batches point | 32 | 32 | 2,794 | 162 | 6.0 | 30.6 | 19.8 |
| batches attrs | 158 | 160 | 3,482 | 404 | 10.3 | 72.8 | 25.2 |
| batches all | 301 | 297 | 7,631 | 843 | 21.0 | 161.6 | 56.9 |
| rows ints | 120 | | 591 | 708 | 239.4 | | |
| rows point | 83 | | 1,128 | 778 | 266.5 | | |
| rows attrs | 150 | | 1,161 | 994 | 365.1 | | |
| rows all | 333 | | 2,856 | 1,893 | 526.1 | | |

Nested file after the level changes (2026-11-12): the buffers of a list or map leaf have no slots for empty and null lists, `levelShape` has no branches on the levels, levels are `u8`, and the batch cursor counts row starts 32 levels at a time. Zig codecs, median of 9 runs:

| Case | ms | allocs | alloc MB | peak MB |
|---|---|---|---|---|
| arrow ints | 60 | 591 | 123 | 42.0 |
| arrow point | 29 | 1,140 | 111 | 31.2 |
| arrow attrs | 73 | 1,157 | 184 | 50.0 |
| arrow all | 166 | 2,852 | 418 | 92.8 |
| batches all | 176 | 7,411 | 503 | 13.9 |
| rows all | 323 | 2,726 | 1,551 | 505.1 |

The batches path reads and decodes pages as batches need them. Between batches it holds, per leaf column, one compressed page, the dictionary and at most one page of decoded entries (since 2026-11-12 also buffers with room for the next batch's entries); its peak is that plus one batch. Batches take rows from as many row groups as they need; a batch that crosses a row group end joins the pieces from both, which adds to the peak (nested file: 1,000,000-row row groups, so 1 batch in 31 crosses one). Reading all columns took 176 ms (flat) and 301 ms (nested) in batches, against 177 ms and 278 ms for whole row groups: entries past the end of a batch are copied, and there are more allocations.

## Threads and read-ahead (2026-11-12, Apple M2 Max, 8 performance and 4 efficiency cores)

All columns, median of 9 runs; the nested rows were measured after the level changes above. `threads=8` decodes the columns of a row group, and the leaf columns of nested columns, as tasks of a `std.Io.Threaded` with 7 pool threads. `ahead=1` (`BatchOptions.read_ahead`) also decodes each row group whole as one task and keeps 1 row group decoding ahead of the batches. Other applications used about one core during the run.

| Case | ms, 1 thread | ms, 8 threads | ms, 8 threads, ahead=1 | peak MB, 1 / 8 / 8 ahead=1 |
|---|---|---|---|---|
| flat arrow all | 174 | 76 | | 46.5 / 61.0 / |
| flat groups all | 167 | 84 | 40 | 49.1 / 49.3 / 108.2 |
| flat batches all | 177 | 89 | 36 | 8.2 / 9.3 / 75.6 |
| nested arrow all | 166 | 64 | | 92.8 / 137.6 / |
| nested groups all | 166 | 65 | 51 | — / 108.2 / 208.2 |
| nested batches all | 176 | 72 | 55 | 13.9 / 15.5 / 154.2 |

Without read-ahead, a row group's columns finish when its slowest column finishes, and the batch is assembled on the calling thread. With read-ahead, the next row group decodes during that time, and the task that decodes a row group also assembles the batches that lie inside it; only batches that take rows from two row groups are assembled on the calling thread. The ahead=1 column was measured after that change (before: flat 44 and 45 ms, nested 57 and 64 ms). The nested file has 2 row groups, so at most 2 overlap; its groups case took 35 to 51 ms between runs. Reading 2 or 3 row groups ahead of the flat file took 44 to 55 ms, with peaks of 102 to 160 MB. PyArrow with `use_threads=True`, measured earlier on the same machine: `read_row_group` 111 ms (flat) and 80 ms (nested); `read_table` 69.5 ms and 43 ms.

The row path writes a `Value` per entry and a `Row` per row. The values of a read are in one block of storage shared by its rows, and string values reference the decoded byte buffers.

Arrow path reading all columns, first measurement → now: flat file 750 → 177 ms (first measured at commit b64724f, before the column-read changes); nested file 479 → 278 ms (first measured at commit 2eee218, when list, struct and map assembly still built a `Value` per entry). Row path reading all columns: flat file 996 → 332 ms, nested file 1,167 → 333 ms (before at commit 32643e1). The commit messages on the branch record each change's effect. PyArrow peak for nested `point` was 62.7 MB in earlier versions of this file, which carried over the `ints` case's peak in the same process; the script now runs each case in its own process.

## Snappy decompression (2026-11-12, Apple M2 Max)

`bench-snappy` decompresses raw snappy blocks with the pure-Zig decoder and with the C library, and checks the output against the uncompressed input. Each run allocates the output buffer, as the reader does.

```bash
cd test-files-arrow
uv run python -I ../zig-parquet/bench/column-read/scripts/snappy_inputs.py /tmp/snappy_inputs   # PyArrow-compressed inputs
cd ../zig-parquet/bench/column-read
zig build snappy -- /tmp/snappy_inputs 30
```

GB/s of output, median of 30 runs:

| Input | Ratio | Zig | C library |
|---|---|---|---|
| int64 ids | 2.00 | 2.4 | 1.7 |
| int64 timestamps | 1.10 | 2.3 | 2.0 |
| random doubles | 1.00 | 13.0 | 13.1 |
| strings, 1,000 distinct | 4.85 | 4.6 | 4.6 |
| JSON-like text | 4.34 | 2.9 | 4.0 |

The decoder handles most of a block in a loop that needs no checks against the ends of the input and output: it runs while 21 input bytes (the longest element header and a 16-byte literal block) and 64 output bytes (the block writes of a copy) remain. It branches on the element type. A version that looked the element up in a table was slower on every input (timestamps 0.9 GB/s): each element's input position then depended on a load, so elements were decoded one after another. JSON-like text has 922,000 elements of 11.7 bytes on average, mostly copies of 16 to 19 bytes; the C library decodes it 1.4 times as fast.

## C stream (2026-11-12, Apple M2 Max)

The `cgroups` and `cbatches` paths read through the C API (`zp_reader_open_file`, `zp_reader_get_stream_with_options`), one batch per row group and 65,536-row batches; `threads=` and `ahead=` set the stream's `threads` and `read_ahead`. The C API allocates with its own allocator, so the allocation columns are empty for them. All columns, median of 9 runs, ms:

| File | groups | cgroups | batches | cbatches |
|---|---|---|---|---|
| flat, C handles on `page_allocator` | 159 | 197 | 150 | 194 |
| flat, C handles on `smp_allocator` | 156 | 160 | 153 | 156 |
| nested, `page_allocator` | 154 | 159 | 156 | 167 |
| nested, `smp_allocator` | 160 | 158 | 167 | 166 |

`page_allocator` maps memory from the OS for each allocation, and the flat file's reads make 17,000 to 25,000 of them. The C handles now use `smp_allocator` in multi-threaded builds.
