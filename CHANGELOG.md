# Changelog

All notable changes to this project will be documented in this file.

## [0.3.0] - 2026-11-12

### Breaking changes
- **Zig 0.17 is required** (`minimum_zig_version = "0.17.0"`; CI uses 0.17.1). All file operations take an `io: std.Io` parameter.
- `readRowGroupAsArrow` is removed. Use `reader.readColumns(rg, options)` on a `DynamicReader`. The returned schema now describes only the columns read, in the order of `.columns`.
- Decimals use Arrow's decimal layout on read and write: decimal128 (`d:p,s`, 16 bytes) up to precision 38 and decimal256 (`d:p,s,256`) up to 76. Before, INT32 and INT64 decimals were exported with 4- or 8-byte values under the decimal128 format; PyArrow read 1.00 as 316912650112397582603894390785.00.
- Reads refuse a page whose decompressed size exceeds 128 MiB and a dictionary over 64 MiB with `error.AllocationLimitExceeded`. Raise the limits with `.limits` in `RowGroupOptions`, `BatchOptions` or `DynamicReaderOptions`, or `max_page_size` in `zp_stream_options`.
- Changing `write_page_index` after the first column write (`Writer`) or after `begin()` (`DynamicWriter`) returns `error.InvalidState`.
- In list, struct and map columns, writing `null` where the schema requires a value returns `NullInRequiredColumn`, and a value of another type than the column's returns `TypeMismatch`. Before, both were written with levels that did not match the values.
- `ColumnDef.fromSchemaNode` is removed; use `ColumnDef.fromNode`.
- `leaf_decoder.Leaf` stores definition and repetition levels as `u8` (`leaf_decoder.Level`).

### Added
- **Page index.** Read and write OffsetIndex and ColumnIndex (`write_page_index` on `Writer` and `DynamicWriter`). `DynamicReader.readRowsFiltered` reads only the pages whose min/max can match a `ColumnFilter`, for flat and nested columns. `pqi page-index` prints a file's page index.
- **Arrow batches.** `reader.columnBatches(options)` returns an `ArrowBatchReader` that reads a fixed number of rows per batch, across row groups, and reads pages as batches need them.
- **Parallel decoding.** `RowGroupOptions`, `BatchOptions` and `DynamicReaderOptions` take an optional `io: std.Io`; with `std.Io.Threaded`, columns and nested leaves decode on several threads. `BatchOptions.read_ahead = k` decodes k row groups ahead of the batches. Example: `examples/basic/src/06_threaded_read.zig`.
- `BatchOptions.large_offsets` reads byte arrays as Arrow `large_string`/`large_binary` (i64 offsets).
- `exportProjectedSchemaAsArrow` returns the Arrow schema of selected columns without reading data.
- `AllocationLimits` is exported and configurable per read.
- C and WASI: `zp_reader_get_stream_with_options` takes a `zp_stream_options` struct (columns, row groups, rows per batch, `ZP_STREAM_LARGE_OFFSETS`, threads, read-ahead, `max_page_size`). The struct starts with its size, so later fields can be added. The freestanding WASM build has `zp_reader_stream_init_with_options`, `zp_arrow_array_release` and `zp_arrow_schema_release`.
- `max_page_size` on the typed `Writer`; nested columns are split into pages that start at a row.
- `ColumnDef.map_value_logical_type`.
- Root `Makefile` (`make test`, `make test-lib FILTER=...`, `make examples`, `make wasm`, `make validate-wild`).

### Changed
- Column chunks decode directly into Arrow-layout buffers through one decoder (`leaf_decoder`) for the Arrow and row APIs; row values reference those buffers instead of copying each byte array. Column benchmark on an M2 Max, 10 million rows: flat batches 149–153 ms (PyArrow 183), nested batches about 160 ms (PyArrow 164); 8 threads with read-ahead: flat 36–40 ms, nested 51–55 ms.
- The C API allocates its handles with `std.heap.smp_allocator`.
- Snappy decoding copies literals and back-references in blocks.

### Fixed
- Arrow write honours `ArrowArray.offset`; a sliced PyArrow batch was written with other rows.
- `importSchemaFromArrow` copies every name; nested names pointed into the caller's schema until close.
- A list with required elements, or a required struct field, was written with fixed definition levels and read back as nulls or other values; PyArrow could not open such files.
- A list of structs was written with the struct's field count on the repeated group; PyArrow reported "Malformed schema".
- Imported Arrow maps keep their key and value types; string keys were written as binary.
- A nested column whose values are all null keeps its schema type; it was written as INT32.
- The gzip encoder produced wrong output for inputs over 32 KiB and wrote past a stack buffer above 64 KiB.
- `zp_reader_close` with a stream open freed the reader under the stream, in the C, WASI and freestanding APIs. Readers now stay open until their last stream is released.
- The `bytes_between` page filter kept pages whose min was above the range.

### Security
Malformed files and Arrow input now return errors where they panicked, leaked or read outside buffers. A new test reads 3,612 damaged copies of the test files (15,997 reads) and fails on a panic or a leak. It runs in `zig build test`.
- Reads: DELTA_BINARY_PACKED, DELTA_BYTE_ARRAY and PLAIN byte-array length overflows; Thrift recursion depth, invalid types and collection sizes; schemas with typeless leaves or deep nesting; page indexes with overlapping ranges; footers that leaked on error.
- Allocations: column chunks, page buffers and page index blobs must lie within the file; at most 64Mi entries per page; DELTA_BYTE_ARRAY output at most 256 MiB per page; string reservations at most 16 times the chunk's bytes.
- Arrow input from C callers is validated before any buffer is read (lengths, offsets, buffer and child counts). Buffers are read without assuming alignment, which the Arrow C Data Interface does not require.

## [0.2.0] - 2026-04-07

### Changed
- **Pure Zig compression is now the default.** All five codecs (zstd, gzip, snappy, lz4, brotli) use pure Zig implementations out of the box. C/C++ backends are opt-in via `-Dcodecs=c-only` or individual `c-<codec>` tokens.
- `-Dcodecs=` token rename: Zig codecs are now plain `zstd`, `snappy`, `gzip`, `lz4`, `brotli`; C codecs are `c-zstd`, `c-snappy`, `c-gzip`, `c-lz4`, `c-brotli`
- Removed `-Dprefer-zig` build flag — Zig always wins when compiled in
- Dissolved `compress.experimental` namespace — Zig codec modules are now top-level
- Renamed `stable` codec preset to `c-only`
- Renamed codec source files: `zig_*.zig` → `*.zig`, `*.zig` (C) → `c_*.zig`

### Fixed
- All five Zig decompressors now treat `uncompressed_size` as a capacity hint only; actual decompressed bytes are returned regardless of hint value. Fixes decompression of files with bogus metadata hints (e.g. `large_string_map.brotli.parquet`)
- Brotli decompressor converted to `ArrayListUnmanaged` output — no longer pre-allocates to hint size
- Fixed pre-existing double-free in lz4 decompressor (errdefer + explicit free)
- Fixed `large_file` example: column count check compared physical schema leaves against logical columns (incorrect for nested types)

### Added
- Pure Zig LZ4 raw block codec with cross-implementation testing
- Pure Zig Brotli codec: full RFC 7932 decompressor + quality-0 compressor, with cross-implementation testing
- SortingColumn support: parse, serialize, and expose in writer API
- C→Zig direction added to all five codec cross-implementation round-trip tests
- Oversized hint test added to all five Zig decompressors

## [0.1.6] - 2026-04-04

### Added
- Experimental pure Zig gzip codec (`zig-gzip`) with full deflate encoder and cross-implementation testing
- Level-9 deflate encoder with LZ77 compression, Huffman tree building, and RLE tree descriptor encoding
- Concatenated gzip member support in the zig_gzip decompressor
- `-Dprefer-zig` build flag to select Zig codec implementations over C when both are available
- Cross-impl tests for concatenated gzip streams (C↔Zig interoperability)

### Changed
- Codec presets split: `all` includes experimental Zig implementations; `stable` for production use

## [0.1.5] - 2026-04-02

### Added
- Dual codec system: stable C/C++ implementations alongside experimental pure Zig compressors
- Experimental pure Zig zstd compressor (level 1, raw literals, predefined FSE tables)
- Experimental pure Zig snappy compressor and decompressor (full block format)
- `-Dcodecs=zig-zstd`, `-Dcodecs=zig-snappy`, `-Dcodecs=zig-only` build options
- `compress.experimental` API namespace for Zig codec implementations
- Cross-implementation test suites validating C↔Zig interoperability for zstd and snappy
- Write-path targeted tests for Zig zstd compressor (literals header boundaries, sequence count encoding, large LL/ML codes, large offsets, mixed block types, rep[0] gaps)
- COMPRESSION.md with codec details, build sizes, and API documentation

### Changed
- Stable C/C++ codecs remain default; dispatch falls back to Zig when stable is unavailable
- `supports_zstd` / `supports_snappy` build flags now cover both implementations

## [0.1.4] - 2026-03-28

### Changed
- Replace `-Dno_compression` with per-codec `-Dcodecs=` build option — enable any combination of zstd, snappy, gzip, lz4, brotli (default: all)

### Fixed
- Fix key-only map regression: fall back to list (Arrow C++ approach)

### Added
- Add validate-wild step to CI pipeline

## [0.1.3] - 2026-03-27

### Fixed
- Fix varint panic on malicious Thrift data in compact reader
- Fix errdefer undefined behavior and add checked arithmetic in core encodings
- Fix critical findings across multiple code review rounds (arrow_batch, column_decoder, column_writer, dynamic_reader, dynamic_writer)
- Remove dead branch in DictHashContext hash function

### Added
- Arrow write round-trip tests for arrow_batch hardening

## [0.1.2] - 2026-03-26

### Added
- REQUIRED column support in DynamicWriter via `TypeInfo.asRequired()` — non-nullable schema, write-time validation, no definition levels on disk

### Fixed
- Fix release script to update `zig fetch` URL in README for new versions

### Changed
- Document interop/ directory and missing subdirectories in test-files-arrow README

## [0.1.1] - 2026-03-25

### Added
- Test file pre-check in `zig build test` — fails early with a clear message instead of 52 FileNotFound errors

### Fixed
- Deduplicate `safeTypeLength` between column_decoder and dynamic_reader
- Fix incorrect file paths in AGENTS.md
- Document all pqi commands in README (count, column, rowgroups, size)
- Fix README feature claims: complete logical types list, clarify DataPage V2 and hardening

### Changed
- Reorder README features to lead with core capabilities
- Clean up CONTRIBUTING.md section ordering

## [0.1.0] - 2026-03-23

Initial release.

### Added
- Full read/write support for all Parquet physical types
- All standard encodings: PLAIN, RLE, DICTIONARY, DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY, BYTE_STREAM_SPLIT
- Nested types: LIST, MAP, STRUCT with arbitrary nesting depth
- Logical types: STRING, DATE, TIME, TIMESTAMP, DECIMAL, UUID, INT, FLOAT16, ENUM, JSON, BSON, INTERVAL, GEOMETRY, GEOGRAPHY
- Compression: zstd, gzip, snappy, lz4_raw, brotli
- DataPage V1 and V2 read support; write uses V1
- Runtime DynamicWriter and DynamicReader for schema-agnostic row I/O
- Column statistics (min/max/null_count)
- Page-level CRC32 checksums (written by default, validated on read)
- Key-value metadata read/write
- Column projection on DynamicReader
- Streaming RowIterator with C ABI support
- C ABI with Arrow C Data Interface (ArrowSchema, ArrowArray, ArrowArrayStream)
- WASM targets: wasm32-wasi (with compression) and wasm32-freestanding (no compression)
- `pqi` CLI: schema, head, cat, count, stats, rowgroups, size, column, validate
- Buffer, file, and callback I/O transports
- Hardening: safe casting, bounds checking, no `@intCast` on external data
- 219/219 pass rate on supported Apache parquet-testing files

[0.3.0]: https://github.com/akeating/zig-parquet/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/akeating/zig-parquet/compare/v0.1.6...v0.2.0
[0.1.7]: https://github.com/akeating/zig-parquet/compare/v0.1.6...v0.1.7
[0.1.6]: https://github.com/akeating/zig-parquet/compare/v0.1.5...v0.1.6
[0.1.5]: https://github.com/akeating/zig-parquet/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/akeating/zig-parquet/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/akeating/zig-parquet/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/akeating/zig-parquet/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/akeating/zig-parquet/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/akeating/zig-parquet/releases/tag/v0.1.0
