# Changelog

All notable changes to this project will be documented in this file.

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

[0.1.6]: https://github.com/akeating/zig-parquet/compare/v0.1.5...v0.1.6
[0.1.5]: https://github.com/akeating/zig-parquet/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/akeating/zig-parquet/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/akeating/zig-parquet/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/akeating/zig-parquet/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/akeating/zig-parquet/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/akeating/zig-parquet/releases/tag/v0.1.0
