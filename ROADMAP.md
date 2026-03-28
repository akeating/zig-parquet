# Roadmap

## Current Status

Full read/write support for the Apache Parquet format:

- **Reading:** All physical types, all standard encodings (PLAIN, RLE, DICTIONARY, DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY, BYTE_STREAM_SPLIT), DataPage V1 and V2, column projection, streaming row iteration
- **Writing:** All physical types, all standard encodings, dictionary encoding with cardinality-based fallback, multi-page columns, column statistics (min/max/null_count), CRC32 page checksums
- **Logical types:** STRING, DATE, TIME, TIMESTAMP (millis/micros/nanos), DECIMAL, UUID, INT annotations, FLOAT16, ENUM, JSON, BSON, INTERVAL, GEOMETRY, GEOGRAPHY
- **Nested types:** LIST, MAP, STRUCT with arbitrary nesting depth
- **Compression:** ZSTD, GZIP, Snappy, LZ4_RAW, Brotli (read and write)
- **APIs:** Zig native, C ABI, WASM (wasm32-wasi and wasm32-freestanding)
- **Hardening:** Zero `@intCast` on external data, bounds-checked slicing, safe casting throughout

## Limitations

Things that are explicitly **not supported** and will return errors:

| Limitation | Notes |
|---|---|
| DATA_PAGE_V2 write | Read supported; write always uses V1 (universally compatible) |
| LZ4 (non-raw) | Hadoop-specific framing format; not the same as LZ4_RAW |
| LZO compression | No implementation |
| VARIANT logical type | Complex binary encoding; currrently returns physical types |
| Column encryption | Parquet Modular Encryption; only Java/Python have implementations |
| Bloom filters | Read or write |
| Page index | Read or write |
| Predicate pushdown | Statistics-based row group skipping exists; page-level filtering does not |
| Freestanding WASM + compression | Compression codecs require libc; freestanding builds use `-Dcodecs=none` |

## Future

Possible development directions, roughly ordered by likely impact. No timeline commitments.

- **Bloom filters** — Write Bloom filter pages; read and apply for row group skipping. Useful for high-cardinality string columns (e.g., UUIDs, URLs).
- **Page index** — Write column/offset indexes; use them for fine-grained page skipping. Biggest win for large files with sorted columns.
- **Native Zig compression** — Replace C library dependencies (zstd, zlib, snappy, lz4, brotli) with pure Zig implementations covering only the subset each codec needs for Parquet. Eliminates the C/C++ compiler requirement, shrinks binary size, and unblocks freestanding WASM + compression.
- **DATA_PAGE_V2 write** — Optional; some engines prefer V2 for its split decompression model.
- **Predicate pushdown** — Push filter expressions down to page level using page index + statistics. Requires page index support first.
- **VARIANT type** — Semi-structured data (Parquet spec addition). Currently reads as its underlying physical type.
- **Column encryption** — Parquet Modular Encryption. Complex spec with limited ecosystem support outside Java.
- **Parallel column decoding** — Decode multiple columns concurrently within a row group. Zig's async model could make this natural.
