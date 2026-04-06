# Roadmap

## Known Limitations

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
| Zig codec compression levels | zig-zstd is level 1 only, zig-brotli is quality 0 only; decompression handles all levels. C backends support full level range |
| Freestanding WASM + all codecs | C/C++ codecs require libc; freestanding WASM can use pure Zig codecs via `-Dcodecs=zig-only` or `-Dcodecs=none` |

## Future

Possible development directions, roughly ordered by likely impact. No timeline commitments.

- **Bloom filters** — Write Bloom filter pages; read and apply for row group skipping. Useful for high-cardinality string columns (e.g., UUIDs, URLs).
- **Page index** — Write column/offset indexes; use them for fine-grained page skipping. Biggest win for large files with sorted columns.
- **Native Zig compression (in progress)** — Pure Zig implementations of all compression codecs (zig-zstd, zig-snappy, zig-gzip, zig-lz4, zig-brotli). Enables no-C/C++ builds, reduces binary size. Experimental versions available via `-Dcodecs=zig-only` or individually. Cross-implementation tests validate interoperability. When mature, Zig implementations graduate to replace C versions as default.
- **DATA_PAGE_V2 write** — Optional; some engines prefer V2 for its split decompression model.
- **Predicate pushdown** — Push filter expressions down to page level using page index + statistics. Requires page index support first.
- **VARIANT type** — Semi-structured data (Parquet spec addition). Currently reads as its underlying physical type.
- **Column encryption** — Parquet Modular Encryption. Complex spec with limited ecosystem support outside Java.
- **Parallel column decoding** — Decode multiple columns concurrently within a row group. Zig's async model could make this natural.
