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
| Freestanding WASM + C codecs | C/C++ codecs require libc; freestanding WASM must use `-Dcodecs=zig-only` or `-Dcodecs=none` |

## Future

Possible development directions, roughly ordered by likely impact. No timeline commitments.

- **Bloom filters** — Write Bloom filter pages; read and apply for row group skipping. Useful for high-cardinality string columns (e.g., UUIDs, URLs).
- **Page index** — Write column/offset indexes; use them for fine-grained page skipping. Biggest win for large files with sorted columns.
- **Brotli LZ77 encoder** — Upgrade the pure Zig Brotli compressor from quality-0 uncompressed meta-blocks to a quality-1 LZ77 pass with hash-chain matching. Closes the compression ratio gap vs. the C quality-11 backend on Parquet data. Self-contained within `zig_brotli.zig`.
- **DATA_PAGE_V2 write** — Optional; some engines prefer V2 for its split decompression model.
- **Predicate pushdown** — Push filter expressions down to page level using page index + statistics. Requires page index support first.
- **VARIANT type** — Semi-structured data (Parquet spec addition). Currently reads as its underlying physical type.
- **Column encryption** — Parquet Modular Encryption. Complex spec with limited ecosystem support outside Java.
- **Parallel column decoding** — Decode multiple columns concurrently within a row group. Zig's async model could make this natural.
