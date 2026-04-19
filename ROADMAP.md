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
| Page index (nested columns) | Emitted for flat columns; nested-leaf emission is a follow-up |
| Freestanding WASM + C codecs | C/C++ codecs require libc; freestanding WASM must use `-Dcodecs=zig-only` or `-Dcodecs=none` |

## Future

Possible development directions, roughly ordered by likely impact. No timeline commitments.

- **Bloom filters** — Write Bloom filter pages; read and apply for row group skipping. Useful for high-cardinality string columns (e.g., UUIDs, URLs). Plugs into the existing `ColumnFilter` / `RowRanges` contract used by page-index filtering — a bloom producer just returns `empty()` or `full(num_rows)` depending on the probe result, and intersects with other filters automatically.
- **Predicate expression parser** — Page-level predicate pushdown already works via `readRowsFiltered(rg, &filters)`; what's missing is a parser that turns a SQL-like expression (`col0 BETWEEN 10 AND 20 AND col1 = "foo"`) into a list of `ColumnFilter`s. The I/O path doesn't change.
- **Brotli LZ77 encoder** — Upgrade the pure Zig Brotli compressor from quality-0 uncompressed meta-blocks to a quality-1 LZ77 pass with hash-chain matching. Closes the compression ratio gap vs. the C quality-11 backend on Parquet data. Self-contained within `zig_brotli.zig`.
- **DATA_PAGE_V2 write** — Optional; some engines prefer V2 for its split decompression model.
- **Page index for nested leaves** — The write path currently emits page indexes only for flat columns. Nested leaves need per-leaf offset/first-row-index tracking through the map/list/struct writers.
- **VARIANT type** — Semi-structured data (Parquet spec addition). Currently reads as its underlying physical type.
- **Column encryption** — Parquet Modular Encryption. Complex spec with limited ecosystem support outside Java.
- **Parallel column decoding** — Decode multiple columns concurrently within a row group. Zig's async model could make this natural.
