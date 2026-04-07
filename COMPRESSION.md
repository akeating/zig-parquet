# Compression

zig-parquet supports all major Parquet compression codecs. Codecs are individually selectable at build time, and disabled codecs are not compiled into the binary.

## Codecs

| Codec | Default | C Backend | Notes |
|-------|---------|-----------|-------|
| zstd | Pure Zig | libzstd 1.5.7 | Zig: level-1 compressor + stdlib decompressor; C: levels 1–22 |
| gzip | Pure Zig | zlib 1.3.1 | Zig: level-9 deflate compressor + stdlib decompressor |
| snappy | Pure Zig | snappy 1.2.2 (C++) | Full Snappy block format |
| lz4 | Pure Zig | lz4 1.10.0 | Full LZ4 raw block format |
| brotli | Pure Zig | brotli 1.2.0 | Zig: quality-0 compressor + full RFC 7932 decompressor; C: quality 11 |

## Build Sizes

Static library (`libparquet.a`) sizes with `ReleaseSmall`, measured on macOS arm64.

| `-Dcodecs=` | ReleaseSmall | Delta vs none | C/C++ required |
|-------------|-------------|---------------|----------------|
| `none` | 807 KB | -- | No |
| `lz4` | 833 KB | +26 KB | No |
| `snappy` | 835 KB | +28 KB | No |
| `gzip` | 872 KB | +65 KB | No |
| `zstd` | 910 KB | +103 KB | No |
| `brotli` | 985 KB | +178 KB | No |
| `zig-only` | 1,110 KB | +303 KB | No |
| `c-snappy` | 877 KB | +70 KB | C++ |
| `c-lz4` | 896 KB | +89 KB | C |
| `c-gzip` | 898 KB | +91 KB | C |
| `c-zstd` | 1,324 KB | +517 KB | C |
| `c-brotli` | 1,641 KB | +834 KB | C |
| `all` (default) | 2,310 KB | +1,503 KB | C, C++ |

### WASM (wasm32-wasi, ReleaseSmall)

| `-Dcodecs=` | Raw `.wasm` | Brotli compressed | Delta vs none |
|-------------|------------|-------------------|---------------|
| `none` | 621 KB | 116 KB | -- |
| `lz4` | 633 KB | 117 KB | +1 KB |
| `snappy` | 634 KB | 117 KB | +1 KB |
| `gzip` | 659 KB | 125 KB | +9 KB |
| `zstd` | 695 KB | 134 KB | +18 KB |
| `brotli` | 792 KB | 156 KB | +40 KB |
| `zig-only` | 886 KB | 184 KB | +68 KB |
| `c-snappy` | 671 KB | 130 KB | +14 KB |
| `c-lz4` | 673 KB | 126 KB | +10 KB |
| `c-gzip` | 681 KB | 134 KB | +18 KB |
| `c-zstd` | 1,032 KB | 196 KB | +80 KB |
| `c-brotli` | 1,348 KB | 329 KB | +213 KB |
| `all` (default) | 1,864 KB | 446 KB | +330 KB |

The `wasm32-freestanding` target hardcodes all codecs to disabled and produces a 519 KB / 103 KB (brotli) binary.

## Build Options

Presets:

```bash
zig build                           # all codecs, Zig implementations used by default
zig build -Dcodecs=zig-only         # pure Zig codecs only (no C/C++ deps at all)
zig build -Dcodecs=c-only           # C/C++ codecs only (opt-in)
zig build -Dcodecs=none             # no compression
```

Custom combinations:

```bash
zig build -Dcodecs=zstd         # pure Zig zstd only
zig build -Dcodecs=snappy       # pure Zig snappy only
zig build -Dcodecs=gzip         # pure Zig gzip only
zig build -Dcodecs=lz4          # pure Zig LZ4 only
zig build -Dcodecs=brotli       # pure Zig brotli only
zig build -Dcodecs=c-zstd             # C libzstd only
zig build -Dcodecs=c-zstd,zstd    # both zstd implementations (cross-impl testing)
zig build -Dcodecs=c-snappy,snappy # both snappy implementations (cross-impl testing)
zig build -Dcodecs=c-gzip,gzip    # both gzip implementations (cross-impl testing)
zig build -Dcodecs=c-lz4,lz4      # both LZ4 implementations (cross-impl testing)
zig build -Dcodecs=c-brotli,brotli # both brotli implementations (cross-impl testing)
```

Disabled codecs return `UnsupportedCompression` at runtime. C dependencies are only fetched for enabled C codecs.

### Preset Definitions

| Preset | Zstd | Snappy | Gzip | LZ4 | Brotli | Zig Zstd | Zig Snappy | Zig Gzip | Zig LZ4 | Zig Brotli | Use Case |
|--------|------|--------|------|-----|--------|----------|------------|----------|---------|------------|----------|
| `all` | ✓ C | ✓ C | ✓ C | ✓ C | ✓ C | ✓ Zig | ✓ Zig | ✓ Zig | ✓ Zig | ✓ Zig | All implementations; Zig used by default, C available for cross-impl testing |
| `zig-only` | ✗ | ✗ | ✗ | ✗ | ✗ | ✓ Zig | ✓ Zig | ✓ Zig | ✓ Zig | ✓ Zig | Pure Zig, zero C/C++ dependencies |
| `c-only` | ✓ C | ✓ C | ✓ C | ✓ C | ✓ C | ✗ | ✗ | ✗ | ✗ | ✗ | C/C++ backends only (opt-in) |
| `none` | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | Minimum binary |

## API

### Dispatch

The internal `compress` and `decompress` functions in `mod.zig` select an implementation automatically. When both Zig and C implementations are compiled in (e.g. `-Dcodecs=all`), the Zig implementation is used. The C implementation is used only when the Zig version is not compiled in (e.g. `-Dcodecs=c-only`).

### Direct access

Both Zig and C implementations are at the top level:

```zig
const compress = @import("zig-parquet").compress;

// Pure Zig (default)
const zig_compressed = try compress.zig_zstd.compress(allocator, data);
const zig_snappy_out = try compress.zig_snappy.compress(allocator, data);
const zig_gzip_out = try compress.zig_gzip.compress(allocator, data);
const zig_lz4_out = try compress.zig_lz4.compress(allocator, data);
const zig_brotli_out = try compress.zig_brotli.compress(allocator, data);

// C backends (when compiled in via -Dcodecs=c-only or individual names)
const c_compressed = try compress.zstd.compress(allocator, data);
const c_level5 = try compress.zstd.compressWithLevel(allocator, data, 5);
```

### Build option flags

| Flag | Meaning |
|------|---------|
| `enable_zstd` | C libzstd compiled in |
| `enable_zig_zstd` | Pure Zig zstd compiled in |
| `supports_zstd` | Either zstd implementation available |
| `enable_snappy` | C++ snappy compiled in |
| `enable_zig_snappy` | Pure Zig snappy compiled in |
| `supports_snappy` | Either snappy implementation available |
| `enable_gzip` | C zlib gzip compiled in |
| `enable_zig_gzip` | Pure Zig gzip compiled in |
| `supports_gzip` | Either gzip implementation available |
| `enable_lz4` | C LZ4 compiled in |
| `enable_zig_lz4` | Pure Zig LZ4 compiled in |
| `supports_lz4` | Either LZ4 implementation available |
| `enable_brotli` | C Brotli compiled in |
| `enable_zig_brotli` | Pure Zig Brotli compiled in |
| `supports_brotli` | Either Brotli implementation available |

Use `supports_zstd` in test guards to cover both implementations:

```zig
test "my zstd test" {
    if (!build_options.supports_zstd) return;
    // ...
}
```

## Pure Zig Codec Details

### Pure Zig snappy

Pure Zig implementation of the Snappy block format with no C/C++ dependencies.

- Greedy LZ matching with hash table lookup
- Copy (match) and literal element encoding per Snappy spec
- 64KB block size matching the reference compressor

Cross-implementation tests validate interoperability between C++ snappy and the Zig implementation. Build with `-Dcodecs=c-snappy,snappy` to enable them.

### Pure Zig LZ4

Pure Zig implementation of the LZ4 raw block format with no C dependencies.

- Greedy hash-based match finder (4096-entry table, multiplicative hash)
- 64KB max match distance per LZ4 spec
- Proper end-of-block literal handling (MFLIMIT/LASTLITERALS constraints)
- Overlapping match copy support (offset < match_len)

Cross-implementation tests validate interoperability between C libLZ4 and the Zig implementation. Build with `-Dcodecs=c-lz4,lz4` to enable them.

### Pure Zig brotli

Pure Zig implementation of Brotli (RFC 7932) with no C dependencies.

**Compressor** -- targets quality 0 (one-pass):
- Uncompressed meta-block encoding (valid Brotli bitstream, no LZ77 compression)
- Multi-block support (blocks up to 64KB)

**Why quality 0:** The C backend (`-Dcodecs=c-brotli`) compresses at quality 11 (maximum). A quality-11 encoder is ~25,000 lines of C (Zopfli-style optimal parsing, binary tree hashers, block splitting, context modeling) — larger than all other Zig codecs combined. Quality 0 is a self-contained ~500-line one-pass encoder that produces valid streams any decoder can read. Compression ratios are reasonable for typical Parquet data, trailing quality 11 by 4–10 percentage points (e.g., timestamps: 50% vs 40%, dictionary indices: 30% vs 21%). Users needing maximum compression can use the C backend (`-Dcodecs=c-brotli`); the Zig backend is for environments where no-C-dependency matters more than ratio.

**Decompressor** -- full RFC 7932 decoder handling all Brotli features:
- All block types (uncompressed, compressed, metadata)
- Complex and simple prefix (Huffman) codes with two-level lookup tables
- Context modeling (4 literal context modes, distance contexts)
- Block type switching with ring buffer tracking
- Static dictionary (120KB, 121 transforms) via `@embedFile`
- Distance ring buffer with short codes, direct codes, and parametric codes

Cross-implementation tests validate interoperability between C libbrotli and the Zig implementation. Build with `-Dcodecs=c-brotli,brotli` to enable them.

### Pure Zig gzip

Pure Zig implementation of gzip (RFC 1952) with deflate compression (RFC 1951) and no C dependencies.

**Compressor** -- targets level-9 quality:
- Dynamic Huffman trees with canonical code assignment
- Lazy LZ77 matching with hash chains (max_chain=4096, good_length=32)
- RLE-encoded tree descriptors per RFC 1951

**Decompressor** -- uses `std.compress.flate` from Zig's standard library. Handles all compression levels.

Cross-implementation tests validate interoperability between C zlib and the Zig implementation. Build with `-Dcodecs=c-gzip,gzip` to enable them.

### Pure Zig zstd

Pure Zig implementation with no C dependencies.

**Compressor** -- deliberately targets zstd level 1 only:
- Greedy LZ matching with hash table lookup
- Repeated offset tracking (rep[0], rep[1], rep[2])
- Raw literals (no Huffman encoding)
- Predefined FSE tables for sequence encoding
- Multi-block support (blocks up to 128KB)

**Why level 1:** Zstd level 1 already delivers strong compression on Parquet's columnar data — repetitive values, run-length patterns, and dictionary-encoded columns compress well even with greedy matching. Level 1 is the default for Apache Arrow's C++ and Go Parquet writers (DuckDB and Spark default to level 3). Higher levels add Huffman-coded literals, optimal parsing, and longer match searches for diminishing returns on typical Parquet pages.

**Decompressor** -- uses `std.compress.zstd` from Zig's standard library. Handles all compression levels.

### Testing

Cross-implementation tests validate interoperability between C libzstd and the Zig compressor. Build with `-Dcodecs=c-zstd,zstd` to enable them.

Coverage includes:
- Bidirectional round-trips (C compress / Zig decompress and vice versa)
- C libzstd at multiple compression levels (1, 3, 6, 9, 15, 19)
- Block boundary behavior (at, around, and across BLOCK_SIZE_MAX)
- Match length extremes (MIN_MATCH through 64KB single match)
- Repeated offset chains and rotation
- Raw block fallback (incompressible data up to 512KB)
- Parquet-realistic data (timestamps, dictionary indices, null bitmaps, numeric columns)
- Frame header FCS field encoding boundaries
- Scale up to 1MB
