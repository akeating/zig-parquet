# Compression

zig-parquet supports all major Parquet compression codecs. Codecs are individually selectable at build time, and disabled codecs are not compiled into the binary.

## Codecs

| Codec | Implementation | Notes |
|-------|---------------|-------|
| zstd | C libzstd 1.5.7 | Recommended default; configurable compression level (1-22) |
| gzip | C zlib 1.3.1 | Wide compatibility |
| snappy | C++ snappy 1.2.2 | Fast, moderate ratio |
| lz4 | C lz4 1.10.0 | Very fast |
| brotli | C brotli 1.2.0 | High ratio, largest binary cost |
| zig-zstd | Pure Zig (experimental) | No C dependency; level-1 compressor + stdlib decompressor |

## Build Sizes

Static library (`libparquet.a`) sizes with `ReleaseSmall`, measured on macOS arm64.

| `-Dcodecs=` | ReleaseSmall | Delta vs none | C/C++ required |
|-------------|-------------|---------------|----------------|
| `none` | 806 KB | -- | No |
| `zig-only` | 910 KB | +104 KB | No |
| `snappy` | 876 KB | +70 KB | C++ |
| `lz4` | 896 KB | +90 KB | C |
| `gzip` | 897 KB | +91 KB | C |
| `zstd` | 1,323 KB | +517 KB | C |
| `brotli` | 1,640 KB | +834 KB | C |
| `all` (default) | 2,309 KB | +1,503 KB | C, C++ |

### WASM (wasm32-wasi, ReleaseSmall)

| `-Dcodecs=` | Raw `.wasm` | Brotli compressed | Delta vs none |
|-------------|------------|-------------------|---------------|
| `none` | 620 KB | 116 KB | -- |
| `zig-only` | 695 KB | 134 KB | +18 KB |
| `snappy` | 671 KB | 130 KB | +14 KB |
| `lz4` | 673 KB | 126 KB | +10 KB |
| `gzip` | 681 KB | 134 KB | +18 KB |
| `zstd` | 1,032 KB | 196 KB | +80 KB |
| `brotli` | 1,348 KB | 329 KB | +213 KB |
| `all` (default) | 1,864 KB | 446 KB | +330 KB |

The `wasm32-freestanding` target hardcodes all codecs to disabled and produces a 519 KB / 103 KB (brotli) binary.

## Build Options

```bash
zig build                           # all codecs (default: C libs)
zig build -Dcodecs=none             # no compression
zig build -Dcodecs=zstd,snappy      # only zstd and snappy
zig build -Dcodecs=zstd             # C libzstd
zig build -Dcodecs=zig-zstd         # experimental pure Zig zstd (no C deps)
zig build -Dcodecs=zig-only         # all available pure Zig codecs
zig build -Dcodecs=zstd,zig-zstd    # both implementations (enables cross-impl tests)
```

Disabled codecs return `UnsupportedCompression` at runtime. C dependencies are only fetched for enabled C codecs.

## API

### Dispatch

The internal `compress` and `decompress` functions in `mod.zig` select an implementation automatically. When both C and Zig implementations of a codec are enabled, the stable (C) implementation is preferred. The Zig implementation is used as a fallback when the C version is not compiled in.

### Direct access

Stable implementations are at the top level, experimental ones behind the `experimental` namespace:

```zig
const compress = @import("zig-parquet").compress;

// Stable (C)
const c_compressed = try compress.zstd.compress(allocator, data);
const c_level5 = try compress.zstd.compressWithLevel(allocator, data, 5);

// Experimental (Zig)
const zig_compressed = try compress.experimental.zig_zstd.compress(allocator, data);
```

### Build option flags

| Flag | Meaning |
|------|---------|
| `enable_zstd` | C libzstd compiled in |
| `enable_zig_zstd` | Pure Zig zstd compiled in |
| `supports_zstd` | Either zstd implementation available |
| `enable_snappy` | Snappy compiled in |
| `enable_gzip` | Gzip compiled in |
| `enable_lz4` | LZ4 compiled in |
| `enable_brotli` | Brotli compiled in |

Use `supports_zstd` in test guards to cover both implementations:

```zig
test "my zstd test" {
    if (!build_options.supports_zstd) return;
    // ...
}
```

## Experimental: Pure Zig zstd

The `zig-zstd` codec is a pure Zig implementation with no C dependencies.

**Compressor** -- deliberately targets zstd level 1 only:
- Greedy LZ matching with hash table lookup
- Repeated offset tracking (rep[0], rep[1], rep[2])
- Raw literals (no Huffman encoding)
- Predefined FSE tables for sequence encoding
- Multi-block support (blocks up to 128KB)

**Decompressor** -- uses `std.compress.zstd` from Zig's standard library. Handles all compression levels.

### Graduation path

When the Zig implementation matures, its code replaces the stable version (`zig_zstd.zig` contents move to `zstd.zig`). The `experimental.zig_zstd` entry is removed, and consumers using `compress.zstd` get the Zig implementation without code changes.

### Testing

Cross-implementation tests validate interoperability between C libzstd and the Zig compressor. Build with `-Dcodecs=zstd,zig-zstd` to enable them.

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
