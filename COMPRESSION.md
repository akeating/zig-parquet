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
| zig-gzip | Pure Zig (experimental) | No C dependency; level-9 deflate compressor + stdlib decompressor |
| zig-snappy | Pure Zig (experimental) | No C/C++ dependency; full Snappy block format |

## Build Sizes

Static library (`libparquet.a`) sizes with `ReleaseSmall`, measured on macOS arm64.

| `-Dcodecs=` | ReleaseSmall | Delta vs none | C/C++ required |
|-------------|-------------|---------------|----------------|
| `none` | 806 KB | -- | No |
| `zig-snappy` | 834 KB | +28 KB | No |
| `zig-gzip` | 870 KB | +64 KB | No |
| `zig-zstd` | 910 KB | +104 KB | No |
| `zig-only` | 952 KB | +146 KB | No |
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
| `zig-snappy` | 633 KB | 117 KB | +1 KB |
| `zig-gzip` | 658 KB | 125 KB | +9 KB |
| `zig-zstd` | 695 KB | 134 KB | +18 KB |
| `zig-only` | 722 KB | 138 KB | +22 KB |
| `snappy` | 671 KB | 130 KB | +14 KB |
| `lz4` | 673 KB | 126 KB | +10 KB |
| `gzip` | 681 KB | 134 KB | +18 KB |
| `zstd` | 1,032 KB | 196 KB | +80 KB |
| `brotli` | 1,348 KB | 329 KB | +213 KB |
| `all` (default) | 1,864 KB | 446 KB | +330 KB |

The `wasm32-freestanding` target hardcodes all codecs to disabled and produces a 519 KB / 103 KB (brotli) binary.

## Build Options

Presets:

```bash
zig build                           # all codecs (C + Zig, default for library)
zig build -Dcodecs=stable           # stable C/C++ only (no experimental Zig)
zig build -Dcodecs=none             # no compression
zig build -Dcodecs=zig-only         # all available pure Zig codecs
```

Custom combinations:

```bash
zig build -Dcodecs=zstd,snappy      # only zstd and snappy (C versions)
zig build -Dcodecs=zstd             # C libzstd only
zig build -Dcodecs=zig-zstd         # pure Zig zstd only (no C deps)
zig build -Dcodecs=zig-snappy       # pure Zig snappy only (no C++ deps)
zig build -Dcodecs=zig-gzip          # pure Zig gzip only (no C deps)
zig build -Dcodecs=zstd,zig-zstd    # both zstd implementations (enables cross-impl tests)
zig build -Dcodecs=snappy,zig-snappy # both snappy implementations (enables cross-impl tests)
zig build -Dcodecs=gzip,zig-gzip    # both gzip implementations (enables cross-impl tests)
```

Disabled codecs return `UnsupportedCompression` at runtime. C dependencies are only fetched for enabled C codecs.

### Implementation preference

When both C and Zig implementations of a codec are enabled (e.g. `-Dcodecs=all`), the C version is used by default. Pass `-Dprefer-zig` to use the Zig implementation instead, with C as fallback for codecs without a Zig version (lz4, brotli):

```bash
zig build -Dcodecs=all -Dprefer-zig   # use Zig for zstd/gzip/snappy, C for lz4/brotli
```

This is a compile-time flag resolved at build time with no runtime overhead.

### Preset Definitions

| Preset | Zstd | Snappy | Gzip | LZ4 | Brotli | Zig Zstd | Zig Snappy | Zig Gzip | Use Case |
|--------|------|--------|------|-----|--------|----------|------------|----------|----------|
| `all` | ✓ C | ✓ C | ✓ C | ✓ C | ✓ C | ✓ Zig | ✓ Zig | ✓ Zig | Maximum codec coverage (library testing) |
| `stable` | ✓ C | ✓ C | ✓ C | ✓ C | ✓ C | ✗ | ✗ | ✗ | Production use (pqi CLI, proven codecs only) |
| `zig-only` | ✗ | ✗ | ✗ | ✗ | ✗ | ✓ Zig | ✓ Zig | ✓ Zig | No C/C++ dependencies |
| `none` | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | Minimum binary |

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
const zig_snappy_out = try compress.experimental.zig_snappy.compress(allocator, data);
const zig_gzip_out = try compress.experimental.zig_gzip.compress(allocator, data);
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
| `enable_lz4` | LZ4 compiled in |
| `enable_brotli` | Brotli compiled in |

Use `supports_zstd` in test guards to cover both implementations:

```zig
test "my zstd test" {
    if (!build_options.supports_zstd) return;
    // ...
}
```

## Experimental Codecs

### Pure Zig snappy

The `zig-snappy` codec is a pure Zig implementation of the Snappy block format with no C/C++ dependencies.

- Greedy LZ matching with hash table lookup
- Copy (match) and literal element encoding per Snappy spec
- 64KB block size matching the reference compressor

Graduation follows the same path as zstd: when mature, the contents of `zig_snappy.zig` replace `snappy.zig`.

Cross-implementation tests validate interoperability between C++ snappy and the Zig implementation. Build with `-Dcodecs=snappy,zig-snappy` to enable them.

### Pure Zig gzip

The `zig-gzip` codec is a pure Zig implementation of gzip (RFC 1952) with deflate compression (RFC 1951) and no C dependencies.

**Compressor** -- targets level-9 quality:
- Dynamic Huffman trees with canonical code assignment
- Lazy LZ77 matching with hash chains (max_chain=4096, good_length=32)
- RLE-encoded tree descriptors per RFC 1951

**Decompressor** -- uses `std.compress.flate` from Zig's standard library. Handles all compression levels.

Cross-implementation tests validate interoperability between C zlib and the Zig implementation. Build with `-Dcodecs=gzip,zig-gzip` to enable them.

### Pure Zig zstd

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
