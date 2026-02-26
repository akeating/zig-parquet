# Codebase Hardening

Eliminating runtime panics when processing malformed or edge-case Parquet files.

## Philosophy

Zig's `@intCast` and array indexing are intentional "promises" that values are valid. In debug builds, violations panic; in release builds, they're undefined behavior. This is appropriate for trusted data but problematic when parsing untrusted external files.

Both reading and writing handle untrusted data:

- **Reading:** External files may be corrupted, maliciously crafted, use unsupported features, or hit unhandled edge cases.
- **Writing:** User code may provide out-of-range values, overflow-prone sizes, or schema mismatches.

The goal is to **return context-aware errors instead of panicking** for any malformed input, regardless of source.

## Safe Casting Module (`src/safe.zig`)

Centralized module replacing all `@intCast` on external data:

```zig
const safe = @import("safe.zig");

// Cast to usize — returns error if negative
const size = try safe.cast(page_header.uncompressed_page_size);

// Cast to specific type — returns error if out of range
const fixed_len = try safe.castTo(u32, type_length);

// Bounds-checked slice
const data = try safe.slice(buffer, offset, length);
```

Grep-verifiable: `rg "safe.cast" src/` audits all external data casts.

### `@intCast` Elimination Status

All production code uses `safe.cast()` — zero `@intCast` remaining:

| Area | Files | `@intCast` Count |
|------|-------|-----------------|
| Reader | `reader.zig`, `column_decoder.zig`, `parquet_reader.zig`, `dynamic_reader.zig`, `row_reader.zig`, `seekable_reader.zig` | 0 |
| Writer | `writer.zig`, `column_writer.zig`, `column_write_*.zig`, `row_writer.zig` | 0 |
| Encoding | `encoding/*.zig` | 0 |
| Thrift | `thrift/*.zig` | 0 |
| Format | `format/*.zig` | 0 |
| Compress | `compress/*.zig` | 0 |
| Arrow Batch | `arrow_batch.zig` | 0 |
| C API | `api/c/*.zig` | 0 |
| WASM API | `api/wasm/*.zig` | 0 |
| Other | `types.zig`, `arrow.zig`, `schema.zig` | 0 |
| Tests | `tests/*.zig`, `arrow_batch.zig` tests | ~96 (acceptable) |

### `catch unreachable` Policy

When a value is mathematically guaranteed to fit, `catch unreachable` is acceptable but **must** include an inline comment explaining the invariant:

```zig
// GOOD: invariant explained
const bit_idx: u3 = safe.castTo(u3, i % 8) catch unreachable; // i % 8 is strictly 0-7

// GOOD: refers to earlier check
if (value.len > std.math.maxInt(i32)) return error.ValueTooLarge;
const len = safe.castTo(i32, value.len) catch unreachable; // checked against maxInt(i32) above

// BAD: no explanation
const bit_idx: u3 = safe.castTo(u3, i % 8) catch unreachable;
```

## Fixes Applied

### Bounds Checks Before Slicing/Indexing

**Boolean decoding** (`src/encoding/plain.zig`):
Check `byte_idx < data.len` before accessing bool bit. Returns `EndOfData`.

**Level decoding** (`src/reader/column_decoder.zig`):
Bounds checks before reading rep/def level length prefixes and data slices. Returns `EndOfData`.

**Fixed-length byte arrays** (`src/reader/column_decoder.zig`):
Check `data_offset + fixed_len <= value_data.len` before slicing. Returns `EndOfData`.

**Page size underflow** (`src/reader/dynamic_reader.zig`):
Check `uncompressed_size >= rep_len + def_len` before subtraction. Returns `EndOfData`.

**Uuid/Interval slice-to-array conversion** (`src/reader/row_reader.zig`):
Call sites convert `[]const u8` from column decoder to fixed-size arrays (`[16]u8` for Uuid, `[12]u8` for Interval). Added length checks before the conversion to prevent out-of-bounds access on short data.

### Integer Cast Safety

**Safe casting helpers** (`src/reader/column_decoder.zig`, `src/reader/row_reader.zig`, `src/reader/dynamic_reader.zig`):
- `extractBitWidth(data, offset) -> !u5` — validates bit width from external data
- `safeTypeLength(type_length) -> !usize` — validates type length field
- `safePageSize(size) -> !usize` — validates i32 page size to usize
- `safeRowCount(count) -> !usize` — validates i64 row count to usize

**Thrift reader** (`src/thrift/compact.zig`):
Length validation before `@intCast` in `readBinary` and `skip`. Bit-masking uses `@truncate` where semantically correct.

**RLE/Delta encoding** (`src/encoding/rle.zig`, `src/encoding/delta_binary_packed.zig`):
Shift amount validation in `readVarInt` (check before use). `std.math.cast` for header values. Bit width validation before shift operations.

### Type Conversion Safety

**Decimal.fromBytes** (`src/types.zig`):
Explicit bounds check that `raw_bytes.len <= 16` before copying into the fixed-size byte array. Returns `InvalidDecimalLength`. Previously assumed the caller would never pass more than 16 bytes — the subtraction `16 - raw_bytes.len` would underflow and `@memcpy` would write out of bounds.

**Int96.fromNanos Julian day** (`src/types.zig`):
Extreme nanosecond values produce day counts outside i32 range. Replaced `catch unreachable` with saturating `std.math.cast` that clamps to `maxInt(i32)` / `minInt(i32)`.

### Decompression Safety

**Gzip decompression** (`src/compress/gzip.zig`):
- Maximum decompression size limit (256MB)
- Compression ratio limit (1000x)
- Safe casts for C API parameters

**Concatenated gzip** (`src/compress/gzip.zig`):
Iterative decompression handles multiple concatenated gzip streams.

## Test Results

### Previously-Panicking Files

| File | Panic Type | Now Returns |
|------|------------|-------------|
| `alltypes_tiny_pages.parquet` | index out of bounds | Passes |
| `alltypes_tiny_pages_plain.parquet` | index out of bounds | Passes |
| `dms_test_table_LOAD00000001.parquet` | index out of bounds | Passes |
| `rle_boolean_encoding.parquet` | index out of bounds | Passes |
| `fixed_length_byte_array.parquet` | index out of bounds | `EndOfData` (malformed) |
| `nation.dict-malformed.parquet` | index out of bounds | `EndOfData` (malformed) |
| `ARROW-GH-41321.parquet` | integer overflow | `InvalidBitWidth` (malformed) |
| `ARROW-RS-GH-6229-DICTHEADER.parquet` | integer overflow | `InvalidPageSize` (malformed) |
| `yellow_tripdata_2023-01.parquet` | integer overflow | Passes |
| `concatenated_gzip_members.parquet` | segfault | Passes |

### Current Failures

| File | Error | Category |
|------|-------|----------|
| `bad_data/ARROW-GH-41321.parquet` | `InvalidBitWidth` | Intentionally malformed |
| `bad_data/ARROW-RS-GH-6229-DICTHEADER.parquet` | `InvalidPageSize` | Intentionally malformed |
| `bad_data/PARQUET-1481.parquet` | `SchemaParseError` | Intentionally malformed |
| `data/fixed_length_byte_array.parquet` | `EndOfData` | Malformed data |
| `data/nation.dict-malformed.parquet` | `EndOfData` | Malformed data |
| `data/datapage_v1-corrupt-checksum.parquet` | `PageChecksumMismatch` | Corrupt checksum (see note) |
| `data/rle-dict-uncompressed-corrupt-checksum.parquet` | `PageChecksumMismatch` | Corrupt checksum (see note) |
| `data/hadoop_lz4_compressed.parquet` | `UnsupportedCompression` | Hadoop LZ4 (not planned) |
| `data/hadoop_lz4_compressed_larger.parquet` | `UnsupportedCompression` | Hadoop LZ4 (not planned) |
| `data/non_hadoop_lz4_compressed.parquet` | `UnsupportedCompression` | Hadoop LZ4 (not planned) |

**Note on checksum failures:** The `pq validate` command enables CRC32 page checksum verification by default. These two files have intentionally corrupt checksums. The library default is `validate_page_checksum: false`, so library users are unaffected. The previous count of 252/260 was measured before checksum verification was added to the validate command.

### Error Types

| Error | Meaning |
|-------|---------|
| `EndOfData` | Attempted to read past end of buffer |
| `InvalidBitWidth` | Bit width value > 31 |
| `InvalidPageSize` | Negative or overflow page size |
| `InvalidDecimalLength` | Decimal bytes exceed 16-byte limit |
| `SchemaParseError` | Invalid or unsupported schema structure |
| `PageChecksumMismatch` | Page CRC checksum verification failed |
| `UnsupportedCompression` | Compression codec not implemented |

### Validation

```bash
cd cli && ./validate-wild.sh        # Show failures only
cd cli && ./validate-wild.sh --all  # Show all results
cd zig-parquet && zig build test    # Unit tests
```

Current: 250/260 passing (96.2%) — 10 failures are all proper errors (3 Hadoop LZ4, 3 intentionally malformed, 2 corrupt checksums, 2 malformed data).

## Remaining Work

### Writer Input Validation

The writer receives user-provided data that needs validation at API boundaries:

```zig
if (num_rows > std.math.maxInt(i64)) return error.TooManyRows;
if (value.len > std.math.maxInt(i32)) return error.ValueTooLarge;
if (precision > 38) return error.InvalidPrecision;
```

Areas to audit:
- `Writer.writeRow` / `Writer.writeRows` — user data entry point
- `ColumnWriter.writeValues` — value encoding
- Page size calculations — potential overflow with large data

### Integer Arithmetic Overflow

Arithmetic on external values should use checked operations:

```zig
std.math.add(usize, a, b) catch return error.Overflow;
std.math.sub(usize, a, b) catch return error.Underflow;
```

## Guidelines for New Code

### Reader
1. **Never `@intCast` external data** — use `safe.cast` / `safe.castTo`
2. **Bounds-check before slicing** — `if (offset + len > data.len) return error.EndOfData`
3. **Use helpers** — `safePageSize`, `extractBitWidth`, etc.
4. **Specific errors** — `InvalidBitWidth` over generic `InvalidData`
5. **Comment `catch unreachable`** — explain the mathematical invariant

### Writer
1. **Validate at API boundary** — check sizes, counts, value ranges
2. **Checked arithmetic** — `std.math.add`/`mul` with error handling
3. **Validate schema constraints** — precision, scale, type lengths
4. **Clear errors** — `ValueTooLarge`, `TooManyRows`, `InvalidSchema`

### Auditing
```bash
rg "@intCast" src/ --type zig -c    # Should be 0 outside tests
rg "catch unreachable" src/ --type zig  # Each should have an invariant comment
rg "\[.*\.\.\]\[0\.\." src/ --type zig  # Check for unchecked slicing
```
