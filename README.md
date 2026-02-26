# zig-parquet

A native Parquet library built for portability, embeddability, and low deployment friction. Use it from Zig or through a C ABI.

[![CI](https://github.com/akeating/zig-parquet/actions/workflows/test.yml/badge.svg)](https://github.com/akeating/zig-parquet/actions/workflows/test.yml)
[![Zig](https://img.shields.io/badge/Zig-0.15.2-f7a41d?logo=zig)](https://ziglang.org/)
[![License](https://img.shields.io/badge/License-MIT%2FApache--2.0-blue.svg)](COPYRIGHT)

## Features

- **Embeddable Native Library** - Link Parquet support directly into native applications
- **C ABI with Arrow C Data Interface** - Call from C, C++, and other languages via ArrowSchema, ArrowArray, and ArrowArrayStream
- **Full Read/Write Support** - Read and write Parquet files with all physical types
- **All Standard Encodings** - PLAIN, RLE, DICTIONARY, DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY, BYTE_STREAM_SPLIT
- **Nested Types** - Lists, structs, maps, and arbitrary nesting depth
- **Compression** - zstd, gzip, snappy, lz4, brotli (via C/C++ libraries)
- **Logical Types** - Timestamps, dates, decimals, UUIDs, JSON, BSON, enums, geometry, geography
- **Comptime Row API** - Type-safe `RowWriter(T)` / `RowReader(T)` with schema inference
- **Dynamic Reader** - Schema-agnostic reading without comptime types
- **Column Statistics** - Min/max/null_count in column metadata
- **Page-Level CRC Checksums** - Written by default, validated on read
- **Key-Value Metadata** - Read and write arbitrary file-level metadata
- **DataPage V1 and V2** - Full support for both page format versions
- **Buffer and Callback Transports** - Read/write from memory, files, or custom I/O backends
- **Hardened Against Malformed Input** - Safe casting, bounds checking, no undefined behavior on untrusted data
- **Portable Deployment** - Native library and CLI for desktops, servers, edge devices, and serverless jobs
- **WASM Compatible** - 103 KB plain or 438 KB with all compression codecs (brotli-compressed)
- **CLI Tool** - `pqi` for inspecting and validating Parquet files

## CLI Tool

The `pqi` command-line tool is included for working with Parquet files:

```bash
# Build the CLI
cd cli && zig build

# Show schema
pqi schema data.parquet

# Preview rows
pqi head data.parquet -n 10

# Output as JSON
pqi cat data.parquet --json

# Show file statistics
pqi stats data.parquet

# Validate file integrity
pqi validate data.parquet
```

## Why zig-parquet?

If you need Parquet support inside a native application, zig-parquet is a straightforward way to ship it.

- **Embed directly** - Use Parquet from Zig or through the C ABI instead of shelling out to a separate tool or service
- **Keep deployment simple** - Stay native without requiring the JVM, Python, or the full Arrow C++ stack
- **Ship across targets** - Use the same core library on desktops, servers, edge devices, serverless workloads, and WASM
- **Start with the CLI** - Use `pqi` to inspect and validate files, then embed the same implementation in your application

## Installation

Add `zig-parquet` to your project using `zig fetch`. This will automatically download the package and update your `build.zig.zon` with the correct cryptographic hash:

```bash
zig fetch --save https://github.com/akeating/zig-parquet/releases/download/v0.1.0/zig-parquet-v0.1.0.tar.gz
```

Then in your `build.zig`:

```zig
const parquet = b.dependency("parquet", .{
    .target = target,
    .optimize = optimize,
});
exe.root_module.addImport("parquet", parquet.module("parquet"));
exe.linkLibrary(parquet.artifact("parquet"));
```

## Quick Start

### Row-Based API (Recommended)

The easiest way to work with Parquet files - define a struct and let the library handle schema inference:

```zig
const std = @import("std");
const parquet = @import("parquet");

const SensorReading = struct {
    sensor_id: i32,
    timestamp: parquet.Timestamp,
    temperature: f64,
    location: ?[]const u8,  // nullable string
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Write
    {
        const file = try std.fs.cwd().createFile("sensors.parquet", .{});
        defer file.close();

        var writer = try parquet.writeToFileRows(SensorReading, allocator, file, .{
            .compression = .zstd,
        });
        defer writer.deinit();

        try writer.writeRow(.{
            .sensor_id = 1,
            .timestamp = parquet.Timestamp.fromMicros(1704067200000000),
            .temperature = 23.5,
            .location = "Building A",
        });

        try writer.close();
    }

    // Read
    {
        const file = try std.fs.cwd().openFile("sensors.parquet", .{});
        defer file.close();

        var reader = try parquet.openFileRowReader(SensorReading, allocator, file, .{});
        defer reader.deinit();

        while (try reader.next()) |row| {
            defer reader.freeRow(&row);
            std.debug.print("Sensor {}: {}°C\n", .{ row.sensor_id, row.temperature });
        }
    }
}
```

### Column-Based API

For more control, use the lower-level column API:

```zig
const parquet = @import("parquet");
const Optional = parquet.Optional;

// Write columns directly
var writer = try parquet.writeToFile(allocator, file, &.{
    .{ .name = "id", .type_ = .int64, .optional = false, .codec = .zstd },
    .{ .name = "name", .type_ = .byte_array, .optional = true },
});
defer writer.deinit();

try writer.writeColumnOptional(i64, 0, &[_]Optional(i64){
    .{ .value = 1 }, .{ .value = 2 }, .{ .value = 3 }, .{ .value = 4 }, .{ .value = 5 },
});
try writer.writeColumnOptional([]const u8, 1, &[_]Optional([]const u8){
    .{ .value = "Alice" }, .null_value, .{ .value = "Charlie" }, .{ .value = "Diana" }, .null_value,
});
try writer.close();

// Read columns — readColumn returns []Optional(T)
var reader = try parquet.openFile(allocator, file);
defer reader.deinit();

const ids = try reader.readColumn(0, i64);
defer allocator.free(ids);

for (ids) |id| {
    if (id.isNull()) { /* handle null */ }
    else std.debug.print("id: {}\n", .{id.value});
}
```

## Supported Types

### Physical Types

| Parquet Type | Zig Type |
|--------------|----------|
| BOOLEAN | `bool` |
| INT32 | `i32` |
| INT64 | `i64` |
| FLOAT | `f32` |
| DOUBLE | `f64` |
| BYTE_ARRAY | `[]const u8` |
| FIXED_LEN_BYTE_ARRAY | `[]const u8` |

### Logical Types

| Logical Type | Usage |
|--------------|-------|
| STRING | `[]const u8` with string annotation |
| DATE | `parquet.Date` (days since epoch) |
| TIMESTAMP | `parquet.Timestamp` (micros since epoch) |
| TIME | `parquet.Time` (micros since midnight) |
| UUID | `parquet.Uuid` (16-byte array) |
| INTERVAL | `parquet.Interval` (months/days/millis) |
| GEOMETRY | `[]const u8` (WKB) with geometry annotation |
| GEOGRAPHY | `[]const u8` (WKB) with geography annotation |
| DECIMAL | Configurable precision/scale |
| JSON | String with JSON annotation |
| BSON | Binary with BSON annotation |
| ENUM | String with enum annotation |

### Nested Types

```zig
const Order = struct {
    order_id: i64,
    items: []const Item,           // LIST
    shipping: Address,             // STRUCT
    metadata: ?[]const u8,         // Nullable
};

const Item = struct {
    product_id: i32,
    quantity: i32,
    price: f64,
};

const Address = struct {
    street: []const u8,
    city: []const u8,
    zip: []const u8,
};
```

## Compression

All major Parquet compression codecs are supported:

| Codec | Library | Notes |
|-------|---------|-------|
| zstd | libzstd 1.5.7 | Recommended default |
| gzip | zlib 1.3.1 | Wide compatibility |
| snappy | snappy 1.2.2 | Fast, moderate ratio |
| lz4 | lz4 1.10.0 | Very fast |
| brotli | brotli 1.2.0 | High ratio |

Set per-column or use `RowWriterOptions`:

```zig
var writer = try parquet.writeToFileRows(T, allocator, file, .{
    .compression = .zstd,
});
```

## Spec Coverage

| Feature | | Notes |
|---------|:-:|-------|
| **Physical Types** | | |
| BOOLEAN, INT32, INT64, FLOAT, DOUBLE | ✅ | All primitive types |
| BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY | ✅ | Variable and fixed-length binary |
| INT96 | ✅ | Legacy timestamp support (read/write); exposed as i64 nanos in C/WASM APIs |
| **Encodings** | | |
| PLAIN | ✅ | All physical types |
| RLE / BIT_PACKED | ✅ | Levels, dictionary indices, booleans |
| PLAIN_DICTIONARY / RLE_DICTIONARY | ✅ | Strings and integers |
| DELTA_BINARY_PACKED | ✅ | Sorted integers, timestamps |
| DELTA_LENGTH_BYTE_ARRAY | ✅ | Variable-length byte arrays |
| DELTA_BYTE_ARRAY | ✅ | Sorted strings (prefix compression) |
| BYTE_STREAM_SPLIT | ✅ | Float/double/int/fixed columns |
| **Compression** | | |
| UNCOMPRESSED | ✅ | |
| SNAPPY | ✅ | Via C++ library |
| GZIP | ✅ | Via zlib |
| ZSTD | ✅ | Via libzstd |
| LZ4_RAW | ✅ | Via lz4 |
| BROTLI | ✅ | Via brotli |
| LZ4 (non-raw) | ❌ | Hadoop-specific framing format |
| LZO | ❌ | Not implemented |
| **Logical Types** | | |
| STRING, ENUM, JSON, BSON | ✅ | BYTE_ARRAY with annotation |
| UUID | ✅ | FIXED_LEN_BYTE_ARRAY(16) |
| INT (8/16/32/64, signed/unsigned) | ✅ | Width annotations |
| DECIMAL | ✅ | INT32/INT64/FIXED backing |
| FLOAT16 | ✅ | Half-precision float |
| DATE | ✅ | Days since epoch |
| TIME (MILLIS/MICROS) | ✅ | Time of day |
| TIMESTAMP (MILLIS/MICROS) | ✅ | Instant or local |
| TIME/TIMESTAMP (NANOS) | ✅ | Full read/write support |
| INTERVAL | ✅ | Legacy ConvertedType (months/days/millis) |
| GEOMETRY / GEOGRAPHY | ✅ | GeoParquet 1.1 compatible |
| VARIANT | ⏳ | Future |
| **Nested Types** | | |
| LIST | ✅ | 3-level structure |
| MAP | ✅ | Key-value pairs |
| Nested structs | ✅ | Arbitrary depth |
| **Page Types** | | |
| DATA_PAGE (v1) | ✅ | |
| DATA_PAGE_V2 | ✅ | Read only; optimized split decompression |
| DICTIONARY_PAGE | ✅ | |
| **Features** | | |
| Column statistics | ✅ | min/max/null_count |
| Multi-page columns | ✅ | Large column support |
| Multi-row-group files | ✅ | |
| Bloom filters | ⏳ | Planned |
| Page Index | ⏳ | Planned |
| CRC checksums | ✅ | Page-level CRC32 |
| Encryption | 🔍 | Under review — Java/Python-only ecosystem support |

Legend: ✅ Supported | ⏳ Planned | 🔍 Under review | ❌ Unsupported

Files containing unsupported features return explicit errors rather than silently producing incorrect results.

## WASM Support

Binary sizes (`ReleaseSmall`, brotli-compressed):

| | Plain | With Compression |
|---|---|---|
| **WASI** (`wasm32-wasi`) | 103 KB | 438 KB |
| **Browser** (`wasm32-freestanding`) | 103 KB | — |

Raw (uncompressed):

| | Plain | With Compression |
|---|---|---|
| **WASI** | 517 KB | 1.7 MB |
| **Browser** | 519 KB | — |

Browser + compression is not currently supported

Build for WASI:

```bash
cd zig-parquet
zig build -Dwasm_wasi -Doptimize=ReleaseSmall
# Output: zig-out/bin/parquet_wasi.wasm
```

Build for browser (freestanding, no compression):

```bash
cd zig-parquet
zig build -Dwasm_freestanding -Doptimize=ReleaseSmall
# Output: zig-out/bin/parquet_freestanding.wasm
```

Run with a WASI runtime:

```bash
wasmtime --dir=. zig-out/bin/parquet_wasi.wasm
```

See `examples/wasm_demo/` and `examples/wasm_freestanding/` for usage examples.

## Requirements

- **Zig 0.15.2**
- C compiler (for compression libraries)
- C++ compiler (for Snappy)

## Project Structure

```
zig-parquet/
├── zig-parquet/          # Core library
│   ├── src/
│   │   ├── lib.zig       # Public API
│   │   ├── reader.zig    # Parquet reader
│   │   ├── writer.zig    # Parquet writer
│   │   ├── format/       # Parquet format types
│   │   ├── encoding/     # PLAIN, RLE, DICTIONARY, DELTA_*
│   │   ├── compress/     # Compression codecs
│   │   └── tests/        # Test suite
│   └── examples/
├── cli/                  # pqi CLI tool
└── test-files-arrow/     # PyArrow-generated test files
```

## Testing

```bash
cd zig-parquet
zig build test
```

## License

Licensed under either of

- [Apache License, Version 2.0](LICENSE-APACHE)
- [MIT License](LICENSE-MIT)

at your option.

## Contributing

Contributions welcome! Please read the existing code style and add tests for new functionality.

## Acknowledgments

- [Apache Parquet](https://parquet.apache.org/) specification
- [PyArrow](https://arrow.apache.org/docs/python/) for reference implementation and test file generation
