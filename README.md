# zig-parquet

A native Parquet library built for portability, embeddability, and low deployment friction. Use it from Zig or through a C ABI.

[![CI](https://github.com/akeating/zig-parquet/actions/workflows/test.yml/badge.svg)](https://github.com/akeating/zig-parquet/actions/workflows/test.yml)
[![Zig](https://img.shields.io/badge/Zig-0.15.2-f7a41d?logo=zig)](https://ziglang.org/)
[![License](https://img.shields.io/badge/License-MIT%2FApache--2.0-blue.svg)](COPYRIGHT)

## Features

- **Embeddable Native Library** - Link Parquet support directly into native applications
- **Full Read/Write Support** - Read and write Parquet files with all physical and logical types
- **All Standard Encodings** - PLAIN, RLE, DICTIONARY, DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY, BYTE_STREAM_SPLIT
- **Nested Types** - Lists, structs, maps, and arbitrary nesting depth
- **Compression** - zstd, gzip, snappy, lz4, brotli (individually selectable; experimental pure Zig zstd, gzip, snappy, lz4, brotli available)
- **Logical Types** - STRING, DATE, TIME, TIMESTAMP (millis/micros/nanos), DECIMAL, UUID, INT annotations, FLOAT16, ENUM, JSON, BSON, INTERVAL, GEOMETRY, GEOGRAPHY
- **Dynamic Row API** - Runtime `DynamicWriter` / `DynamicReader` for all types and arbitrary nesting depth
- **Schema-Agnostic Reading** - Read any Parquet file without knowing the schema at compile time
- **Column Statistics** - Min/max/null_count in column metadata
- **Page-Level CRC Checksums** - Written by default, validated on read
- **Key-Value Metadata** - Read and write arbitrary file-level metadata
- **DataPage V1 and V2** - Read both page formats; write uses V1
- **Buffer and Callback Transports** - Read/write from memory, files, or custom I/O backends
- **Hardened Against Malformed Input** - Designed for safe casting, bounds checking, and no undefined behavior on untrusted data
- **C ABI with Arrow C Data Interface** - Call from C, C++, and other languages via ArrowSchema, ArrowArray, and ArrowArrayStream
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

# Output all rows as JSON
pqi cat data.parquet --json

# Row count
pqi count data.parquet

# File statistics
pqi stats data.parquet

# Row group details
pqi rowgroups data.parquet

# File size breakdown
pqi size data.parquet

# Column detail across row groups
pqi column data.parquet price quantity

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
zig fetch --save https://github.com/akeating/zig-parquet/releases/download/v0.1.6/zig-parquet-v0.1.6.tar.gz
```

Then in your `build.zig`:

```zig
const target = b.standardTargetOptions(.{});
const optimize = b.standardOptimizeOption(.{});

const parquet = b.dependency("parquet", .{
    .target = target,
    .optimize = optimize,
});
exe.root_module.addImport("parquet", parquet.module("parquet"));
exe.linkLibrary(parquet.artifact("parquet"));
```

## Quick Start

### Row-Based API (Recommended)

Define your schema at runtime, write rows with typed setters, and read back dynamically:

```zig
const std = @import("std");
const parquet = @import("parquet");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Write
    {
        const file = try std.fs.cwd().createFile("sensors.parquet", .{});
        defer file.close();

        var writer = try parquet.createFileDynamic(allocator, file);
        defer writer.deinit();

        // Columns are OPTIONAL by default; use .asRequired() for non-nullable
        const TypeInfo = parquet.TypeInfo;
        try writer.addColumn("sensor_id", TypeInfo.int32.asRequired(), .{});
        try writer.addColumn("timestamp", TypeInfo.timestamp_micros, .{});
        try writer.addColumn("temperature", TypeInfo.double_, .{});
        try writer.addColumn("location", TypeInfo.string, .{});
        writer.setCompression(.zstd);
        try writer.begin();

        try writer.setInt32(0, 1);
        try writer.setInt64(1, 1704067200000000);
        try writer.setDouble(2, 23.5);
        try writer.setBytes(3, "Building A");
        try writer.addRow();

        try writer.close();
    }

    // Read
    {
        const file = try std.fs.cwd().openFile("sensors.parquet", .{});
        defer file.close();

        var reader = try parquet.openFileDynamic(allocator, file, .{});
        defer reader.deinit();

        const rows = try reader.readAllRows(0);
        defer {
            for (rows) |row| row.deinit();
            allocator.free(rows);
        }

        for (rows) |row| {
            const id = if (row.getColumn(0)) |v| v.asInt32() orelse 0 else 0;
            const temp = if (row.getColumn(2)) |v| v.asDouble() orelse 0 else 0;
            std.debug.print("Sensor {}: {d}°C\n", .{ id, temp });
        }
    }
}
```

### Column Projection

Read only a subset of top-level columns (skips I/O for unrequested columns):

```zig
var reader = try parquet.openFileDynamic(allocator, file, .{});
defer reader.deinit();

// Read only columns 1 and 3 (dense-packed: returned rows have 2 values)
const rows = try reader.readRowsProjected(0, &.{ 1, 3 });
defer {
    for (rows) |row| row.deinit();
    allocator.free(rows);
}

for (rows) |row| {
    const name = if (row.getColumn(0)) |v| v.asBytes() orelse "" else "";
    const score = if (row.getColumn(1)) |v| v.asDouble() orelse 0 else 0;
    std.debug.print("{s}: {d}\n", .{ name, score });
}
```

### Row Group Filtering

Use column statistics to skip row groups that don't match your criteria:

```zig
var reader = try parquet.openFileDynamic(allocator, file, .{});
defer reader.deinit();

for (0..reader.getNumRowGroups()) |rg| {
    const stats = reader.getColumnStatistics(0, rg) orelse continue;
    const min_bytes = stats.min_value orelse stats.min orelse continue;
    const max_bytes = stats.max_value orelse stats.max orelse continue;
    const min = std.mem.readInt(i32, min_bytes[0..4], .little);
    const max = std.mem.readInt(i32, max_bytes[0..4], .little);

    if (target < min or target > max) continue; // skip this row group

    const rows = try reader.readAllRows(rg);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }
    // ... process matching rows ...
}
```

### Row Iterator

Stream through all rows without managing row groups manually. Only one row group's data is held in memory at a time:

```zig
var reader = try parquet.openFileDynamic(allocator, file, .{});
defer reader.deinit();

var iter = reader.rowIterator();
defer iter.deinit();

while (try iter.next()) |row| {
    const id = if (row.getColumn(0)) |v| v.asInt32() orelse 0 else 0;
    std.debug.print("id={}\n", .{id});
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

| Logical Type | TypeInfo Constant | Physical Storage |
|--------------|-------------------|------------------|
| STRING | `TypeInfo.string` | BYTE_ARRAY |
| DATE | `TypeInfo.date` | INT32 (days since epoch) |
| TIMESTAMP | `TypeInfo.timestamp_micros` | INT64 |
| TIME | `TypeInfo.time_micros` | INT64 |
| UUID | `TypeInfo.uuid` | FIXED_LEN_BYTE_ARRAY(16) |
| INTERVAL | `TypeInfo.interval` | FIXED_LEN_BYTE_ARRAY(12) |
| GEOMETRY | `TypeInfo.geometry` | BYTE_ARRAY (WKB) |
| GEOGRAPHY | `TypeInfo.geography` | BYTE_ARRAY (WKB) |
| DECIMAL | `TypeInfo.forDecimal(p, s)` | INT32/INT64/FIXED |
| JSON | `TypeInfo.json` | BYTE_ARRAY |
| BSON | `TypeInfo.bson` | BYTE_ARRAY |
| ENUM | `TypeInfo.enum_` | BYTE_ARRAY |

### Nested Types

Build arbitrary nested schemas at runtime using `SchemaNode`:

```zig
// list<struct<product_id: i32, quantity: i32, price: f64>>
const pid = try writer.allocSchemaNode(.{ .int32 = .{} });
const qty = try writer.allocSchemaNode(.{ .int32 = .{} });
const price = try writer.allocSchemaNode(.{ .double = .{} });
var fields = try writer.allocSchemaFields(3);
fields[0] = .{ .name = try writer.dupeSchemaName("product_id"), .node = pid };
fields[1] = .{ .name = try writer.dupeSchemaName("quantity"), .node = qty };
fields[2] = .{ .name = try writer.dupeSchemaName("price"), .node = price };
const item = try writer.allocSchemaNode(.{ .struct_ = .{ .fields = fields } });
const items = try writer.allocSchemaNode(.{ .list = item });
try writer.addColumnNested("items", items, .{});
```

Supports lists, structs, maps, and arbitrary nesting depth (e.g., `list<map<string, list<struct<...>>>>`).
See `examples/basic/03_nested_types.zig` for a complete example.

## Compression

All major Parquet compression codecs are supported, individually selectable at build time:

| Codec | Implementation | Notes |
|-------|---------------|-------|
| zstd | C libzstd 1.5.7 | Recommended default |
| gzip | C zlib 1.3.1 | Wide compatibility |
| snappy | C++ snappy 1.2.2 | Fast, moderate ratio |
| lz4 | C lz4 1.10.0 | Very fast |
| brotli | C brotli 1.2.0 | High ratio |
| zig-zstd | Pure Zig (experimental) | No C dependency; level-1 compressor + stdlib decompressor |
| zig-gzip | Pure Zig (experimental) | No C dependency; level-9 deflate compressor + stdlib decompressor |
| zig-snappy | Pure Zig (experimental) | No C/C++ dependency; full Snappy block format |
| zig-lz4 | Pure Zig (experimental) | No C dependency; full LZ4 raw block format |
| zig-brotli | Pure Zig (experimental) | No C dependency; quality-0 compressor + full decompressor |

```zig
var writer = try parquet.createFileDynamic(allocator, file);
writer.setCompression(.zstd);
```

```bash
zig build                           # all codecs (default: C libs)
zig build -Dcodecs=none             # no compression (smallest binary)
zig build -Dcodecs=zstd,snappy      # only zstd and snappy
zig build -Dcodecs=zig-only         # all pure Zig codecs (no C/C++ deps)
```

See [COMPRESSION.md](COMPRESSION.md) for build sizes, API details, and the full set of build options.

### Per-Column and Per-Leaf Options

Set options per column at definition time, or per leaf path for nested types:

```zig
// Per-column options via addColumn
try writer.addColumn("timestamp", parquet.TypeInfo.int64, .{
    .encoding = .delta_binary_packed,
    .compression = .zstd,
});

// Per-leaf options for nested columns via setPathProperties
try writer.addColumnNested("address", struct_node, .{});
try writer.setPathProperties("address.city", .{ .compression = .zstd });
try writer.setPathProperties("address.zip", .{ .use_dictionary = false });
```

Global defaults apply to any column/leaf without an explicit override:

```zig
writer.setUseDictionary(false);        // disable dictionary encoding globally
writer.setIntEncoding(.delta_binary_packed);  // default for int columns
writer.setMaxPageSize(1_048_576);      // 1MB page size limit
```

## Spec Coverage

| Feature | | Notes |
|---------|:-:|-------|
| **Physical Types** | | |
| BOOLEAN, INT32, INT64, FLOAT, DOUBLE | ✅ | All primitive types |
| BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY | ✅ | Variable and fixed-length binary |
| INT96 | ✅ | Legacy timestamp support; read always, write via column API only (not DynamicWriter) |
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
| SNAPPY | ✅ | C++ snappy (default) or pure Zig (experimental via `zig-snappy`) |
| GZIP | ✅ | C zlib (default) or pure Zig (experimental via `zig-gzip`) |
| ZSTD | ✅ | C libzstd (default) or pure Zig (experimental via `zig-zstd`) |
| LZ4_RAW | ✅ | C lz4 (default) or pure Zig (experimental via `zig-lz4`) |
| BROTLI | ✅ | C brotli (default) or pure Zig (experimental via `zig-brotli`) |
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
| Column projection | ✅ | Read subset of columns; skips I/O for unselected columns |
| Row group filtering | ✅ | Statistics-based; skip row groups via min/max/null_count |
| Streaming iteration | ✅ | Row iterator; one row group in memory at a time |
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

Both `wasm32-wasi` and `wasm32-freestanding` targets are supported. WASI supports all codecs via `-Dcodecs=`; freestanding builds without compression. See [COMPRESSION.md](COMPRESSION.md) for per-codec WASM binary sizes.

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
- C compiler (for compression libraries; not needed with `-Dcodecs=none` or `-Dcodecs=zig-only`)
- C++ compiler (for Snappy; not needed if snappy is excluded)

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
