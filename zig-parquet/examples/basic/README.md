# Basic Examples

This sub-project contains simple, single-file scripts that demonstrate the core features of `zig-parquet`.
If you are new to the library, this is the best place to start!

## Building and Running

You can run these examples using the standard `zig build` command from within this directory:

```bash
cd examples/basic

# 1. Basic read and write of a struct
zig build run-01_read_write

# 2. Schema-less reading (useful for tools like pq cat)
zig build run-02_dynamic_read

# 3. Writing and reading nested structs and lists
zig build run-03_nested_types

# 4. Stream/Buffer API (reading/writing without a filesystem)
zig build run-04_in_memory_buffer

# 5. Low-level column-based API
zig build run-05_column_api

# 6. Decoding columns on several threads: Arrow batches with read-ahead, and rows
zig build run-06_threaded_read -Doptimize=ReleaseFast
```

## Structure

All examples in this directory share a single `build.zig` and `build.zig.zon` file. To add a new simple example, put a `.zig` file in `src/` and add its name to the `examples` list in `build.zig`.