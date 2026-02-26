# zig-parquet Examples

This directory contains examples of how to use the `zig-parquet` library.

The examples are grouped into sub-projects by complexity.

## Directory Structure

- **[`basic/`](basic/)** - Start here! Contains simple, single-file scripts demonstrating core features:
  - Reading and writing structs
  - Schema-less dynamic reading
  - Handling nested lists and structs
  - Reading/writing to in-memory buffers instead of files
  - Low-level column-based API

- **[`gen-power-grid/`](gen-power-grid/)** - A showcase of time-series data compression. Generates 6 hours of synthetic 3-phase power grid monitoring data to demonstrate the `.delta_binary_packed` integer encoding.

- **[`large_file/`](large_file/)** - A stress test and validation script. Generates a large Parquet file spanning multiple row groups and pages, demonstrating complex schema topologies and dictionary encoding, then verifies it by reading it back dynamically.

- **[`wasm_demo/`](wasm_demo/)** - Demonstrates how to compile and run `zig-parquet` as a WebAssembly (WASI) module, including decompression with Snappy, LZ4, and Brotli. The host example uses the **row reader API** (cursor-based, no Arrow required).

- **[`wasm_freestanding/`](wasm_freestanding/)** - Freestanding WASM build (`wasm32-freestanding`) with host-imported IO. The host example uses the **row reader API** to iterate rows with typed getters.
