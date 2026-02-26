# WASM Demo (WASI)

This sub-project exercises the full zig-parquet API surface as a `wasm32-wasi`
module. It verifies all type/codec paths compile and run correctly under WASI.

## Build

Build from the zig-parquet root:

```bash
# Build with compression support
zig build -Dwasm_wasi

# Build without compression (smaller binary)
zig build -Dwasm_wasi -Dno_compression
```

Output: `zig-out/bin/parquet_wasi.wasm`

## Run

Run with any WASI runtime:

```bash
wasmtime zig-out/bin/parquet_wasi.wasm
```

## Host Example (Node.js)

The `host/` directory contains a Node.js example that uses the **row reader
API** — a cursor-based interface for reading Parquet data one row at a time
with typed getters. No Arrow knowledge required.

- `host/types.d.ts` — TypeScript types for all WASI module exports (row reader + Arrow)
- `host/example.ts` — Node.js example using `node:wasi` that reads a Parquet
  file's rows through the row reader API

To run:

```bash
# Build the WASM module first
cd ../../  # back to zig-parquet root
zig build -Dwasm_wasi -Dno_compression

# Run the host example
cd examples/wasm_demo
npx tsx host/example.ts path/to/file.parquet
```

### Row Reader API overview

```
open  →  get metadata  →  read_row_group  →  next/get_* loop  →  close
```

The row reader loads one row group at a time into memory, then lets you
iterate rows with `next()` and read typed values with `get_int32()`,
`get_bytes()`, etc. Column types are inspectable via `get_type()`.
