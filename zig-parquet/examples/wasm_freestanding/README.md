# Freestanding WASM Example

Build verification for `wasm32-freestanding` targets. This compiles the parquet
library with `codecs=none` and exposes the freestanding WASM API.

## Build

```bash
zig build
```

The resulting `.wasm` file is at `zig-out/bin/parquet_freestanding_demo.wasm`.

## Host Requirements

The WASM module imports four functions from the `env` namespace that the host
must provide:

| Import | Signature | Description |
|---|---|---|
| `host_read_at` | `(ctx: u32, offset_lo: u32, offset_hi: u32, buf_ptr: *u8, buf_len: u32) -> i32` | Read bytes at offset |
| `host_size` | `(ctx: u32) -> u64` | Total size of data source |
| `host_write` | `(ctx: u32, data_ptr: *const u8, data_len: u32) -> i32` | Write bytes to sink |
| `host_close` | `(ctx: u32) -> i32` | Close output sink |

## Exports

The module exports memory management (`zp_alloc`, `zp_free`), the **row
reader** API (`zp_row_reader_*`), the Arrow reader/writer API
(`zp_reader_*`, `zp_writer_*`), introspection (`zp_version`,
`zp_codec_supported`), and Arrow struct accessors (`zp_arrow_*`).

## Host Example (Deno)

The `host/` directory contains a working Deno example that uses the **row
reader API** — a cursor-based interface for reading Parquet data one row at a
time with typed getters. No Arrow knowledge required.

- `host/types.d.ts` — TypeScript types for all exports and required imports
- `host/example.ts` — Deno example that loads a `.wasm` module, provides
  host IO backed by an ArrayBuffer, and reads a Parquet file's actual row data

To run:

```bash
# First build the WASM module
zig build

# Then run the host example
deno run --allow-read host/example.ts path/to/file.parquet
```

### Row Reader API overview

```
open  →  get metadata  →  read_row_group  →  next/get_* loop  →  close
```

The row reader loads one row group at a time into memory, then lets you
iterate rows with `next()` and read typed values with `get_int32()`,
`get_bytes()`, etc. Column types are inspectable via `get_type()`.
