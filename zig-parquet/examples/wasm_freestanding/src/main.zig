//! Freestanding WASM build verification.
//!
//! This module exercises the freestanding WASM API exports to ensure they
//! compile correctly. It also re-exports the parquet lib to pull in the
//! WASM API surface (freestanding exports and host function imports).
//!
//! The host must provide: host_read_at, host_size, host_write, host_close.

const parquet = @import("parquet");

comptime {
    _ = parquet.wasm_api;
}

export fn ping() i32 {
    return 42;
}
