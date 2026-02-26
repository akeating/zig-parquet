//! WASM API surface for libparquet.
//!
//! Provides wasm32-wasi and wasm32-freestanding wrappers that expose the same
//! Parquet reader/writer functionality as the C ABI but with WASM-compatible
//! calling conventions.

const builtin = @import("builtin");
const is_wasm = builtin.cpu.arch == .wasm32 or builtin.cpu.arch == .wasm64;

pub const handles = @import("handles.zig");
pub const err = @import("../../api/c/error.zig");

pub const wasi = if (builtin.os.tag == .wasi) @import("wasi.zig") else struct {};
pub const freestanding = if (is_wasm and builtin.os.tag == .freestanding) @import("freestanding.zig") else struct {};

test {
    _ = handles;
    _ = err;
}
