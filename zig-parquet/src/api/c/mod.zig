//! C ABI surface for libparquet.
//!
//! Re-exports all C ABI modules for test discoverability and
//! for use as a single import point.

pub const err = @import("error.zig");
pub const handles = @import("handles.zig");
pub const reader = @import("reader.zig");
pub const writer = @import("writer.zig");
pub const row_reader = @import("row_reader.zig");
pub const row_writer = @import("row_writer.zig");
pub const introspect = @import("introspect.zig");

test {
    _ = err;
    _ = handles;
    _ = reader;
    _ = writer;
    _ = row_reader;
    _ = row_writer;
    _ = introspect;
}
