//! SeekableReader - vtable-based interface for random-access reading
//!
//! Enables reading from buffers (WASM, testing) or files with the same API.

const std = @import("std");

/// Vtable-based interface for random-access reading from any data source.
///
/// This enables the same reader code to work with in-memory buffers (for WASM/testing)
/// or files (for native usage) without code duplication.
pub const SeekableReader = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        readAt: *const fn (ctx: *anyopaque, offset: u64, buf: []u8) Error!usize,
        size: *const fn (ctx: *anyopaque) u64,
    };

    pub const Error = error{
        InputOutput,
        Unseekable,
    };

    /// Read data starting at the given offset into the buffer.
    /// Returns the number of bytes read (may be less than buf.len at end of data).
    pub fn readAt(self: SeekableReader, offset: u64, buf: []u8) Error!usize {
        return self.vtable.readAt(self.ptr, offset, buf);
    }

    /// Get the total size of the data source in bytes.
    pub fn size(self: SeekableReader) u64 {
        return self.vtable.size(self.ptr);
    }
};
