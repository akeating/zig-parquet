//! CallbackReader - callback-backed SeekableReader implementation
//!
//! Enables reading from caller-provided function pointers, bridging the
//! SeekableReader vtable to external IO callbacks. This is the primary
//! transport for C ABI, WASM host, and custom IO backends.
//!
//! ## Example
//! ```zig
//! var cb = CallbackReader{
//!     .ctx = @ptrCast(&my_source),
//!     .read_at_fn = myReadAt,
//!     .size_fn = mySize,
//! };
//! const seekable = cb.reader();
//! var reader = try Reader.initFromSeekable(allocator, seekable);
//! ```

const std = @import("std");
const SeekableReader = @import("../core/seekable_reader.zig").SeekableReader;

/// Callback-backed random-access reader.
///
/// The caller owns the context pointer and guarantees it outlives the
/// CallbackReader. The library never frees the callback context.
pub const CallbackReader = struct {
    ctx: *anyopaque,
    read_at_fn: *const fn (ctx: *anyopaque, offset: u64, out: []u8) SeekableReader.Error!usize,
    size_fn: *const fn (ctx: *anyopaque) u64,

    const vtable = SeekableReader.VTable{
        .readAt = callbackReadAt,
        .size = callbackSize,
    };

    fn callbackReadAt(ptr: *anyopaque, offset: u64, buf: []u8) SeekableReader.Error!usize {
        const self: *CallbackReader = @ptrCast(@alignCast(ptr));
        return self.read_at_fn(self.ctx, offset, buf);
    }

    fn callbackSize(ptr: *anyopaque) u64 {
        const self: *CallbackReader = @ptrCast(@alignCast(ptr));
        return self.size_fn(self.ctx);
    }

    /// Get a SeekableReader interface backed by these callbacks.
    /// The CallbackReader must outlive the returned SeekableReader.
    pub fn reader(self: *CallbackReader) SeekableReader {
        return .{ .ptr = @ptrCast(self), .vtable = &vtable };
    }
};
