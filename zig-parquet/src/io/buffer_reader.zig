//! BufferReader - in-memory SeekableReader backend
//!
//! Useful for WASM (where data is fetched via JS and passed as a buffer)
//! and for testing (where test data can be constructed in memory).

const std = @import("std");
const safe = @import("../core/safe.zig");
const SeekableReader = @import("../core/seekable_reader.zig").SeekableReader;

pub const BufferReader = struct {
    data: []const u8,

    /// Create a BufferReader from a byte slice.
    /// The slice must outlive the BufferReader.
    pub fn init(data: []const u8) BufferReader {
        return .{ .data = data };
    }

    /// Get a SeekableReader interface for this buffer.
    /// The BufferReader must outlive the returned SeekableReader.
    pub fn reader(self: *BufferReader) SeekableReader {
        return .{
            .ptr = self,
            .vtable = &vtable,
        };
    }

    const vtable = SeekableReader.VTable{
        .readAt = readAt,
        .size = getSize,
    };

    fn readAt(ctx: *anyopaque, offset: u64, buf: []u8) SeekableReader.Error!usize {
        const self: *BufferReader = @ptrCast(@alignCast(ctx));
        if (offset >= self.data.len) {
            return 0;
        }
        const start = safe.cast(offset) catch return 0; // Safe: validated above
        const len = @min(buf.len, self.data.len - start);
        @memcpy(buf[0..len], self.data[start..][0..len]);
        return len;
    }

    fn getSize(ctx: *anyopaque) u64 {
        const self: *BufferReader = @ptrCast(@alignCast(ctx));
        return self.data.len;
    }
};
