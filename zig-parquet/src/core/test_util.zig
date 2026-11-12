//! Helpers shared by tests.

const std = @import("std");
const SeekableReader = @import("seekable_reader.zig").SeekableReader;

/// Returns `str` repeated `n` times. Replaces the `str ** n` operator removed in Zig 0.17.
/// Same approach as `repeatString` in the Zig 0.17 standard library (std/tar/Writer.zig).
pub inline fn repeatString(comptime n: usize, comptime str: []const u8) []const u8 {
    const buf: [n][str.len]u8 = @splat(str[0..str.len].*);
    return @ptrCast(&buf);
}

/// A `SeekableReader` over `data` that records the offset and length of each `readAt`.
pub const RecordingSource = struct {
    data: []const u8,
    allocator: std.mem.Allocator,
    reads: std.ArrayList(Read) = .empty,

    pub const Read = struct {
        offset: u64,
        len: usize,
    };

    pub fn deinit(self: *RecordingSource) void {
        self.reads.deinit(self.allocator);
    }

    pub fn reader(self: *RecordingSource) SeekableReader {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable: SeekableReader.VTable = .{ .readAt = readAt, .size = size };

    fn readAt(ctx: *anyopaque, offset: u64, buf: []u8) SeekableReader.Error!usize {
        const self: *RecordingSource = @ptrCast(@alignCast(ctx));
        self.reads.append(self.allocator, .{ .offset = offset, .len = buf.len }) catch return error.InputOutput;
        if (offset >= self.data.len) return 0;
        const from: usize = @intCast(offset); // offset < data.len
        const n = @min(buf.len, self.data.len - from);
        @memcpy(buf[0..n], self.data[from..][0..n]);
        return n;
    }

    fn size(ctx: *anyopaque) u64 {
        const self: *RecordingSource = @ptrCast(@alignCast(ctx));
        return self.data.len;
    }
};
