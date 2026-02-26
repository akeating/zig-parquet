//! BufferTarget - in-memory WriteTarget backend
//!
//! Writes data to an in-memory buffer. Useful for WASM and testing.

const std = @import("std");
const write_target = @import("../core/write_target.zig");
const WriteTarget = write_target.WriteTarget;
const WriteError = write_target.WriteError;

pub const BufferTarget = struct {
    allocator: std.mem.Allocator,
    buffer: std.Io.Writer.Allocating,

    const Self = @This();

    /// Initialize a BufferTarget
    pub fn init(allocator: std.mem.Allocator) Self {
        return .{
            .allocator = allocator,
            .buffer = .init(allocator),
        };
    }

    /// Get the WriteTarget interface
    pub fn target(self: *Self) WriteTarget {
        return .{
            .ptr = self,
            .vtable = &vtable,
        };
    }

    /// Get the underlying std.Io.Writer for low-level operations
    pub fn writer(self: *Self) *std.Io.Writer {
        return &self.buffer.writer;
    }

    /// Flush is a no-op for buffer target (data is already in memory)
    pub fn flush(self: *Self) WriteError!void {
        _ = self;
    }

    /// Get the written data as an owned slice.
    /// Caller owns the returned memory and must free it.
    pub fn toOwnedSlice(self: *Self) ![]u8 {
        return try self.buffer.toOwnedSlice();
    }

    /// Get the written data without taking ownership (borrowed slice)
    pub fn written(self: *Self) []u8 {
        return self.buffer.written();
    }

    /// Clean up resources
    pub fn deinit(self: *Self) void {
        self.buffer.deinit();
    }

    const vtable: WriteTarget.VTable = .{
        .write = bufferWrite,
        .close = bufferClose,
    };

    fn bufferWrite(ptr: *anyopaque, data: []const u8) WriteError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        self.buffer.writer.writeAll(data) catch return error.WriteError;
    }

    fn bufferClose(ptr: *anyopaque) WriteError!void {
        _ = ptr;
    }
};
