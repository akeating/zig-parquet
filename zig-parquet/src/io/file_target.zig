//! FileTarget - file-based WriteTarget backend
//!
//! Writes data to a file using buffered I/O.

const std = @import("std");
const write_target = @import("../core/write_target.zig");
const WriteTarget = write_target.WriteTarget;
const WriteError = write_target.WriteError;

pub const FileTarget = struct {
    file: std.fs.File,
    write_buffer: [8192]u8 = undefined,
    streaming_writer: ?std.fs.File.Writer = null,

    const Self = @This();

    /// Initialize a FileTarget for the given file
    pub fn init(file: std.fs.File) Self {
        return .{
            .file = file,
        };
    }

    /// Get the WriteTarget interface
    pub fn target(self: *Self) WriteTarget {
        return .{
            .ptr = self,
            .vtable = &vtable,
        };
    }

    /// Get the underlying std.Io.Writer for low-level operations.
    pub fn writer(self: *Self) *std.Io.Writer {
        if (self.streaming_writer == null) {
            self.streaming_writer = self.file.writerStreaming(&self.write_buffer);
        }
        return &self.streaming_writer.?.interface;
    }

    /// Flush buffered data to file
    pub fn flush(self: *Self) WriteError!void {
        if (self.streaming_writer) |*sw| {
            sw.interface.flush() catch return error.WriteError;
        }
    }

    /// No result for file target - data is written to file
    pub fn deinit(self: *Self) void {
        _ = self;
    }

    const vtable: WriteTarget.VTable = .{
        .write = fileWrite,
        .close = fileClose,
    };

    fn fileWrite(ptr: *anyopaque, data: []const u8) WriteError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (self.streaming_writer == null) {
            self.streaming_writer = self.file.writerStreaming(&self.write_buffer);
        }
        if (self.streaming_writer) |*sw| {
            sw.interface.writeAll(data) catch return error.WriteError;
        }
    }

    fn fileClose(ptr: *anyopaque) WriteError!void {
        const self: *Self = @ptrCast(@alignCast(ptr));
        if (self.streaming_writer) |*sw| {
            sw.interface.flush() catch return error.WriteError;
        }
    }
};
