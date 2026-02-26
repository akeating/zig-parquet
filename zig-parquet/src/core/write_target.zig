//! Write Target Interface
//!
//! Provides a vtable-based interface for writing Parquet data to different backends.
//! This abstraction allows Writer and RowWriter to be backend-agnostic.

const std = @import("std");

/// Error type for write operations
pub const WriteError = error{
    WriteError,
    OutOfMemory,
    IntegerOverflow,
};

/// Vtable-based interface for writing to different backends.
/// The Writer uses this interface to write Parquet data without knowing
/// the underlying storage mechanism.
pub const WriteTarget = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Write data to the target
        write: *const fn (ptr: *anyopaque, data: []const u8) WriteError!void,
        /// Close/finalize the target (flush buffers, complete uploads, etc.)
        close: *const fn (ptr: *anyopaque) WriteError!void,
    };

    /// Write data to the target
    pub fn write(self: WriteTarget, data: []const u8) WriteError!void {
        return self.vtable.write(self.ptr, data);
    }

    /// Close/finalize the target
    pub fn close(self: WriteTarget) WriteError!void {
        return self.vtable.close(self.ptr);
    }
};

/// Adapter that bridges a `WriteTarget` vtable into a `*std.Io.Writer` interface.
///
/// Stores the `WriteTarget` and an unbuffered `std.Io.Writer` whose `drain`
/// forwards every write through `WriteTarget.write()`. This lets the column
/// writer pipeline (which requires `*std.Io.Writer`) work with any external
/// `WriteTarget` implementation (callbacks, custom sinks, etc.).
pub const WriteTargetWriter = struct {
    target: WriteTarget,
    io_writer: std.Io.Writer,

    const io_vtable: std.Io.Writer.VTable = .{
        .drain = drain,
        .flush = std.Io.Writer.noopFlush,
        .rebase = std.Io.Writer.failingRebase,
    };

    pub fn init(target: WriteTarget) WriteTargetWriter {
        return .{
            .target = target,
            .io_writer = .{
                .buffer = &.{},
                .vtable = &io_vtable,
            },
        };
    }

    pub fn writer(self: *WriteTargetWriter) *std.Io.Writer {
        return &self.io_writer;
    }

    /// Flush is a no-op — WriteTarget.write() is unbuffered (no partial writes).
    pub fn flush(_: *WriteTargetWriter) WriteError!void {}

    fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *WriteTargetWriter = @fieldParentPtr("io_writer", w);
        var total: usize = 0;
        for (data[0 .. data.len - 1]) |bytes| {
            self.target.write(bytes) catch return error.WriteFailed;
            total += bytes.len;
        }
        const pattern = data[data.len - 1];
        for (0..splat) |_| {
            self.target.write(pattern) catch return error.WriteFailed;
            total += pattern.len;
        }
        return total;
    }
};
