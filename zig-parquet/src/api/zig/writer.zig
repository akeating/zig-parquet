//! Public Writer API - convenience constructors for file/buffer backends.
//!
//! These standalone functions create io/ adapters, heap-allocate them,
//! and delegate to the core transport-neutral constructors.

const std = @import("std");
const FileTarget = @import("../../io/file_target.zig").FileTarget;
const BufferTarget = @import("../../io/buffer_target.zig").BufferTarget;
const core_writer = @import("../../core/writer.zig");
const core_dynamic_writer = @import("../../core/dynamic_writer.zig");
const write_target = @import("../../core/write_target.zig");
const parquet_reader = @import("../../core/parquet_reader.zig");

pub const Writer = core_writer.Writer;
pub const WriterError = core_writer.WriterError;
pub const ColumnDef = core_writer.ColumnDef;
pub const DynamicWriter = core_dynamic_writer.DynamicWriter;
pub const DynamicWriterError = core_dynamic_writer.DynamicWriterError;
pub const WriteTarget = write_target.WriteTarget;
pub const BackendCleanup = parquet_reader.BackendCleanup;

// -- Writer convenience constructors --

/// Create a column-oriented Parquet writer that writes to a file.
/// Define the schema upfront via `columns`, then write each column
/// with `writeColumnOptional`. Call `close()` to finalize the file.
/// The caller retains ownership of `file`.
pub fn writeToFile(
    allocator: std.mem.Allocator,
    file: std.fs.File,
    columns: []const ColumnDef,
) WriterError!Writer {
    const ft = allocator.create(FileTarget) catch return error.OutOfMemory;
    errdefer allocator.destroy(ft);
    ft.* = FileTarget.init(file);
    var writer = try Writer.initWithTarget(allocator, ft.target(), columns);
    writer._backend_cleanup = .{
        .ptr = @ptrCast(ft),
        .deinit_fn = &fileTargetCleanup,
    };
    return writer;
}

/// Create a column-oriented Parquet writer that writes to an in-memory buffer.
/// After calling `close()`, retrieve the bytes with `toOwnedSlice()`.
pub fn writeToBuffer(
    allocator: std.mem.Allocator,
    columns: []const ColumnDef,
) WriterError!Writer {
    const bt = allocator.create(BufferTarget) catch return error.OutOfMemory;
    errdefer allocator.destroy(bt);
    bt.* = BufferTarget.init(allocator);
    var writer = try Writer.initWithTarget(allocator, bt.target(), columns);
    writer._backend_cleanup = .{
        .ptr = @ptrCast(bt),
        .deinit_fn = &bufferTargetCleanup,
    };
    writer._to_owned_slice_fn = &bufferToOwnedSlice;
    writer._to_owned_slice_ctx = @ptrCast(bt);
    return writer;
}

// -- DynamicWriter convenience constructors --

/// Create a dynamic row-oriented Parquet writer that writes to a file.
/// Define the schema at runtime with `addColumn()` / `addColumnNested()`,
/// then call `begin()` to finalize the schema, write rows, and `close()`.
/// The caller retains ownership of `file`.
pub fn createFileDynamic(
    allocator: std.mem.Allocator,
    file: std.fs.File,
) DynamicWriterError!DynamicWriter {
    const ft = allocator.create(FileTarget) catch return error.OutOfMemory;
    errdefer allocator.destroy(ft);
    ft.* = FileTarget.init(file);
    var writer = DynamicWriter.init(allocator, ft.target());
    writer._backend_cleanup = .{
        .ptr = @ptrCast(ft),
        .deinit_fn = &fileTargetCleanup,
    };
    return writer;
}

/// Create a dynamic row-oriented Parquet writer that writes to an in-memory buffer.
/// After calling `close()`, retrieve the bytes with `toOwnedSlice()`.
pub fn createBufferDynamic(
    allocator: std.mem.Allocator,
) DynamicWriterError!DynamicWriter {
    const bt = allocator.create(BufferTarget) catch return error.OutOfMemory;
    errdefer allocator.destroy(bt);
    bt.* = BufferTarget.init(allocator);
    var writer = DynamicWriter.init(allocator, bt.target());
    writer._backend_cleanup = .{
        .ptr = @ptrCast(bt),
        .deinit_fn = &bufferTargetCleanup,
    };
    writer._to_owned_slice_fn = &bufferToOwnedSlice;
    writer._to_owned_slice_ctx = @ptrCast(bt);
    return writer;
}

// -- Cleanup and slice callbacks --

fn fileTargetCleanup(ptr: *anyopaque, allocator: std.mem.Allocator) void {
    const ft: *FileTarget = @ptrCast(@alignCast(ptr));
    ft.deinit();
    allocator.destroy(ft);
}

fn bufferTargetCleanup(ptr: *anyopaque, allocator: std.mem.Allocator) void {
    const bt: *BufferTarget = @ptrCast(@alignCast(ptr));
    bt.deinit();
    allocator.destroy(bt);
}

fn bufferToOwnedSlice(ptr: *anyopaque) error{OutOfMemory}![]u8 {
    const bt: *BufferTarget = @ptrCast(@alignCast(ptr));
    return bt.toOwnedSlice() catch return error.OutOfMemory;
}
