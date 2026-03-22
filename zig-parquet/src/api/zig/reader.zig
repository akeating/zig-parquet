//! Public Reader API - convenience constructors for file/buffer backends.
//!
//! These standalone functions create io/ adapters, heap-allocate them,
//! and delegate to the core transport-neutral constructors.

const std = @import("std");
const FileReader = @import("../../io/file_reader.zig").FileReader;
const BufferReader = @import("../../io/buffer_reader.zig").BufferReader;
const core_dynamic = @import("../../core/dynamic_reader.zig");
const seekable_reader = @import("../../core/seekable_reader.zig");
const parquet_reader = @import("../../core/parquet_reader.zig");

pub const SeekableReader = seekable_reader.SeekableReader;
pub const DynamicReader = core_dynamic.DynamicReader;
pub const DynamicReaderError = core_dynamic.DynamicReaderError;
pub const DynamicReaderOptions = core_dynamic.DynamicReaderOptions;
pub const BackendCleanup = parquet_reader.BackendCleanup;

// -- DynamicReader convenience constructors --

/// Open a Parquet file for schema-agnostic reading. Returns a `DynamicReader`
/// that can read any file without knowing the schema at compile time.
/// Call `deinit()` when done; the caller retains ownership of `file`.
pub fn openFileDynamic(allocator: std.mem.Allocator, file: std.fs.File, options: DynamicReaderOptions) DynamicReaderError!DynamicReader {
    const fr = allocator.create(FileReader) catch return error.OutOfMemory;
    errdefer allocator.destroy(fr);
    fr.* = FileReader.init(file) catch return error.Unseekable;
    var reader = try DynamicReader.initFromSeekable(allocator, fr.reader(), options);
    reader._backend_cleanup = .{
        .ptr = @ptrCast(fr),
        .deinit_fn = &fileReaderCleanup,
    };
    return reader;
}

/// Open a Parquet file from an in-memory buffer for schema-agnostic reading.
/// The caller must ensure `data` outlives the returned `DynamicReader`.
pub fn openBufferDynamic(allocator: std.mem.Allocator, data: []const u8, options: DynamicReaderOptions) DynamicReaderError!DynamicReader {
    const br = allocator.create(BufferReader) catch return error.OutOfMemory;
    errdefer allocator.destroy(br);
    br.* = BufferReader.init(data);
    var reader = try DynamicReader.initFromSeekable(allocator, br.reader(), options);
    reader._backend_cleanup = .{
        .ptr = @ptrCast(br),
        .deinit_fn = &bufferReaderCleanup,
    };
    return reader;
}

// -- Cleanup callbacks --

fn fileReaderCleanup(ptr: *anyopaque, allocator: std.mem.Allocator) void {
    const fr: *FileReader = @ptrCast(@alignCast(ptr));
    allocator.destroy(fr);
}

fn bufferReaderCleanup(ptr: *anyopaque, allocator: std.mem.Allocator) void {
    const br: *BufferReader = @ptrCast(@alignCast(ptr));
    allocator.destroy(br);
}
