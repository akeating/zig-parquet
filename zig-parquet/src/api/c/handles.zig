//! Opaque handle types for the C ABI.
//!
//! ReaderHandle and WriterHandle own all resources needed for their
//! respective Parquet operations and manage cleanup on close.

const std = @import("std");
const ErrorContext = @import("error.zig").ErrorContext;

const format = @import("../../core/format.zig");
const arrow = @import("../../core/arrow.zig");
const arrow_batch = @import("../../core/arrow_batch.zig");
const seekable_reader = @import("../../core/seekable_reader.zig");
const parquet_reader = @import("../../core/parquet_reader.zig");
const write_target = @import("../../core/write_target.zig");
const core_writer = @import("../../core/writer.zig");
const column_def_mod = @import("../../core/column_def.zig");
const core_dynamic = @import("../../core/dynamic_reader.zig");
const value_mod = @import("../../core/value.zig");
const safe = @import("../../core/safe.zig");

const BufferReader = @import("../../io/buffer_reader.zig").BufferReader;
const CallbackReader = @import("../../io/callback_reader.zig").CallbackReader;
const FileReader = @import("../../io/file_reader.zig").FileReader;
const BufferTarget = @import("../../io/buffer_target.zig").BufferTarget;
const CallbackWriter = @import("../../io/callback_writer.zig").CallbackWriter;
const FileTarget = @import("../../io/file_target.zig").FileTarget;

pub const SeekableReader = seekable_reader.SeekableReader;
pub const WriteTarget = write_target.WriteTarget;
pub const ArrowSchema = arrow.ArrowSchema;
pub const ArrowArray = arrow.ArrowArray;
pub const Writer = core_writer.Writer;
pub const ColumnDef = column_def_mod.ColumnDef;

const Allocator = std.mem.Allocator;

/// Backing allocator for C ABI handles.
/// page_allocator is always available on all targets.
const backing_allocator = std.heap.page_allocator;

/// Thunk adapter bridging C callback signatures to Zig's SeekableReader interface.
///
/// C callbacks use `int` return for error codes and out-params for results.
/// Zig's SeekableReader expects `Error!usize` returns. This struct wraps
/// the C function pointers and translates at the boundary.
pub const CCallbackAdapter = struct {
    user_ctx: ?*anyopaque,
    c_read_at: *const fn (?*anyopaque, u64, [*]u8, usize, *usize) callconv(.c) c_int,
    c_size: *const fn (?*anyopaque) callconv(.c) u64,

    const vtable = SeekableReader.VTable{
        .readAt = thunkReadAt,
        .size = thunkSize,
    };

    fn thunkReadAt(ptr: *anyopaque, offset: u64, buf: []u8) SeekableReader.Error!usize {
        const self: *CCallbackAdapter = @ptrCast(@alignCast(ptr));
        var bytes_read: usize = 0;
        const rc = self.c_read_at(self.user_ctx, offset, buf.ptr, buf.len, &bytes_read);
        if (rc != 0) return error.InputOutput;
        return bytes_read;
    }

    fn thunkSize(ptr: *anyopaque) u64 {
        const self: *CCallbackAdapter = @ptrCast(@alignCast(ptr));
        return self.c_size(self.user_ctx);
    }

    pub fn reader(self: *CCallbackAdapter) SeekableReader {
        return .{ .ptr = @ptrCast(self), .vtable = &vtable };
    }
};

/// Thunk adapter bridging C write callback signatures to Zig's WriteTarget interface.
pub const CWriteCallbackAdapter = struct {
    user_ctx: ?*anyopaque,
    c_write: *const fn (?*anyopaque, [*]const u8, usize) callconv(.c) c_int,
    c_close: ?*const fn (?*anyopaque) callconv(.c) c_int,

    const vtable = WriteTarget.VTable{
        .write = thunkWrite,
        .close = thunkClose,
    };

    fn thunkWrite(ptr: *anyopaque, data: []const u8) write_target.WriteError!void {
        const self: *CWriteCallbackAdapter = @ptrCast(@alignCast(ptr));
        const rc = self.c_write(self.user_ctx, data.ptr, data.len);
        if (rc != 0) return error.WriteError;
    }

    fn thunkClose(ptr: *anyopaque) write_target.WriteError!void {
        const self: *CWriteCallbackAdapter = @ptrCast(@alignCast(ptr));
        if (self.c_close) |close_fn| {
            const rc = close_fn(self.user_ctx);
            if (rc != 0) return error.WriteError;
        }
    }

    pub fn target(self: *CWriteCallbackAdapter) WriteTarget {
        return .{ .ptr = @ptrCast(self), .vtable = &vtable };
    }
};

// ============================================================================
// Reader Handle
// ============================================================================

const ReaderBackend = union(enum) {
    buffer: *BufferReader,
    callback: *CCallbackAdapter,
    file: *FileReader,
};

pub const ReaderHandle = struct {
    allocator: Allocator,
    source: SeekableReader,
    metadata: format.FileMetaData,
    footer_data: []u8,
    backend: ReaderBackend,
    err_ctx: ErrorContext = .{},

    pub fn create(allocator: Allocator) !*ReaderHandle {
        return try allocator.create(ReaderHandle);
    }

    pub fn openMemory(data: [*]const u8, len: usize) !*ReaderHandle {
        const allocator = backing_allocator;
        const br = try allocator.create(BufferReader);
        errdefer allocator.destroy(br);
        br.* = BufferReader.init(data[0..len]);

        const info = parquet_reader.parseFooter(allocator, br.reader()) catch |e| return e;

        const handle = try allocator.create(ReaderHandle);
        handle.* = .{
            .allocator = allocator,
            .source = br.reader(),
            .metadata = info.metadata,
            .footer_data = info.footer_data,
            .backend = .{ .buffer = br },
        };
        return handle;
    }

    pub fn openCallbacks(
        ctx: ?*anyopaque,
        read_at_fn: *const fn (?*anyopaque, u64, [*]u8, usize, *usize) callconv(.c) c_int,
        size_fn: *const fn (?*anyopaque) callconv(.c) u64,
    ) !*ReaderHandle {
        const allocator = backing_allocator;
        const adapter = try allocator.create(CCallbackAdapter);
        errdefer allocator.destroy(adapter);
        adapter.* = .{
            .user_ctx = ctx,
            .c_read_at = read_at_fn,
            .c_size = size_fn,
        };

        const info = parquet_reader.parseFooter(allocator, adapter.reader()) catch |e| return e;

        const handle = try allocator.create(ReaderHandle);
        handle.* = .{
            .allocator = allocator,
            .source = adapter.reader(),
            .metadata = info.metadata,
            .footer_data = info.footer_data,
            .backend = .{ .callback = adapter },
        };
        return handle;
    }

    pub fn openFile(path: [*:0]const u8) !*ReaderHandle {
        const allocator = backing_allocator;
        const fr = try allocator.create(FileReader);
        errdefer allocator.destroy(fr);

        const file = std.fs.cwd().openFileZ(path, .{}) catch return error.InputOutput;
        fr.* = FileReader.init(file) catch |e| {
            file.close();
            return e;
        };

        const info = parquet_reader.parseFooter(allocator, fr.reader()) catch |e| {
            file.close();
            allocator.destroy(fr);
            return e;
        };

        const handle = try allocator.create(ReaderHandle);
        handle.* = .{
            .allocator = allocator,
            .source = fr.reader(),
            .metadata = info.metadata,
            .footer_data = info.footer_data,
            .backend = .{ .file = fr },
        };
        return handle;
    }

    pub fn close(self: *ReaderHandle) void {
        const allocator = self.allocator;

        // Free schema element names
        for (self.metadata.schema) |elem| {
            if (elem.name.len > 0) allocator.free(elem.name);
        }
        allocator.free(self.metadata.schema);

        // Free row groups
        for (self.metadata.row_groups) |rg| {
            for (rg.columns) |col| {
                if (col.meta_data) |meta| {
                    allocator.free(meta.encodings);
                    for (meta.path_in_schema) |path| allocator.free(path);
                    allocator.free(meta.path_in_schema);
                    if (meta.statistics) |stats| {
                        if (stats.max) |m| allocator.free(m);
                        if (stats.min) |m| allocator.free(m);
                        if (stats.max_value) |m| allocator.free(m);
                        if (stats.min_value) |m| allocator.free(m);
                    }
                    if (meta.geospatial_statistics) |geo_stats| {
                        if (geo_stats.geospatial_types) |gt| allocator.free(gt);
                    }
                }
            }
            allocator.free(rg.columns);
        }
        allocator.free(self.metadata.row_groups);

        if (self.metadata.key_value_metadata) |kvs| {
            for (kvs) |kv| {
                allocator.free(kv.key);
                if (kv.value) |v| allocator.free(v);
            }
            allocator.free(kvs);
        }
        if (self.metadata.created_by) |cb| allocator.free(cb);
        allocator.free(self.footer_data);

        switch (self.backend) {
            .buffer => |br| allocator.destroy(br),
            .callback => |cb| allocator.destroy(cb),
            .file => |fr| {
                fr.file.close();
                allocator.destroy(fr);
            },
        }

        allocator.destroy(self);
    }
};

// ============================================================================
// Writer Handle
// ============================================================================

const WriterBackend = union(enum) {
    buffer: *BufferTarget,
    callback: *CWriteCallbackAdapter,
    file: *FileTarget,
};

pub const WriterHandle = struct {
    allocator: Allocator,
    writer: ?*Writer = null,
    backend: WriterBackend,
    column_defs: ?[]ColumnDef = null,
    column_names: ?[][:0]u8 = null,
    err_ctx: ErrorContext = .{},
    transport_failed: bool = false,
    row_group_size: ?usize = null,

    pub fn openMemory() !*WriterHandle {
        const allocator = backing_allocator;
        const bt = try allocator.create(BufferTarget);
        errdefer allocator.destroy(bt);
        bt.* = BufferTarget.init(allocator);

        const handle = try allocator.create(WriterHandle);
        handle.* = .{
            .allocator = allocator,
            .backend = .{ .buffer = bt },
        };
        return handle;
    }

    pub fn openCallbacks(
        ctx: ?*anyopaque,
        write_fn: *const fn (?*anyopaque, [*]const u8, usize) callconv(.c) c_int,
        close_fn: ?*const fn (?*anyopaque) callconv(.c) c_int,
    ) !*WriterHandle {
        const allocator = backing_allocator;
        const adapter = try allocator.create(CWriteCallbackAdapter);
        errdefer allocator.destroy(adapter);
        adapter.* = .{
            .user_ctx = ctx,
            .c_write = write_fn,
            .c_close = close_fn,
        };

        const handle = try allocator.create(WriterHandle);
        handle.* = .{
            .allocator = allocator,
            .backend = .{ .callback = adapter },
        };
        return handle;
    }

    pub fn openFile(path: [*:0]const u8) !*WriterHandle {
        const allocator = backing_allocator;
        const ft = try allocator.create(FileTarget);
        errdefer allocator.destroy(ft);

        const file = std.fs.cwd().createFileZ(path, .{}) catch return error.InputOutput;
        ft.* = FileTarget.init(file);

        const handle = try allocator.create(WriterHandle);
        handle.* = .{
            .allocator = allocator,
            .backend = .{ .file = ft },
        };
        return handle;
    }

    pub fn getTarget(self: *WriterHandle) WriteTarget {
        return switch (self.backend) {
            .buffer => |bt| bt.target(),
            .callback => |cb| cb.target(),
            .file => |ft| ft.target(),
        };
    }

    /// Set schema from Arrow and create the internal Writer.
    pub fn setSchema(self: *WriterHandle, schema: *const ArrowSchema) !void {
        if (self.writer != null) return error.InvalidState;

        const allocator = self.allocator;
        const col_defs = try arrow_batch.importSchemaFromArrow(allocator, schema);
        errdefer arrow_batch.freeImportedColumnDefs(allocator, col_defs);

        // Dupe column names so they outlive the ArrowSchema
        var names = try allocator.alloc([:0]u8, col_defs.len);
        errdefer {
            for (names[0..col_defs.len]) |n| allocator.free(n);
            allocator.free(names);
        }
        for (col_defs, 0..) |*cd, i| {
            names[i] = try allocator.dupeZ(u8, cd.name);
            cd.name = names[i];
        }

        const w = try allocator.create(Writer);
        errdefer allocator.destroy(w);
        w.* = try Writer.initWithTarget(allocator, self.getTarget(), col_defs);

        self.writer = w;
        self.column_defs = col_defs;
        self.column_names = names;
    }

    pub fn close(self: *WriterHandle) !void {
        if (self.writer) |w| {
            try w.close();
        }
    }

    pub fn deinit(self: *WriterHandle) void {
        const allocator = self.allocator;

        if (self.writer) |w| {
            w.deinit();
            allocator.destroy(w);
        }

        if (self.column_names) |names| {
            for (names) |n| allocator.free(n);
            allocator.free(names);
        }
        if (self.column_defs) |defs| allocator.free(defs);

        switch (self.backend) {
            .buffer => |bt| {
                bt.deinit();
                allocator.destroy(bt);
            },
            .callback => |cb| allocator.destroy(cb),
            .file => |ft| {
                ft.file.close();
                allocator.destroy(ft);
            },
        }

        allocator.destroy(self);
    }
};

// ============================================================================
// Row Reader Handle (cursor-based, non-Arrow)
// ============================================================================

pub const RowReaderHandle = struct {
    allocator: Allocator,
    reader: core_dynamic.DynamicReader,
    current_rows: ?[]value_mod.Row = null,
    cursor: usize = 0,
    col_names: [][:0]u8 = &.{},
    num_top_columns: usize = 0,
    auto_rg_index: usize = 0,
    err_ctx: ErrorContext = .{},

    pub fn openMemory(data: [*]const u8, len: usize) !*RowReaderHandle {
        const allocator = backing_allocator;
        const br = try allocator.create(BufferReader);
        br.* = BufferReader.init(data[0..len]);

        var dr = core_dynamic.DynamicReader.initFromSeekable(allocator, br.reader(), .{}) catch |e| {
            allocator.destroy(br);
            return e;
        };
        dr._backend_cleanup = .{ .ptr = @ptrCast(br), .deinit_fn = &bufferCleanup };
        return finishInit(allocator, dr);
    }

    pub fn openCallbacks(
        ctx: ?*anyopaque,
        read_at_fn: *const fn (?*anyopaque, u64, [*]u8, usize, *usize) callconv(.c) c_int,
        size_fn: *const fn (?*anyopaque) callconv(.c) u64,
    ) !*RowReaderHandle {
        const allocator = backing_allocator;
        const adapter = try allocator.create(CCallbackAdapter);
        adapter.* = .{ .user_ctx = ctx, .c_read_at = read_at_fn, .c_size = size_fn };

        var dr = core_dynamic.DynamicReader.initFromSeekable(allocator, adapter.reader(), .{}) catch |e| {
            allocator.destroy(adapter);
            return e;
        };
        dr._backend_cleanup = .{ .ptr = @ptrCast(adapter), .deinit_fn = &callbackAdapterCleanup };
        return finishInit(allocator, dr);
    }

    pub fn openFile(path: [*:0]const u8) !*RowReaderHandle {
        const allocator = backing_allocator;
        const fr = try allocator.create(FileReader);

        const file = std.fs.cwd().openFileZ(path, .{}) catch return error.InputOutput;
        fr.* = FileReader.init(file) catch |e| {
            file.close();
            return e;
        };

        var dr = core_dynamic.DynamicReader.initFromSeekable(allocator, fr.reader(), .{}) catch |e| {
            file.close();
            allocator.destroy(fr);
            return e;
        };
        dr._backend_cleanup = .{ .ptr = @ptrCast(fr), .deinit_fn = &fileReaderCleanup };
        return finishInit(allocator, dr);
    }

    fn finishInit(allocator: Allocator, dr: core_dynamic.DynamicReader) !*RowReaderHandle {
        const handle = allocator.create(RowReaderHandle) catch |e| {
            var dr_mut = dr;
            dr_mut.deinit();
            return e;
        };
        handle.* = .{ .allocator = allocator, .reader = dr };
        handle.cacheColumnNames() catch |e| {
            handle.reader.deinit();
            allocator.destroy(handle);
            return e;
        };
        return handle;
    }

    fn cacheColumnNames(self: *RowReaderHandle) !void {
        const schema = self.reader.metadata.schema;
        if (schema.len <= 1) {
            self.num_top_columns = 0;
            return;
        }
        const num_top = safe.castTo(usize, schema[0].num_children orelse 0) catch 0;
        self.num_top_columns = num_top;
        if (num_top == 0) return;

        self.col_names = try self.allocator.alloc([:0]u8, num_top);
        var col: usize = 0;
        var idx: usize = 1;
        while (col < num_top and idx < schema.len) {
            self.col_names[col] = try self.allocator.dupeZ(u8, schema[idx].name);
            col += 1;
            idx = skipSubtree(schema, idx);
        }
    }

    fn skipSubtree(schema: []const format.SchemaElement, start: usize) usize {
        var idx = start;
        var to_visit: usize = 1;
        while (to_visit > 0 and idx < schema.len) {
            to_visit -= 1;
            if (schema[idx].num_children) |nc| {
                to_visit += safe.castTo(usize, nc) catch 0;
            }
            idx += 1;
        }
        return idx;
    }

    pub fn freeCurrentRows(self: *RowReaderHandle) void {
        if (self.current_rows) |rows| {
            for (rows) |row| row.deinit();
            self.allocator.free(rows);
            self.current_rows = null;
        }
        self.cursor = 0;
    }

    pub fn readRowGroup(self: *RowReaderHandle, rg_index: usize) !void {
        self.freeCurrentRows();
        self.current_rows = try self.reader.readAllRows(rg_index);
        self.cursor = 0;
    }

    pub fn readRowGroupProjected(self: *RowReaderHandle, rg_index: usize, col_indices: []const usize) !void {
        self.freeCurrentRows();
        self.current_rows = try self.reader.readRowsProjected(rg_index, col_indices);
        self.cursor = 0;
    }

    /// Advance cursor. Returns true if a row is available.
    pub fn next(self: *RowReaderHandle) bool {
        const rows = self.current_rows orelse return false;
        if (self.cursor >= rows.len) return false;
        self.cursor += 1;
        return true;
    }

    /// Get the current row (after a successful next()).
    pub fn currentRow(self: *const RowReaderHandle) ?*const value_mod.Row {
        const rows = self.current_rows orelse return null;
        if (self.cursor == 0) return null;
        const idx = self.cursor - 1;
        if (idx >= rows.len) return null;
        return &rows[idx];
    }

    /// Advance cursor, auto-loading the next row group when needed.
    /// Returns true if a row is available, false when all row groups are exhausted.
    pub fn nextAll(self: *RowReaderHandle) !bool {
        if (self.next()) return true;

        while (self.auto_rg_index < self.reader.metadata.row_groups.len) {
            const rg = self.auto_rg_index;
            self.auto_rg_index += 1;
            try self.readRowGroup(rg);
            if (self.next()) return true;
        }
        return false;
    }

    pub fn close(self: *RowReaderHandle) void {
        self.freeCurrentRows();
        for (self.col_names) |name| self.allocator.free(name);
        if (self.col_names.len > 0) self.allocator.free(self.col_names);
        self.reader.deinit();
        self.allocator.destroy(self);
    }

    fn bufferCleanup(ptr: *anyopaque, allocator: Allocator) void {
        const br: *BufferReader = @ptrCast(@alignCast(ptr));
        allocator.destroy(br);
    }

    fn callbackAdapterCleanup(ptr: *anyopaque, allocator: Allocator) void {
        const adapter: *CCallbackAdapter = @ptrCast(@alignCast(ptr));
        allocator.destroy(adapter);
    }

    fn fileReaderCleanup(ptr: *anyopaque, allocator: Allocator) void {
        const fr: *FileReader = @ptrCast(@alignCast(ptr));
        fr.file.close();
        allocator.destroy(fr);
    }
};

// ============================================================================
// Row Writer Handle (cursor-based, non-Arrow, multi-row-group)
// Thin wrapper around DynamicWriter for C ABI usage.
// ============================================================================

const err = @import("error.zig");
const dynamic_writer_mod = @import("../../core/dynamic_writer.zig");
const schema_mod = @import("../../core/schema.zig");
const SchemaNode = schema_mod.SchemaNode;

pub const DynamicWriter = dynamic_writer_mod.DynamicWriter;
pub const ColumnType = dynamic_writer_mod.ColumnType;
pub const TypeInfo = dynamic_writer_mod.TypeInfo;

pub fn typeInfoFromZpType(t: c_int) ?TypeInfo {
    return switch (t) {
        err.ZP_TYPE_BOOL => TypeInfo.bool_,
        err.ZP_TYPE_INT32 => TypeInfo.int32,
        err.ZP_TYPE_INT64 => TypeInfo.int64,
        err.ZP_TYPE_FLOAT => TypeInfo.float_,
        err.ZP_TYPE_DOUBLE => TypeInfo.double_,
        err.ZP_TYPE_BYTES => TypeInfo.bytes,
        err.ZP_TYPE_STRING => TypeInfo.string,
        err.ZP_TYPE_DATE => TypeInfo.date,
        err.ZP_TYPE_TIMESTAMP_MILLIS => TypeInfo.timestamp_millis,
        err.ZP_TYPE_TIMESTAMP_MICROS => TypeInfo.timestamp_micros,
        err.ZP_TYPE_TIMESTAMP_NANOS => TypeInfo.timestamp_nanos,
        err.ZP_TYPE_TIME_MILLIS => TypeInfo.time_millis,
        err.ZP_TYPE_TIME_MICROS => TypeInfo.time_micros,
        err.ZP_TYPE_TIME_NANOS => TypeInfo.time_nanos,
        err.ZP_TYPE_INT8 => TypeInfo.int8,
        err.ZP_TYPE_INT16 => TypeInfo.int16,
        err.ZP_TYPE_UINT8 => TypeInfo.uint8,
        err.ZP_TYPE_UINT16 => TypeInfo.uint16,
        err.ZP_TYPE_UINT32 => TypeInfo.uint32,
        err.ZP_TYPE_UINT64 => TypeInfo.uint64,
        err.ZP_TYPE_UUID => TypeInfo.uuid,
        err.ZP_TYPE_JSON => TypeInfo.json,
        err.ZP_TYPE_ENUM => TypeInfo.enum_,
        err.ZP_TYPE_FLOAT16 => TypeInfo.float16,
        err.ZP_TYPE_BSON => TypeInfo.bson,
        err.ZP_TYPE_INTERVAL => TypeInfo.interval,
        err.ZP_TYPE_GEOMETRY => TypeInfo.geometry,
        err.ZP_TYPE_GEOGRAPHY => TypeInfo.geography,
        else => null,
    };
}

pub const RowWriterHandle = struct {
    allocator: Allocator,
    backend: WriterBackend,
    writer: DynamicWriter,
    err_ctx: ErrorContext = .{},
    transport_failed: bool = false,

    pub fn openMemory() !*RowWriterHandle {
        const allocator = backing_allocator;
        const bt = try allocator.create(BufferTarget);
        errdefer allocator.destroy(bt);
        bt.* = BufferTarget.init(allocator);

        const handle = try allocator.create(RowWriterHandle);
        handle.* = .{
            .allocator = allocator,
            .backend = .{ .buffer = bt },
            .writer = DynamicWriter.init(allocator, bt.target()),
        };
        return handle;
    }

    pub fn openCallbacksC(
        ctx: ?*anyopaque,
        write_fn: *const fn (?*anyopaque, [*]const u8, usize) callconv(.c) c_int,
        close_fn: ?*const fn (?*anyopaque) callconv(.c) c_int,
    ) !*RowWriterHandle {
        const allocator = backing_allocator;
        const adapter = try allocator.create(CWriteCallbackAdapter);
        errdefer allocator.destroy(adapter);
        adapter.* = .{ .user_ctx = ctx, .c_write = write_fn, .c_close = close_fn };

        const handle = try allocator.create(RowWriterHandle);
        handle.* = .{
            .allocator = allocator,
            .backend = .{ .callback = adapter },
            .writer = DynamicWriter.init(allocator, adapter.target()),
        };
        return handle;
    }

    pub fn openFile(path: [*:0]const u8) !*RowWriterHandle {
        const allocator = backing_allocator;
        const ft = try allocator.create(FileTarget);
        errdefer allocator.destroy(ft);

        const file = std.fs.cwd().createFileZ(path, .{}) catch return error.InputOutput;
        ft.* = FileTarget.init(file);

        const handle = try allocator.create(RowWriterHandle);
        handle.* = .{
            .allocator = allocator,
            .backend = .{ .file = ft },
            .writer = DynamicWriter.init(allocator, ft.target()),
        };
        return handle;
    }

    // -- Delegated methods --

    pub fn addColumn(self: *RowWriterHandle, name: [*:0]const u8, info: TypeInfo) !void {
        self.writer.addColumn(std.mem.sliceTo(name, 0), info, .{}) catch |e| return e;
    }

    pub fn addColumnNested(self: *RowWriterHandle, name: [*:0]const u8, node: *const SchemaNode) !void {
        self.writer.addColumnNested(std.mem.sliceTo(name, 0), node, .{}) catch |e| return e;
    }

    pub fn allocSchemaNode(self: *RowWriterHandle, node: SchemaNode) !*const SchemaNode {
        return self.writer.allocSchemaNode(node) catch |e| return e;
    }

    pub fn allocSchemaFields(self: *RowWriterHandle, count: usize) ![]SchemaNode.Field {
        return self.writer.allocSchemaFields(count) catch |e| return e;
    }

    pub fn dupeSchemaName(self: *RowWriterHandle, name: []const u8) ![]const u8 {
        return self.writer.dupeSchemaName(name) catch |e| return e;
    }

    pub fn begin(self: *RowWriterHandle) !void {
        self.writer.begin() catch |e| return e;
    }

    pub fn setNull(self: *RowWriterHandle, col: usize) !void {
        self.writer.setNull(col) catch |e| return e;
    }

    pub fn setBool(self: *RowWriterHandle, col: usize, val: bool) !void {
        self.writer.setBool(col, val) catch |e| return e;
    }

    pub fn setInt32(self: *RowWriterHandle, col: usize, val: i32) !void {
        self.writer.setInt32(col, val) catch |e| return e;
    }

    pub fn setInt64(self: *RowWriterHandle, col: usize, val: i64) !void {
        self.writer.setInt64(col, val) catch |e| return e;
    }

    pub fn setFloat(self: *RowWriterHandle, col: usize, val: f32) !void {
        self.writer.setFloat(col, val) catch |e| return e;
    }

    pub fn setDouble(self: *RowWriterHandle, col: usize, val: f64) !void {
        self.writer.setDouble(col, val) catch |e| return e;
    }

    pub fn setBytes(self: *RowWriterHandle, col: usize, data: [*]const u8, len: usize) !void {
        self.writer.setBytes(col, data[0..len]) catch |e| return e;
    }

    pub fn beginList(self: *RowWriterHandle, col: usize) !void {
        self.writer.beginList(col) catch |e| return e;
    }

    pub fn endList(self: *RowWriterHandle, col: usize) !void {
        self.writer.endList(col) catch |e| return e;
    }

    pub fn beginStruct(self: *RowWriterHandle, col: usize) !void {
        self.writer.beginStruct(col) catch |e| return e;
    }

    pub fn setStructField(self: *RowWriterHandle, col: usize, field_idx: usize, val: value_mod.Value) !void {
        self.writer.setStructField(col, field_idx, val) catch |e| return e;
    }

    pub fn setStructFieldBytes(self: *RowWriterHandle, col: usize, field_idx: usize, data: [*]const u8, len: usize) !void {
        self.writer.setStructFieldBytes(col, field_idx, data[0..len]) catch |e| return e;
    }

    pub fn endStruct(self: *RowWriterHandle, col: usize) !void {
        self.writer.endStruct(col) catch |e| return e;
    }

    pub fn beginMap(self: *RowWriterHandle, col: usize) !void {
        self.writer.beginMap(col) catch |e| return e;
    }

    pub fn beginMapEntry(self: *RowWriterHandle, col: usize) !void {
        self.writer.beginMapEntry(col) catch |e| return e;
    }

    pub fn endMapEntry(self: *RowWriterHandle, col: usize) !void {
        self.writer.endMapEntry(col) catch |e| return e;
    }

    pub fn endMap(self: *RowWriterHandle, col: usize) !void {
        self.writer.endMap(col) catch |e| return e;
    }

    pub fn appendNestedValue(self: *RowWriterHandle, col: usize, val: value_mod.Value) !void {
        self.writer.appendNestedValue(col, val) catch |e| return e;
    }

    pub fn appendNestedBytes(self: *RowWriterHandle, col: usize, data: [*]const u8, len: usize) !void {
        self.writer.appendNestedBytes(col, data[0..len]) catch |e| return e;
    }

    pub fn addRow(self: *RowWriterHandle) !void {
        self.writer.addRow() catch |e| return e;
    }

    pub fn flush(self: *RowWriterHandle) !void {
        self.writer.flush() catch |e| return e;
    }

    pub fn setKvMetadata(self: *RowWriterHandle, key: []const u8, value: ?[]const u8) !void {
        self.writer.setKvMetadata(key, value) catch |e| return e;
    }

    pub fn writerClose(self: *RowWriterHandle) !void {
        self.writer.close() catch |e| return e;
    }

    pub fn deinit(self: *RowWriterHandle) void {
        const allocator = self.allocator;
        self.writer.deinit();

        switch (self.backend) {
            .buffer => |bt| {
                bt.deinit();
                allocator.destroy(bt);
            },
            .callback => |cb| allocator.destroy(cb),
            .file => |ft| {
                ft.file.close();
                allocator.destroy(ft);
            },
        }

        allocator.destroy(self);
    }
};
