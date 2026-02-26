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
        errdefer allocator.free(col_defs);

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
// ============================================================================

const err = @import("error.zig");
const types = @import("../../core/types.zig");
const column_writer = @import("../../core/column_writer.zig");
const thrift = @import("../../core/thrift/mod.zig");
const nested_mod = @import("../../core/nested.zig");
const schema_mod = @import("../../core/schema.zig");
const WriteTargetWriter = write_target.WriteTargetWriter;
const SchemaNode = schema_mod.SchemaNode;

const RowGroupMeta = struct {
    columns: []format.ColumnChunk,
    num_rows: i64,
    total_byte_size: i64,
    file_offset: i64,
};

pub const ColumnType = enum {
    bool_,
    int32,
    int64,
    float_,
    double_,
    bytes,
    fixed_bytes,
    nested,

    pub fn toPhysicalType(self: ColumnType) ?format.PhysicalType {
        return switch (self) {
            .bool_ => .boolean,
            .int32 => .int32,
            .int64 => .int64,
            .float_ => .float,
            .double_ => .double,
            .bytes => .byte_array,
            .fixed_bytes => .fixed_len_byte_array,
            .nested => null,
        };
    }
};

pub const TypeInfo = struct {
    physical: ColumnType,
    logical: ?format.LogicalType,
    type_length: ?i32,

    pub fn fromZpType(t: c_int) ?TypeInfo {
        return switch (t) {
            err.ZP_TYPE_BOOL => .{ .physical = .bool_, .logical = null, .type_length = null },
            err.ZP_TYPE_INT32 => .{ .physical = .int32, .logical = null, .type_length = null },
            err.ZP_TYPE_INT64 => .{ .physical = .int64, .logical = null, .type_length = null },
            err.ZP_TYPE_FLOAT => .{ .physical = .float_, .logical = null, .type_length = null },
            err.ZP_TYPE_DOUBLE => .{ .physical = .double_, .logical = null, .type_length = null },
            err.ZP_TYPE_BYTES => .{ .physical = .bytes, .logical = null, .type_length = null },

            err.ZP_TYPE_STRING => .{ .physical = .bytes, .logical = .string, .type_length = null },
            err.ZP_TYPE_DATE => .{ .physical = .int32, .logical = .date, .type_length = null },
            err.ZP_TYPE_TIMESTAMP_MILLIS => .{ .physical = .int64, .logical = .{ .timestamp = .{ .is_adjusted_to_utc = true, .unit = .millis } }, .type_length = null },
            err.ZP_TYPE_TIMESTAMP_MICROS => .{ .physical = .int64, .logical = .{ .timestamp = .{ .is_adjusted_to_utc = true, .unit = .micros } }, .type_length = null },
            err.ZP_TYPE_TIMESTAMP_NANOS => .{ .physical = .int64, .logical = .{ .timestamp = .{ .is_adjusted_to_utc = true, .unit = .nanos } }, .type_length = null },
            err.ZP_TYPE_TIME_MILLIS => .{ .physical = .int32, .logical = .{ .time = .{ .is_adjusted_to_utc = false, .unit = .millis } }, .type_length = null },
            err.ZP_TYPE_TIME_MICROS => .{ .physical = .int64, .logical = .{ .time = .{ .is_adjusted_to_utc = false, .unit = .micros } }, .type_length = null },
            err.ZP_TYPE_TIME_NANOS => .{ .physical = .int64, .logical = .{ .time = .{ .is_adjusted_to_utc = false, .unit = .nanos } }, .type_length = null },
            err.ZP_TYPE_INT8 => .{ .physical = .int32, .logical = .{ .int = .{ .bit_width = 8, .is_signed = true } }, .type_length = null },
            err.ZP_TYPE_INT16 => .{ .physical = .int32, .logical = .{ .int = .{ .bit_width = 16, .is_signed = true } }, .type_length = null },
            err.ZP_TYPE_UINT8 => .{ .physical = .int32, .logical = .{ .int = .{ .bit_width = 8, .is_signed = false } }, .type_length = null },
            err.ZP_TYPE_UINT16 => .{ .physical = .int32, .logical = .{ .int = .{ .bit_width = 16, .is_signed = false } }, .type_length = null },
            err.ZP_TYPE_UINT32 => .{ .physical = .int32, .logical = .{ .int = .{ .bit_width = 32, .is_signed = false } }, .type_length = null },
            err.ZP_TYPE_UINT64 => .{ .physical = .int64, .logical = .{ .int = .{ .bit_width = 64, .is_signed = false } }, .type_length = null },
            err.ZP_TYPE_UUID => .{ .physical = .fixed_bytes, .logical = .uuid, .type_length = 16 },
            err.ZP_TYPE_JSON => .{ .physical = .bytes, .logical = .json, .type_length = null },
            err.ZP_TYPE_ENUM => .{ .physical = .bytes, .logical = .enum_, .type_length = null },
            err.ZP_TYPE_FLOAT16 => .{ .physical = .fixed_bytes, .logical = .float16, .type_length = 2 },
            err.ZP_TYPE_BSON => .{ .physical = .bytes, .logical = .bson, .type_length = null },
            err.ZP_TYPE_INTERVAL => .{ .physical = .fixed_bytes, .logical = null, .type_length = 12 },
            err.ZP_TYPE_GEOMETRY => .{ .physical = .bytes, .logical = .{ .geometry = .{} }, .type_length = null },
            err.ZP_TYPE_GEOGRAPHY => .{ .physical = .bytes, .logical = .{ .geography = .{} }, .type_length = null },
            else => null,
        };
    }

    pub fn forDecimal(precision: i32, scale: i32) TypeInfo {
        const logical: format.LogicalType = .{ .decimal = .{ .precision = precision, .scale = scale } };
        if (precision <= 9) {
            return .{ .physical = .int32, .logical = logical, .type_length = null };
        } else if (precision <= 18) {
            return .{ .physical = .int64, .logical = logical, .type_length = null };
        } else {
            const byte_len: i32 = @divTrunc(precision + 1, 2);
            return .{ .physical = .fixed_bytes, .logical = logical, .type_length = byte_len };
        }
    }
};

pub const PendingColumn = struct {
    name: [:0]u8,
    col_type: ColumnType,
    logical_type: ?format.LogicalType = null,
    type_length: ?i32 = null,
    codec: ?format.CompressionCodec = null,
    schema_node: ?*const SchemaNode = null,
};

/// Per-column builder state for constructing nested values.
/// Uses a stack to handle arbitrarily deep nesting (list-of-struct-of-list...).
pub const ValueBuilder = struct {
    const FrameKind = enum { list, struct_, map, map_entry };

    const Frame = struct {
        kind: FrameKind,
        items: std.ArrayListUnmanaged(value_mod.Value) = .empty,
        map_entries: std.ArrayListUnmanaged(value_mod.Value.MapEntryValue) = .empty,
        struct_fields: std.ArrayListUnmanaged(value_mod.Value.FieldValue) = .empty,
        entry_key: ?value_mod.Value = null,
    };

    stack: std.ArrayListUnmanaged(Frame) = .empty,

    fn push(self: *ValueBuilder, allocator: Allocator, kind: FrameKind) !void {
        try self.stack.append(allocator, .{ .kind = kind });
    }

    fn top(self: *ValueBuilder) ?*Frame {
        if (self.stack.items.len == 0) return null;
        return &self.stack.items[self.stack.items.len - 1];
    }

    fn pop(self: *ValueBuilder) ?Frame {
        if (self.stack.items.len == 0) return null;
        return self.stack.pop();
    }

    fn appendValue(self: *ValueBuilder, allocator: Allocator, val: value_mod.Value) !void {
        const frame = self.top() orelse return error.InvalidState;
        switch (frame.kind) {
            .list => try frame.items.append(allocator, val),
            .map_entry => {
                if (frame.entry_key == null) {
                    frame.entry_key = val;
                } else {
                    try frame.map_entries.append(allocator, .{
                        .key = frame.entry_key.?,
                        .value = val,
                    });
                    frame.entry_key = null;
                }
            },
            else => return error.InvalidState,
        }
    }

    fn isEmpty(self: *const ValueBuilder) bool {
        return self.stack.items.len == 0;
    }

    fn deinit(self: *ValueBuilder, allocator: Allocator) void {
        for (self.stack.items) |*frame| {
            frame.items.deinit(allocator);
            frame.map_entries.deinit(allocator);
            frame.struct_fields.deinit(allocator);
        }
        self.stack.deinit(allocator);
    }
};

pub const RowWriterHandle = struct {
    allocator: Allocator,
    backend: WriterBackend,
    target_writer: WriteTargetWriter = undefined,

    pending_columns: std.ArrayListUnmanaged(PendingColumn) = .empty,
    default_codec: format.CompressionCodec = .uncompressed,
    began: bool = false,

    num_columns: usize = 0,
    column_types: []ColumnType = &.{},
    column_logical_types: []?format.LogicalType = &.{},
    column_type_lengths: []?i32 = &.{},
    column_codecs: []format.CompressionCodec = &.{},
    column_names_owned: [][:0]u8 = &.{},
    current_row: []value_mod.Value = &.{},
    row_set: []bool = &.{},
    column_buffers: []std.ArrayListUnmanaged(value_mod.Value) = &.{},
    column_schema_nodes: []?*const SchemaNode = &.{},
    value_builders: []ValueBuilder = &.{},
    bytes_arena: std.heap.ArenaAllocator = std.heap.ArenaAllocator.init(backing_allocator),
    schema_arena: std.heap.ArenaAllocator = std.heap.ArenaAllocator.init(backing_allocator),

    row_groups: std.ArrayListUnmanaged(RowGroupMeta) = .empty,
    current_offset: i64 = 4, // after PAR1 magic

    err_ctx: ErrorContext = .{},
    transport_failed: bool = false,

    row_group_row_limit: ?usize = null,
    rows_in_current_group: usize = 0,
    kv_metadata: std.ArrayListUnmanaged(format.KeyValue) = .empty,

    // -- Open --

    pub fn openMemory() !*RowWriterHandle {
        const allocator = backing_allocator;
        const bt = try allocator.create(BufferTarget);
        errdefer allocator.destroy(bt);
        bt.* = BufferTarget.init(allocator);

        const handle = try allocator.create(RowWriterHandle);
        handle.* = .{ .allocator = allocator, .backend = .{ .buffer = bt } };
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
        handle.* = .{ .allocator = allocator, .backend = .{ .callback = adapter } };
        return handle;
    }

    pub fn openFile(path: [*:0]const u8) !*RowWriterHandle {
        const allocator = backing_allocator;
        const ft = try allocator.create(FileTarget);
        errdefer allocator.destroy(ft);

        const file = std.fs.cwd().createFileZ(path, .{}) catch return error.InputOutput;
        ft.* = FileTarget.init(file);

        const handle = try allocator.create(RowWriterHandle);
        handle.* = .{ .allocator = allocator, .backend = .{ .file = ft } };
        return handle;
    }

    fn getTarget(self: *RowWriterHandle) WriteTarget {
        return switch (self.backend) {
            .buffer => |bt| bt.target(),
            .callback => |cb| cb.target(),
            .file => |ft| ft.target(),
        };
    }

    // -- Schema building --

    pub fn addColumn(self: *RowWriterHandle, name: [*:0]const u8, info: TypeInfo) !void {
        if (self.began) return error.InvalidState;
        const duped = try self.allocator.dupeZ(u8, std.mem.sliceTo(name, 0));
        errdefer self.allocator.free(duped);
        try self.pending_columns.append(self.allocator, .{
            .name = duped,
            .col_type = info.physical,
            .logical_type = info.logical,
            .type_length = info.type_length,
        });
    }

    pub fn addColumnNested(self: *RowWriterHandle, name: [*:0]const u8, node: *const SchemaNode) !void {
        if (self.began) return error.InvalidState;
        const duped = try self.allocator.dupeZ(u8, std.mem.sliceTo(name, 0));
        errdefer self.allocator.free(duped);
        try self.pending_columns.append(self.allocator, .{
            .name = duped,
            .col_type = .nested,
            .schema_node = node,
        });
    }

    /// Allocate a SchemaNode on the handle's schema arena.
    pub fn allocSchemaNode(self: *RowWriterHandle, node: SchemaNode) !*const SchemaNode {
        const ptr = try self.schema_arena.allocator().create(SchemaNode);
        ptr.* = node;
        return ptr;
    }

    /// Allocate schema fields on the handle's schema arena.
    pub fn allocSchemaFields(self: *RowWriterHandle, count: usize) ![]SchemaNode.Field {
        return try self.schema_arena.allocator().alloc(SchemaNode.Field, count);
    }

    /// Duplicate a string into the schema arena.
    pub fn dupeSchemaName(self: *RowWriterHandle, name: []const u8) ![]const u8 {
        return try self.schema_arena.allocator().dupe(u8, name);
    }

    pub fn begin(self: *RowWriterHandle) !void {
        if (self.began) return error.InvalidState;
        const n = self.pending_columns.items.len;
        if (n == 0) return error.InvalidState;

        const allocator = self.allocator;

        self.target_writer = WriteTargetWriter.init(self.getTarget());

        // Write PAR1 magic
        self.target_writer.target.write(format.PARQUET_MAGIC) catch return error.WriteError;

        self.column_types = try allocator.alloc(ColumnType, n);
        self.column_logical_types = try allocator.alloc(?format.LogicalType, n);
        self.column_type_lengths = try allocator.alloc(?i32, n);
        self.column_codecs = try allocator.alloc(format.CompressionCodec, n);
        self.column_names_owned = try allocator.alloc([:0]u8, n);
        self.current_row = try allocator.alloc(value_mod.Value, n);
        self.row_set = try allocator.alloc(bool, n);
        self.column_buffers = try allocator.alloc(std.ArrayListUnmanaged(value_mod.Value), n);
        self.column_schema_nodes = try allocator.alloc(?*const SchemaNode, n);
        self.value_builders = try allocator.alloc(ValueBuilder, n);

        for (0..n) |i| {
            const pc = self.pending_columns.items[i];
            self.column_types[i] = pc.col_type;
            self.column_logical_types[i] = pc.logical_type;
            self.column_type_lengths[i] = pc.type_length;
            self.column_codecs[i] = pc.codec orelse self.default_codec;
            self.column_names_owned[i] = pc.name;
            self.current_row[i] = .null_val;
            self.row_set[i] = false;
            self.column_buffers[i] = .empty;
            self.column_schema_nodes[i] = pc.schema_node;
            self.value_builders[i] = .{};
        }

        self.num_columns = n;
        self.began = true;
    }

    // -- Row setters --

    pub fn setNull(self: *RowWriterHandle, col: usize) !void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        self.current_row[col] = .null_val;
        self.row_set[col] = true;
    }

    pub fn setBool(self: *RowWriterHandle, col: usize, val: bool) !void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        if (self.column_types[col] != .bool_) return error.InvalidArgument;
        self.current_row[col] = .{ .bool_val = val };
        self.row_set[col] = true;
    }

    pub fn setInt32(self: *RowWriterHandle, col: usize, val: i32) !void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        if (self.column_types[col] != .int32) return error.InvalidArgument;
        self.current_row[col] = .{ .int32_val = val };
        self.row_set[col] = true;
    }

    pub fn setInt64(self: *RowWriterHandle, col: usize, val: i64) !void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        if (self.column_types[col] != .int64) return error.InvalidArgument;
        self.current_row[col] = .{ .int64_val = val };
        self.row_set[col] = true;
    }

    pub fn setFloat(self: *RowWriterHandle, col: usize, val: f32) !void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        if (self.column_types[col] != .float_) return error.InvalidArgument;
        self.current_row[col] = .{ .float_val = val };
        self.row_set[col] = true;
    }

    pub fn setDouble(self: *RowWriterHandle, col: usize, val: f64) !void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        if (self.column_types[col] != .double_) return error.InvalidArgument;
        self.current_row[col] = .{ .double_val = val };
        self.row_set[col] = true;
    }

    pub fn setBytes(self: *RowWriterHandle, col: usize, data: [*]const u8, len: usize) !void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        const ct = self.column_types[col];
        if (ct != .bytes and ct != .fixed_bytes) return error.InvalidArgument;
        const copy = try self.bytes_arena.allocator().dupe(u8, data[0..len]);
        self.current_row[col] = .{ .bytes_val = copy };
        self.row_set[col] = true;
    }

    // -- Nested value builders --

    pub fn beginList(self: *RowWriterHandle, col: usize) !void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        if (self.column_types[col] != .nested) return error.InvalidArgument;
        try self.value_builders[col].push(self.allocator, .list);
    }

    pub fn endList(self: *RowWriterHandle, col: usize) !void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        var frame = self.value_builders[col].pop() orelse return error.InvalidState;
        if (frame.kind != .list) return error.InvalidState;
        const arena_alloc = self.bytes_arena.allocator();
        const items = try arena_alloc.dupe(value_mod.Value, frame.items.items);
        frame.items.deinit(self.allocator);
        const val = value_mod.Value{ .list_val = items };
        if (self.value_builders[col].top()) |parent| {
            switch (parent.kind) {
                .list => try parent.items.append(self.allocator, val),
                .struct_ => return error.InvalidState,
                .map_entry => try self.value_builders[col].appendValue(self.allocator, val),
                .map => return error.InvalidState,
            }
        } else {
            self.current_row[col] = val;
            self.row_set[col] = true;
        }
    }

    pub fn beginStruct(self: *RowWriterHandle, col: usize) !void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        if (self.column_types[col] != .nested) return error.InvalidArgument;
        try self.value_builders[col].push(self.allocator, .struct_);
    }

    pub fn setStructField(self: *RowWriterHandle, col: usize, field_idx: usize, val: value_mod.Value) !void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        const frame = self.value_builders[col].top() orelse return error.InvalidState;
        if (frame.kind != .struct_) return error.InvalidState;
        const node = self.column_schema_nodes[col] orelse return error.InvalidState;
        const fields = getStructFields(node) orelse return error.InvalidState;
        if (field_idx >= fields.len) return error.InvalidArgument;
        const arena_alloc = self.bytes_arena.allocator();
        const name = try arena_alloc.dupe(u8, fields[field_idx].name);
        try frame.struct_fields.append(self.allocator, .{ .name = name, .value = val });
    }

    pub fn setStructFieldBytes(self: *RowWriterHandle, col: usize, field_idx: usize, data: [*]const u8, len: usize) !void {
        const copy = try self.bytes_arena.allocator().dupe(u8, data[0..len]);
        try self.setStructField(col, field_idx, .{ .bytes_val = copy });
    }

    pub fn endStruct(self: *RowWriterHandle, col: usize) !void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        var frame = self.value_builders[col].pop() orelse return error.InvalidState;
        if (frame.kind != .struct_) return error.InvalidState;
        const arena_alloc = self.bytes_arena.allocator();
        const fields = try arena_alloc.dupe(value_mod.Value.FieldValue, frame.struct_fields.items);
        frame.struct_fields.deinit(self.allocator);
        const val = value_mod.Value{ .struct_val = fields };
        if (self.value_builders[col].top()) |parent| {
            switch (parent.kind) {
                .list => try parent.items.append(self.allocator, val),
                .map_entry => try self.value_builders[col].appendValue(self.allocator, val),
                else => return error.InvalidState,
            }
        } else {
            self.current_row[col] = val;
            self.row_set[col] = true;
        }
    }

    pub fn beginMap(self: *RowWriterHandle, col: usize) !void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        if (self.column_types[col] != .nested) return error.InvalidArgument;
        try self.value_builders[col].push(self.allocator, .map);
    }

    pub fn beginMapEntry(self: *RowWriterHandle, col: usize) !void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        const frame = self.value_builders[col].top() orelse return error.InvalidState;
        if (frame.kind != .map) return error.InvalidState;
        try self.value_builders[col].push(self.allocator, .map_entry);
    }

    pub fn endMapEntry(self: *RowWriterHandle, col: usize) !void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        var frame = self.value_builders[col].pop() orelse return error.InvalidState;
        if (frame.kind != .map_entry) return error.InvalidState;
        if (frame.entry_key == null and frame.map_entries.items.len == 0) return error.InvalidState;
        const parent = self.value_builders[col].top() orelse return error.InvalidState;
        if (parent.kind != .map) return error.InvalidState;
        if (frame.map_entries.items.len == 1) {
            try parent.map_entries.append(self.allocator, frame.map_entries.items[0]);
        } else if (frame.entry_key != null) {
            try parent.map_entries.append(self.allocator, .{
                .key = frame.entry_key.?,
                .value = .null_val,
            });
        }
        frame.map_entries.deinit(self.allocator);
    }

    pub fn endMap(self: *RowWriterHandle, col: usize) !void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        var frame = self.value_builders[col].pop() orelse return error.InvalidState;
        if (frame.kind != .map) return error.InvalidState;
        const arena_alloc = self.bytes_arena.allocator();
        const entries = try arena_alloc.dupe(value_mod.Value.MapEntryValue, frame.map_entries.items);
        frame.map_entries.deinit(self.allocator);
        const val = value_mod.Value{ .map_val = entries };
        if (self.value_builders[col].top()) |parent| {
            switch (parent.kind) {
                .list => try parent.items.append(self.allocator, val),
                .map_entry => try self.value_builders[col].appendValue(self.allocator, val),
                else => return error.InvalidState,
            }
        } else {
            self.current_row[col] = val;
            self.row_set[col] = true;
        }
    }

    pub fn appendNestedValue(self: *RowWriterHandle, col: usize, val: value_mod.Value) !void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        try self.value_builders[col].appendValue(self.allocator, val);
    }

    pub fn appendNestedBytes(self: *RowWriterHandle, col: usize, data: [*]const u8, len: usize) !void {
        const copy = try self.bytes_arena.allocator().dupe(u8, data[0..len]);
        try self.appendNestedValue(col, .{ .bytes_val = copy });
    }

    fn getStructFields(node: *const SchemaNode) ?[]const SchemaNode.Field {
        return switch (node.*) {
            .optional => |child| getStructFields(child),
            .struct_ => |s| s.fields,
            else => null,
        };
    }

    pub fn addRow(self: *RowWriterHandle) !void {
        if (!self.began) return error.InvalidState;
        for (0..self.num_columns) |i| {
            if (!self.row_set[i]) {
                self.current_row[i] = .null_val;
            }
            try self.column_buffers[i].append(self.allocator, self.current_row[i]);
            self.current_row[i] = .null_val;
            self.row_set[i] = false;
        }
        self.rows_in_current_group += 1;
        if (self.row_group_row_limit) |limit| {
            if (self.rows_in_current_group >= limit) {
                try self.flush();
            }
        }
    }

    // -- Flush (writes one row group) --

    pub fn flush(self: *RowWriterHandle) !void {
        if (!self.began) return error.InvalidState;
        if (self.column_buffers.len == 0) return;
        const n = self.column_buffers[0].items.len;
        if (n == 0) return;

        const allocator = self.allocator;
        const writer = self.target_writer.writer();
        const row_group_offset = self.current_offset;

        var col_chunks_list: std.ArrayListUnmanaged(format.ColumnChunk) = .empty;
        errdefer {
            for (col_chunks_list.items) |cc| {
                if (cc.meta_data) |meta| {
                    allocator.free(meta.encodings);
                    for (meta.path_in_schema) |p| allocator.free(p);
                    allocator.free(meta.path_in_schema);
                }
            }
            col_chunks_list.deinit(allocator);
        }

        var total_byte_size: i64 = 0;

        for (0..self.num_columns) |col| {
            const items = self.column_buffers[col].items;
            const codec = self.column_codecs[col];
            const col_name = self.column_names_owned[col];

            if (self.column_types[col] == .nested) {
                const node = self.column_schema_nodes[col] orelse return error.InvalidState;
                try self.flushNestedColumn(&col_chunks_list, items, n, node, col_name, codec, &total_byte_size);
            } else {
                const path: []const []const u8 = &.{col_name};
                const result: column_writer.ColumnChunkResult = switch (self.column_types[col]) {
                    .bool_ => try flushColumnTyped(bool, allocator, writer, path, items, n, self.current_offset, codec),
                    .int32 => try flushColumnTyped(i32, allocator, writer, path, items, n, self.current_offset, codec),
                    .int64 => try flushColumnTyped(i64, allocator, writer, path, items, n, self.current_offset, codec),
                    .float_ => try flushColumnTyped(f32, allocator, writer, path, items, n, self.current_offset, codec),
                    .double_ => try flushColumnTyped(f64, allocator, writer, path, items, n, self.current_offset, codec),
                    .bytes, .fixed_bytes => try flushColumnBytes(allocator, writer, path, items, n, self.current_offset, codec),
                    .nested => unreachable,
                };

                self.target_writer.flush() catch return error.WriteError;

                try col_chunks_list.append(allocator, .{
                    .file_path = null,
                    .file_offset = result.file_offset,
                    .meta_data = result.metadata,
                });
                total_byte_size += result.metadata.total_compressed_size;
                self.current_offset += safe.castTo(i64, result.total_bytes) catch return error.IntegerOverflow;
            }
        }

        const col_chunks = try col_chunks_list.toOwnedSlice(allocator);

        try self.row_groups.append(allocator, .{
            .columns = col_chunks,
            .num_rows = safe.castTo(i64, n) catch return error.TooManyValues,
            .total_byte_size = total_byte_size,
            .file_offset = row_group_offset,
        });

        for (0..self.num_columns) |i| {
            self.column_buffers[i].clearRetainingCapacity();
        }
        _ = self.bytes_arena.reset(.retain_capacity);
        self.rows_in_current_group = 0;
    }

    fn flushNestedColumn(
        self: *RowWriterHandle,
        col_chunks_list: *std.ArrayListUnmanaged(format.ColumnChunk),
        items: []const value_mod.Value,
        n: usize,
        node: *const SchemaNode,
        col_name: [:0]u8,
        codec: format.CompressionCodec,
        total_byte_size: *i64,
    ) !void {
        const allocator = self.allocator;
        const writer = self.target_writer.writer();
        const leaf_count = node.countLeafColumns();

        var all_columns: std.ArrayListUnmanaged(nested_mod.FlatColumn) = .empty;
        defer {
            for (all_columns.items) |*c| c.deinit(allocator);
            all_columns.deinit(allocator);
        }

        for (0..leaf_count) |_| {
            try all_columns.append(allocator, nested_mod.FlatColumn.init());
        }

        for (0..n) |row_idx| {
            const row_value = items[row_idx];
            var row_columns = nested_mod.flattenValue(allocator, node, row_value) catch return error.OutOfMemory;
            defer row_columns.deinit();

            for (row_columns.columns.items, 0..) |rc, i| {
                for (rc.values.items) |v| {
                    all_columns.items[i].values.append(allocator, v) catch return error.OutOfMemory;
                }
                for (rc.def_levels.items) |d| {
                    all_columns.items[i].def_levels.append(allocator, d) catch return error.OutOfMemory;
                }
                for (rc.rep_levels.items) |r| {
                    all_columns.items[i].rep_levels.append(allocator, r) catch return error.OutOfMemory;
                }
            }
        }

        // Build leaf paths
        var leaf_paths: std.ArrayListUnmanaged([]const []const u8) = .empty;
        defer {
            for (leaf_paths.items) |path| {
                for (path) |seg| allocator.free(seg);
                allocator.free(path);
            }
            leaf_paths.deinit(allocator);
        }

        var current_path: std.ArrayListUnmanaged([]const u8) = .empty;
        defer current_path.deinit(allocator);
        try current_path.append(allocator, col_name);
        try buildLeafPathsStatic(allocator, &leaf_paths, &current_path, node);

        const leaf_levels = node.computeLeafLevels(allocator) catch return error.OutOfMemory;
        defer allocator.free(leaf_levels);

        for (all_columns.items, 0..) |*col, leaf_idx| {
            const path = if (leaf_idx < leaf_paths.items.len) leaf_paths.items[leaf_idx] else &[_][]const u8{col_name};
            const levels = if (leaf_idx < leaf_levels.len) leaf_levels[leaf_idx] else SchemaNode.Levels{ .max_def = 0, .max_rep = 0 };

            const result = column_writer.writeColumnChunkFromValues(
                allocator, writer, path,
                col.values.items, col.def_levels.items, col.rep_levels.items,
                levels.max_def, levels.max_rep,
                self.current_offset, codec,
            ) catch return error.WriteError;

            self.target_writer.flush() catch return error.WriteError;

            try col_chunks_list.append(allocator, .{
                .file_path = null,
                .file_offset = result.file_offset,
                .meta_data = result.metadata,
            });
            total_byte_size.* += result.metadata.total_compressed_size;
            self.current_offset += safe.castTo(i64, result.total_bytes) catch return error.IntegerOverflow;
        }
    }

    fn buildLeafPathsStatic(
        allocator: Allocator,
        paths: *std.ArrayListUnmanaged([]const []const u8),
        current_path: *std.ArrayListUnmanaged([]const u8),
        node: *const SchemaNode,
    ) !void {
        switch (node.*) {
            .optional => |child| try buildLeafPathsStatic(allocator, paths, current_path, child),
            .list => |element| {
                try current_path.append(allocator, "list");
                defer _ = current_path.pop();
                try current_path.append(allocator, "element");
                defer _ = current_path.pop();
                try buildLeafPathsStatic(allocator, paths, current_path, element);
            },
            .map => |m| {
                try current_path.append(allocator, "key_value");
                try current_path.append(allocator, "key");
                try buildLeafPathsStatic(allocator, paths, current_path, m.key);
                _ = current_path.pop();
                try current_path.append(allocator, "value");
                try buildLeafPathsStatic(allocator, paths, current_path, m.value);
                _ = current_path.pop();
                _ = current_path.pop();
            },
            .struct_ => |s| {
                for (s.fields) |f| {
                    try current_path.append(allocator, f.name);
                    try buildLeafPathsStatic(allocator, paths, current_path, f.node);
                    _ = current_path.pop();
                }
            },
            .boolean, .int32, .int64, .float, .double, .byte_array, .fixed_len_byte_array => {
                const path = try allocator.alloc([]const u8, current_path.items.len);
                for (current_path.items, 0..) |seg, i| {
                    path[i] = try allocator.dupe(u8, seg);
                }
                try paths.append(allocator, path);
            },
        }
    }

    fn flushColumnTyped(
        comptime T: type,
        allocator: Allocator,
        writer: *std.Io.Writer,
        path: []const []const u8,
        items: []const value_mod.Value,
        n: usize,
        offset: i64,
        codec: format.CompressionCodec,
    ) !column_writer.ColumnChunkResult {
        const typed = try allocator.alloc(types.Optional(T), n);
        defer allocator.free(typed);
        for (items, 0..) |v, j| {
            typed[j] = if (v.isNull()) .null_value else .{
                .value = extractTyped(T, v) orelse return error.InvalidData,
            };
        }
        return column_writer.writeColumnChunkDictOptionalWithPathArray(
            T, allocator, writer, path, typed,
            true, offset, codec, null, null, null, true,
        );
    }

    fn flushColumnBytes(
        allocator: Allocator,
        writer: *std.Io.Writer,
        path: []const []const u8,
        items: []const value_mod.Value,
        n: usize,
        offset: i64,
        codec: format.CompressionCodec,
    ) !column_writer.ColumnChunkResult {
        const typed = try allocator.alloc(types.Optional([]const u8), n);
        defer allocator.free(typed);
        for (items, 0..) |v, j| {
            typed[j] = if (v.isNull()) .null_value else .{
                .value = v.asBytes() orelse return error.InvalidData,
            };
        }
        return column_writer.writeColumnChunkByteArrayDictOptionalWithPathArray(
            allocator, writer, path, typed,
            true, offset, codec, null, null, null, true,
        );
    }

    fn extractTyped(comptime T: type, v: value_mod.Value) ?T {
        return switch (T) {
            bool => v.asBool(),
            i32 => v.asInt32(),
            i64 => v.asInt64(),
            f32 => v.asFloat(),
            f64 => v.asDouble(),
            []const u8 => v.asBytes(),
            else => null,
        };
    }

    // -- Close (flush remaining + write footer) --

    pub fn setKvMetadata(self: *RowWriterHandle, key: []const u8, value: ?[]const u8) !void {
        const allocator = self.allocator;
        for (self.kv_metadata.items) |*kv| {
            if (std.mem.eql(u8, kv.key, key)) {
                if (kv.value) |old_v| allocator.free(old_v);
                kv.value = if (value) |v| try allocator.dupe(u8, v) else null;
                return;
            }
        }
        const owned_key = try allocator.dupe(u8, key);
        errdefer allocator.free(owned_key);
        const owned_value = if (value) |v| try allocator.dupe(u8, v) else null;
        try self.kv_metadata.append(allocator, .{ .key = owned_key, .value = owned_value });
    }

    pub fn writerClose(self: *RowWriterHandle) !void {
        if (self.column_buffers.len > 0 and self.column_buffers[0].items.len > 0) {
            try self.flush();
        }
        try self.writeFooter();
        self.target_writer.target.close() catch return error.WriteError;
    }

    fn writeFooter(self: *RowWriterHandle) !void {
        const allocator = self.allocator;
        const footer_bytes = try self.buildSerializedFooter();
        defer allocator.free(footer_bytes);

        const writer = self.target_writer.writer();
        writer.writeAll(footer_bytes) catch return error.WriteError;

        var len_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &len_buf, safe.castTo(u32, footer_bytes.len) catch return error.IntegerOverflow, .little);
        writer.writeAll(&len_buf) catch return error.WriteError;
        writer.writeAll(format.PARQUET_MAGIC) catch return error.WriteError;

        self.target_writer.flush() catch return error.WriteError;
    }

    fn buildSerializedFooter(self: *RowWriterHandle) ![]u8 {
        const allocator = self.allocator;
        const n = self.num_columns;

        // Count total schema elements (nested columns expand to multiple)
        var total_elements: usize = 1; // root
        for (0..n) |i| {
            if (self.column_schema_nodes[i]) |node| {
                total_elements += core_writer.Writer.countSchemaElements(node);
            } else {
                total_elements += 1;
            }
        }

        const schema = try allocator.alloc(format.SchemaElement, total_elements);
        defer allocator.free(schema);

        schema[0] = .{
            .type_ = null,
            .type_length = null,
            .repetition_type = null,
            .name = "schema",
            .num_children = safe.castTo(i32, n) catch return error.IntegerOverflow,
            .converted_type = null,
            .scale = null,
            .precision = null,
            .field_id = null,
            .logical_type = null,
        };

        var idx: usize = 1;
        for (0..n) |i| {
            if (self.column_schema_nodes[i]) |node| {
                idx = core_writer.Writer.generateSchemaFromNodeStatic(schema, idx, self.column_names_owned[i], node, .required) catch return error.OutOfMemory;
            } else {
                const pt = self.column_types[i].toPhysicalType() orelse return error.InvalidState;
                const lt = self.column_logical_types[i];

                var precision: ?i32 = null;
                var scale: ?i32 = null;
                var converted_type: ?i32 = null;
                if (lt) |l| {
                    if (l == .decimal) {
                        precision = l.decimal.precision;
                        scale = l.decimal.scale;
                    }
                } else if (pt == .fixed_len_byte_array and self.column_type_lengths[i] != null and self.column_type_lengths[i].? == 12) {
                    converted_type = format.ConvertedType.INTERVAL;
                }

                schema[idx] = .{
                    .type_ = pt,
                    .type_length = self.column_type_lengths[i],
                    .repetition_type = .optional,
                    .name = self.column_names_owned[i],
                    .num_children = null,
                    .converted_type = converted_type,
                    .scale = scale,
                    .precision = precision,
                    .field_id = null,
                    .logical_type = lt,
                };
                idx += 1;
            }
        }

        // Build format row groups
        var total_rows: i64 = 0;
        for (self.row_groups.items) |rg| {
            total_rows = std.math.add(i64, total_rows, rg.num_rows) catch return error.IntegerOverflow;
        }

        const fmt_rgs = try allocator.alloc(format.RowGroup, self.row_groups.items.len);
        defer allocator.free(fmt_rgs);

        for (self.row_groups.items, 0..) |rg, i| {
            fmt_rgs[i] = .{
                .columns = rg.columns,
                .total_byte_size = rg.total_byte_size,
                .num_rows = rg.num_rows,
                .sorting_columns = null,
                .file_offset = rg.file_offset,
                .total_compressed_size = rg.total_byte_size,
                .ordinal = safe.castTo(i16, i) catch 0,
            };
        }

        const file_metadata = format.FileMetaData{
            .version = 1,
            .schema = schema,
            .num_rows = total_rows,
            .row_groups = fmt_rgs,
            .key_value_metadata = if (self.kv_metadata.items.len > 0) self.kv_metadata.items else null,
            .created_by = "zig-parquet",
        };

        var thrift_writer = thrift.CompactWriter.init(allocator);
        defer thrift_writer.deinit();
        file_metadata.serialize(&thrift_writer) catch return error.OutOfMemory;

        return allocator.dupe(u8, thrift_writer.getWritten()) catch return error.OutOfMemory;
    }

    // -- Cleanup --

    pub fn deinit(self: *RowWriterHandle) void {
        const allocator = self.allocator;

        for (0..self.num_columns) |i| {
            self.column_buffers[i].deinit(allocator);
            self.value_builders[i].deinit(allocator);
        }
        if (self.column_buffers.len > 0) allocator.free(self.column_buffers);
        if (self.value_builders.len > 0) allocator.free(self.value_builders);
        if (self.column_schema_nodes.len > 0) allocator.free(self.column_schema_nodes);
        if (self.current_row.len > 0) allocator.free(self.current_row);
        if (self.row_set.len > 0) allocator.free(self.row_set);
        if (self.column_types.len > 0) allocator.free(self.column_types);
        if (self.column_logical_types.len > 0) allocator.free(self.column_logical_types);
        if (self.column_type_lengths.len > 0) allocator.free(self.column_type_lengths);
        if (self.column_codecs.len > 0) allocator.free(self.column_codecs);

        self.bytes_arena.deinit();
        self.schema_arena.deinit();

        // Free row group column chunk metadata
        for (self.row_groups.items) |rg| {
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
                }
            }
            allocator.free(rg.columns);
        }
        self.row_groups.deinit(allocator);

        for (self.kv_metadata.items) |kv| {
            allocator.free(kv.key);
            if (kv.value) |v| allocator.free(v);
        }
        self.kv_metadata.deinit(allocator);

        // Column names are owned by pending_columns; free those
        for (self.pending_columns.items) |pc| {
            allocator.free(pc.name);
        }
        self.pending_columns.deinit(allocator);
        if (self.column_names_owned.len > 0) allocator.free(self.column_names_owned);

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
