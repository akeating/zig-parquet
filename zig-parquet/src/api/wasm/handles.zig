//! WASM handle types for the Parquet API.
//!
//! Mirrors the C ABI handle infrastructure but uses io/ adapters directly
//! (CallbackReader/CallbackWriter) instead of C-specific callback adapters
//! that require callconv(.c). All function pointer types use the default
//! calling convention, which is WASM-compatible.

const std = @import("std");
const ErrorContext = @import("../../api/c/error.zig").ErrorContext;

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
const nested_mod = @import("../../core/nested.zig");
const schema_mod = @import("../../core/schema.zig");

const BufferReader = @import("../../io/buffer_reader.zig").BufferReader;
const CallbackReader = @import("../../io/callback_reader.zig").CallbackReader;
const BufferTarget = @import("../../io/buffer_target.zig").BufferTarget;
const CallbackWriter = @import("../../io/callback_writer.zig").CallbackWriter;

pub const SeekableReader = seekable_reader.SeekableReader;
pub const WriteTarget = write_target.WriteTarget;
pub const ArrowSchema = arrow.ArrowSchema;
pub const ArrowArray = arrow.ArrowArray;
pub const Writer = core_writer.Writer;
pub const ColumnDef = column_def_mod.ColumnDef;

const safe = @import("../../core/safe.zig");

pub const SchemaNode = schema_mod.SchemaNode;

const Allocator = std.mem.Allocator;

const backing_allocator = std.heap.page_allocator;

// ============================================================================
// Arrow struct wrapper (shared by WASI and freestanding)
// ============================================================================

const StructWrapperData = struct {
    allocator: std.mem.Allocator,
    child_arrays: []ArrowArray,
    child_ptrs: []*ArrowArray,
    buffers_storage: [1]?*anyopaque,
};

pub const StructWrapperResult = struct {
    array: ArrowArray,
};

pub fn wrapColumnsAsStruct(
    allocator: std.mem.Allocator,
    arrays: []ArrowArray,
    schema: *const ArrowSchema,
) !StructWrapperResult {
    const n = arrays.len;

    const pd = try allocator.create(StructWrapperData);
    errdefer allocator.destroy(pd);

    pd.child_arrays = try allocator.alloc(ArrowArray, n);
    errdefer allocator.free(pd.child_arrays);
    @memcpy(pd.child_arrays, arrays);

    pd.child_ptrs = try allocator.alloc(*ArrowArray, n);
    errdefer allocator.free(pd.child_ptrs);
    for (pd.child_arrays, 0..) |*a, i| {
        pd.child_ptrs[i] = a;
    }

    pd.buffers_storage = .{null};
    pd.allocator = allocator;

    const num_rows: i64 = if (n > 0) arrays[0].length else 0;

    _ = schema;
    return .{
        .array = .{
            .length = num_rows,
            .null_count = 0,
            .offset = 0,
            .n_buffers = 1,
            .n_children = safe.castTo(i64, n) catch return error.OutOfMemory,
            .buffers = &pd.buffers_storage,
            .children = @ptrCast(pd.child_ptrs.ptr),
            .dictionary = null,
            .release = &structWrapperRelease,
            .private_data = @ptrCast(pd),
        },
    };
}

fn structWrapperRelease(arr: *ArrowArray) callconv(.c) void {
    const pd: *StructWrapperData = @ptrCast(@alignCast(arr.private_data));
    const allocator = pd.allocator;

    for (pd.child_arrays) |*child| child.doRelease();
    allocator.free(pd.child_arrays);
    allocator.free(pd.child_ptrs);
    allocator.destroy(pd);
    arr.release = null;
}

// ============================================================================
// Reader Handle
// ============================================================================

const ReaderBackend = union(enum) {
    buffer: *BufferReader,
    callback: *CallbackReader,
};

pub const ReaderHandle = struct {
    allocator: Allocator,
    source: SeekableReader,
    metadata: format.FileMetaData,
    footer_data: []u8,
    backend: ReaderBackend,
    err_ctx: ErrorContext = .{},

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

    pub fn openCallbacks(cb_reader: *CallbackReader) !*ReaderHandle {
        const allocator = backing_allocator;

        const info = parquet_reader.parseFooter(allocator, cb_reader.reader()) catch |e| return e;

        const handle = try allocator.create(ReaderHandle);
        handle.* = .{
            .allocator = allocator,
            .source = cb_reader.reader(),
            .metadata = info.metadata,
            .footer_data = info.footer_data,
            .backend = .{ .callback = cb_reader },
        };
        return handle;
    }

    pub fn close(self: *ReaderHandle) void {
        const allocator = self.allocator;

        for (self.metadata.schema) |elem| {
            if (elem.name.len > 0) allocator.free(elem.name);
        }
        allocator.free(self.metadata.schema);

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
        }

        allocator.destroy(self);
    }
};

// ============================================================================
// Writer Handle
// ============================================================================

const WriterBackend = union(enum) {
    buffer: *BufferTarget,
    callback: *CallbackWriter,
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

    pub fn openCallbackWriter(cb_writer: *CallbackWriter) !*WriterHandle {
        const allocator = backing_allocator;

        const handle = try allocator.create(WriterHandle);
        handle.* = .{
            .allocator = allocator,
            .backend = .{ .callback = cb_writer },
        };
        return handle;
    }

    pub fn getTarget(self: *WriterHandle) WriteTarget {
        return switch (self.backend) {
            .buffer => |bt| bt.target(),
            .callback => |cb| cb.target(),
        };
    }

    pub fn setSchema(self: *WriterHandle, schema: *const ArrowSchema) !void {
        if (self.writer != null) return error.InvalidState;

        const allocator = self.allocator;
        const col_defs = try arrow_batch.importSchemaFromArrow(allocator, schema);
        errdefer allocator.free(col_defs);

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
        }

        allocator.destroy(self);
    }
};

// ============================================================================
// Row Reader Handle (cursor-based, non-Arrow)
// ============================================================================

pub const Value = value_mod.Value;
pub const Row = value_mod.Row;

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

    pub fn openCallbacks(cb_reader: *CallbackReader) !*RowReaderHandle {
        const allocator = backing_allocator;

        var dr = core_dynamic.DynamicReader.initFromSeekable(allocator, cb_reader.reader(), .{}) catch |e| {
            allocator.destroy(cb_reader);
            return e;
        };
        dr._backend_cleanup = .{ .ptr = @ptrCast(cb_reader), .deinit_fn = &callbackCleanup };
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

    pub fn next(self: *RowReaderHandle) bool {
        const rows = self.current_rows orelse return false;
        if (self.cursor >= rows.len) return false;
        self.cursor += 1;
        return true;
    }

    pub fn currentRow(self: *const RowReaderHandle) ?*const value_mod.Row {
        const rows = self.current_rows orelse return null;
        if (self.cursor == 0) return null;
        const idx = self.cursor - 1;
        if (idx >= rows.len) return null;
        return &rows[idx];
    }

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

    fn callbackCleanup(ptr: *anyopaque, allocator: Allocator) void {
        const cb: *CallbackReader = @ptrCast(@alignCast(ptr));
        allocator.destroy(cb);
    }
};

// ============================================================================
// Row Writer Handle (cursor-based, non-Arrow, multi-row-group)
// ============================================================================

const c_err = @import("../../api/c/error.zig");
const dynamic_writer_mod = @import("../../core/dynamic_writer.zig");

pub const DynamicWriter = dynamic_writer_mod.DynamicWriter;
pub const ColumnType = dynamic_writer_mod.ColumnType;
pub const TypeInfo = dynamic_writer_mod.TypeInfo;

pub fn typeInfoFromZpType(t: c_int) ?TypeInfo {
    return switch (t) {
        c_err.ZP_TYPE_BOOL => TypeInfo.bool_,
        c_err.ZP_TYPE_INT32 => TypeInfo.int32,
        c_err.ZP_TYPE_INT64 => TypeInfo.int64,
        c_err.ZP_TYPE_FLOAT => TypeInfo.float_,
        c_err.ZP_TYPE_DOUBLE => TypeInfo.double_,
        c_err.ZP_TYPE_BYTES => TypeInfo.bytes,
        c_err.ZP_TYPE_STRING => TypeInfo.string,
        c_err.ZP_TYPE_DATE => TypeInfo.date,
        c_err.ZP_TYPE_TIMESTAMP_MILLIS => TypeInfo.timestamp_millis,
        c_err.ZP_TYPE_TIMESTAMP_MICROS => TypeInfo.timestamp_micros,
        c_err.ZP_TYPE_TIMESTAMP_NANOS => TypeInfo.timestamp_nanos,
        c_err.ZP_TYPE_TIME_MILLIS => TypeInfo.time_millis,
        c_err.ZP_TYPE_TIME_MICROS => TypeInfo.time_micros,
        c_err.ZP_TYPE_TIME_NANOS => TypeInfo.time_nanos,
        c_err.ZP_TYPE_INT8 => TypeInfo.int8,
        c_err.ZP_TYPE_INT16 => TypeInfo.int16,
        c_err.ZP_TYPE_UINT8 => TypeInfo.uint8,
        c_err.ZP_TYPE_UINT16 => TypeInfo.uint16,
        c_err.ZP_TYPE_UINT32 => TypeInfo.uint32,
        c_err.ZP_TYPE_UINT64 => TypeInfo.uint64,
        c_err.ZP_TYPE_UUID => TypeInfo.uuid,
        c_err.ZP_TYPE_JSON => TypeInfo.json,
        c_err.ZP_TYPE_ENUM => TypeInfo.enum_,
        c_err.ZP_TYPE_FLOAT16 => TypeInfo.float16,
        c_err.ZP_TYPE_BSON => TypeInfo.bson,
        c_err.ZP_TYPE_INTERVAL => TypeInfo.interval,
        c_err.ZP_TYPE_GEOMETRY => TypeInfo.geometry,
        c_err.ZP_TYPE_GEOGRAPHY => TypeInfo.geography,
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

    pub fn openCallbackWriter(cb_writer: *CallbackWriter) !*RowWriterHandle {
        const allocator = backing_allocator;
        const handle = try allocator.create(RowWriterHandle);
        handle.* = .{
            .allocator = allocator,
            .backend = .{ .callback = cb_writer },
            .writer = DynamicWriter.init(allocator, cb_writer.target()),
        };
        return handle;
    }

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
        }

        allocator.destroy(self);
    }
};
