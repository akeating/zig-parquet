//! C ABI row writer entry points.
//!
//! Cursor-based row-oriented API for writing Parquet files without Arrow
//! knowledge. Backed by column_writer directly for multi-row-group support.
//!
//! Workflow:
//!   1. Open with zp_row_writer_open_*
//!   2. Define schema with zp_row_writer_add_column (before begin)
//!   3. Optionally set compression with zp_row_writer_set_compression
//!   4. Call zp_row_writer_begin to finalize schema
//!   5. Build rows: set_int32/set_bytes/... then add_row
//!   6. Optionally flush to write a row group boundary
//!   7. Close with zp_row_writer_close (writes remaining rows + footer)
//!   8. For memory backend: get_buffer to retrieve output
//!   9. Free with zp_row_writer_free

const std = @import("std");
const builtin = @import("builtin");
const err = @import("error.zig");
const handles = @import("handles.zig");
const format = @import("../../core/format.zig");
const safe = @import("../../core/safe.zig");

const RowWriterHandle = handles.RowWriterHandle;
const TypeInfo = handles.TypeInfo;
const SchemaNode = @import("../../core/schema.zig").SchemaNode;
const value_mod = @import("../../core/value.zig");
const BufferTarget = @import("../../io/buffer_target.zig").BufferTarget;

const ZP_OK = err.ZP_OK;
const is_wasm = builtin.cpu.arch == .wasm32 or builtin.cpu.arch == .wasm64;

// ============================================================================
// Open
// ============================================================================

pub export fn zp_row_writer_open_memory(
    handle_out: ?*?*anyopaque,
) callconv(.c) c_int {
    const out = handle_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    out.* = null;
    const handle = RowWriterHandle.openMemory() catch |e| return err.mapError(e);
    out.* = @ptrCast(handle);
    return ZP_OK;
}

pub export fn zp_row_writer_open_callbacks(
    ctx: ?*anyopaque,
    write_fn: ?*const fn (?*anyopaque, [*]const u8, usize) callconv(.c) c_int,
    close_fn: ?*const fn (?*anyopaque) callconv(.c) c_int,
    handle_out: ?*?*anyopaque,
) callconv(.c) c_int {
    const out = handle_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    out.* = null;
    const w_fn = write_fn orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const handle = RowWriterHandle.openCallbacksC(ctx, w_fn, close_fn) catch |e| return err.mapError(e);
    out.* = @ptrCast(handle);
    return ZP_OK;
}

pub export fn zp_row_writer_open_file(
    path: ?[*:0]const u8,
    handle_out: ?*?*anyopaque,
) callconv(.c) c_int {
    if (is_wasm) return err.ZP_ERROR_UNSUPPORTED;
    const out = handle_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    out.* = null;
    const p = path orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const handle = RowWriterHandle.openFile(p) catch |e| return err.mapError(e);
    out.* = @ptrCast(handle);
    return ZP_OK;
}

// ============================================================================
// Schema
// ============================================================================

pub export fn zp_row_writer_add_column(
    handle_ptr: ?*anyopaque,
    name: ?[*:0]const u8,
    col_type: c_int,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const n = name orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const info = TypeInfo.fromZpType(col_type) orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "unknown column type");
        return handle.err_ctx.code;
    };
    handle.addColumn(n, info) catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "addColumn failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

pub export fn zp_row_writer_add_column_decimal(
    handle_ptr: ?*anyopaque,
    name: ?[*:0]const u8,
    precision: c_int,
    scale: c_int,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const n = name orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    if (precision < 1 or precision > 38) {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "decimal precision must be 1-38");
        return handle.err_ctx.code;
    }
    if (scale < 0 or scale > precision) {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "decimal scale must be 0..precision");
        return handle.err_ctx.code;
    }
    const info = TypeInfo.forDecimal(precision, scale);
    handle.addColumn(n, info) catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "addColumn failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

pub export fn zp_row_writer_add_column_geometry(
    handle_ptr: ?*anyopaque,
    name: ?[*:0]const u8,
    crs_ptr: ?[*]const u8,
    crs_len: c_int,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const n = name orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const crs: ?[]const u8 = if (crs_ptr) |p| blk: {
        const len = if (crs_len >= 0) safe.castTo(usize, crs_len) catch unreachable else 0; // guarded by >= 0 check
        break :blk if (len > 0) p[0..len] else null;
    } else null;
    const info = TypeInfo{
        .physical = .bytes,
        .logical = .{ .geometry = .{ .crs = crs } },
        .type_length = null,
    };
    handle.addColumn(n, info) catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "addColumn failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

pub export fn zp_row_writer_add_column_geography(
    handle_ptr: ?*anyopaque,
    name: ?[*:0]const u8,
    crs_ptr: ?[*]const u8,
    crs_len: c_int,
    algorithm: c_int,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const n = name orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const crs: ?[]const u8 = if (crs_ptr) |p| blk: {
        const len = if (crs_len >= 0) safe.castTo(usize, crs_len) catch unreachable else 0; // guarded by >= 0 check
        break :blk if (len > 0) p[0..len] else null;
    } else null;
    const format_ = @import("../../core/format.zig");
    const algo: ?format_.EdgeInterpolationAlgorithm = switch (algorithm) {
        0 => .spherical,
        1 => .vincenty,
        2 => .thomas,
        3 => .andoyer,
        4 => .karney,
        else => null,
    };
    const info = TypeInfo{
        .physical = .bytes,
        .logical = .{ .geography = .{ .crs = crs, .algorithm = algo } },
        .type_length = null,
    };
    handle.addColumn(n, info) catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "addColumn failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

pub export fn zp_row_writer_set_compression(
    handle_ptr: ?*anyopaque,
    codec: c_int,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    if (handle.began) {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_STATE, "cannot set compression after begin");
        return handle.err_ctx.code;
    }
    const cc = format.CompressionCodec.fromInt(codec) catch {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "unknown compression codec");
        return handle.err_ctx.code;
    };
    handle.default_codec = cc;
    return ZP_OK;
}

/// Set per-column compression codec (must be called after add_column, before begin).
pub export fn zp_row_writer_set_column_codec(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
    codec: c_int,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    if (handle.began) {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_STATE, "cannot set column codec after begin");
        return handle.err_ctx.code;
    }
    if (col_index < 0) {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "negative column index");
        return handle.err_ctx.code;
    }
    const idx = safe.castTo(usize, col_index) catch {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "column index out of range");
        return handle.err_ctx.code;
    };
    if (idx >= handle.pending_columns.items.len) {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "column index out of range");
        return handle.err_ctx.code;
    }
    const cc = format.CompressionCodec.fromInt(codec) catch {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "unknown compression codec");
        return handle.err_ctx.code;
    };
    handle.pending_columns.items[idx].codec = cc;
    return ZP_OK;
}

/// Set the maximum number of rows per row group (auto-flush when reached).
pub export fn zp_row_writer_set_row_group_size(
    handle_ptr: ?*anyopaque,
    max_rows: i64,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    if (max_rows <= 0) {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "row group size must be positive");
        return handle.err_ctx.code;
    }
    handle.row_group_row_limit = safe.castTo(usize, max_rows) catch {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "row group size out of range");
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

/// Set a key-value metadata entry (can be called at any time before close).
pub export fn zp_row_writer_set_kv_metadata(
    handle_ptr: ?*anyopaque,
    key: ?[*]const u8,
    key_len: usize,
    value: ?[*]const u8,
    value_len: usize,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const k = key orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const v: ?[]const u8 = if (value) |vp| vp[0..value_len] else null;
    handle.setKvMetadata(k[0..key_len], v) catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "setKvMetadata failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

pub export fn zp_row_writer_begin(
    handle_ptr: ?*anyopaque,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.begin() catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "begin failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

// ============================================================================
// Setters
// ============================================================================

pub export fn zp_row_writer_set_null(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "invalid column index");
        return handle.err_ctx.code;
    };
    handle.setNull(col) catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "setNull failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

pub export fn zp_row_writer_set_bool(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
    value: c_int,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "invalid column index");
        return handle.err_ctx.code;
    };
    handle.setBool(col, value != 0) catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "setBool failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

pub export fn zp_row_writer_set_int32(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
    value: i32,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "invalid column index");
        return handle.err_ctx.code;
    };
    handle.setInt32(col, value) catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "setInt32 failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

pub export fn zp_row_writer_set_int64(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
    value: i64,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "invalid column index");
        return handle.err_ctx.code;
    };
    handle.setInt64(col, value) catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "setInt64 failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

pub export fn zp_row_writer_set_float(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
    value: f32,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "invalid column index");
        return handle.err_ctx.code;
    };
    handle.setFloat(col, value) catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "setFloat failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

pub export fn zp_row_writer_set_double(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
    value: f64,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "invalid column index");
        return handle.err_ctx.code;
    };
    handle.setDouble(col, value) catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "setDouble failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

pub export fn zp_row_writer_set_bytes(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
    data: ?[*]const u8,
    len: usize,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "invalid column index");
        return handle.err_ctx.code;
    };
    const ptr = data orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "null data pointer");
        return handle.err_ctx.code;
    };
    handle.setBytes(col, ptr, len) catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "setBytes failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

// ============================================================================
// Schema builder (nested types)
// ============================================================================

pub export fn zp_schema_primitive(
    handle_ptr: ?*anyopaque,
    zp_type: c_int,
) callconv(.c) ?*const anyopaque {
    const handle = castHandle(handle_ptr) orelse return null;
    const info = TypeInfo.fromZpType(zp_type) orelse return null;
    const node = schemaNodeFromTypeInfo(handle, info) catch return null;
    return @ptrCast(node);
}

pub export fn zp_schema_decimal(
    handle_ptr: ?*anyopaque,
    precision: c_int,
    scale: c_int,
) callconv(.c) ?*const anyopaque {
    const handle = castHandle(handle_ptr) orelse return null;
    if (precision < 1 or precision > 38) return null;
    if (scale < 0 or scale > precision) return null;
    const info = TypeInfo.forDecimal(precision, scale);
    const node = schemaNodeFromTypeInfo(handle, info) catch return null;
    return @ptrCast(node);
}

pub export fn zp_schema_optional(
    handle_ptr: ?*anyopaque,
    inner: ?*const anyopaque,
) callconv(.c) ?*const anyopaque {
    const handle = castHandle(handle_ptr) orelse return null;
    const child = castSchemaNode(inner) orelse return null;
    const node = handle.allocSchemaNode(.{ .optional = child }) catch return null;
    return @ptrCast(node);
}

pub export fn zp_schema_list(
    handle_ptr: ?*anyopaque,
    element: ?*const anyopaque,
) callconv(.c) ?*const anyopaque {
    const handle = castHandle(handle_ptr) orelse return null;
    const elem = castSchemaNode(element) orelse return null;
    const node = handle.allocSchemaNode(.{ .list = elem }) catch return null;
    return @ptrCast(node);
}

pub export fn zp_schema_map(
    handle_ptr: ?*anyopaque,
    key_schema: ?*const anyopaque,
    value_schema: ?*const anyopaque,
) callconv(.c) ?*const anyopaque {
    const handle = castHandle(handle_ptr) orelse return null;
    const k = castSchemaNode(key_schema) orelse return null;
    const v = castSchemaNode(value_schema) orelse return null;
    const node = handle.allocSchemaNode(.{ .map = .{ .key = k, .value = v } }) catch return null;
    return @ptrCast(node);
}

pub export fn zp_schema_struct(
    handle_ptr: ?*anyopaque,
    field_names: ?[*]const ?[*:0]const u8,
    field_schemas: ?[*]const ?*const anyopaque,
    field_count: c_int,
) callconv(.c) ?*const anyopaque {
    const handle = castHandle(handle_ptr) orelse return null;
    const names = field_names orelse return null;
    const schemas = field_schemas orelse return null;
    const count = toUsize(field_count) orelse return null;
    if (count == 0) return null;

    const fields = handle.allocSchemaFields(count) catch return null;
    for (0..count) |i| {
        const name_ptr = names[i] orelse return null;
        const schema_ptr = schemas[i] orelse return null;
        fields[i] = .{
            .name = handle.dupeSchemaName(std.mem.sliceTo(name_ptr, 0)) catch return null,
            .node = castSchemaNode(schema_ptr) orelse return null,
        };
    }

    const node = handle.allocSchemaNode(.{ .struct_ = .{ .fields = fields } }) catch return null;
    return @ptrCast(node);
}

pub export fn zp_row_writer_add_column_nested(
    handle_ptr: ?*anyopaque,
    name: ?[*:0]const u8,
    schema: ?*const anyopaque,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const n = name orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const node = castSchemaNode(schema) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.addColumnNested(n, node) catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "addColumnNested failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

fn schemaNodeFromTypeInfo(handle: *RowWriterHandle, info: TypeInfo) !*const SchemaNode {
    return switch (info.physical) {
        .bool_ => handle.allocSchemaNode(.{ .boolean = .{ .logical = info.logical } }),
        .int32 => handle.allocSchemaNode(.{ .int32 = .{ .logical = info.logical } }),
        .int64 => handle.allocSchemaNode(.{ .int64 = .{ .logical = info.logical } }),
        .float_ => handle.allocSchemaNode(.{ .float = .{ .logical = info.logical } }),
        .double_ => handle.allocSchemaNode(.{ .double = .{ .logical = info.logical } }),
        .bytes => handle.allocSchemaNode(.{ .byte_array = .{ .logical = info.logical } }),
        .fixed_bytes => handle.allocSchemaNode(.{ .fixed_len_byte_array = .{
            .len = safe.castTo(u32, info.type_length orelse 0) catch unreachable, // type_length is at most 16
            .logical = info.logical,
        } }),
        .nested => error.InvalidArgument,
    };
}

fn castSchemaNode(ptr: ?*const anyopaque) ?*const SchemaNode {
    return if (ptr) |p| @ptrCast(@alignCast(p)) else null;
}

// ============================================================================
// Nested value builders
// ============================================================================

pub export fn zp_row_writer_begin_list(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.beginList(col) catch |e| return err.mapError(e);
    return ZP_OK;
}

pub export fn zp_row_writer_end_list(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.endList(col) catch |e| return err.mapError(e);
    return ZP_OK;
}

pub export fn zp_row_writer_append_null(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.appendNestedValue(col, .null_val) catch |e| return err.mapError(e);
    return ZP_OK;
}

pub export fn zp_row_writer_append_bool(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
    value: c_int,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.appendNestedValue(col, .{ .bool_val = value != 0 }) catch |e| return err.mapError(e);
    return ZP_OK;
}

pub export fn zp_row_writer_append_int32(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
    value: i32,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.appendNestedValue(col, .{ .int32_val = value }) catch |e| return err.mapError(e);
    return ZP_OK;
}

pub export fn zp_row_writer_append_int64(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
    value: i64,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.appendNestedValue(col, .{ .int64_val = value }) catch |e| return err.mapError(e);
    return ZP_OK;
}

pub export fn zp_row_writer_append_float(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
    value: f32,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.appendNestedValue(col, .{ .float_val = value }) catch |e| return err.mapError(e);
    return ZP_OK;
}

pub export fn zp_row_writer_append_double(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
    value: f64,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.appendNestedValue(col, .{ .double_val = value }) catch |e| return err.mapError(e);
    return ZP_OK;
}

pub export fn zp_row_writer_append_bytes(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
    data: ?[*]const u8,
    len: usize,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const ptr = data orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.appendNestedBytes(col, ptr, len) catch |e| return err.mapError(e);
    return ZP_OK;
}

pub export fn zp_row_writer_begin_struct(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.beginStruct(col) catch |e| return err.mapError(e);
    return ZP_OK;
}

pub export fn zp_row_writer_set_field_null(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
    field_index: c_int,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const fi = toUsize(field_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setStructField(col, fi, .null_val) catch |e| return err.mapError(e);
    return ZP_OK;
}

pub export fn zp_row_writer_set_field_int32(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
    field_index: c_int,
    value: i32,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const fi = toUsize(field_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setStructField(col, fi, .{ .int32_val = value }) catch |e| return err.mapError(e);
    return ZP_OK;
}

pub export fn zp_row_writer_set_field_int64(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
    field_index: c_int,
    value: i64,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const fi = toUsize(field_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setStructField(col, fi, .{ .int64_val = value }) catch |e| return err.mapError(e);
    return ZP_OK;
}

pub export fn zp_row_writer_set_field_float(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
    field_index: c_int,
    value: f32,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const fi = toUsize(field_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setStructField(col, fi, .{ .float_val = value }) catch |e| return err.mapError(e);
    return ZP_OK;
}

pub export fn zp_row_writer_set_field_double(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
    field_index: c_int,
    value: f64,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const fi = toUsize(field_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setStructField(col, fi, .{ .double_val = value }) catch |e| return err.mapError(e);
    return ZP_OK;
}

pub export fn zp_row_writer_set_field_bool(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
    field_index: c_int,
    value: c_int,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const fi = toUsize(field_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setStructField(col, fi, .{ .bool_val = value != 0 }) catch |e| return err.mapError(e);
    return ZP_OK;
}

pub export fn zp_row_writer_set_field_bytes(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
    field_index: c_int,
    data: ?[*]const u8,
    len: usize,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const fi = toUsize(field_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const ptr = data orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setStructFieldBytes(col, fi, ptr, len) catch |e| return err.mapError(e);
    return ZP_OK;
}

pub export fn zp_row_writer_end_struct(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.endStruct(col) catch |e| return err.mapError(e);
    return ZP_OK;
}

pub export fn zp_row_writer_begin_map(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.beginMap(col) catch |e| return err.mapError(e);
    return ZP_OK;
}

pub export fn zp_row_writer_begin_map_entry(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.beginMapEntry(col) catch |e| return err.mapError(e);
    return ZP_OK;
}

pub export fn zp_row_writer_end_map_entry(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.endMapEntry(col) catch |e| return err.mapError(e);
    return ZP_OK;
}

pub export fn zp_row_writer_end_map(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.endMap(col) catch |e| return err.mapError(e);
    return ZP_OK;
}

// ============================================================================
// Commit / Flush / Close
// ============================================================================

pub export fn zp_row_writer_add_row(
    handle_ptr: ?*anyopaque,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.addRow() catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "addRow failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

pub export fn zp_row_writer_flush(
    handle_ptr: ?*anyopaque,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.flush() catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "flush failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

pub export fn zp_row_writer_close(
    handle_ptr: ?*anyopaque,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.writerClose() catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "close failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

// ============================================================================
// Buffer access (memory backend)
// ============================================================================

pub export fn zp_row_writer_get_buffer(
    handle_ptr: ?*anyopaque,
    data_out: ?*[*]const u8,
    len_out: ?*usize,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const d_out = data_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const l_out = len_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;

    switch (handle.backend) {
        .buffer => |bt| {
            const w = bt.written();
            d_out.* = w.ptr;
            l_out.* = w.len;
            return ZP_OK;
        },
        else => {
            handle.err_ctx.setError(err.ZP_ERROR_INVALID_STATE, "get_buffer only valid for memory backend");
            return handle.err_ctx.code;
        },
    }
}

// ============================================================================
// Error and free
// ============================================================================

pub export fn zp_row_writer_error_code(handle_ptr: ?*anyopaque) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    return handle.err_ctx.code;
}

pub export fn zp_row_writer_error_message(handle_ptr: ?*anyopaque) callconv(.c) [*:0]const u8 {
    const handle = castHandle(handle_ptr) orelse return "";
    return handle.err_ctx.message();
}

pub export fn zp_row_writer_free(handle_ptr: ?*anyopaque) callconv(.c) void {
    const handle = castHandle(handle_ptr) orelse return;
    handle.deinit();
}

// ============================================================================
// Internal helpers
// ============================================================================

fn castHandle(ptr: ?*anyopaque) ?*RowWriterHandle {
    return if (ptr) |p| @ptrCast(@alignCast(p)) else null;
}

fn toUsize(v: c_int) ?usize {
    if (v < 0) return null;
    return safe.castTo(usize, v) catch unreachable; // guarded by v < 0 check above
}
