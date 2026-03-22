//! C ABI row reader entry points.
//!
//! Cursor-based row-oriented API for reading Parquet files without Arrow
//! knowledge. Backed by DynamicReader internally.
//!
//! Workflow:
//!   1. Open with zp_row_reader_open_*
//!   2. Inspect schema with get_column_count / get_column_name
//!   3. Load a row group with zp_row_reader_read_row_group
//!   4. Iterate with zp_row_reader_next
//!   5. Access values with get_int32 / get_bytes / etc.
//!   6. Close with zp_row_reader_close

const std = @import("std");
const builtin = @import("builtin");
const err = @import("error.zig");
const handles = @import("handles.zig");
const safe = @import("../../core/safe.zig");
const value_mod = @import("../../core/value.zig");
const format = @import("../../core/format.zig");

const Value = value_mod.Value;
const RowReaderHandle = handles.RowReaderHandle;

const ZP_OK = err.ZP_OK;
const is_wasm = builtin.cpu.arch == .wasm32 or builtin.cpu.arch == .wasm64;

// ============================================================================
// Open
// ============================================================================

pub export fn zp_row_reader_open_memory(
    data: ?[*]const u8,
    len: usize,
    handle_out: ?*?*anyopaque,
) callconv(.c) c_int {
    const out = handle_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    out.* = null;
    const ptr = data orelse return err.ZP_ERROR_INVALID_ARGUMENT;

    const handle = RowReaderHandle.openMemory(ptr, len) catch |e| return err.mapError(e);
    out.* = @ptrCast(handle);
    return ZP_OK;
}

pub export fn zp_row_reader_open_callbacks(
    ctx: ?*anyopaque,
    read_at_fn: ?*const fn (?*anyopaque, u64, [*]u8, usize, *usize) callconv(.c) c_int,
    size_fn: ?*const fn (?*anyopaque) callconv(.c) u64,
    handle_out: ?*?*anyopaque,
) callconv(.c) c_int {
    const out = handle_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    out.* = null;
    const read_fn = read_at_fn orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const sz_fn = size_fn orelse return err.ZP_ERROR_INVALID_ARGUMENT;

    const handle = RowReaderHandle.openCallbacks(ctx, read_fn, sz_fn) catch |e| return err.mapError(e);
    out.* = @ptrCast(handle);
    return ZP_OK;
}

pub export fn zp_row_reader_open_file(
    path: ?[*:0]const u8,
    handle_out: ?*?*anyopaque,
) callconv(.c) c_int {
    if (is_wasm) return err.ZP_ERROR_UNSUPPORTED;
    const out = handle_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    out.* = null;
    const p = path orelse return err.ZP_ERROR_INVALID_ARGUMENT;

    const handle = RowReaderHandle.openFile(p) catch |e| return err.mapError(e);
    out.* = @ptrCast(handle);
    return ZP_OK;
}

// ============================================================================
// Metadata
// ============================================================================

pub export fn zp_row_reader_get_num_row_groups(
    handle_ptr: ?*anyopaque,
    count_out: ?*c_int,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const out = count_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    out.* = safe.castTo(c_int, handle.reader.metadata.row_groups.len) catch {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_DATA, "row group count too large");
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

pub export fn zp_row_reader_get_num_rows(
    handle_ptr: ?*anyopaque,
    count_out: ?*i64,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const out = count_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    var total: i64 = 0;
    for (handle.reader.metadata.row_groups) |rg| {
        total = std.math.add(i64, total, rg.num_rows) catch {
            handle.err_ctx.setError(err.ZP_ERROR_INVALID_DATA, "row count overflow");
            return handle.err_ctx.code;
        };
    }
    out.* = total;
    return ZP_OK;
}

pub export fn zp_row_reader_get_column_count(
    handle_ptr: ?*anyopaque,
    count_out: ?*c_int,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const out = count_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    out.* = safe.castTo(c_int, handle.num_top_columns) catch {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_DATA, "column count too large");
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

/// Get the name of a top-level column. Returns NULL for invalid index.
/// The returned string is valid for the lifetime of the handle.
pub export fn zp_row_reader_get_column_name(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
) callconv(.c) ?[*:0]const u8 {
    const handle = castHandle(handle_ptr) orelse return null;
    const idx = safe.castTo(usize, col_index) catch return null;
    if (idx >= handle.col_names.len) return null;
    return handle.col_names[idx];
}

// ============================================================================
// Row group loading and iteration
// ============================================================================

/// Load a row group for row-by-row iteration.
/// Any previously loaded rows are freed. Cursor resets to before the first row.
pub export fn zp_row_reader_read_row_group(
    handle_ptr: ?*anyopaque,
    rg_index: c_int,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.err_ctx.setOk();

    if (rg_index < 0) {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "negative row group index");
        return handle.err_ctx.code;
    }
    const idx = safe.castTo(usize, rg_index) catch {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "row group index out of range");
        return handle.err_ctx.code;
    };
    if (idx >= handle.reader.metadata.row_groups.len) {
        handle.err_ctx.setErrorFmt(err.ZP_ERROR_INVALID_ARGUMENT, "row group index {d} >= {d}", .{ idx, handle.reader.metadata.row_groups.len });
        return handle.err_ctx.code;
    }

    handle.readRowGroup(idx) catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "readRowGroup failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

/// Load a row group with column projection.
/// Only the specified top-level columns are read. If col_indices is null, all columns are read.
pub export fn zp_row_reader_read_row_group_projected(
    handle_ptr: ?*anyopaque,
    rg_index: c_int,
    col_indices: ?[*]const c_int,
    num_cols: c_int,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.err_ctx.setOk();

    if (rg_index < 0) {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "negative row group index");
        return handle.err_ctx.code;
    }
    const idx = safe.castTo(usize, rg_index) catch {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "row group index out of range");
        return handle.err_ctx.code;
    };
    if (idx >= handle.reader.metadata.row_groups.len) {
        handle.err_ctx.setErrorFmt(err.ZP_ERROR_INVALID_ARGUMENT, "row group index {d} >= {d}", .{ idx, handle.reader.metadata.row_groups.len });
        return handle.err_ctx.code;
    }

    if (col_indices == null) {
        handle.readRowGroup(idx) catch |e| {
            handle.err_ctx.setErrorFmt(err.mapError(e), "readRowGroup failed: {s}", .{err.errorMessage(e)});
            return handle.err_ctx.code;
        };
        return ZP_OK;
    }

    if (num_cols < 0) {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "negative num_cols");
        return handle.err_ctx.code;
    }
    const n: usize = safe.castTo(usize, num_cols) catch {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "num_cols out of range");
        return handle.err_ctx.code;
    };

    var zig_indices = handle.allocator.alloc(usize, n) catch {
        handle.err_ctx.setError(err.ZP_ERROR_OUT_OF_MEMORY, "alloc failed");
        return handle.err_ctx.code;
    };
    defer handle.allocator.free(zig_indices);

    for (0..n) |i| {
        zig_indices[i] = safe.castTo(usize, col_indices.?[i]) catch {
            handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "negative column index");
            return handle.err_ctx.code;
        };
    }

    handle.readRowGroupProjected(idx, zig_indices) catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "readRowGroupProjected failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

/// Advance to the next row. Returns ZP_OK if a row is available,
/// ZP_ROW_END when all rows have been consumed.
pub export fn zp_row_reader_next(handle_ptr: ?*anyopaque) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    if (handle.next()) return ZP_OK;
    return err.ZP_ROW_END;
}

// ============================================================================
// Current row value access
// ============================================================================

/// Get the value type at a column in the current row.
/// Returns logical type (ZP_TYPE_STRING, ZP_TYPE_DATE, etc.) when available,
/// otherwise the physical type. Returns ZP_TYPE_NULL for null values.
pub export fn zp_row_reader_get_type(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_TYPE_NULL;
    const val = getValueAt(handle_ptr, col_index) orelse return err.ZP_TYPE_NULL;
    if (val.isNull()) return err.ZP_TYPE_NULL;
    const idx = safe.castTo(usize, col_index) catch return physicalValueType(val);
    return columnType(&handle.reader, idx, val);
}

/// Returns 1 if the value is null, 0 otherwise.
pub export fn zp_row_reader_is_null(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
) callconv(.c) c_int {
    const val = getValueAt(handle_ptr, col_index) orelse return 1;
    return if (val.isNull()) @as(c_int, 1) else @as(c_int, 0);
}

pub export fn zp_row_reader_get_int32(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
) callconv(.c) i32 {
    const val = getValueAt(handle_ptr, col_index) orelse return 0;
    return val.asInt32() orelse 0;
}

pub export fn zp_row_reader_get_int64(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
) callconv(.c) i64 {
    const val = getValueAt(handle_ptr, col_index) orelse return 0;
    return val.asInt64() orelse 0;
}

pub export fn zp_row_reader_get_float(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
) callconv(.c) f32 {
    const val = getValueAt(handle_ptr, col_index) orelse return 0;
    return val.asFloat() orelse 0;
}

pub export fn zp_row_reader_get_double(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
) callconv(.c) f64 {
    const val = getValueAt(handle_ptr, col_index) orelse return 0;
    return val.asDouble() orelse 0;
}

/// Returns 1 if true, 0 if false or if the value is not boolean.
pub export fn zp_row_reader_get_bool(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
) callconv(.c) c_int {
    const val = getValueAt(handle_ptr, col_index) orelse return 0;
    return if (val.asBool() orelse false) @as(c_int, 1) else @as(c_int, 0);
}

/// Get byte/string data from the current row.
/// On success, *data_out and *len_out are set. The data pointer is valid
/// until the next read_row_group or close call.
/// Returns ZP_OK on success. On type mismatch, outputs are set to empty.
pub export fn zp_row_reader_get_bytes(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
    data_out: ?*[*]const u8,
    len_out: ?*usize,
) callconv(.c) c_int {
    const d_out = data_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const l_out = len_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;

    const val = getValueAt(handle_ptr, col_index) orelse {
        d_out.* = "";
        l_out.* = 0;
        return err.ZP_ERROR_INVALID_STATE;
    };

    const bytes = val.asBytes() orelse {
        d_out.* = "";
        l_out.* = 0;
        return ZP_OK;
    };
    d_out.* = bytes.ptr;
    l_out.* = bytes.len;
    return ZP_OK;
}

// ============================================================================
// Schema-level column type accessors
// ============================================================================

/// Get the schema-level type for a column (available before iterating rows).
pub export fn zp_row_reader_get_column_type(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_TYPE_NULL;
    const idx = safe.castTo(usize, col_index) catch return err.ZP_TYPE_NULL;
    return schemaColumnType(&handle.reader, idx);
}

/// Get decimal precision for a DECIMAL column. Returns 0 for non-decimal.
pub export fn zp_row_reader_get_decimal_precision(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return 0;
    const idx = safe.castTo(usize, col_index) catch return 0;
    const elem = handle.reader.getLeafSchemaElement(idx) orelse return 0;
    if (elem.logical_type) |lt| {
        if (lt == .decimal) return lt.decimal.precision;
    }
    return elem.precision orelse 0;
}

/// Get decimal scale for a DECIMAL column. Returns 0 for non-decimal.
pub export fn zp_row_reader_get_decimal_scale(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return 0;
    const idx = safe.castTo(usize, col_index) catch return 0;
    const elem = handle.reader.getLeafSchemaElement(idx) orelse return 0;
    if (elem.logical_type) |lt| {
        if (lt == .decimal) return lt.decimal.scale;
    }
    return elem.scale orelse 0;
}

/// Get the number of key-value metadata entries.
pub export fn zp_row_reader_get_kv_metadata_count(
    handle_ptr: ?*anyopaque,
    count_out: ?*c_int,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const out = count_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const kvs = handle.reader.metadata.key_value_metadata orelse {
        out.* = 0;
        return ZP_OK;
    };
    out.* = safe.castTo(c_int, kvs.len) catch {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_DATA, "metadata count too large");
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

/// Get a key-value metadata key by index.
pub export fn zp_row_reader_get_kv_metadata_key(
    handle_ptr: ?*anyopaque,
    index: c_int,
    key_out: ?*[*]const u8,
    len_out: ?*usize,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const k_out = key_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const l_out = len_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const kvs = handle.reader.metadata.key_value_metadata orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "no key-value metadata");
        return handle.err_ctx.code;
    };
    const idx = safe.castTo(usize, index) catch {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "invalid metadata index");
        return handle.err_ctx.code;
    };
    if (idx >= kvs.len) {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "metadata index out of range");
        return handle.err_ctx.code;
    }
    k_out.* = kvs[idx].key.ptr;
    l_out.* = kvs[idx].key.len;
    return ZP_OK;
}

/// Get a key-value metadata value by index.
pub export fn zp_row_reader_get_kv_metadata_value(
    handle_ptr: ?*anyopaque,
    index: c_int,
    val_out: ?*[*]const u8,
    len_out: ?*usize,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const v_out = val_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const l_out = len_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const kvs = handle.reader.metadata.key_value_metadata orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "no key-value metadata");
        return handle.err_ctx.code;
    };
    const idx = safe.castTo(usize, index) catch {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "invalid metadata index");
        return handle.err_ctx.code;
    };
    if (idx >= kvs.len) {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "metadata index out of range");
        return handle.err_ctx.code;
    }
    const val = kvs[idx].value orelse {
        v_out.* = "".ptr;
        l_out.* = 0;
        return ZP_OK;
    };
    v_out.* = val.ptr;
    l_out.* = val.len;
    return ZP_OK;
}

/// Set checksum validation options for the row reader.
pub export fn zp_row_reader_set_checksum_validation(
    handle_ptr: ?*anyopaque,
    validate: c_int,
    strict: c_int,
) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.reader.checksum_options = .{
        .validate_page_checksum = validate != 0,
        .strict_checksum = strict != 0,
    };
    return ZP_OK;
}

// ============================================================================
// Error and close
// ============================================================================

pub export fn zp_row_reader_error_code(handle_ptr: ?*anyopaque) callconv(.c) c_int {
    const handle = castHandle(handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    return handle.err_ctx.code;
}

pub export fn zp_row_reader_error_message(handle_ptr: ?*anyopaque) callconv(.c) [*:0]const u8 {
    const handle = castHandle(handle_ptr) orelse return "";
    return handle.err_ctx.message();
}

pub export fn zp_row_reader_close(handle_ptr: ?*anyopaque) callconv(.c) void {
    const handle = castHandle(handle_ptr) orelse return;
    handle.close();
}

// ============================================================================
// Value handle API (nested type traversal)
// ============================================================================

/// Get an opaque value handle from the current row at the given column.
/// The returned handle is valid until the next read_row_group or close call.
pub export fn zp_row_reader_get_value(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
) callconv(.c) ?*const anyopaque {
    const ref = getValueRefAt(handle_ptr, col_index) orelse return null;
    return @ptrCast(ref);
}

pub export fn zp_value_get_type(value_ptr: ?*const anyopaque) callconv(.c) c_int {
    const val = castValue(value_ptr) orelse return err.ZP_TYPE_NULL;
    return physicalValueType(val.*);
}

pub export fn zp_value_is_null(value_ptr: ?*const anyopaque) callconv(.c) c_int {
    const val = castValue(value_ptr) orelse return 1;
    return if (val.isNull()) @as(c_int, 1) else @as(c_int, 0);
}

pub export fn zp_value_get_int32(value_ptr: ?*const anyopaque) callconv(.c) i32 {
    const val = castValue(value_ptr) orelse return 0;
    return val.asInt32() orelse 0;
}

pub export fn zp_value_get_int64(value_ptr: ?*const anyopaque) callconv(.c) i64 {
    const val = castValue(value_ptr) orelse return 0;
    return val.asInt64() orelse 0;
}

pub export fn zp_value_get_float(value_ptr: ?*const anyopaque) callconv(.c) f32 {
    const val = castValue(value_ptr) orelse return 0;
    return val.asFloat() orelse 0;
}

pub export fn zp_value_get_double(value_ptr: ?*const anyopaque) callconv(.c) f64 {
    const val = castValue(value_ptr) orelse return 0;
    return val.asDouble() orelse 0;
}

pub export fn zp_value_get_bool(value_ptr: ?*const anyopaque) callconv(.c) c_int {
    const val = castValue(value_ptr) orelse return 0;
    return if (val.asBool() orelse false) @as(c_int, 1) else @as(c_int, 0);
}

pub export fn zp_value_get_bytes(
    value_ptr: ?*const anyopaque,
    data_out: ?*[*]const u8,
    len_out: ?*usize,
) callconv(.c) c_int {
    const d_out = data_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const l_out = len_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const val = castValue(value_ptr) orelse {
        d_out.* = "";
        l_out.* = 0;
        return err.ZP_ERROR_INVALID_ARGUMENT;
    };
    const bytes = val.asBytes() orelse {
        d_out.* = "";
        l_out.* = 0;
        return ZP_OK;
    };
    d_out.* = bytes.ptr;
    l_out.* = bytes.len;
    return ZP_OK;
}

pub export fn zp_value_get_list_len(value_ptr: ?*const anyopaque) callconv(.c) c_int {
    const val = castValue(value_ptr) orelse return 0;
    const items = val.asList() orelse return 0;
    return safe.castTo(c_int, items.len) catch 0;
}

pub export fn zp_value_get_list_element(
    value_ptr: ?*const anyopaque,
    index: c_int,
) callconv(.c) ?*const anyopaque {
    const val = castValue(value_ptr) orelse return null;
    const items = val.asList() orelse return null;
    const idx = safe.castTo(usize, index) catch return null;
    if (idx >= items.len) return null;
    return @ptrCast(&items[idx]);
}

pub export fn zp_value_get_map_len(value_ptr: ?*const anyopaque) callconv(.c) c_int {
    const val = castValue(value_ptr) orelse return 0;
    const entries = val.asMap() orelse return 0;
    return safe.castTo(c_int, entries.len) catch 0;
}

pub export fn zp_value_get_map_key(
    value_ptr: ?*const anyopaque,
    entry_index: c_int,
) callconv(.c) ?*const anyopaque {
    const val = castValue(value_ptr) orelse return null;
    const entries = val.asMap() orelse return null;
    const idx = safe.castTo(usize, entry_index) catch return null;
    if (idx >= entries.len) return null;
    return @ptrCast(&entries[idx].key);
}

pub export fn zp_value_get_map_value(
    value_ptr: ?*const anyopaque,
    entry_index: c_int,
) callconv(.c) ?*const anyopaque {
    const val = castValue(value_ptr) orelse return null;
    const entries = val.asMap() orelse return null;
    const idx = safe.castTo(usize, entry_index) catch return null;
    if (idx >= entries.len) return null;
    return @ptrCast(&entries[idx].value);
}

pub export fn zp_value_get_struct_field_count(value_ptr: ?*const anyopaque) callconv(.c) c_int {
    const val = castValue(value_ptr) orelse return 0;
    const fields = val.asStruct() orelse return 0;
    return safe.castTo(c_int, fields.len) catch 0;
}

pub export fn zp_value_get_struct_field_name(
    value_ptr: ?*const anyopaque,
    field_index: c_int,
    name_out: ?*[*]const u8,
    len_out: ?*usize,
) callconv(.c) c_int {
    const n_out = name_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const l_out = len_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const val = castValue(value_ptr) orelse {
        n_out.* = "";
        l_out.* = 0;
        return err.ZP_ERROR_INVALID_ARGUMENT;
    };
    const fields = val.asStruct() orelse {
        n_out.* = "";
        l_out.* = 0;
        return err.ZP_ERROR_INVALID_ARGUMENT;
    };
    const idx = safe.castTo(usize, field_index) catch {
        n_out.* = "";
        l_out.* = 0;
        return err.ZP_ERROR_INVALID_ARGUMENT;
    };
    if (idx >= fields.len) {
        n_out.* = "";
        l_out.* = 0;
        return err.ZP_ERROR_INVALID_ARGUMENT;
    }
    n_out.* = fields[idx].name.ptr;
    l_out.* = fields[idx].name.len;
    return ZP_OK;
}

pub export fn zp_value_get_struct_field_value(
    value_ptr: ?*const anyopaque,
    field_index: c_int,
) callconv(.c) ?*const anyopaque {
    const val = castValue(value_ptr) orelse return null;
    const fields = val.asStruct() orelse return null;
    const idx = safe.castTo(usize, field_index) catch return null;
    if (idx >= fields.len) return null;
    return @ptrCast(&fields[idx].value);
}

// ============================================================================
// Internal helpers
// ============================================================================

fn castHandle(ptr: ?*anyopaque) ?*RowReaderHandle {
    return if (ptr) |p| @ptrCast(@alignCast(p)) else null;
}

fn castValue(ptr: ?*const anyopaque) ?*const Value {
    return if (ptr) |p| @ptrCast(@alignCast(p)) else null;
}

fn getValueAt(handle_ptr: ?*anyopaque, col_index: c_int) ?Value {
    const handle = castHandle(handle_ptr) orelse return null;
    const row = handle.currentRow() orelse return null;
    const idx = safe.castTo(usize, col_index) catch return null;
    if (idx >= row.values.len) return null;
    return row.values[idx];
}

fn getValueRefAt(handle_ptr: ?*anyopaque, col_index: c_int) ?*const Value {
    const handle = castHandle(handle_ptr) orelse return null;
    const row = handle.currentRow() orelse return null;
    const idx = safe.castTo(usize, col_index) catch return null;
    if (idx >= row.values.len) return null;
    return &row.values[idx];
}

const core_dynamic = @import("../../core/dynamic_reader.zig");

fn physicalValueType(v: Value) c_int {
    return switch (v) {
        .null_val => err.ZP_TYPE_NULL,
        .bool_val => err.ZP_TYPE_BOOL,
        .int32_val => err.ZP_TYPE_INT32,
        .int64_val => err.ZP_TYPE_INT64,
        .float_val => err.ZP_TYPE_FLOAT,
        .double_val => err.ZP_TYPE_DOUBLE,
        .bytes_val, .fixed_bytes_val => err.ZP_TYPE_BYTES,
        .list_val => err.ZP_TYPE_LIST,
        .map_val => err.ZP_TYPE_MAP,
        .struct_val => err.ZP_TYPE_STRUCT,
    };
}

fn logicalTypeToZpType(lt: format.LogicalType) c_int {
    return switch (lt) {
        .string => err.ZP_TYPE_STRING,
        .date => err.ZP_TYPE_DATE,
        .timestamp => |ts| switch (ts.unit) {
            .millis => err.ZP_TYPE_TIMESTAMP_MILLIS,
            .micros => err.ZP_TYPE_TIMESTAMP_MICROS,
            .nanos => err.ZP_TYPE_TIMESTAMP_NANOS,
        },
        .time => |t| switch (t.unit) {
            .millis => err.ZP_TYPE_TIME_MILLIS,
            .micros => err.ZP_TYPE_TIME_MICROS,
            .nanos => err.ZP_TYPE_TIME_NANOS,
        },
        .int => |i| {
            if (i.is_signed) {
                return switch (i.bit_width) {
                    8 => err.ZP_TYPE_INT8,
                    16 => err.ZP_TYPE_INT16,
                    else => err.ZP_TYPE_INT32,
                };
            } else {
                return switch (i.bit_width) {
                    8 => err.ZP_TYPE_UINT8,
                    16 => err.ZP_TYPE_UINT16,
                    32 => err.ZP_TYPE_UINT32,
                    64 => err.ZP_TYPE_UINT64,
                    else => err.ZP_TYPE_INT32,
                };
            }
        },
        .decimal => err.ZP_TYPE_DECIMAL,
        .uuid => err.ZP_TYPE_UUID,
        .json => err.ZP_TYPE_JSON,
        .enum_ => err.ZP_TYPE_ENUM,
        .float16 => err.ZP_TYPE_FLOAT16,
        .bson => err.ZP_TYPE_BSON,
        .geometry => err.ZP_TYPE_GEOMETRY,
        .geography => err.ZP_TYPE_GEOGRAPHY,
    };
}

fn convertedTypeToZpType(ct: i32) ?c_int {
    return switch (ct) {
        format.ConvertedType.UTF8 => err.ZP_TYPE_STRING,
        format.ConvertedType.DATE => err.ZP_TYPE_DATE,
        format.ConvertedType.TIMESTAMP_MILLIS => err.ZP_TYPE_TIMESTAMP_MILLIS,
        format.ConvertedType.TIMESTAMP_MICROS => err.ZP_TYPE_TIMESTAMP_MICROS,
        format.ConvertedType.TIME_MILLIS => err.ZP_TYPE_TIME_MILLIS,
        format.ConvertedType.TIME_MICROS => err.ZP_TYPE_TIME_MICROS,
        format.ConvertedType.INT_8 => err.ZP_TYPE_INT8,
        format.ConvertedType.INT_16 => err.ZP_TYPE_INT16,
        format.ConvertedType.UINT_8 => err.ZP_TYPE_UINT8,
        format.ConvertedType.UINT_16 => err.ZP_TYPE_UINT16,
        format.ConvertedType.UINT_32 => err.ZP_TYPE_UINT32,
        format.ConvertedType.UINT_64 => err.ZP_TYPE_UINT64,
        format.ConvertedType.DECIMAL => err.ZP_TYPE_DECIMAL,
        format.ConvertedType.JSON => err.ZP_TYPE_JSON,
        format.ConvertedType.BSON => err.ZP_TYPE_BSON,
        format.ConvertedType.ENUM => err.ZP_TYPE_ENUM,
        format.ConvertedType.INTERVAL => err.ZP_TYPE_INTERVAL,
        else => null,
    };
}

fn columnType(reader: *const core_dynamic.DynamicReader, col_idx: usize, v: Value) c_int {
    if (reader.getColumnLogicalType(col_idx)) |lt| {
        return logicalTypeToZpType(lt);
    }
    const elem = reader.getLeafSchemaElement(col_idx) orelse return physicalValueType(v);
    if (elem.converted_type) |ct| {
        if (convertedTypeToZpType(ct)) |zpt| return zpt;
    }
    return physicalValueType(v);
}

fn schemaColumnType(reader: *const core_dynamic.DynamicReader, col_idx: usize) c_int {
    if (reader.getColumnLogicalType(col_idx)) |lt| {
        return logicalTypeToZpType(lt);
    }
    const elem = reader.getLeafSchemaElement(col_idx) orelse return err.ZP_TYPE_NULL;
    if (elem.converted_type) |ct| {
        if (convertedTypeToZpType(ct)) |zpt| return zpt;
    }
    const pt = elem.type_ orelse return err.ZP_TYPE_NULL;
    return switch (pt) {
        .boolean => err.ZP_TYPE_BOOL,
        .int32 => err.ZP_TYPE_INT32,
        .int64 => err.ZP_TYPE_INT64,
        .float => err.ZP_TYPE_FLOAT,
        .double => err.ZP_TYPE_DOUBLE,
        .byte_array, .fixed_len_byte_array => err.ZP_TYPE_BYTES,
        .int96 => err.ZP_TYPE_INT64,
    };
}
