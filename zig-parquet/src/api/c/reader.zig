//! C ABI reader entry points.
//!
//! All functions return `c_int` status codes (ZP_OK on success).
//! Error details are available via `zp_reader_error_message` / `zp_reader_error_code`.

const std = @import("std");
const builtin = @import("builtin");
const err = @import("error.zig");
const handles = @import("handles.zig");
const arrow_batch = @import("../../core/arrow_batch.zig");
const safe = @import("../../core/safe.zig");

const ArrowSchema = handles.ArrowSchema;
const ArrowArray = handles.ArrowArray;
const ReaderHandle = handles.ReaderHandle;

const ZP_OK = err.ZP_OK;
const is_wasm = builtin.cpu.arch == .wasm32 or builtin.cpu.arch == .wasm64;

/// Open a Parquet reader from an in-memory buffer.
///
/// The buffer must remain valid for the lifetime of the reader handle.
/// On success, `*handle_out` is set to a valid opaque handle.
/// On failure, `*handle_out` is null and the return code indicates the error.
pub export fn zp_reader_open_memory(
    data: ?[*]const u8,
    len: usize,
    handle_out: ?*?*anyopaque,
) callconv(.c) c_int {
    const out = handle_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    out.* = null;
    const ptr = data orelse return err.ZP_ERROR_INVALID_ARGUMENT;

    const handle = ReaderHandle.openMemory(ptr, len) catch |e| {
        return err.mapError(e);
    };
    out.* = @ptrCast(handle);
    return ZP_OK;
}

/// Open a Parquet reader using caller-provided random-access callbacks.
///
/// The callback context and function pointers must remain valid for the
/// lifetime of the reader handle. The library invokes callbacks synchronously.
pub export fn zp_reader_open_callbacks(
    ctx: ?*anyopaque,
    read_at_fn: ?*const fn (?*anyopaque, u64, [*]u8, usize, *usize) callconv(.c) c_int,
    size_fn: ?*const fn (?*anyopaque) callconv(.c) u64,
    handle_out: ?*?*anyopaque,
) callconv(.c) c_int {
    const out = handle_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    out.* = null;
    const read_fn = read_at_fn orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const sz_fn = size_fn orelse return err.ZP_ERROR_INVALID_ARGUMENT;

    const handle = ReaderHandle.openCallbacks(ctx, read_fn, sz_fn) catch |e| {
        return err.mapError(e);
    };
    out.* = @ptrCast(handle);
    return ZP_OK;
}

/// Open a Parquet reader from a file path (non-WASM only).
pub export fn zp_reader_open_file(
    path: ?[*:0]const u8,
    handle_out: ?*?*anyopaque,
) callconv(.c) c_int {
    if (is_wasm) return err.ZP_ERROR_UNSUPPORTED;
    const out = handle_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    out.* = null;
    const p = path orelse return err.ZP_ERROR_INVALID_ARGUMENT;

    const handle = ReaderHandle.openFile(p) catch |e| {
        return err.mapError(e);
    };
    out.* = @ptrCast(handle);
    return ZP_OK;
}

/// Get the number of row groups in the file.
pub export fn zp_reader_get_num_row_groups(
    handle_ptr: ?*anyopaque,
    count_out: ?*c_int,
) callconv(.c) c_int {
    const handle = castHandle(ReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const out = count_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    out.* = safe.castTo(c_int, handle.metadata.row_groups.len) catch {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_DATA, "row group count too large");
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

/// Get the total number of rows across all row groups.
pub export fn zp_reader_get_num_rows(
    handle_ptr: ?*anyopaque,
    count_out: ?*i64,
) callconv(.c) c_int {
    const handle = castHandle(ReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const out = count_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    var total: i64 = 0;
    for (handle.metadata.row_groups) |rg| {
        total = std.math.add(i64, total, rg.num_rows) catch {
            handle.err_ctx.setError(err.ZP_ERROR_INVALID_DATA, "row count overflow");
            return handle.err_ctx.code;
        };
    }
    out.* = total;
    return ZP_OK;
}

/// Get the number of rows in a specific row group.
pub export fn zp_reader_get_row_group_num_rows(
    handle_ptr: ?*anyopaque,
    rg_index: c_int,
    count_out: ?*i64,
) callconv(.c) c_int {
    const handle = castHandle(ReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const out = count_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    if (rg_index < 0) {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "negative row group index");
        return handle.err_ctx.code;
    }
    const idx = safe.castTo(usize, rg_index) catch {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "row group index out of range");
        return handle.err_ctx.code;
    };
    if (idx >= handle.metadata.row_groups.len) {
        handle.err_ctx.setErrorFmt(err.ZP_ERROR_INVALID_ARGUMENT, "row group index {d} >= {d}", .{ idx, handle.metadata.row_groups.len });
        return handle.err_ctx.code;
    }
    out.* = handle.metadata.row_groups[idx].num_rows;
    return ZP_OK;
}

/// Get the number of leaf (physical) columns.
pub export fn zp_reader_get_column_count(
    handle_ptr: ?*anyopaque,
    count_out: ?*c_int,
) callconv(.c) c_int {
    const handle = castHandle(ReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const out = count_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    // Count leaf columns (physical columns) from schema -- same as number of
    // columns in the first row group, or derived from schema element count.
    // The first schema element is the root, so leaf count = total - groups.
    // Simplest: use column count from first row group metadata.
    if (handle.metadata.row_groups.len > 0) {
        out.* = safe.castTo(c_int, handle.metadata.row_groups[0].columns.len) catch {
            handle.err_ctx.setError(err.ZP_ERROR_INVALID_DATA, "column count too large");
            return handle.err_ctx.code;
        };
    } else {
        // No row groups -- count leaf elements from schema
        // Schema element 0 is root; leaves are elements without children
        var leaf_count: c_int = 0;
        if (handle.metadata.schema.len > 1) {
            // Simple heuristic: elements with num_children == null or 0 are leaves
            for (handle.metadata.schema[1..]) |elem| {
                if (elem.num_children == null or elem.num_children.? == 0) {
                    leaf_count += 1;
                }
            }
        }
        out.* = leaf_count;
    }
    return ZP_OK;
}

/// Export the file schema as an ArrowSchema.
///
/// The caller owns the returned schema and must call its `release` callback
/// when done. The schema is valid independently of the reader handle.
pub export fn zp_reader_get_schema(
    handle_ptr: ?*anyopaque,
    schema_out: ?*ArrowSchema,
) callconv(.c) c_int {
    const handle = castHandle(ReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const out = schema_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;

    handle.err_ctx.setOk();
    const schema = arrow_batch.exportSchemaAsArrow(handle.allocator, handle.metadata) catch |e| {
        handle.err_ctx.setError(err.mapError(e), err.errorMessage(e));
        return handle.err_ctx.code;
    };
    out.* = schema;
    return ZP_OK;
}

/// Read a row group as Arrow arrays.
///
/// Decodes the specified columns from row group `rg_index` into Arrow arrays.
/// If `col_indices` is null, all columns are read. `num_cols` is ignored when
/// `col_indices` is null.
///
/// On success, `arrays_out` receives a struct-of-arrays ArrowArray (children
/// are the individual column arrays), and `schema_out` receives the
/// corresponding schema. Both must be released by the caller.
pub export fn zp_reader_read_row_group(
    handle_ptr: ?*anyopaque,
    rg_index: c_int,
    col_indices: ?[*]const c_int,
    num_cols: c_int,
    arrays_out: ?*ArrowArray,
    schema_out: ?*ArrowSchema,
) callconv(.c) c_int {
    const handle = castHandle(ReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const a_out = arrays_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const s_out = schema_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.err_ctx.setOk();

    if (rg_index < 0) {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "negative row group index");
        return handle.err_ctx.code;
    }

    const rg_idx = safe.castTo(usize, rg_index) catch {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "row group index out of range");
        return handle.err_ctx.code;
    };

    // Convert c_int column indices to usize
    var zig_indices: ?[]const usize = null;
    var indices_buf: []usize = &.{};
    defer if (indices_buf.len > 0) handle.allocator.free(indices_buf);

    if (col_indices) |ci| {
        if (num_cols <= 0) {
            handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "num_cols must be positive when col_indices is provided");
            return handle.err_ctx.code;
        }
        const nc = safe.castTo(usize, num_cols) catch {
            handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "num_cols out of range");
            return handle.err_ctx.code;
        };
        indices_buf = handle.allocator.alloc(usize, nc) catch {
            handle.err_ctx.setError(err.ZP_ERROR_OUT_OF_MEMORY, "OutOfMemory");
            return handle.err_ctx.code;
        };
        for (0..nc) |i| {
            indices_buf[i] = safe.castTo(usize, ci[i]) catch {
                handle.err_ctx.setErrorFmt(err.ZP_ERROR_INVALID_ARGUMENT, "invalid column index at position {d}", .{i});
                return handle.err_ctx.code;
            };
        }
        zig_indices = indices_buf;
    }

    var result = arrow_batch.readRowGroupAsArrow(
        handle.allocator,
        handle.source,
        handle.metadata,
        rg_idx,
        zig_indices,
    ) catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "readRowGroupAsArrow failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };

    // Wrap the per-column arrays into a single struct ArrowArray for the C caller.
    // The caller gets one top-level ArrowArray whose children are the column arrays.
    const wrapper = wrapColumnsAsStruct(handle.allocator, result.arrays, &result.schema) catch |e| {
        result.deinit();
        handle.err_ctx.setError(err.mapError(e), err.errorMessage(e));
        return handle.err_ctx.code;
    };

    a_out.* = wrapper.array;
    s_out.* = result.schema;
    // The wrapper took ownership of result.arrays; don't free them through result.
    // Only free the arrays slice itself (the ArrowArrays are now owned by wrapper).
    handle.allocator.free(result.arrays);
    return ZP_OK;
}

/// Get the most recent error code for this handle.
pub export fn zp_reader_error_code(handle_ptr: ?*anyopaque) callconv(.c) c_int {
    const handle = castHandle(ReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    return handle.err_ctx.code;
}

/// Get the most recent error message for this handle.
/// The returned string is valid until the next API call on this handle.
pub export fn zp_reader_error_message(handle_ptr: ?*anyopaque) callconv(.c) [*:0]const u8 {
    const handle = castHandle(ReaderHandle, handle_ptr) orelse return "";
    return handle.err_ctx.message();
}

/// Get an Arrow C Stream Interface that iterates over row groups.
pub export fn zp_reader_get_stream(
    handle_ptr: ?*anyopaque,
    stream_out: ?*ArrowArrayStream,
) callconv(.c) c_int {
    const handle = castHandle(ReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const out = stream_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.err_ctx.setOk();

    const pd = handle.allocator.create(StreamPrivateData) catch {
        handle.err_ctx.setError(err.ZP_ERROR_OUT_OF_MEMORY, "OutOfMemory");
        return handle.err_ctx.code;
    };
    pd.* = .{
        .handle = handle,
        .rg_index = 0,
    };

    out.* = .{
        .get_schema = &streamGetSchema,
        .get_next = &streamGetNext,
        .get_last_error = &streamGetLastError,
        .release = &streamRelease,
        .private_data = @ptrCast(pd),
    };
    return ZP_OK;
}

/// Check if statistics exist for a column in a row group.
/// Returns 1 if statistics exist, 0 if not, or a negative error code.
pub export fn zp_reader_has_statistics(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
    rg_index: c_int,
) callconv(.c) c_int {
    const handle = castHandle(ReaderHandle, handle_ptr) orelse return 0;
    const rg_idx = safe.castTo(usize, rg_index) catch return 0;
    const col_idx = safe.castTo(usize, col_index) catch return 0;
    if (rg_idx >= handle.metadata.row_groups.len) return 0;
    const rg = handle.metadata.row_groups[rg_idx];
    if (col_idx >= rg.columns.len) return 0;
    const meta = rg.columns[col_idx].meta_data orelse return 0;
    return if (meta.statistics != null) 1 else 0;
}

/// Get the null count from column statistics.
pub export fn zp_reader_get_null_count(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
    rg_index: c_int,
    count_out: ?*i64,
) callconv(.c) c_int {
    const handle = castHandle(ReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const out = count_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const stats = getStats(handle, col_index, rg_index) orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "no statistics available");
        return handle.err_ctx.code;
    };
    out.* = stats.null_count orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_DATA, "null count not available");
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

/// Get the distinct count from column statistics.
pub export fn zp_reader_get_distinct_count(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
    rg_index: c_int,
    count_out: ?*i64,
) callconv(.c) c_int {
    const handle = castHandle(ReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const out = count_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const stats = getStats(handle, col_index, rg_index) orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "no statistics available");
        return handle.err_ctx.code;
    };
    out.* = stats.distinct_count orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_DATA, "distinct count not available");
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

/// Get the minimum value from column statistics (raw bytes).
pub export fn zp_reader_get_min_value(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
    rg_index: c_int,
    data_out: ?*[*]const u8,
    len_out: ?*usize,
) callconv(.c) c_int {
    const handle = castHandle(ReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const d_out = data_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const l_out = len_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const stats = getStats(handle, col_index, rg_index) orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "no statistics available");
        return handle.err_ctx.code;
    };
    const min = stats.min_value orelse stats.min orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_DATA, "min value not available");
        return handle.err_ctx.code;
    };
    d_out.* = min.ptr;
    l_out.* = min.len;
    return ZP_OK;
}

/// Get the maximum value from column statistics (raw bytes).
pub export fn zp_reader_get_max_value(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
    rg_index: c_int,
    data_out: ?*[*]const u8,
    len_out: ?*usize,
) callconv(.c) c_int {
    const handle = castHandle(ReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const d_out = data_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const l_out = len_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const stats = getStats(handle, col_index, rg_index) orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "no statistics available");
        return handle.err_ctx.code;
    };
    const max = stats.max_value orelse stats.max orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_DATA, "max value not available");
        return handle.err_ctx.code;
    };
    d_out.* = max.ptr;
    l_out.* = max.len;
    return ZP_OK;
}

/// Get the number of key-value metadata entries.
pub export fn zp_reader_get_kv_metadata_count(
    handle_ptr: ?*anyopaque,
    count_out: ?*c_int,
) callconv(.c) c_int {
    const handle = castHandle(ReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const out = count_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const kvs = handle.metadata.key_value_metadata orelse {
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
pub export fn zp_reader_get_kv_metadata_key(
    handle_ptr: ?*anyopaque,
    index: c_int,
    key_out: ?*[*]const u8,
    len_out: ?*usize,
) callconv(.c) c_int {
    const handle = castHandle(ReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const k_out = key_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const l_out = len_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const kvs = handle.metadata.key_value_metadata orelse {
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
pub export fn zp_reader_get_kv_metadata_value(
    handle_ptr: ?*anyopaque,
    index: c_int,
    val_out: ?*[*]const u8,
    len_out: ?*usize,
) callconv(.c) c_int {
    const handle = castHandle(ReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const v_out = val_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const l_out = len_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const kvs = handle.metadata.key_value_metadata orelse {
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

/// Close the reader handle and release all associated resources.
pub export fn zp_reader_close(handle_ptr: ?*anyopaque) callconv(.c) void {
    const handle = castHandle(ReaderHandle, handle_ptr) orelse return;
    handle.close();
}

// ============================================================================
// Internal helpers
// ============================================================================

fn castHandle(comptime T: type, ptr: ?*anyopaque) ?*T {
    return if (ptr) |p| @ptrCast(@alignCast(p)) else null;
}

// ============================================================================
// Arrow C Stream Interface implementation
// ============================================================================

const ArrowArrayStream = @import("../../core/arrow.zig").ArrowArrayStream;

const StreamPrivateData = struct {
    handle: *ReaderHandle,
    rg_index: usize,
};

fn streamGetSchema(stream: *ArrowArrayStream, schema_out: *ArrowSchema) callconv(.c) c_int {
    const pd: *StreamPrivateData = @ptrCast(@alignCast(stream.private_data));
    const handle = pd.handle;

    const schema = arrow_batch.exportSchemaAsArrow(handle.allocator, handle.metadata) catch |e| {
        handle.err_ctx.setError(err.mapError(e), err.errorMessage(e));
        return handle.err_ctx.code;
    };
    schema_out.* = schema;
    return ZP_OK;
}

fn streamGetNext(stream: *ArrowArrayStream, array_out: *ArrowArray) callconv(.c) c_int {
    const pd: *StreamPrivateData = @ptrCast(@alignCast(stream.private_data));
    const handle = pd.handle;

    if (pd.rg_index >= handle.metadata.row_groups.len) {
        // Signal end of stream with a released array
        array_out.* = .{
            .length = 0,
            .null_count = 0,
            .offset = 0,
            .n_buffers = 0,
            .n_children = 0,
            .buffers = undefined,
            .children = null,
            .dictionary = null,
            .release = null,
            .private_data = null,
        };
        return ZP_OK;
    }

    var result = arrow_batch.readRowGroupAsArrow(
        handle.allocator,
        handle.source,
        handle.metadata,
        pd.rg_index,
        null,
    ) catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "readRowGroupAsArrow failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };

    const wrapper = wrapColumnsAsStruct(handle.allocator, result.arrays, &result.schema) catch |e| {
        result.deinit();
        handle.err_ctx.setError(err.mapError(e), err.errorMessage(e));
        return handle.err_ctx.code;
    };

    array_out.* = wrapper.array;
    // Schema from each row group read is the same; release it since the caller
    // gets the schema via get_schema(). The wrapper owns the column arrays.
    result.schema.doRelease();
    handle.allocator.free(result.arrays);
    pd.rg_index += 1;
    return ZP_OK;
}

fn streamGetLastError(stream: *ArrowArrayStream) callconv(.c) ?[*:0]const u8 {
    const pd: *StreamPrivateData = @ptrCast(@alignCast(stream.private_data));
    const msg = pd.handle.err_ctx.message();
    if (msg[0] == 0) return null;
    return msg;
}

fn streamRelease(stream: *ArrowArrayStream) callconv(.c) void {
    const pd: *StreamPrivateData = @ptrCast(@alignCast(stream.private_data));
    pd.handle.allocator.destroy(pd);
    stream.release = null;
}

/// Private data for the wrapper struct ArrowArray.
const StructWrapperData = struct {
    allocator: std.mem.Allocator,
    child_arrays: []ArrowArray,
    child_ptrs: []*ArrowArray,
    buffers_storage: [1]?*anyopaque,
};

const StructWrapperResult = struct {
    array: ArrowArray,
};

/// Wrap an array of per-column ArrowArrays into a single struct ArrowArray.
fn wrapColumnsAsStruct(
    allocator: std.mem.Allocator,
    arrays: []ArrowArray,
    schema: *const ArrowSchema,
) !StructWrapperResult {
    const n = arrays.len;

    const pd = try allocator.create(StructWrapperData);
    errdefer allocator.destroy(pd);

    // Take ownership of the column arrays
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

    // Determine num_rows from the first column or the schema
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

const format = @import("../../core/format.zig");

fn getStats(handle: *ReaderHandle, col_index: c_int, rg_index: c_int) ?format.Statistics {
    const rg_idx = safe.castTo(usize, rg_index) catch return null;
    const col_idx = safe.castTo(usize, col_index) catch return null;
    if (rg_idx >= handle.metadata.row_groups.len) return null;
    const rg = handle.metadata.row_groups[rg_idx];
    if (col_idx >= rg.columns.len) return null;
    const meta = rg.columns[col_idx].meta_data orelse return null;
    return meta.statistics;
}
