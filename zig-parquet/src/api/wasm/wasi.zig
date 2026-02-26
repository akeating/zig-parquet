//! WASM/WASI API entry points.
//!
//! Export functions mirroring the C ABI but without callconv(.c),
//! suitable for wasm32-wasi targets where the C calling convention
//! is not supported on export functions.
//!
//! All functions return i32 status codes (ZP_OK on success).
//! Error details are available via zp_reader_error_message / zp_reader_error_code.

const std = @import("std");
const handles = @import("handles.zig");

// WASI libc startup references `main`; satisfy it with a no-op for reactor
// (library) modules that set entry = .disabled but link libc for compression.
export fn main() i32 {
    return 0;
}
const err = @import("../../api/c/error.zig");
const introspect = @import("../../api/c/introspect.zig");
const arrow_batch = @import("../../core/arrow_batch.zig");
const safe = @import("../../core/safe.zig");
const format = @import("../../core/format.zig");
const seekable_reader_mod = @import("../../core/seekable_reader.zig");
const write_target_mod = @import("../../core/write_target.zig");

const CallbackReaderMod = @import("../../io/callback_reader.zig").CallbackReader;
const CallbackWriterMod = @import("../../io/callback_writer.zig").CallbackWriter;

const ArrowSchema = handles.ArrowSchema;
const ArrowArray = handles.ArrowArray;
const ReaderHandle = handles.ReaderHandle;
const WriterHandle = handles.WriterHandle;
const RowReaderHandle = handles.RowReaderHandle;
const RowWriterHandle = handles.RowWriterHandle;
const TypeInfo = handles.TypeInfo;
const Value = handles.Value;
const SchemaNode = handles.SchemaNode;
const SeekableReader = seekable_reader_mod.SeekableReader;
const WriteError = write_target_mod.WriteError;

const ZP_OK = err.ZP_OK;
const backing_allocator = std.heap.page_allocator;

// ============================================================================
// Reader exports
// ============================================================================

export fn zp_reader_open_memory(
    data: ?[*]const u8,
    len: usize,
    handle_out: ?*?*anyopaque,
) i32 {
    const out = handle_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    out.* = null;
    const ptr = data orelse return err.ZP_ERROR_INVALID_ARGUMENT;

    const handle = ReaderHandle.openMemory(ptr, len) catch |e| {
        return err.mapError(e);
    };
    out.* = @ptrCast(handle);
    return ZP_OK;
}

/// WASI callback adapter for reader.
const WasiReaderAdapter = struct {
    user_ctx: ?*anyopaque,
    read_at_fn: *const fn (?*anyopaque, u64, [*]u8, usize, *usize) i32,
    size_fn: *const fn (?*anyopaque) u64,

    fn readAt(ptr: *anyopaque, offset: u64, buf: []u8) SeekableReader.Error!usize {
        const self: *WasiReaderAdapter = @ptrCast(@alignCast(ptr));
        var bytes_read: usize = 0;
        const rc = self.read_at_fn(self.user_ctx, offset, buf.ptr, buf.len, &bytes_read);
        if (rc != 0) return error.InputOutput;
        return bytes_read;
    }

    fn size(ptr: *anyopaque) u64 {
        const self: *WasiReaderAdapter = @ptrCast(@alignCast(ptr));
        return self.size_fn(self.user_ctx);
    }
};

export fn zp_reader_open_callbacks(
    ctx: ?*anyopaque,
    read_at_fn: ?*const fn (?*anyopaque, u64, [*]u8, usize, *usize) i32,
    size_fn: ?*const fn (?*anyopaque) u64,
    handle_out: ?*?*anyopaque,
) i32 {
    const out = handle_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    out.* = null;
    const r_fn = read_at_fn orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const s_fn = size_fn orelse return err.ZP_ERROR_INVALID_ARGUMENT;

    const allocator = backing_allocator;
    const adapter = allocator.create(WasiReaderAdapter) catch return err.ZP_ERROR_OUT_OF_MEMORY;
    errdefer allocator.destroy(adapter);
    adapter.* = .{
        .user_ctx = ctx,
        .read_at_fn = r_fn,
        .size_fn = s_fn,
    };

    const cb_reader = allocator.create(CallbackReaderMod) catch {
        allocator.destroy(adapter);
        return err.ZP_ERROR_OUT_OF_MEMORY;
    };
    cb_reader.* = .{
        .ctx = @ptrCast(adapter),
        .read_at_fn = WasiReaderAdapter.readAt,
        .size_fn = WasiReaderAdapter.size,
    };

    const handle = ReaderHandle.openCallbacks(cb_reader) catch |e| {
        allocator.destroy(cb_reader);
        allocator.destroy(adapter);
        return err.mapError(e);
    };
    out.* = @ptrCast(handle);
    return ZP_OK;
}

export fn zp_reader_get_num_row_groups(
    handle_ptr: ?*anyopaque,
    count_out: ?*i32,
) i32 {
    const handle = castHandle(ReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const out = count_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    out.* = safe.castTo(i32, handle.metadata.row_groups.len) catch {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_DATA, "row group count too large");
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

export fn zp_reader_get_num_rows(
    handle_ptr: ?*anyopaque,
    count_out: ?*i64,
) i32 {
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

export fn zp_reader_get_row_group_num_rows(
    handle_ptr: ?*anyopaque,
    rg_index: i32,
    count_out: ?*i64,
) i32 {
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

export fn zp_reader_get_column_count(
    handle_ptr: ?*anyopaque,
    count_out: ?*i32,
) i32 {
    const handle = castHandle(ReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const out = count_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    if (handle.metadata.row_groups.len > 0) {
        out.* = safe.castTo(i32, handle.metadata.row_groups[0].columns.len) catch {
            handle.err_ctx.setError(err.ZP_ERROR_INVALID_DATA, "column count too large");
            return handle.err_ctx.code;
        };
    } else {
        var leaf_count: i32 = 0;
        if (handle.metadata.schema.len > 1) {
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

export fn zp_reader_get_schema(
    handle_ptr: ?*anyopaque,
    schema_out: ?*ArrowSchema,
) i32 {
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

export fn zp_reader_read_row_group(
    handle_ptr: ?*anyopaque,
    rg_index: i32,
    col_indices: ?[*]const i32,
    num_cols: i32,
    arrays_out: ?*ArrowArray,
    schema_out: ?*ArrowSchema,
) i32 {
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

    const wrapper = handles.wrapColumnsAsStruct(handle.allocator, result.arrays, &result.schema) catch |e| {
        result.deinit();
        handle.err_ctx.setError(err.mapError(e), err.errorMessage(e));
        return handle.err_ctx.code;
    };

    a_out.* = wrapper.array;
    s_out.* = result.schema;
    handle.allocator.free(result.arrays);
    return ZP_OK;
}

export fn zp_reader_error_code(handle_ptr: ?*anyopaque) i32 {
    const handle = castHandle(ReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    return handle.err_ctx.code;
}

export fn zp_reader_error_message(handle_ptr: ?*anyopaque) [*:0]const u8 {
    const handle = castHandle(ReaderHandle, handle_ptr) orelse return "";
    return handle.err_ctx.message();
}

export fn zp_reader_has_statistics(
    handle_ptr: ?*anyopaque,
    col_index: i32,
    rg_index: i32,
) i32 {
    const handle = castHandle(ReaderHandle, handle_ptr) orelse return 0;
    const rg_idx = safe.castTo(usize, rg_index) catch return 0;
    const col_idx = safe.castTo(usize, col_index) catch return 0;
    if (rg_idx >= handle.metadata.row_groups.len) return 0;
    const rg = handle.metadata.row_groups[rg_idx];
    if (col_idx >= rg.columns.len) return 0;
    const meta = rg.columns[col_idx].meta_data orelse return 0;
    return if (meta.statistics != null) 1 else 0;
}

export fn zp_reader_get_null_count(
    handle_ptr: ?*anyopaque,
    col_index: i32,
    rg_index: i32,
    count_out: ?*i64,
) i32 {
    const handle = castHandle(ReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const out = count_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const stats = wasiGetStats(handle, col_index, rg_index) orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "no statistics available");
        return handle.err_ctx.code;
    };
    out.* = stats.null_count orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_DATA, "null count not available");
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

export fn zp_reader_get_distinct_count(
    handle_ptr: ?*anyopaque,
    col_index: i32,
    rg_index: i32,
    count_out: ?*i64,
) i32 {
    const handle = castHandle(ReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const out = count_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const stats = wasiGetStats(handle, col_index, rg_index) orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "no statistics available");
        return handle.err_ctx.code;
    };
    out.* = stats.distinct_count orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_DATA, "distinct count not available");
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

export fn zp_reader_get_min_value(
    handle_ptr: ?*anyopaque,
    col_index: i32,
    rg_index: i32,
    data_out: ?*[*]const u8,
    len_out: ?*usize,
) i32 {
    const handle = castHandle(ReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const d_out = data_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const l_out = len_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const stats = wasiGetStats(handle, col_index, rg_index) orelse {
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

export fn zp_reader_get_max_value(
    handle_ptr: ?*anyopaque,
    col_index: i32,
    rg_index: i32,
    data_out: ?*[*]const u8,
    len_out: ?*usize,
) i32 {
    const handle = castHandle(ReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const d_out = data_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const l_out = len_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const stats = wasiGetStats(handle, col_index, rg_index) orelse {
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

export fn zp_reader_get_kv_metadata_count(handle_ptr: ?*anyopaque, count_out: ?*i32) i32 {
    const handle = castHandle(ReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const out = count_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const kvs = handle.metadata.key_value_metadata orelse {
        out.* = 0;
        return ZP_OK;
    };
    out.* = safe.castTo(i32, kvs.len) catch {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_DATA, "metadata count too large");
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

export fn zp_reader_get_kv_metadata_key(handle_ptr: ?*anyopaque, index: i32, key_out: ?*[*]const u8, len_out: ?*usize) i32 {
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

export fn zp_reader_get_kv_metadata_value(handle_ptr: ?*anyopaque, index: i32, val_out: ?*[*]const u8, len_out: ?*usize) i32 {
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

export fn zp_reader_get_stream(
    handle_ptr: ?*anyopaque,
    stream_out: ?*ArrowArrayStream,
) i32 {
    const handle = castHandle(ReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const out = stream_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.err_ctx.setOk();

    const pd = backing_allocator.create(StreamPrivateData) catch {
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

export fn zp_reader_close(handle_ptr: ?*anyopaque) void {
    const handle = castHandle(ReaderHandle, handle_ptr) orelse return;
    handle.close();
}

// ============================================================================
// Writer exports
// ============================================================================

export fn zp_writer_open_memory(
    handle_out: ?*?*anyopaque,
) i32 {
    const out = handle_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    out.* = null;

    const handle = WriterHandle.openMemory() catch |e| {
        return err.mapError(e);
    };
    out.* = @ptrCast(handle);
    return ZP_OK;
}

/// WASI callback adapter for writer.
const WasiWriterAdapter = struct {
    user_ctx: ?*anyopaque,
    write_fn: *const fn (?*anyopaque, [*]const u8, usize) i32,
    close_fn: ?*const fn (?*anyopaque) i32,

    fn write(ptr: *anyopaque, data: []const u8) WriteError!void {
        const self: *WasiWriterAdapter = @ptrCast(@alignCast(ptr));
        const rc = self.write_fn(self.user_ctx, data.ptr, data.len);
        if (rc != 0) return error.WriteError;
    }

    fn close(ptr: *anyopaque) WriteError!void {
        const self: *WasiWriterAdapter = @ptrCast(@alignCast(ptr));
        if (self.close_fn) |cf| {
            const rc = cf(self.user_ctx);
            if (rc != 0) return error.WriteError;
        }
    }
};

export fn zp_writer_open_callbacks(
    ctx: ?*anyopaque,
    write_fn: ?*const fn (?*anyopaque, [*]const u8, usize) i32,
    close_fn: ?*const fn (?*anyopaque) i32,
    handle_out: ?*?*anyopaque,
) i32 {
    const out = handle_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    out.* = null;
    const w_fn = write_fn orelse return err.ZP_ERROR_INVALID_ARGUMENT;

    const allocator = backing_allocator;
    const adapter = allocator.create(WasiWriterAdapter) catch return err.ZP_ERROR_OUT_OF_MEMORY;
    errdefer allocator.destroy(adapter);
    adapter.* = .{
        .user_ctx = ctx,
        .write_fn = w_fn,
        .close_fn = close_fn,
    };

    const cb_writer = allocator.create(CallbackWriterMod) catch {
        allocator.destroy(adapter);
        return err.ZP_ERROR_OUT_OF_MEMORY;
    };
    cb_writer.* = .{
        .ctx = @ptrCast(adapter),
        .write_fn = WasiWriterAdapter.write,
        .close_fn = WasiWriterAdapter.close,
    };

    const handle = WriterHandle.openCallbackWriter(cb_writer) catch |e| {
        allocator.destroy(cb_writer);
        allocator.destroy(adapter);
        return err.mapError(e);
    };
    out.* = @ptrCast(handle);
    return ZP_OK;
}

export fn zp_writer_set_schema(
    handle_ptr: ?*anyopaque,
    schema: ?*const ArrowSchema,
) i32 {
    const handle = castHandle(WriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const s = schema orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.err_ctx.setOk();

    if (handle.transport_failed) {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_STATE, "handle invalidated by transport error");
        return handle.err_ctx.code;
    }

    handle.setSchema(s) catch |e| {
        handle.err_ctx.setError(err.mapError(e), err.errorMessage(e));
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

export fn zp_writer_set_column_codec(
    handle_ptr: ?*anyopaque,
    col_index: i32,
    codec: i32,
) i32 {
    const handle = castHandle(WriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.err_ctx.setOk();

    const defs = handle.column_defs orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_STATE, "schema not set");
        return handle.err_ctx.code;
    };

    if (col_index < 0) {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "negative column index");
        return handle.err_ctx.code;
    }
    const idx = safe.castTo(usize, col_index) catch {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "column index out of range");
        return handle.err_ctx.code;
    };
    if (idx >= defs.len) {
        handle.err_ctx.setErrorFmt(err.ZP_ERROR_INVALID_ARGUMENT, "column index {d} >= {d}", .{ idx, defs.len });
        return handle.err_ctx.code;
    }

    const compression = format.CompressionCodec.fromInt(codec) catch {
        handle.err_ctx.setErrorFmt(err.ZP_ERROR_INVALID_ARGUMENT, "invalid codec value {d}", .{codec});
        return handle.err_ctx.code;
    };
    defs[idx].codec = compression;
    return ZP_OK;
}

export fn zp_writer_set_row_group_size(
    handle_ptr: ?*anyopaque,
    size_bytes: i64,
) i32 {
    const handle = castHandle(WriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.err_ctx.setOk();

    if (size_bytes <= 0) {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "row group size must be positive");
        return handle.err_ctx.code;
    }

    handle.row_group_size = safe.castTo(usize, size_bytes) catch {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "row group size out of range");
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

export fn zp_writer_write_row_group(
    handle_ptr: ?*anyopaque,
    batch: ?*const ArrowArray,
    schema: ?*const ArrowSchema,
) i32 {
    const handle = castHandle(WriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const b = batch orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const s = schema orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.err_ctx.setOk();

    if (handle.transport_failed) {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_STATE, "handle invalidated by transport error");
        return handle.err_ctx.code;
    }

    const w = handle.writer orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_STATE, "schema not set");
        return handle.err_ctx.code;
    };

    const nc = safe.castTo(usize, b.n_children) catch {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "invalid n_children");
        return handle.err_ctx.code;
    };
    const snc = safe.castTo(usize, s.n_children) catch {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "invalid schema n_children");
        return handle.err_ctx.code;
    };
    if (nc != snc) {
        handle.err_ctx.setErrorFmt(err.ZP_ERROR_INVALID_ARGUMENT, "array children ({d}) != schema children ({d})", .{ nc, snc });
        return handle.err_ctx.code;
    }

    const arr_children: [*]*ArrowArray = b.children orelse {
        if (nc == 0) {
            return ZP_OK;
        }
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "null array children pointer");
        return handle.err_ctx.code;
    };
    const sch_children: [*]*ArrowSchema = s.children orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "null schema children pointer");
        return handle.err_ctx.code;
    };

    const arrays = handle.allocator.alloc(ArrowArray, nc) catch {
        handle.err_ctx.setError(err.ZP_ERROR_OUT_OF_MEMORY, "OutOfMemory");
        return handle.err_ctx.code;
    };
    defer handle.allocator.free(arrays);

    const schemas = handle.allocator.alloc(ArrowSchema, nc) catch {
        handle.err_ctx.setError(err.ZP_ERROR_OUT_OF_MEMORY, "OutOfMemory");
        return handle.err_ctx.code;
    };
    defer handle.allocator.free(schemas);

    for (0..nc) |i| {
        arrays[i] = arr_children[i].*;
        schemas[i] = sch_children[i].*;
    }

    arrow_batch.writeRowGroupFromArrow(w, handle.allocator, arrays, schemas) catch |e| {
        const code = err.mapError(e);
        handle.err_ctx.setErrorFmt(code, "writeRowGroupFromArrow failed: {s}", .{err.errorMessage(e)});
        if (code == err.ZP_ERROR_IO) {
            handle.transport_failed = true;
        }
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

export fn zp_writer_get_buffer(
    handle_ptr: ?*anyopaque,
    data_out: ?*[*]const u8,
    len_out: ?*usize,
) i32 {
    const handle = castHandle(WriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const d_out = data_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const l_out = len_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.err_ctx.setOk();

    switch (handle.backend) {
        .buffer => |bt| {
            const w = bt.written();
            d_out.* = w.ptr;
            l_out.* = w.len;
            return ZP_OK;
        },
        .callback => {
            handle.err_ctx.setError(err.ZP_ERROR_INVALID_STATE, "get_buffer not available for callback writers");
            return handle.err_ctx.code;
        },
    }
}

export fn zp_writer_error_code(handle_ptr: ?*anyopaque) i32 {
    const handle = castHandle(WriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    return handle.err_ctx.code;
}

export fn zp_writer_error_message(handle_ptr: ?*anyopaque) [*:0]const u8 {
    const handle = castHandle(WriterHandle, handle_ptr) orelse return "";
    return handle.err_ctx.message();
}

export fn zp_writer_close(handle_ptr: ?*anyopaque) i32 {
    const handle = castHandle(WriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.err_ctx.setOk();

    if (handle.transport_failed) {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_STATE, "handle invalidated by transport error");
        return handle.err_ctx.code;
    }

    handle.close() catch |e| {
        handle.err_ctx.setError(err.mapError(e), err.errorMessage(e));
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

export fn zp_writer_free(handle_ptr: ?*anyopaque) void {
    const handle = castHandle(WriterHandle, handle_ptr) orelse return;
    handle.deinit();
}

// ============================================================================
// Row Reader exports (cursor-based, non-Arrow)
// ============================================================================

export fn zp_row_reader_open_memory(
    data: ?[*]const u8,
    len: usize,
    handle_out: ?*?*anyopaque,
) i32 {
    const out = handle_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    out.* = null;
    const ptr = data orelse return err.ZP_ERROR_INVALID_ARGUMENT;

    const handle = RowReaderHandle.openMemory(ptr, len) catch |e| return err.mapError(e);
    out.* = @ptrCast(handle);
    return ZP_OK;
}

export fn zp_row_reader_open_callbacks(
    ctx: ?*anyopaque,
    read_at_fn: ?*const fn (?*anyopaque, u64, [*]u8, usize, *usize) i32,
    size_fn: ?*const fn (?*anyopaque) u64,
    handle_out: ?*?*anyopaque,
) i32 {
    const out = handle_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    out.* = null;
    const r_fn = read_at_fn orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const s_fn = size_fn orelse return err.ZP_ERROR_INVALID_ARGUMENT;

    const allocator = backing_allocator;
    const adapter = allocator.create(WasiReaderAdapter) catch return err.ZP_ERROR_OUT_OF_MEMORY;
    adapter.* = .{ .user_ctx = ctx, .read_at_fn = r_fn, .size_fn = s_fn };

    const cb_reader = allocator.create(CallbackReaderMod) catch {
        allocator.destroy(adapter);
        return err.ZP_ERROR_OUT_OF_MEMORY;
    };
    cb_reader.* = .{
        .ctx = @ptrCast(adapter),
        .read_at_fn = WasiReaderAdapter.readAt,
        .size_fn = WasiReaderAdapter.size,
    };

    const handle = RowReaderHandle.openCallbacks(cb_reader) catch |e| {
        allocator.destroy(cb_reader);
        allocator.destroy(adapter);
        return err.mapError(e);
    };
    out.* = @ptrCast(handle);
    return ZP_OK;
}

export fn zp_row_reader_get_num_row_groups(handle_ptr: ?*anyopaque, count_out: ?*i32) i32 {
    const handle = castHandle(RowReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const out = count_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    out.* = safe.castTo(i32, handle.reader.metadata.row_groups.len) catch {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_DATA, "row group count too large");
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

export fn zp_row_reader_get_num_rows(handle_ptr: ?*anyopaque, count_out: ?*i64) i32 {
    const handle = castHandle(RowReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
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

export fn zp_row_reader_get_column_count(handle_ptr: ?*anyopaque, count_out: ?*i32) i32 {
    const handle = castHandle(RowReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const out = count_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    out.* = safe.castTo(i32, handle.num_top_columns) catch {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_DATA, "column count too large");
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

export fn zp_row_reader_get_column_name(handle_ptr: ?*anyopaque, col_index: i32) ?[*:0]const u8 {
    const handle = castHandle(RowReaderHandle, handle_ptr) orelse return null;
    const idx = safe.castTo(usize, col_index) catch return null;
    if (idx >= handle.col_names.len) return null;
    return handle.col_names[idx];
}

export fn zp_row_reader_read_row_group(handle_ptr: ?*anyopaque, rg_index: i32) i32 {
    const handle = castHandle(RowReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
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

export fn zp_row_reader_next(handle_ptr: ?*anyopaque) i32 {
    const handle = castHandle(RowReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    if (handle.next()) return ZP_OK;
    return err.ZP_ROW_END;
}

export fn zp_row_reader_get_type(handle_ptr: ?*anyopaque, col_index: i32) i32 {
    const handle = castHandle(RowReaderHandle, handle_ptr) orelse return err.ZP_TYPE_NULL;
    const val = wasiGetValue(handle_ptr, col_index) orelse return err.ZP_TYPE_NULL;
    if (val.isNull()) return err.ZP_TYPE_NULL;
    const idx = safe.castTo(usize, col_index) catch return wasiPhysicalValueType(val);
    return wasiColumnType(&handle.reader, idx, val);
}

export fn zp_row_reader_get_column_type(handle_ptr: ?*anyopaque, col_index: i32) i32 {
    const handle = castHandle(RowReaderHandle, handle_ptr) orelse return err.ZP_TYPE_NULL;
    const idx = safe.castTo(usize, col_index) catch return err.ZP_TYPE_NULL;
    return wasiSchemaColumnType(&handle.reader, idx);
}

export fn zp_row_reader_get_decimal_precision(handle_ptr: ?*anyopaque, col_index: i32) i32 {
    const handle = castHandle(RowReaderHandle, handle_ptr) orelse return 0;
    const idx = safe.castTo(usize, col_index) catch return 0;
    const elem = handle.reader.getLeafSchemaElement(idx) orelse return 0;
    if (elem.logical_type) |lt| {
        if (lt == .decimal) return lt.decimal.precision;
    }
    return elem.precision orelse 0;
}

export fn zp_row_reader_get_decimal_scale(handle_ptr: ?*anyopaque, col_index: i32) i32 {
    const handle = castHandle(RowReaderHandle, handle_ptr) orelse return 0;
    const idx = safe.castTo(usize, col_index) catch return 0;
    const elem = handle.reader.getLeafSchemaElement(idx) orelse return 0;
    if (elem.logical_type) |lt| {
        if (lt == .decimal) return lt.decimal.scale;
    }
    return elem.scale orelse 0;
}

export fn zp_row_reader_is_null(handle_ptr: ?*anyopaque, col_index: i32) i32 {
    const val = wasiGetValue(handle_ptr, col_index) orelse return 1;
    return if (val.isNull()) @as(i32, 1) else @as(i32, 0);
}

export fn zp_row_reader_get_int32(handle_ptr: ?*anyopaque, col_index: i32) i32 {
    const val = wasiGetValue(handle_ptr, col_index) orelse return 0;
    return val.asInt32() orelse 0;
}

export fn zp_row_reader_get_int64(handle_ptr: ?*anyopaque, col_index: i32) i64 {
    const val = wasiGetValue(handle_ptr, col_index) orelse return 0;
    return val.asInt64() orelse 0;
}

export fn zp_row_reader_get_float(handle_ptr: ?*anyopaque, col_index: i32) f32 {
    const val = wasiGetValue(handle_ptr, col_index) orelse return 0;
    return val.asFloat() orelse 0;
}

export fn zp_row_reader_get_double(handle_ptr: ?*anyopaque, col_index: i32) f64 {
    const val = wasiGetValue(handle_ptr, col_index) orelse return 0;
    return val.asDouble() orelse 0;
}

export fn zp_row_reader_get_bool(handle_ptr: ?*anyopaque, col_index: i32) i32 {
    const val = wasiGetValue(handle_ptr, col_index) orelse return 0;
    return if (val.asBool() orelse false) @as(i32, 1) else @as(i32, 0);
}

export fn zp_row_reader_get_bytes(
    handle_ptr: ?*anyopaque,
    col_index: i32,
    data_out: ?*[*]const u8,
    len_out: ?*usize,
) i32 {
    const d_out = data_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const l_out = len_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;

    const val = wasiGetValue(handle_ptr, col_index) orelse {
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

export fn zp_row_reader_get_kv_metadata_count(handle_ptr: ?*anyopaque, count_out: ?*i32) i32 {
    const handle = castHandle(RowReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const out = count_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const kvs = handle.reader.metadata.key_value_metadata orelse {
        out.* = 0;
        return ZP_OK;
    };
    out.* = safe.castTo(i32, kvs.len) catch {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_DATA, "metadata count too large");
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

export fn zp_row_reader_get_kv_metadata_key(handle_ptr: ?*anyopaque, index: i32, key_out: ?*[*]const u8, len_out: ?*usize) i32 {
    const handle = castHandle(RowReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
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

export fn zp_row_reader_get_kv_metadata_value(handle_ptr: ?*anyopaque, index: i32, val_out: ?*[*]const u8, len_out: ?*usize) i32 {
    const handle = castHandle(RowReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
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

export fn zp_row_reader_set_checksum_validation(handle_ptr: ?*anyopaque, validate: i32, strict: i32) i32 {
    const handle = castHandle(RowReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.reader.checksum_options = .{
        .validate_page_checksum = validate != 0,
        .strict_checksum = strict != 0,
    };
    return ZP_OK;
}

export fn zp_row_reader_error_code(handle_ptr: ?*anyopaque) i32 {
    const handle = castHandle(RowReaderHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    return handle.err_ctx.code;
}

export fn zp_row_reader_error_message(handle_ptr: ?*anyopaque) [*:0]const u8 {
    const handle = castHandle(RowReaderHandle, handle_ptr) orelse return "";
    return handle.err_ctx.message();
}

export fn zp_row_reader_get_value(
    handle_ptr: ?*anyopaque,
    col_index: i32,
) ?*const anyopaque {
    const ref = wasiGetValueRef(handle_ptr, col_index) orelse return null;
    return @ptrCast(ref);
}

export fn zp_value_get_type(value_ptr: ?*const anyopaque) i32 {
    const val = castValue(value_ptr) orelse return err.ZP_TYPE_NULL;
    return wasiPhysicalValueType(val.*);
}

export fn zp_value_is_null(value_ptr: ?*const anyopaque) i32 {
    const val = castValue(value_ptr) orelse return 1;
    return if (val.isNull()) @as(i32, 1) else @as(i32, 0);
}

export fn zp_value_get_int32(value_ptr: ?*const anyopaque) i32 {
    const val = castValue(value_ptr) orelse return 0;
    return val.asInt32() orelse 0;
}

export fn zp_value_get_int64(value_ptr: ?*const anyopaque) i64 {
    const val = castValue(value_ptr) orelse return 0;
    return val.asInt64() orelse 0;
}

export fn zp_value_get_float(value_ptr: ?*const anyopaque) f32 {
    const val = castValue(value_ptr) orelse return 0;
    return val.asFloat() orelse 0;
}

export fn zp_value_get_double(value_ptr: ?*const anyopaque) f64 {
    const val = castValue(value_ptr) orelse return 0;
    return val.asDouble() orelse 0;
}

export fn zp_value_get_bool(value_ptr: ?*const anyopaque) i32 {
    const val = castValue(value_ptr) orelse return 0;
    return if (val.asBool() orelse false) @as(i32, 1) else @as(i32, 0);
}

export fn zp_value_get_bytes(
    value_ptr: ?*const anyopaque,
    data_out: ?*[*]const u8,
    len_out: ?*usize,
) i32 {
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

export fn zp_value_get_list_len(value_ptr: ?*const anyopaque) i32 {
    const val = castValue(value_ptr) orelse return 0;
    const items = val.asList() orelse return 0;
    return safe.castTo(i32, items.len) catch 0;
}

export fn zp_value_get_list_element(
    value_ptr: ?*const anyopaque,
    index: i32,
) ?*const anyopaque {
    const val = castValue(value_ptr) orelse return null;
    const items = val.asList() orelse return null;
    const idx = safe.castTo(usize, index) catch return null;
    if (idx >= items.len) return null;
    return @ptrCast(&items[idx]);
}

export fn zp_value_get_map_len(value_ptr: ?*const anyopaque) i32 {
    const val = castValue(value_ptr) orelse return 0;
    const entries = val.asMap() orelse return 0;
    return safe.castTo(i32, entries.len) catch 0;
}

export fn zp_value_get_map_key(
    value_ptr: ?*const anyopaque,
    entry_index: i32,
) ?*const anyopaque {
    const val = castValue(value_ptr) orelse return null;
    const entries = val.asMap() orelse return null;
    const idx = safe.castTo(usize, entry_index) catch return null;
    if (idx >= entries.len) return null;
    return @ptrCast(&entries[idx].key);
}

export fn zp_value_get_map_value(
    value_ptr: ?*const anyopaque,
    entry_index: i32,
) ?*const anyopaque {
    const val = castValue(value_ptr) orelse return null;
    const entries = val.asMap() orelse return null;
    const idx = safe.castTo(usize, entry_index) catch return null;
    if (idx >= entries.len) return null;
    return @ptrCast(&entries[idx].value);
}

export fn zp_value_get_struct_field_count(value_ptr: ?*const anyopaque) i32 {
    const val = castValue(value_ptr) orelse return 0;
    const fields = val.asStruct() orelse return 0;
    return safe.castTo(i32, fields.len) catch 0;
}

export fn zp_value_get_struct_field_name(
    value_ptr: ?*const anyopaque,
    field_index: i32,
    name_out: ?*[*]const u8,
    len_out: ?*usize,
) i32 {
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

export fn zp_value_get_struct_field_value(
    value_ptr: ?*const anyopaque,
    field_index: i32,
) ?*const anyopaque {
    const val = castValue(value_ptr) orelse return null;
    const fields = val.asStruct() orelse return null;
    const idx = safe.castTo(usize, field_index) catch return null;
    if (idx >= fields.len) return null;
    return @ptrCast(&fields[idx].value);
}

export fn zp_row_reader_close(handle_ptr: ?*anyopaque) void {
    const handle = castHandle(RowReaderHandle, handle_ptr) orelse return;
    handle.close();
}

fn wasiGetValue(handle_ptr: ?*anyopaque, col_index: i32) ?Value {
    const handle = castHandle(RowReaderHandle, handle_ptr) orelse return null;
    const row = handle.currentRow() orelse return null;
    const idx = safe.castTo(usize, col_index) catch return null;
    if (idx >= row.values.len) return null;
    return row.values[idx];
}

fn wasiGetValueRef(handle_ptr: ?*anyopaque, col_index: i32) ?*const Value {
    const handle = castHandle(RowReaderHandle, handle_ptr) orelse return null;
    const row = handle.currentRow() orelse return null;
    const idx = safe.castTo(usize, col_index) catch return null;
    if (idx >= row.values.len) return null;
    return &row.values[idx];
}

fn castValue(ptr: ?*const anyopaque) ?*const Value {
    return if (ptr) |p| @ptrCast(@alignCast(p)) else null;
}

const core_dynamic = @import("../../core/dynamic_reader.zig");

fn wasiPhysicalValueType(v: Value) i32 {
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

fn wasiLogicalTypeToZpType(lt: format.LogicalType) i32 {
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

fn wasiConvertedTypeToZpType(ct: i32) ?i32 {
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

fn wasiColumnType(reader: *const core_dynamic.DynamicReader, col_idx: usize, v: Value) i32 {
    if (reader.getColumnLogicalType(col_idx)) |lt| return wasiLogicalTypeToZpType(lt);
    const elem = reader.getLeafSchemaElement(col_idx) orelse return wasiPhysicalValueType(v);
    if (elem.converted_type) |ct| {
        if (wasiConvertedTypeToZpType(ct)) |zpt| return zpt;
    }
    return wasiPhysicalValueType(v);
}

fn wasiSchemaColumnType(reader: *const core_dynamic.DynamicReader, col_idx: usize) i32 {
    if (reader.getColumnLogicalType(col_idx)) |lt| return wasiLogicalTypeToZpType(lt);
    const elem = reader.getLeafSchemaElement(col_idx) orelse return err.ZP_TYPE_NULL;
    if (elem.converted_type) |ct| {
        if (wasiConvertedTypeToZpType(ct)) |zpt| return zpt;
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

// ============================================================================
// Row Writer exports (cursor-based, non-Arrow, multi-row-group)
// ============================================================================

export fn zp_row_writer_open_memory(handle_out: ?*?*anyopaque) i32 {
    const out = handle_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    out.* = null;
    const handle = RowWriterHandle.openMemory() catch |e| return err.mapError(e);
    out.* = @ptrCast(handle);
    return ZP_OK;
}

export fn zp_row_writer_open_callbacks(
    ctx: ?*anyopaque,
    write_fn: ?*const fn (?*anyopaque, [*]const u8, usize) i32,
    close_fn: ?*const fn (?*anyopaque) i32,
    handle_out: ?*?*anyopaque,
) i32 {
    const out = handle_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    out.* = null;
    const w_fn = write_fn orelse return err.ZP_ERROR_INVALID_ARGUMENT;

    const allocator = backing_allocator;
    const adapter = allocator.create(WasiWriterAdapter) catch return err.ZP_ERROR_OUT_OF_MEMORY;
    adapter.* = .{ .user_ctx = ctx, .write_fn = w_fn, .close_fn = close_fn };

    const cb_writer = allocator.create(CallbackWriterMod) catch {
        allocator.destroy(adapter);
        return err.ZP_ERROR_OUT_OF_MEMORY;
    };
    cb_writer.* = .{
        .ctx = @ptrCast(adapter),
        .write_fn = WasiWriterAdapter.write,
        .close_fn = WasiWriterAdapter.close,
    };

    const handle = RowWriterHandle.openCallbackWriter(cb_writer) catch |e| {
        allocator.destroy(cb_writer);
        allocator.destroy(adapter);
        return err.mapError(e);
    };
    out.* = @ptrCast(handle);
    return ZP_OK;
}

export fn zp_row_writer_add_column(handle_ptr: ?*anyopaque, name: ?[*:0]const u8, col_type: i32) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
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

export fn zp_row_writer_add_column_decimal(handle_ptr: ?*anyopaque, name: ?[*:0]const u8, precision: i32, scale: i32) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
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

export fn zp_row_writer_add_column_geometry(handle_ptr: ?*anyopaque, name: ?[*:0]const u8, crs_ptr: ?[*]const u8, crs_len: i32) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
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

export fn zp_row_writer_add_column_geography(handle_ptr: ?*anyopaque, name: ?[*:0]const u8, crs_ptr: ?[*]const u8, crs_len: i32, algorithm: i32) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const n = name orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const crs: ?[]const u8 = if (crs_ptr) |p| blk: {
        const len = if (crs_len >= 0) safe.castTo(usize, crs_len) catch unreachable else 0; // guarded by >= 0 check
        break :blk if (len > 0) p[0..len] else null;
    } else null;
    const algo: ?format.EdgeInterpolationAlgorithm = switch (algorithm) {
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

export fn zp_row_writer_set_compression(handle_ptr: ?*anyopaque, codec: i32) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
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

export fn zp_row_writer_set_column_codec(handle_ptr: ?*anyopaque, col_index: i32, codec: i32) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
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

export fn zp_row_writer_set_row_group_size(handle_ptr: ?*anyopaque, max_rows: i64) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
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

export fn zp_row_writer_set_kv_metadata(handle_ptr: ?*anyopaque, key: ?[*]const u8, key_len: usize, value: ?[*]const u8, value_len: usize) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const k = key orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const v: ?[]const u8 = if (value) |vp| vp[0..value_len] else null;
    handle.setKvMetadata(k[0..key_len], v) catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "setKvMetadata failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

// ============================================================================
// Schema builder (nested types)
// ============================================================================

export fn zp_schema_primitive(handle_ptr: ?*anyopaque, zp_type: i32) ?*const anyopaque {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return null;
    const info = TypeInfo.fromZpType(zp_type) orelse return null;
    const node = schemaNodeFromTypeInfo(handle, info) catch return null;
    return @ptrCast(node);
}

export fn zp_schema_decimal(handle_ptr: ?*anyopaque, precision: i32, scale: i32) ?*const anyopaque {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return null;
    if (precision < 1 or precision > 38) return null;
    if (scale < 0 or scale > precision) return null;
    const info = TypeInfo.forDecimal(precision, scale);
    const node = schemaNodeFromTypeInfo(handle, info) catch return null;
    return @ptrCast(node);
}

export fn zp_schema_optional(handle_ptr: ?*anyopaque, inner: ?*const anyopaque) ?*const anyopaque {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return null;
    const child = castSchemaNode(inner) orelse return null;
    const node = handle.allocSchemaNode(.{ .optional = child }) catch return null;
    return @ptrCast(node);
}

export fn zp_schema_list(handle_ptr: ?*anyopaque, element: ?*const anyopaque) ?*const anyopaque {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return null;
    const elem = castSchemaNode(element) orelse return null;
    const node = handle.allocSchemaNode(.{ .list = elem }) catch return null;
    return @ptrCast(node);
}

export fn zp_schema_map(handle_ptr: ?*anyopaque, key_schema: ?*const anyopaque, value_schema: ?*const anyopaque) ?*const anyopaque {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return null;
    const k = castSchemaNode(key_schema) orelse return null;
    const v = castSchemaNode(value_schema) orelse return null;
    const node = handle.allocSchemaNode(.{ .map = .{ .key = k, .value = v } }) catch return null;
    return @ptrCast(node);
}

export fn zp_schema_struct(
    handle_ptr: ?*anyopaque,
    field_names: ?[*]const ?[*:0]const u8,
    field_schemas: ?[*]const ?*const anyopaque,
    field_count: i32,
) ?*const anyopaque {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return null;
    const names = field_names orelse return null;
    const schemas = field_schemas orelse return null;
    const count = wasiToUsize(field_count) orelse return null;
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

export fn zp_row_writer_add_column_nested(handle_ptr: ?*anyopaque, name: ?[*:0]const u8, schema: ?*const anyopaque) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const n = name orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const node = castSchemaNode(schema) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.addColumnNested(n, node) catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "addColumnNested failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

// ============================================================================
// Nested value builders
// ============================================================================

export fn zp_row_writer_begin_list(handle_ptr: ?*anyopaque, col_index: i32) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = wasiToUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.beginList(col) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_end_list(handle_ptr: ?*anyopaque, col_index: i32) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = wasiToUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.endList(col) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_append_null(handle_ptr: ?*anyopaque, col_index: i32) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = wasiToUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.appendNestedValue(col, .null_val) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_append_bool(handle_ptr: ?*anyopaque, col_index: i32, value: i32) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = wasiToUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.appendNestedValue(col, .{ .bool_val = value != 0 }) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_append_int32(handle_ptr: ?*anyopaque, col_index: i32, value: i32) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = wasiToUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.appendNestedValue(col, .{ .int32_val = value }) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_append_int64(handle_ptr: ?*anyopaque, col_index: i32, value: i64) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = wasiToUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.appendNestedValue(col, .{ .int64_val = value }) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_append_float(handle_ptr: ?*anyopaque, col_index: i32, value: f32) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = wasiToUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.appendNestedValue(col, .{ .float_val = value }) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_append_double(handle_ptr: ?*anyopaque, col_index: i32, value: f64) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = wasiToUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.appendNestedValue(col, .{ .double_val = value }) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_append_bytes(handle_ptr: ?*anyopaque, col_index: i32, data: ?[*]const u8, len: usize) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = wasiToUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const ptr = data orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.appendNestedBytes(col, ptr, len) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_begin_struct(handle_ptr: ?*anyopaque, col_index: i32) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = wasiToUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.beginStruct(col) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_set_field_null(handle_ptr: ?*anyopaque, col_index: i32, field_index: i32) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = wasiToUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const fi = wasiToUsize(field_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setStructField(col, fi, .null_val) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_set_field_int32(handle_ptr: ?*anyopaque, col_index: i32, field_index: i32, value: i32) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = wasiToUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const fi = wasiToUsize(field_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setStructField(col, fi, .{ .int32_val = value }) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_set_field_int64(handle_ptr: ?*anyopaque, col_index: i32, field_index: i32, value: i64) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = wasiToUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const fi = wasiToUsize(field_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setStructField(col, fi, .{ .int64_val = value }) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_set_field_float(handle_ptr: ?*anyopaque, col_index: i32, field_index: i32, value: f32) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = wasiToUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const fi = wasiToUsize(field_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setStructField(col, fi, .{ .float_val = value }) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_set_field_double(handle_ptr: ?*anyopaque, col_index: i32, field_index: i32, value: f64) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = wasiToUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const fi = wasiToUsize(field_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setStructField(col, fi, .{ .double_val = value }) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_set_field_bool(handle_ptr: ?*anyopaque, col_index: i32, field_index: i32, value: i32) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = wasiToUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const fi = wasiToUsize(field_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setStructField(col, fi, .{ .bool_val = value != 0 }) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_set_field_bytes(handle_ptr: ?*anyopaque, col_index: i32, field_index: i32, data: ?[*]const u8, len: usize) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = wasiToUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const fi = wasiToUsize(field_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const ptr = data orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setStructFieldBytes(col, fi, ptr, len) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_end_struct(handle_ptr: ?*anyopaque, col_index: i32) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = wasiToUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.endStruct(col) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_begin_map(handle_ptr: ?*anyopaque, col_index: i32) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = wasiToUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.beginMap(col) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_begin_map_entry(handle_ptr: ?*anyopaque, col_index: i32) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = wasiToUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.beginMapEntry(col) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_end_map_entry(handle_ptr: ?*anyopaque, col_index: i32) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = wasiToUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.endMapEntry(col) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_end_map(handle_ptr: ?*anyopaque, col_index: i32) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = wasiToUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.endMap(col) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_begin(handle_ptr: ?*anyopaque) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.begin() catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "begin failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

export fn zp_row_writer_set_null(handle_ptr: ?*anyopaque, col_index: i32) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = wasiToUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setNull(col) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_set_bool(handle_ptr: ?*anyopaque, col_index: i32, value: i32) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = wasiToUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setBool(col, value != 0) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_set_int32(handle_ptr: ?*anyopaque, col_index: i32, value: i32) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = wasiToUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setInt32(col, value) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_set_int64(handle_ptr: ?*anyopaque, col_index: i32, value: i64) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = wasiToUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setInt64(col, value) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_set_float(handle_ptr: ?*anyopaque, col_index: i32, value: f32) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = wasiToUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setFloat(col, value) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_set_double(handle_ptr: ?*anyopaque, col_index: i32, value: f64) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = wasiToUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setDouble(col, value) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_set_bytes(handle_ptr: ?*anyopaque, col_index: i32, data: ?[*]const u8, len: usize) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = wasiToUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const ptr = data orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setBytes(col, ptr, len) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_add_row(handle_ptr: ?*anyopaque) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.addRow() catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_flush(handle_ptr: ?*anyopaque) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.flush() catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "flush failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

export fn zp_row_writer_close(handle_ptr: ?*anyopaque) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.writerClose() catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "close failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

export fn zp_row_writer_get_buffer(handle_ptr: ?*anyopaque, data_out: ?*[*]const u8, len_out: ?*usize) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const d_out = data_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const l_out = len_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;

    switch (handle.backend) {
        .buffer => |bt| {
            const w = bt.written();
            d_out.* = w.ptr;
            l_out.* = w.len;
            return ZP_OK;
        },
        .callback => {
            handle.err_ctx.setError(err.ZP_ERROR_INVALID_STATE, "get_buffer only valid for memory backend");
            return handle.err_ctx.code;
        },
    }
}

export fn zp_row_writer_error_code(handle_ptr: ?*anyopaque) i32 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    return handle.err_ctx.code;
}

export fn zp_row_writer_error_message(handle_ptr: ?*anyopaque) [*:0]const u8 {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return "";
    return handle.err_ctx.message();
}

export fn zp_row_writer_free(handle_ptr: ?*anyopaque) void {
    const handle = castHandle(RowWriterHandle, handle_ptr) orelse return;
    handle.deinit();
}

fn wasiToUsize(v: i32) ?usize {
    if (v < 0) return null;
    return safe.castTo(usize, v) catch unreachable; // guarded by v < 0 check above
}

// ============================================================================
// Introspection exports
// ============================================================================

export fn zp_version() [*:0]const u8 {
    return introspect.getVersion();
}

export fn zp_codec_supported(codec: i32) i32 {
    return introspect.isCodecSupported(codec);
}

// ============================================================================
// Internal helpers
// ============================================================================

pub const wrapColumnsAsStruct = handles.wrapColumnsAsStruct;

fn castHandle(comptime T: type, ptr: ?*anyopaque) ?*T {
    return if (ptr) |p| @ptrCast(@alignCast(p)) else null;
}

fn castSchemaNode(ptr: ?*const anyopaque) ?*const SchemaNode {
    return if (ptr) |p| @ptrCast(@alignCast(p)) else null;
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

    const wrapper = handles.wrapColumnsAsStruct(handle.allocator, result.arrays, &result.schema) catch |e| {
        result.deinit();
        handle.err_ctx.setError(err.mapError(e), err.errorMessage(e));
        return handle.err_ctx.code;
    };

    array_out.* = wrapper.array;
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

fn wasiGetStats(handle: *ReaderHandle, col_index: i32, rg_index: i32) ?format.Statistics {
    const rg_idx = safe.castTo(usize, rg_index) catch return null;
    const col_idx = safe.castTo(usize, col_index) catch return null;
    if (rg_idx >= handle.metadata.row_groups.len) return null;
    const rg = handle.metadata.row_groups[rg_idx];
    if (col_idx >= rg.columns.len) return null;
    const meta = rg.columns[col_idx].meta_data orelse return null;
    return meta.statistics;
}
