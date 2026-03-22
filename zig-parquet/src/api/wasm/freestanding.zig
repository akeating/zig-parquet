//! WASM freestanding API entry points.
//!
//! Export functions for wasm32-freestanding targets. Uses imported host
//! functions for IO and exports memory management functions so the host
//! can allocate buffers in WASM linear memory.
//!
//! The host provides IO through imported functions (extern fn). The library
//! exports reader/writer functions and memory management helpers.
//!
//! Handle IDs are simple integers (indices into an internal handle table)
//! rather than opaque pointers, which simplifies the host-side interface.

const std = @import("std");
const handles = @import("handles.zig");
const err = @import("../../api/c/error.zig");
const introspect = @import("../../api/c/introspect.zig");
const arrow_batch = @import("../../core/arrow_batch.zig");
const safe = @import("../../core/safe.zig");
const format = @import("../../core/format.zig");
const seekable_reader_mod = @import("../../core/seekable_reader.zig");
const write_target_mod = @import("../../core/write_target.zig");

const CallbackReader = @import("../../io/callback_reader.zig").CallbackReader;
const CallbackWriter = @import("../../io/callback_writer.zig").CallbackWriter;

const ArrowSchema = handles.ArrowSchema;
const ArrowArray = handles.ArrowArray;
const ReaderHandle = handles.ReaderHandle;
const WriterHandle = handles.WriterHandle;
const RowReaderHandle = handles.RowReaderHandle;
const Value = handles.Value;
const SeekableReader = seekable_reader_mod.SeekableReader;
const WriteError = write_target_mod.WriteError;

const ZP_OK = err.ZP_OK;
const backing_allocator = std.heap.page_allocator;

// ============================================================================
// Host-imported IO functions
// ============================================================================

/// Read bytes from position `offset` into `buf_ptr[0..buf_len]`.
/// Returns number of bytes read, or negative on error.
extern "env" fn host_read_at(ctx: u32, offset_lo: u32, offset_hi: u32, buf_ptr: [*]u8, buf_len: u32) i32;

/// Return the total size of the data source in bytes.
extern "env" fn host_size(ctx: u32) u64;

/// Write `data_ptr[0..data_len]` to the output sink.
/// Returns 0 on success, non-zero on error.
extern "env" fn host_write(ctx: u32, data_ptr: [*]const u8, data_len: u32) i32;

/// Close the output sink. Returns 0 on success, non-zero on error.
extern "env" fn host_close(ctx: u32) i32;

// ============================================================================
// Host callback adapter
// ============================================================================

const HostReaderCtx = struct {
    host_ctx: u32,

    fn readAt(ptr: *anyopaque, offset: u64, buf: []u8) SeekableReader.Error!usize {
        const self: *HostReaderCtx = @ptrCast(@alignCast(ptr));
        const off_lo: u32 = @truncate(offset);
        const off_hi: u32 = @truncate(offset >> 32);
        const len: u32 = safe.castTo(u32, buf.len) catch return error.InputOutput;
        const result = host_read_at(self.host_ctx, off_lo, off_hi, buf.ptr, len);
        if (result < 0) return error.InputOutput;
        return safe.castTo(usize, result) catch error.InputOutput;
    }

    fn size(ptr: *anyopaque) u64 {
        const self: *HostReaderCtx = @ptrCast(@alignCast(ptr));
        return host_size(self.host_ctx);
    }
};

const HostWriterCtx = struct {
    host_ctx: u32,

    fn write(ptr: *anyopaque, data: []const u8) WriteError!void {
        const self: *HostWriterCtx = @ptrCast(@alignCast(ptr));
        const len: u32 = safe.castTo(u32, data.len) catch return error.WriteError;
        const rc = host_write(self.host_ctx, data.ptr, len);
        if (rc != 0) return error.WriteError;
    }

    fn close(ptr: *anyopaque) WriteError!void {
        const self: *HostWriterCtx = @ptrCast(@alignCast(ptr));
        const rc = host_close(self.host_ctx);
        if (rc != 0) return error.WriteError;
    }
};

// ============================================================================
// Handle table (integer IDs instead of opaque pointers)
// ============================================================================

/// Maximum number of simultaneous reader/writer handles.
/// Returns ZP_ERROR_HANDLE_LIMIT when exhausted.
const MAX_HANDLES = 64;

const RowWriterHandle = handles.RowWriterHandle;
const TypeInfo = handles.TypeInfo;
const typeInfoFromZpType = handles.typeInfoFromZpType;
const SchemaNode = handles.SchemaNode;

const HandleSlot = union(enum) {
    empty: void,
    reader: *ReaderHandle,
    writer: *WriterHandle,
    row_reader: *RowReaderHandle,
    row_writer: *RowWriterHandle,
    stream: *StreamHandle,
};

const StreamHandle = struct {
    reader: *ReaderHandle,
    rg_index: usize,
};

var handle_table: [MAX_HANDLES]HandleSlot = [_]HandleSlot{.{ .empty = {} }} ** MAX_HANDLES;

fn allocHandleSlot() i32 {
    for (&handle_table, 0..) |*slot, i| {
        if (slot.* == .empty) return safe.castTo(i32, i) catch -1;
    }
    return -@as(i32, err.ZP_ERROR_HANDLE_LIMIT);
}

fn freeHandleSlot(id: i32) void {
    const idx = safe.castTo(usize, id) catch return;
    if (idx >= MAX_HANDLES) return;
    handle_table[idx] = .{ .empty = {} };
}

fn getReaderHandle(id: i32) ?*ReaderHandle {
    const idx = safe.castTo(usize, id) catch return null;
    if (idx >= MAX_HANDLES) return null;
    return switch (handle_table[idx]) {
        .reader => |h| h,
        else => null,
    };
}

fn getWriterHandle(id: i32) ?*WriterHandle {
    const idx = safe.castTo(usize, id) catch return null;
    if (idx >= MAX_HANDLES) return null;
    return switch (handle_table[idx]) {
        .writer => |h| h,
        else => null,
    };
}

fn getRowReaderHandle(id: i32) ?*RowReaderHandle {
    const idx = safe.castTo(usize, id) catch return null;
    if (idx >= MAX_HANDLES) return null;
    return switch (handle_table[idx]) {
        .row_reader => |h| h,
        else => null,
    };
}

fn getRowWriterHandle(id: i32) ?*RowWriterHandle {
    const idx = safe.castTo(usize, id) catch return null;
    if (idx >= MAX_HANDLES) return null;
    return switch (handle_table[idx]) {
        .row_writer => |h| h,
        else => null,
    };
}

fn getStreamHandle(id: i32) ?*StreamHandle {
    const idx = safe.castTo(usize, id) catch return null;
    if (idx >= MAX_HANDLES) return null;
    return switch (handle_table[idx]) {
        .stream => |h| h,
        else => null,
    };
}

// ============================================================================
// Memory management exports
// ============================================================================

export fn zp_alloc(len: u32) ?[*]u8 {
    const size = safe.castTo(usize, len) catch return null;
    const slice = backing_allocator.alloc(u8, size) catch return null;
    return slice.ptr;
}

export fn zp_free(ptr: [*]u8, len: u32) void {
    const size = safe.castTo(usize, len) catch return;
    backing_allocator.free(ptr[0..size]);
}

// ============================================================================
// Reader exports
// ============================================================================

/// Open a reader from a buffer in WASM linear memory.
/// Returns a handle ID (>= 0) on success, or a negative error code.
export fn zp_reader_open_buffer(data: ?[*]const u8, len: u32) i32 {
    const ptr = data orelse return -err.ZP_ERROR_INVALID_ARGUMENT;
    const size = safe.castTo(usize, len) catch return -err.ZP_ERROR_INVALID_ARGUMENT;

    const slot = allocHandleSlot();
    if (slot < 0) return slot;

    const handle = ReaderHandle.openMemory(ptr, size) catch |e| {
        return -err.mapError(e);
    };

    const idx = safe.castTo(usize, slot) catch unreachable; // allocHandleSlot returns valid index
    handle_table[idx] = .{ .reader = handle };
    return slot;
}

/// Open a reader using host-provided IO callbacks.
/// `ctx` is an opaque context ID passed to host_read_at and host_size.
/// Returns a handle ID (>= 0) on success, or a negative error code.
export fn zp_reader_open_host(ctx: u32) i32 {
    const slot = allocHandleSlot();
    if (slot < 0) return slot;

    const allocator = backing_allocator;
    const host_ctx = allocator.create(HostReaderCtx) catch return -err.ZP_ERROR_OUT_OF_MEMORY;
    host_ctx.* = .{ .host_ctx = ctx };

    const cb = allocator.create(CallbackReader) catch {
        allocator.destroy(host_ctx);
        return -err.ZP_ERROR_OUT_OF_MEMORY;
    };
    cb.* = .{
        .ctx = @ptrCast(host_ctx),
        .read_at_fn = HostReaderCtx.readAt,
        .size_fn = HostReaderCtx.size,
    };

    const handle = ReaderHandle.openCallbacks(cb) catch |e| {
        allocator.destroy(cb);
        allocator.destroy(host_ctx);
        return -err.mapError(e);
    };

    const idx = safe.castTo(usize, slot) catch unreachable; // allocHandleSlot returns valid index
    handle_table[idx] = .{ .reader = handle };
    return slot;
}

export fn zp_reader_get_num_row_groups(handle_id: i32) i32 {
    const handle = getReaderHandle(handle_id) orelse return -err.ZP_ERROR_INVALID_ARGUMENT;
    return safe.castTo(i32, handle.metadata.row_groups.len) catch -1;
}

export fn zp_reader_get_num_rows(handle_id: i32, count_out: ?*i64) i32 {
    const handle = getReaderHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
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

export fn zp_reader_get_row_group_num_rows(handle_id: i32, rg_index: i32, count_out: ?*i64) i32 {
    const handle = getReaderHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
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

export fn zp_reader_get_column_count(handle_id: i32, count_out: ?*i32) i32 {
    const handle = getReaderHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
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

export fn zp_reader_get_schema(handle_id: i32, schema_out: ?*ArrowSchema) i32 {
    const handle = getReaderHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
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
    handle_id: i32,
    rg_index: i32,
    col_indices: ?[*]const i32,
    num_cols: i32,
    arrays_out: ?*ArrowArray,
    schema_out: ?*ArrowSchema,
) i32 {
    const handle = getReaderHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
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
            handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "num_cols must be positive");
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

export fn zp_reader_error_code(handle_id: i32) i32 {
    const handle = getReaderHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    return handle.err_ctx.code;
}

export fn zp_reader_error_message(handle_id: i32) [*:0]const u8 {
    const handle = getReaderHandle(handle_id) orelse return "";
    return handle.err_ctx.message();
}

export fn zp_reader_has_statistics(handle_id: i32, col_index: i32, rg_index: i32) i32 {
    const handle = getReaderHandle(handle_id) orelse return 0;
    const rg_idx = safe.castTo(usize, rg_index) catch return 0;
    const col_idx = safe.castTo(usize, col_index) catch return 0;
    if (rg_idx >= handle.metadata.row_groups.len) return 0;
    const rg = handle.metadata.row_groups[rg_idx];
    if (col_idx >= rg.columns.len) return 0;
    const meta = rg.columns[col_idx].meta_data orelse return 0;
    return if (meta.statistics != null) 1 else 0;
}

export fn zp_reader_get_null_count(handle_id: i32, col_index: i32, rg_index: i32, count_out: ?*i64) i32 {
    const handle = getReaderHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const out = count_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const stats = fsGetStats(handle, col_index, rg_index) orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "no statistics available");
        return handle.err_ctx.code;
    };
    out.* = stats.null_count orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_DATA, "null count not available");
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

export fn zp_reader_get_distinct_count(handle_id: i32, col_index: i32, rg_index: i32, count_out: ?*i64) i32 {
    const handle = getReaderHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const out = count_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const stats = fsGetStats(handle, col_index, rg_index) orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "no statistics available");
        return handle.err_ctx.code;
    };
    out.* = stats.distinct_count orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_DATA, "distinct count not available");
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

export fn zp_reader_get_min_value(handle_id: i32, col_index: i32, rg_index: i32, data_out: ?*[*]const u8, len_out: ?*u32) i32 {
    const handle = getReaderHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const d_out = data_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const l_out = len_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const stats = fsGetStats(handle, col_index, rg_index) orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "no statistics available");
        return handle.err_ctx.code;
    };
    const min = stats.min_value orelse stats.min orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_DATA, "min value not available");
        return handle.err_ctx.code;
    };
    d_out.* = min.ptr;
    l_out.* = safe.castTo(u32, min.len) catch {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_DATA, "value too large for u32");
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

export fn zp_reader_get_max_value(handle_id: i32, col_index: i32, rg_index: i32, data_out: ?*[*]const u8, len_out: ?*u32) i32 {
    const handle = getReaderHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const d_out = data_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const l_out = len_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const stats = fsGetStats(handle, col_index, rg_index) orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "no statistics available");
        return handle.err_ctx.code;
    };
    const max = stats.max_value orelse stats.max orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_DATA, "max value not available");
        return handle.err_ctx.code;
    };
    d_out.* = max.ptr;
    l_out.* = safe.castTo(u32, max.len) catch {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_DATA, "value too large for u32");
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

export fn zp_reader_get_kv_metadata_count(handle_id: i32, count_out: ?*i32) i32 {
    const handle = getReaderHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
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

export fn zp_reader_get_kv_metadata_key(handle_id: i32, index: i32) ?[*]const u8 {
    const handle = getReaderHandle(handle_id) orelse return null;
    const kvs = handle.metadata.key_value_metadata orelse return null;
    const idx = safe.castTo(usize, index) catch return null;
    if (idx >= kvs.len) return null;
    return kvs[idx].key.ptr;
}

export fn zp_reader_get_kv_metadata_key_len(handle_id: i32, index: i32) u32 {
    const handle = getReaderHandle(handle_id) orelse return 0;
    const kvs = handle.metadata.key_value_metadata orelse return 0;
    const idx = safe.castTo(usize, index) catch return 0;
    if (idx >= kvs.len) return 0;
    return safe.castTo(u32, kvs[idx].key.len) catch 0;
}

export fn zp_reader_get_kv_metadata_value(handle_id: i32, index: i32) ?[*]const u8 {
    const handle = getReaderHandle(handle_id) orelse return null;
    const kvs = handle.metadata.key_value_metadata orelse return null;
    const idx = safe.castTo(usize, index) catch return null;
    if (idx >= kvs.len) return null;
    const val = kvs[idx].value orelse return null;
    return val.ptr;
}

export fn zp_reader_get_kv_metadata_value_len(handle_id: i32, index: i32) u32 {
    const handle = getReaderHandle(handle_id) orelse return 0;
    const kvs = handle.metadata.key_value_metadata orelse return 0;
    const idx = safe.castTo(usize, index) catch return 0;
    if (idx >= kvs.len) return 0;
    const val = kvs[idx].value orelse return 0;
    return safe.castTo(u32, val.len) catch 0;
}

export fn zp_reader_close(handle_id: i32) void {
    const handle = getReaderHandle(handle_id) orelse return;
    handle.close();
    freeHandleSlot(handle_id);
}

// ============================================================================
// Writer exports
// ============================================================================

/// Open a writer to an in-memory buffer.
/// Returns a handle ID (>= 0) on success, or a negative error code.
export fn zp_writer_open_buffer() i32 {
    const slot = allocHandleSlot();
    if (slot < 0) return slot;

    const handle = WriterHandle.openMemory() catch |e| {
        return -err.mapError(e);
    };

    const idx = safe.castTo(usize, slot) catch unreachable; // allocHandleSlot returns valid index
    handle_table[idx] = .{ .writer = handle };
    return slot;
}

/// Open a writer using host-provided IO callbacks.
/// `ctx` is an opaque context ID passed to host_write and host_close.
/// Returns a handle ID (>= 0) on success, or a negative error code.
export fn zp_writer_open_host(ctx: u32) i32 {
    const slot = allocHandleSlot();
    if (slot < 0) return slot;

    const allocator = backing_allocator;
    const host_ctx = allocator.create(HostWriterCtx) catch return -err.ZP_ERROR_OUT_OF_MEMORY;
    host_ctx.* = .{ .host_ctx = ctx };

    const cb = allocator.create(CallbackWriter) catch {
        allocator.destroy(host_ctx);
        return -err.ZP_ERROR_OUT_OF_MEMORY;
    };
    cb.* = .{
        .ctx = @ptrCast(host_ctx),
        .write_fn = HostWriterCtx.write,
        .close_fn = HostWriterCtx.close,
    };

    const handle = WriterHandle.openCallbackWriter(cb) catch |e| {
        allocator.destroy(cb);
        allocator.destroy(host_ctx);
        return -err.mapError(e);
    };

    const idx = safe.castTo(usize, slot) catch unreachable; // allocHandleSlot returns valid index
    handle_table[idx] = .{ .writer = handle };
    return slot;
}

export fn zp_writer_set_schema(handle_id: i32, schema: ?*const ArrowSchema) i32 {
    const handle = getWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
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
    handle_id: i32,
    col_index: i32,
    codec: i32,
) i32 {
    const handle = getWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
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
    handle_id: i32,
    size_bytes: i64,
) i32 {
    const handle = getWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
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
    handle_id: i32,
    batch: ?*const ArrowArray,
    schema: ?*const ArrowSchema,
) i32 {
    const handle = getWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
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
        if (nc == 0) return ZP_OK;
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
    handle_id: i32,
    data_out: ?*[*]const u8,
    len_out: ?*u32,
) i32 {
    const handle = getWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const d_out = data_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const l_out = len_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.err_ctx.setOk();

    switch (handle.backend) {
        .buffer => |bt| {
            const w = bt.written();
            d_out.* = w.ptr;
            l_out.* = safe.castTo(u32, w.len) catch {
                handle.err_ctx.setError(err.ZP_ERROR_INVALID_DATA, "buffer too large for u32");
                return handle.err_ctx.code;
            };
            return ZP_OK;
        },
        .callback => {
            handle.err_ctx.setError(err.ZP_ERROR_INVALID_STATE, "get_buffer not available for callback writers");
            return handle.err_ctx.code;
        },
    }
}

export fn zp_writer_error_code(handle_id: i32) i32 {
    const handle = getWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    return handle.err_ctx.code;
}

export fn zp_writer_error_message(handle_id: i32) [*:0]const u8 {
    const handle = getWriterHandle(handle_id) orelse return "";
    return handle.err_ctx.message();
}

export fn zp_writer_close(handle_id: i32) i32 {
    const handle = getWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
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

export fn zp_writer_free(handle_id: i32) void {
    const handle = getWriterHandle(handle_id) orelse return;
    handle.deinit();
    freeHandleSlot(handle_id);
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
// Row Reader exports (cursor-based, non-Arrow)
// ============================================================================

export fn zp_row_reader_open_buffer(data: ?[*]const u8, len: u32) i32 {
    const ptr = data orelse return -err.ZP_ERROR_INVALID_ARGUMENT;
    const size = safe.castTo(usize, len) catch return -err.ZP_ERROR_INVALID_ARGUMENT;

    const slot = allocHandleSlot();
    if (slot < 0) return slot;

    const handle = RowReaderHandle.openMemory(ptr, size) catch |e| {
        return -err.mapError(e);
    };

    const idx = safe.castTo(usize, slot) catch unreachable; // allocHandleSlot returns valid index
    handle_table[idx] = .{ .row_reader = handle };
    return slot;
}

export fn zp_row_reader_open_host(ctx: u32) i32 {
    const slot = allocHandleSlot();
    if (slot < 0) return slot;

    const allocator = backing_allocator;
    const host_ctx = allocator.create(HostReaderCtx) catch return -err.ZP_ERROR_OUT_OF_MEMORY;
    host_ctx.* = .{ .host_ctx = ctx };

    const cb = allocator.create(CallbackReader) catch {
        allocator.destroy(host_ctx);
        return -err.ZP_ERROR_OUT_OF_MEMORY;
    };
    cb.* = .{
        .ctx = @ptrCast(host_ctx),
        .read_at_fn = HostReaderCtx.readAt,
        .size_fn = HostReaderCtx.size,
    };

    const handle = RowReaderHandle.openCallbacks(cb) catch |e| {
        allocator.destroy(cb);
        allocator.destroy(host_ctx);
        return -err.mapError(e);
    };

    const idx = safe.castTo(usize, slot) catch unreachable; // allocHandleSlot returns valid index
    handle_table[idx] = .{ .row_reader = handle };
    return slot;
}

export fn zp_row_reader_get_num_row_groups(handle_id: i32) i32 {
    const handle = getRowReaderHandle(handle_id) orelse return -err.ZP_ERROR_INVALID_ARGUMENT;
    return safe.castTo(i32, handle.reader.metadata.row_groups.len) catch -1;
}

export fn zp_row_reader_get_num_rows(handle_id: i32, count_out: ?*i64) i32 {
    const handle = getRowReaderHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
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

export fn zp_row_reader_get_column_count(handle_id: i32, count_out: ?*i32) i32 {
    const handle = getRowReaderHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const out = count_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    out.* = safe.castTo(i32, handle.num_top_columns) catch {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_DATA, "column count too large");
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

export fn zp_row_reader_get_column_name(handle_id: i32, col_index: i32) ?[*:0]const u8 {
    const handle = getRowReaderHandle(handle_id) orelse return null;
    const idx = safe.castTo(usize, col_index) catch return null;
    if (idx >= handle.col_names.len) return null;
    return handle.col_names[idx];
}

export fn zp_row_reader_read_row_group(handle_id: i32, rg_index: i32) i32 {
    const handle = getRowReaderHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
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

export fn zp_row_reader_next(handle_id: i32) i32 {
    const handle = getRowReaderHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    if (handle.next()) return ZP_OK;
    return err.ZP_ROW_END;
}

export fn zp_row_reader_next_all(handle_id: i32) i32 {
    const handle = getRowReaderHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const has_row = handle.nextAll() catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "nextAll failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return if (has_row) ZP_OK else err.ZP_ROW_END;
}

export fn zp_row_reader_read_row_group_projected(
    handle_id: i32,
    rg_index: i32,
    col_indices: ?[*]const i32,
    num_cols: i32,
) i32 {
    const handle = getRowReaderHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
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

export fn zp_row_reader_get_type(handle_id: i32, col_index: i32) i32 {
    const handle = getRowReaderHandle(handle_id) orelse return err.ZP_TYPE_NULL;
    const val = getRowValue(handle_id, col_index) orelse return err.ZP_TYPE_NULL;
    if (val.isNull()) return err.ZP_TYPE_NULL;
    const idx = safe.castTo(usize, col_index) catch return wasmPhysicalValueType(val);
    return wasmColumnType(&handle.reader, idx, val);
}

export fn zp_row_reader_get_column_type(handle_id: i32, col_index: i32) i32 {
    const handle = getRowReaderHandle(handle_id) orelse return err.ZP_TYPE_NULL;
    const idx = safe.castTo(usize, col_index) catch return err.ZP_TYPE_NULL;
    return wasmSchemaColumnType(&handle.reader, idx);
}

export fn zp_row_reader_get_decimal_precision(handle_id: i32, col_index: i32) i32 {
    const handle = getRowReaderHandle(handle_id) orelse return 0;
    const idx = safe.castTo(usize, col_index) catch return 0;
    const elem = handle.reader.getLeafSchemaElement(idx) orelse return 0;
    if (elem.logical_type) |lt| {
        if (lt == .decimal) return lt.decimal.precision;
    }
    return elem.precision orelse 0;
}

export fn zp_row_reader_get_decimal_scale(handle_id: i32, col_index: i32) i32 {
    const handle = getRowReaderHandle(handle_id) orelse return 0;
    const idx = safe.castTo(usize, col_index) catch return 0;
    const elem = handle.reader.getLeafSchemaElement(idx) orelse return 0;
    if (elem.logical_type) |lt| {
        if (lt == .decimal) return lt.decimal.scale;
    }
    return elem.scale orelse 0;
}

export fn zp_row_reader_is_null(handle_id: i32, col_index: i32) i32 {
    const val = getRowValue(handle_id, col_index) orelse return 1;
    return if (val.isNull()) @as(i32, 1) else @as(i32, 0);
}

export fn zp_row_reader_get_int32(handle_id: i32, col_index: i32) i32 {
    const val = getRowValue(handle_id, col_index) orelse return 0;
    return val.asInt32() orelse 0;
}

export fn zp_row_reader_get_int64(handle_id: i32, col_index: i32) i64 {
    const val = getRowValue(handle_id, col_index) orelse return 0;
    return val.asInt64() orelse 0;
}

export fn zp_row_reader_get_float(handle_id: i32, col_index: i32) f32 {
    const val = getRowValue(handle_id, col_index) orelse return 0;
    return val.asFloat() orelse 0;
}

export fn zp_row_reader_get_double(handle_id: i32, col_index: i32) f64 {
    const val = getRowValue(handle_id, col_index) orelse return 0;
    return val.asDouble() orelse 0;
}

export fn zp_row_reader_get_bool(handle_id: i32, col_index: i32) i32 {
    const val = getRowValue(handle_id, col_index) orelse return 0;
    return if (val.asBool() orelse false) @as(i32, 1) else @as(i32, 0);
}

export fn zp_row_reader_get_bytes_ptr(handle_id: i32, col_index: i32) ?[*]const u8 {
    const val = getRowValue(handle_id, col_index) orelse return null;
    const bytes = val.asBytes() orelse return null;
    if (bytes.len == 0) return null;
    return bytes.ptr;
}

export fn zp_row_reader_get_bytes_len(handle_id: i32, col_index: i32) u32 {
    const val = getRowValue(handle_id, col_index) orelse return 0;
    const bytes = val.asBytes() orelse return 0;
    return safe.castTo(u32, bytes.len) catch 0;
}

export fn zp_row_reader_get_kv_metadata_count(handle_id: i32, count_out: ?*i32) i32 {
    const handle = getRowReaderHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
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

export fn zp_row_reader_get_kv_metadata_key(handle_id: i32, index: i32) ?[*]const u8 {
    const handle = getRowReaderHandle(handle_id) orelse return null;
    const kvs = handle.reader.metadata.key_value_metadata orelse return null;
    const idx = safe.castTo(usize, index) catch return null;
    if (idx >= kvs.len) return null;
    return kvs[idx].key.ptr;
}

export fn zp_row_reader_get_kv_metadata_key_len(handle_id: i32, index: i32) u32 {
    const handle = getRowReaderHandle(handle_id) orelse return 0;
    const kvs = handle.reader.metadata.key_value_metadata orelse return 0;
    const idx = safe.castTo(usize, index) catch return 0;
    if (idx >= kvs.len) return 0;
    return safe.castTo(u32, kvs[idx].key.len) catch 0;
}

export fn zp_row_reader_get_kv_metadata_value(handle_id: i32, index: i32) ?[*]const u8 {
    const handle = getRowReaderHandle(handle_id) orelse return null;
    const kvs = handle.reader.metadata.key_value_metadata orelse return null;
    const idx = safe.castTo(usize, index) catch return null;
    if (idx >= kvs.len) return null;
    const val = kvs[idx].value orelse return null;
    return val.ptr;
}

export fn zp_row_reader_get_kv_metadata_value_len(handle_id: i32, index: i32) u32 {
    const handle = getRowReaderHandle(handle_id) orelse return 0;
    const kvs = handle.reader.metadata.key_value_metadata orelse return 0;
    const idx = safe.castTo(usize, index) catch return 0;
    if (idx >= kvs.len) return 0;
    const val = kvs[idx].value orelse return 0;
    return safe.castTo(u32, val.len) catch 0;
}

export fn zp_row_reader_set_checksum_validation(handle_id: i32, validate: i32, strict: i32) i32 {
    const handle = getRowReaderHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.reader.checksum_options = .{
        .validate_page_checksum = validate != 0,
        .strict_checksum = strict != 0,
    };
    return ZP_OK;
}

export fn zp_row_reader_error_code(handle_id: i32) i32 {
    const handle = getRowReaderHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    return handle.err_ctx.code;
}

export fn zp_row_reader_error_message(handle_id: i32) [*:0]const u8 {
    const handle = getRowReaderHandle(handle_id) orelse return "";
    return handle.err_ctx.message();
}

export fn zp_row_reader_get_value(handle_id: i32, col_index: i32) ?*const anyopaque {
    const ref = getRowValueRef(handle_id, col_index) orelse return null;
    return @ptrCast(ref);
}

// ============================================================================
// Value API exports (pointer-based, not handle-based)
// ============================================================================

export fn zp_value_get_type(value_ptr: ?*const anyopaque) i32 {
    const val = castValue(value_ptr) orelse return err.ZP_TYPE_NULL;
    return wasmPhysicalValueType(val.*);
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

export fn zp_value_get_bytes_ptr(value_ptr: ?*const anyopaque) ?[*]const u8 {
    const val = castValue(value_ptr) orelse return null;
    const bytes = val.asBytes() orelse return null;
    if (bytes.len == 0) return null;
    return bytes.ptr;
}

export fn zp_value_get_bytes_len(value_ptr: ?*const anyopaque) u32 {
    const val = castValue(value_ptr) orelse return 0;
    const bytes = val.asBytes() orelse return 0;
    return safe.castTo(u32, bytes.len) catch 0;
}

export fn zp_value_get_list_len(value_ptr: ?*const anyopaque) i32 {
    const val = castValue(value_ptr) orelse return 0;
    const items = val.asList() orelse return 0;
    return safe.castTo(i32, items.len) catch 0;
}

export fn zp_value_get_list_element(value_ptr: ?*const anyopaque, index: i32) ?*const anyopaque {
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

export fn zp_value_get_map_key(value_ptr: ?*const anyopaque, entry_index: i32) ?*const anyopaque {
    const val = castValue(value_ptr) orelse return null;
    const entries = val.asMap() orelse return null;
    const idx = safe.castTo(usize, entry_index) catch return null;
    if (idx >= entries.len) return null;
    return @ptrCast(&entries[idx].key);
}

export fn zp_value_get_map_value(value_ptr: ?*const anyopaque, entry_index: i32) ?*const anyopaque {
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

export fn zp_value_get_struct_field_name_ptr(value_ptr: ?*const anyopaque, field_index: i32) ?[*]const u8 {
    const val = castValue(value_ptr) orelse return null;
    const fields = val.asStruct() orelse return null;
    const idx = safe.castTo(usize, field_index) catch return null;
    if (idx >= fields.len) return null;
    if (fields[idx].name.len == 0) return null;
    return fields[idx].name.ptr;
}

export fn zp_value_get_struct_field_name_len(value_ptr: ?*const anyopaque, field_index: i32) u32 {
    const val = castValue(value_ptr) orelse return 0;
    const fields = val.asStruct() orelse return 0;
    const idx = safe.castTo(usize, field_index) catch return 0;
    if (idx >= fields.len) return 0;
    return safe.castTo(u32, fields[idx].name.len) catch 0;
}

export fn zp_value_get_struct_field_value(value_ptr: ?*const anyopaque, field_index: i32) ?*const anyopaque {
    const val = castValue(value_ptr) orelse return null;
    const fields = val.asStruct() orelse return null;
    const idx = safe.castTo(usize, field_index) catch return null;
    if (idx >= fields.len) return null;
    return @ptrCast(&fields[idx].value);
}

export fn zp_row_reader_close(handle_id: i32) void {
    const handle = getRowReaderHandle(handle_id) orelse return;
    handle.close();
    freeHandleSlot(handle_id);
}

fn getRowValue(handle_id: i32, col_index: i32) ?Value {
    const handle = getRowReaderHandle(handle_id) orelse return null;
    const row = handle.currentRow() orelse return null;
    const idx = safe.castTo(usize, col_index) catch return null;
    if (idx >= row.values.len) return null;
    return row.values[idx];
}

fn getRowValueRef(handle_id: i32, col_index: i32) ?*const Value {
    const handle = getRowReaderHandle(handle_id) orelse return null;
    const row = handle.currentRow() orelse return null;
    const idx = safe.castTo(usize, col_index) catch return null;
    if (idx >= row.values.len) return null;
    return &row.values[idx];
}

fn castValue(ptr: ?*const anyopaque) ?*const Value {
    return if (ptr) |p| @ptrCast(@alignCast(p)) else null;
}

const core_dynamic = @import("../../core/dynamic_reader.zig");

fn wasmPhysicalValueType(v: Value) i32 {
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

fn logicalTypeToZpType(lt: format.LogicalType) i32 {
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

fn convertedTypeToZpType(ct: i32) ?i32 {
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

fn wasmColumnType(reader: *const core_dynamic.DynamicReader, col_idx: usize, v: Value) i32 {
    if (reader.getColumnLogicalType(col_idx)) |lt| return logicalTypeToZpType(lt);
    const elem = reader.getLeafSchemaElement(col_idx) orelse return wasmPhysicalValueType(v);
    if (elem.converted_type) |ct| {
        if (convertedTypeToZpType(ct)) |zpt| return zpt;
    }
    return wasmPhysicalValueType(v);
}

fn wasmSchemaColumnType(reader: *const core_dynamic.DynamicReader, col_idx: usize) i32 {
    if (reader.getColumnLogicalType(col_idx)) |lt| return logicalTypeToZpType(lt);
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

// ============================================================================
// Row Writer exports (cursor-based, non-Arrow, multi-row-group)
// ============================================================================

export fn zp_row_writer_open_buffer() i32 {
    const slot = allocHandleSlot();
    if (slot < 0) return slot;

    const handle = RowWriterHandle.openMemory() catch |e| {
        return -err.mapError(e);
    };

    const idx = safe.castTo(usize, slot) catch unreachable; // allocHandleSlot returns valid index
    handle_table[idx] = .{ .row_writer = handle };
    return slot;
}

export fn zp_row_writer_open_host(ctx: u32) i32 {
    const slot = allocHandleSlot();
    if (slot < 0) return slot;

    const allocator = backing_allocator;
    const host_ctx = allocator.create(HostWriterCtx) catch return -err.ZP_ERROR_OUT_OF_MEMORY;
    host_ctx.* = .{ .host_ctx = ctx };

    const cb = allocator.create(CallbackWriter) catch {
        allocator.destroy(host_ctx);
        return -err.ZP_ERROR_OUT_OF_MEMORY;
    };
    cb.* = .{
        .ctx = @ptrCast(host_ctx),
        .write_fn = HostWriterCtx.write,
        .close_fn = HostWriterCtx.close,
    };

    const handle = RowWriterHandle.openCallbackWriter(cb) catch |e| {
        allocator.destroy(cb);
        allocator.destroy(host_ctx);
        return -err.mapError(e);
    };

    const idx = safe.castTo(usize, slot) catch unreachable; // allocHandleSlot returns valid index
    handle_table[idx] = .{ .row_writer = handle };
    return slot;
}

export fn zp_row_writer_add_column(handle_id: i32, name: ?[*:0]const u8, col_type: i32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const n = name orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const info = typeInfoFromZpType(col_type) orelse {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "unknown column type");
        return handle.err_ctx.code;
    };
    handle.addColumn(n, info) catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "addColumn failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

export fn zp_row_writer_add_column_decimal(handle_id: i32, name: ?[*:0]const u8, precision: i32, scale: i32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
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

export fn zp_row_writer_add_column_geometry(handle_id: i32, name: ?[*:0]const u8, crs_ptr: ?[*]const u8, crs_len: i32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
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

export fn zp_row_writer_add_column_geography(handle_id: i32, name: ?[*:0]const u8, crs_ptr: ?[*]const u8, crs_len: i32, algorithm: i32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
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

export fn zp_row_writer_set_compression(handle_id: i32, codec: i32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    if (handle.writer.began) {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_STATE, "cannot set compression after begin");
        return handle.err_ctx.code;
    }
    const cc = format.CompressionCodec.fromInt(codec) catch {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "unknown compression codec");
        return handle.err_ctx.code;
    };
    handle.writer.setCompression(cc);
    return ZP_OK;
}

export fn zp_row_writer_set_column_codec(handle_id: i32, col_index: i32, codec: i32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    if (handle.writer.began) {
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
    const cc2 = format.CompressionCodec.fromInt(codec) catch {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "unknown compression codec");
        return handle.err_ctx.code;
    };
    handle.writer.setColumnCompression(idx, cc2) catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "setColumnCodec failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

export fn zp_row_writer_set_row_group_size(handle_id: i32, max_rows: i64) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    if (max_rows <= 0) {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "row group size must be positive");
        return handle.err_ctx.code;
    }
    const limit = safe.castTo(usize, max_rows) catch {
        handle.err_ctx.setError(err.ZP_ERROR_INVALID_ARGUMENT, "row group size out of range");
        return handle.err_ctx.code;
    };
    handle.writer.setRowGroupSize(limit);
    return ZP_OK;
}

export fn zp_row_writer_set_kv_metadata(handle_id: i32, key: ?[*]const u8, key_len: u32, value: ?[*]const u8, value_len: u32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const k = key orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const kl = safe.castTo(usize, key_len) catch return err.ZP_ERROR_INVALID_ARGUMENT;
    const vl = safe.castTo(usize, value_len) catch return err.ZP_ERROR_INVALID_ARGUMENT;
    const v: ?[]const u8 = if (value) |vp| vp[0..vl] else null;
    handle.setKvMetadata(k[0..kl], v) catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "setKvMetadata failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

// ============================================================================
// Schema builder (nested types)
// ============================================================================

export fn zp_schema_primitive(handle_id: i32, zp_type: i32) ?*const anyopaque {
    const handle = getRowWriterHandle(handle_id) orelse return null;
    const info = typeInfoFromZpType(zp_type) orelse return null;
    const node = schemaNodeFromTypeInfo(handle, info) catch return null;
    return @ptrCast(node);
}

export fn zp_schema_decimal(handle_id: i32, precision: i32, scale: i32) ?*const anyopaque {
    const handle = getRowWriterHandle(handle_id) orelse return null;
    if (precision < 1 or precision > 38) return null;
    if (scale < 0 or scale > precision) return null;
    const info = TypeInfo.forDecimal(precision, scale);
    const node = schemaNodeFromTypeInfo(handle, info) catch return null;
    return @ptrCast(node);
}

export fn zp_schema_optional(handle_id: i32, inner: ?*const anyopaque) ?*const anyopaque {
    const handle = getRowWriterHandle(handle_id) orelse return null;
    const child = castSchemaNode(inner) orelse return null;
    const node = handle.allocSchemaNode(.{ .optional = child }) catch return null;
    return @ptrCast(node);
}

export fn zp_schema_list(handle_id: i32, element: ?*const anyopaque) ?*const anyopaque {
    const handle = getRowWriterHandle(handle_id) orelse return null;
    const elem = castSchemaNode(element) orelse return null;
    const node = handle.allocSchemaNode(.{ .list = elem }) catch return null;
    return @ptrCast(node);
}

export fn zp_schema_map(handle_id: i32, key_schema: ?*const anyopaque, value_schema: ?*const anyopaque) ?*const anyopaque {
    const handle = getRowWriterHandle(handle_id) orelse return null;
    const k = castSchemaNode(key_schema) orelse return null;
    const v = castSchemaNode(value_schema) orelse return null;
    const node = handle.allocSchemaNode(.{ .map = .{ .key = k, .value = v } }) catch return null;
    return @ptrCast(node);
}

export fn zp_schema_struct(
    handle_id: i32,
    field_names: ?[*]const ?[*:0]const u8,
    field_schemas: ?[*]const ?*const anyopaque,
    field_count: i32,
) ?*const anyopaque {
    const handle = getRowWriterHandle(handle_id) orelse return null;
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

export fn zp_row_writer_add_column_nested(handle_id: i32, name: ?[*:0]const u8, schema: ?*const anyopaque) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
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

export fn zp_row_writer_begin_list(handle_id: i32, col_index: i32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.beginList(col) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_end_list(handle_id: i32, col_index: i32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.endList(col) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_append_null(handle_id: i32, col_index: i32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.appendNestedValue(col, .null_val) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_append_bool(handle_id: i32, col_index: i32, value: i32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.appendNestedValue(col, .{ .bool_val = value != 0 }) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_append_int32(handle_id: i32, col_index: i32, value: i32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.appendNestedValue(col, .{ .int32_val = value }) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_append_int64(handle_id: i32, col_index: i32, value: i64) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.appendNestedValue(col, .{ .int64_val = value }) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_append_float(handle_id: i32, col_index: i32, value: f32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.appendNestedValue(col, .{ .float_val = value }) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_append_double(handle_id: i32, col_index: i32, value: f64) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.appendNestedValue(col, .{ .double_val = value }) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_append_bytes(handle_id: i32, col_index: i32, data: ?[*]const u8, len: u32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const ptr = data orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const size = safe.castTo(usize, len) catch return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.appendNestedBytes(col, ptr, size) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_begin_struct(handle_id: i32, col_index: i32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.beginStruct(col) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_set_field_null(handle_id: i32, col_index: i32, field_index: i32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const fi = toUsize(field_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setStructField(col, fi, .null_val) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_set_field_int32(handle_id: i32, col_index: i32, field_index: i32, value: i32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const fi = toUsize(field_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setStructField(col, fi, .{ .int32_val = value }) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_set_field_int64(handle_id: i32, col_index: i32, field_index: i32, value: i64) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const fi = toUsize(field_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setStructField(col, fi, .{ .int64_val = value }) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_set_field_float(handle_id: i32, col_index: i32, field_index: i32, value: f32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const fi = toUsize(field_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setStructField(col, fi, .{ .float_val = value }) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_set_field_double(handle_id: i32, col_index: i32, field_index: i32, value: f64) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const fi = toUsize(field_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setStructField(col, fi, .{ .double_val = value }) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_set_field_bool(handle_id: i32, col_index: i32, field_index: i32, value: i32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const fi = toUsize(field_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setStructField(col, fi, .{ .bool_val = value != 0 }) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_set_field_bytes(handle_id: i32, col_index: i32, field_index: i32, data: ?[*]const u8, len: u32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const fi = toUsize(field_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const ptr = data orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const size = safe.castTo(usize, len) catch return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setStructFieldBytes(col, fi, ptr, size) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_end_struct(handle_id: i32, col_index: i32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.endStruct(col) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_begin_map(handle_id: i32, col_index: i32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.beginMap(col) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_begin_map_entry(handle_id: i32, col_index: i32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.beginMapEntry(col) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_end_map_entry(handle_id: i32, col_index: i32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.endMapEntry(col) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_end_map(handle_id: i32, col_index: i32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.endMap(col) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_begin(handle_id: i32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.begin() catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "begin failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

export fn zp_row_writer_set_null(handle_id: i32, col_index: i32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setNull(col) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_set_bool(handle_id: i32, col_index: i32, value: i32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setBool(col, value != 0) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_set_int32(handle_id: i32, col_index: i32, value: i32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setInt32(col, value) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_set_int64(handle_id: i32, col_index: i32, value: i64) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setInt64(col, value) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_set_float(handle_id: i32, col_index: i32, value: f32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setFloat(col, value) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_set_double(handle_id: i32, col_index: i32, value: f64) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setDouble(col, value) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_set_bytes(handle_id: i32, col_index: i32, data: ?[*]const u8, len: u32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const col = toUsize(col_index) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const ptr = data orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const size = safe.castTo(usize, len) catch return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.setBytes(col, ptr, size) catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_add_row(handle_id: i32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.addRow() catch |e| return err.mapError(e);
    return ZP_OK;
}

export fn zp_row_writer_flush(handle_id: i32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.flush() catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "flush failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

export fn zp_row_writer_close(handle_id: i32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    handle.writerClose() catch |e| {
        handle.err_ctx.setErrorFmt(err.mapError(e), "close failed: {s}", .{err.errorMessage(e)});
        return handle.err_ctx.code;
    };
    return ZP_OK;
}

export fn zp_row_writer_get_buffer(handle_id: i32, data_out: ?*[*]const u8, len_out: ?*u32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const d_out = data_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const l_out = len_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;

    switch (handle.backend) {
        .buffer => |bt| {
            const w = bt.written();
            d_out.* = w.ptr;
            l_out.* = safe.castTo(u32, w.len) catch {
                handle.err_ctx.setError(err.ZP_ERROR_INVALID_DATA, "buffer too large for u32");
                return handle.err_ctx.code;
            };
            return ZP_OK;
        },
        .callback => {
            handle.err_ctx.setError(err.ZP_ERROR_INVALID_STATE, "get_buffer only valid for memory backend");
            return handle.err_ctx.code;
        },
    }
}

export fn zp_row_writer_error_code(handle_id: i32) i32 {
    const handle = getRowWriterHandle(handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    return handle.err_ctx.code;
}

export fn zp_row_writer_error_message(handle_id: i32) [*:0]const u8 {
    const handle = getRowWriterHandle(handle_id) orelse return "";
    return handle.err_ctx.message();
}

export fn zp_row_writer_free(handle_id: i32) void {
    const handle = getRowWriterHandle(handle_id) orelse return;
    handle.deinit();
    freeHandleSlot(handle_id);
}

// ============================================================================
// Arrow Stream exports (handle-based iteration for freestanding)
// ============================================================================

export fn zp_reader_stream_init(reader_handle_id: i32) i32 {
    const reader = getReaderHandle(reader_handle_id) orelse return -err.ZP_ERROR_INVALID_ARGUMENT;
    const slot = allocHandleSlot();
    if (slot < 0) return slot;

    const sh = backing_allocator.create(StreamHandle) catch return -err.ZP_ERROR_OUT_OF_MEMORY;
    sh.* = .{ .reader = reader, .rg_index = 0 };

    const idx = safe.castTo(usize, slot) catch unreachable; // allocHandleSlot returns valid index
    handle_table[idx] = .{ .stream = sh };
    return slot;
}

export fn zp_reader_stream_get_schema(stream_handle_id: i32, schema_out: ?*ArrowSchema) i32 {
    const sh = getStreamHandle(stream_handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const out = schema_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const handle = sh.reader;

    const schema = arrow_batch.exportSchemaAsArrow(handle.allocator, handle.metadata) catch |e| {
        handle.err_ctx.setError(err.mapError(e), err.errorMessage(e));
        return handle.err_ctx.code;
    };
    out.* = schema;
    return ZP_OK;
}

export fn zp_reader_stream_get_next(stream_handle_id: i32, array_out: ?*ArrowArray, schema_out: ?*ArrowSchema) i32 {
    const sh = getStreamHandle(stream_handle_id) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const a_out = array_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const s_out = schema_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    const handle = sh.reader;

    if (sh.rg_index >= handle.metadata.row_groups.len) {
        return err.ZP_ROW_END;
    }

    var result = arrow_batch.readRowGroupAsArrow(
        handle.allocator,
        handle.source,
        handle.metadata,
        sh.rg_index,
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

    a_out.* = wrapper.array;
    s_out.* = result.schema;
    handle.allocator.free(result.arrays);
    sh.rg_index += 1;
    return ZP_OK;
}

export fn zp_reader_stream_close(stream_handle_id: i32) void {
    const sh = getStreamHandle(stream_handle_id) orelse return;
    backing_allocator.destroy(sh);
    freeHandleSlot(stream_handle_id);
}

fn toUsize(v: i32) ?usize {
    if (v < 0) return null;
    return safe.castTo(usize, v) catch unreachable; // guarded by v < 0 check above
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
// Arrow struct accessors
//
// These let the host read Arrow C Data Interface structs without knowing
// the exact byte layout, which may vary across compiler versions.
// ============================================================================

export fn zp_arrow_array_get_length(arr: ?*const ArrowArray) i64 {
    return if (arr) |a| a.length else 0;
}

export fn zp_arrow_array_get_null_count(arr: ?*const ArrowArray) i64 {
    return if (arr) |a| a.null_count else 0;
}

export fn zp_arrow_array_get_n_children(arr: ?*const ArrowArray) i64 {
    return if (arr) |a| a.n_children else 0;
}

export fn zp_arrow_array_get_n_buffers(arr: ?*const ArrowArray) i64 {
    return if (arr) |a| a.n_buffers else 0;
}

export fn zp_arrow_array_get_child(arr: ?*const ArrowArray, index: i32) ?*ArrowArray {
    const a = arr orelse return null;
    const i = safe.castTo(usize, index) catch return null;
    const nc = safe.castTo(usize, a.n_children) catch return null;
    if (i >= nc) return null;
    const children = a.children orelse return null;
    return children[i];
}

export fn zp_arrow_array_get_buffer(arr: ?*const ArrowArray, index: i32) ?*const anyopaque {
    const a = arr orelse return null;
    const i = safe.castTo(usize, index) catch return null;
    const nb = safe.castTo(usize, a.n_buffers) catch return null;
    if (i >= nb) return null;
    return a.buffers[i];
}

export fn zp_arrow_schema_get_format(sch: ?*const ArrowSchema) ?[*:0]const u8 {
    const s = sch orelse return null;
    return s.format;
}

export fn zp_arrow_schema_get_name(sch: ?*const ArrowSchema) ?[*:0]const u8 {
    const s = sch orelse return null;
    return s.name;
}

export fn zp_arrow_schema_get_n_children(sch: ?*const ArrowSchema) i64 {
    return if (sch) |s| s.n_children else 0;
}

export fn zp_arrow_schema_get_child(sch: ?*const ArrowSchema, index: i32) ?*ArrowSchema {
    const s = sch orelse return null;
    const i = safe.castTo(usize, index) catch return null;
    const nc = safe.castTo(usize, s.n_children) catch return null;
    if (i >= nc) return null;
    const children = s.children orelse return null;
    return children[i];
}

fn fsGetStats(handle: *ReaderHandle, col_index: i32, rg_index: i32) ?format.Statistics {
    const rg_idx = safe.castTo(usize, rg_index) catch return null;
    const col_idx = safe.castTo(usize, col_index) catch return null;
    if (rg_idx >= handle.metadata.row_groups.len) return null;
    const rg = handle.metadata.row_groups[rg_idx];
    if (col_idx >= rg.columns.len) return null;
    const meta = rg.columns[col_idx].meta_data orelse return null;
    return meta.statistics;
}
