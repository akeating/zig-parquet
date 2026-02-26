//! C ABI writer entry points.
//!
//! All functions return `c_int` status codes (ZP_OK on success).
//! Error details are available via `zp_writer_error_message` / `zp_writer_error_code`.

const std = @import("std");
const builtin = @import("builtin");
const err = @import("error.zig");
const handles = @import("handles.zig");
const arrow_batch = @import("../../core/arrow_batch.zig");
const format = @import("../../core/format.zig");
const safe = @import("../../core/safe.zig");

const is_wasm = builtin.cpu.arch == .wasm32 or builtin.cpu.arch == .wasm64;

const ArrowSchema = handles.ArrowSchema;
const ArrowArray = handles.ArrowArray;
const WriterHandle = handles.WriterHandle;

const ZP_OK = err.ZP_OK;

/// Open a Parquet writer that writes to an in-memory buffer.
///
/// After writing, use `zp_writer_get_buffer` to retrieve the buffer contents.
pub export fn zp_writer_open_memory(
    handle_out: ?*?*anyopaque,
) callconv(.c) c_int {
    const out = handle_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    out.* = null;

    const handle = WriterHandle.openMemory() catch |e| {
        return err.mapError(e);
    };
    out.* = @ptrCast(handle);
    return ZP_OK;
}

/// Open a Parquet writer using caller-provided write callbacks.
///
/// The callback context and function pointers must remain valid for the
/// lifetime of the writer handle.
pub export fn zp_writer_open_callbacks(
    ctx: ?*anyopaque,
    write_fn: ?*const fn (?*anyopaque, [*]const u8, usize) callconv(.c) c_int,
    close_fn: ?*const fn (?*anyopaque) callconv(.c) c_int,
    handle_out: ?*?*anyopaque,
) callconv(.c) c_int {
    const out = handle_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    out.* = null;
    const w_fn = write_fn orelse return err.ZP_ERROR_INVALID_ARGUMENT;

    const handle = WriterHandle.openCallbacks(ctx, w_fn, close_fn) catch |e| {
        return err.mapError(e);
    };
    out.* = @ptrCast(handle);
    return ZP_OK;
}

/// Open a Parquet writer to a file path (non-WASM only).
pub export fn zp_writer_open_file(
    path: ?[*:0]const u8,
    handle_out: ?*?*anyopaque,
) callconv(.c) c_int {
    if (is_wasm) return err.ZP_ERROR_UNSUPPORTED;
    const out = handle_out orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    out.* = null;
    const p = path orelse return err.ZP_ERROR_INVALID_ARGUMENT;

    const handle = WriterHandle.openFile(p) catch |e| {
        return err.mapError(e);
    };
    out.* = @ptrCast(handle);
    return ZP_OK;
}

/// Set the output schema from an ArrowSchema.
///
/// Must be called exactly once, before any `zp_writer_write_row_group` calls.
/// The ArrowSchema is consumed (read) but not retained -- the caller may
/// release it after this call returns.
pub export fn zp_writer_set_schema(
    handle_ptr: ?*anyopaque,
    schema: ?*const ArrowSchema,
) callconv(.c) c_int {
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

/// Set the compression codec for a specific column.
///
/// Must be called after `zp_writer_set_schema` and before writing any data.
/// `codec` values match Parquet's CompressionCodec enum:
///   0=UNCOMPRESSED, 1=SNAPPY, 2=GZIP, 4=BROTLI, 6=ZSTD, 7=LZ4_RAW
pub export fn zp_writer_set_column_codec(
    handle_ptr: ?*anyopaque,
    col_index: c_int,
    codec: c_int,
) callconv(.c) c_int {
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

    const codec_i32 = safe.castTo(i32, codec) catch {
        handle.err_ctx.setErrorFmt(err.ZP_ERROR_INVALID_ARGUMENT, "invalid codec value {d}", .{codec});
        return handle.err_ctx.code;
    };
    const compression = format.CompressionCodec.fromInt(codec_i32) catch {
        handle.err_ctx.setErrorFmt(err.ZP_ERROR_INVALID_ARGUMENT, "invalid codec value {d}", .{codec});
        return handle.err_ctx.code;
    };
    defs[idx].codec = compression;
    return ZP_OK;
}

/// Set the target row group size in bytes (advisory).
///
/// Must be called before writing any data.
/// The library may write slightly larger row groups.
pub export fn zp_writer_set_row_group_size(
    handle_ptr: ?*anyopaque,
    size_bytes: i64,
) callconv(.c) c_int {
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

/// Write a row group from Arrow arrays.
///
/// The `batch` ArrowArray must be a struct array whose children are the
/// column arrays. `schema` describes the batch structure and must match
/// the schema set via `zp_writer_set_schema`.
///
/// This is all-or-nothing: data errors are detected before writing;
/// transport errors may leave the output stream corrupted.
pub export fn zp_writer_write_row_group(
    handle_ptr: ?*anyopaque,
    batch: ?*const ArrowArray,
    schema: ?*const ArrowSchema,
) callconv(.c) c_int {
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

    // Extract child arrays and schemas from the struct batch
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

    // Build slices of the dereferenced children
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

/// Retrieve the in-memory buffer contents after writing (memory backend only).
///
/// The returned pointer is valid until the writer handle is closed.
/// Returns ZP_ERROR_INVALID_STATE for callback-backed writers.
pub export fn zp_writer_get_buffer(
    handle_ptr: ?*anyopaque,
    data_out: ?*[*]const u8,
    len_out: ?*usize,
) callconv(.c) c_int {
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
        .callback, .file => {
            handle.err_ctx.setError(err.ZP_ERROR_INVALID_STATE, "get_buffer only available for memory writers");
            return handle.err_ctx.code;
        },
    }
}

/// Get the most recent error code for this handle.
pub export fn zp_writer_error_code(handle_ptr: ?*anyopaque) callconv(.c) c_int {
    const handle = castHandle(WriterHandle, handle_ptr) orelse return err.ZP_ERROR_INVALID_ARGUMENT;
    return handle.err_ctx.code;
}

/// Get the most recent error message for this handle.
pub export fn zp_writer_error_message(handle_ptr: ?*anyopaque) callconv(.c) [*:0]const u8 {
    const handle = castHandle(WriterHandle, handle_ptr) orelse return "";
    return handle.err_ctx.message();
}

/// Finalize the Parquet footer and close the internal writer.
///
/// After this call, `zp_writer_get_buffer` returns the complete Parquet file
/// for memory-backed writers. The handle must still be freed with
/// `zp_writer_free`.
pub export fn zp_writer_close(handle_ptr: ?*anyopaque) callconv(.c) c_int {
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

/// Free all resources associated with the writer handle.
///
/// Must be called after `zp_writer_close`. For memory-backed writers,
/// retrieve the buffer with `zp_writer_get_buffer` before calling this.
pub export fn zp_writer_free(handle_ptr: ?*anyopaque) callconv(.c) void {
    const handle = castHandle(WriterHandle, handle_ptr) orelse return;
    handle.deinit();
}

fn castHandle(comptime T: type, ptr: ?*anyopaque) ?*T {
    return if (ptr) |p| @ptrCast(@alignCast(p)) else null;
}
