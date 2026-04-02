//! C ABI integration tests.
//!
//! Exercises the C ABI reader/writer functions from Zig, verifying:
//! - Open from memory, read schema, read row groups, close
//! - Write round-trip (write via C ABI, read back, verify)
//! - Error codes for invalid input
//! - Handle-state-after-error semantics

const std = @import("std");
const build_options = @import("build_options");
const parquet = @import("../lib.zig");

const c_err = parquet.c_api.err;
const c_reader = parquet.c_api.reader;
const c_writer = parquet.c_api.writer;
const c_handles = parquet.c_api.handles;

const ArrowSchema = parquet.ArrowSchema;
const ArrowArray = parquet.ArrowArray;

// ============================================================================
// Helpers
// ============================================================================

/// Create a simple 2-column Parquet buffer using the Zig API.
/// Columns: "id" (int32), "name" (byte_array/string).
fn createTestParquetBuffer(allocator: std.mem.Allocator) ![]u8 {
    const columns = [_]parquet.ColumnDef{
        .{ .name = "id", .type_ = .int32, .optional = false },
        parquet.ColumnDef.string("name", true),
    };

    var writer = try parquet.writeToBuffer(allocator, &columns);
    defer writer.deinit();

    const ids = [_]i32{ 1, 2, 3, 4, 5 };
    try writer.writeColumn(i32, 0, &ids);

    const names = [_]parquet.Optional([]const u8){
        .{ .value = "alice" },
        .{ .value = "bob" },
        .{ .value = "charlie" },
        .null_value,
        .{ .value = "eve" },
    };
    try writer.writeColumnOptional([]const u8, 1, &names);
    try writer.close();
    return try writer.toOwnedSlice();
}

// ============================================================================
// Reader tests
// ============================================================================

test "C ABI: open from memory and get schema" {
    const allocator = std.testing.allocator;
    const buf = try createTestParquetBuffer(allocator);
    defer allocator.free(buf);

    var handle: ?*anyopaque = null;
    const rc = c_reader.zp_reader_open_memory(buf.ptr, buf.len, &handle);
    try std.testing.expectEqual(c_err.ZP_OK, rc);
    try std.testing.expect(handle != null);
    defer c_reader.zp_reader_close(handle);

    // Row group count (output-parameter pattern)
    var num_rg: c_int = 0;
    try std.testing.expectEqual(c_err.ZP_OK, c_reader.zp_reader_get_num_row_groups(handle, &num_rg));
    try std.testing.expectEqual(@as(c_int, 1), num_rg);

    // Schema
    var schema: ArrowSchema = undefined;
    const src = c_reader.zp_reader_get_schema(handle, &schema);
    try std.testing.expectEqual(c_err.ZP_OK, src);
    defer schema.doRelease();

    try std.testing.expectEqual(@as(i64, 2), schema.n_children);
    const fmt = std.mem.sliceTo(schema.format, 0);
    try std.testing.expectEqualStrings("+s", fmt);

    // Check child names
    const children: [*]*ArrowSchema = schema.children.?;
    const name0 = std.mem.sliceTo(children[0].name.?, 0);
    try std.testing.expectEqualStrings("id", name0);
    const name1 = std.mem.sliceTo(children[1].name.?, 0);
    try std.testing.expectEqualStrings("name", name1);
}

test "C ABI: read row group as arrow" {
    const allocator = std.testing.allocator;
    const buf = try createTestParquetBuffer(allocator);
    defer allocator.free(buf);

    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_reader.zp_reader_open_memory(buf.ptr, buf.len, &handle));
    defer c_reader.zp_reader_close(handle);

    var arr: ArrowArray = undefined;
    var schema: ArrowSchema = undefined;
    const rc = c_reader.zp_reader_read_row_group(handle, 0, null, 0, &arr, &schema);
    try std.testing.expectEqual(c_err.ZP_OK, rc);
    defer arr.doRelease();
    defer schema.doRelease();

    // Top-level struct has 2 children (one per column)
    try std.testing.expectEqual(@as(i64, 2), arr.n_children);
    try std.testing.expectEqual(@as(i64, 5), arr.length);

    // Verify int32 column values
    const id_arr: *ArrowArray = arr.children.?[0];
    try std.testing.expectEqual(@as(i64, 5), id_arr.length);
    const id_data: [*]const i32 = @ptrCast(@alignCast(id_arr.buffers[1].?));
    try std.testing.expectEqual(@as(i32, 1), id_data[0]);
    try std.testing.expectEqual(@as(i32, 5), id_data[4]);
}

test "C ABI: read specific columns" {
    const allocator = std.testing.allocator;
    const buf = try createTestParquetBuffer(allocator);
    defer allocator.free(buf);

    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_reader.zp_reader_open_memory(buf.ptr, buf.len, &handle));
    defer c_reader.zp_reader_close(handle);

    // Read only column 0 (id)
    const cols = [_]c_int{0};
    var arr: ArrowArray = undefined;
    var schema: ArrowSchema = undefined;
    const rc = c_reader.zp_reader_read_row_group(handle, 0, &cols, 1, &arr, &schema);
    try std.testing.expectEqual(c_err.ZP_OK, rc);
    defer arr.doRelease();
    defer schema.doRelease();

    try std.testing.expectEqual(@as(i64, 1), arr.n_children);
}

// ============================================================================
// Writer tests
// ============================================================================

test "C ABI: write and read round-trip" {
    const allocator = std.testing.allocator;

    // 1. Create source data using the Zig API
    const src_buf = try createTestParquetBuffer(allocator);
    defer allocator.free(src_buf);

    // 2. Read source data as Arrow using the Zig batch API
    var src_reader = try parquet.openBufferDynamic(allocator, src_buf, .{});
    defer src_reader.deinit();
    var read_result = try parquet.readRowGroupAsArrow(
        allocator,
        src_reader.getSource(),
        src_reader.metadata,
        0,
        null,
    );
    defer read_result.deinit();

    // 3. Write via C ABI writer
    var w_handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_writer.zp_writer_open_memory(&w_handle));

    // Set schema
    const set_rc = c_writer.zp_writer_set_schema(w_handle, &read_result.schema);
    try std.testing.expectEqual(c_err.ZP_OK, set_rc);

    // Build a struct batch from the column arrays
    // We need to create a wrapper struct ArrowArray for the batch
    const num_cols = read_result.arrays.len;
    var child_ptrs = try allocator.alloc(*ArrowArray, num_cols);
    defer allocator.free(child_ptrs);
    for (read_result.arrays, 0..) |*a, i| {
        child_ptrs[i] = a;
    }
    var null_buf: [1]?*anyopaque = .{null};
    var batch_arr = ArrowArray{
        .length = read_result.arrays[0].length,
        .null_count = 0,
        .offset = 0,
        .n_buffers = 1,
        .n_children = @intCast(num_cols),
        .buffers = &null_buf,
        .children = @ptrCast(child_ptrs.ptr),
        .dictionary = null,
        .release = null,
        .private_data = null,
    };

    const write_rc = c_writer.zp_writer_write_row_group(w_handle, &batch_arr, &read_result.schema);
    try std.testing.expectEqual(c_err.ZP_OK, write_rc);

    // Close finalizes footer
    try std.testing.expectEqual(c_err.ZP_OK, c_writer.zp_writer_close(w_handle));

    // Get buffer after close (data is complete now)
    var out_data: [*]const u8 = undefined;
    var out_len: usize = 0;
    try std.testing.expectEqual(c_err.ZP_OK, c_writer.zp_writer_get_buffer(w_handle, &out_data, &out_len));
    try std.testing.expect(out_len > 0);

    // Copy buffer before free
    const written = try allocator.dupe(u8, out_data[0..out_len]);
    defer allocator.free(written);

    // Free handle resources
    c_writer.zp_writer_free(w_handle);

    // 4. Read back via C ABI reader and verify
    var r_handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_reader.zp_reader_open_memory(written.ptr, written.len, &r_handle));
    defer c_reader.zp_reader_close(r_handle);

    var rg_count: c_int = 0;
    try std.testing.expectEqual(c_err.ZP_OK, c_reader.zp_reader_get_num_row_groups(r_handle, &rg_count));
    try std.testing.expectEqual(@as(c_int, 1), rg_count);

    var result_arr: ArrowArray = undefined;
    var result_schema: ArrowSchema = undefined;
    try std.testing.expectEqual(c_err.ZP_OK, c_reader.zp_reader_read_row_group(r_handle, 0, null, 0, &result_arr, &result_schema));
    defer result_arr.doRelease();
    defer result_schema.doRelease();

    try std.testing.expectEqual(@as(i64, 5), result_arr.length);
    try std.testing.expectEqual(@as(i64, 2), result_arr.n_children);

    // Verify int32 values survived round-trip
    const id_arr: *ArrowArray = result_arr.children.?[0];
    const id_data: [*]const i32 = @ptrCast(@alignCast(id_arr.buffers[1].?));
    try std.testing.expectEqual(@as(i32, 1), id_data[0]);
    try std.testing.expectEqual(@as(i32, 2), id_data[1]);
    try std.testing.expectEqual(@as(i32, 3), id_data[2]);
    try std.testing.expectEqual(@as(i32, 4), id_data[3]);
    try std.testing.expectEqual(@as(i32, 5), id_data[4]);
}

// ============================================================================
// Error handling tests
// ============================================================================

test "C ABI: open memory with bad magic returns NOT_PARQUET" {
    const bad_data = "This is not a parquet file at all!";
    var handle: ?*anyopaque = null;
    const rc = c_reader.zp_reader_open_memory(bad_data.ptr, bad_data.len, &handle);
    try std.testing.expectEqual(c_err.ZP_ERROR_NOT_PARQUET, rc);
    try std.testing.expect(handle == null);
}

test "C ABI: open memory with null pointer returns INVALID_ARGUMENT" {
    var handle: ?*anyopaque = null;
    const rc = c_reader.zp_reader_open_memory(null, 0, &handle);
    try std.testing.expectEqual(c_err.ZP_ERROR_INVALID_ARGUMENT, rc);
}

test "C ABI: null handle_out returns INVALID_ARGUMENT" {
    const data = "PAR1";
    const rc = c_reader.zp_reader_open_memory(data.ptr, data.len, null);
    try std.testing.expectEqual(c_err.ZP_ERROR_INVALID_ARGUMENT, rc);
}

test "C ABI: read_row_group with bad index returns error" {
    const allocator = std.testing.allocator;
    const buf = try createTestParquetBuffer(allocator);
    defer allocator.free(buf);

    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_reader.zp_reader_open_memory(buf.ptr, buf.len, &handle));
    defer c_reader.zp_reader_close(handle);

    var arr: ArrowArray = undefined;
    var schema: ArrowSchema = undefined;
    const rc = c_reader.zp_reader_read_row_group(handle, 99, null, 0, &arr, &schema);
    try std.testing.expect(rc != c_err.ZP_OK);

    // Reader handle should still be usable after the error
    const msg = c_reader.zp_reader_error_message(handle);
    try std.testing.expect(std.mem.sliceTo(msg, 0).len > 0);

    // Can still read valid row group
    var arr2: ArrowArray = undefined;
    var schema2: ArrowSchema = undefined;
    const rc2 = c_reader.zp_reader_read_row_group(handle, 0, null, 0, &arr2, &schema2);
    try std.testing.expectEqual(c_err.ZP_OK, rc2);
    defer arr2.doRelease();
    defer schema2.doRelease();
}

test "C ABI: negative row group index returns error" {
    const allocator = std.testing.allocator;
    const buf = try createTestParquetBuffer(allocator);
    defer allocator.free(buf);

    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_reader.zp_reader_open_memory(buf.ptr, buf.len, &handle));
    defer c_reader.zp_reader_close(handle);

    var arr: ArrowArray = undefined;
    var schema: ArrowSchema = undefined;
    const rc = c_reader.zp_reader_read_row_group(handle, -1, null, 0, &arr, &schema);
    try std.testing.expectEqual(c_err.ZP_ERROR_INVALID_ARGUMENT, rc);
}

test "C ABI: writer set_schema twice returns INVALID_STATE" {
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_writer.zp_writer_open_memory(&handle));

    // Build a minimal schema
    const child_fmt: [:0]const u8 = "i";
    const child_name: [:0]const u8 = "x";
    var child_schema = ArrowSchema{
        .format = child_fmt.ptr,
        .name = child_name.ptr,
        .metadata = null,
        .flags = 0,
        .n_children = 0,
        .children = null,
        .dictionary = null,
        .release = null,
        .private_data = null,
    };
    var children_arr = [_]*ArrowSchema{&child_schema};
    const root_fmt: [:0]const u8 = "+s";
    var root_schema = ArrowSchema{
        .format = root_fmt.ptr,
        .name = null,
        .metadata = null,
        .flags = 0,
        .n_children = 1,
        .children = @ptrCast(&children_arr),
        .dictionary = null,
        .release = null,
        .private_data = null,
    };

    try std.testing.expectEqual(c_err.ZP_OK, c_writer.zp_writer_set_schema(handle, &root_schema));

    // Second set_schema should fail
    const rc2 = c_writer.zp_writer_set_schema(handle, &root_schema);
    try std.testing.expectEqual(c_err.ZP_ERROR_INVALID_STATE, rc2);

    // No data was written so close would fail (all columns must be written).
    // Just free the handle directly.
    c_writer.zp_writer_free(handle);
}

test "C ABI: writer write_row_group before set_schema returns INVALID_STATE" {
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_writer.zp_writer_open_memory(&handle));

    var null_buf: [1]?*anyopaque = .{null};
    var batch = ArrowArray{
        .length = 0,
        .null_count = 0,
        .offset = 0,
        .n_buffers = 1,
        .n_children = 0,
        .buffers = &null_buf,
        .children = null,
        .dictionary = null,
        .release = null,
        .private_data = null,
    };
    const fmt: [:0]const u8 = "+s";
    var schema = ArrowSchema{
        .format = fmt.ptr,
        .name = null,
        .metadata = null,
        .flags = 0,
        .n_children = 0,
        .children = null,
        .dictionary = null,
        .release = null,
        .private_data = null,
    };

    const rc = c_writer.zp_writer_write_row_group(handle, &batch, &schema);
    try std.testing.expectEqual(c_err.ZP_ERROR_INVALID_STATE, rc);

    // No close needed since schema was never set (no writer to finalize)
    c_writer.zp_writer_free(handle);
}

test "C ABI: error code mapping" {
    try std.testing.expectEqual(c_err.ZP_ERROR_NOT_PARQUET, c_err.mapError(error.InvalidMagic));
    try std.testing.expectEqual(c_err.ZP_ERROR_NOT_PARQUET, c_err.mapError(error.FileTooSmall));
    try std.testing.expectEqual(c_err.ZP_ERROR_IO, c_err.mapError(error.InputOutput));
    try std.testing.expectEqual(c_err.ZP_ERROR_IO, c_err.mapError(error.WriteError));
    try std.testing.expectEqual(c_err.ZP_ERROR_OUT_OF_MEMORY, c_err.mapError(error.OutOfMemory));
    try std.testing.expectEqual(c_err.ZP_ERROR_UNSUPPORTED, c_err.mapError(error.UnsupportedCompression));
    try std.testing.expectEqual(c_err.ZP_ERROR_INVALID_ARGUMENT, c_err.mapError(error.InvalidColumnIndex));
    try std.testing.expectEqual(c_err.ZP_ERROR_INVALID_STATE, c_err.mapError(error.InvalidState));
    try std.testing.expectEqual(c_err.ZP_ERROR_CHECKSUM, c_err.mapError(error.PageChecksumMismatch));
    try std.testing.expectEqual(c_err.ZP_ERROR_SCHEMA, c_err.mapError(error.InvalidSchema));
    try std.testing.expectEqual(c_err.ZP_ERROR_INVALID_DATA, c_err.mapError(error.InvalidPageData));
}

// ============================================================================
// Callback transport tests
// ============================================================================

const CMemorySource = struct {
    data: []const u8,

    fn readAt(ctx: ?*anyopaque, offset: u64, buf: [*]u8, buf_len: usize, bytes_read: *usize) callconv(.c) c_int {
        const self: *CMemorySource = @ptrCast(@alignCast(ctx));
        if (offset >= self.data.len) {
            bytes_read.* = 0;
            return 0;
        }
        const start: usize = @intCast(offset);
        const len = @min(buf_len, self.data.len - start);
        @memcpy(buf[0..len], self.data[start..][0..len]);
        bytes_read.* = len;
        return 0;
    }

    fn size(ctx: ?*anyopaque) callconv(.c) u64 {
        const self: *CMemorySource = @ptrCast(@alignCast(ctx));
        return self.data.len;
    }
};

const CFailingWriter = struct {
    calls: usize = 0,
    fail_after: usize,

    fn write(ctx: ?*anyopaque, _: [*]const u8, _: usize) callconv(.c) c_int {
        const self: *CFailingWriter = @ptrCast(@alignCast(ctx));
        self.calls += 1;
        if (self.calls > self.fail_after) return -1;
        return 0;
    }

    fn close(_: ?*anyopaque) callconv(.c) c_int {
        return 0;
    }
};

test "C ABI: writer handle unusable after transport error" {
    const allocator = std.testing.allocator;

    // Read source data to get Arrow arrays for writing
    const src_buf = try createTestParquetBuffer(allocator);
    defer allocator.free(src_buf);

    var src_reader = try parquet.openBufferDynamic(allocator, src_buf, .{});
    defer src_reader.deinit();
    var read_result = try parquet.readRowGroupAsArrow(
        allocator,
        src_reader.getSource(),
        src_reader.metadata,
        0,
        null,
    );
    defer read_result.deinit();

    // Open writer with a callback that fails after a few writes
    var failing = CFailingWriter{ .fail_after = 2 };
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_writer.zp_writer_open_callbacks(
        @ptrCast(&failing),
        CFailingWriter.write,
        CFailingWriter.close,
        &handle,
    ));

    // Set schema (succeeds — initial magic write uses first call)
    try std.testing.expectEqual(c_err.ZP_OK, c_writer.zp_writer_set_schema(handle, &read_result.schema));

    // Build batch from column arrays
    const num_cols = read_result.arrays.len;
    var child_ptrs = try allocator.alloc(*ArrowArray, num_cols);
    defer allocator.free(child_ptrs);
    for (read_result.arrays, 0..) |*a, i| {
        child_ptrs[i] = a;
    }
    var null_buf: [1]?*anyopaque = .{null};
    var batch_arr = ArrowArray{
        .length = read_result.arrays[0].length,
        .null_count = 0,
        .offset = 0,
        .n_buffers = 1,
        .n_children = @intCast(num_cols),
        .buffers = &null_buf,
        .children = @ptrCast(child_ptrs.ptr),
        .dictionary = null,
        .release = null,
        .private_data = null,
    };

    // Write row group — should fail due to transport error
    const write_rc = c_writer.zp_writer_write_row_group(handle, &batch_arr, &read_result.schema);
    try std.testing.expectEqual(c_err.ZP_ERROR_IO, write_rc);

    // Subsequent operations should fail with INVALID_STATE (handle poisoned)
    const write_rc2 = c_writer.zp_writer_write_row_group(handle, &batch_arr, &read_result.schema);
    try std.testing.expectEqual(c_err.ZP_ERROR_INVALID_STATE, write_rc2);

    const close_rc = c_writer.zp_writer_close(handle);
    try std.testing.expectEqual(c_err.ZP_ERROR_INVALID_STATE, close_rc);

    // Error message should indicate transport failure
    const msg = c_writer.zp_writer_error_message(handle);
    try std.testing.expect(std.mem.indexOf(u8, std.mem.sliceTo(msg, 0), "transport") != null);

    c_writer.zp_writer_free(handle);
}

test "C ABI: open from callbacks" {
    const allocator = std.testing.allocator;
    const buf = try createTestParquetBuffer(allocator);
    defer allocator.free(buf);

    var source = CMemorySource{ .data = buf };
    var handle: ?*anyopaque = null;
    const rc = c_reader.zp_reader_open_callbacks(
        @ptrCast(&source),
        CMemorySource.readAt,
        CMemorySource.size,
        &handle,
    );
    try std.testing.expectEqual(c_err.ZP_OK, rc);
    defer c_reader.zp_reader_close(handle);

    var cb_rg_count: c_int = 0;
    try std.testing.expectEqual(c_err.ZP_OK, c_reader.zp_reader_get_num_row_groups(handle, &cb_rg_count));
    try std.testing.expectEqual(@as(c_int, 1), cb_rg_count);

    var arr: ArrowArray = undefined;
    var schema: ArrowSchema = undefined;
    try std.testing.expectEqual(c_err.ZP_OK, c_reader.zp_reader_read_row_group(handle, 0, null, 0, &arr, &schema));
    defer arr.doRelease();
    defer schema.doRelease();

    try std.testing.expectEqual(@as(i64, 5), arr.length);
}

// ============================================================================
// Metadata query tests
// ============================================================================

test "C ABI: metadata queries" {
    const allocator = std.testing.allocator;
    const buf = try createTestParquetBuffer(allocator);
    defer allocator.free(buf);

    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_reader.zp_reader_open_memory(buf.ptr, buf.len, &handle));
    defer c_reader.zp_reader_close(handle);

    // Total row count
    var total_rows: i64 = 0;
    try std.testing.expectEqual(c_err.ZP_OK, c_reader.zp_reader_get_num_rows(handle, &total_rows));
    try std.testing.expectEqual(@as(i64, 5), total_rows);

    // Row group row count
    var rg_rows: i64 = 0;
    try std.testing.expectEqual(c_err.ZP_OK, c_reader.zp_reader_get_row_group_num_rows(handle, 0, &rg_rows));
    try std.testing.expectEqual(@as(i64, 5), rg_rows);

    // Invalid row group index
    try std.testing.expect(c_reader.zp_reader_get_row_group_num_rows(handle, 99, &rg_rows) != c_err.ZP_OK);
    try std.testing.expect(c_reader.zp_reader_get_row_group_num_rows(handle, -1, &rg_rows) != c_err.ZP_OK);

    // Column count
    var col_count: c_int = 0;
    try std.testing.expectEqual(c_err.ZP_OK, c_reader.zp_reader_get_column_count(handle, &col_count));
    try std.testing.expectEqual(@as(c_int, 2), col_count);
}

// ============================================================================
// Introspection tests
// ============================================================================

test "C ABI: version and codec support" {
    const c_introspect = parquet.c_api.introspect;

    // Version returns a non-empty string
    const version = std.mem.sliceTo(c_introspect.getVersion(), 0);
    try std.testing.expect(version.len > 0);

    // Uncompressed is always supported
    try std.testing.expectEqual(@as(c_int, 1), c_introspect.isCodecSupported(0));

    // Invalid codec is not supported
    try std.testing.expectEqual(@as(c_int, 0), c_introspect.isCodecSupported(99));
}

// ============================================================================
// Writer options tests
// ============================================================================

test "C ABI: writer set_row_group_size" {
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_writer.zp_writer_open_memory(&handle));

    // Valid size
    try std.testing.expectEqual(c_err.ZP_OK, c_writer.zp_writer_set_row_group_size(handle, 1024 * 1024));

    // Invalid size (zero)
    try std.testing.expectEqual(c_err.ZP_ERROR_INVALID_ARGUMENT, c_writer.zp_writer_set_row_group_size(handle, 0));

    // Invalid size (negative)
    try std.testing.expectEqual(c_err.ZP_ERROR_INVALID_ARGUMENT, c_writer.zp_writer_set_row_group_size(handle, -1));

    c_writer.zp_writer_free(handle);
}

// ============================================================================
// Arrow C Stream Interface tests
// ============================================================================

const ArrowArrayStream = parquet.ArrowArrayStream;

test "C ABI: reader get_stream iterates row groups" {
    const allocator = std.testing.allocator;
    const buf = try createTestParquetBuffer(allocator);
    defer allocator.free(buf);

    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_reader.zp_reader_open_memory(buf.ptr, buf.len, &handle));
    defer c_reader.zp_reader_close(handle);

    var stream: ArrowArrayStream = undefined;
    try std.testing.expectEqual(c_err.ZP_OK, c_reader.zp_reader_get_stream(handle, &stream));
    defer stream.release.?(&stream);

    // get_schema should succeed
    var schema: ArrowSchema = undefined;
    try std.testing.expectEqual(@as(c_int, 0), stream.get_schema.?(&stream, &schema));
    defer schema.doRelease();
    try std.testing.expectEqual(@as(i64, 2), schema.n_children);

    // get_next should return one batch (one row group)
    var batch: ArrowArray = undefined;
    try std.testing.expectEqual(@as(c_int, 0), stream.get_next.?(&stream, &batch));
    try std.testing.expect(batch.release != null);
    try std.testing.expectEqual(@as(i64, 5), batch.length);
    batch.doRelease();

    // Second get_next should signal end-of-stream (release == null)
    var end: ArrowArray = undefined;
    try std.testing.expectEqual(@as(c_int, 0), stream.get_next.?(&stream, &end));
    try std.testing.expect(end.release == null);
}

test "C ABI: stream get_last_error returns null when no error" {
    const allocator = std.testing.allocator;
    const buf = try createTestParquetBuffer(allocator);
    defer allocator.free(buf);

    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_reader.zp_reader_open_memory(buf.ptr, buf.len, &handle));
    defer c_reader.zp_reader_close(handle);

    var stream: ArrowArrayStream = undefined;
    try std.testing.expectEqual(c_err.ZP_OK, c_reader.zp_reader_get_stream(handle, &stream));
    defer stream.release.?(&stream);

    try std.testing.expectEqual(@as(?[*:0]const u8, null), stream.get_last_error.?(&stream));
}

// ============================================================================
// Nested type roundtrip tests (row writer -> row reader)
// ============================================================================

const c_row_reader = parquet.c_api.row_reader;
const c_row_writer = parquet.c_api.row_writer;

test "C ABI nested: list<int32> roundtrip" {
    // Schema: {id: int32, scores: list<int32>}
    var w_handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_open_memory(&w_handle));

    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_column(w_handle, "id", c_err.ZP_TYPE_INT32));

    const int32_schema = c_row_writer.zp_schema_primitive(w_handle, c_err.ZP_TYPE_INT32);
    try std.testing.expect(int32_schema != null);
    const list_schema = c_row_writer.zp_schema_list(w_handle, int32_schema);
    try std.testing.expect(list_schema != null);
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_column_nested(w_handle, "scores", list_schema));

    // begin() is called once to initialize the writer
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_begin(w_handle));

    // Row 1: id=1, scores=[10, 20, 30]
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_int32(w_handle, 0, 1));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_begin_list(w_handle, 1));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_append_int32(w_handle, 1, 10));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_append_int32(w_handle, 1, 20));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_append_int32(w_handle, 1, 30));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_end_list(w_handle, 1));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_row(w_handle));

    // Row 2: id=2, scores=[40]
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_int32(w_handle, 0, 2));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_begin_list(w_handle, 1));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_append_int32(w_handle, 1, 40));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_end_list(w_handle, 1));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_row(w_handle));

    // Row 3: id=3, scores=[] (empty list)
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_int32(w_handle, 0, 3));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_begin_list(w_handle, 1));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_end_list(w_handle, 1));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_row(w_handle));

    // Flush, close, get buffer
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_flush(w_handle));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_close(w_handle));

    var out_data: [*]const u8 = undefined;
    var out_len: usize = 0;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_get_buffer(w_handle, &out_data, &out_len));
    try std.testing.expect(out_len > 0);

    const allocator = std.testing.allocator;
    const written = try allocator.dupe(u8, out_data[0..out_len]);
    defer allocator.free(written);
    c_row_writer.zp_row_writer_free(w_handle);

    // Read back
    var r_handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_open_memory(written.ptr, written.len, &r_handle));
    defer c_row_reader.zp_row_reader_close(r_handle);

    var col_count: c_int = 0;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_get_column_count(r_handle, &col_count));
    try std.testing.expectEqual(@as(c_int, 2), col_count);

    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_read_row_group(r_handle, 0));

    // Row 1
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_next(r_handle));
    try std.testing.expectEqual(@as(i32, 1), c_row_reader.zp_row_reader_get_int32(r_handle, 0));

    const scores_val = c_row_reader.zp_row_reader_get_value(r_handle, 1);
    try std.testing.expect(scores_val != null);
    try std.testing.expectEqual(c_err.ZP_TYPE_LIST, c_row_reader.zp_value_get_type(scores_val));
    try std.testing.expectEqual(@as(c_int, 3), c_row_reader.zp_value_get_list_len(scores_val));

    const elem0 = c_row_reader.zp_value_get_list_element(scores_val, 0);
    try std.testing.expect(elem0 != null);
    try std.testing.expectEqual(@as(i32, 10), c_row_reader.zp_value_get_int32(elem0));

    const elem1 = c_row_reader.zp_value_get_list_element(scores_val, 1);
    try std.testing.expectEqual(@as(i32, 20), c_row_reader.zp_value_get_int32(elem1));

    const elem2 = c_row_reader.zp_value_get_list_element(scores_val, 2);
    try std.testing.expectEqual(@as(i32, 30), c_row_reader.zp_value_get_int32(elem2));

    // Row 2
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_next(r_handle));
    try std.testing.expectEqual(@as(i32, 2), c_row_reader.zp_row_reader_get_int32(r_handle, 0));

    const scores2 = c_row_reader.zp_row_reader_get_value(r_handle, 1);
    try std.testing.expectEqual(@as(c_int, 1), c_row_reader.zp_value_get_list_len(scores2));
    const e2_0 = c_row_reader.zp_value_get_list_element(scores2, 0);
    try std.testing.expectEqual(@as(i32, 40), c_row_reader.zp_value_get_int32(e2_0));

    // Row 3 (empty list)
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_next(r_handle));
    try std.testing.expectEqual(@as(i32, 3), c_row_reader.zp_row_reader_get_int32(r_handle, 0));
    const scores3 = c_row_reader.zp_row_reader_get_value(r_handle, 1);
    try std.testing.expectEqual(@as(c_int, 0), c_row_reader.zp_value_get_list_len(scores3));

    // No more rows
    try std.testing.expectEqual(c_err.ZP_ROW_END, c_row_reader.zp_row_reader_next(r_handle));
}

test "C ABI nested: struct roundtrip" {
    // Schema: {id: int32, point: struct{x: int32, y: int32}}
    var w_handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_open_memory(&w_handle));

    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_column(w_handle, "id", c_err.ZP_TYPE_INT32));

    const int32_schema = c_row_writer.zp_schema_primitive(w_handle, c_err.ZP_TYPE_INT32);
    const field_names = [_][*:0]const u8{ "x", "y" };
    const field_schemas = [_]?*const anyopaque{ int32_schema, int32_schema };
    const struct_schema = c_row_writer.zp_schema_struct(w_handle, &field_names, &field_schemas, 2);
    try std.testing.expect(struct_schema != null);
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_column_nested(w_handle, "point", struct_schema));

    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_begin(w_handle));

    // Row 1: id=1, point={x:10, y:20}
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_int32(w_handle, 0, 1));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_begin_struct(w_handle, 1));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_field_int32(w_handle, 1, 0, 10));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_field_int32(w_handle, 1, 1, 20));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_end_struct(w_handle, 1));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_row(w_handle));

    // Row 2: id=2, point={x:30, y:40}
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_int32(w_handle, 0, 2));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_begin_struct(w_handle, 1));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_field_int32(w_handle, 1, 0, 30));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_field_int32(w_handle, 1, 1, 40));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_end_struct(w_handle, 1));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_row(w_handle));

    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_flush(w_handle));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_close(w_handle));

    var out_data: [*]const u8 = undefined;
    var out_len: usize = 0;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_get_buffer(w_handle, &out_data, &out_len));

    const allocator = std.testing.allocator;
    const written = try allocator.dupe(u8, out_data[0..out_len]);
    defer allocator.free(written);
    c_row_writer.zp_row_writer_free(w_handle);

    // Read back
    var r_handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_open_memory(written.ptr, written.len, &r_handle));
    defer c_row_reader.zp_row_reader_close(r_handle);

    var col_count: c_int = 0;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_get_column_count(r_handle, &col_count));
    try std.testing.expectEqual(@as(c_int, 2), col_count);

    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_read_row_group(r_handle, 0));

    // Row 1
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_next(r_handle));
    try std.testing.expectEqual(@as(i32, 1), c_row_reader.zp_row_reader_get_int32(r_handle, 0));

    const point_val = c_row_reader.zp_row_reader_get_value(r_handle, 1);
    try std.testing.expect(point_val != null);
    try std.testing.expectEqual(c_err.ZP_TYPE_STRUCT, c_row_reader.zp_value_get_type(point_val));
    try std.testing.expectEqual(@as(c_int, 2), c_row_reader.zp_value_get_struct_field_count(point_val));

    const fx = c_row_reader.zp_value_get_struct_field_value(point_val, 0);
    try std.testing.expect(fx != null);
    try std.testing.expectEqual(@as(i32, 10), c_row_reader.zp_value_get_int32(fx));

    const fy = c_row_reader.zp_value_get_struct_field_value(point_val, 1);
    try std.testing.expectEqual(@as(i32, 20), c_row_reader.zp_value_get_int32(fy));

    // Row 2
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_next(r_handle));
    try std.testing.expectEqual(@as(i32, 2), c_row_reader.zp_row_reader_get_int32(r_handle, 0));

    const point2 = c_row_reader.zp_row_reader_get_value(r_handle, 1);
    const fx2 = c_row_reader.zp_value_get_struct_field_value(point2, 0);
    try std.testing.expectEqual(@as(i32, 30), c_row_reader.zp_value_get_int32(fx2));
    const fy2 = c_row_reader.zp_value_get_struct_field_value(point2, 1);
    try std.testing.expectEqual(@as(i32, 40), c_row_reader.zp_value_get_int32(fy2));

    try std.testing.expectEqual(c_err.ZP_ROW_END, c_row_reader.zp_row_reader_next(r_handle));
}

test "C ABI nested: map<bytes, int32> roundtrip" {
    // Schema: {id: int32, props: map<bytes, int32>}
    var w_handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_open_memory(&w_handle));

    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_column(w_handle, "id", c_err.ZP_TYPE_INT32));

    const key_schema = c_row_writer.zp_schema_primitive(w_handle, c_err.ZP_TYPE_BYTES);
    const val_schema = c_row_writer.zp_schema_primitive(w_handle, c_err.ZP_TYPE_INT32);
    const map_schema = c_row_writer.zp_schema_map(w_handle, key_schema, val_schema);
    try std.testing.expect(map_schema != null);
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_column_nested(w_handle, "props", map_schema));

    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_begin(w_handle));

    // Row 1: id=1, props={"a":100, "b":200}
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_int32(w_handle, 0, 1));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_begin_map(w_handle, 1));

    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_begin_map_entry(w_handle, 1));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_append_bytes(w_handle, 1, "a", 1));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_append_int32(w_handle, 1, 100));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_end_map_entry(w_handle, 1));

    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_begin_map_entry(w_handle, 1));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_append_bytes(w_handle, 1, "b", 1));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_append_int32(w_handle, 1, 200));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_end_map_entry(w_handle, 1));

    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_end_map(w_handle, 1));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_row(w_handle));

    // Row 2: id=2, props={"c":300}
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_int32(w_handle, 0, 2));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_begin_map(w_handle, 1));

    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_begin_map_entry(w_handle, 1));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_append_bytes(w_handle, 1, "c", 1));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_append_int32(w_handle, 1, 300));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_end_map_entry(w_handle, 1));

    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_end_map(w_handle, 1));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_row(w_handle));

    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_flush(w_handle));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_close(w_handle));

    var out_data: [*]const u8 = undefined;
    var out_len: usize = 0;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_get_buffer(w_handle, &out_data, &out_len));

    const allocator = std.testing.allocator;
    const written = try allocator.dupe(u8, out_data[0..out_len]);
    defer allocator.free(written);
    c_row_writer.zp_row_writer_free(w_handle);

    // Read back
    var r_handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_open_memory(written.ptr, written.len, &r_handle));
    defer c_row_reader.zp_row_reader_close(r_handle);

    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_read_row_group(r_handle, 0));

    // Row 1
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_next(r_handle));
    try std.testing.expectEqual(@as(i32, 1), c_row_reader.zp_row_reader_get_int32(r_handle, 0));

    const map_val = c_row_reader.zp_row_reader_get_value(r_handle, 1);
    try std.testing.expect(map_val != null);
    try std.testing.expectEqual(c_err.ZP_TYPE_MAP, c_row_reader.zp_value_get_type(map_val));
    try std.testing.expectEqual(@as(c_int, 2), c_row_reader.zp_value_get_map_len(map_val));

    const key0 = c_row_reader.zp_value_get_map_key(map_val, 0);
    try std.testing.expect(key0 != null);
    var key0_data: [*]const u8 = undefined;
    var key0_len: usize = 0;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_value_get_bytes(key0, &key0_data, &key0_len));
    try std.testing.expectEqualStrings("a", key0_data[0..key0_len]);

    const val0 = c_row_reader.zp_value_get_map_value(map_val, 0);
    try std.testing.expectEqual(@as(i32, 100), c_row_reader.zp_value_get_int32(val0));

    const key1 = c_row_reader.zp_value_get_map_key(map_val, 1);
    var key1_data: [*]const u8 = undefined;
    var key1_len: usize = 0;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_value_get_bytes(key1, &key1_data, &key1_len));
    try std.testing.expectEqualStrings("b", key1_data[0..key1_len]);

    const val1 = c_row_reader.zp_value_get_map_value(map_val, 1);
    try std.testing.expectEqual(@as(i32, 200), c_row_reader.zp_value_get_int32(val1));

    // Row 2
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_next(r_handle));
    const map2 = c_row_reader.zp_row_reader_get_value(r_handle, 1);
    try std.testing.expectEqual(@as(c_int, 1), c_row_reader.zp_value_get_map_len(map2));

    try std.testing.expectEqual(c_err.ZP_ROW_END, c_row_reader.zp_row_reader_next(r_handle));
}

test "C ABI flat: all primitive types roundtrip" {
    var w_handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_open_memory(&w_handle));

    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_column(w_handle, "col_bool", c_err.ZP_TYPE_BOOL));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_column(w_handle, "col_int32", c_err.ZP_TYPE_INT32));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_column(w_handle, "col_int64", c_err.ZP_TYPE_INT64));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_column(w_handle, "col_float", c_err.ZP_TYPE_FLOAT));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_column(w_handle, "col_double", c_err.ZP_TYPE_DOUBLE));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_column(w_handle, "col_bytes", c_err.ZP_TYPE_BYTES));

    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_begin(w_handle));

    // Row 1: all values set
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_bool(w_handle, 0, 1));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_int32(w_handle, 1, 42));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_int64(w_handle, 2, 1234567890123));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_float(w_handle, 3, 3.14));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_double(w_handle, 4, 2.718281828));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_bytes(w_handle, 5, "hello", 5));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_row(w_handle));

    // Row 2: different values
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_bool(w_handle, 0, 0));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_int32(w_handle, 1, -99));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_int64(w_handle, 2, -1));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_float(w_handle, 3, 0.0));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_double(w_handle, 4, -1.0e10));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_bytes(w_handle, 5, "", 0));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_row(w_handle));

    // Row 3: null for all columns
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_null(w_handle, 0));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_null(w_handle, 1));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_null(w_handle, 2));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_null(w_handle, 3));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_null(w_handle, 4));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_null(w_handle, 5));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_row(w_handle));

    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_flush(w_handle));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_close(w_handle));

    var out_data: [*]const u8 = undefined;
    var out_len: usize = 0;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_get_buffer(w_handle, &out_data, &out_len));

    const allocator = std.testing.allocator;
    const written = try allocator.dupe(u8, out_data[0..out_len]);
    defer allocator.free(written);
    c_row_writer.zp_row_writer_free(w_handle);

    // Read back
    var r_handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_open_memory(written.ptr, written.len, &r_handle));
    defer c_row_reader.zp_row_reader_close(r_handle);

    var col_count: c_int = 0;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_get_column_count(r_handle, &col_count));
    try std.testing.expectEqual(@as(c_int, 6), col_count);

    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_read_row_group(r_handle, 0));

    // Row 1
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_next(r_handle));
    try std.testing.expectEqual(@as(c_int, 1), c_row_reader.zp_row_reader_get_bool(r_handle, 0));
    try std.testing.expectEqual(@as(i32, 42), c_row_reader.zp_row_reader_get_int32(r_handle, 1));
    try std.testing.expectEqual(@as(i64, 1234567890123), c_row_reader.zp_row_reader_get_int64(r_handle, 2));
    try std.testing.expectApproxEqAbs(@as(f32, 3.14), c_row_reader.zp_row_reader_get_float(r_handle, 3), 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 2.718281828), c_row_reader.zp_row_reader_get_double(r_handle, 4), 0.0001);
    {
        var data: [*]const u8 = undefined;
        var len: usize = 0;
        try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_get_bytes(r_handle, 5, &data, &len));
        try std.testing.expectEqualStrings("hello", data[0..len]);
    }

    // Row 2
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_next(r_handle));
    try std.testing.expectEqual(@as(c_int, 0), c_row_reader.zp_row_reader_get_bool(r_handle, 0));
    try std.testing.expectEqual(@as(i32, -99), c_row_reader.zp_row_reader_get_int32(r_handle, 1));
    try std.testing.expectEqual(@as(i64, -1), c_row_reader.zp_row_reader_get_int64(r_handle, 2));
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), c_row_reader.zp_row_reader_get_float(r_handle, 3), 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, -1.0e10), c_row_reader.zp_row_reader_get_double(r_handle, 4), 0.0001);
    {
        var data: [*]const u8 = undefined;
        var len: usize = 0;
        try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_get_bytes(r_handle, 5, &data, &len));
        try std.testing.expectEqual(@as(usize, 0), len);
    }

    // Row 3: all nulls
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_next(r_handle));
    try std.testing.expectEqual(@as(c_int, 1), c_row_reader.zp_row_reader_is_null(r_handle, 0));
    try std.testing.expectEqual(@as(c_int, 1), c_row_reader.zp_row_reader_is_null(r_handle, 1));
    try std.testing.expectEqual(@as(c_int, 1), c_row_reader.zp_row_reader_is_null(r_handle, 2));
    try std.testing.expectEqual(@as(c_int, 1), c_row_reader.zp_row_reader_is_null(r_handle, 3));
    try std.testing.expectEqual(@as(c_int, 1), c_row_reader.zp_row_reader_is_null(r_handle, 4));
    try std.testing.expectEqual(@as(c_int, 1), c_row_reader.zp_row_reader_is_null(r_handle, 5));

    try std.testing.expectEqual(c_err.ZP_ROW_END, c_row_reader.zp_row_reader_next(r_handle));
}

test "C ABI struct with bool field roundtrip" {
    var w_handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_open_memory(&w_handle));

    const bool_schema = c_row_writer.zp_schema_primitive(w_handle, c_err.ZP_TYPE_BOOL);
    const int_schema = c_row_writer.zp_schema_primitive(w_handle, c_err.ZP_TYPE_INT32);
    const field_names = [_][*:0]const u8{ "flag", "val" };
    const field_schemas = [_]?*const anyopaque{ bool_schema, int_schema };
    const struct_schema = c_row_writer.zp_schema_struct(w_handle, &field_names, &field_schemas, 2);
    try std.testing.expect(struct_schema != null);
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_column_nested(w_handle, "data", struct_schema));

    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_begin(w_handle));

    // Row 1: {flag: true, val: 10}
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_begin_struct(w_handle, 0));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_field_bool(w_handle, 0, 0, 1));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_field_int32(w_handle, 0, 1, 10));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_end_struct(w_handle, 0));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_row(w_handle));

    // Row 2: {flag: false, val: 20}
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_begin_struct(w_handle, 0));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_field_bool(w_handle, 0, 0, 0));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_field_int32(w_handle, 0, 1, 20));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_end_struct(w_handle, 0));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_row(w_handle));

    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_flush(w_handle));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_close(w_handle));

    var out_data: [*]const u8 = undefined;
    var out_len: usize = 0;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_get_buffer(w_handle, &out_data, &out_len));

    const allocator = std.testing.allocator;
    const written = try allocator.dupe(u8, out_data[0..out_len]);
    defer allocator.free(written);
    c_row_writer.zp_row_writer_free(w_handle);

    var r_handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_open_memory(written.ptr, written.len, &r_handle));
    defer c_row_reader.zp_row_reader_close(r_handle);

    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_read_row_group(r_handle, 0));

    // Row 1
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_next(r_handle));
    const val1 = c_row_reader.zp_row_reader_get_value(r_handle, 0);
    try std.testing.expect(val1 != null);
    try std.testing.expectEqual(c_err.ZP_TYPE_STRUCT, c_row_reader.zp_value_get_type(val1));
    const f1_flag = c_row_reader.zp_value_get_struct_field_value(val1, 0);
    try std.testing.expectEqual(@as(c_int, 1), c_row_reader.zp_value_get_bool(f1_flag));
    const f1_val = c_row_reader.zp_value_get_struct_field_value(val1, 1);
    try std.testing.expectEqual(@as(i32, 10), c_row_reader.zp_value_get_int32(f1_val));

    // Row 2
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_next(r_handle));
    const val2 = c_row_reader.zp_row_reader_get_value(r_handle, 0);
    const f2_flag = c_row_reader.zp_value_get_struct_field_value(val2, 0);
    try std.testing.expectEqual(@as(c_int, 0), c_row_reader.zp_value_get_bool(f2_flag));
    const f2_val = c_row_reader.zp_value_get_struct_field_value(val2, 1);
    try std.testing.expectEqual(@as(i32, 20), c_row_reader.zp_value_get_int32(f2_val));

    try std.testing.expectEqual(c_err.ZP_ROW_END, c_row_reader.zp_row_reader_next(r_handle));
}

test "C ABI row writer options: compression, kv metadata" {
    if (!build_options.supports_snappy) return;
    var w_handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_open_memory(&w_handle));

    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_column(w_handle, "x", c_err.ZP_TYPE_INT32));

    // Set compression (SNAPPY = 1)
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_compression(w_handle, 1));
    // Set row group size
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_row_group_size(w_handle, 100));
    // Set kv metadata
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_kv_metadata(w_handle, "author", 6, "test", 4));

    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_begin(w_handle));

    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_int32(w_handle, 0, 42));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_row(w_handle));

    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_flush(w_handle));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_close(w_handle));

    var out_data: [*]const u8 = undefined;
    var out_len: usize = 0;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_get_buffer(w_handle, &out_data, &out_len));

    const allocator = std.testing.allocator;
    const written = try allocator.dupe(u8, out_data[0..out_len]);
    defer allocator.free(written);
    c_row_writer.zp_row_writer_free(w_handle);

    // Read back and verify data + metadata
    var r_handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_open_memory(written.ptr, written.len, &r_handle));
    defer c_row_reader.zp_row_reader_close(r_handle);

    // Verify kv metadata
    var kv_count: c_int = 0;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_get_kv_metadata_count(r_handle, &kv_count));
    try std.testing.expect(kv_count >= 1);

    // Find our "author" key
    var found_author = false;
    var i: c_int = 0;
    while (i < kv_count) : (i += 1) {
        var key_data: [*]const u8 = undefined;
        var key_len: usize = 0;
        if (c_row_reader.zp_row_reader_get_kv_metadata_key(r_handle, i, &key_data, &key_len) == c_err.ZP_OK) {
            if (std.mem.eql(u8, key_data[0..key_len], "author")) {
                var val_data: [*]const u8 = undefined;
                var val_len: usize = 0;
                try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_get_kv_metadata_value(r_handle, i, &val_data, &val_len));
                try std.testing.expectEqualStrings("test", val_data[0..val_len]);
                found_author = true;
            }
        }
    }
    try std.testing.expect(found_author);

    // Verify data
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_read_row_group(r_handle, 0));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_next(r_handle));
    try std.testing.expectEqual(@as(i32, 42), c_row_reader.zp_row_reader_get_int32(r_handle, 0));
    try std.testing.expectEqual(c_err.ZP_ROW_END, c_row_reader.zp_row_reader_next(r_handle));
}

// ============================================================================
// Focused isolation tests (work backward from known-good)
// ============================================================================

const WriteTargetWriter = parquet.WriteTargetWriter;
const BufferTarget = parquet.io.BufferTarget;
const RowWriterHandle = c_handles.RowWriterHandle;
const TypeInfo = c_handles.TypeInfo;

test "Test A: WriteTargetWriter -> BufferTarget I/O path" {
    const allocator = std.testing.allocator;
    var bt = BufferTarget.init(allocator);
    defer bt.deinit();

    var target_writer = WriteTargetWriter.init(bt.target());
    const writer = target_writer.writer();

    const payload1 = "Hello, Parquet!";
    const payload2 = [_]u8{ 0xDE, 0xAD, 0xBE, 0xEF };
    writer.writeAll(payload1) catch return error.TestFailed;
    writer.writeAll(&payload2) catch return error.TestFailed;

    const written = bt.written();
    try std.testing.expectEqual(payload1.len + payload2.len, written.len);
    try std.testing.expectEqualStrings(payload1, written[0..payload1.len]);
    try std.testing.expectEqualSlices(u8, &payload2, written[payload1.len..]);
}

test "Test B: Regular Writer baseline (known-good)" {
    const allocator = std.testing.allocator;
    const columns = [_]parquet.ColumnDef{
        .{ .name = "x", .type_ = .int32, .optional = true },
    };
    var writer = try parquet.writeToBuffer(allocator, &columns);
    defer writer.deinit();

    const vals = [_]parquet.Optional(i32){
        .{ .value = 10 },
        .{ .value = 20 },
        .{ .value = 30 },
    };
    try writer.writeColumnOptional(i32, 0, &vals);
    try writer.close();

    const buf = try writer.toOwnedSlice();
    defer allocator.free(buf);

    try std.testing.expectEqualStrings("PAR1", buf[0..4]);
    try std.testing.expectEqualStrings("PAR1", buf[buf.len - 4 ..]);

    var reader = try parquet.openBufferDynamic(allocator, buf, .{});
    defer reader.deinit();
    var rr = try parquet.readRowGroupAsArrow(allocator, reader.getSource(), reader.metadata, 0, null);
    defer rr.deinit();
    try std.testing.expectEqual(@as(i64, 3), rr.arrays[0].length);
}

test "Test C: RowWriterHandle minimal flat write via Zig methods" {
    const allocator = std.testing.allocator;

    const handle = try RowWriterHandle.openMemory();

    try handle.addColumn("x", .{ .physical = .int32, .logical = null, .type_length = null });
    try handle.begin();

    try handle.setInt32(0, 10);
    try handle.addRow();
    try handle.setInt32(0, 20);
    try handle.addRow();
    try handle.setInt32(0, 30);
    try handle.addRow();

    try handle.flush();
    try handle.writerClose();

    // Dupe the buffer before deinit frees it (handle uses page_allocator)
    const raw = switch (handle.backend) {
        .buffer => |bt| bt.written(),
        else => {
            handle.deinit();
            return error.TestFailed;
        },
    };
    const written = try allocator.dupe(u8, raw);
    defer allocator.free(written);
    handle.deinit();

    try std.testing.expect(written.len > 8);
    try std.testing.expectEqualStrings("PAR1", written[0..4]);
    try std.testing.expectEqualStrings("PAR1", written[written.len - 4 ..]);

    // Try to read it back
    var reader = try parquet.openBufferDynamic(allocator, written, .{});
    defer reader.deinit();
    try std.testing.expectEqual(@as(usize, 1), reader.metadata.schema.len - 1);
    var rr = try parquet.readRowGroupAsArrow(allocator, reader.getSource(), reader.metadata, 0, null);
    defer rr.deinit();
    try std.testing.expectEqual(@as(i64, 3), rr.arrays[0].length);
}

test "Test D: RowWriterHandle vs Writer both produce readable files" {
    const allocator = std.testing.allocator;

    // -- Path 1: Regular Writer (known-good) --
    const columns = [_]parquet.ColumnDef{
        .{ .name = "x", .type_ = .int32, .optional = true },
    };
    var zig_writer = try parquet.writeToBuffer(allocator, &columns);
    defer zig_writer.deinit();
    const vals = [_]parquet.Optional(i32){
        .{ .value = 10 },
        .{ .value = 20 },
        .{ .value = 30 },
    };
    try zig_writer.writeColumnOptional(i32, 0, &vals);
    try zig_writer.close();
    const good_buf = try zig_writer.toOwnedSlice();
    defer allocator.free(good_buf);

    // -- Path 2: RowWriterHandle --
    const rw_handle = try RowWriterHandle.openMemory();
    try rw_handle.addColumn("x", .{ .physical = .int32, .logical = null, .type_length = null });
    try rw_handle.begin();
    try rw_handle.setInt32(0, 10);
    try rw_handle.addRow();
    try rw_handle.setInt32(0, 20);
    try rw_handle.addRow();
    try rw_handle.setInt32(0, 30);
    try rw_handle.addRow();
    try rw_handle.flush();
    try rw_handle.writerClose();
    const rw_raw = switch (rw_handle.backend) {
        .buffer => |bt| bt.written(),
        else => {
            rw_handle.deinit();
            return error.TestFailed;
        },
    };
    const rw_buf = try allocator.dupe(u8, rw_raw);
    defer allocator.free(rw_buf);
    rw_handle.deinit();

    // Both must be valid Parquet files
    try std.testing.expectEqualStrings("PAR1", good_buf[0..4]);
    try std.testing.expectEqualStrings("PAR1", rw_buf[0..4]);
    try std.testing.expectEqualStrings("PAR1", good_buf[good_buf.len - 4 ..]);
    try std.testing.expectEqualStrings("PAR1", rw_buf[rw_buf.len - 4 ..]);

    // Both must be readable
    {
        var reader = try parquet.openBufferDynamic(allocator, good_buf, .{});
        defer reader.deinit();
        var rr = try parquet.readRowGroupAsArrow(allocator, reader.getSource(), reader.metadata, 0, null);
        defer rr.deinit();
        try std.testing.expectEqual(@as(i64, 3), rr.arrays[0].length);
    }
    {
        var reader = try parquet.openBufferDynamic(allocator, rw_buf, .{});
        defer reader.deinit();
        var rr = try parquet.readRowGroupAsArrow(allocator, reader.getSource(), reader.metadata, 0, null);
        defer rr.deinit();
        try std.testing.expectEqual(@as(i64, 3), rr.arrays[0].length);
    }
}

test "Test E: RowWriterHandle list<int32> roundtrip via Zig" {
    const allocator = std.testing.allocator;

    const handle = try RowWriterHandle.openMemory();

    try handle.addColumn("id", .{ .physical = .int32, .logical = null, .type_length = null });

    const schema_arena = handle.writer.schema_arena.allocator();
    const element_node = try schema_arena.create(parquet.SchemaNode);
    element_node.* = .{ .int32 = .{ .logical = null } };
    const list_node = try schema_arena.create(parquet.SchemaNode);
    list_node.* = .{ .list = element_node };
    const opt_node = try schema_arena.create(parquet.SchemaNode);
    opt_node.* = .{ .optional = list_node };
    try handle.addColumnNested("scores", opt_node);

    try handle.begin();

    // Row 1: id=1, scores=[10,20,30]
    try handle.setInt32(0, 1);
    try handle.beginList(1);
    try handle.appendNestedValue(1, .{ .int32_val = 10 });
    try handle.appendNestedValue(1, .{ .int32_val = 20 });
    try handle.appendNestedValue(1, .{ .int32_val = 30 });
    try handle.endList(1);
    try handle.addRow();

    // Row 2: id=2, scores=[40]
    try handle.setInt32(0, 2);
    try handle.beginList(1);
    try handle.appendNestedValue(1, .{ .int32_val = 40 });
    try handle.endList(1);
    try handle.addRow();

    try handle.flush();
    try handle.writerClose();

    const raw = switch (handle.backend) {
        .buffer => |bt| bt.written(),
        else => { handle.deinit(); return error.TestFailed; },
    };
    const written = try allocator.dupe(u8, raw);
    defer allocator.free(written);
    handle.deinit();

    try std.testing.expectEqualStrings("PAR1", written[0..4]);
    try std.testing.expectEqualStrings("PAR1", written[written.len - 4 ..]);

    // Read back via DynamicReader
    var dyn = try parquet.openBufferDynamic(allocator, written, .{});
    defer dyn.deinit();
    const rows = try dyn.readAllRows(0);
    defer {
        for (rows) |r| r.deinit();
        allocator.free(rows);
    }
    try std.testing.expectEqual(@as(usize, 2), rows.len);
}

test "Test F: RowWriterHandle struct roundtrip via Zig" {
    const allocator = std.testing.allocator;

    const handle = try RowWriterHandle.openMemory();

    try handle.addColumn("id", .{ .physical = .int32, .logical = null, .type_length = null });

    const schema_arena = handle.writer.schema_arena.allocator();
    const x_node = try schema_arena.create(parquet.SchemaNode);
    x_node.* = .{ .int32 = .{ .logical = null } };
    const y_node = try schema_arena.create(parquet.SchemaNode);
    y_node.* = .{ .int32 = .{ .logical = null } };

    const fields = try schema_arena.alloc(parquet.SchemaNode.Field, 2);
    fields[0] = .{ .name = "x", .node = x_node };
    fields[1] = .{ .name = "y", .node = y_node };

    const struct_node = try schema_arena.create(parquet.SchemaNode);
    struct_node.* = .{ .struct_ = .{ .fields = fields } };

    try handle.addColumnNested("point", struct_node);
    try handle.begin();

    // Row 1: id=1, point={x:10, y:20}
    try handle.setInt32(0, 1);
    try handle.beginStruct(1);
    try handle.setStructField(1, 0, .{ .int32_val = 10 });
    try handle.setStructField(1, 1, .{ .int32_val = 20 });
    try handle.endStruct(1);
    try handle.addRow();

    // Row 2: id=2, point={x:30, y:40}
    try handle.setInt32(0, 2);
    try handle.beginStruct(1);
    try handle.setStructField(1, 0, .{ .int32_val = 30 });
    try handle.setStructField(1, 1, .{ .int32_val = 40 });
    try handle.endStruct(1);
    try handle.addRow();

    try handle.flush();
    try handle.writerClose();

    const raw = switch (handle.backend) {
        .buffer => |bt| bt.written(),
        else => { handle.deinit(); return error.TestFailed; },
    };
    const written = try allocator.dupe(u8, raw);
    defer allocator.free(written);
    handle.deinit();

    try std.testing.expectEqualStrings("PAR1", written[0..4]);
    try std.testing.expectEqualStrings("PAR1", written[written.len - 4 ..]);

    var dyn = try parquet.openBufferDynamic(allocator, written, .{});
    defer dyn.deinit();
    const dyn_rows = try dyn.readAllRows(0);
    defer {
        for (dyn_rows) |r| r.deinit();
        allocator.free(dyn_rows);
    }
    try std.testing.expectEqual(@as(usize, 2), dyn_rows.len);
}

// ============================================================================
// C ABI reader tests for nested types written by Zig API
// ============================================================================

test "C ABI reader: nested list (list<list<int32>>)" {
    const allocator = std.testing.allocator;

    var rw = try parquet.createBufferDynamic(allocator);
    defer rw.deinit();

    const int32_node = try rw.allocSchemaNode(.{ .int32 = .{ .logical = null } });
    const inner_list = try rw.allocSchemaNode(.{ .list = int32_node });
    const outer_list = try rw.allocSchemaNode(.{ .list = inner_list });
    try rw.addColumnNested("matrix", outer_list, .{});
    try rw.begin();

    // Row 1: [[1,2],[3]]
    try rw.beginList(0);
    try rw.beginList(0);
    try rw.appendNestedValue(0, .{ .int32_val = 1 });
    try rw.appendNestedValue(0, .{ .int32_val = 2 });
    try rw.endList(0);
    try rw.beginList(0);
    try rw.appendNestedValue(0, .{ .int32_val = 3 });
    try rw.endList(0);
    try rw.endList(0);
    try rw.addRow();

    // Row 2: [[10,20,30]]
    try rw.beginList(0);
    try rw.beginList(0);
    try rw.appendNestedValue(0, .{ .int32_val = 10 });
    try rw.appendNestedValue(0, .{ .int32_val = 20 });
    try rw.appendNestedValue(0, .{ .int32_val = 30 });
    try rw.endList(0);
    try rw.endList(0);
    try rw.addRow();

    try rw.close();
    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var r_handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_open_memory(buffer.ptr, buffer.len, &r_handle));
    defer c_row_reader.zp_row_reader_close(r_handle);

    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_read_row_group(r_handle, 0));

    // Row 1: [[1,2],[3]]
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_next(r_handle));
    const v0 = c_row_reader.zp_row_reader_get_value(r_handle, 0);
    try std.testing.expect(v0 != null);
    try std.testing.expectEqual(c_err.ZP_TYPE_LIST, c_row_reader.zp_value_get_type(v0));
    try std.testing.expectEqual(@as(c_int, 2), c_row_reader.zp_value_get_list_len(v0));

    const inner0 = c_row_reader.zp_value_get_list_element(v0, 0);
    try std.testing.expectEqual(c_err.ZP_TYPE_LIST, c_row_reader.zp_value_get_type(inner0));
    try std.testing.expectEqual(@as(c_int, 2), c_row_reader.zp_value_get_list_len(inner0));
    try std.testing.expectEqual(@as(i32, 1), c_row_reader.zp_value_get_int32(c_row_reader.zp_value_get_list_element(inner0, 0)));
    try std.testing.expectEqual(@as(i32, 2), c_row_reader.zp_value_get_int32(c_row_reader.zp_value_get_list_element(inner0, 1)));

    const inner1 = c_row_reader.zp_value_get_list_element(v0, 1);
    try std.testing.expectEqual(@as(c_int, 1), c_row_reader.zp_value_get_list_len(inner1));
    try std.testing.expectEqual(@as(i32, 3), c_row_reader.zp_value_get_int32(c_row_reader.zp_value_get_list_element(inner1, 0)));

    // Row 2: [[10,20,30]]
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_next(r_handle));
    const v1 = c_row_reader.zp_row_reader_get_value(r_handle, 0);
    try std.testing.expectEqual(@as(c_int, 1), c_row_reader.zp_value_get_list_len(v1));

    const inner2 = c_row_reader.zp_value_get_list_element(v1, 0);
    try std.testing.expectEqual(@as(c_int, 3), c_row_reader.zp_value_get_list_len(inner2));
    try std.testing.expectEqual(@as(i32, 10), c_row_reader.zp_value_get_int32(c_row_reader.zp_value_get_list_element(inner2, 0)));
    try std.testing.expectEqual(@as(i32, 30), c_row_reader.zp_value_get_int32(c_row_reader.zp_value_get_list_element(inner2, 2)));
}

test "C ABI reader: list of struct" {
    const allocator = std.testing.allocator;

    var rw = try parquet.createBufferDynamic(allocator);
    defer rw.deinit();

    const name_node = try rw.allocSchemaNode(.{ .byte_array = .{ .logical = .string } });
    const qty_node = try rw.allocSchemaNode(.{ .int32 = .{ .logical = null } });
    const fields = try rw.allocSchemaFields(2);
    fields[0] = .{ .name = try rw.dupeSchemaName("name"), .node = name_node };
    fields[1] = .{ .name = try rw.dupeSchemaName("qty"), .node = qty_node };
    const struct_node = try rw.allocSchemaNode(.{ .struct_ = .{ .fields = fields } });
    const list_node = try rw.allocSchemaNode(.{ .list = struct_node });
    try rw.addColumnNested("items", list_node, .{});
    try rw.begin();

    // Row 1: [{name:"apple", qty:3}, {name:"banana", qty:5}]
    try rw.beginList(0);
    try rw.beginStruct(0);
    try rw.setStructFieldBytes(0, 0, "apple");
    try rw.setStructField(0, 1, .{ .int32_val = 3 });
    try rw.endStruct(0);
    try rw.beginStruct(0);
    try rw.setStructFieldBytes(0, 0, "banana");
    try rw.setStructField(0, 1, .{ .int32_val = 5 });
    try rw.endStruct(0);
    try rw.endList(0);
    try rw.addRow();

    // Row 2: [{name:"cherry", qty:1}]
    try rw.beginList(0);
    try rw.beginStruct(0);
    try rw.setStructFieldBytes(0, 0, "cherry");
    try rw.setStructField(0, 1, .{ .int32_val = 1 });
    try rw.endStruct(0);
    try rw.endList(0);
    try rw.addRow();

    try rw.close();
    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var r_handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_open_memory(buffer.ptr, buffer.len, &r_handle));
    defer c_row_reader.zp_row_reader_close(r_handle);

    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_read_row_group(r_handle, 0));

    // Row 1: [{name:"apple", qty:3}, {name:"banana", qty:5}]
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_next(r_handle));
    const v0 = c_row_reader.zp_row_reader_get_value(r_handle, 0);
    try std.testing.expect(v0 != null);
    try std.testing.expectEqual(c_err.ZP_TYPE_LIST, c_row_reader.zp_value_get_type(v0));
    try std.testing.expectEqual(@as(c_int, 2), c_row_reader.zp_value_get_list_len(v0));

    const s0 = c_row_reader.zp_value_get_list_element(v0, 0);
    try std.testing.expectEqual(c_err.ZP_TYPE_STRUCT, c_row_reader.zp_value_get_type(s0));
    const name_val0 = c_row_reader.zp_value_get_struct_field_value(s0, 0);
    var name_len: usize = 0;
    var name_ptr: [*]const u8 = undefined;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_value_get_bytes(name_val0, &name_ptr, &name_len));
    try std.testing.expectEqualStrings("apple", name_ptr[0..name_len]);
    const qty_val0 = c_row_reader.zp_value_get_struct_field_value(s0, 1);
    try std.testing.expectEqual(@as(i32, 3), c_row_reader.zp_value_get_int32(qty_val0));

    const s1 = c_row_reader.zp_value_get_list_element(v0, 1);
    const name_val1 = c_row_reader.zp_value_get_struct_field_value(s1, 0);
    var name1_len: usize = 0;
    var name1_ptr: [*]const u8 = undefined;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_value_get_bytes(name_val1, &name1_ptr, &name1_len));
    try std.testing.expectEqualStrings("banana", name1_ptr[0..name1_len]);
    const qty_val1 = c_row_reader.zp_value_get_struct_field_value(s1, 1);
    try std.testing.expectEqual(@as(i32, 5), c_row_reader.zp_value_get_int32(qty_val1));

    // Row 2: [{name:"cherry", qty:1}]
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_next(r_handle));
    const v1 = c_row_reader.zp_row_reader_get_value(r_handle, 0);
    try std.testing.expectEqual(@as(c_int, 1), c_row_reader.zp_value_get_list_len(v1));

    const s2 = c_row_reader.zp_value_get_list_element(v1, 0);
    const name_val2 = c_row_reader.zp_value_get_struct_field_value(s2, 0);
    var name2_len: usize = 0;
    var name2_ptr: [*]const u8 = undefined;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_value_get_bytes(name_val2, &name2_ptr, &name2_len));
    try std.testing.expectEqualStrings("cherry", name2_ptr[0..name2_len]);
    const qty_val2 = c_row_reader.zp_value_get_struct_field_value(s2, 1);
    try std.testing.expectEqual(@as(i32, 1), c_row_reader.zp_value_get_int32(qty_val2));
}

// ============================================================================
// C ABI writer round-trip tests for nested types
// ============================================================================

test "C ABI writer: nested list (list<list<int32>>) roundtrip" {
    const allocator = std.testing.allocator;

    var w_handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_open_memory(&w_handle));

    const int32_schema = c_row_writer.zp_schema_primitive(w_handle, c_err.ZP_TYPE_INT32);
    const inner_list = c_row_writer.zp_schema_list(w_handle, int32_schema);
    const outer_list = c_row_writer.zp_schema_list(w_handle, inner_list);
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_column_nested(w_handle, "matrix", outer_list));

    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_begin(w_handle));

    // Row 1: [[1,2],[3]]
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_begin_list(w_handle, 0)); // outer list
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_begin_list(w_handle, 0)); // inner list [1,2]
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_append_int32(w_handle, 0, 1));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_append_int32(w_handle, 0, 2));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_end_list(w_handle, 0)); // end inner
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_begin_list(w_handle, 0)); // inner list [3]
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_append_int32(w_handle, 0, 3));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_end_list(w_handle, 0)); // end inner
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_end_list(w_handle, 0)); // end outer
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_row(w_handle));

    // Row 2: [[10,20,30]]
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_begin_list(w_handle, 0));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_begin_list(w_handle, 0));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_append_int32(w_handle, 0, 10));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_append_int32(w_handle, 0, 20));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_append_int32(w_handle, 0, 30));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_end_list(w_handle, 0));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_end_list(w_handle, 0));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_row(w_handle));

    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_flush(w_handle));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_close(w_handle));

    var out_data: [*]const u8 = undefined;
    var out_len: usize = 0;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_get_buffer(w_handle, &out_data, &out_len));

    const written = try allocator.dupe(u8, out_data[0..out_len]);
    defer allocator.free(written);
    c_row_writer.zp_row_writer_free(w_handle);

    // Read back with C ABI reader
    var r_handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_open_memory(written.ptr, written.len, &r_handle));
    defer c_row_reader.zp_row_reader_close(r_handle);

    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_read_row_group(r_handle, 0));

    // Row 1: [[1,2],[3]]
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_next(r_handle));
    const rv0 = c_row_reader.zp_row_reader_get_value(r_handle, 0);
    try std.testing.expectEqual(@as(c_int, 2), c_row_reader.zp_value_get_list_len(rv0));

    const ri0 = c_row_reader.zp_value_get_list_element(rv0, 0);
    try std.testing.expectEqual(@as(c_int, 2), c_row_reader.zp_value_get_list_len(ri0));
    try std.testing.expectEqual(@as(i32, 1), c_row_reader.zp_value_get_int32(c_row_reader.zp_value_get_list_element(ri0, 0)));
    try std.testing.expectEqual(@as(i32, 2), c_row_reader.zp_value_get_int32(c_row_reader.zp_value_get_list_element(ri0, 1)));

    const ri1 = c_row_reader.zp_value_get_list_element(rv0, 1);
    try std.testing.expectEqual(@as(c_int, 1), c_row_reader.zp_value_get_list_len(ri1));
    try std.testing.expectEqual(@as(i32, 3), c_row_reader.zp_value_get_int32(c_row_reader.zp_value_get_list_element(ri1, 0)));

    // Row 2: [[10,20,30]]
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_next(r_handle));
    const rv1 = c_row_reader.zp_row_reader_get_value(r_handle, 0);
    try std.testing.expectEqual(@as(c_int, 1), c_row_reader.zp_value_get_list_len(rv1));

    const ri2 = c_row_reader.zp_value_get_list_element(rv1, 0);
    try std.testing.expectEqual(@as(c_int, 3), c_row_reader.zp_value_get_list_len(ri2));
    try std.testing.expectEqual(@as(i32, 10), c_row_reader.zp_value_get_int32(c_row_reader.zp_value_get_list_element(ri2, 0)));
    try std.testing.expectEqual(@as(i32, 30), c_row_reader.zp_value_get_int32(c_row_reader.zp_value_get_list_element(ri2, 2)));
}

test "C ABI writer: list<struct> roundtrip" {
    const allocator = std.testing.allocator;

    var w_handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_open_memory(&w_handle));

    const name_schema = c_row_writer.zp_schema_primitive(w_handle, c_err.ZP_TYPE_BYTES);
    const qty_schema = c_row_writer.zp_schema_primitive(w_handle, c_err.ZP_TYPE_INT32);

    const field_names = [_]?[*:0]const u8{ "name", "qty" };
    const field_schemas = [_]?*const anyopaque{ @ptrCast(name_schema), @ptrCast(qty_schema) };
    const struct_schema = c_row_writer.zp_schema_struct(w_handle, &field_names, &field_schemas, 2);
    const list_schema = c_row_writer.zp_schema_list(w_handle, struct_schema);
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_column_nested(w_handle, "items", list_schema));

    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_begin(w_handle));

    // Row 1: [{name:"apple",qty:3},{name:"banana",qty:5}]
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_begin_list(w_handle, 0));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_begin_struct(w_handle, 0));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_field_bytes(w_handle, 0, 0, "apple", 5));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_field_int32(w_handle, 0, 1, 3));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_end_struct(w_handle, 0));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_begin_struct(w_handle, 0));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_field_bytes(w_handle, 0, 0, "banana", 6));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_field_int32(w_handle, 0, 1, 5));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_end_struct(w_handle, 0));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_end_list(w_handle, 0));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_row(w_handle));

    // Row 2: [{name:"cherry",qty:1}]
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_begin_list(w_handle, 0));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_begin_struct(w_handle, 0));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_field_bytes(w_handle, 0, 0, "cherry", 6));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_set_field_int32(w_handle, 0, 1, 1));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_end_struct(w_handle, 0));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_end_list(w_handle, 0));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_add_row(w_handle));

    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_flush(w_handle));
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_close(w_handle));

    var out_data: [*]const u8 = undefined;
    var out_len: usize = 0;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_writer.zp_row_writer_get_buffer(w_handle, &out_data, &out_len));

    const written = try allocator.dupe(u8, out_data[0..out_len]);
    defer allocator.free(written);
    c_row_writer.zp_row_writer_free(w_handle);

    // Read back with C ABI reader
    var r_handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_open_memory(written.ptr, written.len, &r_handle));
    defer c_row_reader.zp_row_reader_close(r_handle);

    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_read_row_group(r_handle, 0));

    // Row 1
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_next(r_handle));
    const lv0 = c_row_reader.zp_row_reader_get_value(r_handle, 0);
    try std.testing.expectEqual(@as(c_int, 2), c_row_reader.zp_value_get_list_len(lv0));

    const sv0 = c_row_reader.zp_value_get_list_element(lv0, 0);
    try std.testing.expectEqual(c_err.ZP_TYPE_STRUCT, c_row_reader.zp_value_get_type(sv0));
    const nv0 = c_row_reader.zp_value_get_struct_field_value(sv0, 0);
    var nl0: usize = 0;
    var np0: [*]const u8 = undefined;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_value_get_bytes(nv0, &np0, &nl0));
    try std.testing.expectEqualStrings("apple", np0[0..nl0]);
    try std.testing.expectEqual(@as(i32, 3), c_row_reader.zp_value_get_int32(c_row_reader.zp_value_get_struct_field_value(sv0, 1)));

    const sv1 = c_row_reader.zp_value_get_list_element(lv0, 1);
    const nv1 = c_row_reader.zp_value_get_struct_field_value(sv1, 0);
    var nl1: usize = 0;
    var np1: [*]const u8 = undefined;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_value_get_bytes(nv1, &np1, &nl1));
    try std.testing.expectEqualStrings("banana", np1[0..nl1]);
    try std.testing.expectEqual(@as(i32, 5), c_row_reader.zp_value_get_int32(c_row_reader.zp_value_get_struct_field_value(sv1, 1)));

    // Row 2
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_next(r_handle));
    const lv1 = c_row_reader.zp_row_reader_get_value(r_handle, 0);
    try std.testing.expectEqual(@as(c_int, 1), c_row_reader.zp_value_get_list_len(lv1));
    const sv2 = c_row_reader.zp_value_get_list_element(lv1, 0);
    var nl2: usize = 0;
    var np2: [*]const u8 = undefined;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_value_get_bytes(c_row_reader.zp_value_get_struct_field_value(sv2, 0), &np2, &nl2));
    try std.testing.expectEqualStrings("cherry", np2[0..nl2]);
    try std.testing.expectEqual(@as(i32, 1), c_row_reader.zp_value_get_int32(c_row_reader.zp_value_get_struct_field_value(sv2, 1)));
}

test "C ABI reader: struct<id:i32, tags:list<i32>>" {
    const allocator = std.testing.allocator;

    var rw = try parquet.createBufferDynamic(allocator);
    defer rw.deinit();

    try rw.addColumn("id", parquet.TypeInfo.int32, .{});
    const int32_node = try rw.allocSchemaNode(.{ .int32 = .{ .logical = null } });
    const list_node = try rw.allocSchemaNode(.{ .list = int32_node });
    try rw.addColumnNested("tags", list_node, .{});
    try rw.begin();

    // Row 1: id=1, tags=[10,20,30]
    try rw.setInt32(0, 1);
    try rw.beginList(1);
    try rw.appendNestedValue(1, .{ .int32_val = 10 });
    try rw.appendNestedValue(1, .{ .int32_val = 20 });
    try rw.appendNestedValue(1, .{ .int32_val = 30 });
    try rw.endList(1);
    try rw.addRow();

    // Row 2: id=2, tags=[40]
    try rw.setInt32(0, 2);
    try rw.beginList(1);
    try rw.appendNestedValue(1, .{ .int32_val = 40 });
    try rw.endList(1);
    try rw.addRow();

    try rw.close();
    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var r_handle: ?*anyopaque = null;
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_open_memory(buffer.ptr, buffer.len, &r_handle));
    defer c_row_reader.zp_row_reader_close(r_handle);

    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_read_row_group(r_handle, 0));

    // Row 1: {id: 1, tags: [10, 20, 30]}
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_next(r_handle));

    // Column 0 is "id"
    const id0 = c_row_reader.zp_row_reader_get_value(r_handle, 0);
    try std.testing.expect(id0 != null);
    try std.testing.expectEqual(@as(i32, 1), c_row_reader.zp_value_get_int32(id0));

    // Column 1 is "tags" (list<i32>)
    const tags0 = c_row_reader.zp_row_reader_get_value(r_handle, 1);
    try std.testing.expect(tags0 != null);
    try std.testing.expectEqual(c_err.ZP_TYPE_LIST, c_row_reader.zp_value_get_type(tags0));
    try std.testing.expectEqual(@as(c_int, 3), c_row_reader.zp_value_get_list_len(tags0));
    try std.testing.expectEqual(@as(i32, 10), c_row_reader.zp_value_get_int32(c_row_reader.zp_value_get_list_element(tags0, 0)));
    try std.testing.expectEqual(@as(i32, 20), c_row_reader.zp_value_get_int32(c_row_reader.zp_value_get_list_element(tags0, 1)));
    try std.testing.expectEqual(@as(i32, 30), c_row_reader.zp_value_get_int32(c_row_reader.zp_value_get_list_element(tags0, 2)));

    // Row 2: {id: 2, tags: [40]}
    try std.testing.expectEqual(c_err.ZP_OK, c_row_reader.zp_row_reader_next(r_handle));

    const id1 = c_row_reader.zp_row_reader_get_value(r_handle, 0);
    try std.testing.expectEqual(@as(i32, 2), c_row_reader.zp_value_get_int32(id1));

    const tags1 = c_row_reader.zp_row_reader_get_value(r_handle, 1);
    try std.testing.expectEqual(c_err.ZP_TYPE_LIST, c_row_reader.zp_value_get_type(tags1));
    try std.testing.expectEqual(@as(c_int, 1), c_row_reader.zp_value_get_list_len(tags1));
    try std.testing.expectEqual(@as(i32, 40), c_row_reader.zp_value_get_int32(c_row_reader.zp_value_get_list_element(tags1, 0)));
}
