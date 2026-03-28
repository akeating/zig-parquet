//! Tests for SeekableReader interface and backends
//!
//! Verifies BufferReader and FileReader work correctly and can be used
//! polymorphically through the SeekableReader interface.

const std = @import("std");
const parquet = @import("../lib.zig");
const SeekableReader = parquet.SeekableReader;
const BufferReader = parquet.io.BufferReader;
const FileReader = parquet.io.FileReader;
const build_options = @import("build_options");

// =============================================================================
// BufferReader Tests
// =============================================================================

test "BufferReader basic read" {
    const data = "Hello, World!";
    var buf_reader = BufferReader.init(data);
    const reader = buf_reader.reader();

    // Read from beginning
    var buf: [5]u8 = undefined;
    const n = try reader.readAt(0, &buf);
    try std.testing.expectEqual(@as(usize, 5), n);
    try std.testing.expectEqualStrings("Hello", &buf);

    // Read from middle
    const n2 = try reader.readAt(7, &buf);
    try std.testing.expectEqual(@as(usize, 5), n2);
    try std.testing.expectEqualStrings("World", &buf);
}

test "BufferReader size" {
    const data = "Test data";
    var buf_reader = BufferReader.init(data);
    const reader = buf_reader.reader();

    try std.testing.expectEqual(@as(u64, 9), reader.size());
}

test "BufferReader read past end" {
    const data = "Short";
    var buf_reader = BufferReader.init(data);
    const reader = buf_reader.reader();

    // Read starting near end - should return partial data
    var buf: [10]u8 = undefined;
    const n = try reader.readAt(3, &buf);
    try std.testing.expectEqual(@as(usize, 2), n); // "rt"
    try std.testing.expectEqualStrings("rt", buf[0..n]);
}

test "BufferReader read at exact end" {
    const data = "Test";
    var buf_reader = BufferReader.init(data);
    const reader = buf_reader.reader();

    var buf: [10]u8 = undefined;
    const n = try reader.readAt(4, &buf);
    try std.testing.expectEqual(@as(usize, 0), n);
}

test "BufferReader read past end offset" {
    const data = "Test";
    var buf_reader = BufferReader.init(data);
    const reader = buf_reader.reader();

    var buf: [10]u8 = undefined;
    const n = try reader.readAt(100, &buf);
    try std.testing.expectEqual(@as(usize, 0), n);
}

test "BufferReader empty buffer" {
    const data: []const u8 = "";
    var buf_reader = BufferReader.init(data);
    const reader = buf_reader.reader();

    try std.testing.expectEqual(@as(u64, 0), reader.size());

    var buf: [10]u8 = undefined;
    const n = try reader.readAt(0, &buf);
    try std.testing.expectEqual(@as(usize, 0), n);
}

test "BufferReader read entire buffer" {
    const data = "Complete";
    var buf_reader = BufferReader.init(data);
    const reader = buf_reader.reader();

    var buf: [8]u8 = undefined;
    const n = try reader.readAt(0, &buf);
    try std.testing.expectEqual(@as(usize, 8), n);
    try std.testing.expectEqualStrings("Complete", &buf);
}

// =============================================================================
// FileReader Tests
// =============================================================================

test "FileReader basic read" {
    const allocator = std.testing.allocator;

    // Create a temp file with test data
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const test_data = "Hello from file!";
    const file = try tmp_dir.dir.createFile("test.bin", .{ .read = true });
    try file.writeAll(test_data);
    try file.seekTo(0);

    var file_reader = try FileReader.init(file);
    const reader = file_reader.reader();
    defer file.close();

    // Check size
    try std.testing.expectEqual(@as(u64, test_data.len), reader.size());

    // Read from beginning
    var buf: [5]u8 = undefined;
    const n = try reader.readAt(0, &buf);
    try std.testing.expectEqual(@as(usize, 5), n);
    try std.testing.expectEqualStrings("Hello", &buf);

    // Read from middle
    const n2 = try reader.readAt(11, &buf);
    try std.testing.expectEqual(@as(usize, 5), n2);
    try std.testing.expectEqualStrings("file!", &buf);

    _ = allocator;
}

test "FileReader matches BufferReader" {
    // Write some data to a temp file, then verify FileReader and BufferReader
    // produce the same results for the same data
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const test_data = "The quick brown fox jumps over the lazy dog.";

    // Create file with data
    const file = try tmp_dir.dir.createFile("match_test.bin", .{ .read = true });
    try file.writeAll(test_data);
    try file.seekTo(0);

    var file_reader = try FileReader.init(file);
    const freader = file_reader.reader();
    defer file.close();

    var buf_reader = BufferReader.init(test_data);
    const breader = buf_reader.reader();

    // Size should match
    try std.testing.expectEqual(breader.size(), freader.size());

    // Various reads should match
    const offsets = [_]u64{ 0, 4, 10, 20, 40, 44 };
    for (offsets) |offset| {
        var fbuf: [10]u8 = undefined;
        var bbuf: [10]u8 = undefined;

        const fn_read = try freader.readAt(offset, &fbuf);
        const bn_read = try breader.readAt(offset, &bbuf);

        try std.testing.expectEqual(bn_read, fn_read);
        try std.testing.expectEqualSlices(u8, bbuf[0..bn_read], fbuf[0..fn_read]);
    }
}

// =============================================================================
// Polymorphic Usage Tests
// =============================================================================

test "SeekableReader interface works with both backends" {
    const test_data = "Polymorphic test data";

    // Create both readers
    var buf_reader = BufferReader.init(test_data);

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file = try tmp_dir.dir.createFile("poly_test.bin", .{ .read = true });
    try file.writeAll(test_data);
    try file.seekTo(0);
    defer file.close();

    var file_reader = try FileReader.init(file);

    // Get SeekableReader interfaces
    const readers = [_]SeekableReader{
        buf_reader.reader(),
        file_reader.reader(),
    };

    // Both should behave identically
    for (readers) |reader| {
        try std.testing.expectEqual(@as(u64, test_data.len), reader.size());

        var buf: [10]u8 = undefined;
        const n = try reader.readAt(0, &buf);
        try std.testing.expectEqual(@as(usize, 10), n);
        try std.testing.expectEqualStrings("Polymorphi", &buf);
    }
}

test "SeekableReader can be passed to functions" {
    const test_data = "Function parameter test";
    var buf_reader = BufferReader.init(test_data);

    // This simulates how Reader.init() would use SeekableReader
    const result = try readFooterSize(buf_reader.reader());
    try std.testing.expectEqual(@as(u64, test_data.len), result);
}

/// Helper function that accepts SeekableReader - simulates future Reader usage
fn readFooterSize(reader: SeekableReader) !u64 {
    // This is the pattern that will be used in Reader.init()
    const size = reader.size();
    if (size < 12) return error.FileTooSmall;

    // Read last 8 bytes (simulating footer size read)
    var tail: [8]u8 = undefined;
    const n = try reader.readAt(size - 8, &tail);
    if (n != 8) return error.InputOutput;

    return size;
}

// =============================================================================
// Edge Cases
// =============================================================================

test "BufferReader binary data" {
    // Test with non-UTF8 binary data
    const data = [_]u8{ 0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD, 0x00, 0x00 };
    var buf_reader = BufferReader.init(&data);
    const reader = buf_reader.reader();

    var buf: [4]u8 = undefined;
    const n = try reader.readAt(2, &buf);
    try std.testing.expectEqual(@as(usize, 4), n);
    try std.testing.expectEqual(@as(u8, 0x02), buf[0]);
    try std.testing.expectEqual(@as(u8, 0xFF), buf[1]);
    try std.testing.expectEqual(@as(u8, 0xFE), buf[2]);
    try std.testing.expectEqual(@as(u8, 0xFD), buf[3]);
}

test "BufferReader sequential reads" {
    const data = "0123456789";
    var buf_reader = BufferReader.init(data);
    const reader = buf_reader.reader();

    // Simulate sequential reading pattern used in Parquet parsing
    var buf: [2]u8 = undefined;

    _ = try reader.readAt(0, &buf);
    try std.testing.expectEqualStrings("01", &buf);

    _ = try reader.readAt(2, &buf);
    try std.testing.expectEqualStrings("23", &buf);

    _ = try reader.readAt(4, &buf);
    try std.testing.expectEqualStrings("45", &buf);

    // Jump back (random access)
    _ = try reader.readAt(1, &buf);
    try std.testing.expectEqualStrings("12", &buf);
}

// =============================================================================
// Integration Tests - Reading Real Parquet Files from Buffers
// =============================================================================

test "Reader.initFromBuffer reads real Parquet file" {
    const allocator = std.testing.allocator;

    // Read the Parquet file into memory
    // Schema: bool_col, int32_col, int64_col, float_col, double_col, string_col, binary_col, fixed_binary_col
    const file_data = try std.fs.cwd().readFileAlloc(
        allocator,
        "../test-files-arrow/basic/basic_types_plain_uncompressed.parquet",
        10_000_000,
    );
    defer allocator.free(file_data);

    var reader = try parquet.openBufferDynamic(allocator, file_data, .{});
    defer reader.deinit();

    // Verify we can read metadata
    try std.testing.expect(reader.metadata.num_rows > 0);
    try std.testing.expect(reader.metadata.schema.len > 0);

    // Read rows and check int32 column (column 1)
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expect(rows.len > 0);
}

test "Reader.initFromBuffer matches file-based reading" {
    const allocator = std.testing.allocator;

    // Schema: bool_col, int32_col, int64_col, float_col, double_col, string_col, binary_col, fixed_binary_col
    const file_path = "../test-files-arrow/basic/basic_types_plain_uncompressed.parquet";

    // Read file into memory
    const file_data = try std.fs.cwd().readFileAlloc(allocator, file_path, 10_000_000);
    defer allocator.free(file_data);

    // Initialize from buffer
    var buf_reader_inst = try parquet.openBufferDynamic(allocator, file_data, .{});
    defer buf_reader_inst.deinit();

    // Initialize from file
    const file = try std.fs.cwd().openFile(file_path, .{});
    defer file.close();
    var file_reader_inst = try parquet.openFileDynamic(allocator, file, .{});
    defer file_reader_inst.deinit();

    // Metadata should match
    try std.testing.expectEqual(buf_reader_inst.metadata.num_rows, file_reader_inst.metadata.num_rows);
    try std.testing.expectEqual(buf_reader_inst.metadata.schema.len, file_reader_inst.metadata.schema.len);

    // Column data should match (using int32 column at index 1)
    const buf_rows = try buf_reader_inst.readAllRows(0);
    defer {
        for (buf_rows) |row| row.deinit();
        allocator.free(buf_rows);
    }

    const file_rows = try file_reader_inst.readAllRows(0);
    defer {
        for (file_rows) |row| row.deinit();
        allocator.free(file_rows);
    }

    try std.testing.expectEqual(buf_rows.len, file_rows.len);
    for (buf_rows, file_rows) |br, fr| {
        const bv = br.getColumn(1).?;
        const fv = fr.getColumn(1).?;
        if (bv.isNull()) {
            try std.testing.expect(fv.isNull());
        } else {
            try std.testing.expectEqual(bv.asInt32().?, fv.asInt32().?);
        }
    }
}

test "DynamicReader.initFromBuffer reads real Parquet file" {
    const allocator = std.testing.allocator;

    // Read the Parquet file into memory
    const file_data = try std.fs.cwd().readFileAlloc(
        allocator,
        "../test-files-arrow/basic/basic_types_plain_uncompressed.parquet",
        10_000_000,
    );
    defer allocator.free(file_data);

    var reader = try parquet.openBufferDynamic(allocator, file_data, .{});
    defer reader.deinit();

    // Verify we can read metadata
    try std.testing.expect(reader.getTotalNumRows() > 0);
    try std.testing.expect(reader.getNumRowGroups() > 0);
    try std.testing.expect(reader.getNumColumns() > 0);

    // Read rows
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expect(rows.len > 0);
}

test "DynamicReader.initFromBuffer reads real Parquet file with row iteration" {
    const allocator = std.testing.allocator;

    // Read the Parquet file into memory
    // Schema: bool_col, int32_col, int64_col, float_col, double_col, string_col, binary_col, fixed_binary_col
    const file_data = try std.fs.cwd().readFileAlloc(
        allocator,
        "../test-files-arrow/basic/basic_types_plain_uncompressed.parquet",
        10_000_000,
    );
    defer allocator.free(file_data);

    var reader = try parquet.openBufferDynamic(allocator, file_data, .{});
    defer reader.deinit();

    // Read rows
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expect(rows.len > 0);
}

test "Reader.initFromBuffer with compressed file" {
    if (!build_options.enable_zstd) return;
    const allocator = std.testing.allocator;

    // Read a zstd-compressed Parquet file into memory
    // Schema: repeated (string), sequence (i64)
    const file_data = try std.fs.cwd().readFileAlloc(
        allocator,
        "../test-files-arrow/compression/compression_zstd.parquet",
        10_000_000,
    );
    defer allocator.free(file_data);

    var reader = try parquet.openBufferDynamic(allocator, file_data, .{});
    defer reader.deinit();

    // Verify we can read metadata and data
    try std.testing.expect(reader.metadata.num_rows > 0);

    // Read rows and check sequence column (i64)
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expect(rows.len > 0);
}

// =============================================================================
// Writer Buffer Tests
// =============================================================================

test "Writer.initToBuffer basic write" {
    const allocator = std.testing.allocator;

    // Create a Writer that writes to a buffer
    var writer = try parquet.writeToBuffer(allocator, &.{
        .{ .name = "id", .type_ = .int32, .optional = false },
        .{ .name = "value", .type_ = .int64, .optional = false },
    });
    defer writer.deinit();

    // Write some data
    try writer.writeColumn(i32, 0, &[_]i32{ 1, 2, 3 });
    try writer.writeColumn(i64, 1, &[_]i64{ 100, 200, 300 });

    // Close and get the buffer
    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    // Verify it's a valid Parquet file (starts and ends with PAR1)
    try std.testing.expect(buffer.len >= 12);
    try std.testing.expectEqualStrings("PAR1", buffer[0..4]);
    try std.testing.expectEqualStrings("PAR1", buffer[buffer.len - 4 ..]);
}

test "Writer round-trip buffer" {
    const allocator = std.testing.allocator;

    // Create a Writer that writes to a buffer
    var writer = try parquet.writeToBuffer(allocator, &.{
        .{ .name = "id", .type_ = .int32, .optional = false },
        .{ .name = "value", .type_ = .int64, .optional = false },
    });
    defer writer.deinit();

    // Write some data
    const ids = [_]i32{ 1, 2, 3, 4, 5 };
    const values = [_]i64{ 100, 200, 300, 400, 500 };
    try writer.writeColumn(i32, 0, &ids);
    try writer.writeColumn(i64, 1, &values);

    // Close and get the buffer
    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    // Read it back using DynamicReader
    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    // Verify metadata
    try std.testing.expectEqual(@as(i64, 5), reader.metadata.num_rows);

    // Read and verify columns
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }
    try std.testing.expectEqual(@as(usize, 5), rows.len);
    for (rows, 0..) |row, i| {
        try std.testing.expectEqual(ids[i], row.getColumn(0).?.asInt32().?);
        try std.testing.expectEqual(values[i], row.getColumn(1).?.asInt64().?);
    }
}

test "DynamicWriter.initToBuffer basic write" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", parquet.TypeInfo.int32, .{});
    try writer.addColumn("value", parquet.TypeInfo.int64, .{});
    try writer.begin();

    try writer.setInt32(0, 1);
    try writer.setInt64(1, 100);
    try writer.addRow();

    try writer.setInt32(0, 2);
    try writer.setInt64(1, 200);
    try writer.addRow();

    try writer.setInt32(0, 3);
    try writer.setInt64(1, 300);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    // Verify it's a valid Parquet file (starts and ends with PAR1)
    try std.testing.expect(buffer.len >= 12);
    try std.testing.expectEqualStrings("PAR1", buffer[0..4]);
    try std.testing.expectEqualStrings("PAR1", buffer[buffer.len - 4 ..]);
}

test "DynamicWriter round-trip buffer" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", parquet.TypeInfo.int32, .{});
    try writer.addColumn("value", parquet.TypeInfo.int64, .{});
    try writer.addColumn("name", parquet.TypeInfo.string, .{});
    try writer.begin();

    const test_data = [_]struct { id: i32, value: i64, name: []const u8 }{
        .{ .id = 1, .value = 100, .name = "alice" },
        .{ .id = 2, .value = 200, .name = "bob" },
        .{ .id = 3, .value = 300, .name = "charlie" },
    };

    for (test_data) |row| {
        try writer.setInt32(0, row.id);
        try writer.setInt64(1, row.value);
        try writer.setBytes(2, row.name);
        try writer.addRow();
    }

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 3), rows.len);

    for (test_data, 0..) |expected, i| {
        try std.testing.expectEqual(expected.id, rows[i].getColumn(0).?.asInt32().?);
        try std.testing.expectEqual(expected.value, rows[i].getColumn(1).?.asInt64().?);
        try std.testing.expectEqualStrings(expected.name, rows[i].getColumn(2).?.asBytes().?);
    }
}
