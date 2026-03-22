//! Reader integration tests
//!
//! Tests that read actual Parquet files and verify the data.

const std = @import("std");
const parquet = @import("../lib.zig");
const format = parquet.format;

test "read basic_types_plain_uncompressed.parquet" {
    const allocator = std.testing.allocator;

    // Open the test file
    const file = std.fs.cwd().openFile("../test-files-arrow/basic/basic_types_plain_uncompressed.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file (run from zig-parquet dir): {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    // Verify metadata
    try std.testing.expectEqual(@as(i64, 5), reader.metadata.num_rows);
    try std.testing.expectEqual(@as(usize, 1), reader.metadata.row_groups.len);

    // Schema should have 9 elements (1 root + 8 columns)
    try std.testing.expectEqual(@as(usize, 9), reader.metadata.schema.len);

    // Check column names
    try std.testing.expectEqual(@as(usize, 8), reader.getNumColumns());
    try std.testing.expectEqualStrings("bool_col", reader.getLeafSchemaElement(0).?.name);
    try std.testing.expectEqualStrings("int32_col", reader.getLeafSchemaElement(1).?.name);
    try std.testing.expectEqualStrings("int64_col", reader.getLeafSchemaElement(2).?.name);
    try std.testing.expectEqualStrings("float_col", reader.getLeafSchemaElement(3).?.name);
    try std.testing.expectEqualStrings("double_col", reader.getLeafSchemaElement(4).?.name);
    try std.testing.expectEqualStrings("string_col", reader.getLeafSchemaElement(5).?.name);
    try std.testing.expectEqualStrings("binary_col", reader.getLeafSchemaElement(6).?.name);
    try std.testing.expectEqualStrings("fixed_binary_col", reader.getLeafSchemaElement(7).?.name);

    // Verify row group metadata
    const rg = reader.metadata.row_groups[0];
    try std.testing.expectEqual(@as(i64, 5), rg.num_rows);
    try std.testing.expectEqual(@as(usize, 8), rg.columns.len);

    // Verify column metadata
    try std.testing.expectEqual(format.PhysicalType.boolean, rg.columns[0].meta_data.?.type_);
    try std.testing.expectEqual(format.PhysicalType.int32, rg.columns[1].meta_data.?.type_);
    try std.testing.expectEqual(format.PhysicalType.int64, rg.columns[2].meta_data.?.type_);
    try std.testing.expectEqual(format.PhysicalType.float, rg.columns[3].meta_data.?.type_);
    try std.testing.expectEqual(format.PhysicalType.double, rg.columns[4].meta_data.?.type_);
    try std.testing.expectEqual(format.PhysicalType.byte_array, rg.columns[5].meta_data.?.type_);
    try std.testing.expectEqual(format.PhysicalType.byte_array, rg.columns[6].meta_data.?.type_);
    try std.testing.expectEqual(format.PhysicalType.fixed_len_byte_array, rg.columns[7].meta_data.?.type_);
}

test "read int32 column values" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/basic/basic_types_plain_uncompressed.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    // Read int32_col (column index 1)
    // Expected: [1, -2, 2147483647, -2147483648, None]
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 5), rows.len);

    // Check values
    try std.testing.expectEqual(@as(i32, 1), rows[0].getColumn(1).?.asInt32().?);
    try std.testing.expectEqual(@as(i32, -2), rows[1].getColumn(1).?.asInt32().?);
    try std.testing.expectEqual(@as(i32, 2147483647), rows[2].getColumn(1).?.asInt32().?);
    try std.testing.expectEqual(@as(i32, -2147483648), rows[3].getColumn(1).?.asInt32().?);
    try std.testing.expect(rows[4].getColumn(1).?.isNull());
}

test "read int64 column values" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/basic/basic_types_plain_uncompressed.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    // Read int64_col (column index 2)
    // Expected: [1, -2, 9223372036854775807, None, 0]
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 5), rows.len);

    try std.testing.expectEqual(@as(i64, 1), rows[0].getColumn(2).?.asInt64().?);
    try std.testing.expectEqual(@as(i64, -2), rows[1].getColumn(2).?.asInt64().?);
    try std.testing.expectEqual(@as(i64, 9223372036854775807), rows[2].getColumn(2).?.asInt64().?);
    try std.testing.expect(rows[3].getColumn(2).?.isNull());
    try std.testing.expectEqual(@as(i64, 0), rows[4].getColumn(2).?.asInt64().?);
}

test "read float column values" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/basic/basic_types_plain_uncompressed.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    // Read float_col (column index 3)
    // Expected: [1.5, -2.5, inf, -inf, None]
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 5), rows.len);

    try std.testing.expectEqual(@as(f32, 1.5), rows[0].getColumn(3).?.asFloat().?);
    try std.testing.expectEqual(@as(f32, -2.5), rows[1].getColumn(3).?.asFloat().?);
    try std.testing.expect(std.math.isPositiveInf(rows[2].getColumn(3).?.asFloat().?));
    try std.testing.expect(std.math.isNegativeInf(rows[3].getColumn(3).?.asFloat().?));
    try std.testing.expect(rows[4].getColumn(3).?.isNull());
}

test "read double column values" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/basic/basic_types_plain_uncompressed.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    // Read double_col (column index 4)
    // Expected: [1.5, -2.5, 1.7976931348623157e308, None, 0.0]
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 5), rows.len);

    try std.testing.expectEqual(@as(f64, 1.5), rows[0].getColumn(4).?.asDouble().?);
    try std.testing.expectEqual(@as(f64, -2.5), rows[1].getColumn(4).?.asDouble().?);
    try std.testing.expectEqual(@as(f64, 1.7976931348623157e308), rows[2].getColumn(4).?.asDouble().?);
    try std.testing.expect(rows[3].getColumn(4).?.isNull());
    try std.testing.expectEqual(@as(f64, 0.0), rows[4].getColumn(4).?.asDouble().?);
}

test "read string column values" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/basic/basic_types_plain_uncompressed.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    // Read string_col (column index 5)
    // Expected: ["hello", "world", "", None, "🎉 unicode"]
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 5), rows.len);

    try std.testing.expectEqualStrings("hello", rows[0].getColumn(5).?.asBytes().?);
    try std.testing.expectEqualStrings("world", rows[1].getColumn(5).?.asBytes().?);
    try std.testing.expectEqualStrings("", rows[2].getColumn(5).?.asBytes().?);
    try std.testing.expect(rows[3].getColumn(5).?.isNull());
    try std.testing.expectEqualStrings("🎉 unicode", rows[4].getColumn(5).?.asBytes().?);
}

test "read bool column values" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/basic/basic_types_plain_uncompressed.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    // Read bool_col (column index 0)
    // Expected: [True, False, True, None, False]
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 5), rows.len);

    try std.testing.expectEqual(true, rows[0].getColumn(0).?.asBool().?);
    try std.testing.expectEqual(false, rows[1].getColumn(0).?.asBool().?);
    try std.testing.expectEqual(true, rows[2].getColumn(0).?.asBool().?);
    try std.testing.expect(rows[3].getColumn(0).?.isNull());
    try std.testing.expectEqual(false, rows[4].getColumn(0).?.asBool().?);
}

test "read binary column values" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/basic/basic_types_plain_uncompressed.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    // Read binary_col (column index 6)
    // Expected: [b"\x00\x01\x02", b"", None, b"\xff" * 100, b"test"]
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 5), rows.len);

    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x00, 0x01, 0x02 }, rows[0].getColumn(6).?.asBytes().?);
    try std.testing.expectEqualSlices(u8, &[_]u8{}, rows[1].getColumn(6).?.asBytes().?);
    try std.testing.expect(rows[2].getColumn(6).?.isNull());
    try std.testing.expectEqual(@as(usize, 100), rows[3].getColumn(6).?.asBytes().?.len);
    try std.testing.expectEqual(@as(u8, 0xff), rows[3].getColumn(6).?.asBytes().?[0]);
    try std.testing.expectEqualStrings("test", rows[4].getColumn(6).?.asBytes().?);
}

test "read fixed_len_byte_array column values" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/basic/basic_types_plain_uncompressed.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    // Read fixed_binary_col (column index 7) - 16-byte fixed length
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 5), rows.len);

    // uuid1
    const expected_uuid1 = [_]u8{ 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0 };
    try std.testing.expectEqualSlices(u8, &expected_uuid1, rows[0].getColumn(7).?.asBytes().?);

    // uuid2 - all zeros
    const expected_uuid2 = [_]u8{0} ** 16;
    try std.testing.expectEqualSlices(u8, &expected_uuid2, rows[1].getColumn(7).?.asBytes().?);

    // uuid3 - all 0xff
    const expected_uuid3 = [_]u8{0xff} ** 16;
    try std.testing.expectEqualSlices(u8, &expected_uuid3, rows[2].getColumn(7).?.asBytes().?);

    // null
    try std.testing.expect(rows[3].getColumn(7).?.isNull());

    // uuid4
    const expected_uuid4 = [_]u8{ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
    try std.testing.expectEqualSlices(u8, &expected_uuid4, rows[4].getColumn(7).?.asBytes().?);
}
