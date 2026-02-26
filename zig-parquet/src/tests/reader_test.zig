//! Reader integration tests
//!
//! Tests that read actual Parquet files and verify the data.

const std = @import("std");
const parquet = @import("../lib.zig");
const format = parquet.format;
const Reader = parquet.Reader;
const Optional = parquet.Optional;

test "read basic_types_plain_uncompressed.parquet" {
    const allocator = std.testing.allocator;

    // Open the test file
    const file = std.fs.cwd().openFile("../test-files-arrow/basic/basic_types_plain_uncompressed.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file (run from zig-parquet dir): {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Verify metadata
    try std.testing.expectEqual(@as(i64, 5), reader.metadata.num_rows);
    try std.testing.expectEqual(@as(usize, 1), reader.metadata.row_groups.len);

    // Schema should have 9 elements (1 root + 8 columns)
    try std.testing.expectEqual(@as(usize, 9), reader.metadata.schema.len);

    // Check column names
    const names = try reader.getColumnNames(allocator);
    defer allocator.free(names);

    try std.testing.expectEqual(@as(usize, 8), names.len);
    try std.testing.expectEqualStrings("bool_col", names[0]);
    try std.testing.expectEqualStrings("int32_col", names[1]);
    try std.testing.expectEqualStrings("int64_col", names[2]);
    try std.testing.expectEqualStrings("float_col", names[3]);
    try std.testing.expectEqualStrings("double_col", names[4]);
    try std.testing.expectEqualStrings("string_col", names[5]);
    try std.testing.expectEqualStrings("binary_col", names[6]);
    try std.testing.expectEqualStrings("fixed_binary_col", names[7]);

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

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Read int32_col (column index 1)
    // Expected: [1, -2, 2147483647, -2147483648, None]
    const values = try reader.readColumn(1, i32);
    defer allocator.free(values);

    try std.testing.expectEqual(@as(usize, 5), values.len);

    // Check values
    try std.testing.expectEqual(@as(i32, 1), values[0].value);
    try std.testing.expectEqual(@as(i32, -2), values[1].value);
    try std.testing.expectEqual(@as(i32, 2147483647), values[2].value);
    try std.testing.expectEqual(@as(i32, -2147483648), values[3].value);
    try std.testing.expectEqual(Optional(i32){ .null_value = {} }, values[4]);
}

test "read int64 column values" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/basic/basic_types_plain_uncompressed.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Read int64_col (column index 2)
    // Expected: [1, -2, 9223372036854775807, None, 0]
    const values = try reader.readColumn(2, i64);
    defer allocator.free(values);

    try std.testing.expectEqual(@as(usize, 5), values.len);

    try std.testing.expectEqual(@as(i64, 1), values[0].value);
    try std.testing.expectEqual(@as(i64, -2), values[1].value);
    try std.testing.expectEqual(@as(i64, 9223372036854775807), values[2].value);
    try std.testing.expectEqual(Optional(i64){ .null_value = {} }, values[3]);
    try std.testing.expectEqual(@as(i64, 0), values[4].value);
}

test "read float column values" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/basic/basic_types_plain_uncompressed.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Read float_col (column index 3)
    // Expected: [1.5, -2.5, inf, -inf, None]
    const values = try reader.readColumn(3, f32);
    defer allocator.free(values);

    try std.testing.expectEqual(@as(usize, 5), values.len);

    try std.testing.expectEqual(@as(f32, 1.5), values[0].value);
    try std.testing.expectEqual(@as(f32, -2.5), values[1].value);
    try std.testing.expect(std.math.isPositiveInf(values[2].value));
    try std.testing.expect(std.math.isNegativeInf(values[3].value));
    try std.testing.expectEqual(Optional(f32){ .null_value = {} }, values[4]);
}

test "read double column values" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/basic/basic_types_plain_uncompressed.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Read double_col (column index 4)
    // Expected: [1.5, -2.5, 1.7976931348623157e308, None, 0.0]
    const values = try reader.readColumn(4, f64);
    defer allocator.free(values);

    try std.testing.expectEqual(@as(usize, 5), values.len);

    try std.testing.expectEqual(@as(f64, 1.5), values[0].value);
    try std.testing.expectEqual(@as(f64, -2.5), values[1].value);
    try std.testing.expectEqual(@as(f64, 1.7976931348623157e308), values[2].value);
    try std.testing.expectEqual(Optional(f64){ .null_value = {} }, values[3]);
    try std.testing.expectEqual(@as(f64, 0.0), values[4].value);
}

test "read string column values" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/basic/basic_types_plain_uncompressed.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Read string_col (column index 5)
    // Expected: ["hello", "world", "", None, "🎉 unicode"]
    const values = try reader.readColumn(5, []const u8);
    defer {
        for (values) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(values);
    }

    try std.testing.expectEqual(@as(usize, 5), values.len);

    try std.testing.expectEqualStrings("hello", values[0].value);
    try std.testing.expectEqualStrings("world", values[1].value);
    try std.testing.expectEqualStrings("", values[2].value);
    try std.testing.expectEqual(Optional([]const u8){ .null_value = {} }, values[3]);
    try std.testing.expectEqualStrings("🎉 unicode", values[4].value);
}

test "read bool column values" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/basic/basic_types_plain_uncompressed.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Read bool_col (column index 0)
    // Expected: [True, False, True, None, False]
    const values = try reader.readColumn(0, bool);
    defer allocator.free(values);

    try std.testing.expectEqual(@as(usize, 5), values.len);

    try std.testing.expectEqual(true, values[0].value);
    try std.testing.expectEqual(false, values[1].value);
    try std.testing.expectEqual(true, values[2].value);
    try std.testing.expectEqual(Optional(bool){ .null_value = {} }, values[3]);
    try std.testing.expectEqual(false, values[4].value);
}

test "read binary column values" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/basic/basic_types_plain_uncompressed.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Read binary_col (column index 6)
    // Expected: [b"\x00\x01\x02", b"", None, b"\xff" * 100, b"test"]
    const values = try reader.readColumn(6, []const u8);
    defer {
        for (values) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(values);
    }

    try std.testing.expectEqual(@as(usize, 5), values.len);

    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x00, 0x01, 0x02 }, values[0].value);
    try std.testing.expectEqualSlices(u8, &[_]u8{}, values[1].value);
    try std.testing.expectEqual(Optional([]const u8){ .null_value = {} }, values[2]);
    try std.testing.expectEqual(@as(usize, 100), values[3].value.len);
    try std.testing.expectEqual(@as(u8, 0xff), values[3].value[0]);
    try std.testing.expectEqualStrings("test", values[4].value);
}

test "read fixed_len_byte_array column values" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/basic/basic_types_plain_uncompressed.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Read fixed_binary_col (column index 7) - 16-byte fixed length
    const values = try reader.readColumn(7, []const u8);
    defer {
        for (values) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(values);
    }

    try std.testing.expectEqual(@as(usize, 5), values.len);

    // uuid1
    const expected_uuid1 = [_]u8{ 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0 };
    try std.testing.expectEqualSlices(u8, &expected_uuid1, values[0].value);

    // uuid2 - all zeros
    const expected_uuid2 = [_]u8{0} ** 16;
    try std.testing.expectEqualSlices(u8, &expected_uuid2, values[1].value);

    // uuid3 - all 0xff
    const expected_uuid3 = [_]u8{0xff} ** 16;
    try std.testing.expectEqualSlices(u8, &expected_uuid3, values[2].value);

    // null
    try std.testing.expectEqual(Optional([]const u8){ .null_value = {} }, values[3]);

    // uuid4
    const expected_uuid4 = [_]u8{ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
    try std.testing.expectEqualSlices(u8, &expected_uuid4, values[4].value);
}
