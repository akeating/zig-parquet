//! Basic round-trip tests for the Parquet writer
//!
//! Tests write files using the Writer API and read them back with the Reader
//! to verify correctness for all physical types.

const std = @import("std");
const parquet = @import("../lib.zig");
const build_options = @import("build_options");

test "round-trip i64 column" {
    const allocator = std.testing.allocator;

    // Create temp file
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_i64.parquet";

    // Write
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "id", .type_ = .int64, .optional = false },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        const values = [_]i64{ 1, 2, 3, 4, 5, 100, -50, 0, 999999, -123456 };
        try writer.writeColumn(i64, 0, &values);
        try writer.close();
    }

    // Read back
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        // Verify metadata
        try std.testing.expectEqual(@as(i64, 10), reader.metadata.num_rows);
        try std.testing.expectEqual(@as(usize, 1), reader.metadata.row_groups.len);

        // Read column
        const col = try reader.readColumn(0, i64);
        defer allocator.free(col);

        try std.testing.expectEqual(@as(usize, 10), col.len);

        // Verify values
        const expected = [_]i64{ 1, 2, 3, 4, 5, 100, -50, 0, 999999, -123456 };
        for (col, 0..) |v, i| {
            switch (v) {
                .value => |val| try std.testing.expectEqual(expected[i], val),
                .null_value => return error.UnexpectedNull,
            }
        }
    }
}

test "round-trip nullable i64 column" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_nullable_i64.parquet";

    // Write
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "value", .type_ = .int64, .optional = true },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        const values = [_]?i64{ 1, null, 3, null, 5 };
        try writer.writeColumnNullable(i64, 0, &values);
        try writer.close();
    }

    // Read back
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 5), reader.metadata.num_rows);

        const col = try reader.readColumn(0, i64);
        defer allocator.free(col);

        try std.testing.expectEqual(@as(usize, 5), col.len);

        // Check values and nulls
        switch (col[0]) {
            .value => |v| try std.testing.expectEqual(@as(i64, 1), v),
            .null_value => return error.ExpectedValue,
        }
        switch (col[1]) {
            .value => return error.ExpectedNull,
            .null_value => {},
        }
        switch (col[2]) {
            .value => |v| try std.testing.expectEqual(@as(i64, 3), v),
            .null_value => return error.ExpectedValue,
        }
        switch (col[3]) {
            .value => return error.ExpectedNull,
            .null_value => {},
        }
        switch (col[4]) {
            .value => |v| try std.testing.expectEqual(@as(i64, 5), v),
            .null_value => return error.ExpectedValue,
        }
    }
}

test "round-trip double column" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_double.parquet";

    // Write
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "price", .type_ = .double, .optional = false },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        const values = [_]f64{ 1.5, 2.25, 3.125, -4.5, 0.0 };
        try writer.writeColumn(f64, 0, &values);
        try writer.close();
    }

    // Read back
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 5), reader.metadata.num_rows);

        const col = try reader.readColumn(0, f64);
        defer allocator.free(col);

        const expected = [_]f64{ 1.5, 2.25, 3.125, -4.5, 0.0 };
        for (col, 0..) |v, i| {
            switch (v) {
                .value => |val| try std.testing.expectApproxEqAbs(expected[i], val, 0.0001),
                .null_value => return error.UnexpectedNull,
            }
        }
    }
}

test "round-trip multiple columns" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_multi.parquet";

    // Write
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "id", .type_ = .int64, .optional = false },
            .{ .name = "value", .type_ = .double, .optional = true },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        const ids = [_]i64{ 1, 2, 3 };
        try writer.writeColumn(i64, 0, &ids);

        const values = [_]?f64{ 1.5, null, 3.5 };
        try writer.writeColumnNullable(f64, 1, &values);

        try writer.close();
    }

    // Read back
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 3), reader.metadata.num_rows);

        // Check column 0 (id)
        const col0 = try reader.readColumn(0, i64);
        defer allocator.free(col0);

        switch (col0[0]) {
            .value => |v| try std.testing.expectEqual(@as(i64, 1), v),
            .null_value => return error.UnexpectedNull,
        }
        switch (col0[1]) {
            .value => |v| try std.testing.expectEqual(@as(i64, 2), v),
            .null_value => return error.UnexpectedNull,
        }
        switch (col0[2]) {
            .value => |v| try std.testing.expectEqual(@as(i64, 3), v),
            .null_value => return error.UnexpectedNull,
        }

        // Check column 1 (value)
        const col1 = try reader.readColumn(1, f64);
        defer allocator.free(col1);

        switch (col1[0]) {
            .value => |v| try std.testing.expectApproxEqAbs(@as(f64, 1.5), v, 0.0001),
            .null_value => return error.ExpectedValue,
        }
        switch (col1[1]) {
            .value => return error.ExpectedNull,
            .null_value => {},
        }
        switch (col1[2]) {
            .value => |v| try std.testing.expectApproxEqAbs(@as(f64, 3.5), v, 0.0001),
            .null_value => return error.ExpectedValue,
        }
    }
}

test "round-trip i32 column" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_i32.parquet";

    // Write
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "count", .type_ = .int32, .optional = false },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        const values = [_]i32{ 1, -2, 3, 0, 2147483647, -2147483648 };
        try writer.writeColumn(i32, 0, &values);
        try writer.close();
    }

    // Read back
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 6), reader.metadata.num_rows);

        const col = try reader.readColumn(0, i32);
        defer allocator.free(col);

        const expected = [_]i32{ 1, -2, 3, 0, 2147483647, -2147483648 };
        for (col, 0..) |v, i| {
            switch (v) {
                .value => |val| try std.testing.expectEqual(expected[i], val),
                .null_value => return error.UnexpectedNull,
            }
        }
    }
}

test "round-trip bool column" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_bool.parquet";

    // Write
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "flag", .type_ = .boolean, .optional = false },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        const values = [_]bool{ true, false, true, true, false, false, true, false };
        try writer.writeColumn(bool, 0, &values);
        try writer.close();
    }

    // Read back
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 8), reader.metadata.num_rows);

        const col = try reader.readColumn(0, bool);
        defer allocator.free(col);

        const expected = [_]bool{ true, false, true, true, false, false, true, false };
        for (col, 0..) |v, i| {
            switch (v) {
                .value => |val| try std.testing.expectEqual(expected[i], val),
                .null_value => return error.UnexpectedNull,
            }
        }
    }
}

test "round-trip float column" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_float.parquet";

    // Write
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "temp", .type_ = .float, .optional = false },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        const values = [_]f32{ 1.5, -2.25, 0.0, 3.14159 };
        try writer.writeColumn(f32, 0, &values);
        try writer.close();
    }

    // Read back
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 4), reader.metadata.num_rows);

        const col = try reader.readColumn(0, f32);
        defer allocator.free(col);

        const expected = [_]f32{ 1.5, -2.25, 0.0, 3.14159 };
        for (col, 0..) |v, i| {
            switch (v) {
                .value => |val| try std.testing.expectApproxEqAbs(expected[i], val, 0.0001),
                .null_value => return error.UnexpectedNull,
            }
        }
    }
}

test "round-trip byte_array column" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_bytearray.parquet";

    // Write
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "name", .type_ = .byte_array, .optional = false },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        const values = [_][]const u8{ "hello", "world" };
        try writer.writeColumn([]const u8, 0, &values);
        try writer.close();
    }

    // Read back raw bytes to debug
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        // Read entire file
        const stat = try file.stat();
        const file_data = try allocator.alloc(u8, stat.size);
        defer allocator.free(file_data);
        _ = try file.readAll(file_data);

        // Verify magic bytes
        try std.testing.expectEqualStrings("PAR1", file_data[0..4]);

        // Check metadata - the footer should have the correct schema
        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 2), reader.metadata.num_rows);

        // Check schema type
        const schema_elem = reader.metadata.schema[1];
        try std.testing.expectEqual(parquet.format.PhysicalType.byte_array, schema_elem.type_.?);
        try std.testing.expectEqual(parquet.format.RepetitionType.required, schema_elem.repetition_type.?);

        // Get the column chunk metadata
        const chunk = &reader.metadata.row_groups[0].columns[0];
        const meta = chunk.meta_data.?;

        // Verify offsets make sense
        try std.testing.expect(meta.data_page_offset >= 4); // After PAR1
        try std.testing.expect(meta.total_compressed_size > 0);

        // Now read the column
        const col = try reader.readColumn(0, []const u8);
        defer {
            for (col) |v| {
                switch (v) {
                    .value => |val| allocator.free(val),
                    .null_value => {},
                }
            }
            allocator.free(col);
        }

        try std.testing.expectEqual(@as(usize, 2), col.len);

        // Check values
        switch (col[0]) {
            .value => |val| try std.testing.expectEqualStrings("hello", val),
            .null_value => return error.UnexpectedNull,
        }
        switch (col[1]) {
            .value => |val| try std.testing.expectEqualStrings("world", val),
            .null_value => return error.UnexpectedNull,
        }
    }
}

test "round-trip fixed_len_byte_array column" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_fixed.parquet";

    // Write
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "uuid", .type_ = .fixed_len_byte_array, .optional = false, .type_length = 4 },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        const values = [_][]const u8{ "ABCD", "1234", "test" };
        try writer.writeColumnFixedByteArray(0, &values);
        try writer.close();
    }

    // Read back
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 3), reader.metadata.num_rows);

        const col = try reader.readColumn(0, []const u8);
        defer {
            for (col) |v| {
                switch (v) {
                    .value => |val| allocator.free(val),
                    .null_value => {},
                }
            }
            allocator.free(col);
        }

        const expected = [_][]const u8{ "ABCD", "1234", "test" };
        for (col, 0..) |v, i| {
            switch (v) {
                .value => |val| try std.testing.expectEqualStrings(expected[i], val),
                .null_value => return error.UnexpectedNull,
            }
        }
    }
}

test "dictionary cardinality fallback" {
    if (build_options.no_compression) return;
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "dictionary_cardinality_fallback.parquet";

    // Write using DynamicWriter with high-cardinality data
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        var writer = try parquet.createFileDynamic(allocator, file);
        defer writer.deinit();

        try writer.addColumn("id", parquet.TypeInfo.int64, .{});
        try writer.begin();

        for (0..2000) |i| {
            try writer.setInt64(0, @as(i64, @intCast(i)));
            try writer.addRow();
        }

        try writer.close();
    }

    // Read back
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        // Verify metadata
        try std.testing.expectEqual(@as(i64, 2000), reader.metadata.num_rows);

        const col = try reader.readColumn(0, i64);
        defer allocator.free(col);

        try std.testing.expectEqual(@as(usize, 2000), col.len);

        for (col, 0..) |v, i| {
            switch (v) {
                .value => |val| try std.testing.expectEqual(@as(i64, @intCast(i)), val),
                .null_value => return error.UnexpectedNull,
            }
        }
    }
}
