//! Tests for struct column support
//!
//! Tests reading and writing struct columns from Parquet files.

const std = @import("std");
const io = std.testing.io;
const parquet = @import("../lib.zig");

// =============================================================================
// Read Tests
// =============================================================================

test "read struct_simple.parquet" {
    const allocator = std.testing.allocator;

    // Open the test file
    const file = std.Io.Dir.cwd().openFile(io, "../test-files-arrow/nested/struct_simple.parquet", .{}) catch |err| {
        std.debug.print("Skipping test - file not found: {}\n", .{err});
        return;
    };
    defer file.close(io);

    var reader = try parquet.openFileDynamic(allocator, file, io, .{});
    defer reader.deinit();

    // Verify schema
    const schema = reader.getSchema();
    try std.testing.expect(schema.len > 0);

    // Schema should be: point (struct with x, y, name)
    // Top-level column 0: point

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    // Expected data from generate.py:
    // Row 0: {x: 1, y: 2, name: "A"}
    // Row 1: {x: 3, y: 4, name: "B"}
    // Row 2: null (entire struct is null)
    // Row 3: {x: 5, y: 6, name: "C"}
    // Row 4: {x: 7, y: 8, name: null} (null field)

    try std.testing.expectEqual(@as(usize, 5), rows.len);

    // Row 0: {x: 1, y: 2, name: "A"}
    const point0 = rows[0].getColumn(0).?;
    try std.testing.expect(!point0.isNull());
    try std.testing.expectEqual(@as(i32, 1), point0.getField("x").?.asInt32().?);
    try std.testing.expectEqual(@as(i32, 2), point0.getField("y").?.asInt32().?);
    try std.testing.expectEqualStrings("A", point0.getField("name").?.asBytes().?);

    // Row 1: {x: 3, y: 4, name: "B"}
    const point1 = rows[1].getColumn(0).?;
    try std.testing.expect(!point1.isNull());
    try std.testing.expectEqual(@as(i32, 3), point1.getField("x").?.asInt32().?);
    try std.testing.expectEqual(@as(i32, 4), point1.getField("y").?.asInt32().?);
    try std.testing.expectEqualStrings("B", point1.getField("name").?.asBytes().?);

    // Row 2: null (entire struct is null - all fields should be null)
    const point2 = rows[2].getColumn(0).?;
    try std.testing.expect(point2.isNull());

    // Row 3: {x: 5, y: 6, name: "C"}
    const point3 = rows[3].getColumn(0).?;
    try std.testing.expect(!point3.isNull());
    try std.testing.expectEqual(@as(i32, 5), point3.getField("x").?.asInt32().?);
    try std.testing.expectEqual(@as(i32, 6), point3.getField("y").?.asInt32().?);
    try std.testing.expectEqualStrings("C", point3.getField("name").?.asBytes().?);

    // Row 4: {x: 7, y: 8, name: null}
    const point4 = rows[4].getColumn(0).?;
    try std.testing.expect(!point4.isNull());
    try std.testing.expectEqual(@as(i32, 7), point4.getField("x").?.asInt32().?);
    try std.testing.expectEqual(@as(i32, 8), point4.getField("y").?.asInt32().?);
    try std.testing.expect(point4.getField("name").?.isNull()); // null field
}

test "read struct_nested.parquet" {
    const allocator = std.testing.allocator;

    // Open the test file
    const file = std.Io.Dir.cwd().openFile(io, "../test-files-arrow/nested/struct_nested.parquet", .{}) catch |err| {
        std.debug.print("Skipping test - file not found: {}\n", .{err});
        return;
    };
    defer file.close(io);

    var reader = try parquet.openFileDynamic(allocator, file, io, .{});
    defer reader.deinit();

    // Schema: nested (struct with inner (struct with a, b), value (string))
    // Top-level column 0: nested

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    // Expected data from generate.py:
    // Row 0: {inner: {a: 1, b: 2}, value: "first"}
    // Row 1: {inner: {a: 3, b: 4}, value: "second"}
    // Row 2: {inner: null, value: "null_inner"}
    // Row 3: null (entire struct is null)
    // Row 4: {inner: {a: 5, b: 6}, value: null}

    try std.testing.expectEqual(@as(usize, 5), rows.len);

    // Row 0: {inner: {a: 1, b: 2}, value: "first"}
    const nested0 = rows[0].getColumn(0).?;
    try std.testing.expect(!nested0.isNull());
    const inner0 = nested0.getField("inner").?;
    try std.testing.expect(!inner0.isNull());
    try std.testing.expectEqual(@as(i32, 1), inner0.getField("a").?.asInt32().?);
    try std.testing.expectEqual(@as(i32, 2), inner0.getField("b").?.asInt32().?);
    try std.testing.expectEqualStrings("first", nested0.getField("value").?.asBytes().?);

    // Row 1: {inner: {a: 3, b: 4}, value: "second"}
    const nested1 = rows[1].getColumn(0).?;
    try std.testing.expect(!nested1.isNull());
    const inner1 = nested1.getField("inner").?;
    try std.testing.expect(!inner1.isNull());
    try std.testing.expectEqual(@as(i32, 3), inner1.getField("a").?.asInt32().?);
    try std.testing.expectEqual(@as(i32, 4), inner1.getField("b").?.asInt32().?);
    try std.testing.expectEqualStrings("second", nested1.getField("value").?.asBytes().?);

    // Row 2: {inner: null, value: "null_inner"} - inner struct is null
    const nested2 = rows[2].getColumn(0).?;
    try std.testing.expect(!nested2.isNull());
    try std.testing.expect(nested2.getField("inner").?.isNull());
    try std.testing.expectEqualStrings("null_inner", nested2.getField("value").?.asBytes().?);

    // Row 3: null (entire nested struct is null)
    const nested3 = rows[3].getColumn(0).?;
    try std.testing.expect(nested3.isNull());

    // Row 4: {inner: {a: 5, b: 6}, value: null}
    const nested4 = rows[4].getColumn(0).?;
    try std.testing.expect(!nested4.isNull());
    const inner4 = nested4.getField("inner").?;
    try std.testing.expect(!inner4.isNull());
    try std.testing.expectEqual(@as(i32, 5), inner4.getField("a").?.asInt32().?);
    try std.testing.expectEqual(@as(i32, 6), inner4.getField("b").?.asInt32().?);
    try std.testing.expect(nested4.getField("value").?.isNull()); // null field
}

// =============================================================================
// Round-trip Tests
// =============================================================================

const Writer = parquet.Writer;
const ColumnDef = parquet.ColumnDef;
const StructField = parquet.StructField;

test "write and read simple struct" {
    const allocator = std.testing.allocator;

    // Create a temp file
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "struct_roundtrip.parquet";

    // Write a struct with two i32 fields
    {
        const file = try tmp_dir.dir.createFile(io, file_path, .{});
        defer file.close(io);

        const columns = [_]ColumnDef{
            ColumnDef.struct_("point", &.{
                .{ .name = "x", .type_ = .int32, .optional = true },
                .{ .name = "y", .type_ = .int32, .optional = true },
            }, true),
        };

        var writer = try parquet.writeToFile(allocator, file, io, &columns);
        defer writer.deinit();

        // Write point.x: values [1, 3, null, 5]
        // parent_nulls: [false, false, true, false]
        const x_values = [_]?i32{ 1, 3, null, 5 };
        const parent_nulls = [_]bool{ false, false, true, false };
        try writer.writeStructField(i32, 0, 0, &x_values, &parent_nulls);

        // Write point.y: values [2, 4, null, 6]
        const y_values = [_]?i32{ 2, 4, null, 6 };
        try writer.writeStructField(i32, 0, 1, &y_values, &parent_nulls);

        try writer.close();
    }

    // Read it back
    {
        const file = try tmp_dir.dir.openFile(io, file_path, .{});
        defer file.close(io);

        var reader = try parquet.openFileDynamic(allocator, file, io, .{});
        defer reader.deinit();

        const rows = try reader.readAllRows(0);
        defer {
            for (rows) |row| row.deinit();
            allocator.free(rows);
        }

        try std.testing.expectEqual(@as(usize, 4), rows.len);

        // Row 0: {x: 1, y: 2}
        const point0 = rows[0].getColumn(0).?;
        try std.testing.expect(!point0.isNull());
        try std.testing.expectEqual(@as(i32, 1), point0.getField("x").?.asInt32().?);
        try std.testing.expectEqual(@as(i32, 2), point0.getField("y").?.asInt32().?);

        // Row 1: {x: 3, y: 4}
        const point1 = rows[1].getColumn(0).?;
        try std.testing.expect(!point1.isNull());
        try std.testing.expectEqual(@as(i32, 3), point1.getField("x").?.asInt32().?);
        try std.testing.expectEqual(@as(i32, 4), point1.getField("y").?.asInt32().?);

        // Row 2: null struct
        try std.testing.expect(rows[2].getColumn(0).?.isNull());

        // Row 3: {x: 5, y: 6}
        const point3 = rows[3].getColumn(0).?;
        try std.testing.expect(!point3.isNull());
        try std.testing.expectEqual(@as(i32, 5), point3.getField("x").?.asInt32().?);
        try std.testing.expectEqual(@as(i32, 6), point3.getField("y").?.asInt32().?);
    }
}

test "write and read struct with null fields" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "struct_null_fields.parquet";

    // Write a struct where some individual fields are null
    {
        const file = try tmp_dir.dir.createFile(io, file_path, .{});
        defer file.close(io);

        const columns = [_]ColumnDef{
            ColumnDef.struct_("data", &.{
                .{ .name = "a", .type_ = .int32, .optional = true },
                .{ .name = "b", .type_ = .int32, .optional = true },
            }, true),
        };

        var writer = try parquet.writeToFile(allocator, file, io, &columns);
        defer writer.deinit();

        // Row 0: {a: 1, b: 2} - both present
        // Row 1: {a: 3, b: null} - b is null
        // Row 2: {a: null, b: 4} - a is null
        // Row 3: {a: null, b: null} - both null but struct is present
        const a_values = [_]?i32{ 1, 3, null, null };
        const b_values = [_]?i32{ 2, null, 4, null };
        const parent_nulls = [_]bool{ false, false, false, false };

        try writer.writeStructField(i32, 0, 0, &a_values, &parent_nulls);
        try writer.writeStructField(i32, 0, 1, &b_values, &parent_nulls);

        try writer.close();
    }

    // Read it back
    {
        const file = try tmp_dir.dir.openFile(io, file_path, .{});
        defer file.close(io);

        var reader = try parquet.openFileDynamic(allocator, file, io, .{});
        defer reader.deinit();

        const rows = try reader.readAllRows(0);
        defer {
            for (rows) |row| row.deinit();
            allocator.free(rows);
        }

        try std.testing.expectEqual(@as(usize, 4), rows.len);

        // Row 0: {a: 1, b: 2}
        const data0 = rows[0].getColumn(0).?;
        try std.testing.expect(!data0.getField("a").?.isNull());
        try std.testing.expectEqual(@as(i32, 1), data0.getField("a").?.asInt32().?);
        try std.testing.expect(!data0.getField("b").?.isNull());
        try std.testing.expectEqual(@as(i32, 2), data0.getField("b").?.asInt32().?);

        // Row 1: {a: 3, b: null}
        const data1 = rows[1].getColumn(0).?;
        try std.testing.expect(!data1.getField("a").?.isNull());
        try std.testing.expectEqual(@as(i32, 3), data1.getField("a").?.asInt32().?);
        try std.testing.expect(data1.getField("b").?.isNull());

        // Row 2: {a: null, b: 4}
        const data2 = rows[2].getColumn(0).?;
        try std.testing.expect(data2.getField("a").?.isNull());
        try std.testing.expect(!data2.getField("b").?.isNull());
        try std.testing.expectEqual(@as(i32, 4), data2.getField("b").?.asInt32().?);

        // Row 3: {a: null, b: null}
        const data3 = rows[3].getColumn(0).?;
        try std.testing.expect(data3.getField("a").?.isNull());
        try std.testing.expect(data3.getField("b").?.isNull());
    }
}

test "write and read struct with string field" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "struct_string.parquet";

    // Write a struct with int and string fields
    {
        const file = try tmp_dir.dir.createFile(io, file_path, .{});
        defer file.close(io);

        const columns = [_]ColumnDef{
            ColumnDef.struct_("person", &.{
                .{ .name = "id", .type_ = .int32, .optional = true },
                .{ .name = "name", .type_ = .byte_array, .optional = true },
            }, true),
        };

        var writer = try parquet.writeToFile(allocator, file, io, &columns);
        defer writer.deinit();

        // Row 0: {id: 1, name: "Alice"}
        // Row 1: null struct
        // Row 2: {id: 2, name: null}
        const id_values = [_]?i32{ 1, null, 2 };
        const name_values = [_]?[]const u8{ "Alice", null, null };
        const parent_nulls = [_]bool{ false, true, false };

        try writer.writeStructField(i32, 0, 0, &id_values, &parent_nulls);
        try writer.writeStructField([]const u8, 0, 1, &name_values, &parent_nulls);

        try writer.close();
    }

    // Read it back
    {
        const file = try tmp_dir.dir.openFile(io, file_path, .{});
        defer file.close(io);

        var reader = try parquet.openFileDynamic(allocator, file, io, .{});
        defer reader.deinit();

        const rows = try reader.readAllRows(0);
        defer {
            for (rows) |row| row.deinit();
            allocator.free(rows);
        }

        try std.testing.expectEqual(@as(usize, 3), rows.len);

        // Row 0: {id: 1, name: "Alice"}
        const person0 = rows[0].getColumn(0).?;
        try std.testing.expect(!person0.getField("id").?.isNull());
        try std.testing.expectEqual(@as(i32, 1), person0.getField("id").?.asInt32().?);
        try std.testing.expect(!person0.getField("name").?.isNull());
        try std.testing.expectEqualStrings("Alice", person0.getField("name").?.asBytes().?);

        // Row 1: null struct
        try std.testing.expect(rows[1].getColumn(0).?.isNull());

        // Row 2: {id: 2, name: null}
        const person2 = rows[2].getColumn(0).?;
        try std.testing.expect(!person2.getField("id").?.isNull());
        try std.testing.expectEqual(@as(i32, 2), person2.getField("id").?.asInt32().?);
        try std.testing.expect(person2.getField("name").?.isNull());
    }
}
