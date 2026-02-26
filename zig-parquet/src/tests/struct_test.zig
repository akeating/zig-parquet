//! Tests for struct column support
//!
//! Tests reading and writing struct columns from Parquet files.

const std = @import("std");
const parquet = @import("../lib.zig");

const Reader = parquet.Reader;
const Optional = parquet.Optional;

// =============================================================================
// Read Tests
// =============================================================================

test "read struct_simple.parquet" {
    const allocator = std.testing.allocator;

    // Open the test file
    const file = std.fs.cwd().openFile("../test-files-arrow/nested/struct_simple.parquet", .{}) catch |err| {
        std.debug.print("Skipping test - file not found: {}\n", .{err});
        return;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Verify schema
    const schema = reader.getSchema();
    try std.testing.expect(schema.len > 0);

    // Schema should be: point (struct with x, y, name)
    // Physical columns: point.x (0), point.y (1), point.name (2)

    // Read column 0: point.x (i32)
    const col_x = try reader.readColumn(0, i32);
    defer allocator.free(col_x);

    // Read column 1: point.y (i32)
    const col_y = try reader.readColumn(1, i32);
    defer allocator.free(col_y);

    // Read column 2: point.name (string)
    const col_name = try reader.readColumn(2, []const u8);
    defer {
        for (col_name) |opt| {
            if (!opt.isNull()) {
                allocator.free(opt.value);
            }
        }
        allocator.free(col_name);
    }

    // Expected data from generate.py:
    // Row 0: {x: 1, y: 2, name: "A"}
    // Row 1: {x: 3, y: 4, name: "B"}
    // Row 2: null (entire struct is null)
    // Row 3: {x: 5, y: 6, name: "C"}
    // Row 4: {x: 7, y: 8, name: null} (null field)

    try std.testing.expectEqual(@as(usize, 5), col_x.len);
    try std.testing.expectEqual(@as(usize, 5), col_y.len);
    try std.testing.expectEqual(@as(usize, 5), col_name.len);

    // Row 0: {x: 1, y: 2, name: "A"}
    try std.testing.expect(!col_x[0].isNull());
    try std.testing.expectEqual(@as(i32, 1), col_x[0].value);
    try std.testing.expect(!col_y[0].isNull());
    try std.testing.expectEqual(@as(i32, 2), col_y[0].value);
    try std.testing.expect(!col_name[0].isNull());
    try std.testing.expectEqualStrings("A", col_name[0].value);

    // Row 1: {x: 3, y: 4, name: "B"}
    try std.testing.expect(!col_x[1].isNull());
    try std.testing.expectEqual(@as(i32, 3), col_x[1].value);
    try std.testing.expect(!col_y[1].isNull());
    try std.testing.expectEqual(@as(i32, 4), col_y[1].value);
    try std.testing.expect(!col_name[1].isNull());
    try std.testing.expectEqualStrings("B", col_name[1].value);

    // Row 2: null (entire struct is null - all fields should be null)
    try std.testing.expect(col_x[2].isNull());
    try std.testing.expect(col_y[2].isNull());
    try std.testing.expect(col_name[2].isNull());

    // Row 3: {x: 5, y: 6, name: "C"}
    try std.testing.expect(!col_x[3].isNull());
    try std.testing.expectEqual(@as(i32, 5), col_x[3].value);
    try std.testing.expect(!col_y[3].isNull());
    try std.testing.expectEqual(@as(i32, 6), col_y[3].value);
    try std.testing.expect(!col_name[3].isNull());
    try std.testing.expectEqualStrings("C", col_name[3].value);

    // Row 4: {x: 7, y: 8, name: null}
    try std.testing.expect(!col_x[4].isNull());
    try std.testing.expectEqual(@as(i32, 7), col_x[4].value);
    try std.testing.expect(!col_y[4].isNull());
    try std.testing.expectEqual(@as(i32, 8), col_y[4].value);
    try std.testing.expect(col_name[4].isNull()); // null field
}

test "read struct_nested.parquet" {
    const allocator = std.testing.allocator;

    // Open the test file
    const file = std.fs.cwd().openFile("../test-files-arrow/nested/struct_nested.parquet", .{}) catch |err| {
        std.debug.print("Skipping test - file not found: {}\n", .{err});
        return;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Schema: nested (struct with inner (struct with a, b), value (string))
    // Physical columns: nested.inner.a (0), nested.inner.b (1), nested.value (2)
    //
    // For nested.inner.a/b: max_def_level = 3
    //   def=0: nested is null
    //   def=1: nested.inner is null
    //   def=2: inner is present but field is null
    //   def=3: field is present
    //
    // For nested.value: max_def_level = 2
    //   def=0: nested is null
    //   def=1: nested.value is null
    //   def=2: value is present

    // Read column 0: nested.inner.a (i32)
    const col_a = try reader.readColumn(0, i32);
    defer allocator.free(col_a);

    // Read column 1: nested.inner.b (i32)
    const col_b = try reader.readColumn(1, i32);
    defer allocator.free(col_b);

    // Read column 2: nested.value (string)
    const col_value = try reader.readColumn(2, []const u8);
    defer {
        for (col_value) |opt| {
            if (!opt.isNull()) {
                allocator.free(opt.value);
            }
        }
        allocator.free(col_value);
    }

    // Expected data from generate.py:
    // Row 0: {inner: {a: 1, b: 2}, value: "first"}
    // Row 1: {inner: {a: 3, b: 4}, value: "second"}
    // Row 2: {inner: null, value: "null_inner"}
    // Row 3: null (entire struct is null)
    // Row 4: {inner: {a: 5, b: 6}, value: null}

    try std.testing.expectEqual(@as(usize, 5), col_a.len);
    try std.testing.expectEqual(@as(usize, 5), col_b.len);
    try std.testing.expectEqual(@as(usize, 5), col_value.len);

    // Row 0: {inner: {a: 1, b: 2}, value: "first"}
    try std.testing.expect(!col_a[0].isNull());
    try std.testing.expectEqual(@as(i32, 1), col_a[0].value);
    try std.testing.expect(!col_b[0].isNull());
    try std.testing.expectEqual(@as(i32, 2), col_b[0].value);
    try std.testing.expect(!col_value[0].isNull());
    try std.testing.expectEqualStrings("first", col_value[0].value);

    // Row 1: {inner: {a: 3, b: 4}, value: "second"}
    try std.testing.expect(!col_a[1].isNull());
    try std.testing.expectEqual(@as(i32, 3), col_a[1].value);
    try std.testing.expect(!col_b[1].isNull());
    try std.testing.expectEqual(@as(i32, 4), col_b[1].value);
    try std.testing.expect(!col_value[1].isNull());
    try std.testing.expectEqualStrings("second", col_value[1].value);

    // Row 2: {inner: null, value: "null_inner"} - inner struct is null
    try std.testing.expect(col_a[2].isNull()); // inner is null, so a is null
    try std.testing.expect(col_b[2].isNull()); // inner is null, so b is null
    try std.testing.expect(!col_value[2].isNull());
    try std.testing.expectEqualStrings("null_inner", col_value[2].value);

    // Row 3: null (entire nested struct is null)
    try std.testing.expect(col_a[3].isNull());
    try std.testing.expect(col_b[3].isNull());
    try std.testing.expect(col_value[3].isNull());

    // Row 4: {inner: {a: 5, b: 6}, value: null}
    try std.testing.expect(!col_a[4].isNull());
    try std.testing.expectEqual(@as(i32, 5), col_a[4].value);
    try std.testing.expect(!col_b[4].isNull());
    try std.testing.expectEqual(@as(i32, 6), col_b[4].value);
    try std.testing.expect(col_value[4].isNull()); // null field
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
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]ColumnDef{
            ColumnDef.struct_("point", &.{
                .{ .name = "x", .type_ = .int32, .optional = true },
                .{ .name = "y", .type_ = .int32, .optional = true },
            }, true),
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
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
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        // Read x column
        const col_x = try reader.readColumn(0, i32);
        defer allocator.free(col_x);

        // Read y column
        const col_y = try reader.readColumn(1, i32);
        defer allocator.free(col_y);

        try std.testing.expectEqual(@as(usize, 4), col_x.len);
        try std.testing.expectEqual(@as(usize, 4), col_y.len);

        // Row 0: {x: 1, y: 2}
        try std.testing.expect(!col_x[0].isNull());
        try std.testing.expectEqual(@as(i32, 1), col_x[0].value);
        try std.testing.expect(!col_y[0].isNull());
        try std.testing.expectEqual(@as(i32, 2), col_y[0].value);

        // Row 1: {x: 3, y: 4}
        try std.testing.expect(!col_x[1].isNull());
        try std.testing.expectEqual(@as(i32, 3), col_x[1].value);
        try std.testing.expect(!col_y[1].isNull());
        try std.testing.expectEqual(@as(i32, 4), col_y[1].value);

        // Row 2: null struct
        try std.testing.expect(col_x[2].isNull());
        try std.testing.expect(col_y[2].isNull());

        // Row 3: {x: 5, y: 6}
        try std.testing.expect(!col_x[3].isNull());
        try std.testing.expectEqual(@as(i32, 5), col_x[3].value);
        try std.testing.expect(!col_y[3].isNull());
        try std.testing.expectEqual(@as(i32, 6), col_y[3].value);
    }
}

test "write and read struct with null fields" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "struct_null_fields.parquet";

    // Write a struct where some individual fields are null
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]ColumnDef{
            ColumnDef.struct_("data", &.{
                .{ .name = "a", .type_ = .int32, .optional = true },
                .{ .name = "b", .type_ = .int32, .optional = true },
            }, true),
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
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
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        const col_a = try reader.readColumn(0, i32);
        defer allocator.free(col_a);

        const col_b = try reader.readColumn(1, i32);
        defer allocator.free(col_b);

        try std.testing.expectEqual(@as(usize, 4), col_a.len);

        // Row 0: {a: 1, b: 2}
        try std.testing.expect(!col_a[0].isNull());
        try std.testing.expectEqual(@as(i32, 1), col_a[0].value);
        try std.testing.expect(!col_b[0].isNull());
        try std.testing.expectEqual(@as(i32, 2), col_b[0].value);

        // Row 1: {a: 3, b: null}
        try std.testing.expect(!col_a[1].isNull());
        try std.testing.expectEqual(@as(i32, 3), col_a[1].value);
        try std.testing.expect(col_b[1].isNull());

        // Row 2: {a: null, b: 4}
        try std.testing.expect(col_a[2].isNull());
        try std.testing.expect(!col_b[2].isNull());
        try std.testing.expectEqual(@as(i32, 4), col_b[2].value);

        // Row 3: {a: null, b: null}
        try std.testing.expect(col_a[3].isNull());
        try std.testing.expect(col_b[3].isNull());
    }
}

test "write and read struct with string field" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "struct_string.parquet";

    // Write a struct with int and string fields
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]ColumnDef{
            ColumnDef.struct_("person", &.{
                .{ .name = "id", .type_ = .int32, .optional = true },
                .{ .name = "name", .type_ = .byte_array, .optional = true },
            }, true),
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
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
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        const col_id = try reader.readColumn(0, i32);
        defer allocator.free(col_id);

        const col_name = try reader.readColumn(1, []const u8);
        defer {
            for (col_name) |opt| {
                if (!opt.isNull()) {
                    allocator.free(opt.value);
                }
            }
            allocator.free(col_name);
        }

        try std.testing.expectEqual(@as(usize, 3), col_id.len);

        // Row 0: {id: 1, name: "Alice"}
        try std.testing.expect(!col_id[0].isNull());
        try std.testing.expectEqual(@as(i32, 1), col_id[0].value);
        try std.testing.expect(!col_name[0].isNull());
        try std.testing.expectEqualStrings("Alice", col_name[0].value);

        // Row 1: null struct
        try std.testing.expect(col_id[1].isNull());
        try std.testing.expect(col_name[1].isNull());

        // Row 2: {id: 2, name: null}
        try std.testing.expect(!col_id[2].isNull());
        try std.testing.expectEqual(@as(i32, 2), col_id[2].value);
        try std.testing.expect(col_name[2].isNull());
    }
}
