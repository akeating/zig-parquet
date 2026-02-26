//! Tests for list column support
//!
//! Tests reading and writing list columns from Parquet files.

const std = @import("std");
const parquet = @import("../lib.zig");

const Reader = parquet.Reader;
const Optional = parquet.Optional;

// =============================================================================
// Read Tests
// =============================================================================

test "read list_int.parquet" {
    const allocator = std.testing.allocator;

    // Open the test file
    const file = std.fs.cwd().openFile("../test-files-arrow/nested/list_int.parquet", .{}) catch |err| {
        std.debug.print("Skipping test - file not found: {}\n", .{err});
        return;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Verify schema
    const schema = reader.getSchema();
    try std.testing.expect(schema.len > 0);

    // Read the list column
    const lists = try reader.readListColumn(0, i32);
    defer reader.freeListColumn(i32, lists);

    // Expected data: [[1, 2, 3], [4, 5], [6], [], [7, 8, 9, 10]]
    try std.testing.expectEqual(@as(usize, 5), lists.len);

    // Row 0: [1, 2, 3]
    try std.testing.expect(!lists[0].isNull());
    const row0 = lists[0].value;
    try std.testing.expectEqual(@as(usize, 3), row0.len);
    try std.testing.expectEqual(@as(i32, 1), row0[0].value);
    try std.testing.expectEqual(@as(i32, 2), row0[1].value);
    try std.testing.expectEqual(@as(i32, 3), row0[2].value);

    // Row 1: [4, 5]
    try std.testing.expect(!lists[1].isNull());
    const row1 = lists[1].value;
    try std.testing.expectEqual(@as(usize, 2), row1.len);
    try std.testing.expectEqual(@as(i32, 4), row1[0].value);
    try std.testing.expectEqual(@as(i32, 5), row1[1].value);

    // Row 2: [6]
    try std.testing.expect(!lists[2].isNull());
    const row2 = lists[2].value;
    try std.testing.expectEqual(@as(usize, 1), row2.len);
    try std.testing.expectEqual(@as(i32, 6), row2[0].value);

    // Row 3: [] (empty list)
    try std.testing.expect(!lists[3].isNull());
    const row3 = lists[3].value;
    try std.testing.expectEqual(@as(usize, 0), row3.len);

    // Row 4: [7, 8, 9, 10]
    try std.testing.expect(!lists[4].isNull());
    const row4 = lists[4].value;
    try std.testing.expectEqual(@as(usize, 4), row4.len);
    try std.testing.expectEqual(@as(i32, 7), row4[0].value);
    try std.testing.expectEqual(@as(i32, 8), row4[1].value);
    try std.testing.expectEqual(@as(i32, 9), row4[2].value);
    try std.testing.expectEqual(@as(i32, 10), row4[3].value);
}

test "read list_nullable.parquet" {
    const allocator = std.testing.allocator;

    // Open the test file
    const file = std.fs.cwd().openFile("../test-files-arrow/nested/list_nullable.parquet", .{}) catch |err| {
        std.debug.print("Skipping test - file not found: {}\n", .{err});
        return;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Read the list column
    const lists = try reader.readListColumn(0, i32);
    defer reader.freeListColumn(i32, lists);

    // Expected data: [[1, null, 3], null, [4, 5], [], [null, null]]
    try std.testing.expectEqual(@as(usize, 5), lists.len);

    // Row 0: [1, null, 3]
    try std.testing.expect(!lists[0].isNull());
    const row0 = lists[0].value;
    try std.testing.expectEqual(@as(usize, 3), row0.len);
    try std.testing.expectEqual(@as(i32, 1), row0[0].value);
    try std.testing.expect(row0[1].isNull());
    try std.testing.expectEqual(@as(i32, 3), row0[2].value);

    // Row 1: null
    try std.testing.expect(lists[1].isNull());

    // Row 2: [4, 5]
    try std.testing.expect(!lists[2].isNull());
    const row2 = lists[2].value;
    try std.testing.expectEqual(@as(usize, 2), row2.len);
    try std.testing.expectEqual(@as(i32, 4), row2[0].value);
    try std.testing.expectEqual(@as(i32, 5), row2[1].value);

    // Row 3: [] (empty list)
    try std.testing.expect(!lists[3].isNull());
    const row3 = lists[3].value;
    try std.testing.expectEqual(@as(usize, 0), row3.len);

    // Row 4: [null, null]
    try std.testing.expect(!lists[4].isNull());
    const row4 = lists[4].value;
    try std.testing.expectEqual(@as(usize, 2), row4.len);
    try std.testing.expect(row4[0].isNull());
    try std.testing.expect(row4[1].isNull());
}

// =============================================================================
// Write Round-Trip Tests
// =============================================================================

test "write and read list of i32" {
    const allocator = std.testing.allocator;

    // Create temp file
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file = try tmp_dir.dir.createFile("list_roundtrip.parquet", .{ .read = true });
    defer file.close();

    // Define a list column
    const columns = [_]parquet.ColumnDef{
        parquet.ColumnDef.listInt32("numbers", true),
    };

    // Write data: [[1, 2, 3], [4, 5]]
    const inner0 = [_]Optional(i32){ .{ .value = 1 }, .{ .value = 2 }, .{ .value = 3 } };
    const inner1 = [_]Optional(i32){ .{ .value = 4 }, .{ .value = 5 } };
    const lists = [_]Optional([]const Optional(i32)){
        .{ .value = &inner0 },
        .{ .value = &inner1 },
    };

    {
        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        try writer.writeListColumnI32(0, &lists);
        try writer.close();
    }

    // Read it back
    try file.seekTo(0);
    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    const read_lists = try reader.readListColumn(0, i32);
    defer reader.freeListColumn(i32, read_lists);

    // Verify
    try std.testing.expectEqual(@as(usize, 2), read_lists.len);

    // Row 0: [1, 2, 3]
    try std.testing.expect(!read_lists[0].isNull());
    const row0 = read_lists[0].value;
    try std.testing.expectEqual(@as(usize, 3), row0.len);
    try std.testing.expectEqual(@as(i32, 1), row0[0].value);
    try std.testing.expectEqual(@as(i32, 2), row0[1].value);
    try std.testing.expectEqual(@as(i32, 3), row0[2].value);

    // Row 1: [4, 5]
    try std.testing.expect(!read_lists[1].isNull());
    const row1 = read_lists[1].value;
    try std.testing.expectEqual(@as(usize, 2), row1.len);
    try std.testing.expectEqual(@as(i32, 4), row1[0].value);
    try std.testing.expectEqual(@as(i32, 5), row1[1].value);
}

test "write and read list with null list" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file = try tmp_dir.dir.createFile("list_null_roundtrip.parquet", .{ .read = true });
    defer file.close();

    const columns = [_]parquet.ColumnDef{
        parquet.ColumnDef.listInt32("numbers", true),
    };

    // Write data: [[1], null, [2]]
    const inner0 = [_]Optional(i32){.{ .value = 1 }};
    const inner2 = [_]Optional(i32){.{ .value = 2 }};
    const lists = [_]Optional([]const Optional(i32)){
        .{ .value = &inner0 },
        .{ .null_value = {} },
        .{ .value = &inner2 },
    };

    {
        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        try writer.writeListColumnI32(0, &lists);
        try writer.close();
    }

    // Read it back
    try file.seekTo(0);
    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    const read_lists = try reader.readListColumn(0, i32);
    defer reader.freeListColumn(i32, read_lists);

    // Verify
    try std.testing.expectEqual(@as(usize, 3), read_lists.len);

    // Row 0: [1]
    try std.testing.expect(!read_lists[0].isNull());
    try std.testing.expectEqual(@as(usize, 1), read_lists[0].value.len);
    try std.testing.expectEqual(@as(i32, 1), read_lists[0].value[0].value);

    // Row 1: null
    try std.testing.expect(read_lists[1].isNull());

    // Row 2: [2]
    try std.testing.expect(!read_lists[2].isNull());
    try std.testing.expectEqual(@as(usize, 1), read_lists[2].value.len);
    try std.testing.expectEqual(@as(i32, 2), read_lists[2].value[0].value);
}

test "write and read list with empty list" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file = try tmp_dir.dir.createFile("list_empty_roundtrip.parquet", .{ .read = true });
    defer file.close();

    const columns = [_]parquet.ColumnDef{
        parquet.ColumnDef.listInt32("numbers", true),
    };

    // Write data: [[1], [], [2]]
    const inner0 = [_]Optional(i32){.{ .value = 1 }};
    const inner1 = [_]Optional(i32){};
    const inner2 = [_]Optional(i32){.{ .value = 2 }};
    const lists = [_]Optional([]const Optional(i32)){
        .{ .value = &inner0 },
        .{ .value = &inner1 },
        .{ .value = &inner2 },
    };

    {
        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        try writer.writeListColumnI32(0, &lists);
        try writer.close();
    }

    // Read it back
    try file.seekTo(0);
    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    const read_lists = try reader.readListColumn(0, i32);
    defer reader.freeListColumn(i32, read_lists);

    // Verify
    try std.testing.expectEqual(@as(usize, 3), read_lists.len);

    // Row 0: [1]
    try std.testing.expect(!read_lists[0].isNull());
    try std.testing.expectEqual(@as(usize, 1), read_lists[0].value.len);

    // Row 1: [] (empty)
    try std.testing.expect(!read_lists[1].isNull());
    try std.testing.expectEqual(@as(usize, 0), read_lists[1].value.len);

    // Row 2: [2]
    try std.testing.expect(!read_lists[2].isNull());
    try std.testing.expectEqual(@as(usize, 1), read_lists[2].value.len);
}

test "write and read list with null elements" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file = try tmp_dir.dir.createFile("list_null_elem_roundtrip.parquet", .{ .read = true });
    defer file.close();

    const columns = [_]parquet.ColumnDef{
        parquet.ColumnDef.listInt32("numbers", true),
    };

    // Write data: [[1, null, 3]]
    const inner0 = [_]Optional(i32){ .{ .value = 1 }, .{ .null_value = {} }, .{ .value = 3 } };
    const lists = [_]Optional([]const Optional(i32)){
        .{ .value = &inner0 },
    };

    {
        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        try writer.writeListColumnI32(0, &lists);
        try writer.close();
    }

    // Read it back
    try file.seekTo(0);
    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    const read_lists = try reader.readListColumn(0, i32);
    defer reader.freeListColumn(i32, read_lists);

    // Verify
    try std.testing.expectEqual(@as(usize, 1), read_lists.len);

    // Row 0: [1, null, 3]
    try std.testing.expect(!read_lists[0].isNull());
    const row0 = read_lists[0].value;
    try std.testing.expectEqual(@as(usize, 3), row0.len);
    try std.testing.expectEqual(@as(i32, 1), row0[0].value);
    try std.testing.expect(row0[1].isNull());
    try std.testing.expectEqual(@as(i32, 3), row0[2].value);
}
