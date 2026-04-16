//! Tests for list column support
//!
//! Tests reading and writing list columns from Parquet files.

const std = @import("std");
const io = std.testing.io;
const parquet = @import("../lib.zig");

const Value = parquet.Value;
const Optional = parquet.Optional;

// =============================================================================
// Read Tests
// =============================================================================

test "read list_int.parquet" {
    const allocator = std.testing.allocator;

    // Open the test file
    const file = std.Io.Dir.cwd().openFile(io, "../test-files-arrow/nested/list_int.parquet", .{}) catch |err| {
        std.debug.print("Skipping test - file not found: {}\n", .{err});
        return;
    };
    defer file.close(io);

    var reader = try parquet.openFileDynamic(allocator, file, io, .{});
    defer reader.deinit();

    // Verify schema
    const schema = reader.getSchema();
    try std.testing.expect(schema.len > 0);

    // Read all rows
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    // Expected data: [[1, 2, 3], [4, 5], [6], [], [7, 8, 9, 10]]
    try std.testing.expectEqual(@as(usize, 5), rows.len);

    // Row 0: [1, 2, 3]
    {
        const list = rows[0].getColumn(0).?;
        switch (list) {
            .list_val => |items| {
                try std.testing.expectEqual(@as(usize, 3), items.len);
                try std.testing.expectEqual(@as(i32, 1), items[0].asInt32().?);
                try std.testing.expectEqual(@as(i32, 2), items[1].asInt32().?);
                try std.testing.expectEqual(@as(i32, 3), items[2].asInt32().?);
            },
            else => return error.TestUnexpectedResult,
        }
    }

    // Row 1: [4, 5]
    {
        const list = rows[1].getColumn(0).?;
        switch (list) {
            .list_val => |items| {
                try std.testing.expectEqual(@as(usize, 2), items.len);
                try std.testing.expectEqual(@as(i32, 4), items[0].asInt32().?);
                try std.testing.expectEqual(@as(i32, 5), items[1].asInt32().?);
            },
            else => return error.TestUnexpectedResult,
        }
    }

    // Row 2: [6]
    {
        const list = rows[2].getColumn(0).?;
        switch (list) {
            .list_val => |items| {
                try std.testing.expectEqual(@as(usize, 1), items.len);
                try std.testing.expectEqual(@as(i32, 6), items[0].asInt32().?);
            },
            else => return error.TestUnexpectedResult,
        }
    }

    // Row 3: [] (empty list)
    {
        const list = rows[3].getColumn(0).?;
        switch (list) {
            .list_val => |items| {
                try std.testing.expectEqual(@as(usize, 0), items.len);
            },
            else => return error.TestUnexpectedResult,
        }
    }

    // Row 4: [7, 8, 9, 10]
    {
        const list = rows[4].getColumn(0).?;
        switch (list) {
            .list_val => |items| {
                try std.testing.expectEqual(@as(usize, 4), items.len);
                try std.testing.expectEqual(@as(i32, 7), items[0].asInt32().?);
                try std.testing.expectEqual(@as(i32, 8), items[1].asInt32().?);
                try std.testing.expectEqual(@as(i32, 9), items[2].asInt32().?);
                try std.testing.expectEqual(@as(i32, 10), items[3].asInt32().?);
            },
            else => return error.TestUnexpectedResult,
        }
    }
}

test "read list_nullable.parquet" {
    const allocator = std.testing.allocator;

    // Open the test file
    const file = std.Io.Dir.cwd().openFile(io, "../test-files-arrow/nested/list_nullable.parquet", .{}) catch |err| {
        std.debug.print("Skipping test - file not found: {}\n", .{err});
        return;
    };
    defer file.close(io);

    var reader = try parquet.openFileDynamic(allocator, file, io, .{});
    defer reader.deinit();

    // Read all rows
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    // Expected data: [[1, null, 3], null, [4, 5], [], [null, null]]
    try std.testing.expectEqual(@as(usize, 5), rows.len);

    // Row 0: [1, null, 3]
    {
        const list = rows[0].getColumn(0).?;
        switch (list) {
            .list_val => |items| {
                try std.testing.expectEqual(@as(usize, 3), items.len);
                try std.testing.expectEqual(@as(i32, 1), items[0].asInt32().?);
                try std.testing.expect(items[1].isNull());
                try std.testing.expectEqual(@as(i32, 3), items[2].asInt32().?);
            },
            else => return error.TestUnexpectedResult,
        }
    }

    // Row 1: null
    try std.testing.expect(rows[1].getColumn(0).?.isNull());

    // Row 2: [4, 5]
    {
        const list = rows[2].getColumn(0).?;
        switch (list) {
            .list_val => |items| {
                try std.testing.expectEqual(@as(usize, 2), items.len);
                try std.testing.expectEqual(@as(i32, 4), items[0].asInt32().?);
                try std.testing.expectEqual(@as(i32, 5), items[1].asInt32().?);
            },
            else => return error.TestUnexpectedResult,
        }
    }

    // Row 3: [] (empty list)
    {
        const list = rows[3].getColumn(0).?;
        switch (list) {
            .list_val => |items| {
                try std.testing.expectEqual(@as(usize, 0), items.len);
            },
            else => return error.TestUnexpectedResult,
        }
    }

    // Row 4: [null, null]
    {
        const list = rows[4].getColumn(0).?;
        switch (list) {
            .list_val => |items| {
                try std.testing.expectEqual(@as(usize, 2), items.len);
                try std.testing.expect(items[0].isNull());
                try std.testing.expect(items[1].isNull());
            },
            else => return error.TestUnexpectedResult,
        }
    }
}

// =============================================================================
// Write Round-Trip Tests
// =============================================================================

test "write and read list of i32" {
    const allocator = std.testing.allocator;

    // Create temp file
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file = try tmp_dir.dir.createFile(io, "list_roundtrip.parquet", .{ .read = true });
    defer file.close(io);

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
        var writer = try parquet.writeToFile(allocator, file, io, &columns);
        defer writer.deinit();

        try writer.writeListColumnI32(0, &lists);
        try writer.close();
    }

    // Read it back
    var reader = try parquet.openFileDynamic(allocator, file, io, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    // Verify
    try std.testing.expectEqual(@as(usize, 2), rows.len);

    // Row 0: [1, 2, 3]
    {
        const list = rows[0].getColumn(0).?;
        switch (list) {
            .list_val => |items| {
                try std.testing.expectEqual(@as(usize, 3), items.len);
                try std.testing.expectEqual(@as(i32, 1), items[0].asInt32().?);
                try std.testing.expectEqual(@as(i32, 2), items[1].asInt32().?);
                try std.testing.expectEqual(@as(i32, 3), items[2].asInt32().?);
            },
            else => return error.TestUnexpectedResult,
        }
    }

    // Row 1: [4, 5]
    {
        const list = rows[1].getColumn(0).?;
        switch (list) {
            .list_val => |items| {
                try std.testing.expectEqual(@as(usize, 2), items.len);
                try std.testing.expectEqual(@as(i32, 4), items[0].asInt32().?);
                try std.testing.expectEqual(@as(i32, 5), items[1].asInt32().?);
            },
            else => return error.TestUnexpectedResult,
        }
    }
}

test "write and read list with null list" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file = try tmp_dir.dir.createFile(io, "list_null_roundtrip.parquet", .{ .read = true });
    defer file.close(io);

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
        var writer = try parquet.writeToFile(allocator, file, io, &columns);
        defer writer.deinit();

        try writer.writeListColumnI32(0, &lists);
        try writer.close();
    }

    // Read it back
    var reader = try parquet.openFileDynamic(allocator, file, io, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    // Verify
    try std.testing.expectEqual(@as(usize, 3), rows.len);

    // Row 0: [1]
    {
        const list = rows[0].getColumn(0).?;
        switch (list) {
            .list_val => |items| {
                try std.testing.expectEqual(@as(usize, 1), items.len);
                try std.testing.expectEqual(@as(i32, 1), items[0].asInt32().?);
            },
            else => return error.TestUnexpectedResult,
        }
    }

    // Row 1: null
    try std.testing.expect(rows[1].getColumn(0).?.isNull());

    // Row 2: [2]
    {
        const list = rows[2].getColumn(0).?;
        switch (list) {
            .list_val => |items| {
                try std.testing.expectEqual(@as(usize, 1), items.len);
                try std.testing.expectEqual(@as(i32, 2), items[0].asInt32().?);
            },
            else => return error.TestUnexpectedResult,
        }
    }
}

test "write and read list with empty list" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file = try tmp_dir.dir.createFile(io, "list_empty_roundtrip.parquet", .{ .read = true });
    defer file.close(io);

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
        var writer = try parquet.writeToFile(allocator, file, io, &columns);
        defer writer.deinit();

        try writer.writeListColumnI32(0, &lists);
        try writer.close();
    }

    // Read it back
    var reader = try parquet.openFileDynamic(allocator, file, io, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    // Verify
    try std.testing.expectEqual(@as(usize, 3), rows.len);

    // Row 0: [1]
    {
        const list = rows[0].getColumn(0).?;
        switch (list) {
            .list_val => |items| {
                try std.testing.expectEqual(@as(usize, 1), items.len);
            },
            else => return error.TestUnexpectedResult,
        }
    }

    // Row 1: [] (empty)
    {
        const list = rows[1].getColumn(0).?;
        switch (list) {
            .list_val => |items| {
                try std.testing.expectEqual(@as(usize, 0), items.len);
            },
            else => return error.TestUnexpectedResult,
        }
    }

    // Row 2: [2]
    {
        const list = rows[2].getColumn(0).?;
        switch (list) {
            .list_val => |items| {
                try std.testing.expectEqual(@as(usize, 1), items.len);
            },
            else => return error.TestUnexpectedResult,
        }
    }
}

test "write and read list with null elements" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file = try tmp_dir.dir.createFile(io, "list_null_elem_roundtrip.parquet", .{ .read = true });
    defer file.close(io);

    const columns = [_]parquet.ColumnDef{
        parquet.ColumnDef.listInt32("numbers", true),
    };

    // Write data: [[1, null, 3]]
    const inner0 = [_]Optional(i32){ .{ .value = 1 }, .{ .null_value = {} }, .{ .value = 3 } };
    const lists = [_]Optional([]const Optional(i32)){
        .{ .value = &inner0 },
    };

    {
        var writer = try parquet.writeToFile(allocator, file, io, &columns);
        defer writer.deinit();

        try writer.writeListColumnI32(0, &lists);
        try writer.close();
    }

    // Read it back
    var reader = try parquet.openFileDynamic(allocator, file, io, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    // Verify
    try std.testing.expectEqual(@as(usize, 1), rows.len);

    // Row 0: [1, null, 3]
    {
        const list = rows[0].getColumn(0).?;
        switch (list) {
            .list_val => |items| {
                try std.testing.expectEqual(@as(usize, 3), items.len);
                try std.testing.expectEqual(@as(i32, 1), items[0].asInt32().?);
                try std.testing.expect(items[1].isNull());
                try std.testing.expectEqual(@as(i32, 3), items[2].asInt32().?);
            },
            else => return error.TestUnexpectedResult,
        }
    }
}
