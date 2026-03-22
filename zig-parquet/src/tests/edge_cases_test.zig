//! Edge case tests
//!
//! Tests for edge cases: empty tables, single values, large strings, nulls, etc.

const std = @import("std");
const parquet = @import("../lib.zig");

test "read single_value.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/edge_cases/single_value.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    // Verify metadata - single row
    try std.testing.expectEqual(@as(i64, 1), reader.metadata.num_rows);

    // Read the single value (42)
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqual(@as(i32, 42), rows[0].getColumn(0).?.asInt32().?);
}

test "read empty_table.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/edge_cases/empty_table.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    // Verify metadata - zero rows (may still have 1 empty row group)
    try std.testing.expectEqual(@as(i64, 0), reader.metadata.num_rows);
    // Empty tables can have 1 row group with 0 rows
    try std.testing.expect(reader.getNumRowGroups() <= 1);
    if (reader.getNumRowGroups() == 1) {
        try std.testing.expectEqual(@as(i64, 0), reader.getRowGroupNumRows(0));
    }
}

test "read large_strings.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/edge_cases/large_strings.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    // Verify metadata
    try std.testing.expectEqual(@as(i64, 10), reader.metadata.num_rows);

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 10), rows.len);

    // Read small column (col 0) - "x" repeated
    try std.testing.expectEqualStrings("x", rows[0].getColumn(0).?.asBytes().?);

    // Read medium column (col 1) - "y" * 1000
    const medium_val = rows[0].getColumn(1).?.asBytes().?;
    try std.testing.expectEqual(@as(usize, 1000), medium_val.len);
    try std.testing.expectEqual(@as(u8, 'y'), medium_val[0]);
    try std.testing.expectEqual(@as(u8, 'y'), medium_val[999]);

    // Read large column (col 2) - "z" * 100000
    const large_val = rows[0].getColumn(2).?.asBytes().?;
    try std.testing.expectEqual(@as(usize, 100000), large_val.len);
    try std.testing.expectEqual(@as(u8, 'z'), large_val[0]);
    try std.testing.expectEqual(@as(u8, 'z'), large_val[99999]);
}

test "read nulls_and_empties.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/basic/nulls_and_empties.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    // Verify metadata
    try std.testing.expectEqual(@as(i64, 100), reader.metadata.num_rows);

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 100), rows.len);

    // Check all_null column (col 0) - all 100 values should be null
    for (rows) |row| {
        try std.testing.expect(row.getColumn(0).?.isNull());
    }

    // Check all_empty column (col 1) - all 100 values should be empty strings
    for (rows) |row| {
        try std.testing.expectEqualStrings("", row.getColumn(1).?.asBytes().?);
    }

    // Check mixed column (col 2) - [None, "", "x", None, ""] * 20
    // Check first cycle
    try std.testing.expect(rows[0].getColumn(2).?.isNull());
    try std.testing.expectEqualStrings("", rows[1].getColumn(2).?.asBytes().?);
    try std.testing.expectEqualStrings("x", rows[2].getColumn(2).?.asBytes().?);
    try std.testing.expect(rows[3].getColumn(2).?.isNull());
    try std.testing.expectEqualStrings("", rows[4].getColumn(2).?.asBytes().?);
    // Check last cycle
    try std.testing.expect(rows[95].getColumn(2).?.isNull());
    try std.testing.expectEqualStrings("", rows[96].getColumn(2).?.asBytes().?);
    try std.testing.expectEqualStrings("x", rows[97].getColumn(2).?.asBytes().?);
    try std.testing.expect(rows[98].getColumn(2).?.isNull());
    try std.testing.expectEqualStrings("", rows[99].getColumn(2).?.asBytes().?);

    // Check no_nulls column (col 3) - ["a", "b", "c"] * 33 + ["d"]
    try std.testing.expectEqualStrings("a", rows[0].getColumn(3).?.asBytes().?);
    try std.testing.expectEqualStrings("b", rows[1].getColumn(3).?.asBytes().?);
    try std.testing.expectEqualStrings("c", rows[2].getColumn(3).?.asBytes().?);
    try std.testing.expectEqualStrings("d", rows[99].getColumn(3).?.asBytes().?);
}

test "read boundary_values.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/basic/boundary_values.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    // Verify metadata
    try std.testing.expectEqual(@as(i64, 3), reader.metadata.num_rows);

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 3), rows.len);

    // Read i32 column (column 2) - Parquet stores i8/i16 as i32 physical type
    // Values: [-2147483648, 2147483647, 0]
    try std.testing.expectEqual(@as(i32, -2147483648), rows[0].getColumn(2).?.asInt32().?);
    try std.testing.expectEqual(@as(i32, 2147483647), rows[1].getColumn(2).?.asInt32().?);
    try std.testing.expectEqual(@as(i32, 0), rows[2].getColumn(2).?.asInt32().?);

    // Read i64 column (column 3)
    // Values: [-9223372036854775808, 9223372036854775807, 0]
    try std.testing.expectEqual(@as(i64, -9223372036854775808), rows[0].getColumn(3).?.asInt64().?);
    try std.testing.expectEqual(@as(i64, 9223372036854775807), rows[1].getColumn(3).?.asInt64().?);
    try std.testing.expectEqual(@as(i64, 0), rows[2].getColumn(3).?.asInt64().?);
}
