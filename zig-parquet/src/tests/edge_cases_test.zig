//! Edge case tests
//!
//! Tests for edge cases: empty tables, single values, large strings, nulls, etc.

const std = @import("std");
const parquet = @import("../lib.zig");
const Reader = parquet.Reader;
const Optional = parquet.Optional;

test "read single_value.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/edge_cases/single_value.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Verify metadata - single row
    try std.testing.expectEqual(@as(i64, 1), reader.metadata.num_rows);

    // Read the single value (42)
    const values = try reader.readColumn(0, i32);
    defer allocator.free(values);

    try std.testing.expectEqual(@as(usize, 1), values.len);
    try std.testing.expectEqual(@as(i32, 42), values[0].value);
}

test "read empty_table.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/edge_cases/empty_table.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Verify metadata - zero rows (may still have 1 empty row group)
    try std.testing.expectEqual(@as(i64, 0), reader.metadata.num_rows);
    // Empty tables can have 1 row group with 0 rows
    try std.testing.expect(reader.getNumRowGroups() <= 1);
    if (reader.getNumRowGroups() == 1) {
        try std.testing.expectEqual(@as(i64, 0), reader.getRowGroupNumRows(0).?);
    }
}

test "read large_strings.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/edge_cases/large_strings.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Verify metadata
    try std.testing.expectEqual(@as(i64, 10), reader.metadata.num_rows);

    // Read small column - "x" repeated
    const small = try reader.readColumn(0, []const u8);
    defer {
        for (small) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(small);
    }

    try std.testing.expectEqual(@as(usize, 10), small.len);
    try std.testing.expectEqualStrings("x", small[0].value);

    // Read medium column - "y" * 1000
    const medium = try reader.readColumn(1, []const u8);
    defer {
        for (medium) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(medium);
    }

    try std.testing.expectEqual(@as(usize, 10), medium.len);
    try std.testing.expectEqual(@as(usize, 1000), medium[0].value.len);
    try std.testing.expectEqual(@as(u8, 'y'), medium[0].value[0]);
    try std.testing.expectEqual(@as(u8, 'y'), medium[0].value[999]);

    // Read large column - "z" * 100000
    const large = try reader.readColumn(2, []const u8);
    defer {
        for (large) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(large);
    }

    try std.testing.expectEqual(@as(usize, 10), large.len);
    try std.testing.expectEqual(@as(usize, 100000), large[0].value.len);
    try std.testing.expectEqual(@as(u8, 'z'), large[0].value[0]);
    try std.testing.expectEqual(@as(u8, 'z'), large[0].value[99999]);
}

test "read nulls_and_empties.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/basic/nulls_and_empties.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Verify metadata
    try std.testing.expectEqual(@as(i64, 100), reader.metadata.num_rows);

    // Read all_null column - all 100 values should be null
    const all_null = try reader.readColumn(0, []const u8);
    defer {
        for (all_null) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(all_null);
    }

    try std.testing.expectEqual(@as(usize, 100), all_null.len);
    for (all_null) |v| {
        try std.testing.expectEqual(Optional([]const u8){ .null_value = {} }, v);
    }

    // Read all_empty column - all 100 values should be empty strings
    const all_empty = try reader.readColumn(1, []const u8);
    defer {
        for (all_empty) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(all_empty);
    }

    try std.testing.expectEqual(@as(usize, 100), all_empty.len);
    for (all_empty) |v| {
        try std.testing.expectEqualStrings("", v.value);
    }

    // Read mixed column - [None, "", "x", None, ""] * 20
    const mixed = try reader.readColumn(2, []const u8);
    defer {
        for (mixed) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(mixed);
    }

    try std.testing.expectEqual(@as(usize, 100), mixed.len);
    // Check first cycle
    try std.testing.expectEqual(Optional([]const u8){ .null_value = {} }, mixed[0]);
    try std.testing.expectEqualStrings("", mixed[1].value);
    try std.testing.expectEqualStrings("x", mixed[2].value);
    try std.testing.expectEqual(Optional([]const u8){ .null_value = {} }, mixed[3]);
    try std.testing.expectEqualStrings("", mixed[4].value);
    // Check last cycle
    try std.testing.expectEqual(Optional([]const u8){ .null_value = {} }, mixed[95]);
    try std.testing.expectEqualStrings("", mixed[96].value);
    try std.testing.expectEqualStrings("x", mixed[97].value);
    try std.testing.expectEqual(Optional([]const u8){ .null_value = {} }, mixed[98]);
    try std.testing.expectEqualStrings("", mixed[99].value);

    // Read no_nulls column - ["a", "b", "c"] * 33 + ["d"]
    const no_nulls = try reader.readColumn(3, []const u8);
    defer {
        for (no_nulls) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(no_nulls);
    }

    try std.testing.expectEqual(@as(usize, 100), no_nulls.len);
    try std.testing.expectEqualStrings("a", no_nulls[0].value);
    try std.testing.expectEqualStrings("b", no_nulls[1].value);
    try std.testing.expectEqualStrings("c", no_nulls[2].value);
    try std.testing.expectEqualStrings("d", no_nulls[99].value);
}

test "read boundary_values.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/basic/boundary_values.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Verify metadata
    try std.testing.expectEqual(@as(i64, 3), reader.metadata.num_rows);

    // Read i32 column (column 2) - Parquet stores i8/i16 as i32 physical type
    // Values: [-2147483648, 2147483647, 0]
    const i32_col = try reader.readColumn(2, i32);
    defer allocator.free(i32_col);

    try std.testing.expectEqual(@as(usize, 3), i32_col.len);
    try std.testing.expectEqual(@as(i32, -2147483648), i32_col[0].value);
    try std.testing.expectEqual(@as(i32, 2147483647), i32_col[1].value);
    try std.testing.expectEqual(@as(i32, 0), i32_col[2].value);

    // Read i64 column (column 3)
    // Values: [-9223372036854775808, 9223372036854775807, 0]
    const i64_col = try reader.readColumn(3, i64);
    defer allocator.free(i64_col);

    try std.testing.expectEqual(@as(usize, 3), i64_col.len);
    try std.testing.expectEqual(@as(i64, -9223372036854775808), i64_col[0].value);
    try std.testing.expectEqual(@as(i64, 9223372036854775807), i64_col[1].value);
    try std.testing.expectEqual(@as(i64, 0), i64_col[2].value);
}
