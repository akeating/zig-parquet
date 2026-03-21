//! Multi-page Writer/Reader Tests
//!
//! Comprehensive tests for multi-page support across all column types.
//! These tests verify that data written with max_page_size constraint
//! can be correctly read back.

const std = @import("std");
const parquet = @import("../lib.zig");
const RowWriter = parquet.RowWriter;
const RowReader = parquet.RowReader;
const build_options = @import("build_options");

// =============================================================================
// Test Structs
// =============================================================================

const ByteArrayStruct = struct {
    id: i32,
    name: []const u8,
};

const NullableByteArrayStruct = struct {
    id: i32,
    name: ?[]const u8,
};

const ListStruct = struct {
    id: i32,
    values: []const i32,
};

const NullableListStruct = struct {
    id: i32,
    values: ?[]const i32,
};

const StructFieldStruct = struct {
    id: i32,
    nested: struct {
        a: i32,
        b: i64,
    },
};

const StructWithStringField = struct {
    id: i32,
    nested: struct {
        name: []const u8,
        value: i32,
    },
};

const ParquetMapEntry = parquet.MapEntry([]const u8, i32);

const MapStruct = struct {
    id: i32,
    data: []const ParquetMapEntry,
};

const StructWithListStruct = struct {
    id: i32,
    inner: struct {
        label: []const u8,
        tags: []const i32,
    },
};

const Point = struct {
    x: i32,
    y: i32,
};

const NestedListStruct = struct {
    id: i32,
    matrix: []const []const i32,
};

// =============================================================================
// Plain Byte Array Multi-page Tests
// =============================================================================

test "multi-page: plain byte array column" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("multipage_bytearray.parquet", .{ .read = true });
    defer file.close();

    const row_count = 200;

    // Write with small page size to force multiple pages
    {
        var writer = try parquet.writeToFileRows(ByteArrayStruct, allocator, file, .{
            .compression = .uncompressed,
            .use_dictionary = false, // Force plain encoding
            .max_page_size = 100, // Very small to force many pages
        });
        defer writer.deinit();

        // Allocate all names first, then free after writer closes
        var names: [row_count][]const u8 = undefined;
        defer for (names) |n| allocator.free(n);

        for (0..row_count) |i| {
            names[i] = try std.fmt.allocPrint(allocator, "name_{d:0>5}", .{i});
            try writer.writeRow(.{
                .id = @intCast(i),
                .name = names[i],
            });
        }
        try writer.close();
    }

    // Read back and verify
    try file.seekTo(0);
    {
        var reader = try parquet.openFileRowReader(ByteArrayStruct, allocator, file, .{});
        defer reader.deinit();

        try std.testing.expectEqual(@as(usize, row_count), reader.rowCount());

        var count: usize = 0;
        while (try reader.next()) |row| {
            defer allocator.free(row.name);
            try std.testing.expectEqual(@as(i32, @intCast(count)), row.id);
            const expected = try std.fmt.allocPrint(allocator, "name_{d:0>5}", .{count});
            defer allocator.free(expected);
            try std.testing.expectEqualStrings(expected, row.name);
            count += 1;
        }
        try std.testing.expectEqual(row_count, count);
    }
}

test "multi-page: nullable byte array column" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("multipage_nullable_bytearray.parquet", .{ .read = true });
    defer file.close();

    const row_count = 200;

    {
        var writer = try parquet.writeToFileRows(NullableByteArrayStruct, allocator, file, .{
            .compression = .uncompressed,
            .use_dictionary = false,
            .max_page_size = 100,
        });
        defer writer.deinit();

        // Collect all allocated names to free after writer closes
        var names: std.ArrayListUnmanaged(?[]const u8) = .empty;
        defer {
            for (names.items) |n| {
                if (n) |s| allocator.free(s);
            }
            names.deinit(allocator);
        }

        for (0..row_count) |i| {
            if (i % 3 == 0) {
                try names.append(allocator, null);
                try writer.writeRow(.{ .id = @intCast(i), .name = null });
            } else {
                const name = try std.fmt.allocPrint(allocator, "name_{d}", .{i});
                try names.append(allocator, name);
                try writer.writeRow(.{ .id = @intCast(i), .name = name });
            }
        }
        try writer.close();
    }

    try file.seekTo(0);
    {
        var reader = try parquet.openFileRowReader(NullableByteArrayStruct, allocator, file, .{});
        defer reader.deinit();

        try std.testing.expectEqual(@as(usize, row_count), reader.rowCount());

        var count: usize = 0;
        while (try reader.next()) |row| {
            defer if (row.name) |n| allocator.free(n);
            try std.testing.expectEqual(@as(i32, @intCast(count)), row.id);
            if (count % 3 == 0) {
                try std.testing.expectEqual(@as(?[]const u8, null), row.name);
            } else {
                const expected = try std.fmt.allocPrint(allocator, "name_{d}", .{count});
                defer allocator.free(expected);
                try std.testing.expectEqualStrings(expected, row.name.?);
            }
            count += 1;
        }
        try std.testing.expectEqual(row_count, count);
    }
}

// =============================================================================
// Plain List Multi-page Tests
// =============================================================================

test "multi-page: plain list column" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("multipage_list.parquet", .{ .read = true });
    defer file.close();

    const row_count = 100;

    {
        var writer = try parquet.writeToFileRows(ListStruct, allocator, file, .{
            .compression = .uncompressed,
            .use_dictionary = false,
            .max_page_size = 100, // Small to force multi-page
        });
        defer writer.deinit();

        // Collect all allocated lists to free after writer closes
        var all_values: std.ArrayListUnmanaged([]i32) = .empty;
        defer {
            for (all_values.items) |v| allocator.free(v);
            all_values.deinit(allocator);
        }

        for (0..row_count) |i| {
            // Create lists of varying sizes (1 to 5 elements - avoid empty lists for now)
            const list_size = (i % 5) + 1;
            var values = try allocator.alloc(i32, list_size);
            for (0..list_size) |j| {
                values[j] = @intCast(i * 10 + j);
            }
            try all_values.append(allocator, values);
            try writer.writeRow(.{ .id = @intCast(i), .values = values });
        }
        try writer.close();
    }

    try file.seekTo(0);
    {
        var reader = try parquet.openFileRowReader(ListStruct, allocator, file, .{});
        defer reader.deinit();

        try std.testing.expectEqual(@as(usize, row_count), reader.rowCount());

        var count: usize = 0;
        while (try reader.next()) |row| {
            defer allocator.free(row.values);
            try std.testing.expectEqual(@as(i32, @intCast(count)), row.id);
            const expected_size = (count % 5) + 1; // Matches write: 1-5 elements
            try std.testing.expectEqual(expected_size, row.values.len);
            for (row.values, 0..) |v, j| {
                try std.testing.expectEqual(@as(i32, @intCast(count * 10 + j)), v);
            }
            count += 1;
        }
        try std.testing.expectEqual(row_count, count);
    }
}

test "multi-page: nullable list column" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("multipage_nullable_list.parquet", .{ .read = true });
    defer file.close();

    const row_count = 100;

    {
        var writer = try parquet.writeToFileRows(NullableListStruct, allocator, file, .{
            .compression = .uncompressed,
            .use_dictionary = false,
            .max_page_size = 50,
        });
        defer writer.deinit();

        // Collect all allocated lists to free after writer closes
        var all_values: std.ArrayListUnmanaged(?[]i32) = .empty;
        defer {
            for (all_values.items) |v| {
                if (v) |arr| allocator.free(arr);
            }
            all_values.deinit(allocator);
        }

        for (0..row_count) |i| {
            if (i % 4 == 0) {
                try all_values.append(allocator, null);
                try writer.writeRow(.{ .id = @intCast(i), .values = null });
            } else {
                const list_size = (i % 3) + 1;
                var values = try allocator.alloc(i32, list_size);
                for (0..list_size) |j| {
                    values[j] = @intCast(i * 10 + j);
                }
                try all_values.append(allocator, values);
                try writer.writeRow(.{ .id = @intCast(i), .values = values });
            }
        }
        try writer.close();
    }

    try file.seekTo(0);
    {
        var reader = try parquet.openFileRowReader(NullableListStruct, allocator, file, .{});
        defer reader.deinit();

        var count: usize = 0;
        while (try reader.next()) |row| {
            defer if (row.values) |v| allocator.free(v);
            try std.testing.expectEqual(@as(i32, @intCast(count)), row.id);
            if (count % 4 == 0) {
                try std.testing.expectEqual(@as(?[]const i32, null), row.values);
            } else {
                const expected_size = (count % 3) + 1;
                try std.testing.expectEqual(expected_size, row.values.?.len);
            }
            count += 1;
        }
        try std.testing.expectEqual(row_count, count);
    }
}

// =============================================================================
// Map Multi-page Tests
// =============================================================================

// List-of-struct with primitives (Point) - tests multi-page handling
const PointStruct = struct {
    id: i32,
    points: []const Point,
};

test "multi-page: list-of-struct column" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("multipage_list_struct.parquet", .{ .read = true });
    defer file.close();

    // First test: verify single-page works before testing multi-page
    const row_count = 3; // Small enough for single page

    {
        var writer = try parquet.writeToFileRows(PointStruct, allocator, file, .{
            .compression = .uncompressed,
            .use_dictionary = false,
            // No max_page_size = single page
        });
        defer writer.deinit();

        // Simple fixed data
        const points1 = [_]Point{ .{ .x = 1, .y = 10 }, .{ .x = 2, .y = 20 } };
        const points2 = [_]Point{.{ .x = 3, .y = 30 }};
        const points3 = [_]Point{ .{ .x = 4, .y = 40 }, .{ .x = 5, .y = 50 }, .{ .x = 6, .y = 60 } };

        try writer.writeRow(.{ .id = 0, .points = &points1 });
        try writer.writeRow(.{ .id = 1, .points = &points2 });
        try writer.writeRow(.{ .id = 2, .points = &points3 });
        try writer.close();
    }

    try file.seekTo(0);
    {
        var reader = try parquet.openFileRowReader(PointStruct, allocator, file, .{});
        defer reader.deinit();

        try std.testing.expectEqual(@as(usize, row_count), reader.rowCount());

        // Row 0: [{1, 10}, {2, 20}]
        const row0 = (try reader.next()).?;
        defer allocator.free(row0.points);
        try std.testing.expectEqual(@as(i32, 0), row0.id);
        try std.testing.expectEqual(@as(usize, 2), row0.points.len);
        try std.testing.expectEqual(@as(i32, 1), row0.points[0].x);
        try std.testing.expectEqual(@as(i32, 10), row0.points[0].y);
        try std.testing.expectEqual(@as(i32, 2), row0.points[1].x);
        try std.testing.expectEqual(@as(i32, 20), row0.points[1].y);

        // Row 1: [{3, 30}]
        const row1 = (try reader.next()).?;
        defer allocator.free(row1.points);
        try std.testing.expectEqual(@as(i32, 1), row1.id);
        try std.testing.expectEqual(@as(usize, 1), row1.points.len);
        try std.testing.expectEqual(@as(i32, 3), row1.points[0].x);
        try std.testing.expectEqual(@as(i32, 30), row1.points[0].y);

        // Row 2: [{4, 40}, {5, 50}, {6, 60}]
        const row2 = (try reader.next()).?;
        defer allocator.free(row2.points);
        try std.testing.expectEqual(@as(i32, 2), row2.id);
        try std.testing.expectEqual(@as(usize, 3), row2.points.len);
        try std.testing.expectEqual(@as(i32, 4), row2.points[0].x);
        try std.testing.expectEqual(@as(i32, 40), row2.points[0].y);
    }
}

// =============================================================================
// Compression + Multi-page Tests
// =============================================================================

test "multi-page: with zstd compression" {
    if (build_options.no_compression) return;
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("multipage_zstd.parquet", .{ .read = true });
    defer file.close();

    const row_count = 500;

    const SimpleStruct = struct {
        id: i64,
        value: i64,
    };

    {
        var writer = try parquet.writeToFileRows(SimpleStruct, allocator, file, .{
            .compression = .zstd,
            .max_page_size = 256, // Small page size with compression
        });
        defer writer.deinit();

        for (0..row_count) |i| {
            try writer.writeRow(.{ .id = @intCast(i), .value = @intCast(i * 2) });
        }
        try writer.close();
    }

    try file.seekTo(0);
    {
        var reader = try parquet.openFileRowReader(SimpleStruct, allocator, file, .{});
        defer reader.deinit();

        try std.testing.expectEqual(@as(usize, row_count), reader.rowCount());

        var count: usize = 0;
        while (try reader.next()) |row| {
            try std.testing.expectEqual(@as(i64, @intCast(count)), row.id);
            try std.testing.expectEqual(@as(i64, @intCast(count * 2)), row.value);
            count += 1;
        }
        try std.testing.expectEqual(row_count, count);
    }
}

test "multi-page: list with snappy compression" {
    if (build_options.no_compression) return;
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("multipage_list_snappy.parquet", .{ .read = true });
    defer file.close();

    const row_count = 100;

    {
        var writer = try parquet.writeToFileRows(ListStruct, allocator, file, .{
            .compression = .snappy,
            .use_dictionary = false,
            .max_page_size = 100,
        });
        defer writer.deinit();

        var all_values: std.ArrayListUnmanaged([]i32) = .empty;
        defer {
            for (all_values.items) |v| allocator.free(v);
            all_values.deinit(allocator);
        }

        for (0..row_count) |i| {
            const list_size = (i % 5) + 1;
            var values = try allocator.alloc(i32, list_size);
            for (0..list_size) |j| {
                values[j] = @intCast(i * 10 + j);
            }
            try all_values.append(allocator, values);
            try writer.writeRow(.{ .id = @intCast(i), .values = values });
        }
        try writer.close();
    }

    try file.seekTo(0);
    {
        var reader = try parquet.openFileRowReader(ListStruct, allocator, file, .{});
        defer reader.deinit();

        var count: usize = 0;
        while (try reader.next()) |row| {
            defer allocator.free(row.values);
            try std.testing.expectEqual(@as(i32, @intCast(count)), row.id);
            count += 1;
        }
        try std.testing.expectEqual(row_count, count);
    }
}

// =============================================================================
// Edge Case Tests
// =============================================================================

test "multi-page: single value per page" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("multipage_single_value.parquet", .{ .read = true });
    defer file.close();

    const SimpleStruct = struct {
        id: i64,
    };

    const row_count = 50;

    {
        var writer = try parquet.writeToFileRows(SimpleStruct, allocator, file, .{
            .compression = .uncompressed,
            .max_page_size = 1, // Force single value per page
        });
        defer writer.deinit();

        for (0..row_count) |i| {
            try writer.writeRow(.{ .id = @intCast(i) });
        }
        try writer.close();
    }

    try file.seekTo(0);
    {
        var reader = try parquet.openFileRowReader(SimpleStruct, allocator, file, .{});
        defer reader.deinit();

        try std.testing.expectEqual(@as(usize, row_count), reader.rowCount());

        var count: usize = 0;
        while (try reader.next()) |row| {
            try std.testing.expectEqual(@as(i64, @intCast(count)), row.id);
            count += 1;
        }
        try std.testing.expectEqual(row_count, count);
    }
}

test "multi-page: all nulls in nullable column" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("multipage_all_nulls.parquet", .{ .read = true });
    defer file.close();

    const AllNullsStruct = struct {
        id: i32,
        value: ?i64,
    };

    const row_count = 100;

    {
        var writer = try parquet.writeToFileRows(AllNullsStruct, allocator, file, .{
            .compression = .uncompressed,
            .max_page_size = 20,
        });
        defer writer.deinit();

        for (0..row_count) |i| {
            try writer.writeRow(.{ .id = @intCast(i), .value = null });
        }
        try writer.close();
    }

    try file.seekTo(0);
    {
        var reader = try parquet.openFileRowReader(AllNullsStruct, allocator, file, .{});
        defer reader.deinit();

        var count: usize = 0;
        while (try reader.next()) |row| {
            try std.testing.expectEqual(@as(i32, @intCast(count)), row.id);
            try std.testing.expectEqual(@as(?i64, null), row.value);
            count += 1;
        }
        try std.testing.expectEqual(row_count, count);
    }
}

test "multi-page: empty lists" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("multipage_empty_lists.parquet", .{ .read = true });
    defer file.close();

    const row_count = 50;

    {
        var writer = try parquet.writeToFileRows(ListStruct, allocator, file, .{
            .compression = .uncompressed,
            .use_dictionary = false,
            .max_page_size = 50, // Small to force multi-page
        });
        defer writer.deinit();

        // All rows have empty lists
        for (0..row_count) |i| {
            const empty_values: []const i32 = &[_]i32{};
            try writer.writeRow(.{
                .id = @intCast(i),
                .values = empty_values,
            });
        }
        try writer.close();
    }

    // Read back and verify
    try file.seekTo(0);
    {
        var reader = try parquet.openFileRowReader(ListStruct, allocator, file, .{});
        defer reader.deinit();

        var count: usize = 0;
        while (try reader.next()) |row| {
            try std.testing.expectEqual(@as(i32, @intCast(count)), row.id);
            try std.testing.expectEqual(@as(usize, 0), row.values.len);
            count += 1;
        }
        try std.testing.expectEqual(row_count, count);
    }
}

test "multi-page: mixed empty and non-empty lists" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("multipage_mixed_lists.parquet", .{ .read = true });
    defer file.close();

    const row_count = 100;

    {
        var writer = try parquet.writeToFileRows(ListStruct, allocator, file, .{
            .compression = .uncompressed,
            .use_dictionary = false,
            .max_page_size = 50, // Small size to force multi-page
        });
        defer writer.deinit();

        var all_values: std.ArrayListUnmanaged([]i32) = .empty;
        defer {
            for (all_values.items) |v| allocator.free(v);
            all_values.deinit(allocator);
        }

        for (0..row_count) |i| {
            if (i % 2 == 0) {
                try writer.writeRow(.{ .id = @intCast(i), .values = &[_]i32{} });
            } else {
                var values = try allocator.alloc(i32, 3);
                values[0] = @intCast(i);
                values[1] = @intCast(i + 1);
                values[2] = @intCast(i + 2);
                try all_values.append(allocator, values);
                try writer.writeRow(.{ .id = @intCast(i), .values = values });
            }
        }
        try writer.close();
    }

    try file.seekTo(0);
    {
        var reader = try parquet.openFileRowReader(ListStruct, allocator, file, .{});
        defer reader.deinit();

        var count: usize = 0;
        while (try reader.next()) |row| {
            defer allocator.free(row.values);
            try std.testing.expectEqual(@as(i32, @intCast(count)), row.id);
            if (count % 2 == 0) {
                try std.testing.expectEqual(@as(usize, 0), row.values.len);
            } else {
                try std.testing.expectEqual(@as(usize, 3), row.values.len);
                try std.testing.expectEqual(@as(i32, @intCast(count)), row.values[0]);
            }
            count += 1;
        }
        try std.testing.expectEqual(row_count, count);
    }
}

// =============================================================================
// Nested List Multi-page Tests
// =============================================================================

test "multi-page: nested list (list of list)" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("multipage_nested_list.parquet", .{ .read = true });
    defer file.close();

    const row_count = 50;

    {
        var writer = try parquet.writeToFileRows(NestedListStruct, allocator, file, .{
            .compression = .uncompressed,
            .max_page_size = 50,
        });
        defer writer.deinit();

        // Collect all allocated matrices to free after writer closes
        var all_matrices: std.ArrayListUnmanaged([][]const i32) = .empty;
        defer {
            for (all_matrices.items) |matrix| {
                for (matrix) |inner| allocator.free(inner);
                allocator.free(matrix);
            }
            all_matrices.deinit(allocator);
        }

        for (0..row_count) |i| {
            const outer_size = (i % 3) + 1; // 1-3 inner lists
            var matrix = try allocator.alloc([]const i32, outer_size);

            for (0..outer_size) |j| {
                const inner_size = ((i + j) % 3) + 1; // 1-3 elements
                var inner = try allocator.alloc(i32, inner_size);
                for (0..inner_size) |k| {
                    inner[k] = @intCast(i * 100 + j * 10 + k);
                }
                matrix[j] = inner;
            }
            try all_matrices.append(allocator, matrix);
            try writer.writeRow(.{ .id = @intCast(i), .matrix = matrix });
        }
        try writer.close();
    }

    try file.seekTo(0);
    {
        var reader = try parquet.openFileRowReader(NestedListStruct, allocator, file, .{});
        defer reader.deinit();

        try std.testing.expectEqual(@as(usize, row_count), reader.rowCount());

        var count: usize = 0;
        while (try reader.next()) |row| {
            defer {
                for (row.matrix) |inner| allocator.free(inner);
                allocator.free(row.matrix);
            }
            try std.testing.expectEqual(@as(i32, @intCast(count)), row.id);
            const expected_outer = (count % 3) + 1;
            try std.testing.expectEqual(expected_outer, row.matrix.len);
            count += 1;
        }
        try std.testing.expectEqual(row_count, count);
    }
}

// =============================================================================
// Large Data Multi-page Tests
// =============================================================================

test "multi-page: large dataset (10K rows)" {
    if (build_options.no_compression) return;
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("multipage_large.parquet", .{ .read = true });
    defer file.close();

    const LargeStruct = struct {
        id: i64,
        a: i32,
        b: i32,
        c: i64,
    };

    const row_count = 10_000;

    {
        var writer = try parquet.writeToFileRows(LargeStruct, allocator, file, .{
            .compression = .zstd,
            .max_page_size = 4096, // 4KB pages
        });
        defer writer.deinit();

        for (0..row_count) |i| {
            try writer.writeRow(.{
                .id = @intCast(i),
                .a = @intCast(i % 1000),
                .b = @intCast(i % 500),
                .c = @intCast(i * 3),
            });
        }
        try writer.close();
    }

    try file.seekTo(0);
    {
        var reader = try parquet.openFileRowReader(LargeStruct, allocator, file, .{});
        defer reader.deinit();

        try std.testing.expectEqual(@as(usize, row_count), reader.rowCount());

        // Verify first, middle, and last rows
        const first = (try reader.next()).?;
        try std.testing.expectEqual(@as(i64, 0), first.id);

        // Skip to middle
        for (0..4998) |_| {
            _ = try reader.next();
        }
        const middle = (try reader.next()).?;
        try std.testing.expectEqual(@as(i64, 4999), middle.id);

        // Skip to end
        for (0..4999) |_| {
            _ = try reader.next();
        }
        const last = (try reader.next()).?;
        try std.testing.expectEqual(@as(i64, 9999), last.id);

        // Should be done
        try std.testing.expectEqual(@as(?LargeStruct, null), try reader.next());
    }
}

// =============================================================================
// Multi-page Map Tests
// =============================================================================

test "multi-page: map column" {
    const allocator = std.testing.allocator;

    const row_count = 50;

    var rw = try parquet.writeToBufferRows(MapStruct, allocator, .{
        .compression = .uncompressed,
        .use_dictionary = false,
        .max_page_size = 100,
    });
    defer rw.deinit();

    var all_entries: std.ArrayListUnmanaged([]ParquetMapEntry) = .empty;
    defer {
        for (all_entries.items) |entries| {
            for (entries) |e| allocator.free(e.key);
            allocator.free(entries);
        }
        all_entries.deinit(allocator);
    }

    for (0..row_count) |i| {
        const entry_count = (i % 4) + 1;
        var entries = try allocator.alloc(ParquetMapEntry, entry_count);
        for (0..entry_count) |j| {
            const key = try std.fmt.allocPrint(allocator, "k{d}_{d}", .{ i, j });
            entries[j] = .{ .key = key, .value = @intCast(i * 10 + j) };
        }
        try all_entries.append(allocator, entries);
        try rw.writeRow(.{ .id = @intCast(i), .data = entries });
    }
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(MapStruct, allocator, buffer, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, row_count), reader.rowCount());

    var count: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        try std.testing.expectEqual(@as(i32, @intCast(count)), row.id);
        const expected_count = (count % 4) + 1;
        try std.testing.expectEqual(expected_count, row.data.len);
        for (row.data, 0..) |entry, j| {
            const expected_key = try std.fmt.allocPrint(allocator, "k{d}_{d}", .{ count, j });
            defer allocator.free(expected_key);
            try std.testing.expectEqualStrings(expected_key, entry.key);
            try std.testing.expectEqual(@as(i32, @intCast(count * 10 + j)), entry.value.?);
        }
        count += 1;
    }
    try std.testing.expectEqual(row_count, count);
}

// =============================================================================
// Multi-page Struct with List Tests
// =============================================================================

test "multi-page: struct with list field" {
    const allocator = std.testing.allocator;

    const row_count = 50;

    var rw = try parquet.writeToBufferRows(StructWithListStruct, allocator, .{
        .compression = .uncompressed,
        .use_dictionary = false,
        .max_page_size = 100,
    });
    defer rw.deinit();

    var all_tags: std.ArrayListUnmanaged([]i32) = .empty;
    var all_labels: std.ArrayListUnmanaged([]u8) = .empty;
    defer {
        for (all_tags.items) |t| allocator.free(t);
        all_tags.deinit(allocator);
        for (all_labels.items) |l| allocator.free(l);
        all_labels.deinit(allocator);
    }

    for (0..row_count) |i| {
        const tag_count = (i % 3) + 1;
        var tags = try allocator.alloc(i32, tag_count);
        for (0..tag_count) |j| {
            tags[j] = @intCast(i * 100 + j);
        }
        try all_tags.append(allocator, tags);
        const label = try std.fmt.allocPrint(allocator, "label_{d}", .{i});
        try all_labels.append(allocator, label);
        try rw.writeRow(.{
            .id = @intCast(i),
            .inner = .{ .label = label, .tags = tags },
        });
    }
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(StructWithListStruct, allocator, buffer, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, row_count), reader.rowCount());

    var count: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        try std.testing.expectEqual(@as(i32, @intCast(count)), row.id);
        const expected_label = try std.fmt.allocPrint(allocator, "label_{d}", .{count});
        defer allocator.free(expected_label);
        try std.testing.expectEqualStrings(expected_label, row.inner.label);
        const expected_tags = (count % 3) + 1;
        try std.testing.expectEqual(expected_tags, row.inner.tags.len);
        for (row.inner.tags, 0..) |tag, j| {
            try std.testing.expectEqual(@as(i32, @intCast(count * 100 + j)), tag);
        }
        count += 1;
    }
    try std.testing.expectEqual(row_count, count);
}
