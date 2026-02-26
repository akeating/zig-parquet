//! Tests for DynamicReader
//!
//! Verifies schema-agnostic reading by writing with RowWriter(T)
//! and reading back with DynamicReader.

const std = @import("std");
const parquet = @import("../lib.zig");
const DynamicReader = parquet.DynamicReader;
const RowWriter = parquet.RowWriter;
const Value = parquet.Value;
const Row = parquet.Row;
const build_options = @import("build_options");

// =============================================================================
// Test: Basic primitives roundtrip
// =============================================================================

test "DynamicReader: basic primitives roundtrip" {
    const allocator = std.testing.allocator;

    const TestRecord = struct {
        id: i32,
        value: i64,
        score: f64,
        ratio: f32,
        active: bool,
    };

    const tmp_path = "test_dynamic_primitives.parquet";
    defer std.fs.cwd().deleteFile(tmp_path) catch {};

    // Write test data
    {
        const file = try std.fs.cwd().createFile(tmp_path, .{});
        defer file.close();

        var writer = try parquet.writeToFileRows(TestRecord, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{ .id = 1, .value = 100, .score = 1.5, .ratio = 0.5, .active = true });
        try writer.writeRow(.{ .id = 2, .value = 200, .score = 2.5, .ratio = 1.5, .active = false });
        try writer.writeRow(.{ .id = 3, .value = 300, .score = 3.5, .ratio = 2.5, .active = true });
        try writer.close();
    }

    // Read back with DynamicReader
    const file = try std.fs.cwd().openFile(tmp_path, .{});
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    // Verify metadata
    try std.testing.expectEqual(@as(usize, 1), reader.getNumRowGroups());
    try std.testing.expectEqual(@as(i64, 3), reader.getTotalNumRows());
    try std.testing.expectEqual(@as(usize, 5), reader.getNumColumns());

    // Read all rows
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 3), rows.len);

    // Verify row 0
    try std.testing.expectEqual(@as(usize, 5), rows[0].columnCount());
    try std.testing.expectEqual(@as(?i32, 1), rows[0].getColumn(0).?.asInt32());
    try std.testing.expectEqual(@as(?i64, 100), rows[0].getColumn(1).?.asInt64());
    try std.testing.expectEqual(@as(?f64, 1.5), rows[0].getColumn(2).?.asDouble());
    try std.testing.expectEqual(@as(?f32, 0.5), rows[0].getColumn(3).?.asFloat());
    try std.testing.expectEqual(@as(?bool, true), rows[0].getColumn(4).?.asBool());

    // Verify row 1
    try std.testing.expectEqual(@as(?i32, 2), rows[1].getColumn(0).?.asInt32());
    try std.testing.expectEqual(@as(?i64, 200), rows[1].getColumn(1).?.asInt64());
    try std.testing.expectEqual(@as(?bool, false), rows[1].getColumn(4).?.asBool());

    // Verify row 2
    try std.testing.expectEqual(@as(?i32, 3), rows[2].getColumn(0).?.asInt32());
    try std.testing.expectEqual(@as(?i64, 300), rows[2].getColumn(1).?.asInt64());
}

// =============================================================================
// Test: String columns roundtrip
// =============================================================================

test "DynamicReader: string columns roundtrip" {
    const allocator = std.testing.allocator;

    const StringRecord = struct {
        id: i32,
        name: []const u8,
    };

    const tmp_path = "test_dynamic_strings.parquet";
    defer std.fs.cwd().deleteFile(tmp_path) catch {};

    {
        const file = try std.fs.cwd().createFile(tmp_path, .{});
        defer file.close();

        var writer = try parquet.writeToFileRows(StringRecord, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{ .id = 1, .name = "Alice" });
        try writer.writeRow(.{ .id = 2, .name = "Bob" });
        try writer.writeRow(.{ .id = 3, .name = "Charlie" });
        try writer.close();
    }

    const file = try std.fs.cwd().openFile(tmp_path, .{});
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 3), rows.len);

    // Verify strings
    try std.testing.expectEqualStrings("Alice", rows[0].getColumn(1).?.asBytes().?);
    try std.testing.expectEqualStrings("Bob", rows[1].getColumn(1).?.asBytes().?);
    try std.testing.expectEqualStrings("Charlie", rows[2].getColumn(1).?.asBytes().?);
}

// =============================================================================
// Test: Optional fields roundtrip
// =============================================================================

test "DynamicReader: optional fields roundtrip" {
    const allocator = std.testing.allocator;

    const OptionalRecord = struct {
        id: i32,
        maybe_value: ?i64,
        maybe_score: ?f64,
    };

    const tmp_path = "test_dynamic_optionals.parquet";
    defer std.fs.cwd().deleteFile(tmp_path) catch {};

    {
        const file = try std.fs.cwd().createFile(tmp_path, .{});
        defer file.close();

        var writer = try parquet.writeToFileRows(OptionalRecord, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{ .id = 1, .maybe_value = 100, .maybe_score = 1.5 });
        try writer.writeRow(.{ .id = 2, .maybe_value = null, .maybe_score = 2.5 });
        try writer.writeRow(.{ .id = 3, .maybe_value = 300, .maybe_score = null });
        try writer.writeRow(.{ .id = 4, .maybe_value = null, .maybe_score = null });
        try writer.close();
    }

    const file = try std.fs.cwd().openFile(tmp_path, .{});
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 4), rows.len);

    // Row 0: all present
    try std.testing.expectEqual(@as(?i64, 100), rows[0].getColumn(1).?.asInt64());
    try std.testing.expectEqual(@as(?f64, 1.5), rows[0].getColumn(2).?.asDouble());

    // Row 1: first null
    try std.testing.expect(rows[1].getColumn(1).?.isNull());
    try std.testing.expectEqual(@as(?f64, 2.5), rows[1].getColumn(2).?.asDouble());

    // Row 2: second null
    try std.testing.expectEqual(@as(?i64, 300), rows[2].getColumn(1).?.asInt64());
    try std.testing.expect(rows[2].getColumn(2).?.isNull());

    // Row 3: both null
    try std.testing.expect(rows[3].getColumn(1).?.isNull());
    try std.testing.expect(rows[3].getColumn(2).?.isNull());
}

// =============================================================================
// Test: Multiple row groups
// =============================================================================

test "DynamicReader: multiple row groups" {
    const allocator = std.testing.allocator;

    const SimpleRecord = struct {
        id: i32,
        value: i64,
    };

    const tmp_path = "test_dynamic_multirowgroup.parquet";
    defer std.fs.cwd().deleteFile(tmp_path) catch {};

    {
        const file = try std.fs.cwd().createFile(tmp_path, .{});
        defer file.close();

        var writer = try parquet.writeToFileRows(SimpleRecord, allocator, file, .{});
        defer writer.deinit();

        // First row group
        try writer.writeRow(.{ .id = 1, .value = 100 });
        try writer.writeRow(.{ .id = 2, .value = 200 });
        try writer.flush();

        // Second row group
        try writer.writeRow(.{ .id = 3, .value = 300 });
        try writer.writeRow(.{ .id = 4, .value = 400 });
        try writer.close();
    }

    const file = try std.fs.cwd().openFile(tmp_path, .{});
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 2), reader.getNumRowGroups());
    try std.testing.expectEqual(@as(i64, 4), reader.getTotalNumRows());

    // Read first row group
    const rows1 = try reader.readAllRows(0);
    defer {
        for (rows1) |row| row.deinit();
        allocator.free(rows1);
    }

    try std.testing.expectEqual(@as(usize, 2), rows1.len);
    try std.testing.expectEqual(@as(?i32, 1), rows1[0].getColumn(0).?.asInt32());
    try std.testing.expectEqual(@as(?i32, 2), rows1[1].getColumn(0).?.asInt32());

    // Read second row group
    const rows2 = try reader.readAllRows(1);
    defer {
        for (rows2) |row| row.deinit();
        allocator.free(rows2);
    }

    try std.testing.expectEqual(@as(usize, 2), rows2.len);
    try std.testing.expectEqual(@as(?i32, 3), rows2[0].getColumn(0).?.asInt32());
    try std.testing.expectEqual(@as(?i32, 4), rows2[1].getColumn(0).?.asInt32());
}

// =============================================================================
// Test: Compression roundtrip
// =============================================================================

test "DynamicReader: compressed data roundtrip" {
    if (build_options.no_compression) return;
    const allocator = std.testing.allocator;

    const CompressedRecord = struct {
        id: i32,
        name: []const u8,
        value: i64,
    };

    const tmp_path = "test_dynamic_compressed.parquet";
    defer std.fs.cwd().deleteFile(tmp_path) catch {};

    {
        const file = try std.fs.cwd().createFile(tmp_path, .{});
        defer file.close();

        var writer = try parquet.writeToFileRows(CompressedRecord, allocator, file, .{
            .compression = .zstd,
        });
        defer writer.deinit();

        try writer.writeRow(.{ .id = 1, .name = "First", .value = 100 });
        try writer.writeRow(.{ .id = 2, .name = "Second", .value = 200 });
        try writer.close();
    }

    const file = try std.fs.cwd().openFile(tmp_path, .{});
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 2), rows.len);
    try std.testing.expectEqual(@as(?i32, 1), rows[0].getColumn(0).?.asInt32());
    try std.testing.expectEqualStrings("First", rows[0].getColumn(1).?.asBytes().?);
    try std.testing.expectEqual(@as(?i64, 100), rows[0].getColumn(2).?.asInt64());
}

// =============================================================================
// Test: Dictionary encoding roundtrip
// =============================================================================

test "DynamicReader: dictionary encoded data roundtrip" {
    const allocator = std.testing.allocator;

    const DictRecord = struct {
        id: i32,
        category: []const u8,
    };

    const tmp_path = "test_dynamic_dict.parquet";
    defer std.fs.cwd().deleteFile(tmp_path) catch {};

    {
        const file = try std.fs.cwd().createFile(tmp_path, .{});
        defer file.close();

        var writer = try parquet.writeToFileRows(DictRecord, allocator, file, .{
            .use_dictionary = true,
        });
        defer writer.deinit();

        // Repeated values to test dictionary efficiency
        try writer.writeRow(.{ .id = 1, .category = "A" });
        try writer.writeRow(.{ .id = 2, .category = "B" });
        try writer.writeRow(.{ .id = 3, .category = "A" });
        try writer.writeRow(.{ .id = 4, .category = "B" });
        try writer.writeRow(.{ .id = 5, .category = "A" });
        try writer.close();
    }

    const file = try std.fs.cwd().openFile(tmp_path, .{});
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 5), rows.len);
    try std.testing.expectEqualStrings("A", rows[0].getColumn(1).?.asBytes().?);
    try std.testing.expectEqualStrings("B", rows[1].getColumn(1).?.asBytes().?);
    try std.testing.expectEqualStrings("A", rows[2].getColumn(1).?.asBytes().?);
    try std.testing.expectEqualStrings("B", rows[3].getColumn(1).?.asBytes().?);
    try std.testing.expectEqualStrings("A", rows[4].getColumn(1).?.asBytes().?);
}
