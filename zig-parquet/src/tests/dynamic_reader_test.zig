//! Tests for DynamicReader
//!
//! Verifies schema-agnostic reading by writing with DynamicWriter
//! and reading back with DynamicReader.

const std = @import("std");
const io = std.testing.io;
const parquet = @import("../lib.zig");
const DynamicReader = parquet.DynamicReader;
const DynamicWriter = parquet.DynamicWriter;
const TypeInfo = parquet.TypeInfo;
const Value = parquet.Value;
const Row = parquet.Row;
const build_options = @import("build_options");

// =============================================================================
// Test: Basic primitives roundtrip
// =============================================================================

test "DynamicReader: basic primitives roundtrip" {
    const allocator = std.testing.allocator;

    const tmp_path = "test_dynamic_primitives.parquet";
    defer std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};

    // Write test data using DynamicWriter
    {
        const file = try std.Io.Dir.cwd().createFile(io, tmp_path, .{});
        defer file.close(io);

        var writer = try parquet.createFileDynamic(allocator, file, io);
        defer writer.deinit();

        try writer.addColumn("id", TypeInfo.int32, .{});
        try writer.addColumn("value", TypeInfo.int64, .{});
        try writer.addColumn("score", TypeInfo.double_, .{});
        try writer.addColumn("ratio", TypeInfo.float_, .{});
        try writer.addColumn("active", TypeInfo.bool_, .{});
        try writer.begin();

        try writer.setInt32(0, 1);
        try writer.setInt64(1, 100);
        try writer.setDouble(2, 1.5);
        try writer.setFloat(3, 0.5);
        try writer.setBool(4, true);
        try writer.addRow();

        try writer.setInt32(0, 2);
        try writer.setInt64(1, 200);
        try writer.setDouble(2, 2.5);
        try writer.setFloat(3, 1.5);
        try writer.setBool(4, false);
        try writer.addRow();

        try writer.setInt32(0, 3);
        try writer.setInt64(1, 300);
        try writer.setDouble(2, 3.5);
        try writer.setFloat(3, 2.5);
        try writer.setBool(4, true);
        try writer.addRow();

        try writer.close();
    }

    // Read back with DynamicReader
    const file = try std.Io.Dir.cwd().openFile(io, tmp_path, .{});
    defer file.close(io);

    var reader = try parquet.openFileDynamic(allocator, file, io, .{});
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

    const tmp_path = "test_dynamic_strings.parquet";
    defer std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};

    {
        const file = try std.Io.Dir.cwd().createFile(io, tmp_path, .{});
        defer file.close(io);

        var writer = try parquet.createFileDynamic(allocator, file, io);
        defer writer.deinit();

        try writer.addColumn("id", TypeInfo.int32, .{});
        try writer.addColumn("name", TypeInfo.string, .{});
        try writer.begin();

        try writer.setInt32(0, 1);
        try writer.setBytes(1, "Alice");
        try writer.addRow();

        try writer.setInt32(0, 2);
        try writer.setBytes(1, "Bob");
        try writer.addRow();

        try writer.setInt32(0, 3);
        try writer.setBytes(1, "Charlie");
        try writer.addRow();

        try writer.close();
    }

    const file = try std.Io.Dir.cwd().openFile(io, tmp_path, .{});
    defer file.close(io);

    var reader = try parquet.openFileDynamic(allocator, file, io, .{});
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

    const tmp_path = "test_dynamic_optionals.parquet";
    defer std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};

    {
        const file = try std.Io.Dir.cwd().createFile(io, tmp_path, .{});
        defer file.close(io);

        var writer = try parquet.createFileDynamic(allocator, file, io);
        defer writer.deinit();

        try writer.addColumn("id", TypeInfo.int32, .{});
        try writer.addColumn("maybe_value", TypeInfo.int64, .{});
        try writer.addColumn("maybe_score", TypeInfo.double_, .{});
        try writer.begin();

        // Row 0: all present
        try writer.setInt32(0, 1);
        try writer.setInt64(1, 100);
        try writer.setDouble(2, 1.5);
        try writer.addRow();

        // Row 1: first null
        try writer.setInt32(0, 2);
        try writer.setNull(1);
        try writer.setDouble(2, 2.5);
        try writer.addRow();

        // Row 2: second null
        try writer.setInt32(0, 3);
        try writer.setInt64(1, 300);
        try writer.setNull(2);
        try writer.addRow();

        // Row 3: both null
        try writer.setInt32(0, 4);
        try writer.setNull(1);
        try writer.setNull(2);
        try writer.addRow();

        try writer.close();
    }

    const file = try std.Io.Dir.cwd().openFile(io, tmp_path, .{});
    defer file.close(io);

    var reader = try parquet.openFileDynamic(allocator, file, io, .{});
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

    const tmp_path = "test_dynamic_multirowgroup.parquet";
    defer std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};

    {
        const file = try std.Io.Dir.cwd().createFile(io, tmp_path, .{});
        defer file.close(io);

        var writer = try parquet.createFileDynamic(allocator, file, io);
        defer writer.deinit();

        try writer.addColumn("id", TypeInfo.int32, .{});
        try writer.addColumn("value", TypeInfo.int64, .{});
        writer.setRowGroupSize(2);
        try writer.begin();

        try writer.setInt32(0, 1);
        try writer.setInt64(1, 100);
        try writer.addRow();
        try writer.setInt32(0, 2);
        try writer.setInt64(1, 200);
        try writer.addRow();
        // auto-flush after 2 rows

        try writer.setInt32(0, 3);
        try writer.setInt64(1, 300);
        try writer.addRow();
        try writer.setInt32(0, 4);
        try writer.setInt64(1, 400);
        try writer.addRow();

        try writer.close();
    }

    const file = try std.Io.Dir.cwd().openFile(io, tmp_path, .{});
    defer file.close(io);

    var reader = try parquet.openFileDynamic(allocator, file, io, .{});
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
    if (!build_options.supports_zstd) return;
    const allocator = std.testing.allocator;

    const tmp_path = "test_dynamic_compressed.parquet";
    defer std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};

    {
        const file = try std.Io.Dir.cwd().createFile(io, tmp_path, .{});
        defer file.close(io);

        var writer = try parquet.createFileDynamic(allocator, file, io);
        defer writer.deinit();

        try writer.addColumn("id", TypeInfo.int32, .{});
        try writer.addColumn("name", TypeInfo.string, .{});
        try writer.addColumn("value", TypeInfo.int64, .{});
        writer.setCompression(.zstd);
        try writer.begin();

        try writer.setInt32(0, 1);
        try writer.setBytes(1, "First");
        try writer.setInt64(2, 100);
        try writer.addRow();

        try writer.setInt32(0, 2);
        try writer.setBytes(1, "Second");
        try writer.setInt64(2, 200);
        try writer.addRow();

        try writer.close();
    }

    const file = try std.Io.Dir.cwd().openFile(io, tmp_path, .{});
    defer file.close(io);

    var reader = try parquet.openFileDynamic(allocator, file, io, .{});
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

    const tmp_path = "test_dynamic_dict.parquet";
    defer std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};

    {
        const file = try std.Io.Dir.cwd().createFile(io, tmp_path, .{});
        defer file.close(io);

        var writer = try parquet.createFileDynamic(allocator, file, io);
        defer writer.deinit();

        try writer.addColumn("id", TypeInfo.int32, .{});
        try writer.addColumn("category", TypeInfo.string, .{});
        try writer.begin();

        // Repeated values to test dictionary efficiency
        try writer.setInt32(0, 1);
        try writer.setBytes(1, "A");
        try writer.addRow();
        try writer.setInt32(0, 2);
        try writer.setBytes(1, "B");
        try writer.addRow();
        try writer.setInt32(0, 3);
        try writer.setBytes(1, "A");
        try writer.addRow();
        try writer.setInt32(0, 4);
        try writer.setBytes(1, "B");
        try writer.addRow();
        try writer.setInt32(0, 5);
        try writer.setBytes(1, "A");
        try writer.addRow();

        try writer.close();
    }

    const file = try std.Io.Dir.cwd().openFile(io, tmp_path, .{});
    defer file.close(io);

    var reader = try parquet.openFileDynamic(allocator, file, io, .{});
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
