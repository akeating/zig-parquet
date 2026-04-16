//! INTERVAL Type Tests
//!
//! Tests for INTERVAL type support:
//! - Round-trip write/read with Writer/Reader API
//! - Round-trip write/read with DynamicWriter/DynamicReader API
//! - Nullable interval columns
//! - Statistics are NOT written (per Parquet spec, sort order is undefined)

const std = @import("std");
const io = std.testing.io;
const parquet = @import("../lib.zig");
const format = parquet.format;
const types = parquet.types;
const Interval = types.Interval;
const TypeInfo = parquet.TypeInfo;

test "Interval struct basic operations" {
    // Test fromComponents
    const interval1 = Interval.fromComponents(12, 30, 3600000);
    try std.testing.expectEqual(@as(u32, 12), interval1.months);
    try std.testing.expectEqual(@as(u32, 30), interval1.days);
    try std.testing.expectEqual(@as(u32, 3600000), interval1.millis);

    // Test toBytes/fromBytes round-trip
    const bytes = interval1.toBytes();
    const interval2 = Interval.fromBytes(bytes);
    try std.testing.expectEqual(interval1.months, interval2.months);
    try std.testing.expectEqual(interval1.days, interval2.days);
    try std.testing.expectEqual(interval1.millis, interval2.millis);

    // Test convenience constructors
    const from_months = Interval.fromMonths(6);
    try std.testing.expectEqual(@as(u32, 6), from_months.months);
    try std.testing.expectEqual(@as(u32, 0), from_months.days);

    const from_hours = Interval.fromHours(2);
    try std.testing.expectEqual(@as(u32, 7200000), from_hours.millis);
}

test "round-trip INTERVAL with Writer/Reader API" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_interval.parquet";

    // Write INTERVAL column
    {
        const file = try tmp_dir.dir.createFile(io, file_path, .{});
        defer file.close(io);

        const columns = [_]parquet.ColumnDef{
            parquet.ColumnDef.interval("duration", false),
        };

        var writer = try parquet.writeToFile(allocator, file, io, &columns);
        defer writer.deinit();

        // Create interval values as raw bytes
        const interval1 = Interval.fromComponents(1, 15, 3600000); // 1 month, 15 days, 1 hour
        const interval2 = Interval.fromComponents(0, 0, 1000); // 1 second
        const interval3 = Interval.fromComponents(12, 0, 0); // 1 year

        const values = [_][]const u8{
            &interval1.toBytes(),
            &interval2.toBytes(),
            &interval3.toBytes(),
        };

        // Write as fixed byte array (INTERVAL is FIXED_LEN_BYTE_ARRAY(12))
        try writer.writeColumnFixedByteArray(0, &values);
        try writer.close();
    }

    // Read back and verify schema
    {
        const file = try tmp_dir.dir.openFile(io, file_path, .{});
        defer file.close(io);

        var reader = try parquet.openFileDynamic(allocator, file, io, .{});
        defer reader.deinit();

        const schema = reader.getSchema();
        const col_schema = schema[1]; // First column (schema[0] is root)

        // Verify physical type is FIXED_LEN_BYTE_ARRAY
        try std.testing.expectEqual(format.PhysicalType.fixed_len_byte_array, col_schema.type_.?);

        // Verify type_length is 12
        try std.testing.expectEqual(@as(i32, 12), col_schema.type_length.?);

        // Verify converted_type is INTERVAL (21)
        try std.testing.expect(col_schema.converted_type != null);
        try std.testing.expectEqual(@as(i32, format.ConvertedType.INTERVAL), col_schema.converted_type.?);

        // Verify logical_type is NOT set (INTERVAL uses ConvertedType only)
        try std.testing.expect(col_schema.logical_type == null);

        // Read the values back
        const rows = try reader.readAllRows(0);
        defer {
            for (rows) |row| row.deinit();
            allocator.free(rows);
        }

        try std.testing.expectEqual(@as(usize, 3), rows.len);

        // Verify first interval: 1 month, 15 days, 1 hour
        const read_interval1 = Interval.fromBytes(rows[0].getColumn(0).?.asBytes().?[0..12].*);
        try std.testing.expectEqual(@as(u32, 1), read_interval1.months);
        try std.testing.expectEqual(@as(u32, 15), read_interval1.days);
        try std.testing.expectEqual(@as(u32, 3600000), read_interval1.millis);
    }
}

test "round-trip INTERVAL with DynamicWriter/DynamicReader API" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_interval_row.parquet";

    // Write using DynamicWriter
    {
        const file = try tmp_dir.dir.createFile(io, file_path, .{});
        defer file.close(io);

        var writer = try parquet.createFileDynamic(allocator, file, io);
        defer writer.deinit();

        try writer.addColumn("duration", TypeInfo.interval, .{});
        try writer.begin();

        const iv1 = Interval.fromComponents(1, 15, 3600000);
        try writer.setBytes(0, &iv1.toBytes());
        try writer.addRow();

        const iv2 = Interval.fromDays(30);
        try writer.setBytes(0, &iv2.toBytes());
        try writer.addRow();

        const iv3 = Interval.fromHours(24);
        try writer.setBytes(0, &iv3.toBytes());
        try writer.addRow();

        try writer.close();
    }

    // Read using DynamicReader
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

        // Row 0: 1 month, 15 days, 1 hour
        const bytes0 = rows[0].getColumn(0).?.asBytes().?;
        const iv0 = Interval.fromBytes(bytes0[0..12].*);
        try std.testing.expectEqual(@as(u32, 1), iv0.months);
        try std.testing.expectEqual(@as(u32, 15), iv0.days);
        try std.testing.expectEqual(@as(u32, 3600000), iv0.millis);

        // Row 1: 30 days
        const bytes1 = rows[1].getColumn(0).?.asBytes().?;
        const iv1 = Interval.fromBytes(bytes1[0..12].*);
        try std.testing.expectEqual(@as(u32, 0), iv1.months);
        try std.testing.expectEqual(@as(u32, 30), iv1.days);

        // Row 2: 24 hours
        const bytes2 = rows[2].getColumn(0).?.asBytes().?;
        const iv2 = Interval.fromBytes(bytes2[0..12].*);
        try std.testing.expectEqual(@as(u32, 86400000), iv2.millis);
    }
}

test "round-trip nullable INTERVAL with DynamicWriter/DynamicReader API" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_interval_nullable.parquet";

    // Write using DynamicWriter
    {
        const file = try tmp_dir.dir.createFile(io, file_path, .{});
        defer file.close(io);

        var writer = try parquet.createFileDynamic(allocator, file, io);
        defer writer.deinit();

        try writer.addColumn("duration", TypeInfo.interval, .{});
        try writer.begin();

        const iv1 = Interval.fromMonths(6);
        try writer.setBytes(0, &iv1.toBytes());
        try writer.addRow();

        try writer.setNull(0);
        try writer.addRow();

        const iv2 = Interval.fromSeconds(90);
        try writer.setBytes(0, &iv2.toBytes());
        try writer.addRow();

        try writer.close();
    }

    // Read using DynamicReader
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

        // Row 0: 6 months
        const val0 = rows[0].getColumn(0).?;
        try std.testing.expect(!val0.isNull());
        const bytes0 = val0.asBytes().?;
        const iv0 = Interval.fromBytes(bytes0[0..12].*);
        try std.testing.expectEqual(@as(u32, 6), iv0.months);

        // Row 1: null
        try std.testing.expect(rows[1].getColumn(0).?.isNull());

        // Row 2: 90 seconds
        const val2 = rows[2].getColumn(0).?;
        try std.testing.expect(!val2.isNull());
        const bytes2 = val2.asBytes().?;
        const iv2 = Interval.fromBytes(bytes2[0..12].*);
        try std.testing.expectEqual(@as(u32, 90000), iv2.millis);
    }
}

test "INTERVAL statistics are not written" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "interval_no_stats.parquet";

    // Write INTERVAL column
    {
        const file = try tmp_dir.dir.createFile(io, file_path, .{});
        defer file.close(io);

        const columns = [_]parquet.ColumnDef{
            parquet.ColumnDef.interval("duration", false),
        };

        var writer = try parquet.writeToFile(allocator, file, io, &columns);
        defer writer.deinit();

        const interval1 = Interval.fromMonths(1);
        const interval2 = Interval.fromMonths(12);

        const values = [_][]const u8{
            &interval1.toBytes(),
            &interval2.toBytes(),
        };

        try writer.writeColumnFixedByteArray(0, &values);
        try writer.close();
    }

    // Read and verify statistics are NOT present
    {
        const file = try tmp_dir.dir.openFile(io, file_path, .{});
        defer file.close(io);

        var reader = try parquet.openFileDynamic(allocator, file, io, .{});
        defer reader.deinit();

        // Get statistics for the INTERVAL column
        const stats = reader.getColumnStatistics(0, 0);

        // Statistics should be null for INTERVAL columns
        try std.testing.expect(stats == null);
    }
}

test "INTERVAL ColumnDef factory method" {
    const col = parquet.ColumnDef.interval("test_interval", true);

    try std.testing.expectEqual(format.PhysicalType.fixed_len_byte_array, col.type_);
    try std.testing.expectEqual(@as(?i32, 12), col.type_length);
    try std.testing.expect(col.optional);
    try std.testing.expect(col.logical_type == null);
    try std.testing.expectEqual(@as(?i32, format.ConvertedType.INTERVAL), col.converted_type);
}

test "INTERVAL byte layout is little-endian" {
    // Create an interval with known values
    const interval = Interval.fromComponents(0x01020304, 0x05060708, 0x090A0B0C);
    const bytes = interval.toBytes();

    // Months: 0x01020304 in little-endian = 04 03 02 01
    try std.testing.expectEqual(@as(u8, 0x04), bytes[0]);
    try std.testing.expectEqual(@as(u8, 0x03), bytes[1]);
    try std.testing.expectEqual(@as(u8, 0x02), bytes[2]);
    try std.testing.expectEqual(@as(u8, 0x01), bytes[3]);

    // Days: 0x05060708 in little-endian = 08 07 06 05
    try std.testing.expectEqual(@as(u8, 0x08), bytes[4]);
    try std.testing.expectEqual(@as(u8, 0x07), bytes[5]);
    try std.testing.expectEqual(@as(u8, 0x06), bytes[6]);
    try std.testing.expectEqual(@as(u8, 0x05), bytes[7]);

    // Millis: 0x090A0B0C in little-endian = 0C 0B 0A 09
    try std.testing.expectEqual(@as(u8, 0x0C), bytes[8]);
    try std.testing.expectEqual(@as(u8, 0x0B), bytes[9]);
    try std.testing.expectEqual(@as(u8, 0x0A), bytes[10]);
    try std.testing.expectEqual(@as(u8, 0x09), bytes[11]);
}
