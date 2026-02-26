//! Tests for RowWriter

const std = @import("std");
const parquet = @import("../lib.zig");
const RowWriter = parquet.RowWriter;
const Reader = parquet.Reader;

// =============================================================================
// Test Structs
// =============================================================================

const SensorReading = struct {
    sensor_id: i32,
    value: i64,
    temperature: f64,
};

const MixedTypes = struct {
    flag: bool,
    count: i32,
    amount: i64,
    ratio: f32,
    score: f64,
};

const WithStrings = struct {
    id: i32,
    name: []const u8,
};

const WithOptionals = struct {
    id: i32,
    value: ?i64,
    name: ?[]const u8,
};

// =============================================================================
// Basic Round-trip Tests
// =============================================================================

test "RowWriter: basic round-trip with SensorReading" {
    const allocator = std.testing.allocator;

    // Create temp file
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("sensor.parquet", .{ .read = true });
    defer file.close();

    // Write data
    {
        var writer = try parquet.writeToFileRows(SensorReading, allocator, file, .{});
        defer writer.deinit();

        const readings = [_]SensorReading{
            .{ .sensor_id = 1, .value = 100, .temperature = 23.5 },
            .{ .sensor_id = 2, .value = 200, .temperature = 24.0 },
            .{ .sensor_id = 3, .value = 150, .temperature = 22.8 },
        };

        try writer.writeRows(&readings);
        try writer.close();
    }

    // Read back and verify
    try file.seekTo(0);
    {
        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        // Verify metadata
        try std.testing.expectEqual(@as(i64, 3), reader.metadata.num_rows);
        try std.testing.expectEqual(@as(usize, 1), reader.getNumRowGroups());

        // Read columns
        const sensor_ids = try reader.readColumn(0, i32);
        defer allocator.free(sensor_ids);

        const values = try reader.readColumn(1, i64);
        defer allocator.free(values);

        const temps = try reader.readColumn(2, f64);
        defer allocator.free(temps);

        // Verify data
        try std.testing.expectEqual(@as(i32, 1), sensor_ids[0].value);
        try std.testing.expectEqual(@as(i32, 2), sensor_ids[1].value);
        try std.testing.expectEqual(@as(i32, 3), sensor_ids[2].value);

        try std.testing.expectEqual(@as(i64, 100), values[0].value);
        try std.testing.expectEqual(@as(i64, 200), values[1].value);
        try std.testing.expectEqual(@as(i64, 150), values[2].value);

        try std.testing.expectApproxEqAbs(@as(f64, 23.5), temps[0].value, 0.001);
        try std.testing.expectApproxEqAbs(@as(f64, 24.0), temps[1].value, 0.001);
        try std.testing.expectApproxEqAbs(@as(f64, 22.8), temps[2].value, 0.001);
    }
}

test "RowWriter: writeRow single row API" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("single_row.parquet", .{ .read = true });
    defer file.close();

    // Write using single row API
    {
        var writer = try parquet.writeToFileRows(SensorReading, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{ .sensor_id = 10, .value = 500, .temperature = 30.0 });
        try writer.writeRow(.{ .sensor_id = 20, .value = 600, .temperature = 31.5 });
        try writer.close();
    }

    // Read back and verify
    try file.seekTo(0);
    {
        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 2), reader.metadata.num_rows);

        const sensor_ids = try reader.readColumn(0, i32);
        defer allocator.free(sensor_ids);

        try std.testing.expectEqual(@as(i32, 10), sensor_ids[0].value);
        try std.testing.expectEqual(@as(i32, 20), sensor_ids[1].value);
    }
}

test "RowWriter: multiple types" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("mixed.parquet", .{ .read = true });
    defer file.close();

    // Write
    {
        var writer = try parquet.writeToFileRows(MixedTypes, allocator, file, .{});
        defer writer.deinit();

        const rows = [_]MixedTypes{
            .{ .flag = true, .count = 42, .amount = 1000000, .ratio = 0.5, .score = 99.9 },
            .{ .flag = false, .count = -10, .amount = -500, .ratio = 1.5, .score = 0.0 },
        };

        try writer.writeRows(&rows);
        try writer.close();
    }

    // Read back
    try file.seekTo(0);
    {
        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 2), reader.metadata.num_rows);

        const flags = try reader.readColumn(0, bool);
        defer allocator.free(flags);

        const counts = try reader.readColumn(1, i32);
        defer allocator.free(counts);

        try std.testing.expectEqual(true, flags[0].value);
        try std.testing.expectEqual(false, flags[1].value);
        try std.testing.expectEqual(@as(i32, 42), counts[0].value);
        try std.testing.expectEqual(@as(i32, -10), counts[1].value);
    }
}

test "RowWriter: with string fields" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("strings.parquet", .{ .read = true });
    defer file.close();

    // Write
    {
        var writer = try parquet.writeToFileRows(WithStrings, allocator, file, .{});
        defer writer.deinit();

        const rows = [_]WithStrings{
            .{ .id = 1, .name = "Alice" },
            .{ .id = 2, .name = "Bob" },
            .{ .id = 3, .name = "Charlie" },
        };

        try writer.writeRows(&rows);
        try writer.close();
    }

    // Read back
    try file.seekTo(0);
    {
        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 3), reader.metadata.num_rows);

        const ids = try reader.readColumn(0, i32);
        defer allocator.free(ids);

        const names = try reader.readColumn(1, []const u8);
        defer {
            for (names) |n| {
                if (!n.isNull()) allocator.free(n.value);
            }
            allocator.free(names);
        }

        try std.testing.expectEqual(@as(i32, 1), ids[0].value);
        try std.testing.expectEqual(@as(i32, 2), ids[1].value);
        try std.testing.expectEqual(@as(i32, 3), ids[2].value);

        try std.testing.expectEqualStrings("Alice", names[0].value);
        try std.testing.expectEqualStrings("Bob", names[1].value);
        try std.testing.expectEqualStrings("Charlie", names[2].value);
    }
}

// =============================================================================
// Multi-flush (Multiple Row Groups) Tests
// =============================================================================

test "RowWriter: multiple flushes create multiple row groups" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("multi_rg.parquet", .{ .read = true });
    defer file.close();

    // Write with explicit flushes
    {
        var writer = try parquet.writeToFileRows(SensorReading, allocator, file, .{});
        defer writer.deinit();

        // First batch
        const batch1 = [_]SensorReading{
            .{ .sensor_id = 1, .value = 100, .temperature = 20.0 },
            .{ .sensor_id = 2, .value = 200, .temperature = 21.0 },
        };
        try writer.writeRows(&batch1);
        try writer.flush();

        // Verify row group count after first flush
        try std.testing.expectEqual(@as(usize, 1), writer.rowGroupCount());

        // Second batch
        const batch2 = [_]SensorReading{
            .{ .sensor_id = 3, .value = 300, .temperature = 22.0 },
        };
        try writer.writeRows(&batch2);
        try writer.flush();

        // Verify row group count after second flush
        try std.testing.expectEqual(@as(usize, 2), writer.rowGroupCount());

        // Third batch (will be flushed on close)
        const batch3 = [_]SensorReading{
            .{ .sensor_id = 4, .value = 400, .temperature = 23.0 },
            .{ .sensor_id = 5, .value = 500, .temperature = 24.0 },
        };
        try writer.writeRows(&batch3);

        try writer.close();
    }

    // Read back and verify
    try file.seekTo(0);
    {
        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        // Should have 3 row groups
        try std.testing.expectEqual(@as(usize, 3), reader.getNumRowGroups());

        // Total rows: 2 + 1 + 2 = 5
        try std.testing.expectEqual(@as(i64, 5), reader.metadata.num_rows);

        // Check row counts per row group
        try std.testing.expectEqual(@as(?i64, 2), reader.getRowGroupNumRows(0));
        try std.testing.expectEqual(@as(?i64, 1), reader.getRowGroupNumRows(1));
        try std.testing.expectEqual(@as(?i64, 2), reader.getRowGroupNumRows(2));

        // Read sensor IDs from each row group
        // readColumn reads from first row group only, so we check row groups individually
        const rg0_ids = try reader.readColumnFromRowGroup(0, 0, i32);
        defer allocator.free(rg0_ids);
        try std.testing.expectEqual(@as(usize, 2), rg0_ids.len);
        try std.testing.expectEqual(@as(i32, 1), rg0_ids[0].value);
        try std.testing.expectEqual(@as(i32, 2), rg0_ids[1].value);

        const rg1_ids = try reader.readColumnFromRowGroup(0, 1, i32);
        defer allocator.free(rg1_ids);
        try std.testing.expectEqual(@as(usize, 1), rg1_ids.len);
        try std.testing.expectEqual(@as(i32, 3), rg1_ids[0].value);

        const rg2_ids = try reader.readColumnFromRowGroup(0, 2, i32);
        defer allocator.free(rg2_ids);
        try std.testing.expectEqual(@as(usize, 2), rg2_ids.len);
        try std.testing.expectEqual(@as(i32, 4), rg2_ids[0].value);
        try std.testing.expectEqual(@as(i32, 5), rg2_ids[1].value);
    }
}

test "RowWriter: empty flush is no-op" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("empty_flush.parquet", .{ .read = true });
    defer file.close();

    {
        var writer = try parquet.writeToFileRows(SensorReading, allocator, file, .{});
        defer writer.deinit();

        // Flush with no data - should be no-op
        try writer.flush();
        try std.testing.expectEqual(@as(usize, 0), writer.rowGroupCount());

        // Write some data
        try writer.writeRow(.{ .sensor_id = 1, .value = 100, .temperature = 20.0 });
        try writer.flush();
        try std.testing.expectEqual(@as(usize, 1), writer.rowGroupCount());

        // Another empty flush
        try writer.flush();
        try std.testing.expectEqual(@as(usize, 1), writer.rowGroupCount());

        try writer.close();
    }

    // Verify file is valid
    try file.seekTo(0);
    {
        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 1), reader.metadata.num_rows);
        try std.testing.expectEqual(@as(usize, 1), reader.getNumRowGroups());
    }
}

// =============================================================================
// Buffered Row Count Tests
// =============================================================================

test "RowWriter: bufferedRowCount tracks pending rows" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("buffered.parquet", .{ .read = true });
    defer file.close();

    {
        var writer = try parquet.writeToFileRows(SensorReading, allocator, file, .{});
        defer writer.deinit();

        try std.testing.expectEqual(@as(usize, 0), writer.bufferedRowCount());

        try writer.writeRow(.{ .sensor_id = 1, .value = 100, .temperature = 20.0 });
        try std.testing.expectEqual(@as(usize, 1), writer.bufferedRowCount());

        const batch = [_]SensorReading{
            .{ .sensor_id = 2, .value = 200, .temperature = 21.0 },
            .{ .sensor_id = 3, .value = 300, .temperature = 22.0 },
        };
        try writer.writeRows(&batch);
        try std.testing.expectEqual(@as(usize, 3), writer.bufferedRowCount());

        try writer.flush();
        try std.testing.expectEqual(@as(usize, 0), writer.bufferedRowCount());

        try writer.close();
    }
}

// =============================================================================
// Compression Tests
// =============================================================================

test "RowWriter: uncompressed option" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("uncompressed.parquet", .{ .read = true });
    defer file.close();

    {
        var writer = try parquet.writeToFileRows(SensorReading, allocator, file, .{
            .compression = .uncompressed,
        });
        defer writer.deinit();

        try writer.writeRow(.{ .sensor_id = 1, .value = 100, .temperature = 20.0 });
        try writer.close();
    }

    // Verify file is valid
    try file.seekTo(0);
    {
        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 1), reader.metadata.num_rows);
    }
}

// =============================================================================
// Extended Integer Type Tests
// =============================================================================

const SmallIntegers = struct {
    tiny_signed: i8,
    small_signed: i16,
    tiny_unsigned: u8,
    small_unsigned: u16,
    medium_unsigned: u32,
    large_unsigned: u64,
};

test "RowWriter: small integer types with INT logical type" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("small_ints.parquet", .{ .read = true });
    defer file.close();

    {
        var writer = try parquet.writeToFileRows(SmallIntegers, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{
            .tiny_signed = -42,
            .small_signed = -1000,
            .tiny_unsigned = 200,
            .small_unsigned = 50000,
            .medium_unsigned = 3_000_000_000,
            .large_unsigned = 10_000_000_000_000,
        });
        try writer.writeRow(.{
            .tiny_signed = 127,
            .small_signed = 32767,
            .tiny_unsigned = 255,
            .small_unsigned = 65535,
            .medium_unsigned = 4_294_967_295,
            .large_unsigned = 18_446_744_073_709_551_615,
        });
        try writer.close();
    }

    // Verify data is readable and schema has correct logical types
    try file.seekTo(0);
    {
        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 2), reader.metadata.num_rows);

        // Read tiny_signed column (stored as i32)
        const tiny_signed = try reader.readColumn(0, i32);
        defer allocator.free(tiny_signed);
        try std.testing.expectEqual(@as(i32, -42), tiny_signed[0].value);
        try std.testing.expectEqual(@as(i32, 127), tiny_signed[1].value);

        // Read small_signed column (stored as i32)
        const small_signed = try reader.readColumn(1, i32);
        defer allocator.free(small_signed);
        try std.testing.expectEqual(@as(i32, -1000), small_signed[0].value);
        try std.testing.expectEqual(@as(i32, 32767), small_signed[1].value);

        // Read tiny_unsigned column (stored as i32)
        const tiny_unsigned = try reader.readColumn(2, i32);
        defer allocator.free(tiny_unsigned);
        try std.testing.expectEqual(@as(i32, 200), tiny_unsigned[0].value);
        try std.testing.expectEqual(@as(i32, 255), tiny_unsigned[1].value);

        // Read small_unsigned column (stored as i32)
        const small_unsigned = try reader.readColumn(3, i32);
        defer allocator.free(small_unsigned);
        try std.testing.expectEqual(@as(i32, 50000), small_unsigned[0].value);
        try std.testing.expectEqual(@as(i32, 65535), small_unsigned[1].value);

        // Verify schema has INT logical types
        const schema = reader.metadata.schema;
        // Index 1 is tiny_signed
        if (schema[1].logical_type) |lt| {
            try std.testing.expect(lt == .int);
            try std.testing.expectEqual(@as(i8, 8), lt.int.bit_width);
            try std.testing.expect(lt.int.is_signed);
        } else {
            return error.ExpectedLogicalType;
        }
        // Index 3 is tiny_unsigned
        if (schema[3].logical_type) |lt| {
            try std.testing.expect(lt == .int);
            try std.testing.expectEqual(@as(i8, 8), lt.int.bit_width);
            try std.testing.expect(!lt.int.is_signed);
        } else {
            return error.ExpectedLogicalType;
        }
    }
}

// =============================================================================
// Logical Type Wrapper Tests
// =============================================================================

const EventLog = struct {
    event_id: i64,
    created_at: parquet.TimestampMicros,
    event_date: parquet.Date,
    event_time: parquet.TimeMicros,
};

test "RowWriter: logical type wrappers (Timestamp, Date, Time)" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("event_log.parquet", .{ .read = true });
    defer file.close();

    {
        var writer = try parquet.writeToFileRows(EventLog, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{
            .event_id = 1,
            .created_at = parquet.TimestampMicros.fromMicros(1700000000000000), // 2023-11-14
            .event_date = parquet.Date.fromDays(19675), // 2023-11-14
            .event_time = parquet.TimeMicros.fromHms(14, 30, 0, 0), // 14:30:00
        });
        try writer.writeRow(.{
            .event_id = 2,
            .created_at = parquet.TimestampMicros.fromSeconds(1700000000),
            .event_date = parquet.Date.fromDays(19676),
            .event_time = parquet.TimeMicros.fromMicros(52200000000), // 14:30:00 in micros
        });
        try writer.close();
    }

    // Verify data and schema
    try file.seekTo(0);
    {
        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 2), reader.metadata.num_rows);

        // Read event_id
        const event_ids = try reader.readColumn(0, i64);
        defer allocator.free(event_ids);
        try std.testing.expectEqual(@as(i64, 1), event_ids[0].value);
        try std.testing.expectEqual(@as(i64, 2), event_ids[1].value);

        // Read created_at (stored as i64)
        const timestamps = try reader.readColumn(1, i64);
        defer allocator.free(timestamps);
        try std.testing.expectEqual(@as(i64, 1700000000000000), timestamps[0].value);
        try std.testing.expectEqual(@as(i64, 1700000000000000), timestamps[1].value); // fromSeconds * 1000000

        // Read event_date (stored as i32)
        const dates = try reader.readColumn(2, i32);
        defer allocator.free(dates);
        try std.testing.expectEqual(@as(i32, 19675), dates[0].value);
        try std.testing.expectEqual(@as(i32, 19676), dates[1].value);

        // Read event_time (stored as i64)
        const times = try reader.readColumn(3, i64);
        defer allocator.free(times);

        // Verify schema has correct logical types
        const schema = reader.metadata.schema;

        // Index 2 is created_at - should be TIMESTAMP
        if (schema[2].logical_type) |lt| {
            try std.testing.expect(lt == .timestamp);
        } else {
            return error.ExpectedTimestampLogicalType;
        }

        // Index 3 is event_date - should be DATE
        if (schema[3].logical_type) |lt| {
            try std.testing.expect(lt == .date);
        } else {
            return error.ExpectedDateLogicalType;
        }

        // Index 4 is event_time - should be TIME
        if (schema[4].logical_type) |lt| {
            try std.testing.expect(lt == .time);
        } else {
            return error.ExpectedTimeLogicalType;
        }

        // Verify we got the time values (times variable used for defer)
        try std.testing.expect(times.len > 0);
    }
}

// =============================================================================
// Nested Struct Tests
// =============================================================================

const Address = struct {
    city: []const u8,
    zip: i32,
};

const Person = struct {
    name: []const u8,
    age: i32,
    address: Address,
};

test "RowWriter: nested struct fields" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("nested.parquet", .{ .read = true });
    defer file.close();

    {
        var writer = try parquet.writeToFileRows(Person, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{
            .name = "Alice",
            .age = 30,
            .address = .{ .city = "New York", .zip = 10001 },
        });
        try writer.writeRow(.{
            .name = "Bob",
            .age = 25,
            .address = .{ .city = "Boston", .zip = 2101 },
        });
        try writer.close();
    }

    // Verify data is readable
    try file.seekTo(0);
    {
        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 2), reader.metadata.num_rows);

        // Schema should have hierarchical structure:
        // root (num_children=3) + name + age + address (GROUP, num_children=2) + city + zip = 6 elements
        try std.testing.expectEqual(@as(usize, 6), reader.metadata.schema.len);

        // Verify schema structure
        try std.testing.expectEqualStrings("schema", reader.metadata.schema[0].name);
        try std.testing.expectEqual(@as(?i32, 3), reader.metadata.schema[0].num_children);

        try std.testing.expectEqualStrings("name", reader.metadata.schema[1].name);
        try std.testing.expectEqualStrings("age", reader.metadata.schema[2].name);

        // address is a GROUP with 2 children
        try std.testing.expectEqualStrings("address", reader.metadata.schema[3].name);
        try std.testing.expectEqual(@as(?i32, 2), reader.metadata.schema[3].num_children);

        // Nested fields have simple names (not dot-path)
        try std.testing.expectEqualStrings("city", reader.metadata.schema[4].name);
        try std.testing.expectEqualStrings("zip", reader.metadata.schema[5].name);

        // Read and verify name column
        const names = try reader.readColumn(0, []const u8);
        defer {
            for (names) |n| allocator.free(n.value);
            allocator.free(names);
        }
        try std.testing.expectEqualStrings("Alice", names[0].value);
        try std.testing.expectEqualStrings("Bob", names[1].value);

        // Read and verify age column
        const ages = try reader.readColumn(1, i32);
        defer allocator.free(ages);
        try std.testing.expectEqual(@as(i32, 30), ages[0].value);
        try std.testing.expectEqual(@as(i32, 25), ages[1].value);

        // Read and verify address.city column
        const cities = try reader.readColumn(2, []const u8);
        defer {
            for (cities) |c| allocator.free(c.value);
            allocator.free(cities);
        }
        try std.testing.expectEqualStrings("New York", cities[0].value);
        try std.testing.expectEqualStrings("Boston", cities[1].value);

        // Read and verify address.zip column
        const zips = try reader.readColumn(3, i32);
        defer allocator.free(zips);
        try std.testing.expectEqual(@as(i32, 10001), zips[0].value);
        try std.testing.expectEqual(@as(i32, 2101), zips[1].value);
    }
}

const DeepNested = struct {
    id: i64,
    level1: struct {
        a: i32,
        level2: struct {
            b: i32,
            c: f64,
        },
    },
};

test "RowWriter: deeply nested struct (3 levels)" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("deep_nested.parquet", .{ .read = true });
    defer file.close();

    {
        var writer = try parquet.writeToFileRows(DeepNested, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{
            .id = 1,
            .level1 = .{
                .a = 10,
                .level2 = .{ .b = 100, .c = 1.5 },
            },
        });
        try writer.close();
    }

    // Verify
    try file.seekTo(0);
    {
        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 1), reader.metadata.num_rows);

        // Hierarchical schema:
        // root (num_children=2) + id + level1 (GROUP) + a + level2 (GROUP) + b + c = 7 elements
        try std.testing.expectEqual(@as(usize, 7), reader.metadata.schema.len);

        // Verify schema structure
        try std.testing.expectEqual(@as(?i32, 2), reader.metadata.schema[0].num_children);
        try std.testing.expectEqualStrings("id", reader.metadata.schema[1].name);
        try std.testing.expectEqualStrings("level1", reader.metadata.schema[2].name);
        try std.testing.expectEqual(@as(?i32, 2), reader.metadata.schema[2].num_children);
        try std.testing.expectEqualStrings("a", reader.metadata.schema[3].name);
        try std.testing.expectEqualStrings("level2", reader.metadata.schema[4].name);
        try std.testing.expectEqual(@as(?i32, 2), reader.metadata.schema[4].num_children);
        try std.testing.expectEqualStrings("b", reader.metadata.schema[5].name);
        try std.testing.expectEqualStrings("c", reader.metadata.schema[6].name);

        // Read id
        const ids = try reader.readColumn(0, i64);
        defer allocator.free(ids);
        try std.testing.expectEqual(@as(i64, 1), ids[0].value);

        // Read level1.level2.c
        const cs = try reader.readColumn(3, f64);
        defer allocator.free(cs);
        try std.testing.expectEqual(@as(f64, 1.5), cs[0].value);

        // Verify path_in_schema in column metadata is hierarchical
        // Column 0 (id): path should be ["id"]
        // Column 3 (c): path should be ["level1", "level2", "c"]
        const rg = reader.metadata.row_groups[0];

        // Column 0: id
        const col0_path = rg.columns[0].meta_data.?.path_in_schema;
        try std.testing.expectEqual(@as(usize, 1), col0_path.len);
        try std.testing.expectEqualStrings("id", col0_path[0]);

        // Column 1: level1.a -> ["level1", "a"]
        const col1_path = rg.columns[1].meta_data.?.path_in_schema;
        try std.testing.expectEqual(@as(usize, 2), col1_path.len);
        try std.testing.expectEqualStrings("level1", col1_path[0]);
        try std.testing.expectEqualStrings("a", col1_path[1]);

        // Column 3: level1.level2.c -> ["level1", "level2", "c"]
        const col3_path = rg.columns[3].meta_data.?.path_in_schema;
        try std.testing.expectEqual(@as(usize, 3), col3_path.len);
        try std.testing.expectEqualStrings("level1", col3_path[0]);
        try std.testing.expectEqualStrings("level2", col3_path[1]);
        try std.testing.expectEqualStrings("c", col3_path[2]);
    }
}

// =============================================================================
// Slice/List Field Tests
// =============================================================================

const WithScores = struct {
    id: i64,
    scores: []const i32,
};

test "RowWriter: slice field (list of i32)" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("list_i32.parquet", .{ .read = true });
    defer file.close();

    // Create test data - slices must live until flush
    const scores1 = [_]i32{ 100, 95, 88 };
    const scores2 = [_]i32{ 75, 80 };

    {
        var writer = try parquet.writeToFileRows(WithScores, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{ .id = 1, .scores = &scores1 });
        try writer.writeRow(.{ .id = 2, .scores = &scores2 });
        try writer.close();
    }

    // Verify the file was written
    try file.seekTo(0);
    {
        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 2), reader.metadata.num_rows);

        // Schema should have: root + id + (scores: container + list + element) = 5
        // Actually schema structure: root, id, scores, list, element
        try std.testing.expectEqual(@as(usize, 5), reader.metadata.schema.len);

        // Read id column
        const ids = try reader.readColumn(0, i64);
        defer allocator.free(ids);
        try std.testing.expectEqual(@as(i64, 1), ids[0].value);
        try std.testing.expectEqual(@as(i64, 2), ids[1].value);

        // Verify path_in_schema for list column includes ["scores", "list", "element"]
        const rg = reader.metadata.row_groups[0];
        const scores_path = rg.columns[1].meta_data.?.path_in_schema;
        try std.testing.expectEqual(@as(usize, 3), scores_path.len);
        try std.testing.expectEqualStrings("scores", scores_path[0]);
        try std.testing.expectEqualStrings("list", scores_path[1]);
        try std.testing.expectEqualStrings("element", scores_path[2]);
    }
}

const WithI64List = struct {
    name: []const u8,
    values: []const i64,
};

test "RowWriter: slice field (list of i64)" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("list_i64.parquet", .{ .read = true });
    defer file.close();

    const vals1 = [_]i64{ 1000000000000, 2000000000000 };
    const vals2 = [_]i64{ 3000000000000 };

    {
        var writer = try parquet.writeToFileRows(WithI64List, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{ .name = "first", .values = &vals1 });
        try writer.writeRow(.{ .name = "second", .values = &vals2 });
        try writer.close();
    }

    // Verify
    try file.seekTo(0);
    {
        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 2), reader.metadata.num_rows);

        // Read name column
        const names = try reader.readColumn(0, []const u8);
        defer {
            for (names) |n| allocator.free(n.value);
            allocator.free(names);
        }
        try std.testing.expectEqualStrings("first", names[0].value);
        try std.testing.expectEqualStrings("second", names[1].value);
    }
}

const WithOptionalList = struct {
    id: i32,
    tags: ?[]const i32,
};

test "RowWriter: optional slice field" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("optional_list.parquet", .{ .read = true });
    defer file.close();

    const tags1 = [_]i32{ 1, 2, 3 };

    {
        var writer = try parquet.writeToFileRows(WithOptionalList, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{ .id = 1, .tags = &tags1 });
        try writer.writeRow(.{ .id = 2, .tags = null }); // Null list
        try writer.close();
    }

    // Verify
    try file.seekTo(0);
    {
        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 2), reader.metadata.num_rows);

        // Read id column
        const ids = try reader.readColumn(0, i32);
        defer allocator.free(ids);
        try std.testing.expectEqual(@as(i32, 1), ids[0].value);
        try std.testing.expectEqual(@as(i32, 2), ids[1].value);
    }
}

const WithF64List = struct {
    id: i32,
    measurements: []const f64,
};

test "RowWriter: slice field (list of f64)" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("list_f64.parquet", .{ .read = true });
    defer file.close();

    const m1 = [_]f64{ 1.5, 2.5, 3.5 };
    const m2 = [_]f64{ 4.5, 5.5 };

    {
        var writer = try parquet.writeToFileRows(WithF64List, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{ .id = 1, .measurements = &m1 });
        try writer.writeRow(.{ .id = 2, .measurements = &m2 });
        try writer.close();
    }

    // Verify
    try file.seekTo(0);
    {
        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 2), reader.metadata.num_rows);

        // Read id column
        const ids = try reader.readColumn(0, i32);
        defer allocator.free(ids);
        try std.testing.expectEqual(@as(i32, 1), ids[0].value);
        try std.testing.expectEqual(@as(i32, 2), ids[1].value);
    }
}

const WithBoolList = struct {
    id: i32,
    flags: []const bool,
};

test "RowWriter: slice field (list of bool)" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("list_bool.parquet", .{ .read = true });
    defer file.close();

    const f1 = [_]bool{ true, false, true };
    const f2 = [_]bool{ false, true };

    {
        var writer = try parquet.writeToFileRows(WithBoolList, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{ .id = 1, .flags = &f1 });
        try writer.writeRow(.{ .id = 2, .flags = &f2 });
        try writer.close();
    }

    // Verify
    try file.seekTo(0);
    {
        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 2), reader.metadata.num_rows);

        // Read id column
        const ids = try reader.readColumn(0, i32);
        defer allocator.free(ids);
        try std.testing.expectEqual(@as(i32, 1), ids[0].value);
        try std.testing.expectEqual(@as(i32, 2), ids[1].value);
    }
}

const WithStringList = struct {
    id: i32,
    tags: []const []const u8,
};

test "RowWriter: slice field (list of strings)" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("list_string.parquet", .{ .read = true });
    defer file.close();

    const t1 = [_][]const u8{ "foo", "bar", "baz" };
    const t2 = [_][]const u8{ "hello", "world" };

    {
        var writer = try parquet.writeToFileRows(WithStringList, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{ .id = 1, .tags = &t1 });
        try writer.writeRow(.{ .id = 2, .tags = &t2 });
        try writer.close();
    }

    // Verify
    try file.seekTo(0);
    {
        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 2), reader.metadata.num_rows);

        // Read id column
        const ids = try reader.readColumn(0, i32);
        defer allocator.free(ids);
        try std.testing.expectEqual(@as(i32, 1), ids[0].value);
        try std.testing.expectEqual(@as(i32, 2), ids[1].value);
    }
}

// =============================================================================
// Small Integer and Wrapper Type List Tests
// =============================================================================

const WithI8List = struct {
    id: i32,
    values: []const i8,
};

test "RowWriter: slice field (list of i8)" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("list_i8.parquet", .{ .read = true });
    defer file.close();

    const v1 = [_]i8{ 1, -2, 3 };
    const v2 = [_]i8{ -128, 127 };

    {
        var writer = try parquet.writeToFileRows(WithI8List, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{ .id = 1, .values = &v1 });
        try writer.writeRow(.{ .id = 2, .values = &v2 });
        try writer.close();
    }

    // Verify
    try file.seekTo(0);
    {
        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 2), reader.metadata.num_rows);
    }
}

const WithU32List = struct {
    id: i32,
    values: []const u32,
};

test "RowWriter: slice field (list of u32)" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("list_u32.parquet", .{ .read = true });
    defer file.close();

    const v1 = [_]u32{ 0, 1000000, 4294967295 };
    const v2 = [_]u32{ 42 };

    {
        var writer = try parquet.writeToFileRows(WithU32List, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{ .id = 1, .values = &v1 });
        try writer.writeRow(.{ .id = 2, .values = &v2 });
        try writer.close();
    }

    // Verify
    try file.seekTo(0);
    {
        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 2), reader.metadata.num_rows);
    }
}

const WithU64List = struct {
    id: i32,
    values: []const u64,
};

test "RowWriter: slice field (list of u64)" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("list_u64.parquet", .{ .read = true });
    defer file.close();

    const v1 = [_]u64{ 0, 18446744073709551615 };
    const v2 = [_]u64{ 1234567890123456789 };

    {
        var writer = try parquet.writeToFileRows(WithU64List, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{ .id = 1, .values = &v1 });
        try writer.writeRow(.{ .id = 2, .values = &v2 });
        try writer.close();
    }

    // Verify
    try file.seekTo(0);
    {
        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 2), reader.metadata.num_rows);
    }
}

const WithTimestampList = struct {
    id: i32,
    timestamps: []const parquet.TimestampMicros,
};

test "RowWriter: slice field (list of Timestamp)" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("list_timestamp.parquet", .{ .read = true });
    defer file.close();

    const ts1 = [_]parquet.TimestampMicros{
        parquet.TimestampMicros.fromMicros(1000000),
        parquet.TimestampMicros.fromMicros(2000000),
    };
    const ts2 = [_]parquet.TimestampMicros{
        parquet.TimestampMicros.fromMicros(3000000),
    };

    {
        var writer = try parquet.writeToFileRows(WithTimestampList, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{ .id = 1, .timestamps = &ts1 });
        try writer.writeRow(.{ .id = 2, .timestamps = &ts2 });
        try writer.close();
    }

    // Verify
    try file.seekTo(0);
    {
        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 2), reader.metadata.num_rows);

        // Check that the list element has TIMESTAMP logical type
        // Schema: root, id, timestamps, list, element
        try std.testing.expectEqual(@as(usize, 5), reader.metadata.schema.len);
    }
}

const WithDateList = struct {
    id: i32,
    dates: []const parquet.Date,
};

test "RowWriter: slice field (list of Date)" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("list_date.parquet", .{ .read = true });
    defer file.close();

    const d1 = [_]parquet.Date{
        parquet.Date.fromDays(19000),
        parquet.Date.fromDays(19001),
    };
    const d2 = [_]parquet.Date{
        parquet.Date.fromDays(19002),
    };

    {
        var writer = try parquet.writeToFileRows(WithDateList, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{ .id = 1, .dates = &d1 });
        try writer.writeRow(.{ .id = 2, .dates = &d2 });
        try writer.close();
    }

    // Verify
    try file.seekTo(0);
    {
        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 2), reader.metadata.num_rows);
    }
}

const WithUuidList = struct {
    id: i32,
    uuids: []const parquet.Uuid,
};

test "RowWriter: slice field (list of Uuid)" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("list_uuid.parquet", .{ .read = true });
    defer file.close();

    const uuid1 = parquet.Uuid{ .bytes = .{ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10 } };
    const uuid2 = parquet.Uuid{ .bytes = .{ 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20 } };
    const uuid3 = parquet.Uuid{ .bytes = .{ 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2A, 0x2B, 0x2C, 0x2D, 0x2E, 0x2F, 0x30 } };

    const uuids1 = [_]parquet.Uuid{ uuid1, uuid2 };
    const uuids2 = [_]parquet.Uuid{uuid3};

    {
        var writer = try parquet.writeToFileRows(WithUuidList, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{ .id = 1, .uuids = &uuids1 });
        try writer.writeRow(.{ .id = 2, .uuids = &uuids2 });
        try writer.close();
    }

    // Verify
    try file.seekTo(0);
    {
        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 2), reader.metadata.num_rows);

        // Check that the list element has UUID logical type
        // Schema: root, id, uuids, list, element
        try std.testing.expectEqual(@as(usize, 5), reader.metadata.schema.len);
    }
}

// =============================================================================
// List of Structs Tests
// =============================================================================

const Point = struct {
    x: i32,
    y: i32,
};

const WithPointList = struct {
    id: i32,
    points: []const Point,
};

test "RowWriter: list of structs (Point)" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("list_struct.parquet", .{ .read = true });
    defer file.close();

    const points1 = [_]Point{ .{ .x = 1, .y = 2 }, .{ .x = 3, .y = 4 } };
    const points2 = [_]Point{.{ .x = 10, .y = 20 }};
    const points3 = [_]Point{}; // Empty list

    {
        var writer = try parquet.writeToFileRows(WithPointList, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{ .id = 1, .points = &points1 });
        try writer.writeRow(.{ .id = 2, .points = &points2 });
        try writer.writeRow(.{ .id = 3, .points = &points3 });
        try writer.close();
    }

    // Verify
    try file.seekTo(0);
    {
        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 3), reader.metadata.num_rows);

        // Schema: root, id, points, list, element(GROUP), x, y
        // = 7 elements
        try std.testing.expectEqual(@as(usize, 7), reader.metadata.schema.len);

        // Check schema structure
        // Element 0: root
        try std.testing.expectEqualStrings("schema", reader.metadata.schema[0].name);

        // Element 1: id (leaf)
        try std.testing.expectEqualStrings("id", reader.metadata.schema[1].name);
        try std.testing.expectEqual(parquet.format.PhysicalType.int32, reader.metadata.schema[1].type_.?);

        // Element 2: points (list container)
        try std.testing.expectEqualStrings("points", reader.metadata.schema[2].name);
        try std.testing.expectEqual(@as(?i32, 1), reader.metadata.schema[2].num_children);

        // Element 3: list (repeated)
        try std.testing.expectEqualStrings("list", reader.metadata.schema[3].name);
        try std.testing.expectEqual(parquet.format.RepetitionType.repeated, reader.metadata.schema[3].repetition_type.?);

        // Element 4: element (GROUP for struct)
        try std.testing.expectEqualStrings("element", reader.metadata.schema[4].name);
        try std.testing.expectEqual(@as(?i32, 2), reader.metadata.schema[4].num_children); // x and y
        try std.testing.expect(reader.metadata.schema[4].type_ == null); // GROUP has no physical type

        // Element 5: x (leaf inside element)
        try std.testing.expectEqualStrings("x", reader.metadata.schema[5].name);
        try std.testing.expectEqual(parquet.format.PhysicalType.int32, reader.metadata.schema[5].type_.?);

        // Element 6: y (leaf inside element)
        try std.testing.expectEqualStrings("y", reader.metadata.schema[6].name);
        try std.testing.expectEqual(parquet.format.PhysicalType.int32, reader.metadata.schema[6].type_.?);

        // Verify column metadata has correct paths
        // Should have 3 columns: id, points.list.element.x, points.list.element.y
        const rg = reader.metadata.row_groups[0];
        try std.testing.expectEqual(@as(usize, 3), rg.columns.len);

        // Column 0: id
        const col0_path = rg.columns[0].meta_data.?.path_in_schema;
        try std.testing.expectEqual(@as(usize, 1), col0_path.len);
        try std.testing.expectEqualStrings("id", col0_path[0]);

        // Column 1: points.list.element.x
        const col1_path = rg.columns[1].meta_data.?.path_in_schema;
        try std.testing.expectEqual(@as(usize, 4), col1_path.len);
        try std.testing.expectEqualStrings("points", col1_path[0]);
        try std.testing.expectEqualStrings("list", col1_path[1]);
        try std.testing.expectEqualStrings("element", col1_path[2]);
        try std.testing.expectEqualStrings("x", col1_path[3]);

        // Column 2: points.list.element.y
        const col2_path = rg.columns[2].meta_data.?.path_in_schema;
        try std.testing.expectEqual(@as(usize, 4), col2_path.len);
        try std.testing.expectEqualStrings("points", col2_path[0]);
        try std.testing.expectEqualStrings("list", col2_path[1]);
        try std.testing.expectEqualStrings("element", col2_path[2]);
        try std.testing.expectEqualStrings("y", col2_path[3]);
    }
}

// =============================================================================
// List of Structs with Nested Struct Fields Tests
// =============================================================================

const Inner = struct {
    a: i32,
    b: i32,
};

const Outer = struct {
    inner: Inner,
    value: f64,
};

const WithNestedListStruct = struct {
    id: i32,
    items: []const Outer,
};

test "RowWriter: list of structs with nested struct fields" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("nested_list_struct.parquet", .{ .read = true });
    defer file.close();

    const items1 = [_]Outer{
        .{ .inner = .{ .a = 1, .b = 2 }, .value = 1.5 },
        .{ .inner = .{ .a = 3, .b = 4 }, .value = 2.5 },
    };
    const items2 = [_]Outer{
        .{ .inner = .{ .a = 10, .b = 20 }, .value = 10.5 },
    };
    const items3 = [_]Outer{}; // Empty list

    {
        var writer = try parquet.writeToFileRows(WithNestedListStruct, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{ .id = 1, .items = &items1 });
        try writer.writeRow(.{ .id = 2, .items = &items2 });
        try writer.writeRow(.{ .id = 3, .items = &items3 });
        try writer.close();
    }

    // Verify
    try file.seekTo(0);
    {
        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 3), reader.metadata.num_rows);

        // Schema structure:
        // root (num_children=2)
        //   id (INT32)
        //   items (GROUP, num_children=1)  -- list container
        //     list (repeated, num_children=1)
        //       element (GROUP, num_children=2)  -- struct element
        //         inner (GROUP, num_children=2)  -- nested struct
        //           a (INT32)
        //           b (INT32)
        //         value (DOUBLE)
        // Total: 1 (root) + 1 (id) + 1 (items) + 1 (list) + 1 (element) + 1 (inner) + 2 (a, b) + 1 (value) = 9
        try std.testing.expectEqual(@as(usize, 9), reader.metadata.schema.len);

        // Check schema structure - print names for debugging if needed
        // Expected: schema, id, items, list, element, inner, a, b, value
        try std.testing.expectEqualStrings("schema", reader.metadata.schema[0].name);
        try std.testing.expectEqualStrings("id", reader.metadata.schema[1].name);
        try std.testing.expectEqualStrings("items", reader.metadata.schema[2].name);
        try std.testing.expectEqualStrings("list", reader.metadata.schema[3].name);
        try std.testing.expectEqualStrings("element", reader.metadata.schema[4].name);
        try std.testing.expectEqualStrings("inner", reader.metadata.schema[5].name);
        try std.testing.expectEqualStrings("a", reader.metadata.schema[6].name);
        try std.testing.expectEqualStrings("b", reader.metadata.schema[7].name);
        try std.testing.expectEqualStrings("value", reader.metadata.schema[8].name);

        // element GROUP should have 2 children (inner and value)
        try std.testing.expectEqual(@as(?i32, 2), reader.metadata.schema[4].num_children);

        // inner GROUP should have 2 children (a and b)
        try std.testing.expectEqual(@as(?i32, 2), reader.metadata.schema[5].num_children);

        // Verify column metadata has correct paths
        // Should have 4 columns: id, items.list.element.inner.a, items.list.element.inner.b, items.list.element.value
        const rg = reader.metadata.row_groups[0];
        try std.testing.expectEqual(@as(usize, 4), rg.columns.len);

        // Column 0: id
        const col0_path = rg.columns[0].meta_data.?.path_in_schema;
        try std.testing.expectEqual(@as(usize, 1), col0_path.len);
        try std.testing.expectEqualStrings("id", col0_path[0]);

        // Column 1: items.list.element.inner.a
        const col1_path = rg.columns[1].meta_data.?.path_in_schema;
        try std.testing.expectEqual(@as(usize, 5), col1_path.len);
        try std.testing.expectEqualStrings("items", col1_path[0]);
        try std.testing.expectEqualStrings("list", col1_path[1]);
        try std.testing.expectEqualStrings("element", col1_path[2]);
        try std.testing.expectEqualStrings("inner", col1_path[3]);
        try std.testing.expectEqualStrings("a", col1_path[4]);

        // Column 2: items.list.element.inner.b
        const col2_path = rg.columns[2].meta_data.?.path_in_schema;
        try std.testing.expectEqual(@as(usize, 5), col2_path.len);
        try std.testing.expectEqualStrings("items", col2_path[0]);
        try std.testing.expectEqualStrings("list", col2_path[1]);
        try std.testing.expectEqualStrings("element", col2_path[2]);
        try std.testing.expectEqualStrings("inner", col2_path[3]);
        try std.testing.expectEqualStrings("b", col2_path[4]);

        // Column 3: items.list.element.value
        const col3_path = rg.columns[3].meta_data.?.path_in_schema;
        try std.testing.expectEqual(@as(usize, 4), col3_path.len);
        try std.testing.expectEqualStrings("items", col3_path[0]);
        try std.testing.expectEqualStrings("list", col3_path[1]);
        try std.testing.expectEqualStrings("element", col3_path[2]);
        try std.testing.expectEqualStrings("value", col3_path[3]);
    }
}

// =============================================================================
// Key-Value Metadata Tests
// =============================================================================

test "RowWriter: key-value metadata via options" {
    const allocator = std.testing.allocator;

    // Write data with metadata in options
    var writer = try parquet.writeToBufferRows(SensorReading, allocator, .{
        .metadata = &.{
            .{ .key = "source", .value = "sensor-hub-1" },
            .{ .key = "version", .value = "1.0.0" },
        },
    });
    defer writer.deinit();

    const readings = [_]SensorReading{
        .{ .sensor_id = 1, .value = 100, .temperature = 23.5 },
    };
    try writer.writeRows(&readings);
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    // Read back and verify metadata
    var reader = try parquet.openBuffer(allocator, buffer);
    defer reader.deinit();

    const kvs = reader.metadata.key_value_metadata orelse {
        return error.ExpectedMetadata;
    };

    try std.testing.expectEqual(@as(usize, 2), kvs.len);

    // Find and verify each key
    var found_source = false;
    var found_version = false;
    for (kvs) |kv| {
        if (std.mem.eql(u8, kv.key, "source")) {
            try std.testing.expectEqualStrings("sensor-hub-1", kv.value.?);
            found_source = true;
        } else if (std.mem.eql(u8, kv.key, "version")) {
            try std.testing.expectEqualStrings("1.0.0", kv.value.?);
            found_version = true;
        }
    }
    try std.testing.expect(found_source);
    try std.testing.expect(found_version);
}

test "RowWriter: key-value metadata via setKeyValueMetadata" {
    const allocator = std.testing.allocator;

    // Write data and set metadata dynamically
    var writer = try parquet.writeToBufferRows(SensorReading, allocator, .{});
    defer writer.deinit();

    const readings = [_]SensorReading{
        .{ .sensor_id = 1, .value = 100, .temperature = 23.5 },
    };
    try writer.writeRows(&readings);

    // Set metadata after writing data (simulating computed metadata)
    try writer.setKeyValueMetadata("row_count", "1");
    try writer.setKeyValueMetadata("checksum", "abc123");

    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    // Read back and verify metadata
    var reader = try parquet.openBuffer(allocator, buffer);
    defer reader.deinit();

    const kvs = reader.metadata.key_value_metadata orelse {
        return error.ExpectedMetadata;
    };

    try std.testing.expectEqual(@as(usize, 2), kvs.len);

    var found_row_count = false;
    var found_checksum = false;
    for (kvs) |kv| {
        if (std.mem.eql(u8, kv.key, "row_count")) {
            try std.testing.expectEqualStrings("1", kv.value.?);
            found_row_count = true;
        } else if (std.mem.eql(u8, kv.key, "checksum")) {
            try std.testing.expectEqualStrings("abc123", kv.value.?);
            found_checksum = true;
        }
    }
    try std.testing.expect(found_row_count);
    try std.testing.expect(found_checksum);
}

test "RowWriter: key-value metadata update (same key, different value)" {
    const allocator = std.testing.allocator;

    // Write data and update metadata
    var writer = try parquet.writeToBufferRows(SensorReading, allocator, .{
        .metadata = &.{
            .{ .key = "status", .value = "pending" },
        },
    });
    defer writer.deinit();

    const readings = [_]SensorReading{
        .{ .sensor_id = 1, .value = 100, .temperature = 23.5 },
    };
    try writer.writeRows(&readings);

    // Update the status metadata (overwrite existing key)
    try writer.setKeyValueMetadata("status", "complete");

    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    // Read back and verify metadata was updated
    var reader = try parquet.openBuffer(allocator, buffer);
    defer reader.deinit();

    const kvs = reader.metadata.key_value_metadata orelse {
        return error.ExpectedMetadata;
    };

    // Should only have one entry (updated, not duplicated)
    try std.testing.expectEqual(@as(usize, 1), kvs.len);
    try std.testing.expectEqualStrings("status", kvs[0].key);
    try std.testing.expectEqualStrings("complete", kvs[0].value.?);
}

test "RowWriter: key-value metadata combined options and setKeyValueMetadata" {
    const allocator = std.testing.allocator;

    // Write data with both options and dynamic metadata
    var writer = try parquet.writeToBufferRows(SensorReading, allocator, .{
        .metadata = &.{
            .{ .key = "source", .value = "sensors" },
        },
    });
    defer writer.deinit();

    const readings = [_]SensorReading{
        .{ .sensor_id = 1, .value = 100, .temperature = 23.5 },
        .{ .sensor_id = 2, .value = 200, .temperature = 24.0 },
    };
    try writer.writeRows(&readings);

    // Add additional metadata dynamically
    try writer.setKeyValueMetadata("row_count", "2");

    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    // Read back and verify all metadata
    var reader = try parquet.openBuffer(allocator, buffer);
    defer reader.deinit();

    const kvs = reader.metadata.key_value_metadata orelse {
        return error.ExpectedMetadata;
    };

    try std.testing.expectEqual(@as(usize, 2), kvs.len);

    var found_source = false;
    var found_row_count = false;
    for (kvs) |kv| {
        if (std.mem.eql(u8, kv.key, "source")) {
            try std.testing.expectEqualStrings("sensors", kv.value.?);
            found_source = true;
        } else if (std.mem.eql(u8, kv.key, "row_count")) {
            try std.testing.expectEqualStrings("2", kv.value.?);
            found_row_count = true;
        }
    }
    try std.testing.expect(found_source);
    try std.testing.expect(found_row_count);
}

// =============================================================================
// Phase 12 P0: Nanosecond Precision Tests
// =============================================================================

const TimestampPrecisionRecord = struct {
    id: i32,
    ts_nanos: parquet.Timestamp(.nanos),
    ts_micros: parquet.Timestamp(.micros),
    ts_millis: parquet.Timestamp(.millis),
};

test "RowWriter: timestamp precision (nanos, micros, millis)" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("timestamp_precision.parquet", .{ .read = true });
    defer file.close();

    // Write rows with different precisions
    // All represent the same moment: 2024-01-15 12:00:00.123456789 UTC
    const nanos_val: i64 = 1705320000123456789; // nanos since epoch
    const micros_val: i64 = 1705320000123456; // micros since epoch
    const millis_val: i64 = 1705320000123; // millis since epoch

    {
        var writer = try parquet.writeToFileRows(TimestampPrecisionRecord, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{
            .id = 1,
            .ts_nanos = parquet.Timestamp(.nanos).from(nanos_val),
            .ts_micros = parquet.Timestamp(.micros).from(micros_val),
            .ts_millis = parquet.Timestamp(.millis).from(millis_val),
        });
        try writer.close();
    }

    // Verify schema has correct logical types
    try file.seekTo(0);
    {
        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        // Verify row count
        try std.testing.expectEqual(@as(i64, 1), reader.metadata.num_rows);

        // Schema elements: root (1) + id (1) + ts_nanos (1) + ts_micros (1) + ts_millis (1) = 5
        const schema = reader.metadata.schema;
        try std.testing.expectEqual(@as(usize, 5), schema.len);

        // Check ts_nanos has TIMESTAMP(NANOS) logical type
        const ts_nanos_elem = schema[2];
        try std.testing.expectEqualStrings("ts_nanos", ts_nanos_elem.name);
        try std.testing.expect(ts_nanos_elem.logical_type != null);
        const nanos_lt = ts_nanos_elem.logical_type.?;
        try std.testing.expect(std.meta.activeTag(nanos_lt) == .timestamp);
        try std.testing.expectEqual(parquet.format.TimeUnit.nanos, nanos_lt.timestamp.unit);

        // Check ts_micros has TIMESTAMP(MICROS) logical type
        const ts_micros_elem = schema[3];
        try std.testing.expectEqualStrings("ts_micros", ts_micros_elem.name);
        const micros_lt = ts_micros_elem.logical_type.?.timestamp;
        try std.testing.expectEqual(parquet.format.TimeUnit.micros, micros_lt.unit);

        // Check ts_millis has TIMESTAMP(MILLIS) logical type
        const ts_millis_elem = schema[4];
        try std.testing.expectEqualStrings("ts_millis", ts_millis_elem.name);
        const millis_lt = ts_millis_elem.logical_type.?.timestamp;
        try std.testing.expectEqual(parquet.format.TimeUnit.millis, millis_lt.unit);
    }
}

const TimePrecisionRecord = struct {
    id: i32,
    time_nanos: parquet.Time(.nanos),
    time_micros: parquet.Time(.micros),
    time_millis: parquet.Time(.millis),
};

test "RowWriter: time precision (nanos, micros, millis)" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("time_precision.parquet", .{ .read = true });
    defer file.close();

    // Write times with different precisions
    // All represent 12:30:45.123456789
    const nanos_val: i64 = 45045123456789; // 12:30:45.123456789 in nanos
    const micros_val: i64 = 45045123456; // 12:30:45.123456 in micros
    const millis_val: i64 = 45045123; // 12:30:45.123 in millis

    {
        var writer = try parquet.writeToFileRows(TimePrecisionRecord, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{
            .id = 1,
            .time_nanos = parquet.Time(.nanos).from(nanos_val),
            .time_micros = parquet.Time(.micros).from(micros_val),
            .time_millis = parquet.Time(.millis).from(millis_val),
        });
        try writer.close();
    }

    // Verify schema has correct logical types
    try file.seekTo(0);
    {
        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 1), reader.metadata.num_rows);

        const schema = reader.metadata.schema;
        try std.testing.expectEqual(@as(usize, 5), schema.len);

        // Check time_nanos has TIME(NANOS) logical type
        const time_nanos_elem = schema[2];
        try std.testing.expectEqualStrings("time_nanos", time_nanos_elem.name);
        try std.testing.expect(time_nanos_elem.logical_type != null);
        const nanos_lt = time_nanos_elem.logical_type.?;
        try std.testing.expect(std.meta.activeTag(nanos_lt) == .time);
        try std.testing.expectEqual(parquet.format.TimeUnit.nanos, nanos_lt.time.unit);

        // Check time_micros has TIME(MICROS) logical type
        const time_micros_elem = schema[3];
        const micros_lt = time_micros_elem.logical_type.?.time;
        try std.testing.expectEqual(parquet.format.TimeUnit.micros, micros_lt.unit);

        // Check time_millis has TIME(MILLIS) logical type
        const time_millis_elem = schema[4];
        const millis_lt = time_millis_elem.logical_type.?.time;
        try std.testing.expectEqual(parquet.format.TimeUnit.millis, millis_lt.unit);
    }
}

// =============================================================================
// INT96 Timestamp Tests (Legacy format)
// =============================================================================

const Int96TimestampRecord = struct {
    id: i32,
    event_time: parquet.TimestampInt96,
};

test "RowWriter: INT96 timestamp round-trip" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("int96_timestamp.parquet", .{ .read = true });
    defer file.close();

    // Test timestamps: epoch, positive, large positive
    const timestamps = [_]i64{
        0, // Unix epoch
        1705321845_123456789, // 2024-01-15 12:30:45.123456789 UTC
        86_400_000_000_000, // Exactly 1 day after epoch
    };

    {
        var writer = try parquet.writeToFileRows(Int96TimestampRecord, allocator, file, .{});
        defer writer.deinit();

        for (timestamps, 0..) |ts, i| {
            try writer.writeRow(.{
                .id = @intCast(i + 1),
                .event_time = parquet.TimestampInt96.fromNanos(ts),
            });
        }
        try writer.close();
    }

    // Verify the file was written correctly using RowReader (which handles INT96)
    try file.seekTo(0);
    {
        var row_reader = try parquet.openFileRowReader(Int96TimestampRecord, allocator, file, .{});
        defer row_reader.deinit();

        // Read all rows and verify values
        for (timestamps, 0..) |expected_ts, i| {
            const row = (try row_reader.next()).?;
            defer row_reader.freeRow(&row);

            try std.testing.expectEqual(@as(i32, @intCast(i + 1)), row.id);
            try std.testing.expectEqual(expected_ts, row.event_time.value);
        }

        // Verify no more rows
        try std.testing.expect((try row_reader.next()) == null);
    }

    // Also verify schema has INT96 physical type
    try file.seekTo(0);
    {
        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 3), reader.metadata.num_rows);

        const schema = reader.metadata.schema;
        const event_time_elem = schema[2]; // 0=root, 1=id, 2=event_time
        try std.testing.expectEqualStrings("event_time", event_time_elem.name);
        try std.testing.expectEqual(parquet.format.PhysicalType.int96, event_time_elem.type_.?);
    }
}

const OptionalInt96Record = struct {
    id: i32,
    event_time: ?parquet.TimestampInt96,
};

test "RowWriter: INT96 timestamp with nulls" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("int96_nullable.parquet", .{ .read = true });
    defer file.close();

    {
        var writer = try parquet.writeToFileRows(OptionalInt96Record, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{ .id = 1, .event_time = parquet.TimestampInt96.fromNanos(1_000_000_000) });
        try writer.writeRow(.{ .id = 2, .event_time = null });
        try writer.writeRow(.{ .id = 3, .event_time = parquet.TimestampInt96.fromNanos(2_000_000_000) });
        try writer.close();
    }

    // Read back using RowReader which handles INT96
    try file.seekTo(0);
    {
        var row_reader = try parquet.openFileRowReader(OptionalInt96Record, allocator, file, .{});
        defer row_reader.deinit();

        // Row 1: has value
        const row1 = (try row_reader.next()).?;
        defer row_reader.freeRow(&row1);
        try std.testing.expectEqual(@as(i32, 1), row1.id);
        try std.testing.expect(row1.event_time != null);
        try std.testing.expectEqual(@as(i64, 1_000_000_000), row1.event_time.?.value);

        // Row 2: null
        const row2 = (try row_reader.next()).?;
        defer row_reader.freeRow(&row2);
        try std.testing.expectEqual(@as(i32, 2), row2.id);
        try std.testing.expect(row2.event_time == null);

        // Row 3: has value
        const row3 = (try row_reader.next()).?;
        defer row_reader.freeRow(&row3);
        try std.testing.expectEqual(@as(i32, 3), row3.id);
        try std.testing.expect(row3.event_time != null);
        try std.testing.expectEqual(@as(i64, 2_000_000_000), row3.event_time.?.value);
    }
}

test "RowWriter: INT96 timestamp via RowReader" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("int96_rowreader.parquet", .{ .read = true });
    defer file.close();

    // Write some data
    const nanos_val: i64 = 1705321845_123456789;
    {
        var writer = try parquet.writeToFileRows(Int96TimestampRecord, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{
            .id = 1,
            .event_time = parquet.TimestampInt96.fromNanos(nanos_val),
        });
        try writer.close();
    }

    // Read back using RowReader
    try file.seekTo(0);
    {
        var row_reader = try parquet.openFileRowReader(Int96TimestampRecord, allocator, file, .{});
        defer row_reader.deinit();

        const row = (try row_reader.next()).?;
        defer row_reader.freeRow(&row);

        // Verify the values
        try std.testing.expectEqual(@as(i32, 1), row.id);
        try std.testing.expectEqual(nanos_val, row.event_time.value);
    }
}
