//! Logical type round-trip tests for the Parquet writer
//!
//! Tests write files with various logical types and verify they are
//! preserved when reading back.

const std = @import("std");
const parquet = @import("../lib.zig");

test "round-trip STRING logical type" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_string_logical.parquet";

    // Write with STRING logical type
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            parquet.ColumnDef.string("name", false),
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        const values = [_][]const u8{ "hello", "world" };
        try writer.writeColumn([]const u8, 0, &values);
        try writer.close();
    }

    // Read back and verify logical type
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        // Check schema has STRING logical type
        const schema = reader.getSchema();
        try std.testing.expect(schema.len >= 2);

        const col_schema = schema[1];
        try std.testing.expectEqual(parquet.format.PhysicalType.byte_array, col_schema.type_.?);
        try std.testing.expect(col_schema.logical_type != null);

        switch (col_schema.logical_type.?) {
            .string => {}, // Expected
            else => return error.WrongLogicalType,
        }
    }
}

test "round-trip TIMESTAMP logical type" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_timestamp.parquet";

    // Write with TIMESTAMP logical type
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            parquet.ColumnDef.timestamp("created_at", .micros, true, false),
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        // Microseconds since epoch
        const values = [_]i64{ 1704067200000000, 1704153600000000 }; // 2024-01-01, 2024-01-02
        try writer.writeColumn(i64, 0, &values);
        try writer.close();
    }

    // Read back and verify logical type
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        const schema = reader.getSchema();
        const col_schema = schema[1];

        try std.testing.expectEqual(parquet.format.PhysicalType.int64, col_schema.type_.?);
        try std.testing.expect(col_schema.logical_type != null);

        switch (col_schema.logical_type.?) {
            .timestamp => |ts| {
                try std.testing.expect(ts.is_adjusted_to_utc);
                try std.testing.expectEqual(parquet.format.TimeUnit.micros, ts.unit);
            },
            else => return error.WrongLogicalType,
        }
    }
}

test "round-trip DATE logical type" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_date.parquet";

    // Write with DATE logical type
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            parquet.ColumnDef.date("birth_date", false),
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        // Days since epoch
        const values = [_]i32{ 19724, 19725 }; // 2024-01-01, 2024-01-02
        try writer.writeColumn(i32, 0, &values);
        try writer.close();
    }

    // Read back and verify logical type
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        const schema = reader.getSchema();
        const col_schema = schema[1];

        try std.testing.expectEqual(parquet.format.PhysicalType.int32, col_schema.type_.?);
        try std.testing.expect(col_schema.logical_type != null);

        switch (col_schema.logical_type.?) {
            .date => {}, // Expected
            else => return error.WrongLogicalType,
        }
    }
}

test "round-trip TIME logical type" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_time.parquet";

    // Write with TIME logical type (millis uses INT32)
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            parquet.ColumnDef.time("event_time", .millis, false, false),
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        // Milliseconds since midnight
        const values = [_]i32{ 43200000, 86399999 }; // 12:00:00, 23:59:59.999
        try writer.writeColumn(i32, 0, &values);
        try writer.close();
    }

    // Read back and verify logical type
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        const schema = reader.getSchema();
        const col_schema = schema[1];

        try std.testing.expectEqual(parquet.format.PhysicalType.int32, col_schema.type_.?);
        try std.testing.expect(col_schema.logical_type != null);

        switch (col_schema.logical_type.?) {
            .time => |t| {
                try std.testing.expect(!t.is_adjusted_to_utc);
                try std.testing.expectEqual(parquet.format.TimeUnit.millis, t.unit);
            },
            else => return error.WrongLogicalType,
        }
    }
}

test "round-trip DECIMAL logical type" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_decimal.parquet";

    // Write with DECIMAL(9,2) logical type (uses INT32)
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            parquet.ColumnDef.decimal("amount", 9, 2, false),
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        // Scaled integer values: 12345 = 123.45, -99999 = -999.99
        const values = [_]i32{ 12345, -99999, 0 };
        try writer.writeColumn(i32, 0, &values);
        try writer.close();
    }

    // Read back and verify logical type
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        const schema = reader.getSchema();
        const col_schema = schema[1];

        try std.testing.expectEqual(parquet.format.PhysicalType.int32, col_schema.type_.?);
        try std.testing.expect(col_schema.logical_type != null);

        switch (col_schema.logical_type.?) {
            .decimal => |d| {
                try std.testing.expectEqual(@as(i32, 9), d.precision);
                try std.testing.expectEqual(@as(i32, 2), d.scale);
            },
            else => return error.WrongLogicalType,
        }
    }
}

test "round-trip UUID logical type" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_uuid.parquet";

    // Write with UUID logical type
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            parquet.ColumnDef.uuid("id", false),
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        // 16-byte UUID values
        const uuid1 = [_]u8{ 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78 };
        const uuid2 = [_]u8{0} ** 16;
        const values = [_][]const u8{ &uuid1, &uuid2 };
        try writer.writeColumnFixedByteArray(0, &values);
        try writer.close();
    }

    // Read back and verify logical type
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        const schema = reader.getSchema();
        const col_schema = schema[1];

        try std.testing.expectEqual(parquet.format.PhysicalType.fixed_len_byte_array, col_schema.type_.?);
        try std.testing.expectEqual(@as(?i32, 16), col_schema.type_length);
        try std.testing.expect(col_schema.logical_type != null);

        switch (col_schema.logical_type.?) {
            .uuid => {}, // Expected
            else => return error.WrongLogicalType,
        }
    }
}

test "round-trip INT8 logical type" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_int8.parquet";

    // Write with INT8 logical type
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            parquet.ColumnDef.int8("tiny_val", false),
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        const values = [_]i32{ 1, -128, 127 };
        try writer.writeColumn(i32, 0, &values);
        try writer.close();
    }

    // Read back and verify logical type
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        const schema = reader.getSchema();
        const col_schema = schema[1];

        try std.testing.expectEqual(parquet.format.PhysicalType.int32, col_schema.type_.?);
        try std.testing.expect(col_schema.logical_type != null);

        switch (col_schema.logical_type.?) {
            .int => |i| {
                try std.testing.expectEqual(@as(i8, 8), i.bit_width);
                try std.testing.expect(i.is_signed);
            },
            else => return error.WrongLogicalType,
        }
    }
}

test "round-trip UINT32 logical type" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_uint32.parquet";

    // Write with UINT32 logical type
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            parquet.ColumnDef.uint32("unsigned_val", false),
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        // Stored as i32 but interpreted as unsigned
        const values = [_]i32{ 0, 2147483647, -1 }; // -1 = 0xFFFFFFFF = 4294967295 as unsigned
        try writer.writeColumn(i32, 0, &values);
        try writer.close();
    }

    // Read back and verify logical type
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        const schema = reader.getSchema();
        const col_schema = schema[1];

        try std.testing.expectEqual(parquet.format.PhysicalType.int32, col_schema.type_.?);
        try std.testing.expect(col_schema.logical_type != null);

        switch (col_schema.logical_type.?) {
            .int => |i| {
                try std.testing.expectEqual(@as(i8, 32), i.bit_width);
                try std.testing.expect(!i.is_signed);
            },
            else => return error.WrongLogicalType,
        }
    }
}

test "round-trip FLOAT16 logical type" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_float16.parquet";

    // Write with FLOAT16 logical type
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            parquet.ColumnDef.float16("half_precision", false),
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        // 2-byte IEEE 754 half-precision values
        const f16_1 = [_]u8{ 0x00, 0x3C }; // 1.0 in float16
        const f16_2 = [_]u8{ 0x00, 0x00 }; // 0.0
        const values = [_][]const u8{ &f16_1, &f16_2 };
        try writer.writeColumnFixedByteArray(0, &values);
        try writer.close();
    }

    // Read back and verify logical type
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        const schema = reader.getSchema();
        const col_schema = schema[1];

        try std.testing.expectEqual(parquet.format.PhysicalType.fixed_len_byte_array, col_schema.type_.?);
        try std.testing.expectEqual(@as(?i32, 2), col_schema.type_length);
        try std.testing.expect(col_schema.logical_type != null);

        switch (col_schema.logical_type.?) {
            .float16 => {}, // Expected
            else => return error.WrongLogicalType,
        }
    }
}

test "round-trip ENUM logical type" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_enum.parquet";

    // Write with ENUM logical type
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            parquet.ColumnDef.enum_("status", false),
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        const values = [_][]const u8{ "ACTIVE", "INACTIVE", "PENDING" };
        try writer.writeColumn([]const u8, 0, &values);
        try writer.close();
    }

    // Read back and verify logical type
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        const schema = reader.getSchema();
        const col_schema = schema[1];

        try std.testing.expectEqual(parquet.format.PhysicalType.byte_array, col_schema.type_.?);
        try std.testing.expect(col_schema.logical_type != null);

        switch (col_schema.logical_type.?) {
            .enum_ => {}, // Expected
            else => return error.WrongLogicalType,
        }
    }
}

test "round-trip JSON logical type" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_json.parquet";

    // Write with JSON logical type
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            parquet.ColumnDef.json("data", false),
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        const values = [_][]const u8{
            "{\"name\":\"Alice\"}",
            "[]",
            "{\"nested\":{\"key\":\"value\"}}",
        };
        try writer.writeColumn([]const u8, 0, &values);
        try writer.close();
    }

    // Read back and verify logical type
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        const schema = reader.getSchema();
        const col_schema = schema[1];

        try std.testing.expectEqual(parquet.format.PhysicalType.byte_array, col_schema.type_.?);
        try std.testing.expect(col_schema.logical_type != null);

        switch (col_schema.logical_type.?) {
            .json => {}, // Expected
            else => return error.WrongLogicalType,
        }
    }
}
