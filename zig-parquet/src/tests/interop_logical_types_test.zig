//! Interop tests for logical types
//!
//! Tests reading PyArrow-generated Parquet files with logical type annotations
//! to verify our reader correctly parses logical types from real-world files.

const std = @import("std");
const io = std.testing.io;
const parquet = @import("../lib.zig");
const format = parquet.format;

test "read PyArrow STRING logical type" {
    const allocator = std.testing.allocator;

    const file = std.Io.Dir.cwd().openFile(io, "../test-files-arrow/logical_types/string.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file (run from zig-parquet dir): {}\n", .{err});
        return err;
    };
    defer file.close(io);

    var reader = try parquet.openFileDynamic(allocator, file, io, .{});
    defer reader.deinit();

    // Verify metadata
    try std.testing.expectEqual(@as(i64, 5), reader.metadata.num_rows);

    // Check schema has STRING logical type
    const schema = reader.getSchema();
    try std.testing.expect(schema.len >= 3); // root + 2 columns

    // str_col - STRING logical type
    const str_col_schema = schema[1];
    try std.testing.expectEqual(format.PhysicalType.byte_array, str_col_schema.type_.?);
    try std.testing.expect(str_col_schema.logical_type != null);
    switch (str_col_schema.logical_type.?) {
        .string => {}, // Expected
        else => return error.WrongLogicalType,
    }

    // str_required - also STRING
    const str_req_schema = schema[2];
    try std.testing.expect(str_req_schema.logical_type != null);
    switch (str_req_schema.logical_type.?) {
        .string => {}, // Expected
        else => return error.WrongLogicalType,
    }

    // Read and verify data
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 5), rows.len);
    try std.testing.expectEqualStrings("hello", rows[0].getColumn(0).?.asBytes().?);
    try std.testing.expectEqualStrings("world", rows[1].getColumn(0).?.asBytes().?);
    try std.testing.expectEqualStrings("", rows[2].getColumn(0).?.asBytes().?);
    try std.testing.expectEqualStrings("unicode: 🎉", rows[3].getColumn(0).?.asBytes().?);
    try std.testing.expect(rows[4].getColumn(0).?.isNull());
}

test "read PyArrow DATE logical type" {
    const allocator = std.testing.allocator;

    const file = std.Io.Dir.cwd().openFile(io, "../test-files-arrow/logical_types/date.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close(io);

    var reader = try parquet.openFileDynamic(allocator, file, io, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 5), reader.metadata.num_rows);

    // Check schema has DATE logical type
    const schema = reader.getSchema();
    const date_col_schema = schema[1];

    try std.testing.expectEqual(format.PhysicalType.int32, date_col_schema.type_.?);
    try std.testing.expect(date_col_schema.logical_type != null);
    switch (date_col_schema.logical_type.?) {
        .date => {}, // Expected
        else => return error.WrongLogicalType,
    }

    // Read and verify data (days since epoch)
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 5), rows.len);

    // 2024-01-15 = 19737 days since 1970-01-01
    try std.testing.expectEqual(@as(i32, 19737), rows[0].getColumn(0).?.asInt32().?);
    // 1970-01-01 = 0 days
    try std.testing.expectEqual(@as(i32, 0), rows[1].getColumn(0).?.asInt32().?);
    // 2000-06-15 = 11123 days
    try std.testing.expectEqual(@as(i32, 11123), rows[2].getColumn(0).?.asInt32().?);
    // 1999-12-31 = 10956 days
    try std.testing.expectEqual(@as(i32, 10956), rows[3].getColumn(0).?.asInt32().?);
    // null
    try std.testing.expect(rows[4].getColumn(0).?.isNull());
}

test "read PyArrow TIMESTAMP logical type" {
    const allocator = std.testing.allocator;

    const file = std.Io.Dir.cwd().openFile(io, "../test-files-arrow/logical_types/timestamp.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close(io);

    var reader = try parquet.openFileDynamic(allocator, file, io, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 5), reader.metadata.num_rows);

    const schema = reader.getSchema();

    // ts_millis_utc - TIMESTAMP(MILLIS, UTC)
    {
        const ts_schema = schema[1];
        try std.testing.expectEqual(format.PhysicalType.int64, ts_schema.type_.?);
        try std.testing.expect(ts_schema.logical_type != null);

        switch (ts_schema.logical_type.?) {
            .timestamp => |ts| {
                try std.testing.expect(ts.is_adjusted_to_utc);
                try std.testing.expectEqual(format.TimeUnit.millis, ts.unit);
            },
            else => return error.WrongLogicalType,
        }
    }

    // ts_micros_utc - TIMESTAMP(MICROS, UTC)
    {
        const ts_schema = schema[2];
        try std.testing.expect(ts_schema.logical_type != null);

        switch (ts_schema.logical_type.?) {
            .timestamp => |ts| {
                try std.testing.expect(ts.is_adjusted_to_utc);
                try std.testing.expectEqual(format.TimeUnit.micros, ts.unit);
            },
            else => return error.WrongLogicalType,
        }
    }

    // ts_nanos_utc - TIMESTAMP(NANOS, UTC)
    {
        const ts_schema = schema[3];
        try std.testing.expect(ts_schema.logical_type != null);

        switch (ts_schema.logical_type.?) {
            .timestamp => |ts| {
                try std.testing.expect(ts.is_adjusted_to_utc);
                try std.testing.expectEqual(format.TimeUnit.nanos, ts.unit);
            },
            else => return error.WrongLogicalType,
        }
    }

    // ts_micros_local - TIMESTAMP(MICROS, LOCAL)
    {
        const ts_schema = schema[4];
        try std.testing.expect(ts_schema.logical_type != null);

        switch (ts_schema.logical_type.?) {
            .timestamp => |ts| {
                try std.testing.expect(!ts.is_adjusted_to_utc);
                try std.testing.expectEqual(format.TimeUnit.micros, ts.unit);
            },
            else => return error.WrongLogicalType,
        }
    }

    // Read millis column and verify data
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 5), rows.len);
    // First value should be non-null (col 0 = ts_millis_utc)
    try std.testing.expect(!rows[0].getColumn(0).?.isNull());
    // Fourth value should be null
    try std.testing.expect(rows[3].getColumn(0).?.isNull());
}

test "read PyArrow TIME logical type" {
    const allocator = std.testing.allocator;

    const file = std.Io.Dir.cwd().openFile(io, "../test-files-arrow/logical_types/time.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close(io);

    var reader = try parquet.openFileDynamic(allocator, file, io, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 5), reader.metadata.num_rows);

    const schema = reader.getSchema();

    // time_millis - TIME(MILLIS) uses INT32
    {
        const time_schema = schema[1];
        try std.testing.expectEqual(format.PhysicalType.int32, time_schema.type_.?);
        try std.testing.expect(time_schema.logical_type != null);

        switch (time_schema.logical_type.?) {
            .time => |t| {
                try std.testing.expectEqual(format.TimeUnit.millis, t.unit);
            },
            else => return error.WrongLogicalType,
        }
    }

    // time_micros - TIME(MICROS) uses INT64
    {
        const time_schema = schema[2];
        try std.testing.expectEqual(format.PhysicalType.int64, time_schema.type_.?);
        try std.testing.expect(time_schema.logical_type != null);

        switch (time_schema.logical_type.?) {
            .time => |t| {
                try std.testing.expectEqual(format.TimeUnit.micros, t.unit);
            },
            else => return error.WrongLogicalType,
        }
    }

    // time_nanos - TIME(NANOS) uses INT64
    {
        const time_schema = schema[3];
        try std.testing.expectEqual(format.PhysicalType.int64, time_schema.type_.?);
        try std.testing.expect(time_schema.logical_type != null);

        switch (time_schema.logical_type.?) {
            .time => |t| {
                try std.testing.expectEqual(format.TimeUnit.nanos, t.unit);
            },
            else => return error.WrongLogicalType,
        }
    }

    // Read all rows and verify millis column (col 0, INT32)
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 5), rows.len);

    // 10:30:00 = 37800000 ms
    try std.testing.expectEqual(@as(i32, 37800000), rows[0].getColumn(0).?.asInt32().?);
    // 00:00:00 = 0 ms
    try std.testing.expectEqual(@as(i32, 0), rows[1].getColumn(0).?.asInt32().?);
    // 23:59:59 = 86399000 ms
    try std.testing.expectEqual(@as(i32, 86399000), rows[2].getColumn(0).?.asInt32().?);
    // null
    try std.testing.expect(rows[3].getColumn(0).?.isNull());
    // 12:00:00 = 43200000 ms
    try std.testing.expectEqual(@as(i32, 43200000), rows[4].getColumn(0).?.asInt32().?);
}

test "read PyArrow DECIMAL logical type" {
    const allocator = std.testing.allocator;

    const file = std.Io.Dir.cwd().openFile(io, "../test-files-arrow/logical_types/decimal.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close(io);

    var reader = try parquet.openFileDynamic(allocator, file, io, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 5), reader.metadata.num_rows);

    const schema = reader.getSchema();

    // decimal_9_2 - DECIMAL(9,2) should use INT32 or FIXED_LEN_BYTE_ARRAY
    {
        const dec_schema = schema[1];
        try std.testing.expect(dec_schema.logical_type != null);

        switch (dec_schema.logical_type.?) {
            .decimal => |d| {
                try std.testing.expectEqual(@as(i32, 9), d.precision);
                try std.testing.expectEqual(@as(i32, 2), d.scale);
            },
            else => return error.WrongLogicalType,
        }
    }

    // decimal_18_4 - DECIMAL(18,4) should use INT64 or FIXED_LEN_BYTE_ARRAY
    {
        const dec_schema = schema[2];
        try std.testing.expect(dec_schema.logical_type != null);

        switch (dec_schema.logical_type.?) {
            .decimal => |d| {
                try std.testing.expectEqual(@as(i32, 18), d.precision);
                try std.testing.expectEqual(@as(i32, 4), d.scale);
            },
            else => return error.WrongLogicalType,
        }
    }

    // decimal_38_10 - DECIMAL(38,10) requires FIXED_LEN_BYTE_ARRAY
    {
        const dec_schema = schema[3];
        try std.testing.expect(dec_schema.logical_type != null);

        switch (dec_schema.logical_type.?) {
            .decimal => |d| {
                try std.testing.expectEqual(@as(i32, 38), d.precision);
                try std.testing.expectEqual(@as(i32, 10), d.scale);
            },
            else => return error.WrongLogicalType,
        }
    }
}

test "read PyArrow INT types logical type" {
    const allocator = std.testing.allocator;

    const file = std.Io.Dir.cwd().openFile(io, "../test-files-arrow/logical_types/int_types.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close(io);

    var reader = try parquet.openFileDynamic(allocator, file, io, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 5), reader.metadata.num_rows);

    const schema = reader.getSchema();

    // int8_col - INT(8, signed)
    {
        const int_schema = schema[1];
        try std.testing.expectEqual(format.PhysicalType.int32, int_schema.type_.?);
        try std.testing.expect(int_schema.logical_type != null);

        switch (int_schema.logical_type.?) {
            .int => |i| {
                try std.testing.expectEqual(@as(i8, 8), i.bit_width);
                try std.testing.expect(i.is_signed);
            },
            else => return error.WrongLogicalType,
        }
    }

    // uint8_col - INT(8, unsigned)
    {
        const int_schema = schema[5];
        try std.testing.expectEqual(format.PhysicalType.int32, int_schema.type_.?);
        try std.testing.expect(int_schema.logical_type != null);

        switch (int_schema.logical_type.?) {
            .int => |i| {
                try std.testing.expectEqual(@as(i8, 8), i.bit_width);
                try std.testing.expect(!i.is_signed);
            },
            else => return error.WrongLogicalType,
        }
    }

    // uint64_col - INT(64, unsigned)
    {
        const int_schema = schema[8];
        try std.testing.expectEqual(format.PhysicalType.int64, int_schema.type_.?);
        try std.testing.expect(int_schema.logical_type != null);

        switch (int_schema.logical_type.?) {
            .int => |i| {
                try std.testing.expectEqual(@as(i8, 64), i.bit_width);
                try std.testing.expect(!i.is_signed);
            },
            else => return error.WrongLogicalType,
        }
    }

    // Read int8 data and verify (col 0)
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 5), rows.len);
    try std.testing.expectEqual(@as(i32, 1), rows[0].getColumn(0).?.asInt32().?);
    try std.testing.expectEqual(@as(i32, -128), rows[1].getColumn(0).?.asInt32().?);
    try std.testing.expectEqual(@as(i32, 127), rows[2].getColumn(0).?.asInt32().?);
}

test "read PyArrow FLOAT16 logical type" {
    const allocator = std.testing.allocator;

    const file = std.Io.Dir.cwd().openFile(io, "../test-files-arrow/logical_types/float16.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close(io);

    var reader = try parquet.openFileDynamic(allocator, file, io, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 5), reader.metadata.num_rows);

    const schema = reader.getSchema();

    // float16_col - FLOAT16 (FIXED_LEN_BYTE_ARRAY(2))
    const f16_schema = schema[1];
    try std.testing.expectEqual(format.PhysicalType.fixed_len_byte_array, f16_schema.type_.?);
    try std.testing.expectEqual(@as(?i32, 2), f16_schema.type_length);
    try std.testing.expect(f16_schema.logical_type != null);

    switch (f16_schema.logical_type.?) {
        .float16 => {}, // Expected
        else => return error.WrongLogicalType,
    }

    // Read data and verify it's 2-byte values
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 5), rows.len);
    // First non-null value should be 2 bytes
    const val = rows[0].getColumn(0).?;
    if (val.isNull()) return error.ExpectedValue;
    try std.testing.expectEqual(@as(usize, 2), val.asBytes().?.len);
}

test "read PyArrow JSON logical type" {
    const allocator = std.testing.allocator;

    const file = std.Io.Dir.cwd().openFile(io, "../test-files-arrow/logical_types/json.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close(io);

    var reader = try parquet.openFileDynamic(allocator, file, io, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 5), reader.metadata.num_rows);

    const schema = reader.getSchema();

    // json_col - JSON (stored as large_string in PyArrow)
    const json_schema = schema[1];
    // Note: PyArrow stores JSON as large_string which is still BYTE_ARRAY
    try std.testing.expectEqual(format.PhysicalType.byte_array, json_schema.type_.?);

    // Read data
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 5), rows.len);
    try std.testing.expectEqualStrings("{\"name\": \"Alice\", \"age\": 30}", rows[0].getColumn(0).?.asBytes().?);
}
