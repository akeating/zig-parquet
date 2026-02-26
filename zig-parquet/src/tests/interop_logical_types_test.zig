//! Interop tests for logical types
//!
//! Tests reading PyArrow-generated Parquet files with logical type annotations
//! to verify our reader correctly parses logical types from real-world files.

const std = @import("std");
const parquet = @import("../lib.zig");
const format = parquet.format;
const Reader = parquet.Reader;
const Optional = parquet.Optional;

test "read PyArrow STRING logical type" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/logical_types/string.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file (run from zig-parquet dir): {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
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
    const values = try reader.readColumn(0, []const u8);
    defer {
        for (values) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(values);
    }

    try std.testing.expectEqual(@as(usize, 5), values.len);
    try std.testing.expectEqualStrings("hello", values[0].value);
    try std.testing.expectEqualStrings("world", values[1].value);
    try std.testing.expectEqualStrings("", values[2].value);
    try std.testing.expectEqualStrings("unicode: 🎉", values[3].value);
    try std.testing.expectEqual(Optional([]const u8){ .null_value = {} }, values[4]);
}

test "read PyArrow DATE logical type" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/logical_types/date.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
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
    const values = try reader.readColumn(0, i32);
    defer allocator.free(values);

    try std.testing.expectEqual(@as(usize, 5), values.len);

    // 2024-01-15 = 19737 days since 1970-01-01
    try std.testing.expectEqual(@as(i32, 19737), values[0].value);
    // 1970-01-01 = 0 days
    try std.testing.expectEqual(@as(i32, 0), values[1].value);
    // 2000-06-15 = 11123 days
    try std.testing.expectEqual(@as(i32, 11123), values[2].value);
    // 1999-12-31 = 10956 days
    try std.testing.expectEqual(@as(i32, 10956), values[3].value);
    // null
    try std.testing.expectEqual(Optional(i32){ .null_value = {} }, values[4]);
}

test "read PyArrow TIMESTAMP logical type" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/logical_types/timestamp.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
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
    const values = try reader.readColumn(0, i64);
    defer allocator.free(values);

    try std.testing.expectEqual(@as(usize, 5), values.len);
    // First value should be non-null
    switch (values[0]) {
        .value => {}, // Expected
        .null_value => return error.ExpectedValue,
    }
    // Fourth value should be null
    switch (values[3]) {
        .value => return error.ExpectedNull,
        .null_value => {}, // Expected
    }
}

test "read PyArrow TIME logical type" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/logical_types/time.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
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

    // Read millis column and verify data (INT32)
    const millis_values = try reader.readColumn(0, i32);
    defer allocator.free(millis_values);

    try std.testing.expectEqual(@as(usize, 5), millis_values.len);

    // 10:30:00 = 37800000 ms
    try std.testing.expectEqual(@as(i32, 37800000), millis_values[0].value);
    // 00:00:00 = 0 ms
    try std.testing.expectEqual(@as(i32, 0), millis_values[1].value);
    // 23:59:59 = 86399000 ms
    try std.testing.expectEqual(@as(i32, 86399000), millis_values[2].value);
    // null
    try std.testing.expectEqual(Optional(i32){ .null_value = {} }, millis_values[3]);
    // 12:00:00 = 43200000 ms
    try std.testing.expectEqual(@as(i32, 43200000), millis_values[4].value);
}

test "read PyArrow DECIMAL logical type" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/logical_types/decimal.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
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

    const file = std.fs.cwd().openFile("../test-files-arrow/logical_types/int_types.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
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

    // Read int8 data and verify
    const int8_values = try reader.readColumn(0, i32);
    defer allocator.free(int8_values);

    try std.testing.expectEqual(@as(usize, 5), int8_values.len);
    try std.testing.expectEqual(@as(i32, 1), int8_values[0].value);
    try std.testing.expectEqual(@as(i32, -128), int8_values[1].value);
    try std.testing.expectEqual(@as(i32, 127), int8_values[2].value);
}

test "read PyArrow FLOAT16 logical type" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/logical_types/float16.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
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
    const values = try reader.readColumn(0, []const u8);
    defer {
        for (values) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(values);
    }

    try std.testing.expectEqual(@as(usize, 5), values.len);
    // First non-null value should be 2 bytes
    switch (values[0]) {
        .value => |v| try std.testing.expectEqual(@as(usize, 2), v.len),
        .null_value => return error.ExpectedValue,
    }
}

test "read PyArrow JSON logical type" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/logical_types/json.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 5), reader.metadata.num_rows);

    const schema = reader.getSchema();

    // json_col - JSON (stored as large_string in PyArrow)
    const json_schema = schema[1];
    // Note: PyArrow stores JSON as large_string which is still BYTE_ARRAY
    try std.testing.expectEqual(format.PhysicalType.byte_array, json_schema.type_.?);

    // Read data
    const values = try reader.readColumn(0, []const u8);
    defer {
        for (values) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(values);
    }

    try std.testing.expectEqual(@as(usize, 5), values.len);
    try std.testing.expectEqualStrings("{\"name\": \"Alice\", \"age\": 30}", values[0].value);
}
