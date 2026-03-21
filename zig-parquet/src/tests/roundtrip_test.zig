//! Comprehensive round-trip tests for all compression codecs
//!
//! Tests write → read → compare for all supported compression formats.

const std = @import("std");
const parquet = @import("../lib.zig");
const build_options = @import("build_options");

const ALL_CODECS = if (build_options.no_compression)
    [_]parquet.format.CompressionCodec{.uncompressed}
else
    [_]parquet.format.CompressionCodec{
        .uncompressed,
        .zstd,
        .gzip,
        .snappy,
        .lz4_raw,
        .brotli,
    };

fn codecName(codec: parquet.format.CompressionCodec) []const u8 {
    return switch (codec) {
        .uncompressed => "uncompressed",
        .zstd => "zstd",
        .gzip => "gzip",
        .snappy => "snappy",
        .lz4_raw => "lz4_raw",
        .brotli => "brotli",
        .lzo => "lzo",
        .lz4 => "lz4",
    };
}

test "round-trip all codecs with i64" {
    const allocator = std.testing.allocator;

    for (ALL_CODECS) |codec| {
        var tmp_dir = std.testing.tmpDir(.{});
        defer tmp_dir.cleanup();

        const file_path = "roundtrip_i64.parquet";

        // Write
        {
            const file = try tmp_dir.dir.createFile(file_path, .{});
            defer file.close();

            const columns = [_]parquet.ColumnDef{
                .{ .name = "value", .type_ = .int64, .optional = false, .codec = codec },
            };

            var writer = try parquet.writeToFile(allocator, file, &columns);
            defer writer.deinit();

            const values = [_]i64{ 1, 2, 3, 4, 5, -100, 0, 999999, -123456, 42 };
            try writer.writeColumn(i64, 0, &values);
            try writer.close();
        }

        // Read and verify
        {
            const file = try tmp_dir.dir.openFile(file_path, .{});
            defer file.close();

            var reader = try parquet.openFile(allocator, file);
            defer reader.deinit();

            try std.testing.expectEqual(@as(i64, 10), reader.metadata.num_rows);

            // Verify codec
            const col_chunk = reader.metadata.row_groups[0].columns[0];
            try std.testing.expectEqual(codec, col_chunk.meta_data.?.codec);

            // Read and compare
            const col = try reader.readColumn(0, i64);
            defer allocator.free(col);

            const expected = [_]i64{ 1, 2, 3, 4, 5, -100, 0, 999999, -123456, 42 };
            try std.testing.expectEqual(@as(usize, 10), col.len);

            for (col, 0..) |v, i| {
                switch (v) {
                    .value => |val| try std.testing.expectEqual(expected[i], val),
                    .null_value => return error.UnexpectedNull,
                }
            }
        }
    }
}

test "round-trip all codecs with mixed types" {
    const allocator = std.testing.allocator;

    for (ALL_CODECS) |codec| {
        var tmp_dir = std.testing.tmpDir(.{});
        defer tmp_dir.cleanup();

        const file_path = "roundtrip_mixed.parquet";

        // Test values
        const i32_values = [_]i32{ 1, 2, 3, 4, 5 };
        const i64_values = [_]i64{ 100, 200, 300, 400, 500 };
        const f64_values = [_]f64{ 1.1, 2.2, 3.3, 4.4, 5.5 };
        const str_values = [_][]const u8{ "hello", "world", "test", "data", "parquet" };

        // Write
        {
            const file = try tmp_dir.dir.createFile(file_path, .{});
            defer file.close();

            var str_col = parquet.ColumnDef.string("name", false);
            str_col.codec = codec;

            const columns = [_]parquet.ColumnDef{
                .{ .name = "int32_col", .type_ = .int32, .optional = false, .codec = codec },
                .{ .name = "int64_col", .type_ = .int64, .optional = false, .codec = codec },
                .{ .name = "double_col", .type_ = .double, .optional = false, .codec = codec },
                str_col,
            };

            var writer = try parquet.writeToFile(allocator, file, &columns);
            defer writer.deinit();

            try writer.writeColumn(i32, 0, &i32_values);
            try writer.writeColumn(i64, 1, &i64_values);
            try writer.writeColumn(f64, 2, &f64_values);
            try writer.writeColumn([]const u8, 3, &str_values);
            try writer.close();
        }

        // Read and verify
        {
            const file = try tmp_dir.dir.openFile(file_path, .{});
            defer file.close();

            var reader = try parquet.openFile(allocator, file);
            defer reader.deinit();

            try std.testing.expectEqual(@as(i64, 5), reader.metadata.num_rows);

            // Verify i32 column
            {
                const col = try reader.readColumn(0, i32);
                defer allocator.free(col);

                for (col, 0..) |v, i| {
                    switch (v) {
                        .value => |val| try std.testing.expectEqual(i32_values[i], val),
                        .null_value => return error.UnexpectedNull,
                    }
                }
            }

            // Verify i64 column
            {
                const col = try reader.readColumn(1, i64);
                defer allocator.free(col);

                for (col, 0..) |v, i| {
                    switch (v) {
                        .value => |val| try std.testing.expectEqual(i64_values[i], val),
                        .null_value => return error.UnexpectedNull,
                    }
                }
            }

            // Verify f64 column
            {
                const col = try reader.readColumn(2, f64);
                defer allocator.free(col);

                for (col, 0..) |v, i| {
                    switch (v) {
                        .value => |val| try std.testing.expectApproxEqAbs(f64_values[i], val, 0.0001),
                        .null_value => return error.UnexpectedNull,
                    }
                }
            }

            // Verify string column
            {
                const col = try reader.readColumn(3, []const u8);
                defer {
                    for (col) |v| {
                        switch (v) {
                            .value => |s| allocator.free(s),
                            .null_value => {},
                        }
                    }
                    allocator.free(col);
                }

                for (col, 0..) |v, i| {
                    switch (v) {
                        .value => |s| try std.testing.expectEqualStrings(str_values[i], s),
                        .null_value => return error.UnexpectedNull,
                    }
                }
            }
        }
    }
}

test "round-trip with nullable columns" {
    if (build_options.no_compression) return;
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_nullable.parquet";

    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "nullable_int", .type_ = .int64, .optional = true, .codec = .zstd },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        // Write nullable values: some values, some nulls
        const nullable_values = [_]?i64{ 1, null, 3, null, 5, 6, null, 8, 9, null };
        try writer.writeColumnNullable(i64, 0, &nullable_values);
        try writer.close();
    }

    // Read and verify
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 10), reader.metadata.num_rows);

        const col = try reader.readColumn(0, i64);
        defer allocator.free(col);

        const expected = [_]?i64{ 1, null, 3, null, 5, 6, null, 8, 9, null };

        for (col, 0..) |v, i| {
            switch (v) {
                .value => |val| {
                    if (expected[i]) |exp| {
                        try std.testing.expectEqual(exp, val);
                    } else {
                        return error.ExpectedNull;
                    }
                },
                .null_value => {
                    if (expected[i] != null) {
                        return error.UnexpectedNull;
                    }
                },
            }
        }
    }
}

test "round-trip all physical types" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_all_physical.parquet";

    // Test data
    const bool_values = [_]bool{ true, false, true, true, false };
    const i32_values = [_]i32{ 1, -2, 3, -4, 5 };
    const i64_values = [_]i64{ 100, -200, 300, -400, 500 };
    const f32_values = [_]f32{ 1.5, 2.5, 3.5, 4.5, 5.5 };
    const f64_values = [_]f64{ 1.111, 2.222, 3.333, 4.444, 5.555 };
    const str_values = [_][]const u8{ "hello", "world", "test", "data", "end" };
    // Fixed-length byte arrays (4 bytes each)
    const fixed_values = [_][]const u8{ "AAAA", "BBBB", "CCCC", "DDDD", "EEEE" };

    // Write
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "bool_col", .type_ = .boolean, .optional = false },
            .{ .name = "int32_col", .type_ = .int32, .optional = false },
            .{ .name = "int64_col", .type_ = .int64, .optional = false },
            .{ .name = "float_col", .type_ = .float, .optional = false },
            .{ .name = "double_col", .type_ = .double, .optional = false },
            .{ .name = "byte_array_col", .type_ = .byte_array, .optional = false },
            .{ .name = "fixed_col", .type_ = .fixed_len_byte_array, .optional = false, .type_length = 4 },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        try writer.writeColumn(bool, 0, &bool_values);
        try writer.writeColumn(i32, 1, &i32_values);
        try writer.writeColumn(i64, 2, &i64_values);
        try writer.writeColumn(f32, 3, &f32_values);
        try writer.writeColumn(f64, 4, &f64_values);
        try writer.writeColumn([]const u8, 5, &str_values);
        try writer.writeColumnFixedByteArray(6, &fixed_values);
        try writer.close();
    }

    // Read and verify
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 5), reader.metadata.num_rows);

        // Verify bool column
        {
            const col = try reader.readColumn(0, bool);
            defer allocator.free(col);

            for (col, 0..) |v, i| {
                switch (v) {
                    .value => |val| try std.testing.expectEqual(bool_values[i], val),
                    .null_value => return error.UnexpectedNull,
                }
            }
        }

        // Verify i32 column
        {
            const col = try reader.readColumn(1, i32);
            defer allocator.free(col);

            for (col, 0..) |v, i| {
                switch (v) {
                    .value => |val| try std.testing.expectEqual(i32_values[i], val),
                    .null_value => return error.UnexpectedNull,
                }
            }
        }

        // Verify i64 column
        {
            const col = try reader.readColumn(2, i64);
            defer allocator.free(col);

            for (col, 0..) |v, i| {
                switch (v) {
                    .value => |val| try std.testing.expectEqual(i64_values[i], val),
                    .null_value => return error.UnexpectedNull,
                }
            }
        }

        // Verify f32 column
        {
            const col = try reader.readColumn(3, f32);
            defer allocator.free(col);

            for (col, 0..) |v, i| {
                switch (v) {
                    .value => |val| try std.testing.expectApproxEqAbs(f32_values[i], val, 0.0001),
                    .null_value => return error.UnexpectedNull,
                }
            }
        }

        // Verify f64 column
        {
            const col = try reader.readColumn(4, f64);
            defer allocator.free(col);

            for (col, 0..) |v, i| {
                switch (v) {
                    .value => |val| try std.testing.expectApproxEqAbs(f64_values[i], val, 0.0001),
                    .null_value => return error.UnexpectedNull,
                }
            }
        }

        // Verify byte_array column
        {
            const col = try reader.readColumn(5, []const u8);
            defer {
                for (col) |v| {
                    switch (v) {
                        .value => |s| allocator.free(s),
                        .null_value => {},
                    }
                }
                allocator.free(col);
            }

            for (col, 0..) |v, i| {
                switch (v) {
                    .value => |s| try std.testing.expectEqualStrings(str_values[i], s),
                    .null_value => return error.UnexpectedNull,
                }
            }
        }

        // Verify fixed_len_byte_array column
        {
            const col = try reader.readColumn(6, []const u8);
            defer {
                for (col) |v| {
                    switch (v) {
                        .value => |s| allocator.free(s),
                        .null_value => {},
                    }
                }
                allocator.free(col);
            }

            for (col, 0..) |v, i| {
                switch (v) {
                    .value => |s| try std.testing.expectEqualStrings(fixed_values[i], s),
                    .null_value => return error.UnexpectedNull,
                }
            }
        }
    }
}

test "round-trip logical types" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_logical.parquet";

    // Test data
    const string_values = [_][]const u8{ "hello", "world", "parquet" };
    const date_values = [_]i32{ 19000, 19001, 19002 }; // Days since epoch
    const timestamp_values = [_]i64{ 1704067200000000, 1704153600000000, 1704240000000000 }; // Micros since epoch
    const time_values = [_]i64{ 3600000000, 7200000000, 10800000000 }; // Micros since midnight
    const decimal_values = [_]i64{ 12345, -67890, 11111 }; // Scaled integers
    // UUID is 16 bytes
    const uuid_values = [_][]const u8{
        "0123456789ABCDEF",
        "FEDCBA9876543210",
        "ABCDEFGHIJKLMNOP",
    };
    const json_values = [_][]const u8{ "{\"a\":1}", "{\"b\":2}", "{\"c\":3}" };
    const enum_values = [_][]const u8{ "RED", "GREEN", "BLUE" };

    // Write
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const string_col = parquet.ColumnDef.string("string_col", false);
        const date_col = parquet.ColumnDef.date("date_col", false);
        const timestamp_col = parquet.ColumnDef.timestamp("ts_col", .micros, true, false);
        const time_col = parquet.ColumnDef.time("time_col", .micros, false, false);
        const decimal_col = parquet.ColumnDef.decimal("decimal_col", 10, 2, false);
        const uuid_col = parquet.ColumnDef.uuid("uuid_col", false);
        const json_col = parquet.ColumnDef.json("json_col", false);
        const enum_col = parquet.ColumnDef.enum_("enum_col", false);

        const columns = [_]parquet.ColumnDef{
            string_col,
            date_col,
            timestamp_col,
            time_col,
            decimal_col,
            uuid_col,
            json_col,
            enum_col,
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        try writer.writeColumn([]const u8, 0, &string_values); // STRING
        try writer.writeColumn(i32, 1, &date_values); // DATE
        try writer.writeColumn(i64, 2, &timestamp_values); // TIMESTAMP
        try writer.writeColumn(i64, 3, &time_values); // TIME
        try writer.writeColumn(i64, 4, &decimal_values); // DECIMAL
        try writer.writeColumnFixedByteArray(5, &uuid_values); // UUID
        try writer.writeColumn([]const u8, 6, &json_values); // JSON
        try writer.writeColumn([]const u8, 7, &enum_values); // ENUM
        try writer.close();
    }

    // Read and verify
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 3), reader.metadata.num_rows);

        // Verify schema has correct logical types
        const schema = reader.getSchema();

        // Find and verify each column's logical type
        var col_idx: usize = 0;
        for (schema) |elem| {
            if (elem.type_ != null and elem.num_children == null) {
                switch (col_idx) {
                    0 => { // string_col
                        try std.testing.expect(elem.logical_type != null);
                        try std.testing.expectEqual(parquet.format.LogicalType.string, elem.logical_type.?);
                    },
                    1 => { // date_col
                        try std.testing.expect(elem.logical_type != null);
                        try std.testing.expectEqual(parquet.format.LogicalType.date, elem.logical_type.?);
                    },
                    2 => { // timestamp_col
                        try std.testing.expect(elem.logical_type != null);
                        switch (elem.logical_type.?) {
                            .timestamp => |ts| {
                                try std.testing.expectEqual(parquet.format.TimeUnit.micros, ts.unit);
                                try std.testing.expectEqual(true, ts.is_adjusted_to_utc);
                            },
                            else => return error.WrongLogicalType,
                        }
                    },
                    3 => { // time_col
                        try std.testing.expect(elem.logical_type != null);
                        switch (elem.logical_type.?) {
                            .time => |t| {
                                try std.testing.expectEqual(parquet.format.TimeUnit.micros, t.unit);
                            },
                            else => return error.WrongLogicalType,
                        }
                    },
                    4 => { // decimal_col
                        try std.testing.expect(elem.logical_type != null);
                        switch (elem.logical_type.?) {
                            .decimal => |d| {
                                try std.testing.expectEqual(@as(i32, 10), d.precision);
                                try std.testing.expectEqual(@as(i32, 2), d.scale);
                            },
                            else => return error.WrongLogicalType,
                        }
                    },
                    5 => { // uuid_col
                        try std.testing.expect(elem.logical_type != null);
                        try std.testing.expectEqual(parquet.format.LogicalType.uuid, elem.logical_type.?);
                    },
                    6 => { // json_col
                        try std.testing.expect(elem.logical_type != null);
                        try std.testing.expectEqual(parquet.format.LogicalType.json, elem.logical_type.?);
                    },
                    7 => { // enum_col
                        try std.testing.expect(elem.logical_type != null);
                        try std.testing.expectEqual(parquet.format.LogicalType.enum_, elem.logical_type.?);
                    },
                    else => {},
                }
                col_idx += 1;
            }
        }

        // Verify string data
        {
            const col = try reader.readColumn(0, []const u8);
            defer {
                for (col) |v| {
                    switch (v) {
                        .value => |s| allocator.free(s),
                        .null_value => {},
                    }
                }
                allocator.free(col);
            }

            for (col, 0..) |v, i| {
                switch (v) {
                    .value => |s| try std.testing.expectEqualStrings(string_values[i], s),
                    .null_value => return error.UnexpectedNull,
                }
            }
        }

        // Verify date data
        {
            const col = try reader.readColumn(1, i32);
            defer allocator.free(col);

            for (col, 0..) |v, i| {
                switch (v) {
                    .value => |val| try std.testing.expectEqual(date_values[i], val),
                    .null_value => return error.UnexpectedNull,
                }
            }
        }

        // Verify timestamp data
        {
            const col = try reader.readColumn(2, i64);
            defer allocator.free(col);

            for (col, 0..) |v, i| {
                switch (v) {
                    .value => |val| try std.testing.expectEqual(timestamp_values[i], val),
                    .null_value => return error.UnexpectedNull,
                }
            }
        }

        // Verify uuid data
        {
            const col = try reader.readColumn(5, []const u8);
            defer {
                for (col) |v| {
                    switch (v) {
                        .value => |s| allocator.free(s),
                        .null_value => {},
                    }
                }
                allocator.free(col);
            }

            for (col, 0..) |v, i| {
                switch (v) {
                    .value => |s| try std.testing.expectEqualStrings(uuid_values[i], s),
                    .null_value => return error.UnexpectedNull,
                }
            }
        }
    }
}

test "round-trip validates compression actually works" {
    if (build_options.no_compression) return;
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    // Create compressible data (repeated pattern)
    var values: [1000]i64 = undefined;
    for (&values, 0..) |*v, i| {
        v.* = @intCast(i % 10); // Repeating pattern 0-9
    }

    // Write uncompressed
    var uncompressed_size: u64 = 0;
    {
        const file = try tmp_dir.dir.createFile("uncompressed.parquet", .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "value", .type_ = .int64, .optional = false, .codec = .uncompressed },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        try writer.writeColumn(i64, 0, &values);
        try writer.close();

        uncompressed_size = (try file.stat()).size;
    }

    // Write with zstd
    var zstd_size: u64 = 0;
    {
        const file = try tmp_dir.dir.createFile("zstd.parquet", .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "value", .type_ = .int64, .optional = false, .codec = .zstd },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        try writer.writeColumn(i64, 0, &values);
        try writer.close();

        zstd_size = (try file.stat()).size;
    }

    // Compressed file should be smaller
    try std.testing.expect(zstd_size < uncompressed_size);

    // Verify both read back correctly
    {
        const file = try tmp_dir.dir.openFile("zstd.parquet", .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        const col = try reader.readColumn(0, i64);
        defer allocator.free(col);

        try std.testing.expectEqual(@as(usize, 1000), col.len);
        for (col, 0..) |v, i| {
            switch (v) {
                .value => |val| try std.testing.expectEqual(values[i], val),
                .null_value => return error.UnexpectedNull,
            }
        }
    }
}

test "round-trip statistics i32" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const values = [_]i32{ 5, 2, 8, 1, 9, 3, 7, 4, 6, 0 };
    const expected_min: i32 = 0;
    const expected_max: i32 = 9;

    // Write
    {
        const file = try tmp_dir.dir.createFile("stats_i32.parquet", .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "value", .type_ = .int32, .optional = false },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        try writer.writeColumn(i32, 0, &values);
        try writer.close();
    }

    // Read and verify statistics
    {
        const file = try tmp_dir.dir.openFile("stats_i32.parquet", .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        const stats = reader.getColumnStatistics(0, 0);
        try std.testing.expect(stats != null);

        const s = stats.?;
        try std.testing.expectEqual(expected_min, s.minAsI32().?);
        try std.testing.expectEqual(expected_max, s.maxAsI32().?);
        try std.testing.expectEqual(@as(i64, 0), s.null_count.?);
    }
}

test "round-trip statistics i64 with nulls" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const values = [_]?i64{ 100, null, 50, null, 75, 25 };
    const expected_min: i64 = 25;
    const expected_max: i64 = 100;
    const expected_null_count: i64 = 2;

    // Write
    {
        const file = try tmp_dir.dir.createFile("stats_i64_nullable.parquet", .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "value", .type_ = .int64, .optional = true },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        try writer.writeColumnNullable(i64, 0, &values);
        try writer.close();
    }

    // Read and verify statistics
    {
        const file = try tmp_dir.dir.openFile("stats_i64_nullable.parquet", .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        const stats = reader.getColumnStatistics(0, 0);
        try std.testing.expect(stats != null);

        const s = stats.?;
        try std.testing.expectEqual(expected_min, s.minAsI64().?);
        try std.testing.expectEqual(expected_max, s.maxAsI64().?);
        try std.testing.expectEqual(expected_null_count, s.null_count.?);
    }
}

test "round-trip statistics byte array" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const values = [_][]const u8{ "banana", "apple", "cherry", "date" };
    const expected_min = "apple"; // Lexicographically smallest
    const expected_max = "date"; // Lexicographically largest

    // Write
    {
        const file = try tmp_dir.dir.createFile("stats_string.parquet", .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "name", .type_ = .byte_array, .optional = false },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        try writer.writeColumn([]const u8, 0, &values);
        try writer.close();
    }

    // Read and verify statistics
    {
        const file = try tmp_dir.dir.openFile("stats_string.parquet", .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        const stats = reader.getColumnStatistics(0, 0);
        try std.testing.expect(stats != null);

        const s = stats.?;
        try std.testing.expectEqualStrings(expected_min, s.getMinBytes().?);
        try std.testing.expectEqualStrings(expected_max, s.getMaxBytes().?);
        try std.testing.expectEqual(@as(i64, 0), s.null_count.?);
    }
}

test "round-trip statistics double" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const values = [_]f64{ 3.14, 2.71, 1.41, 1.73 };
    const expected_min: f64 = 1.41;
    const expected_max: f64 = 3.14;

    // Write
    {
        const file = try tmp_dir.dir.createFile("stats_double.parquet", .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "value", .type_ = .double, .optional = false },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        try writer.writeColumn(f64, 0, &values);
        try writer.close();
    }

    // Read and verify statistics
    {
        const file = try tmp_dir.dir.openFile("stats_double.parquet", .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        const stats = reader.getColumnStatistics(0, 0);
        try std.testing.expect(stats != null);

        const s = stats.?;
        try std.testing.expectApproxEqAbs(expected_min, s.minAsF64().?, 0.001);
        try std.testing.expectApproxEqAbs(expected_max, s.maxAsF64().?, 0.001);
        try std.testing.expectEqual(@as(i64, 0), s.null_count.?);
    }
}

test "round-trip statistics boolean" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const values = [_]bool{ true, false, true, true, false };
    const expected_min: bool = false;
    const expected_max: bool = true;

    // Write
    {
        const file = try tmp_dir.dir.createFile("stats_bool.parquet", .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "flag", .type_ = .boolean, .optional = false },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        try writer.writeColumn(bool, 0, &values);
        try writer.close();
    }

    // Read and verify statistics
    {
        const file = try tmp_dir.dir.openFile("stats_bool.parquet", .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        const stats = reader.getColumnStatistics(0, 0);
        try std.testing.expect(stats != null);

        const s = stats.?;
        try std.testing.expectEqual(expected_min, s.minAsBool().?);
        try std.testing.expectEqual(expected_max, s.maxAsBool().?);
        try std.testing.expectEqual(@as(i64, 0), s.null_count.?);
    }
}

test "round-trip statistics float" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const values = [_]f32{ 1.5, -2.5, 3.5, 0.5 };
    const expected_min: f32 = -2.5;
    const expected_max: f32 = 3.5;

    // Write
    {
        const file = try tmp_dir.dir.createFile("stats_float.parquet", .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "value", .type_ = .float, .optional = false },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        try writer.writeColumn(f32, 0, &values);
        try writer.close();
    }

    // Read and verify statistics
    {
        const file = try tmp_dir.dir.openFile("stats_float.parquet", .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        const stats = reader.getColumnStatistics(0, 0);
        try std.testing.expect(stats != null);

        const s = stats.?;
        try std.testing.expectApproxEqAbs(expected_min, s.minAsF32().?, 0.001);
        try std.testing.expectApproxEqAbs(expected_max, s.maxAsF32().?, 0.001);
        try std.testing.expectEqual(@as(i64, 0), s.null_count.?);
    }
}

test "round-trip statistics fixed byte array" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    // 4-byte fixed values - lexicographically compared
    const v1 = [_]u8{ 0x00, 0x01, 0x02, 0x03 };
    const v2 = [_]u8{ 0xFF, 0xFE, 0xFD, 0xFC };
    const v3 = [_]u8{ 0x10, 0x20, 0x30, 0x40 };
    const v4 = [_]u8{ 0x00, 0x00, 0x00, 0x01 };
    const values = [_][]const u8{ &v1, &v2, &v3, &v4 };
    const expected_min = [_]u8{ 0x00, 0x00, 0x00, 0x01 };
    const expected_max = [_]u8{ 0xFF, 0xFE, 0xFD, 0xFC };

    // Write
    {
        const file = try tmp_dir.dir.createFile("stats_fixed.parquet", .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "uuid", .type_ = .fixed_len_byte_array, .optional = false, .type_length = 4 },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        try writer.writeColumnFixedByteArray(0, &values);
        try writer.close();
    }

    // Read and verify statistics
    {
        const file = try tmp_dir.dir.openFile("stats_fixed.parquet", .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        const stats = reader.getColumnStatistics(0, 0);
        try std.testing.expect(stats != null);

        const s = stats.?;
        const min_bytes = s.getMinBytes().?;
        const max_bytes = s.getMaxBytes().?;
        try std.testing.expectEqualSlices(u8, &expected_min, min_bytes);
        try std.testing.expectEqualSlices(u8, &expected_max, max_bytes);
        try std.testing.expectEqual(@as(i64, 0), s.null_count.?);
    }
}

test "round-trip statistics edge values i32" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    // Test with extreme values
    const values = [_]i32{
        std.math.minInt(i32), // -2147483648
        std.math.maxInt(i32), // 2147483647
        0,
        -1,
        1,
    };

    // Write
    {
        const file = try tmp_dir.dir.createFile("stats_edge_i32.parquet", .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "value", .type_ = .int32, .optional = false },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        try writer.writeColumn(i32, 0, &values);
        try writer.close();
    }

    // Read and verify statistics
    {
        const file = try tmp_dir.dir.openFile("stats_edge_i32.parquet", .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        const stats = reader.getColumnStatistics(0, 0);
        try std.testing.expect(stats != null);

        const s = stats.?;
        try std.testing.expectEqual(std.math.minInt(i32), s.minAsI32().?);
        try std.testing.expectEqual(std.math.maxInt(i32), s.maxAsI32().?);
        try std.testing.expectEqual(@as(i64, 0), s.null_count.?);
    }
}

test "round-trip statistics edge values i64" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    // Test with extreme values
    const values = [_]i64{
        std.math.minInt(i64), // -9223372036854775808
        std.math.maxInt(i64), // 9223372036854775807
        0,
        -1,
        1,
    };

    // Write
    {
        const file = try tmp_dir.dir.createFile("stats_edge_i64.parquet", .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "value", .type_ = .int64, .optional = false },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        try writer.writeColumn(i64, 0, &values);
        try writer.close();
    }

    // Read and verify statistics
    {
        const file = try tmp_dir.dir.openFile("stats_edge_i64.parquet", .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        const stats = reader.getColumnStatistics(0, 0);
        try std.testing.expect(stats != null);

        const s = stats.?;
        try std.testing.expectEqual(std.math.minInt(i64), s.minAsI64().?);
        try std.testing.expectEqual(std.math.maxInt(i64), s.maxAsI64().?);
        try std.testing.expectEqual(@as(i64, 0), s.null_count.?);
    }
}

test "round-trip statistics all nulls" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    // All null values - statistics should have null_count = 4, no min/max
    const values = [_]?i32{ null, null, null, null };

    // Write
    {
        const file = try tmp_dir.dir.createFile("stats_all_nulls.parquet", .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "value", .type_ = .int32, .optional = true },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        try writer.writeColumnNullable(i32, 0, &values);
        try writer.close();
    }

    // Read and verify statistics
    {
        const file = try tmp_dir.dir.openFile("stats_all_nulls.parquet", .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        const stats = reader.getColumnStatistics(0, 0);
        try std.testing.expect(stats != null);

        const s = stats.?;
        // With all nulls, min/max should be null
        try std.testing.expect(s.minAsI32() == null);
        try std.testing.expect(s.maxAsI32() == null);
        try std.testing.expectEqual(@as(i64, 4), s.null_count.?);
    }
}

// =============================================================================
// Nullable encoding tests (Phase 10 P0 / Phase 11 style)
// =============================================================================

test "round-trip nullable i64 with delta_binary_packed encoding" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "nullable_delta_i64.parquet";

    // Test struct with optional i64 field
    const Record = struct {
        id: ?i64,
    };

    // Write with delta_binary_packed encoding
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        var writer = try parquet.writeToFileRows(Record, allocator, file, .{
            .compression = .uncompressed,
            .use_dictionary = false, // Required for int_encoding to take effect
            .int_encoding = .delta_binary_packed,
        });
        defer writer.deinit();

        // Monotonic sequence with some nulls - optimal for delta encoding
        const records = [_]Record{
            .{ .id = 100 },
            .{ .id = null },
            .{ .id = 102 },
            .{ .id = 103 },
            .{ .id = null },
            .{ .id = 105 },
            .{ .id = 106 },
            .{ .id = null },
            .{ .id = 108 },
            .{ .id = 109 },
        };

        for (records) |rec| {
            try writer.writeRow(rec);
        }
        try writer.close();
    }

    // Read and verify
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 10), reader.metadata.num_rows);

        // Verify encoding is delta_binary_packed
        const col_chunk = reader.metadata.row_groups[0].columns[0];
        const encodings = col_chunk.meta_data.?.encodings;
        var has_delta = false;
        for (encodings) |enc| {
            if (enc == .delta_binary_packed) {
                has_delta = true;
                break;
            }
        }
        try std.testing.expect(has_delta);

        // Read and compare values
        const col = try reader.readColumn(0, i64);
        defer allocator.free(col);

        const expected = [_]?i64{ 100, null, 102, 103, null, 105, 106, null, 108, 109 };
        try std.testing.expectEqual(@as(usize, 10), col.len);

        for (col, 0..) |v, i| {
            switch (v) {
                .value => |val| {
                    if (expected[i]) |exp| {
                        try std.testing.expectEqual(exp, val);
                    } else {
                        return error.ExpectedNull;
                    }
                },
                .null_value => {
                    if (expected[i] != null) {
                        return error.UnexpectedNull;
                    }
                },
            }
        }
    }
}

test "round-trip nullable f64 with byte_stream_split encoding" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "nullable_bss_f64.parquet";

    // Test struct with optional f64 field
    const SensorReading = struct {
        value: ?f64,
    };

    // Write with byte_stream_split encoding
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        var writer = try parquet.writeToFileRows(SensorReading, allocator, file, .{
            .compression = .uncompressed,
            .float_encoding = .byte_stream_split,
        });
        defer writer.deinit();

        // Sensor readings with some nulls (missing readings)
        const records = [_]SensorReading{
            .{ .value = 1.5 },
            .{ .value = 2.7 },
            .{ .value = null }, // sensor failure
            .{ .value = 3.14159 },
            .{ .value = null }, // sensor failure
            .{ .value = 2.71828 },
            .{ .value = 1.41421 },
            .{ .value = null },
            .{ .value = 0.0 },
            .{ .value = -1.5 },
        };

        for (records) |rec| {
            try writer.writeRow(rec);
        }
        try writer.close();
    }

    // Read and verify
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 10), reader.metadata.num_rows);

        // Verify encoding is byte_stream_split
        const col_chunk = reader.metadata.row_groups[0].columns[0];
        const encodings = col_chunk.meta_data.?.encodings;
        var has_bss = false;
        for (encodings) |enc| {
            if (enc == .byte_stream_split) {
                has_bss = true;
                break;
            }
        }
        try std.testing.expect(has_bss);

        // Read and compare values
        const col = try reader.readColumn(0, f64);
        defer allocator.free(col);

        const expected = [_]?f64{ 1.5, 2.7, null, 3.14159, null, 2.71828, 1.41421, null, 0.0, -1.5 };
        try std.testing.expectEqual(@as(usize, 10), col.len);

        for (col, 0..) |v, i| {
            switch (v) {
                .value => |val| {
                    if (expected[i]) |exp| {
                        try std.testing.expectApproxEqAbs(exp, val, 0.00001);
                    } else {
                        return error.ExpectedNull;
                    }
                },
                .null_value => {
                    if (expected[i] != null) {
                        return error.UnexpectedNull;
                    }
                },
            }
        }
    }
}

test "round-trip nullable i32 with delta_binary_packed encoding" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "nullable_delta_i32.parquet";

    const Counter = struct {
        count: ?i32,
    };

    // Write with delta_binary_packed encoding
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        var writer = try parquet.writeToFileRows(Counter, allocator, file, .{
            .compression = if (build_options.no_compression) .uncompressed else .zstd,
            .use_dictionary = false, // Required for int_encoding to take effect
            .int_encoding = .delta_binary_packed,
        });
        defer writer.deinit();

        const records = [_]Counter{
            .{ .count = 0 },
            .{ .count = 1 },
            .{ .count = null },
            .{ .count = 3 },
            .{ .count = 4 },
        };

        for (records) |rec| {
            try writer.writeRow(rec);
        }
        try writer.close();
    }

    // Read and verify
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 5), reader.metadata.num_rows);

        const col = try reader.readColumn(0, i32);
        defer allocator.free(col);

        const expected = [_]?i32{ 0, 1, null, 3, 4 };

        for (col, 0..) |v, i| {
            switch (v) {
                .value => |val| {
                    if (expected[i]) |exp| {
                        try std.testing.expectEqual(exp, val);
                    } else {
                        return error.ExpectedNull;
                    }
                },
                .null_value => {
                    if (expected[i] != null) {
                        return error.UnexpectedNull;
                    }
                },
            }
        }
    }
}

test "round-trip nullable f32 with byte_stream_split encoding" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "nullable_bss_f32.parquet";

    const Measurement = struct {
        temp: ?f32,
    };

    // Write with byte_stream_split encoding
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        var writer = try parquet.writeToFileRows(Measurement, allocator, file, .{
            .compression = if (build_options.no_compression) .uncompressed else .zstd,
            .float_encoding = .byte_stream_split,
        });
        defer writer.deinit();

        const records = [_]Measurement{
            .{ .temp = 20.5 },
            .{ .temp = null },
            .{ .temp = 21.0 },
            .{ .temp = 21.5 },
            .{ .temp = null },
        };

        for (records) |rec| {
            try writer.writeRow(rec);
        }
        try writer.close();
    }

    // Read and verify
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 5), reader.metadata.num_rows);

        const col = try reader.readColumn(0, f32);
        defer allocator.free(col);

        const expected = [_]?f32{ 20.5, null, 21.0, 21.5, null };

        for (col, 0..) |v, i| {
            switch (v) {
                .value => |val| {
                    if (expected[i]) |exp| {
                        try std.testing.expectApproxEqAbs(exp, val, 0.001);
                    } else {
                        return error.ExpectedNull;
                    }
                },
                .null_value => {
                    if (expected[i] != null) {
                        return error.UnexpectedNull;
                    }
                },
            }
        }
    }
}

test "per-column encoding overrides" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "per_column_encoding.parquet";

    // Test struct with multiple columns of the same type but different encodings
    const Record = struct {
        timestamp: i64, // Will use delta_binary_packed (override)
        count: i64, // Will use plain (no override, falls back to int_encoding)
        temperature: f64, // Will use byte_stream_split (override)
        pressure: f64, // Will use plain (no override)
    };

    // Write with per-column encoding overrides
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        var writer = try parquet.writeToFileRows(Record, allocator, file, .{
            .compression = .uncompressed,
            .use_dictionary = false,
            .column_encodings = &.{
                .{ .name = "timestamp", .encoding = .delta_binary_packed },
                .{ .name = "temperature", .encoding = .byte_stream_split },
            },
        });
        defer writer.deinit();

        // Monotonic timestamps - optimal for delta encoding
        const records = [_]Record{
            .{ .timestamp = 1000, .count = 5, .temperature = 20.5, .pressure = 101.3 },
            .{ .timestamp = 1001, .count = 10, .temperature = 21.0, .pressure = 101.2 },
            .{ .timestamp = 1002, .count = 15, .temperature = 21.5, .pressure = 101.1 },
            .{ .timestamp = 1003, .count = 20, .temperature = 22.0, .pressure = 101.0 },
            .{ .timestamp = 1004, .count = 25, .temperature = 22.5, .pressure = 100.9 },
        };

        for (records) |rec| {
            try writer.writeRow(rec);
        }
        try writer.close();
    }

    // Read and verify
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 5), reader.metadata.num_rows);

        // Verify timestamp column uses delta_binary_packed
        const timestamp_chunk = reader.metadata.row_groups[0].columns[0];
        const timestamp_encodings = timestamp_chunk.meta_data.?.encodings;
        var has_delta = false;
        for (timestamp_encodings) |enc| {
            if (enc == .delta_binary_packed) {
                has_delta = true;
                break;
            }
        }
        try std.testing.expect(has_delta);

        // Verify temperature column uses byte_stream_split
        const temp_chunk = reader.metadata.row_groups[0].columns[2];
        const temp_encodings = temp_chunk.meta_data.?.encodings;
        var has_bss = false;
        for (temp_encodings) |enc| {
            if (enc == .byte_stream_split) {
                has_bss = true;
                break;
            }
        }
        try std.testing.expect(has_bss);

        // Verify count column uses plain (no delta)
        const count_chunk = reader.metadata.row_groups[0].columns[1];
        const count_encodings = count_chunk.meta_data.?.encodings;
        var count_has_delta = false;
        for (count_encodings) |enc| {
            if (enc == .delta_binary_packed) {
                count_has_delta = true;
                break;
            }
        }
        try std.testing.expect(!count_has_delta);

        // Verify pressure column uses plain (no byte_stream_split)
        const pressure_chunk = reader.metadata.row_groups[0].columns[3];
        const pressure_encodings = pressure_chunk.meta_data.?.encodings;
        var pressure_has_bss = false;
        for (pressure_encodings) |enc| {
            if (enc == .byte_stream_split) {
                pressure_has_bss = true;
                break;
            }
        }
        try std.testing.expect(!pressure_has_bss);

        // Read and verify data round-trips correctly
        const timestamps = try reader.readColumn(0, i64);
        defer allocator.free(timestamps);
        try std.testing.expectEqual(@as(usize, 5), timestamps.len);

        const expected_ts = [_]i64{ 1000, 1001, 1002, 1003, 1004 };
        for (timestamps, 0..) |v, i| {
            switch (v) {
                .value => |val| try std.testing.expectEqual(expected_ts[i], val),
                .null_value => return error.UnexpectedNull,
            }
        }

        const temps = try reader.readColumn(2, f64);
        defer allocator.free(temps);

        const expected_temps = [_]f64{ 20.5, 21.0, 21.5, 22.0, 22.5 };
        for (temps, 0..) |v, i| {
            switch (v) {
                .value => |val| try std.testing.expectApproxEqAbs(expected_temps[i], val, 0.001),
                .null_value => return error.UnexpectedNull,
            }
        }
    }
}

test "round-trip i32 with byte_stream_split encoding" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "bss_i32.parquet";

    const Counter = struct {
        value: i32,
    };

    // Write with byte_stream_split encoding
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        var writer = try parquet.writeToFileRows(Counter, allocator, file, .{
            .compression = .uncompressed,
            .use_dictionary = false,
            .int_encoding = .byte_stream_split,
        });
        defer writer.deinit();

        const records = [_]Counter{
            .{ .value = 0 },
            .{ .value = 1 },
            .{ .value = -1 },
            .{ .value = 256 },
            .{ .value = -256 },
            .{ .value = 65536 },
            .{ .value = std.math.maxInt(i32) },
            .{ .value = std.math.minInt(i32) },
        };

        for (records) |rec| {
            try writer.writeRow(rec);
        }
        try writer.close();
    }

    // Read and verify
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 8), reader.metadata.num_rows);

        // Verify encoding is byte_stream_split
        const col_chunk = reader.metadata.row_groups[0].columns[0];
        const encodings = col_chunk.meta_data.?.encodings;
        var has_bss = false;
        for (encodings) |enc| {
            if (enc == .byte_stream_split) {
                has_bss = true;
                break;
            }
        }
        try std.testing.expect(has_bss);

        // Read and compare values
        const col = try reader.readColumn(0, i32);
        defer allocator.free(col);

        const expected = [_]i32{ 0, 1, -1, 256, -256, 65536, std.math.maxInt(i32), std.math.minInt(i32) };
        try std.testing.expectEqual(@as(usize, 8), col.len);

        for (col, 0..) |v, i| {
            switch (v) {
                .value => |val| {
                    try std.testing.expectEqual(expected[i], val);
                },
                .null_value => return error.UnexpectedNull,
            }
        }
    }
}

test "round-trip i64 with byte_stream_split encoding" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "bss_i64.parquet";

    const Timestamp = struct {
        value: i64,
    };

    // Write with byte_stream_split encoding
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        var writer = try parquet.writeToFileRows(Timestamp, allocator, file, .{
            .compression = .uncompressed,
            .use_dictionary = false,
            .int_encoding = .byte_stream_split,
        });
        defer writer.deinit();

        const records = [_]Timestamp{
            .{ .value = 0 },
            .{ .value = 1 },
            .{ .value = -1 },
            .{ .value = 0x0102030405060708 },
            .{ .value = std.math.maxInt(i64) },
            .{ .value = std.math.minInt(i64) },
        };

        for (records) |rec| {
            try writer.writeRow(rec);
        }
        try writer.close();
    }

    // Read and verify
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 6), reader.metadata.num_rows);

        // Verify encoding is byte_stream_split
        const col_chunk = reader.metadata.row_groups[0].columns[0];
        const encodings = col_chunk.meta_data.?.encodings;
        var has_bss = false;
        for (encodings) |enc| {
            if (enc == .byte_stream_split) {
                has_bss = true;
                break;
            }
        }
        try std.testing.expect(has_bss);

        // Read and compare values
        const col = try reader.readColumn(0, i64);
        defer allocator.free(col);

        const expected = [_]i64{ 0, 1, -1, 0x0102030405060708, std.math.maxInt(i64), std.math.minInt(i64) };
        try std.testing.expectEqual(@as(usize, 6), col.len);

        for (col, 0..) |v, i| {
            switch (v) {
                .value => |val| {
                    try std.testing.expectEqual(expected[i], val);
                },
                .null_value => return error.UnexpectedNull,
            }
        }
    }
}

test "round-trip nullable i32 with byte_stream_split encoding" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "nullable_bss_i32.parquet";

    const NullableCounter = struct {
        value: ?i32,
    };

    // Write with byte_stream_split encoding
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        var writer = try parquet.writeToFileRows(NullableCounter, allocator, file, .{
            .compression = .uncompressed,
            .use_dictionary = false,
            .int_encoding = .byte_stream_split,
        });
        defer writer.deinit();

        const records = [_]NullableCounter{
            .{ .value = 0 },
            .{ .value = null },
            .{ .value = 100 },
            .{ .value = -200 },
            .{ .value = null },
            .{ .value = 12345 },
        };

        for (records) |rec| {
            try writer.writeRow(rec);
        }
        try writer.close();
    }

    // Read and verify
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 6), reader.metadata.num_rows);

        // Read and compare values
        const col = try reader.readColumn(0, i32);
        defer allocator.free(col);

        const expected = [_]?i32{ 0, null, 100, -200, null, 12345 };
        try std.testing.expectEqual(@as(usize, 6), col.len);

        for (col, 0..) |v, i| {
            switch (v) {
                .value => |val| {
                    if (expected[i]) |exp| {
                        try std.testing.expectEqual(exp, val);
                    } else {
                        return error.ExpectedNull;
                    }
                },
                .null_value => {
                    if (expected[i] != null) {
                        return error.UnexpectedNull;
                    }
                },
            }
        }
    }
}

test "round-trip RLE boolean encoding" {
    const allocator = std.testing.allocator;

    const BoolRow = struct {
        flag: bool,
        nullable_flag: ?bool,
    };

    // Create test data with runs of same values (ideal for RLE)
    const rows = [_]BoolRow{
        .{ .flag = true, .nullable_flag = true },
        .{ .flag = true, .nullable_flag = true },
        .{ .flag = true, .nullable_flag = true },
        .{ .flag = true, .nullable_flag = null },
        .{ .flag = false, .nullable_flag = false },
        .{ .flag = false, .nullable_flag = false },
        .{ .flag = false, .nullable_flag = false },
        .{ .flag = false, .nullable_flag = false },
        .{ .flag = true, .nullable_flag = true },
        .{ .flag = true, .nullable_flag = null },
    };

    // Write with RLE boolean encoding
    var rw = try parquet.writeToBufferRows(BoolRow, allocator, .{
        .compression = .uncompressed,
        .use_dictionary = false,
        .bool_encoding = .rle,
    });
    defer rw.deinit();

    try rw.writeRows(&rows);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    // Verify we got valid data
    try std.testing.expect(buffer.len > 0);

    // Read back and verify
    var reader = try parquet.openBufferRowReader(BoolRow, allocator, buffer, .{});
    defer reader.deinit();

    var i: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        try std.testing.expectEqual(rows[i].flag, row.flag);
        try std.testing.expectEqual(rows[i].nullable_flag, row.nullable_flag);
        i += 1;
    }
    try std.testing.expectEqual(rows.len, i);
}

test "round-trip FLBA with byte_stream_split encoding (low-level Writer)" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const val0 = [_]u8{ 0xAA, 0xBB, 0xCC, 0xDD };
    const val1 = [_]u8{ 0x11, 0x22, 0x33, 0x44 };
    const val2 = [_]u8{ 0xFF, 0x00, 0xFF, 0x00 };
    const values = [_][]const u8{ &val0, &val1, &val2 };

    {
        const file = try tmp_dir.dir.createFile("bss_flba.parquet", .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{
                .name = "fixed_col",
                .type_ = .fixed_len_byte_array,
                .optional = false,
                .type_length = 4,
                .value_encoding = .byte_stream_split,
            },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        try writer.writeColumnFixedByteArray(0, &values);
        try writer.close();
    }

    {
        const file = try tmp_dir.dir.openFile("bss_flba.parquet", .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 3), reader.metadata.num_rows);

        const col_chunk = reader.metadata.row_groups[0].columns[0];
        const encodings = col_chunk.meta_data.?.encodings;
        var has_bss = false;
        for (encodings) |enc| {
            if (enc == .byte_stream_split) {
                has_bss = true;
                break;
            }
        }
        try std.testing.expect(has_bss);

        const col = try reader.readColumn(0, []const u8);
        defer {
            for (col) |v| {
                if (v == .value) allocator.free(v.value);
            }
            allocator.free(col);
        }

        try std.testing.expectEqual(@as(usize, 3), col.len);
        try std.testing.expectEqualSlices(u8, &val0, col[0].value);
        try std.testing.expectEqualSlices(u8, &val1, col[1].value);
        try std.testing.expectEqualSlices(u8, &val2, col[2].value);
    }
}

test "round-trip UUID with byte_stream_split encoding (RowWriter)" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const UuidRow = struct {
        id: parquet.Uuid,
    };

    const rows = [_]UuidRow{
        .{ .id = parquet.Uuid.fromBytes(.{ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10 }) },
        .{ .id = parquet.Uuid.fromBytes(.{ 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20 }) },
        .{ .id = parquet.Uuid.fromBytes(.{ 0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA, 0xF9, 0xF8, 0xF7, 0xF6, 0xF5, 0xF4, 0xF3, 0xF2, 0xF1, 0xF0 }) },
    };

    {
        const file = try tmp_dir.dir.createFile("bss_uuid.parquet", .{});
        defer file.close();

        var writer = try parquet.writeToFileRows(UuidRow, allocator, file, .{
            .compression = .uncompressed,
            .use_dictionary = false,
            .column_encodings = &.{
                .{ .name = "id", .encoding = .byte_stream_split },
            },
        });
        defer writer.deinit();

        for (&rows) |*row| {
            try writer.writeRow(row.*);
        }
        try writer.close();
    }

    // Verify encoding metadata
    {
        const file = try tmp_dir.dir.openFile("bss_uuid.parquet", .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 3), reader.metadata.num_rows);

        const col_chunk = reader.metadata.row_groups[0].columns[0];
        const encodings = col_chunk.meta_data.?.encodings;
        var has_bss = false;
        for (encodings) |enc| {
            if (enc == .byte_stream_split) {
                has_bss = true;
                break;
            }
        }
        try std.testing.expect(has_bss);
    }

    // Read back with RowReader and verify values
    {
        const file = try tmp_dir.dir.openFile("bss_uuid.parquet", .{});
        defer file.close();

        var row_reader = try parquet.openFileRowReader(UuidRow, allocator, file, .{});
        defer row_reader.deinit();

        var i: usize = 0;
        while (try row_reader.next()) |row| {
            defer row_reader.freeRow(&row);
            try std.testing.expectEqualSlices(u8, &rows[i].id.bytes, &row.id.bytes);
            i += 1;
        }
        try std.testing.expectEqual(rows.len, i);
    }
}

test "round-trip list of UUIDs with byte_stream_split encoding" {
    const allocator = std.testing.allocator;

    const UuidListRow = struct {
        ids: []const parquet.Uuid,
    };

    const uuid1 = parquet.Uuid{ .bytes = .{ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10 } };
    const uuid2 = parquet.Uuid{ .bytes = .{ 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20 } };
    const uuid3 = parquet.Uuid{ .bytes = .{ 0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA, 0xF9, 0xF8, 0xF7, 0xF6, 0xF5, 0xF4, 0xF3, 0xF2, 0xF1, 0xF0 } };

    const list1 = [_]parquet.Uuid{ uuid1, uuid2 };
    const list2 = [_]parquet.Uuid{uuid3};
    const list3 = [_]parquet.Uuid{};

    const rows = [_]UuidListRow{
        .{ .ids = &list1 },
        .{ .ids = &list2 },
        .{ .ids = &list3 },
    };

    // Write
    var rw = try parquet.writeToBufferRows(UuidListRow, allocator, .{
        .compression = .uncompressed,
        .use_dictionary = false,
        .column_encodings = &.{
            .{ .name = "ids", .encoding = .byte_stream_split },
        },
    });
    defer rw.deinit();

    for (&rows) |*row| {
        try rw.writeRow(row.*);
    }
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    try std.testing.expect(buffer.len > 0);

    // Read back and verify
    var reader = try parquet.openBufferRowReader(UuidListRow, allocator, buffer, .{});
    defer reader.deinit();

    var row_idx: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);

        const expected = rows[row_idx];
        try std.testing.expectEqual(expected.ids.len, row.ids.len);
        for (expected.ids, 0..) |exp_uuid, j| {
            try std.testing.expectEqualSlices(u8, &exp_uuid.bytes, &row.ids[j].bytes);
        }
        row_idx += 1;
    }
    try std.testing.expectEqual(rows.len, row_idx);
}

test "round-trip list of structs with UUID field" {
    const allocator = std.testing.allocator;

    const TaggedId = struct {
        tag: i32,
        id: parquet.Uuid,
    };

    const Row = struct {
        entries: []const TaggedId,
    };

    const uuid1 = parquet.Uuid{ .bytes = .{ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10 } };
    const uuid2 = parquet.Uuid{ .bytes = .{ 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00 } };

    const entries1 = [_]TaggedId{
        .{ .tag = 1, .id = uuid1 },
        .{ .tag = 2, .id = uuid2 },
    };
    const entries2 = [_]TaggedId{
        .{ .tag = 42, .id = uuid1 },
    };

    const rows = [_]Row{
        .{ .entries = &entries1 },
        .{ .entries = &entries2 },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{
        .compression = .uncompressed,
        .use_dictionary = false,
    });
    defer rw.deinit();

    for (&rows) |*row| {
        try rw.writeRow(row.*);
    }
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var row_idx: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);

        const expected = rows[row_idx];
        try std.testing.expectEqual(expected.entries.len, row.entries.len);
        for (expected.entries, 0..) |exp, j| {
            try std.testing.expectEqual(exp.tag, row.entries[j].tag);
            try std.testing.expectEqualSlices(u8, &exp.id.bytes, &row.entries[j].id.bytes);
        }
        row_idx += 1;
    }
    try std.testing.expectEqual(rows.len, row_idx);
}

test "round-trip Interval column" {
    const allocator = std.testing.allocator;

    const Row = struct {
        duration: parquet.Interval,
    };

    const rows = [_]Row{
        .{ .duration = parquet.Interval.fromComponents(1, 15, 3600000) },
        .{ .duration = parquet.Interval.fromComponents(0, 0, 0) },
        .{ .duration = parquet.Interval.fromComponents(12, 365, 86400000) },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{
        .compression = .uncompressed,
        .use_dictionary = false,
    });
    defer rw.deinit();

    for (&rows) |*row| {
        try rw.writeRow(row.*);
    }
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var row_idx: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        const expected = rows[row_idx];
        try std.testing.expectEqual(expected.duration.months, row.duration.months);
        try std.testing.expectEqual(expected.duration.days, row.duration.days);
        try std.testing.expectEqual(expected.duration.millis, row.duration.millis);
        row_idx += 1;
    }
    try std.testing.expectEqual(rows.len, row_idx);
}

test "round-trip list of Intervals" {
    const allocator = std.testing.allocator;

    const Row = struct {
        durations: []const parquet.Interval,
    };

    const list1 = [_]parquet.Interval{
        parquet.Interval.fromComponents(1, 10, 1000),
        parquet.Interval.fromComponents(2, 20, 2000),
    };
    const list2 = [_]parquet.Interval{
        parquet.Interval.fromComponents(0, 0, 0),
    };
    const list3 = [_]parquet.Interval{};

    const rows = [_]Row{
        .{ .durations = &list1 },
        .{ .durations = &list2 },
        .{ .durations = &list3 },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{
        .compression = .uncompressed,
        .use_dictionary = false,
    });
    defer rw.deinit();

    for (&rows) |*row| {
        try rw.writeRow(row.*);
    }
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var row_idx: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        const expected = rows[row_idx];
        try std.testing.expectEqual(expected.durations.len, row.durations.len);
        for (expected.durations, 0..) |exp, j| {
            try std.testing.expectEqual(exp.months, row.durations[j].months);
            try std.testing.expectEqual(exp.days, row.durations[j].days);
            try std.testing.expectEqual(exp.millis, row.durations[j].millis);
        }
        row_idx += 1;
    }
    try std.testing.expectEqual(rows.len, row_idx);
}

test "round-trip list of structs with Interval field" {
    const allocator = std.testing.allocator;

    const Event = struct {
        code: i32,
        duration: parquet.Interval,
    };

    const Row = struct {
        events: []const Event,
    };

    const events1 = [_]Event{
        .{ .code = 1, .duration = parquet.Interval.fromComponents(1, 15, 3600000) },
        .{ .code = 2, .duration = parquet.Interval.fromComponents(0, 30, 0) },
    };
    const events2 = [_]Event{
        .{ .code = 42, .duration = parquet.Interval.fromComponents(12, 0, 86400000) },
    };

    const rows = [_]Row{
        .{ .events = &events1 },
        .{ .events = &events2 },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{
        .compression = .uncompressed,
        .use_dictionary = false,
    });
    defer rw.deinit();

    for (&rows) |*row| {
        try rw.writeRow(row.*);
    }
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var row_idx: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        const expected = rows[row_idx];
        try std.testing.expectEqual(expected.events.len, row.events.len);
        for (expected.events, 0..) |exp, j| {
            try std.testing.expectEqual(exp.code, row.events[j].code);
            try std.testing.expectEqual(exp.duration.months, row.events[j].duration.months);
            try std.testing.expectEqual(exp.duration.days, row.events[j].duration.days);
            try std.testing.expectEqual(exp.duration.millis, row.events[j].duration.millis);
        }
        row_idx += 1;
    }
    try std.testing.expectEqual(rows.len, row_idx);
}

test "round-trip list of structs with Date leaf field" {
    const allocator = std.testing.allocator;

    const Event = struct {
        code: i32,
        date: parquet.Date,
    };

    const Row = struct {
        events: []const Event,
    };

    const events1 = [_]Event{
        .{ .code = 1, .date = parquet.Date.fromDays(19675) },
        .{ .code = 2, .date = parquet.Date.fromDays(19700) },
    };
    const events2 = [_]Event{
        .{ .code = 42, .date = parquet.Date.fromDays(0) },
    };

    const rows = [_]Row{
        .{ .events = &events1 },
        .{ .events = &events2 },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{
        .compression = .uncompressed,
        .use_dictionary = false,
    });
    defer rw.deinit();

    for (&rows) |*row| {
        try rw.writeRow(row.*);
    }
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var row_idx: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        const expected = rows[row_idx];
        try std.testing.expectEqual(expected.events.len, row.events.len);
        for (expected.events, 0..) |exp, j| {
            try std.testing.expectEqual(exp.code, row.events[j].code);
            try std.testing.expectEqual(exp.date.days, row.events[j].date.days);
        }
        row_idx += 1;
    }
    try std.testing.expectEqual(rows.len, row_idx);
}

test "round-trip list of structs with Timestamp leaf field" {
    const allocator = std.testing.allocator;

    const Measurement = struct {
        sensor_id: i32,
        ts: parquet.TimestampMicros,
    };

    const Row = struct {
        readings: []const Measurement,
    };

    const readings1 = [_]Measurement{
        .{ .sensor_id = 1, .ts = parquet.TimestampMicros.fromMicros(1700000000000000) },
        .{ .sensor_id = 2, .ts = parquet.TimestampMicros.fromMicros(1700000001000000) },
    };
    const readings2 = [_]Measurement{
        .{ .sensor_id = 99, .ts = parquet.TimestampMicros.fromMicros(0) },
    };

    const rows = [_]Row{
        .{ .readings = &readings1 },
        .{ .readings = &readings2 },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{
        .compression = .uncompressed,
        .use_dictionary = false,
    });
    defer rw.deinit();

    for (&rows) |*row| {
        try rw.writeRow(row.*);
    }
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var row_idx: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        const expected = rows[row_idx];
        try std.testing.expectEqual(expected.readings.len, row.readings.len);
        for (expected.readings, 0..) |exp, j| {
            try std.testing.expectEqual(exp.sensor_id, row.readings[j].sensor_id);
            try std.testing.expectEqual(exp.ts.value, row.readings[j].ts.value);
        }
        row_idx += 1;
    }
    try std.testing.expectEqual(rows.len, row_idx);
}

test "round-trip list of structs with Time leaf field" {
    const allocator = std.testing.allocator;

    const Schedule = struct {
        slot: i32,
        time: parquet.TimeMicros,
    };

    const Row = struct {
        entries: []const Schedule,
    };

    const entries1 = [_]Schedule{
        .{ .slot = 1, .time = parquet.TimeMicros.fromHms(9, 0, 0, 0) },
        .{ .slot = 2, .time = parquet.TimeMicros.fromHms(14, 30, 0, 0) },
    };
    const entries2 = [_]Schedule{
        .{ .slot = 10, .time = parquet.TimeMicros.fromHms(23, 59, 59, 999999) },
    };

    const rows = [_]Row{
        .{ .entries = &entries1 },
        .{ .entries = &entries2 },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{
        .compression = .uncompressed,
        .use_dictionary = false,
    });
    defer rw.deinit();

    for (&rows) |*row| {
        try rw.writeRow(row.*);
    }
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var row_idx: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        const expected = rows[row_idx];
        try std.testing.expectEqual(expected.entries.len, row.entries.len);
        for (expected.entries, 0..) |exp, j| {
            try std.testing.expectEqual(exp.slot, row.entries[j].slot);
            try std.testing.expectEqual(exp.time.value, row.entries[j].time.value);
        }
        row_idx += 1;
    }
    try std.testing.expectEqual(rows.len, row_idx);
}

test "round-trip list of TimestampInt96" {
    const allocator = std.testing.allocator;

    const Row = struct {
        timestamps: []const parquet.TimestampInt96,
    };

    const ts1 = [_]parquet.TimestampInt96{
        parquet.TimestampInt96.fromNanos(1700000000000000000),
        parquet.TimestampInt96.fromNanos(1700000001000000000),
        parquet.TimestampInt96.fromNanos(1700000002000000000),
    };
    const ts2 = [_]parquet.TimestampInt96{
        parquet.TimestampInt96.fromSeconds(0),
    };

    const rows = [_]Row{
        .{ .timestamps = &ts1 },
        .{ .timestamps = &ts2 },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{
        .compression = .uncompressed,
        .use_dictionary = false,
    });
    defer rw.deinit();

    for (&rows) |*row| {
        try rw.writeRow(row.*);
    }
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var row_idx: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        const expected = rows[row_idx];
        try std.testing.expectEqual(expected.timestamps.len, row.timestamps.len);
        for (expected.timestamps, 0..) |exp, j| {
            try std.testing.expectEqual(exp.value, row.timestamps[j].value);
        }
        row_idx += 1;
    }
    try std.testing.expectEqual(rows.len, row_idx);
}

test "round-trip list of structs with TimestampInt96 leaf field" {
    const allocator = std.testing.allocator;

    const LogEntry = struct {
        level: i32,
        ts: parquet.TimestampInt96,
    };

    const Row = struct {
        logs: []const LogEntry,
    };

    const logs1 = [_]LogEntry{
        .{ .level = 1, .ts = parquet.TimestampInt96.fromMicros(1700000000000000) },
        .{ .level = 2, .ts = parquet.TimestampInt96.fromMicros(1700000001000000) },
    };
    const logs2 = [_]LogEntry{
        .{ .level = 99, .ts = parquet.TimestampInt96.fromSeconds(0) },
    };

    const rows = [_]Row{
        .{ .logs = &logs1 },
        .{ .logs = &logs2 },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{
        .compression = .uncompressed,
        .use_dictionary = false,
    });
    defer rw.deinit();

    for (&rows) |*row| {
        try rw.writeRow(row.*);
    }
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var row_idx: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        const expected = rows[row_idx];
        try std.testing.expectEqual(expected.logs.len, row.logs.len);
        for (expected.logs, 0..) |exp, j| {
            try std.testing.expectEqual(exp.level, row.logs[j].level);
            try std.testing.expectEqual(exp.ts.value, row.logs[j].ts.value);
        }
        row_idx += 1;
    }
    try std.testing.expectEqual(rows.len, row_idx);
}

test "round-trip nested list of UUIDs ([][]Uuid)" {
    const allocator = std.testing.allocator;

    const NestedUuidRow = struct {
        ids: []const []const parquet.Uuid,
    };

    const uuid1 = parquet.Uuid{ .bytes = .{ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10 } };
    const uuid2 = parquet.Uuid{ .bytes = .{ 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20 } };
    const uuid3 = parquet.Uuid{ .bytes = .{ 0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA, 0xF9, 0xF8, 0xF7, 0xF6, 0xF5, 0xF4, 0xF3, 0xF2, 0xF1, 0xF0 } };

    const inner1 = [_]parquet.Uuid{ uuid1, uuid2 };
    const inner2 = [_]parquet.Uuid{uuid3};
    const inner3 = [_]parquet.Uuid{};

    const outer1 = [_][]const parquet.Uuid{ &inner1, &inner2 };
    const outer2 = [_][]const parquet.Uuid{&inner3};

    const rows = [_]NestedUuidRow{
        .{ .ids = &outer1 },
        .{ .ids = &outer2 },
    };

    var rw = try parquet.writeToBufferRows(NestedUuidRow, allocator, .{
        .compression = .uncompressed,
    });
    defer rw.deinit();

    for (&rows) |*row| {
        try rw.writeRow(row.*);
    }
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    try std.testing.expect(buffer.len > 0);

    var reader = try parquet.openBufferRowReader(NestedUuidRow, allocator, buffer, .{});
    defer reader.deinit();

    var row_idx: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);

        const expected = rows[row_idx];
        try std.testing.expectEqual(expected.ids.len, row.ids.len);
        for (expected.ids, 0..) |exp_inner, i| {
            for (exp_inner, 0..) |exp_uuid, j| {
                try std.testing.expectEqualSlices(u8, &exp_uuid.bytes, &row.ids[i][j].bytes);
            }
        }
        row_idx += 1;
    }
    try std.testing.expectEqual(rows.len, row_idx);
}

test "round-trip nested list of Intervals ([][]Interval)" {
    const allocator = std.testing.allocator;

    const NestedIntervalRow = struct {
        spans: []const []const parquet.Interval,
    };

    const iv1 = parquet.Interval.fromDays(10);
    const iv2 = parquet.Interval.fromDays(20);
    const iv3 = parquet.Interval.fromDays(30);

    const inner1 = [_]parquet.Interval{ iv1, iv2 };
    const inner2 = [_]parquet.Interval{iv3};
    const inner3 = [_]parquet.Interval{};

    const outer1 = [_][]const parquet.Interval{ &inner1, &inner2 };
    const outer2 = [_][]const parquet.Interval{&inner3};

    const rows = [_]NestedIntervalRow{
        .{ .spans = &outer1 },
        .{ .spans = &outer2 },
    };

    var rw = try parquet.writeToBufferRows(NestedIntervalRow, allocator, .{
        .compression = .uncompressed,
    });
    defer rw.deinit();

    for (&rows) |*row| {
        try rw.writeRow(row.*);
    }
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    try std.testing.expect(buffer.len > 0);

    var reader = try parquet.openBufferRowReader(NestedIntervalRow, allocator, buffer, .{});
    defer reader.deinit();

    var row_idx: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);

        const expected = rows[row_idx];
        try std.testing.expectEqual(expected.spans.len, row.spans.len);
        for (expected.spans, 0..) |exp_inner, i| {
            try std.testing.expectEqual(exp_inner.len, row.spans[i].len);
            for (exp_inner, 0..) |exp_iv, j| {
                try std.testing.expectEqualSlices(u8, &exp_iv.toBytes(), &row.spans[i][j].toBytes());
            }
        }
        row_idx += 1;
    }
    try std.testing.expectEqual(rows.len, row_idx);
}

test "round-trip optional Date column (?Date)" {
    const allocator = std.testing.allocator;

    const Row = struct {
        day: ?parquet.Date,
    };

    const rows = [_]Row{
        .{ .day = parquet.Date.fromDays(18000) },
        .{ .day = null },
        .{ .day = parquet.Date.fromDays(19500) },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{});
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var i: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        if (rows[i].day) |expected| {
            try std.testing.expect(row.day != null);
            try std.testing.expectEqual(expected.days, row.day.?.days);
        } else {
            try std.testing.expect(row.day == null);
        }
        i += 1;
    }
    try std.testing.expectEqual(rows.len, i);
}

test "round-trip optional Uuid column (?Uuid)" {
    const allocator = std.testing.allocator;

    const Row = struct {
        id: ?parquet.Uuid,
    };

    const uuid1 = parquet.Uuid{ .bytes = .{ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10 } };
    const uuid2 = parquet.Uuid{ .bytes = .{ 0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA, 0xF9, 0xF8, 0xF7, 0xF6, 0xF5, 0xF4, 0xF3, 0xF2, 0xF1, 0xF0 } };

    const rows = [_]Row{
        .{ .id = uuid1 },
        .{ .id = null },
        .{ .id = uuid2 },
        .{ .id = null },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{});
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var i: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        if (rows[i].id) |expected| {
            try std.testing.expect(row.id != null);
            try std.testing.expectEqualSlices(u8, &expected.bytes, &row.id.?.bytes);
        } else {
            try std.testing.expect(row.id == null);
        }
        i += 1;
    }
    try std.testing.expectEqual(rows.len, i);
}

test "round-trip optional list of Dates (?[]Date)" {
    const allocator = std.testing.allocator;

    const Row = struct {
        dates: ?[]const parquet.Date,
    };

    const list1 = [_]parquet.Date{ parquet.Date.fromDays(18000), parquet.Date.fromDays(18001) };
    const list2 = [_]parquet.Date{parquet.Date.fromDays(19500)};

    const rows = [_]Row{
        .{ .dates = &list1 },
        .{ .dates = null },
        .{ .dates = &list2 },
        .{ .dates = &[_]parquet.Date{} },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{});
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var i: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        if (rows[i].dates) |expected| {
            try std.testing.expect(row.dates != null);
            try std.testing.expectEqual(expected.len, row.dates.?.len);
            for (expected, 0..) |exp, j| {
                try std.testing.expectEqual(exp.days, row.dates.?[j].days);
            }
        } else {
            try std.testing.expect(row.dates == null);
        }
        i += 1;
    }
    try std.testing.expectEqual(rows.len, i);
}

test "round-trip optional list of Uuids (?[]Uuid)" {
    const allocator = std.testing.allocator;

    const Row = struct {
        ids: ?[]const parquet.Uuid,
    };

    const uuid1 = parquet.Uuid{ .bytes = .{ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10 } };
    const uuid2 = parquet.Uuid{ .bytes = .{ 0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA, 0xF9, 0xF8, 0xF7, 0xF6, 0xF5, 0xF4, 0xF3, 0xF2, 0xF1, 0xF0 } };

    const list1 = [_]parquet.Uuid{ uuid1, uuid2 };

    const rows = [_]Row{
        .{ .ids = &list1 },
        .{ .ids = null },
        .{ .ids = &[_]parquet.Uuid{} },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{});
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var i: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        if (rows[i].ids) |expected| {
            try std.testing.expect(row.ids != null);
            try std.testing.expectEqual(expected.len, row.ids.?.len);
            for (expected, 0..) |exp, j| {
                try std.testing.expectEqualSlices(u8, &exp.bytes, &row.ids.?[j].bytes);
            }
        } else {
            try std.testing.expect(row.ids == null);
        }
        i += 1;
    }
    try std.testing.expectEqual(rows.len, i);
}

test "round-trip list of optional Dates ([]?Date)" {
    const allocator = std.testing.allocator;

    const Row = struct {
        dates: []const ?parquet.Date,
    };

    const list1 = [_]?parquet.Date{ parquet.Date.fromDays(18000), null, parquet.Date.fromDays(18002) };
    const list2 = [_]?parquet.Date{null};
    const list3 = [_]?parquet.Date{parquet.Date.fromDays(19500)};

    const rows = [_]Row{
        .{ .dates = &list1 },
        .{ .dates = &list2 },
        .{ .dates = &list3 },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{});
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var i: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        const expected = rows[i].dates;
        try std.testing.expectEqual(expected.len, row.dates.len);
        for (expected, 0..) |exp, j| {
            if (exp) |e| {
                try std.testing.expect(row.dates[j] != null);
                try std.testing.expectEqual(e.days, row.dates[j].?.days);
            } else {
                try std.testing.expect(row.dates[j] == null);
            }
        }
        i += 1;
    }
    try std.testing.expectEqual(rows.len, i);
}

test "round-trip list of optional Uuids ([]?Uuid)" {
    const allocator = std.testing.allocator;

    const Row = struct {
        ids: []const ?parquet.Uuid,
    };

    const uuid1 = parquet.Uuid{ .bytes = .{ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10 } };
    const uuid2 = parquet.Uuid{ .bytes = .{ 0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA, 0xF9, 0xF8, 0xF7, 0xF6, 0xF5, 0xF4, 0xF3, 0xF2, 0xF1, 0xF0 } };

    const list1 = [_]?parquet.Uuid{ uuid1, null, uuid2 };
    const list2 = [_]?parquet.Uuid{null};

    const rows = [_]Row{
        .{ .ids = &list1 },
        .{ .ids = &list2 },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{});
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var i: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        const expected = rows[i].ids;
        try std.testing.expectEqual(expected.len, row.ids.len);
        for (expected, 0..) |exp, j| {
            if (exp) |e| {
                try std.testing.expect(row.ids[j] != null);
                try std.testing.expectEqualSlices(u8, &e.bytes, &row.ids[j].?.bytes);
            } else {
                try std.testing.expect(row.ids[j] == null);
            }
        }
        i += 1;
    }
    try std.testing.expectEqual(rows.len, i);
}

test "round-trip list of optional Intervals ([]?Interval)" {
    const allocator = std.testing.allocator;

    const Row = struct {
        spans: []const ?parquet.Interval,
    };

    const iv1 = parquet.Interval.fromDays(10);
    const iv2 = parquet.Interval.fromDays(20);

    const list1 = [_]?parquet.Interval{ iv1, null, iv2 };
    const list2 = [_]?parquet.Interval{null};
    const list3 = [_]?parquet.Interval{iv1};

    const rows = [_]Row{
        .{ .spans = &list1 },
        .{ .spans = &list2 },
        .{ .spans = &list3 },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{});
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var i: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        const expected = rows[i].spans;
        try std.testing.expectEqual(expected.len, row.spans.len);
        for (expected, 0..) |exp, j| {
            if (exp) |e| {
                try std.testing.expect(row.spans[j] != null);
                try std.testing.expectEqualSlices(u8, &e.toBytes(), &row.spans[j].?.toBytes());
            } else {
                try std.testing.expect(row.spans[j] == null);
            }
        }
        i += 1;
    }
    try std.testing.expectEqual(rows.len, i);
}

// =============================================================================
// Map round-trip tests (Phase 14 P4)
// =============================================================================

test "round-trip map<string, i32>" {
    const allocator = std.testing.allocator;
    const ME = parquet.MapEntry([]const u8, i32);

    const Row = struct {
        attrs: ?[]const ME,
    };

    const entries0 = [_]ME{
        .{ .key = "a", .value = 1 },
        .{ .key = "b", .value = 2 },
    };
    const entries1 = [_]ME{
        .{ .key = "c", .value = 3 },
    };
    const entries_empty = [_]ME{};
    const entries4 = [_]ME{
        .{ .key = "d", .value = 4 },
        .{ .key = "e", .value = null },
        .{ .key = "f", .value = 6 },
    };

    const rows = [_]Row{
        .{ .attrs = &entries0 }, // two entries
        .{ .attrs = &entries1 }, // one entry
        .{ .attrs = &entries_empty }, // empty map
        .{ .attrs = null }, // null map
        .{ .attrs = &entries4 }, // three entries, one null value
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{
        .compression = .uncompressed,
        .use_dictionary = false,
    });
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 5), reader.rowCount());

    // Row 0: [("a", 1), ("b", 2)]
    const row0 = (try reader.next()).?;
    defer reader.freeRow(&row0);
    try std.testing.expect(row0.attrs != null);
    try std.testing.expectEqual(@as(usize, 2), row0.attrs.?.len);
    try std.testing.expectEqualStrings("a", row0.attrs.?[0].key);
    try std.testing.expectEqual(@as(?i32, 1), row0.attrs.?[0].value);
    try std.testing.expectEqualStrings("b", row0.attrs.?[1].key);
    try std.testing.expectEqual(@as(?i32, 2), row0.attrs.?[1].value);

    // Row 1: [("c", 3)]
    const row1 = (try reader.next()).?;
    defer reader.freeRow(&row1);
    try std.testing.expect(row1.attrs != null);
    try std.testing.expectEqual(@as(usize, 1), row1.attrs.?.len);
    try std.testing.expectEqualStrings("c", row1.attrs.?[0].key);
    try std.testing.expectEqual(@as(?i32, 3), row1.attrs.?[0].value);

    // Row 2: [] (empty map)
    const row2 = (try reader.next()).?;
    defer reader.freeRow(&row2);
    try std.testing.expect(row2.attrs != null);
    try std.testing.expectEqual(@as(usize, 0), row2.attrs.?.len);

    // Row 3: null
    const row3 = (try reader.next()).?;
    defer reader.freeRow(&row3);
    try std.testing.expect(row3.attrs == null);

    // Row 4: [("d", 4), ("e", null), ("f", 6)]
    const row4 = (try reader.next()).?;
    defer reader.freeRow(&row4);
    try std.testing.expect(row4.attrs != null);
    try std.testing.expectEqual(@as(usize, 3), row4.attrs.?.len);
    try std.testing.expectEqualStrings("d", row4.attrs.?[0].key);
    try std.testing.expectEqual(@as(?i32, 4), row4.attrs.?[0].value);
    try std.testing.expectEqualStrings("e", row4.attrs.?[1].key);
    try std.testing.expectEqual(@as(?i32, null), row4.attrs.?[1].value);
    try std.testing.expectEqualStrings("f", row4.attrs.?[2].key);
    try std.testing.expectEqual(@as(?i32, 6), row4.attrs.?[2].value);
}

test "round-trip map<string, string>" {
    const allocator = std.testing.allocator;
    const ME = parquet.MapEntry([]const u8, []const u8);

    const Row = struct {
        tags: ?[]const ME,
    };

    const entries0 = [_]ME{
        .{ .key = "color", .value = "red" },
        .{ .key = "size", .value = "large" },
    };
    const entries1 = [_]ME{
        .{ .key = "name", .value = null },
    };

    const rows = [_]Row{
        .{ .tags = &entries0 },
        .{ .tags = &entries1 },
        .{ .tags = null },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{
        .compression = .uncompressed,
        .use_dictionary = false,
    });
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 3), reader.rowCount());

    // Row 0: [("color", "red"), ("size", "large")]
    const row0 = (try reader.next()).?;
    defer reader.freeRow(&row0);
    try std.testing.expect(row0.tags != null);
    try std.testing.expectEqual(@as(usize, 2), row0.tags.?.len);
    try std.testing.expectEqualStrings("color", row0.tags.?[0].key);
    try std.testing.expectEqualStrings("red", row0.tags.?[0].value.?);
    try std.testing.expectEqualStrings("size", row0.tags.?[1].key);
    try std.testing.expectEqualStrings("large", row0.tags.?[1].value.?);

    // Row 1: [("name", null)]
    const row1 = (try reader.next()).?;
    defer reader.freeRow(&row1);
    try std.testing.expect(row1.tags != null);
    try std.testing.expectEqual(@as(usize, 1), row1.tags.?.len);
    try std.testing.expectEqualStrings("name", row1.tags.?[0].key);
    try std.testing.expect(row1.tags.?[0].value == null);

    // Row 2: null
    const row2 = (try reader.next()).?;
    defer reader.freeRow(&row2);
    try std.testing.expect(row2.tags == null);
}

test "round-trip map<i32, i64>" {
    const allocator = std.testing.allocator;
    const ME = parquet.MapEntry(i32, i64);

    const Row = struct {
        lookup: ?[]const ME,
    };

    const entries0 = [_]ME{
        .{ .key = 100, .value = 1_000_000 },
        .{ .key = 200, .value = null },
        .{ .key = 300, .value = -999 },
    };
    const entries_empty = [_]ME{};

    const rows = [_]Row{
        .{ .lookup = &entries0 },
        .{ .lookup = &entries_empty },
        .{ .lookup = null },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{
        .compression = .uncompressed,
        .use_dictionary = false,
    });
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 3), reader.rowCount());

    // Row 0: [(100, 1000000), (200, null), (300, -999)]
    const row0 = (try reader.next()).?;
    defer reader.freeRow(&row0);
    try std.testing.expect(row0.lookup != null);
    try std.testing.expectEqual(@as(usize, 3), row0.lookup.?.len);
    try std.testing.expectEqual(@as(i32, 100), row0.lookup.?[0].key);
    try std.testing.expectEqual(@as(?i64, 1_000_000), row0.lookup.?[0].value);
    try std.testing.expectEqual(@as(i32, 200), row0.lookup.?[1].key);
    try std.testing.expectEqual(@as(?i64, null), row0.lookup.?[1].value);
    try std.testing.expectEqual(@as(i32, 300), row0.lookup.?[2].key);
    try std.testing.expectEqual(@as(?i64, -999), row0.lookup.?[2].value);

    // Row 1: [] (empty)
    const row1 = (try reader.next()).?;
    defer reader.freeRow(&row1);
    try std.testing.expect(row1.lookup != null);
    try std.testing.expectEqual(@as(usize, 0), row1.lookup.?.len);

    // Row 2: null
    const row2 = (try reader.next()).?;
    defer reader.freeRow(&row2);
    try std.testing.expect(row2.lookup == null);
}

// =============================================================================
// Decimal round-trip tests
// =============================================================================

test "round-trip Decimal(9,2) flat column" {
    const allocator = std.testing.allocator;
    const D = parquet.Decimal(9, 2);

    const Row = struct {
        price: D,
    };

    const rows = [_]Row{
        .{ .price = D.fromUnscaled(12345) }, // 123.45
        .{ .price = D.fromUnscaled(-99999) }, // -999.99
        .{ .price = D.fromUnscaled(0) }, // 0.00
        .{ .price = D.fromUnscaled(1) }, // 0.01
        .{ .price = D.fromF64(999.99) },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{});
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var i: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        try std.testing.expectEqual(rows[i].price.toI128(), row.price.toI128());
        i += 1;
    }
    try std.testing.expectEqual(rows.len, i);
}

test "round-trip Decimal(18,4) and Decimal(38,10) flat columns" {
    const allocator = std.testing.allocator;
    const D18 = parquet.Decimal(18, 4);
    const D38 = parquet.Decimal(38, 10);

    const Row = struct {
        amount: D18,
        big_val: D38,
    };

    const rows = [_]Row{
        .{
            .amount = D18.fromUnscaled(123456789012345678),
            .big_val = D38.fromUnscaled(1),
        },
        .{
            .amount = D18.fromUnscaled(-1),
            .big_val = D38.fromUnscaled(-1),
        },
        .{
            .amount = D18.fromUnscaled(0),
            .big_val = D38.fromUnscaled(0),
        },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{});
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var i: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        try std.testing.expectEqual(rows[i].amount.toI128(), row.amount.toI128());
        try std.testing.expectEqual(rows[i].big_val.toI128(), row.big_val.toI128());
        i += 1;
    }
    try std.testing.expectEqual(rows.len, i);
}

test "round-trip optional Decimal (?Decimal(9,2))" {
    const allocator = std.testing.allocator;
    const D = parquet.Decimal(9, 2);

    const Row = struct {
        price: ?D,
    };

    const rows = [_]Row{
        .{ .price = D.fromUnscaled(12345) },
        .{ .price = null },
        .{ .price = D.fromUnscaled(-99999) },
        .{ .price = null },
        .{ .price = D.fromUnscaled(0) },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{});
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var i: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        if (rows[i].price) |expected| {
            try std.testing.expect(row.price != null);
            try std.testing.expectEqual(expected.toI128(), row.price.?.toI128());
        } else {
            try std.testing.expect(row.price == null);
        }
        i += 1;
    }
    try std.testing.expectEqual(rows.len, i);
}

test "round-trip list of Decimal ([]Decimal(9,2))" {
    const allocator = std.testing.allocator;
    const D = parquet.Decimal(9, 2);

    const Row = struct {
        prices: []const D,
    };

    const list1 = [_]D{ D.fromUnscaled(100), D.fromUnscaled(200), D.fromUnscaled(-300) };
    const list2 = [_]D{D.fromUnscaled(999)};

    const rows = [_]Row{
        .{ .prices = &list1 },
        .{ .prices = &[_]D{} },
        .{ .prices = &list2 },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{});
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var i: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        try std.testing.expectEqual(rows[i].prices.len, row.prices.len);
        for (rows[i].prices, 0..) |expected, j| {
            try std.testing.expectEqual(expected.toI128(), row.prices[j].toI128());
        }
        i += 1;
    }
    try std.testing.expectEqual(rows.len, i);
}

test "round-trip Decimal read back as DecimalValue (interop)" {
    const allocator = std.testing.allocator;
    const D = parquet.Decimal(9, 2);

    const WriteRow = struct {
        price: ?D,
    };

    const ReadRow = struct {
        price: ?parquet.DecimalValue,
    };

    const write_rows = [_]WriteRow{
        .{ .price = D.fromUnscaled(12345) },
        .{ .price = null },
        .{ .price = D.fromUnscaled(-99999) },
    };

    var rw = try parquet.writeToBufferRows(WriteRow, allocator, .{});
    defer rw.deinit();
    for (&write_rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    // Read back using the runtime DecimalValue type
    var reader = try parquet.openBufferRowReader(ReadRow, allocator, buffer, .{});
    defer reader.deinit();

    const row0 = (try reader.next()).?;
    defer reader.freeRow(&row0);
    try std.testing.expect(row0.price != null);
    try std.testing.expectEqual(@as(i128, 12345), row0.price.?.toI128());
    try std.testing.expectApproxEqRel(@as(f64, 123.45), row0.price.?.toF64(), 0.001);

    const row1 = (try reader.next()).?;
    defer reader.freeRow(&row1);
    try std.testing.expect(row1.price == null);

    const row2 = (try reader.next()).?;
    defer reader.freeRow(&row2);
    try std.testing.expect(row2.price != null);
    try std.testing.expectEqual(@as(i128, -99999), row2.price.?.toI128());
}

test "RowReader reads INT32-backed decimal columns (low-level writer)" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    // Write with low-level Writer using INT32 physical type + DECIMAL logical type
    {
        const file = try tmp_dir.dir.createFile("decimal_int32.parquet", .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            parquet.ColumnDef.decimal("price", 9, 2, true),
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        const values = [_]parquet.Optional(i32){
            .{ .value = 12345 }, // 123.45
            .{ .value = -99999 }, // -999.99
            .{ .null_value = {} },
            .{ .value = 0 }, // 0.00
            .{ .value = 1 }, // 0.01
        };
        try writer.writeColumnOptional(i32, 0, &values);
        try writer.close();
    }

    // Read back using RowReader with DecimalValue
    {
        const ReadRow = struct {
            price: ?parquet.DecimalValue,
        };

        const file = try tmp_dir.dir.openFile("decimal_int32.parquet", .{});
        defer file.close();

        var reader = try parquet.openFileRowReader(ReadRow, allocator, file, .{});
        defer reader.deinit();

        const row0 = (try reader.next()).?;
        defer reader.freeRow(&row0);
        try std.testing.expect(row0.price != null);
        try std.testing.expectEqual(@as(i128, 12345), row0.price.?.toI128());

        const row1 = (try reader.next()).?;
        defer reader.freeRow(&row1);
        try std.testing.expect(row1.price != null);
        try std.testing.expectEqual(@as(i128, -99999), row1.price.?.toI128());

        const row2 = (try reader.next()).?;
        defer reader.freeRow(&row2);
        try std.testing.expect(row2.price == null);

        const row3 = (try reader.next()).?;
        defer reader.freeRow(&row3);
        try std.testing.expect(row3.price != null);
        try std.testing.expectEqual(@as(i128, 0), row3.price.?.toI128());

        const row4 = (try reader.next()).?;
        defer reader.freeRow(&row4);
        try std.testing.expect(row4.price != null);
        try std.testing.expectEqual(@as(i128, 1), row4.price.?.toI128());
    }
}

test "RowReader reads INT64-backed decimal columns (low-level writer)" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    // Write with low-level Writer using INT64 physical type + DECIMAL logical type
    {
        const file = try tmp_dir.dir.createFile("decimal_int64.parquet", .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            parquet.ColumnDef.decimal("amount", 18, 4, false),
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        const values = [_]i64{ 123456789012345678, -1, 0 };
        try writer.writeColumn(i64, 0, &values);
        try writer.close();
    }

    // Read back using RowReader with DecimalValue
    {
        const ReadRow = struct {
            amount: parquet.DecimalValue,
        };

        const file = try tmp_dir.dir.openFile("decimal_int64.parquet", .{});
        defer file.close();

        var reader = try parquet.openFileRowReader(ReadRow, allocator, file, .{});
        defer reader.deinit();

        const row0 = (try reader.next()).?;
        defer reader.freeRow(&row0);
        try std.testing.expectEqual(@as(i128, 123456789012345678), row0.amount.toI128());

        const row1 = (try reader.next()).?;
        defer reader.freeRow(&row1);
        try std.testing.expectEqual(@as(i128, -1), row1.amount.toI128());

        const row2 = (try reader.next()).?;
        defer reader.freeRow(&row2);
        try std.testing.expectEqual(@as(i128, 0), row2.amount.toI128());
    }
}

test "round-trip optional bool column (?bool)" {
    const allocator = std.testing.allocator;

    const Row = struct {
        flag: ?bool,
    };

    const rows = [_]Row{
        .{ .flag = true },
        .{ .flag = null },
        .{ .flag = false },
        .{ .flag = null },
        .{ .flag = true },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{});
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var i: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        try std.testing.expectEqual(rows[i].flag, row.flag);
        i += 1;
    }
    try std.testing.expectEqual(rows.len, i);
}

test "round-trip optional small integer types (?i8, ?i16, ?u8, ?u16, ?u32, ?u64)" {
    const allocator = std.testing.allocator;

    const Row = struct {
        a: ?i8,
        b: ?i16,
        c: ?u8,
        d: ?u16,
        e: ?u32,
        f: ?u64,
    };

    const rows = [_]Row{
        .{ .a = -1, .b = -1000, .c = 255, .d = 65535, .e = 100000, .f = 999999999 },
        .{ .a = null, .b = null, .c = null, .d = null, .e = null, .f = null },
        .{ .a = 127, .b = 32767, .c = 0, .d = 0, .e = 0, .f = 0 },
        .{ .a = -128, .b = null, .c = 1, .d = null, .e = 42, .f = null },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{});
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var i: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        try std.testing.expectEqual(rows[i].a, row.a);
        try std.testing.expectEqual(rows[i].b, row.b);
        try std.testing.expectEqual(rows[i].c, row.c);
        try std.testing.expectEqual(rows[i].d, row.d);
        try std.testing.expectEqual(rows[i].e, row.e);
        try std.testing.expectEqual(rows[i].f, row.f);
        i += 1;
    }
    try std.testing.expectEqual(rows.len, i);
}

test "round-trip optional Timestamp variants (?Timestamp(micros/millis/nanos))" {
    const allocator = std.testing.allocator;

    const TsMicros = parquet.Timestamp(.micros);
    const TsMillis = parquet.Timestamp(.millis);
    const TsNanos = parquet.Timestamp(.nanos);

    const Row = struct {
        ts_us: ?TsMicros,
        ts_ms: ?TsMillis,
        ts_ns: ?TsNanos,
    };

    const rows = [_]Row{
        .{
            .ts_us = TsMicros.fromMicros(1700000000_000000),
            .ts_ms = TsMillis.fromMillis(1700000000_000),
            .ts_ns = TsNanos.fromNanos(1700000000_000000000),
        },
        .{ .ts_us = null, .ts_ms = null, .ts_ns = null },
        .{
            .ts_us = TsMicros.fromMicros(0),
            .ts_ms = TsMillis.fromMillis(0),
            .ts_ns = TsNanos.fromNanos(0),
        },
        .{
            .ts_us = null,
            .ts_ms = TsMillis.fromMillis(-86400_000),
            .ts_ns = null,
        },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{});
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var i: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        if (rows[i].ts_us) |expected| {
            try std.testing.expect(row.ts_us != null);
            try std.testing.expectEqual(expected.value, row.ts_us.?.value);
        } else {
            try std.testing.expect(row.ts_us == null);
        }
        if (rows[i].ts_ms) |expected| {
            try std.testing.expect(row.ts_ms != null);
            try std.testing.expectEqual(expected.value, row.ts_ms.?.value);
        } else {
            try std.testing.expect(row.ts_ms == null);
        }
        if (rows[i].ts_ns) |expected| {
            try std.testing.expect(row.ts_ns != null);
            try std.testing.expectEqual(expected.value, row.ts_ns.?.value);
        } else {
            try std.testing.expect(row.ts_ns == null);
        }
        i += 1;
    }
    try std.testing.expectEqual(rows.len, i);
}

test "round-trip optional Time variants (?Time(micros/millis/nanos))" {
    const allocator = std.testing.allocator;

    const TimeMicros = parquet.Time(.micros);
    const TimeMillis = parquet.Time(.millis);
    const TimeNanos = parquet.Time(.nanos);

    const Row = struct {
        t_us: ?TimeMicros,
        t_ms: ?TimeMillis,
        t_ns: ?TimeNanos,
    };

    const rows = [_]Row{
        .{
            .t_us = TimeMicros.fromHms(10, 30, 0, 0),
            .t_ms = TimeMillis.fromHms(10, 30, 0, 0),
            .t_ns = TimeNanos.fromHms(10, 30, 0, 0),
        },
        .{ .t_us = null, .t_ms = null, .t_ns = null },
        .{
            .t_us = TimeMicros.fromHms(23, 59, 59, 999999),
            .t_ms = TimeMillis.fromHms(23, 59, 59, 999),
            .t_ns = TimeNanos.fromHms(23, 59, 59, 999999999),
        },
        .{
            .t_us = null,
            .t_ms = TimeMillis.fromHms(0, 0, 0, 0),
            .t_ns = null,
        },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{});
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var i: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        if (rows[i].t_us) |expected| {
            try std.testing.expect(row.t_us != null);
            try std.testing.expectEqual(expected.value, row.t_us.?.value);
        } else {
            try std.testing.expect(row.t_us == null);
        }
        if (rows[i].t_ms) |expected| {
            try std.testing.expect(row.t_ms != null);
            try std.testing.expectEqual(expected.value, row.t_ms.?.value);
        } else {
            try std.testing.expect(row.t_ms == null);
        }
        if (rows[i].t_ns) |expected| {
            try std.testing.expect(row.t_ns != null);
            try std.testing.expectEqual(expected.value, row.t_ns.?.value);
        } else {
            try std.testing.expect(row.t_ns == null);
        }
        i += 1;
    }
    try std.testing.expectEqual(rows.len, i);
}

test "round-trip optional Decimal variants (?Decimal(18,4), ?Decimal(38,10))" {
    const allocator = std.testing.allocator;

    const D18 = parquet.Decimal(18, 4);
    const D38 = parquet.Decimal(38, 10);

    const Row = struct {
        amount: ?D18,
        big: ?D38,
    };

    const rows = [_]Row{
        .{
            .amount = D18.fromUnscaled(123456789012345678),
            .big = D38.fromUnscaled(99999999999999999999),
        },
        .{ .amount = null, .big = null },
        .{
            .amount = D18.fromUnscaled(-1),
            .big = D38.fromUnscaled(0),
        },
        .{
            .amount = D18.fromUnscaled(0),
            .big = null,
        },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{});
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var i: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        if (rows[i].amount) |expected| {
            try std.testing.expect(row.amount != null);
            try std.testing.expectEqualSlices(u8, &expected.bytes, &row.amount.?.bytes);
        } else {
            try std.testing.expect(row.amount == null);
        }
        if (rows[i].big) |expected| {
            try std.testing.expect(row.big != null);
            try std.testing.expectEqualSlices(u8, &expected.bytes, &row.big.?.bytes);
        } else {
            try std.testing.expect(row.big == null);
        }
        i += 1;
    }
    try std.testing.expectEqual(rows.len, i);
}

test "round-trip list of small int/float types ([]i16, []u16, []f32)" {
    const allocator = std.testing.allocator;

    const Row = struct {
        shorts: []const i16,
        ushorts: []const u16,
        floats: []const f32,
    };

    const s1 = [_]i16{ -32768, 0, 32767 };
    const s2 = [_]i16{42};
    const us1 = [_]u16{ 0, 65535 };
    const us2 = [_]u16{1000};
    const f1 = [_]f32{ 1.5, -2.5, 3.14 };
    const f2 = [_]f32{0.0};

    const rows = [_]Row{
        .{ .shorts = &s1, .ushorts = &us1, .floats = &f1 },
        .{ .shorts = &s2, .ushorts = &us2, .floats = &f2 },
        .{ .shorts = &[_]i16{}, .ushorts = &[_]u16{}, .floats = &[_]f32{} },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{});
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var i: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        try std.testing.expectEqual(rows[i].shorts.len, row.shorts.len);
        for (rows[i].shorts, 0..) |exp, j| try std.testing.expectEqual(exp, row.shorts[j]);
        try std.testing.expectEqual(rows[i].ushorts.len, row.ushorts.len);
        for (rows[i].ushorts, 0..) |exp, j| try std.testing.expectEqual(exp, row.ushorts[j]);
        try std.testing.expectEqual(rows[i].floats.len, row.floats.len);
        for (rows[i].floats, 0..) |exp, j| try std.testing.expectEqual(exp, row.floats[j]);
        i += 1;
    }
    try std.testing.expectEqual(rows.len, i);
}

test "round-trip list of Timestamp variants ([]Timestamp(millis/nanos))" {
    const allocator = std.testing.allocator;

    const TsMillis = parquet.Timestamp(.millis);
    const TsNanos = parquet.Timestamp(.nanos);

    const Row = struct {
        ts_ms: []const TsMillis,
        ts_ns: []const TsNanos,
    };

    const ms1 = [_]TsMillis{ TsMillis.fromMillis(1700000000_000), TsMillis.fromMillis(0) };
    const ms2 = [_]TsMillis{TsMillis.fromMillis(-86400_000)};
    const ns1 = [_]TsNanos{ TsNanos.fromNanos(1700000000_000000000), TsNanos.fromNanos(0) };
    const ns2 = [_]TsNanos{TsNanos.fromNanos(999999999)};

    const rows = [_]Row{
        .{ .ts_ms = &ms1, .ts_ns = &ns1 },
        .{ .ts_ms = &ms2, .ts_ns = &ns2 },
        .{ .ts_ms = &[_]TsMillis{}, .ts_ns = &[_]TsNanos{} },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{});
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var i: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        try std.testing.expectEqual(rows[i].ts_ms.len, row.ts_ms.len);
        for (rows[i].ts_ms, 0..) |exp, j| try std.testing.expectEqual(exp.value, row.ts_ms[j].value);
        try std.testing.expectEqual(rows[i].ts_ns.len, row.ts_ns.len);
        for (rows[i].ts_ns, 0..) |exp, j| try std.testing.expectEqual(exp.value, row.ts_ns[j].value);
        i += 1;
    }
    try std.testing.expectEqual(rows.len, i);
}

test "round-trip list of Time variants ([]Time(micros/millis/nanos))" {
    const allocator = std.testing.allocator;

    const TimeMicros = parquet.Time(.micros);
    const TimeMillis = parquet.Time(.millis);
    const TimeNanos = parquet.Time(.nanos);

    const Row = struct {
        t_us: []const TimeMicros,
        t_ms: []const TimeMillis,
        t_ns: []const TimeNanos,
    };

    const us1 = [_]TimeMicros{ TimeMicros.fromHms(10, 30, 0, 0), TimeMicros.fromHms(23, 59, 59, 999999) };
    const us2 = [_]TimeMicros{TimeMicros.fromHms(0, 0, 0, 0)};
    const ms1 = [_]TimeMillis{ TimeMillis.fromHms(12, 0, 0, 0), TimeMillis.fromHms(0, 0, 0, 500) };
    const ms2 = [_]TimeMillis{TimeMillis.fromHms(23, 59, 59, 999)};
    const ns1 = [_]TimeNanos{ TimeNanos.fromHms(8, 15, 30, 123456789), TimeNanos.fromHms(0, 0, 0, 0) };
    const ns2 = [_]TimeNanos{TimeNanos.fromHms(23, 59, 59, 999999999)};

    const rows = [_]Row{
        .{ .t_us = &us1, .t_ms = &ms1, .t_ns = &ns1 },
        .{ .t_us = &us2, .t_ms = &ms2, .t_ns = &ns2 },
        .{ .t_us = &[_]TimeMicros{}, .t_ms = &[_]TimeMillis{}, .t_ns = &[_]TimeNanos{} },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{});
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var i: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        try std.testing.expectEqual(rows[i].t_us.len, row.t_us.len);
        for (rows[i].t_us, 0..) |exp, j| try std.testing.expectEqual(exp.value, row.t_us[j].value);
        try std.testing.expectEqual(rows[i].t_ms.len, row.t_ms.len);
        for (rows[i].t_ms, 0..) |exp, j| try std.testing.expectEqual(exp.value, row.t_ms[j].value);
        try std.testing.expectEqual(rows[i].t_ns.len, row.t_ns.len);
        for (rows[i].t_ns, 0..) |exp, j| try std.testing.expectEqual(exp.value, row.t_ns[j].value);
        i += 1;
    }
    try std.testing.expectEqual(rows.len, i);
}

test "round-trip list of Decimal variants ([]Decimal(18,4), []Decimal(38,10))" {
    const allocator = std.testing.allocator;

    const D18 = parquet.Decimal(18, 4);
    const D38 = parquet.Decimal(38, 10);

    const Row = struct {
        amounts: []const D18,
        bigs: []const D38,
    };

    const a1 = [_]D18{ D18.fromUnscaled(123456789012345678), D18.fromUnscaled(-1) };
    const a2 = [_]D18{D18.fromUnscaled(0)};
    const b1 = [_]D38{ D38.fromUnscaled(99999999999999999999), D38.fromUnscaled(0) };
    const b2 = [_]D38{D38.fromUnscaled(-42)};

    const rows = [_]Row{
        .{ .amounts = &a1, .bigs = &b1 },
        .{ .amounts = &a2, .bigs = &b2 },
        .{ .amounts = &[_]D18{}, .bigs = &[_]D38{} },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{});
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var i: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        try std.testing.expectEqual(rows[i].amounts.len, row.amounts.len);
        for (rows[i].amounts, 0..) |exp, j|
            try std.testing.expectEqual(exp.toI128(), row.amounts[j].toI128());
        try std.testing.expectEqual(rows[i].bigs.len, row.bigs.len);
        for (rows[i].bigs, 0..) |exp, j|
            try std.testing.expectEqual(exp.toI128(), row.bigs[j].toI128());
        i += 1;
    }
    try std.testing.expectEqual(rows.len, i);
}

test "round-trip optional list of primitives (?[]i32, ?[]i64, ?[]f32, ?[]f64, ?[]bool)" {
    const allocator = std.testing.allocator;

    const Row = struct {
        ints: ?[]const i32,
        longs: ?[]const i64,
        floats: ?[]const f32,
        doubles: ?[]const f64,
        flags: ?[]const bool,
    };

    const ints1 = [_]i32{ 1, -2, 3 };
    const longs1 = [_]i64{ 100, -200 };
    const floats1 = [_]f32{ 1.5, -2.5 };
    const doubles1 = [_]f64{ 3.14, 2.71 };
    const bools1 = [_]bool{ true, false, true };

    const rows = [_]Row{
        .{ .ints = &ints1, .longs = &longs1, .floats = &floats1, .doubles = &doubles1, .flags = &bools1 },
        .{ .ints = null, .longs = null, .floats = null, .doubles = null, .flags = null },
        .{ .ints = &[_]i32{}, .longs = null, .floats = &[_]f32{0.0}, .doubles = null, .flags = &[_]bool{} },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{});
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var i: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        inline for (.{ "ints", "longs", "floats", "doubles", "flags" }) |field| {
            if (@field(rows[i], field)) |expected| {
                try std.testing.expect(@field(row, field) != null);
                try std.testing.expectEqual(expected.len, @field(row, field).?.len);
                for (expected, 0..) |exp, j| {
                    try std.testing.expectEqual(exp, @field(row, field).?[j]);
                }
            } else {
                try std.testing.expect(@field(row, field) == null);
            }
        }
        i += 1;
    }
    try std.testing.expectEqual(rows.len, i);
}

test "round-trip optional list of logical types (?[]Interval, ?[]Timestamp, ?[]Time, ?[]Decimal)" {
    const allocator = std.testing.allocator;

    const TsMicros = parquet.Timestamp(.micros);
    const TimeMicros = parquet.Time(.micros);
    const D = parquet.Decimal(9, 2);

    const Row = struct {
        spans: ?[]const parquet.Interval,
        timestamps: ?[]const TsMicros,
        times: ?[]const TimeMicros,
        amounts: ?[]const D,
    };

    const iv1 = [_]parquet.Interval{ parquet.Interval.fromDays(10), parquet.Interval.fromComponents(1, 2, 3000) };
    const ts1 = [_]TsMicros{ TsMicros.fromMicros(1700000000_000000), TsMicros.fromMicros(0) };
    const tm1 = [_]TimeMicros{ TimeMicros.fromHms(10, 30, 0, 0), TimeMicros.fromHms(23, 59, 59, 999999) };
    const d1 = [_]D{ D.fromUnscaled(100), D.fromUnscaled(-200) };

    const rows = [_]Row{
        .{ .spans = &iv1, .timestamps = &ts1, .times = &tm1, .amounts = &d1 },
        .{ .spans = null, .timestamps = null, .times = null, .amounts = null },
        .{ .spans = &[_]parquet.Interval{}, .timestamps = &[_]TsMicros{}, .times = null, .amounts = &[_]D{D.fromUnscaled(0)} },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{});
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var i: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        if (rows[i].spans) |expected| {
            try std.testing.expect(row.spans != null);
            try std.testing.expectEqual(expected.len, row.spans.?.len);
            for (expected, 0..) |exp, j|
                try std.testing.expectEqualSlices(u8, &exp.toBytes(), &row.spans.?[j].toBytes());
        } else {
            try std.testing.expect(row.spans == null);
        }
        if (rows[i].timestamps) |expected| {
            try std.testing.expect(row.timestamps != null);
            try std.testing.expectEqual(expected.len, row.timestamps.?.len);
            for (expected, 0..) |exp, j|
                try std.testing.expectEqual(exp.value, row.timestamps.?[j].value);
        } else {
            try std.testing.expect(row.timestamps == null);
        }
        if (rows[i].times) |expected| {
            try std.testing.expect(row.times != null);
            try std.testing.expectEqual(expected.len, row.times.?.len);
            for (expected, 0..) |exp, j|
                try std.testing.expectEqual(exp.value, row.times.?[j].value);
        } else {
            try std.testing.expect(row.times == null);
        }
        if (rows[i].amounts) |expected| {
            try std.testing.expect(row.amounts != null);
            try std.testing.expectEqual(expected.len, row.amounts.?.len);
            for (expected, 0..) |exp, j|
                try std.testing.expectEqual(exp.toI128(), row.amounts.?[j].toI128());
        } else {
            try std.testing.expect(row.amounts == null);
        }
        i += 1;
    }
    try std.testing.expectEqual(rows.len, i);
}

test "round-trip list of optional primitives ([]?i32, []?i64, []?f32, []?f64, []?bool)" {
    const allocator = std.testing.allocator;

    const Row = struct {
        ints: []const ?i32,
        longs: []const ?i64,
        floats: []const ?f32,
        doubles: []const ?f64,
        flags: []const ?bool,
    };

    const ints1 = [_]?i32{ 1, null, -3 };
    const longs1 = [_]?i64{ 100, null };
    const floats1 = [_]?f32{ 1.5, null, -2.5 };
    const doubles1 = [_]?f64{ 3.14, null };
    const bools1 = [_]?bool{ true, null, false };

    const ints2 = [_]?i32{null};
    const longs2 = [_]?i64{42};
    const floats2 = [_]?f32{null};
    const doubles2 = [_]?f64{2.71};
    const bools2 = [_]?bool{null};

    const rows = [_]Row{
        .{ .ints = &ints1, .longs = &longs1, .floats = &floats1, .doubles = &doubles1, .flags = &bools1 },
        .{ .ints = &ints2, .longs = &longs2, .floats = &floats2, .doubles = &doubles2, .flags = &bools2 },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{});
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var i: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        inline for (.{ "ints", "longs", "floats", "doubles", "flags" }) |field| {
            const expected = @field(rows[i], field);
            const actual = @field(row, field);
            try std.testing.expectEqual(expected.len, actual.len);
            for (expected, 0..) |exp, j| {
                try std.testing.expectEqual(exp, actual[j]);
            }
        }
        i += 1;
    }
    try std.testing.expectEqual(rows.len, i);
}

test "round-trip list of optional logical types ([]?Timestamp, []?Time, []?Decimal)" {
    const allocator = std.testing.allocator;

    const TsMicros = parquet.Timestamp(.micros);
    const TimeMicros = parquet.Time(.micros);
    const D = parquet.Decimal(9, 2);

    const Row = struct {
        timestamps: []const ?TsMicros,
        times: []const ?TimeMicros,
        amounts: []const ?D,
    };

    const ts1 = [_]?TsMicros{ TsMicros.fromMicros(1700000000_000000), null, TsMicros.fromMicros(0) };
    const tm1 = [_]?TimeMicros{ null, TimeMicros.fromHms(12, 0, 0, 0) };
    const d1 = [_]?D{ D.fromUnscaled(100), null, D.fromUnscaled(-99) };

    const ts2 = [_]?TsMicros{null};
    const tm2 = [_]?TimeMicros{TimeMicros.fromHms(0, 0, 0, 0)};
    const d2 = [_]?D{null};

    const rows = [_]Row{
        .{ .timestamps = &ts1, .times = &tm1, .amounts = &d1 },
        .{ .timestamps = &ts2, .times = &tm2, .amounts = &d2 },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{});
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var i: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        {
            const expected = rows[i].timestamps;
            try std.testing.expectEqual(expected.len, row.timestamps.len);
            for (expected, 0..) |exp, j| {
                if (exp) |e| {
                    try std.testing.expect(row.timestamps[j] != null);
                    try std.testing.expectEqual(e.value, row.timestamps[j].?.value);
                } else {
                    try std.testing.expect(row.timestamps[j] == null);
                }
            }
        }
        {
            const expected = rows[i].times;
            try std.testing.expectEqual(expected.len, row.times.len);
            for (expected, 0..) |exp, j| {
                if (exp) |e| {
                    try std.testing.expect(row.times[j] != null);
                    try std.testing.expectEqual(e.value, row.times[j].?.value);
                } else {
                    try std.testing.expect(row.times[j] == null);
                }
            }
        }
        {
            const expected = rows[i].amounts;
            try std.testing.expectEqual(expected.len, row.amounts.len);
            for (expected, 0..) |exp, j| {
                if (exp) |e| {
                    try std.testing.expect(row.amounts[j] != null);
                    try std.testing.expectEqual(e.toI128(), row.amounts[j].?.toI128());
                } else {
                    try std.testing.expect(row.amounts[j] == null);
                }
            }
        }
        i += 1;
    }
    try std.testing.expectEqual(rows.len, i);
}

test "round-trip nested list of primitives ([][]i32, [][]i64, [][]f32, [][]f64, [][]bool)" {
    const allocator = std.testing.allocator;

    const Row = struct {
        ints: []const []const i32,
        longs: []const []const i64,
        floats: []const []const f32,
        doubles: []const []const f64,
        flags: []const []const bool,
    };

    const ia = [_]i32{ 1, -2, 3 };
    const ib = [_]i32{42};
    const la = [_]i64{ 100, -200 };
    const lb = [_]i64{0};
    const fa = [_]f32{ 1.5, -2.5 };
    const fb = [_]f32{3.14};
    const da = [_]f64{ 2.71, 0.0 };
    const db = [_]f64{-1.0};
    const ba = [_]bool{ true, false };
    const bb = [_]bool{true};

    const oi = [_][]const i32{ &ia, &ib };
    const ol = [_][]const i64{ &la, &lb };
    const of_ = [_][]const f32{ &fa, &fb };
    const od = [_][]const f64{ &da, &db };
    const ob = [_][]const bool{ &ba, &bb };

    const oi2 = [_][]const i32{&[_]i32{}};
    const ol2 = [_][]const i64{&[_]i64{}};
    const of2 = [_][]const f32{&[_]f32{}};
    const od2 = [_][]const f64{&[_]f64{}};
    const ob2 = [_][]const bool{&[_]bool{}};

    const rows = [_]Row{
        .{ .ints = &oi, .longs = &ol, .floats = &of_, .doubles = &od, .flags = &ob },
        .{ .ints = &oi2, .longs = &ol2, .floats = &of2, .doubles = &od2, .flags = &ob2 },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{ .compression = .uncompressed });
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var idx: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        inline for (.{ "ints", "longs", "floats", "doubles", "flags" }) |field| {
            const expected = @field(rows[idx], field);
            const actual = @field(row, field);
            try std.testing.expectEqual(expected.len, actual.len);
            for (expected, 0..) |exp_inner, outer| {
                try std.testing.expectEqual(exp_inner.len, actual[outer].len);
                for (exp_inner, 0..) |exp_val, inner| {
                    try std.testing.expectEqual(exp_val, actual[outer][inner]);
                }
            }
        }
        idx += 1;
    }
    try std.testing.expectEqual(rows.len, idx);
}

test "round-trip nested list of strings and logical types ([][]string, [][]Date, [][]Timestamp, [][]Time)" {
    const allocator = std.testing.allocator;

    const TsMicros = parquet.Timestamp(.micros);
    const TimeMicros = parquet.Time(.micros);

    const Row = struct {
        words: []const []const []const u8,
        dates: []const []const parquet.Date,
        timestamps: []const []const TsMicros,
        times: []const []const TimeMicros,
    };

    const wa = [_][]const u8{ "hello", "world" };
    const wb = [_][]const u8{"test"};
    const ow = [_][]const []const u8{ &wa, &wb };
    const ow2 = [_][]const []const u8{&[_][]const u8{}};

    const d_a = [_]parquet.Date{ parquet.Date.fromDays(18000), parquet.Date.fromDays(19000) };
    const d_b = [_]parquet.Date{parquet.Date.fromDays(0)};
    const outer_d = [_][]const parquet.Date{ &d_a, &d_b };
    const outer_d2 = [_][]const parquet.Date{&[_]parquet.Date{}};

    const tsa = [_]TsMicros{ TsMicros.fromMicros(1700000000_000000), TsMicros.fromMicros(0) };
    const tsb = [_]TsMicros{TsMicros.fromMicros(-86400_000000)};
    const ots = [_][]const TsMicros{ &tsa, &tsb };
    const ots2 = [_][]const TsMicros{&[_]TsMicros{}};

    const tma = [_]TimeMicros{ TimeMicros.fromHms(10, 30, 0, 0), TimeMicros.fromHms(23, 59, 59, 999999) };
    const tmb = [_]TimeMicros{TimeMicros.fromHms(0, 0, 0, 0)};
    const otm = [_][]const TimeMicros{ &tma, &tmb };
    const otm2 = [_][]const TimeMicros{&[_]TimeMicros{}};

    const rows = [_]Row{
        .{ .words = &ow, .dates = &outer_d, .timestamps = &ots, .times = &otm },
        .{ .words = &ow2, .dates = &outer_d2, .timestamps = &ots2, .times = &otm2 },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{ .compression = .uncompressed });
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var idx: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        {
            const expected = rows[idx].words;
            try std.testing.expectEqual(expected.len, row.words.len);
            for (expected, 0..) |exp_inner, outer| {
                try std.testing.expectEqual(exp_inner.len, row.words[outer].len);
                for (exp_inner, 0..) |exp_str, inner| {
                    try std.testing.expectEqualStrings(exp_str, row.words[outer][inner]);
                }
            }
        }
        {
            const expected = rows[idx].dates;
            try std.testing.expectEqual(expected.len, row.dates.len);
            for (expected, 0..) |exp_inner, outer| {
                try std.testing.expectEqual(exp_inner.len, row.dates[outer].len);
                for (exp_inner, 0..) |exp_val, inner| {
                    try std.testing.expectEqual(exp_val.days, row.dates[outer][inner].days);
                }
            }
        }
        {
            const expected = rows[idx].timestamps;
            try std.testing.expectEqual(expected.len, row.timestamps.len);
            for (expected, 0..) |exp_inner, outer| {
                try std.testing.expectEqual(exp_inner.len, row.timestamps[outer].len);
                for (exp_inner, 0..) |exp_val, inner| {
                    try std.testing.expectEqual(exp_val.value, row.timestamps[outer][inner].value);
                }
            }
        }
        {
            const expected = rows[idx].times;
            try std.testing.expectEqual(expected.len, row.times.len);
            for (expected, 0..) |exp_inner, outer| {
                try std.testing.expectEqual(exp_inner.len, row.times[outer].len);
                for (exp_inner, 0..) |exp_val, inner| {
                    try std.testing.expectEqual(exp_val.value, row.times[outer][inner].value);
                }
            }
        }
        idx += 1;
    }
    try std.testing.expectEqual(rows.len, idx);
}

test "round-trip nested list of Decimal ([][]Decimal(9,2))" {
    const allocator = std.testing.allocator;

    const D = parquet.Decimal(9, 2);

    const Row = struct {
        amounts: []const []const D,
    };

    const inner_a = [_]D{ D.fromUnscaled(100), D.fromUnscaled(-200), D.fromUnscaled(0) };
    const inner_b = [_]D{D.fromUnscaled(99999)};

    const outer1 = [_][]const D{ &inner_a, &inner_b };
    const outer2 = [_][]const D{&[_]D{}};

    const rows = [_]Row{
        .{ .amounts = &outer1 },
        .{ .amounts = &outer2 },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{ .compression = .uncompressed });
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var idx: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        const expected = rows[idx].amounts;
        try std.testing.expectEqual(expected.len, row.amounts.len);
        for (expected, 0..) |exp_inner, outer| {
            try std.testing.expectEqual(exp_inner.len, row.amounts[outer].len);
            for (exp_inner, 0..) |exp_val, inner| {
                try std.testing.expectEqual(exp_val.toI128(), row.amounts[outer][inner].toI128());
            }
        }
        idx += 1;
    }
    try std.testing.expectEqual(rows.len, idx);
}

test "round-trip struct with bool and f32 fields" {
    const allocator = std.testing.allocator;

    const Inner = struct {
        flag: bool,
        score: f32,
        tag: i32,
    };
    const Row = struct {
        id: i32,
        data: Inner,
    };

    const rows = [_]Row{
        .{ .id = 1, .data = .{ .flag = true, .score = 3.14, .tag = 10 } },
        .{ .id = 2, .data = .{ .flag = false, .score = -0.5, .tag = 20 } },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{ .compression = .uncompressed });
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var idx: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        try std.testing.expectEqual(rows[idx].id, row.id);
        try std.testing.expectEqual(rows[idx].data.flag, row.data.flag);
        try std.testing.expectEqual(rows[idx].data.score, row.data.score);
        try std.testing.expectEqual(rows[idx].data.tag, row.data.tag);
        idx += 1;
    }
    try std.testing.expectEqual(rows.len, idx);
}

test "round-trip struct with small integer fields (i8, i16, u8, u16, u32, u64)" {
    const allocator = std.testing.allocator;

    const SmallInts = struct {
        a: i8,
        b: i16,
        c: u8,
        d: u16,
        e: u32,
        f_val: u64,
    };
    const Row = struct {
        id: i32,
        nums: SmallInts,
    };

    const rows = [_]Row{
        .{ .id = 1, .nums = .{ .a = -128, .b = -32000, .c = 255, .d = 65535, .e = 4000000000, .f_val = 18000000000000000000 } },
        .{ .id = 2, .nums = .{ .a = 0, .b = 0, .c = 0, .d = 0, .e = 0, .f_val = 0 } },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{ .compression = .uncompressed });
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var idx: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        try std.testing.expectEqual(rows[idx].id, row.id);
        try std.testing.expectEqual(rows[idx].nums.a, row.nums.a);
        try std.testing.expectEqual(rows[idx].nums.b, row.nums.b);
        try std.testing.expectEqual(rows[idx].nums.c, row.nums.c);
        try std.testing.expectEqual(rows[idx].nums.d, row.nums.d);
        try std.testing.expectEqual(rows[idx].nums.e, row.nums.e);
        try std.testing.expectEqual(rows[idx].nums.f_val, row.nums.f_val);
        idx += 1;
    }
    try std.testing.expectEqual(rows.len, idx);
}

test "round-trip struct with Decimal(9,2) field" {
    const allocator = std.testing.allocator;

    const D = parquet.Decimal(9, 2);

    const Amount = struct {
        label: i32,
        value: D,
    };
    const Row = struct {
        id: i32,
        amount: Amount,
    };

    const rows = [_]Row{
        .{ .id = 1, .amount = .{ .label = 100, .value = D.fromUnscaled(12345) } },
        .{ .id = 2, .amount = .{ .label = 200, .value = D.fromUnscaled(-99999) } },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{ .compression = .uncompressed });
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var idx: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        try std.testing.expectEqual(rows[idx].id, row.id);
        try std.testing.expectEqual(rows[idx].amount.label, row.amount.label);
        try std.testing.expectEqual(rows[idx].amount.value.toI128(), row.amount.value.toI128());
        idx += 1;
    }
    try std.testing.expectEqual(rows.len, idx);
}

test "round-trip flat f16 column" {
    const allocator = std.testing.allocator;

    const Row = struct {
        val: f16,
    };

    const rows = [_]Row{
        .{ .val = 1.0 },
        .{ .val = -2.25 },
        .{ .val = 0.0 },
        .{ .val = 65504.0 },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{ .compression = .uncompressed });
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var idx: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        try std.testing.expectEqual(rows[idx].val, row.val);
        idx += 1;
    }
    try std.testing.expectEqual(rows.len, idx);
}

test "round-trip optional f16 column (?f16)" {
    const allocator = std.testing.allocator;

    const Row = struct {
        val: ?f16,
    };

    const rows = [_]Row{
        .{ .val = 1.5 },
        .{ .val = null },
        .{ .val = -0.5 },
        .{ .val = null },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{ .compression = .uncompressed });
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var idx: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        try std.testing.expectEqual(rows[idx].val, row.val);
        idx += 1;
    }
    try std.testing.expectEqual(rows.len, idx);
}

test "round-trip list of f16 ([]f16)" {
    const allocator = std.testing.allocator;

    const Row = struct {
        vals: []const f16,
    };

    const list_a = [_]f16{ 1.0, -2.25, 0.0 };
    const list_b = [_]f16{65504.0};
    const list_c = [_]f16{};

    const rows = [_]Row{
        .{ .vals = &list_a },
        .{ .vals = &list_b },
        .{ .vals = &list_c },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{ .compression = .uncompressed });
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var idx: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        try std.testing.expectEqual(rows[idx].vals.len, row.vals.len);
        for (rows[idx].vals, 0..) |exp, j| {
            try std.testing.expectEqual(exp, row.vals[j]);
        }
        idx += 1;
    }
    try std.testing.expectEqual(rows.len, idx);
}

test "round-trip struct with f16 field" {
    const allocator = std.testing.allocator;

    const Inner = struct {
        half: f16,
        tag: i32,
    };
    const Row = struct {
        id: i32,
        data: Inner,
    };

    const rows = [_]Row{
        .{ .id = 1, .data = .{ .half = 3.14, .tag = 10 } },
        .{ .id = 2, .data = .{ .half = -0.5, .tag = 20 } },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{ .compression = .uncompressed });
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var idx: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        try std.testing.expectEqual(rows[idx].id, row.id);
        try std.testing.expectEqual(rows[idx].data.half, row.data.half);
        try std.testing.expectEqual(rows[idx].data.tag, row.data.tag);
        idx += 1;
    }
    try std.testing.expectEqual(rows.len, idx);
}

test "round-trip statistics int32 logical types" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const i8_vals = [_]i32{ 5, -3, 127, -128, 0 };
    const i16_vals = [_]i32{ 1000, -500, 32767, -32768, 0 };
    const u8_vals = [_]i32{ 0, 100, 255, 50, 200 };
    const u16_vals = [_]i32{ 0, 30000, 65535, 100, 50000 };
    const u32_vals = [_]i32{ 0, 1000, 2000000, 500, 1500000 };
    const date_vals = [_]i32{ 18000, 19000, 17500, 19500, 18500 };
    const time_ms_vals = [_]i32{ 0, 43200000, 86399999, 1000, 60000 };
    const dec9_vals = [_]i32{ 123456789, -999999999, 0, 500000000, -100 };

    // Write
    {
        const file = try tmp_dir.dir.createFile("stats_int32_logical.parquet", .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            parquet.ColumnDef.int8("i8_col", false),
            parquet.ColumnDef.int16("i16_col", false),
            parquet.ColumnDef.uint8("u8_col", false),
            parquet.ColumnDef.uint16("u16_col", false),
            parquet.ColumnDef.uint32("u32_col", false),
            parquet.ColumnDef.date("date_col", false),
            parquet.ColumnDef.time("time_ms_col", .millis, false, false),
            parquet.ColumnDef.decimal("dec9_col", 9, 2, false),
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        try writer.writeColumn(i32, 0, &i8_vals);
        try writer.writeColumn(i32, 1, &i16_vals);
        try writer.writeColumn(i32, 2, &u8_vals);
        try writer.writeColumn(i32, 3, &u16_vals);
        try writer.writeColumn(i32, 4, &u32_vals);
        try writer.writeColumn(i32, 5, &date_vals);
        try writer.writeColumn(i32, 6, &time_ms_vals);
        try writer.writeColumn(i32, 7, &dec9_vals);
        try writer.close();
    }

    // Read and verify statistics
    {
        const file = try tmp_dir.dir.openFile("stats_int32_logical.parquet", .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        const Expectation = struct { col: usize, min: i32, max: i32 };
        const expectations = [_]Expectation{
            .{ .col = 0, .min = -128, .max = 127 },
            .{ .col = 1, .min = -32768, .max = 32767 },
            .{ .col = 2, .min = 0, .max = 255 },
            .{ .col = 3, .min = 0, .max = 65535 },
            .{ .col = 4, .min = 0, .max = 2000000 },
            .{ .col = 5, .min = 17500, .max = 19500 },
            .{ .col = 6, .min = 0, .max = 86399999 },
            .{ .col = 7, .min = -999999999, .max = 500000000 },
        };

        for (expectations) |exp| {
            const stats = reader.getColumnStatistics(exp.col, 0);
            try std.testing.expect(stats != null);
            const s = stats.?;
            try std.testing.expectEqual(exp.min, s.minAsI32().?);
            try std.testing.expectEqual(exp.max, s.maxAsI32().?);
            try std.testing.expectEqual(@as(i64, 0), s.null_count.?);
        }
    }
}

test "round-trip statistics int64 logical types" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const u64_vals = [_]i64{ 0, 1000000, 4294967296, 100, 9999999999 };
    const ts_us_vals = [_]i64{ 1700000000000000, 1600000000000000, 1800000000000000, 1650000000000000, 1750000000000000 };
    const ts_ms_vals = [_]i64{ 1700000000000, 1600000000000, 1800000000000, 1650000000000, 1750000000000 };
    const ts_ns_vals = [_]i64{ 1700000000000000000, 1600000000000000000, 1800000000000000000, 1650000000000000000, 1750000000000000000 };
    const time_us_vals = [_]i64{ 0, 43200000000, 86399999999, 1000000, 60000000 };
    const time_ns_vals = [_]i64{ 0, 43200000000000, 86399999999999, 1000000000, 60000000000 };
    const dec18_vals = [_]i64{ 1234567890123456, -9999999999999999, 0, 5000000000000000, -100 };

    // Write
    {
        const file = try tmp_dir.dir.createFile("stats_int64_logical.parquet", .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            parquet.ColumnDef.uint64("u64_col", false),
            parquet.ColumnDef.timestamp("ts_us_col", .micros, true, false),
            parquet.ColumnDef.timestamp("ts_ms_col", .millis, true, false),
            parquet.ColumnDef.timestamp("ts_ns_col", .nanos, true, false),
            parquet.ColumnDef.time("time_us_col", .micros, false, false),
            parquet.ColumnDef.time("time_ns_col", .nanos, false, false),
            parquet.ColumnDef.decimal("dec18_col", 18, 4, false),
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        try writer.writeColumn(i64, 0, &u64_vals);
        try writer.writeColumn(i64, 1, &ts_us_vals);
        try writer.writeColumn(i64, 2, &ts_ms_vals);
        try writer.writeColumn(i64, 3, &ts_ns_vals);
        try writer.writeColumn(i64, 4, &time_us_vals);
        try writer.writeColumn(i64, 5, &time_ns_vals);
        try writer.writeColumn(i64, 6, &dec18_vals);
        try writer.close();
    }

    // Read and verify statistics
    {
        const file = try tmp_dir.dir.openFile("stats_int64_logical.parquet", .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        const Expectation = struct { col: usize, min: i64, max: i64 };
        const expectations = [_]Expectation{
            .{ .col = 0, .min = 0, .max = 9999999999 },
            .{ .col = 1, .min = 1600000000000000, .max = 1800000000000000 },
            .{ .col = 2, .min = 1600000000000, .max = 1800000000000 },
            .{ .col = 3, .min = 1600000000000000000, .max = 1800000000000000000 },
            .{ .col = 4, .min = 0, .max = 86399999999 },
            .{ .col = 5, .min = 0, .max = 86399999999999 },
            .{ .col = 6, .min = -9999999999999999, .max = 5000000000000000 },
        };

        for (expectations) |exp| {
            const stats = reader.getColumnStatistics(exp.col, 0);
            try std.testing.expect(stats != null);
            const s = stats.?;
            try std.testing.expectEqual(exp.min, s.minAsI64().?);
            try std.testing.expectEqual(exp.max, s.maxAsI64().?);
            try std.testing.expectEqual(@as(i64, 0), s.null_count.?);
        }
    }
}

test "round-trip statistics fixed byte array logical types" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    // UUID: 16-byte values, lexicographic byte comparison
    const uuid_a = [_]u8{ 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF };
    const uuid_b = [_]u8{ 0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00 };
    const uuid_c = [_]u8{ 0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90, 0xA0, 0xB0, 0xC0, 0xD0, 0xE0, 0xF0, 0x01 };
    const uuid_values = [_][]const u8{ &uuid_a, &uuid_b, &uuid_c };
    const uuid_expected_min = uuid_a; // 0x00... is lexicographically smallest
    const uuid_expected_max = uuid_b; // 0xFF... is lexicographically largest

    // f16: 2-byte IEEE 754 half-precision (little-endian), positive values only
    const f16_half: f16 = 0.5;
    const f16_one: f16 = 1.0;
    const f16_three: f16 = 3.0;
    const f16_bytes_half: [2]u8 = @bitCast(f16_half);
    const f16_bytes_one: [2]u8 = @bitCast(f16_one);
    const f16_bytes_three: [2]u8 = @bitCast(f16_three);
    const f16_values = [_][]const u8{ &f16_bytes_one, &f16_bytes_half, &f16_bytes_three };

    // Decimal(38,10): 20-byte big-endian two's complement (precision 38 -> (38+1)/2 = 19... let's check)
    // precision=38, byte_len = @divTrunc(38+1, 2) = 19
    const dec_small = [_]u8{0} ** 18 ++ [_]u8{1}; // value = 1
    const dec_medium = [_]u8{0} ** 18 ++ [_]u8{100}; // value = 100
    const dec_large = [_]u8{0} ** 17 ++ [_]u8{ 1, 0 }; // value = 256
    const dec_values = [_][]const u8{ &dec_medium, &dec_small, &dec_large };
    const dec_expected_min = dec_small; // 0x00..01
    const dec_expected_max = dec_large; // 0x00..0100

    // Write
    {
        const file = try tmp_dir.dir.createFile("stats_fixed_logical.parquet", .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            parquet.ColumnDef.uuid("uuid_col", false),
            parquet.ColumnDef.float16("f16_col", false),
            parquet.ColumnDef.decimal("dec38_col", 38, 10, false),
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        try writer.writeColumnFixedByteArray(0, &uuid_values);
        try writer.writeColumnFixedByteArray(1, &f16_values);
        try writer.writeColumnFixedByteArray(2, &dec_values);
        try writer.close();
    }

    // Read and verify statistics
    {
        const file = try tmp_dir.dir.openFile("stats_fixed_logical.parquet", .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        // UUID stats
        {
            const stats = reader.getColumnStatistics(0, 0);
            try std.testing.expect(stats != null);
            const s = stats.?;
            try std.testing.expectEqualSlices(u8, &uuid_expected_min, s.getMinBytes().?);
            try std.testing.expectEqualSlices(u8, &uuid_expected_max, s.getMaxBytes().?);
            try std.testing.expectEqual(@as(i64, 0), s.null_count.?);
        }

        // f16 stats
        {
            const stats = reader.getColumnStatistics(1, 0);
            try std.testing.expect(stats != null);
            const s = stats.?;
            const min_bytes = s.getMinBytes().?;
            const max_bytes = s.getMaxBytes().?;
            try std.testing.expect(min_bytes.len >= 2);
            try std.testing.expect(max_bytes.len >= 2);
            try std.testing.expectEqual(@as(i64, 0), s.null_count.?);
        }

        // Decimal(38,10) stats
        {
            const stats = reader.getColumnStatistics(2, 0);
            try std.testing.expect(stats != null);
            const s = stats.?;
            try std.testing.expectEqualSlices(u8, &dec_expected_min, s.getMinBytes().?);
            try std.testing.expectEqualSlices(u8, &dec_expected_max, s.getMaxBytes().?);
            try std.testing.expectEqual(@as(i64, 0), s.null_count.?);
        }
    }
}

test "round-trip map with non-standard key types" {
    const allocator = std.testing.allocator;

    const ME_bool = parquet.MapEntry(bool, i32);
    const ME_i64 = parquet.MapEntry(i64, i32);
    const ME_f32 = parquet.MapEntry(f32, i32);
    const ME_f64 = parquet.MapEntry(f64, i32);

    const Row = struct {
        bool_map: ?[]const ME_bool,
        i64_map: ?[]const ME_i64,
        f32_map: ?[]const ME_f32,
        f64_map: ?[]const ME_f64,
    };

    const bool_entries = [_]ME_bool{
        .{ .key = true, .value = 1 },
        .{ .key = false, .value = null },
    };
    const i64_entries = [_]ME_i64{
        .{ .key = 100000000000, .value = 10 },
        .{ .key = -999, .value = 20 },
    };
    const f32_entries = [_]ME_f32{
        .{ .key = 1.5, .value = 100 },
        .{ .key = -3.25, .value = null },
    };
    const f64_entries = [_]ME_f64{
        .{ .key = 99.99, .value = 42 },
    };
    const empty_bool = [_]ME_bool{};

    const rows = [_]Row{
        .{ .bool_map = &bool_entries, .i64_map = &i64_entries, .f32_map = &f32_entries, .f64_map = &f64_entries },
        .{ .bool_map = &empty_bool, .i64_map = null, .f32_map = null, .f64_map = null },
        .{ .bool_map = null, .i64_map = null, .f32_map = null, .f64_map = null },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{
        .compression = .uncompressed,
        .use_dictionary = false,
    });
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 3), reader.rowCount());

    // Row 0: all populated
    const row0 = (try reader.next()).?;
    defer reader.freeRow(&row0);

    try std.testing.expect(row0.bool_map != null);
    try std.testing.expectEqual(@as(usize, 2), row0.bool_map.?.len);
    try std.testing.expectEqual(true, row0.bool_map.?[0].key);
    try std.testing.expectEqual(@as(?i32, 1), row0.bool_map.?[0].value);
    try std.testing.expectEqual(false, row0.bool_map.?[1].key);
    try std.testing.expectEqual(@as(?i32, null), row0.bool_map.?[1].value);

    try std.testing.expect(row0.i64_map != null);
    try std.testing.expectEqual(@as(usize, 2), row0.i64_map.?.len);
    try std.testing.expectEqual(@as(i64, 100000000000), row0.i64_map.?[0].key);
    try std.testing.expectEqual(@as(?i32, 10), row0.i64_map.?[0].value);
    try std.testing.expectEqual(@as(i64, -999), row0.i64_map.?[1].key);
    try std.testing.expectEqual(@as(?i32, 20), row0.i64_map.?[1].value);

    try std.testing.expect(row0.f32_map != null);
    try std.testing.expectEqual(@as(usize, 2), row0.f32_map.?.len);
    try std.testing.expectEqual(@as(f32, 1.5), row0.f32_map.?[0].key);
    try std.testing.expectEqual(@as(?i32, 100), row0.f32_map.?[0].value);
    try std.testing.expectEqual(@as(f32, -3.25), row0.f32_map.?[1].key);
    try std.testing.expectEqual(@as(?i32, null), row0.f32_map.?[1].value);

    try std.testing.expect(row0.f64_map != null);
    try std.testing.expectEqual(@as(usize, 1), row0.f64_map.?.len);
    try std.testing.expectEqual(@as(f64, 99.99), row0.f64_map.?[0].key);
    try std.testing.expectEqual(@as(?i32, 42), row0.f64_map.?[0].value);

    // Row 1: bool_map empty, rest null
    const row1 = (try reader.next()).?;
    defer reader.freeRow(&row1);
    try std.testing.expect(row1.bool_map != null);
    try std.testing.expectEqual(@as(usize, 0), row1.bool_map.?.len);
    try std.testing.expect(row1.i64_map == null);
    try std.testing.expect(row1.f32_map == null);
    try std.testing.expect(row1.f64_map == null);

    // Row 2: all null
    const row2 = (try reader.next()).?;
    defer reader.freeRow(&row2);
    try std.testing.expect(row2.bool_map == null);
    try std.testing.expect(row2.i64_map == null);
    try std.testing.expect(row2.f32_map == null);
    try std.testing.expect(row2.f64_map == null);
}

test "round-trip map with temporal value types" {
    const allocator = std.testing.allocator;

    const Date = parquet.Date;
    const Timestamp = parquet.Timestamp(.micros);
    const Time = parquet.Time(.micros);

    const ME_date = parquet.MapEntry(i32, Date);
    const ME_ts = parquet.MapEntry(i32, Timestamp);
    const ME_time = parquet.MapEntry(i32, Time);

    const Row = struct {
        date_map: ?[]const ME_date,
        ts_map: ?[]const ME_ts,
        time_map: ?[]const ME_time,
    };

    const date_entries = [_]ME_date{
        .{ .key = 1, .value = Date{ .days = 18000 } },
        .{ .key = 2, .value = null },
        .{ .key = 3, .value = Date{ .days = 19500 } },
    };
    const ts_entries = [_]ME_ts{
        .{ .key = 10, .value = Timestamp{ .value = 1700000000000000 } },
        .{ .key = 20, .value = null },
    };
    const time_entries = [_]ME_time{
        .{ .key = 100, .value = Time{ .value = 43200000000 } },
    };

    const rows = [_]Row{
        .{ .date_map = &date_entries, .ts_map = &ts_entries, .time_map = &time_entries },
        .{ .date_map = null, .ts_map = null, .time_map = null },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{
        .compression = .uncompressed,
        .use_dictionary = false,
    });
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 2), reader.rowCount());

    // Row 0: all populated
    const row0 = (try reader.next()).?;
    defer reader.freeRow(&row0);

    try std.testing.expect(row0.date_map != null);
    try std.testing.expectEqual(@as(usize, 3), row0.date_map.?.len);
    try std.testing.expectEqual(@as(i32, 1), row0.date_map.?[0].key);
    try std.testing.expectEqual(@as(i32, 18000), row0.date_map.?[0].value.?.days);
    try std.testing.expectEqual(@as(i32, 2), row0.date_map.?[1].key);
    try std.testing.expect(row0.date_map.?[1].value == null);
    try std.testing.expectEqual(@as(i32, 3), row0.date_map.?[2].key);
    try std.testing.expectEqual(@as(i32, 19500), row0.date_map.?[2].value.?.days);

    try std.testing.expect(row0.ts_map != null);
    try std.testing.expectEqual(@as(usize, 2), row0.ts_map.?.len);
    try std.testing.expectEqual(@as(i32, 10), row0.ts_map.?[0].key);
    try std.testing.expectEqual(@as(i64, 1700000000000000), row0.ts_map.?[0].value.?.value);
    try std.testing.expectEqual(@as(i32, 20), row0.ts_map.?[1].key);
    try std.testing.expect(row0.ts_map.?[1].value == null);

    try std.testing.expect(row0.time_map != null);
    try std.testing.expectEqual(@as(usize, 1), row0.time_map.?.len);
    try std.testing.expectEqual(@as(i32, 100), row0.time_map.?[0].key);
    try std.testing.expectEqual(@as(i64, 43200000000), row0.time_map.?[0].value.?.value);

    // Row 1: all null
    const row1 = (try reader.next()).?;
    defer reader.freeRow(&row1);
    try std.testing.expect(row1.date_map == null);
    try std.testing.expect(row1.ts_map == null);
    try std.testing.expect(row1.time_map == null);
}

test "round-trip map with Uuid, Decimal, Interval values" {
    const allocator = std.testing.allocator;

    const Uuid = parquet.Uuid;
    const D = parquet.Decimal(9, 2);
    const Interval = parquet.Interval;

    const ME_uuid = parquet.MapEntry(i32, Uuid);
    const ME_dec = parquet.MapEntry(i32, D);
    const ME_interval = parquet.MapEntry(i32, Interval);

    const Row = struct {
        uuid_map: ?[]const ME_uuid,
        dec_map: ?[]const ME_dec,
        interval_map: ?[]const ME_interval,
    };

    const uuid_a = Uuid{ .bytes = .{ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10 } };
    const uuid_b = Uuid{ .bytes = .{ 0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00 } };

    const uuid_entries = [_]ME_uuid{
        .{ .key = 1, .value = uuid_a },
        .{ .key = 2, .value = null },
        .{ .key = 3, .value = uuid_b },
    };
    const dec_entries = [_]ME_dec{
        .{ .key = 10, .value = D.fromUnscaled(12345) },
        .{ .key = 20, .value = D.fromUnscaled(-99999) },
    };
    const interval_entries = [_]ME_interval{
        .{ .key = 100, .value = Interval{ .months = 12, .days = 30, .millis = 3600000 } },
        .{ .key = 200, .value = null },
    };

    const rows = [_]Row{
        .{ .uuid_map = &uuid_entries, .dec_map = &dec_entries, .interval_map = &interval_entries },
        .{ .uuid_map = null, .dec_map = null, .interval_map = null },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{
        .compression = .uncompressed,
        .use_dictionary = false,
    });
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 2), reader.rowCount());

    // Row 0: all populated
    const row0 = (try reader.next()).?;
    defer reader.freeRow(&row0);

    try std.testing.expect(row0.uuid_map != null);
    try std.testing.expectEqual(@as(usize, 3), row0.uuid_map.?.len);
    try std.testing.expectEqual(@as(i32, 1), row0.uuid_map.?[0].key);
    try std.testing.expectEqualSlices(u8, &uuid_a.bytes, &row0.uuid_map.?[0].value.?.bytes);
    try std.testing.expectEqual(@as(i32, 2), row0.uuid_map.?[1].key);
    try std.testing.expect(row0.uuid_map.?[1].value == null);
    try std.testing.expectEqual(@as(i32, 3), row0.uuid_map.?[2].key);
    try std.testing.expectEqualSlices(u8, &uuid_b.bytes, &row0.uuid_map.?[2].value.?.bytes);

    try std.testing.expect(row0.dec_map != null);
    try std.testing.expectEqual(@as(usize, 2), row0.dec_map.?.len);
    try std.testing.expectEqual(@as(i32, 10), row0.dec_map.?[0].key);
    try std.testing.expectEqual(@as(i128, 12345), row0.dec_map.?[0].value.?.toI128());
    try std.testing.expectEqual(@as(i32, 20), row0.dec_map.?[1].key);
    try std.testing.expectEqual(@as(i128, -99999), row0.dec_map.?[1].value.?.toI128());

    try std.testing.expect(row0.interval_map != null);
    try std.testing.expectEqual(@as(usize, 2), row0.interval_map.?.len);
    try std.testing.expectEqual(@as(i32, 100), row0.interval_map.?[0].key);
    try std.testing.expectEqual(@as(u32, 12), row0.interval_map.?[0].value.?.months);
    try std.testing.expectEqual(@as(u32, 30), row0.interval_map.?[0].value.?.days);
    try std.testing.expectEqual(@as(u32, 3600000), row0.interval_map.?[0].value.?.millis);
    try std.testing.expectEqual(@as(i32, 200), row0.interval_map.?[1].key);
    try std.testing.expect(row0.interval_map.?[1].value == null);

    // Row 1: all null
    const row1 = (try reader.next()).?;
    defer reader.freeRow(&row1);
    try std.testing.expect(row1.uuid_map == null);
    try std.testing.expect(row1.dec_map == null);
    try std.testing.expect(row1.interval_map == null);
}

test "round-trip dictionary encoding for f32 and f64" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const Row = struct {
        temperature: f32,
        pressure: f64,
    };

    const rows = [_]Row{
        .{ .temperature = 20.5, .pressure = 1013.25 },
        .{ .temperature = 20.5, .pressure = 1013.25 },
        .{ .temperature = 21.0, .pressure = 1015.00 },
        .{ .temperature = 20.5, .pressure = 1013.25 },
        .{ .temperature = 22.0, .pressure = 1010.50 },
        .{ .temperature = 21.0, .pressure = 1015.00 },
    };

    // Write with dictionary encoding
    {
        const file = try tmp_dir.dir.createFile("dict_float.parquet", .{});
        defer file.close();

        var writer = try parquet.writeToFileRows(Row, allocator, file, .{
            .compression = .uncompressed,
            .use_dictionary = true,
        });
        defer writer.deinit();

        for (&rows) |*row| try writer.writeRow(row.*);
        try writer.close();
    }

    // Verify encoding metadata
    {
        const file = try tmp_dir.dir.openFile("dict_float.parquet", .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        for (0..2) |col_idx| {
            const col_chunk = reader.metadata.row_groups[0].columns[col_idx];
            const encodings = col_chunk.meta_data.?.encodings;
            var has_dict = false;
            for (encodings) |enc| {
                if (enc == .plain_dictionary or enc == .rle_dictionary) {
                    has_dict = true;
                    break;
                }
            }
            try std.testing.expect(has_dict);
        }
    }

    // Read back and verify values
    {
        const file = try tmp_dir.dir.openFile("dict_float.parquet", .{});
        defer file.close();

        var reader = try parquet.openFileRowReader(Row, allocator, file, .{});
        defer reader.deinit();

        var idx: usize = 0;
        while (try reader.next()) |row| {
            defer reader.freeRow(&row);
            try std.testing.expectApproxEqAbs(rows[idx].temperature, row.temperature, 0.001);
            try std.testing.expectApproxEqAbs(rows[idx].pressure, row.pressure, 0.001);
            idx += 1;
        }
        try std.testing.expectEqual(rows.len, idx);
    }
}

test "round-trip delta binary packed for Date, Timestamp, Time" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const Row = struct {
        event_date: parquet.Date,
        event_ts: parquet.Timestamp(.micros),
        event_time: parquet.Time(.micros),
    };

    const rows = [_]Row{
        .{
            .event_date = parquet.Date.fromDays(19000),
            .event_ts = .{ .value = 1_700_000_000_000_000 },
            .event_time = .{ .value = 36_000_000_000 },
        },
        .{
            .event_date = parquet.Date.fromDays(19001),
            .event_ts = .{ .value = 1_700_000_001_000_000 },
            .event_time = .{ .value = 36_001_000_000 },
        },
        .{
            .event_date = parquet.Date.fromDays(19002),
            .event_ts = .{ .value = 1_700_000_002_000_000 },
            .event_time = .{ .value = 36_002_000_000 },
        },
        .{
            .event_date = parquet.Date.fromDays(19010),
            .event_ts = .{ .value = 1_700_000_010_000_000 },
            .event_time = .{ .value = 36_010_000_000 },
        },
    };

    // Write with delta_binary_packed encoding
    {
        const file = try tmp_dir.dir.createFile("delta_temporal.parquet", .{});
        defer file.close();

        var writer = try parquet.writeToFileRows(Row, allocator, file, .{
            .compression = .uncompressed,
            .use_dictionary = false,
            .int_encoding = .delta_binary_packed,
        });
        defer writer.deinit();

        for (&rows) |*row| try writer.writeRow(row.*);
        try writer.close();
    }

    // Verify encoding metadata
    {
        const file = try tmp_dir.dir.openFile("delta_temporal.parquet", .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        for (0..3) |col_idx| {
            const col_chunk = reader.metadata.row_groups[0].columns[col_idx];
            const encodings = col_chunk.meta_data.?.encodings;
            var has_delta = false;
            for (encodings) |enc| {
                if (enc == .delta_binary_packed) {
                    has_delta = true;
                    break;
                }
            }
            try std.testing.expect(has_delta);
        }
    }

    // Read back and verify values
    {
        const file = try tmp_dir.dir.openFile("delta_temporal.parquet", .{});
        defer file.close();

        var reader = try parquet.openFileRowReader(Row, allocator, file, .{});
        defer reader.deinit();

        var idx: usize = 0;
        while (try reader.next()) |row| {
            defer reader.freeRow(&row);
            try std.testing.expectEqual(rows[idx].event_date.days, row.event_date.days);
            try std.testing.expectEqual(rows[idx].event_ts.value, row.event_ts.value);
            try std.testing.expectEqual(rows[idx].event_time.value, row.event_time.value);
            idx += 1;
        }
        try std.testing.expectEqual(rows.len, idx);
    }
}

test "round-trip byte stream split for Interval, Decimal, f16" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const Dec = parquet.Decimal(9, 2);

    const Row = struct {
        duration: parquet.Interval,
        amount: Dec,
        half: f16,
    };

    const rows = [_]Row{
        .{
            .duration = parquet.Interval.fromComponents(1, 15, 3_600_000),
            .amount = Dec.fromUnscaled(12345),
            .half = 1.5,
        },
        .{
            .duration = parquet.Interval.fromComponents(0, 30, 7_200_000),
            .amount = Dec.fromUnscaled(-6789),
            .half = -2.0,
        },
        .{
            .duration = parquet.Interval.fromComponents(12, 0, 0),
            .amount = Dec.fromUnscaled(0),
            .half = 0.0,
        },
        .{
            .duration = parquet.Interval.fromComponents(2, 10, 1_000),
            .amount = Dec.fromUnscaled(99999),
            .half = 3.14,
        },
    };

    // Write with byte_stream_split encoding (per-column override for FLBA types)
    {
        const file = try tmp_dir.dir.createFile("bss_flba.parquet", .{});
        defer file.close();

        var writer = try parquet.writeToFileRows(Row, allocator, file, .{
            .compression = .uncompressed,
            .use_dictionary = false,
            .column_encodings = &.{
                .{ .name = "duration", .encoding = .byte_stream_split },
                .{ .name = "amount", .encoding = .byte_stream_split },
                .{ .name = "half", .encoding = .byte_stream_split },
            },
        });
        defer writer.deinit();

        for (&rows) |*row| try writer.writeRow(row.*);
        try writer.close();
    }

    // Verify encoding metadata
    {
        const file = try tmp_dir.dir.openFile("bss_flba.parquet", .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        for (0..3) |col_idx| {
            const col_chunk = reader.metadata.row_groups[0].columns[col_idx];
            const encodings = col_chunk.meta_data.?.encodings;
            var has_bss = false;
            for (encodings) |enc| {
                if (enc == .byte_stream_split) {
                    has_bss = true;
                    break;
                }
            }
            try std.testing.expect(has_bss);
        }
    }

    // Read back and verify values
    {
        const file = try tmp_dir.dir.openFile("bss_flba.parquet", .{});
        defer file.close();

        var reader = try parquet.openFileRowReader(Row, allocator, file, .{});
        defer reader.deinit();

        var idx: usize = 0;
        while (try reader.next()) |row| {
            defer reader.freeRow(&row);
            try std.testing.expectEqual(rows[idx].duration.months, row.duration.months);
            try std.testing.expectEqual(rows[idx].duration.days, row.duration.days);
            try std.testing.expectEqual(rows[idx].duration.millis, row.duration.millis);
            try std.testing.expectEqual(rows[idx].amount.toI128(), row.amount.toI128());
            try std.testing.expectApproxEqAbs(@as(f32, rows[idx].half), @as(f32, row.half), 0.01);
            idx += 1;
        }
        try std.testing.expectEqual(rows.len, idx);
    }
}

// =============================================================================
// WriteTarget external target round-trip tests
// =============================================================================

test "Writer.initWithTarget round-trip via BufferTarget" {
    const allocator = std.testing.allocator;

    const columns = [_]parquet.ColumnDef{
        .{ .name = "id", .type_ = .int64, .optional = false },
        .{ .name = "name", .type_ = .byte_array, .optional = true },
    };

    // Write via external WriteTarget backed by BufferTarget
    var bt = parquet.io.BufferTarget.init(allocator);
    defer bt.deinit();

    var writer = try parquet.Writer.initWithTarget(allocator, bt.target(), &columns);
    defer writer.deinit();

    const ids = [_]parquet.Optional(i64){
        .{ .value = 10 }, .{ .value = 20 }, .{ .value = 30 },
    };
    try writer.writeColumnOptional(i64, 0, &ids);

    const names = [_]parquet.Optional([]const u8){
        .{ .value = "alice" }, .null_value, .{ .value = "charlie" },
    };
    try writer.writeColumnOptional([]const u8, 1, &names);

    try writer.close();

    // Read back from the buffer and verify
    const buffer = bt.written();
    var reader = try parquet.openBuffer(allocator, buffer);
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 1), reader.metadata.row_groups.len);
    try std.testing.expectEqual(@as(i64, 3), reader.metadata.row_groups[0].num_rows);

    const col0 = try reader.readColumn(0, i64);
    defer allocator.free(col0);
    try std.testing.expectEqual(@as(usize, 3), col0.len);
    try std.testing.expectEqual(@as(i64, 10), col0[0].value);
    try std.testing.expectEqual(@as(i64, 20), col0[1].value);
    try std.testing.expectEqual(@as(i64, 30), col0[2].value);

    const col1 = try reader.readColumn(1, []const u8);
    defer {
        for (col1) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(col1);
    }
    try std.testing.expectEqual(@as(usize, 3), col1.len);
    try std.testing.expectEqualStrings("alice", col1[0].value);
    try std.testing.expectEqual(parquet.Optional([]const u8).null_value, col1[1]);
    try std.testing.expectEqualStrings("charlie", col1[2].value);
}

test "RowWriter.initWithTarget round-trip via BufferTarget" {
    const allocator = std.testing.allocator;

    const SensorReading = struct {
        sensor_id: i32,
        value: f64,
        label: []const u8,
    };

    // Write via external WriteTarget backed by BufferTarget
    var bt = parquet.io.BufferTarget.init(allocator);
    defer bt.deinit();

    var rw = try parquet.RowWriter(SensorReading).initWithTarget(allocator, bt.target(), .{
        .compression = .uncompressed,
        .use_dictionary = false,
    });
    defer rw.deinit();

    const rows = [_]SensorReading{
        .{ .sensor_id = 1, .value = 23.5, .label = "temp" },
        .{ .sensor_id = 2, .value = 98.1, .label = "humidity" },
        .{ .sensor_id = 3, .value = 1013.25, .label = "pressure" },
    };
    try rw.writeRows(&rows);
    try rw.close();

    // Read back from the buffer and verify
    const buffer = bt.written();
    var reader = try parquet.openBufferRowReader(SensorReading, allocator, buffer, .{});
    defer reader.deinit();

    var idx: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        try std.testing.expectEqual(rows[idx].sensor_id, row.sensor_id);
        try std.testing.expectApproxEqAbs(rows[idx].value, row.value, 0.001);
        try std.testing.expectEqualStrings(rows[idx].label, row.label);
        idx += 1;
    }
    try std.testing.expectEqual(@as(usize, 3), idx);
}

test "round-trip nested list of optionals ([][]?i32)" {
    const allocator = std.testing.allocator;

    const Row = struct {
        values: []const []const ?i32,
    };

    const a1 = [_]?i32{ 1, null, 3 };
    const a2 = [_]?i32{null};
    const a3 = [_]?i32{ 10, 20 };
    const o1 = [_][]const ?i32{ &a1, &a2 };
    const o2 = [_][]const ?i32{&a3};
    const o3 = [_][]const ?i32{ &[_]?i32{}, &[_]?i32{null} };

    const rows = [_]Row{
        .{ .values = &o1 },
        .{ .values = &o2 },
        .{ .values = &o3 },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{});
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var idx: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        const expected = rows[idx].values;
        const actual = row.values;
        try std.testing.expectEqual(expected.len, actual.len);
        for (expected, 0..) |exp_inner, outer| {
            try std.testing.expectEqual(exp_inner.len, actual[outer].len);
            for (exp_inner, 0..) |exp_val, inner| {
                try std.testing.expectEqual(exp_val, actual[outer][inner]);
            }
        }
        idx += 1;
    }
    try std.testing.expectEqual(rows.len, idx);
}

test "round-trip optional nested list (?[][]i32)" {
    const allocator = std.testing.allocator;

    const Row = struct {
        values: ?[]const []const i32,
    };

    const a1 = [_]i32{ 1, 2, 3 };
    const a2 = [_]i32{42};
    const o1 = [_][]const i32{ &a1, &a2 };
    const o2 = [_][]const i32{&[_]i32{}};

    const rows = [_]Row{
        .{ .values = &o1 },
        .{ .values = null },
        .{ .values = &o2 },
    };

    var rw = try parquet.writeToBufferRows(Row, allocator, .{});
    defer rw.deinit();
    for (&rows) |*row| try rw.writeRow(row.*);
    try rw.close();

    const buffer = try rw.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferRowReader(Row, allocator, buffer, .{});
    defer reader.deinit();

    var idx: usize = 0;
    while (try reader.next()) |row| {
        defer reader.freeRow(&row);
        if (rows[idx].values) |expected| {
            try std.testing.expect(row.values != null);
            const actual = row.values.?;
            try std.testing.expectEqual(expected.len, actual.len);
            for (expected, 0..) |exp_inner, outer| {
                try std.testing.expectEqual(exp_inner.len, actual[outer].len);
                for (exp_inner, 0..) |exp_val, inner| {
                    try std.testing.expectEqual(exp_val, actual[outer][inner]);
                }
            }
        } else {
            try std.testing.expect(row.values == null);
        }
        idx += 1;
    }
    try std.testing.expectEqual(rows.len, idx);
}
