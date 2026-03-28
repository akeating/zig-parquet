//! Comprehensive round-trip tests for all compression codecs
//!
//! Tests write → read → compare for all supported compression formats.

const std = @import("std");
const parquet = @import("../lib.zig");
const build_options = @import("build_options");
const Value = parquet.Value;
const DynRow = parquet.Row;
const TypeInfo = parquet.TypeInfo;
const SchemaNode = parquet.SchemaNode;
const Interval = parquet.types.Interval;

const ALL_CODECS = blk: {
    var codecs: [6]parquet.format.CompressionCodec = undefined;
    var n: usize = 0;
    codecs[n] = .uncompressed;
    n += 1;
    if (build_options.enable_zstd) {
        codecs[n] = .zstd;
        n += 1;
    }
    if (build_options.enable_gzip) {
        codecs[n] = .gzip;
        n += 1;
    }
    if (build_options.enable_snappy) {
        codecs[n] = .snappy;
        n += 1;
    }
    if (build_options.enable_lz4) {
        codecs[n] = .lz4_raw;
        n += 1;
    }
    if (build_options.enable_brotli) {
        codecs[n] = .brotli;
        n += 1;
    }
    break :blk codecs[0..n].*;
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

            var reader = try parquet.openFileDynamic(allocator, file, .{});
            defer reader.deinit();

            try std.testing.expectEqual(@as(i64, 10), reader.metadata.num_rows);

            // Verify codec
            const col_chunk = reader.metadata.row_groups[0].columns[0];
            try std.testing.expectEqual(codec, col_chunk.meta_data.?.codec);

            // Read and compare
            const rows = try reader.readAllRows(0);
            defer {
                for (rows) |row| row.deinit();
                allocator.free(rows);
            }

            const expected = [_]i64{ 1, 2, 3, 4, 5, -100, 0, 999999, -123456, 42 };
            try std.testing.expectEqual(@as(usize, 10), rows.len);

            for (rows, 0..) |row, i| {
                try std.testing.expectEqual(@as(?i64, expected[i]), row.getColumn(0).?.asInt64());
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

            var reader = try parquet.openFileDynamic(allocator, file, .{});
            defer reader.deinit();

            try std.testing.expectEqual(@as(i64, 5), reader.metadata.num_rows);

            const rows = try reader.readAllRows(0);
            defer {
                for (rows) |row| row.deinit();
                allocator.free(rows);
            }

            for (rows, 0..) |row, i| {
                try std.testing.expectEqual(@as(?i32, i32_values[i]), row.getColumn(0).?.asInt32());
                try std.testing.expectEqual(@as(?i64, i64_values[i]), row.getColumn(1).?.asInt64());
                try std.testing.expectApproxEqAbs(f64_values[i], row.getColumn(2).?.asDouble().?, 0.0001);
                try std.testing.expectEqualStrings(str_values[i], row.getColumn(3).?.asBytes().?);
            }
        }
    }
}

test "round-trip with nullable columns" {
    if (!build_options.enable_zstd) return;
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

        var reader = try parquet.openFileDynamic(allocator, file, .{});
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 10), reader.metadata.num_rows);

        const rows = try reader.readAllRows(0);
        defer {
            for (rows) |row| row.deinit();
            allocator.free(rows);
        }

        const expected = [_]?i64{ 1, null, 3, null, 5, 6, null, 8, 9, null };

        for (rows, 0..) |row, i| {
            if (expected[i]) |exp| {
                try std.testing.expectEqual(@as(?i64, exp), row.getColumn(0).?.asInt64());
            } else {
                try std.testing.expect(row.getColumn(0).?.isNull());
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

        var reader = try parquet.openFileDynamic(allocator, file, .{});
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 5), reader.metadata.num_rows);

        const rows = try reader.readAllRows(0);
        defer {
            for (rows) |row| row.deinit();
            allocator.free(rows);
        }

        for (rows, 0..) |row, i| {
            try std.testing.expectEqual(@as(?bool, bool_values[i]), row.getColumn(0).?.asBool());
            try std.testing.expectEqual(@as(?i32, i32_values[i]), row.getColumn(1).?.asInt32());
            try std.testing.expectEqual(@as(?i64, i64_values[i]), row.getColumn(2).?.asInt64());
            try std.testing.expectApproxEqAbs(f32_values[i], row.getColumn(3).?.asFloat().?, 0.0001);
            try std.testing.expectApproxEqAbs(f64_values[i], row.getColumn(4).?.asDouble().?, 0.0001);
            try std.testing.expectEqualStrings(str_values[i], row.getColumn(5).?.asBytes().?);
            try std.testing.expectEqualStrings(fixed_values[i], row.getColumn(6).?.asBytes().?);
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

        var reader = try parquet.openFileDynamic(allocator, file, .{});
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

        const rows = try reader.readAllRows(0);
        defer {
            for (rows) |row| row.deinit();
            allocator.free(rows);
        }

        // Verify string data
        for (rows, 0..) |row, i| {
            try std.testing.expectEqualStrings(string_values[i], row.getColumn(0).?.asBytes().?);
        }

        // Verify date data
        for (rows, 0..) |row, i| {
            try std.testing.expectEqual(@as(?i32, date_values[i]), row.getColumn(1).?.asInt32());
        }

        // Verify timestamp data
        for (rows, 0..) |row, i| {
            try std.testing.expectEqual(@as(?i64, timestamp_values[i]), row.getColumn(2).?.asInt64());
        }

        // Verify uuid data
        for (rows, 0..) |row, i| {
            try std.testing.expectEqualStrings(uuid_values[i], row.getColumn(5).?.asBytes().?);
        }
    }
}

test "round-trip validates compression actually works" {
    if (!build_options.enable_zstd) return;
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_compression_check.parquet";

    // Write a large column with compressible data (all same value)
    const values = [_]i64{42} ** 1000;

    // Write uncompressed
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
    }

    // Write compressed with zstd
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "value", .type_ = .int64, .optional = false, .codec = .zstd },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        try writer.writeColumn(i64, 0, &values);
        try writer.close();
    }

    // Compare file sizes
    const uncompressed_stat = try tmp_dir.dir.statFile("uncompressed.parquet");
    const compressed_stat = try tmp_dir.dir.statFile(file_path);

    // Compressed file should be smaller
    try std.testing.expect(compressed_stat.size < uncompressed_stat.size);

    // Verify data round-trips correctly
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFileDynamic(allocator, file, .{});
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 1000), reader.metadata.num_rows);

        const rows = try reader.readAllRows(0);
        defer {
            for (rows) |row| row.deinit();
            allocator.free(rows);
        }

        for (rows) |row| {
            try std.testing.expectEqual(@as(?i64, 42), row.getColumn(0).?.asInt64());
        }
    }
}

// =============================================================================
// Statistics round-trip tests (low-level Writer/Reader)
// =============================================================================

test "round-trip statistics i32" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const values = [_]i32{ 5, -3, 127, -128, 0 };

    {
        const file = try tmp_dir.dir.createFile("stats_i32.parquet", .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "val", .type_ = .int32, .optional = false },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();
        try writer.writeColumn(i32, 0, &values);
        try writer.close();
    }

    {
        const file = try tmp_dir.dir.openFile("stats_i32.parquet", .{});
        defer file.close();

        var reader = try parquet.openFileDynamic(allocator, file, .{});
        defer reader.deinit();

        const stats = reader.getColumnStatistics(0, 0);
        try std.testing.expect(stats != null);
        const s = stats.?;
        try std.testing.expectEqual(@as(i32, -128), s.minAsI32().?);
        try std.testing.expectEqual(@as(i32, 127), s.maxAsI32().?);
        try std.testing.expectEqual(@as(i64, 0), s.null_count.?);
    }
}

test "round-trip statistics i64 with nulls" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const values = [_]?i64{ 100, null, 300, -200, null };

    {
        const file = try tmp_dir.dir.createFile("stats_i64_nulls.parquet", .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "val", .type_ = .int64, .optional = true },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();
        try writer.writeColumnNullable(i64, 0, &values);
        try writer.close();
    }

    {
        const file = try tmp_dir.dir.openFile("stats_i64_nulls.parquet", .{});
        defer file.close();

        var reader = try parquet.openFileDynamic(allocator, file, .{});
        defer reader.deinit();

        const stats = reader.getColumnStatistics(0, 0);
        try std.testing.expect(stats != null);
        const s = stats.?;
        try std.testing.expectEqual(@as(i64, -200), s.minAsI64().?);
        try std.testing.expectEqual(@as(i64, 300), s.maxAsI64().?);
        try std.testing.expectEqual(@as(i64, 2), s.null_count.?);
    }
}

test "round-trip statistics byte array" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const values = [_][]const u8{ "cherry", "apple", "banana" };

    {
        const file = try tmp_dir.dir.createFile("stats_ba.parquet", .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            parquet.ColumnDef.string("val", false),
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();
        try writer.writeColumn([]const u8, 0, &values);
        try writer.close();
    }

    {
        const file = try tmp_dir.dir.openFile("stats_ba.parquet", .{});
        defer file.close();

        var reader = try parquet.openFileDynamic(allocator, file, .{});
        defer reader.deinit();

        const stats = reader.getColumnStatistics(0, 0);
        try std.testing.expect(stats != null);
        const s = stats.?;
        try std.testing.expectEqualStrings("apple", s.getMinBytes().?);
        try std.testing.expectEqualStrings("cherry", s.getMaxBytes().?);
    }
}

test "round-trip statistics double" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const values = [_]f64{ 3.14, -2.71, 0.0, 1.618 };

    {
        const file = try tmp_dir.dir.createFile("stats_double.parquet", .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "val", .type_ = .double, .optional = false },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();
        try writer.writeColumn(f64, 0, &values);
        try writer.close();
    }

    {
        const file = try tmp_dir.dir.openFile("stats_double.parquet", .{});
        defer file.close();

        var reader = try parquet.openFileDynamic(allocator, file, .{});
        defer reader.deinit();

        const stats = reader.getColumnStatistics(0, 0);
        try std.testing.expect(stats != null);
        const s = stats.?;
        try std.testing.expectApproxEqAbs(@as(f64, -2.71), s.minAsF64().?, 0.001);
        try std.testing.expectApproxEqAbs(@as(f64, 3.14), s.maxAsF64().?, 0.001);
    }
}

test "round-trip statistics boolean" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const values = [_]bool{ true, false, true, true, false };

    {
        const file = try tmp_dir.dir.createFile("stats_bool.parquet", .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "val", .type_ = .boolean, .optional = false },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();
        try writer.writeColumn(bool, 0, &values);
        try writer.close();
    }

    {
        const file = try tmp_dir.dir.openFile("stats_bool.parquet", .{});
        defer file.close();

        var reader = try parquet.openFileDynamic(allocator, file, .{});
        defer reader.deinit();

        const stats = reader.getColumnStatistics(0, 0);
        try std.testing.expect(stats != null);
        const s = stats.?;
        const min_bytes = s.getMinBytes().?;
        const max_bytes = s.getMaxBytes().?;
        try std.testing.expectEqual(@as(u8, 0), min_bytes[0]);
        try std.testing.expectEqual(@as(u8, 1), max_bytes[0]);
    }
}

test "round-trip statistics float" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const values = [_]f32{ 1.5, -2.5, 3.14, 0.0 };

    {
        const file = try tmp_dir.dir.createFile("stats_float.parquet", .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "val", .type_ = .float, .optional = false },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();
        try writer.writeColumn(f32, 0, &values);
        try writer.close();
    }

    {
        const file = try tmp_dir.dir.openFile("stats_float.parquet", .{});
        defer file.close();

        var reader = try parquet.openFileDynamic(allocator, file, .{});
        defer reader.deinit();

        const stats = reader.getColumnStatistics(0, 0);
        try std.testing.expect(stats != null);
        const s = stats.?;
        try std.testing.expectApproxEqAbs(@as(f32, -2.5), s.minAsF32().?, 0.001);
        try std.testing.expectApproxEqAbs(@as(f32, 3.14), s.maxAsF32().?, 0.01);
    }
}

test "round-trip statistics fixed byte array" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const val0 = [_]u8{ 0x10, 0x20, 0x30, 0x40 };
    const val1 = [_]u8{ 0x01, 0x02, 0x03, 0x04 };
    const val2 = [_]u8{ 0xFF, 0xFF, 0xFF, 0xFF };
    const values = [_][]const u8{ &val0, &val1, &val2 };

    {
        const file = try tmp_dir.dir.createFile("stats_flba.parquet", .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "val", .type_ = .fixed_len_byte_array, .optional = false, .type_length = 4 },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();
        try writer.writeColumnFixedByteArray(0, &values);
        try writer.close();
    }

    {
        const file = try tmp_dir.dir.openFile("stats_flba.parquet", .{});
        defer file.close();

        var reader = try parquet.openFileDynamic(allocator, file, .{});
        defer reader.deinit();

        const stats = reader.getColumnStatistics(0, 0);
        try std.testing.expect(stats != null);
        const s = stats.?;
        try std.testing.expectEqualSlices(u8, &val1, s.getMinBytes().?);
        try std.testing.expectEqualSlices(u8, &val2, s.getMaxBytes().?);
    }
}

test "round-trip statistics edge values i32" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const values = [_]i32{ std.math.maxInt(i32), std.math.minInt(i32), 0 };

    {
        const file = try tmp_dir.dir.createFile("stats_i32_edge.parquet", .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "val", .type_ = .int32, .optional = false },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();
        try writer.writeColumn(i32, 0, &values);
        try writer.close();
    }

    {
        const file = try tmp_dir.dir.openFile("stats_i32_edge.parquet", .{});
        defer file.close();

        var reader = try parquet.openFileDynamic(allocator, file, .{});
        defer reader.deinit();

        const stats = reader.getColumnStatistics(0, 0);
        try std.testing.expect(stats != null);
        const s = stats.?;
        try std.testing.expectEqual(std.math.minInt(i32), s.minAsI32().?);
        try std.testing.expectEqual(std.math.maxInt(i32), s.maxAsI32().?);
    }
}

test "round-trip statistics edge values i64" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const values = [_]i64{ std.math.maxInt(i64), std.math.minInt(i64), 0 };

    {
        const file = try tmp_dir.dir.createFile("stats_i64_edge.parquet", .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "val", .type_ = .int64, .optional = false },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();
        try writer.writeColumn(i64, 0, &values);
        try writer.close();
    }

    {
        const file = try tmp_dir.dir.openFile("stats_i64_edge.parquet", .{});
        defer file.close();

        var reader = try parquet.openFileDynamic(allocator, file, .{});
        defer reader.deinit();

        const stats = reader.getColumnStatistics(0, 0);
        try std.testing.expect(stats != null);
        const s = stats.?;
        try std.testing.expectEqual(std.math.minInt(i64), s.minAsI64().?);
        try std.testing.expectEqual(std.math.maxInt(i64), s.maxAsI64().?);
    }
}

test "round-trip statistics all nulls" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const values = [_]?i64{ null, null, null, null, null };

    {
        const file = try tmp_dir.dir.createFile("stats_nulls.parquet", .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "val", .type_ = .int64, .optional = true },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();
        try writer.writeColumnNullable(i64, 0, &values);
        try writer.close();
    }

    {
        const file = try tmp_dir.dir.openFile("stats_nulls.parquet", .{});
        defer file.close();

        var reader = try parquet.openFileDynamic(allocator, file, .{});
        defer reader.deinit();

        const stats = reader.getColumnStatistics(0, 0);
        try std.testing.expect(stats != null);
        const s = stats.?;
        try std.testing.expectEqual(@as(i64, 5), s.null_count.?);
    }
}

// =============================================================================
// DynamicWriter/DynamicReader round-trip tests (converted from RowWriter/RowReader)
// =============================================================================

fn deferFreeRows(allocator: std.mem.Allocator, rows: []DynRow) void {
    for (rows) |row| row.deinit();
    allocator.free(rows);
}

test "round-trip nullable i64 data (DynamicWriter)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", TypeInfo.int64, .{});
    try writer.begin();

    const expected = [_]?i64{ 100, null, 102, 103, null, 105, 106, null, 108, 109 };
    for (expected) |val| {
        if (val) |v| {
            try writer.setInt64(0, v);
        } else {
            try writer.setNull(0);
        }
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 10), rows.len);

    for (rows, 0..) |row, i| {
        if (expected[i]) |exp| {
            try std.testing.expectEqual(@as(?i64, exp), row.getColumn(0).?.asInt64());
        } else {
            try std.testing.expect(row.getColumn(0).?.isNull());
        }
    }
}

test "round-trip nullable f64 data (DynamicWriter)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("value", TypeInfo.double_, .{});
    try writer.begin();

    const expected_vals = [_]?f64{ 1.5, 2.7, null, 3.14159, null, 2.71828, 1.41421, null, 0.0, -1.5 };
    for (expected_vals) |val| {
        if (val) |v| {
            try writer.setDouble(0, v);
        } else {
            try writer.setNull(0);
        }
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 10), rows.len);

    for (rows, 0..) |row, i| {
        if (expected_vals[i]) |exp| {
            try std.testing.expectApproxEqAbs(exp, row.getColumn(0).?.asDouble().?, 0.00001);
        } else {
            try std.testing.expect(row.getColumn(0).?.isNull());
        }
    }
}

test "round-trip nullable i32 data (DynamicWriter)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("count", TypeInfo.int32, .{});
    try writer.begin();

    const expected = [_]?i32{ 0, 1, null, 3, 4 };
    for (expected) |val| {
        if (val) |v| {
            try writer.setInt32(0, v);
        } else {
            try writer.setNull(0);
        }
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 5), rows.len);

    for (rows, 0..) |row, i| {
        if (expected[i]) |exp| {
            try std.testing.expectEqual(@as(?i32, exp), row.getColumn(0).?.asInt32());
        } else {
            try std.testing.expect(row.getColumn(0).?.isNull());
        }
    }
}

test "round-trip nullable f32 data (DynamicWriter)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("temp", TypeInfo.float_, .{});
    try writer.begin();

    const expected = [_]?f32{ 20.5, null, 21.0, 21.5, null };
    for (expected) |val| {
        if (val) |v| {
            try writer.setFloat(0, v);
        } else {
            try writer.setNull(0);
        }
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 5), rows.len);

    for (rows, 0..) |row, i| {
        if (expected[i]) |exp| {
            try std.testing.expectApproxEqAbs(exp, row.getColumn(0).?.asFloat().?, 0.001);
        } else {
            try std.testing.expect(row.getColumn(0).?.isNull());
        }
    }
}

test "round-trip multi-column data (DynamicWriter)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("timestamp", TypeInfo.int64, .{});
    try writer.addColumn("count", TypeInfo.int64, .{});
    try writer.addColumn("temperature", TypeInfo.double_, .{});
    try writer.addColumn("pressure", TypeInfo.double_, .{});
    try writer.begin();

    const data = [_][4]f64{
        .{ 1000, 5, 20.5, 101.3 },
        .{ 1001, 10, 21.0, 101.2 },
        .{ 1002, 15, 21.5, 101.1 },
        .{ 1003, 20, 22.0, 101.0 },
        .{ 1004, 25, 22.5, 100.9 },
    };

    for (data) |row| {
        try writer.setInt64(0, @intFromFloat(row[0]));
        try writer.setInt64(1, @intFromFloat(row[1]));
        try writer.setDouble(2, row[2]);
        try writer.setDouble(3, row[3]);
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 5), rows.len);

    for (rows, 0..) |row, i| {
        try std.testing.expectEqual(@as(?i64, @intFromFloat(data[i][0])), row.getColumn(0).?.asInt64());
        try std.testing.expectEqual(@as(?i64, @intFromFloat(data[i][1])), row.getColumn(1).?.asInt64());
        try std.testing.expectApproxEqAbs(data[i][2], row.getColumn(2).?.asDouble().?, 0.001);
        try std.testing.expectApproxEqAbs(data[i][3], row.getColumn(3).?.asDouble().?, 0.001);
    }
}

test "round-trip i32 data (DynamicWriter)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("value", TypeInfo.int32, .{});
    try writer.begin();

    const expected = [_]i32{ 0, 1, -1, 256, -256, 65536, std.math.maxInt(i32), std.math.minInt(i32) };
    for (expected) |v| {
        try writer.setInt32(0, v);
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 8), rows.len);
    for (rows, 0..) |row, i| {
        try std.testing.expectEqual(@as(?i32, expected[i]), row.getColumn(0).?.asInt32());
    }
}

test "round-trip i64 data (DynamicWriter)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("value", TypeInfo.int64, .{});
    try writer.begin();

    const expected = [_]i64{ 0, 1, -1, 0x0102030405060708, std.math.maxInt(i64), std.math.minInt(i64) };
    for (expected) |v| {
        try writer.setInt64(0, v);
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 6), rows.len);
    for (rows, 0..) |row, i| {
        try std.testing.expectEqual(@as(?i64, expected[i]), row.getColumn(0).?.asInt64());
    }
}

test "round-trip RLE boolean encoding (DynamicWriter)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("flag", TypeInfo.bool_, .{});
    try writer.addColumn("nullable_flag", TypeInfo.bool_, .{});
    try writer.begin();

    const flags = [_]bool{ true, true, true, true, false, false, false, false, true, true };
    const nullable_flags = [_]?bool{ true, true, true, null, false, false, false, false, true, null };

    for (flags, 0..) |flag, i| {
        try writer.setBool(0, flag);
        if (nullable_flags[i]) |nf| {
            try writer.setBool(1, nf);
        } else {
            try writer.setNull(1);
        }
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    try std.testing.expect(buffer.len > 0);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 10), rows.len);

    for (rows, 0..) |row, i| {
        try std.testing.expectEqual(@as(?bool, flags[i]), row.getColumn(0).?.asBool());
        if (nullable_flags[i]) |nf| {
            try std.testing.expectEqual(@as(?bool, nf), row.getColumn(1).?.asBool());
        } else {
            try std.testing.expect(row.getColumn(1).?.isNull());
        }
    }
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

        var reader = try parquet.openFileDynamic(allocator, file, .{});
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

        const rows = try reader.readAllRows(0);
        defer {
            for (rows) |row| row.deinit();
            allocator.free(rows);
        }

        try std.testing.expectEqual(@as(usize, 3), rows.len);
        try std.testing.expectEqualSlices(u8, &val0, rows[0].getColumn(0).?.asBytes().?);
        try std.testing.expectEqualSlices(u8, &val1, rows[1].getColumn(0).?.asBytes().?);
        try std.testing.expectEqualSlices(u8, &val2, rows[2].getColumn(0).?.asBytes().?);
    }
}

test "round-trip UUID column (DynamicWriter)" {
    const allocator = std.testing.allocator;

    const uuid1 = [_]u8{ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10 };
    const uuid2 = [_]u8{ 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20 };
    const uuid3 = [_]u8{ 0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA, 0xF9, 0xF8, 0xF7, 0xF6, 0xF5, 0xF4, 0xF3, 0xF2, 0xF1, 0xF0 };

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", TypeInfo.uuid, .{});
    try writer.begin();

    try writer.setBytes(0, &uuid1);
    try writer.addRow();
    try writer.setBytes(0, &uuid2);
    try writer.addRow();
    try writer.setBytes(0, &uuid3);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 3), rows.len);
    try std.testing.expectEqualSlices(u8, &uuid1, rows[0].getColumn(0).?.asBytes().?);
    try std.testing.expectEqualSlices(u8, &uuid2, rows[1].getColumn(0).?.asBytes().?);
    try std.testing.expectEqualSlices(u8, &uuid3, rows[2].getColumn(0).?.asBytes().?);
}

test "round-trip list of UUIDs (DynamicWriter)" {
    const allocator = std.testing.allocator;

    const uuid1 = [_]u8{ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10 };
    const uuid2 = [_]u8{ 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20 };
    const uuid3 = [_]u8{ 0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA, 0xF9, 0xF8, 0xF7, 0xF6, 0xF5, 0xF4, 0xF3, 0xF2, 0xF1, 0xF0 };

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    const list_node = try writer.allocSchemaNode(.{ .list = try writer.allocSchemaNode(.{ .fixed_len_byte_array = .{ .len = 16, .logical = .uuid } }) });
    try writer.addColumnNested("ids", list_node, .{});
    try writer.begin();

    // Row 0: [uuid1, uuid2]
    try writer.beginList(0);
    try writer.appendNestedFixedBytes(0, &uuid1);
    try writer.appendNestedFixedBytes(0, &uuid2);
    try writer.endList(0);
    try writer.addRow();

    // Row 1: [uuid3]
    try writer.beginList(0);
    try writer.appendNestedFixedBytes(0, &uuid3);
    try writer.endList(0);
    try writer.addRow();

    // Row 2: []
    try writer.beginList(0);
    try writer.endList(0);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 3), rows.len);

    const list0 = rows[0].getColumn(0).?.asList().?;
    try std.testing.expectEqual(@as(usize, 2), list0.len);
    try std.testing.expectEqualSlices(u8, &uuid1, list0[0].asBytes().?);
    try std.testing.expectEqualSlices(u8, &uuid2, list0[1].asBytes().?);

    const list1 = rows[1].getColumn(0).?.asList().?;
    try std.testing.expectEqual(@as(usize, 1), list1.len);
    try std.testing.expectEqualSlices(u8, &uuid3, list1[0].asBytes().?);

    const col2 = rows[2].getColumn(0).?;
    if (col2.asList()) |list2| {
        try std.testing.expectEqual(@as(usize, 0), list2.len);
    } else {
        try std.testing.expectEqual(Value.null_val, col2);
    }
}

test "round-trip list of structs with UUID field (DynamicWriter)" {
    const allocator = std.testing.allocator;

    const uuid1 = [_]u8{ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10 };
    const uuid2 = [_]u8{ 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00 };

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    const fields = try writer.allocSchemaFields(2);
    fields[0] = .{ .name = "tag", .node = try writer.allocSchemaNode(.{ .int32 = .{} }) };
    fields[1] = .{ .name = "id", .node = try writer.allocSchemaNode(.{ .fixed_len_byte_array = .{ .len = 16, .logical = .uuid } }) };
    const struct_node = try writer.allocSchemaNode(.{ .struct_ = .{ .fields = fields } });
    const list_node = try writer.allocSchemaNode(.{ .list = struct_node });
    try writer.addColumnNested("entries", list_node, .{});
    try writer.begin();

    // Row 0: [{tag:1, id:uuid1}, {tag:2, id:uuid2}]
    try writer.beginList(0);
    try writer.appendNestedValue(0, .{ .struct_val = &.{
        .{ .name = "tag", .value = .{ .int32_val = 1 } },
        .{ .name = "id", .value = .{ .fixed_bytes_val = &uuid1 } },
    } });
    try writer.appendNestedValue(0, .{ .struct_val = &.{
        .{ .name = "tag", .value = .{ .int32_val = 2 } },
        .{ .name = "id", .value = .{ .fixed_bytes_val = &uuid2 } },
    } });
    try writer.endList(0);
    try writer.addRow();

    // Row 1: [{tag:42, id:uuid1}]
    try writer.beginList(0);
    try writer.appendNestedValue(0, .{ .struct_val = &.{
        .{ .name = "tag", .value = .{ .int32_val = 42 } },
        .{ .name = "id", .value = .{ .fixed_bytes_val = &uuid1 } },
    } });
    try writer.endList(0);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 2), rows.len);

    const list0 = rows[0].getColumn(0).?.asList().?;
    try std.testing.expectEqual(@as(usize, 2), list0.len);
    const s0_0 = list0[0].asStruct().?;
    try std.testing.expectEqual(@as(?i32, 1), s0_0[0].value.asInt32());
    try std.testing.expectEqualSlices(u8, &uuid1, s0_0[1].value.asBytes().?);
    const s0_1 = list0[1].asStruct().?;
    try std.testing.expectEqual(@as(?i32, 2), s0_1[0].value.asInt32());
    try std.testing.expectEqualSlices(u8, &uuid2, s0_1[1].value.asBytes().?);

    const list1 = rows[1].getColumn(0).?.asList().?;
    try std.testing.expectEqual(@as(usize, 1), list1.len);
    const s1_0 = list1[0].asStruct().?;
    try std.testing.expectEqual(@as(?i32, 42), s1_0[0].value.asInt32());
    try std.testing.expectEqualSlices(u8, &uuid1, s1_0[1].value.asBytes().?);
}

test "round-trip Interval column (DynamicWriter)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("duration", TypeInfo.interval, .{});
    try writer.begin();

    const iv1 = Interval.fromComponents(1, 15, 3600000);
    const iv2 = Interval.fromComponents(0, 0, 0);
    const iv3 = Interval.fromComponents(12, 365, 86400000);
    const intervals = [_]Interval{ iv1, iv2, iv3 };

    for (intervals) |iv| {
        try writer.setBytes(0, &iv.toBytes());
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 3), rows.len);
    for (rows, 0..) |row, i| {
        const bytes = row.getColumn(0).?.asBytes().?;
        try std.testing.expectEqual(@as(usize, 12), bytes.len);
        const read_iv = Interval.fromBytes(bytes[0..12].*);
        try std.testing.expectEqual(intervals[i].months, read_iv.months);
        try std.testing.expectEqual(intervals[i].days, read_iv.days);
        try std.testing.expectEqual(intervals[i].millis, read_iv.millis);
    }
}

test "round-trip list of Intervals (DynamicWriter)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    const list_node = try writer.allocSchemaNode(.{ .list = try writer.allocSchemaNode(.{ .fixed_len_byte_array = .{ .len = 12 } }) });
    try writer.addColumnNested("durations", list_node, .{});
    try writer.begin();

    const iv1 = Interval.fromComponents(1, 10, 1000);
    const iv2 = Interval.fromComponents(2, 20, 2000);
    const iv3 = Interval.fromComponents(0, 0, 0);

    // Row 0: [iv1, iv2]
    try writer.beginList(0);
    try writer.appendNestedFixedBytes(0, &iv1.toBytes());
    try writer.appendNestedFixedBytes(0, &iv2.toBytes());
    try writer.endList(0);
    try writer.addRow();

    // Row 1: [iv3]
    try writer.beginList(0);
    try writer.appendNestedFixedBytes(0, &iv3.toBytes());
    try writer.endList(0);
    try writer.addRow();

    // Row 2: []
    try writer.beginList(0);
    try writer.endList(0);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 3), rows.len);

    const expected_lists = [_][]const Interval{ &.{ iv1, iv2 }, &.{iv3}, &.{} };
    for (rows, 0..) |row, i| {
        const col = row.getColumn(0).?;
        if (col.asList()) |list| {
            try std.testing.expectEqual(expected_lists[i].len, list.len);
            for (expected_lists[i], 0..) |exp, j| {
                const bytes = list[j].asBytes().?;
                const read_iv = Interval.fromBytes(bytes[0..12].*);
                try std.testing.expectEqual(exp.months, read_iv.months);
                try std.testing.expectEqual(exp.days, read_iv.days);
                try std.testing.expectEqual(exp.millis, read_iv.millis);
            }
        } else {
            try std.testing.expectEqual(@as(usize, 0), expected_lists[i].len);
        }
    }
}

test "round-trip optional Date column (DynamicWriter)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("day", TypeInfo.date, .{});
    try writer.begin();

    const expected = [_]?i32{ 18000, null, 19500 };
    for (expected) |val| {
        if (val) |v| {
            try writer.setInt32(0, v);
        } else {
            try writer.setNull(0);
        }
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 3), rows.len);
    for (rows, 0..) |row, i| {
        if (expected[i]) |exp| {
            try std.testing.expectEqual(@as(?i32, exp), row.getColumn(0).?.asInt32());
        } else {
            try std.testing.expect(row.getColumn(0).?.isNull());
        }
    }
}

test "round-trip optional Uuid column (DynamicWriter)" {
    const allocator = std.testing.allocator;

    const uuid1 = [_]u8{ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10 };
    const uuid2 = [_]u8{ 0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA, 0xF9, 0xF8, 0xF7, 0xF6, 0xF5, 0xF4, 0xF3, 0xF2, 0xF1, 0xF0 };

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", TypeInfo.uuid, .{});
    try writer.begin();

    try writer.setBytes(0, &uuid1);
    try writer.addRow();
    try writer.setNull(0);
    try writer.addRow();
    try writer.setBytes(0, &uuid2);
    try writer.addRow();
    try writer.setNull(0);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 4), rows.len);
    try std.testing.expectEqualSlices(u8, &uuid1, rows[0].getColumn(0).?.asBytes().?);
    try std.testing.expect(rows[1].getColumn(0).?.isNull());
    try std.testing.expectEqualSlices(u8, &uuid2, rows[2].getColumn(0).?.asBytes().?);
    try std.testing.expect(rows[3].getColumn(0).?.isNull());
}

test "round-trip optional bool column (DynamicWriter)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("flag", TypeInfo.bool_, .{});
    try writer.begin();

    const expected = [_]?bool{ true, null, false, null, true };
    for (expected) |val| {
        if (val) |v| {
            try writer.setBool(0, v);
        } else {
            try writer.setNull(0);
        }
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 5), rows.len);
    for (rows, 0..) |row, i| {
        if (expected[i]) |exp| {
            try std.testing.expectEqual(@as(?bool, exp), row.getColumn(0).?.asBool());
        } else {
            try std.testing.expect(row.getColumn(0).?.isNull());
        }
    }
}

test "round-trip optional small integer types (DynamicWriter)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("a", TypeInfo.int8, .{});
    try writer.addColumn("b", TypeInfo.int16, .{});
    try writer.addColumn("c", TypeInfo.uint8, .{});
    try writer.addColumn("d", TypeInfo.uint16, .{});
    try writer.addColumn("e", TypeInfo.uint32, .{});
    try writer.addColumn("f", TypeInfo.uint64, .{});
    try writer.begin();

    const SmallIntRow = struct { a: ?i32, b: ?i32, c: ?i32, d: ?i32, e: ?i32, f: ?i64 };
    const data = [_]SmallIntRow{
        .{ .a = -1, .b = -1000, .c = 255, .d = 65535, .e = 100000, .f = 999999999 },
        .{ .a = null, .b = null, .c = null, .d = null, .e = null, .f = null },
        .{ .a = 127, .b = 32767, .c = 0, .d = 0, .e = 0, .f = 0 },
        .{ .a = -128, .b = null, .c = 1, .d = null, .e = 42, .f = null },
    };

    for (data) |row| {
        inline for (.{ .{ 0, "a" }, .{ 1, "b" }, .{ 2, "c" }, .{ 3, "d" }, .{ 4, "e" } }) |pair| {
            if (@field(row, pair[1])) |v| {
                try writer.setInt32(pair[0], v);
            } else {
                try writer.setNull(pair[0]);
            }
        }
        if (row.f) |v| {
            try writer.setInt64(5, v);
        } else {
            try writer.setNull(5);
        }
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 4), rows.len);
    for (rows, 0..) |row, i| {
        inline for (.{ .{ 0, "a" }, .{ 1, "b" }, .{ 2, "c" }, .{ 3, "d" }, .{ 4, "e" } }) |pair| {
            if (@field(data[i], pair[1])) |exp| {
                try std.testing.expectEqual(@as(?i32, exp), row.getColumn(pair[0]).?.asInt32());
            } else {
                try std.testing.expect(row.getColumn(pair[0]).?.isNull());
            }
        }
        if (data[i].f) |exp| {
            try std.testing.expectEqual(@as(?i64, exp), row.getColumn(5).?.asInt64());
        } else {
            try std.testing.expect(row.getColumn(5).?.isNull());
        }
    }
}

test "round-trip optional Timestamp variants (DynamicWriter)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("ts_us", TypeInfo.timestamp_micros, .{});
    try writer.addColumn("ts_ms", TypeInfo.timestamp_millis, .{});
    try writer.addColumn("ts_ns", TypeInfo.timestamp_nanos, .{});
    try writer.begin();

    const E = struct { us: ?i64, ms: ?i64, ns: ?i64 };
    const data = [_]E{
        .{ .us = 1700000000_000000, .ms = 1700000000_000, .ns = 1700000000_000000000 },
        .{ .us = null, .ms = null, .ns = null },
        .{ .us = 0, .ms = 0, .ns = 0 },
        .{ .us = null, .ms = -86400_000, .ns = null },
    };

    for (data) |d| {
        inline for (.{ .{ 0, "us" }, .{ 1, "ms" }, .{ 2, "ns" } }) |pair| {
            if (@field(d, pair[1])) |v| {
                try writer.setInt64(pair[0], v);
            } else {
                try writer.setNull(pair[0]);
            }
        }
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 4), rows.len);
    for (rows, 0..) |row, i| {
        inline for (.{ .{ 0, "us" }, .{ 1, "ms" }, .{ 2, "ns" } }) |pair| {
            if (@field(data[i], pair[1])) |exp| {
                try std.testing.expectEqual(@as(?i64, exp), row.getColumn(pair[0]).?.asInt64());
            } else {
                try std.testing.expect(row.getColumn(pair[0]).?.isNull());
            }
        }
    }
}

test "round-trip optional Time variants (DynamicWriter)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("t_us", TypeInfo.time_micros, .{});
    try writer.addColumn("t_ms", TypeInfo.time_millis, .{});
    try writer.addColumn("t_ns", TypeInfo.time_nanos, .{});
    try writer.begin();

    const E = struct { us: ?i64, ms: ?i32, ns: ?i64 };
    const t_10_30 = 10 * 3600_000_000 + 30 * 60_000_000;
    const t_23_59_59_us = 23 * 3600_000_000 + 59 * 60_000_000 + 59 * 1_000_000 + 999999;
    const t_10_30_ms: i32 = 10 * 3600_000 + 30 * 60_000;
    const t_23_59_59_ms: i32 = 23 * 3600_000 + 59 * 60_000 + 59 * 1000 + 999;
    const t_10_30_ns: i64 = 10 * 3600_000_000_000 + 30 * 60_000_000_000;
    const t_23_59_59_ns: i64 = 23 * 3600_000_000_000 + 59 * 60_000_000_000 + 59 * 1_000_000_000 + 999999999;

    const data = [_]E{
        .{ .us = t_10_30, .ms = t_10_30_ms, .ns = t_10_30_ns },
        .{ .us = null, .ms = null, .ns = null },
        .{ .us = t_23_59_59_us, .ms = t_23_59_59_ms, .ns = t_23_59_59_ns },
        .{ .us = null, .ms = 0, .ns = null },
    };

    for (data) |d| {
        if (d.us) |v| try writer.setInt64(0, v) else try writer.setNull(0);
        if (d.ms) |v| try writer.setInt32(1, v) else try writer.setNull(1);
        if (d.ns) |v| try writer.setInt64(2, v) else try writer.setNull(2);
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 4), rows.len);
    for (rows, 0..) |row, i| {
        if (data[i].us) |exp| {
            try std.testing.expectEqual(@as(?i64, exp), row.getColumn(0).?.asInt64());
        } else {
            try std.testing.expect(row.getColumn(0).?.isNull());
        }
        if (data[i].ms) |exp| {
            try std.testing.expectEqual(@as(?i32, exp), row.getColumn(1).?.asInt32());
        } else {
            try std.testing.expect(row.getColumn(1).?.isNull());
        }
        if (data[i].ns) |exp| {
            try std.testing.expectEqual(@as(?i64, exp), row.getColumn(2).?.asInt64());
        } else {
            try std.testing.expect(row.getColumn(2).?.isNull());
        }
    }
}

test "round-trip Decimal(9,2) flat column (DynamicWriter)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("price", TypeInfo.forDecimal(9, 2), .{});
    try writer.begin();

    const expected = [_]i32{ 12345, -99999, 0, 1, 99999 };
    for (expected) |v| {
        try writer.setInt32(0, v);
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 5), rows.len);
    for (rows, 0..) |row, i| {
        try std.testing.expectEqual(@as(?i32, expected[i]), row.getColumn(0).?.asInt32());
    }
}

test "round-trip Decimal(18,4) and Decimal(38,10) flat columns (DynamicWriter)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("amount", TypeInfo.forDecimal(18, 4), .{});
    try writer.addColumn("big_val", TypeInfo.forDecimal(38, 10), .{});
    try writer.begin();

    const D38 = parquet.Decimal(38, 10);
    const d18_vals = [_]i64{ 123456789012345678, -1, 0 };
    const d38_vals = [_]D38{ D38.fromUnscaled(1), D38.fromUnscaled(-1), D38.fromUnscaled(0) };

    for (d18_vals, 0..) |d18, i| {
        try writer.setInt64(0, d18);
        try writer.setBytes(1, &d38_vals[i].bytes);
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 3), rows.len);
    for (rows, 0..) |row, i| {
        try std.testing.expectEqual(@as(?i64, d18_vals[i]), row.getColumn(0).?.asInt64());
        try std.testing.expectEqualSlices(u8, &d38_vals[i].bytes, row.getColumn(1).?.asBytes().?);
    }
}

test "round-trip optional Decimal (DynamicWriter)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("price", TypeInfo.forDecimal(9, 2), .{});
    try writer.begin();

    const expected = [_]?i32{ 12345, null, -99999, null, 0 };
    for (expected) |val| {
        if (val) |v| {
            try writer.setInt32(0, v);
        } else {
            try writer.setNull(0);
        }
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 5), rows.len);
    for (rows, 0..) |row, i| {
        if (expected[i]) |exp| {
            try std.testing.expectEqual(@as(?i32, exp), row.getColumn(0).?.asInt32());
        } else {
            try std.testing.expect(row.getColumn(0).?.isNull());
        }
    }
}

test "round-trip DynamicReader reads INT32-backed decimal columns (low-level writer)" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    {
        const file = try tmp_dir.dir.createFile("decimal_int32.parquet", .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            parquet.ColumnDef.decimal("price", 9, 2, true),
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        const values = [_]parquet.Optional(i32){
            .{ .value = 12345 },
            .{ .value = -99999 },
            .{ .null_value = {} },
            .{ .value = 0 },
            .{ .value = 1 },
        };
        try writer.writeColumnOptional(i32, 0, &values);
        try writer.close();
    }

    {
        const buf = try tmp_dir.dir.readFileAlloc(allocator, "decimal_int32.parquet", 1024 * 1024);
        defer allocator.free(buf);

        var reader = try parquet.openBufferDynamic(allocator, buf, .{});
        defer reader.deinit();

        const rows = try reader.readAllRows(0);
        defer deferFreeRows(allocator, rows);

        try std.testing.expectEqual(@as(usize, 5), rows.len);
        try std.testing.expectEqual(@as(?i32, 12345), rows[0].getColumn(0).?.asInt32());
        try std.testing.expectEqual(@as(?i32, -99999), rows[1].getColumn(0).?.asInt32());
        try std.testing.expect(rows[2].getColumn(0).?.isNull());
        try std.testing.expectEqual(@as(?i32, 0), rows[3].getColumn(0).?.asInt32());
        try std.testing.expectEqual(@as(?i32, 1), rows[4].getColumn(0).?.asInt32());
    }
}

test "round-trip DynamicReader reads INT64-backed decimal columns (low-level writer)" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

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

    {
        const buf = try tmp_dir.dir.readFileAlloc(allocator, "decimal_int64.parquet", 1024 * 1024);
        defer allocator.free(buf);

        var reader = try parquet.openBufferDynamic(allocator, buf, .{});
        defer reader.deinit();

        const rows = try reader.readAllRows(0);
        defer deferFreeRows(allocator, rows);

        try std.testing.expectEqual(@as(usize, 3), rows.len);
        try std.testing.expectEqual(@as(?i64, 123456789012345678), rows[0].getColumn(0).?.asInt64());
        try std.testing.expectEqual(@as(?i64, -1), rows[1].getColumn(0).?.asInt64());
        try std.testing.expectEqual(@as(?i64, 0), rows[2].getColumn(0).?.asInt64());
    }
}

test "round-trip list of i32 (DynamicWriter)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    const list_node = try writer.allocSchemaNode(.{ .list = try writer.allocSchemaNode(.{ .int32 = .{} }) });
    try writer.addColumnNested("items", list_node, .{});
    try writer.begin();

    // Row 0: [1, -2, 3]
    try writer.beginList(0);
    try writer.appendNestedValue(0, .{ .int32_val = 1 });
    try writer.appendNestedValue(0, .{ .int32_val = -2 });
    try writer.appendNestedValue(0, .{ .int32_val = 3 });
    try writer.endList(0);
    try writer.addRow();

    // Row 1: []
    try writer.beginList(0);
    try writer.endList(0);
    try writer.addRow();

    // Row 2: [42]
    try writer.beginList(0);
    try writer.appendNestedValue(0, .{ .int32_val = 42 });
    try writer.endList(0);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 3), rows.len);

    const list0 = rows[0].getColumn(0).?.asList().?;
    try std.testing.expectEqual(@as(usize, 3), list0.len);
    try std.testing.expectEqual(@as(?i32, 1), list0[0].asInt32());
    try std.testing.expectEqual(@as(?i32, -2), list0[1].asInt32());
    try std.testing.expectEqual(@as(?i32, 3), list0[2].asInt32());

    const col1 = rows[1].getColumn(0).?;
    if (col1.asList()) |l1| {
        try std.testing.expectEqual(@as(usize, 0), l1.len);
    } else {
        try std.testing.expectEqual(Value.null_val, col1);
    }

    const list2 = rows[2].getColumn(0).?.asList().?;
    try std.testing.expectEqual(@as(usize, 1), list2.len);
    try std.testing.expectEqual(@as(?i32, 42), list2[0].asInt32());
}

test "round-trip map<string, i32> (DynamicWriter)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    const key_node = try writer.allocSchemaNode(.{ .byte_array = .{ .logical = .string } });
    const val_node = try writer.allocSchemaNode(.{ .optional = try writer.allocSchemaNode(.{ .int32 = .{} }) });
    const map_inner = try writer.allocSchemaNode(.{ .map = .{ .key = key_node, .value = val_node } });
    const map_node = try writer.allocSchemaNode(.{ .optional = map_inner });
    try writer.addColumnNested("attrs", map_node, .{});
    try writer.begin();

    // Row 0: {a:1, b:2}
    try writer.beginMap(0);
    try writer.beginMapEntry(0);
    try writer.appendNestedBytes(0, "a");
    try writer.appendNestedValue(0, .{ .int32_val = 1 });
    try writer.endMapEntry(0);
    try writer.beginMapEntry(0);
    try writer.appendNestedBytes(0, "b");
    try writer.appendNestedValue(0, .{ .int32_val = 2 });
    try writer.endMapEntry(0);
    try writer.endMap(0);
    try writer.addRow();

    // Row 1: {c:3}
    try writer.beginMap(0);
    try writer.beginMapEntry(0);
    try writer.appendNestedBytes(0, "c");
    try writer.appendNestedValue(0, .{ .int32_val = 3 });
    try writer.endMapEntry(0);
    try writer.endMap(0);
    try writer.addRow();

    // Row 2: {} (empty map)
    try writer.beginMap(0);
    try writer.endMap(0);
    try writer.addRow();

    // Row 3: null
    try writer.setNull(0);
    try writer.addRow();

    // Row 4: {d:4, e:null, f:6}
    try writer.beginMap(0);
    try writer.beginMapEntry(0);
    try writer.appendNestedBytes(0, "d");
    try writer.appendNestedValue(0, .{ .int32_val = 4 });
    try writer.endMapEntry(0);
    try writer.beginMapEntry(0);
    try writer.appendNestedBytes(0, "e");
    try writer.appendNestedValue(0, .{ .null_val = {} });
    try writer.endMapEntry(0);
    try writer.beginMapEntry(0);
    try writer.appendNestedBytes(0, "f");
    try writer.appendNestedValue(0, .{ .int32_val = 6 });
    try writer.endMapEntry(0);
    try writer.endMap(0);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 5), rows.len);

    // Row 0: {a:1, b:2}
    const map0 = rows[0].getColumn(0).?.asMap().?;
    try std.testing.expectEqual(@as(usize, 2), map0.len);
    try std.testing.expectEqualStrings("a", map0[0].key.asBytes().?);
    try std.testing.expectEqual(@as(?i32, 1), map0[0].value.asInt32());
    try std.testing.expectEqualStrings("b", map0[1].key.asBytes().?);
    try std.testing.expectEqual(@as(?i32, 2), map0[1].value.asInt32());

    // Row 1: {c:3}
    const map1 = rows[1].getColumn(0).?.asMap().?;
    try std.testing.expectEqual(@as(usize, 1), map1.len);
    try std.testing.expectEqualStrings("c", map1[0].key.asBytes().?);
    try std.testing.expectEqual(@as(?i32, 3), map1[0].value.asInt32());

    // Row 2: {}
    const map2 = rows[2].getColumn(0).?.asMap().?;
    try std.testing.expectEqual(@as(usize, 0), map2.len);

    // Row 3: null
    try std.testing.expect(rows[3].getColumn(0).?.isNull());

    // Row 4: {d:4, e:null, f:6}
    const map4 = rows[4].getColumn(0).?.asMap().?;
    try std.testing.expectEqual(@as(usize, 3), map4.len);
    try std.testing.expectEqualStrings("d", map4[0].key.asBytes().?);
    try std.testing.expectEqual(@as(?i32, 4), map4[0].value.asInt32());
    try std.testing.expectEqualStrings("e", map4[1].key.asBytes().?);
    try std.testing.expect(map4[1].value.isNull());
    try std.testing.expectEqualStrings("f", map4[2].key.asBytes().?);
    try std.testing.expectEqual(@as(?i32, 6), map4[2].value.asInt32());
}

test "round-trip map<i32, i64> (DynamicWriter)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    const key_node = try writer.allocSchemaNode(.{ .int32 = .{} });
    const val_node = try writer.allocSchemaNode(.{ .optional = try writer.allocSchemaNode(.{ .int64 = .{} }) });
    const map_inner = try writer.allocSchemaNode(.{ .map = .{ .key = key_node, .value = val_node } });
    const map_node = try writer.allocSchemaNode(.{ .optional = map_inner });
    try writer.addColumnNested("lookup", map_node, .{});
    try writer.begin();

    // Row 0: {100:1000000, 200:null, 300:-999}
    try writer.beginMap(0);
    try writer.beginMapEntry(0);
    try writer.appendNestedValue(0, .{ .int32_val = 100 });
    try writer.appendNestedValue(0, .{ .int64_val = 1_000_000 });
    try writer.endMapEntry(0);
    try writer.beginMapEntry(0);
    try writer.appendNestedValue(0, .{ .int32_val = 200 });
    try writer.appendNestedValue(0, .{ .null_val = {} });
    try writer.endMapEntry(0);
    try writer.beginMapEntry(0);
    try writer.appendNestedValue(0, .{ .int32_val = 300 });
    try writer.appendNestedValue(0, .{ .int64_val = -999 });
    try writer.endMapEntry(0);
    try writer.endMap(0);
    try writer.addRow();

    // Row 1: {} (empty)
    try writer.beginMap(0);
    try writer.endMap(0);
    try writer.addRow();

    // Row 2: null
    try writer.setNull(0);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 3), rows.len);

    const map0 = rows[0].getColumn(0).?.asMap().?;
    try std.testing.expectEqual(@as(usize, 3), map0.len);
    try std.testing.expectEqual(@as(?i32, 100), map0[0].key.asInt32());
    try std.testing.expectEqual(@as(?i64, 1_000_000), map0[0].value.asInt64());
    try std.testing.expectEqual(@as(?i32, 200), map0[1].key.asInt32());
    try std.testing.expect(map0[1].value.isNull());
    try std.testing.expectEqual(@as(?i32, 300), map0[2].key.asInt32());
    try std.testing.expectEqual(@as(?i64, -999), map0[2].value.asInt64());

    const map1 = rows[1].getColumn(0).?.asMap().?;
    try std.testing.expectEqual(@as(usize, 0), map1.len);

    try std.testing.expect(rows[2].getColumn(0).?.isNull());
}

test "round-trip struct with bool and f32 fields (DynamicWriter)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", TypeInfo.int32, .{});
    const fields = try writer.allocSchemaFields(3);
    fields[0] = .{ .name = "flag", .node = try writer.allocSchemaNode(.{ .boolean = .{} }) };
    fields[1] = .{ .name = "score", .node = try writer.allocSchemaNode(.{ .float = .{} }) };
    fields[2] = .{ .name = "tag", .node = try writer.allocSchemaNode(.{ .int32 = .{} }) };
    const struct_node = try writer.allocSchemaNode(.{ .struct_ = .{ .fields = fields } });
    try writer.addColumnNested("data", struct_node, .{});
    try writer.begin();

    try writer.setInt32(0, 1);
    try writer.beginStruct(1);
    try writer.setStructField(1, 0, .{ .bool_val = true });
    try writer.setStructField(1, 1, .{ .float_val = 3.14 });
    try writer.setStructField(1, 2, .{ .int32_val = 10 });
    try writer.endStruct(1);
    try writer.addRow();

    try writer.setInt32(0, 2);
    try writer.beginStruct(1);
    try writer.setStructField(1, 0, .{ .bool_val = false });
    try writer.setStructField(1, 1, .{ .float_val = -0.5 });
    try writer.setStructField(1, 2, .{ .int32_val = 20 });
    try writer.endStruct(1);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 2), rows.len);

    try std.testing.expectEqual(@as(?i32, 1), rows[0].getColumn(0).?.asInt32());
    const s0 = rows[0].getColumn(1).?.asStruct().?;
    try std.testing.expectEqual(@as(?bool, true), s0[0].value.asBool());
    try std.testing.expectApproxEqAbs(@as(f32, 3.14), s0[1].value.asFloat().?, 0.01);
    try std.testing.expectEqual(@as(?i32, 10), s0[2].value.asInt32());

    try std.testing.expectEqual(@as(?i32, 2), rows[1].getColumn(0).?.asInt32());
    const s1 = rows[1].getColumn(1).?.asStruct().?;
    try std.testing.expectEqual(@as(?bool, false), s1[0].value.asBool());
    try std.testing.expectApproxEqAbs(@as(f32, -0.5), s1[1].value.asFloat().?, 0.01);
    try std.testing.expectEqual(@as(?i32, 20), s1[2].value.asInt32());
}

test "round-trip flat f16 column (DynamicWriter)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("val", TypeInfo.float16, .{});
    try writer.begin();

    const vals = [_]f16{ 1.0, -2.25, 0.0, 65504.0 };
    for (vals) |v| {
        const bytes: [2]u8 = @bitCast(v);
        try writer.setBytes(0, &bytes);
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 4), rows.len);
    for (rows, 0..) |row, i| {
        const bytes = row.getColumn(0).?.asBytes().?;
        try std.testing.expectEqual(@as(usize, 2), bytes.len);
        const got: f16 = @bitCast(bytes[0..2].*);
        try std.testing.expectEqual(vals[i], got);
    }
}

test "round-trip optional f16 column (DynamicWriter)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("val", TypeInfo.float16, .{});
    try writer.begin();

    const vals = [_]?f16{ 1.5, null, -0.5, null };
    for (vals) |v| {
        if (v) |f| {
            const bytes: [2]u8 = @bitCast(f);
            try writer.setBytes(0, &bytes);
        } else {
            try writer.setNull(0);
        }
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 4), rows.len);
    for (rows, 0..) |row, i| {
        if (vals[i]) |exp| {
            const bytes = row.getColumn(0).?.asBytes().?;
            const got: f16 = @bitCast(bytes[0..2].*);
            try std.testing.expectEqual(exp, got);
        } else {
            try std.testing.expect(row.getColumn(0).?.isNull());
        }
    }
}

// =============================================================================
// Statistics round-trip tests for logical types (low-level Writer/Reader)
// =============================================================================

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

    {
        const file = try tmp_dir.dir.openFile("stats_int32_logical.parquet", .{});
        defer file.close();

        var reader = try parquet.openFileDynamic(allocator, file, .{});
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

    {
        const file = try tmp_dir.dir.openFile("stats_int64_logical.parquet", .{});
        defer file.close();

        var reader = try parquet.openFileDynamic(allocator, file, .{});
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

    const uuid_a = [_]u8{ 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF };
    const uuid_b = [_]u8{ 0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00 };
    const uuid_c = [_]u8{ 0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90, 0xA0, 0xB0, 0xC0, 0xD0, 0xE0, 0xF0, 0x01 };
    const uuid_values = [_][]const u8{ &uuid_a, &uuid_b, &uuid_c };
    const uuid_expected_min = uuid_a;
    const uuid_expected_max = uuid_b;

    const f16_half: f16 = 0.5;
    const f16_one: f16 = 1.0;
    const f16_three: f16 = 3.0;
    const f16_bytes_half: [2]u8 = @bitCast(f16_half);
    const f16_bytes_one: [2]u8 = @bitCast(f16_one);
    const f16_bytes_three: [2]u8 = @bitCast(f16_three);
    const f16_values = [_][]const u8{ &f16_bytes_one, &f16_bytes_half, &f16_bytes_three };

    const dec_small = [_]u8{0} ** 15 ++ [_]u8{1};
    const dec_medium = [_]u8{0} ** 15 ++ [_]u8{100};
    const dec_large = [_]u8{0} ** 14 ++ [_]u8{ 1, 0 };
    const dec_values = [_][]const u8{ &dec_medium, &dec_small, &dec_large };
    const dec_expected_min = dec_small;
    const dec_expected_max = dec_large;

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

    {
        const file = try tmp_dir.dir.openFile("stats_fixed_logical.parquet", .{});
        defer file.close();

        var reader = try parquet.openFileDynamic(allocator, file, .{});
        defer reader.deinit();

        {
            const stats = reader.getColumnStatistics(0, 0);
            try std.testing.expect(stats != null);
            const s = stats.?;
            try std.testing.expectEqualSlices(u8, &uuid_expected_min, s.getMinBytes().?);
            try std.testing.expectEqualSlices(u8, &uuid_expected_max, s.getMaxBytes().?);
            try std.testing.expectEqual(@as(i64, 0), s.null_count.?);
        }

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

// =============================================================================
// WriteTarget external target round-trip tests
// =============================================================================

test "Writer.initWithTarget round-trip via BufferTarget" {
    const allocator = std.testing.allocator;

    const columns = [_]parquet.ColumnDef{
        .{ .name = "id", .type_ = .int64, .optional = false },
        .{ .name = "name", .type_ = .byte_array, .optional = true },
    };

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

    const buffer = bt.written();
    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 1), reader.metadata.row_groups.len);
    try std.testing.expectEqual(@as(i64, 3), reader.metadata.row_groups[0].num_rows);

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }
    try std.testing.expectEqual(@as(usize, 3), rows.len);
    try std.testing.expectEqual(@as(?i64, 10), rows[0].getColumn(0).?.asInt64());
    try std.testing.expectEqual(@as(?i64, 20), rows[1].getColumn(0).?.asInt64());
    try std.testing.expectEqual(@as(?i64, 30), rows[2].getColumn(0).?.asInt64());

    try std.testing.expectEqualStrings("alice", rows[0].getColumn(1).?.asBytes().?);
    try std.testing.expect(rows[1].getColumn(1).?.isNull());
    try std.testing.expectEqualStrings("charlie", rows[2].getColumn(1).?.asBytes().?);
}

test "DynamicWriter.initWithTarget round-trip via BufferTarget" {
    const allocator = std.testing.allocator;

    var bt = parquet.io.BufferTarget.init(allocator);
    defer bt.deinit();

    var writer = parquet.DynamicWriter.init(allocator, bt.target());
    defer writer.deinit();

    try writer.addColumn("sensor_id", TypeInfo.int32, .{});
    try writer.addColumn("value", TypeInfo.double_, .{});
    try writer.addColumn("label", TypeInfo.string, .{});
    try writer.begin();

    try writer.setInt32(0, 1);
    try writer.setDouble(1, 23.5);
    try writer.setBytes(2, "temp");
    try writer.addRow();

    try writer.setInt32(0, 2);
    try writer.setDouble(1, 98.1);
    try writer.setBytes(2, "humidity");
    try writer.addRow();

    try writer.setInt32(0, 3);
    try writer.setDouble(1, 1013.25);
    try writer.setBytes(2, "pressure");
    try writer.addRow();

    try writer.close();

    const buffer = bt.written();
    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 3), rows.len);

    try std.testing.expectEqual(@as(?i32, 1), rows[0].getColumn(0).?.asInt32());
    try std.testing.expectApproxEqAbs(@as(f64, 23.5), rows[0].getColumn(1).?.asDouble().?, 0.001);
    try std.testing.expectEqualStrings("temp", rows[0].getColumn(2).?.asBytes().?);

    try std.testing.expectEqual(@as(?i32, 2), rows[1].getColumn(0).?.asInt32());
    try std.testing.expectApproxEqAbs(@as(f64, 98.1), rows[1].getColumn(1).?.asDouble().?, 0.001);
    try std.testing.expectEqualStrings("humidity", rows[1].getColumn(2).?.asBytes().?);

    try std.testing.expectEqual(@as(?i32, 3), rows[2].getColumn(0).?.asInt32());
    try std.testing.expectApproxEqAbs(@as(f64, 1013.25), rows[2].getColumn(1).?.asDouble().?, 0.001);
    try std.testing.expectEqualStrings("pressure", rows[2].getColumn(2).?.asBytes().?);
}

test "round-trip nested list of i32 (DynamicWriter)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    const inner_list = try writer.allocSchemaNode(.{ .list = try writer.allocSchemaNode(.{ .int32 = .{} }) });
    const outer_list = try writer.allocSchemaNode(.{ .list = inner_list });
    try writer.addColumnNested("values", outer_list, .{});
    try writer.begin();

    // Row 0: [[1, -2, 3], [42]]
    try writer.beginList(0);
    try writer.appendNestedValue(0, .{ .list_val = &.{
        .{ .int32_val = 1 },
        .{ .int32_val = -2 },
        .{ .int32_val = 3 },
    } });
    try writer.appendNestedValue(0, .{ .list_val = &.{
        .{ .int32_val = 42 },
    } });
    try writer.endList(0);
    try writer.addRow();

    // Row 1: [[]]
    try writer.beginList(0);
    try writer.appendNestedValue(0, .{ .list_val = &.{} });
    try writer.endList(0);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 2), rows.len);

    const outer0 = rows[0].getColumn(0).?.asList().?;
    try std.testing.expectEqual(@as(usize, 2), outer0.len);
    const inner0_0 = outer0[0].asList().?;
    try std.testing.expectEqual(@as(usize, 3), inner0_0.len);
    try std.testing.expectEqual(@as(?i32, 1), inner0_0[0].asInt32());
    try std.testing.expectEqual(@as(?i32, -2), inner0_0[1].asInt32());
    try std.testing.expectEqual(@as(?i32, 3), inner0_0[2].asInt32());
    const inner0_1 = outer0[1].asList().?;
    try std.testing.expectEqual(@as(usize, 1), inner0_1.len);
    try std.testing.expectEqual(@as(?i32, 42), inner0_1[0].asInt32());

    const outer1 = rows[1].getColumn(0).?.asList().?;
    try std.testing.expectEqual(@as(usize, 1), outer1.len);
    if (outer1[0].asList()) |inner| {
        try std.testing.expectEqual(@as(usize, 0), inner.len);
    } else {
        try std.testing.expectEqual(Value.null_val, outer1[0]);
    }
}

test "round-trip optional list of i32 (DynamicWriter)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    const list_inner = try writer.allocSchemaNode(.{ .list = try writer.allocSchemaNode(.{ .int32 = .{} }) });
    const opt_list = try writer.allocSchemaNode(.{ .optional = list_inner });
    try writer.addColumnNested("ints", opt_list, .{});
    try writer.begin();

    // Row 0: [1, -2, 3]
    try writer.beginList(0);
    try writer.appendNestedValue(0, .{ .int32_val = 1 });
    try writer.appendNestedValue(0, .{ .int32_val = -2 });
    try writer.appendNestedValue(0, .{ .int32_val = 3 });
    try writer.endList(0);
    try writer.addRow();

    // Row 1: null
    try writer.setNull(0);
    try writer.addRow();

    // Row 2: []
    try writer.beginList(0);
    try writer.endList(0);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 3), rows.len);

    const list0 = rows[0].getColumn(0).?.asList().?;
    try std.testing.expectEqual(@as(usize, 3), list0.len);
    try std.testing.expectEqual(@as(?i32, 1), list0[0].asInt32());

    try std.testing.expect(rows[1].getColumn(0).?.isNull());

    const col2 = rows[2].getColumn(0).?;
    if (col2.asList()) |l2| {
        try std.testing.expectEqual(@as(usize, 0), l2.len);
    } else {
        try std.testing.expectEqual(Value.null_val, col2);
    }
}

test "round-trip optional nested list (DynamicWriter)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    const inner = try writer.allocSchemaNode(.{ .list = try writer.allocSchemaNode(.{ .int32 = .{} }) });
    const outer = try writer.allocSchemaNode(.{ .list = inner });
    const opt = try writer.allocSchemaNode(.{ .optional = outer });
    try writer.addColumnNested("values", opt, .{});
    try writer.begin();

    // Row 0: [[1,2,3],[42]]
    try writer.beginList(0);
    try writer.appendNestedValue(0, .{ .list_val = &.{
        .{ .int32_val = 1 },
        .{ .int32_val = 2 },
        .{ .int32_val = 3 },
    } });
    try writer.appendNestedValue(0, .{ .list_val = &.{
        .{ .int32_val = 42 },
    } });
    try writer.endList(0);
    try writer.addRow();

    // Row 1: null
    try writer.setNull(0);
    try writer.addRow();

    // Row 2: [[]]
    try writer.beginList(0);
    try writer.appendNestedValue(0, .{ .list_val = &.{} });
    try writer.endList(0);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 3), rows.len);

    const outer0 = rows[0].getColumn(0).?.asList().?;
    try std.testing.expectEqual(@as(usize, 2), outer0.len);
    const inner0_0 = outer0[0].asList().?;
    try std.testing.expectEqual(@as(usize, 3), inner0_0.len);
    try std.testing.expectEqual(@as(?i32, 1), inner0_0[0].asInt32());
    try std.testing.expectEqual(@as(?i32, 2), inner0_0[1].asInt32());
    try std.testing.expectEqual(@as(?i32, 3), inner0_0[2].asInt32());

    try std.testing.expect(rows[1].getColumn(0).?.isNull());

    const outer2 = rows[2].getColumn(0).?.asList().?;
    try std.testing.expectEqual(@as(usize, 1), outer2.len);
    if (outer2[0].asList()) |empty_inner| {
        try std.testing.expectEqual(@as(usize, 0), empty_inner.len);
    } else {
        try std.testing.expectEqual(Value.null_val, outer2[0]);
    }
}

test "round-trip struct with per-leaf path properties (DynamicWriter)" {
    if (!build_options.enable_snappy or !build_options.enable_zstd) return;
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", TypeInfo.int32, .{});
    const fields = try writer.allocSchemaFields(2);
    fields[0] = .{ .name = "city", .node = try writer.allocSchemaNode(.{ .byte_array = .{ .logical = .string } }) };
    fields[1] = .{ .name = "zip", .node = try writer.allocSchemaNode(.{ .int32 = .{} }) };
    const struct_node = try writer.allocSchemaNode(.{ .struct_ = .{ .fields = fields } });
    try writer.addColumnNested("address", struct_node, .{});

    writer.setCompression(.snappy);
    try writer.setPathProperties("address.city", .{ .compression = .zstd });
    try writer.setPathProperties("address.zip", .{ .use_dictionary = false });

    try writer.begin();

    try writer.setInt32(0, 1);
    try writer.beginStruct(1);
    try writer.setStructFieldBytes(1, 0, "New York");
    try writer.setStructField(1, 1, .{ .int32_val = 10001 });
    try writer.endStruct(1);
    try writer.addRow();

    try writer.setInt32(0, 2);
    try writer.beginStruct(1);
    try writer.setStructFieldBytes(1, 0, "San Francisco");
    try writer.setStructField(1, 1, .{ .int32_val = 94102 });
    try writer.endStruct(1);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 2), rows.len);
    try std.testing.expectEqual(@as(?i32, 1), rows[0].getColumn(0).?.asInt32());
    const s0 = rows[0].getColumn(1).?.asStruct().?;
    try std.testing.expectEqualStrings("New York", s0[0].value.asBytes().?);
    try std.testing.expectEqual(@as(?i32, 10001), s0[1].value.asInt32());

    try std.testing.expectEqual(@as(?i32, 2), rows[1].getColumn(0).?.asInt32());
    const s1 = rows[1].getColumn(1).?.asStruct().?;
    try std.testing.expectEqualStrings("San Francisco", s1[0].value.asBytes().?);
    try std.testing.expectEqual(@as(?i32, 94102), s1[1].value.asInt32());
}

test "path properties with list column (items.list.element)" {
    if (!build_options.enable_zstd) return;
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    const list_node = try writer.allocSchemaNode(.{ .list = try writer.allocSchemaNode(.{ .int32 = .{} }) });
    try writer.addColumnNested("items", list_node, .{});

    writer.setCompression(.uncompressed);
    try writer.setPathProperties("items.list.element", .{ .compression = .zstd });

    try writer.begin();

    try writer.beginList(0);
    try writer.appendNestedValue(0, .{ .int32_val = 10 });
    try writer.appendNestedValue(0, .{ .int32_val = 20 });
    try writer.endList(0);
    try writer.addRow();

    try writer.beginList(0);
    try writer.appendNestedValue(0, .{ .int32_val = 30 });
    try writer.endList(0);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    // Verify the column chunk metadata records zstd compression
    const rg = reader.metadata.row_groups[0];
    try std.testing.expectEqual(@as(usize, 1), rg.columns.len);
    const md = rg.columns[0].meta_data.?;
    try std.testing.expectEqual(parquet.CompressionCodec.zstd, md.codec);

    // Verify data roundtrips correctly
    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 2), rows.len);
    const list0 = rows[0].getColumn(0).?.asList().?;
    try std.testing.expectEqual(@as(usize, 2), list0.len);
    try std.testing.expectEqual(@as(?i32, 10), list0[0].asInt32());
    try std.testing.expectEqual(@as(?i32, 20), list0[1].asInt32());

    const list1 = rows[1].getColumn(0).?.asList().?;
    try std.testing.expectEqual(@as(usize, 1), list1.len);
    try std.testing.expectEqual(@as(?i32, 30), list1[0].asInt32());
}

test "path properties override column-level properties" {
    if (!build_options.enable_snappy or !build_options.enable_zstd) return;
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", TypeInfo.int32, .{});
    const fields = try writer.allocSchemaFields(2);
    fields[0] = .{ .name = "name", .node = try writer.allocSchemaNode(.{ .byte_array = .{ .logical = .string } }) };
    fields[1] = .{ .name = "code", .node = try writer.allocSchemaNode(.{ .int32 = .{} }) };
    const struct_node = try writer.allocSchemaNode(.{ .struct_ = .{ .fields = fields } });
    // Column-level: snappy
    try writer.addColumnNested("data", struct_node, .{ .compression = .snappy });

    // Path-level: override data.name to zstd, data.code inherits column-level snappy
    try writer.setPathProperties("data.name", .{ .compression = .zstd });

    try writer.begin();

    try writer.setInt32(0, 1);
    try writer.beginStruct(1);
    try writer.setStructFieldBytes(1, 0, "hello");
    try writer.setStructField(1, 1, .{ .int32_val = 42 });
    try writer.endStruct(1);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rg = reader.metadata.row_groups[0];
    // 3 column chunks: id, data.name, data.code
    try std.testing.expectEqual(@as(usize, 3), rg.columns.len);

    // id: global default (uncompressed)
    try std.testing.expectEqual(parquet.CompressionCodec.uncompressed, rg.columns[0].meta_data.?.codec);
    // data.name: path override -> zstd
    try std.testing.expectEqual(parquet.CompressionCodec.zstd, rg.columns[1].meta_data.?.codec);
    // data.code: column-level -> snappy
    try std.testing.expectEqual(parquet.CompressionCodec.snappy, rg.columns[2].meta_data.?.codec);

    // Verify data roundtrips
    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqual(@as(?i32, 1), rows[0].getColumn(0).?.asInt32());
    const s0 = rows[0].getColumn(1).?.asStruct().?;
    try std.testing.expectEqualStrings("hello", s0[0].value.asBytes().?);
    try std.testing.expectEqual(@as(?i32, 42), s0[1].value.asInt32());
}

test "path properties with non-matching path are silently ignored" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", TypeInfo.int32, .{});
    const fields = try writer.allocSchemaFields(1);
    fields[0] = .{ .name = "name", .node = try writer.allocSchemaNode(.{ .byte_array = .{ .logical = .string } }) };
    const struct_node = try writer.allocSchemaNode(.{ .struct_ = .{ .fields = fields } });
    try writer.addColumnNested("info", struct_node, .{});

    // Set path properties for a path that doesn't exist in the schema
    try writer.setPathProperties("info.nonexistent", .{ .compression = .zstd });
    try writer.setPathProperties("totally.wrong.path", .{ .compression = .gzip });

    try writer.begin();

    try writer.setInt32(0, 1);
    try writer.beginStruct(1);
    try writer.setStructFieldBytes(1, 0, "test");
    try writer.endStruct(1);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    // Should be able to read the file without issues
    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rg = reader.metadata.row_groups[0];
    // Non-matching paths should not affect anything: both columns get default (uncompressed)
    try std.testing.expectEqual(parquet.CompressionCodec.uncompressed, rg.columns[0].meta_data.?.codec);
    try std.testing.expectEqual(parquet.CompressionCodec.uncompressed, rg.columns[1].meta_data.?.codec);

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqual(@as(?i32, 1), rows[0].getColumn(0).?.asInt32());
    try std.testing.expectEqualStrings("test", rows[0].getColumn(1).?.asStruct().?[0].value.asBytes().?);
}

test "per-leaf compression verified in file metadata" {
    if (!build_options.enable_gzip or !build_options.enable_snappy or !build_options.enable_zstd) return;
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("x", TypeInfo.int32, .{ .compression = .gzip });
    try writer.addColumn("y", TypeInfo.float_, .{ .compression = .snappy });
    try writer.addColumn("z", TypeInfo.double_, .{});

    writer.setCompression(.zstd);

    try writer.begin();

    try writer.setInt32(0, 1);
    try writer.setFloat(1, 2.5);
    try writer.setDouble(2, 3.14);
    try writer.addRow();

    try writer.setInt32(0, 4);
    try writer.setFloat(1, 5.5);
    try writer.setDouble(2, 6.28);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rg = reader.metadata.row_groups[0];
    try std.testing.expectEqual(@as(usize, 3), rg.columns.len);

    // x: column-level gzip overrides global zstd
    try std.testing.expectEqual(parquet.CompressionCodec.gzip, rg.columns[0].meta_data.?.codec);
    // y: column-level snappy overrides global zstd
    try std.testing.expectEqual(parquet.CompressionCodec.snappy, rg.columns[1].meta_data.?.codec);
    // z: no column-level override, inherits global zstd
    try std.testing.expectEqual(parquet.CompressionCodec.zstd, rg.columns[2].meta_data.?.codec);

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 2), rows.len);
    try std.testing.expectEqual(@as(?i32, 1), rows[0].getColumn(0).?.asInt32());
    try std.testing.expectEqual(@as(?f32, 2.5), rows[0].getColumn(1).?.asFloat());
    try std.testing.expectEqual(@as(?f64, 3.14), rows[0].getColumn(2).?.asDouble());
}

test "nested struct leaf with dictionary encoding via path properties" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", TypeInfo.int32, .{});
    const fields = try writer.allocSchemaFields(1);
    fields[0] = .{ .name = "city", .node = try writer.allocSchemaNode(.{ .byte_array = .{ .logical = .string } }) };
    const struct_node = try writer.allocSchemaNode(.{ .struct_ = .{ .fields = fields } });
    try writer.addColumnNested("addr", struct_node, .{});

    writer.setUseDictionary(false);
    try writer.setPathProperties("addr.city", .{ .use_dictionary = true });

    try writer.begin();

    try writer.setInt32(0, 1);
    try writer.beginStruct(1);
    try writer.setStructFieldBytes(1, 0, "NYC");
    try writer.endStruct(1);
    try writer.addRow();

    try writer.setInt32(0, 2);
    try writer.beginStruct(1);
    try writer.setStructFieldBytes(1, 0, "LA");
    try writer.endStruct(1);
    try writer.addRow();

    try writer.setInt32(0, 3);
    try writer.beginStruct(1);
    try writer.setStructFieldBytes(1, 0, "NYC");
    try writer.endStruct(1);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rg = reader.metadata.row_groups[0];
    // id (flat) + addr.city (nested leaf) = 2 column chunks
    try std.testing.expectEqual(@as(usize, 2), rg.columns.len);

    // addr.city should have dictionary page offset set
    const city_meta = rg.columns[1].meta_data.?;
    try std.testing.expect(city_meta.dictionary_page_offset != null);

    // Verify rle_dictionary in encodings
    var has_rle_dict = false;
    for (city_meta.encodings) |enc| {
        if (enc == .rle_dictionary) has_rle_dict = true;
    }
    try std.testing.expect(has_rle_dict);

    // Verify data roundtrips
    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 3), rows.len);
    try std.testing.expectEqualStrings("NYC", rows[0].getColumn(1).?.asStruct().?[0].value.asBytes().?);
    try std.testing.expectEqualStrings("LA", rows[1].getColumn(1).?.asStruct().?[0].value.asBytes().?);
    try std.testing.expectEqualStrings("NYC", rows[2].getColumn(1).?.asStruct().?[0].value.asBytes().?);
}

test "nested struct int leaf with delta encoding via path properties" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    const fields = try writer.allocSchemaFields(2);
    fields[0] = .{ .name = "name", .node = try writer.allocSchemaNode(.{ .byte_array = .{ .logical = .string } }) };
    fields[1] = .{ .name = "score", .node = try writer.allocSchemaNode(.{ .int32 = .{} }) };
    const struct_node = try writer.allocSchemaNode(.{ .struct_ = .{ .fields = fields } });
    try writer.addColumnNested("data", struct_node, .{});

    writer.setUseDictionary(false);
    try writer.setPathProperties("data.score", .{ .encoding = .delta_binary_packed });

    try writer.begin();

    try writer.beginStruct(0);
    try writer.setStructFieldBytes(0, 0, "alice");
    try writer.setStructField(0, 1, .{ .int32_val = 100 });
    try writer.endStruct(0);
    try writer.addRow();

    try writer.beginStruct(0);
    try writer.setStructFieldBytes(0, 0, "bob");
    try writer.setStructField(0, 1, .{ .int32_val = 200 });
    try writer.endStruct(0);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rg = reader.metadata.row_groups[0];
    // data.name + data.score = 2 column chunks
    try std.testing.expectEqual(@as(usize, 2), rg.columns.len);

    // data.score should use delta_binary_packed encoding
    const score_meta = rg.columns[1].meta_data.?;
    var has_delta = false;
    for (score_meta.encodings) |enc| {
        if (enc == .delta_binary_packed) has_delta = true;
    }
    try std.testing.expect(has_delta);

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 2), rows.len);
    const s0 = rows[0].getColumn(0).?.asStruct().?;
    try std.testing.expectEqualStrings("alice", s0[0].value.asBytes().?);
    try std.testing.expectEqual(@as(?i32, 100), s0[1].value.asInt32());

    const s1 = rows[1].getColumn(0).?.asStruct().?;
    try std.testing.expectEqualStrings("bob", s1[0].value.asBytes().?);
    try std.testing.expectEqual(@as(?i32, 200), s1[1].value.asInt32());
}

test "nested list byte_array with dictionary encoding (default)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    const list_node = try writer.allocSchemaNode(.{ .list = try writer.allocSchemaNode(.{ .byte_array = .{ .logical = .string } }) });
    try writer.addColumnNested("tags", list_node, .{});

    // DynamicWriter defaults to use_dictionary=true, which now applies to nested columns
    try writer.begin();

    try writer.beginList(0);
    try writer.appendNestedBytes(0, "red");
    try writer.appendNestedBytes(0, "blue");
    try writer.endList(0);
    try writer.addRow();

    try writer.beginList(0);
    try writer.appendNestedBytes(0, "red");
    try writer.appendNestedBytes(0, "green");
    try writer.endList(0);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rg = reader.metadata.row_groups[0];
    try std.testing.expectEqual(@as(usize, 1), rg.columns.len);

    // Should have dictionary page offset set (default use_dictionary=true)
    const tags_meta = rg.columns[0].meta_data.?;
    try std.testing.expect(tags_meta.dictionary_page_offset != null);

    var has_rle_dict = false;
    for (tags_meta.encodings) |enc| {
        if (enc == .rle_dictionary) has_rle_dict = true;
    }
    try std.testing.expect(has_rle_dict);

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 2), rows.len);

    const list0 = rows[0].getColumn(0).?.asList().?;
    try std.testing.expectEqual(@as(usize, 2), list0.len);
    try std.testing.expectEqualStrings("red", list0[0].asBytes().?);
    try std.testing.expectEqualStrings("blue", list0[1].asBytes().?);

    const list1 = rows[1].getColumn(0).?.asList().?;
    try std.testing.expectEqual(@as(usize, 2), list1.len);
    try std.testing.expectEqualStrings("red", list1[0].asBytes().?);
    try std.testing.expectEqualStrings("green", list1[1].asBytes().?);
}

test "nested struct leaf columns have statistics in metadata" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    const fields = try writer.allocSchemaFields(2);
    fields[0] = .{ .name = "name", .node = try writer.allocSchemaNode(.{ .byte_array = .{ .logical = .string } }) };
    fields[1] = .{ .name = "score", .node = try writer.allocSchemaNode(.{ .int32 = .{} }) };
    const struct_node = try writer.allocSchemaNode(.{ .struct_ = .{ .fields = fields } });
    try writer.addColumnNested("data", struct_node, .{});

    writer.setUseDictionary(false);
    try writer.begin();

    try writer.beginStruct(0);
    try writer.setStructFieldBytes(0, 0, "alice");
    try writer.setStructField(0, 1, .{ .int32_val = 10 });
    try writer.endStruct(0);
    try writer.addRow();

    try writer.beginStruct(0);
    try writer.setStructFieldBytes(0, 0, "charlie");
    try writer.setStructField(0, 1, .{ .int32_val = 30 });
    try writer.endStruct(0);
    try writer.addRow();

    try writer.beginStruct(0);
    try writer.setStructFieldBytes(0, 0, "bob");
    try writer.setStructField(0, 1, .{ .int32_val = 20 });
    try writer.endStruct(0);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rg = reader.metadata.row_groups[0];
    try std.testing.expectEqual(@as(usize, 2), rg.columns.len);

    // data.name (byte_array leaf) should have statistics
    const name_stats = rg.columns[0].meta_data.?.statistics.?;
    try std.testing.expectEqualStrings("alice", name_stats.min_value.?);
    try std.testing.expectEqualStrings("charlie", name_stats.max_value.?);
    try std.testing.expectEqual(@as(i64, 0), name_stats.null_count.?);

    // data.score (int32 leaf) should have statistics
    const score_stats = rg.columns[1].meta_data.?.statistics.?;
    const min_i32 = std.mem.readInt(i32, score_stats.min_value.?[0..4], .little);
    const max_i32 = std.mem.readInt(i32, score_stats.max_value.?[0..4], .little);
    try std.testing.expectEqual(@as(i32, 10), min_i32);
    try std.testing.expectEqual(@as(i32, 30), max_i32);
    try std.testing.expectEqual(@as(i64, 0), score_stats.null_count.?);
}

test "nested struct leaf statistics include null count" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    const fields = try writer.allocSchemaFields(1);
    fields[0] = .{ .name = "val", .node = try writer.allocSchemaNode(.{ .int32 = .{} }) };
    const struct_node = try writer.allocSchemaNode(.{ .struct_ = .{ .fields = fields } });
    try writer.addColumnNested("item", struct_node, .{});

    writer.setUseDictionary(false);
    try writer.begin();

    try writer.beginStruct(0);
    try writer.setStructField(0, 0, .{ .int32_val = 5 });
    try writer.endStruct(0);
    try writer.addRow();

    // Null struct row -- leaf gets null via def_level
    try writer.setNull(0);
    try writer.addRow();

    try writer.beginStruct(0);
    try writer.setStructField(0, 0, .{ .int32_val = 15 });
    try writer.endStruct(0);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rg = reader.metadata.row_groups[0];
    const stats = rg.columns[0].meta_data.?.statistics.?;
    const min_i32 = std.mem.readInt(i32, stats.min_value.?[0..4], .little);
    const max_i32 = std.mem.readInt(i32, stats.max_value.?[0..4], .little);
    try std.testing.expectEqual(@as(i32, 5), min_i32);
    try std.testing.expectEqual(@as(i32, 15), max_i32);
    try std.testing.expect(stats.null_count.? >= 1);
}

test "nested list leaf has statistics" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    const elem = try writer.allocSchemaNode(.{ .int32 = .{} });
    const list_node = try writer.allocSchemaNode(.{ .list = elem });
    try writer.addColumnNested("scores", list_node, .{});

    writer.setUseDictionary(false);
    try writer.begin();

    try writer.beginList(0);
    try writer.appendNestedValue(0, .{ .int32_val = 100 });
    try writer.appendNestedValue(0, .{ .int32_val = 50 });
    try writer.appendNestedValue(0, .{ .int32_val = 200 });
    try writer.endList(0);
    try writer.addRow();

    try writer.beginList(0);
    try writer.appendNestedValue(0, .{ .int32_val = 75 });
    try writer.endList(0);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rg = reader.metadata.row_groups[0];
    const stats = rg.columns[0].meta_data.?.statistics.?;
    const min_i32 = std.mem.readInt(i32, stats.min_value.?[0..4], .little);
    const max_i32 = std.mem.readInt(i32, stats.max_value.?[0..4], .little);
    try std.testing.expectEqual(@as(i32, 50), min_i32);
    try std.testing.expectEqual(@as(i32, 200), max_i32);
}

test "nested dict-encoded leaf has statistics" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    const fields = try writer.allocSchemaFields(1);
    fields[0] = .{ .name = "city", .node = try writer.allocSchemaNode(.{ .byte_array = .{ .logical = .string } }) };
    const struct_node = try writer.allocSchemaNode(.{ .struct_ = .{ .fields = fields } });
    try writer.addColumnNested("loc", struct_node, .{});

    try writer.setPathProperties("loc.city", .{ .use_dictionary = true });
    try writer.begin();

    try writer.beginStruct(0);
    try writer.setStructFieldBytes(0, 0, "NYC");
    try writer.endStruct(0);
    try writer.addRow();

    try writer.beginStruct(0);
    try writer.setStructFieldBytes(0, 0, "LA");
    try writer.endStruct(0);
    try writer.addRow();

    try writer.beginStruct(0);
    try writer.setStructFieldBytes(0, 0, "NYC");
    try writer.endStruct(0);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rg = reader.metadata.row_groups[0];
    const stats = rg.columns[0].meta_data.?.statistics.?;
    try std.testing.expectEqualStrings("LA", stats.min_value.?);
    try std.testing.expectEqualStrings("NYC", stats.max_value.?);
    try std.testing.expectEqual(@as(i64, 0), stats.null_count.?);
}

test "addColumn rejects duplicate column names" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", TypeInfo.int32, .{});
    try writer.addColumn("name", TypeInfo.string, .{});

    const result = writer.addColumn("id", TypeInfo.int64, .{});
    try std.testing.expectError(error.DuplicateColumnName, result);
}

test "addColumnNested rejects duplicate column names" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("data", TypeInfo.int32, .{});

    const fields = try writer.allocSchemaFields(1);
    fields[0] = .{ .name = "x", .node = try writer.allocSchemaNode(.{ .int32 = .{} }) };
    const struct_node = try writer.allocSchemaNode(.{ .struct_ = .{ .fields = fields } });

    const result = writer.addColumnNested("data", struct_node, .{});
    try std.testing.expectError(error.DuplicateColumnName, result);
}

test "setBytes validates fixed-length byte array length" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("uid", TypeInfo.uuid, .{});
    try writer.begin();

    // UUID expects exactly 16 bytes
    try std.testing.expectError(error.InvalidFixedLength, writer.setBytes(0, "too short"));
    try std.testing.expectError(error.InvalidFixedLength, writer.setBytes(0, "this is way too long for a uuid field"));

    // Correct length should succeed
    try writer.setBytes(0, "0123456789abcdef");
}

// =============================================================================
// Column projection tests
// =============================================================================

test "readRowsProjected reads subset of flat columns" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", TypeInfo.int32, .{});
    try writer.addColumn("name", TypeInfo.string, .{});
    try writer.addColumn("score", TypeInfo.double_, .{});
    try writer.addColumn("active", TypeInfo.bool_, .{});
    try writer.begin();

    try writer.setInt32(0, 1);
    try writer.setBytes(1, "alice");
    try writer.setDouble(2, 95.5);
    try writer.setBool(3, true);
    try writer.addRow();

    try writer.setInt32(0, 2);
    try writer.setBytes(1, "bob");
    try writer.setDouble(2, 88.0);
    try writer.setBool(3, false);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    // Project columns 1 (name) and 2 (score)
    const rows = try reader.readRowsProjected(0, &.{ 1, 2 });
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 2), rows.len);
    // Each row should have exactly 2 values (dense packing)
    try std.testing.expectEqual(@as(usize, 2), rows[0].columnCount());

    // Row 0: name=alice, score=95.5
    try std.testing.expectEqualStrings("alice", rows[0].getColumn(0).?.asBytes().?);
    try std.testing.expectApproxEqAbs(@as(f64, 95.5), rows[0].getColumn(1).?.asDouble().?, 0.001);

    // Row 1: name=bob, score=88.0
    try std.testing.expectEqualStrings("bob", rows[1].getColumn(0).?.asBytes().?);
    try std.testing.expectApproxEqAbs(@as(f64, 88.0), rows[1].getColumn(1).?.asDouble().?, 0.001);
}

test "readRowsProjected single column" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("a", TypeInfo.int32, .{});
    try writer.addColumn("b", TypeInfo.int64, .{});
    try writer.addColumn("c", TypeInfo.double_, .{});
    try writer.begin();

    try writer.setInt32(0, 10);
    try writer.setInt64(1, 20);
    try writer.setDouble(2, 30.0);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    // Project only column 2 (c)
    const rows = try reader.readRowsProjected(0, &.{2});
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqual(@as(usize, 1), rows[0].columnCount());
    try std.testing.expectApproxEqAbs(@as(f64, 30.0), rows[0].getColumn(0).?.asDouble().?, 0.001);
}

test "readRowsProjected all columns matches readAllRows" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("x", TypeInfo.int32, .{});
    try writer.addColumn("y", TypeInfo.int64, .{});
    try writer.begin();

    try writer.setInt32(0, 42);
    try writer.setInt64(1, 100);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    // Project all columns explicitly
    const projected = try reader.readRowsProjected(0, &.{ 0, 1 });
    defer deferFreeRows(allocator, projected);

    try std.testing.expectEqual(@as(usize, 1), projected.len);
    try std.testing.expectEqual(@as(usize, 2), projected[0].columnCount());
    try std.testing.expectEqual(@as(?i32, 42), projected[0].getColumn(0).?.asInt32());
    try std.testing.expectEqual(@as(?i64, 100), projected[0].getColumn(1).?.asInt64());
}

test "readRowsProjected out-of-range index returns error" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("x", TypeInfo.int32, .{});
    try writer.addColumn("y", TypeInfo.int64, .{});
    try writer.begin();

    try writer.setInt32(0, 1);
    try writer.setInt64(1, 2);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    // Column index 5 does not exist (only 0 and 1)
    const result = reader.readRowsProjected(0, &.{5});
    try std.testing.expectError(error.InvalidArgument, result);
}

test "readRowsProjected with nested struct columns" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    // Top-level column 0: flat int
    try writer.addColumn("id", TypeInfo.int32, .{});

    // Top-level column 1: struct with 2 leaf columns
    const fields = try writer.allocSchemaFields(2);
    fields[0] = .{ .name = "city", .node = try writer.allocSchemaNode(.{ .byte_array = .{ .logical = .string } }) };
    fields[1] = .{ .name = "zip", .node = try writer.allocSchemaNode(.{ .int32 = .{} }) };
    const struct_node = try writer.allocSchemaNode(.{ .struct_ = .{ .fields = fields } });
    try writer.addColumnNested("address", struct_node, .{});

    // Top-level column 2: flat string
    try writer.addColumn("note", TypeInfo.string, .{});

    try writer.begin();

    // Row 1
    try writer.setInt32(0, 1);
    try writer.beginStruct(1);
    try writer.setStructFieldBytes(1, 0, "NYC");
    try writer.setStructField(1, 1, .{ .int32_val = 10001 });
    try writer.endStruct(1);
    try writer.setBytes(2, "first");
    try writer.addRow();

    // Row 2
    try writer.setInt32(0, 2);
    try writer.beginStruct(1);
    try writer.setStructFieldBytes(1, 0, "LA");
    try writer.setStructField(1, 1, .{ .int32_val = 90001 });
    try writer.endStruct(1);
    try writer.setBytes(2, "second");
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    // Project columns 0 (id) and 1 (address struct), skip column 2 (note)
    const rows = try reader.readRowsProjected(0, &.{ 0, 1 });
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 2), rows.len);
    try std.testing.expectEqual(@as(usize, 2), rows[0].columnCount());

    // Row 0: id=1, address={city:"NYC", zip:10001}
    try std.testing.expectEqual(@as(?i32, 1), rows[0].getColumn(0).?.asInt32());
    const s0 = rows[0].getColumn(1).?.asStruct().?;
    try std.testing.expectEqualStrings("NYC", s0[0].value.asBytes().?);
    try std.testing.expectEqual(@as(?i32, 10001), s0[1].value.asInt32());

    // Row 1: id=2, address={city:"LA", zip:90001}
    try std.testing.expectEqual(@as(?i32, 2), rows[1].getColumn(0).?.asInt32());
    const s1 = rows[1].getColumn(1).?.asStruct().?;
    try std.testing.expectEqualStrings("LA", s1[0].value.asBytes().?);
    try std.testing.expectEqual(@as(?i32, 90001), s1[1].value.asInt32());
}

test "readRowsProjected struct-only skips flat columns" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", TypeInfo.int32, .{});

    const fields = try writer.allocSchemaFields(2);
    fields[0] = .{ .name = "x", .node = try writer.allocSchemaNode(.{ .int32 = .{} }) };
    fields[1] = .{ .name = "y", .node = try writer.allocSchemaNode(.{ .int32 = .{} }) };
    const struct_node = try writer.allocSchemaNode(.{ .struct_ = .{ .fields = fields } });
    try writer.addColumnNested("point", struct_node, .{});

    try writer.addColumn("label", TypeInfo.string, .{});

    try writer.begin();

    try writer.setInt32(0, 99);
    try writer.beginStruct(1);
    try writer.setStructField(1, 0, .{ .int32_val = 10 });
    try writer.setStructField(1, 1, .{ .int32_val = 20 });
    try writer.endStruct(1);
    try writer.setBytes(2, "origin");
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    // Project only the struct column (top-level index 1)
    const rows = try reader.readRowsProjected(0, &.{1});
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqual(@as(usize, 1), rows[0].columnCount());

    const s = rows[0].getColumn(0).?.asStruct().?;
    try std.testing.expectEqual(@as(?i32, 10), s[0].value.asInt32());
    try std.testing.expectEqual(@as(?i32, 20), s[1].value.asInt32());
}

// =============================================================================
// DynamicReader statistics tests
// =============================================================================

test "getColumnStatistics roundtrip with multi-row-group file" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("value", parquet.TypeInfo.int32, .{});
    try writer.addColumn("name", parquet.TypeInfo.string, .{});
    try writer.begin();

    // Row group 0: values 10, 20, null
    try writer.setInt32(0, 10);
    try writer.setBytes(1, "alpha");
    try writer.addRow();
    try writer.setInt32(0, 20);
    try writer.setBytes(1, "beta");
    try writer.addRow();
    try writer.setNull(0);
    try writer.setBytes(1, "gamma");
    try writer.addRow();
    try writer.flush();

    // Row group 1: values 100, 200
    try writer.setInt32(0, 100);
    try writer.setBytes(1, "delta");
    try writer.addRow();
    try writer.setInt32(0, 200);
    try writer.setBytes(1, "epsilon");
    try writer.addRow();
    try writer.flush();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 2), reader.getNumRowGroups());

    // RG 0: int32 column should have min=10, max=20, null_count=1
    const stats0 = reader.getColumnStatistics(0, 0);
    try std.testing.expect(stats0 != null);
    const s0 = stats0.?;
    try std.testing.expectEqual(@as(?i64, 1), s0.null_count);
    const min0_bytes = s0.min_value orelse s0.min;
    try std.testing.expect(min0_bytes != null);
    const min0 = std.mem.readInt(i32, min0_bytes.?[0..4], .little);
    try std.testing.expectEqual(@as(i32, 10), min0);
    const max0_bytes = s0.max_value orelse s0.max;
    try std.testing.expect(max0_bytes != null);
    const max0 = std.mem.readInt(i32, max0_bytes.?[0..4], .little);
    try std.testing.expectEqual(@as(i32, 20), max0);

    // RG 1: int32 column should have min=100, max=200, null_count=0
    const stats1 = reader.getColumnStatistics(0, 1);
    try std.testing.expect(stats1 != null);
    const s1 = stats1.?;
    try std.testing.expectEqual(@as(?i64, 0), s1.null_count);
    const min1_bytes = s1.min_value orelse s1.min;
    try std.testing.expect(min1_bytes != null);
    const min1 = std.mem.readInt(i32, min1_bytes.?[0..4], .little);
    try std.testing.expectEqual(@as(i32, 100), min1);
    const max1_bytes = s1.max_value orelse s1.max;
    try std.testing.expect(max1_bytes != null);
    const max1 = std.mem.readInt(i32, max1_bytes.?[0..4], .little);
    try std.testing.expectEqual(@as(i32, 200), max1);

    // getColumnMetaData should also work
    const meta0 = reader.getColumnMetaData(0, 0);
    try std.testing.expect(meta0 != null);
    try std.testing.expectEqual(parquet.format.PhysicalType.int32, meta0.?.type_);
}

test "getColumnStatistics returns null for out-of-range indices" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("x", parquet.TypeInfo.int32, .{});
    try writer.begin();
    try writer.setInt32(0, 42);
    try writer.addRow();
    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    // Valid
    try std.testing.expect(reader.getColumnStatistics(0, 0) != null);

    // Out-of-range column
    try std.testing.expectEqual(@as(?parquet.format.Statistics, null), reader.getColumnStatistics(99, 0));

    // Out-of-range row group
    try std.testing.expectEqual(@as(?parquet.format.Statistics, null), reader.getColumnStatistics(0, 99));

    // Out-of-range both
    try std.testing.expectEqual(@as(?parquet.format.Statistics, null), reader.getColumnStatistics(99, 99));

    // Same for getColumnMetaData
    try std.testing.expect(reader.getColumnMetaData(0, 0) != null);
    try std.testing.expectEqual(@as(?parquet.format.ColumnMetaData, null), reader.getColumnMetaData(99, 0));
}

test "row group filtering pattern using statistics" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", parquet.TypeInfo.int32, .{});
    try writer.addColumn("label", parquet.TypeInfo.string, .{});
    try writer.begin();

    // Row group 0: ids 1-100
    for (1..101) |i| {
        const val: i32 = @intCast(i);
        try writer.setInt32(0, val);
        try writer.setBytes(1, "low");
        try writer.addRow();
    }
    try writer.flush();

    // Row group 1: ids 1000-1099
    for (0..100) |i| {
        const val: i32 = @intCast(1000 + i);
        try writer.setInt32(0, val);
        try writer.setBytes(1, "high");
        try writer.addRow();
    }
    try writer.flush();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 2), reader.getNumRowGroups());

    // Target: find row groups containing id=50
    const target: i32 = 50;
    var matching_rg: ?usize = null;

    for (0..reader.getNumRowGroups()) |rg| {
        const stats = reader.getColumnStatistics(0, rg) orelse continue;
        const min_bytes = stats.min_value orelse stats.min orelse continue;
        const max_bytes = stats.max_value orelse stats.max orelse continue;
        const min_val = std.mem.readInt(i32, min_bytes[0..4], .little);
        const max_val = std.mem.readInt(i32, max_bytes[0..4], .little);

        if (target >= min_val and target <= max_val) {
            matching_rg = rg;
            break;
        }
    }

    // Should match row group 0 (ids 1-100), not row group 1 (ids 1000-1099)
    try std.testing.expectEqual(@as(?usize, 0), matching_rg);

    // Read only the matching row group
    const rows = try reader.readAllRows(matching_rg.?);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 100), rows.len);
    try std.testing.expectEqual(@as(?i32, 1), rows[0].getColumn(0).?.asInt32());
    try std.testing.expectEqual(@as(?i32, 100), rows[99].getColumn(0).?.asInt32());
}

// =============================================================================
// RowIterator tests
// =============================================================================

test "rowIterator basic iteration" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", parquet.TypeInfo.int32, .{});
    try writer.addColumn("name", parquet.TypeInfo.string, .{});
    try writer.begin();

    try writer.setInt32(0, 10);
    try writer.setBytes(1, "alpha");
    try writer.addRow();
    try writer.setInt32(0, 20);
    try writer.setBytes(1, "beta");
    try writer.addRow();
    try writer.setInt32(0, 30);
    try writer.setBytes(1, "gamma");
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    var iter = reader.rowIterator();
    defer iter.deinit();

    const row0 = (try iter.next()).?;
    try std.testing.expectEqual(@as(?i32, 10), row0.getColumn(0).?.asInt32());

    const row1 = (try iter.next()).?;
    try std.testing.expectEqual(@as(?i32, 20), row1.getColumn(0).?.asInt32());

    const row2 = (try iter.next()).?;
    try std.testing.expectEqual(@as(?i32, 30), row2.getColumn(0).?.asInt32());

    try std.testing.expectEqual(@as(?*const DynRow, null), try iter.next());
}

test "rowIterator multi-row-group seamless traversal" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("val", parquet.TypeInfo.int32, .{});
    try writer.begin();

    // Row group 0: values 1, 2
    try writer.setInt32(0, 1);
    try writer.addRow();
    try writer.setInt32(0, 2);
    try writer.addRow();
    try writer.flush();

    // Row group 1: values 3, 4, 5
    try writer.setInt32(0, 3);
    try writer.addRow();
    try writer.setInt32(0, 4);
    try writer.addRow();
    try writer.setInt32(0, 5);
    try writer.addRow();
    try writer.flush();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 2), reader.getNumRowGroups());

    var iter = reader.rowIterator();
    defer iter.deinit();

    var collected: [5]i32 = undefined;
    var count: usize = 0;
    while (try iter.next()) |row| {
        collected[count] = row.getColumn(0).?.asInt32().?;
        count += 1;
    }

    try std.testing.expectEqual(@as(usize, 5), count);
    try std.testing.expectEqual([_]i32{ 1, 2, 3, 4, 5 }, collected);
}

test "rowIteratorProjected only returns projected columns" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("a", parquet.TypeInfo.int32, .{});
    try writer.addColumn("b", parquet.TypeInfo.string, .{});
    try writer.addColumn("c", parquet.TypeInfo.double_, .{});
    try writer.begin();

    try writer.setInt32(0, 1);
    try writer.setBytes(1, "hello");
    try writer.setDouble(2, 3.14);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    // Project only columns "b" (index 1) and "c" (index 2)
    var iter = reader.rowIteratorProjected(&.{ 1, 2 });
    defer iter.deinit();

    const row = (try iter.next()).?;
    try std.testing.expectEqual(@as(usize, 2), row.columnCount());

    const b_bytes = row.getColumn(0).?.asBytes().?;
    try std.testing.expectEqualStrings("hello", b_bytes);
    try std.testing.expectEqual(@as(?f64, 3.14), row.getColumn(1).?.asDouble());

    try std.testing.expectEqual(@as(?*const DynRow, null), try iter.next());
}

test "rowIterator on empty file returns null immediately" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("x", parquet.TypeInfo.int32, .{});
    try writer.begin();
    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    var iter = reader.rowIterator();
    defer iter.deinit();

    try std.testing.expectEqual(@as(?*const DynRow, null), try iter.next());
}

test "round-trip required columns (DynamicWriter)" {
    const allocator = std.testing.allocator;
    const format = parquet.format;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", TypeInfo.int32.asRequired(), .{});
    try writer.addColumn("name", TypeInfo.string.asRequired(), .{});
    try writer.addColumn("score", TypeInfo.float_, .{});
    try writer.begin();

    try writer.setInt32(0, 1);
    try writer.setBytes(1, "alice");
    try writer.setFloat(2, 1.5);
    try writer.addRow();

    try writer.setInt32(0, 2);
    try writer.setBytes(1, "bob");
    try writer.setNull(2);
    try writer.addRow();

    try writer.setInt32(0, 3);
    try writer.setBytes(1, "charlie");
    try writer.setFloat(2, 3.5);
    try writer.addRow();

    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    // schema[0] is the root "schema" element; columns start at index 1
    try std.testing.expectEqual(format.RepetitionType.required, reader.metadata.schema[1].repetition_type.?);
    try std.testing.expectEqual(format.RepetitionType.required, reader.metadata.schema[2].repetition_type.?);
    try std.testing.expectEqual(format.RepetitionType.optional, reader.metadata.schema[3].repetition_type.?);

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 3), rows.len);

    try std.testing.expectEqual(@as(?i32, 1), rows[0].getColumn(0).?.asInt32());
    try std.testing.expectEqualStrings("alice", rows[0].getColumn(1).?.asBytes().?);
    try std.testing.expectApproxEqAbs(@as(f32, 1.5), rows[0].getColumn(2).?.asFloat().?, 0.01);

    try std.testing.expectEqual(@as(?i32, 2), rows[1].getColumn(0).?.asInt32());
    try std.testing.expectEqualStrings("bob", rows[1].getColumn(1).?.asBytes().?);
    try std.testing.expect(rows[1].getColumn(2).?.isNull());

    try std.testing.expectEqual(@as(?i32, 3), rows[2].getColumn(0).?.asInt32());
    try std.testing.expectEqualStrings("charlie", rows[2].getColumn(1).?.asBytes().?);
    try std.testing.expectApproxEqAbs(@as(f32, 3.5), rows[2].getColumn(2).?.asFloat().?, 0.01);
}

test "required column rejects setNull (DynamicWriter)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", TypeInfo.int32.asRequired(), .{});
    try writer.begin();

    const result = writer.setNull(0);
    try std.testing.expectError(error.InvalidArgument, result);
}

test "required column rejects unset addRow (DynamicWriter)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", TypeInfo.int32.asRequired(), .{});
    try writer.addColumn("name", TypeInfo.string, .{});
    try writer.begin();

    try writer.setBytes(1, "alice");
    const result = writer.addRow();
    try std.testing.expectError(error.InvalidArgument, result);
}

test "round-trip all-null optional UUID column with dictionary encoding (DynamicWriter)" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", TypeInfo.uuid, .{});
    try writer.begin();

    try writer.setNull(0);
    try writer.addRow();
    try writer.setNull(0);
    try writer.addRow();
    try writer.setNull(0);
    try writer.addRow();

    try writer.close();
    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer deferFreeRows(allocator, rows);

    try std.testing.expectEqual(@as(usize, 3), rows.len);
    try std.testing.expect(rows[0].getColumn(0).?.isNull());
    try std.testing.expect(rows[1].getColumn(0).?.isNull());
    try std.testing.expect(rows[2].getColumn(0).?.isNull());
}
