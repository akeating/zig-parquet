//! Compression round-trip tests for the Parquet writer
//!
//! Tests write files using various compression codecs and read them back
//! to verify correctness.

const std = @import("std");
const parquet = @import("../lib.zig");
const build_options = @import("build_options");

test "compression codec in ColumnDef" {
    // Verify that codec field exists and defaults to uncompressed
    const col1 = parquet.ColumnDef{
        .name = "test",
        .type_ = .int64,
    };
    try std.testing.expectEqual(parquet.format.CompressionCodec.uncompressed, col1.codec);

    // Verify codec can be set explicitly
    const col2 = parquet.ColumnDef{
        .name = "test",
        .type_ = .int64,
        .codec = .zstd,
    };
    try std.testing.expectEqual(parquet.format.CompressionCodec.zstd, col2.codec);

    // Verify convenience constructors default to uncompressed
    const string_col = parquet.ColumnDef.string("name", false);
    try std.testing.expectEqual(parquet.format.CompressionCodec.uncompressed, string_col.codec);
}

test "round-trip with zstd compression" {
    if (build_options.no_compression) return;
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_zstd.parquet";

    // Write with zstd compression
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "id", .type_ = .int64, .optional = false, .codec = .zstd },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        const values = [_]i64{ 1, 2, 3, 4, 5, 100, -50, 0, 999999, -123456 };
        try writer.writeColumn(i64, 0, &values);
        try writer.close();
    }

    // Read back and verify
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        // Verify metadata
        try std.testing.expectEqual(@as(i64, 10), reader.metadata.num_rows);

        // Verify compression codec in metadata
        const row_group = reader.metadata.row_groups[0];
        const col_chunk = row_group.columns[0];
        try std.testing.expectEqual(parquet.format.CompressionCodec.zstd, col_chunk.meta_data.?.codec);

        // Read column
        const col = try reader.readColumn(0, i64);
        defer allocator.free(col);

        try std.testing.expectEqual(@as(usize, 10), col.len);

        // Verify values
        const expected = [_]i64{ 1, 2, 3, 4, 5, 100, -50, 0, 999999, -123456 };
        for (col, 0..) |v, i| {
            switch (v) {
                .value => |val| try std.testing.expectEqual(expected[i], val),
                .null_value => return error.UnexpectedNull,
            }
        }
    }
}

test "round-trip zstd compression with byte arrays" {
    if (build_options.no_compression) return;
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_zstd_strings.parquet";

    // Write with zstd compression
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        // Create a STRING column with zstd compression
        var col = parquet.ColumnDef.string("name", false);
        col.codec = .zstd;

        const columns = [_]parquet.ColumnDef{col};

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        const values = [_][]const u8{ "hello", "world", "test", "compression", "parquet" };
        try writer.writeColumn([]const u8, 0, &values);
        try writer.close();
    }

    // Read back and verify
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 5), reader.metadata.num_rows);

        // Verify compression
        const row_group = reader.metadata.row_groups[0];
        const col_chunk = row_group.columns[0];
        try std.testing.expectEqual(parquet.format.CompressionCodec.zstd, col_chunk.meta_data.?.codec);

        // Read column
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

        try std.testing.expectEqual(@as(usize, 5), col.len);

        const expected = [_][]const u8{ "hello", "world", "test", "compression", "parquet" };
        for (col, 0..) |v, i| {
            switch (v) {
                .value => |s| try std.testing.expectEqualStrings(expected[i], s),
                .null_value => return error.UnexpectedNull,
            }
        }
    }
}

test "round-trip mixed compression (zstd and uncompressed)" {
    if (build_options.no_compression) return;
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_mixed_compression.parquet";

    // Write with mixed compression
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "compressed_col", .type_ = .int64, .optional = false, .codec = .zstd },
            .{ .name = "uncompressed_col", .type_ = .int32, .optional = false, .codec = .uncompressed },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        const i64_values = [_]i64{ 100, 200, 300 };
        try writer.writeColumn(i64, 0, &i64_values);

        const i32_values = [_]i32{ 1, 2, 3 };
        try writer.writeColumn(i32, 1, &i32_values);

        try writer.close();
    }

    // Read back and verify
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 3), reader.metadata.num_rows);

        // Verify compression codecs
        const row_group = reader.metadata.row_groups[0];
        try std.testing.expectEqual(parquet.format.CompressionCodec.zstd, row_group.columns[0].meta_data.?.codec);
        try std.testing.expectEqual(parquet.format.CompressionCodec.uncompressed, row_group.columns[1].meta_data.?.codec);

        // Read and verify first column (zstd)
        const col0 = try reader.readColumn(0, i64);
        defer allocator.free(col0);
        try std.testing.expectEqual(@as(usize, 3), col0.len);
        switch (col0[0]) {
            .value => |v| try std.testing.expectEqual(@as(i64, 100), v),
            .null_value => return error.UnexpectedNull,
        }

        // Read and verify second column (uncompressed)
        const col1 = try reader.readColumn(1, i32);
        defer allocator.free(col1);
        try std.testing.expectEqual(@as(usize, 3), col1.len);
        switch (col1[0]) {
            .value => |v| try std.testing.expectEqual(@as(i32, 1), v),
            .null_value => return error.UnexpectedNull,
        }
    }
}

test "round-trip with gzip compression" {
    if (build_options.no_compression) return;
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_gzip.parquet";

    // Write with gzip compression
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "id", .type_ = .int64, .optional = false, .codec = .gzip },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        const values = [_]i64{ 1, 2, 3, 4, 5, 100, -50, 0, 999999, -123456 };
        try writer.writeColumn(i64, 0, &values);
        try writer.close();
    }

    // Read back and verify
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        // Verify metadata
        try std.testing.expectEqual(@as(i64, 10), reader.metadata.num_rows);

        // Verify compression codec in metadata
        const row_group = reader.metadata.row_groups[0];
        const col_chunk = row_group.columns[0];
        try std.testing.expectEqual(parquet.format.CompressionCodec.gzip, col_chunk.meta_data.?.codec);

        // Read column
        const col = try reader.readColumn(0, i64);
        defer allocator.free(col);

        try std.testing.expectEqual(@as(usize, 10), col.len);

        // Verify values
        const expected = [_]i64{ 1, 2, 3, 4, 5, 100, -50, 0, 999999, -123456 };
        for (col, 0..) |v, i| {
            switch (v) {
                .value => |val| try std.testing.expectEqual(expected[i], val),
                .null_value => return error.UnexpectedNull,
            }
        }
    }
}

test "round-trip gzip compression with byte arrays" {
    if (build_options.no_compression) return;
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_gzip_strings.parquet";

    // Write with gzip compression
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        // Create a STRING column with gzip compression
        var col = parquet.ColumnDef.string("name", false);
        col.codec = .gzip;

        const columns = [_]parquet.ColumnDef{col};

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        const values = [_][]const u8{ "hello", "world", "test", "compression", "parquet" };
        try writer.writeColumn([]const u8, 0, &values);
        try writer.close();
    }

    // Read back and verify
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 5), reader.metadata.num_rows);

        // Verify compression
        const row_group = reader.metadata.row_groups[0];
        const col_chunk = row_group.columns[0];
        try std.testing.expectEqual(parquet.format.CompressionCodec.gzip, col_chunk.meta_data.?.codec);

        // Read column
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

        try std.testing.expectEqual(@as(usize, 5), col.len);

        const expected = [_][]const u8{ "hello", "world", "test", "compression", "parquet" };
        for (col, 0..) |v, i| {
            switch (v) {
                .value => |s| try std.testing.expectEqualStrings(expected[i], s),
                .null_value => return error.UnexpectedNull,
            }
        }
    }
}

test "round-trip with snappy compression" {
    if (build_options.no_compression) return;
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_snappy.parquet";

    // Write with snappy compression
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "id", .type_ = .int64, .optional = false, .codec = .snappy },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        const values = [_]i64{ 1, 2, 3, 4, 5, 100, -50, 0, 999999, -123456 };
        try writer.writeColumn(i64, 0, &values);
        try writer.close();
    }

    // Read back and verify
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        // Verify metadata
        try std.testing.expectEqual(@as(i64, 10), reader.metadata.num_rows);

        // Verify compression codec in metadata
        const row_group = reader.metadata.row_groups[0];
        const col_chunk = row_group.columns[0];
        try std.testing.expectEqual(parquet.format.CompressionCodec.snappy, col_chunk.meta_data.?.codec);

        // Read column
        const col = try reader.readColumn(0, i64);
        defer allocator.free(col);

        try std.testing.expectEqual(@as(usize, 10), col.len);

        // Verify values
        const expected_i64 = [_]i64{ 1, 2, 3, 4, 5, 100, -50, 0, 999999, -123456 };
        for (col, 0..) |v, i| {
            switch (v) {
                .value => |val| try std.testing.expectEqual(expected_i64[i], val),
                .null_value => return error.UnexpectedNull,
            }
        }
    }
}

test "round-trip snappy compression with byte arrays" {
    if (build_options.no_compression) return;
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_snappy_strings.parquet";

    // Write with snappy compression
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        // Create a STRING column with snappy compression
        var col_def = parquet.ColumnDef.string("name", false);
        col_def.codec = .snappy;

        const columns = [_]parquet.ColumnDef{col_def};

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        const values = [_][]const u8{ "hello", "world", "test", "compression", "parquet" };
        try writer.writeColumn([]const u8, 0, &values);
        try writer.close();
    }

    // Read back and verify
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 5), reader.metadata.num_rows);

        // Verify compression
        const row_group = reader.metadata.row_groups[0];
        const col_chunk = row_group.columns[0];
        try std.testing.expectEqual(parquet.format.CompressionCodec.snappy, col_chunk.meta_data.?.codec);

        // Read column
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

        try std.testing.expectEqual(@as(usize, 5), col.len);

        const expected_strs = [_][]const u8{ "hello", "world", "test", "compression", "parquet" };
        for (col, 0..) |v, i| {
            switch (v) {
                .value => |s| try std.testing.expectEqualStrings(expected_strs[i], s),
                .null_value => return error.UnexpectedNull,
            }
        }
    }
}

test "round-trip with lz4_raw compression" {
    if (build_options.no_compression) return;
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_lz4.parquet";

    // Write with lz4_raw compression
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "id", .type_ = .int64, .optional = false, .codec = .lz4_raw },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        const values = [_]i64{ 1, 2, 3, 4, 5, 100, -50, 0, 999999, -123456 };
        try writer.writeColumn(i64, 0, &values);
        try writer.close();
    }

    // Read back and verify
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        // Verify metadata
        try std.testing.expectEqual(@as(i64, 10), reader.metadata.num_rows);

        // Verify compression codec in metadata
        const row_group = reader.metadata.row_groups[0];
        const col_chunk = row_group.columns[0];
        try std.testing.expectEqual(parquet.format.CompressionCodec.lz4_raw, col_chunk.meta_data.?.codec);

        // Read column
        const col = try reader.readColumn(0, i64);
        defer allocator.free(col);

        try std.testing.expectEqual(@as(usize, 10), col.len);

        // Verify values
        const expected_i64 = [_]i64{ 1, 2, 3, 4, 5, 100, -50, 0, 999999, -123456 };
        for (col, 0..) |v, i| {
            switch (v) {
                .value => |val| try std.testing.expectEqual(expected_i64[i], val),
                .null_value => return error.UnexpectedNull,
            }
        }
    }
}

test "round-trip lz4_raw compression with byte arrays" {
    if (build_options.no_compression) return;
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_lz4_strings.parquet";

    // Write with lz4_raw compression
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        // Create a STRING column with lz4_raw compression
        var col_def = parquet.ColumnDef.string("name", false);
        col_def.codec = .lz4_raw;

        const columns = [_]parquet.ColumnDef{col_def};

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        const values = [_][]const u8{ "hello", "world", "test", "compression", "parquet" };
        try writer.writeColumn([]const u8, 0, &values);
        try writer.close();
    }

    // Read back and verify
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 5), reader.metadata.num_rows);

        // Verify compression
        const row_group = reader.metadata.row_groups[0];
        const col_chunk = row_group.columns[0];
        try std.testing.expectEqual(parquet.format.CompressionCodec.lz4_raw, col_chunk.meta_data.?.codec);

        // Read column
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

        try std.testing.expectEqual(@as(usize, 5), col.len);

        const expected_lz4_strs = [_][]const u8{ "hello", "world", "test", "compression", "parquet" };
        for (col, 0..) |v, i| {
            switch (v) {
                .value => |s| try std.testing.expectEqualStrings(expected_lz4_strs[i], s),
                .null_value => return error.UnexpectedNull,
            }
        }
    }
}

test "round-trip with brotli compression" {
    if (build_options.no_compression) return;
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_brotli.parquet";

    // Write with brotli compression
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        const columns = [_]parquet.ColumnDef{
            .{ .name = "id", .type_ = .int64, .optional = false, .codec = .brotli },
        };

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        const values = [_]i64{ 1, 2, 3, 4, 5, 100, -50, 0, 999999, -123456 };
        try writer.writeColumn(i64, 0, &values);
        try writer.close();
    }

    // Read back and verify
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        // Verify metadata
        try std.testing.expectEqual(@as(i64, 10), reader.metadata.num_rows);

        // Verify compression codec in metadata
        const row_group = reader.metadata.row_groups[0];
        const col_chunk = row_group.columns[0];
        try std.testing.expectEqual(parquet.format.CompressionCodec.brotli, col_chunk.meta_data.?.codec);

        // Read column
        const col = try reader.readColumn(0, i64);
        defer allocator.free(col);

        try std.testing.expectEqual(@as(usize, 10), col.len);

        // Verify values
        const expected_i64 = [_]i64{ 1, 2, 3, 4, 5, 100, -50, 0, 999999, -123456 };
        for (col, 0..) |v, i| {
            switch (v) {
                .value => |val| try std.testing.expectEqual(expected_i64[i], val),
                .null_value => return error.UnexpectedNull,
            }
        }
    }
}

test "round-trip brotli compression with byte arrays" {
    if (build_options.no_compression) return;
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file_path = "roundtrip_brotli_strings.parquet";

    // Write with brotli compression
    {
        const file = try tmp_dir.dir.createFile(file_path, .{});
        defer file.close();

        // Create a STRING column with brotli compression
        var col_def = parquet.ColumnDef.string("name", false);
        col_def.codec = .brotli;

        const columns = [_]parquet.ColumnDef{col_def};

        var writer = try parquet.writeToFile(allocator, file, &columns);
        defer writer.deinit();

        const values = [_][]const u8{ "hello", "world", "test", "compression", "parquet" };
        try writer.writeColumn([]const u8, 0, &values);
        try writer.close();
    }

    // Read back and verify
    {
        const file = try tmp_dir.dir.openFile(file_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        try std.testing.expectEqual(@as(i64, 5), reader.metadata.num_rows);

        // Verify compression
        const row_group = reader.metadata.row_groups[0];
        const col_chunk = row_group.columns[0];
        try std.testing.expectEqual(parquet.format.CompressionCodec.brotli, col_chunk.meta_data.?.codec);

        // Read column
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

        try std.testing.expectEqual(@as(usize, 5), col.len);

        const expected_brotli_strs = [_][]const u8{ "hello", "world", "test", "compression", "parquet" };
        for (col, 0..) |v, i| {
            switch (v) {
                .value => |s| try std.testing.expectEqualStrings(expected_brotli_strs[i], s),
                .null_value => return error.UnexpectedNull,
            }
        }
    }
}
