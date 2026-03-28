//! Encoding tests
//!
//! Tests for dictionary encoding, compression, and multiple row groups.

const std = @import("std");
const parquet = @import("../lib.zig");
const build_options = @import("build_options");

test "read dictionary_low_cardinality.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/encodings/dictionary_low_cardinality.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    // Verify metadata
    try std.testing.expectEqual(@as(i64, 3000), reader.metadata.num_rows);
    try std.testing.expectEqual(@as(usize, 1), reader.getNumRowGroups());

    // Read all rows
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 3000), rows.len);

    // status column (column 0) - should cycle: active, inactive, pending
    try std.testing.expectEqualStrings("active", rows[0].getColumn(0).?.asBytes().?);
    try std.testing.expectEqualStrings("inactive", rows[1].getColumn(0).?.asBytes().?);
    try std.testing.expectEqualStrings("pending", rows[2].getColumn(0).?.asBytes().?);
    try std.testing.expectEqualStrings("active", rows[3].getColumn(0).?.asBytes().?);
    // Last few values
    try std.testing.expectEqualStrings("active", rows[2997].getColumn(0).?.asBytes().?);
    try std.testing.expectEqualStrings("inactive", rows[2998].getColumn(0).?.asBytes().?);
    try std.testing.expectEqualStrings("pending", rows[2999].getColumn(0).?.asBytes().?);

    // category column (column 1) - should cycle: A, B, C, D
    try std.testing.expectEqualStrings("A", rows[0].getColumn(1).?.asBytes().?);
    try std.testing.expectEqualStrings("B", rows[1].getColumn(1).?.asBytes().?);
    try std.testing.expectEqualStrings("C", rows[2].getColumn(1).?.asBytes().?);
    try std.testing.expectEqualStrings("D", rows[3].getColumn(1).?.asBytes().?);
    try std.testing.expectEqualStrings("A", rows[4].getColumn(1).?.asBytes().?);
    // Last values
    try std.testing.expectEqualStrings("D", rows[2999].getColumn(1).?.asBytes().?);
}

test "read dictionary_high_cardinality.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/encodings/dictionary_high_cardinality.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    // Verify metadata
    try std.testing.expectEqual(@as(i64, 3000), reader.metadata.num_rows);

    // Read all rows
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    // uuid column (column 0) - "uuid-000000" through "uuid-002999"
    try std.testing.expectEqual(@as(usize, 3000), rows.len);
    try std.testing.expectEqualStrings("uuid-000000", rows[0].getColumn(0).?.asBytes().?);
    try std.testing.expectEqualStrings("uuid-000001", rows[1].getColumn(0).?.asBytes().?);
    try std.testing.expectEqualStrings("uuid-001500", rows[1500].getColumn(0).?.asBytes().?);
    try std.testing.expectEqualStrings("uuid-002999", rows[2999].getColumn(0).?.asBytes().?);
}

test "read compression_zstd.parquet" {
    if (!build_options.enable_zstd) return;
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/compression/compression_zstd.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    // Verify metadata
    try std.testing.expectEqual(@as(i64, 10000), reader.metadata.num_rows);

    // Read all rows
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 10000), rows.len);

    // repeated column (column 0) - all "AAAAAAAAAA"
    try std.testing.expectEqualStrings("AAAAAAAAAA", rows[0].getColumn(0).?.asBytes().?);
    try std.testing.expectEqualStrings("AAAAAAAAAA", rows[5000].getColumn(0).?.asBytes().?);
    try std.testing.expectEqualStrings("AAAAAAAAAA", rows[9999].getColumn(0).?.asBytes().?);

    // sequence column (column 1) - 0 to 9999
    try std.testing.expectEqual(@as(i64, 0), rows[0].getColumn(1).?.asInt64().?);
    try std.testing.expectEqual(@as(i64, 5000), rows[5000].getColumn(1).?.asInt64().?);
    try std.testing.expectEqual(@as(i64, 9999), rows[9999].getColumn(1).?.asInt64().?);
}

test "read compression_gzip.parquet" {
    if (!build_options.enable_gzip) return;
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/compression/compression_gzip.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    // Verify metadata
    try std.testing.expectEqual(@as(i64, 10000), reader.metadata.num_rows);

    // Read all rows
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 10000), rows.len);

    // repeated column (column 0) - all "AAAAAAAAAA"
    try std.testing.expectEqualStrings("AAAAAAAAAA", rows[0].getColumn(0).?.asBytes().?);

    // sequence column (column 1) - 0 to 9999
    try std.testing.expectEqual(@as(i64, 0), rows[0].getColumn(1).?.asInt64().?);
    try std.testing.expectEqual(@as(i64, 9999), rows[9999].getColumn(1).?.asInt64().?);
}

test "read multiple_row_groups.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/structure/multiple_row_groups.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    // Verify metadata
    try std.testing.expectEqual(@as(i64, 100000), reader.metadata.num_rows);
    try std.testing.expectEqual(@as(usize, 10), reader.getNumRowGroups());

    // Each row group has 10000 rows
    for (0..10) |rg_idx| {
        try std.testing.expectEqual(@as(i64, 10000), reader.getRowGroupNumRows(rg_idx));
    }

    // Read first row group
    const rows_rg0 = try reader.readAllRows(0);
    defer {
        for (rows_rg0) |row| row.deinit();
        allocator.free(rows_rg0);
    }

    try std.testing.expectEqual(@as(usize, 10000), rows_rg0.len);
    // id column (column 0)
    try std.testing.expectEqual(@as(i64, 0), rows_rg0[0].getColumn(0).?.asInt64().?);
    try std.testing.expectEqual(@as(i64, 9999), rows_rg0[9999].getColumn(0).?.asInt64().?);

    // Read last row group (row group 9)
    const rows_rg9 = try reader.readAllRows(9);
    defer {
        for (rows_rg9) |row| row.deinit();
        allocator.free(rows_rg9);
    }

    try std.testing.expectEqual(@as(usize, 10000), rows_rg9.len);
    // id column (column 0)
    try std.testing.expectEqual(@as(i64, 90000), rows_rg9[0].getColumn(0).?.asInt64().?);
    try std.testing.expectEqual(@as(i64, 99999), rows_rg9[9999].getColumn(0).?.asInt64().?);

    // Verify value column (column 1) in first row group
    try std.testing.expectEqualStrings("row-0", rows_rg0[0].getColumn(1).?.asBytes().?);
    try std.testing.expectEqualStrings("row-9999", rows_rg0[9999].getColumn(1).?.asBytes().?);
}

test "read compression_none.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/compression/compression_none.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    // Verify metadata
    try std.testing.expectEqual(@as(i64, 10000), reader.metadata.num_rows);

    // Read all rows
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 10000), rows.len);

    // repeated column (column 0) - all "AAAAAAAAAA"
    try std.testing.expectEqualStrings("AAAAAAAAAA", rows[0].getColumn(0).?.asBytes().?);
    try std.testing.expectEqualStrings("AAAAAAAAAA", rows[9999].getColumn(0).?.asBytes().?);

    // sequence column (column 1) - 0 to 9999
    try std.testing.expectEqual(@as(i64, 0), rows[0].getColumn(1).?.asInt64().?);
    try std.testing.expectEqual(@as(i64, 9999), rows[9999].getColumn(1).?.asInt64().?);
}

test "read compression_snappy.parquet" {
    if (!build_options.enable_snappy) return;
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/compression/compression_snappy.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    // Verify metadata
    try std.testing.expectEqual(@as(i64, 10000), reader.metadata.num_rows);

    // Read all rows
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 10000), rows.len);

    // repeated column (column 0) - all "AAAAAAAAAA"
    try std.testing.expectEqualStrings("AAAAAAAAAA", rows[0].getColumn(0).?.asBytes().?);
    try std.testing.expectEqualStrings("AAAAAAAAAA", rows[9999].getColumn(0).?.asBytes().?);

    // sequence column (column 1) - 0 to 9999
    try std.testing.expectEqual(@as(i64, 0), rows[0].getColumn(1).?.asInt64().?);
    try std.testing.expectEqual(@as(i64, 9999), rows[9999].getColumn(1).?.asInt64().?);
}

test "read compression_lz4.parquet" {
    if (!build_options.enable_lz4) return;
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/compression/compression_lz4.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    // Verify metadata
    try std.testing.expectEqual(@as(i64, 10000), reader.metadata.num_rows);

    // Read all rows
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 10000), rows.len);

    // repeated column (column 0) - all "AAAAAAAAAA"
    try std.testing.expectEqualStrings("AAAAAAAAAA", rows[0].getColumn(0).?.asBytes().?);
    try std.testing.expectEqualStrings("AAAAAAAAAA", rows[9999].getColumn(0).?.asBytes().?);

    // sequence column (column 1) - 0 to 9999
    try std.testing.expectEqual(@as(i64, 0), rows[0].getColumn(1).?.asInt64().?);
    try std.testing.expectEqual(@as(i64, 9999), rows[9999].getColumn(1).?.asInt64().?);
}

test "read compression_brotli.parquet" {
    if (!build_options.enable_brotli) return;
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/compression/compression_brotli.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    // Verify metadata
    try std.testing.expectEqual(@as(i64, 10000), reader.metadata.num_rows);

    // Read all rows
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 10000), rows.len);

    // repeated column (column 0) - all "AAAAAAAAAA"
    try std.testing.expectEqualStrings("AAAAAAAAAA", rows[0].getColumn(0).?.asBytes().?);
    try std.testing.expectEqualStrings("AAAAAAAAAA", rows[9999].getColumn(0).?.asBytes().?);

    // sequence column (column 1) - 0 to 9999
    try std.testing.expectEqual(@as(i64, 0), rows[0].getColumn(1).?.asInt64().?);
    try std.testing.expectEqual(@as(i64, 9999), rows[9999].getColumn(1).?.asInt64().?);
}

// =============================================================================
// Delta encoding tests (PyArrow-generated)
// =============================================================================

test "read delta_binary_packed_int32.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/encodings/delta_binary_packed_int32.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 1000), reader.metadata.num_rows);

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 1000), rows.len);

    // sequential (column 0): 0..999
    try std.testing.expectEqual(@as(i32, 0), rows[0].getColumn(0).?.asInt32().?);
    try std.testing.expectEqual(@as(i32, 1), rows[1].getColumn(0).?.asInt32().?);
    try std.testing.expectEqual(@as(i32, 500), rows[500].getColumn(0).?.asInt32().?);
    try std.testing.expectEqual(@as(i32, 999), rows[999].getColumn(0).?.asInt32().?);

    // repeated (column 2): all 42
    try std.testing.expectEqual(@as(i32, 42), rows[0].getColumn(2).?.asInt32().?);
    try std.testing.expectEqual(@as(i32, 42), rows[500].getColumn(2).?.asInt32().?);
    try std.testing.expectEqual(@as(i32, 42), rows[999].getColumn(2).?.asInt32().?);

    // boundaries (column 3): [-2147483648, 2147483647, 0, -1, 1] * 200
    try std.testing.expectEqual(@as(i32, -2147483648), rows[0].getColumn(3).?.asInt32().?);
    try std.testing.expectEqual(@as(i32, 2147483647), rows[1].getColumn(3).?.asInt32().?);
    try std.testing.expectEqual(@as(i32, 0), rows[2].getColumn(3).?.asInt32().?);
    try std.testing.expectEqual(@as(i32, -1), rows[3].getColumn(3).?.asInt32().?);
    try std.testing.expectEqual(@as(i32, 1), rows[4].getColumn(3).?.asInt32().?);
    try std.testing.expectEqual(@as(i32, -2147483648), rows[5].getColumn(3).?.asInt32().?);
}

test "read delta_binary_packed_int64.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/encodings/delta_binary_packed_int64.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 1000), reader.metadata.num_rows);

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 1000), rows.len);

    // timestamps (column 0): 1704067200000000 + i * 1000000
    try std.testing.expectEqual(@as(i64, 1704067200000000), rows[0].getColumn(0).?.asInt64().?);
    try std.testing.expectEqual(@as(i64, 1704067201000000), rows[1].getColumn(0).?.asInt64().?);
    try std.testing.expectEqual(@as(i64, 1704067200000000 + 999 * 1000000), rows[999].getColumn(0).?.asInt64().?);

    // small_deltas (column 2): i * 3
    try std.testing.expectEqual(@as(i64, 0), rows[0].getColumn(2).?.asInt64().?);
    try std.testing.expectEqual(@as(i64, 3), rows[1].getColumn(2).?.asInt64().?);
    try std.testing.expectEqual(@as(i64, 999 * 3), rows[999].getColumn(2).?.asInt64().?);
}

test "read delta_length_byte_array.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/encodings/delta_length_byte_array.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    // uniform (column 0): "x" * 10 repeated 500 times
    try std.testing.expectEqual(@as(usize, 500), rows.len);
    try std.testing.expectEqualStrings("xxxxxxxxxx", rows[0].getColumn(0).?.asBytes().?);
    try std.testing.expectEqualStrings("xxxxxxxxxx", rows[499].getColumn(0).?.asBytes().?);

    // varying (column 1): "y" * ((i % 50) + 1)
    try std.testing.expectEqual(@as(usize, 1), rows[0].getColumn(1).?.asBytes().?.len); // "y"
    try std.testing.expectEqual(@as(usize, 2), rows[1].getColumn(1).?.asBytes().?.len); // "yy"
    try std.testing.expectEqual(@as(usize, 50), rows[49].getColumn(1).?.asBytes().?.len); // "y" * 50
    try std.testing.expectEqual(@as(usize, 1), rows[50].getColumn(1).?.asBytes().?.len); // wraps around

    // empty_and_long (column 2): ["", "a"*1000, "", "b"*500, ""] * 100
    try std.testing.expectEqual(@as(usize, 0), rows[0].getColumn(2).?.asBytes().?.len);
    try std.testing.expectEqual(@as(usize, 1000), rows[1].getColumn(2).?.asBytes().?.len);
    try std.testing.expectEqual(@as(usize, 0), rows[2].getColumn(2).?.asBytes().?.len);
    try std.testing.expectEqual(@as(usize, 500), rows[3].getColumn(2).?.asBytes().?.len);
}

test "read delta_byte_array.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/encodings/delta_byte_array.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 500), rows.len);

    // urls (column 0): "https://example.com/api/v1/users/{i}" for i in 0..499
    try std.testing.expectEqualStrings("https://example.com/api/v1/users/0", rows[0].getColumn(0).?.asBytes().?);
    try std.testing.expectEqualStrings("https://example.com/api/v1/users/1", rows[1].getColumn(0).?.asBytes().?);
    try std.testing.expectEqualStrings("https://example.com/api/v1/users/499", rows[499].getColumn(0).?.asBytes().?);

    // sorted (column 2): sorted "word_00000".."word_00499"
    try std.testing.expectEqualStrings("word_00000", rows[0].getColumn(2).?.asBytes().?);
    try std.testing.expectEqualStrings("word_00001", rows[1].getColumn(2).?.asBytes().?);
    try std.testing.expectEqualStrings("word_00499", rows[499].getColumn(2).?.asBytes().?);

    // mixed (column 3): ["apple","application","apply","banana","bandana","band"] * 83 + ["unique","zoo"]
    try std.testing.expectEqualStrings("apple", rows[0].getColumn(3).?.asBytes().?);
    try std.testing.expectEqualStrings("application", rows[1].getColumn(3).?.asBytes().?);
    try std.testing.expectEqualStrings("apply", rows[2].getColumn(3).?.asBytes().?);
    try std.testing.expectEqualStrings("banana", rows[3].getColumn(3).?.asBytes().?);
    try std.testing.expectEqualStrings("bandana", rows[4].getColumn(3).?.asBytes().?);
    try std.testing.expectEqualStrings("band", rows[5].getColumn(3).?.asBytes().?);
    try std.testing.expectEqualStrings("unique", rows[498].getColumn(3).?.asBytes().?);
    try std.testing.expectEqualStrings("zoo", rows[499].getColumn(3).?.asBytes().?);
}

test "read delta_with_nulls_int.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/encodings/delta_with_nulls_int.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 1000), reader.metadata.num_rows);

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 1000), rows.len);

    // int_with_nulls (column 0): i if i % 7 != 0 else null
    try std.testing.expect(rows[0].getColumn(0).?.isNull()); // 0 % 7 == 0
    try std.testing.expectEqual(@as(i32, 1), rows[1].getColumn(0).?.asInt32().?);
    try std.testing.expectEqual(@as(i32, 6), rows[6].getColumn(0).?.asInt32().?);
    try std.testing.expect(rows[7].getColumn(0).?.isNull()); // 7 % 7 == 0
    try std.testing.expectEqual(@as(i32, 8), rows[8].getColumn(0).?.asInt32().?);
    try std.testing.expect(rows[14].getColumn(0).?.isNull()); // 14 % 7 == 0
    try std.testing.expectEqual(@as(i32, 999), rows[999].getColumn(0).?.asInt32().?);
}

test "read delta_with_nulls_string.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/encodings/delta_with_nulls_string.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 500), reader.metadata.num_rows);

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 500), rows.len);

    // str_with_nulls (column 0): "value_{i}" if i % 5 != 0 else null
    try std.testing.expect(rows[0].getColumn(0).?.isNull()); // 0 % 5 == 0
    try std.testing.expectEqualStrings("value_1", rows[1].getColumn(0).?.asBytes().?);
    try std.testing.expectEqualStrings("value_4", rows[4].getColumn(0).?.asBytes().?);
    try std.testing.expect(rows[5].getColumn(0).?.isNull()); // 5 % 5 == 0
    try std.testing.expectEqualStrings("value_6", rows[6].getColumn(0).?.asBytes().?);
    try std.testing.expect(rows[10].getColumn(0).?.isNull());
    try std.testing.expectEqualStrings("value_499", rows[499].getColumn(0).?.asBytes().?);
}

// =============================================================================
// Byte Stream Split encoding tests (PyArrow-generated)
// =============================================================================

test "read byte_stream_split_float.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/encodings/byte_stream_split_float.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 1000), reader.metadata.num_rows);

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 1000), rows.len);

    // sequential (column 0): float(i) * 0.1 for i in 0..999
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), rows[0].getColumn(0).?.asFloat().?, 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.1), rows[1].getColumn(0).?.asFloat().?, 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 99.9), rows[999].getColumn(0).?.asFloat().?, 0.1);

    // special (column 2): [0.0, -0.0, inf, -inf, nan] * 200
    try std.testing.expectEqual(@as(f32, 0.0), rows[0].getColumn(2).?.asFloat().?);
    try std.testing.expect(std.math.isInf(rows[2].getColumn(2).?.asFloat().?));
    try std.testing.expect(std.math.isNegativeInf(rows[3].getColumn(2).?.asFloat().?));
    try std.testing.expect(std.math.isNan(rows[4].getColumn(2).?.asFloat().?));
}

test "read byte_stream_split_double.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/encodings/byte_stream_split_double.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 1000), reader.metadata.num_rows);

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 1000), rows.len);

    // sequential (column 0): float(i) * 0.001 for i in 0..999
    try std.testing.expectApproxEqAbs(@as(f64, 0.0), rows[0].getColumn(0).?.asDouble().?, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 0.001), rows[1].getColumn(0).?.asDouble().?, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 0.999), rows[999].getColumn(0).?.asDouble().?, 0.001);

    // precise (column 2): 1.0 + i * 1e-15
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), rows[0].getColumn(2).?.asDouble().?, 1e-14);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0 + 999e-15), rows[999].getColumn(2).?.asDouble().?, 1e-14);
}

test "read byte_stream_split_int32.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/encodings/byte_stream_split_int32.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 1000), reader.metadata.num_rows);

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 1000), rows.len);

    // sequential (column 0): 0..999
    try std.testing.expectEqual(@as(i32, 0), rows[0].getColumn(0).?.asInt32().?);
    try std.testing.expectEqual(@as(i32, 1), rows[1].getColumn(0).?.asInt32().?);
    try std.testing.expectEqual(@as(i32, 999), rows[999].getColumn(0).?.asInt32().?);
}

test "read byte_stream_split_int64.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/encodings/byte_stream_split_int64.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 1000), reader.metadata.num_rows);

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 1000), rows.len);

    // sequential (column 0): 0..999
    try std.testing.expectEqual(@as(i64, 0), rows[0].getColumn(0).?.asInt64().?);
    try std.testing.expectEqual(@as(i64, 1), rows[1].getColumn(0).?.asInt64().?);
    try std.testing.expectEqual(@as(i64, 999), rows[999].getColumn(0).?.asInt64().?);

    // timestamps (column 2): 1700000000000 + i * 1000
    try std.testing.expectEqual(@as(i64, 1700000000000), rows[0].getColumn(2).?.asInt64().?);
    try std.testing.expectEqual(@as(i64, 1700000001000), rows[1].getColumn(2).?.asInt64().?);
    try std.testing.expectEqual(@as(i64, 1700000000000 + 999 * 1000), rows[999].getColumn(2).?.asInt64().?);
}

test "read byte_stream_split_float16.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/encodings/byte_stream_split_float16.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 200), reader.metadata.num_rows);

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    // sequential (column 0): FLBA(2) values, read as raw bytes
    try std.testing.expectEqual(@as(usize, 200), rows.len);
    try std.testing.expectEqual(@as(usize, 2), rows[0].getColumn(0).?.asBytes().?.len);
}

test "read byte_stream_split_flba.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/encodings/byte_stream_split_flba.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 200), reader.metadata.num_rows);

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    // fixed_bytes (column 0): FLBA(8), read as raw bytes
    try std.testing.expectEqual(@as(usize, 200), rows.len);
    try std.testing.expectEqual(@as(usize, 8), rows[0].getColumn(0).?.asBytes().?.len);
    try std.testing.expectEqual(@as(usize, 8), rows[199].getColumn(0).?.asBytes().?.len);
}

test "read byte_stream_split_with_nulls.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/encodings/byte_stream_split_with_nulls.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 1000), reader.metadata.num_rows);

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 1000), rows.len);

    // float_with_nulls (column 0): random float if i % 3 != 0 else null
    try std.testing.expect(rows[0].getColumn(0).?.isNull()); // 0 % 3 == 0
    try std.testing.expect(!rows[1].getColumn(0).?.isNull());
    try std.testing.expect(!rows[2].getColumn(0).?.isNull());
    try std.testing.expect(rows[3].getColumn(0).?.isNull()); // 3 % 3 == 0
    try std.testing.expect(rows[6].getColumn(0).?.isNull()); // 6 % 3 == 0
    try std.testing.expect(rows[999].getColumn(0).?.isNull()); // 999 % 3 == 0
}
