//! Encoding tests
//!
//! Tests for dictionary encoding, compression, and multiple row groups.

const std = @import("std");
const parquet = @import("../lib.zig");
const build_options = @import("build_options");
const Reader = parquet.Reader;

test "read dictionary_low_cardinality.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/encodings/dictionary_low_cardinality.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Verify metadata
    try std.testing.expectEqual(@as(i64, 3000), reader.metadata.num_rows);
    try std.testing.expectEqual(@as(usize, 1), reader.getNumRowGroups());

    // Read status column (column 0) - should cycle: active, inactive, pending
    const status = try reader.readColumn(0, []const u8);
    defer {
        for (status) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(status);
    }

    try std.testing.expectEqual(@as(usize, 3000), status.len);
    try std.testing.expectEqualStrings("active", status[0].value);
    try std.testing.expectEqualStrings("inactive", status[1].value);
    try std.testing.expectEqualStrings("pending", status[2].value);
    try std.testing.expectEqualStrings("active", status[3].value);
    // Last few values
    try std.testing.expectEqualStrings("active", status[2997].value);
    try std.testing.expectEqualStrings("inactive", status[2998].value);
    try std.testing.expectEqualStrings("pending", status[2999].value);

    // Read category column (column 1) - should cycle: A, B, C, D
    const category = try reader.readColumn(1, []const u8);
    defer {
        for (category) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(category);
    }

    try std.testing.expectEqual(@as(usize, 3000), category.len);
    try std.testing.expectEqualStrings("A", category[0].value);
    try std.testing.expectEqualStrings("B", category[1].value);
    try std.testing.expectEqualStrings("C", category[2].value);
    try std.testing.expectEqualStrings("D", category[3].value);
    try std.testing.expectEqualStrings("A", category[4].value);
    // Last values
    try std.testing.expectEqualStrings("D", category[2999].value);
}

test "read dictionary_high_cardinality.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/encodings/dictionary_high_cardinality.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Verify metadata
    try std.testing.expectEqual(@as(i64, 3000), reader.metadata.num_rows);

    // Read uuid column - "uuid-000000" through "uuid-002999"
    const uuids = try reader.readColumn(0, []const u8);
    defer {
        for (uuids) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(uuids);
    }

    try std.testing.expectEqual(@as(usize, 3000), uuids.len);
    try std.testing.expectEqualStrings("uuid-000000", uuids[0].value);
    try std.testing.expectEqualStrings("uuid-000001", uuids[1].value);
    try std.testing.expectEqualStrings("uuid-001500", uuids[1500].value);
    try std.testing.expectEqualStrings("uuid-002999", uuids[2999].value);
}

test "read compression_zstd.parquet" {
    if (build_options.no_compression) return;
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/compression/compression_zstd.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Verify metadata
    try std.testing.expectEqual(@as(i64, 10000), reader.metadata.num_rows);

    // Read repeated column - all "AAAAAAAAAA"
    const repeated = try reader.readColumn(0, []const u8);
    defer {
        for (repeated) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(repeated);
    }

    try std.testing.expectEqual(@as(usize, 10000), repeated.len);
    try std.testing.expectEqualStrings("AAAAAAAAAA", repeated[0].value);
    try std.testing.expectEqualStrings("AAAAAAAAAA", repeated[5000].value);
    try std.testing.expectEqualStrings("AAAAAAAAAA", repeated[9999].value);

    // Read sequence column - 0 to 9999
    const sequence = try reader.readColumn(1, i64);
    defer allocator.free(sequence);

    try std.testing.expectEqual(@as(usize, 10000), sequence.len);
    try std.testing.expectEqual(@as(i64, 0), sequence[0].value);
    try std.testing.expectEqual(@as(i64, 5000), sequence[5000].value);
    try std.testing.expectEqual(@as(i64, 9999), sequence[9999].value);
}

test "read compression_gzip.parquet" {
    if (build_options.no_compression) return;
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/compression/compression_gzip.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Verify metadata
    try std.testing.expectEqual(@as(i64, 10000), reader.metadata.num_rows);

    // Read repeated column - all "AAAAAAAAAA"
    const repeated = try reader.readColumn(0, []const u8);
    defer {
        for (repeated) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(repeated);
    }

    try std.testing.expectEqual(@as(usize, 10000), repeated.len);
    try std.testing.expectEqualStrings("AAAAAAAAAA", repeated[0].value);

    // Read sequence column - 0 to 9999
    const sequence = try reader.readColumn(1, i64);
    defer allocator.free(sequence);

    try std.testing.expectEqual(@as(usize, 10000), sequence.len);
    try std.testing.expectEqual(@as(i64, 0), sequence[0].value);
    try std.testing.expectEqual(@as(i64, 9999), sequence[9999].value);
}

test "read multiple_row_groups.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/structure/multiple_row_groups.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Verify metadata
    try std.testing.expectEqual(@as(i64, 100000), reader.metadata.num_rows);
    try std.testing.expectEqual(@as(usize, 10), reader.getNumRowGroups());

    // Each row group has 10000 rows
    for (0..10) |rg_idx| {
        try std.testing.expectEqual(@as(i64, 10000), reader.getRowGroupNumRows(rg_idx).?);
    }

    // Read first row group's id column
    const ids_rg0 = try reader.readColumnFromRowGroup(0, 0, i64);
    defer allocator.free(ids_rg0);

    try std.testing.expectEqual(@as(usize, 10000), ids_rg0.len);
    try std.testing.expectEqual(@as(i64, 0), ids_rg0[0].value);
    try std.testing.expectEqual(@as(i64, 9999), ids_rg0[9999].value);

    // Read last row group's id column (row group 9)
    const ids_rg9 = try reader.readColumnFromRowGroup(0, 9, i64);
    defer allocator.free(ids_rg9);

    try std.testing.expectEqual(@as(usize, 10000), ids_rg9.len);
    try std.testing.expectEqual(@as(i64, 90000), ids_rg9[0].value);
    try std.testing.expectEqual(@as(i64, 99999), ids_rg9[9999].value);

    // Verify value column in first row group
    const values_rg0 = try reader.readColumnFromRowGroup(1, 0, []const u8);
    defer {
        for (values_rg0) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(values_rg0);
    }

    try std.testing.expectEqualStrings("row-0", values_rg0[0].value);
    try std.testing.expectEqualStrings("row-9999", values_rg0[9999].value);
}

test "read compression_none.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/compression/compression_none.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Verify metadata
    try std.testing.expectEqual(@as(i64, 10000), reader.metadata.num_rows);

    // Read repeated column - all "AAAAAAAAAA"
    const repeated = try reader.readColumn(0, []const u8);
    defer {
        for (repeated) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(repeated);
    }

    try std.testing.expectEqual(@as(usize, 10000), repeated.len);
    try std.testing.expectEqualStrings("AAAAAAAAAA", repeated[0].value);
    try std.testing.expectEqualStrings("AAAAAAAAAA", repeated[9999].value);

    // Read sequence column - 0 to 9999
    const sequence = try reader.readColumn(1, i64);
    defer allocator.free(sequence);

    try std.testing.expectEqual(@as(usize, 10000), sequence.len);
    try std.testing.expectEqual(@as(i64, 0), sequence[0].value);
    try std.testing.expectEqual(@as(i64, 9999), sequence[9999].value);
}

test "read compression_snappy.parquet" {
    if (build_options.no_compression) return;
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/compression/compression_snappy.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Verify metadata
    try std.testing.expectEqual(@as(i64, 10000), reader.metadata.num_rows);

    // Read repeated column - all "AAAAAAAAAA"
    const repeated = try reader.readColumn(0, []const u8);
    defer {
        for (repeated) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(repeated);
    }

    try std.testing.expectEqual(@as(usize, 10000), repeated.len);
    try std.testing.expectEqualStrings("AAAAAAAAAA", repeated[0].value);
    try std.testing.expectEqualStrings("AAAAAAAAAA", repeated[9999].value);

    // Read sequence column - 0 to 9999
    const sequence = try reader.readColumn(1, i64);
    defer allocator.free(sequence);

    try std.testing.expectEqual(@as(usize, 10000), sequence.len);
    try std.testing.expectEqual(@as(i64, 0), sequence[0].value);
    try std.testing.expectEqual(@as(i64, 9999), sequence[9999].value);
}

test "read compression_lz4.parquet" {
    if (build_options.no_compression) return;
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/compression/compression_lz4.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Verify metadata
    try std.testing.expectEqual(@as(i64, 10000), reader.metadata.num_rows);

    // Read repeated column - all "AAAAAAAAAA"
    const repeated = try reader.readColumn(0, []const u8);
    defer {
        for (repeated) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(repeated);
    }

    try std.testing.expectEqual(@as(usize, 10000), repeated.len);
    try std.testing.expectEqualStrings("AAAAAAAAAA", repeated[0].value);
    try std.testing.expectEqualStrings("AAAAAAAAAA", repeated[9999].value);

    // Read sequence column - 0 to 9999
    const sequence = try reader.readColumn(1, i64);
    defer allocator.free(sequence);

    try std.testing.expectEqual(@as(usize, 10000), sequence.len);
    try std.testing.expectEqual(@as(i64, 0), sequence[0].value);
    try std.testing.expectEqual(@as(i64, 9999), sequence[9999].value);
}

test "read compression_brotli.parquet" {
    if (build_options.no_compression) return;
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/compression/compression_brotli.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Verify metadata
    try std.testing.expectEqual(@as(i64, 10000), reader.metadata.num_rows);

    // Read repeated column - all "AAAAAAAAAA"
    const repeated = try reader.readColumn(0, []const u8);
    defer {
        for (repeated) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(repeated);
    }

    try std.testing.expectEqual(@as(usize, 10000), repeated.len);
    try std.testing.expectEqualStrings("AAAAAAAAAA", repeated[0].value);
    try std.testing.expectEqualStrings("AAAAAAAAAA", repeated[9999].value);

    // Read sequence column - 0 to 9999
    const sequence = try reader.readColumn(1, i64);
    defer allocator.free(sequence);

    try std.testing.expectEqual(@as(usize, 10000), sequence.len);
    try std.testing.expectEqual(@as(i64, 0), sequence[0].value);
    try std.testing.expectEqual(@as(i64, 9999), sequence[9999].value);
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

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 1000), reader.metadata.num_rows);

    // sequential: 0..999
    const sequential = try reader.readColumn(0, i32);
    defer allocator.free(sequential);
    try std.testing.expectEqual(@as(usize, 1000), sequential.len);
    try std.testing.expectEqual(@as(i32, 0), sequential[0].value);
    try std.testing.expectEqual(@as(i32, 1), sequential[1].value);
    try std.testing.expectEqual(@as(i32, 500), sequential[500].value);
    try std.testing.expectEqual(@as(i32, 999), sequential[999].value);

    // repeated: all 42
    const repeated = try reader.readColumn(2, i32);
    defer allocator.free(repeated);
    try std.testing.expectEqual(@as(usize, 1000), repeated.len);
    try std.testing.expectEqual(@as(i32, 42), repeated[0].value);
    try std.testing.expectEqual(@as(i32, 42), repeated[500].value);
    try std.testing.expectEqual(@as(i32, 42), repeated[999].value);

    // boundaries: [-2147483648, 2147483647, 0, -1, 1] * 200
    const boundaries = try reader.readColumn(3, i32);
    defer allocator.free(boundaries);
    try std.testing.expectEqual(@as(usize, 1000), boundaries.len);
    try std.testing.expectEqual(@as(i32, -2147483648), boundaries[0].value);
    try std.testing.expectEqual(@as(i32, 2147483647), boundaries[1].value);
    try std.testing.expectEqual(@as(i32, 0), boundaries[2].value);
    try std.testing.expectEqual(@as(i32, -1), boundaries[3].value);
    try std.testing.expectEqual(@as(i32, 1), boundaries[4].value);
    try std.testing.expectEqual(@as(i32, -2147483648), boundaries[5].value);
}

test "read delta_binary_packed_int64.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/encodings/delta_binary_packed_int64.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 1000), reader.metadata.num_rows);

    // timestamps: 1704067200000000 + i * 1000000
    const timestamps = try reader.readColumn(0, i64);
    defer allocator.free(timestamps);
    try std.testing.expectEqual(@as(usize, 1000), timestamps.len);
    try std.testing.expectEqual(@as(i64, 1704067200000000), timestamps[0].value);
    try std.testing.expectEqual(@as(i64, 1704067201000000), timestamps[1].value);
    try std.testing.expectEqual(@as(i64, 1704067200000000 + 999 * 1000000), timestamps[999].value);

    // small_deltas: i * 3
    const small_deltas = try reader.readColumn(2, i64);
    defer allocator.free(small_deltas);
    try std.testing.expectEqual(@as(usize, 1000), small_deltas.len);
    try std.testing.expectEqual(@as(i64, 0), small_deltas[0].value);
    try std.testing.expectEqual(@as(i64, 3), small_deltas[1].value);
    try std.testing.expectEqual(@as(i64, 999 * 3), small_deltas[999].value);
}

test "read delta_length_byte_array.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/encodings/delta_length_byte_array.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // uniform: "x" * 10 repeated 500 times
    const uniform = try reader.readColumn(0, []const u8);
    defer {
        for (uniform) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(uniform);
    }
    try std.testing.expectEqual(@as(usize, 500), uniform.len);
    try std.testing.expectEqualStrings("xxxxxxxxxx", uniform[0].value);
    try std.testing.expectEqualStrings("xxxxxxxxxx", uniform[499].value);

    // varying: "y" * ((i % 50) + 1)
    const varying = try reader.readColumn(1, []const u8);
    defer {
        for (varying) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(varying);
    }
    try std.testing.expectEqual(@as(usize, 500), varying.len);
    try std.testing.expectEqual(@as(usize, 1), varying[0].value.len); // "y"
    try std.testing.expectEqual(@as(usize, 2), varying[1].value.len); // "yy"
    try std.testing.expectEqual(@as(usize, 50), varying[49].value.len); // "y" * 50
    try std.testing.expectEqual(@as(usize, 1), varying[50].value.len); // wraps around

    // empty_and_long: ["", "a"*1000, "", "b"*500, ""] * 100
    const empty_long = try reader.readColumn(2, []const u8);
    defer {
        for (empty_long) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(empty_long);
    }
    try std.testing.expectEqual(@as(usize, 500), empty_long.len);
    try std.testing.expectEqual(@as(usize, 0), empty_long[0].value.len);
    try std.testing.expectEqual(@as(usize, 1000), empty_long[1].value.len);
    try std.testing.expectEqual(@as(usize, 0), empty_long[2].value.len);
    try std.testing.expectEqual(@as(usize, 500), empty_long[3].value.len);
}

test "read delta_byte_array.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/encodings/delta_byte_array.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // urls: "https://example.com/api/v1/users/{i}" for i in 0..499
    const urls = try reader.readColumn(0, []const u8);
    defer {
        for (urls) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(urls);
    }
    try std.testing.expectEqual(@as(usize, 500), urls.len);
    try std.testing.expectEqualStrings("https://example.com/api/v1/users/0", urls[0].value);
    try std.testing.expectEqualStrings("https://example.com/api/v1/users/1", urls[1].value);
    try std.testing.expectEqualStrings("https://example.com/api/v1/users/499", urls[499].value);

    // sorted: sorted "word_00000".."word_00499"
    const sorted = try reader.readColumn(2, []const u8);
    defer {
        for (sorted) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(sorted);
    }
    try std.testing.expectEqual(@as(usize, 500), sorted.len);
    try std.testing.expectEqualStrings("word_00000", sorted[0].value);
    try std.testing.expectEqualStrings("word_00001", sorted[1].value);
    try std.testing.expectEqualStrings("word_00499", sorted[499].value);

    // mixed: ["apple","application","apply","banana","bandana","band"] * 83 + ["unique","zoo"]
    const mixed = try reader.readColumn(3, []const u8);
    defer {
        for (mixed) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(mixed);
    }
    try std.testing.expectEqual(@as(usize, 500), mixed.len);
    try std.testing.expectEqualStrings("apple", mixed[0].value);
    try std.testing.expectEqualStrings("application", mixed[1].value);
    try std.testing.expectEqualStrings("apply", mixed[2].value);
    try std.testing.expectEqualStrings("banana", mixed[3].value);
    try std.testing.expectEqualStrings("bandana", mixed[4].value);
    try std.testing.expectEqualStrings("band", mixed[5].value);
    try std.testing.expectEqualStrings("unique", mixed[498].value);
    try std.testing.expectEqualStrings("zoo", mixed[499].value);
}

test "read delta_with_nulls_int.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/encodings/delta_with_nulls_int.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 1000), reader.metadata.num_rows);

    // int_with_nulls: i if i % 7 != 0 else null
    const values = try reader.readColumn(0, i32);
    defer allocator.free(values);
    try std.testing.expectEqual(@as(usize, 1000), values.len);

    try std.testing.expect(values[0].isNull()); // 0 % 7 == 0
    try std.testing.expectEqual(@as(i32, 1), values[1].value);
    try std.testing.expectEqual(@as(i32, 6), values[6].value);
    try std.testing.expect(values[7].isNull()); // 7 % 7 == 0
    try std.testing.expectEqual(@as(i32, 8), values[8].value);
    try std.testing.expect(values[14].isNull()); // 14 % 7 == 0
    try std.testing.expectEqual(@as(i32, 999), values[999].value);
}

test "read delta_with_nulls_string.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/encodings/delta_with_nulls_string.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 500), reader.metadata.num_rows);

    // str_with_nulls: "value_{i}" if i % 5 != 0 else null
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
    try std.testing.expectEqual(@as(usize, 500), values.len);

    try std.testing.expect(values[0].isNull()); // 0 % 5 == 0
    try std.testing.expectEqualStrings("value_1", values[1].value);
    try std.testing.expectEqualStrings("value_4", values[4].value);
    try std.testing.expect(values[5].isNull()); // 5 % 5 == 0
    try std.testing.expectEqualStrings("value_6", values[6].value);
    try std.testing.expect(values[10].isNull());
    try std.testing.expectEqualStrings("value_499", values[499].value);
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

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 1000), reader.metadata.num_rows);

    // sequential: float(i) * 0.1 for i in 0..999
    const sequential = try reader.readColumn(0, f32);
    defer allocator.free(sequential);
    try std.testing.expectEqual(@as(usize, 1000), sequential.len);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), sequential[0].value, 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.1), sequential[1].value, 0.001);
    try std.testing.expectApproxEqAbs(@as(f32, 99.9), sequential[999].value, 0.1);

    // special: [0.0, -0.0, inf, -inf, nan] * 200
    const special = try reader.readColumn(2, f32);
    defer allocator.free(special);
    try std.testing.expectEqual(@as(usize, 1000), special.len);
    try std.testing.expectEqual(@as(f32, 0.0), special[0].value);
    try std.testing.expect(std.math.isInf(special[2].value));
    try std.testing.expect(std.math.isNegativeInf(special[3].value));
    try std.testing.expect(std.math.isNan(special[4].value));
}

test "read byte_stream_split_double.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/encodings/byte_stream_split_double.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 1000), reader.metadata.num_rows);

    // sequential: float(i) * 0.001 for i in 0..999
    const sequential = try reader.readColumn(0, f64);
    defer allocator.free(sequential);
    try std.testing.expectEqual(@as(usize, 1000), sequential.len);
    try std.testing.expectApproxEqAbs(@as(f64, 0.0), sequential[0].value, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 0.001), sequential[1].value, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 0.999), sequential[999].value, 0.001);

    // precise: 1.0 + i * 1e-15
    const precise = try reader.readColumn(2, f64);
    defer allocator.free(precise);
    try std.testing.expectEqual(@as(usize, 1000), precise.len);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), precise[0].value, 1e-14);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0 + 999e-15), precise[999].value, 1e-14);
}

test "read byte_stream_split_int32.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/encodings/byte_stream_split_int32.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 1000), reader.metadata.num_rows);

    // sequential: 0..999
    const sequential = try reader.readColumn(0, i32);
    defer allocator.free(sequential);
    try std.testing.expectEqual(@as(usize, 1000), sequential.len);
    try std.testing.expectEqual(@as(i32, 0), sequential[0].value);
    try std.testing.expectEqual(@as(i32, 1), sequential[1].value);
    try std.testing.expectEqual(@as(i32, 999), sequential[999].value);
}

test "read byte_stream_split_int64.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/encodings/byte_stream_split_int64.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 1000), reader.metadata.num_rows);

    // sequential: 0..999
    const sequential = try reader.readColumn(0, i64);
    defer allocator.free(sequential);
    try std.testing.expectEqual(@as(usize, 1000), sequential.len);
    try std.testing.expectEqual(@as(i64, 0), sequential[0].value);
    try std.testing.expectEqual(@as(i64, 1), sequential[1].value);
    try std.testing.expectEqual(@as(i64, 999), sequential[999].value);

    // timestamps: 1700000000000 + i * 1000
    const timestamps = try reader.readColumn(2, i64);
    defer allocator.free(timestamps);
    try std.testing.expectEqual(@as(usize, 1000), timestamps.len);
    try std.testing.expectEqual(@as(i64, 1700000000000), timestamps[0].value);
    try std.testing.expectEqual(@as(i64, 1700000001000), timestamps[1].value);
    try std.testing.expectEqual(@as(i64, 1700000000000 + 999 * 1000), timestamps[999].value);
}

test "read byte_stream_split_float16.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/encodings/byte_stream_split_float16.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 200), reader.metadata.num_rows);

    // sequential: FLBA(2) values, read as raw bytes
    const sequential = try reader.readColumn(0, []const u8);
    defer {
        for (sequential) |v| {
            switch (v) {
                .value => |s| allocator.free(s),
                .null_value => {},
            }
        }
        allocator.free(sequential);
    }
    try std.testing.expectEqual(@as(usize, 200), sequential.len);
    try std.testing.expectEqual(@as(usize, 2), sequential[0].value.len);
}

test "read byte_stream_split_flba.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/encodings/byte_stream_split_flba.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 200), reader.metadata.num_rows);

    // fixed_bytes: FLBA(8), read as raw bytes
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
    try std.testing.expectEqual(@as(usize, 200), values.len);
    try std.testing.expectEqual(@as(usize, 8), values[0].value.len);
    try std.testing.expectEqual(@as(usize, 8), values[199].value.len);
}

test "read byte_stream_split_with_nulls.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/encodings/byte_stream_split_with_nulls.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 1000), reader.metadata.num_rows);

    // float_with_nulls: random float if i % 3 != 0 else null
    const values = try reader.readColumn(0, f32);
    defer allocator.free(values);
    try std.testing.expectEqual(@as(usize, 1000), values.len);

    try std.testing.expect(values[0].isNull()); // 0 % 3 == 0
    try std.testing.expect(!values[1].isNull());
    try std.testing.expect(!values[2].isNull());
    try std.testing.expect(values[3].isNull()); // 3 % 3 == 0
    try std.testing.expect(values[6].isNull()); // 6 % 3 == 0
    try std.testing.expect(values[999].isNull()); // 999 % 3 == 0
}
