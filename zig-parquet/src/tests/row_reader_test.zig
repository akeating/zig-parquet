//! Row Reader tests
//!
//! Tests for the high-level RowReader API.
//! Includes both round-trip tests and interop tests reading PyArrow-generated files.

const std = @import("std");
const parquet = @import("../lib.zig");
const types = @import("../core/types.zig");
const RowWriter = parquet.RowWriter;
const RowReader = parquet.RowReader;
const RowReaderError = parquet.RowReaderError;
const build_options = @import("build_options");

// =============================================================================
// Basic Flat Struct Tests
// =============================================================================

const SimpleStruct = struct {
    id: i32,
    value: i64,
    score: f64,
};

test "RowReader: simple flat struct round-trip" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("simple_roundtrip.parquet", .{ .read = true });
    defer file.close();

    // Write data
    const rows = [_]SimpleStruct{
        .{ .id = 1, .value = 100, .score = 1.5 },
        .{ .id = 2, .value = 200, .score = 2.5 },
        .{ .id = 3, .value = 300, .score = 3.5 },
    };

    {
        var writer = try parquet.writeToFileRows(SimpleStruct, allocator, file, .{});
        defer writer.deinit();

        for (rows) |row| {
            try writer.writeRow(row);
        }
        try writer.close();
    }

    // Read back
    try file.seekTo(0);
    {
        var reader = try parquet.openFileRowReader(SimpleStruct, allocator, file, .{});
        defer reader.deinit();

        try std.testing.expectEqual(@as(usize, 3), reader.rowCount());

        // Read all rows
        var i: usize = 0;
        while (try reader.next()) |row| {
            try std.testing.expectEqual(rows[i].id, row.id);
            try std.testing.expectEqual(rows[i].value, row.value);
            try std.testing.expectEqual(rows[i].score, row.score);
            i += 1;
        }
        try std.testing.expectEqual(@as(usize, 3), i);
    }
}

// =============================================================================
// Optional Fields Tests
// =============================================================================

const WithOptionals = struct {
    id: i32,
    name: ?i64,
    active: ?bool,
};

test "RowReader: optional fields round-trip" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("optional_roundtrip.parquet", .{ .read = true });
    defer file.close();

    // Write data with some nulls
    {
        var writer = try parquet.writeToFileRows(WithOptionals, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{ .id = 1, .name = 100, .active = true });
        try writer.writeRow(.{ .id = 2, .name = null, .active = false });
        try writer.writeRow(.{ .id = 3, .name = 300, .active = null });
        try writer.close();
    }

    // Read back
    try file.seekTo(0);
    {
        var reader = try parquet.openFileRowReader(WithOptionals, allocator, file, .{});
        defer reader.deinit();

        const row1 = (try reader.next()).?;
        try std.testing.expectEqual(@as(i32, 1), row1.id);
        try std.testing.expectEqual(@as(?i64, 100), row1.name);
        try std.testing.expectEqual(@as(?bool, true), row1.active);

        const row2 = (try reader.next()).?;
        try std.testing.expectEqual(@as(i32, 2), row2.id);
        try std.testing.expectEqual(@as(?i64, null), row2.name);
        try std.testing.expectEqual(@as(?bool, false), row2.active);

        const row3 = (try reader.next()).?;
        try std.testing.expectEqual(@as(i32, 3), row3.id);
        try std.testing.expectEqual(@as(?i64, 300), row3.name);
        try std.testing.expectEqual(@as(?bool, null), row3.active);

        try std.testing.expect((try reader.next()) == null);
    }
}

// =============================================================================
// Nested Struct Tests
// =============================================================================

const Address = struct {
    city: i32, // Simplified to i32 for now (avoiding string complexity)
    zip: i32,
};

const Person = struct {
    id: i32,
    address: Address,
};

test "RowReader: nested struct round-trip" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("nested_roundtrip.parquet", .{ .read = true });
    defer file.close();

    // Write data
    {
        var writer = try parquet.writeToFileRows(Person, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{ .id = 1, .address = .{ .city = 100, .zip = 12345 } });
        try writer.writeRow(.{ .id = 2, .address = .{ .city = 200, .zip = 67890 } });
        try writer.close();
    }

    // Read back
    try file.seekTo(0);
    {
        var reader = try parquet.openFileRowReader(Person, allocator, file, .{});
        defer reader.deinit();

        const row1 = (try reader.next()).?;
        try std.testing.expectEqual(@as(i32, 1), row1.id);
        try std.testing.expectEqual(@as(i32, 100), row1.address.city);
        try std.testing.expectEqual(@as(i32, 12345), row1.address.zip);

        const row2 = (try reader.next()).?;
        try std.testing.expectEqual(@as(i32, 2), row2.id);
        try std.testing.expectEqual(@as(i32, 200), row2.address.city);
        try std.testing.expectEqual(@as(i32, 67890), row2.address.zip);

        try std.testing.expect((try reader.next()) == null);
    }
}

// =============================================================================
// readAll Tests
// =============================================================================

test "RowReader: readAll returns all rows" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("readall.parquet", .{ .read = true });
    defer file.close();

    // Write data
    {
        var writer = try parquet.writeToFileRows(SimpleStruct, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{ .id = 1, .value = 10, .score = 0.1 });
        try writer.writeRow(.{ .id = 2, .value = 20, .score = 0.2 });
        try writer.writeRow(.{ .id = 3, .value = 30, .score = 0.3 });
        try writer.writeRow(.{ .id = 4, .value = 40, .score = 0.4 });
        try writer.close();
    }

    // Read back with readAll
    try file.seekTo(0);
    {
        var reader = try parquet.openFileRowReader(SimpleStruct, allocator, file, .{});
        defer reader.deinit();

        const rows = try reader.readAll();
        defer reader.freeRows(rows);

        try std.testing.expectEqual(@as(usize, 4), rows.len);
        try std.testing.expectEqual(@as(i32, 1), rows[0].id);
        try std.testing.expectEqual(@as(i32, 4), rows[3].id);
    }
}

// =============================================================================
// Reset Tests
// =============================================================================

test "RowReader: reset allows re-reading" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("reset.parquet", .{ .read = true });
    defer file.close();

    // Write data
    {
        var writer = try parquet.writeToFileRows(SimpleStruct, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{ .id = 1, .value = 10, .score = 0.1 });
        try writer.writeRow(.{ .id = 2, .value = 20, .score = 0.2 });
        try writer.close();
    }

    // Read, reset, read again
    try file.seekTo(0);
    {
        var reader = try parquet.openFileRowReader(SimpleStruct, allocator, file, .{});
        defer reader.deinit();

        // First read
        const row1 = (try reader.next()).?;
        try std.testing.expectEqual(@as(i32, 1), row1.id);
        _ = try reader.next(); // row2
        try std.testing.expect((try reader.next()) == null);

        // Reset and read again
        reader.reset();
        const row1_again = (try reader.next()).?;
        try std.testing.expectEqual(@as(i32, 1), row1_again.id);
    }
}

// =============================================================================
// Arrow Interop Tests - Reading PyArrow-generated files
// =============================================================================

// Note: These tests verify that RowReader can read files created by PyArrow.
// Some complex types (strings, lists, maps) have limited support.

// --- basic/boundary_values.parquet ---
// Schema: i8, i16, i32, i64, u8, u16, u32, u64

const BoundaryValues = struct {
    i8: ?i8,
    i16: ?i16,
    i32: ?i32,
    i64: ?i64,
    u8: ?u8,
    u16: ?u16,
    u32: ?u32,
    u64: ?u64,
};

test "RowReader interop: boundary_values.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/basic/boundary_values.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file (run from zig-parquet dir): {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(BoundaryValues, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 3), reader.rowCount());

    // Row 0: min values
    const row0 = (try reader.next()).?;
    try std.testing.expectEqual(@as(?i8, -128), row0.i8);
    try std.testing.expectEqual(@as(?i16, -32768), row0.i16);
    try std.testing.expectEqual(@as(?i32, -2147483648), row0.i32);
    try std.testing.expectEqual(@as(?i64, -9223372036854775808), row0.i64);
    try std.testing.expectEqual(@as(?u8, 0), row0.u8);
    try std.testing.expectEqual(@as(?u16, 0), row0.u16);
    try std.testing.expectEqual(@as(?u32, 0), row0.u32);
    try std.testing.expectEqual(@as(?u64, 0), row0.u64);

    // Row 1: max values
    const row1 = (try reader.next()).?;
    try std.testing.expectEqual(@as(?i8, 127), row1.i8);
    try std.testing.expectEqual(@as(?i16, 32767), row1.i16);
    try std.testing.expectEqual(@as(?i32, 2147483647), row1.i32);
    try std.testing.expectEqual(@as(?i64, 9223372036854775807), row1.i64);
    try std.testing.expectEqual(@as(?u8, 255), row1.u8);
    try std.testing.expectEqual(@as(?u16, 65535), row1.u16);
    try std.testing.expectEqual(@as(?u32, 4294967295), row1.u32);
    try std.testing.expectEqual(@as(?u64, 18446744073709551615), row1.u64);

    // Row 2: zeros
    const row2 = (try reader.next()).?;
    try std.testing.expectEqual(@as(?i8, 0), row2.i8);
    try std.testing.expectEqual(@as(?i32, 0), row2.i32);
}

// --- edge_cases/single_value.parquet ---
// Schema: col (INT32)

const SingleValue = struct {
    col: ?i32,
};

test "RowReader interop: single_value.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/edge_cases/single_value.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(SingleValue, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 1), reader.rowCount());

    const row = (try reader.next()).?;
    try std.testing.expectEqual(@as(?i32, 42), row.col);

    try std.testing.expect((try reader.next()) == null);
}

// --- edge_cases/empty_table.parquet ---

test "RowReader interop: empty_table.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/edge_cases/empty_table.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(SingleValue, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 0), reader.rowCount());
    try std.testing.expect((try reader.next()) == null);
}

// --- logical_types/int_types.parquet ---
// Schema: int8_col, int16_col, int32_col, int64_col, uint8_col, uint16_col, uint32_col, uint64_col

const IntTypes = struct {
    int8_col: ?i8,
    int16_col: ?i16,
    int32_col: ?i32,
    int64_col: ?i64,
    uint8_col: ?u8,
    uint16_col: ?u16,
    uint32_col: ?u32,
    uint64_col: ?u64,
};

test "RowReader interop: int_types.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/logical_types/int_types.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(IntTypes, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 5), reader.rowCount());

    // Row 0: value 1
    const row0 = (try reader.next()).?;
    try std.testing.expectEqual(@as(?i8, 1), row0.int8_col);
    try std.testing.expectEqual(@as(?i16, 1), row0.int16_col);
    try std.testing.expectEqual(@as(?u8, 0), row0.uint8_col);
    try std.testing.expectEqual(@as(?u64, 0), row0.uint64_col);

    // Row 1: boundary values
    const row1 = (try reader.next()).?;
    try std.testing.expectEqual(@as(?i8, -128), row1.int8_col);
    try std.testing.expectEqual(@as(?i16, -32768), row1.int16_col);
    try std.testing.expectEqual(@as(?u8, 128), row1.uint8_col);

    // Row 2: max values
    const row2 = (try reader.next()).?;
    try std.testing.expectEqual(@as(?i8, 127), row2.int8_col);
    try std.testing.expectEqual(@as(?u8, 255), row2.uint8_col);

    // Row 3: zeros
    _ = try reader.next();

    // Row 4: nulls
    const row4 = (try reader.next()).?;
    try std.testing.expectEqual(@as(?i8, null), row4.int8_col);
    try std.testing.expectEqual(@as(?u64, null), row4.uint64_col);
}

// --- compression tests ---
// All compression files have schema: repeated (STRING), sequence (INT64)
// We test only the sequence column to avoid string handling complexity

const CompressionTest = struct {
    repeated: ?i64, // Actually a string, but we'll get schema mismatch - skip
    sequence: ?i64,
};

// Note: Compression tests would require matching the exact schema.
// The files have (repeated: STRING, sequence: INT64) but RowReader
// requires exact schema match. We'll skip these for now as they
// demonstrate a limitation - RowReader needs exact type matching.

// --- logical_types/date.parquet ---
// Schema: date_col (DATE32), date_required (DATE32)

const DateTest = struct {
    date_col: ?i32, // DATE is stored as i32 days since epoch
    date_required: ?i32,
};

test "RowReader interop: date.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/logical_types/date.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(DateTest, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 5), reader.rowCount());

    // Row 0: 2024-01-15 = 19737 days since epoch
    const row0 = (try reader.next()).?;
    try std.testing.expectEqual(@as(?i32, 19737), row0.date_col);
    try std.testing.expectEqual(@as(?i32, 18262), row0.date_required); // 2020-01-01

    // Row 1: 1970-01-01 = 0 days since epoch
    const row1 = (try reader.next()).?;
    try std.testing.expectEqual(@as(?i32, 0), row1.date_col);

    // Skip to row 4: null
    _ = try reader.next();
    _ = try reader.next();
    const row4 = (try reader.next()).?;
    try std.testing.expectEqual(@as(?i32, null), row4.date_col);
}

// --- logical_types/timestamp.parquet ---
// Schema has multiple timestamp columns at different precisions
// We'll test with i64 since that's the storage type

const TimestampTest = struct {
    ts_millis_utc: ?i64,
    ts_micros_utc: ?i64,
    ts_nanos_utc: ?i64,
    ts_micros_local: ?i64,
};

test "RowReader interop: timestamp.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/logical_types/timestamp.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(TimestampTest, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 5), reader.rowCount());

    // Row 0: 2024-01-15 10:30:00 UTC
    const row0 = (try reader.next()).?;
    try std.testing.expect(row0.ts_millis_utc != null);
    try std.testing.expect(row0.ts_micros_utc != null);
    // Millis should be 1000x less than micros
    try std.testing.expectEqual(row0.ts_millis_utc.? * 1000, row0.ts_micros_utc.?);

    // Row 1: 1970-01-01 00:00:00 = epoch = 0
    const row1 = (try reader.next()).?;
    try std.testing.expectEqual(@as(?i64, 0), row1.ts_millis_utc);
    try std.testing.expectEqual(@as(?i64, 0), row1.ts_micros_utc);
    try std.testing.expectEqual(@as(?i64, 0), row1.ts_nanos_utc);

    // Row 3: null
    _ = try reader.next();
    const row3 = (try reader.next()).?;
    try std.testing.expectEqual(@as(?i64, null), row3.ts_millis_utc);
    try std.testing.expectEqual(@as(?i64, null), row3.ts_micros_utc);
}

// --- logical_types/time.parquet ---
// Schema: time_millis (TIME32/ms), time_micros (TIME64/us), time_nanos (TIME64/ns)

const TimeTest = struct {
    time_millis: ?i32, // TIME32 stored as i32
    time_micros: ?i64, // TIME64 stored as i64
    time_nanos: ?i64,
};

test "RowReader interop: time.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/logical_types/time.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(TimeTest, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 5), reader.rowCount());

    // Row 0: 10:30:00 = 10*3600 + 30*60 = 37800 seconds
    const row0 = (try reader.next()).?;
    try std.testing.expectEqual(@as(?i32, 37800000), row0.time_millis); // 37800 * 1000 ms
    try std.testing.expectEqual(@as(?i64, 37800000000), row0.time_micros); // 37800 * 1000000 us

    // Row 1: 00:00:00 = 0
    const row1 = (try reader.next()).?;
    try std.testing.expectEqual(@as(?i32, 0), row1.time_millis);
    try std.testing.expectEqual(@as(?i64, 0), row1.time_micros);

    // Row 3: null
    _ = try reader.next();
    const row3 = (try reader.next()).?;
    try std.testing.expectEqual(@as(?i32, null), row3.time_millis);
    try std.testing.expectEqual(@as(?i64, null), row3.time_micros);
}

// --- logical_types/float16.parquet ---
// Note: float16 requires special handling (FIXED_LEN_BYTE_ARRAY(2))
// and PlainDecoder doesn't support [2]u8 arrays yet.
// This file is skipped for RowReader interop testing.

// --- structure/multiple_row_groups.parquet ---
// Schema: id (INT64), value (STRING)
// This file has 10 row groups of 10000 rows each

const MultipleRowGroupsTest = struct {
    id: ?i64,
    value: ?[]const u8, // STRING column
};

test "RowReader interop: multiple_row_groups.parquet - first row group" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/structure/multiple_row_groups.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file (run from zig-parquet dir): {}\n", .{err});
        return err;
    };
    defer file.close();

    // Read just the first row group
    var reader = try parquet.openFileRowReader(MultipleRowGroupsTest, allocator, file, .{ .row_group_index = 0 });
    defer reader.deinit();

    // Each row group has 10000 rows
    try std.testing.expectEqual(@as(usize, 10000), reader.rowCount());

    // First row should be id=0
    const row0 = (try reader.next()).?;
    defer if (row0.value) |v| allocator.free(v);
    try std.testing.expectEqual(@as(?i64, 0), row0.id);
    try std.testing.expectEqualStrings("row-0", row0.value.?);

    // Read a few more to verify
    const row1 = (try reader.next()).?;
    defer if (row1.value) |v| allocator.free(v);
    try std.testing.expectEqual(@as(?i64, 1), row1.id);
    try std.testing.expectEqualStrings("row-1", row1.value.?);
}

test "RowReader interop: multiple_row_groups.parquet - second row group" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/structure/multiple_row_groups.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    // Read the second row group (index 1)
    var reader = try parquet.openFileRowReader(MultipleRowGroupsTest, allocator, file, .{ .row_group_index = 1 });
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 10000), reader.rowCount());

    // First row in second row group should be id=10000
    const row0 = (try reader.next()).?;
    defer if (row0.value) |v| allocator.free(v);
    try std.testing.expectEqual(@as(?i64, 10000), row0.id);
    try std.testing.expectEqualStrings("row-10000", row0.value.?);
}

// --- edge_cases/large_strings.parquet ---
// Schema: small (STRING, "x" * 10), medium (STRING, "y" * 1000), large (STRING, "z" * 100000)

const LargeStringsTest = struct {
    small: ?[]const u8,
    medium: ?[]const u8,
    large: ?[]const u8,
};

test "RowReader interop: large_strings.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/edge_cases/large_strings.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(LargeStringsTest, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 10), reader.rowCount());

    // Each row has small="x", medium="y"*1000, large="z"*100000
    const row0 = (try reader.next()).?;
    defer {
        if (row0.small) |v| allocator.free(v);
        if (row0.medium) |v| allocator.free(v);
        if (row0.large) |v| allocator.free(v);
    }

    try std.testing.expectEqual(@as(usize, 1), row0.small.?.len);
    try std.testing.expectEqual(@as(u8, 'x'), row0.small.?[0]);

    try std.testing.expectEqual(@as(usize, 1000), row0.medium.?.len);
    try std.testing.expectEqual(@as(u8, 'y'), row0.medium.?[0]);

    try std.testing.expectEqual(@as(usize, 100000), row0.large.?.len);
    try std.testing.expectEqual(@as(u8, 'z'), row0.large.?[0]);
}

// --- basic/basic_types_plain_uncompressed.parquet ---
// The most comprehensive test - all basic types
// Schema: bool_col, int32_col, int64_col, float_col, double_col, string_col, binary_col, fixed_binary_col

const BasicTypesTest = struct {
    bool_col: ?bool,
    int32_col: ?i32,
    int64_col: ?i64,
    float_col: ?f32,
    double_col: ?f64,
    string_col: ?[]const u8,
    binary_col: ?[]const u8,
    fixed_binary_col: ?[]const u8, // Use []const u8 for FIXED_LEN_BYTE_ARRAY
};

test "RowReader interop: basic_types_plain_uncompressed.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/basic/basic_types_plain_uncompressed.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(BasicTypesTest, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 5), reader.rowCount());

    // Row 0: true, 1, 1, 1.5, 1.5, "hello", [0,1,2], uuid1
    const row0 = (try reader.next()).?;
    defer {
        if (row0.string_col) |v| allocator.free(v);
        if (row0.binary_col) |v| allocator.free(v);
        if (row0.fixed_binary_col) |v| allocator.free(v);
    }
    try std.testing.expectEqual(@as(?bool, true), row0.bool_col);
    try std.testing.expectEqual(@as(?i32, 1), row0.int32_col);
    try std.testing.expectEqual(@as(?i64, 1), row0.int64_col);
    try std.testing.expectEqual(@as(?f32, 1.5), row0.float_col);
    try std.testing.expectEqual(@as(?f64, 1.5), row0.double_col);
    try std.testing.expectEqualStrings("hello", row0.string_col.?);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x00, 0x01, 0x02 }, row0.binary_col.?);
    // First UUID: 0x12345678...
    try std.testing.expectEqual(@as(usize, 16), row0.fixed_binary_col.?.len);
    try std.testing.expectEqual(@as(u8, 0x12), row0.fixed_binary_col.?[0]);

    // Row 1: false, -2, -2, -2.5, -2.5, "world", [], uuid2
    const row1 = (try reader.next()).?;
    defer {
        if (row1.string_col) |v| allocator.free(v);
        if (row1.binary_col) |v| allocator.free(v);
        if (row1.fixed_binary_col) |v| allocator.free(v);
    }
    try std.testing.expectEqual(@as(?bool, false), row1.bool_col);
    try std.testing.expectEqual(@as(?i32, -2), row1.int32_col);
    try std.testing.expectEqualStrings("world", row1.string_col.?);
    // Empty binary
    try std.testing.expectEqual(@as(usize, 0), row1.binary_col.?.len);

    // Row 2: true, max_i32, max_i64, inf, max_f64, "", [0xff...], uuid3
    const row2 = (try reader.next()).?;
    defer {
        if (row2.string_col) |v| allocator.free(v);
        if (row2.binary_col) |v| allocator.free(v);
        if (row2.fixed_binary_col) |v| allocator.free(v);
    }
    try std.testing.expectEqual(@as(?bool, true), row2.bool_col);
    try std.testing.expectEqual(@as(?i32, 2147483647), row2.int32_col);
    try std.testing.expectEqual(@as(?i64, 9223372036854775807), row2.int64_col);
    try std.testing.expect(std.math.isPositiveInf(row2.float_col.?));
    try std.testing.expectEqualStrings("", row2.string_col.?);

    // Row 3: null bool, min_i32, null i64, -inf, null, null string, [0xff]*100, null uuid
    const row3 = (try reader.next()).?;
    defer {
        if (row3.string_col) |v| allocator.free(v);
        if (row3.binary_col) |v| allocator.free(v);
        if (row3.fixed_binary_col) |v| allocator.free(v);
    }
    try std.testing.expectEqual(@as(?bool, null), row3.bool_col);
    try std.testing.expectEqual(@as(?i32, -2147483648), row3.int32_col);
    try std.testing.expectEqual(@as(?i64, null), row3.int64_col);
    try std.testing.expect(std.math.isNegativeInf(row3.float_col.?));
    try std.testing.expectEqual(@as(?f64, null), row3.double_col);
    try std.testing.expectEqual(@as(?[]const u8, null), row3.string_col);
    // binary is 100 0xff bytes
    try std.testing.expectEqual(@as(usize, 100), row3.binary_col.?.len);

    // Row 4: false, null, 0, null, 0.0, "🎉 unicode", "test", uuid4
    const row4 = (try reader.next()).?;
    defer {
        if (row4.string_col) |v| allocator.free(v);
        if (row4.binary_col) |v| allocator.free(v);
        if (row4.fixed_binary_col) |v| allocator.free(v);
    }
    try std.testing.expectEqual(@as(?bool, false), row4.bool_col);
    try std.testing.expectEqual(@as(?i32, null), row4.int32_col);
    try std.testing.expectEqual(@as(?i64, 0), row4.int64_col);
    try std.testing.expectEqualStrings("🎉 unicode", row4.string_col.?);
}

// --- Summary of interop test coverage ---
// TESTED (Arrow-generated files):
// - basic/boundary_values.parquet (integer boundary values for all int types)
// - basic/basic_types_plain_uncompressed.parquet (bool, ints, floats, strings, binary)
// - edge_cases/single_value.parquet (minimal 1-row file)
// - edge_cases/empty_table.parquet (zero rows)
// - edge_cases/large_strings.parquet (1, 1000, 100000 byte strings)
// - logical_types/int_types.parquet (i8, i16, i32, i64, u8, u16, u32, u64)
// - logical_types/date.parquet (DATE32 logical type)
// - logical_types/timestamp.parquet (TIMESTAMP millis/micros/nanos, UTC/local)
// - logical_types/time.parquet (TIME32 millis, TIME64 micros/nanos)
// - structure/multiple_row_groups.parquet (10 row groups, row_group_index selection)
//
// TESTED (list reconstruction):
// - nested/list_int.parquet
// - nested/list_float.parquet
// - nested/list_string.parquet
//
// TESTED (dictionary encoding):
// - encodings/dictionary_low_cardinality.parquet
// - encodings/dictionary_high_cardinality.parquet
//
// TESTED (compression codecs):
// - compression/compression_zstd.parquet
// - compression/compression_gzip.parquet
// - compression/compression_snappy.parquet
// - compression/compression_lz4.parquet
// - compression/compression_brotli.parquet
//
// TESTED (float16):
// - logical_types/float16.parquet
//
// TESTED (decimal):
// - logical_types/decimal.parquet (all three columns: precision 9, 18, 38)
//
// TESTED (nested types):
// - nested/list_of_list.parquet - nested lists (5-level schema)
// - nested/list_bool.parquet - bool list decoding
// - nested/list_struct.parquet - list of struct
// - nested/list_nullable.parquet - nullable list elements
// - nested/map_string_int.parquet - maps
// - nested/struct_with_list.parquet - mixed struct with list
// - nested/struct_with_temporal.parquet - struct with temporal types
// - nested/struct_simple.parquet - top-level struct column
// - nested/struct_nested.parquet - nested struct columns

// =============================================================================
// List Reconstruction Tests
// =============================================================================

// --- nested/list_int.parquet ---
// Schema: list_col (list<int32>)
// Data: [[1,2,3], [4,5], [6], [], [7,8,9,10]]

const ListIntTest = struct {
    list_col: []const i32,
};

test "RowReader interop: list_int.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/nested/list_int.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(ListIntTest, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 5), reader.rowCount());

    // Row 0: [1, 2, 3]
    const row0 = (try reader.next()).?;
    defer allocator.free(row0.list_col);
    try std.testing.expectEqual(@as(usize, 3), row0.list_col.len);
    try std.testing.expectEqual(@as(i32, 1), row0.list_col[0]);
    try std.testing.expectEqual(@as(i32, 2), row0.list_col[1]);
    try std.testing.expectEqual(@as(i32, 3), row0.list_col[2]);

    // Row 1: [4, 5]
    const row1 = (try reader.next()).?;
    defer allocator.free(row1.list_col);
    try std.testing.expectEqual(@as(usize, 2), row1.list_col.len);
    try std.testing.expectEqual(@as(i32, 4), row1.list_col[0]);
    try std.testing.expectEqual(@as(i32, 5), row1.list_col[1]);

    // Row 2: [6]
    const row2 = (try reader.next()).?;
    defer allocator.free(row2.list_col);
    try std.testing.expectEqual(@as(usize, 1), row2.list_col.len);
    try std.testing.expectEqual(@as(i32, 6), row2.list_col[0]);

    // Row 3: [] (empty)
    const row3 = (try reader.next()).?;
    defer allocator.free(row3.list_col);
    try std.testing.expectEqual(@as(usize, 0), row3.list_col.len);

    // Row 4: [7, 8, 9, 10]
    const row4 = (try reader.next()).?;
    defer allocator.free(row4.list_col);
    try std.testing.expectEqual(@as(usize, 4), row4.list_col.len);
    try std.testing.expectEqual(@as(i32, 7), row4.list_col[0]);
    try std.testing.expectEqual(@as(i32, 10), row4.list_col[3]);
}

// --- nested/list_float.parquet ---
// Schema: list_col (list<float64>)
// Data: [[1.5, -2.5, 0.0], [inf, -inf, nan], null, [], [3.14159]]

const ListFloatTest = struct {
    list_col: []const f64,
};

test "RowReader interop: list_float.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/nested/list_float.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(ListFloatTest, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 5), reader.rowCount());

    // Row 0: [1.5, -2.5, 0.0]
    const row0 = (try reader.next()).?;
    defer allocator.free(row0.list_col);
    try std.testing.expectEqual(@as(usize, 3), row0.list_col.len);
    try std.testing.expectEqual(@as(f64, 1.5), row0.list_col[0]);
    try std.testing.expectEqual(@as(f64, -2.5), row0.list_col[1]);
    try std.testing.expectEqual(@as(f64, 0.0), row0.list_col[2]);

    // Row 1: [inf, -inf, nan]
    const row1 = (try reader.next()).?;
    defer allocator.free(row1.list_col);
    try std.testing.expectEqual(@as(usize, 3), row1.list_col.len);
    try std.testing.expect(std.math.isPositiveInf(row1.list_col[0]));
    try std.testing.expect(std.math.isNegativeInf(row1.list_col[1]));
    try std.testing.expect(std.math.isNan(row1.list_col[2]));

    // Row 2: null (skip for non-optional list)
    _ = try reader.next();

    // Row 3: []
    const row3 = (try reader.next()).?;
    defer allocator.free(row3.list_col);
    try std.testing.expectEqual(@as(usize, 0), row3.list_col.len);

    // Row 4: [3.14159]
    const row4 = (try reader.next()).?;
    defer allocator.free(row4.list_col);
    try std.testing.expectEqual(@as(usize, 1), row4.list_col.len);
    try std.testing.expectApproxEqRel(@as(f64, 3.14159), row4.list_col[0], 0.00001);
}

// --- nested/list_string.parquet ---
// Schema: list_col (list<string>)
// Data: [["hello", "world"], ["", null, "test"], null, [], ["unicode: 🎉"]]

const ListStringTest = struct {
    list_col: []const []const u8,
};

test "RowReader interop: list_string.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/nested/list_string.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(ListStringTest, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 5), reader.rowCount());

    // Row 0: ["hello", "world"]
    const row0 = (try reader.next()).?;
    defer {
        for (row0.list_col) |s| allocator.free(s);
        allocator.free(row0.list_col);
    }
    try std.testing.expectEqual(@as(usize, 2), row0.list_col.len);
    try std.testing.expectEqualStrings("hello", row0.list_col[0]);
    try std.testing.expectEqualStrings("world", row0.list_col[1]);
}

// =============================================================================
// Decimal Test
// =============================================================================

// --- logical_types/decimal.parquet ---
// Schema: decimal_9_2, decimal_18_4, decimal_38_10
// Testing first column only: decimal_9_2 (precision 9, scale 2)
// Data: [123.45, -999.99, 0.01, null, 1234567.89]

const DecimalTest = struct {
    decimal_9_2: ?types.DecimalValue,
    decimal_18_4: ?types.DecimalValue,
    decimal_38_10: ?types.DecimalValue,
};

test "RowReader interop: decimal.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/logical_types/decimal.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(DecimalTest, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 5), reader.rowCount());

    // Row 0: decimal_9_2=123.45, decimal_18_4=12345678901234.5678, decimal_38_10=large
    const row0 = (try reader.next()).?;
    try std.testing.expectApproxEqRel(@as(f64, 123.45), row0.decimal_9_2.?.toF64(), 0.001);
    try std.testing.expectApproxEqRel(@as(f64, 12345678901234.5678), row0.decimal_18_4.?.toF64(), 0.000001);
    try std.testing.expect(row0.decimal_38_10 != null); // Very large value, just check not null

    // Row 1: decimal_9_2=-999.99, decimal_18_4=-99999999999999.9999, decimal_38_10=null
    const row1 = (try reader.next()).?;
    try std.testing.expectApproxEqRel(@as(f64, -999.99), row1.decimal_9_2.?.toF64(), 0.001);
    try std.testing.expectApproxEqRel(@as(f64, -99999999999999.9999), row1.decimal_18_4.?.toF64(), 0.000001);
    try std.testing.expect(row1.decimal_38_10 == null);

    // Row 2: decimal_9_2=0.01, decimal_18_4=null, decimal_38_10=-1E-10
    const row2 = (try reader.next()).?;
    try std.testing.expectApproxEqRel(@as(f64, 0.01), row2.decimal_9_2.?.toF64(), 0.001);
    try std.testing.expect(row2.decimal_18_4 == null);
    try std.testing.expectApproxEqRel(@as(f64, -1e-10), row2.decimal_38_10.?.toF64(), 1e-15);

    // Row 3: decimal_9_2=null, decimal_18_4=0.0001, decimal_38_10=large
    const row3 = (try reader.next()).?;
    try std.testing.expectEqual(@as(?types.DecimalValue, null), row3.decimal_9_2);
    try std.testing.expectApproxEqRel(@as(f64, 0.0001), row3.decimal_18_4.?.toF64(), 0.000001);
    try std.testing.expect(row3.decimal_38_10 != null);

    // Row 4: decimal_9_2=1234567.89, decimal_18_4=1.0000, decimal_38_10=0
    const row4 = (try reader.next()).?;
    try std.testing.expectApproxEqRel(@as(f64, 1234567.89), row4.decimal_9_2.?.toF64(), 0.001);
    try std.testing.expectApproxEqRel(@as(f64, 1.0), row4.decimal_18_4.?.toF64(), 0.000001);
    try std.testing.expectApproxEqRel(@as(f64, 0.0), row4.decimal_38_10.?.toF64(), 1e-15);
}

// =============================================================================
// Float16 Test
// =============================================================================

// --- logical_types/float16.parquet ---
// Schema: float16_col (HALFFLOAT)
// Data: [1.5, -2.25, 0.0, 65504.0, None]

const Float16Test = struct {
    float16_col: ?f16,
};

test "RowReader interop: float16.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/logical_types/float16.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(Float16Test, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 5), reader.rowCount());

    // Row 0: 1.5
    const row0 = (try reader.next()).?;
    try std.testing.expectApproxEqRel(@as(f32, 1.5), @as(f32, row0.float16_col.?), 0.01);

    // Row 1: -2.25
    const row1 = (try reader.next()).?;
    try std.testing.expectApproxEqRel(@as(f32, -2.25), @as(f32, row1.float16_col.?), 0.01);

    // Row 2: 0.0
    const row2 = (try reader.next()).?;
    try std.testing.expectApproxEqRel(@as(f32, 0.0), @as(f32, row2.float16_col.?), 0.01);

    // Row 3: 65504.0 (max finite f16)
    const row3 = (try reader.next()).?;
    try std.testing.expectApproxEqRel(@as(f32, 65504.0), @as(f32, row3.float16_col.?), 0.01);

    // Row 4: null
    const row4 = (try reader.next()).?;
    try std.testing.expectEqual(@as(?f16, null), row4.float16_col);
}

// =============================================================================
// Dictionary Encoding Tests
// =============================================================================

// --- encodings/dictionary_low_cardinality.parquet ---
// Schema: status (STRING, 3 unique values), category (STRING, 4 unique values)
// 3000 rows

const DictionaryLowCardTest = struct {
    status: ?[]const u8,
    category: ?[]const u8,
};

test "RowReader interop: dictionary_low_cardinality.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/encodings/dictionary_low_cardinality.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(DictionaryLowCardTest, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 3000), reader.rowCount());

    // Read first few rows and verify dictionary-encoded values
    const row0 = (try reader.next()).?;
    defer {
        if (row0.status) |s| allocator.free(s);
        if (row0.category) |c| allocator.free(c);
    }
    try std.testing.expectEqualStrings("active", row0.status.?);
    try std.testing.expectEqualStrings("A", row0.category.?);

    const row1 = (try reader.next()).?;
    defer {
        if (row1.status) |s| allocator.free(s);
        if (row1.category) |c| allocator.free(c);
    }
    try std.testing.expectEqualStrings("inactive", row1.status.?);
    try std.testing.expectEqualStrings("B", row1.category.?);

    const row2 = (try reader.next()).?;
    defer {
        if (row2.status) |s| allocator.free(s);
        if (row2.category) |c| allocator.free(c);
    }
    try std.testing.expectEqualStrings("pending", row2.status.?);
    try std.testing.expectEqualStrings("C", row2.category.?);
}

// --- encodings/dictionary_high_cardinality.parquet ---
// Schema: uuid (STRING, 3000 unique values)

const DictionaryHighCardTest = struct {
    uuid: ?[]const u8,
};

test "RowReader interop: dictionary_high_cardinality.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/encodings/dictionary_high_cardinality.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(DictionaryHighCardTest, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 3000), reader.rowCount());

    // Verify first value
    const row0 = (try reader.next()).?;
    defer if (row0.uuid) |u| allocator.free(u);
    try std.testing.expectEqualStrings("uuid-000000", row0.uuid.?);

    // Skip to near end
    for (0..2998) |_| {
        const row = (try reader.next()).?;
        if (row.uuid) |u| allocator.free(u);
    }

    // Verify last value
    const row_last = (try reader.next()).?;
    defer if (row_last.uuid) |u| allocator.free(u);
    try std.testing.expectEqualStrings("uuid-002999", row_last.uuid.?);
}

// =============================================================================
// Compression Tests
// =============================================================================

// All compression files have schema: repeated (STRING), sequence (INT64)

const CompressionInteropTest = struct {
    repeated: ?[]const u8,
    sequence: ?i64,
};

test "RowReader interop: compression_zstd.parquet" {
    if (build_options.no_compression) return;
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/compression/compression_zstd.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(CompressionInteropTest, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 10000), reader.rowCount());

    // First row
    const row0 = (try reader.next()).?;
    defer if (row0.repeated) |r| allocator.free(r);
    try std.testing.expectEqualStrings("AAAAAAAAAA", row0.repeated.?);
    try std.testing.expectEqual(@as(?i64, 0), row0.sequence);

    // Second row
    const row1 = (try reader.next()).?;
    defer if (row1.repeated) |r| allocator.free(r);
    try std.testing.expectEqual(@as(?i64, 1), row1.sequence);
}

test "RowReader interop: compression_gzip.parquet" {
    if (build_options.no_compression) return;
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/compression/compression_gzip.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(CompressionInteropTest, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 10000), reader.rowCount());

    const row0 = (try reader.next()).?;
    defer if (row0.repeated) |r| allocator.free(r);
    try std.testing.expectEqualStrings("AAAAAAAAAA", row0.repeated.?);
    try std.testing.expectEqual(@as(?i64, 0), row0.sequence);
}

test "RowReader interop: compression_snappy.parquet" {
    if (build_options.no_compression) return;
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/compression/compression_snappy.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(CompressionInteropTest, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 10000), reader.rowCount());

    const row0 = (try reader.next()).?;
    defer if (row0.repeated) |r| allocator.free(r);
    try std.testing.expectEqualStrings("AAAAAAAAAA", row0.repeated.?);
    try std.testing.expectEqual(@as(?i64, 0), row0.sequence);
}

test "RowReader interop: compression_lz4.parquet" {
    if (build_options.no_compression) return;
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/compression/compression_lz4.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(CompressionInteropTest, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 10000), reader.rowCount());

    const row0 = (try reader.next()).?;
    defer if (row0.repeated) |r| allocator.free(r);
    try std.testing.expectEqualStrings("AAAAAAAAAA", row0.repeated.?);
    try std.testing.expectEqual(@as(?i64, 0), row0.sequence);
}

test "RowReader interop: compression_brotli.parquet" {
    if (build_options.no_compression) return;
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/compression/compression_brotli.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(CompressionInteropTest, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 10000), reader.rowCount());

    const row0 = (try reader.next()).?;
    defer if (row0.repeated) |r| allocator.free(r);
    try std.testing.expectEqualStrings("AAAAAAAAAA", row0.repeated.?);
    try std.testing.expectEqual(@as(?i64, 0), row0.sequence);
}

// =============================================================================
// NESTED STRUCT TESTS
// =============================================================================

// --- nested/struct_simple.parquet ---
// Schema: point: struct<x: int32, y: int32, name: string>
// Data: [
//   {x: 1, y: 2, name: "A"},
//   {x: 3, y: 4, name: "B"},
//   null (whole struct),
//   {x: 5, y: 6, name: "C"},
//   {x: 7, y: 8, name: null},
// ]

const Point = struct {
    x: ?i32,
    y: ?i32,
    name: ?[]const u8,
};

const StructSimpleTest = struct {
    point: Point,
};

test "RowReader interop: struct_simple.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/nested/struct_simple.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(StructSimpleTest, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 5), reader.rowCount());

    // Row 0: {x: 1, y: 2, name: "A"}
    const row0 = (try reader.next()).?;
    defer if (row0.point.name) |n| allocator.free(n);
    try std.testing.expectEqual(@as(?i32, 1), row0.point.x);
    try std.testing.expectEqual(@as(?i32, 2), row0.point.y);
    try std.testing.expectEqualStrings("A", row0.point.name.?);

    // Row 1: {x: 3, y: 4, name: "B"}
    const row1 = (try reader.next()).?;
    defer if (row1.point.name) |n| allocator.free(n);
    try std.testing.expectEqual(@as(?i32, 3), row1.point.x);
    try std.testing.expectEqual(@as(?i32, 4), row1.point.y);
    try std.testing.expectEqualStrings("B", row1.point.name.?);

    // Row 2: null struct - all fields should be null
    const row2 = (try reader.next()).?;
    defer if (row2.point.name) |n| allocator.free(n);
    try std.testing.expectEqual(@as(?i32, null), row2.point.x);
    try std.testing.expectEqual(@as(?i32, null), row2.point.y);
    try std.testing.expectEqual(@as(?[]const u8, null), row2.point.name);

    // Row 3: {x: 5, y: 6, name: "C"}
    const row3 = (try reader.next()).?;
    defer if (row3.point.name) |n| allocator.free(n);
    try std.testing.expectEqual(@as(?i32, 5), row3.point.x);
    try std.testing.expectEqual(@as(?i32, 6), row3.point.y);
    try std.testing.expectEqualStrings("C", row3.point.name.?);

    // Row 4: {x: 7, y: 8, name: null}
    const row4 = (try reader.next()).?;
    defer if (row4.point.name) |n| allocator.free(n);
    try std.testing.expectEqual(@as(?i32, 7), row4.point.x);
    try std.testing.expectEqual(@as(?i32, 8), row4.point.y);
    try std.testing.expectEqual(@as(?[]const u8, null), row4.point.name);
}

// --- nested/struct_nested.parquet ---
// Schema: nested: struct<inner: struct<a: int32, b: int32>, value: string>
// Data: [
//   {inner: {a: 1, b: 2}, value: "first"},
//   {inner: {a: 3, b: 4}, value: "second"},
//   {inner: null, value: "null_inner"},
//   null (whole struct),
//   {inner: {a: 5, b: 6}, value: null},
// ]

const Inner = struct {
    a: ?i32,
    b: ?i32,
};

const Nested = struct {
    inner: Inner,
    value: ?[]const u8,
};

const StructNestedTest = struct {
    nested: Nested,
};

test "RowReader interop: struct_nested.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/nested/struct_nested.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(StructNestedTest, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 5), reader.rowCount());

    // Row 0: {inner: {a: 1, b: 2}, value: "first"}
    const row0 = (try reader.next()).?;
    defer if (row0.nested.value) |v| allocator.free(v);
    try std.testing.expectEqual(@as(?i32, 1), row0.nested.inner.a);
    try std.testing.expectEqual(@as(?i32, 2), row0.nested.inner.b);
    try std.testing.expectEqualStrings("first", row0.nested.value.?);

    // Row 1: {inner: {a: 3, b: 4}, value: "second"}
    const row1 = (try reader.next()).?;
    defer if (row1.nested.value) |v| allocator.free(v);
    try std.testing.expectEqual(@as(?i32, 3), row1.nested.inner.a);
    try std.testing.expectEqual(@as(?i32, 4), row1.nested.inner.b);
    try std.testing.expectEqualStrings("second", row1.nested.value.?);

    // Row 2: {inner: null, value: "null_inner"}
    const row2 = (try reader.next()).?;
    defer if (row2.nested.value) |v| allocator.free(v);
    try std.testing.expectEqual(@as(?i32, null), row2.nested.inner.a);
    try std.testing.expectEqual(@as(?i32, null), row2.nested.inner.b);
    try std.testing.expectEqualStrings("null_inner", row2.nested.value.?);

    // Row 3: null (whole struct)
    const row3 = (try reader.next()).?;
    defer if (row3.nested.value) |v| allocator.free(v);
    try std.testing.expectEqual(@as(?i32, null), row3.nested.inner.a);
    try std.testing.expectEqual(@as(?i32, null), row3.nested.inner.b);
    try std.testing.expectEqual(@as(?[]const u8, null), row3.nested.value);

    // Row 4: {inner: {a: 5, b: 6}, value: null}
    const row4 = (try reader.next()).?;
    defer if (row4.nested.value) |v| allocator.free(v);
    try std.testing.expectEqual(@as(?i32, 5), row4.nested.inner.a);
    try std.testing.expectEqual(@as(?i32, 6), row4.nested.inner.b);
    try std.testing.expectEqual(@as(?[]const u8, null), row4.nested.value);
}

// --- nested/struct_with_list.parquet ---
// Schema: id: int32, tags: list<string>
// Data: [
//   {id: 1, tags: ["a", "b"]},
//   {id: 2, tags: ["c"]},
//   {id: 3, tags: []},
//   {id: 4, tags: null},
//   {id: 5, tags: ["d", "e", "f"]},
// ]

const StructWithListTest = struct {
    id: ?i32,
    tags: []const []const u8,
};

test "RowReader interop: struct_with_list.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/nested/struct_with_list.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(StructWithListTest, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 5), reader.rowCount());

    // Row 0: {id: 1, tags: ["a", "b"]}
    const row0 = (try reader.next()).?;
    defer {
        for (row0.tags) |t| allocator.free(t);
        allocator.free(row0.tags);
    }
    try std.testing.expectEqual(@as(?i32, 1), row0.id);
    try std.testing.expectEqual(@as(usize, 2), row0.tags.len);
    try std.testing.expectEqualStrings("a", row0.tags[0]);
    try std.testing.expectEqualStrings("b", row0.tags[1]);

    // Row 1: {id: 2, tags: ["c"]}
    const row1 = (try reader.next()).?;
    defer {
        for (row1.tags) |t| allocator.free(t);
        allocator.free(row1.tags);
    }
    try std.testing.expectEqual(@as(?i32, 2), row1.id);
    try std.testing.expectEqual(@as(usize, 1), row1.tags.len);
    try std.testing.expectEqualStrings("c", row1.tags[0]);

    // Row 2: {id: 3, tags: []}
    const row2 = (try reader.next()).?;
    defer allocator.free(row2.tags);
    try std.testing.expectEqual(@as(?i32, 3), row2.id);
    try std.testing.expectEqual(@as(usize, 0), row2.tags.len);

    // Row 3: {id: 4, tags: null} - represented as empty list
    const row3 = (try reader.next()).?;
    defer allocator.free(row3.tags);
    try std.testing.expectEqual(@as(?i32, 4), row3.id);
    try std.testing.expectEqual(@as(usize, 0), row3.tags.len);

    // Row 4: {id: 5, tags: ["d", "e", "f"]}
    const row4 = (try reader.next()).?;
    defer {
        for (row4.tags) |t| allocator.free(t);
        allocator.free(row4.tags);
    }
    try std.testing.expectEqual(@as(?i32, 5), row4.id);
    try std.testing.expectEqual(@as(usize, 3), row4.tags.len);
    try std.testing.expectEqualStrings("d", row4.tags[0]);
    try std.testing.expectEqualStrings("e", row4.tags[1]);
    try std.testing.expectEqualStrings("f", row4.tags[2]);
}

// =============================================================================
// MAP TESTS
// =============================================================================

// --- nested/map_string_int.parquet ---
// Schema: map_col: map<string, int32>
// Parquet columns: map_col.key_value.key, map_col.key_value.value
// Data: [
//   [("a", 1), ("b", 2)],
//   [("c", 3)],
//   [],
//   null,
//   [("d", 4), ("e", 5), ("f", 6)],
// ]
// =============================================================================
// List-of-Struct Tests
// =============================================================================

// --- nested/list_struct.parquet ---
// Schema: points: list<struct<x: int32, y: int32>>
// Data: [
//   [{x: 1, y: 2}, {x: 3, y: 4}],
//   [{x: 5, y: 6}],
//   null,
//   [],
//   [{x: 7, y: 8}, null],
// ]

const PointInList = struct {
    x: ?i32,
    y: ?i32,
};

const ListStructTest = struct {
    points: ?[]const PointInList,
};

test "RowReader interop: list_struct.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/nested/list_struct.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(ListStructTest, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 5), reader.rowCount());

    // Row 0: [{x: 1, y: 2}, {x: 3, y: 4}]
    const row0 = (try reader.next()).?;
    defer if (row0.points) |pts| allocator.free(pts);
    try std.testing.expect(row0.points != null);
    try std.testing.expectEqual(@as(usize, 2), row0.points.?.len);
    try std.testing.expectEqual(@as(?i32, 1), row0.points.?[0].x);
    try std.testing.expectEqual(@as(?i32, 2), row0.points.?[0].y);
    try std.testing.expectEqual(@as(?i32, 3), row0.points.?[1].x);
    try std.testing.expectEqual(@as(?i32, 4), row0.points.?[1].y);

    // Row 1: [{x: 5, y: 6}]
    const row1 = (try reader.next()).?;
    defer if (row1.points) |pts| allocator.free(pts);
    try std.testing.expect(row1.points != null);
    try std.testing.expectEqual(@as(usize, 1), row1.points.?.len);
    try std.testing.expectEqual(@as(?i32, 5), row1.points.?[0].x);
    try std.testing.expectEqual(@as(?i32, 6), row1.points.?[0].y);

    // Row 2: null
    const row2 = (try reader.next()).?;
    defer if (row2.points) |pts| allocator.free(pts);
    try std.testing.expect(row2.points == null);

    // Row 3: []
    const row3 = (try reader.next()).?;
    defer if (row3.points) |pts| allocator.free(pts);
    try std.testing.expect(row3.points != null);
    try std.testing.expectEqual(@as(usize, 0), row3.points.?.len);

    // Row 4: [{x: 7, y: 8}, null] - second element has null fields
    const row4 = (try reader.next()).?;
    defer if (row4.points) |pts| allocator.free(pts);
    try std.testing.expect(row4.points != null);
    try std.testing.expectEqual(@as(usize, 2), row4.points.?.len);
    try std.testing.expectEqual(@as(?i32, 7), row4.points.?[0].x);
    try std.testing.expectEqual(@as(?i32, 8), row4.points.?[0].y);
    // Second element might be null struct fields
    // The exact behavior depends on how PyArrow encodes null elements
}

// =============================================================================
// Map Tests (using list-of-struct)
// =============================================================================

// --- nested/map_string_int.parquet ---
// Schema: map_col: map<string, int32>
// Columns: map_col.key_value.key, map_col.key_value.value
// Data: [
//   [('a', 1), ('b', 2)],
//   [('c', 3)],
//   [],
//   null,
//   [('d', 4), ('e', 5), ('f', 6)],
// ]

const MapEntry = struct {
    key: ?[]const u8,
    value: ?i32,
};

const MapTest = struct {
    map_col: ?[]const MapEntry,
};

test "RowReader interop: map_string_int.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/nested/map_string_int.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(MapTest, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 5), reader.rowCount());

    // Row 0: [('a', 1), ('b', 2)]
    const row0 = (try reader.next()).?;
    defer {
        if (row0.map_col) |entries| {
            for (entries) |entry| {
                if (entry.key) |k| allocator.free(k);
            }
            allocator.free(entries);
        }
    }
    try std.testing.expect(row0.map_col != null);
    try std.testing.expectEqual(@as(usize, 2), row0.map_col.?.len);
    try std.testing.expectEqualStrings("a", row0.map_col.?[0].key.?);
    try std.testing.expectEqual(@as(?i32, 1), row0.map_col.?[0].value);
    try std.testing.expectEqualStrings("b", row0.map_col.?[1].key.?);
    try std.testing.expectEqual(@as(?i32, 2), row0.map_col.?[1].value);

    // Row 1: [('c', 3)]
    const row1 = (try reader.next()).?;
    defer {
        if (row1.map_col) |entries| {
            for (entries) |entry| {
                if (entry.key) |k| allocator.free(k);
            }
            allocator.free(entries);
        }
    }
    try std.testing.expect(row1.map_col != null);
    try std.testing.expectEqual(@as(usize, 1), row1.map_col.?.len);
    try std.testing.expectEqualStrings("c", row1.map_col.?[0].key.?);
    try std.testing.expectEqual(@as(?i32, 3), row1.map_col.?[0].value);

    // Row 2: [] (empty map)
    const row2 = (try reader.next()).?;
    defer {
        if (row2.map_col) |entries| {
            for (entries) |entry| {
                if (entry.key) |k| allocator.free(k);
            }
            allocator.free(entries);
        }
    }
    try std.testing.expect(row2.map_col != null);
    try std.testing.expectEqual(@as(usize, 0), row2.map_col.?.len);

    // Row 3: null
    const row3 = (try reader.next()).?;
    defer {
        if (row3.map_col) |entries| {
            for (entries) |entry| {
                if (entry.key) |k| allocator.free(k);
            }
            allocator.free(entries);
        }
    }
    try std.testing.expect(row3.map_col == null);

    // Row 4: [('d', 4), ('e', 5), ('f', 6)]
    const row4 = (try reader.next()).?;
    defer {
        if (row4.map_col) |entries| {
            for (entries) |entry| {
                if (entry.key) |k| allocator.free(k);
            }
            allocator.free(entries);
        }
    }
    try std.testing.expect(row4.map_col != null);
    try std.testing.expectEqual(@as(usize, 3), row4.map_col.?.len);
    try std.testing.expectEqualStrings("d", row4.map_col.?[0].key.?);
    try std.testing.expectEqual(@as(?i32, 4), row4.map_col.?[0].value);
    try std.testing.expectEqualStrings("e", row4.map_col.?[1].key.?);
    try std.testing.expectEqual(@as(?i32, 5), row4.map_col.?[1].value);
    try std.testing.expectEqualStrings("f", row4.map_col.?[2].key.?);
    try std.testing.expectEqual(@as(?i32, 6), row4.map_col.?[2].value);
}

// =============================================================================
// List<bool> Tests
// =============================================================================

// --- nested/list_bool.parquet ---
// Schema: list_col: list<bool>
// Data: [
//   [true, false, true],
//   [false],
//   null,
//   [],
//   [true, null, false],
// ]

const ListBoolTest = struct {
    list_col: ?[]const ?bool,
};

test "RowReader interop: list_bool.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/nested/list_bool.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(ListBoolTest, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 5), reader.rowCount());

    // Row 0: [true, false, true]
    const row0 = (try reader.next()).?;
    defer if (row0.list_col) |lst| allocator.free(lst);
    try std.testing.expect(row0.list_col != null);
    try std.testing.expectEqual(@as(usize, 3), row0.list_col.?.len);
    try std.testing.expectEqual(@as(?bool, true), row0.list_col.?[0]);
    try std.testing.expectEqual(@as(?bool, false), row0.list_col.?[1]);
    try std.testing.expectEqual(@as(?bool, true), row0.list_col.?[2]);

    // Row 1: [false]
    const row1 = (try reader.next()).?;
    defer if (row1.list_col) |lst| allocator.free(lst);
    try std.testing.expect(row1.list_col != null);
    try std.testing.expectEqual(@as(usize, 1), row1.list_col.?.len);
    try std.testing.expectEqual(@as(?bool, false), row1.list_col.?[0]);

    // Row 2: null
    const row2 = (try reader.next()).?;
    defer if (row2.list_col) |lst| allocator.free(lst);
    try std.testing.expect(row2.list_col == null);

    // Row 3: []
    const row3 = (try reader.next()).?;
    defer if (row3.list_col) |lst| allocator.free(lst);
    try std.testing.expect(row3.list_col != null);
    try std.testing.expectEqual(@as(usize, 0), row3.list_col.?.len);

    // Row 4: [true, null, false]
    const row4 = (try reader.next()).?;
    defer if (row4.list_col) |lst| allocator.free(lst);
    try std.testing.expect(row4.list_col != null);
    try std.testing.expectEqual(@as(usize, 3), row4.list_col.?.len);
    try std.testing.expectEqual(@as(?bool, true), row4.list_col.?[0]);
    try std.testing.expectEqual(@as(?bool, null), row4.list_col.?[1]);
    try std.testing.expectEqual(@as(?bool, false), row4.list_col.?[2]);
}

// =============================================================================
// Nested List Tests (list<list<T>>)
// =============================================================================

// --- nested/list_of_list.parquet ---
// Schema: list_col: list<list<int32>>
// Path: list_col.list.element.list.element
// Data: [
//   [[1, 2], [3]],
//   [[4, 5, 6]],
//   [[], [7]],
//   [],
//   [[8], [9], [10]],
// ]

const NestedListTest = struct {
    list_col: ?[]const []const i32,
};

test "RowReader interop: list_of_list.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/nested/list_of_list.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(NestedListTest, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 5), reader.rowCount());

    // Row 0: [[1, 2], [3]]
    const row0 = (try reader.next()).?;
    defer {
        if (row0.list_col) |outer| {
            for (outer) |inner| allocator.free(inner);
            allocator.free(outer);
        }
    }
    try std.testing.expect(row0.list_col != null);
    try std.testing.expectEqual(@as(usize, 2), row0.list_col.?.len);
    try std.testing.expectEqual(@as(usize, 2), row0.list_col.?[0].len);
    try std.testing.expectEqual(@as(i32, 1), row0.list_col.?[0][0]);
    try std.testing.expectEqual(@as(i32, 2), row0.list_col.?[0][1]);
    try std.testing.expectEqual(@as(usize, 1), row0.list_col.?[1].len);
    try std.testing.expectEqual(@as(i32, 3), row0.list_col.?[1][0]);

    // Row 1: [[4, 5, 6]]
    const row1 = (try reader.next()).?;
    defer {
        if (row1.list_col) |outer| {
            for (outer) |inner| allocator.free(inner);
            allocator.free(outer);
        }
    }
    try std.testing.expect(row1.list_col != null);
    try std.testing.expectEqual(@as(usize, 1), row1.list_col.?.len);
    try std.testing.expectEqual(@as(usize, 3), row1.list_col.?[0].len);

    // Row 2: [[], [7]]
    const row2 = (try reader.next()).?;
    defer {
        if (row2.list_col) |outer| {
            for (outer) |inner| allocator.free(inner);
            allocator.free(outer);
        }
    }
    try std.testing.expect(row2.list_col != null);
    try std.testing.expectEqual(@as(usize, 2), row2.list_col.?.len);
    try std.testing.expectEqual(@as(usize, 0), row2.list_col.?[0].len); // empty inner
    try std.testing.expectEqual(@as(usize, 1), row2.list_col.?[1].len);

    // Row 3: []
    const row3 = (try reader.next()).?;
    defer {
        if (row3.list_col) |outer| {
            for (outer) |inner| allocator.free(inner);
            allocator.free(outer);
        }
    }
    try std.testing.expect(row3.list_col != null);
    try std.testing.expectEqual(@as(usize, 0), row3.list_col.?.len);

    // Row 4: [[8], [9], [10]]
    const row4 = (try reader.next()).?;
    defer {
        if (row4.list_col) |outer| {
            for (outer) |inner| allocator.free(inner);
            allocator.free(outer);
        }
    }
    try std.testing.expect(row4.list_col != null);
    try std.testing.expectEqual(@as(usize, 3), row4.list_col.?.len);
}

// =============================================================================
// Roundtrip Tests - Nested Lists
// =============================================================================

const NestedListRoundtrip = struct {
    values: ?[]const []const i32,
};

test "RowWriter/RowReader roundtrip: nested lists" {
    const allocator = std.testing.allocator;

    // Create test data
    const inner0_0 = [_]i32{ 1, 2, 3 };
    const inner0_1 = [_]i32{ 4, 5 };
    const outer0 = [_][]const i32{ &inner0_0, &inner0_1 };

    const inner1_0 = [_]i32{6};
    const outer1 = [_][]const i32{&inner1_0};

    const outer2 = [_][]const i32{}; // Empty outer list

    const inner3_0 = [_]i32{ 7, 8, 9 };
    const inner3_1 = [_]i32{ 10, 11 };
    const inner3_2 = [_]i32{12};
    const outer3 = [_][]const i32{ &inner3_0, &inner3_1, &inner3_2 };

    const rows = [_]NestedListRoundtrip{
        .{ .values = &outer0 },
        .{ .values = &outer1 },
        .{ .values = &outer2 },
        .{ .values = null }, // Null outer list
        .{ .values = &outer3 },
    };

    // Write to file
    const test_file_path = "test_nested_list_roundtrip.parquet";
    {
        const file = try std.fs.cwd().createFile(test_file_path, .{});
        defer file.close();

        var writer = try parquet.writeToFileRows(NestedListRoundtrip, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRows(&rows);
        try writer.close();
    }

    // Read back and verify
    defer std.fs.cwd().deleteFile(test_file_path) catch {};

    const read_file = try std.fs.cwd().openFile(test_file_path, .{});
    defer read_file.close();

    var reader = try parquet.openFileRowReader(NestedListRoundtrip, allocator, read_file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 5), reader.rowCount());

    // Row 0: [[1, 2, 3], [4, 5]]
    const r0 = (try reader.next()).?;
    defer {
        if (r0.values) |outer| {
            for (outer) |inner| allocator.free(inner);
            allocator.free(outer);
        }
    }
    try std.testing.expect(r0.values != null);
    try std.testing.expectEqual(@as(usize, 2), r0.values.?.len);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 1, 2, 3 }, r0.values.?[0]);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 4, 5 }, r0.values.?[1]);

    // Row 1: [[6]]
    const r1 = (try reader.next()).?;
    defer {
        if (r1.values) |outer| {
            for (outer) |inner| allocator.free(inner);
            allocator.free(outer);
        }
    }
    try std.testing.expect(r1.values != null);
    try std.testing.expectEqual(@as(usize, 1), r1.values.?.len);
    try std.testing.expectEqualSlices(i32, &[_]i32{6}, r1.values.?[0]);

    // Row 2: []
    const r2 = (try reader.next()).?;
    defer {
        if (r2.values) |outer| {
            for (outer) |inner| allocator.free(inner);
            allocator.free(outer);
        }
    }
    try std.testing.expect(r2.values != null);
    try std.testing.expectEqual(@as(usize, 0), r2.values.?.len);

    // Row 3: null
    const r3 = (try reader.next()).?;
    defer {
        if (r3.values) |outer| {
            for (outer) |inner| allocator.free(inner);
            allocator.free(outer);
        }
    }
    try std.testing.expect(r3.values == null);

    // Row 4: [[7, 8, 9], [10, 11], [12]]
    const r4 = (try reader.next()).?;
    defer {
        if (r4.values) |outer| {
            for (outer) |inner| allocator.free(inner);
            allocator.free(outer);
        }
    }
    try std.testing.expect(r4.values != null);
    try std.testing.expectEqual(@as(usize, 3), r4.values.?.len);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 7, 8, 9 }, r4.values.?[0]);
    try std.testing.expectEqualSlices(i32, &[_]i32{ 10, 11 }, r4.values.?[1]);
    try std.testing.expectEqualSlices(i32, &[_]i32{12}, r4.values.?[2]);
}

// =============================================================================
// Test Coverage Gap: list_nullable.parquet
// =============================================================================

const ListNullableTest = struct {
    list_col: ?[]const ?i32,
};

test "RowReader interop: list_nullable.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile(
        "../test-files-arrow/nested/list_nullable.parquet",
        .{},
    ) catch |err| {
        std.debug.print("Skipping test - file not found: {}\n", .{err});
        return;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(ListNullableTest, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 5), reader.rowCount());

    // Row 0: [1, None, 3]
    const row0 = (try reader.next()).?;
    defer if (row0.list_col) |l| allocator.free(l);
    try std.testing.expect(row0.list_col != null);
    try std.testing.expectEqual(@as(usize, 3), row0.list_col.?.len);
    try std.testing.expectEqual(@as(?i32, 1), row0.list_col.?[0]);
    try std.testing.expectEqual(@as(?i32, null), row0.list_col.?[1]);
    try std.testing.expectEqual(@as(?i32, 3), row0.list_col.?[2]);

    // Row 1: None (null list)
    const row1 = (try reader.next()).?;
    defer if (row1.list_col) |l| allocator.free(l);
    try std.testing.expect(row1.list_col == null);

    // Row 2: [4, 5]
    const row2 = (try reader.next()).?;
    defer if (row2.list_col) |l| allocator.free(l);
    try std.testing.expect(row2.list_col != null);
    try std.testing.expectEqual(@as(usize, 2), row2.list_col.?.len);
    try std.testing.expectEqual(@as(?i32, 4), row2.list_col.?[0]);
    try std.testing.expectEqual(@as(?i32, 5), row2.list_col.?[1]);

    // Row 3: [] (empty list)
    const row3 = (try reader.next()).?;
    defer if (row3.list_col) |l| allocator.free(l);
    try std.testing.expect(row3.list_col != null);
    try std.testing.expectEqual(@as(usize, 0), row3.list_col.?.len);

    // Row 4: [None, None]
    const row4 = (try reader.next()).?;
    defer if (row4.list_col) |l| allocator.free(l);
    try std.testing.expect(row4.list_col != null);
    try std.testing.expectEqual(@as(usize, 2), row4.list_col.?.len);
    try std.testing.expectEqual(@as(?i32, null), row4.list_col.?[0]);
    try std.testing.expectEqual(@as(?i32, null), row4.list_col.?[1]);
}

// =============================================================================
// Test Coverage Gap: struct_with_temporal.parquet
// =============================================================================

const StructWithTemporalTest = struct {
    event: []const u8,
    timestamp: types.TimestampMicros,
    date: types.Date,
};

test "RowReader interop: struct_with_temporal.parquet" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile(
        "../test-files-arrow/nested/struct_with_temporal.parquet",
        .{},
    ) catch |err| {
        std.debug.print("Skipping test - file not found: {}\n", .{err});
        return;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(StructWithTemporalTest, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 5), reader.rowCount());

    // Row 0: event='login', timestamp=2024-01-15 10:30:00+00:00, date=2024-01-15
    const row0 = (try reader.next()).?;
    defer allocator.free(row0.event);
    try std.testing.expectEqualStrings("login", row0.event);
    // Just verify we got valid temporal data
    try std.testing.expect(row0.timestamp.value > 0);
    try std.testing.expect(row0.date.days > 0);

    // Row 1: event='logout'
    const row1 = (try reader.next()).?;
    defer allocator.free(row1.event);
    try std.testing.expectEqualStrings("logout", row1.event);
    try std.testing.expect(row1.timestamp.value > 0);
    try std.testing.expect(row1.date.days > 0);
}

// =============================================================================
// Multi-page column chunk tests
// =============================================================================
// These tests verify that columns with multiple data pages are read correctly.
// PyArrow generates files with 64KB page size, resulting in ~65 pages for 500K int64 values.
//
// Note: Our writer currently writes all data in a single page per column.
// True roundtrip multi-page testing requires adding max_page_size option to the writer.
// These tests read PyArrow-generated files to verify multi-page reading works.

const MultiPageStruct = struct {
    id: i64,
    value: i64,
};

test "RowReader interop: multi-page column chunk" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile(
        "../test-files-arrow/multipage/large_column.parquet",
        .{},
    ) catch |err| {
        std.debug.print("Skipping test - file not found: {}\n", .{err});
        return;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(MultiPageStruct, allocator, file, .{});
    defer reader.deinit();

    // File has 500,000 rows
    try std.testing.expectEqual(@as(usize, 500000), reader.rowCount());

    // Verify first row: id=0, value=0
    const row0 = (try reader.next()).?;
    try std.testing.expectEqual(@as(i64, 0), row0.id);
    try std.testing.expectEqual(@as(i64, 0), row0.value);

    // Skip some rows and verify a middle row
    for (1..1000) |_| {
        _ = try reader.next();
    }
    const row1000 = (try reader.next()).?;
    try std.testing.expectEqual(@as(i64, 1000), row1000.id);
    try std.testing.expectEqual(@as(i64, 10000), row1000.value);
}

test "Reader.readColumn: multi-page column chunk" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile(
        "../test-files-arrow/multipage/large_column.parquet",
        .{},
    ) catch |err| {
        std.debug.print("Skipping test - file not found: {}\n", .{err});
        return;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Read the id column (column 0)
    const id_col = try reader.readColumn(0, i64);
    defer allocator.free(id_col);

    // Should have all 500,000 values
    try std.testing.expectEqual(@as(usize, 500000), id_col.len);

    // Verify first value
    try std.testing.expectEqual(@as(i64, 0), id_col[0].value);

    // Verify middle value
    try std.testing.expectEqual(@as(i64, 250000), id_col[250000].value);

    // Verify last value
    try std.testing.expectEqual(@as(i64, 499999), id_col[499999].value);

    // Read the value column (column 1)
    const value_col = try reader.readColumn(1, i64);
    defer allocator.free(value_col);

    // Should have all 500,000 values
    try std.testing.expectEqual(@as(usize, 500000), value_col.len);

    // Verify: value = id * 10
    try std.testing.expectEqual(@as(i64, 0), value_col[0].value);
    try std.testing.expectEqual(@as(i64, 2500000), value_col[250000].value);
    try std.testing.expectEqual(@as(i64, 4999990), value_col[499999].value);
}

// =============================================================================
// Multi-page Writer/Reader Roundtrip Tests
// =============================================================================

test "RowWriter/RowReader: multi-page roundtrip" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("multipage_roundtrip.parquet", .{ .read = true });
    defer file.close();

    // Write 100,000 rows with max_page_size = 64KB
    // Each i64 is 8 bytes, so 64KB / 8 = ~8000 values per page
    // This should create roughly 12-13 pages per column
    const row_count = 100_000;

    {
        var writer = try parquet.writeToFileRows(MultiPageStruct, allocator, file, .{
            .compression = .uncompressed, // Easier to verify page sizes
            .max_page_size = 65536, // 64KB
        });
        defer writer.deinit();

        for (0..row_count) |i| {
            try writer.writeRow(.{
                .id = @intCast(i),
                .value = @intCast(i * 10),
            });
        }
        try writer.close();
    }

    // Read back and verify all values
    try file.seekTo(0);
    {
        var reader = try parquet.openFileRowReader(MultiPageStruct, allocator, file, .{});
        defer reader.deinit();

        try std.testing.expectEqual(@as(usize, row_count), reader.rowCount());

        // Verify first row
        const row0 = (try reader.next()).?;
        try std.testing.expectEqual(@as(i64, 0), row0.id);
        try std.testing.expectEqual(@as(i64, 0), row0.value);

        // Skip to row 1000 and verify
        for (1..1000) |_| {
            _ = try reader.next();
        }
        const row1000 = (try reader.next()).?;
        try std.testing.expectEqual(@as(i64, 1000), row1000.id);
        try std.testing.expectEqual(@as(i64, 10000), row1000.value);

        // Skip to near the end and verify
        for (1001..99999) |_| {
            _ = try reader.next();
        }
        const row_last = (try reader.next()).?;
        try std.testing.expectEqual(@as(i64, 99999), row_last.id);
        try std.testing.expectEqual(@as(i64, 999990), row_last.value);

        // Verify no more rows
        try std.testing.expectEqual(@as(?MultiPageStruct, null), try reader.next());
    }
}

test "RowWriter/RowReader: multi-page with optional fields roundtrip" {
    const allocator = std.testing.allocator;

    const MultiPageOptional = struct {
        id: i32,
        value: ?i64,
    };

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("multipage_optional.parquet", .{ .read = true });
    defer file.close();

    const row_count = 50_000;

    {
        var writer = try parquet.writeToFileRows(MultiPageOptional, allocator, file, .{
            .compression = .uncompressed,
            .max_page_size = 32768, // 32KB - smaller to create more pages
        });
        defer writer.deinit();

        for (0..row_count) |i| {
            try writer.writeRow(.{
                .id = @intCast(i),
                .value = if (i % 3 == 0) null else @as(?i64, @intCast(i * 100)),
            });
        }
        try writer.close();
    }

    // Read back
    try file.seekTo(0);
    {
        var reader = try parquet.openFileRowReader(MultiPageOptional, allocator, file, .{});
        defer reader.deinit();

        try std.testing.expectEqual(@as(usize, row_count), reader.rowCount());

        // Verify first row (null value)
        const row0 = (try reader.next()).?;
        try std.testing.expectEqual(@as(i32, 0), row0.id);
        try std.testing.expectEqual(@as(?i64, null), row0.value);

        // Verify row 1 (non-null)
        const row1 = (try reader.next()).?;
        try std.testing.expectEqual(@as(i32, 1), row1.id);
        try std.testing.expectEqual(@as(?i64, 100), row1.value);

        // Skip to row 3000 (null value since 3000 % 3 == 0)
        for (2..3000) |_| {
            _ = try reader.next();
        }
        const row3000 = (try reader.next()).?;
        try std.testing.expectEqual(@as(i32, 3000), row3000.id);
        try std.testing.expectEqual(@as(?i64, null), row3000.value);
    }
}

// =============================================================================
// Dictionary-encoded Integer Column Tests
// =============================================================================
// These tests verify that dictionary-encoded integer columns are read correctly.
// PyArrow uses dictionary encoding for low-cardinality integer columns.

const DictIntFlat = struct {
    category_id: i64,
    status: i64,
    priority: i64,
};

test "RowReader interop: dictionary-encoded integer flat columns" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile(
        "../test-files-arrow/encodings/dict_integers_flat.parquet",
        .{},
    ) catch |err| {
        std.debug.print("Skipping test - file not found: {}\n", .{err});
        return;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(DictIntFlat, allocator, file, .{});
    defer reader.deinit();

    // File has 10000 rows
    try std.testing.expectEqual(@as(usize, 10000), reader.rowCount());

    // Verify first row: category_id=0, status=0, priority=0
    const row0 = (try reader.next()).?;
    try std.testing.expectEqual(@as(i64, 0), row0.category_id);
    try std.testing.expectEqual(@as(i64, 0), row0.status);
    try std.testing.expectEqual(@as(i64, 0), row0.priority);

    // Verify second row: category_id=1, status=100, priority=1
    const row1 = (try reader.next()).?;
    try std.testing.expectEqual(@as(i64, 1), row1.category_id);
    try std.testing.expectEqual(@as(i64, 100), row1.status);
    try std.testing.expectEqual(@as(i64, 1), row1.priority);

    // Skip to row 100 and verify pattern continues
    for (2..100) |_| {
        _ = try reader.next();
    }
    const row100 = (try reader.next()).?;
    // category_id = 100 % 5 = 0
    try std.testing.expectEqual(@as(i64, 0), row100.category_id);
    // status = (100 % 3) * 100 = 100
    try std.testing.expectEqual(@as(i64, 100), row100.status);
    // priority = 100 % 10 = 0
    try std.testing.expectEqual(@as(i64, 0), row100.priority);
}

test "Reader.readColumn: dictionary-encoded integer column" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile(
        "../test-files-arrow/encodings/dict_integers_flat.parquet",
        .{},
    ) catch |err| {
        std.debug.print("Skipping test - file not found: {}\n", .{err});
        return;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Read category_id column (column 0)
    const cat_col = try reader.readColumn(0, i64);
    defer allocator.free(cat_col);

    try std.testing.expectEqual(@as(usize, 10000), cat_col.len);

    // Verify values follow pattern: i % 5
    try std.testing.expectEqual(@as(i64, 0), cat_col[0].value);
    try std.testing.expectEqual(@as(i64, 1), cat_col[1].value);
    try std.testing.expectEqual(@as(i64, 4), cat_col[4].value);
    try std.testing.expectEqual(@as(i64, 0), cat_col[5].value);
}

const DictIntList = struct {
    list_col: ?[]const i64,
};

test "RowReader interop: dictionary-encoded integer list columns" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile(
        "../test-files-arrow/encodings/dict_integers_list.parquet",
        .{},
    ) catch |err| {
        std.debug.print("Skipping test - file not found: {}\n", .{err});
        return;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(DictIntList, allocator, file, .{});
    defer reader.deinit();

    // File has 10000 rows (pattern repeats every 5: [1,2,3], [1,1,1], [2,3], null, [1,2,3,1,2,3])
    try std.testing.expectEqual(@as(usize, 10000), reader.rowCount());

    // First row should be [1, 2, 3]
    const row0 = (try reader.next()).?;
    defer if (row0.list_col) |list| allocator.free(list);

    try std.testing.expect(row0.list_col != null);
    const list0 = row0.list_col.?;
    try std.testing.expectEqual(@as(usize, 3), list0.len);
    try std.testing.expectEqual(@as(i64, 1), list0[0]);
    try std.testing.expectEqual(@as(i64, 2), list0[1]);
    try std.testing.expectEqual(@as(i64, 3), list0[2]);

    // Second row should be [1, 1, 1]
    const row1 = (try reader.next()).?;
    defer if (row1.list_col) |list| allocator.free(list);

    try std.testing.expect(row1.list_col != null);
    const list1 = row1.list_col.?;
    try std.testing.expectEqual(@as(usize, 3), list1.len);
    try std.testing.expectEqual(@as(i64, 1), list1[0]);

    // Third row should be [2, 3]
    const row2 = (try reader.next()).?;
    defer if (row2.list_col) |list| allocator.free(list);

    try std.testing.expect(row2.list_col != null);
    const list2 = row2.list_col.?;
    try std.testing.expectEqual(@as(usize, 2), list2.len);

    // Fourth row should be null
    const row3 = (try reader.next()).?;
    defer if (row3.list_col) |list| allocator.free(list);
    try std.testing.expectEqual(@as(?[]const i64, null), row3.list_col);
}

test "Reader.readListColumn: dictionary-encoded integer list" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile(
        "../test-files-arrow/encodings/dict_integers_list.parquet",
        .{},
    ) catch |err| {
        std.debug.print("Skipping test - file not found: {}\n", .{err});
        return;
    };
    defer file.close();

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Read list_col column (column 0) - dictionary encoded
    const list_col = try reader.readListColumn(0, i64);
    defer reader.freeListColumn(i64, list_col);

    // File has 10000 rows (pattern repeats every 5 rows: [1,2,3], [1,1,1], [2,3], null, [1,2,3,1,2,3])
    try std.testing.expectEqual(@as(usize, 10000), list_col.len);

    // First row should be [1, 2, 3]
    try std.testing.expect(list_col[0] == .value);
    const row0 = list_col[0].value;
    try std.testing.expectEqual(@as(usize, 3), row0.len);
    try std.testing.expectEqual(@as(i64, 1), row0[0].value);
    try std.testing.expectEqual(@as(i64, 2), row0[1].value);
    try std.testing.expectEqual(@as(i64, 3), row0[2].value);

    // Second row should be [1, 1, 1]
    try std.testing.expect(list_col[1] == .value);
    const row1 = list_col[1].value;
    try std.testing.expectEqual(@as(usize, 3), row1.len);
    try std.testing.expectEqual(@as(i64, 1), row1[0].value);

    // Fourth row (index 3) should be null
    try std.testing.expect(list_col[3] == .null_value);
}

test "RowReader interop: mixed dictionary and plain integer columns" {
    const allocator = std.testing.allocator;

    const MixedStruct = struct {
        id: i64,
        category: i64,
        value: i64,
    };

    const file = std.fs.cwd().openFile(
        "../test-files-arrow/encodings/dict_integers_mixed.parquet",
        .{},
    ) catch |err| {
        std.debug.print("Skipping test - file not found: {}\n", .{err});
        return;
    };
    defer file.close();

    var reader = try parquet.openFileRowReader(MixedStruct, allocator, file, .{});
    defer reader.deinit();

    // File has 5000 rows
    try std.testing.expectEqual(@as(usize, 5000), reader.rowCount());

    // Verify first row
    const row0 = (try reader.next()).?;
    try std.testing.expectEqual(@as(i64, 0), row0.id);
    try std.testing.expectEqual(@as(i64, 0), row0.category);
    try std.testing.expectEqual(@as(i64, 0), row0.value);

    // Verify second row
    const row1 = (try reader.next()).?;
    try std.testing.expectEqual(@as(i64, 1), row1.id);
    try std.testing.expectEqual(@as(i64, 1), row1.category);
    try std.testing.expectEqual(@as(i64, 10), row1.value);

    // Skip to row 1000 and verify
    for (2..1000) |_| {
        _ = try reader.next();
    }
    const row1000 = (try reader.next()).?;
    try std.testing.expectEqual(@as(i64, 1000), row1000.id);
    try std.testing.expectEqual(@as(i64, 1), row1000.category); // 1000 % 3 = 1
    try std.testing.expectEqual(@as(i64, 10000), row1000.value);
}

// =============================================================================
// Dictionary Encoding Roundtrip Tests
// =============================================================================

const DictRoundtripStruct = struct {
    category: i64, // Low cardinality - good for dictionary
    status: i32, // Low cardinality - good for dictionary
};

test "RowWriter/RowReader: dictionary encoding roundtrip" {
    const allocator = std.testing.allocator;

    // Create temp file
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("dict_roundtrip.parquet", .{ .read = true });
    defer file.close();

    // Write with dictionary encoding
    {
        var writer = try parquet.writeToFileRows(DictRoundtripStruct, allocator, file, .{
            .compression = .uncompressed,
            .use_dictionary = true,
        });
        defer writer.deinit();

        // Write 1000 rows with low cardinality values (5 unique categories, 3 unique statuses)
        for (0..1000) |i| {
            try writer.writeRow(.{
                .category = @intCast(i % 5), // 0, 1, 2, 3, 4
                .status = @intCast((i % 3) * 100), // 0, 100, 200
            });
        }
        try writer.close();
    }

    // Read back and verify
    try file.seekTo(0);

    var reader = try parquet.openFileRowReader(DictRoundtripStruct, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 1000), reader.rowCount());

    // Verify first few rows
    const row0 = (try reader.next()).?;
    try std.testing.expectEqual(@as(i64, 0), row0.category);
    try std.testing.expectEqual(@as(i32, 0), row0.status);

    const row1 = (try reader.next()).?;
    try std.testing.expectEqual(@as(i64, 1), row1.category);
    try std.testing.expectEqual(@as(i32, 100), row1.status);

    const row2 = (try reader.next()).?;
    try std.testing.expectEqual(@as(i64, 2), row2.category);
    try std.testing.expectEqual(@as(i32, 200), row2.status);

    // Verify pattern continues
    const row5 = blk: {
        _ = try reader.next(); // row3
        _ = try reader.next(); // row4
        break :blk (try reader.next()).?;
    };
    try std.testing.expectEqual(@as(i64, 0), row5.category); // 5 % 5 = 0
    try std.testing.expectEqual(@as(i32, 200), row5.status); // (5 % 3) * 100 = 2 * 100 = 200
}

const DictNullableStruct = struct {
    value: ?i64,
};

test "RowWriter/RowReader: dictionary encoding with nullable column roundtrip" {
    const allocator = std.testing.allocator;

    // Create temp file
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("dict_nullable_roundtrip.parquet", .{ .read = true });
    defer file.close();

    // Write with dictionary encoding
    {
        var writer = try parquet.writeToFileRows(DictNullableStruct, allocator, file, .{
            .compression = .uncompressed,
            .use_dictionary = true,
        });
        defer writer.deinit();

        // Write 100 rows with some nulls and low cardinality values
        for (0..100) |i| {
            const val: ?i64 = if (i % 5 == 0) null else @intCast(i % 3);
            try writer.writeRow(.{ .value = val });
        }
        try writer.close();
    }

    // Read back and verify
    try file.seekTo(0);

    var reader = try parquet.openFileRowReader(DictNullableStruct, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 100), reader.rowCount());

    // Row 0: null (0 % 5 == 0)
    const row0 = (try reader.next()).?;
    try std.testing.expectEqual(@as(?i64, null), row0.value);

    // Row 1: 1 % 3 = 1
    const row1 = (try reader.next()).?;
    try std.testing.expectEqual(@as(?i64, 1), row1.value);

    // Row 2: 2 % 3 = 2
    const row2 = (try reader.next()).?;
    try std.testing.expectEqual(@as(?i64, 2), row2.value);

    // Row 3: 3 % 3 = 0
    const row3 = (try reader.next()).?;
    try std.testing.expectEqual(@as(?i64, 0), row3.value);

    // Row 4: 4 % 3 = 1
    const row4 = (try reader.next()).?;
    try std.testing.expectEqual(@as(?i64, 1), row4.value);

    // Row 5: null (5 % 5 == 0)
    const row5 = (try reader.next()).?;
    try std.testing.expectEqual(@as(?i64, null), row5.value);
}

const DictListStruct = struct {
    tags: []const i64, // List of integers with low cardinality
};

test "Low-level: dictionary encoding list column write/read" {
    // Low-level test using Reader.readListColumn to verify dictionary list writing
    const allocator = std.testing.allocator;

    // Create temp file
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("dict_list_lowlevel.parquet", .{ .read = true });
    defer file.close();

    // Write with dictionary encoding using RowWriter
    {
        var writer = try parquet.writeToFileRows(DictListStruct, allocator, file, .{
            .compression = .uncompressed,
            .use_dictionary = true,
        });
        defer writer.deinit();

        try writer.writeRow(.{ .tags = &[_]i64{ 1, 2, 3 } });
        try writer.writeRow(.{ .tags = &[_]i64{ 1, 1, 1 } });
        try writer.close();
    }

    // Read back using low-level Reader.readListColumn
    try file.seekTo(0);

    var reader = try parquet.openFile(allocator, file);
    defer reader.deinit();

    // Check that 2 rows are reported
    try std.testing.expectEqual(@as(i64, 2), reader.metadata.num_rows);

    // Read list column (column 0)
    const list_col = try reader.readListColumn(0, i64);
    defer reader.freeListColumn(i64, list_col);

    // Should have 2 rows
    try std.testing.expectEqual(@as(usize, 2), list_col.len);

    // First row should be [1, 2, 3]
    try std.testing.expect(list_col[0] == .value);
    const row0 = list_col[0].value;
    try std.testing.expectEqual(@as(usize, 3), row0.len);
    try std.testing.expectEqual(@as(i64, 1), row0[0].value);
    try std.testing.expectEqual(@as(i64, 2), row0[1].value);
    try std.testing.expectEqual(@as(i64, 3), row0[2].value);

    // Second row should be [1, 1, 1]
    try std.testing.expect(list_col[1] == .value);
    const row1 = list_col[1].value;
    try std.testing.expectEqual(@as(usize, 3), row1.len);
    try std.testing.expectEqual(@as(i64, 1), row1[0].value);
}

test "RowWriter/RowReader: dictionary encoding list column roundtrip" {
    const allocator = std.testing.allocator;

    // Create temp file
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("dict_list_roundtrip.parquet", .{ .read = true });
    defer file.close();

    // Write with dictionary encoding
    {
        var writer = try parquet.writeToFileRows(DictListStruct, allocator, file, .{
            .compression = .uncompressed,
            .use_dictionary = true,
        });
        defer writer.deinit();

        // Write rows with low cardinality list elements (only values 1, 2, 3)
        try writer.writeRow(.{ .tags = &[_]i64{ 1, 2, 3 } });
        try writer.writeRow(.{ .tags = &[_]i64{ 1, 1, 1 } });
        try writer.close();
    }

    // Read back and verify
    try file.seekTo(0);

    var reader = try parquet.openFileRowReader(DictListStruct, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 2), reader.rowCount());

    // Row 0: [1, 2, 3]
    const row0 = (try reader.next()).?;
    defer allocator.free(row0.tags);
    try std.testing.expectEqual(@as(usize, 3), row0.tags.len);
    try std.testing.expectEqual(@as(i64, 1), row0.tags[0]);
    try std.testing.expectEqual(@as(i64, 2), row0.tags[1]);
    try std.testing.expectEqual(@as(i64, 3), row0.tags[2]);

    // Row 1: [1, 1, 1]
    const row1 = (try reader.next()).?;
    defer allocator.free(row1.tags);
    try std.testing.expectEqual(@as(usize, 3), row1.tags.len);
    try std.testing.expectEqual(@as(i64, 1), row1.tags[0]);
    try std.testing.expectEqual(@as(i64, 1), row1.tags[1]);
    try std.testing.expectEqual(@as(i64, 1), row1.tags[2]);
}

test "RowWriter: dictionary size limit fallback to PLAIN" {
    // Test that dictionary encoding falls back to PLAIN when dictionary exceeds size limit
    const allocator = std.testing.allocator;

    // Create temp file
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("dict_fallback.parquet", .{ .read = true });
    defer file.close();

    // Write with very small dictionary limit (8 bytes = 1 i64 value)
    // This should force fallback to PLAIN encoding since we have multiple unique values
    {
        var writer = try parquet.writeToFileRows(DictRoundtripStruct, allocator, file, .{
            .compression = .uncompressed,
            .use_dictionary = true,
            .dictionary_size_limit = 8, // Only 1 i64 can fit - should trigger fallback
        });
        defer writer.deinit();

        // Write rows with multiple unique values - will exceed 8 byte dictionary limit
        for (0..10) |i| {
            try writer.writeRow(.{
                .category = @intCast(i),
                .status = @intCast(i * 100), // All unique values
            });
        }
        try writer.close();
    }

    // Read back and verify data is correct (PLAIN encoding should still work)
    try file.seekTo(0);

    var reader = try parquet.openFileRowReader(DictRoundtripStruct, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 10), reader.rowCount());

    for (0..10) |i| {
        const row = (try reader.next()).?;
        try std.testing.expectEqual(@as(i64, @intCast(i)), row.category);
        try std.testing.expectEqual(@as(i32, @intCast(i * 100)), row.status);
    }
}

test "RowWriter: dictionary enabled by default" {
    // Verify that dictionary encoding is enabled by default
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("dict_default.parquet", .{ .read = true });
    defer file.close();

    // Write with default options (should use dictionary encoding)
    {
        var writer = try parquet.writeToFileRows(DictRoundtripStruct, allocator, file, .{
            .compression = .uncompressed,
            // use_dictionary defaults to true
            // dictionary_size_limit defaults to 1MB
        });
        defer writer.deinit();

        // Write low-cardinality data (good for dictionary)
        for (0..100) |i| {
            try writer.writeRow(.{
                .category = @intCast(i),
                .status = @intCast(i % 3), // Only 3 unique values
            });
        }
        try writer.close();
    }

    // Read back and verify
    try file.seekTo(0);

    var reader = try parquet.openFileRowReader(DictRoundtripStruct, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 100), reader.rowCount());

    for (0..100) |i| {
        const row = (try reader.next()).?;
        try std.testing.expectEqual(@as(i64, @intCast(i)), row.category);
        try std.testing.expectEqual(@as(i32, @intCast(i % 3)), row.status);
    }
}

// =============================================================================
// String Dictionary Encoding Tests
// =============================================================================

const StringDictStruct = struct {
    name: []const u8,
    category: []const u8, // Low cardinality - good for dictionary
};

test "RowWriter/RowReader: string dictionary encoding roundtrip" {
    const allocator = std.testing.allocator;

    // Create temp file
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("string_dict.parquet", .{ .read = true });
    defer file.close();

    // Static names for test (RowWriter stores slice references, not copies)
    const names = [_][]const u8{
        "item_0", "item_1", "item_2", "item_3", "item_4",
        "item_5", "item_6", "item_7", "item_8", "item_9",
    };
    const categories = [_][]const u8{ "A", "B", "C" };

    // Write with dictionary encoding (default)
    {
        var writer = try parquet.writeToFileRows(StringDictStruct, allocator, file, .{
            .compression = .uncompressed,
            // use_dictionary = true by default
        });
        defer writer.deinit();

        // Write rows with low-cardinality strings
        for (0..10) |i| {
            try writer.writeRow(.{
                .name = names[i],
                .category = categories[i % 3],
            });
        }
        try writer.close();
    }

    // Read back and verify
    try file.seekTo(0);

    var reader = try parquet.openFileRowReader(StringDictStruct, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 10), reader.rowCount());

    for (0..10) |i| {
        const row = (try reader.next()).?;
        defer allocator.free(row.name);
        defer allocator.free(row.category);

        try std.testing.expectEqualStrings(names[i], row.name);
        try std.testing.expectEqualStrings(categories[i % 3], row.category);
    }
}

const NullableStringDictStruct = struct {
    name: []const u8,
    tag: ?[]const u8, // Nullable string with low cardinality
};

test "RowWriter/RowReader: nullable string dictionary encoding roundtrip" {
    const allocator = std.testing.allocator;

    // Create temp file
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("nullable_string_dict.parquet", .{ .read = true });
    defer file.close();

    // Static names (RowWriter stores slice references, not copies)
    const names = [_][]const u8{ "row_0", "row_1", "row_2", "row_3", "row_4" };
    const tags = [_]?[]const u8{ "alpha", null, "beta", "alpha", null };

    // Write with dictionary encoding
    {
        var writer = try parquet.writeToFileRows(NullableStringDictStruct, allocator, file, .{
            .compression = .uncompressed,
        });
        defer writer.deinit();

        for (0..5) |i| {
            try writer.writeRow(.{
                .name = names[i],
                .tag = tags[i],
            });
        }
        try writer.close();
    }

    // Read back and verify
    try file.seekTo(0);

    var reader = try parquet.openFileRowReader(NullableStringDictStruct, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 5), reader.rowCount());

    for (0..5) |i| {
        const row = (try reader.next()).?;
        defer allocator.free(row.name);
        defer if (row.tag) |t| allocator.free(t);

        try std.testing.expectEqualStrings(names[i], row.name);

        if (tags[i]) |expected_tag| {
            try std.testing.expect(row.tag != null);
            try std.testing.expectEqualStrings(expected_tag, row.tag.?);
        } else {
            try std.testing.expect(row.tag == null);
        }
    }
}

test "RowWriter: string dictionary size limit fallback" {
    // Test that string dictionary encoding falls back to PLAIN when dictionary exceeds size limit
    const allocator = std.testing.allocator;

    // Create temp file
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("string_dict_fallback.parquet", .{ .read = true });
    defer file.close();

    // Static unique names (RowWriter stores slice references, not copies)
    const names = [_][]const u8{
        "unique_name_0", "unique_name_1", "unique_name_2", "unique_name_3", "unique_name_4",
        "unique_name_5", "unique_name_6", "unique_name_7", "unique_name_8", "unique_name_9",
    };

    // Write with very small dictionary limit
    // Each string is 4 bytes prefix + string length, so limit of 16 bytes allows ~1 small string
    {
        var writer = try parquet.writeToFileRows(StringDictStruct, allocator, file, .{
            .compression = .uncompressed,
            .use_dictionary = true,
            .dictionary_size_limit = 16, // Very small - should trigger fallback for unique strings
        });
        defer writer.deinit();

        // Write rows with many unique values - will exceed 16 byte dictionary limit
        for (0..10) |i| {
            try writer.writeRow(.{
                .name = names[i],
                .category = names[i], // All unique - will exceed dictionary limit
            });
        }
        try writer.close();
    }

    // Read back and verify data is correct (PLAIN encoding should still work)
    try file.seekTo(0);

    var reader = try parquet.openFileRowReader(StringDictStruct, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 10), reader.rowCount());

    for (0..10) |i| {
        const row = (try reader.next()).?;
        defer allocator.free(row.name);
        defer allocator.free(row.category);

        try std.testing.expectEqualStrings(names[i], row.name);
        try std.testing.expectEqualStrings(names[i], row.category);
    }
}

// =============================================================================
// use_dictionary = false Tests
// =============================================================================

test "RowWriter/RowReader: integer columns with use_dictionary=false" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("int_no_dict.parquet", .{ .read = true });
    defer file.close();

    // Write with dictionary disabled
    {
        var writer = try parquet.writeToFileRows(DictRoundtripStruct, allocator, file, .{
            .compression = .uncompressed,
            .use_dictionary = false,
        });
        defer writer.deinit();

        for (0..20) |i| {
            try writer.writeRow(.{
                .category = @intCast(i),
                .status = @intCast(i % 5),
            });
        }
        try writer.close();
    }

    // Read back and verify (PLAIN encoding should work)
    try file.seekTo(0);

    var reader = try parquet.openFileRowReader(DictRoundtripStruct, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 20), reader.rowCount());

    for (0..20) |i| {
        const row = (try reader.next()).?;
        try std.testing.expectEqual(@as(i64, @intCast(i)), row.category);
        try std.testing.expectEqual(@as(i32, @intCast(i % 5)), row.status);
    }
}

test "RowWriter/RowReader: string columns with use_dictionary=false" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("string_no_dict.parquet", .{ .read = true });
    defer file.close();

    const names = [_][]const u8{
        "alpha", "beta", "gamma", "delta", "epsilon",
    };
    const categories = [_][]const u8{ "X", "Y", "Z" };

    // Write with dictionary disabled
    {
        var writer = try parquet.writeToFileRows(StringDictStruct, allocator, file, .{
            .compression = .uncompressed,
            .use_dictionary = false,
        });
        defer writer.deinit();

        for (0..5) |i| {
            try writer.writeRow(.{
                .name = names[i],
                .category = categories[i % 3],
            });
        }
        try writer.close();
    }

    // Read back and verify (PLAIN encoding should work)
    try file.seekTo(0);

    var reader = try parquet.openFileRowReader(StringDictStruct, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 5), reader.rowCount());

    for (0..5) |i| {
        const row = (try reader.next()).?;
        defer allocator.free(row.name);
        defer allocator.free(row.category);

        try std.testing.expectEqualStrings(names[i], row.name);
        try std.testing.expectEqualStrings(categories[i % 3], row.category);
    }
}

test "RowWriter/RowReader: nullable string with use_dictionary=false" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("nullable_string_no_dict.parquet", .{ .read = true });
    defer file.close();

    const names = [_][]const u8{ "one", "two", "three" };
    const tags = [_]?[]const u8{ "tag_a", null, "tag_b" };

    // Write with dictionary disabled
    {
        var writer = try parquet.writeToFileRows(NullableStringDictStruct, allocator, file, .{
            .compression = .uncompressed,
            .use_dictionary = false,
        });
        defer writer.deinit();

        for (0..3) |i| {
            try writer.writeRow(.{
                .name = names[i],
                .tag = tags[i],
            });
        }
        try writer.close();
    }

    // Read back and verify (PLAIN encoding should work)
    try file.seekTo(0);

    var reader = try parquet.openFileRowReader(NullableStringDictStruct, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 3), reader.rowCount());

    for (0..3) |i| {
        const row = (try reader.next()).?;
        defer allocator.free(row.name);
        defer if (row.tag) |t| allocator.free(t);

        try std.testing.expectEqualStrings(names[i], row.name);

        if (tags[i]) |expected_tag| {
            try std.testing.expect(row.tag != null);
            try std.testing.expectEqualStrings(expected_tag, row.tag.?);
        } else {
            try std.testing.expect(row.tag == null);
        }
    }
}

// =============================================================================
// Multi-page Dictionary Encoding Tests
// =============================================================================

test "RowWriter/RowReader: multi-page dictionary encoding for integers" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("multipage_dict_int.parquet", .{ .read = true });
    defer file.close();

    // Write with dictionary enabled and small max_page_size to force multiple pages
    // Using 100 rows with small page size should create multiple data pages
    {
        var writer = try parquet.writeToFileRows(DictRoundtripStruct, allocator, file, .{
            .compression = .uncompressed,
            .use_dictionary = true,
            .max_page_size = 50, // Very small page size to force multiple pages
        });
        defer writer.deinit();

        for (0..100) |i| {
            try writer.writeRow(.{
                .category = @intCast(@mod(i, 5)), // Only 5 unique values
                .status = @intCast((i % 3) * 100),
            });
        }
        try writer.close();
    }

    // Read back and verify
    try file.seekTo(0);

    var reader = try parquet.openFileRowReader(DictRoundtripStruct, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 100), reader.rowCount());

    for (0..100) |i| {
        const row = (try reader.next()).?;
        try std.testing.expectEqual(@as(i64, @intCast(@mod(i, 5))), row.category);
        try std.testing.expectEqual(@as(i32, @intCast((i % 3) * 100)), row.status);
    }
}

test "RowWriter/RowReader: multi-page dictionary encoding for strings" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("multipage_dict_string.parquet", .{ .read = true });
    defer file.close();

    const categories = [_][]const u8{ "alpha", "beta", "gamma", "delta", "epsilon" };

    // Write with dictionary enabled and small max_page_size
    {
        var writer = try parquet.writeToFileRows(StringDictStruct, allocator, file, .{
            .compression = .uncompressed,
            .use_dictionary = true,
            .max_page_size = 50, // Very small page size to force multiple pages
        });
        defer writer.deinit();

        for (0..100) |i| {
            try writer.writeRow(.{
                .name = categories[i % 5],
                .category = categories[(i + 1) % 5],
            });
        }
        try writer.close();
    }

    // Read back and verify
    try file.seekTo(0);

    var reader = try parquet.openFileRowReader(StringDictStruct, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 100), reader.rowCount());

    for (0..100) |i| {
        const row = (try reader.next()).?;
        defer allocator.free(row.name);
        defer allocator.free(row.category);

        try std.testing.expectEqualStrings(categories[i % 5], row.name);
        try std.testing.expectEqualStrings(categories[(i + 1) % 5], row.category);
    }
}

test "RowWriter/RowReader: multi-page dictionary encoding for nullable integers" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const file = try tmp_dir.dir.createFile("multipage_dict_nullable_int.parquet", .{ .read = true });
    defer file.close();

    // Write with dictionary enabled and small max_page_size
    {
        var writer = try parquet.writeToFileRows(DictNullableStruct, allocator, file, .{
            .compression = .uncompressed,
            .use_dictionary = true,
            .max_page_size = 50, // Very small page size
        });
        defer writer.deinit();

        for (0..100) |i| {
            const val: ?i64 = if (i % 3 == 0) null else @intCast(@mod(i, 5));
            try writer.writeRow(.{ .value = val });
        }
        try writer.close();
    }

    // Read back and verify
    try file.seekTo(0);

    var reader = try parquet.openFileRowReader(DictNullableStruct, allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 100), reader.rowCount());

    for (0..100) |i| {
        const row = (try reader.next()).?;

        if (i % 3 == 0) {
            try std.testing.expectEqual(@as(?i64, null), row.value);
        } else {
            try std.testing.expectEqual(@as(?i64, @intCast(@mod(i, 5))), row.value);
        }
    }
}

// =============================================================================
// List of structs with optional string fields (freeRow deep-free coverage)
// =============================================================================

const Location = struct {
    lat: f64,
    lon: f64,
    city: ?[]const u8,
};

const EventWithLocations = struct {
    id: i32,
    history: []const Location,
};

test "RowWriter/RowReader roundtrip: list of structs with optional string fields" {
    const allocator = std.testing.allocator;

    const test_file_path = "test_list_struct_optional_string.parquet";

    {
        const file = try std.fs.cwd().createFile(test_file_path, .{});
        defer file.close();

        var writer = try parquet.writeToFileRows(EventWithLocations, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{
            .id = 1,
            .history = &[_]Location{
                .{ .lat = 37.7, .lon = -122.4, .city = "San Francisco" },
                .{ .lat = 34.0, .lon = -118.2, .city = null },
            },
        });
        try writer.writeRow(.{
            .id = 2,
            .history = &[_]Location{
                .{ .lat = 40.7, .lon = -74.0, .city = "New York" },
            },
        });
        try writer.writeRow(.{
            .id = 3,
            .history = &[_]Location{},
        });
        try writer.close();
    }

    defer std.fs.cwd().deleteFile(test_file_path) catch {};

    const read_file = try std.fs.cwd().openFile(test_file_path, .{});
    defer read_file.close();

    var reader = try parquet.openFileRowReader(EventWithLocations, allocator, read_file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 3), reader.rowCount());

    // Row 0: two locations, one with city, one without
    const r0 = (try reader.next()).?;
    defer reader.freeRow(&r0);
    try std.testing.expectEqual(@as(usize, 2), r0.history.len);
    try std.testing.expectEqualStrings("San Francisco", r0.history[0].city.?);
    try std.testing.expect(r0.history[1].city == null);

    // Row 1: one location with city
    const r1 = (try reader.next()).?;
    defer reader.freeRow(&r1);
    try std.testing.expectEqual(@as(usize, 1), r1.history.len);
    try std.testing.expectEqualStrings("New York", r1.history[0].city.?);

    // Row 2: empty list
    const r2 = (try reader.next()).?;
    defer reader.freeRow(&r2);
    try std.testing.expectEqual(@as(usize, 0), r2.history.len);
}

// =============================================================================
// Optional list of optional strings (freeRow deep-free coverage)
// =============================================================================

const TaggedRecord = struct {
    id: i32,
    tags: ?[]const ?[]const u8,
};

test "RowWriter/RowReader roundtrip: optional list of optional strings" {
    const allocator = std.testing.allocator;

    const test_file_path = "test_optional_list_optional_strings.parquet";

    {
        const file = try std.fs.cwd().createFile(test_file_path, .{});
        defer file.close();

        var writer = try parquet.writeToFileRows(TaggedRecord, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{
            .id = 1,
            .tags = &[_]?[]const u8{ "alpha", "beta" },
        });
        try writer.writeRow(.{
            .id = 2,
            .tags = null,
        });
        try writer.writeRow(.{
            .id = 3,
            .tags = &[_]?[]const u8{ "gamma", null, "delta" },
        });
        try writer.writeRow(.{
            .id = 4,
            .tags = &[_]?[]const u8{},
        });
        try writer.close();
    }

    defer std.fs.cwd().deleteFile(test_file_path) catch {};

    const read_file = try std.fs.cwd().openFile(test_file_path, .{});
    defer read_file.close();

    var reader = try parquet.openFileRowReader(TaggedRecord, allocator, read_file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 4), reader.rowCount());

    // Row 0: ["alpha", "beta"]
    const r0 = (try reader.next()).?;
    defer reader.freeRow(&r0);
    try std.testing.expect(r0.tags != null);
    try std.testing.expectEqual(@as(usize, 2), r0.tags.?.len);
    try std.testing.expectEqualStrings("alpha", r0.tags.?[0].?);
    try std.testing.expectEqualStrings("beta", r0.tags.?[1].?);

    // Row 1: null list
    const r1 = (try reader.next()).?;
    defer reader.freeRow(&r1);
    try std.testing.expect(r1.tags == null);

    // Row 2: ["gamma", null, "delta"]
    const r2 = (try reader.next()).?;
    defer reader.freeRow(&r2);
    try std.testing.expect(r2.tags != null);
    try std.testing.expectEqual(@as(usize, 3), r2.tags.?.len);
    try std.testing.expectEqualStrings("gamma", r2.tags.?[0].?);
    try std.testing.expect(r2.tags.?[1] == null);
    try std.testing.expectEqualStrings("delta", r2.tags.?[2].?);

    // Row 3: empty list
    const r3 = (try reader.next()).?;
    defer reader.freeRow(&r3);
    try std.testing.expect(r3.tags != null);
    try std.testing.expectEqual(@as(usize, 0), r3.tags.?.len);
}

// =============================================================================
// Required list of strings with freeRow (freeRow deep-free coverage)
// =============================================================================

const StringListRecord = struct {
    id: i32,
    names: []const []const u8,
};

test "RowWriter/RowReader roundtrip: list of strings with freeRow" {
    const allocator = std.testing.allocator;

    const test_file_path = "test_list_strings_freerow.parquet";

    {
        const file = try std.fs.cwd().createFile(test_file_path, .{});
        defer file.close();

        var writer = try parquet.writeToFileRows(StringListRecord, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{
            .id = 1,
            .names = &[_][]const u8{ "Alice", "Bob" },
        });
        try writer.writeRow(.{
            .id = 2,
            .names = &[_][]const u8{"Charlie"},
        });
        try writer.writeRow(.{
            .id = 3,
            .names = &[_][]const u8{},
        });
        try writer.close();
    }

    defer std.fs.cwd().deleteFile(test_file_path) catch {};

    const read_file = try std.fs.cwd().openFile(test_file_path, .{});
    defer read_file.close();

    var reader = try parquet.openFileRowReader(StringListRecord, allocator, read_file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(usize, 3), reader.rowCount());

    // Row 0
    const r0 = (try reader.next()).?;
    defer reader.freeRow(&r0);
    try std.testing.expectEqual(@as(usize, 2), r0.names.len);
    try std.testing.expectEqualStrings("Alice", r0.names[0]);
    try std.testing.expectEqualStrings("Bob", r0.names[1]);

    // Row 1
    const r1 = (try reader.next()).?;
    defer reader.freeRow(&r1);
    try std.testing.expectEqual(@as(usize, 1), r1.names.len);
    try std.testing.expectEqualStrings("Charlie", r1.names[0]);

    // Row 2: empty list
    const r2 = (try reader.next()).?;
    defer reader.freeRow(&r2);
    try std.testing.expectEqual(@as(usize, 0), r2.names.len);
}
