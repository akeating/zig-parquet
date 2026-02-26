//! WASM build verification — exercises the full parquet API surface
//! to ensure accurate binary size measurement (no dead-code elimination).
//!
//! Uses buffer-based I/O throughout (no filesystem), which is the
//! actual path a browser WASM build would use.

const std = @import("std");
const parquet = @import("parquet");

const Optional = parquet.Optional;
const ColumnDef = parquet.ColumnDef;


fn testLowLevelWriter(allocator: std.mem.Allocator) ![]u8 {
    const columns = [_]ColumnDef{
        .{ .name = "bool_col", .type_ = .boolean, .optional = true },
        .{ .name = "i32_col", .type_ = .int32, .optional = false, .codec = .zstd },
        .{ .name = "i64_col", .type_ = .int64, .optional = true, .codec = .gzip },
        .{ .name = "f32_col", .type_ = .float, .optional = false, .codec = .snappy },
        .{ .name = "f64_col", .type_ = .double, .optional = true, .codec = .lz4_raw },
        .{ .name = "str_col", .type_ = .byte_array, .optional = true, .codec = .brotli },
        ColumnDef.date("date_col", false),
        ColumnDef.timestamp("ts_col", .micros, true, false),
        ColumnDef.time("time_col", .micros, true, false),
        ColumnDef.json("json_col", true),
        ColumnDef.enum_("enum_col", false),
    };
    var writer = try parquet.writeToBuffer(allocator, &columns);
    defer writer.deinit();

    try writer.writeColumnOptional(bool, 0, &.{
        Optional(bool).from(true),
        Optional(bool).from(false),
        .null_value,
    });
    try writer.writeColumnOptional(i32, 1, &.{
        Optional(i32).from(1),
        Optional(i32).from(2),
        Optional(i32).from(3),
    });
    try writer.writeColumnOptional(i64, 2, &.{
        Optional(i64).from(@as(i64, 100)),
        .null_value,
        Optional(i64).from(@as(i64, 300)),
    });
    try writer.writeColumnOptional(f32, 3, &.{
        Optional(f32).from(@as(f32, 1.5)),
        Optional(f32).from(@as(f32, 2.5)),
        Optional(f32).from(@as(f32, 3.5)),
    });
    try writer.writeColumnOptional(f64, 4, &.{
        Optional(f64).from(@as(f64, 10.1)),
        .null_value,
        Optional(f64).from(@as(f64, 30.3)),
    });
    try writer.writeColumnOptional([]const u8, 5, &.{
        Optional([]const u8).from("hello"),
        Optional([]const u8).from("world"),
        .null_value,
    });
    // Date
    try writer.writeColumnOptional(i32, 6, &.{
        Optional(i32).from(@as(i32, 19000)),
        Optional(i32).from(@as(i32, 19001)),
        Optional(i32).from(@as(i32, 19002)),
    });
    // Timestamp
    try writer.writeColumnOptional(i64, 7, &.{
        Optional(i64).from(@as(i64, 1704067200000000)),
        Optional(i64).from(@as(i64, 1704153600000000)),
        Optional(i64).from(@as(i64, 1704240000000000)),
    });
    // Time
    try writer.writeColumnOptional(i64, 8, &.{
        Optional(i64).from(@as(i64, 3600000000)),
        Optional(i64).from(@as(i64, 7200000000)),
        Optional(i64).from(@as(i64, 10800000000)),
    });
    // JSON
    try writer.writeColumnOptional([]const u8, 9, &.{
        Optional([]const u8).from("{\"a\":1}"),
        Optional([]const u8).from("{\"b\":2}"),
        Optional([]const u8).from("{\"c\":3}"),
    });
    // Enum
    try writer.writeColumnOptional([]const u8, 10, &.{
        Optional([]const u8).from("RED"),
        Optional([]const u8).from("GREEN"),
        Optional([]const u8).from("BLUE"),
    });

    try writer.close();
    return try writer.toOwnedSlice();
}

fn testLowLevelReader(allocator: std.mem.Allocator, data: []const u8) !void {
    var reader = try parquet.openBuffer(allocator, data);
    defer reader.deinit();

    assert(reader.metadata.num_rows == 3);
    const schema = reader.getSchema();
    assert(schema.len > 0);
}

const SensorReading = struct {
    sensor_id: i32,
    timestamp: parquet.Timestamp(.micros),
    date: parquet.Date,
    time: parquet.Time(.micros),
    temperature: f64,
    humidity: ?f32,
    label: ?[]const u8,
    active: bool,
    uuid: parquet.Uuid,
    small_int: i8,
    medium_int: i16,
    unsigned: u32,
    big_unsigned: u64,
};

fn testRowWriterBasic(allocator: std.mem.Allocator) ![]u8 {
    var writer = try parquet.writeToBufferRows(SensorReading, allocator, .{
        .compression = .zstd,
    });
    defer writer.deinit();

    try writer.writeRow(.{
        .sensor_id = 1,
        .timestamp = parquet.Timestamp(.micros).from(1704067200000000),
        .date = parquet.Date.fromDays(19000),
        .time = parquet.Time(.micros).from(3600000000),
        .temperature = 23.5,
        .humidity = 65.0,
        .label = "Building A",
        .active = true,
        .uuid = .{ .bytes = .{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 } },
        .small_int = -5,
        .medium_int = 1000,
        .unsigned = 42,
        .big_unsigned = 9999999,
    });
    try writer.writeRow(.{
        .sensor_id = 2,
        .timestamp = parquet.Timestamp(.micros).from(1704153600000000),
        .date = parquet.Date.fromDays(19001),
        .time = parquet.Time(.micros).from(7200000000),
        .temperature = 18.2,
        .humidity = null,
        .label = null,
        .active = false,
        .uuid = .{ .bytes = .{ 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32 } },
        .small_int = 127,
        .medium_int = -32000,
        .unsigned = 0,
        .big_unsigned = 0,
    });

    try writer.close();
    return try writer.toOwnedSlice();
}

fn testRowReaderBasic(allocator: std.mem.Allocator, data: []const u8) !void {
    var reader = try parquet.openBufferRowReader(SensorReading, allocator, data, .{});
    defer reader.deinit();

    var count: usize = 0;
    while (try reader.next()) |row| {
        reader.freeRow(&row);
        count += 1;
    }
    assert(count == 2);
}

const NestedRecord = struct {
    id: i32,
    tags: []const []const u8,
    scores: []const f64,
    int_lists: ?[]const i32,
    address: Address,
};

const Address = struct {
    street: []const u8,
    city: []const u8,
    zip: i32,
};

fn testNestedTypes(allocator: std.mem.Allocator) ![]u8 {
    var writer = try parquet.writeToBufferRows(NestedRecord, allocator, .{
        .compression = .snappy,
    });
    defer writer.deinit();

    try writer.writeRow(.{
        .id = 1,
        .tags = &.{ "sensor", "outdoor" },
        .scores = &.{ 9.5, 8.3, 7.1 },
        .int_lists = &.{ 10, 20, 30 },
        .address = .{ .street = "123 Main St", .city = "Springfield", .zip = 62701 },
    });
    try writer.writeRow(.{
        .id = 2,
        .tags = &.{"indoor"},
        .scores = &.{},
        .int_lists = null,
        .address = .{ .street = "456 Oak Ave", .city = "Shelbyville", .zip = 62702 },
    });

    try writer.close();
    return try writer.toOwnedSlice();
}

fn testNestedReader(allocator: std.mem.Allocator, data: []const u8) !void {
    var reader = try parquet.openBufferRowReader(NestedRecord, allocator, data, .{});
    defer reader.deinit();

    var count: usize = 0;
    while (try reader.next()) |row| {
        reader.freeRow(&row);
        count += 1;
    }
    assert(count == 2);
}

const MapRecord = struct {
    id: i32,
    labels: ?[]const parquet.MapEntry([]const u8, []const u8),
    counts: ?[]const parquet.MapEntry([]const u8, i32),
};

fn testMapTypes(allocator: std.mem.Allocator) ![]u8 {
    var writer = try parquet.writeToBufferRows(MapRecord, allocator, .{});
    defer writer.deinit();

    try writer.writeRow(.{
        .id = 1,
        .labels = &.{
            .{ .key = "color", .value = "red" },
            .{ .key = "size", .value = "large" },
        },
        .counts = &.{
            .{ .key = "apples", .value = 5 },
        },
    });
    try writer.writeRow(.{
        .id = 2,
        .labels = null,
        .counts = &.{},
    });

    try writer.close();
    return try writer.toOwnedSlice();
}

fn testMapReader(allocator: std.mem.Allocator, data: []const u8) !void {
    var reader = try parquet.openBufferRowReader(MapRecord, allocator, data, .{});
    defer reader.deinit();

    var count: usize = 0;
    while (try reader.next()) |row| {
        reader.freeRow(&row);
        count += 1;
    }
    assert(count == 2);
}

fn testDynamicReader(allocator: std.mem.Allocator, data: []const u8) !void {
    var reader = parquet.openBufferDynamic(allocator, data, .{}) catch |err| {
        std.debug.print("DynamicReader init error: {}\n", .{err});
        return err;
    };
    defer reader.deinit();

    const rows = reader.readAllRows(0) catch |err| {
        std.debug.print("DynamicReader readAllRows error: {}\n", .{err});
        return err;
    };
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }
    assert(rows.len > 0);
}

const DecimalRecord = struct {
    id: i32,
    price: parquet.Decimal(9, 2),
    big_price: parquet.Decimal(18, 4),
    huge_price: parquet.Decimal(38, 10),
};

fn testDecimalTypes(allocator: std.mem.Allocator) ![]u8 {
    var writer = try parquet.writeToBufferRows(DecimalRecord, allocator, .{});
    defer writer.deinit();

    try writer.writeRow(.{
        .id = 1,
        .price = parquet.Decimal(9, 2).fromUnscaled(12345),
        .big_price = parquet.Decimal(18, 4).fromUnscaled(9999999),
        .huge_price = parquet.Decimal(38, 10).fromUnscaled(1234567890),
    });

    try writer.close();
    return try writer.toOwnedSlice();
}

const IntervalRecord = struct {
    id: i32,
    duration: parquet.Interval,
    opt_duration: ?parquet.Interval,
};

fn testIntervalTypes(allocator: std.mem.Allocator) ![]u8 {
    var writer = try parquet.writeToBufferRows(IntervalRecord, allocator, .{});
    defer writer.deinit();

    try writer.writeRow(.{
        .id = 1,
        .duration = parquet.Interval.fromMonths(12),
        .opt_duration = parquet.Interval.fromDays(30),
    });
    try writer.writeRow(.{
        .id = 2,
        .duration = parquet.Interval.fromMillis(86400000),
        .opt_duration = null,
    });

    try writer.close();
    return try writer.toOwnedSlice();
}

const EncodingRecord = struct {
    delta_int: i64,
    delta_str: []const u8,
    bss_float: f64,
};

fn testEncodings(allocator: std.mem.Allocator) ![]u8 {
    var writer = try parquet.writeToBufferRows(EncodingRecord, allocator, .{
        .int_encoding = .delta_binary_packed,
        .float_encoding = .byte_stream_split,
    });
    defer writer.deinit();

    var i: i64 = 0;
    while (i < 100) : (i += 1) {
        try writer.writeRow(.{
            .delta_int = i * 1000,
            .delta_str = "test_string",
            .bss_float = @as(f64, @floatFromInt(i)) * 1.1,
        });
    }

    try writer.close();
    return try writer.toOwnedSlice();
}

const TimePrecisionRecord = struct {
    ts_micros: parquet.Timestamp(.micros),
    ts_millis: parquet.Timestamp(.millis),
    ts_nanos: parquet.Timestamp(.nanos),
    t_micros: parquet.Time(.micros),
    t_millis: parquet.Time(.millis),
    t_nanos: parquet.Time(.nanos),
};

fn testTimePrecisions(allocator: std.mem.Allocator) ![]u8 {
    var writer = try parquet.writeToBufferRows(TimePrecisionRecord, allocator, .{});
    defer writer.deinit();

    try writer.writeRow(.{
        .ts_micros = parquet.Timestamp(.micros).from(1704067200000000),
        .ts_millis = parquet.Timestamp(.millis).from(1704067200000),
        .ts_nanos = parquet.Timestamp(.nanos).from(1704067200000000000),
        .t_micros = parquet.Time(.micros).from(3600000000),
        .t_millis = parquet.Time(.millis).from(3600000),
        .t_nanos = parquet.Time(.nanos).from(3600000000000),
    });

    try writer.close();
    return try writer.toOwnedSlice();
}

const NestedListRecord = struct {
    id: i32,
    matrix: []const []const i32,
};

fn testNestedLists(allocator: std.mem.Allocator) ![]u8 {
    var writer = try parquet.writeToBufferRows(NestedListRecord, allocator, .{});
    defer writer.deinit();

    try writer.writeRow(.{
        .id = 1,
        .matrix = &.{
            &.{ 1, 2, 3 },
            &.{ 4, 5 },
        },
    });

    try writer.close();
    return try writer.toOwnedSlice();
}

const ListOfStructRecord = struct {
    id: i32,
    points: []const Point,
};

const Point = struct {
    x: f64,
    y: f64,
    label: ?[]const u8,
};

fn testListOfStruct(allocator: std.mem.Allocator) ![]u8 {
    var writer = try parquet.writeToBufferRows(ListOfStructRecord, allocator, .{});
    defer writer.deinit();

    try writer.writeRow(.{
        .id = 1,
        .points = &.{
            .{ .x = 1.0, .y = 2.0, .label = "origin" },
            .{ .x = 3.0, .y = 4.0, .label = null },
        },
    });

    try writer.close();
    return try writer.toOwnedSlice();
}

fn testMetadata(allocator: std.mem.Allocator) ![]u8 {
    var writer = try parquet.writeToBufferRows(struct { value: i32 }, allocator, .{
        .metadata = &.{
            .{ .key = "created_by", .value = "zig-parquet-wasm" },
        },
    });
    defer writer.deinit();

    try writer.setKeyValueMetadata("version", "1.0");
    try writer.writeRow(.{ .value = 42 });
    try writer.close();
    return try writer.toOwnedSlice();
}

fn testNestedListReader(allocator: std.mem.Allocator, data: []const u8) !void {
    var reader = try parquet.openBufferRowReader(NestedListRecord, allocator, data, .{});
    defer reader.deinit();

    var count: usize = 0;
    while (try reader.next()) |row| {
        reader.freeRow(&row);
        count += 1;
    }
    assert(count == 1);
}

fn testDictEncoding(allocator: std.mem.Allocator) ![]u8 {
    var writer = try parquet.writeToBufferRows(struct {
        category: []const u8,
        value: i32,
    }, allocator, .{
        .use_dictionary = true,
    });
    defer writer.deinit();

    const categories = [_][]const u8{ "A", "B", "C", "A", "B", "C", "A", "B" };
    for (categories, 0..) |cat, i| {
        try writer.writeRow(.{
            .category = cat,
            .value = @as(i32, @intCast(i)),
        });
    }

    try writer.close();
    return try writer.toOwnedSlice();
}

fn assert(ok: bool) void {
    if (!ok) @panic("assertion failed");
}

pub fn main() !void {
    std.debug.print("zig-parquet WASM — full API surface test\n", .{});
    std.debug.print("=========================================\n\n", .{});

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // 1. Low-level Writer/Reader (all physical types, all codecs)
    {
        std.debug.print("1. Low-level Writer + Reader (all types, all codecs)... ", .{});
        const data = try testLowLevelWriter(allocator);
        defer allocator.free(data);
        try testLowLevelReader(allocator, data);
        try testDynamicReader(allocator, data);
        std.debug.print("OK\n", .{});
    }

    // 2. RowWriter/RowReader (comptime schema, logical types)
    {
        std.debug.print("2. RowWriter + RowReader (logical types, small ints)... ", .{});
        const data = try testRowWriterBasic(allocator);
        defer allocator.free(data);
        try testRowReaderBasic(allocator, data);
        try testDynamicReader(allocator, data);
        std.debug.print("OK\n", .{});
    }

    // 3. Nested types (lists, structs)
    {
        std.debug.print("3. Nested types (lists, structs)... ", .{});
        const data = try testNestedTypes(allocator);
        defer allocator.free(data);
        try testNestedReader(allocator, data);
        try testDynamicReader(allocator, data);
        std.debug.print("OK\n", .{});
    }

    // 4. Map types
    {
        std.debug.print("4. Map types... ", .{});
        const data = try testMapTypes(allocator);
        defer allocator.free(data);
        try testMapReader(allocator, data);
        std.debug.print("OK\n", .{});
    }

    // 5. Decimal types
    {
        std.debug.print("5. Decimal types (9/18/38 precision)... ", .{});
        const data = try testDecimalTypes(allocator);
        defer allocator.free(data);
        try testDynamicReader(allocator, data);
        std.debug.print("OK\n", .{});
    }

    // 6. Interval types
    {
        std.debug.print("6. Interval types... ", .{});
        const data = try testIntervalTypes(allocator);
        defer allocator.free(data);
        try testDynamicReader(allocator, data);
        std.debug.print("OK\n", .{});
    }

    // 7. Delta + byte stream split encodings
    {
        std.debug.print("7. Delta + byte stream split encodings... ", .{});
        const data = try testEncodings(allocator);
        defer allocator.free(data);
        try testDynamicReader(allocator, data);
        std.debug.print("OK\n", .{});
    }

    // 8. Timestamp/Time precisions (micros, millis, nanos)
    {
        std.debug.print("8. Time precisions (micros/millis/nanos)... ", .{});
        const data = try testTimePrecisions(allocator);
        defer allocator.free(data);
        try testDynamicReader(allocator, data);
        std.debug.print("OK\n", .{});
    }

    // 9. Nested lists
    {
        std.debug.print("9. Nested lists ([][]i32)... ", .{});
        const data = try testNestedLists(allocator);
        defer allocator.free(data);
        try testDynamicReader(allocator, data);
        std.debug.print("OK\n", .{});
    }

    // 10. List of structs
    {
        std.debug.print("10. List of structs... ", .{});
        const data = try testListOfStruct(allocator);
        defer allocator.free(data);
        try testDynamicReader(allocator, data);
        std.debug.print("OK\n", .{});
    }

    // 11. Key-value metadata
    {
        std.debug.print("11. Key-value metadata... ", .{});
        const data = try testMetadata(allocator);
        defer allocator.free(data);
        try testDynamicReader(allocator, data);
        std.debug.print("OK\n", .{});
    }

    // 12. Nested list round-trip (RowReader)
    {
        std.debug.print("12. Nested list round-trip (RowReader)... ", .{});
        const data = try testNestedLists(allocator);
        defer allocator.free(data);
        try testNestedListReader(allocator, data);
        std.debug.print("OK\n", .{});
    }

    // 13. Dictionary encoding
    {
        std.debug.print("13. Dictionary encoding... ", .{});
        const data = try testDictEncoding(allocator);
        defer allocator.free(data);
        try testDynamicReader(allocator, data);
        std.debug.print("OK\n", .{});
    }

    std.debug.print("\nAll 13 tests passed.\n", .{});
}
