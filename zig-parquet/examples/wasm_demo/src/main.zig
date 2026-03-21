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

fn testDynamicWriterBasic(allocator: std.mem.Allocator) ![]u8 {
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("sensor_id", parquet.TypeInfo.int32, .{});
    try writer.addColumn("timestamp", parquet.TypeInfo.timestamp_micros, .{});
    try writer.addColumn("date", parquet.TypeInfo.date, .{});
    try writer.addColumn("time", parquet.TypeInfo.time_micros, .{});
    try writer.addColumn("temperature", parquet.TypeInfo.double_, .{});
    try writer.addColumn("humidity", parquet.TypeInfo.float_, .{});
    try writer.addColumn("label", parquet.TypeInfo.string, .{});
    try writer.addColumn("active", parquet.TypeInfo.bool_, .{});
    try writer.addColumn("uuid", parquet.TypeInfo.uuid, .{});
    try writer.addColumn("small_int", parquet.TypeInfo.int8, .{});
    try writer.addColumn("medium_int", parquet.TypeInfo.int16, .{});
    try writer.addColumn("unsigned", parquet.TypeInfo.uint32, .{});
    try writer.addColumn("big_unsigned", parquet.TypeInfo.uint64, .{});
    writer.setCompression(.zstd);
    try writer.begin();

    // Row 1
    try writer.setInt32(0, 1);
    try writer.setInt64(1, 1704067200000000);
    try writer.setInt32(2, 19000);
    try writer.setInt64(3, 3600000000);
    try writer.setDouble(4, 23.5);
    try writer.setFloat(5, 65.0);
    try writer.setBytes(6, "Building A");
    try writer.setBool(7, true);
    try writer.setBytes(8, &[_]u8{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 });
    try writer.setInt32(9, -5);
    try writer.setInt32(10, 1000);
    try writer.setInt32(11, 42);
    try writer.setInt64(12, 9999999);
    try writer.addRow();

    // Row 2
    try writer.setInt32(0, 2);
    try writer.setInt64(1, 1704153600000000);
    try writer.setInt32(2, 19001);
    try writer.setInt64(3, 7200000000);
    try writer.setDouble(4, 18.2);
    try writer.setNull(5);
    try writer.setNull(6);
    try writer.setBool(7, false);
    try writer.setBytes(8, &[_]u8{ 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32 });
    try writer.setInt32(9, 127);
    try writer.setInt32(10, -32000);
    try writer.setInt32(11, 0);
    try writer.setInt64(12, 0);
    try writer.addRow();

    try writer.close();
    return try writer.toOwnedSlice();
}

fn testDynamicReaderBasic(allocator: std.mem.Allocator, data: []const u8) !void {
    var reader = try parquet.openBufferDynamic(allocator, data, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }
    assert(rows.len == 2);
}

fn testNestedTypes(allocator: std.mem.Allocator) ![]u8 {
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", parquet.TypeInfo.int32, .{});

    // tags: list<string>
    const tag_leaf = try writer.allocSchemaNode(.{ .byte_array = .{ .logical = .string } });
    const tags_node = try writer.allocSchemaNode(.{ .list = tag_leaf });
    try writer.addColumnNested("tags", tags_node, .{});

    // scores: list<f64>
    const score_leaf = try writer.allocSchemaNode(.{ .double = .{} });
    const scores_node = try writer.allocSchemaNode(.{ .list = score_leaf });
    try writer.addColumnNested("scores", scores_node, .{});

    // int_lists: ?list<i32>
    const int_leaf = try writer.allocSchemaNode(.{ .int32 = .{} });
    const int_list = try writer.allocSchemaNode(.{ .list = int_leaf });
    const opt_int_list = try writer.allocSchemaNode(.{ .optional = int_list });
    try writer.addColumnNested("int_lists", opt_int_list, .{});

    // address: struct { street: string, city: string, zip: i32 }
    const street_leaf = try writer.allocSchemaNode(.{ .byte_array = .{ .logical = .string } });
    const city_leaf = try writer.allocSchemaNode(.{ .byte_array = .{ .logical = .string } });
    const zip_leaf = try writer.allocSchemaNode(.{ .int32 = .{} });
    var addr_fields = try writer.allocSchemaFields(3);
    addr_fields[0] = .{ .name = try writer.dupeSchemaName("street"), .node = street_leaf };
    addr_fields[1] = .{ .name = try writer.dupeSchemaName("city"), .node = city_leaf };
    addr_fields[2] = .{ .name = try writer.dupeSchemaName("zip"), .node = zip_leaf };
    const addr_node = try writer.allocSchemaNode(.{ .struct_ = .{ .fields = addr_fields } });
    try writer.addColumnNested("address", addr_node, .{});

    writer.setCompression(.snappy);
    try writer.setPathProperties("address.city", .{ .compression = .zstd });
    try writer.setPathProperties("address.zip", .{ .use_dictionary = false });
    try writer.begin();

    // Row 1
    try writer.setInt32(0, 1);

    try writer.beginList(1);
    try writer.appendNestedBytes(1, "sensor");
    try writer.appendNestedBytes(1, "outdoor");
    try writer.endList(1);

    try writer.beginList(2);
    try writer.appendNestedValue(2, .{ .double_val = 9.5 });
    try writer.appendNestedValue(2, .{ .double_val = 8.3 });
    try writer.appendNestedValue(2, .{ .double_val = 7.1 });
    try writer.endList(2);

    try writer.beginList(3);
    try writer.appendNestedValue(3, .{ .int32_val = 10 });
    try writer.appendNestedValue(3, .{ .int32_val = 20 });
    try writer.appendNestedValue(3, .{ .int32_val = 30 });
    try writer.endList(3);

    try writer.beginStruct(4);
    try writer.setStructFieldBytes(4, 0, "123 Main St");
    try writer.setStructFieldBytes(4, 1, "Springfield");
    try writer.setStructField(4, 2, .{ .int32_val = 62701 });
    try writer.endStruct(4);

    try writer.addRow();

    // Row 2
    try writer.setInt32(0, 2);

    try writer.beginList(1);
    try writer.appendNestedBytes(1, "indoor");
    try writer.endList(1);

    try writer.beginList(2);
    try writer.endList(2); // empty list

    try writer.setNull(3); // null list

    try writer.beginStruct(4);
    try writer.setStructFieldBytes(4, 0, "456 Oak Ave");
    try writer.setStructFieldBytes(4, 1, "Shelbyville");
    try writer.setStructField(4, 2, .{ .int32_val = 62702 });
    try writer.endStruct(4);

    try writer.addRow();

    try writer.close();
    return try writer.toOwnedSlice();
}

fn testNestedReader(allocator: std.mem.Allocator, data: []const u8) !void {
    var reader = try parquet.openBufferDynamic(allocator, data, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }
    assert(rows.len == 2);
}

fn testMapTypes(allocator: std.mem.Allocator) ![]u8 {
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", parquet.TypeInfo.int32, .{});

    // labels: ?map<string, string>
    const key_str = try writer.allocSchemaNode(.{ .byte_array = .{ .logical = .string } });
    const val_str = try writer.allocSchemaNode(.{ .byte_array = .{ .logical = .string } });
    const labels_map = try writer.allocSchemaNode(.{ .map = .{ .key = key_str, .value = val_str } });
    const opt_labels = try writer.allocSchemaNode(.{ .optional = labels_map });
    try writer.addColumnNested("labels", opt_labels, .{});

    // counts: ?map<string, i32>
    const val_i32 = try writer.allocSchemaNode(.{ .int32 = .{} });
    const counts_map = try writer.allocSchemaNode(.{ .map = .{ .key = key_str, .value = val_i32 } });
    const opt_counts = try writer.allocSchemaNode(.{ .optional = counts_map });
    try writer.addColumnNested("counts", opt_counts, .{});

    try writer.begin();

    // Row 1: id=1, labels={color:red, size:large}, counts={apples:5}
    try writer.setInt32(0, 1);

    try writer.beginMap(1);
    try writer.beginMapEntry(1);
    try writer.appendNestedBytes(1, "color");
    try writer.appendNestedBytes(1, "red");
    try writer.endMapEntry(1);
    try writer.beginMapEntry(1);
    try writer.appendNestedBytes(1, "size");
    try writer.appendNestedBytes(1, "large");
    try writer.endMapEntry(1);
    try writer.endMap(1);

    try writer.beginMap(2);
    try writer.beginMapEntry(2);
    try writer.appendNestedBytes(2, "apples");
    try writer.appendNestedValue(2, .{ .int32_val = 5 });
    try writer.endMapEntry(2);
    try writer.endMap(2);

    try writer.addRow();

    // Row 2: id=2, labels=null, counts=empty
    try writer.setInt32(0, 2);
    try writer.setNull(1);
    try writer.beginMap(2);
    try writer.endMap(2);

    try writer.addRow();

    try writer.close();
    return try writer.toOwnedSlice();
}

fn testMapReader(allocator: std.mem.Allocator, data: []const u8) !void {
    var reader = try parquet.openBufferDynamic(allocator, data, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }
    assert(rows.len == 2);
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

fn testDecimalTypes(allocator: std.mem.Allocator) ![]u8 {
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", parquet.TypeInfo.int32, .{});
    try writer.addColumn("price", parquet.TypeInfo.forDecimal(9, 2), .{});
    try writer.addColumn("big_price", parquet.TypeInfo.forDecimal(18, 4), .{});
    try writer.addColumn("huge_price", parquet.TypeInfo.forDecimal(38, 10), .{});
    try writer.begin();

    try writer.setInt32(0, 1);
    try writer.setInt32(1, 12345); // Decimal(9,2) -> INT32
    try writer.setInt64(2, 9999999); // Decimal(18,4) -> INT64
    // Decimal(38,10) -> FIXED_LEN_BYTE_ARRAY, encode as big-endian
    var huge_bytes: [16]u8 = std.mem.zeroes([16]u8);
    std.mem.writeInt(i64, huge_bytes[8..16], 1234567890, .big);
    try writer.setBytes(3, &huge_bytes);

    try writer.addRow();

    try writer.close();
    return try writer.toOwnedSlice();
}

fn testIntervalTypes(allocator: std.mem.Allocator) ![]u8 {
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", parquet.TypeInfo.int32, .{});
    try writer.addColumn("duration", parquet.TypeInfo.interval, .{});
    try writer.addColumn("opt_duration", parquet.TypeInfo.interval, .{});
    try writer.begin();

    // Row 1
    try writer.setInt32(0, 1);
    // duration: 12 months
    var dur1: [12]u8 = undefined;
    std.mem.writeInt(i32, dur1[0..4], 12, .little);
    std.mem.writeInt(i32, dur1[4..8], 0, .little);
    std.mem.writeInt(i32, dur1[8..12], 0, .little);
    try writer.setBytes(1, &dur1);
    // opt_duration: 30 days
    var dur2: [12]u8 = undefined;
    std.mem.writeInt(i32, dur2[0..4], 0, .little);
    std.mem.writeInt(i32, dur2[4..8], 30, .little);
    std.mem.writeInt(i32, dur2[8..12], 0, .little);
    try writer.setBytes(2, &dur2);
    try writer.addRow();

    // Row 2
    try writer.setInt32(0, 2);
    // duration: 86400000 millis
    var dur3: [12]u8 = undefined;
    std.mem.writeInt(i32, dur3[0..4], 0, .little);
    std.mem.writeInt(i32, dur3[4..8], 0, .little);
    std.mem.writeInt(i32, dur3[8..12], 86400000, .little);
    try writer.setBytes(1, &dur3);
    try writer.setNull(2);
    try writer.addRow();

    try writer.close();
    return try writer.toOwnedSlice();
}

fn testEncodings(allocator: std.mem.Allocator) ![]u8 {
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("delta_int", parquet.TypeInfo.int64, .{});
    try writer.addColumn("delta_str", parquet.TypeInfo.string, .{});
    try writer.addColumn("bss_float", parquet.TypeInfo.double_, .{ .encoding = .plain });

    writer.setUseDictionary(false);
    writer.setIntEncoding(.delta_binary_packed);
    writer.setMaxPageSize(4096);
    try writer.begin();

    var i: i64 = 0;
    while (i < 100) : (i += 1) {
        try writer.setInt64(0, i * 1000);
        try writer.setBytes(1, "test_string");
        try writer.setDouble(2, @as(f64, @floatFromInt(i)) * 1.1);
        try writer.addRow();
    }

    try writer.close();
    return try writer.toOwnedSlice();
}

fn testTimePrecisions(allocator: std.mem.Allocator) ![]u8 {
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("ts_micros", parquet.TypeInfo.timestamp_micros, .{});
    try writer.addColumn("ts_millis", parquet.TypeInfo.timestamp_millis, .{});
    try writer.addColumn("ts_nanos", parquet.TypeInfo.timestamp_nanos, .{});
    try writer.addColumn("t_micros", parquet.TypeInfo.time_micros, .{});
    try writer.addColumn("t_millis", parquet.TypeInfo.time_millis, .{});
    try writer.addColumn("t_nanos", parquet.TypeInfo.time_nanos, .{});
    try writer.begin();

    try writer.setInt64(0, 1704067200000000);
    try writer.setInt64(1, 1704067200000);
    try writer.setInt64(2, 1704067200000000000);
    try writer.setInt64(3, 3600000000);
    try writer.setInt32(4, 3600000);
    try writer.setInt64(5, 3600000000000);
    try writer.addRow();

    try writer.close();
    return try writer.toOwnedSlice();
}

fn testNestedLists(allocator: std.mem.Allocator) ![]u8 {
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", parquet.TypeInfo.int32, .{});

    // matrix: list<list<i32>>
    const inner_leaf = try writer.allocSchemaNode(.{ .int32 = .{} });
    const inner_list = try writer.allocSchemaNode(.{ .list = inner_leaf });
    const outer_list = try writer.allocSchemaNode(.{ .list = inner_list });
    try writer.addColumnNested("matrix", outer_list, .{});

    try writer.begin();

    try writer.setInt32(0, 1);
    // matrix = [[1, 2, 3], [4, 5]]
    try writer.beginList(1);
    // inner list [1, 2, 3]
    try writer.beginList(1);
    try writer.appendNestedValue(1, .{ .int32_val = 1 });
    try writer.appendNestedValue(1, .{ .int32_val = 2 });
    try writer.appendNestedValue(1, .{ .int32_val = 3 });
    try writer.endList(1);
    // inner list [4, 5]
    try writer.beginList(1);
    try writer.appendNestedValue(1, .{ .int32_val = 4 });
    try writer.appendNestedValue(1, .{ .int32_val = 5 });
    try writer.endList(1);
    try writer.endList(1);
    try writer.addRow();

    try writer.close();
    return try writer.toOwnedSlice();
}

fn testListOfStruct(allocator: std.mem.Allocator) ![]u8 {
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", parquet.TypeInfo.int32, .{});

    // points: list<struct { x: f64, y: f64, label: ?string }>
    const x_leaf = try writer.allocSchemaNode(.{ .double = .{} });
    const y_leaf = try writer.allocSchemaNode(.{ .double = .{} });
    const lbl_leaf = try writer.allocSchemaNode(.{ .byte_array = .{ .logical = .string } });
    const opt_lbl = try writer.allocSchemaNode(.{ .optional = lbl_leaf });
    var pt_fields = try writer.allocSchemaFields(3);
    pt_fields[0] = .{ .name = try writer.dupeSchemaName("x"), .node = x_leaf };
    pt_fields[1] = .{ .name = try writer.dupeSchemaName("y"), .node = y_leaf };
    pt_fields[2] = .{ .name = try writer.dupeSchemaName("label"), .node = opt_lbl };
    const pt_struct = try writer.allocSchemaNode(.{ .struct_ = .{ .fields = pt_fields } });
    const pts_list = try writer.allocSchemaNode(.{ .list = pt_struct });
    try writer.addColumnNested("points", pts_list, .{});

    try writer.begin();

    try writer.setInt32(0, 1);
    try writer.beginList(1);
    // Point 1: {1.0, 2.0, "origin"}
    try writer.beginStruct(1);
    try writer.setStructField(1, 0, .{ .double_val = 1.0 });
    try writer.setStructField(1, 1, .{ .double_val = 2.0 });
    try writer.setStructFieldBytes(1, 2, "origin");
    try writer.endStruct(1);
    // Point 2: {3.0, 4.0, null}
    try writer.beginStruct(1);
    try writer.setStructField(1, 0, .{ .double_val = 3.0 });
    try writer.setStructField(1, 1, .{ .double_val = 4.0 });
    try writer.setStructField(1, 2, .null_val);
    try writer.endStruct(1);
    try writer.endList(1);
    try writer.addRow();

    try writer.close();
    return try writer.toOwnedSlice();
}

fn testMetadata(allocator: std.mem.Allocator) ![]u8 {
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("value", parquet.TypeInfo.int32, .{});
    try writer.setKvMetadata("created_by", "zig-parquet-wasm");
    try writer.begin();

    try writer.setKvMetadata("version", "1.0");
    try writer.setInt32(0, 42);
    try writer.addRow();
    try writer.close();
    return try writer.toOwnedSlice();
}

fn testNestedListReader(allocator: std.mem.Allocator, data: []const u8) !void {
    var reader = try parquet.openBufferDynamic(allocator, data, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }
    assert(rows.len == 1);
}

fn testDictEncoding(allocator: std.mem.Allocator) ![]u8 {
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("category", parquet.TypeInfo.string, .{});
    try writer.addColumn("value", parquet.TypeInfo.int32, .{});

    writer.setDictionarySizeLimit(512 * 1024);
    writer.setDictionaryCardinalityThreshold(0.4);
    try writer.begin();

    const categories = [_][]const u8{ "A", "B", "C", "A", "B", "C", "A", "B" };
    for (categories, 0..) |cat, i| {
        try writer.setBytes(0, cat);
        try writer.setInt32(1, @as(i32, @intCast(i)));
        try writer.addRow();
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

    // 2. DynamicWriter/DynamicReader (logical types, small ints)
    {
        std.debug.print("2. DynamicWriter + DynamicReader (logical types, small ints)... ", .{});
        const data = try testDynamicWriterBasic(allocator);
        defer allocator.free(data);
        try testDynamicReaderBasic(allocator, data);
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

    // 7. Encodings
    {
        std.debug.print("7. Encodings... ", .{});
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
        std.debug.print("9. Nested lists (list<list<i32>>)... ", .{});
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

    // 12. Nested list round-trip (DynamicReader)
    {
        std.debug.print("12. Nested list round-trip (DynamicReader)... ", .{});
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
