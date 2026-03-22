//! PyArrow interop tests (Phase 14)
//!
//! Direction 1: Read PyArrow-generated files with zig-parquet
//! Direction 2: Write files with zig-parquet for PyArrow verification (test_interop.py)

const std = @import("std");
const parquet = @import("../lib.zig");
const format = parquet.format;
const Optional = parquet.Optional;
const Interval = parquet.types.Interval;

const output_dir = "../test-files-arrow/interop";

fn ensureOutputDir() !void {
    std.fs.cwd().makePath(output_dir) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };
}

// =============================================================================
// Direction 1: Read PyArrow-generated files
// =============================================================================

test "PyArrow interop: read UUID" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/logical_types/uuid.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file (run from zig-parquet dir): {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 5), reader.metadata.num_rows);

    const schema = reader.getSchema();
    const uuid_schema = schema[1];

    try std.testing.expectEqual(format.PhysicalType.fixed_len_byte_array, uuid_schema.type_.?);
    try std.testing.expectEqual(@as(?i32, 16), uuid_schema.type_length);

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 5), rows.len);

    // UUID "12345678-1234-5678-1234-567812345678"
    try std.testing.expectEqual(@as(usize, 16), rows[0].getColumn(0).?.asBytes().?.len);
    try std.testing.expectEqualSlices(u8, &.{ 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78 }, rows[0].getColumn(0).?.asBytes().?);

    // UUID "00000000-0000-0000-0000-000000000000"
    try std.testing.expectEqualSlices(u8, &([_]u8{0} ** 16), rows[1].getColumn(0).?.asBytes().?);

    // UUID "ffffffff-ffff-ffff-ffff-ffffffffffff"
    try std.testing.expectEqualSlices(u8, &([_]u8{0xff} ** 16), rows[2].getColumn(0).?.asBytes().?);

    // null
    try std.testing.expect(rows[3].getColumn(0).?.isNull());

    // UUID "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    try std.testing.expectEqualSlices(u8, &.{ 0xa1, 0xb2, 0xc3, 0xd4, 0xe5, 0xf6, 0x78, 0x90, 0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x90 }, rows[4].getColumn(0).?.asBytes().?);
}

test "PyArrow interop: read ENUM" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/logical_types/enum.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 5), reader.metadata.num_rows);

    // Read first column (dictionary-encoded strings)
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 5), rows.len);
    try std.testing.expectEqualStrings("RED", rows[0].getColumn(0).?.asBytes().?);
    try std.testing.expectEqualStrings("GREEN", rows[1].getColumn(0).?.asBytes().?);
    try std.testing.expectEqualStrings("BLUE", rows[2].getColumn(0).?.asBytes().?);
    try std.testing.expect(rows[3].getColumn(0).?.isNull());
    try std.testing.expectEqualStrings("RED", rows[4].getColumn(0).?.asBytes().?);
}

test "PyArrow interop: read INT96 timestamp" {
    const allocator = std.testing.allocator;

    const file = std.fs.cwd().openFile("../test-files-arrow/logical_types/int96_timestamp.parquet", .{}) catch |err| {
        std.debug.print("Could not open test file: {}\n", .{err});
        return err;
    };
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    const expected_nanos = [_]?i64{
        1705314600000000000, // 2024-01-15 10:30:00 UTC
        0, // epoch
        961070400000000000, // 2000-06-15 12:00:00 UTC
        null,
        1720117845123456000, // 2024-07-04 18:30:45.123456 UTC
    };

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 5), rows.len);

    for (expected_nanos, 0..) |expected, i| {
        const val = rows[i].getColumn(0).?;
        if (expected) |exp| {
            try std.testing.expect(!val.isNull());
            try std.testing.expectEqual(exp, val.asInt64().?);
        } else {
            try std.testing.expect(val.isNull());
        }
    }
}

// =============================================================================
// Direction 2: Write files for PyArrow verification
// =============================================================================

test "PyArrow interop: write ENUM" {
    const allocator = std.testing.allocator;

    try ensureOutputDir();

    const file = try std.fs.cwd().createFile(output_dir ++ "/enum.parquet", .{});
    defer file.close();

    const columns = [_]parquet.ColumnDef{
        parquet.ColumnDef.enum_("color", true),
    };

    var writer = try parquet.writeToFile(allocator, file, &columns);
    defer writer.deinit();

    const values = [_]Optional([]const u8){
        .{ .value = "RED" },
        .{ .value = "GREEN" },
        .{ .value = "BLUE" },
        .{ .null_value = {} },
        .{ .value = "RED" },
    };
    try writer.writeColumnOptional([]const u8, 0, &values);
    try writer.close();
}

test "PyArrow interop: write BSON" {
    const allocator = std.testing.allocator;

    try ensureOutputDir();

    const file = try std.fs.cwd().createFile(output_dir ++ "/bson.parquet", .{});
    defer file.close();

    const columns = [_]parquet.ColumnDef{
        parquet.ColumnDef.bson("data", true),
    };

    var writer = try parquet.writeToFile(allocator, file, &columns);
    defer writer.deinit();

    // Example BSON documents as raw bytes
    const bson_doc1 = "\x0c\x00\x00\x00\x10x\x00\x01\x00\x00\x00\x00"; // {"x": 1}
    const bson_doc2 = "\x05\x00\x00\x00\x00"; // {}
    const bson_doc3 = "\x10\x00\x00\x00\x02s\x00\x03\x00\x00\x00hi\x00\x00"; // {"s": "hi"}

    const values = [_]Optional([]const u8){
        .{ .value = bson_doc1 },
        .{ .value = bson_doc2 },
        .{ .null_value = {} },
        .{ .value = bson_doc3 },
    };
    try writer.writeColumnOptional([]const u8, 0, &values);
    try writer.close();
}

test "PyArrow interop: write UUID" {
    const allocator = std.testing.allocator;

    try ensureOutputDir();

    const uuid1 = [_]u8{ 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78 };
    const uuid2 = [_]u8{ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
    const uuid3 = [_]u8{ 0xa1, 0xb2, 0xc3, 0xd4, 0xe5, 0xf6, 0x78, 0x90, 0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x90 };

    var file = try std.fs.cwd().createFile(output_dir ++ "/uuid.parquet", .{});
    defer file.close();

    var dw = try parquet.createFileDynamic(allocator, file);
    defer dw.deinit();

    try dw.addColumn("id", parquet.TypeInfo.uuid, .{});
    try dw.begin();

    try dw.setBytes(0, &uuid1);
    try dw.addRow();
    try dw.setBytes(0, &uuid2);
    try dw.addRow();
    try dw.setNull(0);
    try dw.addRow();
    try dw.setBytes(0, &uuid3);
    try dw.addRow();

    try dw.close();
}

test "PyArrow interop: write Interval" {
    const allocator = std.testing.allocator;

    try ensureOutputDir();

    var file = try std.fs.cwd().createFile(output_dir ++ "/interval.parquet", .{});
    defer file.close();

    var dw = try parquet.createFileDynamic(allocator, file);
    defer dw.deinit();

    try dw.addColumn("duration", parquet.TypeInfo.interval, .{});
    try dw.begin();

    const iv1 = Interval.fromComponents(1, 15, 3600000);
    const iv2 = Interval.fromComponents(0, 0, 0);
    const iv3 = Interval.fromComponents(12, 365, 86400000);

    try dw.setBytes(0, &iv1.toBytes());
    try dw.addRow();
    try dw.setBytes(0, &iv2.toBytes());
    try dw.addRow();
    try dw.setNull(0);
    try dw.addRow();
    try dw.setBytes(0, &iv3.toBytes());
    try dw.addRow();

    try dw.close();
}

test "PyArrow interop: write TimestampInt96" {
    const allocator = std.testing.allocator;

    try ensureOutputDir();

    var file = try std.fs.cwd().createFile(output_dir ++ "/int96_timestamp.parquet", .{});
    defer file.close();

    // INT96 is a special physical type not exposed through the high-level Writer.
    // Use column_writer directly for the data, then assemble the footer manually.
    const column_writer = parquet.internals.writer.column_writer;

    const FileTarget = parquet.io.FileTarget;
    var ft = FileTarget.init(file);
    const write_target = ft.target();
    var target_writer = parquet.WriteTargetWriter.init(write_target);
    const writer = target_writer.writer();

    // Write PAR1 magic
    try writer.writeAll(format.PARQUET_MAGIC);

    const nano_vals = [_]Optional(i64){
        .{ .value = 1705314600000000000 }, // 2024-01-15 10:30:00 UTC
        .{ .value = 0 }, // epoch
        .null_value,
        .{ .value = 961070400000000000 }, // 2000-06-15 12:00:00 UTC
    };

    const result = try column_writer.writeColumnChunkInt96OptionalWithPathArray(
        allocator, writer, &.{"ts"}, &nano_vals, true, 4, .uncompressed, true,
    );
    try ft.flush();

    // Write footer
    const thrift = parquet.internals.thrift;
    const schema = [_]format.SchemaElement{
        .{ .type_ = null, .type_length = null, .repetition_type = null, .name = "schema", .num_children = 1, .converted_type = null, .scale = null, .precision = null, .field_id = null, .logical_type = null },
        .{ .type_ = .int96, .type_length = null, .repetition_type = .optional, .name = "ts", .num_children = null, .converted_type = null, .scale = null, .precision = null, .field_id = null, .logical_type = null },
    };

    const col_chunk = format.ColumnChunk{
        .file_path = null,
        .file_offset = result.file_offset,
        .meta_data = result.metadata,
    };

    const row_group = format.RowGroup{
        .columns = @constCast(&[_]format.ColumnChunk{col_chunk}),
        .total_byte_size = result.metadata.total_compressed_size,
        .num_rows = 4,
        .sorting_columns = null,
        .file_offset = 4,
        .total_compressed_size = result.metadata.total_compressed_size,
        .ordinal = 0,
    };

    const file_metadata = format.FileMetaData{
        .version = 1,
        .schema = @constCast(&schema),
        .num_rows = 4,
        .row_groups = @constCast(&[_]format.RowGroup{row_group}),
        .key_value_metadata = null,
        .created_by = "zig-parquet",
    };

    var thrift_writer = thrift.CompactWriter.init(allocator);
    defer thrift_writer.deinit();
    try file_metadata.serialize(&thrift_writer);
    const footer_bytes = thrift_writer.getWritten();

    try writer.writeAll(footer_bytes);
    var len_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_buf, @intCast(footer_bytes.len), .little);
    try writer.writeAll(&len_buf);
    try writer.writeAll(format.PARQUET_MAGIC);
    try ft.flush();

    // Clean up the column chunk metadata
    allocator.free(result.metadata.encodings);
    for (result.metadata.path_in_schema) |p| allocator.free(p);
    allocator.free(result.metadata.path_in_schema);
    if (result.metadata.statistics) |stats| {
        if (stats.max) |m| allocator.free(m);
        if (stats.min) |m| allocator.free(m);
        if (stats.max_value) |m| allocator.free(m);
        if (stats.min_value) |m| allocator.free(m);
    }
}

test "PyArrow interop: write Geometry" {
    const allocator = std.testing.allocator;

    try ensureOutputDir();

    const file = try std.fs.cwd().createFile(output_dir ++ "/geometry.parquet", .{});
    defer file.close();

    const columns = [_]parquet.ColumnDef{
        parquet.ColumnDef.geometry("geom", true, "OGC:CRS84"),
    };

    var writer = try parquet.writeToFile(allocator, file, &columns);
    defer writer.deinit();

    // WKB Point(1.0, 2.0): byte_order(1) + type(1=Point,4) + x(f64) + y(f64) = 21 bytes
    const wkb_point1 = "\x01" ++ // little-endian
        "\x01\x00\x00\x00" ++ // type = Point
        "\x00\x00\x00\x00\x00\x00\xf0\x3f" ++ // x = 1.0
        "\x00\x00\x00\x00\x00\x00\x00\x40"; // y = 2.0

    const wkb_point2 = "\x01" ++
        "\x01\x00\x00\x00" ++
        "\x00\x00\x00\x00\x00\x80\x46\x40" ++ // x = 45.0
        "\x00\x00\x00\x00\x00\x00\x24\x40"; // y = 10.0

    const values = [_]Optional([]const u8){
        .{ .value = wkb_point1 },
        .{ .value = wkb_point2 },
        .{ .null_value = {} },
    };
    try writer.writeColumnOptional([]const u8, 0, &values);
    try writer.close();
}

test "PyArrow interop: write Geography" {
    const allocator = std.testing.allocator;

    try ensureOutputDir();

    const file = try std.fs.cwd().createFile(output_dir ++ "/geography.parquet", .{});
    defer file.close();

    const columns = [_]parquet.ColumnDef{
        parquet.ColumnDef.geography("geog", true, "OGC:CRS84", null),
    };

    var writer = try parquet.writeToFile(allocator, file, &columns);
    defer writer.deinit();

    const wkb_point1 = "\x01" ++
        "\x01\x00\x00\x00" ++
        "\x00\x00\x00\x00\x00\x00\x59\x40" ++ // x = 100.0
        "\x00\x00\x00\x00\x00\x00\x44\xc0"; // y = -40.0

    const wkb_point2 = "\x01" ++
        "\x01\x00\x00\x00" ++
        "\x00\x00\x00\x00\x00\xc0\x52\xc0" ++ // x = -75.0
        "\x00\x00\x00\x00\x00\x80\x44\x40"; // y = 41.0

    const values = [_]Optional([]const u8){
        .{ .value = wkb_point1 },
        .{ .value = wkb_point2 },
        .{ .null_value = {} },
    };
    try writer.writeColumnOptional([]const u8, 0, &values);
    try writer.close();
}

// =============================================================================
// Direction 2 (continued): Broad type surface
// =============================================================================

test "PyArrow interop: write primitives" {
    const allocator = std.testing.allocator;

    try ensureOutputDir();

    const file = try std.fs.cwd().createFile(output_dir ++ "/primitives.parquet", .{});
    defer file.close();

    const columns = [_]parquet.ColumnDef{
        .{ .name = "bool_col", .type_ = .boolean, .optional = true },
        .{ .name = "int32_col", .type_ = .int32, .optional = true },
        .{ .name = "int64_col", .type_ = .int64, .optional = true },
        .{ .name = "float_col", .type_ = .float, .optional = true },
        .{ .name = "double_col", .type_ = .double, .optional = true },
        parquet.ColumnDef.string("string_col", true),
        .{ .name = "binary_col", .type_ = .byte_array, .optional = true },
    };

    var writer = try parquet.writeToFile(allocator, file, &columns);
    defer writer.deinit();

    const bools = [_]Optional(bool){
        .{ .value = true },
        .{ .value = false },
        .{ .null_value = {} },
        .{ .value = true },
    };
    try writer.writeColumnOptional(bool, 0, &bools);

    const i32s = [_]Optional(i32){
        .{ .value = 42 },
        .{ .value = -100 },
        .{ .value = 2147483647 },
        .{ .null_value = {} },
    };
    try writer.writeColumnOptional(i32, 1, &i32s);

    const i64s = [_]Optional(i64){
        .{ .value = 1000000000000 },
        .{ .null_value = {} },
        .{ .value = -9223372036854775808 },
        .{ .value = 0 },
    };
    try writer.writeColumnOptional(i64, 2, &i64s);

    const f32s = [_]Optional(f32){
        .{ .value = 3.14 },
        .{ .value = -2.5 },
        .{ .null_value = {} },
        .{ .value = 0.0 },
    };
    try writer.writeColumnOptional(f32, 3, &f32s);

    const f64s = [_]Optional(f64){
        .{ .value = 1.23456789012345 },
        .{ .value = -99.99 },
        .{ .value = 0.0 },
        .{ .null_value = {} },
    };
    try writer.writeColumnOptional(f64, 4, &f64s);

    const strings = [_]Optional([]const u8){
        .{ .value = "hello" },
        .{ .value = "world" },
        .{ .null_value = {} },
        .{ .value = "" },
    };
    try writer.writeColumnOptional([]const u8, 5, &strings);

    const binaries = [_]Optional([]const u8){
        .{ .value = "\x00\x01\x02\x03" },
        .{ .null_value = {} },
        .{ .value = "\xff\xfe" },
        .{ .value = "" },
    };
    try writer.writeColumnOptional([]const u8, 6, &binaries);

    try writer.close();
}

test "PyArrow interop: write small ints" {
    const allocator = std.testing.allocator;

    try ensureOutputDir();

    const file = try std.fs.cwd().createFile(output_dir ++ "/small_ints.parquet", .{});
    defer file.close();

    const columns = [_]parquet.ColumnDef{
        parquet.ColumnDef.int8("i8_col", true),
        parquet.ColumnDef.int16("i16_col", true),
        parquet.ColumnDef.uint8("u8_col", true),
        parquet.ColumnDef.uint16("u16_col", true),
        parquet.ColumnDef.uint32("u32_col", true),
        parquet.ColumnDef.uint64("u64_col", true),
    };

    var writer = try parquet.writeToFile(allocator, file, &columns);
    defer writer.deinit();

    // INT8: stored as INT32 with INT(8,signed) annotation
    const i8_vals = [_]Optional(i32){
        .{ .value = -128 },
        .{ .value = 127 },
        .{ .value = 0 },
        .{ .null_value = {} },
    };
    try writer.writeColumnOptional(i32, 0, &i8_vals);

    // INT16
    const i16_vals = [_]Optional(i32){
        .{ .value = -32768 },
        .{ .value = 32767 },
        .{ .null_value = {} },
        .{ .value = 0 },
    };
    try writer.writeColumnOptional(i32, 1, &i16_vals);

    // UINT8
    const u8_vals = [_]Optional(i32){
        .{ .value = 0 },
        .{ .value = 255 },
        .{ .null_value = {} },
        .{ .value = 128 },
    };
    try writer.writeColumnOptional(i32, 2, &u8_vals);

    // UINT16
    const u16_vals = [_]Optional(i32){
        .{ .value = 0 },
        .{ .value = 65535 },
        .{ .value = 32768 },
        .{ .null_value = {} },
    };
    try writer.writeColumnOptional(i32, 3, &u16_vals);

    // UINT32: stored as INT32 with INT(32,unsigned)
    const u32_vals = [_]Optional(i32){
        .{ .value = 0 },
        .{ .value = 2147483647 },
        .{ .null_value = {} },
        .{ .value = 1 },
    };
    try writer.writeColumnOptional(i32, 4, &u32_vals);

    // UINT64: stored as INT64 with INT(64,unsigned)
    const u64_vals = [_]Optional(i64){
        .{ .value = 0 },
        .{ .value = 9223372036854775807 },
        .{ .null_value = {} },
        .{ .value = 1 },
    };
    try writer.writeColumnOptional(i64, 5, &u64_vals);

    try writer.close();
}

test "PyArrow interop: write temporal" {
    const allocator = std.testing.allocator;

    try ensureOutputDir();

    const file = try std.fs.cwd().createFile(output_dir ++ "/temporal.parquet", .{});
    defer file.close();

    const columns = [_]parquet.ColumnDef{
        parquet.ColumnDef.date("date_col", true),
        parquet.ColumnDef.timestamp("ts_millis", .millis, true, true),
        parquet.ColumnDef.timestamp("ts_micros", .micros, true, true),
        parquet.ColumnDef.time("time_millis", .millis, true, true),
        parquet.ColumnDef.time("time_micros", .micros, true, true),
    };

    var writer = try parquet.writeToFile(allocator, file, &columns);
    defer writer.deinit();

    // DATE: days since epoch
    const dates = [_]Optional(i32){
        .{ .value = 19737 }, // 2024-01-15
        .{ .value = 0 }, // 1970-01-01
        .{ .null_value = {} },
        .{ .value = 11123 }, // 2000-06-15
    };
    try writer.writeColumnOptional(i32, 0, &dates);

    // TIMESTAMP millis UTC
    const ts_millis = [_]Optional(i64){
        .{ .value = 1705314600000 }, // 2024-01-15 10:30:00 UTC
        .{ .value = 0 }, // epoch
        .{ .null_value = {} },
        .{ .value = 961070400000 }, // 2000-06-15 12:00:00 UTC
    };
    try writer.writeColumnOptional(i64, 1, &ts_millis);

    // TIMESTAMP micros UTC
    const ts_micros = [_]Optional(i64){
        .{ .value = 1705314600000000 }, // 2024-01-15 10:30:00 UTC
        .{ .value = 0 },
        .{ .null_value = {} },
        .{ .value = 961070400000000 },
    };
    try writer.writeColumnOptional(i64, 2, &ts_micros);

    // TIME millis: milliseconds since midnight
    const time_millis = [_]Optional(i32){
        .{ .value = 37800000 }, // 10:30:00
        .{ .value = 0 }, // 00:00:00
        .{ .null_value = {} },
        .{ .value = 43200000 }, // 12:00:00
    };
    try writer.writeColumnOptional(i32, 3, &time_millis);

    // TIME micros: microseconds since midnight
    const time_micros = [_]Optional(i64){
        .{ .value = 37800000000 }, // 10:30:00
        .{ .value = 0 },
        .{ .null_value = {} },
        .{ .value = 43200000000 },
    };
    try writer.writeColumnOptional(i64, 4, &time_micros);

    try writer.close();
}

test "PyArrow interop: write decimal" {
    const allocator = std.testing.allocator;

    try ensureOutputDir();

    const file = try std.fs.cwd().createFile(output_dir ++ "/decimal.parquet", .{});
    defer file.close();

    const columns = [_]parquet.ColumnDef{
        parquet.ColumnDef.decimal("dec_9_2", 9, 2, true),
        parquet.ColumnDef.decimal("dec_18_4", 18, 4, true),
    };

    var writer = try parquet.writeToFile(allocator, file, &columns);
    defer writer.deinit();

    // DECIMAL(9,2) stored as INT32: value * 100
    const dec9 = [_]Optional(i32){
        .{ .value = 12345 }, // 123.45
        .{ .value = -99999 }, // -999.99
        .{ .value = 1 }, // 0.01
        .{ .null_value = {} },
    };
    try writer.writeColumnOptional(i32, 0, &dec9);

    // DECIMAL(18,4) stored as INT64: value * 10000
    const dec18 = [_]Optional(i64){
        .{ .value = 123456789012345678 }, // 12345678901234.5678
        .{ .null_value = {} },
        .{ .value = 1 }, // 0.0001
        .{ .value = 10000 }, // 1.0000
    };
    try writer.writeColumnOptional(i64, 1, &dec18);

    try writer.close();
}

test "PyArrow interop: write float16" {
    const allocator = std.testing.allocator;

    try ensureOutputDir();

    const file = try std.fs.cwd().createFile(output_dir ++ "/float16.parquet", .{});
    defer file.close();

    const columns = [_]parquet.ColumnDef{
        parquet.ColumnDef.float16("f16_col", true),
    };

    var writer = try parquet.writeToFile(allocator, file, &columns);
    defer writer.deinit();

    // FLOAT16 stored as FLBA(2), IEEE 754 half-precision
    const f16_1_5: [2]u8 = .{ 0x00, 0x3e }; // 1.5
    const f16_neg_2_25: [2]u8 = .{ 0x80, 0xc0 }; // -2.25 (sign=1, exp=16, mantissa=0x080)
    const f16_zero: [2]u8 = .{ 0x00, 0x00 }; // 0.0

    const values = [_]Optional([]const u8){
        .{ .value = &f16_1_5 },
        .{ .value = &f16_neg_2_25 },
        .{ .value = &f16_zero },
        .{ .null_value = {} },
    };
    try writer.writeColumnFixedByteArrayOptional(0, &values);

    try writer.close();
}

test "PyArrow interop: write nested" {
    const allocator = std.testing.allocator;

    try ensureOutputDir();

    var file = try std.fs.cwd().createFile(output_dir ++ "/nested.parquet", .{});
    defer file.close();

    var dw = try parquet.createFileDynamic(allocator, file);
    defer dw.deinit();

    // int_list: optional list of int32
    const list_node = try dw.allocSchemaNode(.{ .optional =
        try dw.allocSchemaNode(.{ .list = try dw.allocSchemaNode(.{ .int32 = .{} }) }) });
    try dw.addColumnNested("int_list", list_node, .{});

    // point: struct { x: i32, y: i32 }
    const fields = try dw.allocSchemaFields(2);
    fields[0] = .{ .name = try dw.dupeSchemaName("x"), .node = try dw.allocSchemaNode(.{ .int32 = .{} }) };
    fields[1] = .{ .name = try dw.dupeSchemaName("y"), .node = try dw.allocSchemaNode(.{ .int32 = .{} }) };
    const struct_node = try dw.allocSchemaNode(.{ .struct_ = .{ .fields = fields } });
    try dw.addColumnNested("point", struct_node, .{});

    try dw.begin();

    // Row 0: int_list=[1,2,3], point={x:10, y:11}
    try dw.beginList(0);
    try dw.appendNestedValue(0, .{ .int32_val = 1 });
    try dw.appendNestedValue(0, .{ .int32_val = 2 });
    try dw.appendNestedValue(0, .{ .int32_val = 3 });
    try dw.endList(0);
    try dw.beginStruct(1);
    try dw.setStructField(1, 0, .{ .int32_val = 10 });
    try dw.setStructField(1, 1, .{ .int32_val = 11 });
    try dw.endStruct(1);
    try dw.addRow();

    // Row 1: int_list=[4,5], point={x:20, y:21}
    try dw.beginList(0);
    try dw.appendNestedValue(0, .{ .int32_val = 4 });
    try dw.appendNestedValue(0, .{ .int32_val = 5 });
    try dw.endList(0);
    try dw.beginStruct(1);
    try dw.setStructField(1, 0, .{ .int32_val = 20 });
    try dw.setStructField(1, 1, .{ .int32_val = 21 });
    try dw.endStruct(1);
    try dw.addRow();

    // Row 2: int_list=null, point={x:0, y:0}
    try dw.setNull(0);
    try dw.beginStruct(1);
    try dw.setStructField(1, 0, .{ .int32_val = 0 });
    try dw.setStructField(1, 1, .{ .int32_val = 0 });
    try dw.endStruct(1);
    try dw.addRow();

    // Row 3: int_list=[] (empty list), point={x:30, y:31}
    try dw.beginList(0);
    try dw.endList(0);
    try dw.beginStruct(1);
    try dw.setStructField(1, 0, .{ .int32_val = 30 });
    try dw.setStructField(1, 1, .{ .int32_val = 31 });
    try dw.endStruct(1);
    try dw.addRow();

    try dw.close();
}
