//! Negative / error-path tests
//!
//! Verifies that invalid inputs, unsupported encoding/type combinations,
//! and incorrect API usage produce explicit errors rather than silent
//! data corruption or panics.

const std = @import("std");
const parquet = @import("../lib.zig");
const leaf_decoder = @import("../core/leaf_decoder.zig");
const format = @import("../core/format.zig");
const thrift = @import("../core/thrift/mod.zig");

const TypeInfo = parquet.TypeInfo;
const ColumnProperties = parquet.DynamicWriter.ColumnProperties;

// =========================================================================
// Decoder: unsupported encoding / physical type combinations (V1)
// =========================================================================

fn makeSchemaElem(physical_type: format.PhysicalType) format.SchemaElement {
    return .{
        .type_ = physical_type,
        .repetition_type = .required,
        .name = "col",
    };
}

/// Decodes a column chunk holding one uncompressed V1 data page with one value of
/// `physical_type` in `value_encoding`, and expects `error.UnsupportedEncoding`. V1 and V2
/// pages share the value decoding (`leaf_decoder` `decodeValues`), so this covers both.
fn expectDecoderUnsupportedEncoding(
    physical_type: format.PhysicalType,
    value_encoding: format.Encoding,
) !void {
    const allocator = std.testing.allocator;
    const body: [16]u8 = @splat(0);
    const header = format.PageHeader{
        .type_ = .data_page,
        .uncompressed_page_size = body.len,
        .compressed_page_size = body.len,
        .data_page_header = .{ .num_values = 1, .encoding = value_encoding },
    };
    var writer = thrift.CompactWriter.init(allocator);
    defer writer.deinit();
    try header.serialize(&writer);
    const page_data = try std.mem.concat(allocator, u8, &.{ writer.getWritten(), &body });
    defer allocator.free(page_data);

    const info = format.ColumnInfo{
        .element = makeSchemaElem(physical_type),
        .max_def_level = 0,
        .max_rep_level = 0,
        .column_index = 0,
    };
    const meta = format.ColumnMetaData{ .type_ = physical_type, .codec = .uncompressed, .num_values = 1 };
    try std.testing.expectError(error.UnsupportedEncoding, leaf_decoder.decodeLeaf(allocator, page_data, meta, info, .{}));
}

test "V1 decoder: RLE encoding on INT32 returns UnsupportedEncoding" {
    try expectDecoderUnsupportedEncoding(.int32, .rle);
}

test "V1 decoder: RLE encoding on BYTE_ARRAY returns UnsupportedEncoding" {
    try expectDecoderUnsupportedEncoding(.byte_array, .rle);
}

test "V1 decoder: RLE encoding on INT64 returns UnsupportedEncoding" {
    try expectDecoderUnsupportedEncoding(.int64, .rle);
}

test "V1 decoder: RLE encoding on FLOAT returns UnsupportedEncoding" {
    try expectDecoderUnsupportedEncoding(.float, .rle);
}

test "V1 decoder: RLE encoding on DOUBLE returns UnsupportedEncoding" {
    try expectDecoderUnsupportedEncoding(.double, .rle);
}

test "V1 decoder: unknown encoding returns UnsupportedEncoding" {
    try expectDecoderUnsupportedEncoding(.int32, .bit_packed);
}

// =========================================================================
// DynamicWriter: encoding / type validation at addColumn
// =========================================================================

test "DynamicWriter: delta_binary_packed on string column returns UnsupportedEncoding" {
    const allocator = std.testing.allocator;
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    const result = writer.addColumn("bad", TypeInfo.string, .{ .encoding = .delta_binary_packed });
    try std.testing.expectError(error.UnsupportedEncoding, result);
}

test "DynamicWriter: delta_binary_packed on bool column returns UnsupportedEncoding" {
    const allocator = std.testing.allocator;
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    const result = writer.addColumn("bad", TypeInfo.bool_, .{ .encoding = .delta_binary_packed });
    try std.testing.expectError(error.UnsupportedEncoding, result);
}

test "DynamicWriter: delta_binary_packed on float column returns UnsupportedEncoding" {
    const allocator = std.testing.allocator;
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    const result = writer.addColumn("bad", TypeInfo.float_, .{ .encoding = .delta_binary_packed });
    try std.testing.expectError(error.UnsupportedEncoding, result);
}

test "DynamicWriter: byte_stream_split on string column returns UnsupportedEncoding" {
    const allocator = std.testing.allocator;
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    const result = writer.addColumn("bad", TypeInfo.string, .{ .encoding = .byte_stream_split });
    try std.testing.expectError(error.UnsupportedEncoding, result);
}

test "DynamicWriter: byte_stream_split on bool column returns UnsupportedEncoding" {
    const allocator = std.testing.allocator;
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    const result = writer.addColumn("bad", TypeInfo.bool_, .{ .encoding = .byte_stream_split });
    try std.testing.expectError(error.UnsupportedEncoding, result);
}

test "DynamicWriter: RLE on int32 column returns UnsupportedEncoding" {
    const allocator = std.testing.allocator;
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    const result = writer.addColumn("bad", TypeInfo.int32, .{ .encoding = .rle });
    try std.testing.expectError(error.UnsupportedEncoding, result);
}

test "DynamicWriter: RLE on string column returns UnsupportedEncoding" {
    const allocator = std.testing.allocator;
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    const result = writer.addColumn("bad", TypeInfo.string, .{ .encoding = .rle });
    try std.testing.expectError(error.UnsupportedEncoding, result);
}

test "DynamicWriter: bit_packed encoding returns UnsupportedEncoding" {
    const allocator = std.testing.allocator;
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    const result = writer.addColumn("bad", TypeInfo.int32, .{ .encoding = .bit_packed });
    try std.testing.expectError(error.UnsupportedEncoding, result);
}

// =========================================================================
// DynamicWriter: setIntEncoding / setFloatEncoding validation
// =========================================================================

test "DynamicWriter: setIntEncoding with rle returns UnsupportedEncoding" {
    const allocator = std.testing.allocator;
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    const result = writer.setIntEncoding(.rle);
    try std.testing.expectError(error.UnsupportedEncoding, result);
}

test "DynamicWriter: setFloatEncoding with delta_binary_packed returns UnsupportedEncoding" {
    const allocator = std.testing.allocator;
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    const result = writer.setFloatEncoding(.delta_binary_packed);
    try std.testing.expectError(error.UnsupportedEncoding, result);
}

test "DynamicWriter: setFloatEncoding with rle returns UnsupportedEncoding" {
    const allocator = std.testing.allocator;
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    const result = writer.setFloatEncoding(.rle);
    try std.testing.expectError(error.UnsupportedEncoding, result);
}

// =========================================================================
// DynamicWriter: valid encoding / type combinations (sanity checks)
// =========================================================================

test "DynamicWriter: delta_binary_packed on int32 succeeds" {
    const allocator = std.testing.allocator;
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("ok", TypeInfo.int32, .{ .encoding = .delta_binary_packed });
}

test "DynamicWriter: delta_binary_packed on int64 succeeds" {
    const allocator = std.testing.allocator;
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("ok", TypeInfo.int64, .{ .encoding = .delta_binary_packed });
}

test "DynamicWriter: byte_stream_split on float succeeds" {
    const allocator = std.testing.allocator;
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("ok", TypeInfo.float_, .{ .encoding = .byte_stream_split });
}

test "DynamicWriter: byte_stream_split on double succeeds" {
    const allocator = std.testing.allocator;
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("ok", TypeInfo.double_, .{ .encoding = .byte_stream_split });
}

test "DynamicWriter: byte_stream_split on int32 succeeds" {
    const allocator = std.testing.allocator;
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("ok", TypeInfo.int32, .{ .encoding = .byte_stream_split });
}

test "DynamicWriter: RLE on bool succeeds" {
    const allocator = std.testing.allocator;
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("ok", TypeInfo.bool_, .{ .encoding = .rle });
}

test "DynamicWriter: setIntEncoding with delta_binary_packed succeeds" {
    const allocator = std.testing.allocator;
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.setIntEncoding(.delta_binary_packed);
}

test "DynamicWriter: setFloatEncoding with byte_stream_split succeeds" {
    const allocator = std.testing.allocator;
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.setFloatEncoding(.byte_stream_split);
}

// =========================================================================
// DynamicWriter: API misuse
// =========================================================================

test "DynamicWriter: duplicate column name returns DuplicateColumnName" {
    const allocator = std.testing.allocator;
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("x", TypeInfo.int32, .{});
    const result = writer.addColumn("x", TypeInfo.int64, .{});
    try std.testing.expectError(error.DuplicateColumnName, result);
}

test "DynamicWriter: addColumn after begin returns InvalidState" {
    const allocator = std.testing.allocator;
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("x", TypeInfo.int32, .{});
    try writer.begin();

    const result = writer.addColumn("y", TypeInfo.int32, .{});
    try std.testing.expectError(error.InvalidState, result);
}

test "DynamicWriter: addRow before begin returns InvalidState" {
    const allocator = std.testing.allocator;
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("x", TypeInfo.int32, .{});
    const result = writer.addRow();
    try std.testing.expectError(error.InvalidState, result);
}

test "DynamicWriter: close with no columns returns InvalidState" {
    const allocator = std.testing.allocator;
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    const result = writer.close();
    try std.testing.expectError(error.InvalidState, result);
}

// =========================================================================
// Decoder: unsupported encoding / type combinations (other encodings)
// =========================================================================

test "decoder: delta_binary_packed on BYTE_ARRAY returns UnsupportedEncoding" {
    try expectDecoderUnsupportedEncoding(.byte_array, .delta_binary_packed);
}

test "decoder: byte_stream_split on BOOLEAN returns UnsupportedEncoding" {
    try expectDecoderUnsupportedEncoding(.boolean, .byte_stream_split);
}

test "decoder: delta_length_byte_array on INT32 returns UnsupportedEncoding" {
    try expectDecoderUnsupportedEncoding(.int32, .delta_length_byte_array);
}

// =========================================================================
// Reader: malformed file detection
// =========================================================================

test "reader: empty file returns error" {
    const allocator = std.testing.allocator;
    const empty = &[_]u8{};
    const result = parquet.openBufferDynamic(allocator, empty, .{});
    try std.testing.expect(std.meta.isError(result));
}

test "reader: too-short file (no magic) returns error" {
    const allocator = std.testing.allocator;
    const short = "PAR";
    const result = parquet.openBufferDynamic(allocator, short, .{});
    try std.testing.expect(std.meta.isError(result));
}

test "reader: wrong magic bytes returns error" {
    const allocator = std.testing.allocator;
    var bad_magic: [64]u8 = @splat(0);
    bad_magic[0] = 'X';
    bad_magic[1] = 'X';
    bad_magic[2] = 'X';
    bad_magic[3] = '1';
    const result = parquet.openBufferDynamic(allocator, &bad_magic, .{});
    try std.testing.expect(std.meta.isError(result));
}

test "reader: truncated footer returns error" {
    const allocator = std.testing.allocator;
    var buf: [12]u8 = undefined;
    buf[0] = 'P';
    buf[1] = 'A';
    buf[2] = 'R';
    buf[3] = '1';
    buf[8] = 'P';
    buf[9] = 'A';
    buf[10] = 'R';
    buf[11] = '1';
    std.mem.writeInt(i32, buf[4..8], 100, .little);
    const result = parquet.openBufferDynamic(allocator, &buf, .{});
    try std.testing.expect(std.meta.isError(result));
}
