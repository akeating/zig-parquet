//! Parquet Page Index types.
//!
//! Two Thrift structures written between the last row group and the footer:
//!
//! - `OffsetIndex` — per-page offset/size/first-row-index, always safe to emit.
//! - `ColumnIndex` — per-page min/max/null-count, skipped for columns whose
//!   sort order is not well-defined (e.g. GEOMETRY, GEOGRAPHY, INTERVAL).
//!
//! Locator fields in `ColumnChunk` (offset+length, fields 4-7) point into the
//! dedicated page-index region. This module handles only parse/serialize; the
//! reader/writer side owners live in `page_index_reader.zig` and
//! `page_index_writer.zig`.

const std = @import("std");
const safe = @import("../safe.zig");
const thrift = @import("../thrift/mod.zig");

/// Boundary order for a ColumnIndex's min/max sequences.
pub const BoundaryOrder = enum(i32) {
    unordered = 0,
    ascending = 1,
    descending = 2,

    pub fn fromInt(v: i32) !BoundaryOrder {
        return std.enums.fromInt(BoundaryOrder, v) orelse error.InvalidBoundaryOrder;
    }
};

/// Location of a single page within a column chunk.
pub const PageLocation = struct {
    /// Absolute file offset of the page header.
    offset: i64,
    /// Compressed page size including the Thrift page header.
    compressed_page_size: i32,
    /// Index (within the row group) of the first top-level row in this page.
    first_row_index: i64,

    const Self = @This();

    pub fn parse(reader: *thrift.CompactReader) !Self {
        var loc = Self{ .offset = 0, .compressed_page_size = 0, .first_row_index = 0 };
        const saved = reader.last_field_id;
        reader.resetFieldTracking();

        while (try reader.readFieldHeader()) |field| {
            switch (field.field_id) {
                1 => loc.offset = try reader.readI64(),
                2 => loc.compressed_page_size = try reader.readI32(),
                3 => loc.first_row_index = try reader.readI64(),
                else => try reader.skip(field.field_type),
            }
        }

        reader.last_field_id = saved;
        return loc;
    }

    pub fn serialize(self: *const Self, writer: *thrift.CompactWriter) !void {
        const saved = writer.saveFieldId();
        writer.resetFieldTracking();

        try writer.writeFieldHeader(1, .i64);
        try writer.writeI64(self.offset);

        try writer.writeFieldHeader(2, .i32);
        try writer.writeI32(self.compressed_page_size);

        try writer.writeFieldHeader(3, .i64);
        try writer.writeI64(self.first_row_index);

        try writer.writeStructEnd();
        writer.restoreFieldId(saved);
    }
};

/// Per-column-chunk offset index: `page_locations` is ordered by `offset`.
pub const OffsetIndex = struct {
    page_locations: []PageLocation = &.{},
    /// Optional per-page unencoded byte counts (BYTE_ARRAY columns). Not
    /// emitted by this writer; parsed-and-ignored on read.
    unencoded_byte_array_data_bytes: ?[]i64 = null,

    const Self = @This();

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        if (self.page_locations.len > 0) allocator.free(self.page_locations);
        if (self.unencoded_byte_array_data_bytes) |b| allocator.free(b);
        self.* = .{};
    }

    pub fn parse(allocator: std.mem.Allocator, reader: *thrift.CompactReader) !Self {
        var oi = Self{};
        errdefer oi.deinit(allocator);

        const saved = reader.last_field_id;
        reader.resetFieldTracking();

        while (try reader.readFieldHeader()) |field| {
            switch (field.field_id) {
                1 => {
                    const list_header = try reader.readListHeader();
                    const locs = try allocator.alloc(PageLocation, list_header.size);
                    errdefer allocator.free(locs);
                    for (0..list_header.size) |i| {
                        locs[i] = try PageLocation.parse(reader);
                    }
                    oi.page_locations = locs;
                },
                2 => {
                    const list_header = try reader.readListHeader();
                    const vals = try allocator.alloc(i64, list_header.size);
                    errdefer allocator.free(vals);
                    for (0..list_header.size) |i| {
                        vals[i] = try reader.readI64();
                    }
                    oi.unencoded_byte_array_data_bytes = vals;
                },
                else => try reader.skip(field.field_type),
            }
        }

        reader.last_field_id = saved;
        return oi;
    }

    pub fn serialize(self: *const Self, writer: *thrift.CompactWriter) !void {
        const saved = writer.saveFieldId();
        writer.resetFieldTracking();

        try writer.writeFieldHeader(1, .list);
        try writer.writeListHeader(.struct_, safe.castTo(u32, self.page_locations.len) catch return error.IntegerOverflow);
        for (self.page_locations) |*loc| {
            try loc.serialize(writer);
        }

        if (self.unencoded_byte_array_data_bytes) |vals| {
            try writer.writeFieldHeader(2, .list);
            try writer.writeListHeader(.i64, safe.castTo(u32, vals.len) catch return error.IntegerOverflow);
            for (vals) |v| try writer.writeI64(v);
        }

        try writer.writeStructEnd();
        writer.restoreFieldId(saved);
    }
};

/// Per-column-chunk column index: parallel arrays indexed by page number.
/// `null_pages[i]` is true when every value in page i is null (in which case
/// `min_values[i]` and `max_values[i]` are ignored).
pub const ColumnIndex = struct {
    null_pages: []bool = &.{},
    min_values: [][]const u8 = &.{},
    max_values: [][]const u8 = &.{},
    boundary_order: BoundaryOrder = .unordered,
    null_counts: ?[]i64 = null,
    /// Optional histograms — not emitted by this writer; parsed-and-ignored.
    repetition_level_histograms: ?[]i64 = null,
    definition_level_histograms: ?[]i64 = null,

    const Self = @This();

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        if (self.null_pages.len > 0) allocator.free(self.null_pages);
        for (self.min_values) |v| allocator.free(v);
        if (self.min_values.len > 0) allocator.free(self.min_values);
        for (self.max_values) |v| allocator.free(v);
        if (self.max_values.len > 0) allocator.free(self.max_values);
        if (self.null_counts) |v| allocator.free(v);
        if (self.repetition_level_histograms) |v| allocator.free(v);
        if (self.definition_level_histograms) |v| allocator.free(v);
        self.* = .{};
    }

    pub fn parse(allocator: std.mem.Allocator, reader: *thrift.CompactReader) !Self {
        var ci = Self{};
        errdefer ci.deinit(allocator);

        const saved = reader.last_field_id;
        reader.resetFieldTracking();

        while (try reader.readFieldHeader()) |field| {
            switch (field.field_id) {
                1 => ci.null_pages = try readBoolList(allocator, reader),
                2 => ci.min_values = try readBinaryList(allocator, reader),
                3 => ci.max_values = try readBinaryList(allocator, reader),
                4 => ci.boundary_order = try BoundaryOrder.fromInt(try reader.readI32()),
                5 => ci.null_counts = try readI64List(allocator, reader),
                6 => ci.repetition_level_histograms = try readI64List(allocator, reader),
                7 => ci.definition_level_histograms = try readI64List(allocator, reader),
                else => try reader.skip(field.field_type),
            }
        }

        reader.last_field_id = saved;
        return ci;
    }

    pub fn serialize(self: *const Self, writer: *thrift.CompactWriter) !void {
        const saved = writer.saveFieldId();
        writer.resetFieldTracking();

        try writer.writeFieldHeader(1, .list);
        try writeBoolList(writer, self.null_pages);

        try writer.writeFieldHeader(2, .list);
        try writer.writeListHeader(.binary, safe.castTo(u32, self.min_values.len) catch return error.IntegerOverflow);
        for (self.min_values) |v| try writer.writeBinary(v);

        try writer.writeFieldHeader(3, .list);
        try writer.writeListHeader(.binary, safe.castTo(u32, self.max_values.len) catch return error.IntegerOverflow);
        for (self.max_values) |v| try writer.writeBinary(v);

        try writer.writeFieldHeader(4, .i32);
        try writer.writeI32(@intFromEnum(self.boundary_order));

        if (self.null_counts) |counts| {
            try writer.writeFieldHeader(5, .list);
            try writer.writeListHeader(.i64, safe.castTo(u32, counts.len) catch return error.IntegerOverflow);
            for (counts) |c| try writer.writeI64(c);
        }

        try writer.writeStructEnd();
        writer.restoreFieldId(saved);
    }
};

// =============================================================================
// list<bool> helpers
//
// Thrift Compact encodes `list<bool>` with element-type nibble = 1 or 2, and
// each element as a single byte. parquet-mr / Apache Thrift emit byte values
// 0x01 (true) and 0x02 (false), matching the field-type compact codes. pyarrow
// also accepts 0x00 / 0x01. We accept all of {0x00, 0x01, 0x02} on read and
// emit the parquet-mr convention (0x01 true / 0x02 false) on write.
// =============================================================================

fn readBoolList(allocator: std.mem.Allocator, reader: *thrift.CompactReader) ![]bool {
    const header = try reader.readListHeader();
    const out = try allocator.alloc(bool, header.size);
    errdefer allocator.free(out);
    for (0..header.size) |i| {
        const b = try reader.readByte();
        out[i] = switch (b) {
            0x01 => true,
            0x00, 0x02 => false,
            else => return error.InvalidBoolEncoding,
        };
    }
    return out;
}

fn writeBoolList(writer: *thrift.CompactWriter, values: []const bool) !void {
    // Element type 2 (bool_false) is the convention used by parquet-mr.
    try writer.writeListHeader(.bool_false, safe.castTo(u32, values.len) catch return error.IntegerOverflow);
    for (values) |v| {
        try writer.writeByte(if (v) @as(u8, 0x01) else @as(u8, 0x02));
    }
}

fn readBinaryList(allocator: std.mem.Allocator, reader: *thrift.CompactReader) ![][]const u8 {
    const header = try reader.readListHeader();
    const out = try allocator.alloc([]const u8, header.size);
    var count: usize = 0;
    errdefer {
        for (out[0..count]) |v| allocator.free(v);
        allocator.free(out);
    }
    for (0..header.size) |i| {
        const bytes = try reader.readBinary();
        out[i] = try allocator.dupe(u8, bytes);
        count = i + 1;
    }
    return out;
}

fn readI64List(allocator: std.mem.Allocator, reader: *thrift.CompactReader) ![]i64 {
    const header = try reader.readListHeader();
    const out = try allocator.alloc(i64, header.size);
    errdefer allocator.free(out);
    for (0..header.size) |i| {
        out[i] = try reader.readI64();
    }
    return out;
}

// =============================================================================
// Tests
// =============================================================================

const testing = std.testing;

test "PageLocation round-trip" {
    var writer = thrift.CompactWriter.init(testing.allocator);
    defer writer.deinit();

    const original = PageLocation{
        .offset = 1234,
        .compressed_page_size = 567,
        .first_row_index = 89,
    };
    try original.serialize(&writer);

    var reader = thrift.CompactReader.init(writer.getWritten());
    const parsed = try PageLocation.parse(&reader);

    try testing.expectEqual(original.offset, parsed.offset);
    try testing.expectEqual(original.compressed_page_size, parsed.compressed_page_size);
    try testing.expectEqual(original.first_row_index, parsed.first_row_index);
}

test "OffsetIndex round-trip" {
    var writer = thrift.CompactWriter.init(testing.allocator);
    defer writer.deinit();

    const locs = [_]PageLocation{
        .{ .offset = 100, .compressed_page_size = 50, .first_row_index = 0 },
        .{ .offset = 150, .compressed_page_size = 60, .first_row_index = 10 },
        .{ .offset = 210, .compressed_page_size = 70, .first_row_index = 20 },
    };
    const original = OffsetIndex{
        .page_locations = @constCast(&locs),
    };
    try original.serialize(&writer);

    var reader = thrift.CompactReader.init(writer.getWritten());
    var parsed = try OffsetIndex.parse(testing.allocator, &reader);
    defer parsed.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 3), parsed.page_locations.len);
    for (locs, 0..) |expected, i| {
        try testing.expectEqual(expected.offset, parsed.page_locations[i].offset);
        try testing.expectEqual(expected.compressed_page_size, parsed.page_locations[i].compressed_page_size);
        try testing.expectEqual(expected.first_row_index, parsed.page_locations[i].first_row_index);
    }
    try testing.expect(parsed.unencoded_byte_array_data_bytes == null);
}

test "ColumnIndex round-trip with nulls" {
    var writer = thrift.CompactWriter.init(testing.allocator);
    defer writer.deinit();

    const null_pages = [_]bool{ false, false, true };
    var mins = [_][]const u8{ "aa", "cc", "" };
    var maxs = [_][]const u8{ "bb", "dd", "" };
    const null_counts = [_]i64{ 0, 1, 5 };

    const original = ColumnIndex{
        .null_pages = @constCast(&null_pages),
        .min_values = &mins,
        .max_values = &maxs,
        .boundary_order = .ascending,
        .null_counts = @constCast(&null_counts),
    };
    try original.serialize(&writer);

    var reader = thrift.CompactReader.init(writer.getWritten());
    var parsed = try ColumnIndex.parse(testing.allocator, &reader);
    defer parsed.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 3), parsed.null_pages.len);
    try testing.expectEqualSlices(bool, &null_pages, parsed.null_pages);
    try testing.expectEqual(BoundaryOrder.ascending, parsed.boundary_order);
    try testing.expectEqualStrings("aa", parsed.min_values[0]);
    try testing.expectEqualStrings("dd", parsed.max_values[1]);
    try testing.expectEqualSlices(i64, &null_counts, parsed.null_counts.?);
}

test "ColumnIndex boundary_order enum values" {
    try testing.expectEqual(@as(i32, 0), @intFromEnum(BoundaryOrder.unordered));
    try testing.expectEqual(@as(i32, 1), @intFromEnum(BoundaryOrder.ascending));
    try testing.expectEqual(@as(i32, 2), @intFromEnum(BoundaryOrder.descending));
}

test "ColumnIndex accepts alternate bool encoding (0x00)" {
    // Manually construct bytes that use 0x00 for false (pyarrow-style).
    var writer = thrift.CompactWriter.init(testing.allocator);
    defer writer.deinit();

    // Field 1: list<bool> header, 3 elements, element type nibble 2.
    try writer.writeFieldHeader(1, .list);
    try writer.writeByte(0x32); // size=3, type=bool_false
    try writer.writeByte(0x01); // true
    try writer.writeByte(0x00); // false (pyarrow convention)
    try writer.writeByte(0x02); // false (parquet-mr convention)

    // Field 2: list<binary>, 3 empty strings
    try writer.writeFieldHeader(2, .list);
    try writer.writeListHeader(.binary, 3);
    try writer.writeBinary("");
    try writer.writeBinary("");
    try writer.writeBinary("");

    // Field 3: list<binary>, 3 empty strings
    try writer.writeFieldHeader(3, .list);
    try writer.writeListHeader(.binary, 3);
    try writer.writeBinary("");
    try writer.writeBinary("");
    try writer.writeBinary("");

    // Field 4: boundary_order = 0 (unordered)
    try writer.writeFieldHeader(4, .i32);
    try writer.writeI32(0);

    try writer.writeStructEnd();

    var reader = thrift.CompactReader.init(writer.getWritten());
    var parsed = try ColumnIndex.parse(testing.allocator, &reader);
    defer parsed.deinit(testing.allocator);

    try testing.expectEqualSlices(bool, &[_]bool{ true, false, false }, parsed.null_pages);
}
