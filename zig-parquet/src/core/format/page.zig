//! Page header structures

const std = @import("std");
const thrift = @import("../thrift/mod.zig");
const types = @import("types.zig");
const statistics = @import("statistics.zig");

pub const Encoding = types.Encoding;
pub const PageType = types.PageType;
pub const Statistics = statistics.Statistics;

/// Data page header
pub const DataPageHeader = struct {
    num_values: i32 = 0,
    encoding: Encoding = .plain,
    definition_level_encoding: Encoding = .rle,
    repetition_level_encoding: Encoding = .rle,
    statistics: ?Statistics = null,

    const Self = @This();

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        if (self.statistics) |*stats| stats.deinit(allocator);
        self.* = .{};
    }

    pub fn parse(allocator: std.mem.Allocator, reader: *thrift.CompactReader) !Self {
        var header = Self{};
        const saved_field_id = reader.last_field_id;
        reader.resetFieldTracking();

        while (try reader.readFieldHeader()) |field| {
            switch (field.field_id) {
                1 => header.num_values = try reader.readI32(),
                2 => header.encoding = try Encoding.fromInt(try reader.readI32()),
                3 => header.definition_level_encoding = try Encoding.fromInt(try reader.readI32()),
                4 => header.repetition_level_encoding = try Encoding.fromInt(try reader.readI32()),
                5 => header.statistics = try Statistics.parse(allocator, reader),
                else => try reader.skip(field.field_type),
            }
        }

        reader.last_field_id = saved_field_id;
        return header;
    }

    /// Serialize to Thrift Compact Protocol
    pub fn serialize(self: *const Self, writer: *thrift.CompactWriter) !void {
        const saved = writer.saveFieldId();
        writer.resetFieldTracking();

        try writer.writeFieldHeader(1, .i32);
        try writer.writeI32(self.num_values);

        try writer.writeFieldHeader(2, .i32);
        try writer.writeI32(@intFromEnum(self.encoding));

        try writer.writeFieldHeader(3, .i32);
        try writer.writeI32(@intFromEnum(self.definition_level_encoding));

        try writer.writeFieldHeader(4, .i32);
        try writer.writeI32(@intFromEnum(self.repetition_level_encoding));

        if (self.statistics) |*stats| {
            try writer.writeFieldHeader(5, .struct_);
            try stats.serialize(writer);
        }

        try writer.writeStructEnd();
        writer.restoreFieldId(saved);
    }
};

/// Data page header V2 (newer format with better compression)
/// In V2, definition and repetition levels are stored uncompressed before the values
pub const DataPageHeaderV2 = struct {
    num_values: i32 = 0,
    num_nulls: i32 = 0,
    num_rows: i32 = 0,
    encoding: Encoding = .plain,
    definition_levels_byte_length: i32 = 0,
    repetition_levels_byte_length: i32 = 0,
    is_compressed: bool = true, // default true per spec
    statistics: ?Statistics = null,

    const Self = @This();

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        if (self.statistics) |*stats| stats.deinit(allocator);
        self.* = .{};
    }

    pub fn parse(allocator: std.mem.Allocator, reader: *thrift.CompactReader) !Self {
        var header = Self{};
        const saved_field_id = reader.last_field_id;
        reader.resetFieldTracking();

        while (try reader.readFieldHeader()) |field| {
            switch (field.field_id) {
                1 => header.num_values = try reader.readI32(),
                2 => header.num_nulls = try reader.readI32(),
                3 => header.num_rows = try reader.readI32(),
                4 => header.encoding = try Encoding.fromInt(try reader.readI32()),
                5 => header.definition_levels_byte_length = try reader.readI32(),
                6 => header.repetition_levels_byte_length = try reader.readI32(),
                7 => header.is_compressed = field.field_type == .bool_true,
                8 => header.statistics = try Statistics.parse(allocator, reader),
                else => try reader.skip(field.field_type),
            }
        }

        reader.last_field_id = saved_field_id;
        return header;
    }
};

/// Dictionary page header
pub const DictionaryPageHeader = struct {
    num_values: i32 = 0,
    encoding: Encoding = .plain,
    is_sorted: bool = false,

    const Self = @This();

    pub fn parse(_: std.mem.Allocator, reader: *thrift.CompactReader) !Self {
        var header = Self{};
        const saved_field_id = reader.last_field_id;
        reader.resetFieldTracking();

        while (try reader.readFieldHeader()) |field| {
            switch (field.field_id) {
                1 => header.num_values = try reader.readI32(),
                2 => header.encoding = try Encoding.fromInt(try reader.readI32()),
                3 => header.is_sorted = field.field_type == .bool_true,
                else => try reader.skip(field.field_type),
            }
        }

        reader.last_field_id = saved_field_id;
        return header;
    }

    /// Serialize to Thrift Compact Protocol
    pub fn serialize(self: *const Self, writer: *thrift.CompactWriter) !void {
        const saved = writer.saveFieldId();
        writer.resetFieldTracking();

        try writer.writeFieldHeader(1, .i32);
        try writer.writeI32(self.num_values);

        try writer.writeFieldHeader(2, .i32);
        try writer.writeI32(@intFromEnum(self.encoding));

        if (self.is_sorted) {
            try writer.writeBoolField(3, true);
        }

        try writer.writeStructEnd();
        writer.restoreFieldId(saved);
    }
};

/// Page header
pub const PageHeader = struct {
    type_: PageType = .data_page,
    uncompressed_page_size: i32 = 0,
    compressed_page_size: i32 = 0,
    crc: ?i32 = null,
    data_page_header: ?DataPageHeader = null,
    dictionary_page_header: ?DictionaryPageHeader = null,
    data_page_header_v2: ?DataPageHeaderV2 = null,

    const Self = @This();

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        if (self.data_page_header) |*dph| dph.deinit(allocator);
        if (self.data_page_header_v2) |*dph| dph.deinit(allocator);
        self.* = .{};
    }

    pub fn parse(allocator: std.mem.Allocator, reader: *thrift.CompactReader) !Self {
        var header = Self{};
        const saved_field_id = reader.last_field_id;
        reader.resetFieldTracking();

        while (try reader.readFieldHeader()) |field| {
            switch (field.field_id) {
                1 => header.type_ = try PageType.fromInt(try reader.readI32()),
                2 => header.uncompressed_page_size = try reader.readI32(),
                3 => header.compressed_page_size = try reader.readI32(),
                4 => header.crc = try reader.readI32(),
                5 => header.data_page_header = try DataPageHeader.parse(allocator, reader),
                7 => header.dictionary_page_header = try DictionaryPageHeader.parse(allocator, reader),
                8 => header.data_page_header_v2 = try DataPageHeaderV2.parse(allocator, reader),
                else => try reader.skip(field.field_type),
            }
        }

        reader.last_field_id = saved_field_id;
        return header;
    }

    /// Serialize to Thrift Compact Protocol
    pub fn serialize(self: *const Self, writer: *thrift.CompactWriter) !void {
        const saved = writer.saveFieldId();
        writer.resetFieldTracking();

        try writer.writeFieldHeader(1, .i32);
        try writer.writeI32(@intFromEnum(self.type_));

        try writer.writeFieldHeader(2, .i32);
        try writer.writeI32(self.uncompressed_page_size);

        try writer.writeFieldHeader(3, .i32);
        try writer.writeI32(self.compressed_page_size);

        if (self.crc) |c| {
            try writer.writeFieldHeader(4, .i32);
            try writer.writeI32(c);
        }

        if (self.data_page_header) |*dph| {
            try writer.writeFieldHeader(5, .struct_);
            try dph.serialize(writer);
        }

        if (self.dictionary_page_header) |*dph| {
            try writer.writeFieldHeader(7, .struct_);
            try dph.serialize(writer);
        }

        try writer.writeStructEnd();
        writer.restoreFieldId(saved);
    }
};
