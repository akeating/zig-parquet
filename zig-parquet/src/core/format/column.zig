//! Column metadata and column chunk structures

const std = @import("std");
const safe = @import("../safe.zig");
const thrift = @import("../thrift/mod.zig");
const types = @import("types.zig");
const statistics = @import("statistics.zig");

pub const PhysicalType = types.PhysicalType;
pub const Encoding = types.Encoding;
pub const CompressionCodec = types.CompressionCodec;
pub const Statistics = statistics.Statistics;
pub const GeospatialStatistics = statistics.GeospatialStatistics;

/// Column metadata
pub const ColumnMetaData = struct {
    type_: PhysicalType = .int32,
    encodings: []Encoding = &.{},
    path_in_schema: [][]const u8 = &.{},
    codec: CompressionCodec = .uncompressed,
    num_values: i64 = 0,
    total_uncompressed_size: i64 = 0,
    total_compressed_size: i64 = 0,
    data_page_offset: i64 = 0,
    index_page_offset: ?i64 = null,
    dictionary_page_offset: ?i64 = null,
    statistics: ?Statistics = null,
    // Field 17: geospatial_statistics for GEOMETRY/GEOGRAPHY columns
    geospatial_statistics: ?GeospatialStatistics = null,

    const Self = @This();

    pub fn parse(allocator: std.mem.Allocator, reader: *thrift.CompactReader) !Self {
        var meta = Self{};
        const saved_field_id = reader.last_field_id;
        reader.resetFieldTracking();

        while (try reader.readFieldHeader()) |field| {
            switch (field.field_id) {
                1 => meta.type_ = try PhysicalType.fromInt(try reader.readI32()),
                2 => {
                    const list_header = try reader.readListHeader();
                    var encodings = try allocator.alloc(Encoding, list_header.size);
                    for (0..list_header.size) |i| {
                        encodings[i] = try Encoding.fromInt(try reader.readI32());
                    }
                    meta.encodings = encodings;
                },
                3 => {
                    const list_header = try reader.readListHeader();
                    var paths = try allocator.alloc([]const u8, list_header.size);
                    for (0..list_header.size) |i| {
                        paths[i] = try allocator.dupe(u8, try reader.readString());
                    }
                    meta.path_in_schema = paths;
                },
                4 => meta.codec = try CompressionCodec.fromInt(try reader.readI32()),
                5 => meta.num_values = try reader.readI64(),
                6 => meta.total_uncompressed_size = try reader.readI64(),
                7 => meta.total_compressed_size = try reader.readI64(),
                9 => meta.data_page_offset = try reader.readI64(),
                10 => meta.index_page_offset = try reader.readI64(),
                11 => meta.dictionary_page_offset = try reader.readI64(),
                12 => meta.statistics = try Statistics.parse(allocator, reader),
                17 => meta.geospatial_statistics = try GeospatialStatistics.parse(allocator, reader),
                else => try reader.skip(field.field_type),
            }
        }

        reader.last_field_id = saved_field_id;
        return meta;
    }

    /// Serialize to Thrift Compact Protocol
    pub fn serialize(self: *const Self, writer: *thrift.CompactWriter) !void {
        const saved = writer.saveFieldId();
        writer.resetFieldTracking();

        try writer.writeFieldHeader(1, .i32);
        try writer.writeI32(@intFromEnum(self.type_));

        try writer.writeFieldHeader(2, .list);
        try writer.writeListHeader(.i32, safe.castTo(u32, self.encodings.len) catch return error.IntegerOverflow);
        for (self.encodings) |enc| {
            try writer.writeI32(@intFromEnum(enc));
        }

        try writer.writeFieldHeader(3, .list);
        try writer.writeListHeader(.binary, safe.castTo(u32, self.path_in_schema.len) catch return error.IntegerOverflow);
        for (self.path_in_schema) |path| {
            try writer.writeString(path);
        }

        try writer.writeFieldHeader(4, .i32);
        try writer.writeI32(@intFromEnum(self.codec));

        try writer.writeFieldHeader(5, .i64);
        try writer.writeI64(self.num_values);

        try writer.writeFieldHeader(6, .i64);
        try writer.writeI64(self.total_uncompressed_size);

        try writer.writeFieldHeader(7, .i64);
        try writer.writeI64(self.total_compressed_size);

        try writer.writeFieldHeader(9, .i64);
        try writer.writeI64(self.data_page_offset);

        if (self.index_page_offset) |v| {
            try writer.writeFieldHeader(10, .i64);
            try writer.writeI64(v);
        }

        if (self.dictionary_page_offset) |v| {
            try writer.writeFieldHeader(11, .i64);
            try writer.writeI64(v);
        }

        if (self.statistics) |*stats| {
            try writer.writeFieldHeader(12, .struct_);
            try stats.serialize(writer);
        }

        if (self.geospatial_statistics) |*geo_stats| {
            try writer.writeFieldHeader(17, .struct_);
            try geo_stats.serialize(writer);
        }

        try writer.writeStructEnd();
        writer.restoreFieldId(saved);
    }
};

/// Column chunk
pub const ColumnChunk = struct {
    file_path: ?[]const u8 = null,
    file_offset: i64 = 0,
    meta_data: ?ColumnMetaData = null,
    // Page index locators (set after row groups are flushed, before the footer).
    offset_index_offset: ?i64 = null,
    offset_index_length: ?i32 = null,
    column_index_offset: ?i64 = null,
    column_index_length: ?i32 = null,

    const Self = @This();

    pub fn parse(allocator: std.mem.Allocator, reader: *thrift.CompactReader) !Self {
        var chunk = Self{};
        const saved_field_id = reader.last_field_id;
        reader.resetFieldTracking();

        while (try reader.readFieldHeader()) |field| {
            switch (field.field_id) {
                1 => chunk.file_path = try allocator.dupe(u8, try reader.readString()),
                2 => chunk.file_offset = try reader.readI64(),
                3 => chunk.meta_data = try ColumnMetaData.parse(allocator, reader),
                4 => chunk.offset_index_offset = try reader.readI64(),
                5 => chunk.offset_index_length = try reader.readI32(),
                6 => chunk.column_index_offset = try reader.readI64(),
                7 => chunk.column_index_length = try reader.readI32(),
                else => try reader.skip(field.field_type),
            }
        }

        reader.last_field_id = saved_field_id;
        return chunk;
    }

    /// Serialize to Thrift Compact Protocol
    pub fn serialize(self: *const Self, writer: *thrift.CompactWriter) !void {
        const saved = writer.saveFieldId();
        writer.resetFieldTracking();

        if (self.file_path) |fp| {
            try writer.writeFieldHeader(1, .binary);
            try writer.writeString(fp);
        }

        try writer.writeFieldHeader(2, .i64);
        try writer.writeI64(self.file_offset);

        if (self.meta_data) |*md| {
            try writer.writeFieldHeader(3, .struct_);
            try md.serialize(writer);
        }

        if (self.offset_index_offset) |v| {
            try writer.writeFieldHeader(4, .i64);
            try writer.writeI64(v);
        }
        if (self.offset_index_length) |v| {
            try writer.writeFieldHeader(5, .i32);
            try writer.writeI32(v);
        }
        if (self.column_index_offset) |v| {
            try writer.writeFieldHeader(6, .i64);
            try writer.writeI64(v);
        }
        if (self.column_index_length) |v| {
            try writer.writeFieldHeader(7, .i32);
            try writer.writeI32(v);
        }

        try writer.writeStructEnd();
        writer.restoreFieldId(saved);
    }
};
