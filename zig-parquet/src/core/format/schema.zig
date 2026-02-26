//! Schema element for Parquet files

const std = @import("std");
const thrift = @import("../thrift/mod.zig");
const types = @import("types.zig");
const logical_types = @import("logical_types.zig");

pub const PhysicalType = types.PhysicalType;
pub const RepetitionType = types.RepetitionType;
pub const LogicalType = logical_types.LogicalType;

/// Schema element - represents a node in the schema tree
pub const SchemaElement = struct {
    type_: ?PhysicalType = null, // Only set for leaf nodes
    type_length: ?i32 = null, // For FIXED_LEN_BYTE_ARRAY
    repetition_type: ?RepetitionType = null,
    name: []const u8 = "",
    num_children: ?i32 = null, // Only set for group nodes
    converted_type: ?i32 = null, // Deprecated, use logical_type
    scale: ?i32 = null,
    precision: ?i32 = null,
    field_id: ?i32 = null,
    logical_type: ?LogicalType = null, // Field 10

    const Self = @This();

    pub fn parse(allocator: std.mem.Allocator, reader: *thrift.CompactReader) !Self {
        var elem = Self{};
        const saved_field_id = reader.last_field_id;
        reader.resetFieldTracking();

        while (try reader.readFieldHeader()) |field| {
            switch (field.field_id) {
                1 => { // type
                    const v = try reader.readI32();
                    elem.type_ = try PhysicalType.fromInt(v);
                },
                2 => elem.type_length = try reader.readI32(), // type_length
                3 => { // repetition_type
                    const v = try reader.readI32();
                    elem.repetition_type = try RepetitionType.fromInt(v);
                },
                4 => elem.name = try allocator.dupe(u8, try reader.readString()), // name
                5 => elem.num_children = try reader.readI32(), // num_children
                6 => elem.converted_type = try reader.readI32(), // converted_type
                7 => elem.scale = try reader.readI32(), // scale
                8 => elem.precision = try reader.readI32(), // precision
                9 => elem.field_id = try reader.readI32(), // field_id
                10 => elem.logical_type = try LogicalType.parse(allocator, reader), // logical_type
                else => try reader.skip(field.field_type),
            }
        }

        reader.last_field_id = saved_field_id;
        return elem;
    }

    /// Serialize to Thrift Compact Protocol
    pub fn serialize(self: *const Self, writer: *thrift.CompactWriter) !void {
        const saved = writer.saveFieldId();
        writer.resetFieldTracking();

        if (self.type_) |t| {
            try writer.writeFieldHeader(1, .i32);
            try writer.writeI32(@intFromEnum(t));
        }

        if (self.type_length) |len| {
            try writer.writeFieldHeader(2, .i32);
            try writer.writeI32(len);
        }

        if (self.repetition_type) |rt| {
            try writer.writeFieldHeader(3, .i32);
            try writer.writeI32(@intFromEnum(rt));
        }

        try writer.writeFieldHeader(4, .binary);
        try writer.writeString(self.name);

        if (self.num_children) |nc| {
            try writer.writeFieldHeader(5, .i32);
            try writer.writeI32(nc);
        }

        if (self.converted_type) |ct| {
            try writer.writeFieldHeader(6, .i32);
            try writer.writeI32(ct);
        }

        if (self.scale) |s| {
            try writer.writeFieldHeader(7, .i32);
            try writer.writeI32(s);
        }

        if (self.precision) |p| {
            try writer.writeFieldHeader(8, .i32);
            try writer.writeI32(p);
        }

        if (self.field_id) |fid| {
            try writer.writeFieldHeader(9, .i32);
            try writer.writeI32(fid);
        }

        if (self.logical_type) |*lt| {
            try writer.writeFieldHeader(10, .struct_);
            try lt.serialize(writer);
        }

        try writer.writeStructEnd();
        writer.restoreFieldId(saved);
    }
};
