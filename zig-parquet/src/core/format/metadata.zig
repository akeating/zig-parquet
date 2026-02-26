//! File metadata - the root structure in the Parquet footer

const std = @import("std");
const safe = @import("../safe.zig");
const thrift = @import("../thrift/mod.zig");
const schema_mod = @import("schema.zig");
const row_group_mod = @import("row_group.zig");

pub const SchemaElement = schema_mod.SchemaElement;
pub const RowGroup = row_group_mod.RowGroup;
pub const KeyValue = row_group_mod.KeyValue;

/// File metadata - the root structure in the footer
pub const FileMetaData = struct {
    version: i32 = 0,
    schema: []SchemaElement = &.{},
    num_rows: i64 = 0,
    row_groups: []RowGroup = &.{},
    key_value_metadata: ?[]KeyValue = null,
    created_by: ?[]const u8 = null,

    const Self = @This();

    pub fn parse(allocator: std.mem.Allocator, reader: *thrift.CompactReader) !Self {
        var meta = Self{};
        reader.resetFieldTracking();

        while (try reader.readFieldHeader()) |field| {
            switch (field.field_id) {
                1 => meta.version = try reader.readI32(),
                2 => {
                    const list_header = try reader.readListHeader();
                    var schema_list = try allocator.alloc(SchemaElement, list_header.size);
                    for (0..list_header.size) |i| {
                        schema_list[i] = try SchemaElement.parse(allocator, reader);
                    }
                    meta.schema = schema_list;
                },
                3 => meta.num_rows = try reader.readI64(),
                4 => {
                    const list_header = try reader.readListHeader();
                    var row_groups_list = try allocator.alloc(RowGroup, list_header.size);
                    for (0..list_header.size) |i| {
                        row_groups_list[i] = try RowGroup.parse(allocator, reader);
                    }
                    meta.row_groups = row_groups_list;
                },
                5 => {
                    const list_header = try reader.readListHeader();
                    var kvs = try allocator.alloc(KeyValue, list_header.size);
                    for (0..list_header.size) |i| {
                        kvs[i] = try KeyValue.parse(allocator, reader);
                    }
                    meta.key_value_metadata = kvs;
                },
                6 => meta.created_by = try allocator.dupe(u8, try reader.readString()),
                else => try reader.skip(field.field_type),
            }
        }

        return meta;
    }

    /// Serialize to Thrift Compact Protocol
    pub fn serialize(self: *const Self, writer: *thrift.CompactWriter) !void {
        writer.resetFieldTracking();

        try writer.writeFieldHeader(1, .i32);
        try writer.writeI32(self.version);

        try writer.writeFieldHeader(2, .list);
        try writer.writeListHeader(.struct_, safe.castTo(u32, self.schema.len) catch return error.IntegerOverflow);
        for (self.schema) |*elem| {
            try elem.serialize(writer);
        }

        try writer.writeFieldHeader(3, .i64);
        try writer.writeI64(self.num_rows);

        try writer.writeFieldHeader(4, .list);
        try writer.writeListHeader(.struct_, safe.castTo(u32, self.row_groups.len) catch return error.IntegerOverflow);
        for (self.row_groups) |*rg| {
            try rg.serialize(writer);
        }

        if (self.key_value_metadata) |kvs| {
            try writer.writeFieldHeader(5, .list);
            try writer.writeListHeader(.struct_, safe.castTo(u32, kvs.len) catch return error.IntegerOverflow);
            for (kvs) |*kv| {
                try kv.serialize(writer);
            }
        }

        if (self.created_by) |cb| {
            try writer.writeFieldHeader(6, .binary);
            try writer.writeString(cb);
        }

        try writer.writeStructEnd();
    }
};
