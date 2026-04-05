//! Row group and key-value metadata structures

const std = @import("std");
const safe = @import("../safe.zig");
const thrift = @import("../thrift/mod.zig");
const column = @import("column.zig");

pub const ColumnChunk = column.ColumnChunk;

/// Row group
pub const RowGroup = struct {
    columns: []ColumnChunk = &.{},
    total_byte_size: i64 = 0,
    num_rows: i64 = 0,
    sorting_columns: ?[]const SortingColumn = null,
    file_offset: ?i64 = null,
    total_compressed_size: ?i64 = null,
    ordinal: ?i16 = null,

    const Self = @This();

    pub fn parse(allocator: std.mem.Allocator, reader: *thrift.CompactReader) !Self {
        var rg = Self{};
        const saved_field_id = reader.last_field_id;
        reader.resetFieldTracking();

        while (try reader.readFieldHeader()) |field| {
            switch (field.field_id) {
                1 => {
                    const list_header = try reader.readListHeader();
                    var columns_list = try allocator.alloc(ColumnChunk, list_header.size);
                    for (0..list_header.size) |i| {
                        columns_list[i] = try ColumnChunk.parse(allocator, reader);
                    }
                    rg.columns = columns_list;
                },
                2 => rg.total_byte_size = try reader.readI64(),
                3 => rg.num_rows = try reader.readI64(),
                4 => {
                    const list_header = try reader.readListHeader();
                    var sorting_list = try allocator.alloc(SortingColumn, list_header.size);
                    for (0..list_header.size) |i| {
                        sorting_list[i] = try SortingColumn.parse(reader);
                    }
                    rg.sorting_columns = sorting_list;
                },
                5 => rg.file_offset = try reader.readI64(),
                6 => rg.total_compressed_size = try reader.readI64(),
                7 => rg.ordinal = try reader.readI16(),
                else => try reader.skip(field.field_type),
            }
        }

        reader.last_field_id = saved_field_id;
        return rg;
    }

    /// Serialize to Thrift Compact Protocol
    pub fn serialize(self: *const Self, writer: *thrift.CompactWriter) !void {
        const saved = writer.saveFieldId();
        writer.resetFieldTracking();

        try writer.writeFieldHeader(1, .list);
        try writer.writeListHeader(.struct_, safe.castTo(u32, self.columns.len) catch return error.IntegerOverflow);
        for (self.columns) |*col| {
            try col.serialize(writer);
        }

        try writer.writeFieldHeader(2, .i64);
        try writer.writeI64(self.total_byte_size);

        try writer.writeFieldHeader(3, .i64);
        try writer.writeI64(self.num_rows);

        if (self.sorting_columns) |sc| {
            try writer.writeFieldHeader(4, .list);
            try writer.writeListHeader(.struct_, safe.castTo(u32, sc.len) catch return error.IntegerOverflow);
            for (sc) |*col| {
                try col.serialize(writer);
            }
        }

        if (self.file_offset) |fo| {
            try writer.writeFieldHeader(5, .i64);
            try writer.writeI64(fo);
        }

        if (self.total_compressed_size) |tcs| {
            try writer.writeFieldHeader(6, .i64);
            try writer.writeI64(tcs);
        }

        if (self.ordinal) |ord| {
            try writer.writeFieldHeader(7, .i16);
            try writer.writeI16(ord);
        }

        try writer.writeStructEnd();
        writer.restoreFieldId(saved);
    }
};

/// Sort order within a RowGroup of a leaf column
pub const SortingColumn = struct {
    column_idx: i32 = 0,
    descending: bool = false,
    nulls_first: bool = false,

    pub fn parse(reader: *thrift.CompactReader) !SortingColumn {
        var sc = SortingColumn{};
        const saved_field_id = reader.last_field_id;
        reader.resetFieldTracking();

        while (try reader.readFieldHeader()) |field| {
            switch (field.field_id) {
                1 => sc.column_idx = try reader.readI32(),
                2 => sc.descending = field.field_type == .bool_true,
                3 => sc.nulls_first = field.field_type == .bool_true,
                else => try reader.skip(field.field_type),
            }
        }

        reader.last_field_id = saved_field_id;
        return sc;
    }

    pub fn serialize(self: *const SortingColumn, writer: *thrift.CompactWriter) !void {
        const saved = writer.saveFieldId();
        writer.resetFieldTracking();

        try writer.writeFieldHeader(1, .i32);
        try writer.writeI32(self.column_idx);

        try writer.writeBoolField(2, self.descending);
        try writer.writeBoolField(3, self.nulls_first);

        try writer.writeStructEnd();
        writer.restoreFieldId(saved);
    }
};

/// Key-value metadata
pub const KeyValue = struct {
    key: []const u8 = "",
    value: ?[]const u8 = null,

    const Self = @This();

    pub fn parse(allocator: std.mem.Allocator, reader: *thrift.CompactReader) !Self {
        var kv = Self{};
        const saved_field_id = reader.last_field_id;
        reader.resetFieldTracking();

        while (try reader.readFieldHeader()) |field| {
            switch (field.field_id) {
                1 => kv.key = try allocator.dupe(u8, try reader.readString()),
                2 => kv.value = try allocator.dupe(u8, try reader.readString()),
                else => try reader.skip(field.field_type),
            }
        }

        reader.last_field_id = saved_field_id;
        return kv;
    }

    /// Serialize to Thrift Compact Protocol
    pub fn serialize(self: *const Self, writer: *thrift.CompactWriter) !void {
        const saved = writer.saveFieldId();
        writer.resetFieldTracking();

        try writer.writeFieldHeader(1, .binary);
        try writer.writeString(self.key);

        if (self.value) |v| {
            try writer.writeFieldHeader(2, .binary);
            try writer.writeString(v);
        }

        try writer.writeStructEnd();
        writer.restoreFieldId(saved);
    }
};
