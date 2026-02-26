//! Statistics for column chunks and pages

const std = @import("std");
const safe = @import("../safe.zig");
const thrift = @import("../thrift/mod.zig");

/// Statistics for column chunk or page
pub const Statistics = struct {
    max: ?[]const u8 = null,
    min: ?[]const u8 = null,
    null_count: ?i64 = null,
    distinct_count: ?i64 = null,
    max_value: ?[]const u8 = null,
    min_value: ?[]const u8 = null,

    const Self = @This();

    /// Free allocated memory
    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        if (self.max) |v| allocator.free(v);
        if (self.min) |v| allocator.free(v);
        if (self.max_value) |v| allocator.free(v);
        if (self.min_value) |v| allocator.free(v);
        self.* = .{};
    }

    pub fn parse(allocator: std.mem.Allocator, reader: *thrift.CompactReader) !Self {
        var stats = Self{};
        const saved_field_id = reader.last_field_id;
        reader.resetFieldTracking();

        while (try reader.readFieldHeader()) |field| {
            switch (field.field_id) {
                1 => stats.max = try allocator.dupe(u8, try reader.readBinary()),
                2 => stats.min = try allocator.dupe(u8, try reader.readBinary()),
                3 => stats.null_count = try reader.readI64(),
                4 => stats.distinct_count = try reader.readI64(),
                5 => stats.max_value = try allocator.dupe(u8, try reader.readBinary()),
                6 => stats.min_value = try allocator.dupe(u8, try reader.readBinary()),
                else => try reader.skip(field.field_type),
            }
        }

        reader.last_field_id = saved_field_id;
        return stats;
    }

    /// Serialize to Thrift Compact Protocol
    pub fn serialize(self: *const Self, writer: *thrift.CompactWriter) !void {
        const saved = writer.saveFieldId();
        writer.resetFieldTracking();

        if (self.max) |v| {
            try writer.writeFieldHeader(1, .binary);
            try writer.writeBinary(v);
        }
        if (self.min) |v| {
            try writer.writeFieldHeader(2, .binary);
            try writer.writeBinary(v);
        }
        if (self.null_count) |v| {
            try writer.writeFieldHeader(3, .i64);
            try writer.writeI64(v);
        }
        if (self.distinct_count) |v| {
            try writer.writeFieldHeader(4, .i64);
            try writer.writeI64(v);
        }
        if (self.max_value) |v| {
            try writer.writeFieldHeader(5, .binary);
            try writer.writeBinary(v);
        }
        if (self.min_value) |v| {
            try writer.writeFieldHeader(6, .binary);
            try writer.writeBinary(v);
        }

        try writer.writeStructEnd();
        writer.restoreFieldId(saved);
    }

    // =========================================================================
    // Typed Value Accessors
    // =========================================================================
    // These decode the PLAIN-encoded min/max bytes into typed values.
    // Prefer min_value/max_value (current standard) over min/max (deprecated).

    /// Get the min value bytes (prefers min_value, falls back to min)
    pub fn getMinBytes(self: *const Self) ?[]const u8 {
        return self.min_value orelse self.min;
    }

    /// Get the max value bytes (prefers max_value, falls back to max)
    pub fn getMaxBytes(self: *const Self) ?[]const u8 {
        return self.max_value orelse self.max;
    }

    /// Decode min value as i32 (for INT32 columns)
    pub fn minAsI32(self: *const Self) ?i32 {
        const bytes = self.getMinBytes() orelse return null;
        if (bytes.len < 4) return null;
        return std.mem.readInt(i32, bytes[0..4], .little);
    }

    /// Decode max value as i32 (for INT32 columns)
    pub fn maxAsI32(self: *const Self) ?i32 {
        const bytes = self.getMaxBytes() orelse return null;
        if (bytes.len < 4) return null;
        return std.mem.readInt(i32, bytes[0..4], .little);
    }

    /// Decode min value as i64 (for INT64 columns)
    pub fn minAsI64(self: *const Self) ?i64 {
        const bytes = self.getMinBytes() orelse return null;
        if (bytes.len < 8) return null;
        return std.mem.readInt(i64, bytes[0..8], .little);
    }

    /// Decode max value as i64 (for INT64 columns)
    pub fn maxAsI64(self: *const Self) ?i64 {
        const bytes = self.getMaxBytes() orelse return null;
        if (bytes.len < 8) return null;
        return std.mem.readInt(i64, bytes[0..8], .little);
    }

    /// Decode min value as f32 (for FLOAT columns)
    pub fn minAsF32(self: *const Self) ?f32 {
        const bytes = self.getMinBytes() orelse return null;
        if (bytes.len < 4) return null;
        const bits = std.mem.readInt(u32, bytes[0..4], .little);
        return @bitCast(bits);
    }

    /// Decode max value as f32 (for FLOAT columns)
    pub fn maxAsF32(self: *const Self) ?f32 {
        const bytes = self.getMaxBytes() orelse return null;
        if (bytes.len < 4) return null;
        const bits = std.mem.readInt(u32, bytes[0..4], .little);
        return @bitCast(bits);
    }

    /// Decode min value as f64 (for DOUBLE columns)
    pub fn minAsF64(self: *const Self) ?f64 {
        const bytes = self.getMinBytes() orelse return null;
        if (bytes.len < 8) return null;
        const bits = std.mem.readInt(u64, bytes[0..8], .little);
        return @bitCast(bits);
    }

    /// Decode max value as f64 (for DOUBLE columns)
    pub fn maxAsF64(self: *const Self) ?f64 {
        const bytes = self.getMaxBytes() orelse return null;
        if (bytes.len < 8) return null;
        const bits = std.mem.readInt(u64, bytes[0..8], .little);
        return @bitCast(bits);
    }

    /// Decode min value as bool (for BOOLEAN columns)
    pub fn minAsBool(self: *const Self) ?bool {
        const bytes = self.getMinBytes() orelse return null;
        if (bytes.len < 1) return null;
        return bytes[0] != 0;
    }

    /// Decode max value as bool (for BOOLEAN columns)
    pub fn maxAsBool(self: *const Self) ?bool {
        const bytes = self.getMaxBytes() orelse return null;
        if (bytes.len < 1) return null;
        return bytes[0] != 0;
    }
};

/// Bounding box for geospatial statistics (Thrift struct BoundingBox)
/// Coordinates are in XYZM order where X=longitude/easting, Y=latitude/northing
pub const BoundingBox = struct {
    xmin: f64,
    xmax: f64,
    ymin: f64,
    ymax: f64,
    zmin: ?f64 = null,
    zmax: ?f64 = null,
    mmin: ?f64 = null,
    mmax: ?f64 = null,

    const Self = @This();

    pub fn parse(reader: *thrift.CompactReader) !Self {
        const saved = reader.last_field_id;
        reader.resetFieldTracking();

        var bbox = Self{
            .xmin = 0,
            .xmax = 0,
            .ymin = 0,
            .ymax = 0,
        };

        while (try reader.readFieldHeader()) |field| {
            switch (field.field_id) {
                1 => bbox.xmin = try reader.readDouble(),
                2 => bbox.xmax = try reader.readDouble(),
                3 => bbox.ymin = try reader.readDouble(),
                4 => bbox.ymax = try reader.readDouble(),
                5 => bbox.zmin = try reader.readDouble(),
                6 => bbox.zmax = try reader.readDouble(),
                7 => bbox.mmin = try reader.readDouble(),
                8 => bbox.mmax = try reader.readDouble(),
                else => try reader.skip(field.field_type),
            }
        }

        reader.last_field_id = saved;
        return bbox;
    }

    pub fn serialize(self: *const Self, writer: *thrift.CompactWriter) !void {
        const saved = writer.saveFieldId();
        writer.resetFieldTracking();

        // Field 1: xmin (required)
        try writer.writeFieldHeader(1, .double);
        try writer.writeDouble(self.xmin);

        // Field 2: xmax (required)
        try writer.writeFieldHeader(2, .double);
        try writer.writeDouble(self.xmax);

        // Field 3: ymin (required)
        try writer.writeFieldHeader(3, .double);
        try writer.writeDouble(self.ymin);

        // Field 4: ymax (required)
        try writer.writeFieldHeader(4, .double);
        try writer.writeDouble(self.ymax);

        // Field 5: zmin (optional)
        if (self.zmin) |z| {
            try writer.writeFieldHeader(5, .double);
            try writer.writeDouble(z);
        }

        // Field 6: zmax (optional)
        if (self.zmax) |z| {
            try writer.writeFieldHeader(6, .double);
            try writer.writeDouble(z);
        }

        // Field 7: mmin (optional)
        if (self.mmin) |m| {
            try writer.writeFieldHeader(7, .double);
            try writer.writeDouble(m);
        }

        // Field 8: mmax (optional)
        if (self.mmax) |m| {
            try writer.writeFieldHeader(8, .double);
            try writer.writeDouble(m);
        }

        try writer.writeStructEnd();
        writer.restoreFieldId(saved);
    }
};

/// Geospatial statistics for GEOMETRY and GEOGRAPHY columns (Thrift field 17 in ColumnMetaData)
pub const GeospatialStatistics = struct {
    /// Bounding box of all geospatial instances
    bbox: ?BoundingBox = null,
    /// WKB geometry type codes encountered (e.g., 1=Point, 2=LineString, 1001=PointZ)
    geospatial_types: ?[]i32 = null,

    const Self = @This();

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        if (self.geospatial_types) |types| {
            allocator.free(types);
        }
        self.* = .{};
    }

    pub fn parse(allocator: std.mem.Allocator, reader: *thrift.CompactReader) !Self {
        const saved = reader.last_field_id;
        reader.resetFieldTracking();

        var stats = Self{};

        while (try reader.readFieldHeader()) |field| {
            switch (field.field_id) {
                1 => stats.bbox = try BoundingBox.parse(reader),
                2 => {
                    const list_header = try reader.readListHeader();
                    var types = try allocator.alloc(i32, list_header.size);
                    for (0..list_header.size) |i| {
                        types[i] = try reader.readI32();
                    }
                    stats.geospatial_types = types;
                },
                else => try reader.skip(field.field_type),
            }
        }

        reader.last_field_id = saved;
        return stats;
    }

    pub fn serialize(self: *const Self, writer: *thrift.CompactWriter) !void {
        const saved = writer.saveFieldId();
        writer.resetFieldTracking();

        // Field 1: bbox (optional struct)
        if (self.bbox) |*bbox| {
            try writer.writeFieldHeader(1, .struct_);
            try bbox.serialize(writer);
        }

        // Field 2: geospatial_types (optional list<i32>)
        if (self.geospatial_types) |types| {
            try writer.writeFieldHeader(2, .list);
            try writer.writeListHeader(.i32, safe.castTo(u32, types.len) catch return error.IntegerOverflow);
            for (types) |t| {
                try writer.writeI32(t);
            }
        }

        try writer.writeStructEnd();
        writer.restoreFieldId(saved);
    }
};

// =============================================================================
// Tests
// =============================================================================

test "Statistics decode i32" {
    var stats = Statistics{};
    // Encode 42 as little-endian i32
    const bytes = [_]u8{ 42, 0, 0, 0 };
    stats.min_value = &bytes;
    stats.max_value = &bytes;

    try std.testing.expectEqual(@as(i32, 42), stats.minAsI32().?);
    try std.testing.expectEqual(@as(i32, 42), stats.maxAsI32().?);
}

test "Statistics decode i64" {
    var stats = Statistics{};
    // Encode 1000000 as little-endian i64
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(i64, &bytes, 1000000, .little);
    stats.min_value = &bytes;

    try std.testing.expectEqual(@as(i64, 1000000), stats.minAsI64().?);
}

test "Statistics decode f64" {
    var stats = Statistics{};
    // Encode 3.14 as little-endian f64
    var bytes: [8]u8 = undefined;
    const bits: u64 = @bitCast(@as(f64, 3.14));
    std.mem.writeInt(u64, &bytes, bits, .little);
    stats.min_value = &bytes;

    try std.testing.expectApproxEqAbs(@as(f64, 3.14), stats.minAsF64().?, 0.001);
}

test "Statistics prefers min_value over min" {
    var stats = Statistics{};
    const old_bytes = [_]u8{ 10, 0, 0, 0 }; // 10 as i32
    const new_bytes = [_]u8{ 20, 0, 0, 0 }; // 20 as i32
    stats.min = &old_bytes; // deprecated
    stats.min_value = &new_bytes; // current

    // Should use min_value (20), not min (10)
    try std.testing.expectEqual(@as(i32, 20), stats.minAsI32().?);
}
