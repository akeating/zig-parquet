//! Well-Known Binary (WKB) parser for geospatial data
//!
//! Parses ISO WKB to extract geometry types and coordinates for bounding box computation.
//! Based on OGC Simple Feature Access Part 1 (ISO 19125-1).
//!
//! Reference: Arrow C++ implementation in parquet/geospatial/util_internal.cc

const std = @import("std");
const safe = @import("../safe.zig");

/// Dimension variants from WKB type code (type / 1000)
pub const Dimensions = enum(u2) {
    xy = 0, // 2D
    xyz = 1, // 3D with Z
    xym = 2, // 3D with M
    xyzm = 3, // 4D

    pub fn coordCount(self: Dimensions) usize {
        return switch (self) {
            .xy => 2,
            .xyz, .xym => 3,
            .xyzm => 4,
        };
    }
};

/// Base geometry type (type % 1000)
pub const GeometryType = enum(u8) {
    point = 1,
    line_string = 2,
    polygon = 3,
    multi_point = 4,
    multi_line_string = 5,
    multi_polygon = 6,
    geometry_collection = 7,
};

pub const WkbError = error{
    UnexpectedEndOfData,
    InvalidGeometryType,
    InvalidDimensions,
    InvalidEndianByte,
    TrailingData,
};

/// Low-level WKB buffer reader
/// Mirrors Arrow's WKBBuffer class
pub const WkbBuffer = struct {
    data: []const u8,
    pos: usize = 0,

    const Self = @This();

    pub fn init(data: []const u8) Self {
        return .{ .data = data, .pos = 0 };
    }

    pub fn remaining(self: *const Self) usize {
        return self.data.len - self.pos;
    }

    pub fn readU8(self: *Self) WkbError!u8 {
        if (self.pos >= self.data.len) return error.UnexpectedEndOfData;
        const val = self.data[self.pos];
        self.pos += 1;
        return val;
    }

    pub fn readU32(self: *Self, swap: bool) WkbError!u32 {
        if (self.pos + 4 > self.data.len) return error.UnexpectedEndOfData;
        const bytes = safe.slice(self.data, self.pos, 4) catch return error.UnexpectedEndOfData;
        self.pos += 4;

        const native_endian = std.mem.readInt(u32, bytes[0..4], .little);
        if (swap) {
            return @byteSwap(native_endian);
        }
        return native_endian;
    }

    pub fn readF64(self: *Self, swap: bool) WkbError!f64 {
        if (self.pos + 8 > self.data.len) return error.UnexpectedEndOfData;
        const bytes = safe.slice(self.data, self.pos, 8) catch return error.UnexpectedEndOfData;
        self.pos += 8;

        var bits = std.mem.readInt(u64, bytes[0..8], .little);
        if (swap) {
            bits = @byteSwap(bits);
        }
        return @bitCast(bits);
    }

    /// Read coordinate values and call visitor for each
    pub fn readCoords(
        self: *Self,
        n_coords: u32,
        dims: Dimensions,
        swap: bool,
        comptime Visitor: type,
        visitor: *Visitor,
    ) WkbError!void {
        const coord_size = dims.coordCount() * 8;
        const total_bytes = @as(usize, n_coords) * coord_size;

        if (self.pos + total_bytes > self.data.len) return error.UnexpectedEndOfData;

        for (0..n_coords) |_| {
            const x = try self.readF64(swap);
            const y = try self.readF64(swap);

            switch (dims) {
                .xy => visitor.updateXY(x, y),
                .xyz => {
                    const z = try self.readF64(swap);
                    visitor.updateXYZ(x, y, z);
                },
                .xym => {
                    const m = try self.readF64(swap);
                    visitor.updateXYM(x, y, m);
                },
                .xyzm => {
                    const z = try self.readF64(swap);
                    const m = try self.readF64(swap);
                    visitor.updateXYZM(x, y, z, m);
                },
            }
        }
    }
};

/// Accumulates bounding box and geometry types from WKB
/// Mirrors Arrow's WKBGeometryBounder class
pub const WkbGeometryBounder = struct {
    /// Bounding box in XYZM order
    /// Empty bounds: [+Inf, -Inf] per dimension
    min: [4]f64,
    max: [4]f64,

    /// Set of geometry type codes encountered
    geometry_types: std.AutoHashMap(i32, void),

    allocator: std.mem.Allocator,

    const Self = @This();
    const inf = std.math.inf(f64);

    pub fn init(allocator: std.mem.Allocator) Self {
        return .{
            .min = .{ inf, inf, inf, inf },
            .max = .{ -inf, -inf, -inf, -inf },
            .geometry_types = std.AutoHashMap(i32, void).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.geometry_types.deinit();
    }

    pub fn reset(self: *Self) void {
        self.min = .{ inf, inf, inf, inf };
        self.max = .{ -inf, -inf, -inf, -inf };
        self.geometry_types.clearRetainingCapacity();
    }

    /// Parse WKB and accumulate bounds and geometry types
    /// Returns error for invalid WKB (caller should mark stats as invalid)
    pub fn mergeGeometry(self: *Self, wkb_bytes: []const u8) !void {
        var buf = WkbBuffer.init(wkb_bytes);
        try self.mergeGeometryInternal(&buf, true);

        // Verify all bytes consumed
        if (buf.remaining() != 0) {
            return error.TrailingData;
        }
    }

    fn mergeGeometryInternal(self: *Self, buf: *WkbBuffer, record_type: bool) !void {
        // Read endian byte
        const endian_byte = try buf.readU8();
        const swap = switch (endian_byte) {
            0x01 => false, // Little endian (native on most systems)
            0x00 => true, // Big endian
            else => return error.InvalidEndianByte,
        };

        // Read geometry type
        const wkb_type = try buf.readU32(swap);
        const base_type = wkb_type % 1000;
        const dim_code = wkb_type / 1000;

        if (base_type < 1 or base_type > 7) return error.InvalidGeometryType;
        if (dim_code > 3) return error.InvalidDimensions;

        const geometry_type: GeometryType = @enumFromInt(safe.castTo(u8, base_type) catch return error.InvalidGeometryType);
        const dims: Dimensions = @enumFromInt(safe.castTo(u2, dim_code) catch return error.InvalidGeometryType);

        // Record geometry type at top level only (per Arrow convention)
        if (record_type) {
            try self.geometry_types.put(safe.castTo(i32, wkb_type) catch return error.InvalidGeometryType, {});
        }

        // Parse geometry based on type
        switch (geometry_type) {
            .point => {
                // Point: single coordinate
                try buf.readCoords(1, dims, swap, Self, self);
            },
            .line_string => {
                // LineString: n coordinates
                const n_coords = try buf.readU32(swap);
                try buf.readCoords(n_coords, dims, swap, Self, self);
            },
            .polygon => {
                // Polygon: n rings, each with m coordinates
                const n_rings = try buf.readU32(swap);
                for (0..n_rings) |_| {
                    const n_coords = try buf.readU32(swap);
                    try buf.readCoords(n_coords, dims, swap, Self, self);
                }
            },
            .multi_point, .multi_line_string, .multi_polygon, .geometry_collection => {
                // Multi* and GeometryCollection: n child geometries
                const n_geoms = try buf.readU32(swap);
                for (0..n_geoms) |_| {
                    try self.mergeGeometryInternal(buf, false); // Don't record child types
                }
            },
        }
    }

    // Coordinate update methods (called by readCoords)
    // Ignore NaN values per Arrow convention (POINT EMPTY has all NaN)

    fn updateXY(self: *Self, x: f64, y: f64) void {
        if (!std.math.isNan(x)) {
            self.min[0] = @min(self.min[0], x);
            self.max[0] = @max(self.max[0], x);
        }
        if (!std.math.isNan(y)) {
            self.min[1] = @min(self.min[1], y);
            self.max[1] = @max(self.max[1], y);
        }
    }

    fn updateXYZ(self: *Self, x: f64, y: f64, z: f64) void {
        self.updateXY(x, y);
        if (!std.math.isNan(z)) {
            self.min[2] = @min(self.min[2], z);
            self.max[2] = @max(self.max[2], z);
        }
    }

    fn updateXYM(self: *Self, x: f64, y: f64, m: f64) void {
        self.updateXY(x, y);
        if (!std.math.isNan(m)) {
            self.min[3] = @min(self.min[3], m);
            self.max[3] = @max(self.max[3], m);
        }
    }

    fn updateXYZM(self: *Self, x: f64, y: f64, z: f64, m: f64) void {
        self.updateXY(x, y);
        if (!std.math.isNan(z)) {
            self.min[2] = @min(self.min[2], z);
            self.max[2] = @max(self.max[2], z);
        }
        if (!std.math.isNan(m)) {
            self.min[3] = @min(self.min[3], m);
            self.max[3] = @max(self.max[3], m);
        }
    }

    /// Check if a dimension is empty (min > max means no values seen)
    pub fn isEmpty(self: *const Self, dim: usize) bool {
        return std.math.isInf(self.min[dim] - self.max[dim]);
    }

    /// Get sorted list of geometry type codes
    pub fn getGeometryTypes(self: *const Self, allocator: std.mem.Allocator) ![]i32 {
        const count = self.geometry_types.count();
        if (count == 0) return &.{};

        const types = try allocator.alloc(i32, count);
        errdefer allocator.free(types);

        var i: usize = 0;
        var iter = self.geometry_types.keyIterator();
        while (iter.next()) |key| {
            types[i] = key.*;
            i += 1;
        }

        std.mem.sort(i32, types, {}, std.sort.asc(i32));
        return types;
    }
};

// =============================================================================
// Tests
// =============================================================================

test "WkbBuffer read primitives" {
    // Little-endian u32 (42) + f64 (1.5)
    const data = [_]u8{
        42, 0, 0, 0, // u32 = 42
        0, 0, 0, 0, 0, 0, 248, 63, // f64 = 1.5
    };
    var buf = WkbBuffer.init(&data);

    try std.testing.expectEqual(@as(u32, 42), try buf.readU32(false));
    try std.testing.expectApproxEqAbs(@as(f64, 1.5), try buf.readF64(false), 0.0001);
}

test "WkbGeometryBounder parse Point XY" {
    const allocator = std.testing.allocator;

    // WKB Point(1.0, 2.0)
    // Endian (1) + Type (1 = Point XY) + X + Y
    const wkb = [_]u8{
        0x01, // Little endian
        0x01, 0x00, 0x00, 0x00, // Type = 1 (Point XY)
    } ++ @as([8]u8, @bitCast(@as(f64, 1.0))) ++ @as([8]u8, @bitCast(@as(f64, 2.0)));

    var bounder = WkbGeometryBounder.init(allocator);
    defer bounder.deinit();

    try bounder.mergeGeometry(&wkb);

    try std.testing.expectApproxEqAbs(@as(f64, 1.0), bounder.min[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), bounder.max[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), bounder.min[1], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), bounder.max[1], 0.0001);

    // Check geometry types
    const types = try bounder.getGeometryTypes(allocator);
    defer allocator.free(types);
    try std.testing.expectEqual(@as(usize, 1), types.len);
    try std.testing.expectEqual(@as(i32, 1), types[0]); // Point = 1
}

test "WkbGeometryBounder parse LineString XY" {
    const allocator = std.testing.allocator;

    // WKB LineString with 3 points: (0,0), (10,5), (20,10)
    var wkb_data: [1 + 4 + 4 + 3 * 16]u8 = undefined;
    var pos: usize = 0;

    // Endian
    wkb_data[pos] = 0x01;
    pos += 1;

    // Type = 2 (LineString XY)
    std.mem.writeInt(u32, wkb_data[pos..][0..4], 2, .little);
    pos += 4;

    // Num points = 3
    std.mem.writeInt(u32, wkb_data[pos..][0..4], 3, .little);
    pos += 4;

    // Points
    const coords = [_][2]f64{ .{ 0, 0 }, .{ 10, 5 }, .{ 20, 10 } };
    for (coords) |c| {
        @memcpy(wkb_data[pos..][0..8], &@as([8]u8, @bitCast(c[0])));
        pos += 8;
        @memcpy(wkb_data[pos..][0..8], &@as([8]u8, @bitCast(c[1])));
        pos += 8;
    }

    var bounder = WkbGeometryBounder.init(allocator);
    defer bounder.deinit();

    try bounder.mergeGeometry(&wkb_data);

    try std.testing.expectApproxEqAbs(@as(f64, 0.0), bounder.min[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 20.0), bounder.max[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 0.0), bounder.min[1], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 10.0), bounder.max[1], 0.0001);

    const types = try bounder.getGeometryTypes(allocator);
    defer allocator.free(types);
    try std.testing.expectEqual(@as(i32, 2), types[0]); // LineString = 2
}

test "WkbGeometryBounder parse Point XYZ" {
    const allocator = std.testing.allocator;

    // WKB Point Z (1001) with coords (1, 2, 3)
    var wkb_data: [1 + 4 + 24]u8 = undefined;
    wkb_data[0] = 0x01; // Little endian
    std.mem.writeInt(u32, wkb_data[1..][0..4], 1001, .little); // Point Z
    @memcpy(wkb_data[5..][0..8], &@as([8]u8, @bitCast(@as(f64, 1.0))));
    @memcpy(wkb_data[13..][0..8], &@as([8]u8, @bitCast(@as(f64, 2.0))));
    @memcpy(wkb_data[21..][0..8], &@as([8]u8, @bitCast(@as(f64, 3.0))));

    var bounder = WkbGeometryBounder.init(allocator);
    defer bounder.deinit();

    try bounder.mergeGeometry(&wkb_data);

    try std.testing.expectApproxEqAbs(@as(f64, 1.0), bounder.min[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), bounder.min[1], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 3.0), bounder.min[2], 0.0001);
    try std.testing.expect(bounder.isEmpty(3)); // M dimension empty

    const types = try bounder.getGeometryTypes(allocator);
    defer allocator.free(types);
    try std.testing.expectEqual(@as(i32, 1001), types[0]); // Point Z = 1001
}

test "WkbGeometryBounder empty after reset" {
    const allocator = std.testing.allocator;

    var bounder = WkbGeometryBounder.init(allocator);
    defer bounder.deinit();

    // All dimensions should be empty initially
    try std.testing.expect(bounder.isEmpty(0));
    try std.testing.expect(bounder.isEmpty(1));
    try std.testing.expect(bounder.isEmpty(2));
    try std.testing.expect(bounder.isEmpty(3));
}
