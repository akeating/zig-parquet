//! Bounding box computation for geospatial data
//!
//! Provides BoundingBoxBuilder for accumulating bounding boxes from WKB geometries.

const std = @import("std");
const wkb = @import("wkb.zig");
const format = @import("../format.zig");

/// Builder for computing bounding boxes from WKB geometries
/// Wraps WkbGeometryBounder and converts to format.BoundingBox
pub const BoundingBoxBuilder = struct {
    bounder: wkb.WkbGeometryBounder,
    is_valid: bool = true,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return .{
            .bounder = wkb.WkbGeometryBounder.init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.bounder.deinit();
    }

    pub fn reset(self: *Self) void {
        self.bounder.reset();
        self.is_valid = true;
    }

    /// Update with a WKB geometry. Invalid WKB marks builder as invalid.
    pub fn update(self: *Self, wkb_bytes: []const u8) void {
        if (!self.is_valid) return;

        self.bounder.mergeGeometry(wkb_bytes) catch {
            self.is_valid = false;
        };
    }

    /// Update with multiple WKB geometries
    pub fn updateBatch(self: *Self, wkb_values: []const []const u8) void {
        for (wkb_values) |wkb_bytes| {
            self.update(wkb_bytes);
            if (!self.is_valid) break;
        }
    }

    /// Build the bounding box. Returns null if invalid or no valid bounds.
    pub fn build(self: *const Self) ?format.BoundingBox {
        if (!self.is_valid) return null;

        // Per Parquet spec, XY bounds are required; Z/M are optional
        const x_empty = self.bounder.isEmpty(0);
        const y_empty = self.bounder.isEmpty(1);

        // If X or Y is empty, we can't produce a valid bbox
        if (x_empty or y_empty) return null;

        var bbox = format.BoundingBox{
            .xmin = self.bounder.min[0],
            .xmax = self.bounder.max[0],
            .ymin = self.bounder.min[1],
            .ymax = self.bounder.max[1],
        };

        // Add Z bounds if present
        if (!self.bounder.isEmpty(2)) {
            bbox.zmin = self.bounder.min[2];
            bbox.zmax = self.bounder.max[2];
        }

        // Add M bounds if present
        if (!self.bounder.isEmpty(3)) {
            bbox.mmin = self.bounder.min[3];
            bbox.mmax = self.bounder.max[3];
        }

        return bbox;
    }

    /// Get sorted list of geometry type codes
    pub fn getGeometryTypes(self: *const Self, allocator: std.mem.Allocator) ![]i32 {
        if (!self.is_valid) return &.{};
        return self.bounder.getGeometryTypes(allocator);
    }

    /// Build GeospatialStatistics
    pub fn buildStatistics(self: *Self, allocator: std.mem.Allocator) !?format.GeospatialStatistics {
        if (!self.is_valid) return null;

        const bbox = self.build();
        const types = try self.getGeometryTypes(allocator);

        return format.GeospatialStatistics{
            .bbox = bbox,
            .geospatial_types = if (types.len > 0) types else null,
        };
    }
};

// =============================================================================
// Tests
// =============================================================================

test "BoundingBoxBuilder single point" {
    const allocator = std.testing.allocator;

    // WKB Point(10.0, 20.0)
    const wkb_data = [_]u8{
        0x01, // Little endian
        0x01, 0x00, 0x00, 0x00, // Type = 1 (Point XY)
    } ++ @as([8]u8, @bitCast(@as(f64, 10.0))) ++ @as([8]u8, @bitCast(@as(f64, 20.0)));

    var builder = BoundingBoxBuilder.init(allocator);
    defer builder.deinit();

    builder.update(&wkb_data);

    const bbox = builder.build();
    try std.testing.expect(bbox != null);
    try std.testing.expectApproxEqAbs(@as(f64, 10.0), bbox.?.xmin, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 10.0), bbox.?.xmax, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 20.0), bbox.?.ymin, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 20.0), bbox.?.ymax, 0.0001);
    try std.testing.expect(bbox.?.zmin == null);
    try std.testing.expect(bbox.?.zmax == null);
}

test "BoundingBoxBuilder multiple points" {
    const allocator = std.testing.allocator;

    var builder = BoundingBoxBuilder.init(allocator);
    defer builder.deinit();

    // Point 1: (0, 0)
    const p1 = [_]u8{0x01} ++ [_]u8{ 0x01, 0, 0, 0 } ++ @as([8]u8, @bitCast(@as(f64, 0.0))) ++ @as([8]u8, @bitCast(@as(f64, 0.0)));
    builder.update(&p1);

    // Point 2: (100, 50)
    const p2 = [_]u8{0x01} ++ [_]u8{ 0x01, 0, 0, 0 } ++ @as([8]u8, @bitCast(@as(f64, 100.0))) ++ @as([8]u8, @bitCast(@as(f64, 50.0)));
    builder.update(&p2);

    const bbox = builder.build();
    try std.testing.expect(bbox != null);
    try std.testing.expectApproxEqAbs(@as(f64, 0.0), bbox.?.xmin, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 100.0), bbox.?.xmax, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 0.0), bbox.?.ymin, 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 50.0), bbox.?.ymax, 0.0001);
}

test "BoundingBoxBuilder invalid WKB" {
    const allocator = std.testing.allocator;

    var builder = BoundingBoxBuilder.init(allocator);
    defer builder.deinit();

    // Invalid: too short
    const invalid = [_]u8{ 0x01, 0x01 };
    builder.update(&invalid);

    try std.testing.expect(!builder.is_valid);
    try std.testing.expect(builder.build() == null);
}

test "BoundingBoxBuilder empty returns null" {
    const allocator = std.testing.allocator;

    var builder = BoundingBoxBuilder.init(allocator);
    defer builder.deinit();

    // No updates
    try std.testing.expect(builder.build() == null);
}
