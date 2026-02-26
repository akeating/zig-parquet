//! Geospatial type tests
//!
//! Tests for GEOMETRY/GEOGRAPHY logical types, WKB parsing,
//! bounding box computation, and geospatial statistics.

const std = @import("std");
const format = @import("../core/format.zig");
const geo = @import("../core/geo/mod.zig");
const schema = @import("../core/schema.zig");
const column_def = @import("../core/column_def.zig");

// =============================================================================
// Format Type Tests
// =============================================================================

test "EdgeInterpolationAlgorithm fromInt" {
    try std.testing.expectEqual(format.EdgeInterpolationAlgorithm.spherical, try format.EdgeInterpolationAlgorithm.fromInt(0));
    try std.testing.expectEqual(format.EdgeInterpolationAlgorithm.vincenty, try format.EdgeInterpolationAlgorithm.fromInt(1));
    try std.testing.expectEqual(format.EdgeInterpolationAlgorithm.karney, try format.EdgeInterpolationAlgorithm.fromInt(4));
    try std.testing.expectError(error.InvalidEdgeInterpolationAlgorithm, format.EdgeInterpolationAlgorithm.fromInt(5));
}

test "LogicalType geometry" {
    const lt = format.LogicalType{ .geometry = .{ .crs = "EPSG:4326" } };
    try std.testing.expectEqualStrings("GEOMETRY", lt.toString());
}

test "LogicalType geography with algorithm" {
    const lt = format.LogicalType{ .geography = .{
        .crs = "OGC:CRS84",
        .algorithm = .vincenty,
    } };
    try std.testing.expectEqualStrings("GEOGRAPHY", lt.toString());
}

// =============================================================================
// ColumnDef Tests
// =============================================================================

test "ColumnDef geometry factory" {
    const col = column_def.ColumnDef.geometry("geom", true, "EPSG:4326");

    try std.testing.expectEqualStrings("geom", col.name);
    try std.testing.expectEqual(format.PhysicalType.byte_array, col.type_);
    try std.testing.expect(col.optional);

    const lt = col.logical_type.?;
    switch (lt) {
        .geometry => |g| {
            try std.testing.expectEqualStrings("EPSG:4326", g.crs.?);
        },
        else => return error.UnexpectedLogicalType,
    }
}

test "ColumnDef geography factory" {
    const col = column_def.ColumnDef.geography("geog", false, null, .karney);

    try std.testing.expectEqualStrings("geog", col.name);
    try std.testing.expectEqual(format.PhysicalType.byte_array, col.type_);
    try std.testing.expect(!col.optional);

    const lt = col.logical_type.?;
    switch (lt) {
        .geography => |g| {
            try std.testing.expect(g.crs == null);
            try std.testing.expectEqual(format.EdgeInterpolationAlgorithm.karney, g.algorithm.?);
        },
        else => return error.UnexpectedLogicalType,
    }
}

// =============================================================================
// SchemaNode Tests
// =============================================================================

test "SchemaNode geometry" {
    const node = schema.SchemaNode.geometry(null);

    const lt = node.getLogicalType();
    try std.testing.expect(lt != null);
    try std.testing.expectEqualStrings("GEOMETRY", lt.?.toString());
}

test "SchemaNode geography with CRS" {
    const node = schema.SchemaNode.geography("OGC:CRS84", .spherical);

    const lt = node.getLogicalType();
    try std.testing.expect(lt != null);
    try std.testing.expectEqualStrings("GEOGRAPHY", lt.?.toString());
}

// =============================================================================
// GeoParquet Metadata Tests
// =============================================================================

test "GeoParquet generateMetadata" {
    const allocator = std.testing.allocator;

    const columns = [_]geo.GeoColumnInfo{
        .{
            .name = "geometry",
            .crs = "OGC:CRS84",
            .bbox = .{
                .xmin = -122.4,
                .ymin = 37.8,
                .xmax = -122.3,
                .ymax = 37.9,
            },
            .geometry_types = &[_]i32{ 1, 3, 6 },
        },
    };

    const json = try geo.generateMetadata(allocator, &columns, null);
    defer allocator.free(json);

    // Verify JSON structure
    try std.testing.expect(std.mem.indexOf(u8, json, "\"version\":\"1.1.0\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"primary_column\":\"geometry\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"encoding\":\"WKB\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"crs\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"bbox\"") != null);
}

// =============================================================================
// BoundingBoxBuilder Tests (using format.BoundingBox from WKB parsing)
// =============================================================================

test "BoundingBoxBuilder single point" {
    const allocator = std.testing.allocator;

    // WKB Point(10.0, 20.0)
    const wkb_data = [_]u8{
        0x01, // Little endian
        0x01, 0x00, 0x00, 0x00, // Type = 1 (Point XY)
    } ++ @as([8]u8, @bitCast(@as(f64, 10.0))) ++ @as([8]u8, @bitCast(@as(f64, 20.0)));

    var builder = geo.BoundingBoxBuilder.init(allocator);
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

test "BoundingBoxBuilder invalid WKB" {
    const allocator = std.testing.allocator;

    var builder = geo.BoundingBoxBuilder.init(allocator);
    defer builder.deinit();

    // Invalid: too short
    const invalid = [_]u8{ 0x01, 0x01 };
    builder.update(&invalid);

    try std.testing.expect(!builder.is_valid);
    try std.testing.expect(builder.build() == null);
}

test "BoundingBoxBuilder empty returns null" {
    const allocator = std.testing.allocator;

    var builder = geo.BoundingBoxBuilder.init(allocator);
    defer builder.deinit();

    // No updates
    try std.testing.expect(builder.build() == null);
}

// =============================================================================
// WKB Parsing Tests
// =============================================================================

test "WkbGeometryBounder parse Point XY" {
    const allocator = std.testing.allocator;

    // WKB Point(1.0, 2.0)
    const wkb_data = [_]u8{
        0x01, // Little endian
        0x01, 0x00, 0x00, 0x00, // Type = 1 (Point XY)
    } ++ @as([8]u8, @bitCast(@as(f64, 1.0))) ++ @as([8]u8, @bitCast(@as(f64, 2.0)));

    var bounder = geo.WkbGeometryBounder.init(allocator);
    defer bounder.deinit();

    try bounder.mergeGeometry(&wkb_data);

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

    var bounder = geo.WkbGeometryBounder.init(allocator);
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

// =============================================================================
// Writer Bbox Tracking Tests
// =============================================================================

const parquet = @import("../lib.zig");

test "Writer tracks bbox for geometry column" {
    const allocator = std.testing.allocator;

    // Create a geometry column
    const columns = [_]column_def.ColumnDef{
        column_def.ColumnDef.geometry("geom", true, "EPSG:4326"),
    };

    // Initialize writer to buffer
    var writer = try parquet.writeToBuffer(allocator, &columns);
    defer writer.deinit();

    // Create WKB Point at (10.0, 20.0)
    var wkb_point1: [21]u8 = undefined;
    wkb_point1[0] = 0x01; // Little endian
    std.mem.writeInt(u32, wkb_point1[1..5], 1, .little); // Point type
    @memcpy(wkb_point1[5..13], &@as([8]u8, @bitCast(@as(f64, 10.0))));
    @memcpy(wkb_point1[13..21], &@as([8]u8, @bitCast(@as(f64, 20.0))));

    // Create WKB Point at (100.0, 50.0)
    var wkb_point2: [21]u8 = undefined;
    wkb_point2[0] = 0x01;
    std.mem.writeInt(u32, wkb_point2[1..5], 1, .little);
    @memcpy(wkb_point2[5..13], &@as([8]u8, @bitCast(@as(f64, 100.0))));
    @memcpy(wkb_point2[13..21], &@as([8]u8, @bitCast(@as(f64, 50.0))));

    // Write geometry column with Optional values
    const values = [_]parquet.Optional([]const u8){
        .{ .value = &wkb_point1 },
        .{ .value = &wkb_point2 },
        .null_value,
    };
    try writer.writeColumnOptional([]const u8, 0, &values);

    // Close writer (this generates the GeoParquet metadata)
    try writer.close();

    // Get the output buffer
    const data = try writer.toOwnedSlice();
    defer allocator.free(data);

    // The GeoParquet metadata should include bbox
    // Check that the output contains "bbox" in the key-value metadata
    const has_bbox = std.mem.indexOf(u8, data, "bbox") != null;
    try std.testing.expect(has_bbox);
}
