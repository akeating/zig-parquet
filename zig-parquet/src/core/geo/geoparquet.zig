//! GeoParquet metadata generation
//!
//! Generates the "geo" key-value metadata per GeoParquet 1.1 specification.
//! Reference: https://geoparquet.org/releases/v1.1.0/

const std = @import("std");
const format = @import("../format.zig");

/// Information about a geometry column for GeoParquet metadata
pub const GeoColumnInfo = struct {
    /// Column name
    name: []const u8,
    /// Coordinate Reference System (null = OGC:CRS84)
    crs: ?[]const u8 = null,
    /// Whether this is GEOGRAPHY (spherical edges) vs GEOMETRY (planar edges)
    is_geography: bool = false,
    /// Edge interpolation algorithm for GEOGRAPHY
    algorithm: ?format.EdgeInterpolationAlgorithm = null,
    /// Bounding box (null if not computed)
    bbox: ?format.BoundingBox = null,
    /// WKB geometry type codes encountered
    geometry_types: ?[]const i32 = null,
};

// ============================================================================
// JSON Metadata Structures (for serialization)
// ============================================================================

/// GeoParquet metadata root structure
const GeoMetadata = struct {
    version: []const u8 = "1.1.0",
    primary_column: []const u8,
    columns: std.json.ArrayHashMap(ColumnMetadata),
};

/// Per-column metadata
const ColumnMetadata = struct {
    encoding: []const u8 = "WKB",
    edges: ?[]const u8 = null,
    crs: []const u8 = "OGC:CRS84",
    geometry_types: ?[]const []const u8 = null,
    bbox: ?[]const f64 = null,
};

/// Generate GeoParquet "geo" metadata JSON
/// Caller owns returned memory.
pub fn generateMetadata(
    allocator: std.mem.Allocator,
    columns: []const GeoColumnInfo,
    primary_column: ?[]const u8,
) ![]u8 {
    // Build column metadata map
    var col_map: std.StringArrayHashMapUnmanaged(ColumnMetadata) = .empty;
    defer col_map.deinit(allocator);

    // Temporary storage for geometry type strings and bbox arrays
    var type_strings: std.ArrayListUnmanaged([]const u8) = .empty;
    defer type_strings.deinit(allocator);

    var bbox_arrays: std.ArrayListUnmanaged([]f64) = .empty;
    defer {
        for (bbox_arrays.items) |arr| allocator.free(arr);
        bbox_arrays.deinit(allocator);
    }

    var type_slices: std.ArrayListUnmanaged([]const []const u8) = .empty;
    defer {
        for (type_slices.items) |slice| allocator.free(slice);
        type_slices.deinit(allocator);
    }

    for (columns) |col| {
        var col_meta = ColumnMetadata{
            .crs = col.crs orelse "OGC:CRS84",
        };

        // Edges (only for geography)
        if (col.is_geography) {
            col_meta.edges = if (col.algorithm) |a| switch (a) {
                .spherical => "spherical",
                .vincenty => "vincenty",
                .thomas => "thomas",
                .andoyer => "andoyer",
                .karney => "karney",
            } else "spherical";
        }

        // Geometry types - convert WKB codes to strings
        if (col.geometry_types) |types| {
            const start = type_strings.items.len;
            for (types) |t| {
                try type_strings.append(allocator, wkbTypeToString(t));
            }
            const type_slice = try allocator.dupe([]const u8, type_strings.items[start..]);
            try type_slices.append(allocator, type_slice);
            col_meta.geometry_types = type_slice;
        }

        // Bbox
        if (col.bbox) |bbox| {
            const bbox_arr = try allocator.alloc(f64, 4);
            bbox_arr[0] = bbox.xmin;
            bbox_arr[1] = bbox.ymin;
            bbox_arr[2] = bbox.xmax;
            bbox_arr[3] = bbox.ymax;
            try bbox_arrays.append(allocator, bbox_arr);
            col_meta.bbox = bbox_arr;
        }

        try col_map.put(allocator, col.name, col_meta);
    }

    const metadata = GeoMetadata{
        .primary_column = primary_column orelse if (columns.len > 0) columns[0].name else "",
        .columns = .{ .map = col_map },
    };

    // Serialize to JSON
    var out: std.io.Writer.Allocating = .init(allocator);
    errdefer out.deinit();

    try std.json.Stringify.value(metadata, .{}, &out.writer);

    return out.toOwnedSlice();
}

/// Convert WKB geometry type code to GeoParquet type name string
fn wkbTypeToString(wkb_type: i32) []const u8 {
    const base_type = @mod(wkb_type, 1000);
    const dim_code = @divTrunc(wkb_type, 1000);

    return switch (dim_code) {
        0 => switch (base_type) { // XY
            1 => "Point",
            2 => "LineString",
            3 => "Polygon",
            4 => "MultiPoint",
            5 => "MultiLineString",
            6 => "MultiPolygon",
            7 => "GeometryCollection",
            else => "Unknown",
        },
        1 => switch (base_type) { // Z
            1 => "Point Z",
            2 => "LineString Z",
            3 => "Polygon Z",
            4 => "MultiPoint Z",
            5 => "MultiLineString Z",
            6 => "MultiPolygon Z",
            7 => "GeometryCollection Z",
            else => "Unknown Z",
        },
        2 => switch (base_type) { // M
            1 => "Point M",
            2 => "LineString M",
            3 => "Polygon M",
            4 => "MultiPoint M",
            5 => "MultiLineString M",
            6 => "MultiPolygon M",
            7 => "GeometryCollection M",
            else => "Unknown M",
        },
        3 => switch (base_type) { // ZM
            1 => "Point ZM",
            2 => "LineString ZM",
            3 => "Polygon ZM",
            4 => "MultiPoint ZM",
            5 => "MultiLineString ZM",
            6 => "MultiPolygon ZM",
            7 => "GeometryCollection ZM",
            else => "Unknown ZM",
        },
        else => "Unknown",
    };
}

// =============================================================================
// Tests
// =============================================================================

/// Parse generated metadata for testing
fn parseTestMetadata(allocator: std.mem.Allocator, json_bytes: []const u8) !std.json.Parsed(GeoMetadata) {
    return std.json.parseFromSlice(GeoMetadata, allocator, json_bytes, .{});
}

test "generateMetadata single column" {
    const allocator = std.testing.allocator;

    const columns = [_]GeoColumnInfo{
        .{
            .name = "geometry",
            .geometry_types = &.{ 1, 3 },
        },
    };

    const json = try generateMetadata(allocator, &columns, null);
    defer allocator.free(json);

    var parsed = try parseTestMetadata(allocator, json);
    defer parsed.deinit();

    const meta = parsed.value;
    try std.testing.expectEqualStrings("1.1.0", meta.version);
    try std.testing.expectEqualStrings("geometry", meta.primary_column);

    const col = meta.columns.map.get("geometry") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("WKB", col.encoding);
    try std.testing.expectEqualStrings("OGC:CRS84", col.crs);

    const types = col.geometry_types orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 2), types.len);
    try std.testing.expectEqualStrings("Point", types[0]);
    try std.testing.expectEqualStrings("Polygon", types[1]);
}

test "generateMetadata with bbox" {
    const allocator = std.testing.allocator;

    const columns = [_]GeoColumnInfo{
        .{
            .name = "geom",
            .bbox = .{
                .xmin = -180,
                .xmax = 180,
                .ymin = -90,
                .ymax = 90,
            },
        },
    };

    const json = try generateMetadata(allocator, &columns, null);
    defer allocator.free(json);

    var parsed = try parseTestMetadata(allocator, json);
    defer parsed.deinit();

    const col = parsed.value.columns.map.get("geom") orelse return error.TestUnexpectedResult;
    const bbox = col.bbox orelse return error.TestUnexpectedResult;

    try std.testing.expectEqual(@as(usize, 4), bbox.len);
    try std.testing.expectEqual(@as(f64, -180), bbox[0]);
    try std.testing.expectEqual(@as(f64, -90), bbox[1]);
    try std.testing.expectEqual(@as(f64, 180), bbox[2]);
    try std.testing.expectEqual(@as(f64, 90), bbox[3]);
}

test "generateMetadata geography" {
    const allocator = std.testing.allocator;

    const columns = [_]GeoColumnInfo{
        .{
            .name = "geo",
            .is_geography = true,
            .algorithm = .vincenty,
        },
    };

    const json = try generateMetadata(allocator, &columns, null);
    defer allocator.free(json);

    var parsed = try parseTestMetadata(allocator, json);
    defer parsed.deinit();

    const col = parsed.value.columns.map.get("geo") orelse return error.TestUnexpectedResult;
    const edges = col.edges orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("vincenty", edges);
}
