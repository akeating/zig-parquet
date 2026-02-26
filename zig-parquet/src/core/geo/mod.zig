//! Geospatial utilities for Parquet
//!
//! This module provides WKB parsing, bounding box computation, and GeoParquet metadata
//! generation for GEOMETRY and GEOGRAPHY logical types.

pub const wkb = @import("wkb.zig");
pub const bbox = @import("bbox.zig");
pub const geoparquet = @import("geoparquet.zig");

// Re-export commonly used types
pub const WkbBuffer = wkb.WkbBuffer;
pub const WkbGeometryBounder = wkb.WkbGeometryBounder;
pub const Dimensions = wkb.Dimensions;
pub const WkbGeometryType = wkb.GeometryType;
pub const BoundingBoxBuilder = bbox.BoundingBoxBuilder;
pub const GeoColumnInfo = geoparquet.GeoColumnInfo;
pub const generateMetadata = geoparquet.generateMetadata;

test {
    _ = wkb;
    _ = bbox;
    _ = geoparquet;
}
