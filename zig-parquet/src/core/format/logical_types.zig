//! Logical Types for Parquet
//!
//! Logical types provide semantic meaning to physical types.
//! For example, a TIMESTAMP is an INT64 with time semantics.

const std = @import("std");
const safe = @import("../safe.zig");
const thrift = @import("../thrift/mod.zig");

/// Edge interpolation algorithm for Geography logical type
/// Thrift enum EdgeInterpolationAlgorithm
pub const EdgeInterpolationAlgorithm = enum(u8) {
    spherical = 0,
    vincenty = 1,
    thomas = 2,
    andoyer = 3,
    karney = 4,

    pub fn fromInt(value: i32) !EdgeInterpolationAlgorithm {
        return switch (value) {
            0 => .spherical,
            1 => .vincenty,
            2 => .thomas,
            3 => .andoyer,
            4 => .karney,
            else => error.InvalidEdgeInterpolationAlgorithm,
        };
    }
};

/// Geometry logical type annotation (Thrift field 17 in LogicalType)
/// Geospatial features in WKB format with linear/planar edge interpolation
pub const GeometryType = struct {
    /// Coordinate Reference System identifier. Default is "OGC:CRS84"
    crs: ?[]const u8 = null,
};

/// Geography logical type annotation (Thrift field 18 in LogicalType)
/// Geospatial features in WKB format with explicit edge interpolation algorithm
pub const GeographyType = struct {
    /// Coordinate Reference System identifier. Default is "OGC:CRS84"
    crs: ?[]const u8 = null,
    /// Edge interpolation algorithm. Default is spherical
    algorithm: ?EdgeInterpolationAlgorithm = null,
};

/// Time units for TIMESTAMP and TIME logical types
pub const TimeUnit = enum {
    millis,
    micros,
    nanos,

    pub fn thriftFieldId(self: TimeUnit) u8 {
        return switch (self) {
            .millis => 1,
            .micros => 2,
            .nanos => 3,
        };
    }
};

/// Timestamp logical type - INT64 with time semantics
pub const TimestampType = struct {
    is_adjusted_to_utc: bool,
    unit: TimeUnit,
};

/// Time logical type - INT32 (millis) or INT64 (micros/nanos)
pub const TimeType = struct {
    is_adjusted_to_utc: bool,
    unit: TimeUnit,
};

/// Decimal logical type - requires precision and scale
/// Physical type: INT32 (precision <= 9), INT64 (precision <= 18), or FIXED_LEN_BYTE_ARRAY
pub const DecimalType = struct {
    precision: i32, // Total digits
    scale: i32, // Digits after decimal point
};

/// Integer logical type - signed/unsigned with bit width
/// Physical type: INT32 (8/16/32 bit) or INT64 (64 bit)
pub const IntType = struct {
    bit_width: i8, // 8, 16, 32, or 64
    is_signed: bool,
};

/// Logical type union - field 10 in SchemaElement
/// Per Parquet spec, this is a Thrift union with specific field IDs
pub const LogicalType = union(enum) {
    string, // Field 1: UTF8 string (empty struct)
    enum_, // Field 4: Enum (empty struct)
    decimal: DecimalType, // Field 5: Decimal with precision/scale
    date, // Field 6: Days since epoch (empty struct)
    time: TimeType, // Field 7
    timestamp: TimestampType, // Field 8
    // Field 9 is reserved for Interval
    int: IntType, // Field 10: Integer with bit width and signedness
    json, // Field 12: JSON (empty struct)
    bson, // Field 13: BSON (empty struct)
    uuid, // Field 14: UUID (empty struct)
    float16, // Field 15: IEEE 754 half-precision float (empty struct)
    // Field 16: VARIANT (not implemented)
    geometry: GeometryType, // Field 17: Geometry with WKB encoding
    geography: GeographyType, // Field 18: Geography with WKB encoding

    const Self = @This();

    /// Parse LogicalType from Thrift Compact Protocol
    pub fn parse(allocator: std.mem.Allocator, reader: *thrift.CompactReader) !?Self {
        const saved_field_id = reader.last_field_id;
        reader.resetFieldTracking();

        var result: ?Self = null;

        while (try reader.readFieldHeader()) |field| {
            switch (field.field_id) {
                1 => {
                    // STRING - empty struct
                    try reader.skip(.struct_);
                    result = .string;
                },
                4 => {
                    // ENUM - empty struct
                    try reader.skip(.struct_);
                    result = .enum_;
                },
                5 => {
                    // DECIMAL - DecimalType struct
                    const decimal_type = try parseDecimalType(reader);
                    result = .{ .decimal = decimal_type };
                },
                6 => {
                    // DATE - empty struct
                    try reader.skip(.struct_);
                    result = .date;
                },
                7 => {
                    // TIME - TimeType struct
                    const time_type = try parseTimeType(reader);
                    result = .{ .time = time_type };
                },
                8 => {
                    // TIMESTAMP - TimestampType struct
                    const ts_type = try parseTimestampType(reader);
                    result = .{ .timestamp = ts_type };
                },
                // Field 9 is reserved for Interval
                10 => {
                    // INT - IntType struct
                    const int_type = try parseIntType(reader);
                    result = .{ .int = int_type };
                },
                12 => {
                    // JSON - empty struct
                    try reader.skip(.struct_);
                    result = .json;
                },
                13 => {
                    // BSON - empty struct
                    try reader.skip(.struct_);
                    result = .bson;
                },
                14 => {
                    // UUID - empty struct
                    try reader.skip(.struct_);
                    result = .uuid;
                },
                15 => {
                    // FLOAT16 - empty struct
                    try reader.skip(.struct_);
                    result = .float16;
                },
                17 => {
                    // GEOMETRY - GeometryType struct
                    const geo_type = try parseGeometryType(allocator, reader);
                    result = .{ .geometry = geo_type };
                },
                18 => {
                    // GEOGRAPHY - GeographyType struct
                    const geo_type = try parseGeographyType(allocator, reader);
                    result = .{ .geography = geo_type };
                },
                else => try reader.skip(field.field_type),
            }
        }

        reader.last_field_id = saved_field_id;
        return result;
    }

    /// Serialize LogicalType to Thrift Compact Protocol
    pub fn serialize(self: *const Self, writer: *thrift.CompactWriter) !void {
        const saved = writer.saveFieldId();
        writer.resetFieldTracking();

        switch (self.*) {
            .string => {
                // Field 1: STRING (empty struct)
                try writer.writeFieldHeader(1, .struct_);
                try writer.writeStructEnd();
            },
            .enum_ => {
                // Field 4: ENUM (empty struct)
                try writer.writeFieldHeader(4, .struct_);
                try writer.writeStructEnd();
            },
            .decimal => |d| {
                // Field 5: DECIMAL (DecimalType struct)
                try writer.writeFieldHeader(5, .struct_);
                try serializeDecimalType(&d, writer);
            },
            .date => {
                // Field 6: DATE (empty struct)
                try writer.writeFieldHeader(6, .struct_);
                try writer.writeStructEnd();
            },
            .time => |t| {
                // Field 7: TIME (TimeType struct)
                try writer.writeFieldHeader(7, .struct_);
                try serializeTimeType(&t, writer);
            },
            .timestamp => |ts| {
                // Field 8: TIMESTAMP (TimestampType struct)
                try writer.writeFieldHeader(8, .struct_);
                try serializeTimestampType(&ts, writer);
            },
            .int => |i| {
                // Field 10: INT (IntType struct)
                try writer.writeFieldHeader(10, .struct_);
                try serializeIntType(&i, writer);
            },
            .json => {
                // Field 12: JSON (empty struct)
                try writer.writeFieldHeader(12, .struct_);
                try writer.writeStructEnd();
            },
            .bson => {
                // Field 13: BSON (empty struct)
                try writer.writeFieldHeader(13, .struct_);
                try writer.writeStructEnd();
            },
            .uuid => {
                // Field 14: UUID (empty struct)
                try writer.writeFieldHeader(14, .struct_);
                try writer.writeStructEnd();
            },
            .float16 => {
                // Field 15: FLOAT16 (empty struct)
                try writer.writeFieldHeader(15, .struct_);
                try writer.writeStructEnd();
            },
            .geometry => |g| {
                // Field 17: GEOMETRY (GeometryType struct)
                try writer.writeFieldHeader(17, .struct_);
                try serializeGeometryType(&g, writer);
            },
            .geography => |g| {
                // Field 18: GEOGRAPHY (GeographyType struct)
                try writer.writeFieldHeader(18, .struct_);
                try serializeGeographyType(&g, writer);
            },
        }

        try writer.writeStructEnd();
        writer.restoreFieldId(saved);
    }

    /// Get a human-readable string representation
    pub fn toString(self: Self) []const u8 {
        return switch (self) {
            .string => "STRING",
            .enum_ => "ENUM",
            .decimal => "DECIMAL",
            .date => "DATE",
            .time => "TIME",
            .timestamp => "TIMESTAMP",
            .int => "INT",
            .json => "JSON",
            .bson => "BSON",
            .uuid => "UUID",
            .float16 => "FLOAT16",
            .geometry => "GEOMETRY",
            .geography => "GEOGRAPHY",
        };
    }

    /// Get detailed string with parameters
    pub fn toDetailedString(self: Self, buf: []u8) []const u8 {
        return switch (self) {
            .string => "STRING",
            .enum_ => "ENUM",
            .decimal => |d| blk: {
                const result = std.fmt.bufPrint(buf, "DECIMAL({},{})", .{ d.precision, d.scale }) catch "DECIMAL(?)";
                break :blk result;
            },
            .date => "DATE",
            .time => |t| blk: {
                const unit_str = switch (t.unit) {
                    .millis => "MILLIS",
                    .micros => "MICROS",
                    .nanos => "NANOS",
                };
                const utc_str = if (t.is_adjusted_to_utc) "UTC" else "LOCAL";
                const result = std.fmt.bufPrint(buf, "TIME({s}, {s})", .{ utc_str, unit_str }) catch "TIME(?)";
                break :blk result;
            },
            .timestamp => |ts| blk: {
                const unit_str = switch (ts.unit) {
                    .millis => "MILLIS",
                    .micros => "MICROS",
                    .nanos => "NANOS",
                };
                const utc_str = if (ts.is_adjusted_to_utc) "UTC" else "LOCAL";
                const result = std.fmt.bufPrint(buf, "TIMESTAMP({s}, {s})", .{ utc_str, unit_str }) catch "TIMESTAMP(?)";
                break :blk result;
            },
            .int => |i| blk: {
                const signed_str = if (i.is_signed) "signed" else "unsigned";
                const result = std.fmt.bufPrint(buf, "INT({},{s})", .{ i.bit_width, signed_str }) catch "INT(?)";
                break :blk result;
            },
            .json => "JSON",
            .bson => "BSON",
            .uuid => "UUID",
            .float16 => "FLOAT16",
            .geometry => |g| blk: {
                if (g.crs) |crs| {
                    const result = std.fmt.bufPrint(buf, "GEOMETRY({s})", .{crs}) catch "GEOMETRY(?)";
                    break :blk result;
                } else {
                    break :blk "GEOMETRY";
                }
            },
            .geography => |g| blk: {
                const algo_str = if (g.algorithm) |a| switch (a) {
                    .spherical => "spherical",
                    .vincenty => "vincenty",
                    .thomas => "thomas",
                    .andoyer => "andoyer",
                    .karney => "karney",
                } else "spherical";
                if (g.crs) |crs| {
                    const result = std.fmt.bufPrint(buf, "GEOGRAPHY({s}, {s})", .{ crs, algo_str }) catch "GEOGRAPHY(?)";
                    break :blk result;
                } else {
                    const result = std.fmt.bufPrint(buf, "GEOGRAPHY({s})", .{algo_str}) catch "GEOGRAPHY(?)";
                    break :blk result;
                }
            },
        };
    }
};

// Helper functions for parsing nested types

fn parseTimeUnit(reader: *thrift.CompactReader) !TimeUnit {
    const saved = reader.last_field_id;
    reader.resetFieldTracking();

    var unit: TimeUnit = .millis; // default

    while (try reader.readFieldHeader()) |field| {
        switch (field.field_id) {
            1 => {
                try reader.skip(.struct_); // MILLIS empty struct
                unit = .millis;
            },
            2 => {
                try reader.skip(.struct_); // MICROS empty struct
                unit = .micros;
            },
            3 => {
                try reader.skip(.struct_); // NANOS empty struct
                unit = .nanos;
            },
            else => try reader.skip(field.field_type),
        }
    }

    reader.last_field_id = saved;
    return unit;
}

fn parseTimestampType(reader: *thrift.CompactReader) !TimestampType {
    const saved = reader.last_field_id;
    reader.resetFieldTracking();

    var ts = TimestampType{
        .is_adjusted_to_utc = false,
        .unit = .millis,
    };

    while (try reader.readFieldHeader()) |field| {
        switch (field.field_id) {
            1 => ts.is_adjusted_to_utc = field.field_type == .bool_true,
            2 => ts.unit = try parseTimeUnit(reader),
            else => try reader.skip(field.field_type),
        }
    }

    reader.last_field_id = saved;
    return ts;
}

fn parseTimeType(reader: *thrift.CompactReader) !TimeType {
    const saved = reader.last_field_id;
    reader.resetFieldTracking();

    var t = TimeType{
        .is_adjusted_to_utc = false,
        .unit = .millis,
    };

    while (try reader.readFieldHeader()) |field| {
        switch (field.field_id) {
            1 => t.is_adjusted_to_utc = field.field_type == .bool_true,
            2 => t.unit = try parseTimeUnit(reader),
            else => try reader.skip(field.field_type),
        }
    }

    reader.last_field_id = saved;
    return t;
}

fn parseDecimalType(reader: *thrift.CompactReader) !DecimalType {
    const saved = reader.last_field_id;
    reader.resetFieldTracking();

    var d = DecimalType{
        .precision = 0,
        .scale = 0,
    };

    while (try reader.readFieldHeader()) |field| {
        switch (field.field_id) {
            1 => d.scale = try reader.readI32(),
            2 => d.precision = try reader.readI32(),
            else => try reader.skip(field.field_type),
        }
    }

    reader.last_field_id = saved;
    return d;
}

fn parseIntType(reader: *thrift.CompactReader) !IntType {
    const saved = reader.last_field_id;
    reader.resetFieldTracking();

    var i = IntType{
        .bit_width = 32,
        .is_signed = true,
    };

    while (try reader.readFieldHeader()) |field| {
        switch (field.field_id) {
            1 => i.bit_width = safe.castTo(i8, try reader.readByte()) catch return error.IntegerOverflow,
            2 => i.is_signed = field.field_type == .bool_true,
            else => try reader.skip(field.field_type),
        }
    }

    reader.last_field_id = saved;
    return i;
}

fn parseGeometryType(allocator: std.mem.Allocator, reader: *thrift.CompactReader) !GeometryType {
    const saved = reader.last_field_id;
    reader.resetFieldTracking();

    var g = GeometryType{};

    while (try reader.readFieldHeader()) |field| {
        switch (field.field_id) {
            1 => g.crs = try allocator.dupe(u8, try reader.readString()),
            else => try reader.skip(field.field_type),
        }
    }

    reader.last_field_id = saved;
    return g;
}

fn parseGeographyType(allocator: std.mem.Allocator, reader: *thrift.CompactReader) !GeographyType {
    const saved = reader.last_field_id;
    reader.resetFieldTracking();

    var g = GeographyType{};

    while (try reader.readFieldHeader()) |field| {
        switch (field.field_id) {
            1 => g.crs = try allocator.dupe(u8, try reader.readString()),
            2 => g.algorithm = try EdgeInterpolationAlgorithm.fromInt(try reader.readI32()),
            else => try reader.skip(field.field_type),
        }
    }

    reader.last_field_id = saved;
    return g;
}

// Helper functions for serializing nested types

fn serializeTimeUnit(unit: TimeUnit, writer: *thrift.CompactWriter) !void {
    const saved = writer.saveFieldId();
    writer.resetFieldTracking();

    switch (unit) {
        .millis => {
            try writer.writeFieldHeader(1, .struct_);
            try writer.writeStructEnd();
        },
        .micros => {
            try writer.writeFieldHeader(2, .struct_);
            try writer.writeStructEnd();
        },
        .nanos => {
            try writer.writeFieldHeader(3, .struct_);
            try writer.writeStructEnd();
        },
    }

    try writer.writeStructEnd();
    writer.restoreFieldId(saved);
}

fn serializeTimestampType(ts: *const TimestampType, writer: *thrift.CompactWriter) !void {
    const saved = writer.saveFieldId();
    writer.resetFieldTracking();

    // Field 1: isAdjustedToUTC (required bool)
    try writer.writeBoolField(1, ts.is_adjusted_to_utc);

    // Field 2: unit (required TimeUnit)
    try writer.writeFieldHeader(2, .struct_);
    try serializeTimeUnit(ts.unit, writer);

    try writer.writeStructEnd();
    writer.restoreFieldId(saved);
}

fn serializeTimeType(t: *const TimeType, writer: *thrift.CompactWriter) !void {
    const saved = writer.saveFieldId();
    writer.resetFieldTracking();

    // Field 1: isAdjustedToUTC (required bool)
    try writer.writeBoolField(1, t.is_adjusted_to_utc);

    // Field 2: unit (required TimeUnit)
    try writer.writeFieldHeader(2, .struct_);
    try serializeTimeUnit(t.unit, writer);

    try writer.writeStructEnd();
    writer.restoreFieldId(saved);
}

fn serializeDecimalType(d: *const DecimalType, writer: *thrift.CompactWriter) !void {
    const saved = writer.saveFieldId();
    writer.resetFieldTracking();

    // Field 1: scale (required i32)
    try writer.writeFieldHeader(1, .i32);
    try writer.writeI32(d.scale);

    // Field 2: precision (required i32)
    try writer.writeFieldHeader(2, .i32);
    try writer.writeI32(d.precision);

    try writer.writeStructEnd();
    writer.restoreFieldId(saved);
}

fn serializeIntType(i: *const IntType, writer: *thrift.CompactWriter) !void {
    const saved = writer.saveFieldId();
    writer.resetFieldTracking();

    // Field 1: bitWidth (required i8)
    try writer.writeFieldHeader(1, .i8);
    try writer.writeByte(safe.castTo(u8, i.bit_width) catch return error.IntegerOverflow);

    // Field 2: isSigned (required bool)
    try writer.writeBoolField(2, i.is_signed);

    try writer.writeStructEnd();
    writer.restoreFieldId(saved);
}

fn serializeGeometryType(g: *const GeometryType, writer: *thrift.CompactWriter) !void {
    const saved = writer.saveFieldId();
    writer.resetFieldTracking();

    // Field 1: crs (optional string)
    if (g.crs) |crs| {
        try writer.writeFieldHeader(1, .binary);
        try writer.writeString(crs);
    }

    try writer.writeStructEnd();
    writer.restoreFieldId(saved);
}

fn serializeGeographyType(g: *const GeographyType, writer: *thrift.CompactWriter) !void {
    const saved = writer.saveFieldId();
    writer.resetFieldTracking();

    // Field 1: crs (optional string)
    if (g.crs) |crs| {
        try writer.writeFieldHeader(1, .binary);
        try writer.writeString(crs);
    }

    // Field 2: algorithm (optional enum)
    if (g.algorithm) |algo| {
        try writer.writeFieldHeader(2, .i32);
        try writer.writeI32(@intFromEnum(algo));
    }

    try writer.writeStructEnd();
    writer.restoreFieldId(saved);
}

// Convenience constructors

/// Create a STRING logical type
pub fn string() LogicalType {
    return .string;
}

/// Create a DATE logical type
pub fn date() LogicalType {
    return .date;
}

/// Create a TIMESTAMP logical type
pub fn timestamp(unit: TimeUnit, is_utc: bool) LogicalType {
    return .{ .timestamp = .{
        .is_adjusted_to_utc = is_utc,
        .unit = unit,
    } };
}

/// Create a TIME logical type
pub fn time(unit: TimeUnit, is_utc: bool) LogicalType {
    return .{ .time = .{
        .is_adjusted_to_utc = is_utc,
        .unit = unit,
    } };
}

/// Create a DECIMAL logical type
pub fn decimal(precision: i32, scale: i32) LogicalType {
    return .{ .decimal = .{
        .precision = precision,
        .scale = scale,
    } };
}

/// Create an INT logical type (for INT8, INT16, INT32, INT64, UINT8, etc.)
pub fn int(bit_width: i8, is_signed: bool) LogicalType {
    return .{ .int = .{
        .bit_width = bit_width,
        .is_signed = is_signed,
    } };
}

/// Create a UUID logical type
pub fn uuid_type() LogicalType {
    return .uuid;
}

/// Create a FLOAT16 logical type
pub fn float16() LogicalType {
    return .float16;
}

/// Create an ENUM logical type
pub fn enum_type() LogicalType {
    return .enum_;
}

/// Create a JSON logical type
pub fn json() LogicalType {
    return .json;
}

/// Create a BSON logical type
pub fn bson() LogicalType {
    return .bson;
}

/// Create a GEOMETRY logical type
pub fn geometry(crs: ?[]const u8) LogicalType {
    return .{ .geometry = .{ .crs = crs } };
}

/// Create a GEOGRAPHY logical type
pub fn geography(crs: ?[]const u8, algorithm: ?EdgeInterpolationAlgorithm) LogicalType {
    return .{ .geography = .{ .crs = crs, .algorithm = algorithm } };
}
