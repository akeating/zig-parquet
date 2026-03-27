//! Column Definition Types
//!
//! Defines the ColumnDef and StructField types used for specifying
//! Parquet column schemas in the Writer API.

const std = @import("std");
const format = @import("format.zig");
const schema_mod = @import("schema.zig");
const safe = @import("safe.zig");

pub const SchemaNode = schema_mod.SchemaNode;

/// Field definition for struct columns
pub const StructField = struct {
    name: []const u8,
    type_: format.PhysicalType,
    optional: bool = true,
    logical_type: ?format.LogicalType = null,
};

/// Column definition for the writer
pub const ColumnDef = struct {
    name: []const u8,
    type_: format.PhysicalType,
    optional: bool = false,
    type_length: ?i32 = null, // For FIXED_LEN_BYTE_ARRAY
    logical_type: ?format.LogicalType = null,
    /// Legacy converted type (for types not in LogicalType, like INTERVAL)
    /// Only used when logical_type is null
    converted_type: ?i32 = null,
    codec: format.CompressionCodec = .uncompressed,
    /// Value encoding for the column (defaults to PLAIN)
    /// Supported encodings: .plain, .delta_binary_packed, .delta_length_byte_array,
    /// .delta_byte_array, .byte_stream_split
    value_encoding: format.Encoding = .plain,
    /// For list columns: indicates this is a list type
    is_list: bool = false,
    /// For list columns: indicates if list elements can be null
    element_optional: bool = true,
    /// For struct columns: indicates this is a struct type
    is_struct: bool = false,
    /// For struct columns: child field definitions
    struct_fields: ?[]const StructField = null,
    /// Whether struct_fields was heap-allocated and must be freed
    struct_fields_owned: bool = false,
    /// For map columns: indicates this is a map type
    is_map: bool = false,
    /// For map columns: value type
    map_value_type: ?format.PhysicalType = null,
    /// For map columns: whether values can be null
    map_value_optional: bool = true,
    /// For complex nested types: the full schema node
    /// When set, this takes precedence over the flat flags (is_list, is_map, etc.)
    schema_node: ?*const SchemaNode = null,

    /// Create a LIST column with the given element type
    /// The list itself is always nullable by default.
    pub fn list(name: []const u8, element_type: format.PhysicalType, element_optional: bool) ColumnDef {
        return .{
            .name = name,
            .type_ = element_type,
            .optional = true, // Lists are nullable by default
            .is_list = true,
            .element_optional = element_optional,
        };
    }

    /// Create a LIST of INT32
    pub fn listInt32(name: []const u8, element_optional: bool) ColumnDef {
        return list(name, .int32, element_optional);
    }

    /// Create a LIST of INT64
    pub fn listInt64(name: []const u8, element_optional: bool) ColumnDef {
        return list(name, .int64, element_optional);
    }

    /// Create a LIST of STRING (BYTE_ARRAY)
    pub fn listString(name: []const u8, element_optional: bool) ColumnDef {
        // Note: element has STRING logical type, but we'll handle this in schema generation
        return list(name, .byte_array, element_optional);
    }

    /// Create a STRUCT column with the given fields
    /// Each field becomes a separate physical column in the Parquet file.
    pub fn struct_(name: []const u8, fields: []const StructField, optional: bool) ColumnDef {
        return .{
            .name = name,
            .type_ = .int32, // placeholder, not used for groups
            .optional = optional,
            .is_struct = true,
            .struct_fields = fields,
        };
    }

    /// Create a column from a SchemaNode for complex nested types.
    /// This enables arbitrary nesting like list<struct<...>>, map<string, list<...>>, etc.
    ///
    /// Example:
    /// ```zig
    /// const id_node = SchemaNode{ .int64 = .{} };
    /// const name_node = SchemaNode{ .byte_array = .{} };
    /// const struct_node = SchemaNode{ .struct_ = .{
    ///     .fields = &.{
    ///         .{ .name = "id", .node = &id_node },
    ///         .{ .name = "name", .node = &name_node },
    ///     },
    /// }};
    /// const list_node = SchemaNode{ .list = &struct_node };
    /// const col = ColumnDef.fromNode("items", &list_node);
    /// ```
    pub fn fromNode(name: []const u8, node: *const SchemaNode) ColumnDef {
        return .{
            .name = name,
            .type_ = .int32, // placeholder, actual type comes from schema_node
            .optional = false,
            .schema_node = node,
        };
    }

    /// Create a MAP column with the given key and value types
    /// Keys are always REQUIRED (cannot be null), values can be optional.
    pub fn map(name: []const u8, key_type: format.PhysicalType, value_type: format.PhysicalType, value_optional: bool) ColumnDef {
        return .{
            .name = name,
            .type_ = key_type, // key type
            .optional = true, // maps are nullable by default
            .is_map = true,
            .map_value_type = value_type,
            .map_value_optional = value_optional,
        };
    }

    /// Create a MAP of STRING to INT32
    pub fn mapStringInt32(name: []const u8, value_optional: bool) ColumnDef {
        return map(name, .byte_array, .int32, value_optional);
    }

    /// Create a MAP of STRING to INT64
    pub fn mapStringInt64(name: []const u8, value_optional: bool) ColumnDef {
        return map(name, .byte_array, .int64, value_optional);
    }

    /// Create a MAP of STRING to STRING
    pub fn mapStringString(name: []const u8, value_optional: bool) ColumnDef {
        return map(name, .byte_array, .byte_array, value_optional);
    }

    /// Create a MAP of STRING to FLOAT
    pub fn mapStringFloat32(name: []const u8, value_optional: bool) ColumnDef {
        return map(name, .byte_array, .float, value_optional);
    }

    /// Create a MAP of STRING to DOUBLE
    pub fn mapStringFloat64(name: []const u8, value_optional: bool) ColumnDef {
        return map(name, .byte_array, .double, value_optional);
    }

    /// Create a MAP of STRING to BOOLEAN
    pub fn mapStringBool(name: []const u8, value_optional: bool) ColumnDef {
        return map(name, .byte_array, .boolean, value_optional);
    }

    /// Create a MAP of INT32 to STRING
    pub fn mapInt32String(name: []const u8, value_optional: bool) ColumnDef {
        return map(name, .int32, .byte_array, value_optional);
    }

    /// Create a MAP of INT64 to STRING
    pub fn mapInt64String(name: []const u8, value_optional: bool) ColumnDef {
        return map(name, .int64, .byte_array, value_optional);
    }

    /// Free heap-allocated struct_fields if owned by this ColumnDef.
    pub fn freeStructFields(self: *ColumnDef, allocator: std.mem.Allocator) void {
        if (self.struct_fields_owned) {
            if (self.struct_fields) |sf| allocator.free(sf);
            self.struct_fields = null;
            self.struct_fields_owned = false;
        }
    }

    /// Create a STRING column (BYTE_ARRAY with STRING annotation)
    pub fn string(name: []const u8, optional: bool) ColumnDef {
        return .{
            .name = name,
            .type_ = .byte_array,
            .optional = optional,
            .logical_type = .string,
        };
    }

    /// Create a DATE column (INT32 with DATE annotation)
    pub fn date(name: []const u8, optional: bool) ColumnDef {
        return .{
            .name = name,
            .type_ = .int32,
            .optional = optional,
            .logical_type = .date,
        };
    }

    /// Create a TIMESTAMP column (INT64 with TIMESTAMP annotation)
    pub fn timestamp(name: []const u8, unit: format.TimeUnit, is_utc: bool, optional: bool) ColumnDef {
        return .{
            .name = name,
            .type_ = .int64,
            .optional = optional,
            .logical_type = .{ .timestamp = .{
                .is_adjusted_to_utc = is_utc,
                .unit = unit,
            } },
        };
    }

    /// Create a TIME column (INT32 for millis, INT64 for micros/nanos)
    pub fn time(name: []const u8, unit: format.TimeUnit, is_utc: bool, optional: bool) ColumnDef {
        const phys_type: format.PhysicalType = if (unit == .millis) .int32 else .int64;
        return .{
            .name = name,
            .type_ = phys_type,
            .optional = optional,
            .logical_type = .{ .time = .{
                .is_adjusted_to_utc = is_utc,
                .unit = unit,
            } },
        };
    }

    /// Create a DECIMAL column
    /// Physical type: INT32 (precision <= 9), INT64 (precision <= 18), or FIXED_LEN_BYTE_ARRAY
    pub fn decimal(name: []const u8, precision: i32, scale: i32, optional: bool) ColumnDef {
        // Determine physical type based on precision
        if (precision <= 9) {
            return .{
                .name = name,
                .type_ = .int32,
                .optional = optional,
                .logical_type = .{ .decimal = .{ .precision = precision, .scale = scale } },
            };
        } else if (precision <= 18) {
            return .{
                .name = name,
                .type_ = .int64,
                .optional = optional,
                .logical_type = .{ .decimal = .{ .precision = precision, .scale = scale } },
            };
        } else {
            // For precision > 18, use FIXED_LEN_BYTE_ARRAY
            // Number of bytes needed: ceil((precision * log2(10)) / 8) ≈ ceil(precision * 0.415)
            // Simplified: (precision + 1) / 2 gives a reasonable approximation
            const byte_len: i32 = @divTrunc(precision + 1, 2);
            return .{
                .name = name,
                .type_ = .fixed_len_byte_array,
                .optional = optional,
                .type_length = byte_len,
                .logical_type = .{ .decimal = .{ .precision = precision, .scale = scale } },
            };
        }
    }

    /// Create a UUID column (FIXED_LEN_BYTE_ARRAY(16))
    pub fn uuid(name: []const u8, optional: bool) ColumnDef {
        return .{
            .name = name,
            .type_ = .fixed_len_byte_array,
            .optional = optional,
            .type_length = 16,
            .logical_type = .uuid,
        };
    }

    /// Create an INT8 column (INT32 with INT(8,signed) annotation)
    pub fn int8(name: []const u8, optional: bool) ColumnDef {
        return .{
            .name = name,
            .type_ = .int32,
            .optional = optional,
            .logical_type = .{ .int = .{ .bit_width = 8, .is_signed = true } },
        };
    }

    /// Create an INT16 column (INT32 with INT(16,signed) annotation)
    pub fn int16(name: []const u8, optional: bool) ColumnDef {
        return .{
            .name = name,
            .type_ = .int32,
            .optional = optional,
            .logical_type = .{ .int = .{ .bit_width = 16, .is_signed = true } },
        };
    }

    /// Create a UINT8 column (INT32 with INT(8,unsigned) annotation)
    pub fn uint8(name: []const u8, optional: bool) ColumnDef {
        return .{
            .name = name,
            .type_ = .int32,
            .optional = optional,
            .logical_type = .{ .int = .{ .bit_width = 8, .is_signed = false } },
        };
    }

    /// Create a UINT16 column (INT32 with INT(16,unsigned) annotation)
    pub fn uint16(name: []const u8, optional: bool) ColumnDef {
        return .{
            .name = name,
            .type_ = .int32,
            .optional = optional,
            .logical_type = .{ .int = .{ .bit_width = 16, .is_signed = false } },
        };
    }

    /// Create a UINT32 column (INT32 with INT(32,unsigned) annotation)
    pub fn uint32(name: []const u8, optional: bool) ColumnDef {
        return .{
            .name = name,
            .type_ = .int32,
            .optional = optional,
            .logical_type = .{ .int = .{ .bit_width = 32, .is_signed = false } },
        };
    }

    /// Create a UINT64 column (INT64 with INT(64,unsigned) annotation)
    pub fn uint64(name: []const u8, optional: bool) ColumnDef {
        return .{
            .name = name,
            .type_ = .int64,
            .optional = optional,
            .logical_type = .{ .int = .{ .bit_width = 64, .is_signed = false } },
        };
    }

    /// Create a FLOAT16 column (FIXED_LEN_BYTE_ARRAY(2) with FLOAT16 annotation)
    pub fn float16(name: []const u8, optional: bool) ColumnDef {
        return .{
            .name = name,
            .type_ = .fixed_len_byte_array,
            .optional = optional,
            .type_length = 2,
            .logical_type = .float16,
        };
    }

    /// Create an ENUM column (BYTE_ARRAY with ENUM annotation)
    pub fn enum_(name: []const u8, optional: bool) ColumnDef {
        return .{
            .name = name,
            .type_ = .byte_array,
            .optional = optional,
            .logical_type = .enum_,
        };
    }

    /// Create a JSON column (BYTE_ARRAY with JSON annotation)
    pub fn json(name: []const u8, optional: bool) ColumnDef {
        return .{
            .name = name,
            .type_ = .byte_array,
            .optional = optional,
            .logical_type = .json,
        };
    }

    /// Create a BSON column (BYTE_ARRAY with BSON annotation)
    pub fn bson(name: []const u8, optional: bool) ColumnDef {
        return .{
            .name = name,
            .type_ = .byte_array,
            .optional = optional,
            .logical_type = .bson,
        };
    }

    /// Create an INTERVAL column (FIXED_LEN_BYTE_ARRAY(12) with INTERVAL converted_type)
    /// INTERVAL is a legacy type using ConvertedType (not LogicalType).
    /// It stores months(u32) + days(u32) + millis(u32) in little-endian format.
    /// Note: Statistics are not written for INTERVAL (sort order is undefined per spec).
    pub fn interval(name: []const u8, optional: bool) ColumnDef {
        return .{
            .name = name,
            .type_ = .fixed_len_byte_array,
            .optional = optional,
            .type_length = 12,
            .logical_type = null, // INTERVAL is not in LogicalType
            .converted_type = format.ConvertedType.INTERVAL,
        };
    }

    /// Create a GEOMETRY column (BYTE_ARRAY with GEOMETRY logical type)
    /// Stores WKB-encoded geospatial data with linear/planar edge interpolation.
    /// Note: Statistics min/max are not written (sort order is undefined per spec).
    pub fn geometry(name: []const u8, optional: bool, crs: ?[]const u8) ColumnDef {
        return .{
            .name = name,
            .type_ = .byte_array,
            .optional = optional,
            .logical_type = .{ .geometry = .{ .crs = crs } },
        };
    }

    /// Create a GEOGRAPHY column (BYTE_ARRAY with GEOGRAPHY logical type)
    /// Stores WKB-encoded geospatial data with explicit edge interpolation algorithm.
    /// Note: Statistics min/max are not written (sort order is undefined per spec).
    pub fn geography(name: []const u8, optional: bool, crs: ?[]const u8, algorithm: ?format.EdgeInterpolationAlgorithm) ColumnDef {
        return .{
            .name = name,
            .type_ = .byte_array,
            .optional = optional,
            .logical_type = .{ .geography = .{ .crs = crs, .algorithm = algorithm } },
        };
    }

    /// Create a ColumnDef from a SchemaNode.
    ///
    /// This bridges the new recursive SchemaNode type to the flat ColumnDef
    /// representation. Supports primitives, optional wrappers, lists, maps,
    /// and structs (but not deeply nested compositions yet).
    pub fn fromSchemaNode(allocator: std.mem.Allocator, name: []const u8, node: *const SchemaNode) !ColumnDef {
        return try fromSchemaNodeRecursive(allocator, name, node, false);
    }

    fn fromSchemaNodeRecursive(allocator: std.mem.Allocator, name: []const u8, node: *const SchemaNode, is_optional: bool) !ColumnDef {
        switch (node.*) {
            .boolean => return .{ .name = name, .type_ = .boolean, .optional = is_optional },
            .int32 => return .{ .name = name, .type_ = .int32, .optional = is_optional },
            .int64 => return .{ .name = name, .type_ = .int64, .optional = is_optional },
            .float => return .{ .name = name, .type_ = .float, .optional = is_optional },
            .double => return .{ .name = name, .type_ = .double, .optional = is_optional },
            .byte_array => return .{ .name = name, .type_ = .byte_array, .optional = is_optional },
            .fixed_len_byte_array => |len| return .{
                .name = name,
                .type_ = .fixed_len_byte_array,
                .optional = is_optional,
                .type_length = try safe.castTo(i32, len),
            },
            .optional => |child| return try fromSchemaNodeRecursive(allocator, name, child, true),
            .list => |element| {
                // Get element type and optionality
                const unwrapped = element.unwrapOptional();
                const element_optional = (element != unwrapped);
                const phys_type = getPhysicalType(unwrapped);
                return .{
                    .name = name,
                    .type_ = phys_type,
                    .optional = is_optional,
                    .is_list = true,
                    .element_optional = element_optional,
                };
            },
            .map => |m| {
                const key_type = getPhysicalType(m.key.unwrapOptional());
                const value_unwrapped = m.value.unwrapOptional();
                const value_type = getPhysicalType(value_unwrapped);
                const value_optional = (m.value != value_unwrapped);
                return .{
                    .name = name,
                    .type_ = key_type,
                    .optional = is_optional,
                    .is_map = true,
                    .map_value_type = value_type,
                    .map_value_optional = value_optional,
                };
            },
            .struct_ => |s| {
                const struct_fields = try allocator.alloc(StructField, s.fields.len);
                errdefer allocator.free(struct_fields);
                for (s.fields, 0..) |f, i| {
                    const unwrapped = f.node.unwrapOptional();
                    const field_optional = (f.node != unwrapped);
                    struct_fields[i] = .{
                        .name = f.name,
                        .type_ = getPhysicalType(unwrapped),
                        .optional = field_optional,
                    };
                }
                return .{
                    .name = name,
                    .type_ = .int32, // placeholder
                    .optional = is_optional,
                    .is_struct = true,
                    .struct_fields = struct_fields,
                    .struct_fields_owned = true,
                };
            },
        }
    }

    fn getPhysicalType(node: *const SchemaNode) format.PhysicalType {
        return switch (node.*) {
            .boolean => .boolean,
            .int32 => .int32,
            .int64 => .int64,
            .float => .float,
            .double => .double,
            .byte_array => .byte_array,
            .fixed_len_byte_array => .fixed_len_byte_array,
            .optional => |child| getPhysicalType(child),
            // For nested types, default to byte_array (would need more logic for proper support)
            .list, .map, .struct_ => .byte_array,
        };
    }
};
