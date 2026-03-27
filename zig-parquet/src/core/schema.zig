//! Schema types for nested Parquet structures
//!
//! This module provides recursive schema definitions that enable arbitrary
//! nesting of lists, maps, and structs (e.g., list<struct<...>>, map<string, list<...>>).
//!
//! Supports both physical types (int32, int64, etc.) and logical types
//! (STRING, DATE, TIMESTAMP, DECIMAL, etc.) via optional annotations.

const std = @import("std");
const safe = @import("safe.zig");
const format = @import("format.zig");

/// Import logical types from format module
pub const LogicalType = format.LogicalType;
pub const TimeUnit = format.TimeUnit;
pub const TimestampType = format.TimestampType;
pub const TimeType = format.TimeType;
pub const DecimalType = format.DecimalType;
pub const IntType = format.IntType;
pub const EdgeInterpolationAlgorithm = format.EdgeInterpolationAlgorithm;
pub const GeometryType = format.GeometryType;
pub const GeographyType = format.GeographyType;

/// Recursive schema node for arbitrary nesting of Parquet types.
///
/// Unlike the flat `ColumnDef` which uses boolean flags, `SchemaNode` is a
/// tagged union that can recursively compose types via pointers.
///
/// Primitives can carry optional logical type annotations. For example,
/// `byte_array` with `.string` logical type represents UTF-8 strings.
///
/// ## Example
/// ```zig
/// // list<struct<id: i64, name: string>>
/// const id_field = SchemaNode{ .int64 = .{} };
/// const name_field = SchemaNode{ .byte_array = .{ .logical = .string } };
/// const struct_node = SchemaNode{ .struct_ = .{
///     .fields = &[_]SchemaNode.Field{
///         .{ .name = "id", .node = &id_field },
///         .{ .name = "name", .node = &name_field },
///     },
/// }};
/// const list_of_struct = SchemaNode{ .list = &struct_node };
/// ```
///
/// ## Logical Types
/// ```zig
/// // DATE (days since epoch)
/// const date_col = SchemaNode{ .int32 = .{ .logical = .date } };
///
/// // TIMESTAMP (microseconds, UTC)
/// const ts_col = SchemaNode{ .int64 = .{ .logical = .{ .timestamp = .{
///     .unit = .micros,
///     .is_adjusted_to_utc = true,
/// }}}};
///
/// // DECIMAL(10, 2)
/// const decimal_col = SchemaNode{ .int64 = .{ .logical = .{ .decimal = .{
///     .precision = 10,
///     .scale = 2,
/// }}}};
/// ```
pub const SchemaNode = union(enum) {
    // Primitive types (leaves) with optional logical type annotation
    boolean: PrimitiveType,
    int32: PrimitiveType,
    int64: PrimitiveType,
    float: PrimitiveType,
    double: PrimitiveType,
    byte_array: PrimitiveType,
    fixed_len_byte_array: FixedLenType,

    // Nested types (recursive via pointer)
    // Uses *const to allow ergonomic static construction: `SchemaNode{ .list = &element }`
    optional: *const SchemaNode, // nullable wrapper
    list: *const SchemaNode, // element type
    map: MapType, // key + value types
    struct_: StructType, // named fields

    /// Primitive type with optional logical annotation
    pub const PrimitiveType = struct {
        logical: ?LogicalType = null,
    };

    /// Fixed-length byte array with length and optional logical type
    pub const FixedLenType = struct {
        len: u32,
        logical: ?LogicalType = null,
    };

    /// Map type with key and value schema nodes
    pub const MapType = struct {
        key: *const SchemaNode,
        value: *const SchemaNode,
    };

    /// Struct type with named fields
    pub const StructType = struct {
        fields: []const Field,
    };

    /// A named field within a struct
    pub const Field = struct {
        name: []const u8,
        node: *const SchemaNode,
    };

    /// Result of level computation
    pub const Levels = struct {
        max_def: u8,
        max_rep: u8,
    };

    /// Compute max definition and repetition levels for this schema node.
    ///
    /// Definition levels track nullability depth, repetition levels track
    /// repeated field depth. These are needed for Parquet's level encoding.
    pub fn computeLevels(self: *const SchemaNode) error{InvalidSchema}!Levels {
        var def: u8 = 0;
        var rep: u8 = 0;
        try computeLevelsRecursive(self, &def, &rep);
        return .{ .max_def = def, .max_rep = rep };
    }

    fn computeLevelsRecursive(node: *const SchemaNode, def: *u8, rep: *u8) error{InvalidSchema}!void {
        switch (node.*) {
            .optional => |child| {
                def.* = std.math.add(u8, def.*, 1) catch return error.InvalidSchema;
                try computeLevelsRecursive(child, def, rep);
            },
            .list => |element| {
                def.* = std.math.add(u8, def.*, 1) catch return error.InvalidSchema;
                rep.* = std.math.add(u8, rep.*, 1) catch return error.InvalidSchema;
                try computeLevelsRecursive(element, def, rep);
            },
            .map => |m| {
                def.* = std.math.add(u8, def.*, 1) catch return error.InvalidSchema;
                rep.* = std.math.add(u8, rep.*, 1) catch return error.InvalidSchema;
                var key_def = def.*;
                var key_rep = rep.*;
                try computeLevelsRecursive(m.key, &key_def, &key_rep);
                var val_def = def.*;
                var val_rep = rep.*;
                try computeLevelsRecursive(m.value, &val_def, &val_rep);
                if (key_def > def.*) def.* = key_def;
                if (key_rep > rep.*) rep.* = key_rep;
                if (val_def > def.*) def.* = val_def;
                if (val_rep > rep.*) rep.* = val_rep;
            },
            .struct_ => |s| {
                for (s.fields) |f| {
                    var field_def = def.*;
                    var field_rep = rep.*;
                    try computeLevelsRecursive(f.node, &field_def, &field_rep);
                    if (field_def > def.*) def.* = field_def;
                    if (field_rep > rep.*) rep.* = field_rep;
                }
            },
            .boolean, .int32, .int64, .float, .double, .byte_array, .fixed_len_byte_array => {},
        }
    }

    /// Compute levels for each leaf column in this schema.
    ///
    /// Returns an array of Levels, one per leaf column. The caller owns the
    /// returned slice and must free it with the provided allocator.
    pub fn computeLeafLevels(self: *const SchemaNode, allocator: std.mem.Allocator) ![]Levels {
        const leaf_count = self.countLeafColumns();
        const result = try allocator.alloc(Levels, leaf_count);
        errdefer allocator.free(result);
        var index: usize = 0;
        try computeLeafLevelsRecursive(self, 0, 0, result, &index);
        return result;
    }

    fn computeLeafLevelsRecursive(
        node: *const SchemaNode,
        current_def: u8,
        current_rep: u8,
        result: []Levels,
        index: *usize,
    ) error{InvalidSchema}!void {
        switch (node.*) {
            .optional => |child| {
                const new_def = std.math.add(u8, current_def, 1) catch return error.InvalidSchema;
                try computeLeafLevelsRecursive(child, new_def, current_rep, result, index);
            },
            .list => |element| {
                const new_def = std.math.add(u8, current_def, 1) catch return error.InvalidSchema;
                const new_rep = std.math.add(u8, current_rep, 1) catch return error.InvalidSchema;
                try computeLeafLevelsRecursive(element, new_def, new_rep, result, index);
            },
            .map => |m| {
                const new_def = std.math.add(u8, current_def, 1) catch return error.InvalidSchema;
                const new_rep = std.math.add(u8, current_rep, 1) catch return error.InvalidSchema;
                try computeLeafLevelsRecursive(m.key, new_def, new_rep, result, index);
                const value_extra_def: u8 = if (m.value.* == .optional) 0 else 1;
                const val_def = std.math.add(u8, new_def, value_extra_def) catch return error.InvalidSchema;
                try computeLeafLevelsRecursive(m.value, val_def, new_rep, result, index);
            },
            .struct_ => |s| {
                for (s.fields) |f| {
                    try computeLeafLevelsRecursive(f.node, current_def, current_rep, result, index);
                }
            },
            .boolean, .int32, .int64, .float, .double, .byte_array, .fixed_len_byte_array => {
                if (index.* >= result.len) return error.InvalidSchema;
                result[index.*] = .{ .max_def = current_def, .max_rep = current_rep };
                index.* += 1;
            },
        }
    }

    /// Count the number of physical (leaf) columns this schema produces.
    ///
    /// Lists and maps each produce one or two leaf columns, structs produce
    /// one leaf per field, and primitives produce one leaf.
    pub fn countLeafColumns(self: *const SchemaNode) usize {
        return countLeafColumnsRecursive(self);
    }

    fn countLeafColumnsRecursive(node: *const SchemaNode) usize {
        return switch (node.*) {
            .optional => |child| countLeafColumnsRecursive(child),
            .list => |element| countLeafColumnsRecursive(element),
            .map => |m| countLeafColumnsRecursive(m.key) + countLeafColumnsRecursive(m.value),
            .struct_ => |s| {
                var count: usize = 0;
                for (s.fields) |f| {
                    count += countLeafColumnsRecursive(f.node);
                }
                return count;
            },
            // Primitives are leaves
            .boolean, .int32, .int64, .float, .double, .byte_array, .fixed_len_byte_array => 1,
        };
    }

    /// Check if this node is a primitive (leaf) type
    pub fn isPrimitive(self: *const SchemaNode) bool {
        return switch (self.*) {
            .boolean, .int32, .int64, .float, .double, .byte_array, .fixed_len_byte_array => true,
            .optional, .list, .map, .struct_ => false,
        };
    }

    /// Unwrap optional wrapper if present, returning the inner node
    pub fn unwrapOptional(self: *const SchemaNode) *const SchemaNode {
        return switch (self.*) {
            .optional => |child| child.unwrapOptional(),
            else => self,
        };
    }

    /// Get the logical type annotation if this is a primitive with one
    pub fn getLogicalType(self: *const SchemaNode) ?LogicalType {
        return switch (self.*) {
            .boolean => |p| p.logical,
            .int32 => |p| p.logical,
            .int64 => |p| p.logical,
            .float => |p| p.logical,
            .double => |p| p.logical,
            .byte_array => |p| p.logical,
            .fixed_len_byte_array => |f| f.logical,
            .optional => |child| child.getLogicalType(),
            else => null,
        };
    }

    // =========================================================================
    // Convenience constructors for common types
    // =========================================================================

    /// Create a STRING column (byte_array with UTF-8 semantics)
    pub fn string() SchemaNode {
        return .{ .byte_array = .{ .logical = .string } };
    }

    /// Create a DATE column (int32 days since epoch)
    pub fn date() SchemaNode {
        return .{ .int32 = .{ .logical = .date } };
    }

    /// Create a TIMESTAMP column
    pub fn timestamp(unit: TimeUnit, is_utc: bool) SchemaNode {
        return .{ .int64 = .{ .logical = .{ .timestamp = .{
            .unit = unit,
            .is_adjusted_to_utc = is_utc,
        } } } };
    }

    /// Create a TIME column
    pub fn time(unit: TimeUnit, is_utc: bool) SchemaNode {
        const phys = if (unit == .millis)
            SchemaNode{ .int32 = .{ .logical = .{ .time = .{
                .unit = unit,
                .is_adjusted_to_utc = is_utc,
            } } } }
        else
            SchemaNode{ .int64 = .{ .logical = .{ .time = .{
                .unit = unit,
                .is_adjusted_to_utc = is_utc,
            } } } };
        return phys;
    }

    /// Create a DECIMAL column
    /// Uses int32 for precision <= 9, int64 for precision <= 18, otherwise fixed_len_byte_array
    /// Precision must be 1-38, scale must be 0-precision (per Parquet spec).
    pub fn decimal(precision: i32, scale: i32) SchemaNode {
        std.debug.assert(precision >= 1 and precision <= 38);
        std.debug.assert(scale >= 0 and scale <= precision);
        const logical = LogicalType{ .decimal = .{ .precision = precision, .scale = scale } };
        if (precision <= 9) {
            return .{ .int32 = .{ .logical = logical } };
        } else if (precision <= 18) {
            return .{ .int64 = .{ .logical = logical } };
        } else {
            const bytes_needed: u32 = safe.castTo(u32, @divFloor(precision * 5, 12) + 1) catch unreachable; // precision bounded to 38; result fits u32
            return .{ .fixed_len_byte_array = .{ .len = bytes_needed, .logical = logical } };
        }
    }

    /// Create a UUID column (fixed 16-byte array)
    pub fn uuid() SchemaNode {
        return .{ .fixed_len_byte_array = .{ .len = 16, .logical = .uuid } };
    }

    /// Create a JSON column (byte_array with JSON semantics)
    pub fn json() SchemaNode {
        return .{ .byte_array = .{ .logical = .json } };
    }

    /// Create a BSON column (byte_array with BSON semantics)
    pub fn bson() SchemaNode {
        return .{ .byte_array = .{ .logical = .bson } };
    }

    /// Create an ENUM column (byte_array with enum semantics)
    pub fn enumType() SchemaNode {
        return .{ .byte_array = .{ .logical = .enum_ } };
    }

    /// Create a signed integer column with specific bit width
    pub fn signedInt(bit_width: i8) SchemaNode {
        const logical = LogicalType{ .int = .{ .bit_width = bit_width, .is_signed = true } };
        if (bit_width <= 32) {
            return .{ .int32 = .{ .logical = logical } };
        } else {
            return .{ .int64 = .{ .logical = logical } };
        }
    }

    /// Create an unsigned integer column with specific bit width
    pub fn unsignedInt(bit_width: i8) SchemaNode {
        const logical = LogicalType{ .int = .{ .bit_width = bit_width, .is_signed = false } };
        if (bit_width <= 32) {
            return .{ .int32 = .{ .logical = logical } };
        } else {
            return .{ .int64 = .{ .logical = logical } };
        }
    }

    // =========================================================================
    // Additional logical type constructors
    // =========================================================================

    /// Create a FLOAT16 column (IEEE 754 half-precision, 2-byte fixed array)
    pub fn float16() SchemaNode {
        return .{ .fixed_len_byte_array = .{ .len = 2, .logical = .float16 } };
    }

    /// Create a FIXED_LEN_BYTE_ARRAY column with the given length
    pub fn fixedBinary(len: u32) SchemaNode {
        return .{ .fixed_len_byte_array = .{ .len = len } };
    }

    /// Create a GEOMETRY column (byte_array with WKB-encoded geospatial data)
    /// Uses linear/planar edge interpolation. Default CRS is OGC:CRS84.
    pub fn geometry(crs: ?[]const u8) SchemaNode {
        return .{ .byte_array = .{ .logical = .{ .geometry = .{ .crs = crs } } } };
    }

    /// Create a GEOGRAPHY column (byte_array with WKB-encoded geospatial data)
    /// Uses explicit edge interpolation algorithm. Default CRS is OGC:CRS84, default algorithm is spherical.
    pub fn geography(crs: ?[]const u8, algorithm: ?format.EdgeInterpolationAlgorithm) SchemaNode {
        return .{ .byte_array = .{ .logical = .{ .geography = .{ .crs = crs, .algorithm = algorithm } } } };
    }

    // =========================================================================
    // Nested type helpers
    // =========================================================================

    /// Create a LIST column with the given element type
    pub fn listOf(comptime element: *const SchemaNode) SchemaNode {
        return .{ .list = element };
    }

    /// Create a MAP column with the given key and value types
    pub fn mapOf(comptime key: *const SchemaNode, comptime value: *const SchemaNode) SchemaNode {
        return .{ .map = .{ .key = key, .value = value } };
    }

    /// Create an OPTIONAL wrapper around a type
    pub fn nullable(comptime inner: *const SchemaNode) SchemaNode {
        return .{ .optional = inner };
    }

    // =========================================================================
    // Build SchemaNode tree from flat SchemaElement array
    // =========================================================================

    pub const BuildResult = struct {
        node: *const SchemaNode,
        next_idx: usize,
    };

    pub const BuildError = error{
        InvalidSchema,
        OutOfMemory,
    };

    /// Build a SchemaNode tree from a flat SchemaElement array starting at `idx`.
    /// The caller must free all allocated memory via the arena allocator.
    /// This is the inverse of Writer.generateSchemaFromNodeStatic.
    const max_schema_depth: u16 = 128;

    pub fn buildFromElements(
        allocator: std.mem.Allocator,
        schema: []const format.SchemaElement,
        idx: usize,
    ) BuildError!BuildResult {
        return buildFromElementsImpl(allocator, schema, idx, 0);
    }

    fn buildFromElementsImpl(
        allocator: std.mem.Allocator,
        schema: []const format.SchemaElement,
        idx: usize,
        depth: u16,
    ) BuildError!BuildResult {
        if (depth > max_schema_depth) return error.InvalidSchema;
        if (idx >= schema.len) return error.InvalidSchema;
        const elem = schema[idx];

        if (isLeafElement(elem)) {
            return buildLeafNode(allocator, elem, idx);
        }

        return buildGroupNode(allocator, schema, idx, elem, depth);
    }

    fn isLeafElement(elem: format.SchemaElement) bool {
        return elem.type_ != null and elem.num_children == null;
    }

    fn buildLeafNode(
        allocator: std.mem.Allocator,
        elem: format.SchemaElement,
        idx: usize,
    ) BuildError!BuildResult {
        const node = try allocator.create(SchemaNode);
        errdefer allocator.destroy(node);
        node.* = try physicalToSchemaNode(elem);

        if (elem.repetition_type) |rt| {
            if (rt == .optional) {
                const wrapper = try allocator.create(SchemaNode);
                wrapper.* = .{ .optional = node };
                return .{ .node = wrapper, .next_idx = idx + 1 };
            }
        }
        return .{ .node = node, .next_idx = idx + 1 };
    }

    fn physicalToSchemaNode(elem: format.SchemaElement) BuildError!SchemaNode {
        const logical = elem.logical_type;
        const pt = elem.type_ orelse return .{ .byte_array = .{ .logical = logical } };
        return switch (pt) {
            .boolean => .{ .boolean = .{ .logical = logical } },
            .int32 => .{ .int32 = .{ .logical = logical } },
            .int64 => .{ .int64 = .{ .logical = logical } },
            .int96 => .{ .int64 = .{ .logical = logical } },
            .float => .{ .float = .{ .logical = logical } },
            .double => .{ .double = .{ .logical = logical } },
            .byte_array => .{ .byte_array = .{ .logical = logical } },
            .fixed_len_byte_array => .{ .fixed_len_byte_array = .{
                .len = if (elem.type_length) |tl| safe.castTo(u32, tl) catch return error.InvalidSchema else return error.InvalidSchema,
                .logical = logical,
            } },
        };
    }

    fn buildGroupNode(
        allocator: std.mem.Allocator,
        schema: []const format.SchemaElement,
        idx: usize,
        elem: format.SchemaElement,
        depth: u16,
    ) BuildError!BuildResult {
        const nc = safe.castTo(usize, elem.num_children orelse return error.InvalidSchema) catch
            return error.InvalidSchema;

        if (elem.converted_type) |ct| {
            if (ct == format.ConvertedType.LIST) {
                return buildListNode(allocator, schema, idx, elem, nc, depth);
            }
            if (ct == format.ConvertedType.MAP or ct == format.ConvertedType.MAP_KEY_VALUE) {
                return buildMapNode(allocator, schema, idx, elem, depth);
            }
        }

        return buildStructNode(allocator, schema, idx, elem, nc, depth);
    }

    fn buildListNode(
        allocator: std.mem.Allocator,
        schema: []const format.SchemaElement,
        idx: usize,
        container: format.SchemaElement,
        nc: usize,
        depth: u16,
    ) BuildError!BuildResult {
        if (nc != 1) return error.InvalidSchema;
        // LIST schema: container(LIST) -> list(REPEATED) -> element
        var inner_idx = idx + 1;
        if (inner_idx >= schema.len) return error.InvalidSchema;

        const repeated_group = schema[inner_idx];
        if (repeated_group.num_children != null) {
            // Check if the repeated child is itself a LIST or MAP (2-level nested encoding).
            // In this case the repeated group IS the element, not a 3-level wrapper.
            const is_nested_type = if (repeated_group.converted_type) |ct|
                ct == format.ConvertedType.LIST or ct == format.ConvertedType.MAP or ct == format.ConvertedType.MAP_KEY_VALUE
            else
                false;

            if (!is_nested_type) {
                // Standard 3-level: skip the wrapper group to reach the element
                inner_idx += 1;
                if (inner_idx >= schema.len) return error.InvalidSchema;
            }
        }

        const element_result = try buildFromElementsImpl(allocator, schema, inner_idx, depth + 1);
        const end_idx = skipSubtree(schema, idx);

        const node = try allocator.create(SchemaNode);
        node.* = .{ .list = element_result.node };

        if (container.repetition_type) |rt| {
            if (rt == .optional) {
                const wrapper = try allocator.create(SchemaNode);
                wrapper.* = .{ .optional = node };
                return .{ .node = wrapper, .next_idx = end_idx };
            }
        }
        return .{ .node = node, .next_idx = end_idx };
    }

    fn buildMapNode(
        allocator: std.mem.Allocator,
        schema: []const format.SchemaElement,
        idx: usize,
        container: format.SchemaElement,
        depth: u16,
    ) BuildError!BuildResult {
        // MAP schema: container(MAP) -> key_value(REPEATED) -> key, value
        const kv_idx = idx + 1;
        if (kv_idx >= schema.len) return error.InvalidSchema;

        const kv_nc = safe.castTo(usize, schema[kv_idx].num_children orelse return error.InvalidSchema) catch
            return error.InvalidSchema;
        if (kv_nc != 2) return error.InvalidSchema;

        const key_idx = kv_idx + 1;
        if (key_idx >= schema.len) return error.InvalidSchema;

        const key_result = try buildFromElementsImpl(allocator, schema, key_idx, depth + 1);
        const value_result = try buildFromElementsImpl(allocator, schema, key_result.next_idx, depth + 1);
        const end_idx = skipSubtree(schema, idx);

        const node = try allocator.create(SchemaNode);
        node.* = .{ .map = .{ .key = key_result.node, .value = value_result.node } };

        if (container.repetition_type) |rt| {
            if (rt == .optional) {
                const wrapper = try allocator.create(SchemaNode);
                wrapper.* = .{ .optional = node };
                return .{ .node = wrapper, .next_idx = end_idx };
            }
        }
        return .{ .node = node, .next_idx = end_idx };
    }

    fn buildStructNode(
        allocator: std.mem.Allocator,
        schema: []const format.SchemaElement,
        idx: usize,
        container: format.SchemaElement,
        nc: usize,
        depth: u16,
    ) BuildError!BuildResult {
        const fields = try allocator.alloc(Field, nc);
        var child_idx = idx + 1;

        for (0..nc) |i| {
            if (child_idx >= schema.len) return error.InvalidSchema;
            const child_name = try allocator.dupe(u8, schema[child_idx].name);
            const child_result = try buildFromElementsImpl(allocator, schema, child_idx, depth + 1);
            fields[i] = .{ .name = child_name, .node = child_result.node };
            child_idx = child_result.next_idx;
        }

        const node = try allocator.create(SchemaNode);
        node.* = .{ .struct_ = .{ .fields = fields } };

        if (container.repetition_type) |rt| {
            if (rt == .optional) {
                const wrapper = try allocator.create(SchemaNode);
                wrapper.* = .{ .optional = node };
                return .{ .node = wrapper, .next_idx = child_idx };
            }
        }
        return .{ .node = node, .next_idx = child_idx };
    }

    fn skipSubtree(schema: []const format.SchemaElement, start: usize) usize {
        var i = start;
        var to_visit: usize = 1;
        while (to_visit > 0 and i < schema.len) {
            to_visit -= 1;
            if (schema[i].num_children) |nc| {
                to_visit += safe.castTo(usize, nc) catch return schema.len;
            }
            i += 1;
        }
        return i;
    }
};

// =============================================================================
// Tests
// =============================================================================

test "SchemaNode primitive levels" {
    const int_node = SchemaNode{ .int32 = .{} };
    const levels = try int_node.computeLevels();
    try std.testing.expectEqual(@as(u8, 0), levels.max_def);
    try std.testing.expectEqual(@as(u8, 0), levels.max_rep);
}

test "SchemaNode optional primitive levels" {
    const int_node = SchemaNode{ .int32 = .{} };
    const opt_node = SchemaNode{ .optional = &int_node };
    const levels = try opt_node.computeLevels();
    try std.testing.expectEqual(@as(u8, 1), levels.max_def);
    try std.testing.expectEqual(@as(u8, 0), levels.max_rep);
}

test "SchemaNode list levels" {
    const int_node = SchemaNode{ .int32 = .{} };
    const list_node = SchemaNode{ .list = &int_node };
    const levels = try list_node.computeLevels();
    try std.testing.expectEqual(@as(u8, 1), levels.max_def);
    try std.testing.expectEqual(@as(u8, 1), levels.max_rep);
}

test "SchemaNode optional list with optional elements" {
    // optional<list<optional<int32>>>
    // def levels: optional(1) + list(1) + optional(1) = 3
    // rep levels: list(1) = 1
    const int_node = SchemaNode{ .int32 = .{} };
    const opt_int = SchemaNode{ .optional = &int_node };
    const list_node = SchemaNode{ .list = &opt_int };
    const opt_list = SchemaNode{ .optional = &list_node };

    const levels = try opt_list.computeLevels();
    try std.testing.expectEqual(@as(u8, 3), levels.max_def);
    try std.testing.expectEqual(@as(u8, 1), levels.max_rep);
}

test "SchemaNode map levels" {
    const key_node = SchemaNode{ .byte_array = .{} };
    const value_node = SchemaNode{ .int32 = .{} };
    const opt_value = SchemaNode{ .optional = &value_node };
    const bare_map = SchemaNode{ .map = .{ .key = &key_node, .value = &opt_value } };
    const map_node = SchemaNode{ .optional = &bare_map };

    const levels = try map_node.computeLevels();
    // optional(+1), map(+1 def, +1 rep), optional value(+1)
    try std.testing.expectEqual(@as(u8, 3), levels.max_def);
    try std.testing.expectEqual(@as(u8, 1), levels.max_rep);
}

test "SchemaNode struct levels" {
    const id_node = SchemaNode{ .int64 = .{} };
    const name_node = SchemaNode{ .byte_array = .{} };
    const struct_node = SchemaNode{ .struct_ = .{
        .fields = &[_]SchemaNode.Field{
            .{ .name = "id", .node = &id_node },
            .{ .name = "name", .node = &name_node },
        },
    } };

    const levels = try struct_node.computeLevels();
    try std.testing.expectEqual(@as(u8, 0), levels.max_def);
    try std.testing.expectEqual(@as(u8, 0), levels.max_rep);
}

test "SchemaNode list of struct levels" {
    // list<struct<id: int64, name: optional<string>>>
    const id_node = SchemaNode{ .int64 = .{} };
    const name_node = SchemaNode{ .byte_array = .{} };
    const opt_name = SchemaNode{ .optional = &name_node };
    const struct_node = SchemaNode{ .struct_ = .{
        .fields = &[_]SchemaNode.Field{
            .{ .name = "id", .node = &id_node },
            .{ .name = "name", .node = &opt_name },
        },
    } };
    const list_node = SchemaNode{ .list = &struct_node };

    const levels = try list_node.computeLevels();
    // list adds def=1, rep=1; optional name adds def=1
    try std.testing.expectEqual(@as(u8, 2), levels.max_def);
    try std.testing.expectEqual(@as(u8, 1), levels.max_rep);
}

test "SchemaNode countLeafColumns primitive" {
    const int_node = SchemaNode{ .int32 = .{} };
    try std.testing.expectEqual(@as(usize, 1), int_node.countLeafColumns());
}

test "SchemaNode countLeafColumns struct" {
    const id_node = SchemaNode{ .int64 = .{} };
    const name_node = SchemaNode{ .byte_array = .{} };
    const struct_node = SchemaNode{ .struct_ = .{
        .fields = &[_]SchemaNode.Field{
            .{ .name = "id", .node = &id_node },
            .{ .name = "name", .node = &name_node },
        },
    } };
    try std.testing.expectEqual(@as(usize, 2), struct_node.countLeafColumns());
}

test "SchemaNode countLeafColumns map" {
    const key_node = SchemaNode{ .byte_array = .{} };
    const value_node = SchemaNode{ .int32 = .{} };
    const bare_map = SchemaNode{ .map = .{ .key = &key_node, .value = &value_node } };
    const map_node = SchemaNode{ .optional = &bare_map };
    try std.testing.expectEqual(@as(usize, 2), map_node.countLeafColumns());
}

test "SchemaNode countLeafColumns list of struct" {
    const id_node = SchemaNode{ .int64 = .{} };
    const name_node = SchemaNode{ .byte_array = .{} };
    const struct_node = SchemaNode{ .struct_ = .{
        .fields = &[_]SchemaNode.Field{
            .{ .name = "id", .node = &id_node },
            .{ .name = "name", .node = &name_node },
        },
    } };
    const list_node = SchemaNode{ .list = &struct_node };
    try std.testing.expectEqual(@as(usize, 2), list_node.countLeafColumns());
}

// =============================================================================
// buildFromElements tests
// =============================================================================

test "buildFromElements: required int32 leaf" {
    // Schema: [root(1 child), col(INT32, REQUIRED)]
    const elements = [_]format.SchemaElement{
        .{ .name = "schema", .num_children = 1 },
        .{ .name = "col", .type_ = .int32, .repetition_type = .required },
    };
    const allocator = std.testing.allocator;
    const result = try SchemaNode.buildFromElements(allocator, &elements, 0);
    defer freeSchemaNode(allocator, result.node);
    try std.testing.expectEqual(@as(usize, 2), result.next_idx);

    // Root is a struct with 1 field
    switch (result.node.*) {
        .struct_ => |s| {
            try std.testing.expectEqual(@as(usize, 1), s.fields.len);
            try std.testing.expectEqualStrings("col", s.fields[0].name);
            try std.testing.expect(s.fields[0].node.* == .int32);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "buildFromElements: optional int64 leaf" {
    const elements = [_]format.SchemaElement{
        .{ .name = "schema", .num_children = 1 },
        .{ .name = "val", .type_ = .int64, .repetition_type = .optional },
    };
    const allocator = std.testing.allocator;
    const result = try SchemaNode.buildFromElements(allocator, &elements, 0);
    defer freeSchemaNode(allocator, result.node);

    switch (result.node.*) {
        .struct_ => |s| {
            try std.testing.expectEqual(@as(usize, 1), s.fields.len);
            // Optional leaf should be wrapped
            switch (s.fields[0].node.*) {
                .optional => |inner| try std.testing.expect(inner.* == .int64),
                else => return error.TestUnexpectedResult,
            }
        },
        else => return error.TestUnexpectedResult,
    }
}

test "buildFromElements: list<int32>" {
    // Parquet LIST encoding: container(LIST,1 child) -> list(REPEATED,1 child) -> element(INT32)
    const elements = [_]format.SchemaElement{
        .{ .name = "schema", .num_children = 1 },
        .{ .name = "scores", .num_children = 1, .repetition_type = .optional, .converted_type = format.ConvertedType.LIST },
        .{ .name = "list", .num_children = 1, .repetition_type = .repeated },
        .{ .name = "element", .type_ = .int32, .repetition_type = .required },
    };
    const allocator = std.testing.allocator;
    const result = try SchemaNode.buildFromElements(allocator, &elements, 0);
    defer freeSchemaNode(allocator, result.node);
    try std.testing.expectEqual(@as(usize, 4), result.next_idx);

    switch (result.node.*) {
        .struct_ => |s| {
            try std.testing.expectEqual(@as(usize, 1), s.fields.len);
            // optional<list<int32>>
            switch (s.fields[0].node.*) {
                .optional => |inner| switch (inner.*) {
                    .list => |elem| try std.testing.expect(elem.* == .int32),
                    else => return error.TestUnexpectedResult,
                },
                else => return error.TestUnexpectedResult,
            }
        },
        else => return error.TestUnexpectedResult,
    }
}

test "buildFromElements: struct{x: int32, y: float}" {
    const elements = [_]format.SchemaElement{
        .{ .name = "schema", .num_children = 1 },
        .{ .name = "point", .num_children = 2, .repetition_type = .required },
        .{ .name = "x", .type_ = .int32, .repetition_type = .required },
        .{ .name = "y", .type_ = .float, .repetition_type = .required },
    };
    const allocator = std.testing.allocator;
    const result = try SchemaNode.buildFromElements(allocator, &elements, 0);
    defer freeSchemaNode(allocator, result.node);
    try std.testing.expectEqual(@as(usize, 4), result.next_idx);

    switch (result.node.*) {
        .struct_ => |root| {
            try std.testing.expectEqual(@as(usize, 1), root.fields.len);
            switch (root.fields[0].node.*) {
                .struct_ => |s| {
                    try std.testing.expectEqual(@as(usize, 2), s.fields.len);
                    try std.testing.expectEqualStrings("x", s.fields[0].name);
                    try std.testing.expect(s.fields[0].node.* == .int32);
                    try std.testing.expectEqualStrings("y", s.fields[1].name);
                    try std.testing.expect(s.fields[1].node.* == .float);
                },
                else => return error.TestUnexpectedResult,
            }
        },
        else => return error.TestUnexpectedResult,
    }
}

test "buildFromElements: map<bytes, int32>" {
    // MAP encoding: container(MAP,1 child) -> key_value(REPEATED,2 children) -> key(BYTE_ARRAY), value(INT32)
    const elements = [_]format.SchemaElement{
        .{ .name = "schema", .num_children = 1 },
        .{ .name = "props", .num_children = 1, .repetition_type = .optional, .converted_type = format.ConvertedType.MAP },
        .{ .name = "key_value", .num_children = 2, .repetition_type = .repeated },
        .{ .name = "key", .type_ = .byte_array, .repetition_type = .required },
        .{ .name = "value", .type_ = .int32, .repetition_type = .required },
    };
    const allocator = std.testing.allocator;
    const result = try SchemaNode.buildFromElements(allocator, &elements, 0);
    defer freeSchemaNode(allocator, result.node);
    try std.testing.expectEqual(@as(usize, 5), result.next_idx);

    switch (result.node.*) {
        .struct_ => |root| {
            try std.testing.expectEqual(@as(usize, 1), root.fields.len);
            switch (root.fields[0].node.*) {
                .optional => |inner| switch (inner.*) {
                    .map => |m| {
                        try std.testing.expect(m.key.* == .byte_array);
                        try std.testing.expect(m.value.* == .int32);
                    },
                    else => return error.TestUnexpectedResult,
                },
                else => return error.TestUnexpectedResult,
            }
        },
        else => return error.TestUnexpectedResult,
    }
}

test "buildFromElements: list<struct{a: int32, b: byte_array}>" {
    const elements = [_]format.SchemaElement{
        .{ .name = "schema", .num_children = 1 },
        .{ .name = "items", .num_children = 1, .repetition_type = .required, .converted_type = format.ConvertedType.LIST },
        .{ .name = "list", .num_children = 1, .repetition_type = .repeated },
        .{ .name = "element", .num_children = 2, .repetition_type = .required },
        .{ .name = "a", .type_ = .int32, .repetition_type = .required },
        .{ .name = "b", .type_ = .byte_array, .repetition_type = .required },
    };
    const allocator = std.testing.allocator;
    const result = try SchemaNode.buildFromElements(allocator, &elements, 0);
    defer freeSchemaNode(allocator, result.node);
    try std.testing.expectEqual(@as(usize, 6), result.next_idx);

    switch (result.node.*) {
        .struct_ => |root| {
            try std.testing.expectEqual(@as(usize, 1), root.fields.len);
            switch (root.fields[0].node.*) {
                .list => |elem| switch (elem.*) {
                    .struct_ => |s| {
                        try std.testing.expectEqual(@as(usize, 2), s.fields.len);
                        try std.testing.expectEqualStrings("a", s.fields[0].name);
                        try std.testing.expectEqualStrings("b", s.fields[1].name);
                    },
                    else => return error.TestUnexpectedResult,
                },
                else => return error.TestUnexpectedResult,
            }
        },
        else => return error.TestUnexpectedResult,
    }
}

test "buildFromElements: multi-column schema" {
    // Schema with 2 columns: id(INT32), name(BYTE_ARRAY optional)
    const elements = [_]format.SchemaElement{
        .{ .name = "schema", .num_children = 2 },
        .{ .name = "id", .type_ = .int32, .repetition_type = .required },
        .{ .name = "name", .type_ = .byte_array, .repetition_type = .optional },
    };
    const allocator = std.testing.allocator;
    const result = try SchemaNode.buildFromElements(allocator, &elements, 0);
    defer freeSchemaNode(allocator, result.node);
    try std.testing.expectEqual(@as(usize, 3), result.next_idx);

    switch (result.node.*) {
        .struct_ => |root| {
            try std.testing.expectEqual(@as(usize, 2), root.fields.len);
            try std.testing.expectEqualStrings("id", root.fields[0].name);
            try std.testing.expect(root.fields[0].node.* == .int32);
            try std.testing.expectEqualStrings("name", root.fields[1].name);
            switch (root.fields[1].node.*) {
                .optional => |inner| try std.testing.expect(inner.* == .byte_array),
                else => return error.TestUnexpectedResult,
            }
        },
        else => return error.TestUnexpectedResult,
    }
}

test "buildFromElements: 2-level list<list<int32>> (legacy encoding)" {
    // 2-level LIST encoding from old_list_structure.parquet:
    // required group a (LIST) { repeated group array (LIST) { repeated int32 array; } }
    const elements = [_]format.SchemaElement{
        .{ .name = "schema", .num_children = 1 },
        .{ .name = "a", .num_children = 1, .repetition_type = .required, .converted_type = format.ConvertedType.LIST },
        .{ .name = "array", .num_children = 1, .repetition_type = .repeated, .converted_type = format.ConvertedType.LIST },
        .{ .name = "array", .type_ = .int32, .repetition_type = .repeated },
    };
    const allocator = std.testing.allocator;
    const result = try SchemaNode.buildFromElements(allocator, &elements, 0);
    defer freeSchemaNode(allocator, result.node);
    try std.testing.expectEqual(@as(usize, 4), result.next_idx);

    // Should produce: struct { a: list<list<int32>> }
    switch (result.node.*) {
        .struct_ => |root| {
            try std.testing.expectEqual(@as(usize, 1), root.fields.len);
            try std.testing.expectEqualStrings("a", root.fields[0].name);
            switch (root.fields[0].node.*) {
                .list => |outer_elem| switch (outer_elem.*) {
                    .list => |inner_elem| try std.testing.expect(inner_elem.* == .int32),
                    else => return error.TestUnexpectedResult,
                },
                else => return error.TestUnexpectedResult,
            }
        },
        else => return error.TestUnexpectedResult,
    }
}

test "buildFromElements: empty schema" {
    const elements = [_]format.SchemaElement{};
    const allocator = std.testing.allocator;
    const result = SchemaNode.buildFromElements(allocator, &elements, 0);
    try std.testing.expectError(error.InvalidSchema, result);
}

test "buildFromElements: deeply nested schema rejected" {
    // Use arena since buildFromElements is designed for arena-based allocation;
    // partial tree is cleaned up by arena.deinit().
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var elements: [151]format.SchemaElement = undefined;
    elements[0] = .{ .name = "root", .num_children = 1, .repetition_type = .required };
    for (1..150) |i| {
        elements[i] = .{ .name = "group", .num_children = 1, .repetition_type = .optional };
    }
    elements[150] = .{ .name = "leaf", .type_ = .int32, .repetition_type = .required };

    const result = SchemaNode.buildFromElements(allocator, &elements, 0);
    try std.testing.expectError(error.InvalidSchema, result);
}

test "buildFromElements: LIST with num_children != 1 rejected" {
    const allocator = std.testing.allocator;
    const elements = [_]format.SchemaElement{
        .{ .name = "my_list", .num_children = 2, .converted_type = format.ConvertedType.LIST, .repetition_type = .optional },
        .{ .name = "list", .num_children = 1, .repetition_type = .repeated },
        .{ .name = "element", .type_ = .int32, .repetition_type = .required },
    };
    const result = SchemaNode.buildFromElements(allocator, &elements, 0);
    try std.testing.expectError(error.InvalidSchema, result);
}

test "buildFromElements: MAP key_value with num_children != 2 rejected" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const elements = [_]format.SchemaElement{
        .{ .name = "my_map", .num_children = 1, .converted_type = format.ConvertedType.MAP, .repetition_type = .optional },
        .{ .name = "key_value", .num_children = 1, .repetition_type = .repeated },
        .{ .name = "key", .type_ = .byte_array, .repetition_type = .required },
    };
    const result = SchemaNode.buildFromElements(allocator, &elements, 0);
    try std.testing.expectError(error.InvalidSchema, result);
}

test "buildFromElements: negative type_length rejected" {
    const allocator = std.testing.allocator;
    const elements = [_]format.SchemaElement{
        .{ .name = "bad_flba", .type_ = .fixed_len_byte_array, .type_length = -5, .repetition_type = .required },
    };
    const result = SchemaNode.buildFromElements(allocator, &elements, 0);
    try std.testing.expectError(error.InvalidSchema, result);
}

test "SchemaNode decimal max precision" {
    const node = SchemaNode.decimal(38, 10);
    switch (node) {
        .fixed_len_byte_array => |flba| {
            try std.testing.expect(flba.len > 0);
            try std.testing.expect(flba.logical != null);
        },
        else => return error.TestUnexpectedResult,
    }
}

fn freeSchemaNode(allocator: std.mem.Allocator, node: *const SchemaNode) void {
    switch (node.*) {
        .optional => |inner| {
            freeSchemaNode(allocator, inner);
        },
        .list => |elem| {
            freeSchemaNode(allocator, elem);
        },
        .map => |m| {
            freeSchemaNode(allocator, m.key);
            freeSchemaNode(allocator, m.value);
        },
        .struct_ => |s| {
            for (s.fields) |f| {
                allocator.free(f.name);
                freeSchemaNode(allocator, f.node);
            }
            allocator.free(s.fields);
        },
        else => {},
    }
    allocator.destroy(node);
}
