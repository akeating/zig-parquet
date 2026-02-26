//! Struct utilities for Parquet row operations
//!
//! Comptime utilities for analyzing Zig struct types and flattening them
//! into Parquet column descriptors. Used by both RowWriter and RowReader.

const std = @import("std");
const types = @import("types.zig");

// =============================================================================
// Parameterized Type Detection
// =============================================================================

/// Check if a type is a parameterized Timestamp type
pub fn isTimestampType(comptime T: type) bool {
    const info = @typeInfo(T);
    if (info != .@"struct") return false;
    if (!@hasDecl(T, "parquet_logical_type")) return false;
    if (!@hasDecl(T, "time_unit")) return false;
    const lt = T.parquet_logical_type;
    return std.meta.activeTag(lt) == .timestamp;
}

/// Check if a type is a parameterized Time type
pub fn isTimeType(comptime T: type) bool {
    const info = @typeInfo(T);
    if (info != .@"struct") return false;
    if (!@hasDecl(T, "parquet_logical_type")) return false;
    if (!@hasDecl(T, "time_unit")) return false;
    const lt = T.parquet_logical_type;
    return std.meta.activeTag(lt) == .time;
}

/// Check if a type is a TimestampInt96 type (legacy INT96 timestamp)
pub fn isInt96TimestampType(comptime T: type) bool {
    const info = @typeInfo(T);
    if (info == .optional) {
        return isInt96TimestampType(info.optional.child);
    }
    if (info != .@"struct") return false;
    // TimestampInt96 has parquet_physical_type = .int96
    if (@hasDecl(T, "parquet_physical_type")) {
        const format = @import("format.zig");
        return T.parquet_physical_type == format.PhysicalType.int96;
    }
    return false;
}

/// Check if a type is a parameterized Decimal(precision, scale) type
pub fn isDecimalType(comptime T: type) bool {
    const info = @typeInfo(T);
    if (info == .optional) return isDecimalType(info.optional.child);
    if (info != .@"struct") return false;
    if (!@hasDecl(T, "parquet_logical_type")) return false;
    if (!@hasDecl(T, "decimal_precision")) return false;
    const lt = T.parquet_logical_type;
    return std.meta.activeTag(lt) == .decimal;
}

// =============================================================================
// Type Predicates
// =============================================================================

/// Check if a type has a parquet_logical_type declaration (wrapper type)
pub fn hasLogicalTypeDecl(comptime T: type) bool {
    const info = @typeInfo(T);
    if (info == .optional) {
        return hasLogicalTypeDecl(info.optional.child);
    }
    // Only structs can have declarations
    if (info != .@"struct") return false;
    return @hasDecl(T, "parquet_logical_type");
}

/// Check if a type is a nested struct (struct without parquet type markers)
/// These get flattened into multiple columns with dot-path names
pub fn isNestedStruct(comptime T: type) bool {
    const info = @typeInfo(T);
    if (info == .optional) {
        return isNestedStruct(info.optional.child);
    }
    // Must be a struct
    if (info != .@"struct") return false;
    // Must NOT have parquet_logical_type (wrappers like Timestamp, Date, Uuid)
    if (@hasDecl(T, "parquet_logical_type")) return false;
    // Must NOT have parquet_converted_type (legacy types like Interval)
    if (@hasDecl(T, "parquet_converted_type")) return false;
    // Must NOT have parquet_physical_type (special types like TimestampInt96)
    if (@hasDecl(T, "parquet_physical_type")) return false;
    return true;
}

/// Check if a type is a slice (list) type
/// Excludes []const u8 which is treated as a byte array/string
pub fn isSliceType(comptime T: type) bool {
    const info = @typeInfo(T);
    if (info == .optional) {
        return isSliceType(info.optional.child);
    }
    // []const u8 is a string, not a list
    if (T == []const u8) return false;
    return info == .pointer and info.pointer.size == .slice;
}

/// Check if a type is a MapEntry(K, V) from types.zig
pub fn isMapEntryType(comptime T: type) bool {
    const info = @typeInfo(T);
    if (info == .optional) {
        return isMapEntryType(info.optional.child);
    }
    if (info != .@"struct") return false;
    return @hasDecl(T, "is_parquet_map_entry");
}

/// Check if a type is optional
pub fn isOptionalType(comptime T: type) bool {
    return @typeInfo(T) == .optional;
}

// =============================================================================
// Type Extraction
// =============================================================================

/// Get the element type of a slice
pub fn SliceElementType(comptime T: type) type {
    const info = @typeInfo(T);
    if (info == .optional) {
        return SliceElementType(info.optional.child);
    }
    if (info == .pointer and info.pointer.size == .slice) {
        return info.pointer.child;
    }
    @compileError("Not a slice type: " ++ @typeName(T));
}

/// Unwrap optional type to get the child type
pub fn unwrapOptionalType(comptime T: type) type {
    const info = @typeInfo(T);
    if (info == .optional) {
        return info.optional.child;
    }
    return T;
}

// =============================================================================
// Slice Info
// =============================================================================

/// Information about a slice type for Parquet list encoding
pub const SliceInfo = struct {
    /// The innermost element type (unwrapped from optionals)
    element_type: type,
    /// The widened/unwrapped type for encoding (e.g., i8 -> i32, Timestamp -> i64)
    storage_type: type,
    /// Whether the list itself is optional (?[]const T)
    list_optional: bool,
    /// Whether elements are optional ([]const ?T)
    element_optional: bool,
    /// The raw element type as declared (may be optional)
    raw_element_type: type,

    /// Compute max definition level for this slice configuration
    /// For Parquet 3-level list encoding, matches what the schema generates.
    pub fn maxDefLevel(comptime self: SliceInfo) u8 {
        // Parquet 3-level list schema:
        // - container (optional/required) -> +1 if optional
        // - "list" group (repeated) -> +1 always
        // - "element" (always OPTIONAL in schema to distinguish empty lists)
        //
        // We ALWAYS make element OPTIONAL in schema even for required elements.
        // This is necessary to distinguish:
        // - def=N-1: empty list (repeated group marker, no element)
        // - def=N: present value
        //
        // For required list with required elements: 0 + 1 + 1 = 2
        // For optional list with required elements: 1 + 1 + 1 = 3
        // For required list with optional elements: 0 + 1 + 1 = 2 (same as required)
        // For optional list with optional elements: 1 + 1 + 1 = 3 (same as optional list)
        var level: u8 = 2; // Base: repeated "list" group (1) + element always optional (1)
        if (self.list_optional) level += 1;
        // Note: element_optional doesn't add extra level since element is always optional in schema
        return level;
    }
};

/// Compute the storage type for a list element (widened/unwrapped for encoding)
pub fn listElementStorageType(comptime T: type) type {
    // Handle parameterized Timestamp/Time types
    if (comptime isInt96TimestampType(T)) return i64;
    if (isTimestampType(T)) return i64;
    if (isTimeType(T)) return i64;
    if (isDecimalType(T)) return [T.decimal_byte_len]u8;

    // Handle other wrapper types
    if (T == types.Date) return i32;
    if (T == types.Uuid) return [16]u8;
    if (T == types.Interval) return [12]u8;
    if (T == f16) return [2]u8;

    // Handle small integers - widen to i32/i64
    return switch (T) {
        i8, i16, u8, u16, u32 => i32,
        u64 => i64,
        else => T, // Already a native type
    };
}

/// Widen/unwrap a list element value to its storage type
pub fn widenListElement(comptime T: type, value: T) listElementStorageType(T) {
    // Handle parameterized Timestamp/Time types - extract .value field
    if (comptime isInt96TimestampType(T)) {
        return value.value;
    } else if (comptime isTimestampType(T)) {
        return value.value;
    } else if (comptime isTimeType(T)) {
        return value.value;
    } else if (comptime isDecimalType(T)) {
        return value.bytes;
    } else if (T == types.Date) {
        // Handle other wrapper types - extract inner value
        return value.days;
    } else if (T == types.Uuid) {
        return value.bytes;
    } else if (T == types.Interval) {
        return value.toBytes();
    } else if (T == f16) {
        return @as([2]u8, @bitCast(value));
    } else {
        // Handle small integers - widen to i32/i64
        return switch (T) {
            i8, i16 => @as(i32, value),
            u8, u16 => @as(i32, value),
            u32 => @as(i32, @bitCast(value)),
            u64 => @as(i64, @bitCast(value)),
            else => value, // Already a native type
        };
    }
}

/// Analyze a slice type to extract element type and nullability info
pub fn getSliceInfo(comptime T: type) SliceInfo {
    const info = @typeInfo(T);

    // Check if list itself is optional
    if (info == .optional) {
        const inner_info = getSliceInfo(info.optional.child);
        return .{
            .element_type = inner_info.element_type,
            .storage_type = inner_info.storage_type,
            .list_optional = true,
            .element_optional = inner_info.element_optional,
            .raw_element_type = inner_info.raw_element_type,
        };
    }

    // Must be a slice at this point
    if (info != .pointer or info.pointer.size != .slice) {
        @compileError("Expected slice type: " ++ @typeName(T));
    }

    const elem_type = info.pointer.child;
    const elem_info = @typeInfo(elem_type);

    // Check if elements are optional
    if (elem_info == .optional) {
        const inner_elem = elem_info.optional.child;
        return .{
            .element_type = inner_elem,
            .storage_type = listElementStorageType(inner_elem),
            .list_optional = false,
            .element_optional = true,
            .raw_element_type = elem_type,
        };
    }

    return .{
        .element_type = elem_type,
        .storage_type = listElementStorageType(elem_type),
        .list_optional = false,
        .element_optional = false,
        .raw_element_type = elem_type,
    };
}

// =============================================================================
// Flat Field Descriptors
// =============================================================================

/// Context for fields inside a list-of-struct
pub const ListFieldContext = struct {
    /// The list field index in the parent path
    list_path: []const usize,
    /// Path segments up to and including the list field name
    list_path_segments: []const []const u8,
    /// Whether the list itself is optional
    list_optional: bool,
    /// Whether elements are optional
    element_optional: bool,
    /// The original slice type for computing def/rep levels
    slice_type: type,
    /// Path of field indices within the element struct (e.g., [0, 1] for inner.b)
    struct_field_path: []const usize,
    /// Field names for the path within element struct (e.g., ["inner", "b"])
    struct_field_names: []const []const u8,
};

/// A flattened field descriptor - represents a leaf column
pub const FlatField = struct {
    /// Column name (e.g., "address.city") - kept for backwards compatibility
    name: []const u8,
    /// The leaf field type (for list-of-struct, this is the struct field type)
    field_type: type,
    /// Path of field indices to reach this field from root
    path: []const usize,
    /// Whether any field in the path is optional
    is_optional: bool,
    /// Path segments for hierarchical column metadata (e.g., ["address", "city"])
    path_segments: []const []const u8,
    /// Index into the buffer array (maps to original struct fields)
    buffer_index: usize,
    /// If this field is inside a list-of-struct, the list field info
    list_context: ?ListFieldContext = null,
};

// =============================================================================
// Counting Functions
// =============================================================================

/// Count total leaf fields in a struct type (recursively flattening nested structs)
pub fn countLeafFields(comptime T: type) usize {
    const info = @typeInfo(T);
    if (info != .@"struct") return 1;

    // Check if this is a wrapper type (not nested)
    if (@hasDecl(T, "parquet_logical_type")) return 1;

    const fields = std.meta.fields(T);
    var count: usize = 0;
    for (fields) |field| {
        count += countLeafFieldsForField(field.type);
    }
    return count;
}

/// Count leaf fields for a single field type
pub fn countLeafFieldsForField(comptime FieldType: type) usize {
    const Unwrapped = unwrapOptionalType(FieldType);

    // Check if this is a slice/list type
    if (isSliceType(Unwrapped)) {
        const ElemType = SliceElementType(Unwrapped);
        const UnwrappedElem = unwrapOptionalType(ElemType);
        // If list element is a nested struct, each field becomes a leaf column
        if (isNestedStruct(UnwrappedElem)) {
            return countLeafFields(UnwrappedElem);
        }
        return 1; // Primitive list = 1 leaf
    }

    // Check for nested struct
    if (isNestedStruct(Unwrapped)) {
        return countLeafFields(Unwrapped);
    }

    // Primitive or wrapper type = 1 leaf
    return 1;
}

/// Count buffer slots needed (one per original struct field, flattening nested structs only)
/// This differs from countLeafFields in that list-of-struct still gets one buffer slot
pub fn countBufferFields(comptime T: type) usize {
    const info = @typeInfo(T);
    if (info != .@"struct") return 1;

    // Check if this is a wrapper type (not nested)
    if (@hasDecl(T, "parquet_logical_type")) return 1;

    const fields = std.meta.fields(T);
    var count: usize = 0;
    for (fields) |field| {
        const FieldType = unwrapOptionalType(field.type);
        if (isNestedStruct(FieldType)) {
            count += countBufferFields(FieldType);
        } else {
            // Slices (including list-of-struct) get one buffer slot
            count += 1;
        }
    }
    return count;
}

/// Count schema elements for a struct's fields (not including the struct GROUP itself)
pub fn countSchemaElementsForType(comptime T: type) usize {
    const info = @typeInfo(T);
    if (info != .@"struct") {
        @compileError("countSchemaElementsForType expects a struct type");
    }

    var count: usize = 0;
    const fields = std.meta.fields(T);
    for (fields) |field| {
        count += countSchemaElementsForField(field.type);
    }
    return count;
}

/// Count schema elements for a single field (may be leaf, list, or nested struct)
pub fn countSchemaElementsForField(comptime FieldType: type) usize {
    const info = @typeInfo(FieldType);

    // Handle optional wrapper
    if (info == .optional) {
        return countSchemaElementsForField(info.optional.child);
    }

    // Check for slice/list types (excluding []const u8 which is a string)
    if (isSliceType(FieldType)) {
        const ElemType = SliceElementType(FieldType);
        const UnwrappedElem = unwrapOptionalType(ElemType);

        // Check for nested list (element is itself a slice)
        if (isSliceType(UnwrappedElem)) {
            // Nested list: outer container + outer list + inner container + inner list + element
            return 5;
        }

        if (isMapEntryType(UnwrappedElem)) {
            // Map: container(MAP) + key_value(REPEATED) + key + value = 2 + struct fields
            return 2 + countSchemaElementsForType(UnwrappedElem);
        }

        if (isNestedStruct(UnwrappedElem)) {
            // List of structs: container + list + element(GROUP) + struct fields
            return 3 + countSchemaElementsForType(UnwrappedElem);
        }
        // Primitive list: container + list + element
        return 3;
    }

    // Check for nested struct - 1 GROUP element + its fields
    if (isNestedStruct(FieldType)) {
        return 1 + countSchemaElementsForType(FieldType);
    }

    // Leaf type
    return 1;
}

/// Count the number of direct (top-level) fields in a struct
pub fn countTopLevelFields(comptime T: type) usize {
    const info = @typeInfo(T);
    if (info != .@"struct") return 0;
    return std.meta.fields(T).len;
}

// =============================================================================
// Field Flattening
// =============================================================================

/// Generate flattened field descriptors for a struct type
pub fn FlattenedFields(comptime T: type) type {
    const leaf_count = countLeafFields(T);

    return struct {
        const fields_array: [leaf_count]FlatField = blk: {
            var result: [leaf_count]FlatField = undefined;
            var idx: usize = 0;
            var buf_idx: usize = 0;
            flattenFieldsImpl(T, leaf_count, "", &[_]usize{}, &[_][]const u8{}, false, &result, &idx, &buf_idx);
            break :blk result;
        };

        pub const count = leaf_count;

        pub fn get(comptime index: usize) FlatField {
            return fields_array[index];
        }

        pub fn getAll() [leaf_count]FlatField {
            return fields_array;
        }
    };
}

/// Recursive helper to flatten struct fields (parameterized by root array size)
fn flattenFieldsImpl(
    comptime T: type,
    comptime root_count: usize,
    comptime prefix: []const u8,
    comptime path: []const usize,
    comptime path_names: []const []const u8,
    comptime parent_optional: bool,
    result: *[root_count]FlatField,
    idx: *usize,
    buf_idx: *usize,
) void {
    const struct_info = @typeInfo(T);
    if (struct_info != .@"struct") {
        @compileError("Expected struct type");
    }

    const struct_fields = std.meta.fields(T);
    inline for (struct_fields, 0..) |field, field_idx| {
        const field_optional = @typeInfo(field.type) == .optional;
        const FieldType = unwrapOptionalType(field.type);
        const is_opt = parent_optional or field_optional;

        // Build the column name (dot-path for backwards compat)
        const name = if (prefix.len == 0)
            field.name
        else
            prefix ++ "." ++ field.name;

        // Build the path indices
        const new_path = path ++ [_]usize{field_idx};

        // Build the path segments (field names)
        const new_path_names = path_names ++ [_][]const u8{field.name};

        if (isNestedStruct(FieldType)) {
            // Recurse into nested struct (pass through root_count, buf_idx continues)
            flattenFieldsImpl(FieldType, root_count, name, new_path, new_path_names, is_opt, result, idx, buf_idx);
        } else if (isSliceType(FieldType)) {
            // Check if list element is a nested struct
            const ElemType = SliceElementType(FieldType);
            const UnwrappedElem = unwrapOptionalType(ElemType);

            // Capture current buffer index for this list field
            const current_buf_idx = buf_idx.*;
            buf_idx.* += 1; // Advance buffer index (one buffer per list field)

            if (isMapEntryType(UnwrappedElem)) {
                // Map field - create a FlatField for key and value
                const slice_info = getSliceInfo(field.type);

                flattenMapInImpl(
                    UnwrappedElem,
                    root_count,
                    name,
                    new_path,
                    new_path_names,
                    current_buf_idx,
                    slice_info.list_optional or is_opt,
                    field.type,
                    result,
                    idx,
                );
            } else if (isNestedStruct(UnwrappedElem)) {
                // List of structs - create a FlatField for each struct field
                const slice_info = getSliceInfo(field.type);

                // Flatten the struct element's fields
                flattenStructInListImpl(
                    UnwrappedElem,
                    root_count,
                    name,
                    new_path,
                    new_path_names,
                    current_buf_idx,
                    slice_info.list_optional or is_opt,
                    slice_info.element_optional,
                    field.type,
                    result,
                    idx,
                );
            } else {
                // Primitive list - single leaf
                result[idx.*] = .{
                    .name = name,
                    .field_type = field.type,
                    .path = new_path,
                    .is_optional = is_opt,
                    .path_segments = new_path_names,
                    .buffer_index = current_buf_idx,
                    .list_context = null,
                };
                idx.* += 1;
            }
        } else {
            // Leaf field
            result[idx.*] = .{
                .name = name,
                .field_type = field.type,
                .path = new_path,
                .is_optional = is_opt,
                .path_segments = new_path_names,
                .buffer_index = buf_idx.*,
                .list_context = null,
            };
            buf_idx.* += 1;
            idx.* += 1;
        }
    }
}

/// Flatten MapEntry fields inside a map (entry point)
/// Maps use "key_value" container instead of "list"/"element", and element_optional=false
fn flattenMapInImpl(
    comptime MapEntryType: type,
    comptime root_count: usize,
    comptime map_prefix: []const u8,
    comptime map_path: []const usize,
    comptime map_path_names: []const []const u8,
    comptime buffer_index: usize,
    comptime map_optional: bool,
    comptime slice_type: type,
    result: *[root_count]FlatField,
    idx: *usize,
) void {
    flattenStructInListRecursive(
        MapEntryType,
        root_count,
        map_prefix ++ ".key_value",
        map_path,
        map_path_names,
        map_path_names ++ [_][]const u8{"key_value"},
        buffer_index,
        map_optional,
        false, // element_optional: map entries are not individually nullable
        slice_type,
        &[_]usize{},
        &[_][]const u8{},
        result,
        idx,
    );
}

/// Flatten struct fields inside a list element (entry point)
fn flattenStructInListImpl(
    comptime StructType: type,
    comptime root_count: usize,
    comptime list_prefix: []const u8,
    comptime list_path: []const usize,
    comptime list_path_names: []const []const u8,
    comptime buffer_index: usize,
    comptime list_optional: bool,
    comptime element_optional: bool,
    comptime slice_type: type,
    result: *[root_count]FlatField,
    idx: *usize,
) void {
    // Start with empty struct field path (we're at the element struct level)
    flattenStructInListRecursive(
        StructType,
        root_count,
        list_prefix ++ ".list.element",
        list_path,
        list_path_names, // Original list path names (e.g., ["points"])
        list_path_names ++ [_][]const u8{ "list", "element" }, // Current path with list structure
        buffer_index,
        list_optional,
        element_optional,
        slice_type,
        &[_]usize{}, // Empty struct field path at start
        &[_][]const u8{}, // Empty struct field names at start
        result,
        idx,
    );
}

/// Recursive helper for flattening struct fields inside a list element
fn flattenStructInListRecursive(
    comptime StructType: type,
    comptime root_count: usize,
    comptime current_prefix: []const u8,
    comptime list_path: []const usize,
    comptime list_path_names: []const []const u8, // Original list field path names
    comptime current_path_names: []const []const u8, // Current path including list structure
    comptime buffer_index: usize,
    comptime list_optional: bool,
    comptime element_optional: bool,
    comptime slice_type: type,
    comptime struct_field_path: []const usize,
    comptime struct_field_names: []const []const u8,
    result: *[root_count]FlatField,
    idx: *usize,
) void {
    const struct_fields = std.meta.fields(StructType);
    inline for (struct_fields, 0..) |struct_field, struct_field_idx| {
        const StructFieldType = unwrapOptionalType(struct_field.type);

        // Build the column name with current path
        const name = current_prefix ++ "." ++ struct_field.name;

        // Build path indices (for legacy compatibility)
        const new_path = list_path ++ [_]usize{struct_field_idx};

        // Build path segments for column metadata
        const new_path_names = current_path_names ++ [_][]const u8{struct_field.name};

        // Build struct field path (path within element struct for value extraction)
        const new_struct_field_path = struct_field_path ++ [_]usize{struct_field_idx};
        const new_struct_field_names = struct_field_names ++ [_][]const u8{struct_field.name};

        if (isNestedStruct(StructFieldType)) {
            // Recurse into nested struct within list element
            flattenStructInListRecursive(
                StructFieldType,
                root_count,
                name,
                list_path,
                list_path_names, // Pass original list path names unchanged
                new_path_names,
                buffer_index,
                list_optional,
                element_optional,
                slice_type,
                new_struct_field_path,
                new_struct_field_names,
                result,
                idx,
            );
        } else {
            // Leaf field inside list element
            const list_ctx = ListFieldContext{
                .list_path = list_path,
                .list_path_segments = list_path_names, // Original path up to list field
                .list_optional = list_optional,
                .element_optional = element_optional,
                .slice_type = slice_type,
                .struct_field_path = new_struct_field_path,
                .struct_field_names = new_struct_field_names,
            };

            result[idx.*] = .{
                .name = name,
                .field_type = struct_field.type,
                .path = new_path,
                .is_optional = @typeInfo(struct_field.type) == .optional,
                .path_segments = new_path_names,
                .buffer_index = buffer_index, // All struct fields share the same buffer (the list buffer)
                .list_context = list_ctx,
            };
            idx.* += 1;
        }
    }
}
