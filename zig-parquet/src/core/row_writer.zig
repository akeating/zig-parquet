//! Row Writer
//!
//! High-level row-oriented API for writing Parquet files.
//! Provides a comptime-generic interface that accepts Zig structs and
//! handles column transposition, buffering, and flushing internally.
//!
//! ## Example
//! ```zig
//! const parquet = @import("parquet");
//!
//! const SensorReading = struct {
//!     sensor_id: i32,
//!     value: i64,
//!     temperature: f64,
//! };
//!
//! var writer = try parquet.RowWriter(SensorReading).init(allocator, file, .{});
//! defer writer.deinit();
//!
//! try writer.writeRows(&readings);
//! try writer.flush();  // Write row group
//! try writer.writeRows(&more_readings);
//! try writer.close();  // Final flush + footer
//! ```

const std = @import("std");
const format = @import("format.zig");
const column_writer = @import("column_writer.zig");
const list_encoder = @import("list_encoder.zig");
const thrift = @import("thrift/mod.zig");
const types = @import("types.zig");
const struct_utils = @import("struct_utils.zig");
const safe = @import("safe.zig");
const write_target = @import("write_target.zig");
const Optional = types.Optional;
const build_options = @import("build_options");
const WriteTarget = write_target.WriteTarget;
const WriteTargetWriter = write_target.WriteTargetWriter;
const BackendCleanup = @import("parquet_reader.zig").BackendCleanup;

// Re-export wrapper types for convenience
pub const Timestamp = types.Timestamp;
pub const TimestampMicros = types.TimestampMicros;
pub const TimestampInt96 = types.TimestampInt96;
pub const Date = types.Date;
pub const Uuid = types.Uuid;
pub const Time = types.Time;
pub const TimeMicros = types.TimeMicros;
pub const TimeUnit = types.TimeUnit;
pub const Int96 = types.Int96;
pub const Interval = types.Interval;

// Import shared utilities from struct_utils
const hasLogicalTypeDecl = struct_utils.hasLogicalTypeDecl;
const isNestedStruct = struct_utils.isNestedStruct;
const isMapEntryType = struct_utils.isMapEntryType;
const isSliceType = struct_utils.isSliceType;
const SliceElementType = struct_utils.SliceElementType;
const SliceInfo = struct_utils.SliceInfo;
const listElementStorageType = struct_utils.listElementStorageType;
const widenListElement = struct_utils.widenListElement;
const getSliceInfo = struct_utils.getSliceInfo;
const FlatField = struct_utils.FlatField;
const ListFieldContext = struct_utils.ListFieldContext;
const countLeafFields = struct_utils.countLeafFields;
const countLeafFieldsForField = struct_utils.countLeafFieldsForField;
const countBufferFields = struct_utils.countBufferFields;
const countSchemaElementsForType = struct_utils.countSchemaElementsForType;
const countSchemaElementsForField = struct_utils.countSchemaElementsForField;
const countTopLevelFields = struct_utils.countTopLevelFields;
const unwrapOptionalType = struct_utils.unwrapOptionalType;
const FlattenedFields = struct_utils.FlattenedFields;
const isOptionalType = struct_utils.isOptionalType;
const isTimestampType = struct_utils.isTimestampType;
const isTimeType = struct_utils.isTimeType;
const isInt96TimestampType = struct_utils.isInt96TimestampType;
const isDecimalType = struct_utils.isDecimalType;

/// Check if a type is a fixed-size byte array (e.g. [16]u8, [12]u8, [4]u8)
fn isFixedByteArray(comptime T: type) bool {
    const info = @typeInfo(T);
    return info == .array and info.array.child == u8;
}

/// Get the inner storage type for a wrapper, or the type itself
fn WrapperInnerType(comptime T: type) type {
    const info = @typeInfo(T);
    if (info == .optional) {
        return ?WrapperInnerType(info.optional.child);
    }

    // Check for parameterized Timestamp/Time types (they have time_unit and value field)
    if (isInt96TimestampType(T)) return i64; // INT96 still uses i64 for buffer storage
    if (isTimestampType(T)) return i64;
    if (isTimeType(T)) return i64;
    if (isDecimalType(T)) return [T.decimal_byte_len]u8;

    // Check for other known wrapper types
    if (T == types.Date) return i32;
    if (T == types.Uuid) return [16]u8;
    if (T == types.Interval) return [12]u8;

    return T;
}

/// Extract the inner value from a wrapper type.
/// Uses if-else chain so Zig eliminates dead branches and doesn't type-check
/// field accesses (e.g. `.value`, `.days`) on types that lack them.
fn unwrapValue(comptime T: type, value: T) WrapperInnerType(T) {
    const info = @typeInfo(T);
    if (info == .optional) {
        if (value) |v| {
            return unwrapValue(info.optional.child, v);
        } else {
            return null;
        }
    }

    if (comptime isInt96TimestampType(T)) {
        return value.value;
    } else if (comptime isTimestampType(T)) {
        return value.value;
    } else if (comptime isTimeType(T)) {
        return value.value;
    } else if (comptime isDecimalType(T)) {
        return value.bytes;
    } else if (T == types.Date) {
        return value.days;
    } else if (T == types.Uuid) {
        return value.bytes;
    } else if (T == types.Interval) {
        return value.toBytes();
    } else {
        return value;
    }
}

/// Per-column encoding override
pub const ColumnEncoding = struct {
    name: []const u8,
    encoding: format.Encoding,
};

/// Options for RowWriter initialization
pub const RowWriterOptions = struct {
    compression: format.CompressionCodec = if (build_options.no_compression) .uncompressed else .zstd,
    /// Maximum size of a data page in bytes. When set, large columns are split
    /// into multiple pages. null = no limit (all values in single page).
    /// Common values: 1MB (1048576), 64KB (65536) for testing.
    max_page_size: ?usize = null,
    /// Enable dictionary encoding for compatible types (strings, integers).
    /// Dictionary encoding is more efficient for columns with few unique values.
    /// Works for both flat columns and list element columns.
    /// Default: true (matches Arrow/parquet-go behavior)
    use_dictionary: bool = true,
    /// Maximum dictionary size in bytes before falling back to PLAIN encoding.
    /// When a dictionary exceeds this limit, the column uses PLAIN instead.
    /// Default: 1MB (matches Arrow). Set to null for unlimited.
    dictionary_size_limit: ?usize = 1024 * 1024,
    /// Threshold ratio for aborting dictionary encoding when data has high cardinality.
    /// Calculated as (unique_values / total_values). If the ratio exceeds this
    /// threshold, the column will fall back to PLAIN encoding.
    /// Default: null (no cardinality fallback). A common value is 0.5 (50% unique).
    dictionary_cardinality_threshold: ?f32 = null,

    /// Value encoding for integer columns (i32, i64).
    /// - .plain: Standard encoding (default when use_dictionary=false)
    /// - .delta_binary_packed: Excellent for monotonic/sequential data (timestamps, IDs)
    /// - .byte_stream_split: Better compression for correlated integer data (Parquet 2024+)
    /// Note: When use_dictionary=true, dictionary encoding takes precedence for integers.
    /// Set use_dictionary=false to use this encoding.
    int_encoding: format.Encoding = .plain,

    /// Value encoding for floating-point columns (f32, f64).
    /// - .plain: Standard encoding (default)
    /// - .byte_stream_split: Better compression for continuous float data (sensors, measurements)
    float_encoding: format.Encoding = .plain,

    /// Value encoding for boolean columns.
    /// - .plain: Standard bit-packed encoding (default, 1 bit per value)
    /// - .rle: RLE/bit-packed hybrid encoding (efficient for runs of same value)
    bool_encoding: format.Encoding = .plain,

    /// Per-column encoding overrides. Takes precedence over int_encoding/float_encoding/bool_encoding.
    /// Columns not listed use the type-based defaults (int_encoding, float_encoding).
    /// Example: .column_encodings = &.{
    ///     .{ .name = "timestamp", .encoding = .delta_binary_packed },
    ///     .{ .name = "temperature", .encoding = .byte_stream_split },
    /// }
    column_encodings: ?[]const ColumnEncoding = null,

    /// Write CRC32 checksums for each page (data integrity validation).
    /// Enabled by default (matches Arrow/PyArrow behavior, negligible overhead).
    /// CRC32 covers the compressed page data per Parquet spec.
    write_page_checksum: bool = true,

    /// Optional key-value metadata to include in the file footer.
    /// Keys should be unique; duplicates will use the last value.
    /// Can also be set/updated via setKeyValueMetadata() before close().
    metadata: ?[]const format.KeyValue = null,
};

/// Metadata for a completed row group
const RowGroupMeta = struct {
    columns: []format.ColumnChunk,
    num_rows: i64,
    total_byte_size: i64,
    file_offset: i64,
};

/// Maps a Zig type to its buffer element type
/// Widens smaller integers to i32/i64 for Parquet storage
/// For optional types, preserves optionality with widened inner type
fn BufferElementType(comptime T: type) type {
    const info = @typeInfo(T);

    // Handle optional types - widen the child type
    if (info == .optional) {
        const ChildBuffer = BufferElementType(info.optional.child);
        return ?ChildBuffer;
    }

    // Handle slice types - store the slice reference directly
    // We don't widen slice elements here; that happens at flush time
    if (isSliceType(T)) {
        return T; // Store the slice as-is
    }

    // Handle wrapper types - use their inner storage type
    if (isInt96TimestampType(T)) return i64; // INT96 stores as i64 nanos in buffer
    if (isTimestampType(T)) return i64;
    if (isTimeType(T)) return i64;
    if (isDecimalType(T)) return [T.decimal_byte_len]u8;
    if (T == types.Date) return i32;
    if (T == types.Uuid) return [16]u8;
    if (T == types.Interval) return [12]u8;
    if (T == f16) return [2]u8;

    // Widen smaller integers to Parquet native types
    return switch (T) {
        i8, i16, u8, u16, u32 => i32,
        u64 => i64,
        else => T,
    };
}

/// Widen a value to its buffer storage type
fn widenValue(comptime T: type, value: T) BufferElementType(T) {
    const info = @typeInfo(T);

    // Handle optional types
    if (info == .optional) {
        if (value) |v| {
            return widenValue(info.optional.child, v);
        } else {
            return null;
        }
    }

    // Handle slice types - store reference as-is
    if (comptime isSliceType(T)) {
        return value;
    }

    // Handle wrapper types - extract inner value
    if (comptime isInt96TimestampType(T)) {
        return value.value;
    } else if (comptime isTimestampType(T)) {
        return value.value;
    } else if (comptime isTimeType(T)) {
        return value.value;
    } else if (comptime isDecimalType(T)) {
        return value.bytes;
    } else if (T == types.Date) {
        return value.days;
    } else if (T == types.Uuid) {
        return value.bytes;
    } else if (T == types.Interval) {
        return value.toBytes();
    } else if (T == f16) {
        return @as([2]u8, @bitCast(value));
    } else {
        // Widen integers
        return switch (T) {
            i8, i16 => @as(i32, value),
            u8, u16 => @as(i32, value),
            u32 => @as(i32, @bitCast(value)),
            u64 => @as(i64, @bitCast(value)),
            else => value,
        };
    }
}

/// Flatten a slice buffer into values + def/rep levels for Parquet list encoding
/// Converts native Zig slice types to the Optional format expected by list_encoder
/// Uses storage_type (widened) for encoding
fn flattenSliceBuffer(
    comptime SliceType: type,
    allocator: std.mem.Allocator,
    slices: []const SliceType,
) !list_encoder.FlattenedList(getSliceInfo(SliceType).storage_type) {
    const slice_info = comptime getSliceInfo(SliceType);
    const ElemType = slice_info.element_type;
    const StorageType = slice_info.storage_type;
    const max_def = comptime slice_info.maxDefLevel();

    // Convert native slices to Optional([]const Optional(StorageType)) format
    var converted = try allocator.alloc(Optional([]const Optional(StorageType)), slices.len);
    defer allocator.free(converted);

    // Temporary storage for element arrays (using StorageType)
    var elem_arrays = try allocator.alloc([]Optional(StorageType), slices.len);
    defer {
        for (elem_arrays) |arr| {
            if (arr.len > 0) allocator.free(arr);
        }
        allocator.free(elem_arrays);
    }

    for (slices, 0..) |slice_val, i| {
        // Handle optional list
        if (comptime slice_info.list_optional) {
            if (slice_val) |inner_slice| {
                // List exists - convert and widen elements
                const elems = try convertElements(ElemType, StorageType, slice_info.element_optional, allocator, inner_slice);
                elem_arrays[i] = elems;
                converted[i] = .{ .value = elems };
            } else {
                // Null list
                elem_arrays[i] = &[_]Optional(StorageType){};
                converted[i] = .{ .null_value = {} };
            }
        } else {
            // Required list - convert and widen elements
            const elems = try convertElements(ElemType, StorageType, slice_info.element_optional, allocator, slice_val);
            elem_arrays[i] = elems;
            converted[i] = .{ .value = elems };
        }
    }

    // Call list_encoder with StorageType
    return list_encoder.flattenList(StorageType, allocator, converted, max_def, 1);
}

/// Flatten a nested slice buffer (list<list<T>>) into values + def/rep levels
/// Uses the innermost storage type for encoding
fn flattenNestedSliceBuffer(
    comptime SliceType: type,
    allocator: std.mem.Allocator,
    slices: []const SliceType,
    max_def_level: u8,
) !list_encoder.FlattenedList(getNestedStorageType(SliceType)) {
    const outer_slice_info = comptime getSliceInfo(SliceType);
    const InnerSliceType = outer_slice_info.element_type;
    const inner_slice_info = comptime getSliceInfo(unwrapOptionalType(InnerSliceType));
    const InnerStorageType = inner_slice_info.storage_type;

    // For nested lists:
    // OuterSliceType = ?[]const ?[]const i32
    // InnerSliceType = ?[]const i32 (the element type of outer)
    // InnerStorageType = i32 (the widened innermost element)

    // Convert to nested Optional format for list_encoder
    var converted = try allocator.alloc(Optional([]const Optional([]const Optional(InnerStorageType))), slices.len);
    defer allocator.free(converted);

    // Temporary storage for inner arrays
    var inner_list_arrays = try allocator.alloc([]Optional([]const Optional(InnerStorageType)), slices.len);
    defer {
        for (inner_list_arrays) |arr| {
            if (arr.len > 0) {
                for (arr) |inner| {
                    if (inner == .value and inner.value.len > 0) {
                        allocator.free(inner.value);
                    }
                }
                allocator.free(arr);
            }
        }
        allocator.free(inner_list_arrays);
    }

    for (slices, 0..) |outer_slice_val, i| {
        // Handle optional outer list
        if (comptime outer_slice_info.list_optional) {
            if (outer_slice_val) |inner_slices| {
                // Outer list exists - convert inner slices
                const inner_arr = try convertInnerSlices(InnerSliceType, InnerStorageType, inner_slice_info, allocator, inner_slices);
                inner_list_arrays[i] = inner_arr;
                converted[i] = .{ .value = inner_arr };
            } else {
                // Outer list is null
                inner_list_arrays[i] = &[_]Optional([]const Optional(InnerStorageType)){};
                converted[i] = .{ .null_value = {} };
            }
        } else {
            // Required outer list
            const inner_arr = try convertInnerSlices(InnerSliceType, InnerStorageType, inner_slice_info, allocator, outer_slice_val);
            inner_list_arrays[i] = inner_arr;
            converted[i] = .{ .value = inner_arr };
        }
    }

    // Call nested list encoder
    return list_encoder.flattenNestedList(InnerStorageType, allocator, converted, max_def_level, 2, inner_slice_info.element_optional);
}

/// Helper to get the innermost storage type for nested lists
fn getNestedStorageType(comptime SliceType: type) type {
    const outer_info = getSliceInfo(SliceType);
    const inner_info = getSliceInfo(unwrapOptionalType(outer_info.element_type));
    return inner_info.storage_type;
}

/// Convert inner slices to Optional format for nested list encoding
fn convertInnerSlices(
    comptime _: type, // InnerSliceType - unused but kept for API clarity
    comptime InnerStorageType: type,
    comptime inner_slice_info: SliceInfo,
    allocator: std.mem.Allocator,
    inner_slices: anytype,
) ![]Optional([]const Optional(InnerStorageType)) {
    if (inner_slices.len == 0) {
        return &[_]Optional([]const Optional(InnerStorageType)){};
    }

    var result = try allocator.alloc(Optional([]const Optional(InnerStorageType)), inner_slices.len);
    errdefer allocator.free(result);

    for (inner_slices, 0..) |inner_slice_val, j| {
        if (comptime inner_slice_info.list_optional) {
            if (inner_slice_val) |elements| {
                // Inner list exists
                const elem_arr = try convertElements(
                    inner_slice_info.element_type,
                    InnerStorageType,
                    inner_slice_info.element_optional,
                    allocator,
                    elements,
                );
                result[j] = .{ .value = elem_arr };
            } else {
                // Inner list is null
                result[j] = .{ .null_value = {} };
            }
        } else {
            // Required inner list
            const elem_arr = try convertElements(
                inner_slice_info.element_type,
                InnerStorageType,
                inner_slice_info.element_optional,
                allocator,
                inner_slice_val,
            );
            result[j] = .{ .value = elem_arr };
        }
    }

    return result;
}

/// Convert slice elements to Optional format with widening
/// ElemType is the original element type, StorageType is the widened type
fn convertElements(
    comptime ElemType: type,
    comptime StorageType: type,
    comptime elem_optional: bool,
    allocator: std.mem.Allocator,
    slice: anytype,
) ![]Optional(StorageType) {
    if (slice.len == 0) {
        return &[_]Optional(StorageType){};
    }

    var result = try allocator.alloc(Optional(StorageType), slice.len);
    errdefer allocator.free(result);

    for (slice, 0..) |elem, i| {
        if (comptime elem_optional) {
            // Elements are ?T - check for null
            if (elem) |v| {
                result[i] = .{ .value = widenListElement(ElemType, v) };
            } else {
                result[i] = .{ .null_value = {} };
            }
        } else {
            // Elements are T - always present
            result[i] = .{ .value = widenListElement(ElemType, elem) };
        }
    }

    return result;
}

/// Maps a Zig type to Parquet physical type
pub fn zigTypeToParquet(comptime T: type) ?format.PhysicalType {
    const info = @typeInfo(T);

    // Handle optional types - extract the child type
    if (info == .optional) {
        return zigTypeToParquet(info.optional.child);
    }

    // Handle wrapper types
    if (isInt96TimestampType(T)) return .int96;
    if (isTimestampType(T)) return .int64;
    if (isTimeType(T)) return .int64;
    if (isDecimalType(T)) return .fixed_len_byte_array;
    if (T == types.Date) return .int32;
    if (T == types.Uuid) return .fixed_len_byte_array;
    if (T == types.Interval) return .fixed_len_byte_array;

    return switch (T) {
        bool => .boolean,
        // Native INT32 types
        i32 => .int32,
        // Smaller integers stored as INT32
        i8, i16, u8, u16, u32 => .int32,
        // Native INT64 types
        i64 => .int64,
        // u64 stored as INT64 (bit reinterpret)
        u64 => .int64,
        f32 => .float,
        f64 => .double,
        f16 => .fixed_len_byte_array,
        []const u8 => .byte_array,
        else => null,
    };
}

/// Maps a Zig type to Parquet logical type (for INT annotations and wrapper types)
/// Returns null for types that don't need logical type annotations
pub fn zigTypeToLogicalType(comptime T: type) ?format.LogicalType {
    const info = @typeInfo(T);

    // Handle optional types - extract the child type
    if (info == .optional) {
        return zigTypeToLogicalType(info.optional.child);
    }

    // Handle wrapper types with parquet_logical_type decl
    // Only check @hasDecl on struct types
    if (info == .@"struct") {
        if (@hasDecl(T, "parquet_logical_type")) {
            return T.parquet_logical_type;
        }
    }

    // Smaller integers and u64 need INT logical type annotations
    // i32 and i64 are native Parquet types and don't need annotations
    return switch (T) {
        i8 => format.logical_types.int(8, true),
        i16 => format.logical_types.int(16, true),
        u8 => format.logical_types.int(8, false),
        u16 => format.logical_types.int(16, false),
        u32 => format.logical_types.int(32, false),
        u64 => format.logical_types.int(64, false),
        f16 => .float16,
        []const u8 => .string,
        else => null,
    };
}

/// Maps a Zig type to Parquet type_length (for FIXED_LEN_BYTE_ARRAY)
/// Returns null for types that don't have a fixed length
fn zigTypeToTypeLength(comptime T: type) ?i32 {
    const info = @typeInfo(T);

    // Handle optional types - extract the child type
    if (info == .optional) {
        return zigTypeToTypeLength(info.optional.child);
    }

    // Decimal(p,s) stored as FIXED_LEN_BYTE_ARRAY(decimalByteLength(p))
    if (comptime isDecimalType(T)) return comptime T.decimal_byte_len;
    // UUID is stored as FIXED_LEN_BYTE_ARRAY(16)
    if (T == types.Uuid) return 16;
    // INTERVAL is stored as FIXED_LEN_BYTE_ARRAY(12)
    if (T == types.Interval) return 12;
    if (T == f16) return 2;

    return null;
}

/// Maps a Zig type to Parquet converted_type (for legacy types like INTERVAL)
/// Returns null for types that don't have a converted_type annotation
fn zigTypeToConvertedType(comptime T: type) ?i32 {
    const info = @typeInfo(T);

    // Handle optional types - extract the child type
    if (info == .optional) {
        return zigTypeToConvertedType(info.optional.child);
    }

    // Handle wrapper types with parquet_converted_type decl
    // Only check @hasDecl on struct types
    if (info == .@"struct") {
        if (@hasDecl(T, "parquet_converted_type")) {
            return T.parquet_converted_type;
        }
    }

    return null;
}

/// Get the underlying type for an optional, or the type itself
fn UnwrapOptional(comptime T: type) type {
    const info = @typeInfo(T);
    if (info == .optional) {
        return info.optional.child;
    }
    return T;
}

/// Generate a tuple type containing ArrayListUnmanaged for each buffer slot
/// Buffer slots correspond to original struct fields (not expanded list-of-struct)
fn BufferTuple(comptime T: type) type {
    const buffer_count = countBufferFields(T);
    var buffer_types: [buffer_count]type = undefined;

    // Build buffer types based on original fields (using BufferFieldTypes helper)
    const BufferFields = BufferFieldTypes(T);
    for (0..buffer_count) |i| {
        buffer_types[i] = std.ArrayListUnmanaged(BufferFields.get(i));
    }

    return std.meta.Tuple(&buffer_types);
}

/// Helper to get buffer element types for each buffer slot
fn BufferFieldTypes(comptime T: type) type {
    const buffer_count = countBufferFields(T);

    return struct {
        const types_array: [buffer_count]type = blk: {
            var result: [buffer_count]type = undefined;
            var idx: usize = 0;
            collectBufferTypes(T, &result, &idx);
            break :blk result;
        };

        pub fn get(comptime index: usize) type {
            return types_array[index];
        }
    };
}

/// Collect buffer element types for a struct
fn collectBufferTypes(comptime T: type, result: anytype, idx: *usize) void {
    const struct_fields = std.meta.fields(T);
    inline for (struct_fields) |field| {
        const FieldType = unwrapOptionalType(field.type);
        if (isNestedStruct(FieldType)) {
            collectBufferTypes(FieldType, result, idx);
        } else {
            result[idx.*] = BufferElementType(field.type);
            idx.* += 1;
        }
    }
}

/// Row-oriented Parquet writer
/// Generic over a struct type T - each field becomes a column
pub fn RowWriter(comptime T: type) type {
    const type_info = @typeInfo(T);
    if (type_info != .@"struct") {
        @compileError("RowWriter requires a struct type");
    }

    const Flattened = FlattenedFields(T);
    const leaf_count = Flattened.count;

    // Validate all leaf fields map to Parquet types
    for (0..leaf_count) |i| {
        const flat_field = Flattened.get(i);
        if (isSliceType(flat_field.field_type)) {
            // Validate slice element type is supported
            const slice_info = getSliceInfo(flat_field.field_type);
            const elem_type = slice_info.element_type;
            const unwrapped_elem = unwrapOptionalType(elem_type);

            // Check for nested list (element is itself a slice)
            if (isSliceType(unwrapped_elem)) {
                // Nested list - validate the innermost element type
                const inner_slice_info = getSliceInfo(unwrapped_elem);
                const inner_storage_type = inner_slice_info.storage_type;
                const is_inner_supported = inner_storage_type == i32 or inner_storage_type == i64 or
                    inner_storage_type == f32 or inner_storage_type == f64 or
                    inner_storage_type == bool or inner_storage_type == []const u8 or
                    comptime isFixedByteArray(inner_storage_type);
                if (!is_inner_supported) {
                    @compileError("Unsupported nested list element type: " ++ @typeName(inner_slice_info.element_type));
                }
            } else {
                const storage_type = slice_info.storage_type;
                // Supported list storage types (after widening/unwrapping)
                // Small integers widen to i32/i64
                // Timestamp/Date/Time unwrap to i32/i64
                // Supported list storage types (after widening/unwrapping)
                const is_supported = storage_type == i32 or storage_type == i64 or
                    storage_type == f32 or storage_type == f64 or
                    storage_type == bool or storage_type == []const u8 or
                    comptime isFixedByteArray(storage_type);
                if (!is_supported) {
                    @compileError("Unsupported list element type: " ++ @typeName(slice_info.element_type) ++ " (storage: " ++ @typeName(storage_type) ++ ").");
                }
            }
        } else if (zigTypeToParquet(flat_field.field_type) == null) {
            @compileError("Unsupported field type: " ++ @typeName(flat_field.field_type));
        }
    }

    const buffer_count = countBufferFields(T);

    return struct {
        const Self = @This();
        const FlatFields = Flattened;

        allocator: std.mem.Allocator,
        target_writer: WriteTargetWriter,
        compression: format.CompressionCodec,
        max_page_size: ?usize,
        use_dictionary: bool,
        dictionary_size_limit: ?usize,
        dictionary_cardinality_threshold: ?f32,
        int_encoding: format.Encoding,
        float_encoding: format.Encoding,
        bool_encoding: format.Encoding,
        column_encodings: ?[]const ColumnEncoding,
        write_page_checksum: bool,

        metadata: std.ArrayListUnmanaged(format.KeyValue),
        buffers: BufferTuple(T),
        row_groups: std.ArrayListUnmanaged(RowGroupMeta),
        current_offset: i64,

        _backend_cleanup: ?BackendCleanup = null,
        _to_owned_slice_fn: ?*const fn (*anyopaque) error{OutOfMemory}![]u8 = null,
        _to_owned_slice_ctx: ?*anyopaque = null,

        /// Initialize a writer with a WriteTarget. The caller manages target lifetime
        /// unless _backend_cleanup is set (by convenience constructors in api/zig/).
        pub fn initWithTarget(
            allocator: std.mem.Allocator,
            target: WriteTarget,
            options: RowWriterOptions,
        ) !Self {
            target.write(format.PARQUET_MAGIC) catch return error.WriteError;

            var buffers: BufferTuple(T) = undefined;
            inline for (0..buffer_count) |i| {
                buffers[i] = .empty;
            }

            var metadata: std.ArrayListUnmanaged(format.KeyValue) = .empty;
            if (options.metadata) |kvs| {
                try metadata.appendSlice(allocator, kvs);
            }

            return Self{
                .allocator = allocator,
                .target_writer = WriteTargetWriter.init(target),
                .compression = options.compression,
                .max_page_size = options.max_page_size,
                .use_dictionary = options.use_dictionary,
                .dictionary_size_limit = options.dictionary_size_limit,
                .dictionary_cardinality_threshold = options.dictionary_cardinality_threshold,
                .int_encoding = options.int_encoding,
                .float_encoding = options.float_encoding,
                .bool_encoding = options.bool_encoding,
                .column_encodings = options.column_encodings,
                .write_page_checksum = options.write_page_checksum,
                .metadata = metadata,
                .buffers = buffers,
                .row_groups = .empty,
                .current_offset = 4,
            };
        }

        fn getWriter(self: *Self) *std.Io.Writer {
            return self.target_writer.writer();
        }

        fn flushWriter(self: *Self) !void {
            self.target_writer.flush() catch return error.WriteError;
        }

        /// Get encoding for a column by name.
        /// Checks column_encodings first, then falls back to type-based defaults.
        fn getColumnEncoding(self: *const Self, column_name: []const u8, comptime BaseType: type) format.Encoding {
            // Check per-column overrides first
            if (self.column_encodings) |encodings| {
                for (encodings) |ce| {
                    if (std.mem.eql(u8, ce.name, column_name)) {
                        return ce.encoding;
                    }
                }
            }
            // Fall back to type-based defaults
            if (BaseType == i32 or BaseType == i64) return self.int_encoding;
            if (BaseType == f32 or BaseType == f64) return self.float_encoding;
            if (BaseType == bool) return self.bool_encoding;
            return .plain;
        }

        /// Clean up resources
        pub fn deinit(self: *Self) void {
            // Free buffers
            inline for (0..buffer_count) |i| {
                // For byte array fields, we need to check if we own the memory
                // In practice, we don't own string data - it's borrowed from the caller
                self.buffers[i].deinit(self.allocator);
            }

            // Free row group metadata
            for (self.row_groups.items) |rg| {
                for (rg.columns) |col| {
                    if (col.meta_data) |md| {
                        self.allocator.free(md.encodings);
                        for (md.path_in_schema) |path| {
                            self.allocator.free(path);
                        }
                        self.allocator.free(md.path_in_schema);
                        // Free statistics if present
                        if (md.statistics) |stats| {
                            if (stats.min) |m| self.allocator.free(m);
                            if (stats.max) |m| self.allocator.free(m);
                            if (stats.min_value) |m| self.allocator.free(m);
                            if (stats.max_value) |m| self.allocator.free(m);
                        }
                        // Free geospatial statistics if present
                        if (md.geospatial_statistics) |geo_stats| {
                            if (geo_stats.geospatial_types) |gt| self.allocator.free(gt);
                        }
                    }
                }
                self.allocator.free(rg.columns);
            }
            self.row_groups.deinit(self.allocator);

            // Free metadata
            self.metadata.deinit(self.allocator);

            if (self._backend_cleanup) |cleanup| {
                cleanup.deinit_fn(cleanup.ptr, self.allocator);
            }
        }

        /// Write a single row
        pub fn writeRow(self: *Self, row: T) !void {
            const rows = [_]T{row};
            try self.writeRows(&rows);
        }

        /// Comptime function to check if this is the primary FlatField for its buffer
        /// (i.e., the first FlatField with this buffer_index)
        fn isPrimaryForBuffer(comptime field_index: usize) bool {
            const buf_idx = FlatFields.get(field_index).buffer_index;
            // Check if any earlier FlatField has the same buffer_index
            inline for (0..field_index) |earlier| {
                if (FlatFields.get(earlier).buffer_index == buf_idx) {
                    return false; // Earlier field is the primary
                }
            }
            return true; // This is the first/primary
        }

        /// Write multiple rows (batch)
        pub fn writeRows(self: *Self, rows: []const T) !void {
            for (rows) |row| {
                inline for (0..leaf_count) |i| {
                    // Only append for primary FlatField of each buffer (comptime check)
                    if (comptime !isPrimaryForBuffer(i)) continue;

                    const flat_field = FlatFields.get(i);
                    const buf_idx = flat_field.buffer_index;

                    // For list-of-struct, use the list path, not the struct field path
                    if (flat_field.list_context) |list_ctx| {
                        const value = extractFieldValue(T, list_ctx.list_path, row);
                        const widened = widenValue(@TypeOf(value), value);
                        try self.buffers[buf_idx].append(self.allocator, widened);
                    } else {
                        const value = extractFieldValue(T, flat_field.path, row);
                        const widened = widenValue(@TypeOf(value), value);
                        try self.buffers[buf_idx].append(self.allocator, widened);
                    }
                }
            }
        }

        /// Extract a field value from a struct using a path of field indices
        fn extractFieldValue(comptime StructType: type, comptime path: []const usize, value: StructType) ExtractedType(StructType, path) {
            if (path.len == 0) {
                @compileError("Empty path");
            }

            const struct_fields = std.meta.fields(StructType);
            const field = struct_fields[path[0]];
            const field_value = @field(value, field.name);

            if (path.len == 1) {
                return field_value;
            } else {
                // Recurse into nested struct
                const FieldType = unwrapOptionalType(field.type);
                if (@typeInfo(field.type) == .optional) {
                    if (field_value) |v| {
                        return extractFieldValue(FieldType, path[1..], v);
                    } else {
                        return null;
                    }
                } else {
                    return extractFieldValue(FieldType, path[1..], field_value);
                }
            }
        }

        /// Compute the return type of extractFieldValue
        fn ExtractedType(comptime StructType: type, comptime path: []const usize) type {
            if (path.len == 0) {
                @compileError("Empty path");
            }

            const struct_fields = std.meta.fields(StructType);
            const field = struct_fields[path[0]];

            if (path.len == 1) {
                return field.type;
            } else {
                const FieldType = unwrapOptionalType(field.type);
                const inner_type = ExtractedType(FieldType, path[1..]);
                // If any field in the path is optional, result is optional
                if (@typeInfo(field.type) == .optional) {
                    return ?unwrapOptionalType(inner_type);
                } else {
                    return inner_type;
                }
            }
        }

        /// Flush current buffers as a row group
        pub fn flush(self: *Self) !void {
            if (self.buffers[0].items.len == 0) {
                return; // Nothing to flush
            }

            const num_rows = self.buffers[0].items.len;
            if (num_rows > std.math.maxInt(i64)) return error.TooManyRows;
            const row_group_offset = self.current_offset;

            // Allocate column chunks array (one per leaf column)
            var column_chunks = try self.allocator.alloc(format.ColumnChunk, leaf_count);
            errdefer self.allocator.free(column_chunks);

            var total_byte_size: i64 = 0;

            // Write each leaf column
            inline for (0..leaf_count) |i| {
                const result = try self.writeColumnFromFlatField(i);
                column_chunks[i] = .{
                    .file_path = null,
                    .file_offset = result.file_offset,
                    .meta_data = result.metadata,
                };
                total_byte_size += result.metadata.total_compressed_size;
                self.current_offset += try safe.castTo(i64, result.total_bytes);
            }

            // Record row group
            try self.row_groups.append(self.allocator, .{
                .columns = column_chunks,
                .num_rows = try safe.castTo(i64, num_rows),
                .total_byte_size = total_byte_size,
                .file_offset = row_group_offset,
            });

            // Clear buffers
            inline for (0..buffer_count) |i| {
                self.buffers[i].clearRetainingCapacity();
            }
        }

        /// Write a single column based on FlatField metadata
        fn writeColumnFromFlatField(
            self: *Self,
            comptime flat_field_index: usize,
        ) !column_writer.ColumnChunkResult {
            const flat_field = FlatFields.get(flat_field_index);
            const path_segments = flat_field.path_segments;
            const buffer_idx = flat_field.buffer_index;

            // Check if this is a list-of-struct column
            const writer = self.getWriter();

            if (flat_field.list_context) |list_ctx| {
                // List-of-struct: extract struct field values from the list buffer
                const result = try writeListOfStructColumn(
                    list_ctx.slice_type,
                    list_ctx.struct_field_path,
                    list_ctx.struct_field_names,
                    flat_field.field_type,
                    self.allocator,
                    writer,
                    path_segments,
                    self.buffers[buffer_idx].items,
                    list_ctx.list_optional,
                    list_ctx.element_optional,
                    self.current_offset,
                    self.compression,
                );
                try self.flushWriter();
                return result;
            }

            // Regular column (not list-of-struct)
            const FieldType = flat_field.field_type;
            const BufferType = BufferElementType(FieldType);
            const values = self.buffers[buffer_idx].items;

            // Get encoding for this column (per-column override or type-based default)
            const column_name = path_segments[path_segments.len - 1];
            const BaseType = if (comptime isOptionalType(FieldType)) UnwrapOptional(BufferType) else BufferType;
            const value_encoding = self.getColumnEncoding(column_name, BaseType);

            // Handle slice types (primitive lists) specially
            if (comptime isSliceType(FieldType)) {
                const result = try writeListColumn(FieldType, self.allocator, writer, path_segments, values, self.current_offset, self.compression, self.use_dictionary, self.dictionary_size_limit, self.dictionary_cardinality_threshold, self.max_page_size, value_encoding);
                try self.flushWriter();
                return result;
            }

            // Handle INT96 timestamp columns (legacy format)
            const UnwrappedFieldType = unwrapOptionalType(FieldType);
            if (comptime isInt96TimestampType(UnwrappedFieldType)) {
                const is_optional = comptime isOptionalType(FieldType);
                const result = try writeInt96Column(
                    self.allocator,
                    writer,
                    path_segments,
                    values,
                    is_optional,
                    self.current_offset,
                    self.compression,
                );
                try self.flushWriter();
                return result;
            }

            // Use comptime type matching to call appropriate writer
            const result = if (comptime isOptionalType(FieldType))
                try writeOptionalColumn(BufferType, self.allocator, writer, path_segments, values, self.current_offset, self.compression, self.max_page_size, self.use_dictionary, self.dictionary_size_limit, self.dictionary_cardinality_threshold, value_encoding)
            else
                try writeNonOptionalColumn(BufferType, self.allocator, writer, path_segments, values, self.current_offset, self.compression, self.max_page_size, self.use_dictionary, self.dictionary_size_limit, self.dictionary_cardinality_threshold, value_encoding);

            try self.flushWriter();
            return result;
        }

        /// Write a column from a list-of-struct field
        fn writeListOfStructColumn(
            comptime SliceType: type,
            comptime struct_field_path: []const usize,
            comptime _: []const []const u8, // struct_field_names - unused, path indices are sufficient
            comptime LeafFieldType: type,
            allocator: std.mem.Allocator,
            writer: *std.Io.Writer,
            comptime path_segments: []const []const u8,
            slices: anytype,
            comptime list_optional: bool,
            comptime element_optional: bool,
            offset: i64,
            compression: format.CompressionCodec,
        ) !column_writer.ColumnChunkResult {
            // Get slice info and struct element type
            const slice_info = comptime getSliceInfo(SliceType);
            const ElemType = slice_info.element_type;
            const UnwrappedElem = unwrapOptionalType(ElemType);

            // Compute max def/rep levels based on Parquet spec:
            // - Required fields contribute 0 to def level
            // - Optional fields contribute 1 to def level
            // - Repeated fields contribute 1 to def level
            // For list<struct<field>>:
            //   list_container (required=0/optional=1) + list (repeated=1) + element (required=0/optional=1) + field
            var max_def: u8 = 1; // Base: repeated list contributes 1
            if (list_optional) max_def += 1; // Optional container contributes 1
            if (element_optional) max_def += 1; // Optional element contributes 1
            // Add def levels for optional structs/fields in the path
            max_def += comptime countOptionalLevelsInPath(UnwrappedElem, struct_field_path);
            const max_rep: u8 = 1;

            // Extract field values and compute levels
            const ValueType = unwrapOptionalType(LeafFieldType);
            var values_list: std.ArrayListUnmanaged(ValueType) = .empty;
            defer values_list.deinit(allocator);
            var def_levels: std.ArrayListUnmanaged(u32) = .empty;
            defer def_levels.deinit(allocator);
            var rep_levels: std.ArrayListUnmanaged(u32) = .empty;
            defer rep_levels.deinit(allocator);

            for (slices) |slice_val| {
                // Handle optional list
                const actual_slice = if (comptime slice_info.list_optional) blk: {
                    if (slice_val) |s| {
                        break :blk s;
                    } else {
                        // Null list: def=0 (optional container is null, nothing below is defined)
                        // rep=0: start of new record (this placeholder represents the row's list column)
                        // This branch only executes when list_optional=true (slice_info.list_optional implies it)
                        try def_levels.append(allocator, 0);
                        try rep_levels.append(allocator, 0);
                        continue;
                    }
                } else slice_val;

                if (actual_slice.len == 0) {
                    // Empty list: container exists but repeated group has no elements
                    // - optional list (max_def includes +1 for optional): def=1 (container defined, list empty)
                    // - required list (no optional contribution): def=0 (list level not repeated)
                    // rep=0: start of new record (this placeholder represents the row's empty list)
                    const def: u32 = if (list_optional) 1 else 0;
                    try def_levels.append(allocator, def);
                    try rep_levels.append(allocator, 0);
                    continue;
                }

                // Process each element in the list
                // Repetition level semantics (per Dremel/Parquet spec):
                //   rep=0: first element of a new record (new row)
                //   rep=1: subsequent element within the same list (repeated at list level)
                for (actual_slice, 0..) |elem, elem_idx| {
                    const rep: u32 = if (elem_idx == 0) 0 else 1;

                    // Handle optional element
                    const actual_elem = if (comptime element_optional) blk: {
                        if (elem) |e| {
                            break :blk e;
                        } else {
                            // Null element: element slot exists but element value is null
                            // def = (list_optional ? 1 : 0) + 1 (repeated) + 0 (element null, not defined)
                            // - optional list with optional element (max_def=3): def=2
                            // - required list with optional element (max_def=2): def=1
                            const null_elem_def: u32 = if (list_optional) 2 else 1;
                            try def_levels.append(allocator, null_elem_def);
                            try rep_levels.append(allocator, rep);
                            continue;
                        }
                    } else elem;

                    // Extract the nested field value using the path
                    const field_val = extractNestedStructField(UnwrappedElem, struct_field_path, actual_elem);

                    // Handle optional field
                    if (comptime @typeInfo(LeafFieldType) == .optional) {
                        if (field_val) |v| {
                            // Value present: def=max_def (all levels fully defined)
                            try values_list.append(allocator, v);
                            try def_levels.append(allocator, max_def);
                        } else {
                            // Null field: element exists but this optional field is null
                            // def=max_def-1 (all levels defined except the final optional field)
                            try def_levels.append(allocator, max_def - 1);
                        }
                    } else {
                        // Required field: value always present, def=max_def
                        try values_list.append(allocator, field_val);
                        try def_levels.append(allocator, max_def);
                    }
                    try rep_levels.append(allocator, rep);
                }
            }

            // Write the column with levels using the full path (no suffix appending)
            // values_list only contains non-null values (nulls encoded via def_levels),
            // so StorageType must match the unwrapped ValueType, not the optional LeafFieldType.
            const StorageType = BufferElementType(ValueType);

            // FLBA types (UUID, Interval, Decimal) need the specialized fixed byte array writer
            if (comptime isFixedByteArray(StorageType)) {
                const fixed_len = comptime @typeInfo(StorageType).array.len;
                const flba_slices = try allocator.alloc([]const u8, values_list.items.len);
                defer allocator.free(flba_slices);
                if (StorageType != ValueType) {
                    // Wrapper type: unwrap to [N]u8 then take slice
                    for (values_list.items, 0..) |v, i| {
                        const bytes = unwrapValue(ValueType, v);
                        const duped = try allocator.alloc(u8, fixed_len);
                        @memcpy(duped, &bytes);
                        flba_slices[i] = duped;
                    }
                } else {
                    for (values_list.items, 0..) |*v, i| {
                        flba_slices[i] = v[0..fixed_len];
                    }
                }
                defer if (StorageType != ValueType) for (flba_slices) |s| allocator.free(s);
                return column_writer.writeColumnChunkFixedByteArrayWithLevelsAndFullPath(
                    allocator, writer, path_segments, flba_slices, fixed_len,
                    def_levels.items, rep_levels.items, max_def, max_rep,
                    offset, compression, .plain,
                );
            } else if (comptime isInt96TimestampType(ValueType)) {
                const nanos = try allocator.alloc(i64, values_list.items.len);
                defer allocator.free(nanos);
                for (values_list.items, 0..) |v, i| {
                    nanos[i] = v.value;
                }
                return column_writer.writeColumnChunkInt96WithLevelsAndFullPath(
                    allocator, writer, path_segments, nanos,
                    def_levels.items, rep_levels.items, max_def, max_rep,
                    offset, compression,
                );
            } else if (StorageType != ValueType) {
                // Wrapper types (Date, Timestamp, Time) need unwrapping to storage representation
                const converted = try allocator.alloc(StorageType, values_list.items.len);
                defer allocator.free(converted);
                for (values_list.items, 0..) |v, i| {
                    converted[i] = unwrapValue(ValueType, v);
                }
                return column_writer.writeColumnChunkWithLevelsAndFullPath(
                    StorageType,
                    allocator,
                    writer,
                    path_segments,
                    converted,
                    def_levels.items,
                    rep_levels.items,
                    max_def,
                    max_rep,
                    offset,
                    compression,
                );
            }

            return column_writer.writeColumnChunkWithLevelsAndFullPath(
                StorageType,
                allocator,
                writer,
                path_segments,
                values_list.items,
                def_levels.items,
                rep_levels.items,
                max_def,
                max_rep,
                offset,
                compression,
            );
        }

        /// Extract a nested field value from a struct using a path of field indices
        fn extractNestedStructField(
            comptime StructType: type,
            comptime field_path: []const usize,
            value: StructType,
        ) NestedFieldType(StructType, field_path) {
            if (field_path.len == 0) {
                @compileError("Empty field path");
            }

            const struct_fields = std.meta.fields(StructType);
            const first_field = struct_fields[field_path[0]];
            const field_val = @field(value, first_field.name);

            if (field_path.len == 1) {
                return field_val;
            } else {
                // Recurse into nested struct
                const FieldType = unwrapOptionalType(first_field.type);
                if (@typeInfo(first_field.type) == .optional) {
                    if (field_val) |v| {
                        return extractNestedStructField(FieldType, field_path[1..], v);
                    } else {
                        return null;
                    }
                } else {
                    return extractNestedStructField(FieldType, field_path[1..], field_val);
                }
            }
        }

        /// Compute the return type for extractNestedStructField
        fn NestedFieldType(comptime StructType: type, comptime field_path: []const usize) type {
            if (field_path.len == 0) {
                @compileError("Empty field path");
            }

            const struct_fields = std.meta.fields(StructType);
            const first_field = struct_fields[field_path[0]];

            if (field_path.len == 1) {
                return first_field.type;
            } else {
                const FieldType = unwrapOptionalType(first_field.type);
                const inner_type = NestedFieldType(FieldType, field_path[1..]);
                // If any field in the path is optional, result is optional
                if (@typeInfo(first_field.type) == .optional) {
                    return ?unwrapOptionalType(inner_type);
                } else {
                    return inner_type;
                }
            }
        }

        /// Count optional levels in a struct field path (for def level calculation)
        fn countOptionalLevelsInPath(comptime StructType: type, comptime field_path: []const usize) u8 {
            if (field_path.len == 0) {
                return 0;
            }

            const struct_fields = std.meta.fields(StructType);
            const first_field = struct_fields[field_path[0]];
            const is_optional: u8 = if (@typeInfo(first_field.type) == .optional) 1 else 0;

            if (field_path.len == 1) {
                return is_optional;
            } else {
                const FieldType = unwrapOptionalType(first_field.type);
                return is_optional + countOptionalLevelsInPath(FieldType, field_path[1..]);
            }
        }

        /// Write a list column
        fn writeListColumn(
            comptime SliceType: type,
            allocator: std.mem.Allocator,
            writer: *std.Io.Writer,
            comptime path_segments: []const []const u8,
            slices: []const SliceType,
            offset: i64,
            compression: format.CompressionCodec,
            use_dictionary: bool,
            dictionary_size_limit: ?usize,
            dictionary_cardinality_threshold: ?f32,
            max_page_size: ?usize,
            value_encoding: format.Encoding,
        ) !column_writer.ColumnChunkResult {
            const slice_info = comptime getSliceInfo(SliceType);
            const ElemType = slice_info.element_type;
            const UnwrappedElem = unwrapOptionalType(ElemType);

            // Check if this is a nested list (element is itself a slice)
            if (comptime isSliceType(UnwrappedElem)) {
                return writeNestedListColumn(SliceType, allocator, writer, path_segments, slices, offset, compression);
            }

            const StorageType = slice_info.storage_type;
            const max_def = comptime slice_info.maxDefLevel();
            const max_rep: u8 = 1; // Single-level lists have rep=1

            // Flatten the slices into values + def/rep levels (uses StorageType)
            var flattened = try flattenSliceBuffer(SliceType, allocator, slices);
            defer flattened.deinit();

            // FLBA types (UUID, Interval, Decimal) need the fixed byte array writer
            if (comptime isFixedByteArray(StorageType)) {
                const fixed_len = comptime @typeInfo(StorageType).array.len;
                const slices_for_write = try allocator.alloc([]const u8, flattened.values.len);
                defer allocator.free(slices_for_write);
                for (flattened.values, 0..) |*v, idx| {
                    slices_for_write[idx] = v[0..fixed_len];
                }
                return column_writer.writeColumnChunkListFixedByteArrayWithPathArrayAndEncoding(
                    allocator,
                    writer,
                    path_segments,
                    slices_for_write,
                    fixed_len,
                    flattened.def_levels,
                    flattened.rep_levels,
                    max_def,
                    max_rep,
                    offset,
                    compression,
                    value_encoding,
                );
            } else if (comptime isInt96TimestampType(ElemType)) {
                return column_writer.writeColumnChunkListInt96WithPathArray(
                    allocator,
                    writer,
                    path_segments,
                    flattened.values,
                    flattened.def_levels,
                    flattened.rep_levels,
                    max_def,
                    max_rep,
                    offset,
                    compression,
                );
            } else if (use_dictionary and (StorageType == i32 or StorageType == i64)) {
                // Dictionary encoding for integer list elements
                return column_writer.writeColumnChunkListDictWithPathArray(
                    StorageType,
                    allocator,
                    writer,
                    path_segments,
                    flattened.values,
                    flattened.def_levels,
                    flattened.rep_levels,
                    max_def,
                    max_rep,
                    offset,
                    compression,
                    dictionary_size_limit,
                    dictionary_cardinality_threshold,
                    max_page_size,
                );
            } else {
                // Generic path for i32, i64, f32, f64, bool, []const u8 - supports multi-page
                return column_writer.writeColumnChunkListWithPathArrayMultiPage(
                    StorageType,
                    allocator,
                    writer,
                    path_segments,
                    flattened.values,
                    flattened.def_levels,
                    flattened.rep_levels,
                    max_def,
                    max_rep,
                    offset,
                    compression,
                    max_page_size,
                );
            }
        }

        /// Write a nested list column (list<list<T>>)
        fn writeNestedListColumn(
            comptime SliceType: type,
            allocator: std.mem.Allocator,
            writer: *std.Io.Writer,
            comptime path_segments: []const []const u8,
            slices: []const SliceType,
            offset: i64,
            compression: format.CompressionCodec,
        ) !column_writer.ColumnChunkResult {
            const InnerStorageType = getNestedStorageType(SliceType);
            const outer_slice_info = comptime getSliceInfo(SliceType);
            const inner_slice_info = comptime getSliceInfo(unwrapOptionalType(outer_slice_info.element_type));
            const max_def: u8 = comptime blk: {
                var d: u8 = 0;
                if (outer_slice_info.list_optional) d += 1; // outer container optional
                d += 1; // outer list (repeated)
                if (outer_slice_info.element_optional) d += 1; // inner container optional
                d += 1; // inner list (repeated)
                if (inner_slice_info.element_optional) d += 1; // leaf element optional
                break :blk d;
            };
            const max_rep: u8 = 2; // Nested lists always have max rep=2

            // Flatten the nested slices into values + def/rep levels
            var flattened = try flattenNestedSliceBuffer(SliceType, allocator, slices, max_def);
            defer flattened.deinit();

            // FLBA types (UUID, Interval, Decimal) need the fixed byte array nested list writer
            if (comptime isFixedByteArray(InnerStorageType)) {
                const fixed_len: usize = comptime @typeInfo(InnerStorageType).array.len;
                const slices_for_write = try allocator.alloc([]const u8, flattened.values.len);
                defer allocator.free(slices_for_write);
                for (flattened.values, 0..) |*v, i| {
                    slices_for_write[i] = v[0..fixed_len];
                }
                return column_writer.writeColumnChunkNestedListFixedByteArrayWithPathArray(
                    allocator,
                    writer,
                    path_segments,
                    slices_for_write,
                    fixed_len,
                    flattened.def_levels,
                    flattened.rep_levels,
                    max_def,
                    max_rep,
                    offset,
                    compression,
                );
            }

            // Write using the nested list writer (adds 4 path elements)
            return column_writer.writeColumnChunkNestedListWithPathArray(
                InnerStorageType,
                allocator,
                writer,
                path_segments,
                flattened.values,
                flattened.def_levels,
                flattened.rep_levels,
                max_def,
                max_rep,
                offset,
                compression,
            );
        }

        /// Write an INT96 column (legacy timestamp format)
        /// The buffer stores i64 nanoseconds, which are encoded as 12-byte INT96 values
        fn writeInt96Column(
            allocator: std.mem.Allocator,
            writer: *std.Io.Writer,
            comptime path_segments: []const []const u8,
            values: anytype, // []const i64 or []const ?i64
            comptime is_optional: bool,
            offset: i64,
            compression: format.CompressionCodec,
        ) !column_writer.ColumnChunkResult {
            // Convert values to Optional(i64) format for the column writer
            const ValType = @TypeOf(values);
            const ElemType = std.meta.Elem(ValType);

            var optional_values: []Optional(i64) = undefined;
            var needs_free = false;

            if (ElemType == i64) {
                // Non-optional values - convert to Optional
                optional_values = try allocator.alloc(Optional(i64), values.len);
                needs_free = true;
                for (values, 0..) |v, i| {
                    optional_values[i] = .{ .value = v };
                }
            } else if (ElemType == ?i64) {
                // Already optional - convert from ?i64 to Optional(i64)
                optional_values = try allocator.alloc(Optional(i64), values.len);
                needs_free = true;
                for (values, 0..) |v, i| {
                    optional_values[i] = if (v) |nanos| .{ .value = nanos } else .null_value;
                }
            } else {
                @compileError("Expected []const i64 or []const ?i64 for INT96 column");
            }
            defer if (needs_free) allocator.free(optional_values);

            return column_writer.writeColumnChunkInt96OptionalWithPathArray(
                allocator,
                writer,
                path_segments,
                optional_values,
                is_optional,
                offset,
                compression,
                true, // write_page_checksum
            );
        }

        /// Write non-optional column
        /// BufferType is the widened storage type (e.g., i32 for i8/i16/u8/u16/u32)
        fn writeNonOptionalColumn(
            comptime BufferType: type,
            allocator: std.mem.Allocator,
            writer: *std.Io.Writer,
            comptime path_segments: []const []const u8,
            values: []const BufferType,
            offset: i64,
            compression: format.CompressionCodec,
            max_page_size: ?usize,
            use_dictionary: bool,
            dictionary_size_limit: ?usize,
            dictionary_cardinality_threshold: ?f32,
            value_encoding: format.Encoding,
        ) !column_writer.ColumnChunkResult {
            // Convert non-optional values to Optional and use unified APIs with is_optional=false
            // This ensures no definition levels are written for required columns
            if (BufferType == []const u8) {
                // Byte arrays - convert to Optional and use unified API
                const optional_values = try convertNonOptionalToOptional([]const u8, allocator, values);
                defer allocator.free(optional_values);
                if (use_dictionary) {
                    return column_writer.writeColumnChunkByteArrayDictOptionalWithPathArray(allocator, writer, path_segments, optional_values, false, offset, compression, dictionary_size_limit, dictionary_cardinality_threshold, max_page_size, true);
                } else {
                    return column_writer.writeColumnChunkByteArrayOptionalWithPathArrayMultiPage(allocator, writer, path_segments, optional_values, false, offset, compression, max_page_size, true);
                }
            } else if (comptime isFixedByteArray(BufferType)) {
                const fixed_len = comptime @typeInfo(BufferType).array.len;
                const slices = try allocator.alloc(Optional([]const u8), values.len);
                defer allocator.free(slices);
                for (values, 0..) |*v, idx| {
                    slices[idx] = Optional([]const u8).from(v[0..fixed_len]);
                }
                return column_writer.writeColumnChunkFixedByteArrayOptionalWithPathArrayAndEncoding(allocator, writer, path_segments, slices, fixed_len, false, offset, compression, value_encoding, true);
            } else if (use_dictionary and (BufferType == i32 or BufferType == i64 or ((BufferType == f32 or BufferType == f64) and value_encoding == .plain))) {
                // Dictionary encoding for integer/float columns - convert to Optional
                const optional_values = try convertNonOptionalToOptional(BufferType, allocator, values);
                defer allocator.free(optional_values);
                return column_writer.writeColumnChunkDictOptionalWithPathArray(BufferType, allocator, writer, path_segments, optional_values, false, offset, compression, dictionary_size_limit, dictionary_cardinality_threshold, max_page_size, true);
            } else if ((BufferType == i32 or BufferType == i64 or BufferType == f32 or BufferType == f64) and value_encoding != .plain) {
                // Use specified encoding - convert to Optional
                const optional_values = try convertNonOptionalToOptional(BufferType, allocator, values);
                defer allocator.free(optional_values);
                return column_writer.writeColumnChunkOptionalWithEncoding(BufferType, allocator, writer, path_segments, optional_values, false, offset, compression, value_encoding, max_page_size, true);
            } else {
                // Generic path for bool, i32, i64, f32, f64 - convert to Optional
                const optional_values = try convertNonOptionalToOptional(BufferType, allocator, values);
                defer allocator.free(optional_values);
                return column_writer.writeColumnChunkOptionalWithPathArrayMultiPage(BufferType, allocator, writer, path_segments, optional_values, false, offset, compression, max_page_size, true);
            }
        }

        /// Write optional column
        /// BufferType is the widened optional storage type (e.g., ?i32 for ?i8)
        fn writeOptionalColumn(
            comptime BufferType: type,
            allocator: std.mem.Allocator,
            writer: *std.Io.Writer,
            comptime path_segments: []const []const u8,
            values: []const BufferType,
            offset: i64,
            compression: format.CompressionCodec,
            max_page_size: ?usize,
            use_dictionary: bool,
            dictionary_size_limit: ?usize,
            dictionary_cardinality_threshold: ?f32,
            value_encoding: format.Encoding,
        ) !column_writer.ColumnChunkResult {
            const BaseType = UnwrapOptional(BufferType);

            // Special cases for types that need different encoding
            // is_optional=true because this function handles nullable columns
            if (BaseType == []const u8) {
                // Byte arrays - convert to Optional and use unified API
                const optional_values = try convertToOptional([]const u8, allocator, values);
                defer allocator.free(optional_values);
                if (use_dictionary) {
                    return column_writer.writeColumnChunkByteArrayDictOptionalWithPathArray(allocator, writer, path_segments, optional_values, true, offset, compression, dictionary_size_limit, dictionary_cardinality_threshold, max_page_size, true);
                } else {
                    return column_writer.writeColumnChunkByteArrayOptionalWithPathArrayMultiPage(allocator, writer, path_segments, optional_values, true, offset, compression, max_page_size, true);
                }
            } else if (comptime isFixedByteArray(BaseType)) {
                const fixed_len = comptime @typeInfo(BaseType).array.len;
                const slices = try allocator.alloc(Optional([]const u8), values.len);
                defer allocator.free(slices);
                for (values, 0..) |*v, idx| {
                    const slice: ?[]const u8 = if (v.*) |*bytes| bytes[0..fixed_len] else null;
                    slices[idx] = Optional([]const u8).from(slice);
                }
                return column_writer.writeColumnChunkFixedByteArrayOptionalWithPathArrayAndEncoding(allocator, writer, path_segments, slices, fixed_len, true, offset, compression, value_encoding, true);
            } else if (use_dictionary and (BaseType == i32 or BaseType == i64 or ((BaseType == f32 or BaseType == f64) and value_encoding == .plain))) {
                // Dictionary encoding for nullable integer/float columns - convert to Optional
                const optional_values = try convertToOptional(BaseType, allocator, values);
                defer allocator.free(optional_values);
                return column_writer.writeColumnChunkDictOptionalWithPathArray(BaseType, allocator, writer, path_segments, optional_values, true, offset, compression, dictionary_size_limit, dictionary_cardinality_threshold, max_page_size, true);
            } else if ((BaseType == i32 or BaseType == i64 or BaseType == f32 or BaseType == f64) and value_encoding != .plain) {
                // Custom encoding for nullable columns (delta_binary_packed for ints, byte_stream_split for floats)
                const optional_values = try convertToOptional(BaseType, allocator, values);
                defer allocator.free(optional_values);
                return column_writer.writeColumnChunkOptionalWithEncoding(BaseType, allocator, writer, path_segments, optional_values, true, offset, compression, value_encoding, max_page_size, true);
            } else {
                // Generic path for bool, i32, i64, f32, f64 - convert to Optional
                const optional_values = try convertToOptional(BaseType, allocator, values);
                defer allocator.free(optional_values);
                return column_writer.writeColumnChunkOptionalWithPathArrayMultiPage(BaseType, allocator, writer, path_segments, optional_values, true, offset, compression, max_page_size, true);
            }
        }

        /// Convert []const ?V to []const Optional(V)
        fn convertToOptional(comptime V: type, allocator: std.mem.Allocator, values: []const ?V) ![]const Optional(V) {
            const result = try allocator.alloc(Optional(V), values.len);
            for (values, 0..) |v, i| {
                result[i] = Optional(V).from(v);
            }
            return result;
        }

        /// Convert []const V to []const Optional(V) (all values present, no nulls)
        fn convertNonOptionalToOptional(comptime V: type, allocator: std.mem.Allocator, values: []const V) ![]const Optional(V) {
            const result = try allocator.alloc(Optional(V), values.len);
            for (values, 0..) |v, i| {
                result[i] = Optional(V).from(v);
            }
            return result;
        }

        /// Set or update a key-value metadata entry.
        /// Keys are assumed to be unique; if the key already exists, its value is updated.
        /// Metadata is written to the file footer when close() is called.
        pub fn setKeyValueMetadata(self: *Self, key: []const u8, value: ?[]const u8) !void {
            // Check for existing key, update if found
            for (self.metadata.items) |*kv| {
                if (std.mem.eql(u8, kv.key, key)) {
                    kv.value = value;
                    return;
                }
            }
            // Otherwise append new entry
            try self.metadata.append(self.allocator, .{ .key = key, .value = value });
        }

        /// Close the writer, flushing any remaining data and writing the footer.
        pub fn close(self: *Self) !void {
            try self.writeFooterToWriter(self.getWriter());
            try self.flushWriter();
            self.target_writer.target.close() catch return error.WriteError;
        }

        /// Get the written buffer data after close (buffer backend only).
        /// Only works when _to_owned_slice_fn is set by convenience constructors.
        pub fn toOwnedSlice(self: *Self) ![]u8 {
            const func = self._to_owned_slice_fn orelse return error.InvalidState;
            return func(self._to_owned_slice_ctx.?) catch return error.OutOfMemory;
        }

        /// Build and serialize the footer metadata. Caller must free the returned bytes.
        fn buildSerializedFooter(self: *Self) ![]u8 {
            // Build schema
            const schema = try self.buildSchema();
            defer self.allocator.free(schema);

            // Calculate total rows
            var total_rows: i64 = 0;
            for (self.row_groups.items) |rg| {
                total_rows = std.math.add(i64, total_rows, rg.num_rows) catch return error.TooManyRows;
            }

            // Build format row groups
            const format_row_groups = try self.allocator.alloc(format.RowGroup, self.row_groups.items.len);
            defer self.allocator.free(format_row_groups);

            for (self.row_groups.items, 0..) |rg, i| {
                format_row_groups[i] = .{
                    .columns = rg.columns,
                    .total_byte_size = rg.total_byte_size,
                    .num_rows = rg.num_rows,
                    .sorting_columns = null,
                    .file_offset = rg.file_offset,
                    .total_compressed_size = rg.total_byte_size,
                    .ordinal = try safe.castTo(i16, i),
                };
            }

            // Build file metadata
            const file_metadata = format.FileMetaData{
                .version = 1,
                .schema = schema,
                .num_rows = total_rows,
                .row_groups = format_row_groups,
                .key_value_metadata = if (self.metadata.items.len > 0) self.metadata.items else null,
                .created_by = "zig-parquet",
            };

            // Serialize footer
            var thrift_writer = thrift.CompactWriter.init(self.allocator);
            defer thrift_writer.deinit();

            try file_metadata.serialize(&thrift_writer);

            // Return a copy of the serialized bytes (caller owns)
            return try self.allocator.dupe(u8, thrift_writer.getWritten());
        }

        /// Internal: write footer to a generic writer
        fn writeFooterToWriter(self: *Self, writer: *std.Io.Writer) !void {
            // Flush any remaining data
            try self.flush();

            const footer_bytes = try self.buildSerializedFooter();
            defer self.allocator.free(footer_bytes);

            try writer.writeAll(footer_bytes);

            var len_buf: [4]u8 = undefined;
            std.mem.writeInt(u32, &len_buf, try safe.castTo(u32, footer_bytes.len), .little);
            try writer.writeAll(&len_buf);

            try writer.writeAll(format.PARQUET_MAGIC);
        }

        // writeFooterToTarget removed — external targets now use writeFooterToWriter
        // via the WriteTargetWriter adapter.

        /// Build the schema for the file metadata
        /// Generates hierarchical schema with GROUP elements for nested structs
        fn buildSchema(self: *Self) ![]format.SchemaElement {
            // Count includes root + all elements for the struct type
            const schema_count = comptime 1 + countSchemaElementsForType(T);
            const schema = try self.allocator.alloc(format.SchemaElement, schema_count);

            // Root element - num_children is the count of top-level fields
            const top_level_count = comptime countTopLevelFields(T);
            schema[0] = .{
                .type_ = null,
                .type_length = null,
                .repetition_type = null,
                .name = "schema",
                .num_children = try safe.castTo(i32, top_level_count),
                .converted_type = null,
                .scale = null,
                .precision = null,
                .field_id = null,
                .logical_type = null,
            };

            // Build schema elements recursively for the struct type
            _ = try buildSchemaForStruct(T, schema, 1, false);

            return schema;
        }

        /// Recursively build schema elements for a struct type
        /// Returns the next available index in the schema array
        fn buildSchemaForStruct(
            comptime StructType: type,
            schema: []format.SchemaElement,
            start_idx: usize,
            comptime parent_optional: bool,
        ) !usize {
            const struct_fields = std.meta.fields(StructType);
            var idx: usize = start_idx;

            inline for (struct_fields) |field| {
                const field_optional = @typeInfo(field.type) == .optional;
                const is_optional = parent_optional or field_optional;
                const FieldType = unwrapOptionalType(field.type);

                if (comptime isSliceType(FieldType)) {
                    const slice_info = comptime getSliceInfo(field.type);
                    const elem_type = slice_info.element_type;
                    const list_optional = slice_info.list_optional or is_optional;
                    const elem_optional = slice_info.element_optional;
                    const UnwrappedElem = unwrapOptionalType(elem_type);

                    if (comptime isMapEntryType(UnwrappedElem)) {
                        // Map type: container(MAP) -> key_value(REPEATED) -> key + value
                        const KeyType = UnwrappedElem.KeyType;
                        const ValueType = UnwrappedElem.ValueType;

                        // Level 1: Container group with MAP converted type
                        schema[idx] = .{
                            .type_ = null,
                            .type_length = null,
                            .repetition_type = if (list_optional) .optional else .required,
                            .name = field.name,
                            .num_children = 1,
                            .converted_type = format.ConvertedType.MAP,
                            .scale = null,
                            .precision = null,
                            .field_id = null,
                            .logical_type = null,
                        };
                        idx += 1;

                        // Level 2: key_value repeated group
                        schema[idx] = .{
                            .type_ = null,
                            .type_length = null,
                            .repetition_type = .repeated,
                            .name = "key_value",
                            .num_children = 2,
                            .converted_type = null,
                            .scale = null,
                            .precision = null,
                            .field_id = null,
                            .logical_type = null,
                        };
                        idx += 1;

                        // Level 3: key (REQUIRED)
                        const key_parquet_type = zigTypeToParquet(KeyType).?;
                        const key_logical_type = zigTypeToLogicalType(KeyType);
                        const key_type_length = zigTypeToTypeLength(KeyType);

                        schema[idx] = .{
                            .type_ = key_parquet_type,
                            .type_length = key_type_length,
                            .repetition_type = .required,
                            .name = "key",
                            .num_children = null,
                            .converted_type = null,
                            .scale = null,
                            .precision = null,
                            .field_id = null,
                            .logical_type = key_logical_type,
                        };
                        idx += 1;

                        // Level 4: value (OPTIONAL)
                        const value_parquet_type = zigTypeToParquet(ValueType).?;
                        const value_logical_type = zigTypeToLogicalType(ValueType);
                        const value_type_length = zigTypeToTypeLength(ValueType);

                        schema[idx] = .{
                            .type_ = value_parquet_type,
                            .type_length = value_type_length,
                            .repetition_type = .optional,
                            .name = "value",
                            .num_children = null,
                            .converted_type = null,
                            .scale = null,
                            .precision = null,
                            .field_id = null,
                            .logical_type = value_logical_type,
                        };
                        idx += 1;
                    } else {
                    // List type
                    // Level 1: Container group with LIST converted type (3 = LIST)
                    schema[idx] = .{
                        .type_ = null,
                        .type_length = null,
                        .repetition_type = if (list_optional) .optional else .required,
                        .name = field.name,
                        .num_children = 1,
                        .converted_type = format.ConvertedType.LIST,
                        .scale = null,
                        .precision = null,
                        .field_id = null,
                        .logical_type = null,
                    };
                    idx += 1;

                    // Level 2: Repeated group "list"
                    schema[idx] = .{
                        .type_ = null,
                        .type_length = null,
                        .repetition_type = .repeated,
                        .name = "list",
                        .num_children = 1,
                        .converted_type = null,
                        .scale = null,
                        .precision = null,
                        .field_id = null,
                        .logical_type = null,
                    };
                    idx += 1;

                    if (comptime isSliceType(UnwrappedElem)) {
                        // Nested list: element is itself a list
                        // 5-level schema: container -> list -> element(container) -> list -> element(primitive)
                        const inner_slice_info = comptime getSliceInfo(elem_type);
                        const inner_elem_type = inner_slice_info.element_type;
                        const inner_elem_optional = inner_slice_info.element_optional;

                        // Level 3: Inner container "element" (GROUP) with LIST converted type
                        schema[idx] = .{
                            .type_ = null,
                            .type_length = null,
                            .repetition_type = if (elem_optional) .optional else .required,
                            .name = "element",
                            .num_children = 1,
                            .converted_type = format.ConvertedType.LIST,
                            .scale = null,
                            .precision = null,
                            .field_id = null,
                            .logical_type = null,
                        };
                        idx += 1;

                        // Level 4: Inner repeated group "list"
                        schema[idx] = .{
                            .type_ = null,
                            .type_length = null,
                            .repetition_type = .repeated,
                            .name = "list",
                            .num_children = 1,
                            .converted_type = null,
                            .scale = null,
                            .precision = null,
                            .field_id = null,
                            .logical_type = null,
                        };
                        idx += 1;

                        // Level 5: Inner element (primitive leaf)
                        const inner_parquet_type = zigTypeToParquet(inner_elem_type).?;
                        const inner_logical_type = zigTypeToLogicalType(inner_elem_type);
                        const inner_type_length = zigTypeToTypeLength(inner_elem_type);

                        schema[idx] = .{
                            .type_ = inner_parquet_type,
                            .type_length = inner_type_length,
                            .repetition_type = if (inner_elem_optional) .optional else .required,
                            .name = "element",
                            .num_children = null,
                            .converted_type = null,
                            .scale = null,
                            .precision = null,
                            .field_id = null,
                            .logical_type = inner_logical_type,
                        };
                        idx += 1;
                    } else if (comptime isNestedStruct(UnwrappedElem)) {
                        // List of structs: element is a GROUP
                        const struct_field_count = comptime countTopLevelFields(UnwrappedElem);
                        schema[idx] = .{
                            .type_ = null,
                            .type_length = null,
                            .repetition_type = if (elem_optional) .optional else .required,
                            .name = "element",
                            .num_children = try safe.castTo(i32, struct_field_count),
                            .converted_type = null,
                            .scale = null,
                            .precision = null,
                            .field_id = null,
                            .logical_type = null,
                        };
                        idx += 1;

                        // Add struct fields under element
                        idx = try buildSchemaForStruct(UnwrappedElem, schema, idx, elem_optional);
                    } else {
                        // Primitive list: element is a leaf
                        // IMPORTANT: Element is ALWAYS OPTIONAL in schema to distinguish
                        // empty lists (def=max-1) from present values (def=max).
                        // Even for []const i32 (non-optional elements), we need this extra
                        // definition level. The writer will simply never write null elements.
                        const elem_parquet_type = zigTypeToParquet(elem_type).?;
                        const elem_logical_type = zigTypeToLogicalType(elem_type);
                        const elem_type_length = zigTypeToTypeLength(elem_type);

                        schema[idx] = .{
                            .type_ = elem_parquet_type,
                            .type_length = elem_type_length,
                            .repetition_type = .optional, // Always optional for proper 3-level encoding
                            .name = "element",
                            .num_children = null,
                            .converted_type = null,
                            .scale = null,
                            .precision = null,
                            .field_id = null,
                            .logical_type = elem_logical_type,
                        };
                        idx += 1;
                    }
                    } // end LIST branch (else of isMapEntryType)
                } else if (comptime isNestedStruct(FieldType)) {
                    // Nested struct - emit GROUP element and recurse
                    const child_count = comptime countTopLevelFields(FieldType);
                    schema[idx] = .{
                        .type_ = null,
                        .type_length = null,
                        .repetition_type = if (is_optional) .optional else .required,
                        .name = field.name,
                        .num_children = try safe.castTo(i32, child_count),
                        .converted_type = null,
                        .scale = null,
                        .precision = null,
                        .field_id = null,
                        .logical_type = null,
                    };
                    idx += 1;

                    // Recurse into nested struct
                    idx = try buildSchemaForStruct(FieldType, schema, idx, is_optional);
                } else {
                    // Simple leaf field
                    const parquet_type = zigTypeToParquet(FieldType).?;
                    const logical_type = zigTypeToLogicalType(FieldType);
                    const type_length = zigTypeToTypeLength(FieldType);
                    const converted_type = zigTypeToConvertedType(FieldType);

                    schema[idx] = .{
                        .type_ = parquet_type,
                        .type_length = type_length,
                        .repetition_type = if (is_optional) .optional else .required,
                        .name = field.name,
                        .num_children = null,
                        .converted_type = converted_type,
                        .scale = null,
                        .precision = null,
                        .field_id = null,
                        .logical_type = logical_type,
                    };
                    idx += 1;
                }
            }

            return idx;
        }

        /// Get the number of rows currently buffered
        pub fn bufferedRowCount(self: *Self) usize {
            return self.buffers[0].items.len;
        }

        /// Get the number of row groups written so far
        pub fn rowGroupCount(self: *Self) usize {
            return self.row_groups.items.len;
        }
    };
}
