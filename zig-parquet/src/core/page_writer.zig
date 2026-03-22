//! Page Writer
//!
//! Creates data pages by combining definition levels with encoded values.
//! For V1 data pages, the format is:
//!   - Definition levels (RLE encoded, with 4-byte length prefix)
//!   - Repetition levels (RLE encoded, with 4-byte length prefix) - not used for flat schemas
//!   - Values (PLAIN encoded)

const std = @import("std");
const format = @import("format.zig");
const types = @import("types.zig");
const rle_encoder = @import("encoding/rle_encoder.zig");
const plain_encoder = @import("encoding/plain_encoder.zig");
const byte_stream_split_encoder = @import("encoding/byte_stream_split_encoder.zig");
const delta_binary_packed_encoder = @import("encoding/delta_binary_packed_encoder.zig");
const delta_length_byte_array_encoder = @import("encoding/delta_length_byte_array_encoder.zig");
const delta_byte_array_encoder = @import("encoding/delta_byte_array_encoder.zig");

const Optional = types.Optional;
const Int96 = types.Int96;

pub const PageWriteError = error{
    OutOfMemory,
    InvalidFixedLength,
    IntegerOverflow,
    ValueTooLarge,
    UnsupportedEncoding,
};

/// Result of writing a data page
pub const DataPageResult = struct {
    /// The raw page data (definition levels + values)
    data: []u8,
    /// Number of values in the page
    num_values: usize,
    /// Number of non-null values
    num_non_null: usize,

    pub fn deinit(self: *DataPageResult, allocator: std.mem.Allocator) void {
        allocator.free(self.data);
    }
};

// =============================================================================
// Byte Array Data Pages (non-generic encoding)
// =============================================================================

/// Write a data page for Optional byte array values (unified API).
/// This is the preferred method - accepts the same Optional type that Reader returns.
pub fn writeDataPageByteArrayOptional(
    allocator: std.mem.Allocator,
    values: []const Optional([]const u8),
    is_optional: bool,
) PageWriteError!DataPageResult {
    return writeDataPageByteArrayOptionalWithEncoding(allocator, values, is_optional, .plain);
}

/// Write a data page for Optional fixed-length byte array values (unified API).
/// This is the preferred method - accepts the same Optional type that Reader returns.
/// Definition levels are written based on is_optional (schema), not data content.
pub fn writeDataPageFixedByteArrayOptional(
    allocator: std.mem.Allocator,
    values: []const Optional([]const u8),
    fixed_len: usize,
    is_optional: bool,
) PageWriteError!DataPageResult {
    return writeDataPageFixedByteArrayOptionalWithEncoding(allocator, values, fixed_len, is_optional, .plain);
}

/// Write a data page for Optional fixed-length byte array values with a specific encoding.
pub fn writeDataPageFixedByteArrayOptionalWithEncoding(
    allocator: std.mem.Allocator,
    values: []const Optional([]const u8),
    fixed_len: usize,
    is_optional: bool,
    encoding: format.Encoding,
) PageWriteError!DataPageResult {
    var non_null_values: std.ArrayList([]const u8) = .empty;
    defer non_null_values.deinit(allocator);

    var is_defined: std.ArrayList(bool) = .empty;
    defer is_defined.deinit(allocator);

    for (values) |v| {
        const defined = v != .null_value;
        try is_defined.append(allocator, defined);
        if (defined) {
            if (v.value.len != fixed_len) return error.InvalidFixedLength;
            try non_null_values.append(allocator, v.value);
        }
    }

    const encoded_values = try encodeFixedByteArraysWithEncoding(allocator, non_null_values.items, fixed_len, encoding);
    defer allocator.free(encoded_values);

    if (!is_optional) {
        const result = try allocator.dupe(u8, encoded_values);
        return .{
            .data = result,
            .num_values = values.len,
            .num_non_null = values.len,
        };
    }
    return combineDefLevelsAndValues(allocator, is_defined.items, encoded_values, values.len);
}

/// Write a data page with Optional(T) values (unified API).
/// This is the preferred method - accepts the same Optional(T) type that Reader returns.
pub fn writeDataPageOptional(
    allocator: std.mem.Allocator,
    comptime T: type,
    values: []const Optional(T),
    is_optional: bool,
) PageWriteError!DataPageResult {
    return writeDataPageOptionalWithEncoding(allocator, T, values, is_optional, .plain);
}

/// Internal generic function to write a typed data page
fn writeDataPageTyped(
    allocator: std.mem.Allocator,
    comptime T: type,
    values: []const T,
    is_defined: ?[]const bool,
    is_optional: bool,
) PageWriteError!DataPageResult {
    // Encode values based on type
    const encoded_values = switch (T) {
        i32 => try plain_encoder.encodeInt32(allocator, values),
        i64 => try plain_encoder.encodeInt64(allocator, values),
        f32 => try plain_encoder.encodeFloat(allocator, values),
        f64 => try plain_encoder.encodeDouble(allocator, values),
        bool => try plain_encoder.encodeBooleans(allocator, values),
        else => @compileError("Unsupported type for writeDataPageTyped: " ++ @typeName(T)),
    };
    defer allocator.free(encoded_values);

    const total_values = if (is_defined) |def| def.len else values.len;

    if (!is_optional) {
        // No definition levels needed
        const result = try allocator.dupe(u8, encoded_values);
        return .{
            .data = result,
            .num_values = values.len,
            .num_non_null = values.len,
        };
    }

    // Create definition levels
    const def_levels = if (is_defined) |def|
        def
    else blk: {
        // All values are defined
        const all_defined = try allocator.alloc(bool, values.len);
        @memset(all_defined, true);
        break :blk all_defined;
    };
    defer if (is_defined == null) allocator.free(def_levels);

    return combineDefLevelsAndValues(allocator, def_levels, encoded_values, total_values);
}

/// Combine definition levels and values into a data page
fn combineDefLevelsAndValues(
    allocator: std.mem.Allocator,
    is_defined: []const bool,
    encoded_values: []const u8,
    total_values: usize,
) PageWriteError!DataPageResult {
    // Encode definition levels with length prefix
    const def_level_data = try rle_encoder.encodeDefLevelsWithLength(allocator, is_defined);
    defer allocator.free(def_level_data);

    // Count non-null values
    var num_non_null: usize = 0;
    for (is_defined) |d| {
        if (d) num_non_null += 1;
    }

    // Combine: def levels + values
    const total_size = def_level_data.len + encoded_values.len;
    const result = try allocator.alloc(u8, total_size);

    @memcpy(result[0..def_level_data.len], def_level_data);
    @memcpy(result[def_level_data.len..], encoded_values);

    return .{
        .data = result,
        .num_values = total_values,
        .num_non_null = num_non_null,
    };
}

/// Write a data page for list columns with definition and repetition levels.
/// This is used for nested types (lists, structs, maps).
pub fn writeDataPageWithLevels(
    allocator: std.mem.Allocator,
    comptime T: type,
    values: []const T,
    def_levels: []const u32,
    rep_levels: []const u32,
    max_def_level: u8,
    max_rep_level: u8,
) PageWriteError!DataPageResult {
    // Encode values based on type
    const encoded_values = switch (T) {
        i32 => try plain_encoder.encodeInt32(allocator, values),
        i64 => try plain_encoder.encodeInt64(allocator, values),
        f32 => try plain_encoder.encodeFloat(allocator, values),
        f64 => try plain_encoder.encodeDouble(allocator, values),
        bool => try plain_encoder.encodeBooleans(allocator, values),
        []const u8 => try plain_encoder.encodeByteArrays(allocator, values),
        else => @compileError("Unsupported type for writeDataPageWithLevels"),
    };
    defer allocator.free(encoded_values);

    // Encode repetition levels with length prefix (if max_rep_level > 0)
    var rep_level_data: ?[]u8 = null;
    if (max_rep_level > 0) {
        rep_level_data = try rle_encoder.encodeLevelsWithLength(allocator, rep_levels, max_rep_level);
    }
    defer if (rep_level_data) |d| allocator.free(d);

    // Encode definition levels with length prefix (if max_def_level > 0)
    // When max_def_level=0, all values are required and no def levels are stored
    var def_level_data: ?[]u8 = null;
    if (max_def_level > 0) {
        def_level_data = try rle_encoder.encodeLevelsWithLength(allocator, def_levels, max_def_level);
    }
    defer if (def_level_data) |d| allocator.free(d);

    // Calculate total size: rep_levels + def_levels + values
    const rep_size = if (rep_level_data) |d| d.len else 0;
    const def_size = if (def_level_data) |d| d.len else 0;
    const total_size = rep_size + def_size + encoded_values.len;
    const result = try allocator.alloc(u8, total_size);

    var offset: usize = 0;

    // Write repetition levels (if any)
    if (rep_level_data) |d| {
        @memcpy(result[offset..][0..d.len], d);
        offset += d.len;
    }

    // Write definition levels (if any)
    if (def_level_data) |d| {
        @memcpy(result[offset..][0..d.len], d);
        offset += d.len;
    }

    // Write values
    @memcpy(result[offset..], encoded_values);

    return .{
        .data = result,
        .num_values = def_levels.len, // Total slots
        .num_non_null = values.len, // Actual values
    };
}

/// Like writeDataPageWithLevels but uses a specified value encoding instead of PLAIN.
pub fn writeDataPageWithLevelsAndEncoding(
    allocator: std.mem.Allocator,
    comptime T: type,
    values: []const T,
    def_levels: []const u32,
    rep_levels: []const u32,
    max_def_level: u8,
    max_rep_level: u8,
    encoding: format.Encoding,
) PageWriteError!DataPageResult {
    const encoded_values = try encodeValuesWithEncoding(allocator, T, values, encoding);
    defer allocator.free(encoded_values);

    var rep_level_data: ?[]u8 = null;
    if (max_rep_level > 0) {
        rep_level_data = try rle_encoder.encodeLevelsWithLength(allocator, rep_levels, max_rep_level);
    }
    defer if (rep_level_data) |d| allocator.free(d);

    var def_level_data: ?[]u8 = null;
    if (max_def_level > 0) {
        def_level_data = try rle_encoder.encodeLevelsWithLength(allocator, def_levels, max_def_level);
    }
    defer if (def_level_data) |d| allocator.free(d);

    const rep_size = if (rep_level_data) |d| d.len else 0;
    const def_size = if (def_level_data) |d| d.len else 0;
    const total_size = rep_size + def_size + encoded_values.len;
    const result = try allocator.alloc(u8, total_size);

    var offset: usize = 0;

    if (rep_level_data) |d| {
        @memcpy(result[offset..][0..d.len], d);
        offset += d.len;
    }

    if (def_level_data) |d| {
        @memcpy(result[offset..][0..d.len], d);
        offset += d.len;
    }

    @memcpy(result[offset..], encoded_values);

    return .{
        .data = result,
        .num_values = def_levels.len,
        .num_non_null = values.len,
    };
}

/// Like writeDataPageWithLevelsByteArray but uses a specified value encoding instead of PLAIN.
pub fn writeDataPageWithLevelsByteArrayWithEncoding(
    allocator: std.mem.Allocator,
    values: []const []const u8,
    def_levels: []const u32,
    rep_levels: []const u32,
    max_def_level: u8,
    max_rep_level: u8,
    encoding: format.Encoding,
) PageWriteError!DataPageResult {
    const encoded_values = try encodeByteArraysWithEncoding(allocator, values, encoding);
    defer allocator.free(encoded_values);

    var rep_level_data: ?[]u8 = null;
    if (max_rep_level > 0) {
        rep_level_data = try rle_encoder.encodeLevelsWithLength(allocator, rep_levels, max_rep_level);
    }
    defer if (rep_level_data) |d| allocator.free(d);

    var def_level_data: ?[]u8 = null;
    if (max_def_level > 0) {
        def_level_data = try rle_encoder.encodeLevelsWithLength(allocator, def_levels, max_def_level);
    }
    defer if (def_level_data) |d| allocator.free(d);

    const rep_size = if (rep_level_data) |d| d.len else 0;
    const def_size = if (def_level_data) |d| d.len else 0;
    const total_size = rep_size + def_size + encoded_values.len;
    const result = try allocator.alloc(u8, total_size);

    var offset: usize = 0;

    if (rep_level_data) |d| {
        @memcpy(result[offset..][0..d.len], d);
        offset += d.len;
    }

    if (def_level_data) |d| {
        @memcpy(result[offset..][0..d.len], d);
        offset += d.len;
    }

    @memcpy(result[offset..], encoded_values);

    return .{
        .data = result,
        .num_values = def_levels.len,
        .num_non_null = values.len,
    };
}

/// Write a data page for byte array values with definition and repetition levels.
/// This is used for nested types (structs, lists) with string/binary fields.
pub fn writeDataPageWithLevelsByteArray(
    allocator: std.mem.Allocator,
    values: []const []const u8,
    def_levels: []const u32,
    rep_levels: []const u32,
    max_def_level: u8,
    max_rep_level: u8,
) PageWriteError!DataPageResult {
    // Encode values as byte arrays (length-prefixed)
    const encoded_values = try plain_encoder.encodeByteArrays(allocator, values);
    defer allocator.free(encoded_values);

    // Encode repetition levels with length prefix (if max_rep_level > 0)
    var rep_level_data: ?[]u8 = null;
    if (max_rep_level > 0) {
        rep_level_data = try rle_encoder.encodeLevelsWithLength(allocator, rep_levels, max_rep_level);
    }
    defer if (rep_level_data) |d| allocator.free(d);

    // Encode definition levels with length prefix (if max_def_level > 0)
    // When max_def_level=0, all values are required and no def levels are stored
    var def_level_data: ?[]u8 = null;
    if (max_def_level > 0) {
        def_level_data = try rle_encoder.encodeLevelsWithLength(allocator, def_levels, max_def_level);
    }
    defer if (def_level_data) |d| allocator.free(d);

    // Calculate total size: rep_levels + def_levels + values
    const rep_size = if (rep_level_data) |d| d.len else 0;
    const def_size = if (def_level_data) |d| d.len else 0;
    const total_size = rep_size + def_size + encoded_values.len;
    const result = try allocator.alloc(u8, total_size);

    var offset: usize = 0;

    // Write repetition levels (if any)
    if (rep_level_data) |d| {
        @memcpy(result[offset..][0..d.len], d);
        offset += d.len;
    }

    // Write definition levels (if any)
    if (def_level_data) |d| {
        @memcpy(result[offset..][0..d.len], d);
        offset += d.len;
    }

    // Write values
    @memcpy(result[offset..], encoded_values);

    return .{
        .data = result,
        .num_values = def_levels.len, // Total slots
        .num_non_null = values.len, // Actual values
    };
}

/// Write a data page for fixed-length byte array values with definition and repetition levels.
/// This is used for nested types (structs, lists) with fixed-length binary fields like UUID.
pub fn writeDataPageWithLevelsFixedByteArray(
    allocator: std.mem.Allocator,
    values: []const []const u8,
    fixed_len: usize,
    def_levels: []const u32,
    rep_levels: []const u32,
    max_def_level: u8,
    max_rep_level: u8,
) PageWriteError!DataPageResult {
    return writeDataPageWithLevelsFixedByteArrayWithEncoding(allocator, values, fixed_len, def_levels, rep_levels, max_def_level, max_rep_level, .plain);
}

/// Write a data page for fixed-length byte array values with definition/repetition levels and a specific encoding.
pub fn writeDataPageWithLevelsFixedByteArrayWithEncoding(
    allocator: std.mem.Allocator,
    values: []const []const u8,
    fixed_len: usize,
    def_levels: []const u32,
    rep_levels: []const u32,
    max_def_level: u8,
    max_rep_level: u8,
    encoding: format.Encoding,
) PageWriteError!DataPageResult {
    const encoded_values = try encodeFixedByteArraysWithEncoding(allocator, values, fixed_len, encoding);
    defer allocator.free(encoded_values);

    // Encode repetition levels with length prefix (if max_rep_level > 0)
    var rep_level_data: ?[]u8 = null;
    if (max_rep_level > 0) {
        rep_level_data = try rle_encoder.encodeLevelsWithLength(allocator, rep_levels, max_rep_level);
    }
    defer if (rep_level_data) |d| allocator.free(d);

    // Encode definition levels with length prefix (if max_def_level > 0)
    var def_level_data: ?[]u8 = null;
    if (max_def_level > 0) {
        def_level_data = try rle_encoder.encodeLevelsWithLength(allocator, def_levels, max_def_level);
    }
    defer if (def_level_data) |d| allocator.free(d);

    // Calculate total size: rep_levels + def_levels + values
    const rep_size = if (rep_level_data) |d| d.len else 0;
    const def_size = if (def_level_data) |d| d.len else 0;
    const total_size = rep_size + def_size + encoded_values.len;
    const result = try allocator.alloc(u8, total_size);

    var write_offset: usize = 0;

    // Write repetition levels (if any)
    if (rep_level_data) |d| {
        @memcpy(result[write_offset..][0..d.len], d);
        write_offset += d.len;
    }

    // Write definition levels (if any)
    if (def_level_data) |d| {
        @memcpy(result[write_offset..][0..d.len], d);
        write_offset += d.len;
    }

    // Write values
    @memcpy(result[write_offset..], encoded_values);

    return .{
        .data = result,
        .num_values = def_levels.len, // Total slots
        .num_non_null = values.len, // Actual values
    };
}

// =============================================================================
// Encoding-aware Data Page Writers
// =============================================================================

/// Write a data page with a specific value encoding
pub fn writeDataPageWithEncoding(
    allocator: std.mem.Allocator,
    comptime T: type,
    values: []const T,
    is_optional: bool,
    encoding: format.Encoding,
) PageWriteError!DataPageResult {
    return writeDataPageTypedWithEncoding(allocator, T, values, null, is_optional, encoding);
}

/// Write a nullable data page with a specific value encoding
pub fn writeDataPageNullableWithEncoding(
    allocator: std.mem.Allocator,
    comptime T: type,
    values: []const ?T,
    encoding: format.Encoding,
) PageWriteError!DataPageResult {
    // Separate into values and null mask
    var non_null_values: std.ArrayList(T) = .empty;
    defer non_null_values.deinit(allocator);

    var is_defined: std.ArrayList(bool) = .empty;
    defer is_defined.deinit(allocator);

    for (values) |v| {
        try is_defined.append(allocator, v != null);
        if (v) |val| {
            try non_null_values.append(allocator, val);
        }
    }

    return writeDataPageTypedWithEncoding(
        allocator,
        T,
        non_null_values.items,
        is_defined.items,
        true,
        encoding,
    );
}

/// Internal function to write a typed data page with encoding
fn writeDataPageTypedWithEncoding(
    allocator: std.mem.Allocator,
    comptime T: type,
    values: []const T,
    is_defined: ?[]const bool,
    is_optional: bool,
    encoding: format.Encoding,
) PageWriteError!DataPageResult {
    // Encode values based on type and encoding
    const encoded_values = try encodeValuesWithEncoding(allocator, T, values, encoding);
    defer allocator.free(encoded_values);

    const total_values = if (is_defined) |def| def.len else values.len;

    if (!is_optional) {
        // No definition levels needed
        const result = try allocator.dupe(u8, encoded_values);
        return .{
            .data = result,
            .num_values = values.len,
            .num_non_null = values.len,
        };
    }

    // Create definition levels
    const def_levels = if (is_defined) |def|
        def
    else blk: {
        // All values are defined
        const all_defined = try allocator.alloc(bool, values.len);
        @memset(all_defined, true);
        break :blk all_defined;
    };
    defer if (is_defined == null) allocator.free(def_levels);

    return combineDefLevelsAndValues(allocator, def_levels, encoded_values, total_values);
}

/// Encode values with the specified encoding
fn encodeValuesWithEncoding(
    allocator: std.mem.Allocator,
    comptime T: type,
    values: []const T,
    encoding: format.Encoding,
) ![]u8 {
    return switch (encoding) {
        .byte_stream_split => blk: {
            if (T == f32) {
                break :blk byte_stream_split_encoder.encodeFloat32(allocator, values) catch return error.OutOfMemory;
            } else if (T == f64) {
                break :blk byte_stream_split_encoder.encodeFloat64(allocator, values) catch return error.OutOfMemory;
            } else if (T == i32) {
                break :blk byte_stream_split_encoder.encodeInt32(allocator, values) catch return error.OutOfMemory;
            } else if (T == i64) {
                break :blk byte_stream_split_encoder.encodeInt64(allocator, values) catch return error.OutOfMemory;
            } else if (T == f16) {
                break :blk byte_stream_split_encoder.encodeFloat16(allocator, values) catch return error.OutOfMemory;
            } else {
                return error.UnsupportedEncoding;
            }
        },
        .delta_binary_packed => blk: {
            if (T == i32) {
                break :blk delta_binary_packed_encoder.encodeInt32(allocator, values) catch return error.OutOfMemory;
            } else if (T == i64) {
                break :blk delta_binary_packed_encoder.encodeInt64(allocator, values) catch return error.OutOfMemory;
            } else {
                return error.UnsupportedEncoding;
            }
        },
        .rle => blk: {
            if (T == bool) {
                break :blk rle_encoder.encodeBooleans(allocator, values) catch return error.OutOfMemory;
            } else {
                return error.UnsupportedEncoding;
            }
        },
        else => try encodePlain(allocator, T, values),
    };
}

/// Encode values using PLAIN encoding
fn encodePlain(allocator: std.mem.Allocator, comptime T: type, values: []const T) ![]u8 {
    return switch (T) {
        i32 => try plain_encoder.encodeInt32(allocator, values),
        i64 => try plain_encoder.encodeInt64(allocator, values),
        f32 => try plain_encoder.encodeFloat(allocator, values),
        f64 => try plain_encoder.encodeDouble(allocator, values),
        bool => try plain_encoder.encodeBooleans(allocator, values),
        else => @compileError("Unsupported type for encodePlain: " ++ @typeName(T)),
    };
}

// =============================================================================
// Unified Optional Data Page Writers (Phase 11 style)
// =============================================================================

/// Write a data page with Optional(T) values and a specific encoding.
/// This is the unified function that handles both nullable and non-nullable cases.
/// The old writeDataPageWithEncoding and writeDataPageNullableWithEncoding are
/// thin wrappers around this function.
/// Note: Always includes definition levels since Optional indicates an optional column.
pub fn writeDataPageOptionalWithEncoding(
    allocator: std.mem.Allocator,
    comptime T: type,
    values: []const Optional(T),
    is_optional: bool,
    encoding: format.Encoding,
) PageWriteError!DataPageResult {
    // Separate into non-null values and definition mask
    var non_null_values: std.ArrayList(T) = .empty;
    defer non_null_values.deinit(allocator);

    var is_defined: std.ArrayList(bool) = .empty;
    defer is_defined.deinit(allocator);

    for (values) |v| {
        const defined = v != .null_value;
        try is_defined.append(allocator, defined);
        if (defined) {
            try non_null_values.append(allocator, v.value);
        }
    }

    // Definition levels based on schema (is_optional), not data
    return writeDataPageTypedWithEncoding(
        allocator,
        T,
        non_null_values.items,
        is_defined.items,
        is_optional,
        encoding,
    );
}

/// Write a byte array data page with a specific encoding
pub fn writeDataPageByteArrayWithEncoding(
    allocator: std.mem.Allocator,
    values: []const []const u8,
    is_optional: bool,
    encoding: format.Encoding,
) PageWriteError!DataPageResult {
    // Encode byte arrays with specified encoding
    const encoded_values = try encodeByteArraysWithEncoding(allocator, values, encoding);
    defer allocator.free(encoded_values);

    if (!is_optional) {
        const result = try allocator.dupe(u8, encoded_values);
        return .{
            .data = result,
            .num_values = values.len,
            .num_non_null = values.len,
        };
    }

    // For optional, create all-defined mask
    const is_defined = try allocator.alloc(bool, values.len);
    defer allocator.free(is_defined);
    @memset(is_defined, true);

    return combineDefLevelsAndValues(allocator, is_defined, encoded_values, values.len);
}

/// Write a nullable byte array data page with a specific encoding
pub fn writeDataPageByteArrayNullableWithEncoding(
    allocator: std.mem.Allocator,
    values: []const ?[]const u8,
    encoding: format.Encoding,
) PageWriteError!DataPageResult {
    var non_null_values: std.ArrayList([]const u8) = .empty;
    defer non_null_values.deinit(allocator);

    var is_defined: std.ArrayList(bool) = .empty;
    defer is_defined.deinit(allocator);

    for (values) |v| {
        try is_defined.append(allocator, v != null);
        if (v) |val| {
            try non_null_values.append(allocator, val);
        }
    }

    // Encode the non-null values with specified encoding
    const encoded_values = try encodeByteArraysWithEncoding(allocator, non_null_values.items, encoding);
    defer allocator.free(encoded_values);

    return combineDefLevelsAndValues(allocator, is_defined.items, encoded_values, values.len);
}

/// Encode byte arrays with the specified encoding
fn encodeByteArraysWithEncoding(
    allocator: std.mem.Allocator,
    values: []const []const u8,
    encoding: format.Encoding,
) ![]u8 {
    return switch (encoding) {
        .delta_length_byte_array => delta_length_byte_array_encoder.encode(allocator, values) catch return error.OutOfMemory,
        .delta_byte_array => delta_byte_array_encoder.encode(allocator, values) catch return error.OutOfMemory,
        else => try plain_encoder.encodeByteArrays(allocator, values),
    };
}

/// Encode fixed-length byte arrays with the specified encoding
fn encodeFixedByteArraysWithEncoding(
    allocator: std.mem.Allocator,
    values: []const []const u8,
    fixed_len: usize,
    encoding: format.Encoding,
) PageWriteError![]u8 {
    return switch (encoding) {
        .byte_stream_split => byte_stream_split_encoder.encodeFixedByteArray(allocator, values, fixed_len) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
            error.InvalidArgument => return error.InvalidFixedLength,
        },
        else => plain_encoder.encodeFixedByteArrays(allocator, values, fixed_len) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
            error.InvalidFixedLength => return error.InvalidFixedLength,
            error.ValueTooLarge => return error.ValueTooLarge,
        },
    };
}

// =============================================================================
// INT96 Data Page Writers (Legacy timestamp format)
// =============================================================================

/// Encode INT96 values from i64 nanoseconds
/// Each value is 12 bytes: 8 bytes nanoseconds within day + 4 bytes Julian day
pub fn encodeInt96Values(allocator: std.mem.Allocator, values: []const i64) ![]u8 {
    const result = try allocator.alloc(u8, values.len * 12);
    errdefer allocator.free(result);

    for (values, 0..) |nanos, i| {
        const int96 = Int96.fromNanos(nanos);
        @memcpy(result[i * 12 ..][0..12], &int96.bytes);
    }

    return result;
}

/// Write a data page for INT96 values (legacy timestamp format)
/// Takes i64 nanoseconds and encodes as 12-byte INT96 values
pub fn writeDataPageInt96(
    allocator: std.mem.Allocator,
    values: []const i64,
    is_optional: bool,
) PageWriteError!DataPageResult {
    // Encode all values as INT96
    const encoded_values = try encodeInt96Values(allocator, values);
    defer allocator.free(encoded_values);

    if (!is_optional) {
        const result = try allocator.dupe(u8, encoded_values);
        return .{
            .data = result,
            .num_values = values.len,
            .num_non_null = values.len,
        };
    }

    // For optional, create all-defined mask
    const is_defined = try allocator.alloc(bool, values.len);
    defer allocator.free(is_defined);
    @memset(is_defined, true);

    return combineDefLevelsAndValues(allocator, is_defined, encoded_values, values.len);
}

/// Write a data page for Optional INT96 values (with nulls)
pub fn writeDataPageInt96Optional(
    allocator: std.mem.Allocator,
    values: []const Optional(i64),
    is_optional: bool,
) PageWriteError!DataPageResult {
    // Separate into non-null values and definition mask
    var non_null_values: std.ArrayList(i64) = .empty;
    defer non_null_values.deinit(allocator);

    var is_defined: std.ArrayList(bool) = .empty;
    defer is_defined.deinit(allocator);

    for (values) |v| {
        const defined = v != .null_value;
        try is_defined.append(allocator, defined);
        if (defined) {
            try non_null_values.append(allocator, v.value);
        }
    }

    // Encode the non-null values as INT96
    const encoded_values = try encodeInt96Values(allocator, non_null_values.items);
    defer allocator.free(encoded_values);

    // Definition levels based on schema (is_optional), not data
    if (!is_optional) {
        const result = try allocator.dupe(u8, encoded_values);
        return .{
            .data = result,
            .num_values = values.len,
            .num_non_null = non_null_values.items.len,
        };
    }
    return combineDefLevelsAndValues(allocator, is_defined.items, encoded_values, values.len);
}

/// Write a data page for INT96 values with explicit definition and repetition levels.
/// Used for list and list-of-struct contexts where def/rep levels encode the nesting structure.
pub fn writeDataPageInt96WithLevels(
    allocator: std.mem.Allocator,
    values: []const i64,
    def_levels: []const u32,
    rep_levels: []const u32,
    max_def_level: u8,
    max_rep_level: u8,
) PageWriteError!DataPageResult {
    const encoded_values = try encodeInt96Values(allocator, values);
    defer allocator.free(encoded_values);

    var rep_level_data: ?[]u8 = null;
    if (max_rep_level > 0) {
        rep_level_data = try rle_encoder.encodeLevelsWithLength(allocator, rep_levels, max_rep_level);
    }
    defer if (rep_level_data) |d| allocator.free(d);

    var def_level_data: ?[]u8 = null;
    if (max_def_level > 0) {
        def_level_data = try rle_encoder.encodeLevelsWithLength(allocator, def_levels, max_def_level);
    }
    defer if (def_level_data) |d| allocator.free(d);

    const rep_size = if (rep_level_data) |d| d.len else 0;
    const def_size = if (def_level_data) |d| d.len else 0;
    const total_size = rep_size + def_size + encoded_values.len;
    const result = try allocator.alloc(u8, total_size);

    var offset: usize = 0;

    if (rep_level_data) |d| {
        @memcpy(result[offset..][0..d.len], d);
        offset += d.len;
    }

    if (def_level_data) |d| {
        @memcpy(result[offset..][0..d.len], d);
        offset += d.len;
    }

    @memcpy(result[offset..][0..encoded_values.len], encoded_values);

    return .{
        .data = result,
        .num_values = def_levels.len,
        .num_non_null = values.len,
    };
}

/// Write a byte array data page with Optional values and a specific encoding.
/// Unified function for byte arrays (Phase 11 style).
/// Definition levels are written based on is_optional (schema), not data content.
pub fn writeDataPageByteArrayOptionalWithEncoding(
    allocator: std.mem.Allocator,
    values: []const Optional([]const u8),
    is_optional: bool,
    encoding: format.Encoding,
) PageWriteError!DataPageResult {
    var non_null_values: std.ArrayList([]const u8) = .empty;
    defer non_null_values.deinit(allocator);

    var is_defined: std.ArrayList(bool) = .empty;
    defer is_defined.deinit(allocator);

    for (values) |v| {
        const defined = v != .null_value;
        try is_defined.append(allocator, defined);
        if (defined) {
            try non_null_values.append(allocator, v.value);
        }
    }

    // Encode the non-null values with specified encoding
    const encoded_values = try encodeByteArraysWithEncoding(allocator, non_null_values.items, encoding);
    defer allocator.free(encoded_values);

    // Definition levels based on schema (is_optional), not data
    if (!is_optional) {
        const result = try allocator.dupe(u8, encoded_values);
        return .{
            .data = result,
            .num_values = values.len,
            .num_non_null = values.len,
        };
    }
    return combineDefLevelsAndValues(allocator, is_defined.items, encoded_values, values.len);
}

// =============================================================================
// Tests
// =============================================================================

test "write data page i32 non-optional" {
    const allocator = std.testing.allocator;

    const values = [_]Optional(i32){
        .{ .value = 1 },
        .{ .value = 2 },
        .{ .value = 3 },
    };
    var result = try writeDataPageOptional(allocator, i32, &values, false);
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), result.num_values);
    try std.testing.expectEqual(@as(usize, 3), result.num_non_null);
    // Data should be 3 * 4 = 12 bytes (no def levels for required columns)
    try std.testing.expectEqual(@as(usize, 12), result.data.len);
}

test "write data page i32 optional with nulls" {
    const allocator = std.testing.allocator;

    const values = [_]Optional(i32){
        .{ .value = 1 },
        .{ .null_value = {} },
        .{ .value = 3 },
    };
    var result = try writeDataPageOptional(allocator, i32, &values, true);
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), result.num_values);
    try std.testing.expectEqual(@as(usize, 2), result.num_non_null);
    // Data should have def levels (4 byte prefix + encoded) + values (2 * 4 = 8 bytes)
    try std.testing.expect(result.data.len > 8);
}

test "write data page byte array optional with nulls" {
    const allocator = std.testing.allocator;

    const values = [_]Optional([]const u8){
        .{ .value = "hello" },
        .{ .null_value = {} },
        .{ .value = "world" },
    };
    var result = try writeDataPageByteArrayOptional(allocator, &values, true);
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), result.num_values);
    try std.testing.expectEqual(@as(usize, 2), result.num_non_null);
}

// =============================================================================
// Tests for Optional page writers (Phase 11 unified API)
// =============================================================================

test "writeDataPageOptional i32 with mixed values" {
    const allocator = std.testing.allocator;

    const values = [_]Optional(i32){
        .{ .value = 1 },
        .{ .null_value = {} },
        .{ .value = 3 },
    };
    var result = try writeDataPageOptional(allocator, i32, &values, true);
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), result.num_values);
    try std.testing.expectEqual(@as(usize, 2), result.num_non_null);
    // Data should have def levels + values
    try std.testing.expect(result.data.len > 8);
}

test "writeDataPageOptional i32 all values - optional column" {
    const allocator = std.testing.allocator;

    const values = [_]Optional(i32){
        .{ .value = 1 },
        .{ .value = 2 },
        .{ .value = 3 },
    };
    // is_optional=true: includes def levels (6 bytes header + 12 bytes values = 18 bytes)
    var result = try writeDataPageOptional(allocator, i32, &values, true);
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), result.num_values);
    try std.testing.expectEqual(@as(usize, 3), result.num_non_null);
    try std.testing.expectEqual(@as(usize, 18), result.data.len);
}

test "writeDataPageOptional i32 all values - required column" {
    const allocator = std.testing.allocator;

    const values = [_]Optional(i32){
        .{ .value = 1 },
        .{ .value = 2 },
        .{ .value = 3 },
    };
    // is_optional=false: no def levels (just 12 bytes values)
    var result = try writeDataPageOptional(allocator, i32, &values, false);
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), result.num_values);
    try std.testing.expectEqual(@as(usize, 3), result.num_non_null);
    try std.testing.expectEqual(@as(usize, 12), result.data.len);
}

test "writeDataPageByteArrayOptional with mixed values" {
    const allocator = std.testing.allocator;

    const values = [_]Optional([]const u8){
        .{ .value = "hello" },
        .{ .null_value = {} },
        .{ .value = "world" },
    };
    var result = try writeDataPageByteArrayOptional(allocator, &values, true);
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), result.num_values);
    try std.testing.expectEqual(@as(usize, 2), result.num_non_null);
}

test "writeDataPageByteArrayOptional all values" {
    const allocator = std.testing.allocator;

    const values = [_]Optional([]const u8){
        .{ .value = "hello" },
        .{ .value = "world" },
    };
    var result = try writeDataPageByteArrayOptional(allocator, &values, true);
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), result.num_values);
    try std.testing.expectEqual(@as(usize, 2), result.num_non_null);
}

test "writeDataPageFixedByteArrayOptional with mixed values" {
    const allocator = std.testing.allocator;

    // UUID-like 16-byte arrays
    const uuid1 = [_]u8{ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
    const uuid2 = [_]u8{ 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20 };

    const values = [_]Optional([]const u8){
        .{ .value = &uuid1 },
        .{ .null_value = {} },
        .{ .value = &uuid2 },
    };
    var result = try writeDataPageFixedByteArrayOptional(allocator, &values, 16, true);
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), result.num_values);
    try std.testing.expectEqual(@as(usize, 2), result.num_non_null);
}

test "writeDataPageFixedByteArrayOptional all values - optional column" {
    const allocator = std.testing.allocator;

    const uuid1 = [_]u8{ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
    const uuid2 = [_]u8{ 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20 };

    const values = [_]Optional([]const u8){
        .{ .value = &uuid1 },
        .{ .value = &uuid2 },
    };
    // is_optional=true: includes def levels (6 bytes header + 32 bytes values = 38 bytes)
    var result = try writeDataPageFixedByteArrayOptional(allocator, &values, 16, true);
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), result.num_values);
    try std.testing.expectEqual(@as(usize, 2), result.num_non_null);
    try std.testing.expectEqual(@as(usize, 38), result.data.len);
}

test "writeDataPageFixedByteArrayOptional all values - required column" {
    const allocator = std.testing.allocator;

    const uuid1 = [_]u8{ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10 };
    const uuid2 = [_]u8{ 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20 };

    const values = [_]Optional([]const u8){
        .{ .value = &uuid1 },
        .{ .value = &uuid2 },
    };
    // is_optional=false: no def levels (just 32 bytes values)
    var result = try writeDataPageFixedByteArrayOptional(allocator, &values, 16, false);
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), result.num_values);
    try std.testing.expectEqual(@as(usize, 2), result.num_non_null);
    try std.testing.expectEqual(@as(usize, 32), result.data.len);
}
