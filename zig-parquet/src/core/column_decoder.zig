//! Column value decoder
//!
//! Decodes column values from Parquet page data, handling:
//! - Definition levels (for nullable columns)
//! - Repetition levels (for repeated/list columns)
//! - Dictionary encoding
//! - PLAIN encoding
//! - Various physical types (bool, i32, i64, f32, f64, byte arrays)

const std = @import("std");
const safe = @import("safe.zig");
const format = @import("format.zig");
const plain = @import("encoding/plain.zig");
const rle = @import("encoding/rle.zig");
const dictionary = @import("encoding/dictionary.zig");
const delta_binary_packed = @import("encoding/delta_binary_packed.zig");
const delta_length_byte_array = @import("encoding/delta_length_byte_array.zig");
const delta_byte_array = @import("encoding/delta_byte_array.zig");
const byte_stream_split = @import("encoding/byte_stream_split.zig");
const types = @import("types.zig");
const value_mod = @import("value.zig");

pub const Optional = types.Optional;
pub const Int96 = types.Int96;
pub const Value = value_mod.Value;

/// Extract and validate bit_width from dictionary-encoded data.
/// Bit width must be 0-31 to fit in u5 for RLE decoding.
fn extractBitWidth(data: []const u8, offset: usize) error{InvalidBitWidth, EndOfData}!u5 {
    if (offset >= data.len) return error.EndOfData;
    const raw = data[offset];
    if (raw > 31) return error.InvalidBitWidth;
    return @truncate(raw); // Safe: validated above
}

/// Safely cast type_length (i32) to usize, validating non-negative.
fn safeTypeLength(type_length: ?i32) error{InvalidTypeLength}!usize {
    const tl = type_length orelse return 0;
    return safe.cast(tl) catch return error.InvalidTypeLength;
}

/// Check if schema indicates a decimal column stored as INT32 or INT64 (not FLBA)
fn isDecimalIntColumn(ctx: DecodeContext) bool {
    if (ctx.schema_elem.type_ != .int32 and ctx.schema_elem.type_ != .int64) return false;
    if (ctx.schema_elem.logical_type) |lt| {
        return lt == .decimal;
    }
    return false;
}

/// Convert a fixed-size integer (read as little-endian bytes) to big-endian []const u8
/// for decimal columns stored as INT32/INT64
fn decodeDecimalInt(allocator: std.mem.Allocator, data: []const u8, data_offset: usize, int_size: usize) ![]const u8 {
    if (data_offset + int_size > data.len) return error.EndOfData;
    const result = try allocator.alloc(u8, int_size);
    errdefer allocator.free(result);
    // Parquet stores ints as little-endian; decimal expects big-endian two's complement
    var j: usize = 0;
    while (j < int_size) : (j += 1) {
        result[j] = data[data_offset + int_size - 1 - j];
    }
    return result;
}

/// Context needed to decode a column
pub const DecodeContext = struct {
    allocator: std.mem.Allocator,
    schema_elem: format.SchemaElement,
    num_values: usize,
    uses_dict: bool,
    string_dict: ?*dictionary.StringDictionary,
    int32_dict: ?*dictionary.Int32Dictionary = null,
    int64_dict: ?*dictionary.Int64Dictionary = null,
    float32_dict: ?*dictionary.Float32Dictionary = null,
    float64_dict: ?*dictionary.Float64Dictionary = null,
    int96_dict: ?*dictionary.Int96Dictionary = null,
    fixed_byte_array_dict: ?*dictionary.FixedByteArrayDictionary = null,
    /// Maximum definition level for this column (defaults to 1 for simple optional columns)
    max_definition_level: u8 = 1,
    /// Maximum repetition level for this column (defaults to 0 for non-repeated columns)
    max_repetition_level: u8 = 0,
    /// Definition level encoding (defaults to RLE)
    def_level_encoding: format.Encoding = .rle,
    /// Repetition level encoding (defaults to RLE)
    rep_level_encoding: format.Encoding = .rle,
    /// Value encoding (defaults to PLAIN)
    value_encoding: format.Encoding = .plain,
};

/// Result of decoding a column - includes values and levels
pub fn DecodeResult(comptime T: type) type {
    return struct {
        values: []Optional(T),
        def_levels: ?[]u32,
        rep_levels: ?[]u32,

        pub fn deinit(self: *@This(), allocator: std.mem.Allocator, is_byte_array: bool) void {
            // Free individual byte arrays if applicable
            if (is_byte_array) {
                for (self.values) |v| {
                    switch (v) {
                        .value => |s| allocator.free(s),
                        .null_value => {},
                    }
                }
            }
            allocator.free(self.values);
            if (self.def_levels) |dl| allocator.free(dl);
            if (self.rep_levels) |rl| allocator.free(rl);
        }
    };
}

/// Decode column values from page data, returning values and levels
pub fn decodeColumnWithLevels(
    comptime T: type,
    ctx: DecodeContext,
    value_data: []const u8,
) !DecodeResult(T) {
    switch (ctx.value_encoding) {
        .delta_binary_packed, .delta_length_byte_array, .delta_byte_array, .byte_stream_split => {
            return decodeColumnWithLevelsAndDeltaEncoding(T, ctx, value_data);
        },
        else => {},
    }

    const is_optional = ctx.schema_elem.repetition_type == .optional;
    const is_fixed_len = ctx.schema_elem.type_ == .fixed_len_byte_array;
    const fixed_len: usize = if (ctx.schema_elem.type_length) |tl| blk: {
        if (tl < 0) return error.InvalidTypeLength;
        break :blk safe.cast(tl) catch return error.InvalidTypeLength;
    } else 0;

    const result = try ctx.allocator.alloc(Optional(T), ctx.num_values);
    errdefer ctx.allocator.free(result);

    var def_levels: ?[]u32 = null;
    var rep_levels: ?[]u32 = null;

    if (is_optional or ctx.max_repetition_level > 0) {
        const levels = try decodeOptionalColumnWithLevels(T, ctx, value_data, result, is_fixed_len, fixed_len);
        def_levels = levels.def_levels;
        rep_levels = levels.rep_levels;
    } else {
        try decodeRequiredColumn(T, ctx, value_data, result, is_fixed_len, fixed_len);
    }

    return .{
        .values = result,
        .def_levels = def_levels,
        .rep_levels = rep_levels,
    };
}

/// Decode column with delta/BSS encoding, returning values and levels
fn decodeColumnWithLevelsAndDeltaEncoding(
    comptime T: type,
    ctx: DecodeContext,
    value_data: []const u8,
) anyerror!DecodeResult(T) {
    const dynamic_result = try decodeColumnDynamicWithValueEncoding(
        ctx.allocator,
        ctx.schema_elem,
        value_data,
        ctx.num_values,
        ctx.max_definition_level,
        ctx.max_repetition_level,
        ctx.uses_dict,
        ctx.string_dict,
        ctx.int32_dict,
        ctx.int64_dict,
        ctx.float32_dict,
        ctx.float64_dict,
        ctx.fixed_byte_array_dict,
        ctx.int96_dict,
        ctx.def_level_encoding,
        ctx.rep_level_encoding,
        ctx.value_encoding,
    );
    defer {
        for (dynamic_result.values) |v| {
            switch (v) {
                .bytes_val => |ba| ctx.allocator.free(ba),
                .fixed_bytes_val => |ba| ctx.allocator.free(ba),
                else => {},
            }
        }
        ctx.allocator.free(dynamic_result.values);
    }

    const result = try ctx.allocator.alloc(Optional(T), ctx.num_values);
    errdefer {
        if (T == []const u8) {
            for (result) |opt| {
                switch (opt) {
                    .value => |v| ctx.allocator.free(v),
                    .null_value => {},
                }
            }
        }
        ctx.allocator.free(result);
    }
    @memset(result, .null_value);

    for (dynamic_result.values, 0..) |v, i| {
        result[i] = try convertDynamicValue(T, v, ctx.allocator);
    }

    return .{
        .values = result,
        .def_levels = dynamic_result.def_levels,
        .rep_levels = dynamic_result.rep_levels,
    };
}

/// Convert a dynamic Value to a typed Optional(T)
fn convertDynamicValue(comptime T: type, v: Value, allocator: std.mem.Allocator) !Optional(T) {
    return switch (v) {
        .null_val => .null_value,
        .int32_val => |val| blk: {
            if (T == i32) {
                break :blk Optional(T).from(val);
            } else if (T == i64) {
                break :blk Optional(T).from(@as(i64, val)); // Widening cast always safe
            } else {
                break :blk .null_value;
            }
        },
        .int64_val => |val| blk: {
            if (T == i64) {
                break :blk Optional(T).from(val);
            } else if (T == i32) {
                // Check if i64 value fits in i32
                if (val > std.math.maxInt(i32) or val < std.math.minInt(i32)) {
                    break :blk .null_value;
                }
                break :blk Optional(T).from(@as(i32, @truncate(val))); // Safe: validated above
            } else {
                break :blk .null_value;
            }
        },
        .float_val => |val| blk: {
            if (T == f32) {
                break :blk Optional(T).from(val);
            } else if (T == f64) {
                break :blk Optional(T).from(@as(f64, @floatCast(val)));
            } else {
                break :blk .null_value;
            }
        },
        .double_val => |val| blk: {
            if (T == f64) {
                break :blk Optional(T).from(val);
            } else if (T == f32) {
                break :blk Optional(T).from(@as(f32, @floatCast(val)));
            } else {
                break :blk .null_value;
            }
        },
        .bytes_val, .fixed_bytes_val => |val| blk: {
            if (T == []const u8) {
                const dup = try allocator.dupe(u8, val);
                break :blk Optional(T).from(dup);
            } else if (comptime @typeInfo(T) == .array and @typeInfo(T).array.child == u8) {
                const len = @typeInfo(T).array.len;
                if (val.len >= len) {
                    break :blk Optional(T).from(val[0..len].*);
                }
                break :blk .null_value;
            } else if (T == f16) {
                if (val.len >= 2) {
                    break :blk Optional(T).from(@as(f16, @bitCast(val[0..2].*)));
                }
                break :blk .null_value;
            } else {
                break :blk .null_value;
            }
        },
        .bool_val => |val| blk: {
            if (T == bool) {
                break :blk Optional(T).from(val);
            } else {
                break :blk .null_value;
            }
        },
        .list_val, .map_val, .struct_val => .null_value,
    };
}

/// Result of decoding levels
const LevelsResult = struct {
    def_levels: ?[]u32,
    rep_levels: ?[]u32,
};

/// Decode a column with OPTIONAL repetition (has definition levels)
/// Also handles repetition levels for nested/repeated columns.
/// Returns the decoded levels for list reconstruction.
fn decodeOptionalColumnWithLevels(
    comptime T: type,
    ctx: DecodeContext,
    value_data: []const u8,
    result: []Optional(T),
    is_fixed_len: bool,
    fixed_len: usize,
) !LevelsResult {
    var data_offset: usize = 0;
    var rep_levels_result: ?[]u32 = null;
    errdefer if (rep_levels_result) |rl| ctx.allocator.free(rl);

    // Read repetition levels first (if present)
    // In Parquet, repetition levels come before definition levels
    if (ctx.max_repetition_level > 0) {
        const rep_bit_width = format.computeBitWidth(ctx.max_repetition_level);

        if (ctx.rep_level_encoding == .bit_packed) {
            // Pure bit-packed: no length prefix
            const rep_bytes = rle.bitPackedSize(ctx.num_values, rep_bit_width);
            const rep_levels_data = try safe.slice(value_data, data_offset, rep_bytes);
            data_offset += rep_bytes;
            rep_levels_result = try rle.decodeBitPackedLevels(ctx.allocator, rep_levels_data, rep_bit_width, ctx.num_values);
        } else {
            // RLE hybrid: has length prefix
            const rep_levels_len = try plain.decodeU32(value_data[data_offset..]);
            data_offset += 4;
            const rep_levels_data = try safe.slice(value_data, data_offset, rep_levels_len);
            data_offset += rep_levels_len;
            rep_levels_result = try rle.decode(ctx.allocator, rep_levels_data, rep_bit_width, ctx.num_values);
        }
    }

    // Read definition levels
    const def_bit_width = format.computeBitWidth(ctx.max_definition_level);
    var def_levels_raw: []u32 = undefined;

    if (ctx.def_level_encoding == .bit_packed) {
        // Pure bit-packed: no length prefix
        const def_bytes = rle.bitPackedSize(ctx.num_values, def_bit_width);
        const def_levels_data = try safe.slice(value_data, data_offset, def_bytes);
        data_offset += def_bytes;
        def_levels_raw = try rle.decodeBitPackedLevels(ctx.allocator, def_levels_data, def_bit_width, ctx.num_values);
    } else {
        // RLE hybrid: has length prefix
        const def_levels_len = try plain.decodeU32(value_data[data_offset..]);
        data_offset += 4;
        const def_levels_data = try safe.slice(value_data, data_offset, def_levels_len);
        data_offset += def_levels_len;
        def_levels_raw = try rle.decodeLevels(ctx.allocator, def_levels_data, def_bit_width, ctx.num_values);
    }
    errdefer ctx.allocator.free(def_levels_raw);

    const values_start = data_offset;

    // Convert to boolean mask: value is present only when def_level == max_def_level
    // For nested types (structs, lists), lower def levels mean a parent is null
    const def_mask = try ctx.allocator.alloc(bool, ctx.num_values);
    defer ctx.allocator.free(def_mask);

    var non_null_count: usize = 0;
    for (def_levels_raw, 0..) |level, i| {
        const is_present = level == ctx.max_definition_level;
        def_mask[i] = is_present;
        if (is_present) non_null_count += 1;
    }

    // The caller has already checked if this page uses dictionary encoding
    // and set uses_dict accordingly (accounting for dictionary fallback)
    if (ctx.uses_dict) {
        // Dictionary encoded values with definition levels
        if (T == []const u8 and is_fixed_len and ctx.fixed_byte_array_dict != null) {
            // Fixed-length byte array with dictionary
            try decodeDictFixedByteArrayWithDefLevels(ctx, value_data, values_start, def_mask, non_null_count, result);
        } else if (T == []const u8 and !is_fixed_len) {
            // Variable-length byte array with dictionary
            try decodeDictStringsWithDefLevels(ctx, value_data, values_start, def_mask, non_null_count, result);
        } else if (T == i32) {
            try decodeDictInt32WithDefLevels(ctx, value_data, values_start, def_mask, non_null_count, result);
        } else if (T == i64) {
            try decodeDictInt64WithDefLevels(ctx, value_data, values_start, def_mask, non_null_count, result);
        } else if (T == f32 and ctx.float32_dict != null) {
            try decodeDictFloat32WithDefLevels(ctx, value_data, values_start, def_mask, non_null_count, result);
        } else if (T == f64 and ctx.float64_dict != null) {
            try decodeDictFloat64WithDefLevels(ctx, value_data, values_start, def_mask, non_null_count, result);
        } else if (T == Int96 and ctx.int96_dict != null) {
            try decodeDictInt96WithDefLevels(ctx, value_data, values_start, def_mask, non_null_count, result);
        } else {
            // Unsupported dictionary type - fall back to PLAIN
            try decodePlainWithDefLevels(T, ctx, value_data, values_start, def_mask, result, is_fixed_len, fixed_len);
        }
    } else {
        // PLAIN encoding with definition levels
        try decodePlainWithDefLevels(T, ctx, value_data, values_start, def_mask, result, is_fixed_len, fixed_len);
    }

    return .{
        .def_levels = def_levels_raw,
        .rep_levels = rep_levels_result,
    };
}

/// Decode a column with OPTIONAL repetition (has definition levels)
/// Legacy version that discards levels.
fn decodeOptionalColumn(
    comptime T: type,
    ctx: DecodeContext,
    value_data: []const u8,
    result: []Optional(T),
    is_fixed_len: bool,
    fixed_len: usize,
) !void {
    const levels = try decodeOptionalColumnWithLevels(T, ctx, value_data, result, is_fixed_len, fixed_len);
    // Discard levels in legacy mode
    if (levels.def_levels) |dl| ctx.allocator.free(dl);
    if (levels.rep_levels) |rl| ctx.allocator.free(rl);
}

/// Decode dictionary-encoded strings with definition levels
fn decodeDictStringsWithDefLevels(
    ctx: DecodeContext,
    value_data: []const u8,
    values_start: usize,
    def_levels: []const bool,
    non_null_count: usize,
    result: []Optional([]const u8),
) !void {
    // After def levels, we have bit_width byte, then RLE-encoded indices
    const bit_width = try extractBitWidth(value_data, values_start);
    const indices_data = value_data[values_start + 1 ..];

    // Decode only non-null indices
    const indices = try rle.decode(ctx.allocator, indices_data, bit_width, non_null_count);
    defer ctx.allocator.free(indices);

    var idx_pos: usize = 0;
    for (0..ctx.num_values) |i| {
        if (def_levels[i]) {
            const dict_idx = indices[idx_pos];
            idx_pos += 1;
            if (ctx.string_dict.?.get(dict_idx)) |v| {
                result[i] = .{ .value = try ctx.allocator.dupe(u8, v) };
            } else {
                result[i] = .{ .value = try ctx.allocator.dupe(u8, "") };
            }
        } else {
            result[i] = .{ .null_value = {} };
        }
    }
}

/// Decode dictionary-encoded i32 values with definition levels
fn decodeDictInt32WithDefLevels(
    ctx: DecodeContext,
    value_data: []const u8,
    values_start: usize,
    def_levels: []const bool,
    non_null_count: usize,
    result: []Optional(i32),
) !void {
    const bit_width = try extractBitWidth(value_data, values_start);
    const indices_data = value_data[values_start + 1 ..];

    const indices = try rle.decode(ctx.allocator, indices_data, bit_width, non_null_count);
    defer ctx.allocator.free(indices);

    var idx_pos: usize = 0;
    for (0..ctx.num_values) |i| {
        if (def_levels[i]) {
            const dict_idx = indices[idx_pos];
            idx_pos += 1;
            if (ctx.int32_dict.?.get(dict_idx)) |v| {
                result[i] = .{ .value = v };
            } else {
                result[i] = .{ .value = 0 };
            }
        } else {
            result[i] = .{ .null_value = {} };
        }
    }
}

/// Decode dictionary-encoded i64 values with definition levels
fn decodeDictInt64WithDefLevels(
    ctx: DecodeContext,
    value_data: []const u8,
    values_start: usize,
    def_levels: []const bool,
    non_null_count: usize,
    result: []Optional(i64),
) !void {
    const bit_width = try extractBitWidth(value_data, values_start);
    const indices_data = value_data[values_start + 1 ..];

    const indices = try rle.decode(ctx.allocator, indices_data, bit_width, non_null_count);
    defer ctx.allocator.free(indices);

    var idx_pos: usize = 0;
    for (0..ctx.num_values) |i| {
        if (def_levels[i]) {
            const dict_idx = indices[idx_pos];
            idx_pos += 1;
            if (ctx.int64_dict.?.get(dict_idx)) |v| {
                result[i] = .{ .value = v };
            } else {
                result[i] = .{ .value = 0 };
            }
        } else {
            result[i] = .{ .null_value = {} };
        }
    }
}

/// Decode dictionary-encoded f32 values with definition levels
fn decodeDictFloat32WithDefLevels(
    ctx: DecodeContext,
    value_data: []const u8,
    values_start: usize,
    def_levels: []const bool,
    non_null_count: usize,
    result: []Optional(f32),
) !void {
    const bit_width = try extractBitWidth(value_data, values_start);
    const indices_data = value_data[values_start + 1 ..];

    const indices = try rle.decode(ctx.allocator, indices_data, bit_width, non_null_count);
    defer ctx.allocator.free(indices);

    var idx_pos: usize = 0;
    for (0..ctx.num_values) |i| {
        if (def_levels[i]) {
            const dict_idx = indices[idx_pos];
            idx_pos += 1;
            if (ctx.float32_dict.?.get(dict_idx)) |v| {
                result[i] = .{ .value = v };
            } else {
                result[i] = .{ .value = 0.0 };
            }
        } else {
            result[i] = .{ .null_value = {} };
        }
    }
}

/// Decode dictionary-encoded f64 values with definition levels
fn decodeDictFloat64WithDefLevels(
    ctx: DecodeContext,
    value_data: []const u8,
    values_start: usize,
    def_levels: []const bool,
    non_null_count: usize,
    result: []Optional(f64),
) !void {
    const bit_width = try extractBitWidth(value_data, values_start);
    const indices_data = value_data[values_start + 1 ..];

    const indices = try rle.decode(ctx.allocator, indices_data, bit_width, non_null_count);
    defer ctx.allocator.free(indices);

    var idx_pos: usize = 0;
    for (0..ctx.num_values) |i| {
        if (def_levels[i]) {
            const dict_idx = indices[idx_pos];
            idx_pos += 1;
            if (ctx.float64_dict.?.get(dict_idx)) |v| {
                result[i] = .{ .value = v };
            } else {
                result[i] = .{ .value = 0.0 };
            }
        } else {
            result[i] = .{ .null_value = {} };
        }
    }
}

/// Decode dictionary-encoded Int96 values with definition levels
fn decodeDictInt96WithDefLevels(
    ctx: DecodeContext,
    value_data: []const u8,
    values_start: usize,
    def_levels: []const bool,
    non_null_count: usize,
    result: []Optional(Int96),
) !void {
    const bit_width = try extractBitWidth(value_data, values_start);
    const indices_data = value_data[values_start + 1 ..];

    const indices = try rle.decode(ctx.allocator, indices_data, bit_width, non_null_count);
    defer ctx.allocator.free(indices);

    var idx_pos: usize = 0;
    for (0..ctx.num_values) |i| {
        if (def_levels[i]) {
            const dict_idx = indices[idx_pos];
            idx_pos += 1;
            if (ctx.int96_dict.?.get(dict_idx)) |bytes| {
                result[i] = .{ .value = Int96.fromBytes(bytes) };
            } else {
                result[i] = .{ .value = Int96.fromBytes([_]u8{0} ** 12) };
            }
        } else {
            result[i] = .{ .null_value = {} };
        }
    }
}

/// Decode dictionary-encoded fixed-length byte arrays with definition levels
fn decodeDictFixedByteArrayWithDefLevels(
    ctx: DecodeContext,
    value_data: []const u8,
    values_start: usize,
    def_levels: []const bool,
    non_null_count: usize,
    result: []Optional([]const u8),
) !void {
    const bit_width = try extractBitWidth(value_data, values_start);
    const indices_data = value_data[values_start + 1 ..];

    const indices = try rle.decode(ctx.allocator, indices_data, bit_width, non_null_count);
    defer ctx.allocator.free(indices);

    var idx_pos: usize = 0;
    for (0..ctx.num_values) |i| {
        if (def_levels[i]) {
            const dict_idx = indices[idx_pos];
            idx_pos += 1;
            if (ctx.fixed_byte_array_dict.?.get(dict_idx)) |v| {
                result[i] = .{ .value = try ctx.allocator.dupe(u8, v) };
            } else {
                result[i] = .{ .null_value = {} };
            }
        } else {
            result[i] = .{ .null_value = {} };
        }
    }
}

/// Decode PLAIN-encoded values with definition levels
fn decodePlainWithDefLevels(
    comptime T: type,
    ctx: DecodeContext,
    value_data: []const u8,
    values_start: usize,
    def_levels: []const bool,
    result: []Optional(T),
    is_fixed_len: bool,
    fixed_len: usize,
) !void {
    var value_pos: usize = 0;
    var data_offset: usize = values_start;

    for (0..ctx.num_values) |i| {
        if (def_levels[i]) {
            // Value is present
            if (T == bool) {
                // Booleans are bit-packed: use value_pos as cumulative bit index
                // decodeBool handles the byte/bit offset calculation internally
                result[i] = .{ .value = try plain.decodeBool(value_data[values_start..], value_pos) };
                value_pos += 1;
            } else if (T == i32) {
                if (data_offset + 4 > value_data.len) return error.EndOfData;
                result[i] = .{ .value = try plain.decodeI32(value_data[data_offset..]) };
                data_offset += 4;
            } else if (T == i64) {
                if (data_offset + 8 > value_data.len) return error.EndOfData;
                result[i] = .{ .value = try plain.decodeI64(value_data[data_offset..]) };
                data_offset += 8;
            } else if (T == f32) {
                if (data_offset + 4 > value_data.len) return error.EndOfData;
                result[i] = .{ .value = try plain.decodeFloat(value_data[data_offset..]) };
                data_offset += 4;
            } else if (T == f64) {
                if (data_offset + 8 > value_data.len) return error.EndOfData;
                result[i] = .{ .value = try plain.decodeDouble(value_data[data_offset..]) };
                data_offset += 8;
            } else if (T == f16) {
                // Float16: 2 bytes, IEEE 754 half-precision (stored as FIXED_LEN_BYTE_ARRAY(2))
                if (data_offset + 2 > value_data.len) return error.EndOfData;
                const bytes: [2]u8 = .{ value_data[data_offset], value_data[data_offset + 1] };
                result[i] = .{ .value = @bitCast(bytes) };
                data_offset += 2;
            } else if (T == []const u8) {
                if (is_fixed_len) {
                    // Fixed-length byte array - no length prefix
                    if (data_offset + fixed_len > value_data.len) return error.EndOfData;
                    const value = value_data[data_offset..][0..fixed_len];
                    result[i] = .{ .value = try ctx.allocator.dupe(u8, value) };
                    data_offset += fixed_len;
                } else if (isDecimalIntColumn(ctx)) {
                    // Decimal stored as INT32/INT64: read integer bytes, convert to big-endian
                    const int_size: usize = if (ctx.schema_elem.type_ == .int32) 4 else 8;
                    result[i] = .{ .value = try decodeDecimalInt(ctx.allocator, value_data, data_offset, int_size) };
                    data_offset += int_size;
                } else {
                    // Variable-length byte array - has 4-byte length prefix
                    if (data_offset + 4 > value_data.len) return error.EndOfData;
                    const ba = try plain.decodeByteArray(value_data[data_offset..]);
                    if (data_offset + ba.bytes_read > value_data.len) return error.EndOfData;
                    result[i] = .{ .value = try ctx.allocator.dupe(u8, ba.value) };
                    data_offset += ba.bytes_read;
                }
            } else if (T == Int96) {
                if (data_offset + 12 > value_data.len) return error.EndOfData;
                result[i] = .{ .value = Int96.fromBytes(value_data[data_offset..][0..12].*) };
                data_offset += 12;
            }
        } else {
            result[i] = .{ .null_value = {} };
        }
    }
}

/// Decode a column with REQUIRED repetition (no definition levels)
fn decodeRequiredColumn(
    comptime T: type,
    ctx: DecodeContext,
    value_data: []const u8,
    result: []Optional(T),
    is_fixed_len: bool,
    fixed_len: usize,
) !void {
    // The caller has already checked if this page uses dictionary encoding
    // and set uses_dict accordingly (accounting for dictionary fallback)
    if (ctx.uses_dict and T == []const u8 and is_fixed_len and ctx.fixed_byte_array_dict != null) {
        // Dictionary encoded fixed-length byte array, required column
        const bit_width = try extractBitWidth(value_data, 0);
        const indices_data = value_data[1..];

        const indices = try rle.decode(ctx.allocator, indices_data, bit_width, ctx.num_values);
        defer ctx.allocator.free(indices);

        for (0..ctx.num_values) |i| {
            if (ctx.fixed_byte_array_dict.?.get(indices[i])) |v| {
                result[i] = .{ .value = try ctx.allocator.dupe(u8, v) };
            } else {
                result[i] = .{ .null_value = {} };
            }
        }
    } else if (ctx.uses_dict and T == []const u8 and !is_fixed_len) {
        // Dictionary encoded strings (variable-length), required column
        // First byte is bit_width, then RLE-encoded indices
        const bit_width = try extractBitWidth(value_data, 0);
        const indices_data = value_data[1..];

        const indices = try rle.decode(ctx.allocator, indices_data, bit_width, ctx.num_values);
        defer ctx.allocator.free(indices);

        for (0..ctx.num_values) |i| {
            if (ctx.string_dict.?.get(indices[i])) |v| {
                result[i] = .{ .value = try ctx.allocator.dupe(u8, v) };
            } else {
                result[i] = .{ .value = try ctx.allocator.dupe(u8, "") };
            }
        }
    } else if (ctx.uses_dict and T == i32) {
        // Dictionary encoded i32, required column
        const bit_width = try extractBitWidth(value_data, 0);
        const indices_data = value_data[1..];

        const indices = try rle.decode(ctx.allocator, indices_data, bit_width, ctx.num_values);
        defer ctx.allocator.free(indices);

        for (0..ctx.num_values) |i| {
            if (ctx.int32_dict.?.get(indices[i])) |v| {
                result[i] = .{ .value = v };
            } else {
                result[i] = .{ .value = 0 };
            }
        }
    } else if (ctx.uses_dict and T == i64) {
        // Dictionary encoded i64, required column
        const bit_width = try extractBitWidth(value_data, 0);
        const indices_data = value_data[1..];

        const indices = try rle.decode(ctx.allocator, indices_data, bit_width, ctx.num_values);
        defer ctx.allocator.free(indices);

        for (0..ctx.num_values) |i| {
            if (ctx.int64_dict.?.get(indices[i])) |v| {
                result[i] = .{ .value = v };
            } else {
                result[i] = .{ .value = 0 };
            }
        }
    } else if (ctx.uses_dict and T == f32 and ctx.float32_dict != null) {
        const bit_width = try extractBitWidth(value_data, 0);
        const indices = try rle.decode(ctx.allocator, value_data[1..], bit_width, ctx.num_values);
        defer ctx.allocator.free(indices);

        for (0..ctx.num_values) |i| {
            if (ctx.float32_dict.?.get(indices[i])) |v| {
                result[i] = .{ .value = v };
            } else {
                result[i] = .{ .value = 0.0 };
            }
        }
    } else if (ctx.uses_dict and T == f64 and ctx.float64_dict != null) {
        const bit_width = try extractBitWidth(value_data, 0);
        const indices = try rle.decode(ctx.allocator, value_data[1..], bit_width, ctx.num_values);
        defer ctx.allocator.free(indices);

        for (0..ctx.num_values) |i| {
            if (ctx.float64_dict.?.get(indices[i])) |v| {
                result[i] = .{ .value = v };
            } else {
                result[i] = .{ .value = 0.0 };
            }
        }
    } else if (T == []const u8 and is_fixed_len) {
        // Fixed-length byte array - no length prefix
        var data_offset: usize = 0;
        for (0..ctx.num_values) |i| {
            if (data_offset + fixed_len > value_data.len) return error.EndOfData;
            const value = value_data[data_offset..][0..fixed_len];
            result[i] = .{ .value = try ctx.allocator.dupe(u8, value) };
            data_offset += fixed_len;
        }
    } else if (T == f16 and is_fixed_len and fixed_len == 2) {
        // Float16: stored as FIXED_LEN_BYTE_ARRAY(2)
        var data_offset: usize = 0;
        for (0..ctx.num_values) |i| {
            if (data_offset + 2 > value_data.len) return error.EndOfData;
            const bytes: [2]u8 = .{ value_data[data_offset], value_data[data_offset + 1] };
            result[i] = .{ .value = @bitCast(bytes) };
            data_offset += 2;
        }
    } else if (T == []const u8 and !is_fixed_len and isDecimalIntColumn(ctx)) {
        // Decimal stored as INT32/INT64: read integer bytes, convert to big-endian
        const int_size: usize = if (ctx.schema_elem.type_ == .int32) 4 else 8;
        var data_offset: usize = 0;
        for (0..ctx.num_values) |i| {
            result[i] = .{ .value = try decodeDecimalInt(ctx.allocator, value_data, data_offset, int_size) };
            data_offset += int_size;
        }
    } else {
        // PLAIN encoding, required column
        var decoder = plain.PlainDecoder(T).init(value_data, ctx.num_values);
        for (0..ctx.num_values) |i| {
            if (decoder.next()) |v| {
                // For byte arrays, we need to dupe since value_data may be freed
                if (T == []const u8) {
                    result[i] = .{ .value = try ctx.allocator.dupe(u8, v) };
                } else {
                    result[i] = .{ .value = v };
                }
            } else {
                result[i] = .{ .null_value = {} };
            }
        }
    }
}

// =============================================================================
// Dynamic (runtime) column decoding - returns Value instead of typed Optional(T)
// =============================================================================

/// Result of dynamic column decoding
pub const DynamicDecodeResult = struct {
    values: []Value,
    def_levels: ?[]u32,
    rep_levels: ?[]u32,

    pub fn deinit(self: *DynamicDecodeResult, allocator: std.mem.Allocator) void {
        for (self.values) |v| {
            v.deinit(allocator);
        }
        allocator.free(self.values);
        if (self.def_levels) |dl| allocator.free(dl);
        if (self.rep_levels) |rl| allocator.free(rl);
    }
};

/// Decode column values dynamically based on physical type (runtime dispatch)
/// Returns Value tagged union instead of requiring comptime type
pub fn decodeColumnDynamic(
    allocator: std.mem.Allocator,
    schema_elem: format.SchemaElement,
    value_data: []const u8,
    num_values: usize,
    max_def_level: u8,
    max_rep_level: u8,
    uses_dict: bool,
    string_dict: ?*dictionary.StringDictionary,
    int32_dict: ?*dictionary.Int32Dictionary,
    int64_dict: ?*dictionary.Int64Dictionary,
) !DynamicDecodeResult {
    // Default to RLE encoding for backwards compatibility
    return decodeColumnDynamicWithEncoding(
        allocator,
        schema_elem,
        value_data,
        num_values,
        max_def_level,
        max_rep_level,
        uses_dict,
        string_dict,
        int32_dict,
        int64_dict,
        null,
        null,
        null,
        null,
        .rle,
        .rle,
    );
}

/// Decode column values dynamically with explicit level encodings
pub fn decodeColumnDynamicWithEncoding(
    allocator: std.mem.Allocator,
    schema_elem: format.SchemaElement,
    value_data: []const u8,
    num_values: usize,
    max_def_level: u8,
    max_rep_level: u8,
    uses_dict: bool,
    string_dict: ?*dictionary.StringDictionary,
    int32_dict: ?*dictionary.Int32Dictionary,
    int64_dict: ?*dictionary.Int64Dictionary,
    float32_dict: ?*dictionary.Float32Dictionary,
    float64_dict: ?*dictionary.Float64Dictionary,
    fixed_byte_array_dict: ?*dictionary.FixedByteArrayDictionary,
    int96_dict: ?*dictionary.Int96Dictionary,
    def_level_encoding: format.Encoding,
    rep_level_encoding: format.Encoding,
) !DynamicDecodeResult {
    return decodeColumnDynamicWithValueEncoding(
        allocator,
        schema_elem,
        value_data,
        num_values,
        max_def_level,
        max_rep_level,
        uses_dict,
        string_dict,
        int32_dict,
        int64_dict,
        float32_dict,
        float64_dict,
        fixed_byte_array_dict,
        int96_dict,
        def_level_encoding,
        rep_level_encoding,
        .plain,
    );
}

/// Decode column values dynamically with explicit level and value encodings
pub fn decodeColumnDynamicWithValueEncoding(
    allocator: std.mem.Allocator,
    schema_elem: format.SchemaElement,
    value_data: []const u8,
    num_values: usize,
    max_def_level: u8,
    max_rep_level: u8,
    uses_dict: bool,
    string_dict: ?*dictionary.StringDictionary,
    int32_dict: ?*dictionary.Int32Dictionary,
    int64_dict: ?*dictionary.Int64Dictionary,
    float32_dict: ?*dictionary.Float32Dictionary,
    float64_dict: ?*dictionary.Float64Dictionary,
    fixed_byte_array_dict: ?*dictionary.FixedByteArrayDictionary,
    int96_dict: ?*dictionary.Int96Dictionary,
    def_level_encoding: format.Encoding,
    rep_level_encoding: format.Encoding,
    value_encoding: format.Encoding,
) !DynamicDecodeResult {
    const physical_type = schema_elem.type_ orelse return error.InvalidArgument;

    // Check for delta encodings and dispatch to specialized decoders
    switch (value_encoding) {
        .delta_binary_packed => {
            return decodeDeltaBinaryPacked(allocator, schema_elem, value_data, num_values, max_def_level, max_rep_level, def_level_encoding, rep_level_encoding);
        },
        .delta_length_byte_array => {
            return decodeDeltaLengthByteArray(allocator, schema_elem, value_data, num_values, max_def_level, max_rep_level, def_level_encoding, rep_level_encoding);
        },
        .delta_byte_array => {
            return decodeDeltaByteArray(allocator, schema_elem, value_data, num_values, max_def_level, max_rep_level, def_level_encoding, rep_level_encoding);
        },
        .byte_stream_split => {
            return decodeByteStreamSplit(allocator, schema_elem, value_data, num_values, max_def_level, max_rep_level, def_level_encoding, rep_level_encoding);
        },
        .rle => {
            if (physical_type == .boolean) {
                return decodeDynamicBoolRLE(allocator, value_data, num_values, max_def_level, max_rep_level, def_level_encoding, rep_level_encoding);
            }
            return error.UnsupportedEncoding;
        },
        .plain, .rle_dictionary, .plain_dictionary => {},
        else => return error.UnsupportedEncoding,
    }

    // Standard PLAIN/dictionary encoding path
    // Only use dictionary decoding if the encoding is actually dictionary-based
    const is_dict_encoded = uses_dict and
        (value_encoding == .rle_dictionary or value_encoding == .plain_dictionary);

    return switch (physical_type) {
        .boolean => decodeDynamicBool(allocator, schema_elem, value_data, num_values, max_def_level, max_rep_level, def_level_encoding, rep_level_encoding),
        .int32 => decodeDynamicInt32(allocator, schema_elem, value_data, num_values, max_def_level, max_rep_level, is_dict_encoded, int32_dict, def_level_encoding, rep_level_encoding),
        .int64 => decodeDynamicInt64(allocator, schema_elem, value_data, num_values, max_def_level, max_rep_level, is_dict_encoded, int64_dict, def_level_encoding, rep_level_encoding),
        .float => decodeDynamicFloat(allocator, schema_elem, value_data, num_values, max_def_level, max_rep_level, is_dict_encoded, float32_dict, def_level_encoding, rep_level_encoding),
        .double => decodeDynamicDouble(allocator, schema_elem, value_data, num_values, max_def_level, max_rep_level, is_dict_encoded, float64_dict, def_level_encoding, rep_level_encoding),
        .byte_array => decodeDynamicByteArray(allocator, schema_elem, value_data, num_values, max_def_level, max_rep_level, is_dict_encoded, string_dict, def_level_encoding, rep_level_encoding),
        .fixed_len_byte_array => decodeDynamicFixedByteArray(allocator, schema_elem, value_data, num_values, max_def_level, max_rep_level, is_dict_encoded, fixed_byte_array_dict, def_level_encoding, rep_level_encoding),
        .int96 => decodeDynamicInt96(allocator, schema_elem, value_data, num_values, max_def_level, max_rep_level, is_dict_encoded, int96_dict, def_level_encoding, rep_level_encoding),
    };
}

/// Decode column values dynamically for DataPageV2 format
/// In V2, levels are pre-extracted and passed separately (no length prefix in data)
pub fn decodeColumnDynamicV2(
    allocator: std.mem.Allocator,
    schema_elem: format.SchemaElement,
    rep_levels_data: []const u8,
    def_levels_data: []const u8,
    values_data: []const u8,
    num_values: usize,
    max_def_level: u8,
    max_rep_level: u8,
    uses_dict: bool,
    string_dict: ?*dictionary.StringDictionary,
    int32_dict: ?*dictionary.Int32Dictionary,
    int64_dict: ?*dictionary.Int64Dictionary,
    float32_dict: ?*dictionary.Float32Dictionary,
    float64_dict: ?*dictionary.Float64Dictionary,
    fixed_byte_array_dict: ?*dictionary.FixedByteArrayDictionary,
    int96_dict: ?*dictionary.Int96Dictionary,
    value_encoding: format.Encoding,
) !DynamicDecodeResult {
    // Decode levels from pre-extracted data (V2 format: RLE without length prefix)
    const levels_result = try decodeLevelsV2(allocator, rep_levels_data, def_levels_data, num_values, max_def_level, max_rep_level);
    errdefer {
        if (levels_result.def_levels) |dl| allocator.free(dl);
        if (levels_result.rep_levels) |rl| allocator.free(rl);
        allocator.free(levels_result.def_mask);
    }

    const physical_type = schema_elem.type_ orelse return error.InvalidArgument;

    // Check for delta encodings and dispatch appropriately
    const values = try switch (value_encoding) {
        .delta_binary_packed, .delta_length_byte_array, .delta_byte_array, .byte_stream_split => blk: {
            // Use the delta encoding dispatcher
            break :blk try decodeDeltaEncodedValuesV2(allocator, physical_type, values_data, num_values, levels_result.def_mask, levels_result.non_null_count, value_encoding);
        },
        .rle => blk: {
            if (physical_type == .boolean) {
                break :blk try decodeDynamicBoolRLEV2(allocator, values_data, num_values, levels_result.def_mask, levels_result.non_null_count);
            }
            return error.UnsupportedEncoding;
        },
        .plain, .rle_dictionary, .plain_dictionary => blk: {
            const is_dict_encoded = uses_dict and
                (value_encoding == .rle_dictionary or value_encoding == .plain_dictionary);

            break :blk switch (physical_type) {
                .boolean => decodeDynamicBoolV2(allocator, values_data, num_values, levels_result.def_mask, levels_result.non_null_count),
                .int32 => decodeDynamicInt32V2(allocator, values_data, num_values, levels_result.def_mask, levels_result.non_null_count, is_dict_encoded, int32_dict),
                .int64 => decodeDynamicInt64V2(allocator, values_data, num_values, levels_result.def_mask, levels_result.non_null_count, is_dict_encoded, int64_dict),
                .float => decodeDynamicFloatV2(allocator, values_data, num_values, levels_result.def_mask, levels_result.non_null_count, is_dict_encoded, float32_dict),
                .double => decodeDynamicDoubleV2(allocator, values_data, num_values, levels_result.def_mask, levels_result.non_null_count, is_dict_encoded, float64_dict),
                .byte_array => decodeDynamicByteArrayV2(allocator, values_data, num_values, levels_result.def_mask, levels_result.non_null_count, is_dict_encoded, string_dict),
                .fixed_len_byte_array => decodeDynamicFixedByteArrayV2(allocator, schema_elem, values_data, num_values, levels_result.def_mask, levels_result.non_null_count, is_dict_encoded, fixed_byte_array_dict),
                .int96 => decodeDynamicInt96V2(allocator, values_data, num_values, levels_result.def_mask, levels_result.non_null_count, is_dict_encoded, int96_dict),
            };
        },
        else => return error.UnsupportedEncoding,
    };

    allocator.free(levels_result.def_mask);

    return .{
        .values = values,
        .def_levels = levels_result.def_levels,
        .rep_levels = levels_result.rep_levels,
    };
}

/// Decode levels for V2 format (pre-extracted, no length prefix)
fn decodeLevelsV2(
    allocator: std.mem.Allocator,
    rep_levels_data: []const u8,
    def_levels_data: []const u8,
    num_values: usize,
    max_def_level: u8,
    max_rep_level: u8,
) !struct { def_levels: ?[]u32, rep_levels: ?[]u32, def_mask: []bool, non_null_count: usize } {
    var rep_levels: ?[]u32 = null;
    errdefer if (rep_levels) |rl| allocator.free(rl);

    // Decode repetition levels if present
    if (max_rep_level > 0 and rep_levels_data.len > 0) {
        const rep_bit_width = format.computeBitWidth(max_rep_level);
        // V2 uses RLE without length prefix
        rep_levels = try rle.decodeLevels(allocator, rep_levels_data, rep_bit_width, num_values);
    }

    // Decode definition levels
    var def_levels: ?[]u32 = null;
    var def_mask = try allocator.alloc(bool, num_values);
    errdefer allocator.free(def_mask);
    var non_null_count: usize = num_values;

    if (max_def_level > 0 and def_levels_data.len > 0) {
        const def_bit_width = format.computeBitWidth(max_def_level);
        // V2 uses RLE without length prefix
        def_levels = try rle.decodeLevels(allocator, def_levels_data, def_bit_width, num_values);
        errdefer if (def_levels) |dl| allocator.free(dl);

        non_null_count = 0;
        for (def_levels.?, 0..) |level, i| {
            const is_present = level == max_def_level;
            def_mask[i] = is_present;
            if (is_present) non_null_count += 1;
        }
    } else {
        // All values present
        @memset(def_mask, true);
    }

    return .{
        .def_levels = def_levels,
        .rep_levels = rep_levels,
        .def_mask = def_mask,
        .non_null_count = non_null_count,
    };
}

// Delta encoding decoder for V2 pages
fn decodeDeltaEncodedValuesV2(
    allocator: std.mem.Allocator,
    physical_type: format.PhysicalType,
    values_data: []const u8,
    num_values: usize,
    def_mask: []const bool,
    non_null_count: usize,
    value_encoding: format.Encoding,
) ![]Value {
    const values = try allocator.alloc(Value, num_values);
    errdefer allocator.free(values);

    switch (value_encoding) {
        .delta_binary_packed => {
            // Decode all non-null values
            switch (physical_type) {
                .int32 => {
                    const decoded = try delta_binary_packed.decodeInt32(allocator, values_data);
                    defer allocator.free(decoded);

                    var decoded_idx: usize = 0;
                    for (0..num_values) |i| {
                        if (def_mask[i]) {
                            if (decoded_idx < decoded.len) {
                                values[i] = .{ .int32_val = decoded[decoded_idx] };
                                decoded_idx += 1;
                            } else {
                                values[i] = .{ .null_val = {} };
                            }
                        } else {
                            values[i] = .{ .null_val = {} };
                        }
                    }
                },
                .int64 => {
                    const decoded = try delta_binary_packed.decodeInt64(allocator, values_data);
                    defer allocator.free(decoded);

                    var decoded_idx: usize = 0;
                    for (0..num_values) |i| {
                        if (def_mask[i]) {
                            if (decoded_idx < decoded.len) {
                                values[i] = .{ .int64_val = decoded[decoded_idx] };
                                decoded_idx += 1;
                            } else {
                                values[i] = .{ .null_val = {} };
                            }
                        } else {
                            values[i] = .{ .null_val = {} };
                        }
                    }
                },
                else => return error.UnsupportedEncoding,
            }
        },
        .delta_length_byte_array => {
            var result = try delta_length_byte_array.decode(allocator, values_data);
            defer result.deinit();

            var decoded_idx: usize = 0;
            for (0..num_values) |i| {
                if (def_mask[i]) {
                    if (decoded_idx < result.values.len) {
                        values[i] = .{ .bytes_val = try allocator.dupe(u8, result.values[decoded_idx]) };
                        decoded_idx += 1;
                    } else {
                        values[i] = .{ .null_val = {} };
                    }
                } else {
                    values[i] = .{ .null_val = {} };
                }
            }
        },
        .delta_byte_array => {
            var result = try delta_byte_array.decode(allocator, values_data);
            defer result.deinit();

            var decoded_idx: usize = 0;
            for (0..num_values) |i| {
                if (def_mask[i]) {
                    if (decoded_idx < result.values.len) {
                        values[i] = .{ .bytes_val = try allocator.dupe(u8, result.values[decoded_idx]) };
                        decoded_idx += 1;
                    } else {
                        values[i] = .{ .null_val = {} };
                    }
                } else {
                    values[i] = .{ .null_val = {} };
                }
            }
        },
        .byte_stream_split => {
            switch (physical_type) {
                .float => {
                    const decoded = try allocator.alloc(f32, non_null_count);
                    defer allocator.free(decoded);
                    try byte_stream_split.decodeFloat32Into(values_data, decoded);

                    var decoded_idx: usize = 0;
                    for (0..num_values) |i| {
                        if (def_mask[i]) {
                            if (decoded_idx < decoded.len) {
                                values[i] = .{ .float_val = decoded[decoded_idx] };
                                decoded_idx += 1;
                            } else {
                                values[i] = .{ .null_val = {} };
                            }
                        } else {
                            values[i] = .{ .null_val = {} };
                        }
                    }
                },
                .double => {
                    const decoded = try allocator.alloc(f64, non_null_count);
                    defer allocator.free(decoded);
                    try byte_stream_split.decodeFloat64Into(values_data, decoded);

                    var decoded_idx: usize = 0;
                    for (0..num_values) |i| {
                        if (def_mask[i]) {
                            if (decoded_idx < decoded.len) {
                                values[i] = .{ .double_val = decoded[decoded_idx] };
                                decoded_idx += 1;
                            } else {
                                values[i] = .{ .null_val = {} };
                            }
                        } else {
                            values[i] = .{ .null_val = {} };
                        }
                    }
                },
                .int32 => {
                    const decoded = try allocator.alloc(i32, non_null_count);
                    defer allocator.free(decoded);
                    try byte_stream_split.decodeInt32Into(values_data, decoded);

                    var decoded_idx: usize = 0;
                    for (0..num_values) |i| {
                        if (def_mask[i]) {
                            if (decoded_idx < decoded.len) {
                                values[i] = .{ .int32_val = decoded[decoded_idx] };
                                decoded_idx += 1;
                            } else {
                                values[i] = .{ .null_val = {} };
                            }
                        } else {
                            values[i] = .{ .null_val = {} };
                        }
                    }
                },
                .int64 => {
                    const decoded = try allocator.alloc(i64, non_null_count);
                    defer allocator.free(decoded);
                    try byte_stream_split.decodeInt64Into(values_data, decoded);

                    var decoded_idx: usize = 0;
                    for (0..num_values) |i| {
                        if (def_mask[i]) {
                            if (decoded_idx < decoded.len) {
                                values[i] = .{ .int64_val = decoded[decoded_idx] };
                                decoded_idx += 1;
                            } else {
                                values[i] = .{ .null_val = {} };
                            }
                        } else {
                            values[i] = .{ .null_val = {} };
                        }
                    }
                },
                else => return error.UnsupportedEncoding,
            }
        },
        else => return error.UnsupportedEncoding,
    }

    return values;
}

// V2 decode helpers - decode values with pre-computed def_mask

fn decodeDynamicBoolV2(allocator: std.mem.Allocator, values_data: []const u8, num_values: usize, def_mask: []const bool, non_null_count: usize) ![]Value {
    const values = try allocator.alloc(Value, num_values);
    errdefer allocator.free(values);

    // Handle empty case - all nulls
    if (non_null_count == 0 or values_data.len == 0) {
        for (0..num_values) |i| {
            values[i] = .{ .null_val = {} };
        }
        return values;
    }

    // Validate data size: non_null_count booleans need (non_null_count + 7) / 8 bytes
    const required_bytes = (non_null_count + 7) / 8;
    if (values_data.len < required_bytes) {
        allocator.free(values);
        return error.EndOfData;
    }

    // Decode packed booleans - each boolean is 1 bit, 8 per byte
    var value_pos: usize = 0;

    for (0..num_values) |i| {
        if (def_mask[i]) {
            // value_pos is the index of the non-null value
            // decodeBool uses value_pos to compute byte_idx = value_pos / 8 and bit_idx = value_pos % 8
            values[i] = .{ .bool_val = try plain.decodeBool(values_data, value_pos) };
            value_pos += 1;
        } else {
            values[i] = .{ .null_val = {} };
        }
    }
    return values;
}

/// Decode RLE-encoded boolean values for V2 pages (pre-extracted levels)
fn decodeDynamicBoolRLEV2(allocator: std.mem.Allocator, values_data: []const u8, num_values: usize, def_mask: []const bool, non_null_count: usize) ![]Value {
    const values = try allocator.alloc(Value, num_values);
    errdefer allocator.free(values);

    if (non_null_count == 0 or values_data.len == 0) {
        // All nulls
        for (0..num_values) |i| {
            values[i] = .{ .null_val = {} };
        }
        return values;
    }

    // Decode RLE-encoded booleans with bit_width = 1
    const decoded_u32 = try rle.decode(allocator, values_data, 1, non_null_count);
    defer allocator.free(decoded_u32);

    // Map decoded values to output, respecting nulls
    var decoded_idx: usize = 0;
    for (0..num_values) |i| {
        if (def_mask[i]) {
            if (decoded_idx < decoded_u32.len) {
                values[i] = .{ .bool_val = decoded_u32[decoded_idx] != 0 };
                decoded_idx += 1;
            } else {
                values[i] = .{ .null_val = {} };
            }
        } else {
            values[i] = .{ .null_val = {} };
        }
    }

    return values;
}

fn decodeDynamicInt32V2(allocator: std.mem.Allocator, values_data: []const u8, num_values: usize, def_mask: []const bool, non_null_count: usize, uses_dict: bool, int32_dict: ?*dictionary.Int32Dictionary) ![]Value {
    const values = try allocator.alloc(Value, num_values);
    errdefer allocator.free(values);

    if (uses_dict and int32_dict != null) {
        const bit_width = try extractBitWidth(values_data, 0);
        const indices = try rle.decode(allocator, values_data[1..], bit_width, non_null_count);
        defer allocator.free(indices);

        var idx_pos: usize = 0;
        for (0..num_values) |i| {
            if (def_mask[i]) {
                if (int32_dict.?.get(indices[idx_pos])) |v| {
                    values[i] = .{ .int32_val = v };
                } else {
                    values[i] = .{ .null_val = {} };
                }
                idx_pos += 1;
            } else {
                values[i] = .{ .null_val = {} };
            }
        }
    } else {
        var data_offset: usize = 0;
        for (0..num_values) |i| {
            if (def_mask[i]) {
                values[i] = .{ .int32_val = try plain.decodeI32(values_data[data_offset..]) };
                data_offset += 4;
            } else {
                values[i] = .{ .null_val = {} };
            }
        }
    }
    return values;
}

fn decodeDynamicInt64V2(allocator: std.mem.Allocator, values_data: []const u8, num_values: usize, def_mask: []const bool, non_null_count: usize, uses_dict: bool, int64_dict: ?*dictionary.Int64Dictionary) ![]Value {
    const values = try allocator.alloc(Value, num_values);
    errdefer allocator.free(values);

    if (uses_dict and int64_dict != null) {
        const bit_width = try extractBitWidth(values_data, 0);
        const indices = try rle.decode(allocator, values_data[1..], bit_width, non_null_count);
        defer allocator.free(indices);

        var idx_pos: usize = 0;
        for (0..num_values) |i| {
            if (def_mask[i]) {
                if (int64_dict.?.get(indices[idx_pos])) |v| {
                    values[i] = .{ .int64_val = v };
                } else {
                    values[i] = .{ .null_val = {} };
                }
                idx_pos += 1;
            } else {
                values[i] = .{ .null_val = {} };
            }
        }
    } else {
        var data_offset: usize = 0;
        for (0..num_values) |i| {
            if (def_mask[i]) {
                values[i] = .{ .int64_val = try plain.decodeI64(values_data[data_offset..]) };
                data_offset += 8;
            } else {
                values[i] = .{ .null_val = {} };
            }
        }
    }
    return values;
}

fn decodeDynamicFloatV2(allocator: std.mem.Allocator, values_data: []const u8, num_values: usize, def_mask: []const bool, non_null_count: usize, uses_dict: bool, float32_dict: ?*dictionary.Float32Dictionary) ![]Value {
    const values = try allocator.alloc(Value, num_values);
    errdefer allocator.free(values);

    if (uses_dict and float32_dict != null) {
        const bit_width = try extractBitWidth(values_data, 0);
        const indices = try rle.decode(allocator, values_data[1..], bit_width, non_null_count);
        defer allocator.free(indices);

        var idx_pos: usize = 0;
        for (0..num_values) |i| {
            if (def_mask[i]) {
                if (float32_dict.?.get(indices[idx_pos])) |v| {
                    values[i] = .{ .float_val = v };
                } else {
                    values[i] = .{ .null_val = {} };
                }
                idx_pos += 1;
            } else {
                values[i] = .{ .null_val = {} };
            }
        }
    } else {
        var data_offset: usize = 0;
        for (0..num_values) |i| {
            if (def_mask[i]) {
                values[i] = .{ .float_val = try plain.decodeFloat(values_data[data_offset..]) };
                data_offset += 4;
            } else {
                values[i] = .{ .null_val = {} };
            }
        }
    }
    return values;
}

fn decodeDynamicDoubleV2(allocator: std.mem.Allocator, values_data: []const u8, num_values: usize, def_mask: []const bool, non_null_count: usize, uses_dict: bool, float64_dict: ?*dictionary.Float64Dictionary) ![]Value {
    const values = try allocator.alloc(Value, num_values);
    errdefer allocator.free(values);

    if (uses_dict and float64_dict != null) {
        const bit_width = try extractBitWidth(values_data, 0);
        const indices = try rle.decode(allocator, values_data[1..], bit_width, non_null_count);
        defer allocator.free(indices);

        var idx_pos: usize = 0;
        for (0..num_values) |i| {
            if (def_mask[i]) {
                if (float64_dict.?.get(indices[idx_pos])) |v| {
                    values[i] = .{ .double_val = v };
                } else {
                    values[i] = .{ .null_val = {} };
                }
                idx_pos += 1;
            } else {
                values[i] = .{ .null_val = {} };
            }
        }
    } else {
        var data_offset: usize = 0;
        for (0..num_values) |i| {
            if (def_mask[i]) {
                values[i] = .{ .double_val = try plain.decodeDouble(values_data[data_offset..]) };
                data_offset += 8;
            } else {
                values[i] = .{ .null_val = {} };
            }
        }
    }
    return values;
}

fn decodeDynamicByteArrayV2(allocator: std.mem.Allocator, values_data: []const u8, num_values: usize, def_mask: []const bool, non_null_count: usize, uses_dict: bool, string_dict: ?*dictionary.StringDictionary) ![]Value {
    const values = try allocator.alloc(Value, num_values);
    errdefer allocator.free(values);

    if (uses_dict and string_dict != null) {
        const bit_width = try extractBitWidth(values_data, 0);
        const indices = try rle.decode(allocator, values_data[1..], bit_width, non_null_count);
        defer allocator.free(indices);

        var idx_pos: usize = 0;
        for (0..num_values) |i| {
            if (def_mask[i]) {
                if (string_dict.?.get(indices[idx_pos])) |v| {
                    values[i] = .{ .bytes_val = try allocator.dupe(u8, v) };
                } else {
                    values[i] = .{ .bytes_val = try allocator.dupe(u8, "") };
                }
                idx_pos += 1;
            } else {
                values[i] = .{ .null_val = {} };
            }
        }
    } else {
        var data_offset: usize = 0;
        for (0..num_values) |i| {
            if (def_mask[i]) {
                const ba = try plain.decodeByteArray(values_data[data_offset..]);
                values[i] = .{ .bytes_val = try allocator.dupe(u8, ba.value) };
                data_offset += ba.bytes_read;
            } else {
                values[i] = .{ .null_val = {} };
            }
        }
    }
    return values;
}

fn decodeDynamicFixedByteArrayV2(allocator: std.mem.Allocator, schema_elem: format.SchemaElement, values_data: []const u8, num_values: usize, def_mask: []const bool, non_null_count: usize, uses_dict: bool, fixed_byte_array_dict: ?*dictionary.FixedByteArrayDictionary) ![]Value {
    const values = try allocator.alloc(Value, num_values);
    errdefer allocator.free(values);

    if (uses_dict and fixed_byte_array_dict != null) {
        const bit_width = try extractBitWidth(values_data, 0);
        const indices = try rle.decode(allocator, values_data[1..], bit_width, non_null_count);
        defer allocator.free(indices);

        var idx_pos: usize = 0;
        for (0..num_values) |i| {
            if (def_mask[i]) {
                if (fixed_byte_array_dict.?.get(indices[idx_pos])) |v| {
                    values[i] = .{ .fixed_bytes_val = try allocator.dupe(u8, v) };
                } else {
                    values[i] = .{ .null_val = {} };
                }
                idx_pos += 1;
            } else {
                values[i] = .{ .null_val = {} };
            }
        }
    } else {
        const fixed_len = try safeTypeLength(schema_elem.type_length);

        var data_offset: usize = 0;
        for (0..num_values) |i| {
            if (def_mask[i]) {
                if (data_offset + fixed_len > values_data.len) return error.EndOfData;
                const value = values_data[data_offset..][0..fixed_len];
                values[i] = .{ .fixed_bytes_val = try allocator.dupe(u8, value) };
                data_offset += fixed_len;
            } else {
                values[i] = .{ .null_val = {} };
            }
        }
    }
    return values;
}

/// Helper to decode levels and return offset into value data
fn decodeLevelsForDynamic(
    allocator: std.mem.Allocator,
    value_data: []const u8,
    num_values: usize,
    max_def_level: u8,
    max_rep_level: u8,
) !struct { data_offset: usize, def_levels: ?[]u32, rep_levels: ?[]u32, def_mask: []bool, non_null_count: usize } {
    // Default to RLE encoding
    return decodeLevelsForDynamicWithEncoding(allocator, value_data, num_values, max_def_level, max_rep_level, .rle, .rle);
}

/// Helper to decode levels with explicit encodings
fn decodeLevelsForDynamicWithEncoding(
    allocator: std.mem.Allocator,
    value_data: []const u8,
    num_values: usize,
    max_def_level: u8,
    max_rep_level: u8,
    def_level_encoding: format.Encoding,
    rep_level_encoding: format.Encoding,
) !struct { data_offset: usize, def_levels: ?[]u32, rep_levels: ?[]u32, def_mask: []bool, non_null_count: usize } {
    var data_offset: usize = 0;
    var rep_levels: ?[]u32 = null;
    errdefer if (rep_levels) |rl| allocator.free(rl);

    // Read repetition levels first (if present)
    if (max_rep_level > 0) {
        const rep_bit_width = format.computeBitWidth(max_rep_level);

        if (rep_level_encoding == .bit_packed) {
            // Pure bit-packed: no length prefix
            const rep_bytes = rle.bitPackedSize(num_values, rep_bit_width);
            if (data_offset + rep_bytes > value_data.len) return error.EndOfData;
            const rep_levels_data = try safe.slice(value_data, data_offset, rep_bytes);
            data_offset += rep_bytes;
            rep_levels = try rle.decodeBitPackedLevels(allocator, rep_levels_data, rep_bit_width, num_values);
        } else {
            // RLE hybrid: has length prefix
            if (data_offset + 4 > value_data.len) return error.EndOfData;
            const rep_levels_len = try plain.decodeU32(value_data[data_offset..]);
            data_offset += 4;
            if (data_offset + rep_levels_len > value_data.len) return error.EndOfData;
            const rep_levels_data = try safe.slice(value_data, data_offset, rep_levels_len);
            data_offset += rep_levels_len;
            rep_levels = try rle.decode(allocator, rep_levels_data, rep_bit_width, num_values);
        }
    }

    // Read definition levels (if present)
    var def_levels: ?[]u32 = null;
    var def_mask = try allocator.alloc(bool, num_values);
    errdefer allocator.free(def_mask);
    var non_null_count: usize = num_values;

    if (max_def_level > 0) {
        const def_bit_width = format.computeBitWidth(max_def_level);

        if (def_level_encoding == .bit_packed) {
            // Pure bit-packed: no length prefix
            const def_bytes = rle.bitPackedSize(num_values, def_bit_width);
            if (data_offset + def_bytes > value_data.len) return error.EndOfData;
            const def_levels_data = try safe.slice(value_data, data_offset, def_bytes);
            data_offset += def_bytes;
            def_levels = try rle.decodeBitPackedLevels(allocator, def_levels_data, def_bit_width, num_values);
        } else {
            // RLE hybrid: has length prefix
            if (data_offset + 4 > value_data.len) return error.EndOfData;
            const def_levels_len = try plain.decodeU32(value_data[data_offset..]);
            data_offset += 4;
            if (data_offset + def_levels_len > value_data.len) return error.EndOfData;
            const def_levels_data = try safe.slice(value_data, data_offset, def_levels_len);
            data_offset += def_levels_len;
            def_levels = try rle.decodeLevels(allocator, def_levels_data, def_bit_width, num_values);
        }
        errdefer if (def_levels) |dl| allocator.free(dl);

        non_null_count = 0;
        for (def_levels.?, 0..) |level, i| {
            const is_present = level == max_def_level;
            def_mask[i] = is_present;
            if (is_present) non_null_count += 1;
        }
    } else {
        // REQUIRED column (max_def_level == 0): no definition levels to decode.
        // All values are present per the schema.
        //
        // Note: V2 pages handle malformed files (REQUIRED with level bytes) correctly
        // because the header provides explicit byte counts that are skipped in the caller.
        // V1 pages with malformed REQUIRED columns will fail, matching Go/C++/Java/PyArrow.
        @memset(def_mask, true);
    }

    return .{
        .data_offset = data_offset,
        .def_levels = def_levels,
        .rep_levels = rep_levels,
        .def_mask = def_mask,
        .non_null_count = non_null_count,
    };
}

fn decodeDynamicBool(
    allocator: std.mem.Allocator,
    schema_elem: format.SchemaElement,
    value_data: []const u8,
    num_values: usize,
    max_def_level: u8,
    max_rep_level: u8,
    def_level_encoding: format.Encoding,
    rep_level_encoding: format.Encoding,
) !DynamicDecodeResult {
    const ctx = DecodeContext{
        .allocator = allocator,
        .schema_elem = schema_elem,
        .num_values = num_values,
        .uses_dict = false,
        .string_dict = null,
        .max_definition_level = max_def_level,
        .max_repetition_level = max_rep_level,
        .def_level_encoding = def_level_encoding,
        .rep_level_encoding = rep_level_encoding,
    };

    const typed_result = try decodeColumnWithLevels(bool, ctx, value_data);
    defer allocator.free(typed_result.values);

    // Convert to Value
    const values = try allocator.alloc(Value, num_values);
    for (typed_result.values, 0..) |opt, i| {
        values[i] = switch (opt) {
            .value => |v| .{ .bool_val = v },
            .null_value => .{ .null_val = {} },
        };
    }

    return .{
        .values = values,
        .def_levels = typed_result.def_levels,
        .rep_levels = typed_result.rep_levels,
    };
}

/// Decode RLE-encoded boolean values (Parquet spec allows RLE for booleans)
fn decodeDynamicBoolRLE(
    allocator: std.mem.Allocator,
    value_data: []const u8,
    num_values: usize,
    max_def_level: u8,
    max_rep_level: u8,
    def_level_encoding: format.Encoding,
    rep_level_encoding: format.Encoding,
) !DynamicDecodeResult {
    // Parse definition/repetition levels first
    const levels_info = try decodeLevelsForDynamicWithEncoding(
        allocator, value_data, num_values, max_def_level, max_rep_level,
        def_level_encoding, rep_level_encoding,
    );
    defer allocator.free(levels_info.def_mask);

    const values_data = value_data[levels_info.data_offset..];
    const values = try allocator.alloc(Value, num_values);
    errdefer allocator.free(values);

    if (levels_info.non_null_count == 0 or values_data.len == 0) {
        // All nulls
        for (0..num_values) |i| {
            values[i] = .{ .null_val = {} };
        }
    } else {
        // Decode RLE-encoded booleans with bit_width = 1
        const decoded_u32 = try rle.decode(allocator, values_data, 1, levels_info.non_null_count);
        defer allocator.free(decoded_u32);

        // Map decoded values to output, respecting nulls
        var decoded_idx: usize = 0;
        for (0..num_values) |i| {
            if (levels_info.def_mask[i]) {
                if (decoded_idx < decoded_u32.len) {
                    values[i] = .{ .bool_val = decoded_u32[decoded_idx] != 0 };
                    decoded_idx += 1;
                } else {
                    values[i] = .{ .null_val = {} };
                }
            } else {
                values[i] = .{ .null_val = {} };
            }
        }
    }

    return .{
        .values = values,
        .def_levels = levels_info.def_levels,
        .rep_levels = levels_info.rep_levels,
    };
}

fn decodeDynamicInt32(
    allocator: std.mem.Allocator,
    schema_elem: format.SchemaElement,
    value_data: []const u8,
    num_values: usize,
    max_def_level: u8,
    max_rep_level: u8,
    uses_dict: bool,
    int32_dict: ?*dictionary.Int32Dictionary,
    def_level_encoding: format.Encoding,
    rep_level_encoding: format.Encoding,
) !DynamicDecodeResult {
    const ctx = DecodeContext{
        .allocator = allocator,
        .schema_elem = schema_elem,
        .num_values = num_values,
        .uses_dict = uses_dict,
        .string_dict = null,
        .int32_dict = int32_dict,
        .max_definition_level = max_def_level,
        .max_repetition_level = max_rep_level,
        .def_level_encoding = def_level_encoding,
        .rep_level_encoding = rep_level_encoding,
    };

    const typed_result = try decodeColumnWithLevels(i32, ctx, value_data);
    defer allocator.free(typed_result.values);

    const values = try allocator.alloc(Value, num_values);
    for (typed_result.values, 0..) |opt, i| {
        values[i] = switch (opt) {
            .value => |v| .{ .int32_val = v },
            .null_value => .{ .null_val = {} },
        };
    }

    return .{
        .values = values,
        .def_levels = typed_result.def_levels,
        .rep_levels = typed_result.rep_levels,
    };
}

fn decodeDynamicInt64(
    allocator: std.mem.Allocator,
    schema_elem: format.SchemaElement,
    value_data: []const u8,
    num_values: usize,
    max_def_level: u8,
    max_rep_level: u8,
    uses_dict: bool,
    int64_dict: ?*dictionary.Int64Dictionary,
    def_level_encoding: format.Encoding,
    rep_level_encoding: format.Encoding,
) !DynamicDecodeResult {
    const ctx = DecodeContext{
        .allocator = allocator,
        .schema_elem = schema_elem,
        .num_values = num_values,
        .uses_dict = uses_dict,
        .string_dict = null,
        .int64_dict = int64_dict,
        .max_definition_level = max_def_level,
        .max_repetition_level = max_rep_level,
        .def_level_encoding = def_level_encoding,
        .rep_level_encoding = rep_level_encoding,
    };

    const typed_result = try decodeColumnWithLevels(i64, ctx, value_data);
    defer allocator.free(typed_result.values);

    const values = try allocator.alloc(Value, num_values);
    for (typed_result.values, 0..) |opt, i| {
        values[i] = switch (opt) {
            .value => |v| .{ .int64_val = v },
            .null_value => .{ .null_val = {} },
        };
    }

    return .{
        .values = values,
        .def_levels = typed_result.def_levels,
        .rep_levels = typed_result.rep_levels,
    };
}

fn decodeDynamicFloat(
    allocator: std.mem.Allocator,
    schema_elem: format.SchemaElement,
    value_data: []const u8,
    num_values: usize,
    max_def_level: u8,
    max_rep_level: u8,
    uses_dict: bool,
    float32_dict: ?*dictionary.Float32Dictionary,
    def_level_encoding: format.Encoding,
    rep_level_encoding: format.Encoding,
) !DynamicDecodeResult {
    // Handle dictionary encoding specially
    if (uses_dict and float32_dict != null) {
        return decodeDictFloat32(allocator, value_data, num_values, max_def_level, max_rep_level, float32_dict.?, def_level_encoding, rep_level_encoding);
    }

    const ctx = DecodeContext{
        .allocator = allocator,
        .schema_elem = schema_elem,
        .num_values = num_values,
        .uses_dict = false,
        .string_dict = null,
        .max_definition_level = max_def_level,
        .max_repetition_level = max_rep_level,
        .def_level_encoding = def_level_encoding,
        .rep_level_encoding = rep_level_encoding,
    };

    const typed_result = try decodeColumnWithLevels(f32, ctx, value_data);
    defer allocator.free(typed_result.values);

    const values = try allocator.alloc(Value, num_values);
    for (typed_result.values, 0..) |opt, i| {
        values[i] = switch (opt) {
            .value => |v| .{ .float_val = v },
            .null_value => .{ .null_val = {} },
        };
    }

    return .{
        .values = values,
        .def_levels = typed_result.def_levels,
        .rep_levels = typed_result.rep_levels,
    };
}

fn decodeDynamicDouble(
    allocator: std.mem.Allocator,
    schema_elem: format.SchemaElement,
    value_data: []const u8,
    num_values: usize,
    max_def_level: u8,
    max_rep_level: u8,
    uses_dict: bool,
    float64_dict: ?*dictionary.Float64Dictionary,
    def_level_encoding: format.Encoding,
    rep_level_encoding: format.Encoding,
) !DynamicDecodeResult {
    // Handle dictionary encoding specially
    if (uses_dict and float64_dict != null) {
        return decodeDictFloat64(allocator, value_data, num_values, max_def_level, max_rep_level, float64_dict.?, def_level_encoding, rep_level_encoding);
    }

    const ctx = DecodeContext{
        .allocator = allocator,
        .schema_elem = schema_elem,
        .num_values = num_values,
        .uses_dict = false,
        .string_dict = null,
        .max_definition_level = max_def_level,
        .max_repetition_level = max_rep_level,
        .def_level_encoding = def_level_encoding,
        .rep_level_encoding = rep_level_encoding,
    };

    const typed_result = try decodeColumnWithLevels(f64, ctx, value_data);
    defer allocator.free(typed_result.values);

    const values = try allocator.alloc(Value, num_values);
    for (typed_result.values, 0..) |opt, i| {
        values[i] = switch (opt) {
            .value => |v| .{ .double_val = v },
            .null_value => .{ .null_val = {} },
        };
    }

    return .{
        .values = values,
        .def_levels = typed_result.def_levels,
        .rep_levels = typed_result.rep_levels,
    };
}

/// Decode dictionary-encoded f32 values
fn decodeDictFloat32(
    allocator: std.mem.Allocator,
    value_data: []const u8,
    num_values: usize,
    max_def_level: u8,
    max_rep_level: u8,
    float32_dict: *dictionary.Float32Dictionary,
    def_level_encoding: format.Encoding,
    rep_level_encoding: format.Encoding,
) !DynamicDecodeResult {
    const levels_info = try decodeLevelsForDynamicWithEncoding(allocator, value_data, num_values, max_def_level, max_rep_level, def_level_encoding, rep_level_encoding);
    defer allocator.free(levels_info.def_mask);

    const values = try allocator.alloc(Value, num_values);
    errdefer allocator.free(values);

    // Decode dictionary indices (RLE encoded)
    const indices_data = value_data[levels_info.data_offset..];
    if (indices_data.len == 0) {
        // All nulls
        for (0..num_values) |i| {
            values[i] = .{ .null_val = {} };
        }
    } else {
        const bit_width = try extractBitWidth(indices_data, 0);
        const indices = try rle.decode(allocator, indices_data[1..], bit_width, levels_info.non_null_count);
        defer allocator.free(indices);

        var idx_pos: usize = 0;
        for (0..num_values) |i| {
            if (levels_info.def_mask[i]) {
                if (float32_dict.get(indices[idx_pos])) |v| {
                    values[i] = .{ .float_val = v };
                } else {
                    values[i] = .{ .float_val = 0 };
                }
                idx_pos += 1;
            } else {
                values[i] = .{ .null_val = {} };
            }
        }
    }

    return .{
        .values = values,
        .def_levels = levels_info.def_levels,
        .rep_levels = levels_info.rep_levels,
    };
}

/// Decode dictionary-encoded f64 values
fn decodeDictFloat64(
    allocator: std.mem.Allocator,
    value_data: []const u8,
    num_values: usize,
    max_def_level: u8,
    max_rep_level: u8,
    float64_dict: *dictionary.Float64Dictionary,
    def_level_encoding: format.Encoding,
    rep_level_encoding: format.Encoding,
) !DynamicDecodeResult {
    const levels_info = try decodeLevelsForDynamicWithEncoding(allocator, value_data, num_values, max_def_level, max_rep_level, def_level_encoding, rep_level_encoding);
    defer allocator.free(levels_info.def_mask);

    const values = try allocator.alloc(Value, num_values);
    errdefer allocator.free(values);

    // Decode dictionary indices (RLE encoded)
    const indices_data = value_data[levels_info.data_offset..];
    if (indices_data.len == 0) {
        // All nulls
        for (0..num_values) |i| {
            values[i] = .{ .null_val = {} };
        }
    } else {
        const bit_width = try extractBitWidth(indices_data, 0);
        const indices = try rle.decode(allocator, indices_data[1..], bit_width, levels_info.non_null_count);
        defer allocator.free(indices);

        var idx_pos: usize = 0;
        for (0..num_values) |i| {
            if (levels_info.def_mask[i]) {
                if (float64_dict.get(indices[idx_pos])) |v| {
                    values[i] = .{ .double_val = v };
                } else {
                    values[i] = .{ .double_val = 0 };
                }
                idx_pos += 1;
            } else {
                values[i] = .{ .null_val = {} };
            }
        }
    }

    return .{
        .values = values,
        .def_levels = levels_info.def_levels,
        .rep_levels = levels_info.rep_levels,
    };
}

fn decodeDynamicByteArray(
    allocator: std.mem.Allocator,
    schema_elem: format.SchemaElement,
    value_data: []const u8,
    num_values: usize,
    max_def_level: u8,
    max_rep_level: u8,
    uses_dict: bool,
    string_dict: ?*dictionary.StringDictionary,
    def_level_encoding: format.Encoding,
    rep_level_encoding: format.Encoding,
) !DynamicDecodeResult {
    const ctx = DecodeContext{
        .allocator = allocator,
        .schema_elem = schema_elem,
        .num_values = num_values,
        .uses_dict = uses_dict,
        .string_dict = string_dict,
        .max_definition_level = max_def_level,
        .max_repetition_level = max_rep_level,
        .def_level_encoding = def_level_encoding,
        .rep_level_encoding = rep_level_encoding,
    };

    const typed_result = try decodeColumnWithLevels([]const u8, ctx, value_data);
    // Don't defer free - we're transferring ownership of the byte arrays

    const values = try allocator.alloc(Value, num_values);
    errdefer allocator.free(values);

    for (typed_result.values, 0..) |opt, i| {
        values[i] = switch (opt) {
            .value => |v| .{ .bytes_val = v }, // Transfer ownership
            .null_value => .{ .null_val = {} },
        };
    }

    // Free just the container, not the byte array contents (transferred to Value)
    allocator.free(typed_result.values);

    return .{
        .values = values,
        .def_levels = typed_result.def_levels,
        .rep_levels = typed_result.rep_levels,
    };
}

fn decodeDynamicFixedByteArray(
    allocator: std.mem.Allocator,
    schema_elem: format.SchemaElement,
    value_data: []const u8,
    num_values: usize,
    max_def_level: u8,
    max_rep_level: u8,
    uses_dict: bool,
    fixed_byte_array_dict: ?*dictionary.FixedByteArrayDictionary,
    def_level_encoding: format.Encoding,
    rep_level_encoding: format.Encoding,
) !DynamicDecodeResult {
    if (uses_dict and fixed_byte_array_dict != null) {
        // Dictionary-encoded: decode levels first, then look up values from dictionary
        var pos: usize = 0;

        // Decode definition levels
        var def_levels: ?[]u32 = null;
        errdefer if (def_levels) |dl| allocator.free(dl);

        var non_null_count: usize = num_values;
        if (max_def_level > 0) {
            const def_bit_width = format.computeBitWidth(max_def_level);
            if (def_level_encoding == .bit_packed) {
                def_levels = try rle.decodeBitPackedLevels(allocator, value_data[pos..], def_bit_width, num_values);
                pos += rle.bitPackedSize(num_values, def_bit_width);
            } else {
                // RLE with 4-byte length prefix
                if (pos + 4 > value_data.len) return error.EndOfData;
                if (value_data.len < pos + 4) return error.EndOfData;
                if (value_data.len < pos + 4) return error.EndOfData;
                const def_len = std.mem.readInt(u32, value_data[pos..][0..4], .little);
                pos += 4;
                def_levels = try rle.decodeLevels(allocator, value_data[pos..][0..def_len], def_bit_width, num_values);
                pos += def_len;
            }

            non_null_count = 0;
            for (def_levels.?) |level| {
                if (level == max_def_level) non_null_count += 1;
            }
        }

        // Skip repetition levels
        var rep_levels: ?[]u32 = null;
        errdefer if (rep_levels) |rl| allocator.free(rl);

        if (max_rep_level > 0) {
            const rep_bit_width = format.computeBitWidth(max_rep_level);
            if (rep_level_encoding == .bit_packed) {
                rep_levels = try rle.decodeBitPackedLevels(allocator, value_data[pos..], rep_bit_width, num_values);
                pos += rle.bitPackedSize(num_values, rep_bit_width);
            } else {
                if (pos + 4 > value_data.len) return error.EndOfData;
                if (value_data.len < pos + 4) return error.EndOfData;
                if (value_data.len < pos + 4) return error.EndOfData;
                const rep_len = std.mem.readInt(u32, value_data[pos..][0..4], .little);
                pos += 4;
                rep_levels = try rle.decodeLevels(allocator, value_data[pos..][0..rep_len], rep_bit_width, num_values);
                pos += rep_len;
            }
        }

        // Decode dictionary indices
        const indices_data = value_data[pos..];
        if (indices_data.len == 0) return error.EndOfData;

        const bit_width = try extractBitWidth(indices_data, 0);
        const indices = try rle.decode(allocator, indices_data[1..], bit_width, non_null_count);
        defer allocator.free(indices);

        // Build output values
        const values = try allocator.alloc(Value, num_values);
        errdefer allocator.free(values);

        var idx_pos: usize = 0;
        for (0..num_values) |i| {
            const is_present = if (def_levels) |dl| dl[i] == max_def_level else true;
            if (is_present) {
                if (fixed_byte_array_dict.?.get(indices[idx_pos])) |v| {
                    values[i] = .{ .fixed_bytes_val = try allocator.dupe(u8, v) };
                } else {
                    values[i] = .{ .null_val = {} };
                }
                idx_pos += 1;
            } else {
                values[i] = .{ .null_val = {} };
            }
        }

        return .{
            .values = values,
            .def_levels = def_levels,
            .rep_levels = rep_levels,
        };
    }

    // Plain encoding - use the typed decoder path
    const ctx = DecodeContext{
        .allocator = allocator,
        .schema_elem = schema_elem,
        .num_values = num_values,
        .uses_dict = false,
        .string_dict = null,
        .max_definition_level = max_def_level,
        .max_repetition_level = max_rep_level,
        .def_level_encoding = def_level_encoding,
        .rep_level_encoding = rep_level_encoding,
    };

    const typed_result = try decodeColumnWithLevels([]const u8, ctx, value_data);

    const values = try allocator.alloc(Value, num_values);
    errdefer allocator.free(values);

    for (typed_result.values, 0..) |opt, i| {
        values[i] = switch (opt) {
            .value => |v| .{ .fixed_bytes_val = v }, // Transfer ownership
            .null_value => .{ .null_val = {} },
        };
    }

    allocator.free(typed_result.values);

    return .{
        .values = values,
        .def_levels = typed_result.def_levels,
        .rep_levels = typed_result.rep_levels,
    };
}

// =============================================================================
// INT96 Decoders (Legacy timestamp format)
// =============================================================================

/// Decode INT96 values (12-byte legacy timestamp format)
/// Converts to i64 nanoseconds since Unix epoch for compatibility with modern timestamps
pub fn decodeDynamicInt96(
    allocator: std.mem.Allocator,
    schema_elem: format.SchemaElement,
    value_data: []const u8,
    num_values: usize,
    max_def_level: u8,
    max_rep_level: u8,
    uses_dict: bool,
    int96_dict: ?*dictionary.Int96Dictionary,
    def_level_encoding: format.Encoding,
    rep_level_encoding: format.Encoding,
) !DynamicDecodeResult {
    _ = schema_elem;

    if (uses_dict and int96_dict != null) {
        return decodeDictInt96(allocator, value_data, num_values, max_def_level, max_rep_level, int96_dict.?, def_level_encoding, rep_level_encoding);
    }

    // Decode levels first to get data offset
    const levels_info = try decodeLevelsForDynamicWithEncoding(
        allocator, value_data, num_values, max_def_level, max_rep_level,
        def_level_encoding, rep_level_encoding,
    );
    defer allocator.free(levels_info.def_mask);

    const values_data = value_data[levels_info.data_offset..];
    const values = try allocator.alloc(Value, num_values);
    errdefer allocator.free(values);

    var data_offset: usize = 0;
    for (0..num_values) |i| {
        if (levels_info.def_mask[i]) {
            if (data_offset + 12 > values_data.len) {
                values[i] = .{ .null_val = {} };
                continue;
            }
            const int96_bytes: [12]u8 = values_data[data_offset..][0..12].*;
            const int96 = Int96.fromBytes(int96_bytes);
            values[i] = .{ .int64_val = int96.toNanos() };
            data_offset += 12;
        } else {
            values[i] = .{ .null_val = {} };
        }
    }

    return .{
        .values = values,
        .def_levels = levels_info.def_levels,
        .rep_levels = levels_info.rep_levels,
    };
}

fn decodeDictInt96(
    allocator: std.mem.Allocator,
    value_data: []const u8,
    num_values: usize,
    max_def_level: u8,
    max_rep_level: u8,
    int96_dict: *dictionary.Int96Dictionary,
    def_level_encoding: format.Encoding,
    rep_level_encoding: format.Encoding,
) !DynamicDecodeResult {
    const levels_info = try decodeLevelsForDynamicWithEncoding(allocator, value_data, num_values, max_def_level, max_rep_level, def_level_encoding, rep_level_encoding);
    defer allocator.free(levels_info.def_mask);

    const values = try allocator.alloc(Value, num_values);
    errdefer allocator.free(values);

    const indices_data = value_data[levels_info.data_offset..];
    if (indices_data.len == 0) {
        for (0..num_values) |i| {
            values[i] = .{ .null_val = {} };
        }
    } else {
        const bit_width = try extractBitWidth(indices_data, 0);
        const indices = try rle.decode(allocator, indices_data[1..], bit_width, levels_info.non_null_count);
        defer allocator.free(indices);

        var idx_pos: usize = 0;
        for (0..num_values) |i| {
            if (levels_info.def_mask[i]) {
                if (int96_dict.get(indices[idx_pos])) |bytes| {
                    values[i] = .{ .int64_val = Int96.fromBytes(bytes).toNanos() };
                } else {
                    values[i] = .{ .int64_val = 0 };
                }
                idx_pos += 1;
            } else {
                values[i] = .{ .null_val = {} };
            }
        }
    }

    return .{
        .values = values,
        .def_levels = levels_info.def_levels,
        .rep_levels = levels_info.rep_levels,
    };
}

/// Decode INT96 values for V2 pages (pre-extracted levels)
fn decodeDynamicInt96V2(
    allocator: std.mem.Allocator,
    values_data: []const u8,
    num_values: usize,
    def_mask: []const bool,
    non_null_count: usize,
    uses_dict: bool,
    int96_dict: ?*dictionary.Int96Dictionary,
) ![]Value {
    const values = try allocator.alloc(Value, num_values);
    errdefer allocator.free(values);

    if (uses_dict and int96_dict != null) {
        if (values_data.len == 0) {
            for (0..num_values) |i| {
                values[i] = .{ .null_val = {} };
            }
        } else {
            const bit_width = try extractBitWidth(values_data, 0);
            const indices = try rle.decode(allocator, values_data[1..], bit_width, non_null_count);
            defer allocator.free(indices);

            var idx_pos: usize = 0;
            for (0..num_values) |i| {
                if (def_mask[i]) {
                    if (int96_dict.?.get(indices[idx_pos])) |bytes| {
                        values[i] = .{ .int64_val = Int96.fromBytes(bytes).toNanos() };
                    } else {
                        values[i] = .{ .int64_val = 0 };
                    }
                    idx_pos += 1;
                } else {
                    values[i] = .{ .null_val = {} };
                }
            }
        }
        return values;
    }

    var data_offset: usize = 0;
    for (0..num_values) |i| {
        if (def_mask[i]) {
            if (data_offset + 12 > values_data.len) {
                values[i] = .{ .null_val = {} };
                continue;
            }
            const int96_bytes: [12]u8 = values_data[data_offset..][0..12].*;
            const int96 = Int96.fromBytes(int96_bytes);
            values[i] = .{ .int64_val = int96.toNanos() };
            data_offset += 12;
        } else {
            values[i] = .{ .null_val = {} };
        }
    }
    return values;
}

// =============================================================================
// Delta Encoding Decoders
// =============================================================================

/// Decode DELTA_BINARY_PACKED encoded int32/int64 values
fn decodeDeltaBinaryPacked(
    allocator: std.mem.Allocator,
    schema_elem: format.SchemaElement,
    value_data: []const u8,
    num_values: usize,
    max_def_level: u8,
    max_rep_level: u8,
    def_level_encoding: format.Encoding,
    rep_level_encoding: format.Encoding,
) !DynamicDecodeResult {
    const physical_type = schema_elem.type_ orelse return error.InvalidArgument;
    
    // Decode levels first to get data offset
    const levels_info = try decodeLevelsForDynamicWithEncoding(
        allocator, value_data, num_values, max_def_level, max_rep_level,
        def_level_encoding, rep_level_encoding,
    );
    defer allocator.free(levels_info.def_mask);
    
    const values_data = value_data[levels_info.data_offset..];
    const values = try allocator.alloc(Value, num_values);
    errdefer allocator.free(values);
    
    if (physical_type == .int32) {
        const decoded = delta_binary_packed.decodeInt32(allocator, values_data) catch |err| {
            return switch (err) {
                error.OutOfMemory => error.OutOfMemory,
                else => error.InvalidArgument,
            };
        };
        defer allocator.free(decoded);
        
        // Map decoded values to output, respecting nulls
        var decoded_idx: usize = 0;
        for (0..num_values) |i| {
            if (levels_info.def_mask[i]) {
                if (decoded_idx < decoded.len) {
                    values[i] = .{ .int32_val = decoded[decoded_idx] };
                    decoded_idx += 1;
                } else {
                    values[i] = .{ .null_val = {} };
                }
            } else {
                values[i] = .{ .null_val = {} };
            }
        }
    } else if (physical_type == .int64) {
        const decoded = delta_binary_packed.decodeInt64(allocator, values_data) catch |err| {
            return switch (err) {
                error.OutOfMemory => error.OutOfMemory,
                else => error.InvalidArgument,
            };
        };
        defer allocator.free(decoded);
        
        var decoded_idx: usize = 0;
        for (0..num_values) |i| {
            if (levels_info.def_mask[i]) {
                if (decoded_idx < decoded.len) {
                    values[i] = .{ .int64_val = decoded[decoded_idx] };
                    decoded_idx += 1;
                } else {
                    values[i] = .{ .null_val = {} };
                }
            } else {
                values[i] = .{ .null_val = {} };
            }
        }
    } else {
        return error.InvalidArgument;
    }
    
    return .{
        .values = values,
        .def_levels = levels_info.def_levels,
        .rep_levels = levels_info.rep_levels,
    };
}

/// Decode DELTA_LENGTH_BYTE_ARRAY encoded byte arrays
fn decodeDeltaLengthByteArray(
    allocator: std.mem.Allocator,
    schema_elem: format.SchemaElement,
    value_data: []const u8,
    num_values: usize,
    max_def_level: u8,
    max_rep_level: u8,
    def_level_encoding: format.Encoding,
    rep_level_encoding: format.Encoding,
) !DynamicDecodeResult {
    _ = schema_elem;
    
    // Decode levels first
    const levels_info = try decodeLevelsForDynamicWithEncoding(
        allocator, value_data, num_values, max_def_level, max_rep_level,
        def_level_encoding, rep_level_encoding,
    );
    defer allocator.free(levels_info.def_mask);
    
    const values_data = value_data[levels_info.data_offset..];
    const values = try allocator.alloc(Value, num_values);
    errdefer allocator.free(values);
    
    // Decode using delta length byte array decoder
    var decode_result = delta_length_byte_array.decode(allocator, values_data) catch |err| {
        return switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            else => error.InvalidArgument,
        };
    };
    defer decode_result.deinit();
    
    // Map decoded values to output
    var decoded_idx: usize = 0;
    for (0..num_values) |i| {
        if (levels_info.def_mask[i]) {
            if (decoded_idx < decode_result.values.len) {
                values[i] = .{ .bytes_val = try allocator.dupe(u8, decode_result.values[decoded_idx]) };
                decoded_idx += 1;
            } else {
                values[i] = .{ .null_val = {} };
            }
        } else {
            values[i] = .{ .null_val = {} };
        }
    }
    
    return .{
        .values = values,
        .def_levels = levels_info.def_levels,
        .rep_levels = levels_info.rep_levels,
    };
}

/// Decode DELTA_BYTE_ARRAY (incremental) encoded strings
fn decodeDeltaByteArray(
    allocator: std.mem.Allocator,
    schema_elem: format.SchemaElement,
    value_data: []const u8,
    num_values: usize,
    max_def_level: u8,
    max_rep_level: u8,
    def_level_encoding: format.Encoding,
    rep_level_encoding: format.Encoding,
) !DynamicDecodeResult {
    _ = schema_elem;
    
    // Decode levels first
    const levels_info = try decodeLevelsForDynamicWithEncoding(
        allocator, value_data, num_values, max_def_level, max_rep_level,
        def_level_encoding, rep_level_encoding,
    );
    defer allocator.free(levels_info.def_mask);
    
    const values_data = value_data[levels_info.data_offset..];
    const values = try allocator.alloc(Value, num_values);
    errdefer allocator.free(values);
    
    // Decode using delta byte array decoder
    var decode_result = delta_byte_array.decode(allocator, values_data) catch |err| {
        return switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            else => error.InvalidArgument,
        };
    };
    defer decode_result.deinit();
    
    // Map decoded values to output
    var decoded_idx: usize = 0;
    for (0..num_values) |i| {
        if (levels_info.def_mask[i]) {
            if (decoded_idx < decode_result.values.len) {
                values[i] = .{ .bytes_val = try allocator.dupe(u8, decode_result.values[decoded_idx]) };
                decoded_idx += 1;
            } else {
                values[i] = .{ .null_val = {} };
            }
        } else {
            values[i] = .{ .null_val = {} };
        }
    }
    
    return .{
        .values = values,
        .def_levels = levels_info.def_levels,
        .rep_levels = levels_info.rep_levels,
    };
}

/// Decode BYTE_STREAM_SPLIT encoded float/double values
fn decodeByteStreamSplit(
    allocator: std.mem.Allocator,
    schema_elem: format.SchemaElement,
    value_data: []const u8,
    num_values: usize,
    max_def_level: u8,
    max_rep_level: u8,
    def_level_encoding: format.Encoding,
    rep_level_encoding: format.Encoding,
) !DynamicDecodeResult {
    const physical_type = schema_elem.type_ orelse return error.InvalidArgument;
    
    // Decode levels first
    const levels_info = try decodeLevelsForDynamicWithEncoding(
        allocator, value_data, num_values, max_def_level, max_rep_level,
        def_level_encoding, rep_level_encoding,
    );
    defer allocator.free(levels_info.def_mask);
    
    const values_data = value_data[levels_info.data_offset..];
    const values = try allocator.alloc(Value, num_values);
    errdefer allocator.free(values);
    
    if (physical_type == .float) {
        const decoded = byte_stream_split.decodeFloat32Alloc(allocator, values_data, levels_info.non_null_count) catch |err| {
            return switch (err) {
                error.OutOfMemory => error.OutOfMemory,
                else => error.InvalidArgument,
            };
        };
        defer allocator.free(decoded);
        
        var decoded_idx: usize = 0;
        for (0..num_values) |i| {
            if (levels_info.def_mask[i]) {
                if (decoded_idx < decoded.len) {
                    values[i] = .{ .float_val = decoded[decoded_idx] };
                    decoded_idx += 1;
                } else {
                    values[i] = .{ .null_val = {} };
                }
            } else {
                values[i] = .{ .null_val = {} };
            }
        }
    } else if (physical_type == .double) {
        const decoded = byte_stream_split.decodeFloat64Alloc(allocator, values_data, levels_info.non_null_count) catch |err| {
            return switch (err) {
                error.OutOfMemory => error.OutOfMemory,
                else => error.InvalidArgument,
            };
        };
        defer allocator.free(decoded);
        
        var decoded_idx: usize = 0;
        for (0..num_values) |i| {
            if (levels_info.def_mask[i]) {
                if (decoded_idx < decoded.len) {
                    values[i] = .{ .double_val = decoded[decoded_idx] };
                    decoded_idx += 1;
                } else {
                    values[i] = .{ .null_val = {} };
                }
            } else {
                values[i] = .{ .null_val = {} };
            }
        }
    } else if (physical_type == .int32) {
        const decoded = byte_stream_split.decodeInt32Alloc(allocator, values_data, levels_info.non_null_count) catch |err| {
            return switch (err) {
                error.OutOfMemory => error.OutOfMemory,
                else => error.InvalidArgument,
            };
        };
        defer allocator.free(decoded);
        
        var decoded_idx: usize = 0;
        for (0..num_values) |i| {
            if (levels_info.def_mask[i]) {
                if (decoded_idx < decoded.len) {
                    values[i] = .{ .int32_val = decoded[decoded_idx] };
                    decoded_idx += 1;
                } else {
                    values[i] = .{ .null_val = {} };
                }
            } else {
                values[i] = .{ .null_val = {} };
            }
        }
    } else if (physical_type == .int64) {
        const decoded = byte_stream_split.decodeInt64Alloc(allocator, values_data, levels_info.non_null_count) catch |err| {
            return switch (err) {
                error.OutOfMemory => error.OutOfMemory,
                else => error.InvalidArgument,
            };
        };
        defer allocator.free(decoded);
        
        var decoded_idx: usize = 0;
        for (0..num_values) |i| {
            if (levels_info.def_mask[i]) {
                if (decoded_idx < decoded.len) {
                    values[i] = .{ .int64_val = decoded[decoded_idx] };
                    decoded_idx += 1;
                } else {
                    values[i] = .{ .null_val = {} };
                }
            } else {
                values[i] = .{ .null_val = {} };
            }
        }
    } else if (physical_type == .fixed_len_byte_array) {
        const type_length = try safeTypeLength(schema_elem.type_length);
        if (type_length == 0) return error.InvalidTypeLength;
        const decoded = byte_stream_split.decodeFixedLenAlloc(allocator, values_data, levels_info.non_null_count, type_length) catch |err| {
            return switch (err) {
                error.OutOfMemory => error.OutOfMemory,
                else => error.InvalidArgument,
            };
        };
        defer {
            for (decoded) |slice| allocator.free(slice);
            allocator.free(decoded);
        }
        
        var decoded_idx: usize = 0;
        for (0..num_values) |i| {
            if (levels_info.def_mask[i]) {
                if (decoded_idx < decoded.len) {
                    // Copy the bytes to owned memory
                    const owned = try allocator.dupe(u8, decoded[decoded_idx]);
                    values[i] = .{ .fixed_bytes_val = owned };
                    decoded_idx += 1;
                } else {
                    values[i] = .{ .null_val = {} };
                }
            } else {
                values[i] = .{ .null_val = {} };
            }
        }
    } else {
        return error.InvalidArgument;
    }
    
    return .{
        .values = values,
        .def_levels = levels_info.def_levels,
        .rep_levels = levels_info.rep_levels,
    };
}

