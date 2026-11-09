//! RLE/Bit-Packed Hybrid Encoding
//!
//! This encoding is used for:
//! - Definition levels
//! - Repetition levels
//! - Dictionary indices
//!
//! The encoding alternates between:
//! - RLE runs: repeated value
//! - Bit-packed runs: groups of 8 values packed by bit width
//!
//! Reference: https://parquet.apache.org/docs/file-format/data-pages/encodings/#rle

const std = @import("std");
const safe = @import("../safe.zig");

/// Decode RLE/bit-packed hybrid encoded data
/// Returns a slice of u32 values (caller owns the memory)
pub fn decode(allocator: std.mem.Allocator, data: []const u8, bit_width: u5, num_values: usize) ![]u32 {
    const result = try allocator.alloc(u32, num_values);
    errdefer allocator.free(result);
    try decodeInto(data, bit_width, result);
    return result;
}

/// Decode RLE/bit-packed hybrid encoded data into `result`, filling all of it.
/// Entries the data does not cover are set to 0.
pub fn decodeInto(data: []const u8, bit_width: u5, result: []u32) !void {
    const num_values = result.len;
    if (bit_width == 0) {
        // All values are 0
        @memset(result, 0);
        return;
    }

    var pos: usize = 0;
    var out_idx: usize = 0;

    while (pos < data.len and out_idx < num_values) {
        // Read the indicator (varint)
        const indicator = readVarInt(data[pos..]) catch break;
        pos += indicator.bytes_read;

        if (indicator.value & 1 == 1) {
            // Bit-packed run
            // indicator >> 1 = number of groups of 8 values
            const num_groups = indicator.value >> 1;
            const num_packed_values = std.math.mul(usize, num_groups, 8) catch return error.EndOfData;

            const values_to_read = @min(num_packed_values, num_values - out_idx);
            const out_slice = safe.sliceMutOf(u32, result, out_idx, values_to_read) catch return error.EndOfData;
            try decodeBitPacked(safe.slice(data, pos, data.len - pos) catch return error.EndOfData, bit_width, out_slice);
            out_idx += values_to_read;

            // Advance position by the number of bytes used
            const total_bits = std.math.mul(usize, num_packed_values, bit_width) catch return error.EndOfData;
            const bytes_used = (total_bits + 7) / 8;
            pos += bytes_used;
        } else {
            // RLE run
            // indicator >> 1 = repeat count
            const repeat_count = indicator.value >> 1;
            const values_to_write = @min(repeat_count, num_values - out_idx);

            // Read the value (bit_width bits, rounded up to bytes)
            const value_bytes = (bit_width + 7) / 8;
            if (pos + value_bytes > data.len) break;

            var value: u32 = 0;
            for (0..value_bytes) |i| {
                value |= @as(u32, data[pos + i]) << (safe.castTo(u5, i * 8) catch unreachable); // value_bytes <= 4, so i * 8 <= 24
            }
            // Mask to bit_width
            const mask = (@as(u32, 1) << bit_width) - 1;
            value &= mask;
            pos += value_bytes;

            // Fill with repeated value
            @memset(result[out_idx..][0..values_to_write], value);
            out_idx += values_to_write;
        }
    }

    // Fill remaining with 0 if we didn't get enough values
    while (out_idx < num_values) {
        result[out_idx] = 0;
        out_idx += 1;
    }
}

/// Decode definition levels (special case for bool output)
/// Use this for flat schemas where max_def_level = 1 (just need defined/not-defined)
pub fn decodeDefLevels(allocator: std.mem.Allocator, data: []const u8, bit_width: u5, num_values: usize) ![]bool {
    const values = try decode(allocator, data, bit_width, num_values);
    defer allocator.free(values);

    var result = try allocator.alloc(bool, num_values);
    for (0..num_values) |i| {
        result[i] = values[i] != 0;
    }
    return result;
}

/// Decode definition or repetition levels as raw u32 values
/// Use this for nested schemas where max_def_level > 1 and you need actual level values
pub fn decodeLevels(allocator: std.mem.Allocator, data: []const u8, bit_width: u5, num_values: usize) ![]u32 {
    return decode(allocator, data, bit_width, num_values);
}

/// Decode pure bit-packed levels (without RLE hybrid wrapper)
/// This is used for old Parquet files that use BIT_PACKED encoding for definition/repetition levels
/// Unlike RLE hybrid, this format has no length prefix and no indicator bytes - just raw bit-packed values
pub fn decodeBitPackedLevels(allocator: std.mem.Allocator, data: []const u8, bit_width: u5, num_values: usize) ![]u32 {
    const result = try allocator.alloc(u32, num_values);
    errdefer allocator.free(result);
    try decodeBitPacked(data, bit_width, result);
    return result;
}

/// Decode pure bit-packed levels into `result`, filling all of it.
pub fn decodeBitPackedLevelsInto(data: []const u8, bit_width: u5, result: []u32) !void {
    try decodeBitPacked(data, bit_width, result);
}

/// Calculate the byte size needed for bit-packed levels
pub fn bitPackedSize(num_values: usize, bit_width: u5) usize {
    return (num_values * bit_width + 7) / 8;
}

const VarIntResult = struct {
    value: usize,
    bytes_read: usize,
};

fn readVarInt(data: []const u8) !VarIntResult {
    var result: usize = 0;
    var shift: usize = 0;
    var i: usize = 0;

    const ShiftType = std.math.Log2Int(usize);

    while (i < data.len) {
        // Check shift before using it to avoid overflow
        if (shift >= @bitSizeOf(usize)) return error.VarIntTooLong;

        const b = data[i];
        i += 1;
        result |= @as(usize, b & 0x7F) << (safe.castTo(ShiftType, shift) catch unreachable); // shift < @bitSizeOf(usize) checked above
        if (b & 0x80 == 0) {
            return .{ .value = result, .bytes_read = i };
        }
        shift += 7;
    }
    return error.EndOfData;
}

/// Unpacks `output.len` values of `bit_width` bits, LSB first. Values past the end of `data`
/// are 0, and bytes missing from a value at the end of `data` count as 0.
fn decodeBitPacked(data: []const u8, bit_width: u5, output: []u32) !void {
    if (bit_width == 0) {
        @memset(output, 0);
        return;
    }
    const mask = (@as(u64, 1) << bit_width) - 1;
    var bit_pos: usize = 0;
    var i: usize = 0;

    // While 8 bytes are available at the value's first byte, read them as one u64. A value
    // spans at most 5 bytes (bit offset up to 7 plus up to 31 bits).
    while (i < output.len) : (i += 1) {
        const byte_pos = bit_pos / 8;
        if (data.len < 8 or byte_pos > data.len - 8) break;
        const word = std.mem.readInt(u64, data[byte_pos..][0..8], .little);
        output[i] = @intCast((word >> @intCast(bit_pos % 8)) & mask);
        bit_pos += bit_width;
    }

    // Remaining values: read byte by byte, treating bytes past the end of `data` as 0.
    while (i < output.len) : (i += 1) {
        const byte_pos = bit_pos / 8;
        if (byte_pos >= data.len) {
            @memset(output[i..], 0);
            return;
        }
        const bit_offset: u6 = @intCast(bit_pos % 8);
        const bytes_needed = (bit_offset + bit_width + 7) / 8;
        var value: u64 = 0;
        for (0..bytes_needed) |j| {
            if (byte_pos + j < data.len) value |= @as(u64, data[byte_pos + j]) << @intCast(j * 8);
        }
        output[i] = @intCast((value >> bit_offset) & mask);
        bit_pos += bit_width;
    }
}

// Tests
test "RLE decode simple run" {
    const allocator = std.testing.allocator;

    // RLE run: indicator = 6 (3 << 1 | 0), value = 0x05
    // This means: repeat value 5 three times
    const data = [_]u8{ 0x06, 0x05 };
    const result = try decode(allocator, &data, 8, 3);
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 3), result.len);
    try std.testing.expectEqual(@as(u32, 5), result[0]);
    try std.testing.expectEqual(@as(u32, 5), result[1]);
    try std.testing.expectEqual(@as(u32, 5), result[2]);
}

test "RLE decode bit-packed run" {
    const allocator = std.testing.allocator;

    // Bit-packed run: indicator = 0x03 (1 group of 8 values, bit-packed flag)
    // For bit_width=1: 8 values in 1 byte
    // Byte 0b10101010 = values [0,1,0,1,0,1,0,1]
    const data = [_]u8{ 0x03, 0xAA };
    const result = try decode(allocator, &data, 1, 8);
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 8), result.len);
    try std.testing.expectEqual(@as(u32, 0), result[0]);
    try std.testing.expectEqual(@as(u32, 1), result[1]);
    try std.testing.expectEqual(@as(u32, 0), result[2]);
    try std.testing.expectEqual(@as(u32, 1), result[3]);
}

test "RLE decode definition levels" {
    const allocator = std.testing.allocator;

    // RLE run of 4 ones, then RLE run of 1 zero
    // indicator=8 (4<<1), value=1
    // indicator=2 (1<<1), value=0
    const data = [_]u8{ 0x08, 0x01, 0x02, 0x00 };
    const result = try decodeDefLevels(allocator, &data, 1, 5);
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 5), result.len);
    try std.testing.expectEqual(true, result[0]);
    try std.testing.expectEqual(true, result[1]);
    try std.testing.expectEqual(true, result[2]);
    try std.testing.expectEqual(true, result[3]);
    try std.testing.expectEqual(false, result[4]);
}

test "RLE decode levels for nested types" {
    const allocator = std.testing.allocator;

    // For a list column with max_def_level=3:
    // 0 = null list, 1 = empty list, 2 = null element, 3 = element present
    // RLE run: indicator=10 (5<<1), value=3 (5 values of 3)
    const data = [_]u8{ 0x0A, 0x03 };
    const result = try decodeLevels(allocator, &data, 2, 5);
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 5), result.len);
    for (result) |v| {
        try std.testing.expectEqual(@as(u32, 3), v);
    }
}

test "RLE decode malicious indicator does not overflow" {
    const allocator = std.testing.allocator;

    // Craft a bit-packed indicator with a huge group count that would overflow
    // num_groups * 8 or num_packed_values * bit_width.
    // Varint encoding of 0x7FFFFFFF (max i32): 0xFF 0xFF 0xFF 0xFF 0x07
    // LSB=1 means bit-packed, so num_groups = 0x7FFFFFFF >> 1 = 0x3FFFFFFF
    // num_groups * 8 would overflow on 32-bit or produce a huge value on 64-bit.
    // With insufficient backing data, decode should not panic.
    const data = [_]u8{ 0xFF, 0xFF, 0xFF, 0xFF, 0x07 };
    const result = try decode(allocator, &data, 8, 2);
    defer allocator.free(result);

    // Should gracefully produce partial/zero results rather than panicking
    try std.testing.expect(result.len == 2);
}

/// The bit-unpacking loop `decodeBitPacked` replaced, kept as the reference for the test below.
fn decodeBitPackedReference(data: []const u8, bit_width: u5, output: []u32) void {
    var bit_pos: usize = 0;
    for (output) |*out| {
        const byte_pos = bit_pos / 8;
        const bit_offset: u3 = @intCast(bit_pos % 8);
        if (byte_pos >= data.len) {
            out.* = 0;
            continue;
        }
        var value: u64 = 0;
        const bytes_needed = (@as(usize, bit_offset) + bit_width + 7) / 8;
        for (0..bytes_needed) |i| {
            if (byte_pos + i < data.len) value |= @as(u64, data[byte_pos + i]) << @intCast(i * 8);
        }
        value >>= bit_offset;
        const mask = (@as(u64, 1) << bit_width) - 1;
        out.* = @intCast(value & mask);
        bit_pos += bit_width;
    }
}

test "decodeBitPacked matches the reference for all bit widths and short data" {
    var prng = std.Random.DefaultPrng.init(0x5eed);
    const random = prng.random();
    var data: [200]u8 = undefined;
    var got: [300]u32 = undefined;
    var want: [300]u32 = undefined;
    for (1..32) |bw| {
        const bit_width: u5 = @intCast(bw);
        for (0..40) |_| {
            const n = random.uintLessThan(usize, got.len + 1);
            // Data lengths from empty up to more than the values need.
            const len = random.uintLessThan(usize, data.len + 1);
            random.bytes(data[0..len]);
            try decodeBitPacked(data[0..len], bit_width, got[0..n]);
            decodeBitPackedReference(data[0..len], bit_width, want[0..n]);
            try std.testing.expectEqualSlices(u32, want[0..n], got[0..n]);
        }
    }
}
