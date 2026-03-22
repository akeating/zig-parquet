//! DELTA_BINARY_PACKED Encoder
//!
//! This encoding is used for integer types (INT32, INT64). It stores the first value
//! directly, then encodes subsequent values as deltas from the previous value.
//!
//! Structure:
//! - Header (all unsigned varints except first_value which is signed):
//!   - block_size: number of values per block (must be multiple of 128)
//!   - num_mini_blocks: number of mini-blocks per block
//!   - total_values: total count of values in the encoded data
//!   - first_value: the first value (signed zigzag-encoded varint)
//!
//! - For each block:
//!   - min_delta: minimum delta in the block (signed zigzag-encoded varint)
//!   - bit_widths: one byte per mini-block, the bit width used
//!   - mini-blocks: bit-packed relative deltas (delta - min_delta)
//!
//! Reference: https://parquet.apache.org/docs/file-format/data-pages/encodings/#delta-encoding-delta_binary_packed--5

const std = @import("std");
const safe = @import("../safe.zig");

pub const Error = error{
    OutOfMemory,
    InvalidInput,
};

// Standard block configuration (matches parquet-go and Arrow)
const BLOCK_SIZE: usize = 128;
const NUM_MINI_BLOCKS: usize = 4;
const MINI_BLOCK_SIZE: usize = BLOCK_SIZE / NUM_MINI_BLOCKS; // 32

/// Encode i32 values using DELTA_BINARY_PACKED encoding
pub fn encodeInt32(allocator: std.mem.Allocator, values: []const i32) Error![]u8 {
    return encodeGeneric(i32, allocator, values);
}

/// Encode i64 values using DELTA_BINARY_PACKED encoding
pub fn encodeInt64(allocator: std.mem.Allocator, values: []const i64) Error![]u8 {
    return encodeGeneric(i64, allocator, values);
}

/// Generic encoder for any signed integer type
fn encodeGeneric(comptime T: type, allocator: std.mem.Allocator, values: []const T) Error![]u8 {
    var buffer: std.ArrayList(u8) = .empty;
    errdefer buffer.deinit(allocator);

    if (values.len == 0) {
        // Write header with 0 values
        try writeUnsignedVarint(&buffer, allocator, BLOCK_SIZE);
        try writeUnsignedVarint(&buffer, allocator, NUM_MINI_BLOCKS);
        try writeUnsignedVarint(&buffer, allocator, 0); // total_values
        try writeSignedVarint(&buffer, allocator, 0); // first_value (doesn't matter)
        return buffer.toOwnedSlice(allocator) catch return error.OutOfMemory;
    }

    // Write header
    try writeUnsignedVarint(&buffer, allocator, BLOCK_SIZE);
    try writeUnsignedVarint(&buffer, allocator, NUM_MINI_BLOCKS);
    try writeUnsignedVarint(&buffer, allocator, values.len);
    try writeSignedVarint(&buffer, allocator, values[0]); // first_value

    if (values.len == 1) {
        return buffer.toOwnedSlice(allocator) catch return error.OutOfMemory;
    }

    // Compute deltas
    const deltas = allocator.alloc(i64, values.len - 1) catch return error.OutOfMemory;
    defer allocator.free(deltas);

    for (values[1..], 0..) |val, i| {
        deltas[i] = @as(i64, val) - @as(i64, values[i]);
    }

    // Process in blocks
    var delta_idx: usize = 0;
    while (delta_idx < deltas.len) {
        const block_end = @min(delta_idx + BLOCK_SIZE, deltas.len);
        const block_deltas = deltas[delta_idx..block_end];

        try encodeBlock(&buffer, allocator, block_deltas);

        delta_idx = block_end;
    }

    return buffer.toOwnedSlice(allocator) catch return error.OutOfMemory;
}

/// Encode a single block of deltas
fn encodeBlock(buffer: *std.ArrayList(u8), allocator: std.mem.Allocator, deltas: []const i64) Error!void {
    if (deltas.len == 0) return;

    // Find min delta in block
    var min_delta: i64 = deltas[0];
    for (deltas[1..]) |d| {
        min_delta = @min(min_delta, d);
    }

    // Write min_delta
    try writeSignedVarint(buffer, allocator, min_delta);

    // Compute relative deltas and bit widths for each mini-block
    var bit_widths: [NUM_MINI_BLOCKS]u8 = .{0} ** NUM_MINI_BLOCKS;
    var mini_block_idx: usize = 0;
    var delta_idx: usize = 0;

    while (delta_idx < deltas.len and mini_block_idx < NUM_MINI_BLOCKS) {
        const mb_end = @min(delta_idx + MINI_BLOCK_SIZE, deltas.len);
        const mb_deltas = deltas[delta_idx..mb_end];

        // Find max relative delta to determine bit width
        var max_relative: u64 = 0;
        for (mb_deltas) |d| {
            const relative: u64 = safe.castTo(u64, d - min_delta) catch unreachable; // d >= min_delta by definition, so difference is non-negative
            max_relative = @max(max_relative, relative);
        }

        bit_widths[mini_block_idx] = bitWidth(max_relative);
        mini_block_idx += 1;
        delta_idx = mb_end;
    }

    // Write bit widths
    buffer.appendSlice(allocator, &bit_widths) catch return error.OutOfMemory;

    // Pack mini-blocks
    delta_idx = 0;
    mini_block_idx = 0;

    while (delta_idx < deltas.len and mini_block_idx < NUM_MINI_BLOCKS) {
        const mb_end = @min(delta_idx + MINI_BLOCK_SIZE, deltas.len);
        const mb_deltas = deltas[delta_idx..mb_end];
        const bw = bit_widths[mini_block_idx];

        try packMiniBlock(buffer, allocator, mb_deltas, min_delta, bw);

        mini_block_idx += 1;
        delta_idx = mb_end;
    }
}

/// Pack a mini-block of relative deltas
fn packMiniBlock(buffer: *std.ArrayList(u8), allocator: std.mem.Allocator, deltas: []const i64, min_delta: i64, bit_width: u8) Error!void {
    if (bit_width == 0) {
        // All values are min_delta, no bytes needed
        return;
    }

    // Allocate enough bytes for MINI_BLOCK_SIZE values at bit_width bits each
    // We always write full mini-blocks (padded with zeros if needed)
    const total_bits = MINI_BLOCK_SIZE * @as(usize, bit_width);
    const total_bytes = (total_bits + 7) / 8;

    const packed_data = allocator.alloc(u8, total_bytes) catch return error.OutOfMemory;
    defer allocator.free(packed_data);
    @memset(packed_data, 0);

    var bit_pos: usize = 0;
    for (0..MINI_BLOCK_SIZE) |i| {
        const relative: u64 = if (i < deltas.len)
            safe.castTo(u64, deltas[i] - min_delta) catch unreachable // deltas[i] >= min_delta by definition
        else
            0; // Padding

        // Write bit_width bits at bit_pos
        writeBits(packed_data, bit_pos, relative, bit_width);
        bit_pos += bit_width;
    }

    buffer.appendSlice(allocator, packed_data) catch return error.OutOfMemory;
}

/// Write bits to a byte array at a given bit position
fn writeBits(data: []u8, bit_pos: usize, value: u64, bit_width: u8) void {
    if (bit_width == 0) return;

    var remaining_bits: u8 = bit_width;
    var val = value;
    var pos = bit_pos;

    while (remaining_bits > 0) {
        const byte_idx = pos / 8;
        const bit_offset: u3 = safe.castTo(u3, pos % 8) catch unreachable; // pos % 8 is strictly 0-7
        const bits_available: u8 = 8 - @as(u8, bit_offset);
        const bits_in_byte: u8 = @min(remaining_bits, bits_available);

        const mask: u8 = safe.castTo(u8, (@as(u16, 1) << (safe.castTo(u4, bits_in_byte) catch unreachable)) - 1) catch unreachable; // bits_in_byte <= 8, fits u4 for shift and u8 for mask
        const byte_val: u8 = safe.castTo(u8, val & mask) catch unreachable; // by mask, it fits in u8
        data[byte_idx] |= byte_val << bit_offset;

        val >>= safe.castTo(u6, bits_in_byte) catch unreachable; // bits_in_byte <= 8
        pos += bits_in_byte;
        remaining_bits -= bits_in_byte;
    }
}

/// Compute the minimum number of bits needed to represent a value
fn bitWidth(value: u64) u8 {
    if (value == 0) return 0;
    return safe.castTo(u8, 64 - @clz(value)) catch unreachable; // value != 0, so 64 - @clz is 1..64, fits u8
}

/// Write an unsigned varint (ULEB128)
fn writeUnsignedVarint(buffer: *std.ArrayList(u8), allocator: std.mem.Allocator, value: usize) Error!void {
    var val = value;
    while (val >= 0x80) {
        buffer.append(allocator, safe.castTo(u8, (val & 0x7F) | 0x80) catch unreachable) catch return error.OutOfMemory; // low 7 bits | 0x80 fits u8
        val >>= 7;
    }
    buffer.append(allocator, safe.castTo(u8, val) catch unreachable) catch return error.OutOfMemory; // val < 0x80 after loop, fits u8
}

/// Write a signed varint (zigzag + ULEB128)
fn writeSignedVarint(buffer: *std.ArrayList(u8), allocator: std.mem.Allocator, value: i64) Error!void {
    // Zigzag encode: (n << 1) ^ (n >> 63)
    const zigzag: u64 = @bitCast((value << 1) ^ (value >> 63));
    try writeUnsignedVarint64(buffer, allocator, zigzag);
}

/// Write an unsigned 64-bit varint
fn writeUnsignedVarint64(buffer: *std.ArrayList(u8), allocator: std.mem.Allocator, value: u64) Error!void {
    var val = value;
    while (val >= 0x80) {
        buffer.append(allocator, safe.castTo(u8, (val & 0x7F) | 0x80) catch unreachable) catch return error.OutOfMemory; // low 7 bits | 0x80 fits u8
        val >>= 7;
    }
    buffer.append(allocator, safe.castTo(u8, val) catch unreachable) catch return error.OutOfMemory; // val < 0x80 after loop, fits u8
}

// =============================================================================
// Tests
// =============================================================================

test "delta binary packed encode empty" {
    const allocator = std.testing.allocator;

    const values = [_]i32{};
    const encoded = try encodeInt32(allocator, &values);
    defer allocator.free(encoded);

    // Should have header with 0 values
    try std.testing.expect(encoded.len > 0);
}

test "delta binary packed encode single value" {
    const allocator = std.testing.allocator;

    const values = [_]i32{42};
    const encoded = try encodeInt32(allocator, &values);
    defer allocator.free(encoded);

    // Decode and verify
    const decoder = @import("delta_binary_packed.zig");
    const decoded = try decoder.decodeInt32(allocator, encoded);
    defer allocator.free(decoded);

    try std.testing.expectEqual(@as(usize, 1), decoded.len);
    try std.testing.expectEqual(@as(i32, 42), decoded[0]);
}

test "delta binary packed encode simple sequence" {
    const allocator = std.testing.allocator;

    const values = [_]i32{ 1, 2, 3, 4, 5 };
    const encoded = try encodeInt32(allocator, &values);
    defer allocator.free(encoded);

    // Decode and verify
    const decoder = @import("delta_binary_packed.zig");
    const decoded = try decoder.decodeInt32(allocator, encoded);
    defer allocator.free(decoded);

    try std.testing.expectEqual(@as(usize, 5), decoded.len);
    try std.testing.expectEqualSlices(i32, &values, decoded);
}

test "delta binary packed encode with negative deltas" {
    const allocator = std.testing.allocator;

    const values = [_]i32{ 100, 90, 80, 70, 60 };
    const encoded = try encodeInt32(allocator, &values);
    defer allocator.free(encoded);

    // Decode and verify
    const decoder = @import("delta_binary_packed.zig");
    const decoded = try decoder.decodeInt32(allocator, encoded);
    defer allocator.free(decoded);

    try std.testing.expectEqualSlices(i32, &values, decoded);
}

test "delta binary packed encode i64" {
    const allocator = std.testing.allocator;

    const values = [_]i64{ 1000000000000, 1000000000001, 1000000000002 };
    const encoded = try encodeInt64(allocator, &values);
    defer allocator.free(encoded);

    // Decode and verify
    const decoder = @import("delta_binary_packed.zig");
    const decoded = try decoder.decodeInt64(allocator, encoded);
    defer allocator.free(decoded);

    try std.testing.expectEqualSlices(i64, &values, decoded);
}

test "delta binary packed encode large sequence" {
    const allocator = std.testing.allocator;

    // Create 200 values (more than one block of 128)
    var values: [200]i32 = undefined;
    for (&values, 0..) |*v, i| {
        v.* = safe.castTo(i32, i * 10) catch unreachable; // i < 200, so i * 10 <= 1990, fits i32
    }

    const encoded = try encodeInt32(allocator, &values);
    defer allocator.free(encoded);

    // Decode and verify
    const decoder = @import("delta_binary_packed.zig");
    const decoded = try decoder.decodeInt32(allocator, encoded);
    defer allocator.free(decoded);

    try std.testing.expectEqualSlices(i32, &values, decoded);
}

test "delta binary packed encode mixed deltas" {
    const allocator = std.testing.allocator;

    const values = [_]i32{ 0, 100, 50, 200, -50, 300, 0, 1 };
    const encoded = try encodeInt32(allocator, &values);
    defer allocator.free(encoded);

    // Decode and verify
    const decoder = @import("delta_binary_packed.zig");
    const decoded = try decoder.decodeInt32(allocator, encoded);
    defer allocator.free(decoded);

    try std.testing.expectEqualSlices(i32, &values, decoded);
}

test "bit width calculation" {
    try std.testing.expectEqual(@as(u8, 0), bitWidth(0));
    try std.testing.expectEqual(@as(u8, 1), bitWidth(1));
    try std.testing.expectEqual(@as(u8, 2), bitWidth(2));
    try std.testing.expectEqual(@as(u8, 2), bitWidth(3));
    try std.testing.expectEqual(@as(u8, 3), bitWidth(4));
    try std.testing.expectEqual(@as(u8, 8), bitWidth(255));
    try std.testing.expectEqual(@as(u8, 9), bitWidth(256));
}
