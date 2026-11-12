//! DELTA_BINARY_PACKED Encoding
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
//! Decoding: unpack bits → add min_delta → add to previous value
//!
//! Reference: https://parquet.apache.org/docs/file-format/data-pages/encodings/#delta-encoding-delta_binary_packed--5

const std = @import("std");
const safe = @import("../safe.zig");

pub const Error = error{
    InvalidHeader,
    InvalidBlockSize,
    InvalidMiniBlockCount,
    InvalidValueCount,
    InsufficientData,
    VarintOverflow,
    OutOfMemory,
};

/// Decode DELTA_BINARY_PACKED encoded i32 values. The values are allocated before they are
/// decoded, so a header that declares more than `max_values` values (the count the caller
/// expects, such as a page's non-null values) returns `error.InvalidValueCount`.
pub fn decodeInt32(allocator: std.mem.Allocator, data: []const u8, max_values: usize) Error![]i32 {
    var decoder = Decoder(i32).init(data, max_values);
    return decoder.decode(allocator);
}

/// Decode DELTA_BINARY_PACKED encoded i64 values; see `decodeInt32`.
pub fn decodeInt64(allocator: std.mem.Allocator, data: []const u8, max_values: usize) Error![]i64 {
    var decoder = Decoder(i64).init(data, max_values);
    return decoder.decode(allocator);
}

/// Generic delta binary packed decoder
pub fn Decoder(comptime T: type) type {
    return struct {
        const Self = @This();

        data: []const u8,
        pos: usize,

        // Header values
        block_size: usize,
        num_mini_blocks: usize,
        total_values: usize,
        first_value: T,
        /// Largest value count the header may declare.
        max_values: usize,

        /// A decoder that returns `error.InvalidValueCount` when the header declares more than
        /// `max_values` values.
        pub fn init(data: []const u8, max_values: usize) Self {
            return .{
                .data = data,
                .pos = 0,
                .block_size = 0,
                .num_mini_blocks = 0,
                .total_values = 0,
                .first_value = 0,
                .max_values = max_values,
            };
        }

        pub fn decode(self: *Self, allocator: std.mem.Allocator) Error![]T {
            // Parse header
            try self.readHeader();

            if (self.total_values == 0) {
                return allocator.alloc(T, 0) catch return error.OutOfMemory;
            }

            // Allocate result
            const result = allocator.alloc(T, self.total_values) catch return error.OutOfMemory;
            errdefer allocator.free(result);

            // First value is stored directly
            result[0] = self.first_value;

            if (self.total_values == 1) {
                return result;
            }

            // Decode remaining values in blocks
            var values_decoded: usize = 1;
            var last_value = self.first_value;
            const mini_block_size = self.block_size / self.num_mini_blocks;

            while (values_decoded < self.total_values) {
                // Read block header
                const min_delta_raw = try self.readSignedVarint();
                // Validate min_delta fits in T
                const min_delta = std.math.cast(T, min_delta_raw) orelse return error.InvalidValueCount;

                // Read bit widths for each mini-block
                if (self.num_mini_blocks > self.data.len - self.pos) {
                    return error.InsufficientData;
                }
            const bit_widths = safe.slice(self.data, self.pos, self.num_mini_blocks) catch return error.InsufficientData;
                self.pos += self.num_mini_blocks;

                // Decode each mini-block
                for (bit_widths) |bit_width| {
                    if (values_decoded >= self.total_values) break;

                    const values_in_mini_block = @min(mini_block_size, self.total_values - values_decoded);

                    if (bit_width == 0) {
                        // All values have delta = min_delta
                        for (0..values_in_mini_block) |_| {
                            const value = last_value +% min_delta;
                            result[values_decoded] = value;
                            last_value = value;
                            values_decoded += 1;
                        }
                    } else {
                        // Bit-packed values. A delta of an i64 column takes at most 64 bits.
                        if (bit_width > 64) return error.InvalidHeader;
                        const mini_block_bits = std.math.mul(usize, mini_block_size, bit_width) catch return error.InsufficientData;
                        const mini_block_bytes = mini_block_bits / 8 + @intFromBool(mini_block_bits % 8 != 0);
                        if (mini_block_bytes > self.data.len - self.pos) {
                            return error.InsufficientData;
                        }
                        const packed_data = safe.slice(self.data, self.pos, mini_block_bytes) catch return error.InsufficientData;
                        self.pos += mini_block_bytes;

                        // Unpack and accumulate
                        try self.unpackMiniBlock(packed_data, bit_width, min_delta, &last_value, safe.sliceMutOf(T, result, values_decoded, values_in_mini_block) catch return error.InsufficientData);
                        values_decoded += values_in_mini_block;
                    }
                }
            }

            return result;
        }

        fn readHeader(self: *Self) Error!void {
            const block_size_raw = try self.readUnsignedVarint();
            const num_mini_blocks_raw = try self.readUnsignedVarint();
            const total_values_raw = try self.readUnsignedVarint();
            const first_value_raw = try self.readSignedVarint();

            // Validate and cast header values
            self.block_size = std.math.cast(usize, block_size_raw) orelse return error.InvalidBlockSize;
            self.num_mini_blocks = std.math.cast(usize, num_mini_blocks_raw) orelse return error.InvalidMiniBlockCount;
            self.total_values = std.math.cast(usize, total_values_raw) orelse return error.InvalidValueCount;
            self.first_value = std.math.cast(T, first_value_raw) orelse return error.InvalidValueCount;

            // Validate header
            if (self.block_size == 0 or self.block_size % 128 != 0) {
                return error.InvalidBlockSize;
            }
            if (self.num_mini_blocks == 0 or self.num_mini_blocks > self.block_size) {
                return error.InvalidMiniBlockCount;
            }
            if (self.total_values > self.max_values) {
                return error.InvalidValueCount;
            }
            const mini_block_size = self.block_size / self.num_mini_blocks;
            if (mini_block_size % 32 != 0) {
                return error.InvalidMiniBlockCount;
            }
        }

        /// A ULEB128 varint of at most 10 bytes. Bits past the 64th are an error.
        fn readUnsignedVarint(self: *Self) Error!u64 {
            var result: u64 = 0;
            var shift: u32 = 0;

            while (self.pos < self.data.len) {
                const b = self.data[self.pos];
                self.pos += 1;

                const bits: u64 = b & 0x7F;
                const s: u6 = @intCast(shift); // shift <= 63, checked below before it grows past
                if (shift == 63 and bits > 1) return error.VarintOverflow;
                result |= bits << s;

                if (b & 0x80 == 0) {
                    return result;
                }

                shift += 7;
                if (shift > 63) {
                    return error.VarintOverflow;
                }
            }

            return error.InsufficientData;
        }

        fn readSignedVarint(self: *Self) Error!i64 {
            // Zigzag: (n >> 1) ^ -(n & 1), with a logical shift.
            const n = try self.readUnsignedVarint();
            return @bitCast((n >> 1) ^ (0 -% (n & 1)));
        }

        fn unpackMiniBlock(
            self: *Self,
            packed_data: []const u8,
            bit_width: u8,
            min_delta: T,
            last_value: *T,
            output: []T,
        ) Error!void {
            _ = self;

            // Validate bit_width is reasonable (max 64 bits for u64 values)
            if (bit_width > 64) return error.InvalidHeader;

            var bit_pos: usize = 0;
            const mask = if (bit_width >= 64) ~@as(u64, 0) else (@as(u64, 1) << (safe.castTo(u6, bit_width) catch unreachable)) - 1; // bit_width <= 64 from check above

            for (output) |*out| {
                // Extract bit_width bits starting at bit_pos
                const byte_pos = bit_pos / 8;
                const bit_offset: u6 = safe.castTo(u6, bit_pos % 8) catch unreachable; // bit_pos % 8 is strictly 0-7, fits u6

                // A value starts at a bit offset of up to 7 and spans up to 64 bits, so up to 9
                // bytes: accumulate them in a u128. Bytes past the end of `packed_data` count as 0.
                var bits: u128 = 0;
                const bytes_needed = (@as(usize, bit_offset) + bit_width + 7) / 8;
                const bytes_to_read = @min(bytes_needed, packed_data.len -| byte_pos);
                for (0..bytes_to_read) |i| {
                    bits |= @as(u128, packed_data[byte_pos + i]) << (safe.castTo(u7, i * 8) catch unreachable); // i < 9, so i * 8 <= 64, fits u7
                }

                // Shift and mask
                const value: u64 = @truncate(bits >> bit_offset);
                const masked = value & mask;

                // Add min_delta and accumulate
                // The packed value is an unsigned offset from min_delta
                // Do all arithmetic in i128 to avoid overflow, then truncate to T
                const delta_i128: i128 = @as(i128, min_delta) + @as(i128, masked);
                const last_i128: i128 = @as(i128, last_value.*);
                const new_value_i128 = last_i128 + delta_i128;
                const new_value: T = @truncate(@as(i64, @truncate(new_value_i128)));
                out.* = new_value;
                last_value.* = new_value;

                bit_pos += bit_width;
            }
        }
    };
}

// =============================================================================
// Tests
// =============================================================================

/// The value count the tests allow; more than any test's data holds.
const test_max_values = 1 << 20;

test "delta binary packed decode simple sequence" {
    const allocator = std.testing.allocator;

    // Manually encoded: [1, 2, 3, 4, 5]
    // Header: block_size=128, mini_blocks=4, total=5, first=1
    // Block: min_delta=1, bit_widths=[0,0,0,0] (all deltas are exactly 1)
    const data = [_]u8{
        0x80, 0x01, // block_size = 128 (varint)
        0x04, // num_mini_blocks = 4
        0x05, // total_values = 5
        0x02, // first_value = 1 (zigzag: 1 -> 2)
        0x02, // min_delta = 1 (zigzag: 1 -> 2)
        0x00, 0x00, 0x00, 0x00, // bit_widths all 0
    };

    const result = try decodeInt32(allocator, &data, test_max_values);
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 5), result.len);
    try std.testing.expectEqual(@as(i32, 1), result[0]);
    try std.testing.expectEqual(@as(i32, 2), result[1]);
    try std.testing.expectEqual(@as(i32, 3), result[2]);
    try std.testing.expectEqual(@as(i32, 4), result[3]);
    try std.testing.expectEqual(@as(i32, 5), result[4]);
}

test "delta binary packed decode single value" {
    const allocator = std.testing.allocator;

    // Single value: 42
    const data = [_]u8{
        0x80, 0x01, // block_size = 128
        0x04, // num_mini_blocks = 4
        0x01, // total_values = 1
        0x54, // first_value = 42 (zigzag: 42 -> 84 = 0x54)
    };

    const result = try decodeInt32(allocator, &data, test_max_values);
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 1), result.len);
    try std.testing.expectEqual(@as(i32, 42), result[0]);
}

test "delta binary packed decode empty" {
    const allocator = std.testing.allocator;

    const data = [_]u8{
        0x80, 0x01, // block_size = 128
        0x04, // num_mini_blocks = 4
        0x00, // total_values = 0
        0x00, // first_value = 0
    };

    const result = try decodeInt32(allocator, &data, test_max_values);
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 0), result.len);
}

test "delta binary packed decode negative values" {
    const allocator = std.testing.allocator;

    // Values: [-5, -4, -3] (deltas of +1)
    const data = [_]u8{
        0x80, 0x01, // block_size = 128
        0x04, // num_mini_blocks = 4
        0x03, // total_values = 3
        0x09, // first_value = -5 (zigzag: -5 -> 9)
        0x02, // min_delta = 1 (zigzag: 1 -> 2)
        0x00, 0x00, 0x00, 0x00, // bit_widths all 0
    };

    const result = try decodeInt32(allocator, &data, test_max_values);
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 3), result.len);
    try std.testing.expectEqual(@as(i32, -5), result[0]);
    try std.testing.expectEqual(@as(i32, -4), result[1]);
    try std.testing.expectEqual(@as(i32, -3), result[2]);
}

test "delta binary packed i64" {
    const allocator = std.testing.allocator;

    // Values: [1000000000000, 1000000000001, 1000000000002]
    // For simplicity, test with sequential values
    const data = [_]u8{
        0x80, 0x01, // block_size = 128
        0x04, // num_mini_blocks = 4
        0x03, // total_values = 3
        // first_value = 100 (zigzag: 100 -> 200 = 0xC8, 0x01)
        0xC8,
        0x01,
        0x02, // min_delta = 1
        0x00, 0x00, 0x00, 0x00, // bit_widths all 0
    };

    const result = try decodeInt64(allocator, &data, test_max_values);
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 3), result.len);
    try std.testing.expectEqual(@as(i64, 100), result[0]);
    try std.testing.expectEqual(@as(i64, 101), result[1]);
    try std.testing.expectEqual(@as(i64, 102), result[2]);
}

test "delta binary packed: 63-bit mini-block values spanning 9 bytes" {
    // Header: block_size 128, 4 mini-blocks, 3 values, first value 0.
    // Block: min_delta 0, bit widths [63, 0, 0, 0]; the first mini-block holds deltas
    // 1 and 2^62 + 5. The second delta starts at bit 63 (bit offset 7 in byte 7 of the
    // mini-block) and spans its bytes 7..15, nine bytes.
    const allocator = std.testing.allocator;
    var data: [10 + 252]u8 = @splat(0);
    data[0] = 0x80; // block_size 128 (varint 0x80 0x01)
    data[1] = 0x01;
    data[2] = 4; // mini-blocks
    data[3] = 3; // total values
    data[4] = 0; // first value 0 (zigzag)
    data[5] = 0; // min_delta 0 (zigzag)
    data[6] = 63; // bit widths: data[6..10]
    const mini_block = data[10..][0..252]; // 32 values of 63 bits
    const deltas = [_]u64{ 1, (@as(u64, 1) << 62) + 5 };
    for (deltas, 0..) |d, k| {
        for (0..63) |b| {
            if ((d >> @intCast(b)) & 1 == 1) {
                const bit = k * 63 + b;
                mini_block[bit / 8] |= @as(u8, 1) << @intCast(bit % 8);
            }
        }
    }
    const values = try decodeInt64(allocator, &data, test_max_values);
    defer allocator.free(values);
    try std.testing.expectEqualSlices(i64, &.{ 0, 1, 1 + (@as(i64, 1) << 62) + 5 }, values);
}

test "delta binary packed rejects an overlong varint and more values than allowed" {
    const allocator = std.testing.allocator;
    // block_size as a varint of 11 bytes.
    const overlong = [_]u8{ 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01 };
    try std.testing.expectError(error.VarintOverflow, decodeInt32(allocator, &overlong, test_max_values));
    // block_size 128, 4 mini-blocks, 1000 values, first value 0: more than the 10 allowed.
    const many = [_]u8{ 0x80, 0x01, 0x04, 0xe8, 0x07, 0x00 };
    try std.testing.expectError(error.InvalidValueCount, decodeInt32(allocator, &many, 10));
}
