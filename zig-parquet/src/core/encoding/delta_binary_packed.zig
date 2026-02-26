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

/// Decode DELTA_BINARY_PACKED encoded i32 values
pub fn decodeInt32(allocator: std.mem.Allocator, data: []const u8) Error![]i32 {
    var decoder = Decoder(i32).init(data);
    return decoder.decode(allocator);
}

/// Decode DELTA_BINARY_PACKED encoded i64 values
pub fn decodeInt64(allocator: std.mem.Allocator, data: []const u8) Error![]i64 {
    var decoder = Decoder(i64).init(data);
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

        pub fn init(data: []const u8) Self {
            return .{
                .data = data,
                .pos = 0,
                .block_size = 0,
                .num_mini_blocks = 0,
                .total_values = 0,
                .first_value = 0,
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
                if (self.pos + self.num_mini_blocks > self.data.len) {
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
                        // Bit-packed values
                        const mini_block_bytes = (mini_block_size * bit_width + 7) / 8;
                        if (self.pos + mini_block_bytes > self.data.len) {
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
            if (self.num_mini_blocks == 0) {
                return error.InvalidMiniBlockCount;
            }
            const mini_block_size = self.block_size / self.num_mini_blocks;
            if (mini_block_size % 32 != 0) {
                return error.InvalidMiniBlockCount;
            }
        }

        fn readUnsignedVarint(self: *Self) Error!u64 {
            var result: u64 = 0;
            var shift: u6 = 0;

            while (self.pos < self.data.len) {
                const b = self.data[self.pos];
                self.pos += 1;

                result |= @as(u64, b & 0x7F) << shift;

                if (b & 0x80 == 0) {
                    return result;
                }

                shift += 7;
                if (shift >= 64) {
                    return error.VarintOverflow;
                }
            }

            return error.InsufficientData;
        }

        fn readSignedVarint(self: *Self) Error!i64 {
            // Zigzag decode
            const unsigned = try self.readUnsignedVarint();
            // Zigzag: (n >> 1) ^ -(n & 1)
            const n: i64 = @bitCast(unsigned);
            return (n >> 1) ^ (-(n & 1));
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
            // bit_width is <= 64 from earlier checks so it can fit in u6 shift
            const mask = if (bit_width >= 64) ~@as(u64, 0) else (@as(u64, 1) << (safe.castTo(u6, bit_width) catch unreachable)) - 1;

            for (output) |*out| {
                // Extract bit_width bits starting at bit_pos
                const byte_pos = bit_pos / 8;
                // bit_pos % 8 is strictly 0-7, which safely fits in u6
                const bit_offset: u6 = safe.castTo(u6, bit_pos % 8) catch unreachable;

                // Read up to 8 bytes (enough for 64 bits)
                var value: u64 = 0;
                const bytes_needed = (bit_offset + bit_width + 7) / 8;

                        const bytes_to_read = @min(bytes_needed, packed_data.len - byte_pos);
                        for (0..@min(bytes_to_read, 8)) |i| {
                            // i <= 8 from earlier conditions. i * 8 <= 64, well within u6 bounds.
                            value |= @as(u64, packed_data[byte_pos + i]) << (safe.castTo(u6, i * 8) catch unreachable);
                        }

                // Shift and mask
                value = (value >> bit_offset) & mask;

                // Add min_delta and accumulate
                // The packed value is an unsigned offset from min_delta
                // Do all arithmetic in i128 to avoid overflow, then truncate to T
                const delta_i128: i128 = @as(i128, min_delta) + @as(i128, value);
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

    const result = try decodeInt32(allocator, &data);
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

    const result = try decodeInt32(allocator, &data);
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

    const result = try decodeInt32(allocator, &data);
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

    const result = try decodeInt32(allocator, &data);
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

    const result = try decodeInt64(allocator, &data);
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 3), result.len);
    try std.testing.expectEqual(@as(i64, 100), result[0]);
    try std.testing.expectEqual(@as(i64, 101), result[1]);
    try std.testing.expectEqual(@as(i64, 102), result[2]);
}
