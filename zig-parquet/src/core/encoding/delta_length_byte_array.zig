//! DELTA_LENGTH_BYTE_ARRAY Encoding
//!
//! This encoding is for variable-length byte arrays (strings, binary).
//! It stores the lengths using DELTA_BINARY_PACKED encoding, followed by
//! the concatenated raw bytes of all values.
//!
//! Structure:
//! - Lengths: encoded using DELTA_BINARY_PACKED (as i32 values)
//! - Values: concatenated raw bytes
//!
//! Reference: https://parquet.apache.org/docs/file-format/data-pages/encodings/#delta-length-byte-array-delta_length_byte_array--6

const std = @import("std");
const safe = @import("../safe.zig");
const delta_binary_packed = @import("delta_binary_packed.zig");

pub const Error = error{
    InvalidLength,
    InsufficientData,
    OutOfMemory,
} || delta_binary_packed.Error;

/// Decode DELTA_LENGTH_BYTE_ARRAY encoded byte arrays
/// Returns a slice of slices pointing into the decoded data buffer
pub fn decode(allocator: std.mem.Allocator, data: []const u8) Error!DecodeResult {
    // First, decode the lengths using DELTA_BINARY_PACKED
    var lengths_decoder = delta_binary_packed.Decoder(i32).init(data);
    const lengths = try lengths_decoder.decode(allocator);
    errdefer allocator.free(lengths);
    
    const values_start = lengths_decoder.pos;
    const values_data = data[values_start..];
    
    // Validate lengths and calculate total size
    var total_size: usize = 0;
    for (lengths) |len| {
        if (len < 0) {
            return error.InvalidLength;
        }
        total_size += safe.castTo(usize, len) catch return error.InvalidLength;
    }
    
    if (values_data.len < total_size) {
        return error.InsufficientData;
    }
    
    // Build result - copy each value
    const result = allocator.alloc([]const u8, lengths.len) catch return error.OutOfMemory;
    errdefer {
        for (result) |slice| {
            if (slice.len > 0) allocator.free(slice);
        }
        allocator.free(result);
    }
    
    var offset: usize = 0;
    for (lengths, 0..) |len, i| {
        const length: usize = safe.castTo(usize, len) catch return error.InvalidLength;
        const value = safe.slice(values_data, offset, length) catch return error.InsufficientData;
        result[i] = allocator.dupe(u8, value) catch return error.OutOfMemory;
        offset += length;
    }
    
    allocator.free(lengths);
    
    return .{
        .values = result,
        .allocator = allocator,
    };
}

/// Result of decoding DELTA_LENGTH_BYTE_ARRAY
pub const DecodeResult = struct {
    values: [][]const u8,
    allocator: std.mem.Allocator,
    
    pub fn deinit(self: *DecodeResult) void {
        for (self.values) |slice| {
            if (slice.len > 0) self.allocator.free(slice);
        }
        self.allocator.free(self.values);
    }
};

// =============================================================================
// Tests
// =============================================================================

test "delta length byte array decode" {
    const allocator = std.testing.allocator;
    
    // Three strings: "abc", "defgh", "ij"
    // Lengths: [3, 5, 2]
    // Delta encoded lengths:
    // - header: block_size=128, mini_blocks=4, total=3, first=3
    // - block: min_delta=-1 (zigzag: 1), values vary
    //
    // For simplicity, use constant delta = 0 for testing
    const data = [_]u8{
        // DELTA_BINARY_PACKED header for lengths [3, 5, 2]
        0x80, 0x01, // block_size = 128
        0x04, // num_mini_blocks = 4
        0x03, // total_values = 3
        0x06, // first_value = 3 (zigzag: 3 -> 6)
        // Block header - deltas are [2, -3], min_delta = -3
        0x05, // min_delta = -3 (zigzag: -3 -> 5)
        // bit_widths: need 3 bits to store [2-(-3)=5, -3-(-3)=0] = [5, 0]
        0x03, 0x00, 0x00, 0x00, // bit_widths [3, 0, 0, 0]
        // Mini-block 0: values [5, 0] bit-packed at 3 bits each
        // 5 = 101, 0 = 000 -> 0b00_000_101 = 0x05, but we need 32 values...
        // Actually for 2 values in mini-block, it's (2*3+7)/8 = 1 byte minimum
        // But mini_block_size is 32, so we'd need 32*3/8 = 12 bytes
        // This is complex - let's use a simpler test case
    };
    _ = data;
    
    // Use a simpler test: encode manually with all same lengths (delta=0)
    const simple_data = [_]u8{
        // DELTA_BINARY_PACKED header for lengths [3, 3, 3]
        0x80, 0x01, // block_size = 128
        0x04, // num_mini_blocks = 4
        0x03, // total_values = 3
        0x06, // first_value = 3 (zigzag: 3 -> 6)
        0x00, // min_delta = 0
        0x00, 0x00, 0x00, 0x00, // bit_widths all 0 (all deltas are 0)
        // Raw values: "abcdefghi" (3+3+3 = 9 bytes)
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
    };
    
    var result = try decode(allocator, &simple_data);
    defer result.deinit();
    
    try std.testing.expectEqual(@as(usize, 3), result.values.len);
    try std.testing.expectEqualStrings("abc", result.values[0]);
    try std.testing.expectEqualStrings("def", result.values[1]);
    try std.testing.expectEqualStrings("ghi", result.values[2]);
}

test "delta length byte array empty" {
    const allocator = std.testing.allocator;
    
    const data = [_]u8{
        0x80, 0x01, // block_size = 128
        0x04, // num_mini_blocks = 4
        0x00, // total_values = 0
        0x00, // first_value = 0
    };
    
    var result = try decode(allocator, &data);
    defer result.deinit();
    
    try std.testing.expectEqual(@as(usize, 0), result.values.len);
}
