//! DELTA_BYTE_ARRAY Encoding (Incremental Encoding)
//!
//! This encoding is optimized for sorted strings or strings with common prefixes.
//! It stores each value as:
//! - Prefix length: how many bytes to copy from the previous value
//! - Suffix: the new bytes after the prefix
//!
//! Structure:
//! - Prefix lengths: encoded using DELTA_BINARY_PACKED (as i32)
//! - Suffix lengths: encoded using DELTA_BINARY_PACKED (as i32)
//! - Suffixes: concatenated raw suffix bytes
//!
//! Example: ["apple", "application", "apply"]
//! - "apple" -> prefix=0, suffix="apple"
//! - "application" -> prefix=5, suffix="ication" (shares "apple" prefix... wait no)
//! - Actually: "appl" is common, so prefix=4, suffix="ication"
//!
//! Reference: https://parquet.apache.org/docs/file-format/data-pages/encodings/#delta-strings-delta_byte_array--7

const std = @import("std");
const safe = @import("../safe.zig");
const delta_binary_packed = @import("delta_binary_packed.zig");

pub const Error = error{
    InvalidPrefixLength,
    InvalidSuffixLength,
    PrefixLengthTooLarge,
    InsufficientData,
    OutOfMemory,
} || delta_binary_packed.Error;

/// Decode DELTA_BYTE_ARRAY encoded strings
pub fn decode(allocator: std.mem.Allocator, data: []const u8) Error!DecodeResult {
    if (data.len == 0) {
        return .{
            .values = &[_][]const u8{},
            .allocator = allocator,
        };
    }
    
    // Decode prefix lengths
    var prefix_decoder = delta_binary_packed.Decoder(i32).init(data);
    const prefix_lengths = try prefix_decoder.decode(allocator);
    errdefer allocator.free(prefix_lengths);
    
    const num_values = prefix_lengths.len;
    if (num_values == 0) {
        allocator.free(prefix_lengths);
        return .{
            .values = allocator.alloc([]const u8, 0) catch return error.OutOfMemory,
            .allocator = allocator,
        };
    }
    
    // Decode suffix lengths from remaining data
    var suffix_decoder = delta_binary_packed.Decoder(i32).init(data[prefix_decoder.pos..]);
    const suffix_lengths = try suffix_decoder.decode(allocator);
    errdefer allocator.free(suffix_lengths);
    
    if (suffix_lengths.len != num_values) {
        return error.InsufficientData;
    }
    
    // Calculate total suffix bytes and validate
    var total_suffix_bytes: usize = 0;
    for (suffix_lengths) |len| {
        if (len < 0) {
            return error.InvalidSuffixLength;
        }
        total_suffix_bytes += safe.castTo(usize, len) catch return error.InvalidSuffixLength;
    }
    
    const suffix_data_start = prefix_decoder.pos + suffix_decoder.pos;
    const suffix_data = data[suffix_data_start..];
    
    if (suffix_data.len < total_suffix_bytes) {
        return error.InsufficientData;
    }
    
    // Build result values
    const result = allocator.alloc([]u8, num_values) catch return error.OutOfMemory;
    errdefer {
        for (result) |slice| {
            if (slice.len > 0) allocator.free(slice);
        }
        allocator.free(result);
    }
    
    var suffix_offset: usize = 0;
    var last_value: []const u8 = "";
    
    for (0..num_values) |i| {
        const prefix_len = prefix_lengths[i];
        const suffix_len = suffix_lengths[i];
        
        if (prefix_len < 0) {
            return error.InvalidPrefixLength;
        }
        
        const prefix_length: usize = safe.castTo(usize, prefix_len) catch return error.InvalidPrefixLength;
        const suffix_length: usize = safe.castTo(usize, suffix_len) catch return error.InvalidSuffixLength;
        
        if (prefix_length > last_value.len) {
            return error.PrefixLengthTooLarge;
        }
        
        // Allocate space for this value
        const value_len = prefix_length + suffix_length;
        const value = allocator.alloc(u8, value_len) catch return error.OutOfMemory;
        errdefer allocator.free(value);
        
        // Copy prefix from previous value
        if (prefix_length > 0) {
            @memcpy(value[0..prefix_length], last_value[0..prefix_length]);
        }
        
        // Copy suffix from data
        if (suffix_length > 0) {
            @memcpy(value[prefix_length..], safe.slice(suffix_data, suffix_offset, suffix_length) catch return error.InsufficientData);
        }
        
        result[i] = value;
        last_value = value;
        suffix_offset += suffix_length;
    }
    
    allocator.free(prefix_lengths);
    allocator.free(suffix_lengths);
    
    // Cast to const slices
    const const_result = allocator.alloc([]const u8, num_values) catch return error.OutOfMemory;
    for (result, 0..) |v, i| {
        const_result[i] = v;
    }
    allocator.free(result);
    
    return .{
        .values = const_result,
        .allocator = allocator,
    };
}

/// Result of decoding DELTA_BYTE_ARRAY
pub const DecodeResult = struct {
    values: [][]const u8,
    allocator: std.mem.Allocator,
    
    pub fn deinit(self: *DecodeResult) void {
        for (self.values) |slice| {
            if (slice.len > 0) {
                // Need to cast away const to free
                const mutable: []u8 = @constCast(slice);
                self.allocator.free(mutable);
            }
        }
        self.allocator.free(self.values);
    }
};

// =============================================================================
// Tests
// =============================================================================

test "delta byte array decode simple" {
    const allocator = std.testing.allocator;
    
    // Three strings: "ab", "abc", "abcd" (growing by one char each)
    // prefix_lengths: [0, 2, 3]  -> deltas [2, 1], min_delta=1
    // suffix_lengths: [2, 1, 1]  -> deltas [-1, 0], min_delta=-1
    // suffixes: "ab" + "c" + "d" = "abcd"
    //
    // For simplicity, use constant deltas:
    // prefix_lengths: [0, 1, 2] -> first=0, deltas=[1,1], min_delta=1, bit_widths=[0,...]
    // suffix_lengths: [1, 1, 1] -> first=1, deltas=[0,0], min_delta=0, bit_widths=[0,...]
    // Values: "a", "ab", "abc" with suffixes "a", "b", "c"
    
    const data = [_]u8{
        // Prefix lengths [0, 1, 2] - DELTA_BINARY_PACKED
        0x80, 0x01, // block_size = 128
        0x04, // num_mini_blocks = 4
        0x03, // total = 3
        0x00, // first = 0 (zigzag: 0 -> 0)
        0x02, // min_delta = 1 (zigzag: 1 -> 2)
        0x00, 0x00, 0x00, 0x00, // bit_widths all 0
        
        // Suffix lengths [1, 1, 1] - DELTA_BINARY_PACKED
        0x80, 0x01, // block_size = 128
        0x04, // num_mini_blocks = 4
        0x03, // total = 3
        0x02, // first = 1 (zigzag: 1 -> 2)
        0x00, // min_delta = 0 (zigzag: 0 -> 0)
        0x00, 0x00, 0x00, 0x00, // bit_widths all 0
        
        // Suffixes: "a" + "b" + "c"
        'a', 'b', 'c',
    };
    
    var result = try decode(allocator, &data);
    defer result.deinit();
    
    try std.testing.expectEqual(@as(usize, 3), result.values.len);
    try std.testing.expectEqualStrings("a", result.values[0]);
    try std.testing.expectEqualStrings("ab", result.values[1]);
    try std.testing.expectEqualStrings("abc", result.values[2]);
}

test "delta byte array decode empty" {
    const allocator = std.testing.allocator;
    
    const data = [_]u8{
        // Empty prefix lengths
        0x80, 0x01, // block_size = 128
        0x04, // num_mini_blocks = 4
        0x00, // total = 0
        0x00, // first = 0
    };
    
    var result = try decode(allocator, &data);
    defer result.deinit();
    
    try std.testing.expectEqual(@as(usize, 0), result.values.len);
}
