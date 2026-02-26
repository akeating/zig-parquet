//! DELTA_LENGTH_BYTE_ARRAY Encoder
//!
//! This encoding is used for variable-length byte arrays (strings, binary).
//! It encodes the lengths of each byte array using DELTA_BINARY_PACKED,
//! then concatenates all the raw bytes.
//!
//! Structure:
//! - Lengths encoded with DELTA_BINARY_PACKED (as i32)
//! - Raw bytes of all values concatenated
//!
//! Reference: https://parquet.apache.org/docs/file-format/data-pages/encodings/#delta-length-byte-array-delta_length_byte_array--6

const std = @import("std");
const safe = @import("../safe.zig");
const delta_binary_packed_encoder = @import("delta_binary_packed_encoder.zig");

pub const Error = error{
    OutOfMemory,
    ValueTooLarge,
};

/// Encode byte arrays using DELTA_LENGTH_BYTE_ARRAY encoding
pub fn encode(allocator: std.mem.Allocator, values: []const []const u8) Error![]u8 {
    if (values.len == 0) {
        // Just encode empty lengths
        return delta_binary_packed_encoder.encodeInt32(allocator, &.{}) catch return error.OutOfMemory;
    }

    // Extract lengths
    const lengths = allocator.alloc(i32, values.len) catch return error.OutOfMemory;
    defer allocator.free(lengths);

    var total_bytes: usize = 0;
    for (values, 0..) |v, i| {
        if (v.len > std.math.maxInt(i32)) return error.ValueTooLarge;
        lengths[i] = safe.castTo(i32, v.len) catch return error.OutOfMemory;
        total_bytes += v.len;
    }

    // Encode lengths with DELTA_BINARY_PACKED
    const encoded_lengths = delta_binary_packed_encoder.encodeInt32(allocator, lengths) catch return error.OutOfMemory;
    defer allocator.free(encoded_lengths);

    // Allocate result: encoded lengths + raw bytes
    const result = allocator.alloc(u8, encoded_lengths.len + total_bytes) catch return error.OutOfMemory;
    errdefer allocator.free(result);

    // Copy encoded lengths
    @memcpy(result[0..encoded_lengths.len], encoded_lengths);

    // Copy raw bytes
    var pos: usize = encoded_lengths.len;
    for (values) |v| {
        @memcpy(result[pos..][0..v.len], v);
        pos += v.len;
    }

    return result;
}

// =============================================================================
// Tests
// =============================================================================

test "delta length byte array encode empty" {
    const allocator = std.testing.allocator;

    const values = [_][]const u8{};
    const encoded = try encode(allocator, &values);
    defer allocator.free(encoded);

    // Should have header for empty lengths
    try std.testing.expect(encoded.len > 0);
}

test "delta length byte array encode single value" {
    const allocator = std.testing.allocator;

    const values = [_][]const u8{"hello"};
    const encoded = try encode(allocator, &values);
    defer allocator.free(encoded);

    // Decode and verify
    const decoder = @import("delta_length_byte_array.zig");
    var result = try decoder.decode(allocator, encoded);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.values.len);
    try std.testing.expectEqualStrings("hello", result.values[0]);
}

test "delta length byte array encode multiple values" {
    const allocator = std.testing.allocator;

    const values = [_][]const u8{ "hello", "world", "test" };
    const encoded = try encode(allocator, &values);
    defer allocator.free(encoded);

    // Decode and verify
    const decoder = @import("delta_length_byte_array.zig");
    var result = try decoder.decode(allocator, encoded);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 3), result.values.len);
    try std.testing.expectEqualStrings("hello", result.values[0]);
    try std.testing.expectEqualStrings("world", result.values[1]);
    try std.testing.expectEqualStrings("test", result.values[2]);
}

test "delta length byte array encode with empty strings" {
    const allocator = std.testing.allocator;

    const values = [_][]const u8{ "", "a", "", "bc", "" };
    const encoded = try encode(allocator, &values);
    defer allocator.free(encoded);

    // Decode and verify
    const decoder = @import("delta_length_byte_array.zig");
    var result = try decoder.decode(allocator, encoded);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 5), result.values.len);
    try std.testing.expectEqualStrings("", result.values[0]);
    try std.testing.expectEqualStrings("a", result.values[1]);
    try std.testing.expectEqualStrings("", result.values[2]);
    try std.testing.expectEqualStrings("bc", result.values[3]);
    try std.testing.expectEqualStrings("", result.values[4]);
}

test "delta length byte array encode uniform lengths" {
    const allocator = std.testing.allocator;

    const values = [_][]const u8{ "aaaa", "bbbb", "cccc", "dddd" };
    const encoded = try encode(allocator, &values);
    defer allocator.free(encoded);

    // Decode and verify
    const decoder = @import("delta_length_byte_array.zig");
    var result = try decoder.decode(allocator, encoded);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 4), result.values.len);
    for (values, result.values) |expected, actual| {
        try std.testing.expectEqualStrings(expected, actual);
    }
}
