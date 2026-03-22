//! DELTA_BYTE_ARRAY Encoder (Incremental Encoding)
//!
//! This encoding is used for byte arrays with common prefixes (sorted strings).
//! Each value is compared to the previous value to find the common prefix,
//! then only the suffix is stored.
//!
//! Structure:
//! - Prefix lengths encoded with DELTA_BINARY_PACKED (as i32)
//! - Suffix lengths encoded with DELTA_BINARY_PACKED (as i32)
//! - Suffix bytes concatenated
//!
//! For the first value, prefix_length is 0 and suffix is the entire value.
//!
//! Reference: https://parquet.apache.org/docs/file-format/data-pages/encodings/#delta-strings-delta_byte_array--7

const std = @import("std");
const safe = @import("../safe.zig");
const delta_binary_packed_encoder = @import("delta_binary_packed_encoder.zig");

pub const Error = error{
    OutOfMemory,
    ValueTooLarge,
};

/// Encode byte arrays using DELTA_BYTE_ARRAY (incremental) encoding
pub fn encode(allocator: std.mem.Allocator, values: []const []const u8) Error![]u8 {
    if (values.len == 0) {
        // Encode empty prefix and suffix lengths
        const empty_prefixes = delta_binary_packed_encoder.encodeInt32(allocator, &.{}) catch return error.OutOfMemory;
        defer allocator.free(empty_prefixes);
        const empty_suffixes = delta_binary_packed_encoder.encodeInt32(allocator, &.{}) catch return error.OutOfMemory;
        defer allocator.free(empty_suffixes);

        const result = allocator.alloc(u8, empty_prefixes.len + empty_suffixes.len) catch return error.OutOfMemory;
        @memcpy(result[0..empty_prefixes.len], empty_prefixes);
        @memcpy(result[empty_prefixes.len..], empty_suffixes);
        return result;
    }

    // Compute prefix lengths and suffixes
    const prefix_lengths = allocator.alloc(i32, values.len) catch return error.OutOfMemory;
    defer allocator.free(prefix_lengths);

    const suffix_lengths = allocator.alloc(i32, values.len) catch return error.OutOfMemory;
    defer allocator.free(suffix_lengths);

    // Collect suffixes
    var suffixes: std.ArrayList(u8) = .empty;
    defer suffixes.deinit(allocator);

    var prev_value: []const u8 = "";
    for (values, 0..) |value, i| {
        if (value.len > std.math.maxInt(i32)) return error.ValueTooLarge;
        const prefix_len = commonPrefixLength(prev_value, value);
    prefix_lengths[i] = safe.castTo(i32, prefix_len) catch unreachable; // prefix_len <= value.len <= maxInt(i32), checked above
    suffix_lengths[i] = safe.castTo(i32, value.len - prefix_len) catch unreachable; // value.len <= maxInt(i32) checked above, prefix_len <= value.len

        // Append suffix
        suffixes.appendSlice(allocator, value[prefix_len..]) catch return error.OutOfMemory;

        prev_value = value;
    }

    // Encode prefix lengths
    const encoded_prefixes = delta_binary_packed_encoder.encodeInt32(allocator, prefix_lengths) catch return error.OutOfMemory;
    defer allocator.free(encoded_prefixes);

    // Encode suffix lengths
    const encoded_suffixes = delta_binary_packed_encoder.encodeInt32(allocator, suffix_lengths) catch return error.OutOfMemory;
    defer allocator.free(encoded_suffixes);

    // Combine: prefix_lengths | suffix_lengths | suffix_bytes
    const total_len = encoded_prefixes.len + encoded_suffixes.len + suffixes.items.len;
    const result = allocator.alloc(u8, total_len) catch return error.OutOfMemory;
    errdefer allocator.free(result);

    var pos: usize = 0;
    @memcpy(result[pos..][0..encoded_prefixes.len], encoded_prefixes);
    pos += encoded_prefixes.len;

    @memcpy(result[pos..][0..encoded_suffixes.len], encoded_suffixes);
    pos += encoded_suffixes.len;

    @memcpy(result[pos..][0..suffixes.items.len], suffixes.items);

    return result;
}

/// Find the length of the common prefix between two byte slices
fn commonPrefixLength(a: []const u8, b: []const u8) usize {
    const min_len = @min(a.len, b.len);
    var i: usize = 0;
    while (i < min_len and a[i] == b[i]) : (i += 1) {}
    return i;
}

// =============================================================================
// Tests
// =============================================================================

test "delta byte array encode empty" {
    const allocator = std.testing.allocator;

    const values = [_][]const u8{};
    const encoded = try encode(allocator, &values);
    defer allocator.free(encoded);

    // Should have headers for empty prefix/suffix lengths
    try std.testing.expect(encoded.len > 0);
}

test "delta byte array encode single value" {
    const allocator = std.testing.allocator;

    const values = [_][]const u8{"hello"};
    const encoded = try encode(allocator, &values);
    defer allocator.free(encoded);

    // Decode and verify
    const decoder = @import("delta_byte_array.zig");
    var result = try decoder.decode(allocator, encoded);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.values.len);
    try std.testing.expectEqualStrings("hello", result.values[0]);
}

test "delta byte array encode with common prefixes" {
    const allocator = std.testing.allocator;

    const values = [_][]const u8{ "apple", "application", "apply" };
    const encoded = try encode(allocator, &values);
    defer allocator.free(encoded);

    // Decode and verify
    const decoder = @import("delta_byte_array.zig");
    var result = try decoder.decode(allocator, encoded);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 3), result.values.len);
    try std.testing.expectEqualStrings("apple", result.values[0]);
    try std.testing.expectEqualStrings("application", result.values[1]);
    try std.testing.expectEqualStrings("apply", result.values[2]);
}

test "delta byte array encode no common prefix" {
    const allocator = std.testing.allocator;

    const values = [_][]const u8{ "abc", "xyz", "123" };
    const encoded = try encode(allocator, &values);
    defer allocator.free(encoded);

    // Decode and verify
    const decoder = @import("delta_byte_array.zig");
    var result = try decoder.decode(allocator, encoded);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 3), result.values.len);
    try std.testing.expectEqualStrings("abc", result.values[0]);
    try std.testing.expectEqualStrings("xyz", result.values[1]);
    try std.testing.expectEqualStrings("123", result.values[2]);
}

test "delta byte array encode sorted strings" {
    const allocator = std.testing.allocator;

    const values = [_][]const u8{
        "https://example.com/api/v1/users/0",
        "https://example.com/api/v1/users/1",
        "https://example.com/api/v1/users/2",
    };
    const encoded = try encode(allocator, &values);
    defer allocator.free(encoded);

    // Decode and verify
    const decoder = @import("delta_byte_array.zig");
    var result = try decoder.decode(allocator, encoded);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 3), result.values.len);
    for (values, result.values) |expected, actual| {
        try std.testing.expectEqualStrings(expected, actual);
    }
}

test "common prefix length" {
    try std.testing.expectEqual(@as(usize, 0), commonPrefixLength("", ""));
    try std.testing.expectEqual(@as(usize, 0), commonPrefixLength("abc", ""));
    try std.testing.expectEqual(@as(usize, 0), commonPrefixLength("", "abc"));
    try std.testing.expectEqual(@as(usize, 0), commonPrefixLength("abc", "xyz"));
    try std.testing.expectEqual(@as(usize, 3), commonPrefixLength("abc", "abcd"));
    try std.testing.expectEqual(@as(usize, 4), commonPrefixLength("abcd", "abcd"));
    try std.testing.expectEqual(@as(usize, 4), commonPrefixLength("apple", "application")); // "appl" is common
    try std.testing.expectEqual(@as(usize, 5), commonPrefixLength("apple", "apple"));
}
