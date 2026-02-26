//! BYTE_STREAM_SPLIT Encoder
//!
//! This encoding splits each value's bytes and groups them by byte position.
//! For example, for 32-bit floats with N values:
//! - All first bytes together (N bytes)
//! - All second bytes together (N bytes)
//! - All third bytes together (N bytes)
//! - All fourth bytes together (N bytes)
//!
//! This is effective for floating-point data because similar values often share
//! byte patterns, making the grouped bytes more compressible.
//!
//! Reference: https://parquet.apache.org/docs/file-format/data-pages/encodings/#byte-stream-split

const std = @import("std");

/// Encode 32-bit floats using BYTE_STREAM_SPLIT encoding
pub fn encodeFloat32(allocator: std.mem.Allocator, values: []const f32) ![]u8 {
    return encodeGeneric(f32, allocator, values);
}

/// Encode 64-bit doubles using BYTE_STREAM_SPLIT encoding
pub fn encodeFloat64(allocator: std.mem.Allocator, values: []const f64) ![]u8 {
    return encodeGeneric(f64, allocator, values);
}

/// Encode 32-bit integers using BYTE_STREAM_SPLIT encoding
pub fn encodeInt32(allocator: std.mem.Allocator, values: []const i32) ![]u8 {
    return encodeGeneric(i32, allocator, values);
}

/// Encode 64-bit integers using BYTE_STREAM_SPLIT encoding
pub fn encodeInt64(allocator: std.mem.Allocator, values: []const i64) ![]u8 {
    return encodeGeneric(i64, allocator, values);
}

/// Encode 16-bit floats using BYTE_STREAM_SPLIT encoding
pub fn encodeFloat16(allocator: std.mem.Allocator, values: []const f16) ![]u8 {
    return encodeGeneric(f16, allocator, values);
}

/// Encode fixed-length byte arrays using BYTE_STREAM_SPLIT encoding
/// Each value must have exactly type_length bytes.
pub fn encodeFixedByteArray(allocator: std.mem.Allocator, values: []const []const u8, type_length: usize) ![]u8 {
    const num_values = values.len;

    if (num_values == 0) {
        return try allocator.alloc(u8, 0);
    }

    const result = try allocator.alloc(u8, num_values * type_length);
    errdefer allocator.free(result);

    // For each value, split its bytes into separate streams
    // Stream 0 gets byte 0 of each value, Stream 1 gets byte 1, etc.
    for (values, 0..) |value, value_idx| {
        if (value.len != type_length) {
            return error.InvalidArgument;
        }
        for (0..type_length) |byte_idx| {
            // Stream byte_idx starts at offset byte_idx * num_values
            result[byte_idx * num_values + value_idx] = value[byte_idx];
        }
    }

    return result;
}

/// Generic encoder for any fixed-size type
fn encodeGeneric(comptime T: type, allocator: std.mem.Allocator, values: []const T) ![]u8 {
    const byte_width = @sizeOf(T);
    const num_values = values.len;

    if (num_values == 0) {
        return try allocator.alloc(u8, 0);
    }

    const result = try allocator.alloc(u8, num_values * byte_width);
    errdefer allocator.free(result);

    // For each value, split its bytes into separate streams
    // Stream 0 gets byte 0 of each value, Stream 1 gets byte 1, etc.
    for (values, 0..) |value, value_idx| {
        const value_bytes: [byte_width]u8 = @bitCast(value);
        for (0..byte_width) |byte_idx| {
            // Stream byte_idx starts at offset byte_idx * num_values
            result[byte_idx * num_values + value_idx] = value_bytes[byte_idx];
        }
    }

    return result;
}

// =============================================================================
// Tests
// =============================================================================

test "byte stream split encode f32" {
    const allocator = std.testing.allocator;

    // Encode two floats: 1.0 and 2.0
    const values = [_]f32{ 1.0, 2.0 };
    const encoded = try encodeFloat32(allocator, &values);
    defer allocator.free(encoded);

    // 1.0f = 0x3F800000, 2.0f = 0x40000000
    // Stream 0 (byte 0 of each): 0x00, 0x00
    // Stream 1 (byte 1 of each): 0x00, 0x00
    // Stream 2 (byte 2 of each): 0x80, 0x00
    // Stream 3 (byte 3 of each): 0x3F, 0x40
    try std.testing.expectEqual(@as(usize, 8), encoded.len);
    try std.testing.expectEqualSlices(u8, &[_]u8{
        0x00, 0x00, // Stream 0
        0x00, 0x00, // Stream 1
        0x80, 0x00, // Stream 2
        0x3F, 0x40, // Stream 3
    }, encoded);
}

test "byte stream split encode f64" {
    const allocator = std.testing.allocator;

    const values = [_]f64{1.5};
    const encoded = try encodeFloat64(allocator, &values);
    defer allocator.free(encoded);

    // 1.5 as f64 = 0x3FF8000000000000
    // Each stream has 1 byte (1 value)
    try std.testing.expectEqual(@as(usize, 8), encoded.len);
    // Bytes of 1.5 in little-endian: 00 00 00 00 00 00 F8 3F
    try std.testing.expectEqualSlices(u8, &[_]u8{
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF8, 0x3F,
    }, encoded);
}

test "byte stream split encode empty" {
    const allocator = std.testing.allocator;

    const values = [_]f32{};
    const encoded = try encodeFloat32(allocator, &values);
    defer allocator.free(encoded);

    try std.testing.expectEqual(@as(usize, 0), encoded.len);
}

test "byte stream split round-trip f32" {
    const allocator = std.testing.allocator;
    const decoder = @import("byte_stream_split.zig");

    const original = [_]f32{ 1.0, 2.5, -3.14159, 0.0, std.math.inf(f32) };
    const encoded = try encodeFloat32(allocator, &original);
    defer allocator.free(encoded);

    // Decode
    var decoded: [5]f32 = undefined;
    try decoder.decodeFloat32Into(encoded, &decoded);

    for (original, decoded) |orig, dec| {
        if (std.math.isNan(orig)) {
            try std.testing.expect(std.math.isNan(dec));
        } else {
            try std.testing.expectEqual(orig, dec);
        }
    }
}

test "byte stream split round-trip f64" {
    const allocator = std.testing.allocator;
    const decoder = @import("byte_stream_split.zig");

    const original = [_]f64{ 1.0, 2.5, -3.14159265358979, 0.0, std.math.inf(f64) };
    const encoded = try encodeFloat64(allocator, &original);
    defer allocator.free(encoded);

    // Decode
    var decoded: [5]f64 = undefined;
    try decoder.decodeFloat64Into(encoded, &decoded);

    for (original, decoded) |orig, dec| {
        if (std.math.isNan(orig)) {
            try std.testing.expect(std.math.isNan(dec));
        } else {
            try std.testing.expectEqual(orig, dec);
        }
    }
}

test "byte stream split encode i32" {
    const allocator = std.testing.allocator;

    // Encode two i32s: 1 and 256
    const values = [_]i32{ 1, 256 };
    const encoded = try encodeInt32(allocator, &values);
    defer allocator.free(encoded);

    // 1 = 0x00000001, 256 = 0x00000100 (little-endian)
    // Stream 0 (byte 0 of each): 0x01, 0x00
    // Stream 1 (byte 1 of each): 0x00, 0x01
    // Stream 2 (byte 2 of each): 0x00, 0x00
    // Stream 3 (byte 3 of each): 0x00, 0x00
    try std.testing.expectEqual(@as(usize, 8), encoded.len);
    try std.testing.expectEqualSlices(u8, &[_]u8{
        0x01, 0x00, // Stream 0
        0x00, 0x01, // Stream 1
        0x00, 0x00, // Stream 2
        0x00, 0x00, // Stream 3
    }, encoded);
}

test "byte stream split encode i64" {
    const allocator = std.testing.allocator;

    const values = [_]i64{0x0102030405060708};
    const encoded = try encodeInt64(allocator, &values);
    defer allocator.free(encoded);

    // Each stream has 1 byte (1 value)
    // Little-endian bytes: 08 07 06 05 04 03 02 01
    try std.testing.expectEqual(@as(usize, 8), encoded.len);
    try std.testing.expectEqualSlices(u8, &[_]u8{
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
    }, encoded);
}

test "byte stream split round-trip i32" {
    const allocator = std.testing.allocator;
    const decoder = @import("byte_stream_split.zig");

    const original = [_]i32{ 0, 1, -1, 256, -256, std.math.maxInt(i32), std.math.minInt(i32) };
    const encoded = try encodeInt32(allocator, &original);
    defer allocator.free(encoded);

    // Decode
    var decoded: [7]i32 = undefined;
    try decoder.decodeInt32Into(encoded, &decoded);

    try std.testing.expectEqualSlices(i32, &original, &decoded);
}

test "byte stream split round-trip i64" {
    const allocator = std.testing.allocator;
    const decoder = @import("byte_stream_split.zig");

    const original = [_]i64{ 0, 1, -1, 0x0102030405060708, std.math.maxInt(i64), std.math.minInt(i64) };
    const encoded = try encodeInt64(allocator, &original);
    defer allocator.free(encoded);

    // Decode
    var decoded: [6]i64 = undefined;
    try decoder.decodeInt64Into(encoded, &decoded);

    try std.testing.expectEqualSlices(i64, &original, &decoded);
}

test "byte stream split encode f16" {
    const allocator = std.testing.allocator;

    // Encode two f16s: 1.0 and 2.0
    const values = [_]f16{ 1.0, 2.0 };
    const encoded = try encodeFloat16(allocator, &values);
    defer allocator.free(encoded);

    // f16 1.0 = 0x3C00, f16 2.0 = 0x4000 (IEEE 754 half-precision)
    // Little-endian: 1.0 = 00 3C, 2.0 = 00 40
    // Stream 0 (byte 0 of each): 0x00, 0x00
    // Stream 1 (byte 1 of each): 0x3C, 0x40
    try std.testing.expectEqual(@as(usize, 4), encoded.len);
    try std.testing.expectEqualSlices(u8, &[_]u8{
        0x00, 0x00, // Stream 0
        0x3C, 0x40, // Stream 1
    }, encoded);
}

test "byte stream split round-trip f16" {
    const allocator = std.testing.allocator;
    const decoder = @import("byte_stream_split.zig");

    const original = [_]f16{ 0.0, 1.0, 2.0, -1.5, 0.5 };
    const encoded = try encodeFloat16(allocator, &original);
    defer allocator.free(encoded);

    // Decode
    var decoded: [5]f16 = undefined;
    try decoder.decodeFloat16Into(encoded, &decoded);

    for (original, decoded) |orig, dec| {
        if (std.math.isNan(orig)) {
            try std.testing.expect(std.math.isNan(dec));
        } else {
            try std.testing.expectEqual(orig, dec);
        }
    }
}

test "byte stream split encode fixed byte array" {
    const allocator = std.testing.allocator;

    // Encode two 3-byte arrays
    const values = [_][]const u8{
        &[_]u8{ 0x01, 0x02, 0x03 },
        &[_]u8{ 0x04, 0x05, 0x06 },
    };
    const encoded = try encodeFixedByteArray(allocator, &values, 3);
    defer allocator.free(encoded);

    // Stream 0 (byte 0 of each): 0x01, 0x04
    // Stream 1 (byte 1 of each): 0x02, 0x05
    // Stream 2 (byte 2 of each): 0x03, 0x06
    try std.testing.expectEqual(@as(usize, 6), encoded.len);
    try std.testing.expectEqualSlices(u8, &[_]u8{
        0x01, 0x04, // Stream 0
        0x02, 0x05, // Stream 1
        0x03, 0x06, // Stream 2
    }, encoded);
}

test "byte stream split round-trip fixed byte array" {
    const allocator = std.testing.allocator;
    const decoder = @import("byte_stream_split.zig");

    const original = [_][]const u8{
        &[_]u8{ 0xAA, 0xBB, 0xCC, 0xDD },
        &[_]u8{ 0x11, 0x22, 0x33, 0x44 },
        &[_]u8{ 0xFF, 0x00, 0xFF, 0x00 },
    };
    const encoded = try encodeFixedByteArray(allocator, &original, 4);
    defer allocator.free(encoded);

    // Decode
    const decoded = try decoder.decodeFixedLenAlloc(allocator, encoded, 3, 4);
    defer {
        for (decoded) |d| allocator.free(d);
        allocator.free(decoded);
    }

    for (original, decoded) |orig, dec| {
        try std.testing.expectEqualSlices(u8, orig, dec);
    }
}

test "byte stream split encode fixed byte array invalid length" {
    const allocator = std.testing.allocator;

    // One value has wrong length
    const values = [_][]const u8{
        &[_]u8{ 0x01, 0x02, 0x03 },
        &[_]u8{ 0x04, 0x05 }, // Only 2 bytes, should be 3
    };
    const result = encodeFixedByteArray(allocator, &values, 3);
    try std.testing.expectError(error.InvalidArgument, result);
}
