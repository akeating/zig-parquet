//! PLAIN encoding encoder
//!
//! PLAIN is the simplest encoding in Parquet:
//! - Boolean: 1 bit per value, packed into bytes (LSB first)
//! - Int32/Int64: 4/8 bytes little-endian
//! - Float/Double: 4/8 bytes IEEE 754 little-endian
//! - ByteArray: 4-byte length prefix + bytes
//! - FixedLenByteArray: just the bytes (length known from schema)

const std = @import("std");
const safe = @import("../safe.zig");

/// Encode booleans as bit-packed bytes (LSB first)
pub fn encodeBooleans(allocator: std.mem.Allocator, values: []const bool) ![]u8 {
    const num_bytes = (values.len + 7) / 8;
    const result = try allocator.alloc(u8, num_bytes);
    @memset(result, 0);

    for (values, 0..) |value, i| {
        if (value) {
            const byte_idx = i / 8;
            const bit_idx: u3 = safe.castTo(u3, i % 8) catch unreachable; // i % 8 is strictly 0-7
            result[byte_idx] |= @as(u8, 1) << bit_idx;
        }
    }

    return result;
}

/// Encode i32 values as little-endian 4-byte integers
pub fn encodeInt32(allocator: std.mem.Allocator, values: []const i32) ![]u8 {
    const result = try allocator.alloc(u8, values.len * 4);

    for (values, 0..) |value, i| {
        std.mem.writeInt(i32, result[i * 4 ..][0..4], value, .little);
    }

    return result;
}

/// Encode i64 values as little-endian 8-byte integers
pub fn encodeInt64(allocator: std.mem.Allocator, values: []const i64) ![]u8 {
    const result = try allocator.alloc(u8, values.len * 8);

    for (values, 0..) |value, i| {
        std.mem.writeInt(i64, result[i * 8 ..][0..8], value, .little);
    }

    return result;
}

/// Encode f32 values as little-endian 4-byte IEEE 754
pub fn encodeFloat(allocator: std.mem.Allocator, values: []const f32) ![]u8 {
    const result = try allocator.alloc(u8, values.len * 4);

    for (values, 0..) |value, i| {
        const bits: u32 = @bitCast(value);
        std.mem.writeInt(u32, result[i * 4 ..][0..4], bits, .little);
    }

    return result;
}

/// Encode f64 values as little-endian 8-byte IEEE 754
pub fn encodeDouble(allocator: std.mem.Allocator, values: []const f64) ![]u8 {
    const result = try allocator.alloc(u8, values.len * 8);

    for (values, 0..) |value, i| {
        const bits: u64 = @bitCast(value);
        std.mem.writeInt(u64, result[i * 8 ..][0..8], bits, .little);
    }

    return result;
}

/// Encode byte arrays with 4-byte length prefix each
pub fn encodeByteArrays(allocator: std.mem.Allocator, values: []const []const u8) ![]u8 {
    // Calculate total size
    var total_size: usize = 0;
    for (values) |value| {
        if (value.len > std.math.maxInt(i32)) return error.ValueTooLarge;
        const entry_size = std.math.add(usize, 4, value.len) catch return error.OutOfMemory;
        total_size = std.math.add(usize, total_size, entry_size) catch return error.OutOfMemory;
    }

    const result = try allocator.alloc(u8, total_size);
    var pos: usize = 0;

    for (values) |value| {
        // Write length as u32 little-endian
        std.mem.writeInt(u32, result[pos..][0..4], try safe.castTo(u32, value.len), .little);
        pos += 4;

        // Write data
        @memcpy(result[pos..][0..value.len], value);
        pos += value.len;
    }

    return result;
}

/// Encode fixed-length byte arrays (no length prefix)
pub fn encodeFixedByteArrays(allocator: std.mem.Allocator, values: []const []const u8, fixed_len: usize) ![]u8 {
    if (fixed_len > std.math.maxInt(i32)) return error.ValueTooLarge;
    const result = try allocator.alloc(u8, values.len * fixed_len);

    for (values, 0..) |value, i| {
        if (value.len != fixed_len) {
            return error.InvalidFixedLength;
        }
        @memcpy(result[i * fixed_len ..][0..fixed_len], value);
    }

    return result;
}
// Tests
test "encode booleans" {
    const allocator = std.testing.allocator;

    // Bits: 1,0,1,0,1,1,0,1 = 0xB5
    const values = [_]bool{ true, false, true, false, true, true, false, true };
    const encoded = try encodeBooleans(allocator, &values);
    defer allocator.free(encoded);

    try std.testing.expectEqualSlices(u8, &[_]u8{0xB5}, encoded);
}

test "encode booleans partial byte" {
    const allocator = std.testing.allocator;

    // 5 bits: 1,0,1,0,1 = 0b00010101 = 0x15
    const values = [_]bool{ true, false, true, false, true };
    const encoded = try encodeBooleans(allocator, &values);
    defer allocator.free(encoded);

    try std.testing.expectEqualSlices(u8, &[_]u8{0x15}, encoded);
}

test "encode i32" {
    const allocator = std.testing.allocator;

    const values = [_]i32{ 0x01020304, -1 };
    const encoded = try encodeInt32(allocator, &values);
    defer allocator.free(encoded);

    try std.testing.expectEqualSlices(u8, &[_]u8{
        0x04, 0x03, 0x02, 0x01, // 0x01020304
        0xFF, 0xFF, 0xFF, 0xFF, // -1
    }, encoded);
}

test "encode i64" {
    const allocator = std.testing.allocator;

    const values = [_]i64{0x0102030405060708};
    const encoded = try encodeInt64(allocator, &values);
    defer allocator.free(encoded);

    try std.testing.expectEqualSlices(u8, &[_]u8{
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
    }, encoded);
}

test "encode f64" {
    const allocator = std.testing.allocator;

    const values = [_]f64{1.5};
    const encoded = try encodeDouble(allocator, &values);
    defer allocator.free(encoded);

    // 1.5 in IEEE 754 double
    try std.testing.expectEqualSlices(u8, &[_]u8{
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF8, 0x3F,
    }, encoded);
}

test "encode byte arrays" {
    const allocator = std.testing.allocator;

    const values = [_][]const u8{ "hi", "bye" };
    const encoded = try encodeByteArrays(allocator, &values);
    defer allocator.free(encoded);

    try std.testing.expectEqualSlices(u8, &[_]u8{
        0x02, 0x00, 0x00, 0x00, 'h', 'i', // "hi"
        0x03, 0x00, 0x00, 0x00, 'b', 'y', 'e', // "bye"
    }, encoded);
}

test "encodeByteArrays round-trip" {
    const allocator = std.testing.allocator;
    const plain = @import("plain.zig");

    const values = [_][]const u8{ "hello", "world" };
    const encoded = try encodeByteArrays(allocator, &values);
    defer allocator.free(encoded);

    // Expected: 4 bytes length + 5 bytes "hello" + 4 bytes length + 5 bytes "world" = 18 bytes
    try std.testing.expectEqual(@as(usize, 18), encoded.len);

    // Decode first value
    var decoder = plain.PlainDecoder([]const u8).init(encoded, 2);
    const first = decoder.next();
    try std.testing.expect(first != null);
    try std.testing.expectEqualStrings("hello", first.?);

    const second = decoder.next();
    try std.testing.expect(second != null);
    try std.testing.expectEqualStrings("world", second.?);
}
