//! BYTE_STREAM_SPLIT Encoding
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


/// Decode BYTE_STREAM_SPLIT encoded floats into pre-allocated buffer
pub fn decodeFloat32Into(data: []const u8, result: []f32) !void {
    return decodeGenericInto(f32, data, result);
}

/// Decode BYTE_STREAM_SPLIT encoded doubles into pre-allocated buffer
pub fn decodeFloat64Into(data: []const u8, result: []f64) !void {
    return decodeGenericInto(f64, data, result);
}

/// Decode BYTE_STREAM_SPLIT encoded 32-bit integers into pre-allocated buffer
pub fn decodeInt32Into(data: []const u8, result: []i32) !void {
    return decodeGenericInto(i32, data, result);
}

/// Decode BYTE_STREAM_SPLIT encoded 64-bit integers into pre-allocated buffer
pub fn decodeInt64Into(data: []const u8, result: []i64) !void {
    return decodeGenericInto(i64, data, result);
}

/// Decode BYTE_STREAM_SPLIT encoded 16-bit floats into pre-allocated buffer
pub fn decodeFloat16Into(data: []const u8, result: []f16) !void {
    return decodeGenericInto(f16, data, result);
}

/// Generic decoder that writes into pre-allocated buffer
fn decodeGenericInto(comptime T: type, data: []const u8, result: []T) !void {
    const byte_width = @sizeOf(T);
    const num_values = result.len;
    
    if (data.len < num_values * byte_width) {
        return error.InsufficientData;
    }
    
    // Each stream contains one byte from each value
    // Stream 0: all byte 0s, Stream 1: all byte 1s, etc.
    const streams: [byte_width][]const u8 = blk: {
        var s: [byte_width][]const u8 = undefined;
        for (0..byte_width) |i| {
            const start = i * num_values;
            const end = (i + 1) * num_values;
            s[i] = data[start..end];
        }
        break :blk s;
    };
    
    // Reconstruct each value from its bytes
    for (0..num_values) |i| {
        var bytes: [@sizeOf(T)]u8 = undefined;
        inline for (0..byte_width) |j| {
            bytes[j] = streams[j][i];
        }
        result[i] = @bitCast(bytes);
    }
}

/// Decode BYTE_STREAM_SPLIT for fixed-length byte arrays
fn decodeFixedLenInto(data: []const u8, result: [][]u8, type_length: usize) !void {
    const num_values = result.len;
    
    if (data.len < num_values * type_length) {
        return error.InsufficientData;
    }
    
    // For fixed-length byte arrays, streams are organized similarly
    for (0..num_values) |i| {
        for (0..type_length) |j| {
            const stream_offset = j * num_values;
            result[i][j] = data[stream_offset + i];
        }
    }
}

// =============================================================================
// Allocating versions
// =============================================================================

/// Decode BYTE_STREAM_SPLIT encoded f32 values with allocation
pub fn decodeFloat32Alloc(allocator: std.mem.Allocator, data: []const u8, num_values: usize) ![]f32 {
    const result = try allocator.alloc(f32, num_values);
    errdefer allocator.free(result);
    try decodeFloat32Into(data, result);
    return result;
}

/// Decode BYTE_STREAM_SPLIT encoded f64 values with allocation
pub fn decodeFloat64Alloc(allocator: std.mem.Allocator, data: []const u8, num_values: usize) ![]f64 {
    const result = try allocator.alloc(f64, num_values);
    errdefer allocator.free(result);
    try decodeFloat64Into(data, result);
    return result;
}

/// Decode BYTE_STREAM_SPLIT encoded i32 values with allocation
pub fn decodeInt32Alloc(allocator: std.mem.Allocator, data: []const u8, num_values: usize) ![]i32 {
    const result = try allocator.alloc(i32, num_values);
    errdefer allocator.free(result);
    try decodeInt32Into(data, result);
    return result;
}

/// Decode BYTE_STREAM_SPLIT encoded i64 values with allocation
pub fn decodeInt64Alloc(allocator: std.mem.Allocator, data: []const u8, num_values: usize) ![]i64 {
    const result = try allocator.alloc(i64, num_values);
    errdefer allocator.free(result);
    try decodeInt64Into(data, result);
    return result;
}

/// Decode BYTE_STREAM_SPLIT encoded fixed-length byte arrays with allocation
pub fn decodeFixedLenAlloc(allocator: std.mem.Allocator, data: []const u8, num_values: usize, type_length: usize) ![][]u8 {
    if (data.len < num_values * type_length) {
        return error.InsufficientData;
    }

    // Allocate the outer slice
    const result = try allocator.alloc([]u8, num_values);
    errdefer allocator.free(result);

    // Allocate each inner slice
    var allocated: usize = 0;
    errdefer {
        for (result[0..allocated]) |slice| {
            allocator.free(slice);
        }
    }

    for (0..num_values) |i| {
        result[i] = try allocator.alloc(u8, type_length);
        allocated += 1;
    }

    // Decode into the allocated buffers
    try decodeFixedLenInto(data, result, type_length);
    return result;
}

// =============================================================================
// Tests
// =============================================================================

test "byte stream split decode f32" {
    // 3 values: 1.0, 2.0, 3.0
    // f32 1.0 = 0x3F800000 -> bytes: 00 00 80 3F (little endian)
    // f32 2.0 = 0x40000000 -> bytes: 00 00 00 40
    // f32 3.0 = 0x40400000 -> bytes: 00 00 40 40
    //
    // Byte stream split layout:
    // Stream 0 (byte 0 of each): 00 00 00
    // Stream 1 (byte 1 of each): 00 00 00
    // Stream 2 (byte 2 of each): 80 00 40
    // Stream 3 (byte 3 of each): 3F 40 40
    const data = [_]u8{
        0x00, 0x00, 0x00, // stream 0
        0x00, 0x00, 0x00, // stream 1
        0x80, 0x00, 0x40, // stream 2
        0x3F, 0x40, 0x40, // stream 3
    };
    
    var result: [3]f32 = undefined;
    try decodeFloat32Into(&data, &result);
    
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), result[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), result[1], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), result[2], 0.0001);
}

test "byte stream split decode f64" {
    const allocator = std.testing.allocator;
    
    // 2 values: 1.0, 2.0
    // f64 1.0 = 0x3FF0000000000000 -> bytes: 00 00 00 00 00 00 F0 3F
    // f64 2.0 = 0x4000000000000000 -> bytes: 00 00 00 00 00 00 00 40
    const data = [_]u8{
        0x00, 0x00, // stream 0
        0x00, 0x00, // stream 1
        0x00, 0x00, // stream 2
        0x00, 0x00, // stream 3
        0x00, 0x00, // stream 4
        0x00, 0x00, // stream 5
        0xF0, 0x00, // stream 6
        0x3F, 0x40, // stream 7
    };
    
    const result = try decodeFloat64Alloc(allocator, &data, 2);
    defer allocator.free(result);
    
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), result[0], 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), result[1], 0.0001);
}

test "byte stream split insufficient data" {
    var result: [3]f32 = undefined;
    // Only 8 bytes, but need 12 for 3 f32s
    const data = [_]u8{ 0, 0, 0, 0, 0, 0, 0, 0 };
    try std.testing.expectError(error.InsufficientData, decodeFloat32Into(&data, &result));
}

test "byte stream split decode i32" {
    // 2 values: 1 and 256
    // i32 1 = 0x00000001 -> bytes: 01 00 00 00 (little endian)
    // i32 256 = 0x00000100 -> bytes: 00 01 00 00
    //
    // Byte stream split layout:
    // Stream 0 (byte 0 of each): 01 00
    // Stream 1 (byte 1 of each): 00 01
    // Stream 2 (byte 2 of each): 00 00
    // Stream 3 (byte 3 of each): 00 00
    const data = [_]u8{
        0x01, 0x00, // stream 0
        0x00, 0x01, // stream 1
        0x00, 0x00, // stream 2
        0x00, 0x00, // stream 3
    };

    var result: [2]i32 = undefined;
    try decodeInt32Into(&data, &result);

    try std.testing.expectEqual(@as(i32, 1), result[0]);
    try std.testing.expectEqual(@as(i32, 256), result[1]);
}

test "byte stream split decode i64" {
    const allocator = std.testing.allocator;

    // 1 value: 0x0102030405060708
    // Little-endian bytes: 08 07 06 05 04 03 02 01
    const data = [_]u8{
        0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
    };

    const result = try decodeInt64Alloc(allocator, &data, 1);
    defer allocator.free(result);

    try std.testing.expectEqual(@as(i64, 0x0102030405060708), result[0]);
}

test "byte stream split decode i32 with negative" {
    const allocator = std.testing.allocator;

    // Test with negative number: -1 = 0xFFFFFFFF
    // Byte stream split for single value: FF FF FF FF
    const data = [_]u8{ 0xFF, 0xFF, 0xFF, 0xFF };

    const result = try decodeInt32Alloc(allocator, &data, 1);
    defer allocator.free(result);

    try std.testing.expectEqual(@as(i32, -1), result[0]);
}

test "byte stream split decode fixed len" {
    const allocator = std.testing.allocator;

    // 2 values of 3 bytes each
    // Value 0: 0x01, 0x02, 0x03
    // Value 1: 0x04, 0x05, 0x06
    //
    // Byte stream split layout:
    // Stream 0 (byte 0 of each): 0x01, 0x04
    // Stream 1 (byte 1 of each): 0x02, 0x05
    // Stream 2 (byte 2 of each): 0x03, 0x06
    const data = [_]u8{
        0x01, 0x04, // stream 0
        0x02, 0x05, // stream 1
        0x03, 0x06, // stream 2
    };

    const result = try decodeFixedLenAlloc(allocator, &data, 2, 3);
    defer {
        for (result) |slice| allocator.free(slice);
        allocator.free(result);
    }

    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x01, 0x02, 0x03 }, result[0]);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x04, 0x05, 0x06 }, result[1]);
}

test "byte stream split decode fixed len insufficient data" {
    const allocator = std.testing.allocator;

    // Only 4 bytes, but need 6 for 2 values of 3 bytes each
    const data = [_]u8{ 0x01, 0x02, 0x03, 0x04 };
    const result = decodeFixedLenAlloc(allocator, &data, 2, 3);
    try std.testing.expectError(error.InsufficientData, result);
}
