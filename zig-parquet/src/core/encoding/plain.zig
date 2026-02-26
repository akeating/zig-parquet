//! PLAIN encoding decoder
//!
//! PLAIN is the simplest encoding in Parquet:
//! - Boolean: 1 bit per value, packed into bytes
//! - Int32/Int64: 4/8 bytes little-endian
//! - Float/Double: 4/8 bytes IEEE 754 little-endian
//! - ByteArray: 4-byte length prefix + bytes
//! - FixedLenByteArray: just the bytes (length known from schema)
//! - Int96: 12 bytes (legacy timestamp format)

const std = @import("std");
const safe = @import("../safe.zig");
const types = @import("../types.zig");
const Int96 = types.Int96;

/// Decode a boolean at the given index (booleans are bit-packed)
/// Returns error if index is out of bounds.
pub fn decodeBool(data: []const u8, index: usize) error{EndOfData}!bool {
    const byte_idx = index / 8;
    if (byte_idx >= data.len) return error.EndOfData;
        const bit_idx: u3 = safe.castTo(u3, index % 8) catch return error.EndOfData;
    return (data[byte_idx] >> bit_idx) & 1 == 1;
}

/// Decode an i32 from the current position
pub fn decodeI32(data: []const u8) !i32 {
    if (data.len < 4) return error.EndOfData;
    return std.mem.readInt(i32, data[0..4], .little);
}

/// Decode a u32 from the current position
pub fn decodeU32(data: []const u8) !u32 {
    if (data.len < 4) return error.EndOfData;
    return std.mem.readInt(u32, data[0..4], .little);
}

/// Decode an i64 from the current position
pub fn decodeI64(data: []const u8) !i64 {
    if (data.len < 8) return error.EndOfData;
    return std.mem.readInt(i64, data[0..8], .little);
}

/// Decode a u64 from the current position
pub fn decodeU64(data: []const u8) !u64 {
    if (data.len < 8) return error.EndOfData;
    return std.mem.readInt(u64, data[0..8], .little);
}

/// Decode an f32 from the current position
pub fn decodeFloat(data: []const u8) !f32 {
    if (data.len < 4) return error.EndOfData;
    const bits = std.mem.readInt(u32, data[0..4], .little);
    return @bitCast(bits);
}

/// Decode an f64 from the current position
pub fn decodeDouble(data: []const u8) !f64 {
    if (data.len < 8) return error.EndOfData;
    const bits = std.mem.readInt(u64, data[0..8], .little);
    return @bitCast(bits);
}

/// Decode a byte array (returns slice and bytes consumed)
pub const ByteArrayResult = struct {
    value: []const u8,
    bytes_read: usize,
};

pub fn decodeByteArray(data: []const u8) !ByteArrayResult {
    const len = try decodeU32(data);
    if (data.len < 4 + len) return error.EndOfData;
    return .{
        .value = data[4..][0..len],
        .bytes_read = 4 + len,
    };
}

/// A streaming decoder for PLAIN-encoded values
pub fn PlainDecoder(comptime T: type) type {
    return struct {
        data: []const u8,
        pos: usize = 0,
        count: usize,
        current_index: usize = 0,

        const Self = @This();

        pub fn init(data: []const u8, count: usize) Self {
            return .{ .data = data, .count = count };
        }

        pub fn next(self: *Self) ?T {
            if (self.current_index >= self.count) return null;
            self.current_index += 1;

            if (T == bool) {
                const result = decodeBool(self.data, self.current_index - 1) catch return null;
                return result;
            } else if (T == i32) {
                const result = decodeI32(self.data[self.pos..]) catch return null;
                self.pos += 4;
                return result;
            } else if (T == i64) {
                const result = decodeI64(self.data[self.pos..]) catch return null;
                self.pos += 8;
                return result;
            } else if (T == f32) {
                const result = decodeFloat(self.data[self.pos..]) catch return null;
                self.pos += 4;
                return result;
            } else if (T == f64) {
                const result = decodeDouble(self.data[self.pos..]) catch return null;
                self.pos += 8;
                return result;
            } else if (T == []const u8) {
                const result = decodeByteArray(self.data[self.pos..]) catch return null;
                self.pos += result.bytes_read;
                return result.value;
            } else if (T == f16) {
                // Float16: 2 bytes, IEEE 754 half-precision
                if (self.pos + 2 > self.data.len) return null;
                const bytes: [2]u8 = .{ self.data[self.pos], self.data[self.pos + 1] };
                const result: f16 = @bitCast(bytes);
                self.pos += 2;
                return result;
            } else if (T == Int96) {
                // Int96: 12 bytes (legacy timestamp format)
                if (self.pos + 12 > self.data.len) return null;
                const slice_12 = safe.slice(self.data, self.pos, 12) catch return null;
                const result = Int96.fromBytes(slice_12[0..12].*);
                self.pos += 12;
                return result;
            } else {
                @compileError("Unsupported type for PlainDecoder");
            }
        }

        pub fn remaining(self: *const Self) usize {
            return self.count - self.current_index;
        }
    };
}

// Tests
test "decode bool" {
    // Byte 0xB5 = 0b10110101 = bits 0,2,4,5,7 are set
    const data = [_]u8{0xB5};
    try std.testing.expect(try decodeBool(&data, 0) == true);
    try std.testing.expect(try decodeBool(&data, 1) == false);
    try std.testing.expect(try decodeBool(&data, 2) == true);
    try std.testing.expect(try decodeBool(&data, 3) == false);
    try std.testing.expect(try decodeBool(&data, 4) == true);
    try std.testing.expect(try decodeBool(&data, 5) == true);
    try std.testing.expect(try decodeBool(&data, 6) == false);
    try std.testing.expect(try decodeBool(&data, 7) == true);
    // Test out of bounds returns error
    try std.testing.expectError(error.EndOfData, decodeBool(&data, 8));
}

test "decode i32" {
    // 0x01020304 little-endian
    const data = [_]u8{ 0x04, 0x03, 0x02, 0x01 };
    try std.testing.expectEqual(@as(i32, 0x01020304), try decodeI32(&data));
}

test "decode i32 negative" {
    // -1 = 0xFFFFFFFF
    const data = [_]u8{ 0xFF, 0xFF, 0xFF, 0xFF };
    try std.testing.expectEqual(@as(i32, -1), try decodeI32(&data));
}

test "decode f64" {
    // 1.5 in IEEE 754 double
    const data = [_]u8{ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF8, 0x3F };
    try std.testing.expectEqual(@as(f64, 1.5), try decodeDouble(&data));
}

test "decode byte array" {
    // Length 5, then "hello"
    const data = [_]u8{ 0x05, 0x00, 0x00, 0x00, 'h', 'e', 'l', 'l', 'o' };
    const result = try decodeByteArray(&data);
    try std.testing.expectEqualStrings("hello", result.value);
    try std.testing.expectEqual(@as(usize, 9), result.bytes_read);
}

test "PlainDecoder i32" {
    const data = [_]u8{
        0x01, 0x00, 0x00, 0x00, // 1
        0xFE, 0xFF, 0xFF, 0xFF, // -2
        0xFF, 0xFF, 0xFF, 0x7F, // 2147483647
    };
    var decoder = PlainDecoder(i32).init(&data, 3);
    try std.testing.expectEqual(@as(?i32, 1), decoder.next());
    try std.testing.expectEqual(@as(?i32, -2), decoder.next());
    try std.testing.expectEqual(@as(?i32, 2147483647), decoder.next());
    try std.testing.expectEqual(@as(?i32, null), decoder.next());
}
