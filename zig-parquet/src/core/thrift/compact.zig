//! Thrift Compact Protocol decoder
//!
//! The Compact Protocol is a binary protocol that uses variable-length encoding
//! for integers and packs field headers efficiently.

const std = @import("std");
const safe = @import("../safe.zig");

/// Thrift type codes used in Compact Protocol
pub const Type = enum(u4) {
    stop = 0,
    bool_true = 1,
    bool_false = 2,
    i8 = 3,
    i16 = 4,
    i32 = 5,
    i64 = 6,
    double = 7,
    binary = 8, // Also used for strings
    list = 9,
    set = 10,
    map = 11,
    struct_ = 12,
    // uuid = 13, // Not used in Parquet
};

pub const FieldHeader = struct {
    field_id: i16,
    field_type: Type,
};

pub const ListHeader = struct {
    element_type: Type,
    size: u32,
};

/// A reader for Thrift Compact Protocol encoded data
pub const CompactReader = struct {
    data: []const u8,
    pos: usize = 0,
    last_field_id: i16 = 0,

    const Self = @This();

    pub fn init(data: []const u8) Self {
        return .{ .data = data };
    }

    /// Read a single byte
    pub fn readByte(self: *Self) !u8 {
        if (self.pos >= self.data.len) return error.EndOfData;
        const b = self.data[self.pos];
        self.pos += 1;
        return b;
    }

    /// Read N bytes
    fn readBytes(self: *Self, n: usize) ![]const u8 {
        if (self.pos + n > self.data.len) return error.EndOfData;
            const bytes = try safe.slice(self.data, self.pos, n);
        self.pos += n;
        return bytes;
    }

    /// Read an unsigned variable-length integer (varint)
    pub fn readVarInt(self: *Self) !u64 {
        var result: u64 = 0;
        var shift: u6 = 0;
        while (true) {
            const b = try self.readByte();
            result |= @as(u64, b & 0x7F) << shift;
            if (b & 0x80 == 0) break;
            shift += 7;
            if (shift >= 64) return error.VarIntTooLong;
        }
        return result;
    }

    /// Decode a zigzag-encoded signed integer
    fn zigzagDecode(n: u64) i64 {
        return @bitCast((n >> 1) ^ -%@as(u64, n & 1));
    }

    /// Read a signed 16-bit integer (zigzag + varint)
    pub fn readI16(self: *Self) !i16 {
        const v = try self.readVarInt();
        const decoded = zigzagDecode(v);
        if (decoded < std.math.minInt(i16) or decoded > std.math.maxInt(i16)) {
            return error.IntegerOverflow;
        }
        return safe.castTo(i16, decoded) catch return error.IntegerOverflow;
    }

    /// Read a signed 32-bit integer (zigzag + varint)
    pub fn readI32(self: *Self) !i32 {
        const v = try self.readVarInt();
        const decoded = zigzagDecode(v);
        if (decoded < std.math.minInt(i32) or decoded > std.math.maxInt(i32)) {
            return error.IntegerOverflow;
        }
        return safe.castTo(i32, decoded) catch return error.IntegerOverflow;
    }

    /// Read a signed 64-bit integer (zigzag + varint)
    pub fn readI64(self: *Self) !i64 {
        const v = try self.readVarInt();
        return zigzagDecode(v);
    }

    /// Read an 8-byte double
    pub fn readDouble(self: *Self) !f64 {
        const bytes = try self.readBytes(8);
        return @bitCast(std.mem.readInt(u64, bytes[0..8], .little));
    }

    /// Read a length-prefixed binary blob
    pub fn readBinary(self: *Self) ![]const u8 {
        const len = try self.readVarInt();
        if (len > std.math.maxInt(usize)) return error.LengthTooLong;
        return try self.readBytes(safe.castTo(usize, len) catch return error.IntegerOverflow);
    }

    /// Read a length-prefixed string (same as binary but semantically UTF-8)
    pub fn readString(self: *Self) ![]const u8 {
        return self.readBinary();
    }

    /// Read a field header, returns null when STOP field is encountered
    pub fn readFieldHeader(self: *Self) !?FieldHeader {
        const b = try self.readByte();

        // STOP field
        if (b == 0) return null;

        // Mask to 4 bits: value 0-15 always fits in u4
        const type_id: u4 = @truncate(b & 0x0F);
        const field_type = std.meta.intToEnum(Type, type_id) catch return error.InvalidFieldType;

        // Delta encoding: high nibble contains field ID delta
        const delta: u4 = @truncate((b >> 4) & 0x0F);
        var field_id: i16 = undefined;

        if (delta != 0) {
            // Delta encoded
            field_id = self.last_field_id + delta;
        } else {
            // Full field ID follows
            field_id = try self.readI16();
        }

        self.last_field_id = field_id;
        return .{ .field_id = field_id, .field_type = field_type };
    }

    /// Read a list header
    pub fn readListHeader(self: *Self) !ListHeader {
        const b = try self.readByte();

        // Mask to 4 bits: value 0-15 always fits in u4
        const size_nibble: u4 = @truncate((b >> 4) & 0x0F);
        const type_nibble: u4 = @truncate(b & 0x0F);
        const element_type = std.meta.intToEnum(Type, type_nibble) catch return error.InvalidListType;

        var size: u32 = undefined;
        if (size_nibble == 0x0F) {
            // Size is in following varint
            const s = try self.readVarInt();
            if (s > std.math.maxInt(u32)) return error.ListTooLong;
            size = safe.castTo(u32, s) catch return error.IntegerOverflow;
        } else {
            size = size_nibble;
        }

        return .{ .element_type = element_type, .size = size };
    }

    /// Reset field ID tracking (call when entering a new struct)
    pub fn resetFieldTracking(self: *Self) void {
        self.last_field_id = 0;
    }

    /// Skip a value of the given type
    pub fn skip(self: *Self, t: Type) !void {
        switch (t) {
            .stop => {},
            .bool_true, .bool_false => {},
            .i8 => _ = try self.readByte(),
            .i16, .i32, .i64 => _ = try self.readVarInt(),
            .double => _ = try self.readBytes(8),
            .binary => {
                const len = try self.readVarInt();
                if (len > std.math.maxInt(usize)) return error.LengthTooLong;
                _ = try self.readBytes(safe.castTo(usize, len) catch return error.IntegerOverflow);
            },
            .list, .set => {
                const header = try self.readListHeader();
                for (0..header.size) |_| {
                    try self.skip(header.element_type);
                }
            },
            .map => {
                const size = try self.readVarInt();
                if (size == 0) return;
                if (size > std.math.maxInt(usize)) return error.ListTooLong;
                const types = try self.readByte();
                // Mask to 4 bits: (0-15) always fits in u4
                const key_type: Type = @enumFromInt(@as(u4, @truncate((types >> 4) & 0x0F)));
                const val_type: Type = @enumFromInt(@as(u4, @truncate(types & 0x0F)));
                for (0..safe.castTo(usize, size) catch return error.IntegerOverflow) |_| {
                    try self.skip(key_type);
                    try self.skip(val_type);
                }
            },
            .struct_ => {
                const saved_field_id = self.last_field_id;
                self.resetFieldTracking();
                while (try self.readFieldHeader()) |field| {
                    try self.skip(field.field_type);
                }
                self.last_field_id = saved_field_id;
            },
        }
    }

    /// Get remaining unread data
    pub fn remaining(self: *Self) []const u8 {
        return self.data[self.pos..];
    }

    /// Check if at end of data
    pub fn isAtEnd(self: *Self) bool {
        return self.pos >= self.data.len;
    }
};

// Tests
test "varint decoding" {
    // Test value: 300 = 0b100101100 encoded as 0xAC 0x02
    var reader = CompactReader.init(&[_]u8{ 0xAC, 0x02 });
    const v = try reader.readVarInt();
    try std.testing.expectEqual(@as(u64, 300), v);
}

test "zigzag decoding" {
    // 0 -> 0, 1 -> -1, 2 -> 1, 3 -> -2, 4 -> 2
    try std.testing.expectEqual(@as(i64, 0), CompactReader.zigzagDecode(0));
    try std.testing.expectEqual(@as(i64, -1), CompactReader.zigzagDecode(1));
    try std.testing.expectEqual(@as(i64, 1), CompactReader.zigzagDecode(2));
    try std.testing.expectEqual(@as(i64, -2), CompactReader.zigzagDecode(3));
    try std.testing.expectEqual(@as(i64, 2), CompactReader.zigzagDecode(4));
}

test "field header with delta" {
    // Field with delta=1 and type=i32 (5): byte = 0x15
    var reader = CompactReader.init(&[_]u8{0x15});
    const field = try reader.readFieldHeader();
    try std.testing.expect(field != null);
    try std.testing.expectEqual(@as(i16, 1), field.?.field_id);
    try std.testing.expectEqual(Type.i32, field.?.field_type);
}

test "stop field" {
    var reader = CompactReader.init(&[_]u8{0x00});
    const field = try reader.readFieldHeader();
    try std.testing.expect(field == null);
}
