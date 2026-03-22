//! Thrift Compact Protocol encoder
//!
//! The Compact Protocol is a binary protocol that uses variable-length encoding
//! for integers and packs field headers efficiently.
//!
//! This is the inverse of CompactReader - used for serializing Parquet metadata.

const std = @import("std");
const safe = @import("../safe.zig");
const compact = @import("compact.zig");
const Type = compact.Type;

/// A writer for Thrift Compact Protocol encoded data
pub const CompactWriter = struct {
    buffer: std.ArrayList(u8),
    allocator: std.mem.Allocator,
    last_field_id: i16 = 0,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return .{
            .buffer = .empty,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.buffer.deinit(self.allocator);
    }

    /// Get the written bytes
    pub fn getWritten(self: *const Self) []const u8 {
        return self.buffer.items;
    }

    /// Reset the writer for reuse
    pub fn reset(self: *Self) void {
        self.buffer.clearRetainingCapacity();
        self.last_field_id = 0;
    }

    /// Write a single byte
    pub fn writeByte(self: *Self, b: u8) !void {
        try self.buffer.append(self.allocator, b);
    }

    /// Write multiple bytes
    pub fn writeBytes(self: *Self, bytes: []const u8) !void {
        try self.buffer.appendSlice(self.allocator, bytes);
    }

    /// Write an unsigned variable-length integer (varint)
    pub fn writeVarInt(self: *Self, value: u64) !void {
        var v = value;
        while (v >= 0x80) {
            try self.writeByte(@as(u8, @truncate(v)) | 0x80);
            v >>= 7;
        }
        try self.writeByte(@truncate(v));
    }

    /// Encode a signed integer using zigzag encoding
    fn zigzagEncode(n: i64) u64 {
        const un: u64 = @bitCast(n);
        return (un << 1) ^ @as(u64, @bitCast(n >> 63));
    }

    /// Write a signed 16-bit integer (zigzag + varint)
    pub fn writeI16(self: *Self, value: i16) !void {
        try self.writeVarInt(zigzagEncode(value));
    }

    /// Write a signed 32-bit integer (zigzag + varint)
    pub fn writeI32(self: *Self, value: i32) !void {
        try self.writeVarInt(zigzagEncode(value));
    }

    /// Write a signed 64-bit integer (zigzag + varint)
    pub fn writeI64(self: *Self, value: i64) !void {
        try self.writeVarInt(zigzagEncode(value));
    }

    /// Write an 8-byte double (little-endian)
    pub fn writeDouble(self: *Self, value: f64) !void {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, @bitCast(value), .little);
        try self.writeBytes(&buf);
    }

    /// Write a length-prefixed binary blob
    pub fn writeBinary(self: *Self, data: []const u8) !void {
        try self.writeVarInt(data.len);
        try self.writeBytes(data);
    }

    /// Write a length-prefixed string (same as binary)
    pub fn writeString(self: *Self, str: []const u8) !void {
        try self.writeBinary(str);
    }

    /// Write a field header with delta encoding
    pub fn writeFieldHeader(self: *Self, field_id: i16, field_type: Type) !void {
        const delta = field_id - self.last_field_id;

        if (delta > 0 and delta <= 15) {
            // Delta fits in high nibble
        const header: u8 = (@as(u8, safe.castTo(u8, delta) catch unreachable) << 4) | @intFromEnum(field_type); // delta is 1..15 from check above
            try self.writeByte(header);
        } else {
            // Write type nibble with 0 delta, then full field ID
            try self.writeByte(@intFromEnum(field_type));
            try self.writeI16(field_id);
        }

        self.last_field_id = field_id;
    }

    /// Write a boolean field (type encodes the value)
    pub fn writeBoolField(self: *Self, field_id: i16, value: bool) !void {
        const field_type: Type = if (value) .bool_true else .bool_false;
        try self.writeFieldHeader(field_id, field_type);
    }

    /// Write a struct end marker (STOP field)
    pub fn writeStructEnd(self: *Self) !void {
        try self.writeByte(0x00);
    }

    /// Write a list header
    pub fn writeListHeader(self: *Self, element_type: Type, size: u32) !void {
        const type_nibble: u8 = @intFromEnum(element_type);
        if (size < 15) {
            // Size fits in high nibble
            const header: u8 = (@as(u8, safe.castTo(u8, size) catch unreachable) << 4) | type_nibble; // size < 15 from check above
            try self.writeByte(header);
        } else {
            // Size in separate varint
            const header: u8 = 0xF0 | type_nibble;
            try self.writeByte(header);
            try self.writeVarInt(size);
        }
    }

    /// Reset field ID tracking (call when entering a new struct)
    pub fn resetFieldTracking(self: *Self) void {
        self.last_field_id = 0;
    }

    /// Save current field ID (for nested structs)
    pub fn saveFieldId(self: *const Self) i16 {
        return self.last_field_id;
    }

    /// Restore field ID (after nested struct)
    pub fn restoreFieldId(self: *Self, saved: i16) void {
        self.last_field_id = saved;
    }
};

// Tests
test "varint encoding" {
    var writer = CompactWriter.init(std.testing.allocator);
    defer writer.deinit();

    // Test value: 300 = 0b100101100 encoded as 0xAC 0x02
    try writer.writeVarInt(300);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0xAC, 0x02 }, writer.getWritten());
}

test "zigzag encoding" {
    // 0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3, 2 -> 4
    try std.testing.expectEqual(@as(u64, 0), CompactWriter.zigzagEncode(0));
    try std.testing.expectEqual(@as(u64, 1), CompactWriter.zigzagEncode(-1));
    try std.testing.expectEqual(@as(u64, 2), CompactWriter.zigzagEncode(1));
    try std.testing.expectEqual(@as(u64, 3), CompactWriter.zigzagEncode(-2));
    try std.testing.expectEqual(@as(u64, 4), CompactWriter.zigzagEncode(2));
}

test "field header with delta" {
    var writer = CompactWriter.init(std.testing.allocator);
    defer writer.deinit();

    // Field ID 1 with type i32 (5): delta=1, so byte = 0x15
    try writer.writeFieldHeader(1, .i32);
    try std.testing.expectEqualSlices(u8, &[_]u8{0x15}, writer.getWritten());
}

test "struct end" {
    var writer = CompactWriter.init(std.testing.allocator);
    defer writer.deinit();

    try writer.writeStructEnd();
    try std.testing.expectEqualSlices(u8, &[_]u8{0x00}, writer.getWritten());
}

test "list header small" {
    var writer = CompactWriter.init(std.testing.allocator);
    defer writer.deinit();

    // List of 3 i32s: size=3 in high nibble, type=5 in low nibble = 0x35
    try writer.writeListHeader(.i32, 3);
    try std.testing.expectEqualSlices(u8, &[_]u8{0x35}, writer.getWritten());
}

test "binary encoding" {
    var writer = CompactWriter.init(std.testing.allocator);
    defer writer.deinit();

    try writer.writeBinary("hello");
    // Length 5 as varint (0x05), then "hello"
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x05, 'h', 'e', 'l', 'l', 'o' }, writer.getWritten());
}

test "round-trip zigzag" {
    const compact_reader = @import("compact.zig");

    // Write a signed value
    var writer = CompactWriter.init(std.testing.allocator);
    defer writer.deinit();

    try writer.writeI32(-12345);

    // Read it back
    var reader = compact_reader.CompactReader.init(writer.getWritten());
    const value = try reader.readI32();

    try std.testing.expectEqual(@as(i32, -12345), value);
}
