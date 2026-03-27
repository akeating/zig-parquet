//! DICTIONARY encoding support
//!
//! Dictionary encoding stores unique values in a dictionary page, then
//! references them by index in data pages. This is efficient for columns
//! with low cardinality (few unique values).
//!
//! The dictionary page contains PLAIN-encoded values.
//! Data pages contain RLE/bit-packed encoded indices.

const std = @import("std");
const plain = @import("plain.zig");
const rle = @import("rle.zig");
const safe = @import("../safe.zig");

/// Dictionary for string/binary values
pub const StringDictionary = struct {
    allocator: std.mem.Allocator,
    values: [][]const u8,
    owns_memory: bool,

    const Self = @This();

    /// Create a dictionary from PLAIN-encoded byte arrays
    pub fn fromPlain(allocator: std.mem.Allocator, data: []const u8, num_values: usize) !Self {
        var values = try allocator.alloc([]const u8, num_values);
        var initialized: usize = 0;
        errdefer {
            for (values[0..initialized]) |v| allocator.free(v);
            allocator.free(values);
        }

        var pos: usize = 0;
        for (0..num_values) |i| {
            if (pos + 4 > data.len) return error.EndOfData;
            const ba = try plain.decodeByteArray(data[pos..]);
            values[i] = try allocator.dupe(u8, ba.value);
            initialized += 1;
            pos += ba.bytes_read;
        }

        return .{
            .allocator = allocator,
            .values = values,
            .owns_memory = true,
        };
    }

    /// Look up a value by index
    pub fn get(self: *const Self, index: u32) ?[]const u8 {
        if (index >= self.values.len) return null;
        return self.values[index];
    }

    pub fn deinit(self: *Self) void {
        if (self.owns_memory) {
            for (self.values) |v| {
                self.allocator.free(v);
            }
        }
        self.allocator.free(self.values);
    }
};

/// Dictionary for i32 values
pub const Int32Dictionary = struct {
    allocator: std.mem.Allocator,
    values: []i32,

    const Self = @This();

    /// Create a dictionary from PLAIN-encoded i32 values
    pub fn fromPlain(allocator: std.mem.Allocator, data: []const u8, num_values: usize) !Self {
        const required = std.math.mul(usize, num_values, 4) catch return error.EndOfData;
        if (data.len < required) return error.EndOfData;

        var values = try allocator.alloc(i32, num_values);
        errdefer allocator.free(values);

        for (0..num_values) |i| {
            values[i] = try plain.decodeI32(data[i * 4 ..]);
        }

        return .{
            .allocator = allocator,
            .values = values,
        };
    }

    /// Look up a value by index
    pub fn get(self: *const Self, index: u32) ?i32 {
        if (index >= self.values.len) return null;
        return self.values[index];
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.values);
    }
};

/// Dictionary for i64 values
pub const Int64Dictionary = struct {
    allocator: std.mem.Allocator,
    values: []i64,

    const Self = @This();

    /// Create a dictionary from PLAIN-encoded i64 values
    pub fn fromPlain(allocator: std.mem.Allocator, data: []const u8, num_values: usize) !Self {
        const required = std.math.mul(usize, num_values, 8) catch return error.EndOfData;
        if (data.len < required) return error.EndOfData;

        var values = try allocator.alloc(i64, num_values);
        errdefer allocator.free(values);

        for (0..num_values) |i| {
            values[i] = try plain.decodeI64(data[i * 8 ..]);
        }

        return .{
            .allocator = allocator,
            .values = values,
        };
    }

    /// Look up a value by index
    pub fn get(self: *const Self, index: u32) ?i64 {
        if (index >= self.values.len) return null;
        return self.values[index];
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.values);
    }
};

/// Dictionary for f32 (float) values
pub const Float32Dictionary = struct {
    allocator: std.mem.Allocator,
    values: []f32,

    const Self = @This();

    /// Create a dictionary from PLAIN-encoded f32 values
    pub fn fromPlain(allocator: std.mem.Allocator, data: []const u8, num_values: usize) !Self {
        const required = std.math.mul(usize, num_values, 4) catch return error.EndOfData;
        if (data.len < required) return error.EndOfData;

        var values = try allocator.alloc(f32, num_values);
        errdefer allocator.free(values);

        for (0..num_values) |i| {
            values[i] = try plain.decodeFloat(data[i * 4 ..]);
        }

        return .{
            .allocator = allocator,
            .values = values,
        };
    }

    /// Look up a value by index
    pub fn get(self: *const Self, index: u32) ?f32 {
        if (index >= self.values.len) return null;
        return self.values[index];
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.values);
    }
};

/// Dictionary for f64 (double) values
pub const Float64Dictionary = struct {
    allocator: std.mem.Allocator,
    values: []f64,

    const Self = @This();

    /// Create a dictionary from PLAIN-encoded f64 values
    pub fn fromPlain(allocator: std.mem.Allocator, data: []const u8, num_values: usize) !Self {
        const required = std.math.mul(usize, num_values, 8) catch return error.EndOfData;
        if (data.len < required) return error.EndOfData;

        var values = try allocator.alloc(f64, num_values);
        errdefer allocator.free(values);

        for (0..num_values) |i| {
            values[i] = try plain.decodeDouble(data[i * 8 ..]);
        }

        return .{
            .allocator = allocator,
            .values = values,
        };
    }

    /// Look up a value by index
    pub fn get(self: *const Self, index: u32) ?f64 {
        if (index >= self.values.len) return null;
        return self.values[index];
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.values);
    }
};

/// Dictionary for Int96 (legacy timestamp) values
pub const Int96Dictionary = struct {
    allocator: std.mem.Allocator,
    values: [][12]u8,

    const Self = @This();

    /// Create a dictionary from PLAIN-encoded Int96 values (12 bytes each)
    pub fn fromPlain(allocator: std.mem.Allocator, data: []const u8, num_values: usize) !Self {
        const required = std.math.mul(usize, num_values, 12) catch return error.EndOfData;
        if (data.len < required) return error.EndOfData;

        var values = try allocator.alloc([12]u8, num_values);
        errdefer allocator.free(values);

        for (0..num_values) |i| {
            values[i] = (safe.slice(data, i * 12, 12) catch return error.EndOfData)[0..12].*;
        }

        return .{
            .allocator = allocator,
            .values = values,
        };
    }

    /// Look up a value by index (returns raw 12-byte array)
    pub fn get(self: *const Self, index: u32) ?[12]u8 {
        if (index >= self.values.len) return null;
        return self.values[index];
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.values);
    }
};

/// Dictionary for fixed-length byte array values (e.g., float16, UUID)
pub const FixedByteArrayDictionary = struct {
    allocator: std.mem.Allocator,
    values: [][]const u8,
    fixed_len: usize,

    const Self = @This();

    /// Create a dictionary from PLAIN-encoded fixed-length byte arrays
    pub fn fromPlain(allocator: std.mem.Allocator, data: []const u8, num_values: usize, fixed_len: usize) !Self {
        const required = std.math.mul(usize, num_values, fixed_len) catch return error.EndOfData;
        if (data.len < required) return error.EndOfData;

        var values = try allocator.alloc([]const u8, num_values);
        var initialized: usize = 0;
        errdefer {
            for (values[0..initialized]) |v| allocator.free(v);
            allocator.free(values);
        }

        for (0..num_values) |i| {
            const start = i * fixed_len;
            values[i] = try allocator.dupe(u8, safe.slice(data, start, fixed_len) catch return error.EndOfData);
            initialized += 1;
        }

        return .{
            .allocator = allocator,
            .values = values,
            .fixed_len = fixed_len,
        };
    }

    /// Look up a value by index
    pub fn get(self: *const Self, index: u32) ?[]const u8 {
        if (index >= self.values.len) return null;
        return self.values[index];
    }

    pub fn deinit(self: *Self) void {
        for (self.values) |v| {
            self.allocator.free(v);
        }
        self.allocator.free(self.values);
    }
};

// Tests
test "StringDictionary basic" {
    const allocator = std.testing.allocator;

    // Create PLAIN-encoded dictionary data
    // Value 0: "hello" (5 bytes)
    // Value 1: "world" (5 bytes)
    const dict_data = [_]u8{
        5, 0, 0, 0, 'h', 'e', 'l', 'l', 'o',
        5, 0, 0, 0, 'w', 'o', 'r', 'l', 'd',
    };

    var dict = try StringDictionary.fromPlain(allocator, &dict_data, 2);
    defer dict.deinit();

    try std.testing.expectEqualStrings("hello", dict.get(0).?);
    try std.testing.expectEqualStrings("world", dict.get(1).?);
    try std.testing.expectEqual(@as(?[]const u8, null), dict.get(2));
}

test "Int32Dictionary basic" {
    const allocator = std.testing.allocator;

    // Create PLAIN-encoded dictionary data: [1, 2, 3]
    const dict_data = [_]u8{
        1, 0, 0, 0, // 1
        2, 0, 0, 0, // 2
        3, 0, 0, 0, // 3
    };

    var dict = try Int32Dictionary.fromPlain(allocator, &dict_data, 3);
    defer dict.deinit();

    try std.testing.expectEqual(@as(i32, 1), dict.get(0).?);
    try std.testing.expectEqual(@as(i32, 2), dict.get(1).?);
    try std.testing.expectEqual(@as(i32, 3), dict.get(2).?);
    try std.testing.expectEqual(@as(?i32, null), dict.get(3));
}

test "FixedByteArrayDictionary basic" {
    const allocator = std.testing.allocator;

    // 3 values of fixed_len=4: [0x01020304, 0x05060708, 0x090A0B0C]
    const dict_data = [_]u8{
        0x01, 0x02, 0x03, 0x04,
        0x05, 0x06, 0x07, 0x08,
        0x09, 0x0A, 0x0B, 0x0C,
    };

    var dict = try FixedByteArrayDictionary.fromPlain(allocator, &dict_data, 3, 4);
    defer dict.deinit();

    try std.testing.expectEqualSlices(u8, &.{ 0x01, 0x02, 0x03, 0x04 }, dict.get(0).?);
    try std.testing.expectEqualSlices(u8, &.{ 0x05, 0x06, 0x07, 0x08 }, dict.get(1).?);
    try std.testing.expectEqualSlices(u8, &.{ 0x09, 0x0A, 0x0B, 0x0C }, dict.get(2).?);
    try std.testing.expectEqual(@as(?[]const u8, null), dict.get(3));
}

test "Dictionary truncated data returns EndOfData" {
    const allocator = std.testing.allocator;

    const short_data = [_]u8{ 0x01, 0x02 };

    try std.testing.expectError(error.EndOfData, Int32Dictionary.fromPlain(allocator, &short_data, 100));
    try std.testing.expectError(error.EndOfData, Int64Dictionary.fromPlain(allocator, &short_data, 100));
    try std.testing.expectError(error.EndOfData, Float32Dictionary.fromPlain(allocator, &short_data, 100));
    try std.testing.expectError(error.EndOfData, Float64Dictionary.fromPlain(allocator, &short_data, 100));
    try std.testing.expectError(error.EndOfData, Int96Dictionary.fromPlain(allocator, &short_data, 100));
    try std.testing.expectError(error.EndOfData, FixedByteArrayDictionary.fromPlain(allocator, &short_data, 100, 8));

    // Overflow case: num_values so large that num_values * N overflows usize
    const max = std.math.maxInt(usize);
    try std.testing.expectError(error.EndOfData, Int32Dictionary.fromPlain(allocator, &short_data, max));
    try std.testing.expectError(error.EndOfData, FixedByteArrayDictionary.fromPlain(allocator, &short_data, max, 8));
}
