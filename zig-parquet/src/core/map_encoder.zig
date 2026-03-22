//! Map Encoder
//!
//! Flattens nested map structures into flat keys, values, and definition/repetition levels.
//!
//! For a standard map schema (map_col -> key_value -> key/value):
//!   Key column (REQUIRED):
//!     - def=0: map is null
//!     - def=1: map is empty
//!     - def=2: key is present (entry exists)
//!
//!   Value column (OPTIONAL):
//!     - def=0: map is null
//!     - def=1: map is empty
//!     - def=2: entry exists but value is null
//!     - def=3: value is present
//!
//!   Repetition levels (same for both):
//!     - rep=0: new map (new row)
//!     - rep=1: additional entry in same map

const std = @import("std");
const types = @import("types.zig");

pub const Optional = types.Optional;

/// A single map entry with key and optional value
pub fn MapEntry(comptime K: type, comptime V: type) type {
    return struct {
        key: K,
        value: Optional(V),
    };
}

/// Result of flattening a map column
pub fn FlattenedMap(comptime K: type, comptime V: type) type {
    return struct {
        /// Flat array of keys
        keys: []K,
        /// Flat array of non-null values
        values: []V,
        /// Definition levels for key column
        key_def_levels: []u32,
        /// Definition levels for value column
        value_def_levels: []u32,
        /// Repetition levels (same for both columns)
        rep_levels: []u32,
        /// Total number of slots
        num_slots: usize,

        allocator: std.mem.Allocator,

        const Self = @This();

        pub fn deinit(self: *Self) void {
            if (K == []const u8) {
                for (self.keys) |k| self.allocator.free(k);
            }
            if (V == []const u8) {
                for (self.values) |v| self.allocator.free(v);
            }
            self.allocator.free(self.keys);
            self.allocator.free(self.values);
            self.allocator.free(self.key_def_levels);
            self.allocator.free(self.value_def_levels);
            self.allocator.free(self.rep_levels);
        }
    };
}

/// Flatten a map column into keys, values, and levels.
///
/// Takes nested slices []Optional([]MapEntry(K, V)) and produces:
/// - Flat array of keys
/// - Flat array of non-null values
/// - Definition levels for key and value columns
/// - Repetition levels for both
///
/// For key column (max_def=2):
///   - def=0: map is null
///   - def=1: map is empty
///   - def=2: key present (entry exists)
///
/// For value column (max_def=3 if value_optional):
///   - def=0: map is null
///   - def=1: map is empty
///   - def=2: value is null
///   - def=3: value present
pub fn flattenMap(
    comptime K: type,
    comptime V: type,
    allocator: std.mem.Allocator,
    maps: []const Optional([]const MapEntry(K, V)),
    value_max_def: u8,
) !FlattenedMap(K, V) {

    // First pass: count total slots, keys, and values
    var num_slots: usize = 0;
    var num_keys: usize = 0;
    var num_values: usize = 0;

    for (maps) |map_opt| {
        switch (map_opt) {
            .null_value => {
                // Null map: 1 slot with def=0
                num_slots += 1;
            },
            .value => |entries| {
                if (entries.len == 0) {
                    // Empty map: 1 slot with def=1
                    num_slots += 1;
                } else {
                    // Map with entries: 1 slot per entry
                    num_slots += entries.len;
                    num_keys += entries.len;
                    for (entries) |entry| {
                        switch (entry.value) {
                            .value => num_values += 1,
                            .null_value => {},
                        }
                    }
                }
            },
        }
    }

    // Allocate result arrays
    var key_def_levels = try allocator.alloc(u32, num_slots);
    errdefer allocator.free(key_def_levels);

    var value_def_levels = try allocator.alloc(u32, num_slots);
    errdefer allocator.free(value_def_levels);

    var rep_levels = try allocator.alloc(u32, num_slots);
    errdefer allocator.free(rep_levels);

    var keys = try allocator.alloc(K, num_keys);
    errdefer {
        if (K == []const u8) {
            for (keys) |k| allocator.free(k);
        }
        allocator.free(keys);
    }

    var values = try allocator.alloc(V, num_values);
    errdefer {
        if (V == []const u8) {
            for (values) |v| allocator.free(v);
        }
        allocator.free(values);
    }

    // Second pass: fill in levels and values
    var slot_idx: usize = 0;
    var key_idx: usize = 0;
    var value_idx: usize = 0;

    for (maps) |map_opt| {
        switch (map_opt) {
            .null_value => {
                // Null map: key_def=0, value_def=0, rep=0
                key_def_levels[slot_idx] = 0;
                value_def_levels[slot_idx] = 0;
                rep_levels[slot_idx] = 0;
                slot_idx += 1;
            },
            .value => |entries| {
                if (entries.len == 0) {
                    // Empty map: key_def=1, value_def=1, rep=0
                    key_def_levels[slot_idx] = 1;
                    value_def_levels[slot_idx] = 1;
                    rep_levels[slot_idx] = 0;
                    slot_idx += 1;
                } else {
                    // Map with entries
                    for (entries, 0..) |entry, i| {
                        // rep=0 for first entry, rep=1 for subsequent
                        rep_levels[slot_idx] = if (i == 0) 0 else 1;

                        // Key is always present (REQUIRED)
                        key_def_levels[slot_idx] = 2;
                        if (K == []const u8) {
                            keys[key_idx] = try allocator.dupe(u8, entry.key);
                        } else {
                            keys[key_idx] = entry.key;
                        }
                        key_idx += 1;

                        // Value may be null
                        switch (entry.value) {
                            .null_value => {
                                // Null value: def=2
                                value_def_levels[slot_idx] = 2;
                            },
                            .value => |v| {
                                // Value present: def=max_def
                                value_def_levels[slot_idx] = value_max_def;
                                if (V == []const u8) {
                                    values[value_idx] = try allocator.dupe(u8, v);
                                } else {
                                    values[value_idx] = v;
                                }
                                value_idx += 1;
                            },
                        }
                        slot_idx += 1;
                    }
                }
            },
        }
    }

    return FlattenedMap(K, V){
        .keys = keys,
        .values = values,
        .key_def_levels = key_def_levels,
        .value_def_levels = value_def_levels,
        .rep_levels = rep_levels,
        .num_slots = num_slots,
        .allocator = allocator,
    };
}

// =============================================================================
// Tests
// =============================================================================

test "flattenMap simple" {
    const allocator = std.testing.allocator;

    // Data: [{a: 1, b: 2}, {c: 3}]
    const entries0 = [_]MapEntry([]const u8, i32){
        .{ .key = "a", .value = .{ .value = 1 } },
        .{ .key = "b", .value = .{ .value = 2 } },
    };
    const entries1 = [_]MapEntry([]const u8, i32){
        .{ .key = "c", .value = .{ .value = 3 } },
    };
    const maps = [_]Optional([]const MapEntry([]const u8, i32)){
        .{ .value = &entries0 },
        .{ .value = &entries1 },
    };

    var result = try flattenMap([]const u8, i32, allocator, &maps, 3);
    defer result.deinit();

    // 3 slots total
    try std.testing.expectEqual(@as(usize, 3), result.num_slots);
    try std.testing.expectEqual(@as(usize, 3), result.keys.len);
    try std.testing.expectEqual(@as(usize, 3), result.values.len);

    // Check key def levels: all 2 (key present)
    try std.testing.expectEqualSlices(u32, &[_]u32{ 2, 2, 2 }, result.key_def_levels);

    // Check value def levels: all 3 (value present)
    try std.testing.expectEqualSlices(u32, &[_]u32{ 3, 3, 3 }, result.value_def_levels);

    // Check rep levels: [0, 1, 0]
    try std.testing.expectEqualSlices(u32, &[_]u32{ 0, 1, 0 }, result.rep_levels);

    // Check keys
    try std.testing.expectEqualStrings("a", result.keys[0]);
    try std.testing.expectEqualStrings("b", result.keys[1]);
    try std.testing.expectEqualStrings("c", result.keys[2]);

    // Check values
    try std.testing.expectEqualSlices(i32, &[_]i32{ 1, 2, 3 }, result.values);
}

test "flattenMap with null map" {
    const allocator = std.testing.allocator;

    // Data: [{a: 1}, null, {b: 2}]
    const entries0 = [_]MapEntry([]const u8, i32){
        .{ .key = "a", .value = .{ .value = 1 } },
    };
    const entries2 = [_]MapEntry([]const u8, i32){
        .{ .key = "b", .value = .{ .value = 2 } },
    };
    const maps = [_]Optional([]const MapEntry([]const u8, i32)){
        .{ .value = &entries0 },
        .{ .null_value = {} },
        .{ .value = &entries2 },
    };

    var result = try flattenMap([]const u8, i32, allocator, &maps, 3);
    defer result.deinit();

    // 3 slots total
    try std.testing.expectEqual(@as(usize, 3), result.num_slots);
    try std.testing.expectEqual(@as(usize, 2), result.keys.len);
    try std.testing.expectEqual(@as(usize, 2), result.values.len);

    // Check key def levels: [2, 0, 2]
    try std.testing.expectEqualSlices(u32, &[_]u32{ 2, 0, 2 }, result.key_def_levels);

    // Check value def levels: [3, 0, 3]
    try std.testing.expectEqualSlices(u32, &[_]u32{ 3, 0, 3 }, result.value_def_levels);

    // Check rep levels: [0, 0, 0]
    try std.testing.expectEqualSlices(u32, &[_]u32{ 0, 0, 0 }, result.rep_levels);
}

test "flattenMap with empty map" {
    const allocator = std.testing.allocator;

    // Data: [{a: 1}, {}, {b: 2}]
    const entries0 = [_]MapEntry([]const u8, i32){
        .{ .key = "a", .value = .{ .value = 1 } },
    };
    const entries1 = [_]MapEntry([]const u8, i32){};
    const entries2 = [_]MapEntry([]const u8, i32){
        .{ .key = "b", .value = .{ .value = 2 } },
    };
    const maps = [_]Optional([]const MapEntry([]const u8, i32)){
        .{ .value = &entries0 },
        .{ .value = &entries1 },
        .{ .value = &entries2 },
    };

    var result = try flattenMap([]const u8, i32, allocator, &maps, 3);
    defer result.deinit();

    // 3 slots total
    try std.testing.expectEqual(@as(usize, 3), result.num_slots);

    // Check key def levels: [2, 1, 2] (1 = empty map)
    try std.testing.expectEqualSlices(u32, &[_]u32{ 2, 1, 2 }, result.key_def_levels);

    // Check value def levels: [3, 1, 3] (1 = empty map)
    try std.testing.expectEqualSlices(u32, &[_]u32{ 3, 1, 3 }, result.value_def_levels);

    // Check rep levels: [0, 0, 0]
    try std.testing.expectEqualSlices(u32, &[_]u32{ 0, 0, 0 }, result.rep_levels);
}

test "flattenMap with null value" {
    const allocator = std.testing.allocator;

    // Data: [{a: 1, b: null, c: 3}]
    const entries0 = [_]MapEntry([]const u8, i32){
        .{ .key = "a", .value = .{ .value = 1 } },
        .{ .key = "b", .value = .{ .null_value = {} } },
        .{ .key = "c", .value = .{ .value = 3 } },
    };
    const maps = [_]Optional([]const MapEntry([]const u8, i32)){
        .{ .value = &entries0 },
    };

    var result = try flattenMap([]const u8, i32, allocator, &maps, 3);
    defer result.deinit();

    // 3 slots
    try std.testing.expectEqual(@as(usize, 3), result.num_slots);
    try std.testing.expectEqual(@as(usize, 3), result.keys.len);
    try std.testing.expectEqual(@as(usize, 2), result.values.len); // only 2 non-null values

    // Check key def levels: all 2 (keys always present)
    try std.testing.expectEqualSlices(u32, &[_]u32{ 2, 2, 2 }, result.key_def_levels);

    // Check value def levels: [3, 2, 3] (2 = null value)
    try std.testing.expectEqualSlices(u32, &[_]u32{ 3, 2, 3 }, result.value_def_levels);

    // Check rep levels: [0, 1, 1]
    try std.testing.expectEqualSlices(u32, &[_]u32{ 0, 1, 1 }, result.rep_levels);

    // Check values
    try std.testing.expectEqualSlices(i32, &[_]i32{ 1, 3 }, result.values);
}
