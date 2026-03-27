//! List Encoder
//!
//! Flattens nested list structures into flat values with definition/repetition levels.
//!
//! For a 3-level list schema (list_col -> list -> element):
//!   - def=0: list is null
//!   - def=1: list is empty (no elements)
//!   - def=2: element is null
//!   - def=3: element value is present
//!
//!   - rep=0: new record (row)
//!   - rep=1: new element in same list

const std = @import("std");
const types = @import("types.zig");

pub const Optional = types.Optional;

/// Result of flattening a list column
pub fn FlattenedList(comptime T: type) type {
    return struct {
        /// Flat array of non-null values
        values: []T,
        /// Definition levels for each slot
        def_levels: []u32,
        /// Repetition levels for each slot
        rep_levels: []u32,
        /// Total number of slots (length of def_levels and rep_levels)
        num_slots: usize,

        allocator: std.mem.Allocator,

        const Self = @This();

        pub fn deinit(self: *Self) void {
            if (T == []const u8) {
                for (self.values) |v| self.allocator.free(v);
            }
            self.allocator.free(self.values);
            self.allocator.free(self.def_levels);
            self.allocator.free(self.rep_levels);
        }
    };
}

/// Flatten a list column into values and levels.
///
/// Takes nested slices []Optional([]Optional(T)) and produces:
/// - Flat array of non-null values
/// - Definition levels for each slot (including nulls and empty list markers)
/// - Repetition levels for each slot
///
/// For max_def_level=3 (optional list of optional elements):
///   - def=0: list is null
///   - def=1: list is empty
///   - def=2: element is null
///   - def=3: element value present
pub fn flattenList(
    comptime T: type,
    allocator: std.mem.Allocator,
    lists: []const Optional([]const Optional(T)),
    max_def_level: u8,
) !FlattenedList(T) {

    if (max_def_level < 2) return error.OutOfMemory;

    // First pass: count total slots and values (checked arithmetic)
    var num_slots: usize = 0;
    var num_values: usize = 0;

    for (lists) |list_opt| {
        switch (list_opt) {
            .null_value => {
                num_slots = std.math.add(usize, num_slots, 1) catch return error.OutOfMemory;
            },
            .value => |elements| {
                if (elements.len == 0) {
                    num_slots = std.math.add(usize, num_slots, 1) catch return error.OutOfMemory;
                } else {
                    num_slots = std.math.add(usize, num_slots, elements.len) catch return error.OutOfMemory;
                    for (elements) |elem| {
                        switch (elem) {
                            .value => {
                                num_values = std.math.add(usize, num_values, 1) catch return error.OutOfMemory;
                            },
                            .null_value => {},
                        }
                    }
                }
            },
        }
    }

    // Allocate result arrays
    var def_levels = try allocator.alloc(u32, num_slots);
    errdefer allocator.free(def_levels);

    var rep_levels = try allocator.alloc(u32, num_slots);
    errdefer allocator.free(rep_levels);

    var values = try allocator.alloc(T, num_values);
    var values_initialized: usize = 0;
    errdefer {
        if (T == []const u8) {
            for (values[0..values_initialized]) |v| allocator.free(v);
        }
        allocator.free(values);
    }

    const value_def: u32 = max_def_level;
    const null_elem_def: u32 = max_def_level - 1;
    const empty_list_def: u32 = max_def_level - 2;

    // Second pass: fill in levels and values
    var slot_idx: usize = 0;
    var value_idx: usize = 0;

    for (lists) |list_opt| {
        switch (list_opt) {
            .null_value => {
                def_levels[slot_idx] = 0;
                rep_levels[slot_idx] = 0;
                slot_idx += 1;
            },
            .value => |elements| {
                if (elements.len == 0) {
                    def_levels[slot_idx] = empty_list_def;
                    rep_levels[slot_idx] = 0;
                    slot_idx += 1;
                } else {
                    for (elements, 0..) |elem, i| {
                        rep_levels[slot_idx] = if (i == 0) 0 else 1;

                        switch (elem) {
                            .null_value => {
                                def_levels[slot_idx] = null_elem_def;
                            },
                            .value => |v| {
                                def_levels[slot_idx] = value_def;
                                if (T == []const u8) {
                                    values[value_idx] = try allocator.dupe(u8, v);
                                    values_initialized += 1;
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

    return FlattenedList(T){
        .values = values,
        .def_levels = def_levels,
        .rep_levels = rep_levels,
        .num_slots = num_slots,
        .allocator = allocator,
    };
}

// =============================================================================
// Tests
// =============================================================================

test "flattenList simple non-null" {
    const allocator = std.testing.allocator;

    // Data: [[1, 2, 3], [4, 5]]
    const inner0 = [_]Optional(i32){ .{ .value = 1 }, .{ .value = 2 }, .{ .value = 3 } };
    const inner1 = [_]Optional(i32){ .{ .value = 4 }, .{ .value = 5 } };
    const lists = [_]Optional([]const Optional(i32)){
        .{ .value = &inner0 },
        .{ .value = &inner1 },
    };

    var result = try flattenList(i32, allocator, &lists, 3);
    defer result.deinit();

    // 5 slots total
    try std.testing.expectEqual(@as(usize, 5), result.num_slots);
    try std.testing.expectEqual(@as(usize, 5), result.values.len);

    // Check def levels: all 3 (value present)
    try std.testing.expectEqualSlices(u32, &[_]u32{ 3, 3, 3, 3, 3 }, result.def_levels);

    // Check rep levels: [0, 1, 1, 0, 1]
    try std.testing.expectEqualSlices(u32, &[_]u32{ 0, 1, 1, 0, 1 }, result.rep_levels);

    // Check values
    try std.testing.expectEqualSlices(i32, &[_]i32{ 1, 2, 3, 4, 5 }, result.values);
}

test "flattenList with null list" {
    const allocator = std.testing.allocator;

    // Data: [[1], null, [2]]
    const inner0 = [_]Optional(i32){.{ .value = 1 }};
    const inner2 = [_]Optional(i32){.{ .value = 2 }};
    const lists = [_]Optional([]const Optional(i32)){
        .{ .value = &inner0 },
        .{ .null_value = {} },
        .{ .value = &inner2 },
    };

    var result = try flattenList(i32, allocator, &lists, 3);
    defer result.deinit();

    // 3 slots total
    try std.testing.expectEqual(@as(usize, 3), result.num_slots);
    try std.testing.expectEqual(@as(usize, 2), result.values.len);

    // Check def levels: [3, 0, 3]
    try std.testing.expectEqualSlices(u32, &[_]u32{ 3, 0, 3 }, result.def_levels);

    // Check rep levels: [0, 0, 0]
    try std.testing.expectEqualSlices(u32, &[_]u32{ 0, 0, 0 }, result.rep_levels);
}

test "flattenList with empty list" {
    const allocator = std.testing.allocator;

    // Data: [[1], [], [2]]
    const inner0 = [_]Optional(i32){.{ .value = 1 }};
    const inner1 = [_]Optional(i32){};
    const inner2 = [_]Optional(i32){.{ .value = 2 }};
    const lists = [_]Optional([]const Optional(i32)){
        .{ .value = &inner0 },
        .{ .value = &inner1 },
        .{ .value = &inner2 },
    };

    var result = try flattenList(i32, allocator, &lists, 3);
    defer result.deinit();

    // 3 slots total
    try std.testing.expectEqual(@as(usize, 3), result.num_slots);
    try std.testing.expectEqual(@as(usize, 2), result.values.len);

    // Check def levels: [3, 1, 3] (1 = empty list)
    try std.testing.expectEqualSlices(u32, &[_]u32{ 3, 1, 3 }, result.def_levels);

    // Check rep levels: [0, 0, 0]
    try std.testing.expectEqualSlices(u32, &[_]u32{ 0, 0, 0 }, result.rep_levels);
}

test "flattenList with null elements" {
    const allocator = std.testing.allocator;

    // Data: [[1, null, 3]]
    const inner0 = [_]Optional(i32){ .{ .value = 1 }, .{ .null_value = {} }, .{ .value = 3 } };
    const lists = [_]Optional([]const Optional(i32)){
        .{ .value = &inner0 },
    };

    var result = try flattenList(i32, allocator, &lists, 3);
    defer result.deinit();

    // 3 slots
    try std.testing.expectEqual(@as(usize, 3), result.num_slots);
    try std.testing.expectEqual(@as(usize, 2), result.values.len);

    // Check def levels: [3, 2, 3] (2 = null element)
    try std.testing.expectEqualSlices(u32, &[_]u32{ 3, 2, 3 }, result.def_levels);

    // Check rep levels: [0, 1, 1]
    try std.testing.expectEqualSlices(u32, &[_]u32{ 0, 1, 1 }, result.rep_levels);

    // Check values
    try std.testing.expectEqualSlices(i32, &[_]i32{ 1, 3 }, result.values);
}

test "flattenList with string values" {
    const allocator = std.testing.allocator;

    const inner0 = [_]Optional([]const u8){
        .{ .value = "hello" },
        .{ .value = "world" },
    };
    const inner1 = [_]Optional([]const u8){
        .{ .value = "foo" },
        .null_value,
    };

    const lists = [_]Optional([]const Optional([]const u8)){
        .{ .value = &inner0 },
        .{ .value = &inner1 },
    };

    var result = try flattenList([]const u8, allocator, &lists, 3);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 4), result.num_slots);
    try std.testing.expectEqual(@as(usize, 3), result.values.len);

    try std.testing.expectEqualStrings("hello", result.values[0]);
    try std.testing.expectEqualStrings("world", result.values[1]);
    try std.testing.expectEqualStrings("foo", result.values[2]);
}
