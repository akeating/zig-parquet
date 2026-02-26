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
    max_rep_level: u8,
) !FlattenedList(T) {
    _ = max_rep_level; // Currently only single-level lists

    // First pass: count total slots and values
    var num_slots: usize = 0;
    var num_values: usize = 0;

    for (lists) |list_opt| {
        switch (list_opt) {
            .null_value => {
                // Null list: 1 slot with def=0
                num_slots += 1;
            },
            .value => |elements| {
                if (elements.len == 0) {
                    // Empty list: 1 slot with def=1
                    num_slots += 1;
                } else {
                    // List with elements: 1 slot per element
                    num_slots += elements.len;
                    for (elements) |elem| {
                        switch (elem) {
                            .value => num_values += 1,
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
    errdefer {
        if (T == []const u8) {
            for (values) |v| allocator.free(v);
        }
        allocator.free(values);
    }

    // Compute def levels dynamically from max_def_level.
    // Schema always has element as OPTIONAL, so:
    //   max_def=2 (required list): empty_list=0, null_elem=1, value=2
    //   max_def=3 (optional list): null_list=0, empty_list=1, null_elem=2, value=3
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

/// Flatten nested list (list<list<T>>) into values + def/rep levels
///
/// For a 5-level nested list schema with optional outer list and required elements:
///   - def=0: outer list is null
///   - def=1: outer list is empty
///   - def=2: inner list is empty (element container exists but inner list is empty)
///   - def=3: element value is present
///
///   - rep=0: new record (row)
///   - rep=1: new inner list (outer element)
///   - rep=2: continue same inner list
pub fn flattenNestedList(
    comptime T: type,
    allocator: std.mem.Allocator,
    outer_lists: []const Optional([]const Optional([]const Optional(T))),
    max_def_level: u8,
    max_rep_level: u8,
) !FlattenedList(T) {
    _ = max_rep_level; // Always 2 for nested lists
    const value_def: u32 = max_def_level;
    const inner_empty_def: u32 = max_def_level - 1;
    const outer_empty_def: u32 = if (max_def_level >= 2) max_def_level - 2 else 0;

    // First pass: count total slots and values
    var num_slots: usize = 0;
    var num_values: usize = 0;

    for (outer_lists) |outer_opt| {
        switch (outer_opt) {
            .null_value => {
                // Outer list null: 1 slot
                num_slots += 1;
            },
            .value => |inner_lists| {
                if (inner_lists.len == 0) {
                    // Outer list empty: 1 slot
                    num_slots += 1;
                } else {
                    for (inner_lists) |inner_opt| {
                        switch (inner_opt) {
                            .null_value => {
                                // Inner list null: 1 slot
                                num_slots += 1;
                            },
                            .value => |elements| {
                                if (elements.len == 0) {
                                    // Inner list empty: 1 slot
                                    num_slots += 1;
                                } else {
                                    // Elements
                                    num_slots += elements.len;
                                    for (elements) |elem| {
                                        switch (elem) {
                                            .value => num_values += 1,
                                            .null_value => {},
                                        }
                                    }
                                }
                            },
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
    errdefer {
        if (T == []const u8) {
            for (values) |v| allocator.free(v);
        }
        allocator.free(values);
    }

    // Second pass: fill in levels and values
    var slot_idx: usize = 0;
    var value_idx: usize = 0;

    for (outer_lists) |outer_opt| {
        switch (outer_opt) {
            .null_value => {
                // Outer list null: def=0, rep=0
                def_levels[slot_idx] = 0;
                rep_levels[slot_idx] = 0;
                slot_idx += 1;
            },
            .value => |inner_lists| {
                if (inner_lists.len == 0) {
                    def_levels[slot_idx] = outer_empty_def;
                    rep_levels[slot_idx] = 0;
                    slot_idx += 1;
                } else {
                    for (inner_lists, 0..) |inner_opt, inner_idx| {
                        // rep=0 for first inner list, rep=1 for subsequent
                        const base_rep: u32 = if (inner_idx == 0) 0 else 1;

                        switch (inner_opt) {
                            .null_value => {
                                def_levels[slot_idx] = inner_empty_def;
                                rep_levels[slot_idx] = base_rep;
                                slot_idx += 1;
                            },
                            .value => |elements| {
                                if (elements.len == 0) {
                                    def_levels[slot_idx] = inner_empty_def;
                                    rep_levels[slot_idx] = base_rep;
                                    slot_idx += 1;
                                } else {
                                    for (elements, 0..) |elem, elem_idx| {
                                        // rep=base_rep for first element, rep=2 for subsequent
                                        rep_levels[slot_idx] = if (elem_idx == 0) base_rep else 2;

                                        switch (elem) {
                                            .null_value => {
                                                def_levels[slot_idx] = inner_empty_def;
                                            },
                                            .value => |v| {
                                                def_levels[slot_idx] = value_def;
                                                if (T == []const u8) {
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

    var result = try flattenList(i32, allocator, &lists, 3, 1);
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

    var result = try flattenList(i32, allocator, &lists, 3, 1);
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

    var result = try flattenList(i32, allocator, &lists, 3, 1);
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

    var result = try flattenList(i32, allocator, &lists, 3, 1);
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
