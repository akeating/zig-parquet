//! List value decoder
//!
//! Reconstructs nested list structures from flat Parquet data.
//!
//! Parquet stores lists as flat values with definition and repetition levels:
//! - Definition level: indicates how "deep" into the schema tree the value is defined
//! - Repetition level: indicates which level of the schema is being repeated
//!
//! For a standard 3-level list schema (list_col -> list -> element):
//!   - def=0: list is null
//!   - def=1: list is empty (no elements)
//!   - def=2: element is null (only for nullable elements)
//!   - def=3: element value is present
//!
//!   - rep=0: new record (row)
//!   - rep=1: new element in same list

const std = @import("std");
const types = @import("types.zig");

pub const Optional = types.Optional;

/// Result of list assembly containing the nested structure
pub fn ListColumn(comptime T: type) type {
    return []Optional([]Optional(T));
}

/// Assemble a list column from flat values and levels.
///
/// Parameters:
/// - T: The element type (e.g., i32, []const u8)
/// - allocator: Memory allocator
/// - values: Flat array of non-null values
/// - def_levels: Definition levels for each slot
/// - rep_levels: Repetition levels for each slot
/// - max_def_level: Maximum definition level for this column
/// - max_rep_level: Maximum repetition level for this column
///
/// Returns: Nested slice structure []Optional([]Optional(T))
pub fn assembleList(
    comptime T: type,
    allocator: std.mem.Allocator,
    values: []const T,
    def_levels: []const u32,
    rep_levels: []const u32,
    max_def_level: u8,
    max_rep_level: u8,
) !ListColumn(T) {
    _ = max_rep_level; // Currently only single-level lists supported

    if (def_levels.len == 0) {
        return try allocator.alloc(Optional([]Optional(T)), 0);
    }

    // The definition level thresholds for a 3-level list:
    // For list_col (OPTIONAL) -> list (REPEATED) -> element (OPTIONAL):
    //   - def = 0: list_col is null (null list)
    //   - def = 1: list_col defined, list group empty (empty list)
    //   - def = 2: element is null
    //   - def = 3: element value is present
    //
    // More generally:
    //   - null_list_level = 0: the outermost optional is null
    //   - empty_list_level = 1: list exists but is empty
    //   - elements exist when def >= 2
    // def == 0 means null list (unused as constant, just use 0 directly)
    const element_level: u32 = if (max_def_level >= 2) 2 else 1; // def >= this means element exists
    const value_present_level: u32 = max_def_level;

    // First pass: count rows
    var num_rows: usize = 0;
    for (rep_levels) |rep| {
        if (rep == 0) num_rows += 1;
    }

    // Allocate result
    var result = try allocator.alloc(Optional([]Optional(T)), num_rows);
    errdefer {
        for (result) |row| {
            switch (row) {
                .value => |elements| {
                    for (elements) |elem| {
                        if (T == []const u8) {
                            switch (elem) {
                                .value => |v| allocator.free(v),
                                .null_value => {},
                            }
                        }
                    }
                    allocator.free(elements);
                },
                .null_value => {},
            }
        }
        allocator.free(result);
    }

    // Second pass: count elements per row
    var elements_per_row = try allocator.alloc(usize, num_rows);
    defer allocator.free(elements_per_row);
    @memset(elements_per_row, 0);

    var row_idx: usize = 0;
    var row_is_null: std.ArrayList(bool) = .empty;
    defer row_is_null.deinit(allocator);

    for (0..def_levels.len) |i| {
        const def = def_levels[i];
        const rep = rep_levels[i];

        if (rep == 0) {
            // New row
            if (i > 0) row_idx += 1;

            // Check if this row's list is null (def == 0)
            if (def == 0) {
                // Null list
                try row_is_null.append(allocator, true);
            } else {
                // List exists (might be empty or have elements)
                try row_is_null.append(allocator, false);
                // Count this as an element only if def >= element_level
                if (def >= element_level) {
                    elements_per_row[row_idx] += 1;
                }
                // If def == empty_list_level, list is empty (0 elements, which is default)
            }
        } else {
            // Continuation of current list - this is always an element
            elements_per_row[row_idx] += 1;
        }
    }

    // Allocate elements for each row
    row_idx = 0;
    for (row_is_null.items, 0..) |is_null, i| {
        if (is_null) {
            result[i] = .{ .null_value = {} };
        } else {
            const elements = try allocator.alloc(Optional(T), elements_per_row[i]);
            result[i] = .{ .value = elements };
        }
    }

    // Third pass: fill in values
    row_idx = 0;
    var elem_idx: usize = 0;
    var value_idx: usize = 0;

    for (0..def_levels.len) |i| {
        const def = def_levels[i];
        const rep = rep_levels[i];

        if (rep == 0 and i > 0) {
            // Move to next row
            row_idx += 1;
            elem_idx = 0;
        }

        // Skip null lists and empty list markers
        if (row_is_null.items[row_idx]) {
            continue;
        }

        // Only process if def indicates an element exists
        if (def >= element_level) {
            const elements = result[row_idx].value;
            if (elem_idx < elements.len) {
                if (def >= value_present_level) {
                    // Element value is present
                    if (value_idx < values.len) {
                        if (T == []const u8) {
                            elements[elem_idx] = .{ .value = try allocator.dupe(u8, values[value_idx]) };
                        } else {
                            elements[elem_idx] = .{ .value = values[value_idx] };
                        }
                        value_idx += 1;
                    } else {
                        elements[elem_idx] = .{ .null_value = {} };
                    }
                } else {
                    // Element is null (def >= element_level but < value_present_level)
                    elements[elem_idx] = .{ .null_value = {} };
                }
                elem_idx += 1;
            }
        }
        // If def == empty_list_level, this is just an empty list marker, skip
    }

    return result;
}

/// Free a list column allocated by assembleList
pub fn freeListColumn(comptime T: type, allocator: std.mem.Allocator, list: ListColumn(T)) void {
    for (list) |row| {
        switch (row) {
            .value => |elements| {
                if (T == []const u8) {
                    for (elements) |elem| {
                        switch (elem) {
                            .value => |v| allocator.free(v),
                            .null_value => {},
                        }
                    }
                }
                allocator.free(elements);
            },
            .null_value => {},
        }
    }
    allocator.free(list);
}

// =============================================================================
// Tests
// =============================================================================

test "assembleList simple non-null list" {
    const allocator = std.testing.allocator;

    // Data: [[1, 2, 3], [4, 5]]
    // Flattened: [1, 2, 3, 4, 5]
    // def_levels: [3, 3, 3, 3, 3] (all elements present)
    // rep_levels: [0, 1, 1, 0, 1] (0=new row, 1=continue)
    const values = [_]i32{ 1, 2, 3, 4, 5 };
    const def_levels = [_]u32{ 3, 3, 3, 3, 3 };
    const rep_levels = [_]u32{ 0, 1, 1, 0, 1 };

    const result = try assembleList(i32, allocator, &values, &def_levels, &rep_levels, 3, 1);
    defer freeListColumn(i32, allocator, result);

    try std.testing.expectEqual(@as(usize, 2), result.len);

    // Row 0: [1, 2, 3]
    try std.testing.expect(!result[0].isNull());
    const row0 = result[0].value;
    try std.testing.expectEqual(@as(usize, 3), row0.len);
    try std.testing.expectEqual(@as(i32, 1), row0[0].value);
    try std.testing.expectEqual(@as(i32, 2), row0[1].value);
    try std.testing.expectEqual(@as(i32, 3), row0[2].value);

    // Row 1: [4, 5]
    try std.testing.expect(!result[1].isNull());
    const row1 = result[1].value;
    try std.testing.expectEqual(@as(usize, 2), row1.len);
    try std.testing.expectEqual(@as(i32, 4), row1[0].value);
    try std.testing.expectEqual(@as(i32, 5), row1[1].value);
}

test "assembleList with null list" {
    const allocator = std.testing.allocator;

    // Data: [[1], null, [2]]
    // Flattened: [1, 2]
    // def_levels: [3, 0, 3] (row 1 is null)
    // rep_levels: [0, 0, 0]
    const values = [_]i32{ 1, 2 };
    const def_levels = [_]u32{ 3, 0, 3 };
    const rep_levels = [_]u32{ 0, 0, 0 };

    const result = try assembleList(i32, allocator, &values, &def_levels, &rep_levels, 3, 1);
    defer freeListColumn(i32, allocator, result);

    try std.testing.expectEqual(@as(usize, 3), result.len);

    // Row 0: [1]
    try std.testing.expect(!result[0].isNull());
    try std.testing.expectEqual(@as(usize, 1), result[0].value.len);
    try std.testing.expectEqual(@as(i32, 1), result[0].value[0].value);

    // Row 1: null
    try std.testing.expect(result[1].isNull());

    // Row 2: [2]
    try std.testing.expect(!result[2].isNull());
    try std.testing.expectEqual(@as(usize, 1), result[2].value.len);
    try std.testing.expectEqual(@as(i32, 2), result[2].value[0].value);
}

test "assembleList with empty list" {
    const allocator = std.testing.allocator;

    // Data: [[1], [], [2]]
    // For empty list, def_level = 1 (list defined, but no elements)
    // Flattened values: [1, 2]
    // def_levels: [3, 1, 3]
    // rep_levels: [0, 0, 0]
    const values = [_]i32{ 1, 2 };
    const def_levels = [_]u32{ 3, 1, 3 };
    const rep_levels = [_]u32{ 0, 0, 0 };

    const result = try assembleList(i32, allocator, &values, &def_levels, &rep_levels, 3, 1);
    defer freeListColumn(i32, allocator, result);

    try std.testing.expectEqual(@as(usize, 3), result.len);

    // Row 0: [1]
    try std.testing.expect(!result[0].isNull());
    try std.testing.expectEqual(@as(usize, 1), result[0].value.len);

    // Row 1: [] (empty, not null)
    try std.testing.expect(!result[1].isNull());
    try std.testing.expectEqual(@as(usize, 0), result[1].value.len);

    // Row 2: [2]
    try std.testing.expect(!result[2].isNull());
    try std.testing.expectEqual(@as(usize, 1), result[2].value.len);
}

test "assembleList with null elements" {
    const allocator = std.testing.allocator;

    // Data: [[1, null, 3]]
    // Flattened values: [1, 3]
    // def_levels: [3, 2, 3] (middle element is null)
    // rep_levels: [0, 1, 1]
    const values = [_]i32{ 1, 3 };
    const def_levels = [_]u32{ 3, 2, 3 };
    const rep_levels = [_]u32{ 0, 1, 1 };

    const result = try assembleList(i32, allocator, &values, &def_levels, &rep_levels, 3, 1);
    defer freeListColumn(i32, allocator, result);

    try std.testing.expectEqual(@as(usize, 1), result.len);

    // Row 0: [1, null, 3]
    const row0 = result[0].value;
    try std.testing.expectEqual(@as(usize, 3), row0.len);
    try std.testing.expectEqual(@as(i32, 1), row0[0].value);
    try std.testing.expect(row0[1].isNull());
    try std.testing.expectEqual(@as(i32, 3), row0[2].value);
}
