//! Map value decoder
//!
//! Reconstructs nested map structures from flat Parquet data.
//!
//! Parquet stores maps as two correlated columns (keys and values) with levels:
//! - Definition level: indicates how "deep" into the schema tree the value is defined
//! - Repetition level: indicates which level of the schema is being repeated
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

/// Result of map assembly containing the nested structure
pub fn MapColumn(comptime K: type, comptime V: type) type {
    return []Optional([]MapEntry(K, V));
}

/// Assemble a map column from flat keys, values, and levels.
///
/// Parameters:
/// - K: The key type (e.g., []const u8)
/// - V: The value type (e.g., i32)
/// - allocator: Memory allocator
/// - keys: Flat array of key values (keys are never null)
/// - values: Flat array of non-null value values
/// - key_def_levels: Definition levels for key column
/// - value_def_levels: Definition levels for value column
/// - rep_levels: Repetition levels (same for both columns)
/// - key_max_def: Maximum definition level for key column (typically 2)
/// - value_max_def: Maximum definition level for value column (typically 3)
///
/// Returns: Nested slice structure []Optional([]MapEntry(K, V))
pub fn assembleMap(
    comptime K: type,
    comptime V: type,
    allocator: std.mem.Allocator,
    keys: []const K,
    values: []const V,
    key_def_levels: []const u32,
    value_def_levels: []const u32,
    rep_levels: []const u32,
    key_max_def: u8,
    value_max_def: u8,
) !MapColumn(K, V) {
    _ = key_max_def; // Keys are REQUIRED, so max_def doesn't affect null handling

    if (key_def_levels.len == 0) {
        return try allocator.alloc(Optional([]MapEntry(K, V)), 0);
    }

    // Definition level thresholds:
    // For key column: def >= 2 means entry exists
    // For value column: def >= value_max_def means value is present
    const entry_exists_level: u32 = 2;
    const value_present_level: u32 = value_max_def;

    // First pass: count rows
    var num_rows: usize = 0;
    for (rep_levels) |rep| {
        if (rep == 0) num_rows += 1;
    }

    // Allocate result
    var result = try allocator.alloc(Optional([]MapEntry(K, V)), num_rows);
    errdefer {
        for (result) |row| {
            switch (row) {
                .value => |entries| {
                    if (K == []const u8) {
                        for (entries) |entry| {
                            allocator.free(entry.key);
                        }
                    }
                    if (V == []const u8) {
                        for (entries) |entry| {
                            switch (entry.value) {
                                .value => |v| allocator.free(v),
                                .null_value => {},
                            }
                        }
                    }
                    allocator.free(entries);
                },
                .null_value => {},
            }
        }
        allocator.free(result);
    }

    // Second pass: count entries per row
    var entries_per_row = try allocator.alloc(usize, num_rows);
    defer allocator.free(entries_per_row);
    @memset(entries_per_row, 0);

    var row_idx: usize = 0;
    var row_is_null: std.ArrayList(bool) = .empty;
    defer row_is_null.deinit(allocator);

    for (0..key_def_levels.len) |i| {
        const key_def = key_def_levels[i];
        const rep = rep_levels[i];

        if (rep == 0) {
            // New row
            if (i > 0) row_idx += 1;

            if (key_def == 0) {
                // Null map
                try row_is_null.append(allocator, true);
            } else {
                // Map exists (might be empty or have entries)
                try row_is_null.append(allocator, false);
                // Count this as an entry only if key_def >= entry_exists_level
                if (key_def >= entry_exists_level) {
                    entries_per_row[row_idx] += 1;
                }
            }
        } else {
            // Continuation of current map - this is always an entry
            entries_per_row[row_idx] += 1;
        }
    }

    // Allocate entries for each row
    for (row_is_null.items, 0..) |is_null, i| {
        if (is_null) {
            result[i] = .{ .null_value = {} };
        } else {
            const entries = try allocator.alloc(MapEntry(K, V), entries_per_row[i]);
            result[i] = .{ .value = entries };
        }
    }

    // Third pass: fill in keys and values
    row_idx = 0;
    var entry_idx: usize = 0;
    var key_idx: usize = 0;
    var value_idx: usize = 0;

    for (0..key_def_levels.len) |i| {
        const key_def = key_def_levels[i];
        const value_def = value_def_levels[i];
        const rep = rep_levels[i];

        if (rep == 0 and i > 0) {
            // Move to next row
            row_idx += 1;
            entry_idx = 0;
        }

        // Skip null maps and empty map markers
        if (row_is_null.items[row_idx]) {
            continue;
        }

        // Only process if key_def indicates an entry exists
        if (key_def >= entry_exists_level) {
            const entries = result[row_idx].value;
            if (entry_idx < entries.len) {
                // Key is always present (REQUIRED)
                if (key_idx < keys.len) {
                    if (K == []const u8) {
                        entries[entry_idx].key = try allocator.dupe(u8, keys[key_idx]);
                    } else {
                        entries[entry_idx].key = keys[key_idx];
                    }
                    key_idx += 1;
                }

                // Value may be null
                if (value_def >= value_present_level) {
                    if (value_idx < values.len) {
                        if (V == []const u8) {
                            entries[entry_idx].value = .{ .value = try allocator.dupe(u8, values[value_idx]) };
                        } else {
                            entries[entry_idx].value = .{ .value = values[value_idx] };
                        }
                        value_idx += 1;
                    } else {
                        entries[entry_idx].value = .{ .null_value = {} };
                    }
                } else {
                    // Value is null
                    entries[entry_idx].value = .{ .null_value = {} };
                }

                entry_idx += 1;
            }
        }
    }

    return result;
}

/// Free a map column allocated by assembleMap
pub fn freeMapColumn(comptime K: type, comptime V: type, allocator: std.mem.Allocator, map: MapColumn(K, V)) void {
    for (map) |row| {
        switch (row) {
            .value => |entries| {
                if (K == []const u8) {
                    for (entries) |entry| {
                        allocator.free(entry.key);
                    }
                }
                if (V == []const u8) {
                    for (entries) |entry| {
                        switch (entry.value) {
                            .value => |v| allocator.free(v),
                            .null_value => {},
                        }
                    }
                }
                allocator.free(entries);
            },
            .null_value => {},
        }
    }
    allocator.free(map);
}

// =============================================================================
// Tests
// =============================================================================

test "assembleMap simple map" {
    const allocator = std.testing.allocator;

    // Data: [{a: 1, b: 2}, {c: 3}]
    // Keys: [a, b, c]
    // Values: [1, 2, 3]
    // key_def_levels: [2, 2, 2] (all keys present)
    // value_def_levels: [3, 3, 3] (all values present)
    // rep_levels: [0, 1, 0] (0=new row, 1=continue)
    const keys = [_][]const u8{ "a", "b", "c" };
    const values = [_]i32{ 1, 2, 3 };
    const key_def_levels = [_]u32{ 2, 2, 2 };
    const value_def_levels = [_]u32{ 3, 3, 3 };
    const rep_levels = [_]u32{ 0, 1, 0 };

    const result = try assembleMap([]const u8, i32, allocator, &keys, &values, &key_def_levels, &value_def_levels, &rep_levels, 2, 3);
    defer freeMapColumn([]const u8, i32, allocator, result);

    try std.testing.expectEqual(@as(usize, 2), result.len);

    // Row 0: {a: 1, b: 2}
    try std.testing.expect(!result[0].isNull());
    const row0 = result[0].value;
    try std.testing.expectEqual(@as(usize, 2), row0.len);
    try std.testing.expectEqualStrings("a", row0[0].key);
    try std.testing.expectEqual(@as(i32, 1), row0[0].value.value);
    try std.testing.expectEqualStrings("b", row0[1].key);
    try std.testing.expectEqual(@as(i32, 2), row0[1].value.value);

    // Row 1: {c: 3}
    try std.testing.expect(!result[1].isNull());
    const row1 = result[1].value;
    try std.testing.expectEqual(@as(usize, 1), row1.len);
    try std.testing.expectEqualStrings("c", row1[0].key);
    try std.testing.expectEqual(@as(i32, 3), row1[0].value.value);
}

test "assembleMap with null map" {
    const allocator = std.testing.allocator;

    // Data: [{a: 1}, null, {b: 2}]
    const keys = [_][]const u8{ "a", "b" };
    const values = [_]i32{ 1, 2 };
    const key_def_levels = [_]u32{ 2, 0, 2 };
    const value_def_levels = [_]u32{ 3, 0, 3 };
    const rep_levels = [_]u32{ 0, 0, 0 };

    const result = try assembleMap([]const u8, i32, allocator, &keys, &values, &key_def_levels, &value_def_levels, &rep_levels, 2, 3);
    defer freeMapColumn([]const u8, i32, allocator, result);

    try std.testing.expectEqual(@as(usize, 3), result.len);

    // Row 0: {a: 1}
    try std.testing.expect(!result[0].isNull());
    try std.testing.expectEqual(@as(usize, 1), result[0].value.len);

    // Row 1: null
    try std.testing.expect(result[1].isNull());

    // Row 2: {b: 2}
    try std.testing.expect(!result[2].isNull());
    try std.testing.expectEqual(@as(usize, 1), result[2].value.len);
}

test "assembleMap with empty map" {
    const allocator = std.testing.allocator;

    // Data: [{a: 1}, {}, {b: 2}]
    // Empty map has def=1
    const keys = [_][]const u8{ "a", "b" };
    const values = [_]i32{ 1, 2 };
    const key_def_levels = [_]u32{ 2, 1, 2 };
    const value_def_levels = [_]u32{ 3, 1, 3 };
    const rep_levels = [_]u32{ 0, 0, 0 };

    const result = try assembleMap([]const u8, i32, allocator, &keys, &values, &key_def_levels, &value_def_levels, &rep_levels, 2, 3);
    defer freeMapColumn([]const u8, i32, allocator, result);

    try std.testing.expectEqual(@as(usize, 3), result.len);

    // Row 0: {a: 1}
    try std.testing.expect(!result[0].isNull());
    try std.testing.expectEqual(@as(usize, 1), result[0].value.len);

    // Row 1: {} (empty, not null)
    try std.testing.expect(!result[1].isNull());
    try std.testing.expectEqual(@as(usize, 0), result[1].value.len);

    // Row 2: {b: 2}
    try std.testing.expect(!result[2].isNull());
    try std.testing.expectEqual(@as(usize, 1), result[2].value.len);
}

test "assembleMap with null value" {
    const allocator = std.testing.allocator;

    // Data: [{a: 1, b: null, c: 3}]
    // Value at b is null (def=2 for value column)
    const keys = [_][]const u8{ "a", "b", "c" };
    const values = [_]i32{ 1, 3 }; // only non-null values
    const key_def_levels = [_]u32{ 2, 2, 2 };
    const value_def_levels = [_]u32{ 3, 2, 3 }; // middle value is null
    const rep_levels = [_]u32{ 0, 1, 1 };

    const result = try assembleMap([]const u8, i32, allocator, &keys, &values, &key_def_levels, &value_def_levels, &rep_levels, 2, 3);
    defer freeMapColumn([]const u8, i32, allocator, result);

    try std.testing.expectEqual(@as(usize, 1), result.len);

    // Row 0: {a: 1, b: null, c: 3}
    const row0 = result[0].value;
    try std.testing.expectEqual(@as(usize, 3), row0.len);

    try std.testing.expectEqualStrings("a", row0[0].key);
    try std.testing.expectEqual(@as(i32, 1), row0[0].value.value);

    try std.testing.expectEqualStrings("b", row0[1].key);
    try std.testing.expect(row0[1].value.isNull());

    try std.testing.expectEqualStrings("c", row0[2].key);
    try std.testing.expectEqual(@as(i32, 3), row0[2].value.value);
}
