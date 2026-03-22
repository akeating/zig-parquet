//! Schema tree utilities
//!
//! Functions for traversing Parquet schemas and computing definition/repetition levels.
//!
//! In Parquet, nested types use definition and repetition levels:
//! - Definition level: how many optional/repeated fields in the path are defined
//! - Repetition level: which repeated field in the path is being repeated
//!
//! Rules:
//! - max_definition_level += 1 for each OPTIONAL or REPEATED field in path
//! - max_repetition_level += 1 for each REPEATED field in path

const std = @import("std");
const safe = @import("../safe.zig");
const schema_mod = @import("schema.zig");
const types = @import("types.zig");

pub const SchemaElement = schema_mod.SchemaElement;
pub const RepetitionType = types.RepetitionType;

/// Information about a leaf column including its path and level info
pub const ColumnInfo = struct {
    /// The leaf schema element
    element: SchemaElement,
    /// Maximum definition level for this column
    max_def_level: u8,
    /// Maximum repetition level for this column
    max_rep_level: u8,
    /// Column index (0-based leaf index)
    column_index: usize,
};

/// Compute the bit width needed to encode values up to max_level
pub fn computeBitWidth(max_level: u8) u5 {
    if (max_level == 0) return 0;
    // Number of bits needed = floor(log2(max_level)) + 1
    // log2_int(max_level) for max_level=255 is 7. +1 is 8. Safely fits in u5.
    return safe.castTo(u5, std.math.log2_int(u8, max_level) + 1) catch unreachable; // max_level is u8, so log2 + 1 <= 8, fits u5
}

/// Get information about a leaf column by its index.
///
/// The schema is a flat array representing a tree via num_children.
/// We traverse depth-first to find the nth leaf column.
pub fn getColumnInfo(schema: []const SchemaElement, column_index: usize) ?ColumnInfo {
    if (schema.len == 0) return null;

    var state = TraversalState{
        .schema = schema,
        .target_leaf_index = column_index,
    };

    // Start from root (index 0), which is typically the schema root group
    // The root usually has no repetition type and just contains the top-level fields
    if (schema[0].num_children) |num_children| {
        // Root is a group, traverse its children
        _ = traverseChildren(&state, 1, safe.castTo(usize, num_children) catch return null, 0, 0);
    } else {
        // Schema has only one element which is the leaf
        if (column_index == 0) {
            return ColumnInfo{
                .element = schema[0],
                .max_def_level = levelContribution(schema[0].repetition_type, true),
                .max_rep_level = levelContribution(schema[0].repetition_type, false),
                .column_index = 0,
            };
        }
    }

    return state.result;
}

/// Compute max definition level for a column by its index
pub fn maxDefinitionLevel(schema: []const SchemaElement, column_index: usize) u8 {
    if (getColumnInfo(schema, column_index)) |info| {
        return info.max_def_level;
    }
    return 1; // Default for simple optional column
}

/// Compute max repetition level for a column by its index
pub fn maxRepetitionLevel(schema: []const SchemaElement, column_index: usize) u8 {
    if (getColumnInfo(schema, column_index)) |info| {
        return info.max_rep_level;
    }
    return 0; // Default for non-repeated column
}

/// Check if a schema element is a leaf (has a physical type, not a group)
pub fn isLeaf(elem: SchemaElement) bool {
    return elem.type_ != null and elem.num_children == null;
}

/// Check if a schema element is a group (has children)
pub fn isGroup(elem: SchemaElement) bool {
    return elem.num_children != null;
}

// Internal traversal state
const TraversalState = struct {
    schema: []const SchemaElement,
    target_leaf_index: usize,
    current_leaf_index: usize = 0,
    result: ?ColumnInfo = null,
};

/// Traverse children of a group node, accumulating levels
fn traverseChildren(
    state: *TraversalState,
    start_idx: usize,
    num_children: usize,
    current_def_level: u8,
    current_rep_level: u8,
) usize {
    var idx = start_idx;
    var children_processed: usize = 0;

    while (children_processed < num_children and idx < state.schema.len) {
        if (state.result != null) return idx; // Already found

        const elem = state.schema[idx];

        // Add level contribution from this element
        const def_level = current_def_level + levelContribution(elem.repetition_type, true);
        const rep_level = current_rep_level + levelContribution(elem.repetition_type, false);

        if (isLeaf(elem)) {
            // This is a leaf column
            if (state.current_leaf_index == state.target_leaf_index) {
                state.result = ColumnInfo{
                    .element = elem,
                    .max_def_level = def_level,
                    .max_rep_level = rep_level,
                    .column_index = state.current_leaf_index,
                };
            }
            state.current_leaf_index += 1;
            idx += 1;
        } else if (elem.num_children) |nc| {
            // This is a group, recurse into its children
            idx = traverseChildren(state, idx + 1, safe.castTo(usize, nc) catch 0, def_level, rep_level);
        } else {
            idx += 1;
        }

        children_processed += 1;
    }

    return idx;
}

/// Get the level contribution from a repetition type
fn levelContribution(rep_type: ?RepetitionType, for_definition: bool) u8 {
    const rt = rep_type orelse return 0;
    return switch (rt) {
        .required => 0,
        .optional => if (for_definition) 1 else 0,
        .repeated => 1, // Repeated contributes to both def and rep levels
    };
}

// =============================================================================
// Tests
// =============================================================================

test "computeBitWidth" {
    try std.testing.expectEqual(@as(u5, 0), computeBitWidth(0));
    try std.testing.expectEqual(@as(u5, 1), computeBitWidth(1));
    try std.testing.expectEqual(@as(u5, 2), computeBitWidth(2));
    try std.testing.expectEqual(@as(u5, 2), computeBitWidth(3));
    try std.testing.expectEqual(@as(u5, 3), computeBitWidth(4));
    try std.testing.expectEqual(@as(u5, 3), computeBitWidth(7));
    try std.testing.expectEqual(@as(u5, 4), computeBitWidth(8));
    try std.testing.expectEqual(@as(u5, 4), computeBitWidth(15));
}

test "flat schema with required column" {
    // Schema: root -> col1 (required int32)
    const schema = [_]SchemaElement{
        .{ .name = "root", .num_children = 1 },
        .{ .name = "col1", .type_ = .int32, .repetition_type = .required },
    };

    const info = getColumnInfo(&schema, 0);
    try std.testing.expect(info != null);
    try std.testing.expectEqual(@as(u8, 0), info.?.max_def_level);
    try std.testing.expectEqual(@as(u8, 0), info.?.max_rep_level);
}

test "flat schema with optional column" {
    // Schema: root -> col1 (optional int32)
    const schema = [_]SchemaElement{
        .{ .name = "root", .num_children = 1 },
        .{ .name = "col1", .type_ = .int32, .repetition_type = .optional },
    };

    const info = getColumnInfo(&schema, 0);
    try std.testing.expect(info != null);
    try std.testing.expectEqual(@as(u8, 1), info.?.max_def_level);
    try std.testing.expectEqual(@as(u8, 0), info.?.max_rep_level);
}

test "schema with repeated field (list)" {
    // Schema: root -> list (repeated) -> element (required int32)
    // This represents a non-nullable list of non-nullable ints
    const schema = [_]SchemaElement{
        .{ .name = "root", .num_children = 1 },
        .{ .name = "list", .num_children = 1, .repetition_type = .repeated },
        .{ .name = "element", .type_ = .int32, .repetition_type = .required },
    };

    const info = getColumnInfo(&schema, 0);
    try std.testing.expect(info != null);
    try std.testing.expectEqual(@as(u8, 1), info.?.max_def_level); // repeated adds 1
    try std.testing.expectEqual(@as(u8, 1), info.?.max_rep_level); // repeated adds 1
}

test "schema with nullable list of nullable elements" {
    // Schema: root -> list (optional) -> element (optional int32)
    // This represents a nullable list of nullable ints
    // Actually in Parquet this would be:
    // root -> my_list (optional, group) -> list (repeated, group) -> element (optional)
    // But simplified for test:
    const schema = [_]SchemaElement{
        .{ .name = "root", .num_children = 1 },
        .{ .name = "my_list", .num_children = 1, .repetition_type = .optional },
        .{ .name = "list", .num_children = 1, .repetition_type = .repeated },
        .{ .name = "element", .type_ = .int32, .repetition_type = .optional },
    };

    const info = getColumnInfo(&schema, 0);
    try std.testing.expect(info != null);
    // optional(1) + repeated(1) + optional(1) = 3
    try std.testing.expectEqual(@as(u8, 3), info.?.max_def_level);
    // repeated(1) = 1
    try std.testing.expectEqual(@as(u8, 1), info.?.max_rep_level);
}

test "multiple columns" {
    // Schema: root -> col1 (optional), col2 (required), col3 (optional)
    const schema = [_]SchemaElement{
        .{ .name = "root", .num_children = 3 },
        .{ .name = "col1", .type_ = .int32, .repetition_type = .optional },
        .{ .name = "col2", .type_ = .int64, .repetition_type = .required },
        .{ .name = "col3", .type_ = .double, .repetition_type = .optional },
    };

    const info0 = getColumnInfo(&schema, 0);
    try std.testing.expect(info0 != null);
    try std.testing.expectEqual(@as(u8, 1), info0.?.max_def_level);
    try std.testing.expectEqualStrings("col1", info0.?.element.name);

    const info1 = getColumnInfo(&schema, 1);
    try std.testing.expect(info1 != null);
    try std.testing.expectEqual(@as(u8, 0), info1.?.max_def_level);
    try std.testing.expectEqualStrings("col2", info1.?.element.name);

    const info2 = getColumnInfo(&schema, 2);
    try std.testing.expect(info2 != null);
    try std.testing.expectEqual(@as(u8, 1), info2.?.max_def_level);
    try std.testing.expectEqualStrings("col3", info2.?.element.name);

    // Out of bounds
    const info3 = getColumnInfo(&schema, 3);
    try std.testing.expect(info3 == null);
}
