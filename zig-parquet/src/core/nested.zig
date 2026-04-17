//! Recursive flattening and assembly for nested Parquet data
//!
//! This module provides generalized functions to:
//! - Flatten nested `Value` structures into flat column arrays with def/rep levels
//! - Assemble flat column data back into nested `Value` structures
//!
//! These functions handle arbitrary nesting depth (list<struct<list<...>>>).

const std = @import("std");
const schema_mod = @import("schema.zig");
const value_mod = @import("value.zig");

const SchemaNode = schema_mod.SchemaNode;
const Value = value_mod.Value;

const max_nesting_depth: u32 = 64;

/// Represents a single leaf column's flattened data
pub const FlatColumn = struct {
    values: std.ArrayList(Value),
    def_levels: std.ArrayList(u32),
    rep_levels: std.ArrayList(u32),

    pub fn init() FlatColumn {
        return .{
            .values = .empty,
            .def_levels = .empty,
            .rep_levels = .empty,
        };
    }

    pub fn deinit(self: *FlatColumn, allocator: std.mem.Allocator) void {
        self.values.deinit(allocator);
        self.def_levels.deinit(allocator);
        self.rep_levels.deinit(allocator);
    }
};

/// Collection of flattened columns for a schema
pub const FlatColumns = struct {
    columns: std.ArrayList(FlatColumn),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) FlatColumns {
        return .{
            .columns = .empty,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *FlatColumns) void {
        for (self.columns.items) |*col| {
            col.deinit(self.allocator);
        }
        self.columns.deinit(self.allocator);
    }

    pub fn addColumn(self: *FlatColumns) !*FlatColumn {
        try self.columns.append(self.allocator, FlatColumn.init());
        return &self.columns.items[self.columns.items.len - 1];
    }
};

/// Context for tracking levels during flattening
const FlattenContext = struct {
    allocator: std.mem.Allocator,
    columns: *FlatColumns,
    current_def: u32,
    current_rep: u32,
    rep_depth: u32, // tracks which repetition level we're at
    column_index: usize,
};

/// Flatten a nested Value according to its schema into flat columns with levels.
///
/// This is the inverse of `assembleValue` - it takes a nested structure and
/// produces flat arrays suitable for Parquet column encoding.
pub fn flattenValue(
    allocator: std.mem.Allocator,
    node: *const SchemaNode,
    value: Value,
) !FlatColumns {
    var columns = FlatColumns.init(allocator);
    errdefer columns.deinit();

    // Pre-allocate columns for all leaves
    const num_leaves = node.countLeafColumns();
    for (0..num_leaves) |_| {
        _ = try columns.addColumn();
    }

    var ctx = FlattenContext{
        .allocator = allocator,
        .columns = &columns,
        .current_def = 0,
        .current_rep = 0,
        .rep_depth = 0,
        .column_index = 0,
    };

    try flattenRecursive(node, value, &ctx, false, 0);

    return columns;
}

fn flattenRecursive(
    node: *const SchemaNode,
    value: Value,
    ctx: *FlattenContext,
    is_repeated: bool,
    depth: u32,
) !void {
    if (depth > max_nesting_depth) return error.NestingTooDeep;
    switch (node.*) {
        .optional => |child| {
            if (value.isNull()) {
                // Write null marker to all leaf columns under this optional
                try writeNullToLeaves(child, ctx);
            } else {
                ctx.current_def += 1;
                try flattenRecursive(child, value, ctx, is_repeated, depth + 1);
                ctx.current_def -= 1;
            }
        },
        .list => |element| {
            // Parquet list encoding with definition and repetition levels:
            // Definition levels:
            //   def=0: list is null
            //   def=1: list exists but is empty
            //   def>=2: element exists (higher values for nested optional fields)
            // Repetition levels:
            //   rep=0: first element of a new record (new row)
            //   rep=1: subsequent element in the same list (repeated at list level)
            //   rep=2+: for nested lists, repeating at inner level
            const items = value.asList();
            if (items == null or value.isNull()) {
                // Null list: def=0, rep unchanged (uses current_rep from context)
                try writeNullToLeaves(element, ctx);
            } else if (items.?.len == 0) {
                // Empty list: def stays at current level (repeated group has no instances)
                try writeNullToLeaves(element, ctx);
            } else {
                ctx.current_def += 1;
                ctx.rep_depth += 1;
                for (items.?, 0..) |item, i| {
                    if (i > 0) {
                        // Subsequent elements: rep=rep_depth (indicates repetition at this list level)
                        ctx.current_rep = ctx.rep_depth;
                    }
                    try flattenRecursive(element, item, ctx, true, depth + 1);
                }
                ctx.rep_depth -= 1;
                ctx.current_def -= 1;
                if (!is_repeated) {
                    // Reset rep to 0 after processing list (ready for next top-level record)
                    ctx.current_rep = 0;
                }
            }
        },
        .map => |m| {
            // Map levels: key_value repeated group adds +1 def, +1 rep.
            // Parquet schema always marks map values as OPTIONAL, so add +1 def for value
            // unless the node already has an .optional wrapper.
            const entries = value.asMap();
            const key_leaves = m.key.countLeafColumns();
            const value_extra_def: u32 = if (m.value.* == .optional) 0 else 1;

            if (entries == null or value.isNull()) {
                try writeNullToLeaves(m.key, ctx);
                ctx.column_index += key_leaves;
                try writeNullToLeaves(m.value, ctx);
                ctx.column_index -= key_leaves;
            } else if (entries.?.len == 0) {
                try writeNullToLeaves(m.key, ctx);
                ctx.column_index += key_leaves;
                try writeNullToLeaves(m.value, ctx);
                ctx.column_index -= key_leaves;
            } else {
                ctx.current_def += 1;
                ctx.rep_depth += 1;
                for (entries.?, 0..) |entry, i| {
                    if (i > 0) {
                        ctx.current_rep = ctx.rep_depth;
                    }
                    try flattenRecursive(m.key, entry.key, ctx, true, depth + 1);
                    ctx.column_index += key_leaves;
                    ctx.current_def += value_extra_def;
                    try flattenRecursive(m.value, entry.value, ctx, true, depth + 1);
                    ctx.current_def -= value_extra_def;
                    ctx.column_index -= key_leaves;
                }
                ctx.rep_depth -= 1;
                ctx.current_def -= 1;
                if (!is_repeated) {
                    ctx.current_rep = 0;
                }
            }
        },
        .struct_ => |s| {
            const fields = value.asStruct();
            if (fields == null or value.isNull()) {
                // Null struct - write nulls to all field columns
                for (s.fields) |f| {
                    try writeNullToLeaves(f.node, ctx);
                    ctx.column_index += f.node.countLeafColumns();
                }
                // Reset column index
                for (s.fields) |f| {
                    ctx.column_index -= f.node.countLeafColumns();
                }
            } else {
                // Match fields by name
                for (s.fields) |schema_field| {
                    var found = false;
                    for (fields.?) |value_field| {
                        if (std.mem.eql(u8, schema_field.name, value_field.name)) {
                            try flattenRecursive(schema_field.node, value_field.value, ctx, is_repeated, depth + 1);
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        // Field not present, write null
                        try writeNullToLeaves(schema_field.node, ctx);
                    }
                    ctx.column_index += schema_field.node.countLeafColumns();
                }
                // Reset column index
                for (s.fields) |f| {
                    ctx.column_index -= f.node.countLeafColumns();
                }
            }
        },
        // Primitive types - write to column
        .boolean, .int32, .int64, .float, .double, .byte_array, .fixed_len_byte_array => {
            const col = &ctx.columns.columns.items[ctx.column_index];
            try col.values.append(ctx.allocator, value);
            try col.def_levels.append(ctx.allocator, ctx.current_def);
            try col.rep_levels.append(ctx.allocator, ctx.current_rep);
        },
    }
}

fn writeNullToLeaves(node: *const SchemaNode, ctx: *FlattenContext) !void {
    switch (node.*) {
        .optional => |child| try writeNullToLeaves(child, ctx),
        .list => |element| try writeNullToLeaves(element, ctx),
        .map => |m| {
            try writeNullToLeaves(m.key, ctx);
            ctx.column_index += m.key.countLeafColumns();
            try writeNullToLeaves(m.value, ctx);
            ctx.column_index -= m.key.countLeafColumns();
        },
        .struct_ => |s| {
            for (s.fields) |f| {
                try writeNullToLeaves(f.node, ctx);
                ctx.column_index += f.node.countLeafColumns();
            }
            for (s.fields) |f| {
                ctx.column_index -= f.node.countLeafColumns();
            }
        },
        .boolean, .int32, .int64, .float, .double, .byte_array, .fixed_len_byte_array => {
            const col = &ctx.columns.columns.items[ctx.column_index];
            try col.values.append(ctx.allocator, .{ .null_val = {} });
            try col.def_levels.append(ctx.allocator, ctx.current_def);
            try col.rep_levels.append(ctx.allocator, ctx.current_rep);
        },
    }
}

/// Context for tracking position during assembly
const AssembleContext = struct {
    allocator: std.mem.Allocator,
    columns: []const FlatColumnSlice,
    column_index: usize,
    positions: []usize, // current position in each column
};

/// A view into a flat column's data
pub const FlatColumnSlice = struct {
    values: []const Value,
    def_levels: []const u32,
    rep_levels: []const u32,
};

/// Assemble a nested Value from flat columns according to schema.
///
/// This is the inverse of `flattenValue` - it takes flat column data with
/// definition and repetition levels and reconstructs the nested structure.
fn assembleValue(
    allocator: std.mem.Allocator,
    node: *const SchemaNode,
    columns: []const FlatColumnSlice,
) !Value {
    const num_leaves = node.countLeafColumns();
    if (columns.len < num_leaves) {
        return error.NotEnoughColumns;
    }

    const positions = try allocator.alloc(usize, columns.len);
    defer allocator.free(positions);
    @memset(positions, 0);

    var ctx = AssembleContext{
        .allocator = allocator,
        .columns = columns,
        .column_index = 0,
        .positions = positions,
    };

    return try assembleRecursive(node, &ctx, 0, 0, 0);
}

/// Assemble multiple rows of nested Values from flat columns.
///
/// Like `assembleValue` but processes `num_rows` consecutive rows, keeping
/// position state across rows so each row picks up where the previous left off.
pub fn assembleValues(
    allocator: std.mem.Allocator,
    node: *const SchemaNode,
    columns: []const FlatColumnSlice,
    num_rows: usize,
) ![]Value {
    const num_leaves = node.countLeafColumns();
    if (columns.len < num_leaves) {
        return error.NotEnoughColumns;
    }

    const positions = try allocator.alloc(usize, columns.len);
    defer allocator.free(positions);
    @memset(positions, 0);

    var ctx = AssembleContext{
        .allocator = allocator,
        .columns = columns,
        .column_index = 0,
        .positions = positions,
    };

    const result = try allocator.alloc(Value, num_rows);
    var initialized: usize = 0;
    errdefer {
        for (result[0..initialized]) |v| v.deinit(allocator);
        allocator.free(result);
    }

    for (0..num_rows) |i| {
        ctx.column_index = 0;
        result[i] = try assembleRecursive(node, &ctx, 0, 0, 0);
        initialized += 1;
    }
    return result;
}

fn assembleRecursive(
    node: *const SchemaNode,
    ctx: *AssembleContext,
    def_threshold: u32,
    rep_threshold: u32,
    depth: u32,
) !Value {
    if (depth > max_nesting_depth) return error.NestingTooDeep;
    switch (node.*) {
        .optional => |child| {
            // Check if we have data
            const col = ctx.columns[ctx.column_index];
            const pos = ctx.positions[ctx.column_index];
            if (pos >= col.def_levels.len) {
                return .{ .null_val = {} };
            }

            const def = col.def_levels[pos];
            if (def <= def_threshold) {
                // Null at this level - consume the null
                consumeNullFromLeaves(child, ctx);
                return .{ .null_val = {} };
            }

            return try assembleRecursive(child, ctx, def_threshold + 1, rep_threshold, depth + 1);
        },
        .list => |element| {
            const col = ctx.columns[ctx.column_index];
            const pos = ctx.positions[ctx.column_index];
            if (pos >= col.def_levels.len) {
                return .{ .null_val = {} };
            }

            const def = col.def_levels[pos];
            if (def < def_threshold) {
                // Null list (for required lists this never triggers; for optional
                // lists the .optional wrapper handles null before reaching here)
                consumeNullFromLeaves(element, ctx);
                return .{ .null_val = {} };
            }

            // Assemble list elements
            var items: std.ArrayList(Value) = .empty;
            errdefer {
                for (items.items) |item| item.deinit(ctx.allocator);
                items.deinit(ctx.allocator);
            }

            var first = true;
            while (true) {
                const current_pos = ctx.positions[ctx.column_index];
                if (current_pos >= col.def_levels.len) break;

                const current_rep = col.rep_levels[current_pos];
                if (!first and current_rep <= rep_threshold) break;

                const current_def = col.def_levels[current_pos];
                if (current_def <= def_threshold) {
                    // Empty list marker
                    consumeNullFromLeaves(element, ctx);
                    break;
                }

                const item = try assembleRecursive(element, ctx, def_threshold + 1, rep_threshold + 1, depth + 1);
                try items.append(ctx.allocator, item);
                first = false;
            }

            const owned = try items.toOwnedSlice(ctx.allocator);
            return .{ .list_val = owned };
        },
        .map => |m| {
            const col = ctx.columns[ctx.column_index];
            const pos = ctx.positions[ctx.column_index];
            if (pos >= col.def_levels.len) {
                return .{ .null_val = {} };
            }

            const def = col.def_levels[pos];
            if (def < def_threshold) {
                // Null map (for required maps, this never triggers; for optional
                // maps the .optional wrapper handles null before reaching here)
                consumeNullFromLeaves(m.key, ctx);
                ctx.column_index += m.key.countLeafColumns();
                consumeNullFromLeaves(m.value, ctx);
                ctx.column_index -= m.key.countLeafColumns();
                return .{ .null_val = {} };
            }

            var entries: std.ArrayList(Value.MapEntryValue) = .empty;
            errdefer {
                for (entries.items) |entry| {
                    entry.key.deinit(ctx.allocator);
                    entry.value.deinit(ctx.allocator);
                }
                entries.deinit(ctx.allocator);
            }

            const key_leaves = m.key.countLeafColumns();
            var first = true;
            while (true) {
                const current_pos = ctx.positions[ctx.column_index];
                if (current_pos >= col.def_levels.len) break;

                const current_rep = col.rep_levels[current_pos];
                if (!first and current_rep <= rep_threshold) break;

                const current_def = col.def_levels[current_pos];
                if (current_def <= def_threshold) {
                    // Empty map marker or end of entries
                    consumeNullFromLeaves(m.key, ctx);
                    ctx.column_index += key_leaves;
                    consumeNullFromLeaves(m.value, ctx);
                    ctx.column_index -= key_leaves;
                    break;
                }

                const key = try assembleRecursive(m.key, ctx, def_threshold + 1, rep_threshold + 1, depth + 1);
                errdefer key.deinit(ctx.allocator);
                ctx.column_index += key_leaves;
                const val = try assembleRecursive(m.value, ctx, def_threshold + 1, rep_threshold + 1, depth + 1);
                errdefer val.deinit(ctx.allocator);
                ctx.column_index -= key_leaves;

                try entries.append(ctx.allocator, .{ .key = key, .value = val });
                first = false;
            }

            const owned = try entries.toOwnedSlice(ctx.allocator);
            return .{ .map_val = owned };
        },
        .struct_ => |s| {
            var fields: std.ArrayList(Value.FieldValue) = .empty;
            errdefer {
                for (fields.items) |f| {
                    ctx.allocator.free(f.name);
                    f.value.deinit(ctx.allocator);
                }
                fields.deinit(ctx.allocator);
            }

            for (s.fields) |schema_field| {
                const field_value = try assembleRecursive(schema_field.node, ctx, def_threshold, rep_threshold, depth + 1);
                const name_copy = try ctx.allocator.dupe(u8, schema_field.name);
                try fields.append(ctx.allocator, .{ .name = name_copy, .value = field_value });
                ctx.column_index += schema_field.node.countLeafColumns();
            }
            // Reset column index
            for (s.fields) |f| {
                ctx.column_index -= f.node.countLeafColumns();
            }

            const owned = try fields.toOwnedSlice(ctx.allocator);
            return .{ .struct_val = owned };
        },
        // Primitives - read from column (clone heap-allocated data to avoid double-free)
        .boolean, .int32, .int64, .float, .double, .byte_array, .fixed_len_byte_array => {
            const col = ctx.columns[ctx.column_index];
            const pos = ctx.positions[ctx.column_index];
            if (pos >= col.values.len) {
                return .{ .null_val = {} };
            }
            ctx.positions[ctx.column_index] += 1;
            const v = col.values[pos];
            return switch (v) {
                .bytes_val => |b| .{ .bytes_val = try value_mod.dupeBytes(ctx.allocator, b) },
                .fixed_bytes_val => |b| .{ .fixed_bytes_val = try value_mod.dupeBytes(ctx.allocator, b) },
                else => v,
            };
        },
    }
}

fn consumeNullFromLeaves(node: *const SchemaNode, ctx: *AssembleContext) void {
    switch (node.*) {
        .optional => |child| consumeNullFromLeaves(child, ctx),
        .list => |element| consumeNullFromLeaves(element, ctx),
        .map => |m| {
            consumeNullFromLeaves(m.key, ctx);
            ctx.column_index += m.key.countLeafColumns();
            consumeNullFromLeaves(m.value, ctx);
            ctx.column_index -= m.key.countLeafColumns();
        },
        .struct_ => |s| {
            for (s.fields) |f| {
                consumeNullFromLeaves(f.node, ctx);
                ctx.column_index += f.node.countLeafColumns();
            }
            for (s.fields) |f| {
                ctx.column_index -= f.node.countLeafColumns();
            }
        },
        .boolean, .int32, .int64, .float, .double, .byte_array, .fixed_len_byte_array => {
            ctx.positions[ctx.column_index] += 1;
        },
    }
}

pub const NestedError = error{
    NotEnoughColumns,
    OutOfMemory,
};

// =============================================================================
// Tests
// =============================================================================

test "flatten and assemble primitive" {
    const allocator = std.testing.allocator;

    const schema = SchemaNode{ .int32 = .{} };
    const value = Value{ .int32_val = 42 };

    var columns = try flattenValue(allocator, &schema, value);
    defer columns.deinit();

    try std.testing.expectEqual(@as(usize, 1), columns.columns.items.len);
    try std.testing.expectEqual(@as(usize, 1), columns.columns.items[0].values.items.len);
    try std.testing.expectEqual(@as(i32, 42), columns.columns.items[0].values.items[0].asInt32().?);

    // Assemble back
    const slices = [_]FlatColumnSlice{
        .{
            .values = columns.columns.items[0].values.items,
            .def_levels = columns.columns.items[0].def_levels.items,
            .rep_levels = columns.columns.items[0].rep_levels.items,
        },
    };
    const assembled = try assembleValue(allocator, &schema, &slices);
    try std.testing.expectEqual(@as(i32, 42), assembled.asInt32().?);
}

test "flatten and assemble simple list" {
    const allocator = std.testing.allocator;

    const element = SchemaNode{ .int32 = .{} };
    const schema = SchemaNode{ .list = &element };

    const items = [_]Value{
        .{ .int32_val = 1 },
        .{ .int32_val = 2 },
        .{ .int32_val = 3 },
    };
    const value = Value{ .list_val = &items };

    var columns = try flattenValue(allocator, &schema, value);
    defer columns.deinit();

    try std.testing.expectEqual(@as(usize, 1), columns.columns.items.len);
    try std.testing.expectEqual(@as(usize, 3), columns.columns.items[0].values.items.len);

    // Check levels
    const col = &columns.columns.items[0];
    try std.testing.expectEqual(@as(u32, 1), col.def_levels.items[0]);
    try std.testing.expectEqual(@as(u32, 1), col.def_levels.items[1]);
    try std.testing.expectEqual(@as(u32, 0), col.rep_levels.items[0]); // first item
    try std.testing.expectEqual(@as(u32, 1), col.rep_levels.items[1]); // continuation
}

test "flatten and assemble struct" {
    const allocator = std.testing.allocator;

    const id_node = SchemaNode{ .int64 = .{} };
    const name_node = SchemaNode{ .byte_array = .{} };
    const schema = SchemaNode{ .struct_ = .{
        .fields = &[_]SchemaNode.Field{
            .{ .name = "id", .node = &id_node },
            .{ .name = "name", .node = &name_node },
        },
    } };

    const fields = [_]Value.FieldValue{
        .{ .name = "id", .value = .{ .int64_val = 123 } },
        .{ .name = "name", .value = .{ .bytes_val = "test" } },
    };
    const value = Value{ .struct_val = &fields };

    var columns = try flattenValue(allocator, &schema, value);
    defer columns.deinit();

    try std.testing.expectEqual(@as(usize, 2), columns.columns.items.len);
    try std.testing.expectEqual(@as(i64, 123), columns.columns.items[0].values.items[0].asInt64().?);
    try std.testing.expectEqualStrings("test", columns.columns.items[1].values.items[0].asBytes().?);

    // Assemble back
    const slices = [_]FlatColumnSlice{
        .{
            .values = columns.columns.items[0].values.items,
            .def_levels = columns.columns.items[0].def_levels.items,
            .rep_levels = columns.columns.items[0].rep_levels.items,
        },
        .{
            .values = columns.columns.items[1].values.items,
            .def_levels = columns.columns.items[1].def_levels.items,
            .rep_levels = columns.columns.items[1].rep_levels.items,
        },
    };
    var assembled = try assembleValue(allocator, &schema, &slices);
    defer assembled.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 123), assembled.getField("id").?.asInt64().?);
    try std.testing.expectEqualStrings("test", assembled.getField("name").?.asBytes().?);
}

test "assembleValues multi-row round trip" {
    const allocator = std.testing.allocator;

    const schema = SchemaNode{ .int32 = .{} };

    const values = [_]Value{ .{ .int32_val = 10 }, .{ .int32_val = 20 }, .{ .int32_val = 30 } };
    const def_levels = [_]u32{ 0, 0, 0 };
    const rep_levels = [_]u32{ 0, 0, 0 };

    const slices = [_]FlatColumnSlice{
        .{
            .values = &values,
            .def_levels = &def_levels,
            .rep_levels = &rep_levels,
        },
    };

    const result = try assembleValues(allocator, &schema, &slices, 3);
    defer {
        for (result) |v| v.deinit(allocator);
        allocator.free(result);
    }

    try std.testing.expectEqual(@as(usize, 3), result.len);
    try std.testing.expectEqual(@as(i32, 10), result[0].asInt32().?);
    try std.testing.expectEqual(@as(i32, 20), result[1].asInt32().?);
    try std.testing.expectEqual(@as(i32, 30), result[2].asInt32().?);
}

test "assembleValues not enough columns returns error" {
    const allocator = std.testing.allocator;

    const id_node = SchemaNode{ .int64 = .{} };
    const name_node = SchemaNode{ .byte_array = .{} };
    const schema = SchemaNode{ .struct_ = .{
        .fields = &[_]SchemaNode.Field{
            .{ .name = "id", .node = &id_node },
            .{ .name = "name", .node = &name_node },
        },
    } };

    const slices = [_]FlatColumnSlice{
        .{ .values = &.{}, .def_levels = &.{}, .rep_levels = &.{} },
    };

    try std.testing.expectError(error.NotEnoughColumns, assembleValues(allocator, &schema, &slices, 1));
}
