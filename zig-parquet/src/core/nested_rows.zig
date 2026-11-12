//! Row values of nested columns, assembled from the definition and repetition levels of their
//! leaf columns with the schema tree of the Arrow read path (`schema_tree.Node`).
//!
//! Each node's values are built for all of its slots at once. A list's items are one array
//! and each list value is a slice of it; map entries likewise. Struct fields are one block,
//! a run of `fields.len` per slot.

const std = @import("std");
const format = @import("format.zig");
const leaf_decoder = @import("leaf_decoder.zig");
const schema_tree = @import("schema_tree.zig");
const value_mod = @import("value.zig");

const Allocator = std.mem.Allocator;
const Value = value_mod.Value;
const Node = schema_tree.Node;

/// A decoded leaf column with its levels.
pub const Leaf = struct {
    /// Buffers with the values in place; byte payloads must live as long as the row values.
    buffers: leaf_decoder.Buffers,
    def_levels: ?[]const leaf_decoder.Level,
    rep_levels: ?[]const leaf_decoder.Level,
    element: format.SchemaElement,
    max_def: u32,
    max_rep: u32,
};

pub const Context = struct {
    /// Holds the list items, map entries, struct fields and field names of the values.
    arena: Allocator,
    /// Holds temporary arrays.
    scratch: Allocator,
    schema: []const format.SchemaElement,
    /// `leaves[k]` is leaf column `base + k`.
    leaves: []const Leaf,
    base: usize,
};

pub const Error = error{
    OutOfMemory,
    InvalidPageData,
    IntegerOverflow,
    InvalidSchema,
    InvalidTypeLength,
};

/// The values of `node`, one per slot, in an array allocated with `out`.
pub fn nodeValues(ctx: *const Context, node: *const Node, out: Allocator) Error![]Value {
    const first = &ctx.leaves[node.physical_start - ctx.base];
    switch (node.kind) {
        .leaf => {
            if (first.max_def != node.levels.def_level or first.max_rep != node.levels.rep_level) return error.InvalidSchema;
            const reader = try leaf_decoder.ValueReader.init(&first.buffers, first.element);
            return switch (reader.physical) {
                inline else => |physical| leafValues(physical, reader, first, node.levels.ancestor_def_level, out),
            };
        },
        .struct_ => {
            const shape = try schema_tree.levelShape(ctx.scratch, first.def_levels, first.rep_levels, first.buffers.length, node.levels, false);
            defer ctx.scratch.free(shape.validity);
            const n = node.children.len;
            const fields = try ctx.arena.alloc(Value.FieldValue, std.math.mul(usize, shape.length, n) catch return error.IntegerOverflow);
            for (node.children, 0..) |*child, k| {
                const name = try ctx.arena.dupe(u8, ctx.schema[child.schema_idx].name);
                const child_values = try nodeValues(ctx, child, ctx.scratch);
                defer ctx.scratch.free(child_values);
                if (child_values.len != shape.length) return error.InvalidPageData;
                for (child_values, 0..) |v, j| fields[j * n + k] = .{ .name = name, .value = v };
            }
            const values = try out.alloc(Value, shape.length);
            for (values, 0..) |*v, j| {
                v.* = if (isValid(shape.validity, j)) .{ .struct_val = fields[j * n ..][0..n] } else .{ .null_val = {} };
            }
            return values;
        },
        .list => {
            const shape = try schema_tree.levelShape(ctx.scratch, first.def_levels, first.rep_levels, first.buffers.length, node.levels, true);
            defer ctx.scratch.free(shape.validity);
            defer ctx.scratch.free(shape.offsets.?);
            // The items are referenced by the list values, so they go to the arena.
            const items = try nodeValues(ctx, &node.children[0], ctx.arena);
            if (items.len != shape.elements) return error.InvalidPageData;
            const offsets = offsetsOf(shape);
            const values = try out.alloc(Value, shape.length);
            for (values, 0..) |*v, j| {
                v.* = if (isValid(shape.validity, j)) .{ .list_val = items[offsets[j]..offsets[j + 1]] } else .{ .null_val = {} };
            }
            return values;
        },
        .map => {
            const entries_node = &node.children[0];
            if (entries_node.kind != .struct_ or entries_node.children.len != 2) return error.InvalidSchema;
            const shape = try schema_tree.levelShape(ctx.scratch, first.def_levels, first.rep_levels, first.buffers.length, node.levels, true);
            defer ctx.scratch.free(shape.validity);
            defer ctx.scratch.free(shape.offsets.?);
            const keys = try nodeValues(ctx, &entries_node.children[0], ctx.scratch);
            defer ctx.scratch.free(keys);
            const items = try nodeValues(ctx, &entries_node.children[1], ctx.scratch);
            defer ctx.scratch.free(items);
            if (keys.len != shape.elements or items.len != shape.elements) return error.InvalidPageData;
            const entries = try ctx.arena.alloc(Value.MapEntryValue, shape.elements);
            for (entries, keys, items) |*e, k, v| e.* = .{ .key = k, .value = v };
            const offsets = offsetsOf(shape);
            const values = try out.alloc(Value, shape.length);
            for (values, 0..) |*v, j| {
                v.* = if (isValid(shape.validity, j)) .{ .map_val = entries[offsets[j]..offsets[j + 1]] } else .{ .null_val = {} };
            }
            return values;
        },
    }
}

/// The values of a leaf's slots: entries at or above the nearest repeated ancestor's
/// definition level `min_def`.
fn leafValues(
    comptime physical: format.PhysicalType,
    reader: leaf_decoder.ValueReader,
    leaf: *const Leaf,
    min_def: u32,
    out: Allocator,
) Error![]Value {
    const entries = leaf.buffers.length;
    const def = leaf.def_levels orelse &.{};
    if (min_def == 0 or def.len == 0) {
        const values = try out.alloc(Value, entries);
        for (values, 0..) |*v, i| v.* = reader.atType(physical, i);
        return values;
    }
    if (def.len != entries) return error.InvalidPageData;
    var count: usize = 0;
    for (def) |d| count += @intFromBool(d >= min_def);
    const values = try out.alloc(Value, count);
    var slot: usize = 0;
    for (def, 0..) |d, i| {
        if (d < min_def) continue;
        values[slot] = reader.atType(physical, i);
        slot += 1;
    }
    return values;
}

fn isValid(validity: []const u8, i: usize) bool {
    return (validity[i / 8] >> @intCast(i % 8)) & 1 == 1;
}

/// The offsets of a list or map shape; `levelShape` writes them in order from 0, so they
/// are non-negative and non-decreasing.
fn offsetsOf(shape: schema_tree.LevelShape) []const u32 {
    const o = shape.offsets.?;
    return @as([*]const u32, @ptrCast(@alignCast(o.ptr)))[0 .. shape.length + 1];
}
