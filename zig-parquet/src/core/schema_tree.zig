//! The Arrow view of a Parquet schema: a tree of struct, list, map and leaf nodes with the
//! definition and repetition levels of each node, as in Arrow's Parquet reader. Both the Arrow
//! read path and the row API assemble nested values from it.

const std = @import("std");
const safe = @import("safe.zig");
const format = @import("format.zig");
const arrow = @import("arrow.zig");
const leaf_decoder = @import("leaf_decoder.zig");

const Allocator = std.mem.Allocator;
const Buffer = leaf_decoder.Buffer;

pub const TreeError = error{ OutOfMemory, InvalidSchema };
pub const ShapeError = error{ OutOfMemory, InvalidPageData, IntegerOverflow };

fn allocBuffer(allocator: Allocator, len: usize) Allocator.Error!Buffer {
    return allocator.alignedAlloc(u8, leaf_decoder.buffer_alignment, len);
}

/// Definition and repetition levels of a schema tree node, as in Arrow's Parquet reader
/// (`LevelInfo` in parquet/level_conversion.h). For an entry of a leaf column under the node:
/// - Entries with a definition level below `ancestor_def_level` (the level of the nearest
///   repeated ancestor) or a repetition level above `rep_level` have no slot at the node.
/// - Struct and leaf: every other entry is a slot, non-null when the definition level is at
///   least `def_level`.
/// - List and map: an entry whose repetition level equals `rep_level` adds an element to the
///   current slot; an entry with a lower repetition level starts a slot, which is non-null
///   when the definition level is at least `def_level - 1` and has a first element when it
///   is at least `def_level`.
pub const LevelInfo = struct {
    def_level: u32 = 0,
    rep_level: u32 = 0,
    ancestor_def_level: u32 = 0,

    /// Levels after a repeated field: one more definition and repetition level.
    pub fn repeated(self: LevelInfo) LevelInfo {
        return .{ .def_level = self.def_level + 1, .rep_level = self.rep_level + 1, .ancestor_def_level = self.ancestor_def_level };
    }

    /// Levels of the elements of a list or map with these levels.
    pub fn elements(self: LevelInfo) LevelInfo {
        return .{ .def_level = self.def_level, .rep_level = self.rep_level, .ancestor_def_level = self.def_level };
    }
};

pub const NodeKind = enum { leaf, struct_, list, map };

/// A node of the Arrow view of the Parquet schema.
pub const Node = struct {
    kind: NodeKind,
    /// Schema element that names the node and, for a leaf, holds its type.
    schema_idx: usize,
    nullable: bool,
    levels: LevelInfo,
    /// Leaf columns under the node: `physical_start .. physical_start + physical_count`.
    physical_start: usize,
    physical_count: usize,
    /// struct: fields. list: the element. map: the "entries" struct of key and value.
    children: []Node,
};

/// Builds the top-level nodes of `schema`. Nodes are allocated with `allocator` and not freed
/// individually; use an arena.
pub fn buildSchemaTree(allocator: Allocator, schema: []const format.SchemaElement) TreeError![]Node {
    if (schema.len == 0) return error.InvalidSchema;
    const nc = safe.castTo(usize, schema[0].num_children orelse return error.InvalidSchema) catch return error.InvalidSchema;
    var builder = TreeBuilder{ .allocator = allocator, .schema = schema };
    const columns = try allocator.alloc(Node, nc);
    for (columns) |*column| column.* = try builder.node(.{});
    return columns;
}

/// Reads schema elements in order (`next_idx`), assigning leaf columns in order
/// (`next_physical`).
const TreeBuilder = struct {
    allocator: Allocator,
    schema: []const format.SchemaElement,
    next_idx: usize = 1,
    next_physical: usize = 0,

    /// The node at `next_idx`, under a parent with levels `info`.
    fn node(self: *TreeBuilder, info: LevelInfo) TreeError!Node {
        if (self.next_idx >= self.schema.len) return error.InvalidSchema;
        const idx = self.next_idx;
        if (self.schema[idx].repetition_type == .repeated) {
            // A repeated field outside a LIST or MAP annotation is a list of required elements.
            const list_info = info.repeated();
            const start = self.next_physical;
            const children = try self.allocator.alloc(Node, 1);
            children[0] = try self.plain(list_info.elements());
            children[0].nullable = false;
            return .{
                .kind = .list,
                .schema_idx = idx,
                .nullable = false,
                .levels = list_info,
                .physical_start = start,
                .physical_count = self.next_physical - start,
                .children = children,
            };
        }
        return self.plain(info);
    }

    /// The node at `next_idx`, ignoring a `repeated` repetition (already applied by the caller).
    fn plain(self: *TreeBuilder, parent: LevelInfo) TreeError!Node {
        const idx = self.next_idx;
        const elem = self.schema[idx];
        const optional = elem.repetition_type == .optional;
        var info = parent;
        if (optional) info.def_level += 1;

        if (elem.num_children == null) {
            self.next_idx += 1;
            self.next_physical += 1;
            return .{
                .kind = .leaf,
                .schema_idx = idx,
                .nullable = optional,
                .levels = info,
                .physical_start = self.next_physical - 1,
                .physical_count = 1,
                .children = &.{},
            };
        }

        const nc = safe.castTo(usize, elem.num_children.?) catch return error.InvalidSchema;
        if (nc == 0) return error.InvalidSchema;
        if (elem.converted_type) |ct| {
            if (ct == format.ConvertedType.LIST) return self.list(idx, info, optional);
            if (ct == format.ConvertedType.MAP or ct == format.ConvertedType.MAP_KEY_VALUE) return self.map(idx, info, optional);
        }

        self.next_idx += 1;
        const start = self.next_physical;
        const children = try self.allocator.alloc(Node, nc);
        for (children) |*child| child.* = try self.node(info);
        return .{
            .kind = .struct_,
            .schema_idx = idx,
            .nullable = optional,
            .levels = info,
            .physical_start = start,
            .physical_count = self.next_physical - start,
            .children = children,
        };
    }

    /// A LIST-annotated group at `idx` with levels `info`: a repeated field holding the
    /// element. Backward-compatibility rules of the Parquet spec decide whether the repeated
    /// field is the element itself (a primitive, a group with several fields, or a group with
    /// one field named "array" or "<list name>_tuple") or a group whose only field is the
    /// element.
    fn list(self: *TreeBuilder, idx: usize, info: LevelInfo, nullable: bool) TreeError!Node {
        const r_idx = idx + 1;
        if (r_idx >= self.schema.len) return error.InvalidSchema;
        const repeated = self.schema[r_idx];
        if (repeated.repetition_type != .repeated) return error.InvalidSchema;
        const list_info = info.repeated();
        const start = self.next_physical;
        const children = try self.allocator.alloc(Node, 1);

        self.next_idx = r_idx;
        if (repeatedFieldIsElement(repeated, self.schema[idx].name)) {
            children[0] = try self.plain(list_info.elements());
            children[0].nullable = false;
        } else {
            self.next_idx = r_idx + 1;
            children[0] = try self.node(list_info.elements());
        }
        return .{
            .kind = .list,
            .schema_idx = idx,
            .nullable = nullable,
            .levels = list_info,
            .physical_start = start,
            .physical_count = self.next_physical - start,
            .children = children,
        };
    }

    /// A MAP-annotated group at `idx` with levels `info`: a repeated group of two fields, key
    /// and value. A repeated group with only a key is read as a list of keys, as PyArrow does.
    fn map(self: *TreeBuilder, idx: usize, info: LevelInfo, nullable: bool) TreeError!Node {
        const kv_idx = idx + 1;
        if (kv_idx >= self.schema.len) return error.InvalidSchema;
        const kv = self.schema[kv_idx];
        const kv_nc = safe.castTo(usize, kv.num_children orelse return error.InvalidSchema) catch return error.InvalidSchema;
        if (kv.repetition_type != .repeated) return error.InvalidSchema;
        const map_info = info.repeated();
        const entries_info = map_info.elements();
        const start = self.next_physical;

        if (kv_nc == 1) {
            self.next_idx = kv_idx + 1;
            const children = try self.allocator.alloc(Node, 1);
            children[0] = try self.node(entries_info);
            return .{
                .kind = .list,
                .schema_idx = idx,
                .nullable = nullable,
                .levels = map_info,
                .physical_start = start,
                .physical_count = self.next_physical - start,
                .children = children,
            };
        }
        if (kv_nc != 2) return error.InvalidSchema;

        self.next_idx = kv_idx + 1;
        const fields = try self.allocator.alloc(Node, 2);
        for (fields) |*field| field.* = try self.node(entries_info);
        const children = try self.allocator.alloc(Node, 1);
        children[0] = .{
            .kind = .struct_,
            .schema_idx = kv_idx,
            .nullable = false,
            .levels = entries_info,
            .physical_start = start,
            .physical_count = self.next_physical - start,
            .children = fields,
        };
        return .{
            .kind = .map,
            .schema_idx = idx,
            .nullable = nullable,
            .levels = map_info,
            .physical_start = start,
            .physical_count = self.next_physical - start,
            .children = children,
        };
    }
};

pub fn repeatedFieldIsElement(repeated: format.SchemaElement, list_name: []const u8) bool {
    const nc = repeated.num_children orelse return true;
    if (nc != 1) return true;
    if (std.mem.eql(u8, repeated.name, "array")) return true;
    return std.mem.endsWith(u8, repeated.name, "_tuple") and
        repeated.name.len == list_name.len + "_tuple".len and
        std.mem.startsWith(u8, repeated.name, list_name);
}

/// Slots of a node computed from one leaf's levels (see `LevelInfo`).
pub const LevelShape = struct {
    length: usize,
    /// List and map: total elements (the last offset).
    elements: usize,
    validity: Buffer,
    null_count: i64,
    /// List and map: `length + 1` i32 offsets.
    offsets: ?Buffer,
};

pub fn levelShape(
    allocator: Allocator,
    def: ?[]const u32,
    rep: ?[]const u32,
    entries: usize,
    info: LevelInfo,
    is_list: bool,
) ShapeError!LevelShape {
    var length: usize = 0;
    for (0..entries) |i| {
        const d = if (def) |x| x[i] else 0;
        const r = if (rep) |x| x[i] else 0;
        if (r > info.rep_level or d < info.ancestor_def_level) continue;
        if (is_list and r == info.rep_level) continue;
        length += 1;
    }

    const validity = try allocBuffer(allocator, (length + 7) / 8);
    errdefer allocator.free(validity);
    @memset(validity, 0xFF);
    const offsets_buf: ?Buffer = if (is_list) try allocBuffer(allocator, (length + 1) * 4) else null;
    errdefer if (offsets_buf) |o| allocator.free(o);
    const offsets: [*]i32 = if (offsets_buf) |o| @ptrCast(@alignCast(o.ptr)) else undefined;

    var slot: usize = 0;
    var end: usize = 0;
    var null_count: i64 = 0;
    for (0..entries) |i| {
        const d = if (def) |x| x[i] else 0;
        const r = if (rep) |x| x[i] else 0;
        if (r > info.rep_level or d < info.ancestor_def_level) continue;
        var valid: bool = undefined;
        if (is_list) {
            if (r == info.rep_level) {
                // Another element of the current slot.
                if (slot == 0) return error.InvalidPageData;
                end += 1;
                continue;
            }
            offsets[slot] = try safe.castTo(i32, end);
            if (d >= info.def_level) end += 1;
            valid = d + 1 >= info.def_level;
        } else {
            valid = d >= info.def_level;
        }
        if (!valid) {
            arrow.clearBit(validity, slot);
            null_count += 1;
        }
        slot += 1;
    }
    if (is_list) offsets[length] = try safe.castTo(i32, end);

    return .{ .length = length, .elements = end, .validity = validity, .null_count = null_count, .offsets = offsets_buf };
}
