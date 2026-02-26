//! Parquet ↔ Arrow Batch API
//!
//! Runtime type dispatch for reading/writing row groups as Arrow arrays.
//! This is the bridge between Parquet's physical types and the Arrow C Data Interface.
//!
//! Functions:
//! - `exportSchemaAsArrow`: Convert Parquet FileMetaData schema to ArrowSchema
//! - `importSchemaFromArrow`: Convert ArrowSchema to Parquet ColumnDef[]
//! - `readRowGroupAsArrow`: Decode a row group's columns into ArrowArray[]
//! - `writeRowGroupFromArrow`: Encode ArrowArray[] into a Parquet row group

const std = @import("std");
const safe = @import("safe.zig");
const format = @import("format.zig");
const arrow = @import("arrow.zig");
const column_decoder = @import("column_decoder.zig");
const parquet_reader = @import("parquet_reader.zig");
const column_def_mod = @import("column_def.zig");
const value_mod = @import("value.zig");
const types = @import("types.zig");
const thrift = @import("thrift/mod.zig");
const compress = @import("compress/mod.zig");
const dictionary = @import("encoding/dictionary.zig");

const Allocator = std.mem.Allocator;

fn fmtZ(allocator: Allocator, comptime fmt_str: []const u8, args: anytype) error{OutOfMemory}![:0]u8 {
    const s = try std.fmt.allocPrint(allocator, fmt_str, args);
    defer allocator.free(s);
    return allocator.dupeZ(u8, s);
}
const ArrowSchema = arrow.ArrowSchema;
const ArrowArray = arrow.ArrowArray;
const Value = value_mod.Value;
const ColumnDef = column_def_mod.ColumnDef;
const StructField = column_def_mod.StructField;
const schema_mod = @import("schema.zig");
const SchemaNode = schema_mod.SchemaNode;
const SeekableReader = parquet_reader.SeekableReader;
const Optional = types.Optional;

pub const BatchError = error{
    OutOfMemory,
    InvalidArgument,
    UnsupportedType,
    InvalidSchema,
    InvalidMagic,
    FileTooSmall,
    FooterTooLarge,
    InputOutput,
    Unseekable,
    UnsupportedCompression,
    DecompressionError,
    UnsupportedEncoding,
    EndOfData,
    InvalidRowCount,
    InvalidPageSize,
    InvalidTypeLength,
    InvalidFieldType,
    IntegerOverflow,
    InvalidListType,
    ListTooLong,
    InvalidPhysicalType,
    InvalidRepetitionType,
    InvalidEncoding,
    InvalidCompressionCodec,
    PageChecksumMismatch,
    MissingPageChecksum,
    InvalidPageData,
    InvalidBitWidth,
    InvalidCompressionState,
    AllocationLimitExceeded,
    InvalidBitPackedLength,
};

// ============================================================================
// Private data for ArrowSchema release callback
// ============================================================================

const SchemaPrivateData = struct {
    allocator: Allocator,
    format_alloc: [:0]u8,
    name_alloc: ?[:0]u8,
    children_alloc: ?[]ArrowSchema,
    children_ptrs_alloc: ?[]*ArrowSchema,
};

fn schemaRelease(schema_ptr: *ArrowSchema) callconv(.c) void {
    const pd: *SchemaPrivateData = @ptrCast(@alignCast(schema_ptr.private_data));
    const allocator = pd.allocator;

    if (pd.children_alloc) |children| {
        for (children) |*child| child.doRelease();
        allocator.free(children);
    }
    if (pd.children_ptrs_alloc) |ptrs| allocator.free(ptrs);

    allocator.free(pd.format_alloc);
    if (pd.name_alloc) |name| allocator.free(name);

    allocator.destroy(pd);
    schema_ptr.release = null;
}

fn createSchemaNode(
    allocator: Allocator,
    format_str: []const u8,
    name: []const u8,
    nullable: bool,
    children_schemas: ?[]ArrowSchema,
    children_ptrs: ?[]*ArrowSchema,
) SchemaConvError!ArrowSchema {
    const pd = try allocator.create(SchemaPrivateData);
    errdefer allocator.destroy(pd);

    const fmt = try allocator.dupeZ(u8, format_str);
    errdefer allocator.free(fmt);

    const name_z = try allocator.dupeZ(u8, name);
    errdefer allocator.free(name_z);

    pd.* = .{
        .allocator = allocator,
        .format_alloc = fmt,
        .name_alloc = name_z,
        .children_alloc = children_schemas,
        .children_ptrs_alloc = children_ptrs,
    };

    const n_children: i64 = if (children_schemas) |c| (safe.castTo(i64, c.len) catch unreachable) else 0; // usize fits in i64

    return .{
        .format = fmt.ptr,
        .name = name_z.ptr,
        .metadata = null,
        .flags = if (nullable) arrow.ARROW_FLAG_NULLABLE else 0,
        .n_children = n_children,
        .children = if (children_ptrs) |p| @ptrCast(p.ptr) else null,
        .dictionary = null,
        .release = &schemaRelease,
        .private_data = @ptrCast(pd),
    };
}

// ============================================================================
// Schema Conversion: Parquet → Arrow
// ============================================================================

/// Convert a Parquet file schema to an ArrowSchema (struct type with column children).
/// Caller must call `schema.doRelease()` when done.
pub fn exportSchemaAsArrow(allocator: Allocator, metadata: format.FileMetaData) !ArrowSchema {
    const schema = metadata.schema;
    if (schema.len == 0) return error.InvalidSchema;

    const root = schema[0];
    const nc = root.num_children orelse return error.InvalidSchema;
    const num_children = safe.castTo(usize, nc) catch return error.InvalidSchema;

    var children = try allocator.alloc(ArrowSchema, num_children);
    var init_count: usize = 0;
    errdefer {
        for (children[0..init_count]) |*child| child.doRelease();
        allocator.free(children);
    }

    var children_ptrs = try allocator.alloc(*ArrowSchema, num_children);
    errdefer allocator.free(children_ptrs);

    var idx: usize = 1;
    for (0..num_children) |i| {
        const result = try buildSchemaSubtree(allocator, schema, idx);
        children[i] = result.schema;
        children_ptrs[i] = &children[i];
        idx = result.next_idx;
        init_count = i + 1;
    }

    return createSchemaNode(allocator, "+s", root.name, false, children, children_ptrs);
}

const SubtreeResult = struct {
    schema: ArrowSchema,
    next_idx: usize,
};

const SchemaConvError = error{
    OutOfMemory,
    InvalidSchema,
    UnsupportedType,
};

fn buildSchemaSubtree(allocator: Allocator, schema: []const format.SchemaElement, idx: usize) SchemaConvError!SubtreeResult {
    if (idx >= schema.len) return error.InvalidSchema;
    const elem = schema[idx];
    const nullable = if (elem.repetition_type) |rt| rt == .optional else false;

    if (elem.num_children != null) {
        return buildGroupSubtree(allocator, schema, idx, elem, nullable);
    }

    const fmt = try parquetTypeToArrowFormat(allocator, elem);
    errdefer allocator.free(fmt);

    const node = try createSchemaNode(allocator, fmt, elem.name, nullable, null, null);
    allocator.free(fmt);
    return .{ .schema = node, .next_idx = idx + 1 };
}

fn buildGroupSubtree(
    allocator: Allocator,
    schema: []const format.SchemaElement,
    idx: usize,
    elem: format.SchemaElement,
    nullable: bool,
) SchemaConvError!SubtreeResult {
    const nc = safe.castTo(usize, elem.num_children.?) catch return error.InvalidSchema;

    if (elem.converted_type) |ct| {
        if (ct == format.ConvertedType.LIST) {
            return buildListSubtree(allocator, schema, idx, elem.name, nullable, nc);
        }
        if (ct == format.ConvertedType.MAP or ct == format.ConvertedType.MAP_KEY_VALUE) {
            return buildMapSubtree(allocator, schema, idx, elem.name, nullable, nc);
        }
    }

    return buildStructSubtree(allocator, schema, idx, elem.name, nullable, nc);
}

fn buildListSubtree(
    allocator: Allocator,
    schema: []const format.SchemaElement,
    idx: usize,
    name: []const u8,
    nullable: bool,
    nc: usize,
) SchemaConvError!SubtreeResult {
    _ = nc;
    // LIST: container → repeated group → element
    // Skip the container and the repeated group, get the element
    var inner_idx = idx + 1;
    if (inner_idx >= schema.len) return error.InvalidSchema;

    const repeated_group = schema[inner_idx];
    if (repeated_group.num_children != null) {
        inner_idx += 1;
        if (inner_idx >= schema.len) return error.InvalidSchema;
    }

    const element_result = try buildSchemaSubtree(allocator, schema, inner_idx);
    var element_schema = element_result.schema;
    errdefer element_schema.doRelease();

    var list_children = try allocator.alloc(ArrowSchema, 1);
    errdefer allocator.free(list_children);
    list_children[0] = element_schema;

    var list_ptrs = try allocator.alloc(*ArrowSchema, 1);
    errdefer allocator.free(list_ptrs);
    list_ptrs[0] = &list_children[0];

    const list_schema = try createSchemaNode(allocator, "+l", name, nullable, list_children, list_ptrs);
    return .{ .schema = list_schema, .next_idx = element_result.next_idx };
}

fn buildMapSubtree(
    allocator: Allocator,
    schema: []const format.SchemaElement,
    idx: usize,
    name: []const u8,
    nullable: bool,
    nc: usize,
) SchemaConvError!SubtreeResult {
    _ = nc;
    // MAP: container → key_value (repeated) → key, value
    const kv_idx = idx + 1;
    if (kv_idx >= schema.len) return error.InvalidSchema;

    const kv_group = schema[kv_idx];
    const kv_nc = safe.castTo(usize, kv_group.num_children orelse return error.InvalidSchema) catch return error.InvalidSchema;

    var entries_children = try allocator.alloc(ArrowSchema, kv_nc);
    var entries_init: usize = 0;
    errdefer {
        for (entries_children[0..entries_init]) |*c| c.doRelease();
        allocator.free(entries_children);
    }

    var entries_ptrs = try allocator.alloc(*ArrowSchema, kv_nc);
    errdefer allocator.free(entries_ptrs);

    var child_idx = kv_idx + 1;
    for (0..kv_nc) |i| {
        const result = try buildSchemaSubtree(allocator, schema, child_idx);
        entries_children[i] = result.schema;
        entries_ptrs[i] = &entries_children[i];
        child_idx = result.next_idx;
        entries_init = i + 1;
    }

    var entries_schema = try createSchemaNode(allocator, "+s", "entries", false, entries_children, entries_ptrs);
    errdefer entries_schema.doRelease();

    var map_children = try allocator.alloc(ArrowSchema, 1);
    errdefer allocator.free(map_children);
    map_children[0] = entries_schema;

    var map_ptrs = try allocator.alloc(*ArrowSchema, 1);
    errdefer allocator.free(map_ptrs);
    map_ptrs[0] = &map_children[0];

    const map_schema = try createSchemaNode(allocator, "+m", name, nullable, map_children, map_ptrs);
    return .{ .schema = map_schema, .next_idx = child_idx };
}

fn buildStructSubtree(
    allocator: Allocator,
    schema: []const format.SchemaElement,
    idx: usize,
    name: []const u8,
    nullable: bool,
    nc: usize,
) SchemaConvError!SubtreeResult {
    var children = try allocator.alloc(ArrowSchema, nc);
    var init_count: usize = 0;
    errdefer {
        for (children[0..init_count]) |*c| c.doRelease();
        allocator.free(children);
    }

    var ptrs = try allocator.alloc(*ArrowSchema, nc);
    errdefer allocator.free(ptrs);

    var child_idx = idx + 1;
    for (0..nc) |i| {
        const result = try buildSchemaSubtree(allocator, schema, child_idx);
        children[i] = result.schema;
        ptrs[i] = &children[i];
        child_idx = result.next_idx;
        init_count = i + 1;
    }

    const struct_schema = try createSchemaNode(allocator, "+s", name, nullable, children, ptrs);
    return .{ .schema = struct_schema, .next_idx = child_idx };
}

/// Map a Parquet SchemaElement to an Arrow format string.
/// Caller must free the returned sentinel-terminated slice.
fn parquetTypeToArrowFormat(allocator: Allocator, elem: format.SchemaElement) SchemaConvError![:0]u8 {
    if (elem.logical_type) |lt| {
        return logicalTypeToArrowFormat(allocator, lt, elem);
    }

    if (elem.converted_type) |ct| {
        return convertedTypeToArrowFormat(allocator, ct, elem);
    }

    return physicalTypeToArrowFormat(allocator, elem);
}

fn logicalTypeToArrowFormat(allocator: Allocator, lt: format.LogicalType, elem: format.SchemaElement) SchemaConvError![:0]u8 {
    return switch (lt) {
        .string => allocator.dupeZ(u8, "u"),
        .enum_ => allocator.dupeZ(u8, "u"),
        .json => allocator.dupeZ(u8, "u"),
        .bson => allocator.dupeZ(u8, "z"),
        .date => allocator.dupeZ(u8, "tdD"),
        .uuid => allocator.dupeZ(u8, "w:16"),
        .float16 => allocator.dupeZ(u8, "e"),
        .geometry => allocator.dupeZ(u8, "z"),
        .geography => allocator.dupeZ(u8, "z"),
        .int => |i| {
            if (i.is_signed) {
                return switch (i.bit_width) {
                    8 => allocator.dupeZ(u8, "c"),
                    16 => allocator.dupeZ(u8, "s"),
                    32 => allocator.dupeZ(u8, "i"),
                    64 => allocator.dupeZ(u8, "l"),
                    else => error.UnsupportedType,
                };
            } else {
                return switch (i.bit_width) {
                    8 => allocator.dupeZ(u8, "C"),
                    16 => allocator.dupeZ(u8, "S"),
                    32 => allocator.dupeZ(u8, "I"),
                    64 => allocator.dupeZ(u8, "L"),
                    else => error.UnsupportedType,
                };
            }
        },
        .timestamp => |ts| {
            const unit_char: u8 = switch (ts.unit) {
                .millis => 'm',
                .micros => 'u',
                .nanos => 'n',
            };
            const tz = if (ts.is_adjusted_to_utc) "UTC" else "";
            return fmtZ(allocator, "ts{c}:{s}", .{ unit_char, tz });
        },
        .time => |t| switch (t.unit) {
            .millis => allocator.dupeZ(u8, "ttm"),
            .micros => allocator.dupeZ(u8, "ttu"),
            .nanos => allocator.dupeZ(u8, "ttn"),
        },
        .decimal => |d| {
            if (elem.type_) |pt| {
                if (pt == .fixed_len_byte_array) {
                    const tl = elem.type_length orelse 0;
                    return fmtZ(allocator, "d:{d},{d},{d}", .{ d.precision, d.scale, tl });
                }
            }
            return fmtZ(allocator, "d:{d},{d}", .{ d.precision, d.scale });
        },
    };
}

fn convertedTypeToArrowFormat(allocator: Allocator, ct: i32, elem: format.SchemaElement) SchemaConvError![:0]u8 {
    return switch (ct) {
        format.ConvertedType.UTF8 => allocator.dupeZ(u8, "u"),
        format.ConvertedType.DATE => allocator.dupeZ(u8, "tdD"),
        format.ConvertedType.TIMESTAMP_MILLIS => allocator.dupeZ(u8, "tsm:"),
        format.ConvertedType.TIMESTAMP_MICROS => allocator.dupeZ(u8, "tsu:"),
        format.ConvertedType.TIME_MILLIS => allocator.dupeZ(u8, "ttm"),
        format.ConvertedType.TIME_MICROS => allocator.dupeZ(u8, "ttu"),
        format.ConvertedType.INT_8 => allocator.dupeZ(u8, "c"),
        format.ConvertedType.INT_16 => allocator.dupeZ(u8, "s"),
        format.ConvertedType.INT_32 => allocator.dupeZ(u8, "i"),
        format.ConvertedType.INT_64 => allocator.dupeZ(u8, "l"),
        format.ConvertedType.UINT_8 => allocator.dupeZ(u8, "C"),
        format.ConvertedType.UINT_16 => allocator.dupeZ(u8, "S"),
        format.ConvertedType.UINT_32 => allocator.dupeZ(u8, "I"),
        format.ConvertedType.UINT_64 => allocator.dupeZ(u8, "L"),
        format.ConvertedType.ENUM => allocator.dupeZ(u8, "u"),
        format.ConvertedType.JSON => allocator.dupeZ(u8, "u"),
        format.ConvertedType.BSON => allocator.dupeZ(u8, "z"),
        format.ConvertedType.INTERVAL => allocator.dupeZ(u8, "w:12"),
        format.ConvertedType.DECIMAL => blk: {
            const p = elem.precision orelse 0;
            const s = elem.scale orelse 0;
            break :blk fmtZ(allocator, "d:{d},{d}", .{ p, s });
        },
        else => physicalTypeToArrowFormat(allocator, elem),
    };
}

fn physicalTypeToArrowFormat(allocator: Allocator, elem: format.SchemaElement) SchemaConvError![:0]u8 {
    const pt = elem.type_ orelse return error.InvalidSchema;
    return switch (pt) {
        .boolean => allocator.dupeZ(u8, "b"),
        .int32 => allocator.dupeZ(u8, "i"),
        .int64 => allocator.dupeZ(u8, "l"),
        .int96 => allocator.dupeZ(u8, "tsn:"),
        .float => allocator.dupeZ(u8, "f"),
        .double => allocator.dupeZ(u8, "g"),
        .byte_array => allocator.dupeZ(u8, "z"),
        .fixed_len_byte_array => blk: {
            const tl = elem.type_length orelse return error.InvalidSchema;
            break :blk fmtZ(allocator, "w:{d}", .{tl});
        },
    };
}

// ============================================================================
// Schema Conversion: Arrow → ColumnDef
// ============================================================================

/// Convert an ArrowSchema (root struct) to Parquet ColumnDef[].
/// The caller owns the returned slice.
pub fn importSchemaFromArrow(allocator: Allocator, schema: *const ArrowSchema) ![]ColumnDef {
    const fmt = std.mem.sliceTo(schema.format, 0);
    if (!std.mem.eql(u8, fmt, "+s")) return error.InvalidSchema;

    const nc = safe.castTo(usize, schema.n_children) catch return error.InvalidSchema;
    var col_defs = try allocator.alloc(ColumnDef, nc);
    errdefer allocator.free(col_defs);

    const children: [*]*ArrowSchema = schema.children orelse return error.InvalidSchema;
    for (0..nc) |i| {
        const child = children[i];
        col_defs[i] = try arrowSchemaToColumnDef(allocator, child);
    }

    return col_defs;
}

fn arrowSchemaToColumnDef(allocator: Allocator, schema: *const ArrowSchema) !ColumnDef {
    const fmt = std.mem.sliceTo(schema.format, 0);
    const name = if (schema.name) |n| std.mem.sliceTo(n, 0) else "";
    const nullable = (schema.flags & arrow.ARROW_FLAG_NULLABLE) != 0;

    if (std.mem.eql(u8, fmt, "+l")) {
        return arrowListToColumnDef(allocator, schema, name, nullable);
    }
    if (std.mem.eql(u8, fmt, "+m")) {
        return arrowMapToColumnDef(allocator, schema, name, nullable);
    }
    if (std.mem.eql(u8, fmt, "+s")) {
        return arrowStructToColumnDef(allocator, schema, name, nullable);
    }

    return arrowLeafToColumnDef(fmt, name, nullable);
}

fn hasNestedFormat(fmt: []const u8) bool {
    return fmt.len >= 2 and fmt[0] == '+';
}

fn arrowSchemaToSchemaNode(allocator: Allocator, schema: *const ArrowSchema, nullable: bool) !*const SchemaNode {
    const fmt = std.mem.sliceTo(schema.format, 0);

    const node = allocator.create(SchemaNode) catch return error.OutOfMemory;

    if (std.mem.eql(u8, fmt, "+l") or std.mem.eql(u8, fmt, "+L")) {
        if (schema.n_children != 1) return error.InvalidSchema;
        const children: [*]*ArrowSchema = schema.children orelse return error.InvalidSchema;
        const elem = children[0];
        const elem_nullable = (elem.flags & arrow.ARROW_FLAG_NULLABLE) != 0;
        const elem_node = try arrowSchemaToSchemaNode(allocator, elem, elem_nullable);
        node.* = .{ .list = elem_node };
    } else if (std.mem.eql(u8, fmt, "+s")) {
        const nc = safe.castTo(usize, schema.n_children) catch return error.InvalidSchema;
        const children: [*]*ArrowSchema = schema.children orelse return error.InvalidSchema;
        const fields = allocator.alloc(SchemaNode.Field, nc) catch return error.OutOfMemory;
        for (0..nc) |i| {
            const child = children[i];
            const child_name = if (child.name) |n| std.mem.sliceTo(n, 0) else "";
            const child_nullable = (child.flags & arrow.ARROW_FLAG_NULLABLE) != 0;
            fields[i] = .{
                .name = child_name,
                .node = try arrowSchemaToSchemaNode(allocator, child, child_nullable),
            };
        }
        node.* = .{ .struct_ = .{ .fields = fields } };
    } else if (std.mem.eql(u8, fmt, "+m")) {
        if (schema.n_children != 1) return error.InvalidSchema;
        const entries: [*]*ArrowSchema = schema.children orelse return error.InvalidSchema;
        const entries_sch = entries[0];
        if (entries_sch.n_children != 2) return error.InvalidSchema;
        const kv_children: [*]*ArrowSchema = entries_sch.children orelse return error.InvalidSchema;
        const key_node = try arrowSchemaToSchemaNode(allocator, kv_children[0], false);
        const val_nullable = (kv_children[1].flags & arrow.ARROW_FLAG_NULLABLE) != 0;
        const val_node = try arrowSchemaToSchemaNode(allocator, kv_children[1], val_nullable);
        node.* = .{ .map = .{ .key = key_node, .value = val_node } };
    } else {
        node.* = try arrowFormatToSchemaNode(fmt);
    }

    if (nullable) {
        const opt_node = allocator.create(SchemaNode) catch return error.OutOfMemory;
        opt_node.* = .{ .optional = node };
        return opt_node;
    }

    return node;
}

fn arrowFormatToSchemaNode(fmt: []const u8) !SchemaNode {
    if (fmt.len == 1) {
        return switch (fmt[0]) {
            'b' => SchemaNode{ .boolean = .{} },
            'c', 's', 'i' => SchemaNode{ .int32 = .{} },
            'C', 'S' => SchemaNode{ .int32 = .{} },
            'l' => SchemaNode{ .int64 = .{} },
            'I', 'L' => SchemaNode{ .int64 = .{} },
            'e' => SchemaNode{ .fixed_len_byte_array = .{ .len = 2, .logical = .float16 } },
            'f' => SchemaNode{ .float = .{} },
            'g' => SchemaNode{ .double = .{} },
            'u', 'U' => SchemaNode{ .byte_array = .{ .logical = .string } },
            'z', 'Z' => SchemaNode{ .byte_array = .{} },
            else => return error.UnsupportedType,
        };
    }
    if (std.mem.eql(u8, fmt, "tdD") or std.mem.eql(u8, fmt, "tdm")) {
        return SchemaNode{ .int32 = .{ .logical = .date } };
    }
    if (std.mem.eql(u8, fmt, "ttm") or std.mem.eql(u8, fmt, "tts")) {
        return SchemaNode{ .int32 = .{ .logical = .{ .time = .{ .is_adjusted_to_utc = false, .unit = .millis } } } };
    }
    if (std.mem.eql(u8, fmt, "ttu")) {
        return SchemaNode{ .int64 = .{ .logical = .{ .time = .{ .is_adjusted_to_utc = false, .unit = .micros } } } };
    }
    if (std.mem.eql(u8, fmt, "ttn")) {
        return SchemaNode{ .int64 = .{ .logical = .{ .time = .{ .is_adjusted_to_utc = false, .unit = .nanos } } } };
    }
    if (fmt.len >= 4 and std.mem.eql(u8, fmt[0..2], "ts")) {
        const unit: format.TimeUnit = switch (fmt[2]) {
            's', 'm' => .millis,
            'u' => .micros,
            'n' => .nanos,
            else => return error.UnsupportedType,
        };
        const tz_part = fmt[4..];
        const is_utc = tz_part.len > 0 and std.mem.eql(u8, tz_part, "UTC");
        return SchemaNode{ .int64 = .{ .logical = .{ .timestamp = .{ .is_adjusted_to_utc = is_utc, .unit = unit } } } };
    }
    if (fmt.len >= 2 and fmt[0] == 'w' and fmt[1] == ':') {
        const type_length = std.fmt.parseInt(u32, fmt[2..], 10) catch return error.InvalidSchema;
        const logical: ?format.LogicalType = if (type_length == 16) .uuid else null;
        return SchemaNode{ .fixed_len_byte_array = .{ .len = type_length, .logical = logical } };
    }
    if (fmt.len >= 4 and fmt[0] == 'd' and fmt[1] == ':') {
        const params = fmt[2..];
        var parts = std.mem.splitScalar(u8, params, ',');
        const prec_str = parts.next() orelse return error.InvalidSchema;
        const scale_str = parts.next() orelse return error.InvalidSchema;
        const precision = std.fmt.parseInt(i32, prec_str, 10) catch return error.InvalidSchema;
        const scale = std.fmt.parseInt(i32, scale_str, 10) catch return error.InvalidSchema;

        if (parts.next()) |bw_str| {
            const bw = std.fmt.parseInt(u32, bw_str, 10) catch return error.InvalidSchema;
            return SchemaNode{ .fixed_len_byte_array = .{ .len = bw, .logical = .{ .decimal = .{ .precision = precision, .scale = scale } } } };
        } else if (precision <= 9) {
            return SchemaNode{ .int32 = .{ .logical = .{ .decimal = .{ .precision = precision, .scale = scale } } } };
        } else if (precision <= 18) {
            return SchemaNode{ .int64 = .{ .logical = .{ .decimal = .{ .precision = precision, .scale = scale } } } };
        } else {
            const bw: u32 = safe.castTo(u32, decimalByteLength(precision)) catch unreachable; // decimalByteLength max is 16
            return SchemaNode{ .fixed_len_byte_array = .{ .len = bw, .logical = .{ .decimal = .{ .precision = precision, .scale = scale } } } };
        }
    }
    return error.UnsupportedType;
}

fn arrowLeafToColumnDef(fmt: []const u8, name: []const u8, nullable: bool) !ColumnDef {
    var def = ColumnDef{
        .name = name,
        .type_ = .int32,
        .optional = nullable,
    };

    if (fmt.len == 1) {
        switch (fmt[0]) {
            'b' => def.type_ = .boolean,
            'c' => {
                def.type_ = .int32;
                def.logical_type = .{ .int = .{ .bit_width = 8, .is_signed = true } };
            },
            'C' => {
                def.type_ = .int32;
                def.logical_type = .{ .int = .{ .bit_width = 8, .is_signed = false } };
            },
            's' => {
                def.type_ = .int32;
                def.logical_type = .{ .int = .{ .bit_width = 16, .is_signed = true } };
            },
            'S' => {
                def.type_ = .int32;
                def.logical_type = .{ .int = .{ .bit_width = 16, .is_signed = false } };
            },
            'i' => def.type_ = .int32,
            'I' => {
                def.type_ = .int32;
                def.logical_type = .{ .int = .{ .bit_width = 32, .is_signed = false } };
            },
            'l' => def.type_ = .int64,
            'L' => {
                def.type_ = .int64;
                def.logical_type = .{ .int = .{ .bit_width = 64, .is_signed = false } };
            },
            'e' => {
                def.type_ = .fixed_len_byte_array;
                def.type_length = 2;
                def.logical_type = .float16;
            },
            'f' => def.type_ = .float,
            'g' => def.type_ = .double,
            'u' => {
                def.type_ = .byte_array;
                def.logical_type = .string;
            },
            'z' => def.type_ = .byte_array,
            'Z' => def.type_ = .byte_array,
            'U' => {
                def.type_ = .byte_array;
                def.logical_type = .string;
            },
            else => return error.UnsupportedType,
        }
        return def;
    }

    // Multi-character format strings
    if (std.mem.eql(u8, fmt, "tdD")) {
        def.type_ = .int32;
        def.logical_type = .date;
        return def;
    }
    if (std.mem.eql(u8, fmt, "tdm")) {
        // Date64 (millis) → stored as INT32 days in Parquet; conversion happens in write path
        def.type_ = .int32;
        def.logical_type = .date;
        return def;
    }
    if (std.mem.eql(u8, fmt, "tts")) {
        // Time32[s] → stored as INT32 TIME_MILLIS in Parquet; conversion happens in write path
        def.type_ = .int32;
        def.logical_type = .{ .time = .{ .is_adjusted_to_utc = false, .unit = .millis } };
        return def;
    }
    if (std.mem.eql(u8, fmt, "ttm")) {
        def.type_ = .int32;
        def.logical_type = .{ .time = .{ .is_adjusted_to_utc = false, .unit = .millis } };
        return def;
    }
    if (std.mem.eql(u8, fmt, "ttu")) {
        def.type_ = .int64;
        def.logical_type = .{ .time = .{ .is_adjusted_to_utc = false, .unit = .micros } };
        return def;
    }
    if (std.mem.eql(u8, fmt, "ttn")) {
        def.type_ = .int64;
        def.logical_type = .{ .time = .{ .is_adjusted_to_utc = false, .unit = .nanos } };
        return def;
    }

    // Timestamp: ts{unit}:{tz}
    if (fmt.len >= 4 and std.mem.eql(u8, fmt[0..2], "ts")) {
        def.type_ = .int64;
        const unit: format.TimeUnit = switch (fmt[2]) {
            's' => .millis,
            'm' => .millis,
            'u' => .micros,
            'n' => .nanos,
            else => return error.UnsupportedType,
        };
        const tz_part = fmt[4..]; // after "ts?:"
        const is_utc = tz_part.len > 0 and std.mem.eql(u8, tz_part, "UTC");
        def.logical_type = .{ .timestamp = .{ .is_adjusted_to_utc = is_utc, .unit = unit } };
        return def;
    }

    // Fixed-width binary: w:{N}
    if (fmt.len >= 2 and fmt[0] == 'w' and fmt[1] == ':') {
        def.type_ = .fixed_len_byte_array;
        const len_str = fmt[2..];
        def.type_length = std.fmt.parseInt(i32, len_str, 10) catch return error.InvalidSchema;
        if (def.type_length.? == 16) {
            def.logical_type = .uuid;
        }
        return def;
    }

    // Decimal: d:{precision},{scale}[,{byte_width}]
    if (fmt.len >= 4 and fmt[0] == 'd' and fmt[1] == ':') {
        const params = fmt[2..];
        var parts = std.mem.splitScalar(u8, params, ',');
        const prec_str = parts.next() orelse return error.InvalidSchema;
        const scale_str = parts.next() orelse return error.InvalidSchema;
        const precision = std.fmt.parseInt(i32, prec_str, 10) catch return error.InvalidSchema;
        const scale = std.fmt.parseInt(i32, scale_str, 10) catch return error.InvalidSchema;

        if (parts.next()) |bw_str| {
            def.type_ = .fixed_len_byte_array;
            def.type_length = std.fmt.parseInt(i32, bw_str, 10) catch return error.InvalidSchema;
        } else if (precision <= 9) {
            def.type_ = .int32;
        } else if (precision <= 18) {
            def.type_ = .int64;
        } else {
            def.type_ = .fixed_len_byte_array;
            def.type_length = safe.castTo(i32, decimalByteLength(precision)) catch unreachable; // decimalByteLength max is 16
        }
        def.logical_type = .{ .decimal = .{ .precision = precision, .scale = scale } };
        return def;
    }

    return error.UnsupportedType;
}

fn decimalByteLength(precision: i32) usize {
    // ceil((P * log2(10) + 1) / 8)
    const p: usize = if (precision > 0) (safe.castTo(usize, precision) catch unreachable) else 1; // guarded > 0
    const lookup = [_]usize{ 1, 1, 1, 2, 2, 3, 3, 4, 4, 4, 5, 5, 6, 6, 6, 7, 7, 8, 8, 9, 9, 9, 10, 10, 11, 11, 11, 12, 12, 13, 13, 14, 14, 14, 15, 15, 16, 16, 16 };
    if (p < lookup.len) return lookup[p];
    return 16;
}

fn arrowListToColumnDef(allocator: Allocator, schema: *const ArrowSchema, name: []const u8, nullable: bool) !ColumnDef {
    if (schema.n_children != 1) return error.InvalidSchema;
    const children: [*]*ArrowSchema = schema.children orelse return error.InvalidSchema;
    const element = children[0];
    const elem_fmt = std.mem.sliceTo(element.format, 0);
    const elem_nullable = (element.flags & arrow.ARROW_FLAG_NULLABLE) != 0;

    if (hasNestedFormat(elem_fmt)) {
        const node = try arrowSchemaToSchemaNode(allocator, schema, nullable);
        return ColumnDef.fromNode(name, node);
    }

    var def = try arrowLeafToColumnDef(elem_fmt, name, nullable);
    def.is_list = true;
    def.element_optional = elem_nullable;
    return def;
}

fn arrowMapToColumnDef(allocator: Allocator, schema: *const ArrowSchema, name: []const u8, nullable: bool) !ColumnDef {
    if (schema.n_children != 1) return error.InvalidSchema;
    const entries: [*]*ArrowSchema = schema.children orelse return error.InvalidSchema;
    const entries_schema = entries[0];
    if (entries_schema.n_children != 2) return error.InvalidSchema;
    const kv_children: [*]*ArrowSchema = entries_schema.children orelse return error.InvalidSchema;

    const key_fmt = std.mem.sliceTo(kv_children[0].format, 0);
    const val_fmt = std.mem.sliceTo(kv_children[1].format, 0);
    const val_nullable = (kv_children[1].flags & arrow.ARROW_FLAG_NULLABLE) != 0;

    if (hasNestedFormat(key_fmt) or hasNestedFormat(val_fmt)) {
        const node = try arrowSchemaToSchemaNode(allocator, schema, nullable);
        return ColumnDef.fromNode(name, node);
    }

    const key_def = try arrowLeafToColumnDef(key_fmt, name, nullable);
    const val_def = try arrowLeafToColumnDef(val_fmt, "", false);

    return .{
        .name = name,
        .type_ = key_def.type_,
        .optional = nullable,
        .is_map = true,
        .map_value_type = val_def.type_,
        .map_value_optional = val_nullable,
        .logical_type = key_def.logical_type,
    };
}

fn arrowStructToColumnDef(allocator: Allocator, schema: *const ArrowSchema, name: []const u8, nullable: bool) !ColumnDef {
    const nc = safe.castTo(usize, schema.n_children) catch return error.InvalidSchema;
    const children: [*]*ArrowSchema = schema.children orelse return error.InvalidSchema;

    var has_nested = false;
    for (0..nc) |i| {
        const child_fmt = std.mem.sliceTo(children[i].format, 0);
        if (hasNestedFormat(child_fmt)) {
            has_nested = true;
            break;
        }
    }

    if (has_nested) {
        const node = try arrowSchemaToSchemaNode(allocator, schema, nullable);
        return ColumnDef.fromNode(name, node);
    }

    var struct_fields_buf: [32]StructField = undefined;
    const field_count = @min(nc, struct_fields_buf.len);

    for (0..field_count) |i| {
        const child = children[i];
        const child_fmt = std.mem.sliceTo(child.format, 0);
        const child_name = if (child.name) |n| std.mem.sliceTo(n, 0) else "";
        const child_nullable = (child.flags & arrow.ARROW_FLAG_NULLABLE) != 0;

        const child_def = try arrowLeafToColumnDef(child_fmt, child_name, child_nullable);
        struct_fields_buf[i] = .{
            .name = child_name,
            .type_ = child_def.type_,
            .optional = child_nullable,
            .logical_type = child_def.logical_type,
        };
    }

    return .{
        .name = name,
        .type_ = .int32,
        .optional = nullable,
        .is_struct = true,
        .struct_fields = struct_fields_buf[0..field_count],
    };
}

// ============================================================================
// Private data for ArrowArray release callback
// ============================================================================

const ArrayPrivateData = struct {
    allocator: Allocator,
    buffer_allocs: [3]?[]u8,
    children_alloc: ?[]ArrowArray,
    children_ptrs_alloc: ?[]*ArrowArray,
    buffers_alloc: []?*anyopaque,
};

fn arrayRelease(array_ptr: *ArrowArray) callconv(.c) void {
    const pd: *ArrayPrivateData = @ptrCast(@alignCast(array_ptr.private_data));
    const allocator = pd.allocator;

    if (pd.children_alloc) |children| {
        for (children) |*child| child.doRelease();
        allocator.free(children);
    }
    if (pd.children_ptrs_alloc) |ptrs| allocator.free(ptrs);

    for (&pd.buffer_allocs) |maybe_buf| {
        if (maybe_buf) |buf| allocator.free(buf);
    }

    allocator.free(pd.buffers_alloc);
    allocator.destroy(pd);
    array_ptr.release = null;
}

// ============================================================================
// Logical Column Mapping
// ============================================================================

const LogicalColumnKind = enum { leaf, list, struct_, map };

const LogicalColumn = struct {
    kind: LogicalColumnKind,
    schema_idx: usize,
    physical_col_start: usize,
    physical_col_count: usize,
    children: []LogicalColumn,
    allocator: Allocator,

    fn deinit(self: *LogicalColumn) void {
        for (self.children) |*child| child.deinit();
        if (self.children.len > 0) self.allocator.free(self.children);
    }
};

const LogicalColumnBuildResult = struct {
    column: LogicalColumn,
    next_schema_idx: usize,
    next_physical_idx: usize,
};

fn buildLogicalColumns(allocator: Allocator, schema: []const format.SchemaElement) BatchError![]LogicalColumn {
    if (schema.len == 0) return error.InvalidSchema;
    const root = schema[0];
    const nc = safe.castTo(usize, root.num_children orelse return error.InvalidSchema) catch return error.InvalidSchema;

    var result = allocator.alloc(LogicalColumn, nc) catch return error.OutOfMemory;
    var init_count: usize = 0;
    errdefer {
        for (result[0..init_count]) |*c| c.deinit();
        allocator.free(result);
    }

    var schema_idx: usize = 1;
    var physical_idx: usize = 0;

    for (0..nc) |i| {
        const col_result = try buildLogicalColumn(allocator, schema, schema_idx, physical_idx);
        result[i] = col_result.column;
        schema_idx = col_result.next_schema_idx;
        physical_idx = col_result.next_physical_idx;
        init_count = i + 1;
    }

    return result;
}

fn buildLogicalColumn(allocator: Allocator, schema: []const format.SchemaElement, schema_idx: usize, physical_idx: usize) BatchError!LogicalColumnBuildResult {
    if (schema_idx >= schema.len) return error.InvalidSchema;
    const elem = schema[schema_idx];

    if (elem.num_children == null) {
        return .{
            .column = .{
                .kind = .leaf,
                .schema_idx = schema_idx,
                .physical_col_start = physical_idx,
                .physical_col_count = 1,
                .children = &.{},
                .allocator = allocator,
            },
            .next_schema_idx = schema_idx + 1,
            .next_physical_idx = physical_idx + 1,
        };
    }

    const nc = safe.castTo(usize, elem.num_children.?) catch return error.InvalidSchema;

    if (elem.converted_type) |ct| {
        if (ct == format.ConvertedType.LIST) {
            return buildLogicalList(allocator, schema, schema_idx, physical_idx);
        }
        if (ct == format.ConvertedType.MAP or ct == format.ConvertedType.MAP_KEY_VALUE) {
            return buildLogicalMap(allocator, schema, schema_idx, physical_idx);
        }
    }

    return buildLogicalStruct(allocator, schema, schema_idx, physical_idx, nc);
}

fn buildLogicalList(allocator: Allocator, schema: []const format.SchemaElement, schema_idx: usize, physical_idx: usize) BatchError!LogicalColumnBuildResult {
    var inner_idx = schema_idx + 1;
    if (inner_idx >= schema.len) return error.InvalidSchema;

    const repeated_group = schema[inner_idx];
    if (repeated_group.num_children != null) {
        inner_idx += 1;
        if (inner_idx >= schema.len) return error.InvalidSchema;
    }

    const element_result = try buildLogicalColumn(allocator, schema, inner_idx, physical_idx);

    var children = allocator.alloc(LogicalColumn, 1) catch return error.OutOfMemory;
    children[0] = element_result.column;

    return .{
        .column = .{
            .kind = .list,
            .schema_idx = schema_idx,
            .physical_col_start = physical_idx,
            .physical_col_count = element_result.column.physical_col_count,
            .children = children,
            .allocator = allocator,
        },
        .next_schema_idx = element_result.next_schema_idx,
        .next_physical_idx = element_result.next_physical_idx,
    };
}

fn buildLogicalMap(allocator: Allocator, schema: []const format.SchemaElement, schema_idx: usize, physical_idx: usize) BatchError!LogicalColumnBuildResult {
    const kv_idx = schema_idx + 1;
    if (kv_idx >= schema.len) return error.InvalidSchema;

    const kv_group = schema[kv_idx];
    const kv_nc = safe.castTo(usize, kv_group.num_children orelse return error.InvalidSchema) catch return error.InvalidSchema;
    if (kv_nc != 2) return error.InvalidSchema;

    var children = allocator.alloc(LogicalColumn, 2) catch return error.OutOfMemory;
    errdefer allocator.free(children);

    var child_schema_idx = kv_idx + 1;
    var child_phys_idx = physical_idx;

    for (0..2) |i| {
        const child_result = try buildLogicalColumn(allocator, schema, child_schema_idx, child_phys_idx);
        children[i] = child_result.column;
        child_schema_idx = child_result.next_schema_idx;
        child_phys_idx = child_result.next_physical_idx;
    }

    const total_physical = child_phys_idx - physical_idx;

    return .{
        .column = .{
            .kind = .map,
            .schema_idx = schema_idx,
            .physical_col_start = physical_idx,
            .physical_col_count = total_physical,
            .children = children,
            .allocator = allocator,
        },
        .next_schema_idx = child_schema_idx,
        .next_physical_idx = child_phys_idx,
    };
}

fn buildLogicalStruct(allocator: Allocator, schema: []const format.SchemaElement, schema_idx: usize, physical_idx: usize, nc: usize) BatchError!LogicalColumnBuildResult {
    var children = allocator.alloc(LogicalColumn, nc) catch return error.OutOfMemory;
    var init_count: usize = 0;
    errdefer {
        for (children[0..init_count]) |*c| c.deinit();
        allocator.free(children);
    }

    var child_schema_idx = schema_idx + 1;
    var child_phys_idx = physical_idx;

    for (0..nc) |i| {
        const child_result = try buildLogicalColumn(allocator, schema, child_schema_idx, child_phys_idx);
        children[i] = child_result.column;
        child_schema_idx = child_result.next_schema_idx;
        child_phys_idx = child_result.next_physical_idx;
        init_count = i + 1;
    }

    const total_physical = child_phys_idx - physical_idx;

    return .{
        .column = .{
            .kind = .struct_,
            .schema_idx = schema_idx,
            .physical_col_start = physical_idx,
            .physical_col_count = total_physical,
            .children = children,
            .allocator = allocator,
        },
        .next_schema_idx = child_schema_idx,
        .next_physical_idx = child_phys_idx,
    };
}

// ============================================================================
// Raw Physical Column Data (preserves rep/def levels)
// ============================================================================

const RawColumnData = struct {
    values: []Value,
    def_levels: []u32,
    rep_levels: []u32,
    column_info: format.ColumnInfo,
    allocator: Allocator,

    fn deinit(self: *RawColumnData) void {
        for (self.values) |v| v.deinit(self.allocator);
        self.allocator.free(self.values);
        self.allocator.free(self.def_levels);
        self.allocator.free(self.rep_levels);
    }
};

fn readPhysicalColumnRaw(
    allocator: Allocator,
    source: SeekableReader,
    metadata: format.FileMetaData,
    rg: *const format.RowGroup,
    col_idx: usize,
) BatchError!RawColumnData {
    if (col_idx >= rg.columns.len) return error.InvalidArgument;

    const chunk = &rg.columns[col_idx];
    const meta = chunk.meta_data orelse return error.InvalidArgument;

    const page_data = parquet_reader.readColumnChunkData(allocator, source, chunk) catch return error.InputOutput;
    defer allocator.free(page_data);

    const column_info = format.getColumnInfo(metadata.schema, col_idx) orelse
        return error.InvalidArgument;

    var dict_set = parquet_reader.DictionarySet.init(allocator);
    defer dict_set.deinit();

    var all_values: std.ArrayListUnmanaged(Value) = .empty;
    errdefer {
        for (all_values.items) |v| v.deinit(allocator);
        all_values.deinit(allocator);
    }
    var all_def_levels: std.ArrayListUnmanaged(u32) = .empty;
    errdefer all_def_levels.deinit(allocator);
    var all_rep_levels: std.ArrayListUnmanaged(u32) = .empty;
    errdefer all_rep_levels.deinit(allocator);

    var pos: usize = 0;

    if (pos < page_data.len) {
        var peek_thrift = thrift.CompactReader.init(page_data[pos..]);
        const first_header = format.PageHeader.parse(allocator, &peek_thrift) catch return error.EndOfData;
        defer parquet_reader.freePageHeaderContents(allocator, &first_header);

        if (first_header.dictionary_page_header) |dph| {
            pos += peek_thrift.pos;
            const dict_size = safe.cast(first_header.compressed_page_size) catch return error.InvalidPageSize;
            const dict_body = safe.slice(page_data, pos, dict_size) catch return error.EndOfData;
            pos += dict_size;

            const uncompressed_size = safe.cast(first_header.uncompressed_page_size) catch return error.InvalidPageSize;
            const num_values = safe.cast(dph.num_values) catch return error.InvalidPageSize;
            dict_set.initFromPage(
                dict_body,
                num_values,
                column_info.element.type_,
                column_info.element.type_length,
                meta.codec,
                uncompressed_size,
            ) catch return error.EndOfData;
        }
    }

    while (pos < page_data.len) {
        var thrift_reader = thrift.CompactReader.init(page_data[pos..]);
        const header = format.PageHeader.parse(allocator, &thrift_reader) catch return error.EndOfData;
        defer parquet_reader.freePageHeaderContents(allocator, &header);

        pos += thrift_reader.pos;

        if (header.data_page_header == null and header.data_page_header_v2 == null) {
            const skip = safe.cast(header.compressed_page_size) catch return error.InvalidPageSize;
            pos += skip;
            continue;
        }

        const compressed_size = safe.cast(header.compressed_page_size) catch return error.InvalidPageSize;
        if (pos + compressed_size > page_data.len) return error.EndOfData;
        const compressed_body = safe.slice(page_data, pos, compressed_size) catch return error.EndOfData;
        pos += compressed_size;

        if (header.data_page_header_v2) |v2| {
            try decodeV2PageRaw(allocator, &all_values, &all_def_levels, &all_rep_levels, compressed_body, v2, header, meta, column_info, &dict_set);
        } else if (header.data_page_header) |dph| {
            try decodeV1PageRaw(allocator, &all_values, &all_def_levels, &all_rep_levels, compressed_body, dph, header, meta, column_info, &dict_set);
        }
    }

    const values = all_values.toOwnedSlice(allocator) catch return error.OutOfMemory;
    errdefer {
        for (values) |v| v.deinit(allocator);
        allocator.free(values);
    }
    const def_levels = all_def_levels.toOwnedSlice(allocator) catch return error.OutOfMemory;
    errdefer allocator.free(def_levels);
    const rep_levels = all_rep_levels.toOwnedSlice(allocator) catch return error.OutOfMemory;

    return .{
        .values = values,
        .def_levels = def_levels,
        .rep_levels = rep_levels,
        .column_info = column_info,
        .allocator = allocator,
    };
}

fn decodeV2PageRaw(
    allocator: Allocator,
    all_values: *std.ArrayListUnmanaged(Value),
    all_def_levels: *std.ArrayListUnmanaged(u32),
    all_rep_levels: *std.ArrayListUnmanaged(u32),
    compressed_body: []const u8,
    v2: format.DataPageHeaderV2,
    header: format.PageHeader,
    meta: format.ColumnMetaData,
    column_info: format.ColumnInfo,
    dict_set: *parquet_reader.DictionarySet,
) BatchError!void {
    const rep_len = safe.cast(v2.repetition_levels_byte_length) catch return error.InvalidPageSize;
    const def_len = safe.cast(v2.definition_levels_byte_length) catch return error.InvalidPageSize;
    const num_values = safe.cast(v2.num_values) catch return error.InvalidPageSize;
    if (num_values == 0) return;

    const rep_data = compressed_body[0..rep_len];
    const def_data = safe.slice(compressed_body, rep_len, def_len) catch return error.EndOfData;
    const values_compressed = compressed_body[rep_len + def_len ..];

    var values_allocated = false;
    const values_data = if (v2.is_compressed and meta.codec != .uncompressed) blk: {
        const uncompressed = safe.cast(header.uncompressed_page_size) catch return error.InvalidPageSize;
        if (uncompressed < rep_len + def_len) return error.EndOfData;
        const val_size = uncompressed - rep_len - def_len;
        if (val_size == 0 or values_compressed.len == 0) break :blk values_compressed;
        values_allocated = true;
        break :blk compress.decompress(allocator, values_compressed, meta.codec, val_size) catch return error.DecompressionError;
    } else values_compressed;
    defer if (values_allocated) allocator.free(values_data);

    const has_dict = dict_set.hasDictionary();
    const result = column_decoder.decodeColumnDynamicV2(
        allocator,
        column_info.element,
        rep_data,
        def_data,
        values_data,
        num_values,
        column_info.max_def_level,
        column_info.max_rep_level,
        has_dict,
        if (dict_set.string_dict) |*d| d else null,
        if (dict_set.int32_dict) |*d| d else null,
        if (dict_set.int64_dict) |*d| d else null,
        if (dict_set.float32_dict) |*d| d else null,
        if (dict_set.float64_dict) |*d| d else null,
        if (dict_set.fixed_byte_array_dict) |*d| d else null,
        v2.encoding,
    ) catch return error.EndOfData;

    all_values.appendSlice(allocator, result.values) catch return error.OutOfMemory;
    allocator.free(result.values);

    if (result.def_levels) |dl| {
        all_def_levels.appendSlice(allocator, dl) catch return error.OutOfMemory;
        allocator.free(dl);
    } else {
        all_def_levels.appendNTimes(allocator, column_info.max_def_level, result.values.len) catch return error.OutOfMemory;
    }

    if (result.rep_levels) |rl| {
        all_rep_levels.appendSlice(allocator, rl) catch return error.OutOfMemory;
        allocator.free(rl);
    } else {
        all_rep_levels.appendNTimes(allocator, 0, result.values.len) catch return error.OutOfMemory;
    }
}

fn decodeV1PageRaw(
    allocator: Allocator,
    all_values: *std.ArrayListUnmanaged(Value),
    all_def_levels: *std.ArrayListUnmanaged(u32),
    all_rep_levels: *std.ArrayListUnmanaged(u32),
    compressed_body: []const u8,
    dph: format.DataPageHeader,
    header: format.PageHeader,
    meta: format.ColumnMetaData,
    column_info: format.ColumnInfo,
    dict_set: *parquet_reader.DictionarySet,
) BatchError!void {
    const value_data = if (meta.codec != .uncompressed) blk: {
        const uncompressed = safe.cast(header.uncompressed_page_size) catch return error.InvalidPageSize;
        break :blk compress.decompress(allocator, compressed_body, meta.codec, uncompressed) catch return error.DecompressionError;
    } else compressed_body;
    defer if (meta.codec != .uncompressed) allocator.free(value_data);

    const num_values = safe.cast(dph.num_values) catch return error.InvalidPageSize;
    if (num_values == 0) return;

    const has_dict = dict_set.hasDictionary();

    const result = column_decoder.decodeColumnDynamicWithValueEncoding(
        allocator,
        column_info.element,
        value_data,
        num_values,
        column_info.max_def_level,
        column_info.max_rep_level,
        has_dict,
        if (dict_set.string_dict) |*d| d else null,
        if (dict_set.int32_dict) |*d| d else null,
        if (dict_set.int64_dict) |*d| d else null,
        if (dict_set.float32_dict) |*d| d else null,
        if (dict_set.float64_dict) |*d| d else null,
        if (dict_set.fixed_byte_array_dict) |*d| d else null,
        dph.definition_level_encoding,
        dph.repetition_level_encoding,
        dph.encoding,
    ) catch return error.EndOfData;

    all_values.appendSlice(allocator, result.values) catch return error.OutOfMemory;
    allocator.free(result.values);

    if (result.def_levels) |dl| {
        all_def_levels.appendSlice(allocator, dl) catch return error.OutOfMemory;
        allocator.free(dl);
    } else {
        all_def_levels.appendNTimes(allocator, column_info.max_def_level, result.values.len) catch return error.OutOfMemory;
    }

    if (result.rep_levels) |rl| {
        all_rep_levels.appendSlice(allocator, rl) catch return error.OutOfMemory;
        allocator.free(rl);
    } else {
        all_rep_levels.appendNTimes(allocator, 0, result.values.len) catch return error.OutOfMemory;
    }
}

// ============================================================================
// Read Path: Parquet → ArrowArray
// ============================================================================

/// Result of reading a row group as Arrow arrays.
pub const ReadResult = struct {
    arrays: []ArrowArray,
    schema: ArrowSchema,
    allocator: Allocator,

    pub fn deinit(self: *ReadResult) void {
        for (self.arrays) |*arr| arr.doRelease();
        self.allocator.free(self.arrays);
        self.schema.doRelease();
    }
};

/// Read a row group's columns as Arrow arrays with runtime type dispatch.
/// Returns one ArrowArray per top-level logical column (matching the ArrowSchema structure).
/// If `col_indices` is null, all logical columns are read.
pub fn readRowGroupAsArrow(
    allocator: Allocator,
    source: SeekableReader,
    metadata: format.FileMetaData,
    rg_index: usize,
    col_indices: ?[]const usize,
) !ReadResult {
    if (rg_index >= metadata.row_groups.len) return error.InvalidArgument;
    const rg = &metadata.row_groups[rg_index];

    var logical_cols = try buildLogicalColumns(allocator, metadata.schema);
    defer {
        for (logical_cols) |*lc| lc.deinit();
        allocator.free(logical_cols);
    }

    const num_cols = if (col_indices) |ci| ci.len else logical_cols.len;

    var arrays = try allocator.alloc(ArrowArray, num_cols);
    var init_count: usize = 0;
    errdefer {
        for (arrays[0..init_count]) |*a| a.doRelease();
        allocator.free(arrays);
    }

    for (0..num_cols) |i| {
        const col_idx = if (col_indices) |ci| ci[i] else i;
        if (col_idx >= logical_cols.len) return error.InvalidArgument;
        arrays[i] = try readLogicalColumnAsArrow(allocator, source, metadata, rg, &logical_cols[col_idx]);
        init_count = i + 1;
    }

    var schema = try exportSchemaAsArrow(allocator, metadata);
    errdefer schema.doRelease();

    return .{
        .arrays = arrays,
        .schema = schema,
        .allocator = allocator,
    };
}

fn readLogicalColumnAsArrow(
    allocator: Allocator,
    source: SeekableReader,
    metadata: format.FileMetaData,
    rg: *const format.RowGroup,
    logical_col: *const LogicalColumn,
) !ArrowArray {
    return switch (logical_col.kind) {
        .leaf => readColumnAsArrow(allocator, source, metadata, rg, logical_col.physical_col_start),
        .list => readListColumnAsArrow(allocator, source, metadata, rg, logical_col),
        .struct_ => readStructColumnAsArrow(allocator, source, metadata, rg, logical_col),
        .map => readMapColumnAsArrow(allocator, source, metadata, rg, logical_col),
    };
}

fn readListColumnAsArrow(
    allocator: Allocator,
    source: SeekableReader,
    metadata: format.FileMetaData,
    rg: *const format.RowGroup,
    logical_col: *const LogicalColumn,
) !ArrowArray {
    var raw = try readPhysicalColumnRaw(allocator, source, metadata, rg, logical_col.physical_col_start);
    defer raw.deinit();

    const max_def: u32 = raw.column_info.max_def_level;
    const element_level: u32 = if (max_def >= 2) 2 else 1;

    var num_rows: usize = 0;
    for (raw.rep_levels) |rep| {
        if (rep == 0) num_rows += 1;
    }

    var num_elements: usize = 0;
    for (raw.def_levels) |def| {
        if (def >= element_level) num_elements += 1;
    }

    // Build offsets
    const offsets_buf = allocator.alloc(u8, (num_rows + 1) * 4) catch return error.OutOfMemory;
    errdefer allocator.free(offsets_buf);
    const offsets: [*]i32 = @ptrCast(@alignCast(offsets_buf.ptr));

    const parent_bitmap_len = (num_rows + 7) / 8;
    const parent_validity = allocator.alloc(u8, parent_bitmap_len) catch return error.OutOfMemory;
    errdefer allocator.free(parent_validity);
    @memset(parent_validity, 0xFF);

    var row_idx: usize = 0;
    var elem_count: usize = 0;
    var parent_null_count: i64 = 0;

    for (raw.def_levels, raw.rep_levels, 0..) |def, rep, i| {
        if (rep == 0) {
            if (i > 0) {
                row_idx += 1;
            }
            offsets[row_idx] = try safe.castTo(i32, elem_count);
            if (def == 0) {
                arrow.clearBit(parent_validity, row_idx);
                parent_null_count += 1;
            }
        }
        if (def >= element_level) {
            elem_count += 1;
        }
    }
    offsets[num_rows] = try safe.castTo(i32, elem_count);

    // Build child element values (only entries where def >= element_level)
    var element_values = allocator.alloc(Value, num_elements) catch return error.OutOfMemory;
    var elem_idx: usize = 0;
    for (raw.def_levels, raw.values) |def, val| {
        if (def >= element_level) {
            element_values[elem_idx] = val;
            elem_idx += 1;
        }
    }
    defer allocator.free(element_values);

    // Convert element values to ArrowArray (child)
    var child_array = try valuesToArrowArray(allocator, element_values, raw.column_info.element);
    errdefer child_array.doRelease();

    // Build parent list array
    return buildListArray(allocator, num_rows, parent_null_count, parent_validity, offsets_buf, &child_array);
}

fn readStructColumnAsArrow(
    allocator: Allocator,
    source: SeekableReader,
    metadata: format.FileMetaData,
    rg: *const format.RowGroup,
    logical_col: *const LogicalColumn,
) !ArrowArray {
    const nc = logical_col.children.len;

    var children = allocator.alloc(ArrowArray, nc) catch return error.OutOfMemory;
    var init_count: usize = 0;
    errdefer {
        for (children[0..init_count]) |*c| c.doRelease();
        allocator.free(children);
    }

    for (0..nc) |i| {
        children[i] = try readLogicalColumnAsArrow(allocator, source, metadata, rg, &logical_col.children[i]);
        init_count = i + 1;
    }

    // Struct validity: null if struct-level def == 0. Use first child's raw data to determine.
    const num_rows: usize = if (nc > 0) safe.castTo(usize, children[0].length) catch return error.IntegerOverflow else 0;
    const schema_elem = metadata.schema[logical_col.schema_idx];
    const nullable = if (schema_elem.repetition_type) |rt| rt == .optional else false;

    const bitmap_len = (num_rows + 7) / 8;
    const validity = allocator.alloc(u8, bitmap_len) catch return error.OutOfMemory;
    errdefer allocator.free(validity);
    @memset(validity, 0xFF);

    var null_count: i64 = 0;

    if (nullable and nc > 0) {
        // Struct is null at row i if ALL children are null at i
        for (0..num_rows) |i| {
            var all_null = true;
            for (children) |*child| {
                if (child.null_count == 0) {
                    all_null = false;
                    break;
                }
                const child_validity: ?[*]const u8 = if (child.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;
                if (child_validity) |cv| {
                    if (arrow.getBit(cv[0..bitmap_len], i)) {
                        all_null = false;
                        break;
                    }
                } else {
                    all_null = false;
                    break;
                }
            }
            if (all_null) {
                arrow.clearBit(validity, i);
                null_count += 1;
            }
        }
    }

    return buildStructArray(allocator, num_rows, null_count, validity, children);
}

fn readMapColumnAsArrow(
    allocator: Allocator,
    source: SeekableReader,
    metadata: format.FileMetaData,
    rg: *const format.RowGroup,
    logical_col: *const LogicalColumn,
) !ArrowArray {
    // MAP reads like a LIST but has 2 children (key, value) inside an entries struct
    // Read the key and value physical columns with levels
    if (logical_col.children.len != 2) return error.InvalidSchema;

    const key_phys_idx = logical_col.children[0].physical_col_start;
    const val_phys_idx = logical_col.children[1].physical_col_start;

    // Read key column (keys determine the list structure via rep/def levels)
    var key_raw = try readPhysicalColumnRaw(allocator, source, metadata, rg, key_phys_idx);
    defer key_raw.deinit();

    var val_raw = try readPhysicalColumnRaw(allocator, source, metadata, rg, val_phys_idx);
    defer val_raw.deinit();

    const key_max_def: u32 = key_raw.column_info.max_def_level;
    const element_level: u32 = if (key_max_def >= 2) 2 else 1;

    // Count rows and elements from key rep/def levels
    var num_rows: usize = 0;
    for (key_raw.rep_levels) |rep| {
        if (rep == 0) num_rows += 1;
    }

    var num_entries: usize = 0;
    for (key_raw.def_levels) |def| {
        if (def >= element_level) num_entries += 1;
    }

    // Build offsets from key rep levels
    const offsets_buf = allocator.alloc(u8, (num_rows + 1) * 4) catch return error.OutOfMemory;
    errdefer allocator.free(offsets_buf);
    const offsets: [*]i32 = @ptrCast(@alignCast(offsets_buf.ptr));

    const parent_bitmap_len = (num_rows + 7) / 8;
    const parent_validity = allocator.alloc(u8, parent_bitmap_len) catch return error.OutOfMemory;
    errdefer allocator.free(parent_validity);
    @memset(parent_validity, 0xFF);

    var row_idx: usize = 0;
    var entry_count: usize = 0;
    var parent_null_count: i64 = 0;

    for (key_raw.def_levels, key_raw.rep_levels, 0..) |def, rep, i| {
        if (rep == 0) {
            if (i > 0) row_idx += 1;
            offsets[row_idx] = try safe.castTo(i32, entry_count);
            if (def == 0) {
                arrow.clearBit(parent_validity, row_idx);
                parent_null_count += 1;
            }
        }
        if (def >= element_level) {
            entry_count += 1;
        }
    }
    offsets[num_rows] = try safe.castTo(i32, entry_count);

    // Filter key and value values to only entries
    var key_elements = allocator.alloc(Value, num_entries) catch return error.OutOfMemory;
    defer allocator.free(key_elements);
    var key_elem_idx: usize = 0;
    for (key_raw.def_levels, key_raw.values) |def, val| {
        if (def >= element_level) {
            key_elements[key_elem_idx] = val;
            key_elem_idx += 1;
        }
    }

    const val_max_def: u32 = val_raw.column_info.max_def_level;
    const val_element_level: u32 = if (val_max_def >= 2) 2 else 1;
    var val_elements = allocator.alloc(Value, num_entries) catch return error.OutOfMemory;
    defer allocator.free(val_elements);
    var val_elem_idx: usize = 0;
    for (val_raw.def_levels, val_raw.values) |def, val| {
        if (def >= val_element_level) {
            if (val_elem_idx < num_entries) {
                val_elements[val_elem_idx] = val;
                val_elem_idx += 1;
            }
        }
    }

    // Convert to ArrowArrays
    var key_array = try valuesToArrowArray(allocator, key_elements, key_raw.column_info.element);
    errdefer key_array.doRelease();

    var val_array = try valuesToArrowArray(allocator, val_elements, val_raw.column_info.element);
    errdefer val_array.doRelease();

    // Build entries struct with key and value children
    var entries_children = allocator.alloc(ArrowArray, 2) catch return error.OutOfMemory;
    entries_children[0] = key_array;
    entries_children[1] = val_array;
    // Transfer ownership: don't release key_array/val_array individually

    const entries_validity = allocator.alloc(u8, (num_entries + 7) / 8) catch return error.OutOfMemory;
    @memset(entries_validity, 0xFF);
    var entries_struct = try buildStructArray(allocator, num_entries, 0, entries_validity, entries_children);
    errdefer entries_struct.doRelease();

    // Build map array (like a list array with entries struct as child)
    return buildListArray(allocator, num_rows, parent_null_count, parent_validity, offsets_buf, &entries_struct);
}

// ============================================================================
// Nested ArrowArray Builders
// ============================================================================

fn buildListArray(
    allocator: Allocator,
    length: usize,
    null_count: i64,
    validity: []u8,
    offsets: []u8,
    child: *ArrowArray,
) !ArrowArray {
    const pd = allocator.create(ArrayPrivateData) catch return error.OutOfMemory;
    errdefer allocator.destroy(pd);

    var buffers = allocator.alloc(?*anyopaque, 2) catch return error.OutOfMemory;
    errdefer allocator.free(buffers);

    buffers[0] = if (null_count > 0) @ptrCast(validity.ptr) else null;
    buffers[1] = @ptrCast(offsets.ptr);

    var children = allocator.alloc(ArrowArray, 1) catch return error.OutOfMemory;
    errdefer allocator.free(children);
    children[0] = child.*;

    var child_ptrs = allocator.alloc(*ArrowArray, 1) catch return error.OutOfMemory;
    errdefer allocator.free(child_ptrs);
    child_ptrs[0] = &children[0];

    pd.* = .{
        .allocator = allocator,
        .buffer_allocs = .{ validity, offsets, null },
        .children_alloc = children,
        .children_ptrs_alloc = child_ptrs,
        .buffers_alloc = buffers,
    };

    // Prevent double-free: the child is now owned by the parent's private data
    child.release = null;

    return .{
        .length = safe.castTo(i64, length) catch unreachable, // usize fits in i64
        .null_count = null_count,
        .offset = 0,
        .n_buffers = 2,
        .n_children = 1,
        .buffers = buffers.ptr,
        .children = @ptrCast(child_ptrs.ptr),
        .dictionary = null,
        .release = &arrayRelease,
        .private_data = @ptrCast(pd),
    };
}

fn buildStructArray(
    allocator: Allocator,
    length: usize,
    null_count: i64,
    validity: []u8,
    children: []ArrowArray,
) !ArrowArray {
    const pd = allocator.create(ArrayPrivateData) catch return error.OutOfMemory;
    errdefer allocator.destroy(pd);

    var buffers = allocator.alloc(?*anyopaque, 1) catch return error.OutOfMemory;
    errdefer allocator.free(buffers);
    buffers[0] = if (null_count > 0) @ptrCast(validity.ptr) else null;

    const nc = children.len;
    var child_ptrs = allocator.alloc(*ArrowArray, nc) catch return error.OutOfMemory;
    errdefer allocator.free(child_ptrs);
    for (0..nc) |i| {
        child_ptrs[i] = &children[i];
    }

    pd.* = .{
        .allocator = allocator,
        .buffer_allocs = .{ validity, null, null },
        .children_alloc = children,
        .children_ptrs_alloc = child_ptrs,
        .buffers_alloc = buffers,
    };

    return .{
        .length = safe.castTo(i64, length) catch unreachable, // usize fits in i64
        .null_count = null_count,
        .offset = 0,
        .n_buffers = 1,
        .n_children = safe.castTo(i64, nc) catch unreachable, // usize fits in i64
        .buffers = buffers.ptr,
        .children = @ptrCast(child_ptrs.ptr),
        .dictionary = null,
        .release = &arrayRelease,
        .private_data = @ptrCast(pd),
    };
}

fn readColumnAsArrow(
    allocator: Allocator,
    source: SeekableReader,
    metadata: format.FileMetaData,
    rg: *const format.RowGroup,
    col_idx: usize,
) !ArrowArray {
    if (col_idx >= rg.columns.len) return error.InvalidArgument;

    const chunk = &rg.columns[col_idx];
    const meta = chunk.meta_data orelse return error.InvalidArgument;

    const page_data = try parquet_reader.readColumnChunkData(allocator, source, chunk);
    defer allocator.free(page_data);

    const column_info = format.getColumnInfo(metadata.schema, col_idx) orelse
        return error.InvalidArgument;

    // Parse pages, handling dictionary and data pages
    var dict_set = parquet_reader.DictionarySet.init(allocator);
    defer dict_set.deinit();

    var all_values: std.ArrayListUnmanaged(Value) = .empty;
    errdefer {
        for (all_values.items) |v| v.deinit(allocator);
        all_values.deinit(allocator);
    }

    var pos: usize = 0;

    // Check for dictionary page
    if (pos < page_data.len) {
        var peek_thrift = thrift.CompactReader.init(page_data[pos..]);
        const first_header = try format.PageHeader.parse(allocator, &peek_thrift);
        defer parquet_reader.freePageHeaderContents(allocator, &first_header);

        if (first_header.dictionary_page_header) |dph| {
            pos += peek_thrift.pos;
            const dict_size = safe.cast(first_header.compressed_page_size) catch return error.InvalidPageSize;
            const dict_body = try safe.slice(page_data, pos, dict_size);
            pos += dict_size;

            const uncompressed_size = safe.cast(first_header.uncompressed_page_size) catch return error.InvalidPageSize;
            const num_values = safe.cast(dph.num_values) catch return error.InvalidPageSize;
            try dict_set.initFromPage(
                dict_body,
                num_values,
                column_info.element.type_,
                column_info.element.type_length,
                meta.codec,
                uncompressed_size,
            );
        }
    }

    // Iterate data pages
    while (pos < page_data.len) {
        var thrift_reader = thrift.CompactReader.init(page_data[pos..]);
        const header = try format.PageHeader.parse(allocator, &thrift_reader);
        defer parquet_reader.freePageHeaderContents(allocator, &header);

        pos += thrift_reader.pos;

        if (header.data_page_header == null and header.data_page_header_v2 == null) {
            const skip = safe.cast(header.compressed_page_size) catch return error.InvalidPageSize;
            pos += skip;
            continue;
        }

        const compressed_size = safe.cast(header.compressed_page_size) catch return error.InvalidPageSize;
        if (pos + compressed_size > page_data.len) return error.EndOfData;
        const compressed_body = try safe.slice(page_data, pos, compressed_size);
        pos += compressed_size;

        if (header.data_page_header_v2) |v2| {
            try decodeV2Page(allocator, &all_values, compressed_body, v2, header, meta, column_info, &dict_set);
        } else if (header.data_page_header) |dph| {
            try decodeV1Page(allocator, &all_values, compressed_body, dph, header, meta, column_info, &dict_set);
        }
    }

    // Convert Value[] to ArrowArray, then free the intermediate Values
    const result = try valuesToArrowArray(allocator, all_values.items, column_info.element);
    for (all_values.items) |v| v.deinit(allocator);
    all_values.deinit(allocator);
    return result;
}

fn decodeV2Page(
    allocator: Allocator,
    all_values: *std.ArrayListUnmanaged(Value),
    compressed_body: []const u8,
    v2: format.DataPageHeaderV2,
    header: format.PageHeader,
    meta: format.ColumnMetaData,
    column_info: format.ColumnInfo,
    dict_set: *parquet_reader.DictionarySet,
) !void {
    const rep_len = safe.cast(v2.repetition_levels_byte_length) catch return error.InvalidPageSize;
    const def_len = safe.cast(v2.definition_levels_byte_length) catch return error.InvalidPageSize;
    const num_values = safe.cast(v2.num_values) catch return error.InvalidPageSize;
    if (num_values == 0) return;

    const rep_data = compressed_body[0..rep_len];
    const def_data = try safe.slice(compressed_body, rep_len, def_len);
    const values_compressed = compressed_body[rep_len + def_len ..];

    var values_allocated = false;
    const values_data = if (v2.is_compressed and meta.codec != .uncompressed) blk: {
        const uncompressed = safe.cast(header.uncompressed_page_size) catch return error.InvalidPageSize;
        if (uncompressed < rep_len + def_len) return error.EndOfData;
        const val_size = uncompressed - rep_len - def_len;
        if (val_size == 0 or values_compressed.len == 0) break :blk values_compressed;
        values_allocated = true;
        break :blk try compress.decompress(allocator, values_compressed, meta.codec, val_size);
    } else values_compressed;
    defer if (values_allocated) allocator.free(values_data);

    const has_dict = dict_set.hasDictionary();
    const result = try column_decoder.decodeColumnDynamicV2(
        allocator,
        column_info.element,
        rep_data,
        def_data,
        values_data,
        num_values,
        column_info.max_def_level,
        column_info.max_rep_level,
        has_dict,
        if (dict_set.string_dict) |*d| d else null,
        if (dict_set.int32_dict) |*d| d else null,
        if (dict_set.int64_dict) |*d| d else null,
        if (dict_set.float32_dict) |*d| d else null,
        if (dict_set.float64_dict) |*d| d else null,
        if (dict_set.fixed_byte_array_dict) |*d| d else null,
        v2.encoding,
    );
    defer {
        if (result.def_levels) |dl| allocator.free(dl);
        if (result.rep_levels) |rl| allocator.free(rl);
    }

    try all_values.appendSlice(allocator, result.values);
    allocator.free(result.values);
}

fn decodeV1Page(
    allocator: Allocator,
    all_values: *std.ArrayListUnmanaged(Value),
    compressed_body: []const u8,
    dph: format.DataPageHeader,
    header: format.PageHeader,
    meta: format.ColumnMetaData,
    column_info: format.ColumnInfo,
    dict_set: *parquet_reader.DictionarySet,
) !void {
    const value_data = if (meta.codec != .uncompressed) blk: {
        const uncompressed = safe.cast(header.uncompressed_page_size) catch return error.InvalidPageSize;
        break :blk try compress.decompress(allocator, compressed_body, meta.codec, uncompressed);
    } else compressed_body;
    defer if (meta.codec != .uncompressed) allocator.free(value_data);

    const num_values = safe.cast(dph.num_values) catch return error.InvalidPageSize;
    if (num_values == 0) return;

    const has_dict = dict_set.hasDictionary();

    const result = try column_decoder.decodeColumnDynamicWithValueEncoding(
        allocator,
        column_info.element,
        value_data,
        num_values,
        column_info.max_def_level,
        column_info.max_rep_level,
        has_dict,
        if (dict_set.string_dict) |*d| d else null,
        if (dict_set.int32_dict) |*d| d else null,
        if (dict_set.int64_dict) |*d| d else null,
        if (dict_set.float32_dict) |*d| d else null,
        if (dict_set.float64_dict) |*d| d else null,
        if (dict_set.fixed_byte_array_dict) |*d| d else null,
        dph.definition_level_encoding,
        dph.repetition_level_encoding,
        dph.encoding,
    );
    defer {
        if (result.def_levels) |dl| allocator.free(dl);
        if (result.rep_levels) |rl| allocator.free(rl);
    }

    try all_values.appendSlice(allocator, result.values);
    allocator.free(result.values);
}

// ============================================================================
// Value[] → ArrowArray conversion
// ============================================================================

fn valuesToArrowArray(allocator: Allocator, values: []const Value, elem: format.SchemaElement) !ArrowArray {
    const pt = elem.type_ orelse return error.InvalidSchema;
    return switch (pt) {
        .boolean => boolValuesToArrow(allocator, values),
        .int32 => int32ValuesToArrow(allocator, values),
        .int64, .int96 => int64ValuesToArrow(allocator, values),
        .float => floatValuesToArrow(allocator, values),
        .double => doubleValuesToArrow(allocator, values),
        .byte_array => byteArrayValuesToArrow(allocator, values),
        .fixed_len_byte_array => fixedByteArrayValuesToArrow(allocator, values, elem),
    };
}

fn boolValuesToArrow(allocator: Allocator, values: []const Value) !ArrowArray {
    const n = values.len;
    const bitmap_len = (n + 7) / 8;

    const validity = try allocator.alloc(u8, bitmap_len);
    errdefer allocator.free(validity);
    @memset(validity, 0xFF);

    const data = try allocator.alloc(u8, bitmap_len);
    errdefer allocator.free(data);
    @memset(data, 0);

    var null_count: i64 = 0;
    for (values, 0..) |v, i| {
        switch (v) {
            .bool_val => |b| {
                if (b) {
                    const byte_idx = i / 8;
                    const bit_idx: u3 = safe.castTo(u3, i % 8) catch unreachable; // i % 8 is 0-7
                    data[byte_idx] |= @as(u8, 1) << bit_idx;
                }
            },
            .null_val => {
                arrow.clearBit(validity, i);
                null_count += 1;
            },
            else => {
                arrow.clearBit(validity, i);
                null_count += 1;
            },
        }
    }

    return buildPrimitiveArray(allocator, n, null_count, validity, data);
}

fn int32ValuesToArrow(allocator: Allocator, values: []const Value) !ArrowArray {
    const n = values.len;
    const bitmap_len = (n + 7) / 8;

    const validity = try allocator.alloc(u8, bitmap_len);
    errdefer allocator.free(validity);
    @memset(validity, 0xFF);

    const data = try allocator.alloc(u8, n * 4);
    errdefer allocator.free(data);
    const typed: [*]i32 = @ptrCast(@alignCast(data.ptr));

    var null_count: i64 = 0;
    for (values, 0..) |v, i| {
        switch (v) {
            .int32_val => |x| typed[i] = x,
            .null_val => {
                typed[i] = 0;
                arrow.clearBit(validity, i);
                null_count += 1;
            },
            else => {
                typed[i] = 0;
                arrow.clearBit(validity, i);
                null_count += 1;
            },
        }
    }

    return buildPrimitiveArray(allocator, n, null_count, validity, data);
}

fn int64ValuesToArrow(allocator: Allocator, values: []const Value) !ArrowArray {
    const n = values.len;
    const bitmap_len = (n + 7) / 8;

    const validity = try allocator.alloc(u8, bitmap_len);
    errdefer allocator.free(validity);
    @memset(validity, 0xFF);

    const data = try allocator.alloc(u8, n * 8);
    errdefer allocator.free(data);
    const typed: [*]i64 = @ptrCast(@alignCast(data.ptr));

    var null_count: i64 = 0;
    for (values, 0..) |v, i| {
        switch (v) {
            .int64_val => |x| typed[i] = x,
            .null_val => {
                typed[i] = 0;
                arrow.clearBit(validity, i);
                null_count += 1;
            },
            else => {
                typed[i] = 0;
                arrow.clearBit(validity, i);
                null_count += 1;
            },
        }
    }

    return buildPrimitiveArray(allocator, n, null_count, validity, data);
}

fn floatValuesToArrow(allocator: Allocator, values: []const Value) !ArrowArray {
    const n = values.len;
    const bitmap_len = (n + 7) / 8;

    const validity = try allocator.alloc(u8, bitmap_len);
    errdefer allocator.free(validity);
    @memset(validity, 0xFF);

    const data = try allocator.alloc(u8, n * 4);
    errdefer allocator.free(data);
    const typed: [*]f32 = @ptrCast(@alignCast(data.ptr));

    var null_count: i64 = 0;
    for (values, 0..) |v, i| {
        switch (v) {
            .float_val => |x| typed[i] = x,
            .null_val => {
                typed[i] = 0;
                arrow.clearBit(validity, i);
                null_count += 1;
            },
            else => {
                typed[i] = 0;
                arrow.clearBit(validity, i);
                null_count += 1;
            },
        }
    }

    return buildPrimitiveArray(allocator, n, null_count, validity, data);
}

fn doubleValuesToArrow(allocator: Allocator, values: []const Value) !ArrowArray {
    const n = values.len;
    const bitmap_len = (n + 7) / 8;

    const validity = try allocator.alloc(u8, bitmap_len);
    errdefer allocator.free(validity);
    @memset(validity, 0xFF);

    const data = try allocator.alloc(u8, n * 8);
    errdefer allocator.free(data);
    const typed: [*]f64 = @ptrCast(@alignCast(data.ptr));

    var null_count: i64 = 0;
    for (values, 0..) |v, i| {
        switch (v) {
            .double_val => |x| typed[i] = x,
            .null_val => {
                typed[i] = 0;
                arrow.clearBit(validity, i);
                null_count += 1;
            },
            else => {
                typed[i] = 0;
                arrow.clearBit(validity, i);
                null_count += 1;
            },
        }
    }

    return buildPrimitiveArray(allocator, n, null_count, validity, data);
}

fn byteArrayValuesToArrow(allocator: Allocator, values: []const Value) !ArrowArray {
    const n = values.len;
    const bitmap_len = (n + 7) / 8;

    const validity = try allocator.alloc(u8, bitmap_len);
    errdefer allocator.free(validity);
    @memset(validity, 0xFF);

    // First pass: compute total data size
    var total_len: usize = 0;
    for (values) |v| {
        switch (v) {
            .bytes_val => |b| total_len += b.len,
            else => {},
        }
    }

    // Offsets buffer (n+1 int32 values)
    const offsets_buf = try allocator.alloc(u8, (n + 1) * 4);
    errdefer allocator.free(offsets_buf);
    const offsets: [*]i32 = @ptrCast(@alignCast(offsets_buf.ptr));

    // Data buffer
    const data_buf = try allocator.alloc(u8, if (total_len > 0) total_len else 1);
    errdefer allocator.free(data_buf);

    var null_count: i64 = 0;
    var data_pos: usize = 0;
    for (values, 0..) |v, i| {
        offsets[i] = try safe.castTo(i32, data_pos);
        switch (v) {
            .bytes_val => |b| {
                @memcpy(data_buf[data_pos..][0..b.len], b);
                data_pos += b.len;
            },
            .null_val => {
                arrow.clearBit(validity, i);
                null_count += 1;
            },
            else => {
                arrow.clearBit(validity, i);
                null_count += 1;
            },
        }
    }
    offsets[n] = try safe.castTo(i32, data_pos);

    return buildVariableArray(allocator, n, null_count, validity, offsets_buf, data_buf);
}

fn fixedByteArrayValuesToArrow(allocator: Allocator, values: []const Value, elem: format.SchemaElement) !ArrowArray {
    const tl = safe.cast(elem.type_length orelse return error.InvalidSchema) catch return error.InvalidTypeLength;
    const n = values.len;
    const bitmap_len = (n + 7) / 8;

    const validity = try allocator.alloc(u8, bitmap_len);
    errdefer allocator.free(validity);
    @memset(validity, 0xFF);

    const data = try allocator.alloc(u8, n * tl);
    errdefer allocator.free(data);
    @memset(data, 0);

    var null_count: i64 = 0;
    for (values, 0..) |v, i| {
        switch (v) {
            .fixed_bytes_val => |b| {
                const copy_len = @min(b.len, tl);
                @memcpy(data[i * tl ..][0..copy_len], b[0..copy_len]);
            },
            .null_val => {
                arrow.clearBit(validity, i);
                null_count += 1;
            },
            else => {
                arrow.clearBit(validity, i);
                null_count += 1;
            },
        }
    }

    return buildPrimitiveArray(allocator, n, null_count, validity, data);
}

// ============================================================================
// ArrowArray builders
// ============================================================================

fn buildPrimitiveArray(
    allocator: Allocator,
    length: usize,
    null_count: i64,
    validity: []u8,
    data: []u8,
) !ArrowArray {
    const pd = try allocator.create(ArrayPrivateData);
    errdefer allocator.destroy(pd);

    var buffers = try allocator.alloc(?*anyopaque, 2);
    errdefer allocator.free(buffers);

    buffers[0] = if (null_count > 0) @ptrCast(validity.ptr) else null;
    buffers[1] = @ptrCast(data.ptr);

    pd.* = .{
        .allocator = allocator,
        .buffer_allocs = .{ validity, data, null },
        .children_alloc = null,
        .children_ptrs_alloc = null,
        .buffers_alloc = buffers,
    };

    return .{
        .length = safe.castTo(i64, length) catch unreachable, // usize fits in i64
        .null_count = null_count,
        .offset = 0,
        .n_buffers = 2,
        .n_children = 0,
        .buffers = buffers.ptr,
        .children = null,
        .dictionary = null,
        .release = &arrayRelease,
        .private_data = @ptrCast(pd),
    };
}

fn buildVariableArray(
    allocator: Allocator,
    length: usize,
    null_count: i64,
    validity: []u8,
    offsets: []u8,
    data: []u8,
) !ArrowArray {
    const pd = try allocator.create(ArrayPrivateData);
    errdefer allocator.destroy(pd);

    var buffers = try allocator.alloc(?*anyopaque, 3);
    errdefer allocator.free(buffers);

    buffers[0] = if (null_count > 0) @ptrCast(validity.ptr) else null;
    buffers[1] = @ptrCast(offsets.ptr);
    buffers[2] = @ptrCast(data.ptr);

    pd.* = .{
        .allocator = allocator,
        .buffer_allocs = .{ validity, offsets, data },
        .children_alloc = null,
        .children_ptrs_alloc = null,
        .buffers_alloc = buffers,
    };

    return .{
        .length = safe.castTo(i64, length) catch unreachable, // usize fits in i64
        .null_count = null_count,
        .offset = 0,
        .n_buffers = 3,
        .n_children = 0,
        .buffers = buffers.ptr,
        .children = null,
        .dictionary = null,
        .release = &arrayRelease,
        .private_data = @ptrCast(pd),
    };
}

// ============================================================================
// Write Path: ArrowArray → Parquet
// ============================================================================

const Writer = @import("writer.zig").Writer;
const WriterError = types.WriterError;

/// Write Arrow arrays as a Parquet row group.
/// The Writer must be initialized with matching ColumnDefs (use importSchemaFromArrow).
pub fn writeRowGroupFromArrow(
    writer: *Writer,
    allocator: Allocator,
    arrays: []const ArrowArray,
    schemas: []const ArrowSchema,
) WriterError!void {
    if (arrays.len != schemas.len) return error.InvalidColumnIndex;

    for (arrays, schemas, 0..) |arr, sch, col_idx| {
        try writeArrowColumnToParquet(writer, allocator, col_idx, arr, sch);
    }
}

fn writeArrowColumnToParquet(
    writer: *Writer,
    allocator: Allocator,
    col_idx: usize,
    arr: ArrowArray,
    sch: ArrowSchema,
) WriterError!void {
    if (col_idx < writer.columns.len and writer.columns[col_idx].schema_node != null) {
        return writeNestedColumnFromArrow(writer, allocator, col_idx, arr, sch);
    }

    const fmt = std.mem.sliceTo(sch.format, 0);
    const n = safe.castTo(usize, arr.length) catch return error.IntegerOverflow;

    if (fmt.len == 1) {
        switch (fmt[0]) {
            'b' => try writeTypedColumn(bool, writer, allocator, col_idx, arr, n),
            'c' => try writeWidenedColumn(i8, i32, writer, allocator, col_idx, arr, n),
            'C' => try writeWidenedColumn(u8, i32, writer, allocator, col_idx, arr, n),
            's' => try writeWidenedColumn(i16, i32, writer, allocator, col_idx, arr, n),
            'S' => try writeWidenedColumn(u16, i32, writer, allocator, col_idx, arr, n),
            'i' => try writeTypedColumn(i32, writer, allocator, col_idx, arr, n),
            'I' => try writeWidenedColumn(u32, i64, writer, allocator, col_idx, arr, n),
            'l' => try writeTypedColumn(i64, writer, allocator, col_idx, arr, n),
            'L' => try writeWidenedColumn(u64, i64, writer, allocator, col_idx, arr, n),
            'f' => try writeTypedColumn(f32, writer, allocator, col_idx, arr, n),
            'g' => try writeTypedColumn(f64, writer, allocator, col_idx, arr, n),
            'e' => try writeFixedByteArrayColumn(writer, allocator, col_idx, arr, n, 2),
            'u', 'z' => try writeByteArrayColumn(writer, allocator, col_idx, arr, n, false),
            'U', 'Z' => try writeByteArrayColumn(writer, allocator, col_idx, arr, n, true),
            else => return error.TypeMismatch,
        }
        return;
    }

    // Date32 (days as i32)
    if (std.mem.eql(u8, fmt, "tdD")) {
        try writeTypedColumn(i32, writer, allocator, col_idx, arr, n);
        return;
    }

    // Date64 (milliseconds as i64 → convert to days as i32)
    if (std.mem.eql(u8, fmt, "tdm")) {
        try writeDate64Column(writer, allocator, col_idx, arr, n);
        return;
    }

    // Timestamps → INT64
    if (fmt.len >= 4 and std.mem.eql(u8, fmt[0..2], "ts")) {
        try writeTypedColumn(i64, writer, allocator, col_idx, arr, n);
        return;
    }

    // Time32 millis → INT32
    if (std.mem.eql(u8, fmt, "ttm")) {
        try writeTypedColumn(i32, writer, allocator, col_idx, arr, n);
        return;
    }

    // Time32 seconds → INT32 millis (multiply by 1000)
    if (std.mem.eql(u8, fmt, "tts")) {
        try writeTime32SecondsColumn(writer, allocator, col_idx, arr, n);
        return;
    }
    if (std.mem.eql(u8, fmt, "ttu") or std.mem.eql(u8, fmt, "ttn")) {
        try writeTypedColumn(i64, writer, allocator, col_idx, arr, n);
        return;
    }

    // Fixed-width binary: w:{N}
    if (fmt.len >= 2 and fmt[0] == 'w' and fmt[1] == ':') {
        const type_len_i = std.fmt.parseInt(i32, fmt[2..], 10) catch return error.TypeMismatch;
        const type_len = safe.castTo(usize, type_len_i) catch return error.IntegerOverflow;
        try writeFixedByteArrayColumn(writer, allocator, col_idx, arr, n, type_len);
        return;
    }

    // Decimal
    if (fmt.len >= 4 and fmt[0] == 'd' and fmt[1] == ':') {
        // Determine backing type from the Writer's column def
        if (col_idx < writer.columns.len) {
            const col_def = &writer.columns[col_idx];
            switch (col_def.type_) {
                .int32 => {
                    try writeTypedColumn(i32, writer, allocator, col_idx, arr, n);
                    return;
                },
                .int64 => {
                    try writeTypedColumn(i64, writer, allocator, col_idx, arr, n);
                    return;
                },
                .fixed_len_byte_array => {
                    const tl = safe.castTo(usize, col_def.type_length orelse return error.InvalidFixedLength) catch return error.IntegerOverflow;
                    try writeFixedByteArrayColumn(writer, allocator, col_idx, arr, n, tl);
                    return;
                },
                else => {},
            }
        }
        return error.TypeMismatch;
    }

    // LIST
    if (std.mem.eql(u8, fmt, "+l")) {
        try writeListFromArrow(writer, allocator, col_idx, arr, sch);
        return;
    }

    // STRUCT
    if (std.mem.eql(u8, fmt, "+s")) {
        try writeStructFromArrow(writer, allocator, col_idx, arr, sch);
        return;
    }

    // MAP
    if (std.mem.eql(u8, fmt, "+m")) {
        try writeMapFromArrow(writer, allocator, col_idx, arr, sch);
        return;
    }

    return error.TypeMismatch;
}

// ============================================================================
// Nested Write: Arrow LIST/STRUCT/MAP → Parquet
// ============================================================================

fn writeNestedColumnFromArrow(
    writer: *Writer,
    allocator: Allocator,
    col_idx: usize,
    arr: ArrowArray,
    sch: ArrowSchema,
) WriterError!void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer _ = arena.reset(.free_all);
    const arena_alloc = arena.allocator();

    const n = safe.castTo(usize, arr.length) catch return error.IntegerOverflow;
    var values = arena_alloc.alloc(Value, n) catch return error.OutOfMemory;

    for (0..n) |i| {
        values[i] = arrowToValue(arena_alloc, arr, sch, i) catch return error.TypeMismatch;
    }

    try writer.writeNestedColumn(col_idx, values);
}

const ArrowValueError = error{ InvalidSchema, IntegerOverflow, UnsupportedType, OutOfMemory };

fn arrowToValue(allocator: Allocator, arr: ArrowArray, sch: ArrowSchema, idx: usize) ArrowValueError!Value {
    const fmt = std.mem.sliceTo(sch.format, 0);
    const validity: ?[*]const u8 = if (arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;
    const n = safe.castTo(usize, arr.length) catch return error.InvalidSchema;

    if (validity) |v| {
        if (!arrow.getBit(v[0 .. (n + 7) / 8], idx)) return .null_val;
    }

    if (std.mem.eql(u8, fmt, "+l") or std.mem.eql(u8, fmt, "+L")) {
        return arrowListToValue(allocator, arr, sch, idx);
    }
    if (std.mem.eql(u8, fmt, "+s")) {
        return arrowStructToValue(allocator, arr, sch, idx);
    }
    if (std.mem.eql(u8, fmt, "+m")) {
        return arrowMapToValue(allocator, arr, sch, idx);
    }

    return arrowLeafToValue(arr, sch, fmt, idx);
}

fn arrowLeafToValue(arr: ArrowArray, sch: ArrowSchema, fmt: []const u8, idx: usize) ArrowValueError!Value {
    _ = sch;
    if (fmt.len == 1) {
        switch (fmt[0]) {
            'b' => {
                const data: [*]const u8 = @ptrCast(@alignCast(arr.buffers[1].?));
                const n = safe.castTo(usize, arr.length) catch return error.InvalidSchema;
                return .{ .bool_val = arrow.getBit(data[0 .. (n + 7) / 8], idx) };
            },
            'c' => {
                const data: [*]const i8 = @ptrCast(@alignCast(arr.buffers[1].?));
                return .{ .int32_val = safe.castTo(i32, data[idx]) catch unreachable }; // i8 always fits in i32
            },
            'C' => {
                const data: [*]const u8 = @ptrCast(@alignCast(arr.buffers[1].?));
                return .{ .int32_val = safe.castTo(i32, data[idx]) catch unreachable }; // u8 always fits in i32
            },
            's' => {
                const data: [*]const i16 = @ptrCast(@alignCast(arr.buffers[1].?));
                return .{ .int32_val = safe.castTo(i32, data[idx]) catch unreachable }; // i16 always fits in i32
            },
            'S' => {
                const data: [*]const u16 = @ptrCast(@alignCast(arr.buffers[1].?));
                return .{ .int32_val = safe.castTo(i32, data[idx]) catch unreachable }; // u16 always fits in i32
            },
            'i' => {
                const data: [*]const i32 = @ptrCast(@alignCast(arr.buffers[1].?));
                return .{ .int32_val = data[idx] };
            },
            'I' => {
                const data: [*]const u32 = @ptrCast(@alignCast(arr.buffers[1].?));
                return .{ .int64_val = safe.castTo(i64, data[idx]) catch unreachable }; // u32 always fits in i64
            },
            'l' => {
                const data: [*]const i64 = @ptrCast(@alignCast(arr.buffers[1].?));
                return .{ .int64_val = data[idx] };
            },
            'L' => {
                const data: [*]const u64 = @ptrCast(@alignCast(arr.buffers[1].?));
                return .{ .int64_val = try safe.castTo(i64, data[idx]) };
            },
            'f' => {
                const data: [*]const f32 = @ptrCast(@alignCast(arr.buffers[1].?));
                return .{ .float_val = data[idx] };
            },
            'g' => {
                const data: [*]const f64 = @ptrCast(@alignCast(arr.buffers[1].?));
                return .{ .double_val = data[idx] };
            },
            'u', 'z' => {
                const offsets: [*]const i32 = @ptrCast(@alignCast(arr.buffers[1].?));
                const data: [*]const u8 = if (arr.buffers[2]) |b| @ptrCast(@alignCast(b)) else @as([*]const u8, &[_]u8{});
                const s: usize = try safe.cast(offsets[idx]);
                const e: usize = try safe.cast(offsets[idx + 1]);
                return .{ .bytes_val = data[s..e] };
            },
            'U', 'Z' => {
                const offsets: [*]const i64 = @ptrCast(@alignCast(arr.buffers[1].?));
                const data: [*]const u8 = if (arr.buffers[2]) |b| @ptrCast(@alignCast(b)) else @as([*]const u8, &[_]u8{});
                const s: usize = try safe.cast(offsets[idx]);
                const e: usize = try safe.cast(offsets[idx + 1]);
                return .{ .bytes_val = data[s..e] };
            },
            'e' => {
                const data: [*]const u8 = @ptrCast(@alignCast(arr.buffers[1].?));
                return .{ .fixed_bytes_val = data[idx * 2 ..][0..2] };
            },
            else => return error.UnsupportedType,
        }
    }

    // Multi-char: temporal types → int32/int64 values
    if (std.mem.eql(u8, fmt, "tdD")) {
        const data: [*]const i32 = @ptrCast(@alignCast(arr.buffers[1].?));
        return .{ .int32_val = data[idx] };
    }
    if (std.mem.eql(u8, fmt, "tdm")) {
        const data: [*]const i64 = @ptrCast(@alignCast(arr.buffers[1].?));
        return .{ .int32_val = safe.castTo(i32, @divTrunc(data[idx], 86_400_000)) catch return error.IntegerOverflow };
    }
    if (std.mem.eql(u8, fmt, "ttm")) {
        const data: [*]const i32 = @ptrCast(@alignCast(arr.buffers[1].?));
        return .{ .int32_val = data[idx] };
    }
    if (std.mem.eql(u8, fmt, "tts")) {
        const data: [*]const i32 = @ptrCast(@alignCast(arr.buffers[1].?));
        return .{ .int32_val = std.math.mul(i32, data[idx], 1000) catch return error.IntegerOverflow };
    }
    if (std.mem.eql(u8, fmt, "ttu") or std.mem.eql(u8, fmt, "ttn")) {
        const data: [*]const i64 = @ptrCast(@alignCast(arr.buffers[1].?));
        return .{ .int64_val = data[idx] };
    }
    if (fmt.len >= 4 and std.mem.eql(u8, fmt[0..2], "ts")) {
        const data: [*]const i64 = @ptrCast(@alignCast(arr.buffers[1].?));
        return .{ .int64_val = data[idx] };
    }
    if (fmt.len >= 2 and fmt[0] == 'w' and fmt[1] == ':') {
        const type_len = std.fmt.parseInt(usize, fmt[2..], 10) catch return error.InvalidSchema;
        const data: [*]const u8 = @ptrCast(@alignCast(arr.buffers[1].?));
        return .{ .fixed_bytes_val = data[idx * type_len ..][0..type_len] };
    }
    if (fmt.len >= 4 and fmt[0] == 'd' and fmt[1] == ':') {
        // Decimal: stored as int32, int64, or fixed bytes depending on precision
        // We infer from Arrow's buffer layout (int128 for decimal128, etc.)
        const params = fmt[2..];
        var parts = std.mem.splitScalar(u8, params, ',');
        const prec_str = parts.next() orelse return error.InvalidSchema;
        _ = parts.next() orelse return error.InvalidSchema;
        const precision = std.fmt.parseInt(i32, prec_str, 10) catch return error.InvalidSchema;

        if (parts.next()) |bw_str| {
            const bw = std.fmt.parseInt(usize, bw_str, 10) catch return error.InvalidSchema;
            const data: [*]const u8 = @ptrCast(@alignCast(arr.buffers[1].?));
            return .{ .fixed_bytes_val = data[idx * bw ..][0..bw] };
        } else if (precision <= 9) {
            const data: [*]const i32 = @ptrCast(@alignCast(arr.buffers[1].?));
            return .{ .int32_val = data[idx] };
        } else if (precision <= 18) {
            const data: [*]const i64 = @ptrCast(@alignCast(arr.buffers[1].?));
            return .{ .int64_val = data[idx] };
        } else {
            const bw = decimalByteLength(precision);
            const data: [*]const u8 = @ptrCast(@alignCast(arr.buffers[1].?));
            return .{ .fixed_bytes_val = data[idx * bw ..][0..bw] };
        }
    }

    return error.UnsupportedType;
}

fn arrowListToValue(allocator: Allocator, arr: ArrowArray, sch: ArrowSchema, idx: usize) ArrowValueError!Value {
    const offsets: [*]const i32 = @ptrCast(@alignCast(arr.buffers[1].?));
    const child_arr_ptr: [*]*ArrowArray = arr.children orelse return error.InvalidSchema;
    const child_sch_ptr: [*]*ArrowSchema = sch.children orelse return error.InvalidSchema;
    const child_arr = child_arr_ptr[0];
    const child_sch = child_sch_ptr[0];

    const start = safe.castTo(usize, offsets[idx]) catch return error.IntegerOverflow;
    const end = safe.castTo(usize, offsets[idx + 1]) catch return error.IntegerOverflow;
    const len = end - start;

    var elems = allocator.alloc(Value, len) catch return error.OutOfMemory;
    for (0..len) |j| {
        elems[j] = try arrowToValue(allocator, child_arr.*, child_sch.*, start + j);
    }
    return .{ .list_val = elems };
}

fn arrowStructToValue(allocator: Allocator, arr: ArrowArray, sch: ArrowSchema, idx: usize) ArrowValueError!Value {
    const nc = safe.castTo(usize, arr.n_children) catch return error.InvalidSchema;
    const child_arrs: [*]*ArrowArray = arr.children orelse return error.InvalidSchema;
    const child_schs: [*]*ArrowSchema = sch.children orelse return error.InvalidSchema;

    var fields = allocator.alloc(Value.FieldValue, nc) catch return error.OutOfMemory;
    for (0..nc) |i| {
        const child_name = if (child_schs[i].name) |n| std.mem.sliceTo(n, 0) else "";
        fields[i] = .{
            .name = child_name,
            .value = try arrowToValue(allocator, child_arrs[i].*, child_schs[i].*, idx),
        };
    }
    return .{ .struct_val = fields };
}

fn arrowMapToValue(allocator: Allocator, arr: ArrowArray, sch: ArrowSchema, idx: usize) ArrowValueError!Value {
    const offsets: [*]const i32 = @ptrCast(@alignCast(arr.buffers[1].?));
    const entries_arr_ptr: [*]*ArrowArray = arr.children orelse return error.InvalidSchema;
    const entries_sch_ptr: [*]*ArrowSchema = sch.children orelse return error.InvalidSchema;
    const entries_arr = entries_arr_ptr[0];
    const entries_sch = entries_sch_ptr[0];

    const kv_arrs: [*]*ArrowArray = entries_arr.children orelse return error.InvalidSchema;
    const kv_schs: [*]*ArrowSchema = entries_sch.children orelse return error.InvalidSchema;
    const key_arr = kv_arrs[0];
    const val_arr = kv_arrs[1];
    const key_sch = kv_schs[0];
    const val_sch = kv_schs[1];

    const start = safe.castTo(usize, offsets[idx]) catch return error.IntegerOverflow;
    const end = safe.castTo(usize, offsets[idx + 1]) catch return error.IntegerOverflow;
    const len = end - start;

    var entries = allocator.alloc(Value.MapEntryValue, len) catch return error.OutOfMemory;
    for (0..len) |j| {
        entries[j] = .{
            .key = try arrowToValue(allocator, key_arr.*, key_sch.*, start + j),
            .value = try arrowToValue(allocator, val_arr.*, val_sch.*, start + j),
        };
    }
    return .{ .map_val = entries };
}

const list_encoder = @import("list_encoder.zig");
const map_encoder = @import("map_encoder.zig");
const MapEntry = map_encoder.MapEntry;

fn writeListFromArrow(
    writer: *Writer,
    allocator: Allocator,
    col_idx: usize,
    arr: ArrowArray,
    sch: ArrowSchema,
) WriterError!void {
    if (arr.n_children != 1) return error.TypeMismatch;
    const children: [*]*ArrowArray = arr.children orelse return error.TypeMismatch;
    const child_arr = children[0];
    const schema_children: [*]*ArrowSchema = sch.children orelse return error.TypeMismatch;
    const child_sch = schema_children[0];

    const child_fmt = std.mem.sliceTo(child_sch.format, 0);
    const n = safe.castTo(usize, arr.length) catch return error.IntegerOverflow;

    if (child_fmt.len == 1) {
        switch (child_fmt[0]) {
            'b' => try writeListColumnBool(writer, allocator, col_idx, arr, child_arr.*, n),
            'c' => try writeListColumnWidened(i8, i32, writer, allocator, col_idx, arr, child_arr.*, n),
            'C' => try writeListColumnWidened(u8, i32, writer, allocator, col_idx, arr, child_arr.*, n),
            's' => try writeListColumnWidened(i16, i32, writer, allocator, col_idx, arr, child_arr.*, n),
            'S' => try writeListColumnWidened(u16, i32, writer, allocator, col_idx, arr, child_arr.*, n),
            'i' => try writeListColumnTyped(i32, writer, allocator, col_idx, arr, child_arr.*, n),
            'I' => try writeListColumnWidened(u32, i64, writer, allocator, col_idx, arr, child_arr.*, n),
            'l' => try writeListColumnTyped(i64, writer, allocator, col_idx, arr, child_arr.*, n),
            'L' => try writeListColumnWidened(u64, i64, writer, allocator, col_idx, arr, child_arr.*, n),
            'e' => try writeListColumnFixedByteArray(writer, allocator, col_idx, arr, child_arr.*, n, 2),
            'f' => try writeListColumnTyped(f32, writer, allocator, col_idx, arr, child_arr.*, n),
            'g' => try writeListColumnTyped(f64, writer, allocator, col_idx, arr, child_arr.*, n),
            'u', 'z' => try writeListColumnByteArray(writer, allocator, col_idx, arr, child_arr.*, n, false),
            'U', 'Z' => try writeListColumnByteArray(writer, allocator, col_idx, arr, child_arr.*, n, true),
            else => return error.TypeMismatch,
        }
        return;
    }

    // Multi-char element formats: date, time, timestamp, decimal, fixed binary
    if (std.mem.eql(u8, child_fmt, "tdD")) {
        try writeListColumnTyped(i32, writer, allocator, col_idx, arr, child_arr.*, n);
        return;
    }
    if (std.mem.eql(u8, child_fmt, "tdm")) {
        try writeListColumnDate64(writer, allocator, col_idx, arr, child_arr.*, n);
        return;
    }
    if (std.mem.eql(u8, child_fmt, "ttm")) {
        try writeListColumnTyped(i32, writer, allocator, col_idx, arr, child_arr.*, n);
        return;
    }
    if (std.mem.eql(u8, child_fmt, "tts")) {
        try writeListColumnTime32Seconds(writer, allocator, col_idx, arr, child_arr.*, n);
        return;
    }
    if (std.mem.eql(u8, child_fmt, "ttu") or std.mem.eql(u8, child_fmt, "ttn")) {
        try writeListColumnTyped(i64, writer, allocator, col_idx, arr, child_arr.*, n);
        return;
    }
    if (child_fmt.len >= 4 and std.mem.eql(u8, child_fmt[0..2], "ts")) {
        try writeListColumnTyped(i64, writer, allocator, col_idx, arr, child_arr.*, n);
        return;
    }
    if (child_fmt.len >= 2 and child_fmt[0] == 'w' and child_fmt[1] == ':') {
        const type_len = std.fmt.parseInt(usize, child_fmt[2..], 10) catch return error.TypeMismatch;
        try writeListColumnFixedByteArray(writer, allocator, col_idx, arr, child_arr.*, n, type_len);
        return;
    }
    if (child_fmt.len >= 4 and child_fmt[0] == 'd' and child_fmt[1] == ':') {
        try writeListColumnDecimal(writer, allocator, col_idx, arr, child_arr.*, n, child_fmt);
        return;
    }

    return error.TypeMismatch;
}

fn writeListColumnTyped(
    comptime T: type,
    writer: *Writer,
    allocator: Allocator,
    col_idx: usize,
    parent_arr: ArrowArray,
    child_arr: ArrowArray,
    n: usize,
) WriterError!void {
    const offsets: [*]const i32 = @ptrCast(@alignCast(parent_arr.buffers[1].?));
    const parent_validity: ?[*]const u8 = if (parent_arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;
    const child_data: [*]const T = @ptrCast(@alignCast(child_arr.buffers[1].?));
    const child_validity: ?[*]const u8 = if (child_arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;
    const child_len = safe.castTo(usize, child_arr.length) catch return error.IntegerOverflow;

    var lists = allocator.alloc(Optional([]const Optional(T)), n) catch return error.OutOfMemory;
    defer {
        for (lists) |l| switch (l) {
            .value => |elems| allocator.free(elems),
            .null_value => {},
        };
        allocator.free(lists);
    }

    for (0..n) |i| {
        if (parent_validity != null and !arrow.getBit(parent_validity.?[0 .. (n + 7) / 8], i)) {
            lists[i] = .null_value;
        } else {
            const start = safe.castTo(usize, offsets[i]) catch return error.IntegerOverflow;
            const end = safe.castTo(usize, offsets[i + 1]) catch return error.IntegerOverflow;
            const len = end - start;
            var elems = allocator.alloc(Optional(T), len) catch return error.OutOfMemory;
            for (0..len) |j| {
                const idx = start + j;
                if (idx >= child_len) {
                    elems[j] = .null_value;
                } else if (child_validity != null and !arrow.getBit(child_validity.?[0 .. (child_len + 7) / 8], idx)) {
                    elems[j] = .null_value;
                } else {
                    elems[j] = .{ .value = child_data[idx] };
                }
            }
            lists[i] = .{ .value = elems };
        }
    }

    try writer.writeListColumn(T, col_idx, lists);
}

fn writeListColumnBool(
    writer: *Writer,
    allocator: Allocator,
    col_idx: usize,
    parent_arr: ArrowArray,
    child_arr: ArrowArray,
    n: usize,
) WriterError!void {
    const offsets: [*]const i32 = @ptrCast(@alignCast(parent_arr.buffers[1].?));
    const parent_validity: ?[*]const u8 = if (parent_arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;
    const child_data_bits: [*]const u8 = @ptrCast(@alignCast(child_arr.buffers[1].?));
    const child_validity: ?[*]const u8 = if (child_arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;
    const child_len = safe.castTo(usize, child_arr.length) catch return error.IntegerOverflow;

    var lists = allocator.alloc(Optional([]const Optional(bool)), n) catch return error.OutOfMemory;
    defer {
        for (lists) |l| switch (l) {
            .value => |elems| allocator.free(elems),
            .null_value => {},
        };
        allocator.free(lists);
    }

    for (0..n) |i| {
        if (parent_validity != null and !arrow.getBit(parent_validity.?[0 .. (n + 7) / 8], i)) {
            lists[i] = .null_value;
        } else {
            const start = safe.castTo(usize, offsets[i]) catch return error.IntegerOverflow;
            const end = safe.castTo(usize, offsets[i + 1]) catch return error.IntegerOverflow;
            const len = end - start;
            var elems = allocator.alloc(Optional(bool), len) catch return error.OutOfMemory;
            for (0..len) |j| {
                const idx = start + j;
                if (idx >= child_len) {
                    elems[j] = .null_value;
                } else if (child_validity != null and !arrow.getBit(child_validity.?[0 .. (child_len + 7) / 8], idx)) {
                    elems[j] = .null_value;
                } else {
                    elems[j] = .{ .value = arrow.getBit(child_data_bits[0 .. (child_len + 7) / 8], idx) };
                }
            }
            lists[i] = .{ .value = elems };
        }
    }

    try writer.writeListColumn(bool, col_idx, lists);
}

fn writeListColumnByteArray(
    writer: *Writer,
    allocator: Allocator,
    col_idx: usize,
    parent_arr: ArrowArray,
    child_arr: ArrowArray,
    n: usize,
    large: bool,
) WriterError!void {
    const offsets: [*]const i32 = @ptrCast(@alignCast(parent_arr.buffers[1].?));
    const parent_validity: ?[*]const u8 = if (parent_arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;

    const child_data: [*]const u8 = if (child_arr.buffers[2]) |b| @ptrCast(@alignCast(b)) else @as([*]const u8, &[_]u8{});
    const child_validity: ?[*]const u8 = if (child_arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;
    const child_len = safe.castTo(usize, child_arr.length) catch return error.IntegerOverflow;

    var lists = allocator.alloc(Optional([]const Optional([]const u8)), n) catch return error.OutOfMemory;
    defer {
        for (lists) |l| switch (l) {
            .value => |elems| allocator.free(elems),
            .null_value => {},
        };
        allocator.free(lists);
    }

    for (0..n) |i| {
        if (parent_validity != null and !arrow.getBit(parent_validity.?[0 .. (n + 7) / 8], i)) {
            lists[i] = .null_value;
        } else {
            const start = safe.castTo(usize, offsets[i]) catch return error.IntegerOverflow;
            const end = safe.castTo(usize, offsets[i + 1]) catch return error.IntegerOverflow;
            const len = end - start;
            var elems = allocator.alloc(Optional([]const u8), len) catch return error.OutOfMemory;
            for (0..len) |j| {
                const idx = start + j;
                if (idx >= child_len) {
                    elems[j] = .null_value;
                } else if (child_validity != null and !arrow.getBit(child_validity.?[0 .. (child_len + 7) / 8], idx)) {
                    elems[j] = .null_value;
                } else {
                    if (large) {
                        const child_offsets_64: [*]const i64 = @ptrCast(@alignCast(child_arr.buffers[1].?));
                        const s = safe.castTo(usize, child_offsets_64[idx]) catch return error.IntegerOverflow;
                        const e = safe.castTo(usize, child_offsets_64[idx + 1]) catch return error.IntegerOverflow;
                        elems[j] = .{ .value = child_data[s..e] };
                    } else {
                        const child_offsets_32: [*]const i32 = @ptrCast(@alignCast(child_arr.buffers[1].?));
                        const s = safe.castTo(usize, child_offsets_32[idx]) catch return error.IntegerOverflow;
                        const e = safe.castTo(usize, child_offsets_32[idx + 1]) catch return error.IntegerOverflow;
                        elems[j] = .{ .value = child_data[s..e] };
                    }
                }
            }
            lists[i] = .{ .value = elems };
        }
    }

    try writer.writeListColumn([]const u8, col_idx, lists);
}

fn writeListColumnWidened(
    comptime Src: type,
    comptime Dst: type,
    writer: *Writer,
    allocator: Allocator,
    col_idx: usize,
    parent_arr: ArrowArray,
    child_arr: ArrowArray,
    n: usize,
) WriterError!void {
    const offsets: [*]const i32 = @ptrCast(@alignCast(parent_arr.buffers[1].?));
    const parent_validity: ?[*]const u8 = if (parent_arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;
    const child_data: [*]const Src = @ptrCast(@alignCast(child_arr.buffers[1].?));
    const child_validity: ?[*]const u8 = if (child_arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;
    const child_len = safe.castTo(usize, child_arr.length) catch return error.IntegerOverflow;

    var lists = allocator.alloc(Optional([]const Optional(Dst)), n) catch return error.OutOfMemory;
    defer {
        for (lists) |l| switch (l) {
            .value => |elems| allocator.free(elems),
            .null_value => {},
        };
        allocator.free(lists);
    }

    for (0..n) |i| {
        if (parent_validity != null and !arrow.getBit(parent_validity.?[0 .. (n + 7) / 8], i)) {
            lists[i] = .null_value;
        } else {
            const start = safe.castTo(usize, offsets[i]) catch return error.IntegerOverflow;
            const end = safe.castTo(usize, offsets[i + 1]) catch return error.IntegerOverflow;
            const len = end - start;
            var elems = allocator.alloc(Optional(Dst), len) catch return error.OutOfMemory;
            for (0..len) |j| {
                const idx = start + j;
                if (idx >= child_len) {
                    elems[j] = .null_value;
                } else if (child_validity != null and !arrow.getBit(child_validity.?[0 .. (child_len + 7) / 8], idx)) {
                    elems[j] = .null_value;
                } else {
                    elems[j] = .{ .value = safe.castTo(Dst, child_data[idx]) catch return error.IntegerOverflow };
                }
            }
            lists[i] = .{ .value = elems };
        }
    }

    try writer.writeListColumn(Dst, col_idx, lists);
}

fn writeListColumnFixedByteArray(
    writer: *Writer,
    allocator: Allocator,
    col_idx: usize,
    parent_arr: ArrowArray,
    child_arr: ArrowArray,
    n: usize,
    type_len: usize,
) WriterError!void {
    const offsets: [*]const i32 = @ptrCast(@alignCast(parent_arr.buffers[1].?));
    const parent_validity: ?[*]const u8 = if (parent_arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;
    const child_data: [*]const u8 = @ptrCast(@alignCast(child_arr.buffers[1].?));
    const child_validity: ?[*]const u8 = if (child_arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;
    const child_len = safe.castTo(usize, child_arr.length) catch return error.IntegerOverflow;

    var lists = allocator.alloc(Optional([]const Optional([]const u8)), n) catch return error.OutOfMemory;
    defer {
        for (lists) |l| switch (l) {
            .value => |elems| allocator.free(elems),
            .null_value => {},
        };
        allocator.free(lists);
    }

    for (0..n) |i| {
        if (parent_validity != null and !arrow.getBit(parent_validity.?[0 .. (n + 7) / 8], i)) {
            lists[i] = .null_value;
        } else {
            const start = safe.castTo(usize, offsets[i]) catch return error.IntegerOverflow;
            const end = safe.castTo(usize, offsets[i + 1]) catch return error.IntegerOverflow;
            const len = end - start;
            var elems = allocator.alloc(Optional([]const u8), len) catch return error.OutOfMemory;
            for (0..len) |j| {
                const idx = start + j;
                if (idx >= child_len) {
                    elems[j] = .null_value;
                } else if (child_validity != null and !arrow.getBit(child_validity.?[0 .. (child_len + 7) / 8], idx)) {
                    elems[j] = .null_value;
                } else {
                    elems[j] = .{ .value = child_data[idx * type_len ..][0..type_len] };
                }
            }
            lists[i] = .{ .value = elems };
        }
    }

    try writer.writeListColumn([]const u8, col_idx, lists);
}

fn writeListColumnDate64(
    writer: *Writer,
    allocator: Allocator,
    col_idx: usize,
    parent_arr: ArrowArray,
    child_arr: ArrowArray,
    n: usize,
) WriterError!void {
    const offsets: [*]const i32 = @ptrCast(@alignCast(parent_arr.buffers[1].?));
    const parent_validity: ?[*]const u8 = if (parent_arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;
    const child_data: [*]const i64 = @ptrCast(@alignCast(child_arr.buffers[1].?));
    const child_validity: ?[*]const u8 = if (child_arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;
    const child_len = safe.castTo(usize, child_arr.length) catch return error.IntegerOverflow;

    const millis_per_day: i64 = 86_400_000;
    var lists = allocator.alloc(Optional([]const Optional(i32)), n) catch return error.OutOfMemory;
    defer {
        for (lists) |l| switch (l) {
            .value => |elems| allocator.free(elems),
            .null_value => {},
        };
        allocator.free(lists);
    }

    for (0..n) |i| {
        if (parent_validity != null and !arrow.getBit(parent_validity.?[0 .. (n + 7) / 8], i)) {
            lists[i] = .null_value;
        } else {
            const start = safe.castTo(usize, offsets[i]) catch return error.IntegerOverflow;
            const end = safe.castTo(usize, offsets[i + 1]) catch return error.IntegerOverflow;
            const len = end - start;
            var elems = allocator.alloc(Optional(i32), len) catch return error.OutOfMemory;
            for (0..len) |j| {
                const idx = start + j;
                if (idx >= child_len) {
                    elems[j] = .null_value;
                } else if (child_validity != null and !arrow.getBit(child_validity.?[0 .. (child_len + 7) / 8], idx)) {
                    elems[j] = .null_value;
                } else {
                    const days = @divTrunc(child_data[idx], millis_per_day);
                    elems[j] = .{ .value = safe.castTo(i32, days) catch return error.IntegerOverflow };
                }
            }
            lists[i] = .{ .value = elems };
        }
    }

    try writer.writeListColumn(i32, col_idx, lists);
}

fn writeListColumnTime32Seconds(
    writer: *Writer,
    allocator: Allocator,
    col_idx: usize,
    parent_arr: ArrowArray,
    child_arr: ArrowArray,
    n: usize,
) WriterError!void {
    const offsets: [*]const i32 = @ptrCast(@alignCast(parent_arr.buffers[1].?));
    const parent_validity: ?[*]const u8 = if (parent_arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;
    const child_data: [*]const i32 = @ptrCast(@alignCast(child_arr.buffers[1].?));
    const child_validity: ?[*]const u8 = if (child_arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;
    const child_len = safe.castTo(usize, child_arr.length) catch return error.IntegerOverflow;

    var lists = allocator.alloc(Optional([]const Optional(i32)), n) catch return error.OutOfMemory;
    defer {
        for (lists) |l| switch (l) {
            .value => |elems| allocator.free(elems),
            .null_value => {},
        };
        allocator.free(lists);
    }

    for (0..n) |i| {
        if (parent_validity != null and !arrow.getBit(parent_validity.?[0 .. (n + 7) / 8], i)) {
            lists[i] = .null_value;
        } else {
            const start = safe.castTo(usize, offsets[i]) catch return error.IntegerOverflow;
            const end = safe.castTo(usize, offsets[i + 1]) catch return error.IntegerOverflow;
            const len = end - start;
            var elems = allocator.alloc(Optional(i32), len) catch return error.OutOfMemory;
            for (0..len) |j| {
                const idx = start + j;
                if (idx >= child_len) {
                    elems[j] = .null_value;
                } else if (child_validity != null and !arrow.getBit(child_validity.?[0 .. (child_len + 7) / 8], idx)) {
                    elems[j] = .null_value;
                } else {
                    elems[j] = .{ .value = std.math.mul(i32, child_data[idx], 1000) catch return error.IntegerOverflow };
                }
            }
            lists[i] = .{ .value = elems };
        }
    }

    try writer.writeListColumn(i32, col_idx, lists);
}

fn writeListColumnDecimal(
    writer: *Writer,
    allocator: Allocator,
    col_idx: usize,
    parent_arr: ArrowArray,
    child_arr: ArrowArray,
    n: usize,
    child_fmt: []const u8,
) WriterError!void {
    if (col_idx >= writer.columns.len) return error.TypeMismatch;
    const col_def = &writer.columns[col_idx];
    switch (col_def.type_) {
        .int32 => try writeListColumnTyped(i32, writer, allocator, col_idx, parent_arr, child_arr, n),
        .int64 => try writeListColumnTyped(i64, writer, allocator, col_idx, parent_arr, child_arr, n),
        .fixed_len_byte_array => {
            const tl = safe.castTo(usize, col_def.type_length orelse return error.InvalidFixedLength) catch return error.IntegerOverflow;
            try writeListColumnFixedByteArray(writer, allocator, col_idx, parent_arr, child_arr, n, tl);
        },
        else => {
            _ = child_fmt;
            return error.TypeMismatch;
        },
    }
}

fn writeStructFromArrow(
    writer: *Writer,
    allocator: Allocator,
    col_idx: usize,
    arr: ArrowArray,
    sch: ArrowSchema,
) WriterError!void {
    const nc = safe.castTo(usize, arr.n_children) catch return error.IntegerOverflow;
    const n = safe.castTo(usize, arr.length) catch return error.IntegerOverflow;
    const arr_children: [*]*ArrowArray = arr.children orelse return error.TypeMismatch;
    const sch_children: [*]*ArrowSchema = sch.children orelse return error.TypeMismatch;

    // Compute parent nulls from struct validity
    const parent_validity: ?[*]const u8 = if (arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;
    var parent_nulls = allocator.alloc(bool, n) catch return error.OutOfMemory;
    defer allocator.free(parent_nulls);
    for (0..n) |i| {
        parent_nulls[i] = if (parent_validity) |pv| !arrow.getBit(pv[0 .. (n + 7) / 8], i) else false;
    }

    for (0..nc) |field_idx| {
        const child_arr = arr_children[field_idx];
        const child_sch = sch_children[field_idx];
        const child_fmt = std.mem.sliceTo(child_sch.format, 0);

        try writeStructFieldFromArrow(writer, allocator, col_idx, field_idx, child_arr.*, child_fmt, n, parent_nulls);
    }
}

fn writeStructFieldFromArrow(
    writer: *Writer,
    allocator: Allocator,
    struct_col_idx: usize,
    field_idx: usize,
    child_arr: ArrowArray,
    child_fmt: []const u8,
    n: usize,
    parent_nulls: []const bool,
) WriterError!void {
    if (child_fmt.len == 1) {
        switch (child_fmt[0]) {
            'b' => try writeStructFieldBool(writer, allocator, struct_col_idx, field_idx, child_arr, n, parent_nulls),
            'c' => try writeStructFieldWidened(i8, i32, writer, allocator, struct_col_idx, field_idx, child_arr, n, parent_nulls),
            'C' => try writeStructFieldWidened(u8, i32, writer, allocator, struct_col_idx, field_idx, child_arr, n, parent_nulls),
            's' => try writeStructFieldWidened(i16, i32, writer, allocator, struct_col_idx, field_idx, child_arr, n, parent_nulls),
            'S' => try writeStructFieldWidened(u16, i32, writer, allocator, struct_col_idx, field_idx, child_arr, n, parent_nulls),
            'i' => try writeStructFieldTyped(i32, writer, allocator, struct_col_idx, field_idx, child_arr, n, parent_nulls),
            'I' => try writeStructFieldWidened(u32, i64, writer, allocator, struct_col_idx, field_idx, child_arr, n, parent_nulls),
            'l' => try writeStructFieldTyped(i64, writer, allocator, struct_col_idx, field_idx, child_arr, n, parent_nulls),
            'L' => try writeStructFieldWidened(u64, i64, writer, allocator, struct_col_idx, field_idx, child_arr, n, parent_nulls),
            'e' => try writeStructFieldFixedByteArray(writer, allocator, struct_col_idx, field_idx, child_arr, n, parent_nulls, 2),
            'f' => try writeStructFieldTyped(f32, writer, allocator, struct_col_idx, field_idx, child_arr, n, parent_nulls),
            'g' => try writeStructFieldTyped(f64, writer, allocator, struct_col_idx, field_idx, child_arr, n, parent_nulls),
            'u', 'z' => try writeStructFieldByteArray(writer, allocator, struct_col_idx, field_idx, child_arr, n, parent_nulls, false),
            'U', 'Z' => try writeStructFieldByteArray(writer, allocator, struct_col_idx, field_idx, child_arr, n, parent_nulls, true),
            else => return error.TypeMismatch,
        }
        return;
    }

    // Multi-char struct field formats
    if (std.mem.eql(u8, child_fmt, "tdD")) {
        try writeStructFieldTyped(i32, writer, allocator, struct_col_idx, field_idx, child_arr, n, parent_nulls);
        return;
    }
    if (std.mem.eql(u8, child_fmt, "tdm")) {
        try writeStructFieldDate64(writer, allocator, struct_col_idx, field_idx, child_arr, n, parent_nulls);
        return;
    }
    if (std.mem.eql(u8, child_fmt, "ttm")) {
        try writeStructFieldTyped(i32, writer, allocator, struct_col_idx, field_idx, child_arr, n, parent_nulls);
        return;
    }
    if (std.mem.eql(u8, child_fmt, "tts")) {
        try writeStructFieldTime32Seconds(writer, allocator, struct_col_idx, field_idx, child_arr, n, parent_nulls);
        return;
    }
    if (std.mem.eql(u8, child_fmt, "ttu") or std.mem.eql(u8, child_fmt, "ttn")) {
        try writeStructFieldTyped(i64, writer, allocator, struct_col_idx, field_idx, child_arr, n, parent_nulls);
        return;
    }
    if (child_fmt.len >= 4 and std.mem.eql(u8, child_fmt[0..2], "ts")) {
        try writeStructFieldTyped(i64, writer, allocator, struct_col_idx, field_idx, child_arr, n, parent_nulls);
        return;
    }
    if (child_fmt.len >= 2 and child_fmt[0] == 'w' and child_fmt[1] == ':') {
        const type_len = std.fmt.parseInt(usize, child_fmt[2..], 10) catch return error.TypeMismatch;
        try writeStructFieldFixedByteArray(writer, allocator, struct_col_idx, field_idx, child_arr, n, parent_nulls, type_len);
        return;
    }
    if (child_fmt.len >= 4 and child_fmt[0] == 'd' and child_fmt[1] == ':') {
        try writeStructFieldDecimal(writer, allocator, struct_col_idx, field_idx, child_arr, child_fmt, n, parent_nulls);
        return;
    }

    return error.TypeMismatch;
}

fn writeStructFieldTyped(
    comptime T: type,
    writer: *Writer,
    allocator: Allocator,
    struct_col_idx: usize,
    field_idx: usize,
    child_arr: ArrowArray,
    n: usize,
    parent_nulls: []const bool,
) WriterError!void {
    const data: [*]const T = @ptrCast(@alignCast(child_arr.buffers[1].?));
    const validity: ?[*]const u8 = if (child_arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;

    var values = allocator.alloc(?T, n) catch return error.OutOfMemory;
    defer allocator.free(values);

    for (0..n) |i| {
        if (validity != null and !arrow.getBit(validity.?[0 .. (n + 7) / 8], i)) {
            values[i] = null;
        } else {
            values[i] = data[i];
        }
    }

    try writer.writeStructField(T, struct_col_idx, field_idx, values, parent_nulls);
}

fn writeStructFieldByteArray(
    writer: *Writer,
    allocator: Allocator,
    struct_col_idx: usize,
    field_idx: usize,
    child_arr: ArrowArray,
    n: usize,
    parent_nulls: []const bool,
    large: bool,
) WriterError!void {
    const str_data: [*]const u8 = if (child_arr.buffers[2]) |b| @ptrCast(@alignCast(b)) else @as([*]const u8, &[_]u8{});
    const validity: ?[*]const u8 = if (child_arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;

    var values = allocator.alloc(?[]const u8, n) catch return error.OutOfMemory;
    defer allocator.free(values);

    if (large) {
        const str_offsets: [*]const i64 = @ptrCast(@alignCast(child_arr.buffers[1].?));
        for (0..n) |i| {
            if (validity != null and !arrow.getBit(validity.?[0 .. (n + 7) / 8], i)) {
                values[i] = null;
            } else {
                const s = safe.castTo(usize, str_offsets[i]) catch return error.IntegerOverflow;
                const e = safe.castTo(usize, str_offsets[i + 1]) catch return error.IntegerOverflow;
                values[i] = str_data[s..e];
            }
        }
    } else {
        const str_offsets: [*]const i32 = @ptrCast(@alignCast(child_arr.buffers[1].?));
        for (0..n) |i| {
            if (validity != null and !arrow.getBit(validity.?[0 .. (n + 7) / 8], i)) {
                values[i] = null;
            } else {
                const s = safe.castTo(usize, str_offsets[i]) catch return error.IntegerOverflow;
                const e = safe.castTo(usize, str_offsets[i + 1]) catch return error.IntegerOverflow;
                values[i] = str_data[s..e];
            }
        }
    }

    try writer.writeStructField([]const u8, struct_col_idx, field_idx, values, parent_nulls);
}

fn writeStructFieldBool(
    writer: *Writer,
    allocator: Allocator,
    struct_col_idx: usize,
    field_idx: usize,
    child_arr: ArrowArray,
    n: usize,
    parent_nulls: []const bool,
) WriterError!void {
    const data_bits: [*]const u8 = @ptrCast(@alignCast(child_arr.buffers[1].?));
    const validity: ?[*]const u8 = if (child_arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;

    var values = allocator.alloc(?bool, n) catch return error.OutOfMemory;
    defer allocator.free(values);

    for (0..n) |i| {
        if (validity != null and !arrow.getBit(validity.?[0 .. (n + 7) / 8], i)) {
            values[i] = null;
        } else {
            values[i] = arrow.getBit(data_bits[0 .. (n + 7) / 8], i);
        }
    }

    try writer.writeStructField(bool, struct_col_idx, field_idx, values, parent_nulls);
}

fn writeStructFieldWidened(
    comptime Src: type,
    comptime Dst: type,
    writer: *Writer,
    allocator: Allocator,
    struct_col_idx: usize,
    field_idx: usize,
    child_arr: ArrowArray,
    n: usize,
    parent_nulls: []const bool,
) WriterError!void {
    const data: [*]const Src = @ptrCast(@alignCast(child_arr.buffers[1].?));
    const validity: ?[*]const u8 = if (child_arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;

    var values = allocator.alloc(?Dst, n) catch return error.OutOfMemory;
    defer allocator.free(values);

    for (0..n) |i| {
        if (validity != null and !arrow.getBit(validity.?[0 .. (n + 7) / 8], i)) {
            values[i] = null;
        } else {
            values[i] = safe.castTo(Dst, data[i]) catch return error.IntegerOverflow;
        }
    }

    try writer.writeStructField(Dst, struct_col_idx, field_idx, values, parent_nulls);
}

fn writeStructFieldFixedByteArray(
    writer: *Writer,
    allocator: Allocator,
    struct_col_idx: usize,
    field_idx: usize,
    child_arr: ArrowArray,
    n: usize,
    parent_nulls: []const bool,
    type_len: usize,
) WriterError!void {
    const data: [*]const u8 = @ptrCast(@alignCast(child_arr.buffers[1].?));
    const validity: ?[*]const u8 = if (child_arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;

    var values = allocator.alloc(?[]const u8, n) catch return error.OutOfMemory;
    defer allocator.free(values);

    for (0..n) |i| {
        if (validity != null and !arrow.getBit(validity.?[0 .. (n + 7) / 8], i)) {
            values[i] = null;
        } else {
            values[i] = data[i * type_len ..][0..type_len];
        }
    }

    try writer.writeStructField([]const u8, struct_col_idx, field_idx, values, parent_nulls);
}

fn writeStructFieldDate64(
    writer: *Writer,
    allocator: Allocator,
    struct_col_idx: usize,
    field_idx: usize,
    child_arr: ArrowArray,
    n: usize,
    parent_nulls: []const bool,
) WriterError!void {
    const data: [*]const i64 = @ptrCast(@alignCast(child_arr.buffers[1].?));
    const validity: ?[*]const u8 = if (child_arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;

    var values = allocator.alloc(?i32, n) catch return error.OutOfMemory;
    defer allocator.free(values);

    const millis_per_day: i64 = 86_400_000;
    for (0..n) |i| {
        if (validity != null and !arrow.getBit(validity.?[0 .. (n + 7) / 8], i)) {
            values[i] = null;
        } else {
            const days = @divTrunc(data[i], millis_per_day);
            values[i] = safe.castTo(i32, days) catch return error.IntegerOverflow;
        }
    }

    try writer.writeStructField(i32, struct_col_idx, field_idx, values, parent_nulls);
}

fn writeStructFieldTime32Seconds(
    writer: *Writer,
    allocator: Allocator,
    struct_col_idx: usize,
    field_idx: usize,
    child_arr: ArrowArray,
    n: usize,
    parent_nulls: []const bool,
) WriterError!void {
    const data: [*]const i32 = @ptrCast(@alignCast(child_arr.buffers[1].?));
    const validity: ?[*]const u8 = if (child_arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;

    var values = allocator.alloc(?i32, n) catch return error.OutOfMemory;
    defer allocator.free(values);

    for (0..n) |i| {
        if (validity != null and !arrow.getBit(validity.?[0 .. (n + 7) / 8], i)) {
            values[i] = null;
        } else {
            values[i] = std.math.mul(i32, data[i], 1000) catch return error.IntegerOverflow;
        }
    }

    try writer.writeStructField(i32, struct_col_idx, field_idx, values, parent_nulls);
}

fn writeStructFieldDecimal(
    writer: *Writer,
    allocator: Allocator,
    struct_col_idx: usize,
    field_idx: usize,
    child_arr: ArrowArray,
    child_fmt: []const u8,
    n: usize,
    parent_nulls: []const bool,
) WriterError!void {
    _ = child_fmt;
    const phys_col_idx = struct_col_idx + 1 + field_idx;
    if (phys_col_idx >= writer.columns.len) return error.TypeMismatch;
    const col_def = &writer.columns[phys_col_idx];
    switch (col_def.type_) {
        .int32 => try writeStructFieldTyped(i32, writer, allocator, struct_col_idx, field_idx, child_arr, n, parent_nulls),
        .int64 => try writeStructFieldTyped(i64, writer, allocator, struct_col_idx, field_idx, child_arr, n, parent_nulls),
        .fixed_len_byte_array => {
            const tl = safe.castTo(usize, col_def.type_length orelse return error.InvalidFixedLength) catch return error.IntegerOverflow;
            try writeStructFieldFixedByteArray(writer, allocator, struct_col_idx, field_idx, child_arr, n, parent_nulls, tl);
        },
        else => return error.TypeMismatch,
    }
}

fn writeMapFromArrow(
    writer: *Writer,
    allocator: Allocator,
    col_idx: usize,
    arr: ArrowArray,
    sch: ArrowSchema,
) WriterError!void {
    if (arr.n_children != 1) return error.TypeMismatch;
    const arr_children: [*]*ArrowArray = arr.children orelse return error.TypeMismatch;
    const sch_children: [*]*ArrowSchema = sch.children orelse return error.TypeMismatch;

    const entries_arr = arr_children[0];
    const entries_sch = sch_children[0];
    if (entries_arr.n_children != 2) return error.TypeMismatch;

    const kv_arr: [*]*ArrowArray = entries_arr.children orelse return error.TypeMismatch;
    const kv_sch: [*]*ArrowSchema = entries_sch.children orelse return error.TypeMismatch;

    const key_arr = kv_arr[0];
    const val_arr = kv_arr[1];
    const key_fmt = std.mem.sliceTo(kv_sch[0].format, 0);
    const val_fmt = std.mem.sliceTo(kv_sch[1].format, 0);

    // Dispatch on key type, then value type
    if (key_fmt.len == 1 and (key_fmt[0] == 'u' or key_fmt[0] == 'z')) {
        try dispatchMapValue([]const u8, writer, allocator, col_idx, arr, key_arr.*, val_arr.*, val_fmt);
    } else if (key_fmt.len == 1 and key_fmt[0] == 'i') {
        try dispatchMapValue(i32, writer, allocator, col_idx, arr, key_arr.*, val_arr.*, val_fmt);
    } else if (key_fmt.len == 1 and key_fmt[0] == 'l') {
        try dispatchMapValue(i64, writer, allocator, col_idx, arr, key_arr.*, val_arr.*, val_fmt);
    } else {
        return error.TypeMismatch;
    }
}

fn dispatchMapValue(
    comptime K: type,
    writer: *Writer,
    allocator: Allocator,
    col_idx: usize,
    parent_arr: ArrowArray,
    key_arr: ArrowArray,
    val_arr: ArrowArray,
    val_fmt: []const u8,
) WriterError!void {
    if (val_fmt.len == 1) {
        switch (val_fmt[0]) {
            'b' => try writeMapColumnBool(K, writer, allocator, col_idx, parent_arr, key_arr, val_arr),
            'i' => try writeMapColumnTyped(K, i32, writer, allocator, col_idx, parent_arr, key_arr, val_arr),
            'l' => try writeMapColumnTyped(K, i64, writer, allocator, col_idx, parent_arr, key_arr, val_arr),
            'f' => try writeMapColumnTyped(K, f32, writer, allocator, col_idx, parent_arr, key_arr, val_arr),
            'g' => try writeMapColumnTyped(K, f64, writer, allocator, col_idx, parent_arr, key_arr, val_arr),
            'u', 'z' => try writeMapColumnTyped(K, []const u8, writer, allocator, col_idx, parent_arr, key_arr, val_arr),
            else => return error.TypeMismatch,
        }
    } else {
        return error.TypeMismatch;
    }
}

fn writeMapColumnTyped(
    comptime K: type,
    comptime V: type,
    writer: *Writer,
    allocator: Allocator,
    col_idx: usize,
    parent_arr: ArrowArray,
    key_arr: ArrowArray,
    val_arr: ArrowArray,
) WriterError!void {
    const n = safe.castTo(usize, parent_arr.length) catch return error.IntegerOverflow;
    const offsets: [*]const i32 = @ptrCast(@alignCast(parent_arr.buffers[1].?));
    const parent_validity: ?[*]const u8 = if (parent_arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;

    var maps = allocator.alloc(Optional([]const MapEntry(K, V)), n) catch return error.OutOfMemory;
    defer {
        for (maps) |m| switch (m) {
            .value => |entries| allocator.free(entries),
            .null_value => {},
        };
        allocator.free(maps);
    }

    for (0..n) |i| {
        if (parent_validity != null and !arrow.getBit(parent_validity.?[0 .. (n + 7) / 8], i)) {
            maps[i] = .null_value;
        } else {
            const start = safe.castTo(usize, offsets[i]) catch return error.IntegerOverflow;
            const end = safe.castTo(usize, offsets[i + 1]) catch return error.IntegerOverflow;
            const len = end - start;
            var entries = allocator.alloc(MapEntry(K, V), len) catch return error.OutOfMemory;
            for (0..len) |j| {
                entries[j] = .{
                    .key = readArrowValue(K, key_arr, start + j) catch return error.IntegerOverflow,
                    .value = readArrowNullableValue(V, val_arr, start + j) catch return error.IntegerOverflow,
                };
            }
            maps[i] = .{ .value = entries };
        }
    }

    try writer.writeMapColumn(K, V, col_idx, maps);
}

fn writeMapColumnBool(
    comptime K: type,
    writer: *Writer,
    allocator: Allocator,
    col_idx: usize,
    parent_arr: ArrowArray,
    key_arr: ArrowArray,
    val_arr: ArrowArray,
) WriterError!void {
    const n = safe.castTo(usize, parent_arr.length) catch return error.IntegerOverflow;
    const offsets: [*]const i32 = @ptrCast(@alignCast(parent_arr.buffers[1].?));
    const parent_validity: ?[*]const u8 = if (parent_arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;
    const val_bits: [*]const u8 = @ptrCast(@alignCast(val_arr.buffers[1].?));
    const val_validity: ?[*]const u8 = if (val_arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;
    const val_len = safe.castTo(usize, val_arr.length) catch return error.IntegerOverflow;

    var maps = allocator.alloc(Optional([]const MapEntry(K, bool)), n) catch return error.OutOfMemory;
    defer {
        for (maps) |m| switch (m) {
            .value => |entries| allocator.free(entries),
            .null_value => {},
        };
        allocator.free(maps);
    }

    for (0..n) |i| {
        if (parent_validity != null and !arrow.getBit(parent_validity.?[0 .. (n + 7) / 8], i)) {
            maps[i] = .null_value;
        } else {
            const start = safe.castTo(usize, offsets[i]) catch return error.IntegerOverflow;
            const end = safe.castTo(usize, offsets[i + 1]) catch return error.IntegerOverflow;
            const len = end - start;
            var entries = allocator.alloc(MapEntry(K, bool), len) catch return error.OutOfMemory;
            for (0..len) |j| {
                const idx = start + j;
                entries[j].key = readArrowValue(K, key_arr, idx) catch return error.IntegerOverflow;
                if (val_validity) |vv| {
                    if (!arrow.getBit(vv[0 .. (val_len + 7) / 8], idx)) {
                        entries[j].value = .null_value;
                        continue;
                    }
                }
                entries[j].value = .{ .value = arrow.getBit(val_bits[0 .. (val_len + 7) / 8], idx) };
            }
            maps[i] = .{ .value = entries };
        }
    }

    try writer.writeMapColumn(K, bool, col_idx, maps);
}

fn readArrowValue(comptime T: type, arr: ArrowArray, idx: usize) error{IntegerOverflow}!T {
    if (T == []const u8) {
        const str_offsets: [*]const i32 = @ptrCast(@alignCast(arr.buffers[1].?));
        const str_data: [*]const u8 = if (arr.buffers[2]) |b| @ptrCast(@alignCast(b)) else @as([*]const u8, &[_]u8{});
        const s: usize = try safe.cast(str_offsets[idx]);
        const e: usize = try safe.cast(str_offsets[idx + 1]);
        return str_data[s..e];
    } else {
        const data: [*]const T = @ptrCast(@alignCast(arr.buffers[1].?));
        return data[idx];
    }
}

fn readArrowNullableValue(comptime T: type, arr: ArrowArray, idx: usize) error{IntegerOverflow}!Optional(T) {
    const validity: ?[*]const u8 = if (arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;
    const len = safe.castTo(usize, arr.length) catch return .null_value;
    if (validity) |v| {
        if (!arrow.getBit(v[0 .. (len + 7) / 8], idx)) return .null_value;
    }
    return .{ .value = try readArrowValue(T, arr, idx) };
}

fn writeTypedColumn(
    comptime T: type,
    writer: *Writer,
    allocator: Allocator,
    col_idx: usize,
    arr: ArrowArray,
    n: usize,
) WriterError!void {
    const validity: ?[*]const u8 = if (arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;

    if (T == bool) {
        const data_bits: [*]const u8 = @ptrCast(@alignCast(arr.buffers[1].?));
        var optionals = allocator.alloc(Optional(bool), n) catch return error.OutOfMemory;
        defer allocator.free(optionals);
        for (0..n) |i| {
            if (validity != null and !arrow.getBit(validity.?[0 .. (n + 7) / 8], i)) {
                optionals[i] = .null_value;
            } else {
                const byte_idx = i / 8;
                const bit_idx: u3 = safe.castTo(u3, i % 8) catch unreachable; // i % 8 is 0-7
                optionals[i] = .{ .value = (data_bits[byte_idx] & (@as(u8, 1) << bit_idx)) != 0 };
            }
        }
        try writer.writeColumnOptional(bool, col_idx, optionals);
    } else {
        const data: [*]const T = @ptrCast(@alignCast(arr.buffers[1].?));
        var optionals = allocator.alloc(Optional(T), n) catch return error.OutOfMemory;
        defer allocator.free(optionals);
        for (0..n) |i| {
            if (validity != null and !arrow.getBit(validity.?[0 .. (n + 7) / 8], i)) {
                optionals[i] = .null_value;
            } else {
                optionals[i] = .{ .value = data[i] };
            }
        }
        try writer.writeColumnOptional(T, col_idx, optionals);
    }
}

/// Write an Arrow column by widening from Src to Dst (e.g. i8 → i32 for Parquet INT32).
fn writeWidenedColumn(
    comptime Src: type,
    comptime Dst: type,
    writer: *Writer,
    allocator: Allocator,
    col_idx: usize,
    arr: ArrowArray,
    n: usize,
) WriterError!void {
    const validity: ?[*]const u8 = if (arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;
    const data: [*]const Src = @ptrCast(@alignCast(arr.buffers[1].?));
    var optionals = allocator.alloc(Optional(Dst), n) catch return error.OutOfMemory;
    defer allocator.free(optionals);
    for (0..n) |i| {
        if (validity != null and !arrow.getBit(validity.?[0 .. (n + 7) / 8], i)) {
            optionals[i] = .null_value;
        } else {
            optionals[i] = .{ .value = safe.castTo(Dst, data[i]) catch return error.IntegerOverflow };
        }
    }
    try writer.writeColumnOptional(Dst, col_idx, optionals);
}

fn writeByteArrayColumn(
    writer: *Writer,
    allocator: Allocator,
    col_idx: usize,
    arr: ArrowArray,
    n: usize,
    large: bool,
) WriterError!void {
    const validity: ?[*]const u8 = if (arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;
    const data: [*]const u8 = if (arr.buffers[2]) |b| @ptrCast(@alignCast(b)) else @as([*]const u8, &[_]u8{});

    var optionals = allocator.alloc(Optional([]const u8), n) catch return error.OutOfMemory;
    defer allocator.free(optionals);

    if (large) {
        const offsets: [*]const i64 = @ptrCast(@alignCast(arr.buffers[1].?));
        for (0..n) |i| {
            if (validity != null and !arrow.getBit(validity.?[0 .. (n + 7) / 8], i)) {
                optionals[i] = .null_value;
            } else {
                const start = safe.castTo(usize, offsets[i]) catch return error.IntegerOverflow;
                const end = safe.castTo(usize, offsets[i + 1]) catch return error.IntegerOverflow;
                optionals[i] = .{ .value = data[start..end] };
            }
        }
    } else {
        const offsets: [*]const i32 = @ptrCast(@alignCast(arr.buffers[1].?));
        for (0..n) |i| {
            if (validity != null and !arrow.getBit(validity.?[0 .. (n + 7) / 8], i)) {
                optionals[i] = .null_value;
            } else {
                const start = safe.castTo(usize, offsets[i]) catch return error.IntegerOverflow;
                const end = safe.castTo(usize, offsets[i + 1]) catch return error.IntegerOverflow;
                optionals[i] = .{ .value = data[start..end] };
            }
        }
    }

    try writer.writeColumnOptional([]const u8, col_idx, optionals);
}

fn writeDate64Column(
    writer: *Writer,
    allocator: Allocator,
    col_idx: usize,
    arr: ArrowArray,
    n: usize,
) WriterError!void {
    const validity: ?[*]const u8 = if (arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;
    const data: [*]const i64 = @ptrCast(@alignCast(arr.buffers[1].?));

    var optionals = allocator.alloc(Optional(i32), n) catch return error.OutOfMemory;
    defer allocator.free(optionals);

    const millis_per_day: i64 = 86_400_000;
    for (0..n) |i| {
        if (validity != null and !arrow.getBit(validity.?[0 .. (n + 7) / 8], i)) {
            optionals[i] = .null_value;
        } else {
            const days = @divTrunc(data[i], millis_per_day);
            optionals[i] = .{ .value = safe.castTo(i32, days) catch return error.IntegerOverflow };
        }
    }

    try writer.writeColumnOptional(i32, col_idx, optionals);
}

fn writeTime32SecondsColumn(
    writer: *Writer,
    allocator: Allocator,
    col_idx: usize,
    arr: ArrowArray,
    n: usize,
) WriterError!void {
    const validity: ?[*]const u8 = if (arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;
    const data: [*]const i32 = @ptrCast(@alignCast(arr.buffers[1].?));

    var optionals = allocator.alloc(Optional(i32), n) catch return error.OutOfMemory;
    defer allocator.free(optionals);

    for (0..n) |i| {
        if (validity != null and !arrow.getBit(validity.?[0 .. (n + 7) / 8], i)) {
            optionals[i] = .null_value;
        } else {
            optionals[i] = .{ .value = std.math.mul(i32, data[i], 1000) catch return error.IntegerOverflow };
        }
    }

    try writer.writeColumnOptional(i32, col_idx, optionals);
}

fn writeFixedByteArrayColumn(
    writer: *Writer,
    allocator: Allocator,
    col_idx: usize,
    arr: ArrowArray,
    n: usize,
    type_len: usize,
) WriterError!void {
    const validity: ?[*]const u8 = if (arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;
    const data: [*]const u8 = @ptrCast(@alignCast(arr.buffers[1].?));

    var optionals = allocator.alloc(Optional([]const u8), n) catch return error.OutOfMemory;
    defer allocator.free(optionals);

    for (0..n) |i| {
        if (validity != null and !arrow.getBit(validity.?[0 .. (n + 7) / 8], i)) {
            optionals[i] = .null_value;
        } else {
            optionals[i] = .{ .value = data[i * type_len ..][0..type_len] };
        }
    }

    try writer.writeColumnFixedByteArrayOptional(col_idx, optionals);
}

// ============================================================================
// Tests
// ============================================================================

test "schema conversion - flat types" {
    const allocator = std.testing.allocator;

    // Build a simple Parquet schema: root → col1(int32), col2(string)
    const schema_elems = [_]format.SchemaElement{
        .{ .name = "root", .num_children = 2 },
        .{ .name = "id", .type_ = .int32, .repetition_type = .required },
        .{ .name = "name", .type_ = .byte_array, .repetition_type = .optional, .logical_type = .string },
    };

    const metadata = format.FileMetaData{
        .version = 2,
        .schema = @constCast(&schema_elems),
        .num_rows = 0,
        .row_groups = &.{},
    };

    var arrow_schema = try exportSchemaAsArrow(allocator, metadata);
    defer arrow_schema.doRelease();

    // Verify root is struct
    try std.testing.expectEqualStrings("+s", std.mem.sliceTo(arrow_schema.format, 0));
    try std.testing.expectEqual(@as(i64, 2), arrow_schema.n_children);

    // Verify children
    const children: [*]*ArrowSchema = arrow_schema.children.?;

    // col1: int32, required
    const col1 = children[0];
    try std.testing.expectEqualStrings("i", std.mem.sliceTo(col1.format, 0));
    try std.testing.expectEqualStrings("id", std.mem.sliceTo(col1.name.?, 0));
    try std.testing.expectEqual(@as(i64, 0), col1.flags & arrow.ARROW_FLAG_NULLABLE);

    // col2: string, optional
    const col2 = children[1];
    try std.testing.expectEqualStrings("u", std.mem.sliceTo(col2.format, 0));
    try std.testing.expectEqualStrings("name", std.mem.sliceTo(col2.name.?, 0));
    try std.testing.expect((col2.flags & arrow.ARROW_FLAG_NULLABLE) != 0);
}

test "schema conversion - temporal types" {
    const allocator = std.testing.allocator;

    const schema_elems = [_]format.SchemaElement{
        .{ .name = "root", .num_children = 2 },
        .{ .name = "created", .type_ = .int32, .repetition_type = .optional, .logical_type = .date },
        .{ .name = "ts", .type_ = .int64, .repetition_type = .optional, .logical_type = .{ .timestamp = .{ .is_adjusted_to_utc = true, .unit = .micros } } },
    };

    const metadata = format.FileMetaData{
        .version = 2,
        .schema = @constCast(&schema_elems),
        .num_rows = 0,
        .row_groups = &.{},
    };

    var arrow_schema = try exportSchemaAsArrow(allocator, metadata);
    defer arrow_schema.doRelease();

    const children: [*]*ArrowSchema = arrow_schema.children.?;
    try std.testing.expectEqualStrings("tdD", std.mem.sliceTo(children[0].format, 0));
    try std.testing.expectEqualStrings("tsu:UTC", std.mem.sliceTo(children[1].format, 0));
}

test "import schema from arrow - flat types" {
    const allocator = std.testing.allocator;

    // Build an Arrow schema manually
    const schema_elems = [_]format.SchemaElement{
        .{ .name = "root", .num_children = 2 },
        .{ .name = "id", .type_ = .int32, .repetition_type = .required },
        .{ .name = "name", .type_ = .byte_array, .repetition_type = .optional, .logical_type = .string },
    };

    const metadata = format.FileMetaData{
        .version = 2,
        .schema = @constCast(&schema_elems),
        .num_rows = 0,
        .row_groups = &.{},
    };

    var arrow_schema = try exportSchemaAsArrow(allocator, metadata);
    defer arrow_schema.doRelease();

    // Import back to ColumnDef
    const col_defs = try importSchemaFromArrow(allocator, &arrow_schema);
    defer allocator.free(col_defs);

    try std.testing.expectEqual(@as(usize, 2), col_defs.len);
    try std.testing.expectEqual(format.PhysicalType.int32, col_defs[0].type_);
    try std.testing.expectEqual(false, col_defs[0].optional);
    try std.testing.expectEqual(format.PhysicalType.byte_array, col_defs[1].type_);
    try std.testing.expectEqual(true, col_defs[1].optional);
}

test "values to arrow array - int32" {
    const allocator = std.testing.allocator;

    const values = [_]Value{
        .{ .int32_val = 10 },
        .{ .int32_val = 20 },
        .null_val,
        .{ .int32_val = 40 },
    };

    var arr = try int32ValuesToArrow(allocator, &values);
    defer arr.doRelease();

    try std.testing.expectEqual(@as(i64, 4), arr.length);
    try std.testing.expectEqual(@as(i64, 1), arr.null_count);
    try std.testing.expectEqual(@as(i64, 2), arr.n_buffers);

    // Check values
    const data: [*]const i32 = @ptrCast(@alignCast(arr.buffers[1].?));
    try std.testing.expectEqual(@as(i32, 10), data[0]);
    try std.testing.expectEqual(@as(i32, 20), data[1]);
    try std.testing.expectEqual(@as(i32, 40), data[3]);

    // Check validity bitmap
    const validity: [*]const u8 = @ptrCast(@alignCast(arr.buffers[0].?));
    try std.testing.expect(arrow.getBit(validity[0..1], 0));
    try std.testing.expect(arrow.getBit(validity[0..1], 1));
    try std.testing.expect(!arrow.getBit(validity[0..1], 2));
    try std.testing.expect(arrow.getBit(validity[0..1], 3));
}

test "values to arrow array - byte array" {
    const allocator = std.testing.allocator;

    const values = [_]Value{
        .{ .bytes_val = "hello" },
        .null_val,
        .{ .bytes_val = "world" },
    };

    var arr = try byteArrayValuesToArrow(allocator, &values);
    defer arr.doRelease();

    try std.testing.expectEqual(@as(i64, 3), arr.length);
    try std.testing.expectEqual(@as(i64, 1), arr.null_count);
    try std.testing.expectEqual(@as(i64, 3), arr.n_buffers);

    // Check offsets
    const offsets: [*]const i32 = @ptrCast(@alignCast(arr.buffers[1].?));
    try std.testing.expectEqual(@as(i32, 0), offsets[0]);
    try std.testing.expectEqual(@as(i32, 5), offsets[1]);
    try std.testing.expectEqual(@as(i32, 5), offsets[2]); // null value
    try std.testing.expectEqual(@as(i32, 10), offsets[3]);

    // Check data
    const data: [*]const u8 = @ptrCast(@alignCast(arr.buffers[2].?));
    try std.testing.expectEqualStrings("helloworld", data[0..10]);
}

test "values to arrow array - boolean" {
    const allocator = std.testing.allocator;

    const values = [_]Value{
        .{ .bool_val = true },
        .{ .bool_val = false },
        .null_val,
        .{ .bool_val = true },
    };

    var arr = try boolValuesToArrow(allocator, &values);
    defer arr.doRelease();

    try std.testing.expectEqual(@as(i64, 4), arr.length);
    try std.testing.expectEqual(@as(i64, 1), arr.null_count);
}

// ============================================================================
// Round-trip integration tests
// ============================================================================

const api_writer_mod = @import("../api/zig/writer.zig");
const api_reader_mod = @import("../api/zig/reader.zig");

test "round-trip: write parquet, read as arrow - int32 + string" {
    const allocator = std.testing.allocator;

    // Write a Parquet file to buffer
    var writer = try api_writer_mod.writeToBuffer(allocator, &.{
        .{ .name = "id", .type_ = .int32, .optional = true },
        .{ .name = "name", .type_ = .byte_array, .optional = true, .logical_type = .string },
    });

    try writer.writeColumnOptional(i32, 0, &.{
        .{ .value = 1 }, .{ .value = 2 }, .null_value, .{ .value = 4 },
    });
    try writer.writeColumnOptional([]const u8, 1, &.{
        .{ .value = "alice" }, .null_value, .{ .value = "charlie" }, .{ .value = "diana" },
    });

    try writer.close();
    const buf = try writer.toOwnedSlice();
    defer allocator.free(buf);
    writer.deinit();

    var dr = try api_reader_mod.openBufferDynamic(allocator, buf, .{});
    defer dr.deinit();

    var result = try readRowGroupAsArrow(allocator, dr.getSource(), dr.metadata, 0, null);
    defer result.deinit();

    // Verify INT32 column
    try std.testing.expectEqual(@as(usize, 2), result.arrays.len);
    const int_arr = &result.arrays[0];
    try std.testing.expectEqual(@as(i64, 4), int_arr.length);
    try std.testing.expectEqual(@as(i64, 1), int_arr.null_count);

    const int_data: [*]const i32 = @ptrCast(@alignCast(int_arr.buffers[1].?));
    try std.testing.expectEqual(@as(i32, 1), int_data[0]);
    try std.testing.expectEqual(@as(i32, 2), int_data[1]);
    try std.testing.expectEqual(@as(i32, 4), int_data[3]);

    const int_validity: [*]const u8 = @ptrCast(@alignCast(int_arr.buffers[0].?));
    try std.testing.expect(arrow.getBit(int_validity[0..1], 0));
    try std.testing.expect(arrow.getBit(int_validity[0..1], 1));
    try std.testing.expect(!arrow.getBit(int_validity[0..1], 2));
    try std.testing.expect(arrow.getBit(int_validity[0..1], 3));

    // Verify string column
    const str_arr = &result.arrays[1];
    try std.testing.expectEqual(@as(i64, 4), str_arr.length);
    try std.testing.expectEqual(@as(i64, 1), str_arr.null_count);
    try std.testing.expectEqual(@as(i64, 3), str_arr.n_buffers);

    const str_offsets: [*]const i32 = @ptrCast(@alignCast(str_arr.buffers[1].?));
    const str_data: [*]const u8 = @ptrCast(@alignCast(str_arr.buffers[2].?));
    const s0_start: usize = @intCast(str_offsets[0]);
    const s0_end: usize = @intCast(str_offsets[1]);
    try std.testing.expectEqualStrings("alice", str_data[s0_start..s0_end]);

    const str_validity: [*]const u8 = @ptrCast(@alignCast(str_arr.buffers[0].?));
    try std.testing.expect(!arrow.getBit(str_validity[0..1], 1));
}

test "round-trip: arrow write then arrow read - int64 + float64" {
    const allocator = std.testing.allocator;

    // Step 1: Build Arrow arrays manually
    const n: usize = 3;
    const bitmap_len = 1;

    // INT64 column
    const i64_validity = try allocator.alloc(u8, bitmap_len);
    defer allocator.free(i64_validity);
    @memset(i64_validity, 0xFF);

    const i64_data = try allocator.alloc(u8, n * 8);
    defer allocator.free(i64_data);
    const i64_typed: [*]i64 = @ptrCast(@alignCast(i64_data.ptr));
    i64_typed[0] = 100;
    i64_typed[1] = 200;
    i64_typed[2] = 300;

    var i64_buffers = [_]?*anyopaque{ @ptrCast(i64_validity.ptr), @ptrCast(i64_data.ptr) };
    const i64_arr = ArrowArray{
        .length = 3,
        .null_count = 0,
        .offset = 0,
        .n_buffers = 2,
        .n_children = 0,
        .buffers = &i64_buffers,
        .children = null,
        .dictionary = null,
        .release = null,
        .private_data = null,
    };

    // FLOAT64 column
    const f64_validity = try allocator.alloc(u8, bitmap_len);
    defer allocator.free(f64_validity);
    @memset(f64_validity, 0xFF);
    arrow.clearBit(f64_validity, 1);

    const f64_data = try allocator.alloc(u8, n * 8);
    defer allocator.free(f64_data);
    const f64_typed: [*]f64 = @ptrCast(@alignCast(f64_data.ptr));
    f64_typed[0] = 1.5;
    f64_typed[1] = 0;
    f64_typed[2] = 3.14;

    var f64_buffers = [_]?*anyopaque{ @ptrCast(f64_validity.ptr), @ptrCast(f64_data.ptr) };
    const f64_arr = ArrowArray{
        .length = 3,
        .null_count = 1,
        .offset = 0,
        .n_buffers = 2,
        .n_children = 0,
        .buffers = &f64_buffers,
        .children = null,
        .dictionary = null,
        .release = null,
        .private_data = null,
    };

    // Create matching schemas
    const i64_schema = ArrowSchema{
        .format = "l",
        .name = "amount",
        .metadata = null,
        .flags = 0,
        .n_children = 0,
        .children = null,
        .dictionary = null,
        .release = null,
        .private_data = null,
    };
    const f64_schema = ArrowSchema{
        .format = "g",
        .name = "score",
        .metadata = null,
        .flags = arrow.ARROW_FLAG_NULLABLE,
        .n_children = 0,
        .children = null,
        .dictionary = null,
        .release = null,
        .private_data = null,
    };

    // Step 2: Write to Parquet buffer
    const col_defs = [_]ColumnDef{
        .{ .name = "amount", .type_ = .int64, .optional = false },
        .{ .name = "score", .type_ = .double, .optional = true },
    };

    var writer = try api_writer_mod.writeToBuffer(allocator, &col_defs);
    const arrays = [_]ArrowArray{ i64_arr, f64_arr };
    const schemas = [_]ArrowSchema{ i64_schema, f64_schema };
    try writeRowGroupFromArrow(&writer, allocator, &arrays, &schemas);
    try writer.close();
    const buf = try writer.toOwnedSlice();
    defer allocator.free(buf);
    writer.deinit();

    // Step 3: Read back as Arrow
    var dr = try api_reader_mod.openBufferDynamic(allocator, buf, .{});
    defer dr.deinit();

    var result = try readRowGroupAsArrow(allocator, dr.getSource(), dr.metadata, 0, null);
    defer result.deinit();

    // Verify INT64 column
    const read_i64: [*]const i64 = @ptrCast(@alignCast(result.arrays[0].buffers[1].?));
    try std.testing.expectEqual(@as(i64, 100), read_i64[0]);
    try std.testing.expectEqual(@as(i64, 200), read_i64[1]);
    try std.testing.expectEqual(@as(i64, 300), read_i64[2]);

    // Verify FLOAT64 column
    const read_f64: [*]const f64 = @ptrCast(@alignCast(result.arrays[1].buffers[1].?));
    try std.testing.expectApproxEqAbs(@as(f64, 1.5), read_f64[0], 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 3.14), read_f64[2], 0.001);

    // Check null in float64 column
    try std.testing.expectEqual(@as(i64, 1), result.arrays[1].null_count);
    const read_f64_validity: [*]const u8 = @ptrCast(@alignCast(result.arrays[1].buffers[0].?));
    try std.testing.expect(!arrow.getBit(read_f64_validity[0..1], 1));
}

test "schema round-trip: export then import" {
    const allocator = std.testing.allocator;

    const schema_elems = [_]format.SchemaElement{
        .{ .name = "root", .num_children = 3 },
        .{ .name = "id", .type_ = .int64, .repetition_type = .required },
        .{ .name = "name", .type_ = .byte_array, .repetition_type = .optional, .logical_type = .string },
        .{ .name = "score", .type_ = .double, .repetition_type = .optional },
    };

    const metadata = format.FileMetaData{
        .version = 2,
        .schema = @constCast(&schema_elems),
        .num_rows = 0,
        .row_groups = &.{},
    };

    var arrow_schema = try exportSchemaAsArrow(allocator, metadata);
    defer arrow_schema.doRelease();

    const col_defs = try importSchemaFromArrow(allocator, &arrow_schema);
    defer allocator.free(col_defs);

    try std.testing.expectEqual(@as(usize, 3), col_defs.len);

    try std.testing.expectEqual(format.PhysicalType.int64, col_defs[0].type_);
    try std.testing.expectEqual(false, col_defs[0].optional);

    try std.testing.expectEqual(format.PhysicalType.byte_array, col_defs[1].type_);
    try std.testing.expectEqual(true, col_defs[1].optional);

    try std.testing.expectEqual(format.PhysicalType.double, col_defs[2].type_);
    try std.testing.expectEqual(true, col_defs[2].optional);
}

// ============================================================================
// Nested Arrow Round-Trip Tests
// ============================================================================

test "round-trip: LIST of int32" {
    const allocator = std.testing.allocator;

    const col_defs = [_]ColumnDef{
        .{ .name = "tags", .type_ = .int32, .optional = true, .is_list = true, .element_optional = true },
    };

    var writer = try api_writer_mod.writeToBuffer(allocator, &col_defs);

    try writer.writeListColumn(i32, 0, &.{
        .{ .value = &.{ .{ .value = 1 }, .{ .value = 2 }, .{ .value = 3 } } },
        .null_value,
        .{ .value = &.{} },
        .{ .value = &.{ .{ .value = 10 }, .null_value, .{ .value = 30 } } },
    });

    try writer.close();
    const buf = try writer.toOwnedSlice();
    defer allocator.free(buf);
    writer.deinit();

    var dr = try api_reader_mod.openBufferDynamic(allocator, buf, .{});
    defer dr.deinit();

    var result = try readRowGroupAsArrow(allocator, dr.getSource(), dr.metadata, 0, null);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.arrays.len);
    const list_arr = &result.arrays[0];
    try std.testing.expectEqual(@as(i64, 4), list_arr.length);
    try std.testing.expectEqual(@as(i64, 1), list_arr.null_count);

    // Check offsets
    const list_offsets: [*]const i32 = @ptrCast(@alignCast(list_arr.buffers[1].?));
    try std.testing.expectEqual(@as(i32, 0), list_offsets[0]);
    try std.testing.expectEqual(@as(i32, 3), list_offsets[1]);
    try std.testing.expectEqual(@as(i32, 3), list_offsets[2]); // null list
    try std.testing.expectEqual(@as(i32, 3), list_offsets[3]); // empty list

    // Check parent validity
    const list_validity: [*]const u8 = @ptrCast(@alignCast(list_arr.buffers[0].?));
    try std.testing.expect(arrow.getBit(list_validity[0..1], 0));
    try std.testing.expect(!arrow.getBit(list_validity[0..1], 1)); // null
    try std.testing.expect(arrow.getBit(list_validity[0..1], 2));
    try std.testing.expect(arrow.getBit(list_validity[0..1], 3));

    // Check child array
    try std.testing.expectEqual(@as(i64, 1), list_arr.n_children);
    const children: [*]*ArrowArray = list_arr.children.?;
    const child = children[0];
    try std.testing.expectEqual(@as(i64, 6), child.length);

    const child_data: [*]const i32 = @ptrCast(@alignCast(child.buffers[1].?));
    try std.testing.expectEqual(@as(i32, 1), child_data[0]);
    try std.testing.expectEqual(@as(i32, 2), child_data[1]);
    try std.testing.expectEqual(@as(i32, 3), child_data[2]);
    try std.testing.expectEqual(@as(i32, 10), child_data[3]);
    try std.testing.expectEqual(@as(i32, 30), child_data[5]);

    // Check child validity (element at index 4 should be null)
    try std.testing.expectEqual(@as(i64, 1), child.null_count);
    const child_validity: [*]const u8 = @ptrCast(@alignCast(child.buffers[0].?));
    try std.testing.expect(!arrow.getBit(child_validity[0..1], 4));
}

test "round-trip: LIST of strings" {
    const allocator = std.testing.allocator;

    const col_defs = [_]ColumnDef{
        .{ .name = "names", .type_ = .byte_array, .optional = true, .is_list = true, .element_optional = true, .logical_type = .string },
    };

    var writer = try api_writer_mod.writeToBuffer(allocator, &col_defs);

    try writer.writeListColumn([]const u8, 0, &.{
        .{ .value = &.{ .{ .value = "alice" }, .{ .value = "bob" } } },
        .null_value,
        .{ .value = &.{ .{ .value = "charlie" } } },
    });

    try writer.close();
    const buf = try writer.toOwnedSlice();
    defer allocator.free(buf);
    writer.deinit();

    var dr = try api_reader_mod.openBufferDynamic(allocator, buf, .{});
    defer dr.deinit();

    var result = try readRowGroupAsArrow(allocator, dr.getSource(), dr.metadata, 0, null);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.arrays.len);
    const list_arr = &result.arrays[0];
    try std.testing.expectEqual(@as(i64, 3), list_arr.length);
    try std.testing.expectEqual(@as(i64, 1), list_arr.null_count);

    // Check child array contains string data
    const children: [*]*ArrowArray = list_arr.children.?;
    const child = children[0];
    try std.testing.expectEqual(@as(i64, 3), child.length);
    try std.testing.expectEqual(@as(i64, 3), child.n_buffers);

    // Verify string data
    const str_offsets: [*]const i32 = @ptrCast(@alignCast(child.buffers[1].?));
    const str_data: [*]const u8 = @ptrCast(@alignCast(child.buffers[2].?));
    const s0_start: usize = @intCast(str_offsets[0]);
    const s0_end: usize = @intCast(str_offsets[1]);
    try std.testing.expectEqualStrings("alice", str_data[s0_start..s0_end]);

    const s1_start: usize = @intCast(str_offsets[1]);
    const s1_end: usize = @intCast(str_offsets[2]);
    try std.testing.expectEqualStrings("bob", str_data[s1_start..s1_end]);

    const s2_start: usize = @intCast(str_offsets[2]);
    const s2_end: usize = @intCast(str_offsets[3]);
    try std.testing.expectEqualStrings("charlie", str_data[s2_start..s2_end]);
}

test "round-trip: STRUCT with mixed fields" {
    const allocator = std.testing.allocator;

    const struct_fields = [_]column_def_mod.StructField{
        .{ .name = "x", .type_ = .int32 },
        .{ .name = "label", .type_ = .byte_array },
    };

    const col_defs = [_]ColumnDef{
        .{ .name = "point", .type_ = .int32, .optional = true, .is_struct = true, .struct_fields = &struct_fields },
    };

    var writer = try api_writer_mod.writeToBuffer(allocator, &col_defs);

    try writer.writeStructField(i32, 0, 0, &.{ 10, 20, null }, &.{ false, false, true });
    try writer.writeStructField([]const u8, 0, 1, &.{ "hello", null, null }, &.{ false, false, true });

    try writer.close();
    const buf = try writer.toOwnedSlice();
    defer allocator.free(buf);
    writer.deinit();

    var dr = try api_reader_mod.openBufferDynamic(allocator, buf, .{});
    defer dr.deinit();

    var result = try readRowGroupAsArrow(allocator, dr.getSource(), dr.metadata, 0, null);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.arrays.len);
    const struct_arr = &result.arrays[0];
    try std.testing.expectEqual(@as(i64, 3), struct_arr.length);
    try std.testing.expectEqual(@as(i64, 2), struct_arr.n_children);

    // Check int32 child
    const children: [*]*ArrowArray = struct_arr.children.?;
    const x_arr = children[0];
    try std.testing.expectEqual(@as(i64, 3), x_arr.length);
    const x_data: [*]const i32 = @ptrCast(@alignCast(x_arr.buffers[1].?));
    try std.testing.expectEqual(@as(i32, 10), x_data[0]);
    try std.testing.expectEqual(@as(i32, 20), x_data[1]);

    // Check string child
    const label_arr = children[1];
    try std.testing.expectEqual(@as(i64, 3), label_arr.length);
    const label_offsets: [*]const i32 = @ptrCast(@alignCast(label_arr.buffers[1].?));
    const label_data: [*]const u8 = @ptrCast(@alignCast(label_arr.buffers[2].?));
    const l0_s: usize = @intCast(label_offsets[0]);
    const l0_e: usize = @intCast(label_offsets[1]);
    try std.testing.expectEqualStrings("hello", label_data[l0_s..l0_e]);
}

test "round-trip: MAP(string->int32)" {
    const allocator = std.testing.allocator;

    const col_defs = [_]ColumnDef{
        .{ .name = "attrs", .type_ = .byte_array, .optional = true, .is_map = true, .map_value_type = .int32, .map_value_optional = true, .logical_type = .string },
    };

    var writer = try api_writer_mod.writeToBuffer(allocator, &col_defs);

    const MapEntryType = MapEntry([]const u8, i32);
    try writer.writeMapColumn([]const u8, i32, 0, &.{
        .{ .value = &.{
            MapEntryType{ .key = "a", .value = .{ .value = 1 } },
            MapEntryType{ .key = "b", .value = .{ .value = 2 } },
        } },
        .null_value,
        .{ .value = &.{
            MapEntryType{ .key = "c", .value = .{ .value = 3 } },
        } },
    });

    try writer.close();
    const buf = try writer.toOwnedSlice();
    defer allocator.free(buf);
    writer.deinit();

    var dr = try api_reader_mod.openBufferDynamic(allocator, buf, .{});
    defer dr.deinit();

    var result = try readRowGroupAsArrow(allocator, dr.getSource(), dr.metadata, 0, null);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.arrays.len);
    const map_arr = &result.arrays[0];
    try std.testing.expectEqual(@as(i64, 3), map_arr.length);
    try std.testing.expectEqual(@as(i64, 1), map_arr.null_count);

    // Check map offsets
    const map_offsets: [*]const i32 = @ptrCast(@alignCast(map_arr.buffers[1].?));
    try std.testing.expectEqual(@as(i32, 0), map_offsets[0]);
    try std.testing.expectEqual(@as(i32, 2), map_offsets[1]); // 2 entries
    try std.testing.expectEqual(@as(i32, 2), map_offsets[2]); // null map
    try std.testing.expectEqual(@as(i32, 3), map_offsets[3]); // 1 entry

    // Check entries struct child
    const map_children: [*]*ArrowArray = map_arr.children.?;
    const entries = map_children[0];
    try std.testing.expectEqual(@as(i64, 3), entries.length); // total entries
    try std.testing.expectEqual(@as(i64, 2), entries.n_children); // key + value

    // Check key array (strings)
    const kv_children: [*]*ArrowArray = entries.children.?;
    const key_arr = kv_children[0];
    try std.testing.expectEqual(@as(i64, 3), key_arr.length);

    const key_offsets: [*]const i32 = @ptrCast(@alignCast(key_arr.buffers[1].?));
    const key_data: [*]const u8 = @ptrCast(@alignCast(key_arr.buffers[2].?));
    const k0_s: usize = @intCast(key_offsets[0]);
    const k0_e: usize = @intCast(key_offsets[1]);
    try std.testing.expectEqualStrings("a", key_data[k0_s..k0_e]);

    // Check value array (int32)
    const val_arr = kv_children[1];
    try std.testing.expectEqual(@as(i64, 3), val_arr.length);
    const val_data: [*]const i32 = @ptrCast(@alignCast(val_arr.buffers[1].?));
    try std.testing.expectEqual(@as(i32, 1), val_data[0]);
    try std.testing.expectEqual(@as(i32, 2), val_data[1]);
    try std.testing.expectEqual(@as(i32, 3), val_data[2]);
}
