//! Parquet ↔ Arrow Batch API
//!
//! Runtime type dispatch for reading/writing row groups as Arrow arrays.
//! This is the bridge between Parquet's physical types and the Arrow C Data Interface.
//!
//! Functions:
//! - `exportSchemaAsArrow`: Convert Parquet FileMetaData schema to ArrowSchema
//! - `importSchemaFromArrow`: Convert ArrowSchema to Parquet ColumnDef[]
//! - `readColumns` (`DynamicReader.readColumns`): Decode a row group's columns into ArrowArray[]
//! - `writeRowGroupFromArrow`: Encode ArrowArray[] into a Parquet row group

const std = @import("std");
const safe = @import("safe.zig");
const format = @import("format.zig");
const arrow = @import("arrow.zig");
const parquet_reader = @import("parquet_reader.zig");
const column_def_mod = @import("column_def.zig");
const value_mod = @import("value.zig");
const types = @import("types.zig");
const thrift = @import("thrift/mod.zig");
const compress = @import("compress/mod.zig");
const dictionary = @import("encoding/dictionary.zig");
const leaf_decoder = @import("leaf_decoder.zig");
const schema_tree = @import("schema_tree.zig");
const LevelInfo = schema_tree.LevelInfo;
const Node = schema_tree.Node;
const buildSchemaTree = schema_tree.buildSchemaTree;
const levelShape = schema_tree.levelShape;

const Allocator = std.mem.Allocator;
const native_endian = @import("builtin").cpu.arch.endian();

fn fmtZ(allocator: Allocator, comptime fmt_str: []const u8, args: anytype) error{OutOfMemory}![:0]u8 {
    return std.fmt.allocPrintSentinel(allocator, fmt_str, args, 0);
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
const ReaderError = types.ReaderError;
const parallel = @import("parallel.zig");

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
    InvalidFieldId,
    NestingTooDeep,
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

    const fmt = try allocator.dupeSentinel(u8, format_str, 0);
    errdefer allocator.free(fmt);

    const name_z = try allocator.dupeSentinel(u8, name, 0);
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
    return exportProjectedSchemaAsArrow(allocator, metadata, null);
}

/// Like `exportSchemaAsArrow`, with only the top-level columns `col_indices`, in that order.
/// If `col_indices` is null, all columns are included.
pub fn exportProjectedSchemaAsArrow(allocator: Allocator, metadata: format.FileMetaData, col_indices: ?[]const usize) !ArrowSchema {
    const schema = metadata.schema;
    if (schema.len == 0) return error.InvalidSchema;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const columns = try buildSchemaTree(arena.allocator(), schema);
    return treeSchema(allocator, schema, columns, col_indices, false);
}

/// The ArrowSchema of the top-level columns `col_indices` of a schema tree, or of all of them.
/// With `large_offsets`, BYTE_ARRAY leaves are large_string ("U") or large_binary ("Z").
fn treeSchema(allocator: Allocator, schema: []const format.SchemaElement, columns: []const Node, col_indices: ?[]const usize, large_offsets: bool) !ArrowSchema {
    const n = if (col_indices) |ci| ci.len else columns.len;
    var children = try allocator.alloc(ArrowSchema, n);
    var init_count: usize = 0;
    errdefer {
        for (children[0..init_count]) |*child| child.doRelease();
        allocator.free(children);
    }

    var children_ptrs = try allocator.alloc(*ArrowSchema, n);
    errdefer allocator.free(children_ptrs);

    for (0..n) |i| {
        const col_idx = if (col_indices) |ci| ci[i] else i;
        if (col_idx >= columns.len) return error.InvalidArgument;
        children[i] = try nodeSchema(allocator, schema, &columns[col_idx], null, large_offsets);
        children_ptrs[i] = &children[i];
        init_count = i + 1;
    }

    return createSchemaNode(allocator, "+s", schema[0].name, false, children, children_ptrs);
}

const SchemaConvError = error{
    OutOfMemory,
    InvalidSchema,
    UnsupportedType,
};

/// ArrowSchema of a schema tree node. `name_override` replaces the schema element's name.
fn nodeSchema(allocator: Allocator, schema: []const format.SchemaElement, node: *const Node, name_override: ?[]const u8, large_offsets: bool) SchemaConvError!ArrowSchema {
    const elem = schema[node.schema_idx];
    const name = name_override orelse elem.name;
    if (node.kind == .leaf) {
        const fmt = try parquetTypeToArrowFormat(allocator, elem);
        defer allocator.free(fmt);
        // BYTE_ARRAY leaves read with i64 offsets: "u" -> "U", "z" -> "Z".
        if (large_offsets and elem.type_ == .byte_array and fmt.len == 1 and (fmt[0] == 'u' or fmt[0] == 'z')) fmt[0] = std.ascii.toUpper(fmt[0]);
        return createSchemaNode(allocator, fmt, name, node.nullable, null, null);
    }

    const n = node.children.len;
    var children = try allocator.alloc(ArrowSchema, n);
    var init_count: usize = 0;
    errdefer {
        for (children[0..init_count]) |*c| c.doRelease();
        allocator.free(children);
    }
    var ptrs = try allocator.alloc(*ArrowSchema, n);
    errdefer allocator.free(ptrs);

    for (node.children, 0..) |*child, i| {
        // An Arrow map's child is the struct "entries" (key, value).
        children[i] = try nodeSchema(allocator, schema, child, if (node.kind == .map) "entries" else null, large_offsets);
        ptrs[i] = &children[i];
        init_count = i + 1;
    }

    const fmt: []const u8 = switch (node.kind) {
        .struct_ => "+s",
        .list => "+l",
        .map => "+m",
        .leaf => unreachable,
    };
    return createSchemaNode(allocator, fmt, name, node.nullable, children, ptrs);
}

/// Map a Parquet SchemaElement to an Arrow format string.
/// Caller must free the returned sentinel-terminated slice.
fn parquetTypeToArrowFormat(allocator: Allocator, elem: format.SchemaElement) SchemaConvError![:0]u8 {
    if (elem.logical_type) |lt| {
        return logicalTypeToArrowFormat(allocator, lt);
    }

    if (elem.converted_type) |ct| {
        return convertedTypeToArrowFormat(allocator, ct, elem);
    }

    return physicalTypeToArrowFormat(allocator, elem);
}

fn logicalTypeToArrowFormat(allocator: Allocator, lt: format.LogicalType) SchemaConvError![:0]u8 {
    return switch (lt) {
        .string => allocator.dupeSentinel(u8, "u", 0),
        .enum_ => allocator.dupeSentinel(u8, "u", 0),
        .json => allocator.dupeSentinel(u8, "u", 0),
        .bson => allocator.dupeSentinel(u8, "z", 0),
        .date => allocator.dupeSentinel(u8, "tdD", 0),
        .uuid => allocator.dupeSentinel(u8, "w:16", 0),
        .float16 => allocator.dupeSentinel(u8, "e", 0),
        .geometry => allocator.dupeSentinel(u8, "z", 0),
        .geography => allocator.dupeSentinel(u8, "z", 0),
        .int => |i| {
            if (i.is_signed) {
                return switch (i.bit_width) {
                    8 => allocator.dupeSentinel(u8, "c", 0),
                    16 => allocator.dupeSentinel(u8, "s", 0),
                    32 => allocator.dupeSentinel(u8, "i", 0),
                    64 => allocator.dupeSentinel(u8, "l", 0),
                    else => error.UnsupportedType,
                };
            } else {
                return switch (i.bit_width) {
                    8 => allocator.dupeSentinel(u8, "C", 0),
                    16 => allocator.dupeSentinel(u8, "S", 0),
                    32 => allocator.dupeSentinel(u8, "I", 0),
                    64 => allocator.dupeSentinel(u8, "L", 0),
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
            .millis => allocator.dupeSentinel(u8, "ttm", 0),
            .micros => allocator.dupeSentinel(u8, "ttu", 0),
            .nanos => allocator.dupeSentinel(u8, "ttn", 0),
        },
        .decimal => |d| decimalFormat(allocator, d.precision, d.scale),
    };
}

/// Arrow format of a decimal read from Parquet: decimal128 ("d:p,s", 16 bytes per entry) up
/// to precision 38, decimal256 ("d:p,s,256", 32 bytes) up to 76, as PyArrow reads them.
fn decimalFormat(allocator: Allocator, precision: i32, scale: i32) SchemaConvError![:0]u8 {
    if (precision < 1 or precision > 76) return error.UnsupportedType;
    if (precision <= 38) return fmtZ(allocator, "d:{d},{d}", .{ precision, scale });
    return fmtZ(allocator, "d:{d},{d},256", .{ precision, scale });
}

fn convertedTypeToArrowFormat(allocator: Allocator, ct: i32, elem: format.SchemaElement) SchemaConvError![:0]u8 {
    return switch (ct) {
        format.ConvertedType.UTF8 => allocator.dupeSentinel(u8, "u", 0),
        format.ConvertedType.DATE => allocator.dupeSentinel(u8, "tdD", 0),
        format.ConvertedType.TIMESTAMP_MILLIS => allocator.dupeSentinel(u8, "tsm:", 0),
        format.ConvertedType.TIMESTAMP_MICROS => allocator.dupeSentinel(u8, "tsu:", 0),
        format.ConvertedType.TIME_MILLIS => allocator.dupeSentinel(u8, "ttm", 0),
        format.ConvertedType.TIME_MICROS => allocator.dupeSentinel(u8, "ttu", 0),
        format.ConvertedType.INT_8 => allocator.dupeSentinel(u8, "c", 0),
        format.ConvertedType.INT_16 => allocator.dupeSentinel(u8, "s", 0),
        format.ConvertedType.INT_32 => allocator.dupeSentinel(u8, "i", 0),
        format.ConvertedType.INT_64 => allocator.dupeSentinel(u8, "l", 0),
        format.ConvertedType.UINT_8 => allocator.dupeSentinel(u8, "C", 0),
        format.ConvertedType.UINT_16 => allocator.dupeSentinel(u8, "S", 0),
        format.ConvertedType.UINT_32 => allocator.dupeSentinel(u8, "I", 0),
        format.ConvertedType.UINT_64 => allocator.dupeSentinel(u8, "L", 0),
        format.ConvertedType.ENUM => allocator.dupeSentinel(u8, "u", 0),
        format.ConvertedType.JSON => allocator.dupeSentinel(u8, "u", 0),
        format.ConvertedType.BSON => allocator.dupeSentinel(u8, "z", 0),
        format.ConvertedType.INTERVAL => allocator.dupeSentinel(u8, "w:12", 0),
        format.ConvertedType.DECIMAL => decimalFormat(allocator, elem.precision orelse 0, elem.scale orelse 0),
        else => physicalTypeToArrowFormat(allocator, elem),
    };
}

fn physicalTypeToArrowFormat(allocator: Allocator, elem: format.SchemaElement) SchemaConvError![:0]u8 {
    const pt = elem.type_ orelse return error.InvalidSchema;
    return switch (pt) {
        .boolean => allocator.dupeSentinel(u8, "b", 0),
        .int32 => allocator.dupeSentinel(u8, "i", 0),
        .int64 => allocator.dupeSentinel(u8, "l", 0),
        .int96 => allocator.dupeSentinel(u8, "tsn:", 0),
        .float => allocator.dupeSentinel(u8, "f", 0),
        .double => allocator.dupeSentinel(u8, "g", 0),
        .byte_array => allocator.dupeSentinel(u8, "z", 0),
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
/// The caller owns the returned slice and must call `freeImportedColumnDefs` to free it.
/// Names are copied, so `schema` can be released once this returns.
pub fn importSchemaFromArrow(allocator: Allocator, schema: *const ArrowSchema) ![]ColumnDef {
    const fmt = std.mem.sliceTo(schema.format, 0);
    if (!std.mem.eql(u8, fmt, "+s")) return error.InvalidSchema;

    const nc = safe.castTo(usize, schema.n_children) catch return error.InvalidSchema;
    const col_defs = try allocator.alloc(ColumnDef, nc);
    var built: usize = 0;
    errdefer {
        for (col_defs[0..built]) |*cd| freeImportedColumnDef(allocator, cd);
        allocator.free(col_defs);
    }

    const children: [*]*ArrowSchema = schema.children orelse return error.InvalidSchema;
    for (0..nc) |i| {
        const child = children[i];
        col_defs[i] = try arrowSchemaToColumnDef(allocator, child);
        built = i + 1;
    }

    return col_defs;
}

/// Free ColumnDef[] returned by importSchemaFromArrow, including any
/// heap-allocated struct_fields within individual column definitions.
pub fn freeImportedColumnDefs(allocator: Allocator, col_defs: []ColumnDef) void {
    for (col_defs) |*cd| freeImportedColumnDef(allocator, cd);
    allocator.free(col_defs);
}

/// Frees the names, struct fields and schema nodes of a ColumnDef made by
/// `arrowSchemaToColumnDef`.
fn freeImportedColumnDef(allocator: Allocator, cd: *ColumnDef) void {
    if (cd.struct_fields_owned) {
        if (cd.struct_fields) |fields| for (fields) |f| allocator.free(f.name);
    }
    cd.freeStructFields(allocator);
    if (cd.schema_node) |node| freeSchemaNode(allocator, node);
    cd.schema_node = null;
    allocator.free(cd.name);
}

/// Frees a schema node tree allocated by `arrowSchemaToSchemaNode`.
fn freeSchemaNode(allocator: Allocator, node: *const SchemaNode) void {
    switch (node.*) {
        .optional => |inner| freeSchemaNode(allocator, inner),
        .list => |element| freeSchemaNode(allocator, element),
        .map => |m| {
            freeSchemaNode(allocator, m.key);
            freeSchemaNode(allocator, m.value);
        },
        .struct_ => |st| {
            for (st.fields) |field| {
                freeSchemaNode(allocator, field.node);
                allocator.free(field.name);
            }
            allocator.free(st.fields);
        },
        else => {},
    }
    allocator.destroy(node);
}

/// A ColumnDef of a top-level Arrow column; it owns a copy of every name it holds.
fn arrowSchemaToColumnDef(allocator: Allocator, schema: *const ArrowSchema) !ColumnDef {
    const fmt = std.mem.sliceTo(schema.format, 0);
    const name = try allocator.dupe(u8, if (schema.name) |n| std.mem.sliceTo(n, 0) else "");
    errdefer allocator.free(name);
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

/// Allocates the schema node tree of an Arrow schema; free it with `freeSchemaNode`.
fn arrowSchemaToSchemaNode(allocator: Allocator, schema: *const ArrowSchema, nullable: bool, depth: usize) !*const SchemaNode {
    // The schema comes from the caller and recursion is per level, so the depth is bounded.
    if (depth > max_arrow_depth) return error.InvalidSchema;
    const fmt = std.mem.sliceTo(schema.format, 0);

    // The optional wrapper is allocated first, so nothing can fail after the children exist.
    const opt_node: ?*SchemaNode = if (nullable) allocator.create(SchemaNode) catch return error.OutOfMemory else null;
    errdefer if (opt_node) |o| allocator.destroy(o);
    const node = allocator.create(SchemaNode) catch return error.OutOfMemory;
    errdefer allocator.destroy(node);

    if (std.mem.eql(u8, fmt, "+l") or std.mem.eql(u8, fmt, "+L")) {
        if (schema.n_children != 1) return error.InvalidSchema;
        const children: [*]*ArrowSchema = schema.children orelse return error.InvalidSchema;
        const elem = children[0];
        const elem_nullable = (elem.flags & arrow.ARROW_FLAG_NULLABLE) != 0;
        const elem_node = try arrowSchemaToSchemaNode(allocator, elem, elem_nullable, depth + 1);
        node.* = .{ .list = elem_node };
    } else if (std.mem.eql(u8, fmt, "+s")) {
        const nc = safe.castTo(usize, schema.n_children) catch return error.InvalidSchema;
        const children: [*]*ArrowSchema = schema.children orelse return error.InvalidSchema;
        const fields = allocator.alloc(SchemaNode.Field, nc) catch return error.OutOfMemory;
        var built: usize = 0;
        errdefer {
            for (fields[0..built]) |f| {
                freeSchemaNode(allocator, f.node);
                allocator.free(f.name);
            }
            allocator.free(fields);
        }
        for (0..nc) |i| {
            const child = children[i];
            const child_name = try allocator.dupe(u8, if (child.name) |n| std.mem.sliceTo(n, 0) else "");
            errdefer allocator.free(child_name);
            const child_nullable = (child.flags & arrow.ARROW_FLAG_NULLABLE) != 0;
            fields[i] = .{
                .name = child_name,
                .node = try arrowSchemaToSchemaNode(allocator, child, child_nullable, depth + 1),
            };
            built = i + 1;
        }
        node.* = .{ .struct_ = .{ .fields = fields } };
    } else if (std.mem.eql(u8, fmt, "+m")) {
        if (schema.n_children != 1) return error.InvalidSchema;
        const entries: [*]*ArrowSchema = schema.children orelse return error.InvalidSchema;
        const entries_sch = entries[0];
        if (entries_sch.n_children != 2) return error.InvalidSchema;
        const kv_children: [*]*ArrowSchema = entries_sch.children orelse return error.InvalidSchema;
        const key_node = try arrowSchemaToSchemaNode(allocator, kv_children[0], false, depth + 1);
        errdefer freeSchemaNode(allocator, key_node);
        const val_nullable = (kv_children[1].flags & arrow.ARROW_FLAG_NULLABLE) != 0;
        const val_node = try arrowSchemaToSchemaNode(allocator, kv_children[1], val_nullable, depth + 1);
        node.* = .{ .map = .{ .key = key_node, .value = val_node } };
    } else {
        node.* = try arrowFormatToSchemaNode(fmt);
    }

    if (opt_node) |o| {
        o.* = .{ .optional = node };
        return o;
    }
    return node;
}

/// Parsed Arrow format string → Parquet type mapping.
/// Centralizes format string parsing that was previously duplicated across
/// arrowFormatToSchemaNode, arrowLeafToColumnDef, and other dispatch sites.
const ArrowFormatInfo = struct {
    physical_type: format.PhysicalType,
    logical_type: ?format.LogicalType = null,
    type_length: ?i32 = null,

    fn parse(fmt: []const u8) !ArrowFormatInfo {
        if (fmt.len == 1) {
            return switch (fmt[0]) {
                'b' => .{ .physical_type = .boolean },
                'c' => .{ .physical_type = .int32, .logical_type = .{ .int = .{ .bit_width = 8, .is_signed = true } } },
                'C' => .{ .physical_type = .int32, .logical_type = .{ .int = .{ .bit_width = 8, .is_signed = false } } },
                's' => .{ .physical_type = .int32, .logical_type = .{ .int = .{ .bit_width = 16, .is_signed = true } } },
                'S' => .{ .physical_type = .int32, .logical_type = .{ .int = .{ .bit_width = 16, .is_signed = false } } },
                'i' => .{ .physical_type = .int32 },
                'I' => .{ .physical_type = .int32, .logical_type = .{ .int = .{ .bit_width = 32, .is_signed = false } } },
                'l' => .{ .physical_type = .int64 },
                'L' => .{ .physical_type = .int64, .logical_type = .{ .int = .{ .bit_width = 64, .is_signed = false } } },
                'e' => .{ .physical_type = .fixed_len_byte_array, .type_length = 2, .logical_type = .float16 },
                'f' => .{ .physical_type = .float },
                'g' => .{ .physical_type = .double },
                'u', 'U' => .{ .physical_type = .byte_array, .logical_type = .string },
                'z', 'Z' => .{ .physical_type = .byte_array },
                else => return error.UnsupportedType,
            };
        }
        if (std.mem.eql(u8, fmt, "tdD") or std.mem.eql(u8, fmt, "tdm")) {
            return .{ .physical_type = .int32, .logical_type = .date };
        }
        if (std.mem.eql(u8, fmt, "ttm") or std.mem.eql(u8, fmt, "tts")) {
            return .{ .physical_type = .int32, .logical_type = .{ .time = .{ .is_adjusted_to_utc = false, .unit = .millis } } };
        }
        if (std.mem.eql(u8, fmt, "ttu")) {
            return .{ .physical_type = .int64, .logical_type = .{ .time = .{ .is_adjusted_to_utc = false, .unit = .micros } } };
        }
        if (std.mem.eql(u8, fmt, "ttn")) {
            return .{ .physical_type = .int64, .logical_type = .{ .time = .{ .is_adjusted_to_utc = false, .unit = .nanos } } };
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
            return .{ .physical_type = .int64, .logical_type = .{ .timestamp = .{ .is_adjusted_to_utc = is_utc, .unit = unit } } };
        }
        if (fmt.len >= 2 and fmt[0] == 'w' and fmt[1] == ':') {
            const type_length = std.fmt.parseInt(i32, fmt[2..], 10) catch return error.InvalidSchema;
            // toSchemaNode casts it to u32.
            if (type_length <= 0) return error.InvalidSchema;
            const logical: ?format.LogicalType = if (type_length == 16) .uuid else null;
            return .{ .physical_type = .fixed_len_byte_array, .type_length = type_length, .logical_type = logical };
        }
        if (fmt.len >= 4 and fmt[0] == 'd' and fmt[1] == ':') {
            const params = fmt[2..];
            var parts = std.mem.splitScalar(u8, params, ',');
            const prec_str = parts.next() orelse return error.InvalidSchema;
            const scale_str = parts.next() orelse return error.InvalidSchema;
            const precision = std.fmt.parseInt(i32, prec_str, 10) catch return error.InvalidSchema;
            const scale = std.fmt.parseInt(i32, scale_str, 10) catch return error.InvalidSchema;
            // The bit width (32, 64, 128, 256) is the Arrow layout; the Parquet storage follows
            // from the precision.
            _ = arrowDecimalWidth(fmt) catch return error.InvalidSchema;
            if (precision < 1 or precision > 76) return error.InvalidSchema;
            const decimal: format.LogicalType = .{ .decimal = .{ .precision = precision, .scale = scale } };
            if (precision <= 9) return .{ .physical_type = .int32, .logical_type = decimal };
            if (precision <= 18) return .{ .physical_type = .int64, .logical_type = decimal };
            const len = safe.castTo(i32, decimalStorageBytes(precision)) catch unreachable; // at most 32
            return .{ .physical_type = .fixed_len_byte_array, .type_length = len, .logical_type = decimal };
        }
        return error.UnsupportedType;
    }

    fn toSchemaNode(self: ArrowFormatInfo) SchemaNode {
        return switch (self.physical_type) {
            .boolean => .{ .boolean = .{} },
            .int32 => .{ .int32 = .{ .logical = self.logicalForSchema() } },
            .int64 => .{ .int64 = .{ .logical = self.logicalForSchema() } },
            .float => .{ .float = .{} },
            .double => .{ .double = .{} },
            .byte_array => .{ .byte_array = .{ .logical = self.logicalForSchema() } },
            .fixed_len_byte_array => .{ .fixed_len_byte_array = .{
                .len = safe.castTo(u32, self.type_length orelse 0) catch unreachable, // type_length from parsed format
                .logical = self.logicalForSchema(),
            } },
            .int96 => .{ .int64 = .{} },
        };
    }

    /// Schema nodes don't preserve Arrow int-width annotations
    fn logicalForSchema(self: ArrowFormatInfo) ?format.LogicalType {
        if (self.logical_type) |lt| {
            return switch (lt) {
                .int => null,
                else => lt,
            };
        }
        return null;
    }
};

fn arrowFormatToSchemaNode(fmt: []const u8) !SchemaNode {
    const info = try ArrowFormatInfo.parse(fmt);
    return info.toSchemaNode();
}

fn arrowLeafToColumnDef(fmt: []const u8, name: []const u8, nullable: bool) !ColumnDef {
    const info = try ArrowFormatInfo.parse(fmt);
    return .{
        .name = name,
        .type_ = info.physical_type,
        .optional = nullable,
        .logical_type = info.logical_type,
        .type_length = info.type_length,
    };
}

const decimalByteLength = types.decimalByteLengthRuntime;

fn arrowListToColumnDef(allocator: Allocator, schema: *const ArrowSchema, name: []const u8, nullable: bool) !ColumnDef {
    if (schema.n_children != 1) return error.InvalidSchema;
    const children: [*]*ArrowSchema = schema.children orelse return error.InvalidSchema;
    const element = children[0];
    const elem_fmt = std.mem.sliceTo(element.format, 0);
    const elem_nullable = (element.flags & arrow.ARROW_FLAG_NULLABLE) != 0;

    if (hasNestedFormat(elem_fmt)) {
        const node = try arrowSchemaToSchemaNode(allocator, schema, nullable, 0);
        return ColumnDef.fromNode(name, node);
    }

    var def = try arrowLeafToColumnDef(elem_fmt, name, nullable);
    def.is_list = true;
    def.element_optional = elem_nullable;
    return def;
}

/// Maps are written from a schema node, which keeps the key's and value's logical types and
/// supports every key and value format.
fn arrowMapToColumnDef(allocator: Allocator, schema: *const ArrowSchema, name: []const u8, nullable: bool) !ColumnDef {
    if (schema.n_children != 1) return error.InvalidSchema;
    const entries: [*]*ArrowSchema = schema.children orelse return error.InvalidSchema;
    if (entries[0].n_children != 2) return error.InvalidSchema;
    const node = try arrowSchemaToSchemaNode(allocator, schema, nullable, 0);
    return ColumnDef.fromNode(name, node);
}

fn arrowStructToColumnDef(allocator: Allocator, schema: *const ArrowSchema, name: []const u8, nullable: bool) !ColumnDef {
    const nc = safe.castTo(usize, schema.n_children) catch return error.InvalidSchema;
    const children: [*]*ArrowSchema = schema.children orelse return error.InvalidSchema;

    // Structs with nested or FIXED_LEN_BYTE_ARRAY fields are written from a schema node; the
    // flat struct field writer handles neither.
    var needs_node = false;
    for (0..nc) |i| {
        const child_fmt = std.mem.sliceTo(children[i].format, 0);
        if (hasNestedFormat(child_fmt)) {
            needs_node = true;
            break;
        }
        const info = ArrowFormatInfo.parse(child_fmt) catch continue;
        if (info.physical_type == .fixed_len_byte_array) {
            needs_node = true;
            break;
        }
    }

    if (needs_node) {
        const node = try arrowSchemaToSchemaNode(allocator, schema, nullable, 0);
        return ColumnDef.fromNode(name, node);
    }

    const struct_fields = try allocator.alloc(StructField, nc);
    var built: usize = 0;
    errdefer {
        for (struct_fields[0..built]) |f| allocator.free(f.name);
        allocator.free(struct_fields);
    }

    for (0..nc) |i| {
        const child = children[i];
        const child_fmt = std.mem.sliceTo(child.format, 0);
        const child_nullable = (child.flags & arrow.ARROW_FLAG_NULLABLE) != 0;

        const child_def = try arrowLeafToColumnDef(child_fmt, "", child_nullable);
        const child_name = try allocator.dupe(u8, if (child.name) |n| std.mem.sliceTo(n, 0) else "");
        built = i + 1;
        struct_fields[i] = .{
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
        .struct_fields = struct_fields,
        .struct_fields_owned = true,
    };
}

// ============================================================================
// Private data for ArrowArray release callback
// ============================================================================

/// Alignment of every buffer this module allocates for an ArrowArray. The Arrow C Data
/// Interface requires at least 8 bytes and recommends 64.
pub const buffer_alignment: std.mem.Alignment = .@"64";
pub const Buffer = []align(buffer_alignment.toByteUnits()) u8;

fn allocBuffer(allocator: Allocator, len: usize) Allocator.Error!Buffer {
    return allocator.alignedAlloc(u8, buffer_alignment, len);
}

const ArrayPrivateData = struct {
    allocator: Allocator,
    buffer_allocs: [3]?Buffer,
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

/// Options of `readColumns` and `DynamicReader.readColumns`.
pub const RowGroupOptions = struct {
    /// Top-level columns to read, in this order; null reads all.
    columns: ?[]const usize = null,
    /// Decodes the columns, and the leaves of nested columns, as tasks of this `Io`, so an
    /// `Io` whose `async` uses threads (`std.Io.Threaded`) decodes them in parallel; see
    /// `parallel`. Then `allocator` and the source must allow concurrent use. Null decodes
    /// them one after another on the calling thread.
    io: ?std.Io = null,
};

/// Reads the columns of row group `rg_index` as Arrow arrays: one ArrowArray per top-level
/// column read, and a schema whose children describe those columns in the same order.
/// `DynamicReader.readColumns` calls this with the reader's source and metadata.
pub fn readColumns(
    allocator: Allocator,
    source: SeekableReader,
    metadata: format.FileMetaData,
    rg_index: usize,
    options: RowGroupOptions,
) !ReadResult {
    if (rg_index >= metadata.row_groups.len) return error.InvalidArgument;
    const rg = &metadata.row_groups[rg_index];

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const columns = try buildSchemaTree(arena.allocator(), metadata.schema);

    const col_indices = options.columns;
    const num_cols = if (col_indices) |ci| ci.len else columns.len;
    if (col_indices) |ci| for (ci) |c| if (c >= columns.len) return error.InvalidArgument;

    const arrays = try allocator.alloc(ArrowArray, num_cols);
    errdefer allocator.free(arrays);
    const built = try allocator.alloc(bool, num_cols);
    defer allocator.free(built);
    @memset(built, false);
    errdefer for (arrays, built) |*a, b| if (b) a.doRelease();

    const task: ColumnTask = .{
        .allocator = allocator,
        .source = source,
        .metadata = metadata,
        .rg = rg,
        .columns = columns,
        .col_indices = col_indices,
        .arrays = arrays,
        .built = built,
        .io = options.io,
    };
    try parallel.forEach(options.io, allocator, num_cols, &task, ColumnTask.run);

    var schema = try treeSchema(allocator, metadata.schema, columns, col_indices, false);
    errdefer schema.doRelease();

    return .{
        .arrays = arrays,
        .schema = schema,
        .allocator = allocator,
    };
}

/// Reads output column `i` of `readColumns` into `arrays[i]`.
const ColumnTask = struct {
    allocator: Allocator,
    source: SeekableReader,
    metadata: format.FileMetaData,
    rg: *const format.RowGroup,
    columns: []const Node,
    col_indices: ?[]const usize,
    arrays: []ArrowArray,
    built: []bool,
    io: ?std.Io,

    fn run(self: *const ColumnTask, i: usize) ReaderError!void {
        const column = &self.columns[if (self.col_indices) |ci| ci[i] else i];
        self.arrays[i] = if (column.kind == .leaf)
            try readColumnAsArrow(self.allocator, self.source, self.metadata, self.rg, column.physical_start)
        else
            try readNestedColumn(self.allocator, self.source, self.metadata, self.rg, column, self.io);
        self.built[i] = true;
    }
};

pub const BatchOptions = struct {
    /// Top-level columns to read, in this order; null reads all.
    columns: ?[]const usize = null,
    /// Row groups to read, in this order; null reads all.
    row_groups: ?[]const usize = null,
    /// Rows per batch. Batches span row groups; only the last batch can have fewer rows.
    /// Null returns one batch per row group.
    batch_rows: ?usize = 65536,
    /// Read BYTE_ARRAY columns as Arrow large_string / large_binary (formats "U" and "Z",
    /// i64 offsets). With i32 offsets, a batch whose column holds more than 2^31 - 1 bytes
    /// of byte-array data returns `error.IntegerOverflow`.
    large_offsets: bool = false,
    /// Decodes each batch's leaf columns, and assembles its columns, as tasks of this `Io`,
    /// so an `Io` whose `async` uses threads (`std.Io.Threaded`) does them in parallel; see
    /// `parallel`. Then the allocator and the source must allow concurrent use. Null does
    /// them one after another on the calling thread.
    io: ?std.Io = null,
    /// Row groups to decode ahead of the batches, as tasks of `io` (required when this is
    /// above 0). Each such row group is decoded whole and cut into the pieces the batches
    /// take, while the caller consumes earlier batches; see `ArrowBatchReader`. 0 decodes
    /// pages as batches need them.
    read_ahead: usize = 0,
};

/// Reads a file's columns as Arrow arrays, `batch_rows` rows at a time. A batch takes rows
/// from as many row groups as it needs.
///
/// With `read_ahead` 0, pages of the selected columns are read from the source and decoded
/// as batches need them; a page's entries past the end of a batch are kept for the next one.
/// Memory held between calls, per leaf column: one compressed page plus up to 1 KiB after it
/// (the start of the next page header), the dictionary, at most one page of decoded entries,
/// and buffers with room for the next batch's entries.
///
/// With `read_ahead` k above 0, the selected columns of a row group are decoded whole, as
/// one task of `io` per row group, and k row groups are decoded ahead of the one the batches
/// take rows from. Memory held between calls: the decoded entries of up to k + 1 row groups.
/// An error in a row group is returned by the first batch that takes rows from it.
///
/// After `next` returns an error, later calls return `error.InvalidState`; call `deinit`.
pub const ArrowBatchReader = struct {
    allocator: Allocator,
    source: SeekableReader,
    metadata: format.FileMetaData,
    /// Holds the schema tree and the column and row group lists.
    arena: std.heap.ArenaAllocator,
    tree: []const Node,
    columns: []const usize,
    row_groups: []const usize,
    batch_rows: ?usize,
    large_offsets: bool,
    io: ?std.Io,
    /// Levels of the selected columns' leaves, in column order.
    leaf_info: []const LeafInfo,
    /// Position in `row_groups` of the next row group to open.
    next_rg: usize = 0,
    /// Rows of the open row group not yet returned.
    rows_left: usize = 0,
    /// One entry per selected column while a row group is open.
    open: []BatchColumn = &.{},
    /// The cursors of `open`'s leaves, in column order.
    cursors: []*leaf_decoder.ColumnCursor = &.{},

    /// With `read_ahead`: row groups being decoded, in `row_groups` order.
    read_ahead: usize,
    pending: std.ArrayList(std.Io.Future(ReaderError!Decoded)) = .empty,
    /// With `read_ahead`: the decoded row group the batches take rows from.
    current: ?Decoded = null,
    /// With `read_ahead`: rows a batch holds when row group `next_rg` starts.
    next_fill: usize = 0,
    /// Set when `next` returns an error; later calls return `error.InvalidState`.
    failed: bool = false,

    const BatchLeaf = struct {
        cursor: leaf_decoder.ColumnCursor,
    };

    const LeafInfo = struct {
        max_def: u32,
        max_rep: u32,
        /// The leaf's column chunk index.
        col_idx: usize,
        /// Whether the leaf is part of a list, map or struct column, which needs its levels.
        nested: bool,
        /// `leaf_decoder.Options.slot_level` of the leaf (see `slotLevels`).
        slot_level: u32,
    };

    /// A row group decoded by `RowGroupJob`, cut into the pieces batches take.
    const Decoded = struct {
        allocator: Allocator,
        /// Rows of each piece.
        piece_rows: []usize,
        /// Piece `p` of leaf `k` is `pieces[k * piece_rows.len + p]`.
        pieces: []leaf_decoder.Leaf,
        /// The arrays of the pieces that are whole batches, assembled by the task; their
        /// leaves in `pieces` were moved out.
        batches: []?[]ArrowArray,
        /// Pieces before this one have been taken by batches.
        next_piece: usize = 0,

        fn deinit(self: *Decoded) void {
            const n = self.piece_rows.len;
            const leaves = if (n > 0) self.pieces.len / n else 0;
            for (self.next_piece..n) |p| {
                if (self.batches[p]) |arrays| {
                    for (arrays) |*arr| arr.doRelease();
                    self.allocator.free(arrays);
                } else {
                    for (0..leaves) |k| self.pieces[k * n + p].deinit(self.allocator);
                }
            }
            self.allocator.free(self.batches);
            self.allocator.free(self.pieces);
            self.allocator.free(self.piece_rows);
        }
    };

    const BatchColumn = struct {
        node: *const Node,
        leaves: []BatchLeaf,
    };

    pub fn init(allocator: Allocator, source: SeekableReader, metadata: format.FileMetaData, options: BatchOptions) !ArrowBatchReader {
        if (options.batch_rows == 0) return error.InvalidArgument;
        if (options.read_ahead > 0 and options.io == null) return error.InvalidArgument;
        var arena = std.heap.ArenaAllocator.init(allocator);
        errdefer arena.deinit();
        const a = arena.allocator();
        const tree = try buildSchemaTree(a, metadata.schema);

        const columns = if (options.columns) |c| try a.dupe(usize, c) else blk: {
            const all = try a.alloc(usize, tree.len);
            for (all, 0..) |*c, i| c.* = i;
            break :blk all;
        };
        for (columns) |c| if (c >= tree.len) return error.InvalidArgument;

        const row_groups = if (options.row_groups) |r| try a.dupe(usize, r) else blk: {
            const all = try a.alloc(usize, metadata.row_groups.len);
            for (all, 0..) |*r, i| r.* = i;
            break :blk all;
        };
        for (row_groups) |r| if (r >= metadata.row_groups.len) return error.InvalidArgument;

        var leaf_count: usize = 0;
        for (columns) |c| leaf_count += tree[c].physical_count;
        const leaf_info = try a.alloc(LeafInfo, leaf_count);
        var k: usize = 0;
        for (columns) |c| {
            const node = &tree[c];
            if (node.physical_count == 0) return error.InvalidSchema;
            const slot_levels = try a.alloc(u32, node.physical_count);
            slotLevels(node, node.physical_start, slot_levels);
            for (0..node.physical_count) |j| {
                const col_idx = node.physical_start + j;
                const info = format.getColumnInfo(metadata.schema, col_idx) orelse return error.InvalidSchema;
                leaf_info[k] = .{
                    .max_def = info.max_def_level,
                    .max_rep = info.max_rep_level,
                    .col_idx = col_idx,
                    .nested = node.kind != .leaf,
                    .slot_level = slot_levels[j],
                };
                k += 1;
            }
        }

        return .{
            .allocator = allocator,
            .source = source,
            .metadata = metadata,
            .arena = arena,
            .tree = tree,
            .columns = columns,
            .row_groups = row_groups,
            .batch_rows = options.batch_rows,
            .large_offsets = options.large_offsets,
            .io = options.io,
            .read_ahead = options.read_ahead,
            .leaf_info = leaf_info,
        };
    }

    pub fn deinit(self: *ArrowBatchReader) void {
        self.closeRowGroup();
        for (self.pending.items) |*f| {
            var d = f.cancel(self.io.?) catch continue;
            d.deinit();
        }
        self.pending.deinit(self.allocator);
        if (self.current) |*d| d.deinit();
        self.arena.deinit();
    }

    /// The schema of the batches: a struct whose children are the selected columns.
    pub fn schema(self: *const ArrowBatchReader) !ArrowSchema {
        return treeSchema(self.allocator, self.metadata.schema, self.tree, self.columns, self.large_offsets);
    }

    /// The next batch, or null after the last row group.
    pub fn next(self: *ArrowBatchReader) !?ReadResult {
        // After an error the cursors and decoded row groups are in an undefined state.
        if (self.failed) return error.InvalidState;
        errdefer self.failed = true;
        const allocator = self.allocator;
        if (self.read_ahead > 0) {
            // A batch the row group's task assembled.
            if (!try self.advanceCurrent()) return null;
            const c = &self.current.?;
            if (c.batches[c.next_piece]) |arrays| {
                c.batches[c.next_piece] = null;
                c.next_piece += 1;
                errdefer {
                    for (arrays) |*arr| arr.doRelease();
                    allocator.free(arrays);
                }
                const sch = try self.schema();
                return .{ .arrays = arrays, .schema = sch, .allocator = allocator };
            }
        }
        // Decoded rows of each leaf, one piece per row group the batch takes rows from.
        const parts = try allocator.alloc(std.ArrayList(leaf_decoder.Leaf), self.leaf_info.len);
        for (parts) |*p| p.* = .empty;
        defer {
            for (parts) |*p| {
                for (p.items) |*l| l.deinit(allocator);
                p.deinit(allocator);
            }
            allocator.free(parts);
        }

        const rows = if (self.read_ahead > 0) try self.takeDecoded(parts) else try self.decodePieces(parts);
        if (rows == 0) return null;

        const assembly: Assembly = .{
            .allocator = allocator,
            .schema = self.metadata.schema,
            .tree = self.tree,
            .columns = self.columns,
            .leaf_info = self.leaf_info,
            .io = self.io,
        };
        const arrays = try assembly.arrays(parts, rows);
        errdefer {
            for (arrays) |*arr| arr.doRelease();
            allocator.free(arrays);
        }
        const sch = try self.schema();
        return .{ .arrays = arrays, .schema = sch, .allocator = allocator };
    }

    /// With `read_ahead`: makes `current` a decoded row group with pieces not yet taken,
    /// waiting for its task. Returns false after the last row group.
    fn advanceCurrent(self: *ArrowBatchReader) !bool {
        while (true) {
            if (self.current) |*c| {
                if (c.next_piece < c.piece_rows.len) return true;
                c.deinit();
                self.current = null;
            }
            try self.startDecoding();
            if (self.pending.items.len == 0) return false;
            var future = self.pending.orderedRemove(0);
            self.current = try future.await(self.io.?);
        }
    }

    /// Decodes the rows of the next batch from the open row group's cursors, opening row
    /// groups as needed, and appends them to `parts`. Returns the number of rows.
    fn decodePieces(self: *ArrowBatchReader, parts: []std.ArrayList(leaf_decoder.Leaf)) !usize {
        var rows: usize = 0;
        while (true) {
            if (self.batch_rows) |b| if (rows == b) break;
            if (self.rows_left == 0) {
                // Without batch_rows, a batch is one row group.
                if (self.batch_rows == null and rows > 0) break;
                self.closeRowGroup();
                if (self.next_rg == self.row_groups.len) break;
                try self.openRowGroup(self.row_groups[self.next_rg]);
                self.next_rg += 1;
                continue;
            }
            const n = if (self.batch_rows) |b| @min(b - rows, self.rows_left) else self.rows_left;
            const pieces: PieceTask = .{ .allocator = self.allocator, .cursors = self.cursors, .parts = parts, .rows = n };
            try parallel.forEach(self.io, self.allocator, self.cursors.len, &pieces, PieceTask.run);
            self.rows_left -= n;
            rows += n;
        }
        return rows;
    }

    /// Moves the pieces of the next batch from decoded row groups to `parts`, starting the
    /// decoding of further row groups. Returns the number of rows.
    fn takeDecoded(self: *ArrowBatchReader, parts: []std.ArrayList(leaf_decoder.Leaf)) !usize {
        const allocator = self.allocator;
        var rows: usize = 0;
        while (true) {
            if (self.batch_rows) |b| if (rows == b) break;
            const cur = if (self.current) |*c| c else null;
            if (cur == null or cur.?.next_piece == cur.?.piece_rows.len) {
                // Without batch_rows, a batch is one row group.
                if (self.batch_rows == null and rows > 0) break;
                if (!try self.advanceCurrent()) break;
                continue;
            }
            const c = cur.?;
            // Assembled pieces are whole batches, which `next` takes before calling this.
            if (c.batches[c.next_piece] != null) return error.InvalidPageData;
            for (parts) |*p| try p.ensureUnusedCapacity(allocator, 1);
            const n = c.piece_rows.len;
            for (parts, 0..) |*p, k| p.appendAssumeCapacity(c.pieces[k * n + c.next_piece]);
            rows += c.piece_rows[c.next_piece];
            c.next_piece += 1;
        }
        return rows;
    }

    /// Starts decoding row groups until `read_ahead` + 1 are decoding or decoded and not
    /// yet taken from, besides the current one.
    fn startDecoding(self: *ArrowBatchReader) !void {
        const io = self.io.?;
        while (self.pending.items.len < self.read_ahead + 1 and self.next_rg < self.row_groups.len) {
            try self.pending.ensureUnusedCapacity(self.allocator, 1);
            const rg = &self.metadata.row_groups[self.row_groups[self.next_rg]];
            const job: RowGroupJob = .{
                .allocator = self.allocator,
                .source = self.source,
                .schema = self.metadata.schema,
                .rg = rg,
                .tree = self.tree,
                .columns = self.columns,
                .leaf_info = self.leaf_info,
                .batch_rows = self.batch_rows,
                .fill = self.next_fill,
                .last = self.next_rg + 1 == self.row_groups.len,
                .large_offsets = self.large_offsets,
                .io = io,
            };
            self.pending.appendAssumeCapacity(io.async(RowGroupJob.run, .{job}));
            self.next_rg += 1;
            // The job returns error.InvalidRowCount for a negative row count; the fill of
            // later row groups does not matter then.
            if (self.batch_rows) |b| {
                const n = safe.castTo(usize, rg.num_rows) catch 0;
                // (next_fill + n) % b, without overflow when b is near maxInt(usize).
                const add = n % b;
                self.next_fill = if (add >= b - self.next_fill) add - (b - self.next_fill) else self.next_fill + add;
            }
        }
    }

    /// Decodes the selected columns of a row group and cuts each leaf into the pieces the
    /// batches take: a batch holding `fill` rows takes the first piece. Assembles the pieces
    /// that are whole batches: those that start a batch and fill it, or end the last row
    /// group (`last`).
    const RowGroupJob = struct {
        allocator: Allocator,
        source: SeekableReader,
        schema: []const format.SchemaElement,
        rg: *const format.RowGroup,
        tree: []const Node,
        columns: []const usize,
        leaf_info: []const LeafInfo,
        batch_rows: ?usize,
        fill: usize,
        last: bool,
        large_offsets: bool,
        io: std.Io,

        fn run(job: RowGroupJob) ReaderError!Decoded {
            return job.decode() catch |e| asErrorOf(ReaderError, e);
        }

        fn decode(job: RowGroupJob) !Decoded {
            const allocator = job.allocator;
            const num_rows = safe.castTo(usize, job.rg.num_rows) catch return error.InvalidRowCount;
            // Each row has at least one entry in every leaf, so a row count above a chunk's
            // value count is wrong; the pieces below are sized from it before any decoding.
            for (job.leaf_info) |info| {
                if (info.col_idx >= job.rg.columns.len) return error.InvalidArgument;
                const meta = job.rg.columns[info.col_idx].meta_data orelse return error.InvalidArgument;
                const values = safe.castTo(usize, meta.num_values) catch return error.InvalidRowCount;
                if (num_rows > values) return error.InvalidRowCount;
            }

            var piece_rows: std.ArrayList(usize) = .empty;
            defer piece_rows.deinit(allocator);
            var left = num_rows;
            var fill = job.fill;
            while (left > 0) {
                const n = if (job.batch_rows) |b| @min(b - fill, left) else left;
                try piece_rows.append(allocator, n);
                left -= n;
                fill = if (job.batch_rows) |b| (fill + n) % b else 0;
            }

            const np = piece_rows.items.len;
            const num_leaves = job.leaf_info.len;
            const pieces = try allocator.alloc(leaf_decoder.Leaf, num_leaves * np);
            errdefer allocator.free(pieces);
            const done = try allocator.alloc(bool, num_leaves);
            defer allocator.free(done);
            @memset(done, false);
            const batches = try allocator.alloc(?[]ArrowArray, np);
            errdefer allocator.free(batches);
            @memset(batches, null);
            // Pieces whose leaves were moved out to be assembled.
            const moved = try allocator.alloc(bool, np);
            defer allocator.free(moved);
            @memset(moved, false);
            errdefer for (done, 0..) |d, k| if (d) for (0..np) |p| if (!moved[p]) pieces[k * np + p].deinit(allocator);
            errdefer for (batches) |b| if (b) |arrays| {
                for (arrays) |*arr| arr.doRelease();
                allocator.free(arrays);
            };

            const task: LeafPiecesTask = .{ .job = &job, .piece_rows = piece_rows.items, .pieces = pieces, .done = done };
            try parallel.forEach(job.io, allocator, num_leaves, &task, LeafPiecesTask.run);

            // One list of one piece per leaf, for `Assembly.arrays`.
            const parts = try allocator.alloc(std.ArrayList(leaf_decoder.Leaf), num_leaves);
            for (parts) |*part| part.* = .empty;
            defer {
                for (parts) |*part| {
                    for (part.items) |*l| l.deinit(allocator);
                    part.deinit(allocator);
                }
                allocator.free(parts);
            }
            for (parts) |*part| try part.ensureTotalCapacity(allocator, 1);
            const assembly: Assembly = .{
                .allocator = allocator,
                .schema = job.schema,
                .tree = job.tree,
                .columns = job.columns,
                .leaf_info = job.leaf_info,
                .io = job.io,
            };
            var batch_fill = job.fill;
            for (piece_rows.items, 0..) |n, p| {
                const whole = if (job.batch_rows) |b| batch_fill == 0 and (n == b or (job.last and p == np - 1)) else true;
                batch_fill = if (job.batch_rows) |b| (batch_fill + n) % b else 0;
                if (!whole) continue;
                moved[p] = true;
                for (parts, 0..) |*part, k| part.appendAssumeCapacity(pieces[k * np + p]);
                batches[p] = try assembly.arrays(parts, n);
                for (parts) |*part| {
                    for (part.items) |*l| l.deinit(allocator);
                    part.clearRetainingCapacity();
                }
            }
            return .{ .allocator = allocator, .piece_rows = try piece_rows.toOwnedSlice(allocator), .pieces = pieces, .batches = batches };
        }
    };

    /// Decodes leaf `k` of a `RowGroupJob` into its pieces.
    const LeafPiecesTask = struct {
        job: *const RowGroupJob,
        piece_rows: []const usize,
        pieces: []leaf_decoder.Leaf,
        done: []bool,

        fn run(self: *const LeafPiecesTask, k: usize) ReaderError!void {
            const job = self.job;
            const allocator = job.allocator;
            const info = job.leaf_info[k];
            var cursor = openCursor(allocator, job.source, job.schema, job.rg, info, job.large_offsets, job.batch_rows) catch |e|
                return asErrorOf(ReaderError, e);
            defer cursor.deinit();
            const out = self.pieces[k * self.piece_rows.len ..][0..self.piece_rows.len];
            for (out, self.piece_rows, 0..) |*l, n, p| {
                l.* = cursor.next(n) catch |e| {
                    for (out[0..p]) |*prev| prev.deinit(allocator);
                    return asErrorOf(ReaderError, e);
                };
            }
            self.done[k] = true;
        }
    };

    /// Opens a cursor over the column chunk of leaf `info` in row group `rg`.
    fn openCursor(
        allocator: Allocator,
        source: SeekableReader,
        schema_elems: []const format.SchemaElement,
        rg: *const format.RowGroup,
        info: LeafInfo,
        large_offsets: bool,
        batch_rows: ?usize,
    ) !leaf_decoder.ColumnCursor {
        if (info.col_idx >= rg.columns.len) return error.InvalidArgument;
        const chunk = &rg.columns[info.col_idx];
        const meta = chunk.meta_data orelse return error.InvalidArgument;
        const col_info = format.getColumnInfo(schema_elems, info.col_idx) orelse return error.InvalidArgument;
        const range = try parquet_reader.columnChunkRange(chunk);
        const pages = leaf_decoder.PageReader.fromSource(allocator, source, range.start, range.size);
        const reserve = @min(batch_rows orelse std.math.maxInt(usize), leaf_decoder.reservedEntries(meta.num_values));
        return leaf_decoder.ColumnCursor.init(allocator, pages, meta, col_info, .{
            .keep_levels = info.nested,
            .large_offsets = large_offsets,
            .slot_level = if (info.nested) info.slot_level else 0,
        }, reserve);
    }

    const CursorError = @typeInfo(@typeInfo(@TypeOf(leaf_decoder.ColumnCursor.next)).@"fn".return_type.?).error_union.error_set;

    /// Decodes the next `rows` rows of leaf `k` and appends them to `parts[k]`.
    const PieceTask = struct {
        allocator: Allocator,
        cursors: []const *leaf_decoder.ColumnCursor,
        parts: []std.ArrayList(leaf_decoder.Leaf),
        rows: usize,

        fn run(self: *const PieceTask, k: usize) (CursorError || Allocator.Error)!void {
            var piece = try self.cursors[k].next(self.rows);
            self.parts[k].append(self.allocator, piece) catch |e| {
                piece.deinit(self.allocator);
                return e;
            };
        }
    };

    /// What assembling the arrays of a batch needs: the selected columns and their leaves.
    const Assembly = struct {
        allocator: Allocator,
        schema: []const format.SchemaElement,
        tree: []const Node,
        columns: []const usize,
        leaf_info: []const LeafInfo,
        io: ?std.Io,

        /// Assembles the arrays of a batch of `rows` rows from `parts`, the pieces of each
        /// leaf, with the columns as tasks of `io`. Pieces moved out of `parts` are removed
        /// from it.
        fn arrays(self: *const Assembly, parts: []std.ArrayList(leaf_decoder.Leaf), rows: usize) ReaderError![]ArrowArray {
            const allocator = self.allocator;
            const out = try allocator.alloc(ArrowArray, self.columns.len);
            errdefer allocator.free(out);
            const built = try allocator.alloc(bool, self.columns.len);
            defer allocator.free(built);
            @memset(built, false);
            errdefer for (out, built) |*arr, b| if (b) arr.doRelease();
            // The first leaf of each column in `parts`.
            const first_leaf = try allocator.alloc(usize, self.columns.len);
            defer allocator.free(first_leaf);
            var k: usize = 0;
            for (self.columns, first_leaf) |c, *f| {
                f.* = k;
                k += self.tree[c].physical_count;
            }
            const task: AssembleTask = .{ .assembly = self, .parts = parts, .first_leaf = first_leaf, .arrays = out, .built = built };
            parallel.forEach(self.io, allocator, self.columns.len, &task, AssembleTask.run) catch |e| return asErrorOf(ReaderError, e);
            for (out) |arr| if (arr.length != rows) return error.InvalidPageData;
            return out;
        }
    };

    /// Assembles output column `i` of a batch into `arrays[i]`.
    const AssembleTask = struct {
        assembly: *const Assembly,
        parts: []std.ArrayList(leaf_decoder.Leaf),
        first_leaf: []const usize,
        arrays: []ArrowArray,
        built: []bool,

        fn run(self: *const AssembleTask, i: usize) ReaderError!void {
            const a = self.assembly;
            const node = &a.tree[a.columns[i]];
            const k = self.first_leaf[i];
            const count = node.physical_count;
            self.arrays[i] = try assembleColumn(a.allocator, a.schema, node, self.parts[k..][0..count], a.leaf_info[k..][0..count]);
            self.built[i] = true;
        }
    };

    /// Joins each leaf's pieces and assembles the column's array. Pieces moved out of
    /// `parts` are removed from it.
    fn assembleColumn(allocator: Allocator, schema_elems: []const format.SchemaElement, node: *const Node, parts: []std.ArrayList(leaf_decoder.Leaf), info: []const LeafInfo) ReaderError!ArrowArray {
        const leaves = try allocator.alloc(NestedLeaf, parts.len);
        var joined: usize = 0;
        defer {
            for (leaves[0..joined]) |*l| l.leaf.deinit(allocator);
            allocator.free(leaves);
        }
        for (leaves, parts, info, 0..) |*l, *p, levels, k| {
            if (p.items.len == 0) return error.InvalidPageData;
            if (p.items.len == 1) {
                l.leaf = p.items[0];
                p.clearRetainingCapacity();
            } else {
                l.leaf = leaf_decoder.concatLeaves(allocator, p.items) catch |e| return asErrorOf(ReaderError, e);
            }
            l.entries = levelCount(&l.leaf);
            l.max_def = levels.max_def;
            l.max_rep = levels.max_rep;
            joined = k + 1;
        }
        if (node.kind == .leaf) return leafToArrow(allocator, leaves[0].leaf.buffers.takeAll(), schema_elems[node.schema_idx]);
        return assembleNode(allocator, schema_elems, node, leaves, node.physical_start);
    }

    fn openRowGroup(self: *ArrowBatchReader, rg_index: usize) !void {
        const allocator = self.allocator;
        const rg = &self.metadata.row_groups[rg_index];
        const num_rows = safe.castTo(usize, rg.num_rows) catch return error.InvalidRowCount;

        const open = try allocator.alloc(BatchColumn, self.columns.len);
        var opened: usize = 0;
        errdefer {
            for (open[0..opened]) |*col| freeColumn(allocator, col);
            allocator.free(open);
        }
        var first_leaf: usize = 0;
        for (open, self.columns) |*col, c| {
            const node = &self.tree[c];
            if (node.physical_count == 0) return error.InvalidSchema;
            const leaves = try allocator.alloc(BatchLeaf, node.physical_count);
            var leaf_count: usize = 0;
            errdefer {
                for (leaves[0..leaf_count]) |*l| freeLeaf(l);
                allocator.free(leaves);
            }
            for (leaves, self.leaf_info[first_leaf..][0..leaves.len], 0..) |*l, info, k| {
                const cursor = try openCursor(allocator, self.source, self.metadata.schema, rg, info, self.large_offsets, self.batch_rows);
                l.* = .{ .cursor = cursor };
                leaf_count = k + 1;
            }
            first_leaf += leaves.len;
            col.* = .{ .node = node, .leaves = leaves };
            opened += 1;
        }
        const cursors = try allocator.alloc(*leaf_decoder.ColumnCursor, self.leaf_info.len);
        var k: usize = 0;
        for (open) |*col| for (col.leaves) |*l| {
            cursors[k] = &l.cursor;
            k += 1;
        };
        self.open = open;
        self.cursors = cursors;
        self.rows_left = num_rows;
    }

    fn closeRowGroup(self: *ArrowBatchReader) void {
        for (self.open) |*col| freeColumn(self.allocator, col);
        if (self.open.len > 0) self.allocator.free(self.open);
        if (self.cursors.len > 0) self.allocator.free(self.cursors);
        self.open = &.{};
        self.cursors = &.{};
        self.rows_left = 0;
    }

    fn freeColumn(allocator: Allocator, col: *BatchColumn) void {
        for (col.leaves) |*l| freeLeaf(l);
        allocator.free(col.leaves);
    }

    fn freeLeaf(l: *BatchLeaf) void {
        l.cursor.deinit();
    }
};

/// A leaf column of a nested column, decoded with its levels.
const NestedLeaf = struct {
    /// Decoded with `Options.slot_level` from `slotLevels`: the buffers have no slots for
    /// the entries of empty and null lists and maps.
    leaf: leaf_decoder.Leaf,
    /// Entries decoded: the number of levels.
    entries: usize,
    max_def: u32,
    max_rep: u32,
};

/// Reads a list, map or struct column: decodes its leaf columns with levels and assembles
/// the Arrow array from the levels (see `LevelInfo`).
fn readNestedColumn(
    allocator: Allocator,
    source: SeekableReader,
    metadata: format.FileMetaData,
    rg: *const format.RowGroup,
    node: *const Node,
    io: ?std.Io,
) ReaderError!ArrowArray {
    if (node.physical_count == 0) return error.InvalidSchema;
    if (node.physical_start + node.physical_count > rg.columns.len) return error.InvalidArgument;
    const leaves = try allocator.alloc(NestedLeaf, node.physical_count);
    defer allocator.free(leaves);
    const decoded = try allocator.alloc(bool, node.physical_count);
    defer allocator.free(decoded);
    @memset(decoded, false);
    defer for (leaves, decoded) |*l, d| if (d) l.leaf.deinit(allocator);
    const slot_levels = try allocator.alloc(u32, node.physical_count);
    defer allocator.free(slot_levels);
    slotLevels(node, node.physical_start, slot_levels);
    const task: NestedLeafTask = .{
        .allocator = allocator,
        .source = source,
        .metadata = metadata,
        .rg = rg,
        .first = node.physical_start,
        .slot_levels = slot_levels,
        .leaves = leaves,
        .decoded = decoded,
    };
    parallel.forEach(io, allocator, leaves.len, &task, NestedLeafTask.run) catch |e| return asErrorOf(ReaderError, e);
    return assembleNode(allocator, metadata.schema, node, leaves, node.physical_start);
}

/// Decodes leaf `first + k` of a nested column, with levels, into `leaves[k]`.
const NestedLeafTask = struct {
    allocator: Allocator,
    source: SeekableReader,
    metadata: format.FileMetaData,
    rg: *const format.RowGroup,
    first: usize,
    slot_levels: []const u32,
    leaves: []NestedLeaf,
    decoded: []bool,

    fn run(self: *const NestedLeafTask, k: usize) ReaderError!void {
        const allocator = self.allocator;
        const col_idx = self.first + k;
        const chunk = &self.rg.columns[col_idx];
        const meta = chunk.meta_data orelse return error.InvalidArgument;
        const page_data = try parquet_reader.readColumnChunkData(allocator, self.source, chunk);
        defer allocator.free(page_data);
        const info = format.getColumnInfo(self.metadata.schema, col_idx) orelse return error.InvalidArgument;
        const l = &self.leaves[k];
        l.leaf = leaf_decoder.decodeLeaf(allocator, page_data, meta, info, .{ .keep_levels = true, .slot_level = self.slot_levels[k] }) catch |e|
            return asErrorOf(ReaderError, e);
        l.entries = levelCount(&l.leaf);
        l.max_def = info.max_def_level;
        l.max_rep = info.max_rep_level;
        self.decoded[k] = true;
    }
};

/// Sets `out[k]` to the `leaf_decoder.Options.slot_level` of leaf column `base + k` under
/// `node`: the definition level of its innermost list or map, or 0 outside lists and maps.
fn slotLevels(node: *const Node, base: usize, out: []u32) void {
    if (node.kind == .leaf) {
        out[node.physical_start - base] = node.levels.ancestor_def_level;
        return;
    }
    for (node.children) |*c| slotLevels(c, base, out);
}

/// The number of entries of a leaf decoded with levels.
fn levelCount(leaf: *const leaf_decoder.Leaf) usize {
    if (leaf.def_levels) |d| return d.len;
    if (leaf.rep_levels) |r| return r.len;
    return leaf.buffers.length;
}

/// Assembles `node` from its decoded leaves; `leaves[k]` is leaf column `base + k`.
/// Shapes are computed from the node's first leaf before the children take their buffers.
fn assembleNode(allocator: Allocator, schema: []const format.SchemaElement, node: *const Node, leaves: []NestedLeaf, base: usize) ReaderError!ArrowArray {
    const first = &leaves[node.physical_start - base];
    switch (node.kind) {
        .leaf => {
            if (first.max_def != node.levels.def_level or first.max_rep != node.levels.rep_level) return error.InvalidSchema;
            // Slots: entries at or above the nearest repeated ancestor's definition level.
            // A leaf decoded with that `slot_level` has buffers with fewer entries than
            // levels, or the same number when every entry has a slot.
            const bufs = if (first.leaf.def_levels) |d|
                if (first.leaf.buffers.length < d.len)
                    first.leaf.buffers.takeAll()
                else
                    first.leaf.buffers.take(allocator, d, node.levels.ancestor_def_level, std.math.maxInt(usize)) catch |e|
                        return asErrorOf(ReaderError, e)
            else
                first.leaf.buffers.takeAll();
            return leafToArrow(allocator, bufs, schema[node.schema_idx]);
        },
        .struct_ => {
            const shape = try levelShape(allocator, first.leaf.def_levels, first.leaf.rep_levels, first.entries, node.levels, false);
            var shape_owned = true;
            errdefer if (shape_owned) allocator.free(shape.validity);

            const children = try allocator.alloc(ArrowArray, node.children.len);
            var init_count: usize = 0;
            var children_owned = true;
            errdefer if (children_owned) {
                for (children[0..init_count]) |*c| c.doRelease();
                allocator.free(children);
            };
            for (node.children, 0..) |*child, i| {
                children[i] = try assembleNode(allocator, schema, child, leaves, base);
                init_count = i + 1;
                if (children[i].length != shape.length) return error.InvalidPageData;
            }
            const array = try buildStructArray(allocator, shape.length, shape.null_count, shape.validity, children);
            shape_owned = false;
            children_owned = false;
            return array;
        },
        .list, .map => {
            const shape = try levelShape(allocator, first.leaf.def_levels, first.leaf.rep_levels, first.entries, node.levels, true);
            var shape_owned = true;
            errdefer if (shape_owned) {
                allocator.free(shape.validity);
                allocator.free(shape.offsets.?);
            };
            var child = try assembleNode(allocator, schema, &node.children[0], leaves, base);
            errdefer if (child.release != null) child.doRelease();
            if (child.length != shape.elements) return error.InvalidPageData;
            const array = try buildListArray(allocator, shape.length, shape.null_count, shape.validity, shape.offsets.?, &child);
            shape_owned = false;
            return array;
        },
    }
}

/// Wraps a leaf's decoded buffers in an ArrowArray, converting decimals to Arrow's layout.
/// Takes ownership of `bufs` (they are freed on error).
fn leafToArrow(allocator: Allocator, bufs: leaf_decoder.Buffers, elem: format.SchemaElement) ReaderError!ArrowArray {
    const d = decimalOf(elem) orelse return buffersToArrow(allocator, bufs);
    return buffersToArrow(allocator, try decimalToArrow(allocator, bufs, elem.type_ orelse return error.InvalidSchema, d.precision));
}

const DecimalParams = struct { precision: i32, scale: i32 };

/// Precision and scale of a decimal leaf, from its logical type or DECIMAL converted type.
fn decimalOf(elem: format.SchemaElement) ?DecimalParams {
    if (elem.logical_type) |lt| return switch (lt) {
        .decimal => |d| .{ .precision = d.precision, .scale = d.scale },
        else => null,
    };
    const ct = elem.converted_type orelse return null;
    if (ct != format.ConvertedType.DECIMAL) return null;
    return .{ .precision = elem.precision orelse 0, .scale = elem.scale orelse 0 };
}

/// Rewrites a decimal leaf's values as Arrow decimal128 (precision up to 38) or decimal256 (up
/// to 76): 16 or 32 bytes per entry, little-endian two's complement. Parquet stores them as
/// native INT32 or INT64, or as big-endian two's complement bytes (FIXED_LEN_BYTE_ARRAY,
/// BYTE_ARRAY). Takes ownership of `bufs`; the validity bitmap is kept.
fn decimalToArrow(allocator: Allocator, bufs: leaf_decoder.Buffers, physical: format.PhysicalType, precision: i32) ReaderError!leaf_decoder.Buffers {
    var src = bufs;
    errdefer src.deinit(allocator);
    if (precision < 1 or precision > 76) return error.UnsupportedType;
    const width: usize = if (precision <= 38) 16 else 32;
    const n = src.length;
    const values = try allocBuffer(allocator, std.math.mul(usize, n, width) catch return error.IntegerOverflow);
    errdefer allocator.free(values);
    for (0..n) |i| {
        const v: i256 = switch (physical) {
            .int32 => std.mem.bytesToValue(i32, src.values[i * 4 ..][0..4]),
            .int64 => std.mem.bytesToValue(i64, src.values[i * 8 ..][0..8]),
            .fixed_len_byte_array => blk: {
                const w = src.layout.fixed;
                break :blk bigEndianToInt(src.values[i * w ..][0..w]) orelse return error.InvalidPageData;
            },
            .byte_array => bigEndianToInt(src.bytesAt(i)) orelse return error.InvalidPageData,
            else => return error.InvalidSchema,
        };
        const out = values[i * width ..][0..width];
        if (width == 16) {
            std.mem.writeInt(i128, out[0..16], std.math.cast(i128, v) orelse return error.InvalidPageData, .little);
        } else {
            std.mem.writeInt(i256, out[0..32], v, .little);
        }
    }
    allocator.free(src.values);
    if (src.offsets) |o| allocator.free(o);
    return .{
        .length = n,
        .null_count = src.null_count,
        .validity = src.validity,
        .values = values,
        .offsets = null,
        .layout = .{ .fixed = width },
    };
}

/// A big-endian two's complement integer of any length, or null when it does not fit in i256.
fn bigEndianToInt(b: []const u8) ?i256 {
    if (b.len == 0) return 0;
    const negative = b[0] & 0x80 != 0;
    var start: usize = 0;
    if (b.len > 32) {
        // Bytes before the last 32 must only extend the sign.
        start = b.len - 32;
        const pad: u8 = if (negative) 0xFF else 0;
        for (b[0..start]) |x| if (x != pad) return null;
        if ((b[start] & 0x80 != 0) != negative) return null;
    }
    var u: u256 = if (negative) std.math.maxInt(u256) else 0;
    for (b[start..]) |x| u = (u << 8) | x;
    return @bitCast(u);
}

/// Wraps decoded buffers in an ArrowArray, taking ownership of them (they are freed on error).
fn buffersToArrow(allocator: Allocator, bufs: leaf_decoder.Buffers) ReaderError!ArrowArray {
    var b = bufs;
    const null_count = safe.castTo(i64, b.null_count) catch unreachable; // null_count <= length, an allocated entry count
    const array = if (b.offsets) |offsets|
        buildVariableArray(allocator, b.length, null_count, b.validity, offsets, b.values)
    else
        buildPrimitiveArray(allocator, b.length, null_count, b.validity, b.values);
    return array catch |e| {
        b.deinit(allocator);
        return e;
    };
}

/// Returns `e` when it is in `Set`, otherwise `error.InvalidPageData`.
fn asErrorOf(comptime Set: type, e: anyerror) Set {
    inline for (@typeInfo(Set).error_set.error_names.?) |name| {
        if (e == @field(anyerror, name)) return @field(Set, name);
    }
    return error.InvalidPageData;
}

// ============================================================================
// Nested ArrowArray Builders
// ============================================================================

fn buildListArray(
    allocator: Allocator,
    length: usize,
    null_count: i64,
    validity: Buffer,
    offsets: Buffer,
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
    validity: Buffer,
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

/// Decodes a leaf column chunk into an ArrowArray with one entry per level entry, null where
/// the definition level is below the maximum. For flat columns the entries are rows.
fn readColumnAsArrow(
    allocator: Allocator,
    source: SeekableReader,
    metadata: format.FileMetaData,
    rg: *const format.RowGroup,
    col_idx: usize,
) ReaderError!ArrowArray {
    if (col_idx >= rg.columns.len) return error.InvalidArgument;
    const chunk = &rg.columns[col_idx];
    const meta = chunk.meta_data orelse return error.InvalidArgument;
    const page_data = try parquet_reader.readColumnChunkData(allocator, source, chunk);
    defer allocator.free(page_data);
    const column_info = format.getColumnInfo(metadata.schema, col_idx) orelse return error.InvalidArgument;

    const leaf = leaf_decoder.decodeLeaf(allocator, page_data, meta, column_info, .{}) catch |e|
        return asErrorOf(ReaderError, e);
    return leafToArrow(allocator, leaf.buffers, column_info.element);
}

/// Decodes a flat column chunk (see `leaf_decoder.isFlat`) directly into an ArrowArray.
pub fn decodeFlatChunk(
    allocator: Allocator,
    page_data: []const u8,
    meta: format.ColumnMetaData,
    column_info: format.ColumnInfo,
) ReaderError!ArrowArray {
    const bufs = try leaf_decoder.decodeChunk(allocator, page_data, meta, column_info);
    return leafToArrow(allocator, bufs, column_info.element);
}

// ============================================================================
// ArrowArray builders
// ============================================================================

fn buildPrimitiveArray(
    allocator: Allocator,
    length: usize,
    null_count: i64,
    validity: Buffer,
    data: Buffer,
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
    validity: Buffer,
    offsets: Buffer,
    data: Buffer,
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

    // The column writers index buffers from 0, so arrays with an offset (slices) are first
    // rewritten without one.
    // Arrays from C callers are checked before any buffer is read: the writers index buffers
    // by the lengths and offsets they carry.
    for (arrays, schemas) |*arr, *sch| {
        const len = safe.castTo(usize, arr.length) catch return error.TypeMismatch;
        try validateArrow(arr, sch, 0, len, 0);
    }

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    for (arrays, schemas, 0..) |arr, sch, col_idx| {
        const flat = try withoutOffset(arena.allocator(), arr, &sch, 0, arr.length);
        try writeArrowColumnToParquet(writer, allocator, col_idx, flat, sch);
    }
}

/// Arrow arrays nested deeper than this are rejected by `validateArrow`.
const max_arrow_depth = 64;

/// How an Arrow format lays out its buffers, as far as `validateArrow` checks them.
const ArrowLayout = union(enum) {
    null_,
    bits,
    fixed: usize,
    /// Offsets of 4 or 8 bytes and a data buffer.
    bytes: usize,
    /// Offsets of 4 or 8 bytes and one child; `map` requires the child to be a struct of two.
    list: struct { offset_bytes: usize, map: bool },
    struct_,
    fixed_list: usize,

    fn of(fmt: []const u8) ?ArrowLayout {
        if (std.mem.eql(u8, fmt, "n")) return .null_;
        if (std.mem.eql(u8, fmt, "b")) return .bits;
        if (std.mem.eql(u8, fmt, "u") or std.mem.eql(u8, fmt, "z")) return .{ .bytes = 4 };
        if (std.mem.eql(u8, fmt, "U") or std.mem.eql(u8, fmt, "Z")) return .{ .bytes = 8 };
        if (std.mem.eql(u8, fmt, "+l")) return .{ .list = .{ .offset_bytes = 4, .map = false } };
        if (std.mem.eql(u8, fmt, "+L")) return .{ .list = .{ .offset_bytes = 8, .map = false } };
        if (std.mem.eql(u8, fmt, "+m")) return .{ .list = .{ .offset_bytes = 4, .map = true } };
        if (std.mem.eql(u8, fmt, "+s")) return .struct_;
        if (std.mem.startsWith(u8, fmt, "+w:")) {
            const size = std.fmt.parseInt(usize, fmt[3..], 10) catch return null;
            return .{ .fixed_list = size };
        }
        if (fixedWidthOf(fmt)) |w| return if (w == 0) null else .{ .fixed = w };
        return null;
    }

    fn bufferCount(self: ArrowLayout) usize {
        return switch (self) {
            .null_ => 0,
            .struct_, .fixed_list => 1,
            .bits, .fixed, .list => 2,
            .bytes => 3,
        };
    }
};

/// Returns `error.TypeMismatch` unless entries `start .. start + count` of `arr` (counted
/// from `arr.offset`) can be read through `sch` without leaving the array: lengths and
/// offsets are non-negative and in range, there are as many buffers as the format uses and
/// the child counts match it, the buffers the entries use are present, offsets do not
/// decrease, and the entries they address lie in the child. The size of a byte-array data
/// buffer is not part of the Arrow C interface, so the last offset is not checked against it.
fn validateArrow(arr: *const ArrowArray, sch: *const ArrowSchema, start: usize, count: usize, depth: usize) WriterError!void {
    if (depth > max_arrow_depth) return error.TypeMismatch;
    const fmt = std.mem.sliceTo(sch.format, 0);
    const layout = ArrowLayout.of(fmt) orelse return error.TypeMismatch;
    const off = safe.castTo(usize, arr.offset) catch return error.TypeMismatch;
    const len = safe.castTo(usize, arr.length) catch return error.TypeMismatch;
    if (start > len or count > len - start) return error.TypeMismatch;
    const first = std.math.add(usize, off, start) catch return error.IntegerOverflow;
    const end = std.math.add(usize, first, count) catch return error.IntegerOverflow;

    const nb = safe.castTo(usize, arr.n_buffers) catch return error.TypeMismatch;
    const nc = safe.castTo(usize, arr.n_children) catch return error.TypeMismatch;
    // Fewer buffers than the format uses would be read past; more are not read.
    if (nb < layout.bufferCount()) return error.TypeMismatch;
    if (nb > 0 and @intFromPtr(arr.buffers) == 0) return error.TypeMismatch;
    if (arr.n_children != sch.n_children) return error.TypeMismatch;
    if (nc > 0 and (arr.children == null or sch.children == null)) return error.TypeMismatch;

    switch (layout) {
        .null_ => {},
        .bits => if (count > 0 and arr.buffers[1] == null) return error.TypeMismatch,
        .fixed => |w| {
            if (count > 0 and arr.buffers[1] == null) return error.TypeMismatch;
            _ = std.math.mul(usize, end, w) catch return error.IntegerOverflow;
        },
        .bytes => |ob| {
            const range = try offsetRange(arr, ob, first, end);
            if (range.last > range.first and arr.buffers[2] == null) return error.TypeMismatch;
        },
        .list => |l| {
            if (nc != 1) return error.TypeMismatch;
            const range = try offsetRange(arr, l.offset_bytes, first, end);
            const child = arr.children.?[0];
            const child_sch = sch.children.?[0];
            if (l.map) {
                if (!std.mem.eql(u8, std.mem.sliceTo(child_sch.format, 0), "+s") or child_sch.n_children != 2) return error.TypeMismatch;
            }
            try validateArrow(child, child_sch, range.first, range.last - range.first, depth + 1);
        },
        .struct_ => for (0..nc) |i| {
            // A struct's offset applies to its children.
            try validateArrow(arr.children.?[i], sch.children.?[i], first, count, depth + 1);
        },
        .fixed_list => |size| {
            if (nc != 1) return error.TypeMismatch;
            const child_start = std.math.mul(usize, first, size) catch return error.IntegerOverflow;
            const child_count = std.math.mul(usize, count, size) catch return error.IntegerOverflow;
            try validateArrow(arr.children.?[0], sch.children.?[0], child_start, child_count, depth + 1);
        },
    }
}

/// The first and last offsets of entries `first .. end` of a byte-array or list array, after
/// checking that the offsets buffer is present and the offsets are non-negative and do not
/// decrease.
fn offsetRange(arr: *const ArrowArray, offset_bytes: usize, first: usize, end: usize) WriterError!struct { first: usize, last: usize } {
    const raw: [*]const u8 = @ptrCast(arr.buffers[1] orelse return error.TypeMismatch);
    _ = std.math.mul(usize, end +| 1, offset_bytes) catch return error.IntegerOverflow;
    var prev: usize = 0;
    var first_offset: usize = 0;
    for (first..end + 1) |i| {
        const v: i64 = if (offset_bytes == 4)
            std.mem.readInt(i32, raw[i * 4 ..][0..4], native_endian)
        else
            std.mem.readInt(i64, raw[i * 8 ..][0..8], native_endian);
        const o = safe.castTo(usize, v) catch return error.TypeMismatch;
        if (i == first) first_offset = o else if (o < prev) return error.TypeMismatch;
        prev = o;
    }
    return .{ .first = first_offset, .last = prev };
}

/// Bytes per entry of an Arrow format with one fixed-width values buffer, or null.
fn fixedWidthOf(fmt: []const u8) ?usize {
    if (fmt.len == 1) return switch (fmt[0]) {
        'c', 'C' => 1,
        's', 'S', 'e' => 2,
        'i', 'I', 'f' => 4,
        'l', 'L', 'g' => 8,
        else => null,
    };
    if (std.mem.eql(u8, fmt, "tdD") or std.mem.eql(u8, fmt, "tts") or std.mem.eql(u8, fmt, "ttm") or std.mem.eql(u8, fmt, "tiM")) return 4;
    if (std.mem.eql(u8, fmt, "tdm") or std.mem.eql(u8, fmt, "ttu") or std.mem.eql(u8, fmt, "ttn") or std.mem.eql(u8, fmt, "tiD")) return 8;
    if (std.mem.eql(u8, fmt, "tin")) return 16;
    if (std.mem.startsWith(u8, fmt, "ts") or std.mem.startsWith(u8, fmt, "tD")) return 8;
    if (std.mem.startsWith(u8, fmt, "d:")) return arrowDecimalWidth(fmt) catch null;
    if (std.mem.startsWith(u8, fmt, "w:")) return std.fmt.parseInt(usize, fmt[2..], 10) catch null;
    return null;
}

/// The array of entries `extra + arr.offset` to `extra + arr.offset + length` of `arr`, with
/// offset 0 at every level: fixed-width and offsets buffers start at the first entry, and
/// bitmaps that do not start on a byte are copied. A struct's offset and length apply to its
/// children; a list's children are addressed through its offsets. Allocates with `arena`.
fn withoutOffset(arena: Allocator, arr: ArrowArray, sch: *const ArrowSchema, extra: i64, length: i64) WriterError!ArrowArray {
    const fmt = std.mem.sliceTo(sch.format, 0);
    const off = safe.castTo(usize, std.math.add(i64, arr.offset, extra) catch return error.IntegerOverflow) catch return error.IntegerOverflow;
    const n = safe.castTo(usize, length) catch return error.IntegerOverflow;
    const nb = safe.castTo(usize, arr.n_buffers) catch return error.IntegerOverflow;
    const nc = safe.castTo(usize, arr.n_children) catch return error.IntegerOverflow;

    var out = arr;
    out.offset = 0;
    out.length = length;
    if (off != 0) out.null_count = -1;

    if (nb == 0) return out;
    const src_buffers: [*]?*anyopaque = @ptrCast(arr.buffers);
    const buffers = arena.alloc(?*anyopaque, nb) catch return error.OutOfMemory;
    @memcpy(buffers, src_buffers[0..nb]);
    out.buffers = @ptrCast(buffers.ptr);
    if (n == 0) {
        // The Arrow format lets an empty array leave its buffers null; the writers read the
        // data buffer, and an offsets buffer's first entry, regardless.
        for (buffers[1..]) |*b| if (b.* == null) {
            b.* = @ptrCast(@constCast(&empty_buffer));
        };
    }

    if (off != 0 and !std.mem.eql(u8, fmt, "n")) {
        // Validity bitmap.
        if (nb > 0) if (buffers[0]) |v| {
            buffers[0] = try shiftedBitmap(arena, @ptrCast(v), off, n);
        };
        if (std.mem.eql(u8, fmt, "b")) {
            if (nb < 2) return error.TypeMismatch;
            buffers[1] = try shiftedBitmap(arena, @ptrCast(buffers[1] orelse return error.TypeMismatch), off, n);
        } else if (fixedWidthOf(fmt)) |w| {
            if (nb < 2) return error.TypeMismatch;
            const values: [*]u8 = @ptrCast(buffers[1] orelse return error.TypeMismatch);
            buffers[1] = @ptrCast(values + off * w);
        } else if (std.mem.eql(u8, fmt, "u") or std.mem.eql(u8, fmt, "z") or std.mem.eql(u8, fmt, "+l") or std.mem.eql(u8, fmt, "+m")) {
            if (nb < 2) return error.TypeMismatch;
            const offsets: [*]u8 = @ptrCast(buffers[1] orelse return error.TypeMismatch);
            buffers[1] = @ptrCast(offsets + off * 4);
        } else if (std.mem.eql(u8, fmt, "U") or std.mem.eql(u8, fmt, "Z") or std.mem.eql(u8, fmt, "+L")) {
            if (nb < 2) return error.TypeMismatch;
            const offsets: [*]u8 = @ptrCast(buffers[1] orelse return error.TypeMismatch);
            buffers[1] = @ptrCast(offsets + off * 8);
        } else if (!std.mem.eql(u8, fmt, "+s") and !std.mem.startsWith(u8, fmt, "+w:")) {
            // A layout not handled here; refuse rather than write other entries.
            return error.TypeMismatch;
        }
    }

    if (nc == 0) return out;
    const src_children: [*]*ArrowArray = arr.children orelse return error.TypeMismatch;
    const child_schemas: [*]*ArrowSchema = sch.children orelse return error.TypeMismatch;
    if (sch.n_children != arr.n_children) return error.TypeMismatch;
    const children = arena.alloc(*ArrowArray, nc) catch return error.OutOfMemory;
    for (children, 0..) |*c, i| {
        const child = src_children[i].*;
        const child_sch = child_schemas[i];
        c.* = arena.create(ArrowArray) catch return error.OutOfMemory;
        c.*.* = if (std.mem.eql(u8, fmt, "+s"))
            // Struct children are indexed like the struct.
            try withoutOffset(arena, child, child_sch, std.math.add(i64, arr.offset, extra) catch return error.IntegerOverflow, length)
        else if (std.mem.startsWith(u8, fmt, "+w:")) blk: {
            const size = std.fmt.parseInt(i64, fmt[3..], 10) catch return error.TypeMismatch;
            const start = std.math.mul(i64, std.math.add(i64, arr.offset, extra) catch return error.IntegerOverflow, size) catch return error.IntegerOverflow;
            break :blk try withoutOffset(arena, child, child_sch, start, std.math.mul(i64, length, size) catch return error.IntegerOverflow);
        } else
            // List and map children are addressed through the offsets; only their own offset
            // is removed.
            try withoutOffset(arena, child, child_sch, 0, child.length);
    }
    out.children = @ptrCast(children.ptr);
    return out;
}

/// Zeros standing in for the null buffers of an empty array: one offset of 0 of either width.
const empty_buffer: [16]u8 align(16) = @splat(0);

/// Bits `off` to `off + n` of `bits`, starting at bit 0 of a new bitmap.
fn shiftedBitmap(arena: Allocator, bits: [*]const u8, off: usize, n: usize) WriterError!?*anyopaque {
    if (off % 8 == 0) return @ptrCast(@constCast(bits + off / 8));
    const out = arena.alloc(u8, (n + 7) / 8) catch return error.OutOfMemory;
    @memset(out, 0);
    for (0..n) |i| {
        const j = off + i;
        if ((bits[j / 8] >> @intCast(j % 8)) & 1 == 1) out[i / 8] |= @as(u8, 1) << @intCast(i % 8);
    }
    return @ptrCast(out.ptr);
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
        if (fmt[2] == 's') {
            try writeTimestampSecondsColumn(writer, allocator, col_idx, arr, n);
        } else {
            try writeTypedColumn(i64, writer, allocator, col_idx, arr, n);
        }
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

    // Decimal: converted to the column's Parquet storage first.
    if (fmt.len >= 4 and fmt[0] == 'd' and fmt[1] == ':') {
        if (col_idx >= writer.columns.len) return error.TypeMismatch;
        const col_def = &writer.columns[col_idx];
        var buffers: [2]?*anyopaque = undefined;
        const conv = try decimalToParquet(allocator, arr, fmt, col_def, &buffers);
        defer allocator.free(conv.data);
        switch (col_def.type_) {
            .int32 => try writeTypedColumn(i32, writer, allocator, col_idx, conv.array, n),
            .int64 => try writeTypedColumn(i64, writer, allocator, col_idx, conv.array, n),
            .fixed_len_byte_array => try writeFixedByteArrayColumn(writer, allocator, col_idx, conv.array, n, conv.width),
            else => return error.TypeMismatch,
        }
        return;
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

    return arrowLeafToValue(allocator, arr, fmt, idx);
}

/// `allocator` holds the bytes of FIXED_LEN_BYTE_ARRAY decimal values.
fn arrowLeafToValue(allocator: Allocator, arr: ArrowArray, fmt: []const u8, idx: usize) ArrowValueError!Value {
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
        if (fmt[2] == 's') {
            return .{ .int64_val = std.math.mul(i64, data[idx], 1000) catch return error.IntegerOverflow };
        }
        return .{ .int64_val = data[idx] };
    }
    if (fmt.len >= 2 and fmt[0] == 'w' and fmt[1] == ':') {
        const type_len = std.fmt.parseInt(usize, fmt[2..], 10) catch return error.InvalidSchema;
        const data: [*]const u8 = @ptrCast(@alignCast(arr.buffers[1].?));
        return .{ .fixed_bytes_val = data[idx * type_len ..][0..type_len] };
    }
    if (fmt.len >= 4 and fmt[0] == 'd' and fmt[1] == ':') {
        // Decimal: the Arrow value, as the Parquet storage its precision maps to (see
        // ArrowFormatInfo): i32 up to 9 digits, i64 up to 18, big-endian bytes above.
        var parts = std.mem.splitScalar(u8, fmt[2..], ',');
        const precision = std.fmt.parseInt(i32, parts.next() orelse return error.InvalidSchema, 10) catch return error.InvalidSchema;
        const width = arrowDecimalWidth(fmt) catch return error.InvalidSchema;
        const data: [*]const u8 = @ptrCast(arr.buffers[1] orelse return error.InvalidSchema);
        const value = arrowDecimalAt(data, width, idx);
        if (precision <= 9) return .{ .int32_val = std.math.cast(i32, value) orelse return error.IntegerOverflow };
        if (precision <= 18) return .{ .int64_val = std.math.cast(i64, value) orelse return error.IntegerOverflow };
        if (precision > 76) return error.InvalidSchema;
        const bytes = allocator.alloc(u8, decimalStorageBytes(precision)) catch return error.OutOfMemory;
        if (!intToBigEndian(value, bytes)) return error.IntegerOverflow;
        return .{ .fixed_bytes_val = bytes };
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
        if (child_fmt[2] == 's') {
            try writeListColumnTimestampSeconds(writer, allocator, col_idx, arr, child_arr.*, n);
        } else {
            try writeListColumnTyped(i64, writer, allocator, col_idx, arr, child_arr.*, n);
        }
        return;
    }
    if (child_fmt.len >= 2 and child_fmt[0] == 'w' and child_fmt[1] == ':') {
        const type_len = std.fmt.parseInt(usize, child_fmt[2..], 10) catch return error.TypeMismatch;
        try writeListColumnFixedByteArray(writer, allocator, col_idx, arr, child_arr.*, n, type_len);
        return;
    }
    if (child_fmt.len >= 4 and child_fmt[0] == 'd' and child_fmt[1] == ':') {
        try writeListColumnDecimal(writer, allocator, col_idx, arr, child_arr.*, child_fmt, n);
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

fn writeListColumnTimestampSeconds(
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

    var lists = allocator.alloc(Optional([]const Optional(i64)), n) catch return error.OutOfMemory;
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
            var elems = allocator.alloc(Optional(i64), len) catch return error.OutOfMemory;
            for (0..len) |j| {
                const idx = start + j;
                if (idx >= child_len) {
                    elems[j] = .null_value;
                } else if (child_validity != null and !arrow.getBit(child_validity.?[0 .. (child_len + 7) / 8], idx)) {
                    elems[j] = .null_value;
                } else {
                    elems[j] = .{ .value = std.math.mul(i64, child_data[idx], 1000) catch return error.IntegerOverflow };
                }
            }
            lists[i] = .{ .value = elems };
        }
    }

    try writer.writeListColumn(i64, col_idx, lists);
}

fn writeListColumnDecimal(
    writer: *Writer,
    allocator: Allocator,
    col_idx: usize,
    parent_arr: ArrowArray,
    child_arr: ArrowArray,
    child_fmt: []const u8,
    n: usize,
) WriterError!void {
    if (col_idx >= writer.columns.len) return error.TypeMismatch;
    const col_def = &writer.columns[col_idx];
    var buffers: [2]?*anyopaque = undefined;
    const conv = try decimalToParquet(allocator, child_arr, child_fmt, col_def, &buffers);
    defer allocator.free(conv.data);
    switch (col_def.type_) {
        .int32 => try writeListColumnTyped(i32, writer, allocator, col_idx, parent_arr, conv.array, n),
        .int64 => try writeListColumnTyped(i64, writer, allocator, col_idx, parent_arr, conv.array, n),
        .fixed_len_byte_array => try writeListColumnFixedByteArray(writer, allocator, col_idx, parent_arr, conv.array, n, conv.width),
        else => return error.TypeMismatch,
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
        if (child_fmt[2] == 's') {
            try writeStructFieldTimestampSeconds(writer, allocator, struct_col_idx, field_idx, child_arr, n, parent_nulls);
        } else {
            try writeStructFieldTyped(i64, writer, allocator, struct_col_idx, field_idx, child_arr, n, parent_nulls);
        }
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

fn writeStructFieldTimestampSeconds(
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

    var values = allocator.alloc(?i64, n) catch return error.OutOfMemory;
    defer allocator.free(values);

    for (0..n) |i| {
        if (validity != null and !arrow.getBit(validity.?[0 .. (n + 7) / 8], i)) {
            values[i] = null;
        } else {
            values[i] = std.math.mul(i64, data[i], 1000) catch return error.IntegerOverflow;
        }
    }

    try writer.writeStructField(i64, struct_col_idx, field_idx, values, parent_nulls);
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
    if (struct_col_idx >= writer.columns.len) return error.TypeMismatch;
    const fields = writer.columns[struct_col_idx].struct_fields orelse return error.TypeMismatch;
    if (field_idx >= fields.len) return error.TypeMismatch;
    // Flat struct decimal fields are INT32 or INT64; wider ones take the schema node path.
    const field_def: ColumnDef = .{ .name = fields[field_idx].name, .type_ = fields[field_idx].type_ };
    const col_def = &field_def;
    var buffers: [2]?*anyopaque = undefined;
    const conv = try decimalToParquet(allocator, child_arr, child_fmt, col_def, &buffers);
    defer allocator.free(conv.data);
    switch (col_def.type_) {
        .int32 => try writeStructFieldTyped(i32, writer, allocator, struct_col_idx, field_idx, conv.array, n, parent_nulls),
        .int64 => try writeStructFieldTyped(i64, writer, allocator, struct_col_idx, field_idx, conv.array, n, parent_nulls),
        else => return error.TypeMismatch,
    }
}

/// Bytes per entry of an Arrow decimal format "d:p,s[,bit width]": 16 (decimal128) without a
/// bit width; 4, 8, 16 or 32 for a bit width of 32, 64, 128 or 256.
fn arrowDecimalWidth(fmt: []const u8) error{InvalidSchema}!usize {
    var parts = std.mem.splitScalar(u8, fmt[2..], ',');
    _ = parts.next();
    _ = parts.next();
    const bits = parts.next() orelse return 16;
    return switch (std.fmt.parseInt(u16, bits, 10) catch return error.InvalidSchema) {
        32 => 4,
        64 => 8,
        128 => 16,
        256 => 32,
        else => error.InvalidSchema,
    };
}

/// Bytes of the FIXED_LEN_BYTE_ARRAY storage of a decimal of `precision` (1 to 76): the
/// fewest that hold every value of that many digits.
fn decimalStorageBytes(precision: i32) usize {
    if (precision <= 38) return decimalByteLength(precision);
    // ceil((precision * log2(10) + 1) / 8)
    const bits = @as(f64, @floatFromInt(precision)) * std.math.log2(10.0) + 1.0;
    return @intFromFloat(@ceil(bits / 8.0));
}

/// Arrow decimal entry `i`: little-endian two's complement of `width` bytes.
fn arrowDecimalAt(data: [*]const u8, width: usize, i: usize) i256 {
    return switch (width) {
        4 => std.mem.readInt(i32, data[i * 4 ..][0..4], .little),
        8 => std.mem.readInt(i64, data[i * 8 ..][0..8], .little),
        16 => std.mem.readInt(i128, data[i * 16 ..][0..16], .little),
        else => std.mem.readInt(i256, data[i * 32 ..][0..32], .little),
    };
}

/// Writes `v` as big-endian two's complement into `out`, or returns false when it does not fit.
fn intToBigEndian(v: i256, out: []u8) bool {
    var u: u256 = @bitCast(v);
    var k = out.len;
    while (k > 0) {
        k -= 1;
        out[k] = @truncate(u);
        u = @bitCast(@as(i256, @bitCast(u)) >> 8);
    }
    // What is left must be the sign extension of the bytes written.
    const sign_ok = if (out.len > 0) (out[0] & 0x80 != 0) == (v < 0) else v == 0;
    const rest: i256 = @bitCast(u);
    return sign_ok and rest == (if (v < 0) @as(i256, -1) else 0);
}

const DecimalConversion = struct {
    /// `arr` with its values buffer replaced by `data`.
    array: ArrowArray,
    /// Converted values; the caller frees them.
    data: []align(16) u8,
    /// Bytes per converted value.
    width: usize,
};

/// Converts the values of Arrow decimal array `arr` (format `fmt`) to the Parquet storage of
/// `col_def`: native i32 or i64, or big-endian bytes of the column's type length. Null
/// entries become 0. A value that does not fit the storage returns `error.IntegerOverflow`.
/// `buffers` holds the returned array's buffer pointers and must outlive it.
fn decimalToParquet(allocator: Allocator, arr: ArrowArray, fmt: []const u8, col_def: *const ColumnDef, buffers: *[2]?*anyopaque) WriterError!DecimalConversion {
    const src_width = arrowDecimalWidth(fmt) catch return error.TypeMismatch;
    const n = safe.castTo(usize, arr.length) catch return error.IntegerOverflow;
    const width: usize = switch (col_def.type_) {
        .int32 => 4,
        .int64 => 8,
        .fixed_len_byte_array => safe.castTo(usize, col_def.type_length orelse return error.InvalidFixedLength) catch return error.IntegerOverflow,
        else => return error.TypeMismatch,
    };
    const data = allocator.alignedAlloc(u8, .@"16", std.math.mul(usize, n, width) catch return error.IntegerOverflow) catch return error.OutOfMemory;
    errdefer allocator.free(data);
    @memset(data, 0);
    const validity: ?[*]const u8 = if (arr.buffers[0]) |b| @ptrCast(b) else null;
    if (n > 0) {
        const src: [*]const u8 = @ptrCast(arr.buffers[1] orelse return error.TypeMismatch);
        for (0..n) |i| {
            if (validity) |v| if ((v[i / 8] >> @intCast(i % 8)) & 1 == 0) continue;
            const value = arrowDecimalAt(src, src_width, i);
            const out = data[i * width ..][0..width];
            switch (col_def.type_) {
                .int32 => std.mem.bytesAsValue(i32, out[0..4]).* = std.math.cast(i32, value) orelse return error.IntegerOverflow,
                .int64 => std.mem.bytesAsValue(i64, out[0..8]).* = std.math.cast(i64, value) orelse return error.IntegerOverflow,
                else => if (!intToBigEndian(value, out)) return error.IntegerOverflow,
            }
        }
    }
    buffers.* = .{ arr.buffers[0], @ptrCast(data.ptr) };
    var array = arr;
    array.buffers = buffers;
    return .{ .array = array, .data = data, .width = width };
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

fn writeTimestampSecondsColumn(
    writer: *Writer,
    allocator: Allocator,
    col_idx: usize,
    arr: ArrowArray,
    n: usize,
) WriterError!void {
    const validity: ?[*]const u8 = if (arr.buffers[0]) |b| @ptrCast(@alignCast(b)) else null;
    const data: [*]const i64 = @ptrCast(@alignCast(arr.buffers[1].?));

    var optionals = allocator.alloc(Optional(i64), n) catch return error.OutOfMemory;
    defer allocator.free(optionals);

    for (0..n) |i| {
        if (validity != null and !arrow.getBit(validity.?[0 .. (n + 7) / 8], i)) {
            optionals[i] = .null_value;
        } else {
            optionals[i] = .{ .value = std.math.mul(i64, data[i], 1000) catch return error.IntegerOverflow };
        }
    }

    try writer.writeColumnOptional(i64, col_idx, optionals);
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
    defer freeImportedColumnDefs(allocator, col_defs);

    try std.testing.expectEqual(@as(usize, 2), col_defs.len);
    try std.testing.expectEqual(format.PhysicalType.int32, col_defs[0].type_);
    try std.testing.expectEqual(false, col_defs[0].optional);
    try std.testing.expectEqual(format.PhysicalType.byte_array, col_defs[1].type_);
    try std.testing.expectEqual(true, col_defs[1].optional);
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

    var result = try readColumns(allocator, dr.getSource(), dr.metadata, 0, .{});
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

test "projected read returns a schema of the selected columns" {
    const allocator = std.testing.allocator;

    var writer = try api_writer_mod.writeToBuffer(allocator, &.{
        .{ .name = "id", .type_ = .int32, .optional = true },
        .{ .name = "name", .type_ = .byte_array, .optional = true, .logical_type = .string },
        .{ .name = "score", .type_ = .double, .optional = false },
    });
    try writer.writeColumnOptional(i32, 0, &.{ .{ .value = 1 }, .null_value });
    try writer.writeColumnOptional([]const u8, 1, &.{ .{ .value = "a" }, .{ .value = "b" } });
    try writer.writeColumn(f64, 2, &.{ 1.5, 2.5 });
    try writer.close();
    const buf = try writer.toOwnedSlice();
    defer allocator.free(buf);
    writer.deinit();

    var dr = try api_reader_mod.openBufferDynamic(allocator, buf, .{});
    defer dr.deinit();

    var result = try readColumns(allocator, dr.getSource(), dr.metadata, 0, .{ .columns = &.{ 2, 0 } });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.arrays.len);
    try std.testing.expectEqual(@as(i64, 2), result.schema.n_children);
    const children = result.schema.children.?;
    try std.testing.expectEqualStrings("score", std.mem.span(children[0].name.?));
    try std.testing.expectEqualStrings("g", std.mem.span(children[0].format));
    try std.testing.expectEqualStrings("id", std.mem.span(children[1].name.?));
    try std.testing.expectEqualStrings("i", std.mem.span(children[1].format));

    var projected = try exportProjectedSchemaAsArrow(allocator, dr.metadata, &.{1});
    defer projected.doRelease();
    try std.testing.expectEqual(@as(i64, 1), projected.n_children);
    try std.testing.expectEqualStrings("name", std.mem.span(projected.children.?[0].name.?));

    try std.testing.expectError(error.InvalidArgument, exportProjectedSchemaAsArrow(allocator, dr.metadata, &.{3}));
}

test "batch reader with large_offsets returns large_string with i64 offsets" {
    const allocator = std.testing.allocator;

    const col_defs = [_]ColumnDef{
        .{ .name = "name", .type_ = .byte_array, .optional = true, .logical_type = .string },
        .{ .name = "tags", .type_ = .byte_array, .optional = true, .is_list = true, .element_optional = true, .logical_type = .string },
    };
    var writer = try api_writer_mod.writeToBuffer(allocator, &col_defs);
    try writer.writeColumnOptional([]const u8, 0, &.{ .{ .value = "alice" }, .null_value, .{ .value = "charlie" } });
    try writer.writeListColumn([]const u8, 1, &.{
        .{ .value = &.{ .{ .value = "a" }, .{ .value = "bc" } } },
        .null_value,
        .{ .value = &.{.{ .value = "def" }} },
    });
    try writer.close();
    const buf = try writer.toOwnedSlice();
    defer allocator.free(buf);
    writer.deinit();

    var dr = try api_reader_mod.openBufferDynamic(allocator, buf, .{});
    defer dr.deinit();

    // Batches of 2 rows: [0, 1], [2].
    var batches = try ArrowBatchReader.init(allocator, dr.getSource(), dr.metadata, .{ .batch_rows = 2, .large_offsets = true });
    defer batches.deinit();

    var first = (try batches.next()).?;
    defer first.deinit();
    const children = first.schema.children.?;
    try std.testing.expectEqualStrings("U", std.mem.span(children[0].format));
    try std.testing.expectEqualStrings("+l", std.mem.span(children[1].format));
    try std.testing.expectEqualStrings("U", std.mem.span(children[1].children.?[0].format));

    const names = &first.arrays[0];
    try std.testing.expectEqual(@as(i64, 2), names.length);
    try std.testing.expectEqual(@as(i64, 1), names.null_count);
    const name_offsets: [*]const i64 = @ptrCast(@alignCast(names.buffers[1].?));
    const name_data: [*]const u8 = @ptrCast(names.buffers[2].?);
    try std.testing.expectEqualStrings("alice", name_data[@intCast(name_offsets[0])..@intCast(name_offsets[1])]);
    try std.testing.expectEqual(name_offsets[1], name_offsets[2]);

    var second = (try batches.next()).?;
    defer second.deinit();
    const tags = second.arrays[1].children.?[0];
    try std.testing.expectEqual(@as(i64, 1), tags.length);
    const tag_offsets: [*]const i64 = @ptrCast(@alignCast(tags.buffers[1].?));
    const tag_data: [*]const u8 = @ptrCast(tags.buffers[2].?);
    try std.testing.expectEqualStrings("def", tag_data[@intCast(tag_offsets[0])..@intCast(tag_offsets[1])]);

    try std.testing.expect((try batches.next()) == null);
}

test "batches span row groups" {
    const allocator = std.testing.allocator;

    // 5 rows in row groups of 2, 2 and 1; name is null in row 3.
    var writer = try api_writer_mod.createBufferDynamic(allocator);
    defer writer.deinit();
    try writer.addColumn("id", @import("dynamic_writer.zig").TypeInfo.int32, .{});
    try writer.addColumn("name", @import("dynamic_writer.zig").TypeInfo.string, .{});
    writer.setRowGroupSize(2);
    try writer.begin();
    const names = [_]?[]const u8{ "a", "bb", "ccc", null, "eeeee" };
    for (names, 0..) |name, i| {
        try writer.setInt32(0, @intCast(i));
        if (name) |n| try writer.setBytes(1, n) else try writer.setNull(1);
        try writer.addRow();
    }
    try writer.close();
    const buf = try writer.toOwnedSlice();
    defer allocator.free(buf);

    var dr = try api_reader_mod.openBufferDynamic(allocator, buf, .{});
    defer dr.deinit();
    try std.testing.expectEqual(@as(usize, 3), dr.metadata.row_groups.len);

    // Batches of 3: rows 0-2 (row groups 0 and 1), rows 3-4 (row groups 1 and 2).
    var batches = try ArrowBatchReader.init(allocator, dr.getSource(), dr.metadata, .{ .batch_rows = 3 });
    defer batches.deinit();
    var next_id: i32 = 0;
    for ([_]i64{ 3, 2 }, [_]i64{ 0, 1 }) |len, nulls| {
        var result = (try batches.next()).?;
        defer result.deinit();
        const ids = &result.arrays[0];
        const strs = &result.arrays[1];
        try std.testing.expectEqual(len, ids.length);
        try std.testing.expectEqual(len, strs.length);
        try std.testing.expectEqual(nulls, strs.null_count);
        const id_values: [*]const i32 = @ptrCast(@alignCast(ids.buffers[1].?));
        const offsets: [*]const i32 = @ptrCast(@alignCast(strs.buffers[1].?));
        const data: [*]const u8 = @ptrCast(strs.buffers[2].?);
        for (0..@intCast(len)) |i| {
            try std.testing.expectEqual(next_id, id_values[i]);
            const expected = names[@intCast(next_id)] orelse "";
            try std.testing.expectEqualStrings(expected, data[@intCast(offsets[i])..@intCast(offsets[i + 1])]);
            next_id += 1;
        }
    }
    try std.testing.expect((try batches.next()) == null);

    // Without batch_rows, one batch per row group.
    var whole = try ArrowBatchReader.init(allocator, dr.getSource(), dr.metadata, .{ .batch_rows = null });
    defer whole.deinit();
    for ([_]i64{ 2, 2, 1 }) |len| {
        var result = (try whole.next()).?;
        defer result.deinit();
        try std.testing.expectEqual(len, result.arrays[0].length);
    }
    try std.testing.expect((try whole.next()) == null);
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

    var result = try readColumns(allocator, dr.getSource(), dr.metadata, 0, .{});
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
    defer freeImportedColumnDefs(allocator, col_defs);

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

    var result = try readColumns(allocator, dr.getSource(), dr.metadata, 0, .{});
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

    var result = try readColumns(allocator, dr.getSource(), dr.metadata, 0, .{});
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

    var result = try readColumns(allocator, dr.getSource(), dr.metadata, 0, .{});
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

    var result = try readColumns(allocator, dr.getSource(), dr.metadata, 0, .{});
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

/// A one-entry, non-null int32 ArrowArray.
fn testInt32Array(allocator: Allocator, v: i32) !ArrowArray {
    const validity = try allocBuffer(allocator, 1);
    errdefer allocator.free(validity);
    @memset(validity, 0xFF);
    const data = try allocBuffer(allocator, 4);
    errdefer allocator.free(data);
    @memcpy(data, std.mem.asBytes(&v));
    return buildPrimitiveArray(allocator, 1, 0, validity, data);
}

test "ownership: local release must be null after transfer to struct children" {
    const allocator = std.testing.allocator;

    var key_array = try testInt32Array(allocator, 10);
    errdefer key_array.doRelease();

    var val_array = try testInt32Array(allocator, 20);
    errdefer val_array.doRelease();

    var entries_children = try allocator.alloc(ArrowArray, 2);
    entries_children[0] = key_array;
    entries_children[1] = val_array;
    key_array.release = null; // ownership transferred
    val_array.release = null; // ownership transferred

    // After ownership transfer, local copies must have null release pointers
    // to prevent double-free on error paths.
    try std.testing.expect(key_array.release == null);
    try std.testing.expect(val_array.release == null);

    const validity = try allocBuffer(allocator, 1);
    @memset(validity, 0xFF);
    var entries_struct = try buildStructArray(allocator, 1, 0, validity, entries_children);
    entries_struct.doRelease();
}

// ============================================================================
// Arrow write round-trip tests: temporal types
// ============================================================================

test "round-trip: arrow write then read - timestamps and date32" {
    const allocator = std.testing.allocator;
    const n: usize = 3;
    const bitmap_len = 1;

    // Timestamp seconds (tss:UTC)
    const tss_validity = try allocator.alloc(u8, bitmap_len);
    defer allocator.free(tss_validity);
    @memset(tss_validity, 0xFF);
    arrow.clearBit(tss_validity, 2);

    const tss_data = try allocator.alloc(u8, n * 8);
    defer allocator.free(tss_data);
    const tss_typed: [*]i64 = @ptrCast(@alignCast(tss_data.ptr));
    tss_typed[0] = 1705312200; // 2024-01-15 10:30:00 UTC
    tss_typed[1] = 0; // epoch
    tss_typed[2] = 0; // null

    var tss_buffers = [_]?*anyopaque{ @ptrCast(tss_validity.ptr), @ptrCast(tss_data.ptr) };
    const tss_arr = ArrowArray{
        .length = 3, .null_count = 1, .offset = 0, .n_buffers = 2,
        .n_children = 0, .buffers = &tss_buffers, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };
    const tss_sch = ArrowSchema{
        .format = "tss:UTC", .name = "ts_sec", .metadata = null,
        .flags = arrow.ARROW_FLAG_NULLABLE, .n_children = 0, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };

    // Timestamp millis (tsm:UTC)
    const tsm_validity = try allocator.alloc(u8, bitmap_len);
    defer allocator.free(tsm_validity);
    @memset(tsm_validity, 0xFF);

    const tsm_data = try allocator.alloc(u8, n * 8);
    defer allocator.free(tsm_data);
    const tsm_typed: [*]i64 = @ptrCast(@alignCast(tsm_data.ptr));
    tsm_typed[0] = 1705312200000;
    tsm_typed[1] = 0;
    tsm_typed[2] = 86400000;

    var tsm_buffers = [_]?*anyopaque{ @ptrCast(tsm_validity.ptr), @ptrCast(tsm_data.ptr) };
    const tsm_arr = ArrowArray{
        .length = 3, .null_count = 0, .offset = 0, .n_buffers = 2,
        .n_children = 0, .buffers = &tsm_buffers, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };
    const tsm_sch = ArrowSchema{
        .format = "tsm:UTC", .name = "ts_ms", .metadata = null,
        .flags = arrow.ARROW_FLAG_NULLABLE, .n_children = 0, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };

    // Timestamp micros (tsu:UTC)
    const tsu_validity = try allocator.alloc(u8, bitmap_len);
    defer allocator.free(tsu_validity);
    @memset(tsu_validity, 0xFF);

    const tsu_data = try allocator.alloc(u8, n * 8);
    defer allocator.free(tsu_data);
    const tsu_typed: [*]i64 = @ptrCast(@alignCast(tsu_data.ptr));
    tsu_typed[0] = 1705312200000000;
    tsu_typed[1] = 0;
    tsu_typed[2] = 86400000000;

    var tsu_buffers = [_]?*anyopaque{ @ptrCast(tsu_validity.ptr), @ptrCast(tsu_data.ptr) };
    const tsu_arr = ArrowArray{
        .length = 3, .null_count = 0, .offset = 0, .n_buffers = 2,
        .n_children = 0, .buffers = &tsu_buffers, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };
    const tsu_sch = ArrowSchema{
        .format = "tsu:UTC", .name = "ts_us", .metadata = null,
        .flags = arrow.ARROW_FLAG_NULLABLE, .n_children = 0, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };

    // Date32 (tdD)
    const d32_validity = try allocator.alloc(u8, bitmap_len);
    defer allocator.free(d32_validity);
    @memset(d32_validity, 0xFF);
    arrow.clearBit(d32_validity, 1);

    const d32_data = try allocator.alloc(u8, n * 4);
    defer allocator.free(d32_data);
    const d32_typed: [*]i32 = @ptrCast(@alignCast(d32_data.ptr));
    d32_typed[0] = 19738; // 2024-01-15
    d32_typed[1] = 0; // null
    d32_typed[2] = 0; // epoch

    var d32_buffers = [_]?*anyopaque{ @ptrCast(d32_validity.ptr), @ptrCast(d32_data.ptr) };
    const d32_arr = ArrowArray{
        .length = 3, .null_count = 1, .offset = 0, .n_buffers = 2,
        .n_children = 0, .buffers = &d32_buffers, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };
    const d32_sch = ArrowSchema{
        .format = "tdD", .name = "date_col", .metadata = null,
        .flags = arrow.ARROW_FLAG_NULLABLE, .n_children = 0, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };

    // Write to Parquet
    const col_defs = [_]ColumnDef{
        .{ .name = "ts_sec", .type_ = .int64, .optional = true, .logical_type = .{ .timestamp = .{ .is_adjusted_to_utc = true, .unit = .millis } } },
        .{ .name = "ts_ms", .type_ = .int64, .optional = true, .logical_type = .{ .timestamp = .{ .is_adjusted_to_utc = true, .unit = .millis } } },
        .{ .name = "ts_us", .type_ = .int64, .optional = true, .logical_type = .{ .timestamp = .{ .is_adjusted_to_utc = true, .unit = .micros } } },
        .{ .name = "date_col", .type_ = .int32, .optional = true, .logical_type = .date },
    };

    var writer = try api_writer_mod.writeToBuffer(allocator, &col_defs);
    const arrays = [_]ArrowArray{ tss_arr, tsm_arr, tsu_arr, d32_arr };
    const schemas = [_]ArrowSchema{ tss_sch, tsm_sch, tsu_sch, d32_sch };
    try writeRowGroupFromArrow(&writer, allocator, &arrays, &schemas);
    try writer.close();
    const buf = try writer.toOwnedSlice();
    defer allocator.free(buf);
    writer.deinit();

    // Read back
    var dr = try api_reader_mod.openBufferDynamic(allocator, buf, .{});
    defer dr.deinit();
    var result = try readColumns(allocator, dr.getSource(), dr.metadata, 0, .{});
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 4), result.arrays.len);

    // Timestamp seconds should be converted to millis (* 1000)
    const read_tss: [*]const i64 = @ptrCast(@alignCast(result.arrays[0].buffers[1].?));
    try std.testing.expectEqual(@as(i64, 1705312200000), read_tss[0]);
    try std.testing.expectEqual(@as(i64, 0), read_tss[1]);
    try std.testing.expectEqual(@as(i64, 1), result.arrays[0].null_count);

    // Timestamp millis passthrough
    const read_tsm: [*]const i64 = @ptrCast(@alignCast(result.arrays[1].buffers[1].?));
    try std.testing.expectEqual(@as(i64, 1705312200000), read_tsm[0]);
    try std.testing.expectEqual(@as(i64, 0), read_tsm[1]);
    try std.testing.expectEqual(@as(i64, 86400000), read_tsm[2]);

    // Timestamp micros passthrough
    const read_tsu: [*]const i64 = @ptrCast(@alignCast(result.arrays[2].buffers[1].?));
    try std.testing.expectEqual(@as(i64, 1705312200000000), read_tsu[0]);
    try std.testing.expectEqual(@as(i64, 0), read_tsu[1]);

    // Date32 passthrough
    const read_d32: [*]const i32 = @ptrCast(@alignCast(result.arrays[3].buffers[1].?));
    try std.testing.expectEqual(@as(i32, 19738), read_d32[0]);
    try std.testing.expectEqual(@as(i32, 0), read_d32[2]);
    try std.testing.expectEqual(@as(i64, 1), result.arrays[3].null_count);
}

// ============================================================================
// Arrow write round-trip tests: temporal types in nested contexts
// ============================================================================

test "round-trip: arrow write LIST of timestamp seconds" {
    const allocator = std.testing.allocator;

    const col_defs = [_]ColumnDef{
        .{ .name = "ts_list", .type_ = .int64, .optional = true, .is_list = true, .element_optional = true, .logical_type = .{ .timestamp = .{ .is_adjusted_to_utc = true, .unit = .millis } } },
    };

    var writer = try api_writer_mod.writeToBuffer(allocator, &col_defs);

    // Build Arrow list array with tss: child
    const n: usize = 3;
    const bitmap_len: usize = 1;

    // Parent offsets
    const offsets_mem = try allocator.alloc(u8, (n + 1) * 4);
    defer allocator.free(offsets_mem);
    const offsets: [*]i32 = @ptrCast(@alignCast(offsets_mem.ptr));
    offsets[0] = 0;
    offsets[1] = 2; // first list has 2 elements
    offsets[2] = 2; // second list is null
    offsets[3] = 3; // third list has 1 element

    // Parent validity
    const parent_validity = try allocator.alloc(u8, bitmap_len);
    defer allocator.free(parent_validity);
    @memset(parent_validity, 0xFF);
    arrow.clearBit(parent_validity, 1);

    // Child data: 3 timestamp-second values
    const child_data_mem = try allocator.alloc(u8, 3 * 8);
    defer allocator.free(child_data_mem);
    const child_typed: [*]i64 = @ptrCast(@alignCast(child_data_mem.ptr));
    child_typed[0] = 1000; // 1000 seconds
    child_typed[1] = 2000; // 2000 seconds
    child_typed[2] = 3000; // 3000 seconds

    const child_validity = try allocator.alloc(u8, bitmap_len);
    defer allocator.free(child_validity);
    @memset(child_validity, 0xFF);

    var child_buffers = [_]?*anyopaque{ @ptrCast(child_validity.ptr), @ptrCast(child_data_mem.ptr) };
    var child_arr_val = ArrowArray{
        .length = 3, .null_count = 0, .offset = 0, .n_buffers = 2,
        .n_children = 0, .buffers = &child_buffers, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };

    var children_ptrs = [_]*ArrowArray{&child_arr_val};
    var parent_buffers = [_]?*anyopaque{ @ptrCast(parent_validity.ptr), @ptrCast(offsets_mem.ptr) };
    const list_arr = ArrowArray{
        .length = 3, .null_count = 1, .offset = 0, .n_buffers = 2,
        .n_children = 1, .buffers = &parent_buffers, .children = @ptrCast(&children_ptrs),
        .dictionary = null, .release = null, .private_data = null,
    };

    const child_sch = ArrowSchema{
        .format = "tss:UTC", .name = "item", .metadata = null,
        .flags = arrow.ARROW_FLAG_NULLABLE, .n_children = 0, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };
    var child_sch_ptrs = [_]*const ArrowSchema{&child_sch};
    const list_sch = ArrowSchema{
        .format = "+l", .name = "ts_list", .metadata = null,
        .flags = arrow.ARROW_FLAG_NULLABLE, .n_children = 1,
        .children = @constCast(@ptrCast(&child_sch_ptrs)),
        .dictionary = null, .release = null, .private_data = null,
    };

    const arrays = [_]ArrowArray{list_arr};
    const schemas = [_]ArrowSchema{list_sch};
    try writeRowGroupFromArrow(&writer, allocator, &arrays, &schemas);
    try writer.close();
    const buf = try writer.toOwnedSlice();
    defer allocator.free(buf);
    writer.deinit();

    // Read back
    var dr = try api_reader_mod.openBufferDynamic(allocator, buf, .{});
    defer dr.deinit();
    var result = try readColumns(allocator, dr.getSource(), dr.metadata, 0, .{});
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.arrays.len);
    const read_list = &result.arrays[0];
    try std.testing.expectEqual(@as(i64, 3), read_list.length);

    // Check child values are scaled: 1000s -> 1000000ms, 2000s -> 2000000ms, 3000s -> 3000000ms
    const read_children: [*]*ArrowArray = read_list.children.?;
    const read_child = read_children[0];
    const read_child_data: [*]const i64 = @ptrCast(@alignCast(read_child.buffers[1].?));
    try std.testing.expectEqual(@as(i64, 1000000), read_child_data[0]);
    try std.testing.expectEqual(@as(i64, 2000000), read_child_data[1]);
    try std.testing.expectEqual(@as(i64, 3000000), read_child_data[2]);
}

test "round-trip: arrow write STRUCT with timestamp seconds field" {
    const allocator = std.testing.allocator;

    const struct_fields = [_]StructField{
        .{ .name = "label", .type_ = .byte_array },
        .{ .name = "ts", .type_ = .int64, .logical_type = .{ .timestamp = .{ .is_adjusted_to_utc = true, .unit = .millis } } },
    };

    const col_defs = [_]ColumnDef{
        .{ .name = "event", .type_ = .byte_array, .optional = true, .is_struct = true, .struct_fields = &struct_fields },
    };

    var writer = try api_writer_mod.writeToBuffer(allocator, &col_defs);
    const n: usize = 2;
    const bitmap_len: usize = 1;

    // Struct parent
    const struct_validity = try allocator.alloc(u8, bitmap_len);
    defer allocator.free(struct_validity);
    @memset(struct_validity, 0xFF);

    // Child 0: label (string)
    const str_offsets_mem = try allocator.alloc(u8, (n + 1) * 4);
    defer allocator.free(str_offsets_mem);
    const str_offsets: [*]i32 = @ptrCast(@alignCast(str_offsets_mem.ptr));
    str_offsets[0] = 0;
    str_offsets[1] = 5; // "hello"
    str_offsets[2] = 10; // "world"

    const str_data = "helloworld";
    const str_validity = try allocator.alloc(u8, bitmap_len);
    defer allocator.free(str_validity);
    @memset(str_validity, 0xFF);

    var str_buffers = [_]?*anyopaque{ @ptrCast(str_validity.ptr), @ptrCast(str_offsets_mem.ptr), @ptrCast(@constCast(str_data.ptr)) };
    var str_arr = ArrowArray{
        .length = 2, .null_count = 0, .offset = 0, .n_buffers = 3,
        .n_children = 0, .buffers = &str_buffers, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };

    // Child 1: ts (timestamp seconds)
    const ts_data_mem = try allocator.alloc(u8, n * 8);
    defer allocator.free(ts_data_mem);
    const ts_typed: [*]i64 = @ptrCast(@alignCast(ts_data_mem.ptr));
    ts_typed[0] = 5000; // 5000 seconds
    ts_typed[1] = 10000; // 10000 seconds

    const ts_validity = try allocator.alloc(u8, bitmap_len);
    defer allocator.free(ts_validity);
    @memset(ts_validity, 0xFF);

    var ts_buffers = [_]?*anyopaque{ @ptrCast(ts_validity.ptr), @ptrCast(ts_data_mem.ptr) };
    var ts_arr = ArrowArray{
        .length = 2, .null_count = 0, .offset = 0, .n_buffers = 2,
        .n_children = 0, .buffers = &ts_buffers, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };

    var struct_child_ptrs = [_]*ArrowArray{ &str_arr, &ts_arr };
    var struct_buffers = [_]?*anyopaque{ @ptrCast(struct_validity.ptr), null };
    const struct_arr = ArrowArray{
        .length = 2, .null_count = 0, .offset = 0, .n_buffers = 2,
        .n_children = 2, .buffers = &struct_buffers,
        .children = @ptrCast(&struct_child_ptrs),
        .dictionary = null, .release = null, .private_data = null,
    };

    const str_sch = ArrowSchema{
        .format = "u", .name = "label", .metadata = null,
        .flags = arrow.ARROW_FLAG_NULLABLE, .n_children = 0, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };
    const ts_sch = ArrowSchema{
        .format = "tss:UTC", .name = "ts", .metadata = null,
        .flags = arrow.ARROW_FLAG_NULLABLE, .n_children = 0, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };
    var struct_child_sch_ptrs = [_]*const ArrowSchema{ &str_sch, &ts_sch };
    const struct_sch = ArrowSchema{
        .format = "+s", .name = "event", .metadata = null,
        .flags = arrow.ARROW_FLAG_NULLABLE, .n_children = 2,
        .children = @constCast(@ptrCast(&struct_child_sch_ptrs)),
        .dictionary = null, .release = null, .private_data = null,
    };

    const arr_slice = [_]ArrowArray{struct_arr};
    const sch_slice = [_]ArrowSchema{struct_sch};
    try writeRowGroupFromArrow(&writer, allocator, &arr_slice, &sch_slice);
    try writer.close();
    const buf = try writer.toOwnedSlice();
    defer allocator.free(buf);
    writer.deinit();

    // Read back
    var dr = try api_reader_mod.openBufferDynamic(allocator, buf, .{});
    defer dr.deinit();
    var result = try readColumns(allocator, dr.getSource(), dr.metadata, 0, .{});
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.arrays.len);
    const read_struct = &result.arrays[0];
    try std.testing.expectEqual(@as(i64, 2), read_struct.n_children);

    // Check timestamp field: 5000s -> 5000000ms, 10000s -> 10000000ms
    const children: [*]*ArrowArray = read_struct.children.?;
    const ts_child = children[1];
    const read_ts: [*]const i64 = @ptrCast(@alignCast(ts_child.buffers[1].?));
    try std.testing.expectEqual(@as(i64, 5000000), read_ts[0]);
    try std.testing.expectEqual(@as(i64, 10000000), read_ts[1]);
}

// ============================================================================
// Arrow write round-trip tests: type widening
// ============================================================================

test "round-trip: arrow write then read - widened integer types" {
    const allocator = std.testing.allocator;
    const n: usize = 3;
    const bitmap_len = 1;

    // i8 column (c -> i32)
    const i8_validity = try allocator.alloc(u8, bitmap_len);
    defer allocator.free(i8_validity);
    @memset(i8_validity, 0xFF);

    const i8_data = try allocator.alloc(u8, n);
    defer allocator.free(i8_data);
    const i8_typed: [*]i8 = @ptrCast(i8_data.ptr);
    i8_typed[0] = -128;
    i8_typed[1] = 0;
    i8_typed[2] = 127;

    var i8_buffers = [_]?*anyopaque{ @ptrCast(i8_validity.ptr), @ptrCast(i8_data.ptr) };
    const i8_arr = ArrowArray{
        .length = 3, .null_count = 0, .offset = 0, .n_buffers = 2,
        .n_children = 0, .buffers = &i8_buffers, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };
    const i8_sch = ArrowSchema{
        .format = "c", .name = "i8_col", .metadata = null,
        .flags = 0, .n_children = 0, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };

    // u8 column (C -> i32)
    const u8_validity = try allocator.alloc(u8, bitmap_len);
    defer allocator.free(u8_validity);
    @memset(u8_validity, 0xFF);

    const u8_data = try allocator.alloc(u8, n);
    defer allocator.free(u8_data);
    u8_data[0] = 0;
    u8_data[1] = 128;
    u8_data[2] = 255;

    var u8_buffers = [_]?*anyopaque{ @ptrCast(u8_validity.ptr), @ptrCast(u8_data.ptr) };
    const u8_arr = ArrowArray{
        .length = 3, .null_count = 0, .offset = 0, .n_buffers = 2,
        .n_children = 0, .buffers = &u8_buffers, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };
    const u8_sch = ArrowSchema{
        .format = "C", .name = "u8_col", .metadata = null,
        .flags = 0, .n_children = 0, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };

    // u32 column (I -> i64)
    const u32_validity = try allocator.alloc(u8, bitmap_len);
    defer allocator.free(u32_validity);
    @memset(u32_validity, 0xFF);

    const u32_data = try allocator.alloc(u8, n * 4);
    defer allocator.free(u32_data);
    const u32_typed: [*]u32 = @ptrCast(@alignCast(u32_data.ptr));
    u32_typed[0] = 0;
    u32_typed[1] = 2147483648; // exceeds i32 max
    u32_typed[2] = 4294967295; // u32 max

    var u32_buffers = [_]?*anyopaque{ @ptrCast(u32_validity.ptr), @ptrCast(u32_data.ptr) };
    const u32_arr = ArrowArray{
        .length = 3, .null_count = 0, .offset = 0, .n_buffers = 2,
        .n_children = 0, .buffers = &u32_buffers, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };
    const u32_sch = ArrowSchema{
        .format = "I", .name = "u32_col", .metadata = null,
        .flags = 0, .n_children = 0, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };

    // Write
    const col_defs = [_]ColumnDef{
        .{ .name = "i8_col", .type_ = .int32, .optional = false, .logical_type = .{ .int = .{ .bit_width = 8, .is_signed = true } } },
        .{ .name = "u8_col", .type_ = .int32, .optional = false, .logical_type = .{ .int = .{ .bit_width = 8, .is_signed = false } } },
        .{ .name = "u32_col", .type_ = .int64, .optional = false, .logical_type = .{ .int = .{ .bit_width = 32, .is_signed = false } } },
    };

    var writer = try api_writer_mod.writeToBuffer(allocator, &col_defs);
    const arrays = [_]ArrowArray{ i8_arr, u8_arr, u32_arr };
    const schemas = [_]ArrowSchema{ i8_sch, u8_sch, u32_sch };
    try writeRowGroupFromArrow(&writer, allocator, &arrays, &schemas);
    try writer.close();
    const buf = try writer.toOwnedSlice();
    defer allocator.free(buf);
    writer.deinit();

    // Read back
    var dr = try api_reader_mod.openBufferDynamic(allocator, buf, .{});
    defer dr.deinit();
    var result = try readColumns(allocator, dr.getSource(), dr.metadata, 0, .{});
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 3), result.arrays.len);

    // i8 -> stored as i32
    const read_i8: [*]const i32 = @ptrCast(@alignCast(result.arrays[0].buffers[1].?));
    try std.testing.expectEqual(@as(i32, -128), read_i8[0]);
    try std.testing.expectEqual(@as(i32, 0), read_i8[1]);
    try std.testing.expectEqual(@as(i32, 127), read_i8[2]);

    // u8 -> stored as i32
    const read_u8: [*]const i32 = @ptrCast(@alignCast(result.arrays[1].buffers[1].?));
    try std.testing.expectEqual(@as(i32, 0), read_u8[0]);
    try std.testing.expectEqual(@as(i32, 128), read_u8[1]);
    try std.testing.expectEqual(@as(i32, 255), read_u8[2]);

    // u32 -> stored as i64
    const read_u32: [*]const i64 = @ptrCast(@alignCast(result.arrays[2].buffers[1].?));
    try std.testing.expectEqual(@as(i64, 0), read_u32[0]);
    try std.testing.expectEqual(@as(i64, 2147483648), read_u32[1]);
    try std.testing.expectEqual(@as(i64, 4294967295), read_u32[2]);
}

// ============================================================================
// Arrow write round-trip tests: boolean
// ============================================================================

test "round-trip: arrow write then read - boolean" {
    const allocator = std.testing.allocator;
    const bitmap_len: usize = 1;

    const validity = try allocator.alloc(u8, bitmap_len);
    defer allocator.free(validity);
    @memset(validity, 0xFF);
    arrow.clearBit(validity, 2);

    const bool_data = try allocator.alloc(u8, bitmap_len);
    defer allocator.free(bool_data);
    @memset(bool_data, 0);
    arrow.setBit(bool_data, 0);
    // index 1 = false
    // index 2 = null
    arrow.setBit(bool_data, 3);

    var buffers = [_]?*anyopaque{ @ptrCast(validity.ptr), @ptrCast(bool_data.ptr) };
    const bool_arr = ArrowArray{
        .length = 4, .null_count = 1, .offset = 0, .n_buffers = 2,
        .n_children = 0, .buffers = &buffers, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };
    const bool_sch = ArrowSchema{
        .format = "b", .name = "flag", .metadata = null,
        .flags = arrow.ARROW_FLAG_NULLABLE, .n_children = 0, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };

    const col_defs = [_]ColumnDef{
        .{ .name = "flag", .type_ = .boolean, .optional = true },
    };

    var writer = try api_writer_mod.writeToBuffer(allocator, &col_defs);
    const arrays = [_]ArrowArray{bool_arr};
    const schemas = [_]ArrowSchema{bool_sch};
    try writeRowGroupFromArrow(&writer, allocator, &arrays, &schemas);
    try writer.close();
    const buf = try writer.toOwnedSlice();
    defer allocator.free(buf);
    writer.deinit();

    var dr = try api_reader_mod.openBufferDynamic(allocator, buf, .{});
    defer dr.deinit();
    var result = try readColumns(allocator, dr.getSource(), dr.metadata, 0, .{});
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.arrays.len);
    const read_arr = &result.arrays[0];
    try std.testing.expectEqual(@as(i64, 4), read_arr.length);
    try std.testing.expectEqual(@as(i64, 1), read_arr.null_count);

    const read_data: [*]const u8 = @ptrCast(@alignCast(read_arr.buffers[1].?));
    try std.testing.expect(arrow.getBit(read_data[0..1], 0)); // true
    try std.testing.expect(!arrow.getBit(read_data[0..1], 1)); // false
    try std.testing.expect(arrow.getBit(read_data[0..1], 3)); // true

    const read_validity: [*]const u8 = @ptrCast(@alignCast(read_arr.buffers[0].?));
    try std.testing.expect(!arrow.getBit(read_validity[0..1], 2)); // null
}

// ============================================================================
// Arrow write round-trip tests: large string/binary
// ============================================================================

test "round-trip: arrow write then read - large utf8" {
    const allocator = std.testing.allocator;
    const n: usize = 3;
    const bitmap_len: usize = 1;

    const validity = try allocator.alloc(u8, bitmap_len);
    defer allocator.free(validity);
    @memset(validity, 0xFF);
    arrow.clearBit(validity, 1);

    // Large string uses i64 offsets
    const offsets_mem = try allocator.alloc(u8, (n + 1) * 8);
    defer allocator.free(offsets_mem);
    const offsets: [*]i64 = @ptrCast(@alignCast(offsets_mem.ptr));
    offsets[0] = 0;
    offsets[1] = 5; // "hello"
    offsets[2] = 5; // null
    offsets[3] = 10; // "world"

    const str_data = "helloworld";

    var buffers = [_]?*anyopaque{ @ptrCast(validity.ptr), @ptrCast(offsets_mem.ptr), @ptrCast(@constCast(str_data.ptr)) };
    const large_str_arr = ArrowArray{
        .length = 3, .null_count = 1, .offset = 0, .n_buffers = 3,
        .n_children = 0, .buffers = &buffers, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };
    const large_str_sch = ArrowSchema{
        .format = "U", .name = "text", .metadata = null,
        .flags = arrow.ARROW_FLAG_NULLABLE, .n_children = 0, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };

    const col_defs = [_]ColumnDef{
        .{ .name = "text", .type_ = .byte_array, .optional = true, .logical_type = .string },
    };

    var writer = try api_writer_mod.writeToBuffer(allocator, &col_defs);
    const arrays = [_]ArrowArray{large_str_arr};
    const schemas = [_]ArrowSchema{large_str_sch};
    try writeRowGroupFromArrow(&writer, allocator, &arrays, &schemas);
    try writer.close();
    const buf = try writer.toOwnedSlice();
    defer allocator.free(buf);
    writer.deinit();

    var dr = try api_reader_mod.openBufferDynamic(allocator, buf, .{});
    defer dr.deinit();
    var result = try readColumns(allocator, dr.getSource(), dr.metadata, 0, .{});
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.arrays.len);
    const read_arr = &result.arrays[0];
    try std.testing.expectEqual(@as(i64, 3), read_arr.length);
    try std.testing.expectEqual(@as(i64, 1), read_arr.null_count);

    const read_offsets: [*]const i32 = @ptrCast(@alignCast(read_arr.buffers[1].?));
    const read_data: [*]const u8 = @ptrCast(@alignCast(read_arr.buffers[2].?));
    const s0 = @as(usize, @intCast(read_offsets[0]));
    const e0 = @as(usize, @intCast(read_offsets[1]));
    try std.testing.expectEqualStrings("hello", read_data[s0..e0]);

    const s2 = @as(usize, @intCast(read_offsets[2]));
    const e2 = @as(usize, @intCast(read_offsets[3]));
    try std.testing.expectEqualStrings("world", read_data[s2..e2]);

    const read_validity: [*]const u8 = @ptrCast(@alignCast(read_arr.buffers[0].?));
    try std.testing.expect(!arrow.getBit(read_validity[0..1], 1));
}

// ============================================================================
// Arrow write round-trip tests: decimal
// ============================================================================

/// A flat Arrow decimal array of `width` bytes per entry (4, 8, 16 or 32); null values are
/// nulls. Buffers are allocated with `arena`.
fn testDecimalArray(arena: Allocator, width: usize, values: []const ?i256) !ArrowArray {
    const n = values.len;
    const validity = try arena.alloc(u8, (n + 7) / 8 + 1);
    @memset(validity, 0xFF);
    const data = try arena.alignedAlloc(u8, .@"16", n * width + 1);
    @memset(data, 0);
    var nulls: i64 = 0;
    for (values, 0..) |v, i| {
        const value = v orelse {
            arrow.clearBit(validity, i);
            nulls += 1;
            continue;
        };
        const out = data[i * width ..];
        switch (width) {
            4 => std.mem.writeInt(i32, out[0..4], @intCast(value), .little),
            8 => std.mem.writeInt(i64, out[0..8], @intCast(value), .little),
            16 => std.mem.writeInt(i128, out[0..16], @intCast(value), .little),
            else => std.mem.writeInt(i256, out[0..32], value, .little),
        }
    }
    const buffers = try arena.alloc(?*anyopaque, 2);
    buffers[0] = @ptrCast(validity.ptr);
    buffers[1] = @ptrCast(data.ptr);
    return .{
        .length = @intCast(n), .null_count = nulls, .offset = 0, .n_buffers = 2,
        .n_children = 0, .buffers = buffers.ptr, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };
}

fn testLeafSchema(format_str: [*:0]const u8, name: [*:0]const u8) ArrowSchema {
    return .{
        .format = format_str, .name = name, .metadata = null,
        .flags = arrow.ARROW_FLAG_NULLABLE, .n_children = 0, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };
}

/// Checks a decimal array read back: `width` bytes per entry, null where `expected` is null.
fn expectDecimalArray(arr: *const ArrowArray, width: usize, expected: []const ?i256) !void {
    try std.testing.expectEqual(@as(i64, @intCast(expected.len)), arr.length);
    const data: [*]const u8 = @ptrCast(arr.buffers[1].?);
    const validity: ?[*]const u8 = if (arr.buffers[0]) |v| @ptrCast(v) else null;
    for (expected, 0..) |e, i| {
        const valid = if (validity) |v| (v[i / 8] >> @intCast(i % 8)) & 1 == 1 else true;
        try std.testing.expectEqual(e != null, valid);
        if (e) |value| try std.testing.expectEqual(value, arrowDecimalAt(data, width, i));
    }
}

test "imported schema keeps no references to the Arrow schema's names" {
    const allocator = std.testing.allocator;

    // Names in heap memory, as a C caller's schema would have them. Schema:
    // top: int32, point: struct<x: int32>, items: list<struct<y: int64>>.
    const name_list = [_][]const u8{ "top", "point", "x", "items", "element", "y" };
    var names: [name_list.len][:0]u8 = undefined;
    for (name_list, 0..) |n, i| names[i] = try allocator.dupeSentinel(u8, n, 0);
    var freed = false;
    defer if (!freed) for (names) |n| allocator.free(n);

    var top = testLeafSchema("i", names[0].ptr);
    var x = testLeafSchema("i", names[2].ptr);
    var point_children = [_]*ArrowSchema{&x};
    var point = ArrowSchema{ .format = "+s", .name = names[1].ptr, .metadata = null, .flags = arrow.ARROW_FLAG_NULLABLE, .n_children = 1, .children = &point_children, .dictionary = null, .release = null, .private_data = null };
    var y = testLeafSchema("l", names[5].ptr);
    var element_children = [_]*ArrowSchema{&y};
    var element = ArrowSchema{ .format = "+s", .name = names[4].ptr, .metadata = null, .flags = arrow.ARROW_FLAG_NULLABLE, .n_children = 1, .children = &element_children, .dictionary = null, .release = null, .private_data = null };
    var items_children = [_]*ArrowSchema{&element};
    var items = ArrowSchema{ .format = "+l", .name = names[3].ptr, .metadata = null, .flags = arrow.ARROW_FLAG_NULLABLE, .n_children = 1, .children = &items_children, .dictionary = null, .release = null, .private_data = null };
    var root_children = [_]*ArrowSchema{ &top, &point, &items };
    const root = ArrowSchema{ .format = "+s", .name = "", .metadata = null, .flags = 0, .n_children = 3, .children = &root_children, .dictionary = null, .release = null, .private_data = null };

    const col_defs = try importSchemaFromArrow(allocator, &root);
    defer freeImportedColumnDefs(allocator, col_defs);

    // The caller releases its schema.
    for (names) |n| {
        @memset(n, 'Z');
        allocator.free(n);
    }
    freed = true;

    // One row group of 0 rows.
    var zero_offsets = [_]i32{0};
    var no_data: [8]u8 = @splat(0);
    var leaf_buffers = [_]?*anyopaque{ null, @ptrCast(&no_data) };
    var y_arr = ArrowArray{ .length = 0, .null_count = 0, .offset = 0, .n_buffers = 2, .n_children = 0, .buffers = &leaf_buffers, .children = null, .dictionary = null, .release = null, .private_data = null };
    var struct_buffers = [_]?*anyopaque{null};
    var element_arr_children = [_]*ArrowArray{&y_arr};
    var element_arr = ArrowArray{ .length = 0, .null_count = 0, .offset = 0, .n_buffers = 1, .n_children = 1, .buffers = &struct_buffers, .children = &element_arr_children, .dictionary = null, .release = null, .private_data = null };
    var list_buffers = [_]?*anyopaque{ null, @ptrCast(&zero_offsets) };
    var items_arr_children = [_]*ArrowArray{&element_arr};
    const items_arr = ArrowArray{ .length = 0, .null_count = 0, .offset = 0, .n_buffers = 2, .n_children = 1, .buffers = &list_buffers, .children = &items_arr_children, .dictionary = null, .release = null, .private_data = null };
    var x_arr = y_arr;
    var point_arr_children = [_]*ArrowArray{&x_arr};
    const point_arr = ArrowArray{ .length = 0, .null_count = 0, .offset = 0, .n_buffers = 1, .n_children = 1, .buffers = &struct_buffers, .children = &point_arr_children, .dictionary = null, .release = null, .private_data = null };
    const top_arr = y_arr;

    var writer = try api_writer_mod.writeToBuffer(allocator, col_defs);
    try writeRowGroupFromArrow(&writer, allocator, &.{ top_arr, point_arr, items_arr }, &.{ top, point, items });
    try writer.close();
    const buf = try writer.toOwnedSlice();
    defer allocator.free(buf);
    writer.deinit();

    var dr = try api_reader_mod.openBufferDynamic(allocator, buf, .{});
    defer dr.deinit();
    var written: std.ArrayList(u8) = .empty;
    defer written.deinit(allocator);
    for (dr.metadata.schema[1..]) |elem| {
        try written.appendSlice(allocator, elem.name);
        try written.append(allocator, ' ');
    }
    try std.testing.expectEqualStrings("top point x items list element y ", written.items);
}

test "arrow write honours array offsets" {
    const allocator = std.testing.allocator;

    // Each column holds 12 entries and is written from entry 3 (offset 3, length 6), so the
    // validity bits start mid-byte. Entry i is null when i % 4 == 1.
    const len = 6;
    const off = 3;
    var validity = [_]u8{ 0b1101_1101, 0b1101 };

    var ints = [_]i32{ 0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110 };
    var int_buffers = [_]?*anyopaque{ @ptrCast(&validity), @ptrCast(&ints) };
    const int_arr = ArrowArray{ .length = len, .null_count = -1, .offset = off, .n_buffers = 2, .n_children = 0, .buffers = &int_buffers, .children = null, .dictionary = null, .release = null, .private_data = null };

    var bools = [_]u8{ 0b0101_0101, 0b0101 };
    var bool_buffers = [_]?*anyopaque{ @ptrCast(&validity), @ptrCast(&bools) };
    const bool_arr = ArrowArray{ .length = len, .null_count = -1, .offset = off, .n_buffers = 2, .n_children = 0, .buffers = &bool_buffers, .children = null, .dictionary = null, .release = null, .private_data = null };

    // Strings "s0".."s11" (2 or 3 bytes each).
    var str_data: [64]u8 = undefined;
    var str_offsets: [13]i32 = undefined;
    var pos: usize = 0;
    for (0..12) |i| {
        str_offsets[i] = @intCast(pos);
        pos += (try std.fmt.bufPrint(str_data[pos..], "s{d}", .{i})).len;
    }
    str_offsets[12] = @intCast(pos);
    var str_buffers = [_]?*anyopaque{ @ptrCast(&validity), @ptrCast(&str_offsets), @ptrCast(&str_data) };
    const str_arr = ArrowArray{ .length = len, .null_count = -1, .offset = off, .n_buffers = 3, .n_children = 0, .buffers = &str_buffers, .children = null, .dictionary = null, .release = null, .private_data = null };

    // list<int32>: entry i holds i % 3 elements from a 30-element child that itself has
    // offset 2.
    var child_values: [32]i32 = undefined;
    for (&child_values, 0..) |*v, i| v.* = @intCast(i * 7);
    var child_buffers = [_]?*anyopaque{ null, @ptrCast(&child_values) };
    var child_arr = ArrowArray{ .length = 30, .null_count = 0, .offset = 2, .n_buffers = 2, .n_children = 0, .buffers = &child_buffers, .children = null, .dictionary = null, .release = null, .private_data = null };
    var list_offsets: [13]i32 = undefined;
    var total: i32 = 0;
    for (0..12) |i| {
        list_offsets[i] = total;
        total += @intCast(i % 3);
    }
    list_offsets[12] = total;
    var list_buffers = [_]?*anyopaque{ @ptrCast(&validity), @ptrCast(&list_offsets) };
    var list_children = [_]*ArrowArray{&child_arr};
    const list_arr = ArrowArray{ .length = len, .null_count = -1, .offset = off, .n_buffers = 2, .n_children = 1, .buffers = &list_buffers, .children = &list_children, .dictionary = null, .release = null, .private_data = null };

    // struct<a: int32>: the field is the ints array with its own offset 1, so struct entry i
    // holds ints[i + 1].
    var field_arr = ArrowArray{ .length = 11, .null_count = 0, .offset = 1, .n_buffers = 2, .n_children = 0, .buffers = &int_buffers, .children = null, .dictionary = null, .release = null, .private_data = null };
    var all_valid = [_]u8{ 0xFF, 0xFF };
    field_arr.buffers = @ptrCast(@constCast(&[_]?*anyopaque{ @ptrCast(&all_valid), @ptrCast(&ints) }));
    var struct_buffers = [_]?*anyopaque{@ptrCast(&validity)};
    var struct_children = [_]*ArrowArray{&field_arr};
    const struct_arr = ArrowArray{ .length = len, .null_count = -1, .offset = off, .n_buffers = 1, .n_children = 1, .buffers = &struct_buffers, .children = &struct_children, .dictionary = null, .release = null, .private_data = null };

    var element_sch = testLeafSchema("i", "element");
    var list_sch_children = [_]*ArrowSchema{&element_sch};
    var a_sch = testLeafSchema("i", "a");
    var struct_sch_children = [_]*ArrowSchema{&a_sch};
    var schemas = [_]ArrowSchema{
        testLeafSchema("i", "ints"),
        testLeafSchema("b", "bools"),
        testLeafSchema("u", "strs"),
        .{ .format = "+l", .name = "list", .metadata = null, .flags = arrow.ARROW_FLAG_NULLABLE, .n_children = 1, .children = &list_sch_children, .dictionary = null, .release = null, .private_data = null },
        .{ .format = "+s", .name = "st", .metadata = null, .flags = arrow.ARROW_FLAG_NULLABLE, .n_children = 1, .children = &struct_sch_children, .dictionary = null, .release = null, .private_data = null },
    };
    var schema_ptrs: [schemas.len]*ArrowSchema = undefined;
    for (&schema_ptrs, &schemas) |*ptr, *sch| ptr.* = sch;
    const root = ArrowSchema{ .format = "+s", .name = "", .metadata = null, .flags = 0, .n_children = schemas.len, .children = &schema_ptrs, .dictionary = null, .release = null, .private_data = null };
    const col_defs = try importSchemaFromArrow(allocator, &root);
    defer freeImportedColumnDefs(allocator, col_defs);

    var writer = try api_writer_mod.writeToBuffer(allocator, col_defs);
    try writeRowGroupFromArrow(&writer, allocator, &.{ int_arr, bool_arr, str_arr, list_arr, struct_arr }, &schemas);
    try writer.close();
    const buf = try writer.toOwnedSlice();
    defer allocator.free(buf);
    writer.deinit();

    var dr = try api_reader_mod.openBufferDynamic(allocator, buf, .{});
    defer dr.deinit();
    var tree_arena = std.heap.ArenaAllocator.init(allocator);
    defer tree_arena.deinit();
    const tree = try buildSchemaTree(tree_arena.allocator(), dr.metadata.schema);
    var result = try readColumns(allocator, dr.getSource(), dr.metadata, 0, .{});
    defer result.deinit();

    var got: std.Io.Writer.Allocating = .init(allocator);
    defer got.deinit();
    for (0..len) |row| {
        for (tree, 0..) |*node, c| {
            try canonEntry(&got.writer, dr.metadata.schema, node, &result.arrays[c], row, false);
            try got.writer.writeByte(' ');
        }
        try got.writer.writeByte('\n');
    }
    // Entries 3 to 8; entry 5 is null. Bools: true for even i. Strings: hex of "s<i>".
    // Lists: element j of entry i is child entry 2 + list_offsets[i] + j, value 7 times that.
    // Struct field: ints[i + 1].
    try std.testing.expectEqualStrings(
        "i30 f x7333 [] {i40} \n" ++
            "i40 t x7334 [i35] {i50} \n" ++
            "n n n n n \n" ++
            "i60 t x7336 [] {i70} \n" ++
            "i70 f x7337 [i56] {i80} \n" ++
            "i80 t x7338 [i63,i70] {i90} \n",
        got.written(),
    );
}

test "arrow write of maps keeps key and value types, also when every value is null" {
    const allocator = std.testing.allocator;

    // Two map columns over the same keys: {a: null}, null, {}, {b: null, c: null} with int64
    // values (all null), and {a: 1.50}, null, {}, {b: null, c: -2.25} with decimal(9,2).
    var key_data = "abc".*;
    var key_offsets = [_]i32{ 0, 1, 2, 3 };
    var key_buffers = [_]?*anyopaque{ null, @ptrCast(&key_offsets), @ptrCast(&key_data) };
    var keys = ArrowArray{ .length = 3, .null_count = 0, .offset = 0, .n_buffers = 3, .n_children = 0, .buffers = &key_buffers, .children = null, .dictionary = null, .release = null, .private_data = null };
    var no_values = [_]u8{0b000};
    var int_values = [_]i64{ 0, 0, 0 };
    var int_buffers = [_]?*anyopaque{ @ptrCast(&no_values), @ptrCast(&int_values) };
    var ints = ArrowArray{ .length = 3, .null_count = 3, .offset = 0, .n_buffers = 2, .n_children = 0, .buffers = &int_buffers, .children = null, .dictionary = null, .release = null, .private_data = null };
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    var decimals = try testDecimalArray(arena_state.allocator(), 16, &.{ 150, null, -225 });

    var map_offsets = [_]i32{ 0, 1, 1, 1, 3 };
    var map_validity = [_]u8{0b1101};
    var struct_buffers = [_]?*anyopaque{null};
    var int_entry_children = [_]*ArrowArray{ &keys, &ints };
    var int_entries = ArrowArray{ .length = 3, .null_count = 0, .offset = 0, .n_buffers = 1, .n_children = 2, .buffers = &struct_buffers, .children = &int_entry_children, .dictionary = null, .release = null, .private_data = null };
    var dec_entry_children = [_]*ArrowArray{ &keys, &decimals };
    var dec_entries = ArrowArray{ .length = 3, .null_count = 0, .offset = 0, .n_buffers = 1, .n_children = 2, .buffers = &struct_buffers, .children = &dec_entry_children, .dictionary = null, .release = null, .private_data = null };
    var map_buffers = [_]?*anyopaque{ @ptrCast(&map_validity), @ptrCast(&map_offsets) };
    var int_map_children = [_]*ArrowArray{&int_entries};
    const int_map = ArrowArray{ .length = 4, .null_count = 1, .offset = 0, .n_buffers = 2, .n_children = 1, .buffers = &map_buffers, .children = &int_map_children, .dictionary = null, .release = null, .private_data = null };
    var dec_map_children = [_]*ArrowArray{&dec_entries};
    const dec_map = ArrowArray{ .length = 4, .null_count = 1, .offset = 0, .n_buffers = 2, .n_children = 1, .buffers = &map_buffers, .children = &dec_map_children, .dictionary = null, .release = null, .private_data = null };

    var key_sch = ArrowSchema{ .format = "u", .name = "key", .metadata = null, .flags = 0, .n_children = 0, .children = null, .dictionary = null, .release = null, .private_data = null };
    var int_sch = testLeafSchema("l", "value");
    var dec_sch = testLeafSchema("d:9,2", "value");
    var int_entry_sch_children = [_]*ArrowSchema{ &key_sch, &int_sch };
    var int_entry_sch = ArrowSchema{ .format = "+s", .name = "entries", .metadata = null, .flags = 0, .n_children = 2, .children = &int_entry_sch_children, .dictionary = null, .release = null, .private_data = null };
    var dec_entry_sch_children = [_]*ArrowSchema{ &key_sch, &dec_sch };
    var dec_entry_sch = ArrowSchema{ .format = "+s", .name = "entries", .metadata = null, .flags = 0, .n_children = 2, .children = &dec_entry_sch_children, .dictionary = null, .release = null, .private_data = null };
    var int_map_sch_children = [_]*ArrowSchema{&int_entry_sch};
    var dec_map_sch_children = [_]*ArrowSchema{&dec_entry_sch};
    var schemas = [_]ArrowSchema{
        .{ .format = "+m", .name = "ints", .metadata = null, .flags = arrow.ARROW_FLAG_NULLABLE, .n_children = 1, .children = &int_map_sch_children, .dictionary = null, .release = null, .private_data = null },
        .{ .format = "+m", .name = "decs", .metadata = null, .flags = arrow.ARROW_FLAG_NULLABLE, .n_children = 1, .children = &dec_map_sch_children, .dictionary = null, .release = null, .private_data = null },
    };
    var schema_ptrs = [_]*ArrowSchema{ &schemas[0], &schemas[1] };
    const root = ArrowSchema{ .format = "+s", .name = "", .metadata = null, .flags = 0, .n_children = 2, .children = &schema_ptrs, .dictionary = null, .release = null, .private_data = null };
    const col_defs = try importSchemaFromArrow(allocator, &root);
    defer freeImportedColumnDefs(allocator, col_defs);

    var writer = try api_writer_mod.writeToBuffer(allocator, col_defs);
    try writeRowGroupFromArrow(&writer, allocator, &.{ int_map, dec_map }, &schemas);
    try writer.close();
    const buf = try writer.toOwnedSlice();
    defer allocator.free(buf);
    writer.deinit();

    var dr = try api_reader_mod.openBufferDynamic(allocator, buf, .{});
    defer dr.deinit();
    // Leaf columns: ints.key, ints.value, decs.key, decs.value.
    const chunks = dr.metadata.row_groups[0].columns;
    try std.testing.expectEqual(format.PhysicalType.byte_array, chunks[0].meta_data.?.type_);
    try std.testing.expectEqual(format.PhysicalType.int64, chunks[1].meta_data.?.type_);
    try std.testing.expectEqual(format.PhysicalType.int32, chunks[3].meta_data.?.type_);
    for (dr.metadata.schema) |elem| {
        if (std.mem.eql(u8, elem.name, "key")) try std.testing.expect(elem.logical_type != null and elem.logical_type.? == .string);
    }

    const tree = try buildSchemaTree(arena_state.allocator(), dr.metadata.schema);
    var result = try readColumns(allocator, dr.getSource(), dr.metadata, 0, .{});
    defer result.deinit();
    var got: std.Io.Writer.Allocating = .init(allocator);
    defer got.deinit();
    for (0..4) |row| {
        for (tree, 0..) |*node, c| {
            try canonEntry(&got.writer, dr.metadata.schema, node, &result.arrays[c], row, false);
            try got.writer.writeByte(' ');
        }
        try got.writer.writeByte('\n');
    }
    try std.testing.expectEqualStrings(
        "[(x61:n)] [(x61:e150)] \n" ++
            "n n \n" ++
            "[] [] \n" ++
            "[(x62:n),(x63:n)] [(x62:n),(x63:e-225)] \n",
        got.written(),
    );
}

test "round-trip: arrow write then read - decimals in Arrow layout" {
    const allocator = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const max30: i256 = std.math.pow(i256, 10, 30) - 1;
    const max50: i256 = std.math.pow(i256, 10, 50) - 1;
    const d9 = [_]?i256{ 12345, -99999, null };
    const d18 = [_]?i256{ 123456789012345678, -1, 0 };
    const d30 = [_]?i256{ -max30, null, max30 };
    const d50 = [_]?i256{ max50, -max50, null };
    const d9_32 = [_]?i256{ 1, null, -2 };

    // Format, bytes per Arrow entry, values; the Parquet storage follows from the precision.
    const Case = struct { format: [*:0]const u8, name: [*:0]const u8, width: usize, values: []const ?i256, physical: format.PhysicalType, type_length: ?i32, read_format: []const u8, read_width: usize };
    const cases = [_]Case{
        .{ .format = "d:9,2", .name = "d9", .width = 16, .values = &d9, .physical = .int32, .type_length = null, .read_format = "d:9,2", .read_width = 16 },
        .{ .format = "d:18,4,128", .name = "d18", .width = 16, .values = &d18, .physical = .int64, .type_length = null, .read_format = "d:18,4", .read_width = 16 },
        .{ .format = "d:30,2", .name = "d30", .width = 16, .values = &d30, .physical = .fixed_len_byte_array, .type_length = 13, .read_format = "d:30,2", .read_width = 16 },
        .{ .format = "d:50,2,256", .name = "d50", .width = 32, .values = &d50, .physical = .fixed_len_byte_array, .type_length = 21, .read_format = "d:50,2,256", .read_width = 32 },
        .{ .format = "d:9,2,32", .name = "d9_32", .width = 4, .values = &d9_32, .physical = .int32, .type_length = null, .read_format = "d:9,2", .read_width = 16 },
    };

    var arrays: [cases.len]ArrowArray = undefined;
    var schemas: [cases.len]ArrowSchema = undefined;
    var schema_ptrs: [cases.len]*ArrowSchema = undefined;
    for (cases, 0..) |c, i| {
        arrays[i] = try testDecimalArray(arena, c.width, c.values);
        schemas[i] = testLeafSchema(c.format, c.name);
        schema_ptrs[i] = &schemas[i];
    }
    const root = ArrowSchema{
        .format = "+s", .name = "", .metadata = null, .flags = 0, .n_children = cases.len,
        .children = @ptrCast(&schema_ptrs), .dictionary = null, .release = null, .private_data = null,
    };
    const col_defs = try importSchemaFromArrow(allocator, &root);
    defer freeImportedColumnDefs(allocator, col_defs);
    for (cases, col_defs) |c, def| {
        try std.testing.expectEqual(c.physical, def.type_);
        try std.testing.expectEqual(c.type_length, def.type_length);
    }

    var writer = try api_writer_mod.writeToBuffer(allocator, col_defs);
    try writeRowGroupFromArrow(&writer, allocator, &arrays, &schemas);
    try writer.close();
    const buf = try writer.toOwnedSlice();
    defer allocator.free(buf);
    writer.deinit();

    var dr = try api_reader_mod.openBufferDynamic(allocator, buf, .{});
    defer dr.deinit();
    var result = try readColumns(allocator, dr.getSource(), dr.metadata, 0, .{});
    defer result.deinit();
    for (cases, 0..) |c, i| {
        try std.testing.expectEqualStrings(c.read_format, std.mem.span(result.schema.children.?[i].format));
        try expectDecimalArray(&result.arrays[i], c.read_width, c.values);
    }
}

test "round-trip: arrow write then read - decimals in a list and a struct" {
    const allocator = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // list<decimal(18,2)>: [[1.50, null], null, [], [-3.25]]
    const elements = [_]?i256{ 150, null, -325 };
    var element_arr = try testDecimalArray(arena, 16, &elements);
    var element_sch = testLeafSchema("d:18,2", "element");
    var list_offsets = [_]i32{ 0, 2, 2, 2, 3 };
    var list_validity = [_]u8{0b1101};
    var list_buffers = [_]?*anyopaque{ @ptrCast(&list_validity), @ptrCast(&list_offsets) };
    var list_children = [_]*ArrowArray{&element_arr};
    var list_child_schemas = [_]*ArrowSchema{&element_sch};
    const list_arr = ArrowArray{
        .length = 4, .null_count = 1, .offset = 0, .n_buffers = 2, .n_children = 1,
        .buffers = &list_buffers, .children = &list_children, .dictionary = null, .release = null, .private_data = null,
    };
    const list_sch = ArrowSchema{
        .format = "+l", .name = "prices", .metadata = null, .flags = arrow.ARROW_FLAG_NULLABLE, .n_children = 1,
        .children = &list_child_schemas, .dictionary = null, .release = null, .private_data = null,
    };

    // struct<a: decimal(30,2)>: [{a: 1.00}, {a: null}, null, {a: -10^29}]
    const big: i256 = -std.math.pow(i256, 10, 29);
    const fields = [_]?i256{ 100, null, 0, big };
    var field_arr = try testDecimalArray(arena, 16, &fields);
    var field_sch = testLeafSchema("d:30,2", "a");
    var struct_validity = [_]u8{0b1011};
    var struct_buffers = [_]?*anyopaque{@ptrCast(&struct_validity)};
    var struct_children = [_]*ArrowArray{&field_arr};
    var struct_child_schemas = [_]*ArrowSchema{&field_sch};
    const struct_arr = ArrowArray{
        .length = 4, .null_count = 1, .offset = 0, .n_buffers = 1, .n_children = 1,
        .buffers = &struct_buffers, .children = &struct_children, .dictionary = null, .release = null, .private_data = null,
    };
    const struct_sch = ArrowSchema{
        .format = "+s", .name = "point", .metadata = null, .flags = arrow.ARROW_FLAG_NULLABLE, .n_children = 1,
        .children = &struct_child_schemas, .dictionary = null, .release = null, .private_data = null,
    };

    var schemas = [_]ArrowSchema{ list_sch, struct_sch };
    var schema_ptrs = [_]*ArrowSchema{ &schemas[0], &schemas[1] };
    const root = ArrowSchema{
        .format = "+s", .name = "", .metadata = null, .flags = 0, .n_children = 2,
        .children = &schema_ptrs, .dictionary = null, .release = null, .private_data = null,
    };
    const col_defs = try importSchemaFromArrow(allocator, &root);
    defer freeImportedColumnDefs(allocator, col_defs);

    var writer = try api_writer_mod.writeToBuffer(allocator, col_defs);
    try writeRowGroupFromArrow(&writer, allocator, &.{ list_arr, struct_arr }, &schemas);
    try writer.close();
    const buf = try writer.toOwnedSlice();
    defer allocator.free(buf);
    writer.deinit();

    var dr = try api_reader_mod.openBufferDynamic(allocator, buf, .{});
    defer dr.deinit();
    var result = try readColumns(allocator, dr.getSource(), dr.metadata, 0, .{});
    defer result.deinit();

    const list = &result.arrays[0];
    try std.testing.expectEqual(@as(i64, 1), list.null_count);
    const read_offsets: [*]const i32 = @ptrCast(@alignCast(list.buffers[1].?));
    try std.testing.expectEqualSlices(i32, &list_offsets, read_offsets[0..5]);
    try std.testing.expectEqualStrings("d:18,2", std.mem.span(result.schema.children.?[0].children.?[0].format));
    try expectDecimalArray(list.children.?[0], 16, &elements);

    const st = &result.arrays[1];
    try std.testing.expectEqual(@as(i64, 1), st.null_count);
    try std.testing.expectEqualStrings("d:30,2", std.mem.span(result.schema.children.?[1].children.?[0].format));
    // Struct slot 2 is null; its field reads back as null.
    try expectDecimalArray(st.children.?[0], 16, &.{ 100, null, null, big });
}

test "decimal files read as Arrow decimal128" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    // parquet-testing files holding 1.00, 2.00, ... in INT32, INT64, FIXED_LEN_BYTE_ARRAY
    // and BYTE_ARRAY decimals.
    const files = [_][]const u8{ "int32_decimal.parquet", "int64_decimal.parquet", "fixed_length_decimal.parquet", "fixed_length_decimal_legacy.parquet", "byte_array_decimal.parquet" };
    var dir = std.Io.Dir.cwd().openDir(io, "../test-files-wild/parquet-testing/data", .{}) catch return error.SkipZigTest;
    defer dir.close(io);
    for (files) |name| {
        const file = try dir.openFile(io, name, .{});
        defer file.close(io);
        var reader = try @import("../lib.zig").openFileDynamic(allocator, file, io, .{});
        defer reader.deinit();
        var result = try readColumns(allocator, reader.getSource(), reader.metadata, 0, .{});
        defer result.deinit();
        const fmt = std.mem.span(result.schema.children.?[0].format);
        try std.testing.expect(std.mem.startsWith(u8, fmt, "d:") and std.mem.count(u8, fmt, ",") == 1);
        const arr = &result.arrays[0];
        try std.testing.expect(arr.length >= 4);
        const data: [*]const u8 = @ptrCast(arr.buffers[1].?);
        for (0..@intCast(arr.length)) |i| {
            std.testing.expectEqual(@as(i256, @intCast((i + 1) * 100)), arrowDecimalAt(data, 16, i)) catch |err| {
                std.debug.print("{s} entry {d}\n", .{ name, i });
                return err;
            };
        }
    }
}

// ============================================================================
// Arrow write round-trip tests: date64
// ============================================================================

test "round-trip: arrow write then read - date64" {
    const allocator = std.testing.allocator;
    const n: usize = 3;
    const bitmap_len: usize = 1;

    const validity = try allocator.alloc(u8, bitmap_len);
    defer allocator.free(validity);
    @memset(validity, 0xFF);
    arrow.clearBit(validity, 1);

    const data_mem = try allocator.alloc(u8, n * 8);
    defer allocator.free(data_mem);
    const typed: [*]i64 = @ptrCast(@alignCast(data_mem.ptr));
    typed[0] = 1705363200000; // 2024-01-15 in millis = day 19738
    typed[1] = 0; // null
    typed[2] = 0; // epoch = day 0

    var buffers = [_]?*anyopaque{ @ptrCast(validity.ptr), @ptrCast(data_mem.ptr) };
    const d64_arr = ArrowArray{
        .length = 3, .null_count = 1, .offset = 0, .n_buffers = 2,
        .n_children = 0, .buffers = &buffers, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };
    const d64_sch = ArrowSchema{
        .format = "tdm", .name = "date_col", .metadata = null,
        .flags = arrow.ARROW_FLAG_NULLABLE, .n_children = 0, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };

    const col_defs = [_]ColumnDef{
        .{ .name = "date_col", .type_ = .int32, .optional = true, .logical_type = .date },
    };

    var writer = try api_writer_mod.writeToBuffer(allocator, &col_defs);
    const arrays = [_]ArrowArray{d64_arr};
    const schemas = [_]ArrowSchema{d64_sch};
    try writeRowGroupFromArrow(&writer, allocator, &arrays, &schemas);
    try writer.close();
    const buf = try writer.toOwnedSlice();
    defer allocator.free(buf);
    writer.deinit();

    var dr = try api_reader_mod.openBufferDynamic(allocator, buf, .{});
    defer dr.deinit();
    var result = try readColumns(allocator, dr.getSource(), dr.metadata, 0, .{});
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.arrays.len);
    const read_arr = &result.arrays[0];
    try std.testing.expectEqual(@as(i64, 3), read_arr.length);
    try std.testing.expectEqual(@as(i64, 1), read_arr.null_count);

    const read_data: [*]const i32 = @ptrCast(@alignCast(read_arr.buffers[1].?));
    try std.testing.expectEqual(@as(i32, 19738), read_data[0]); // 1705276800000 / 86400000
    try std.testing.expectEqual(@as(i32, 0), read_data[2]); // epoch
}

// ============================================================================
// Arrow write round-trip tests: time32
// ============================================================================

test "round-trip: arrow write then read - time32 seconds and millis" {
    const allocator = std.testing.allocator;
    const n: usize = 3;
    const bitmap_len: usize = 1;

    // Time32 seconds (tts -> stored as millis)
    const tts_validity = try allocator.alloc(u8, bitmap_len);
    defer allocator.free(tts_validity);
    @memset(tts_validity, 0xFF);

    const tts_data = try allocator.alloc(u8, n * 4);
    defer allocator.free(tts_data);
    const tts_typed: [*]i32 = @ptrCast(@alignCast(tts_data.ptr));
    tts_typed[0] = 3600; // 1 hour in seconds
    tts_typed[1] = 0; // midnight
    tts_typed[2] = 43200; // noon

    var tts_buffers = [_]?*anyopaque{ @ptrCast(tts_validity.ptr), @ptrCast(tts_data.ptr) };
    const tts_arr = ArrowArray{
        .length = 3, .null_count = 0, .offset = 0, .n_buffers = 2,
        .n_children = 0, .buffers = &tts_buffers, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };
    const tts_sch = ArrowSchema{
        .format = "tts", .name = "time_sec", .metadata = null,
        .flags = arrow.ARROW_FLAG_NULLABLE, .n_children = 0, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };

    // Time32 millis (ttm -> passthrough)
    const ttm_validity = try allocator.alloc(u8, bitmap_len);
    defer allocator.free(ttm_validity);
    @memset(ttm_validity, 0xFF);
    arrow.clearBit(ttm_validity, 2);

    const ttm_data = try allocator.alloc(u8, n * 4);
    defer allocator.free(ttm_data);
    const ttm_typed: [*]i32 = @ptrCast(@alignCast(ttm_data.ptr));
    ttm_typed[0] = 3600000; // 1 hour in millis
    ttm_typed[1] = 0; // midnight
    ttm_typed[2] = 0; // null

    var ttm_buffers = [_]?*anyopaque{ @ptrCast(ttm_validity.ptr), @ptrCast(ttm_data.ptr) };
    const ttm_arr = ArrowArray{
        .length = 3, .null_count = 1, .offset = 0, .n_buffers = 2,
        .n_children = 0, .buffers = &ttm_buffers, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };
    const ttm_sch = ArrowSchema{
        .format = "ttm", .name = "time_ms", .metadata = null,
        .flags = arrow.ARROW_FLAG_NULLABLE, .n_children = 0, .children = null,
        .dictionary = null, .release = null, .private_data = null,
    };

    const col_defs = [_]ColumnDef{
        .{ .name = "time_sec", .type_ = .int32, .optional = true, .logical_type = .{ .time = .{ .is_adjusted_to_utc = false, .unit = .millis } } },
        .{ .name = "time_ms", .type_ = .int32, .optional = true, .logical_type = .{ .time = .{ .is_adjusted_to_utc = false, .unit = .millis } } },
    };

    var writer = try api_writer_mod.writeToBuffer(allocator, &col_defs);
    const arrays = [_]ArrowArray{ tts_arr, ttm_arr };
    const schemas = [_]ArrowSchema{ tts_sch, ttm_sch };
    try writeRowGroupFromArrow(&writer, allocator, &arrays, &schemas);
    try writer.close();
    const buf = try writer.toOwnedSlice();
    defer allocator.free(buf);
    writer.deinit();

    var dr = try api_reader_mod.openBufferDynamic(allocator, buf, .{});
    defer dr.deinit();
    var result = try readColumns(allocator, dr.getSource(), dr.metadata, 0, .{});
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.arrays.len);

    // Time32 seconds -> millis (* 1000)
    const read_tts: [*]const i32 = @ptrCast(@alignCast(result.arrays[0].buffers[1].?));
    try std.testing.expectEqual(@as(i32, 3600000), read_tts[0]); // 3600 * 1000
    try std.testing.expectEqual(@as(i32, 0), read_tts[1]);
    try std.testing.expectEqual(@as(i32, 43200000), read_tts[2]); // 43200 * 1000

    // Time32 millis passthrough
    const read_ttm: [*]const i32 = @ptrCast(@alignCast(result.arrays[1].buffers[1].?));
    try std.testing.expectEqual(@as(i32, 3600000), read_ttm[0]);
    try std.testing.expectEqual(@as(i32, 0), read_ttm[1]);
    try std.testing.expectEqual(@as(i64, 1), result.arrays[1].null_count);
}

// ============================================================================
// Nested columns compared with PyArrow
// ============================================================================

const NestedTotals = struct {
    files: usize = 0,
    pyarrow_errors: usize = 0,
    columns: usize = 0,
    equal: usize = 0,
    differ: usize = 0,
    read_errors: usize = 0,
};

// test-files-arrow/nested_reference.json holds PyArrow's reading of every column of the test
// files that have a list, map or struct column (first 2,000 rows per row group), as canonical
// text; see test-files-arrow/nested_reference.py. generate.py writes it.
test "Arrow arrays match PyArrow on the test files with nested columns" {
    const io = std.testing.io;
    var arena_state = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const json_text = std.Io.Dir.cwd().readFileAlloc(io, "../test-files-arrow/nested_reference.json", arena, .limited(1 << 30)) catch |err| {
        std.debug.print("cannot read ../test-files-arrow/nested_reference.json ({s}); regenerate the test files: cd test-files-arrow && uv run python generate.py\n", .{@errorName(err)});
        return err;
    };
    const reference = try std.json.parseFromSliceLeaky(std.json.Value, arena, json_text, .{});

    const totals = try compareAllFiles(reference.object, null, false, null, 0);
    std.debug.print("\nPyArrow comparison: {d} files ({d} more unreadable by PyArrow), {d} columns: {d} equal, {d} differ, {d} read errors\n", .{
        totals.files, totals.pyarrow_errors, totals.columns, totals.equal, totals.differ, totals.read_errors,
    });
    try std.testing.expectEqual(@as(usize, 0), totals.differ);
    try std.testing.expectEqual(@as(usize, 0), totals.read_errors);
}

test "Arrow batches match PyArrow on the test files with nested columns" {
    const io = std.testing.io;
    var arena_state = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const json_text = try std.Io.Dir.cwd().readFileAlloc(io, "../test-files-arrow/nested_reference.json", arena, .limited(1 << 30));
    const reference = try std.json.parseFromSliceLeaky(std.json.Value, arena, json_text, .{});

    // 7 rows per batch cuts inside pages; 1000 cuts mostly at or near page ends. The second
    // pass reads byte arrays with i64 offsets.
    for ([_]usize{ 7, 1000 }, [_]bool{ false, true }) |batch_rows, large| {
        const totals = try compareAllFiles(reference.object, batch_rows, large, null, 0);
        std.debug.print("\nPyArrow comparison, {d} rows per batch{s}: {d} files, {d} columns: {d} equal, {d} differ, {d} read errors\n", .{
            batch_rows, if (large) ", i64 offsets" else "", totals.files, totals.columns, totals.equal, totals.differ, totals.read_errors,
        });
        try std.testing.expectEqual(@as(usize, 0), totals.differ);
        try std.testing.expectEqual(@as(usize, 0), totals.read_errors);
    }
}

test "Arrow arrays and batches match PyArrow when leaves are decoded on 4 threads" {
    const io = std.testing.io;
    var arena_state = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const json_text = try std.Io.Dir.cwd().readFileAlloc(io, "../test-files-arrow/nested_reference.json", arena, .limited(1 << 30));
    const reference = try std.json.parseFromSliceLeaky(std.json.Value, arena, json_text, .{});

    var threaded: std.Io.Threaded = .init(std.testing.allocator, .{ .async_limit = .limited(3) });
    defer threaded.deinit();

    // Whole row groups, then batches of 7 rows; each without and with read-ahead (one row
    // group per read here, so the batches take rows from the one decoded row group).
    for ([_]?usize{ null, 7 }) |batch_rows| for ([_]usize{ 0, 1 }) |read_ahead| {
        const totals = try compareAllFiles(reference.object, batch_rows, false, threaded.io(), read_ahead);
        std.debug.print("\nPyArrow comparison, 4 threads, {s}{s}: {d} files, {d} columns: {d} equal, {d} differ, {d} read errors\n", .{
            if (batch_rows == null) "row groups" else "7 rows per batch", if (read_ahead > 0) ", read-ahead" else "", totals.files, totals.columns, totals.equal, totals.differ, totals.read_errors,
        });
        try std.testing.expectEqual(@as(usize, 0), totals.differ);
        try std.testing.expectEqual(@as(usize, 0), totals.read_errors);
    };
}

test "Arrow batches spanning row groups equal row group reads on the test files" {
    const io = std.testing.io;
    const allocator = std.testing.allocator;
    var threaded: std.Io.Threaded = .init(allocator, .{ .async_limit = .limited(3) });
    defer threaded.deinit();
    var totals: SpanTotals = .{};
    for ([_][]const u8{ "../test-files-arrow", "../test-files-wild" }) |root| {
        var dir = std.Io.Dir.cwd().openDir(io, root, .{ .iterate = true }) catch continue;
        defer dir.close(io);
        var walker = try dir.walk(allocator);
        defer walker.deinit();
        while (try walker.next(io)) |entry| {
            if (entry.kind != .file or !std.mem.endsWith(u8, entry.basename, ".parquet")) continue;
            if (std.mem.indexOf(u8, entry.path, ".venv") != null) continue;
            try compareSpanningBatches(entry.dir, entry.basename, &totals, threaded.io());
        }
    }
    std.debug.print("\nbatches spanning row groups: {d} files with several row groups, {d} columns equal to row group reads ({d} columns with row group read errors skipped)\n", .{ totals.files, totals.columns, totals.skipped });
    try std.testing.expect(totals.columns > 0);
}

const SpanTotals = struct {
    files: usize = 0,
    columns: usize = 0,
    skipped: usize = 0,
};

/// Reads each column of a file with several row groups row group by row group and in batches
/// of 7 and 1000 rows, which span row groups, without and with read-ahead on `ahead_io`, and
/// checks that the first rows agree. Only columns whose row groups all read without error are
/// compared.
fn compareSpanningBatches(dir: std.Io.Dir, basename: []const u8, totals: *SpanTotals, ahead_io: std.Io) !void {
    const io = std.testing.io;
    // Rows compared per column, from the start of the file.
    const max_rows = 3000;
    var arena_state = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const file = dir.openFile(io, basename, .{}) catch return;
    defer file.close(io);
    var reader = @import("../lib.zig").openFileDynamic(arena, file, io, .{}) catch return;
    defer reader.deinit();
    const md = reader.metadata;
    if (md.row_groups.len < 2) return;
    const tree = buildSchemaTree(arena, md.schema) catch return;
    totals.files += 1;

    for (tree, 0..) |*node, c| {
        const compared = expectSpanningBatchesMatch(reader.getSource(), md, node, c, max_rows, &.{ 7, 1000 }, ahead_io) catch |err| {
            std.debug.print("spanning batches: {s} column {d}: {s}\n", .{ basename, c, @errorName(err) });
            return err;
        };
        if (compared) totals.columns += 1 else totals.skipped += 1;
    }
}

/// Reads the first `max_rows` rows of column `c` row group by row group, and in batches of
/// each size in `batch_sizes` (which span row groups), and checks that they agree. Returns
/// false, comparing nothing, when a row group read fails.
fn expectSpanningBatchesMatch(source: SeekableReader, md: format.FileMetaData, node: *const Node, c: usize, max_rows: usize, batch_sizes: []const usize, ahead_io: ?std.Io) !bool {
    const allocator = std.testing.allocator;
    var expected: std.Io.Writer.Allocating = .init(allocator);
    defer expected.deinit();
    var written: usize = 0;
    for (0..md.row_groups.len) |rg| {
        if (written == max_rows) break;
        var result = readColumns(allocator, source, md, rg, .{ .columns = &.{c} }) catch return false;
        defer result.deinit();
        const arr = &result.arrays[0];
        const rows = @min(@as(usize, @intCast(arr.length)), max_rows - written);
        for (0..rows) |i| {
            try canonEntry(&expected.writer, md.schema, node, arr, i, false);
            try expected.writer.writeByte('\n');
        }
        written += rows;
    }

    // Each batch size without read-ahead, then, with `ahead_io`, with 1 and 3 row groups
    // decoded ahead; and with `ahead_io`, one batch per row group with 2 decoded ahead.
    const reads_per_size: usize = if (ahead_io == null) 1 else 3;
    const reads = batch_sizes.len * reads_per_size + @intFromBool(ahead_io != null);
    for (0..reads) |r| {
        const batch_rows: ?usize = if (r < batch_sizes.len * reads_per_size) batch_sizes[r / reads_per_size] else null;
        const read_ahead: usize = if (batch_rows == null) 2 else ([_]usize{ 0, 1, 3 })[r % reads_per_size];
        var got: std.Io.Writer.Allocating = .init(allocator);
        defer got.deinit();
        var batches = try ArrowBatchReader.init(allocator, source, md, .{
            .columns = &.{c},
            .batch_rows = batch_rows,
            .io = if (read_ahead > 0) ahead_io else null,
            .read_ahead = read_ahead,
        });
        defer batches.deinit();
        var got_rows: usize = 0;
        while (got_rows < written) {
            var result = (try batches.next()) orelse break;
            defer result.deinit();
            const arr = &result.arrays[0];
            const len: usize = @intCast(arr.length);
            // Every batch but the last has batch_rows rows.
            if (batch_rows) |b| if (got_rows + len < written) try std.testing.expectEqual(b, len);
            const rows = @min(len, written - got_rows);
            for (0..rows) |i| {
                try canonEntry(&got.writer, md.schema, node, arr, i, false);
                try got.writer.writeByte('\n');
            }
            got_rows += rows;
        }
        std.testing.expectEqualStrings(expected.written(), got.written()) catch |err| {
            std.debug.print("{?d} rows per batch, {d} row groups ahead, differ from row group reads\n", .{ batch_rows, read_ahead });
            return err;
        };
    }
    return true;
}

test "with read-ahead, an error in a row group is returned by the first batch that takes rows from it" {
    const allocator = std.testing.allocator;

    // 30 rows in row groups of 10; the page header of row group 1 is overwritten.
    var writer = try api_writer_mod.createBufferDynamic(allocator);
    defer writer.deinit();
    try writer.addColumn("x", @import("dynamic_writer.zig").TypeInfo.int64, .{});
    writer.setRowGroupSize(10);
    try writer.begin();
    for (0..30) |i| {
        try writer.setInt64(0, @intCast(i));
        try writer.addRow();
    }
    try writer.close();
    const buf = try writer.toOwnedSlice();
    defer allocator.free(buf);
    {
        var dr = try api_reader_mod.openBufferDynamic(allocator, buf, .{});
        defer dr.deinit();
        try std.testing.expectEqual(@as(usize, 3), dr.metadata.row_groups.len);
        const offset: usize = @intCast(dr.metadata.row_groups[1].columns[0].meta_data.?.data_page_offset);
        @memset(buf[offset..][0..16], 0xff);
    }
    var dr = try api_reader_mod.openBufferDynamic(allocator, buf, .{});
    defer dr.deinit();

    var threaded: std.Io.Threaded = .init(allocator, .{ .async_limit = .limited(3) });
    defer threaded.deinit();

    // Batches of 4: rows 0-3 and 4-7, then rows 8-11 need row group 1. One batch per row
    // group: row group 0, then row group 1. The same holds without read-ahead. Row group 2
    // may still be decoding when `deinit` runs.
    for ([_]?usize{ 4, null }) |batch_rows| for ([_]usize{ 0, 1, 2 }) |read_ahead| {
        var batches = try ArrowBatchReader.init(allocator, dr.getSource(), dr.metadata, .{
            .batch_rows = batch_rows,
            .io = threaded.io(),
            .read_ahead = read_ahead,
        });
        defer batches.deinit();
        var first: i64 = 0;
        const ok_batches: usize = if (batch_rows == null) 1 else 2;
        for (0..ok_batches) |_| {
            var result = (try batches.next()).?;
            defer result.deinit();
            const values: [*]const i64 = @ptrCast(@alignCast(result.arrays[0].buffers[1].?));
            try std.testing.expectEqual(first, values[0]);
            first += result.arrays[0].length;
        }
        if (batches.next()) |result| {
            var r = result;
            if (r) |*b| b.deinit();
            return error.TestUnexpectedResult;
        } else |_| {}
    };

    try std.testing.expectError(error.InvalidArgument, ArrowBatchReader.init(allocator, dr.getSource(), dr.metadata, .{ .read_ahead = 1 }));
}

test "batches of list and map columns span row groups" {
    const allocator = std.testing.allocator;

    // 50 rows in row groups of 10. scores: optional list<int32>, null every 7th row and
    // empty every 5th; attrs: map<string, int32> with 0 to 3 entries.
    var writer = try api_writer_mod.createBufferDynamic(allocator);
    defer writer.deinit();
    const element = try writer.allocSchemaNode(.{ .int32 = .{} });
    const list = try writer.allocSchemaNode(.{ .list = element });
    const scores = try writer.allocSchemaNode(.{ .optional = list });
    try writer.addColumnNested("scores", scores, .{});
    const key = try writer.allocSchemaNode(.{ .byte_array = .{ .logical = .string } });
    const value = try writer.allocSchemaNode(.{ .int32 = .{} });
    const map = try writer.allocSchemaNode(.{ .map = .{ .key = key, .value = value } });
    try writer.addColumnNested("attrs", map, .{});
    writer.setRowGroupSize(10);
    try writer.begin();
    var key_buf: [16]u8 = undefined;
    for (0..50) |i| {
        if (i % 7 == 3) {
            try writer.setNull(0);
        } else {
            try writer.beginList(0);
            if (i % 5 != 0) for (0..i % 4 + 1) |j| try writer.appendNestedValue(0, .{ .int32_val = @intCast(i * 10 + j) });
            try writer.endList(0);
        }
        try writer.beginMap(1);
        for (0..i % 4) |j| {
            try writer.beginMapEntry(1);
            try writer.appendNestedBytes(1, try std.fmt.bufPrint(&key_buf, "k{d}_{d}", .{ i, j }));
            try writer.appendNestedValue(1, .{ .int32_val = @intCast(j) });
            try writer.endMapEntry(1);
        }
        try writer.endMap(1);
        try writer.addRow();
    }
    try writer.close();
    const buf = try writer.toOwnedSlice();
    defer allocator.free(buf);

    var dr = try api_reader_mod.openBufferDynamic(allocator, buf, .{});
    defer dr.deinit();
    try std.testing.expectEqual(@as(usize, 5), dr.metadata.row_groups.len);
    var threaded: std.Io.Threaded = .init(allocator, .{ .async_limit = .limited(3) });
    defer threaded.deinit();
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const tree = try buildSchemaTree(arena.allocator(), dr.metadata.schema);
    for (tree, 0..) |*node, c| {
        try std.testing.expect(try expectSpanningBatchesMatch(dr.getSource(), dr.metadata, node, c, 50, &.{ 1, 3, 7, 13, 50 }, threaded.io()));
    }
}

/// Writes the first `max_rows` rows of column `c` of row group `rg`, one per line, read
/// whole (`batch_rows` null) or through an `ArrowBatchReader`.
fn canonColumn(
    allocator: Allocator,
    w: *std.Io.Writer,
    source: SeekableReader,
    md: format.FileMetaData,
    node: *const Node,
    rg: usize,
    c: usize,
    batch_rows: ?usize,
    large: bool,
    max_rows: usize,
    decode_io: ?std.Io,
    read_ahead: usize,
) !void {
    if (batch_rows == null and read_ahead == 0) {
        var result = try readColumns(allocator, source, md, rg, .{ .columns = &.{c}, .io = decode_io });
        defer result.deinit();
        const arr = &result.arrays[0];
        const rows = @min(@as(usize, @intCast(arr.length)), max_rows);
        for (0..rows) |i| {
            if (i > 0) try w.writeByte('\n');
            try canonEntry(w, md.schema, node, arr, i, false);
        }
        return;
    }
    var batches = try ArrowBatchReader.init(allocator, source, md, .{ .columns = &.{c}, .row_groups = &.{rg}, .batch_rows = batch_rows, .large_offsets = large, .io = decode_io, .read_ahead = read_ahead });
    defer batches.deinit();
    var written: usize = 0;
    while (written < max_rows) {
        var result = (try batches.next()) orelse break;
        defer result.deinit();
        const arr = &result.arrays[0];
        if (batch_rows) |b| if (arr.length > b) return error.TestUnexpectedResult;
        const rows = @min(@as(usize, @intCast(arr.length)), max_rows - written);
        for (0..rows) |i| {
            if (written > 0) try w.writeByte('\n');
            try canonEntry(w, md.schema, node, arr, i, large);
            written += 1;
        }
    }
}

/// Compares every file of `reference` with PyArrow's reading of it (`compareFileWithReference`).
/// The files are compared as tasks of `std.testing.io`, so several at a time; leaks fail the
/// comparison of the file that leaked.
const test_util = @import("test_util.zig");

fn compareAllFiles(reference: std.json.ObjectMap, batch_rows: ?usize, large: bool, decode_io: ?std.Io, read_ahead: usize) !NestedTotals {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const FileJob = struct {
        path: []const u8,
        columns: std.json.ObjectMap,
        totals: NestedTotals = .{},
        err: ?anyerror = null,

        /// Uses an allocator of its own, so that jobs on different threads do not wait on
        /// one allocator's lock; it fails the job when memory leaks. The allocator records
        /// no stack traces: on macOS, capturing one per allocation made the 7-row batch pass
        /// take about 90 s for each of the largest files.
        fn run(job: *@This(), rows: ?usize, large_offsets: bool, dio: ?std.Io, ahead: usize) void {
            var gpa: test_util.LeakCheckAllocator = .init;
            compareFileWithReference(gpa.allocator(), job.path, job.columns, &job.totals, rows, large_offsets, dio, ahead) catch |e| {
                job.err = e;
            };
            if (gpa.deinit() == .leak and job.err == null) job.err = error.MemoryLeak;
        }
    };

    var totals: NestedTotals = .{};
    var jobs: std.ArrayList(FileJob) = .empty;
    defer jobs.deinit(allocator);
    var it = reference.iterator();
    while (it.next()) |entry| {
        const columns = entry.value_ptr.*.object;
        if (columns.get("error") != null) {
            totals.pyarrow_errors += 1;
            continue;
        }
        try jobs.append(allocator, .{ .path = entry.key_ptr.*, .columns = columns });
    }
    var group: std.Io.Group = .init;
    for (jobs.items) |*job| group.async(io, FileJob.run, .{ job, batch_rows, large, decode_io, read_ahead });
    try group.await(io);

    totals.files = jobs.items.len;
    for (jobs.items) |job| {
        if (job.err) |e| return e;
        totals.columns += job.totals.columns;
        totals.equal += job.totals.equal;
        totals.differ += job.totals.differ;
        totals.read_errors += job.totals.read_errors;
    }
    return totals;
}

fn compareFileWithReference(allocator: Allocator, path: []const u8, reference: std.json.ObjectMap, totals: *NestedTotals, batch_rows: ?usize, large: bool, decode_io: ?std.Io, read_ahead: usize) !void {
    const io = std.testing.io;
    // The reader uses an arena (footer parsing leaks on some malformed files); the Arrow read
    // uses `allocator`.
    var arena_state = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    var reader = @import("../lib.zig").openFileDynamic(arena, file, io, .{}) catch |err| {
        totals.read_errors += 1;
        std.debug.print("READ ERROR {s}: {s}\n", .{ path, @errorName(err) });
        return;
    };
    defer reader.deinit();
    const md = reader.metadata;
    const tree = buildSchemaTree(arena, md.schema) catch |err| {
        totals.read_errors += 1;
        std.debug.print("READ ERROR {s}: schema: {s}\n", .{ path, @errorName(err) });
        return;
    };

    for (0..md.row_groups.len) |rg| {
        for (tree, 0..) |*node, c| {
            var key_buf: [32]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buf, "{d}:{d}", .{ rg, c });
            const expected = (reference.get(key) orelse continue).string;
            totals.columns += 1;

            var out: std.Io.Writer.Allocating = .init(allocator);
            defer out.deinit();
            canonColumn(allocator, &out.writer, reader.getSource(), md, node, rg, c, batch_rows, large, 2000, decode_io, read_ahead) catch |err| {
                totals.read_errors += 1;
                std.debug.print("READ ERROR {s} {s} ({s}): {s}\n", .{ path, key, md.schema[node.schema_idx].name, @errorName(err) });
                continue;
            };
            const got = out.written();
            if (std.mem.eql(u8, got, expected)) {
                totals.equal += 1;
            } else {
                totals.differ += 1;
                var gl = std.mem.splitScalar(u8, got, '\n');
                var el = std.mem.splitScalar(u8, expected, '\n');
                var row: usize = 0;
                while (true) : (row += 1) {
                    const g = gl.next();
                    const e = el.next();
                    if (g == null and e == null) break;
                    if (g == null or e == null or !std.mem.eql(u8, g.?, e.?)) {
                        const gs = g orelse "<none>";
                        const es = e orelse "<none>";
                        const at = std.mem.indexOfDiff(u8, gs, es) orelse 0;
                        std.debug.print("DIFF {s} {s} ({s}) row {d}, from character {d}:\n  zig     {s}\n  pyarrow {s}\n", .{
                            path, key, md.schema[node.schema_idx].name, row, at, window(gs, at), window(es, at),
                        });
                        break;
                    }
                }
            }
        }
    }
}

/// Up to 80 characters before and 120 from `at`.
fn window(s: []const u8, at: usize) []const u8 {
    const start = at -| 80;
    return s[@min(start, s.len)..@min(at + 120, s.len)];
}

/// Writes entry `i` of `arr` (the Arrow array of `node`) in the canonical text of
/// test-files-arrow/nested_reference.py.
/// `large`: BYTE_ARRAY leaves have i64 offsets.
fn canonEntry(w: *std.Io.Writer, schema: []const format.SchemaElement, node: *const Node, arr: *const ArrowArray, i: usize, large: bool) !void {
    if (arr.buffers[0]) |v| {
        const bits: [*]const u8 = @ptrCast(v);
        if ((bits[i / 8] >> @intCast(i % 8)) & 1 == 0) return w.writeAll("n");
    }
    switch (node.kind) {
        .leaf => try canonLeaf(w, schema[node.schema_idx], arr, i, large),
        .struct_ => {
            try w.writeByte('{');
            for (node.children, 0..) |*child, k| {
                if (k > 0) try w.writeByte(',');
                try canonEntry(w, schema, child, arr.children.?[k], i, large);
            }
            try w.writeByte('}');
        },
        .list, .map => {
            const offsets: [*]const i32 = @ptrCast(@alignCast(arr.buffers[1].?));
            const child = arr.children.?[0];
            try w.writeByte('[');
            const start: usize = @intCast(offsets[i]);
            const end: usize = @intCast(offsets[i + 1]);
            for (start..end) |j| {
                if (j > start) try w.writeByte(',');
                if (node.kind == .list) {
                    try canonEntry(w, schema, &node.children[0], child, j, large);
                } else {
                    const entries = &node.children[0];
                    try w.writeByte('(');
                    try canonEntry(w, schema, &entries.children[0], child.children.?[0], j, large);
                    try w.writeByte(':');
                    try canonEntry(w, schema, &entries.children[1], child.children.?[1], j, large);
                    try w.writeByte(')');
                }
            }
            try w.writeByte(']');
        },
    }
}

/// How nested_reference.py writes a leaf of this schema element (from PyArrow's type for it).
const LeafCategory = enum { boolean, int, float, double, bytes, decimal, other };

fn leafCategory(elem: format.SchemaElement) LeafCategory {
    if (decimalOf(elem) != null) return .decimal;
    const physical = elem.type_ orelse return .other;
    return switch (physical) {
        .boolean => .boolean,
        .int32, .int64 => blk: {
            if (elem.logical_type) |lt| break :blk if (lt == .int) .int else .other;
            if (elem.converted_type) |ct| break :blk if (ct >= format.ConvertedType.UINT_8 and ct <= format.ConvertedType.INT_64) .int else .other;
            break :blk .int;
        },
        .float => .float,
        .double => .double,
        .byte_array => blk: {
            if (elem.logical_type) |lt| if (lt == .decimal) break :blk .other;
            if (elem.converted_type) |ct| if (ct == format.ConvertedType.DECIMAL) break :blk .other;
            break :blk .bytes;
        },
        .fixed_len_byte_array => blk: {
            if (elem.logical_type) |lt| break :blk if (lt == .uuid) .bytes else .other;
            if (elem.converted_type != null) break :blk .other;
            break :blk .bytes;
        },
        .int96 => .other,
    };
}

fn canonLeaf(w: *std.Io.Writer, elem: format.SchemaElement, arr: *const ArrowArray, i: usize, large: bool) !void {
    const data: [*]const u8 = @ptrCast(arr.buffers[1].?);
    switch (leafCategory(elem)) {
        .boolean => try w.writeAll(if ((data[i / 8] >> @intCast(i % 8)) & 1 == 1) "t" else "f"),
        .int => if (elem.type_.? == .int32)
            try w.print("i{d}", .{std.mem.bytesToValue(i32, data[i * 4 ..][0..4])})
        else
            try w.print("i{d}", .{std.mem.bytesToValue(i64, data[i * 8 ..][0..8])}),
        .float => try writeDouble(w, std.mem.bytesToValue(f32, data[i * 4 ..][0..4])),
        .double => try writeDouble(w, std.mem.bytesToValue(f64, data[i * 8 ..][0..8])),
        .decimal => try w.print("e{d}", .{arrowDecimalAt(data, if (decimalOf(elem).?.precision <= 38) 16 else 32, i)}),
        .bytes => {
            try w.writeByte('x');
            const bytes = if (elem.type_.? == .byte_array) blk: {
                const values: [*]const u8 = @ptrCast(arr.buffers[2].?);
                if (large) {
                    const offsets: [*]const i64 = @ptrCast(@alignCast(data));
                    break :blk values[@intCast(offsets[i])..@intCast(offsets[i + 1])];
                }
                const offsets: [*]const i32 = @ptrCast(@alignCast(data));
                break :blk values[@intCast(offsets[i])..@intCast(offsets[i + 1])];
            } else blk: {
                const width: usize = @intCast(elem.type_length.?);
                break :blk data[i * width ..][0..width];
            };
            for (bytes) |b| try w.print("{x:0>2}", .{b});
        },
        .other => try w.writeByte('v'),
    }
}

fn writeDouble(w: *std.Io.Writer, v: f64) !void {
    try w.writeByte('d');
    for (std.mem.toBytes(std.mem.nativeToLittle(u64, @bitCast(v)))) |b| try w.print("{x:0>2}", .{b});
}

test "validateArrow rejects offsets and lengths outside the array" {
    const ints = [_]i32{ 1, 2, 3 };
    var int_buffers = [_]?*anyopaque{ null, @ptrCast(@constCast(&ints)) };
    var int_sch = std.mem.zeroes(ArrowSchema);
    int_sch.format = "i";
    var arr = std.mem.zeroes(ArrowArray);
    arr.length = 3;
    arr.n_buffers = 2;
    arr.buffers = &int_buffers;
    try validateArrow(&arr, &int_sch, 0, 3, 0);
    arr.offset = -1;
    try std.testing.expectError(error.TypeMismatch, validateArrow(&arr, &int_sch, 0, 3, 0));
    arr.offset = 0;
    arr.n_buffers = 1; // fewer than the 2 buffers of "i"
    try std.testing.expectError(error.TypeMismatch, validateArrow(&arr, &int_sch, 0, 3, 0));

    // Strings with decreasing offsets.
    const offsets = [_]i32{ 0, 5, 2 };
    const data = "hello";
    var str_buffers = [_]?*anyopaque{ null, @ptrCast(@constCast(&offsets)), @ptrCast(@constCast(data.ptr)) };
    var str_sch = std.mem.zeroes(ArrowSchema);
    str_sch.format = "u";
    var str = std.mem.zeroes(ArrowArray);
    str.length = 2;
    str.n_buffers = 3;
    str.buffers = &str_buffers;
    try std.testing.expectError(error.TypeMismatch, validateArrow(&str, &str_sch, 0, 2, 0));

    // A list whose offsets address more entries than its child holds.
    const list_offsets = [_]i32{ 0, 100 };
    var list_buffers = [_]?*anyopaque{ null, @ptrCast(@constCast(&list_offsets)) };
    var child_ptr: *ArrowArray = &arr;
    var child_sch_ptr: *ArrowSchema = &int_sch;
    var list_sch = std.mem.zeroes(ArrowSchema);
    list_sch.format = "+l";
    list_sch.n_children = 1;
    list_sch.children = @ptrCast(&child_sch_ptr);
    var list = std.mem.zeroes(ArrowArray);
    list.length = 1;
    list.n_buffers = 2;
    list.n_children = 1;
    list.buffers = &list_buffers;
    list.children = @ptrCast(&child_ptr);
    try std.testing.expectError(error.TypeMismatch, validateArrow(&list, &list_sch, 0, 1, 0));

    // A fixed-size binary width of -1.
    try std.testing.expectError(error.InvalidSchema, ArrowFormatInfo.parse("w:-1"));
}
