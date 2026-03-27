//! Dynamic Writer
//!
//! Runtime row-oriented API for writing Parquet files without comptime types.
//! Schema is defined at runtime via `addColumn` / `addColumnNested` calls,
//! rows are built with typed setters and nested value builders, and flushed
//! as row groups.
//!
//! Supports all Parquet types and arbitrary nesting depth (lists, maps, structs).
//!
//! ## Example
//! ```zig
//! const parquet = @import("parquet");
//!
//! var writer = try parquet.createFileDynamic(allocator, file, .{});
//! defer writer.deinit();
//!
//! try writer.addColumn("id", .int32, .{});
//! try writer.addColumn("name", .string, .{});
//! try writer.begin();
//!
//! try writer.setInt32(0, 42);
//! try writer.setBytes(1, "Alice");
//! try writer.addRow();
//!
//! try writer.close();
//! ```

const std = @import("std");
const format = @import("format.zig");
const column_writer = @import("column_writer.zig");
const thrift = @import("thrift/mod.zig");
const types = @import("types.zig");
const nested_mod = @import("nested.zig");
const schema_mod = @import("schema.zig");
const safe = @import("safe.zig");
const write_target = @import("write_target.zig");
const value_mod = @import("value.zig");
const core_writer = @import("writer.zig");
const parquet_reader = @import("parquet_reader.zig");

const WriteTarget = write_target.WriteTarget;
const WriteTargetWriter = write_target.WriteTargetWriter;
const SchemaNode = schema_mod.SchemaNode;
const Value = value_mod.Value;
const Allocator = std.mem.Allocator;
const BackendCleanup = parquet_reader.BackendCleanup;

pub const ColumnType = enum {
    bool_,
    int32,
    int64,
    float_,
    double_,
    bytes,
    fixed_bytes,
    nested,

    fn toPhysicalType(self: ColumnType) ?format.PhysicalType {
        return switch (self) {
            .bool_ => .boolean,
            .int32 => .int32,
            .int64 => .int64,
            .float_ => .float,
            .double_ => .double,
            .bytes => .byte_array,
            .fixed_bytes => .fixed_len_byte_array,
            .nested => null,
        };
    }
};

pub const TypeInfo = struct {
    physical: ColumnType,
    logical: ?format.LogicalType,
    type_length: ?i32,
    required: bool = false,

    pub const bool_ = TypeInfo{ .physical = .bool_, .logical = null, .type_length = null };
    pub const int32 = TypeInfo{ .physical = .int32, .logical = null, .type_length = null };
    pub const int64 = TypeInfo{ .physical = .int64, .logical = null, .type_length = null };
    pub const float_ = TypeInfo{ .physical = .float_, .logical = null, .type_length = null };
    pub const double_ = TypeInfo{ .physical = .double_, .logical = null, .type_length = null };
    pub const bytes = TypeInfo{ .physical = .bytes, .logical = null, .type_length = null };
    pub const string = TypeInfo{ .physical = .bytes, .logical = .string, .type_length = null };
    pub const date = TypeInfo{ .physical = .int32, .logical = .date, .type_length = null };
    pub const uuid = TypeInfo{ .physical = .fixed_bytes, .logical = .uuid, .type_length = 16 };
    pub const json = TypeInfo{ .physical = .bytes, .logical = .json, .type_length = null };
    pub const enum_ = TypeInfo{ .physical = .bytes, .logical = .enum_, .type_length = null };
    pub const bson = TypeInfo{ .physical = .bytes, .logical = .bson, .type_length = null };
    pub const float16 = TypeInfo{ .physical = .fixed_bytes, .logical = .float16, .type_length = 2 };
    pub const interval = TypeInfo{ .physical = .fixed_bytes, .logical = null, .type_length = 12 };
    pub const geometry = TypeInfo{ .physical = .bytes, .logical = .{ .geometry = .{} }, .type_length = null };
    pub const geography = TypeInfo{ .physical = .bytes, .logical = .{ .geography = .{} }, .type_length = null };

    pub const timestamp_millis = TypeInfo{ .physical = .int64, .logical = .{ .timestamp = .{ .is_adjusted_to_utc = true, .unit = .millis } }, .type_length = null };
    pub const timestamp_micros = TypeInfo{ .physical = .int64, .logical = .{ .timestamp = .{ .is_adjusted_to_utc = true, .unit = .micros } }, .type_length = null };
    pub const timestamp_nanos = TypeInfo{ .physical = .int64, .logical = .{ .timestamp = .{ .is_adjusted_to_utc = true, .unit = .nanos } }, .type_length = null };
    pub const time_millis = TypeInfo{ .physical = .int32, .logical = .{ .time = .{ .is_adjusted_to_utc = false, .unit = .millis } }, .type_length = null };
    pub const time_micros = TypeInfo{ .physical = .int64, .logical = .{ .time = .{ .is_adjusted_to_utc = false, .unit = .micros } }, .type_length = null };
    pub const time_nanos = TypeInfo{ .physical = .int64, .logical = .{ .time = .{ .is_adjusted_to_utc = false, .unit = .nanos } }, .type_length = null };

    pub const int8 = TypeInfo{ .physical = .int32, .logical = .{ .int = .{ .bit_width = 8, .is_signed = true } }, .type_length = null };
    pub const int16 = TypeInfo{ .physical = .int32, .logical = .{ .int = .{ .bit_width = 16, .is_signed = true } }, .type_length = null };
    pub const uint8 = TypeInfo{ .physical = .int32, .logical = .{ .int = .{ .bit_width = 8, .is_signed = false } }, .type_length = null };
    pub const uint16 = TypeInfo{ .physical = .int32, .logical = .{ .int = .{ .bit_width = 16, .is_signed = false } }, .type_length = null };
    pub const uint32 = TypeInfo{ .physical = .int32, .logical = .{ .int = .{ .bit_width = 32, .is_signed = false } }, .type_length = null };
    pub const uint64 = TypeInfo{ .physical = .int64, .logical = .{ .int = .{ .bit_width = 64, .is_signed = false } }, .type_length = null };

    pub fn forDecimal(precision: i32, scale: i32) TypeInfo {
        const logical: format.LogicalType = .{ .decimal = .{ .precision = precision, .scale = scale } };
        if (precision <= 9) {
            return .{ .physical = .int32, .logical = logical, .type_length = null };
        } else if (precision <= 18) {
            return .{ .physical = .int64, .logical = logical, .type_length = null };
        } else {
            const byte_len = types.decimalByteLengthRuntime(precision);
            const type_len = safe.castTo(i32, byte_len) catch unreachable; // decimalByteLengthRuntime returns max 16
            return .{ .physical = .fixed_bytes, .logical = logical, .type_length = type_len };
        }
    }

    pub fn fixedBytes(len: i32) TypeInfo {
        return .{ .physical = .fixed_bytes, .logical = null, .type_length = len };
    }

    pub fn asRequired(self: TypeInfo) TypeInfo {
        var copy = self;
        copy.required = true;
        return copy;
    }
};

/// Per-column or per-leaf-path property overrides.
/// Any non-null field overrides the global default.
pub const ColumnProperties = struct {
    compression: ?format.CompressionCodec = null,
    encoding: ?format.Encoding = null,
    use_dictionary: ?bool = null,
    dictionary_size_limit: ?usize = null,
    max_page_size: ?usize = null,
};

const PendingColumn = struct {
    name: [:0]u8,
    col_type: ColumnType,
    logical_type: ?format.LogicalType = null,
    type_length: ?i32 = null,
    required: bool = false,
    props: ColumnProperties = .{},
    schema_node: ?*const SchemaNode = null,
};

/// Per-column builder state for constructing nested values.
/// Uses a stack to handle arbitrarily deep nesting (list-of-struct-of-list...).
pub const ValueBuilder = struct {
    pub const FrameKind = enum { list, struct_, map, map_entry };

    pub const Frame = struct {
        kind: FrameKind,
        items: std.ArrayListUnmanaged(Value) = .empty,
        map_entries: std.ArrayListUnmanaged(Value.MapEntryValue) = .empty,
        struct_fields: std.ArrayListUnmanaged(Value.FieldValue) = .empty,
        entry_key: ?Value = null,
    };

    stack: std.ArrayListUnmanaged(Frame) = .empty,

    fn push(self: *ValueBuilder, allocator: Allocator, kind: FrameKind) !void {
        try self.stack.append(allocator, .{ .kind = kind });
    }

    pub fn top(self: *ValueBuilder) ?*Frame {
        if (self.stack.items.len == 0) return null;
        return &self.stack.items[self.stack.items.len - 1];
    }

    pub fn pop(self: *ValueBuilder) ?Frame {
        if (self.stack.items.len == 0) return null;
        return self.stack.pop();
    }

    fn appendValue(self: *ValueBuilder, allocator: Allocator, val: Value) !void {
        const frame = self.top() orelse return error.InvalidState;
        switch (frame.kind) {
            .list => try frame.items.append(allocator, val),
            .map_entry => {
                if (frame.entry_key == null) {
                    frame.entry_key = val;
                } else {
                    try frame.map_entries.append(allocator, .{
                        .key = frame.entry_key.?,
                        .value = val,
                    });
                    frame.entry_key = null;
                }
            },
            else => return error.InvalidState,
        }
    }

    pub fn isEmpty(self: *const ValueBuilder) bool {
        return self.stack.items.len == 0;
    }

    pub fn deinit(self: *ValueBuilder, allocator: Allocator) void {
        for (self.stack.items) |*frame| {
            frame.items.deinit(allocator);
            frame.map_entries.deinit(allocator);
            frame.struct_fields.deinit(allocator);
        }
        self.stack.deinit(allocator);
    }
};

const RowGroupMeta = struct {
    columns: []format.ColumnChunk,
    num_rows: i64,
    total_byte_size: i64,
    file_offset: i64,
};

pub const DynamicWriterError = error{
    InvalidState,
    InvalidArgument,
    WriteError,
    OutOfMemory,
    IntegerOverflow,
    InvalidData,
    TooManyValues,
    InvalidFixedLength,
    DuplicateColumnName,
    UnsupportedEncoding,
};

pub const DynamicWriter = struct {
    allocator: Allocator,
    target_writer: WriteTargetWriter = undefined,
    target: WriteTarget,

    pending_columns: std.ArrayListUnmanaged(PendingColumn) = .empty,
    default_codec: format.CompressionCodec = .uncompressed,
    began: bool = false,

    // Encoding options (set before begin())
    use_dictionary: bool = true,
    int_encoding: format.Encoding = .plain,
    float_encoding: format.Encoding = .plain,
    max_page_size: ?usize = null,
    dictionary_size_limit: ?usize = 1_048_576, // 1MB default, matches Arrow/parquet-go
    dictionary_cardinality_threshold: ?f32 = null,
    write_page_checksum: bool = true,

    path_properties: std.StringHashMapUnmanaged(ColumnProperties) = .empty,

    num_columns: usize = 0,
    column_types: []ColumnType = &.{},
    column_logical_types: []?format.LogicalType = &.{},
    column_type_lengths: []?i32 = &.{},
    column_codecs: []format.CompressionCodec = &.{},
    column_props: []ColumnProperties = &.{},
    column_required: []bool = &.{},
    column_names_owned: [][:0]u8 = &.{},
    current_row: []Value = &.{},
    row_set: []bool = &.{},
    column_buffers: []std.ArrayListUnmanaged(Value) = &.{},
    column_schema_nodes: []?*const SchemaNode = &.{},
    value_builders: []ValueBuilder = &.{},
    bytes_arena: std.heap.ArenaAllocator,
    schema_arena: std.heap.ArenaAllocator,

    row_groups: std.ArrayListUnmanaged(RowGroupMeta) = .empty,
    current_offset: i64 = 4, // after PAR1 magic

    row_group_row_limit: ?usize = null,
    rows_in_current_group: usize = 0,
    kv_metadata: std.ArrayListUnmanaged(format.KeyValue) = .empty,

    _backend_cleanup: ?BackendCleanup = null,
    _to_owned_slice_fn: ?*const fn (*anyopaque) error{OutOfMemory}![]u8 = null,
    _to_owned_slice_ctx: ?*anyopaque = null,

    pub fn init(allocator: Allocator, target: WriteTarget) DynamicWriter {
        return .{
            .allocator = allocator,
            .target = target,
            .bytes_arena = std.heap.ArenaAllocator.init(allocator),
            .schema_arena = std.heap.ArenaAllocator.init(allocator),
        };
    }

    // -- Schema building --

    /// Validates that a value encoding is compatible with the given column type.
    fn validateEncodingForType(enc: format.Encoding, col_type: ColumnType) DynamicWriterError!void {
        switch (enc) {
            .byte_stream_split => switch (col_type) {
                .int32, .int64, .float_, .double_ => {},
                else => return error.UnsupportedEncoding,
            },
            .delta_binary_packed => switch (col_type) {
                .int32, .int64 => {},
                else => return error.UnsupportedEncoding,
            },
            .rle => switch (col_type) {
                .bool_ => {},
                else => return error.UnsupportedEncoding,
            },
            .plain, .rle_dictionary => {},
            else => return error.UnsupportedEncoding,
        }
    }

    pub fn addColumn(self: *DynamicWriter, name: []const u8, info: TypeInfo, opts: ColumnProperties) DynamicWriterError!void {
        if (self.began) return error.InvalidState;
        if (opts.encoding) |enc| {
            try validateEncodingForType(enc, info.physical);
        }
        for (self.pending_columns.items) |col| {
            if (std.mem.eql(u8, col.name, name)) return error.DuplicateColumnName;
        }
        const duped = self.allocator.dupeZ(u8, name) catch return error.OutOfMemory;
        errdefer self.allocator.free(duped);
        self.pending_columns.append(self.allocator, .{
            .name = duped,
            .col_type = info.physical,
            .logical_type = info.logical,
            .type_length = info.type_length,
            .required = info.required,
            .props = opts,
        }) catch return error.OutOfMemory;
    }

    pub fn addColumnNested(self: *DynamicWriter, name: []const u8, node: *const SchemaNode, opts: ColumnProperties) DynamicWriterError!void {
        if (self.began) return error.InvalidState;
        for (self.pending_columns.items) |col| {
            if (std.mem.eql(u8, col.name, name)) return error.DuplicateColumnName;
        }
        const duped = self.allocator.dupeZ(u8, name) catch return error.OutOfMemory;
        errdefer self.allocator.free(duped);
        self.pending_columns.append(self.allocator, .{
            .name = duped,
            .col_type = .nested,
            .schema_node = node,
            .props = opts,
        }) catch return error.OutOfMemory;
    }

    pub fn allocSchemaNode(self: *DynamicWriter, node: SchemaNode) DynamicWriterError!*const SchemaNode {
        const ptr = self.schema_arena.allocator().create(SchemaNode) catch return error.OutOfMemory;
        ptr.* = node;
        return ptr;
    }

    pub fn allocSchemaFields(self: *DynamicWriter, count: usize) DynamicWriterError![]SchemaNode.Field {
        return self.schema_arena.allocator().alloc(SchemaNode.Field, count) catch return error.OutOfMemory;
    }

    pub fn dupeSchemaName(self: *DynamicWriter, name: []const u8) DynamicWriterError![]const u8 {
        return self.schema_arena.allocator().dupe(u8, name) catch return error.OutOfMemory;
    }

    pub fn setCompression(self: *DynamicWriter, codec: format.CompressionCodec) void {
        self.default_codec = codec;
    }

    pub fn setColumnCompression(self: *DynamicWriter, col_index: usize, codec: format.CompressionCodec) DynamicWriterError!void {
        if (self.began) {
            if (col_index >= self.num_columns) return error.InvalidArgument;
            self.column_codecs[col_index] = codec;
        } else {
            if (col_index >= self.pending_columns.items.len) return error.InvalidArgument;
            self.pending_columns.items[col_index].props.compression = codec;
        }
    }

    pub fn setRowGroupSize(self: *DynamicWriter, limit: usize) void {
        self.row_group_row_limit = limit;
    }

    pub fn setUseDictionary(self: *DynamicWriter, enable: bool) void {
        self.use_dictionary = enable;
    }

    pub fn setIntEncoding(self: *DynamicWriter, enc: format.Encoding) DynamicWriterError!void {
        try validateEncodingForType(enc, .int32);
        self.int_encoding = enc;
    }

    pub fn setFloatEncoding(self: *DynamicWriter, enc: format.Encoding) DynamicWriterError!void {
        try validateEncodingForType(enc, .float_);
        self.float_encoding = enc;
    }

    pub fn setMaxPageSize(self: *DynamicWriter, size: usize) void {
        self.max_page_size = size;
    }

    pub fn setDictionarySizeLimit(self: *DynamicWriter, limit: ?usize) void {
        self.dictionary_size_limit = limit;
    }

    pub fn setDictionaryCardinalityThreshold(self: *DynamicWriter, threshold: ?f32) void {
        self.dictionary_cardinality_threshold = threshold;
    }

    pub fn setWritePageChecksum(self: *DynamicWriter, enable: bool) void {
        self.write_page_checksum = enable;
    }

    /// Set per-leaf-path property overrides. The path is dot-joined
    /// (e.g. "address.city", "items.list.element"). Any non-null field
    /// in `props` overrides the column-level and global defaults for
    /// that specific leaf column.
    pub fn setPathProperties(self: *DynamicWriter, path: []const u8, props: ColumnProperties) DynamicWriterError!void {
        const duped = self.schema_arena.allocator().dupe(u8, path) catch return error.OutOfMemory;
        self.path_properties.put(self.allocator, duped, props) catch return error.OutOfMemory;
    }

    /// Resolve encoding options for a leaf column by merging (most specific wins):
    /// 1. path_properties lookup (dot-joined leaf path)
    /// 2. column_props[col] (top-level column override from addColumn)
    /// 3. global defaults
    fn resolveOpts(self: *const DynamicWriter, col: usize, leaf_path: []const []const u8) EncodingOpts {
        const cp = self.column_props[col];

        var opts: EncodingOpts = .{
            .use_dictionary = cp.use_dictionary orelse self.use_dictionary,
            .encoding_override = cp.encoding,
            .int_encoding = self.int_encoding,
            .float_encoding = self.float_encoding,
            .max_page_size = cp.max_page_size orelse self.max_page_size,
            .dictionary_size_limit = cp.dictionary_size_limit orelse self.dictionary_size_limit,
            .dictionary_cardinality_threshold = self.dictionary_cardinality_threshold,
            .write_page_checksum = self.write_page_checksum,
        };

        if (cp.compression) |c| {
            // Column-level compression override is handled via column_codecs,
            // but if the caller also set it via ColumnProperties we want it
            // to be consistent. column_codecs[col] is already resolved in begin().
            _ = c;
        }

        // Look up dot-joined leaf path in path_properties
        const joined = joinPath(self.allocator, leaf_path) catch return opts;
        defer self.allocator.free(joined);

        if (self.path_properties.get(joined)) |pp| {
            if (pp.encoding) |e| opts.encoding_override = e;
            if (pp.use_dictionary) |d| opts.use_dictionary = d;
            if (pp.dictionary_size_limit) |l| opts.dictionary_size_limit = l;
            if (pp.max_page_size) |s| opts.max_page_size = s;
        }

        return opts;
    }

    fn joinPath(allocator: Allocator, segments: []const []const u8) ![]u8 {
        if (segments.len == 0) return allocator.dupe(u8, "");
        var total: usize = segments.len - 1; // dots
        for (segments) |s| total += s.len;
        const buf = try allocator.alloc(u8, total);
        var pos: usize = 0;
        for (segments, 0..) |s, i| {
            @memcpy(buf[pos..][0..s.len], s);
            pos += s.len;
            if (i < segments.len - 1) {
                buf[pos] = '.';
                pos += 1;
            }
        }
        return buf;
    }

    pub fn begin(self: *DynamicWriter) DynamicWriterError!void {
        if (self.began) return error.InvalidState;
        const n = self.pending_columns.items.len;
        if (n == 0) return error.InvalidState;

        const allocator = self.allocator;

        self.target_writer = WriteTargetWriter.init(self.target);
        self.target_writer.target.write(format.PARQUET_MAGIC) catch return error.WriteError;

        errdefer {
            if (self.column_types.len > 0) allocator.free(self.column_types);
            if (self.column_logical_types.len > 0) allocator.free(self.column_logical_types);
            if (self.column_type_lengths.len > 0) allocator.free(self.column_type_lengths);
            if (self.column_codecs.len > 0) allocator.free(self.column_codecs);
            if (self.column_props.len > 0) allocator.free(self.column_props);
            if (self.column_required.len > 0) allocator.free(self.column_required);
            if (self.column_names_owned.len > 0) allocator.free(self.column_names_owned);
            if (self.current_row.len > 0) allocator.free(self.current_row);
            if (self.row_set.len > 0) allocator.free(self.row_set);
            if (self.column_buffers.len > 0) allocator.free(self.column_buffers);
            if (self.column_schema_nodes.len > 0) allocator.free(self.column_schema_nodes);
            if (self.value_builders.len > 0) allocator.free(self.value_builders);
        }
        self.column_types = allocator.alloc(ColumnType, n) catch return error.OutOfMemory;
        self.column_logical_types = allocator.alloc(?format.LogicalType, n) catch return error.OutOfMemory;
        self.column_type_lengths = allocator.alloc(?i32, n) catch return error.OutOfMemory;
        self.column_codecs = allocator.alloc(format.CompressionCodec, n) catch return error.OutOfMemory;
        self.column_props = allocator.alloc(ColumnProperties, n) catch return error.OutOfMemory;
        self.column_required = allocator.alloc(bool, n) catch return error.OutOfMemory;
        self.column_names_owned = allocator.alloc([:0]u8, n) catch return error.OutOfMemory;
        self.current_row = allocator.alloc(Value, n) catch return error.OutOfMemory;
        self.row_set = allocator.alloc(bool, n) catch return error.OutOfMemory;
        self.column_buffers = allocator.alloc(std.ArrayListUnmanaged(Value), n) catch return error.OutOfMemory;
        self.column_schema_nodes = allocator.alloc(?*const SchemaNode, n) catch return error.OutOfMemory;
        self.value_builders = allocator.alloc(ValueBuilder, n) catch return error.OutOfMemory;

        for (0..n) |i| {
            const pc = self.pending_columns.items[i];
            self.column_types[i] = pc.col_type;
            self.column_logical_types[i] = pc.logical_type;
            self.column_type_lengths[i] = pc.type_length;
            self.column_codecs[i] = pc.props.compression orelse self.default_codec;
            self.column_props[i] = pc.props;
            self.column_required[i] = pc.required;
            self.column_names_owned[i] = pc.name;
            self.current_row[i] = .null_val;
            self.row_set[i] = false;
            self.column_buffers[i] = .empty;
            self.column_schema_nodes[i] = pc.schema_node;
            self.value_builders[i] = .{};
        }

        self.num_columns = n;
        self.began = true;
    }

    // -- Row setters --

    pub fn setNull(self: *DynamicWriter, col: usize) DynamicWriterError!void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        if (self.column_required[col]) return error.InvalidArgument;
        self.current_row[col] = .null_val;
        self.row_set[col] = true;
    }

    pub fn setBool(self: *DynamicWriter, col: usize, val: bool) DynamicWriterError!void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        if (self.column_types[col] != .bool_) return error.InvalidArgument;
        self.current_row[col] = .{ .bool_val = val };
        self.row_set[col] = true;
    }

    pub fn setInt32(self: *DynamicWriter, col: usize, val: i32) DynamicWriterError!void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        if (self.column_types[col] != .int32) return error.InvalidArgument;
        self.current_row[col] = .{ .int32_val = val };
        self.row_set[col] = true;
    }

    pub fn setInt64(self: *DynamicWriter, col: usize, val: i64) DynamicWriterError!void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        if (self.column_types[col] != .int64) return error.InvalidArgument;
        self.current_row[col] = .{ .int64_val = val };
        self.row_set[col] = true;
    }

    pub fn setFloat(self: *DynamicWriter, col: usize, val: f32) DynamicWriterError!void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        if (self.column_types[col] != .float_) return error.InvalidArgument;
        self.current_row[col] = .{ .float_val = val };
        self.row_set[col] = true;
    }

    pub fn setDouble(self: *DynamicWriter, col: usize, val: f64) DynamicWriterError!void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        if (self.column_types[col] != .double_) return error.InvalidArgument;
        self.current_row[col] = .{ .double_val = val };
        self.row_set[col] = true;
    }

    pub fn setBytes(self: *DynamicWriter, col: usize, data: []const u8) DynamicWriterError!void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        const ct = self.column_types[col];
        if (ct != .bytes and ct != .fixed_bytes) return error.InvalidArgument;
        if (ct == .fixed_bytes) {
            if (self.column_type_lengths[col]) |expected_len| {
                const expected: usize = safe.castTo(usize, expected_len) catch return error.InvalidFixedLength;
                if (data.len != expected) return error.InvalidFixedLength;
            }
        }
        const copy = self.bytes_arena.allocator().dupe(u8, data) catch return error.OutOfMemory;
        self.current_row[col] = .{ .bytes_val = copy };
        self.row_set[col] = true;
    }

    pub fn setValue(self: *DynamicWriter, col: usize, val: Value) DynamicWriterError!void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        self.current_row[col] = val;
        self.row_set[col] = true;
    }

    // -- Nested value builders --

    pub fn beginList(self: *DynamicWriter, col: usize) DynamicWriterError!void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        if (self.column_types[col] != .nested) return error.InvalidArgument;
        self.value_builders[col].push(self.allocator, .list) catch return error.OutOfMemory;
    }

    pub fn endList(self: *DynamicWriter, col: usize) DynamicWriterError!void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        var frame = self.value_builders[col].pop() orelse return error.InvalidState;
        if (frame.kind != .list) {
            self.value_builders[col].stack.appendAssumeCapacity(frame); // just popped, capacity guaranteed
            return error.InvalidState;
        }
        const arena_alloc = self.bytes_arena.allocator();
        const items = arena_alloc.dupe(Value, frame.items.items) catch return error.OutOfMemory;
        frame.items.deinit(self.allocator);
        const val = Value{ .list_val = items };
        if (self.value_builders[col].top()) |parent| {
            switch (parent.kind) {
                .list => parent.items.append(self.allocator, val) catch return error.OutOfMemory,
                .struct_ => return error.InvalidState,
                .map_entry => self.value_builders[col].appendValue(self.allocator, val) catch return error.OutOfMemory,
                .map => return error.InvalidState,
            }
        } else {
            self.current_row[col] = val;
            self.row_set[col] = true;
        }
    }

    pub fn beginStruct(self: *DynamicWriter, col: usize) DynamicWriterError!void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        if (self.column_types[col] != .nested) return error.InvalidArgument;
        self.value_builders[col].push(self.allocator, .struct_) catch return error.OutOfMemory;
    }

    pub fn setStructField(self: *DynamicWriter, col: usize, field_idx: usize, val: Value) DynamicWriterError!void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        const frame = self.value_builders[col].top() orelse return error.InvalidState;
        if (frame.kind != .struct_) return error.InvalidState;
        const node = self.column_schema_nodes[col] orelse return error.InvalidState;
        const fields = getStructFields(node) orelse return error.InvalidState;
        if (field_idx >= fields.len) return error.InvalidArgument;
        const arena_alloc = self.bytes_arena.allocator();
        const name = arena_alloc.dupe(u8, fields[field_idx].name) catch return error.OutOfMemory;
        frame.struct_fields.append(self.allocator, .{ .name = name, .value = val }) catch return error.OutOfMemory;
    }

    pub fn setStructFieldBytes(self: *DynamicWriter, col: usize, field_idx: usize, data: []const u8) DynamicWriterError!void {
        const copy = self.bytes_arena.allocator().dupe(u8, data) catch return error.OutOfMemory;
        try self.setStructField(col, field_idx, .{ .bytes_val = copy });
    }

    pub fn endStruct(self: *DynamicWriter, col: usize) DynamicWriterError!void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        var frame = self.value_builders[col].pop() orelse return error.InvalidState;
        if (frame.kind != .struct_) {
            self.value_builders[col].stack.appendAssumeCapacity(frame); // just popped, capacity guaranteed
            return error.InvalidState;
        }
        const arena_alloc = self.bytes_arena.allocator();
        const fields = arena_alloc.dupe(Value.FieldValue, frame.struct_fields.items) catch return error.OutOfMemory;
        frame.struct_fields.deinit(self.allocator);
        const val = Value{ .struct_val = fields };
        if (self.value_builders[col].top()) |parent| {
            switch (parent.kind) {
                .list => parent.items.append(self.allocator, val) catch return error.OutOfMemory,
                .map_entry => self.value_builders[col].appendValue(self.allocator, val) catch return error.OutOfMemory,
                else => return error.InvalidState,
            }
        } else {
            self.current_row[col] = val;
            self.row_set[col] = true;
        }
    }

    pub fn beginMap(self: *DynamicWriter, col: usize) DynamicWriterError!void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        if (self.column_types[col] != .nested) return error.InvalidArgument;
        self.value_builders[col].push(self.allocator, .map) catch return error.OutOfMemory;
    }

    pub fn beginMapEntry(self: *DynamicWriter, col: usize) DynamicWriterError!void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        const frame = self.value_builders[col].top() orelse return error.InvalidState;
        if (frame.kind != .map) return error.InvalidState;
        self.value_builders[col].push(self.allocator, .map_entry) catch return error.OutOfMemory;
    }

    pub fn endMapEntry(self: *DynamicWriter, col: usize) DynamicWriterError!void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        var frame = self.value_builders[col].pop() orelse return error.InvalidState;
        if (frame.kind != .map_entry) {
            self.value_builders[col].stack.appendAssumeCapacity(frame); // just popped, capacity guaranteed
            return error.InvalidState;
        }
        if (frame.entry_key == null and frame.map_entries.items.len == 0) return error.InvalidState;
        const parent = self.value_builders[col].top() orelse return error.InvalidState;
        if (parent.kind != .map) return error.InvalidState;
        if (frame.map_entries.items.len == 1) {
            parent.map_entries.append(self.allocator, frame.map_entries.items[0]) catch return error.OutOfMemory;
        } else if (frame.entry_key != null) {
            parent.map_entries.append(self.allocator, .{
                .key = frame.entry_key.?,
                .value = .null_val,
            }) catch return error.OutOfMemory;
        }
        frame.map_entries.deinit(self.allocator);
    }

    pub fn endMap(self: *DynamicWriter, col: usize) DynamicWriterError!void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        var frame = self.value_builders[col].pop() orelse return error.InvalidState;
        if (frame.kind != .map) {
            self.value_builders[col].stack.appendAssumeCapacity(frame); // just popped, capacity guaranteed
            return error.InvalidState;
        }
        const arena_alloc = self.bytes_arena.allocator();
        const entries = arena_alloc.dupe(Value.MapEntryValue, frame.map_entries.items) catch return error.OutOfMemory;
        frame.map_entries.deinit(self.allocator);
        const val = Value{ .map_val = entries };
        if (self.value_builders[col].top()) |parent| {
            switch (parent.kind) {
                .list => parent.items.append(self.allocator, val) catch return error.OutOfMemory,
                .map_entry => self.value_builders[col].appendValue(self.allocator, val) catch return error.OutOfMemory,
                else => return error.InvalidState,
            }
        } else {
            self.current_row[col] = val;
            self.row_set[col] = true;
        }
    }

    pub fn appendNestedValue(self: *DynamicWriter, col: usize, val: Value) DynamicWriterError!void {
        if (!self.began) return error.InvalidState;
        if (col >= self.num_columns) return error.InvalidArgument;
        self.value_builders[col].appendValue(self.allocator, val) catch return error.OutOfMemory;
    }

    pub fn appendNestedBytes(self: *DynamicWriter, col: usize, data: []const u8) DynamicWriterError!void {
        const copy = self.bytes_arena.allocator().dupe(u8, data) catch return error.OutOfMemory;
        try self.appendNestedValue(col, .{ .bytes_val = copy });
    }

    pub fn appendNestedFixedBytes(self: *DynamicWriter, col: usize, data: []const u8) DynamicWriterError!void {
        const copy = self.bytes_arena.allocator().dupe(u8, data) catch return error.OutOfMemory;
        try self.appendNestedValue(col, .{ .fixed_bytes_val = copy });
    }

    fn getStructFields(node: *const SchemaNode) ?[]const SchemaNode.Field {
        return switch (node.*) {
            .optional => |child| getStructFields(child),
            .list => |element| getStructFields(element),
            .map => |m| getStructFields(m.value),
            .struct_ => |s| s.fields,
            else => null,
        };
    }

    // -- Row finalization --

    pub fn addRow(self: *DynamicWriter) DynamicWriterError!void {
        if (!self.began) return error.InvalidState;
        for (0..self.num_columns) |i| {
            if (!self.row_set[i]) {
                if (self.column_required[i]) return error.InvalidArgument;
                self.current_row[i] = .null_val;
            }
            self.column_buffers[i].append(self.allocator, self.current_row[i]) catch return error.OutOfMemory;
            self.current_row[i] = .null_val;
            self.row_set[i] = false;
        }
        self.rows_in_current_group += 1;
        if (self.row_group_row_limit) |limit| {
            if (self.rows_in_current_group >= limit) {
                try self.flush();
            }
        }
    }

    // -- Flush (writes one row group) --

    pub fn flush(self: *DynamicWriter) DynamicWriterError!void {
        if (!self.began) return error.InvalidState;
        if (self.column_buffers.len == 0) return;
        const n = self.column_buffers[0].items.len;
        if (n == 0) return;

        const allocator = self.allocator;
        const writer = self.target_writer.writer();
        const row_group_offset = self.current_offset;

        var col_chunks_list: std.ArrayListUnmanaged(format.ColumnChunk) = .empty;
        errdefer {
            for (col_chunks_list.items) |cc| {
                if (cc.meta_data) |meta| {
                    allocator.free(meta.encodings);
                    for (meta.path_in_schema) |p| allocator.free(p);
                    allocator.free(meta.path_in_schema);
                }
            }
            col_chunks_list.deinit(allocator);
        }

        var total_byte_size: i64 = 0;

        for (0..self.num_columns) |col| {
            const items = self.column_buffers[col].items;
            const codec = self.column_codecs[col];
            const col_name = self.column_names_owned[col];

            if (self.column_types[col] == .nested) {
                const node = self.column_schema_nodes[col] orelse return error.InvalidState;
                try self.flushNestedColumn(&col_chunks_list, items, n, node, col, col_name, codec, &total_byte_size);
            } else {
                const path: []const []const u8 = &.{col_name};
                const opts = self.resolveOpts(col, &.{col_name});
                const is_optional = !self.column_required[col];
                const result: column_writer.ColumnChunkResult = switch (self.column_types[col]) {
                    .bool_ => flushColumnBool(allocator, writer, path, items, n, self.current_offset, codec, is_optional, opts) catch |e| return mapFlushError(e),
                    .int32 => flushColumnTyped(i32, allocator, writer, path, items, n, self.current_offset, codec, is_optional, opts) catch |e| return mapFlushError(e),
                    .int64 => flushColumnTyped(i64, allocator, writer, path, items, n, self.current_offset, codec, is_optional, opts) catch |e| return mapFlushError(e),
                    .float_ => flushColumnTyped(f32, allocator, writer, path, items, n, self.current_offset, codec, is_optional, opts) catch |e| return mapFlushError(e),
                    .double_ => flushColumnTyped(f64, allocator, writer, path, items, n, self.current_offset, codec, is_optional, opts) catch |e| return mapFlushError(e),
                    .bytes => flushColumnBytes(allocator, writer, path, items, n, self.current_offset, codec, is_optional, opts) catch |e| return mapFlushError(e),
                    .fixed_bytes => flushColumnFixedBytes(
                        allocator, writer, path, items, n, self.current_offset, codec,
                        self.column_type_lengths[col] orelse return error.InvalidState, is_optional, opts,
                    ) catch |e| return mapFlushError(e),
                    .nested => return error.InvalidState,
                };

                self.target_writer.flush() catch return error.WriteError;

                col_chunks_list.append(allocator, .{
                    .file_path = null,
                    .file_offset = result.file_offset,
                    .meta_data = result.metadata,
                }) catch return error.OutOfMemory;
                total_byte_size += result.metadata.total_compressed_size;
                self.current_offset += safe.castTo(i64, result.total_bytes) catch return error.IntegerOverflow;
            }
        }

        const col_chunks = col_chunks_list.toOwnedSlice(allocator) catch return error.OutOfMemory;

        self.row_groups.append(allocator, .{
            .columns = col_chunks,
            .num_rows = safe.castTo(i64, n) catch return error.TooManyValues,
            .total_byte_size = total_byte_size,
            .file_offset = row_group_offset,
        }) catch {
            allocator.free(col_chunks);
            return error.OutOfMemory;
        };

        for (0..self.num_columns) |i| {
            self.column_buffers[i].clearRetainingCapacity();
        }
        _ = self.bytes_arena.reset(.retain_capacity);
        self.rows_in_current_group = 0;
    }

    fn flushNestedColumn(
        self: *DynamicWriter,
        col_chunks_list: *std.ArrayListUnmanaged(format.ColumnChunk),
        items: []const Value,
        n: usize,
        node: *const SchemaNode,
        col: usize,
        col_name: [:0]u8,
        default_codec: format.CompressionCodec,
        total_byte_size: *i64,
    ) DynamicWriterError!void {
        const allocator = self.allocator;
        const writer = self.target_writer.writer();
        const leaf_count = node.countLeafColumns();

        var all_columns: std.ArrayListUnmanaged(nested_mod.FlatColumn) = .empty;
        defer {
            for (all_columns.items) |*c| c.deinit(allocator);
            all_columns.deinit(allocator);
        }

        for (0..leaf_count) |_| {
            all_columns.append(allocator, nested_mod.FlatColumn.init()) catch return error.OutOfMemory;
        }

        for (0..n) |row_idx| {
            const row_value = items[row_idx];
            var row_columns = nested_mod.flattenValue(allocator, node, row_value) catch return error.OutOfMemory;
            defer row_columns.deinit();

            for (row_columns.columns.items, 0..) |rc, i| {
                for (rc.values.items) |v| {
                    all_columns.items[i].values.append(allocator, v) catch return error.OutOfMemory;
                }
                for (rc.def_levels.items) |d| {
                    all_columns.items[i].def_levels.append(allocator, d) catch return error.OutOfMemory;
                }
                for (rc.rep_levels.items) |r| {
                    all_columns.items[i].rep_levels.append(allocator, r) catch return error.OutOfMemory;
                }
            }
        }

        var leaf_paths: std.ArrayListUnmanaged([]const []const u8) = .empty;
        defer {
            for (leaf_paths.items) |path| {
                for (path) |seg| allocator.free(seg);
                allocator.free(path);
            }
            leaf_paths.deinit(allocator);
        }

        var current_path: std.ArrayListUnmanaged([]const u8) = .empty;
        defer current_path.deinit(allocator);
        current_path.append(allocator, col_name) catch return error.OutOfMemory;
        buildLeafPaths(allocator, &leaf_paths, &current_path, node) catch return error.OutOfMemory;

        const leaf_levels = node.computeLeafLevels(allocator) catch return error.OutOfMemory;
        defer allocator.free(leaf_levels);

        for (all_columns.items, 0..) |*flat_col, leaf_idx| {
            const path = if (leaf_idx < leaf_paths.items.len) leaf_paths.items[leaf_idx] else &[_][]const u8{col_name};
            const levels = if (leaf_idx < leaf_levels.len) leaf_levels[leaf_idx] else SchemaNode.Levels{ .max_def = 0, .max_rep = 0 };

            const opts = self.resolveOpts(col, path);
            const leaf_codec = if (self.path_properties.count() > 0) blk: {
                const joined = joinPath(allocator, path) catch break :blk default_codec;
                defer allocator.free(joined);
                if (self.path_properties.get(joined)) |pp| {
                    break :blk pp.compression orelse default_codec;
                }
                break :blk default_codec;
            } else default_codec;

            const result = column_writer.writeColumnChunkFromValues(
                allocator, writer, path,
                flat_col.values.items, flat_col.def_levels.items, flat_col.rep_levels.items,
                levels.max_def, levels.max_rep,
                self.current_offset, leaf_codec,
                .{
                    .use_dictionary = opts.use_dictionary,
                    .encoding = opts.encoding_override,
                    .int_encoding = opts.int_encoding,
                    .float_encoding = opts.float_encoding,
                    .max_page_size = opts.max_page_size,
                    .dictionary_size_limit = opts.dictionary_size_limit,
                    .dictionary_cardinality_threshold = opts.dictionary_cardinality_threshold,
                    .write_page_checksum = opts.write_page_checksum,
                },
            ) catch return error.WriteError;

            self.target_writer.flush() catch return error.WriteError;

            col_chunks_list.append(allocator, .{
                .file_path = null,
                .file_offset = result.file_offset,
                .meta_data = result.metadata,
            }) catch return error.OutOfMemory;
            total_byte_size.* += result.metadata.total_compressed_size;
            self.current_offset += safe.castTo(i64, result.total_bytes) catch return error.IntegerOverflow;
        }
    }

    fn buildLeafPaths(
        allocator: Allocator,
        paths: *std.ArrayListUnmanaged([]const []const u8),
        current_path: *std.ArrayListUnmanaged([]const u8),
        node: *const SchemaNode,
    ) !void {
        switch (node.*) {
            .optional => |child| try buildLeafPaths(allocator, paths, current_path, child),
            .list => |element| {
                try current_path.append(allocator, "list");
                defer _ = current_path.pop();
                try current_path.append(allocator, "element");
                defer _ = current_path.pop();
                try buildLeafPaths(allocator, paths, current_path, element);
            },
            .map => |m| {
                try current_path.append(allocator, "key_value");
                try current_path.append(allocator, "key");
                try buildLeafPaths(allocator, paths, current_path, m.key);
                _ = current_path.pop();
                try current_path.append(allocator, "value");
                try buildLeafPaths(allocator, paths, current_path, m.value);
                _ = current_path.pop();
                _ = current_path.pop();
            },
            .struct_ => |s| {
                for (s.fields) |f| {
                    try current_path.append(allocator, f.name);
                    try buildLeafPaths(allocator, paths, current_path, f.node);
                    _ = current_path.pop();
                }
            },
            .boolean, .int32, .int64, .float, .double, .byte_array, .fixed_len_byte_array => {
                const path = try allocator.alloc([]const u8, current_path.items.len);
                for (current_path.items, 0..) |seg, i| {
                    path[i] = try allocator.dupe(u8, seg);
                }
                try paths.append(allocator, path);
            },
        }
    }

    const EncodingOpts = struct {
        use_dictionary: bool = true,
        encoding_override: ?format.Encoding = null,
        int_encoding: format.Encoding = .plain,
        float_encoding: format.Encoding = .plain,
        max_page_size: ?usize = null,
        dictionary_size_limit: ?usize = 1_048_576,
        dictionary_cardinality_threshold: ?f32 = null,
        write_page_checksum: bool = true,
    };

    fn mapFlushError(e: anyerror) DynamicWriterError {
        return switch (e) {
            error.UnsupportedEncoding => error.UnsupportedEncoding,
            error.OutOfMemory => error.OutOfMemory,
            else => error.WriteError,
        };
    }

    fn flushColumnBool(
        allocator: Allocator,
        writer: *std.Io.Writer,
        path: []const []const u8,
        items: []const Value,
        n: usize,
        offset: i64,
        codec: format.CompressionCodec,
        is_optional: bool,
        opts: EncodingOpts,
    ) !column_writer.ColumnChunkResult {
        const typed = try allocator.alloc(types.Optional(bool), n);
        defer allocator.free(typed);
        for (items, 0..) |v, j| {
            typed[j] = if (v.isNull()) .null_value else .{
                .value = v.asBool() orelse return error.InvalidData,
            };
        }
        return column_writer.writeColumnChunkOptionalWithPathArray(
            bool, allocator, writer, path, typed,
            is_optional, offset, codec, opts.write_page_checksum,
        );
    }

    fn flushColumnTyped(
        comptime T: type,
        allocator: Allocator,
        writer: *std.Io.Writer,
        path: []const []const u8,
        items: []const Value,
        n: usize,
        offset: i64,
        codec: format.CompressionCodec,
        is_optional: bool,
        opts: EncodingOpts,
    ) !column_writer.ColumnChunkResult {
        const typed = try allocator.alloc(types.Optional(T), n);
        defer allocator.free(typed);
        for (items, 0..) |v, j| {
            typed[j] = if (v.isNull()) .null_value else .{
                .value = extractTyped(T, v) orelse return error.InvalidData,
            };
        }

        if (opts.encoding_override) |enc| {
            return column_writer.writeColumnChunkOptionalWithEncoding(
                T, allocator, writer, path, typed,
                is_optional, offset, codec, enc, opts.max_page_size, opts.write_page_checksum,
            );
        }

        if (!opts.use_dictionary) {
            const enc = switch (T) {
                i32, i64 => opts.int_encoding,
                f32, f64 => opts.float_encoding,
                else => format.Encoding.plain,
            };
            return column_writer.writeColumnChunkOptionalWithEncoding(
                T, allocator, writer, path, typed,
                is_optional, offset, codec, enc, opts.max_page_size, opts.write_page_checksum,
            );
        }

        return column_writer.writeColumnChunkDictOptionalWithPathArray(
            T, allocator, writer, path, typed,
            is_optional, offset, codec,
            opts.dictionary_size_limit,
            opts.dictionary_cardinality_threshold,
            opts.max_page_size,
            opts.write_page_checksum,
        );
    }

    fn flushColumnBytes(
        allocator: Allocator,
        writer: *std.Io.Writer,
        path: []const []const u8,
        items: []const Value,
        n: usize,
        offset: i64,
        codec: format.CompressionCodec,
        is_optional: bool,
        opts: EncodingOpts,
    ) !column_writer.ColumnChunkResult {
        const typed = try allocator.alloc(types.Optional([]const u8), n);
        defer allocator.free(typed);
        for (items, 0..) |v, j| {
            typed[j] = if (v.isNull()) .null_value else .{
                .value = v.asBytes() orelse return error.InvalidData,
            };
        }

        if (opts.encoding_override) |enc| {
            return column_writer.writeColumnChunkByteArrayOptionalWithEncoding(
                allocator, writer, path, typed,
                is_optional, offset, codec, enc, opts.max_page_size, opts.write_page_checksum,
            );
        }

        if (!opts.use_dictionary) {
            return column_writer.writeColumnChunkByteArrayOptionalWithEncoding(
                allocator, writer, path, typed,
                is_optional, offset, codec, .plain, opts.max_page_size, opts.write_page_checksum,
            );
        }

        return column_writer.writeColumnChunkByteArrayDictOptionalWithPathArray(
            allocator, writer, path, typed,
            is_optional, offset, codec,
            opts.dictionary_size_limit,
            opts.dictionary_cardinality_threshold,
            opts.max_page_size,
            opts.write_page_checksum,
        );
    }

    fn flushColumnFixedBytes(
        allocator: Allocator,
        writer: *std.Io.Writer,
        path: []const []const u8,
        items: []const Value,
        n: usize,
        offset: i64,
        codec: format.CompressionCodec,
        type_length: i32,
        is_optional: bool,
        opts: EncodingOpts,
    ) !column_writer.ColumnChunkResult {
        const typed = try allocator.alloc(types.Optional([]const u8), n);
        defer allocator.free(typed);
        for (items, 0..) |v, j| {
            typed[j] = if (v.isNull()) .null_value else .{
                .value = v.asBytes() orelse return error.InvalidData,
            };
        }
        const fixed_len = safe.castTo(u32, type_length) catch return error.InvalidTypeLength;
        return column_writer.writeColumnChunkFixedByteArrayOptionalWithPathArray(
            allocator, writer, path, typed,
            fixed_len, is_optional, offset, codec, opts.write_page_checksum,
        );
    }

    fn extractTyped(comptime T: type, v: Value) ?T {
        return switch (T) {
            bool => v.asBool(),
            i32 => v.asInt32(),
            i64 => v.asInt64(),
            f32 => v.asFloat(),
            f64 => v.asDouble(),
            []const u8 => v.asBytes(),
            else => null,
        };
    }

    // -- Key-value metadata --

    pub fn setKvMetadata(self: *DynamicWriter, key: []const u8, val: ?[]const u8) DynamicWriterError!void {
        const allocator = self.allocator;
        for (self.kv_metadata.items) |*kv| {
            if (std.mem.eql(u8, kv.key, key)) {
                if (kv.value) |old_v| allocator.free(old_v);
                kv.value = null;
                kv.value = if (val) |v| allocator.dupe(u8, v) catch return error.OutOfMemory else null;
                return;
            }
        }
        const owned_key = allocator.dupe(u8, key) catch return error.OutOfMemory;
        errdefer allocator.free(owned_key);
        const owned_value = if (val) |v| allocator.dupe(u8, v) catch return error.OutOfMemory else null;
        self.kv_metadata.append(allocator, .{ .key = owned_key, .value = owned_value }) catch return error.OutOfMemory;
    }

    // -- Close (flush remaining + write footer) --

    pub fn close(self: *DynamicWriter) DynamicWriterError!void {
        if (!self.began) return error.InvalidState;
        if (self.column_buffers.len > 0 and self.column_buffers[0].items.len > 0) {
            try self.flush();
        }
        try self.writeFooter();
        self.target_writer.target.close() catch return error.WriteError;
    }

    fn writeFooter(self: *DynamicWriter) DynamicWriterError!void {
        const allocator = self.allocator;
        const footer_bytes = try self.buildSerializedFooter();
        defer allocator.free(footer_bytes);

        const writer = self.target_writer.writer();
        writer.writeAll(footer_bytes) catch return error.WriteError;

        var len_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &len_buf, safe.castTo(u32, footer_bytes.len) catch return error.IntegerOverflow, .little);
        writer.writeAll(&len_buf) catch return error.WriteError;
        writer.writeAll(format.PARQUET_MAGIC) catch return error.WriteError;

        self.target_writer.flush() catch return error.WriteError;
    }

    fn buildSerializedFooter(self: *DynamicWriter) DynamicWriterError![]u8 {
        const allocator = self.allocator;
        const n = self.num_columns;

        var total_elements: usize = 1; // root
        for (0..n) |i| {
            if (self.column_schema_nodes[i]) |node| {
                total_elements += core_writer.Writer.countSchemaElements(node);
            } else {
                total_elements += 1;
            }
        }

        const schema = allocator.alloc(format.SchemaElement, total_elements) catch return error.OutOfMemory;
        defer allocator.free(schema);

        schema[0] = .{
            .type_ = null,
            .type_length = null,
            .repetition_type = null,
            .name = "schema",
            .num_children = safe.castTo(i32, n) catch return error.IntegerOverflow,
            .converted_type = null,
            .scale = null,
            .precision = null,
            .field_id = null,
            .logical_type = null,
        };

        var idx: usize = 1;
        for (0..n) |i| {
            if (self.column_schema_nodes[i]) |node| {
                idx = core_writer.Writer.generateSchemaFromNodeStatic(schema, idx, self.column_names_owned[i], node, .required) catch return error.OutOfMemory;
            } else {
                const pt = self.column_types[i].toPhysicalType() orelse return error.InvalidState;
                const lt = self.column_logical_types[i];

                var precision: ?i32 = null;
                var scale: ?i32 = null;
                var converted_type: ?i32 = null;
                if (lt) |l| {
                    if (l == .decimal) {
                        precision = l.decimal.precision;
                        scale = l.decimal.scale;
                    }
                } else if (pt == .fixed_len_byte_array and self.column_type_lengths[i] != null and self.column_type_lengths[i].? == 12) {
                    converted_type = format.ConvertedType.INTERVAL;
                }

                schema[idx] = .{
                    .type_ = pt,
                    .type_length = self.column_type_lengths[i],
                    .repetition_type = if (self.column_required[i]) .required else .optional,
                    .name = self.column_names_owned[i],
                    .num_children = null,
                    .converted_type = converted_type,
                    .scale = scale,
                    .precision = precision,
                    .field_id = null,
                    .logical_type = lt,
                };
                idx += 1;
            }
        }

        var total_rows: i64 = 0;
        for (self.row_groups.items) |rg| {
            total_rows = std.math.add(i64, total_rows, rg.num_rows) catch return error.IntegerOverflow;
        }

        const fmt_rgs = allocator.alloc(format.RowGroup, self.row_groups.items.len) catch return error.OutOfMemory;
        defer allocator.free(fmt_rgs);

        for (self.row_groups.items, 0..) |rg, i| {
            fmt_rgs[i] = .{
                .columns = rg.columns,
                .total_byte_size = rg.total_byte_size,
                .num_rows = rg.num_rows,
                .sorting_columns = null,
                .file_offset = rg.file_offset,
                .total_compressed_size = rg.total_byte_size,
                .ordinal = safe.castTo(i16, i) catch null, // Parquet spec limits ordinal to i16
            };
        }

        const file_metadata = format.FileMetaData{
            .version = 1,
            .schema = schema,
            .num_rows = total_rows,
            .row_groups = fmt_rgs,
            .key_value_metadata = if (self.kv_metadata.items.len > 0) self.kv_metadata.items else null,
            .created_by = "zig-parquet",
        };

        var thrift_writer = thrift.CompactWriter.init(allocator);
        defer thrift_writer.deinit();
        file_metadata.serialize(&thrift_writer) catch return error.OutOfMemory;

        return allocator.dupe(u8, thrift_writer.getWritten()) catch return error.OutOfMemory;
    }

    // -- Buffer access --

    pub fn toOwnedSlice(self: *DynamicWriter) error{OutOfMemory}![]u8 {
        if (self._to_owned_slice_fn) |func| {
            return func(self._to_owned_slice_ctx.?);
        }
        return error.OutOfMemory;
    }

    // -- Cleanup --

    pub fn deinit(self: *DynamicWriter) void {
        const allocator = self.allocator;

        for (0..self.num_columns) |i| {
            self.column_buffers[i].deinit(allocator);
            self.value_builders[i].deinit(allocator);
        }
        if (self.column_buffers.len > 0) allocator.free(self.column_buffers);
        if (self.value_builders.len > 0) allocator.free(self.value_builders);
        if (self.column_schema_nodes.len > 0) allocator.free(self.column_schema_nodes);
        if (self.current_row.len > 0) allocator.free(self.current_row);
        if (self.row_set.len > 0) allocator.free(self.row_set);
        if (self.column_types.len > 0) allocator.free(self.column_types);
        if (self.column_logical_types.len > 0) allocator.free(self.column_logical_types);
        if (self.column_type_lengths.len > 0) allocator.free(self.column_type_lengths);
        if (self.column_codecs.len > 0) allocator.free(self.column_codecs);
        if (self.column_props.len > 0) allocator.free(self.column_props);
        if (self.column_required.len > 0) allocator.free(self.column_required);
        self.path_properties.deinit(allocator);

        self.bytes_arena.deinit();
        self.schema_arena.deinit();

        for (self.row_groups.items) |rg| {
            for (rg.columns) |col| {
                if (col.meta_data) |meta| {
                    allocator.free(meta.encodings);
                    for (meta.path_in_schema) |path| allocator.free(path);
                    allocator.free(meta.path_in_schema);
                    if (meta.statistics) |stats| {
                        if (stats.max) |m| allocator.free(m);
                        if (stats.min) |m| allocator.free(m);
                        if (stats.max_value) |m| allocator.free(m);
                        if (stats.min_value) |m| allocator.free(m);
                    }
                }
            }
            allocator.free(rg.columns);
        }
        self.row_groups.deinit(allocator);

        for (self.kv_metadata.items) |kv| {
            allocator.free(kv.key);
            if (kv.value) |v| allocator.free(v);
        }
        self.kv_metadata.deinit(allocator);

        for (self.pending_columns.items) |pc| {
            allocator.free(pc.name);
        }
        self.pending_columns.deinit(allocator);
        if (self.column_names_owned.len > 0) allocator.free(self.column_names_owned);

        if (self._backend_cleanup) |cleanup| {
            cleanup.deinit_fn(cleanup.ptr, allocator);
        }
    }
};
