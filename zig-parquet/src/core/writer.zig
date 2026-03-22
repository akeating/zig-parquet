//! Parquet Writer
//!
//! High-level API for writing Parquet files.
//!
//! ## Example
//! ```zig
//! const parquet = @import("parquet");
//!
//! var writer = try parquet.Writer.init(allocator, file, &.{
//!     .{ .name = "id", .type = .int64, .optional = true },
//!     .{ .name = "name", .type = .byte_array, .optional = true },
//! });
//! defer writer.deinit();
//!
//! // Use writeColumnOptional for unified nullable/non-nullable handling
//! try writer.writeColumnOptional(i64, 0, &[_]parquet.Optional(i64){
//!     .{ .value = 1 }, .{ .value = 2 }, .{ .null_value = {} },
//! });
//! try writer.writeColumnOptional([]const u8, 1, &[_]parquet.Optional([]const u8){
//!     .{ .value = "a" }, .{ .null_value = {} }, .{ .value = "c" },
//! });
//!
//! try writer.close();
//! ```

const std = @import("std");
const safe = @import("safe.zig");
const format = @import("format.zig");
const thrift = @import("thrift/mod.zig");
const column_writer = @import("column_writer.zig");
const list_encoder = @import("list_encoder.zig");
const geo = @import("geo/mod.zig");
const statistics = @import("statistics.zig");
const map_encoder = @import("map_encoder.zig");
const types = @import("types.zig");
const schema_mod = @import("schema.zig");
const value_mod = @import("value.zig");
const nested_mod = @import("nested.zig");
const column_def_mod = @import("column_def.zig");
const write_target = @import("write_target.zig");

const Optional = types.Optional;
const MapEntry = map_encoder.MapEntry;
pub const SchemaNode = schema_mod.SchemaNode;
pub const Value = value_mod.Value;

// Re-export column definition types
pub const ColumnDef = column_def_mod.ColumnDef;
pub const StructField = column_def_mod.StructField;

// Re-export write target types
pub const WriteTarget = write_target.WriteTarget;
pub const WriteTargetWriter = write_target.WriteTargetWriter;
pub const WriteError = write_target.WriteError;
pub const BackendCleanup = @import("parquet_reader.zig").BackendCleanup;


/// Writer state
const State = enum {
    initialized,
    writing_columns,
    closed,
};

pub const WriterError = types.WriterError;

/// Parquet file writer
pub const Writer = struct {
    allocator: std.mem.Allocator,
    target_writer: WriteTargetWriter,
    columns: []const ColumnDef,
    state: State,
    current_offset: i64,
    num_rows: ?i64,

    num_physical_columns: usize,
    columns_written: []bool,
    column_chunks: []?format.ColumnChunk,

    geo_stats_builders: std.AutoHashMapUnmanaged(usize, *statistics.GeospatialStatisticsBuilder),

    _backend_cleanup: ?BackendCleanup = null,
    _to_owned_slice_fn: ?*const fn (*anyopaque) error{OutOfMemory}![]u8 = null,
    _to_owned_slice_ctx: ?*anyopaque = null,

    const Self = @This();

    /// Count total physical columns across all ColumnDefs
    fn countPhysicalColumns(columns: []const ColumnDef) usize {
        var count: usize = 0;
        for (columns) |col| {
            if (col.schema_node) |node| {
                // Use SchemaNode's leaf counting for complex nested types
                count += node.countLeafColumns();
            } else if (col.is_struct) {
                count += if (col.struct_fields) |fields| fields.len else 0;
            } else if (col.is_map) {
                count += 2; // Map has 2 physical columns: key and value
            } else if (col.is_list) {
                count += 1; // List is one physical column
            } else {
                count += 1;
            }
        }
        return count;
    }

    /// Initialize geospatial statistics builders for geometry/geography columns
    fn initGeoStatsBuilders(
        allocator: std.mem.Allocator,
        columns: []const ColumnDef,
        builders: *std.AutoHashMapUnmanaged(usize, *statistics.GeospatialStatisticsBuilder),
    ) !void {
        for (columns, 0..) |col, idx| {
            if (col.logical_type) |lt| {
                switch (lt) {
                    .geometry, .geography => {
                        const builder = try allocator.create(statistics.GeospatialStatisticsBuilder);
                        builder.* = statistics.GeospatialStatisticsBuilder.init(allocator);
                        try builders.put(allocator, idx, builder);
                    },
                    else => {},
                }
            }
        }
    }

    /// Get physical column index for a given ColumnDef and field index
    /// For non-struct columns, field_index should be 0
    pub fn getPhysicalColumnIndex(self: *Self, col_def_index: usize, field_index: usize) WriterError!usize {
        if (col_def_index >= self.columns.len) return error.InvalidColumnIndex;

        var physical_idx: usize = 0;
        for (self.columns[0..col_def_index]) |col| {
            if (col.schema_node) |node| {
                physical_idx += node.countLeafColumns();
            } else if (col.is_struct) {
                physical_idx += if (col.struct_fields) |fields| fields.len else 0;
            } else if (col.is_map) {
                physical_idx += 2; // key and value columns
            } else {
                physical_idx += 1;
            }
        }

        const col_def = self.columns[col_def_index];
        if (col_def.schema_node) |node| {
            const leaf_count = node.countLeafColumns();
            if (field_index >= leaf_count) return error.InvalidColumnIndex;
            return physical_idx + field_index;
        } else if (col_def.is_struct) {
            const fields = col_def.struct_fields orelse return error.InvalidColumnIndex;
            if (field_index >= fields.len) return error.InvalidColumnIndex;
            return physical_idx + field_index;
        } else if (col_def.is_map) {
            // field_index 0 = key column, field_index 1 = value column
            if (field_index >= 2) return error.InvalidColumnIndex;
            return physical_idx + field_index;
        } else {
            if (field_index != 0) return error.InvalidColumnIndex;
            return physical_idx;
        }
    }

    /// Initialize a writer with a WriteTarget. The caller manages target lifetime
    /// unless _backend_cleanup is set (by convenience constructors in api/zig/).
    pub fn initWithTarget(
        allocator: std.mem.Allocator,
        target: WriteTarget,
        columns: []const ColumnDef,
    ) WriterError!Self {
        target.write(format.PARQUET_MAGIC) catch return error.WriteError;

        const num_physical = countPhysicalColumns(columns);

        const columns_written = allocator.alloc(bool, num_physical) catch return error.OutOfMemory;
        errdefer allocator.free(columns_written);
        @memset(columns_written, false);

        const column_chunks = allocator.alloc(?format.ColumnChunk, num_physical) catch return error.OutOfMemory;
        @memset(column_chunks, null);

        var geo_stats_builders: std.AutoHashMapUnmanaged(usize, *statistics.GeospatialStatisticsBuilder) = .empty;
        initGeoStatsBuilders(allocator, columns, &geo_stats_builders) catch return error.OutOfMemory;

        return Self{
            .allocator = allocator,
            .target_writer = WriteTargetWriter.init(target),
            .columns = columns,
            .state = .initialized,
            .current_offset = 4,
            .num_rows = null,
            .num_physical_columns = num_physical,
            .columns_written = columns_written,
            .column_chunks = column_chunks,
            .geo_stats_builders = geo_stats_builders,
        };
    }

    fn getWriter(self: *Self) *std.Io.Writer {
        return self.target_writer.writer();
    }

    fn flushWriter(self: *Self) WriterError!void {
        self.target_writer.flush() catch return error.WriteError;
    }

    /// Clean up resources
    pub fn deinit(self: *Self) void {
        // Free column chunk metadata
        for (self.column_chunks) |*maybe_chunk| {
            if (maybe_chunk.*) |*chunk| {
                if (chunk.meta_data) |*md| {
                    self.allocator.free(md.encodings);
                    for (md.path_in_schema) |path| {
                        self.allocator.free(path);
                    }
                    self.allocator.free(md.path_in_schema);
                    // Free statistics if present
                    if (md.statistics) |stats| {
                        if (stats.min) |m| self.allocator.free(m);
                        if (stats.max) |m| self.allocator.free(m);
                        if (stats.min_value) |m| self.allocator.free(m);
                        if (stats.max_value) |m| self.allocator.free(m);
                    }
                    // Free geospatial statistics if present
                    if (md.geospatial_statistics) |geo_stats| {
                        if (geo_stats.geospatial_types) |gt| self.allocator.free(gt);
                    }
                }
            }
        }
        self.allocator.free(self.column_chunks);
        self.allocator.free(self.columns_written);

        // Clean up geospatial statistics builders
        var iter = self.geo_stats_builders.iterator();
        while (iter.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.geo_stats_builders.deinit(self.allocator);

        if (self._backend_cleanup) |cleanup| {
            cleanup.deinit_fn(cleanup.ptr, self.allocator);
        }
    }

    /// Map Zig type to Parquet PhysicalType at comptime
    fn physicalTypeFor(comptime T: type) format.PhysicalType {
        return if (T == i32) .int32
        else if (T == i64) .int64
        else if (T == f32) .float
        else if (T == f64) .double
        else if (T == bool) .boolean
        else if (T == []const u8) .byte_array
        else @compileError("Unsupported column type: " ++ @typeName(T));
    }

    /// Write a column (non-nullable)
    /// Supports: i32, i64, f32, f64, bool, []const u8
    /// Deprecated: Use writeColumnOptional with Optional(T) instead.
    pub fn writeColumn(self: *Self, comptime T: type, column_index: usize, values: []const T) WriterError!void {
        // Convert to Optional and delegate to writeColumnOptional
        const optional_values = self.allocator.alloc(Optional(T), values.len) catch return error.OutOfMemory;
        defer self.allocator.free(optional_values);
        for (values, 0..) |v, i| {
            optional_values[i] = Optional(T).from(v);
        }
        return self.writeColumnOptional(T, column_index, optional_values);
    }

    /// Write a nullable column
    /// Supports: i32, i64, f32, f64, bool, []const u8
    /// Deprecated: Use writeColumnOptional with Optional(T) instead.
    pub fn writeColumnNullable(self: *Self, comptime T: type, column_index: usize, values: []const ?T) WriterError!void {
        // Convert ?T to Optional(T) and delegate to writeColumnOptional
        const optional_values = self.allocator.alloc(Optional(T), values.len) catch return error.OutOfMemory;
        defer self.allocator.free(optional_values);
        for (values, 0..) |v, i| {
            optional_values[i] = Optional(T).from(v);
        }
        return self.writeColumnOptional(T, column_index, optional_values);
    }

    /// Write a column with Optional values (unified API).
    /// This is the preferred method - accepts the same Optional(T) type that Reader returns.
    /// Works for both required and optional columns (uses schema to control def levels).
    /// Supports: i32, i64, f32, f64, bool, []const u8
    pub fn writeColumnOptional(self: *Self, comptime T: type, column_index: usize, values: []const Optional(T)) WriterError!void {
        const physical_type = comptime physicalTypeFor(T);
        // Pass false - Optional API works for both required and optional columns
        try self.validateColumnWrite(column_index, physical_type, false);
        const physical_idx = try self.getPhysicalColumnIndex(column_index, 0);

        const col_def = self.columns[column_index];
        const writer = self.getWriter();

        var result = if (T == []const u8)
            column_writer.writeColumnChunkByteArrayOptionalWithPathArray(
                self.allocator,
                writer,
                &.{col_def.name},
                values,
                col_def.optional,
                self.current_offset,
                col_def.codec,
                true, // write_page_checksum
            ) catch |e| return mapColumnError(e)
        else
            column_writer.writeColumnChunkOptionalWithPathArray(
                T,
                self.allocator,
                writer,
                &.{col_def.name},
                values,
                col_def.optional,
                self.current_offset,
                col_def.codec,
                true, // write_page_checksum
            ) catch |e| return mapColumnError(e);

        // For byte_array geometry/geography columns, update bbox tracking
        if (T == []const u8) {
            if (col_def.logical_type) |lt| {
                switch (lt) {
                    .geometry, .geography => {
                        if (self.geo_stats_builders.get(column_index)) |builder| {
                            builder.updateOptional(values);
                        }
                    },
                    else => {},
                }
            }
        }

        try self.flushWriter();
        try self.recordColumnWrite(physical_idx, &result, values.len);
    }

    /// Write a fixed length byte array column (non-nullable)
    /// Deprecated: Use writeColumnFixedByteArrayOptional with Optional([]const u8) instead.
    pub fn writeColumnFixedByteArray(self: *Self, column_index: usize, values: []const []const u8) WriterError!void {
        // Convert to Optional and delegate to writeColumnFixedByteArrayOptional
        const optional_values = self.allocator.alloc(Optional([]const u8), values.len) catch return error.OutOfMemory;
        defer self.allocator.free(optional_values);
        for (values, 0..) |v, i| {
            optional_values[i] = Optional([]const u8).from(v);
        }
        return self.writeColumnFixedByteArrayOptional(column_index, optional_values);
    }

    /// Write a nullable fixed length byte array column
    /// Deprecated: Use writeColumnFixedByteArrayOptional with Optional([]const u8) instead.
    pub fn writeColumnFixedByteArrayNullable(self: *Self, column_index: usize, values: []const ?[]const u8) WriterError!void {
        // Convert ?[]const u8 to Optional([]const u8) and delegate
        const optional_values = self.allocator.alloc(Optional([]const u8), values.len) catch return error.OutOfMemory;
        defer self.allocator.free(optional_values);
        for (values, 0..) |v, i| {
            optional_values[i] = Optional([]const u8).from(v);
        }
        return self.writeColumnFixedByteArrayOptional(column_index, optional_values);
    }

    /// Write a fixed length byte array column with Optional values (unified API).
    /// This is the preferred method - accepts the same Optional type that Reader returns.
    /// Works for both required and optional columns (uses schema to control def levels).
    pub fn writeColumnFixedByteArrayOptional(self: *Self, column_index: usize, values: []const Optional([]const u8)) WriterError!void {
        // Pass false - Optional API works for both required and optional columns
        try self.validateColumnWrite(column_index, .fixed_len_byte_array, false);
        const physical_idx = try self.getPhysicalColumnIndex(column_index, 0);

        const col_def = self.columns[column_index];
        const fixed_len = try safe.castTo(u32, col_def.type_length orelse return error.InvalidFixedLength);
        const writer = self.getWriter();

        var result = column_writer.writeColumnChunkFixedByteArrayOptionalWithPathArrayAndEncoding(
            self.allocator,
            writer,
            &.{col_def.name},
            values,
            fixed_len,
            col_def.optional,
            self.current_offset,
            col_def.codec,
            col_def.value_encoding,
            true, // write_page_checksum
        ) catch |e| return mapColumnError(e);

        // INTERVAL type: skip statistics (sort order undefined per Parquet spec)
        if (col_def.converted_type) |ct| {
            if (ct == format.ConvertedType.INTERVAL) {
                if (result.metadata.statistics) |stats| {
                    // Free the statistics memory
                    if (stats.min) |m| self.allocator.free(m);
                    if (stats.max) |m| self.allocator.free(m);
                    if (stats.min_value) |m| self.allocator.free(m);
                    if (stats.max_value) |m| self.allocator.free(m);
                }
                result.metadata.statistics = null;
            }
        }

        // GEOMETRY/GEOGRAPHY types: skip min/max statistics (sort order undefined)
        // but keep null_count, and update bbox tracking
        if (col_def.logical_type) |lt| {
            switch (lt) {
                .geometry, .geography => {
                    if (result.metadata.statistics) |*stats| {
                        // Free min/max memory but keep null_count
                        if (stats.min) |m| self.allocator.free(m);
                        if (stats.max) |m| self.allocator.free(m);
                        if (stats.min_value) |m| self.allocator.free(m);
                        if (stats.max_value) |m| self.allocator.free(m);
                        stats.min = null;
                        stats.max = null;
                        stats.min_value = null;
                        stats.max_value = null;
                    }
                    // Update geospatial statistics builder for bbox tracking
                    if (self.geo_stats_builders.get(column_index)) |builder| {
                        builder.updateOptional(values);
                    }
                },
                else => {},
            }
        }

        try self.flushWriter();
        try self.recordColumnWrite(physical_idx, &result, values.len);
    }

    /// Write a list column of i32 values
    pub fn writeListColumnI32(
        self: *Self,
        column_index: usize,
        lists: []const Optional([]const Optional(i32)),
    ) WriterError!void {
        try self.validateListColumnWrite(column_index, .int32);
        const physical_idx = try self.getPhysicalColumnIndex(column_index, 0);

        const col_def = self.columns[column_index];
        const writer = self.getWriter();

        // Flatten the list data
        var flattened = list_encoder.flattenList(
            i32,
            self.allocator,
            lists,
            3,
        ) catch return error.OutOfMemory;
        defer flattened.deinit();

        var result = column_writer.writeColumnChunkList(
            i32,
            self.allocator,
            writer,
            col_def.name,
            flattened.values,
            flattened.def_levels,
            flattened.rep_levels,
            3,
            1,
            self.current_offset,
            col_def.codec,
        ) catch |e| return mapColumnError(e);

        try self.flushWriter();
        try self.recordColumnWrite(physical_idx, &result, lists.len);
    }

    /// Write a list column of i64 values
    pub fn writeListColumnI64(
        self: *Self,
        column_index: usize,
        lists: []const Optional([]const Optional(i64)),
    ) WriterError!void {
        try self.validateListColumnWrite(column_index, .int64);
        const physical_idx = try self.getPhysicalColumnIndex(column_index, 0);

        const col_def = self.columns[column_index];
        const writer = self.getWriter();

        // Flatten the list data
        var flattened = list_encoder.flattenList(
            i64,
            self.allocator,
            lists,
            3,
        ) catch return error.OutOfMemory;
        defer flattened.deinit();

        var result = column_writer.writeColumnChunkList(
            i64,
            self.allocator,
            writer,
            col_def.name,
            flattened.values,
            flattened.def_levels,
            flattened.rep_levels,
            3,
            1,
            self.current_offset,
            col_def.codec,
        ) catch |e| return mapColumnError(e);

        try self.flushWriter();
        try self.recordColumnWrite(physical_idx, &result, lists.len);
    }

    /// Write a list column for any supported element type (i32, i64, f32, f64, bool, []const u8).
    pub fn writeListColumn(
        self: *Self,
        comptime T: type,
        column_index: usize,
        lists: []const Optional([]const Optional(T)),
    ) WriterError!void {
        const pt = comptime physicalTypeFor(T);
        try self.validateListColumnWrite(column_index, pt);
        const physical_idx = try self.getPhysicalColumnIndex(column_index, 0);

        const col_def = self.columns[column_index];
        const writer = self.getWriter();

        var flattened = list_encoder.flattenList(
            T,
            self.allocator,
            lists,
            3,
        ) catch return error.OutOfMemory;
        defer flattened.deinit();

        var result = column_writer.writeColumnChunkList(
            T,
            self.allocator,
            writer,
            col_def.name,
            flattened.values,
            flattened.def_levels,
            flattened.rep_levels,
            3,
            1,
            self.current_offset,
            col_def.codec,
        ) catch |e| return mapColumnError(e);

        try self.flushWriter();
        try self.recordColumnWrite(physical_idx, &result, lists.len);
    }

    fn validateListColumnWrite(self: *Self, column_index: usize, expected_type: format.PhysicalType) WriterError!void {
        if (self.state == .closed) return error.InvalidState;
        if (column_index >= self.columns.len) return error.InvalidColumnIndex;

        const physical_idx = self.getPhysicalColumnIndex(column_index, 0) catch return error.InvalidColumnIndex;
        if (self.columns_written[physical_idx]) return error.ColumnAlreadyWritten;

        const col_def = self.columns[column_index];
        if (!col_def.is_list) return error.TypeMismatch;
        if (col_def.type_ != expected_type) return error.TypeMismatch;

        self.state = .writing_columns;
    }

    // =========================================================================
    // Struct field write methods
    // =========================================================================

    /// Write a struct field
    /// Supports: i32, i64, []const u8
    /// parent_nulls indicates which rows have a null parent struct
    pub fn writeStructField(
        self: *Self,
        comptime T: type,
        struct_col_index: usize,
        field_index: usize,
        values: []const ?T,
        parent_nulls: []const bool,
    ) WriterError!void {
        const physical_type = comptime physicalTypeFor(T);
        try self.validateStructFieldWrite(struct_col_index, field_index, physical_type);
        const physical_idx = try self.getPhysicalColumnIndex(struct_col_index, field_index);

        const col_def = self.columns[struct_col_index];
        const fields = col_def.struct_fields orelse return error.InvalidColumnIndex;
        const field = fields[field_index];

        // Compute definition levels:
        // def=0: parent struct is null
        // def=1: parent present, but field is null
        // def=2: field is present
        const max_def_level: u8 = 2;
        const def_levels = self.allocator.alloc(u32, values.len) catch return error.OutOfMemory;
        defer self.allocator.free(def_levels);

        var actual_values: std.ArrayList(T) = .empty;
        defer actual_values.deinit(self.allocator);

        for (values, parent_nulls, 0..) |maybe_val, parent_null, i| {
            if (parent_null) {
                def_levels[i] = 0;
            } else if (maybe_val) |val| {
                def_levels[i] = 2;
                actual_values.append(self.allocator, val) catch return error.OutOfMemory;
            } else {
                def_levels[i] = 1;
            }
        }

        // All rep levels are 0 for struct fields
        const rep_levels = self.allocator.alloc(u32, values.len) catch return error.OutOfMemory;
        defer self.allocator.free(rep_levels);
        @memset(rep_levels, 0);

        const writer = self.getWriter();
        var result = if (T == []const u8)
            column_writer.writeColumnChunkStructByteArray(
                self.allocator,
                writer,
                col_def.name,
                field.name,
                actual_values.items,
                def_levels,
                rep_levels,
                max_def_level,
                self.current_offset,
                col_def.codec,
            ) catch |e| return mapColumnError(e)
        else
            column_writer.writeColumnChunkStruct(
                T,
                self.allocator,
                writer,
                col_def.name,
                field.name,
                actual_values.items,
                def_levels,
                rep_levels,
                max_def_level,
                self.current_offset,
                col_def.codec,
            ) catch |e| return mapColumnError(e);

        try self.flushWriter();
        try self.recordColumnWrite(physical_idx, &result, values.len);
    }

    fn validateStructFieldWrite(self: *Self, struct_col_index: usize, field_index: usize, expected_type: format.PhysicalType) WriterError!void {
        if (self.state == .closed) return error.InvalidState;
        if (struct_col_index >= self.columns.len) return error.InvalidColumnIndex;

        const col_def = self.columns[struct_col_index];
        if (!col_def.is_struct) return error.TypeMismatch;

        const fields = col_def.struct_fields orelse return error.InvalidColumnIndex;
        if (field_index >= fields.len) return error.InvalidColumnIndex;

        const field = fields[field_index];
        if (field.type_ != expected_type) return error.TypeMismatch;

        const physical_idx = self.getPhysicalColumnIndex(struct_col_index, field_index) catch return error.InvalidColumnIndex;
        if (self.columns_written[physical_idx]) return error.ColumnAlreadyWritten;

        self.state = .writing_columns;
    }

    // =========================================================================
    // Map column write methods
    // =========================================================================

    /// Write a map column with the given key and value types.
    /// Supported types: i32, i64, f32, f64, bool, []const u8.
    pub fn writeMapColumn(
        self: *Self,
        comptime K: type,
        comptime V: type,
        column_index: usize,
        maps: []const Optional([]const MapEntry(K, V)),
    ) WriterError!void {
        const key_pt = comptime zigTypeToPhysicalType(K);
        const value_pt = comptime zigTypeToPhysicalType(V);
        try self.validateMapColumnWrite(column_index, key_pt, value_pt);
        const key_physical_idx = try self.getPhysicalColumnIndex(column_index, 0);
        const value_physical_idx = try self.getPhysicalColumnIndex(column_index, 1);

        const col_def = self.columns[column_index];
        const writer = self.getWriter();

        var flattened = map_encoder.flattenMap(
            K,
            V,
            self.allocator,
            maps,
            3,
        ) catch return error.OutOfMemory;
        defer flattened.deinit();

        const key_result = column_writer.writeColumnChunkMapKey(
            self.allocator,
            writer,
            col_def.name,
            K,
            flattened.keys,
            flattened.key_def_levels,
            flattened.rep_levels,
            2,
            1,
            self.current_offset,
            col_def.codec,
        ) catch |e| return mapColumnError(e);

        try self.flushWriter();
        self.columns_written[key_physical_idx] = true;
        self.current_offset = std.math.add(i64, self.current_offset, try safe.castTo(i64, key_result.total_bytes)) catch return error.IntegerOverflow;
        self.column_chunks[key_physical_idx] = .{
            .file_path = null,
            .file_offset = key_result.file_offset,
            .meta_data = key_result.metadata,
        };

        const value_result = column_writer.writeColumnChunkMapValue(
            self.allocator,
            writer,
            col_def.name,
            V,
            flattened.values,
            flattened.value_def_levels,
            flattened.rep_levels,
            3,
            1,
            self.current_offset,
            col_def.codec,
        ) catch |e| return mapColumnError(e);

        try self.flushWriter();
        self.columns_written[value_physical_idx] = true;
        self.current_offset = std.math.add(i64, self.current_offset, try safe.castTo(i64, value_result.total_bytes)) catch return error.IntegerOverflow;
        self.column_chunks[value_physical_idx] = .{
            .file_path = null,
            .file_offset = value_result.file_offset,
            .meta_data = value_result.metadata,
        };

        if (self.num_rows) |existing| {
            if (existing != (safe.castTo(i64, maps.len) catch return error.RowCountMismatch)) {
                return error.RowCountMismatch;
            }
        } else {
            if (maps.len > std.math.maxInt(i64)) return error.TooManyRows;
            self.num_rows = try safe.castTo(i64, maps.len);
        }

        self.state = .writing_columns;
    }

    /// Write a map column with string keys and i32 values.
    /// Convenience wrapper around writeMapColumn.
    pub fn writeMapColumnStringInt32(
        self: *Self,
        column_index: usize,
        maps: []const Optional([]const MapEntry([]const u8, i32)),
    ) WriterError!void {
        return self.writeMapColumn([]const u8, i32, column_index, maps);
    }

    fn zigTypeToPhysicalType(comptime T: type) format.PhysicalType {
        if (T == i32) return .int32;
        if (T == i64) return .int64;
        if (T == f32) return .float;
        if (T == f64) return .double;
        if (T == bool) return .boolean;
        if (T == []const u8) return .byte_array;
        @compileError("Unsupported map key/value type");
    }

    /// Write a column using the Value type for nested data.
    /// This method uses the SchemaNode defined in the ColumnDef to flatten
    /// the nested Value data and write it to the appropriate physical columns.
    ///
    /// Example:
    /// ```zig
    /// // Define schema: list<struct<id: i64, name: string>>
    /// const id_node = SchemaNode{ .int64 = .{} };
    /// const name_node = SchemaNode{ .byte_array = .{} };
    /// const struct_node = SchemaNode{ .struct_ = .{ .fields = &.{
    ///     .{ .name = "id", .node = &id_node },
    ///     .{ .name = "name", .node = &name_node },
    /// }}};
    /// const list_node = SchemaNode{ .list = &struct_node };
    ///
    /// const columns = [_]ColumnDef{ColumnDef.fromNode("items", &list_node)};
    /// var writer = try Writer.init(allocator, file, &columns);
    ///
    /// // Write data as Value
    /// const row_values = [_]Value{...};  // Array of list values, one per row
    /// try writer.writeNestedColumn(0, &row_values);
    /// ```
    pub fn writeNestedColumn(
        self: *Self,
        column_index: usize,
        rows: []const Value,
    ) WriterError!void {
        if (self.state == .closed) return error.InvalidState;
        if (column_index >= self.columns.len) return error.InvalidColumnIndex;

        const col_def = self.columns[column_index];
        const node = col_def.schema_node orelse return error.TypeMismatch;

        // Flatten each row and combine
        var all_columns: std.ArrayList(nested_mod.FlatColumn) = .empty;
        defer {
            for (all_columns.items) |*col| {
                col.deinit(self.allocator);
            }
            all_columns.deinit(self.allocator);
        }

        const leaf_count = node.countLeafColumns();

        // Initialize columns for accumulating all rows
        for (0..leaf_count) |_| {
            all_columns.append(self.allocator, nested_mod.FlatColumn.init()) catch return error.OutOfMemory;
        }

        // Flatten each row and append to columns
        for (rows) |row_value| {
            var row_columns = nested_mod.flattenValue(self.allocator, node, row_value) catch return error.OutOfMemory;
            defer row_columns.deinit();

            for (row_columns.columns.items, 0..) |col, i| {
                for (col.values.items) |v| {
                    all_columns.items[i].values.append(self.allocator, v) catch return error.OutOfMemory;
                }
                for (col.def_levels.items) |d| {
                    all_columns.items[i].def_levels.append(self.allocator, d) catch return error.OutOfMemory;
                }
                for (col.rep_levels.items) |r| {
                    all_columns.items[i].rep_levels.append(self.allocator, r) catch return error.OutOfMemory;
                }
            }
        }

        // Build paths for each leaf column
        var leaf_paths: std.ArrayList([]const []const u8) = .empty;
        defer {
            for (leaf_paths.items) |path| {
                for (path) |segment| {
                    self.allocator.free(segment);
                }
                self.allocator.free(path);
            }
            leaf_paths.deinit(self.allocator);
        }
        self.buildLeafPaths(&leaf_paths, col_def.name, node) catch return error.OutOfMemory;

        // Compute levels for each leaf column (each leaf has its own max levels based on path)
        const leaf_levels = node.computeLeafLevels(self.allocator) catch return error.OutOfMemory;
        defer self.allocator.free(leaf_levels);

        // Write each physical column
        const base_physical_idx = self.getPhysicalColumnIndex(column_index, 0) catch return error.InvalidColumnIndex;

        for (all_columns.items, 0..) |*col, leaf_idx| {
            const physical_idx = base_physical_idx + leaf_idx;

            // Get path for this leaf
            const path = if (leaf_idx < leaf_paths.items.len) leaf_paths.items[leaf_idx] else &[_][]const u8{col_def.name};

            if (col.values.items.len == 0) {
                // Empty column - create minimal metadata with zero values
                const path_copy = self.allocator.alloc([]const u8, path.len) catch return error.OutOfMemory;
                for (path, 0..) |seg, j| {
                    path_copy[j] = self.allocator.dupe(u8, seg) catch return error.OutOfMemory;
                }

                const encodings = self.allocator.alloc(format.Encoding, 1) catch return error.OutOfMemory;
                encodings[0] = .plain;

                self.column_chunks[physical_idx] = .{
                    .file_path = null,
                    .file_offset = self.current_offset,
                    .meta_data = .{
                        .type_ = .int32, // Default type for empty column
                        .encodings = encodings,
                        .path_in_schema = path_copy,
                        .codec = col_def.codec,
                        .num_values = 0,
                        .total_uncompressed_size = 0,
                        .total_compressed_size = 0,
                        .data_page_offset = self.current_offset,
                        .index_page_offset = null,
                        .dictionary_page_offset = null,
                        .statistics = null,
                    },
                };
                self.columns_written[physical_idx] = true;
                continue;
            }

            // Get levels for this specific leaf column
            const levels = if (leaf_idx < leaf_levels.len) leaf_levels[leaf_idx] else schema_mod.SchemaNode.Levels{ .max_def = 0, .max_rep = 0 };

            // Write the column chunk using the generic Value-based writer
            const writer = self.getWriter();
            var result = column_writer.writeColumnChunkFromValues(
                self.allocator,
                writer,
                path,
                col.values.items,
                col.def_levels.items,
                col.rep_levels.items,
                levels.max_def,
                levels.max_rep,
                self.current_offset,
                col_def.codec,
                .{},
            ) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                error.WriteError => return error.WriteError,
                else => return error.WriteError,
            };

            try self.flushWriter();

            // Store the column chunk metadata
            self.column_chunks[physical_idx] = .{
                .file_path = null,
                .file_offset = result.file_offset,
                .meta_data = result.metadata,
            };

            self.columns_written[physical_idx] = true;
            self.current_offset = std.math.add(i64, self.current_offset, try safe.castTo(i64, result.total_bytes)) catch return error.IntegerOverflow;

            // Don't call result.deinit - we're keeping the metadata
            _ = &result;
        }

        // Set row count
        if (self.num_rows) |existing| {
            if (existing != (safe.castTo(i64, rows.len) catch return error.RowCountMismatch)) {
                return error.RowCountMismatch;
            }
        } else {
            if (rows.len > std.math.maxInt(i64)) return error.TooManyRows;
            self.num_rows = try safe.castTo(i64, rows.len);
        }

        self.state = .writing_columns;
    }

    /// Build paths_in_schema for each leaf column in a SchemaNode tree
    fn buildLeafPaths(
        self: *Self,
        paths: *std.ArrayList([]const []const u8),
        root_name: []const u8,
        node: *const SchemaNode,
    ) !void {
        var current_path: std.ArrayList([]const u8) = .empty;
        defer current_path.deinit(self.allocator);

        try current_path.append(self.allocator, root_name);
        try self.buildLeafPathsRecursive(paths, &current_path, node);
    }

    fn buildLeafPathsRecursive(
        self: *Self,
        paths: *std.ArrayList([]const []const u8),
        current_path: *std.ArrayList([]const u8),
        node: *const SchemaNode,
    ) !void {
        switch (node.*) {
            .optional => |child| {
                try self.buildLeafPathsRecursive(paths, current_path, child);
            },
            .list => |element| {
                try current_path.append(self.allocator, "list");
                defer _ = current_path.pop();
                try current_path.append(self.allocator, "element");
                defer _ = current_path.pop();
                try self.buildLeafPathsRecursive(paths, current_path, element);
            },
            .map => |m| {
                try current_path.append(self.allocator, "key_value");

                // Key path
                try current_path.append(self.allocator, "key");
                try self.buildLeafPathsRecursive(paths, current_path, m.key);
                _ = current_path.pop();

                // Value path
                try current_path.append(self.allocator, "value");
                try self.buildLeafPathsRecursive(paths, current_path, m.value);
                _ = current_path.pop();

                _ = current_path.pop();
            },
            .struct_ => |s| {
                for (s.fields) |f| {
                    try current_path.append(self.allocator, f.name);
                    try self.buildLeafPathsRecursive(paths, current_path, f.node);
                    _ = current_path.pop();
                }
            },
            // Primitives are leaves - save the current path
            .boolean, .int32, .int64, .float, .double, .byte_array, .fixed_len_byte_array => {
                // Duplicate the path
                const path = try self.allocator.alloc([]const u8, current_path.items.len);
                for (current_path.items, 0..) |segment, i| {
                    path[i] = try self.allocator.dupe(u8, segment);
                }
                try paths.append(self.allocator, path);
            },
        }
    }

    fn validateMapColumnWrite(self: *Self, column_index: usize, expected_key_type: format.PhysicalType, expected_value_type: format.PhysicalType) WriterError!void {
        if (self.state == .closed) return error.InvalidState;
        if (column_index >= self.columns.len) return error.InvalidColumnIndex;

        const col_def = self.columns[column_index];
        if (!col_def.is_map) return error.TypeMismatch;
        if (col_def.type_ != expected_key_type) return error.TypeMismatch;
        if (col_def.map_value_type != expected_value_type) return error.TypeMismatch;

        const key_physical_idx = self.getPhysicalColumnIndex(column_index, 0) catch return error.InvalidColumnIndex;
        if (self.columns_written[key_physical_idx]) return error.ColumnAlreadyWritten;

        self.state = .writing_columns;
    }

    /// Finalize and close the Parquet writer.
    /// Finalize the file: write footer and close the target.
    pub fn close(self: *Self) WriterError!void {
        if (self.state == .closed) return error.InvalidState;

        try self.writeFooterToWriter(self.getWriter());
        try self.flushWriter();
        self.target_writer.target.close() catch return error.WriteError;

        self.state = .closed;
    }

    /// Get the written buffer data after close (buffer backend only).
    /// Only works when _to_owned_slice_fn is set by convenience constructors.
    /// Caller owns the returned slice and must free it with the allocator.
    pub fn toOwnedSlice(self: *Self) WriterError![]u8 {
        if (self.state != .closed) return error.InvalidState;
        const func = self._to_owned_slice_fn orelse return error.InvalidState;
        return func(self._to_owned_slice_ctx.?) catch return error.OutOfMemory;
    }

    /// Build and serialize the footer metadata. Caller must free the returned bytes.
    fn buildSerializedFooter(self: *Self) WriterError![]u8 {
        // Verify all physical columns were written and have valid chunks
        for (self.columns_written, self.column_chunks) |written, maybe_chunk| {
            if (!written) return error.ColumnNotWritten;
            if (maybe_chunk == null) return error.ColumnNotWritten;
        }

        // Build row group with all physical columns
        const column_chunks = self.allocator.alloc(format.ColumnChunk, self.num_physical_columns) catch return error.OutOfMemory;
        defer self.allocator.free(column_chunks);

        var total_byte_size: i64 = 0;
        for (self.column_chunks, 0..) |maybe_chunk, i| {
            const chunk = maybe_chunk.?;
            column_chunks[i] = chunk;
            if (chunk.meta_data) |md| {
                total_byte_size = std.math.add(i64, total_byte_size, md.total_compressed_size) catch return error.IntegerOverflow;
            }
        }

        const row_group = format.RowGroup{
            .columns = column_chunks,
            .total_byte_size = total_byte_size,
            .num_rows = self.num_rows orelse 0,
            .sorting_columns = null,
            .file_offset = 4, // After PAR1
            .total_compressed_size = total_byte_size,
            .ordinal = 0,
        };

        // Build schema
        const schema = self.buildSchema() catch return error.OutOfMemory;
        defer self.allocator.free(schema);

        // Build row groups array
        const row_groups = self.allocator.alloc(format.RowGroup, 1) catch return error.OutOfMemory;
        defer self.allocator.free(row_groups);
        row_groups[0] = row_group;

        // Build GeoParquet metadata if there are geospatial columns
        const geo_metadata = self.buildGeoParquetMetadata();
        defer if (geo_metadata) |gm| {
            self.allocator.free(gm);
        };

        // Build key-value metadata array if we have geo metadata
        var key_value_slice: ?[]format.KeyValue = null;
        if (geo_metadata) |gm| {
            const kv_array = self.allocator.alloc(format.KeyValue, 1) catch null;
            if (kv_array) |kv| {
                kv[0] = .{
                    .key = "geo",
                    .value = gm,
                };
                key_value_slice = kv;
            }
        }
        defer if (key_value_slice) |kv| self.allocator.free(kv);

        // Build file metadata
        const metadata = format.FileMetaData{
            .version = 1,
            .schema = schema,
            .num_rows = self.num_rows orelse 0,
            .row_groups = row_groups,
            .key_value_metadata = key_value_slice,
            .created_by = "zig-parquet",
        };

        // Serialize footer
        var thrift_writer = thrift.CompactWriter.init(self.allocator);
        defer thrift_writer.deinit();

        metadata.serialize(&thrift_writer) catch return error.OutOfMemory;

        // Return a copy of the serialized bytes (caller owns)
        return self.allocator.dupe(u8, thrift_writer.getWritten()) catch return error.OutOfMemory;
    }

    /// Build GeoParquet metadata JSON for geospatial columns.
    /// Returns null if there are no geospatial columns.
    fn buildGeoParquetMetadata(self: *Self) ?[]u8 {
        // Count geospatial columns
        var geo_count: usize = 0;
        for (self.columns) |col| {
            if (col.logical_type) |lt| {
                switch (lt) {
                    .geometry, .geography => geo_count += 1,
                    else => {},
                }
            }
        }

        if (geo_count == 0) return null;

        // Build GeoColumnInfo for each geospatial column
        const geo_columns = self.allocator.alloc(geo.GeoColumnInfo, geo_count) catch return null;
        defer self.allocator.free(geo_columns);

        var idx: usize = 0;
        var primary_column: ?[]const u8 = null;

        for (self.columns, 0..) |col, col_idx| {
            if (col.logical_type) |lt| {
                switch (lt) {
                    .geometry => |g| {
                        // Get bbox from the statistics builder if available
                        const bbox = if (self.geo_stats_builders.get(col_idx)) |builder|
                            builder.bbox_builder.build()
                        else
                            null;

                        geo_columns[idx] = .{
                            .name = col.name,
                            .crs = g.crs,
                            .is_geography = false,
                            .algorithm = null,
                            .bbox = bbox,
                            .geometry_types = null,
                        };
                        if (primary_column == null) primary_column = col.name;
                        idx += 1;
                    },
                    .geography => |g| {
                        // Get bbox from the statistics builder if available
                        const bbox = if (self.geo_stats_builders.get(col_idx)) |builder|
                            builder.bbox_builder.build()
                        else
                            null;

                        geo_columns[idx] = .{
                            .name = col.name,
                            .crs = g.crs,
                            .is_geography = true,
                            .algorithm = g.algorithm,
                            .bbox = bbox,
                            .geometry_types = null,
                        };
                        if (primary_column == null) primary_column = col.name;
                        idx += 1;
                    },
                    else => {},
                }
            }
        }

        return geo.generateMetadata(self.allocator, geo_columns, primary_column) catch null;
    }

    /// Write footer bytes to output (shared logic for writer and target)
    fn writeFooterBytes(footer_bytes: []const u8, writeAllFn: anytype) WriterError!void {
        // Write footer
        writeAllFn(footer_bytes) catch return error.WriteError;

        // Write footer length (4 bytes, little-endian)
        var len_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &len_buf, try safe.castTo(u32, footer_bytes.len), .little);
        writeAllFn(&len_buf) catch return error.WriteError;

        // Write magic bytes
        writeAllFn(format.PARQUET_MAGIC) catch return error.WriteError;
    }

    /// Internal: write footer to a generic writer
    fn writeFooterToWriter(self: *Self, writer: *std.Io.Writer) WriterError!void {
        const footer_bytes = try self.buildSerializedFooter();
        defer self.allocator.free(footer_bytes);

        writer.writeAll(footer_bytes) catch return error.WriteError;

        var len_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &len_buf, try safe.castTo(u32, footer_bytes.len), .little);
        writer.writeAll(&len_buf) catch return error.WriteError;

        writer.writeAll(format.PARQUET_MAGIC) catch return error.WriteError;
    }

    fn validateColumnWrite(self: *Self, column_index: usize, expected_type: format.PhysicalType, is_nullable_call: bool) WriterError!void {
        if (self.state == .closed) return error.InvalidState;
        if (column_index >= self.columns.len) return error.InvalidColumnIndex;

        const col_def = self.columns[column_index];
        // Don't allow writing to struct columns with regular methods
        if (col_def.is_struct) return error.TypeMismatch;

        const physical_idx = self.getPhysicalColumnIndex(column_index, 0) catch return error.InvalidColumnIndex;
        if (self.columns_written[physical_idx]) return error.ColumnAlreadyWritten;

        if (col_def.type_ != expected_type) return error.TypeMismatch;

        // For nullable calls, column must be optional
        // For non-nullable calls, column can be either (we just write all values as defined)
        if (is_nullable_call and !col_def.optional) return error.TypeMismatch;

        self.state = .writing_columns;
    }

    fn recordColumnWrite(self: *Self, physical_column_index: usize, result: *column_writer.ColumnChunkResult, num_values: usize) WriterError!void {
        self.columns_written[physical_column_index] = true;
        self.current_offset += try safe.castTo(i64, result.total_bytes);

        // Create column chunk
        self.column_chunks[physical_column_index] = .{
            .file_path = null,
            .file_offset = result.file_offset,
            .meta_data = result.metadata,
        };

        // Verify/set row count
        if (self.num_rows) |existing| {
            if (existing != (safe.castTo(i64, num_values) catch return error.RowCountMismatch)) {
                return error.RowCountMismatch;
            }
        } else {
            if (num_values > std.math.maxInt(i64)) return error.TooManyRows;
            self.num_rows = try safe.castTo(i64, num_values);
        }
    }

    /// Count schema elements needed for a SchemaNode
    pub fn countSchemaElements(node: *const SchemaNode) usize {
        return switch (node.*) {
            .optional => |child| countSchemaElements(child),
            .list => |element| 2 + countSchemaElements(element), // container + list + element(s)
            .map => |m| 2 + countSchemaElements(m.key) + countSchemaElements(m.value), // container + key_value + key + value
            .struct_ => |s| {
                var count: usize = 1; // group element
                for (s.fields) |f| {
                    count += countSchemaElements(f.node);
                }
                return count;
            },
            // Primitives: 1 element each
            .boolean, .int32, .int64, .float, .double, .byte_array, .fixed_len_byte_array => 1,
        };
    }

    /// Generate schema elements from a SchemaNode recursively
    pub fn generateSchemaFromNodeStatic(
        schema: []format.SchemaElement,
        start_idx: usize,
        name: []const u8,
        node: *const SchemaNode,
        rep_type: format.RepetitionType,
    ) WriterError!usize {
        var idx = start_idx;

        switch (node.*) {
            .optional => |child| {
                return generateSchemaFromNodeStatic(schema, idx, name, child, .optional);
            },
            .list => |element| {
                const num_element_children = try countStructChildren(element);

                schema[idx] = .{
                    .type_ = null,
                    .type_length = null,
                    .repetition_type = rep_type,
                    .name = name,
                    .num_children = 1,
                    .converted_type = format.ConvertedType.LIST,
                    .scale = null,
                    .precision = null,
                    .field_id = null,
                    .logical_type = null,
                };
                idx += 1;

                schema[idx] = .{
                    .type_ = null,
                    .type_length = null,
                    .repetition_type = .repeated,
                    .name = "list",
                    .num_children = num_element_children,
                    .converted_type = null,
                    .scale = null,
                    .precision = null,
                    .field_id = null,
                    .logical_type = null,
                };
                idx += 1;

                idx = try generateSchemaFromNodeStatic(schema, idx, "element", element, .required);
            },
            .map => |m| {
                schema[idx] = .{
                    .type_ = null,
                    .type_length = null,
                    .repetition_type = rep_type,
                    .name = name,
                    .num_children = 1,
                    .converted_type = format.ConvertedType.MAP,
                    .scale = null,
                    .precision = null,
                    .field_id = null,
                    .logical_type = null,
                };
                idx += 1;

                schema[idx] = .{
                    .type_ = null,
                    .type_length = null,
                    .repetition_type = .repeated,
                    .name = "key_value",
                    .num_children = 2,
                    .converted_type = null,
                    .scale = null,
                    .precision = null,
                    .field_id = null,
                    .logical_type = null,
                };
                idx += 1;

                idx = try generateSchemaFromNodeStatic(schema, idx, "key", m.key, .required);
                idx = try generateSchemaFromNodeStatic(schema, idx, "value", m.value, .optional);
            },
            .struct_ => |s| {
                schema[idx] = .{
                    .type_ = null,
                    .type_length = null,
                    .repetition_type = rep_type,
                    .name = name,
                    .num_children = try safe.castTo(i32, s.fields.len),
                    .converted_type = null,
                    .scale = null,
                    .precision = null,
                    .field_id = null,
                    .logical_type = null,
                };
                idx += 1;

                for (s.fields) |f| {
                    idx = try generateSchemaFromNodeStatic(schema, idx, f.name, f.node, .required);
                }
            },
            .boolean => |p| {
                schema[idx] = makePrimitiveElement(name, .boolean, null, rep_type, p.logical);
                idx += 1;
            },
            .int32 => |p| {
                schema[idx] = makePrimitiveElement(name, .int32, null, rep_type, p.logical);
                idx += 1;
            },
            .int64 => |p| {
                schema[idx] = makePrimitiveElement(name, .int64, null, rep_type, p.logical);
                idx += 1;
            },
            .float => |p| {
                schema[idx] = makePrimitiveElement(name, .float, null, rep_type, p.logical);
                idx += 1;
            },
            .double => |p| {
                schema[idx] = makePrimitiveElement(name, .double, null, rep_type, p.logical);
                idx += 1;
            },
            .byte_array => |p| {
                schema[idx] = makePrimitiveElement(name, .byte_array, null, rep_type, p.logical);
                idx += 1;
            },
            .fixed_len_byte_array => |f| {
                schema[idx] = makePrimitiveElement(name, .fixed_len_byte_array, try safe.castTo(i32, f.len), rep_type, f.logical);
                idx += 1;
            },
        }

        return idx;
    }

    pub fn countStructChildren(node: *const SchemaNode) WriterError!i32 {
        return switch (node.*) {
            .optional => |child| countStructChildren(child),
            .struct_ => |s| safe.castTo(i32, s.fields.len),
            else => 1,
        };
    }

    pub fn makePrimitiveElement(
        name: []const u8,
        type_: format.PhysicalType,
        type_length: ?i32,
        rep_type: format.RepetitionType,
        logical_type: ?schema_mod.LogicalType,
    ) format.SchemaElement {
        // Extract scale/precision for decimal types
        var scale: ?i32 = null;
        var precision: ?i32 = null;
        if (logical_type) |lt| {
            if (lt == .decimal) {
                scale = lt.decimal.scale;
                precision = lt.decimal.precision;
            }
        }

        // Set converted_type for older Parquet readers that don't support LogicalType
        const converted_type: ?i32 = if (logical_type) |lt| blk: {
            break :blk switch (lt) {
                .decimal => format.ConvertedType.DECIMAL,
                .date => format.ConvertedType.DATE,
                .time => |t| if (t.unit == .millis) format.ConvertedType.TIME_MILLIS else format.ConvertedType.TIME_MICROS,
                .timestamp => |t| if (t.unit == .millis) format.ConvertedType.TIMESTAMP_MILLIS else format.ConvertedType.TIMESTAMP_MICROS,
                .string => format.ConvertedType.UTF8,
                .json => format.ConvertedType.JSON,
                .bson => format.ConvertedType.BSON,
                .enum_ => format.ConvertedType.ENUM,
                .uuid => null, // No converted_type for UUID (new logical type only)
                else => null,
            };
        } else null;

        return .{
            .type_ = type_,
            .type_length = type_length,
            .repetition_type = rep_type,
            .name = name,
            .num_children = null,
            .converted_type = converted_type,
            .scale = scale,
            .precision = precision,
            .field_id = null,
            .logical_type = logical_type,
        };
    }

    fn buildSchema(self: *Self) ![]format.SchemaElement {
        // Calculate total schema elements needed
        // For flat columns: 1 element
        // For list columns: 3 elements (container, list, element)
        // For struct columns: 1 (group) + N (fields)
        // For map columns: 4 elements (container, key_value, key, value)
        // For schema_node: count recursively
        var total_elements: usize = 1; // root
        for (self.columns) |col| {
            if (col.schema_node) |node| {
                total_elements += countSchemaElements(node);
            } else if (col.is_list) {
                total_elements += 3;
            } else if (col.is_struct) {
                const fields = col.struct_fields orelse continue;
                total_elements += 1 + fields.len; // group + fields
            } else if (col.is_map) {
                total_elements += 4; // container + key_value + key + value
            } else {
                total_elements += 1;
            }
        }

        const schema = try self.allocator.alloc(format.SchemaElement, total_elements);

        // Root element
        schema[0] = .{
            .type_ = null,
            .type_length = null,
            .repetition_type = null,
            .name = "schema",
            .num_children = try safe.castTo(i32, self.columns.len),
            .converted_type = null,
            .scale = null,
            .precision = null,
            .field_id = null,
        };

        // Column elements
        var idx: usize = 1;
        for (self.columns) |col| {
            if (col.schema_node) |node| {
                // Generate schema elements from SchemaNode
                idx = try generateSchemaFromNodeStatic(schema, idx, col.name, node, .required);
            } else if (col.is_list) {
                // List column: 3-level structure
                // Level 1: Container group (OPTIONAL)
                schema[idx] = .{
                    .type_ = null,
                    .type_length = null,
                    .repetition_type = if (col.optional) .optional else .required,
                    .name = col.name,
                    .num_children = 1,
                    .converted_type = format.ConvertedType.LIST,
                    .scale = null,
                    .precision = null,
                    .field_id = null,
                    .logical_type = null,
                };
                idx += 1;

                // Level 2: Repeated group (REPEATED)
                schema[idx] = .{
                    .type_ = null,
                    .type_length = null,
                    .repetition_type = .repeated,
                    .name = "list",
                    .num_children = 1,
                    .converted_type = null,
                    .scale = null,
                    .precision = null,
                    .field_id = null,
                    .logical_type = null,
                };
                idx += 1;

                // Level 3: Element (OPTIONAL or REQUIRED)
                schema[idx] = .{
                    .type_ = col.type_,
                    .type_length = col.type_length,
                    .repetition_type = if (col.element_optional) .optional else .required,
                    .name = "element",
                    .num_children = null,
                    .converted_type = null,
                    .scale = null,
                    .precision = null,
                    .field_id = null,
                    .logical_type = col.logical_type,
                };
                idx += 1;
            } else if (col.is_struct) {
                // Struct column: group + child fields
                const fields = col.struct_fields orelse continue;

                // Parent group element
                schema[idx] = .{
                    .type_ = null,
                    .type_length = null,
                    .repetition_type = if (col.optional) .optional else .required,
                    .name = col.name,
                    .num_children = try safe.castTo(i32, fields.len),
                    .converted_type = null,
                    .scale = null,
                    .precision = null,
                    .field_id = null,
                    .logical_type = null,
                };
                idx += 1;

                // Child field elements
                for (fields) |field| {
                    schema[idx] = .{
                        .type_ = field.type_,
                        .type_length = null,
                        .repetition_type = if (field.optional) .optional else .required,
                        .name = field.name,
                        .num_children = null,
                        .converted_type = null,
                        .scale = null,
                        .precision = null,
                        .field_id = null,
                        .logical_type = field.logical_type,
                    };
                    idx += 1;
                }
            } else if (col.is_map) {
                // Map column: 4-level structure
                // Level 1: Container group (OPTIONAL, MAP converted_type)
                schema[idx] = .{
                    .type_ = null,
                    .type_length = null,
                    .repetition_type = if (col.optional) .optional else .required,
                    .name = col.name,
                    .num_children = 1,
                    .converted_type = format.ConvertedType.MAP,
                    .scale = null,
                    .precision = null,
                    .field_id = null,
                    .logical_type = null,
                };
                idx += 1;

                // Level 2: key_value group (REPEATED)
                schema[idx] = .{
                    .type_ = null,
                    .type_length = null,
                    .repetition_type = .repeated,
                    .name = "key_value",
                    .num_children = 2,
                    .converted_type = null,
                    .scale = null,
                    .precision = null,
                    .field_id = null,
                    .logical_type = null,
                };
                idx += 1;

                // Level 3: key (REQUIRED)
                schema[idx] = .{
                    .type_ = col.type_, // key type
                    .type_length = col.type_length,
                    .repetition_type = .required, // keys are always required
                    .name = "key",
                    .num_children = null,
                    .converted_type = null,
                    .scale = null,
                    .precision = null,
                    .field_id = null,
                    .logical_type = null,
                };
                idx += 1;

                // Level 4: value (OPTIONAL or REQUIRED)
                schema[idx] = .{
                    .type_ = col.map_value_type,
                    .type_length = null,
                    .repetition_type = if (col.map_value_optional) .optional else .required,
                    .name = "value",
                    .num_children = null,
                    .converted_type = null,
                    .scale = null,
                    .precision = null,
                    .field_id = null,
                    .logical_type = null,
                };
                idx += 1;
            } else {
                // Flat column: single element
                // Determine converted_type: use explicit value if set, otherwise derive from logical_type
                const converted_type: ?i32 = if (col.converted_type) |ct|
                    ct
                else if (col.logical_type) |lt|
                    switch (lt) {
                        .decimal => format.ConvertedType.DECIMAL,
                        .date => format.ConvertedType.DATE,
                        .time => |t| if (t.unit == .millis) format.ConvertedType.TIME_MILLIS else format.ConvertedType.TIME_MICROS,
                        .timestamp => |t| if (t.unit == .millis) format.ConvertedType.TIMESTAMP_MILLIS else format.ConvertedType.TIMESTAMP_MICROS,
                        .string => format.ConvertedType.UTF8,
                        .json => format.ConvertedType.JSON,
                        .bson => format.ConvertedType.BSON,
                        .enum_ => format.ConvertedType.ENUM,
                        .uuid => null, // No converted_type for UUID (new logical type only)
                        else => null,
                    }
                else
                    null;

                // Extract scale/precision for DECIMAL type
                var scale: ?i32 = null;
                var precision: ?i32 = null;
                if (col.logical_type) |lt| {
                    if (lt == .decimal) {
                        scale = lt.decimal.scale;
                        precision = lt.decimal.precision;
                    }
                }

                schema[idx] = .{
                    .type_ = col.type_,
                    .type_length = col.type_length,
                    .repetition_type = if (col.optional) .optional else .required,
                    .name = col.name,
                    .num_children = null,
                    .converted_type = converted_type,
                    .scale = scale,
                    .precision = precision,
                    .field_id = null,
                    .logical_type = col.logical_type,
                };
                idx += 1;
            }
        }

        return schema;
    }

    fn mapColumnError(e: column_writer.ColumnWriteError) WriterError {
        return switch (e) {
            error.OutOfMemory => error.OutOfMemory,
            error.WriteError => error.WriteError,
            error.InvalidFixedLength => error.InvalidFixedLength,
            error.CompressionError => error.CompressionError,
            error.UnsupportedCompression => error.UnsupportedCompression,
            error.IntegerOverflow => error.IntegerOverflow,
            error.ValueTooLarge => error.ValueTooLarge,
            error.UnsupportedEncoding => error.UnsupportedEncoding,
        };
    }
};

// Tests
test "Writer basic i64 column" {
    const allocator = std.testing.allocator;

    // Create temp file
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file = try tmp_dir.dir.createFile("test.parquet", .{ .read = true });
    defer file.close();

    const columns = [_]ColumnDef{
        .{ .name = "id", .type_ = .int64, .optional = false },
    };

    const api_writer = @import("../api/zig/writer.zig");
    var writer = try api_writer.writeToFile(allocator, file, &columns);
    defer writer.deinit();

    const values = [_]i64{ 1, 2, 3, 4, 5 };
    try writer.writeColumn(i64, 0, &values);

    try writer.close();

    // Verify file has content
    try file.seekTo(0);
    var magic: [4]u8 = undefined;
    _ = try file.read(&magic);
    try std.testing.expectEqualStrings("PAR1", &magic);
}

test "Writer nullable column" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file = try tmp_dir.dir.createFile("test_nullable.parquet", .{});
    defer file.close();

    const columns = [_]ColumnDef{
        .{ .name = "value", .type_ = .double, .optional = true },
    };

    const api_writer2 = @import("../api/zig/writer.zig");
    var writer = try api_writer2.writeToFile(allocator, file, &columns);
    defer writer.deinit();

    const values = [_]?f64{ 1.5, null, 3.5 };
    try writer.writeColumnNullable(f64, 0, &values);

    try writer.close();
}

test "Writer multiple columns" {
    const allocator = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file = try tmp_dir.dir.createFile("test_multi.parquet", .{});
    defer file.close();

    const columns = [_]ColumnDef{
        .{ .name = "id", .type_ = .int64, .optional = false },
        .{ .name = "name", .type_ = .byte_array, .optional = true },
        .{ .name = "value", .type_ = .double, .optional = true },
    };

    const api_writer3 = @import("../api/zig/writer.zig");
    var writer = try api_writer3.writeToFile(allocator, file, &columns);
    defer writer.deinit();

    const ids = [_]i64{ 1, 2, 3 };
    try writer.writeColumn(i64, 0, &ids);

    const names = [_]?[]const u8{ "alice", null, "charlie" };
    try writer.writeColumnNullable([]const u8, 1, &names);

    const values = [_]?f64{ 1.5, 2.5, null };
    try writer.writeColumnNullable(f64, 2, &values);

    try writer.close();
}
