//! Dynamic Row Reader
//!
//! Schema-agnostic Parquet reader that reads files into dynamic Value/Row types
//! without requiring compile-time type information.
//!
//! This is inspired by parquet-go's Row/Value API and enables:
//! - Reading files without knowing the schema at compile time
//! - Generic validation tools (like `pq validate`)
//! - Schema evolution and migration utilities
//!
//! ## Example
//! ```zig
//! var reader = try DynamicReader.init(allocator, file);
//! defer reader.deinit();
//!
//! for (0..reader.getNumRowGroups()) |rg_idx| {
//!     const rows = try reader.readAllRows(rg_idx);
//!     defer for (rows) |row| row.deinit();
//!
//!     for (rows) |row| {
//!         // row.values[i] contains Value for column i
//!     }
//! }
//! ```

const std = @import("std");
const safe = @import("safe.zig");
const format = @import("format.zig");
const leaf_decoder = @import("leaf_decoder.zig");
const value_mod = @import("value.zig");
const seekable_reader = @import("seekable_reader.zig");
const parquet_reader = @import("parquet_reader.zig");
const errors_mod = @import("errors.zig");
const schema_mod = @import("schema.zig");
const nested_mod = @import("nested.zig");
const row_ranges_mod = @import("row_ranges.zig");
const page_filter_mod = @import("page_filter.zig");
const page_index_reader_mod = @import("page_index_reader.zig");
const page_range_reader_mod = @import("page_range_reader.zig");

pub const Value = value_mod.Value;
pub const Row = value_mod.Row;
pub const SeekableReader = seekable_reader.SeekableReader;
pub const BackendCleanup = parquet_reader.BackendCleanup;
pub const ChecksumOptions = parquet_reader.ChecksumOptions;
pub const RowRanges = row_ranges_mod.RowRanges;
pub const ColumnFilter = page_filter_mod.ColumnFilter;

/// Options for DynamicReader initialization
pub const DynamicReaderOptions = struct {
    /// Checksum validation options
    checksum: ChecksumOptions = .{},
};

/// Error type for dynamic reader operations.
/// Composed from categorized error sets — no more `SchemaParseError` coalescing.
pub const DynamicReaderError = errors_mod.DynamicReaderError;


/// Safely cast i64 row count to usize, validating non-negative.
fn safeRowCount(count: i64) error{InvalidRowCount}!usize {
    return safe.cast(count) catch return error.InvalidRowCount;
}


/// Schema-agnostic Parquet reader
pub const DynamicReader = struct {
    allocator: std.mem.Allocator,
    source: SeekableReader,
    metadata: format.FileMetaData,
    footer_data: []u8,
    checksum_options: ChecksumOptions,
    _backend_cleanup: ?BackendCleanup = null,
    /// Storage of the read in progress (see `value_mod.RowStorage`): rows'
    /// `values` arrays, nested values and byte payloads are allocated in its
    /// arena, and each row built holds a reference to it. Set by
    /// `beginStorage` for the duration of one read call.
    storage: ?*value_mod.RowStorage = null,

    const Self = @This();

    const TopColInfo = struct {
        leaf_start: usize,
        leaf_count: usize,
        is_nested: bool,
        schema_node: *const schema_mod.SchemaNode,
    };

    /// Get the SeekableReader interface.
    pub fn getSource(self: *const Self) SeekableReader {
        return self.source;
    }

    /// Initialize from a SeekableReader. The caller manages backend lifetime
    /// unless _backend_cleanup is set (by convenience constructors in api/zig/).
    pub fn initFromSeekable(allocator: std.mem.Allocator, source: SeekableReader, options: DynamicReaderOptions) DynamicReaderError!Self {
        const info = try parquet_reader.parseFooter(allocator, source);
        return Self{
            .allocator = allocator,
            .source = source,
            .metadata = info.metadata,
            .footer_data = info.footer_data,
            .checksum_options = options.checksum,
        };
    }

    pub fn deinit(self: *Self) void {
        // Free schema element names
        for (self.metadata.schema) |elem| {
            if (elem.name.len > 0) {
                self.allocator.free(elem.name);
            }
        }
        self.allocator.free(self.metadata.schema);

        // Free row groups
        for (self.metadata.row_groups) |rg| {
            for (rg.columns) |col| {
                if (col.file_path) |fp| self.allocator.free(fp);
                if (col.meta_data) |meta| {
                    self.allocator.free(meta.encodings);
                    for (meta.path_in_schema) |path| {
                        self.allocator.free(path);
                    }
                    self.allocator.free(meta.path_in_schema);
                    if (meta.statistics) |stats| {
                        if (stats.max) |m| self.allocator.free(m);
                        if (stats.min) |m| self.allocator.free(m);
                        if (stats.max_value) |m| self.allocator.free(m);
                        if (stats.min_value) |m| self.allocator.free(m);
                    }
                    // Free geospatial statistics if present
                    if (meta.geospatial_statistics) |geo_stats| {
                        if (geo_stats.geospatial_types) |gt| self.allocator.free(gt);
                    }
                }
            }
            if (rg.sorting_columns) |sc| self.allocator.free(sc);
            self.allocator.free(rg.columns);
        }
        self.allocator.free(self.metadata.row_groups);

        // Free key-value metadata
        if (self.metadata.key_value_metadata) |kvs| {
            for (kvs) |kv| {
                self.allocator.free(kv.key);
                if (kv.value) |v| self.allocator.free(v);
            }
            self.allocator.free(kvs);
        }

        // Free created_by
        if (self.metadata.created_by) |cb| {
            self.allocator.free(cb);
        }

        self.allocator.free(self.footer_data);

        if (self._backend_cleanup) |cleanup| {
            cleanup.deinit_fn(cleanup.ptr, self.allocator);
        }
    }

    /// Get the schema
    pub fn getSchema(self: *const Self) []const format.SchemaElement {
        return self.metadata.schema;
    }

    /// Get the number of row groups
    pub fn getNumRowGroups(self: *const Self) usize {
        return self.metadata.row_groups.len;
    }

    /// Get the number of rows in a specific row group
    pub fn getRowGroupNumRows(self: *const Self, row_group_index: usize) i64 {
        if (row_group_index >= self.metadata.row_groups.len) return 0;
        return self.metadata.row_groups[row_group_index].num_rows;
    }

    /// Get total number of rows across all row groups
    pub fn getTotalNumRows(self: *const Self) i64 {
        return self.metadata.num_rows;
    }

    /// Get the number of leaf columns
    pub fn getNumColumns(self: *const Self) usize {
        var count: usize = 0;
        for (self.metadata.schema) |elem| {
            if (elem.type_ != null and elem.num_children == null) {
                count += 1;
            }
        }
        return count;
    }

    /// Get the schema element for a leaf column by column index
    pub fn getLeafSchemaElement(self: *const Self, col_idx: usize) ?format.SchemaElement {
        var count: usize = 0;
        for (self.metadata.schema) |elem| {
            if (elem.type_ != null and elem.num_children == null) {
                if (count == col_idx) {
                    return elem;
                }
                count += 1;
            }
        }
        return null;
    }

    /// Get the logical type for a column, if any
    pub fn getColumnLogicalType(self: *const Self, col_idx: usize) ?format.LogicalType {
        const elem = self.getLeafSchemaElement(col_idx) orelse return null;
        return elem.logical_type;
    }

    /// Check if a column has a specific logical type
    pub fn isColumnType(self: *const Self, col_idx: usize, logical: format.LogicalType) bool {
        const lt = self.getColumnLogicalType(col_idx) orelse return false;
        return std.meta.eql(lt, logical);
    }

    /// Get statistics for a column in a specific row group.
    /// Returns null if the column or row group doesn't exist, or if no statistics are available.
    pub fn getColumnStatistics(
        self: *const Self,
        column_index: usize,
        row_group_index: usize,
    ) ?format.Statistics {
        if (row_group_index >= self.metadata.row_groups.len) return null;
        const rg = self.metadata.row_groups[row_group_index];
        if (column_index >= rg.columns.len) return null;
        const col = rg.columns[column_index];
        const meta = col.meta_data orelse return null;
        return meta.statistics;
    }

    /// Get column metadata for a specific column in a row group.
    /// Returns null if the column or row group doesn't exist.
    pub fn getColumnMetaData(
        self: *const Self,
        column_index: usize,
        row_group_index: usize,
    ) ?format.ColumnMetaData {
        if (row_group_index >= self.metadata.row_groups.len) return null;
        const rg = self.metadata.row_groups[row_group_index];
        if (column_index >= rg.columns.len) return null;
        return rg.columns[column_index].meta_data;
    }

    /// Streaming row iterator that advances through row groups automatically.
    /// Holds at most one row group's worth of rows in memory at a time.
    pub const RowIterator = struct {
        reader: *Self,
        current_rows: ?[]Row,
        cursor: usize,
        current_rg: usize,
        num_rgs: usize,
        col_indices: ?[]const usize,

        /// Advance to the next row. Returns a pointer to the current row,
        /// or null when all row groups have been exhausted.
        /// The returned pointer is valid until the next call to `next()` that
        /// crosses a row group boundary, or until `deinit()` is called.
        pub fn next(self: *RowIterator) !?*const Row {
            while (true) {
                if (self.current_rows) |rows| {
                    if (self.cursor < rows.len) {
                        const row = &rows[self.cursor];
                        self.cursor += 1;
                        return row;
                    }
                    self.freeCurrentRows();
                }

                if (self.current_rg >= self.num_rgs) return null;

                const rows = try self.reader.readRowsInternal(self.current_rg, self.col_indices);
                self.current_rg += 1;

                if (rows.len > 0) {
                    self.current_rows = rows;
                    self.cursor = 1;
                    return &rows[0];
                }
                self.reader.allocator.free(rows);
            }
        }

        /// Free any currently held rows.
        pub fn deinit(self: *RowIterator) void {
            self.freeCurrentRows();
        }

        fn freeCurrentRows(self: *RowIterator) void {
            if (self.current_rows) |rows| {
                for (rows) |row| row.deinit();
                self.reader.allocator.free(rows);
                self.current_rows = null;
            }
        }
    };

    /// Create a row iterator that streams through all row groups.
    /// No I/O occurs until the first call to `next()`.
    pub fn rowIterator(self: *Self) RowIterator {
        return .{
            .reader = self,
            .current_rows = null,
            .cursor = 0,
            .current_rg = 0,
            .num_rgs = self.metadata.row_groups.len,
            .col_indices = null,
        };
    }

    /// Create a row iterator with column projection.
    /// Only the specified top-level columns are read and returned.
    pub fn rowIteratorProjected(self: *Self, col_indices: []const usize) RowIterator {
        return .{
            .reader = self,
            .current_rows = null,
            .cursor = 0,
            .current_rg = 0,
            .num_rgs = self.metadata.row_groups.len,
            .col_indices = col_indices,
        };
    }

    /// Read all rows from a row group as dynamic Value types
    pub fn readAllRows(self: *Self, row_group_index: usize) ![]Row {
        return self.readRowsInternal(row_group_index, null);
    }

    /// Read rows from a row group, returning only the specified top-level columns.
    /// Column indices refer to top-level schema columns (not leaf columns).
    /// Returned rows contain values in dense order matching the projection list.
    pub fn readRowsProjected(self: *Self, row_group_index: usize, col_indices: []const usize) ![]Row {
        return self.readRowsInternal(row_group_index, col_indices);
    }

    /// Read rows from a row group, applying page-index-driven filters to skip
    /// pages whose min/max ranges cannot match the predicates.
    ///
    /// Behavior:
    ///   - Every leaf column in the row group must advertise an `OffsetIndex`;
    ///     otherwise `error.PageIndexMissing` is returned (use `readAllRows`
    ///     for unindexed files).
    ///   - Only flat schemas are supported; `error.NestedNotSupported` is
    ///     returned for nested / list / map / struct columns.
    ///   - `filters` target leaf column indices. Columns without a filter are
    ///     treated as wildcards (`full(rg_num_rows)`).
    ///   - Pages whose row ranges don't intersect the final (intersected)
    ///     `RowRanges` are never read or decompressed.
    ///
    /// The returned rows are in row-group order, restricted to the final
    /// `RowRanges`. A stats-level predicate pushdown may still leave rows that
    /// don't actually match — apply value-level predicates post-decode.
    pub fn readRowsFiltered(
        self: *Self,
        row_group_index: usize,
        filters: []const ColumnFilter,
    ) ![]Row {
        if (row_group_index >= self.metadata.row_groups.len) {
            return try self.allocator.alloc(Row, 0);
        }

        // Allocate the rows' memory in storage shared by the rows of this read.
        const storage = try self.beginStorage();
        defer self.endStorage(storage);

        const rg = &self.metadata.row_groups[row_group_index];
        const num_rows = try safeRowCount(rg.num_rows);
        const num_leaf_columns = rg.columns.len;
        if (num_rows == 0 or num_leaf_columns == 0) {
            return try self.allocator.alloc(Row, 0);
        }

        // Flat-schema-only in this first cut.
        const schema = self.metadata.schema;
        const num_top: usize = if (schema.len > 0)
            safe.castTo(usize, schema[0].num_children orelse 0) catch 0
        else
            0;
        if (num_top != 0 and num_top != num_leaf_columns) {
            return error.NestedNotSupported;
        }

        // Validate filter column indices.
        for (filters) |f| {
            if (f.column >= num_leaf_columns) return error.InvalidArgument;
        }

        // Load offset + column indexes for the whole row group.
        var pir = page_index_reader_mod.PageIndexReader.init(self.allocator, self.getSource());
        const offset_indexes = try pir.readOffsetIndexesForRowGroup(rg);
        defer page_index_reader_mod.freeOptionalIndexes(format.OffsetIndex, self.allocator, offset_indexes);
        for (offset_indexes) |oi| {
            if (oi == null) return error.PageIndexMissing;
        }
        const column_indexes = try pir.readColumnIndexesForRowGroup(rg);
        defer page_index_reader_mod.freeOptionalIndexes(format.ColumnIndex, self.allocator, column_indexes);

        // Compute final_ranges by intersecting each filter's RowRanges.
        var final_ranges = try RowRanges.full(self.allocator, num_rows);
        defer final_ranges.deinit(self.allocator);

        for (filters) |f| {
            const oi_ref = &offset_indexes[f.column].?;
            const ci_ref: ?*const format.ColumnIndex = if (column_indexes[f.column]) |*ci| ci else null;

            var per_filter = try f.evaluate(ci_ref, oi_ref, num_rows, self.allocator);
            defer per_filter.deinit(self.allocator);

            const next = try RowRanges.intersect(self.allocator, final_ranges, per_filter);
            final_ranges.deinit(self.allocator);
            final_ranges = next;
            // Early exit if the intersection is already empty — no column read needed.
            if (final_ranges.isEmpty()) break;
        }

        if (final_ranges.isEmpty()) {
            return try self.allocator.alloc(Row, 0);
        }

        // Per-column: fetch just the pages intersecting final_ranges and decode.
        const column_values = try self.allocator.alloc([]Value, num_leaf_columns);
        @memset(column_values, &.{});
        defer {
            for (column_values) |vals| {
                for (vals) |v| v.deinit(self.allocator);
                if (vals.len > 0) self.allocator.free(vals);
            }
            self.allocator.free(column_values);
        }

        const column_rg_indices = try self.allocator.alloc([]u64, num_leaf_columns);
        @memset(column_rg_indices, &.{});
        defer {
            for (column_rg_indices) |idx| {
                if (idx.len > 0) self.allocator.free(idx);
            }
            self.allocator.free(column_rg_indices);
        }

        for (0..num_leaf_columns) |col_idx| {
            const chunk = &rg.columns[col_idx];
            const oi = &offset_indexes[col_idx].?;

            var page_result = try page_range_reader_mod.readSelectedPages(
                self.allocator,
                self.getSource(),
                chunk,
                oi,
                final_ranges,
                num_rows,
            );
            defer page_result.deinit(self.allocator);

            if (page_result.data.len == 0) {
                // Nothing selected for this column → no decoded values.
                continue;
            }

            // Decode using the substituted buffer.
            const decoded = try self.readColumnDynamicWithData(col_idx, row_group_index, page_result.data);
            // We don't use def/rep levels in the flat path; free them if present.
            if (decoded.def_levels) |dl| self.allocator.free(dl);
            if (decoded.rep_levels) |rl| self.allocator.free(rl);

            // Build the decoded-row-idx → rg-local-row-idx mapping, constrained
            // to final_ranges. For flat columns, each decoded value is one row.
            const total = decoded.values.len;
            var kept_values = try std.ArrayList(Value).initCapacity(self.allocator, total);
            errdefer {
                for (kept_values.items) |v| v.deinit(self.allocator);
                kept_values.deinit(self.allocator);
            }
            var kept_indices = try std.ArrayList(u64).initCapacity(self.allocator, total);
            errdefer kept_indices.deinit(self.allocator);

            for (decoded.values, 0..) |v, i| {
                const rg_row = page_result.mapDecodedRow(i) orelse {
                    // Shouldn't happen if the decoder produced the expected count.
                    return error.InvalidPageData;
                };
                if (final_ranges.contains(rg_row)) {
                    try kept_values.append(self.allocator, v);
                    try kept_indices.append(self.allocator, rg_row);
                } else {
                    v.deinit(self.allocator);
                }
            }

            // Free the now-drained values slice (values themselves were moved or freed above).
            self.allocator.free(decoded.values);

            column_values[col_idx] = try kept_values.toOwnedSlice(self.allocator);
            column_rg_indices[col_idx] = try kept_indices.toOwnedSlice(self.allocator);
        }

        // Every column must agree on the kept row indices.
        const first_non_empty = blk: {
            for (column_rg_indices, 0..) |idx, i| {
                if (idx.len > 0) break :blk i;
            }
            break :blk null;
        };
        if (first_non_empty == null) {
            return try self.allocator.alloc(Row, 0);
        }
        const canonical = column_rg_indices[first_non_empty.?];
        for (column_rg_indices) |idx| {
            if (idx.len == 0) continue;
            if (idx.len != canonical.len) return error.InconsistentPageAlignment;
            for (idx, canonical) |a, b| {
                if (a != b) return error.InconsistentPageAlignment;
            }
        }

        // Assemble flat rows from the kept per-column values.
        return self.assembleFlatRows(canonical.len, num_leaf_columns, column_values, null);
    }

    const StorageScope = struct {
        storage: *value_mod.RowStorage,
        prev_alloc: ?std.mem.Allocator,
        prev_storage: ?*value_mod.RowStorage,
    };

    /// Creates row storage for one read and routes `value_mod.dupeBytes` to its arena.
    fn beginStorage(self: *Self) !StorageScope {
        const storage = try value_mod.RowStorage.create(self.allocator);
        const scope: StorageScope = .{
            .storage = storage,
            .prev_alloc = value_mod.bytes_arena_allocator,
            .prev_storage = self.storage,
        };
        value_mod.bytes_arena_allocator = storage.arena.allocator();
        self.storage = storage;
        return scope;
    }

    /// Restores the previous routing and drops the read's reference; the storage stays alive
    /// while rows hold references to it.
    fn endStorage(self: *Self, scope: StorageScope) void {
        value_mod.bytes_arena_allocator = scope.prev_alloc;
        self.storage = scope.prev_storage;
        scope.storage.release();
    }

    /// Allocator for memory owned by the rows of the read in progress.
    fn rowAllocator(self: *Self) std.mem.Allocator {
        return self.storage.?.arena.allocator();
    }

    fn readRowsInternal(self: *Self, row_group_index: usize, col_indices: ?[]const usize) ![]Row {
        if (row_group_index >= self.metadata.row_groups.len) {
            return try self.allocator.alloc(Row, 0);
        }

        // Allocate the rows' memory (values arrays, nested values, byte
        // payloads via `value_mod.dupeBytes`) in storage shared by the rows of
        // this read. Each row holds a reference (`Row.storage`); the storage is
        // freed with the last row.
        const storage = try self.beginStorage();
        defer self.endStorage(storage);

        const rg = &self.metadata.row_groups[row_group_index];
        const num_rows = try safeRowCount(rg.num_rows);
        const num_leaf_columns = rg.columns.len;

        if (num_rows == 0 or num_leaf_columns == 0) {
            return try self.allocator.alloc(Row, 0);
        }

        const schema = self.metadata.schema;
        const num_top: usize = if (schema.len > 0)
            safe.castTo(usize, schema[0].num_children orelse 0) catch 0
        else
            0;

        const is_flat = num_top == 0;
        const num_top_columns = if (is_flat) num_leaf_columns else num_top;

        // Validate projection indices
        if (col_indices) |indices| {
            for (indices) |idx| {
                if (idx >= num_top_columns) return error.InvalidArgument;
            }
        }

        // Compute top-level-to-leaf mapping for nested schemas. The schema nodes are built in
        // the row storage: struct values in the rows reference their field names.
        const sa = self.rowAllocator();

        const top_infos = if (!is_flat) blk: {
            const infos = try self.allocator.alloc(TopColInfo, num_top);
            errdefer self.allocator.free(infos);

            var schema_idx: usize = 1;
            var leaf_offset: usize = 0;
            for (0..num_top) |top_idx| {
                if (schema_idx >= schema.len) return error.InvalidSchema;
                const build_result = try schema_mod.SchemaNode.buildFromElements(sa, schema, schema_idx);
                const lc = build_result.node.countLeafColumns();
                infos[top_idx] = .{
                    .leaf_start = leaf_offset,
                    .leaf_count = lc,
                    .is_nested = lc > 1 or !build_result.node.unwrapOptional().isPrimitive(),
                    .schema_node = build_result.node,
                };
                schema_idx = build_result.next_idx;
                leaf_offset += lc;
            }
            break :blk infos;
        } else null;
        defer if (top_infos) |ti| self.allocator.free(ti);

        // Every row's values in one block of the row storage, filled one output column at a
        // time.
        const output_indices = col_indices orelse blk: {
            const all = try self.allocator.alloc(usize, num_top_columns);
            for (all, 0..) |*v, i| v.* = i;
            break :blk all;
        };
        defer if (col_indices == null) self.allocator.free(output_indices);
        const num_out = output_indices.len;
        const all_values = try self.rowAllocator().alloc(Value, try std.math.mul(usize, num_rows, num_out));

        for (output_indices, 0..) |top_idx, out_idx| {
            const column_out = all_values[out_idx..];
            if (top_infos) |infos| {
                const info = infos[top_idx];
                if (info.is_nested) {
                    try self.readNestedIntoRows(info, row_group_index, num_leaf_columns, column_out, num_out, num_rows);
                } else {
                    try self.readLeafIntoRows(info.leaf_start, row_group_index, column_out, num_out, num_rows);
                }
            } else {
                try self.readLeafIntoRows(top_idx, row_group_index, column_out, num_out, num_rows);
            }
        }

        const rows = try self.allocator.alloc(Row, num_rows);
        for (rows, 0..) |*row, r| {
            row.* = .{ .values = all_values[r * num_out ..][0..num_out], .allocator = self.allocator, .storage = self.storage };
        }
        self.storage.?.retainMany(num_rows);
        return rows;
    }

    const RowLeaf = struct {
        leaf: leaf_decoder.Leaf,
        /// The decoded values buffer. For byte arrays it is owned by the row storage and
        /// `leaf.buffers.values` is empty; otherwise it is `leaf.buffers.values`.
        values: leaf_decoder.Buffer,
        element: format.SchemaElement,

        /// The buffers with `values` in place, limited to the first `max_len` entries.
        fn view(self: *const RowLeaf, max_len: usize) leaf_decoder.Buffers {
            var v = self.leaf.buffers;
            v.values = self.values;
            v.length = @min(v.length, max_len);
            return v;
        }
    };

    /// Decodes leaf column `leaf_idx` of a row group for the row API. Byte-array data is handed
    /// to the row storage, so row values can reference it.
    fn decodeRowLeaf(self: *Self, leaf_idx: usize, row_group_index: usize, keep_levels: bool) !RowLeaf {
        const rg = &self.metadata.row_groups[row_group_index];
        if (leaf_idx >= rg.columns.len) return error.InvalidArgument;
        const chunk = &rg.columns[leaf_idx];
        const meta = chunk.meta_data orelse return error.InvalidArgument;
        const info = format.getColumnInfo(self.metadata.schema, leaf_idx) orelse return error.InvalidArgument;
        const physical = info.element.type_ orelse return error.InvalidSchema;
        const page_data = try self.readColumnChunkData(chunk);
        defer self.allocator.free(page_data);

        var leaf = try leaf_decoder.decodeLeaf(self.allocator, page_data, meta, info, .{
            .keep_levels = keep_levels,
            .checksum = self.checksum_options,
            .large_offsets = true,
        });
        const values = leaf.buffers.values;
        if (physical == .byte_array or physical == .fixed_len_byte_array) {
            self.storage.?.adopt(values) catch |e| {
                leaf.deinit(self.allocator);
                return e;
            };
            leaf.buffers.values = &.{};
        }
        return .{ .leaf = leaf, .values = values, .element = info.element };
    }

    /// Decodes a top-level primitive column and writes its values to `out[r * stride]` for
    /// `r < num_rows`; rows past the column's entries get null.
    fn readLeafIntoRows(self: *Self, leaf_idx: usize, row_group_index: usize, out: []Value, stride: usize, num_rows: usize) !void {
        var rl = try self.decodeRowLeaf(leaf_idx, row_group_index, false);
        defer rl.leaf.deinit(self.allocator);
        const view = rl.view(num_rows);
        try leaf_decoder.writeValues(self.rowAllocator(), &view, rl.element, out, stride, .borrow);
        for (view.length..num_rows) |r| out[r * stride] = .{ .null_val = {} };
    }

    /// Reads a nested top-level column's leaves, assembles its values into the row storage and
    /// writes them to `out[r * stride]` for the first `num_rows` rows.
    fn readNestedIntoRows(
        self: *Self,
        info: TopColInfo,
        row_group_index: usize,
        num_leaf_columns: usize,
        out: []Value,
        stride: usize,
        num_rows: usize,
    ) !void {
        const columns = try self.allocator.alloc(ColumnValues, info.leaf_count);
        @memset(columns, .{ .values = &.{}, .def_levels = null, .rep_levels = null });
        defer {
            // Byte payloads are in the row storage; only the arrays are freed.
            for (columns) |*c| {
                self.allocator.free(c.values);
                if (c.def_levels) |l| self.allocator.free(l);
                if (c.rep_levels) |l| self.allocator.free(l);
            }
            self.allocator.free(columns);
        }
        const slices = try self.allocator.alloc(nested_mod.FlatColumnSlice, info.leaf_count);
        defer self.allocator.free(slices);

        for (columns, slices, 0..) |*c, *slice, li| {
            const phys = info.leaf_start + li;
            if (phys >= num_leaf_columns) {
                slice.* = .{ .values = &.{}, .def_levels = &.{}, .rep_levels = &.{} };
                continue;
            }
            var rl = try self.decodeRowLeaf(phys, row_group_index, true);
            defer rl.leaf.deinit(self.allocator);
            const view = rl.view(std.math.maxInt(usize));
            const values = try self.allocator.alloc(Value, view.length);
            errdefer self.allocator.free(values);
            try leaf_decoder.writeValues(self.rowAllocator(), &view, rl.element, values, 1, .borrow);
            c.* = .{ .values = values, .def_levels = rl.leaf.def_levels, .rep_levels = rl.leaf.rep_levels };
            rl.leaf.def_levels = null;
            rl.leaf.rep_levels = null;
            slice.* = .{
                .values = c.values,
                .def_levels = c.def_levels orelse &.{},
                .rep_levels = c.rep_levels orelse &.{},
            };
        }

        const assembled = try self.allocator.alloc(Value, num_rows);
        defer self.allocator.free(assembled);
        try nested_mod.assembleValuesBorrowed(self.rowAllocator(), self.allocator, info.schema_node, slices, assembled);
        for (assembled, 0..) |v, r| out[r * stride] = v;
    }

    /// Read a single column dynamically
    fn readColumnDynamic(self: *Self, column_index: usize, row_group_index: usize) !ColumnValues {
        return self.readColumnDynamicWithData(column_index, row_group_index, null);
    }

    /// Read a single column, optionally using caller-supplied page bytes.
    ///
    /// When `override_page_data` is non-null it must contain `[dict_page?]
    /// [data_pages...]` in the layout produced by `PageRangeReader`. This is
    /// the hook for filtered reads: the caller pre-reads just the data pages
    /// whose rows intersect the wanted `RowRanges`.
    fn readColumnDynamicWithData(
        self: *Self,
        column_index: usize,
        row_group_index: usize,
        override_page_data: ?[]const u8,
    ) !ColumnValues {
        const rg = &self.metadata.row_groups[row_group_index];
        if (column_index >= rg.columns.len) return error.InvalidArgument;

        const chunk = &rg.columns[column_index];
        const meta = chunk.meta_data orelse return error.InvalidArgument;

        // Read the raw page data (or use caller's buffer).
        const owned_page_data: ?[]u8 = if (override_page_data == null)
            try self.readColumnChunkData(chunk)
        else
            null;
        defer if (owned_page_data) |pd| self.allocator.free(pd);
        const page_data: []const u8 = owned_page_data orelse override_page_data.?;

        const column_info = format.getColumnInfo(self.metadata.schema, column_index) orelse
            return error.InvalidArgument;

        var leaf = try leaf_decoder.decodeLeaf(self.allocator, page_data, meta, column_info, .{
            .keep_levels = true,
            .checksum = self.checksum_options,
            .large_offsets = true,
        });
        defer leaf.buffers.deinit(self.allocator);
        errdefer {
            if (leaf.def_levels) |l| self.allocator.free(l);
            if (leaf.rep_levels) |l| self.allocator.free(l);
        }

        const values = try leaf_decoder.toValues(self.allocator, &leaf.buffers, column_info.element);
        return .{
            .values = values,
            .def_levels = leaf.def_levels,
            .rep_levels = leaf.rep_levels,
        };
    }

    fn assembleFlatRows(
        self: *Self,
        num_rows: usize,
        num_columns: usize,
        column_values: [][]Value,
        projected_col_indices: ?[]const usize,
    ) ![]Row {
        const output_indices = projected_col_indices orelse blk: {
            const all = try self.allocator.alloc(usize, num_columns);
            for (0..num_columns) |i| all[i] = i;
            break :blk all;
        };
        defer if (projected_col_indices == null) self.allocator.free(output_indices);

        // Every row's values in one block of the row storage; the column values are moved in.
        const num_output_cols = output_indices.len;
        const all_values = try self.rowAllocator().alloc(Value, try std.math.mul(usize, num_rows, num_output_cols));
        const rows = try self.allocator.alloc(Row, num_rows);

        for (rows, 0..) |*row, row_idx| {
            const values = all_values[row_idx * num_output_cols ..][0..num_output_cols];
            for (output_indices, 0..) |col_idx, out_idx| {
                values[out_idx] = if (row_idx < column_values[col_idx].len)
                    column_values[col_idx][row_idx]
                else
                    .{ .null_val = {} };
            }
            row.* = .{ .values = values, .allocator = self.allocator, .storage = self.storage };
        }
        self.storage.?.retainMany(num_rows);
        return rows;
    }

    /// Read raw page data for a column chunk
    fn readColumnChunkData(self: *Self, chunk: *const format.ColumnChunk) ![]u8 {
        return parquet_reader.readColumnChunkData(self.allocator, self.getSource(), chunk) catch |e| {
            return switch (e) {
                error.InvalidArgument => error.InvalidArgument,
                error.OutOfMemory => error.OutOfMemory,
                error.InputOutput => error.InputOutput,
                else => error.InputOutput,
            };
        };
    }
};

/// One decoded leaf column chunk: a `Value` per level entry and the levels.
const ColumnValues = struct {
    values: []Value,
    /// Null when the column's maximum definition level is 0.
    def_levels: ?[]u32,
    /// Null when the column's maximum repetition level is 0.
    rep_levels: ?[]u32,

    pub fn deinit(self: *ColumnValues, allocator: std.mem.Allocator) void {
        for (self.values) |v| v.deinit(allocator);
        allocator.free(self.values);
        if (self.def_levels) |dl| allocator.free(dl);
        if (self.rep_levels) |rl| allocator.free(rl);
    }
};
