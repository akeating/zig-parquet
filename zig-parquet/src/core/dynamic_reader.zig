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
const nested_rows = @import("nested_rows.zig");
const schema_tree = @import("schema_tree.zig");
const row_ranges_mod = @import("row_ranges.zig");
const page_filter_mod = @import("page_filter.zig");
const page_index_reader_mod = @import("page_index_reader.zig");
const page_selection = @import("page_selection.zig");
const parallel = @import("parallel.zig");
const arrow_batch = @import("arrow_batch.zig");

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
    /// Decodes the columns of a row read (`readAllRows`, `readRowsProjected`,
    /// `readRowsFiltered`, `rowIterator`) as tasks of this `Io`, so an `Io` whose `async`
    /// uses threads (`std.Io.Threaded`) decodes them in parallel; see `parallel.zig`. Then
    /// the reader's allocator and source must allow concurrent use. Null decodes them one
    /// after another on the calling thread.
    io: ?std.Io = null,
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
    /// `DynamicReaderOptions.io`.
    decode_io: ?std.Io = null,
    _backend_cleanup: ?BackendCleanup = null,
    /// Storage of the read in progress (see `value_mod.RowStorage`): rows'
    /// `values` arrays and nested values are allocated in its arena, it owns
    /// the decoded byte buffers that byte payloads reference, and each row
    /// built holds a reference to it. Set by `beginStorage` for the duration
    /// of one read call.
    storage: ?*value_mod.RowStorage = null,

    const Self = @This();

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
            .decode_io = options.io,
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

                const rows = try self.reader.readRowsInternal(self.current_rg, self.col_indices, null);
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

    /// Reads the columns of a row group as Arrow arrays: one `ArrowArray` per top-level
    /// column read (`options.columns`, in that order; null reads all), and a schema whose
    /// children describe them. The columns are decoded with `options.io`, on the calling
    /// thread when it is null; the reader's `DynamicReaderOptions.io` applies to row reads
    /// only. The arrays are allocated with the reader's allocator; `ReadResult.deinit`
    /// releases them.
    pub fn readColumns(self: *Self, row_group_index: usize, options: arrow_batch.RowGroupOptions) !arrow_batch.ReadResult {
        return arrow_batch.readColumns(self.allocator, self.getSource(), self.metadata, row_group_index, options);
    }

    /// An `ArrowBatchReader` over the file's columns: Arrow arrays of `options.batch_rows`
    /// rows (65,536 by default), taken from as many row groups as needed. The columns are
    /// decoded with `options.io`, on the calling thread when it is null. The reader must
    /// outlive the batch reader.
    pub fn columnBatches(self: *Self, options: arrow_batch.BatchOptions) !arrow_batch.ArrowBatchReader {
        return arrow_batch.ArrowBatchReader.init(self.allocator, self.getSource(), self.metadata, options);
    }

    /// Read all rows from a row group as dynamic Value types
    pub fn readAllRows(self: *Self, row_group_index: usize) ![]Row {
        return self.readRowsInternal(row_group_index, null, null);
    }

    /// Read rows from a row group, returning only the specified top-level columns.
    /// Column indices refer to top-level schema columns (not leaf columns).
    /// Returned rows contain values in dense order matching the projection list.
    pub fn readRowsProjected(self: *Self, row_group_index: usize, col_indices: []const usize) ![]Row {
        return self.readRowsInternal(row_group_index, col_indices, null);
    }

    /// Read rows from a row group, applying page-index-driven filters to skip
    /// pages whose min/max ranges cannot match the predicates.
    ///
    /// Behavior:
    ///   - Every leaf column in the row group must advertise an `OffsetIndex`;
    ///     otherwise `error.PageIndexMissing` is returned (use `readAllRows`
    ///     for unindexed files).
    ///   - `filters` target leaf column indices. Each filter keeps the rows of
    ///     the pages of its column whose statistics might match, and the
    ///     filters' rows are intersected. Columns without a filter are treated
    ///     as wildcards (`full(rg_num_rows)`). A filter on a leaf of a list or
    ///     map keeps a row when the page holding the row might contain a
    ///     matching element of that row.
    ///   - Pages whose row ranges don't intersect the final (intersected)
    ///     `RowRanges` are never read or decompressed. The pages that are read
    ///     are read from the source as they are decoded, one range of adjacent
    ///     pages (at most `page_selection.max_span_bytes`) per read.
    ///   - Nested columns (lists, structs, maps) are returned as by `readAllRows`.
    ///     The page index counts rows, so the data pages of a column with
    ///     repetition must start at a row (repetition level 0), as the Parquet
    ///     format requires of files with an offset index. A page read that does
    ///     not start at a row returns `error.PageNotAtRowStart`, and a page whose
    ///     row count differs from the offset index returns
    ///     `error.PageRowCountMismatch`. Pages that are not read are not checked:
    ///     when the page after a kept row continues that row, the row is
    ///     returned without the entries in that page.
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

        const rg = &self.metadata.row_groups[row_group_index];
        const num_rows = try safeRowCount(rg.num_rows);
        const num_leaf_columns = rg.columns.len;
        if (num_rows == 0 or num_leaf_columns == 0) {
            return try self.allocator.alloc(Row, 0);
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

        const filter: FilteredRead = .{
            .ranges = final_ranges,
            .offset_indexes = offset_indexes,
            .rg_num_rows = num_rows,
        };
        return self.readRowsInternal(row_group_index, null, &filter);
    }

    /// The rows a filtered read keeps, and the page indexes that locate them.
    const FilteredRead = struct {
        /// Rows kept, as row-group row indices.
        ranges: RowRanges,
        /// Offset index of each leaf column; none is null.
        offset_indexes: []const ?format.OffsetIndex,
        rg_num_rows: u64,
    };

    /// Where the row API reads a row group's leaf columns from.
    const RowSource = struct {
        row_group_index: usize,
        /// Set by `readRowsFiltered`: only the pages and rows it selects are read.
        filter: ?*const FilteredRead = null,
    };

    const StorageScope = struct {
        storage: *value_mod.RowStorage,
        prev_storage: ?*value_mod.RowStorage,
    };

    /// Creates row storage for one read and makes it the reader's `storage`.
    fn beginStorage(self: *Self) !StorageScope {
        const storage = try value_mod.RowStorage.create(self.allocator);
        const scope: StorageScope = .{ .storage = storage, .prev_storage = self.storage };
        self.storage = storage;
        return scope;
    }

    /// Restores the previous `storage` and drops the read's reference; the storage stays
    /// alive while rows hold references to it.
    fn endStorage(self: *Self, scope: StorageScope) void {
        self.storage = scope.prev_storage;
        scope.storage.release();
    }

    /// Allocator for memory owned by the rows of the read in progress.
    fn rowAllocator(self: *Self) std.mem.Allocator {
        return self.storage.?.arena.allocator();
    }

    /// Reads the rows of a row group: all of them or, with `filter`, those it keeps. With
    /// `col_indices`, only those top-level columns are read.
    fn readRowsInternal(self: *Self, row_group_index: usize, col_indices: ?[]const usize, filter: ?*const FilteredRead) ![]Row {
        if (row_group_index >= self.metadata.row_groups.len) {
            return try self.allocator.alloc(Row, 0);
        }

        // The rows' memory (values, nested arrays and the decoded byte buffers the values
        // reference) is in storage shared by the rows of this read. Each row holds a reference
        // (`Row.storage`); the storage is freed with the last row.
        const storage = try self.beginStorage();
        defer self.endStorage(storage);

        const rg = &self.metadata.row_groups[row_group_index];
        const num_leaf_columns = rg.columns.len;
        const num_rows = if (filter) |f|
            safe.castTo(usize, f.ranges.rowCount()) catch return error.InvalidRowCount
        else
            try safeRowCount(rg.num_rows);

        if (num_rows == 0 or num_leaf_columns == 0) {
            return try self.allocator.alloc(Row, 0);
        }
        const src: RowSource = .{ .row_group_index = row_group_index, .filter = filter };

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

        // The Arrow view of the schema: nested columns are assembled from it (`nested_rows`).
        var tree_arena = std.heap.ArenaAllocator.init(self.allocator);
        defer tree_arena.deinit();
        const tree = if (is_flat) null else try schema_tree.buildSchemaTree(tree_arena.allocator(), schema);

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

        // The columns are decoded as parallel tasks when there is a `decode_io`. The row
        // storage and its arena are not thread-safe, so handing byte buffers to the storage
        // and assembling nested columns (which allocates in the arena) wait until the tasks
        // finish. Without a `decode_io`, each column is completed in its turn, as before.
        const slots = try self.allocator.alloc(ColumnSlot, num_out);
        defer self.allocator.free(slots);
        @memset(slots, .{});
        defer for (slots) |*slot| slot.release(self.allocator);
        const columns: RowColumnTask = .{
            .reader = self,
            .src = src,
            .tree = tree,
            .output_indices = output_indices,
            .values = all_values,
            .num_out = num_out,
            .num_rows = num_rows,
            .slots = slots,
            .in_task = self.decode_io == null,
        };
        try parallel.forEach(self.decode_io, self.allocator, num_out, &columns, RowColumnTask.run);
        for (slots, 0..) |*slot, out_idx| try self.finishColumn(slot, all_values[out_idx..], num_out, num_rows);

        const rows = try self.allocator.alloc(Row, num_rows);
        for (rows, 0..) |*row, r| {
            row.* = .{ .values = all_values[r * num_out ..][0..num_out], .allocator = self.allocator, .storage = self.storage };
        }
        self.storage.?.retainMany(num_rows);
        return rows;
    }

    const RowLeaf = struct {
        leaf: leaf_decoder.Leaf,
        /// The decoded values buffer. Once `adoptBytes` hands a byte-array buffer to the row
        /// storage, the storage owns it and `leaf.buffers.values` is empty; until then, and
        /// for other types, it is `leaf.buffers.values`.
        values: leaf_decoder.Buffer,
        /// A byte-array or fixed-length byte-array leaf, whose rows' values reference `values`.
        bytes: bool,
        element: format.SchemaElement,
        max_def: u32,
        max_rep: u32,

        /// The buffers with `values` in place, limited to the first `max_len` entries.
        fn view(self: *const RowLeaf, max_len: usize) leaf_decoder.Buffers {
            var v = self.leaf.buffers;
            v.values = self.values;
            v.length = @min(v.length, max_len);
            return v;
        }
    };

    /// Decodes leaf column `leaf_idx` of a row group for the row API: the whole chunk or, with
    /// a filter, the rows it keeps. Byte-array data is handed to the row storage, so row values
    /// can reference it.
    fn decodeRowLeaf(self: *Self, leaf_idx: usize, src: RowSource, keep_levels: bool) !RowLeaf {
        const rg = &self.metadata.row_groups[src.row_group_index];
        if (leaf_idx >= rg.columns.len) return error.InvalidArgument;
        const chunk = &rg.columns[leaf_idx];
        const meta = chunk.meta_data orelse return error.InvalidArgument;
        const info = format.getColumnInfo(self.metadata.schema, leaf_idx) orelse return error.InvalidArgument;
        const physical = info.element.type_ orelse return error.InvalidSchema;
        const options: leaf_decoder.Options = .{
            .keep_levels = keep_levels,
            .checksum = self.checksum_options,
            .large_offsets = true,
        };

        const leaf = if (src.filter) |filter|
            try self.decodeFilteredLeaf(chunk, meta, info, options, filter, &filter.offset_indexes[leaf_idx].?)
        else blk: {
            const page_data = try self.readColumnChunkData(chunk);
            defer self.allocator.free(page_data);
            break :blk try leaf_decoder.decodeLeaf(self.allocator, page_data, meta, info, options);
        };
        return .{
            .leaf = leaf,
            .values = leaf.buffers.values,
            .bytes = physical == .byte_array or physical == .fixed_len_byte_array,
            .element = info.element,
            .max_def = info.max_def_level,
            .max_rep = info.max_rep_level,
        };
    }

    /// Hands a byte-array leaf's values buffer, which the rows' byte values reference, to the
    /// row storage. Not thread-safe.
    fn adoptBytes(self: *Self, rl: *RowLeaf) !void {
        if (!rl.bytes or rl.leaf.buffers.values.len == 0) return;
        try self.storage.?.adopt(rl.values);
        rl.leaf.buffers.values = &.{};
    }

    fn ErrorSetOf(comptime f: anytype) type {
        return @typeInfo(@typeInfo(@TypeOf(f)).@"fn".return_type.?).error_union.error_set;
    }

    const RowColumnError = ErrorSetOf(decodeRowLeaf) || ErrorSetOf(leaf_decoder.writeValues) ||
        ErrorSetOf(nested_rows.nodeValues) || std.mem.Allocator.Error;

    /// What a column task leaves for `finishColumn`.
    const ColumnSlot = struct {
        /// A flat byte-array column's values buffer, which its rows' values reference.
        bytes: ?RowLeaf = null,
        /// A nested column whose leaves are decoded and whose values are not yet assembled.
        node: ?*const schema_tree.Node = null,
        leaves: ?[]RowLeaf = null,

        fn release(self: *ColumnSlot, allocator: std.mem.Allocator) void {
            if (self.bytes) |*rl| rl.leaf.deinit(allocator);
            if (self.leaves) |leaves| {
                for (leaves) |*rl| rl.leaf.deinit(allocator);
                allocator.free(leaves);
            }
            self.* = .{};
        }
    };

    /// Decodes output column `i` of a row read and writes its values to every `num_out`-th
    /// entry of `values[i..]`. A flat column is written in the task. A nested column is
    /// assembled in the task when `in_task` is set, otherwise by `finishColumn`.
    const RowColumnTask = struct {
        reader: *Self,
        src: RowSource,
        tree: ?[]const schema_tree.Node,
        output_indices: []const usize,
        values: []Value,
        num_out: usize,
        num_rows: usize,
        slots: []ColumnSlot,
        in_task: bool,

        fn run(self: *const RowColumnTask, i: usize) RowColumnError!void {
            const r = self.reader;
            const top_idx = self.output_indices[i];
            const out = self.values[i..];
            const slot = &self.slots[i];
            const node: ?*const schema_tree.Node = if (self.tree) |t| &t[top_idx] else null;
            if (node == null or node.?.kind == .leaf) {
                const leaf_idx = if (node) |n| n.physical_start else top_idx;
                // A byte-array leaf, which the written values reference, goes to the slot;
                // from there `ColumnSlot.release` or `finishColumn` frees it.
                slot.bytes = try self.writeLeaf(leaf_idx, out) orelse return;
            } else {
                slot.node = node.?;
                slot.leaves = try self.decodeNodeLeaves(node.?);
            }
            if (self.in_task) try r.finishColumn(slot, out, self.num_out, self.num_rows);
        }

        /// Decodes leaf `leaf_idx` and writes its values to every `num_out`-th entry of
        /// `out`. Returns the leaf when its values reference its values buffer (byte
        /// arrays); otherwise frees it and returns null.
        fn writeLeaf(self: *const RowColumnTask, leaf_idx: usize, out: []Value) RowColumnError!?RowLeaf {
            const r = self.reader;
            var rl = try r.decodeRowLeaf(leaf_idx, self.src, false);
            errdefer rl.leaf.deinit(r.allocator);
            const view = rl.view(self.num_rows);
            try leaf_decoder.writeValues(&view, rl.element, out, self.num_out);
            for (view.length..self.num_rows) |row| out[row * self.num_out] = .{ .null_val = {} };
            if (rl.bytes) return rl;
            rl.leaf.deinit(r.allocator);
            return null;
        }

        /// Decodes the leaves of nested column `node`, with levels.
        fn decodeNodeLeaves(self: *const RowColumnTask, node: *const schema_tree.Node) RowColumnError![]RowLeaf {
            const r = self.reader;
            const leaves = try r.allocator.alloc(RowLeaf, node.physical_count);
            var decoded: usize = 0;
            errdefer {
                for (leaves[0..decoded]) |*rl| rl.leaf.deinit(r.allocator);
                r.allocator.free(leaves);
            }
            for (leaves, 0..) |*rl, k| {
                rl.* = try r.decodeRowLeaf(node.physical_start + k, self.src, true);
                decoded = k + 1;
            }
            return leaves;
        }
    };

    /// Completes a column after its task: hands its byte buffers to the row storage and
    /// assembles a nested column's values into every `stride`-th entry of `out`. Not
    /// thread-safe.
    fn finishColumn(self: *Self, slot: *ColumnSlot, out: []Value, stride: usize, num_rows: usize) !void {
        if (slot.bytes) |*rl| {
            try self.adoptBytes(rl);
            rl.leaf.deinit(self.allocator);
            slot.bytes = null;
        }
        const leaves = slot.leaves orelse return;
        for (leaves) |*rl| try self.adoptBytes(rl);
        try self.assembleNodeIntoRows(slot.node.?, leaves, out, stride, num_rows);
        slot.release(self.allocator);
    }

    /// Reads the pages of a leaf column chunk that hold rows `filter` keeps (see
    /// `page_selection`) from the source as they are decoded, and returns the
    /// entries of the kept rows.
    fn decodeFilteredLeaf(
        self: *Self,
        chunk: *const format.ColumnChunk,
        meta: format.ColumnMetaData,
        info: format.ColumnInfo,
        options: leaf_decoder.Options,
        filter: *const FilteredRead,
        offset_index: *const format.OffsetIndex,
    ) !leaf_decoder.Leaf {
        const allocator = self.allocator;
        const repeated = info.max_rep_level > 0;
        var opts = options;
        // The rows of a column with repetition start at entries with repetition level 0.
        if (repeated) opts.keep_levels = true;

        var pages = try page_selection.selectPages(allocator, chunk, offset_index, filter.ranges, filter.rg_num_rows);
        defer pages.deinit(allocator);

        const page_rows = try allocator.alloc(usize, pages.selected.len);
        defer allocator.free(page_rows);
        for (pages.selected, page_rows) |sp, *n| {
            n.* = safe.castTo(usize, sp.end_row_index - sp.first_row_index) catch return error.IntegerOverflow;
        }

        const reader = leaf_decoder.PageReader.fromSpans(allocator, self.getSource(), pages.spans);
        var leaf = try leaf_decoder.decodeSelectedPages(allocator, reader, meta, info, opts, page_rows);
        errdefer leaf.deinit(allocator);

        const ranges = try keptEntryRanges(allocator, pages.selected, filter.ranges, leaf.rep_levels, leaf.buffers.length);
        defer allocator.free(ranges);
        if (ranges.len == 1 and ranges[0].start == 0 and ranges[0].end == leaf.buffers.length) return leaf;

        const kept = try selectLeafEntries(allocator, &leaf, ranges);
        leaf.deinit(allocator);
        return kept;
    }

    /// Assembles the values of nested top-level column `node` from its decoded `leaves` in the
    /// row storage (see `nested_rows`) and writes them to `out[r * stride]` for `r < num_rows`;
    /// rows past the column's values get null.
    fn assembleNodeIntoRows(self: *Self, node: *const schema_tree.Node, row_leaves: []RowLeaf, out: []Value, stride: usize, num_rows: usize) !void {
        const leaves = try self.allocator.alloc(nested_rows.Leaf, row_leaves.len);
        defer self.allocator.free(leaves);
        for (row_leaves, leaves) |*rl, *l| {
            l.* = .{
                .buffers = rl.view(std.math.maxInt(usize)),
                .def_levels = rl.leaf.def_levels,
                .rep_levels = rl.leaf.rep_levels,
                .element = rl.element,
                .max_def = rl.max_def,
                .max_rep = rl.max_rep,
            };
        }

        const ctx = nested_rows.Context{
            .arena = self.rowAllocator(),
            .scratch = self.allocator,
            .schema = self.metadata.schema,
            .leaves = leaves,
            .base = node.physical_start,
        };
        const values = try nested_rows.nodeValues(&ctx, node, self.allocator);
        defer self.allocator.free(values);
        for (0..num_rows) |r| out[r * stride] = if (r < values.len) values[r] else .{ .null_val = {} };
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

/// The entries of the rows in `ranges`, in a leaf decoded from the `selected` pages. Entries
/// with repetition level 0 start rows; without `rep_levels` (a column without repetition) each
/// entry is a row. The entry ranges are in ascending order, with adjacent ones merged.
fn keptEntryRanges(
    allocator: std.mem.Allocator,
    selected: []const page_selection.SelectedPage,
    ranges: RowRanges,
    rep_levels: ?[]const leaf_decoder.Level,
    num_entries: usize,
) ![]leaf_decoder.EntryRange {
    // The entry at which each decoded row starts, then `num_entries`.
    var starts: ?[]usize = null;
    defer if (starts) |s| allocator.free(s);
    if (rep_levels) |rep| {
        if (rep.len != num_entries) return error.InvalidPageData;
        var count: usize = 0;
        for (rep) |lv| count += @intFromBool(lv == 0);
        const s = try allocator.alloc(usize, count + 1);
        starts = s;
        var k: usize = 0;
        for (rep, 0..) |lv, i| {
            if (lv != 0) continue;
            s[k] = i;
            k += 1;
        }
        s[count] = num_entries;
    }
    const decoded_rows: u64 = if (starts) |s| s.len - 1 else num_entries;

    var out: std.ArrayList(leaf_decoder.EntryRange) = .empty;
    errdefer out.deinit(allocator);
    // Decoded row index of the first row of the current page.
    var base: u64 = 0;
    var kept: u64 = 0;
    for (selected) |sp| {
        for (ranges.ranges) |r| {
            if (r.first >= sp.end_row_index) break;
            const first = @max(r.first, sp.first_row_index);
            const last = @min(r.last, sp.end_row_index);
            if (first >= last) continue;
            const row_start = base + (first - sp.first_row_index);
            const row_end = base + (last - sp.first_row_index);
            if (row_end > decoded_rows) return error.PageRowCountMismatch;
            kept += last - first;
            const rs: usize = @intCast(row_start); // row_start < row_end <= decoded_rows, a usize
            const re: usize = @intCast(row_end); // row_end <= decoded_rows, a usize
            const start, const end = if (starts) |s| .{ s[rs], s[re] } else .{ rs, re };
            if (out.items.len > 0 and out.items[out.items.len - 1].end == start) {
                out.items[out.items.len - 1].end = end;
            } else {
                try out.append(allocator, .{ .start = start, .end = end });
            }
        }
        base += sp.end_row_index - sp.first_row_index;
    }
    // Every kept row is in a selected page when the pages cover the row group.
    if (kept != ranges.rowCount()) return error.InvalidPageIndex;
    return out.toOwnedSlice(allocator);
}

/// A leaf with the entries of `leaf` in `ranges` (see `keptEntryRanges`) and their levels.
fn selectLeafEntries(allocator: std.mem.Allocator, leaf: *const leaf_decoder.Leaf, ranges: []const leaf_decoder.EntryRange) !leaf_decoder.Leaf {
    var buffers = try leaf.buffers.selectRanges(allocator, ranges);
    errdefer buffers.deinit(allocator);
    const def_levels = if (leaf.def_levels) |l| try selectLevels(allocator, l, ranges) else null;
    errdefer if (def_levels) |l| allocator.free(l);
    const rep_levels = if (leaf.rep_levels) |l| try selectLevels(allocator, l, ranges) else null;
    return .{ .buffers = buffers, .def_levels = def_levels, .rep_levels = rep_levels };
}

/// The levels of the entries in `ranges`.
fn selectLevels(allocator: std.mem.Allocator, levels: []const leaf_decoder.Level, ranges: []const leaf_decoder.EntryRange) ![]leaf_decoder.Level {
    var count: usize = 0;
    for (ranges) |r| {
        if (r.start > r.end or r.end > levels.len) return error.InvalidPageData;
        count += r.end - r.start;
    }
    const out = try allocator.alloc(leaf_decoder.Level, count);
    var at: usize = 0;
    for (ranges) |r| {
        @memcpy(out[at..][0 .. r.end - r.start], levels[r.start..r.end]);
        at += r.end - r.start;
    }
    return out;
}
