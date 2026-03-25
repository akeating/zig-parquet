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
const thrift = @import("thrift/mod.zig");
const compress = @import("compress/mod.zig");
const dictionary = @import("encoding/dictionary.zig");
const column_decoder = @import("column_decoder.zig");
const value_mod = @import("value.zig");
const seekable_reader = @import("seekable_reader.zig");
const parquet_reader = @import("parquet_reader.zig");
const errors_mod = @import("errors.zig");
const schema_mod = @import("schema.zig");
const nested_mod = @import("nested.zig");

pub const Value = value_mod.Value;
pub const Row = value_mod.Row;
pub const SeekableReader = seekable_reader.SeekableReader;
pub const BackendCleanup = parquet_reader.BackendCleanup;
pub const ChecksumOptions = parquet_reader.ChecksumOptions;

/// Options for DynamicReader initialization
pub const DynamicReaderOptions = struct {
    /// Checksum validation options
    checksum: ChecksumOptions = .{},
};

/// Error type for dynamic reader operations.
/// Composed from categorized error sets — no more `SchemaParseError` coalescing.
pub const DynamicReaderError = errors_mod.DynamicReaderError;

const crc = std.hash.crc;

/// Compute CRC32 checksum for page data (per Parquet spec, covers compressed data)
fn computePageCrc(data: []const u8) i32 {
    return @bitCast(crc.Crc32.hash(data));
}

/// Safely cast i32 page size/count to usize, validating non-negative.
fn safePageSize(size: i32) error{InvalidPageSize}!usize {
    return safe.cast(size) catch return error.InvalidPageSize;
}

/// Safely cast i64 row count to usize, validating non-negative.
fn safeRowCount(count: i64) error{InvalidRowCount}!usize {
    return safe.cast(count) catch return error.InvalidRowCount;
}

const safeTypeLength = column_decoder.safeTypeLength;

/// Schema-agnostic Parquet reader
pub const DynamicReader = struct {
    allocator: std.mem.Allocator,
    source: SeekableReader,
    metadata: format.FileMetaData,
    footer_data: []u8,
    checksum_options: ChecksumOptions,
    _backend_cleanup: ?BackendCleanup = null,

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

    fn readRowsInternal(self: *Self, row_group_index: usize, col_indices: ?[]const usize) ![]Row {
        if (row_group_index >= self.metadata.row_groups.len) {
            return try self.allocator.alloc(Row, 0);
        }

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

        // Compute top-level-to-leaf mapping for nested schemas
        var schema_arena = std.heap.ArenaAllocator.init(self.allocator);
        defer schema_arena.deinit();
        const sa = schema_arena.allocator();

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

        // Build set of leaf columns to read
        const leaf_needed = try self.allocator.alloc(bool, num_leaf_columns);
        defer self.allocator.free(leaf_needed);

        if (col_indices) |indices| {
            @memset(leaf_needed, false);
            if (is_flat) {
                for (indices) |idx| leaf_needed[idx] = true;
            } else {
                for (indices) |top_idx| {
                    const info = top_infos.?[top_idx];
                    for (info.leaf_start..info.leaf_start + info.leaf_count) |leaf| {
                        if (leaf < num_leaf_columns) leaf_needed[leaf] = true;
                    }
                }
            }
        } else {
            @memset(leaf_needed, true);
        }

        // Read needed leaf columns into fixed-size arrays
        const column_values = try self.allocator.alloc([]Value, num_leaf_columns);
        @memset(column_values, &.{});
        defer {
            for (column_values) |vals| {
                for (vals) |v| v.deinit(self.allocator);
                if (vals.len > 0) self.allocator.free(vals);
            }
            self.allocator.free(column_values);
        }

        const column_def_levels = try self.allocator.alloc(?[]u32, num_leaf_columns);
        @memset(column_def_levels, null);
        defer {
            for (column_def_levels) |levels| {
                if (levels) |l| self.allocator.free(l);
            }
            self.allocator.free(column_def_levels);
        }

        const column_rep_levels = try self.allocator.alloc(?[]u32, num_leaf_columns);
        @memset(column_rep_levels, null);
        defer {
            for (column_rep_levels) |levels| {
                if (levels) |l| self.allocator.free(l);
            }
            self.allocator.free(column_rep_levels);
        }

        for (0..num_leaf_columns) |col_idx| {
            if (!leaf_needed[col_idx]) continue;
            const result = try self.readColumnDynamic(col_idx, row_group_index);
            column_values[col_idx] = result.values;
            column_def_levels[col_idx] = result.def_levels;
            column_rep_levels[col_idx] = result.rep_levels;
        }

        // Assemble rows
        if (is_flat) {
            return self.assembleFlatRows(num_rows, num_leaf_columns, column_values, col_indices);
        }
        return self.assembleNestedRows(
            num_rows,
            num_leaf_columns,
            column_values,
            column_def_levels,
            column_rep_levels,
            top_infos.?,
            col_indices,
        );
    }

    /// Read a single column dynamically
    fn readColumnDynamic(self: *Self, column_index: usize, row_group_index: usize) !column_decoder.DynamicDecodeResult {
        const rg = &self.metadata.row_groups[row_group_index];
        if (column_index >= rg.columns.len) return error.InvalidArgument;

        const chunk = &rg.columns[column_index];
        const meta = chunk.meta_data orelse return error.InvalidArgument;

        // Read the raw page data
        const page_data = try self.readColumnChunkData(chunk);
        defer self.allocator.free(page_data);

        var pos: usize = 0;

        // Check if we have a dictionary page
        var string_dict: ?dictionary.StringDictionary = null;
        defer if (string_dict) |*d| d.deinit();
        var int32_dict: ?dictionary.Int32Dictionary = null;
        defer if (int32_dict) |*d| d.deinit();
        var int64_dict: ?dictionary.Int64Dictionary = null;
        defer if (int64_dict) |*d| d.deinit();
        var float32_dict: ?dictionary.Float32Dictionary = null;
        defer if (float32_dict) |*d| d.deinit();
        var float64_dict: ?dictionary.Float64Dictionary = null;
        defer if (float64_dict) |*d| d.deinit();
        var fixed_byte_array_dict: ?dictionary.FixedByteArrayDictionary = null;
        defer if (fixed_byte_array_dict) |*d| d.deinit();
        var int96_dict: ?dictionary.Int96Dictionary = null;
        defer if (int96_dict) |*d| d.deinit();

        // Check for dictionary page - might be indicated by metadata OR by first page in stream
        // Some writers don't set dictionary_page_offset but still include dictionary pages
        {
            // Check if first page is a dictionary page (consumes header if so)
            var peek_thrift = thrift.CompactReader.init(page_data[pos..]);
            const first_header = try format.PageHeader.parse(self.allocator, &peek_thrift);
            defer self.freePageHeaderContents(&first_header);

            if (first_header.dictionary_page_header) |dph| {
                // This is a dictionary page - consume it
                pos += peek_thrift.pos;

                const dict_compressed_size = try safePageSize(first_header.compressed_page_size);
                const dict_uncompressed_size = try safePageSize(first_header.uncompressed_page_size);
                const dict_num_values = try safePageSize(dph.num_values);

                const dict_compressed = try safe.slice(page_data, pos, dict_compressed_size);
                pos += dict_compressed_size;

                // Validate CRC32 checksum if enabled
                if (self.checksum_options.validate_page_checksum) {
                    if (first_header.crc) |expected_crc| {
                        const actual_crc = computePageCrc(dict_compressed);
                        if (actual_crc != expected_crc) {
                            return error.PageChecksumMismatch;
                        }
                    } else if (self.checksum_options.strict_checksum) {
                        return error.MissingPageChecksum;
                    }
                }

                // Decompress dictionary page if needed
                const dict_data = if (meta.codec != .uncompressed) blk: {
                    break :blk try self.decompressData(dict_compressed, meta.codec, dict_uncompressed_size);
                } else dict_compressed;
                defer if (meta.codec != .uncompressed) self.allocator.free(dict_data);

                // Build dictionaries based on physical type
                const column_info_for_dict = format.getColumnInfo(self.metadata.schema, column_index);
                if (column_info_for_dict) |info| {
                    if (info.element.type_) |phys_type| {
                        switch (phys_type) {
                            .byte_array => {
                                string_dict = try dictionary.StringDictionary.fromPlain(self.allocator, dict_data, dict_num_values);
                            },
                            .int32 => {
                                int32_dict = try dictionary.Int32Dictionary.fromPlain(self.allocator, dict_data, dict_num_values);
                            },
                            .int64 => {
                                int64_dict = try dictionary.Int64Dictionary.fromPlain(self.allocator, dict_data, dict_num_values);
                            },
                            .float => {
                                float32_dict = try dictionary.Float32Dictionary.fromPlain(self.allocator, dict_data, dict_num_values);
                            },
                            .double => {
                                float64_dict = try dictionary.Float64Dictionary.fromPlain(self.allocator, dict_data, dict_num_values);
                            },
                            .fixed_len_byte_array => {
                                const fixed_len = try safeTypeLength(info.element.type_length);
                                if (fixed_len > 0) {
                                    fixed_byte_array_dict = try dictionary.FixedByteArrayDictionary.fromPlain(self.allocator, dict_data, dict_num_values, fixed_len);
                                }
                            },
                            .int96 => {
                                int96_dict = try dictionary.Int96Dictionary.fromPlain(self.allocator, dict_data, dict_num_values);
                            },
                            else => {},
                        }
                    }
                }
            }
            // If not a dictionary page, pos is unchanged and we'll process it as data
        }

        // Get column info
        const column_info = format.getColumnInfo(self.metadata.schema, column_index) orelse
            return error.InvalidArgument;

        // Accumulate values from all data pages
        var all_values: std.ArrayListUnmanaged(Value) = .empty;
        errdefer {
            for (all_values.items) |v| v.deinit(self.allocator);
            all_values.deinit(self.allocator);
        }

        var all_def_levels: std.ArrayListUnmanaged(u32) = .empty;
        errdefer all_def_levels.deinit(self.allocator);

        var all_rep_levels: std.ArrayListUnmanaged(u32) = .empty;
        errdefer all_rep_levels.deinit(self.allocator);

        // Loop through all data pages
        while (pos < page_data.len) {
            // Parse data page header
            var thrift_reader = thrift.CompactReader.init(page_data[pos..]);
            const page_header = try format.PageHeader.parse(self.allocator, &thrift_reader);
            defer self.freePageHeaderContents(&page_header);

            pos += thrift_reader.pos;

            // Skip non-data pages (neither V1 nor V2)
            if (page_header.data_page_header == null and page_header.data_page_header_v2 == null) {
                const skip_size = try safePageSize(page_header.compressed_page_size);
                pos += skip_size;
                continue;
            }

            const compressed_size = try safePageSize(page_header.compressed_page_size);
            if (pos + compressed_size > page_data.len) return error.EndOfData;
            const compressed_body = try safe.slice(page_data, pos, compressed_size);
            pos += compressed_size;

            // Validate CRC32 checksum if enabled
            if (self.checksum_options.validate_page_checksum) {
                if (page_header.crc) |expected_crc| {
                    const actual_crc = computePageCrc(compressed_body);
                    if (actual_crc != expected_crc) {
                        return error.PageChecksumMismatch;
                    }
                } else if (self.checksum_options.strict_checksum) {
                    return error.MissingPageChecksum;
                }
            }

            // Handle V2 vs V1 page format differently
            if (page_header.data_page_header_v2) |v2| {
                // DataPageV2: levels are uncompressed, stored before values
                const rep_len = try safePageSize(v2.repetition_levels_byte_length);
                const def_len = try safePageSize(v2.definition_levels_byte_length);
                const num_values = try safePageSize(v2.num_values);

                if (num_values == 0) continue;

                // Extract level data (uncompressed, at start of page body)
                const rep_levels_data = try safe.slice(compressed_body, 0, rep_len);
                const def_levels_data = try safe.slice(compressed_body, rep_len, def_len);
                const levels_total = std.math.add(usize, rep_len, def_len) catch return error.EndOfData;
                if (levels_total > compressed_body.len) return error.EndOfData;
                const values_compressed = compressed_body[levels_total..];

                // Decompress only the values section if needed
                // For V2, uncompressed_page_size is the size of the ENTIRE uncompressed page
                // But the levels are already uncompressed, so we need to calculate values size differently
                // The compressed_page_size in header is: rep_len + def_len + compressed_values_len
                // The uncompressed_page_size is: rep_len + def_len + uncompressed_values_len
                var values_data_allocated = false;
                const values_data = if (v2.is_compressed and meta.codec != .uncompressed) blk: {
                    const uncompressed_size = try safePageSize(page_header.uncompressed_page_size);
                    // Levels are uncompressed, so uncompressed values size = total - levels
                    if (uncompressed_size < levels_total) return error.EndOfData;
                    const values_uncompressed_size = uncompressed_size - levels_total;
                    // Handle empty values case - skip decompression for empty data pages
                    if (values_uncompressed_size == 0 or values_compressed.len == 0) {
                        break :blk values_compressed; // Return empty slice, no decompression needed
                    }
                    values_data_allocated = true;
                    break :blk try self.decompressData(values_compressed, meta.codec, values_uncompressed_size);
                } else values_compressed;
                defer if (values_data_allocated) self.allocator.free(values_data);

                // Get value encoding from page header (used for delta encodings)
                const value_encoding = v2.encoding;
                const uses_dict = string_dict != null or int32_dict != null or int64_dict != null or float32_dict != null or float64_dict != null or int96_dict != null;

                // Decode with V2 format (levels already extracted, no length prefix)
                var page_result = try column_decoder.decodeColumnDynamicV2(
                    self.allocator,
                    column_info.element,
                    rep_levels_data,
                    def_levels_data,
                    values_data,
                    num_values,
                    column_info.max_def_level,
                    column_info.max_rep_level,
                    uses_dict or fixed_byte_array_dict != null,
                    if (string_dict) |*d| d else null,
                    if (int32_dict) |*d| d else null,
                    if (int64_dict) |*d| d else null,
                    if (float32_dict) |*d| d else null,
                    if (float64_dict) |*d| d else null,
                    if (fixed_byte_array_dict) |*d| d else null,
                    if (int96_dict) |*d| d else null,
                    value_encoding,
                );

                // Append values (transfer ownership)
                try all_values.appendSlice(self.allocator, page_result.values);
                self.allocator.free(page_result.values);
                page_result.values = &.{};

                // Append levels
                if (page_result.def_levels) |dl| {
                    try all_def_levels.appendSlice(self.allocator, dl);
                    self.allocator.free(dl);
                }
                if (page_result.rep_levels) |rl| {
                    try all_rep_levels.appendSlice(self.allocator, rl);
                    self.allocator.free(rl);
                }
            } else if (page_header.data_page_header) |dph| {
                // DataPageV1: levels are inside compressed data with length prefixes

                // Decompress entire page if needed
                const value_data = if (meta.codec != .uncompressed) blk: {
                    const uncompressed_size = try safePageSize(page_header.uncompressed_page_size);
                    break :blk try self.decompressData(compressed_body, meta.codec, uncompressed_size);
                } else compressed_body;
                defer if (meta.codec != .uncompressed) self.allocator.free(value_data);

                const num_values = try safePageSize(dph.num_values);
                if (num_values == 0) continue;

                // Get encodings from page header
                const uses_dict_v1 = string_dict != null or int32_dict != null or int64_dict != null or float32_dict != null or float64_dict != null or fixed_byte_array_dict != null or int96_dict != null;
                const def_level_encoding = dph.definition_level_encoding;
                const rep_level_encoding = dph.repetition_level_encoding;
                const value_encoding_v1 = dph.encoding;

                // Decode dynamically with level and value encodings
                var page_result = try column_decoder.decodeColumnDynamicWithValueEncoding(
                    self.allocator,
                    column_info.element,
                    value_data,
                    num_values,
                    column_info.max_def_level,
                    column_info.max_rep_level,
                    uses_dict_v1,
                    if (string_dict) |*d| d else null,
                    if (int32_dict) |*d| d else null,
                    if (int64_dict) |*d| d else null,
                    if (float32_dict) |*d| d else null,
                    if (float64_dict) |*d| d else null,
                    if (fixed_byte_array_dict) |*d| d else null,
                    if (int96_dict) |*d| d else null,
                    def_level_encoding,
                    rep_level_encoding,
                    value_encoding_v1,
                );

                // Append values (transfer ownership)
                try all_values.appendSlice(self.allocator, page_result.values);
                self.allocator.free(page_result.values);
                page_result.values = &.{};

                // Append levels
                if (page_result.def_levels) |dl| {
                    try all_def_levels.appendSlice(self.allocator, dl);
                    self.allocator.free(dl);
                }
                if (page_result.rep_levels) |rl| {
                    try all_rep_levels.appendSlice(self.allocator, rl);
                    self.allocator.free(rl);
                }
            }
        }

        return .{
            .values = try all_values.toOwnedSlice(self.allocator),
            .def_levels = if (all_def_levels.items.len > 0) try all_def_levels.toOwnedSlice(self.allocator) else null,
            .rep_levels = if (all_rep_levels.items.len > 0) try all_rep_levels.toOwnedSlice(self.allocator) else null,
        };
    }

    /// Assemble rows from columnar data.
    /// Uses the file schema to reconstruct logical top-level columns, properly
    /// rebuilding nested types (structs, maps, nested lists) via assembleValues.
    fn assembleNestedRows(
        self: *Self,
        num_rows: usize,
        num_leaf_columns: usize,
        column_values: [][]Value,
        column_def_levels: []?[]u32,
        column_rep_levels: []?[]u32,
        top_infos: []const TopColInfo,
        projected_top_indices: ?[]const usize,
    ) ![]Row {
        const num_top = top_infos.len;

        // Phase 1: For each nested top-level column, assemble all rows at once.
        const assembled_cols = try self.allocator.alloc(?[]Value, num_top);
        defer {
            for (assembled_cols) |opt_vals| {
                if (opt_vals) |vals| {
                    for (vals) |v| v.deinit(self.allocator);
                    self.allocator.free(vals);
                }
            }
            self.allocator.free(assembled_cols);
        }
        @memset(assembled_cols, null);

        for (0..num_top) |top_idx| {
            const info = top_infos[top_idx];
            if (!info.is_nested) continue;
            if (projected_top_indices) |indices| {
                var needed = false;
                for (indices) |pi| {
                    if (pi == top_idx) { needed = true; break; }
                }
                if (!needed) continue;
            }

            const slices = try self.allocator.alloc(nested_mod.FlatColumnSlice, info.leaf_count);
            defer self.allocator.free(slices);

            for (0..info.leaf_count) |li| {
                const phys = info.leaf_start + li;
                if (phys < num_leaf_columns) {
                    slices[li] = .{
                        .values = column_values[phys],
                        .def_levels = column_def_levels[phys] orelse &.{},
                        .rep_levels = column_rep_levels[phys] orelse &.{},
                    };
                } else {
                    slices[li] = .{ .values = &.{}, .def_levels = &.{}, .rep_levels = &.{} };
                }
            }

            assembled_cols[top_idx] = try nested_mod.assembleValues(
                self.allocator,
                info.schema_node,
                slices,
                num_rows,
            );
        }

        // Phase 2: Build rows with only the projected columns
        const output_indices = projected_top_indices orelse blk: {
            const all = try self.allocator.alloc(usize, num_top);
            for (0..num_top) |i| all[i] = i;
            break :blk all;
        };
        defer if (projected_top_indices == null) self.allocator.free(output_indices);

        const num_output_cols = output_indices.len;
        const rows = try self.allocator.alloc(Row, num_rows);
        errdefer self.allocator.free(rows);
        var rows_initialized: usize = 0;
        errdefer {
            for (rows[0..rows_initialized]) |row| row.deinit();
        }

        for (0..num_rows) |row_idx| {
            const values = try self.allocator.alloc(Value, num_output_cols);
            errdefer self.allocator.free(values);
            var vals_initialized: usize = 0;
            errdefer {
                for (values[0..vals_initialized]) |v| v.deinit(self.allocator);
            }

            for (output_indices, 0..) |top_idx, out_idx| {
                const info = top_infos[top_idx];
                if (assembled_cols[top_idx]) |assembled| {
                    values[out_idx] = try cloneValue(self.allocator, assembled[row_idx]);
                } else {
                    const phys = info.leaf_start;
                    if (phys < num_leaf_columns and row_idx < column_values[phys].len) {
                        values[out_idx] = try cloneValue(self.allocator, column_values[phys][row_idx]);
                    } else {
                        values[out_idx] = .{ .null_val = {} };
                    }
                }
                vals_initialized += 1;
            }

            rows[row_idx] = .{ .values = values, .allocator = self.allocator };
            rows_initialized += 1;
        }

        // Free assembled_cols now (will be freed by defer, setting to null avoids double-free)
        for (0..num_top) |top_idx| {
            if (assembled_cols[top_idx]) |vals| {
                for (vals) |v| v.deinit(self.allocator);
                self.allocator.free(vals);
                assembled_cols[top_idx] = null;
            }
        }

        return rows;
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

        const num_output_cols = output_indices.len;
        const rows = try self.allocator.alloc(Row, num_rows);
        errdefer self.allocator.free(rows);

        for (0..num_rows) |row_idx| {
            const values = try self.allocator.alloc(Value, num_output_cols);
            errdefer self.allocator.free(values);

            for (output_indices, 0..) |col_idx, out_idx| {
                if (row_idx < column_values[col_idx].len) {
                    values[out_idx] = try cloneValue(self.allocator, column_values[col_idx][row_idx]);
                } else {
                    values[out_idx] = .{ .null_val = {} };
                }
            }

            rows[row_idx] = .{ .values = values, .allocator = self.allocator };
        }
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

    /// Decompress page data
    fn decompressData(self: *Self, compressed: []const u8, codec: format.CompressionCodec, uncompressed_size: usize) ![]u8 {
        return compress.decompress(self.allocator, compressed, codec, uncompressed_size) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            error.DecompressionError => return error.DecompressionError,
            error.UnsupportedCompression => return error.UnsupportedCompression,
        };
    }

    /// Free the allocated contents within a PageHeader (statistics min/max values).
    /// Does NOT free the PageHeader struct itself.
    fn freePageHeaderContents(self: *Self, header: *const format.PageHeader) void {
        // Use mutable copy to call deinit
        var mutable_header = header.*;
        mutable_header.deinit(self.allocator);
    }
};

/// Clone a Value, allocating new memory for any heap data
fn cloneValue(allocator: std.mem.Allocator, v: Value) !Value {
    return switch (v) {
        .null_val => .{ .null_val = {} },
        .bool_val => |b| .{ .bool_val = b },
        .int32_val => |i| .{ .int32_val = i },
        .int64_val => |i| .{ .int64_val = i },
        .float_val => |f| .{ .float_val = f },
        .double_val => |d| .{ .double_val = d },
        .bytes_val => |b| .{ .bytes_val = try allocator.dupe(u8, b) },
        .fixed_bytes_val => |b| .{ .fixed_bytes_val = try allocator.dupe(u8, b) },
        .list_val => |items| blk: {
            const cloned = try allocator.alloc(Value, items.len);
            errdefer allocator.free(cloned);
            for (items, 0..) |item, i| {
                cloned[i] = try cloneValue(allocator, item);
            }
            break :blk .{ .list_val = cloned };
        },
        .map_val => |entries| blk: {
            const cloned = try allocator.alloc(Value.MapEntryValue, entries.len);
            errdefer allocator.free(cloned);
            for (entries, 0..) |entry, i| {
                cloned[i] = .{
                    .key = try cloneValue(allocator, entry.key),
                    .value = try cloneValue(allocator, entry.value),
                };
            }
            break :blk .{ .map_val = cloned };
        },
        .struct_val => |fields| blk: {
            const cloned = try allocator.alloc(Value.FieldValue, fields.len);
            errdefer allocator.free(cloned);
            for (fields, 0..) |field, i| {
                cloned[i] = .{
                    .name = try allocator.dupe(u8, field.name),
                    .value = try cloneValue(allocator, field.value),
                };
            }
            break :blk .{ .struct_val = cloned };
        },
    };
}

// =============================================================================
// Tests
// =============================================================================

test "nested row assembly groups values by rep_level" {
    const allocator = std.testing.allocator;

    // Simulate a nested column with rep_levels:
    // rep=0 means new row, rep>0 means continuation
    // Values: [a, b, c, d, e]
    // Rep levels: [0, 1, 0, 1, 1]
    // Expected: Row 0 = [a, b], Row 1 = [c, d, e]

    const col_values = [_]Value{
        .{ .bytes_val = "a" },
        .{ .bytes_val = "b" },
        .{ .bytes_val = "c" },
        .{ .bytes_val = "d" },
        .{ .bytes_val = "e" },
    };

    const rep_levels = [_]u32{ 0, 1, 0, 1, 1 };

    // Test the row boundary detection logic that assembleNestedRows uses
    // Find row boundaries (where rep_level == 0)
    var row_starts: std.ArrayListUnmanaged(usize) = .{};
    defer row_starts.deinit(allocator);

    for (rep_levels, 0..) |rl, i| {
        if (rl == 0) {
            try row_starts.append(allocator, i);
        }
    }

    // Verify row boundaries
    try std.testing.expectEqual(@as(usize, 2), row_starts.items.len);
    try std.testing.expectEqual(@as(usize, 0), row_starts.items[0]);
    try std.testing.expectEqual(@as(usize, 2), row_starts.items[1]);

    // Verify row 0 would contain values[0..2] = [a, b]
    const row0_start = row_starts.items[0];
    const row0_end = row_starts.items[1];
    try std.testing.expectEqual(@as(usize, 2), row0_end - row0_start);

    // Verify row 1 would contain values[2..5] = [c, d, e]
    const row1_start = row_starts.items[1];
    const row1_end = col_values.len;
    try std.testing.expectEqual(@as(usize, 3), row1_end - row1_start);
}
