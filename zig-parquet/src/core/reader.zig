//! Parquet file reader
//!
//! Reads Parquet files by:
//! 1. Reading the footer (last 8 bytes give footer size + magic)
//! 2. Parsing the Thrift-encoded FileMetaData from the footer
//! 3. Reading column data from row groups

const std = @import("std");
const safe = @import("safe.zig");
const format = @import("format.zig");
const thrift = @import("thrift/mod.zig");
const dictionary = @import("encoding/dictionary.zig");
const compress = @import("compress/mod.zig");
const column_decoder = @import("column_decoder.zig");
const list_decoder = @import("list_decoder.zig");
const map_decoder = @import("map_decoder.zig");
const rle = @import("encoding/rle.zig");
const plain = @import("encoding/plain.zig");
const types = @import("types.zig");
const schema_mod = @import("schema.zig");
const value_mod = @import("value.zig");
const seekable_reader = @import("seekable_reader.zig");
const parquet_reader = @import("parquet_reader.zig");

pub const Optional = types.Optional;
pub const ReaderError = types.ReaderError;
pub const ListColumn = list_decoder.ListColumn;
pub const MapColumn = map_decoder.MapColumn;
pub const MapEntry = map_decoder.MapEntry;
pub const SchemaNode = schema_mod.SchemaNode;
pub const Value = value_mod.Value;
pub const SeekableReader = seekable_reader.SeekableReader;
pub const BackendCleanup = parquet_reader.BackendCleanup;

/// Parquet file reader
pub const Reader = struct {
    allocator: std.mem.Allocator,
    source: SeekableReader,
    metadata: format.FileMetaData,
    footer_data: []u8,
    _backend_cleanup: ?BackendCleanup = null,

    const Self = @This();

    /// Get the SeekableReader interface.
    pub fn getSource(self: *const Self) SeekableReader {
        return self.source;
    }

    /// Initialize from a SeekableReader. The caller manages backend lifetime
    /// unless _backend_cleanup is set (by convenience constructors in api/zig/).
    pub fn initFromSeekable(allocator: std.mem.Allocator, source: SeekableReader) ReaderError!Self {
        const info = parquet_reader.parseFooter(allocator, source) catch |e| return e;
        return Self{
            .allocator = allocator,
            .source = source,
            .metadata = info.metadata,
            .footer_data = info.footer_data,
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
                    // Free statistics
                    if (meta.statistics) |stats| {
                        if (stats.max) |m| self.allocator.free(m);
                        if (stats.min) |m| self.allocator.free(m);
                        if (stats.max_value) |m| self.allocator.free(m);
                        if (stats.min_value) |m| self.allocator.free(m);
                    }
                    // Free geospatial statistics
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

    /// Get the schema (column names and types)
    pub fn getSchema(self: *const Self) []const format.SchemaElement {
        return self.metadata.schema;
    }

    /// Get the number of row groups
    pub fn getNumRowGroups(self: *const Self) usize {
        return self.metadata.row_groups.len;
    }

    /// Get the number of rows in a specific row group
    pub fn getRowGroupNumRows(self: *const Self, row_group_index: usize) ?i64 {
        if (row_group_index >= self.metadata.row_groups.len) return null;
        return self.metadata.row_groups[row_group_index].num_rows;
    }

    /// Get total number of rows across all row groups
    pub fn getTotalNumRows(self: *const Self) i64 {
        return self.metadata.num_rows;
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

    /// Get the physical type for a column.
    /// Returns null if the column index is out of bounds.
    pub fn getColumnType(self: *const Self, column_index: usize) ?format.PhysicalType {
        // Find the leaf column at the given index
        var leaf_idx: usize = 0;
        for (self.metadata.schema) |elem| {
            // Leaf columns have a type but no children
            if (elem.type_ != null and elem.num_children == null) {
                if (leaf_idx == column_index) {
                    return elem.type_;
                }
                leaf_idx += 1;
            }
        }
        return null;
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

    /// Free the allocated contents within a PageHeader (statistics min/max values).
    /// Does NOT free the PageHeader struct itself.
    fn freePageHeaderContents(self: *Self, header: *const format.PageHeader) void {
        if (header.data_page_header) |dph| {
            if (dph.statistics) |stats| {
                self.freeStatistics(&stats);
            }
        }
        if (header.data_page_header_v2) |v2| {
            if (v2.statistics) |stats| {
                self.freeStatistics(&stats);
            }
        }
        if (header.dictionary_page_header) |_| {
            // DictionaryPageHeader has no allocations currently
        }
    }

    /// Free Statistics allocated memory
    fn freeStatistics(self: *Self, stats: *const format.Statistics) void {
        if (stats.max) |m| self.allocator.free(m);
        if (stats.min) |m| self.allocator.free(m);
        if (stats.max_value) |m| self.allocator.free(m);
        if (stats.min_value) |m| self.allocator.free(m);
    }

    /// Decompress page data based on codec
    pub fn decompressData(self: *Self, compressed: []const u8, codec: format.CompressionCodec, uncompressed_size: usize) ![]u8 {
        return compress.decompress(self.allocator, compressed, codec, uncompressed_size) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            error.DecompressionError => return error.DecompressionError,
            error.UnsupportedCompression => return error.UnsupportedCompression,
        };
    }

    /// Get column names (leaf columns only)
    pub fn getColumnNames(self: *const Self, allocator: std.mem.Allocator) ![][]const u8 {
        // Count leaf columns first
        var count: usize = 0;
        for (self.metadata.schema) |elem| {
            if (elem.type_ != null and elem.num_children == null) {
                count += 1;
            }
        }

        var names = try allocator.alloc([]const u8, count);
        var idx: usize = 0;
        for (self.metadata.schema) |elem| {
            // Leaf nodes have a type but no children
            if (elem.type_ != null and elem.num_children == null) {
                names[idx] = elem.name;
                idx += 1;
            }
        }

        return names;
    }

    /// Read raw page data for a column chunk
    pub fn readColumnChunkData(self: *Self, chunk: *const format.ColumnChunk) ![]u8 {
        return parquet_reader.readColumnChunkData(self.allocator, self.getSource(), chunk);
    }

    /// Read values from a column (from the first row group)
    pub fn readColumn(self: *Self, column_index: usize, comptime T: type) ![]Optional(T) {
        return self.readColumnFromRowGroup(column_index, 0, T);
    }

    /// Read values from a specific row group
    pub fn readColumnFromRowGroup(self: *Self, column_index: usize, row_group_index: usize, comptime T: type) ![]Optional(T) {
        if (row_group_index >= self.metadata.row_groups.len) {
            return try self.allocator.alloc(Optional(T), 0);
        }

        const rg = &self.metadata.row_groups[row_group_index];
        if (column_index >= rg.columns.len) return error.InvalidArgument;

        const chunk = &rg.columns[column_index];
        const meta = chunk.meta_data orelse return error.InvalidArgument;

        // Read the raw page data
        const page_data = try self.readColumnChunkData(chunk);
        defer self.allocator.free(page_data);

        // Get column info including schema element and definition/repetition levels
        const column_info = format.getColumnInfo(self.metadata.schema, column_index) orelse
            return error.InvalidArgument;

        // Initialize page iterator and dictionary set
        var page_iter = parquet_reader.PageIterator.init(self.allocator, page_data);
        var dict_set = parquet_reader.DictionarySet.init(self.allocator);
        defer dict_set.deinit();

        // OWNERSHIP MODEL FOR PAGE LOOPS:
        // - `all_values` accumulates decoded values from multiple pages
        // - `errdefer` ensures cleanup if any page fails mid-decode
        // - On success, `toOwnedSlice()` transfers ownership to caller
        // - Individual string values (if T == []const u8) are owned by the list
        //   until transfer, then owned by the returned slice
        var all_values: std.ArrayListUnmanaged(Optional(T)) = .empty;
        errdefer parquet_reader.deinitOptionalArrayList(T, self.allocator, &all_values);

        // Loop through all pages using PageIterator
        while (try page_iter.next()) |page_info| {
            defer parquet_reader.freePageHeaderContents(self.allocator, &page_info.header);

            // Handle dictionary page
            if (page_info.is_dictionary) {
                if (page_info.header.dictionary_page_header) |dph| {
                    try dict_set.initFromPage(
                        page_info.body,
                        try safe.cast(dph.num_values),
                        column_info.element.type_,
                        column_info.element.type_length,
                        meta.codec,
                        try safe.cast(page_info.header.uncompressed_page_size),
                    );
                }
                continue;
            }

            // Skip non-data pages
            if (!page_info.is_data_page) continue;

            const page_header = page_info.header;
            const compressed_body = page_info.body;
            const uses_dict = dict_set.hasDictionary();

            // Handle V2 vs V1 page format differently
            if (page_header.data_page_header_v2) |v2| {
                // DataPageV2: levels are uncompressed, stored before values
                const rep_len = try safe.cast(v2.repetition_levels_byte_length);
                const def_len = try safe.cast(v2.definition_levels_byte_length);
                const num_values = try safe.cast(v2.num_values);

                if (num_values == 0) continue;

                // Extract level data (uncompressed, at start of page body)
                const rep_levels_data = compressed_body[0..rep_len];
                const def_levels_data = try safe.slice(compressed_body, rep_len, def_len);
                const values_compressed = compressed_body[rep_len + def_len ..];

                // Decompress only the values section if needed
                var values_data_allocated = false;
                const values_data = if (v2.is_compressed and meta.codec != .uncompressed) blk: {
                    const uncompressed_size = try safe.cast(page_header.uncompressed_page_size);
                    if (uncompressed_size < rep_len + def_len) return error.EndOfData;
                    const values_uncompressed_size = uncompressed_size - rep_len - def_len;
                    // Handle empty values case - skip decompression for empty data pages
                    if (values_uncompressed_size == 0 or values_compressed.len == 0) {
                        break :blk values_compressed;
                    }
                    values_data_allocated = true;
                    break :blk try self.decompressData(values_compressed, meta.codec, values_uncompressed_size);
                } else values_compressed;
                defer if (values_data_allocated) self.allocator.free(values_data);

                // Decode with V2 format
                const decode_result = try column_decoder.decodeColumnV2(
                    T,
                    self.allocator,
                    column_info.element,
                    rep_levels_data,
                    def_levels_data,
                    values_data,
                    num_values,
                    column_info.max_def_level,
                    column_info.max_rep_level,
                    uses_dict,
                    if (dict_set.string_dict) |*d| d else null,
                    if (dict_set.int32_dict) |*d| d else null,
                    if (dict_set.int64_dict) |*d| d else null,
                    if (dict_set.fixed_byte_array_dict) |*d| d else null,
                    v2.encoding,
                );
                defer {
                    if (decode_result.def_levels) |dl| self.allocator.free(dl);
                    if (decode_result.rep_levels) |rl| self.allocator.free(rl);
                    self.allocator.free(decode_result.values);
                }

                // Append values to accumulator
                try all_values.appendSlice(self.allocator, decode_result.values);
            } else if (page_header.data_page_header) |dph| {
                // DataPageV1: levels are inside compressed data with length prefixes

                // Decompress entire page if needed
                const value_data = if (meta.codec != .uncompressed) blk: {
                    const uncompressed_size = try safe.cast(page_header.uncompressed_page_size);
                    break :blk try self.decompressData(compressed_body, meta.codec, uncompressed_size);
                } else compressed_body;
                defer if (meta.codec != .uncompressed) self.allocator.free(value_data);

                const num_values = try safe.cast(dph.num_values);
                if (num_values == 0) continue;

                // Decode the column values for this page
                const ctx = column_decoder.DecodeContext{
                    .allocator = self.allocator,
                    .schema_elem = column_info.element,
                    .num_values = num_values,
                    .uses_dict = uses_dict,
                    .string_dict = if (dict_set.string_dict) |*d| d else null,
                    .int32_dict = if (dict_set.int32_dict) |*d| d else null,
                    .int64_dict = if (dict_set.int64_dict) |*d| d else null,
                    .float32_dict = if (dict_set.float32_dict) |*d| d else null,
                    .float64_dict = if (dict_set.float64_dict) |*d| d else null,
                    .int96_dict = if (dict_set.int96_dict) |*d| d else null,
                    .fixed_byte_array_dict = if (dict_set.fixed_byte_array_dict) |*d| d else null,
                    .max_definition_level = column_info.max_def_level,
                    .max_repetition_level = column_info.max_rep_level,
                    .value_encoding = dph.encoding,
                };

                const page_values = try column_decoder.decodeColumn(T, ctx, value_data);
                defer self.allocator.free(page_values);

                // Append values to accumulator
                try all_values.appendSlice(self.allocator, page_values);
            }
        }

        return try all_values.toOwnedSlice(self.allocator);
    }

    /// Read a list column (from the first row group)
    /// Returns nested slices: []Optional([]Optional(T))
    pub fn readListColumn(self: *Self, column_index: usize, comptime T: type) !ListColumn(T) {
        return self.readListColumnFromRowGroup(column_index, 0, T);
    }

    /// Read a list column from a specific row group
    pub fn readListColumnFromRowGroup(self: *Self, column_index: usize, row_group_index: usize, comptime T: type) !ListColumn(T) {
        if (row_group_index >= self.metadata.row_groups.len) {
            return try self.allocator.alloc(Optional([]Optional(T)), 0);
        }

        const rg = &self.metadata.row_groups[row_group_index];
        if (column_index >= rg.columns.len) return error.InvalidArgument;

        const chunk = &rg.columns[column_index];
        const meta = chunk.meta_data orelse return error.InvalidArgument;

        // Get column info for level computation
        const column_info = format.getColumnInfo(self.metadata.schema, column_index) orelse
            return error.InvalidArgument;
        const max_def_level = column_info.max_def_level;
        const max_rep_level = column_info.max_rep_level;

        // Read the raw page data
        const page_data = try self.readColumnChunkData(chunk);
        defer self.allocator.free(page_data);

        // Initialize page iterator and dictionary set
        var page_iter = parquet_reader.PageIterator.init(self.allocator, page_data);
        var dict_set = parquet_reader.DictionarySet.init(self.allocator);
        defer dict_set.deinit();

        // Accumulators for multi-page support
        var all_rep_levels: std.ArrayListUnmanaged(u32) = .empty;
        defer all_rep_levels.deinit(self.allocator);
        var all_def_levels: std.ArrayListUnmanaged(u32) = .empty;
        defer all_def_levels.deinit(self.allocator);
        var all_flat_values: std.ArrayListUnmanaged(T) = .empty;
        defer parquet_reader.deinitArrayList(T, self.allocator, &all_flat_values);

        // Loop through all pages using PageIterator
        while (try page_iter.next()) |page_info| {
            defer parquet_reader.freePageHeaderContents(self.allocator, &page_info.header);

            // Handle dictionary page
            if (page_info.is_dictionary) {
                if (page_info.header.dictionary_page_header) |dph| {
                    // For lists, use the element type to determine dictionary type
                    const physical_type: ?format.PhysicalType = if (T == []const u8)
                        .byte_array
                    else if (T == i32)
                        .int32
                    else if (T == i64)
                        .int64
                    else
                        null;

                    try dict_set.initFromPage(
                        page_info.body,
                        try safe.cast(dph.num_values),
                        physical_type,
                        column_info.element.type_length,
                        meta.codec,
                        try safe.cast(page_info.header.uncompressed_page_size),
                    );
                }
                continue;
            }

            // Skip non-data pages
            if (!page_info.is_data_page) continue;

            const page_header = page_info.header;
            const compressed_body = page_info.body;

            // Handle V2 vs V1 page format differently
            if (page_header.data_page_header_v2) |v2| {
                // DataPageV2: levels are uncompressed, stored before values
                const rep_len = try safe.cast(v2.repetition_levels_byte_length);
                const def_len = try safe.cast(v2.definition_levels_byte_length);
                const num_values = try safe.cast(v2.num_values);

                if (num_values == 0) continue;

                // Extract level data (uncompressed, at start of page body)
                const rep_levels_data = compressed_body[0..rep_len];
                const def_levels_data = try safe.slice(compressed_body, rep_len, def_len);
                const values_compressed = compressed_body[rep_len + def_len ..];

                // Decompress only the values section if needed
                var values_data_allocated_nested = false;
                const values_data = if (v2.is_compressed and meta.codec != .uncompressed) blk: {
                    const uncompressed_size = try safe.cast(page_header.uncompressed_page_size);
                    if (uncompressed_size < rep_len + def_len) return error.EndOfData;
                    const values_uncompressed_size = uncompressed_size - rep_len - def_len;
                    // Handle empty values case - skip decompression for empty data pages
                    if (values_uncompressed_size == 0 or values_compressed.len == 0) {
                        break :blk values_compressed;
                    }
                    values_data_allocated_nested = true;
                    break :blk try self.decompressData(values_compressed, meta.codec, values_uncompressed_size);
                } else values_compressed;
                defer if (values_data_allocated_nested) self.allocator.free(values_data);

                // Decode levels for V2 format (RLE without length prefix)
                var rep_levels: []u32 = undefined;
                if (max_rep_level > 0 and rep_len > 0) {
                    const rep_bit_width = format.computeBitWidth(max_rep_level);
                    rep_levels = try rle.decodeLevels(self.allocator, rep_levels_data, rep_bit_width, num_values);
                } else {
                    rep_levels = try self.allocator.alloc(u32, num_values);
                    @memset(rep_levels, 0);
                }
                defer self.allocator.free(rep_levels);

                var def_levels: []u32 = undefined;
                if (max_def_level > 0 and def_len > 0) {
                    const def_bit_width = format.computeBitWidth(max_def_level);
                    def_levels = try rle.decodeLevels(self.allocator, def_levels_data, def_bit_width, num_values);
                } else {
                    def_levels = try self.allocator.alloc(u32, num_values);
                    @memset(def_levels, max_def_level);
                }
                defer self.allocator.free(def_levels);

                // Count actual values (where def_level == max_def_level)
                var value_count: usize = 0;
                for (def_levels) |def| {
                    if (def == max_def_level) value_count += 1;
                }

                // Decode the actual values
                const flat_values = try self.decodeFlatValuesWithDictSet(T, values_data, value_count, &dict_set);
                defer self.allocator.free(flat_values);

                // Append to accumulators
                try all_rep_levels.appendSlice(self.allocator, rep_levels);
                try all_def_levels.appendSlice(self.allocator, def_levels);

                if (T == []const u8) {
                    for (flat_values) |v| {
                        try all_flat_values.append(self.allocator, v);
                    }
                } else {
                    try all_flat_values.appendSlice(self.allocator, flat_values);
                }
            } else if (page_header.data_page_header) |dph| {
                // DataPageV1: levels are inside compressed data with length prefixes

                // Decompress entire page if needed
                const value_data = if (meta.codec != .uncompressed) blk: {
                    const uncompressed_size = try safe.cast(page_header.uncompressed_page_size);
                    break :blk try self.decompressData(compressed_body, meta.codec, uncompressed_size);
                } else compressed_body;
                defer if (meta.codec != .uncompressed) self.allocator.free(value_data);

                const num_values = try safe.cast(dph.num_values);
                if (num_values == 0) continue;

                // Decode levels and values for this page
                var data_offset: usize = 0;

                const rep_level_encoding = dph.repetition_level_encoding;
                const def_level_encoding = dph.definition_level_encoding;

                // Decode repetition levels
                var rep_levels: []u32 = undefined;
                if (max_rep_level > 0) {
                    const rep_bit_width = format.computeBitWidth(max_rep_level);

                    if (rep_level_encoding == .bit_packed) {
                        const rep_bytes = rle.bitPackedSize(num_values, rep_bit_width);
                        const rep_levels_data = try safe.slice(value_data, data_offset, rep_bytes);
                        data_offset += rep_bytes;
                        rep_levels = try rle.decodeBitPackedLevels(self.allocator, rep_levels_data, rep_bit_width, num_values);
                    } else {
                        const rep_levels_len = try plain.decodeU32(value_data[data_offset..]);
                        data_offset += 4;
                        const rep_levels_data = try safe.slice(value_data, data_offset, rep_levels_len);
                        data_offset += rep_levels_len;
                        rep_levels = try rle.decodeLevels(self.allocator, rep_levels_data, rep_bit_width, num_values);
                    }
                } else {
                    rep_levels = try self.allocator.alloc(u32, num_values);
                    @memset(rep_levels, 0);
                }
                defer self.allocator.free(rep_levels);

                // Decode definition levels
                var def_levels: []u32 = undefined;
                if (max_def_level > 0) {
                    const def_bit_width = format.computeBitWidth(max_def_level);

                    if (def_level_encoding == .bit_packed) {
                        const def_bytes = rle.bitPackedSize(num_values, def_bit_width);
                        const def_levels_data = try safe.slice(value_data, data_offset, def_bytes);
                        data_offset += def_bytes;
                        def_levels = try rle.decodeBitPackedLevels(self.allocator, def_levels_data, def_bit_width, num_values);
                    } else {
                        const def_levels_len = try plain.decodeU32(value_data[data_offset..]);
                        data_offset += 4;
                        const def_levels_data = try safe.slice(value_data, data_offset, def_levels_len);
                        data_offset += def_levels_len;
                        def_levels = try rle.decodeLevels(self.allocator, def_levels_data, def_bit_width, num_values);
                    }
                } else {
                    def_levels = try self.allocator.alloc(u32, num_values);
                    @memset(def_levels, max_def_level);
                }
                defer self.allocator.free(def_levels);

                // Count actual values (where def_level == max_def_level)
                var value_count: usize = 0;
                for (def_levels) |def| {
                    if (def == max_def_level) value_count += 1;
                }

                // Decode the actual values
                const values_data = value_data[data_offset..];
                const flat_values = try self.decodeFlatValuesWithDictSet(T, values_data, value_count, &dict_set);
                defer self.allocator.free(flat_values);

                // Append to accumulators
                try all_rep_levels.appendSlice(self.allocator, rep_levels);
                try all_def_levels.appendSlice(self.allocator, def_levels);

                if (T == []const u8) {
                    for (flat_values) |v| {
                        try all_flat_values.append(self.allocator, v);
                    }
                } else {
                    try all_flat_values.appendSlice(self.allocator, flat_values);
                }
            }
        }

        // Assemble the list structure from accumulated data
        return list_decoder.assembleList(
            T,
            self.allocator,
            all_flat_values.items,
            all_def_levels.items,
            all_rep_levels.items,
            max_def_level,
            max_rep_level,
        );
    }

    /// Decode flat values from page data using DictionarySet
    fn decodeFlatValuesWithDictSet(
        self: *Self,
        comptime T: type,
        data: []const u8,
        count: usize,
        dict_set: *parquet_reader.DictionarySet,
    ) ![]T {
        var result = try self.allocator.alloc(T, count);
        errdefer self.allocator.free(result);

        if (T == i32) {
            if (dict_set.int32_dict) |*dict| {
                // Dictionary encoded
                if (data.len == 0) return error.EndOfData;
                if (data[0] > 31) return error.InvalidBitWidth;
                const bit_width: u5 = @truncate(data[0]); // Safe: validated above
                const indices_data = data[1..];
                const indices = try rle.decode(self.allocator, indices_data, bit_width, count);
                defer self.allocator.free(indices);

                for (0..count) |i| {
                    if (dict.get(indices[i])) |v| {
                        result[i] = v;
                    } else {
                        result[i] = 0;
                    }
                }
            } else {
                // Plain encoded
                var offset: usize = 0;
                for (0..count) |i| {
                    result[i] = try plain.decodeI32(data[offset..]);
                    offset += 4;
                }
            }
        } else if (T == i64) {
            if (dict_set.int64_dict) |*dict| {
                // Dictionary encoded
                if (data.len == 0) return error.EndOfData;
                if (data[0] > 31) return error.InvalidBitWidth;
                const bit_width: u5 = @truncate(data[0]); // Safe: validated above
                const indices_data = data[1..];
                const indices = try rle.decode(self.allocator, indices_data, bit_width, count);
                defer self.allocator.free(indices);

                for (0..count) |i| {
                    if (dict.get(indices[i])) |v| {
                        result[i] = v;
                    } else {
                        result[i] = 0;
                    }
                }
            } else {
                // Plain encoded
                var offset: usize = 0;
                for (0..count) |i| {
                    result[i] = try plain.decodeI64(data[offset..]);
                    offset += 8;
                }
            }
        } else if (T == f32) {
            var offset: usize = 0;
            for (0..count) |i| {
                result[i] = try plain.decodeFloat(data[offset..]);
                offset += 4;
            }
        } else if (T == f64) {
            var offset: usize = 0;
            for (0..count) |i| {
                result[i] = try plain.decodeDouble(data[offset..]);
                offset += 8;
            }
        } else if (T == bool) {
            for (0..count) |i| {
                result[i] = try plain.decodeBool(data, i);
            }
        } else if (T == []const u8) {
            // Check if dictionary encoded
            if (dict_set.string_dict) |*dict| {
                // Dictionary encoded - first byte is bit width
                if (data.len == 0) return error.EndOfData;
                if (data[0] > 31) return error.InvalidBitWidth;
                const bit_width: u5 = @truncate(data[0]); // Safe: validated above
                const indices_data = data[1..];
                const indices = try rle.decode(self.allocator, indices_data, bit_width, count);
                defer self.allocator.free(indices);

                for (0..count) |i| {
                    if (dict.get(indices[i])) |v| {
                        result[i] = try self.allocator.dupe(u8, v);
                    } else {
                        result[i] = try self.allocator.dupe(u8, "");
                    }
                }
            } else {
                // Plain encoded byte arrays
                var offset: usize = 0;
                for (0..count) |i| {
                    const ba = try plain.decodeByteArray(data[offset..]);
                    result[i] = try self.allocator.dupe(u8, ba.value);
                    offset += ba.bytes_read;
                }
            }
        } else {
            @compileError("Unsupported type for list values");
        }

        return result;
    }

    /// Free a list column returned by readListColumn
    pub fn freeListColumn(self: *Self, comptime T: type, list: ListColumn(T)) void {
        list_decoder.freeListColumn(T, self.allocator, list);
    }

    // =========================================================================
    // Map column reading
    // =========================================================================

    /// Read a map column (from the first row group)
    /// Returns nested slices: []Optional([]MapEntry(K, V))
    pub fn readMapColumn(self: *Self, column_index: usize, comptime K: type, comptime V: type) !MapColumn(K, V) {
        return self.readMapColumnFromRowGroup(column_index, 0, K, V);
    }

    /// Read a map column from a specific row group
    /// column_index is the index of the KEY column (value column is column_index + 1)
    pub fn readMapColumnFromRowGroup(self: *Self, column_index: usize, row_group_index: usize, comptime K: type, comptime V: type) !MapColumn(K, V) {
        if (row_group_index >= self.metadata.row_groups.len) {
            return try self.allocator.alloc(Optional([]MapEntry(K, V)), 0);
        }

        const rg = &self.metadata.row_groups[row_group_index];
        // Need both key (column_index) and value (column_index + 1) columns
        if (column_index + 1 >= rg.columns.len) return error.InvalidArgument;

        // Get column info for both columns
        const key_info = format.getColumnInfo(self.metadata.schema, column_index) orelse
            return error.InvalidArgument;
        const value_info = format.getColumnInfo(self.metadata.schema, column_index + 1) orelse
            return error.InvalidArgument;

        const key_max_def = key_info.max_def_level;
        const value_max_def = value_info.max_def_level;
        const max_rep_level = key_info.max_rep_level;

        // Read key column data
        const key_chunk = &rg.columns[column_index];
        const key_meta = key_chunk.meta_data orelse return error.InvalidArgument;
        const key_page_data = try self.readColumnChunkData(key_chunk);
        defer self.allocator.free(key_page_data);

        // Read value column data
        const value_chunk = &rg.columns[column_index + 1];
        const value_meta = value_chunk.meta_data orelse return error.InvalidArgument;
        const value_page_data = try self.readColumnChunkData(value_chunk);
        defer self.allocator.free(value_page_data);

        // Parse and decode key column
        const key_result = try self.decodeMapColumn(K, key_page_data, key_meta, key_max_def, max_rep_level, key_info);
        defer self.allocator.free(key_result.def_levels);
        defer self.allocator.free(key_result.rep_levels);
        defer {
            if (K == []const u8) {
                for (key_result.values) |v| self.allocator.free(v);
            }
            self.allocator.free(key_result.values);
        }

        // Parse and decode value column
        const value_result = try self.decodeMapColumn(V, value_page_data, value_meta, value_max_def, max_rep_level, value_info);
        defer self.allocator.free(value_result.def_levels);
        defer self.allocator.free(value_result.rep_levels);
        defer {
            if (V == []const u8) {
                for (value_result.values) |v| self.allocator.free(v);
            }
            self.allocator.free(value_result.values);
        }

        // Assemble the map structure
        return map_decoder.assembleMap(
            K,
            V,
            self.allocator,
            key_result.values,
            value_result.values,
            key_result.def_levels,
            value_result.def_levels,
            key_result.rep_levels,
            key_max_def,
            value_max_def,
        );
    }

    /// Decode a single column for map reading (key or value)
    /// Supports multiple data pages within a column chunk.
    fn decodeMapColumn(
        self: *Self,
        comptime T: type,
        page_data: []const u8,
        meta: format.ColumnMetaData,
        max_def_level: u8,
        max_rep_level: u8,
        column_info: format.ColumnInfo,
    ) !struct { values: []T, def_levels: []u32, rep_levels: []u32 } {
        // Initialize page iterator and dictionary set
        var page_iter = parquet_reader.PageIterator.init(self.allocator, page_data);
        var dict_set = parquet_reader.DictionarySet.init(self.allocator);
        defer dict_set.deinit();

        // Accumulators for multi-page support
        var all_values: std.ArrayListUnmanaged(T) = .empty;
        defer parquet_reader.deinitArrayList(T, self.allocator, &all_values);
        var all_def_levels: std.ArrayListUnmanaged(u32) = .empty;
        defer all_def_levels.deinit(self.allocator);
        var all_rep_levels: std.ArrayListUnmanaged(u32) = .empty;
        defer all_rep_levels.deinit(self.allocator);

        // Loop through all pages using PageIterator
        while (try page_iter.next()) |page_info| {
            defer parquet_reader.freePageHeaderContents(self.allocator, &page_info.header);

            // Handle dictionary page
            if (page_info.is_dictionary) {
                if (page_info.header.dictionary_page_header) |dph| {
                    // For maps, use the element type to determine dictionary type
                    const physical_type: ?format.PhysicalType = if (T == []const u8)
                        .byte_array
                    else if (T == i32)
                        .int32
                    else if (T == i64)
                        .int64
                    else
                        null;

                    try dict_set.initFromPage(
                        page_info.body,
                        try safe.cast(dph.num_values),
                        physical_type,
                        column_info.element.type_length,
                        meta.codec,
                        try safe.cast(page_info.header.uncompressed_page_size),
                    );
                }
                continue;
            }

            // Skip non-data pages
            if (!page_info.is_data_page) continue;

            const page_header = page_info.header;
            const compressed_body = page_info.body;

            // Handle V2 vs V1 page format differently
            if (page_header.data_page_header_v2) |v2| {
                // DataPageV2: levels are uncompressed, stored before values
                const rep_len = try safe.cast(v2.repetition_levels_byte_length);
                const def_len = try safe.cast(v2.definition_levels_byte_length);
                const num_values = try safe.cast(v2.num_values);

                if (num_values == 0) continue;

                // Extract level data (uncompressed, at start of page body)
                const rep_levels_data = compressed_body[0..rep_len];
                const def_levels_data = try safe.slice(compressed_body, rep_len, def_len);
                const values_compressed = compressed_body[rep_len + def_len ..];

                // Decompress only the values section if needed
                var values_data_allocated_flat = false;
                const values_data = if (v2.is_compressed and meta.codec != .uncompressed) blk: {
                    const uncompressed_size = try safe.cast(page_header.uncompressed_page_size);
                    if (uncompressed_size < rep_len + def_len) return error.EndOfData;
                    const values_uncompressed_size = uncompressed_size - rep_len - def_len;
                    // Handle empty values case - skip decompression for empty data pages
                    if (values_uncompressed_size == 0 or values_compressed.len == 0) {
                        break :blk values_compressed;
                    }
                    values_data_allocated_flat = true;
                    break :blk try self.decompressData(values_compressed, meta.codec, values_uncompressed_size);
                } else values_compressed;
                defer if (values_data_allocated_flat) self.allocator.free(values_data);

                // Decode levels for V2 format (RLE without length prefix)
                var page_rep_levels: []u32 = undefined;
                if (max_rep_level > 0 and rep_len > 0) {
                    const rep_bit_width = format.computeBitWidth(max_rep_level);
                    page_rep_levels = try rle.decodeLevels(self.allocator, rep_levels_data, rep_bit_width, num_values);
                } else {
                    page_rep_levels = try self.allocator.alloc(u32, num_values);
                    @memset(page_rep_levels, 0);
                }
                defer self.allocator.free(page_rep_levels);

                var page_def_levels: []u32 = undefined;
                if (max_def_level > 0 and def_len > 0) {
                    const def_bit_width = format.computeBitWidth(max_def_level);
                    page_def_levels = try rle.decodeLevels(self.allocator, def_levels_data, def_bit_width, num_values);
                } else {
                    page_def_levels = try self.allocator.alloc(u32, num_values);
                    @memset(page_def_levels, max_def_level);
                }
                defer self.allocator.free(page_def_levels);

                // Count actual values (where def_level == max_def_level)
                var value_count: usize = 0;
                for (page_def_levels) |def| {
                    if (def == max_def_level) value_count += 1;
                }

                // Decode the actual values
                const page_values = try self.decodeFlatValuesWithDictSet(T, values_data, value_count, &dict_set);
                defer self.allocator.free(page_values);

                // Append to accumulators
                try all_rep_levels.appendSlice(self.allocator, page_rep_levels);
                try all_def_levels.appendSlice(self.allocator, page_def_levels);
                try all_values.appendSlice(self.allocator, page_values);
            } else {
                // DataPageV1: levels are inside compressed data with length prefixes

                // Decompress entire page if needed
                const value_data = if (meta.codec != .uncompressed) blk: {
                    const uncompressed_size = try safe.cast(page_header.uncompressed_page_size);
                    break :blk try self.decompressData(compressed_body, meta.codec, uncompressed_size);
                } else compressed_body;
                defer if (meta.codec != .uncompressed) self.allocator.free(value_data);

                // Get number of values
                const num_values = if (page_header.data_page_header) |dph|
                    try safe.cast(dph.num_values)
                else
                    try safe.cast(meta.num_values);

                if (num_values == 0) continue;

                var data_offset: usize = 0;

                // Decode repetition levels
                var page_rep_levels: []u32 = undefined;
                if (max_rep_level > 0) {
                    const rep_levels_len = try plain.decodeU32(value_data[data_offset..]);
                    data_offset += 4;
                    const rep_levels_data = try safe.slice(value_data, data_offset, rep_levels_len);
                    data_offset += rep_levels_len;

                    const rep_bit_width = format.computeBitWidth(max_rep_level);
                    page_rep_levels = try rle.decodeLevels(self.allocator, rep_levels_data, rep_bit_width, num_values);
                } else {
                    page_rep_levels = try self.allocator.alloc(u32, num_values);
                    @memset(page_rep_levels, 0);
                }
                defer self.allocator.free(page_rep_levels);

                // Decode definition levels
                var page_def_levels: []u32 = undefined;
                if (max_def_level > 0) {
                    const def_levels_len = try plain.decodeU32(value_data[data_offset..]);
                    data_offset += 4;
                    const def_levels_data = try safe.slice(value_data, data_offset, def_levels_len);
                    data_offset += def_levels_len;

                    const def_bit_width = format.computeBitWidth(max_def_level);
                    page_def_levels = try rle.decodeLevels(self.allocator, def_levels_data, def_bit_width, num_values);
                } else {
                    page_def_levels = try self.allocator.alloc(u32, num_values);
                    @memset(page_def_levels, max_def_level);
                }
                defer self.allocator.free(page_def_levels);

                // Count actual values (where def_level == max_def_level)
                var value_count: usize = 0;
                for (page_def_levels) |def| {
                    if (def == max_def_level) value_count += 1;
                }

                // Decode the actual values
                const values_section = value_data[data_offset..];
                const page_values = try self.decodeFlatValuesWithDictSet(T, values_section, value_count, &dict_set);
                defer self.allocator.free(page_values);

                // Append to accumulators
                try all_rep_levels.appendSlice(self.allocator, page_rep_levels);
                try all_def_levels.appendSlice(self.allocator, page_def_levels);
                try all_values.appendSlice(self.allocator, page_values);
            }
        }

        // Transfer ownership from accumulators to returned slices
        const result_values = try all_values.toOwnedSlice(self.allocator);
        errdefer {
            if (T == []const u8) {
                for (result_values) |v| self.allocator.free(v);
            }
            self.allocator.free(result_values);
        }
        const result_def_levels = try all_def_levels.toOwnedSlice(self.allocator);
        errdefer self.allocator.free(result_def_levels);
        const result_rep_levels = try all_rep_levels.toOwnedSlice(self.allocator);

        return .{
            .values = result_values,
            .def_levels = result_def_levels,
            .rep_levels = result_rep_levels,
        };
    }

    /// Free a map column allocated by readMapColumn
    pub fn freeMapColumn(self: *Self, comptime K: type, comptime V: type, map: MapColumn(K, V)) void {
        map_decoder.freeMapColumn(K, V, self.allocator, map);
    }
};
