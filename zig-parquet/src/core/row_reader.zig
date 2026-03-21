//! Row Reader
//!
//! High-level row-oriented API for reading Parquet files.
//! Provides a comptime-generic interface that reads Parquet data into Zig structs,
//! symmetric to RowWriter.
//!
//! ## Example
//! ```zig
//! const parquet = @import("parquet");
//!
//! const SensorReading = struct {
//!     sensor_id: i32,
//!     value: i64,
//!     temperature: f64,
//! };
//!
//! var reader = try parquet.RowReader(SensorReading).init(allocator, file);
//! defer reader.deinit();
//!
//! while (try reader.next()) |row| {
//!     defer reader.freeRow(row);
//!     // use row...
//! }
//! ```

const std = @import("std");
const safe = @import("safe.zig");
const format = @import("format.zig");
const types = @import("types.zig");
const errors_mod = @import("errors.zig");
const struct_utils = @import("struct_utils.zig");
const column_decoder = @import("column_decoder.zig");
const list_decoder = @import("list_decoder.zig");
const thrift = @import("thrift/mod.zig");
const compress = @import("compress/mod.zig");
const dictionary = @import("encoding/dictionary.zig");
const seekable_reader = @import("seekable_reader.zig");
const parquet_reader = @import("parquet_reader.zig");

const Optional = types.Optional;
pub const SeekableReader = seekable_reader.SeekableReader;
pub const BackendCleanup = parquet_reader.BackendCleanup;

// Import shared utilities
const FlatField = struct_utils.FlatField;
const ListFieldContext = struct_utils.ListFieldContext;
const FlattenedFields = struct_utils.FlattenedFields;
const countLeafFields = struct_utils.countLeafFields;
const countBufferFields = struct_utils.countBufferFields;
const isNestedStruct = struct_utils.isNestedStruct;
const isSliceType = struct_utils.isSliceType;
const isOptionalType = struct_utils.isOptionalType;
const unwrapOptionalType = struct_utils.unwrapOptionalType;
const SliceElementType = struct_utils.SliceElementType;
const getSliceInfo = struct_utils.getSliceInfo;
const isTimestampType = struct_utils.isTimestampType;
const isTimeType = struct_utils.isTimeType;
const isInt96TimestampType = struct_utils.isInt96TimestampType;
const isDecimalType = struct_utils.isDecimalType;

/// Error type for row reader operations.
/// Composed from categorized error sets — no more `SchemaParseError` coalescing.
pub const RowReaderError = errors_mod.ReaderError || error{
    SchemaMismatch,
    InvalidDecimalLength,
};

/// Safely cast i32 page size/count to usize, validating non-negative.
fn safePageSize(size: i32) error{InvalidPageSize}!usize {
    return safe.cast(size) catch return error.InvalidPageSize;
}

/// Row reader options
pub const RowReaderOptions = struct {
    /// Row group to read from (default: 0)
    row_group_index: usize = 0,

    /// Allocation limits for untrusted input protection
    limits: parquet_reader.AllocationLimits = .{},
};

/// Comptime-generic row reader for Parquet files
pub fn RowReader(comptime T: type) type {
    // Validate T is a struct
    const type_info = @typeInfo(T);
    if (type_info != .@"struct") {
        @compileError("RowReader requires a struct type, got: " ++ @typeName(T));
    }

    // Generate flattened field descriptors at comptime
    const FlatFields = FlattenedFields(T);
    const column_count = FlatFields.count;

    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        source: SeekableReader,
        metadata: format.FileMetaData,
        footer_data: []u8,
        _backend_cleanup: ?BackendCleanup = null,

        column_data: [column_count]ColumnBuffer,

        current_row: usize,
        total_rows: usize,

        row_group_index: usize,

        /// Get the SeekableReader interface.
        pub fn getSource(self: *const Self) SeekableReader {
            return self.source;
        }

        /// Buffer for a single column's data
        const ColumnBuffer = struct {
            /// Raw values pointer (type-erased)
            values_ptr: [*]u8,
            /// Number of values
            num_values: usize,
            /// Alignment of the original allocation
            alignment: u8,
            /// Size of each element
            element_size: usize,
            /// Definition levels (for optional fields)
            def_levels: ?[]u32,
            /// Repetition levels (for list fields)
            rep_levels: ?[]u32,
            /// Whether this column contains byte arrays that need individual cleanup
            is_byte_array: bool,
            /// Maximum definition level for this column (used for list assembly)
            max_def_level: u8 = 0,

            fn deinit(self: *ColumnBuffer, allocator: std.mem.Allocator) void {
                if (self.num_values > 0) {
                    // If this column contains byte arrays, free each individual value first
                    if (self.is_byte_array) {
                        const OptionalByteArray = Optional([]const u8);
                        const values: [*]OptionalByteArray = @ptrCast(@alignCast(self.values_ptr));
                        for (0..self.num_values) |i| {
                            switch (values[i]) {
                                .value => |v| allocator.free(v),
                                .null_value => {},
                            }
                        }
                    }

                    // Free the values array itself
                    const total_size = self.num_values * self.element_size;
                    const slice = self.values_ptr[0..total_size];
                    const log2_align: std.mem.Alignment = @enumFromInt(std.math.log2_int(u8, self.alignment));
                    allocator.rawFree(slice, log2_align, @returnAddress());
                }
                if (self.def_levels) |dl| allocator.free(dl);
                if (self.rep_levels) |rl| allocator.free(rl);
                self.* = .{
                    .values_ptr = undefined,
                    .num_values = 0,
                    .alignment = 1,
                    .element_size = 0,
                    .def_levels = null,
                    .rep_levels = null,
                    .is_byte_array = false,
                };
            }
        };

        /// Initialize from a SeekableReader. The caller manages backend lifetime
        /// unless _backend_cleanup is set (by convenience constructors in api/zig/).
        pub fn initFromSeekable(allocator: std.mem.Allocator, source: SeekableReader, options: RowReaderOptions) RowReaderError!Self {
            var self: Self = undefined;
            self.source = source;
            self._backend_cleanup = null;
            return initWithSource(allocator, &self, options);
        }

        fn initWithSource(allocator: std.mem.Allocator, self: *Self, options: RowReaderOptions) RowReaderError!Self {
            const info = try parquet_reader.parseFooter(allocator, self.getSource());

            const metadata = info.metadata;
            const footer_data = info.footer_data;

            // errdefer to clean up metadata if init fails later
            errdefer {
                // Free schema element names
                for (metadata.schema) |elem| {
                    if (elem.name.len > 0) {
                        allocator.free(elem.name);
                    }
                }
                allocator.free(metadata.schema);

                // Free row groups
                for (metadata.row_groups) |rg| {
                    for (rg.columns) |col| {
                        if (col.meta_data) |meta| {
                            allocator.free(meta.encodings);
                            for (meta.path_in_schema) |path| {
                                allocator.free(path);
                            }
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
                allocator.free(metadata.row_groups);

                // Free key-value metadata
                if (metadata.key_value_metadata) |kvs| {
                    for (kvs) |kv| {
                        allocator.free(kv.key);
                        if (kv.value) |v| allocator.free(v);
                    }
                    allocator.free(kvs);
                }

                // Free footer data
                allocator.free(footer_data);
            }

            // Validate schema matches T
            try validateSchema(metadata);

            // Get total rows for this row group
            const row_group_index = options.row_group_index;
            const total_rows: usize = if (row_group_index < metadata.row_groups.len) blk: {
                const num_rows = metadata.row_groups[row_group_index].num_rows;
                if (num_rows < 0) return error.InvalidRowCount;
                break :blk safe.cast(num_rows) catch return error.InvalidRowCount;
            } else 0;

            // Initialize empty column buffers
            var column_data: [column_count]ColumnBuffer = undefined;
            inline for (0..column_count) |i| {
                column_data[i] = .{
                    .values_ptr = undefined,
                    .num_values = 0,
                    .alignment = 1,
                    .element_size = 0,
                    .def_levels = null,
                    .rep_levels = null,
                    .is_byte_array = false,
                    .max_def_level = 0,
                };
            }

            self.allocator = allocator;
            self.metadata = metadata;
            self.footer_data = footer_data;
            self.column_data = column_data;
            self.current_row = 0;
            self.total_rows = total_rows;
            self.row_group_index = row_group_index;

            // Load column data for the row group - for empty files, skip column loading
            if (total_rows > 0) {
                try self.loadRowGroupColumns();
            }

            return self.*;
        }

        /// Clean up resources
        pub fn deinit(self: *Self) void {
            // Free column buffers
            inline for (0..column_count) |i| {
                self.column_data[i].deinit(self.allocator);
            }

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

        /// Validate that the file schema matches the expected struct type
        fn validateSchema(metadata: format.FileMetaData) RowReaderError!void {
            // Get column count from file
            if (metadata.row_groups.len == 0) return; // Empty file is OK

            const file_columns = metadata.row_groups[0].columns.len;
            if (file_columns != column_count) {
                return error.SchemaMismatch;
            }

            // For each expected column, verify the path matches
            inline for (0..column_count) |i| {
                const expected = FlatFields.get(i);
                const file_col = metadata.row_groups[0].columns[i];
                const file_meta = file_col.meta_data orelse return error.SchemaMismatch;

                // Handle list-of-struct fields (have list_context)
                if (expected.list_context) |list_ctx| {
                    // For list-of-struct, we expect the file path to contain:
                    // [list_name, container_name, field_name]
                    // Where container_name can be "list.element" (list) or "key_value" (map)
                    // We only verify the first and last segments match
                    const expected_first = list_ctx.list_path_segments[0];
                    const expected_last = list_ctx.struct_field_names[list_ctx.struct_field_names.len - 1];
                    const file_path = file_meta.path_in_schema;

                    if (file_path.len < 2) {
                        return error.SchemaMismatch;
                    }

                    // Check first segment (list name)
                    if (!std.mem.eql(u8, expected_first, file_path[0])) {
                        return error.SchemaMismatch;
                    }

                    // Check last segment (field name)
                    if (!std.mem.eql(u8, expected_last, file_path[file_path.len - 1])) {
                        return error.SchemaMismatch;
                    }
                    continue;
                }

                // For list fields, the file path includes "list" and "element" suffixes
                const FieldType = expected.field_type;
                const is_list = comptime isSliceType(FieldType);

                if (is_list) {
                    // Check if it's a nested list
                    const ElemType = SliceElementType(unwrapOptionalType(FieldType));
                    const is_nested_list = comptime isSliceType(unwrapOptionalType(ElemType));

                    if (is_nested_list) {
                        // Nested list: expected ["foo"] -> file ["foo", "list", "element", "list", "element"]
                        if (file_meta.path_in_schema.len != expected.path_segments.len + 4) {
                            return error.SchemaMismatch;
                        }

                        // Compare base path
                        for (expected.path_segments, file_meta.path_in_schema[0..expected.path_segments.len]) |exp, actual| {
                            if (!std.mem.eql(u8, exp, actual)) {
                                return error.SchemaMismatch;
                            }
                        }

                        // Verify nested list structure
                        const suffix_start = expected.path_segments.len;
                        if (!std.mem.eql(u8, "list", file_meta.path_in_schema[suffix_start])) return error.SchemaMismatch;
                        if (!std.mem.eql(u8, "element", file_meta.path_in_schema[suffix_start + 1])) return error.SchemaMismatch;
                        if (!std.mem.eql(u8, "list", file_meta.path_in_schema[suffix_start + 2])) return error.SchemaMismatch;
                        if (!std.mem.eql(u8, "element", file_meta.path_in_schema[suffix_start + 3])) return error.SchemaMismatch;
                    } else {
                        // Simple list: expected ["foo"] -> file ["foo", "list", "element"]
                        if (file_meta.path_in_schema.len != expected.path_segments.len + 2) {
                            return error.SchemaMismatch;
                        }

                        // Compare base path
                        for (expected.path_segments, file_meta.path_in_schema[0..expected.path_segments.len]) |exp, actual| {
                            if (!std.mem.eql(u8, exp, actual)) {
                                return error.SchemaMismatch;
                            }
                        }

                        // Verify "list" and "element" suffixes
                        if (!std.mem.eql(u8, "list", file_meta.path_in_schema[expected.path_segments.len])) {
                            return error.SchemaMismatch;
                        }
                        if (!std.mem.eql(u8, "element", file_meta.path_in_schema[expected.path_segments.len + 1])) {
                            return error.SchemaMismatch;
                        }
                    }
                } else {
                    // Non-list columns: direct path comparison
                    if (expected.path_segments.len != file_meta.path_in_schema.len) {
                        return error.SchemaMismatch;
                    }

                    for (expected.path_segments, file_meta.path_in_schema) |exp, actual| {
                        if (!std.mem.eql(u8, exp, actual)) {
                            return error.SchemaMismatch;
                        }
                    }
                }
            }
        }

        /// Load all columns for the current row group
        fn loadRowGroupColumns(self: *Self) RowReaderError!void {
            if (self.row_group_index >= self.metadata.row_groups.len) return;

            const rg = &self.metadata.row_groups[self.row_group_index];

            // Load each column
            inline for (0..column_count) |col_idx| {
                const flat_field = FlatFields.get(col_idx);
                try self.loadColumn(col_idx, &rg.columns[col_idx], flat_field);
            }
        }

        /// Load a single column's data
        fn loadColumn(
            self: *Self,
            comptime col_idx: usize,
            chunk: *const format.ColumnChunk,
            comptime flat_field: FlatField,
        ) RowReaderError!void {
            const meta = chunk.meta_data orelse return error.InvalidArgument;

            // Read the column chunk data using shared utility
            const page_data = parquet_reader.readColumnChunkData(self.allocator, self.getSource(), chunk) catch |e| {
                return switch (e) {
                    error.InvalidArgument => error.InvalidArgument,
                    error.OutOfMemory => error.OutOfMemory,
                    error.InputOutput => error.InputOutput,
                    else => error.InputOutput,
                };
            };
            defer self.allocator.free(page_data);

            var pos: usize = 0;

            // Handle dictionary page if present
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

            // Some writers set dictionary_page_offset=0 to mean "no dictionary"
            // (0 is invalid as it's before the PAR1 magic bytes)
            const has_dict_page = if (meta.dictionary_page_offset) |off| off > 0 else false;
            if (has_dict_page) {
                var dict_thrift = thrift.CompactReader.init(page_data[pos..]);
                const dict_header = format.PageHeader.parse(self.allocator, &dict_thrift) catch
                    return error.InvalidPageData;
                defer self.freePageHeaderContents(&dict_header);

                pos += dict_thrift.pos;

                if (dict_header.dictionary_page_header) |dph| {
                    const dict_compressed_size = try safePageSize(dict_header.compressed_page_size);
                    const dict_uncompressed_size = try safePageSize(dict_header.uncompressed_page_size);
                    const dict_num_values = try safePageSize(dph.num_values);

                    const dict_compressed = try safe.slice(page_data, pos, dict_compressed_size);
                    pos += dict_compressed_size;

                    const dict_data = if (meta.codec != .uncompressed) blk: {
                        break :blk compress.decompress(self.allocator, dict_compressed, meta.codec, dict_uncompressed_size) catch
                            return error.DecompressionError;
                    } else dict_compressed;
                    defer if (meta.codec != .uncompressed) self.allocator.free(dict_data);

                    // Build appropriate dictionary based on element type
                    // Use ColumnStorageType to handle lists (extracts element type)
                    const DictElemType = ColumnStorageType(flat_field.field_type);
                    if (DictElemType == []const u8) {
                        string_dict = dictionary.StringDictionary.fromPlain(self.allocator, dict_data, dict_num_values) catch
                            return error.OutOfMemory;
                    } else if (DictElemType == i32) {
                        // All small integer types are stored as i32 in Parquet
                        int32_dict = dictionary.Int32Dictionary.fromPlain(self.allocator, dict_data, dict_num_values) catch
                            return error.OutOfMemory;
                    } else if (DictElemType == i64) {
                        int64_dict = dictionary.Int64Dictionary.fromPlain(self.allocator, dict_data, dict_num_values) catch
                            return error.OutOfMemory;
                    } else if (DictElemType == f32) {
                        float32_dict = dictionary.Float32Dictionary.fromPlain(self.allocator, dict_data, dict_num_values) catch
                            return error.OutOfMemory;
                    } else if (DictElemType == f64) {
                        float64_dict = dictionary.Float64Dictionary.fromPlain(self.allocator, dict_data, dict_num_values) catch
                            return error.OutOfMemory;
                    }
                }
            }

            // Get column info
            const column_info = format.getColumnInfo(self.metadata.schema, col_idx) orelse
                return error.InvalidArgument;

            const FieldType = flat_field.field_type;
            const StorageType = ColumnStorageType(FieldType);

            // Check if this is an INT96 timestamp type at comptime.
            // For list fields, unwrap the slice to check the element type.
            const UnwrappedFieldType = unwrapOptionalType(FieldType);
            const is_int96_comptime = comptime blk: {
                if (isSliceType(UnwrappedFieldType)) {
                    const elem = unwrapOptionalType(SliceElementType(UnwrappedFieldType));
                    break :blk isInt96TimestampType(elem);
                }
                break :blk isInt96TimestampType(UnwrappedFieldType);
            };

            // Accumulators for multi-page support
            var all_values: std.ArrayListUnmanaged(Optional(StorageType)) = .empty;
            errdefer {
                if (StorageType == []const u8) {
                    for (all_values.items) |v| {
                        if (v == .value) self.allocator.free(v.value);
                    }
                }
                all_values.deinit(self.allocator);
            }
            var all_def_levels: ?std.ArrayListUnmanaged(u32) = if (column_info.max_def_level > 0) .empty else null;
            errdefer if (all_def_levels) |*dl| dl.deinit(self.allocator);
            var all_rep_levels: ?std.ArrayListUnmanaged(u32) = if (column_info.max_rep_level > 0) .empty else null;
            errdefer if (all_rep_levels) |*rl| rl.deinit(self.allocator);

            // Loop through all data pages
            while (pos < page_data.len) {
                // Parse data page header
                var thrift_reader = thrift.CompactReader.init(page_data[pos..]);
                const page_header = format.PageHeader.parse(self.allocator, &thrift_reader) catch
                    return error.InvalidPageData;
                defer self.freePageHeaderContents(&page_header);

                pos += thrift_reader.pos;

                // Skip non-data pages
                if (page_header.data_page_header == null) {
                    const skip_size = try safePageSize(page_header.compressed_page_size);
                    pos += skip_size;
                    continue;
                }

                const compressed_size = try safePageSize(page_header.compressed_page_size);
                if (pos + compressed_size > page_data.len) return error.EndOfData;
                const compressed_body = try safe.slice(page_data, pos, compressed_size);
                pos += compressed_size;

                // Decompress if needed
                const value_data = if (meta.codec != .uncompressed) blk: {
                    const uncompressed_size = try safePageSize(page_header.uncompressed_page_size);
                    break :blk compress.decompress(self.allocator, compressed_body, meta.codec, uncompressed_size) catch
                        return error.DecompressionError;
                } else compressed_body;
                defer if (meta.codec != .uncompressed) self.allocator.free(value_data);

                // Get number of values
                const num_values: usize = if (page_header.data_page_header) |dph| blk: {
                    break :blk try safePageSize(dph.num_values);
                } else 0;

                if (num_values == 0) continue;

                // INT96 columns need special handling - decode as dynamic values
                if (is_int96_comptime) {
                    // Use the dynamic decoder which handles INT96 -> i64 conversion
                    const dph = page_header.data_page_header.?;
                    const dynamic_result = column_decoder.decodeDynamicInt96(
                        self.allocator,
                        column_info.element,
                        value_data,
                        num_values,
                        column_info.max_def_level,
                        column_info.max_rep_level,
                        dph.definition_level_encoding,
                        dph.repetition_level_encoding,
                    ) catch return error.UnsupportedEncoding;
                    defer self.allocator.free(dynamic_result.values);
                    defer if (dynamic_result.def_levels) |dl| self.allocator.free(dl);
                    defer if (dynamic_result.rep_levels) |rl| self.allocator.free(rl);

                    // Convert Value to Optional(i64) - StorageType is i64 for INT96
                    const page_values = try self.allocator.alloc(Optional(StorageType), num_values);
                    defer self.allocator.free(page_values);
                    for (dynamic_result.values, 0..) |v, i| {
                        page_values[i] = if (v == .null_val) .null_value else .{ .value = v.int64_val };
                    }

                    all_values.appendSlice(self.allocator, page_values) catch return error.OutOfMemory;
                    if (all_def_levels) |*dl| {
                        if (dynamic_result.def_levels) |page_dl| {
                            dl.appendSlice(self.allocator, page_dl) catch return error.OutOfMemory;
                        }
                    }
                    if (all_rep_levels) |*rl| {
                        if (dynamic_result.rep_levels) |page_rl| {
                            rl.appendSlice(self.allocator, page_rl) catch return error.OutOfMemory;
                        }
                    }
                    continue;
                }

                // Decode the column values for this page (non-INT96 path)
                const dph = page_header.data_page_header.?;
                const ctx = column_decoder.DecodeContext{
                    .allocator = self.allocator,
                    .schema_elem = column_info.element,
                    .num_values = num_values,
                    .uses_dict = string_dict != null or int32_dict != null or int64_dict != null or float32_dict != null or float64_dict != null,
                    .string_dict = if (string_dict) |*d| d else null,
                    .int32_dict = if (int32_dict) |*d| d else null,
                    .int64_dict = if (int64_dict) |*d| d else null,
                    .float32_dict = if (float32_dict) |*d| d else null,
                    .float64_dict = if (float64_dict) |*d| d else null,
                    .max_definition_level = column_info.max_def_level,
                    .max_repetition_level = column_info.max_rep_level,
                    .value_encoding = dph.encoding,
                    .def_level_encoding = dph.definition_level_encoding,
                    .rep_level_encoding = dph.repetition_level_encoding,
                };

                const decode_result = column_decoder.decodeColumnWithLevels(StorageType, ctx, value_data) catch
                    return error.UnsupportedEncoding;
                defer self.allocator.free(decode_result.values);
                defer if (decode_result.def_levels) |dl| self.allocator.free(dl);
                defer if (decode_result.rep_levels) |rl| self.allocator.free(rl);

                // Append to accumulators
                all_values.appendSlice(self.allocator, decode_result.values) catch return error.OutOfMemory;
                if (all_def_levels) |*dl| {
                    if (decode_result.def_levels) |page_dl| {
                        dl.appendSlice(self.allocator, page_dl) catch return error.OutOfMemory;
                    }
                }
                if (all_rep_levels) |*rl| {
                    if (decode_result.rep_levels) |page_rl| {
                        rl.appendSlice(self.allocator, page_rl) catch return error.OutOfMemory;
                    }
                }
            }

            // Convert to owned slices and store in column buffer
            const final_values = all_values.toOwnedSlice(self.allocator) catch return error.OutOfMemory;
            const final_def_levels: ?[]u32 = if (all_def_levels) |*dl| dl.toOwnedSlice(self.allocator) catch return error.OutOfMemory else null;
            const final_rep_levels: ?[]u32 = if (all_rep_levels) |*rl| rl.toOwnedSlice(self.allocator) catch return error.OutOfMemory else null;

            // Store in column buffer with alignment info and levels
            self.column_data[col_idx] = .{
                .values_ptr = @ptrCast(final_values.ptr),
                .num_values = final_values.len,
                .alignment = @alignOf(Optional(StorageType)),
                .element_size = @sizeOf(Optional(StorageType)),
                .def_levels = final_def_levels,
                .rep_levels = final_rep_levels,
                .is_byte_array = (StorageType == []const u8),
                .max_def_level = column_info.max_def_level,
            };
        }

        /// Free the allocated contents within a PageHeader (statistics min/max values).
        /// Does NOT free the PageHeader struct itself.
        fn freePageHeaderContents(self: *Self, header: *const format.PageHeader) void {
            if (header.data_page_header) |dph| {
                if (dph.statistics) |stats| {
                    if (stats.max) |m| self.allocator.free(m);
                    if (stats.min) |m| self.allocator.free(m);
                    if (stats.max_value) |m| self.allocator.free(m);
                    if (stats.min_value) |m| self.allocator.free(m);
                }
            }
        }

        /// Get the storage type for a column
        fn ColumnStorageType(comptime FieldType: type) type {
            const Unwrapped = unwrapOptionalType(FieldType);

            // Handle slice/list types - extract element type
            if (comptime isSliceType(Unwrapped)) {
                const ElemType = SliceElementType(Unwrapped);
                return ColumnStorageType(ElemType);
            }

            // Handle wrapper types
            // INT96 timestamps are converted to i64 nanoseconds by the column decoder
            if (isInt96TimestampType(Unwrapped)) return i64;
            if (isTimestampType(Unwrapped)) return i64;
            if (isTimeType(Unwrapped)) return i64;
            if (Unwrapped == types.Date) return i32;
            if (Unwrapped == types.Uuid) return []const u8; // FIXED_LEN_BYTE_ARRAY returns []const u8
            if (Unwrapped == types.Interval) return []const u8; // FIXED_LEN_BYTE_ARRAY returns []const u8
            if (Unwrapped == types.DecimalValue) return []const u8; // Raw bytes, converted later
            if (comptime isDecimalType(Unwrapped)) return []const u8; // Raw bytes, converted later

            // Handle small integers
            return switch (Unwrapped) {
                i8, i16, u8, u16, u32 => i32,
                u64 => i64,
                else => Unwrapped,
            };
        }

        /// Read the next row, returning null when exhausted
        pub fn next(self: *Self) RowReaderError!?T {
            if (self.current_row >= self.total_rows) {
                return null;
            }

            const row = try self.readRowAt(self.current_row);
            self.current_row += 1;
            return row;
        }

        /// Check if there are more rows
        pub fn hasNext(self: *const Self) bool {
            return self.current_row < self.total_rows;
        }

        /// Get total number of rows
        pub fn rowCount(self: *const Self) usize {
            return self.total_rows;
        }

        /// Reset to beginning
        pub fn reset(self: *Self) void {
            self.current_row = 0;
        }

        /// Read a specific row
        fn readRowAt(self: *Self, row_idx: usize) RowReaderError!T {
            var result: T = undefined;

            inline for (0..column_count) |col_idx| {
                const flat_field = FlatFields.get(col_idx);

                // Handle list-of-struct fields
                if (flat_field.list_context) |list_ctx| {
                    // Check if this is the first field of the list-of-struct
                    // (all indices in struct_field_path are 0)
                    const is_first_field = comptime blk: {
                        for (list_ctx.struct_field_path) |idx| {
                            if (idx != 0) break :blk false;
                        }
                        break :blk true;
                    };

                    if (is_first_field) {
                        // This is the primary field - reconstruct the entire list-of-struct
                        const list_value = try self.readListOfStructField(col_idx, row_idx, list_ctx);
                        self.setFieldValue(&result, list_ctx.list_path, list_value);
                    }
                    // Skip non-primary fields (already handled by primary)
                    continue;
                }

                const FieldType = flat_field.field_type;

                // Check if this is a list field
                if (comptime isSliceType(FieldType)) {
                    const ElemType = SliceElementType(FieldType);
                    // Check if it's a nested list (element is also a slice)
                    if (comptime isSliceType(unwrapOptionalType(ElemType))) {
                        // Nested list - use specialized handler
                        const list_value = try self.readNestedListField(col_idx, row_idx, flat_field);
                        self.setFieldValue(&result, flat_field.path, list_value);
                    } else {
                        const list_value = try self.readListField(col_idx, row_idx, flat_field);
                        self.setFieldValue(&result, flat_field.path, list_value);
                    }
                } else {
                    const value = try self.readFieldValue(col_idx, row_idx, flat_field);
                    self.setFieldValue(&result, flat_field.path, value);
                }
            }

            return result;
        }

        /// Read a nested list field (list<list<T>>) for a specific row
        fn readNestedListField(
            self: *Self,
            comptime col_idx: usize,
            row_idx: usize,
            comptime flat_field: FlatField,
        ) RowReaderError!flat_field.field_type {
            const FieldType = flat_field.field_type;
            const OuterElemType = SliceElementType(FieldType);
            const InnerElemType = SliceElementType(unwrapOptionalType(OuterElemType));
            const StorageType = ColumnStorageType(InnerElemType);
            const col_buf = &self.column_data[col_idx];

            // Get max_def_level from the schema
            const column_info = format.getColumnInfo(self.metadata.schema, col_idx) orelse
                return error.InvalidArgument;
            const max_def: u32 = column_info.max_def_level;

            const rep_levels = col_buf.rep_levels orelse {
                if (comptime isOptionalType(FieldType)) {
                    return null;
                }
                return &[_]OuterElemType{};
            };

            const def_levels = col_buf.def_levels;

            // Find the value range for this row (rep_level == 0 means new row)
            var value_start: usize = 0;
            var current_row: usize = 0;

            for (rep_levels, 0..) |rep, i| {
                if (rep == 0) {
                    if (current_row == row_idx) {
                        value_start = i;
                        break;
                    }
                    current_row += 1;
                }
            }

            if (current_row != row_idx) {
                if (comptime isOptionalType(FieldType)) {
                    return null;
                }
                return &[_]OuterElemType{};
            }

            // Find the end of this row
            var value_end = value_start + 1;
            while (value_end < rep_levels.len and rep_levels[value_end] != 0) {
                value_end += 1;
            }

            // Check if the outer list is null (def_level == 0)
            if (def_levels) |dl| {
                if (dl[value_start] == 0) {
                    if (comptime isOptionalType(FieldType)) {
                        return null;
                    }
                    return &[_]OuterElemType{};
                }
            }

            // For nested lists (5-level schema), the def level at which an inner list
            // "exists" (even if empty) depends on how many optional/repeated levels
            // precede the inner REPEATED group. Formula: max_def - D - E, where
            // D=1 (inner REPEATED) and E=1 if leaf is OPTIONAL, 0 if REQUIRED.
            const leaf_opt: u32 = if (column_info.element.repetition_type == .optional) 1 else 0;
            const inner_exists_threshold: u32 = if (max_def >= 1 + leaf_opt) max_def - 1 - leaf_opt else 0;

            // Count outer list elements (rep <= 1 means new inner list)
            var outer_count: usize = 0;
            var i: usize = value_start;
            while (i < value_end) : (i += 1) {
                const is_new_inner = (i == value_start) or (rep_levels[i] <= 1);
                if (is_new_inner) {
                    if (def_levels) |dl| {
                        if (dl[i] >= inner_exists_threshold) {
                            outer_count += 1;
                        }
                    } else {
                        outer_count += 1;
                    }
                }
            }

            // Handle empty outer list
            if (outer_count == 0) {
                return &[_]OuterElemType{};
            }

            // Allocate outer list
            const outer_result = self.allocator.alloc(OuterElemType, outer_count) catch return error.OutOfMemory;
            errdefer {
                for (outer_result) |inner| {
                    self.allocator.free(inner);
                }
                self.allocator.free(outer_result);
            }

            // Get values
            const values = @as([*]Optional(StorageType), @ptrCast(@alignCast(col_buf.values_ptr)))[0..col_buf.num_values];

            // Fill in the nested structure
            var outer_idx: usize = 0;
            var inner_start: usize = value_start;

            var j: usize = value_start;
            while (j < value_end) : (j += 1) {
                const is_new_inner = (j == value_start) or (rep_levels[j] <= 1);

                if (is_new_inner and j > inner_start) {
                    // Finish previous inner list
                    outer_result[outer_idx] = try self.buildInnerList(
                        InnerElemType,
                        StorageType,
                        values,
                        def_levels,
                        inner_start,
                        j,
                        max_def,
                    );
                    outer_idx += 1;
                    inner_start = j;
                }
            }

            // Finish last inner list
            if (outer_idx < outer_count) {
                outer_result[outer_idx] = try self.buildInnerList(
                    InnerElemType,
                    StorageType,
                    values,
                    def_levels,
                    inner_start,
                    value_end,
                    max_def,
                );
            }

            return outer_result;
        }

        /// Build an inner list for nested list reconstruction
        fn buildInnerList(
            self: *Self,
            comptime InnerElemType: type,
            comptime StorageType: type,
            values: []Optional(StorageType),
            def_levels: ?[]u32,
            start: usize,
            end: usize,
            max_def: u32,
        ) RowReaderError![]const InnerElemType {
            // For optional elements, def == max_def means non-null,
            // def == max_def - 1 means null element (still an element in the list).
            const elem_threshold: u32 = if (comptime isOptionalType(InnerElemType))
                max_def - 1
            else
                max_def;

            var count: usize = 0;
            for (start..end) |i| {
                if (def_levels) |dl| {
                    if (dl[i] >= elem_threshold) {
                        count += 1;
                    }
                } else {
                    count += 1;
                }
            }

            if (count == 0) {
                return &[_]InnerElemType{};
            }

            const inner_result = self.allocator.alloc(InnerElemType, count) catch return error.OutOfMemory;
            errdefer self.allocator.free(inner_result);

            var result_idx: usize = 0;
            for (start..end) |i| {
                const is_element = if (def_levels) |dl| dl[i] >= elem_threshold else true;
                if (!is_element) continue;

                if (result_idx >= inner_result.len) break;

                const opt_val = values[i];
                if (opt_val == .null_value) {
                    if (comptime isOptionalType(InnerElemType)) {
                        inner_result[result_idx] = null;
                    } else {
                        inner_result[result_idx] = std.mem.zeroes(InnerElemType);
                    }
                } else {
                    const converted = convertValue(InnerElemType, StorageType, opt_val);
                    if (comptime StorageType == []const u8) {
                        const BaseElem = unwrapOptionalType(InnerElemType);
                        if (comptime BaseElem == []const u8) {
                            inner_result[result_idx] = self.allocator.dupe(u8, converted) catch return error.OutOfMemory;
                        } else {
                            inner_result[result_idx] = converted;
                        }
                    } else {
                        inner_result[result_idx] = converted;
                    }
                }
                result_idx += 1;
            }

            return inner_result[0..result_idx];
        }

        /// Read a list-of-struct field for a specific row
        fn readListOfStructField(
            self: *Self,
            comptime first_col_idx: usize,
            row_idx: usize,
            comptime list_ctx: ListFieldContext,
        ) RowReaderError!list_ctx.slice_type {
            const SliceType = list_ctx.slice_type;
            const ElemType = SliceElementType(SliceType);
            const StructType = unwrapOptionalType(ElemType);

            // Get the first column's data to determine list boundaries
            const first_col = &self.column_data[first_col_idx];
            const rep_levels = first_col.rep_levels orelse {
                // No repetition levels - return empty or null
                if (comptime isOptionalType(SliceType)) {
                    return null;
                }
                return &[_]ElemType{};
            };

            const def_levels = first_col.def_levels;

            // Find the value range for this row using repetition levels
            var value_start: usize = 0;
            var current_row: usize = 0;

            for (rep_levels, 0..) |rep, i| {
                if (rep == 0) {
                    if (current_row == row_idx) {
                        value_start = i;
                        break;
                    }
                    current_row += 1;
                }
            }

            if (current_row != row_idx) {
                if (comptime isOptionalType(SliceType)) {
                    return null;
                }
                return &[_]ElemType{};
            }

            // Find the end of this row
            var value_end = value_start + 1;
            while (value_end < rep_levels.len and rep_levels[value_end] != 0) {
                value_end += 1;
            }

            // Check if the list itself is null
            if (def_levels) |dl| {
                if (dl[value_start] == 0) {
                    if (comptime isOptionalType(SliceType)) {
                        return null;
                    }
                    return &[_]ElemType{};
                }
            }

            // Count elements (each entry at rep_level >= 1 or the first entry)
            // For list-of-struct, we count by looking at def_levels
            const max_def: u32 = first_col.max_def_level;
            // For list/map element counting:
            // - Standard 3-level lists: def=0 is null, def=1 is empty, def>=2 is element
            // - For files with fewer levels (e.g., our writer's required lists): use max_def
            // The element threshold is min(2, max_def) for counting purposes
            const threshold: u32 = if (max_def >= 2) 2 else max_def;
            var count: usize = 0;
            for (value_start..value_end) |i| {
                if (def_levels) |dl| {
                    // Count elements at or above threshold
                    if (dl[i] >= threshold) {
                        count += 1;
                    }
                } else {
                    count += 1;
                }
            }

            // Handle empty list
            if (count == 0) {
                // Check if it's an empty list vs null list
                if (def_levels) |dl| {
                    if (dl[value_start] == 1) {
                        // Empty list
                        return &[_]ElemType{};
                    }
                }
                if (comptime isOptionalType(SliceType)) {
                    return null;
                }
                return &[_]ElemType{};
            }

            // Allocate result
            const result = self.allocator.alloc(ElemType, count) catch return error.OutOfMemory;
            errdefer self.allocator.free(result);

            // Initialize all structs to zero
            for (result) |*elem| {
                elem.* = std.mem.zeroes(ElemType);
            }

            // Fill in values from each column that belongs to this list-of-struct
            inline for (0..column_count) |col_idx| {
                const field = FlatFields.get(col_idx);
                if (field.list_context) |ctx| {
                    // Check if this column belongs to the same list
                    if (comptime std.mem.eql(usize, ctx.list_path, list_ctx.list_path)) {
                        try self.fillStructFieldFromColumn(
                            StructType,
                            col_idx,
                            ctx,
                            result,
                            value_start,
                            value_end,
                            def_levels,
                        );
                    }
                }
            }

            return result;
        }

        /// Fill a single field in all struct instances from a column
        fn fillStructFieldFromColumn(
            self: *Self,
            comptime StructType: type,
            comptime col_idx: usize,
            comptime list_ctx: ListFieldContext,
            result: anytype,
            value_start: usize,
            value_end: usize,
            def_levels: ?[]u32,
        ) RowReaderError!void {
            const flat_field = FlatFields.get(col_idx);
            const FieldType = flat_field.field_type;
            const StorageType = ColumnStorageType(FieldType);
            const col_buf = &self.column_data[col_idx];

            const values = @as([*]Optional(StorageType), @ptrCast(@alignCast(col_buf.values_ptr)))[0..col_buf.num_values];

            // Use max_def_level from the column to determine element threshold
            const max_def: u32 = col_buf.max_def_level;
            // For optional fields, the element threshold is max_def - 1
            const field_optional = comptime isOptionalType(FieldType);
            const threshold: u32 = if (field_optional and max_def > 0) max_def - 1 else max_def;

            var result_idx: usize = 0;
            for (value_start..value_end) |i| {
                // Check if this slot represents a struct element (at or above element threshold)
                const is_element = if (def_levels) |dl| dl[i] >= threshold else true;
                if (!is_element) continue;

                if (result_idx >= result.len) break;

                // Get the value at this position
                const opt_val = values[i];
                const field_value = blk: {
                    if (opt_val == .null_value) {
                        if (comptime isOptionalType(FieldType)) {
                            break :blk @as(FieldType, null);
                        } else {
                            break :blk std.mem.zeroes(FieldType);
                        }
                    }

                    // Handle byte arrays — wrapper types (UUID, Interval) need
                    // conversion rather than raw dupe
                    if (StorageType == []const u8) {
                        const BaseFieldType = comptime unwrapOptionalType(FieldType);
                        if (BaseFieldType == types.Uuid or BaseFieldType == types.Interval or comptime isDecimalType(BaseFieldType)) {
                            break :blk convertValue(FieldType, StorageType, opt_val);
                        }
                        const duped = self.allocator.dupe(u8, opt_val.value) catch return error.OutOfMemory;
                        if (comptime isOptionalType(FieldType)) {
                            break :blk @as(FieldType, duped);
                        } else {
                            break :blk duped;
                        }
                    }

                    break :blk convertValue(FieldType, StorageType, opt_val);
                };

                // Set the field in the struct using struct_field_path
                setStructFieldByPath(StructType, &result[result_idx], list_ctx.struct_field_path, field_value);
                result_idx += 1;
            }
        }

        /// Set a field in a struct by path (for list-of-struct reconstruction)
        fn setStructFieldByPath(
            comptime StructType: type,
            ptr: *StructType,
            comptime path: []const usize,
            value: anytype,
        ) void {
            if (path.len == 0) return;

            const fields = std.meta.fields(StructType);
            const field = fields[path[0]];

            if (path.len == 1) {
                @field(ptr, field.name) = value;
            } else {
                // Navigate into nested struct
                const FieldType = unwrapOptionalType(field.type);
                if (comptime isOptionalType(field.type)) {
                    if (@field(ptr, field.name) == null) {
                        @field(ptr, field.name) = std.mem.zeroes(FieldType);
                    }
                    setStructFieldByPath(FieldType, &(@field(ptr, field.name).?), path[1..], value);
                } else {
                    setStructFieldByPath(FieldType, &@field(ptr, field.name), path[1..], value);
                }
            }
        }

        /// Read a list field value for a specific row
        fn readListField(
            self: *Self,
            comptime col_idx: usize,
            row_idx: usize,
            comptime flat_field: FlatField,
        ) RowReaderError!flat_field.field_type {
            const FieldType = flat_field.field_type;
            const ElemType = SliceElementType(FieldType);
            const StorageType = ColumnStorageType(ElemType);
            const col_buf = &self.column_data[col_idx];

            // For lists, we need to use repetition levels to find row boundaries
            const rep_levels = col_buf.rep_levels orelse {
                // No repetition levels means this might be a single-value or empty list
                // Fall back to treating as empty list for optional, or error
                if (comptime isOptionalType(FieldType)) {
                    return null;
                }
                return &[_]ElemType{};
            };

            const def_levels = col_buf.def_levels;

            // Find the value range for this row using repetition levels
            // rep_level == 0 means start of a new row
            var value_start: usize = 0;
            var current_row: usize = 0;

            // Find the start of this row
            for (rep_levels, 0..) |rep, i| {
                if (rep == 0) {
                    if (current_row == row_idx) {
                        value_start = i;
                        break;
                    }
                    current_row += 1;
                }
            }

            if (current_row != row_idx) {
                // Row not found - return empty or null
                if (comptime isOptionalType(FieldType)) {
                    return null;
                }
                return &[_]ElemType{};
            }

            // Find the end of this row (next rep_level == 0 or end of data)
            var value_end = value_start + 1;
            while (value_end < rep_levels.len and rep_levels[value_end] != 0) {
                value_end += 1;
            }

            // Check if the list itself is null (def_level indicates parent null)
            // For lists, def_level 0 typically means the list is null
            if (def_levels) |dl| {
                if (dl[value_start] == 0) {
                    if (comptime isOptionalType(FieldType)) {
                        return null;
                    }
                    return &[_]ElemType{};
                }
            }

            // Get all values for this row
            const values = @as([*]Optional(StorageType), @ptrCast(@alignCast(col_buf.values_ptr)))[0..col_buf.num_values];

            // Count elements to allocate result
            // For optional element types, we include nulls in the count
            // Use max_def_level to determine element threshold:
            // - def == max_def_level: element with value
            // - def == max_def_level - 1: null element (for optional elements)
            // - def < threshold: empty list or null list (don't count)
            const max_def: u32 = col_buf.max_def_level;
            // For optional elements, also count null elements (def = max_def - 1)
            const elem_is_optional = comptime isOptionalType(ElemType);
            const def_threshold: u32 = if (elem_is_optional and max_def > 0) max_def - 1 else max_def;
            var count: usize = 0;
            for (value_start..value_end) |i| {
                if (def_levels) |dl| {
                    if (dl[i] >= def_threshold) {
                        count += 1;
                    }
                } else {
                    count += 1;
                }
            }

            // Handle empty list (def_level == 1)
            if (count == 0) {
                return &[_]ElemType{};
            }

            // Allocate result slice
            const result = self.allocator.alloc(ElemType, count) catch return error.OutOfMemory;
            errdefer self.allocator.free(result);

            // Copy values, using def_levels to determine which slots are elements
            var result_idx: usize = 0;
            for (value_start..value_end) |i| {
                // Check if this is an element (def >= def_threshold)
                const is_element = if (def_levels) |dl| dl[i] >= def_threshold else true;
                if (!is_element) continue;

                if (result_idx >= result.len) break;

                const opt_val = values[i];
                if (opt_val == .null_value) {
                    // Handle null elements in list
                    if (comptime isOptionalType(ElemType)) {
                        result[result_idx] = null;
                    } else {
                        result[result_idx] = std.mem.zeroes(ElemType);
                    }
                } else {
                    if (StorageType == []const u8) {
                        const UnwrappedElem = comptime if (@typeInfo(ElemType) == .optional)
                            @typeInfo(ElemType).optional.child
                        else
                            ElemType;

                        if (UnwrappedElem == types.Uuid) {
                            if (opt_val.value.len >= 16) {
                                result[result_idx] = types.Uuid{ .bytes = opt_val.value[0..16].* };
                            } else {
                                result[result_idx] = std.mem.zeroes(ElemType);
                            }
                        } else if (UnwrappedElem == types.Interval) {
                            if (opt_val.value.len >= 12) {
                                result[result_idx] = types.Interval.fromBytes(opt_val.value[0..12].*);
                            } else {
                                result[result_idx] = std.mem.zeroes(ElemType);
                            }
                        } else if (comptime isDecimalType(UnwrappedElem)) {
                            result[result_idx] = UnwrappedElem.fromBytes(opt_val.value) catch
                                return error.InvalidArgument;
                        } else {
                            result[result_idx] = self.allocator.dupe(u8, opt_val.value) catch return error.OutOfMemory;
                        }
                    } else {
                        result[result_idx] = convertValue(ElemType, StorageType, opt_val);
                    }
                }
                result_idx += 1;
            }

            return result[0..result_idx];
        }

        /// Read a single field value from column data
        fn readFieldValue(
            self: *Self,
            comptime col_idx: usize,
            row_idx: usize,
            comptime flat_field: FlatField,
        ) RowReaderError!flat_field.field_type {
            const FieldType = flat_field.field_type;
            const StorageType = ColumnStorageType(FieldType);
            const col_buf = &self.column_data[col_idx];

            if (row_idx >= col_buf.num_values) {
                return error.InvalidArgument;
            }

            // Get the Optional(StorageType) slice
            const values = @as([*]Optional(StorageType), @ptrCast(@alignCast(col_buf.values_ptr)))[0..col_buf.num_values];
            const opt_val = values[row_idx];

            // Handle Decimal type specially (needs schema info for precision/scale)
            const BaseFieldType = unwrapOptionalType(FieldType);
            if (BaseFieldType == types.DecimalValue) {
                if (opt_val == .null_value) {
                    if (comptime isOptionalType(FieldType)) {
                        return null;
                    } else {
                        return std.mem.zeroes(FieldType);
                    }
                }

                // Get precision/scale from schema
                const column_info = format.getColumnInfo(self.metadata.schema, col_idx) orelse
                    return error.InvalidArgument;

                var precision: u8 = 0;
                var scale: u8 = 0;

                if (column_info.element.logical_type) |lt| {
                    if (lt == .decimal) {
                        // Decimal precision is typically 1-38, scale 0-38
                        precision = if (lt.decimal.precision >= 0 and lt.decimal.precision <= 255)
                            @truncate(@as(u32, @bitCast(lt.decimal.precision))) // Safe: validated in condition
                        else
                            0;
                        scale = if (lt.decimal.scale >= 0 and lt.decimal.scale <= 255)
                            @truncate(@as(u32, @bitCast(lt.decimal.scale))) // Safe: validated in condition
                        else
                            0;
                    }
                }

                const decimal = try types.DecimalValue.fromBytes(opt_val.value, precision, scale);
                // Note: Don't free opt_val.value here - the column buffer owns it
                // and will free it in deinit(). Decimal.fromBytes already copies the bytes.

                if (comptime isOptionalType(FieldType)) {
                    return decimal;
                } else {
                    return decimal;
                }
            }

            // Handle parameterized Decimal(p,s) type
            if (comptime isDecimalType(BaseFieldType)) {
                if (opt_val == .null_value) {
                    if (comptime isOptionalType(FieldType)) {
                        return null;
                    } else {
                        return std.mem.zeroes(FieldType);
                    }
                }

                const typed_decimal = BaseFieldType.fromBytes(opt_val.value) catch
                    return error.InvalidArgument;

                if (comptime isOptionalType(FieldType)) {
                    return typed_decimal;
                } else {
                    return typed_decimal;
                }
            }

            // For byte arrays, duplicate so caller owns the memory
            // Skip this for UUID/Interval which need conversion to their wrapper types
            const needs_wrapper_conversion = (BaseFieldType == types.Uuid or BaseFieldType == types.Interval);

            if (StorageType == []const u8 and !needs_wrapper_conversion) {
                if (opt_val == .null_value) {
                    if (comptime isOptionalType(FieldType)) {
                        return null;
                    } else {
                        return &[_]u8{};
                    }
                }
                const duped = self.allocator.dupe(u8, opt_val.value) catch return error.OutOfMemory;
                if (comptime isOptionalType(FieldType)) {
                    return duped;
                } else {
                    return duped;
                }
            }

            // Convert from Optional(StorageType) to FieldType
            return convertValue(FieldType, StorageType, opt_val);
        }

        /// Convert from storage type to field type
        fn convertValue(
            comptime FieldType: type,
            comptime StorageType: type,
            opt_val: Optional(StorageType),
        ) FieldType {
            const BaseType = unwrapOptionalType(FieldType);

            if (opt_val == .null_value) {
                if (comptime isOptionalType(FieldType)) {
                    return null;
                } else {
                    // Non-optional field got null - return default
                    return std.mem.zeroes(FieldType);
                }
            }

            const storage_val = opt_val.value;

            // Convert storage type to base type
            const base_val: BaseType = blk: {
                // Handle parameterized Timestamp/Time types (use .value field)
                // INT96 timestamps are already converted to i64 nanoseconds by the decoder
                if (comptime isInt96TimestampType(BaseType)) {
                    break :blk types.TimestampInt96{ .value = storage_val };
                } else if (comptime isTimestampType(BaseType)) {
                    break :blk BaseType{ .value = storage_val };
                } else if (comptime isTimeType(BaseType)) {
                    break :blk BaseType{ .value = storage_val };
                } else if (BaseType == types.Date) {
                    // Handle other wrapper types
                    break :blk types.Date{ .days = storage_val };
                } else if (comptime isDecimalType(BaseType)) {
                    break :blk BaseType.fromBytes(storage_val) catch return std.mem.zeroes(FieldType);
                } else if (BaseType == types.Uuid) {
                    if (storage_val.len < 16) return std.mem.zeroes(FieldType);
                    break :blk types.Uuid{ .bytes = storage_val[0..16].* };
                } else if (BaseType == types.Interval) {
                    if (storage_val.len < 12) return std.mem.zeroes(FieldType);
                    break :blk types.Interval.fromBytes(storage_val[0..12].*);
                } else if (BaseType == i8) {
                    // Handle integer conversions with bounds checking
                    break :blk std.math.cast(i8, storage_val) orelse 0;
                } else if (BaseType == i16) {
                    break :blk std.math.cast(i16, storage_val) orelse 0;
                } else if (BaseType == u8) {
                    break :blk std.math.cast(u8, storage_val) orelse 0;
                } else if (BaseType == u16) {
                    break :blk std.math.cast(u16, storage_val) orelse 0;
                } else if (BaseType == u32) {
                    break :blk @bitCast(storage_val);
                } else if (BaseType == u64) {
                    break :blk @bitCast(storage_val);
                } else {
                    // Direct assignment for matching types
                    break :blk storage_val;
                }
            };

            if (comptime isOptionalType(FieldType)) {
                return base_val;
            } else {
                return base_val;
            }
        }

        /// Set a field value in the result struct using path
        fn setFieldValue(
            self: *Self,
            result: *T,
            comptime path: []const usize,
            value: anytype,
        ) void {
            _ = self;
            if (path.len == 0) return;

            // Navigate to the target field
            setFieldValueRecursive(T, result, path, value);
        }

        fn setFieldValueRecursive(
            comptime StructType: type,
            ptr: *StructType,
            comptime path: []const usize,
            value: anytype,
        ) void {
            const fields = std.meta.fields(StructType);
            const field = fields[path[0]];

            if (path.len == 1) {
                // Final field - set the value
                @field(ptr, field.name) = value;
            } else {
                // Navigate into nested struct
                const FieldType = unwrapOptionalType(field.type);
                if (comptime isOptionalType(field.type)) {
                    // Initialize optional nested struct if null
                    if (@field(ptr, field.name) == null) {
                        @field(ptr, field.name) = std.mem.zeroes(FieldType);
                    }
                    setFieldValueRecursive(FieldType, &(@field(ptr, field.name).?), path[1..], value);
                } else {
                    setFieldValueRecursive(FieldType, &@field(ptr, field.name), path[1..], value);
                }
            }
        }

        /// Free memory allocated for a row (strings, slices)
        pub fn freeRow(self: *Self, row: *const T) void {
            // Free any allocated strings/slices in the row
            freeRowFields(T, self.allocator, row);
        }

        fn freeRowFields(comptime StructType: type, allocator: std.mem.Allocator, ptr: *const StructType) void {
            const fields = std.meta.fields(StructType);
            inline for (fields) |field| {
                const FieldType = unwrapOptionalType(field.type);
                const field_ptr = &@field(ptr, field.name);

                if (comptime isOptionalType(field.type)) {
                    if (field_ptr.*) |val| {
                        freeFieldValue(FieldType, allocator, &val);
                    }
                } else {
                    freeFieldValue(FieldType, allocator, field_ptr);
                }
            }
        }

        fn freeFieldValue(comptime FieldType: type, allocator: std.mem.Allocator, ptr: *const FieldType) void {
            if (FieldType == []const u8) {
                if (ptr.len > 0) {
                    allocator.free(ptr.*);
                }
            } else if (comptime isSliceType(FieldType)) {
                const ElemType = SliceElementType(FieldType);
                const UnwrappedElem = unwrapOptionalType(ElemType);
                for (ptr.*) |*elem| {
                    if (comptime isOptionalType(ElemType)) {
                        if (elem.*) |val| {
                            freeFieldValue(UnwrappedElem, allocator, &val);
                        }
                    } else if (comptime isSliceType(UnwrappedElem)) {
                        freeFieldValue(UnwrappedElem, allocator, elem);
                    } else if (comptime isNestedStruct(UnwrappedElem)) {
                        freeRowFields(UnwrappedElem, allocator, elem);
                    } else if (UnwrappedElem == []const u8) {
                        if (elem.len > 0) {
                            allocator.free(elem.*);
                        }
                    }
                }
                if (ptr.len > 0) {
                    allocator.free(ptr.*);
                }
            } else if (comptime isNestedStruct(FieldType)) {
                freeRowFields(FieldType, allocator, ptr);
            }
        }

        /// Read all remaining rows into a slice
        pub fn readAll(self: *Self) RowReaderError![]T {
            const remaining = self.total_rows - self.current_row;
            const rows = self.allocator.alloc(T, remaining) catch return error.OutOfMemory;
            errdefer self.allocator.free(rows);

            for (rows, 0..) |*row, i| {
                row.* = try self.next() orelse {
                    // Shrink to actual size
                    return self.allocator.realloc(rows, i) catch rows[0..i];
                };
            }

            return rows;
        }

        /// Free a slice of rows allocated by readAll
        pub fn freeRows(self: *Self, rows: []T) void {
            for (rows) |*row| {
                self.freeRow(row);
            }
            self.allocator.free(rows);
        }
    };
}
