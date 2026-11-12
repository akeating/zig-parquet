//! Column Writer
//!
//! Writes column chunks to a Parquet file, including:
//! - Page headers (Thrift serialized)
//! - Page data (optionally compressed)
//! - Tracks file offsets for metadata

const std = @import("std");
const format = @import("format.zig");
const types = @import("types.zig");
const thrift = @import("thrift/mod.zig");
const page_writer = @import("page_writer.zig");
const compress = @import("compress/mod.zig");
const rle_encoder = @import("encoding/rle_encoder.zig");
const plain = @import("encoding/plain.zig");
const statistics = @import("statistics.zig");
const page_index_writer = @import("page_index_writer.zig");
const safe = @import("safe.zig");
const build_options = @import("build_options");

const Optional = types.Optional;

/// Hash context for dictionary keys that supports float types by bitcasting to integers.
/// std.AutoHashMap rejects f32/f64; this context hashes their bit patterns instead.
pub fn DictHashContext(comptime K: type) type {
    return struct {
        pub fn hash(_: @This(), key: K) u64 {
            return std.hash.Wyhash.hash(0, std.mem.asBytes(&key));
        }
        pub fn eql(_: @This(), a: K, b: K) bool {
            if (K == f32 or K == f64) {
                return std.mem.eql(u8, std.mem.asBytes(&a), std.mem.asBytes(&b));
            }
            return a == b;
        }
    };
}

/// Compute CRC32 checksum for page data (per Parquet spec, covers compressed data)
pub fn computePageCrc(data: []const u8) i32 {
    return @bitCast(std.hash.Crc32.hash(data));
}

pub const ColumnWriteError = error{
    OutOfMemory,
    InvalidFixedLength,
    WriteError,
    CompressionError,
    UnsupportedCompression,
    IntegerOverflow,
    ValueTooLarge,
    UnsupportedEncoding,
    NullInRequiredColumn,
    /// A nested leaf's value has another type than the column, or an entry's value does not
    /// match its definition level (see `column_write_values.writeColumnChunkFromValues`).
    TypeMismatch,
};

/// Result of writing a column chunk
pub const ColumnChunkResult = struct {
    /// Column metadata for the footer
    metadata: format.ColumnMetaData,
    /// File offset where the column chunk starts
    file_offset: i64,
    /// Total bytes written
    total_bytes: usize,

    pub fn deinit(self: *ColumnChunkResult, allocator: std.mem.Allocator) void {
        allocator.free(self.metadata.encodings);
        for (self.metadata.path_in_schema) |path| {
            allocator.free(path);
        }
        allocator.free(self.metadata.path_in_schema);
        // Free statistics if present
        if (self.metadata.statistics) |stats| {
            freeStatistics(allocator, stats);
        }
        // Free geospatial statistics if present
        if (self.metadata.geospatial_statistics) |*geo_stats| {
            var gs = geo_stats.*;
            gs.deinit(allocator);
        }
    }
};

// =============================================================================
// Helper Functions
// =============================================================================

/// Free statistics memory
pub fn freeStatistics(allocator: std.mem.Allocator, stats: format.Statistics) void {
    if (stats.min) |m| allocator.free(m);
    if (stats.max) |m| allocator.free(m);
    if (stats.min_value) |m| allocator.free(m);
    if (stats.max_value) |m| allocator.free(m);
}

/// Records a data page of a flat column, one row per value, in `pib`: its location, first
/// row, row count and statistics.
fn recordFlatPage(
    comptime T: type,
    allocator: std.mem.Allocator,
    pib: *page_index_writer.PageIndexBuilder,
    page_values: []const Optional(T),
    page_offset: i64,
    page_bytes: usize,
    first_row: i64,
) ColumnWriteError!void {
    const stats = try flatStatistics(T, allocator, page_values);
    defer if (stats) |st| freeStatistics(allocator, st);

    const empty: []const u8 = &.{};
    const null_count = if (stats) |st| (st.null_count orelse 0) else 0;
    const rows = try safe.castTo(i64, page_values.len);
    pib.recordPage(
        page_offset,
        try safe.castTo(i32, page_bytes),
        first_row,
        rows,
        if (stats) |st| st.getMinBytes() orelse empty else empty,
        if (stats) |st| st.getMaxBytes() orelse empty else empty,
        null_count,
        null_count == rows,
    ) catch return error.OutOfMemory;
}

/// Map a Zig type to Parquet physical type (comptime)
fn typeToPhysicalType(comptime T: type) format.PhysicalType {
    return switch (T) {
        i32 => .int32,
        i64 => .int64,
        f32 => .float,
        f64 => .double,
        bool => .boolean,
        []const u8 => .byte_array,
        else => @compileError("Unsupported type for Parquet: " ++ @typeName(T)),
    };
}

// =============================================================================
// Generic Column Writing (Path-based API)
// =============================================================================

// =============================================================================
// Dictionary-Encoded Column Writing
// =============================================================================

/// Write a dictionary-encoded column chunk with Optional(T) values (unified API).
/// Uses RLE_DICTIONARY encoding for efficient storage of low-cardinality columns.
/// is_optional: true if column is optional in schema (writes def levels), false for required columns.
pub fn writeColumnChunkDictOptionalWithPathArray(
    comptime T: type,
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    path_in_schema: []const []const u8,
    values: []const Optional(T),
    is_optional: bool,
    start_offset: i64,
    codec: format.CompressionCodec,
    dictionary_size_limit: ?usize,
    dictionary_cardinality_threshold: ?f32,
    max_page_size: ?usize,
    write_page_checksum: bool,
    page_index_builder: ?*page_index_writer.PageIndexBuilder,
) ColumnWriteError!ColumnChunkResult {
    if (values.len == 0) {
        // Fall back to plain encoding for empty columns
        return writeColumnChunkOptionalWithEncoding(T, allocator, output, path_in_schema, values, is_optional, start_offset, codec, .plain, max_page_size, write_page_checksum, page_index_builder);
    }

    // Compute statistics using unified API
    var stats_builder = statistics.StatisticsBuilder(T){};
    stats_builder.updateOptional(values);
    var stats = stats_builder.build(allocator) catch return error.OutOfMemory;
    errdefer if (stats) |s| freeStatistics(allocator, s);

    // Build dictionary from non-null values
    var unique_map = std.HashMap(T, u32, DictHashContext(T), std.hash_map.default_max_load_percentage).init(allocator);
    defer unique_map.deinit();
    var dict_values: std.ArrayListUnmanaged(T) = .empty;
    defer dict_values.deinit(allocator);

    var non_null_count: usize = 0;
    for (values) |v| {
        if (v != .null_value) non_null_count += 1;
    }

    var indices = allocator.alloc(u32, non_null_count) catch return error.OutOfMemory;
    defer allocator.free(indices);

    var idx: usize = 0;
    for (values) |v| {
        if (v != .null_value) {
            const val = v.value;
            if (unique_map.get(val)) |dict_idx| {
                indices[idx] = dict_idx;
            } else {
                const new_idx: u32 = try safe.castTo(u32, dict_values.items.len);
                unique_map.put(val, new_idx) catch return error.OutOfMemory;
                dict_values.append(allocator, val) catch return error.OutOfMemory;
                indices[idx] = new_idx;
            }
            idx += 1;

            // Check cardinality threshold early abort
            if (dictionary_cardinality_threshold) |threshold| {
                if (idx >= 1024) {
                    const ratio = @as(f32, @floatFromInt(dict_values.items.len)) / @as(f32, @floatFromInt(idx));
                    if (ratio > threshold) {
                        if (stats) |s| freeStatistics(allocator, s);
                        stats = null;
                        return writeColumnChunkOptionalWithEncoding(T, allocator, output, path_in_schema, values, is_optional, start_offset, codec, .plain, max_page_size, write_page_checksum, page_index_builder);
                    }
                }
            }
        }
    }

    const dict_size = dict_values.items.len;

    // Check dictionary size limit
    const dict_bytes = dict_size * @sizeOf(T);
    if (dictionary_size_limit) |limit| {
        if (dict_bytes > limit) {
            if (stats) |s| freeStatistics(allocator, s);
            stats = null;
            return writeColumnChunkOptionalWithEncoding(T, allocator, output, path_in_schema, values, is_optional, start_offset, codec, .plain, max_page_size, write_page_checksum, page_index_builder);
        }
    }

    // Write dictionary page (PLAIN encoded)
    const bytes_per_value = @sizeOf(T);
    const dict_data = allocator.alloc(u8, dict_size * bytes_per_value) catch return error.OutOfMemory;
    defer allocator.free(dict_data);

    for (dict_values.items, 0..) |v, i| {
        if (T == i32) {
            std.mem.writeInt(i32, dict_data[i * 4 ..][0..4], v, .little);
        } else if (T == i64) {
            std.mem.writeInt(i64, dict_data[i * 8 ..][0..8], v, .little);
        } else if (T == f32) {
            std.mem.writeInt(u32, dict_data[i * 4 ..][0..4], @bitCast(v), .little);
        } else if (T == f64) {
            std.mem.writeInt(u64, dict_data[i * 8 ..][0..8], @bitCast(v), .little);
        }
    }

    // Compress dictionary data if needed
    const dict_compressed: []const u8 = if (codec == .uncompressed)
        dict_data
    else blk: {
        break :blk compress.compress(allocator, dict_data, codec) catch |err| switch (err) {
            error.UnsupportedCompression => return error.UnsupportedCompression,
            error.CompressionError => return error.CompressionError,
            error.OutOfMemory => return error.OutOfMemory,
        };
    };
    defer if (codec != .uncompressed) allocator.free(dict_compressed);

    // Write dictionary page header
    const dict_page_header = format.PageHeader{
        .type_ = .dictionary_page,
        .uncompressed_page_size = try safe.castTo(i32, dict_data.len),
        .compressed_page_size = try safe.castTo(i32, dict_compressed.len),
        .crc = if (write_page_checksum) computePageCrc(dict_compressed) else null,
        .data_page_header = null,
        .dictionary_page_header = .{
            .num_values = try safe.castTo(i32, dict_size),
            .encoding = .plain,
            .is_sorted = false,
        },
    };

    var dict_thrift = thrift.CompactWriter.init(allocator);
    defer dict_thrift.deinit();
    dict_page_header.serialize(&dict_thrift) catch return error.OutOfMemory;
    const dict_header_bytes = dict_thrift.getWritten();

    output.writeAll(dict_header_bytes) catch return error.WriteError;
    output.writeAll(dict_compressed) catch return error.WriteError;

    var total_bytes_written: usize = dict_header_bytes.len + dict_compressed.len;
    var total_uncompressed_written: usize = dict_header_bytes.len + dict_data.len;
    const dict_page_offset = start_offset;
    const data_page_offset = start_offset + try safe.castTo(i64, total_bytes_written);

    // Compute bit width for indices
    const bit_width: u5 = if (dict_size <= 1) 0 else try safe.castTo(u5, std.math.log2_int(usize, dict_size - 1) + 1);

    // Calculate values per page
    const values_per_page: usize = if (max_page_size) |max_size| blk: {
        const bytes_per_value_estimate: usize = 3;
        const usable_size = if (max_size > 20) max_size - 20 else max_size;
        const vpp = usable_size / bytes_per_value_estimate;
        break :blk if (vpp > 0) vpp else 1;
    } else values.len;

    // Write data pages
    var value_offset: usize = 0;
    var index_offset: usize = 0;

    while (value_offset < values.len) {
        const page_end = @min(value_offset + values_per_page, values.len);
        const page_values = values[value_offset..page_end];

        // Count non-null values in this page
        var page_non_null_count: usize = 0;
        for (page_values) |v| {
            if (v != .null_value) page_non_null_count += 1;
        }

        // Get indices for this page
        const page_indices = indices[index_offset .. index_offset + page_non_null_count];
        index_offset += page_non_null_count;

        // Encode indices using RLE/bit-packed hybrid
        const rle_data = rle_encoder.encode(allocator, page_indices, bit_width) catch return error.OutOfMemory;
        defer allocator.free(rle_data);

        // Optional columns always need definition levels, even when all values are non-null
        const data_page_uncompressed = if (is_optional) blk: {
            var page_def_levels = allocator.alloc(bool, page_values.len) catch return error.OutOfMemory;
            defer allocator.free(page_def_levels);

            for (page_values, 0..) |v, i| {
                page_def_levels[i] = v != .null_value;
            }

            const def_level_data = rle_encoder.encodeDefLevelsWithLength(allocator, page_def_levels) catch return error.OutOfMemory;
            defer allocator.free(def_level_data);

            const page_data = allocator.alloc(u8, def_level_data.len + 1 + rle_data.len) catch return error.OutOfMemory;
            @memcpy(page_data[0..def_level_data.len], def_level_data);
            page_data[def_level_data.len] = bit_width;
            @memcpy(page_data[def_level_data.len + 1 ..], rle_data);
            break :blk page_data;
        } else blk: {
            const page_data = allocator.alloc(u8, 1 + rle_data.len) catch return error.OutOfMemory;
            page_data[0] = bit_width;
            @memcpy(page_data[1..], rle_data);
            break :blk page_data;
        };
        defer allocator.free(data_page_uncompressed);

        // Compress data page if needed
        const data_compressed: []const u8 = if (codec == .uncompressed)
            data_page_uncompressed
        else cblk: {
            break :cblk compress.compress(allocator, data_page_uncompressed, codec) catch |err| switch (err) {
                error.UnsupportedCompression => return error.UnsupportedCompression,
                error.CompressionError => return error.CompressionError,
                error.OutOfMemory => return error.OutOfMemory,
            };
        };
        defer if (codec != .uncompressed) allocator.free(data_compressed);

        // Write data page header
        const data_page_header = format.PageHeader{
            .type_ = .data_page,
            .uncompressed_page_size = try safe.castTo(i32, data_page_uncompressed.len),
            .compressed_page_size = try safe.castTo(i32, data_compressed.len),
            .crc = if (write_page_checksum) computePageCrc(data_compressed) else null,
            .data_page_header = .{
                .num_values = try safe.castTo(i32, page_values.len),
                .encoding = .rle_dictionary,
                .definition_level_encoding = .rle,
                .repetition_level_encoding = .rle,
                .statistics = null,
            },
            .dictionary_page_header = null,
        };

        var data_thrift = thrift.CompactWriter.init(allocator);
        defer data_thrift.deinit();
        data_page_header.serialize(&data_thrift) catch return error.OutOfMemory;
        const data_header_bytes = data_thrift.getWritten();

        output.writeAll(data_header_bytes) catch return error.WriteError;
        output.writeAll(data_compressed) catch return error.WriteError;

        const page_bytes = std.math.add(usize, data_header_bytes.len, data_compressed.len) catch return error.IntegerOverflow;
        if (page_index_builder) |pib| try recordFlatPage(T, allocator, pib, page_values, start_offset + try safe.castTo(i64, total_bytes_written), page_bytes, try safe.castTo(i64, value_offset));
        total_bytes_written = std.math.add(usize, total_bytes_written, page_bytes) catch return error.IntegerOverflow;
        const uncompressed_page_bytes = std.math.add(usize, data_header_bytes.len, data_page_uncompressed.len) catch return error.IntegerOverflow;
        total_uncompressed_written = std.math.add(usize, total_uncompressed_written, uncompressed_page_bytes) catch return error.IntegerOverflow;
        value_offset = page_end;
    }

    // Build metadata (integer dict function)
    const path = allocator.alloc([]const u8, path_in_schema.len) catch return error.OutOfMemory;
    for (path_in_schema, 0..) |segment, i| {
        path[i] = allocator.dupe(u8, segment) catch return error.OutOfMemory;
    }

    const encodings = allocator.alloc(format.Encoding, 3) catch return error.OutOfMemory;
    encodings[0] = .plain; // Dictionary page
    encodings[1] = .rle; // Definition levels
    encodings[2] = .rle_dictionary; // Data page

    return .{
        .metadata = .{
            .type_ = comptime typeToPhysicalType(T),
            .encodings = encodings,
            .path_in_schema = path,
            .codec = codec,
            .num_values = try safe.castTo(i64, values.len),
            .total_uncompressed_size = try safe.castTo(i64, total_uncompressed_written),
            .total_compressed_size = try safe.castTo(i64, total_bytes_written),
            .data_page_offset = data_page_offset,
            .index_page_offset = null,
            .dictionary_page_offset = dict_page_offset,
            .statistics = stats,
        },
        .file_offset = start_offset,
        .total_bytes = total_bytes_written,
    };
}

// =============================================================================
// Byte Array Column Writing (non-generic encoding)
// =============================================================================

/// Dictionary-encoded byte array column chunk with Optional values (unified API).
/// is_optional: true if column is optional in schema (writes def levels), false for required columns.
pub fn writeColumnChunkByteArrayDictOptionalWithPathArray(
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    path_in_schema: []const []const u8,
    values: []const Optional([]const u8),
    is_optional: bool,
    start_offset: i64,
    codec: format.CompressionCodec,
    dictionary_size_limit: ?usize,
    dictionary_cardinality_threshold: ?f32,
    max_page_size: ?usize,
    write_page_checksum: bool,
    page_index_builder: ?*page_index_writer.PageIndexBuilder,
) ColumnWriteError!ColumnChunkResult {
    if (values.len == 0) {
        return writeColumnChunkByteArrayOptionalWithEncoding(allocator, output, path_in_schema, values, is_optional, start_offset, codec, .plain, max_page_size, write_page_checksum, page_index_builder);
    }

    // Compute statistics using unified API
    var stats_builder = statistics.ByteArrayStatisticsBuilder.init(allocator);
    stats_builder.updateOptional(values) catch return error.OutOfMemory;
    var stats = stats_builder.build();
    errdefer if (stats) |s| freeStatistics(allocator, s);

    // Build dictionary from non-null values
    var unique_map = std.StringHashMap(u32).init(allocator);
    defer unique_map.deinit();
    var dict_values: std.ArrayListUnmanaged([]const u8) = .empty;
    defer dict_values.deinit(allocator);

    var non_null_count: usize = 0;
    for (values) |v| {
        if (v != .null_value) non_null_count += 1;
    }

    var indices = allocator.alloc(u32, non_null_count) catch return error.OutOfMemory;
    defer allocator.free(indices);

    var dict_bytes: usize = 0;
    var idx: usize = 0;
    for (values) |v| {
        if (v != .null_value) {
            const val = v.value;
            if (val.len > std.math.maxInt(i32)) return error.ValueTooLarge;
            if (unique_map.get(val)) |dict_idx| {
                indices[idx] = dict_idx;
            } else {
                const new_idx: u32 = try safe.castTo(u32, dict_values.items.len);
                unique_map.put(val, new_idx) catch return error.OutOfMemory;
                dict_values.append(allocator, val) catch return error.OutOfMemory;
                dict_bytes += 4 + val.len;
                indices[idx] = new_idx;
            }
            idx += 1;

            if (dictionary_cardinality_threshold) |threshold| {
                if (idx >= 1024) {
                    const ratio = @as(f32, @floatFromInt(dict_values.items.len)) / @as(f32, @floatFromInt(idx));
                    if (ratio > threshold) {
                        if (stats) |s| freeStatistics(allocator, s);
                        stats = null;
                        return writeColumnChunkByteArrayOptionalWithEncoding(allocator, output, path_in_schema, values, is_optional, start_offset, codec, .plain, max_page_size, write_page_checksum, page_index_builder);
                    }
                }
            }
        }
    }

    const dict_size = dict_values.items.len;

    // Check dictionary size limit
    if (dictionary_size_limit) |limit| {
        if (dict_bytes > limit) {
            if (stats) |s| freeStatistics(allocator, s);
            stats = null;
            return writeColumnChunkByteArrayOptionalWithEncoding(allocator, output, path_in_schema, values, is_optional, start_offset, codec, .plain, max_page_size, write_page_checksum, page_index_builder);
        }
    }

    // Write dictionary page (length-prefixed byte arrays)
    const dict_data = allocator.alloc(u8, dict_bytes) catch return error.OutOfMemory;
    defer allocator.free(dict_data);

    var dict_offset: usize = 0;
    for (dict_values.items) |v| {
        std.mem.writeInt(u32, dict_data[dict_offset..][0..4], try safe.castTo(u32, v.len), .little);
        dict_offset += 4;
        @memcpy(dict_data[dict_offset..][0..v.len], v);
        dict_offset += v.len;
    }

    // Compress dictionary data if needed
    const dict_compressed: []const u8 = if (codec == .uncompressed)
        dict_data
    else blk: {
        break :blk compress.compress(allocator, dict_data, codec) catch |err| switch (err) {
            error.UnsupportedCompression => return error.UnsupportedCompression,
            error.CompressionError => return error.CompressionError,
            error.OutOfMemory => return error.OutOfMemory,
        };
    };
    defer if (codec != .uncompressed) allocator.free(dict_compressed);

    // Write dictionary page header
    const dict_page_header = format.PageHeader{
        .type_ = .dictionary_page,
        .uncompressed_page_size = try safe.castTo(i32, dict_data.len),
        .compressed_page_size = try safe.castTo(i32, dict_compressed.len),
        .crc = if (write_page_checksum) computePageCrc(dict_compressed) else null,
        .data_page_header = null,
        .dictionary_page_header = .{
            .num_values = try safe.castTo(i32, dict_size),
            .encoding = .plain,
            .is_sorted = false,
        },
    };

    var dict_thrift = thrift.CompactWriter.init(allocator);
    defer dict_thrift.deinit();
    dict_page_header.serialize(&dict_thrift) catch return error.OutOfMemory;
    const dict_header_bytes = dict_thrift.getWritten();

    output.writeAll(dict_header_bytes) catch return error.WriteError;
    output.writeAll(dict_compressed) catch return error.WriteError;

    var total_bytes_written: usize = dict_header_bytes.len + dict_compressed.len;
    var total_uncompressed_written: usize = dict_header_bytes.len + dict_data.len;
    const dict_page_offset = start_offset;
    const data_page_offset = start_offset + try safe.castTo(i64, total_bytes_written);

    // Compute bit width for indices
    const bit_width: u5 = if (dict_size <= 1) 0 else try safe.castTo(u5, std.math.log2_int(usize, dict_size - 1) + 1);

    // Calculate values per page
    const values_per_page: usize = if (max_page_size) |max_size| blk: {
        const bytes_per_value_estimate: usize = 3;
        const usable_size = if (max_size > 20) max_size - 20 else max_size;
        const vpp = usable_size / bytes_per_value_estimate;
        break :blk if (vpp > 0) vpp else 1;
    } else values.len;

    // Write data pages
    var value_offset: usize = 0;
    var index_offset: usize = 0;

    while (value_offset < values.len) {
        const page_end = @min(value_offset + values_per_page, values.len);
        const page_values = values[value_offset..page_end];

        // Count non-null values in this page
        var page_non_null_count: usize = 0;
        for (page_values) |v| {
            if (v != .null_value) page_non_null_count += 1;
        }

        // Get indices for this page
        const page_indices = indices[index_offset .. index_offset + page_non_null_count];
        index_offset += page_non_null_count;

        // Encode indices using RLE/bit-packed hybrid
        const rle_data = rle_encoder.encode(allocator, page_indices, bit_width) catch return error.OutOfMemory;
        defer allocator.free(rle_data);

        // Optional columns always need definition levels, even when all values are non-null
        const data_page_uncompressed = if (is_optional) blk: {
            var page_def_levels = allocator.alloc(bool, page_values.len) catch return error.OutOfMemory;
            defer allocator.free(page_def_levels);

            for (page_values, 0..) |v, i| {
                page_def_levels[i] = v != .null_value;
            }

            const def_level_data = rle_encoder.encodeDefLevelsWithLength(allocator, page_def_levels) catch return error.OutOfMemory;
            defer allocator.free(def_level_data);

            const page_data = allocator.alloc(u8, def_level_data.len + 1 + rle_data.len) catch return error.OutOfMemory;
            @memcpy(page_data[0..def_level_data.len], def_level_data);
            page_data[def_level_data.len] = bit_width;
            @memcpy(page_data[def_level_data.len + 1 ..], rle_data);
            break :blk page_data;
        } else blk: {
            const page_data = allocator.alloc(u8, 1 + rle_data.len) catch return error.OutOfMemory;
            page_data[0] = bit_width;
            @memcpy(page_data[1..], rle_data);
            break :blk page_data;
        };
        defer allocator.free(data_page_uncompressed);

        // Compress data page if needed
        const data_compressed: []const u8 = if (codec == .uncompressed)
            data_page_uncompressed
        else cblk: {
            break :cblk compress.compress(allocator, data_page_uncompressed, codec) catch |err| switch (err) {
                error.UnsupportedCompression => return error.UnsupportedCompression,
                error.CompressionError => return error.CompressionError,
                error.OutOfMemory => return error.OutOfMemory,
            };
        };
        defer if (codec != .uncompressed) allocator.free(data_compressed);

        // Write data page header
        const data_page_header = format.PageHeader{
            .type_ = .data_page,
            .uncompressed_page_size = try safe.castTo(i32, data_page_uncompressed.len),
            .compressed_page_size = try safe.castTo(i32, data_compressed.len),
            .crc = if (write_page_checksum) computePageCrc(data_compressed) else null,
            .data_page_header = .{
                .num_values = try safe.castTo(i32, page_values.len),
                .encoding = .rle_dictionary,
                .definition_level_encoding = .rle,
                .repetition_level_encoding = .rle,
                .statistics = null,
            },
            .dictionary_page_header = null,
        };

        var data_thrift = thrift.CompactWriter.init(allocator);
        defer data_thrift.deinit();
        data_page_header.serialize(&data_thrift) catch return error.OutOfMemory;
        const data_header_bytes = data_thrift.getWritten();

        output.writeAll(data_header_bytes) catch return error.WriteError;
        output.writeAll(data_compressed) catch return error.WriteError;

        const page_bytes = std.math.add(usize, data_header_bytes.len, data_compressed.len) catch return error.IntegerOverflow;
        if (page_index_builder) |pib| try recordFlatPage([]const u8, allocator, pib, page_values, start_offset + try safe.castTo(i64, total_bytes_written), page_bytes, try safe.castTo(i64, value_offset));
        total_bytes_written = std.math.add(usize, total_bytes_written, page_bytes) catch return error.IntegerOverflow;
        const uncompressed_page_bytes = std.math.add(usize, data_header_bytes.len, data_page_uncompressed.len) catch return error.IntegerOverflow;
        total_uncompressed_written = std.math.add(usize, total_uncompressed_written, uncompressed_page_bytes) catch return error.IntegerOverflow;
        value_offset = page_end;
    }

    // Build metadata (byte array dict function)
    const path = allocator.alloc([]const u8, path_in_schema.len) catch return error.OutOfMemory;
    for (path_in_schema, 0..) |segment, i| {
        path[i] = allocator.dupe(u8, segment) catch return error.OutOfMemory;
    }

    const encodings = allocator.alloc(format.Encoding, 3) catch return error.OutOfMemory;
    encodings[0] = .plain; // Dictionary page
    encodings[1] = .rle; // Definition levels
    encodings[2] = .rle_dictionary; // Data page

    return .{
        .metadata = .{
            .type_ = .byte_array,
            .encodings = encodings,
            .path_in_schema = path,
            .codec = codec,
            .num_values = try safe.castTo(i64, values.len),
            .total_uncompressed_size = try safe.castTo(i64, total_uncompressed_written),
            .total_compressed_size = try safe.castTo(i64, total_bytes_written),
            .data_page_offset = data_page_offset,
            .index_page_offset = null,
            .dictionary_page_offset = dict_page_offset,
            .statistics = stats,
        },
        .file_offset = start_offset,
        .total_bytes = total_bytes_written,
    };
}

/// Write a FIXED_LEN_BYTE_ARRAY column chunk with Optional values and a specific encoding.
/// With `max_page_size`, a data page holds `max_page_size / fixed_len` values (at least one);
/// without it, the chunk is one page.
/// is_optional: true if column is optional in schema (writes def levels), false for required columns.
/// page_index_builder (optional) receives one entry per data page.
pub fn writeColumnChunkFixedByteArrayOptionalWithEncoding(
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    path_in_schema: []const []const u8,
    values: []const Optional([]const u8),
    fixed_len: u32,
    is_optional: bool,
    start_offset: i64,
    codec: format.CompressionCodec,
    value_encoding: format.Encoding,
    max_page_size: ?usize,
    write_page_checksum: bool,
    page_index_builder: ?*page_index_writer.PageIndexBuilder,
) ColumnWriteError!ColumnChunkResult {
    if (fixed_len == 0) return error.InvalidFixedLength;
    const values_per_page = if (max_page_size) |mps| @max(1, mps / fixed_len) else values.len;
    return writeFlatChunk([]const u8, allocator, output, path_in_schema, values, .fixed_len_byte_array, fixed_len, is_optional, start_offset, codec, value_encoding, .{ .values = values_per_page }, write_page_checksum, page_index_builder);
}

// =============================================================================
// Value-based column writing for nested types
// =============================================================================

pub const values_writer = @import("column_write_values.zig");
pub const NestedEncodingOpts = values_writer.NestedEncodingOpts;
pub const writeColumnChunkFromValues = values_writer.writeColumnChunkFromValues;

// Tests
test "write column chunk i32 optional" {
    const allocator = std.testing.allocator;

    var aw: std.Io.Writer.Allocating = .init(allocator);
    defer aw.deinit();

    const values = [_]Optional(i32){
        .{ .value = 1 },
        .{ .value = 2 },
        .{ .value = 3 },
        .{ .value = 4 },
        .{ .value = 5 },
    };
    var result = try writeColumnChunkOptionalWithPathArray(
        i32,
        allocator,
        &aw.writer,
        &.{"test_col"},
        &values,
        false, // is_optional
        4, // After PAR1 magic
        .uncompressed,
        true, // write_page_checksum
    );
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 5), result.metadata.num_values);
    try std.testing.expectEqual(format.PhysicalType.int32, result.metadata.type_);
    try std.testing.expect(result.total_bytes > 0);

    // Check that data was written (aw.writer.end tracks bytes written)
    try std.testing.expectEqual(result.total_bytes, aw.writer.end);
}

test "write column chunk i64 optional" {
    const allocator = std.testing.allocator;

    var aw: std.Io.Writer.Allocating = .init(allocator);
    defer aw.deinit();

    const values = [_]Optional(i64){
        .{ .value = 100 },
        .{ .null_value = {} },
        .{ .value = 300 },
    };
    var result = try writeColumnChunkOptionalWithPathArray(
        i64,
        allocator,
        &aw.writer,
        &.{"nullable_col"},
        &values,
        true, // is_optional
        4,
        .uncompressed,
        true, // write_page_checksum
    );
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 3), result.metadata.num_values);
    try std.testing.expectEqual(format.PhysicalType.int64, result.metadata.type_);
}

test "write column chunk with statistics i32 optional" {
    const allocator = std.testing.allocator;

    var aw: std.Io.Writer.Allocating = .init(allocator);
    defer aw.deinit();

    const values = [_]Optional(i32){
        .{ .value = 5 },
        .{ .value = 2 },
        .{ .value = 8 },
        .{ .value = 1 },
        .{ .value = 9 },
    };
    var result = try writeColumnChunkOptionalWithPathArray(
        i32,
        allocator,
        &aw.writer,
        &.{"stats_col"},
        &values,
        false, // is_optional
        4,
        .uncompressed,
        true, // write_page_checksum
    );
    defer result.deinit(allocator);

    // Check that statistics are present
    try std.testing.expect(result.metadata.statistics != null);

    const stats = result.metadata.statistics.?;

    // Verify min_value is 1 (PLAIN encoded as little-endian i32)
    try std.testing.expect(stats.min_value != null);
    try std.testing.expectEqual(@as(usize, 4), stats.min_value.?.len);
    try std.testing.expectEqual(@as(i32, 1), std.mem.readInt(i32, stats.min_value.?[0..4], .little));

    // Verify max_value is 9
    try std.testing.expect(stats.max_value != null);
    try std.testing.expectEqual(@as(usize, 4), stats.max_value.?.len);
    try std.testing.expectEqual(@as(i32, 9), std.mem.readInt(i32, stats.max_value.?[0..4], .little));

    // Verify null_count is 0
    try std.testing.expect(stats.null_count != null);
    try std.testing.expectEqual(@as(i64, 0), stats.null_count.?);

    // Deprecated fields should also be set
    try std.testing.expect(stats.min != null);
    try std.testing.expect(stats.max != null);
}

// =============================================================================
// Encoding-Aware Column Writing
// =============================================================================

/// Write a column chunk with a specific value encoding.
/// Supports delta encodings for improved compression of specific data patterns.
fn writeColumnChunkWithEncoding(
    comptime T: type,
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    path_in_schema: []const []const u8,
    values: []const T,
    is_optional: bool,
    start_offset: i64,
    codec: format.CompressionCodec,
    value_encoding: format.Encoding,
    write_page_checksum: bool,
) ColumnWriteError!ColumnChunkResult {
    // Compute statistics from values
    var stats_builder = statistics.StatisticsBuilder(T){};
    stats_builder.update(values);
    const stats = stats_builder.build(allocator) catch return error.OutOfMemory;
    errdefer if (stats) |s| freeStatistics(allocator, s);

    // Write data page with specified encoding
    var page_result = page_writer.writeDataPageWithEncoding(allocator, T, values, is_optional, value_encoding) catch |e| switch (e) {
        error.OutOfMemory => return error.OutOfMemory,
        error.InvalidFixedLength => return error.InvalidFixedLength,
        error.IntegerOverflow => return error.IntegerOverflow,
        error.ValueTooLarge => return error.ValueTooLarge,
        error.NullInRequiredColumn => return error.NullInRequiredColumn,
        error.UnsupportedEncoding => return error.UnsupportedEncoding,
    };
    defer page_result.deinit(allocator);

    // Compress if needed
    const compressed_data = if (codec != .uncompressed) blk: {
        break :blk compress.compress(allocator, page_result.data, codec) catch |err| switch (err) {
            error.UnsupportedCompression => return error.UnsupportedCompression,
            else => return error.CompressionError,
        };
    } else page_result.data;
    defer if (codec != .uncompressed) allocator.free(compressed_data);

    // Create page header with correct encoding
    const page_header = format.PageHeader{
        .type_ = .data_page,
        .uncompressed_page_size = try safe.castTo(i32, page_result.data.len),
        .compressed_page_size = try safe.castTo(i32, compressed_data.len),
        .crc = if (write_page_checksum) computePageCrc(compressed_data) else null,
        .data_page_header = .{
            .num_values = try safe.castTo(i32, page_result.num_values),
            .encoding = value_encoding,
            .definition_level_encoding = .rle,
            .repetition_level_encoding = .rle,
            .statistics = null,
        },
        .dictionary_page_header = null,
    };

    // Serialize page header
    var thrift_writer = thrift.CompactWriter.init(allocator);
    defer thrift_writer.deinit();

    page_header.serialize(&thrift_writer) catch return error.OutOfMemory;
    const header_bytes = thrift_writer.getWritten();

    // Write header and compressed data
    output.writeAll(header_bytes) catch return error.WriteError;
    output.writeAll(compressed_data) catch return error.WriteError;

    const total_bytes_written = header_bytes.len + compressed_data.len;

    // Duplicate path_in_schema for metadata
    const path = allocator.alloc([]const u8, path_in_schema.len) catch return error.OutOfMemory;
    for (path_in_schema, 0..) |segment, i| {
        path[i] = allocator.dupe(u8, segment) catch return error.OutOfMemory;
    }

    // Build encodings list with the actual encoding used
    const encodings = allocator.alloc(format.Encoding, 2) catch return error.OutOfMemory;
    encodings[0] = .rle; // Definition levels
    encodings[1] = value_encoding; // Values

    var result = ColumnChunkResult{
        .metadata = .{
            .type_ = comptime typeToPhysicalType(T),
            .encodings = encodings,
            .path_in_schema = path,
            .codec = codec,
            .num_values = try safe.castTo(i64, page_result.num_values),
            .total_uncompressed_size = try safe.castTo(i64, header_bytes.len + page_result.data.len),
            .total_compressed_size = try safe.castTo(i64, total_bytes_written),
            .data_page_offset = start_offset,
            .index_page_offset = null,
            .dictionary_page_offset = null,
            .statistics = null,
        },
        .file_offset = start_offset,
        .total_bytes = total_bytes_written,
    };
    result.metadata.statistics = stats;
    return result;
}

// =============================================================================
// Unified Optional Column Writers (Phase 11 style)
// =============================================================================

/// Write a column chunk with Optional(T) values and a specific encoding.
/// With `max_page_size`, the chunk is split into data pages of about that many bytes of
/// PLAIN-encoded values; without it, the chunk is one page.
/// is_optional: true if column is optional in schema (writes def levels), false for required columns.
/// page_index_builder (optional) receives one entry per data page.
pub fn writeColumnChunkOptionalWithEncoding(
    comptime T: type,
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    path_in_schema: []const []const u8,
    values: []const Optional(T),
    is_optional: bool,
    start_offset: i64,
    codec: format.CompressionCodec,
    value_encoding: format.Encoding,
    max_page_size: ?usize,
    write_page_checksum: bool,
    page_index_builder: ?*page_index_writer.PageIndexBuilder,
) ColumnWriteError!ColumnChunkResult {
    // PLAIN encodes a boolean in one bit.
    const value_bits: usize = if (T == bool) 1 else @bitSizeOf(T);
    const values_per_page = if (max_page_size) |mps| @max(1, (mps *| 8) / value_bits) else values.len;
    return writeFlatChunk(T, allocator, output, path_in_schema, values, comptime typeToPhysicalType(T), 0, is_optional, start_offset, codec, value_encoding, .{ .values = values_per_page }, write_page_checksum, page_index_builder);
}

/// Write a byte array column chunk with Optional values and a specific encoding.
/// With `max_page_size`, a page ends before the first value at which its values reach
/// `max_page_size` bytes in PLAIN form (4 length bytes and the bytes of each non-null value),
/// as `column_write_values` cuts the pages of nested columns; without it, the chunk is one
/// page.
/// is_optional: true if column is optional in schema (writes def levels), false for required columns.
/// page_index_builder (optional) receives one entry per data page.
pub fn writeColumnChunkByteArrayOptionalWithEncoding(
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    path_in_schema: []const []const u8,
    values: []const Optional([]const u8),
    is_optional: bool,
    start_offset: i64,
    codec: format.CompressionCodec,
    value_encoding: format.Encoding,
    max_page_size: ?usize,
    write_page_checksum: bool,
    page_index_builder: ?*page_index_writer.PageIndexBuilder,
) ColumnWriteError!ColumnChunkResult {
    const limit: PageLimit = if (max_page_size) |mps| .{ .plain_bytes = mps } else .{ .values = values.len };
    return writeFlatChunk([]const u8, allocator, output, path_in_schema, values, .byte_array, 0, is_optional, start_offset, codec, value_encoding, limit, write_page_checksum, page_index_builder);
}

/// Statistics of a flat column's values, including the null count.
fn flatStatistics(comptime T: type, allocator: std.mem.Allocator, values: []const Optional(T)) ColumnWriteError!?format.Statistics {
    if (T == []const u8) {
        var b = statistics.ByteArrayStatisticsBuilder.init(allocator);
        defer b.deinit();
        try b.updateOptional(values);
        return b.build();
    }
    var b = statistics.StatisticsBuilder(T){};
    b.updateOptional(values);
    return b.build(allocator) catch error.OutOfMemory;
}

fn mapPageWriteError(e: page_writer.PageWriteError) ColumnWriteError {
    return switch (e) {
        error.OutOfMemory => error.OutOfMemory,
        error.InvalidFixedLength => error.InvalidFixedLength,
        error.IntegerOverflow => error.IntegerOverflow,
        error.ValueTooLarge => error.ValueTooLarge,
        error.NullInRequiredColumn => error.NullInRequiredColumn,
        error.UnsupportedEncoding => error.UnsupportedEncoding,
    };
}

/// Where `writeFlatChunk` ends a data page.
const PageLimit = union(enum) {
    /// At most this many values.
    values: usize,
    /// Before the first value at which the page's byte-array values reach this many bytes
    /// in PLAIN form; a page holds at least one value.
    plain_bytes: usize,

    /// The end of the page that starts at `start`.
    fn pageEnd(self: PageLimit, comptime T: type, values: []const Optional(T), start: usize) usize {
        switch (self) {
            .values => |n| return start + @min(@max(n, 1), values.len - start),
            .plain_bytes => |limit| {
                if (T != []const u8) unreachable; // only byte-array columns use this limit
                var bytes: usize = 0;
                var end = start;
                while (end < values.len) : (end += 1) {
                    if (end > start and bytes >= limit) break;
                    switch (values[end]) {
                        .value => |v| bytes +|= 4 + v.len,
                        .null_value => {},
                    }
                }
                return end;
            },
        }
    }
};

/// Writes a flat column chunk as data pages ended by `limit` (one row per value) and records
/// each page in `page_index_builder`. `fixed_len` is the value length of a
/// FIXED_LEN_BYTE_ARRAY column and 0 for every other type.
fn writeFlatChunk(
    comptime T: type,
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    path_in_schema: []const []const u8,
    values: []const Optional(T),
    physical_type: format.PhysicalType,
    fixed_len: usize,
    is_optional: bool,
    start_offset: i64,
    codec: format.CompressionCodec,
    value_encoding: format.Encoding,
    limit: PageLimit,
    write_page_checksum: bool,
    page_index_builder: ?*page_index_writer.PageIndexBuilder,
) ColumnWriteError!ColumnChunkResult {
    const stats = try flatStatistics(T, allocator, values);
    errdefer if (stats) |s| freeStatistics(allocator, s);

    var total_bytes_written: usize = 0;
    var total_uncompressed_written: usize = 0;
    var total_values: usize = 0;
    var offset: usize = 0;

    while (offset < values.len) {
        const chunk_end = limit.pageEnd(T, values, offset);
        const chunk = values[offset..chunk_end];

        var page_result = (if (T == []const u8 and physical_type == .fixed_len_byte_array)
            page_writer.writeDataPageFixedByteArrayOptionalWithEncoding(allocator, chunk, fixed_len, is_optional, value_encoding)
        else if (T == []const u8)
            page_writer.writeDataPageByteArrayOptionalWithEncoding(allocator, chunk, is_optional, value_encoding)
        else
            page_writer.writeDataPageOptionalWithEncoding(allocator, T, chunk, is_optional, value_encoding)) catch |e| return mapPageWriteError(e);
        defer page_result.deinit(allocator);

        const compressed_data = if (codec != .uncompressed) blk: {
            break :blk compress.compress(allocator, page_result.data, codec) catch |err| switch (err) {
                error.UnsupportedCompression => return error.UnsupportedCompression,
                else => return error.CompressionError,
            };
        } else page_result.data;
        defer if (codec != .uncompressed) allocator.free(compressed_data);

        const page_header = format.PageHeader{
            .type_ = .data_page,
            .uncompressed_page_size = try safe.castTo(i32, page_result.data.len),
            .compressed_page_size = try safe.castTo(i32, compressed_data.len),
            .crc = if (write_page_checksum) computePageCrc(compressed_data) else null,
            .data_page_header = .{
                .num_values = try safe.castTo(i32, page_result.num_values),
                .encoding = value_encoding,
                .definition_level_encoding = .rle,
                .repetition_level_encoding = .rle,
                .statistics = null,
            },
            .dictionary_page_header = null,
        };

        var thrift_writer = thrift.CompactWriter.init(allocator);
        defer thrift_writer.deinit();
        page_header.serialize(&thrift_writer) catch return error.OutOfMemory;
        const header_bytes = thrift_writer.getWritten();

        output.writeAll(header_bytes) catch return error.WriteError;
        output.writeAll(compressed_data) catch return error.WriteError;

        const page_bytes = std.math.add(usize, header_bytes.len, compressed_data.len) catch return error.IntegerOverflow;
        if (page_index_builder) |pib| try recordFlatPage(T, allocator, pib, chunk, start_offset + try safe.castTo(i64, total_bytes_written), page_bytes, try safe.castTo(i64, offset));

        total_bytes_written = std.math.add(usize, total_bytes_written, page_bytes) catch return error.IntegerOverflow;
        const uncompressed_page_bytes = std.math.add(usize, header_bytes.len, page_result.data.len) catch return error.IntegerOverflow;
        total_uncompressed_written = std.math.add(usize, total_uncompressed_written, uncompressed_page_bytes) catch return error.IntegerOverflow;
        total_values += page_result.num_values;
        offset = chunk_end;
    }

    const path = allocator.alloc([]const u8, path_in_schema.len) catch return error.OutOfMemory;
    var copied: usize = 0;
    errdefer {
        for (path[0..copied]) |seg| allocator.free(seg);
        allocator.free(path);
    }
    for (path_in_schema, path) |segment, *dst| {
        dst.* = allocator.dupe(u8, segment) catch return error.OutOfMemory;
        copied += 1;
    }

    // Definition levels, then values.
    const encodings = allocator.dupe(format.Encoding, &.{ .rle, value_encoding }) catch return error.OutOfMemory;

    return .{
        .metadata = .{
            .type_ = physical_type,
            .encodings = encodings,
            .path_in_schema = path,
            .codec = codec,
            .num_values = try safe.castTo(i64, total_values),
            .total_uncompressed_size = try safe.castTo(i64, total_uncompressed_written),
            .total_compressed_size = try safe.castTo(i64, total_bytes_written),
            .data_page_offset = start_offset,
            .index_page_offset = null,
            .dictionary_page_offset = null,
            .statistics = stats,
        },
        .file_offset = start_offset,
        .total_bytes = total_bytes_written,
    };
}

/// Write a column chunk with Optional(T) values (unified API).
/// This is the preferred method - accepts the same Optional(T) type that Reader returns.
/// is_optional: true if column is optional in schema (writes def levels), false for required columns.
pub fn writeColumnChunkOptionalWithPathArray(
    comptime T: type,
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    path_in_schema: []const []const u8,
    values: []const Optional(T),
    is_optional: bool,
    start_offset: i64,
    codec: format.CompressionCodec,
    write_page_checksum: bool,
) ColumnWriteError!ColumnChunkResult {
    return writeColumnChunkOptionalWithEncoding(T, allocator, output, path_in_schema, values, is_optional, start_offset, codec, .plain, null, write_page_checksum, null);
}

/// Write a byte array column chunk with Optional values (unified API).
/// is_optional: true if column is optional in schema (writes def levels), false for required columns.
pub fn writeColumnChunkByteArrayOptionalWithPathArray(
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    path_in_schema: []const []const u8,
    values: []const Optional([]const u8),
    is_optional: bool,
    start_offset: i64,
    codec: format.CompressionCodec,
    write_page_checksum: bool,
) ColumnWriteError!ColumnChunkResult {
    return writeColumnChunkByteArrayOptionalWithEncoding(allocator, output, path_in_schema, values, is_optional, start_offset, codec, .plain, null, write_page_checksum, null);
}

// =============================================================================
// INT96 Column Writing (Legacy timestamp format)
// =============================================================================

/// Write an INT96 column chunk (legacy timestamp format).
/// Takes i64 nanoseconds and encodes as 12-byte INT96 values.
/// is_optional: true if column is optional in schema (writes def levels), false for required columns.
pub fn writeColumnChunkInt96OptionalWithPathArray(
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    path_in_schema: []const []const u8,
    values: []const Optional(i64),
    is_optional: bool,
    start_offset: i64,
    codec: format.CompressionCodec,
    write_page_checksum: bool,
) ColumnWriteError!ColumnChunkResult {
    // Write data page using INT96 encoding
    var page_result = page_writer.writeDataPageInt96Optional(allocator, values, is_optional) catch |e| switch (e) {
        error.OutOfMemory => return error.OutOfMemory,
        error.InvalidFixedLength => return error.InvalidFixedLength,
        error.IntegerOverflow => return error.IntegerOverflow,
        error.ValueTooLarge => return error.ValueTooLarge,
        error.NullInRequiredColumn => return error.NullInRequiredColumn,
        error.UnsupportedEncoding => return error.UnsupportedEncoding,
    };
    defer page_result.deinit(allocator);

    // Compress if needed
    const compressed_data = if (codec != .uncompressed) blk: {
        break :blk compress.compress(allocator, page_result.data, codec) catch |err| switch (err) {
            error.UnsupportedCompression => return error.UnsupportedCompression,
            else => return error.CompressionError,
        };
    } else page_result.data;
    defer if (codec != .uncompressed) allocator.free(compressed_data);

    // Create page header
    const page_header = format.PageHeader{
        .type_ = .data_page,
        .uncompressed_page_size = try safe.castTo(i32, page_result.data.len),
        .compressed_page_size = try safe.castTo(i32, compressed_data.len),
        .crc = if (write_page_checksum) computePageCrc(compressed_data) else null,
        .data_page_header = .{
            .num_values = try safe.castTo(i32, page_result.num_values),
            .encoding = .plain,
            .definition_level_encoding = .rle,
            .repetition_level_encoding = .rle,
            .statistics = null,
        },
        .dictionary_page_header = null,
        .data_page_header_v2 = null,
    };

    // Serialize header
    var header_serializer = thrift.CompactWriter.init(allocator);
    defer header_serializer.deinit();
    page_header.serialize(&header_serializer) catch return error.WriteError;
    const header_bytes = header_serializer.getWritten();

    // Write header + data
    output.writeAll(header_bytes) catch return error.WriteError;
    output.writeAll(compressed_data) catch return error.WriteError;

    const total_bytes_written = header_bytes.len + compressed_data.len;

    // Create path array
    const path = allocator.alloc([]const u8, path_in_schema.len) catch return error.OutOfMemory;
    errdefer allocator.free(path);
    for (path_in_schema, 0..) |seg, i| {
        path[i] = allocator.dupe(u8, seg) catch return error.OutOfMemory;
    }

    // Build encodings list
    const encodings = allocator.alloc(format.Encoding, 2) catch return error.OutOfMemory;
    encodings[0] = .rle; // Definition levels
    encodings[1] = .plain; // Values

    return ColumnChunkResult{
        .metadata = .{
            .type_ = .int96, // INT96 physical type
            .encodings = encodings,
            .path_in_schema = path,
            .codec = codec,
            .num_values = try safe.castTo(i64, page_result.num_values),
            .total_uncompressed_size = try safe.castTo(i64, header_bytes.len + page_result.data.len),
            .total_compressed_size = try safe.castTo(i64, total_bytes_written),
            .data_page_offset = start_offset,
            .index_page_offset = null,
            .dictionary_page_offset = null,
            .statistics = null, // INT96 statistics not typically needed
        },
        .file_offset = start_offset,
        .total_bytes = total_bytes_written,
    };
}

test "write column chunk with statistics optional i64" {
    const allocator = std.testing.allocator;

    var aw: std.Io.Writer.Allocating = .init(allocator);
    defer aw.deinit();

    const values = [_]Optional(i64){
        .{ .value = 100 },
        .{ .null_value = {} },
        .{ .value = 50 },
        .{ .null_value = {} },
        .{ .value = 75 },
    };
    var result = try writeColumnChunkOptionalWithPathArray(
        i64,
        allocator,
        &aw.writer,
        &.{"nullable_stats_col"},
        &values,
        true, // is_optional
        4,
        .uncompressed,
        true, // write_page_checksum
    );
    defer result.deinit(allocator);

    // Check that statistics are present
    try std.testing.expect(result.metadata.statistics != null);

    const stats = result.metadata.statistics.?;

    // Verify min_value is 50
    try std.testing.expect(stats.min_value != null);
    try std.testing.expectEqual(@as(i64, 50), std.mem.readInt(i64, stats.min_value.?[0..8], .little));

    // Verify max_value is 100
    try std.testing.expect(stats.max_value != null);
    try std.testing.expectEqual(@as(i64, 100), std.mem.readInt(i64, stats.max_value.?[0..8], .little));

    // Verify null_count is 2
    try std.testing.expect(stats.null_count != null);
    try std.testing.expectEqual(@as(i64, 2), stats.null_count.?);
}

test "write column chunk with statistics byte array" {
    const allocator = std.testing.allocator;

    var aw: std.Io.Writer.Allocating = .init(allocator);
    defer aw.deinit();

    const values = [_]Optional([]const u8){
        .{ .value = "banana" },
        .{ .value = "apple" },
        .{ .value = "cherry" },
    };
    var result = try writeColumnChunkByteArrayOptionalWithPathArray(
        allocator,
        &aw.writer,
        &.{"string_stats_col"},
        &values,
        false, // is_optional
        4,
        .uncompressed,
        true, // write_page_checksum
    );
    defer result.deinit(allocator);

    // Check that statistics are present
    try std.testing.expect(result.metadata.statistics != null);

    const stats = result.metadata.statistics.?;

    // Verify min_value is "apple" (lexicographically smallest)
    try std.testing.expect(stats.min_value != null);
    try std.testing.expectEqualStrings("apple", stats.min_value.?);

    // Verify max_value is "cherry" (lexicographically largest)
    try std.testing.expect(stats.max_value != null);
    try std.testing.expectEqualStrings("cherry", stats.max_value.?);

    // Verify null_count is 0
    try std.testing.expect(stats.null_count != null);
    try std.testing.expectEqual(@as(i64, 0), stats.null_count.?);
}

// =============================================================================
// Delta Encoding Round-Trip Tests
// =============================================================================

test "write column chunk with delta_binary_packed encoding" {
    const allocator = std.testing.allocator;

    var aw: std.Io.Writer.Allocating = .init(allocator);
    defer aw.deinit();

    const values = [_]i32{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    var result = try writeColumnChunkWithEncoding(
        i32,
        allocator,
        &aw.writer,
        &.{"delta_int_col"},
        &values,
        false,
        4,
        .uncompressed,
        .delta_binary_packed,
        true, // write_page_checksum
    );
    defer result.deinit(allocator);

    // Verify metadata
    try std.testing.expectEqual(format.PhysicalType.int32, result.metadata.type_);
    try std.testing.expectEqual(@as(i64, 10), result.metadata.num_values);
    try std.testing.expectEqual(format.Encoding.delta_binary_packed, result.metadata.encodings[1]);
}

test "write column chunk with byte_stream_split encoding" {
    const allocator = std.testing.allocator;

    var aw: std.Io.Writer.Allocating = .init(allocator);
    defer aw.deinit();

    const values = [_]f32{ 1.0, 2.0, 3.0, 4.0, 5.0 };
    var result = try writeColumnChunkWithEncoding(
        f32,
        allocator,
        &aw.writer,
        &.{"float_col"},
        &values,
        false,
        4,
        .uncompressed,
        .byte_stream_split,
        true, // write_page_checksum
    );
    defer result.deinit(allocator);

    // Verify metadata
    try std.testing.expectEqual(format.PhysicalType.float, result.metadata.type_);
    try std.testing.expectEqual(@as(i64, 5), result.metadata.num_values);
    try std.testing.expectEqual(format.Encoding.byte_stream_split, result.metadata.encodings[1]);
}

test "write nullable byte array column tracks null_count in statistics" {
    const allocator = std.testing.allocator;

    var aw: std.Io.Writer.Allocating = .init(allocator);
    defer aw.deinit();

    const values = [_]Optional([]const u8){
        .{ .value = "hello" },
        .null_value,
        .{ .value = "world" },
        .null_value,
        .null_value,
    };
    var result = try writeColumnChunkByteArrayOptionalWithPathArray(
        allocator,
        &aw.writer,
        &.{"nullable_col"},
        &values,
        true,
        4,
        .uncompressed,
        false,
    );
    defer result.deinit(allocator);

    try std.testing.expect(result.metadata.statistics != null);
    const stats = result.metadata.statistics.?;
    try std.testing.expect(stats.null_count != null);
    try std.testing.expectEqual(@as(i64, 3), stats.null_count.?);
}

test "compressed column tracks uncompressed vs compressed sizes separately" {
    if (!build_options.supports_snappy) return;
    const allocator = std.testing.allocator;

    var aw: std.Io.Writer.Allocating = .init(allocator);
    defer aw.deinit();

    const values = [_]Optional(i32){
        .{ .value = 1 },
        .{ .value = 2 },
        .{ .value = 3 },
        .{ .value = 4 },
        .{ .value = 5 },
        .{ .value = 6 },
        .{ .value = 7 },
        .{ .value = 8 },
    };
    var result = try writeColumnChunkOptionalWithPathArray(
        i32,
        allocator,
        &aw.writer,
        &.{"compressed_col"},
        &values,
        false,
        4,
        .snappy,
        false,
    );
    defer result.deinit(allocator);

    try std.testing.expect(result.metadata.total_uncompressed_size > 0);
    try std.testing.expect(result.metadata.total_compressed_size > 0);
    // With compression, uncompressed and compressed sizes should differ
    // (for small data snappy may expand, but the sizes must still be tracked independently)
    try std.testing.expect(result.metadata.total_uncompressed_size != result.metadata.total_compressed_size);
}


test "byte array pages end once their values reach max_page_size bytes in PLAIN form" {
    const allocator = std.testing.allocator;

    // 40 values of 6 bytes (10 in PLAIN form), then 10 of 96 bytes (100), then nulls (0).
    var long_value: [96]u8 = undefined;
    @memset(&long_value, 'x');
    var values: [60]Optional([]const u8) = undefined;
    for (values[0..40]) |*v| v.* = .{ .value = "abcdef" };
    for (values[40..50]) |*v| v.* = .{ .value = &long_value };
    for (values[50..60]) |*v| v.* = .null_value;

    var aw: std.Io.Writer.Allocating = .init(allocator);
    defer aw.deinit();
    var pib = page_index_writer.PageIndexBuilder.init(allocator, .byte_array, true);
    defer pib.deinit();
    var result = try writeColumnChunkByteArrayOptionalWithEncoding(allocator, &aw.writer, &.{"s"}, &values, true, 4, .uncompressed, .plain, 200, false, &pib);
    defer result.deinit(allocator);

    // A page takes values until they reach 200 bytes: 20 short values twice, 2 long values
    // five times, then the nulls, which add no bytes.
    const want = [_]i64{ 20, 20, 2, 2, 2, 2, 2, 10 };
    try std.testing.expectEqual(want.len, pib.entries.items.len);
    var first_row: i64 = 0;
    for (pib.entries.items, want) |entry, rows| {
        try std.testing.expectEqual(first_row, entry.first_row_index);
        try std.testing.expectEqual(rows, entry.num_rows);
        first_row += rows;
    }
}
