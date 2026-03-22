//! List Column Writing
//!
//! Functions for writing list (repeated) column chunks to Parquet files.
//! Supports simple lists, nested lists, and dictionary-encoded lists.

const std = @import("std");
const safe = @import("safe.zig");
const format = @import("format.zig");
const thrift = @import("thrift/mod.zig");
const page_writer = @import("page_writer.zig");
const compress = @import("compress/mod.zig");
const rle_encoder = @import("encoding/rle_encoder.zig");
const statistics = @import("statistics.zig");

// Import shared types from column_writer
const column_writer = @import("column_writer.zig");
pub const ColumnWriteError = column_writer.ColumnWriteError;
pub const ColumnChunkResult = column_writer.ColumnChunkResult;
const computePageCrc = column_writer.computePageCrc;

/// Free statistics memory
fn freeStatistics(allocator: std.mem.Allocator, stats: format.Statistics) void {
    if (stats.min) |m| allocator.free(m);
    if (stats.max) |m| allocator.free(m);
    if (stats.min_value) |m| allocator.free(m);
    if (stats.max_value) |m| allocator.free(m);
}

/// Map a Zig type to Parquet physical type (comptime)
pub fn typeToPhysicalType(comptime T: type) format.PhysicalType {
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

/// Generic function to write a column chunk for a list of values.
/// Replaces writeColumnChunkListI32, writeColumnChunkListI64, etc.
pub fn writeColumnChunkList(
    comptime T: type,
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    column_name: []const u8,
    values: []const T,
    def_levels: []const u32,
    rep_levels: []const u32,
    max_def_level: u8,
    max_rep_level: u8,
    start_offset: i64,
    codec: format.CompressionCodec,
) ColumnWriteError!ColumnChunkResult {
    // Compute statistics on values (these are the actual non-null values)
    var stats_builder = statistics.StatisticsBuilder(T){};
    stats_builder.update(values);
    // Count nulls from def_levels (positions where def_level < max_def_level)
    var null_count: i64 = 0;
    for (def_levels) |dl| {
        if (dl < max_def_level) null_count += 1;
    }
    stats_builder.addNulls(null_count);
    const stats = stats_builder.build(allocator) catch return error.OutOfMemory;
    errdefer if (stats) |s| freeStatistics(allocator, s);

    var page_result = page_writer.writeDataPageWithLevels(
        allocator,
        T,
        values,
        def_levels,
        rep_levels,
        max_def_level,
        max_rep_level,
    ) catch |e| switch (e) {
        error.OutOfMemory => return error.OutOfMemory,
        error.InvalidFixedLength => return error.InvalidFixedLength,
        error.IntegerOverflow => return error.IntegerOverflow,
        error.ValueTooLarge => return error.ValueTooLarge,
        error.UnsupportedEncoding => return error.UnsupportedEncoding,
    };
    defer page_result.deinit(allocator);

    var result = writeColumnChunkWithDataList(
        allocator,
        output,
        column_name,
        comptime typeToPhysicalType(T),
        page_result.data,
        page_result.num_values,
        start_offset,
        codec,
    ) catch |e| return e;
    result.metadata.statistics = stats;
    return result;
}

/// Write a column chunk for a list of fixed-length byte array values (e.g., UUID)
pub fn writeColumnChunkListFixedByteArray(
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    column_name: []const u8,
    values: []const []const u8,
    fixed_len: usize,
    def_levels: []const u32,
    rep_levels: []const u32,
    max_def_level: u8,
    max_rep_level: u8,
    start_offset: i64,
    codec: format.CompressionCodec,
) ColumnWriteError!ColumnChunkResult {
    // Compute statistics on byte array values
    var stats_builder = statistics.ByteArrayStatisticsBuilder.init(allocator);
    stats_builder.update(values) catch return error.OutOfMemory;
    // Count nulls from def_levels
    var null_count: i64 = 0;
    for (def_levels) |dl| {
        if (dl < max_def_level) null_count += 1;
    }
    stats_builder.addNulls(null_count);
    const stats = stats_builder.build();
    // Note: stats_builder.deinit() is NOT called here because build() transfers ownership

    var page_result = page_writer.writeDataPageWithLevelsFixedByteArray(
        allocator,
        values,
        fixed_len,
        def_levels,
        rep_levels,
        max_def_level,
        max_rep_level,
    ) catch |e| switch (e) {
        error.OutOfMemory => return error.OutOfMemory,
        error.InvalidFixedLength => return error.InvalidFixedLength,
        error.IntegerOverflow => return error.IntegerOverflow,
        error.ValueTooLarge => return error.ValueTooLarge,
        error.UnsupportedEncoding => return error.UnsupportedEncoding,
    };
    defer page_result.deinit(allocator);

    var result = writeColumnChunkWithDataList(
        allocator,
        output,
        column_name,
        .fixed_len_byte_array,
        page_result.data,
        page_result.num_values,
        start_offset,
        codec,
    ) catch |e| return e;
    result.metadata.statistics = stats;
    return result;
}

/// Generic list column chunk with path array.
/// Appends "list" and "element" to the base path.
pub fn writeColumnChunkListWithPathArray(
    comptime T: type,
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    base_path: []const []const u8,
    values: []const T,
    def_levels: []const u32,
    rep_levels: []const u32,
    max_def_level: u8,
    max_rep_level: u8,
    start_offset: i64,
    codec: format.CompressionCodec,
) ColumnWriteError!ColumnChunkResult {
    return writeColumnChunkListWithPathArrayMultiPage(T, allocator, output, base_path, values, def_levels, rep_levels, max_def_level, max_rep_level, start_offset, codec, null);
}

/// Generic list column chunk with path array and optional multi-page support.
/// Appends "list" and "element" to the base path.
pub fn writeColumnChunkListWithPathArrayMultiPage(
    comptime T: type,
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    base_path: []const []const u8,
    values: []const T,
    def_levels: []const u32,
    rep_levels: []const u32,
    max_def_level: u8,
    max_rep_level: u8,
    start_offset: i64,
    codec: format.CompressionCodec,
    max_page_size: ?usize,
) ColumnWriteError!ColumnChunkResult {
    // Compute statistics on values
    var stats_builder = statistics.StatisticsBuilder(T){};
    stats_builder.update(values);
    // Count nulls from def_levels
    var null_count: i64 = 0;
    for (def_levels) |dl| {
        if (dl < max_def_level) null_count += 1;
    }
    stats_builder.addNulls(null_count);
    const stats = stats_builder.build(allocator) catch return error.OutOfMemory;
    errdefer if (stats) |s| freeStatistics(allocator, s);

    // Calculate slots per page based on max_page_size
    // For lists: rep_levels + def_levels (each ~1 byte) + value bytes
    const bytes_per_slot: usize = 2 + @sizeOf(T); // Conservative: 1 rep + 1 def + value
    const slots_per_page: usize = if (max_page_size) |max_size| blk: {
        const usable_size = if (max_size > 20) max_size - 20 else max_size;
        const spp = usable_size / bytes_per_slot;
        break :blk if (spp > 0) spp else 1;
    } else def_levels.len;

    // If single page is enough, use the simple path
    if (slots_per_page >= def_levels.len) {
        var page_result = page_writer.writeDataPageWithLevels(
            allocator,
            T,
            values,
            def_levels,
            rep_levels,
            max_def_level,
            max_rep_level,
        ) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
            error.InvalidFixedLength => return error.InvalidFixedLength,
            error.IntegerOverflow => return error.IntegerOverflow,
            error.ValueTooLarge => return error.ValueTooLarge,
            error.UnsupportedEncoding => return error.UnsupportedEncoding,
        };
        defer page_result.deinit(allocator);

        var result = writeColumnChunkWithDataListPath(
            allocator,
            output,
            base_path,
            comptime typeToPhysicalType(T),
            page_result.data,
            page_result.num_values,
            start_offset,
            codec,
        ) catch |e| return e;
        result.metadata.statistics = stats;
        return result;
    }

    // Multi-page path: iterate over level slots
    var total_bytes_written: usize = 0;
    var slot_offset: usize = 0;
    var value_offset: usize = 0;

    // Cast max_def_level to u32 for comparison with def_levels
    const max_def_u32: u32 = max_def_level;

    while (slot_offset < def_levels.len) {
        const page_end = @min(slot_offset + slots_per_page, def_levels.len);
        const page_def_levels = def_levels[slot_offset..page_end];
        const page_rep_levels = rep_levels[slot_offset..page_end];

        // Count values in this page (slots where def_level == max_def_level)
        var page_value_count: usize = 0;
        for (page_def_levels) |dl| {
            if (dl == max_def_u32) page_value_count += 1;
        }

        // Ensure we don't exceed available values (handles empty list edge case)
        const available_values = values.len - value_offset;
        const actual_value_count = @min(page_value_count, available_values);
        const page_values = values[value_offset .. value_offset + actual_value_count];
        value_offset += actual_value_count;

        var page_result = page_writer.writeDataPageWithLevels(
            allocator,
            T,
            page_values,
            page_def_levels,
            page_rep_levels,
            max_def_level,
            max_rep_level,
        ) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
            error.InvalidFixedLength => return error.InvalidFixedLength,
            error.IntegerOverflow => return error.IntegerOverflow,
            error.ValueTooLarge => return error.ValueTooLarge,
            error.UnsupportedEncoding => return error.UnsupportedEncoding,
        };
        defer page_result.deinit(allocator);

        const compressed_data: []const u8 = if (codec == .uncompressed)
            page_result.data
        else blk: {
            break :blk compress.compress(allocator, page_result.data, codec) catch |err| switch (err) {
                error.UnsupportedCompression => return error.UnsupportedCompression,
                error.CompressionError => return error.CompressionError,
                error.OutOfMemory => return error.OutOfMemory,
            };
        };
        defer if (codec != .uncompressed) allocator.free(compressed_data);

        const page_header = format.PageHeader{
            .type_ = .data_page,
            .uncompressed_page_size = try safe.castTo(i32, page_result.data.len),
            .compressed_page_size = try safe.castTo(i32, compressed_data.len),
            .crc = computePageCrc(compressed_data),
            .data_page_header = .{
                .num_values = try safe.castTo(i32, page_def_levels.len),
                .encoding = .plain,
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
        total_bytes_written = std.math.add(usize, total_bytes_written, page_bytes) catch return error.IntegerOverflow;
        slot_offset = page_end;
    }

    // Build path with "list" and "element" appended
    const full_path = allocator.alloc([]const u8, base_path.len + 2) catch return error.OutOfMemory;
    for (base_path, 0..) |segment, i| {
        full_path[i] = allocator.dupe(u8, segment) catch return error.OutOfMemory;
    }
    full_path[base_path.len] = allocator.dupe(u8, "list") catch return error.OutOfMemory;
    full_path[base_path.len + 1] = allocator.dupe(u8, "element") catch return error.OutOfMemory;

    const encodings = allocator.alloc(format.Encoding, 2) catch return error.OutOfMemory;
    encodings[0] = .rle;
    encodings[1] = .plain;

    return .{
        .metadata = .{
            .type_ = comptime typeToPhysicalType(T),
            .encodings = encodings,
            .path_in_schema = full_path,
            .codec = codec,
            .num_values = try safe.castTo(i64, def_levels.len),
            .total_uncompressed_size = try safe.castTo(i64, total_bytes_written),
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

/// Dictionary-encoded list column chunk with path array.
/// Uses RLE_DICTIONARY encoding for integer list elements.
/// Appends "list" and "element" to the base path.
pub fn writeColumnChunkListDictWithPathArray(
    comptime T: type,
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    base_path: []const []const u8,
    values: []const T,
    def_levels: []const u32,
    rep_levels: []const u32,
    max_def_level: u8,
    max_rep_level: u8,
    start_offset: i64,
    codec: format.CompressionCodec,
    dictionary_size_limit: ?usize,
    dictionary_cardinality_threshold: ?f32,
    max_page_size: ?usize,
) ColumnWriteError!ColumnChunkResult {
    if (values.len == 0) {
        // Fall back to plain encoding for empty columns (includes statistics)
        return writeColumnChunkListWithPathArray(T, allocator, output, base_path, values, def_levels, rep_levels, max_def_level, max_rep_level, start_offset, codec);
    }

    // Compute statistics on original values (not dictionary indices)
    var stats_builder = statistics.StatisticsBuilder(T){};
    stats_builder.update(values);
    // Count nulls from def_levels
    var null_count: i64 = 0;
    for (def_levels) |dl| {
        if (dl < max_def_level) null_count += 1;
    }
    stats_builder.addNulls(null_count);
    const stats = stats_builder.build(allocator) catch return error.OutOfMemory;
    errdefer if (stats) |s| freeStatistics(allocator, s);

    // Step 1: Build dictionary (unique values -> indices)
    var unique_map = std.AutoHashMap(T, u32).init(allocator);
    defer unique_map.deinit();
    var dict_values: std.ArrayListUnmanaged(T) = .empty;
    defer dict_values.deinit(allocator);

    var indices = allocator.alloc(u32, values.len) catch return error.OutOfMemory;
    defer allocator.free(indices);

    for (values, 0..) |v, i| {
        if (unique_map.get(v)) |idx| {
            indices[i] = idx;
        } else {
            const new_idx: u32 = try safe.castTo(u32, dict_values.items.len);
            unique_map.put(v, new_idx) catch return error.OutOfMemory;
            dict_values.append(allocator, v) catch return error.OutOfMemory;
            indices[i] = new_idx;
        }

        // Check cardinality threshold early abort
        if (dictionary_cardinality_threshold) |threshold| {
            // Only evaluate after seeing enough values to get a meaningful sample
            if (i >= 1024) {
                const ratio = @as(f32, @floatFromInt(dict_values.items.len)) / @as(f32, @floatFromInt(i + 1));
                if (ratio > threshold) {
                    if (stats) |s| freeStatistics(allocator, s);
                    return writeColumnChunkListWithPathArray(T, allocator, output, base_path, values, def_levels, rep_levels, max_def_level, max_rep_level, start_offset, codec);
                }
            }
        }
    }

    const dict_size = dict_values.items.len;

    // Check dictionary size limit - fall back to PLAIN if exceeded
    const dict_bytes = dict_size * @sizeOf(T);
    if (dictionary_size_limit) |limit| {
        if (dict_bytes > limit) {
            // Dictionary too large, fall back to plain encoding (which will compute its own statistics)
            // Free our computed statistics since the fallback will create its own
            if (stats) |s| freeStatistics(allocator, s);
            return writeColumnChunkListWithPathArray(T, allocator, output, base_path, values, def_levels, rep_levels, max_def_level, max_rep_level, start_offset, codec);
        }
    }

    // Step 2: Write dictionary page (PLAIN encoded)
    const bytes_per_value = @sizeOf(T);
    const dict_data = allocator.alloc(u8, dict_size * bytes_per_value) catch return error.OutOfMemory;
    defer allocator.free(dict_data);

    for (dict_values.items, 0..) |v, i| {
        if (T == i32) {
            std.mem.writeInt(i32, dict_data[i * 4 ..][0..4], v, .little);
        } else if (T == i64) {
            std.mem.writeInt(i64, dict_data[i * 8 ..][0..8], v, .little);
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
        .crc = computePageCrc(dict_compressed),
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
    const dict_page_offset = start_offset;
    const data_page_offset = start_offset + try safe.castTo(i64, total_bytes_written);

    // Compute bit width for indices
    const bit_width: u5 = if (dict_size <= 1) 0 else try safe.castTo(u5, std.math.log2_int(usize, dict_size - 1) + 1);

    // Calculate slots per page based on max_page_size
    // For lists: rep_levels + def_levels (each ~1 byte) + indices (bit_width bits)
    const slots_per_page: usize = if (max_page_size) |max_size| blk: {
        const bytes_per_slot: usize = 4; // Conservative: 1 rep + 1 def + 2 index
        const usable_size = if (max_size > 20) max_size - 20 else max_size;
        const spp = usable_size / bytes_per_slot;
        break :blk if (spp > 0) spp else 1;
    } else def_levels.len;

    // Write data pages
    var slot_offset: usize = 0;
    var value_offset: usize = 0;

    while (slot_offset < def_levels.len) {
        const page_end = @min(slot_offset + slots_per_page, def_levels.len);
        const page_def_levels = def_levels[slot_offset..page_end];
        const page_rep_levels = rep_levels[slot_offset..page_end];

        // Count values in this page (slots where def_level == max_def_level)
        var page_value_count: usize = 0;
        for (page_def_levels) |dl| {
            if (dl == max_def_level) page_value_count += 1;
        }

        const page_indices = indices[value_offset .. value_offset + page_value_count];
        value_offset += page_value_count;

        // Encode repetition levels with length prefix
        var rep_level_data: ?[]u8 = null;
        if (max_rep_level > 0) {
            rep_level_data = rle_encoder.encodeLevelsWithLength(allocator, page_rep_levels, max_rep_level) catch return error.OutOfMemory;
        }
        defer if (rep_level_data) |d| allocator.free(d);

        // Encode definition levels with length prefix
        var def_level_data: ?[]u8 = null;
        if (max_def_level > 0) {
            def_level_data = rle_encoder.encodeLevelsWithLength(allocator, page_def_levels, max_def_level) catch return error.OutOfMemory;
        }
        defer if (def_level_data) |d| allocator.free(d);

        // Encode indices using RLE/bit-packed hybrid
        const rle_data = rle_encoder.encode(allocator, page_indices, bit_width) catch return error.OutOfMemory;
        defer allocator.free(rle_data);

        // Data page format: rep_levels + def_levels + bit_width (1 byte) + RLE indices
        const rep_size = if (rep_level_data) |d| d.len else 0;
        const def_size = if (def_level_data) |d| d.len else 0;
        const data_page_uncompressed = allocator.alloc(u8, rep_size + def_size + 1 + rle_data.len) catch return error.OutOfMemory;
        defer allocator.free(data_page_uncompressed);

        var write_pos: usize = 0;
        if (rep_level_data) |d| {
            @memcpy(data_page_uncompressed[write_pos..][0..d.len], d);
            write_pos += d.len;
        }
        if (def_level_data) |d| {
            @memcpy(data_page_uncompressed[write_pos..][0..d.len], d);
            write_pos += d.len;
        }
        data_page_uncompressed[write_pos] = bit_width;
        write_pos += 1;
        @memcpy(data_page_uncompressed[write_pos..], rle_data);

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
            .crc = computePageCrc(data_compressed),
            .data_page_header = .{
                .num_values = try safe.castTo(i32, page_def_levels.len),
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
        total_bytes_written = std.math.add(usize, total_bytes_written, page_bytes) catch return error.IntegerOverflow;
        slot_offset = page_end;
    }

    // Build path_in_schema: base_path + ["list", "element"]
    const path = allocator.alloc([]const u8, base_path.len + 2) catch return error.OutOfMemory;
    for (base_path, 0..) |segment, i| {
        path[i] = allocator.dupe(u8, segment) catch return error.OutOfMemory;
    }
    path[base_path.len] = allocator.dupe(u8, "list") catch return error.OutOfMemory;
    path[base_path.len + 1] = allocator.dupe(u8, "element") catch return error.OutOfMemory;

    const encodings = allocator.alloc(format.Encoding, 3) catch return error.OutOfMemory;
    encodings[0] = .plain; // Dictionary page
    encodings[1] = .rle; // Definition/repetition levels
    encodings[2] = .rle_dictionary; // Data page

    return .{
        .metadata = .{
            .type_ = comptime typeToPhysicalType(T),
            .encodings = encodings,
            .path_in_schema = path,
            .codec = codec,
            .num_values = try safe.castTo(i64, def_levels.len),
            .total_uncompressed_size = try safe.castTo(i64, total_bytes_written),
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

/// Fixed byte array list column chunk with path array.
pub fn writeColumnChunkListFixedByteArrayWithPathArray(
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    base_path: []const []const u8,
    values: []const []const u8,
    fixed_len: usize,
    def_levels: []const u32,
    rep_levels: []const u32,
    max_def_level: u8,
    max_rep_level: u8,
    start_offset: i64,
    codec: format.CompressionCodec,
) ColumnWriteError!ColumnChunkResult {
    return writeColumnChunkListFixedByteArrayWithPathArrayMultiPage(allocator, output, base_path, values, fixed_len, def_levels, rep_levels, max_def_level, max_rep_level, start_offset, codec, null, .plain);
}

/// Fixed byte array list column chunk with path array and encoding.
pub fn writeColumnChunkListFixedByteArrayWithPathArrayAndEncoding(
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    base_path: []const []const u8,
    values: []const []const u8,
    fixed_len: usize,
    def_levels: []const u32,
    rep_levels: []const u32,
    max_def_level: u8,
    max_rep_level: u8,
    start_offset: i64,
    codec: format.CompressionCodec,
    value_encoding: format.Encoding,
) ColumnWriteError!ColumnChunkResult {
    return writeColumnChunkListFixedByteArrayWithPathArrayMultiPage(allocator, output, base_path, values, fixed_len, def_levels, rep_levels, max_def_level, max_rep_level, start_offset, codec, null, value_encoding);
}

/// Fixed byte array list with optional multi-page support.
pub fn writeColumnChunkListFixedByteArrayWithPathArrayMultiPage(
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    base_path: []const []const u8,
    values: []const []const u8,
    fixed_len: usize,
    def_levels: []const u32,
    rep_levels: []const u32,
    max_def_level: u8,
    max_rep_level: u8,
    start_offset: i64,
    codec: format.CompressionCodec,
    max_page_size: ?usize,
    value_encoding: format.Encoding,
) ColumnWriteError!ColumnChunkResult {
    // Compute statistics on byte array values
    var stats_builder = statistics.ByteArrayStatisticsBuilder.init(allocator);
    stats_builder.update(values) catch return error.OutOfMemory;
    // Count nulls from def_levels
    var null_count: i64 = 0;
    for (def_levels) |dl| {
        if (dl < max_def_level) null_count += 1;
    }
    stats_builder.addNulls(null_count);
    const stats = stats_builder.build();
    errdefer if (stats) |s| freeStatistics(allocator, s);

    // Calculate slots per page
    const bytes_per_slot: usize = 2 + fixed_len; // rep + def levels + fixed value
    const slots_per_page: usize = if (max_page_size) |max_size| blk: {
        const usable_size = if (max_size > 20) max_size - 20 else max_size;
        const spp = usable_size / bytes_per_slot;
        break :blk if (spp > 0) spp else 1;
    } else def_levels.len;

    // If single page is enough, use the simple path
    if (slots_per_page >= def_levels.len) {
        var page_result = page_writer.writeDataPageWithLevelsFixedByteArrayWithEncoding(
            allocator,
            values,
            fixed_len,
            def_levels,
            rep_levels,
            max_def_level,
            max_rep_level,
            value_encoding,
        ) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
            error.InvalidFixedLength => return error.InvalidFixedLength,
            error.IntegerOverflow => return error.IntegerOverflow,
            error.ValueTooLarge => return error.ValueTooLarge,
            error.UnsupportedEncoding => return error.UnsupportedEncoding,
        };
        defer page_result.deinit(allocator);

        var result = writeColumnChunkWithDataListPathAndEncoding(
            allocator,
            output,
            base_path,
            .fixed_len_byte_array,
            page_result.data,
            page_result.num_values,
            start_offset,
            codec,
            value_encoding,
        ) catch |e| return e;
        result.metadata.statistics = stats;
        return result;
    }

    // Multi-page path
    var total_bytes_written: usize = 0;
    var slot_offset: usize = 0;
    var value_offset: usize = 0;

    while (slot_offset < def_levels.len) {
        const page_end = @min(slot_offset + slots_per_page, def_levels.len);
        const page_def_levels = def_levels[slot_offset..page_end];
        const page_rep_levels = rep_levels[slot_offset..page_end];

        var page_value_count: usize = 0;
        for (page_def_levels) |dl| {
            if (dl == max_def_level) page_value_count += 1;
        }

        const page_values = values[value_offset .. value_offset + page_value_count];
        value_offset += page_value_count;

        var page_result = page_writer.writeDataPageWithLevelsFixedByteArrayWithEncoding(
            allocator,
            page_values,
            fixed_len,
            page_def_levels,
            page_rep_levels,
            max_def_level,
            max_rep_level,
            value_encoding,
        ) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
            error.InvalidFixedLength => return error.InvalidFixedLength,
            error.IntegerOverflow => return error.IntegerOverflow,
            error.ValueTooLarge => return error.ValueTooLarge,
            error.UnsupportedEncoding => return error.UnsupportedEncoding,
        };
        defer page_result.deinit(allocator);

        const compressed_data: []const u8 = if (codec == .uncompressed)
            page_result.data
        else blk: {
            break :blk compress.compress(allocator, page_result.data, codec) catch |err| switch (err) {
                error.UnsupportedCompression => return error.UnsupportedCompression,
                error.CompressionError => return error.CompressionError,
                error.OutOfMemory => return error.OutOfMemory,
            };
        };
        defer if (codec != .uncompressed) allocator.free(compressed_data);

        const page_header = format.PageHeader{
            .type_ = .data_page,
            .uncompressed_page_size = try safe.castTo(i32, page_result.data.len),
            .compressed_page_size = try safe.castTo(i32, compressed_data.len),
            .crc = computePageCrc(compressed_data),
            .data_page_header = .{
                .num_values = try safe.castTo(i32, page_def_levels.len),
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
        total_bytes_written = std.math.add(usize, total_bytes_written, page_bytes) catch return error.IntegerOverflow;
        slot_offset = page_end;
    }

    // Build path
    const full_path = allocator.alloc([]const u8, base_path.len + 2) catch return error.OutOfMemory;
    for (base_path, 0..) |segment, i| {
        full_path[i] = allocator.dupe(u8, segment) catch return error.OutOfMemory;
    }
    full_path[base_path.len] = allocator.dupe(u8, "list") catch return error.OutOfMemory;
    full_path[base_path.len + 1] = allocator.dupe(u8, "element") catch return error.OutOfMemory;

    const encodings = allocator.alloc(format.Encoding, 2) catch return error.OutOfMemory;
    encodings[0] = .rle;
    encodings[1] = value_encoding;

    return .{
        .metadata = .{
            .type_ = .fixed_len_byte_array,
            .encodings = encodings,
            .path_in_schema = full_path,
            .codec = codec,
            .num_values = try safe.castTo(i64, def_levels.len),
            .total_uncompressed_size = try safe.castTo(i64, total_bytes_written),
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

/// Nested list column chunk with path array.
/// Appends ["list", "element", "list", "element"] to base_path for 5-level schema.
pub fn writeColumnChunkNestedListWithPathArray(
    comptime T: type,
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    base_path: []const []const u8,
    values: []const T,
    def_levels: []const u32,
    rep_levels: []const u32,
    max_def_level: u8,
    max_rep_level: u8,
    start_offset: i64,
    codec: format.CompressionCodec,
) ColumnWriteError!ColumnChunkResult {
    return writeColumnChunkNestedListWithPathArrayMultiPage(T, allocator, output, base_path, values, def_levels, rep_levels, max_def_level, max_rep_level, start_offset, codec, null);
}

/// Nested list column chunk with path array and optional multi-page support.
pub fn writeColumnChunkNestedListWithPathArrayMultiPage(
    comptime T: type,
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    base_path: []const []const u8,
    values: []const T,
    def_levels: []const u32,
    rep_levels: []const u32,
    max_def_level: u8,
    max_rep_level: u8,
    start_offset: i64,
    codec: format.CompressionCodec,
    max_page_size: ?usize,
) ColumnWriteError!ColumnChunkResult {
    // Compute statistics on values
    var stats_builder = statistics.StatisticsBuilder(T){};
    stats_builder.update(values);
    // Count nulls from def_levels
    var null_count: i64 = 0;
    for (def_levels) |dl| {
        if (dl < max_def_level) null_count += 1;
    }
    stats_builder.addNulls(null_count);
    const stats = stats_builder.build(allocator) catch return error.OutOfMemory;
    errdefer if (stats) |s| freeStatistics(allocator, s);

    // Calculate slots per page
    const bytes_per_slot: usize = 2 + @sizeOf(T);
    const slots_per_page: usize = if (max_page_size) |max_size| blk: {
        const usable_size = if (max_size > 20) max_size - 20 else max_size;
        const spp = usable_size / bytes_per_slot;
        break :blk if (spp > 0) spp else 1;
    } else def_levels.len;

    // If single page is enough, use the simple path
    if (slots_per_page >= def_levels.len) {
        var page_result = page_writer.writeDataPageWithLevels(
            allocator,
            T,
            values,
            def_levels,
            rep_levels,
            max_def_level,
            max_rep_level,
        ) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
            error.InvalidFixedLength => return error.InvalidFixedLength,
            error.IntegerOverflow => return error.IntegerOverflow,
            error.ValueTooLarge => return error.ValueTooLarge,
            error.UnsupportedEncoding => return error.UnsupportedEncoding,
        };
        defer page_result.deinit(allocator);

        var result = writeColumnChunkWithNestedListPath(
            allocator,
            output,
            base_path,
            comptime typeToPhysicalType(T),
            page_result.data,
            page_result.num_values,
            start_offset,
            codec,
        ) catch |e| return e;
        result.metadata.statistics = stats;
        return result;
    }

    // Multi-page path
    var total_bytes_written: usize = 0;
    var slot_offset: usize = 0;
    var value_offset: usize = 0;

    while (slot_offset < def_levels.len) {
        const page_end = @min(slot_offset + slots_per_page, def_levels.len);
        const page_def_levels = def_levels[slot_offset..page_end];
        const page_rep_levels = rep_levels[slot_offset..page_end];

        var page_value_count: usize = 0;
        for (page_def_levels) |dl| {
            if (dl == max_def_level) page_value_count += 1;
        }

        const page_values = values[value_offset .. value_offset + page_value_count];
        value_offset += page_value_count;

        var page_result = page_writer.writeDataPageWithLevels(
            allocator,
            T,
            page_values,
            page_def_levels,
            page_rep_levels,
            max_def_level,
            max_rep_level,
        ) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
            error.InvalidFixedLength => return error.InvalidFixedLength,
            error.IntegerOverflow => return error.IntegerOverflow,
            error.ValueTooLarge => return error.ValueTooLarge,
            error.UnsupportedEncoding => return error.UnsupportedEncoding,
        };
        defer page_result.deinit(allocator);

        const compressed_data: []const u8 = if (codec == .uncompressed)
            page_result.data
        else blk: {
            break :blk compress.compress(allocator, page_result.data, codec) catch |err| switch (err) {
                error.UnsupportedCompression => return error.UnsupportedCompression,
                error.CompressionError => return error.CompressionError,
                error.OutOfMemory => return error.OutOfMemory,
            };
        };
        defer if (codec != .uncompressed) allocator.free(compressed_data);

        const page_header = format.PageHeader{
            .type_ = .data_page,
            .uncompressed_page_size = try safe.castTo(i32, page_result.data.len),
            .compressed_page_size = try safe.castTo(i32, compressed_data.len),
            .crc = computePageCrc(compressed_data),
            .data_page_header = .{
                .num_values = try safe.castTo(i32, page_def_levels.len),
                .encoding = .plain,
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
        total_bytes_written = std.math.add(usize, total_bytes_written, page_bytes) catch return error.IntegerOverflow;
        slot_offset = page_end;
    }

    // Build path for nested list: base_path + ["list", "element", "list", "element"]
    const full_path = allocator.alloc([]const u8, base_path.len + 4) catch return error.OutOfMemory;
    for (base_path, 0..) |segment, i| {
        full_path[i] = allocator.dupe(u8, segment) catch return error.OutOfMemory;
    }
    full_path[base_path.len] = allocator.dupe(u8, "list") catch return error.OutOfMemory;
    full_path[base_path.len + 1] = allocator.dupe(u8, "element") catch return error.OutOfMemory;
    full_path[base_path.len + 2] = allocator.dupe(u8, "list") catch return error.OutOfMemory;
    full_path[base_path.len + 3] = allocator.dupe(u8, "element") catch return error.OutOfMemory;

    const encodings = allocator.alloc(format.Encoding, 2) catch return error.OutOfMemory;
    encodings[0] = .rle;
    encodings[1] = .plain;

    return .{
        .metadata = .{
            .type_ = comptime typeToPhysicalType(T),
            .encodings = encodings,
            .path_in_schema = full_path,
            .codec = codec,
            .num_values = try safe.castTo(i64, def_levels.len),
            .total_uncompressed_size = try safe.castTo(i64, total_bytes_written),
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

/// Write a FLBA nested list column chunk with path array.
/// Appends ["list", "element", "list", "element"] to base_path for 5-level schema.
/// Uses fixed-length byte array encoding with `.fixed_len_byte_array` physical type.
pub fn writeColumnChunkNestedListFixedByteArrayWithPathArray(
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    base_path: []const []const u8,
    values: []const []const u8,
    fixed_len: usize,
    def_levels: []const u32,
    rep_levels: []const u32,
    max_def_level: u8,
    max_rep_level: u8,
    start_offset: i64,
    codec: format.CompressionCodec,
) ColumnWriteError!ColumnChunkResult {
    var stats_builder = statistics.ByteArrayStatisticsBuilder.init(allocator);
    stats_builder.update(values) catch return error.OutOfMemory;
    var null_count: i64 = 0;
    for (def_levels) |dl| {
        if (dl < max_def_level) null_count += 1;
    }
    stats_builder.addNulls(null_count);
    const stats = stats_builder.build();
    errdefer if (stats) |s| freeStatistics(allocator, s);

    var page_result = page_writer.writeDataPageWithLevelsFixedByteArray(
        allocator,
        values,
        fixed_len,
        def_levels,
        rep_levels,
        max_def_level,
        max_rep_level,
    ) catch |e| switch (e) {
        error.OutOfMemory => return error.OutOfMemory,
        error.InvalidFixedLength => return error.InvalidFixedLength,
        error.IntegerOverflow => return error.IntegerOverflow,
        error.ValueTooLarge => return error.ValueTooLarge,
        error.UnsupportedEncoding => return error.UnsupportedEncoding,
    };
    defer page_result.deinit(allocator);

    var result = writeColumnChunkWithNestedListPath(
        allocator,
        output,
        base_path,
        .fixed_len_byte_array,
        page_result.data,
        page_result.num_values,
        start_offset,
        codec,
    ) catch |e| return e;
    result.metadata.statistics = stats;
    return result;
}

/// Write a column chunk for list with a base path (appends "list" and "element")
fn writeColumnChunkWithDataListPath(
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    base_path: []const []const u8,
    physical_type: format.PhysicalType,
    page_data: []const u8,
    num_values: usize,
    start_offset: i64,
    codec: format.CompressionCodec,
) ColumnWriteError!ColumnChunkResult {
    return writeColumnChunkWithDataListPathAndEncoding(allocator, output, base_path, physical_type, page_data, num_values, start_offset, codec, .plain);
}

fn writeColumnChunkWithDataListPathAndEncoding(
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    base_path: []const []const u8,
    physical_type: format.PhysicalType,
    page_data: []const u8,
    num_values: usize,
    start_offset: i64,
    codec: format.CompressionCodec,
    value_encoding: format.Encoding,
) ColumnWriteError!ColumnChunkResult {
    // Build path_in_schema: base_path + ["list", "element"]
    const path = allocator.alloc([]const u8, base_path.len + 2) catch return error.OutOfMemory;
    for (base_path, 0..) |segment, i| {
        path[i] = allocator.dupe(u8, segment) catch return error.OutOfMemory;
    }
    path[base_path.len] = allocator.dupe(u8, "list") catch return error.OutOfMemory;
    path[base_path.len + 1] = allocator.dupe(u8, "element") catch return error.OutOfMemory;

    return writeColumnChunkWithPathOwnedAndEncoding(allocator, output, path, physical_type, page_data, num_values, start_offset, codec, value_encoding);
}

fn writeColumnChunkWithNestedListPath(
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    base_path: []const []const u8,
    physical_type: format.PhysicalType,
    page_data: []const u8,
    num_values: usize,
    start_offset: i64,
    codec: format.CompressionCodec,
) ColumnWriteError!ColumnChunkResult {
    // Build path_in_schema for nested list: base_path + ["list", "element", "list", "element"]
    const path = allocator.alloc([]const u8, base_path.len + 4) catch return error.OutOfMemory;
    for (base_path, 0..) |segment, i| {
        path[i] = allocator.dupe(u8, segment) catch return error.OutOfMemory;
    }
    path[base_path.len] = allocator.dupe(u8, "list") catch return error.OutOfMemory;
    path[base_path.len + 1] = allocator.dupe(u8, "element") catch return error.OutOfMemory;
    path[base_path.len + 2] = allocator.dupe(u8, "list") catch return error.OutOfMemory;
    path[base_path.len + 3] = allocator.dupe(u8, "element") catch return error.OutOfMemory;

    // Use the existing writeColumnChunkWithPathOwned (transfer ownership of path)
    return writeColumnChunkWithPathOwned(allocator, output, path, physical_type, page_data, num_values, start_offset, codec);
}

/// Write a column chunk with levels and full path (no suffix appending).
/// Used for list-of-struct where the path already includes all segments.
pub fn writeColumnChunkWithLevelsAndFullPath(
    comptime T: type,
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    full_path: []const []const u8,
    values: []const T,
    def_levels: []const u32,
    rep_levels: []const u32,
    max_def_level: u8,
    max_rep_level: u8,
    start_offset: i64,
    codec: format.CompressionCodec,
) ColumnWriteError!ColumnChunkResult {
    // Compute statistics on values
    var stats_builder = statistics.StatisticsBuilder(T){};
    stats_builder.update(values);
    // Count nulls from def_levels
    var null_count: i64 = 0;
    for (def_levels) |dl| {
        if (dl < max_def_level) null_count += 1;
    }
    stats_builder.addNulls(null_count);
    const stats = stats_builder.build(allocator) catch return error.OutOfMemory;
    errdefer if (stats) |s| freeStatistics(allocator, s);

    var page_result = page_writer.writeDataPageWithLevels(
        allocator,
        T,
        values,
        def_levels,
        rep_levels,
        max_def_level,
        max_rep_level,
    ) catch |e| switch (e) {
        error.OutOfMemory => return error.OutOfMemory,
        error.InvalidFixedLength => return error.InvalidFixedLength,
        error.IntegerOverflow => return error.IntegerOverflow,
        error.ValueTooLarge => return error.ValueTooLarge,
        error.UnsupportedEncoding => return error.UnsupportedEncoding,
    };
    defer page_result.deinit(allocator);

    // Duplicate the path for ownership
    const path = allocator.alloc([]const u8, full_path.len) catch return error.OutOfMemory;
    for (full_path, 0..) |segment, i| {
        path[i] = allocator.dupe(u8, segment) catch return error.OutOfMemory;
    }

    var result = writeColumnChunkWithPathOwned(allocator, output, path, comptime typeToPhysicalType(T), page_result.data, page_result.num_values, start_offset, codec) catch |e| return e;
    result.metadata.statistics = stats;
    return result;
}

/// Write a FLBA column chunk with levels and full path (no suffix appending).
/// Used for list-of-struct where the leaf field is a fixed-length byte array (e.g. UUID).
pub fn writeColumnChunkFixedByteArrayWithLevelsAndFullPath(
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    full_path: []const []const u8,
    values: []const []const u8,
    fixed_len: usize,
    def_levels: []const u32,
    rep_levels: []const u32,
    max_def_level: u8,
    max_rep_level: u8,
    start_offset: i64,
    codec: format.CompressionCodec,
    value_encoding: format.Encoding,
) ColumnWriteError!ColumnChunkResult {
    var stats_builder = statistics.ByteArrayStatisticsBuilder.init(allocator);
    stats_builder.update(values) catch return error.OutOfMemory;
    var null_count: i64 = 0;
    for (def_levels) |dl| {
        if (dl < max_def_level) null_count += 1;
    }
    stats_builder.addNulls(null_count);
    const stats = stats_builder.build();
    errdefer if (stats) |s| freeStatistics(allocator, s);

    var page_result = page_writer.writeDataPageWithLevelsFixedByteArrayWithEncoding(
        allocator,
        values,
        fixed_len,
        def_levels,
        rep_levels,
        max_def_level,
        max_rep_level,
        value_encoding,
    ) catch |e| switch (e) {
        error.OutOfMemory => return error.OutOfMemory,
        error.InvalidFixedLength => return error.InvalidFixedLength,
        error.IntegerOverflow => return error.IntegerOverflow,
        error.ValueTooLarge => return error.ValueTooLarge,
        error.UnsupportedEncoding => return error.UnsupportedEncoding,
    };
    defer page_result.deinit(allocator);

    const path = allocator.alloc([]const u8, full_path.len) catch return error.OutOfMemory;
    for (full_path, 0..) |segment, i| {
        path[i] = allocator.dupe(u8, segment) catch return error.OutOfMemory;
    }

    var result = writeColumnChunkWithPathOwnedAndEncoding(allocator, output, path, .fixed_len_byte_array, page_result.data, page_result.num_values, start_offset, codec, value_encoding) catch |e| return e;
    result.metadata.statistics = stats;
    return result;
}

/// Write an INT96 list column chunk with base path.
/// Appends "list" and "element" to the base path (for simple list columns like `[]TimestampInt96`).
pub fn writeColumnChunkListInt96WithPathArray(
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    base_path: []const []const u8,
    values: []const i64,
    def_levels: []const u32,
    rep_levels: []const u32,
    max_def_level: u8,
    max_rep_level: u8,
    start_offset: i64,
    codec: format.CompressionCodec,
) ColumnWriteError!ColumnChunkResult {
    var page_result = page_writer.writeDataPageInt96WithLevels(
        allocator,
        values,
        def_levels,
        rep_levels,
        max_def_level,
        max_rep_level,
    ) catch |e| switch (e) {
        error.OutOfMemory => return error.OutOfMemory,
        error.InvalidFixedLength => return error.InvalidFixedLength,
        error.IntegerOverflow => return error.IntegerOverflow,
        error.ValueTooLarge => return error.ValueTooLarge,
        error.UnsupportedEncoding => return error.UnsupportedEncoding,
    };
    defer page_result.deinit(allocator);

    return writeColumnChunkWithDataListPath(allocator, output, base_path, .int96, page_result.data, page_result.num_values, start_offset, codec) catch |e| return e;
}

/// Write an INT96 column chunk with levels and full path (no suffix appending).
/// Used for list-of-struct contexts where the leaf field is a TimestampInt96.
pub fn writeColumnChunkInt96WithLevelsAndFullPath(
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    full_path: []const []const u8,
    values: []const i64,
    def_levels: []const u32,
    rep_levels: []const u32,
    max_def_level: u8,
    max_rep_level: u8,
    start_offset: i64,
    codec: format.CompressionCodec,
) ColumnWriteError!ColumnChunkResult {
    var page_result = page_writer.writeDataPageInt96WithLevels(
        allocator,
        values,
        def_levels,
        rep_levels,
        max_def_level,
        max_rep_level,
    ) catch |e| switch (e) {
        error.OutOfMemory => return error.OutOfMemory,
        error.InvalidFixedLength => return error.InvalidFixedLength,
        error.IntegerOverflow => return error.IntegerOverflow,
        error.ValueTooLarge => return error.ValueTooLarge,
        error.UnsupportedEncoding => return error.UnsupportedEncoding,
    };
    defer page_result.deinit(allocator);

    const path = allocator.alloc([]const u8, full_path.len) catch return error.OutOfMemory;
    for (full_path, 0..) |segment, i| {
        path[i] = allocator.dupe(u8, segment) catch return error.OutOfMemory;
    }

    return writeColumnChunkWithPathOwned(allocator, output, path, .int96, page_result.data, page_result.num_values, start_offset, codec) catch |e| return e;
}

/// Write a column chunk with owned path (caller has already allocated the path array)
fn writeColumnChunkWithPathOwned(
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    path: [][]const u8,
    physical_type: format.PhysicalType,
    page_data: []const u8,
    num_values: usize,
    start_offset: i64,
    codec: format.CompressionCodec,
) ColumnWriteError!ColumnChunkResult {
    return writeColumnChunkWithPathOwnedAndEncoding(allocator, output, path, physical_type, page_data, num_values, start_offset, codec, .plain);
}

fn writeColumnChunkWithPathOwnedAndEncoding(
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    path: [][]const u8,
    physical_type: format.PhysicalType,
    page_data: []const u8,
    num_values: usize,
    start_offset: i64,
    codec: format.CompressionCodec,
    value_encoding: format.Encoding,
) ColumnWriteError!ColumnChunkResult {
    // Compress page data if needed
    const compressed_data: []const u8 = if (codec == .uncompressed)
        page_data
    else blk: {
        break :blk compress.compress(allocator, page_data, codec) catch |err| switch (err) {
            error.UnsupportedCompression => return error.UnsupportedCompression,
            error.CompressionError => return error.CompressionError,
            error.OutOfMemory => return error.OutOfMemory,
        };
    };
    defer if (codec != .uncompressed) allocator.free(compressed_data);

    // Create page header
    const page_header = format.PageHeader{
        .type_ = .data_page,
        .uncompressed_page_size = try safe.castTo(i32, page_data.len),
        .compressed_page_size = try safe.castTo(i32, compressed_data.len),
        .crc = computePageCrc(compressed_data),
        .data_page_header = .{
            .num_values = try safe.castTo(i32, num_values),
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

    const total_compressed_bytes = header_bytes.len + compressed_data.len;
    const total_uncompressed_bytes = header_bytes.len + page_data.len;

    // Build encodings list
    const encodings = allocator.alloc(format.Encoding, 2) catch return error.OutOfMemory;
    encodings[0] = .rle;
    encodings[1] = value_encoding;

    return .{
        .metadata = .{
            .type_ = physical_type,
            .encodings = encodings,
            .path_in_schema = path, // Transfer ownership
            .codec = codec,
            .num_values = try safe.castTo(i32, num_values),
            .total_uncompressed_size = try safe.castTo(i64, total_uncompressed_bytes),
            .total_compressed_size = try safe.castTo(i64, total_compressed_bytes),
            .data_page_offset = start_offset,
            .index_page_offset = null,
            .dictionary_page_offset = null,
            .statistics = null,
        },
        .file_offset = start_offset,
        .total_bytes = total_compressed_bytes,
    };
}

/// Write a column chunk with pre-encoded page data for a list column
pub fn writeColumnChunkWithDataList(
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    column_name: []const u8,
    physical_type: format.PhysicalType,
    page_data: []const u8,
    num_values: usize,
    start_offset: i64,
    codec: format.CompressionCodec,
) ColumnWriteError!ColumnChunkResult {
    // Compress page data if needed
    const compressed_data: []const u8 = if (codec == .uncompressed)
        page_data
    else blk: {
        break :blk compress.compress(allocator, page_data, codec) catch |err| switch (err) {
            error.UnsupportedCompression => return error.UnsupportedCompression,
            error.CompressionError => return error.CompressionError,
            error.OutOfMemory => return error.OutOfMemory,
        };
    };
    defer if (codec != .uncompressed) allocator.free(compressed_data);

    // Create page header
    const page_header = format.PageHeader{
        .type_ = .data_page,
        .uncompressed_page_size = try safe.castTo(i32, page_data.len),
        .compressed_page_size = try safe.castTo(i32, compressed_data.len),
        .crc = computePageCrc(compressed_data),
        .data_page_header = .{
            .num_values = try safe.castTo(i32, num_values),
            .encoding = .plain,
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

    const total_compressed_bytes = header_bytes.len + compressed_data.len;
    const total_uncompressed_bytes = header_bytes.len + page_data.len;

    // Build path_in_schema for list: [column_name, "list", "element"]
    const path = allocator.alloc([]const u8, 3) catch return error.OutOfMemory;
    path[0] = allocator.dupe(u8, column_name) catch return error.OutOfMemory;
    path[1] = allocator.dupe(u8, "list") catch return error.OutOfMemory;
    path[2] = allocator.dupe(u8, "element") catch return error.OutOfMemory;

    // Build encodings list
    const encodings = allocator.alloc(format.Encoding, 2) catch return error.OutOfMemory;
    encodings[0] = .rle; // Levels
    encodings[1] = .plain; // Values

    return .{
        .metadata = .{
            .type_ = physical_type,
            .encodings = encodings,
            .path_in_schema = path,
            .codec = codec,
            .num_values = try safe.castTo(i32, num_values),
            .total_uncompressed_size = try safe.castTo(i64, total_uncompressed_bytes),
            .total_compressed_size = try safe.castTo(i64, total_compressed_bytes),
            .data_page_offset = start_offset,
            .dictionary_page_offset = null,
            .statistics = null,
        },
        .file_offset = start_offset,
        .total_bytes = total_compressed_bytes,
    };
}
