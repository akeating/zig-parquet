//! Map Column Writing
//!
//! Functions for writing map (key-value) column chunks to Parquet files.

const std = @import("std");
const safe = @import("safe.zig");
const format = @import("format.zig");
const thrift = @import("thrift/mod.zig");
const page_writer = @import("page_writer.zig");
const compress = @import("compress/mod.zig");
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

/// Write a column chunk for map keys
pub fn writeColumnChunkMapKey(
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    map_name: []const u8,
    comptime T: type,
    values: []const T,
    def_levels: []const u32,
    rep_levels: []const u32,
    max_def_level: u8,
    max_rep_level: u8,
    start_offset: i64,
    codec: format.CompressionCodec,
) ColumnWriteError!ColumnChunkResult {
    return writeColumnChunkMapKeyMultiPage(allocator, output, map_name, T, values, def_levels, rep_levels, max_def_level, max_rep_level, start_offset, codec, null);
}

/// Write a column chunk for map keys with optional multi-page support
pub fn writeColumnChunkMapKeyMultiPage(
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    map_name: []const u8,
    comptime T: type,
    values: []const T,
    def_levels: []const u32,
    rep_levels: []const u32,
    max_def_level: u8,
    max_rep_level: u8,
    start_offset: i64,
    codec: format.CompressionCodec,
    max_page_size: ?usize,
) ColumnWriteError!ColumnChunkResult {
    // Compute statistics - handle byte array specially
    const stats = if (T == []const u8) blk: {
        var stats_builder = statistics.ByteArrayStatisticsBuilder.init(allocator);
        stats_builder.update(values) catch return error.OutOfMemory;
        var null_count: i64 = 0;
        for (def_levels) |dl| {
            if (dl < max_def_level) null_count += 1;
        }
        stats_builder.addNulls(null_count);
        break :blk stats_builder.build();
    } else blk: {
        var stats_builder = statistics.StatisticsBuilder(T){};
        stats_builder.update(values);
        var null_count: i64 = 0;
        for (def_levels) |dl| {
            if (dl < max_def_level) null_count += 1;
        }
        stats_builder.addNulls(null_count);
        break :blk stats_builder.build(allocator) catch return error.OutOfMemory;
    };
    errdefer if (stats) |s| freeStatistics(allocator, s);

    const physical_type: format.PhysicalType = if (T == i32)
        .int32
    else if (T == i64)
        .int64
    else if (T == f32)
        .float
    else if (T == f64)
        .double
    else if (T == bool)
        .boolean
    else if (T == []const u8)
        .byte_array
    else
        @compileError("Unsupported map key type");

    // Calculate slots per page
    const bytes_per_slot: usize = if (T == []const u8) 10 else if (T == bool) 2 + 1 else 2 + @sizeOf(T);
    const slots_per_page: usize = if (max_page_size) |max_size| blk: {
        const usable_size = if (max_size > 20) max_size - 20 else max_size;
        const spp = usable_size / bytes_per_slot;
        break :blk if (spp > 0) spp else 1;
    } else def_levels.len;

    // If single page is enough, use the simple path
    if (slots_per_page >= def_levels.len) {
        var page_result = if (T == []const u8)
            page_writer.writeDataPageWithLevelsByteArray(
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
            }
        else
            page_writer.writeDataPageWithLevels(
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
        };
        defer page_result.deinit(allocator);

        var result = writeColumnChunkWithDataMap(
            allocator,
            output,
            map_name,
            "key",
            physical_type,
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

        var page_result = if (T == []const u8)
            page_writer.writeDataPageWithLevelsByteArray(
                allocator,
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
            }
        else
            page_writer.writeDataPageWithLevels(
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

    // Build path
    const path = allocator.alloc([]const u8, 3) catch return error.OutOfMemory;
    path[0] = allocator.dupe(u8, map_name) catch return error.OutOfMemory;
    path[1] = allocator.dupe(u8, "key_value") catch return error.OutOfMemory;
    path[2] = allocator.dupe(u8, "key") catch return error.OutOfMemory;

    const encodings = allocator.alloc(format.Encoding, 2) catch return error.OutOfMemory;
    encodings[0] = .rle;
    encodings[1] = .plain;

    return .{
        .metadata = .{
            .type_ = physical_type,
            .encodings = encodings,
            .path_in_schema = path,
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

/// Write a column chunk for map values
pub fn writeColumnChunkMapValue(
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    map_name: []const u8,
    comptime T: type,
    values: []const T,
    def_levels: []const u32,
    rep_levels: []const u32,
    max_def_level: u8,
    max_rep_level: u8,
    start_offset: i64,
    codec: format.CompressionCodec,
) ColumnWriteError!ColumnChunkResult {
    return writeColumnChunkMapValueMultiPage(allocator, output, map_name, T, values, def_levels, rep_levels, max_def_level, max_rep_level, start_offset, codec, null);
}

/// Write a column chunk for map values with optional multi-page support
pub fn writeColumnChunkMapValueMultiPage(
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    map_name: []const u8,
    comptime T: type,
    values: []const T,
    def_levels: []const u32,
    rep_levels: []const u32,
    max_def_level: u8,
    max_rep_level: u8,
    start_offset: i64,
    codec: format.CompressionCodec,
    max_page_size: ?usize,
) ColumnWriteError!ColumnChunkResult {
    // Compute statistics - handle byte array specially
    const stats = if (T == []const u8) blk: {
        var stats_builder = statistics.ByteArrayStatisticsBuilder.init(allocator);
        stats_builder.update(values) catch return error.OutOfMemory;
        var null_count: i64 = 0;
        for (def_levels) |dl| {
            if (dl < max_def_level) null_count += 1;
        }
        stats_builder.addNulls(null_count);
        break :blk stats_builder.build();
    } else blk: {
        var stats_builder = statistics.StatisticsBuilder(T){};
        stats_builder.update(values);
        var null_count: i64 = 0;
        for (def_levels) |dl| {
            if (dl < max_def_level) null_count += 1;
        }
        stats_builder.addNulls(null_count);
        break :blk stats_builder.build(allocator) catch return error.OutOfMemory;
    };
    errdefer if (stats) |s| freeStatistics(allocator, s);

    const physical_type: format.PhysicalType = if (T == i32)
        .int32
    else if (T == i64)
        .int64
    else if (T == f32)
        .float
    else if (T == f64)
        .double
    else if (T == bool)
        .boolean
    else if (T == []const u8)
        .byte_array
    else
        @compileError("Unsupported map value type");

    // Calculate slots per page
    const bytes_per_slot: usize = if (T == []const u8) 10 else if (T == bool) 2 + 1 else 2 + @sizeOf(T);
    const slots_per_page: usize = if (max_page_size) |max_size| blk: {
        const usable_size = if (max_size > 20) max_size - 20 else max_size;
        const spp = usable_size / bytes_per_slot;
        break :blk if (spp > 0) spp else 1;
    } else def_levels.len;

    // If single page is enough, use the simple path
    if (slots_per_page >= def_levels.len) {
        var page_result = if (T == []const u8)
            page_writer.writeDataPageWithLevelsByteArray(
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
            }
        else
            page_writer.writeDataPageWithLevels(
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
        };
        defer page_result.deinit(allocator);

        var result = writeColumnChunkWithDataMap(
            allocator,
            output,
            map_name,
            "value",
            physical_type,
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

        var page_result = if (T == []const u8)
            page_writer.writeDataPageWithLevelsByteArray(
                allocator,
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
            }
        else
            page_writer.writeDataPageWithLevels(
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

    // Build path
    const path = allocator.alloc([]const u8, 3) catch return error.OutOfMemory;
    path[0] = allocator.dupe(u8, map_name) catch return error.OutOfMemory;
    path[1] = allocator.dupe(u8, "key_value") catch return error.OutOfMemory;
    path[2] = allocator.dupe(u8, "value") catch return error.OutOfMemory;

    const encodings = allocator.alloc(format.Encoding, 2) catch return error.OutOfMemory;
    encodings[0] = .rle;
    encodings[1] = .plain;

    return .{
        .metadata = .{
            .type_ = physical_type,
            .encodings = encodings,
            .path_in_schema = path,
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

/// Write a column chunk with map path (map_name/key_value/key or value)
fn writeColumnChunkWithDataMap(
    allocator: std.mem.Allocator,
    output: *std.Io.Writer,
    map_name: []const u8,
    field_name: []const u8, // "key" or "value"
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

    // Build path_in_schema for map: [map_name, "key_value", "key"/"value"]
    const path = allocator.alloc([]const u8, 3) catch return error.OutOfMemory;
    path[0] = allocator.dupe(u8, map_name) catch return error.OutOfMemory;
    path[1] = allocator.dupe(u8, "key_value") catch return error.OutOfMemory;
    path[2] = allocator.dupe(u8, field_name) catch return error.OutOfMemory;

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
