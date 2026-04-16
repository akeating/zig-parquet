//! Column command - per-column deep dive across all row groups
//!
//! Usage:
//!   pqi column <file>                     -- all columns summary
//!   pqi column <file> <col1> [col2...]    -- detailed view for named columns

const std = @import("std");
const parquet = @import("parquet");
const helpers = @import("../helpers.zig");
const cat = @import("cat.zig");

pub fn run(allocator: std.mem.Allocator, io: std.Io, file_path: []const u8, column_names: []const []const u8) !void {
    var stdout_buf: [8192]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writerStreaming(io, &stdout_buf);
    const stdout = &stdout_writer.interface;

    var stderr_buf: [4096]u8 = undefined;
    var stderr_writer = std.Io.File.stderr().writerStreaming(io, &stderr_buf);
    const stderr = &stderr_writer.interface;

    const file = std.Io.Dir.cwd().openFile(io, file_path, .{}) catch |err| {
        try stderr.print("Error: Cannot open file '{s}': {}\n", .{ file_path, err });
        try stderr.flush();
        std.process.exit(1);
    };
    defer file.close(io);

    var reader = parquet.openFileDynamic(allocator, file, io, .{}) catch |err| {
        try stderr.print("Error: Cannot read parquet file: {}\n", .{err});
        try stderr.flush();
        std.process.exit(1);
    };
    defer reader.deinit();

    const schema = reader.getSchema();
    const num_columns = helpers.countLeafColumns(schema);

    if (column_names.len == 0) {
        try printAllColumnsSummary(stdout, &reader, schema, num_columns, allocator);
    } else {
        for (column_names, 0..) |name, i| {
            const col_idx = helpers.findColumnIndex(schema, name) orelse {
                try stderr.print("Error: Column '{s}' not found\n", .{name});
                try stderr.flush();
                std.process.exit(1);
            };

            try printColumnDetail(stdout, &reader, schema, col_idx, allocator);

            if (i + 1 < column_names.len) {
                try stdout.writeAll("\n");
            }
        }
    }

    try stdout.flush();
}

fn printAllColumnsSummary(
    stdout: *std.Io.Writer,
    reader: *parquet.DynamicReader,
    schema: []const parquet.format.SchemaElement,
    num_columns: usize,
    allocator: std.mem.Allocator,
) !void {
    const col_names = cat.getTopLevelColumnNames(allocator, reader.getSchema()) catch &.{};
    defer allocator.free(col_names);

    var logical_type_buf: [64]u8 = undefined;

    for (0..num_columns) |col_idx| {
        const elem = helpers.getLeafElement(schema, col_idx) orelse continue;
        const col_name = if (col_idx < col_names.len) col_names[col_idx] else "?";

        try stdout.print("{s}: {s}", .{
            col_name,
            helpers.physicalTypeToString(elem.type_.?),
        });

        if (elem.logical_type) |lt| {
            const lt_str = lt.toDetailedString(&logical_type_buf);
            try stdout.print("/{s}", .{lt_str});
        }

        try stdout.print(" ({s})", .{helpers.repetitionToString(elem.repetition_type)});

        // Aggregate across row groups
        var total_values: i64 = 0;
        var total_nulls: i64 = 0;
        var total_compressed: i64 = 0;
        var total_uncompressed: i64 = 0;
        var has_nulls = false;

        for (reader.metadata.row_groups) |rg| {
            if (col_idx >= rg.columns.len) continue;
            if (rg.columns[col_idx].meta_data) |meta| {
                total_values += meta.num_values;
                total_compressed += meta.total_compressed_size;
                total_uncompressed += meta.total_uncompressed_size;
                if (meta.statistics) |stats| {
                    if (stats.null_count) |nc| {
                        total_nulls += nc;
                        has_nulls = true;
                    }
                }
            }
        }

        try stdout.print(", values: {}", .{total_values});
        if (has_nulls and total_nulls > 0) {
            try stdout.print(", nulls: {}", .{total_nulls});
        }
        try stdout.print(", compressed: {} bytes", .{total_compressed});
        if (total_uncompressed != total_compressed) {
            try stdout.print(", uncompressed: {} bytes", .{total_uncompressed});
        }
        try stdout.writeAll("\n");
    }
}

fn printColumnDetail(
    stdout: *std.Io.Writer,
    reader: *parquet.DynamicReader,
    schema: []const parquet.format.SchemaElement,
    col_idx: usize,
    allocator: std.mem.Allocator,
) !void {
    const col_names = cat.getTopLevelColumnNames(allocator, reader.getSchema()) catch &.{};
    defer allocator.free(col_names);

    const elem = helpers.getLeafElement(schema, col_idx) orelse return;
    const col_name = if (col_idx < col_names.len) col_names[col_idx] else "?";

    var logical_type_buf: [64]u8 = undefined;

    try stdout.print("Column: {s}\n", .{col_name});
    try stdout.print("  Physical type: {s}\n", .{helpers.physicalTypeToString(elem.type_.?)});
    if (elem.logical_type) |lt| {
        try stdout.print("  Logical type:  {s}\n", .{lt.toDetailedString(&logical_type_buf)});
    }
    try stdout.print("  Repetition:    {s}\n", .{helpers.repetitionToString(elem.repetition_type)});
    if (elem.type_ == .fixed_len_byte_array) {
        if (elem.type_length) |len| {
            try stdout.print("  Type length:   {}\n", .{len});
        }
    }

    // Per-row-group detail
    for (reader.metadata.row_groups, 0..) |rg, rg_idx| {
        if (col_idx >= rg.columns.len) continue;
        const meta = rg.columns[col_idx].meta_data orelse continue;

        try stdout.print("\n  Row Group {}:\n", .{rg_idx});

        // Encodings
        try stdout.writeAll("    Encodings:    ");
        for (meta.encodings, 0..) |enc, i| {
            if (i > 0) try stdout.writeAll(", ");
            try stdout.writeAll(helpers.encodingToString(enc));
        }
        try stdout.writeAll("\n");

        try stdout.print("    Codec:        {s}\n", .{helpers.codecToString(meta.codec)});
        try stdout.print("    Values:       {}\n", .{meta.num_values});
        try stdout.print("    Compressed:   {} bytes\n", .{meta.total_compressed_size});
        try stdout.print("    Uncompressed: {} bytes\n", .{meta.total_uncompressed_size});

        if (meta.dictionary_page_offset) |dpo| {
            if (dpo > 0) {
                try stdout.writeAll("    Dictionary:   yes\n");
            }
        }

        if (meta.statistics) |stats| {
            if (stats.null_count) |nc| {
                try stdout.print("    Null count:   {}\n", .{nc});
            }
            if (stats.distinct_count) |dc| {
                try stdout.print("    Distinct:     {}\n", .{dc});
            }
            const col_type: ?parquet.format.PhysicalType = meta.type_;
            if (stats.getMinBytes() != null) {
                try stdout.writeAll("    Min:          ");
                try helpers.printStatValue(stdout, stats.getMinBytes(), col_type);
                try stdout.writeAll("\n");
            }
            if (stats.getMaxBytes() != null) {
                try stdout.writeAll("    Max:          ");
                try helpers.printStatValue(stdout, stats.getMaxBytes(), col_type);
                try stdout.writeAll("\n");
            }
        }
    }
}
