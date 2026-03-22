//! Stats command - displays parquet file statistics

const std = @import("std");
const parquet = @import("parquet");
const helpers = @import("../helpers.zig");
const cat = @import("cat.zig");

pub fn run(allocator: std.mem.Allocator, file_path: []const u8) !void {
    var stdout_buf: [8192]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buf);
    const stdout = &stdout_writer.interface;

    var stderr_buf: [4096]u8 = undefined;
    var stderr_writer = std.fs.File.stderr().writer(&stderr_buf);
    const stderr = &stderr_writer.interface;

    // Open the file
    const file = std.fs.cwd().openFile(file_path, .{}) catch |err| {
        try stderr.print("Error: Cannot open file '{s}': {}\n", .{ file_path, err });
        try stderr.flush();
        std.process.exit(1);
    };
    defer file.close();

    // Get file size
    const file_stat = file.stat() catch |err| {
        try stderr.print("Error: Cannot stat file: {}\n", .{err});
        try stderr.flush();
        std.process.exit(1);
    };
    const file_size = file_stat.size;

    // Create reader
    var reader = parquet.openFileDynamic(allocator, file, .{}) catch |err| {
        try stderr.print("Error: Cannot read parquet file: {}\n", .{err});
        try stderr.flush();
        std.process.exit(1);
    };
    defer reader.deinit();

    const num_columns = helpers.countLeafColumns(reader.getSchema());

    // Determine compression codec(s) used
    var compression_set = std.AutoHashMap(parquet.format.CompressionCodec, void).init(allocator);
    defer compression_set.deinit();

    for (reader.metadata.row_groups) |rg| {
        for (rg.columns) |col| {
            if (col.meta_data) |meta| {
                compression_set.put(meta.codec, {}) catch {};
            }
        }
    }

    // Print stats
    try stdout.print("File: {s}\n", .{file_path});
    try helpers.formatFileSize(stdout, file_size);
    try stdout.print("Rows: {}\n", .{reader.metadata.num_rows});
    try stdout.print("Row Groups: {}\n", .{reader.getNumRowGroups()});
    try stdout.print("Columns: {}\n", .{num_columns});

    // Print compression
    try stdout.writeAll("Compression: ");
    var first = true;
    var iter = compression_set.keyIterator();
    while (iter.next()) |codec| {
        if (!first) try stdout.writeAll(", ");
        try stdout.writeAll(helpers.codecToString(codec.*));
        first = false;
    }
    if (first) try stdout.writeAll("(none)");
    try stdout.writeAll("\n");

    // Print created_by if available
    if (reader.metadata.created_by) |created_by| {
        try stdout.print("Created by: {s}\n", .{created_by});
    }

    // Print version
    try stdout.print("Format version: {}\n", .{reader.metadata.version});

    // Print key-value metadata if present
    if (reader.metadata.key_value_metadata) |kvs| {
        try stdout.writeAll("\nMetadata:\n");
        for (kvs) |kv| {
            try stdout.print("  {s}: {s}\n", .{ kv.key, kv.value orelse "(null)" });
        }
    }

    // Print row group details
    if (reader.getNumRowGroups() > 1) {
        try stdout.writeAll("\nRow Groups:\n");
        for (reader.metadata.row_groups, 0..) |rg, i| {
            try stdout.print("  [{:>2}] {} rows, {} bytes\n", .{
                i,
                rg.num_rows,
                rg.total_byte_size,
            });
        }
    }

    // Print column statistics (for first row group)
    try stdout.writeAll("\nColumn Statistics:\n");
    const col_names = cat.getTopLevelColumnNames(allocator, reader.getSchema()) catch {
        try stdout.writeAll("  (unable to get column names)\n");
        try stdout.flush();
        return;
    };
    defer allocator.free(col_names);

    for (0..num_columns) |col_idx| {
        const col_type: ?parquet.format.PhysicalType = if (reader.getLeafSchemaElement(col_idx)) |elem| elem.type_ else null;
        const stats = reader.getColumnStatistics(col_idx, 0);

        try stdout.print("  [{:>2}] {s} ({s}): ", .{
            col_idx,
            col_names[col_idx],
            if (col_type) |t| helpers.physicalTypeToString(t) else "?",
        });

        if (stats) |s| {
            try stdout.writeAll("min=");
            try helpers.printStatValue(stdout, s.getMinBytes(), col_type);

            try stdout.writeAll(", max=");
            try helpers.printStatValue(stdout, s.getMaxBytes(), col_type);

            if (s.null_count) |nc| {
                try stdout.print(", nulls={}", .{nc});
            }
            try stdout.writeAll("\n");
        } else {
            try stdout.writeAll("(no statistics)\n");
        }
    }

    try stdout.flush();
}
