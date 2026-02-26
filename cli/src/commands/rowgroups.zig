//! Rowgroups command - per-row-group detail from metadata

const std = @import("std");
const parquet = @import("parquet");
const helpers = @import("../helpers.zig");

pub fn run(allocator: std.mem.Allocator, file_path: []const u8) !void {
    var stdout_buf: [8192]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buf);
    const stdout = &stdout_writer.interface;

    var stderr_buf: [4096]u8 = undefined;
    var stderr_writer = std.fs.File.stderr().writer(&stderr_buf);
    const stderr = &stderr_writer.interface;

    const file = std.fs.cwd().openFile(file_path, .{}) catch |err| {
        try stderr.print("Error: Cannot open file '{s}': {}\n", .{ file_path, err });
        try stderr.flush();
        std.process.exit(1);
    };
    defer file.close();

    var reader = parquet.openFile(allocator, file) catch |err| {
        try stderr.print("Error: Cannot read parquet file: {}\n", .{err});
        try stderr.flush();
        std.process.exit(1);
    };
    defer reader.deinit();

    const col_names = reader.getColumnNames(allocator) catch &.{};
    defer allocator.free(col_names);

    for (reader.metadata.row_groups, 0..) |rg, rg_idx| {
        try stdout.print("Row Group {}: {} rows\n", .{ rg_idx, rg.num_rows });

        // Sizes and compression ratio
        const compressed: u64 = if (rg.total_compressed_size) |cs|
            if (cs > 0) @intCast(cs) else @as(u64, @intCast(rg.total_byte_size))
        else
            @intCast(rg.total_byte_size);
        const uncompressed: u64 = @intCast(rg.total_byte_size);

        try stdout.writeAll("  Compressed: ");
        try helpers.printFileSize(stdout, compressed);
        try stdout.writeAll(", Uncompressed: ");
        try helpers.printFileSize(stdout, uncompressed);

        if (uncompressed > 0 and compressed > 0) {
            const ratio: f64 = @as(f64, @floatFromInt(uncompressed)) / @as(f64, @floatFromInt(compressed));
            try stdout.print(" (ratio: {d:.2}x)\n", .{ratio});
        } else {
            try stdout.writeAll("\n");
        }

        // Per-column detail
        try stdout.writeAll("  Columns:\n");
        for (rg.columns, 0..) |col, col_idx| {
            const meta = col.meta_data orelse continue;

            const col_name = if (col_idx < col_names.len) col_names[col_idx] else "?";
            try stdout.print("    [{:>2}] {s} ({s}, {s})\n", .{
                col_idx,
                col_name,
                helpers.physicalTypeToString(meta.type_),
                helpers.codecToString(meta.codec),
            });

            // Encodings
            try stdout.writeAll("         encodings: ");
            for (meta.encodings, 0..) |enc, i| {
                if (i > 0) try stdout.writeAll(", ");
                try stdout.writeAll(helpers.encodingToString(enc));
            }
            try stdout.writeAll("\n");

            // Sizes
            try stdout.print("         compressed: {} bytes, uncompressed: {} bytes\n", .{
                meta.total_compressed_size,
                meta.total_uncompressed_size,
            });

            // Values and nulls
            try stdout.print("         values: {}", .{meta.num_values});
            if (meta.statistics) |stats| {
                if (stats.null_count) |nc| {
                    try stdout.print(", nulls: {}", .{nc});
                }
            }
            try stdout.writeAll("\n");

            // Min/max from statistics
            if (meta.statistics) |stats| {
                const col_type: ?parquet.format.PhysicalType = meta.type_;
                if (stats.getMinBytes() != null or stats.getMaxBytes() != null) {
                    try stdout.writeAll("         min: ");
                    try helpers.printStatValue(stdout, stats.getMinBytes(), col_type);
                    try stdout.writeAll(", max: ");
                    try helpers.printStatValue(stdout, stats.getMaxBytes(), col_type);
                    try stdout.writeAll("\n");
                }
            }
        }

        if (rg_idx + 1 < reader.metadata.row_groups.len) {
            try stdout.writeAll("\n");
        }
    }

    try stdout.flush();
}
