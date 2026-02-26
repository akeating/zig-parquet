//! Size command - file size breakdown with percentages

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

    const file_stat = file.stat() catch |err| {
        try stderr.print("Error: Cannot stat file: {}\n", .{err});
        try stderr.flush();
        std.process.exit(1);
    };
    const file_size = file_stat.size;

    var reader = parquet.openFile(allocator, file) catch |err| {
        try stderr.print("Error: Cannot read parquet file: {}\n", .{err});
        try stderr.flush();
        std.process.exit(1);
    };
    defer reader.deinit();

    // Compute footer size from the last 8 bytes: 4 bytes footer length + 4 bytes PAR1 magic
    // The footer length is stored as little-endian u32 at file_size - 8
    const footer_metadata_size: u64 = blk: {
        var footer_buf: [8]u8 = undefined;
        const source = reader.getSource();
        if (file_size < 12) break :blk 0;
        const read = source.readAt(file_size - 8, &footer_buf) catch break :blk 0;
        if (read < 8) break :blk 0;
        const footer_len = std.mem.readInt(u32, footer_buf[0..4], .little);
        break :blk @as(u64, footer_len) + 8; // footer content + 4-byte length + 4-byte magic
    };
    const header_size: u64 = 4; // PAR1 magic

    // Column compressed sizes summed across all row groups
    const col_names = reader.getColumnNames(allocator) catch &.{};
    defer allocator.free(col_names);

    const num_columns = helpers.countLeafColumns(reader.getSchema());
    const file_size_f: f64 = @floatFromInt(file_size);

    try stdout.print("File: {s}\n", .{file_path});
    try stdout.writeAll("Total: ");
    try helpers.printFileSize(stdout, file_size);
    try stdout.writeAll("\n\n");

    // Header
    try stdout.print("  Header (PAR1):   {:>10} bytes  ({d:>5.1}%)\n", .{
        header_size,
        pct(header_size, file_size_f),
    });

    // Data (per-column breakdown)
    var total_data: u64 = 0;
    var col_sizes = allocator.alloc(u64, num_columns) catch {
        try stdout.writeAll("  (unable to compute column sizes)\n");
        try stdout.flush();
        return;
    };
    defer allocator.free(col_sizes);
    @memset(col_sizes, 0);

    for (reader.metadata.row_groups) |rg| {
        for (rg.columns, 0..) |col, col_idx| {
            if (col.meta_data) |meta| {
                const cs: u64 = if (meta.total_compressed_size > 0)
                    @intCast(meta.total_compressed_size)
                else
                    0;
                if (col_idx < num_columns) {
                    col_sizes[col_idx] += cs;
                }
                total_data += cs;
            }
        }
    }

    try stdout.print("  Data:            {:>10} bytes  ({d:>5.1}%)\n", .{
        total_data,
        pct(total_data, file_size_f),
    });

    for (0..num_columns) |col_idx| {
        const col_name = if (col_idx < col_names.len) col_names[col_idx] else "?";
        try stdout.print("    {s}: {:>10} bytes  ({d:>5.1}%)\n", .{
            col_name,
            col_sizes[col_idx],
            pct(col_sizes[col_idx], file_size_f),
        });
    }

    // Footer
    try stdout.print("  Footer:          {:>10} bytes  ({d:>5.1}%)\n", .{
        footer_metadata_size,
        pct(footer_metadata_size, file_size_f),
    });

    try stdout.flush();
}

fn pct(part: u64, total: f64) f64 {
    if (total == 0) return 0;
    return @as(f64, @floatFromInt(part)) / total * 100.0;
}
