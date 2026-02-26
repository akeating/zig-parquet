//! Read a random 10-second segment from a power grid parquet file
//!
//! Outputs each channel as a JSON array of arrays:
//! [[voltage_a], [voltage_b], [voltage_c], [current_a], [current_b], [current_c], [frequency], [power_factor]]
//!
//! Usage: read-segment [file.parquet]

const std = @import("std");
const parquet = @import("parquet");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    const file_path = if (args.len > 1) args[1] else "grid_data.parquet";

    const file = std.fs.cwd().openFile(file_path, .{}) catch |err| {
        std.debug.print("Error opening file '{s}': {}\n", .{ file_path, err });
        return err;
    };
    defer file.close();

    var reader = parquet.openFile(allocator, file) catch |err| {
        std.debug.print("Error initializing parquet reader: {}\n", .{err});
        return err;
    };
    defer reader.deinit();

    const total_rows = reader.getTotalNumRows();
    const num_row_groups = reader.getNumRowGroups();
    const rows_per_group: usize = 2500;

    std.debug.print("File: {s}\n", .{file_path});
    std.debug.print("Total rows: {}\n", .{total_rows});
    std.debug.print("Row groups: {}\n", .{num_row_groups});
    std.debug.print("Total duration: {d:.1} seconds\n", .{@as(f64, @floatFromInt(total_rows)) / 250.0});

    var prng = std.Random.DefaultPrng.init(@intCast(std.time.nanoTimestamp()));
    const random = prng.random();
    const row_group_idx = random.intRangeAtMost(usize, 0, num_row_groups - 1);
    const start_row = row_group_idx * rows_per_group;
    const time_seconds = @as(f64, @floatFromInt(start_row)) / 250.0;
    const hours = @as(usize, @intFromFloat(time_seconds / 3600.0));
    const minutes = @as(usize, @intFromFloat(@mod(time_seconds, 3600.0) / 60.0));
    const seconds = @as(usize, @intFromFloat(@mod(time_seconds, 60.0)));

    std.debug.print("Reading row group {} (rows {} to {}, time {}:{:0>2}:{:0>2})\n", .{
        row_group_idx,
        start_row,
        start_row + rows_per_group,
        hours,
        minutes,
        seconds,
    });

    // Column indices: timestamp=0, seq=1, voltage_a..power_factor=2..9
    const channel_indices = [_]usize{ 2, 3, 4, 5, 6, 7, 8, 9 };

    var stdout_buf: [8192]u8 = undefined;
    var stdout_stream = std.fs.File.stdout().writerStreaming(&stdout_buf);
    const stdout = &stdout_stream.interface;

    try stdout.writeAll("[\n");

    for (channel_indices, 0..) |col_index, i| {
        const column_data = reader.readColumnFromRowGroup(col_index, row_group_idx, i32) catch |err| {
            std.debug.print("Error reading column {} from row group {}: {}\n", .{ col_index, row_group_idx, err });
            continue;
        };
        defer allocator.free(column_data);

        if (i > 0) {
            try stdout.writeAll(",\n");
        }
        try stdout.writeAll("  [");

        for (column_data, 0..) |val, j| {
            if (j > 0) {
                try stdout.writeAll(", ");
            }
            if (val.isNull()) {
                try stdout.writeAll("null");
            } else {
                var buf: [16]u8 = undefined;
                const num_str = std.fmt.bufPrint(&buf, "{}", .{val.value}) catch "?";
                try stdout.writeAll(num_str);
            }
        }

        try stdout.writeAll("]");
    }

    try stdout.writeAll("\n]\n");
    try stdout_stream.interface.flush();

    std.debug.print("\nOutput: 8 channels x {} samples = {} values\n", .{ rows_per_group, 8 * rows_per_group });
}
