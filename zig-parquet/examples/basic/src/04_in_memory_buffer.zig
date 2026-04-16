const std = @import("std");
const parquet = @import("parquet");

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    _ = init.io;

    std.debug.print("Writing to an in-memory buffer...\n", .{});

    // 1. Write to buffer instead of file
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("timestamp", parquet.TypeInfo.int64, .{});
    try writer.addColumn("value", parquet.TypeInfo.float_, .{});
    try writer.begin();

    try writer.setInt64(0, 1000);
    try writer.setFloat(1, 42.5);
    try writer.addRow();

    try writer.setInt64(0, 2000);
    try writer.setFloat(1, 43.1);
    try writer.addRow();

    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    std.debug.print("Created a Parquet buffer of {d} bytes.\n", .{buffer.len});
    std.debug.print("Reading directly from buffer...\n", .{});

    // 2. Read from buffer
    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    var sum: f32 = 0;
    var count: usize = 0;

    for (rows) |row| {
        const val = if (row.getColumn(1)) |v| v.asFloat() orelse 0 else 0;
        sum += val;
        count += 1;
    }

    std.debug.print("Read {d} metrics, average: {d:.2}\n", .{ count, sum / @as(f32, @floatFromInt(count)) });
}
