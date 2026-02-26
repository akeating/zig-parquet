const std = @import("std");
const parquet = @import("parquet");

const Metric = struct {
    timestamp: i64,
    value: f32,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("Writing to an in-memory buffer...\n", .{});

    // 1. Write to buffer instead of file
    var writer = try parquet.writeToBufferRows(Metric, allocator, .{});
    defer writer.deinit();

    try writer.writeRow(.{ .timestamp = 1000, .value = 42.5 });
    try writer.writeRow(.{ .timestamp = 2000, .value = 43.1 });

    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    std.debug.print("Created a Parquet buffer of {d} bytes.\n", .{buffer.len});
    std.debug.print("Reading directly from buffer...\n", .{});

    // 2. Read from buffer instead of file
    var reader = try parquet.openBufferRowReader(Metric, allocator, buffer, .{});
    defer reader.deinit();

    var sum: f32 = 0;
    var count: usize = 0;

    while (try reader.next()) |metric| {
        defer reader.freeRow(&metric);
        sum += metric.value;
        count += 1;
    }

    std.debug.print("Read {d} metrics, average: {d:.2}\n", .{ count, sum / @as(f32, @floatFromInt(count)) });
}
