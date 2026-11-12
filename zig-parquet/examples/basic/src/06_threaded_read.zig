//! Reads a file with its columns decoded on several threads: Arrow batches with a row group
//! read ahead, and rows through DynamicReader.
//!
//! A read decodes on threads when it is given a `std.Io` whose `async` runs tasks on them,
//! such as a `std.Io.Threaded`. The allocator must then allow concurrent use
//! (`std.heap.smp_allocator`; an `ArenaAllocator` does not).
//!
//! Timings are representative only in a release build:
//!   zig build run-06_threaded_read -Doptimize=ReleaseFast

const std = @import("std");
const parquet = @import("parquet");

const num_rows = 400_000;
const row_group_rows = 100_000;

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const allocator = std.heap.smp_allocator;

    const path = "threaded_read.parquet";
    defer std.Io.Dir.cwd().deleteFile(io, path) catch {};
    try writeFile(allocator, io, path);

    // Up to 4 threads: 3 in the pool and the calling one.
    var threaded: std.Io.Threaded = .init(allocator, .{ .async_limit = .limited(3) });
    defer threaded.deinit();
    const decode_io = threaded.io();

    const file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);

    // Two different `Io`s: `io` reads the file; `.io` in the options decodes rows on the
    // pool's threads.
    var reader = try parquet.openFileDynamic(allocator, file, io, .{ .io = decode_io });
    defer reader.deinit();

    // Arrow batches of 65,536 rows: on the calling thread, then on the pool's threads with
    // the next row group decoded while the current one's batches are returned.
    for ([_]?std.Io{ null, decode_io }) |batch_io| {
        const start = std.Io.Clock.awake.now(io);
        var batches = try parquet.ArrowBatchReader.init(allocator, reader.getSource(), reader.metadata, .{
            .batch_rows = 65536,
            .io = batch_io,
            .read_ahead = if (batch_io != null) 1 else 0,
        });
        defer batches.deinit();

        var rows: i64 = 0;
        var count: usize = 0;
        while (try batches.next()) |batch| {
            var result = batch;
            defer result.deinit();
            rows += result.arrays[0].length;
            count += 1;
        }
        const elapsed = start.durationTo(std.Io.Clock.awake.now(io));
        std.debug.print("{s}: {d} batches, {d} rows, {d:.1} ms\n", .{
            if (batch_io == null) "batches on the calling thread" else "batches on 4 threads, 1 row group ahead",
            count,
            rows,
            @as(f64, @floatFromInt(elapsed.nanoseconds)) / 1e6,
        });
    }

    // Rows of the first row group; the reader decodes its columns on the pool's threads.
    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }
    const first = rows[0].values;
    std.debug.print("row group 0: {d} rows; first row: id {d}, score {d:.2}, name {s}\n", .{
        rows.len,
        first[0].int64_val,
        first[1].double_val,
        first[2].bytes_val,
    });
}

/// Writes `num_rows` rows of id, score and name, in row groups of `row_group_rows`.
fn writeFile(allocator: std.mem.Allocator, io: std.Io, path: []const u8) !void {
    const file = try std.Io.Dir.cwd().createFile(io, path, .{});
    defer file.close(io);

    var writer = try parquet.createFileDynamic(allocator, file, io);
    defer writer.deinit();
    writer.setRowGroupSize(row_group_rows);
    try writer.addColumn("id", parquet.TypeInfo.int64, .{});
    try writer.addColumn("score", parquet.TypeInfo.double_, .{});
    try writer.addColumn("name", parquet.TypeInfo.string, .{});
    try writer.begin();

    var name_buf: [32]u8 = undefined;
    for (0..num_rows) |i| {
        try writer.setInt64(0, @intCast(i));
        try writer.setDouble(1, @as(f64, @floatFromInt(i % 1000)) / 10.0);
        try writer.setBytes(2, try std.fmt.bufPrint(&name_buf, "user_{d}", .{i % 5000}));
        try writer.addRow();
    }
    try writer.close();
}
