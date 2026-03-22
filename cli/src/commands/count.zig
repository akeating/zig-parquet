//! Count command - prints total row count

const std = @import("std");
const parquet = @import("parquet");

pub fn run(allocator: std.mem.Allocator, file_path: []const u8) !void {
    var stderr_buf: [4096]u8 = undefined;
    var stderr_writer = std.fs.File.stderr().writer(&stderr_buf);
    const stderr = &stderr_writer.interface;

    const file = std.fs.cwd().openFile(file_path, .{}) catch |err| {
        try stderr.print("Error: Cannot open file '{s}': {}\n", .{ file_path, err });
        try stderr.flush();
        std.process.exit(1);
    };
    defer file.close();

    var reader = parquet.openFileDynamic(allocator, file, .{}) catch |err| {
        try stderr.print("Error: Cannot read parquet file: {}\n", .{err});
        try stderr.flush();
        std.process.exit(1);
    };
    defer reader.deinit();

    var stdout_buf: [256]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buf);
    const stdout = &stdout_writer.interface;

    try stdout.print("{}\n", .{reader.metadata.num_rows});
    try stdout.flush();
}
