const std = @import("std");
const parquet = @import("parquet");
const Optional = parquet.Optional;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    const output_path = "column_api.parquet";
    defer std.Io.Dir.cwd().deleteFile(io, output_path) catch {};

    std.debug.print("Writing columns to {s}...\n", .{output_path});

    // Write phase — define schema explicitly, write each column separately
    {
        const file = try std.Io.Dir.cwd().createFile(io, output_path, .{});
        defer file.close(io);

        var writer = try parquet.writeToFile(allocator, file, io, &.{
            .{ .name = "id", .type_ = .int64, .optional = false, .codec = .zstd },
            .{ .name = "score", .type_ = .double, .optional = false },
            .{ .name = "name", .type_ = .byte_array, .optional = true },
        });
        defer writer.deinit();

        try writer.writeColumnOptional(i64, 0, &[_]Optional(i64){
            .{ .value = 1 }, .{ .value = 2 }, .{ .value = 3 }, .{ .value = 4 }, .{ .value = 5 },
        });
        try writer.writeColumnOptional(f64, 1, &[_]Optional(f64){
            .{ .value = 95.0 }, .{ .value = 87.5 }, .{ .value = 92.3 }, .{ .value = 78.1 }, .{ .value = 100.0 },
        });
        try writer.writeColumnOptional([]const u8, 2, &[_]Optional([]const u8){
            .{ .value = "Alice" }, .null_value, .{ .value = "Charlie" }, .{ .value = "Diana" }, .null_value,
        });

        try writer.close();
    }

    std.debug.print("Reading columns from {s}...\n", .{output_path});

    // Read phase — DynamicReader returns rows of Value types
    {
        const file = try std.Io.Dir.cwd().openFile(io, output_path, .{});
        defer file.close(io);

        var reader = try parquet.openFileDynamic(allocator, file, io, .{});
        defer reader.deinit();

        const rows = try reader.readAllRows(0);
        defer {
            for (rows) |row| row.deinit();
            allocator.free(rows);
        }

        for (rows) |row| {
            const id_val = if (row.getColumn(0)) |v| v.asInt64() orelse 0 else 0;
            const score_val = if (row.getColumn(1)) |v| v.asDouble() orelse 0 else 0;
            const name_val = if (row.getColumn(2)) |v| v.asBytes() else null;

            if (name_val) |name| {
                std.debug.print("ID {d}: score {d:.1}, name: {s}\n", .{ id_val, score_val, name });
            } else {
                std.debug.print("ID {d}: score {d:.1}, name: (null)\n", .{ id_val, score_val });
            }
        }
    }

    std.debug.print("Done!\n", .{});
}
