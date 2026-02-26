const std = @import("std");
const parquet = @import("parquet");
const Optional = parquet.Optional;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const output_path = "column_api.parquet";
    defer std.fs.cwd().deleteFile(output_path) catch {};

    std.debug.print("Writing columns to {s}...\n", .{output_path});

    // Write phase — define schema explicitly, write each column separately
    {
        const file = try std.fs.cwd().createFile(output_path, .{});
        defer file.close();

        var writer = try parquet.writeToFile(allocator, file, &.{
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

    // Read phase — readColumn returns []Optional(T)
    {
        const file = try std.fs.cwd().openFile(output_path, .{});
        defer file.close();

        var reader = try parquet.openFile(allocator, file);
        defer reader.deinit();

        const ids = try reader.readColumn(0, i64);
        defer allocator.free(ids);

        const scores = try reader.readColumn(1, f64);
        defer allocator.free(scores);

        const names = try reader.readColumn(2, []const u8);
        defer {
            for (names) |n| if (!n.isNull()) allocator.free(n.value);
            allocator.free(names);
        }

        for (ids, scores, names) |id, score, name| {
            const id_val = id.value;
            const score_val = score.value;

            if (name.isNull()) {
                std.debug.print("ID {d}: score {d:.1}, name: (null)\n", .{ id_val, score_val });
            } else {
                std.debug.print("ID {d}: score {d:.1}, name: {s}\n", .{ id_val, score_val, name.value });
            }
        }
    }

    std.debug.print("Done!\n", .{});
}
