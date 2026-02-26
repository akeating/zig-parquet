const std = @import("std");
const parquet = @import("parquet");

const User = struct {
    id: i32,
    name: []const u8,
    active: bool,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const output_path = "basic_users.parquet";
    defer std.fs.cwd().deleteFile(output_path) catch {};

    std.debug.print("Writing to {s}...\n", .{output_path});

    // Write phase
    {
        const file = try std.fs.cwd().createFile(output_path, .{});
        defer file.close();

        var writer = try parquet.writeToFileRows(User, allocator, file, .{
            .compression = .zstd,
        });
        defer writer.deinit();

        const users = [_]User{
            .{ .id = 1, .name = "Alice", .active = true },
            .{ .id = 2, .name = "Bob", .active = false },
            .{ .id = 3, .name = "Charlie", .active = true },
        };

        try writer.writeRows(&users);
        try writer.close();
    }

    std.debug.print("Reading from {s}...\n", .{output_path});

    // Read phase
    {
        const file = try std.fs.cwd().openFile(output_path, .{});
        defer file.close();

        var reader = try parquet.openFileRowReader(User, allocator, file, .{});
        defer reader.deinit();

        while (try reader.next()) |user| {
            defer reader.freeRow(&user);
            std.debug.print("User {d}: {s} (active: {any})\n", .{ user.id, user.name, user.active });
        }
    }

    std.debug.print("Done!\n", .{});
}
