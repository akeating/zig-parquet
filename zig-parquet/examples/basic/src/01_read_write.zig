const std = @import("std");
const parquet = @import("parquet");

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

        var writer = try parquet.createFileDynamic(allocator, file);
        defer writer.deinit();

        try writer.addColumn("id", parquet.TypeInfo.int32, .{});
        try writer.addColumn("name", parquet.TypeInfo.string, .{});
        try writer.addColumn("active", parquet.TypeInfo.bool_, .{});
        writer.setCompression(.zstd);
        try writer.begin();

        const users = [_]struct { id: i32, name: []const u8, active: bool }{
            .{ .id = 1, .name = "Alice", .active = true },
            .{ .id = 2, .name = "Bob", .active = false },
            .{ .id = 3, .name = "Charlie", .active = true },
        };

        for (users) |user| {
            try writer.setInt32(0, user.id);
            try writer.setBytes(1, user.name);
            try writer.setBool(2, user.active);
            try writer.addRow();
        }

        try writer.close();
    }

    std.debug.print("Reading from {s}...\n", .{output_path});

    // Read phase
    {
        const file = try std.fs.cwd().openFile(output_path, .{});
        defer file.close();

        var reader = try parquet.openFileDynamic(allocator, file, .{});
        defer reader.deinit();

        const rows = try reader.readAllRows(0);
        defer {
            for (rows) |row| row.deinit();
            allocator.free(rows);
        }

        for (rows) |row| {
            const id = if (row.getColumn(0)) |v| v.asInt32() orelse 0 else 0;
            const name = if (row.getColumn(1)) |v| v.asBytes() orelse "(null)" else "(null)";
            const active = if (row.getColumn(2)) |v| v.asBool() orelse false else false;
            std.debug.print("User {d}: {s} (active: {any})\n", .{ id, name, active });
        }
    }

    std.debug.print("Done!\n", .{});
}
