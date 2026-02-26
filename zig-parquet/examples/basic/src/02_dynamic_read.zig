const std = @import("std");
const parquet = @import("parquet");

const Record = struct {
    id: i32,
    score: f64,
    tags: []const []const u8,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const output_path = "dynamic_test.parquet";
    defer std.fs.cwd().deleteFile(output_path) catch {};

    // First create a file to read
    {
        const file = try std.fs.cwd().createFile(output_path, .{});
        defer file.close();

        var writer = try parquet.writeToFileRows(Record, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{ .id = 1, .score = 95.5, .tags = &[_][]const u8{ "fast", "reliable" } });
        try writer.close();
    }

    std.debug.print("Dynamically reading {s}...\n", .{output_path});

    // Now read it dynamically
    const file = try std.fs.cwd().openFile(output_path, .{});
    defer file.close();

    var reader = try parquet.openFileDynamic(allocator, file, .{});
    defer reader.deinit();

    // Print schema
    const schema = reader.getSchema();
    std.debug.print("Schema has {d} columns:\n", .{schema.len});
    for (schema, 0..) |col, i| {
        std.debug.print("  {d}: {s}\n", .{ i, col.name });
    }

    // Read all rows from the first row group
    if (reader.getNumRowGroups() > 0) {
        const rows = try reader.readAllRows(0);
        defer {
            for (rows) |row| row.deinit();
            allocator.free(rows);
        }

        std.debug.print("\nRead {d} rows:\n", .{rows.len});
        for (rows, 0..) |row, row_idx| {
            std.debug.print("Row {d}:\n", .{row_idx});
            for (0..row.columnCount()) |col_idx| {
                const val = row.getColumn(col_idx);
                std.debug.print("  Col {d}: {any}\n", .{ col_idx, val });
            }
        }
    }
}
