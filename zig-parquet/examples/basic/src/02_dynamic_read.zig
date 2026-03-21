const std = @import("std");
const parquet = @import("parquet");

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

        var writer = try parquet.createFileDynamic(allocator, file);
        defer writer.deinit();

        try writer.addColumn("id", parquet.TypeInfo.int32, .{});
        try writer.addColumn("score", parquet.TypeInfo.double_, .{});

        // tags: list<string>
        const str_node = try writer.allocSchemaNode(.{ .byte_array = .{} });
        const list_node = try writer.allocSchemaNode(.{ .list = str_node });
        try writer.addColumnNested("tags", list_node, .{});

        try writer.begin();

        try writer.setInt32(0, 1);
        try writer.setDouble(1, 95.5);
        try writer.beginList(2);
        try writer.appendNestedBytes(2, "fast");
        try writer.appendNestedBytes(2, "reliable");
        try writer.endList(2);
        try writer.addRow();

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
                if (row.getColumn(col_idx)) |val| {
                    std.debug.print("  Col {d}: {any}\n", .{ col_idx, val });
                } else {
                    std.debug.print("  Col {d}: (missing)\n", .{col_idx});
                }
            }
        }
    }
}
