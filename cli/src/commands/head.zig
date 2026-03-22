//! Head command - displays first N rows of a parquet file

const std = @import("std");
const parquet = @import("parquet");
const safe = parquet.safe;
const cat = @import("cat.zig");

pub fn run(allocator: std.mem.Allocator, file_path: []const u8, num_rows: usize) !void {
    var stdout_buf: [16384]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buf);
    const stdout = &stdout_writer.interface;

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

    const column_names = try cat.getTopLevelColumnNames(allocator, reader.getSchema());
    defer allocator.free(column_names);

    const total_rows: usize = safe.cast(reader.getTotalNumRows()) catch 0;
    const rows_to_show = @min(num_rows, total_rows);

    if (rows_to_show == 0) {
        try stdout.writeAll("(empty table)\n");
        try stdout.flush();
        return;
    }

    // Buffer rows as formatted strings for column width calculation
    var all_rows: std.ArrayListUnmanaged([][]const u8) = .empty;
    defer {
        for (all_rows.items) |row| {
            for (row) |cell_val| allocator.free(cell_val);
            allocator.free(row);
        }
        all_rows.deinit(allocator);
    }

    var col_widths = try allocator.alloc(usize, column_names.len);
    defer allocator.free(col_widths);
    for (column_names, 0..) |name, i| {
        col_widths[i] = name.len;
    }

    var iter = reader.rowIterator();
    defer iter.deinit();
    var rows_collected: usize = 0;

    while (rows_collected < rows_to_show) {
        const row = try iter.next() orelse break;
        const num_cols = @min(row.values.len, column_names.len);
        var cells = try allocator.alloc([]const u8, column_names.len);
        for (0..column_names.len) |col_idx| {
            const cell = if (col_idx < num_cols)
                try cat.formatCellValue(allocator, row.values[col_idx])
            else
                try allocator.dupe(u8, "null");
            cells[col_idx] = cell;
            col_widths[col_idx] = @max(col_widths[col_idx], cell.len);
        }
        try all_rows.append(allocator, cells);
        rows_collected += 1;
    }

    // Print header
    try stdout.writeAll("|");
    for (column_names, 0..) |name, i| {
        try stdout.print(" {s:<[1]} |", .{ name, col_widths[i] });
    }
    try stdout.writeAll("\n");

    // Print separator
    try stdout.writeAll("|");
    for (col_widths) |width| {
        for (0..width + 2) |_| {
            try stdout.writeByte('-');
        }
        try stdout.writeAll("|");
    }
    try stdout.writeAll("\n");

    // Print rows
    for (all_rows.items) |row| {
        try stdout.writeAll("|");
        for (row, 0..) |cell, i| {
            try stdout.print(" {s:<[1]} |", .{ cell, col_widths[i] });
        }
        try stdout.writeAll("\n");
    }

    // Print row count
    if (total_rows > rows_to_show) {
        try stdout.print("\n({} of {} rows shown)\n", .{ rows_to_show, total_rows });
    }

    try stdout.flush();
}
