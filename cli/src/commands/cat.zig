//! Cat command - outputs all rows of a parquet file

const std = @import("std");
const parquet = @import("parquet");

pub fn run(allocator: std.mem.Allocator, file_path: []const u8, json_mode: bool) !void {
    var stdout_buf: [16384]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buf);
    const stdout = &stdout_writer.interface;

    var stderr_buf: [4096]u8 = undefined;
    var stderr_writer = std.fs.File.stderr().writer(&stderr_buf);
    const stderr = &stderr_writer.interface;

    // Open the file
    const file = std.fs.cwd().openFile(file_path, .{}) catch |err| {
        try stderr.print("Error: Cannot open file '{s}': {}\n", .{ file_path, err });
        try stderr.flush();
        std.process.exit(1);
    };
    defer file.close();

    // Create reader
    var reader = parquet.openFile(allocator, file) catch |err| {
        try stderr.print("Error: Cannot read parquet file: {}\n", .{err});
        try stderr.flush();
        std.process.exit(1);
    };
    defer reader.deinit();

    // Get column info
    const column_names = try reader.getColumnNames(allocator);
    defer allocator.free(column_names);

    const schema = reader.getSchema();

    // Get column types
    var col_types = try allocator.alloc(parquet.format.PhysicalType, column_names.len);
    defer allocator.free(col_types);

    var leaf_idx: usize = 0;
    for (schema) |elem| {
        if (elem.type_ != null and elem.num_children == null) {
            if (leaf_idx < column_names.len) {
                col_types[leaf_idx] = elem.type_.?;
                leaf_idx += 1;
            }
        }
    }

    if (json_mode) {
        try outputJson(allocator, &reader, column_names, col_types, stdout);
    } else {
        try outputTable(allocator, &reader, column_names, col_types, stdout);
    }

    try stdout.flush();
}

fn outputJson(
    allocator: std.mem.Allocator,
    reader: *parquet.Reader,
    column_names: []const []const u8,
    col_types: []const parquet.format.PhysicalType,
    writer: *std.Io.Writer,
) !void {
    const num_row_groups = reader.getNumRowGroups();

    for (0..num_row_groups) |rg_idx| {
        const rg_rows: usize = @intCast(reader.getRowGroupNumRows(rg_idx) orelse 0);
        if (rg_rows == 0) continue;

        // Read all columns for this row group
        var column_data = try allocator.alloc(ColumnValues, column_names.len);
        defer {
            for (column_data) |*col| {
                col.deinit(allocator);
            }
            allocator.free(column_data);
        }

        for (0..column_names.len) |col_idx| {
            column_data[col_idx] = try readColumnValues(reader, col_idx, rg_idx, col_types[col_idx]);
        }

        // Output each row as JSON
        for (0..rg_rows) |row_idx| {
            try writer.writeAll("{");
            for (column_names, 0..) |name, col_idx| {
                if (col_idx > 0) try writer.writeAll(",");
                try writer.print("\"{s}\":", .{name});
                try writeJsonValue(writer, &column_data[col_idx], row_idx);
            }
            try writer.writeAll("}\n");
        }
    }
}

fn outputTable(
    allocator: std.mem.Allocator,
    reader: *parquet.Reader,
    column_names: []const []const u8,
    col_types: []const parquet.format.PhysicalType,
    writer: *std.Io.Writer,
) !void {
    const num_row_groups = reader.getNumRowGroups();
    const total_rows: usize = @intCast(reader.metadata.num_rows);

    if (total_rows == 0) {
        try writer.writeAll("(empty table)\n");
        return;
    }

    // For table output, we need to calculate column widths first
    // This requires reading all data, so we buffer it

    var all_rows: std.ArrayList([][]const u8) = .empty;
    defer {
        for (all_rows.items) |row| {
            for (row) |cell| {
                allocator.free(cell);
            }
            allocator.free(row);
        }
        all_rows.deinit(allocator);
    }

    // Calculate initial widths from column names
    var col_widths = try allocator.alloc(usize, column_names.len);
    defer allocator.free(col_widths);
    for (column_names, 0..) |name, i| {
        col_widths[i] = name.len;
    }

    // Read all row groups
    for (0..num_row_groups) |rg_idx| {
        const rg_rows: usize = @intCast(reader.getRowGroupNumRows(rg_idx) orelse 0);
        if (rg_rows == 0) continue;

        // Read all columns for this row group
        var column_data = try allocator.alloc(ColumnValues, column_names.len);
        defer {
            for (column_data) |*col| {
                col.deinit(allocator);
            }
            allocator.free(column_data);
        }

        for (0..column_names.len) |col_idx| {
            column_data[col_idx] = try readColumnValues(reader, col_idx, rg_idx, col_types[col_idx]);
        }

        // Convert to string rows
        for (0..rg_rows) |row_idx| {
            var row = try allocator.alloc([]const u8, column_names.len);
            for (0..column_names.len) |col_idx| {
                const cell = try formatCellValue(allocator, &column_data[col_idx], row_idx);
                row[col_idx] = cell;
                col_widths[col_idx] = @max(col_widths[col_idx], cell.len);
            }
            try all_rows.append(allocator, row);
        }
    }

    // Print header
    try writer.writeAll("|");
    for (column_names, 0..) |name, i| {
        try writer.print(" {s:<[1]} |", .{ name, col_widths[i] });
    }
    try writer.writeAll("\n");

    // Print separator
    try writer.writeAll("|");
    for (col_widths) |width| {
        for (0..width + 2) |_| {
            try writer.writeByte('-');
        }
        try writer.writeAll("|");
    }
    try writer.writeAll("\n");

    // Print rows
    for (all_rows.items) |row| {
        try writer.writeAll("|");
        for (row, 0..) |cell, i| {
            try writer.print(" {s:<[1]} |", .{ cell, col_widths[i] });
        }
        try writer.writeAll("\n");
    }
}

const ColumnValues = union(enum) {
    bools: []parquet.Optional(bool),
    i32s: []parquet.Optional(i32),
    i64s: []parquet.Optional(i64),
    f32s: []parquet.Optional(f32),
    f64s: []parquet.Optional(f64),
    strings: []parquet.Optional([]const u8),

    fn deinit(self: *ColumnValues, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .strings => |values| {
                for (values) |v| {
                    switch (v) {
                        .value => |s| allocator.free(s),
                        .null_value => {},
                    }
                }
                allocator.free(values);
            },
            .bools => |v| allocator.free(v),
            .i32s => |v| allocator.free(v),
            .i64s => |v| allocator.free(v),
            .f32s => |v| allocator.free(v),
            .f64s => |v| allocator.free(v),
        }
    }
};

fn readColumnValues(
    reader: *parquet.Reader,
    col_idx: usize,
    rg_idx: usize,
    col_type: parquet.format.PhysicalType,
) !ColumnValues {
    return switch (col_type) {
        .boolean => .{ .bools = try reader.readColumnFromRowGroup(col_idx, rg_idx, bool) },
        .int32 => .{ .i32s = try reader.readColumnFromRowGroup(col_idx, rg_idx, i32) },
        .int64 => .{ .i64s = try reader.readColumnFromRowGroup(col_idx, rg_idx, i64) },
        .float => .{ .f32s = try reader.readColumnFromRowGroup(col_idx, rg_idx, f32) },
        .double => .{ .f64s = try reader.readColumnFromRowGroup(col_idx, rg_idx, f64) },
        .byte_array, .fixed_len_byte_array => .{ .strings = try reader.readColumnFromRowGroup(col_idx, rg_idx, []const u8) },
        .int96 => return error.UnsupportedType, // INT96 is deprecated and not supported
    };
}

fn writeJsonValue(writer: *std.Io.Writer, col: *const ColumnValues, row_idx: usize) !void {
    switch (col.*) {
        .bools => |values| {
            switch (values[row_idx]) {
                .value => |v| try writer.writeAll(if (v) "true" else "false"),
                .null_value => try writer.writeAll("null"),
            }
        },
        .i32s => |values| {
            switch (values[row_idx]) {
                .value => |v| try writer.print("{}", .{v}),
                .null_value => try writer.writeAll("null"),
            }
        },
        .i64s => |values| {
            switch (values[row_idx]) {
                .value => |v| try writer.print("{}", .{v}),
                .null_value => try writer.writeAll("null"),
            }
        },
        .f32s => |values| {
            switch (values[row_idx]) {
                .value => |v| try writer.print("{d}", .{v}),
                .null_value => try writer.writeAll("null"),
            }
        },
        .f64s => |values| {
            switch (values[row_idx]) {
                .value => |v| try writer.print("{d}", .{v}),
                .null_value => try writer.writeAll("null"),
            }
        },
        .strings => |values| {
            switch (values[row_idx]) {
                .value => |v| try writeJsonString(writer, v),
                .null_value => try writer.writeAll("null"),
            }
        },
    }
}

fn writeJsonString(writer: *std.Io.Writer, s: []const u8) !void {
    try writer.writeAll("\"");
    for (s) |c| {
        switch (c) {
            '"' => try writer.writeAll("\\\""),
            '\\' => try writer.writeAll("\\\\"),
            '\n' => try writer.writeAll("\\n"),
            '\r' => try writer.writeAll("\\r"),
            '\t' => try writer.writeAll("\\t"),
            else => {
                if (c < 32) {
                    try writer.print("\\u{x:0>4}", .{c});
                } else {
                    try writer.writeByte(c);
                }
            },
        }
    }
    try writer.writeAll("\"");
}

fn formatCellValue(allocator: std.mem.Allocator, col: *const ColumnValues, row_idx: usize) ![]const u8 {
    switch (col.*) {
        .bools => |values| {
            return switch (values[row_idx]) {
                .value => |v| try allocator.dupe(u8, if (v) "true" else "false"),
                .null_value => try allocator.dupe(u8, "null"),
            };
        },
        .i32s => |values| {
            return switch (values[row_idx]) {
                .value => |v| try std.fmt.allocPrint(allocator, "{}", .{v}),
                .null_value => try allocator.dupe(u8, "null"),
            };
        },
        .i64s => |values| {
            return switch (values[row_idx]) {
                .value => |v| try std.fmt.allocPrint(allocator, "{}", .{v}),
                .null_value => try allocator.dupe(u8, "null"),
            };
        },
        .f32s => |values| {
            return switch (values[row_idx]) {
                .value => |v| try std.fmt.allocPrint(allocator, "{d}", .{v}),
                .null_value => try allocator.dupe(u8, "null"),
            };
        },
        .f64s => |values| {
            return switch (values[row_idx]) {
                .value => |v| try std.fmt.allocPrint(allocator, "{d}", .{v}),
                .null_value => try allocator.dupe(u8, "null"),
            };
        },
        .strings => |values| {
            return switch (values[row_idx]) {
                .value => |v| try formatStringForTable(allocator, v),
                .null_value => try allocator.dupe(u8, "null"),
            };
        },
    }
}

fn formatStringForTable(allocator: std.mem.Allocator, data: []const u8) ![]const u8 {
    // Check if printable
    var is_printable = true;
    for (data) |c| {
        if (c < 32 and c != '\t' and c != '\n' and c != '\r') {
            is_printable = false;
            break;
        }
    }

    if (is_printable) {
        if (data.len > 50) {
            return try std.fmt.allocPrint(allocator, "{s}...", .{data[0..47]});
        }
        return try allocator.dupe(u8, data);
    } else {
        // Binary data - show as hex
        if (data.len > 16) {
            var buf = try allocator.alloc(u8, 16 * 2 + 3);
            for (0..16) |i| {
                _ = std.fmt.bufPrint(buf[i * 2 ..][0..2], "{x:0>2}", .{data[i]}) catch unreachable;
            }
            buf[32] = '.';
            buf[33] = '.';
            buf[34] = '.';
            return buf;
        }
        var buf = try allocator.alloc(u8, data.len * 2);
        for (data, 0..) |b, i| {
            _ = std.fmt.bufPrint(buf[i * 2 ..][0..2], "{x:0>2}", .{b}) catch unreachable;
        }
        return buf;
    }
}
