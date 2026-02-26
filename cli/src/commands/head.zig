//! Head command - displays first N rows of a parquet file

const std = @import("std");
const parquet = @import("parquet");

pub fn run(allocator: std.mem.Allocator, file_path: []const u8, num_rows: usize) !void {
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

    // Get column names
    const column_names = try reader.getColumnNames(allocator);
    defer allocator.free(column_names);

    const total_rows: usize = @intCast(reader.metadata.num_rows);
    const rows_to_show = @min(num_rows, total_rows);

    if (rows_to_show == 0) {
        try stdout.writeAll("(empty table)\n");
        try stdout.flush();
        return;
    }

    // Read all column data and format as strings
    var column_data = try allocator.alloc([][]const u8, column_names.len);
    // Initialize all slots to empty slices for safe cleanup
    for (column_data) |*col| {
        col.* = &[_][]const u8{};
    }
    defer {
        for (column_data) |col| {
            for (col) |cell| {
                allocator.free(cell);
            }
            if (col.len > 0) allocator.free(col);
        }
        allocator.free(column_data);
    }

    // Read each column based on its type
    for (0..column_names.len) |col_idx| {
        column_data[col_idx] = try readColumnAsStrings(allocator, &reader, col_idx, rows_to_show);
    }

    // Calculate column widths
    var col_widths = try allocator.alloc(usize, column_names.len);
    defer allocator.free(col_widths);

    for (column_names, 0..) |name, i| {
        col_widths[i] = name.len;
        for (column_data[i]) |cell| {
            col_widths[i] = @max(col_widths[i], cell.len);
        }
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
    for (0..rows_to_show) |row_idx| {
        try stdout.writeAll("|");
        for (0..column_names.len) |col_idx| {
            const cell = column_data[col_idx][row_idx];
            try stdout.print(" {s:<[1]} |", .{ cell, col_widths[col_idx] });
        }
        try stdout.writeAll("\n");
    }

    // Print row count
    if (total_rows > rows_to_show) {
        try stdout.print("\n({} of {} rows shown)\n", .{ rows_to_show, total_rows });
    }

    try stdout.flush();
}

fn readColumnAsStrings(
    allocator: std.mem.Allocator,
    reader: *parquet.Reader,
    col_idx: usize,
    num_rows: usize,
) ![][]const u8 {
    // Get the column type from schema
    const schema = reader.getSchema();
    var leaf_idx: usize = 0;
    var col_type: ?parquet.format.PhysicalType = null;

    for (schema) |elem| {
        if (elem.type_ != null and elem.num_children == null) {
            if (leaf_idx == col_idx) {
                col_type = elem.type_;
                break;
            }
            leaf_idx += 1;
        }
    }

    const result = try allocator.alloc([]const u8, num_rows);
    errdefer allocator.free(result);

    // Read based on type
    if (col_type) |t| {
        switch (t) {
            .boolean => {
                const values = try reader.readColumn(col_idx, bool);
                defer allocator.free(values);
                for (0..num_rows) |i| {
                    result[i] = switch (values[i]) {
                        .value => |v| try allocator.dupe(u8, if (v) "true" else "false"),
                        .null_value => try allocator.dupe(u8, "null"),
                    };
                }
            },
            .int32 => {
                const values = reader.readColumn(col_idx, i32) catch |err| {
                    // Handle unsupported encodings gracefully
                    for (0..num_rows) |i| {
                        result[i] = try std.fmt.allocPrint(allocator, "({s})", .{@errorName(err)});
                    }
                    return result;
                };
                defer allocator.free(values);
                for (0..num_rows) |i| {
                    result[i] = switch (values[i]) {
                        .value => |v| try std.fmt.allocPrint(allocator, "{}", .{v}),
                        .null_value => try allocator.dupe(u8, "null"),
                    };
                }
            },
            .int64 => {
                const values = try reader.readColumn(col_idx, i64);
                defer allocator.free(values);
                for (0..num_rows) |i| {
                    result[i] = switch (values[i]) {
                        .value => |v| try std.fmt.allocPrint(allocator, "{}", .{v}),
                        .null_value => try allocator.dupe(u8, "null"),
                    };
                }
            },
            .float => {
                const values = reader.readColumn(col_idx, f32) catch |err| {
                    for (0..num_rows) |i| {
                        result[i] = try std.fmt.allocPrint(allocator, "({s})", .{@errorName(err)});
                    }
                    return result;
                };
                defer allocator.free(values);
                for (0..num_rows) |i| {
                    result[i] = switch (values[i]) {
                        .value => |v| try std.fmt.allocPrint(allocator, "{d}", .{v}),
                        .null_value => try allocator.dupe(u8, "null"),
                    };
                }
            },
            .double => {
                const values = try reader.readColumn(col_idx, f64);
                defer allocator.free(values);
                for (0..num_rows) |i| {
                    result[i] = switch (values[i]) {
                        .value => |v| try std.fmt.allocPrint(allocator, "{d}", .{v}),
                        .null_value => try allocator.dupe(u8, "null"),
                    };
                }
            },
            .byte_array, .fixed_len_byte_array => {
                const values = try reader.readColumn(col_idx, []const u8);
                defer {
                    for (values) |v| {
                        switch (v) {
                            .value => |s| allocator.free(s),
                            .null_value => {},
                        }
                    }
                    allocator.free(values);
                }
                for (0..num_rows) |i| {
                    result[i] = switch (values[i]) {
                        .value => |v| try formatStringValue(allocator, v),
                        .null_value => try allocator.dupe(u8, "null"),
                    };
                }
            },
            .int96 => {
                // INT96 is a legacy 12-byte timestamp format (nanoseconds since epoch)
                const values = reader.readColumn(col_idx, parquet.Int96) catch |err| {
                    for (0..num_rows) |i| {
                        result[i] = try std.fmt.allocPrint(allocator, "({s})", .{@errorName(err)});
                    }
                    return result;
                };
                defer allocator.free(values);
                for (0..num_rows) |i| {
                    result[i] = switch (values[i]) {
                        .value => |v| try std.fmt.allocPrint(allocator, "{}", .{v.toNanos()}),
                        .null_value => try allocator.dupe(u8, "null"),
                    };
                }
            },
        }
    } else {
        for (0..num_rows) |i| {
            result[i] = try allocator.dupe(u8, "(unknown)");
        }
    }

    return result;
}

fn formatStringValue(allocator: std.mem.Allocator, data: []const u8) ![]const u8 {
    // Check if it's printable text
    var is_printable = true;
    for (data) |c| {
        if (c < 32 and c != '\t' and c != '\n' and c != '\r') {
            is_printable = false;
            break;
        }
    }

    if (is_printable) {
        // Truncate long strings
        if (data.len > 50) {
            return try std.fmt.allocPrint(allocator, "{s}...", .{data[0..47]});
        }
        return try allocator.dupe(u8, data);
    } else {
        // Show as hex for binary data
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
