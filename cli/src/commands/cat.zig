//! Cat command - outputs all rows of a parquet file

const std = @import("std");
const parquet = @import("parquet");
const safe = parquet.safe;

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

    // Create dynamic reader — handles all types including INT96 and nested
    var reader = parquet.openFileDynamic(allocator, file, .{}) catch |err| {
        try stderr.print("Error: Cannot read parquet file: {}\n", .{err});
        try stderr.flush();
        std.process.exit(1);
    };
    defer reader.deinit();

    const column_names = try getTopLevelColumnNames(allocator, reader.getSchema());
    defer allocator.free(column_names);

    if (json_mode) {
        try outputJson(allocator, &reader, column_names, stdout);
    } else {
        try outputTable(allocator, &reader, column_names, stdout);
    }

    try stdout.flush();
}

fn outputJson(
    allocator: std.mem.Allocator,
    reader: *parquet.DynamicReader,
    column_names: []const []const u8,
    writer: *std.Io.Writer,
) !void {
    _ = allocator;
    var iter = reader.rowIterator();
    defer iter.deinit();

    while (try iter.next()) |row| {
        try writer.writeAll("{");
        for (row.values, 0..) |val, col_idx| {
            if (col_idx > 0) try writer.writeAll(",");
            if (col_idx < column_names.len) {
                try writeJsonString(writer, column_names[col_idx]);
            } else {
                try writer.print("\"col{d}\"", .{col_idx});
            }
            try writer.writeAll(":");
            try writeJsonValue(writer, val);
        }
        try writer.writeAll("}\n");
    }
}

fn outputTable(
    allocator: std.mem.Allocator,
    reader: *parquet.DynamicReader,
    column_names: []const []const u8,
    writer: *std.Io.Writer,
) !void {
    const total_rows: usize = safe.cast(reader.getTotalNumRows()) catch 0;

    if (total_rows == 0) {
        try writer.writeAll("(empty table)\n");
        return;
    }

    // Buffer all rows as formatted strings for column width calculation
    var all_rows: std.ArrayListUnmanaged([][]const u8) = .empty;
    defer {
        for (all_rows.items) |row| {
            for (row) |cell| allocator.free(cell);
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

    while (try iter.next()) |row| {
        const num_cols = @min(row.values.len, column_names.len);
        var cells = try allocator.alloc([]const u8, column_names.len);
        for (0..column_names.len) |col_idx| {
            const cell = if (col_idx < num_cols)
                try formatCellValue(allocator, row.values[col_idx])
            else
                try allocator.dupe(u8, "null");
            cells[col_idx] = cell;
            col_widths[col_idx] = @max(col_widths[col_idx], cell.len);
        }
        try all_rows.append(allocator, cells);
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

// ============================================================================
// JSON formatting
// ============================================================================

fn writeJsonValue(writer: *std.Io.Writer, val: parquet.Value) !void {
    switch (val) {
        .null_val => try writer.writeAll("null"),
        .bool_val => |v| try writer.writeAll(if (v) "true" else "false"),
        .int32_val => |v| try writer.print("{}", .{v}),
        .int64_val => |v| try writer.print("{}", .{v}),
        .float_val => |v| try writeJsonFloat(f32, v, writer),
        .double_val => |v| try writeJsonFloat(f64, v, writer),
        .bytes_val => |v| try writeJsonString(writer, v),
        .fixed_bytes_val => |v| {
            if (isPrintable(v)) {
                try writeJsonString(writer, v);
            } else {
                try writeJsonHex(writer, v);
            }
        },
        .list_val => |items| {
            try writer.writeAll("[");
            for (items, 0..) |item, i| {
                if (i > 0) try writer.writeAll(",");
                try writeJsonValue(writer, item);
            }
            try writer.writeAll("]");
        },
        .map_val => |entries| {
            try writer.writeAll("{");
            for (entries, 0..) |entry, i| {
                if (i > 0) try writer.writeAll(",");
                if (entry.key.asBytes()) |k| {
                    try writeJsonString(writer, k);
                } else {
                    try writer.writeAll("\"");
                    try writeJsonValue(writer, entry.key);
                    try writer.writeAll("\"");
                }
                try writer.writeAll(":");
                try writeJsonValue(writer, entry.value);
            }
            try writer.writeAll("}");
        },
        .struct_val => |fields| {
            try writer.writeAll("{");
            for (fields, 0..) |field, i| {
                if (i > 0) try writer.writeAll(",");
                try writeJsonString(writer, field.name);
                try writer.writeAll(":");
                try writeJsonValue(writer, field.value);
            }
            try writer.writeAll("}");
        },
    }
}

fn writeJsonFloat(comptime T: type, v: T, writer: *std.Io.Writer) !void {
    if (std.math.isInf(v)) {
        try writer.writeAll(if (v < 0) "\"-inf\"" else "\"inf\"");
    } else if (std.math.isNan(v)) {
        try writer.writeAll("\"nan\"");
    } else {
        try writer.print("{d}", .{v});
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

fn writeJsonHex(writer: *std.Io.Writer, data: []const u8) !void {
    try writer.writeAll("\"");
    for (data) |b| {
        try writer.print("{x:0>2}", .{b});
    }
    try writer.writeAll("\"");
}

// ============================================================================
// Table cell formatting
// ============================================================================

pub fn formatCellValue(allocator: std.mem.Allocator, val: parquet.Value) ![]const u8 {
    return switch (val) {
        .null_val => try allocator.dupe(u8, "null"),
        .bool_val => |v| try allocator.dupe(u8, if (v) "true" else "false"),
        .int32_val => |v| try std.fmt.allocPrint(allocator, "{}", .{v}),
        .int64_val => |v| try std.fmt.allocPrint(allocator, "{}", .{v}),
        .float_val => |v| try formatFloat(allocator, f32, v),
        .double_val => |v| try formatFloat(allocator, f64, v),
        .bytes_val => |v| try formatStringForTable(allocator, v),
        .fixed_bytes_val => |v| try formatStringForTable(allocator, v),
        .list_val, .map_val, .struct_val => blk: {
            var buf: std.ArrayListUnmanaged(u8) = .empty;
            errdefer buf.deinit(allocator);
            val.format("", .{}, buf.writer(allocator)) catch |e| switch (e) {
                error.OutOfMemory => return error.OutOfMemory,
            };
            break :blk try buf.toOwnedSlice(allocator);
        },
    };
}

fn formatFloat(allocator: std.mem.Allocator, comptime T: type, v: T) ![]const u8 {
    if (std.math.isInf(v)) {
        return try allocator.dupe(u8, if (v < 0) "-inf" else "inf");
    } else if (std.math.isNan(v)) {
        return try allocator.dupe(u8, "nan");
    }
    return try std.fmt.allocPrint(allocator, "{d}", .{v});
}

fn formatStringForTable(allocator: std.mem.Allocator, data: []const u8) ![]const u8 {
    if (!isPrintable(data)) {
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

    if (data.len > 50) {
        return try std.fmt.allocPrint(allocator, "{s}...", .{data[0..47]});
    }
    return try allocator.dupe(u8, data);
}

fn isPrintable(data: []const u8) bool {
    for (data) |c| {
        if (c < 32 and c != '\t' and c != '\n' and c != '\r') return false;
    }
    return true;
}

// ============================================================================
// Schema helpers
// ============================================================================

pub fn getTopLevelColumnNames(
    allocator: std.mem.Allocator,
    schema_elements: []const parquet.format.SchemaElement,
) ![]const []const u8 {
    if (schema_elements.len == 0) return try allocator.alloc([]const u8, 0);

    const root = schema_elements[0];
    const num_children_raw = root.num_children orelse {
        // Flat schema: all leaf elements are top-level columns
        var count: usize = 0;
        for (schema_elements[1..]) |elem| {
            if (elem.type_ != null and elem.num_children == null) count += 1;
        }
        var names = try allocator.alloc([]const u8, count);
        var idx: usize = 0;
        for (schema_elements[1..]) |elem| {
            if (elem.type_ != null and elem.num_children == null) {
                names[idx] = elem.name;
                idx += 1;
            }
        }
        return names;
    };

    const num_children = safe.cast(num_children_raw) catch return try allocator.alloc([]const u8, 0);
    var names = try allocator.alloc([]const u8, num_children);
    var schema_idx: usize = 1;
    for (0..num_children) |i| {
        if (schema_idx >= schema_elements.len) {
            allocator.free(names);
            return error.InvalidSchema;
        }
        names[i] = schema_elements[schema_idx].name;
        schema_idx = skipSchemaElement(schema_elements, schema_idx);
    }
    return names;
}

/// Advance past a schema element and all its descendants.
fn skipSchemaElement(schema: []const parquet.format.SchemaElement, idx: usize) usize {
    if (idx >= schema.len) return idx;
    const nc = schema[idx].num_children orelse return idx + 1;
    const children: usize = safe.cast(nc) catch return idx + 1;
    var next = idx + 1;
    for (0..children) |_| {
        next = skipSchemaElement(schema, next);
    }
    return next;
}
