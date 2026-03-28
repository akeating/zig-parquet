//! Multi-page Writer/Reader Tests
//!
//! Comprehensive tests for multi-page support across all column types.
//! These tests verify that data written with row group size constraints
//! can be correctly read back, using the DynamicWriter/DynamicReader APIs.

const std = @import("std");
const parquet = @import("../lib.zig");
const DynamicWriter = parquet.DynamicWriter;
const DynamicReader = parquet.DynamicReader;
const TypeInfo = parquet.TypeInfo;
const Value = parquet.Value;
const Row = parquet.Row;
const SchemaNode = parquet.SchemaNode;
const build_options = @import("build_options");

fn readAllRowsAllGroups(allocator: std.mem.Allocator, reader: *DynamicReader) ![]Row {
    const num_groups = reader.getNumRowGroups();
    var all_rows: std.ArrayListUnmanaged(Row) = .empty;
    errdefer {
        for (all_rows.items) |r| r.deinit();
        all_rows.deinit(allocator);
    }
    for (0..num_groups) |rg| {
        const rows = try reader.readAllRows(rg);
        defer allocator.free(rows);
        for (rows) |r| {
            try all_rows.append(allocator, r);
        }
    }
    return all_rows.toOwnedSlice(allocator);
}

// =============================================================================
// Plain Byte Array Multi-page Tests
// =============================================================================

test "multi-page: plain byte array column" {
    const allocator = std.testing.allocator;
    const row_count = 200;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", TypeInfo.int32, .{});
    try writer.addColumn("name", TypeInfo.string, .{});
    writer.setRowGroupSize(50);
    try writer.begin();

    for (0..row_count) |i| {
        const name = try std.fmt.allocPrint(allocator, "name_{d:0>5}", .{i});
        defer allocator.free(name);
        try writer.setInt32(0, @intCast(i));
        try writer.setBytes(1, name);
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, row_count), reader.getTotalNumRows());

    const rows = try readAllRowsAllGroups(allocator, &reader);
    defer {
        for (rows) |r| r.deinit();
        allocator.free(rows);
    }
    try std.testing.expectEqual(@as(usize, row_count), rows.len);

    for (rows, 0..) |row, count| {
        try std.testing.expectEqual(@as(i32, @intCast(count)), row.getColumn(0).?.asInt32().?);
        const expected = try std.fmt.allocPrint(allocator, "name_{d:0>5}", .{count});
        defer allocator.free(expected);
        try std.testing.expectEqualStrings(expected, row.getColumn(1).?.asBytes().?);
    }
}

test "multi-page: nullable byte array column" {
    const allocator = std.testing.allocator;
    const row_count = 200;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", TypeInfo.int32, .{});
    try writer.addColumn("name", TypeInfo.string, .{});
    writer.setRowGroupSize(50);
    try writer.begin();

    for (0..row_count) |i| {
        try writer.setInt32(0, @intCast(i));
        if (i % 3 == 0) {
            try writer.setNull(1);
        } else {
            const name = try std.fmt.allocPrint(allocator, "name_{d}", .{i});
            defer allocator.free(name);
            try writer.setBytes(1, name);
        }
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, row_count), reader.getTotalNumRows());

    const rows = try readAllRowsAllGroups(allocator, &reader);
    defer {
        for (rows) |r| r.deinit();
        allocator.free(rows);
    }
    try std.testing.expectEqual(@as(usize, row_count), rows.len);

    for (rows, 0..) |row, count| {
        try std.testing.expectEqual(@as(i32, @intCast(count)), row.getColumn(0).?.asInt32().?);
        if (count % 3 == 0) {
            try std.testing.expect(row.getColumn(1).?.isNull());
        } else {
            const expected = try std.fmt.allocPrint(allocator, "name_{d}", .{count});
            defer allocator.free(expected);
            try std.testing.expectEqualStrings(expected, row.getColumn(1).?.asBytes().?);
        }
    }
}

// =============================================================================
// Plain List Multi-page Tests
// =============================================================================

test "multi-page: plain list column" {
    const allocator = std.testing.allocator;
    const row_count = 100;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", TypeInfo.int32, .{});
    const element_node = try writer.allocSchemaNode(.{ .int32 = .{} });
    const list_node = try writer.allocSchemaNode(.{ .list = element_node });
    try writer.addColumnNested("values", list_node, .{});
    writer.setRowGroupSize(25);
    try writer.begin();

    for (0..row_count) |i| {
        try writer.setInt32(0, @intCast(i));
        const list_size = (i % 5) + 1;
        try writer.beginList(1);
        for (0..list_size) |j| {
            try writer.appendNestedValue(1, .{ .int32_val = @intCast(i * 10 + j) });
        }
        try writer.endList(1);
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try readAllRowsAllGroups(allocator, &reader);
    defer {
        for (rows) |r| r.deinit();
        allocator.free(rows);
    }
    try std.testing.expectEqual(@as(usize, row_count), rows.len);

    for (rows, 0..) |row, count| {
        try std.testing.expectEqual(@as(i32, @intCast(count)), row.getColumn(0).?.asInt32().?);
        const list = row.getColumn(1).?.asList().?;
        const expected_size = (count % 5) + 1;
        try std.testing.expectEqual(expected_size, list.len);
        for (list, 0..) |v, j| {
            try std.testing.expectEqual(@as(i32, @intCast(count * 10 + j)), v.asInt32().?);
        }
    }
}

test "multi-page: nullable list column" {
    const allocator = std.testing.allocator;
    const row_count = 100;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", TypeInfo.int32, .{});
    const element_node = try writer.allocSchemaNode(.{ .int32 = .{} });
    const list_node = try writer.allocSchemaNode(.{ .list = element_node });
    const opt_node = try writer.allocSchemaNode(.{ .optional = list_node });
    try writer.addColumnNested("values", opt_node, .{});
    writer.setRowGroupSize(25);
    try writer.begin();

    for (0..row_count) |i| {
        try writer.setInt32(0, @intCast(i));
        if (i % 4 == 0) {
            try writer.setNull(1);
        } else {
            const list_size = (i % 3) + 1;
            try writer.beginList(1);
            for (0..list_size) |j| {
                try writer.appendNestedValue(1, .{ .int32_val = @intCast(i * 10 + j) });
            }
            try writer.endList(1);
        }
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try readAllRowsAllGroups(allocator, &reader);
    defer {
        for (rows) |r| r.deinit();
        allocator.free(rows);
    }
    try std.testing.expectEqual(@as(usize, row_count), rows.len);

    for (rows, 0..) |row, count| {
        try std.testing.expectEqual(@as(i32, @intCast(count)), row.getColumn(0).?.asInt32().?);
        const val = row.getColumn(1).?;
        if (count % 4 == 0) {
            try std.testing.expect(val.isNull());
        } else {
            const list = val.asList().?;
            const expected_size = (count % 3) + 1;
            try std.testing.expectEqual(expected_size, list.len);
        }
    }
}

// =============================================================================
// List-of-struct Multi-page Tests
// =============================================================================

test "multi-page: list-of-struct column" {
    const allocator = std.testing.allocator;
    const row_count = 3;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", TypeInfo.int32, .{});

    const struct_fields = try writer.allocSchemaFields(2);
    const x_name = try writer.dupeSchemaName("x");
    const y_name = try writer.dupeSchemaName("y");
    const x_node = try writer.allocSchemaNode(.{ .int32 = .{} });
    const y_node = try writer.allocSchemaNode(.{ .int32 = .{} });
    struct_fields[0] = .{ .name = x_name, .node = x_node };
    struct_fields[1] = .{ .name = y_name, .node = y_node };
    const struct_node = try writer.allocSchemaNode(.{ .struct_ = .{ .fields = struct_fields } });
    const list_node = try writer.allocSchemaNode(.{ .list = struct_node });
    try writer.addColumnNested("points", list_node, .{});
    try writer.begin();

    // Row 0: [{1,10}, {2,20}]
    try writer.setInt32(0, 0);
    try writer.beginList(1);
    try writer.beginStruct(1);
    try writer.setStructField(1, 0, .{ .int32_val = 1 });
    try writer.setStructField(1, 1, .{ .int32_val = 10 });
    try writer.endStruct(1);
    try writer.beginStruct(1);
    try writer.setStructField(1, 0, .{ .int32_val = 2 });
    try writer.setStructField(1, 1, .{ .int32_val = 20 });
    try writer.endStruct(1);
    try writer.endList(1);
    try writer.addRow();

    // Row 1: [{3,30}]
    try writer.setInt32(0, 1);
    try writer.beginList(1);
    try writer.beginStruct(1);
    try writer.setStructField(1, 0, .{ .int32_val = 3 });
    try writer.setStructField(1, 1, .{ .int32_val = 30 });
    try writer.endStruct(1);
    try writer.endList(1);
    try writer.addRow();

    // Row 2: [{4,40}, {5,50}, {6,60}]
    try writer.setInt32(0, 2);
    try writer.beginList(1);
    try writer.beginStruct(1);
    try writer.setStructField(1, 0, .{ .int32_val = 4 });
    try writer.setStructField(1, 1, .{ .int32_val = 40 });
    try writer.endStruct(1);
    try writer.beginStruct(1);
    try writer.setStructField(1, 0, .{ .int32_val = 5 });
    try writer.setStructField(1, 1, .{ .int32_val = 50 });
    try writer.endStruct(1);
    try writer.beginStruct(1);
    try writer.setStructField(1, 0, .{ .int32_val = 6 });
    try writer.setStructField(1, 1, .{ .int32_val = 60 });
    try writer.endStruct(1);
    try writer.endList(1);
    try writer.addRow();

    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, row_count), reader.getTotalNumRows());

    const rows = try readAllRowsAllGroups(allocator, &reader);
    defer {
        for (rows) |r| r.deinit();
        allocator.free(rows);
    }
    try std.testing.expectEqual(@as(usize, row_count), rows.len);

    // Row 0: [{1,10}, {2,20}]
    try std.testing.expectEqual(@as(i32, 0), rows[0].getColumn(0).?.asInt32().?);
    const pts0 = rows[0].getColumn(1).?.asList().?;
    try std.testing.expectEqual(@as(usize, 2), pts0.len);
    try std.testing.expectEqual(@as(i32, 1), pts0[0].getField("x").?.asInt32().?);
    try std.testing.expectEqual(@as(i32, 10), pts0[0].getField("y").?.asInt32().?);
    try std.testing.expectEqual(@as(i32, 2), pts0[1].getField("x").?.asInt32().?);
    try std.testing.expectEqual(@as(i32, 20), pts0[1].getField("y").?.asInt32().?);

    // Row 1: [{3,30}]
    try std.testing.expectEqual(@as(i32, 1), rows[1].getColumn(0).?.asInt32().?);
    const pts1 = rows[1].getColumn(1).?.asList().?;
    try std.testing.expectEqual(@as(usize, 1), pts1.len);
    try std.testing.expectEqual(@as(i32, 3), pts1[0].getField("x").?.asInt32().?);
    try std.testing.expectEqual(@as(i32, 30), pts1[0].getField("y").?.asInt32().?);

    // Row 2: [{4,40}, {5,50}, {6,60}]
    try std.testing.expectEqual(@as(i32, 2), rows[2].getColumn(0).?.asInt32().?);
    const pts2 = rows[2].getColumn(1).?.asList().?;
    try std.testing.expectEqual(@as(usize, 3), pts2.len);
    try std.testing.expectEqual(@as(i32, 4), pts2[0].getField("x").?.asInt32().?);
    try std.testing.expectEqual(@as(i32, 40), pts2[0].getField("y").?.asInt32().?);
}

// =============================================================================
// Compression + Multi-page Tests
// =============================================================================

test "multi-page: with zstd compression" {
    if (!build_options.enable_zstd) return;
    const allocator = std.testing.allocator;
    const row_count = 500;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", TypeInfo.int64, .{});
    try writer.addColumn("value", TypeInfo.int64, .{});
    writer.setCompression(.zstd);
    writer.setRowGroupSize(100);
    try writer.begin();

    for (0..row_count) |i| {
        try writer.setInt64(0, @intCast(i));
        try writer.setInt64(1, @intCast(i * 2));
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, row_count), reader.getTotalNumRows());

    const rows = try readAllRowsAllGroups(allocator, &reader);
    defer {
        for (rows) |r| r.deinit();
        allocator.free(rows);
    }
    try std.testing.expectEqual(@as(usize, row_count), rows.len);

    for (rows, 0..) |row, count| {
        try std.testing.expectEqual(@as(i64, @intCast(count)), row.getColumn(0).?.asInt64().?);
        try std.testing.expectEqual(@as(i64, @intCast(count * 2)), row.getColumn(1).?.asInt64().?);
    }
}

test "multi-page: list with snappy compression" {
    if (!build_options.enable_snappy) return;
    const allocator = std.testing.allocator;
    const row_count = 100;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", TypeInfo.int32, .{});
    const element_node = try writer.allocSchemaNode(.{ .int32 = .{} });
    const list_node = try writer.allocSchemaNode(.{ .list = element_node });
    try writer.addColumnNested("values", list_node, .{});
    writer.setCompression(.snappy);
    writer.setRowGroupSize(25);
    try writer.begin();

    for (0..row_count) |i| {
        try writer.setInt32(0, @intCast(i));
        const list_size = (i % 5) + 1;
        try writer.beginList(1);
        for (0..list_size) |j| {
            try writer.appendNestedValue(1, .{ .int32_val = @intCast(i * 10 + j) });
        }
        try writer.endList(1);
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try readAllRowsAllGroups(allocator, &reader);
    defer {
        for (rows) |r| r.deinit();
        allocator.free(rows);
    }
    try std.testing.expectEqual(@as(usize, row_count), rows.len);

    for (rows, 0..) |row, count| {
        try std.testing.expectEqual(@as(i32, @intCast(count)), row.getColumn(0).?.asInt32().?);
    }
}

// =============================================================================
// Edge Case Tests
// =============================================================================

test "multi-page: single value per page" {
    const allocator = std.testing.allocator;
    const row_count = 50;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", TypeInfo.int64, .{});
    writer.setRowGroupSize(1);
    try writer.begin();

    for (0..row_count) |i| {
        try writer.setInt64(0, @intCast(i));
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, row_count), reader.getTotalNumRows());

    const rows = try readAllRowsAllGroups(allocator, &reader);
    defer {
        for (rows) |r| r.deinit();
        allocator.free(rows);
    }
    try std.testing.expectEqual(@as(usize, row_count), rows.len);

    for (rows, 0..) |row, count| {
        try std.testing.expectEqual(@as(i64, @intCast(count)), row.getColumn(0).?.asInt64().?);
    }
}

test "multi-page: all nulls in nullable column" {
    const allocator = std.testing.allocator;
    const row_count = 100;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", TypeInfo.int32, .{});
    try writer.addColumn("value", TypeInfo.int64, .{});
    writer.setRowGroupSize(20);
    try writer.begin();

    for (0..row_count) |i| {
        try writer.setInt32(0, @intCast(i));
        try writer.setNull(1);
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try readAllRowsAllGroups(allocator, &reader);
    defer {
        for (rows) |r| r.deinit();
        allocator.free(rows);
    }
    try std.testing.expectEqual(@as(usize, row_count), rows.len);

    for (rows, 0..) |row, count| {
        try std.testing.expectEqual(@as(i32, @intCast(count)), row.getColumn(0).?.asInt32().?);
        try std.testing.expect(row.getColumn(1).?.isNull());
    }
}

test "multi-page: empty lists" {
    const allocator = std.testing.allocator;
    const row_count = 50;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", TypeInfo.int32, .{});
    const element_node = try writer.allocSchemaNode(.{ .int32 = .{} });
    const list_node = try writer.allocSchemaNode(.{ .list = element_node });
    try writer.addColumnNested("values", list_node, .{});
    writer.setRowGroupSize(10);
    try writer.begin();

    for (0..row_count) |i| {
        try writer.setInt32(0, @intCast(i));
        try writer.beginList(1);
        try writer.endList(1);
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try readAllRowsAllGroups(allocator, &reader);
    defer {
        for (rows) |r| r.deinit();
        allocator.free(rows);
    }
    try std.testing.expectEqual(@as(usize, row_count), rows.len);

    for (rows, 0..) |row, count| {
        try std.testing.expectEqual(@as(i32, @intCast(count)), row.getColumn(0).?.asInt32().?);
        const col = row.getColumn(1).?;
        if (col.asList()) |list| {
            try std.testing.expectEqual(@as(usize, 0), list.len);
        } else {
            try std.testing.expect(col.isNull());
        }
    }
}

test "multi-page: mixed empty and non-empty lists" {
    const allocator = std.testing.allocator;
    const row_count = 100;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", TypeInfo.int32, .{});
    const element_node = try writer.allocSchemaNode(.{ .int32 = .{} });
    const list_node = try writer.allocSchemaNode(.{ .list = element_node });
    try writer.addColumnNested("values", list_node, .{});
    writer.setRowGroupSize(25);
    try writer.begin();

    for (0..row_count) |i| {
        try writer.setInt32(0, @intCast(i));
        try writer.beginList(1);
        if (i % 2 != 0) {
            try writer.appendNestedValue(1, .{ .int32_val = @intCast(i) });
            try writer.appendNestedValue(1, .{ .int32_val = @intCast(i + 1) });
            try writer.appendNestedValue(1, .{ .int32_val = @intCast(i + 2) });
        }
        try writer.endList(1);
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    const rows = try readAllRowsAllGroups(allocator, &reader);
    defer {
        for (rows) |r| r.deinit();
        allocator.free(rows);
    }
    try std.testing.expectEqual(@as(usize, row_count), rows.len);

    for (rows, 0..) |row, count| {
        try std.testing.expectEqual(@as(i32, @intCast(count)), row.getColumn(0).?.asInt32().?);
        const col = row.getColumn(1).?;
        if (count % 2 == 0) {
            if (col.asList()) |list| {
                try std.testing.expectEqual(@as(usize, 0), list.len);
            } else {
                try std.testing.expect(col.isNull());
            }
        } else {
            const list = col.asList().?;
            try std.testing.expectEqual(@as(usize, 3), list.len);
            try std.testing.expectEqual(@as(i32, @intCast(count)), list[0].asInt32().?);
        }
    }
}

// =============================================================================
// Nested List Multi-page Tests
// =============================================================================

test "multi-page: nested list (list of list)" {
    const allocator = std.testing.allocator;
    const row_count = 50;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", TypeInfo.int32, .{});
    const inner_element = try writer.allocSchemaNode(.{ .int32 = .{} });
    const inner_list = try writer.allocSchemaNode(.{ .list = inner_element });
    const outer_list = try writer.allocSchemaNode(.{ .list = inner_list });
    try writer.addColumnNested("matrix", outer_list, .{});
    writer.setRowGroupSize(10);
    try writer.begin();

    for (0..row_count) |i| {
        try writer.setInt32(0, @intCast(i));
        const outer_size = (i % 3) + 1;

        try writer.beginList(1);
        for (0..outer_size) |j| {
            const inner_size = ((i + j) % 3) + 1;
            try writer.beginList(1);
            for (0..inner_size) |k| {
                try writer.appendNestedValue(1, .{ .int32_val = @intCast(i * 100 + j * 10 + k) });
            }
            try writer.endList(1);
        }
        try writer.endList(1);
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, row_count), reader.getTotalNumRows());

    const rows = try readAllRowsAllGroups(allocator, &reader);
    defer {
        for (rows) |r| r.deinit();
        allocator.free(rows);
    }
    try std.testing.expectEqual(@as(usize, row_count), rows.len);

    for (rows, 0..) |row, count| {
        try std.testing.expectEqual(@as(i32, @intCast(count)), row.getColumn(0).?.asInt32().?);
        const outer = row.getColumn(1).?.asList().?;
        const expected_outer = (count % 3) + 1;
        try std.testing.expectEqual(expected_outer, outer.len);
    }
}

// =============================================================================
// Large Data Multi-page Tests
// =============================================================================

test "multi-page: large dataset (10K rows)" {
    if (!build_options.enable_zstd) return;
    const allocator = std.testing.allocator;
    const row_count = 10_000;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", TypeInfo.int64, .{});
    try writer.addColumn("a", TypeInfo.int32, .{});
    try writer.addColumn("b", TypeInfo.int32, .{});
    try writer.addColumn("c", TypeInfo.int64, .{});
    writer.setCompression(.zstd);
    writer.setRowGroupSize(1000);
    try writer.begin();

    for (0..row_count) |i| {
        try writer.setInt64(0, @intCast(i));
        try writer.setInt32(1, @intCast(i % 1000));
        try writer.setInt32(2, @intCast(i % 500));
        try writer.setInt64(3, @intCast(i * 3));
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, row_count), reader.getTotalNumRows());

    const rows = try readAllRowsAllGroups(allocator, &reader);
    defer {
        for (rows) |r| r.deinit();
        allocator.free(rows);
    }
    try std.testing.expectEqual(@as(usize, row_count), rows.len);

    // Verify first, middle, and last rows
    try std.testing.expectEqual(@as(i64, 0), rows[0].getColumn(0).?.asInt64().?);
    try std.testing.expectEqual(@as(i64, 4999), rows[4999].getColumn(0).?.asInt64().?);
    try std.testing.expectEqual(@as(i64, 9999), rows[9999].getColumn(0).?.asInt64().?);
}

// =============================================================================
// Multi-page Map Tests
// =============================================================================

test "multi-page: map column" {
    const allocator = std.testing.allocator;
    const row_count = 50;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", TypeInfo.int32, .{});
    const key_node = try writer.allocSchemaNode(.{ .byte_array = .{ .logical = .string } });
    const val_node = try writer.allocSchemaNode(.{ .int32 = .{} });
    const map_node = try writer.allocSchemaNode(.{ .map = .{ .key = key_node, .value = val_node } });
    try writer.addColumnNested("data", map_node, .{});
    writer.setRowGroupSize(10);
    try writer.begin();

    for (0..row_count) |i| {
        try writer.setInt32(0, @intCast(i));
        const entry_count = (i % 4) + 1;
        try writer.beginMap(1);
        for (0..entry_count) |j| {
            try writer.beginMapEntry(1);
            const key = try std.fmt.allocPrint(allocator, "k{d}_{d}", .{ i, j });
            defer allocator.free(key);
            try writer.appendNestedBytes(1, key);
            try writer.appendNestedValue(1, .{ .int32_val = @intCast(i * 10 + j) });
            try writer.endMapEntry(1);
        }
        try writer.endMap(1);
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, row_count), reader.getTotalNumRows());

    const rows = try readAllRowsAllGroups(allocator, &reader);
    defer {
        for (rows) |r| r.deinit();
        allocator.free(rows);
    }
    try std.testing.expectEqual(@as(usize, row_count), rows.len);

    for (rows, 0..) |row, count| {
        try std.testing.expectEqual(@as(i32, @intCast(count)), row.getColumn(0).?.asInt32().?);
        const map = row.getColumn(1).?.asMap().?;
        const expected_count = (count % 4) + 1;
        try std.testing.expectEqual(expected_count, map.len);
        for (map, 0..) |entry, j| {
            const expected_key = try std.fmt.allocPrint(allocator, "k{d}_{d}", .{ count, j });
            defer allocator.free(expected_key);
            try std.testing.expectEqualStrings(expected_key, entry.key.asBytes().?);
            try std.testing.expectEqual(@as(i32, @intCast(count * 10 + j)), entry.value.asInt32().?);
        }
    }
}

// =============================================================================
// Multi-page Struct with List Tests
// =============================================================================

test "multi-page: struct with list field" {
    const allocator = std.testing.allocator;
    const row_count = 50;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();

    try writer.addColumn("id", TypeInfo.int32, .{});

    const label_node = try writer.allocSchemaNode(.{ .byte_array = .{ .logical = .string } });
    const tag_element = try writer.allocSchemaNode(.{ .int32 = .{} });
    const tags_list = try writer.allocSchemaNode(.{ .list = tag_element });
    const fields = try writer.allocSchemaFields(2);
    const label_name = try writer.dupeSchemaName("label");
    const tags_name = try writer.dupeSchemaName("tags");
    fields[0] = .{ .name = label_name, .node = label_node };
    fields[1] = .{ .name = tags_name, .node = tags_list };
    const struct_node = try writer.allocSchemaNode(.{ .struct_ = .{ .fields = fields } });
    try writer.addColumnNested("inner", struct_node, .{});
    writer.setRowGroupSize(10);
    try writer.begin();

    for (0..row_count) |i| {
        try writer.setInt32(0, @intCast(i));

        // Build the tags list value first (can't nest list inside struct builder)
        const tag_count = (i % 3) + 1;
        try writer.beginList(1);
        for (0..tag_count) |j| {
            try writer.appendNestedValue(1, .{ .int32_val = @intCast(i * 100 + j) });
        }
        try writer.endList(1);
        const tags_val = writer.current_row[1];

        // Now build the struct with label and the pre-built tags list
        try writer.beginStruct(1);
        const label = try std.fmt.allocPrint(allocator, "label_{d}", .{i});
        defer allocator.free(label);
        try writer.setStructFieldBytes(1, 0, label);
        try writer.setStructField(1, 1, tags_val);
        try writer.endStruct(1);
        try writer.addRow();
    }
    try writer.close();

    const buffer = try writer.toOwnedSlice();
    defer allocator.free(buffer);

    var reader = try parquet.openBufferDynamic(allocator, buffer, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, row_count), reader.getTotalNumRows());

    const rows = try readAllRowsAllGroups(allocator, &reader);
    defer {
        for (rows) |r| r.deinit();
        allocator.free(rows);
    }
    try std.testing.expectEqual(@as(usize, row_count), rows.len);

    for (rows, 0..) |row, count| {
        try std.testing.expectEqual(@as(i32, @intCast(count)), row.getColumn(0).?.asInt32().?);
        const s = row.getColumn(1).?;
        const expected_label = try std.fmt.allocPrint(allocator, "label_{d}", .{count});
        defer allocator.free(expected_label);
        try std.testing.expectEqualStrings(expected_label, s.getField("label").?.asBytes().?);
        const tags = s.getField("tags").?.asList().?;
        const expected_tags = (count % 3) + 1;
        try std.testing.expectEqual(expected_tags, tags.len);
        for (tags, 0..) |tag, j| {
            try std.testing.expectEqual(@as(i32, @intCast(count * 100 + j)), tag.asInt32().?);
        }
    }
}
