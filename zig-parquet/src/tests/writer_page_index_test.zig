//! Page splitting and page indexes written by the typed `Writer` and by DynamicWriter's flat
//! boolean and fixed-length byte array columns, and the checks on values of nested leaves.
//!
//! Each paged file is compared with a single-page write of the same data, its offset indexes
//! are checked (two or more pages per leaf, first rows increasing from 0), and
//! `readRowsFiltered` with fixed row ranges must return exactly those rows.

const std = @import("std");
const io = std.testing.io;
const parquet = @import("../lib.zig");
const format = parquet.format;
const page_filter = parquet.page_filter;
const Value = parquet.Value;
const Row = parquet.Row;
const Optional = parquet.Optional;
const ColumnDef = parquet.ColumnDef;
const RowRange = @import("../core/row_ranges.zig").RowRange;
const MapEntry = @import("../core/map_encoder.zig").MapEntry;
const values_writer = @import("../core/column_write_values.zig");

// -- Helpers (copied from page_index_test.zig) --

fn expectValueEqual(expected: Value, actual: Value) !void {
    try std.testing.expectEqual(std.meta.activeTag(expected), std.meta.activeTag(actual));
    switch (expected) {
        .bytes_val => |x| try std.testing.expectEqualStrings(x, actual.bytes_val),
        .fixed_bytes_val => |x| try std.testing.expectEqualStrings(x, actual.fixed_bytes_val),
        .list_val => |x| {
            try std.testing.expectEqual(x.len, actual.list_val.len);
            for (x, actual.list_val) |e, v| try expectValueEqual(e, v);
        },
        .map_val => |x| {
            try std.testing.expectEqual(x.len, actual.map_val.len);
            for (x, actual.map_val) |e, v| {
                try expectValueEqual(e.key, v.key);
                try expectValueEqual(e.value, v.value);
            }
        },
        .struct_val => |x| {
            try std.testing.expectEqual(x.len, actual.struct_val.len);
            for (x, actual.struct_val) |e, v| {
                try std.testing.expectEqualStrings(e.name, v.name);
                try expectValueEqual(e.value, v.value);
            }
        },
        else => try std.testing.expectEqual(expected, actual),
    }
}

fn freeRows(allocator: std.mem.Allocator, rows: []Row) void {
    for (rows) |row| row.deinit();
    allocator.free(rows);
}

fn expectRowsEqual(want: []const Row, got: []const Row) !void {
    try std.testing.expectEqual(want.len, got.len);
    for (want, got) |w, g| {
        try std.testing.expectEqual(w.values.len, g.values.len);
        for (w.values, g.values) |e, v| try expectValueEqual(e, v);
    }
}

/// Checks that `readRowsFiltered(0, filters)` returns the rows of `readAllRows(0)` in
/// `expected` (row ranges, ascending).
fn expectFilteredRows(allocator: std.mem.Allocator, reader: *parquet.DynamicReader, filters: []const parquet.ColumnFilter, expected: []const RowRange) !void {
    const all = try reader.readAllRows(0);
    defer freeRows(allocator, all);
    const rows = try reader.readRowsFiltered(0, filters);
    defer freeRows(allocator, rows);

    var count: usize = 0;
    for (expected) |r| count += @intCast(r.last - r.first);
    try std.testing.expectEqual(count, rows.len);
    var i: usize = 0;
    for (expected) |r| {
        for (r.first..r.last) |row_idx| {
            const want = all[row_idx];
            const got = rows[i];
            try std.testing.expectEqual(want.values.len, got.values.len);
            for (want.values, got.values) |e, v| try expectValueEqual(e, v);
            i += 1;
        }
    }
}

/// A `raw` filter producer that returns the ranges in `ctx` (a `*const []const RowRange`).
fn fixedRanges(ctx: *anyopaque, ci: ?*const format.ColumnIndex, oi: *const format.OffsetIndex, rg_num_rows: u64, allocator: std.mem.Allocator) anyerror!page_filter.RowRanges {
    _ = ci;
    _ = oi;
    _ = rg_num_rows;
    const ranges: *const []const RowRange = @ptrCast(@alignCast(ctx));
    return page_filter.RowRanges.fromUnsorted(allocator, ranges.*);
}

fn rangesFilter(column: usize, ranges: *const []const RowRange) parquet.ColumnFilter {
    return page_filter.raw(column, fixedRanges, @ptrCast(@constCast(ranges)));
}

/// Checks that every leaf column of row group 0 has an offset index of two or more pages whose
/// first rows start at 0 and increase, and a column index with one entry per page.
fn expectWriterPages(allocator: std.mem.Allocator, reader: *parquet.DynamicReader) !void {
    const rg = &reader.metadata.row_groups[0];
    var pir = parquet.PageIndexReader.init(allocator, reader.getSource());
    for (rg.columns) |*chunk| {
        var oi = (try pir.readOffsetIndex(chunk)) orelse return error.TestExpectedOffsetIndex;
        defer oi.deinit(allocator);
        var ci = (try pir.readColumnIndex(chunk)) orelse return error.TestExpectedColumnIndex;
        defer ci.deinit(allocator);
        const pages = oi.page_locations;
        try std.testing.expect(pages.len >= 2);
        try std.testing.expectEqual(pages.len, ci.null_pages.len);
        try std.testing.expectEqual(@as(i64, 0), pages[0].first_row_index);
        for (pages[1..], pages[0 .. pages.len - 1]) |p, prev| {
            try std.testing.expect(p.first_row_index > prev.first_row_index);
            try std.testing.expectEqual(prev.offset + prev.compressed_page_size, p.offset);
        }
        try std.testing.expect(pages[pages.len - 1].first_row_index < rg.num_rows);
    }
}

/// Checks that no column chunk of row group 0 has a page index.
fn expectNoPageIndex(reader: *parquet.DynamicReader) !void {
    for (reader.metadata.row_groups[0].columns) |chunk| {
        try std.testing.expectEqual(@as(?i64, null), chunk.offset_index_offset);
        try std.testing.expectEqual(@as(?i64, null), chunk.column_index_offset);
    }
}

/// Row ranges inside pages and across them, for files of 300 rows.
const test_ranges: []const RowRange = &.{
    .{ .first = 0, .last = 1 },
    .{ .first = 17, .last = 90 },
    .{ .first = 151, .last = 152 },
    .{ .first = 250, .last = 300 },
};

/// Reads `single` and `paged`, checks the pages of `paged`, that both hold the same rows, and
/// that filtering each leaf column by `test_ranges` returns those rows.
fn expectPagedMatchesSingle(allocator: std.mem.Allocator, single: []const u8, paged: []const u8, num_rows: usize) !void {
    var single_reader = try parquet.openBufferDynamic(allocator, single, .{});
    defer single_reader.deinit();
    try expectNoPageIndex(&single_reader);
    const want = try single_reader.readAllRows(0);
    defer freeRows(allocator, want);
    try std.testing.expectEqual(num_rows, want.len);

    var reader = try parquet.openBufferDynamic(allocator, paged, .{});
    defer reader.deinit();
    try expectWriterPages(allocator, &reader);
    const got = try reader.readAllRows(0);
    defer freeRows(allocator, got);
    try expectRowsEqual(want, got);

    for (0..reader.metadata.row_groups[0].columns.len) |c| {
        try expectFilteredRows(allocator, &reader, &.{rangesFilter(c, &test_ranges)}, test_ranges);
    }
}

// -- Typed Writer --

const struct_fields = [_]parquet.StructField{
    .{ .name = "a", .type_ = .int64 },
    .{ .name = "b", .type_ = .byte_array, .logical_type = .string },
};

/// Leaf columns: 0 id, 1 big, 2 score, 3 ratio, 4 flag, 5 name, 6 code, 7 values.list.element,
/// 8 tags.list.element, 9 s.a, 10 s.b, 11 m.key_value.key, 12 m.key_value.value.
const typed_columns = [_]ColumnDef{
    .{ .name = "id", .type_ = .int32 },
    .{ .name = "big", .type_ = .int64, .optional = true },
    .{ .name = "score", .type_ = .double },
    .{ .name = "ratio", .type_ = .float },
    .{ .name = "flag", .type_ = .boolean, .optional = true },
    .{ .name = "name", .type_ = .byte_array, .optional = true, .logical_type = .string },
    .{ .name = "code", .type_ = .fixed_len_byte_array, .type_length = 4 },
    ColumnDef.list("values", .int32, true),
    .{ .name = "tags", .type_ = .byte_array, .is_list = true, .element_optional = false, .logical_type = .string },
    ColumnDef.struct_("s", &struct_fields, true),
    .{ .name = "m", .type_ = .byte_array, .optional = true, .is_map = true, .map_value_type = .int32, .logical_type = .string },
};

const TypedOptions = struct {
    max_page_size: ?usize = null,
    write_page_index: bool = false,
};

/// Writes `num_rows` rows of `typed_columns` with the typed Writer:
///   id = i; big = i * 1000, null when i % 5 == 0; score = i / 4; ratio = i * 0.5;
///   flag = i % 3 == 0, null when i % 11 == 0; name = "n{i}", null when i % 6 == 1;
///   code = 4 bytes of i;
///   values: null when i % 7 == 3, otherwise i % 4 items i * 10 + j, item j == 1 null;
///   tags: i % 3 items "t{i}_{j}" (required elements);
///   s: null when i % 5 == 2, otherwise a = i * 1000 and b = "b{i / 16}" (null when i % 3 == 0);
///   m: null when i % 9 == 4, otherwise i % 4 entries "k{i}_{j}" -> i * 10 + j (null when
///   j == 2).
fn writeTypedFile(allocator: std.mem.Allocator, num_rows: usize, options: TypedOptions) ![]u8 {
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var writer = try parquet.writeToBuffer(allocator, &typed_columns);
    defer writer.deinit();
    writer.max_page_size = options.max_page_size;
    writer.write_page_index = options.write_page_index;

    const ids = try arena.alloc(Optional(i32), num_rows);
    const bigs = try arena.alloc(Optional(i64), num_rows);
    const scores = try arena.alloc(Optional(f64), num_rows);
    const ratios = try arena.alloc(Optional(f32), num_rows);
    const flags = try arena.alloc(Optional(bool), num_rows);
    const names = try arena.alloc(Optional([]const u8), num_rows);
    const codes = try arena.alloc(Optional([]const u8), num_rows);
    const lists = try arena.alloc(Optional([]const Optional(i32)), num_rows);
    const tags = try arena.alloc(Optional([]const Optional([]const u8)), num_rows);
    const field_a = try arena.alloc(?i64, num_rows);
    const field_b = try arena.alloc(?[]const u8, num_rows);
    const parent_nulls = try arena.alloc(bool, num_rows);
    const maps = try arena.alloc(Optional([]const MapEntry([]const u8, i32)), num_rows);

    for (0..num_rows) |i| {
        const n: i32 = @intCast(i);
        ids[i] = .{ .value = n };
        bigs[i] = if (i % 5 == 0) .null_value else .{ .value = @as(i64, n) * 1000 };
        scores[i] = .{ .value = @as(f64, @floatFromInt(i / 4)) };
        ratios[i] = .{ .value = @as(f32, @floatFromInt(i)) * 0.5 };
        flags[i] = if (i % 11 == 0) .null_value else .{ .value = i % 3 == 0 };
        names[i] = if (i % 6 == 1) .null_value else .{ .value = try std.fmt.allocPrint(arena, "n{d}", .{i}) };
        const code = try arena.alloc(u8, 4);
        std.mem.writeInt(u32, code[0..4], @intCast(i), .big);
        codes[i] = .{ .value = code };

        if (i % 7 == 3) {
            lists[i] = .null_value;
        } else {
            const items = try arena.alloc(Optional(i32), i % 4);
            for (items, 0..) |*item, j| item.* = if (j == 1) .null_value else .{ .value = n * 10 + @as(i32, @intCast(j)) };
            lists[i] = .{ .value = items };
        }

        const tag_items = try arena.alloc(Optional([]const u8), i % 3);
        for (tag_items, 0..) |*item, j| item.* = .{ .value = try std.fmt.allocPrint(arena, "t{d}_{d}", .{ i, j }) };
        tags[i] = .{ .value = tag_items };

        parent_nulls[i] = i % 5 == 2;
        field_a[i] = @as(i64, n) * 1000;
        field_b[i] = if (i % 3 == 0) null else try std.fmt.allocPrint(arena, "b{d:0>3}", .{i / 16});

        if (i % 9 == 4) {
            maps[i] = .null_value;
        } else {
            const entries = try arena.alloc(MapEntry([]const u8, i32), i % 4);
            for (entries, 0..) |*e, j| e.* = .{
                .key = try std.fmt.allocPrint(arena, "k{d}_{d}", .{ i, j }),
                .value = if (j == 2) .null_value else .{ .value = n * 10 + @as(i32, @intCast(j)) },
            };
            maps[i] = .{ .value = entries };
        }
    }

    try writer.writeColumnOptional(i32, 0, ids);
    try writer.writeColumnOptional(i64, 1, bigs);
    try writer.writeColumnOptional(f64, 2, scores);
    try writer.writeColumnOptional(f32, 3, ratios);
    try writer.writeColumnOptional(bool, 4, flags);
    try writer.writeColumnOptional([]const u8, 5, names);
    try writer.writeColumnFixedByteArrayOptional(6, codes);
    try writer.writeListColumn(i32, 7, lists);
    try writer.writeListColumn([]const u8, 8, tags);
    try writer.writeStructField(i64, 9, 0, field_a, parent_nulls);
    try writer.writeStructField([]const u8, 9, 1, field_b, parent_nulls);
    try writer.writeMapColumn([]const u8, i32, 10, maps);
    try writer.close();
    return writer.toOwnedSlice();
}

test "writer page index: typed Writer splits flat, list, struct and map columns into indexed pages" {
    const allocator = std.testing.allocator;
    const num_rows = 300;
    const single = try writeTypedFile(allocator, num_rows, .{});
    defer allocator.free(single);
    // 32 bytes: 256 booleans, 8 int32 or 4 int64 values per page.
    const paged = try writeTypedFile(allocator, num_rows, .{ .max_page_size = 32, .write_page_index = true });
    defer allocator.free(paged);
    try expectPagedMatchesSingle(allocator, single, paged, num_rows);

    var reader = try parquet.openBufferDynamic(allocator, paged, .{});
    defer reader.deinit();
    try std.testing.expectEqual(@as(usize, 13), reader.metadata.row_groups[0].columns.len);
}

test "writer page index: typed Writer rows match the values written" {
    const allocator = std.testing.allocator;
    const bytes = try writeTypedFile(allocator, 30, .{ .max_page_size = 32, .write_page_index = true });
    defer allocator.free(bytes);
    var reader = try parquet.openBufferDynamic(allocator, bytes, .{});
    defer reader.deinit();
    const rows = try reader.readAllRows(0);
    defer freeRows(allocator, rows);

    // Row 11: big 11000, flag null, name "n11", values [110, null, 112], tags ["t11_0", "t11_1"],
    // s {a 11000, b "b000"}, m {"k11_0": 110, "k11_1": 111, "k11_2": null}.
    const row = rows[11].values;
    try expectValueEqual(.{ .int32_val = 11 }, row[0]);
    try expectValueEqual(.{ .int64_val = 11000 }, row[1]);
    try expectValueEqual(.{ .double_val = 2 }, row[2]);
    try expectValueEqual(.{ .float_val = 5.5 }, row[3]);
    try expectValueEqual(.null_val, row[4]);
    try expectValueEqual(.{ .bytes_val = "n11" }, row[5]);
    var code: [4]u8 = undefined;
    std.mem.writeInt(u32, &code, 11, .big);
    try expectValueEqual(.{ .fixed_bytes_val = &code }, row[6]);
    try expectValueEqual(.{ .list_val = &.{ .{ .int32_val = 110 }, .null_val, .{ .int32_val = 112 } } }, row[7]);
    try expectValueEqual(.{ .list_val = &.{ .{ .bytes_val = "t11_0" }, .{ .bytes_val = "t11_1" } } }, row[8]);
    try expectValueEqual(.{ .struct_val = &.{
        .{ .name = "a", .value = .{ .int64_val = 11000 } },
        .{ .name = "b", .value = .{ .bytes_val = "b000" } },
    } }, row[9]);
    try expectValueEqual(.{ .map_val = &.{
        .{ .key = .{ .bytes_val = "k11_0" }, .value = .{ .int32_val = 110 } },
        .{ .key = .{ .bytes_val = "k11_1" }, .value = .{ .int32_val = 111 } },
        .{ .key = .{ .bytes_val = "k11_2" }, .value = .null_val },
    } }, row[10]);
    // Row 3: values is null; row 7: name and s are null; row 13: m is null.
    try expectValueEqual(.null_val, rows[3].values[7]);
    try expectValueEqual(.null_val, rows[7].values[5]);
    try expectValueEqual(.null_val, rows[7].values[9]);
    try expectValueEqual(.null_val, rows[13].values[10]);
}

test "writer page index: typed Writer writes levels that match required list, struct and map fields" {
    const allocator = std.testing.allocator;
    const fields = [_]parquet.StructField{.{ .name = "a", .type_ = .int32, .optional = false }};
    const columns = [_]ColumnDef{
        .{ .name = "x", .type_ = .int32, .is_list = true, .element_optional = false },
        ColumnDef.struct_("s", &fields, false),
        .{ .name = "m", .type_ = .int32, .is_map = true, .map_value_type = .int64, .map_value_optional = false },
    };
    var writer = try parquet.writeToBuffer(allocator, &columns);
    defer writer.deinit();
    writer.max_page_size = 8;
    writer.write_page_index = true;
    const items = [_]Optional(i32){ .{ .value = 1 }, .{ .value = 2 } };
    try writer.writeListColumn(i32, 0, &.{ .{ .value = &items }, .{ .value = &.{} }, .{ .value = items[1..] } });
    try writer.writeStructField(i32, 1, 0, &.{ 5, 6, 7 }, &.{ false, false, false });
    const entries = [_]MapEntry(i32, i64){ .{ .key = 1, .value = .{ .value = 10 } }, .{ .key = 2, .value = .{ .value = 20 } } };
    try writer.writeMapColumn(i32, i64, 2, &.{ .{ .value = &entries }, .{ .value = &.{} }, .{ .value = entries[1..] } });
    try writer.close();
    const bytes = try writer.toOwnedSlice();
    defer allocator.free(bytes);

    var reader = try parquet.openBufferDynamic(allocator, bytes, .{});
    defer reader.deinit();
    try expectWriterPages(allocator, &reader);
    const rows = try reader.readAllRows(0);
    defer freeRows(allocator, rows);
    try std.testing.expectEqual(@as(usize, 3), rows.len);
    try expectValueEqual(.{ .list_val = &.{ .{ .int32_val = 1 }, .{ .int32_val = 2 } } }, rows[0].values[0]);
    try expectValueEqual(.{ .list_val = &.{} }, rows[1].values[0]);
    try expectValueEqual(.{ .list_val = &.{.{ .int32_val = 2 }} }, rows[2].values[0]);
    for (rows, [_]i32{ 5, 6, 7 }) |row, a| {
        try expectValueEqual(.{ .struct_val = &.{.{ .name = "a", .value = .{ .int32_val = a } }} }, row.values[1]);
    }
    try expectValueEqual(.{ .map_val = &.{
        .{ .key = .{ .int32_val = 1 }, .value = .{ .int64_val = 10 } },
        .{ .key = .{ .int32_val = 2 }, .value = .{ .int64_val = 20 } },
    } }, rows[0].values[2]);
    try expectValueEqual(.{ .map_val = &.{} }, rows[1].values[2]);
}

test "writer page index: typed Writer rejects nulls in required list, struct and map fields" {
    const allocator = std.testing.allocator;
    const fields = [_]parquet.StructField{.{ .name = "a", .type_ = .int32, .optional = false }};
    const columns = [_]ColumnDef{
        .{ .name = "x", .type_ = .int32, .is_list = true, .element_optional = false },
        ColumnDef.struct_("s", &fields, false),
        .{ .name = "m", .type_ = .int32, .is_map = true, .map_value_type = .int64, .map_value_optional = false },
    };
    var writer = try parquet.writeToBuffer(allocator, &columns);
    defer writer.deinit();
    const items = [_]Optional(i32){ .{ .value = 1 }, .null_value };
    try std.testing.expectError(error.NullInRequiredColumn, writer.writeListColumn(i32, 0, &.{.{ .value = &items }}));
    try std.testing.expectError(error.NullInRequiredColumn, writer.writeListColumn(i32, 0, &.{.null_value}));
    try std.testing.expectError(error.NullInRequiredColumn, writer.writeStructField(i32, 1, 0, &.{null}, &.{false}));
    try std.testing.expectError(error.NullInRequiredColumn, writer.writeStructField(i32, 1, 0, &.{5}, &.{true}));
    try std.testing.expectError(error.RowCountMismatch, writer.writeStructField(i32, 1, 0, &.{ 5, 6 }, &.{false}));
    const entries = [_]MapEntry(i32, i64){.{ .key = 1, .value = .null_value }};
    try std.testing.expectError(error.NullInRequiredColumn, writer.writeMapColumn(i32, i64, 2, &.{.{ .value = &entries }}));
    try std.testing.expectError(error.NullInRequiredColumn, writer.writeMapColumn(i32, i64, 2, &.{.null_value}));
}

test "writer page index: typed Writer writeNestedColumn splits pages and indexes them" {
    const allocator = std.testing.allocator;
    const element = parquet.SchemaNode{ .int32 = .{} };
    const list = parquet.SchemaNode{ .list = &element };
    const optional_list = parquet.SchemaNode{ .optional = &list };
    const columns = [_]ColumnDef{ .{ .name = "id", .type_ = .int32 }, ColumnDef.fromNode("values", &optional_list) };

    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const num_rows = 300;
    const ids = try arena.alloc(Optional(i32), num_rows);
    const rows = try arena.alloc(Value, num_rows);
    for (0..num_rows) |i| {
        ids[i] = .{ .value = @intCast(i) };
        if (i % 7 == 3) {
            rows[i] = .null_val;
        } else {
            const items = try arena.alloc(Value, i % 4);
            for (items, 0..) |*item, j| item.* = .{ .int32_val = @intCast(i * 10 + j) };
            rows[i] = .{ .list_val = items };
        }
    }

    var files: [2][]u8 = undefined;
    for (&files, [_]?usize{ null, 32 }) |*file, max_page_size| {
        var writer = try parquet.writeToBuffer(allocator, &columns);
        defer writer.deinit();
        writer.max_page_size = max_page_size;
        writer.write_page_index = max_page_size != null;
        try writer.writeColumnOptional(i32, 0, ids);
        try writer.writeNestedColumn(1, rows);
        try writer.close();
        file.* = try writer.toOwnedSlice();
    }
    defer for (files) |f| allocator.free(f);
    try expectPagedMatchesSingle(allocator, files[0], files[1], num_rows);
}

test "writer page index: typed Writer files with pages for test_interop.py" {
    const allocator = std.testing.allocator;
    std.Io.Dir.cwd().createDirPath(io, "../test-files-arrow/interop") catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };
    // 256-byte pages: fewer pages than the tests above use, for a smaller committed file.
    const bytes = try writeTypedFile(allocator, 300, .{ .max_page_size = 256, .write_page_index = true });
    defer allocator.free(bytes);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = "../test-files-arrow/interop/typed_pages.parquet", .data = bytes });
}

// -- DynamicWriter flat boolean and fixed-length byte array columns --

/// Writes `num_rows` rows: id (int32) = i, flag (boolean) = i % 3 == 0 and null when
/// i % 11 == 0, code (fixed 4 bytes) = i big-endian and null when i % 13 == 5,
/// uuid (fixed 16 bytes) = i in the last byte.
fn writeDynamicFlatFile(allocator: std.mem.Allocator, num_rows: usize, max_page_size: ?usize) ![]u8 {
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();
    writer.setRowGroupSize(num_rows);
    if (max_page_size) |m| {
        writer.setMaxPageSize(m);
        writer.write_page_index = true;
    }
    try writer.addColumn("id", parquet.TypeInfo.int32, .{});
    try writer.addColumn("flag", parquet.TypeInfo.bool_, .{});
    try writer.addColumn("code", parquet.TypeInfo.fixedBytes(4), .{});
    try writer.addColumn("uuid", parquet.TypeInfo.uuid, .{});
    try writer.begin();
    for (0..num_rows) |i| {
        try writer.setInt32(0, @intCast(i));
        if (i % 11 == 0) try writer.setNull(1) else try writer.setBool(1, i % 3 == 0);
        var code: [4]u8 = undefined;
        std.mem.writeInt(u32, &code, @intCast(i), .big);
        if (i % 13 == 5) try writer.setNull(2) else try writer.setBytes(2, &code);
        var uuid: [16]u8 = @splat(0);
        uuid[15] = @truncate(i);
        try writer.setBytes(3, &uuid);
        try writer.addRow();
    }
    try writer.close();
    return writer.toOwnedSlice();
}

test "writer page index: DynamicWriter splits boolean and fixed-length byte array columns into indexed pages" {
    const allocator = std.testing.allocator;
    const num_rows = 300;
    const single = try writeDynamicFlatFile(allocator, num_rows, null);
    defer allocator.free(single);
    // 32 bytes: 256 booleans, 8 int32 values, 8 4-byte or 2 16-byte values per page.
    const paged = try writeDynamicFlatFile(allocator, num_rows, 32);
    defer allocator.free(paged);
    try expectPagedMatchesSingle(allocator, single, paged, num_rows);
}

// -- Values of nested leaves --

test "writer page index: writeColumnChunkFromValues rejects values that differ from the column type or levels" {
    const allocator = std.testing.allocator;
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();

    // A list<int32> leaf: def 3 holds a value, def 2 is a null element.
    const def = [_]u32{ 3, 3, 2 };
    const rep = [_]u32{ 0, 1, 1 };
    const cases = [_][3]Value{
        // An int64 value in an int32 column.
        .{ .{ .int32_val = 1 }, .{ .int64_val = 2 }, .null_val },
        // A value at a definition level below the maximum.
        .{ .{ .int32_val = 1 }, .{ .int32_val = 2 }, .{ .int32_val = 3 } },
        // No value at the maximum definition level.
        .{ .{ .int32_val = 1 }, .null_val, .null_val },
    };
    for (cases) |values| {
        try std.testing.expectError(error.TypeMismatch, values_writer.writeColumnChunkFromValues(
            allocator,
            &out.writer,
            &.{ "x", "list", "element" },
            &values,
            &def,
            &rep,
            3,
            1,
            4,
            .uncompressed,
            .{ .physical_type = .int32 },
        ));
    }

    var ok = try values_writer.writeColumnChunkFromValues(
        allocator,
        &out.writer,
        &.{ "x", "list", "element" },
        &.{ .{ .int32_val = 1 }, .{ .int32_val = 2 }, .null_val },
        &def,
        &rep,
        3,
        1,
        4,
        .uncompressed,
        .{ .physical_type = .int32 },
    );
    defer ok.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 3), ok.metadata.num_values);
}

test "writer page index: nested leaf with a value of another type is an error" {
    const allocator = std.testing.allocator;

    // DynamicWriter: an int64 value in a list<int32> column.
    {
        var writer = try parquet.createBufferDynamic(allocator);
        defer writer.deinit();
        const element = try writer.allocSchemaNode(.{ .int32 = .{} });
        try writer.addColumnNested("values", try writer.allocSchemaNode(.{ .list = element }), .{});
        try writer.begin();
        try writer.beginList(0);
        try writer.appendNestedValue(0, .{ .int32_val = 1 });
        try writer.appendNestedValue(0, .{ .int64_val = 2 });
        try writer.endList(0);
        try writer.addRow();
        try std.testing.expectError(error.InvalidData, writer.close());
    }

    // Typed Writer: the same through writeNestedColumn.
    {
        const element = parquet.SchemaNode{ .int32 = .{} };
        const list = parquet.SchemaNode{ .list = &element };
        const columns = [_]ColumnDef{ColumnDef.fromNode("values", &list)};
        var writer = try parquet.writeToBuffer(allocator, &columns);
        defer writer.deinit();
        const items = [_]Value{ .{ .int32_val = 1 }, .{ .int64_val = 2 } };
        try std.testing.expectError(error.TypeMismatch, writer.writeNestedColumn(0, &.{.{ .list_val = &items }}));
    }
}
