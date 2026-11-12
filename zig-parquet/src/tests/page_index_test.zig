//! Page index read-path tests, driven by a pyarrow-generated file that
//! carries OffsetIndex + ColumnIndex blobs.
//!
//! The fixture is `test-files-arrow/multipage/with_page_index.parquet` —
//! 4000 rows, 2 columns (`id` = 0..3999, `bucket` = id/1000), flat schema,
//! ~128-byte data pages (so `id` is split into many pages with ascending
//! min/max, perfect for exercising per-page skip).
//!
//! The tests of filtered reads of nested columns write their files with the
//! dynamic writer; see "Filtered reads of nested columns" below.

const std = @import("std");
const io = std.testing.io;
const parquet = @import("../lib.zig");
const format = parquet.format;

const FIXTURE = "../test-files-arrow/multipage/with_page_index.parquet";

fn openFixture(allocator: std.mem.Allocator) !parquet.DynamicReader {
    const file = std.Io.Dir.cwd().openFile(io, FIXTURE, .{}) catch |err| {
        std.debug.print("Could not open {s}: {}\n", .{ FIXTURE, err });
        return err;
    };
    return parquet.openFileDynamic(allocator, file, io, .{}) catch |err| {
        file.close(io);
        return err;
    };
}

test "page index: fixture advertises offset+column indexes" {
    const allocator = std.testing.allocator;
    var reader = try openFixture(allocator);
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 4000), reader.metadata.num_rows);
    try std.testing.expectEqual(@as(usize, 1), reader.getNumRowGroups());

    const rg = reader.metadata.row_groups[0];
    try std.testing.expectEqual(@as(usize, 2), rg.columns.len);

    // Each chunk should carry both index locators, with positive offsets.
    for (rg.columns) |chunk| {
        try std.testing.expect(chunk.offset_index_offset != null);
        try std.testing.expect(chunk.offset_index_length != null);
        try std.testing.expect(chunk.column_index_offset != null);
        try std.testing.expect(chunk.column_index_length != null);
        try std.testing.expect(chunk.offset_index_offset.? > 0);
        try std.testing.expect(chunk.column_index_offset.? > 0);
    }
}

test "page index: parse OffsetIndex + ColumnIndex for id column" {
    const allocator = std.testing.allocator;
    var reader = try openFixture(allocator);
    defer reader.deinit();

    const rg = &reader.metadata.row_groups[0];
    const id_chunk = &rg.columns[0];

    var pir = @import("../core/page_index_reader.zig").PageIndexReader.init(allocator, reader.getSource());

    var oi = (try pir.readOffsetIndex(id_chunk)).?;
    defer oi.deinit(allocator);
    try std.testing.expect(oi.page_locations.len > 1);

    // first_row_index monotonically increasing from 0.
    try std.testing.expectEqual(@as(i64, 0), oi.page_locations[0].first_row_index);
    for (oi.page_locations[1..], oi.page_locations[0 .. oi.page_locations.len - 1]) |cur, prev| {
        try std.testing.expect(cur.first_row_index > prev.first_row_index);
        try std.testing.expect(cur.offset > prev.offset);
        try std.testing.expect(cur.compressed_page_size > 0);
    }

    var ci = (try pir.readColumnIndex(id_chunk)).?;
    defer ci.deinit(allocator);

    try std.testing.expectEqual(oi.page_locations.len, ci.null_pages.len);
    try std.testing.expectEqual(oi.page_locations.len, ci.min_values.len);
    try std.testing.expectEqual(oi.page_locations.len, ci.max_values.len);

    // id is monotonically ascending 0..4000 so per-page min<=max and
    // inter-page ordering is ascending.
    try std.testing.expectEqual(format.BoundaryOrder.ascending, ci.boundary_order);
    for (ci.min_values, ci.max_values) |mn, mx| {
        try std.testing.expectEqual(@as(usize, 4), mn.len);
        try std.testing.expectEqual(@as(usize, 4), mx.len);
        const mn_i = std.mem.readInt(i32, mn[0..4], .little);
        const mx_i = std.mem.readInt(i32, mx[0..4], .little);
        try std.testing.expect(mn_i <= mx_i);
    }
}

test "page index: readRowsFiltered on id equality matches exactly one value" {
    const allocator = std.testing.allocator;
    var reader = try openFixture(allocator);
    defer reader.deinit();

    const filter = page_filter.cmpI32(0, .eq, 42);

    const rows = try reader.readRowsFiltered(0, &.{filter});
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    // Stats-level filter is conservative: it keeps the whole page whose
    // [min,max] contains 42. So we expect >= 1 row, 42 is among them, and
    // the kept set is strictly smaller than the full file.
    try std.testing.expect(rows.len >= 1);
    try std.testing.expect(rows.len < 4000);

    var saw_42 = false;
    for (rows) |row| {
        const id = row.getColumn(0).?.asInt32().?;
        if (id == 42) saw_42 = true;
        // id is monotonic, so the kept page is the initial one — all kept
        // ids should be small (much less than half the file).
        try std.testing.expect(id < 2000);
    }
    try std.testing.expect(saw_42);
}

test "page index: readRowsFiltered on id between narrows to a page window" {
    const allocator = std.testing.allocator;
    var reader = try openFixture(allocator);
    defer reader.deinit();

    // Pick a window well-inside the data so the filter actually excludes pages.
    const filter = page_filter.betweenI32(0, 2000, 2050);

    const rows = try reader.readRowsFiltered(0, &.{filter});
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    // Kept rows must at minimum cover every id in [2000, 2050].
    var have: [51]bool = @splat(false);
    for (rows) |row| {
        const id = row.getColumn(0).?.asInt32().?;
        if (id >= 2000 and id <= 2050) have[@intCast(id - 2000)] = true;
    }
    for (have) |hit| try std.testing.expect(hit);

    // And the caller's cost is bounded: far fewer than 4000 rows came back.
    try std.testing.expect(rows.len < 4000);
}

test "page index: readRowsFiltered with impossible predicate returns empty" {
    const allocator = std.testing.allocator;
    var reader = try openFixture(allocator);
    defer reader.deinit();

    // id is 0..3999; nothing matches 10000.
    const filter = page_filter.cmpI32(0, .eq, 10000);

    const rows = try reader.readRowsFiltered(0, &.{filter});
    defer allocator.free(rows);
    try std.testing.expectEqual(@as(usize, 0), rows.len);
}

test "page index: writer emits + reader consumes round-trip" {
    const allocator = std.testing.allocator;

    // Write a small file with write_page_index enabled.
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();
    writer.write_page_index = true;
    writer.use_dictionary = false; // force plain so per-page stats aren't hidden

    try writer.addColumn("a", parquet.TypeInfo.int32, .{});
    try writer.addColumn("b", parquet.TypeInfo.int64, .{});
    try writer.begin();

    const N: i32 = 32;
    for (0..@intCast(N)) |i| {
        try writer.setInt32(0, @intCast(i));
        try writer.setInt64(1, @as(i64, @intCast(i)) * 10);
        try writer.addRow();
    }
    try writer.close();
    const bytes = try writer.toOwnedSlice();
    defer allocator.free(bytes);

    // Read it back.
    var reader = try parquet.openBufferDynamic(allocator, bytes, .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, N), reader.metadata.num_rows);
    const rg = &reader.metadata.row_groups[0];

    // Both columns should carry index locators.
    for (rg.columns) |chunk| {
        try std.testing.expect(chunk.offset_index_offset != null);
        try std.testing.expect(chunk.column_index_offset != null);
        try std.testing.expect(chunk.offset_index_length.? > 0);
        try std.testing.expect(chunk.column_index_length.? > 0);
    }

    var pir = @import("../core/page_index_reader.zig").PageIndexReader.init(allocator, reader.getSource());

    // OffsetIndex for column 0: one page, first_row_index = 0.
    var oi_a = (try pir.readOffsetIndex(&rg.columns[0])).?;
    defer oi_a.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), oi_a.page_locations.len);
    try std.testing.expectEqual(@as(i64, 0), oi_a.page_locations[0].first_row_index);
    try std.testing.expect(oi_a.page_locations[0].compressed_page_size > 0);

    // ColumnIndex for column 0: min=0, max=31, ascending (single-page trivially).
    var ci_a = (try pir.readColumnIndex(&rg.columns[0])).?;
    defer ci_a.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), ci_a.null_pages.len);
    try std.testing.expectEqual(false, ci_a.null_pages[0]);
    try std.testing.expectEqual(@as(i32, 0), std.mem.readInt(i32, ci_a.min_values[0][0..4], .little));
    try std.testing.expectEqual(@as(i32, N - 1), std.mem.readInt(i32, ci_a.max_values[0][0..4], .little));

    // Round-trip readRowsFiltered with equality predicate.
    const filter = page_filter.cmpI32(0, .eq, 7);
    const rows = try reader.readRowsFiltered(0, &.{filter});
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }
    // Single page contains everything, so all 32 rows come back (stats filter is
    // conservative; it just confirms the page _might_ contain 7).
    try std.testing.expectEqual(@as(usize, @intCast(N)), rows.len);

    // Impossible predicate returns empty.
    const impossible = page_filter.cmpI32(0, .eq, 10_000);
    const empty_rows = try reader.readRowsFiltered(0, &.{impossible});
    defer allocator.free(empty_rows);
    try std.testing.expectEqual(@as(usize, 0), empty_rows.len);
}

test "page index: multi-page write captures distinct per-page min/max" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();
    writer.write_page_index = true;
    writer.use_dictionary = false;
    // 4 bytes per i32 * 64 = 256 bytes per page → 4 pages across 256 rows.
    writer.max_page_size = 256;

    try writer.addColumn("id", parquet.TypeInfo.int32, .{});
    try writer.begin();

    const N: i32 = 256;
    for (0..@intCast(N)) |i| {
        try writer.setInt32(0, @intCast(i));
        try writer.addRow();
    }
    try writer.close();
    const bytes = try writer.toOwnedSlice();
    defer allocator.free(bytes);

    var reader = try parquet.openBufferDynamic(allocator, bytes, .{});
    defer reader.deinit();
    const rg = &reader.metadata.row_groups[0];

    var pir = @import("../core/page_index_reader.zig").PageIndexReader.init(allocator, reader.getSource());
    var oi = (try pir.readOffsetIndex(&rg.columns[0])).?;
    defer oi.deinit(allocator);
    var ci = (try pir.readColumnIndex(&rg.columns[0])).?;
    defer ci.deinit(allocator);

    // Expect multiple pages.
    try std.testing.expect(oi.page_locations.len > 1);
    try std.testing.expectEqual(oi.page_locations.len, ci.null_pages.len);
    try std.testing.expectEqual(format.BoundaryOrder.ascending, ci.boundary_order);

    // Per-page min/max must reflect the id range in that page, not the chunk
    // min/max. Verify each page covers a distinct window.
    var prev_max: i32 = -1;
    for (ci.min_values, ci.max_values, 0..) |mn_b, mx_b, i| {
        const mn = std.mem.readInt(i32, mn_b[0..4], .little);
        const mx = std.mem.readInt(i32, mx_b[0..4], .little);
        try std.testing.expect(mn <= mx);
        // Monotonically ascending id → this page's min must exceed the
        // previous page's max.
        if (i > 0) try std.testing.expect(mn > prev_max);
        prev_max = mx;
    }
    try std.testing.expectEqual(@as(i32, 0), std.mem.readInt(i32, ci.min_values[0][0..4], .little));
    try std.testing.expectEqual(@as(i32, N - 1), std.mem.readInt(i32, ci.max_values[ci.max_values.len - 1][0..4], .little));

    // Filtered read: equality predicate should now narrow to one page of rows.
    const filter = page_filter.cmpI32(0, .eq, 130);
    const rows = try reader.readRowsFiltered(0, &.{filter});
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }
    try std.testing.expect(rows.len < @as(usize, @intCast(N))); // skipped some pages
    try std.testing.expect(rows.len >= 1);
    var saw_130 = false;
    for (rows) |row| {
        if (row.getColumn(0).?.asInt32().? == 130) saw_130 = true;
    }
    try std.testing.expect(saw_130);
}

test "page index: Zig-written file is readable by pyarrow (Direction 2)" {
    const allocator = std.testing.allocator;

    // Write a file with page index to the interop/ directory so test_interop.py
    // (or a manual pyarrow invocation) can validate it.
    std.Io.Dir.cwd().createDirPath(io, "../test-files-arrow/interop") catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };
    const file = try std.Io.Dir.cwd().createFile(io, "../test-files-arrow/interop/page_index_roundtrip.parquet", .{});
    defer file.close(io);

    var writer = try parquet.createFileDynamic(allocator, file, io);
    defer writer.deinit();
    writer.write_page_index = true;
    writer.use_dictionary = false;

    try writer.addColumn("a", parquet.TypeInfo.int32, .{});
    try writer.addColumn("b", parquet.TypeInfo.int64, .{});
    try writer.begin();

    const N: i32 = 64;
    for (0..@intCast(N)) |i| {
        try writer.setInt32(0, @intCast(i));
        try writer.setInt64(1, @as(i64, @intCast(i)) * 10);
        try writer.addRow();
    }
    try writer.close();
}

test "page index: intersecting filters narrow further than either alone" {
    const allocator = std.testing.allocator;
    var reader = try openFixture(allocator);
    defer reader.deinit();

    // bucket = id/1000, so bucket==2 corresponds to id in [2000, 3000).
    // Combined with id<2100, answer should be id in [2000, 2100).
    const filters = [_]parquet.ColumnFilter{
        page_filter.cmpI32(1, .eq, 2),
        page_filter.cmpI32(0, .lt, 2100),
    };

    const rows = try reader.readRowsFiltered(0, &filters);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    // Must include every id in [2000, 2100).
    var have: [100]bool = @splat(false);
    for (rows) |row| {
        const id = row.getColumn(0).?.asInt32().?;
        if (id >= 2000 and id < 2100) have[@intCast(id - 2000)] = true;
    }
    for (have) |hit| try std.testing.expect(hit);
}

// =============================================================================
// Filtered reads of nested columns
// =============================================================================
//
// The dynamic writer writes one data page per nested column chunk and writes page indexes
// only for flat numeric columns. `indexRowGroupsAsPages` turns a file written with several
// row groups into a file with one row group in which each former row group's column chunk is
// a data page, and writes an offset index and a column index for every column. The pages of
// such a file start at rows, as the Parquet format requires of files with an offset index.

const thrift = @import("../core/thrift/mod.zig");
const leaf_decoder = @import("../core/leaf_decoder.zig");
const page_writer = @import("../core/page_writer.zig");
const page_filter = parquet.page_filter;
const Value = parquet.Value;
const Row = parquet.Row;
const RowRange = @import("../core/row_ranges.zig").RowRange;

const IndexOptions = struct {
    /// Added to the `first_row_index` of the second page of every column, which then
    /// disagrees with the pages.
    shift_second_page: i64 = 0,
};

/// Rewrites `bytes`, written with no dictionary and one data page per column chunk, into a
/// file with one row group whose pages are the former column chunks, with page indexes. A
/// page's column index entry is the chunk's statistics; a chunk without min/max is a null
/// page.
fn indexRowGroupsAsPages(allocator: std.mem.Allocator, bytes: []const u8, options: IndexOptions) ![]u8 {
    var reader = try parquet.openBufferDynamic(allocator, bytes, .{});
    defer reader.deinit();
    const md = &reader.metadata;
    const rgs = md.row_groups;
    const num_cols = rgs[0].columns.len;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var out: std.Io.Writer.Allocating = .init(allocator);
    errdefer out.deinit();
    const w = &out.writer;
    try w.writeAll(format.PARQUET_MAGIC);

    const columns = try a.alloc(format.ColumnChunk, num_cols);
    const offset_indexes = try a.alloc(format.OffsetIndex, num_cols);
    const column_indexes = try a.alloc(format.ColumnIndex, num_cols);
    for (0..num_cols) |c| {
        const locs = try a.alloc(format.PageLocation, rgs.len);
        const null_pages = try a.alloc(bool, rgs.len);
        const mins = try a.alloc([]const u8, rgs.len);
        const maxs = try a.alloc([]const u8, rgs.len);
        var meta = rgs[0].columns[c].meta_data.?;
        meta.num_values = 0;
        meta.total_compressed_size = 0;
        meta.total_uncompressed_size = 0;
        meta.statistics = null;
        meta.data_page_offset = @intCast(out.written().len);
        var first_row: i64 = 0;
        for (rgs, 0..) |rg, r| {
            const m = rg.columns[c].meta_data.?;
            try std.testing.expect(m.dictionary_page_offset == null);
            const start: usize = @intCast(m.data_page_offset);
            const len: usize = @intCast(m.total_compressed_size);
            locs[r] = .{
                .offset = @intCast(out.written().len),
                .compressed_page_size = @intCast(len),
                .first_row_index = first_row + if (r == 1) options.shift_second_page else 0,
            };
            try w.writeAll(bytes[start..][0..len]);
            first_row += rg.num_rows;
            meta.num_values += m.num_values;
            meta.total_compressed_size += m.total_compressed_size;
            meta.total_uncompressed_size += m.total_uncompressed_size;
            const stats = m.statistics orelse format.Statistics{};
            null_pages[r] = stats.min_value == null or stats.max_value == null;
            mins[r] = stats.min_value orelse "";
            maxs[r] = stats.max_value orelse "";
        }
        columns[c] = .{ .file_offset = meta.data_page_offset, .meta_data = meta };
        offset_indexes[c] = .{ .page_locations = locs };
        column_indexes[c] = .{ .null_pages = null_pages, .min_values = mins, .max_values = maxs };
    }

    for (column_indexes, columns) |*ci, *cc| {
        const blob = try thriftBytes(a, ci);
        cc.column_index_offset = @intCast(out.written().len);
        cc.column_index_length = @intCast(blob.len);
        try w.writeAll(blob);
    }
    for (offset_indexes, columns) |*oi, *cc| {
        const blob = try thriftBytes(a, oi);
        cc.offset_index_offset = @intCast(out.written().len);
        cc.offset_index_length = @intCast(blob.len);
        try w.writeAll(blob);
    }

    var row_group = [_]format.RowGroup{.{ .columns = columns, .num_rows = md.num_rows }};
    const footer_md = format.FileMetaData{ .version = md.version, .schema = md.schema, .num_rows = md.num_rows, .row_groups = &row_group };
    const footer = try thriftBytes(a, &footer_md);
    try w.writeAll(footer);
    var len_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_buf, @intCast(footer.len), .little);
    try w.writeAll(&len_buf);
    try w.writeAll(format.PARQUET_MAGIC);
    return out.toOwnedSlice();
}

/// The Thrift compact encoding of `value`, allocated with `arena`.
fn thriftBytes(arena: std.mem.Allocator, value: anytype) ![]const u8 {
    var tw = thrift.CompactWriter.init(arena);
    try value.serialize(&tw);
    return tw.getWritten();
}

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

/// Writes `num_rows` rows in row groups of 10 with columns
///   0: id (int32)
///   1: values, an optional list of int32: null when i % 7 == 3, otherwise i % 4 items
///      i * 10 + j
///   2: matrix, a list of lists of int32: i % 3 lists, list k holding k items
///      i * 100 + k * 10 + m
/// and returns the file with page indexes (see `indexRowGroupsAsPages`).
fn writeListFile(allocator: std.mem.Allocator, num_rows: usize, options: IndexOptions) ![]u8 {
    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();
    writer.setUseDictionary(false);
    writer.setRowGroupSize(10);

    try writer.addColumn("id", parquet.TypeInfo.int32, .{});
    const element = try writer.allocSchemaNode(.{ .int32 = .{} });
    const list = try writer.allocSchemaNode(.{ .list = element });
    try writer.addColumnNested("values", try writer.allocSchemaNode(.{ .optional = list }), .{});
    const inner = try writer.allocSchemaNode(.{ .list = element });
    try writer.addColumnNested("matrix", try writer.allocSchemaNode(.{ .list = inner }), .{});
    try writer.begin();

    for (0..num_rows) |i| {
        try writer.setInt32(0, @intCast(i));
        if (i % 7 == 3) {
            try writer.setNull(1);
        } else {
            try writer.beginList(1);
            for (0..i % 4) |j| try writer.appendNestedValue(1, .{ .int32_val = @intCast(i * 10 + j) });
            try writer.endList(1);
        }
        try writer.beginList(2);
        for (0..i % 3) |k| {
            try writer.beginList(2);
            for (0..k) |m| try writer.appendNestedValue(2, .{ .int32_val = @intCast(i * 100 + k * 10 + m) });
            try writer.endList(2);
        }
        try writer.endList(2);
        try writer.addRow();
    }
    try writer.close();
    const bytes = try writer.toOwnedSlice();
    defer allocator.free(bytes);
    return indexRowGroupsAsPages(allocator, bytes, options);
}

test "page index: rewritten list file has one row group of pages with the original rows" {
    const allocator = std.testing.allocator;
    const indexed = try writeListFile(allocator, 60, .{});
    defer allocator.free(indexed);

    var reader = try parquet.openBufferDynamic(allocator, indexed, .{});
    defer reader.deinit();
    try std.testing.expectEqual(@as(usize, 1), reader.getNumRowGroups());

    var pir = @import("../core/page_index_reader.zig").PageIndexReader.init(allocator, reader.getSource());
    for (reader.metadata.row_groups[0].columns) |*chunk| {
        var oi = (try pir.readOffsetIndex(chunk)).?;
        defer oi.deinit(allocator);
        try std.testing.expectEqual(@as(usize, 6), oi.page_locations.len);
    }

    const rows = try reader.readAllRows(0);
    defer freeRows(allocator, rows);
    try std.testing.expectEqual(@as(usize, 60), rows.len);
    for (rows, 0..) |row, i| {
        try std.testing.expectEqual(@as(i32, @intCast(i)), row.getColumn(0).?.asInt32().?);
        const values = row.getColumn(1).?;
        if (i % 7 == 3) {
            try std.testing.expect(values.isNull());
        } else {
            const items = values.asList().?;
            try std.testing.expectEqual(i % 4, items.len);
            for (items, 0..) |v, j| try std.testing.expectEqual(@as(i32, @intCast(i * 10 + j)), v.asInt32().?);
        }
        try std.testing.expectEqual(i % 3, row.getColumn(2).?.asList().?.len);
    }
}

test "page index: readRowsFiltered on a list leaf keeps the rows of matching pages" {
    const allocator = std.testing.allocator;
    const indexed = try writeListFile(allocator, 60, .{});
    defer allocator.free(indexed);
    var reader = try parquet.openBufferDynamic(allocator, indexed, .{});
    defer reader.deinit();

    // Items 150..260 are in rows 15..26, in pages 1 (rows 10..20) and 2 (rows 20..30).
    try expectFilteredRows(allocator, &reader, &.{page_filter.betweenI32(1, 150, 260)}, &.{.{ .first = 10, .last = 30 }});
    // A filter on the inner list's leaf: items 1210..1211 are in row 12, in page 1.
    try expectFilteredRows(allocator, &reader, &.{page_filter.betweenI32(2, 1210, 1211)}, &.{.{ .first = 10, .last = 20 }});
    // Both filters: their rows intersect.
    try expectFilteredRows(allocator, &reader, &.{
        page_filter.betweenI32(1, 150, 260),
        page_filter.betweenI32(2, 1210, 1211),
    }, &.{.{ .first = 10, .last = 20 }});
    // No item is above 600.
    try expectFilteredRows(allocator, &reader, &.{page_filter.cmpI32(1, .gt, 600)}, &.{});
}

test "page index: readRowsFiltered on list columns keeps rows inside and across pages" {
    const allocator = std.testing.allocator;
    const indexed = try writeListFile(allocator, 60, .{});
    defer allocator.free(indexed);
    var reader = try parquet.openBufferDynamic(allocator, indexed, .{});
    defer reader.deinit();

    // Ranges inside page 0, across pages 1 and 2, ending at the end of page 2 (rows 27..30),
    // inside page 4, and the last row.
    const ranges: []const RowRange = &.{
        .{ .first = 3, .last = 5 },
        .{ .first = 18, .last = 23 },
        .{ .first = 27, .last = 30 },
        .{ .first = 41, .last = 42 },
        .{ .first = 59, .last = 60 },
    };
    try expectFilteredRows(allocator, &reader, &.{rangesFilter(1, &ranges)}, ranges);
    try expectFilteredRows(allocator, &reader, &.{rangesFilter(2, &ranges)}, ranges);
    // With a filter on id < 50 (pages 0 to 4), the last row is dropped.
    try expectFilteredRows(allocator, &reader, &.{
        rangesFilter(2, &ranges),
        page_filter.cmpI32(0, .lt, 50),
    }, ranges[0..4]);
}

test "page index: readRowsFiltered on a struct column" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();
    writer.setUseDictionary(false);
    writer.setRowGroupSize(8);

    try writer.addColumn("id", parquet.TypeInfo.int32, .{});
    const fields = try writer.allocSchemaFields(2);
    fields[0] = .{ .name = "a", .node = try writer.allocSchemaNode(.{ .int64 = .{} }) };
    const string = try writer.allocSchemaNode(.{ .byte_array = .{ .logical = .string } });
    fields[1] = .{ .name = "b", .node = try writer.allocSchemaNode(.{ .optional = string }) };
    const record = try writer.allocSchemaNode(.{ .struct_ = .{ .fields = fields } });
    try writer.addColumnNested("s", try writer.allocSchemaNode(.{ .optional = record }), .{});
    try writer.begin();

    const num_rows = 40;
    for (0..num_rows) |i| {
        try writer.setInt32(0, @intCast(i));
        if (i % 5 == 2) {
            try writer.setNull(1);
        } else {
            try writer.beginStruct(1);
            try writer.setStructField(1, 0, .{ .int64_val = @intCast(i * 1000) });
            if (i % 3 == 0) {
                try writer.setStructField(1, 1, .null_val);
            } else {
                var buf: [16]u8 = undefined;
                try writer.setStructFieldBytes(1, 1, try std.fmt.bufPrint(&buf, "b{d:0>3}", .{i}));
            }
            try writer.endStruct(1);
        }
        try writer.addRow();
    }
    try writer.close();
    const bytes = try writer.toOwnedSlice();
    defer allocator.free(bytes);
    const indexed = try indexRowGroupsAsPages(allocator, bytes, .{});
    defer allocator.free(indexed);

    var reader = try parquet.openBufferDynamic(allocator, indexed, .{});
    defer reader.deinit();

    // s.a in 9000..17000: rows 9..17, in pages 1 (rows 8..16) and 2 (rows 16..24).
    try expectFilteredRows(allocator, &reader, &.{page_filter.betweenI64(1, 9000, 17000)}, &.{.{ .first = 8, .last = 24 }});
    // s.b >= "b030": rows 31.. in pages 3 (rows 24..32) and 4 (rows 32..40).
    try expectFilteredRows(allocator, &reader, &.{page_filter.cmpBytes(2, .ge, "b030")}, &.{.{ .first = 24, .last = 40 }});
    // Partial pages on s.b, intersected with s.a < 20000 (pages 0 to 2).
    const ranges: []const RowRange = &.{
        .{ .first = 1, .last = 3 },
        .{ .first = 6, .last = 11 },
        .{ .first = 30, .last = 32 },
    };
    try expectFilteredRows(allocator, &reader, &.{
        rangesFilter(2, &ranges),
        page_filter.cmpI64(1, .lt, 20000),
    }, ranges[0..2]);
}

test "page index: readRowsFiltered on a map column" {
    const allocator = std.testing.allocator;

    var writer = try parquet.createBufferDynamic(allocator);
    defer writer.deinit();
    writer.setUseDictionary(false);
    writer.setRowGroupSize(10);

    try writer.addColumn("id", parquet.TypeInfo.int32, .{});
    const key_node = try writer.allocSchemaNode(.{ .byte_array = .{ .logical = .string } });
    const val_node = try writer.allocSchemaNode(.{ .int32 = .{} });
    try writer.addColumnNested("m", try writer.allocSchemaNode(.{ .map = .{ .key = key_node, .value = val_node } }), .{});
    try writer.begin();

    const num_rows = 50;
    for (0..num_rows) |i| {
        try writer.setInt32(0, @intCast(i));
        try writer.beginMap(1);
        for (0..i % 4) |j| {
            try writer.beginMapEntry(1);
            var buf: [16]u8 = undefined;
            try writer.appendNestedBytes(1, try std.fmt.bufPrint(&buf, "k{d:0>3}_{d}", .{ i, j }));
            try writer.appendNestedValue(1, .{ .int32_val = @intCast(i * 10 + j) });
            try writer.endMapEntry(1);
        }
        try writer.endMap(1);
        try writer.addRow();
    }
    try writer.close();
    const bytes = try writer.toOwnedSlice();
    defer allocator.free(bytes);
    const indexed = try indexRowGroupsAsPages(allocator, bytes, .{});
    defer allocator.free(indexed);

    var reader = try parquet.openBufferDynamic(allocator, indexed, .{});
    defer reader.deinit();

    // Keys "k020".."k035" are in pages 2 (rows 20..30) and 3 (rows 30..40).
    try expectFilteredRows(allocator, &reader, &.{page_filter.betweenBytes(1, "k020", "k035")}, &.{.{ .first = 20, .last = 40 }});
    // Values 105..112 are in rows 10 and 11, in page 1.
    try expectFilteredRows(allocator, &reader, &.{page_filter.betweenI32(2, 105, 112)}, &.{.{ .first = 10, .last = 20 }});
    // Partial pages on the map's value leaf.
    const ranges: []const RowRange = &.{
        .{ .first = 0, .last = 1 },
        .{ .first = 9, .last = 12 },
        .{ .first = 39, .last = 40 },
        .{ .first = 44, .last = 47 },
    };
    try expectFilteredRows(allocator, &reader, &.{rangesFilter(2, &ranges)}, ranges);
}

test "page index: readRowsFiltered returns an error when a page's rows differ from the offset index" {
    const allocator = std.testing.allocator;
    // The offset index puts the second page at row 11; it holds rows 10..20.
    const indexed = try writeListFile(allocator, 30, .{ .shift_second_page = 1 });
    defer allocator.free(indexed);
    var reader = try parquet.openBufferDynamic(allocator, indexed, .{});
    defer reader.deinit();

    const ranges: []const RowRange = &.{.{ .first = 2, .last = 4 }};
    try std.testing.expectError(error.PageRowCountMismatch, reader.readRowsFiltered(0, &.{rangesFilter(1, &ranges)}));
}

/// A V1 data page of a required list of required int32 (maximum repetition and definition
/// level 1), with all entries defined.
fn listPage(allocator: std.mem.Allocator, values: []const i32, rep_levels: []const u32) ![]u8 {
    const def_levels = try allocator.alloc(u32, values.len);
    defer allocator.free(def_levels);
    @memset(def_levels, 1);
    var page = try page_writer.writeDataPageWithLevels(allocator, i32, values, def_levels, rep_levels, 1, 1);
    defer page.deinit(allocator);
    const header = format.PageHeader{
        .type_ = .data_page,
        .uncompressed_page_size = @intCast(page.data.len),
        .compressed_page_size = @intCast(page.data.len),
        .data_page_header = .{ .num_values = @intCast(values.len) },
    };
    var tw = thrift.CompactWriter.init(allocator);
    defer tw.deinit();
    try header.serialize(&tw);
    return std.mem.concat(allocator, u8, &.{ tw.getWritten(), page.data });
}

test "page index: decodeSelectedPages checks that pages start at rows" {
    const allocator = std.testing.allocator;
    // Page 0 holds rows [1, 2] and [3]; page 1 continues row [3] with 4 and holds row [5].
    const p0 = try listPage(allocator, &.{ 1, 2, 3 }, &.{ 0, 1, 0 });
    defer allocator.free(p0);
    const p1 = try listPage(allocator, &.{ 4, 5 }, &.{ 1, 0 });
    defer allocator.free(p1);
    const both = try std.mem.concat(allocator, u8, &.{ p0, p1 });
    defer allocator.free(both);

    const meta = format.ColumnMetaData{ .type_ = .int32, .codec = .uncompressed, .num_values = 5 };
    const info = format.ColumnInfo{
        .element = .{ .name = "element", .type_ = .int32, .repetition_type = .required },
        .max_def_level = 1,
        .max_rep_level = 1,
        .column_index = 0,
    };
    const options: leaf_decoder.Options = .{ .keep_levels = true };

    var leaf = try leaf_decoder.decodeSelectedPages(allocator, leaf_decoder.PageReader.fromMemory(allocator, p0), meta, info, options, &.{2});
    try std.testing.expectEqual(@as(usize, 3), leaf.buffers.length);
    leaf.deinit(allocator);
    try std.testing.expectError(error.PageRowCountMismatch, leaf_decoder.decodeSelectedPages(allocator, leaf_decoder.PageReader.fromMemory(allocator, p0), meta, info, options, &.{3}));
    try std.testing.expectError(error.PageNotAtRowStart, leaf_decoder.decodeSelectedPages(allocator, leaf_decoder.PageReader.fromMemory(allocator, p1), meta, info, options, &.{1}));
    // Page 1, read after page 0 to check where page 0's last row ends, does not start at a row.
    try std.testing.expectError(error.PageNotAtRowStart, leaf_decoder.decodeSelectedPages(allocator, leaf_decoder.PageReader.fromMemory(allocator, both), meta, info, options, &.{ 2, 1 }));
    // More data pages than expected.
    try std.testing.expectError(error.PageRowCountMismatch, leaf_decoder.decodeSelectedPages(allocator, leaf_decoder.PageReader.fromMemory(allocator, both), meta, info, options, &.{2}));
}
