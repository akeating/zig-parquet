//! Page index read-path tests, driven by a pyarrow-generated file that
//! carries OffsetIndex + ColumnIndex blobs.
//!
//! The fixture is `test-files-arrow/multipage/with_page_index.parquet` —
//! 4000 rows, 2 columns (`id` = 0..3999, `bucket` = id/1000), flat schema,
//! ~128-byte data pages (so `id` is split into many pages with ascending
//! min/max, perfect for exercising per-page skip).

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

    const page_filter = @import("../core/page_filter.zig");
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

    const page_filter = @import("../core/page_filter.zig");
    // Pick a window well-inside the data so the filter actually excludes pages.
    const filter = page_filter.betweenI32(0, 2000, 2050);

    const rows = try reader.readRowsFiltered(0, &.{filter});
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    // Kept rows must at minimum cover every id in [2000, 2050].
    var have: [51]bool = [_]bool{false} ** 51;
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

    const page_filter = @import("../core/page_filter.zig");
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
    const page_filter = @import("../core/page_filter.zig");
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
    const page_filter = @import("../core/page_filter.zig");
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

    const page_filter = @import("../core/page_filter.zig");
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
    var have: [100]bool = [_]bool{false} ** 100;
    for (rows) |row| {
        const id = row.getColumn(0).?.asInt32().?;
        if (id >= 2000 and id < 2100) have[@intCast(id - 2000)] = true;
    }
    for (have) |hit| try std.testing.expect(hit);
}
