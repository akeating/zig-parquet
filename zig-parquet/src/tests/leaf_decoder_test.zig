//! Tests of `leaf_decoder` on real files.
//!
//! - Every leaf column chunk in test-files-arrow and test-files-wild is decoded with the
//!   testing allocator (leaks fail the test); flat chunks also through the Arrow path.
//! - Values from files where the previous Value-based decoder was wrong are checked against
//!   PyArrow 23.0.1, through each read path: required leaves in optional groups, and RLE
//!   booleans (which have a 4-byte length prefix).

const std = @import("std");
const io = std.testing.io;
const parquet = @import("../lib.zig");
const format = parquet.format;
const arrow_batch = @import("../core/arrow_batch.zig");
const leaf_decoder = @import("../core/leaf_decoder.zig");
const parquet_reader = @import("../core/parquet_reader.zig");
const ArrowArray = parquet.ArrowArray;

const roots = [_][]const u8{ "../test-files-arrow", "../test-files-wild" };

/// Column chunks with more uncompressed bytes than this are skipped by the sweep below. It
/// keeps the test's memory bounded: large_string_map.brotli.parquet holds more than 2 GiB of
/// strings in one chunk.
const max_sweep_chunk_bytes: i64 = 256 * 1024 * 1024;

const Totals = struct {
    files: usize = 0,
    files_unreadable: usize = 0,
    chunks: usize = 0,
    decoded: usize = 0,
    errors: usize = 0,
    skipped_large: usize = 0,
};

test "leaf decoder decodes every leaf column chunk of the test files without leaks" {
    const allocator = std.testing.allocator;
    var totals: Totals = .{};

    for (roots) |root| {
        var dir = std.Io.Dir.cwd().openDir(io, root, .{ .iterate = true }) catch |err| {
            std.debug.print("skipping {s}: {s}\n", .{ root, @errorName(err) });
            continue;
        };
        defer dir.close(io);
        var walker = try dir.walk(allocator);
        defer walker.deinit();
        while (try walker.next(io)) |entry| {
            if (entry.kind != .file or !std.mem.endsWith(u8, entry.basename, ".parquet")) continue;
            if (std.mem.indexOf(u8, entry.path, ".venv") != null) continue;
            totals.files += 1;
            try decodeFile(allocator, entry.dir, entry.basename, &totals);
        }
    }

    std.debug.print(
        "\nleaf decoder sweep: {d} files ({d} unreadable), {d} leaf chunks: {d} decoded, {d} errors, {d} skipped (over {d} MiB)\n",
        .{ totals.files, totals.files_unreadable, totals.chunks, totals.decoded, totals.errors, totals.skipped_large, @divExact(max_sweep_chunk_bytes, 1024 * 1024) },
    );
    try std.testing.expect(totals.decoded > 0);
}

fn decodeFile(allocator: std.mem.Allocator, dir: std.Io.Dir, basename: []const u8, totals: *Totals) !void {
    // The reader uses an arena: footer parsing leaks on some malformed files. The decoder
    // uses `allocator` (the testing allocator).
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const ref = arena.allocator();

    const file = dir.openFile(io, basename, .{}) catch {
        totals.files_unreadable += 1;
        return;
    };
    defer file.close(io);
    var reader = parquet.openFileDynamic(ref, file, io, .{}) catch {
        totals.files_unreadable += 1;
        return;
    };
    defer reader.deinit();

    const md = reader.metadata;
    for (md.row_groups) |rg| {
        for (rg.columns, 0..) |*chunk, col| {
            const info = format.getColumnInfo(md.schema, col) orelse continue;
            const meta = chunk.meta_data orelse continue;
            totals.chunks += 1;
            if (meta.total_uncompressed_size > max_sweep_chunk_bytes) {
                totals.skipped_large += 1;
                continue;
            }
            const page_data = parquet_reader.readColumnChunkData(ref, reader.getSource(), chunk) catch continue;

            var leaf = leaf_decoder.decodeLeaf(allocator, page_data, meta, info, .{ .keep_levels = true, .large_offsets = true }) catch {
                totals.errors += 1;
                continue;
            };
            defer leaf.deinit(allocator);
            totals.decoded += 1;

            // Batches from a cursor, joined, equal the whole chunk. Small batch sizes only on
            // small chunks, to bound the test's run time.
            const entries = leaf.buffers.length;
            for ([_]usize{ 1, 7, 1000 }) |rows| {
                if (entries > rows * 1000) continue;
                expectCursorMatches(allocator, page_data, meta, info, &leaf, rows) catch |err| {
                    std.debug.print("cursor mismatch, {d} rows per batch: {s} column {d}: {s}\n", .{ rows, basename, col, @errorName(err) });
                    return err;
                };
            }

            if (leaf_decoder.isFlat(info)) {
                var arr = arrow_batch.decodeFlatChunk(allocator, page_data, meta, info) catch continue;
                arr.doRelease();
            }
        }
    }
}

/// Reads the chunk through a `ColumnCursor`, `rows` rows per call, and checks that the
/// batches hold `rows` rows each (except the last), start at a row boundary, and joined
/// equal `full`.
fn expectCursorMatches(
    allocator: std.mem.Allocator,
    page_data: []const u8,
    meta: format.ColumnMetaData,
    info: format.ColumnInfo,
    full: *const leaf_decoder.Leaf,
    rows: usize,
) !void {
    var cursor = try leaf_decoder.ColumnCursor.init(allocator, page_data, meta, info, .{ .keep_levels = true, .large_offsets = true }, 0);
    defer cursor.deinit();
    var at: usize = 0;
    var short = false;
    while (true) {
        var batch = try cursor.next(rows);
        defer batch.deinit(allocator);
        const b = &batch.buffers;
        if (b.length == 0) break;
        // Only the last batch may hold fewer rows.
        try std.testing.expect(!short);
        const batch_rows = if (batch.rep_levels) |r| countZeros(r) else b.length;
        if (batch_rows < rows) short = true;
        try std.testing.expect(batch_rows <= rows);
        // A batch starts at a row boundary (except in files whose levels do not start at 0).
        if (batch.rep_levels) |r| {
            if (at > 0 or full.rep_levels.?[0] == 0) try std.testing.expectEqual(@as(u32, 0), r[0]);
        }

        try std.testing.expect(at + b.length <= full.buffers.length);
        var nulls: usize = 0;
        for (0..b.length) |i| {
            const j = at + i;
            try std.testing.expectEqual(full.buffers.isValid(j), b.isValid(i));
            nulls += @intFromBool(!b.isValid(i));
            switch (b.layout) {
                .bits => try std.testing.expectEqual(getBit(full.buffers.values.ptr, j), getBit(b.values.ptr, i)),
                .fixed => |w| try std.testing.expectEqualSlices(u8, full.buffers.values[j * w ..][0..w], b.values[i * w ..][0..w]),
                .bytes => try std.testing.expectEqualSlices(u8, full.buffers.bytesAt(j), b.bytesAt(i)),
            }
        }
        try std.testing.expectEqual(nulls, b.null_count);
        // Bits past the last entry of the validity bitmap are set.
        if (b.length % 8 != 0) {
            const pad: u8 = ~((@as(u8, 1) << @intCast(b.length % 8)) - 1);
            try std.testing.expectEqual(pad, b.validity[b.length / 8] & pad);
        }
        if (full.def_levels) |d| try std.testing.expectEqualSlices(u32, d[at..][0..b.length], batch.def_levels.?);
        if (full.rep_levels) |r| try std.testing.expectEqualSlices(u32, r[at..][0..b.length], batch.rep_levels.?);
        at += b.length;
    }
    try std.testing.expectEqual(full.buffers.length, at);
}

fn countZeros(levels: []const u32) usize {
    var n: usize = 0;
    for (levels) |lv| n += @intFromBool(lv == 0);
    return n;
}

// =============================================================================
// Values checked against PyArrow 23.0.1
// =============================================================================

/// Row group 0, one top-level column, read through readRowGroupAsArrow.
const ArrowRead = struct {
    result: arrow_batch.ReadResult,
    reader: parquet.DynamicReader,
    file: std.Io.File,

    fn deinit(self: *ArrowRead) void {
        self.result.deinit();
        self.reader.deinit();
        self.file.close(io);
    }
};

fn readArrow(allocator: std.mem.Allocator, path: []const u8, col: usize) !ArrowRead {
    const file = try std.Io.Dir.cwd().openFile(io, path, .{});
    errdefer file.close(io);
    var reader = try parquet.openFileDynamic(allocator, file, io, .{});
    errdefer reader.deinit();
    const result = try arrow_batch.readRowGroupAsArrow(allocator, reader.getSource(), reader.metadata, 0, &.{col});
    return .{ .result = result, .reader = reader, .file = file };
}

/// Checks the first `expected.len` entries of a BOOLEAN array.
fn expectBools(arr: *const ArrowArray, expected: []const ?bool) !void {
    try std.testing.expect(arr.length >= expected.len);
    const values: [*]const u8 = @ptrCast(arr.buffers[1].?);
    for (expected, 0..) |e, i| {
        const valid = if (arr.buffers[0]) |v| getBit(@ptrCast(v), i) else true;
        if (e) |b| {
            try std.testing.expect(valid);
            try std.testing.expectEqual(b, getBit(values, i));
        } else {
            try std.testing.expect(!valid);
        }
    }
}

/// Entry `i` of a BYTE_ARRAY ArrowArray with i32 offsets.
fn arrowBytes(arr: *const ArrowArray, i: usize) []const u8 {
    const offsets: [*]const i32 = @ptrCast(@alignCast(arr.buffers[1].?));
    const data: [*]const u8 = @ptrCast(arr.buffers[2].?);
    return data[@intCast(offsets[i])..@intCast(offsets[i + 1])];
}

fn getBit(bits: [*]const u8, i: usize) bool {
    return (bits[i / 8] >> @intCast(i % 8)) & 1 == 1;
}

test "RLE booleans (V1 page) match PyArrow" {
    var r = try readArrow(std.testing.allocator, "../test-files-wild/parquet-testing/data/rle_boolean_encoding.parquet", 0);
    defer r.deinit();
    // 68 values; the first six are below.
    try std.testing.expectEqual(@as(i64, 68), r.result.arrays[0].length);
    try expectBools(&r.result.arrays[0], &.{ true, false, null, true, true, false });
}

test "RLE booleans (V2 page) match PyArrow" {
    // `d` is the fourth top-level column of datapage_v2.snappy.parquet.
    var r = try readArrow(std.testing.allocator, "../test-files-wild/parquet-testing/data/datapage_v2.snappy.parquet", 3);
    defer r.deinit();
    try std.testing.expectEqual(@as(i64, 5), r.result.arrays[0].length);
    try expectBools(&r.result.arrays[0], &.{ true, true, true, false, true });
}

/// var.metadata (a required leaf in the optional group `var`, leaf column 1) of two
/// shredded_variant files, as PyArrow reads it.
const variant_cases = [_]struct { file: []const u8, metadata: []const u8 }{
    .{ .file = "case-094.parquet", .metadata = "\x01\x00\x00" },
    .{ .file = "case-046.parquet", .metadata = "\x11\x05\x00\x01\x02\x03\x04\x05abcde" },
};

fn variantPath(buf: []u8, file: []const u8) ![]const u8 {
    return std.fmt.bufPrint(buf, "../test-files-wild/parquet-testing/shredded_variant/{s}", .{file});
}

test "required leaf in optional group matches PyArrow: leaf chunk" {
    const allocator = std.testing.allocator;
    for (variant_cases) |c| {
        var path_buf: [256]u8 = undefined;
        const file = try std.Io.Dir.cwd().openFile(io, try variantPath(&path_buf, c.file), .{});
        defer file.close(io);
        var reader = try parquet.openFileDynamic(allocator, file, io, .{});
        defer reader.deinit();
        const chunk = &reader.metadata.row_groups[0].columns[1];
        const page_data = try parquet_reader.readColumnChunkData(allocator, reader.getSource(), chunk);
        defer allocator.free(page_data);
        const info = format.getColumnInfo(reader.metadata.schema, 1).?;
        var arr = try arrow_batch.decodeFlatChunk(allocator, page_data, chunk.meta_data.?, info);
        defer arr.doRelease();
        try std.testing.expectEqualSlices(u8, c.metadata, arrowBytes(&arr, 0));
    }
}

test "required leaf in optional group matches PyArrow: Arrow struct" {
    for (variant_cases) |c| {
        var path_buf: [256]u8 = undefined;
        // `var` is top-level column 1; its first child is `metadata`.
        var r = try readArrow(std.testing.allocator, try variantPath(&path_buf, c.file), 1);
        defer r.deinit();
        const var_struct = &r.result.arrays[0];
        try std.testing.expect(var_struct.n_children >= 1);
        try std.testing.expectEqualSlices(u8, c.metadata, arrowBytes(var_struct.children.?[0], 0));
    }
}

test "required leaf in optional group matches PyArrow: rows" {
    const allocator = std.testing.allocator;
    for (variant_cases) |c| {
        var path_buf: [256]u8 = undefined;
        const file = try std.Io.Dir.cwd().openFile(io, try variantPath(&path_buf, c.file), .{});
        defer file.close(io);
        var reader = try parquet.openFileDynamic(allocator, file, io, .{});
        defer reader.deinit();
        const rows = try reader.readAllRows(0);
        defer {
            for (rows) |row| row.deinit();
            allocator.free(rows);
        }
        const var_value = rows[0].getColumn(1).?;
        const metadata = var_value.getField("metadata").?;
        try std.testing.expectEqualSlices(u8, c.metadata, metadata.asBytes().?);
    }
}
