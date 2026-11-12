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
const test_util = @import("../core/test_util.zig");
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
    /// Page headers longer than the page reader's first read.
    long_headers: usize = 0,
    /// Chunks of repeated columns decoded with each slot level.
    slotted: usize = 0,
};

test "leaf decoder decodes every leaf column chunk of the test files without leaks" {
    const allocator = std.testing.allocator;
    var totals: Totals = .{};

    // The files are decoded as tasks of `std.testing.io`, several at a time. Each job has an
    // allocator of its own that fails the job when memory leaks.
    const Job = struct {
        path: []const u8,
        totals: Totals = .{},
        err: ?anyerror = null,

        fn run(job: *@This()) void {
            var gpa: test_util.LeakCheckAllocator = .init;
            decodeFile(gpa.allocator(), std.Io.Dir.cwd(), job.path, &job.totals) catch |e| {
                job.err = e;
            };
            if (gpa.deinit() == .leak and job.err == null) job.err = error.MemoryLeak;
        }
    };
    var paths: std.heap.ArenaAllocator = .init(allocator);
    defer paths.deinit();
    var jobs: std.ArrayList(Job) = .empty;
    defer jobs.deinit(allocator);
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
            const path = try std.fs.path.join(paths.allocator(), &.{ root, entry.path });
            try jobs.append(allocator, .{ .path = path });
        }
    }
    var group: std.Io.Group = .init;
    for (jobs.items) |*job| group.async(io, Job.run, .{job});
    try group.await(io);
    for (jobs.items) |job| {
        if (job.err) |e| {
            std.debug.print("leaf decoder sweep: {s}: {s}\n", .{ job.path, @errorName(e) });
            return e;
        }
        inline for (@typeInfo(Totals).@"struct".field_names) |name| @field(totals, name) += @field(job.totals, name);
    }
    totals.files = jobs.items.len;

    std.debug.print(
        "\nleaf decoder sweep: {d} files ({d} unreadable), {d} leaf chunks: {d} decoded, {d} errors, {d} skipped (over {d} MiB); {d} page headers over 1 KiB; {d} repeated chunks equal with each slot level\n",
        .{ totals.files, totals.files_unreadable, totals.chunks, totals.decoded, totals.errors, totals.skipped_large, @divExact(max_sweep_chunk_bytes, 1024 * 1024), totals.long_headers, totals.slotted },
    );
    try std.testing.expect(totals.decoded > 0);
    try std.testing.expect(totals.slotted > 0);
}

fn decodeFile(allocator: std.mem.Allocator, dir: std.Io.Dir, basename: []const u8, totals: *Totals) !void {
    // The reader and the decoder use `allocator`, which fails the file's job on a leak; the
    // arena holds the column chunk bytes.
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const ref = arena.allocator();

    const file = dir.openFile(io, basename, .{}) catch {
        totals.files_unreadable += 1;
        return;
    };
    defer file.close(io);
    var reader = parquet.openFileDynamic(allocator, file, io, .{}) catch {
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
            const range = try parquet_reader.columnChunkRange(chunk);
            expectSamePages(allocator, page_data, reader.getSource(), range.start, range.size, totals) catch |err| {
                std.debug.print("page reader mismatch: {s} column {d}: {s}\n", .{ basename, col, @errorName(err) });
                return err;
            };

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
                expectCursorMatches(allocator, page_data, meta, info, &leaf, rows, 0) catch |err| {
                    std.debug.print("cursor mismatch, {d} rows per batch: {s} column {d}: {s}\n", .{ rows, basename, col, @errorName(err) });
                    return err;
                };
            }

            // With each slot level of a repeated column, the buffers hold the entries at or
            // above it, whole and through a cursor, and the levels are unchanged.
            if (info.max_rep_level > 0 and leaf.def_levels != null) {
                var slot_level: u32 = 1;
                while (slot_level <= info.max_def_level) : (slot_level += 1) {
                    expectSlotsMatch(allocator, page_data, meta, info, &leaf, slot_level) catch |err| {
                        std.debug.print("slot level {d} mismatch: {s} column {d}: {s}\n", .{ slot_level, basename, col, @errorName(err) });
                        return err;
                    };
                }
                totals.slotted += 1;
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
    slot_level: u32,
) !void {
    var cursor = try leaf_decoder.ColumnCursor.init(allocator, leaf_decoder.PageReader.fromMemory(allocator, page_data), meta, info, .{ .keep_levels = true, .large_offsets = true, .slot_level = slot_level }, 0);
    defer cursor.deinit();
    // Buffer entries and levels returned so far; they differ with a slot level.
    var at: usize = 0;
    var level_at: usize = 0;
    var short = false;
    while (true) {
        var batch = try cursor.next(rows);
        defer batch.deinit(allocator);
        const b = &batch.buffers;
        const batch_entries = if (batch.def_levels) |d| d.len else b.length;
        if (batch_entries == 0) break;
        // Only the last batch may hold fewer rows.
        try std.testing.expect(!short);
        const batch_rows = if (batch.rep_levels) |r| countZeros(r) else batch_entries;
        if (batch_rows < rows) short = true;
        try std.testing.expect(batch_rows <= rows);
        // A batch starts at a row boundary (except in files whose levels do not start at 0).
        if (batch.rep_levels) |r| {
            if (level_at > 0 or full.rep_levels.?[0] == 0) try std.testing.expectEqual(@as(u32, 0), r[0]);
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
        if (full.def_levels) |d| try std.testing.expectEqualSlices(leaf_decoder.Level, d[level_at..][0..batch_entries], batch.def_levels.?);
        if (full.rep_levels) |r| try std.testing.expectEqualSlices(leaf_decoder.Level, r[level_at..][0..batch_entries], batch.rep_levels.?);
        at += b.length;
        level_at += batch_entries;
    }
    try std.testing.expectEqual(full.buffers.length, at);
}

/// Decodes the chunk with `slot_level` and checks that the buffers hold the entries of
/// `full` (decoded without it) whose definition level is at least `slot_level`, and that the
/// levels are those of `full`; then the same through a cursor, 7 rows per call.
fn expectSlotsMatch(
    allocator: std.mem.Allocator,
    page_data: []const u8,
    meta: format.ColumnMetaData,
    info: format.ColumnInfo,
    full: *const leaf_decoder.Leaf,
    slot_level: u32,
) !void {
    var slotted = try leaf_decoder.decodeLeaf(allocator, page_data, meta, info, .{ .keep_levels = true, .large_offsets = true, .slot_level = slot_level });
    defer slotted.deinit(allocator);
    var want = try full.buffers.select(allocator, full.def_levels.?, slot_level, std.math.maxInt(usize));
    defer want.deinit(allocator);
    const b = &slotted.buffers;
    try std.testing.expectEqual(want.length, b.length);
    try std.testing.expectEqual(want.null_count, b.null_count);
    for (0..b.length) |i| {
        try std.testing.expectEqual(want.isValid(i), b.isValid(i));
        switch (b.layout) {
            .bits => try std.testing.expectEqual(getBit(want.values.ptr, i), getBit(b.values.ptr, i)),
            .fixed => |w| try std.testing.expectEqualSlices(u8, want.values[i * w ..][0..w], b.values[i * w ..][0..w]),
            .bytes => try std.testing.expectEqualSlices(u8, want.bytesAt(i), b.bytesAt(i)),
        }
    }
    try std.testing.expectEqualSlices(leaf_decoder.Level, full.def_levels.?, slotted.def_levels.?);
    if (full.rep_levels) |r| try std.testing.expectEqualSlices(leaf_decoder.Level, r, slotted.rep_levels.?);
    if (full.def_levels.?.len <= 7000) try expectCursorMatches(allocator, page_data, meta, info, &slotted, 7, slot_level);
}

/// Reads a chunk's pages from memory and from the source, and checks that headers and bodies
/// agree. Pages whose header cannot be parsed end the comparison, as in the decoder.
fn expectSamePages(allocator: std.mem.Allocator, page_data: []const u8, source: parquet.SeekableReader, start: u64, size: u64, totals: *Totals) !void {
    var mem = leaf_decoder.PageReader.fromMemory(allocator, page_data);
    defer mem.deinit();
    var src = leaf_decoder.PageReader.fromSource(allocator, source, start, size);
    defer src.deinit();
    while (!mem.done()) {
        try std.testing.expect(!src.done());
        const before = mem.pos;
        const a = mem.next() catch |err| {
            try std.testing.expectError(err, src.next());
            return;
        };
        defer parquet_reader.freePageHeaderContents(allocator, &a);
        const b = try src.next();
        defer parquet_reader.freePageHeaderContents(allocator, &b);
        if (mem.pos - before > 1024) totals.long_headers += 1;
        try std.testing.expectEqual(a.compressed_page_size, b.compressed_page_size);
        try std.testing.expectEqual(mem.pos, src.pos - start);
        const body_a = mem.body() catch |err| {
            try std.testing.expectError(err, src.body());
            return;
        };
        try std.testing.expectEqualSlices(u8, body_a, try src.body());
    }
    try std.testing.expect(src.done());
}

fn countZeros(levels: []const leaf_decoder.Level) usize {
    var n: usize = 0;
    for (levels) |lv| n += @intFromBool(lv == 0);
    return n;
}

test "page reader reads headers longer than its first read" {
    const allocator = std.testing.allocator;
    const thrift = @import("../core/thrift/mod.zig");

    // A required INT32 chunk of 3 PLAIN pages of 100 values, each header carrying 3000-byte
    // statistics.
    const long_stat: [3000]u8 = @splat('x');
    var chunk: std.ArrayList(u8) = .empty;
    defer chunk.deinit(allocator);
    for (0..3) |p| {
        var body: [400]u8 = undefined;
        for (0..100) |i| std.mem.writeInt(i32, body[i * 4 ..][0..4], @intCast(p * 100 + i), .little);
        const header: format.PageHeader = .{
            .type_ = .data_page,
            .uncompressed_page_size = body.len,
            .compressed_page_size = body.len,
            .data_page_header = .{ .num_values = 100, .statistics = .{ .max = &long_stat, .min = &long_stat } },
        };
        var w = thrift.CompactWriter.init(allocator);
        defer w.deinit();
        try header.serialize(&w);
        try std.testing.expect(w.getWritten().len > 3000);
        try chunk.appendSlice(allocator, w.getWritten());
        try chunk.appendSlice(allocator, &body);
    }

    const meta: format.ColumnMetaData = .{ .type_ = .int32, .num_values = 300, .total_compressed_size = @intCast(chunk.items.len) };
    const info: format.ColumnInfo = .{
        .element = .{ .name = "v", .type_ = .int32, .repetition_type = .required },
        .max_def_level = 0,
        .max_rep_level = 0,
        .column_index = 0,
    };
    var buffer_reader = parquet.io.BufferReader.init(chunk.items);
    var cursor = try leaf_decoder.ColumnCursor.init(allocator, leaf_decoder.PageReader.fromSource(allocator, buffer_reader.reader(), 0, chunk.items.len), meta, info, .{}, 0);
    defer cursor.deinit();

    var expected: i32 = 0;
    while (true) {
        var batch = try cursor.next(64);
        defer batch.deinit(allocator);
        if (batch.buffers.length == 0) break;
        for (0..batch.buffers.length) |i| {
            try std.testing.expectEqual(expected, std.mem.readInt(i32, batch.buffers.values[i * 4 ..][0..4], .little));
            expected += 1;
        }
    }
    try std.testing.expectEqual(@as(i32, 300), expected);
}

test "page reader reads each span with one read" {
    const allocator = std.testing.allocator;
    const thrift = @import("../core/thrift/mod.zig");

    // A required INT32 chunk of 3 PLAIN pages of 100 values. Page 1's header carries
    // 3000-byte statistics, so it is longer than the first read of a header.
    const long_stat: [3000]u8 = @splat('x');
    var chunk: std.ArrayList(u8) = .empty;
    defer chunk.deinit(allocator);
    var spans: [3]leaf_decoder.PageReader.Span = undefined;
    for (0..3) |p| {
        const start = chunk.items.len;
        var body: [400]u8 = undefined;
        for (0..100) |i| std.mem.writeInt(i32, body[i * 4 ..][0..4], @intCast(p * 100 + i), .little);
        var header: format.PageHeader = .{
            .type_ = .data_page,
            .uncompressed_page_size = body.len,
            .compressed_page_size = body.len,
            .data_page_header = .{ .num_values = 100 },
        };
        if (p == 1) header.data_page_header.?.statistics = .{ .max = &long_stat, .min = &long_stat };
        var w = thrift.CompactWriter.init(allocator);
        defer w.deinit();
        try header.serialize(&w);
        try chunk.appendSlice(allocator, w.getWritten());
        try chunk.appendSlice(allocator, &body);
        spans[p] = .{ .start = start, .size = chunk.items.len - start };
    }

    const meta: format.ColumnMetaData = .{ .type_ = .int32, .num_values = 300, .total_compressed_size = @intCast(chunk.items.len) };
    const info: format.ColumnInfo = .{
        .element = .{ .name = "v", .type_ = .int32, .repetition_type = .required },
        .max_def_level = 0,
        .max_rep_level = 0,
        .column_index = 0,
    };
    const cases = [_][]const leaf_decoder.PageReader.Span{
        &.{ spans[0], spans[2] },
        &.{spans[1]},
        &.{ spans[0], spans[1], spans[2] },
    };
    const first_values = [_][]const i32{ &.{ 0, 200 }, &.{100}, &.{ 0, 100, 200 } };
    for (cases, first_values) |selected, firsts| {
        var source = test_util.RecordingSource{ .data = chunk.items, .allocator = allocator };
        defer source.deinit();
        const page_rows = [_]usize{ 100, 100, 100 };
        const pages = leaf_decoder.PageReader.fromSpans(allocator, source.reader(), selected);
        var leaf = try leaf_decoder.decodeSelectedPages(allocator, pages, meta, info, .{}, page_rows[0..selected.len]);
        defer leaf.deinit(allocator);

        try std.testing.expectEqual(selected.len * 100, leaf.buffers.length);
        for (firsts, 0..) |first, k| {
            for (0..100) |i| {
                const at = (k * 100 + i) * 4;
                try std.testing.expectEqual(first + @as(i32, @intCast(i)), std.mem.readInt(i32, leaf.buffers.values[at..][0..4], .little));
            }
        }
        // One read per span, of the whole span.
        try std.testing.expectEqual(selected.len, source.reads.items.len);
        for (selected, source.reads.items) |want, got| {
            try std.testing.expectEqual(want.start, got.offset);
            try std.testing.expectEqual(want.size, @as(u64, got.len));
        }
    }
}

// =============================================================================
// Values checked against PyArrow 23.0.1
// =============================================================================

/// Row group 0, one top-level column, read through arrow_batch.readColumns.
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
    const result = try arrow_batch.readColumns(allocator, reader.getSource(), reader.metadata, 0, .{ .columns = &.{col} });
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
