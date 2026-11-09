//! Compares the flat-column decoder (`arrow_batch.decodeFlatChunk`) with the `Value`-based
//! decoder (`arrow_batch.decodeLeafChunkViaValues`) on every flat column chunk of every file
//! in test-files-arrow and test-files-wild.
//!
//! Where both decoders succeed, their Arrow output must be identical: length, null count,
//! validity bits, values (all bytes, including null slots) and offsets. Where only one
//! succeeds, the chunk is reported. Known differences are listed in `expected_differences`.

const std = @import("std");
const io = std.testing.io;
const parquet = @import("../lib.zig");
const format = parquet.format;
const arrow_batch = @import("../core/arrow_batch.zig");
const leaf_decoder = @import("../core/leaf_decoder.zig");
const parquet_reader = @import("../core/parquet_reader.zig");
const ArrowArray = parquet.ArrowArray;

const roots = [_][]const u8{ "../test-files-arrow", "../test-files-wild" };

/// Chunks where the two decoders are expected to differ: `path` is a substring of the file
/// path. In each case the flat decoder's output matches PyArrow 23.0.1 and the Value-based
/// decoder's does not; the tests below check the flat decoder against PyArrow's values.
const ExpectedDifference = struct { path: []const u8, column: usize, reason: []const u8 };
const expected_differences = [_]ExpectedDifference{
    .{
        .path = "parquet-testing/shredded_variant/",
        .column = 1,
        .reason = "`var.metadata` is a required leaf in an optional group (max_def 1). The Value " ++
            "decoder treats it as having no definition levels and reads the level bytes as values.",
    },
    .{
        .path = "parquet-testing/data/rle_boolean_encoding.parquet",
        .column = 0,
        .reason = "RLE booleans have a 4-byte length prefix; the Value decoder reads it as RLE data.",
    },
    .{
        .path = "parquet-testing/data/datapage_v2.snappy.parquet",
        .column = 3,
        .reason = "RLE booleans have a 4-byte length prefix; the Value decoder reads it as RLE data.",
    },
};

const Totals = struct {
    files: usize = 0,
    files_unreadable: usize = 0,
    chunks: usize = 0,
    both_ok: usize = 0,
    both_error: usize = 0,
    differences: usize = 0,
    expected: usize = 0,
};

test "flat column decoder matches Value decoder on all test files" {
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
            try compareFile(allocator, root, entry.dir, entry.basename, entry.path, &totals);
        }
    }

    std.debug.print(
        "\nflat column comparison: {d} files ({d} unreadable), {d} flat chunks: {d} equal, {d} both error, {d} differ ({d} expected)\n",
        .{ totals.files, totals.files_unreadable, totals.chunks, totals.both_ok, totals.both_error, totals.differences, totals.expected },
    );
    try std.testing.expectEqual(totals.expected, totals.differences);
    try std.testing.expect(totals.chunks > 0);
}

fn compareFile(allocator: std.mem.Allocator, root: []const u8, dir: std.Io.Dir, basename: []const u8, path: []const u8, totals: *Totals) !void {
    // The reader and the Value-based decoder use an arena: both leak on some malformed
    // inputs (footer parsing, FIXED_LEN_BYTE_ARRAY pages that end early). The flat decoder
    // uses `allocator` (the testing allocator), so its leaks fail the test.
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
    const source = reader.getSource();
    for (md.row_groups, 0..) |rg, rg_idx| {
        for (rg.columns, 0..) |*chunk, col| {
            const info = format.getColumnInfo(md.schema, col) orelse continue;
            if (!leaf_decoder.isFlat(info)) continue;
            const meta = chunk.meta_data orelse continue;
            const page_data = parquet_reader.readColumnChunkData(ref, source, chunk) catch continue;
            totals.chunks += 1;

            var new = arrow_batch.decodeFlatChunk(allocator, page_data, meta, info);
            defer if (new) |*a| a.doRelease() else |_| {};
            var old = arrow_batch.decodeLeafChunkViaValues(ref, page_data, meta, info);
            defer if (old) |*a| a.doRelease() else |_| {};

            var why_buf: [256]u8 = undefined;
            const why: ?[]const u8 = blk: {
                const new_arr = new catch |ne| {
                    _ = old catch {
                        totals.both_error += 1;
                        break :blk null;
                    };
                    break :blk std.fmt.bufPrint(&why_buf, "new error {s}, old ok", .{@errorName(ne)}) catch "new error";
                };
                const old_arr = old catch |oe| {
                    break :blk std.fmt.bufPrint(&why_buf, "old error {s}, new ok", .{@errorName(oe)}) catch "old error";
                };
                break :blk diffArrays(&why_buf, &new_arr, &old_arr, info);
            };

            if (why) |reason| {
                totals.differences += 1;
                const expected = isExpected(path, col);
                if (expected) totals.expected += 1;
                std.debug.print("{s}{s}/{s} rg {d} col {d} ({s}, {s}, encodings", .{
                    if (expected) "expected: " else "DIFF: ", root, path, rg_idx, col, info.element.name, @tagName(info.element.type_ orelse .int32),
                });
                for (meta.encodings) |e| std.debug.print(" {s}", .{@tagName(e)});
                std.debug.print("): {s}\n", .{reason});
            } else if (new) |_| {
                if (old) |_| totals.both_ok += 1 else |_| {}
            } else |_| {}
        }
    }
}

fn isExpected(path: []const u8, column: usize) bool {
    for (expected_differences) |e| {
        if (e.column == column and std.mem.indexOf(u8, path, e.path) != null) return true;
    }
    return false;
}

/// Row group 0, one column, read through readRowGroupAsArrow (which uses the flat decoder for
/// flat columns).
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

test "flat decoder: RLE booleans (V1 page) match PyArrow" {
    var r = try readArrow(std.testing.allocator, "../test-files-wild/parquet-testing/data/rle_boolean_encoding.parquet", 0);
    defer r.deinit();
    // PyArrow 23.0.1: 68 values; the first six are below.
    try std.testing.expectEqual(@as(i64, 68), r.result.arrays[0].length);
    try expectBools(&r.result.arrays[0], &.{ true, false, null, true, true, false });
}

test "flat decoder: RLE booleans (V2 page) match PyArrow" {
    // `d` is the fourth top-level column of datapage_v2.snappy.parquet.
    var r = try readArrow(std.testing.allocator, "../test-files-wild/parquet-testing/data/datapage_v2.snappy.parquet", 3);
    defer r.deinit();
    try std.testing.expectEqual(@as(i64, 5), r.result.arrays[0].length);
    try expectBools(&r.result.arrays[0], &.{ true, true, true, false, true });
}

test "flat decoder: required leaf in optional group matches PyArrow" {
    // var.metadata of case-094 is b"\x01\x00\x00"; of case-046 is 13 bytes.
    const cases = [_]struct { file: []const u8, value: []const u8 }{
        .{ .file = "case-094.parquet", .value = "\x01\x00\x00" },
        .{ .file = "case-046.parquet", .value = "\x11\x05\x00\x01\x02\x03\x04\x05abcde" },
    };
    for (cases) |c| {
        const allocator = std.testing.allocator;
        var path_buf: [256]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buf, "../test-files-wild/parquet-testing/shredded_variant/{s}", .{c.file});
        const file = try std.Io.Dir.cwd().openFile(io, path, .{});
        defer file.close(io);
        var reader = try parquet.openFileDynamic(allocator, file, io, .{});
        defer reader.deinit();
        const chunk = &reader.metadata.row_groups[0].columns[1];
        const page_data = try parquet_reader.readColumnChunkData(allocator, reader.getSource(), chunk);
        defer allocator.free(page_data);
        const info = format.getColumnInfo(reader.metadata.schema, 1).?;
        var arr = try arrow_batch.decodeFlatChunk(allocator, page_data, chunk.meta_data.?, info);
        defer arr.doRelease();
        const offsets: [*]const i32 = @ptrCast(@alignCast(arr.buffers[1].?));
        const data: [*]const u8 = @ptrCast(arr.buffers[2].?);
        try std.testing.expectEqualSlices(u8, c.value, data[@intCast(offsets[0])..@intCast(offsets[1])]);
    }
}

/// Returns a description of the first difference, or null when the arrays are identical.
fn diffArrays(buf: []u8, a: *const ArrowArray, b: *const ArrowArray, info: format.ColumnInfo) ?[]const u8 {
    if (a.length != b.length) return std.fmt.bufPrint(buf, "length {d} vs {d}", .{ a.length, b.length }) catch "length";
    if (a.null_count != b.null_count) return std.fmt.bufPrint(buf, "null_count {d} vs {d}", .{ a.null_count, b.null_count }) catch "null_count";
    if (a.n_buffers != b.n_buffers) return std.fmt.bufPrint(buf, "n_buffers {d} vs {d}", .{ a.n_buffers, b.n_buffers }) catch "n_buffers";
    if ((a.buffers[0] == null) != (b.buffers[0] == null)) return "validity buffer present in one only";
    const n: usize = @intCast(a.length);

    if (a.buffers[0]) |av| {
        const va: [*]const u8 = @ptrCast(av);
        const vb: [*]const u8 = @ptrCast(b.buffers[0].?);
        for (0..n) |i| {
            if (getBit(va, i) != getBit(vb, i)) return std.fmt.bufPrint(buf, "validity bit {d}", .{i}) catch "validity";
        }
    }

    const physical = info.element.type_ orelse return "no physical type";
    const da: [*]const u8 = @ptrCast(a.buffers[1].?);
    const db: [*]const u8 = @ptrCast(b.buffers[1].?);
    switch (physical) {
        .boolean => for (0..n) |i| {
            if (getBit(da, i) != getBit(db, i)) {
                var new_bits: [8]u8 = undefined;
                var old_bits: [8]u8 = undefined;
                for (0..@min(n, 8)) |j| {
                    new_bits[j] = if (getBit(da, j)) '1' else '0';
                    old_bits[j] = if (getBit(db, j)) '1' else '0';
                }
                const m = @min(n, 8);
                return std.fmt.bufPrint(buf, "value bit {d}; first bits new {s} old {s}", .{ i, new_bits[0..m], old_bits[0..m] }) catch "value bit";
            }
        },
        .byte_array => {
            const oa: [*]const i32 = @ptrCast(@alignCast(da));
            const ob: [*]const i32 = @ptrCast(@alignCast(db));
            for (0..n + 1) |i| {
                if (oa[i] != ob[i]) return std.fmt.bufPrint(buf, "offset {d}: {d} vs {d}", .{ i, oa[i], ob[i] }) catch "offset";
            }
            const end: usize = @intCast(oa[n]);
            const ba: [*]const u8 = @ptrCast(a.buffers[2].?);
            const bb: [*]const u8 = @ptrCast(b.buffers[2].?);
            if (!std.mem.eql(u8, ba[0..end], bb[0..end])) return "byte data differs";
        },
        else => {
            const w: usize = switch (physical) {
                .int32, .float => 4,
                .int64, .double, .int96 => 8,
                .fixed_len_byte_array => @intCast(info.element.type_length orelse 0),
                else => unreachable,
            };
            for (0..n) |i| {
                if (!std.mem.eql(u8, da[i * w ..][0..w], db[i * w ..][0..w])) {
                    return std.fmt.bufPrint(buf, "value {d} differs", .{i}) catch "value";
                }
            }
        },
    }
    return null;
}

fn getBit(bits: [*]const u8, i: usize) bool {
    return (bits[i / 8] >> @intCast(i % 8)) & 1 == 1;
}

// =============================================================================
// All leaf columns: values and levels
// =============================================================================

test "leaf decoder matches Value decoder on all leaf columns, with levels" {
    const allocator = std.testing.allocator;
    var totals: Totals = .{};

    for (roots) |root| {
        var dir = std.Io.Dir.cwd().openDir(io, root, .{ .iterate = true }) catch continue;
        defer dir.close(io);
        var walker = try dir.walk(allocator);
        defer walker.deinit();
        while (try walker.next(io)) |entry| {
            if (entry.kind != .file or !std.mem.endsWith(u8, entry.basename, ".parquet")) continue;
            if (std.mem.indexOf(u8, entry.path, ".venv") != null) continue;
            totals.files += 1;
            try compareLeafFile(allocator, root, entry.dir, entry.basename, entry.path, &totals);
        }
    }

    std.debug.print(
        "\nleaf comparison: {d} files ({d} unreadable), {d} leaf chunks: {d} equal, {d} both error, {d} differ ({d} expected)\n",
        .{ totals.files, totals.files_unreadable, totals.chunks, totals.both_ok, totals.both_error, totals.differences, totals.expected },
    );
    try std.testing.expectEqual(totals.expected, totals.differences);
    try std.testing.expect(totals.chunks > 0);
}

fn compareLeafFile(allocator: std.mem.Allocator, root: []const u8, dir: std.Io.Dir, basename: []const u8, path: []const u8, totals: *Totals) !void {
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
    const source = reader.getSource();
    for (md.row_groups, 0..) |rg, rg_idx| {
        for (rg.columns, 0..) |*chunk, col| {
            const info = format.getColumnInfo(md.schema, col) orelse continue;
            const meta = chunk.meta_data orelse continue;
            const page_data = parquet_reader.readColumnChunkData(ref, source, chunk) catch continue;
            totals.chunks += 1;

            var new = leaf_decoder.decodeLeaf(allocator, page_data, meta, info, .{ .keep_levels = true, .large_offsets = true });
            defer if (new) |*l| l.deinit(allocator) else |_| {};
            const old = arrow_batch.decodeRawColumn(ref, page_data, meta, info);

            var why_buf: [256]u8 = undefined;
            const why: ?[]const u8 = blk: {
                const leaf = new catch |ne| {
                    _ = old catch {
                        totals.both_error += 1;
                        break :blk null;
                    };
                    break :blk std.fmt.bufPrint(&why_buf, "new error {s}, old ok", .{@errorName(ne)}) catch "new error";
                };
                const raw = old catch |oe| {
                    break :blk std.fmt.bufPrint(&why_buf, "old error {s}, new ok", .{@errorName(oe)}) catch "old error";
                };
                break :blk diffLeaf(&why_buf, &leaf, &raw, info);
            };

            if (why) |reason| {
                totals.differences += 1;
                const expected = isExpected(path, col);
                if (expected) totals.expected += 1;
                std.debug.print("{s}{s}/{s} rg {d} col {d} ({s}, {s}, def {d}, rep {d}): {s}\n", .{
                    if (expected) "expected: " else "DIFF: ", root, path, rg_idx, col, info.element.name,
                    @tagName(info.element.type_ orelse .int32), info.max_def_level, info.max_rep_level, reason,
                });
            } else if (new) |_| {
                if (old) |_| totals.both_ok += 1 else |_| {}
            } else |_| {}
        }
    }
}

/// Compares a decoded leaf (one slot per level entry) with the Value decoder's output.
fn diffLeaf(buf: []u8, leaf: *const leaf_decoder.Leaf, raw: *const arrow_batch.RawColumnData, info: format.ColumnInfo) ?[]const u8 {
    const b = &leaf.buffers;
    const n = raw.values.len;
    if (b.length != n) return std.fmt.bufPrint(buf, "entries {d} vs {d}", .{ b.length, n }) catch "entries";
    if (raw.def_levels.len != n or raw.rep_levels.len != n) return "old level count differs from value count";
    for (0..n) |i| {
        const d = if (leaf.def_levels) |l| l[i] else info.max_def_level;
        if (d != raw.def_levels[i]) return std.fmt.bufPrint(buf, "def level {d}: {d} vs {d}", .{ i, d, raw.def_levels[i] }) catch "def";
        const r = if (leaf.rep_levels) |l| l[i] else 0;
        if (r != raw.rep_levels[i]) return std.fmt.bufPrint(buf, "rep level {d}: {d} vs {d}", .{ i, r, raw.rep_levels[i] }) catch "rep";
    }

    const physical = info.element.type_ orelse return "no physical type";
    for (raw.values, 0..) |v, i| {
        const valid = b.isValid(i);
        if (v == .null_val) {
            if (valid) return std.fmt.bufPrint(buf, "entry {d}: old null, new valid", .{i}) catch "null";
            continue;
        }
        if (!valid) return std.fmt.bufPrint(buf, "entry {d}: old {s}, new null", .{ i, @tagName(v) }) catch "null";
        const same = switch (physical) {
            .boolean => v == .bool_val and v.bool_val == getBit(b.values.ptr, i),
            .int32 => v == .int32_val and v.int32_val == std.mem.bytesToValue(i32, b.values[i * 4 ..][0..4]),
            .int64, .int96 => v == .int64_val and v.int64_val == std.mem.bytesToValue(i64, b.values[i * 8 ..][0..8]),
            .float => v == .float_val and @as(u32, @bitCast(v.float_val)) == std.mem.bytesToValue(u32, b.values[i * 4 ..][0..4]),
            .double => v == .double_val and @as(u64, @bitCast(v.double_val)) == std.mem.bytesToValue(u64, b.values[i * 8 ..][0..8]),
            .byte_array => v == .bytes_val and std.mem.eql(u8, v.bytes_val, b.bytesAt(i)),
            .fixed_len_byte_array => blk: {
                const w: usize = @intCast(info.element.type_length orelse 0);
                break :blk v == .fixed_bytes_val and std.mem.eql(u8, v.fixed_bytes_val, b.values[i * w ..][0..w]);
            },
        };
        if (!same) return std.fmt.bufPrint(buf, "entry {d} differs (old {s})", .{ i, @tagName(v) }) catch "value";
    }
    return null;
}
