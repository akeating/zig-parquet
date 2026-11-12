//! Row reads with columns decoded as tasks of a 4-thread `std.Io.Threaded`
//! (`DynamicReaderOptions.io`) return the same rows as reads on the calling thread, for every
//! row group of the files in test-files-arrow and test-files-wild.

const std = @import("std");
const io = std.testing.io;
const parquet = @import("../lib.zig");
const format = parquet.format;
const page_filter = parquet.page_filter;
const Value = parquet.Value;
const Row = parquet.Row;
const RowRange = @import("../core/row_ranges.zig").RowRange;

const roots = [_][]const u8{ "../test-files-arrow", "../test-files-wild" };

/// Row groups with more bytes than this are skipped, to bound the test's memory.
const max_row_group_bytes: i64 = 64 * 1024 * 1024;

const Totals = struct {
    files: usize = 0,
    row_groups: usize = 0,
    equal: usize = 0,
    /// Row groups that fail to read on the calling thread; the threaded read must fail too.
    errors: usize = 0,
    filtered: usize = 0,
    skipped_large: usize = 0,
};

test "row reads decoded on 4 threads equal row reads on the calling thread" {
    // The readers use a thread-safe allocator without leak checks: footer parsing leaks on
    // some malformed files.
    const allocator = std.heap.smp_allocator;
    var threaded: std.Io.Threaded = .init(allocator, .{ .async_limit = .limited(3) });
    defer threaded.deinit();

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
            compareFile(allocator, entry.dir, entry.basename, threaded.io(), &totals) catch |err| {
                std.debug.print("rows differ: {s}: {s}\n", .{ entry.path, @errorName(err) });
                return err;
            };
        }
    }
    std.debug.print("\nrow reads on 4 threads: {d} files, {d} row groups: {d} equal, {d} errors on both; {d} filtered reads equal; {d} row groups skipped (over {d} MiB)\n", .{
        totals.files, totals.row_groups, totals.equal, totals.errors, totals.filtered, totals.skipped_large, @divExact(max_row_group_bytes, 1024 * 1024),
    });
    try std.testing.expect(totals.equal > 100);
}

fn compareFile(allocator: std.mem.Allocator, dir: std.Io.Dir, basename: []const u8, threads: std.Io, totals: *Totals) !void {
    const file = dir.openFile(io, basename, .{}) catch return;
    defer file.close(io);
    var serial = parquet.openFileDynamic(allocator, file, io, .{}) catch return;
    defer serial.deinit();
    var parallel = try parquet.openFileDynamic(allocator, file, io, .{ .io = threads });
    defer parallel.deinit();
    totals.files += 1;

    for (serial.metadata.row_groups, 0..) |rg, i| {
        if (rg.total_byte_size > max_row_group_bytes) {
            totals.skipped_large += 1;
            continue;
        }
        totals.row_groups += 1;
        const want = serial.readAllRows(i) catch |err| {
            totals.errors += 1;
            if (parallel.readAllRows(i)) |rows| {
                freeRows(allocator, rows);
                std.debug.print("{s} row group {d}: {s} on the calling thread, none on 4 threads\n", .{ basename, i, @errorName(err) });
                return error.TestUnexpectedResult;
            } else |_| {}
            continue;
        };
        defer freeRows(allocator, want);
        const got = try parallel.readAllRows(i);
        defer freeRows(allocator, got);
        try expectRowsEqual(want, got);
        totals.equal += 1;

        // Filtered reads of row groups with an offset index for every column: the first
        // half of the rows, and one row in the middle.
        const all_indexed = for (rg.columns) |c| {
            if (c.offset_index_offset == null) break false;
        } else rg.columns.len > 0;
        if (!all_indexed or rg.num_rows < 2) continue;
        const n: u64 = @intCast(rg.num_rows);
        const ranges: []const RowRange = &.{ .{ .first = 0, .last = n / 2 }, .{ .first = n / 2 + n / 4, .last = n / 2 + n / 4 + 1 } };
        const filter = page_filter.raw(0, fixedRanges, @ptrCast(@constCast(&ranges)));
        const want_f = serial.readRowsFiltered(i, &.{filter}) catch continue;
        defer freeRows(allocator, want_f);
        const got_f = try parallel.readRowsFiltered(i, &.{filter});
        defer freeRows(allocator, got_f);
        try expectRowsEqual(want_f, got_f);
        totals.filtered += 1;
    }
}

fn fixedRanges(ctx: *anyopaque, ci: ?*const format.ColumnIndex, oi: *const format.OffsetIndex, rg_num_rows: u64, allocator: std.mem.Allocator) anyerror!page_filter.RowRanges {
    _ = ci;
    _ = oi;
    _ = rg_num_rows;
    const ranges: *const []const RowRange = @ptrCast(@alignCast(ctx));
    return page_filter.RowRanges.fromUnsorted(allocator, ranges.*);
}

fn freeRows(allocator: std.mem.Allocator, rows: []Row) void {
    for (rows) |row| row.deinit();
    allocator.free(rows);
}

fn expectRowsEqual(want: []const Row, got: []const Row) !void {
    if (want.len != got.len) return error.RowCountDiffers;
    for (want, got) |w, g| {
        if (w.values.len != g.values.len) return error.ColumnCountDiffers;
        for (w.values, g.values) |a, b| if (!valuesEqual(a, b)) return error.ValueDiffers;
    }
}

/// Whether two values are equal; floats compare by bit pattern, so NaN equals NaN.
fn valuesEqual(a: Value, b: Value) bool {
    if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
    return switch (a) {
        .float_val => |x| @as(u32, @bitCast(x)) == @as(u32, @bitCast(b.float_val)),
        .double_val => |x| @as(u64, @bitCast(x)) == @as(u64, @bitCast(b.double_val)),
        .bytes_val => |x| std.mem.eql(u8, x, b.bytes_val),
        .fixed_bytes_val => |x| std.mem.eql(u8, x, b.fixed_bytes_val),
        .list_val => |x| x.len == b.list_val.len and for (x, b.list_val) |e, v| {
            if (!valuesEqual(e, v)) break false;
        } else true,
        .map_val => |x| x.len == b.map_val.len and for (x, b.map_val) |e, v| {
            if (!valuesEqual(e.key, v.key) or !valuesEqual(e.value, v.value)) break false;
        } else true,
        .struct_val => |x| x.len == b.struct_val.len and for (x, b.struct_val) |e, v| {
            if (!std.mem.eql(u8, e.name, v.name) or !valuesEqual(e.value, v.value)) break false;
        } else true,
        else => std.meta.eql(a, b),
    };
}
