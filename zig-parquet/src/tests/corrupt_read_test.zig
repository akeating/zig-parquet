//! Reads damaged copies of the test files through every read path and checks that each read
//! returns rows, arrays or an error: no panic (out-of-bounds access, integer overflow,
//! reachable `unreachable`; the test build has safety checks) and no leaked memory.
//!
//! Each file in test-files-arrow and test-files-wild that opens cleanly is copied
//! `copies_per_file` times. Two thirds of the copies have bytes changed between the magic
//! number and the footer (the column chunks and the page indexes), so the copy still opens
//! and the damage reaches the page and level decoders; the other third have bytes of the
//! footer changed, which the footer parser must reject or read without leaking. The changes are chosen by
//! a generator seeded from the file's path and the copy number, so a failure can be
//! reproduced.
//!
//! Read paths: `readColumns` for each row group; `columnBatches` in batches of 7 rows; batches
//! of one row group decoded on 4 threads with one row group read ahead; `readAllRows` on the
//! calling thread and on 4 threads; `readRowsFiltered` where every leaf has an offset index.

const std = @import("std");
const io = std.testing.io;
const parquet = @import("../lib.zig");
const page_filter = parquet.page_filter;
const RowRange = @import("../core/row_ranges.zig").RowRange;
const test_util = @import("../core/test_util.zig");

const roots = [_][]const u8{ "../test-files-arrow", "../test-files-wild" };

/// Damaged copies read per file.
const copies_per_file = 12;
/// Files larger than this are skipped, to bound the test's run time.
const max_file_bytes = 4 * 1024 * 1024;
/// Files with a column chunk larger than this uncompressed are skipped: a 3 KB file in
/// test-files-wild holds a page of 2 GiB.
const max_chunk_bytes: i64 = 64 * 1024 * 1024;
/// Batches read per batch reader, to bound the run time on large files.
const max_batches = 64;

const Totals = struct {
    files: usize = 0,
    copies: usize = 0,
    reads: usize = 0,
    read_errors: usize = 0,
};

test "damaged files return errors from every read path, without panics or leaks" {
    const allocator = std.testing.allocator;
    var paths: std.heap.ArenaAllocator = .init(allocator);
    defer paths.deinit();

    const Job = struct {
        path: []const u8,
        totals: Totals = .{},
        err: ?anyerror = null,

        fn run(job: *@This()) void {
            readDamagedCopies(job.path, &job.totals) catch |e| {
                job.err = e;
            };
        }
    };
    var jobs: std.ArrayList(Job) = .empty;
    defer jobs.deinit(allocator);
    for (roots) |root| {
        var dir = std.Io.Dir.cwd().openDir(io, root, .{ .iterate = true }) catch continue;
        defer dir.close(io);
        var walker = try dir.walk(allocator);
        defer walker.deinit();
        while (try walker.next(io)) |entry| {
            if (entry.kind != .file or !std.mem.endsWith(u8, entry.basename, ".parquet")) continue;
            if (std.mem.indexOf(u8, entry.path, ".venv") != null) continue;
            try jobs.append(allocator, .{ .path = try std.fs.path.join(paths.allocator(), &.{ root, entry.path }) });
        }
    }

    var group: std.Io.Group = .init;
    for (jobs.items) |*job| group.async(io, Job.run, .{job});
    try group.await(io);

    var totals: Totals = .{};
    for (jobs.items) |job| {
        if (job.err) |e| {
            std.debug.print("damaged reads: {s}: {s}\n", .{ job.path, @errorName(e) });
            return e;
        }
        inline for (@typeInfo(Totals).@"struct".field_names) |name| @field(totals, name) += @field(job.totals, name);
    }
    std.debug.print("\ndamaged reads: {d} files, {d} damaged copies, {d} reads: {d} returned errors\n", .{
        totals.files, totals.copies, totals.reads, totals.read_errors,
    });
    try std.testing.expect(totals.copies > 0);
}

fn readDamagedCopies(path: []const u8, totals: *Totals) !void {
    const original = std.Io.Dir.cwd().readFileAlloc(io, path, std.heap.smp_allocator, .limited(max_file_bytes)) catch return;
    defer std.heap.smp_allocator.free(original);
    // Footer: ... footer, 4-byte footer length, "PAR1".
    if (original.len < 12) return;
    const footer_len = std.mem.readInt(u32, original[original.len - 8 ..][0..4], .little);
    if (footer_len > original.len - 12) return;
    const data_end = original.len - 8 - footer_len;
    if (data_end <= 4) return;
    {
        // Files that do not open undamaged are left out: the damage is meant for the decoders.
        var reader = parquet.openBufferDynamic(std.heap.smp_allocator, original, .{}) catch return;
        defer reader.deinit();
        for (reader.metadata.row_groups) |rg| for (rg.columns) |c| {
            const meta = c.meta_data orelse continue;
            if (meta.total_uncompressed_size > max_chunk_bytes) return;
        };
    }
    totals.files += 1;

    const copy = try std.heap.smp_allocator.alloc(u8, original.len);
    defer std.heap.smp_allocator.free(copy);
    var seed: u64 = std.hash.Wyhash.hash(0, path);
    for (0..copies_per_file) |n| {
        seed +%= 0x9e3779b97f4a7c15;
        @memcpy(copy, original);
        var prng = std.Random.DefaultPrng.init(seed);
        if (n % 3 == 2) {
            damage(prng.random(), copy[data_end .. original.len - 8]);
        } else {
            damage(prng.random(), copy[4..data_end]);
        }
        totals.copies += 1;

        var gpa: test_util.LeakCheckAllocator = .init;
        readAllPaths(gpa.allocator(), copy, totals) catch |e| {
            std.debug.print("damaged reads: {s} copy {d}: {s}\n", .{ path, n, @errorName(e) });
            return e;
        };
        if (gpa.deinit() == .leak) {
            std.debug.print("damaged reads: {s} copy {d}: memory leaked\n", .{ path, n });
            return error.MemoryLeak;
        }
    }
}

/// Changes bytes of `data`: single bytes set at random, runs set to 0x00 or 0xff, or bits
/// flipped.
fn damage(random: std.Random, data: []u8) void {
    const edits = 1 + random.uintLessThan(usize, 8);
    for (0..edits) |_| {
        const at = random.uintLessThan(usize, data.len);
        switch (random.uintLessThan(u8, 3)) {
            0 => data[at] = random.int(u8),
            1 => {
                const run = @min(data.len - at, 1 + random.uintLessThan(usize, 32));
                @memset(data[at..][0..run], if (random.boolean()) 0x00 else 0xff);
            },
            else => data[at] ^= @as(u8, 1) << random.int(u3),
        }
    }
}

/// Reads `bytes` through every read path. Returns an error only for a failure of the test
/// itself (out of memory); errors from the reads are counted.
fn readAllPaths(allocator: std.mem.Allocator, bytes: []const u8, totals: *Totals) !void {
    var threaded: std.Io.Threaded = .init(allocator, .{ .async_limit = .limited(3) });
    defer threaded.deinit();
    const threads = threaded.io();

    var reader = parquet.openBufferDynamic(allocator, bytes, .{}) catch {
        totals.read_errors += 1;
        return;
    };
    defer reader.deinit();
    var threaded_reader = parquet.openBufferDynamic(allocator, bytes, .{ .io = threads }) catch {
        totals.read_errors += 1;
        return;
    };
    defer threaded_reader.deinit();

    const num_rgs = reader.metadata.row_groups.len;
    for (0..num_rgs) |rg| {
        totals.reads += 1;
        if (reader.readColumns(rg, .{})) |result| {
            var r = result;
            r.deinit();
        } else |_| totals.read_errors += 1;
    }

    const batch_options = [_]parquet.BatchOptions{
        .{ .batch_rows = 7 },
        .{ .batch_rows = null, .io = threads, .read_ahead = 1 },
    };
    for (batch_options) |options| {
        totals.reads += 1;
        var batches = reader.columnBatches(options) catch {
            totals.read_errors += 1;
            continue;
        };
        defer batches.deinit();
        for (0..max_batches) |_| {
            const batch = batches.next() catch {
                totals.read_errors += 1;
                break;
            };
            var result = batch orelse break;
            result.deinit();
        }
    }

    for ([_]*parquet.DynamicReader{ &reader, &threaded_reader }) |r| {
        for (0..num_rgs) |rg| {
            totals.reads += 1;
            if (r.readAllRows(rg)) |rows| {
                freeRows(allocator, rows);
            } else |_| totals.read_errors += 1;
        }
    }

    for (reader.metadata.row_groups, 0..) |rg, i| {
        const all_indexed = for (rg.columns) |c| {
            if (c.offset_index_offset == null) break false;
        } else rg.columns.len > 0;
        if (!all_indexed or rg.num_rows < 2) continue;
        const n: u64 = @intCast(rg.num_rows); // the footer is undamaged and rg.num_rows >= 2
        const ranges: []const RowRange = &.{ .{ .first = 0, .last = n / 2 }, .{ .first = n / 2 + n / 4, .last = n / 2 + n / 4 + 1 } };
        const filter = page_filter.raw(0, fixedRanges, @ptrCast(@constCast(&ranges)));
        totals.reads += 1;
        if (reader.readRowsFiltered(i, &.{filter})) |rows| {
            freeRows(allocator, rows);
        } else |_| totals.read_errors += 1;
    }
}

fn fixedRanges(ctx: *anyopaque, ci: ?*const parquet.format.ColumnIndex, oi: *const parquet.format.OffsetIndex, rg_num_rows: u64, allocator: std.mem.Allocator) anyerror!page_filter.RowRanges {
    _ = ci;
    _ = oi;
    _ = rg_num_rows;
    const ranges: *const []const RowRange = @ptrCast(@alignCast(ctx));
    return page_filter.RowRanges.fromUnsorted(allocator, ranges.*);
}

fn freeRows(allocator: std.mem.Allocator, rows: []parquet.Row) void {
    for (rows) |row| row.deinit();
    allocator.free(rows);
}
