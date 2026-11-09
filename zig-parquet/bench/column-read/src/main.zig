//! Column read benchmark (Phase 0 baseline).
//!
//! Reads every row group of FILE through each read path, one column at a time and all columns
//! together, and reports wall time plus allocator activity.
//!
//! Usage: zig build run -- FILE.parquet [RUNS]
//!   RUNS: measured runs per case after one warm-up run (default 5).
//!   Time: median and minimum over the measured runs. The file is read through the OS page
//!   cache, which the warm-up run fills, so times exclude disk reads.
//!   Allocations: counted on the last run (they are the same on every run).

const std = @import("std");
const parquet = @import("parquet");

/// Allocator wrapper that counts calls and tracks live and peak bytes. Single-threaded.
const CountingAllocator = struct {
    backing: std.mem.Allocator,
    allocs: usize = 0,
    resizes: usize = 0,
    frees: usize = 0,
    bytes_allocated: usize = 0,
    live: usize = 0,
    peak: usize = 0,

    fn allocator(self: *CountingAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn reset(self: *CountingAllocator) void {
        self.allocs = 0;
        self.resizes = 0;
        self.frees = 0;
        self.bytes_allocated = 0;
        self.peak = self.live;
    }

    fn grew(self: *CountingAllocator, old_len: usize, new_len: usize) void {
        if (new_len > old_len) {
            self.bytes_allocated += new_len - old_len;
            self.live += new_len - old_len;
        } else {
            self.live -= old_len - new_len;
        }
        self.peak = @max(self.peak, self.live);
    }

    const vtable: std.mem.Allocator.VTable = .{
        .alloc = alloc,
        .resize = resize,
        .remap = remap,
        .free = free,
    };

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const ptr = self.backing.rawAlloc(len, alignment, ret_addr) orelse return null;
        self.allocs += 1;
        self.grew(0, len);
        return ptr;
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        if (!self.backing.rawResize(memory, alignment, new_len, ret_addr)) return false;
        self.resizes += 1;
        self.grew(memory.len, new_len);
        return true;
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const ptr = self.backing.rawRemap(memory, alignment, new_len, ret_addr) orelse return null;
        self.resizes += 1;
        self.grew(memory.len, new_len);
        return ptr;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        self.backing.rawFree(memory, alignment, ret_addr);
        self.frees += 1;
        self.live -= memory.len;
    }
};

const Path = enum { arrow, rows };

const Case = struct {
    path: Path,
    /// null: all columns
    column: ?usize,
};

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    if (args.len < 2) {
        std.debug.print("usage: {s} FILE.parquet [RUNS] [PATH:COLUMN]\n", .{args[0]});
        return error.InvalidArguments;
    }
    const path = args[1];
    const runs: usize = if (args.len > 2) try std.fmt.parseInt(usize, args[2], 10) else 5;

    var counting: CountingAllocator = .{ .backing = std.heap.smp_allocator };
    const allocator = counting.allocator();

    const file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    var reader = try parquet.openFileDynamic(allocator, file, io, .{});
    defer reader.deinit();

    const num_rgs = reader.getNumRowGroups();
    const num_cols = reader.getNumColumns();
    std.debug.print("file: {s}\nrows: {d}, row groups: {d}, columns: {d}, measured runs: {d}\n\n", .{
        path, reader.getTotalNumRows(), num_rgs, num_cols, runs,
    });

    var cases: std.ArrayList(Case) = .empty;
    defer cases.deinit(init.gpa);
    // Optional CASE argument "path:column" (e.g. "arrow:id", "rows:all") runs only that case.
    const filter: ?[]const u8 = if (args.len > 3) args[3] else null;
    for ([_]Path{ .arrow, .rows }) |p| {
        for (0..num_cols + 1) |c| {
            const column: ?usize = if (c < num_cols) c else null;
            if (filter) |f| {
                const col_name = if (column) |ci| reader.getLeafSchemaElement(ci).?.name else "all";
                var buf: [64]u8 = undefined;
                const name = try std.fmt.bufPrint(&buf, "{s}:{s}", .{ @tagName(p), col_name });
                if (!std.mem.eql(u8, name, f)) continue;
            }
            try cases.append(init.gpa, .{ .path = p, .column = column });
        }
    }

    std.debug.print("{s:<6} {s:<10} {s:>10} {s:>10} {s:>12} {s:>10} {s:>12} {s:>11} {s:>12} {s:>10}\n", .{
        "path", "column", "median ms", "min ms", "allocs", "resizes", "alloc MB", "peak MB", "retained MB", "check",
    });

    const times = try init.gpa.alloc(u64, runs);
    defer init.gpa.free(times);

    for (cases.items) |case| {
        var check: u64 = 0;
        // Peak and retained bytes are relative to the bytes live when the run starts.
        var peak: usize = 0;
        var retained: isize = 0;
        // One warm-up run, then `runs` measured runs.
        for (0..runs + 1) |run| {
            counting.reset();
            const live_before = counting.live;
            const start = std.Io.Clock.awake.now(io);
            check = switch (case.path) {
                .arrow => try readArrow(allocator, &reader, num_rgs, case.column),
                .rows => try readRows(allocator, &reader, num_rgs, case.column),
            };
            const elapsed = start.durationTo(std.Io.Clock.awake.now(io));
            if (run > 0) times[run - 1] = @intCast(elapsed.nanoseconds);
            peak = counting.peak - live_before;
            retained = @as(isize, @intCast(counting.live)) - @as(isize, @intCast(live_before));
        }
        std.mem.sort(u64, times, {}, std.sort.asc(u64));

        var col_buf: [16]u8 = undefined;
        const col_name = if (case.column) |c|
            reader.getLeafSchemaElement(c).?.name
        else
            try std.fmt.bufPrint(&col_buf, "all", .{});

        std.debug.print("{s:<6} {s:<10} {d:>10.1} {d:>10.1} {d:>12} {d:>10} {d:>12.1} {d:>11.1} {d:>12.1} {x:>10}\n", .{
            @tagName(case.path),
            col_name,
            nsToMs(times[times.len / 2]),
            nsToMs(times[0]),
            counting.allocs,
            counting.resizes,
            bytesToMb(counting.bytes_allocated),
            bytesToMb(peak),
            @as(f64, @floatFromInt(retained)) / (1024.0 * 1024.0),
            check & 0xffffffff,
        });
    }
}

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1e6;
}

fn bytesToMb(bytes: usize) f64 {
    return @as(f64, @floatFromInt(bytes)) / (1024.0 * 1024.0);
}

/// Reads all row groups through readRowGroupAsArrow. Returns a checksum of the array lengths
/// and null counts so the work cannot be skipped.
fn readArrow(allocator: std.mem.Allocator, reader: *parquet.DynamicReader, num_rgs: usize, column: ?usize) !u64 {
    var check: u64 = 0;
    for (0..num_rgs) |rg| {
        const cols: ?[]const usize = if (column) |c| &.{c} else null;
        var result = try parquet.readRowGroupAsArrow(allocator, reader.getSource(), reader.metadata, rg, cols);
        defer result.deinit();
        for (result.arrays) |arr| {
            check +%= @as(u64, @intCast(arr.length)) *% 31 +% @as(u64, @intCast(arr.null_count));
        }
    }
    return check;
}

/// Reads all row groups through the row API. Returns the same checksum as readArrow.
fn readRows(allocator: std.mem.Allocator, reader: *parquet.DynamicReader, num_rgs: usize, column: ?usize) !u64 {
    var check: u64 = 0;
    for (0..num_rgs) |rg| {
        const rows = if (column) |c| try reader.readRowsProjected(rg, &.{c}) else try reader.readAllRows(rg);
        defer {
            for (rows) |row| row.deinit();
            allocator.free(rows);
        }
        const num_values = if (rows.len > 0) rows[0].columnCount() else 0;
        for (0..num_values) |i| {
            var nulls: u64 = 0;
            for (rows) |row| {
                if (row.getColumn(i)) |v| {
                    if (v == .null_val) nulls += 1;
                } else nulls += 1;
            }
            check +%= @as(u64, rows.len) *% 31 +% nulls;
        }
    }
    return check;
}
