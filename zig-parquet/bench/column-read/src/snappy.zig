//! Snappy decompression benchmark: the pure-Zig decoder against the C library.
//!
//! Usage: zig build snappy -- DIR [RUNS]
//!   DIR holds NAME.raw and NAME.snappy pairs written by scripts/snappy_inputs.py.
//!   Reports GB/s of output, median and best over RUNS (default 30) after one warm-up run.
//!   Each run allocates the output buffer, as the Parquet reader does.

const std = @import("std");
const parquet = @import("parquet");
const compress = parquet.internals.compress;

const names = [_][]const u8{ "ids", "timestamps", "doubles", "strings", "json" };

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const gpa = std.heap.smp_allocator;
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    if (args.len < 2) {
        std.debug.print("usage: {s} DIR [RUNS]\n", .{args[0]});
        return error.InvalidArguments;
    }
    const runs: usize = if (args.len > 2) try std.fmt.parseInt(usize, args[2], 10) else 30;
    var dir = try std.Io.Dir.cwd().openDir(io, args[1], .{});
    defer dir.close(io);

    std.debug.print("{s:<11} {s:>7} {s:>12} {s:>12} {s:>12} {s:>12}\n", .{ "input", "ratio", "zig median", "zig best", "C median", "C best" });
    const times = try gpa.alloc(u64, runs);
    defer gpa.free(times);
    for (names) |name| {
        var buf: [64]u8 = undefined;
        const raw = try dir.readFileAlloc(io, try std.fmt.bufPrint(&buf, "{s}.raw", .{name}), gpa, .limited(1 << 30));
        defer gpa.free(raw);
        const comp = try dir.readFileAlloc(io, try std.fmt.bufPrint(&buf, "{s}.snappy", .{name}), gpa, .limited(1 << 30));
        defer gpa.free(comp);

        var result: [2][2]f64 = undefined;
        inline for (.{ compress.zig_snappy, compress.snappy }, 0..) |codec, c| {
            for (0..runs + 1) |run| {
                const start = std.Io.Clock.awake.now(io);
                const out = try codec.decompress(gpa, comp, raw.len);
                const elapsed = start.durationTo(std.Io.Clock.awake.now(io));
                defer gpa.free(out);
                if (!std.mem.eql(u8, out, raw)) return error.OutputDiffers;
                if (run > 0) times[run - 1] = @intCast(elapsed.nanoseconds);
            }
            std.mem.sort(u64, times, {}, std.sort.asc(u64));
            const bytes: f64 = @floatFromInt(raw.len);
            result[c] = .{ bytes / @as(f64, @floatFromInt(times[runs / 2])), bytes / @as(f64, @floatFromInt(times[0])) };
        }
        const ratio = @as(f64, @floatFromInt(raw.len)) / @as(f64, @floatFromInt(comp.len));
        std.debug.print("{s:<11} {d:>7.2} {d:>12.2} {d:>12.2} {d:>12.2} {d:>12.2}\n", .{ name, ratio, result[0][0], result[0][1], result[1][0], result[1][1] });
    }
}
