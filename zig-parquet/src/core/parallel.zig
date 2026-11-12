//! Runs the independent tasks of a read, such as decoding the leaf columns of a row group,
//! through a `std.Io`.
//!
//! With an `Io` whose `async` runs tasks on threads (`std.Io.Threaded`), the tasks run in
//! parallel, on up to the `Io`'s `async_limit` threads plus the calling thread. With an `Io`
//! that runs `async` tasks immediately (a single-threaded build, `Io.Threaded.init_single_threaded`)
//! or without an `Io`, they run one after another on the calling thread.
//!
//! The allocator the tasks use must be thread-safe when they run in parallel
//! (`std.heap.smp_allocator`, `std.heap.DebugAllocator` with its default configuration);
//! `std.heap.ArenaAllocator` is not.

const std = @import("std");
const Allocator = std.mem.Allocator;

fn ErrorOf(comptime task: anytype) type {
    const ret = @typeInfo(@TypeOf(task)).@"fn".return_type.?;
    return @typeInfo(ret).error_union.error_set;
}

pub fn Error(comptime task: anytype) type {
    return ErrorOf(task) || Allocator.Error || std.Io.Cancelable;
}

/// Calls `task(context, i)` for each `i < n`.
///
/// Without `io`, or with `n` below 2, the calls run in order and the first error is returned
/// at once. With `io`, every call runs even when one fails, and the error of the call with
/// the lowest `i` is returned. Either way the caller must find out which calls completed (for
/// example from flags the task sets) to release their results after an error.
pub fn forEach(io: ?std.Io, allocator: Allocator, n: usize, context: anytype, comptime task: anytype) Error(task)!void {
    const E = ErrorOf(task);
    const Context = @TypeOf(context);
    const the_io = io orelse {
        for (0..n) |i| try task(context, i);
        return;
    };
    if (n < 2) {
        for (0..n) |i| try task(context, i);
        return;
    }

    const errors = try allocator.alloc(?E, n);
    defer allocator.free(errors);
    @memset(errors, null);

    const Run = struct {
        fn run(ctx: Context, i: usize, slot: *?E) void {
            task(ctx, i) catch |e| {
                slot.* = e;
            };
        }
    };

    var group: std.Io.Group = .init;
    for (errors, 0..) |*slot, i| group.async(the_io, Run.run, .{ context, i, slot });
    group.await(the_io) catch |e| {
        // `await` returned before every task ran; `cancel` waits for the rest.
        group.cancel(the_io);
        return e;
    };
    for (errors) |e| if (e) |err| return err;
}

test "forEach runs every task, in parallel or in order" {
    const allocator = std.testing.allocator;
    const Ctx = struct {
        out: []usize,
        fn task(self: *const @This(), i: usize) error{Odd}!void {
            self.out[i] = i * i;
        }
        fn failing(self: *const @This(), i: usize) error{Odd}!void {
            self.out[i] = i;
            if (i % 2 == 1) return error.Odd;
        }
    };
    var threaded: std.Io.Threaded = .init(allocator, .{});
    defer threaded.deinit();

    for ([_]?std.Io{ null, threaded.io() }) |io| {
        var out: [64]usize = undefined;
        const ctx: Ctx = .{ .out = &out };
        try forEach(io, allocator, out.len, &ctx, Ctx.task);
        for (out, 0..) |v, i| try std.testing.expectEqual(i * i, v);

        @memset(&out, 1000);
        try std.testing.expectError(error.Odd, forEach(io, allocator, out.len, &ctx, Ctx.failing));
        if (io != null) {
            // Every task ran despite the errors.
            for (out, 0..) |v, i| try std.testing.expectEqual(i, v);
        } else {
            // In order, the first error (i = 1) stops the loop.
            try std.testing.expectEqual(@as(usize, 1), out[1]);
            try std.testing.expectEqual(@as(usize, 1000), out[2]);
        }
    }
}
