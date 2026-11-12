//! Options of the Arrow batch streams in the C, WASI and freestanding WASM APIs
//! (`zp_stream_options` in libparquet.h), and their conversion to `ArrowBatchReader` options.
//!
//! The struct starts with its size in bytes, so that fields can be added later. A caller built
//! against a newer header passes a larger size; the extra bytes must then be zero, otherwise
//! the options would ask for something this version does not do. A caller built against an
//! older header passes a smaller size; the fields it does not have are zero.

const std = @import("std");
const builtin = @import("builtin");
const arrow_batch = @import("../core/arrow_batch.zig");
const parquet_reader = @import("../core/parquet_reader.zig");
const safe = @import("../core/safe.zig");

const Allocator = std.mem.Allocator;

/// Flag: read BYTE_ARRAY columns as Arrow large_string / large_binary (i64 offsets).
pub const ZP_STREAM_LARGE_OFFSETS: u32 = 1;
const known_flags: u32 = ZP_STREAM_LARGE_OFFSETS;

/// Layout of `zp_stream_options` in libparquet.h.
pub const StreamOptions = extern struct {
    /// sizeof(zp_stream_options) as the caller was built.
    size: u32,
    flags: u32,
    /// Top-level column indices, or null for all columns.
    col_indices: ?[*]const c_int,
    num_cols: c_int,
    /// Row group indices, read in this order, or null for all row groups.
    row_groups: ?[*]const c_int,
    num_row_groups: c_int,
    /// Rows per batch, or 0 for one batch per row group.
    batch_rows: i64,
    /// Threads that decode the batches, including the calling one; 0 or 1: the calling
    /// thread only.
    threads: c_int,
    /// Row groups decoded ahead of the batches; needs `threads` of 2 or more.
    read_ahead: c_int,
    /// Largest decompressed page accepted, in bytes; 0: the default (128 MiB). Larger pages
    /// return ZP_ERROR_INVALID_DATA (AllocationLimitExceeded).
    max_page_size: i64,
};

/// The size of the first version of `zp_stream_options`, which ended with `batch_rows`.
const first_size = @offsetOf(StreamOptions, "threads");

/// Whether this build can decode on several threads. WASM builds decode on the calling
/// thread and ignore `threads` and `read_ahead`.
pub const threads_supported = !builtin.single_threaded and !builtin.cpu.arch.isWasm();

/// The threads of a stream: a `std.Io.Threaded` when the options ask for more than one
/// thread and the build has threads. Must not move after `start`.
pub const Threads = struct {
    threaded: if (threads_supported) ?std.Io.Threaded else void = if (threads_supported) null else {},

    pub fn start(self: *Threads, count: usize) void {
        if (!threads_supported or count < 2) return;
        self.threaded = .init(std.heap.smp_allocator, .{ .async_limit = .limited(count - 1) });
    }

    /// The `Io` the batches are decoded with, or null for the calling thread.
    pub fn io(self: *Threads) ?std.Io {
        if (!threads_supported) return null;
        return if (self.threaded) |*t| t.io() else null;
    }

    pub fn deinit(self: *Threads) void {
        if (!threads_supported) return;
        if (self.threaded) |*t| t.deinit();
        self.threaded = null;
    }
};

/// Stream options converted for `ArrowBatchReader`, with owned index lists.
pub const Parsed = struct {
    columns: ?[]usize = null,
    row_groups: ?[]usize = null,
    batch_rows: ?usize = null,
    large_offsets: bool = false,
    threads: usize = 1,
    read_ahead: usize = 0,
    max_page_size: ?usize = null,

    pub fn deinit(self: *Parsed, allocator: Allocator) void {
        if (self.columns) |c| allocator.free(c);
        if (self.row_groups) |r| allocator.free(r);
        self.* = .{};
    }

    /// The options of the batch reader. With `io` null (one thread, or a build without
    /// threads), the batches are decoded on the calling thread without read-ahead.
    pub fn batchOptions(self: *const Parsed, io: ?std.Io) arrow_batch.BatchOptions {
        return .{
            .columns = self.columns,
            .row_groups = self.row_groups,
            .batch_rows = self.batch_rows,
            .large_offsets = self.large_offsets,
            .io = io,
            .read_ahead = if (io == null) 0 else self.read_ahead,
            .limits = .{ .max_page_size = self.max_page_size orelse (parquet_reader.AllocationLimits{}).max_page_size },
        };
    }
};

pub const ParseError = error{ InvalidArgument, OutOfMemory };

/// Converts `options` (null: defaults, all columns and row groups in whole row groups). On
/// `error.InvalidArgument`, `message` says which field is invalid. Index ranges are checked
/// by `ArrowBatchReader.init`.
pub fn parse(allocator: Allocator, options: ?*const StreamOptions, message: *[]const u8) ParseError!Parsed {
    const given = options orelse return .{};
    if (given.size < first_size) {
        message.* = "zp_stream_options.size is smaller than any version of zp_stream_options";
        return error.InvalidArgument;
    }
    if (given.size > @sizeOf(StreamOptions)) {
        const bytes: [*]const u8 = @ptrCast(given);
        for (bytes[@sizeOf(StreamOptions)..given.size]) |b| if (b != 0) {
            message.* = "zp_stream_options has fields this version does not support";
            return error.InvalidArgument;
        };
    }
    // Fields past the caller's size are zero.
    var copy = std.mem.zeroes(StreamOptions);
    @memcpy(std.mem.asBytes(&copy)[0..@min(given.size, @sizeOf(StreamOptions))], @as([*]const u8, @ptrCast(given)));
    const o = &copy;
    if (o.flags & ~known_flags != 0) {
        message.* = "zp_stream_options.flags has unknown bits";
        return error.InvalidArgument;
    }
    if (o.batch_rows < 0) {
        message.* = "zp_stream_options.batch_rows must not be negative";
        return error.InvalidArgument;
    }
    if (o.threads < 0 or o.read_ahead < 0) {
        message.* = "zp_stream_options.threads and read_ahead must not be negative";
        return error.InvalidArgument;
    }
    if (o.max_page_size < 0) {
        message.* = "zp_stream_options.max_page_size must not be negative";
        return error.InvalidArgument;
    }
    if (o.read_ahead > 0 and o.threads < 2) {
        message.* = "zp_stream_options.read_ahead needs threads of 2 or more";
        return error.InvalidArgument;
    }

    var parsed: Parsed = .{
        .batch_rows = if (o.batch_rows == 0) null else safe.castTo(usize, o.batch_rows) catch std.math.maxInt(usize),
        .large_offsets = o.flags & ZP_STREAM_LARGE_OFFSETS != 0,
        .threads = @max(1, safe.castTo(usize, o.threads) catch unreachable), // checked non-negative above
        .read_ahead = safe.castTo(usize, o.read_ahead) catch unreachable, // checked non-negative above
        .max_page_size = if (o.max_page_size == 0) null else safe.castTo(usize, o.max_page_size) catch std.math.maxInt(usize),
    };
    errdefer parsed.deinit(allocator);
    if (o.col_indices) |ci| parsed.columns = try indexList(allocator, ci, o.num_cols, "zp_stream_options.col_indices", message);
    if (o.row_groups) |rg| parsed.row_groups = try indexList(allocator, rg, o.num_row_groups, "zp_stream_options.row_groups", message);
    return parsed;
}

/// Copies `count` non-negative indices. An empty list is allowed for row groups and columns
/// alike: it selects none.
fn indexList(allocator: Allocator, items: [*]const c_int, count: c_int, comptime field: []const u8, message: *[]const u8) ParseError![]usize {
    if (count < 0) {
        message.* = field ++ " count must not be negative";
        return error.InvalidArgument;
    }
    const n = safe.castTo(usize, count) catch unreachable; // non-negative c_int fits in usize
    const out = try allocator.alloc(usize, n);
    errdefer allocator.free(out);
    for (out, items[0..n]) |*o, v| {
        o.* = safe.castTo(usize, v) catch {
            message.* = field ++ " has a negative index";
            return error.InvalidArgument;
        };
    }
    return out;
}

test "parse checks size, flags, batch_rows and indices" {
    const allocator = std.testing.allocator;
    var msg: []const u8 = "";

    var parsed = try parse(allocator, null, &msg);
    try std.testing.expect(parsed.columns == null and parsed.row_groups == null and parsed.batch_rows == null);

    const cols = [_]c_int{ 2, 0 };
    const rgs = [_]c_int{1};
    var o: StreamOptions = .{ .size = @sizeOf(StreamOptions), .flags = ZP_STREAM_LARGE_OFFSETS, .col_indices = &cols, .num_cols = 2, .row_groups = &rgs, .num_row_groups = 1, .batch_rows = 100, .threads = 4, .read_ahead = 1, .max_page_size = 256 << 20 };
    parsed = try parse(allocator, &o, &msg);
    defer parsed.deinit(allocator);
    try std.testing.expectEqualSlices(usize, &.{ 2, 0 }, parsed.columns.?);
    try std.testing.expectEqualSlices(usize, &.{1}, parsed.row_groups.?);
    try std.testing.expectEqual(@as(?usize, 100), parsed.batch_rows);
    try std.testing.expect(parsed.large_offsets);
    try std.testing.expectEqual(@as(usize, 4), parsed.threads);
    try std.testing.expectEqual(@as(usize, 1), parsed.read_ahead);
    try std.testing.expectEqual(@as(?usize, 256 << 20), parsed.max_page_size);
    try std.testing.expectEqual(@as(usize, 256 << 20), parsed.batchOptions(null).limits.max_page_size);

    var bad = o;
    bad.size = 8;
    try std.testing.expectError(error.InvalidArgument, parse(allocator, &bad, &msg));
    bad = o;
    bad.flags = 4;
    try std.testing.expectError(error.InvalidArgument, parse(allocator, &bad, &msg));
    bad = o;
    bad.batch_rows = -1;
    try std.testing.expectError(error.InvalidArgument, parse(allocator, &bad, &msg));
    bad = o;
    bad.max_page_size = -1;
    try std.testing.expectError(error.InvalidArgument, parse(allocator, &bad, &msg));
    bad = o;
    bad.threads = -1;
    try std.testing.expectError(error.InvalidArgument, parse(allocator, &bad, &msg));
    bad = o;
    bad.threads = 1;
    try std.testing.expectError(error.InvalidArgument, parse(allocator, &bad, &msg));

    // A caller built with the first version of the struct: threads and read_ahead are zero,
    // although the bytes after its end are not.
    var older = o;
    older.size = first_size;
    var p1 = try parse(allocator, &older, &msg);
    try std.testing.expectEqual(@as(usize, 1), p1.threads);
    try std.testing.expectEqual(@as(usize, 0), p1.read_ahead);
    try std.testing.expectEqual(@as(?usize, 100), p1.batch_rows);
    p1.deinit(allocator);
    const negative = [_]c_int{-1};
    bad = o;
    bad.row_groups = &negative;
    try std.testing.expectError(error.InvalidArgument, parse(allocator, &bad, &msg));

    // A newer caller's larger struct: zero extra bytes are accepted, others are not.
    const Newer = extern struct { base: StreamOptions, extra: u64 };
    var newer: Newer = .{ .base = o, .extra = 0 };
    newer.base.size = @sizeOf(Newer);
    var p2 = try parse(allocator, &newer.base, &msg);
    p2.deinit(allocator);
    newer.extra = 1;
    try std.testing.expectError(error.InvalidArgument, parse(allocator, &newer.base, &msg));
}
