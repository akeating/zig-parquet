//! Options of the Arrow batch streams in the C, WASI and freestanding WASM APIs
//! (`zp_stream_options` in libparquet.h), and their conversion to `ArrowBatchReader` options.
//!
//! The struct starts with its size in bytes, so that fields can be added later. A caller built
//! against a newer header passes a larger size; the extra bytes must then be zero, otherwise
//! the options would ask for something this version does not do.

const std = @import("std");
const arrow_batch = @import("../core/arrow_batch.zig");
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
};

/// Stream options converted for `ArrowBatchReader`, with owned index lists.
pub const Parsed = struct {
    columns: ?[]usize = null,
    row_groups: ?[]usize = null,
    batch_rows: ?usize = null,
    large_offsets: bool = false,

    pub fn deinit(self: *Parsed, allocator: Allocator) void {
        if (self.columns) |c| allocator.free(c);
        if (self.row_groups) |r| allocator.free(r);
        self.* = .{};
    }

    pub fn batchOptions(self: *const Parsed) arrow_batch.BatchOptions {
        return .{
            .columns = self.columns,
            .row_groups = self.row_groups,
            .batch_rows = self.batch_rows,
            .large_offsets = self.large_offsets,
        };
    }
};

pub const ParseError = error{ InvalidArgument, OutOfMemory };

/// Converts `options` (null: defaults, all columns and row groups in whole row groups). On
/// `error.InvalidArgument`, `message` says which field is invalid. Index ranges are checked
/// by `ArrowBatchReader.init`.
pub fn parse(allocator: Allocator, options: ?*const StreamOptions, message: *[]const u8) ParseError!Parsed {
    const o = options orelse return .{};
    if (o.size < @sizeOf(StreamOptions)) {
        message.* = "zp_stream_options.size is smaller than this version's zp_stream_options";
        return error.InvalidArgument;
    }
    if (o.size > @sizeOf(StreamOptions)) {
        const bytes: [*]const u8 = @ptrCast(o);
        for (bytes[@sizeOf(StreamOptions)..o.size]) |b| if (b != 0) {
            message.* = "zp_stream_options has fields this version does not support";
            return error.InvalidArgument;
        };
    }
    if (o.flags & ~known_flags != 0) {
        message.* = "zp_stream_options.flags has unknown bits";
        return error.InvalidArgument;
    }
    if (o.batch_rows < 0) {
        message.* = "zp_stream_options.batch_rows must not be negative";
        return error.InvalidArgument;
    }

    var parsed: Parsed = .{
        .batch_rows = if (o.batch_rows == 0) null else safe.castTo(usize, o.batch_rows) catch std.math.maxInt(usize),
        .large_offsets = o.flags & ZP_STREAM_LARGE_OFFSETS != 0,
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
    var o: StreamOptions = .{ .size = @sizeOf(StreamOptions), .flags = ZP_STREAM_LARGE_OFFSETS, .col_indices = &cols, .num_cols = 2, .row_groups = &rgs, .num_row_groups = 1, .batch_rows = 100 };
    parsed = try parse(allocator, &o, &msg);
    defer parsed.deinit(allocator);
    try std.testing.expectEqualSlices(usize, &.{ 2, 0 }, parsed.columns.?);
    try std.testing.expectEqualSlices(usize, &.{1}, parsed.row_groups.?);
    try std.testing.expectEqual(@as(?usize, 100), parsed.batch_rows);
    try std.testing.expect(parsed.large_offsets);

    var bad = o;
    bad.size = 8;
    try std.testing.expectError(error.InvalidArgument, parse(allocator, &bad, &msg));
    bad = o;
    bad.flags = 4;
    try std.testing.expectError(error.InvalidArgument, parse(allocator, &bad, &msg));
    bad = o;
    bad.batch_rows = -1;
    try std.testing.expectError(error.InvalidArgument, parse(allocator, &bad, &msg));
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
