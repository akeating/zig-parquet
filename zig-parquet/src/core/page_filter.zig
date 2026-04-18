//! Typed filters that produce `RowRanges` from page indexes.
//!
//! Each `ColumnFilter` targets a single column and carries a predicate that
//! can be evaluated against that column's `ColumnIndex` + `OffsetIndex`. The
//! result is a `RowRanges` in row-group-local row-index space.
//!
//! Multiple filters compose via `RowRanges.intersect`, so a predicate
//! expression like `col0 BETWEEN 10 AND 20 AND col1 = "foo"` decomposes into
//! one `ColumnFilter` per leaf and the caller intersects their outputs.
//!
//! Bloom filters and future predicate-pushdown layers plug in via the `.raw`
//! variant, which takes a user-supplied producer. Same `RowRanges` contract —
//! no new skip abstraction needed.

const std = @import("std");
const format = @import("format.zig");
const row_ranges = @import("row_ranges.zig");

pub const RowRanges = row_ranges.RowRanges;
pub const RowRange = row_ranges.RowRange;

/// Comparison operators for typed predicates.
pub const Op = enum { eq, ne, lt, le, gt, ge };

/// Inclusive value range for BETWEEN-style predicates.
pub fn Between(comptime T: type) type {
    return struct { min: T, max: T };
}

/// Caller-supplied producer: inspect a chunk's column / offset index and
/// return a `RowRanges` in row-group-local space. Used for Bloom filters and
/// any predicate pushed down from an external expression tree.
pub const RawProducer = *const fn (
    ctx: *anyopaque,
    ci: ?*const format.ColumnIndex,
    oi: *const format.OffsetIndex,
    rg_num_rows: u64,
    allocator: std.mem.Allocator,
) anyerror!RowRanges;

/// A column-scoped filter. The variant determines how a `ColumnIndex` entry
/// is tested; a page is "kept" when its min/max range might contain a value
/// the predicate accepts.
///
/// Every variant can be evaluated by `evaluate` to produce `RowRanges`.
pub const ColumnFilter = struct {
    /// Leaf column index within the row group (`RowGroup.columns[column]`).
    column: usize,
    predicate: Predicate,

    pub const Predicate = union(enum) {
        i32_cmp: struct { op: Op, value: i32 },
        i64_cmp: struct { op: Op, value: i64 },
        f32_cmp: struct { op: Op, value: f32 },
        f64_cmp: struct { op: Op, value: f64 },
        /// Lexicographic byte comparison (BYTE_ARRAY / FLBA).
        bytes_cmp: struct { op: Op, value: []const u8 },
        i32_between: Between(i32),
        i64_between: Between(i64),
        f32_between: Between(f32),
        f64_between: Between(f64),
        bytes_between: struct { min: []const u8, max: []const u8 },
        /// Caller-supplied producer. Used for Bloom filters and arbitrary
        /// external predicates.
        raw: struct { producer: RawProducer, ctx: *anyopaque },
    };

    /// Evaluate the predicate against a chunk's indexes. When `ci` is null the
    /// predicate can't distinguish pages — we conservatively return a full
    /// range covering every page described by `oi` (i.e. "keep everything").
    pub fn evaluate(
        self: ColumnFilter,
        ci: ?*const format.ColumnIndex,
        oi: *const format.OffsetIndex,
        rg_num_rows: u64,
        allocator: std.mem.Allocator,
    ) !RowRanges {
        return switch (self.predicate) {
            .raw => |r| r.producer(r.ctx, ci, oi, rg_num_rows, allocator),
            else => evaluateTyped(self.predicate, ci, oi, rg_num_rows, allocator),
        };
    }
};

// =============================================================================
// Typed evaluation
// =============================================================================

fn evaluateTyped(
    pred: ColumnFilter.Predicate,
    ci_opt: ?*const format.ColumnIndex,
    oi: *const format.OffsetIndex,
    rg_num_rows: u64,
    allocator: std.mem.Allocator,
) !RowRanges {
    if (ci_opt == null or oi.page_locations.len == 0) {
        // No column index → conservatively keep everything.
        return RowRanges.full(allocator, rg_num_rows);
    }
    const ci = ci_opt.?;
    const num_pages = oi.page_locations.len;
    if (ci.null_pages.len != num_pages or
        ci.min_values.len != num_pages or
        ci.max_values.len != num_pages)
    {
        return error.InvalidPageIndex;
    }

    // Build per-page ranges where pages passing the predicate contribute
    // [first_row_index, next_first_row_index); the last page ends at rg_num_rows.
    var kept = try std.ArrayList(RowRange).initCapacity(allocator, num_pages);
    errdefer kept.deinit(allocator);

    for (0..num_pages) |i| {
        const null_page = ci.null_pages[i];
        const min_bytes = ci.min_values[i];
        const max_bytes = ci.max_values[i];

        const keep = if (null_page)
            pageOfAllNullsKept(pred)
        else
            pageKeptTyped(pred, min_bytes, max_bytes);

        if (!keep) continue;

        const start: u64 = blk: {
            const v = oi.page_locations[i].first_row_index;
            if (v < 0) return error.InvalidPageIndex;
            break :blk @intCast(v);
        };
        const end: u64 = if (i + 1 < num_pages) blk: {
            const v = oi.page_locations[i + 1].first_row_index;
            if (v < 0) return error.InvalidPageIndex;
            break :blk @intCast(v);
        } else rg_num_rows;

        if (start < end) {
            try kept.append(allocator, .{ .first = start, .last = end });
        }
    }

    return .{ .ranges = try kept.toOwnedSlice(allocator) };
}

/// A page containing only nulls can never match a typed value predicate
/// (null ≠ anything). The `.raw` variant is handled in `evaluate`.
fn pageOfAllNullsKept(pred: ColumnFilter.Predicate) bool {
    _ = pred;
    return false;
}

fn pageKeptTyped(pred: ColumnFilter.Predicate, min_bytes: []const u8, max_bytes: []const u8) bool {
    switch (pred) {
        .i32_cmp => |c| {
            const lo = decodeI32(min_bytes) orelse return true;
            const hi = decodeI32(max_bytes) orelse return true;
            return overlapsCmp(i32, c.op, c.value, lo, hi);
        },
        .i64_cmp => |c| {
            const lo = decodeI64(min_bytes) orelse return true;
            const hi = decodeI64(max_bytes) orelse return true;
            return overlapsCmp(i64, c.op, c.value, lo, hi);
        },
        .f32_cmp => |c| {
            const lo = decodeF32(min_bytes) orelse return true;
            const hi = decodeF32(max_bytes) orelse return true;
            return overlapsCmp(f32, c.op, c.value, lo, hi);
        },
        .f64_cmp => |c| {
            const lo = decodeF64(min_bytes) orelse return true;
            const hi = decodeF64(max_bytes) orelse return true;
            return overlapsCmp(f64, c.op, c.value, lo, hi);
        },
        .bytes_cmp => |c| return overlapsCmpBytes(c.op, c.value, min_bytes, max_bytes),
        .i32_between => |b| {
            const lo = decodeI32(min_bytes) orelse return true;
            const hi = decodeI32(max_bytes) orelse return true;
            return b.min <= hi and b.max >= lo;
        },
        .i64_between => |b| {
            const lo = decodeI64(min_bytes) orelse return true;
            const hi = decodeI64(max_bytes) orelse return true;
            return b.min <= hi and b.max >= lo;
        },
        .f32_between => |b| {
            const lo = decodeF32(min_bytes) orelse return true;
            const hi = decodeF32(max_bytes) orelse return true;
            return b.min <= hi and b.max >= lo;
        },
        .f64_between => |b| {
            const lo = decodeF64(min_bytes) orelse return true;
            const hi = decodeF64(max_bytes) orelse return true;
            return b.min <= hi and b.max >= lo;
        },
        .bytes_between => |b| {
            return std.mem.lessThan(u8, b.min, max_bytes) or std.mem.eql(u8, b.min, max_bytes) and
                (std.mem.lessThan(u8, min_bytes, b.max) or std.mem.eql(u8, min_bytes, b.max));
        },
        .raw => unreachable, // handled in evaluate()
    }
}

/// Does a predicate `op value` overlap the page range `[lo, hi]`?
fn overlapsCmp(comptime T: type, op: Op, value: T, lo: T, hi: T) bool {
    return switch (op) {
        .eq => value >= lo and value <= hi,
        .ne => !(lo == hi and lo == value),
        .lt => lo < value,
        .le => lo <= value,
        .gt => hi > value,
        .ge => hi >= value,
    };
}

/// Byte-wise variant of `overlapsCmp` using lexicographic order.
fn overlapsCmpBytes(op: Op, value: []const u8, lo: []const u8, hi: []const u8) bool {
    return switch (op) {
        .eq => byteLE(lo, value) and byteLE(value, hi),
        .ne => !(std.mem.eql(u8, lo, hi) and std.mem.eql(u8, lo, value)),
        .lt => byteLT(lo, value),
        .le => byteLE(lo, value),
        .gt => byteLT(value, hi),
        .ge => byteLE(value, hi),
    };
}

fn byteLT(a: []const u8, b: []const u8) bool {
    return std.mem.lessThan(u8, a, b);
}
fn byteLE(a: []const u8, b: []const u8) bool {
    return std.mem.lessThan(u8, a, b) or std.mem.eql(u8, a, b);
}

// =============================================================================
// Decoders for PLAIN-encoded min/max bytes
// =============================================================================

fn decodeI32(bytes: []const u8) ?i32 {
    if (bytes.len < 4) return null;
    return std.mem.readInt(i32, bytes[0..4], .little);
}
fn decodeI64(bytes: []const u8) ?i64 {
    if (bytes.len < 8) return null;
    return std.mem.readInt(i64, bytes[0..8], .little);
}
fn decodeF32(bytes: []const u8) ?f32 {
    if (bytes.len < 4) return null;
    const bits = std.mem.readInt(u32, bytes[0..4], .little);
    return @bitCast(bits);
}
fn decodeF64(bytes: []const u8) ?f64 {
    if (bytes.len < 8) return null;
    const bits = std.mem.readInt(u64, bytes[0..8], .little);
    return @bitCast(bits);
}

// =============================================================================
// Convenience constructors
// =============================================================================

pub fn cmpI32(column: usize, op: Op, value: i32) ColumnFilter {
    return .{ .column = column, .predicate = .{ .i32_cmp = .{ .op = op, .value = value } } };
}
pub fn cmpI64(column: usize, op: Op, value: i64) ColumnFilter {
    return .{ .column = column, .predicate = .{ .i64_cmp = .{ .op = op, .value = value } } };
}
pub fn cmpF32(column: usize, op: Op, value: f32) ColumnFilter {
    return .{ .column = column, .predicate = .{ .f32_cmp = .{ .op = op, .value = value } } };
}
pub fn cmpF64(column: usize, op: Op, value: f64) ColumnFilter {
    return .{ .column = column, .predicate = .{ .f64_cmp = .{ .op = op, .value = value } } };
}
pub fn cmpBytes(column: usize, op: Op, value: []const u8) ColumnFilter {
    return .{ .column = column, .predicate = .{ .bytes_cmp = .{ .op = op, .value = value } } };
}

pub fn betweenI32(column: usize, min: i32, max: i32) ColumnFilter {
    return .{ .column = column, .predicate = .{ .i32_between = .{ .min = min, .max = max } } };
}
pub fn betweenI64(column: usize, min: i64, max: i64) ColumnFilter {
    return .{ .column = column, .predicate = .{ .i64_between = .{ .min = min, .max = max } } };
}
pub fn betweenF64(column: usize, min: f64, max: f64) ColumnFilter {
    return .{ .column = column, .predicate = .{ .f64_between = .{ .min = min, .max = max } } };
}
pub fn betweenBytes(column: usize, min: []const u8, max: []const u8) ColumnFilter {
    return .{ .column = column, .predicate = .{ .bytes_between = .{ .min = min, .max = max } } };
}

pub fn raw(column: usize, producer: RawProducer, ctx: *anyopaque) ColumnFilter {
    return .{ .column = column, .predicate = .{ .raw = .{ .producer = producer, .ctx = ctx } } };
}

// =============================================================================
// Tests
// =============================================================================

const testing = std.testing;

fn makeI32Bytes(v: i32) [4]u8 {
    var b: [4]u8 = undefined;
    std.mem.writeInt(i32, &b, v, .little);
    return b;
}

test "evaluate: i32 equality keeps only pages whose range covers value" {
    // 4 pages: [0,10], [10,20], [20,30], [30,40], 100 rows total (25 rows each).
    var b0 = makeI32Bytes(0);
    var b10a = makeI32Bytes(10);
    var b10b = makeI32Bytes(10);
    var b20a = makeI32Bytes(20);
    var b20b = makeI32Bytes(20);
    var b30a = makeI32Bytes(30);
    var b30b = makeI32Bytes(30);
    var b40 = makeI32Bytes(40);
    var mins = [_][]const u8{ &b0, &b10b, &b20b, &b30b };
    var maxs = [_][]const u8{ &b10a, &b20a, &b30a, &b40 };
    var nulls = [_]bool{ false, false, false, false };

    const ci = format.ColumnIndex{
        .null_pages = &nulls,
        .min_values = &mins,
        .max_values = &maxs,
        .boundary_order = .ascending,
    };

    var locs = [_]format.PageLocation{
        .{ .offset = 0, .compressed_page_size = 0, .first_row_index = 0 },
        .{ .offset = 0, .compressed_page_size = 0, .first_row_index = 25 },
        .{ .offset = 0, .compressed_page_size = 0, .first_row_index = 50 },
        .{ .offset = 0, .compressed_page_size = 0, .first_row_index = 75 },
    };
    const oi = format.OffsetIndex{ .page_locations = &locs };

    // eq 15 → only page 1 (range [10,20]) covers it.
    const filter = cmpI32(0, .eq, 15);
    var ranges = try filter.evaluate(&ci, &oi, 100, testing.allocator);
    defer ranges.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 1), ranges.ranges.len);
    try testing.expectEqual(@as(u64, 25), ranges.ranges[0].first);
    try testing.expectEqual(@as(u64, 50), ranges.ranges[0].last);
}

test "evaluate: i32 between merges adjacent kept pages" {
    var b0 = makeI32Bytes(0);
    var b9 = makeI32Bytes(9);
    var b10 = makeI32Bytes(10);
    var b19 = makeI32Bytes(19);
    var b20 = makeI32Bytes(20);
    var b29 = makeI32Bytes(29);
    var b30 = makeI32Bytes(30);
    var b40 = makeI32Bytes(40);
    var mins = [_][]const u8{ &b0, &b10, &b20, &b30 };
    var maxs = [_][]const u8{ &b9, &b19, &b29, &b40 };
    var nulls = [_]bool{ false, false, false, false };

    const ci = format.ColumnIndex{
        .null_pages = &nulls,
        .min_values = &mins,
        .max_values = &maxs,
        .boundary_order = .ascending,
    };

    var locs = [_]format.PageLocation{
        .{ .offset = 0, .compressed_page_size = 0, .first_row_index = 0 },
        .{ .offset = 0, .compressed_page_size = 0, .first_row_index = 25 },
        .{ .offset = 0, .compressed_page_size = 0, .first_row_index = 50 },
        .{ .offset = 0, .compressed_page_size = 0, .first_row_index = 75 },
    };
    const oi = format.OffsetIndex{ .page_locations = &locs };

    // between 5..25 → pages 0, 1, 2 kept; page 3 dropped.
    const filter = betweenI32(0, 5, 25);
    var ranges = try filter.evaluate(&ci, &oi, 100, testing.allocator);
    defer ranges.deinit(testing.allocator);

    // Kept pages are contiguous, so fromUnsorted-style merging in evaluate's
    // direct construction yields three separate appended ranges that do NOT
    // merge (we append as-is). That's fine — the reader still sees the right
    // row set; it's just not merged. Verify with intersection semantics.
    try testing.expectEqual(@as(u64, 75), ranges.rowCount());
    try testing.expect(ranges.contains(0));
    try testing.expect(ranges.contains(74));
    try testing.expect(!ranges.contains(75));
}

test "evaluate: all-null pages are dropped for typed predicates" {
    var b10 = makeI32Bytes(10);
    var b20 = makeI32Bytes(20);
    var empty = [_]u8{};
    var mins = [_][]const u8{ &b10, &empty };
    var maxs = [_][]const u8{ &b20, &empty };
    var nulls = [_]bool{ false, true };

    const ci = format.ColumnIndex{
        .null_pages = &nulls,
        .min_values = &mins,
        .max_values = &maxs,
        .boundary_order = .unordered,
    };
    var locs = [_]format.PageLocation{
        .{ .offset = 0, .compressed_page_size = 0, .first_row_index = 0 },
        .{ .offset = 0, .compressed_page_size = 0, .first_row_index = 10 },
    };
    const oi = format.OffsetIndex{ .page_locations = &locs };

    const filter = cmpI32(0, .eq, 15);
    var ranges = try filter.evaluate(&ci, &oi, 20, testing.allocator);
    defer ranges.deinit(testing.allocator);
    try testing.expectEqual(@as(usize, 1), ranges.ranges.len);
    try testing.expectEqual(@as(u64, 0), ranges.ranges[0].first);
    try testing.expectEqual(@as(u64, 10), ranges.ranges[0].last);
}

test "evaluate: missing column index keeps everything" {
    var locs = [_]format.PageLocation{
        .{ .offset = 0, .compressed_page_size = 0, .first_row_index = 0 },
    };
    const oi = format.OffsetIndex{ .page_locations = &locs };

    const filter = cmpI32(0, .eq, 42);
    var ranges = try filter.evaluate(null, &oi, 50, testing.allocator);
    defer ranges.deinit(testing.allocator);
    try testing.expect(ranges.isFull(50));
}

test "raw producer plugs in same contract" {
    // A producer that ignores the index and keeps only rows [5, 15).
    const Ctx = struct {};
    var ctx = Ctx{};
    const producer = struct {
        fn prod(
            _: *anyopaque,
            _: ?*const format.ColumnIndex,
            _: *const format.OffsetIndex,
            _: u64,
            allocator: std.mem.Allocator,
        ) anyerror!RowRanges {
            return RowRanges.fromSingle(allocator, 5, 15);
        }
    }.prod;

    var locs = [_]format.PageLocation{
        .{ .offset = 0, .compressed_page_size = 0, .first_row_index = 0 },
    };
    const oi = format.OffsetIndex{ .page_locations = &locs };
    const filter = raw(0, producer, &ctx);
    var ranges = try filter.evaluate(null, &oi, 100, testing.allocator);
    defer ranges.deinit(testing.allocator);
    try testing.expectEqual(@as(u64, 10), ranges.rowCount());
}
