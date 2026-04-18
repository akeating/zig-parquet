//! Shared row-range abstraction for page / row-group / bloom-filter skipping.
//!
//! A `RowRanges` is a sorted, disjoint, non-adjacent-merged set of half-open
//! row intervals `[first, last)` in row-group-local row-index space. It is the
//! single contract spoken by every skip producer (row-group stats, page index,
//! bloom filters) and every consumer (column-chunk reader, row assembler).
//!
//! Invariants maintained by every public constructor / mutator:
//!   - `ranges[i].first < ranges[i].last`   (no empty intervals)
//!   - `ranges[i].last <= ranges[i+1].first` (sorted + disjoint)
//!   - adjacent intervals are merged: `ranges[i].last < ranges[i+1].first`
//!
//! Intervals use `u64`; row counts in Parquet are `i64`, but skip logic always
//! works with non-negative counts.

const std = @import("std");

pub const RowRange = struct {
    /// Inclusive lower bound.
    first: u64,
    /// Exclusive upper bound.
    last: u64,

    pub fn count(self: RowRange) u64 {
        return self.last - self.first;
    }

    pub fn overlaps(self: RowRange, other: RowRange) bool {
        return self.first < other.last and other.first < self.last;
    }
};

pub const RowRanges = struct {
    /// Caller-owned storage; may be empty. Always satisfies the invariants above.
    ranges: []RowRange = &.{},

    const Self = @This();

    /// An empty set (skips every row).
    pub fn empty() Self {
        return .{ .ranges = &.{} };
    }

    /// The full range `[0, num_rows)`. When `num_rows == 0` returns empty.
    pub fn full(allocator: std.mem.Allocator, num_rows: u64) !Self {
        if (num_rows == 0) return empty();
        const out = try allocator.alloc(RowRange, 1);
        out[0] = .{ .first = 0, .last = num_rows };
        return .{ .ranges = out };
    }

    /// A single range `[first, last)`. Requires `first < last`.
    pub fn fromSingle(allocator: std.mem.Allocator, first: u64, last: u64) !Self {
        if (first >= last) return empty();
        const out = try allocator.alloc(RowRange, 1);
        out[0] = .{ .first = first, .last = last };
        return .{ .ranges = out };
    }

    /// Build from a caller-supplied slice of (possibly unsorted, overlapping) ranges.
    /// Normalises into sorted, disjoint, merged form. Empty input yields empty output.
    pub fn fromUnsorted(allocator: std.mem.Allocator, input: []const RowRange) !Self {
        if (input.len == 0) return empty();

        const tmp = try allocator.alloc(RowRange, input.len);
        defer allocator.free(tmp);
        @memcpy(tmp, input);

        std.mem.sort(RowRange, tmp, {}, struct {
            fn lt(_: void, a: RowRange, b: RowRange) bool {
                return a.first < b.first;
            }
        }.lt);

        // Merge in-place into `merged` so we never produce empty / touching ranges.
        var merged = try std.ArrayList(RowRange).initCapacity(allocator, input.len);
        errdefer merged.deinit(allocator);

        for (tmp) |r| {
            if (r.first >= r.last) continue; // skip empty
            if (merged.items.len == 0) {
                try merged.append(allocator, r);
            } else {
                const last = &merged.items[merged.items.len - 1];
                if (r.first <= last.last) {
                    // overlap or adjacent — merge
                    if (r.last > last.last) last.last = r.last;
                } else {
                    try merged.append(allocator, r);
                }
            }
        }

        return .{ .ranges = try merged.toOwnedSlice(allocator) };
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        if (self.ranges.len > 0) allocator.free(self.ranges);
        self.* = .{};
    }

    pub fn isEmpty(self: Self) bool {
        return self.ranges.len == 0;
    }

    /// True when this set covers `[0, num_rows)` exactly.
    pub fn isFull(self: Self, num_rows: u64) bool {
        if (num_rows == 0) return self.isEmpty();
        return self.ranges.len == 1 and
            self.ranges[0].first == 0 and
            self.ranges[0].last == num_rows;
    }

    pub fn rowCount(self: Self) u64 {
        var total: u64 = 0;
        for (self.ranges) |r| total += r.count();
        return total;
    }

    /// Binary-search containment check. O(log n) in the number of ranges.
    pub fn contains(self: Self, row: u64) bool {
        var lo: usize = 0;
        var hi: usize = self.ranges.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const r = self.ranges[mid];
            if (row < r.first) {
                hi = mid;
            } else if (row >= r.last) {
                lo = mid + 1;
            } else {
                return true;
            }
        }
        return false;
    }

    /// Intersect two ranges. Result is newly allocated and owned by caller.
    pub fn intersect(allocator: std.mem.Allocator, a: Self, b: Self) !Self {
        if (a.isEmpty() or b.isEmpty()) return empty();

        var out = try std.ArrayList(RowRange).initCapacity(allocator, @max(a.ranges.len, b.ranges.len));
        errdefer out.deinit(allocator);

        var i: usize = 0;
        var j: usize = 0;
        while (i < a.ranges.len and j < b.ranges.len) {
            const ra = a.ranges[i];
            const rb = b.ranges[j];
            const first = @max(ra.first, rb.first);
            const last = @min(ra.last, rb.last);
            if (first < last) {
                try out.append(allocator, .{ .first = first, .last = last });
            }
            // Advance whichever range ends first.
            if (ra.last <= rb.last) {
                i += 1;
            } else {
                j += 1;
            }
        }

        return .{ .ranges = try out.toOwnedSlice(allocator) };
    }

    /// Union two ranges. Result is newly allocated and owned by caller.
    pub fn unionRanges(allocator: std.mem.Allocator, a: Self, b: Self) !Self {
        if (a.isEmpty()) return cloneSlice(allocator, b.ranges);
        if (b.isEmpty()) return cloneSlice(allocator, a.ranges);

        var out = try std.ArrayList(RowRange).initCapacity(allocator, a.ranges.len + b.ranges.len);
        errdefer out.deinit(allocator);

        var i: usize = 0;
        var j: usize = 0;
        while (i < a.ranges.len or j < b.ranges.len) {
            const pick_a = j >= b.ranges.len or
                (i < a.ranges.len and a.ranges[i].first <= b.ranges[j].first);
            const next = if (pick_a) a.ranges[i] else b.ranges[j];
            if (pick_a) i += 1 else j += 1;

            if (out.items.len == 0) {
                try out.append(allocator, next);
            } else {
                const last = &out.items[out.items.len - 1];
                if (next.first <= last.last) {
                    if (next.last > last.last) last.last = next.last;
                } else {
                    try out.append(allocator, next);
                }
            }
        }

        return .{ .ranges = try out.toOwnedSlice(allocator) };
    }

    fn cloneSlice(allocator: std.mem.Allocator, src: []const RowRange) !Self {
        if (src.len == 0) return empty();
        const out = try allocator.alloc(RowRange, src.len);
        @memcpy(out, src);
        return .{ .ranges = out };
    }
};

// =============================================================================
// Tests
// =============================================================================

const testing = std.testing;

test "full and empty constructors" {
    var full = try RowRanges.full(testing.allocator, 100);
    defer full.deinit(testing.allocator);
    try testing.expect(full.isFull(100));
    try testing.expectEqual(@as(u64, 100), full.rowCount());

    const zero = try RowRanges.full(testing.allocator, 0);
    try testing.expect(zero.isEmpty());
    try testing.expect(zero.isFull(0));

    const empty = RowRanges.empty();
    try testing.expect(empty.isEmpty());
    try testing.expectEqual(@as(u64, 0), empty.rowCount());
}

test "fromUnsorted normalises overlap and adjacency" {
    const input = [_]RowRange{
        .{ .first = 20, .last = 30 }, // out of order
        .{ .first = 0, .last = 10 },
        .{ .first = 10, .last = 15 }, // adjacent -> merge with previous
        .{ .first = 25, .last = 40 }, // overlaps 20..30 -> merge
        .{ .first = 50, .last = 50 }, // empty -> dropped
    };
    var rr = try RowRanges.fromUnsorted(testing.allocator, &input);
    defer rr.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 2), rr.ranges.len);
    try testing.expectEqual(@as(u64, 0), rr.ranges[0].first);
    try testing.expectEqual(@as(u64, 15), rr.ranges[0].last);
    try testing.expectEqual(@as(u64, 20), rr.ranges[1].first);
    try testing.expectEqual(@as(u64, 40), rr.ranges[1].last);
}

test "contains" {
    const input = [_]RowRange{
        .{ .first = 0, .last = 10 },
        .{ .first = 20, .last = 30 },
        .{ .first = 100, .last = 200 },
    };
    var rr = try RowRanges.fromUnsorted(testing.allocator, &input);
    defer rr.deinit(testing.allocator);

    try testing.expect(rr.contains(0));
    try testing.expect(rr.contains(9));
    try testing.expect(!rr.contains(10));
    try testing.expect(rr.contains(25));
    try testing.expect(!rr.contains(99));
    try testing.expect(rr.contains(199));
    try testing.expect(!rr.contains(200));
}

test "intersect" {
    const a_input = [_]RowRange{
        .{ .first = 0, .last = 10 },
        .{ .first = 20, .last = 30 },
        .{ .first = 40, .last = 50 },
    };
    const b_input = [_]RowRange{
        .{ .first = 5, .last = 25 },
        .{ .first = 45, .last = 100 },
    };
    var a = try RowRanges.fromUnsorted(testing.allocator, &a_input);
    defer a.deinit(testing.allocator);
    var b = try RowRanges.fromUnsorted(testing.allocator, &b_input);
    defer b.deinit(testing.allocator);

    var r = try RowRanges.intersect(testing.allocator, a, b);
    defer r.deinit(testing.allocator);

    // Expect: [5,10) ∪ [20,25) ∪ [45,50)
    try testing.expectEqual(@as(usize, 3), r.ranges.len);
    try testing.expectEqual(@as(u64, 5), r.ranges[0].first);
    try testing.expectEqual(@as(u64, 10), r.ranges[0].last);
    try testing.expectEqual(@as(u64, 20), r.ranges[1].first);
    try testing.expectEqual(@as(u64, 25), r.ranges[1].last);
    try testing.expectEqual(@as(u64, 45), r.ranges[2].first);
    try testing.expectEqual(@as(u64, 50), r.ranges[2].last);
}

test "intersect with empty is empty" {
    var a = try RowRanges.full(testing.allocator, 100);
    defer a.deinit(testing.allocator);
    const b = RowRanges.empty();

    var r = try RowRanges.intersect(testing.allocator, a, b);
    defer r.deinit(testing.allocator);
    try testing.expect(r.isEmpty());
}

test "intersect with full is unchanged" {
    const a_input = [_]RowRange{
        .{ .first = 3, .last = 7 },
        .{ .first = 10, .last = 12 },
    };
    var a = try RowRanges.fromUnsorted(testing.allocator, &a_input);
    defer a.deinit(testing.allocator);
    var b = try RowRanges.full(testing.allocator, 1000);
    defer b.deinit(testing.allocator);

    var r = try RowRanges.intersect(testing.allocator, a, b);
    defer r.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 2), r.ranges.len);
    try testing.expectEqual(@as(u64, 3), r.ranges[0].first);
    try testing.expectEqual(@as(u64, 7), r.ranges[0].last);
    try testing.expectEqual(@as(u64, 10), r.ranges[1].first);
    try testing.expectEqual(@as(u64, 12), r.ranges[1].last);
}

test "union merges adjacent and overlapping" {
    const a_input = [_]RowRange{
        .{ .first = 0, .last = 10 },
        .{ .first = 30, .last = 40 },
    };
    const b_input = [_]RowRange{
        .{ .first = 10, .last = 20 }, // adjacent to 0..10
        .{ .first = 35, .last = 50 }, // overlaps 30..40
    };
    var a = try RowRanges.fromUnsorted(testing.allocator, &a_input);
    defer a.deinit(testing.allocator);
    var b = try RowRanges.fromUnsorted(testing.allocator, &b_input);
    defer b.deinit(testing.allocator);

    var r = try RowRanges.unionRanges(testing.allocator, a, b);
    defer r.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 2), r.ranges.len);
    try testing.expectEqual(@as(u64, 0), r.ranges[0].first);
    try testing.expectEqual(@as(u64, 20), r.ranges[0].last);
    try testing.expectEqual(@as(u64, 30), r.ranges[1].first);
    try testing.expectEqual(@as(u64, 50), r.ranges[1].last);
}
