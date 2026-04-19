//! Page-index writer support.
//!
//! A `PageIndexBuilder` accumulates one `PageEntry` per data page as a column
//! chunk is written, then produces `OffsetIndex` + `ColumnIndex` at row-group
//! flush time.
//!
//! In the current first cut the single-data-page writer path produces exactly
//! one entry per chunk. Multi-page support is just "append more entries" —
//! nothing else changes at this layer.
//!
//! A column opts out of `ColumnIndex` emission (via `is_sort_order_valid = false`)
//! when the Parquet spec doesn't define a total ordering for its physical /
//! logical type — GEOMETRY, GEOGRAPHY, INTERVAL (fixed-len 12), and the legacy
//! unsigned-stored-as-signed case. `OffsetIndex` is always safe to emit.

const std = @import("std");
const safe = @import("safe.zig");
const format = @import("format.zig");
const thrift = @import("thrift/mod.zig");

pub const PageEntry = struct {
    /// Absolute file offset of this page's header.
    offset: i64,
    /// Compressed size including the page header.
    compressed_page_size: i32,
    /// First top-level row index (within the row group) covered by this page.
    first_row_index: i64,
    /// Number of top-level rows this page covers. Used to set
    /// `PageLocation.first_row_index` on the *next* page.
    num_rows: i64,
    /// PLAIN-encoded min. Ignored when `all_null = true`.
    min_bytes: []u8,
    /// PLAIN-encoded max. Ignored when `all_null = true`.
    max_bytes: []u8,
    null_count: i64,
    /// True when every value on this page is null. When set, min/max are
    /// written as zero-length byte strings per the ColumnIndex spec.
    all_null: bool,
};

pub const PageIndexBuilder = struct {
    allocator: std.mem.Allocator,
    physical_type: format.PhysicalType,
    /// Whether this column emits a ColumnIndex. Set to false for
    /// GEOMETRY/GEOGRAPHY/INTERVAL/legacy-unsigned types.
    is_sort_order_valid: bool,
    entries: std.ArrayListUnmanaged(PageEntry) = .empty,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, physical_type: format.PhysicalType, is_sort_order_valid: bool) Self {
        return .{
            .allocator = allocator,
            .physical_type = physical_type,
            .is_sort_order_valid = is_sort_order_valid,
        };
    }

    pub fn deinit(self: *Self) void {
        for (self.entries.items) |entry| {
            self.allocator.free(entry.min_bytes);
            self.allocator.free(entry.max_bytes);
        }
        self.entries.deinit(self.allocator);
        self.* = .{
            .allocator = self.allocator,
            .physical_type = self.physical_type,
            .is_sort_order_valid = false,
        };
    }

    /// Record one page. The builder takes ownership of `min_bytes` and
    /// `max_bytes` via dupe; the caller keeps its own copies.
    pub fn recordPage(
        self: *Self,
        offset: i64,
        compressed_page_size: i32,
        first_row_index: i64,
        num_rows: i64,
        min_bytes: []const u8,
        max_bytes: []const u8,
        null_count: i64,
        all_null: bool,
    ) !void {
        const mn = try self.allocator.dupe(u8, min_bytes);
        errdefer self.allocator.free(mn);
        const mx = try self.allocator.dupe(u8, max_bytes);
        errdefer self.allocator.free(mx);

        try self.entries.append(self.allocator, .{
            .offset = offset,
            .compressed_page_size = compressed_page_size,
            .first_row_index = first_row_index,
            .num_rows = num_rows,
            .min_bytes = mn,
            .max_bytes = mx,
            .null_count = null_count,
            .all_null = all_null,
        });
    }

    pub fn pageCount(self: *const Self) usize {
        return self.entries.items.len;
    }

    /// Produce the OffsetIndex. Caller owns returned slices.
    pub fn buildOffsetIndex(self: *const Self, allocator: std.mem.Allocator) !format.OffsetIndex {
        const locs = try allocator.alloc(format.PageLocation, self.entries.items.len);
        errdefer allocator.free(locs);
        for (self.entries.items, 0..) |entry, i| {
            locs[i] = .{
                .offset = entry.offset,
                .compressed_page_size = entry.compressed_page_size,
                .first_row_index = entry.first_row_index,
            };
        }
        return .{ .page_locations = locs };
    }

    /// Produce the ColumnIndex, or null when sort order is not well-defined
    /// for this column's type. Caller owns returned slices.
    pub fn buildColumnIndex(self: *const Self, allocator: std.mem.Allocator) !?format.ColumnIndex {
        if (!self.is_sort_order_valid) return null;
        if (self.entries.items.len == 0) return null;

        const n = self.entries.items.len;

        const null_pages = try allocator.alloc(bool, n);
        errdefer allocator.free(null_pages);

        const mins = try allocator.alloc([]const u8, n);
        var mins_count: usize = 0;
        errdefer {
            for (mins[0..mins_count]) |m| allocator.free(m);
            allocator.free(mins);
        }
        const maxs = try allocator.alloc([]const u8, n);
        var maxs_count: usize = 0;
        errdefer {
            for (maxs[0..maxs_count]) |m| allocator.free(m);
            allocator.free(maxs);
        }
        const null_counts = try allocator.alloc(i64, n);
        errdefer allocator.free(null_counts);

        for (self.entries.items, 0..) |entry, i| {
            null_pages[i] = entry.all_null;
            if (entry.all_null) {
                mins[i] = try allocator.dupe(u8, &[_]u8{});
                mins_count = i + 1;
                maxs[i] = try allocator.dupe(u8, &[_]u8{});
                maxs_count = i + 1;
            } else {
                mins[i] = try allocator.dupe(u8, entry.min_bytes);
                mins_count = i + 1;
                maxs[i] = try allocator.dupe(u8, entry.max_bytes);
                maxs_count = i + 1;
            }
            null_counts[i] = entry.null_count;
        }

        const boundary_order = self.computeBoundaryOrder();

        return format.ColumnIndex{
            .null_pages = null_pages,
            .min_values = mins,
            .max_values = maxs,
            .boundary_order = boundary_order,
            .null_counts = null_counts,
        };
    }

    /// Classify the per-page min/max sequence. Pages that are all-null are
    /// ignored when determining order, since their min/max are undefined.
    fn computeBoundaryOrder(self: *const Self) format.BoundaryOrder {
        var ascending = true;
        var descending = true;
        var prev_min: ?[]const u8 = null;
        var prev_max: ?[]const u8 = null;

        for (self.entries.items) |entry| {
            if (entry.all_null) continue;
            if (prev_min) |pm| {
                const min_cmp = self.compare(pm, entry.min_bytes);
                const max_cmp = self.compare(prev_max.?, entry.max_bytes);
                if (min_cmp == .gt or max_cmp == .gt) ascending = false;
                if (min_cmp == .lt or max_cmp == .lt) descending = false;
            }
            prev_min = entry.min_bytes;
            prev_max = entry.max_bytes;
        }

        if (ascending and descending) return .ascending; // all equal — pick ascending
        if (ascending) return .ascending;
        if (descending) return .descending;
        return .unordered;
    }

    /// Typed comparison of two PLAIN-encoded values.
    fn compare(self: *const Self, a: []const u8, b: []const u8) std.math.Order {
        return switch (self.physical_type) {
            .int32 => blk: {
                if (a.len < 4 or b.len < 4) break :blk std.mem.order(u8, a, b);
                break :blk std.math.order(
                    std.mem.readInt(i32, a[0..4], .little),
                    std.mem.readInt(i32, b[0..4], .little),
                );
            },
            .int64 => blk: {
                if (a.len < 8 or b.len < 8) break :blk std.mem.order(u8, a, b);
                break :blk std.math.order(
                    std.mem.readInt(i64, a[0..8], .little),
                    std.mem.readInt(i64, b[0..8], .little),
                );
            },
            .float => blk: {
                if (a.len < 4 or b.len < 4) break :blk std.mem.order(u8, a, b);
                const af: f32 = @bitCast(std.mem.readInt(u32, a[0..4], .little));
                const bf: f32 = @bitCast(std.mem.readInt(u32, b[0..4], .little));
                break :blk std.math.order(af, bf);
            },
            .double => blk: {
                if (a.len < 8 or b.len < 8) break :blk std.mem.order(u8, a, b);
                const ad: f64 = @bitCast(std.mem.readInt(u64, a[0..8], .little));
                const bd: f64 = @bitCast(std.mem.readInt(u64, b[0..8], .little));
                break :blk std.math.order(ad, bd);
            },
            .boolean => std.mem.order(u8, a, b),
            // byte_array / fixed_len_byte_array use lexicographic order — correct
            // for UTF-8 strings and for the generic byte case. Collation-aware
            // comparisons aren't supported here yet.
            else => std.mem.order(u8, a, b),
        };
    }
};

/// True when a column's physical+logical type has a total order defined by
/// the Parquet spec. Columns for which it isn't should skip ColumnIndex
/// emission (but still emit OffsetIndex).
pub fn isSortOrderValid(schema_element: *const format.SchemaElement) bool {
    // GEOMETRY / GEOGRAPHY logical types: no well-defined total order.
    if (schema_element.logical_type) |lt| {
        switch (lt) {
            .geometry, .geography => return false,
            else => {},
        }
    }
    // INTERVAL is fixed_len_byte_array(12) with converted_type = INTERVAL.
    if (schema_element.converted_type) |ct| {
        if (ct == format.ConvertedType.INTERVAL) return false;
    }
    return true;
}

/// Is the sort-order-valid flag `false` for legacy-unsigned-as-signed
/// situations we care about? (Kept as a separate predicate so writer callers
/// can extend it for cases we haven't surfaced yet.)
pub fn isLegacyUnsignedAsSigned(_: *const format.SchemaElement) bool {
    return false;
}

// =============================================================================
// Tests
// =============================================================================

const testing = std.testing;

fn i32Bytes(v: i32) [4]u8 {
    var b: [4]u8 = undefined;
    std.mem.writeInt(i32, &b, v, .little);
    return b;
}

test "PageIndexBuilder: single page round-trip via OffsetIndex + ColumnIndex" {
    var b = PageIndexBuilder.init(testing.allocator, .int32, true);
    defer b.deinit();

    const mn = i32Bytes(5);
    const mx = i32Bytes(42);
    try b.recordPage(1000, 256, 0, 100, &mn, &mx, 3, false);

    var oi = try b.buildOffsetIndex(testing.allocator);
    defer oi.deinit(testing.allocator);
    try testing.expectEqual(@as(usize, 1), oi.page_locations.len);
    try testing.expectEqual(@as(i64, 1000), oi.page_locations[0].offset);
    try testing.expectEqual(@as(i32, 256), oi.page_locations[0].compressed_page_size);
    try testing.expectEqual(@as(i64, 0), oi.page_locations[0].first_row_index);

    var ci = (try b.buildColumnIndex(testing.allocator)).?;
    defer ci.deinit(testing.allocator);
    try testing.expectEqual(@as(usize, 1), ci.null_pages.len);
    try testing.expectEqual(false, ci.null_pages[0]);
    try testing.expectEqualSlices(u8, &mn, ci.min_values[0]);
    try testing.expectEqualSlices(u8, &mx, ci.max_values[0]);
    try testing.expectEqual(format.BoundaryOrder.ascending, ci.boundary_order);
    try testing.expectEqual(@as(i64, 3), ci.null_counts.?[0]);
}

test "PageIndexBuilder: ascending boundary detected over multiple pages" {
    var b = PageIndexBuilder.init(testing.allocator, .int32, true);
    defer b.deinit();

    const pages = [_]struct { mn: i32, mx: i32 }{
        .{ .mn = 0, .mx = 99 },
        .{ .mn = 100, .mx = 199 },
        .{ .mn = 200, .mx = 299 },
    };
    for (pages, 0..) |p, i| {
        const mnb = i32Bytes(p.mn);
        const mxb = i32Bytes(p.mx);
        try b.recordPage(@intCast(1000 + i * 500), 400, @intCast(i * 100), 100, &mnb, &mxb, 0, false);
    }

    var ci = (try b.buildColumnIndex(testing.allocator)).?;
    defer ci.deinit(testing.allocator);
    try testing.expectEqual(format.BoundaryOrder.ascending, ci.boundary_order);
}

test "PageIndexBuilder: unordered when pages mix direction" {
    var b = PageIndexBuilder.init(testing.allocator, .int32, true);
    defer b.deinit();

    const mins = [_]i32{ 0, 100, 50 };
    const maxs = [_]i32{ 99, 199, 149 };
    for (mins, maxs, 0..) |mn, mx, i| {
        const mnb = i32Bytes(mn);
        const mxb = i32Bytes(mx);
        try b.recordPage(@intCast(i), 10, @intCast(i * 100), 100, &mnb, &mxb, 0, false);
    }

    var ci = (try b.buildColumnIndex(testing.allocator)).?;
    defer ci.deinit(testing.allocator);
    try testing.expectEqual(format.BoundaryOrder.unordered, ci.boundary_order);
}

test "PageIndexBuilder: all-null page skipped from boundary computation" {
    var b = PageIndexBuilder.init(testing.allocator, .int32, true);
    defer b.deinit();

    const mn0 = i32Bytes(0);
    const mx0 = i32Bytes(99);
    try b.recordPage(0, 10, 0, 100, &mn0, &mx0, 0, false);
    // Middle page is all-null — ignored when checking order.
    try b.recordPage(10, 10, 100, 100, "", "", 100, true);
    const mn2 = i32Bytes(200);
    const mx2 = i32Bytes(299);
    try b.recordPage(20, 10, 200, 100, &mn2, &mx2, 0, false);

    var ci = (try b.buildColumnIndex(testing.allocator)).?;
    defer ci.deinit(testing.allocator);
    try testing.expectEqual(format.BoundaryOrder.ascending, ci.boundary_order);
    try testing.expectEqual(true, ci.null_pages[1]);
    try testing.expectEqual(@as(usize, 0), ci.min_values[1].len);
}

test "PageIndexBuilder: suppresses ColumnIndex when sort order invalid" {
    var b = PageIndexBuilder.init(testing.allocator, .fixed_len_byte_array, false);
    defer b.deinit();

    try b.recordPage(0, 10, 0, 5, "aaa", "zzz", 0, false);
    const ci = try b.buildColumnIndex(testing.allocator);
    try testing.expect(ci == null);

    // But OffsetIndex is still produced.
    var oi = try b.buildOffsetIndex(testing.allocator);
    defer oi.deinit(testing.allocator);
    try testing.expectEqual(@as(usize, 1), oi.page_locations.len);
}

test "isSortOrderValid: geometry / interval suppressed" {
    var geom_el = format.SchemaElement{
        .type_ = .byte_array,
        .name = "g",
        .logical_type = .{ .geometry = .{ .crs = null } },
    };
    try testing.expect(!isSortOrderValid(&geom_el));

    var interval_el = format.SchemaElement{
        .type_ = .fixed_len_byte_array,
        .type_length = 12,
        .name = "i",
        .converted_type = format.ConvertedType.INTERVAL,
    };
    try testing.expect(!isSortOrderValid(&interval_el));

    var plain_el = format.SchemaElement{ .type_ = .int32, .name = "x" };
    try testing.expect(isSortOrderValid(&plain_el));
}
