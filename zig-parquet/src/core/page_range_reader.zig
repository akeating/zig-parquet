//! Selects the pages of a column chunk whose row ranges intersect a caller-supplied
//! `RowRanges`, from the chunk's offset index, and gives the byte range of each.
//!
//! `leaf_decoder.PageReader.fromSpans` reads the selected pages from the source as they are
//! decoded, one range of adjacent pages (at most `max_span_bytes`) per read. Pages that are
//! not selected are not read from the source and not decompressed.
//!
//! The dictionary page (if any) is always selected, since every data page in the chunk may
//! reference it.

const std = @import("std");
const safe = @import("safe.zig");
const format = @import("format.zig");
const leaf_decoder = @import("leaf_decoder.zig");
const row_ranges = @import("row_ranges.zig");

pub const RowRanges = row_ranges.RowRanges;
pub const RowRange = row_ranges.RowRange;
pub const Span = leaf_decoder.PageReader.Span;

pub const Error = error{
    OutOfMemory,
    InvalidPageIndex,
    InvalidArgument,
    IntegerOverflow,
};

/// One selected data page.
pub const SelectedPage = struct {
    /// Index into the `OffsetIndex.page_locations` slice.
    page_index: usize,
    /// First top-level row index within the row group.
    first_row_index: u64,
    /// Exclusive upper bound of this page's rows within the row group.
    /// Equals `rg_num_rows` for the final page.
    end_row_index: u64,
};

pub const PageSelection = struct {
    /// Byte ranges to read, in chunk order: the dictionary page, when the chunk has one, and
    /// the selected data pages. Pages that are adjacent in the file share a range while the
    /// range stays within `max_span_bytes`, so a run of kept pages is read with one read.
    spans: []Span,
    /// Selected data pages, in chunk order (ascending `page_index`).
    selected: []SelectedPage,

    pub fn deinit(self: *PageSelection, allocator: std.mem.Allocator) void {
        allocator.free(self.spans);
        allocator.free(self.selected);
        self.* = .{ .spans = &.{}, .selected = &.{} };
    }
};

/// Adjacent pages are joined into one range of at most this many bytes. A range is read
/// whole, so this bounds the compressed bytes held per column while a filtered read decodes
/// it; a single page larger than this is one range by itself.
pub const max_span_bytes: u64 = 8 << 20;

/// Selects the data pages of `chunk` that hold rows in `wanted`, and the dictionary page when
/// at least one data page is selected. No data is read.
pub fn selectPages(
    allocator: std.mem.Allocator,
    chunk: *const format.ColumnChunk,
    offset_index: *const format.OffsetIndex,
    wanted: RowRanges,
    rg_num_rows: u64,
) Error!PageSelection {
    const meta = chunk.meta_data orelse return error.InvalidArgument;
    const locations = offset_index.page_locations;
    if (locations.len == 0) return .{ .spans = &.{}, .selected = &.{} };

    var selected: std.ArrayList(SelectedPage) = .empty;
    errdefer selected.deinit(allocator);
    var page_spans: std.ArrayList(Span) = .empty;
    defer page_spans.deinit(allocator);

    // Pages cover rows `first_row_index[0]..rg_num_rows`; rows before the first page would
    // be in no page.
    if (locations[0].first_row_index != 0) return error.InvalidPageIndex;
    for (locations, 0..) |loc, i| {
        if (loc.offset < 0 or loc.compressed_page_size < 0 or loc.first_row_index < 0) {
            return error.InvalidPageIndex;
        }
        const first: u64 = @intCast(loc.first_row_index); // checked >= 0 above
        const end: u64 = if (i + 1 < locations.len) blk: {
            const next = locations[i + 1].first_row_index;
            if (next < 0) return error.InvalidPageIndex;
            break :blk @intCast(next); // checked >= 0 above
        } else rg_num_rows;
        if (end < first) return error.InvalidPageIndex;
        if (end == first) continue; // empty page — shouldn't happen but skip safely

        if (!rangeIntersects(wanted, first, end)) continue;

        // A page holds at least its header.
        if (loc.compressed_page_size == 0) return error.InvalidPageIndex;
        _ = std.math.add(i64, loc.offset, loc.compressed_page_size) catch return error.IntegerOverflow;
        try selected.append(allocator, .{ .page_index = i, .first_row_index = first, .end_row_index = end });
        try page_spans.append(allocator, .{
            .start = @intCast(loc.offset), // checked >= 0 above
            .size = @intCast(loc.compressed_page_size), // checked > 0 above
        });
    }
    if (selected.items.len == 0) {
        selected.deinit(allocator);
        return .{ .spans = &.{}, .selected = &.{} };
    }

    // The dictionary page, when there is one, is the chunk's first page and precedes the
    // first data page; it spans from its offset to the first data page's offset. Its offset
    // is `dictionary_page_offset`. Some writers leave that out and point `data_page_offset`
    // at the dictionary page instead, so bytes from `data_page_offset` to the first data page
    // are read as well. A `dictionary_page_offset` of 0 means no dictionary (0 is before the
    // PAR1 magic bytes).
    var dictionary: ?Span = null;
    const chunk_first_data = locations[0].offset;
    const declared_dict: ?i64 = if (meta.dictionary_page_offset) |d| (if (d > 0) d else null) else null;
    if (declared_dict) |d| {
        if (chunk_first_data <= d) return error.InvalidPageIndex;
    }
    const dict_off = declared_dict orelse meta.data_page_offset;
    if (dict_off >= 0 and dict_off < chunk_first_data) {
        dictionary = .{
            .start = @intCast(dict_off), // checked >= 0 above
            .size = @intCast(chunk_first_data - dict_off), // dict_off < chunk_first_data
        };
    }

    var spans: std.ArrayList(Span) = .empty;
    errdefer spans.deinit(allocator);
    if (dictionary) |d| try spans.append(allocator, d);
    for (page_spans.items) |span| {
        if (spans.items.len > 0) {
            const last = &spans.items[spans.items.len - 1];
            if (last.start + last.size == span.start and last.size + span.size <= max_span_bytes) {
                last.size += span.size;
                continue;
            }
        }
        try spans.append(allocator, span);
    }
    const owned_spans = try spans.toOwnedSlice(allocator);
    errdefer allocator.free(owned_spans);
    return .{ .spans = owned_spans, .selected = try selected.toOwnedSlice(allocator) };
}

fn rangeIntersects(wanted: RowRanges, first: u64, end: u64) bool {
    if (wanted.isEmpty()) return false;
    for (wanted.ranges) |r| {
        if (r.first >= end) break; // ranges sorted — no further overlap possible
        if (r.last > first) return true;
    }
    return false;
}

// =============================================================================
// Tests
// =============================================================================

const testing = std.testing;

fn expectSpans(expected: []const Span, actual: []const Span) !void {
    try testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |e, a| {
        try testing.expectEqual(e.start, a.start);
        try testing.expectEqual(e.size, a.size);
    }
}

test "selectPages: skips pages outside wanted range" {
    // 4 data pages at offsets 0, 100, 200, 300; each 100 bytes long.
    // first_row_index: 0, 25, 50, 75.
    var locs = [_]format.PageLocation{
        .{ .offset = 0, .compressed_page_size = 100, .first_row_index = 0 },
        .{ .offset = 100, .compressed_page_size = 100, .first_row_index = 25 },
        .{ .offset = 200, .compressed_page_size = 100, .first_row_index = 50 },
        .{ .offset = 300, .compressed_page_size = 100, .first_row_index = 75 },
    };
    const oi = format.OffsetIndex{ .page_locations = &locs };
    const chunk = format.ColumnChunk{
        .meta_data = format.ColumnMetaData{ .data_page_offset = 0 },
    };

    // Wanted: rows [30, 65) → pages 1 and 2 only.
    var wanted = try RowRanges.fromSingle(testing.allocator, 30, 65);
    defer wanted.deinit(testing.allocator);

    var result = try selectPages(testing.allocator, &chunk, &oi, wanted, 100);
    defer result.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 2), result.selected.len);
    try testing.expectEqual(@as(usize, 1), result.selected[0].page_index);
    try testing.expectEqual(@as(usize, 2), result.selected[1].page_index);
    // No dictionary. Pages 1 and 2 are adjacent, so they are one range.
    try expectSpans(&.{.{ .start = 100, .size = 200 }}, result.spans);

    // The selected pages hold rows 25..50 and 50..75.
    try testing.expectEqual(@as(u64, 25), result.selected[0].first_row_index);
    try testing.expectEqual(@as(u64, 50), result.selected[0].end_row_index);
    try testing.expectEqual(@as(u64, 50), result.selected[1].first_row_index);
    try testing.expectEqual(@as(u64, 75), result.selected[1].end_row_index);
}

test "selectPages: non-contiguous pages" {
    // Pages 0, 1, 2, 3 with a gap between 1 and 2 (offsets 0, 100, 250, 350).
    var locs = [_]format.PageLocation{
        .{ .offset = 0, .compressed_page_size = 100, .first_row_index = 0 },
        .{ .offset = 100, .compressed_page_size = 100, .first_row_index = 25 },
        .{ .offset = 250, .compressed_page_size = 100, .first_row_index = 50 },
        .{ .offset = 350, .compressed_page_size = 50, .first_row_index = 75 },
    };
    const oi = format.OffsetIndex{ .page_locations = &locs };
    const chunk = format.ColumnChunk{
        .meta_data = format.ColumnMetaData{ .data_page_offset = 0 },
    };

    // Want pages 0, 2, 3.
    var wanted = try RowRanges.fromUnsorted(testing.allocator, &[_]RowRange{
        .{ .first = 0, .last = 25 },
        .{ .first = 50, .last = 100 },
    });
    defer wanted.deinit(testing.allocator);

    var result = try selectPages(testing.allocator, &chunk, &oi, wanted, 100);
    defer result.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 3), result.selected.len);
    // Page 1 is not selected, so page 0 is a range by itself; pages 2 and 3 are adjacent.
    try expectSpans(&.{
        .{ .start = 0, .size = 100 },
        .{ .start = 250, .size = 150 },
    }, result.spans);
}

test "selectPages: empty wanted produces empty result" {
    var locs = [_]format.PageLocation{
        .{ .offset = 0, .compressed_page_size = 100, .first_row_index = 0 },
    };
    const oi = format.OffsetIndex{ .page_locations = &locs };
    const chunk = format.ColumnChunk{
        .meta_data = format.ColumnMetaData{ .data_page_offset = 0 },
    };

    var result = try selectPages(testing.allocator, &chunk, &oi, RowRanges.empty(), 25);
    defer result.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 0), result.selected.len);
    try testing.expectEqual(@as(usize, 0), result.spans.len);
}

test "selectPages: includes dictionary page when present" {
    // Dict at 4..54 (after PAR1 magic), data pages at 54, 154, 254.
    // dict_page_offset=0 is the "no dict" sentinel, so we use offset 4.
    var locs = [_]format.PageLocation{
        .{ .offset = 54, .compressed_page_size = 100, .first_row_index = 0 },
        .{ .offset = 154, .compressed_page_size = 100, .first_row_index = 10 },
        .{ .offset = 254, .compressed_page_size = 100, .first_row_index = 20 },
    };
    const oi = format.OffsetIndex{ .page_locations = &locs };
    const chunk = format.ColumnChunk{
        .meta_data = format.ColumnMetaData{
            .data_page_offset = 54,
            .dictionary_page_offset = 4,
        },
    };

    // Want rows 12..18 → page 1 only.
    var wanted = try RowRanges.fromSingle(testing.allocator, 12, 18);
    defer wanted.deinit(testing.allocator);

    var result = try selectPages(testing.allocator, &chunk, &oi, wanted, 30);
    defer result.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 1), result.selected.len);
    try testing.expectEqual(@as(usize, 1), result.selected[0].page_index);
    // The dictionary at 4..54, then page 1.
    try expectSpans(&.{ .{ .start = 4, .size = 50 }, .{ .start = 154, .size = 100 } }, result.spans);
}

test "selectPages: selects a dictionary page that data_page_offset points at" {
    // Some writers leave out dictionary_page_offset and set data_page_offset to the
    // dictionary page at 4..54; data pages are at 54 and 154.
    var locs = [_]format.PageLocation{
        .{ .offset = 54, .compressed_page_size = 100, .first_row_index = 0 },
        .{ .offset = 154, .compressed_page_size = 100, .first_row_index = 10 },
    };
    const oi = format.OffsetIndex{ .page_locations = &locs };
    const chunk = format.ColumnChunk{
        .meta_data = format.ColumnMetaData{ .data_page_offset = 4 },
    };

    var wanted = try RowRanges.fromSingle(testing.allocator, 12, 18);
    defer wanted.deinit(testing.allocator);

    var result = try selectPages(testing.allocator, &chunk, &oi, wanted, 20);
    defer result.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 1), result.selected.len);
    try expectSpans(&.{ .{ .start = 4, .size = 50 }, .{ .start = 154, .size = 100 } }, result.spans);
}

test "selectPages: joins adjacent pages up to max_span_bytes" {
    // 3 adjacent data pages of 3 MiB: pages 0 and 1 make 6 MiB; with page 2 the range would
    // be 9 MiB, over the 8 MiB limit.
    const mib: i32 = 1 << 20;
    var locs = [_]format.PageLocation{
        .{ .offset = 4, .compressed_page_size = 3 * mib, .first_row_index = 0 },
        .{ .offset = 4 + 3 * mib, .compressed_page_size = 3 * mib, .first_row_index = 10 },
        .{ .offset = 4 + 6 * mib, .compressed_page_size = 3 * mib, .first_row_index = 20 },
    };
    const oi = format.OffsetIndex{ .page_locations = &locs };
    const chunk = format.ColumnChunk{ .meta_data = format.ColumnMetaData{ .data_page_offset = 4 } };
    var wanted = try RowRanges.fromSingle(testing.allocator, 0, 30);
    defer wanted.deinit(testing.allocator);

    var result = try selectPages(testing.allocator, &chunk, &oi, wanted, 30);
    defer result.deinit(testing.allocator);
    try testing.expectEqual(@as(usize, 3), result.selected.len);
    try expectSpans(&.{
        .{ .start = 4, .size = 6 << 20 },
        .{ .start = 4 + (6 << 20), .size = 3 << 20 },
    }, result.spans);
}

test "selectPages: an offset index whose first page is not at row 0 is invalid" {
    var locs = [_]format.PageLocation{
        .{ .offset = 0, .compressed_page_size = 100, .first_row_index = 5 },
    };
    const oi = format.OffsetIndex{ .page_locations = &locs };
    const chunk = format.ColumnChunk{
        .meta_data = format.ColumnMetaData{ .data_page_offset = 0 },
    };

    var wanted = try RowRanges.fromSingle(testing.allocator, 0, 10);
    defer wanted.deinit(testing.allocator);
    try testing.expectError(error.InvalidPageIndex, selectPages(testing.allocator, &chunk, &oi, wanted, 10));
}

test "selectPages: rejects selected pages of size 0 and pages that end past maxInt(i64)" {
    const chunk = format.ColumnChunk{
        .meta_data = format.ColumnMetaData{ .data_page_offset = 0 },
    };
    var wanted = try RowRanges.fromSingle(testing.allocator, 0, 10);
    defer wanted.deinit(testing.allocator);

    var empty_page = [_]format.PageLocation{.{ .offset = 4, .compressed_page_size = 0, .first_row_index = 0 }};
    try testing.expectError(error.InvalidPageIndex, selectPages(testing.allocator, &chunk, &.{ .page_locations = &empty_page }, wanted, 10));
    var past_end = [_]format.PageLocation{.{ .offset = std.math.maxInt(i64) - 10, .compressed_page_size = 100, .first_row_index = 0 }};
    try testing.expectError(error.IntegerOverflow, selectPages(testing.allocator, &chunk, &.{ .page_locations = &past_end }, wanted, 10));
}
