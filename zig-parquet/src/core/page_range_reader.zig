//! Lazy reader that fetches only the pages of a column chunk whose row ranges
//! intersect a caller-supplied `RowRanges`.
//!
//! Produces a byte buffer laid out as `[dict_page?][data_page_k0][data_page_k1]...`
//! — i.e. exactly what the existing `PageIterator` / `readColumnDynamic`
//! already know how to consume. Pages that are not selected are never read
//! from the source and never decompressed, which is the whole point of the
//! page-index skip path.
//!
//! The dictionary page (if any) is always included, since every data page in
//! the chunk may reference it.
//!
//! Contiguous runs of selected pages are coalesced into a single `readAt` for
//! remote-I/O friendliness.

const std = @import("std");
const safe = @import("safe.zig");
const format = @import("format.zig");
const seekable_reader = @import("seekable_reader.zig");
const row_ranges = @import("row_ranges.zig");

pub const SeekableReader = seekable_reader.SeekableReader;
pub const RowRanges = row_ranges.RowRanges;
pub const RowRange = row_ranges.RowRange;

pub const Error = error{
    InputOutput,
    OutOfMemory,
    InvalidPageIndex,
    InvalidArgument,
    IntegerOverflow,
};

/// Describes one selected data page within the returned buffer.
pub const SelectedPage = struct {
    /// Index into the `OffsetIndex.page_locations` slice.
    page_index: usize,
    /// First top-level row index within the row group.
    first_row_index: u64,
    /// Exclusive upper bound of this page's rows within the row group.
    /// Equals `rg_num_rows` for the final page.
    end_row_index: u64,
    /// Offset within `PageRangeResult.data` where this page (header+body)
    /// begins. Dict page — if present — occupies `[0, data[0].buffer_offset)`.
    buffer_offset: usize,
    /// Number of bytes in `PageRangeResult.data` belonging to this page.
    buffer_length: usize,
};

pub const PageRangeResult = struct {
    /// `[dict_page?][selected data pages concatenated in chunk order]`.
    /// Drop-in replacement for `readColumnChunkData`'s output for a filtered read.
    data: []u8,
    /// True when `data[0..selected[0].buffer_offset]` is a dictionary page.
    has_dictionary: bool,
    /// Selected data pages, in chunk order (ascending `page_index`).
    selected: []SelectedPage,

    pub fn deinit(self: *PageRangeResult, allocator: std.mem.Allocator) void {
        if (self.data.len > 0) allocator.free(self.data);
        if (self.selected.len > 0) allocator.free(self.selected);
        self.* = .{ .data = &.{}, .has_dictionary = false, .selected = &.{} };
    }

    /// Translate a decoded-row index (0-based within the decoded output) into
    /// the row-group-local row index. Returns null when `decoded_row` is past
    /// the total decoded row count.
    pub fn mapDecodedRow(self: *const PageRangeResult, decoded_row: u64) ?u64 {
        var seen: u64 = 0;
        for (self.selected) |sp| {
            const page_rows = sp.end_row_index - sp.first_row_index;
            if (decoded_row < seen + page_rows) {
                return sp.first_row_index + (decoded_row - seen);
            }
            seen += page_rows;
        }
        return null;
    }

    /// Total decoded rows across every selected page.
    pub fn totalDecodedRows(self: *const PageRangeResult) u64 {
        var total: u64 = 0;
        for (self.selected) |sp| total += sp.end_row_index - sp.first_row_index;
        return total;
    }
};

pub fn readSelectedPages(
    allocator: std.mem.Allocator,
    source: SeekableReader,
    chunk: *const format.ColumnChunk,
    offset_index: *const format.OffsetIndex,
    wanted: RowRanges,
    rg_num_rows: u64,
) Error!PageRangeResult {
    const meta = chunk.meta_data orelse return error.InvalidArgument;
    if (offset_index.page_locations.len == 0) {
        return .{ .data = &.{}, .has_dictionary = false, .selected = &.{} };
    }

    // Identify which data pages intersect `wanted`.
    var selected_raw = try std.ArrayList(SelectedPage).initCapacity(allocator, offset_index.page_locations.len);
    errdefer selected_raw.deinit(allocator);

    const n_pages = offset_index.page_locations.len;
    for (offset_index.page_locations, 0..) |loc, i| {
        if (loc.offset < 0 or loc.compressed_page_size < 0 or loc.first_row_index < 0) {
            return error.InvalidPageIndex;
        }
        const first: u64 = @intCast(loc.first_row_index);
        const end: u64 = if (i + 1 < n_pages) blk: {
            const next = offset_index.page_locations[i + 1].first_row_index;
            if (next < 0) return error.InvalidPageIndex;
            break :blk @intCast(next);
        } else rg_num_rows;
        if (end < first) return error.InvalidPageIndex;
        if (end == first) continue; // empty page — shouldn't happen but skip safely

        if (!rangeIntersects(wanted, first, end)) continue;

        const len = safe.castTo(usize, @as(u64, @intCast(loc.compressed_page_size))) catch return error.IntegerOverflow;
        try selected_raw.append(allocator, .{
            .page_index = i,
            .first_row_index = first,
            .end_row_index = end,
            .buffer_offset = 0, // filled in below
            .buffer_length = len,
        });
    }

    // Compute dict-page presence + size.
    // Per Parquet convention, a dict page (when present) immediately precedes
    // the first data page; its size is `first_data_page_offset - dict_offset`.
    var dict_info: ?struct { offset: u64, length: usize } = null;
    if (meta.dictionary_page_offset) |dict_off| {
        if (dict_off > 0) {
            const first_data_off = if (selected_raw.items.len > 0) blk: {
                const first_selected_loc = offset_index.page_locations[selected_raw.items[0].page_index];
                break :blk first_selected_loc.offset;
            } else meta.data_page_offset;
            if (first_data_off <= dict_off) return error.InvalidPageIndex;
            // Dict size is distance from dict_off to the *first* data page in the
            // chunk — not the first selected data page (we might have skipped
            // initial data pages).
            const chunk_first_data = offset_index.page_locations[0].offset;
            if (chunk_first_data <= dict_off) return error.InvalidPageIndex;
            const dict_len_i64 = chunk_first_data - dict_off;
            const dict_len = safe.castTo(usize, dict_len_i64) catch return error.IntegerOverflow;
            dict_info = .{ .offset = @intCast(dict_off), .length = dict_len };
        }
    }

    // Total output buffer size.
    var total: usize = 0;
    if (dict_info) |d| total += d.length;
    for (selected_raw.items) |*sp| total += sp.buffer_length;

    if (total == 0) {
        selected_raw.deinit(allocator);
        return .{ .data = &.{}, .has_dictionary = false, .selected = &.{} };
    }

    const buffer = try allocator.alloc(u8, total);
    errdefer allocator.free(buffer);

    // Read dict page (if any) — single readAt.
    var cursor: usize = 0;
    if (dict_info) |d| {
        const got = source.readAt(d.offset, buffer[0..d.length]) catch return error.InputOutput;
        if (got != d.length) return error.InputOutput;
        cursor = d.length;
    }

    // Coalesce contiguous runs of selected data pages into one readAt each.
    var i: usize = 0;
    while (i < selected_raw.items.len) {
        const run_start = i;
        var run_len: usize = selected_raw.items[i].buffer_length;
        var expected_next_offset: i64 = blk: {
            const loc = offset_index.page_locations[selected_raw.items[i].page_index];
            break :blk loc.offset + @as(i64, @intCast(selected_raw.items[i].buffer_length));
        };
        var j = i + 1;
        while (j < selected_raw.items.len) : (j += 1) {
            const next_loc = offset_index.page_locations[selected_raw.items[j].page_index];
            if (next_loc.offset != expected_next_offset) break;
            run_len += selected_raw.items[j].buffer_length;
            expected_next_offset = next_loc.offset + @as(i64, @intCast(selected_raw.items[j].buffer_length));
        }

        // Single readAt covering the run.
        const run_first_loc = offset_index.page_locations[selected_raw.items[run_start].page_index];
        if (run_first_loc.offset < 0) return error.InvalidPageIndex;
        const run_off: u64 = @intCast(run_first_loc.offset);
        const got = source.readAt(run_off, buffer[cursor .. cursor + run_len]) catch return error.InputOutput;
        if (got != run_len) return error.InputOutput;

        // Assign buffer offsets for each page in the run.
        var sub_cursor = cursor;
        for (selected_raw.items[run_start..j]) |*sp| {
            sp.buffer_offset = sub_cursor;
            sub_cursor += sp.buffer_length;
        }
        cursor = sub_cursor;

        i = j;
    }

    return .{
        .data = buffer,
        .has_dictionary = dict_info != null,
        .selected = try selected_raw.toOwnedSlice(allocator),
    };
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

/// Counting in-memory source — also verifies we only issue `readAt` for wanted
/// byte ranges, not the whole chunk.
const Source = struct {
    data: []const u8,
    reads: std.ArrayListUnmanaged(Record) = .empty,
    allocator: std.mem.Allocator,

    const Record = struct { offset: u64, length: usize };

    fn readAt(ctx: *anyopaque, offset: u64, buf: []u8) SeekableReader.Error!usize {
        const self: *Source = @ptrCast(@alignCast(ctx));
        self.reads.append(self.allocator, .{ .offset = offset, .length = buf.len }) catch return error.InputOutput;
        if (offset >= self.data.len) return 0;
        const end = @min(offset + buf.len, self.data.len);
        const n = end - offset;
        @memcpy(buf[0..n], self.data[offset..end]);
        return n;
    }
    fn sizeFn(ctx: *anyopaque) u64 {
        const self: *Source = @ptrCast(@alignCast(ctx));
        return self.data.len;
    }
    const vtable: SeekableReader.VTable = .{ .readAt = readAt, .size = sizeFn };
    fn reader(self: *Source) SeekableReader {
        return .{ .ptr = self, .vtable = &vtable };
    }
    fn deinit(self: *Source) void {
        self.reads.deinit(self.allocator);
    }
};

test "readSelectedPages: skips pages outside wanted range" {
    // Layout: 4 data pages at offsets 0, 100, 200, 300; each 100 bytes long.
    // first_row_index: 0, 25, 50, 75.
    const file_len: usize = 400;
    const file = try testing.allocator.alloc(u8, file_len);
    defer testing.allocator.free(file);
    for (file, 0..) |*b, idx| b.* = @intCast(idx % 251);

    var src = Source{ .data = file, .allocator = testing.allocator };
    defer src.deinit();

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

    var result = try readSelectedPages(testing.allocator, src.reader(), &chunk, &oi, wanted, 100);
    defer result.deinit(testing.allocator);

    try testing.expect(!result.has_dictionary);
    try testing.expectEqual(@as(usize, 2), result.selected.len);
    try testing.expectEqual(@as(usize, 1), result.selected[0].page_index);
    try testing.expectEqual(@as(usize, 2), result.selected[1].page_index);

    // Pages 1 and 2 are contiguous (offsets 100 and 200) so they coalesce into
    // one readAt.
    try testing.expectEqual(@as(usize, 1), src.reads.items.len);
    try testing.expectEqual(@as(u64, 100), src.reads.items[0].offset);
    try testing.expectEqual(@as(usize, 200), src.reads.items[0].length);

    // Output buffer should contain page 1 bytes then page 2 bytes.
    try testing.expectEqualSlices(u8, file[100..300], result.data);

    // Decoded-row mapping: decoded rows 0..25 → original rows 25..50,
    // decoded rows 25..50 → original rows 50..75.
    try testing.expectEqual(@as(u64, 25), result.mapDecodedRow(0).?);
    try testing.expectEqual(@as(u64, 49), result.mapDecodedRow(24).?);
    try testing.expectEqual(@as(u64, 50), result.mapDecodedRow(25).?);
    try testing.expectEqual(@as(u64, 74), result.mapDecodedRow(49).?);
    try testing.expect(result.mapDecodedRow(50) == null);
    try testing.expectEqual(@as(u64, 50), result.totalDecodedRows());
}

test "readSelectedPages: non-contiguous pages use separate reads" {
    const file_len: usize = 400;
    const file = try testing.allocator.alloc(u8, file_len);
    defer testing.allocator.free(file);
    @memset(file, 0);

    var src = Source{ .data = file, .allocator = testing.allocator };
    defer src.deinit();

    // Pages 0, 1, 2, 3 but with a gap between 1 and 2 (offsets 0, 100, 250, 350).
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

    // Want pages 0, 2, 3 → two runs (0; 2+3).
    var wanted = try RowRanges.fromUnsorted(testing.allocator, &[_]RowRange{
        .{ .first = 0, .last = 25 },
        .{ .first = 50, .last = 100 },
    });
    defer wanted.deinit(testing.allocator);

    var result = try readSelectedPages(testing.allocator, src.reader(), &chunk, &oi, wanted, 100);
    defer result.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 3), result.selected.len);
    // Expect 2 readAts: page 0 alone, then pages 2+3 coalesced.
    try testing.expectEqual(@as(usize, 2), src.reads.items.len);
    try testing.expectEqual(@as(u64, 0), src.reads.items[0].offset);
    try testing.expectEqual(@as(usize, 100), src.reads.items[0].length);
    try testing.expectEqual(@as(u64, 250), src.reads.items[1].offset);
    try testing.expectEqual(@as(usize, 150), src.reads.items[1].length);
}

test "readSelectedPages: empty wanted produces empty result" {
    var locs = [_]format.PageLocation{
        .{ .offset = 0, .compressed_page_size = 100, .first_row_index = 0 },
    };
    const oi = format.OffsetIndex{ .page_locations = &locs };
    const chunk = format.ColumnChunk{
        .meta_data = format.ColumnMetaData{ .data_page_offset = 0 },
    };
    const data = [_]u8{0} ** 100;
    var src = Source{ .data = &data, .allocator = testing.allocator };
    defer src.deinit();

    var result = try readSelectedPages(testing.allocator, src.reader(), &chunk, &oi, RowRanges.empty(), 25);
    defer result.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 0), result.selected.len);
    try testing.expectEqual(@as(usize, 0), result.data.len);
    try testing.expectEqual(@as(usize, 0), src.reads.items.len);
}

test "readSelectedPages: includes dictionary page when present" {
    // Dict at 4..54 (after PAR1 magic), data pages at 54, 154, 254.
    // dict_page_offset=0 is the "no dict" sentinel, so we use offset 4.
    const file_len: usize = 354;
    const file = try testing.allocator.alloc(u8, file_len);
    defer testing.allocator.free(file);
    for (file, 0..) |*b, idx| b.* = @intCast(idx % 251);

    var src = Source{ .data = file, .allocator = testing.allocator };
    defer src.deinit();

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

    var result = try readSelectedPages(testing.allocator, src.reader(), &chunk, &oi, wanted, 30);
    defer result.deinit(testing.allocator);

    try testing.expect(result.has_dictionary);
    try testing.expectEqual(@as(usize, 1), result.selected.len);
    try testing.expectEqual(@as(usize, 1), result.selected[0].page_index);

    // Buffer layout: 50 bytes dict + 100 bytes page 1 = 150 bytes total.
    try testing.expectEqual(@as(usize, 150), result.data.len);
    try testing.expectEqualSlices(u8, file[4..54], result.data[0..50]); // dict
    try testing.expectEqualSlices(u8, file[154..254], result.data[50..150]); // page 1

    // page 1's buffer_offset is after the dict.
    try testing.expectEqual(@as(usize, 50), result.selected[0].buffer_offset);

    // 2 readAts: dict + page 1.
    try testing.expectEqual(@as(usize, 2), src.reads.items.len);
    try testing.expectEqual(@as(u64, 4), src.reads.items[0].offset);
    try testing.expectEqual(@as(usize, 50), src.reads.items[0].length);
    try testing.expectEqual(@as(u64, 154), src.reads.items[1].offset);
    try testing.expectEqual(@as(usize, 100), src.reads.items[1].length);
}
