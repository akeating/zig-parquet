//! Decoding of leaf column chunks into Arrow-layout buffers.
//!
//! Each data page is decoded directly into the output buffers; no `Value` is built per entry.
//! The output has one entry per level entry (per value slot in the column chunk). An entry is
//! non-null when its definition level equals the column's maximum definition level. For a flat
//! column (no repetition, maximum definition level 0 or 1) entries are rows, and the buffers
//! form an Arrow array. For other leaves, `decodeLeaf` also returns the definition and
//! repetition levels, from which callers assemble lists, structs and maps.
//!
//! Output (Arrow C Data Interface layout, native byte order):
//! - `validity`: one bit per entry, LSB first, set when the entry is non-null. Bits past
//!   `length` in the last byte are set.
//! - `values`:
//!     BOOLEAN: one bit per entry, LSB first.
//!     INT32, FLOAT: 4 bytes per entry. INT64, DOUBLE: 8 bytes per entry.
//!     INT96: 8 bytes per entry, converted to nanoseconds since the Unix epoch.
//!     FIXED_LEN_BYTE_ARRAY: `type_length` bytes per entry.
//!     BYTE_ARRAY: the entries' bytes back to back (at least 1 byte is allocated).
//!   Null entries hold zero bytes (fixed width) or a zero bit (BOOLEAN), or add no bytes
//!   (BYTE_ARRAY).
//! - `offsets` (BYTE_ARRAY only): `length + 1` offsets into `values`, i32 by default or i64
//!   with `Options.large_offsets` (Arrow "large" binary layout).
//!
//! Supported: PLAIN, PLAIN_DICTIONARY / RLE_DICTIONARY, RLE (BOOLEAN), DELTA_BINARY_PACKED
//! (INT32, INT64), DELTA_LENGTH_BYTE_ARRAY (BYTE_ARRAY), DELTA_BYTE_ARRAY (BYTE_ARRAY,
//! FIXED_LEN_BYTE_ARRAY) and BYTE_STREAM_SPLIT (INT32, INT64, FLOAT, DOUBLE,
//! FIXED_LEN_BYTE_ARRAY), in V1 and V2 data pages.
//!
//! Malformed input returns an error instead of producing values: a value section shorter
//! than the number of non-null entries needs, a dictionary index outside the dictionary, a
//! dictionary-encoded page without a dictionary, and a delta-encoded page that holds fewer
//! values than non-null entries. Level and dictionary-index streams follow `rle.decodeInto`,
//! which sets entries the data does not cover to 0.

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

const format = @import("format.zig");
const safe = @import("safe.zig");
const types = @import("types.zig");
const thrift = @import("thrift/mod.zig");
const compress = @import("compress/mod.zig");
const parquet_reader = @import("parquet_reader.zig");
const SeekableReader = parquet_reader.SeekableReader;
const value_mod = @import("value.zig");
const Value = value_mod.Value;
const rle = @import("encoding/rle.zig");
const delta_binary_packed = @import("encoding/delta_binary_packed.zig");
const delta_length_byte_array = @import("encoding/delta_length_byte_array.zig");
const delta_byte_array = @import("encoding/delta_byte_array.zig");

const native_little = builtin.cpu.arch.endian() == .little;

/// Upper bound on entries reserved before decoding, taken from the column chunk's
/// `num_values`. That count comes from the file footer; past this bound, the buffers grow as
/// pages are decoded instead.
const max_reserved_entries: usize = 64 * 1024 * 1024;

/// Upper bound on the bytes of byte-array entries reserved before decoding, taken from the
/// column chunk's `total_uncompressed_size`.
const max_reserved_bytes: usize = 256 * 1024 * 1024;

/// Alignment of the output buffers. The Arrow C Data Interface requires at least 8 bytes and
/// recommends 64.
pub const buffer_alignment: std.mem.Alignment = .@"64";
pub const Buffer = []align(buffer_alignment.toByteUnits()) u8;
const BufferList = std.array_list.Aligned(u8, buffer_alignment);

/// Decoded column chunk. All slices are owned and allocated with the allocator passed to
/// `decodeChunk` or `decodeLeaf`.
pub const Buffers = struct {
    length: usize,
    null_count: usize,
    validity: Buffer,
    values: Buffer,
    offsets: ?Buffer,
    /// Offsets are i64 instead of i32.
    large_offsets: bool = false,
    /// How `values` holds entries.
    layout: Layout,

    pub fn deinit(self: *Buffers, allocator: Allocator) void {
        allocator.free(self.validity);
        allocator.free(self.values);
        if (self.offsets) |o| allocator.free(o);
    }

    pub fn isValid(self: *const Buffers, i: usize) bool {
        return (self.validity[i / 8] >> @intCast(i % 8)) & 1 == 1;
    }

    /// Bytes of BYTE_ARRAY entry `i`.
    pub fn bytesAt(self: *const Buffers, i: usize) []const u8 {
        return self.values[self.offsetAt(i)..self.offsetAt(i + 1)];
    }

    /// BYTE_ARRAY offset `i` (0 to `length`).
    pub fn offsetAt(self: *const Buffers, i: usize) usize {
        const o = self.offsets.?;
        return if (self.large_offsets)
            @intCast(std.mem.bytesToValue(i64, o[i * 8 ..][0..8]))
        else
            @intCast(std.mem.bytesToValue(i32, o[i * 4 ..][0..4]));
    }

    /// Copies the entries `i` with `levels[i] >= min_level`, in order and at most `limit` of
    /// them, into new buffers with offsets of the same width.
    pub fn select(self: *const Buffers, allocator: Allocator, levels: []const Level, min_level: u32, limit: usize) !Buffers {
        var count: usize = 0;
        var bytes: usize = 0;
        for (levels, 0..) |lv, i| {
            if (lv < min_level) continue;
            if (count == limit) break;
            count += 1;
            if (self.layout == .bytes) bytes += self.bytesAt(i).len;
        }
        var b = Builder{ .allocator = allocator, .kind = self.layout, .large_offsets = self.large_offsets };
        errdefer b.deinit();
        try b.init(count);
        try b.reserve(count);
        if (self.layout == .bytes) try b.values.ensureTotalCapacity(allocator, bytes);
        switch (self.layout) {
            .fixed => |w| switch (w) {
                inline 4, 8 => |cw| self.selectFixed(cw, &b, levels, min_level, count),
                else => self.selectFixed(0, &b, levels, min_level, count),
            },
            .bits, .bytes => {
                var taken: usize = 0;
                for (levels, 0..) |lv, i| {
                    if (lv < min_level) continue;
                    if (taken == count) break;
                    taken += 1;
                    if (self.layout == .bits) {
                        b.pushBit((self.values[i / 8] >> @intCast(i % 8)) & 1 == 1);
                    } else {
                        b.values.appendSliceAssumeCapacity(self.bytesAt(i));
                        try b.pushOffset();
                    }
                    b.pushValidity(self.isValid(i));
                }
            },
        }
        return b.finish();
    }

    /// `select` for fixed-width entries. `comptime_width` is the width when known at compile
    /// time (4 or 8), else 0.
    fn selectFixed(self: *const Buffers, comptime comptime_width: usize, b: *Builder, levels: []const Level, min_level: u32, count: usize) void {
        const w = if (comptime_width != 0) comptime_width else self.layout.fixed;
        var taken: usize = 0;
        for (levels, 0..) |lv, i| {
            if (lv < min_level) continue;
            if (taken == count) break;
            taken += 1;
            if (comptime_width != 0) {
                b.values.addManyAsArrayAssumeCapacity(comptime_width).* = self.values[i * w ..][0..comptime_width].*;
            } else {
                b.values.appendSliceAssumeCapacity(self.values[i * w ..][0..w]);
            }
            b.pushValidity(self.isValid(i));
        }
    }

    /// Like `select`, but when every entry is selected, moves the buffers out of `self`
    /// (leaving it empty) instead of copying them.
    pub fn take(self: *Buffers, allocator: Allocator, levels: []const Level, min_level: u32, limit: usize) !Buffers {
        var all = levels.len == self.length and self.length <= limit;
        if (all) {
            for (levels) |lv| {
                if (lv < min_level) {
                    all = false;
                    break;
                }
            }
        }
        if (!all) return self.select(allocator, levels, min_level, limit);
        return self.takeAll();
    }

    /// Copies the entries in `ranges` into new buffers with offsets of the same width. The
    /// ranges are in ascending order and within `0..length`.
    pub fn selectRanges(self: *const Buffers, allocator: Allocator, ranges: []const EntryRange) !Buffers {
        var count: usize = 0;
        for (ranges) |r| {
            if (r.start > r.end or r.end > self.length) return error.InvalidArgument;
            count += r.end - r.start;
        }
        var b = Builder{ .allocator = allocator, .kind = self.layout, .large_offsets = self.large_offsets };
        errdefer b.deinit();
        try b.init(count);
        for (ranges) |r| try b.appendRange(self, r.start, r.end - r.start);
        return b.finish();
    }

    /// Moves the buffers out of `self`, leaving it empty.
    pub fn takeAll(self: *Buffers) Buffers {
        const moved = self.*;
        self.validity = self.validity[0..0];
        self.values = self.values[0..0];
        if (self.offsets) |o| self.offsets = o[0..0];
        self.length = 0;
        return moved;
    }
};

/// Entries `start..end` of a `Buffers`.
pub const EntryRange = struct {
    start: usize,
    end: usize,
};

/// How a `Buffers` holds its entries.
pub const Layout = union(enum) {
    /// BOOLEAN: one bit per entry.
    bits,
    /// Fixed width in bytes.
    fixed: usize,
    /// BYTE_ARRAY: offsets and data.
    bytes,
};

/// Whether the column's entries are rows: no repetition and a maximum definition level of 0
/// or 1. `decodeChunk` handles these columns.
pub fn isFlat(info: format.ColumnInfo) bool {
    return info.max_rep_level == 0 and info.max_def_level <= 1;
}

pub const Options = struct {
    /// Return the definition and repetition levels of all pages.
    keep_levels: bool = false,
    /// Page CRC32 validation. Pages without a `crc` field pass unless `strict_checksum` is set.
    checksum: parquet_reader.ChecksumOptions = .{},
    /// i64 BYTE_ARRAY offsets. With i32 offsets, a chunk whose entries total more than
    /// 2^31 - 1 bytes returns `error.IntegerOverflow`.
    large_offsets: bool = false,
    /// Entries with a definition level below this have no slot in the buffers, which then
    /// have fewer entries than the levels. For a leaf under a list or map, the definition
    /// level of its innermost list or map: the entries below it are empty or null lists and
    /// maps, which have no element. Needs `keep_levels`. 0 gives every entry a slot.
    slot_level: u32 = 0,
};

/// A definition or repetition level, or a presence flag derived from one (0 or 1). Levels
/// are at most 255 (`format.ColumnInfo.max_def_level` and `max_rep_level` are `u8`).
pub const Level = u8;

/// A decoded leaf column chunk.
pub const Leaf = struct {
    buffers: Buffers,
    /// Definition level of each entry. Null when not requested or when the column's maximum
    /// definition level is 0.
    def_levels: ?[]Level,
    /// Repetition level of each entry. Null when not requested or when the column's maximum
    /// repetition level is 0.
    rep_levels: ?[]Level,

    pub fn deinit(self: *Leaf, allocator: Allocator) void {
        self.buffers.deinit(allocator);
        if (self.def_levels) |l| allocator.free(l);
        if (self.rep_levels) |l| allocator.free(l);
    }
};

/// Joins leaves of one column, each starting at a row (from consecutive row groups, say),
/// into one leaf. `pieces` must not be empty and have the same layout, offset width and
/// levels kept; they are left unchanged. A piece decoded with `Options.slot_level` has fewer
/// buffer entries than levels.
pub fn concatLeaves(allocator: Allocator, pieces: []const Leaf) !Leaf {
    const first = &pieces[0].buffers;
    var total: usize = 0;
    for (pieces) |*p| {
        if (!std.meta.eql(p.buffers.layout, first.layout) or p.buffers.large_offsets != first.large_offsets) return error.InvalidArgument;
        if ((p.def_levels == null) != (pieces[0].def_levels == null) or (p.rep_levels == null) != (pieces[0].rep_levels == null)) return error.InvalidArgument;
        total += p.buffers.length;
    }
    var b = Builder{ .allocator = allocator, .kind = first.layout, .large_offsets = first.large_offsets };
    errdefer b.deinit();
    try b.init(total);
    for (pieces) |*p| try b.appendBuffers(&p.buffers);
    var buffers = try b.finish();
    errdefer buffers.deinit(allocator);
    // Each piece's levels: as many as its entries, at least as many as its buffer entries.
    for (pieces) |*p| {
        const def_len = if (p.def_levels) |l| l.len else null;
        const rep_len = if (p.rep_levels) |l| l.len else null;
        if (def_len != null and rep_len != null and def_len.? != rep_len.?) return error.InvalidPageData;
        const entries = def_len orelse rep_len orelse p.buffers.length;
        if (entries < p.buffers.length) return error.InvalidPageData;
    }
    const def_levels = if (pieces[0].def_levels != null) try concatLevels(allocator, pieces, .def) else null;
    errdefer if (def_levels) |l| allocator.free(l);
    const rep_levels = if (pieces[0].rep_levels != null) try concatLevels(allocator, pieces, .rep) else null;
    return .{ .buffers = buffers, .def_levels = def_levels, .rep_levels = rep_levels };
}

fn concatLevels(allocator: Allocator, pieces: []const Leaf, comptime which: enum { def, rep }) ![]Level {
    var total: usize = 0;
    for (pieces) |*p| total += (if (which == .def) p.def_levels else p.rep_levels).?.len;
    const out = try allocator.alloc(Level, total);
    var at: usize = 0;
    for (pieces) |*p| {
        const levels = (if (which == .def) p.def_levels else p.rep_levels).?;
        @memcpy(out[at..][0..levels.len], levels);
        at += levels.len;
    }
    return out;
}

/// Decodes all data pages of a flat column chunk (see `isFlat`). `page_data` is the column
/// chunk's bytes as stored in the file (page headers and page bodies, starting with the
/// dictionary page if there is one).
pub fn decodeChunk(
    allocator: Allocator,
    page_data: []const u8,
    meta: format.ColumnMetaData,
    info: format.ColumnInfo,
) !Buffers {
    if (!isFlat(info)) return error.InvalidArgument;
    const leaf = try decodeLeaf(allocator, page_data, meta, info, .{});
    return leaf.buffers;
}

/// Decodes all data pages of any leaf column chunk.
pub fn decodeLeaf(
    allocator: Allocator,
    page_data: []const u8,
    meta: format.ColumnMetaData,
    info: format.ColumnInfo,
    options: Options,
) !Leaf {
    var cursor = try ColumnCursor.init(allocator, PageReader.fromMemory(allocator, page_data), meta, info, options, reservedEntries(meta.num_values));
    defer cursor.deinit();
    return cursor.next(std.math.maxInt(usize));
}

/// Decodes a selection of the data pages of a leaf column chunk. `pages` returns the
/// chunk's dictionary page first, if the chunk has one, then the selected data pages in
/// chunk order. Data page `k` must hold `page_rows[k]` rows, and in a column with
/// repetition it must start at a row (repetition level 0); otherwise the page does not hold
/// the rows the caller expects, and `error.PageRowCountMismatch` or
/// `error.PageNotAtRowStart` is returned. Data pages past `page_rows.len` return
/// `error.PageRowCountMismatch`. Returns the entries of all selected pages, in order.
pub fn decodeSelectedPages(
    allocator: Allocator,
    pages: PageReader,
    meta: format.ColumnMetaData,
    info: format.ColumnInfo,
    options: Options,
    page_rows: []const usize,
) !Leaf {
    var cursor = try ColumnCursor.init(allocator, pages, meta, info, options, 0);
    defer cursor.deinit();
    const repeated = info.max_rep_level > 0;
    for (page_rows) |want| {
        const rows_before = cursor.dec.row_starts;
        while (true) {
            if (!cursor.hasPages()) return error.PageRowCountMismatch;
            if (try cursor.decodeNextPage()) break;
        }
        if (cursor.dec.row_starts - rows_before != want) return error.PageRowCountMismatch;
        if (repeated and want > 0 and !cursor.dec.page_starts_row) return error.PageNotAtRowStart;
    }
    while (cursor.hasPages()) {
        if (try cursor.decodeNextPage()) return error.PageRowCountMismatch;
    }
    return cursor.take(cursor.pendingEntries(), cursor.dec.row_starts);
}

/// The pages of a column chunk, from a buffer holding the whole chunk, or read from a source
/// one page at a time: from a contiguous range of the source, or from a list of byte ranges
/// (`fromSpans`). `next` returns a page's header; its body is read by `body` or passed over by
/// `skip`.
pub const PageReader = struct {
    allocator: Allocator,
    /// Bytes at offsets `window_start..window_start + window.len`.
    window: []const u8,
    window_start: u64,
    /// Offset of the next unread byte: a page header, or a body after `next`.
    pos: u64,
    /// Offset of the end of the chunk, or of the current span.
    end: u64,
    /// Size of the body of the header last returned by `next`.
    body_size: usize = 0,
    /// When set, `window` is filled from here, into `buf`.
    source: ?SeekableReader = null,
    buf: []u8 = &.{},
    /// Spans not yet started (`fromSpans`).
    spans: []const Span = &.{},
    /// The first header read of a span reads the whole span (`fromSpans`).
    whole_spans: bool = false,

    /// Bytes `start..start + size` of a source.
    pub const Span = struct {
        start: u64,
        size: u64,
    };

    /// Bytes read for a page header, and read past a body for the next header. A header
    /// longer than this is read again with a 4 times longer prefix.
    const header_prefix: u64 = 1024;

    /// Pages of a chunk held in `data` (offset 0 is the chunk's first byte). `data` is
    /// borrowed and must outlive the reader.
    pub fn fromMemory(allocator: Allocator, data: []const u8) PageReader {
        return .{ .allocator = allocator, .window = data, .window_start = 0, .pos = 0, .end = data.len };
    }

    /// Pages of the chunk at `start..start + size` of `source`, read as they are needed.
    pub fn fromSource(allocator: Allocator, source: SeekableReader, start: u64, size: u64) PageReader {
        return .{ .allocator = allocator, .window = &.{}, .window_start = start, .pos = start, .end = start +| size, .source = source };
    }

    /// Pages held in `spans` of `source`, in order. Each span holds whole pages, usually one.
    /// A span is read with one `readAt` when its first header is needed, so the reader holds
    /// one span at a time. `spans` is borrowed and must outlive the reader.
    pub fn fromSpans(allocator: Allocator, source: SeekableReader, spans: []const Span) PageReader {
        return .{ .allocator = allocator, .window = &.{}, .window_start = 0, .pos = 0, .end = 0, .source = source, .spans = spans, .whole_spans = true };
    }

    pub fn deinit(self: *PageReader) void {
        self.allocator.free(self.buf);
    }

    /// Whether all pages have been returned.
    pub fn done(self: *const PageReader) bool {
        return self.pos >= self.end and self.spans.len == 0;
    }

    /// The next page's header. The caller frees its contents with
    /// `parquet_reader.freePageHeaderContents`, and calls `body` or `skip` before `next`.
    pub fn next(self: *PageReader) !format.PageHeader {
        while (self.pos >= self.end) {
            if (self.spans.len == 0) return error.EndOfData;
            const span = self.spans[0];
            self.spans = self.spans[1..];
            self.pos = span.start;
            self.end = span.start +| span.size;
        }
        const remaining = self.end - self.pos;
        // `@min` with a comptime bound has a narrow type; `want` grows past it.
        var want: usize = @intCast(@min(remaining, header_prefix));
        // The rest of the span is read with the header.
        const extra: u64 = if (self.whole_spans) remaining - want else 0;
        while (true) {
            const bytes = try self.read(self.pos, want, extra);
            // The header is skipped first, which does not allocate, to find out whether `bytes`
            // holds all of it: `PageHeader.parse` does not free what it allocated before
            // reaching the end of its input.
            var probe = thrift.CompactReader.init(bytes);
            probe.skip(.struct_) catch |e| {
                if (e == error.EndOfData and want < remaining) {
                    want = @intCast(@min(remaining, want *| 4));
                    continue;
                }
            };
            var r = thrift.CompactReader.init(bytes);
            const header = try format.PageHeader.parse(self.allocator, &r);
            errdefer parquet_reader.freePageHeaderContents(self.allocator, &header);
            self.body_size = safe.cast(header.compressed_page_size) catch return error.InvalidPageSize;
            self.pos += r.pos;
            return header;
        }
    }

    /// The body of the page whose header `next` returned, valid until the next call.
    pub fn body(self: *PageReader) ![]const u8 {
        if (self.body_size > self.end - self.pos) return error.EndOfData;
        const bytes = try self.read(self.pos, self.body_size, header_prefix);
        self.pos += self.body_size;
        return bytes;
    }

    /// Passes over the body of the page whose header `next` returned.
    pub fn skip(self: *PageReader) void {
        self.pos = @min(self.end, self.pos +| self.body_size);
    }

    /// Bytes `at..at + n`. When they are not in the window, reads them from the source,
    /// with up to `extra` more bytes of the chunk after them.
    fn read(self: *PageReader, at: u64, n: usize, extra: u64) ![]const u8 {
        if (at >= self.window_start and at - self.window_start <= self.window.len and n <= self.window.len - (at - self.window_start)) {
            const from: usize = @intCast(at - self.window_start);
            return self.window[from..][0..n];
        }
        const source = self.source orelse return error.EndOfData;
        const more = std.math.cast(usize, @min(extra, self.end - (at + n))) orelse return error.OutOfMemory;
        const len = std.math.add(usize, n, more) catch return error.OutOfMemory;
        if (self.buf.len < len) {
            self.allocator.free(self.buf);
            self.buf = &.{};
            self.buf = try self.allocator.alloc(u8, len);
        }
        const got = source.readAt(at, self.buf[0..len]) catch return error.InputOutput;
        if (got < n) return error.InputOutput;
        self.window = self.buf[0..got];
        self.window_start = at;
        return self.window[0..n];
    }
};

/// Decodes a leaf column chunk a number of rows at a time. Pages are decoded whole; entries
/// of a page past the rows returned are kept for the next call, along with the dictionary.
///
/// A row of a column with repetition starts at each entry with repetition level 0, so a cut
/// before row `n` needs the start of row `n` to have been decoded. Such a column needs
/// `Options.keep_levels`.
pub const ColumnCursor = struct {
    pages: PageReader,
    /// A data page header read by `init` while looking for a dictionary page; its body is
    /// next in `pages`.
    first: ?format.PageHeader = null,
    checksum: parquet_reader.ChecksumOptions,
    dec: ChunkDecoder,
    /// Entries decoded; those from `start` on are not yet returned. With
    /// `Options.slot_level`, the builder holds slots of only some entries, and those from
    /// `slot_start` on are not yet returned; otherwise `slot_start` equals `start`.
    builder: Builder,
    start: usize = 0,
    slot_start: usize = 0,

    /// Reads the dictionary page, if any. Takes ownership of `pages`, also on error.
    /// `reserve` is the number of entries to allocate room for before the first page.
    pub fn init(
        allocator: Allocator,
        pages_in: PageReader,
        meta: format.ColumnMetaData,
        info: format.ColumnInfo,
        options: Options,
        reserve: usize,
    ) !ColumnCursor {
        var pages = pages_in;
        errdefer pages.deinit();
        const physical = info.element.type_ orelse return error.InvalidSchema;
        var builder = Builder{
            .allocator = allocator,
            .large_offsets = options.large_offsets,
            .kind = switch (physical) {
                .boolean => .bits,
                .int32, .float => .{ .fixed = 4 },
                .int64, .double, .int96 => .{ .fixed = 8 },
                .fixed_len_byte_array => .{ .fixed = try fixedLength(info.element) },
                .byte_array => .bytes,
            },
        };
        errdefer builder.deinit();
        try builder.init(reserve);
        // A cursor that returns the whole chunk at once reserves the bytes of byte-array
        // entries: the chunk's uncompressed size bounds those of PLAIN pages (each value has
        // a 4-byte length before it). Growing the buffer page by page instead copies the
        // bytes decoded so far each time it grows. Dictionary pages can produce more bytes;
        // the buffer then grows.
        if (builder.kind == .bytes and reserve >= (safe.castTo(usize, meta.num_values) catch std.math.maxInt(usize))) {
            const bytes = safe.castTo(usize, meta.total_uncompressed_size) catch 0;
            try builder.values.ensureTotalCapacityPrecise(allocator, @min(bytes, max_reserved_bytes));
        }

        var dec = ChunkDecoder{
            .allocator = allocator,
            .physical = physical,
            .info = info,
            .codec = meta.codec,
            .dict_set = parquet_reader.DictionarySet.init(allocator),
            .def_bits = format.computeBitWidth(info.max_def_level),
            .rep_bits = format.computeBitWidth(info.max_rep_level),
            .keep_levels = options.keep_levels,
            .slot_level = if (info.max_def_level > 0) options.slot_level else 0,
        };
        errdefer dec.deinit();
        if (dec.slot_level > 0 and !options.keep_levels) return error.InvalidArgument;
        if (options.keep_levels) {
            if (info.max_def_level > 0) try dec.out_def.ensureTotalCapacity(allocator, reserve);
            if (info.max_rep_level > 0) try dec.out_rep.ensureTotalCapacity(allocator, reserve);
        }

        // A dictionary page can only be the first page of the chunk.
        var first: ?format.PageHeader = null;
        if (!pages.done()) {
            const header = try pages.next();
            if (header.dictionary_page_header) |dph| {
                defer parquet_reader.freePageHeaderContents(allocator, &header);
                const body = try pages.body();
                try checkCrc(options.checksum, header, body);
                const uncompressed = safe.cast(header.uncompressed_page_size) catch return error.InvalidPageSize;
                const count = safe.cast(dph.num_values) catch return error.InvalidPageSize;
                try dec.dict_set.initFromPage(body, count, physical, info.element.type_length, meta.codec, uncompressed);
                try dec.buildDictView();
            } else {
                first = header;
            }
        }
        return .{ .pages = pages, .first = first, .checksum = options.checksum, .dec = dec, .builder = builder };
    }

    pub fn deinit(self: *ColumnCursor) void {
        if (self.first) |*h| parquet_reader.freePageHeaderContents(self.dec.allocator, h);
        self.pages.deinit();
        self.builder.deinit();
        self.dec.deinit();
    }

    /// Returns the next `rows` rows, or the remaining rows when fewer are left. An empty leaf
    /// means the chunk is finished.
    pub fn next(self: *ColumnCursor, rows: usize) !Leaf {
        const repeated = self.dec.info.max_rep_level > 0;
        // A row of a repeated column is complete once the next row has started.
        const need = if (repeated) rows +| 1 else rows;
        while (self.dec.row_starts < need and self.hasPages()) _ = try self.decodeNextPage();
        if (self.dec.row_starts < need) return self.take(self.pendingEntries(), self.dec.row_starts);
        if (!repeated) return self.take(rows, rows);
        if (!self.dec.keep_levels) return error.InvalidArgument;
        // Entry index of the start of row `rows`.
        const i = nthZero(self.dec.out_rep.items[self.start..], rows) orelse
            unreachable; // row_starts counts the zeros in out_rep from `start`
        return self.take(i, rows);
    }

    /// The index of zero number `n` (from 0) in `levels`, or null when there are fewer.
    /// Counts the zeros of 32 levels at a time up to the block that holds it.
    fn nthZero(levels: []const Level, n: usize) ?usize {
        const block = 32;
        const V = @Vector(block, Level);
        var seen: usize = 0;
        var i: usize = 0;
        while (i + block <= levels.len) : (i += block) {
            const v: V = levels[i..][0..block].*;
            const zeros: usize = @popCount(@as(u32, @bitCast(v == @as(V, @splat(0)))));
            if (seen + zeros > n) break;
            seen += zeros;
        }
        for (levels[i..], i..) |lv, j| {
            if (lv != 0) continue;
            if (seen == n) return j;
            seen += 1;
        }
        return null;
    }

    /// Returns the next `n` pending entries, which hold `rows` rows.
    ///
    /// When they are at least half of the pending entries, the builder's buffers are moved
    /// out and the rest is copied into a new builder. Otherwise the entries are copied out
    /// and `start` advances; the returned entries are dropped once they are more than half of
    /// the builder. Either way an entry is copied a bounded number of times, also when `rows`
    /// is much smaller than a page.
    fn take(self: *ColumnCursor, n: usize, rows: usize) !Leaf {
        const allocator = self.dec.allocator;
        const dec = &self.dec;
        const keep_def = dec.keep_levels and dec.info.max_def_level > 0;
        const keep_rep = dec.keep_levels and dec.info.max_rep_level > 0;
        const end = self.start + n;
        // Slots of the entries, and the slots after them.
        const slots = if (dec.slot_level == 0) n else countAtLeast(dec.out_def.items[self.start..end], dec.slot_level);
        const slot_end = self.slot_start + slots;
        const rest = self.builder.length - slot_end;

        if (self.start == 0 and slots >= rest) {
            var tail = try self.builder.splitOff(slots);
            errdefer tail.deinit();
            // The next call likely returns as many entries and bytes as this one: room for
            // them now saves growing the buffers, and copying them, page by page. A chunk
            // whose pages are all decoded has no next call.
            if (self.hasPages()) {
                try tail.reserve(slots);
                if (tail.kind == .bytes) try tail.values.ensureUnusedCapacity(allocator, self.builder.values.items.len);
            }
            var tail_def = try levelsFrom(allocator, &dec.out_def, n, keep_def);
            errdefer tail_def.deinit(allocator);
            var tail_rep = try levelsFrom(allocator, &dec.out_rep, n, keep_rep);
            errdefer tail_rep.deinit(allocator);
            dec.out_def.shrinkRetainingCapacity(@min(n, dec.out_def.items.len));
            dec.out_rep.shrinkRetainingCapacity(@min(n, dec.out_rep.items.len));

            var buffers = try self.builder.finish();
            errdefer buffers.deinit(allocator);
            const def_levels = if (keep_def) try dec.out_def.toOwnedSlice(allocator) else null;
            errdefer if (def_levels) |l| allocator.free(l);
            const rep_levels = if (keep_rep) try dec.out_rep.toOwnedSlice(allocator) else null;

            self.builder.deinit();
            self.builder = tail;
            dec.out_def.deinit(allocator);
            dec.out_def = tail_def;
            dec.out_rep.deinit(allocator);
            dec.out_rep = tail_rep;
            dec.row_starts -= rows;
            return .{ .buffers = buffers, .def_levels = def_levels, .rep_levels = rep_levels };
        }

        var head = try self.builder.copyRange(self.slot_start, slots);
        var buffers = head.finish() catch |e| {
            head.deinit();
            return e;
        };
        errdefer buffers.deinit(allocator);
        const def_levels = if (keep_def) try allocator.dupe(Level, dec.out_def.items[self.start..end]) else null;
        errdefer if (def_levels) |l| allocator.free(l);
        const rep_levels = if (keep_rep) try allocator.dupe(Level, dec.out_rep.items[self.start..end]) else null;
        errdefer if (rep_levels) |l| allocator.free(l);

        if (slot_end * 2 > self.builder.length or (keep_def and end * 2 > dec.out_def.items.len)) {
            const tail = try self.builder.copyRange(slot_end, rest);
            self.builder.deinit();
            self.builder = tail;
            if (keep_def) dropFront(&dec.out_def, end);
            if (keep_rep) dropFront(&dec.out_rep, end);
            self.start = 0;
            self.slot_start = 0;
        } else {
            self.start = end;
            self.slot_start = slot_end;
        }
        dec.row_starts -= rows;
        return .{ .buffers = buffers, .def_levels = def_levels, .rep_levels = rep_levels };
    }

    /// Entries decoded and not yet returned.
    fn pendingEntries(self: *const ColumnCursor) usize {
        if (self.dec.slot_level > 0) return self.dec.out_def.items.len - self.start;
        return self.builder.length - self.start;
    }

    fn countAtLeast(levels: []const Level, min: u32) usize {
        var count: usize = 0;
        for (levels) |lv| count += @intFromBool(lv >= min);
        return count;
    }

    /// A list with `list.items[from..]`, or an empty list when `keep` is false.
    fn levelsFrom(allocator: Allocator, list: *const std.ArrayList(Level), from: usize, keep: bool) !std.ArrayList(Level) {
        var out: std.ArrayList(Level) = .empty;
        if (keep) try out.appendSlice(allocator, list.items[from..]);
        return out;
    }

    fn dropFront(list: *std.ArrayList(Level), n: usize) void {
        const rest = list.items.len - n;
        std.mem.copyForwards(Level, list.items[0..rest], list.items[n..]);
        list.shrinkRetainingCapacity(rest);
    }

    /// Decodes the next page into `builder` when it is a data page. Other page types are
    /// skipped. Returns whether the page was a data page.
    fn decodeNextPage(self: *ColumnCursor) !bool {
        const allocator = self.dec.allocator;
        const header = if (self.first) |h| h else try self.pages.next();
        self.first = null;
        defer parquet_reader.freePageHeaderContents(allocator, &header);

        if (header.data_page_header == null and header.data_page_header_v2 == null) {
            // Index pages and a second dictionary page are skipped.
            self.pages.skip();
            return false;
        }
        const body = try self.pages.body();
        try checkCrc(self.checksum, header, body);

        if (header.data_page_header) |dph| {
            try self.dec.decodeV1Page(&self.builder, body, dph, header);
        } else if (header.data_page_header_v2) |v2| {
            try self.dec.decodeV2Page(&self.builder, body, v2, header);
        }
        return true;
    }

    /// Whether pages remain to be decoded.
    fn hasPages(self: *const ColumnCursor) bool {
        return self.first != null or !self.pages.done();
    }
};

fn checkCrc(options: parquet_reader.ChecksumOptions, header: format.PageHeader, body: []const u8) !void {
    if (!options.validate_page_checksum) return;
    if (header.crc) |expected| {
        const actual: i32 = @bitCast(std.hash.Crc32.hash(body));
        if (actual != expected) return error.PageChecksumMismatch;
    } else if (options.strict_checksum) {
        return error.MissingPageChecksum;
    }
}

fn fixedLength(element: format.SchemaElement) !usize {
    const tl = element.type_length orelse return error.InvalidSchema;
    return safe.cast(tl) catch return error.InvalidTypeLength;
}

fn reservedEntries(num_values: i64) usize {
    const n = safe.cast(num_values) catch return 0;
    return @min(n, max_reserved_entries);
}

fn bitBytes(n: usize) usize {
    return n / 8 + @intFromBool(n % 8 != 0);
}

// =============================================================================
// Output buffers
// =============================================================================

const Kind = Layout;

const Builder = struct {
    allocator: Allocator,
    kind: Kind,
    large_offsets: bool = false,
    length: usize = 0,
    null_count: usize = 0,
    validity: BufferList = .empty,
    values: BufferList = .empty,
    offsets: BufferList = .empty,

    fn init(self: *Builder, entries: usize) !void {
        try self.validity.ensureTotalCapacityPrecise(self.allocator, bitBytes(entries));
        switch (self.kind) {
            .bits => try self.values.ensureTotalCapacityPrecise(self.allocator, bitBytes(entries)),
            .fixed => |w| try self.values.ensureTotalCapacityPrecise(self.allocator, try mulSize(entries, w)),
            .bytes => {
                try self.offsets.ensureTotalCapacityPrecise(self.allocator, try mulSize(entries + 1, self.offsetWidth()));
                self.offsets.appendNTimesAssumeCapacity(0, self.offsetWidth());
            },
        }
    }

    fn deinit(self: *Builder) void {
        self.validity.deinit(self.allocator);
        self.values.deinit(self.allocator);
        self.offsets.deinit(self.allocator);
    }

    /// Reserves room for `n` more entries in the validity bitmap, and in the values (fixed
    /// width, bits) or offsets (byte arrays).
    fn reserve(self: *Builder, n: usize) !void {
        const total = std.math.add(usize, self.length, n) catch return error.OutOfMemory;
        try self.validity.ensureTotalCapacity(self.allocator, bitBytes(total));
        switch (self.kind) {
            .bits => try self.values.ensureTotalCapacity(self.allocator, bitBytes(total)),
            .fixed => |w| try self.values.ensureTotalCapacity(self.allocator, try mulSize(total, w)),
            .bytes => try self.offsets.ensureTotalCapacity(self.allocator, try mulSize(total + 1, self.offsetWidth())),
        }
    }

    /// Marks `n` more entries valid. Requires `reserve(n)`.
    fn validRun(self: *Builder, n: usize) void {
        const need = bitBytes(self.length + n) - self.validity.items.len;
        self.validity.appendNTimesAssumeCapacity(0xFF, need);
        self.length += n;
    }

    /// Marks one more entry valid or null. Requires `reserve`.
    fn pushValidity(self: *Builder, valid: bool) void {
        const i = self.length;
        if (i % 8 == 0) self.validity.appendAssumeCapacity(0xFF);
        if (!valid) {
            self.validity.items[i / 8] &= ~(@as(u8, 1) << @intCast(i % 8));
            self.null_count += 1;
        }
        self.length += 1;
    }

    /// Appends one bit to the BOOLEAN values. Call before `pushValidity` for the same entry.
    /// Requires `reserve`.
    fn pushBit(self: *Builder, bit: bool) void {
        const i = self.length;
        if (i % 8 == 0) self.values.appendAssumeCapacity(0);
        if (bit) self.values.items[i / 8] |= @as(u8, 1) << @intCast(i % 8);
    }

    /// Appends the current data length as the next offset. Requires `reserve`.
    fn pushOffset(self: *Builder) !void {
        try pushOffsetAt(self, self.values.items.len);
    }

    fn offsetWidth(self: *const Builder) usize {
        return if (self.large_offsets) 8 else 4;
    }

    /// A new builder with copies of entries `start..start + n`.
    fn copyRange(self: *const Builder, start: usize, n: usize) !Builder {
        var out = Builder{ .allocator = self.allocator, .kind = self.kind, .large_offsets = self.large_offsets };
        errdefer out.deinit();
        try out.init(n);
        if (n == 0) return out;
        appendBitRange(&out.validity, self.validity.items, start, n, true);
        var ones: usize = 0;
        for (out.validity.items) |byte| ones += @popCount(byte);
        // Padding bits past `n` are set, so they count as valid.
        out.null_count = out.validity.items.len * 8 - ones;
        out.length = n;
        switch (self.kind) {
            .bits => appendBitRange(&out.values, self.values.items, start, n, false),
            .fixed => |w| out.values.appendSliceAssumeCapacity(self.values.items[start * w .. (start + n) * w]),
            .bytes => {
                const first = self.offsetAt(start);
                try out.values.appendSlice(self.allocator, self.values.items[first..self.offsetAt(start + n)]);
                for (start + 1..start + n + 1) |i| try pushOffsetAt(&out, self.offsetAt(i) - first);
            },
        }
        return out;
    }

    /// Moves the entries from `cut` on into a new builder and truncates this one to `cut`
    /// entries.
    fn splitOff(self: *Builder, cut: usize) !Builder {
        const tail = try self.copyRange(cut, self.length - cut);
        self.validity.shrinkRetainingCapacity(bitBytes(cut));
        if (cut % 8 != 0) self.validity.items[cut / 8] |= ~((@as(u8, 1) << @intCast(cut % 8)) - 1);
        switch (self.kind) {
            .bits => {
                self.values.shrinkRetainingCapacity(bitBytes(cut));
                if (cut % 8 != 0) self.values.items[cut / 8] &= (@as(u8, 1) << @intCast(cut % 8)) - 1;
            },
            .fixed => |w| self.values.shrinkRetainingCapacity(cut * w),
            .bytes => {
                self.values.shrinkRetainingCapacity(self.offsetAt(cut));
                self.offsets.shrinkRetainingCapacity((cut + 1) * self.offsetWidth());
            },
        }
        self.null_count -= tail.null_count;
        self.length = cut;
        return tail;
    }

    /// Appends the entries of `src`, which has this builder's layout and offset width.
    fn appendBuffers(self: *Builder, src: *const Buffers) !void {
        try self.appendRange(src, 0, src.length);
    }

    /// Appends entries `start..start + n` of `src`, which has this builder's layout and
    /// offset width.
    fn appendRange(self: *Builder, src: *const Buffers, start: usize, n: usize) !void {
        const end = start + n;
        try self.reserve(n);
        switch (self.kind) {
            .bits => {
                // `pushBit` and `pushValidity` of an entry go together.
                for (start..end) |i| {
                    self.pushBit((src.values[i / 8] >> @intCast(i % 8)) & 1 == 1);
                    self.pushValidity(src.isValid(i));
                }
                return;
            },
            .fixed => |w| self.values.appendSliceAssumeCapacity(src.values[start * w .. end * w]),
            .bytes => {
                const base = self.values.items.len;
                const first = src.offsetAt(start);
                try self.values.appendSlice(self.allocator, src.values[first..src.offsetAt(end)]);
                for (start + 1..end + 1) |i| try pushOffsetAt(self, base + (src.offsetAt(i) - first));
            },
        }
        if (src.null_count == 0) {
            self.validRun(n);
        } else {
            for (start..end) |i| self.pushValidity(src.isValid(i));
        }
    }

    /// Byte-array offset `i`.
    fn offsetAt(self: *const Builder, i: usize) usize {
        const o = self.offsets.items;
        return if (self.large_offsets)
            @intCast(std.mem.bytesToValue(i64, o[i * 8 ..][0..8]))
        else
            @intCast(std.mem.bytesToValue(i32, o[i * 4 ..][0..4]));
    }

    fn finish(self: *Builder) !Buffers {
        if (self.kind == .bytes and self.values.items.len == 0) {
            // Arrow consumers expect a non-null data buffer.
            try self.values.append(self.allocator, 0);
        }
        const validity = try self.validity.toOwnedSlice(self.allocator);
        errdefer self.allocator.free(validity);
        const values = try self.values.toOwnedSlice(self.allocator);
        errdefer self.allocator.free(values);
        const offsets: ?Buffer = if (self.kind == .bytes) try self.offsets.toOwnedSlice(self.allocator) else null;
        // The lists are left empty, so `deinit` can still be called.
        return .{
            .length = self.length,
            .null_count = self.null_count,
            .validity = validity,
            .values = values,
            .offsets = offsets,
            .large_offsets = self.large_offsets,
            .layout = self.kind,
        };
    }
};

/// Appends bits `start..start + n` of `src` to an empty bit list, starting at bit 0. Bits
/// past `n` in the last byte are set to `pad`. Requires capacity for `bitBytes(n)` bytes.
fn appendBitRange(list: *BufferList, src: []const u8, start: usize, n: usize, pad: bool) void {
    const shift: u3 = @intCast(start % 8);
    const first = start / 8;
    for (0..bitBytes(n)) |j| {
        const k = first + j;
        var byte = src[k] >> shift;
        if (shift != 0 and k + 1 < src.len) byte |= src[k + 1] << @intCast(@as(u4, 8) - shift);
        list.appendAssumeCapacity(byte);
    }
    if (n % 8 != 0) {
        const mask = (@as(u8, 1) << @intCast(n % 8)) - 1;
        const last = &list.items[list.items.len - 1];
        last.* = if (pad) last.* | ~mask else last.* & mask;
    }
}

fn mulSize(a: usize, b: usize) error{OutOfMemory}!usize {
    return std.math.mul(usize, a, b) catch error.OutOfMemory;
}

// =============================================================================
// Page decoding
// =============================================================================

/// Dictionary entries in output form.
const DictView = union(enum) {
    none,
    /// `count` entries of the column's fixed width, back to back, native byte order.
    fixed: struct { bytes: []const u8, count: usize },
    /// BYTE_ARRAY entries.
    bytes: BytesDict,
};

/// BYTE_ARRAY dictionary: entry i is `data[offsets[i]..offsets[i + 1]]`. `data` has
/// `copy_block` bytes of padding after the last entry, so `copyBlocks` can read whole blocks.
const BytesDict = struct {
    data: []const u8,
    offsets: []const usize,

    fn count(self: BytesDict) usize {
        return self.offsets.len - 1;
    }
};

/// Byte arrays are copied in blocks of this many bytes; see `copyBlocks`.
const copy_block = 16;

/// Copies `len` bytes in `copy_block`-byte blocks, so up to `copy_block - 1` bytes past `len`
/// are read from `src` and written to `dst`. Both must have that much room.
inline fn copyBlocks(dst: [*]u8, src: [*]const u8, len: usize) void {
    var i: usize = 0;
    while (i < len) : (i += copy_block) {
        dst[i..][0..copy_block].* = src[i..][0..copy_block].*;
    }
}

const ChunkDecoder = struct {
    allocator: Allocator,
    physical: format.PhysicalType,
    info: format.ColumnInfo,
    codec: format.CompressionCodec,
    dict_set: parquet_reader.DictionarySet,
    dict: DictView = .none,
    /// Dictionary bytes built by `buildDictView` (INT96, FIXED_LEN_BYTE_ARRAY, BYTE_ARRAY).
    dict_owned: ?[]u8 = null,
    /// BYTE_ARRAY dictionary offsets built by `buildDictView`.
    dict_offsets: ?[]usize = null,
    /// Bit widths of the definition and repetition levels.
    def_bits: u5,
    rep_bits: u5,
    /// Whether to collect all pages' levels in `out_def` and `out_rep`.
    keep_levels: bool,
    /// `Options.slot_level`; 0 when the column has no definition levels.
    slot_level: u32 = 0,
    /// Definition levels of the current page.
    levels: std.ArrayList(Level) = .empty,
    /// Repetition levels of the current page.
    rep: std.ArrayList(Level) = .empty,
    /// Presence of each entry of the current page (1 when the definition level is the
    /// maximum), when the maximum definition level is above 1.
    mask: std.ArrayList(Level) = .empty,
    out_def: std.ArrayList(Level) = .empty,
    out_rep: std.ArrayList(Level) = .empty,
    /// Whether the page decoded last starts at a row: its first repetition level is 0, or
    /// the column has no repetition.
    page_starts_row: bool = true,
    /// Rows started in the entries decoded and not yet returned by `ColumnCursor`: entries
    /// with repetition level 0, or all entries when the column has no repetition.
    row_starts: usize = 0,
    /// Dictionary indices or RLE booleans of the current page.
    indices: std.ArrayList(u32) = .empty,
    /// Dense values of the current page when they must be converted before copying.
    scratch: std.ArrayList(u8) = .empty,

    fn deinit(self: *ChunkDecoder) void {
        self.dict_set.deinit();
        if (self.dict_owned) |d| self.allocator.free(d);
        if (self.dict_offsets) |o| self.allocator.free(o);
        self.levels.deinit(self.allocator);
        self.rep.deinit(self.allocator);
        self.mask.deinit(self.allocator);
        self.out_def.deinit(self.allocator);
        self.out_rep.deinit(self.allocator);
        self.indices.deinit(self.allocator);
        self.scratch.deinit(self.allocator);
    }

    fn buildDictView(self: *ChunkDecoder) !void {
        const ds = &self.dict_set;
        self.dict = switch (self.physical) {
            .int32 => if (ds.int32_dict) |d| fixedView(std.mem.sliceAsBytes(d.values), d.values.len) else .none,
            .int64 => if (ds.int64_dict) |d| fixedView(std.mem.sliceAsBytes(d.values), d.values.len) else .none,
            .float => if (ds.float32_dict) |d| fixedView(std.mem.sliceAsBytes(d.values), d.values.len) else .none,
            .double => if (ds.float64_dict) |d| fixedView(std.mem.sliceAsBytes(d.values), d.values.len) else .none,
            .byte_array => blk: {
                const d = ds.string_dict orelse break :blk .none;
                const offsets = try self.allocator.alloc(usize, d.values.len + 1);
                self.dict_offsets = offsets;
                var total: usize = 0;
                for (d.values, 0..) |entry, i| {
                    offsets[i] = total;
                    total = std.math.add(usize, total, entry.len) catch return error.OutOfMemory;
                }
                offsets[d.values.len] = total;
                const data = try self.allocator.alloc(u8, std.math.add(usize, total, copy_block) catch return error.OutOfMemory);
                self.dict_owned = data;
                for (d.values, 0..) |entry, i| @memcpy(data[offsets[i]..][0..entry.len], entry);
                @memset(data[total..], 0);
                break :blk .{ .bytes = .{ .data = data, .offsets = offsets } };
            },
            .int96 => blk: {
                const d = ds.int96_dict orelse break :blk .none;
                const nanos = try self.allocator.alloc(u8, try mulSize(d.values.len, 8));
                self.dict_owned = nanos;
                for (d.values, 0..) |raw, i| {
                    const v = types.Int96.fromBytes(raw).toNanos();
                    @memcpy(nanos[i * 8 ..][0..8], std.mem.asBytes(&v));
                }
                break :blk fixedView(nanos, d.values.len);
            },
            .fixed_len_byte_array => blk: {
                const d = ds.fixed_byte_array_dict orelse break :blk .none;
                const w = d.fixed_len;
                const bytes = try self.allocator.alloc(u8, try mulSize(d.values.len, w));
                self.dict_owned = bytes;
                for (d.values, 0..) |entry, i| @memcpy(bytes[i * w ..][0..w], entry[0..w]);
                break :blk fixedView(bytes, d.values.len);
            },
            .boolean => .none,
        };
    }

    fn fixedView(bytes: []const u8, count: usize) DictView {
        return .{ .fixed = .{ .bytes = bytes, .count = count } };
    }

    fn decodeV1Page(self: *ChunkDecoder, b: *Builder, body: []const u8, dph: format.DataPageHeader, header: format.PageHeader) !void {
        const n = safe.cast(dph.num_values) catch return error.InvalidPageSize;
        if (n == 0) return;

        const decompressed = if (self.codec != .uncompressed) blk: {
            const size = safe.cast(header.uncompressed_page_size) catch return error.InvalidPageSize;
            break :blk compress.decompress(self.allocator, body, self.codec, size) catch return error.DecompressionError;
        } else body;
        defer if (self.codec != .uncompressed) self.allocator.free(decompressed);

        var data: []const u8 = decompressed;
        // Repetition levels come before definition levels.
        var rep: ?[]const Level = null;
        if (self.info.max_rep_level > 0) {
            const out = try self.levelsOut(&self.out_rep, &self.rep, n);
            data = try decodeV1Levels(data, dph.repetition_level_encoding, self.rep_bits, out);
            rep = out;
        }
        var def: ?[]const Level = null;
        if (self.info.max_def_level > 0) {
            const out = try self.levelsOut(&self.out_def, &self.levels, n);
            data = try decodeV1Levels(data, dph.definition_level_encoding, self.def_bits, out);
            def = out;
        }
        try self.decodePage(b, data, n, def, rep, dph.encoding);
    }

    /// Decodes V1 levels at the start of `data` into `out` and returns the data after them.
    /// BIT_PACKED levels have no length prefix; RLE levels have a 4-byte little-endian length.
    fn decodeV1Levels(data: []const u8, encoding: format.Encoding, bit_width: u5, out: []Level) ![]const u8 {
        if (encoding == .bit_packed) {
            const len = rle.bitPackedSize(out.len, bit_width);
            try rle.decodeBitPackedLevelsInto(try safe.slice(data, 0, len), bit_width, out);
            return data[len..];
        }
        if (data.len < 4) return error.EndOfData;
        const len: usize = std.mem.readInt(u32, data[0..4], .little);
        try rle.decodeInto(try safe.slice(data, 4, len), bit_width, out);
        return data[4 + len ..];
    }

    fn decodeV2Page(self: *ChunkDecoder, b: *Builder, body: []const u8, v2: format.DataPageHeaderV2, header: format.PageHeader) !void {
        const n = safe.cast(v2.num_values) catch return error.InvalidPageSize;
        if (n == 0) return;
        const rep_len = safe.cast(v2.repetition_levels_byte_length) catch return error.InvalidPageSize;
        const def_len = safe.cast(v2.definition_levels_byte_length) catch return error.InvalidPageSize;
        const levels_len = std.math.add(usize, rep_len, def_len) catch return error.EndOfData;
        if (levels_len > body.len) return error.EndOfData;
        const rep_data = body[0..rep_len];
        const def_data = body[rep_len..levels_len];
        const values_raw = body[levels_len..];

        var allocated = false;
        const data = if (v2.is_compressed and self.codec != .uncompressed) blk: {
            const total = safe.cast(header.uncompressed_page_size) catch return error.InvalidPageSize;
            if (total < levels_len) return error.EndOfData;
            const size = total - levels_len;
            if (size == 0 or values_raw.len == 0) break :blk values_raw;
            allocated = true;
            break :blk compress.decompress(self.allocator, values_raw, self.codec, size) catch return error.DecompressionError;
        } else values_raw;
        defer if (allocated) self.allocator.free(data);

        // V2 levels are RLE without a length prefix. An empty level section means all levels
        // are 0 (repetition) or the maximum (definition).
        var rep: ?[]const Level = null;
        if (self.info.max_rep_level > 0) {
            const out = try self.levelsOut(&self.out_rep, &self.rep, n);
            if (rep_data.len > 0) try rle.decodeInto(rep_data, self.rep_bits, out) else @memset(out, 0);
            rep = out;
        }
        var def: ?[]const Level = null;
        if (self.info.max_def_level > 0) {
            const out = try self.levelsOut(&self.out_def, &self.levels, n);
            if (def_data.len > 0) try rle.decodeInto(def_data, self.def_bits, out) else @memset(out, self.info.max_def_level);
            def = out;
        }
        try self.decodePage(b, data, n, def, rep, v2.encoding);
    }

    /// Records the page's levels (when kept), derives which entries are present, and decodes
    /// the value section.
    fn decodePage(
        self: *ChunkDecoder,
        b: *Builder,
        data: []const u8,
        n: usize,
        def: ?[]const Level,
        rep: ?[]const Level,
        encoding: format.Encoding,
    ) !void {
        if (self.keep_levels) {
            if (def) |d| try self.keepLevels(&self.out_def, d);
            if (rep) |r| try self.keepLevels(&self.out_rep, r);
        }
        self.page_starts_row = if (rep) |r| r.len == 0 or r[0] == 0 else true;
        if (rep) |r| {
            for (r) |lv| self.row_starts += @intFromBool(lv == 0);
        } else self.row_starts += n;
        var present: ?[]const Level = null;
        var non_null = n;
        var slots = n;
        if (def) |d| {
            const max_def = self.info.max_def_level;
            if (self.slot_level > 0) {
                // The presence of the entries that have a slot.
                const m = try resizeList(self.allocator, &self.mask, n);
                const slot_level = self.slot_level;
                var k: usize = 0;
                for (d) |lv| {
                    m[k] = @intFromBool(lv == max_def);
                    k += @intFromBool(lv >= slot_level);
                }
                slots = k;
                present = m[0..k];
            } else if (max_def == 1) {
                // Levels are 0 or 1 (bit width 1), so they serve as the presence mask.
                present = d;
            } else {
                const m = try resizeList(self.allocator, &self.mask, n);
                for (d, m) |lv, *p| p.* = @intFromBool(lv == max_def);
                present = m;
            }
            non_null = countPresent(present.?);
        }
        try self.decodeValues(b, data, slots, present, non_null, encoding);
    }

    /// Where a page's `n` levels are decoded. When levels are kept, that is the unused
    /// capacity of `kept`, so `keepLevels` adds them without copying; otherwise `scratch`.
    fn levelsOut(self: *ChunkDecoder, kept: *std.ArrayList(Level), scratch: *std.ArrayList(Level), n: usize) ![]Level {
        if (!self.keep_levels) return resizeList(self.allocator, scratch, n);
        try kept.ensureUnusedCapacity(self.allocator, n);
        return kept.unusedCapacitySlice()[0..n];
    }

    /// Adds a page's levels to `list`; levels decoded into its unused capacity by
    /// `levelsOut` are not copied.
    fn keepLevels(self: *ChunkDecoder, list: *std.ArrayList(Level), levels: []const Level) !void {
        if (list.capacity > list.items.len and levels.ptr == list.items.ptr + list.items.len) {
            list.items.len += levels.len;
        } else {
            try list.appendSlice(self.allocator, levels);
        }
    }

    fn resizeList(allocator: Allocator, list: *std.ArrayList(Level), n: usize) ![]Level {
        try list.resize(allocator, n);
        return list.items;
    }

    fn countPresent(levels: []const Level) usize {
        var count: usize = 0;
        for (levels) |lv| count += @intFromBool(lv != 0);
        return count;
    }

    /// Decodes the value section of a page with `n` entries, `non_null` of them present
    /// (`levels[i] != 0`; all present when `levels` is null).
    fn decodeValues(
        self: *ChunkDecoder,
        b: *Builder,
        data: []const u8,
        n: usize,
        levels: ?[]const Level,
        non_null: usize,
        encoding: format.Encoding,
    ) !void {
        try b.reserve(n);
        // A page without nulls takes the same path as a required column.
        const lv = if (non_null == n) null else levels;

        switch (encoding) {
            .plain_dictionary, .rle_dictionary => {
                const indices = try self.decodeIndices(data, non_null);
                switch (self.dict) {
                    .none => return error.InvalidPageData,
                    .fixed => |d| try appendFixedDict(b, d.bytes, d.count, lv, n, indices),
                    .bytes => |dict| try appendBytesDict(b, dict, lv, n, indices),
                }
            },
            .plain => switch (b.kind) {
                .bits => {
                    if (data.len < bitBytes(non_null)) return error.EndOfData;
                    appendBits(b, lv, n, PlainBits{ .data = data });
                },
                .bytes => try appendBytesPlain(b, lv, n, data),
                .fixed => |w| {
                    if (self.physical == .int96) {
                        const dense = try self.int96ToNanos(data, non_null);
                        try appendFixedDense(b, 8, lv, n, dense);
                    } else {
                        const len = try mulSize(non_null, w);
                        if (data.len < len) return error.EndOfData;
                        const dense = if (native_little or self.physical == .fixed_len_byte_array)
                            data[0..len]
                        else
                            try self.swapToNative(data[0..len], w);
                        try appendFixedDense(b, w, lv, n, dense);
                    }
                },
            },
            .rle => {
                if (b.kind != .bits) return error.UnsupportedEncoding;
                // RLE-encoded booleans are prefixed with their length (4 bytes, little-endian).
                if (non_null > 0) {
                    if (data.len < 4) return error.EndOfData;
                    const len: usize = std.mem.readInt(u32, data[0..4], .little);
                    const encoded = try safe.slice(data, 4, len);
                    const bits = try self.indexBuffer(non_null);
                    try rle.decodeInto(encoded, 1, bits);
                    appendBits(b, lv, n, ListBits{ .values = bits });
                } else {
                    appendBits(b, lv, n, ListBits{ .values = &.{} });
                }
            },
            .delta_binary_packed => switch (self.physical) {
                .int32 => try self.appendDeltaInts(i32, b, data, lv, n, non_null),
                .int64 => try self.appendDeltaInts(i64, b, data, lv, n, non_null),
                else => return error.UnsupportedEncoding,
            },
            .delta_length_byte_array => {
                if (self.physical != .byte_array) return error.UnsupportedEncoding;
                var result = delta_length_byte_array.decode(self.allocator, data) catch |e| return mapEncodingError(e);
                defer result.deinit();
                try self.appendByteValues(b, lv, n, non_null, result.values);
            },
            .delta_byte_array => {
                if (self.physical != .byte_array and self.physical != .fixed_len_byte_array) return error.UnsupportedEncoding;
                var result = delta_byte_array.decode(self.allocator, data) catch |e| return mapEncodingError(e);
                defer result.deinit();
                try self.appendByteValues(b, lv, n, non_null, result.values);
            },
            .byte_stream_split => switch (b.kind) {
                .fixed => |w| {
                    if (self.physical == .int96) return error.UnsupportedEncoding;
                    const dense = try self.byteStreamSplit(data, non_null, w);
                    try appendFixedDense(b, w, lv, n, dense);
                },
                else => return error.UnsupportedEncoding,
            },
            else => return error.UnsupportedEncoding,
        }
    }

    /// Appends entries decoded by a DELTA_LENGTH_BYTE_ARRAY or DELTA_BYTE_ARRAY decoder.
    fn appendByteValues(self: *ChunkDecoder, b: *Builder, levels: ?[]const Level, n: usize, non_null: usize, values: []const []const u8) !void {
        if (values.len < non_null) return error.EndOfData;
        switch (b.kind) {
            .bytes => try appendBytesList(b, levels, n, values),
            .fixed => |w| {
                if (self.physical != .fixed_len_byte_array) return error.UnsupportedEncoding;
                const dense = try self.scratchBuffer(try mulSize(non_null, w));
                for (values[0..non_null], 0..) |v, i| {
                    if (v.len != w) return error.InvalidPageData;
                    @memcpy(dense[i * w ..][0..w], v);
                }
                try appendFixedDense(b, w, levels, n, dense);
            },
            .bits => return error.UnsupportedEncoding,
        }
    }

    fn indexBuffer(self: *ChunkDecoder, n: usize) ![]u32 {
        try self.indices.resize(self.allocator, n);
        return self.indices.items;
    }

    fn scratchBuffer(self: *ChunkDecoder, n: usize) ![]u8 {
        try self.scratch.resize(self.allocator, n);
        return self.scratch.items;
    }

    /// Decodes `count` dictionary indices: one bit-width byte followed by RLE/bit-packed data.
    /// The bit width is validated whenever it is present, also on pages without values.
    fn decodeIndices(self: *ChunkDecoder, data: []const u8, count: usize) ![]const u32 {
        if (data.len > 0 and data[0] > 31) return error.InvalidBitWidth;
        if (count == 0) return &.{};
        if (data.len == 0) return error.EndOfData;
        const bit_width: u5 = @intCast(data[0]);
        const out = try self.indexBuffer(count);
        try rle.decodeInto(data[1..], bit_width, out);
        return out;
    }

    fn int96ToNanos(self: *ChunkDecoder, data: []const u8, count: usize) ![]const u8 {
        if (data.len < try mulSize(count, 12)) return error.EndOfData;
        const out = try self.scratchBuffer(try mulSize(count, 8));
        for (0..count) |i| {
            const v = types.Int96.fromBytes(data[i * 12 ..][0..12].*).toNanos();
            @memcpy(out[i * 8 ..][0..8], std.mem.asBytes(&v));
        }
        return out;
    }

    fn swapToNative(self: *ChunkDecoder, data: []const u8, w: usize) ![]const u8 {
        const out = try self.scratchBuffer(data.len);
        var i: usize = 0;
        while (i < data.len) : (i += w) {
            for (0..w) |j| out[i + j] = data[i + w - 1 - j];
        }
        return out;
    }

    /// BYTE_STREAM_SPLIT: byte j of entry i is at `data[j * count + i]`.
    fn byteStreamSplit(self: *ChunkDecoder, data: []const u8, count: usize, w: usize) ![]const u8 {
        if (data.len < try mulSize(count, w)) return error.EndOfData;
        const out = try self.scratchBuffer(count * w);
        for (0..w) |j| {
            const stream = data[j * count ..][0..count];
            for (stream, 0..) |byte, i| out[i * w + j] = byte;
        }
        return out;
    }

    fn appendDeltaInts(self: *ChunkDecoder, comptime T: type, b: *Builder, data: []const u8, levels: ?[]const Level, n: usize, non_null: usize) !void {
        const decoded = (if (T == i32)
            delta_binary_packed.decodeInt32(self.allocator, data)
        else
            delta_binary_packed.decodeInt64(self.allocator, data)) catch |e| return mapEncodingError(e);
        defer self.allocator.free(decoded);
        if (decoded.len < non_null) return error.EndOfData;
        try appendFixedDense(b, @sizeOf(T), levels, n, std.mem.sliceAsBytes(decoded[0..non_null]));
    }
};

fn mapEncodingError(e: anyerror) error{ OutOfMemory, InvalidPageData } {
    return if (e == error.OutOfMemory) error.OutOfMemory else error.InvalidPageData;
}

// =============================================================================
// Appending a page's entries
// =============================================================================

/// Appends `n` fixed-width entries. `dense` holds the non-null entries back to back.
/// `levels` is null when all entries are present.
fn appendFixedDense(b: *Builder, w: usize, levels: ?[]const Level, n: usize, dense: []const u8) !void {
    const lv = levels orelse {
        b.values.appendSliceAssumeCapacity(dense[0 .. n * w]);
        b.validRun(n);
        return;
    };
    switch (w) {
        inline 4, 8 => |cw| scatterFixed(cw, b, lv, dense),
        else => scatterFixedRuntime(w, b, lv, dense),
    }
}

fn scatterFixed(comptime w: usize, b: *Builder, levels: []const Level, dense: []const u8) void {
    var k: usize = 0;
    for (levels) |lv| {
        const present = lv != 0;
        const dst = b.values.addManyAsArrayAssumeCapacity(w);
        if (present) {
            dst.* = dense[k * w ..][0..w].*;
            k += 1;
        } else {
            dst.* = @splat(0);
        }
        b.pushValidity(present);
    }
}

fn scatterFixedRuntime(w: usize, b: *Builder, levels: []const Level, dense: []const u8) void {
    var k: usize = 0;
    for (levels) |lv| {
        const present = lv != 0;
        const dst = b.values.addManyAsSliceAssumeCapacity(w);
        if (present) {
            @memcpy(dst, dense[k * w ..][0..w]);
            k += 1;
        } else {
            @memset(dst, 0);
        }
        b.pushValidity(present);
    }
}

/// Appends `n` fixed-width entries looked up in a dictionary of `count` entries.
fn appendFixedDict(b: *Builder, dict: []const u8, count: usize, levels: ?[]const Level, n: usize, indices: []const u32) !void {
    const w = b.kind.fixed;
    switch (w) {
        inline 4, 8 => |cw| try lookupFixed(cw, b, dict, count, levels, n, indices),
        else => try lookupFixedRuntime(w, b, dict, count, levels, n, indices),
    }
}

fn lookupFixed(comptime w: usize, b: *Builder, dict: []const u8, count: usize, levels: ?[]const Level, n: usize, indices: []const u32) !void {
    if (levels) |lvs| {
        var k: usize = 0;
        for (lvs) |lv| {
            const present = lv != 0;
            const dst = b.values.addManyAsArrayAssumeCapacity(w);
            if (present) {
                const idx = indices[k];
                if (idx >= count) return error.InvalidPageData;
                dst.* = dict[idx * w ..][0..w].*;
                k += 1;
            } else {
                dst.* = @splat(0);
            }
            b.pushValidity(present);
        }
    } else {
        for (indices[0..n]) |idx| {
            if (idx >= count) return error.InvalidPageData;
            b.values.addManyAsArrayAssumeCapacity(w).* = dict[idx * w ..][0..w].*;
        }
        b.validRun(n);
    }
}

fn lookupFixedRuntime(w: usize, b: *Builder, dict: []const u8, count: usize, levels: ?[]const Level, n: usize, indices: []const u32) !void {
    var k: usize = 0;
    for (0..n) |i| {
        const present = if (levels) |lvs| lvs[i] != 0 else true;
        const dst = b.values.addManyAsSliceAssumeCapacity(w);
        if (present) {
            const idx = indices[k];
            if (idx >= count) return error.InvalidPageData;
            @memcpy(dst, dict[idx * w ..][0..w]);
            k += 1;
        } else {
            @memset(dst, 0);
        }
        b.pushValidity(present);
    }
}

/// Appends `n` PLAIN byte-array entries: each non-null entry is a 4-byte little-endian length
/// followed by that many bytes.
fn appendBytesPlain(b: *Builder, levels: ?[]const Level, n: usize, data: []const u8) !void {
    // The entries' bytes cannot exceed the page's value section.
    try b.values.ensureUnusedCapacity(b.allocator, std.math.add(usize, data.len, copy_block) catch return error.OutOfMemory);
    var out = b.values.items.len;
    const dst = b.values.items.ptr;
    var pos: usize = 0;
    for (0..n) |i| {
        const present = if (levels) |lvs| lvs[i] != 0 else true;
        if (present) {
            if (data.len - pos < 4) return error.EndOfData;
            const len: usize = std.mem.readInt(u32, data[pos..][0..4], .little);
            pos += 4;
            if (data.len - pos < len) return error.EndOfData;
            if (data.len - pos >= len + copy_block) {
                copyBlocks(dst + out, data.ptr + pos, len);
            } else {
                @memcpy(dst[out..][0..len], data[pos..][0..len]);
            }
            out += len;
            pos += len;
        }
        try pushOffsetAt(b, out);
        if (levels != null) b.pushValidity(present);
    }
    b.values.items.len = out;
    if (levels == null) b.validRun(n);
}

/// Appends `n` byte-array entries looked up in a dictionary.
fn appendBytesDict(b: *Builder, dict: BytesDict, levels: ?[]const Level, n: usize, indices: []const u32) !void {
    const count = dict.count();
    var total: usize = 0;
    for (indices) |idx| {
        if (idx >= count) return error.InvalidPageData;
        total = std.math.add(usize, total, dict.offsets[idx + 1] - dict.offsets[idx]) catch return error.OutOfMemory;
    }
    try b.values.ensureUnusedCapacity(b.allocator, std.math.add(usize, total, copy_block) catch return error.OutOfMemory);
    var out = b.values.items.len;
    const dst = b.values.items.ptr;
    var k: usize = 0;
    for (0..n) |i| {
        const present = if (levels) |lvs| lvs[i] != 0 else true;
        if (present) {
            const idx = indices[k];
            k += 1;
            const start = dict.offsets[idx];
            const len = dict.offsets[idx + 1] - start;
            copyBlocks(dst + out, dict.data.ptr + start, len);
            out += len;
        }
        try pushOffsetAt(b, out);
        if (levels != null) b.pushValidity(present);
    }
    b.values.items.len = out;
    if (levels == null) b.validRun(n);
}

/// Appends `end` as the next byte-array offset. Requires `reserve`.
fn pushOffsetAt(b: *Builder, end: usize) !void {
    if (b.large_offsets) {
        const v: i64 = @intCast(end);
        b.offsets.appendSliceAssumeCapacity(std.mem.asBytes(&v));
    } else {
        if (end > std.math.maxInt(i32)) return error.IntegerOverflow;
        const v: i32 = @intCast(end);
        b.offsets.appendSliceAssumeCapacity(std.mem.asBytes(&v));
    }
}

/// Appends `n` byte-array entries from decoded values (non-null entries in order).
fn appendBytesList(b: *Builder, levels: ?[]const Level, n: usize, values: []const []const u8) !void {
    var k: usize = 0;
    for (0..n) |i| {
        const present = if (levels) |lvs| lvs[i] != 0 else true;
        if (present) {
            try b.values.appendSlice(b.allocator, values[k]);
            k += 1;
        }
        try b.pushOffset();
        b.pushValidity(present);
    }
}

/// PLAIN booleans: bit k of `data` is the k-th non-null entry.
const PlainBits = struct {
    data: []const u8,
    fn get(self: PlainBits, k: usize) bool {
        return (self.data[k / 8] >> @intCast(k % 8)) & 1 == 1;
    }
};

/// RLE booleans decoded to one u32 per non-null entry.
const ListBits = struct {
    values: []const u32,
    fn get(self: ListBits, k: usize) bool {
        return self.values[k] != 0;
    }
};

/// Appends `n` boolean entries. The source must hold one bit per non-null entry.
fn appendBits(b: *Builder, levels: ?[]const Level, n: usize, src: anytype) void {
    var k: usize = 0;
    for (0..n) |i| {
        const present = if (levels) |lvs| lvs[i] != 0 else true;
        if (present) {
            b.pushBit(src.get(k));
            k += 1;
        } else {
            b.pushBit(false);
        }
        b.pushValidity(present);
    }
}

/// Writes entry `i` of `b` as a `Value` to `out[i * stride]`, for every entry (`out` must
/// have room): `null_val` for null entries, otherwise the value in the physical type's `Value`
/// form (INT96 as nanoseconds in `int64_val`). Byte payloads reference `b.values`; the caller
/// keeps `b.values` alive as long as the values.
pub fn writeValues(b: *const Buffers, element: format.SchemaElement, out: []Value, stride: usize) !void {
    if (b.length > 0 and (b.length - 1) * stride >= out.len) return error.InvalidArgument;
    const reader = try ValueReader.init(b, element);
    switch (reader.physical) {
        inline else => |physical| for (0..b.length) |i| {
            out[i * stride] = reader.atType(physical, i);
        },
    }
}

/// Reads single entries of decoded buffers as `Value`s (see `writeValues` for the forms).
/// Byte payloads reference `b.values`.
pub const ValueReader = struct {
    b: *const Buffers,
    physical: format.PhysicalType,
    width: usize,

    pub fn init(b: *const Buffers, element: format.SchemaElement) !ValueReader {
        const physical = element.type_ orelse return error.InvalidSchema;
        const width: usize = switch (physical) {
            .fixed_len_byte_array => safe.cast(element.type_length orelse 0) catch return error.InvalidTypeLength,
            else => 0,
        };
        return .{ .b = b, .physical = physical, .width = width };
    }

    pub fn at(self: ValueReader, i: usize) Value {
        return switch (self.physical) {
            inline else => |physical| self.atType(physical, i),
        };
    }

    /// `at` for a physical type known at compile time.
    pub inline fn atType(self: ValueReader, comptime physical: format.PhysicalType, i: usize) Value {
        const b = self.b;
        if (!b.isValid(i)) return .{ .null_val = {} };
        return switch (physical) {
            .boolean => .{ .bool_val = (b.values[i / 8] >> @intCast(i % 8)) & 1 == 1 },
            .int32 => .{ .int32_val = std.mem.bytesToValue(i32, b.values[i * 4 ..][0..4]) },
            .int64, .int96 => .{ .int64_val = std.mem.bytesToValue(i64, b.values[i * 8 ..][0..8]) },
            .float => .{ .float_val = std.mem.bytesToValue(f32, b.values[i * 4 ..][0..4]) },
            .double => .{ .double_val = std.mem.bytesToValue(f64, b.values[i * 8 ..][0..8]) },
            .byte_array => .{ .bytes_val = b.bytesAt(i) },
            .fixed_len_byte_array => .{ .fixed_bytes_val = b.values[i * self.width ..][0..self.width] },
        };
    }
};

test "ColumnCursor.nthZero finds the same zero as a scan" {
    var prng = std.Random.DefaultPrng.init(7);
    const random = prng.random();
    var levels: [200]Level = undefined;
    for (0..500) |_| {
        const len = random.uintAtMost(usize, levels.len);
        // Mostly non-zero, so runs of blocks without a zero occur.
        const zero_odds = random.uintAtMost(u32, 40) + 1;
        for (levels[0..len]) |*lv| lv.* = if (random.uintLessThan(u32, zero_odds) == 0) 0 else 1;
        var zeros: usize = 0;
        for (levels[0..len]) |lv| zeros += @intFromBool(lv == 0);
        for (0..zeros + 2) |n| {
            var want: ?usize = null;
            var seen: usize = 0;
            for (levels[0..len], 0..) |lv, i| {
                if (lv != 0) continue;
                if (seen == n) {
                    want = i;
                    break;
                }
                seen += 1;
            }
            try std.testing.expectEqual(want, ColumnCursor.nthZero(levels[0..len], n));
        }
    }
}
