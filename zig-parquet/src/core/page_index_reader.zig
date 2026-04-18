//! Lazy page-index loader.
//!
//! A `ColumnChunk` points to its page-index blobs via optional fields:
//!   - `offset_index_offset` + `offset_index_length`
//!   - `column_index_offset` + `column_index_length`
//!
//! This module reads and Thrift-parses those blobs on demand. It owns no state
//! beyond the source reference; the caller owns returned `OffsetIndex` /
//! `ColumnIndex` values and must `deinit` them.
//!
//! The batch helpers (`readOffsetIndexesForRowGroup` /
//! `readColumnIndexesForRowGroup`) coalesce per-column reads into a single
//! `readAt` where possible, which matters for remote I/O.

const std = @import("std");
const safe = @import("safe.zig");
const format = @import("format.zig");
const thrift = @import("thrift/mod.zig");
const seekable_reader = @import("seekable_reader.zig");

pub const SeekableReader = seekable_reader.SeekableReader;

pub const Error = error{
    InputOutput,
    OutOfMemory,
    InvalidPageIndex,
    IntegerOverflow,
} || thrift_errors;

const thrift_errors = error{
    EndOfData,
    VarIntTooLong,
    IntegerOverflow,
    LengthTooLong,
    ListTooLong,
    InvalidFieldType,
    InvalidListType,
    InvalidBoundaryOrder,
    InvalidBoolEncoding,
};

pub const PageIndexReader = struct {
    source: SeekableReader,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, source: SeekableReader) Self {
        return .{ .source = source, .allocator = allocator };
    }

    /// Read the `OffsetIndex` for a single chunk, or null if the chunk does
    /// not advertise one. Caller owns the returned value.
    pub fn readOffsetIndex(self: *Self, chunk: *const format.ColumnChunk) Error!?format.OffsetIndex {
        const offset = chunk.offset_index_offset orelse return null;
        const length = chunk.offset_index_length orelse return null;
        const blob = try self.readBlob(offset, length);
        defer self.allocator.free(blob);

        var reader = thrift.CompactReader.init(blob);
        return try format.OffsetIndex.parse(self.allocator, &reader);
    }

    /// Read the `ColumnIndex` for a single chunk, or null if the chunk does
    /// not advertise one. Caller owns the returned value.
    pub fn readColumnIndex(self: *Self, chunk: *const format.ColumnChunk) Error!?format.ColumnIndex {
        const offset = chunk.column_index_offset orelse return null;
        const length = chunk.column_index_length orelse return null;
        const blob = try self.readBlob(offset, length);
        defer self.allocator.free(blob);

        var reader = thrift.CompactReader.init(blob);
        return try format.ColumnIndex.parse(self.allocator, &reader);
    }

    /// Read the offset index for every column chunk in a row group.
    /// Returned slice has one entry per chunk (null where absent). Caller owns
    /// the slice and each contained index.
    pub fn readOffsetIndexesForRowGroup(self: *Self, rg: *const format.RowGroup) Error![]?format.OffsetIndex {
        return self.readIndexesForRowGroup(.offset, rg);
    }

    /// Read the column index for every column chunk in a row group.
    pub fn readColumnIndexesForRowGroup(self: *Self, rg: *const format.RowGroup) Error![]?format.ColumnIndex {
        return self.readIndexesForRowGroup(.column, rg);
    }

    const IndexKind = enum { offset, column };

    fn IndexResult(comptime kind: IndexKind) type {
        return switch (kind) {
            .offset => format.OffsetIndex,
            .column => format.ColumnIndex,
        };
    }

    fn readIndexesForRowGroup(self: *Self, comptime kind: IndexKind, rg: *const format.RowGroup) Error![]?IndexResult(kind) {
        const T = IndexResult(kind);
        const out = try self.allocator.alloc(?T, rg.columns.len);
        @memset(out, null);

        // Track which entries were successfully populated so errdefer can clean up.
        var populated: usize = 0;
        errdefer {
            for (out[0..populated]) |*maybe| {
                if (maybe.*) |*idx| idx.deinit(self.allocator);
            }
            self.allocator.free(out);
        }

        // Coalesce reads: find the tightest byte span covering every requested
        // blob, pull it in one go, then parse each from its local offset.
        var span_lo: ?u64 = null;
        var span_hi: u64 = 0;
        for (rg.columns) |*chunk| {
            const off_opt = switch (kind) {
                .offset => chunk.offset_index_offset,
                .column => chunk.column_index_offset,
            };
            const len_opt = switch (kind) {
                .offset => chunk.offset_index_length,
                .column => chunk.column_index_length,
            };
            const off = off_opt orelse continue;
            const len = len_opt orelse continue;
            if (off < 0 or len < 0) return error.InvalidPageIndex;
            const o: u64 = @intCast(off);
            const l: u64 = @intCast(len);
            const end = std.math.add(u64, o, l) catch return error.IntegerOverflow;
            span_lo = if (span_lo) |s| @min(s, o) else o;
            if (end > span_hi) span_hi = end;
        }

        if (span_lo == null) {
            // Nothing to read — every entry stays null.
            populated = out.len;
            return out;
        }

        const total_len_u64 = span_hi - span_lo.?;
        const total_len = safe.castTo(usize, total_len_u64) catch return error.IntegerOverflow;
        const buffer = try self.allocator.alloc(u8, total_len);
        defer self.allocator.free(buffer);
        const got = self.source.readAt(span_lo.?, buffer) catch return error.InputOutput;
        if (got != total_len) return error.InputOutput;

        for (rg.columns, 0..) |*chunk, i| {
            const off_opt = switch (kind) {
                .offset => chunk.offset_index_offset,
                .column => chunk.column_index_offset,
            };
            const len_opt = switch (kind) {
                .offset => chunk.offset_index_length,
                .column => chunk.column_index_length,
            };
            const off = off_opt orelse {
                populated = i + 1;
                continue;
            };
            const len = len_opt orelse {
                populated = i + 1;
                continue;
            };
            const o: u64 = @intCast(off);
            const l: usize = safe.castTo(usize, @as(u64, @intCast(len))) catch return error.IntegerOverflow;
            const rel = safe.castTo(usize, o - span_lo.?) catch return error.IntegerOverflow;
            if (rel + l > buffer.len) return error.InvalidPageIndex;

            var reader = thrift.CompactReader.init(buffer[rel .. rel + l]);
            out[i] = try switch (kind) {
                .offset => format.OffsetIndex.parse(self.allocator, &reader),
                .column => format.ColumnIndex.parse(self.allocator, &reader),
            };
            populated = i + 1;
        }

        return out;
    }

    fn readBlob(self: *Self, offset: i64, length: i32) Error![]u8 {
        if (offset < 0 or length < 0) return error.InvalidPageIndex;
        const o: u64 = @intCast(offset);
        const len = safe.castTo(usize, @as(u64, @intCast(length))) catch return error.IntegerOverflow;

        const buffer = try self.allocator.alloc(u8, len);
        errdefer self.allocator.free(buffer);

        const got = self.source.readAt(o, buffer) catch return error.InputOutput;
        if (got != len) return error.InputOutput;
        return buffer;
    }
};

/// Free a slice of optional indexes as produced by `readOffsetIndexesForRowGroup`
/// / `readColumnIndexesForRowGroup`.
pub fn freeOptionalIndexes(
    comptime T: type,
    allocator: std.mem.Allocator,
    items: []?T,
) void {
    for (items) |*maybe| {
        if (maybe.*) |*idx| idx.deinit(allocator);
    }
    allocator.free(items);
}

// =============================================================================
// Tests
// =============================================================================

const testing = std.testing;

/// Minimal in-memory SeekableReader for tests.
const BufferSource = struct {
    data: []const u8,

    fn readAt(ctx: *anyopaque, offset: u64, buf: []u8) SeekableReader.Error!usize {
        const self: *BufferSource = @ptrCast(@alignCast(ctx));
        if (offset >= self.data.len) return 0;
        const end = @min(offset + buf.len, self.data.len);
        const n = end - offset;
        @memcpy(buf[0..n], self.data[offset..end]);
        return n;
    }

    fn sizeFn(ctx: *anyopaque) u64 {
        const self: *BufferSource = @ptrCast(@alignCast(ctx));
        return self.data.len;
    }

    const vtable: SeekableReader.VTable = .{
        .readAt = readAt,
        .size = sizeFn,
    };

    fn reader(self: *BufferSource) SeekableReader {
        return .{ .ptr = self, .vtable = &vtable };
    }
};

test "PageIndexReader: read single OffsetIndex" {
    var thrift_writer = thrift.CompactWriter.init(testing.allocator);
    defer thrift_writer.deinit();

    const locs = [_]format.PageLocation{
        .{ .offset = 100, .compressed_page_size = 50, .first_row_index = 0 },
        .{ .offset = 150, .compressed_page_size = 60, .first_row_index = 10 },
    };
    const oi = format.OffsetIndex{ .page_locations = @constCast(&locs) };
    try oi.serialize(&thrift_writer);

    const blob = thrift_writer.getWritten();
    // Lay out file with the blob at offset 1000.
    const prefix: [1000]u8 = undefined;
    _ = prefix;
    const file = try testing.allocator.alloc(u8, 1000 + blob.len);
    defer testing.allocator.free(file);
    @memset(file[0..1000], 0);
    @memcpy(file[1000 .. 1000 + blob.len], blob);

    var src = BufferSource{ .data = file };
    var pir = PageIndexReader.init(testing.allocator, src.reader());

    const chunk = format.ColumnChunk{
        .offset_index_offset = 1000,
        .offset_index_length = @intCast(blob.len),
    };

    var parsed = (try pir.readOffsetIndex(&chunk)).?;
    defer parsed.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 2), parsed.page_locations.len);
    try testing.expectEqual(@as(i64, 100), parsed.page_locations[0].offset);
    try testing.expectEqual(@as(i64, 10), parsed.page_locations[1].first_row_index);
}

test "PageIndexReader: returns null when chunk has no index" {
    const file = [_]u8{0} ** 100;
    var src = BufferSource{ .data = &file };
    var pir = PageIndexReader.init(testing.allocator, src.reader());

    const chunk = format.ColumnChunk{};
    try testing.expect((try pir.readOffsetIndex(&chunk)) == null);
    try testing.expect((try pir.readColumnIndex(&chunk)) == null);
}

test "PageIndexReader: batch read coalesces into one readAt" {
    var thrift_writer_a = thrift.CompactWriter.init(testing.allocator);
    defer thrift_writer_a.deinit();
    const oi_a = format.OffsetIndex{ .page_locations = @constCast(&[_]format.PageLocation{
        .{ .offset = 4, .compressed_page_size = 20, .first_row_index = 0 },
    }) };
    try oi_a.serialize(&thrift_writer_a);
    const blob_a = thrift_writer_a.getWritten();

    var thrift_writer_b = thrift.CompactWriter.init(testing.allocator);
    defer thrift_writer_b.deinit();
    const oi_b = format.OffsetIndex{ .page_locations = @constCast(&[_]format.PageLocation{
        .{ .offset = 24, .compressed_page_size = 15, .first_row_index = 0 },
        .{ .offset = 39, .compressed_page_size = 17, .first_row_index = 5 },
    }) };
    try oi_b.serialize(&thrift_writer_b);
    const blob_b = thrift_writer_b.getWritten();

    const base: u64 = 500;
    const gap: u64 = 3; // unrelated bytes between the two blobs
    const total = base + blob_a.len + gap + blob_b.len;
    const file = try testing.allocator.alloc(u8, total);
    defer testing.allocator.free(file);
    @memset(file, 0xAA);
    @memcpy(file[base .. base + blob_a.len], blob_a);
    @memcpy(file[base + blob_a.len + gap .. base + blob_a.len + gap + blob_b.len], blob_b);

    var src = CountingSource{ .data = file };
    var pir = PageIndexReader.init(testing.allocator, src.reader());

    const chunks = [_]format.ColumnChunk{
        .{ .offset_index_offset = @intCast(base), .offset_index_length = @intCast(blob_a.len) },
        .{ .offset_index_offset = @intCast(base + blob_a.len + gap), .offset_index_length = @intCast(blob_b.len) },
    };
    const rg = format.RowGroup{ .columns = @constCast(&chunks) };

    const result = try pir.readOffsetIndexesForRowGroup(&rg);
    defer freeOptionalIndexes(format.OffsetIndex, testing.allocator, result);

    try testing.expectEqual(@as(usize, 2), result.len);
    try testing.expect(result[0] != null);
    try testing.expect(result[1] != null);
    try testing.expectEqual(@as(usize, 1), result[0].?.page_locations.len);
    try testing.expectEqual(@as(usize, 2), result[1].?.page_locations.len);
    // Batch read must use a single readAt.
    try testing.expectEqual(@as(usize, 1), src.read_count);
}

/// Like BufferSource but counts readAt invocations — used to verify the batch
/// path really only issues a single I/O.
const CountingSource = struct {
    data: []const u8,
    read_count: usize = 0,

    fn readAt(ctx: *anyopaque, offset: u64, buf: []u8) SeekableReader.Error!usize {
        const self: *CountingSource = @ptrCast(@alignCast(ctx));
        self.read_count += 1;
        if (offset >= self.data.len) return 0;
        const end = @min(offset + buf.len, self.data.len);
        const n = end - offset;
        @memcpy(buf[0..n], self.data[offset..end]);
        return n;
    }

    fn sizeFn(ctx: *anyopaque) u64 {
        const self: *CountingSource = @ptrCast(@alignCast(ctx));
        return self.data.len;
    }

    const vtable: SeekableReader.VTable = .{
        .readAt = readAt,
        .size = sizeFn,
    };

    fn reader(self: *CountingSource) SeekableReader {
        return .{ .ptr = self, .vtable = &vtable };
    }
};
