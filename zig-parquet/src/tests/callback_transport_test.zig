//! Tests for callback-backed transport adapters (CallbackReader, CallbackWriter).
//!
//! Verifies that Parquet data can be written through CallbackWriter and
//! read back through CallbackReader, using in-memory arrays as backing.

const std = @import("std");
const parquet = @import("../lib.zig");
const CallbackReader = parquet.io.CallbackReader;
const CallbackWriter = parquet.io.CallbackWriter;
const SeekableReader = parquet.SeekableReader;
const WriteTarget = parquet.WriteTarget;

/// In-memory source for testing CallbackReader.
/// Wraps a byte slice and exposes it through function pointers.
const MemorySource = struct {
    data: []const u8,

    fn readAt(ctx: *anyopaque, offset: u64, out: []u8) SeekableReader.Error!usize {
        const self: *MemorySource = @ptrCast(@alignCast(ctx));
        if (offset >= self.data.len) return 0;
        const start: usize = @intCast(offset);
        const len = @min(out.len, self.data.len - start);
        @memcpy(out[0..len], self.data[start..][0..len]);
        return len;
    }

    fn size(ctx: *anyopaque) u64 {
        const self: *MemorySource = @ptrCast(@alignCast(ctx));
        return self.data.len;
    }
};

/// In-memory sink for testing CallbackWriter.
/// Collects written bytes into an ArrayList.
const MemorySink = struct {
    buffer: std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    closed: bool = false,

    fn init(allocator: std.mem.Allocator) MemorySink {
        return .{ .buffer = .empty, .allocator = allocator };
    }

    fn deinit(self: *MemorySink) void {
        self.buffer.deinit(self.allocator);
    }

    fn write(ctx: *anyopaque, data: []const u8) parquet.WriteError!void {
        const self: *MemorySink = @ptrCast(@alignCast(ctx));
        self.buffer.appendSlice(self.allocator, data) catch return error.OutOfMemory;
    }

    fn close(ctx: *anyopaque) parquet.WriteError!void {
        const self: *MemorySink = @ptrCast(@alignCast(ctx));
        self.closed = true;
    }

    fn getWritten(self: *const MemorySink) []const u8 {
        return self.buffer.items;
    }
};

// =============================================================================
// CallbackReader Unit Tests
// =============================================================================

test "CallbackReader basic read" {
    const data = "Hello, Callbacks!";
    var source = MemorySource{ .data = data };
    var cb = CallbackReader{
        .ctx = @ptrCast(&source),
        .read_at_fn = MemorySource.readAt,
        .size_fn = MemorySource.size,
    };
    const reader = cb.reader();

    try std.testing.expectEqual(@as(u64, data.len), reader.size());

    var buf: [5]u8 = undefined;
    const n = try reader.readAt(0, &buf);
    try std.testing.expectEqual(@as(usize, 5), n);
    try std.testing.expectEqualStrings("Hello", &buf);

    const n2 = try reader.readAt(7, &buf);
    try std.testing.expectEqual(@as(usize, 5), n2);
    try std.testing.expectEqualStrings("Callb", &buf);
}

test "CallbackReader past end returns 0" {
    const data = "Short";
    var source = MemorySource{ .data = data };
    var cb = CallbackReader{
        .ctx = @ptrCast(&source),
        .read_at_fn = MemorySource.readAt,
        .size_fn = MemorySource.size,
    };
    const reader = cb.reader();

    var buf: [10]u8 = undefined;
    const n = try reader.readAt(100, &buf);
    try std.testing.expectEqual(@as(usize, 0), n);
}

// =============================================================================
// CallbackWriter Unit Tests
// =============================================================================

test "CallbackWriter basic write" {
    const allocator = std.testing.allocator;
    var sink = MemorySink.init(allocator);
    defer sink.deinit();

    var cb = CallbackWriter{
        .ctx = @ptrCast(&sink),
        .write_fn = MemorySink.write,
        .close_fn = MemorySink.close,
    };
    const target = cb.target();

    try target.write("Hello");
    try target.write(", World!");
    try target.close();

    try std.testing.expectEqualStrings("Hello, World!", sink.getWritten());
    try std.testing.expect(sink.closed);
}

test "CallbackWriter without close callback" {
    const allocator = std.testing.allocator;
    var sink = MemorySink.init(allocator);
    defer sink.deinit();

    var cb = CallbackWriter{
        .ctx = @ptrCast(&sink),
        .write_fn = MemorySink.write,
    };
    const target = cb.target();

    try target.write("data");
    try target.close();

    try std.testing.expectEqualStrings("data", sink.getWritten());
    try std.testing.expect(!sink.closed);
}

// =============================================================================
// Round-Trip: Writer with CallbackWriter -> Reader with CallbackReader
// =============================================================================

test "callback round-trip: Writer -> Reader" {
    const allocator = std.testing.allocator;

    // Write via callbacks
    var sink = MemorySink.init(allocator);
    defer sink.deinit();

    var cb_writer = CallbackWriter{
        .ctx = @ptrCast(&sink),
        .write_fn = MemorySink.write,
        .close_fn = MemorySink.close,
    };

    var writer = try parquet.Writer.initWithTarget(allocator, cb_writer.target(), &.{
        .{ .name = "id", .type_ = .int32, .optional = false },
        .{ .name = "value", .type_ = .int64, .optional = false },
    });
    defer writer.deinit();

    const ids = [_]i32{ 10, 20, 30, 40, 50 };
    const values = [_]i64{ 100, 200, 300, 400, 500 };
    try writer.writeColumn(i32, 0, &ids);
    try writer.writeColumn(i64, 1, &values);
    try writer.close();

    // Read back via callbacks
    const written = sink.getWritten();
    var source = MemorySource{ .data = written };
    var cb_reader = CallbackReader{
        .ctx = @ptrCast(&source),
        .read_at_fn = MemorySource.readAt,
        .size_fn = MemorySource.size,
    };

    var reader = try parquet.DynamicReader.initFromSeekable(allocator, cb_reader.reader(), .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 5), reader.metadata.num_rows);

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }
    try std.testing.expectEqual(@as(usize, 5), rows.len);
    for (rows, 0..) |row, i| {
        try std.testing.expectEqual(ids[i], row.getColumn(0).?.asInt32().?);
        try std.testing.expectEqual(values[i], row.getColumn(1).?.asInt64().?);
    }
}

test "callback round-trip: DynamicWriter -> DynamicReader" {
    const allocator = std.testing.allocator;

    // Write via callbacks using DynamicWriter
    var sink = MemorySink.init(allocator);
    defer sink.deinit();

    var cb_writer = CallbackWriter{
        .ctx = @ptrCast(&sink),
        .write_fn = MemorySink.write,
        .close_fn = MemorySink.close,
    };

    var writer = parquet.DynamicWriter.init(allocator, cb_writer.target());
    defer writer.deinit();

    try writer.addColumn("id", parquet.TypeInfo.int32, .{});
    try writer.addColumn("value", parquet.TypeInfo.int64, .{});
    try writer.addColumn("name", parquet.TypeInfo.string, .{});
    try writer.begin();

    const test_data = [_]struct { id: i32, value: i64, name: []const u8 }{
        .{ .id = 1, .value = 100, .name = "alice" },
        .{ .id = 2, .value = 200, .name = "bob" },
        .{ .id = 3, .value = 300, .name = "charlie" },
    };

    for (test_data) |row| {
        try writer.setInt32(0, row.id);
        try writer.setInt64(1, row.value);
        try writer.setBytes(2, row.name);
        try writer.addRow();
    }
    try writer.close();

    // Read back via callbacks using DynamicReader
    const written = sink.getWritten();
    var source = MemorySource{ .data = written };
    var cb_reader = CallbackReader{
        .ctx = @ptrCast(&source),
        .read_at_fn = MemorySource.readAt,
        .size_fn = MemorySource.size,
    };

    var reader = try parquet.DynamicReader.initFromSeekable(allocator, cb_reader.reader(), .{});
    defer reader.deinit();

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 3), rows.len);
    for (test_data, 0..) |expected, i| {
        try std.testing.expectEqual(expected.id, rows[i].getColumn(0).?.asInt32().?);
        try std.testing.expectEqual(expected.value, rows[i].getColumn(1).?.asInt64().?);
        try std.testing.expectEqualStrings(expected.name, rows[i].getColumn(2).?.asBytes().?);
    }
}

test "callback round-trip: DynamicReader with optional columns" {
    const allocator = std.testing.allocator;

    // Write via callbacks
    var sink = MemorySink.init(allocator);
    defer sink.deinit();

    var cb_writer = CallbackWriter{
        .ctx = @ptrCast(&sink),
        .write_fn = MemorySink.write,
        .close_fn = MemorySink.close,
    };

    var writer = try parquet.Writer.initWithTarget(allocator, cb_writer.target(), &.{
        .{ .name = "x", .type_ = .int32, .optional = false },
        .{ .name = "y", .type_ = .double, .optional = true },
    });
    defer writer.deinit();

    try writer.writeColumn(i32, 0, &[_]i32{ 1, 2, 3 });
    const Optional_f64 = parquet.Optional(f64);
    try writer.writeColumnOptional(f64, 1, &[_]Optional_f64{
        .{ .value = 1.5 },
        .null_value,
        .{ .value = 3.5 },
    });
    try writer.close();

    // Read back via callbacks using DynamicReader
    const written = sink.getWritten();
    var source = MemorySource{ .data = written };
    var cb_reader = CallbackReader{
        .ctx = @ptrCast(&source),
        .read_at_fn = MemorySource.readAt,
        .size_fn = MemorySource.size,
    };

    var reader = try parquet.DynamicReader.initFromSeekable(allocator, cb_reader.reader(), .{});
    defer reader.deinit();

    try std.testing.expectEqual(@as(i64, 3), reader.getTotalNumRows());
    try std.testing.expectEqual(@as(usize, 2), reader.getNumColumns());

    const rows = try reader.readAllRows(0);
    defer {
        for (rows) |row| row.deinit();
        allocator.free(rows);
    }
    try std.testing.expectEqual(@as(usize, 3), rows.len);
}
