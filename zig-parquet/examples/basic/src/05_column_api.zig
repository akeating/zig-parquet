const std = @import("std");
const parquet = @import("parquet");
const Optional = parquet.Optional;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    const output_path = "column_api.parquet";
    defer std.Io.Dir.cwd().deleteFile(io, output_path) catch {};

    std.debug.print("Writing columns to {s}...\n", .{output_path});

    // Write phase — define schema explicitly, write each column separately
    {
        const file = try std.Io.Dir.cwd().createFile(io, output_path, .{});
        defer file.close(io);

        var writer = try parquet.writeToFile(allocator, file, io, &.{
            .{ .name = "id", .type_ = .int64, .optional = false, .codec = .zstd },
            .{ .name = "score", .type_ = .double, .optional = false },
            .{ .name = "name", .type_ = .byte_array, .optional = true },
        });
        defer writer.deinit();

        try writer.writeColumnOptional(i64, 0, &[_]Optional(i64){
            .{ .value = 1 }, .{ .value = 2 }, .{ .value = 3 }, .{ .value = 4 }, .{ .value = 5 },
        });
        try writer.writeColumnOptional(f64, 1, &[_]Optional(f64){
            .{ .value = 95.0 }, .{ .value = 87.5 }, .{ .value = 92.3 }, .{ .value = 78.1 }, .{ .value = 100.0 },
        });
        try writer.writeColumnOptional([]const u8, 2, &[_]Optional([]const u8){
            .{ .value = "Alice" }, .null_value, .{ .value = "Charlie" }, .{ .value = "Diana" }, .null_value,
        });

        try writer.close();
    }

    std.debug.print("Reading columns from {s}...\n", .{output_path});

    // Read phase — readColumns returns one Arrow array per column,
    // each with its values in one contiguous buffer
    {
        const file = try std.Io.Dir.cwd().openFile(io, output_path, .{});
        defer file.close(io);

        var reader = try parquet.openFileDynamic(allocator, file, io, .{});
        defer reader.deinit();

        // null reads all columns; pass e.g. &.{ 0, 2 } to read only those columns
        var result = try reader.readColumns(0, .{});
        defer result.deinit();

        const id_col = &result.arrays[0];
        const score_col = &result.arrays[1];
        const name_col = &result.arrays[2];

        // Primitive columns are slices: sum a column without touching the others
        const scores = primitiveValues(f64, score_col);
        var total: f64 = 0;
        for (scores) |s| total += s;
        std.debug.print("{d} scores, mean {d:.2}\n", .{ scores.len, total / @as(f64, @floatFromInt(scores.len)) });

        const ids = primitiveValues(i64, id_col);
        for (ids, scores, 0..) |id, score, i| {
            if (isValid(name_col, i)) {
                std.debug.print("ID {d}: score {d:.1}, name: {s}\n", .{ id, score, byteValue(name_col, i) });
            } else {
                std.debug.print("ID {d}: score {d:.1}, name: (null)\n", .{ id, score });
            }
        }
    }

    std.debug.print("Done!\n", .{});
}

// Helpers for reading Arrow C Data Interface buffers.
// buffers[0] is the validity bitmap (null when the column has no nulls).
// Primitive columns: buffers[1] holds the values.
// Byte array / string columns: buffers[1] holds i32 offsets, buffers[2] the bytes.

/// Values of a primitive column as a slice. Entries at null positions are undefined.
fn primitiveValues(comptime T: type, arr: *const parquet.ArrowArray) []const T {
    const start: usize = @intCast(arr.offset);
    const len: usize = @intCast(arr.length);
    const ptr: [*]const T = @ptrCast(@alignCast(arr.buffers[1].?));
    return ptr[start..][0..len];
}

/// Whether entry `i` is non-null. The bitmap is LSB-first, one bit per entry.
fn isValid(arr: *const parquet.ArrowArray, i: usize) bool {
    const bitmap: [*]const u8 = @ptrCast(arr.buffers[0] orelse return true);
    const bit = @as(usize, @intCast(arr.offset)) + i;
    return (bitmap[bit / 8] >> @intCast(bit % 8)) & 1 == 1;
}

/// Entry `i` of a byte array or string column.
fn byteValue(arr: *const parquet.ArrowArray, i: usize) []const u8 {
    const offsets: [*]const i32 = @ptrCast(@alignCast(arr.buffers[1].?));
    const data: [*]const u8 = @ptrCast(arr.buffers[2].?);
    const j = @as(usize, @intCast(arr.offset)) + i;
    return data[@intCast(offsets[j])..@intCast(offsets[j + 1])];
}
