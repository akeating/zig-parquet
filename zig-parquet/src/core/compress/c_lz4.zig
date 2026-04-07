//! LZ4 compression/decompression for Parquet
//!
//! Uses the LZ4 C library via @cImport.
//! Parquet uses LZ4_RAW codec (raw LZ4 block format, no framing).

const std = @import("std");
const safe = @import("../safe.zig");
const c = @cImport({
    @cInclude("lz4.h");
});

pub const Error = error{
    CompressionError,
    DecompressionError,
    OutOfMemory,
};

/// Compress data using LZ4 (raw block format)
pub fn compress(allocator: std.mem.Allocator, data: []const u8) Error![]u8 {
    const bound = c.LZ4_compressBound(safe.castTo(c_int, data.len) catch return error.CompressionError);
    if (bound <= 0) return error.CompressionError;

    const dst = allocator.alloc(u8, safe.castTo(usize, bound) catch return error.CompressionError) catch return error.OutOfMemory;
    errdefer allocator.free(dst);

    const compressed_size = c.LZ4_compress_default(
        data.ptr,
        dst.ptr,
        safe.castTo(c_int, data.len) catch return error.CompressionError,
        bound,
    );

    if (compressed_size <= 0) {
        allocator.free(dst);
        return error.CompressionError;
    }

    // Resize to actual compressed size
    const resized = allocator.realloc(dst, safe.castTo(usize, compressed_size) catch return error.CompressionError) catch {
        return dst[0..safe.castTo(usize, compressed_size) catch return error.CompressionError];
    };
    return resized;
}

/// Decompress LZ4-compressed data (raw block format)
pub fn decompress(allocator: std.mem.Allocator, compressed: []const u8, uncompressed_size: usize) Error![]u8 {
    const result = allocator.alloc(u8, uncompressed_size) catch return error.OutOfMemory;
    errdefer allocator.free(result);

    const decompressed_size = c.LZ4_decompress_safe(
        compressed.ptr,
        result.ptr,
        safe.castTo(c_int, compressed.len) catch return error.DecompressionError,
        safe.castTo(c_int, uncompressed_size) catch return error.DecompressionError,
    );

    if (decompressed_size < 0) {
        allocator.free(result);
        return error.DecompressionError;
    }

    if ((safe.castTo(usize, decompressed_size) catch return error.DecompressionError) != uncompressed_size) {
        allocator.free(result);
        return error.DecompressionError;
    }

    return result;
}

test "lz4 round-trip" {
    const allocator = std.testing.allocator;

    const original = "Hello, World! This is a test of LZ4 compression.";

    // Compress
    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    // Decompress
    const decompressed = try decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    // Verify
    try std.testing.expectEqualStrings(original, decompressed);
}

test "lz4 compress empty data" {
    const allocator = std.testing.allocator;

    const original = "";

    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    const decompressed = try decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqual(@as(usize, 0), decompressed.len);
}
