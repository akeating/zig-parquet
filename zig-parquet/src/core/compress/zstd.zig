//! Zstandard (zstd) compression/decompression for Parquet
//!
//! Uses the official libzstd C library for both compression and decompression.

const std = @import("std");

const c = @cImport({
    @cInclude("zstd.h");
});

pub const Error = error{
    CompressionError,
    DecompressionError,
    OutOfMemory,
};

/// Compress data using zstd
pub fn compress(allocator: std.mem.Allocator, data: []const u8) Error![]u8 {
    // Get the maximum compressed size
    const bound = c.ZSTD_compressBound(data.len);
    if (bound == 0) return error.CompressionError;

    // Allocate output buffer
    const dst = allocator.alloc(u8, bound) catch return error.OutOfMemory;
    errdefer allocator.free(dst);

    // Compress with default compression level (3)
    const result = c.ZSTD_compress(
        dst.ptr,
        dst.len,
        data.ptr,
        data.len,
        3, // compression level
    );

    // Check for errors
    if (c.ZSTD_isError(result) != 0) {
        allocator.free(dst);
        return error.CompressionError;
    }

    // Resize to actual compressed size
    const resized = allocator.realloc(dst, result) catch {
        // If realloc fails, just return the oversized buffer
        return dst[0..result];
    };
    return resized;
}

/// Decompress zstd-compressed data
pub fn decompress(allocator: std.mem.Allocator, compressed: []const u8, uncompressed_size: usize) Error![]u8 {
    // Allocate output buffer
    const dst = allocator.alloc(u8, uncompressed_size) catch return error.OutOfMemory;
    errdefer allocator.free(dst);

    // Decompress
    const result = c.ZSTD_decompress(
        dst.ptr,
        dst.len,
        compressed.ptr,
        compressed.len,
    );

    // Check for errors
    if (c.ZSTD_isError(result) != 0) {
        allocator.free(dst);
        return error.DecompressionError;
    }

    // Verify output size matches expected
    if (result != uncompressed_size) {
        allocator.free(dst);
        return error.DecompressionError;
    }

    return dst;
}

test "zstd round-trip" {
    const allocator = std.testing.allocator;

    const original = "Hello, World! This is a test of zstd compression.";

    // Compress
    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    // Decompress
    const decompressed = try decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    // Verify
    try std.testing.expectEqualStrings(original, decompressed);
}

test "zstd compress empty data" {
    const allocator = std.testing.allocator;

    const original = "";

    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    const decompressed = try decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqual(@as(usize, 0), decompressed.len);
}
