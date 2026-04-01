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
    InvalidSize,
};

pub const DEFAULT_LEVEL: c_int = 3;

/// Compress data using zstd at the default level (3)
pub fn compress(allocator: std.mem.Allocator, data: []const u8) Error![]u8 {
    return compressWithLevel(allocator, data, DEFAULT_LEVEL);
}

/// Compress data using zstd at a specific compression level (1-22)
pub fn compressWithLevel(allocator: std.mem.Allocator, data: []const u8, level: c_int) Error![]u8 {
    const bound = c.ZSTD_compressBound(data.len);
    if (bound == 0) return error.CompressionError;

    const dst = allocator.alloc(u8, bound) catch return error.OutOfMemory;
    errdefer allocator.free(dst);

    const result = c.ZSTD_compress(
        dst.ptr,
        dst.len,
        data.ptr,
        data.len,
        level,
    );

    if (c.ZSTD_isError(result) != 0) {
        allocator.free(dst);
        return error.CompressionError;
    }

    const resized = allocator.realloc(dst, result) catch {
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
