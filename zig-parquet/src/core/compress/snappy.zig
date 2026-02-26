//! Snappy compression/decompression for Parquet
//!
//! Uses the Snappy C library via @cImport.
//! Note: Snappy is a C++ library with a C API (snappy-c.h).

const std = @import("std");
const c = @cImport({
    @cInclude("snappy-c.h");
});

pub const Error = error{
    CompressionError,
    DecompressionError,
    OutOfMemory,
};

/// Compress data using Snappy
pub fn compress(allocator: std.mem.Allocator, data: []const u8) Error![]u8 {
    const max_len = c.snappy_max_compressed_length(data.len);

    const dst = allocator.alloc(u8, max_len) catch return error.OutOfMemory;
    errdefer allocator.free(dst);

    var compressed_len: usize = max_len;
    const status = c.snappy_compress(
        data.ptr,
        data.len,
        dst.ptr,
        &compressed_len,
    );

    if (status != c.SNAPPY_OK) {
        allocator.free(dst);
        return error.CompressionError;
    }

    // Resize to actual compressed size
    const resized = allocator.realloc(dst, compressed_len) catch {
        return dst[0..compressed_len];
    };
    return resized;
}

/// Decompress Snappy-compressed data
pub fn decompress(allocator: std.mem.Allocator, compressed: []const u8, uncompressed_size: usize) Error![]u8 {
    const result = allocator.alloc(u8, uncompressed_size) catch return error.OutOfMemory;
    errdefer allocator.free(result);

    var output_length: usize = uncompressed_size;
    const status = c.snappy_uncompress(
        compressed.ptr,
        compressed.len,
        result.ptr,
        &output_length,
    );

    if (status != c.SNAPPY_OK) {
        allocator.free(result);
        return error.DecompressionError;
    }

    if (output_length != uncompressed_size) {
        allocator.free(result);
        return error.DecompressionError;
    }

    return result;
}

/// Get the uncompressed length from snappy-compressed data
pub fn getUncompressedLength(compressed: []const u8) ?usize {
    var result: usize = 0;
    const status = c.snappy_uncompressed_length(
        compressed.ptr,
        compressed.len,
        &result,
    );
    if (status != c.SNAPPY_OK) {
        return null;
    }
    return result;
}

test "snappy round-trip" {
    const allocator = std.testing.allocator;

    const original = "Hello, World! This is a test of snappy compression.";

    // Compress
    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    // Decompress
    const decompressed = try decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    // Verify
    try std.testing.expectEqualStrings(original, decompressed);
}

test "snappy compress empty data" {
    const allocator = std.testing.allocator;

    const original = "";

    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    const decompressed = try decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqual(@as(usize, 0), decompressed.len);
}
