//! Brotli compression/decompression for Parquet
//!
//! Uses the Brotli C library via @cImport.

const std = @import("std");
const c = @cImport({
    @cInclude("brotli/decode.h");
    @cInclude("brotli/encode.h");
});

pub const Error = error{
    CompressionError,
    DecompressionError,
    OutOfMemory,
};

/// Compress data using Brotli
pub fn compress(allocator: std.mem.Allocator, data: []const u8) Error![]u8 {
    const max_size = c.BrotliEncoderMaxCompressedSize(data.len);
    if (max_size == 0) return error.CompressionError;

    const dst = allocator.alloc(u8, max_size) catch return error.OutOfMemory;
    errdefer allocator.free(dst);

    var encoded_size: usize = max_size;
    const result = c.BrotliEncoderCompress(
        c.BROTLI_DEFAULT_QUALITY, // quality (0-11, default 11)
        c.BROTLI_DEFAULT_WINDOW, // lgwin (10-24, default 22)
        c.BROTLI_MODE_GENERIC, // mode
        data.len,
        data.ptr,
        &encoded_size,
        dst.ptr,
    );

    if (result != c.BROTLI_TRUE) {
        allocator.free(dst);
        return error.CompressionError;
    }

    // Resize to actual compressed size
    const resized = allocator.realloc(dst, encoded_size) catch {
        return dst[0..encoded_size];
    };
    return resized;
}

/// Decompress Brotli-compressed data
pub fn decompress(allocator: std.mem.Allocator, compressed: []const u8, uncompressed_size: usize) Error![]u8 {
    const result = allocator.alloc(u8, uncompressed_size) catch return error.OutOfMemory;
    errdefer allocator.free(result);

    var decoded_size: usize = uncompressed_size;
    const status = c.BrotliDecoderDecompress(
        compressed.len,
        compressed.ptr,
        &decoded_size,
        result.ptr,
    );

    if (status != c.BROTLI_DECODER_RESULT_SUCCESS) {
        allocator.free(result);
        return error.DecompressionError;
    }

    if (decoded_size != uncompressed_size) {
        allocator.free(result);
        return error.DecompressionError;
    }

    return result;
}

test "brotli round-trip" {
    const allocator = std.testing.allocator;

    const original = "Hello, World! This is a test of Brotli compression.";

    // Compress
    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    // Decompress
    const decompressed = try decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    // Verify
    try std.testing.expectEqualStrings(original, decompressed);
}

test "brotli compress empty data" {
    const allocator = std.testing.allocator;

    const original = "";

    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    const decompressed = try decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqual(@as(usize, 0), decompressed.len);
}
