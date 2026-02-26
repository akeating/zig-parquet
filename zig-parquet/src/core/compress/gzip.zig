//! Gzip compression/decompression for Parquet
//!
//! Uses the zlib C library with gzip container format (RFC 1952).
//! Parquet requires gzip format, not zlib format (RFC 1950).

const std = @import("std");

const safe = @import("../safe.zig");
const c = @cImport({
    @cInclude("zlib.h");
});

pub const Error = error{
    CompressionError,
    DecompressionError,
    OutOfMemory,
    InvalidSize,
};

// Maximum decompression size (256MB) - protects against corrupted size values
const MAX_DECOMPRESS_SIZE: usize = 256 * 1024 * 1024;

// Gzip format uses windowBits = 15 + 16 = 31
const GZIP_WINDOW_BITS: c_int = 15 + 16;

/// Compress data using gzip format
pub fn compress(allocator: std.mem.Allocator, data: []const u8) Error![]u8 {
    // Estimate output size (deflateBound is for zlib format, gzip adds ~18 bytes header/footer)
    const bound = c.deflateBound(null, safe.castTo(c_ulong, data.len) catch return error.InvalidSize) + 32;

    const dst = allocator.alloc(u8, bound) catch return error.OutOfMemory;
    errdefer allocator.free(dst);

    // Initialize zlib stream for gzip compression
    var stream: c.z_stream = .{
        .next_in = @constCast(data.ptr),
        .avail_in = safe.castTo(c_uint, data.len) catch return error.InvalidSize,
        .next_out = dst.ptr,
        .avail_out = safe.castTo(c_uint, dst.len) catch return error.InvalidSize,
        .zalloc = null,
        .zfree = null,
        .@"opaque" = null,
        .total_in = 0,
        .total_out = 0,
        .msg = null,
        .state = null,
        .data_type = 0,
        .adler = 0,
        .reserved = 0,
    };

    // deflateInit2 with gzip format (windowBits = 15 + 16)
    var result = c.deflateInit2(
        &stream,
        c.Z_DEFAULT_COMPRESSION, // compression level
        c.Z_DEFLATED, // method
        GZIP_WINDOW_BITS, // windowBits: 15 + 16 for gzip
        8, // memLevel (default)
        c.Z_DEFAULT_STRATEGY, // strategy
    );
    if (result != c.Z_OK) {
        allocator.free(dst);
        return error.CompressionError;
    }

    // Compress in one shot
    result = c.deflate(&stream, c.Z_FINISH);
    if (result != c.Z_STREAM_END) {
        _ = c.deflateEnd(&stream);
        allocator.free(dst);
        return error.CompressionError;
    }

    const compressed_size = stream.total_out;
    _ = c.deflateEnd(&stream);

    // Resize to actual compressed size
    const resized = allocator.realloc(dst, compressed_size) catch {
        return dst[0..compressed_size];
    };
    return resized;
}

/// Decompress gzip-compressed data
pub fn decompress(allocator: std.mem.Allocator, compressed: []const u8, uncompressed_size: usize) Error![]u8 {
    // Empty uncompressed size is valid - return empty slice
    if (uncompressed_size == 0) {
        return allocator.alloc(u8, 0) catch return error.OutOfMemory;
    }
    // Sanity check to prevent allocation of corrupted sizes
    if (uncompressed_size > MAX_DECOMPRESS_SIZE) {
        return error.InvalidSize;
    }
    // Check compressed data is valid
    if (compressed.len == 0 or compressed.len > MAX_DECOMPRESS_SIZE) {
        return error.DecompressionError;
    }
    // Additional sanity: uncompressed shouldn't be more than 1000x the compressed size
    // (typical gzip ratio is 5-10x, 1000x is very conservative)
    if (uncompressed_size > compressed.len * 1000) {
        return error.InvalidSize;
    }
    const dst = allocator.alloc(u8, uncompressed_size) catch return error.OutOfMemory;
    errdefer allocator.free(dst);

    // Initialize zlib stream for gzip decompression
    const avail_out = std.math.cast(c_uint, dst.len) orelse {
        allocator.free(dst);
        return error.DecompressionError;
    };
    var stream: c.z_stream = .{
        .next_in = @constCast(compressed.ptr),
        .avail_in = std.math.cast(c_uint, compressed.len) orelse return error.DecompressionError,
        .next_out = dst.ptr,
        .avail_out = avail_out,
        .zalloc = null,
        .zfree = null,
        .@"opaque" = null,
        .total_in = 0,
        .total_out = 0,
        .msg = null,
        .state = null,
        .data_type = 0,
        .adler = 0,
        .reserved = 0,
    };

    // inflateInit2 with gzip format (windowBits = 15 + 16)
    var result = c.inflateInit2(&stream, GZIP_WINDOW_BITS);
    if (result != c.Z_OK) {
        allocator.free(dst);
        return error.DecompressionError;
    }

    // Decompress - handle concatenated gzip streams (multiple gzip members)
    // Some Parquet files contain pages with multiple gzip streams concatenated together.
    // We need to loop until all input is consumed.
    // Note: inflateReset preserves next_out/avail_out, so output position is maintained.

    while (stream.avail_in > 0 and stream.avail_out > 0) {
        result = c.inflate(&stream, c.Z_NO_FLUSH);
        if (result == c.Z_STREAM_END) {
            // End of current gzip stream - check for more data (concatenated member)
            if (stream.avail_in > 0) {
                // Reset for next concatenated gzip member
                // Note: inflateReset resets total_out but preserves next_out/avail_out
                const reset_result = c.inflateReset(&stream);
                if (reset_result != c.Z_OK) {
                    _ = c.inflateEnd(&stream);
                    allocator.free(dst);
                    return error.DecompressionError;
                }
            }
        } else if (result != c.Z_OK) {
            _ = c.inflateEnd(&stream);
            allocator.free(dst);
            return error.DecompressionError;
        }
    }

    // Calculate final output size from remaining avail_out
    const decompressed_size = uncompressed_size - stream.avail_out;
    _ = c.inflateEnd(&stream);

    // Verify output size matches expected
    if (decompressed_size != uncompressed_size) {
        allocator.free(dst);
        return error.DecompressionError;
    }

    return dst;
}

test "gzip round-trip" {
    const allocator = std.testing.allocator;

    const original = "Hello, World! This is a test of gzip compression.";

    // Compress
    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    // Verify gzip magic bytes (0x1f, 0x8b)
    try std.testing.expectEqual(@as(u8, 0x1f), compressed[0]);
    try std.testing.expectEqual(@as(u8, 0x8b), compressed[1]);

    // Decompress
    const decompressed = try decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    // Verify
    try std.testing.expectEqualStrings(original, decompressed);
}

test "gzip compress empty data" {
    const allocator = std.testing.allocator;

    const original = "";

    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    // Gzip header is still present even for empty data
    try std.testing.expect(compressed.len > 0);

    const decompressed = try decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqual(@as(usize, 0), decompressed.len);
}

test "gzip concatenated streams" {
    const allocator = std.testing.allocator;

    // Create two separate gzip streams
    const part1 = "Hello, ";
    const part2 = "World!";

    const compressed1 = try compress(allocator, part1);
    defer allocator.free(compressed1);

    const compressed2 = try compress(allocator, part2);
    defer allocator.free(compressed2);

    // Concatenate them
    const concatenated = try allocator.alloc(u8, compressed1.len + compressed2.len);
    defer allocator.free(concatenated);
    @memcpy(concatenated[0..compressed1.len], compressed1);
    @memcpy(concatenated[compressed1.len..], compressed2);

    // Decompress should handle both streams
    const total_len = part1.len + part2.len;
    const decompressed = try decompress(allocator, concatenated, total_len);
    defer allocator.free(decompressed);

    // Verify combined output
    try std.testing.expectEqualStrings("Hello, World!", decompressed);
}
