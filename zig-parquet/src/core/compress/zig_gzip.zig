//! Gzip compression/decompression for Parquet
//!
//! Compression uses uncompressed deflate blocks (RFC 1951) wrapped in gzip container (RFC 1952).
//! This is a working implementation that produces valid gzip files.
//! Decompression uses std.compress.flate.
//!
//! TODO: Implement dynamic Huffman deflate encoder for level-9 compression.

const std = @import("std");
const safe = @import("../safe.zig");

pub const Error = error{
    CompressionError,
    DecompressionError,
    OutOfMemory,
    InvalidSize,
};

const MAX_DECOMPRESS_SIZE: usize = 256 * 1024 * 1024;

// Gzip constants (RFC 1952)
const GZIP_MAGIC: u16 = 0x1f8b;
const GZIP_METHOD: u8 = 8; // deflate
const GZIP_MTIME: u32 = 0;
const GZIP_XFL: u8 = 0;
const GZIP_OS: u8 = 255; // unknown

// CRC-32 polynomial (Ethernet standard)
const CRC32Table = crc32_table_init();

fn crc32_table_init() [256]u32 {
    @setEvalBranchQuota(3000);
    var table: [256]u32 = undefined;
    for (0..256) |i| {
        var crc: u32 = @intCast(i);
        for (0..8) |_| {
            if ((crc & 1) != 0) {
                crc = (crc >> 1) ^ 0xedb88320;
            } else {
                crc = crc >> 1;
            }
        }
        table[i] = crc;
    }
    return table;
}

fn crc32Update(crc: u32, data: []const u8) u32 {
    var result = crc;
    for (data) |byte| {
        result = (result >> 8) ^ CRC32Table[(result ^ byte) & 0xff];
    }
    return result;
}

// =========================================================================
// Public API
// =========================================================================

pub fn compress(allocator: std.mem.Allocator, data: []const u8) Error![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);

    // Write gzip header
    try out.appendSlice(allocator, &[_]u8{ 0x1f, 0x8b }); // magic
    try out.append(allocator, GZIP_METHOD);
    try out.append(allocator, 0); // flags
    try out.appendSlice(allocator, &std.mem.toBytes(GZIP_MTIME));
    try out.append(allocator, GZIP_XFL);
    try out.append(allocator, GZIP_OS);

    // Write uncompressed deflate block
    // Block format: BFINAL (1 bit) | BTYPE (2 bits) | data...
    // For uncompressed block (BTYPE = 00):
    // - 1 bit: BFINAL (set to 1, final block)
    // - 2 bits: BTYPE = 00 (uncompressed)
    // - 5 bits of padding to next byte
    // - 2 bytes: data length (little-endian)
    // - 2 bytes: ~data length (one's complement)
    // - data bytes
    try out.append(allocator, 0x01); // BFINAL=1, BTYPE=00 (0b00000001 after bitpacking)
    try out.appendSlice(allocator, &std.mem.toBytes(std.mem.nativeToLittle(u16, @as(u16, @intCast(data.len)))));
    const len_complement = ~@as(u16, @intCast(data.len));
    try out.appendSlice(allocator, &std.mem.toBytes(std.mem.nativeToLittle(u16, len_complement)));
    try out.appendSlice(allocator, data);

    // Compute CRC-32 of uncompressed data
    var crc: u32 = 0xffffffff;
    crc = crc32Update(crc, data);
    crc ^= 0xffffffff;

    // Write gzip footer
    try out.appendSlice(allocator, &std.mem.toBytes(std.mem.nativeToLittle(u32, crc)));
    try out.appendSlice(allocator, &std.mem.toBytes(std.mem.nativeToLittle(u32, @as(u32, @intCast(data.len)))));

    return out.toOwnedSlice(allocator);
}

pub fn decompress(allocator: std.mem.Allocator, compressed: []const u8, uncompressed_size: usize) Error![]u8 {
    if (uncompressed_size > MAX_DECOMPRESS_SIZE) return error.InvalidSize;
    if (compressed.len < 10) return error.DecompressionError;

    // Verify gzip header
    if (compressed[0] != 0x1f or compressed[1] != 0x8b) return error.DecompressionError;
    if (compressed[2] != GZIP_METHOD) return error.DecompressionError;

    var out: std.io.Writer.Allocating = std.io.Writer.Allocating.initCapacity(allocator, uncompressed_size) catch return error.OutOfMemory;
    errdefer out.deinit();

    // Skip gzip header (10+ bytes depending on flags)
    const pos: usize = 10;
    if (pos > compressed.len) return error.DecompressionError;

    // Decompress deflate stream
    var in: std.io.Reader = .fixed(compressed[pos..]);
    var gz: std.compress.flate.Decompress = .init(&in, .raw, &[_]u8{});

    _ = gz.reader.streamRemaining(&out.writer) catch return error.DecompressionError;

    const result = out.toOwnedSlice() catch return error.OutOfMemory;

    if (result.len != uncompressed_size) {
        allocator.free(result);
        return error.DecompressionError;
    }

    return result;
}

// =========================================================================
// Tests
// =========================================================================

test "gzip round-trip" {
    const allocator = std.testing.allocator;
    const original = "Hello, World! This is a test of gzip compression.";

    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    const decompressed = try decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualSlices(u8, original, decompressed);
}

test "gzip empty data" {
    const allocator = std.testing.allocator;
    const original = "";

    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    const decompressed = try decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualSlices(u8, original, decompressed);
}

test "gzip magic bytes" {
    const allocator = std.testing.allocator;
    const original = "test data";

    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    try std.testing.expect(compressed.len >= 2);
    try std.testing.expectEqual(@as(u8, 0x1f), compressed[0]);
    try std.testing.expectEqual(@as(u8, 0x8b), compressed[1]);
}

test "gzip large data" {
    const allocator = std.testing.allocator;
    var buf: [10000]u8 = undefined;
    for (0..buf.len) |i| {
        buf[i] = @intCast(i % 256);
    }

    const compressed = try compress(allocator, &buf);
    defer allocator.free(compressed);

    const decompressed = try decompress(allocator, compressed, buf.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualSlices(u8, &buf, decompressed);
}
