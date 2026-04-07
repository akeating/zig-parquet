//! Snappy compression/decompression for Parquet
//!
//! Both compression and decompression are pure Zig implementations
//! of the Snappy block format. No C/C++ dependencies.

const std = @import("std");
const safe = @import("../safe.zig");

pub const Error = error{
    CompressionError,
    DecompressionError,
    OutOfMemory,
    InvalidSize,
};

const BLOCK_SIZE: usize = 1 << 16; // 64KB, matching reference compressor
const HASH_LOG: u5 = 14;
const HASH_SIZE: usize = 1 << HASH_LOG;
const MIN_MATCH: usize = 4;
const HASH_MUL: u32 = 0x1e35a7bd;

fn maxCompressedLength(src_len: usize) usize {
    return 32 + src_len + src_len / 6;
}

/// Compress data using Snappy (pure Zig)
pub fn compress(allocator: std.mem.Allocator, data: []const u8) Error![]u8 {
    const max_out = maxCompressedLength(data.len);
    var dst = allocator.alloc(u8, max_out) catch return error.OutOfMemory;

    var pos: usize = writeVarint(dst, safe.castTo(u32, data.len) catch return error.CompressionError);

    if (data.len == 0) {
        const result = allocator.realloc(dst, pos) catch return dst[0..pos];
        return result;
    }

    const hash_table = allocator.alloc(u16, HASH_SIZE) catch {
        allocator.free(dst);
        return error.OutOfMemory;
    };
    defer allocator.free(hash_table);

    var block_start: usize = 0;
    while (block_start < data.len) {
        const block_end = @min(block_start + BLOCK_SIZE, data.len);
        pos += compressBlock(data[block_start..block_end], dst[pos..], hash_table);
        block_start = block_end;
    }

    const result = allocator.realloc(dst, pos) catch return dst[0..pos];
    return result;
}

/// Decompress Snappy-compressed data (pure Zig)
pub fn decompress(allocator: std.mem.Allocator, compressed: []const u8, uncompressed_size: usize) Error![]u8 {
    if (compressed.len == 0) return error.DecompressionError;

    const dst = allocator.alloc(u8, uncompressed_size) catch return error.OutOfMemory;
    errdefer allocator.free(dst);

    const written = snappyDecode(compressed, dst) catch return error.DecompressionError;

    if (written < uncompressed_size) {
        return allocator.realloc(dst, written) catch dst[0..written];
    }

    return dst;
}

/// Get the uncompressed length from snappy-compressed data (pure Zig)
pub fn getUncompressedLength(compressed: []const u8) ?usize {
    const varint = readVarint(compressed) catch return null;
    return safe.cast(varint.value) catch null;
}

// =========================================================================
// Internal: Snappy block format encoder
// =========================================================================

fn writeVarint(dst: []u8, value: u32) usize {
    var v = value;
    var i: usize = 0;
    while (v >= 0x80) {
        dst[i] = @as(u8, @truncate(v)) | 0x80;
        v >>= 7;
        i += 1;
    }
    dst[i] = @truncate(v);
    return i + 1;
}

fn hashBytes(data: [*]const u8) u32 {
    const val: u32 = std.mem.readInt(u32, data[0..4], .little);
    return (val *% HASH_MUL) >> @intCast(32 - @as(u32, HASH_LOG));
}

fn compressBlock(src: []const u8, dst: []u8, hash_table: []u16) usize {
    @memset(hash_table, 0);

    if (src.len < MIN_MATCH) {
        return emitLiteral(dst, 0, src);
    }

    var pos: usize = 0;
    var anchor: usize = 0;
    var ip: usize = 0;
    const ip_limit = src.len - MIN_MATCH;

    while (ip <= ip_limit) {
        const h = hashBytes(src.ptr + ip);
        const candidate: usize = hash_table[h];
        hash_table[h] = safe.castTo(u16, ip) catch unreachable; // ip < BLOCK_SIZE (64KB)

        if (candidate < ip and
            std.mem.readInt(u32, src[candidate..][0..4], .little) ==
            std.mem.readInt(u32, src[ip..][0..4], .little))
        {
            if (ip > anchor) {
                pos += emitLiteral(dst, pos, src[anchor..ip]);
            }

            var match_len: usize = 4;
            while (ip + match_len < src.len and
                candidate + match_len < ip and
                src[candidate + match_len] == src[ip + match_len])
            {
                match_len += 1;
            }

            pos += emitCopy(dst, pos, ip - candidate, match_len);
            ip += match_len;
            anchor = ip;
        } else {
            ip += 1;
        }
    }

    if (anchor < src.len) {
        pos += emitLiteral(dst, pos, src[anchor..]);
    }

    return pos;
}

fn emitLiteral(dst: []u8, pos: usize, literal: []const u8) usize {
    const n = literal.len;
    var p = pos;

    if (n <= 60) {
        dst[p] = safe.castTo(u8, (n - 1) << 2) catch unreachable; // max (59<<2) = 236
        p += 1;
    } else if (n <= 256) {
        dst[p] = 60 << 2;
        dst[p + 1] = safe.castTo(u8, n - 1) catch unreachable; // max 255
        p += 2;
    } else if (n <= 65536) {
        dst[p] = 61 << 2;
        std.mem.writeInt(u16, dst[p + 1 ..][0..2], safe.castTo(u16, n - 1) catch unreachable, .little);
        p += 3;
    } else if (n <= 16777216) {
        dst[p] = 62 << 2;
        const val = safe.castTo(u24, n - 1) catch unreachable;
        dst[p + 1] = @truncate(val);
        dst[p + 2] = @truncate(val >> 8);
        dst[p + 3] = @truncate(val >> 16);
        p += 4;
    } else {
        dst[p] = 63 << 2;
        std.mem.writeInt(u32, dst[p + 1 ..][0..4], safe.castTo(u32, n - 1) catch unreachable, .little);
        p += 5;
    }

    @memcpy(dst[p..][0..n], literal);
    return (p + n) - pos;
}

fn emitCopy(dst: []u8, pos: usize, offset: usize, length: usize) usize {
    var remaining = length;
    var p = pos;

    while (remaining >= 68) {
        p += emitCopyAtMost64(dst, p, offset, 64);
        remaining -= 64;
    }
    if (remaining > 64) {
        p += emitCopyAtMost64(dst, p, offset, 60);
        remaining -= 60;
    }
    p += emitCopyAtMost64(dst, p, offset, remaining);

    return p - pos;
}

fn emitCopyAtMost64(dst: []u8, pos: usize, offset: usize, length: usize) usize {
    if (length < 12 and offset < 2048) {
        // Copy-1: 2 bytes (tag + offset low byte)
        dst[pos] = safe.castTo(u8, ((length - 4) << 2) | ((offset >> 8) << 5) | 0x01) catch unreachable; // max 253
        dst[pos + 1] = @truncate(offset);
        return 2;
    } else {
        // Copy-2: 3 bytes (tag + 2-byte LE offset)
        dst[pos] = safe.castTo(u8, ((length - 1) << 2) | 0x02) catch unreachable; // max 254
        std.mem.writeInt(u16, dst[pos + 1 ..][0..2], safe.castTo(u16, offset) catch unreachable, .little);
        return 3;
    }
}

// =========================================================================
// Internal: Snappy block format decoder
// =========================================================================

const Varint = struct { value: u32, bytes_read: usize };

fn readVarint(data: []const u8) error{DecompressionError}!Varint {
    var result: u32 = 0;
    var shift: u32 = 0;
    for (0..5) |i| { // varint is at most 5 bytes for u32
        if (i >= data.len) return error.DecompressionError;
        const byte = data[i];
        result |= @as(u32, byte & 0x7F) << (safe.castTo(u5, shift) catch return error.DecompressionError); // shift is 0,7,14,21,28
        if (byte & 0x80 == 0) {
            return .{ .value = result, .bytes_read = i + 1 };
        }
        shift += 7;
    }
    return error.DecompressionError;
}

fn snappyDecode(src: []const u8, dst: []u8) error{DecompressionError}!usize {
    const varint = try readVarint(src);
    const expected_len = safe.cast(varint.value) catch return error.DecompressionError;
    if (expected_len > dst.len) return error.DecompressionError;

    var sp: usize = varint.bytes_read;
    var dp: usize = 0;

    while (sp < src.len and dp < expected_len) {
        const tag = src[sp];
        sp += 1;
        const tag_type: u2 = @truncate(tag);

        switch (tag_type) {
            0 => { // Literal
                const upper6: usize = tag >> 2;
                var lit_len: usize = undefined;

                if (upper6 < 60) {
                    lit_len = upper6 + 1;
                } else {
                    const extra: usize = upper6 - 59; // 1..4
                    if (sp + extra > src.len) return error.DecompressionError;
                    var raw: u32 = 0;
                    for (0..extra) |j| {
                        raw |= @as(u32, src[sp + j]) << (safe.castTo(u5, j * 8) catch unreachable); // j*8 is 0,8,16,24
                    }
                    sp += extra;
                    lit_len = (safe.cast(raw) catch return error.DecompressionError) + 1;
                }

                if (sp + lit_len > src.len) return error.DecompressionError;
                if (dp + lit_len > expected_len) return error.DecompressionError;
                @memcpy(dst[dp..][0..lit_len], src[sp..][0..lit_len]);
                sp += lit_len;
                dp += lit_len;
            },
            1 => { // Copy with 1-byte offset
                if (sp >= src.len) return error.DecompressionError;
                const length: usize = @as(usize, (tag >> 2) & 7) + 4;
                const offset: usize = (@as(usize, tag >> 5) << 8) | @as(usize, src[sp]);
                sp += 1;

                if (offset == 0 or offset > dp) return error.DecompressionError;
                if (dp + length > expected_len) return error.DecompressionError;
                copyAtOffset(dst, dp, offset, length);
                dp += length;
            },
            2 => { // Copy with 2-byte offset
                if (sp + 2 > src.len) return error.DecompressionError;
                const length: usize = @as(usize, tag >> 2) + 1;
                const offset: usize = std.mem.readInt(u16, src[sp..][0..2], .little);
                sp += 2;

                if (offset == 0 or offset > dp) return error.DecompressionError;
                if (dp + length > expected_len) return error.DecompressionError;
                copyAtOffset(dst, dp, offset, length);
                dp += length;
            },
            3 => { // Copy with 4-byte offset
                if (sp + 4 > src.len) return error.DecompressionError;
                const length: usize = @as(usize, tag >> 2) + 1;
                const offset: usize = std.mem.readInt(u32, src[sp..][0..4], .little);
                sp += 4;

                if (offset == 0 or offset > dp) return error.DecompressionError;
                if (dp + length > expected_len) return error.DecompressionError;
                copyAtOffset(dst, dp, offset, length);
                dp += length;
            },
        }
    }

    if (dp != expected_len) return error.DecompressionError;
    return dp;
}

/// Copy `length` bytes from `dst[pos - offset..]` to `dst[pos..]`.
/// Handles the overlapping case (length > offset) which acts as RLE.
fn copyAtOffset(dst: []u8, pos: usize, offset: usize, length: usize) void {
    const copy_src = pos - offset;
    if (offset >= length) {
        @memcpy(dst[pos..][0..length], dst[copy_src..][0..length]);
    } else {
        for (0..length) |i| {
            dst[pos + i] = dst[copy_src + i];
        }
    }
}

// =========================================================================
// Tests
// =========================================================================

test "snappy round-trip" {
    const allocator = std.testing.allocator;

    const original = "Hello, World! This is a test of snappy compression.";

    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    const decompressed = try decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

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

test "snappy round-trip repetitive data" {
    const allocator = std.testing.allocator;
    const original = "abcdefgh" ** 1000;

    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    try std.testing.expect(compressed.len < original.len);

    const decompressed = try decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualStrings(original, decompressed);
}

test "snappy getUncompressedLength" {
    const allocator = std.testing.allocator;
    const original = "test data for length check";

    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    const len = getUncompressedLength(compressed);
    try std.testing.expectEqual(@as(?usize, original.len), len);
}

test "snappy decompress ignores oversized size hint" {
    const allocator = std.testing.allocator;
    const original = "test";
    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);
    const result = try decompress(allocator, compressed, original.len + 10);
    defer allocator.free(result);
    try std.testing.expectEqualSlices(u8, original, result);
}

test "snappy decompress rejects empty compressed data" {
    const allocator = std.testing.allocator;

    const result = decompress(allocator, "", 100);
    try std.testing.expectError(error.DecompressionError, result);
}

test "snappy decompress rejects truncated input" {
    const allocator = std.testing.allocator;
    const original = "Hello, World! This is a test of snappy compression.";

    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    // Truncate to just the varint header
    const result = decompress(allocator, compressed[0..1], original.len);
    try std.testing.expectError(error.DecompressionError, result);
}

test "snappy readVarint" {
    // Single byte: 64 = 0x40
    const v1 = try readVarint(&.{0x40});
    try std.testing.expectEqual(@as(u32, 64), v1.value);
    try std.testing.expectEqual(@as(usize, 1), v1.bytes_read);

    // Multi-byte: 2097150 (0x1FFFFE) = 0xFE 0xFF 0x7F
    const v2 = try readVarint(&.{ 0xFE, 0xFF, 0x7F });
    try std.testing.expectEqual(@as(u32, 2097150), v2.value);
    try std.testing.expectEqual(@as(usize, 3), v2.bytes_read);

    // Zero
    const v3 = try readVarint(&.{0x00});
    try std.testing.expectEqual(@as(u32, 0), v3.value);

    // Truncated varint (continuation bit set, no more bytes)
    try std.testing.expectError(error.DecompressionError, readVarint(&.{0x80}));

    // Empty input
    try std.testing.expectError(error.DecompressionError, readVarint(&.{}));
}

test "snappy decode literal-only stream" {
    // Hand-crafted: varint=5, then literal tag for 5 bytes "hello"
    // Tag byte: (4 << 2) | 0 = 0x10 (literal, len-1=4, so len=5)
    const stream = [_]u8{ 0x05, 0x10, 'h', 'e', 'l', 'l', 'o' };
    var dst: [5]u8 = undefined;

    const written = try snappyDecode(&stream, &dst);
    try std.testing.expectEqual(@as(usize, 5), written);
    try std.testing.expectEqualStrings("hello", &dst);
}

test "snappy round-trip incompressible data" {
    const allocator = std.testing.allocator;

    var rng = std.Random.DefaultPrng.init(0xdeadbeef);
    var random_data: [4096]u8 = undefined;
    rng.fill(&random_data);

    const compressed = try compress(allocator, &random_data);
    defer allocator.free(compressed);

    const decompressed = try decompress(allocator, compressed, random_data.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualSlices(u8, &random_data, decompressed);
}

test "snappy round-trip multi-block data" {
    const allocator = std.testing.allocator;

    // 200KB of repetitive data spans multiple 64KB blocks
    const original = try allocator.alloc(u8, 200 * 1024);
    defer allocator.free(original);
    for (0..original.len) |i| {
        original[i] = @truncate(i % 251);
    }

    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    try std.testing.expect(compressed.len < original.len);

    const decompressed = try decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualSlices(u8, original, decompressed);
}

test "snappy writeVarint round-trip with readVarint" {
    var buf: [5]u8 = undefined;

    const cases = [_]u32{ 0, 1, 127, 128, 16383, 16384, 2097151, 0xFFFFFFFF };
    for (cases) |val| {
        const n = writeVarint(&buf, val);
        const result = try readVarint(buf[0..n]);
        try std.testing.expectEqual(val, result.value);
        try std.testing.expectEqual(n, result.bytes_read);
    }
}

test "snappy emitLiteral and decode literal" {
    var dst: [300]u8 = undefined;
    const literal = "abcdefghijklmnopqrstuvwxyz";

    const n = emitLiteral(&dst, 0, literal);

    // Prepend varint length header to make a valid snappy stream
    var stream: [310]u8 = undefined;
    const hdr = writeVarint(&stream, @intCast(literal.len));
    @memcpy(stream[hdr..][0..n], dst[0..n]);

    var decoded: [26]u8 = undefined;
    const written = try snappyDecode(stream[0 .. hdr + n], &decoded);
    try std.testing.expectEqual(@as(usize, literal.len), written);
    try std.testing.expectEqualStrings(literal, &decoded);
}

test "snappy varint round-trip at u32 max" {
    var buf: [5]u8 = undefined;
    const n = writeVarint(&buf, std.math.maxInt(u32));
    const result = try readVarint(buf[0..n]);
    try std.testing.expectEqual(std.math.maxInt(u32), result.value);
}
