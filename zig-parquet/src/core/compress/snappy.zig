//! Snappy compression/decompression for Parquet
//!
//! Both compression and decompression are pure Zig implementations
//! of the Snappy block format. No C/C++ dependencies.

const std = @import("std");
const test_util = @import("../test_util.zig");
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
        return allocator.realloc(dst, written) catch return error.OutOfMemory;
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
    const out = dst[0..expected_len];

    // Fast loop: while the input has room for the longest element header (5 bytes) and a
    // 16-byte literal block after it, and the output has room for the block writes of any
    // short literal or copy (`copy_slack`), elements are decoded without checks on those
    // ends. The loop below decodes the rest.
    while (src.len - sp >= 21 and expected_len - dp >= copy_slack) {
        const tag = src[sp];
        sp += 1;
        // A branch per element type: the input position after the element is then known
        // without waiting for loads, so the CPU can start on the next element.
        switch (@as(u2, @truncate(tag))) {
            0 => {
                const upper6: usize = tag >> 2;
                var lit_len = upper6 + 1;
                if (upper6 >= 60) {
                    const extra = upper6 - 59; // 1..4
                    const raw = std.mem.readInt(u32, src[sp..][0..4], .little) & trailer_mask[extra];
                    sp += extra;
                    // On 32-bit targets a 4-byte length of 0xffffffff would overflow usize.
                    lit_len = std.math.add(usize, raw, 1) catch return error.DecompressionError;
                }
                if (lit_len <= 16) {
                    // 21 input bytes were left before the header of at most 5 bytes, and the
                    // output has `copy_slack` bytes; the bytes past `lit_len` are overwritten
                    // by what follows.
                    out[dp..][0..16].* = src[sp..][0..16].*;
                } else {
                    if (src.len - sp < lit_len or expected_len - dp < lit_len) return error.DecompressionError;
                    @memcpy(out[dp..][0..lit_len], src[sp..][0..lit_len]);
                }
                sp += lit_len;
                dp += lit_len;
            },
            1 => {
                const length = @as(usize, (tag >> 2) & 7) + 4;
                const offset = (@as(usize, tag >> 5) << 8) | @as(usize, src[sp]);
                sp += 1;
                if (offset == 0 or offset > dp) return error.DecompressionError;
                // length <= 64 = copy_slack, so the copy and its block writes stay in `out`.
                copyBlocks(out, dp, offset, length);
                dp += length;
            },
            2 => {
                const length = @as(usize, tag >> 2) + 1;
                const offset: usize = std.mem.readInt(u16, src[sp..][0..2], .little);
                sp += 2;
                if (offset == 0 or offset > dp) return error.DecompressionError;
                copyBlocks(out, dp, offset, length);
                dp += length;
            },
            3 => {
                const length = @as(usize, tag >> 2) + 1;
                const offset: usize = std.mem.readInt(u32, src[sp..][0..4], .little);
                sp += 4;
                if (offset == 0 or offset > dp) return error.DecompressionError;
                copyBlocks(out, dp, offset, length);
                dp += length;
            },
        }
    }

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
                    lit_len = std.math.add(usize, safe.cast(raw) catch return error.DecompressionError, 1) catch return error.DecompressionError;
                }

                if (lit_len > src.len - sp) return error.DecompressionError;
                if (lit_len > expected_len - dp) return error.DecompressionError;
                if (lit_len <= 16 and src.len - sp >= 16 and expected_len - dp >= 16) {
                    // Copy 16 bytes; the bytes past `lit_len` are overwritten by what follows.
                    dst[dp..][0..16].* = src[sp..][0..16].*;
                } else {
                    @memcpy(dst[dp..][0..lit_len], src[sp..][0..lit_len]);
                }
                sp += lit_len;
                dp += lit_len;
            },
            else => |kind| { // Copy: decode length and offset, then one shared copy
                var length: usize = undefined;
                var offset: usize = undefined;
                switch (kind) {
                    1 => { // 1-byte offset
                        if (sp >= src.len) return error.DecompressionError;
                        length = @as(usize, (tag >> 2) & 7) + 4;
                        offset = (@as(usize, tag >> 5) << 8) | @as(usize, src[sp]);
                        sp += 1;
                    },
                    2 => { // 2-byte offset
                        if (sp + 2 > src.len) return error.DecompressionError;
                        length = @as(usize, tag >> 2) + 1;
                        offset = std.mem.readInt(u16, src[sp..][0..2], .little);
                        sp += 2;
                    },
                    else => { // 4-byte offset
                        if (sp + 4 > src.len) return error.DecompressionError;
                        length = @as(usize, tag >> 2) + 1;
                        offset = std.mem.readInt(u32, src[sp..][0..4], .little);
                        sp += 4;
                    },
                }

                if (offset == 0 or offset > dp) return error.DecompressionError;
                if (dp + length > expected_len) return error.DecompressionError;
                copyAtOffset(dst[0..expected_len], dp, offset, length);
                dp += length;
            },
        }
    }

    if (dp != expected_len) return error.DecompressionError;
    return dp;
}

/// The mask of the first `extra` bytes of a little-endian u32.
const trailer_mask = [5]u32{ 0, 0xff, 0xffff, 0xffffff, 0xffffffff };

/// Bytes that must remain in `dst` after the start of a copy for `copyAtOffset` to use block
/// copies: blocks cover the copy's at most 64 bytes and write past its end.
const copy_slack = 64;

/// Copy `length` bytes from `dst[pos - offset..]` to `dst[pos..]`. The source and destination
/// overlap when `length > offset`; the result repeats the `offset` bytes before `pos` (RLE).
/// The caller checks that `offset <= pos`, `pos + length <= dst.len`, and `length <= 64`
/// (the largest snappy copy element).
inline fn copyAtOffset(dst: []u8, pos: usize, offset: usize, length: usize) void {
    if (dst.len - pos < copy_slack) {
        // Near the end of the output: byte by byte, writing nothing past `pos + length`.
        const from = pos - offset;
        for (0..length) |i| dst[pos + i] = dst[from + i];
        return;
    }
    copyBlocks(dst, pos, offset, length);
}

/// `copyAtOffset` when `dst` has at least `copy_slack` bytes from `pos` on.
inline fn copyBlocks(dst: []u8, pos: usize, offset: usize, length: usize) void {
    const from = pos - offset;
    // Block copies of a fixed size where possible, so the work does not branch on the exact
    // length. Each block reads bytes before the position it writes (blocks are at most `offset`
    // bytes, or the source repeats with a period that is), so it reads output already written.
    // Bytes written past `pos + length` are overwritten by later elements.
    const p = dst.ptr;
    if (offset >= 16) {
        // Copies of 9-32 bytes at offsets of 64 or more dominate text and string data.
        if (length <= 32) {
            inline for (0..2) |k| p[pos + k * 16 ..][0..16].* = p[from + k * 16 ..][0..16].*;
        } else {
            inline for (0..4) |k| p[pos + k * 16 ..][0..16].* = p[from + k * 16 ..][0..16].*;
        }
    } else if (offset >= 8) {
        // Copies of 4-8 bytes at offsets 8-15 dominate fixed-width integer data.
        if (length <= 8) {
            p[pos..][0..8].* = p[from..][0..8].*;
        } else {
            var i: usize = 0;
            while (i < length) : (i += 8) p[pos + i ..][0..8].* = p[from + i ..][0..8].*;
        }
    } else {
        // Write the first 8 bytes one at a time. From there on the output repeats with period
        // `offset`, so `step`, the smallest multiple of `offset` that is at least 8, works as
        // the offset of 8-byte blocks.
        for (0..8) |i| p[pos + i] = p[from + i];
        const step = offset * ((8 + offset - 1) / offset);
        var i: usize = 8;
        while (i < length) : (i += 8) p[pos + i ..][0..8].* = p[pos + i - step ..][0..8].*;
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
    const original = test_util.repeatString(1000, "abcdefgh");

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

/// Builds a snappy stream of random literals and copies with offsets 1..max_offset and lengths
/// 4..max_length, and the output it decodes to (computed byte by byte).
fn testStream(allocator: std.mem.Allocator, seed: u64, ops: usize, max_offset: usize, max_length: usize) !struct { stream: []u8, expected: []u8 } {
    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();
    var expected: std.ArrayList(u8) = .empty;
    errdefer expected.deinit(allocator);
    var body: std.ArrayList(u8) = .empty;
    defer body.deinit(allocator);
    var scratch: [64 * 1024]u8 = undefined;

    for (0..ops) |op| {
        if (op == 0 or random.uintLessThan(u8, 4) == 0) {
            const len = random.intRangeAtMost(usize, 1, 100);
            const lit = scratch[0..len];
            random.bytes(lit);
            var tmp: [128]u8 = undefined;
            const n = emitLiteral(&tmp, 0, lit);
            try body.appendSlice(allocator, tmp[0..n]);
            try expected.appendSlice(allocator, lit);
        } else {
            const offset = random.intRangeAtMost(usize, 1, @min(max_offset, expected.items.len));
            const length = random.intRangeAtMost(usize, 4, max_length);
            var tmp: [64]u8 = undefined;
            const n = emitCopy(&tmp, 0, offset, length);
            try body.appendSlice(allocator, tmp[0..n]);
            const start = expected.items.len - offset;
            for (0..length) |i| try expected.append(allocator, expected.items[start + i]);
        }
    }
    var header: [5]u8 = undefined;
    const hn = writeVarint(&header, safe.castTo(u32, expected.items.len) catch unreachable); // test sizes are far below 4 GiB
    const stream = try std.mem.concat(allocator, u8, &.{ header[0..hn], body.items });
    return .{ .stream = stream, .expected = try expected.toOwnedSlice(allocator) };
}

test "snappy decode: copies at offsets 1-20 (overlapping) and up to 70 bytes" {
    const allocator = std.testing.allocator;
    for (0..50) |seed| {
        const t = try testStream(allocator, seed, 300, 20, 70);
        defer allocator.free(t.stream);
        defer allocator.free(t.expected);
        const out = try decompress(allocator, t.stream, t.expected.len);
        defer allocator.free(out);
        try std.testing.expectEqualSlices(u8, t.expected, out);
    }
}

test "snappy decode: copies at offsets up to 60000 and up to 200 bytes" {
    const allocator = std.testing.allocator;
    for (0..20) |seed| {
        const t = try testStream(allocator, 1000 + seed, 600, 60000, 200);
        defer allocator.free(t.stream);
        defer allocator.free(t.expected);
        const out = try decompress(allocator, t.stream, t.expected.len);
        defer allocator.free(out);
        try std.testing.expectEqualSlices(u8, t.expected, out);
    }
}

test "snappy decode: truncated and corrupted streams return errors without out-of-bounds access" {
    // In Debug builds an out-of-bounds slice access panics, which fails the test.
    const allocator = std.testing.allocator;
    const t = try testStream(allocator, 7, 80, 20, 70);
    defer allocator.free(t.stream);
    defer allocator.free(t.expected);

    for (0..t.stream.len) |len| {
        if (decompress(allocator, t.stream[0..len], t.expected.len)) |out| allocator.free(out) else |_| {}
    }

    const corrupt = try allocator.dupe(u8, t.stream);
    defer allocator.free(corrupt);
    var prng = std.Random.DefaultPrng.init(99);
    const random = prng.random();
    for (0..2000) |_| {
        @memcpy(corrupt, t.stream);
        const pos = random.uintLessThan(usize, corrupt.len);
        corrupt[pos] = random.int(u8);
        if (decompress(allocator, corrupt, t.expected.len)) |out| allocator.free(out) else |_| {}
    }
}
