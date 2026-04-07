//! LZ4 block compression/decompression for Parquet (pure Zig)
//!
//! Implements the LZ4 raw block format used by Parquet's LZ4_RAW codec.
//! No framing layer — just raw LZ4 block encoding/decoding.
//! Reference: https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md

const std = @import("std");

pub const Error = error{
    CompressionError,
    DecompressionError,
    OutOfMemory,
    InvalidSize,
};

const MINMATCH: usize = 4;
const MFLIMIT: usize = 12;
const LASTLITERALS: usize = 5;
const MAX_DISTANCE: usize = 65535;
const ML_BITS: u4 = 4;
const ML_MASK: u8 = 0x0F;
const HASH_LOG: u5 = 12;
const HASH_SIZE: usize = 1 << HASH_LOG;

// =========================================================================
// Public API
// =========================================================================

/// Compress data using LZ4 raw block format (pure Zig)
pub fn compress(allocator: std.mem.Allocator, data: []const u8) Error![]u8 {
    // LZ4 worst case: inputSize + (inputSize / 255) + 16
    const max_out = data.len + (data.len / 255) + 16;
    var dst = allocator.alloc(u8, if (max_out < 1) 1 else max_out) catch return error.OutOfMemory;
    errdefer allocator.free(dst);

    const written = lz4CompressBlock(data, dst);

    const result = allocator.realloc(dst, written) catch return dst[0..written];
    return result;
}

/// Decompress LZ4 raw block format (pure Zig)
pub fn decompress(allocator: std.mem.Allocator, compressed: []const u8, uncompressed_size: usize) Error![]u8 {

    if (uncompressed_size == 0) {
        return allocator.alloc(u8, 0) catch return error.OutOfMemory;
    }

    const dst = allocator.alloc(u8, uncompressed_size) catch return error.OutOfMemory;
    errdefer allocator.free(dst);

    const written = lz4DecompressBlock(compressed, dst) catch return error.DecompressionError;

    if (written < uncompressed_size) {
        return allocator.realloc(dst, written) catch dst[0..written];
    }

    return dst;
}

// =========================================================================
// Internal: LZ4 block format decoder
// =========================================================================

fn lz4DecompressBlock(src: []const u8, dst: []u8) error{DecompressionError}!usize {
    var sp: usize = 0;
    var dp: usize = 0;

    while (sp < src.len) {
        // Read token
        const token = src[sp];
        sp += 1;

        // Decode literal length
        var lit_len: usize = token >> ML_BITS;
        if (lit_len == 15) {
            while (true) {
                if (sp >= src.len) return error.DecompressionError;
                const extra = src[sp];
                sp += 1;
                lit_len += extra;
                if (extra != 255) break;
            }
        }

        // Copy literals
        if (lit_len > 0) {
            if (sp + lit_len > src.len) return error.DecompressionError;
            if (dp + lit_len > dst.len) return error.DecompressionError;
            @memcpy(dst[dp..][0..lit_len], src[sp..][0..lit_len]);
            sp += lit_len;
            dp += lit_len;
        }

        // Last sequence has no match
        if (sp >= src.len) break;

        // Read offset (2 bytes, little-endian)
        if (sp + 2 > src.len) return error.DecompressionError;
        const offset: usize = @as(usize, src[sp]) | (@as(usize, src[sp + 1]) << 8);
        sp += 2;

        if (offset == 0) return error.DecompressionError;
        if (offset > dp) return error.DecompressionError;

        // Decode match length
        var match_len: usize = (token & ML_MASK) + MINMATCH;
        if ((token & ML_MASK) == ML_MASK) {
            while (true) {
                if (sp >= src.len) return error.DecompressionError;
                const extra = src[sp];
                sp += 1;
                match_len += extra;
                if (extra != 255) break;
            }
        }

        // Copy match (handle overlapping copies)
        if (dp + match_len > dst.len) return error.DecompressionError;
        const match_pos = dp - offset;
        if (offset >= match_len) {
            @memcpy(dst[dp..][0..match_len], dst[match_pos..][0..match_len]);
        } else {
            for (0..match_len) |i| {
                dst[dp + i] = dst[match_pos + i];
            }
        }
        dp += match_len;
    }

    return dp;
}

// =========================================================================
// Internal: LZ4 block format encoder
// =========================================================================

fn lz4CompressBlock(src: []const u8, dst: []u8) usize {
    if (src.len == 0) {
        // Empty block: single zero token byte (per LZ4 block format spec)
        dst[0] = 0;
        return 1;
    }

    // Too small to contain a match — emit as literal-only
    if (src.len < MFLIMIT) {
        return writeLastLiterals(dst, 0, src[0..]);
    }

    var hash_table: [HASH_SIZE]u32 = [_]u32{0} ** HASH_SIZE;
    var dp: usize = 0;
    var anchor: usize = 0;
    var ip: usize = 1; // skip first byte

    const match_limit = src.len - MFLIMIT;

    while (ip < match_limit) {
        const h = hashU32(readU32(src, ip));
        const candidate = hash_table[h];
        hash_table[h] = @intCast(ip);

        if (candidate < ip and
            ip - candidate <= MAX_DISTANCE and
            readU32(src, candidate) == readU32(src, ip))
        {
            // Found a match — extend it
            var match_len: usize = MINMATCH;
            while (ip + match_len < src.len and
                src[candidate + match_len] == src[ip + match_len])
            {
                match_len += 1;
            }

            // Clamp: last match must end at least LASTLITERALS before end
            if (ip + match_len > src.len - LASTLITERALS) {
                match_len = src.len - LASTLITERALS - ip;
                if (match_len < MINMATCH) {
                    ip += 1;
                    continue;
                }
            }

            const lit_len = ip - anchor;
            const offset = ip - candidate;

            dp = writeSequence(dst, dp, src[anchor..][0..lit_len], offset, match_len);

            ip += match_len;
            anchor = ip;
        } else {
            ip += 1;
        }
    }

    // Emit remaining literals
    dp = writeLastLiterals(dst, dp, src[anchor..]);
    return dp;
}

/// Write a full LZ4 sequence: token + lit_length_ext + literals + offset + match_length_ext
fn writeSequence(dst: []u8, dp_in: usize, literals: []const u8, offset: usize, match_len: usize) usize {
    var dp = dp_in;
    const lit_len = literals.len;
    const ml_code = match_len - MINMATCH;

    // Token byte
    const lit_nibble: u8 = if (lit_len >= 15) 15 else @intCast(lit_len);
    const ml_nibble: u8 = if (ml_code >= 15) 15 else @intCast(ml_code);
    dst[dp] = (lit_nibble << ML_BITS) | ml_nibble;
    dp += 1;

    // Extended literal length
    if (lit_len >= 15) {
        var remaining = lit_len - 15;
        while (remaining >= 255) {
            dst[dp] = 255;
            dp += 1;
            remaining -= 255;
        }
        dst[dp] = @intCast(remaining);
        dp += 1;
    }

    // Literal bytes
    @memcpy(dst[dp..][0..lit_len], literals);
    dp += lit_len;

    // Offset (2 bytes, little-endian)
    dst[dp] = @truncate(offset);
    dst[dp + 1] = @truncate(offset >> 8);
    dp += 2;

    // Extended match length
    if (ml_code >= 15) {
        var remaining = ml_code - 15;
        while (remaining >= 255) {
            dst[dp] = 255;
            dp += 1;
            remaining -= 255;
        }
        dst[dp] = @intCast(remaining);
        dp += 1;
    }

    return dp;
}

/// Write the final literal-only sequence (no match, no offset)
fn writeLastLiterals(dst: []u8, dp_in: usize, literals: []const u8) usize {
    var dp = dp_in;
    const lit_len = literals.len;

    // Token: literal length in high nibble, 0 in low nibble
    if (lit_len >= 15) {
        dst[dp] = 15 << ML_BITS;
        dp += 1;
        var remaining = lit_len - 15;
        while (remaining >= 255) {
            dst[dp] = 255;
            dp += 1;
            remaining -= 255;
        }
        dst[dp] = @intCast(remaining);
        dp += 1;
    } else {
        dst[dp] = @as(u8, @intCast(lit_len)) << ML_BITS;
        dp += 1;
    }

    // Copy literal bytes
    @memcpy(dst[dp..][0..lit_len], literals);
    dp += lit_len;

    return dp;
}

fn hashU32(val: u32) usize {
    return @intCast((val *% 2654435761) >> (32 - @as(u32, HASH_LOG)));
}

fn readU32(src: []const u8, pos: usize) u32 {
    return std.mem.readInt(u32, src[pos..][0..4], .little);
}

// =========================================================================
// Tests
// =========================================================================

test "lz4 round-trip" {
    const allocator = std.testing.allocator;
    const original = "Hello, World! This is a test of LZ4 compression.";

    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    const decompressed = try decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualStrings(original, decompressed);
}

test "lz4 empty data" {
    const allocator = std.testing.allocator;

    const compressed = try compress(allocator, "");
    defer allocator.free(compressed);

    const decompressed = try decompress(allocator, compressed, 0);
    defer allocator.free(decompressed);
    try std.testing.expectEqual(@as(usize, 0), decompressed.len);
}

test "lz4 round-trip repetitive data" {
    const allocator = std.testing.allocator;
    const original = "abcdefgh" ** 1000;

    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);

    try std.testing.expect(compressed.len < original.len);

    const decompressed = try decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualStrings(original, decompressed);
}

test "lz4 round-trip incompressible data" {
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

test "lz4 decompress ignores oversized size hint" {
    const allocator = std.testing.allocator;
    const original = "hello world hello world hello world";
    const compressed = try compress(allocator, original);
    defer allocator.free(compressed);
    const result = try decompress(allocator, compressed, original.len + 100);
    defer allocator.free(result);
    try std.testing.expectEqualSlices(u8, original, result);
}

test "lz4 small inputs" {
    const allocator = std.testing.allocator;
    const cases = [_][]const u8{ "X", "AB", "ABC", "ABCD", "Hello" };

    for (cases) |original| {
        const compressed = try compress(allocator, original);
        defer allocator.free(compressed);
        const decompressed = try decompress(allocator, compressed, original.len);
        defer allocator.free(decompressed);
        try std.testing.expectEqualSlices(u8, original, decompressed);
    }
}

test "lz4 large data" {
    const allocator = std.testing.allocator;
    const data = try allocator.alloc(u8, 200 * 1024);
    defer allocator.free(data);
    for (data, 0..) |*b, i| b.* = @truncate(i % 251);

    const compressed = try compress(allocator, data);
    defer allocator.free(compressed);

    try std.testing.expect(compressed.len < data.len);

    const decompressed = try decompress(allocator, compressed, data.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualSlices(u8, data, decompressed);
}
