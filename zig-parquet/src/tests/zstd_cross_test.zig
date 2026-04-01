//! Cross-implementation zstd tests
//!
//! Validates interoperability between C libzstd and pure Zig zstd implementations.
//! Cross-impl tests run when both are compiled in: -Dcodecs=zstd,zig-zstd
//! Edge-case tests always run (Zig-only round-trips) with cross-validation when available.

const std = @import("std");
const build_options = @import("build_options");
const c_zstd = @import("../core/compress/zstd.zig");
const zig_zstd = @import("../core/compress/zig_zstd.zig");

const both_enabled = build_options.enable_zstd and build_options.enable_zig_zstd;

// =========================================================================
// Helpers
// =========================================================================

fn zigRoundTrip(allocator: std.mem.Allocator, data: []const u8) !void {
    if (!build_options.enable_zig_zstd) return;
    const compressed = try zig_zstd.compress(allocator, data);
    defer allocator.free(compressed);

    const decompressed = try zig_zstd.decompress(allocator, compressed, data.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualSlices(u8, data, decompressed);
}

fn crossRoundTrip(allocator: std.mem.Allocator, data: []const u8) !void {
    if (!build_options.enable_zig_zstd) return;
    const zig_compressed = try zig_zstd.compress(allocator, data);
    defer allocator.free(zig_compressed);

    if (both_enabled) {
        const zc = try c_zstd.decompress(allocator, zig_compressed, data.len);
        defer allocator.free(zc);
        try std.testing.expectEqualSlices(u8, data, zc);
    }

    const decompressed = try zig_zstd.decompress(allocator, zig_compressed, data.len);
    defer allocator.free(decompressed);
    try std.testing.expectEqualSlices(u8, data, decompressed);
}

// =========================================================================
// Cross-implementation tests (require both C and Zig zstd)
// =========================================================================

test "cross-impl: C compress, Zig decompress" {
    if (!both_enabled) return;
    const allocator = std.testing.allocator;

    const original = "Hello, World! This is a cross-implementation zstd test." ** 20;

    const compressed = try c_zstd.compress(allocator, original);
    defer allocator.free(compressed);

    const decompressed = try zig_zstd.decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualStrings(original, decompressed);
}

test "cross-impl: Zig compress, C decompress" {
    if (!both_enabled) return;
    const allocator = std.testing.allocator;

    const original = "Hello, World! This is a cross-implementation zstd test." ** 20;

    const compressed = try zig_zstd.compress(allocator, original);
    defer allocator.free(compressed);

    const decompressed = try c_zstd.decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualStrings(original, decompressed);
}

test "cross-impl: bidirectional round-trip (all 4 combinations)" {
    if (!both_enabled) return;
    const allocator = std.testing.allocator;

    const original = "ABCDEFGH" ** 200;

    const c_compressed = try c_zstd.compress(allocator, original);
    defer allocator.free(c_compressed);
    const zig_compressed = try zig_zstd.compress(allocator, original);
    defer allocator.free(zig_compressed);

    const cc = try c_zstd.decompress(allocator, c_compressed, original.len);
    defer allocator.free(cc);
    try std.testing.expectEqualStrings(original, cc);

    const cz = try zig_zstd.decompress(allocator, c_compressed, original.len);
    defer allocator.free(cz);
    try std.testing.expectEqualStrings(original, cz);

    const zc = try c_zstd.decompress(allocator, zig_compressed, original.len);
    defer allocator.free(zc);
    try std.testing.expectEqualStrings(original, zc);

    const zz = try zig_zstd.decompress(allocator, zig_compressed, original.len);
    defer allocator.free(zz);
    try std.testing.expectEqualStrings(original, zz);
}

test "cross-impl: empty data" {
    if (!both_enabled) return;
    const allocator = std.testing.allocator;

    const original = "";

    const c_compressed = try c_zstd.compress(allocator, original);
    defer allocator.free(c_compressed);
    const cz = try zig_zstd.decompress(allocator, c_compressed, 0);
    defer allocator.free(cz);
    try std.testing.expectEqual(@as(usize, 0), cz.len);

    const zig_compressed = try zig_zstd.compress(allocator, original);
    defer allocator.free(zig_compressed);
    const zc = try c_zstd.decompress(allocator, zig_compressed, 0);
    defer allocator.free(zc);
    try std.testing.expectEqual(@as(usize, 0), zc.len);
}

test "cross-impl: repetitive columnar data" {
    if (!both_enabled) return;
    const allocator = std.testing.allocator;

    var data: [8000]u8 = undefined;
    for (0..1000) |i| {
        @memcpy(data[i * 8 ..][0..8], "column__");
    }

    const c_compressed = try c_zstd.compress(allocator, &data);
    defer allocator.free(c_compressed);
    const zig_compressed = try zig_zstd.compress(allocator, &data);
    defer allocator.free(zig_compressed);

    const cz = try zig_zstd.decompress(allocator, c_compressed, data.len);
    defer allocator.free(cz);
    try std.testing.expectEqualSlices(u8, &data, cz);

    const zc = try c_zstd.decompress(allocator, zig_compressed, data.len);
    defer allocator.free(zc);
    try std.testing.expectEqualSlices(u8, &data, zc);

    try std.testing.expect(c_compressed.len < data.len);
    try std.testing.expect(zig_compressed.len < data.len);
}

test "cross-impl: large multi-block data (> 128KB)" {
    if (!both_enabled) return;
    const allocator = std.testing.allocator;

    const size = 256 * 1024;
    const data = try allocator.alloc(u8, size);
    defer allocator.free(data);
    for (data, 0..) |*b, i| b.* = @truncate(i *% 31 +% 17);

    const c_compressed = try c_zstd.compress(allocator, data);
    defer allocator.free(c_compressed);
    const zig_compressed = try zig_zstd.compress(allocator, data);
    defer allocator.free(zig_compressed);

    const cz = try zig_zstd.decompress(allocator, c_compressed, size);
    defer allocator.free(cz);
    try std.testing.expectEqualSlices(u8, data, cz);

    const zc = try c_zstd.decompress(allocator, zig_compressed, size);
    defer allocator.free(zc);
    try std.testing.expectEqualSlices(u8, data, zc);
}

test "cross-impl: C level 1 compress, Zig decompress" {
    if (!both_enabled) return;
    const allocator = std.testing.allocator;

    const original = "Level 1 is the apples-to-apples comparison with our Zig compressor. " ** 30;

    const compressed = try c_zstd.compressWithLevel(allocator, original, 1);
    defer allocator.free(compressed);

    const decompressed = try zig_zstd.decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualStrings(original, decompressed);
}

test "cross-impl: C multi-level compress, Zig decompress" {
    if (!both_enabled) return;
    const allocator = std.testing.allocator;

    const original = "Testing across compression levels to validate Zig decompressor handles varying frame complexity. " ** 50;

    for ([_]c_int{ 1, 3, 6, 9, 15, 19 }) |level| {
        const compressed = try c_zstd.compressWithLevel(allocator, original, level);
        defer allocator.free(compressed);

        const decompressed = try zig_zstd.decompress(allocator, compressed, original.len);
        defer allocator.free(decompressed);

        try std.testing.expectEqualStrings(original, decompressed);
    }
}

test "cross-impl: Zig compress, C decompress (varied patterns)" {
    if (!both_enabled) return;
    const allocator = std.testing.allocator;

    const patterns = [_][]const u8{
        "short",
        "a]" ** 500,
        "abcdefghijklmnop" ** 200,
        "The quick brown fox jumps over the lazy dog. " ** 100,
    };

    for (patterns) |original| {
        const compressed = try zig_zstd.compress(allocator, original);
        defer allocator.free(compressed);

        const decompressed = try c_zstd.decompress(allocator, compressed, original.len);
        defer allocator.free(decompressed);

        try std.testing.expectEqualStrings(original, decompressed);
    }
}

test "cross-impl: random incompressible data" {
    if (!both_enabled) return;
    const allocator = std.testing.allocator;

    var prng = std.Random.DefaultPrng.init(0xdeadbeef);
    const random = prng.random();
    const size = 4096;
    const data = try allocator.alloc(u8, size);
    defer allocator.free(data);
    random.bytes(data);

    const c_compressed = try c_zstd.compress(allocator, data);
    defer allocator.free(c_compressed);
    const zig_compressed = try zig_zstd.compress(allocator, data);
    defer allocator.free(zig_compressed);

    const cz = try zig_zstd.decompress(allocator, c_compressed, size);
    defer allocator.free(cz);
    try std.testing.expectEqualSlices(u8, data, cz);

    const zc = try c_zstd.decompress(allocator, zig_compressed, size);
    defer allocator.free(zc);
    try std.testing.expectEqualSlices(u8, data, zc);
}

// =========================================================================
// Edge cases: block boundaries
// =========================================================================

test "edge: block boundary - exactly BLOCK_SIZE_MAX (128KB)" {
    const allocator = std.testing.allocator;
    const data = try allocator.alloc(u8, 1 << 17);
    defer allocator.free(data);
    for (data, 0..) |*b, i| b.* = @truncate(i *% 7 +% 13);
    try crossRoundTrip(allocator, data);
}

test "edge: block boundary - BLOCK_SIZE_MAX + 1" {
    const allocator = std.testing.allocator;
    const data = try allocator.alloc(u8, (1 << 17) + 1);
    defer allocator.free(data);
    for (data, 0..) |*b, i| b.* = @truncate(i *% 7 +% 13);
    try crossRoundTrip(allocator, data);
}

test "edge: block boundary - exactly 2 blocks" {
    const allocator = std.testing.allocator;
    const data = try allocator.alloc(u8, 2 * (1 << 17));
    defer allocator.free(data);
    for (data, 0..) |*b, i| b.* = @truncate(i *% 11 +% 3);
    try crossRoundTrip(allocator, data);
}

// =========================================================================
// Edge cases: match length extremes
// =========================================================================

test "edge: single byte repeated 100KB (long RLE match)" {
    const allocator = std.testing.allocator;
    const data = try allocator.alloc(u8, 100_000);
    defer allocator.free(data);
    @memset(data, 'A');
    try crossRoundTrip(allocator, data);
}

test "edge: minimum-length matches only (4 bytes each)" {
    const allocator = std.testing.allocator;
    var data: [4000]u8 = undefined;
    var prng = std.Random.DefaultPrng.init(42);
    const random = prng.random();
    var i: usize = 0;
    while (i + 8 <= data.len) {
        @memcpy(data[i..][0..4], "XYZW");
        random.bytes(data[i + 4 ..][0..4]);
        i += 8;
    }
    try crossRoundTrip(allocator, &data);
}

test "edge: very long match (64KB of same 4 bytes)" {
    const allocator = std.testing.allocator;
    const data = try allocator.alloc(u8, 65536);
    defer allocator.free(data);
    var i: usize = 0;
    while (i + 4 <= data.len) : (i += 4) {
        @memcpy(data[i..][0..4], "DEAD");
    }
    try crossRoundTrip(allocator, data);
}

// =========================================================================
// Edge cases: repeated offset logic
// =========================================================================

test "edge: consecutive rep[0] matches (same chunk repeated)" {
    const allocator = std.testing.allocator;
    var data: [8008]u8 = undefined;
    @memcpy(data[0..8], "PREAMBLE");
    for (1..1001) |j| {
        @memcpy(data[j * 8 ..][0..8], "PREAMBLE");
    }
    try crossRoundTrip(allocator, &data);
}

test "edge: alternating match distances (rep offset rotation)" {
    const allocator = std.testing.allocator;
    var data: [12000]u8 = undefined;
    var i: usize = 0;
    while (i + 16 <= data.len) {
        @memcpy(data[i..][0..8], "AAAABBBB");
        @memcpy(data[i + 8 ..][0..8], "CCCCDDDD");
        i += 16;
    }
    try crossRoundTrip(allocator, &data);
}

test "edge: three interleaved patterns (exercises rep[0], rep[1], rep[2])" {
    const allocator = std.testing.allocator;
    var data: [12000]u8 = undefined;
    var i: usize = 0;
    var phase: usize = 0;
    while (i + 8 <= data.len) {
        const pattern: *const [8]u8 = switch (phase % 3) {
            0 => "PATTERN1",
            1 => "PATTERN2",
            2 => "PATTERN3",
            else => unreachable,
        };
        @memcpy(data[i..][0..8], pattern);
        i += 8;
        phase += 1;
    }
    try crossRoundTrip(allocator, &data);
}

// =========================================================================
// Edge cases: raw block fallback
// =========================================================================

test "edge: no matches at all (shuffled bytes)" {
    const allocator = std.testing.allocator;
    var data: [1024]u8 = undefined;
    for (&data, 0..) |*b, idx| b.* = @truncate(idx);
    var prng = std.Random.DefaultPrng.init(12345);
    prng.random().shuffle(u8, &data);
    try crossRoundTrip(allocator, &data);
}

test "edge: large incompressible data (512KB random)" {
    const allocator = std.testing.allocator;
    const size = 512 * 1024;
    const data = try allocator.alloc(u8, size);
    defer allocator.free(data);
    var prng = std.Random.DefaultPrng.init(0xCAFEBABE);
    prng.random().bytes(data);
    try crossRoundTrip(allocator, data);
}

// =========================================================================
// Edge cases: scale
// =========================================================================

test "edge: 1MB patterned data" {
    const allocator = std.testing.allocator;
    const size = 1024 * 1024;
    const data = try allocator.alloc(u8, size);
    defer allocator.free(data);
    for (data, 0..) |*b, i| b.* = @truncate((i / 8) *% 31 +% (i % 8));
    try crossRoundTrip(allocator, data);
}

// =========================================================================
// Edge cases: Parquet-realistic data patterns
// =========================================================================

test "edge: parquet-like little-endian i64 timestamps" {
    const allocator = std.testing.allocator;
    const count = 1000;
    var data: [count * 8]u8 = undefined;
    var ts: i64 = 1711500000000000;
    for (0..count) |i| {
        std.mem.writeInt(i64, data[i * 8 ..][0..8], ts, .little);
        ts += 1000000;
    }
    try crossRoundTrip(allocator, &data);
}

test "edge: parquet-like dictionary indices (small ints)" {
    const allocator = std.testing.allocator;
    var data: [4000]u8 = undefined;
    var prng = std.Random.DefaultPrng.init(99);
    const random = prng.random();
    for (&data) |*b| b.* = random.intRangeAtMost(u8, 0, 15);
    try crossRoundTrip(allocator, &data);
}

test "edge: parquet-like null bitmap (95% non-null)" {
    const allocator = std.testing.allocator;
    var data: [1024]u8 = undefined;
    @memset(&data, 0xFF);
    var prng = std.Random.DefaultPrng.init(777);
    const random = prng.random();
    for (&data) |*b| {
        if (random.intRangeAtMost(u8, 0, 19) == 0) b.* = random.int(u8);
    }
    try crossRoundTrip(allocator, &data);
}

test "edge: parquet-like i32 column (small monotonic values)" {
    const allocator = std.testing.allocator;
    const count = 2000;
    var data: [count * 4]u8 = undefined;
    for (0..count) |i| {
        const val: i32 = @intCast(i * 3 + 100);
        std.mem.writeInt(i32, data[i * 4 ..][0..4], val, .little);
    }
    try crossRoundTrip(allocator, &data);
}

test "edge: parquet-like f64 column (temperature readings)" {
    const allocator = std.testing.allocator;
    const count = 1000;
    var data: [count * 8]u8 = undefined;
    var prng = std.Random.DefaultPrng.init(555);
    const random = prng.random();
    for (0..count) |i| {
        const temp: f64 = 20.0 + @as(f64, @floatFromInt(random.intRangeAtMost(u16, 0, 200))) / 10.0;
        std.mem.writeInt(u64, data[i * 8 ..][0..8], @bitCast(temp), .little);
    }
    try crossRoundTrip(allocator, &data);
}

// =========================================================================
// Edge cases: frame header encoding
// =========================================================================

test "edge: frame header FCS field boundaries" {
    const allocator = std.testing.allocator;
    const sizes = [_]usize{ 1, 255, 256, 65791, 65792, 100000 };
    for (sizes) |size| {
        const data = try allocator.alloc(u8, size);
        defer allocator.free(data);
        @memset(data, 'Z');
        try crossRoundTrip(allocator, data);
    }
}

// =========================================================================
// Edge cases: very small inputs
// =========================================================================

test "edge: single byte" {
    try zigRoundTrip(std.testing.allocator, "X");
}

test "edge: two bytes" {
    try zigRoundTrip(std.testing.allocator, "AB");
}

test "edge: three bytes" {
    try zigRoundTrip(std.testing.allocator, "ABC");
}
