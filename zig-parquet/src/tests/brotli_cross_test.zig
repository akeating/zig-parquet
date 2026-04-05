//! Cross-implementation Brotli tests
//!
//! Validates interoperability between C libbrotli and pure Zig Brotli implementations.
//! Cross-impl tests run when both are compiled in: -Dcodecs=brotli,zig-brotli
//! Edge-case tests always run (Zig-only round-trips) with cross-validation when available.

const std = @import("std");
const build_options = @import("build_options");
const c_brotli = @import("../core/compress/brotli.zig");
const zig_brotli = @import("../core/compress/zig_brotli.zig");

const both_enabled = build_options.enable_brotli and build_options.enable_zig_brotli;

// =========================================================================
// Helpers
// =========================================================================

fn zigRoundTrip(allocator: std.mem.Allocator, data: []const u8) !void {
    if (!build_options.enable_zig_brotli) return;
    const compressed = try zig_brotli.compress(allocator, data);
    defer allocator.free(compressed);

    const decompressed = try zig_brotli.decompress(allocator, compressed, data.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualSlices(u8, data, decompressed);
}

fn crossRoundTrip(allocator: std.mem.Allocator, data: []const u8) !void {
    if (!build_options.enable_zig_brotli) return;
    const zig_compressed = try zig_brotli.compress(allocator, data);
    defer allocator.free(zig_compressed);

    if (both_enabled) {
        const zc = try c_brotli.decompress(allocator, zig_compressed, data.len);
        defer allocator.free(zc);
        try std.testing.expectEqualSlices(u8, data, zc);
    }

    const decompressed = try zig_brotli.decompress(allocator, zig_compressed, data.len);
    defer allocator.free(decompressed);
    try std.testing.expectEqualSlices(u8, data, decompressed);
}

// =========================================================================
// Cross-implementation tests (require both C and Zig Brotli)
// =========================================================================

test "cross-impl: C compress, Zig decompress" {
    if (!both_enabled) return;
    const allocator = std.testing.allocator;

    const original = "Hello, World! This is a cross-implementation Brotli test." ** 20;

    const compressed = try c_brotli.compress(allocator, original);
    defer allocator.free(compressed);

    const decompressed = try zig_brotli.decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualStrings(original, decompressed);
}

test "cross-impl: Zig compress, C decompress" {
    if (!both_enabled) return;
    const allocator = std.testing.allocator;

    const original = "Hello, World! This is a cross-implementation Brotli test." ** 20;

    const compressed = try zig_brotli.compress(allocator, original);
    defer allocator.free(compressed);

    const decompressed = try c_brotli.decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualStrings(original, decompressed);
}

test "cross-impl: bidirectional round-trip (all 4 combinations)" {
    if (!both_enabled) return;
    const allocator = std.testing.allocator;

    const original = "ABCDEFGH" ** 200;

    const c_compressed = try c_brotli.compress(allocator, original);
    defer allocator.free(c_compressed);
    const zig_compressed = try zig_brotli.compress(allocator, original);
    defer allocator.free(zig_compressed);

    const cc = try c_brotli.decompress(allocator, c_compressed, original.len);
    defer allocator.free(cc);
    try std.testing.expectEqualStrings(original, cc);

    const cz = try zig_brotli.decompress(allocator, c_compressed, original.len);
    defer allocator.free(cz);
    try std.testing.expectEqualStrings(original, cz);

    const zc = try c_brotli.decompress(allocator, zig_compressed, original.len);
    defer allocator.free(zc);
    try std.testing.expectEqualStrings(original, zc);

    const zz = try zig_brotli.decompress(allocator, zig_compressed, original.len);
    defer allocator.free(zz);
    try std.testing.expectEqualStrings(original, zz);
}

test "cross-impl: empty data" {
    if (!both_enabled) return;
    const allocator = std.testing.allocator;

    const c_compressed = try c_brotli.compress(allocator, "");
    defer allocator.free(c_compressed);
    const cz = try zig_brotli.decompress(allocator, c_compressed, 0);
    defer allocator.free(cz);
    try std.testing.expectEqual(@as(usize, 0), cz.len);

    const zig_compressed = try zig_brotli.compress(allocator, "");
    defer allocator.free(zig_compressed);
    const zc = try c_brotli.decompress(allocator, zig_compressed, 0);
    defer allocator.free(zc);
    try std.testing.expectEqual(@as(usize, 0), zc.len);
}

test "cross-impl: varied patterns" {
    if (!both_enabled) return;
    const allocator = std.testing.allocator;

    const patterns = [_][]const u8{
        "short",
        "a" ** 500,
        "abcdefghijklmnop" ** 200,
        "The quick brown fox jumps over the lazy dog. " ** 100,
    };

    for (patterns) |original| {
        const zig_compressed = try zig_brotli.compress(allocator, original);
        defer allocator.free(zig_compressed);
        const decompressed = try c_brotli.decompress(allocator, zig_compressed, original.len);
        defer allocator.free(decompressed);
        try std.testing.expectEqualStrings(original, decompressed);
    }
}

test "cross-impl: random incompressible data" {
    if (!both_enabled) return;
    const allocator = std.testing.allocator;

    var prng = std.Random.DefaultPrng.init(0xdeadbeef);
    const size = 4096;
    const data = try allocator.alloc(u8, size);
    defer allocator.free(data);
    prng.random().bytes(data);

    const c_compressed = try c_brotli.compress(allocator, data);
    defer allocator.free(c_compressed);
    const zig_compressed = try zig_brotli.compress(allocator, data);
    defer allocator.free(zig_compressed);

    const cz = try zig_brotli.decompress(allocator, c_compressed, size);
    defer allocator.free(cz);
    try std.testing.expectEqualSlices(u8, data, cz);

    const zc = try c_brotli.decompress(allocator, zig_compressed, size);
    defer allocator.free(zc);
    try std.testing.expectEqualSlices(u8, data, zc);
}

// =========================================================================
// Edge cases
// =========================================================================

test "edge: single byte repeated 100KB" {
    const allocator = std.testing.allocator;
    const data = try allocator.alloc(u8, 100_000);
    defer allocator.free(data);
    @memset(data, 'A');
    try crossRoundTrip(allocator, data);
}

test "edge: large incompressible data (256KB random)" {
    const allocator = std.testing.allocator;
    const size = 256 * 1024;
    const data = try allocator.alloc(u8, size);
    defer allocator.free(data);
    var prng = std.Random.DefaultPrng.init(0xCAFEBABE);
    prng.random().bytes(data);
    try crossRoundTrip(allocator, data);
}

test "edge: 512KB patterned data" {
    const allocator = std.testing.allocator;
    const size = 512 * 1024;
    const data = try allocator.alloc(u8, size);
    defer allocator.free(data);
    for (data, 0..) |*b, i| b.* = @truncate((i / 8) *% 31 +% (i % 8));
    try crossRoundTrip(allocator, data);
}

test "edge: parquet-like i64 timestamps" {
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

test "edge: parquet-like dictionary indices" {
    const allocator = std.testing.allocator;
    var data: [4000]u8 = undefined;
    var prng = std.Random.DefaultPrng.init(99);
    const random = prng.random();
    for (&data) |*b| b.* = random.intRangeAtMost(u8, 0, 15);
    try crossRoundTrip(allocator, &data);
}

test "edge: single byte" {
    try zigRoundTrip(std.testing.allocator, "X");
}

test "edge: two bytes" {
    try zigRoundTrip(std.testing.allocator, "AB");
}

test "edge: three bytes" {
    try zigRoundTrip(std.testing.allocator, "ABC");
}
