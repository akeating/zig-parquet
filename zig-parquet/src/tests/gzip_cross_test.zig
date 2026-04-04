//! Cross-implementation gzip tests
//!
//! Validates interoperability between C gzip and pure Zig gzip implementations.
//! Cross-impl tests run when both are compiled in: -Dcodecs=gzip,zig-gzip
//! Edge-case tests always run (Zig-only round-trips) with cross-validation when available.

const std = @import("std");
const build_options = @import("build_options");
const c_gzip = @import("../core/compress/gzip.zig");
const zig_gzip = @import("../core/compress/zig_gzip.zig");

const both_enabled = build_options.enable_gzip and build_options.enable_zig_gzip;

// =========================================================================
// Helpers
// =========================================================================

fn zigRoundTrip(allocator: std.mem.Allocator, data: []const u8) !void {
    if (!build_options.enable_zig_gzip) return;
    const compressed = try zig_gzip.compress(allocator, data);
    defer allocator.free(compressed);

    const decompressed = try zig_gzip.decompress(allocator, compressed, data.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualSlices(u8, data, decompressed);
}

fn crossRoundTrip(allocator: std.mem.Allocator, data: []const u8) !void {
    if (!build_options.enable_zig_gzip) return;
    const zig_compressed = try zig_gzip.compress(allocator, data);
    defer allocator.free(zig_compressed);

    if (both_enabled) {
        const cc = try c_gzip.decompress(allocator, zig_compressed, data.len);
        defer allocator.free(cc);
        try std.testing.expectEqualSlices(u8, data, cc);
    }

    const decompressed = try zig_gzip.decompress(allocator, zig_compressed, data.len);
    defer allocator.free(decompressed);
    try std.testing.expectEqualSlices(u8, data, decompressed);
}

// =========================================================================
// Cross-implementation tests (require both C and Zig gzip)
// =========================================================================

test "cross-impl: C compress, Zig decompress" {
    if (!both_enabled) return;
    const allocator = std.testing.allocator;

    const original = "Hello, World! This is a cross-implementation gzip test." ** 20;

    const compressed = try c_gzip.compress(allocator, original);
    defer allocator.free(compressed);

    const decompressed = try zig_gzip.decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualStrings(original, decompressed);
}

test "cross-impl: Zig compress, C decompress" {
    if (!both_enabled) return;
    const allocator = std.testing.allocator;

    const original = "Hello, World! This is a cross-implementation gzip test." ** 20;

    const compressed = try zig_gzip.compress(allocator, original);
    defer allocator.free(compressed);

    const decompressed = try c_gzip.decompress(allocator, compressed, original.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualStrings(original, decompressed);
}

test "cross-impl: bidirectional round-trip (all 4 combinations)" {
    if (!both_enabled) return;
    const allocator = std.testing.allocator;

    const original = "ABCDEFGH" ** 200;

    const c_compressed = try c_gzip.compress(allocator, original);
    defer allocator.free(c_compressed);
    const zig_compressed = try zig_gzip.compress(allocator, original);
    defer allocator.free(zig_compressed);

    const cc = try c_gzip.decompress(allocator, c_compressed, original.len);
    defer allocator.free(cc);
    try std.testing.expectEqualStrings(original, cc);

    const cz = try zig_gzip.decompress(allocator, c_compressed, original.len);
    defer allocator.free(cz);
    try std.testing.expectEqualStrings(original, cz);

    const zc = try c_gzip.decompress(allocator, zig_compressed, original.len);
    defer allocator.free(zc);
    try std.testing.expectEqualStrings(original, zc);

    const zz = try zig_gzip.decompress(allocator, zig_compressed, original.len);
    defer allocator.free(zz);
    try std.testing.expectEqualStrings(original, zz);
}

test "cross-impl: empty data" {
    if (!both_enabled) return;
    const allocator = std.testing.allocator;

    const original = "";

    const c_compressed = try c_gzip.compress(allocator, original);
    defer allocator.free(c_compressed);
    const zig_compressed = try zig_gzip.compress(allocator, original);
    defer allocator.free(zig_compressed);

    const cc = try c_gzip.decompress(allocator, c_compressed, original.len);
    defer allocator.free(cc);
    try std.testing.expectEqualStrings(original, cc);

    const zz = try zig_gzip.decompress(allocator, zig_compressed, original.len);
    defer allocator.free(zz);
    try std.testing.expectEqualStrings(original, zz);
}

test "cross-impl: magic bytes (0x1f 0x8b)" {
    if (!both_enabled) return;
    const allocator = std.testing.allocator;

    const original = "test data";

    const c_compressed = try c_gzip.compress(allocator, original);
    defer allocator.free(c_compressed);
    const zig_compressed = try zig_gzip.compress(allocator, original);
    defer allocator.free(zig_compressed);

    try std.testing.expect(c_compressed.len >= 2);
    try std.testing.expectEqual(@as(u8, 0x1f), c_compressed[0]);
    try std.testing.expectEqual(@as(u8, 0x8b), c_compressed[1]);

    try std.testing.expect(zig_compressed.len >= 2);
    try std.testing.expectEqual(@as(u8, 0x1f), zig_compressed[0]);
    try std.testing.expectEqual(@as(u8, 0x8b), zig_compressed[1]);
}

test "cross-impl: concatenated gzip streams" {
    if (!both_enabled) return;
    const allocator = std.testing.allocator;

    const part1 = "First gzip member data. " ** 50;
    const part2 = "Second gzip member data. " ** 50;
    const total_len = part1.len + part2.len;

    // Concatenate two independently compressed streams (C compressor)
    const c1 = try c_gzip.compress(allocator, part1);
    defer allocator.free(c1);
    const c2 = try c_gzip.compress(allocator, part2);
    defer allocator.free(c2);

    const concatenated = try allocator.alloc(u8, c1.len + c2.len);
    defer allocator.free(concatenated);
    @memcpy(concatenated[0..c1.len], c1);
    @memcpy(concatenated[c1.len..], c2);

    // Zig decompressor must handle concatenated members
    const decompressed = try zig_gzip.decompress(allocator, concatenated, total_len);
    defer allocator.free(decompressed);
    try std.testing.expectEqualStrings(part1 ++ part2, decompressed);

    // C decompressor should also handle it (baseline)
    const c_decompressed = try c_gzip.decompress(allocator, concatenated, total_len);
    defer allocator.free(c_decompressed);
    try std.testing.expectEqualStrings(part1 ++ part2, c_decompressed);
}

test "cross-impl: concatenated Zig-compressed streams via C decompress" {
    if (!both_enabled) return;
    const allocator = std.testing.allocator;

    const part1 = "Zig compressed part one. " ** 40;
    const part2 = "Zig compressed part two. " ** 40;
    const total_len = part1.len + part2.len;

    const z1 = try zig_gzip.compress(allocator, part1);
    defer allocator.free(z1);
    const z2 = try zig_gzip.compress(allocator, part2);
    defer allocator.free(z2);

    const concatenated = try allocator.alloc(u8, z1.len + z2.len);
    defer allocator.free(concatenated);
    @memcpy(concatenated[0..z1.len], z1);
    @memcpy(concatenated[z1.len..], z2);

    // C decompressor handles concatenated Zig-compressed members
    const c_decompressed = try c_gzip.decompress(allocator, concatenated, total_len);
    defer allocator.free(c_decompressed);
    try std.testing.expectEqualStrings(part1 ++ part2, c_decompressed);

    // Zig decompressor round-trip
    const z_decompressed = try zig_gzip.decompress(allocator, concatenated, total_len);
    defer allocator.free(z_decompressed);
    try std.testing.expectEqualStrings(part1 ++ part2, z_decompressed);
}

// =========================================================================
// Edge-case tests (Zig implementation, cross-validate if C available)
// =========================================================================

test "edge: small data" {
    const allocator = std.testing.allocator;
    try zigRoundTrip(allocator, "a");
    try zigRoundTrip(allocator, "ab");
    try zigRoundTrip(allocator, "abc");
}

test "edge: repetitive data" {
    const allocator = std.testing.allocator;
    try zigRoundTrip(allocator, "AAAA" ** 1000);
}

test "edge: large data" {
    const allocator = std.testing.allocator;
    var buf: [10000]u8 = undefined;
    for (0..buf.len) |i| {
        buf[i] = @intCast(i % 256);
    }
    try zigRoundTrip(allocator, &buf);
}

test "edge: random-like data" {
    const allocator = std.testing.allocator;
    var buf: [10000]u8 = undefined;
    var seed: u64 = 0x853c49e6748fea9b;
    for (0..buf.len) |i| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        buf[i] = @intCast(seed >> 56);
    }
    try zigRoundTrip(allocator, &buf);
}

// =========================================================================
// Write-path tests (Zig compression round-trip)
// =========================================================================

test "write-path: compression produces valid gzip" {
    const allocator = std.testing.allocator;
    const original = "This data should compress and decompress correctly";

    try crossRoundTrip(allocator, original);
}

test "write-path: large dataset" {
    const allocator = std.testing.allocator;
    var buf: [10000]u8 = undefined;
    for (0..buf.len) |i| {
        buf[i] = @intCast((i * 7) % 256);
    }

    try crossRoundTrip(allocator, &buf);
}
