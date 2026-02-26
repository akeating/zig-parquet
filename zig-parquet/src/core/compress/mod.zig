//! Compression codec support for Parquet
//!
//! Parquet supports multiple compression codecs. This module provides
//! compression and decompression for supported codecs.
//!
//! When built with `-Dno_compression`, only `.uncompressed` is supported
//! and all codec C/C++ dependencies are excluded from the build.

const build_options = @import("build_options");
const no_compression = build_options.no_compression;

pub const zstd = if (no_compression) {} else @import("zstd.zig");
pub const gzip = if (no_compression) {} else @import("gzip.zig");
pub const lz4 = if (no_compression) {} else @import("lz4.zig");
pub const brotli = if (no_compression) {} else @import("brotli.zig");
pub const snappy = if (no_compression) {} else @import("snappy.zig");

const std = @import("std");
const format = @import("../format.zig");

pub const CompressError = error{
    UnsupportedCompression,
    CompressionError,
    OutOfMemory,
};

pub const DecompressError = error{
    UnsupportedCompression,
    DecompressionError,
    OutOfMemory,
};

/// Compress data based on the codec
pub fn compress(
    allocator: std.mem.Allocator,
    data: []const u8,
    codec: format.CompressionCodec,
) CompressError![]u8 {
    if (no_compression) {
        return switch (codec) {
            .uncompressed => allocator.dupe(u8, data) catch return error.OutOfMemory,
            else => error.UnsupportedCompression,
        };
    }
    switch (codec) {
        .uncompressed => {
            return allocator.dupe(u8, data) catch return error.OutOfMemory;
        },
        .zstd => {
            return zstd.compress(allocator, data) catch |e| switch (e) {
                error.CompressionError => return error.CompressionError,
                error.OutOfMemory => return error.OutOfMemory,
                error.DecompressionError => return error.CompressionError,
            };
        },
        .gzip => {
            return gzip.compress(allocator, data) catch |e| switch (e) {
                error.CompressionError => return error.CompressionError,
                error.OutOfMemory => return error.OutOfMemory,
                error.DecompressionError => return error.CompressionError,
                error.InvalidSize => return error.CompressionError,
            };
        },
        .snappy => {
            return snappy.compress(allocator, data) catch |e| switch (e) {
                error.CompressionError => return error.CompressionError,
                error.OutOfMemory => return error.OutOfMemory,
                error.DecompressionError => return error.CompressionError,
            };
        },
        .lz4_raw => {
            return lz4.compress(allocator, data) catch |e| switch (e) {
                error.CompressionError => return error.CompressionError,
                error.OutOfMemory => return error.OutOfMemory,
                error.DecompressionError => return error.CompressionError,
            };
        },
        .brotli => {
            return brotli.compress(allocator, data) catch |e| switch (e) {
                error.CompressionError => return error.CompressionError,
                error.OutOfMemory => return error.OutOfMemory,
                error.DecompressionError => return error.CompressionError,
            };
        },
        else => return error.UnsupportedCompression,
    }
}

/// Check if a codec is supported for compression
pub fn isCompressionSupported(codec: format.CompressionCodec) bool {
    if (no_compression) {
        return codec == .uncompressed;
    }
    return switch (codec) {
        .uncompressed, .zstd, .gzip, .snappy, .lz4_raw, .brotli => true,
        else => false,
    };
}

/// Decompress data based on the codec
pub fn decompress(
    allocator: std.mem.Allocator,
    compressed: []const u8,
    codec: format.CompressionCodec,
    uncompressed_size: usize,
) DecompressError![]u8 {
    if (no_compression) {
        return switch (codec) {
            .uncompressed => allocator.dupe(u8, compressed) catch return error.OutOfMemory,
            else => error.UnsupportedCompression,
        };
    }
    switch (codec) {
        .uncompressed => {
            return allocator.dupe(u8, compressed) catch return error.OutOfMemory;
        },
        .zstd => {
            return zstd.decompress(allocator, compressed, uncompressed_size) catch |e| switch (e) {
                error.DecompressionError => return error.DecompressionError,
                error.OutOfMemory => return error.OutOfMemory,
                error.CompressionError => return error.DecompressionError,
            };
        },
        .gzip => {
            return gzip.decompress(allocator, compressed, uncompressed_size) catch |e| switch (e) {
                error.DecompressionError => return error.DecompressionError,
                error.OutOfMemory => return error.OutOfMemory,
                error.CompressionError => return error.DecompressionError,
                error.InvalidSize => return error.DecompressionError,
            };
        },
        .lz4_raw => {
            return lz4.decompress(allocator, compressed, uncompressed_size) catch |e| switch (e) {
                error.DecompressionError => return error.DecompressionError,
                error.OutOfMemory => return error.OutOfMemory,
                error.CompressionError => return error.DecompressionError,
            };
        },
        .brotli => {
            return brotli.decompress(allocator, compressed, uncompressed_size) catch |e| switch (e) {
                error.DecompressionError => return error.DecompressionError,
                error.OutOfMemory => return error.OutOfMemory,
                error.CompressionError => return error.DecompressionError,
            };
        },
        .snappy => {
            return snappy.decompress(allocator, compressed, uncompressed_size) catch |e| switch (e) {
                error.DecompressionError => return error.DecompressionError,
                error.OutOfMemory => return error.OutOfMemory,
                error.CompressionError => return error.DecompressionError,
            };
        },
        else => return error.UnsupportedCompression,
    }
}

/// Check if a codec is supported for decompression
pub fn isDecompressionSupported(codec: format.CompressionCodec) bool {
    if (no_compression) {
        return codec == .uncompressed;
    }
    return switch (codec) {
        .uncompressed, .zstd, .gzip, .lz4_raw, .brotli, .snappy => true,
        else => false,
    };
}

/// Get a human-readable name for a codec
pub fn codecName(codec: format.CompressionCodec) []const u8 {
    return switch (codec) {
        .uncompressed => "UNCOMPRESSED",
        .snappy => "SNAPPY",
        .gzip => "GZIP",
        .lzo => "LZO",
        .brotli => "BROTLI",
        .lz4 => "LZ4",
        .zstd => "ZSTD",
        .lz4_raw => "LZ4_RAW",
    };
}
