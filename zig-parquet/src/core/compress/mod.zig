//! Compression codec support for Parquet
//!
//! Parquet supports multiple compression codecs. This module provides
//! compression and decompression for supported codecs.
//!
//! Use `-Dcodecs=` to control which codecs are included (default: all).
//! Values: all, c-only, none, zig-only, or comma-separated list of: c-zstd,zstd,c-snappy,snappy,c-gzip,gzip,c-lz4,lz4,c-brotli,brotli

const build_options = @import("build_options");

pub const zstd = if (build_options.enable_zstd) @import("c_zstd.zig") else {};
pub const snappy = if (build_options.enable_snappy) @import("c_snappy.zig") else {};
pub const gzip = if (build_options.enable_gzip) @import("c_gzip.zig") else {};
pub const lz4 = if (build_options.enable_lz4) @import("c_lz4.zig") else {};
pub const brotli = if (build_options.enable_brotli) @import("c_brotli.zig") else {};

pub const zig_zstd = if (build_options.enable_zig_zstd) @import("zstd.zig") else {};
pub const zig_snappy = if (build_options.enable_zig_snappy) @import("snappy.zig") else {};
pub const zig_gzip = if (build_options.enable_zig_gzip) @import("gzip.zig") else {};
pub const zig_lz4 = if (build_options.enable_zig_lz4) @import("lz4.zig") else {};
pub const zig_brotli = if (build_options.enable_zig_brotli) @import("brotli.zig") else {};

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
    switch (codec) {
        .uncompressed => {
            return allocator.dupe(u8, data) catch return error.OutOfMemory;
        },
        .zstd => {
            if (build_options.enable_zig_zstd) {
                return zig_zstd.compress(allocator, data) catch |e| switch (e) {
                    error.CompressionError => return error.CompressionError,
                    error.OutOfMemory => return error.OutOfMemory,
                    error.DecompressionError => return error.CompressionError,
                    error.InvalidSize => return error.CompressionError,
                };
            } else if (build_options.enable_zstd) {
                return zstd.compress(allocator, data) catch |e| switch (e) {
                    error.CompressionError => return error.CompressionError,
                    error.OutOfMemory => return error.OutOfMemory,
                    error.DecompressionError => return error.CompressionError,
                    error.InvalidSize => return error.CompressionError,
                };
            }
            return error.UnsupportedCompression;
        },
        .gzip => {
            if (build_options.enable_zig_gzip) {
                return zig_gzip.compress(allocator, data) catch |e| switch (e) {
                    error.CompressionError => return error.CompressionError,
                    error.OutOfMemory => return error.OutOfMemory,
                    error.DecompressionError => return error.CompressionError,
                    error.InvalidSize => return error.CompressionError,
                };
            } else if (build_options.enable_gzip) {
                return gzip.compress(allocator, data) catch |e| switch (e) {
                    error.CompressionError => return error.CompressionError,
                    error.OutOfMemory => return error.OutOfMemory,
                    error.DecompressionError => return error.CompressionError,
                    error.InvalidSize => return error.CompressionError,
                };
            }
            return error.UnsupportedCompression;
        },
        .snappy => {
            if (build_options.enable_zig_snappy) {
                return zig_snappy.compress(allocator, data) catch |e| switch (e) {
                    error.CompressionError => return error.CompressionError,
                    error.OutOfMemory => return error.OutOfMemory,
                    error.DecompressionError => return error.CompressionError,
                    error.InvalidSize => return error.CompressionError,
                };
            } else if (build_options.enable_snappy) {
                return snappy.compress(allocator, data) catch |e| switch (e) {
                    error.CompressionError => return error.CompressionError,
                    error.OutOfMemory => return error.OutOfMemory,
                    error.DecompressionError => return error.CompressionError,
                    error.InvalidSize => return error.CompressionError,
                };
            }
            return error.UnsupportedCompression;
        },
        .lz4_raw => {
            if (build_options.enable_zig_lz4) {
                return zig_lz4.compress(allocator, data) catch |e| switch (e) {
                    error.CompressionError => return error.CompressionError,
                    error.OutOfMemory => return error.OutOfMemory,
                    error.DecompressionError => return error.CompressionError,
                    error.InvalidSize => return error.CompressionError,
                };
            } else if (build_options.enable_lz4) {
                return lz4.compress(allocator, data) catch |e| switch (e) {
                    error.CompressionError => return error.CompressionError,
                    error.OutOfMemory => return error.OutOfMemory,
                    error.DecompressionError => return error.CompressionError,
                };
            }
            return error.UnsupportedCompression;
        },
        .brotli => {
            if (build_options.enable_zig_brotli) {
                return zig_brotli.compress(allocator, data) catch |e| switch (e) {
                    error.CompressionError => return error.CompressionError,
                    error.OutOfMemory => return error.OutOfMemory,
                    error.DecompressionError => return error.CompressionError,
                    error.InvalidSize => return error.CompressionError,
                };
            } else if (build_options.enable_brotli) {
                return brotli.compress(allocator, data) catch |e| switch (e) {
                    error.CompressionError => return error.CompressionError,
                    error.OutOfMemory => return error.OutOfMemory,
                    error.DecompressionError => return error.CompressionError,
                };
            }
            return error.UnsupportedCompression;
        },
        else => return error.UnsupportedCompression,
    }
}

/// Decompress data based on the codec
pub fn decompress(
    allocator: std.mem.Allocator,
    compressed: []const u8,
    codec: format.CompressionCodec,
    uncompressed_size: usize,
) DecompressError![]u8 {
    switch (codec) {
        .uncompressed => {
            return allocator.dupe(u8, compressed) catch return error.OutOfMemory;
        },
        .zstd => {
            if (build_options.enable_zig_zstd) {
                return zig_zstd.decompress(allocator, compressed, uncompressed_size) catch |e| switch (e) {
                    error.DecompressionError => return error.DecompressionError,
                    error.OutOfMemory => return error.OutOfMemory,
                    error.CompressionError => return error.DecompressionError,
                    error.InvalidSize => return error.DecompressionError,
                };
            } else if (build_options.enable_zstd) {
                return zstd.decompress(allocator, compressed, uncompressed_size) catch |e| switch (e) {
                    error.DecompressionError => return error.DecompressionError,
                    error.OutOfMemory => return error.OutOfMemory,
                    error.CompressionError => return error.DecompressionError,
                    error.InvalidSize => return error.DecompressionError,
                };
            }
            return error.UnsupportedCompression;
        },
        .gzip => {
            if (build_options.enable_zig_gzip) {
                return zig_gzip.decompress(allocator, compressed, uncompressed_size) catch |e| switch (e) {
                    error.DecompressionError => return error.DecompressionError,
                    error.OutOfMemory => return error.OutOfMemory,
                    error.CompressionError => return error.DecompressionError,
                    error.InvalidSize => return error.DecompressionError,
                };
            } else if (build_options.enable_gzip) {
                return gzip.decompress(allocator, compressed, uncompressed_size) catch |e| switch (e) {
                    error.DecompressionError => return error.DecompressionError,
                    error.OutOfMemory => return error.OutOfMemory,
                    error.CompressionError => return error.DecompressionError,
                    error.InvalidSize => return error.DecompressionError,
                };
            }
            return error.UnsupportedCompression;
        },
        .lz4_raw => {
            if (build_options.enable_zig_lz4) {
                return zig_lz4.decompress(allocator, compressed, uncompressed_size) catch |e| switch (e) {
                    error.DecompressionError => return error.DecompressionError,
                    error.OutOfMemory => return error.OutOfMemory,
                    error.CompressionError => return error.DecompressionError,
                    error.InvalidSize => return error.DecompressionError,
                };
            } else if (build_options.enable_lz4) {
                return lz4.decompress(allocator, compressed, uncompressed_size) catch |e| switch (e) {
                    error.DecompressionError => return error.DecompressionError,
                    error.OutOfMemory => return error.OutOfMemory,
                    error.CompressionError => return error.DecompressionError,
                };
            }
            return error.UnsupportedCompression;
        },
        .brotli => {
            if (build_options.enable_zig_brotli) {
                return zig_brotli.decompress(allocator, compressed, uncompressed_size) catch |e| switch (e) {
                    error.DecompressionError => return error.DecompressionError,
                    error.OutOfMemory => return error.OutOfMemory,
                    error.CompressionError => return error.DecompressionError,
                    error.InvalidSize => return error.DecompressionError,
                };
            } else if (build_options.enable_brotli) {
                return brotli.decompress(allocator, compressed, uncompressed_size) catch |e| switch (e) {
                    error.DecompressionError => return error.DecompressionError,
                    error.OutOfMemory => return error.OutOfMemory,
                    error.CompressionError => return error.DecompressionError,
                };
            }
            return error.UnsupportedCompression;
        },
        .snappy => {
            if (build_options.enable_zig_snappy) {
                return zig_snappy.decompress(allocator, compressed, uncompressed_size) catch |e| switch (e) {
                    error.DecompressionError => return error.DecompressionError,
                    error.OutOfMemory => return error.OutOfMemory,
                    error.CompressionError => return error.DecompressionError,
                    error.InvalidSize => return error.DecompressionError,
                };
            } else if (build_options.enable_snappy) {
                return snappy.decompress(allocator, compressed, uncompressed_size) catch |e| switch (e) {
                    error.DecompressionError => return error.DecompressionError,
                    error.OutOfMemory => return error.OutOfMemory,
                    error.CompressionError => return error.DecompressionError,
                    error.InvalidSize => return error.DecompressionError,
                };
            }
            return error.UnsupportedCompression;
        },
        else => return error.UnsupportedCompression,
    }
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
