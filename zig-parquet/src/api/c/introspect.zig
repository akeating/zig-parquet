//! Library introspection functions (version, codec support).
//! Used by both C ABI and WASM surfaces. Only the C ABI surface
//! exports with callconv(.c); WASM surfaces re-export with their
//! own signatures.

const std = @import("std");
const format = @import("../../core/format.zig");
const CompressionCodec = format.CompressionCodec;

const build_options = @import("build_options");
const no_compression = build_options.no_compression;
pub const version: [:0]const u8 = build_options.version[0..build_options.version.len :0];

pub fn getVersion() [*:0]const u8 {
    return version;
}

pub fn isCodecSupported(codec: i32) i32 {
    const c = CompressionCodec.fromInt(codec) catch return 0;
    if (no_compression) {
        return if (c == .uncompressed) 1 else 0;
    }
    return switch (c) {
        .uncompressed, .snappy, .gzip, .brotli, .zstd, .lz4_raw => 1,
        .lzo, .lz4 => 0,
    };
}

const is_wasm = switch (@import("builtin").target.cpu.arch) {
    .wasm32, .wasm64 => true,
    else => false,
};

comptime {
    if (!is_wasm) {
        @export(&zp_version_impl, .{ .name = "zp_version" });
        @export(&zp_codec_supported_impl, .{ .name = "zp_codec_supported" });
    }
}

fn zp_version_impl() callconv(.c) [*:0]const u8 {
    return getVersion();
}

fn zp_codec_supported_impl(codec: c_int) callconv(.c) c_int {
    return isCodecSupported(codec);
}
