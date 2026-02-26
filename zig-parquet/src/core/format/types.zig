//! Core Parquet format enums
//!
//! These enums mirror the Thrift definitions from the Parquet format specification.

const std = @import("std");

/// Physical types supported by Parquet
pub const PhysicalType = enum(i32) {
    boolean = 0,
    int32 = 1,
    int64 = 2,
    int96 = 3, // Deprecated, only used by legacy implementations
    float = 4,
    double = 5,
    byte_array = 6,
    fixed_len_byte_array = 7,

    pub fn fromInt(v: i32) !PhysicalType {
        return std.meta.intToEnum(PhysicalType, v) catch error.InvalidPhysicalType;
    }
};

/// Repetition types for schema elements
pub const RepetitionType = enum(i32) {
    required = 0,
    optional = 1,
    repeated = 2,

    pub fn fromInt(v: i32) !RepetitionType {
        return std.meta.intToEnum(RepetitionType, v) catch error.InvalidRepetitionType;
    }
};

/// Encoding types
pub const Encoding = enum(i32) {
    plain = 0,
    plain_dictionary = 2,
    rle = 3,
    bit_packed = 4,
    delta_binary_packed = 5,
    delta_length_byte_array = 6,
    delta_byte_array = 7,
    rle_dictionary = 8,
    byte_stream_split = 9,

    pub fn fromInt(v: i32) !Encoding {
        return std.meta.intToEnum(Encoding, v) catch error.InvalidEncoding;
    }
};

/// Compression codecs
pub const CompressionCodec = enum(i32) {
    uncompressed = 0,
    snappy = 1,
    gzip = 2,
    lzo = 3,
    brotli = 4,
    lz4 = 5,
    zstd = 6,
    lz4_raw = 7,

    pub fn fromInt(v: i32) !CompressionCodec {
        return std.meta.intToEnum(CompressionCodec, v) catch error.InvalidCompressionCodec;
    }
};

/// Page types
pub const PageType = enum(i32) {
    data_page = 0,
    index_page = 1,
    dictionary_page = 2,
    data_page_v2 = 3,

    pub fn fromInt(v: i32) !PageType {
        return std.meta.intToEnum(PageType, v) catch error.InvalidPageType;
    }
};
