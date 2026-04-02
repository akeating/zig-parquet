//! Test module - imports all test files
//!
//! Run tests with: zig build test

const std = @import("std");

// Import all test modules to run their tests
test {
    _ = @import("reader_test.zig");
    _ = @import("encoding_test.zig");
    _ = @import("edge_cases_test.zig");
    _ = @import("writer_basic_test.zig");
    _ = @import("writer_logical_test.zig");
    _ = @import("writer_compression_test.zig");
    _ = @import("interop_logical_types_test.zig");
    _ = @import("roundtrip_test.zig");
    _ = @import("list_test.zig");
    _ = @import("struct_test.zig");
    _ = @import("map_test.zig");
    _ = @import("nested_test.zig");
    _ = @import("dynamic_reader_test.zig");
    _ = @import("seekable_reader_test.zig");
    _ = @import("multipage_test.zig");
    _ = @import("bson_test.zig");
    _ = @import("interval_test.zig");
    _ = @import("geo_test.zig");
    _ = @import("checksum_test.zig");
    _ = @import("interop_pyarrow_test.zig");
    _ = @import("callback_transport_test.zig");
    _ = @import("c_abi_test.zig");
    _ = @import("negative_test.zig");
    _ = @import("zstd_cross_test.zig");
    _ = @import("snappy_cross_test.zig");
}
