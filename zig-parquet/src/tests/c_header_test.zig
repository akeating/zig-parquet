//! Checks that libparquet.h, translated from C, declares what the Zig code exports. The C
//! header and the Zig code are written separately; nothing else compares them.

const std = @import("std");
const c = @import("c_libparquet");
const stream_options = @import("../api/stream_options.zig");

test "zp_stream_options has the layout of StreamOptions" {
    const C = c.zp_stream_options;
    const Z = stream_options.StreamOptions;
    try std.testing.expectEqual(@sizeOf(Z), @sizeOf(C));
    inline for (.{ "size", "flags", "col_indices", "num_cols", "row_groups", "num_row_groups", "batch_rows", "threads", "read_ahead" }) |field| {
        try std.testing.expectEqual(@offsetOf(Z, field), @offsetOf(C, field));
        try std.testing.expectEqual(@sizeOf(@FieldType(Z, field)), @sizeOf(@FieldType(C, field)));
    }
    try std.testing.expectEqual(stream_options.ZP_STREAM_LARGE_OFFSETS, c.ZP_STREAM_LARGE_OFFSETS);
}
