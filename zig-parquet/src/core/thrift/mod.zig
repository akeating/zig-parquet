//! Thrift Compact Protocol codec
//!
//! Implements just enough of the Thrift Compact Protocol to parse and write Parquet metadata.
//! Reference: https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md

pub const CompactReader = @import("compact.zig").CompactReader;
pub const CompactWriter = @import("compact_writer.zig").CompactWriter;
pub const Type = @import("compact.zig").Type;

test {
    _ = @import("compact.zig");
    _ = @import("compact_writer.zig");
}
