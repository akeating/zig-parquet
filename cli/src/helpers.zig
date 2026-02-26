const std = @import("std");
const parquet = @import("parquet");

pub fn codecToString(codec: parquet.format.CompressionCodec) []const u8 {
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

pub fn physicalTypeToString(t: parquet.format.PhysicalType) []const u8 {
    return switch (t) {
        .boolean => "BOOLEAN",
        .int32 => "INT32",
        .int64 => "INT64",
        .int96 => "INT96",
        .float => "FLOAT",
        .double => "DOUBLE",
        .byte_array => "BYTE_ARRAY",
        .fixed_len_byte_array => "FIXED_LEN_BYTE_ARRAY",
    };
}

pub fn repetitionToString(r: ?parquet.format.RepetitionType) []const u8 {
    if (r) |rep| {
        return switch (rep) {
            .required => "REQUIRED",
            .optional => "OPTIONAL",
            .repeated => "REPEATED",
        };
    }
    return "REQUIRED";
}

pub fn encodingToString(e: parquet.format.Encoding) []const u8 {
    return switch (e) {
        .plain => "PLAIN",
        .plain_dictionary => "PLAIN_DICTIONARY",
        .rle => "RLE",
        .bit_packed => "BIT_PACKED",
        .delta_binary_packed => "DELTA_BINARY_PACKED",
        .delta_length_byte_array => "DELTA_LENGTH_BYTE_ARRAY",
        .delta_byte_array => "DELTA_BYTE_ARRAY",
        .rle_dictionary => "RLE_DICTIONARY",
        .byte_stream_split => "BYTE_STREAM_SPLIT",
    };
}

pub fn printFileSize(writer: *std.Io.Writer, bytes: u64) !void {
    const units = [_][]const u8{ "B", "KB", "MB", "GB", "TB" };
    var size: f64 = @floatFromInt(bytes);
    var unit_idx: usize = 0;

    while (size >= 1024 and unit_idx < units.len - 1) {
        size /= 1024;
        unit_idx += 1;
    }

    if (unit_idx == 0) {
        try writer.print("{} {s}", .{ bytes, units[0] });
    } else {
        try writer.print("{d:.1} {s}", .{ size, units[unit_idx] });
    }
}

pub fn formatFileSize(writer: *std.Io.Writer, bytes: u64) !void {
    try writer.writeAll("Size: ");
    try printFileSize(writer, bytes);
    try writer.writeAll("\n");
}

pub fn printStatValue(writer: *std.Io.Writer, value: ?[]const u8, col_type: ?parquet.format.PhysicalType) !void {
    const bytes = value orelse {
        try writer.writeAll("null");
        return;
    };

    const ptype = col_type orelse {
        try printHex(writer, bytes);
        return;
    };

    switch (ptype) {
        .int32 => {
            if (bytes.len >= 4) {
                const val = std.mem.readInt(i32, bytes[0..4], .little);
                try writer.print("{}", .{val});
            } else {
                try printHex(writer, bytes);
            }
        },
        .int64 => {
            if (bytes.len >= 8) {
                const val = std.mem.readInt(i64, bytes[0..8], .little);
                try writer.print("{}", .{val});
            } else {
                try printHex(writer, bytes);
            }
        },
        .float => {
            if (bytes.len >= 4) {
                const bits = std.mem.readInt(u32, bytes[0..4], .little);
                const val: f32 = @bitCast(bits);
                if (std.math.isNan(val)) {
                    try writer.writeAll("NaN");
                } else if (std.math.isInf(val)) {
                    try writer.writeAll(if (val < 0) "-inf" else "inf");
                } else if (@abs(val) > 1e10 or (@abs(val) < 1e-4 and val != 0)) {
                    try writer.print("{e:.4}", .{val});
                } else {
                    try writer.print("{d:.4}", .{val});
                }
            } else {
                try printHex(writer, bytes);
            }
        },
        .double => {
            if (bytes.len >= 8) {
                const bits = std.mem.readInt(u64, bytes[0..8], .little);
                const val: f64 = @bitCast(bits);
                if (std.math.isNan(val)) {
                    try writer.writeAll("NaN");
                } else if (std.math.isInf(val)) {
                    try writer.writeAll(if (val < 0) "-inf" else "inf");
                } else if (@abs(val) > 1e10 or (@abs(val) < 1e-4 and val != 0)) {
                    try writer.print("{e:.4}", .{val});
                } else {
                    try writer.print("{d:.4}", .{val});
                }
            } else {
                try printHex(writer, bytes);
            }
        },
        .boolean => {
            if (bytes.len >= 1) {
                try writer.writeAll(if (bytes[0] != 0) "true" else "false");
            } else {
                try writer.writeAll("?");
            }
        },
        .byte_array, .fixed_len_byte_array => {
            if (isPrintableString(bytes)) {
                try writer.print("\"{s}\"", .{bytes});
            } else if (bytes.len <= 16) {
                try printHex(writer, bytes);
            } else {
                try writer.print("({} bytes)", .{bytes.len});
            }
        },
        .int96 => {
            try writer.print("({} bytes)", .{bytes.len});
        },
    }
}

pub fn printHex(writer: *std.Io.Writer, bytes: []const u8) !void {
    try writer.writeAll("0x");
    for (bytes) |b| {
        try writer.print("{x:0>2}", .{b});
    }
}

pub fn isPrintableString(bytes: []const u8) bool {
    if (bytes.len == 0) return true;
    if (bytes.len > 100) return false;
    for (bytes) |b| {
        if (b < 0x20 or b > 0x7e) return false;
    }
    return true;
}

pub fn countLeafColumns(schema: []const parquet.format.SchemaElement) usize {
    var count: usize = 0;
    for (schema) |elem| {
        if (elem.type_ != null and elem.num_children == null) {
            count += 1;
        }
    }
    return count;
}

/// Find the leaf column index for a given column name. Returns null if not found.
pub fn findColumnIndex(schema: []const parquet.format.SchemaElement, name: []const u8) ?usize {
    var leaf_idx: usize = 0;
    for (schema) |elem| {
        if (elem.type_ != null and elem.num_children == null) {
            if (std.mem.eql(u8, elem.name, name)) {
                return leaf_idx;
            }
            leaf_idx += 1;
        }
    }
    return null;
}

/// Get schema element for a leaf column by index.
pub fn getLeafElement(schema: []const parquet.format.SchemaElement, column_index: usize) ?parquet.format.SchemaElement {
    var leaf_idx: usize = 0;
    for (schema) |elem| {
        if (elem.type_ != null and elem.num_children == null) {
            if (leaf_idx == column_index) {
                return elem;
            }
            leaf_idx += 1;
        }
    }
    return null;
}

