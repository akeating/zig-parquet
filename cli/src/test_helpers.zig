//! Unit tests for helper functions

const std = @import("std");
const testing = std.testing;

// ============================================================================
// Schema command helpers
// ============================================================================

pub fn typeToString(t: PhysicalType) []const u8 {
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

pub fn repetitionToString(r: ?RepetitionType) []const u8 {
    if (r) |rep| {
        return switch (rep) {
            .required => "REQUIRED",
            .optional => "OPTIONAL",
            .repeated => "REPEATED",
        };
    }
    return "REQUIRED";
}

// ============================================================================
// Stats command helpers
// ============================================================================

pub fn codecToString(codec: CompressionCodec) []const u8 {
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

pub const FileSizeFormatted = struct {
    value: f64,
    unit: []const u8,
    is_bytes: bool,
    bytes: u64,
};

pub fn formatFileSize(bytes: u64) FileSizeFormatted {
    const units = [_][]const u8{ "B", "KB", "MB", "GB", "TB" };
    var size: f64 = @floatFromInt(bytes);
    var unit_idx: usize = 0;

    while (size >= 1024 and unit_idx < units.len - 1) {
        size /= 1024;
        unit_idx += 1;
    }

    return .{
        .value = size,
        .unit = units[unit_idx],
        .is_bytes = unit_idx == 0,
        .bytes = bytes,
    };
}

// ============================================================================
// Head/Cat command helpers
// ============================================================================

pub fn formatStringValue(allocator: std.mem.Allocator, data: []const u8) ![]const u8 {
    // Check if it's printable text
    var is_printable = true;
    for (data) |c| {
        if (c < 32 and c != '\t' and c != '\n' and c != '\r') {
            is_printable = false;
            break;
        }
    }

    if (is_printable) {
        // Truncate long strings
        if (data.len > 50) {
            return try std.fmt.allocPrint(allocator, "{s}...", .{data[0..47]});
        }
        return try allocator.dupe(u8, data);
    } else {
        // Show as hex for binary data
        if (data.len > 16) {
            var buf = try allocator.alloc(u8, 16 * 2 + 3);
            for (0..16) |i| {
                _ = std.fmt.bufPrint(buf[i * 2 ..][0..2], "{x:0>2}", .{data[i]}) catch unreachable;
            }
            buf[32] = '.';
            buf[33] = '.';
            buf[34] = '.';
            return buf;
        }
        var buf = try allocator.alloc(u8, data.len * 2);
        for (data, 0..) |b, i| {
            _ = std.fmt.bufPrint(buf[i * 2 ..][0..2], "{x:0>2}", .{b}) catch unreachable;
        }
        return buf;
    }
}

pub fn escapeJsonString(allocator: std.mem.Allocator, s: []const u8) ![]const u8 {
    // Count needed space
    var extra: usize = 0;
    for (s) |c| {
        switch (c) {
            '"', '\\', '\n', '\r', '\t' => extra += 1,
            else => if (c < 32) {
                extra += 5; // \uXXXX instead of c
            },
        }
    }

    if (extra == 0) return try allocator.dupe(u8, s);

    var result = try allocator.alloc(u8, s.len + extra);
    var i: usize = 0;
    for (s) |c| {
        switch (c) {
            '"' => {
                result[i] = '\\';
                result[i + 1] = '"';
                i += 2;
            },
            '\\' => {
                result[i] = '\\';
                result[i + 1] = '\\';
                i += 2;
            },
            '\n' => {
                result[i] = '\\';
                result[i + 1] = 'n';
                i += 2;
            },
            '\r' => {
                result[i] = '\\';
                result[i + 1] = 'r';
                i += 2;
            },
            '\t' => {
                result[i] = '\\';
                result[i + 1] = 't';
                i += 2;
            },
            else => {
                if (c < 32) {
                    _ = std.fmt.bufPrint(result[i..][0..6], "\\u{x:0>4}", .{c}) catch unreachable;
                    i += 6;
                } else {
                    result[i] = c;
                    i += 1;
                }
            },
        }
    }

    return result[0..i];
}

// ============================================================================
// Argument parsing helpers
// ============================================================================

pub const HeadArgs = struct {
    file_path: ?[]const u8,
    num_rows: usize,
};

pub fn parseHeadArgs(args: []const []const u8) HeadArgs {
    var result = HeadArgs{
        .file_path = null,
        .num_rows = 5, // default
    };

    if (args.len < 1) return result;

    result.file_path = args[0];

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "-n")) {
            if (i + 1 < args.len) {
                result.num_rows = std.fmt.parseInt(usize, args[i + 1], 10) catch 5;
                i += 1;
            }
        }
    }

    return result;
}

pub const CatArgs = struct {
    file_path: ?[]const u8,
    json_mode: bool,
};

pub fn parseCatArgs(args: []const []const u8) CatArgs {
    var result = CatArgs{
        .file_path = null,
        .json_mode = false,
    };

    if (args.len < 1) return result;

    result.file_path = args[0];

    for (args[1..]) |arg| {
        if (std.mem.eql(u8, arg, "--json")) {
            result.json_mode = true;
        }
    }

    return result;
}

// Mock types for testing (mirror parquet format types)
pub const PhysicalType = enum {
    boolean,
    int32,
    int64,
    int96,
    float,
    double,
    byte_array,
    fixed_len_byte_array,
};

pub const RepetitionType = enum {
    required,
    optional,
    repeated,
};

pub const CompressionCodec = enum {
    uncompressed,
    snappy,
    gzip,
    lzo,
    brotli,
    lz4,
    zstd,
    lz4_raw,
};

// ============================================================================
// Unit Tests
// ============================================================================

test "typeToString converts all physical types" {
    try testing.expectEqualStrings("BOOLEAN", typeToString(.boolean));
    try testing.expectEqualStrings("INT32", typeToString(.int32));
    try testing.expectEqualStrings("INT64", typeToString(.int64));
    try testing.expectEqualStrings("INT96", typeToString(.int96));
    try testing.expectEqualStrings("FLOAT", typeToString(.float));
    try testing.expectEqualStrings("DOUBLE", typeToString(.double));
    try testing.expectEqualStrings("BYTE_ARRAY", typeToString(.byte_array));
    try testing.expectEqualStrings("FIXED_LEN_BYTE_ARRAY", typeToString(.fixed_len_byte_array));
}

test "repetitionToString converts all repetition types" {
    try testing.expectEqualStrings("REQUIRED", repetitionToString(.required));
    try testing.expectEqualStrings("OPTIONAL", repetitionToString(.optional));
    try testing.expectEqualStrings("REPEATED", repetitionToString(.repeated));
    try testing.expectEqualStrings("REQUIRED", repetitionToString(null));
}

test "codecToString converts all compression codecs" {
    try testing.expectEqualStrings("UNCOMPRESSED", codecToString(.uncompressed));
    try testing.expectEqualStrings("SNAPPY", codecToString(.snappy));
    try testing.expectEqualStrings("GZIP", codecToString(.gzip));
    try testing.expectEqualStrings("LZO", codecToString(.lzo));
    try testing.expectEqualStrings("BROTLI", codecToString(.brotli));
    try testing.expectEqualStrings("LZ4", codecToString(.lz4));
    try testing.expectEqualStrings("ZSTD", codecToString(.zstd));
    try testing.expectEqualStrings("LZ4_RAW", codecToString(.lz4_raw));
}

test "formatFileSize formats bytes correctly" {
    // Bytes
    const b = formatFileSize(512);
    try testing.expect(b.is_bytes);
    try testing.expectEqualStrings("B", b.unit);
    try testing.expectEqual(@as(u64, 512), b.bytes);

    // Kilobytes
    const kb = formatFileSize(2048);
    try testing.expect(!kb.is_bytes);
    try testing.expectEqualStrings("KB", kb.unit);
    try testing.expectApproxEqRel(@as(f64, 2.0), kb.value, 0.01);

    // Megabytes
    const mb = formatFileSize(5 * 1024 * 1024);
    try testing.expectEqualStrings("MB", mb.unit);
    try testing.expectApproxEqRel(@as(f64, 5.0), mb.value, 0.01);

    // Gigabytes
    const gb = formatFileSize(3 * 1024 * 1024 * 1024);
    try testing.expectEqualStrings("GB", gb.unit);
    try testing.expectApproxEqRel(@as(f64, 3.0), gb.value, 0.01);
}

test "formatStringValue handles printable strings" {
    const allocator = testing.allocator;

    // Short printable string
    const short = try formatStringValue(allocator, "hello");
    defer allocator.free(short);
    try testing.expectEqualStrings("hello", short);

    // String with allowed control chars
    const with_tabs = try formatStringValue(allocator, "hello\tworld\n");
    defer allocator.free(with_tabs);
    try testing.expectEqualStrings("hello\tworld\n", with_tabs);
}

test "formatStringValue truncates long strings" {
    const allocator = testing.allocator;

    const long_str = "This is a very long string that exceeds fifty characters in length";
    const result = try formatStringValue(allocator, long_str);
    defer allocator.free(result);

    try testing.expect(result.len == 50);
    try testing.expect(std.mem.endsWith(u8, result, "..."));
}

test "formatStringValue converts binary to hex" {
    const allocator = testing.allocator;

    // Short binary
    const binary = try formatStringValue(allocator, &[_]u8{ 0x00, 0x01, 0x02 });
    defer allocator.free(binary);
    try testing.expectEqualStrings("000102", binary);

    // Long binary gets truncated
    const long_binary = try formatStringValue(allocator, &[_]u8{
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
        0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10, 0x11, 0x12, // extra bytes that should be truncated
    });
    defer allocator.free(long_binary);
    try testing.expectEqualStrings("000102030405060708090a0b0c0d0e0f...", long_binary);
}

test "escapeJsonString escapes special characters" {
    const allocator = testing.allocator;

    // No escaping needed
    const simple = try escapeJsonString(allocator, "hello");
    defer allocator.free(simple);
    try testing.expectEqualStrings("hello", simple);

    // Quote escaping
    const quotes = try escapeJsonString(allocator, "say \"hi\"");
    defer allocator.free(quotes);
    try testing.expectEqualStrings("say \\\"hi\\\"", quotes);

    // Backslash escaping
    const backslash = try escapeJsonString(allocator, "path\\to\\file");
    defer allocator.free(backslash);
    try testing.expectEqualStrings("path\\\\to\\\\file", backslash);

    // Newline escaping
    const newline = try escapeJsonString(allocator, "line1\nline2");
    defer allocator.free(newline);
    try testing.expectEqualStrings("line1\\nline2", newline);

    // Tab escaping
    const tab = try escapeJsonString(allocator, "col1\tcol2");
    defer allocator.free(tab);
    try testing.expectEqualStrings("col1\\tcol2", tab);
}

test "parseHeadArgs parses file path" {
    const args = [_][]const u8{"data.parquet"};
    const result = parseHeadArgs(&args);

    try testing.expectEqualStrings("data.parquet", result.file_path.?);
    try testing.expectEqual(@as(usize, 5), result.num_rows);
}

test "parseHeadArgs parses -n option" {
    const args = [_][]const u8{ "data.parquet", "-n", "10" };
    const result = parseHeadArgs(&args);

    try testing.expectEqualStrings("data.parquet", result.file_path.?);
    try testing.expectEqual(@as(usize, 10), result.num_rows);
}

test "parseHeadArgs handles missing file" {
    const args = [_][]const u8{};
    const result = parseHeadArgs(&args);

    try testing.expect(result.file_path == null);
}

test "parseHeadArgs handles invalid -n value" {
    const args = [_][]const u8{ "data.parquet", "-n", "invalid" };
    const result = parseHeadArgs(&args);

    try testing.expectEqual(@as(usize, 5), result.num_rows); // defaults to 5
}

test "parseCatArgs parses file path" {
    const args = [_][]const u8{"data.parquet"};
    const result = parseCatArgs(&args);

    try testing.expectEqualStrings("data.parquet", result.file_path.?);
    try testing.expect(!result.json_mode);
}

test "parseCatArgs parses --json flag" {
    const args = [_][]const u8{ "data.parquet", "--json" };
    const result = parseCatArgs(&args);

    try testing.expectEqualStrings("data.parquet", result.file_path.?);
    try testing.expect(result.json_mode);
}

test "parseCatArgs handles missing file" {
    const args = [_][]const u8{};
    const result = parseCatArgs(&args);

    try testing.expect(result.file_path == null);
}
