//! Integration tests for pqi CLI
//! These tests run the compiled pqi binary against real parquet files

const std = @import("std");
const testing = std.testing;

const test_file_basic = "../test-files-arrow/basic/basic_types_plain_uncompressed.parquet";
const test_file_multi_rg = "../test-files-arrow/structure/multiple_row_groups.parquet";

fn runPq(allocator: std.mem.Allocator, args: []const []const u8) !struct {
    stdout: []const u8,
    stderr: []const u8,
    term: std.process.Child.Term,
} {
    var argv: std.ArrayList([]const u8) = .empty;
    defer argv.deinit(allocator);

    try argv.append(allocator, "./zig-out/bin/pqi");
    for (args) |arg| {
        try argv.append(allocator, arg);
    }

    var child = std.process.Child.init(argv.items, allocator);
    child.cwd = null; // Use current working directory

    child.stdout_behavior = .Pipe;
    child.stderr_behavior = .Pipe;

    try child.spawn();

    var stdout_buf: std.ArrayList(u8) = .empty;
    var stderr_buf: std.ArrayList(u8) = .empty;
    errdefer stdout_buf.deinit(allocator);
    errdefer stderr_buf.deinit(allocator);

    child.collectOutput(allocator, &stdout_buf, &stderr_buf, 1024 * 1024) catch {
        return error.CollectOutputFailed;
    };

    const term = try child.wait();

    return .{
        .stdout = try stdout_buf.toOwnedSlice(allocator),
        .stderr = try stderr_buf.toOwnedSlice(allocator),
        .term = term,
    };
}

// ============================================================================
// Help tests
// ============================================================================

test "pq --help shows usage" {
    const allocator = testing.allocator;

    const result = try runPq(allocator, &.{"--help"});
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    try testing.expect(result.term == .Exited);
    try testing.expectEqual(@as(u8, 0), result.term.Exited);
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "pqi - Parquet file inspection tool"));
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "schema <file>"));
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "head <file>"));
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "cat <file>"));
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "stats <file>"));
}

test "pq -h shows usage" {
    const allocator = testing.allocator;

    const result = try runPq(allocator, &.{"-h"});
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    try testing.expect(result.term == .Exited);
    try testing.expectEqual(@as(u8, 0), result.term.Exited);
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "Usage:"));
}

// ============================================================================
// Schema command tests
// ============================================================================

test "pq schema shows column tree" {
    const allocator = testing.allocator;

    const result = try runPq(allocator, &.{ "schema", test_file_basic });
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    try testing.expect(result.term == .Exited);
    try testing.expectEqual(@as(u8, 0), result.term.Exited);

    // Check for expected schema elements
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "8 columns"));
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "bool_col: BOOLEAN"));
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "int32_col: INT32"));
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "int64_col: INT64"));
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "float_col: FLOAT"));
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "double_col: DOUBLE"));
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "string_col: BYTE_ARRAY"));

    // Check tree formatting
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "├──") or std.mem.containsAtLeast(u8, result.stdout, 1, "└──"));
}

test "pq schema shows fixed length info" {
    const allocator = testing.allocator;

    const result = try runPq(allocator, &.{ "schema", test_file_basic });
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    try testing.expect(result.term == .Exited);
    try testing.expectEqual(@as(u8, 0), result.term.Exited);

    // Should show fixed_len_byte_array with length
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "FIXED_LEN_BYTE_ARRAY"));
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "[len=16]"));
}

test "pq schema errors on missing file" {
    const allocator = testing.allocator;

    const result = try runPq(allocator, &.{ "schema", "nonexistent.parquet" });
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    try testing.expect(result.term == .Exited);
    try testing.expect(result.term.Exited != 0);
    try testing.expect(std.mem.containsAtLeast(u8, result.stderr, 1, "Error"));
}

// ============================================================================
// Stats command tests
// ============================================================================

test "pq stats shows file metadata" {
    const allocator = testing.allocator;

    const result = try runPq(allocator, &.{ "stats", test_file_basic });
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    try testing.expect(result.term == .Exited);
    try testing.expectEqual(@as(u8, 0), result.term.Exited);

    // Check for expected stats
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "File:"));
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "Size:"));
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "Rows: 5"));
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "Row Groups: 1"));
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "Columns: 8"));
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "Compression:"));
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "Created by:"));
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "Format version:"));
}

test "pq stats shows multiple row groups" {
    const allocator = testing.allocator;

    const result = try runPq(allocator, &.{ "stats", test_file_multi_rg });
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    try testing.expect(result.term == .Exited);
    try testing.expectEqual(@as(u8, 0), result.term.Exited);

    // Check for row group details
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "Row Groups: 10"));
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "Row Groups:"));
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "[ 0]"));
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "rows"));
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "bytes"));
}

// ============================================================================
// Head command tests
// ============================================================================

test "pq head shows default 5 rows" {
    const allocator = testing.allocator;

    const result = try runPq(allocator, &.{ "head", test_file_basic });
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    try testing.expect(result.term == .Exited);
    try testing.expectEqual(@as(u8, 0), result.term.Exited);

    // Check for table structure
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "|"));
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "bool_col"));
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "int32_col"));
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "---")); // separator

    // Should show actual data
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "true") or std.mem.containsAtLeast(u8, result.stdout, 1, "false"));
}

test "pq head -n limits rows" {
    const allocator = testing.allocator;

    const result = try runPq(allocator, &.{ "head", test_file_basic, "-n", "2" });
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    try testing.expect(result.term == .Exited);
    try testing.expectEqual(@as(u8, 0), result.term.Exited);

    // Count data rows (lines after header separator)
    var line_count: usize = 0;
    var found_separator = false;
    var lines = std.mem.splitScalar(u8, result.stdout, '\n');
    while (lines.next()) |line| {
        if (std.mem.containsAtLeast(u8, line, 1, "---")) {
            found_separator = true;
            continue;
        }
        if (found_separator and line.len > 0 and line[0] == '|') {
            line_count += 1;
        }
    }

    try testing.expectEqual(@as(usize, 2), line_count);
}

test "pq head handles large -n gracefully" {
    const allocator = testing.allocator;

    // Request more rows than exist
    const result = try runPq(allocator, &.{ "head", test_file_basic, "-n", "1000" });
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    try testing.expect(result.term == .Exited);
    try testing.expectEqual(@as(u8, 0), result.term.Exited);

    // Should show all 5 rows without error
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "bool_col"));
}

// ============================================================================
// Cat command tests
// ============================================================================

test "pq cat outputs table format by default" {
    const allocator = testing.allocator;

    const result = try runPq(allocator, &.{ "cat", test_file_basic });
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    try testing.expect(result.term == .Exited);
    try testing.expectEqual(@as(u8, 0), result.term.Exited);

    // Check for table structure
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "|"));
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "---"));
}

test "pq cat --json outputs NDJSON" {
    const allocator = testing.allocator;

    const result = try runPq(allocator, &.{ "cat", test_file_basic, "--json" });
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    try testing.expect(result.term == .Exited);
    try testing.expectEqual(@as(u8, 0), result.term.Exited);

    // Check for JSON structure
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "{"));
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "}"));
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "\"bool_col\":"));
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "\"int32_col\":"));

    // Should NOT have table elements
    try testing.expect(!std.mem.containsAtLeast(u8, result.stdout, 1, "---"));

    // Each line should be valid JSON (ends with })
    var lines = std.mem.splitScalar(u8, result.stdout, '\n');
    var json_count: usize = 0;
    while (lines.next()) |line| {
        if (line.len > 0 and line[0] == '{') {
            try testing.expect(line[line.len - 1] == '}');
            json_count += 1;
        }
    }
    try testing.expectEqual(@as(usize, 5), json_count); // 5 rows
}

test "pq cat --json has correct boolean values" {
    const allocator = testing.allocator;

    const result = try runPq(allocator, &.{ "cat", test_file_basic, "--json" });
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    try testing.expect(result.term == .Exited);

    // JSON booleans should be lowercase without quotes
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, "\"bool_col\":true") or
        std.mem.containsAtLeast(u8, result.stdout, 1, "\"bool_col\":false"));

    // Should have null values for nullable fields
    try testing.expect(std.mem.containsAtLeast(u8, result.stdout, 1, ":null"));
}

// ============================================================================
// Error handling tests
// ============================================================================

test "pq with no args shows usage" {
    const allocator = testing.allocator;

    const result = try runPq(allocator, &.{});
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    try testing.expect(result.term == .Exited);
    try testing.expect(result.term.Exited != 0);
}

test "pq with unknown command shows error" {
    const allocator = testing.allocator;

    const result = try runPq(allocator, &.{"unknown"});
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    try testing.expect(result.term == .Exited);
    try testing.expect(result.term.Exited != 0);
    try testing.expect(std.mem.containsAtLeast(u8, result.stderr, 1, "Unknown command"));
}

test "pq schema without file shows error" {
    const allocator = testing.allocator;

    const result = try runPq(allocator, &.{"schema"});
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    try testing.expect(result.term == .Exited);
    try testing.expect(result.term.Exited != 0);
    try testing.expect(std.mem.containsAtLeast(u8, result.stderr, 1, "Error"));
}
