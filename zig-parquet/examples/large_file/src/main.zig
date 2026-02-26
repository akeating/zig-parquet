//! Large File Roundtrip Example
//!
//! Generates a large Parquet file with diverse data types, then validates
//! it can be read back using DynamicReader.
//!
//! Run with: zig build run-large_file_roundtrip
//!
//! This is useful for stress testing and validating the library handles
//! multi-page, multi-row-group files correctly.

const std = @import("std");
const parquet = @import("parquet");
const DynamicReader = parquet.DynamicReader;
const Timestamp = parquet.TimestampMicros;
const Date = parquet.Date;
const Time = parquet.TimeMicros;
const Uuid = parquet.Uuid;
const Interval = parquet.Interval;

/// Comprehensive struct with all supported data types
const LargeRecord = struct {
    // Integer types (signed)
    id: i64,
    small_int: i32,
    medium_val: i16,
    tiny_val: i8,

    // Integer types (unsigned)
    unsigned_huge: u64,
    unsigned_large: u32,
    unsigned_medium: u16,
    unsigned_small: u8,

    // Floating point
    score: f64,
    ratio: f32,

    // Boolean
    active: bool,

    // Strings
    name: []const u8,
    description: []const u8,

    // Optional types
    maybe_count: ?i32,
    maybe_score: ?f64,
    maybe_name: ?[]const u8,

    // Nested struct
    metadata: Metadata,

    // Lists
    tags: []const []const u8,

    // Logical types
    created_at: Timestamp,
    birth_date: Date,
    event_time: Time,
    uuid: Uuid,
    duration: Interval,
};

const Metadata = struct {
    version: i32,
    source: []const u8,
};

// Pre-generated names and descriptions for variety
const names = [_][]const u8{
    "Alice", "Bob",  "Charlie", "Diana", "Eve",  "Frank",  "Grace",  "Henry",
    "Ivy",   "Jack", "Kate",    "Leo",   "Mia",  "Noah",   "Olivia", "Paul",
    "Quinn", "Rose", "Sam",     "Tara",  "Uma",  "Victor", "Wendy",  "Xavier",
    "Yara",  "Zack", "Anna",    "Ben",   "Cara", "Dan",    "Emma",   "Finn",
};

const descriptions = [_][]const u8{
    "A short description",
    "This is a medium-length description with some details",
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
    "Quick brown fox",
    "The quick brown fox jumps over the lazy dog.",
    "Testing 123",
    "Another test entry with various characters",
    "Unicode test: cafe naive resume",
};

const sources = [_][]const u8{
    "api", "web", "mobile", "batch", "stream", "import", "manual", "sync",
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const output_path = "test_large_file.parquet";
    defer std.fs.cwd().deleteFile(output_path) catch {};

    // Target rows - use a moderate count for testing
    // Increase to 100_000+ for real stress testing
    const target_rows: usize = 10_000;
    const flush_interval: usize = 2_500; // Create multiple row groups

    std.debug.print("\n=== Generating large Parquet file ===\n", .{});
    std.debug.print("Target: {} rows, flush every {} rows\n", .{ target_rows, flush_interval });

    // Write phase
    {
        const file = try std.fs.cwd().createFile(output_path, .{});
        defer file.close();

        var writer = try parquet.writeToFileRows(LargeRecord, allocator, file, .{
            .compression = .zstd,
            .use_dictionary = true,
            .max_page_size = 1024 * 1024, // 1MB pages
        });
        defer writer.deinit();

        var prng = std.Random.DefaultPrng.init(42);
        const rand = prng.random();

        var rows_written: usize = 0;

        while (rows_written < target_rows) {
            const name_idx = rows_written % names.len;
            const desc_idx = (rows_written / 7) % descriptions.len;
            const source_idx = (rows_written / 13) % sources.len;

            // Nullable fields: null every 5th/7th/11th row
            const maybe_count: ?i32 = if (rows_written % 5 == 0) null else @intCast(rows_written % 1000);
            const maybe_score: ?f64 = if (rows_written % 7 == 0) null else @as(f64, @floatFromInt(rows_written % 100)) / 10.0;
            const maybe_name: ?[]const u8 = if (rows_written % 11 == 0) null else names[name_idx];

            // Generate UUID bytes
            var uuid_bytes: [16]u8 = undefined;
            rand.bytes(&uuid_bytes);

            // Tags list - vary the number of tags
            const tag_count = rows_written % 4;
            const tags: []const []const u8 = switch (tag_count) {
                0 => &[_][]const u8{},
                1 => &[_][]const u8{"alpha"},
                2 => &[_][]const u8{ "alpha", "beta" },
                else => &[_][]const u8{ "alpha", "beta", "gamma" },
            };

            const row = LargeRecord{
                .id = @intCast(rows_written),
                .small_int = @intCast(rows_written % 10000),
                .medium_val = @intCast(rows_written % 30000),
                .tiny_val = @intCast(rows_written % 127),

                .unsigned_huge = @intCast(rows_written * 1000000),
                .unsigned_large = @intCast(rows_written % 1000000),
                .unsigned_medium = @intCast(rows_written % 65000),
                .unsigned_small = @intCast(rows_written % 255),

                .score = @as(f64, @floatFromInt(rows_written)) * 0.001,
                .ratio = @as(f32, @floatFromInt(rows_written % 1000)) / 100.0,

                .active = rows_written % 2 == 0,

                .name = names[name_idx],
                .description = descriptions[desc_idx],

                .maybe_count = maybe_count,
                .maybe_score = maybe_score,
                .maybe_name = maybe_name,

                .metadata = .{
                    .version = @intCast((rows_written / 1000) % 10 + 1),
                    .source = sources[source_idx],
                },

                .tags = tags,

                .created_at = .{ .value = @intCast(1700000000000000 + rows_written * 1000) },
                .birth_date = .{ .days = @intCast(10000 + (rows_written % 20000)) },
                .event_time = .{ .value = @intCast((rows_written % 86400) * 1000000) }, // Time of day (micros since midnight)
                .uuid = .{ .bytes = uuid_bytes },
                .duration = .{ .months = @intCast(rows_written % 12), .days = @intCast(rows_written % 30), .millis = @intCast(rows_written % 1000) },
            };

            try writer.writeRow(row);
            rows_written += 1;

            // Flush periodically to create multiple row groups
            if (rows_written % flush_interval == 0) {
                try writer.flush();
            }
        }

        try writer.close();
        std.debug.print("Wrote {} rows\n", .{rows_written});
    }

    // Validation phase using DynamicReader
    std.debug.print("\n=== Validating with DynamicReader ===\n", .{});

    {
        const file = try std.fs.cwd().openFile(output_path, .{});
        defer file.close();

        var reader = try parquet.openFileDynamic(allocator, file, .{});
        defer reader.deinit();

        const num_row_groups = reader.getNumRowGroups();
        const total_rows = reader.getTotalNumRows();
        const num_columns = reader.getNumColumns();

        std.debug.print("File has {} row groups, {} total rows, {} columns\n", .{
            num_row_groups,
            total_rows,
            num_columns,
        });

        if (total_rows != target_rows) {
            std.debug.print("ERROR: Expected {} rows, got {}\n", .{ target_rows, total_rows });
            return error.ValidationFailed;
        }

        // Read and validate each row group
        var total_read: usize = 0;
        for (0..num_row_groups) |rg_idx| {
            const rows = try reader.readAllRows(rg_idx);
            defer {
                for (rows) |row| row.deinit();
                allocator.free(rows);
            }

            total_read += rows.len;

            // Verify each row has the expected number of columns
            for (rows) |row| {
                if (row.columnCount() != num_columns) {
                    std.debug.print("ERROR: Row has {} columns, expected {}\n", .{ row.columnCount(), num_columns });
                    return error.ValidationFailed;
                }
            }

            std.debug.print("  Row group {}: {} rows OK\n", .{ rg_idx, rows.len });
        }

        if (total_read != target_rows) {
            std.debug.print("ERROR: Read {} rows, expected {}\n", .{ total_read, target_rows });
            return error.ValidationFailed;
        }

        std.debug.print("\nValidation complete: {} rows verified\n", .{total_read});
    }

    std.debug.print("\nSUCCESS: Large file roundtrip test passed!\n", .{});
}
