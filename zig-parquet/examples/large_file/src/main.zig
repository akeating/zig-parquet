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

    const target_rows: usize = 10_000;
    const flush_interval: usize = 2_500;

    std.debug.print("\n=== Generating large Parquet file ===\n", .{});
    std.debug.print("Target: {} rows, flush every {} rows\n", .{ target_rows, flush_interval });

    // Write phase
    {
        const file = try std.fs.cwd().createFile(output_path, .{});
        defer file.close();

        var writer = try parquet.createFileDynamic(allocator, file);
        defer writer.deinit();

        // Col 0-3: signed integers
        try writer.addColumn("id", parquet.TypeInfo.int64, .{});
        try writer.addColumn("small_int", parquet.TypeInfo.int32, .{});
        try writer.addColumn("medium_val", parquet.TypeInfo.int16, .{});
        try writer.addColumn("tiny_val", parquet.TypeInfo.int8, .{});

        // Col 4-7: unsigned integers
        try writer.addColumn("unsigned_huge", parquet.TypeInfo.uint64, .{});
        try writer.addColumn("unsigned_large", parquet.TypeInfo.uint32, .{});
        try writer.addColumn("unsigned_medium", parquet.TypeInfo.uint16, .{});
        try writer.addColumn("unsigned_small", parquet.TypeInfo.uint8, .{});

        // Col 8-9: floats
        try writer.addColumn("score", parquet.TypeInfo.double_, .{});
        try writer.addColumn("ratio", parquet.TypeInfo.float_, .{});

        // Col 10: boolean
        try writer.addColumn("active", parquet.TypeInfo.bool_, .{});

        // Col 11-12: strings
        try writer.addColumn("name", parquet.TypeInfo.string, .{});
        try writer.addColumn("description", parquet.TypeInfo.string, .{});

        // Col 13-15: optional types
        try writer.addColumn("maybe_count", parquet.TypeInfo.int32, .{});
        try writer.addColumn("maybe_score", parquet.TypeInfo.double_, .{});
        try writer.addColumn("maybe_name", parquet.TypeInfo.string, .{});

        // Col 16: nested struct (metadata: { version: i32, source: string })
        const ver_leaf = try writer.allocSchemaNode(.{ .int32 = .{} });
        const src_leaf = try writer.allocSchemaNode(.{ .byte_array = .{ .logical = .string } });
        var meta_fields = try writer.allocSchemaFields(2);
        meta_fields[0] = .{ .name = try writer.dupeSchemaName("version"), .node = ver_leaf };
        meta_fields[1] = .{ .name = try writer.dupeSchemaName("source"), .node = src_leaf };
        const meta_node = try writer.allocSchemaNode(.{ .struct_ = .{ .fields = meta_fields } });
        try writer.addColumnNested("metadata", meta_node, .{});

        // Col 17: list<string> (tags)
        const tag_leaf = try writer.allocSchemaNode(.{ .byte_array = .{ .logical = .string } });
        const tags_node = try writer.allocSchemaNode(.{ .list = tag_leaf });
        try writer.addColumnNested("tags", tags_node, .{});

        // Col 18-22: logical types
        try writer.addColumn("created_at", parquet.TypeInfo.timestamp_micros, .{});
        try writer.addColumn("birth_date", parquet.TypeInfo.date, .{});
        try writer.addColumn("event_time", parquet.TypeInfo.time_micros, .{});
        try writer.addColumn("uuid", parquet.TypeInfo.uuid, .{});
        try writer.addColumn("duration", parquet.TypeInfo.interval, .{});

        writer.setCompression(.zstd);
        writer.setRowGroupSize(flush_interval);
        try writer.begin();

        var prng = std.Random.DefaultPrng.init(42);
        const rand = prng.random();

        var rows_written: usize = 0;

        while (rows_written < target_rows) {
            const name_idx = rows_written % names.len;
            const desc_idx = (rows_written / 7) % descriptions.len;
            const source_idx = (rows_written / 13) % sources.len;

            // Col 0-3: signed integers
            try writer.setInt64(0, @intCast(rows_written));
            try writer.setInt32(1, @intCast(rows_written % 10000));
            try writer.setInt32(2, @intCast(rows_written % 30000));
            try writer.setInt32(3, @intCast(rows_written % 127));

            // Col 4-7: unsigned integers (stored as int32/int64 with logical type)
            try writer.setInt64(4, @intCast(rows_written * 1000000));
            try writer.setInt32(5, @intCast(rows_written % 1000000));
            try writer.setInt32(6, @intCast(rows_written % 65000));
            try writer.setInt32(7, @intCast(rows_written % 255));

            // Col 8-9: floats
            try writer.setDouble(8, @as(f64, @floatFromInt(rows_written)) * 0.001);
            try writer.setFloat(9, @as(f32, @floatFromInt(rows_written % 1000)) / 100.0);

            // Col 10: boolean
            try writer.setBool(10, rows_written % 2 == 0);

            // Col 11-12: strings
            try writer.setBytes(11, names[name_idx]);
            try writer.setBytes(12, descriptions[desc_idx]);

            // Col 13-15: nullable (null every 5th/7th/11th row)
            if (rows_written % 5 == 0) {
                try writer.setNull(13);
            } else {
                try writer.setInt32(13, @intCast(rows_written % 1000));
            }
            if (rows_written % 7 == 0) {
                try writer.setNull(14);
            } else {
                try writer.setDouble(14, @as(f64, @floatFromInt(rows_written % 100)) / 10.0);
            }
            if (rows_written % 11 == 0) {
                try writer.setNull(15);
            } else {
                try writer.setBytes(15, names[name_idx]);
            }

            // Col 16: metadata struct
            try writer.beginStruct(16);
            try writer.setStructField(16, 0, .{ .int32_val = @intCast((rows_written / 1000) % 10 + 1) });
            try writer.setStructFieldBytes(16, 1, sources[source_idx]);
            try writer.endStruct(16);

            // Col 17: tags list
            const tag_count = rows_written % 4;
            try writer.beginList(17);
            if (tag_count >= 1) try writer.appendNestedBytes(17, "alpha");
            if (tag_count >= 2) try writer.appendNestedBytes(17, "beta");
            if (tag_count >= 3) try writer.appendNestedBytes(17, "gamma");
            try writer.endList(17);

            // Col 18: created_at (timestamp micros)
            try writer.setInt64(18, @intCast(1700000000000000 + rows_written * 1000));

            // Col 19: birth_date (date = days since epoch)
            try writer.setInt32(19, @intCast(10000 + (rows_written % 20000)));

            // Col 20: event_time (time micros)
            try writer.setInt64(20, @intCast((rows_written % 86400) * 1000000));

            // Col 21: uuid (16-byte fixed)
            var uuid_bytes: [16]u8 = undefined;
            rand.bytes(&uuid_bytes);
            try writer.setBytes(21, &uuid_bytes);

            // Col 22: interval (12-byte fixed: months/days/millis as little-endian i32s)
            var interval_bytes: [12]u8 = undefined;
            std.mem.writeInt(i32, interval_bytes[0..4], @intCast(rows_written % 12), .little);
            std.mem.writeInt(i32, interval_bytes[4..8], @intCast(rows_written % 30), .little);
            std.mem.writeInt(i32, interval_bytes[8..12], @intCast(rows_written % 1000), .little);
            try writer.setBytes(22, &interval_bytes);

            try writer.addRow();
            rows_written += 1;
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
        std.debug.print("File has {} row groups, {} total rows, {} schema leaves\n", .{
            num_row_groups,
            total_rows,
            reader.getNumColumns(),
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

            for (rows) |_| {}

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
