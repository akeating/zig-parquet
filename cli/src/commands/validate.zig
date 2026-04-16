//! Validate command - validates parquet file integrity
//!
//! Checks:
//! - File can be opened
//! - Magic bytes (PAR1) at start and end
//! - Metadata can be parsed
//! - All columns in all row groups can be decoded
//! - CRC32 page checksums (if present)

const std = @import("std");
const parquet = @import("parquet");

/// Options for the validate command
pub const ValidateOptions = struct {
    /// Whether to validate page CRC32 checksums (default: true)
    validate_checksum: bool = true,
};

pub fn run(allocator: std.mem.Allocator, io: std.Io, file_path: []const u8) !void {
    return runWithOptions(allocator, io, file_path, .{});
}

pub fn runWithOptions(allocator: std.mem.Allocator, io: std.Io, file_path: []const u8, options: ValidateOptions) !void {
    var stdout_buf: [8192]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writerStreaming(io, &stdout_buf);
    const stdout = &stdout_writer.interface;

    var stderr_buf: [4096]u8 = undefined;
    var stderr_writer = std.Io.File.stderr().writerStreaming(io, &stderr_buf);
    const stderr = &stderr_writer.interface;
    _ = stderr;

    try stdout.print("Validating: {s}\n", .{file_path});

    // Step 1: Open the file
    const file = std.Io.Dir.cwd().openFile(io, file_path, .{}) catch |err| {
        try stdout.print("  File access: FAILED ({s})\n", .{@errorName(err)});
        try stdout.writeAll("Invalid\n");
        try stdout.flush();
        std.process.exit(1);
    };
    defer file.close(io);

    // Step 2: Check magic bytes
    const magic_ok = checkMagicBytes(file, io) catch false;
    if (!magic_ok) {
        try stdout.writeAll("  Magic bytes: FAILED (not a valid parquet file)\n");
        try stdout.writeAll("Invalid\n");
        try stdout.flush();
        std.process.exit(1);
    }
    try stdout.writeAll("  Magic bytes: OK\n");

    // Step 3: Initialize DynamicReader (parses metadata)
    // Enable CRC checksum validation by default for validation command
    var reader = parquet.openFileDynamic(allocator, file, io, .{
        .checksum = .{
            .validate_page_checksum = options.validate_checksum,
            .strict_checksum = false, // Missing CRC is OK
        },
    }) catch |err| {
        try stdout.print("  Metadata: FAILED ({s})\n", .{@errorName(err)});
        try stdout.writeAll("Invalid\n");
        try stdout.flush();
        std.process.exit(1);
    };
    defer reader.deinit();

    const num_columns = reader.getNumColumns();
    const num_row_groups = reader.getNumRowGroups();

    try stdout.print("  Metadata: OK ({} columns, {} rows, {} row groups)\n", .{
        num_columns,
        reader.getTotalNumRows(),
        num_row_groups,
    });

    if (options.validate_checksum) {
        try stdout.writeAll("  Checksums: enabled\n");
    } else {
        try stdout.writeAll("  Checksums: disabled\n");
    }

    // Step 4: Validate each row group by reading all data
    var all_valid = true;
    for (0..num_row_groups) |rg_idx| {
        const rows = reader.readAllRows(rg_idx) catch |err| {
            try stdout.print("  Row group {}: FAILED ({s})\n", .{ rg_idx, @errorName(err) });
            all_valid = false;
            continue;
        };
        defer {
            for (rows) |row| row.deinit();
            allocator.free(rows);
        }

        const expected_rows: usize = @intCast(reader.getRowGroupNumRows(rg_idx));
        if (rows.len == expected_rows) {
            try stdout.print("  Row group {}: OK ({} rows, {} columns)\n", .{
                rg_idx,
                rows.len,
                num_columns,
            });
        } else {
            try stdout.print("  Row group {}: FAILED (got {} rows, expected {})\n", .{
                rg_idx,
                rows.len,
                expected_rows,
            });
            all_valid = false;
        }
    }

    if (all_valid) {
        try stdout.writeAll("Valid\n");
        try stdout.flush();
    } else {
        try stdout.writeAll("Invalid\n");
        try stdout.flush();
        std.process.exit(1);
    }
}

fn checkMagicBytes(file: std.Io.File, io: std.Io) !bool {
    const MAGIC = "PAR1";

    // Check start magic using positional reads
    var start_buf: [4]u8 = undefined;
    const start_read = file.readPositionalAll(io, &start_buf, 0) catch return false;
    if (start_read != 4) return false;
    if (!std.mem.eql(u8, &start_buf, MAGIC)) return false;

    // Check end magic
    const end_pos = file.length(io) catch return false;
    if (end_pos < 8) return false;
    var end_buf: [4]u8 = undefined;
    const end_read = file.readPositionalAll(io, &end_buf, end_pos - 4) catch return false;
    if (end_read != 4) return false;
    if (!std.mem.eql(u8, &end_buf, MAGIC)) return false;

    return true;
}
