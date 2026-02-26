//! pqi - Parquet file inspection CLI
//!
//! Commands:
//!   pqi schema <file>                   Show schema
//!   pqi head <file> [-n NUM]            Show first N rows (default 5)
//!   pqi cat <file> [--json]             Output all rows
//!   pqi stats <file>                    Show file statistics
//!   pqi count <file>                    Print row count
//!   pqi rowgroups <file>                Show row group details
//!   pqi size <file>                     Show file size breakdown
//!   pqi column <file> [columns...]      Column detail across row groups
//!   pqi validate <file>                 Validate file integrity

const std = @import("std");
const parquet = @import("parquet");
const build_options = @import("build_options");

const version = build_options.version;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        try printUsage();
        std.process.exit(1);
    }

    const command = args[1];

    if (std.mem.eql(u8, command, "schema")) {
        try runSchema(allocator, args[2..]);
    } else if (std.mem.eql(u8, command, "head")) {
        try runHead(allocator, args[2..]);
    } else if (std.mem.eql(u8, command, "cat")) {
        try runCat(allocator, args[2..]);
    } else if (std.mem.eql(u8, command, "stats")) {
        try runStats(allocator, args[2..]);
    } else if (std.mem.eql(u8, command, "count")) {
        try runCount(allocator, args[2..]);
    } else if (std.mem.eql(u8, command, "rowgroups")) {
        try runRowgroups(allocator, args[2..]);
    } else if (std.mem.eql(u8, command, "size")) {
        try runSize(allocator, args[2..]);
    } else if (std.mem.eql(u8, command, "column")) {
        try runColumn(allocator, args[2..]);
    } else if (std.mem.eql(u8, command, "validate")) {
        try runValidate(allocator, args[2..]);
    } else if (std.mem.eql(u8, command, "version") or std.mem.eql(u8, command, "--version")) {
        try printVersion();
    } else if (std.mem.eql(u8, command, "--help") or std.mem.eql(u8, command, "-h")) {
        try printUsage();
    } else {
        var buf: [4096]u8 = undefined;
        var file_writer = std.fs.File.stderr().writer(&buf);
        try file_writer.interface.print("Unknown command: {s}\n\n", .{command});
        try file_writer.interface.flush();
        try printUsage();
        std.process.exit(1);
    }
}

fn printUsage() !void {
    var buf: [4096]u8 = undefined;
    var file_writer = std.fs.File.stdout().writer(&buf);
    const w = &file_writer.interface;
    try w.writeAll(
        \\pqi - Parquet file inspection tool
        \\
        \\Usage: pqi <command> <file> [options]
        \\
        \\Commands:
        \\  schema <file>              Show the file schema
        \\  head <file> [-n NUM]       Show first N rows (default: 5)
        \\  cat <file> [--json]        Output all rows (table or JSON format)
        \\  stats <file>               Show file statistics
        \\  count <file>               Print row count
        \\  rowgroups <file>           Show per-row-group details
        \\  size <file>                Show file size breakdown with percentages
        \\  column <file> [cols...]    Column detail across row groups
        \\  validate <file>            Validate file integrity (incl. CRC checksums)
        \\
        \\Validate options:
        \\  --no-checksum              Skip CRC32 page checksum validation
        \\
        \\Options:
        \\  -h, --help                 Show this help message
        \\  --version                  Show version
        \\
        \\Examples:
        \\  pqi schema data.parquet
        \\  pqi head data.parquet -n 10
        \\  pqi cat data.parquet --json
        \\  pqi stats data.parquet
        \\  pqi count data.parquet
        \\  pqi column data.parquet price quantity
        \\  pqi validate data.parquet
        \\
    );
    try w.flush();
}

fn printVersion() !void {
    var buf: [256]u8 = undefined;
    var file_writer = std.fs.File.stdout().writer(&buf);
    try file_writer.interface.print("pqi {s}\n", .{version});
    try file_writer.interface.flush();
}

fn runSchema(allocator: std.mem.Allocator, args: []const []const u8) !void {
    if (args.len < 1) {
        try stderrExit("Error: schema command requires a file path\n");
    }
    const schema_cmd = @import("commands/schema.zig");
    try schema_cmd.run(allocator, args[0]);
}

fn runHead(allocator: std.mem.Allocator, args: []const []const u8) !void {
    if (args.len < 1) {
        try stderrExit("Error: head command requires a file path\n");
    }

    const file_path = args[0];
    var num_rows: usize = 5;

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "-n")) {
            if (i + 1 < args.len) {
                num_rows = std.fmt.parseInt(usize, args[i + 1], 10) catch {
                    var buf: [4096]u8 = undefined;
                    var file_writer = std.fs.File.stderr().writer(&buf);
                    try file_writer.interface.print("Error: invalid number '{s}'\n", .{args[i + 1]});
                    try file_writer.interface.flush();
                    std.process.exit(1);
                };
                i += 1;
            }
        }
    }

    const head_cmd = @import("commands/head.zig");
    try head_cmd.run(allocator, file_path, num_rows);
}

fn runCat(allocator: std.mem.Allocator, args: []const []const u8) !void {
    if (args.len < 1) {
        try stderrExit("Error: cat command requires a file path\n");
    }

    const file_path = args[0];
    var json_mode = false;

    for (args[1..]) |arg| {
        if (std.mem.eql(u8, arg, "--json")) {
            json_mode = true;
        }
    }

    const cat_cmd = @import("commands/cat.zig");
    try cat_cmd.run(allocator, file_path, json_mode);
}

fn runStats(allocator: std.mem.Allocator, args: []const []const u8) !void {
    if (args.len < 1) {
        try stderrExit("Error: stats command requires a file path\n");
    }
    const stats_cmd = @import("commands/stats.zig");
    try stats_cmd.run(allocator, args[0]);
}

fn runCount(allocator: std.mem.Allocator, args: []const []const u8) !void {
    if (args.len < 1) {
        try stderrExit("Error: count command requires a file path\n");
    }
    const count_cmd = @import("commands/count.zig");
    try count_cmd.run(allocator, args[0]);
}

fn runRowgroups(allocator: std.mem.Allocator, args: []const []const u8) !void {
    if (args.len < 1) {
        try stderrExit("Error: rowgroups command requires a file path\n");
    }
    const rowgroups_cmd = @import("commands/rowgroups.zig");
    try rowgroups_cmd.run(allocator, args[0]);
}

fn runSize(allocator: std.mem.Allocator, args: []const []const u8) !void {
    if (args.len < 1) {
        try stderrExit("Error: size command requires a file path\n");
    }
    const size_cmd = @import("commands/size.zig");
    try size_cmd.run(allocator, args[0]);
}

fn runColumn(allocator: std.mem.Allocator, args: []const []const u8) !void {
    if (args.len < 1) {
        try stderrExit("Error: column command requires a file path\n");
    }
    const column_cmd = @import("commands/column.zig");
    try column_cmd.run(allocator, args[0], args[1..]);
}

fn runValidate(allocator: std.mem.Allocator, args: []const []const u8) !void {
    if (args.len < 1) {
        try stderrExit("Error: validate command requires a file path\n");
    }

    var file_path: []const u8 = "";
    var validate_checksum = true;

    for (args) |arg| {
        if (std.mem.eql(u8, arg, "--no-checksum")) {
            validate_checksum = false;
        } else if (!std.mem.startsWith(u8, arg, "-")) {
            file_path = arg;
        }
    }

    if (file_path.len == 0) {
        try stderrExit("Error: validate command requires a file path\n");
    }

    const validate_cmd = @import("commands/validate.zig");
    try validate_cmd.runWithOptions(allocator, file_path, .{
        .validate_checksum = validate_checksum,
    });
}

fn stderrExit(msg: []const u8) !noreturn {
    var buf: [4096]u8 = undefined;
    var file_writer = std.fs.File.stderr().writer(&buf);
    try file_writer.interface.writeAll(msg);
    try file_writer.interface.flush();
    std.process.exit(1);
}
