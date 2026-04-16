//! Schema command - displays parquet file schema in tree format

const std = @import("std");
const parquet = @import("parquet");
const helpers = @import("../helpers.zig");

pub fn run(allocator: std.mem.Allocator, io: std.Io, file_path: []const u8) !void {
    var stdout_buf: [8192]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writerStreaming(io, &stdout_buf);
    const stdout = &stdout_writer.interface;

    var stderr_buf: [4096]u8 = undefined;
    var stderr_writer = std.Io.File.stderr().writerStreaming(io, &stderr_buf);
    const stderr = &stderr_writer.interface;

    // Open the file
    const file = std.Io.Dir.cwd().openFile(io, file_path, .{}) catch |err| {
        try stderr.print("Error: Cannot open file '{s}': {}\n", .{ file_path, err });
        try stderr.flush();
        std.process.exit(1);
    };
    defer file.close(io);

    var reader = parquet.openFileDynamic(allocator, file, io, .{}) catch |err| {
        try stderr.print("Error: Cannot read parquet file: {}\n", .{err});
        try stderr.flush();
        std.process.exit(1);
    };
    defer reader.deinit();

    const schema = reader.getSchema();

    // Find the root element (first element with num_children set)
    var root_idx: usize = 0;
    for (schema, 0..) |elem, i| {
        if (elem.num_children != null) {
            root_idx = i;
            break;
        }
    }

    // Print root
    const root = schema[root_idx];
    const num_children = root.num_children orelse 0;
    try stdout.print("{s} ({} columns)\n", .{ root.name, num_children });

    // Print children (leaf columns)
    var child_count: usize = 0;
    var logical_type_buf: [64]u8 = undefined;
    for (schema[root_idx + 1 ..]) |elem| {
        if (elem.type_ != null and elem.num_children == null) {
            child_count += 1;
            const is_last = child_count == num_children;
            const prefix = if (is_last) "└── " else "├── ";

            const type_name = helpers.physicalTypeToString(elem.type_.?);
            const repetition = helpers.repetitionToString(elem.repetition_type);

            try stdout.print("{s}{s}: {s}", .{ prefix, elem.name, type_name });

            if (elem.logical_type) |lt| {
                const lt_str = lt.toDetailedString(&logical_type_buf);
                try stdout.print("/{s}", .{lt_str});
            }

            try stdout.print(" ({s})", .{repetition});

            if (elem.type_ == .fixed_len_byte_array) {
                if (elem.type_length) |len| {
                    try stdout.print(" [len={}]", .{len});
                }
            }

            try stdout.writeAll("\n");
        }
    }
    try stdout.flush();
}
