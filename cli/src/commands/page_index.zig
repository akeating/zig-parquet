//! Page-index command — show OffsetIndex and ColumnIndex for a file.
//!
//! Usage:
//!   pqi page-index <file>

const std = @import("std");
const parquet = @import("parquet");
const helpers = @import("../helpers.zig");
const cat = @import("cat.zig");

pub fn run(allocator: std.mem.Allocator, io: std.Io, file_path: []const u8) !void {
    var stdout_buf: [8192]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writerStreaming(io, &stdout_buf);
    const stdout = &stdout_writer.interface;

    var stderr_buf: [4096]u8 = undefined;
    var stderr_writer = std.Io.File.stderr().writerStreaming(io, &stderr_buf);
    const stderr = &stderr_writer.interface;

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
    const col_names = cat.getTopLevelColumnNames(allocator, schema) catch &.{};
    defer allocator.free(col_names);

    var pir = parquet.PageIndexReader.init(allocator, reader.getSource());

    var total_indexed: usize = 0;
    var total_pages: usize = 0;

    for (reader.metadata.row_groups, 0..) |rg, rg_idx| {
        for (rg.columns, 0..) |chunk, col_idx| {
            const col_name = if (col_idx < col_names.len) col_names[col_idx] else "?";

            const has_oi = chunk.offset_index_offset != null;
            const has_ci = chunk.column_index_offset != null;
            if (!has_oi and !has_ci) {
                try stdout.print("rg={d} col={d} ({s}): no page index\n", .{ rg_idx, col_idx, col_name });
                continue;
            }

            const oi_opt = pir.readOffsetIndex(&chunk) catch |err| {
                try stderr.print("Error reading offset index for rg={d} col={d}: {}\n", .{ rg_idx, col_idx, err });
                continue;
            };
            const ci_opt = pir.readColumnIndex(&chunk) catch |err| blk: {
                try stderr.print("Warning: could not read column index for rg={d} col={d}: {}\n", .{ rg_idx, col_idx, err });
                break :blk null;
            };
            var oi = oi_opt orelse continue;
            defer oi.deinit(allocator);
            var ci_holder = ci_opt;
            defer if (ci_holder) |*ci| ci.deinit(allocator);

            total_indexed += 1;
            total_pages += oi.page_locations.len;

            const meta = chunk.meta_data orelse continue;
            const rg_num_rows = rg.num_rows;

            const boundary_str = if (ci_holder) |ci| boundaryOrderString(ci.boundary_order) else "n/a";
            try stdout.print(
                "rg={d} col={d} ({s}): pages={d} boundary={s}\n",
                .{ rg_idx, col_idx, col_name, oi.page_locations.len, boundary_str },
            );

            for (oi.page_locations, 0..) |loc, i| {
                const end_row: i64 = if (i + 1 < oi.page_locations.len)
                    oi.page_locations[i + 1].first_row_index
                else
                    rg_num_rows;
                try stdout.print("  page {d}: rows {d}..{d} offset={d} sz={d}", .{
                    i, loc.first_row_index, end_row, loc.offset, loc.compressed_page_size,
                });

                if (ci_holder) |ci| {
                    if (i < ci.null_pages.len and ci.null_pages[i]) {
                        try stdout.writeAll(" min=<null> max=<null>");
                    } else {
                        try stdout.writeAll(" min=");
                        try helpers.printStatValue(stdout, ci.min_values[i], meta.type_);
                        try stdout.writeAll(" max=");
                        try helpers.printStatValue(stdout, ci.max_values[i], meta.type_);
                    }
                    if (ci.null_counts) |ncs| {
                        if (i < ncs.len) try stdout.print(" nulls={d}", .{ncs[i]});
                    }
                }
                try stdout.writeAll("\n");
            }
        }
    }

    if (total_indexed == 0) {
        try stdout.writeAll("\nFile has no page indexes.\n");
    } else {
        try stdout.print("\nSummary: {d} indexed column chunks, {d} total pages.\n", .{ total_indexed, total_pages });
    }

    try stdout.flush();
}

fn boundaryOrderString(o: parquet.format.BoundaryOrder) []const u8 {
    return switch (o) {
        .unordered => "unordered",
        .ascending => "ascending",
        .descending => "descending",
    };
}
