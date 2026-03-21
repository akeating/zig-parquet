const std = @import("std");
const parquet = @import("parquet");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const output_path = "nested_events.parquet";
    defer std.fs.cwd().deleteFile(output_path) catch {};

    std.debug.print("Writing nested structs to {s}...\n", .{output_path});

    {
        const file = try std.fs.cwd().createFile(output_path, .{});
        defer file.close();

        var writer = try parquet.createFileDynamic(allocator, file);
        defer writer.deinit();

        // id: string
        try writer.addColumn("id", parquet.TypeInfo.string, .{});

        // location: struct { lat: f64, lon: f64, city: ?string }
        const city_leaf = try writer.allocSchemaNode(.{ .byte_array = .{ .logical = .string } });
        const opt_city = try writer.allocSchemaNode(.{ .optional = city_leaf });
        const lat_leaf = try writer.allocSchemaNode(.{ .double = .{} });
        const lon_leaf = try writer.allocSchemaNode(.{ .double = .{} });
        var loc_fields = try writer.allocSchemaFields(3);
        loc_fields[0] = .{ .name = try writer.dupeSchemaName("lat"), .node = lat_leaf };
        loc_fields[1] = .{ .name = try writer.dupeSchemaName("lon"), .node = lon_leaf };
        loc_fields[2] = .{ .name = try writer.dupeSchemaName("city"), .node = opt_city };
        const loc_node = try writer.allocSchemaNode(.{ .struct_ = .{ .fields = loc_fields } });
        try writer.addColumnNested("location", loc_node, .{});

        // tags: ?list<?string>
        const tag_str = try writer.allocSchemaNode(.{ .byte_array = .{ .logical = .string } });
        const opt_tag_str = try writer.allocSchemaNode(.{ .optional = tag_str });
        const tag_list = try writer.allocSchemaNode(.{ .list = opt_tag_str });
        const opt_tag_list = try writer.allocSchemaNode(.{ .optional = tag_list });
        try writer.addColumnNested("tags", opt_tag_list, .{});

        // history: list<struct { lat: f64, lon: f64, city: ?string }>
        // Reuse same field layout as location
        var hist_fields = try writer.allocSchemaFields(3);
        hist_fields[0] = .{ .name = try writer.dupeSchemaName("lat"), .node = lat_leaf };
        hist_fields[1] = .{ .name = try writer.dupeSchemaName("lon"), .node = lon_leaf };
        hist_fields[2] = .{ .name = try writer.dupeSchemaName("city"), .node = opt_city };
        const hist_struct = try writer.allocSchemaNode(.{ .struct_ = .{ .fields = hist_fields } });
        const hist_list = try writer.allocSchemaNode(.{ .list = hist_struct });
        try writer.addColumnNested("history", hist_list, .{});

        try writer.begin();

        // Event 1
        try writer.setBytes(0, "evt_001");

        // location struct
        try writer.beginStruct(1);
        try writer.setStructField(1, 0, .{ .double_val = 37.7749 });
        try writer.setStructField(1, 1, .{ .double_val = -122.4194 });
        try writer.setStructFieldBytes(1, 2, "San Francisco");
        try writer.endStruct(1);

        // tags: ["tech", "conference"]
        try writer.beginList(2);
        try writer.appendNestedBytes(2, "tech");
        try writer.appendNestedBytes(2, "conference");
        try writer.endList(2);

        // history: two locations
        try writer.beginList(3);
        try writer.beginStruct(3);
        try writer.setStructField(3, 0, .{ .double_val = 37.7 });
        try writer.setStructField(3, 1, .{ .double_val = -122.4 });
        try writer.setStructField(3, 2, .null_val);
        try writer.endStruct(3);
        try writer.beginStruct(3);
        try writer.setStructField(3, 0, .{ .double_val = 34.0 });
        try writer.setStructField(3, 1, .{ .double_val = -118.2 });
        try writer.setStructFieldBytes(3, 2, "Los Angeles");
        try writer.endStruct(3);
        try writer.endList(3);

        try writer.addRow();

        // Event 2
        try writer.setBytes(0, "evt_002");

        try writer.beginStruct(1);
        try writer.setStructField(1, 0, .{ .double_val = 40.7128 });
        try writer.setStructField(1, 1, .{ .double_val = -74.0060 });
        try writer.setStructField(1, 2, .null_val);
        try writer.endStruct(1);

        // tags: null
        try writer.setNull(2);

        // history: one location
        try writer.beginList(3);
        try writer.beginStruct(3);
        try writer.setStructField(3, 0, .{ .double_val = 42.3 });
        try writer.setStructField(3, 1, .{ .double_val = -71.0 });
        try writer.setStructFieldBytes(3, 2, "Boston");
        try writer.endStruct(3);
        try writer.endList(3);

        try writer.addRow();

        try writer.close();
    }

    std.debug.print("Reading nested structs back...\n", .{});

    {
        const file = try std.fs.cwd().openFile(output_path, .{});
        defer file.close();

        var reader = try parquet.openFileDynamic(allocator, file, .{});
        defer reader.deinit();

        const rows = try reader.readAllRows(0);
        defer {
            for (rows) |row| row.deinit();
            allocator.free(rows);
        }

        for (rows) |row| {
            const id_val = row.getColumn(0) orelse continue;
            const id = id_val.asBytes() orelse "(null)";
            std.debug.print("Event: {s}\n", .{id});

            // location struct
            if (row.getColumn(1)) |loc| {
                if (loc.asStruct()) |fields| {
                    const lat = fields[0].value.asDouble() orelse 0;
                    const lon = fields[1].value.asDouble() orelse 0;
                    const city = fields[2].value.asBytes();
                    if (city) |c| {
                        std.debug.print("  Location: {d:.4}, {d:.4} (City: {s})\n", .{ lat, lon, c });
                    } else {
                        std.debug.print("  Location: {d:.4}, {d:.4} (City: null)\n", .{ lat, lon });
                    }
                }
            }

            // tags
            if (row.getColumn(2)) |tags_val| {
                if (tags_val.asList()) |tags| {
                    std.debug.print("  Tags ({d}): ", .{tags.len});
                    for (tags) |t| {
                        if (t.asBytes()) |s| {
                            std.debug.print("{s} ", .{s});
                        } else {
                            std.debug.print("null ", .{});
                        }
                    }
                    std.debug.print("\n", .{});
                } else {
                    std.debug.print("  Tags: (none)\n", .{});
                }
            } else {
                std.debug.print("  Tags: (none)\n", .{});
            }

            // history
            if (row.getColumn(3)) |hist_val| {
                if (hist_val.asList()) |history| {
                    std.debug.print("  History ({d} points):\n", .{history.len});
                    for (history, 0..) |h, i| {
                        if (h.asStruct()) |fields| {
                            const lat = fields[0].value.asDouble() orelse 0;
                            const lon = fields[1].value.asDouble() orelse 0;
                            const city = fields[2].value.asBytes();
                            if (city) |c| {
                                std.debug.print("    [{d}] {d:.1}, {d:.1} ({s})\n", .{ i, lat, lon, c });
                            } else {
                                std.debug.print("    [{d}] {d:.1}, {d:.1} (null)\n", .{ i, lat, lon });
                            }
                        }
                    }
                }
            }
        }
    }

    std.debug.print("Done!\n", .{});
}
