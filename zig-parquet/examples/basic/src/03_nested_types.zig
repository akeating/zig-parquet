const std = @import("std");
const parquet = @import("parquet");

const Location = struct {
    lat: f64,
    lon: f64,
    city: ?[]const u8,
};

const Event = struct {
    id: []const u8,
    location: Location,
    tags: ?[]const ?[]const u8,
    history: []const Location,
};

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

        var writer = try parquet.writeToFileRows(Event, allocator, file, .{});
        defer writer.deinit();

        try writer.writeRow(.{
            .id = "evt_001",
            .location = .{ .lat = 37.7749, .lon = -122.4194, .city = "San Francisco" },
            .tags = &[_]?[]const u8{ "tech", "conference" },
            .history = &[_]Location{
                .{ .lat = 37.7, .lon = -122.4, .city = null },
                .{ .lat = 34.0, .lon = -118.2, .city = "Los Angeles" },
            },
        });

        try writer.writeRow(.{
            .id = "evt_002",
            .location = .{ .lat = 40.7128, .lon = -74.0060, .city = null },
            .tags = null,
            .history = &[_]Location{
                .{ .lat = 42.3, .lon = -71.0, .city = "Boston" },
            },
        });

        try writer.close();
    }

    std.debug.print("Reading nested structs back...\n", .{});

    {
        const file = try std.fs.cwd().openFile(output_path, .{});
        defer file.close();

        var reader = try parquet.openFileRowReader(Event, allocator, file, .{});
        defer reader.deinit();

        while (try reader.next()) |event| {
            defer reader.freeRow(&event);
            std.debug.print("Event: {s}\n", .{event.id});
            std.debug.print("  Location: {d:.4}, {d:.4} (City: {?s})\n", .{
                event.location.lat,
                event.location.lon,
                event.location.city,
            });
            if (event.tags) |tags| {
                std.debug.print("  Tags ({d}): ", .{tags.len});
                for (tags) |t| std.debug.print("{?s} ", .{t});
                std.debug.print("\n", .{});
            } else {
                std.debug.print("  Tags: (none)\n", .{});
            }
            std.debug.print("  History ({d} points):\n", .{event.history.len});
            for (event.history, 0..) |loc, i| {
                std.debug.print("    [{d}] {d:.1}, {d:.1} ({?s})\n", .{ i, loc.lat, loc.lon, loc.city });
            }
        }
    }

    std.debug.print("Done!\n", .{});
}
