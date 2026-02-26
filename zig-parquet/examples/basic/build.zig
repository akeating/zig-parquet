const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Get the main zig-parquet dependency
    const parquet_dep = b.dependency("parquet", .{
        .target = target,
        .optimize = optimize,
    });
    const parquet_mod = parquet_dep.module("parquet");

    // Discover examples in src/
    var examples_dir = std.fs.cwd().openDir("src", .{ .iterate = true }) catch return;
    defer examples_dir.close();

    var walker = examples_dir.walk(b.allocator) catch return;
    defer walker.deinit();

    while (walker.next() catch null) |entry| {
        if (entry.kind == .file and std.mem.endsWith(u8, entry.path, ".zig")) {
            const example_name = entry.basename[0 .. entry.basename.len - ".zig".len];
            
            const exe = b.addExecutable(.{
                .name = example_name,
                .root_module = b.createModule(.{
                    .root_source_file = b.path(b.fmt("src/{s}", .{entry.basename})),
                    .target = target,
                    .optimize = optimize,
                }),
            });

            // Add the parquet module
            exe.root_module.addImport("parquet", parquet_mod);

            b.installArtifact(exe);

            // Create a convenient run command: `zig build run-{name}`
            const run_cmd = b.addRunArtifact(exe);
            const run_step = b.step(
                b.fmt("run-{s}", .{example_name}), 
                b.fmt("Run the {s} example", .{example_name})
            );
            run_step.dependOn(&run_cmd.step);
        }
    }
}
