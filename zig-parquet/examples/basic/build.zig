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

    const examples = [_][]const u8{
        "01_read_write",
        "02_dynamic_read",
        "03_nested_types",
        "04_in_memory_buffer",
        "05_column_api",
    };

    for (examples) |example_name| {
        const exe = b.addExecutable(.{
            .name = example_name,
            .root_module = b.createModule(.{
                .root_source_file = b.path(b.fmt("src/{s}.zig", .{example_name})),
                .target = target,
                .optimize = optimize,
            }),
        });

        exe.root_module.addImport("parquet", parquet_mod);
        b.installArtifact(exe);

        const run_cmd = b.addRunArtifact(exe);
        const run_step = b.step(
            b.fmt("run-{s}", .{example_name}),
            b.fmt("Run the {s} example", .{example_name}),
        );
        run_step.dependOn(&run_cmd.step);
    }
}
