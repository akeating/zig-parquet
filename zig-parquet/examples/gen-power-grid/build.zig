const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const parquet_dep = b.dependency("parquet", .{
        .target = target,
        .optimize = optimize,
    });

    // Generator executable
    const gen_mod = b.addModule("gen_power_grid", .{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    gen_mod.addImport("parquet", parquet_dep.module("parquet"));

    const gen_exe = b.addExecutable(.{
        .name = "gen-power-grid",
        .root_module = gen_mod,
    });
    b.installArtifact(gen_exe);

    // Reader executable
    const read_mod = b.addModule("read_segment", .{
        .root_source_file = b.path("src/read_segment.zig"),
        .target = target,
        .optimize = optimize,
    });
    read_mod.addImport("parquet", parquet_dep.module("parquet"));

    const read_exe = b.addExecutable(.{
        .name = "read-segment",
        .root_module = read_mod,
    });
    b.installArtifact(read_exe);

    // Run steps
    const run_gen = b.addRunArtifact(gen_exe);
    run_gen.step.dependOn(b.getInstallStep());
    const run_gen_step = b.step("run", "Generate power grid data file");
    run_gen_step.dependOn(&run_gen.step);

    const run_read = b.addRunArtifact(read_exe);
    run_read.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_read.addArgs(args);
    }
    const run_read_step = b.step("read", "Read random segment from grid data file");
    run_read_step.dependOn(&run_read.step);
}
