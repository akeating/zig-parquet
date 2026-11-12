const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    // Default to fast (ReleaseFast). standardOptimizeOption defaults to Debug unless --release is
    // passed, and Debug timings are about 4x the release timings for this benchmark.
    const optimize = b.option(std.builtin.OptimizeMode, "optimize", "Optimization mode (default: fast)") orelse .fast;

    const codecs = b.option([]const u8, "codecs", "Codecs passed to zig-parquet (default: all)") orelse "all";

    const parquet_dep = b.dependency("parquet", .{
        .target = target,
        .optimize = optimize,
        .codecs = codecs,
    });

    const mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    mod.addImport("parquet", parquet_dep.module("parquet"));

    const exe = b.addExecutable(.{
        .name = "bench-column-read",
        .root_module = mod,
    });
    b.installArtifact(exe);

    const run = b.addRunArtifact(exe);
    run.step.dependOn(b.getInstallStep());
    run.addPassthruArgs();
    b.step("run", "Run the column read benchmark: zig build run -- FILE.parquet [RUNS]").dependOn(&run.step);

    const snappy_mod = b.createModule(.{
        .root_source_file = b.path("src/snappy.zig"),
        .target = target,
        .optimize = optimize,
    });
    snappy_mod.addImport("parquet", parquet_dep.module("parquet"));
    const snappy_exe = b.addExecutable(.{ .name = "bench-snappy", .root_module = snappy_mod });
    b.installArtifact(snappy_exe);
    const snappy_run = b.addRunArtifact(snappy_exe);
    snappy_run.addPassthruArgs();
    b.step("snappy", "Run the snappy decompression benchmark: zig build snappy -- DIR [RUNS]").dependOn(&snappy_run.step);
}
