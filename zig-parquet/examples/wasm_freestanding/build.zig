const std = @import("std");

pub fn build(b: *std.Build) void {
    const optimize = b.standardOptimizeOption(.{});

    const parquet_dep = b.dependency("parquet", .{
        .target = b.resolveTargetQuery(.{
            .cpu_arch = .wasm32,
            .os_tag = .freestanding,
        }),
        .optimize = optimize,
        .no_compression = true,
    });

    const exe = b.addExecutable(.{
        .name = "parquet_freestanding_demo",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = b.resolveTargetQuery(.{
                .cpu_arch = .wasm32,
                .os_tag = .freestanding,
            }),
            .optimize = optimize,
        }),
    });

    exe.root_module.addImport("parquet", parquet_dep.module("parquet"));
    exe.entry = .disabled;
    exe.rdynamic = true;

    b.installArtifact(exe);
}
