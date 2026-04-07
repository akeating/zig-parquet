const std = @import("std");
const zon = @import("build.zig.zon");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const codecs = b.option([]const u8, "codecs", "Compression codecs (default: all). See zig-parquet for values.") orelse "all";

    const parquet_dep = b.dependency("parquet", .{
        .target = target,
        .optimize = optimize,
        .codecs = codecs,
    });

    const build_options = b.addOptions();
    build_options.addOption([]const u8, "version", zon.version);

    const pq_mod = b.addModule("pqi", .{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Add the parquet module as import
    pq_mod.addImport("parquet", parquet_dep.module("parquet"));
    pq_mod.addImport("build_options", build_options.createModule());

    const exe = b.addExecutable(.{
        .name = "pqi",
        .root_module = pq_mod,
    });

    b.installArtifact(exe);

    // Run step
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run the pqi CLI");
    run_step.dependOn(&run_cmd.step);

    // Unit tests for helper functions
    const unit_test_mod = b.addModule("test_helpers", .{
        .root_source_file = b.path("src/test_helpers.zig"),
        .target = target,
        .optimize = optimize,
    });
    const unit_tests = b.addTest(.{
        .root_module = unit_test_mod,
    });
    const run_unit_tests = b.addRunArtifact(unit_tests);

    // Integration tests (require pq binary to be built first)
    const integration_test_mod = b.addModule("test_integration", .{
        .root_source_file = b.path("src/test_integration.zig"),
        .target = target,
        .optimize = optimize,
    });
    const integration_tests = b.addTest(.{
        .root_module = integration_test_mod,
    });
    const run_integration_tests = b.addRunArtifact(integration_tests);
    // Integration tests depend on the executable being built
    run_integration_tests.step.dependOn(b.getInstallStep());

    // Combined test step
    const test_step = b.step("test", "Run all tests");
    test_step.dependOn(&run_unit_tests.step);
    test_step.dependOn(&run_integration_tests.step);

    // Separate steps for running just unit or integration tests
    const unit_test_step = b.step("test-unit", "Run unit tests only");
    unit_test_step.dependOn(&run_unit_tests.step);

    const integration_test_step = b.step("test-integration", "Run integration tests only");
    integration_test_step.dependOn(&run_integration_tests.step);
}
