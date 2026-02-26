const std = @import("std");
const zon = @import("build.zig.zon");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const no_compression = b.option(bool, "no_compression", "Disable all compression codecs (no C/C++ dependencies)") orelse false;

    const build_options = b.addOptions();
    build_options.addOption(bool, "no_compression", no_compression);
    build_options.addOption([]const u8, "version", zon.version);

    // Get C library dependencies
    const lz4_dep = b.dependency("lz4", .{});
    const brotli_dep = b.dependency("brotli", .{});
    const snappy_dep = b.dependency("snappy", .{});
    const zstd_dep = b.dependency("zstd", .{});
    const zlib_dep = b.dependency("zlib", .{});

    // Brotli C sources (common, decoder, encoder)
    const brotli_common_sources = &[_][]const u8{
        "c/common/constants.c",
        "c/common/context.c",
        "c/common/dictionary.c",
        "c/common/platform.c",
        "c/common/shared_dictionary.c",
        "c/common/transform.c",
    };

    const brotli_dec_sources = &[_][]const u8{
        "c/dec/bit_reader.c",
        "c/dec/decode.c",
        "c/dec/huffman.c",
        "c/dec/prefix.c",
        "c/dec/state.c",
        "c/dec/static_init.c",
    };

    const brotli_enc_sources = &[_][]const u8{
        "c/enc/backward_references.c",
        "c/enc/backward_references_hq.c",
        "c/enc/bit_cost.c",
        "c/enc/block_splitter.c",
        "c/enc/brotli_bit_stream.c",
        "c/enc/cluster.c",
        "c/enc/command.c",
        "c/enc/compound_dictionary.c",
        "c/enc/compress_fragment.c",
        "c/enc/compress_fragment_two_pass.c",
        "c/enc/dictionary_hash.c",
        "c/enc/encode.c",
        "c/enc/encoder_dict.c",
        "c/enc/entropy_encode.c",
        "c/enc/fast_log.c",
        "c/enc/histogram.c",
        "c/enc/literal_cost.c",
        "c/enc/memory.c",
        "c/enc/metablock.c",
        "c/enc/static_dict.c",
        "c/enc/static_dict_lut.c",
        "c/enc/static_init.c",
        "c/enc/utf8_util.c",
    };

    // Snappy C++ sources
    const snappy_sources = &[_][]const u8{
        "snappy.cc",
        "snappy-c.cc",
        "snappy-sinksource.cc",
        "snappy-stubs-internal.cc",
    };

    // Zstd C sources (common, compress, decompress)
    const zstd_common_sources = &[_][]const u8{
        "lib/common/debug.c",
        "lib/common/entropy_common.c",
        "lib/common/error_private.c",
        "lib/common/fse_decompress.c",
        "lib/common/pool.c",
        "lib/common/threading.c",
        "lib/common/xxhash.c",
        "lib/common/zstd_common.c",
    };

    const zstd_compress_sources = &[_][]const u8{
        "lib/compress/fse_compress.c",
        "lib/compress/hist.c",
        "lib/compress/huf_compress.c",
        "lib/compress/zstd_compress.c",
        "lib/compress/zstd_compress_literals.c",
        "lib/compress/zstd_compress_sequences.c",
        "lib/compress/zstd_compress_superblock.c",
        "lib/compress/zstd_double_fast.c",
        "lib/compress/zstd_fast.c",
        "lib/compress/zstd_lazy.c",
        "lib/compress/zstd_ldm.c",
        "lib/compress/zstd_opt.c",
        "lib/compress/zstd_preSplit.c",
        "lib/compress/zstdmt_compress.c",
    };

    const zstd_decompress_sources = &[_][]const u8{
        "lib/decompress/huf_decompress.c",
        "lib/decompress/zstd_ddict.c",
        "lib/decompress/zstd_decompress.c",
        "lib/decompress/zstd_decompress_block.c",
    };

    // Zlib C sources
    const zlib_sources = &[_][]const u8{
        "adler32.c",
        "compress.c",
        "crc32.c",
        "deflate.c",
        "inffast.c",
        "inflate.c",
        "inftrees.c",
        "trees.c",
        "uncompr.c",
        "zutil.c",
    };

    const zstd_flags: []const []const u8 = &.{"-DZSTD_DISABLE_ASM"};

    // Create a module for the library
    const parquet_mod = b.addModule("parquet", .{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });
    parquet_mod.addImport("build_options", build_options.createModule());

    // Library artifact
    const lib = b.addLibrary(.{
        .name = "parquet",
        .root_module = parquet_mod,
    });

    if (!no_compression) {
        parquet_mod.addIncludePath(lz4_dep.path("lib"));
        parquet_mod.addIncludePath(brotli_dep.path("c/include"));
        parquet_mod.addIncludePath(snappy_dep.path(""));
        parquet_mod.addIncludePath(zstd_dep.path("lib"));
        parquet_mod.addIncludePath(zlib_dep.path(""));
        parquet_mod.addIncludePath(b.path("src/core/compress"));

        addCodecSources(lib.root_module, lz4_dep, brotli_dep, snappy_dep, zstd_dep, zlib_dep, brotli_common_sources, brotli_dec_sources, brotli_enc_sources, snappy_sources, zstd_common_sources, zstd_compress_sources, zstd_decompress_sources, zlib_sources, zstd_flags);
        lib.root_module.link_libc = true;
        lib.root_module.link_libcpp = true;
    }

    b.installArtifact(lib);

    // C API shared library (opt-in)
    const c_api = b.option(bool, "c_api", "Build shared library with C ABI exports") orelse false;
    if (c_api) {
        const capi_mod = b.addModule("parquet_capi", .{
            .root_source_file = b.path("src/lib.zig"),
            .target = target,
            .optimize = optimize,
        });
        capi_mod.addImport("build_options", build_options.createModule());

        const capi_lib = b.addLibrary(.{
            .name = "parquet_capi",
            .root_module = capi_mod,
            .linkage = .dynamic,
        });

        if (!no_compression) {
            capi_mod.addIncludePath(lz4_dep.path("lib"));
            capi_mod.addIncludePath(brotli_dep.path("c/include"));
            capi_mod.addIncludePath(snappy_dep.path(""));
            capi_mod.addIncludePath(zstd_dep.path("lib"));
            capi_mod.addIncludePath(zlib_dep.path(""));
            capi_mod.addIncludePath(b.path("src/core/compress"));

            addCodecSources(capi_lib.root_module, lz4_dep, brotli_dep, snappy_dep, zstd_dep, zlib_dep, brotli_common_sources, brotli_dec_sources, brotli_enc_sources, snappy_sources, zstd_common_sources, zstd_compress_sources, zstd_decompress_sources, zlib_sources, zstd_flags);
            capi_lib.root_module.link_libc = true;
            capi_lib.root_module.link_libcpp = true;
        }

        b.installArtifact(capi_lib);
        capi_lib.root_module.addIncludePath(b.path("src/api/c"));
        b.installFile("src/api/c/libparquet.h", "include/libparquet.h");
    }

    // WASM/WASI library (opt-in)
    const wasm_wasi = b.option(bool, "wasm_wasi", "Build WASM library for wasm32-wasi") orelse false;
    if (wasm_wasi) {
        const wasi_target = b.resolveTargetQuery(.{
            .cpu_arch = .wasm32,
            .os_tag = .wasi,
        });

        const wasm_no_compression = b.addOptions();
        wasm_no_compression.addOption(bool, "no_compression", no_compression);
        wasm_no_compression.addOption([]const u8, "version", zon.version);

        const wasi_mod = b.addModule("parquet_wasi", .{
            .root_source_file = b.path("src/lib.zig"),
            .target = wasi_target,
            .optimize = optimize,
        });
        wasi_mod.addImport("build_options", wasm_no_compression.createModule());

        const wasi_lib = b.addExecutable(.{
            .name = "parquet_wasi",
            .root_module = wasi_mod,
        });

        wasi_lib.entry = .disabled;
        wasi_lib.rdynamic = true;

        if (!no_compression) {
            wasi_mod.addIncludePath(lz4_dep.path("lib"));
            wasi_mod.addIncludePath(brotli_dep.path("c/include"));
            wasi_mod.addIncludePath(snappy_dep.path(""));
            wasi_mod.addIncludePath(zstd_dep.path("lib"));
            wasi_mod.addIncludePath(zlib_dep.path(""));
            wasi_mod.addIncludePath(b.path("src/core/compress"));

            addCodecSources(wasi_lib.root_module, lz4_dep, brotli_dep, snappy_dep, zstd_dep, zlib_dep, brotli_common_sources, brotli_dec_sources, brotli_enc_sources, snappy_sources, zstd_common_sources, zstd_compress_sources, zstd_decompress_sources, zlib_sources, zstd_flags);
            wasi_lib.root_module.link_libc = true;
            wasi_lib.root_module.link_libcpp = true;
        }

        b.installArtifact(wasi_lib);
    }

    // WASM freestanding library (opt-in, always no_compression)
    const wasm_freestanding = b.option(bool, "wasm_freestanding", "Build WASM library for wasm32-freestanding (no compression)") orelse false;
    if (wasm_freestanding) {
        const freestanding_target = b.resolveTargetQuery(.{
            .cpu_arch = .wasm32,
            .os_tag = .freestanding,
        });

        const freestanding_opts = b.addOptions();
        freestanding_opts.addOption(bool, "no_compression", true);
        freestanding_opts.addOption([]const u8, "version", zon.version);

        const freestanding_mod = b.addModule("parquet_freestanding", .{
            .root_source_file = b.path("src/lib.zig"),
            .target = freestanding_target,
            .optimize = optimize,
        });
        freestanding_mod.addImport("build_options", freestanding_opts.createModule());

        const freestanding_lib = b.addExecutable(.{
            .name = "parquet_freestanding",
            .root_module = freestanding_mod,
        });

        freestanding_lib.entry = .disabled;
        freestanding_lib.rdynamic = true;

        b.installArtifact(freestanding_lib);
    }

    // Unit tests
    const test_mod = b.addModule("parquet_test", .{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_mod.addImport("build_options", build_options.createModule());

    const lib_unit_tests = b.addTest(.{
        .root_module = test_mod,
    });

    if (!no_compression) {
        test_mod.addIncludePath(lz4_dep.path("lib"));
        test_mod.addIncludePath(brotli_dep.path("c/include"));
        test_mod.addIncludePath(snappy_dep.path(""));
        test_mod.addIncludePath(zstd_dep.path("lib"));
        test_mod.addIncludePath(zlib_dep.path(""));
        test_mod.addIncludePath(b.path("src/core/compress"));

        addCodecSources(lib_unit_tests.root_module, lz4_dep, brotli_dep, snappy_dep, zstd_dep, zlib_dep, brotli_common_sources, brotli_dec_sources, brotli_enc_sources, snappy_sources, zstd_common_sources, zstd_compress_sources, zstd_decompress_sources, zlib_sources, zstd_flags);
        lib_unit_tests.root_module.link_libc = true;
        lib_unit_tests.root_module.link_libcpp = true;
    }

    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_unit_tests.step);

    // WASM smoke test: compile both WASM targets (no-compression) to verify exports link
    const wasm_smoke_step = b.step("wasm-smoke", "Verify WASM targets compile and link");
    {
        const wasi_target = b.resolveTargetQuery(.{ .cpu_arch = .wasm32, .os_tag = .wasi });
        const wasi_opts = b.addOptions();
        wasi_opts.addOption(bool, "no_compression", true);
        wasi_opts.addOption([]const u8, "version", zon.version);
        const wasi_mod = b.addModule("parquet_wasi_smoke", .{
            .root_source_file = b.path("src/lib.zig"),
            .target = wasi_target,
            .optimize = optimize,
        });
        wasi_mod.addImport("build_options", wasi_opts.createModule());
        const wasi_lib = b.addExecutable(.{
            .name = "parquet_wasi_smoke",
            .root_module = wasi_mod,
        });
        wasi_lib.entry = .disabled;
        wasi_lib.rdynamic = true;
        wasm_smoke_step.dependOn(&wasi_lib.step);
    }
    {
        const free_target = b.resolveTargetQuery(.{ .cpu_arch = .wasm32, .os_tag = .freestanding });
        const free_opts = b.addOptions();
        free_opts.addOption(bool, "no_compression", true);
        free_opts.addOption([]const u8, "version", zon.version);
        const free_mod = b.addModule("parquet_free_smoke", .{
            .root_source_file = b.path("src/lib.zig"),
            .target = free_target,
            .optimize = optimize,
        });
        free_mod.addImport("build_options", free_opts.createModule());
        const free_lib = b.addExecutable(.{
            .name = "parquet_free_smoke",
            .root_module = free_mod,
        });
        free_lib.entry = .disabled;
        free_lib.rdynamic = true;
        wasm_smoke_step.dependOn(&free_lib.step);
    }
}

fn addCodecSources(
    module: *std.Build.Module,
    lz4_dep: *std.Build.Dependency,
    brotli_dep: *std.Build.Dependency,
    snappy_dep: *std.Build.Dependency,
    zstd_dep: *std.Build.Dependency,
    zlib_dep: *std.Build.Dependency,
    brotli_common_sources: []const []const u8,
    brotli_dec_sources: []const []const u8,
    brotli_enc_sources: []const []const u8,
    snappy_sources: []const []const u8,
    zstd_common_sources: []const []const u8,
    zstd_compress_sources: []const []const u8,
    zstd_decompress_sources: []const []const u8,
    zlib_sources: []const []const u8,
    zstd_flags: []const []const u8,
) void {
    module.addCSourceFile(.{
        .file = lz4_dep.path("lib/lz4.c"),
        .flags = &.{"-DXXH_NAMESPACE=LZ4_"},
    });

    for (brotli_common_sources) |src| {
        module.addCSourceFile(.{ .file = brotli_dep.path(src), .flags = &.{} });
    }
    for (brotli_dec_sources) |src| {
        module.addCSourceFile(.{ .file = brotli_dep.path(src), .flags = &.{} });
    }
    for (brotli_enc_sources) |src| {
        module.addCSourceFile(.{ .file = brotli_dep.path(src), .flags = &.{} });
    }

    for (snappy_sources) |src| {
        module.addCSourceFile(.{
            .file = snappy_dep.path(src),
            .flags = &.{ "-std=c++11", "-DNDEBUG", "-fno-exceptions" },
        });
    }

    for (zstd_common_sources) |src| {
        module.addCSourceFile(.{ .file = zstd_dep.path(src), .flags = zstd_flags });
    }
    for (zstd_compress_sources) |src| {
        module.addCSourceFile(.{ .file = zstd_dep.path(src), .flags = zstd_flags });
    }
    for (zstd_decompress_sources) |src| {
        module.addCSourceFile(.{ .file = zstd_dep.path(src), .flags = zstd_flags });
    }

    for (zlib_sources) |src| {
        module.addCSourceFile(.{ .file = zlib_dep.path(src), .flags = &.{} });
    }
}
