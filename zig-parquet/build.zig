const std = @import("std");
const zon = @import("build.zig.zon");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const codecs_str = b.option(
        []const u8,
        "codecs",
        "Compression codecs (default: all). Values: all, none, or comma-separated list of: zstd,snappy,gzip,lz4,brotli",
    ) orelse "all";

    const codecs = parseCodecs(codecs_str);

    const build_options = b.addOptions();
    build_options.addOption(bool, "enable_zstd", codecs.zstd);
    build_options.addOption(bool, "enable_snappy", codecs.snappy);
    build_options.addOption(bool, "enable_gzip", codecs.gzip);
    build_options.addOption(bool, "enable_lz4", codecs.lz4);
    build_options.addOption(bool, "enable_brotli", codecs.brotli);
    build_options.addOption([]const u8, "version", zon.version);

    const deps = resolveDeps(b, codecs);

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

    configureCodecs(lib.root_module, deps, b);

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

        configureCodecs(capi_lib.root_module, deps, b);

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

        const wasm_opts = b.addOptions();
        wasm_opts.addOption(bool, "enable_zstd", codecs.zstd);
        wasm_opts.addOption(bool, "enable_snappy", codecs.snappy);
        wasm_opts.addOption(bool, "enable_gzip", codecs.gzip);
        wasm_opts.addOption(bool, "enable_lz4", codecs.lz4);
        wasm_opts.addOption(bool, "enable_brotli", codecs.brotli);
        wasm_opts.addOption([]const u8, "version", zon.version);

        const wasi_mod = b.addModule("parquet_wasi", .{
            .root_source_file = b.path("src/lib.zig"),
            .target = wasi_target,
            .optimize = optimize,
        });
        wasi_mod.addImport("build_options", wasm_opts.createModule());

        const wasi_lib = b.addExecutable(.{
            .name = "parquet_wasi",
            .root_module = wasi_mod,
        });

        wasi_lib.entry = .disabled;
        wasi_lib.rdynamic = true;

        configureCodecs(wasi_lib.root_module, deps, b);

        b.installArtifact(wasi_lib);
    }

    // WASM freestanding library (opt-in, always no codecs)
    const wasm_freestanding = b.option(bool, "wasm_freestanding", "Build WASM library for wasm32-freestanding (no compression)") orelse false;
    if (wasm_freestanding) {
        const freestanding_target = b.resolveTargetQuery(.{
            .cpu_arch = .wasm32,
            .os_tag = .freestanding,
        });

        const freestanding_opts = b.addOptions();
        freestanding_opts.addOption(bool, "enable_zstd", false);
        freestanding_opts.addOption(bool, "enable_snappy", false);
        freestanding_opts.addOption(bool, "enable_gzip", false);
        freestanding_opts.addOption(bool, "enable_lz4", false);
        freestanding_opts.addOption(bool, "enable_brotli", false);
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

    configureCodecs(lib_unit_tests.root_module, deps, b);

    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);

    const check_test_files = b.addSystemCommand(&.{
        "sh", "-c",
        \\test -f ../test-files-arrow/basic/basic_types_plain_uncompressed.parquet || {
        \\  echo ""
        \\  echo "ERROR: Test files not found."
        \\  echo "Generate them first:  cd test-files-arrow && uv run python generate.py"
        \\  echo ""
        \\  exit 1
        \\}
    });
    run_lib_unit_tests.step.dependOn(&check_test_files.step);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_unit_tests.step);

    // WASM smoke test: compile both WASM targets (no codecs) to verify exports link
    const wasm_smoke_step = b.step("wasm-smoke", "Verify WASM targets compile and link");
    {
        const wasi_target = b.resolveTargetQuery(.{ .cpu_arch = .wasm32, .os_tag = .wasi });
        const wasi_opts = b.addOptions();
        wasi_opts.addOption(bool, "enable_zstd", false);
        wasi_opts.addOption(bool, "enable_snappy", false);
        wasi_opts.addOption(bool, "enable_gzip", false);
        wasi_opts.addOption(bool, "enable_lz4", false);
        wasi_opts.addOption(bool, "enable_brotli", false);
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
        free_opts.addOption(bool, "enable_zstd", false);
        free_opts.addOption(bool, "enable_snappy", false);
        free_opts.addOption(bool, "enable_gzip", false);
        free_opts.addOption(bool, "enable_lz4", false);
        free_opts.addOption(bool, "enable_brotli", false);
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

// =========================================================================
// Codec configuration
// =========================================================================

const Codecs = struct {
    zstd: bool,
    snappy: bool,
    gzip: bool,
    lz4: bool,
    brotli: bool,

    fn anyC(self: Codecs) bool {
        return self.zstd or self.snappy or self.gzip or self.lz4 or self.brotli;
    }
};

fn parseCodecs(str: []const u8) Codecs {
    if (std.mem.eql(u8, str, "all")) return .{ .zstd = true, .snappy = true, .gzip = true, .lz4 = true, .brotli = true };
    if (std.mem.eql(u8, str, "none")) return .{ .zstd = false, .snappy = false, .gzip = false, .lz4 = false, .brotli = false };
    return .{
        .zstd = containsCodec(str, "zstd"),
        .snappy = containsCodec(str, "snappy"),
        .gzip = containsCodec(str, "gzip"),
        .lz4 = containsCodec(str, "lz4"),
        .brotli = containsCodec(str, "brotli"),
    };
}

fn containsCodec(csv: []const u8, name: []const u8) bool {
    var iter = std.mem.splitScalar(u8, csv, ',');
    while (iter.next()) |token| {
        const trimmed = std.mem.trim(u8, token, " ");
        if (std.mem.eql(u8, trimmed, name)) return true;
    }
    return false;
}

const Deps = struct {
    lz4: ?*std.Build.Dependency,
    brotli: ?*std.Build.Dependency,
    snappy: ?*std.Build.Dependency,
    zstd: ?*std.Build.Dependency,
    zlib: ?*std.Build.Dependency,
    codecs: Codecs,
};

fn resolveDeps(b: *std.Build, codecs: Codecs) Deps {
    return .{
        .lz4 = if (codecs.lz4) b.dependency("lz4", .{}) else null,
        .brotli = if (codecs.brotli) b.dependency("brotli", .{}) else null,
        .snappy = if (codecs.snappy) b.dependency("snappy", .{}) else null,
        .zstd = if (codecs.zstd) b.dependency("zstd", .{}) else null,
        .zlib = if (codecs.gzip) b.dependency("zlib", .{}) else null,
        .codecs = codecs,
    };
}

fn configureCodecs(module: *std.Build.Module, deps: Deps, b: *std.Build) void {
    if (!deps.codecs.anyC()) return;

    if (deps.lz4) |dep| {
        module.addIncludePath(dep.path("lib"));
        module.addCSourceFile(.{
            .file = dep.path("lib/lz4.c"),
            .flags = &.{"-DXXH_NAMESPACE=LZ4_"},
        });
    }

    if (deps.brotli) |dep| {
        module.addIncludePath(dep.path("c/include"));
        for (brotli_common_sources) |src| {
            module.addCSourceFile(.{ .file = dep.path(src), .flags = &.{} });
        }
        for (brotli_dec_sources) |src| {
            module.addCSourceFile(.{ .file = dep.path(src), .flags = &.{} });
        }
        for (brotli_enc_sources) |src| {
            module.addCSourceFile(.{ .file = dep.path(src), .flags = &.{} });
        }
    }

    if (deps.snappy) |dep| {
        module.addIncludePath(dep.path(""));
        module.addIncludePath(b.path("src/core/compress"));
        for (snappy_sources) |src| {
            module.addCSourceFile(.{
                .file = dep.path(src),
                .flags = &.{ "-std=c++11", "-DNDEBUG", "-fno-exceptions" },
            });
        }
        module.link_libcpp = true;
    }

    if (deps.zstd) |dep| {
        module.addIncludePath(dep.path("lib"));
        for (zstd_common_sources) |src| {
            module.addCSourceFile(.{ .file = dep.path(src), .flags = zstd_flags });
        }
        for (zstd_compress_sources) |src| {
            module.addCSourceFile(.{ .file = dep.path(src), .flags = zstd_flags });
        }
        for (zstd_decompress_sources) |src| {
            module.addCSourceFile(.{ .file = dep.path(src), .flags = zstd_flags });
        }
    }

    if (deps.zlib) |dep| {
        module.addIncludePath(dep.path(""));
        for (zlib_sources) |src| {
            module.addCSourceFile(.{ .file = dep.path(src), .flags = &.{} });
        }
    }

    module.link_libc = true;
}

// =========================================================================
// C source file lists
// =========================================================================

const zstd_flags: []const []const u8 = &.{"-DZSTD_DISABLE_ASM"};

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

const snappy_sources = &[_][]const u8{
    "snappy.cc",
    "snappy-c.cc",
    "snappy-sinksource.cc",
    "snappy-stubs-internal.cc",
};

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
