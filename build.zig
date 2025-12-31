const std = @import("std");

const AtomicOps = enum {
    auto,
    gcc_builtins,
    solaris,
    innodb,
};

fn addVerboseTest(
    b: *std.Build,
    source: []const u8,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    test_filter: ?[]const u8,
    build_options: ?*std.Build.Step.Options,
) *std.Build.Step.Run {
    const run = b.addSystemCommand(&.{ b.graph.zig_exe, "test" });
    run.stdio = .inherit;

    const target_triple = target.query.zigTriple(b.allocator) catch @panic("OOM");
    run.addArg("-target");
    run.addArg(target_triple);
    run.addArg("-O");
    run.addArg(@tagName(optimize));

    if (build_options != null) {
        run.addArg("--dep");
        run.addArg("build_options");
    }
    run.addPrefixedFileArg("-Mroot=", b.path(source));
    if (build_options) |opts| {
        run.addPrefixedFileArg("-Mbuild_options=", opts.getOutput());
    }
    if (test_filter) |filter| {
        run.addArg("--test-filter");
        run.addArg(filter);
    }

    return run;
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const enable_compression = b.option(
        bool,
        "compression",
        "Enable compressed tables support (set false to mirror --disable-compression)",
    ) orelse true;
    const build_shared = b.option(
        bool,
        "shared",
        "Build a shared library (set false to mirror --disable-shared)",
    ) orelse false;
    const atomic_ops = b.option(
        AtomicOps,
        "atomic_ops",
        "Select atomic ops implementation (auto|gcc_builtins|solaris|innodb)",
    ) orelse .auto;
    const test_filter = b.option(
        []const u8,
        "test-filter",
        "Only run tests matching this substring",
    );
    const test_verbose = b.option(
        bool,
        "test-verbose",
        "Show stdout/stderr for tests",
    ) orelse false;
    const c_tests_root = b.option(
        []const u8,
        "c-tests-root",
        "Path to the embedded InnoDB C source tree for running tests",
    ) orelse "/home/cslog/oss-embedded-innodb";
    const c_tests_list = b.option(
        []const u8,
        "c-tests-list",
        "Override TEST_EXECUTABLES for C tests (space-separated)",
    );
    const c_tests_stress_list = b.option(
        []const u8,
        "c-tests-stress-list",
        "Override TEST_STRESS_EXECUTABLES for stress C tests (space-separated)",
    );
    const c_tests_make = b.option(
        []const u8,
        "c-tests-make",
        "Make binary to use for C test runs",
    ) orelse "make";

    const build_options = b.addOptions();
    build_options.addOption(bool, "enable_compression", enable_compression);
    build_options.addOption(bool, "build_shared", build_shared);
    build_options.addOption(AtomicOps, "atomic_ops", atomic_ops);

    const lib_module = b.createModule(.{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });
    lib_module.addOptions("build_options", build_options);

    const lib = b.addLibrary(.{
        .name = "innodb_zig",
        .root_module = lib_module,
        .linkage = if (build_shared) .dynamic else .static,
    });
    b.installArtifact(lib);

    const btr_trace_module = b.createModule(.{
        .root_source_file = b.path("src/tools/btr_trace.zig"),
        .target = target,
        .optimize = optimize,
    });
    btr_trace_module.addImport("innodb", lib_module);

    const btr_trace = b.addExecutable(.{
        .name = "btr_trace",
        .root_module = btr_trace_module,
    });
    b.installArtifact(btr_trace);

    const btr_trace_run = b.addRunArtifact(btr_trace);
    if (b.args) |args| {
        btr_trace_run.addArgs(args);
    }
    const btr_trace_step = b.step("btr-trace", "Run B-tree trace generator");
    btr_trace_step.dependOn(&btr_trace_run.step);

    const lib_tests = b.addTest(.{
        .name = "lib_tests",
        .root_module = lib_module,
        .filters = if (test_filter) |filter| &[_][]const u8{filter} else &.{},
    });

    const map_module = b.createModule(.{
        .root_source_file = b.path("src/module_map.zig"),
        .target = target,
        .optimize = optimize,
    });
    map_module.addOptions("build_options", build_options);

    const map_tests = b.addTest(.{
        .name = "module_map_tests",
        .root_module = map_module,
        .filters = if (test_filter) |filter| &[_][]const u8{filter} else &.{},
    });

    const test_step = b.step("test", "Run unit tests");
    if (test_verbose) {
        const verbose_lib = addVerboseTest(b, "src/lib.zig", target, optimize, test_filter, build_options);
        const verbose_map = addVerboseTest(b, "src/module_map.zig", target, optimize, test_filter, null);
        test_step.dependOn(&verbose_lib.step);
        test_step.dependOn(&verbose_map.step);
    } else {
        const run_lib_tests = b.addRunArtifact(lib_tests);
        const run_map_tests = b.addRunArtifact(map_tests);
        test_step.dependOn(&run_lib_tests.step);
        test_step.dependOn(&run_map_tests.step);
    }

    const c_tests = b.step("c-tests", "Build and run embedded InnoDB C tests");
    const c_tests_cmd = b.addSystemCommand(&.{ c_tests_make, "-C", c_tests_root, "test" });
    c_tests_cmd.stdio = .inherit;
    if (c_tests_list) |list| {
        c_tests_cmd.addArg(b.fmt("TEST_EXECUTABLES={s}", .{list}));
    }
    c_tests.dependOn(&c_tests_cmd.step);

    const c_tests_stress = b.step("c-tests-stress", "Build and run embedded InnoDB C stress tests");
    const c_tests_stress_cmd = b.addSystemCommand(&.{ c_tests_make, "-C", c_tests_root, "test-stress" });
    c_tests_stress_cmd.stdio = .inherit;
    if (c_tests_stress_list) |list| {
        c_tests_stress_cmd.addArg(b.fmt("TEST_STRESS_EXECUTABLES={s}", .{list}));
    }
    c_tests_stress.dependOn(&c_tests_stress_cmd.step);
}
