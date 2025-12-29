const std = @import("std");

const AtomicOps = enum {
    auto,
    gcc_builtins,
    solaris,
    innodb,
};

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

    const lib_tests = b.addTest(.{
        .name = "lib_tests",
        .root_module = lib_module,
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
    });

    const test_step = b.step("test", "Run unit tests");
    const run_lib_tests = b.addRunArtifact(lib_tests);
    const run_map_tests = b.addRunArtifact(map_tests);
    test_step.dependOn(&run_lib_tests.step);
    test_step.dependOn(&run_map_tests.step);
}
