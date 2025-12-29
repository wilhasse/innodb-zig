const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const lib_module = b.createModule(.{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });

    const lib = b.addLibrary(.{
        .name = "innodb_zig",
        .root_module = lib_module,
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
