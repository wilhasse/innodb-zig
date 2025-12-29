const std = @import("std");
const build_options = @import("build_options");
const module_map = @import("module_map.zig");

pub const ut = @import("ut/mod.zig");

pub const Module = module_map.Module;
pub const modules = module_map.modules;

test "module map is non-empty" {
    try std.testing.expect(modules.len > 0);
}

test "build options are wired" {
    if (build_options.enable_compression) {} else {}
    if (build_options.build_shared) {} else {}

    switch (build_options.atomic_ops) {
        .auto, .gcc_builtins, .solaris, .innodb => {},
    }

    try std.testing.expect(true);
}
