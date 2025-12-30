const std = @import("std");
const build_options = @import("build_options");
const module_map = @import("module_map.zig");

pub const ut = @import("ut/mod.zig");
pub const mem = @import("mem/mod.zig");
pub const os = @import("os/mod.zig");
pub const sync = @import("sync/mod.zig");
pub const thr = @import("thr/mod.zig");
pub const dyn = @import("dyn/mod.zig");
pub const buf = @import("buf/mod.zig");
pub const btr = @import("btr/mod.zig");
pub const data = @import("data/mod.zig");
pub const eval = @import("eval/mod.zig");
pub const ddl = @import("ddl/mod.zig");
pub const dict = @import("dict/mod.zig");
pub const api = @import("api/mod.zig").impl;
pub const BuildOptions = build_options;

comptime {
    _ = build_options;
}

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
