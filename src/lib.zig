const std = @import("std");
const module_map = @import("module_map.zig");

pub const Module = module_map.Module;
pub const modules = module_map.modules;

test "module map is non-empty" {
    try std.testing.expect(modules.len > 0);
}
