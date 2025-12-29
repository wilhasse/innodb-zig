const std = @import("std");

pub const Module = struct {
    name: []const u8,
    c_dir: []const u8,
    zig_path: []const u8,
    depends_on: []const []const u8,
};

pub const modules = [_]Module{
    .{ .name = "ut", .c_dir = "ut", .zig_path = "zig/ut", .depends_on = &[_][]const u8{} },
    .{ .name = "mach", .c_dir = "mach", .zig_path = "zig/mach", .depends_on = &[_][]const u8{} },
    .{ .name = "mem", .c_dir = "mem", .zig_path = "zig/mem", .depends_on = &[_][]const u8{ "ut" } },
    .{ .name = "os", .c_dir = "os", .zig_path = "zig/os", .depends_on = &[_][]const u8{ "ut", "mem", "mach" } },
    .{ .name = "sync", .c_dir = "sync", .zig_path = "zig/sync", .depends_on = &[_][]const u8{ "os", "ut" } },
    .{ .name = "thr", .c_dir = "thr", .zig_path = "zig/thr", .depends_on = &[_][]const u8{ "os", "sync", "ut" } },
    .{ .name = "dyn", .c_dir = "dyn", .zig_path = "zig/dyn", .depends_on = &[_][]const u8{ "mem", "ut" } },
    .{ .name = "data", .c_dir = "data", .zig_path = "zig/data", .depends_on = &[_][]const u8{ "mach", "mem", "ut" } },
    .{ .name = "log", .c_dir = "log", .zig_path = "zig/log", .depends_on = &[_][]const u8{ "os", "sync", "mach", "mem", "ut" } },
    .{ .name = "fil", .c_dir = "fil", .zig_path = "zig/fil", .depends_on = &[_][]const u8{ "os", "sync", "mach", "mem", "ut" } },
    .{ .name = "fsp", .c_dir = "fsp", .zig_path = "zig/fsp", .depends_on = &[_][]const u8{ "fil", "mach", "mem", "ut" } },
    .{ .name = "mtr", .c_dir = "mtr", .zig_path = "zig/mtr", .depends_on = &[_][]const u8{ "log", "sync", "mach", "mem", "ut" } },
    .{ .name = "page", .c_dir = "page", .zig_path = "zig/page", .depends_on = &[_][]const u8{ "mtr", "mach", "mem", "ut" } },
    .{ .name = "buf", .c_dir = "buf", .zig_path = "zig/buf", .depends_on = &[_][]const u8{ "fil", "fsp", "log", "mtr", "sync", "mach", "mem", "ut" } },
    .{ .name = "fut", .c_dir = "fut", .zig_path = "zig/fut", .depends_on = &[_][]const u8{ "page", "buf", "mtr", "ut" } },
    .{ .name = "ibuf", .c_dir = "ibuf", .zig_path = "zig/ibuf", .depends_on = &[_][]const u8{ "buf", "btr", "mtr", "page", "ut" } },
    .{ .name = "rem", .c_dir = "rem", .zig_path = "zig/rem", .depends_on = &[_][]const u8{ "page", "mach", "mem", "ut" } },
    .{ .name = "btr", .c_dir = "btr", .zig_path = "zig/btr", .depends_on = &[_][]const u8{ "page", "buf", "mtr", "rem", "mem", "ut" } },
    .{ .name = "dict", .c_dir = "dict", .zig_path = "zig/dict", .depends_on = &[_][]const u8{ "btr", "buf", "data", "mem", "ut" } },
    .{ .name = "lock", .c_dir = "lock", .zig_path = "zig/lock", .depends_on = &[_][]const u8{ "dict", "mem", "ut" } },
    .{ .name = "trx", .c_dir = "trx", .zig_path = "zig/trx", .depends_on = &[_][]const u8{ "log", "lock", "dict", "mem", "ut" } },
    .{ .name = "row", .c_dir = "row", .zig_path = "zig/row", .depends_on = &[_][]const u8{ "btr", "rem", "dict", "trx", "mem", "ut" } },
    .{ .name = "read", .c_dir = "read", .zig_path = "zig/read", .depends_on = &[_][]const u8{ "trx", "row", "mem", "ut" } },
    .{ .name = "que", .c_dir = "que", .zig_path = "zig/que", .depends_on = &[_][]const u8{ "row", "trx", "dict", "read", "mem", "ut" } },
    .{ .name = "pars", .c_dir = "pars", .zig_path = "zig/pars", .depends_on = &[_][]const u8{ "que", "mem", "ut" } },
    .{ .name = "eval", .c_dir = "eval", .zig_path = "zig/eval", .depends_on = &[_][]const u8{ "data", "row", "que", "mem", "ut" } },
    .{ .name = "ddl", .c_dir = "ddl", .zig_path = "zig/ddl", .depends_on = &[_][]const u8{ "dict", "row", "trx", "que", "mem", "ut" } },
    .{ .name = "usr", .c_dir = "usr", .zig_path = "zig/usr", .depends_on = &[_][]const u8{ "trx", "mem", "ut" } },
    .{ .name = "api", .c_dir = "api", .zig_path = "zig/api", .depends_on = &[_][]const u8{ "srv", "trx", "dict", "mem", "ut" } },
    .{ .name = "ha", .c_dir = "ha", .zig_path = "zig/ha", .depends_on = &[_][]const u8{ "api", "srv", "mem", "ut" } },
    .{ .name = "srv", .c_dir = "srv", .zig_path = "zig/srv", .depends_on = &[_][]const u8{ "os", "sync", "thr", "log", "fil", "buf", "trx", "dict", "row", "mem", "ut" } },
};

fn hasModule(name: []const u8) bool {
    for (modules) |mod| {
        if (std.mem.eql(u8, mod.name, name)) {
            return true;
        }
    }
    return false;
}

test "module map names and dependencies are valid" {
    for (modules, 0..) |mod, i| {
        for (modules[0..i]) |prev| {
            try std.testing.expect(!std.mem.eql(u8, mod.name, prev.name));
        }
        try std.testing.expect(std.mem.eql(u8, mod.c_dir, mod.name));
        try std.testing.expect(std.mem.startsWith(u8, mod.zig_path, "zig/"));
        try std.testing.expect(std.mem.eql(u8, mod.zig_path["zig/".len..], mod.name));

        for (mod.depends_on) |dep| {
            try std.testing.expect(!std.mem.eql(u8, dep, mod.name));
            try std.testing.expect(hasModule(dep));
        }
    }
}
