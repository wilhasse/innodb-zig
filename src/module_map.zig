const std = @import("std");

pub const Module = struct {
    name: []const u8,
    c_dir: []const u8,
    zig_path: []const u8,
    depends_on: []const []const u8,
};

pub const modules = [_]Module{
    .{ .name = "ut", .c_dir = "ut", .zig_path = "src/ut", .depends_on = &[_][]const u8{} },
    .{ .name = "mach", .c_dir = "mach", .zig_path = "src/mach", .depends_on = &[_][]const u8{ "ut" } },
    .{ .name = "mem", .c_dir = "mem", .zig_path = "src/mem", .depends_on = &[_][]const u8{ "ut" } },
    .{ .name = "os", .c_dir = "os", .zig_path = "src/os", .depends_on = &[_][]const u8{ "ut", "mem", "mach" } },
    .{ .name = "sync", .c_dir = "sync", .zig_path = "src/sync", .depends_on = &[_][]const u8{ "os", "ut" } },
    .{ .name = "thr", .c_dir = "thr", .zig_path = "src/thr", .depends_on = &[_][]const u8{ "os", "sync", "ut" } },
    .{ .name = "dyn", .c_dir = "dyn", .zig_path = "src/dyn", .depends_on = &[_][]const u8{ "mem", "ut" } },
    .{ .name = "data", .c_dir = "data", .zig_path = "src/data", .depends_on = &[_][]const u8{ "mach", "mem", "ut" } },
    .{ .name = "log", .c_dir = "log", .zig_path = "src/log", .depends_on = &[_][]const u8{ "os", "sync", "mach", "mem", "ut" } },
    .{ .name = "fil", .c_dir = "fil", .zig_path = "src/fil", .depends_on = &[_][]const u8{ "os", "sync", "mach", "mem", "ut" } },
    .{ .name = "fsp", .c_dir = "fsp", .zig_path = "src/fsp", .depends_on = &[_][]const u8{ "fil", "mach", "mem", "ut" } },
    .{ .name = "mtr", .c_dir = "mtr", .zig_path = "src/mtr", .depends_on = &[_][]const u8{ "log", "sync", "mach", "mem", "ut" } },
    .{ .name = "page", .c_dir = "page", .zig_path = "src/page", .depends_on = &[_][]const u8{ "mtr", "mach", "mem", "fil", "fsp", "rec", "ut" } },
    .{ .name = "buf", .c_dir = "buf", .zig_path = "src/buf", .depends_on = &[_][]const u8{ "fil", "fsp", "log", "mtr", "sync", "mach", "mem", "ut" } },
    .{ .name = "fut", .c_dir = "fut", .zig_path = "src/fut", .depends_on = &[_][]const u8{ "page", "buf", "mtr", "ut" } },
    .{ .name = "ibuf", .c_dir = "ibuf", .zig_path = "src/ibuf", .depends_on = &[_][]const u8{ "buf", "btr", "mtr", "page", "ut" } },
    .{ .name = "rem", .c_dir = "rem", .zig_path = "src/rem", .depends_on = &[_][]const u8{ "page", "mach", "mem", "ut" } },
    .{ .name = "rec", .c_dir = "rec", .zig_path = "src/rec", .depends_on = &[_][]const u8{ "data", "mach", "ut" } },
    .{ .name = "btr", .c_dir = "btr", .zig_path = "src/btr", .depends_on = &[_][]const u8{ "page", "buf", "mtr", "rem", "mem", "rec", "ut" } },
    .{ .name = "dict", .c_dir = "dict", .zig_path = "src/dict", .depends_on = &[_][]const u8{ "btr", "buf", "data", "mem", "ut" } },
    .{ .name = "lock", .c_dir = "lock", .zig_path = "src/lock", .depends_on = &[_][]const u8{ "dict", "mem", "ut" } },
    .{ .name = "trx", .c_dir = "trx", .zig_path = "src/trx", .depends_on = &[_][]const u8{ "log", "lock", "dict", "mem", "ut" } },
    .{ .name = "row", .c_dir = "row", .zig_path = "src/row", .depends_on = &[_][]const u8{ "btr", "rem", "dict", "trx", "mem", "ut" } },
    .{ .name = "read", .c_dir = "read", .zig_path = "src/read", .depends_on = &[_][]const u8{ "trx", "row", "mem", "ut" } },
    .{ .name = "que", .c_dir = "que", .zig_path = "src/que", .depends_on = &[_][]const u8{ "data", "row", "trx", "dict", "read", "mem", "ut" } },
    .{ .name = "pars", .c_dir = "pars", .zig_path = "src/pars", .depends_on = &[_][]const u8{ "que", "row", "mem", "ut" } },
    .{ .name = "eval", .c_dir = "eval", .zig_path = "src/eval", .depends_on = &[_][]const u8{ "data", "mach", "pars", "row", "que", "mem", "ut" } },
    .{ .name = "ddl", .c_dir = "ddl", .zig_path = "src/ddl", .depends_on = &[_][]const u8{ "dict", "row", "trx", "que", "mem", "ut" } },
    .{ .name = "usr", .c_dir = "usr", .zig_path = "src/usr", .depends_on = &[_][]const u8{ "trx", "mem", "ut" } },
    .{ .name = "api", .c_dir = "api", .zig_path = "src/api", .depends_on = &[_][]const u8{ "srv", "trx", "dict", "mem", "rec", "ut" } },
    .{ .name = "ha", .c_dir = "ha", .zig_path = "src/ha", .depends_on = &[_][]const u8{ "api", "srv", "mem", "ut" } },
    .{ .name = "srv", .c_dir = "srv", .zig_path = "src/srv", .depends_on = &[_][]const u8{ "os", "sync", "thr", "log", "fil", "buf", "trx", "dict", "row", "mem", "ut" } },
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
        try std.testing.expect(std.mem.startsWith(u8, mod.zig_path, "src/"));
        try std.testing.expect(std.mem.eql(u8, mod.zig_path["src/".len..], mod.name));

        for (mod.depends_on) |dep| {
            try std.testing.expect(!std.mem.eql(u8, dep, mod.name));
            try std.testing.expect(hasModule(dep));
        }

        const file_path = try std.fmt.allocPrint(
            std.testing.allocator,
            "{s}/mod.zig",
            .{mod.zig_path},
        );
        defer std.testing.allocator.free(file_path);
        const file = try std.fs.cwd().openFile(file_path, .{});
        file.close();
    }
}
