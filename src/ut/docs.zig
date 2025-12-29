const std = @import("std");

fn docContains(path: []const u8, needle: []const u8) !bool {
    const file = try std.fs.cwd().openFile(path, .{});
    defer file.close();
    const contents = try file.readToEndAlloc(std.testing.allocator, 128 * 1024);
    defer std.testing.allocator.free(contents);
    return std.mem.indexOf(u8, contents, needle) != null;
}

test "porting conventions doc exists" {
    try std.testing.expect(try docContains("docs/porting_conventions.md", "Zig Porting Conventions"));
}
