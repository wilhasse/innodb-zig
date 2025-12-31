const std = @import("std");

pub fn utf8_casedown(buf: []u8) void {
    for (buf) |*ch| {
        ch.* = std.ascii.toLower(ch.*);
    }
}
