const std = @import("std");
const core = @import("core.zig");

pub fn srv_startup(srv: *core.SrvCore) void {
    srv.start();
}

pub fn srv_shutdown(srv: *core.SrvCore) void {
    srv.stop();
}

test "srv startup/shutdown" {
    var srv = core.SrvCore{};
    srv_startup(&srv);
    try std.testing.expect(srv.state == .running);
    srv_shutdown(&srv);
    try std.testing.expect(srv.state == .stopped);
}
