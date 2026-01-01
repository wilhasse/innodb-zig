const std = @import("std");
const master = @import("master.zig");

pub const SrvState = enum { stopped, running };

pub const SrvCore = struct {
    state: SrvState = .stopped,
    master: master.SrvMaster,

    pub fn init(allocator: std.mem.Allocator) SrvCore {
        return .{
            .state = .stopped,
            .master = master.SrvMaster.init(allocator, master.DEFAULT_TICK_US),
        };
    }

    pub fn deinit(self: *SrvCore) void {
        self.master.deinit();
    }

    pub fn start(self: *SrvCore) void {
        self.state = .running;
        self.master.start() catch @panic("srv master start failed");
    }

    pub fn stop(self: *SrvCore) void {
        self.master.stop();
        self.state = .stopped;
    }
};

test "srv core lifecycle" {
    var core = SrvCore.init(std.testing.allocator);
    defer core.deinit();
    try std.testing.expect(core.state == .stopped);
    core.start();
    try std.testing.expect(core.state == .running);
    core.stop();
    try std.testing.expect(core.state == .stopped);
}
