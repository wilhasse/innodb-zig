const std = @import("std");

pub const SrvState = enum { stopped, running };

pub const SrvCore = struct {
    state: SrvState = .stopped,

    pub fn start(self: *SrvCore) void {
        self.state = .running;
    }

    pub fn stop(self: *SrvCore) void {
        self.state = .stopped;
    }
};

test "srv core lifecycle" {
    var core = SrvCore{};
    try std.testing.expect(core.state == .stopped);
    core.start();
    try std.testing.expect(core.state == .running);
    core.stop();
    try std.testing.expect(core.state == .stopped);
}
