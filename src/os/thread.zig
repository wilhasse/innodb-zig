const std = @import("std");

pub const ThreadId = std.Thread.Id;

pub const Thread = struct {
    handle: std.Thread,

    pub fn join(self: Thread) void {
        self.handle.join();
    }
};

pub fn spawn(comptime start: anytype, args: anytype) !Thread {
    return .{ .handle = try std.Thread.spawn(.{}, start, args) };
}

pub fn currentId() ThreadId {
    return std.Thread.getCurrentId();
}

pub fn yield() void {
    std.Thread.yield();
}

pub fn sleepMicros(us: u64) void {
    std.time.sleep(us * std.time.ns_per_us);
}

test "thread spawn/join updates flag" {
    var flag = std.atomic.Value(u32).init(0);

    const worker = struct {
        fn run(ptr: *std.atomic.Value(u32)) void {
            ptr.store(1, .seq_cst);
        }
    };

    const thread = try spawn(worker.run, .{&flag});
    thread.join();

    try std.testing.expectEqual(@as(u32, 1), flag.load(.seq_cst));
}
