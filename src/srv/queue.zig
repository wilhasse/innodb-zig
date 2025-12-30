const std = @import("std");

pub const SrvQueue = struct {
    items: std.ArrayList(u64),

    pub fn init(allocator: std.mem.Allocator) SrvQueue {
        return .{ .items = std.ArrayList(u64).init(allocator) };
    }

    pub fn deinit(self: *SrvQueue) void {
        self.items.deinit();
    }

    pub fn push(self: *SrvQueue, value: u64) void {
        self.items.append(value) catch @panic("SrvQueue.push");
    }

    pub fn pop(self: *SrvQueue) ?u64 {
        if (self.items.items.len == 0) {
            return null;
        }
        return self.items.orderedRemove(0);
    }
};

test "srv queue push/pop" {
    var q = SrvQueue.init(std.testing.allocator);
    defer q.deinit();
    q.push(1);
    q.push(2);
    try std.testing.expectEqual(@as(u64, 1), q.pop().?);
    try std.testing.expectEqual(@as(u64, 2), q.pop().?);
    try std.testing.expect(q.pop() == null);
}
