const std = @import("std");
const sync = @import("mod.zig");

pub const RwLockEx = struct {
    inner: sync.RwLock = .{},
    readers: usize = 0,
    writer: bool = false,

    pub fn lockShared(self: *RwLockEx) void {
        self.inner.lockShared();
        self.readers += 1;
    }

    pub fn unlockShared(self: *RwLockEx) void {
        if (self.readers > 0) {
            self.readers -= 1;
        }
        self.inner.unlockShared();
    }

    pub fn lock(self: *RwLockEx) void {
        self.inner.lock();
        self.writer = true;
    }

    pub fn unlock(self: *RwLockEx) void {
        self.writer = false;
        self.inner.unlock();
    }
};

test "rwlock ex counters" {
    var rw = RwLockEx{};
    rw.lockShared();
    try std.testing.expectEqual(@as(usize, 1), rw.readers);
    rw.unlockShared();
    try std.testing.expectEqual(@as(usize, 0), rw.readers);
    rw.lock();
    try std.testing.expect(rw.writer);
    rw.unlock();
    try std.testing.expect(!rw.writer);
}
