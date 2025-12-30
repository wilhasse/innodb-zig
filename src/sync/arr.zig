const std = @import("std");
const sync = @import("mod.zig");

pub const SyncArray = struct {
    mutex: sync.Mutex = .{},
    slots: []bool,

    pub fn init(allocator: std.mem.Allocator, n: usize) SyncArray {
        return .{ .slots = allocator.alloc(bool, n) catch @panic("SyncArray.init") };
    }

    pub fn deinit(self: *SyncArray, allocator: std.mem.Allocator) void {
        allocator.free(self.slots);
    }

    pub fn reserve(self: *SyncArray) ?usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        for (self.slots, 0..) |used, i| {
            if (!used) {
                self.slots[i] = true;
                return i;
            }
        }
        return null;
    }

    pub fn release(self: *SyncArray, index: usize) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (index < self.slots.len) {
            self.slots[index] = false;
        }
    }
};

test "sync array reserve/release" {
    var arr = SyncArray.init(std.testing.allocator, 2);
    defer arr.deinit(std.testing.allocator);
    const a = arr.reserve().?;
    const b = arr.reserve().?;
    try std.testing.expect(arr.reserve() == null);
    arr.release(a);
    try std.testing.expect(arr.reserve().? == a);
    arr.release(b);
}
