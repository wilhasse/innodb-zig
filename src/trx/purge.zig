const std = @import("std");
const page = @import("../page/mod.zig");

pub const TrxPurge = struct {
    pending: std.ArrayList(*page.rec_t),

    pub fn init(allocator: std.mem.Allocator) TrxPurge {
        return .{ .pending = std.ArrayList(*page.rec_t).init(allocator) };
    }

    pub fn deinit(self: *TrxPurge) void {
        self.pending.deinit();
    }

    pub fn add(self: *TrxPurge, rec: *page.rec_t) void {
        self.pending.append(rec) catch @panic("TrxPurge.add");
    }

    pub fn apply(self: *TrxPurge) usize {
        var count: usize = 0;
        for (self.pending.items) |rec| {
            rec.deleted = true;
            count += 1;
        }
        self.pending.clearRetainingCapacity();
        return count;
    }
};

test "trx purge apply marks deleted" {
    var purge = TrxPurge.init(std.testing.allocator);
    defer purge.deinit();
    var rec1 = page.rec_t{ .key = 1 };
    var rec2 = page.rec_t{ .key = 2 };
    purge.add(&rec1);
    purge.add(&rec2);
    const n = purge.apply();
    try std.testing.expectEqual(@as(usize, 2), n);
    try std.testing.expect(rec1.deleted);
    try std.testing.expect(rec2.deleted);
}
