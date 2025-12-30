const std = @import("std");
const page = @import("../page/mod.zig");

pub const UndoOp = enum { insert, delete, modify };

pub const UndoEntry = struct {
    op: UndoOp,
    rec: *page.rec_t,
    old_key: i64 = 0,
};

pub const UndoLog = struct {
    entries: std.ArrayList(UndoEntry),

    pub fn init(allocator: std.mem.Allocator) UndoLog {
        return .{ .entries = std.ArrayList(UndoEntry).init(allocator) };
    }

    pub fn deinit(self: *UndoLog) void {
        self.entries.deinit();
    }

    pub fn addInsert(self: *UndoLog, rec: *page.rec_t) void {
        self.entries.append(.{ .op = .insert, .rec = rec }) catch @panic("addInsert");
    }

    pub fn addDelete(self: *UndoLog, rec: *page.rec_t) void {
        self.entries.append(.{ .op = .delete, .rec = rec }) catch @panic("addDelete");
    }

    pub fn addModify(self: *UndoLog, rec: *page.rec_t, old_key: i64) void {
        self.entries.append(.{ .op = .modify, .rec = rec, .old_key = old_key }) catch @panic("addModify");
    }

    pub fn apply(self: *UndoLog) void {
        var i = self.entries.items.len;
        while (i > 0) {
            i -= 1;
            const entry = self.entries.items[i];
            switch (entry.op) {
                .insert => entry.rec.deleted = true,
                .delete => entry.rec.deleted = false,
                .modify => entry.rec.key = entry.old_key,
            }
        }
    }
};

test "undo log apply restores state" {
    var rec = page.rec_t{ .key = 10, .deleted = true };
    var log = UndoLog.init(std.testing.allocator);
    defer log.deinit();
    log.addModify(&rec, 3);
    log.addDelete(&rec);
    log.apply();
    try std.testing.expectEqual(@as(i64, 3), rec.key);
    try std.testing.expectEqual(false, rec.deleted);
}

test "undo log insert marks deleted" {
    var rec = page.rec_t{ .key = 1 };
    var log = UndoLog.init(std.testing.allocator);
    defer log.deinit();
    log.addInsert(&rec);
    log.apply();
    try std.testing.expect(rec.deleted);
}
