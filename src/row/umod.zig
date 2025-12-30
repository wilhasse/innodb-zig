const std = @import("std");
const page = @import("../page/mod.zig");

pub fn row_undo_mod_simple(rec: *page.rec_t, old_key: i64) void {
    rec.key = old_key;
}

test "row undo modify restores key" {
    var rec = page.rec_t{ .key = 5 };
    row_undo_mod_simple(&rec, 2);
    try std.testing.expectEqual(@as(i64, 2), rec.key);
}
