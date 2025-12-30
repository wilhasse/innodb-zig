const std = @import("std");
const page = @import("../page/mod.zig");

pub fn row_upd_simple(rec: *page.rec_t, new_key: i64) void {
    rec.key = new_key;
}

test "row update simple" {
    var rec = page.rec_t{ .key = 1 };
    row_upd_simple(&rec, 9);
    try std.testing.expectEqual(@as(i64, 9), rec.key);
}
