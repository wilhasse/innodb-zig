const std = @import("std");
const page = @import("../page/mod.zig");

pub fn row_undo_ins_simple(rec: *page.rec_t) void {
    rec.deleted = true;
}

test "row undo insert marks deleted" {
    var rec = page.rec_t{ .key = 1 };
    row_undo_ins_simple(&rec);
    try std.testing.expect(rec.deleted);
}
