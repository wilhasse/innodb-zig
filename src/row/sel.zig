const std = @import("std");
const page = @import("../page/mod.zig");

pub fn row_sel_simple(page_obj: *page.page_t, allocator: std.mem.Allocator) []i64 {
    var count: usize = 0;
    var node = page_obj.infimum.next;
    while (node) |rec| {
        if (rec.is_supremum) break;
        if (!rec.deleted) {
            count += 1;
        }
        node = rec.next;
    }
    const out = allocator.alloc(i64, count) catch @panic("row_sel_simple");
    var idx: usize = 0;
    node = page_obj.infimum.next;
    while (node) |rec| {
        if (rec.is_supremum) break;
        if (!rec.deleted) {
            out[idx] = rec.key;
            idx += 1;
        }
        node = rec.next;
    }
    return out;
}

test "row select simple collects keys" {
    var page_obj = page.page_t{};
    page.page_init(&page_obj);
    const allocator = std.testing.allocator;
    const rec1 = allocator.create(page.rec_t) catch unreachable;
    const rec2 = allocator.create(page.rec_t) catch unreachable;
    rec1.* = .{ .key = 1 };
    rec2.* = .{ .key = 2, .deleted = true };
    page_obj.infimum.next = rec1;
    rec1.prev = &page_obj.infimum;
    rec1.next = rec2;
    rec2.prev = rec1;
    rec2.next = &page_obj.supremum;
    page_obj.supremum.prev = rec2;
    page_obj.header.n_recs = 2;

    const rows = row_sel_simple(&page_obj, allocator);
    defer allocator.free(rows);
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqual(@as(i64, 1), rows[0]);

    allocator.destroy(rec1);
    allocator.destroy(rec2);
}
