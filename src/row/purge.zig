const std = @import("std");
const page = @import("../page/mod.zig");

pub fn row_purge_deleted(page_obj: *page.page_t, allocator: std.mem.Allocator) usize {
    var removed: usize = 0;
    var node = page_obj.infimum.next;
    while (node) |rec| {
        if (rec.is_supremum) break;
        const next = rec.next;
        if (rec.deleted) {
            const prev = rec.prev orelse &page_obj.infimum;
            const after = rec.next orelse &page_obj.supremum;
            prev.next = after;
            after.prev = prev;
            page_obj.header.n_recs -= 1;
            allocator.destroy(rec);
            removed += 1;
        }
        node = next;
    }
    return removed;
}

test "row purge deleted removes records" {
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

    const removed = row_purge_deleted(&page_obj, allocator);
    try std.testing.expectEqual(@as(usize, 1), removed);
    try std.testing.expectEqual(@as(usize, 1), page_obj.header.n_recs);
    try std.testing.expect(page_obj.infimum.next == rec1);
    try std.testing.expect(rec1.next == &page_obj.supremum);
    allocator.destroy(rec1);
}
