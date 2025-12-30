const std = @import("std");
const page = @import("../page/mod.zig");

pub fn row_ins_simple_insert(target: *page.page_t, key: i64, allocator: std.mem.Allocator) *page.rec_t {
    const rec = allocator.create(page.rec_t) catch @panic("row_ins_simple_insert");
    rec.* = .{ .key = key };

    var cur = target.infimum.next;
    while (cur) |node| {
        if (node.is_supremum or node.key > key) {
            break;
        }
        cur = node.next;
    }

    const insert_before = cur orelse &target.supremum;
    const prev = insert_before.prev orelse &target.infimum;
    rec.prev = prev;
    rec.next = insert_before;
    prev.next = rec;
    insert_before.prev = rec;
    target.header.n_recs += 1;
    return rec;
}

test "row simple insert orders keys" {
    var page_obj = page.page_t{};
    page.page_init(&page_obj);

    const allocator = std.testing.allocator;
    _ = row_ins_simple_insert(&page_obj, 5, allocator);
    _ = row_ins_simple_insert(&page_obj, 1, allocator);
    _ = row_ins_simple_insert(&page_obj, 3, allocator);

    var node = page_obj.infimum.next;
    const expected = [_]i64{ 1, 3, 5 };
    var idx: usize = 0;
    while (node) |rec| {
        if (rec.is_supremum) break;
        try std.testing.expectEqual(expected[idx], rec.key);
        idx += 1;
        node = rec.next;
    }
    try std.testing.expectEqual(@as(usize, 3), idx);

    node = page_obj.infimum.next;
    while (node) |rec| {
        if (rec.is_supremum) break;
        const next = rec.next;
        allocator.destroy(rec);
        node = next;
    }
}
