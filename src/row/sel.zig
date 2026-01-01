const std = @import("std");
const btr = @import("../btr/mod.zig");
const compat = @import("../ut/compat.zig");
const data = @import("../data/mod.zig");
const ins = @import("ins.zig");

pub fn row_sel_simple(index: *btr.dict_index_t, allocator: std.mem.Allocator) []i64 {
    var cursor = btr.btr_cur_t{};
    var mtr = btr.mtr_t{};
    btr.btr_cur_open_at_index_side_func(compat.TRUE, index, 0, &cursor, "row", 0, &mtr);
    var node = btr.btr_get_next_user_rec(cursor.rec, null);

    var list = std.ArrayList(i64).init(allocator);
    defer list.deinit();
    while (node) |rec| {
        if (!rec.deleted) {
            list.append(rec.key) catch @panic("row_sel_simple");
        }
        node = btr.btr_get_next_user_rec(rec, null);
    }

    const out = allocator.alloc(i64, list.items.len) catch @panic("row_sel_simple");
    std.mem.copyForwards(i64, out, list.items);
    return out;
}

test "row select simple collects keys" {
    var index = btr.dict_index_t{};
    var mtr = btr.mtr_t{};
    _ = btr.btr_create(0, 1, 0, .{ .high = 0, .low = 20 }, &index, &mtr);

    var key_a: i64 = 1;
    var fields_a = [_]data.dfield_t{.{ .data = &key_a, .len = @intCast(@sizeOf(i64)) }};
    var tuple_a = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = fields_a[0..] };
    var key_b: i64 = 2;
    var fields_b = [_]data.dfield_t{.{ .data = &key_b, .len = @intCast(@sizeOf(i64)) }};
    var tuple_b = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = fields_b[0..] };

    _ = ins.row_ins_simple_insert(&index, &tuple_a) orelse return error.OutOfMemory;
    const rec_b = ins.row_ins_simple_insert(&index, &tuple_b) orelse return error.OutOfMemory;
    rec_b.deleted = true;

    const rows = row_sel_simple(&index, std.testing.allocator);
    defer std.testing.allocator.free(rows);
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqual(@as(i64, 1), rows[0]);
    btr.btr_free_index(&index);
}
