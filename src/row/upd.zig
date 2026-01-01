const std = @import("std");
const btr = @import("../btr/mod.zig");
const data = @import("../data/mod.zig");
const page = @import("../page/mod.zig");
const ins = @import("ins.zig");

pub fn row_upd_simple(index: *btr.dict_index_t, rec: *page.rec_t, new_key: i64) ?*page.rec_t {
    const block = btr.btr_block_for_rec(index, rec) orelse return null;
    var cursor = btr.btr_cur_t{ .index = index, .block = block, .rec = rec, .opened = true };
    var update = btr.upd_t{ .new_key = new_key, .size_change = true };
    var heap: ?*btr.mem_heap_t = null;
    var big: ?*btr.big_rec_t = null;
    var thr = btr.que_thr_t{};
    var mtr = btr.mtr_t{};
    if (btr.btr_cur_pessimistic_update(0, &cursor, &heap, &big, &update, 0, &thr, &mtr) != 0) {
        return null;
    }
    return cursor.rec;
}

test "row update simple" {
    var index = btr.dict_index_t{};
    var mtr = btr.mtr_t{};
    _ = btr.btr_create(0, 1, 0, .{ .high = 0, .low = 30 }, &index, &mtr);

    var key_val: i64 = 1;
    var fields = [_]data.dfield_t{.{ .data = &key_val, .len = @intCast(@sizeOf(i64)) }};
    var tuple = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = fields[0..] };
    _ = ins.row_ins_simple_insert(&index, &tuple) orelse return error.TestExpectedEqual;
    const found = btr.btr_find_rec_by_key(&index, 1) orelse return error.TestExpectedEqual;
    const updated = row_upd_simple(&index, found, 9) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(i64, 9), updated.key);
    try std.testing.expect(btr.btr_find_rec_by_key(&index, 9) != null);
    btr.btr_free_index(&index);
}
