const std = @import("std");
const btr = @import("../btr/mod.zig");
const compat = @import("../ut/compat.zig");
const data = @import("../data/mod.zig");
const page = @import("../page/mod.zig");

fn dtupleKey(tuple: *const data.dtuple_t) i64 {
    if (tuple.n_fields == 0) {
        return 0;
    }
    const field = &tuple.fields[0];
    const ptr = data.dfield_get_data(field) orelse return 0;
    const len = data.dfield_get_len(field);
    const bytes = @as([*]const u8, @ptrCast(ptr));
    return switch (len) {
        4 => @as(i64, @intCast(std.mem.readInt(i32, bytes[0..4], .little))),
        8 => std.mem.readInt(i64, bytes[0..8], .little),
        else => 0,
    };
}

pub fn row_ins_simple_insert(index: *btr.dict_index_t, entry: *data.dtuple_t) ?*page.rec_t {
    const key = dtupleKey(entry);
    const block = btr.btr_find_leaf_for_key(index, key) orelse return null;
    var cursor = btr.btr_cur_t{ .index = index, .block = block, .rec = null, .opened = true };
    var rec_out: ?*page.rec_t = null;
    var big_out: ?*data.big_rec_t = null;
    var mtr = btr.mtr_t{};
    if (btr.btr_cur_optimistic_insert(0, &cursor, entry, &rec_out, &big_out, 0, null, &mtr) != 0) {
        return null;
    }
    return rec_out;
}

test "row simple insert orders keys" {
    var index = btr.dict_index_t{};
    var mtr = btr.mtr_t{};
    _ = btr.btr_create(0, 1, 0, .{ .high = 0, .low = 10 }, &index, &mtr);

    var key_a: i64 = 5;
    var fields_a = [_]data.dfield_t{.{ .data = &key_a, .len = @intCast(@sizeOf(i64)) }};
    var tuple_a = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = fields_a[0..] };
    var key_b: i64 = 1;
    var fields_b = [_]data.dfield_t{.{ .data = &key_b, .len = @intCast(@sizeOf(i64)) }};
    var tuple_b = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = fields_b[0..] };
    var key_c: i64 = 3;
    var fields_c = [_]data.dfield_t{.{ .data = &key_c, .len = @intCast(@sizeOf(i64)) }};
    var tuple_c = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = fields_c[0..] };

    try std.testing.expect(row_ins_simple_insert(&index, &tuple_a) != null);
    try std.testing.expect(row_ins_simple_insert(&index, &tuple_b) != null);
    try std.testing.expect(row_ins_simple_insert(&index, &tuple_c) != null);

    var cursor = btr.btr_cur_t{};
    btr.btr_cur_open_at_index_side_func(compat.TRUE, &index, 0, &cursor, "row", 0, &mtr);
    var node = btr.btr_get_next_user_rec(cursor.rec, null);
    const expected = [_]i64{ 1, 3, 5 };
    var idx: usize = 0;
    while (node) |rec| {
        try std.testing.expectEqual(expected[idx], rec.key);
        idx += 1;
        node = btr.btr_get_next_user_rec(rec, null);
    }
    try std.testing.expectEqual(@as(usize, 3), idx);
    btr.btr_free_index(&index);
}
