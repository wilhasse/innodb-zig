const std = @import("std");
const btr = @import("../btr/mod.zig");
const compat = @import("../ut/compat.zig");
const data = @import("../data/mod.zig");
const format = @import("format.zig");
const ins = @import("ins.zig");
const sel = @import("sel.zig");

pub fn row_merge_sorted(a: []const i64, b: []const i64, allocator: std.mem.Allocator) []i64 {
    const out = allocator.alloc(i64, a.len + b.len) catch @panic("row_merge_sorted");
    var i: usize = 0;
    var j: usize = 0;
    var k: usize = 0;
    while (i < a.len and j < b.len) : (k += 1) {
        if (a[i] <= b[j]) {
            out[k] = a[i];
            i += 1;
        } else {
            out[k] = b[j];
            j += 1;
        }
    }
    while (i < a.len) : (i += 1) {
        out[k] = a[i];
        k += 1;
    }
    while (j < b.len) : (j += 1) {
        out[k] = b[j];
        k += 1;
    }
    return out;
}

pub fn row_merge_build_index(
    clustered: *btr.dict_index_t,
    secondary: *btr.dict_index_t,
    allocator: std.mem.Allocator,
) bool {
    var cursor = btr.btr_cur_t{};
    var mtr = btr.mtr_t{};
    btr.btr_cur_open_at_index_side_func(compat.TRUE, clustered, 0, &cursor, "row", 0, &mtr);
    var rec_opt = btr.btr_get_next_user_rec(cursor.rec, null);
    while (rec_opt) |rec| {
        const entry = format.row_build_index_entry_simple(rec, allocator);
        defer format.row_free_tuple_simple(entry, allocator);
        if (ins.row_ins_simple_insert(secondary, entry) == null) {
            return false;
        }
        rec_opt = btr.btr_get_next_user_rec(rec, null);
    }
    return true;
}

test "row merge sorted arrays" {
    const allocator = std.testing.allocator;
    const a = [_]i64{ 1, 3, 5 };
    const b = [_]i64{ 2, 4, 6 };
    const merged = row_merge_sorted(a[0..], b[0..], allocator);
    defer allocator.free(merged);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3, 4, 5, 6 }, merged);
}

test "row merge build secondary index from clustered" {
    const allocator = std.testing.allocator;
    var clustered = btr.dict_index_t{};
    var secondary = btr.dict_index_t{};
    var mtr = btr.mtr_t{};
    _ = btr.btr_create(0, 1, 0, .{ .high = 0, .low = 40 }, &clustered, &mtr);
    _ = btr.btr_create(0, 1, 0, .{ .high = 0, .low = 41 }, &secondary, &mtr);

    var key_a: i64 = 3;
    var fields_a = [_]data.dfield_t{.{ .data = &key_a, .len = @intCast(@sizeOf(i64)) }};
    var tuple_a = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = fields_a[0..] };
    var key_b: i64 = 1;
    var fields_b = [_]data.dfield_t{.{ .data = &key_b, .len = @intCast(@sizeOf(i64)) }};
    var tuple_b = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = fields_b[0..] };
    var key_c: i64 = 2;
    var fields_c = [_]data.dfield_t{.{ .data = &key_c, .len = @intCast(@sizeOf(i64)) }};
    var tuple_c = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = fields_c[0..] };

    try std.testing.expect(ins.row_ins_simple_insert(&clustered, &tuple_a) != null);
    try std.testing.expect(ins.row_ins_simple_insert(&clustered, &tuple_b) != null);
    try std.testing.expect(ins.row_ins_simple_insert(&clustered, &tuple_c) != null);

    try std.testing.expect(row_merge_build_index(&clustered, &secondary, allocator));
    const rows = sel.row_sel_simple(&secondary, null, allocator);
    defer allocator.free(rows);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3 }, rows);

    btr.btr_free_index(&clustered);
    btr.btr_free_index(&secondary);
}
