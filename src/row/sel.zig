const std = @import("std");
const btr = @import("../btr/mod.zig");
const compat = @import("../ut/compat.zig");
const data = @import("../data/mod.zig");
const ins = @import("ins.zig");
const read = @import("../read/mod.zig");
const trx_types = @import("../trx/types.zig");
const vers = @import("vers.zig");

fn row_sel_visible_key(
    rec: *btr.rec_t,
    view: ?*read.read_view_t,
    isolation_level: trx_types.ulint,
) ?i64 {
    const repeatable = isolation_level == trx_types.TRX_ISO_REPEATABLE_READ or
        isolation_level == trx_types.TRX_ISO_SERIALIZABLE;

    if (repeatable) {
        if (view) |rv| {
            if (rec.payload) |payload| {
                const head: ?*vers.RowVersion = @ptrCast(@alignCast(payload));
                if (head) |chain| {
                    const result = vers.row_vers_build_for_consistent_read(chain, rv);
                    if (result.result == vers.VersionResult.visible and result.version != null and !result.version.?.deleted) {
                        return result.version.?.key;
                    }
                    return null;
                }
            } else if (!rec.deleted) {
                return rec.key;
            }
            return null;
        }

        if (!rec.deleted) {
            return rec.key;
        }
        return null;
    }

    if (rec.payload) |payload| {
        const head: ?*vers.RowVersion = @ptrCast(@alignCast(payload));
        if (head) |chain| {
            if (!chain.deleted) {
                return chain.key;
            }
            return null;
        }
    }

    if (!rec.deleted) {
        return rec.key;
    }
    return null;
}

pub fn row_sel_simple_isolation(
    index: *btr.dict_index_t,
    view: ?*read.read_view_t,
    isolation_level: trx_types.ulint,
    allocator: std.mem.Allocator,
) []i64 {
    var cursor = btr.btr_cur_t{};
    var mtr = btr.mtr_t{};
    btr.btr_cur_open_at_index_side_func(compat.TRUE, index, 0, &cursor, "row", 0, &mtr);
    var node = btr.btr_get_next_user_rec(cursor.rec, null);

    var list = std.ArrayList(i64).init(allocator);
    defer list.deinit();
    while (node) |rec| {
        if (row_sel_visible_key(rec, view, isolation_level)) |key| {
            list.append(key) catch @panic("row_sel_simple");
        }
        node = btr.btr_get_next_user_rec(rec, null);
    }

    const out = allocator.alloc(i64, list.items.len) catch @panic("row_sel_simple");
    std.mem.copyForwards(i64, out, list.items);
    return out;
}

pub fn row_sel_simple(index: *btr.dict_index_t, view: ?*read.read_view_t, allocator: std.mem.Allocator) []i64 {
    return row_sel_simple_isolation(index, view, trx_types.TRX_ISO_REPEATABLE_READ, allocator);
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

    const rows = row_sel_simple(&index, null, std.testing.allocator);
    defer std.testing.allocator.free(rows);
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqual(@as(i64, 1), rows[0]);
    btr.btr_free_index(&index);
}

test "row select with view uses version chain" {
    var index = btr.dict_index_t{};
    var mtr = btr.mtr_t{};
    _ = btr.btr_create(0, 1, 0, .{ .high = 0, .low = 25 }, &index, &mtr);

    var key_val: i64 = 1;
    var fields = [_]data.dfield_t{.{ .data = &key_val, .len = @intCast(@sizeOf(i64)) }};
    var tuple = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = fields[0..] };
    const rec = ins.row_ins_simple_insert(&index, &tuple) orelse return error.OutOfMemory;

    var head: ?*vers.RowVersion = null;
    head = vers.row_version_add_with_trx(head, 10, false, 10, std.testing.allocator);
    head = vers.row_version_add_with_trx(head, 20, false, 30, std.testing.allocator);
    head = vers.row_version_add_with_trx(head, 30, false, 50, std.testing.allocator);
    defer vers.row_version_free(head, std.testing.allocator);

    rec.payload = @ptrCast(@alignCast(head.?));
    rec.deleted = true;

    const active = [_]read.trx_id_t{30};
    const view = read.read_view_open_with_active(40, 60, &active, std.testing.allocator);
    defer read.read_view_close(view);

    const rows = row_sel_simple(&index, view, std.testing.allocator);
    defer std.testing.allocator.free(rows);
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqual(@as(i64, 10), rows[0]);
    btr.btr_free_index(&index);
}

test "row select read committed ignores view for secondary reads" {
    var index = btr.dict_index_t{};
    var mtr = btr.mtr_t{};
    _ = btr.btr_create(0, 1, 0, .{ .high = 0, .low = 30 }, &index, &mtr);

    var key_val: i64 = 1;
    var fields = [_]data.dfield_t{.{ .data = &key_val, .len = @intCast(@sizeOf(i64)) }};
    var tuple = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = fields[0..] };
    const rec = ins.row_ins_simple_insert(&index, &tuple) orelse return error.OutOfMemory;

    var head: ?*vers.RowVersion = null;
    head = vers.row_version_add_with_trx(head, 10, false, 10, std.testing.allocator);
    head = vers.row_version_add_with_trx(head, 20, false, 20, std.testing.allocator);
    head = vers.row_version_add_with_trx(head, 30, false, 30, std.testing.allocator);
    defer vers.row_version_free(head, std.testing.allocator);

    rec.payload = @ptrCast(@alignCast(head.?));
    rec.deleted = true;

    const view = read.read_view_open_with_active(15, 25, &[_]read.trx_id_t{}, std.testing.allocator);
    defer read.read_view_close(view);

    const repeatable = row_sel_simple_isolation(&index, view, trx_types.TRX_ISO_REPEATABLE_READ, std.testing.allocator);
    defer std.testing.allocator.free(repeatable);
    try std.testing.expectEqual(@as(usize, 1), repeatable.len);
    try std.testing.expectEqual(@as(i64, 20), repeatable[0]);

    const read_committed = row_sel_simple_isolation(&index, view, trx_types.TRX_ISO_READ_COMMITTED, std.testing.allocator);
    defer std.testing.allocator.free(read_committed);
    try std.testing.expectEqual(@as(usize, 1), read_committed.len);
    try std.testing.expectEqual(@as(i64, 30), read_committed[0]);

    btr.btr_free_index(&index);
}
