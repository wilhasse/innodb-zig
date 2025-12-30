const std = @import("std");
const compat = @import("../ut/compat.zig");
const trx = @import("../trx/mod.zig");

pub const module_name = "read";

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;
pub const trx_id_t = compat.ib_uint64_t;
pub const undo_no_t = compat.ib_uint64_t;

pub const VIEW_NORMAL: ulint = 1;
pub const VIEW_HIGH_GRANULARITY: ulint = 2;

pub const read_view_t = struct {
    type: ulint = VIEW_NORMAL,
    undo_no: undo_no_t = 0,
    low_limit_no: trx_id_t = 0,
    low_limit_id: trx_id_t = 0,
    up_limit_id: trx_id_t = 0,
    n_trx_ids: ulint = 0,
    trx_ids: []trx_id_t = &[_]trx_id_t{},
    creator_trx_id: trx_id_t = 0,
    allocator: std.mem.Allocator = std.heap.page_allocator,
};

pub const cursor_view_t = struct {
    read_view: ?*read_view_t = null,
    n_client_tables_in_use: ulint = 0,
    allocator: std.mem.Allocator = std.heap.page_allocator,
};

fn read_view_create_low(n: ulint, allocator: std.mem.Allocator) *read_view_t {
    const view = allocator.create(read_view_t) catch @panic("read_view_create_low");
    view.* = .{};
    view.n_trx_ids = n;
    view.allocator = allocator;
    if (n > 0) {
        view.trx_ids = allocator.alloc(trx_id_t, n) catch @panic("read_view_create_low");
    } else {
        view.trx_ids = &[_]trx_id_t{};
    }
    return view;
}

pub fn read_view_open_now(cr_trx_id: trx_id_t, allocator: std.mem.Allocator) *read_view_t {
    const view = read_view_create_low(0, allocator);
    view.creator_trx_id = cr_trx_id;
    view.low_limit_id = cr_trx_id;
    view.up_limit_id = cr_trx_id;
    view.low_limit_no = cr_trx_id;
    return view;
}

pub fn read_view_oldest_copy_or_open_new(cr_trx_id: trx_id_t, allocator: std.mem.Allocator) *read_view_t {
    return read_view_open_now(cr_trx_id, allocator);
}

pub fn read_view_close(view: *read_view_t) void {
    if (view.trx_ids.len > 0) {
        view.allocator.free(view.trx_ids);
    }
    view.allocator.destroy(view);
}

pub fn read_view_close_for_read_committed(trx_: *trx.trx_t) void {
    _ = trx_;
}

pub fn read_view_sees_trx_id(view: *const read_view_t, trx_id: trx_id_t) ibool {
    if (trx_id < view.up_limit_id) {
        return compat.TRUE;
    }
    if (trx_id >= view.low_limit_id) {
        return compat.FALSE;
    }
    for (view.trx_ids) |id| {
        if (id == trx_id) {
            return compat.FALSE;
        }
    }
    return compat.TRUE;
}

pub fn read_view_print(view: *const read_view_t) void {
    std.debug.print("read_view: low_limit_id={d} up_limit_id={d} n_trx_ids={d}\n", .{
        view.low_limit_id,
        view.up_limit_id,
        view.n_trx_ids,
    });
}

pub fn read_cursor_view_create(trx_: *trx.trx_t, allocator: std.mem.Allocator) *cursor_view_t {
    const cur = allocator.create(cursor_view_t) catch @panic("read_cursor_view_create");
    cur.* = .{};
    cur.allocator = allocator;
    cur.read_view = read_view_open_now(trx_.id, allocator);
    return cur;
}

pub fn read_cursor_view_close(trx_: *trx.trx_t, cur: *cursor_view_t) void {
    _ = trx_;
    if (cur.read_view) |view| {
        read_view_close(view);
    }
    cur.allocator.destroy(cur);
}

pub fn read_cursor_set(trx_: *trx.trx_t, curview: ?*cursor_view_t) void {
    _ = trx_;
    _ = curview;
}

test "read view open now defaults" {
    const allocator = std.testing.allocator;
    const view = read_view_open_now(10, allocator);
    defer read_view_close(view);
    try std.testing.expectEqual(@as(ulint, 0), view.n_trx_ids);
    try std.testing.expectEqual(@as(trx_id_t, 10), view.low_limit_id);
    try std.testing.expectEqual(@as(trx_id_t, 10), view.up_limit_id);
}

test "read view sees trx id logic" {
    const allocator = std.testing.allocator;
    const view = read_view_create_low(1, allocator);
    defer read_view_close(view);
    view.up_limit_id = 5;
    view.low_limit_id = 10;
    view.trx_ids[0] = 8;
    try std.testing.expectEqual(compat.TRUE, read_view_sees_trx_id(view, 4));
    try std.testing.expectEqual(compat.FALSE, read_view_sees_trx_id(view, 8));
    try std.testing.expectEqual(compat.TRUE, read_view_sees_trx_id(view, 9));
    try std.testing.expectEqual(compat.FALSE, read_view_sees_trx_id(view, 10));
}
