const std = @import("std");
const compat = @import("../ut/compat.zig");

pub const module_name = "page";

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;
pub const byte = compat.byte;

pub const PAGE_CUR_UNSUPP: ulint = 0;
pub const PAGE_CUR_G: ulint = 1;
pub const PAGE_CUR_GE: ulint = 2;
pub const PAGE_CUR_L: ulint = 3;
pub const PAGE_CUR_LE: ulint = 4;

pub const page_zip_des_t = struct {};
pub const dict_index_t = struct {};
pub const dtuple_t = struct {};
pub const mtr_t = struct {};

pub const rec_t = struct {
    prev: ?*rec_t = null,
    next: ?*rec_t = null,
    is_infimum: bool = false,
    is_supremum: bool = false,
    key: i64 = 0,
};

pub const page_t = struct {
    infimum: rec_t = .{ .is_infimum = true },
    supremum: rec_t = .{ .is_supremum = true },
    n_recs: ulint = 0,
};

pub const buf_block_t = struct {
    frame: *page_t,
    page_zip: ?*page_zip_des_t = null,
};

pub const page_cur_t = struct {
    rec: ?*rec_t = null,
    block: ?*buf_block_t = null,
};

pub fn page_init(page: *page_t) void {
    page.infimum.is_infimum = true;
    page.supremum.is_supremum = true;
    page.infimum.prev = null;
    page.infimum.next = &page.supremum;
    page.supremum.prev = &page.infimum;
    page.supremum.next = null;
    page.n_recs = 0;
}

pub fn buf_block_get_frame(block: *const buf_block_t) *page_t {
    return block.frame;
}

pub fn buf_block_get_page_zip(block: *const buf_block_t) ?*page_zip_des_t {
    return block.page_zip;
}

pub fn page_get_infimum_rec(page: *page_t) *rec_t {
    return &page.infimum;
}

pub fn page_get_supremum_rec(page: *page_t) *rec_t {
    return &page.supremum;
}

pub fn page_rec_is_infimum(rec: *const rec_t) bool {
    return rec.is_infimum;
}

pub fn page_rec_is_supremum(rec: *const rec_t) bool {
    return rec.is_supremum;
}

pub fn page_rec_get_next(rec: *rec_t) *rec_t {
    return rec.next orelse rec;
}

pub fn page_rec_get_prev(rec: *rec_t) *rec_t {
    return rec.prev orelse rec;
}

pub fn page_cur_get_page(cur: *page_cur_t) ?*page_t {
    return if (cur.block) |block| block.frame else null;
}

pub fn page_cur_get_block(cur: *page_cur_t) ?*buf_block_t {
    return cur.block;
}

pub fn page_cur_get_page_zip(cur: *page_cur_t) ?*page_zip_des_t {
    const block = cur.block orelse return null;
    return block.page_zip;
}

pub fn page_cur_get_rec(cur: *page_cur_t) ?*rec_t {
    return cur.rec;
}

pub fn page_cur_set_before_first(block: *const buf_block_t, cur: *page_cur_t) void {
    cur.block = @constCast(block);
    cur.rec = page_get_infimum_rec(block.frame);
}

pub fn page_cur_set_after_last(block: *const buf_block_t, cur: *page_cur_t) void {
    cur.block = @constCast(block);
    cur.rec = page_get_supremum_rec(block.frame);
}

pub fn page_cur_is_before_first(cur: *const page_cur_t) ibool {
    const rec = cur.rec orelse return compat.FALSE;
    return if (page_rec_is_infimum(rec)) compat.TRUE else compat.FALSE;
}

pub fn page_cur_is_after_last(cur: *const page_cur_t) ibool {
    const rec = cur.rec orelse return compat.FALSE;
    return if (page_rec_is_supremum(rec)) compat.TRUE else compat.FALSE;
}

pub fn page_cur_position(rec: *const rec_t, block: *const buf_block_t, cur: *page_cur_t) void {
    cur.rec = @constCast(rec);
    cur.block = @constCast(block);
}

pub fn page_cur_invalidate(cur: *page_cur_t) void {
    cur.rec = null;
    cur.block = null;
}

pub fn page_cur_move_to_next(cur: *page_cur_t) void {
    if (cur.rec) |rec| {
        cur.rec = rec.next;
    }
}

pub fn page_cur_move_to_prev(cur: *page_cur_t) void {
    if (cur.rec) |rec| {
        cur.rec = rec.prev;
    }
}

pub fn page_cur_tuple_insert(cursor: *page_cur_t, tuple: *const dtuple_t, index: *dict_index_t, n_ext: ulint, mtr: *mtr_t) ?*rec_t {
    _ = tuple;
    _ = index;
    _ = n_ext;
    _ = mtr;
    _ = cursor;
    return null;
}

pub fn page_cur_rec_insert(cursor: *page_cur_t, rec: *rec_t, index: *dict_index_t, offsets: *ulint, mtr: *mtr_t) ?*rec_t {
    _ = index;
    _ = offsets;
    _ = mtr;
    const current = cursor.rec orelse return null;
    const insert_after = if (current.is_supremum) current.prev orelse current else current;
    const next = insert_after.next;
    rec.prev = insert_after;
    rec.next = next;
    insert_after.next = rec;
    if (next) |nxt| {
        nxt.prev = rec;
    }
    if (cursor.block) |block| {
        block.frame.n_recs += 1;
    }
    return rec;
}

pub fn page_cur_insert_rec_low(current_rec: *rec_t, index: *dict_index_t, rec: *rec_t, offsets: *ulint, mtr: *mtr_t) ?*rec_t {
    _ = index;
    _ = offsets;
    _ = mtr;
    const next = current_rec.next;
    rec.prev = current_rec;
    rec.next = next;
    current_rec.next = rec;
    if (next) |nxt| {
        nxt.prev = rec;
    }
    return rec;
}

pub fn page_cur_insert_rec_zip(current_rec: *?*rec_t, block: *buf_block_t, index: *dict_index_t, rec: *rec_t, offsets: *ulint, mtr: *mtr_t) ?*rec_t {
    _ = block;
    const cur = current_rec.* orelse return null;
    const inserted = page_cur_insert_rec_low(cur, index, rec, offsets, mtr);
    current_rec.* = inserted;
    return inserted;
}

pub fn page_copy_rec_list_end_to_created_page(new_page: *page_t, rec: *rec_t, index: *dict_index_t, mtr: *mtr_t) void {
    _ = index;
    _ = mtr;
    var current: ?*rec_t = rec;
    while (current) |node| {
        if (node.is_supremum) {
            break;
        }
        const copy = std.heap.page_allocator.create(rec_t) catch return;
        copy.* = .{ .key = node.key };
        const sup = &new_page.supremum;
        const prev = sup.prev orelse &new_page.infimum;
        copy.prev = prev;
        copy.next = sup;
        prev.next = copy;
        sup.prev = copy;
        new_page.n_recs += 1;
        current = node.next;
    }
}

pub fn page_cur_delete_rec(cursor: *page_cur_t, index: *dict_index_t, offsets: *const ulint, mtr: *mtr_t) void {
    _ = index;
    _ = offsets;
    _ = mtr;
    const current = cursor.rec orelse return;
    if (current.is_infimum or current.is_supremum) {
        return;
    }
    const next = current.next;
    if (current.prev) |prev| {
        prev.next = next;
    }
    if (next) |nxt| {
        nxt.prev = current.prev;
    }
    if (cursor.block) |block| {
        if (block.frame.n_recs > 0) {
            block.frame.n_recs -= 1;
        }
    }
    cursor.rec = next;
}

pub fn page_cur_search(block: *const buf_block_t, index: *const dict_index_t, tuple: *const dtuple_t, mode: ulint, cursor: *page_cur_t) ulint {
    _ = index;
    _ = tuple;
    _ = mode;
    const page = block.frame;
    const first = page.infimum.next orelse &page.supremum;
    page_cur_position(first, block, cursor);
    return 0;
}

pub fn page_cur_search_with_match(
    block: *const buf_block_t,
    index: *const dict_index_t,
    tuple: *const dtuple_t,
    mode: ulint,
    iup_matched_fields: *ulint,
    iup_matched_bytes: *ulint,
    ilow_matched_fields: *ulint,
    ilow_matched_bytes: *ulint,
    cursor: *page_cur_t,
) void {
    _ = index;
    _ = tuple;
    _ = mode;
    iup_matched_fields.* = 0;
    iup_matched_bytes.* = 0;
    ilow_matched_fields.* = 0;
    ilow_matched_bytes.* = 0;
    const page = block.frame;
    const first = page.infimum.next orelse &page.supremum;
    page_cur_position(first, block, cursor);
}

pub fn page_cur_open_on_rnd_user_rec(block: *buf_block_t, cursor: *page_cur_t) void {
    const page = block.frame;
    const first = page.infimum.next orelse &page.supremum;
    if (page_rec_is_supremum(first)) {
        page_cur_set_before_first(block, cursor);
    } else {
        page_cur_position(first, block, cursor);
    }
}

pub fn page_cur_parse_insert_rec(is_short: ibool, ptr: [*]byte, end_ptr: [*]byte, block: ?*buf_block_t, index: ?*dict_index_t, mtr: ?*mtr_t) ?[*]byte {
    _ = is_short;
    _ = end_ptr;
    _ = block;
    _ = index;
    _ = mtr;
    return ptr;
}

pub fn page_parse_copy_rec_list_to_created_page(ptr: [*]byte, end_ptr: [*]byte, block: ?*buf_block_t, index: ?*dict_index_t, mtr: ?*mtr_t) ?[*]byte {
    _ = end_ptr;
    _ = block;
    _ = index;
    _ = mtr;
    return ptr;
}

pub fn page_cur_parse_delete_rec(ptr: [*]byte, end_ptr: [*]byte, block: ?*buf_block_t, index: ?*dict_index_t, mtr: ?*mtr_t) ?[*]byte {
    _ = end_ptr;
    _ = block;
    _ = index;
    _ = mtr;
    return ptr;
}

test "page cursor movement and insert/delete" {
    var page = page_t{};
    page_init(&page);
    var block = buf_block_t{ .frame = &page };
    var cursor = page_cur_t{};
    var index = dict_index_t{};
    var mtr = mtr_t{};
    var offsets: ulint = 0;

    page_cur_set_before_first(&block, &cursor);
    try std.testing.expectEqual(compat.TRUE, page_cur_is_before_first(&cursor));

    var rec1 = rec_t{ .key = 10 };
    _ = page_cur_rec_insert(&cursor, &rec1, &index, &offsets, &mtr);
    try std.testing.expectEqual(@as(ulint, 1), page.n_recs);

    page_cur_move_to_next(&cursor);
    try std.testing.expect(cursor.rec == &rec1);

    page_cur_delete_rec(&cursor, &index, &offsets, &mtr);
    try std.testing.expectEqual(@as(ulint, 0), page.n_recs);
    try std.testing.expect(page_cur_is_after_last(&cursor) == compat.TRUE);
}

test "page copy rec list end" {
    var page = page_t{};
    page_init(&page);
    var block = buf_block_t{ .frame = &page };
    _ = block;
    var index = dict_index_t{};
    var mtr = mtr_t{};

    const rec1 = try std.heap.page_allocator.create(rec_t);
    const rec2 = try std.heap.page_allocator.create(rec_t);
    rec1.* = .{ .key = 1 };
    rec2.* = .{ .key = 2 };

    const sup = &page.supremum;
    rec1.prev = &page.infimum;
    rec1.next = rec2;
    rec2.prev = rec1;
    rec2.next = sup;
    page.infimum.next = rec1;
    sup.prev = rec2;
    page.n_recs = 2;

    var new_page = page_t{};
    page_init(&new_page);
    page_copy_rec_list_end_to_created_page(&new_page, rec1, &index, &mtr);

    try std.testing.expectEqual(@as(ulint, 2), new_page.n_recs);
    try std.testing.expect(new_page.infimum.next != &new_page.supremum);

    var node = new_page.infimum.next;
    while (node) |rec| {
        if (rec.is_supremum) break;
        const next = rec.next;
        std.heap.page_allocator.destroy(rec);
        node = next;
    }

    std.heap.page_allocator.destroy(rec1);
    std.heap.page_allocator.destroy(rec2);
}

test "page cursor open on rnd user rec" {
    var page = page_t{};
    page_init(&page);
    var block = buf_block_t{ .frame = &page };
    var cursor = page_cur_t{};

    page_cur_open_on_rnd_user_rec(&block, &cursor);
    try std.testing.expectEqual(compat.TRUE, page_cur_is_before_first(&cursor));

    var rec1 = rec_t{ .key = 42 };
    page.infimum.next = &rec1;
    rec1.prev = &page.infimum;
    rec1.next = &page.supremum;
    page.supremum.prev = &rec1;
    page.n_recs = 1;

    page_cur_open_on_rnd_user_rec(&block, &cursor);
    try std.testing.expect(cursor.rec == &rec1);
}
