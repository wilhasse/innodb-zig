const std = @import("std");
const compat = @import("../ut/compat.zig");

pub const module_name = "btr";

pub const ulint = compat.ulint;
pub const byte = compat.byte;
pub const ibool = compat.ibool;
pub const dulint = compat.Dulint;

pub const BTR_PAGE_MAX_REC_SIZE: ulint = compat.UNIV_PAGE_SIZE / 2 - 200;
pub const BTR_MAX_LEVELS: ulint = 100;

const RW_NO_LATCH: u32 = 0;
const RW_S_LATCH: u32 = 1;
const RW_X_LATCH: u32 = 2;

pub const btr_latch_mode = enum(u32) {
    BTR_SEARCH_LEAF = RW_S_LATCH,
    BTR_MODIFY_LEAF = RW_X_LATCH,
    BTR_NO_LATCHES = RW_NO_LATCH,
    BTR_MODIFY_TREE = 33,
    BTR_CONT_MODIFY_TREE = 34,
    BTR_SEARCH_PREV = 35,
    BTR_MODIFY_PREV = 36,
};

pub const BTR_INSERT: ulint = 512;
pub const BTR_ESTIMATE: ulint = 1024;
pub const BTR_IGNORE_SEC_UNIQUE: ulint = 2048;

pub const dict_index_t = struct {};
pub const mtr_t = struct {};
pub const buf_block_t = struct {};
pub const page_t = struct {};
pub const btr_cur_t = struct {};
pub const dtuple_t = struct {};
pub const trx_t = struct {};

pub const rec_t = struct {
    prev: ?*rec_t = null,
    next: ?*rec_t = null,
    is_infimum: bool = false,
    is_supremum: bool = false,
};

pub fn btr_root_get(index: *dict_index_t, mtr: *mtr_t) ?*page_t {
    _ = index;
    _ = mtr;
    return null;
}

pub fn btr_get_prev_user_rec(rec: ?*rec_t, mtr: ?*mtr_t) ?*rec_t {
    _ = mtr;
    const current = rec orelse return null;
    if (current.is_infimum) {
        return null;
    }
    const prev = current.prev orelse return null;
    if (prev.is_infimum) {
        return null;
    }
    return prev;
}

pub fn btr_get_next_user_rec(rec: ?*rec_t, mtr: ?*mtr_t) ?*rec_t {
    _ = mtr;
    const current = rec orelse return null;
    if (current.is_supremum) {
        return null;
    }
    const next = current.next orelse return null;
    if (next.is_supremum) {
        return null;
    }
    return next;
}

pub fn btr_page_alloc(index: *dict_index_t, hint_page_no: ulint, file_direction: byte, level: ulint, mtr: *mtr_t) ?*buf_block_t {
    _ = index;
    _ = hint_page_no;
    _ = file_direction;
    _ = level;
    _ = mtr;
    return null;
}

pub fn btr_get_size(index: *dict_index_t, flag: ulint) ulint {
    _ = index;
    _ = flag;
    return 0;
}

pub fn btr_page_free_low(index: *dict_index_t, block: *buf_block_t, level: ulint, mtr: *mtr_t) void {
    _ = index;
    _ = block;
    _ = level;
    _ = mtr;
}

pub fn btr_page_free(index: *dict_index_t, block: *buf_block_t, mtr: *mtr_t) void {
    _ = index;
    _ = block;
    _ = mtr;
}

pub fn btr_create(type_: ulint, space: ulint, zip_size: ulint, index_id: dulint, index: *dict_index_t, mtr: *mtr_t) ulint {
    _ = type_;
    _ = space;
    _ = zip_size;
    _ = index_id;
    _ = index;
    _ = mtr;
    return 0;
}

pub fn btr_free_but_not_root(space: ulint, zip_size: ulint, root_page_no: ulint) void {
    _ = space;
    _ = zip_size;
    _ = root_page_no;
}

pub fn btr_free_root(space: ulint, zip_size: ulint, root_page_no: ulint, mtr: *mtr_t) void {
    _ = space;
    _ = zip_size;
    _ = root_page_no;
    _ = mtr;
}

pub fn btr_page_reorganize(block: *buf_block_t, index: *dict_index_t, mtr: *mtr_t) ibool {
    _ = block;
    _ = index;
    _ = mtr;
    return compat.FALSE;
}

pub fn btr_parse_page_reorganize(ptr: [*]byte, end_ptr: [*]byte, index: ?*dict_index_t, block: ?*buf_block_t, mtr: ?*mtr_t) [*]byte {
    _ = end_ptr;
    _ = index;
    _ = block;
    _ = mtr;
    return ptr;
}

pub fn btr_root_raise_and_insert(cursor: *btr_cur_t, tuple: *const dtuple_t, n_ext: ulint, mtr: *mtr_t) ?*rec_t {
    _ = cursor;
    _ = tuple;
    _ = n_ext;
    _ = mtr;
    return null;
}

pub fn btr_page_get_split_rec_to_left(cursor: *btr_cur_t, split_rec: *?*rec_t) ibool {
    _ = cursor;
    split_rec.* = null;
    return compat.FALSE;
}

pub fn btr_page_get_split_rec_to_right(cursor: *btr_cur_t, split_rec: *?*rec_t) ibool {
    _ = cursor;
    split_rec.* = null;
    return compat.FALSE;
}

pub fn btr_insert_on_non_leaf_level_func(index: *dict_index_t, level: ulint, tuple: *dtuple_t, file: []const u8, line: ulint, mtr: *mtr_t) void {
    _ = index;
    _ = level;
    _ = tuple;
    _ = file;
    _ = line;
    _ = mtr;
}

pub fn btr_page_split_and_insert(cursor: *btr_cur_t, tuple: *const dtuple_t, n_ext: ulint, mtr: *mtr_t) ?*rec_t {
    _ = cursor;
    _ = tuple;
    _ = n_ext;
    _ = mtr;
    return null;
}

pub fn btr_parse_set_min_rec_mark(ptr: [*]byte, end_ptr: [*]byte, comp: ulint, page: ?*page_t, mtr: ?*mtr_t) [*]byte {
    _ = end_ptr;
    _ = comp;
    _ = page;
    _ = mtr;
    return ptr;
}

pub fn btr_set_min_rec_mark(rec: *rec_t, mtr: *mtr_t) void {
    _ = rec;
    _ = mtr;
}

pub fn btr_node_ptr_delete(index: *dict_index_t, block: *buf_block_t, mtr: *mtr_t) void {
    _ = index;
    _ = block;
    _ = mtr;
}

pub fn btr_compress(cursor: *btr_cur_t, mtr: *mtr_t) ibool {
    _ = cursor;
    _ = mtr;
    return compat.FALSE;
}

pub fn btr_discard_page(cursor: *btr_cur_t, mtr: *mtr_t) void {
    _ = cursor;
    _ = mtr;
}

pub fn btr_print_size(index: *dict_index_t) void {
    _ = index;
}

pub fn btr_print_index(index: *dict_index_t, width: ulint) void {
    _ = index;
    _ = width;
}

pub fn btr_check_node_ptr(index: *dict_index_t, block: *buf_block_t, mtr: *mtr_t) ibool {
    _ = index;
    _ = block;
    _ = mtr;
    return compat.TRUE;
}

pub fn btr_index_rec_validate(rec: *const rec_t, index: *const dict_index_t, dump_on_error: ibool) ibool {
    _ = rec;
    _ = index;
    _ = dump_on_error;
    return compat.TRUE;
}

pub fn btr_validate_index(index: *dict_index_t, trx: ?*trx_t) ibool {
    _ = index;
    _ = trx;
    return compat.TRUE;
}

test "btr prev and next user record stubs" {
    var a = rec_t{};
    var b = rec_t{};
    var c = rec_t{};

    a.next = &b;
    b.prev = &a;
    b.next = &c;
    c.prev = &b;

    try std.testing.expect(btr_get_prev_user_rec(&b, null) == &a);
    try std.testing.expect(btr_get_next_user_rec(&b, null) == &c);

    var inf = rec_t{ .is_infimum = true };
    try std.testing.expect(btr_get_prev_user_rec(&inf, null) == null);

    var sup = rec_t{ .is_supremum = true };
    try std.testing.expect(btr_get_next_user_rec(&sup, null) == null);
}

test "btr split rec helpers default" {
    var cursor = btr_cur_t{};
    var rec = rec_t{};
    var split: ?*rec_t = &rec;

    try std.testing.expect(btr_page_get_split_rec_to_left(&cursor, &split) == compat.FALSE);
    try std.testing.expect(split == null);

    split = &rec;
    try std.testing.expect(btr_page_get_split_rec_to_right(&cursor, &split) == compat.FALSE);
    try std.testing.expect(split == null);
}
