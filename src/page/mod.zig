const std = @import("std");
const compat = @import("../ut/compat.zig");
const data = @import("../data/mod.zig");
const dict = @import("../dict/mod.zig");
const fil = @import("../fil/mod.zig");
const fsp = @import("../fsp/mod.zig");
const mach = @import("../mach/mod.zig");

pub const module_name = "page";

pub const ulint = compat.ulint;
pub const lint = compat.lint;
pub const ibool = compat.ibool;
pub const byte = compat.byte;

pub const PAGE_CUR_UNSUPP: ulint = 0;
pub const PAGE_CUR_G: ulint = 1;
pub const PAGE_CUR_GE: ulint = 2;
pub const PAGE_CUR_L: ulint = 3;
pub const PAGE_CUR_LE: ulint = 4;

pub const PAGE_HEADER: ulint = fsp.FSEG_PAGE_DATA;
pub const PAGE_DIR: ulint = fil.FIL_PAGE_DATA_END;
pub const PAGE_DIR_SLOT_SIZE: ulint = 2;
pub const PAGE_EMPTY_DIR_START: ulint = PAGE_DIR + 2 * PAGE_DIR_SLOT_SIZE;

pub const PAGE_N_DIR_SLOTS: ulint = 0;
pub const PAGE_HEAP_TOP: ulint = 2;
pub const PAGE_N_HEAP: ulint = 4;
pub const PAGE_FREE: ulint = 6;
pub const PAGE_GARBAGE: ulint = 8;
pub const PAGE_LAST_INSERT: ulint = 10;
pub const PAGE_DIRECTION: ulint = 12;
pub const PAGE_N_DIRECTION: ulint = 14;
pub const PAGE_N_RECS: ulint = 16;
pub const PAGE_MAX_TRX_ID: ulint = 18;
pub const PAGE_LEVEL: ulint = 26;
pub const PAGE_INDEX_ID: ulint = 28;

pub const PAGE_HEAP_NO_INFIMUM: ulint = 0;
pub const PAGE_HEAP_NO_SUPREMUM: ulint = 1;
pub const PAGE_HEAP_NO_USER_LOW: ulint = 2;

pub const PAGE_LEFT: ulint = 1;
pub const PAGE_RIGHT: ulint = 2;
pub const PAGE_SAME_REC: ulint = 3;
pub const PAGE_SAME_PAGE: ulint = 4;
pub const PAGE_NO_DIRECTION: ulint = 5;

pub const trx_id_t = compat.ib_uint64_t;

pub const PAGE_ZIP_MIN_SIZE_SHIFT: ulint = 10;
pub const PAGE_ZIP_MIN_SIZE: ulint = 1 << PAGE_ZIP_MIN_SIZE_SHIFT;
pub const PAGE_ZIP_NUM_SSIZE: ulint = @as(ulint, compat.UNIV_PAGE_SIZE_SHIFT) - PAGE_ZIP_MIN_SIZE_SHIFT + 2;

pub const page_zip_des_t = struct {
    data: ?[]u8 = null,
    m_end: u16 = 0,
    m_nonempty: bool = false,
    n_blobs: u16 = 0,
    ssize: u8 = 0,
};
pub const dict_index_t = dict.dict_index_t;
pub const dtuple_t = data.dtuple_t;
pub const mtr_t = struct {};

pub const extern_field_t = struct {
    field_no: ulint = 0,
    data: []u8 = &[_]u8{},
};

pub const rec_t = struct {
    prev: ?*rec_t = null,
    next: ?*rec_t = null,
    page: ?*page_t = null,
    is_infimum: bool = false,
    is_supremum: bool = false,
    deleted: bool = false,
    min_rec_mark: bool = false,
    key: i64 = 0,
    payload: ?*anyopaque = null,
    rec_bytes: ?[]u8 = null,
    rec_offset: ulint = 0,
    child_page_no: ulint = 0,
    child_block: ?*buf_block_t = null,
    extern_fields: []extern_field_t = &[_]extern_field_t{},
};

pub const PageHeader = struct {
    n_dir_slots: ulint = 0,
    heap_top: ulint = 0,
    n_heap: ulint = 0,
    free: ulint = 0,
    garbage: ulint = 0,
    last_insert: ulint = 0,
    direction: ulint = PAGE_NO_DIRECTION,
    n_direction: ulint = 0,
    n_recs: ulint = 0,
    max_trx_id: trx_id_t = 0,
    level: ulint = 0,
    index_id: compat.Dulint = .{ .high = 0, .low = 0 },
};

pub fn page_header_get_field_bytes(page: [*]const byte, field: ulint) ulint {
    std.debug.assert(field <= PAGE_INDEX_ID);
    return mach.mach_read_from_2(page + PAGE_HEADER + field);
}

pub fn page_header_set_field_bytes(page: [*]byte, field: ulint, val: ulint) void {
    std.debug.assert(field <= PAGE_N_RECS);
    mach.mach_write_to_2(page + PAGE_HEADER + field, val);
}

pub fn page_header_get_offs_bytes(page: [*]const byte, field: ulint) ulint {
    std.debug.assert(field == PAGE_FREE or field == PAGE_LAST_INSERT or field == PAGE_HEAP_TOP);
    return page_header_get_field_bytes(page, field);
}

pub fn page_header_set_offs_bytes(page: [*]byte, field: ulint, val: ulint) void {
    std.debug.assert(field == PAGE_FREE or field == PAGE_LAST_INSERT or field == PAGE_HEAP_TOP);
    page_header_set_field_bytes(page, field, val);
}

pub fn page_get_max_trx_id_bytes(page: [*]const byte) trx_id_t {
    const d = mach.mach_read_from_8(page + PAGE_HEADER + PAGE_MAX_TRX_ID);
    return (@as(trx_id_t, @intCast(d.high)) << 32) | d.low;
}

pub fn page_set_max_trx_id_bytes(page: [*]byte, trx_id: trx_id_t) void {
    const d = compat.Dulint{
        .high = @as(ulint, @intCast(trx_id >> 32)),
        .low = @as(ulint, @intCast(trx_id & 0xFFFF_FFFF)),
    };
    mach.mach_write_to_8(page + PAGE_HEADER + PAGE_MAX_TRX_ID, d);
}

pub fn page_get_index_id_bytes(page: [*]const byte) compat.Dulint {
    return mach.mach_read_from_8(page + PAGE_HEADER + PAGE_INDEX_ID);
}

pub fn page_set_index_id_bytes(page: [*]byte, id: compat.Dulint) void {
    mach.mach_write_to_8(page + PAGE_HEADER + PAGE_INDEX_ID, id);
}

pub fn page_dir_get_nth_slot(page: [*]const byte, n: ulint) [*]const byte {
    const offs = compat.UNIV_PAGE_SIZE - PAGE_DIR - (n + 1) * PAGE_DIR_SLOT_SIZE;
    return page + @as(usize, @intCast(offs));
}

pub fn page_dir_get_nth_slot_val(page: [*]const byte, n: ulint) ulint {
    return mach.mach_read_from_2(page_dir_get_nth_slot(page, n));
}

pub fn page_dir_set_nth_slot(page: [*]byte, n: ulint, val: ulint) void {
    const slot = page_dir_get_nth_slot(@as([*]const byte, page), n);
    mach.mach_write_to_2(@constCast(slot), val);
}

pub fn page_free_get_bytes(page: [*]const byte) ulint {
    return page_header_get_field_bytes(page, PAGE_FREE);
}

pub fn page_free_set_bytes(page: [*]byte, offs: ulint) void {
    page_header_set_field_bytes(page, PAGE_FREE, offs);
}

pub fn page_garbage_get_bytes(page: [*]const byte) ulint {
    return page_header_get_field_bytes(page, PAGE_GARBAGE);
}

pub fn page_garbage_set_bytes(page: [*]byte, val: ulint) void {
    page_header_set_field_bytes(page, PAGE_GARBAGE, val);
}

pub fn page_garbage_add_bytes(page: [*]byte, delta: ulint) void {
    page_garbage_set_bytes(page, page_garbage_get_bytes(page) + delta);
}

pub fn page_free_push_bytes(page: [*]byte, rec_offs: ulint, rec_size: ulint) void {
    page_free_set_bytes(page, rec_offs);
    page_garbage_add_bytes(page, rec_size);
}

pub const page_t = struct {
    header: PageHeader = .{},
    infimum: rec_t = .{ .is_infimum = true },
    supremum: rec_t = .{ .is_supremum = true },
    page_no: ulint = 0,
    parent_block: ?*buf_block_t = null,
    prev_block: ?*buf_block_t = null,
    next_block: ?*buf_block_t = null,
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
    const page_no = page.page_no;
    const parent = page.parent_block;
    const prev = page.prev_block;
    const next = page.next_block;
    page.header = .{};
    page.header.n_heap = PAGE_HEAP_NO_USER_LOW;
    page.infimum = .{ .is_infimum = true };
    page.supremum = .{ .is_supremum = true };
    page.infimum.page = page;
    page.supremum.page = page;
    page.infimum.prev = null;
    page.infimum.next = &page.supremum;
    page.supremum.prev = &page.infimum;
    page.supremum.next = null;
    page.header.n_recs = 0;
    page.page_no = page_no;
    page.parent_block = parent;
    page.prev_block = prev;
    page.next_block = next;
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

pub fn page_align(ptr: *const anyopaque) *page_t {
    return @ptrCast(@alignCast(ptr));
}

pub fn page_offset(ptr: *const anyopaque) ulint {
    _ = ptr;
    return 0;
}

pub fn page_get_max_trx_id(page: *const page_t) trx_id_t {
    return page.header.max_trx_id;
}

pub fn page_set_max_trx_id(block: *const buf_block_t, page_zip: ?*page_zip_des_t, trx_id: trx_id_t, mtr: *mtr_t) void {
    _ = page_zip;
    _ = mtr;
    block.frame.header.max_trx_id = trx_id;
}

pub fn page_update_max_trx_id(block: *const buf_block_t, page_zip: ?*page_zip_des_t, trx_id: trx_id_t, mtr: *mtr_t) void {
    _ = page_zip;
    _ = mtr;
    if (trx_id > block.frame.header.max_trx_id) {
        block.frame.header.max_trx_id = trx_id;
    }
}

pub fn page_header_get_field(page: *const page_t, field: ulint) ulint {
    return switch (field) {
        PAGE_N_DIR_SLOTS => page.header.n_dir_slots,
        PAGE_HEAP_TOP => page.header.heap_top,
        PAGE_N_HEAP => page.header.n_heap,
        PAGE_FREE => page.header.free,
        PAGE_GARBAGE => page.header.garbage,
        PAGE_LAST_INSERT => page.header.last_insert,
        PAGE_DIRECTION => page.header.direction,
        PAGE_N_DIRECTION => page.header.n_direction,
        PAGE_N_RECS => page.header.n_recs,
        PAGE_LEVEL => page.header.level,
        PAGE_INDEX_ID => page.header.index_id.low,
        PAGE_MAX_TRX_ID => @as(ulint, @intCast(page.header.max_trx_id & 0xFFFF_FFFF)),
        else => 0,
    };
}

pub fn page_header_set_field(page: *page_t, page_zip: ?*page_zip_des_t, field: ulint, val: ulint) void {
    _ = page_zip;
    switch (field) {
        PAGE_N_DIR_SLOTS => page.header.n_dir_slots = val,
        PAGE_HEAP_TOP => page.header.heap_top = val,
        PAGE_N_HEAP => page.header.n_heap = val,
        PAGE_FREE => page.header.free = val,
        PAGE_GARBAGE => page.header.garbage = val,
        PAGE_LAST_INSERT => page.header.last_insert = val,
        PAGE_DIRECTION => page.header.direction = val,
        PAGE_N_DIRECTION => page.header.n_direction = val,
        PAGE_N_RECS => page.header.n_recs = val,
        PAGE_LEVEL => page.header.level = val,
        PAGE_INDEX_ID => page.header.index_id.low = val,
        PAGE_MAX_TRX_ID => page.header.max_trx_id = @as(trx_id_t, @intCast(val)),
        else => {},
    }
}

pub fn page_header_get_offs(page: *const page_t, field: ulint) ulint {
    return page_header_get_field(page, field);
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
        block.frame.header.n_recs += 1;
        rec.page = block.frame;
    } else {
        rec.page = insert_after.page;
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
    rec.page = current_rec.page;
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
        copy.* = .{
            .key = node.key,
            .payload = node.payload,
            .rec_bytes = node.rec_bytes,
            .rec_offset = node.rec_offset,
            .deleted = node.deleted,
            .min_rec_mark = node.min_rec_mark,
            .extern_fields = node.extern_fields,
            .child_block = node.child_block,
            .child_page_no = node.child_page_no,
        };
        copy.page = new_page;
        const sup = &new_page.supremum;
        const prev = sup.prev orelse &new_page.infimum;
        copy.prev = prev;
        copy.next = sup;
        prev.next = copy;
        sup.prev = copy;
        new_page.header.n_recs += 1;
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
        if (block.frame.header.n_recs > 0) {
            block.frame.header.n_recs -= 1;
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

pub fn page_cur_open_on_rnd_user_rec(block: *const buf_block_t, cursor: *page_cur_t) void {
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

pub fn page_mem_alloc_heap(page: *page_t, page_zip: ?*page_zip_des_t, need: ulint, heap_no: *ulint) ?[*]byte {
    _ = page_zip;
    if (need == 0) {
        heap_no.* = 0;
        return null;
    }
    const buf = std.heap.page_allocator.alloc(byte, need) catch return null;
    heap_no.* = page.header.n_heap;
    page.header.n_heap += 1;
    return buf.ptr;
}

pub fn page_mem_free(page: *page_t, page_zip: ?*page_zip_des_t, rec: *rec_t, index: *dict_index_t, offsets: *const ulint) void {
    _ = page;
    _ = page_zip;
    _ = rec;
    _ = index;
    _ = offsets;
}

pub fn page_create(block: *const buf_block_t, mtr: *mtr_t, comp: ulint) *page_t {
    _ = mtr;
    _ = comp;
    page_init(block.frame);
    return block.frame;
}

pub fn page_create_zip(block: *const buf_block_t, index: *dict_index_t, level: ulint, mtr: *mtr_t) *page_t {
    _ = index;
    _ = mtr;
    page_init(block.frame);
    block.frame.header.level = level;
    return block.frame;
}

pub fn page_copy_rec_list_end_no_locks(new_block: *buf_block_t, block: *buf_block_t, rec: *rec_t, index: *dict_index_t, mtr: *mtr_t) void {
    _ = block;
    page_copy_rec_list_end_to_created_page(new_block.frame, rec, index, mtr);
}

pub fn page_copy_rec_list_end(new_block: *buf_block_t, block: *buf_block_t, rec: *rec_t, index: *dict_index_t, mtr: *mtr_t) ?*rec_t {
    page_copy_rec_list_end_no_locks(new_block, block, rec, index, mtr);
    return new_block.frame.infimum.next;
}

pub fn page_copy_rec_list_start(new_block: *buf_block_t, block: *buf_block_t, rec: *rec_t, index: *dict_index_t, mtr: *mtr_t) ?*rec_t {
    _ = index;
    _ = mtr;
    var current = block.frame.infimum.next;
    while (current) |node| {
        if (node == rec or node.is_supremum) {
            break;
        }
        const copy = std.heap.page_allocator.create(rec_t) catch break;
        copy.* = .{
            .key = node.key,
            .payload = node.payload,
            .rec_bytes = node.rec_bytes,
            .rec_offset = node.rec_offset,
            .deleted = node.deleted,
            .min_rec_mark = node.min_rec_mark,
            .extern_fields = node.extern_fields,
            .child_block = node.child_block,
            .child_page_no = node.child_page_no,
        };
        copy.page = new_block.frame;
        const sup = &new_block.frame.supremum;
        const prev = sup.prev orelse &new_block.frame.infimum;
        copy.prev = prev;
        copy.next = sup;
        prev.next = copy;
        sup.prev = copy;
        new_block.frame.header.n_recs += 1;
        current = node.next;
    }
    return new_block.frame.supremum.prev;
}

pub fn page_get_middle_rec(page: *page_t) ?*rec_t {
    if (page.header.n_recs == 0) {
        return null;
    }
    const target = page.header.n_recs / 2;
    var idx: ulint = 0;
    var current = page.infimum.next;
    while (current) |node| {
        if (node.is_supremum) {
            return null;
        }
        if (idx == target) {
            return node;
        }
        idx += 1;
        current = node.next;
    }
    return null;
}

pub fn page_rec_get_n_recs_before(page: *page_t, rec: *rec_t) ulint {
    var count: ulint = 0;
    var current = page.infimum.next;
    while (current) |node| {
        if (node == rec or node.is_supremum) {
            break;
        }
        count += 1;
        current = node.next;
    }
    return count;
}

pub fn page_rec_validate(rec: *rec_t, offsets: *const ulint) ibool {
    _ = offsets;
    if (rec.is_infimum or rec.is_supremum) {
        return compat.TRUE;
    }
    if (rec.prev == null or rec.next == null) {
        return compat.FALSE;
    }
    return compat.TRUE;
}

pub fn page_check_dir(page: *const page_t) void {
    _ = page;
}

pub fn page_simple_validate_old(page: *page_t) ibool {
    _ = page;
    return compat.TRUE;
}

pub fn page_simple_validate_new(page: *page_t) ibool {
    _ = page;
    return compat.TRUE;
}

pub fn page_validate(page: *page_t, index: *dict_index_t) ibool {
    _ = page;
    _ = index;
    return compat.TRUE;
}

pub fn page_find_rec_with_heap_no(page: *page_t, heap_no: ulint) ?*rec_t {
    var count: ulint = 0;
    var current = page.infimum.next;
    while (current) |node| {
        if (node.is_supremum) {
            break;
        }
        if (count == heap_no) {
            return node;
        }
        count += 1;
        current = node.next;
    }
    return null;
}

pub fn page_zip_get_size(page_zip: *const page_zip_des_t) ulint {
    if (page_zip.ssize == 0) {
        return 0;
    }
    const shift = @as(u6, @intCast(page_zip.ssize - 1));
    return PAGE_ZIP_MIN_SIZE << shift;
}

pub fn page_zip_set_size(page_zip: *page_zip_des_t, size: ulint) void {
    if (size == 0) {
        page_zip.ssize = 0;
        return;
    }
    var shift: u8 = 1;
    var current = PAGE_ZIP_MIN_SIZE;
    while (current < size and shift < 255) {
        current <<= 1;
        shift += 1;
    }
    page_zip.ssize = shift;
}

pub fn page_zip_rec_needs_ext(rec_size: ulint, comp: ulint, n_fields: ulint, zip_size: ulint) ibool {
    _ = comp;
    _ = n_fields;
    if (zip_size == 0) {
        return compat.FALSE;
    }
    return if (rec_size > zip_size / 2) compat.TRUE else compat.FALSE;
}

pub fn page_zip_empty_size(n_fields: ulint, zip_size: ulint) ulint {
    _ = n_fields;
    if (zip_size == 0) {
        return compat.UNIV_PAGE_SIZE;
    }
    if (zip_size > 64) {
        return zip_size - 64;
    }
    return zip_size;
}

pub fn page_zip_des_init(page_zip: *page_zip_des_t) void {
    page_zip.* = .{};
}

pub fn page_zip_set_alloc(stream: ?*anyopaque, heap: ?*anyopaque) void {
    _ = stream;
    _ = heap;
}

pub fn page_zip_compress(page_zip: *page_zip_des_t, page: *const page_t, index: *dict_index_t, mtr: *mtr_t) ibool {
    _ = page;
    _ = index;
    _ = mtr;
    const size = page_zip_get_size(page_zip);
    if (size == 0) {
        return compat.FALSE;
    }
    if (page_zip.data == null or page_zip.data.?.len != size) {
        if (page_zip.data) |buf| {
            std.heap.page_allocator.free(buf);
        }
        const buf = std.heap.page_allocator.alloc(u8, size) catch return compat.FALSE;
        page_zip.data = buf;
    }
    if (page_zip.data) |buf| {
        @memset(buf, 0);
    }
    page_zip.m_end = 0;
    page_zip.m_nonempty = false;
    page_zip.n_blobs = 0;
    return compat.TRUE;
}

pub fn page_zip_decompress(page_zip: *page_zip_des_t, page: *page_t, all: ibool) ibool {
    _ = page;
    _ = all;
    if (page_zip.data == null) {
        return compat.FALSE;
    }
    page_zip.m_nonempty = false;
    return compat.TRUE;
}

pub fn page_zip_simple_validate(page_zip: *const page_zip_des_t) ibool {
    if (page_zip.ssize == 0) {
        return compat.TRUE;
    }
    const size = page_zip_get_size(page_zip);
    return if (size <= compat.UNIV_PAGE_SIZE) compat.TRUE else compat.FALSE;
}

pub fn page_zip_max_ins_size(page_zip: *const page_zip_des_t, is_clust: ibool) lint {
    _ = is_clust;
    const size = page_zip_get_size(page_zip);
    return @as(lint, @intCast(size / 2));
}

pub fn page_zip_available(page_zip: *const page_zip_des_t, is_clust: ibool, length: ulint, create: ulint) ibool {
    _ = create;
    const max_size = page_zip_max_ins_size(page_zip, is_clust);
    return if (length <= @as(ulint, @intCast(max_size))) compat.TRUE else compat.FALSE;
}

test "page cursor movement and insert/delete" {
    var page = page_t{};
    page_init(&page);
    const block = buf_block_t{ .frame = &page };
    var cursor = page_cur_t{};
    var index = dict_index_t{};
    var mtr = mtr_t{};
    var offsets: ulint = 0;

    page_cur_set_before_first(&block, &cursor);
    try std.testing.expectEqual(compat.TRUE, page_cur_is_before_first(&cursor));

    var rec1 = rec_t{ .key = 10 };
    _ = page_cur_rec_insert(&cursor, &rec1, &index, &offsets, &mtr);
    try std.testing.expectEqual(@as(ulint, 1), page.header.n_recs);

    page_cur_move_to_next(&cursor);
    try std.testing.expect(cursor.rec == &rec1);

    page_cur_delete_rec(&cursor, &index, &offsets, &mtr);
    try std.testing.expectEqual(@as(ulint, 0), page.header.n_recs);
    try std.testing.expect(page_cur_is_after_last(&cursor) == compat.TRUE);
}

test "page copy rec list end" {
    var page = page_t{};
    page_init(&page);
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
    page.header.n_recs = 2;

    var new_page = page_t{};
    page_init(&new_page);
    page_copy_rec_list_end_to_created_page(&new_page, rec1, &index, &mtr);

    try std.testing.expectEqual(@as(ulint, 2), new_page.header.n_recs);
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
    const block = buf_block_t{ .frame = &page };
    var cursor = page_cur_t{};

    page_cur_open_on_rnd_user_rec(&block, &cursor);
    try std.testing.expectEqual(compat.TRUE, page_cur_is_before_first(&cursor));

    var rec1 = rec_t{ .key = 42 };
    page.infimum.next = &rec1;
    rec1.prev = &page.infimum;
    rec1.next = &page.supremum;
    page.supremum.prev = &rec1;
    page.header.n_recs = 1;

    page_cur_open_on_rnd_user_rec(&block, &cursor);
    try std.testing.expect(cursor.rec == &rec1);
}

test "page header fields and create" {
    var page = page_t{};
    page_init(&page);
    page_header_set_field(&page, null, PAGE_N_RECS, 7);
    page_header_set_field(&page, null, PAGE_DIRECTION, PAGE_RIGHT);
    try std.testing.expectEqual(@as(ulint, 7), page_header_get_field(&page, PAGE_N_RECS));
    try std.testing.expectEqual(@as(ulint, PAGE_RIGHT), page_header_get_field(&page, PAGE_DIRECTION));

    const block = buf_block_t{ .frame = &page };
    var mtr = mtr_t{};
    page_set_max_trx_id(&block, null, 123, &mtr);
    try std.testing.expectEqual(@as(trx_id_t, 123), page_get_max_trx_id(&page));
    page_update_max_trx_id(&block, null, 100, &mtr);
    try std.testing.expectEqual(@as(trx_id_t, 123), page_get_max_trx_id(&page));
    page_update_max_trx_id(&block, null, 200, &mtr);
    try std.testing.expectEqual(@as(trx_id_t, 200), page_get_max_trx_id(&page));

    _ = page_create(&block, &mtr, 0);
    try std.testing.expectEqual(@as(ulint, 0), page.header.n_recs);
    try std.testing.expect(page.infimum.next == &page.supremum);
}

test "page middle rec and count" {
    var page = page_t{};
    page_init(&page);

    var rec1 = rec_t{ .key = 1 };
    var rec2 = rec_t{ .key = 2 };
    var rec3 = rec_t{ .key = 3 };

    page.infimum.next = &rec1;
    rec1.prev = &page.infimum;
    rec1.next = &rec2;
    rec2.prev = &rec1;
    rec2.next = &rec3;
    rec3.prev = &rec2;
    rec3.next = &page.supremum;
    page.supremum.prev = &rec3;
    page.header.n_recs = 3;

    const middle = page_get_middle_rec(&page) orelse return error.TestExpectedEqual;
    try std.testing.expect(middle == &rec2);
    try std.testing.expectEqual(@as(ulint, 2), page_rec_get_n_recs_before(&page, &rec3));
}

test "page zip basic" {
    var zip = page_zip_des_t{};
    page_zip_des_init(&zip);
    page_zip_set_size(&zip, PAGE_ZIP_MIN_SIZE);
    try std.testing.expectEqual(PAGE_ZIP_MIN_SIZE, page_zip_get_size(&zip));
    try std.testing.expectEqual(compat.TRUE, page_zip_simple_validate(&zip));

    var page = page_t{};
    page_init(&page);
    var index = dict_index_t{};
    var mtr = mtr_t{};
    try std.testing.expectEqual(compat.TRUE, page_zip_compress(&zip, &page, &index, &mtr));
    try std.testing.expectEqual(compat.TRUE, page_zip_decompress(&zip, &page, compat.TRUE));
    try std.testing.expectEqual(compat.TRUE, page_zip_available(&zip, compat.TRUE, 100, 0));

    if (zip.data) |buf| {
        std.heap.page_allocator.free(buf);
    }
}

test "page header byte helpers" {
    var buf = [_]byte{0} ** 256;
    const page_bytes = buf[0..].ptr;

    page_header_set_field_bytes(page_bytes, PAGE_N_RECS, 7);
    try std.testing.expectEqual(@as(ulint, 7), page_header_get_field_bytes(page_bytes, PAGE_N_RECS));

    page_header_set_offs_bytes(page_bytes, PAGE_HEAP_TOP, 1234);
    try std.testing.expectEqual(@as(ulint, 1234), page_header_get_offs_bytes(page_bytes, PAGE_HEAP_TOP));

    page_set_max_trx_id_bytes(page_bytes, 0x0102030405060708);
    try std.testing.expectEqual(@as(trx_id_t, 0x0102030405060708), page_get_max_trx_id_bytes(page_bytes));

    const id = compat.Dulint{ .high = 0x12345678, .low = 0x9abcdef0 };
    page_set_index_id_bytes(page_bytes, id);
    const read_id = page_get_index_id_bytes(page_bytes);
    try std.testing.expectEqual(id.high, read_id.high);
    try std.testing.expectEqual(id.low, read_id.low);
}

test "page dir slot byte helpers" {
    var buf = [_]byte{0} ** compat.UNIV_PAGE_SIZE;
    const page_bytes = buf[0..].ptr;

    page_dir_set_nth_slot(page_bytes, 0, 123);
    try std.testing.expectEqual(@as(ulint, 123), page_dir_get_nth_slot_val(page_bytes, 0));

    page_dir_set_nth_slot(page_bytes, 3, 456);
    try std.testing.expectEqual(@as(ulint, 456), page_dir_get_nth_slot_val(page_bytes, 3));
}

test "page free list and garbage byte helpers" {
    var buf = [_]byte{0} ** compat.UNIV_PAGE_SIZE;
    const page_bytes = buf[0..].ptr;

    page_free_set_bytes(page_bytes, 120);
    try std.testing.expectEqual(@as(ulint, 120), page_free_get_bytes(page_bytes));

    page_garbage_set_bytes(page_bytes, 0);
    page_free_push_bytes(page_bytes, 200, 32);
    try std.testing.expectEqual(@as(ulint, 200), page_free_get_bytes(page_bytes));
    try std.testing.expectEqual(@as(ulint, 32), page_garbage_get_bytes(page_bytes));

    page_garbage_add_bytes(page_bytes, 16);
    try std.testing.expectEqual(@as(ulint, 48), page_garbage_get_bytes(page_bytes));
}
