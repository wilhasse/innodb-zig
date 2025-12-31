const std = @import("std");
const compat = @import("../ut/compat.zig");
const data = @import("../data/mod.zig");
const dict = @import("../dict/mod.zig");
const fil = @import("../fil/mod.zig");
const fsp = @import("../fsp/mod.zig");
const rec_mod = @import("../rec/mod.zig");
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

pub fn page_bytes_insert_append(page: []u8, rec_bytes: []const u8) ?ulint {
    const heap_top = page_header_get_offs_bytes(page.ptr, PAGE_HEAP_TOP);
    if (heap_top == 0) {
        return null;
    }
    const rec_len: ulint = @intCast(rec_bytes.len);
    const end = heap_top + rec_len;
    if (end > compat.UNIV_PAGE_SIZE - PAGE_DIR) {
        return null;
    }

    const start_idx = @as(usize, @intCast(heap_top));
    const end_idx = @as(usize, @intCast(end));
    std.mem.copyForwards(u8, page[start_idx..end_idx], rec_bytes);

    page_header_set_field_bytes(page.ptr, PAGE_HEAP_TOP, end);
    page_header_set_field_bytes(page.ptr, PAGE_N_HEAP, page_header_get_field_bytes(page.ptr, PAGE_N_HEAP) + 1);
    page_header_set_field_bytes(page.ptr, PAGE_N_RECS, page_header_get_field_bytes(page.ptr, PAGE_N_RECS) + 1);
    return heap_top;
}

pub fn page_rec_set_deleted_bytes(page: [*]byte, rec_offs: ulint, deleted: bool) void {
    const rec_ptr = page + @as(usize, @intCast(rec_offs));
    rec_mod.rec_set_deleted_flag_new(rec_ptr, if (deleted) 1 else 0);
}

pub fn page_rec_is_deleted_bytes(page: [*]const byte, rec_offs: ulint) bool {
    const rec_ptr = page + @as(usize, @intCast(rec_offs));
    return rec_mod.rec_get_deleted_flag(rec_ptr, true) != 0;
}

pub fn page_rec_delete_bytes(page: [*]byte, rec_offs: ulint, rec_size: ulint) void {
    page_rec_set_deleted_bytes(page, rec_offs, true);
    page_garbage_add_bytes(page, rec_size);
}

const PageRecSpan = struct {
    header_offs: ulint,
    extra: ulint,
    total_len: ulint,
};

fn page_dir_rebuild_bytes(page: [*]byte, rec_offsets: []const ulint) void {
    page_header_set_field_bytes(page, PAGE_N_DIR_SLOTS, @as(ulint, @intCast(rec_offsets.len)));
    var i: ulint = 0;
    while (i < rec_offsets.len) : (i += 1) {
        page_dir_set_nth_slot(page, i, rec_offsets[@as(usize, @intCast(i))]);
    }
}

pub fn page_reorganize_bytes(page: []u8, fields: []const rec_mod.FieldMeta) bool {
    if (fields.len == 0) {
        return false;
    }

    const n_slots = page_header_get_field_bytes(page.ptr, PAGE_N_DIR_SLOTS);
    if (n_slots == 0) {
        return false;
    }

    const allocator = std.heap.page_allocator;
    const slot_count: usize = @intCast(n_slots);
    var rec_offsets = allocator.alloc(ulint, slot_count) catch return false;
    defer allocator.free(rec_offsets);

    var i: usize = 0;
    while (i < slot_count) : (i += 1) {
        rec_offsets[i] = page_dir_get_nth_slot_val(page.ptr, @as(ulint, @intCast(i)));
    }
    std.sort.pdq(ulint, rec_offsets, {}, comptime std.sort.asc(ulint));

    const offs_needed = rec_mod.REC_OFFS_HEADER_SIZE + 1 + @as(ulint, @intCast(fields.len));
    const offsets_buf = allocator.alloc(ulint, @as(usize, @intCast(offs_needed))) catch return false;
    defer allocator.free(offsets_buf);

    var spans = allocator.alloc(PageRecSpan, slot_count) catch return false;
    defer allocator.free(spans);

    var live_count: usize = 0;
    var total_size: ulint = 0;
    var min_header_live: ?ulint = null;
    var min_header_any: ?ulint = null;
    var deleted_found = false;
    const had_garbage = page_garbage_get_bytes(page.ptr) != 0;

    for (rec_offsets) |rec_offs| {
        if (rec_offs == 0) {
            continue;
        }
        const rec_ptr = page.ptr + @as(usize, @intCast(rec_offs));
        rec_mod.rec_init_offsets_compact(rec_ptr, rec_mod.REC_N_NEW_EXTRA_BYTES, fields, offsets_buf);
        const n_fields = rec_mod.rec_offs_n_fields(offsets_buf);
        if (n_fields == 0) {
            continue;
        }
        var len: ulint = 0;
        const last_offs = rec_mod.rec_get_nth_field_offs(offsets_buf, n_fields - 1, &len);
        const data_len = last_offs + len;
        const extra = offsets_buf[rec_mod.REC_OFFS_HEADER_SIZE] & rec_mod.REC_OFFS_MASK;
        if (rec_offs < extra) {
            return false;
        }
        const header_offs = rec_offs - extra;
        if (min_header_any == null or header_offs < min_header_any.?) {
            min_header_any = header_offs;
        }

        if (page_rec_is_deleted_bytes(page.ptr, rec_offs)) {
            deleted_found = true;
            continue;
        }

        if (min_header_live == null or header_offs < min_header_live.?) {
            min_header_live = header_offs;
        }
        const total = extra + data_len;
        spans[live_count] = .{
            .header_offs = header_offs,
            .extra = extra,
            .total_len = total,
        };
        live_count += 1;
        total_size += total;
    }

    if (live_count == 0) {
        const reset_top = min_header_any orelse page_header_get_offs_bytes(page.ptr, PAGE_HEAP_TOP);
        page_header_set_field_bytes(page.ptr, PAGE_HEAP_TOP, reset_top);
        page_header_set_field_bytes(page.ptr, PAGE_N_HEAP, 0);
        page_header_set_field_bytes(page.ptr, PAGE_N_RECS, 0);
        page_header_set_field_bytes(page.ptr, PAGE_N_DIR_SLOTS, 0);
        page_free_set_bytes(page.ptr, 0);
        page_garbage_set_bytes(page.ptr, 0);
        return deleted_found or had_garbage;
    }

    const start = min_header_live orelse return false;
    const dir_limit = compat.UNIV_PAGE_SIZE - PAGE_DIR - @as(ulint, @intCast(live_count)) * PAGE_DIR_SLOT_SIZE;
    if (start + total_size > dir_limit) {
        return false;
    }

    var temp = allocator.alloc(u8, @as(usize, @intCast(total_size))) catch return false;
    defer allocator.free(temp);
    var new_offsets = allocator.alloc(ulint, live_count) catch return false;
    defer allocator.free(new_offsets);

    var temp_pos: ulint = 0;
    i = 0;
    while (i < live_count) : (i += 1) {
        const span = spans[i];
        const src_start = span.header_offs;
        const src_end = span.header_offs + span.total_len;
        const src = page[@as(usize, @intCast(src_start))..@as(usize, @intCast(src_end))];
        const dst = temp[@as(usize, @intCast(temp_pos))..@as(usize, @intCast(temp_pos + span.total_len))];
        std.mem.copyForwards(u8, dst, src);

        const new_header = start + temp_pos;
        new_offsets[i] = new_header + span.extra;
        temp_pos += span.total_len;
    }

    const dest_start = @as(usize, @intCast(start));
    std.mem.copyForwards(u8, page[dest_start .. dest_start + temp.len], temp);

    page_header_set_field_bytes(page.ptr, PAGE_HEAP_TOP, start + total_size);
    page_header_set_field_bytes(page.ptr, PAGE_N_HEAP, @as(ulint, @intCast(live_count)));
    page_header_set_field_bytes(page.ptr, PAGE_N_RECS, @as(ulint, @intCast(live_count)));
    page_free_set_bytes(page.ptr, 0);
    page_garbage_set_bytes(page.ptr, 0);
    page_dir_rebuild_bytes(page.ptr, new_offsets);

    return deleted_found or had_garbage;
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

pub const page_cur_bytes_t = struct {
    page: []u8 = &[_]u8{},
    slot: isize = -1,
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

pub fn page_cur_bytes_set_before_first(page: []u8, cur: *page_cur_bytes_t) void {
    cur.page = page;
    cur.slot = -1;
}

pub fn page_cur_bytes_set_after_last(page: []u8, cur: *page_cur_bytes_t) void {
    cur.page = page;
    const n_slots = page_header_get_field_bytes(page.ptr, PAGE_N_DIR_SLOTS);
    cur.slot = @as(isize, @intCast(n_slots));
}

pub fn page_cur_bytes_is_before_first(cur: *const page_cur_bytes_t) bool {
    return cur.slot < 0;
}

pub fn page_cur_bytes_is_after_last(cur: *const page_cur_bytes_t) bool {
    if (cur.page.len == 0) {
        return true;
    }
    const n_slots = page_header_get_field_bytes(cur.page.ptr, PAGE_N_DIR_SLOTS);
    return cur.slot >= @as(isize, @intCast(n_slots));
}

pub fn page_cur_bytes_move_to_next(cur: *page_cur_bytes_t) void {
    const n_slots = page_header_get_field_bytes(cur.page.ptr, PAGE_N_DIR_SLOTS);
    if (cur.slot < @as(isize, @intCast(n_slots))) {
        cur.slot += 1;
    }
}

pub fn page_cur_bytes_move_to_prev(cur: *page_cur_bytes_t) void {
    if (cur.slot >= 0) {
        cur.slot -= 1;
    }
}

pub fn page_cur_bytes_get_rec_offs(cur: *const page_cur_bytes_t) ?ulint {
    if (page_cur_bytes_is_before_first(cur) or page_cur_bytes_is_after_last(cur)) {
        return null;
    }
    return page_dir_get_nth_slot_val(cur.page.ptr, @as(ulint, @intCast(cur.slot)));
}

pub fn page_cur_bytes_get_rec_ptr(cur: *const page_cur_bytes_t) ?[*]const byte {
    const rec_offs = page_cur_bytes_get_rec_offs(cur) orelse return null;
    return cur.page.ptr + @as(usize, @intCast(rec_offs));
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

test "page bytes append insert" {
    var buf = [_]byte{0} ** compat.UNIV_PAGE_SIZE;
    const page = buf[0..];
    page_header_set_field_bytes(page.ptr, PAGE_HEAP_TOP, 200);
    page_header_set_field_bytes(page.ptr, PAGE_N_HEAP, 0);
    page_header_set_field_bytes(page.ptr, PAGE_N_RECS, 0);

    const rec1 = "abc";
    const off1 = page_bytes_insert_append(page, rec1) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(ulint, 200), off1);
    try std.testing.expect(std.mem.eql(u8, page[@intCast(off1) .. @intCast(off1 + rec1.len)], rec1));

    const rec2 = "xyz12";
    const off2 = page_bytes_insert_append(page, rec2) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(ulint, 200 + rec1.len), off2);
    try std.testing.expect(std.mem.eql(u8, page[@intCast(off2) .. @intCast(off2 + rec2.len)], rec2));

    try std.testing.expectEqual(@as(ulint, 2), page_header_get_field_bytes(page.ptr, PAGE_N_HEAP));
    try std.testing.expectEqual(@as(ulint, 2), page_header_get_field_bytes(page.ptr, PAGE_N_RECS));
}

test "page delete mark bytes" {
    var buf = [_]byte{0} ** compat.UNIV_PAGE_SIZE;
    const page = buf[0..];
    page_header_set_field_bytes(page.ptr, PAGE_HEAP_TOP, 200);
    page_header_set_field_bytes(page.ptr, PAGE_N_HEAP, 0);
    page_header_set_field_bytes(page.ptr, PAGE_N_RECS, 0);
    page_garbage_set_bytes(page.ptr, 0);

    const meta = [_]rec_mod.FieldMeta{.{ .fixed_len = 0, .max_len = 10, .nullable = false }};
    var field = data.dfield_t{};
    data.dfield_set_data(&field, "abc".ptr, 3);
    var tuple = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = (&field)[0..1] };

    const header_len: ulint = rec_mod.REC_N_NEW_EXTRA_BYTES + 1 + 1;
    var rec_buf = [_]byte{0} ** 16;
    const rec_len = header_len + 3;
    const rec_storage = rec_buf[0..@as(usize, @intCast(rec_len))];
    const rec_ptr = @as([*]byte, @ptrCast(rec_storage[@as(usize, @intCast(header_len))..].ptr));
    _ = rec_mod.rec_encode_compact(rec_ptr, rec_mod.REC_N_NEW_EXTRA_BYTES, &meta, &tuple);

    const off = page_bytes_insert_append(page, rec_storage) orelse return error.TestExpectedEqual;
    const rec_offs = off + header_len;
    page_rec_delete_bytes(page.ptr, rec_offs, rec_len);

    try std.testing.expect(page_rec_is_deleted_bytes(page.ptr, rec_offs));
    try std.testing.expectEqual(rec_len, page_garbage_get_bytes(page.ptr));
}

test "page reorganize bytes compacts records" {
    var buf = [_]byte{0} ** compat.UNIV_PAGE_SIZE;
    const page = buf[0..];
    page_header_set_field_bytes(page.ptr, PAGE_HEAP_TOP, 200);
    page_header_set_field_bytes(page.ptr, PAGE_N_HEAP, 0);
    page_header_set_field_bytes(page.ptr, PAGE_N_RECS, 0);
    page_header_set_field_bytes(page.ptr, PAGE_N_DIR_SLOTS, 0);
    page_garbage_set_bytes(page.ptr, 0);

    const meta = [_]rec_mod.FieldMeta{.{ .fixed_len = 0, .max_len = 10, .nullable = false }};
    const header_len: ulint = rec_mod.REC_N_NEW_EXTRA_BYTES + 1 + 1;

    var field1 = data.dfield_t{};
    data.dfield_set_data(&field1, "aa".ptr, 2);
    var tuple1 = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = (&field1)[0..1] };
    var rec1_buf = [_]byte{0} ** 16;
    const rec1_len: ulint = header_len + 2;
    const rec1_storage = rec1_buf[0..@as(usize, @intCast(rec1_len))];
    const rec1_ptr = @as([*]byte, @ptrCast(rec1_storage[@as(usize, @intCast(header_len))..].ptr));
    _ = rec_mod.rec_encode_compact(rec1_ptr, rec_mod.REC_N_NEW_EXTRA_BYTES, &meta, &tuple1);

    var field2 = data.dfield_t{};
    data.dfield_set_data(&field2, "bbb".ptr, 3);
    var tuple2 = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = (&field2)[0..1] };
    var rec2_buf = [_]byte{0} ** 16;
    const rec2_len: ulint = header_len + 3;
    const rec2_storage = rec2_buf[0..@as(usize, @intCast(rec2_len))];
    const rec2_ptr = @as([*]byte, @ptrCast(rec2_storage[@as(usize, @intCast(header_len))..].ptr));
    _ = rec_mod.rec_encode_compact(rec2_ptr, rec_mod.REC_N_NEW_EXTRA_BYTES, &meta, &tuple2);

    var field3 = data.dfield_t{};
    data.dfield_set_data(&field3, "cccc".ptr, 4);
    var tuple3 = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = (&field3)[0..1] };
    var rec3_buf = [_]byte{0} ** 16;
    const rec3_len: ulint = header_len + 4;
    const rec3_storage = rec3_buf[0..@as(usize, @intCast(rec3_len))];
    const rec3_ptr = @as([*]byte, @ptrCast(rec3_storage[@as(usize, @intCast(header_len))..].ptr));
    _ = rec_mod.rec_encode_compact(rec3_ptr, rec_mod.REC_N_NEW_EXTRA_BYTES, &meta, &tuple3);

    const off1 = page_bytes_insert_append(page, rec1_storage) orelse return error.TestExpectedEqual;
    const off2 = page_bytes_insert_append(page, rec2_storage) orelse return error.TestExpectedEqual;
    const off3 = page_bytes_insert_append(page, rec3_storage) orelse return error.TestExpectedEqual;

    const rec1_offs = off1 + header_len;
    const rec2_offs = off2 + header_len;
    const rec3_offs = off3 + header_len;

    var size_offsets = [_]ulint{0} ** 16;
    rec_mod.rec_init_offsets_compact(rec2_ptr, rec_mod.REC_N_NEW_EXTRA_BYTES, &meta, size_offsets[0..]);
    const rec2_extra = size_offsets[rec_mod.REC_OFFS_HEADER_SIZE] & rec_mod.REC_OFFS_MASK;
    var rec2_field_len: ulint = 0;
    const rec2_last_offs = rec_mod.rec_get_nth_field_offs(size_offsets[0..], 0, &rec2_field_len);
    const rec2_total = rec2_extra + rec2_last_offs + rec2_field_len;

    page_rec_delete_bytes(page.ptr, rec2_offs, rec2_total);
    page_header_set_field_bytes(page.ptr, PAGE_N_DIR_SLOTS, 3);
    page_dir_set_nth_slot(page.ptr, 0, rec1_offs);
    page_dir_set_nth_slot(page.ptr, 1, rec2_offs);
    page_dir_set_nth_slot(page.ptr, 2, rec3_offs);

    rec_mod.rec_init_offsets_compact(rec1_ptr, rec_mod.REC_N_NEW_EXTRA_BYTES, &meta, size_offsets[0..]);
    const rec1_extra = size_offsets[rec_mod.REC_OFFS_HEADER_SIZE] & rec_mod.REC_OFFS_MASK;
    var rec1_field_len: ulint = 0;
    const rec1_last_offs = rec_mod.rec_get_nth_field_offs(size_offsets[0..], 0, &rec1_field_len);
    const rec1_total = rec1_extra + rec1_last_offs + rec1_field_len;

    @memset(size_offsets[0..], 0);
    rec_mod.rec_init_offsets_compact(rec3_ptr, rec_mod.REC_N_NEW_EXTRA_BYTES, &meta, size_offsets[0..]);
    const rec3_extra = size_offsets[rec_mod.REC_OFFS_HEADER_SIZE] & rec_mod.REC_OFFS_MASK;
    var rec3_field_len: ulint = 0;
    const rec3_last_offs = rec_mod.rec_get_nth_field_offs(size_offsets[0..], 0, &rec3_field_len);
    const rec3_total = rec3_extra + rec3_last_offs + rec3_field_len;

    const expected_start = rec1_offs - rec1_extra;
    const expected_heap_top = expected_start + rec1_total + rec3_total;
    try std.testing.expect(page_reorganize_bytes(page, &meta));

    try std.testing.expectEqual(@as(ulint, 0), page_garbage_get_bytes(page.ptr));
    try std.testing.expectEqual(@as(ulint, 2), page_header_get_field_bytes(page.ptr, PAGE_N_RECS));
    try std.testing.expectEqual(@as(ulint, 2), page_header_get_field_bytes(page.ptr, PAGE_N_HEAP));
    try std.testing.expectEqual(@as(ulint, 2), page_header_get_field_bytes(page.ptr, PAGE_N_DIR_SLOTS));
    try std.testing.expectEqual(expected_heap_top, page_header_get_offs_bytes(page.ptr, PAGE_HEAP_TOP));

    const new_rec1 = page_dir_get_nth_slot_val(page.ptr, 0);
    const new_rec2 = page_dir_get_nth_slot_val(page.ptr, 1);

    var offsets = [_]ulint{0} ** 16;
    rec_mod.rec_init_offsets_compact(page.ptr + @as(usize, @intCast(new_rec1)), rec_mod.REC_N_NEW_EXTRA_BYTES, &meta, offsets[0..]);
    var out_field1 = data.dfield_t{};
    var out_tuple1 = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = (&out_field1)[0..1] };
    rec_mod.rec_decode_to_dtuple(page.ptr + @as(usize, @intCast(new_rec1)), offsets[0..], &out_tuple1);
    const f1_ptr = data.dfield_get_data(&out_field1).?;
    const f1_len = data.dfield_get_len(&out_field1);
    try std.testing.expectEqualStrings("aa", @as([*]const byte, @ptrCast(f1_ptr))[0..@as(usize, @intCast(f1_len))]);

    @memset(offsets[0..], 0);
    rec_mod.rec_init_offsets_compact(page.ptr + @as(usize, @intCast(new_rec2)), rec_mod.REC_N_NEW_EXTRA_BYTES, &meta, offsets[0..]);
    var out_field2 = data.dfield_t{};
    var out_tuple2 = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = (&out_field2)[0..1] };
    rec_mod.rec_decode_to_dtuple(page.ptr + @as(usize, @intCast(new_rec2)), offsets[0..], &out_tuple2);
    const f2_ptr = data.dfield_get_data(&out_field2).?;
    const f2_len = data.dfield_get_len(&out_field2);
    try std.testing.expectEqualStrings("cccc", @as([*]const byte, @ptrCast(f2_ptr))[0..@as(usize, @intCast(f2_len))]);
}

test "page cursor bytes walks directory slots" {
    var buf = [_]byte{0} ** compat.UNIV_PAGE_SIZE;
    const page = buf[0..];
    page_header_set_field_bytes(page.ptr, PAGE_HEAP_TOP, 200);
    page_header_set_field_bytes(page.ptr, PAGE_N_HEAP, 0);
    page_header_set_field_bytes(page.ptr, PAGE_N_RECS, 0);
    page_header_set_field_bytes(page.ptr, PAGE_N_DIR_SLOTS, 0);

    const meta = [_]rec_mod.FieldMeta{.{ .fixed_len = 0, .max_len = 10, .nullable = false }};
    const header_len: ulint = rec_mod.REC_N_NEW_EXTRA_BYTES + 1 + 1;

    var field1 = data.dfield_t{};
    data.dfield_set_data(&field1, "one".ptr, 3);
    var tuple1 = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = (&field1)[0..1] };
    var rec1_buf = [_]byte{0} ** 32;
    const rec1_len: ulint = header_len + 3;
    const rec1_storage = rec1_buf[0..@as(usize, @intCast(rec1_len))];
    const rec1_ptr = @as([*]byte, @ptrCast(rec1_storage[@as(usize, @intCast(header_len))..].ptr));
    _ = rec_mod.rec_encode_compact(rec1_ptr, rec_mod.REC_N_NEW_EXTRA_BYTES, &meta, &tuple1);

    var field2 = data.dfield_t{};
    data.dfield_set_data(&field2, "two".ptr, 3);
    var tuple2 = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = (&field2)[0..1] };
    var rec2_buf = [_]byte{0} ** 32;
    const rec2_len: ulint = header_len + 3;
    const rec2_storage = rec2_buf[0..@as(usize, @intCast(rec2_len))];
    const rec2_ptr = @as([*]byte, @ptrCast(rec2_storage[@as(usize, @intCast(header_len))..].ptr));
    _ = rec_mod.rec_encode_compact(rec2_ptr, rec_mod.REC_N_NEW_EXTRA_BYTES, &meta, &tuple2);

    var field3 = data.dfield_t{};
    data.dfield_set_data(&field3, "three".ptr, 5);
    var tuple3 = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = (&field3)[0..1] };
    var rec3_buf = [_]byte{0} ** 32;
    const rec3_len: ulint = header_len + 5;
    const rec3_storage = rec3_buf[0..@as(usize, @intCast(rec3_len))];
    const rec3_ptr = @as([*]byte, @ptrCast(rec3_storage[@as(usize, @intCast(header_len))..].ptr));
    _ = rec_mod.rec_encode_compact(rec3_ptr, rec_mod.REC_N_NEW_EXTRA_BYTES, &meta, &tuple3);

    const off1 = page_bytes_insert_append(page, rec1_storage) orelse return error.TestExpectedEqual;
    const off2 = page_bytes_insert_append(page, rec2_storage) orelse return error.TestExpectedEqual;
    const off3 = page_bytes_insert_append(page, rec3_storage) orelse return error.TestExpectedEqual;

    const rec1_offs = off1 + header_len;
    const rec2_offs = off2 + header_len;
    const rec3_offs = off3 + header_len;

    page_header_set_field_bytes(page.ptr, PAGE_N_DIR_SLOTS, 3);
    page_dir_set_nth_slot(page.ptr, 0, rec1_offs);
    page_dir_set_nth_slot(page.ptr, 1, rec2_offs);
    page_dir_set_nth_slot(page.ptr, 2, rec3_offs);

    var cur = page_cur_bytes_t{};
    page_cur_bytes_set_before_first(page, &cur);
    try std.testing.expect(page_cur_bytes_is_before_first(&cur));

    page_cur_bytes_move_to_next(&cur);
    const p1 = page_cur_bytes_get_rec_ptr(&cur) orelse return error.TestExpectedEqual;
    var offsets = [_]ulint{0} ** 16;
    rec_mod.rec_init_offsets_compact(p1, rec_mod.REC_N_NEW_EXTRA_BYTES, &meta, offsets[0..]);
    var out_field = data.dfield_t{};
    var out_tuple = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = (&out_field)[0..1] };
    rec_mod.rec_decode_to_dtuple(p1, offsets[0..], &out_tuple);
    const f1_ptr = data.dfield_get_data(&out_field).?;
    const f1_len = data.dfield_get_len(&out_field);
    try std.testing.expectEqualStrings("one", @as([*]const byte, @ptrCast(f1_ptr))[0..@as(usize, @intCast(f1_len))]);

    page_cur_bytes_move_to_next(&cur);
    const p2 = page_cur_bytes_get_rec_ptr(&cur) orelse return error.TestExpectedEqual;
    @memset(offsets[0..], 0);
    rec_mod.rec_init_offsets_compact(p2, rec_mod.REC_N_NEW_EXTRA_BYTES, &meta, offsets[0..]);
    out_field = .{};
    out_tuple = .{ .n_fields = 1, .n_fields_cmp = 1, .fields = (&out_field)[0..1] };
    rec_mod.rec_decode_to_dtuple(p2, offsets[0..], &out_tuple);
    const f2_ptr = data.dfield_get_data(&out_field).?;
    const f2_len = data.dfield_get_len(&out_field);
    try std.testing.expectEqualStrings("two", @as([*]const byte, @ptrCast(f2_ptr))[0..@as(usize, @intCast(f2_len))]);

    page_cur_bytes_move_to_next(&cur);
    const p3 = page_cur_bytes_get_rec_ptr(&cur) orelse return error.TestExpectedEqual;
    @memset(offsets[0..], 0);
    rec_mod.rec_init_offsets_compact(p3, rec_mod.REC_N_NEW_EXTRA_BYTES, &meta, offsets[0..]);
    out_field = .{};
    out_tuple = .{ .n_fields = 1, .n_fields_cmp = 1, .fields = (&out_field)[0..1] };
    rec_mod.rec_decode_to_dtuple(p3, offsets[0..], &out_tuple);
    const f3_ptr = data.dfield_get_data(&out_field).?;
    const f3_len = data.dfield_get_len(&out_field);
    try std.testing.expectEqualStrings("three", @as([*]const byte, @ptrCast(f3_ptr))[0..@as(usize, @intCast(f3_len))]);

    page_cur_bytes_move_to_next(&cur);
    try std.testing.expect(page_cur_bytes_is_after_last(&cur));

    page_cur_bytes_move_to_prev(&cur);
    const p3b = page_cur_bytes_get_rec_ptr(&cur) orelse return error.TestExpectedEqual;
    @memset(offsets[0..], 0);
    rec_mod.rec_init_offsets_compact(p3b, rec_mod.REC_N_NEW_EXTRA_BYTES, &meta, offsets[0..]);
    out_field = .{};
    out_tuple = .{ .n_fields = 1, .n_fields_cmp = 1, .fields = (&out_field)[0..1] };
    rec_mod.rec_decode_to_dtuple(p3b, offsets[0..], &out_tuple);
    const f3b_ptr = data.dfield_get_data(&out_field).?;
    const f3b_len = data.dfield_get_len(&out_field);
    try std.testing.expectEqualStrings("three", @as([*]const byte, @ptrCast(f3b_ptr))[0..@as(usize, @intCast(f3b_len))]);
}
