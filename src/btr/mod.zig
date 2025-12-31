const std = @import("std");
const compat = @import("../ut/compat.zig");
const page = @import("../page/mod.zig");
const dict = @import("../dict/mod.zig");

pub const module_name = "btr";

pub const ulint = compat.ulint;
pub const byte = compat.byte;
pub const ibool = compat.ibool;
pub const dulint = compat.Dulint;
pub const ib_int64_t = compat.ib_int64_t;

pub const BTR_EXTERN_FIELD_REF_SIZE: usize = 20;

pub const BTR_PAGE_MAX_REC_SIZE: ulint = compat.UNIV_PAGE_SIZE / 2 - 200;
pub const BTR_MAX_LEVELS: ulint = 100;
pub const BTR_CUR_PAGE_REORGANIZE_LIMIT: ulint = compat.UNIV_PAGE_SIZE / 32;
pub const BTR_BLOB_HDR_PART_LEN: ulint = 0;
pub const BTR_BLOB_HDR_NEXT_PAGE_NO: ulint = 4;
pub const BTR_BLOB_HDR_SIZE: ulint = 8;

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
pub const BTR_NO_UNDO_LOG_FLAG: ulint = 1;
pub const BTR_NO_LOCKING_FLAG: ulint = 2;
pub const BTR_KEEP_SYS_FLAG: ulint = 4;
pub const BTR_PCUR_ON: ulint = 1;
pub const BTR_PCUR_BEFORE: ulint = 2;
pub const BTR_PCUR_AFTER: ulint = 3;
pub const BTR_PCUR_BEFORE_FIRST_IN_TREE: ulint = 4;
pub const BTR_PCUR_AFTER_LAST_IN_TREE: ulint = 5;

pub var btr_cur_print_record_ops: ibool = compat.FALSE;
pub var btr_cur_n_non_sea: ulint = 0;
pub var btr_cur_n_sea: ulint = 0;
pub var btr_cur_n_non_sea_old: ulint = 0;
pub var btr_cur_n_sea_old: ulint = 0;
pub const field_ref_zero = [_]byte{0} ** BTR_EXTERN_FIELD_REF_SIZE;
pub var btr_search_enabled: u8 = 1;
pub var btr_search_this_is_zero: ulint = 0;
pub var btr_search_sys: ?*btr_search_sys_t = null;

const IndexState = struct {
    pages: std.AutoHashMap(ulint, *buf_block_t),
    next_page_no: ulint,
};

var index_states = std.AutoHashMap(*dict_index_t, *IndexState).init(std.heap.page_allocator);

pub const rec_t = page.rec_t;
pub const dict_index_t = dict.dict_index_t;
pub const mtr_t = page.mtr_t;
pub const buf_block_t = page.buf_block_t;
pub const page_t = page.page_t;
pub const page_zip_des_t = page.page_zip_des_t;
pub const upd_t = struct {};
pub const que_thr_t = struct {};
pub const big_rec_t = struct {};
pub const mem_heap_t = struct {};
pub const trx_t = struct {};
pub const page_cur_t = page.page_cur_t;
pub const trx_rb_ctx = enum(u8) {
    TRX_RB_NONE = 0,
};

pub const btr_cur_t = struct {
    index: ?*dict_index_t = null,
    rec: ?*rec_t = null,
    block: ?*buf_block_t = null,
    opened: bool = false,
};

pub const btr_pcur_t = struct {
    btr_cur: btr_cur_t = .{},
    rel_pos: ulint = 0,
    stored: bool = false,
};

pub const btr_search_t = struct {
    ref_count: ulint = 0,
};

pub const btr_search_sys_t = struct {
    hash_size: ulint = 0,
};

pub const dtuple_t = struct {};

pub fn btr_root_get(index: *dict_index_t, mtr: *mtr_t) ?*page_t {
    _ = index;
    _ = mtr;
    return null;
}

fn btr_page_set_index_id(page_obj: *page_t, page_zip: ?*page_zip_des_t, id: dulint, mtr: *mtr_t) void {
    _ = page_zip;
    _ = mtr;
    page_obj.header.index_id = id;
}

fn btr_page_get_index_id(page_obj: *const page_t) dulint {
    return page_obj.header.index_id;
}

fn btr_page_set_level(page_obj: *page_t, page_zip: ?*page_zip_des_t, level: ulint, mtr: *mtr_t) void {
    _ = page_zip;
    _ = mtr;
    page_obj.header.level = level;
}

fn btr_page_get_level(page_obj: *const page_t, mtr: *mtr_t) ulint {
    _ = mtr;
    return page_obj.header.level;
}

fn index_state_get(index: *dict_index_t) ?*IndexState {
    if (index_states.get(index)) |state| {
        return state;
    }
    const allocator = std.heap.page_allocator;
    const state = allocator.create(IndexState) catch return null;
    state.* = .{
        .pages = std.AutoHashMap(ulint, *buf_block_t).init(allocator),
        .next_page_no = 1,
    };
    index_states.put(index, state) catch {
        state.pages.deinit();
        allocator.destroy(state);
        return null;
    };
    return state;
}

fn index_state_remove(index: *dict_index_t) void {
    const allocator = std.heap.page_allocator;
    if (index_states.fetchRemove(index)) |entry| {
        var it = entry.value.pages.valueIterator();
        while (it.next()) |block_ptr| {
            allocator.destroy(block_ptr.*.frame);
            allocator.destroy(block_ptr.*);
        }
        entry.value.pages.deinit();
        allocator.destroy(entry.value);
    }
}

fn btr_node_ptr_set_child_page_no(rec: *rec_t, page_no: ulint) void {
    rec.child_page_no = page_no;
}

fn btr_node_ptr_get_child_page_no(rec: *const rec_t) ulint {
    return rec.child_page_no;
}

fn btr_page_create(block: *buf_block_t, page_zip: ?*page_zip_des_t, index: *dict_index_t, level: ulint, mtr: *mtr_t) void {
    if (page_zip != null) {
        _ = page.page_create_zip(block, index, level, mtr);
    } else {
        _ = page.page_create(block, mtr, 0);
        btr_page_set_level(block.frame, null, level, mtr);
    }
    btr_page_set_index_id(block.frame, page_zip, index.id, mtr);
}

fn btr_page_empty(block: *buf_block_t, page_zip: ?*page_zip_des_t, index: *dict_index_t, level: ulint, mtr: *mtr_t) void {
    btr_search_drop_page_hash_index(block);
    if (page_zip != null) {
        _ = page.page_create_zip(block, index, level, mtr);
    } else {
        _ = page.page_create(block, mtr, 0);
        btr_page_set_level(block.frame, null, level, mtr);
    }
    btr_page_set_index_id(block.frame, page_zip, index.id, mtr);
}

fn page_first_user_rec(page_obj: *page_t) ?*rec_t {
    const first = page_obj.infimum.next orelse return null;
    return if (first.is_supremum) null else first;
}

fn find_node_ptr(parent_page: *page_t, child_block: *buf_block_t) ?*rec_t {
    var current = parent_page.infimum.next;
    const child_no = child_block.frame.page_no;
    while (current) |node| {
        if (node.is_supremum) {
            return null;
        }
        if (node.child_block) |blk| {
            if (blk == child_block) {
                return node;
            }
        } else if (node.child_page_no != 0 and node.child_page_no == child_no) {
            return node;
        }
        current = node.next;
    }
    return null;
}

pub fn btr_node_ptr_get_child(node_ptr: *const rec_t, index: *dict_index_t, offsets: *const ulint, mtr: *mtr_t) ?*buf_block_t {
    _ = index;
    _ = offsets;
    _ = mtr;
    return node_ptr.child_block;
}

pub fn btr_page_get_father_node_ptr_func(
    offsets: ?*ulint,
    heap: ?*mem_heap_t,
    cursor: *btr_cur_t,
    file: []const u8,
    line: ulint,
    mtr: *mtr_t,
) ?*ulint {
    _ = heap;
    _ = file;
    _ = line;
    _ = mtr;
    const child_block = cursor.block orelse return offsets;
    const parent_block = child_block.frame.parent_block orelse return offsets;
    const node_ptr = find_node_ptr(parent_block.frame, child_block) orelse return offsets;
    cursor.block = parent_block;
    cursor.rec = node_ptr;
    cursor.opened = true;
    return offsets;
}

pub fn btr_page_get_father_block(
    offsets: ?*ulint,
    heap: ?*mem_heap_t,
    index: *dict_index_t,
    block: *buf_block_t,
    mtr: *mtr_t,
    cursor: *btr_cur_t,
) ?*ulint {
    _ = index;
    cursor.block = block;
    cursor.rec = page_first_user_rec(block.frame);
    cursor.opened = true;
    return btr_page_get_father_node_ptr_func(offsets, heap, cursor, "btr_page_get_father_block", 0, mtr);
}

pub fn btr_page_get_father(index: *dict_index_t, block: *buf_block_t, mtr: *mtr_t, cursor: *btr_cur_t) void {
    _ = btr_page_get_father_block(null, null, index, block, mtr, cursor);
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
    _ = hint_page_no;
    _ = file_direction;
    _ = mtr;
    const state = index_state_get(index) orelse return null;
    const allocator = std.heap.page_allocator;
    const page_obj = allocator.create(page_t) catch return null;
    page_obj.* = .{};
    page_obj.page_no = state.next_page_no;
    state.next_page_no += 1;
    page.page_init(page_obj);
    page_obj.header.level = level;

    const block = allocator.create(buf_block_t) catch {
        allocator.destroy(page_obj);
        return null;
    };
    block.* = .{ .frame = page_obj, .page_zip = null };

    state.pages.put(page_obj.page_no, block) catch {
        allocator.destroy(block);
        allocator.destroy(page_obj);
        return null;
    };

    return block;
}

pub fn btr_get_size(index: *dict_index_t, flag: ulint) ulint {
    _ = flag;
    const state = index_states.get(index) orelse return 0;
    return @intCast(state.pages.count());
}

pub fn btr_page_free_low(index: *dict_index_t, block: *buf_block_t, level: ulint, mtr: *mtr_t) void {
    _ = level;
    _ = mtr;
    const state = index_states.get(index) orelse return;
    const page_no = block.frame.page_no;
    if (state.pages.fetchRemove(page_no)) |entry| {
        std.heap.page_allocator.destroy(entry.value.frame);
        std.heap.page_allocator.destroy(entry.value);
    }
}

pub fn btr_page_free(index: *dict_index_t, block: *buf_block_t, mtr: *mtr_t) void {
    btr_page_free_low(index, block, 0, mtr);
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

pub fn btr_parse_set_min_rec_mark(ptr: [*]byte, end_ptr: [*]byte, comp: ulint, page_ptr: ?*page_t, mtr: ?*mtr_t) [*]byte {
    _ = end_ptr;
    _ = comp;
    _ = page_ptr;
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

pub fn btr_cur_var_init() void {
    btr_cur_n_non_sea = 0;
    btr_cur_n_sea = 0;
    btr_cur_n_non_sea_old = 0;
    btr_cur_n_sea_old = 0;
}

pub fn btr_cur_search_to_nth_level(
    index: *dict_index_t,
    level: ulint,
    tuple: *const dtuple_t,
    mode: ulint,
    latch_mode: ulint,
    cursor: *btr_cur_t,
    has_search_latch: ulint,
    file: []const u8,
    line: ulint,
    mtr: *mtr_t,
) void {
    _ = level;
    _ = tuple;
    _ = mode;
    _ = latch_mode;
    _ = has_search_latch;
    _ = file;
    _ = line;
    _ = mtr;
    cursor.index = index;
    cursor.rec = null;
    cursor.block = null;
    cursor.opened = true;
    btr_cur_n_non_sea += 1;
}

pub fn btr_cur_open_at_index_side_func(
    from_left: ibool,
    index: *dict_index_t,
    latch_mode: ulint,
    cursor: *btr_cur_t,
    file: []const u8,
    line: ulint,
    mtr: *mtr_t,
) void {
    _ = from_left;
    _ = latch_mode;
    _ = file;
    _ = line;
    _ = mtr;
    cursor.index = index;
    cursor.rec = null;
    cursor.block = null;
    cursor.opened = true;
}

pub fn btr_cur_open_at_rnd_pos_func(
    index: *dict_index_t,
    latch_mode: ulint,
    cursor: *btr_cur_t,
    file: []const u8,
    line: ulint,
    mtr: *mtr_t,
) void {
    _ = latch_mode;
    _ = file;
    _ = line;
    _ = mtr;
    cursor.index = index;
    cursor.rec = null;
    cursor.block = null;
    cursor.opened = true;
}

pub fn btr_cur_optimistic_insert(
    flags: ulint,
    cursor: *btr_cur_t,
    entry: *dtuple_t,
    rec: *?*rec_t,
    big_rec: *?*big_rec_t,
    n_ext: ulint,
    thr: ?*que_thr_t,
    mtr: *mtr_t,
) ulint {
    _ = flags;
    _ = cursor;
    _ = entry;
    _ = n_ext;
    _ = thr;
    _ = mtr;
    rec.* = null;
    big_rec.* = null;
    return 0;
}

pub fn btr_cur_pessimistic_insert(
    flags: ulint,
    cursor: *btr_cur_t,
    entry: *dtuple_t,
    rec: *?*rec_t,
    big_rec: *?*big_rec_t,
    n_ext: ulint,
    thr: ?*que_thr_t,
    mtr: *mtr_t,
) ulint {
    _ = flags;
    _ = cursor;
    _ = entry;
    _ = n_ext;
    _ = thr;
    _ = mtr;
    rec.* = null;
    big_rec.* = null;
    return 0;
}

pub fn btr_cur_parse_update_in_place(
    ptr: [*]byte,
    end_ptr: [*]byte,
    page_ptr: ?*page_t,
    page_zip: ?*page_zip_des_t,
    index: *dict_index_t,
) [*]byte {
    _ = end_ptr;
    _ = page_ptr;
    _ = page_zip;
    _ = index;
    return ptr;
}

pub fn btr_cur_update_in_place(
    flags: ulint,
    cursor: *btr_cur_t,
    update: *const upd_t,
    cmpl_info: ulint,
    thr: *que_thr_t,
    mtr: *mtr_t,
) ulint {
    _ = flags;
    _ = cursor;
    _ = update;
    _ = cmpl_info;
    _ = thr;
    _ = mtr;
    return 0;
}

pub fn btr_cur_optimistic_update(
    flags: ulint,
    cursor: *btr_cur_t,
    update: *const upd_t,
    cmpl_info: ulint,
    thr: *que_thr_t,
    mtr: *mtr_t,
) ulint {
    _ = flags;
    _ = cursor;
    _ = update;
    _ = cmpl_info;
    _ = thr;
    _ = mtr;
    return 0;
}

pub fn btr_cur_pessimistic_update(
    flags: ulint,
    cursor: *btr_cur_t,
    heap: *?*mem_heap_t,
    big_rec: *?*big_rec_t,
    update: *const upd_t,
    cmpl_info: ulint,
    thr: *que_thr_t,
    mtr: *mtr_t,
) ulint {
    _ = flags;
    _ = cursor;
    _ = heap;
    _ = update;
    _ = cmpl_info;
    _ = thr;
    _ = mtr;
    big_rec.* = null;
    return 0;
}

pub fn btr_cur_parse_del_mark_set_clust_rec(
    ptr: [*]byte,
    end_ptr: [*]byte,
    page_ptr: ?*page_t,
    page_zip: ?*page_zip_des_t,
    index: *dict_index_t,
) [*]byte {
    _ = end_ptr;
    _ = page_ptr;
    _ = page_zip;
    _ = index;
    return ptr;
}

pub fn btr_cur_del_mark_set_clust_rec(
    flags: ulint,
    cursor: *btr_cur_t,
    val: ibool,
    thr: *que_thr_t,
    mtr: *mtr_t,
) ulint {
    _ = flags;
    _ = cursor;
    _ = val;
    _ = thr;
    _ = mtr;
    return 0;
}

pub fn btr_cur_parse_del_mark_set_sec_rec(
    ptr: [*]byte,
    end_ptr: [*]byte,
    page_ptr: ?*page_t,
    page_zip: ?*page_zip_des_t,
) [*]byte {
    _ = end_ptr;
    _ = page_ptr;
    _ = page_zip;
    return ptr;
}

pub fn btr_cur_del_mark_set_sec_rec(
    flags: ulint,
    cursor: *btr_cur_t,
    val: ibool,
    thr: *que_thr_t,
    mtr: *mtr_t,
) ulint {
    _ = flags;
    _ = cursor;
    _ = val;
    _ = thr;
    _ = mtr;
    return 0;
}

pub fn btr_cur_del_unmark_for_ibuf(rec: *rec_t, page_zip: ?*page_zip_des_t, mtr: *mtr_t) void {
    _ = rec;
    _ = page_zip;
    _ = mtr;
}

pub fn btr_cur_compress_if_useful(cursor: *btr_cur_t, mtr: *mtr_t) ibool {
    _ = cursor;
    _ = mtr;
    return compat.FALSE;
}

pub fn btr_cur_optimistic_delete(cursor: *btr_cur_t, mtr: *mtr_t) ibool {
    _ = cursor;
    _ = mtr;
    return compat.FALSE;
}

pub fn btr_cur_pessimistic_delete(err: *ulint, has_reserved_extents: ibool, cursor: *btr_cur_t, rb_ctx: trx_rb_ctx, mtr: *mtr_t) ibool {
    _ = has_reserved_extents;
    _ = cursor;
    _ = rb_ctx;
    _ = mtr;
    err.* = 0;
    return compat.FALSE;
}

pub fn btr_estimate_n_rows_in_range(
    index: *dict_index_t,
    tuple1: *const dtuple_t,
    mode1: ulint,
    tuple2: *const dtuple_t,
    mode2: ulint,
) ib_int64_t {
    _ = index;
    _ = tuple1;
    _ = mode1;
    _ = tuple2;
    _ = mode2;
    return 0;
}

pub fn btr_estimate_number_of_different_key_vals(index: *dict_index_t) void {
    _ = index;
}

pub fn btr_cur_mark_extern_inherited_fields(
    page_zip: ?*page_zip_des_t,
    rec: *rec_t,
    index: *dict_index_t,
    offsets: *const ulint,
    update: *const upd_t,
    mtr: ?*mtr_t,
) void {
    _ = page_zip;
    _ = rec;
    _ = index;
    _ = offsets;
    _ = update;
    _ = mtr;
}

pub fn btr_cur_mark_dtuple_inherited_extern(entry: *dtuple_t, update: *const upd_t) void {
    _ = entry;
    _ = update;
}

pub fn btr_cur_unmark_dtuple_extern_fields(entry: *dtuple_t) void {
    _ = entry;
}

pub fn btr_push_update_extern_fields(tuple: *dtuple_t, update: *const upd_t, heap: *mem_heap_t) ulint {
    _ = tuple;
    _ = update;
    _ = heap;
    return 0;
}

pub fn btr_store_big_rec_extern_fields(
    index: *dict_index_t,
    rec_block: *buf_block_t,
    rec: *rec_t,
    offsets: *const ulint,
    big_rec_vec: *big_rec_t,
    local_mtr: ?*mtr_t,
) ulint {
    _ = index;
    _ = rec_block;
    _ = rec;
    _ = offsets;
    _ = big_rec_vec;
    _ = local_mtr;
    return 0;
}

pub fn btr_free_externally_stored_field(
    index: *dict_index_t,
    field_ref: [*]byte,
    rec: ?*const rec_t,
    offsets: ?*const ulint,
    page_zip: ?*page_zip_des_t,
    i: ulint,
    rb_ctx: trx_rb_ctx,
    local_mtr: ?*mtr_t,
) void {
    _ = index;
    _ = field_ref;
    _ = rec;
    _ = offsets;
    _ = page_zip;
    _ = i;
    _ = rb_ctx;
    _ = local_mtr;
}

pub fn btr_copy_externally_stored_field_prefix(
    buf: [*]byte,
    len: ulint,
    zip_size: ulint,
    data: [*]const byte,
    local_len: ulint,
) ulint {
    _ = zip_size;
    const max = @min(len, local_len);
    const count = @as(usize, @intCast(max));
    if (count > 0) {
        std.mem.copyForwards(byte, buf[0..count], data[0..count]);
    }
    return max;
}

pub fn btr_rec_copy_externally_stored_field(
    rec: *const rec_t,
    offsets: *const ulint,
    zip_size: ulint,
    no: ulint,
    len: *ulint,
    heap: *mem_heap_t,
) ?[*]byte {
    _ = rec;
    _ = offsets;
    _ = zip_size;
    _ = no;
    _ = heap;
    len.* = 0;
    return null;
}

pub fn btr_pcur_create() ?*btr_pcur_t {
    const cursor = std.heap.page_allocator.create(btr_pcur_t) catch return null;
    cursor.* = btr_pcur_t{};
    return cursor;
}

pub fn btr_pcur_free(cursor: ?*btr_pcur_t) void {
    if (cursor) |pcur| {
        std.heap.page_allocator.destroy(pcur);
    }
}

pub fn btr_pcur_copy_stored_position(pcur_receive: *btr_pcur_t, pcur_donate: *btr_pcur_t) void {
    pcur_receive.rel_pos = pcur_donate.rel_pos;
    pcur_receive.stored = pcur_donate.stored;
}

pub fn btr_pcur_open_on_user_rec_func(
    index: *dict_index_t,
    tuple: *const dtuple_t,
    mode: ulint,
    latch_mode: ulint,
    cursor: *btr_pcur_t,
    file: []const u8,
    line: ulint,
    mtr: *mtr_t,
) void {
    _ = tuple;
    _ = mode;
    _ = latch_mode;
    _ = file;
    _ = line;
    _ = mtr;
    cursor.btr_cur.index = index;
    cursor.btr_cur.rec = null;
    cursor.btr_cur.block = null;
    cursor.btr_cur.opened = true;
    cursor.rel_pos = BTR_PCUR_ON;
    cursor.stored = false;
}

pub fn btr_pcur_store_position(cursor: *btr_pcur_t, mtr: *mtr_t) void {
    _ = mtr;
    cursor.rel_pos = BTR_PCUR_ON;
    cursor.stored = true;
}

pub fn btr_pcur_restore_position_func(
    latch_mode: ulint,
    cursor: *btr_pcur_t,
    file: []const u8,
    line: ulint,
    mtr: *mtr_t,
) ibool {
    _ = latch_mode;
    _ = file;
    _ = line;
    _ = mtr;
    cursor.btr_cur.opened = true;
    return if (cursor.stored) compat.TRUE else compat.FALSE;
}

pub fn btr_pcur_release_leaf(cursor: *btr_pcur_t, mtr: *mtr_t) void {
    _ = cursor;
    _ = mtr;
}

pub fn btr_pcur_move_to_next_page(cursor: *btr_pcur_t, mtr: *mtr_t) void {
    _ = mtr;
    cursor.rel_pos = BTR_PCUR_AFTER;
}

pub fn btr_pcur_move_backward_from_page(cursor: *btr_pcur_t, mtr: *mtr_t) void {
    _ = mtr;
    cursor.rel_pos = BTR_PCUR_BEFORE;
}

pub fn btr_search_sys_create(hash_size: ulint) void {
    btr_search_sys_free();
    const sys = std.heap.page_allocator.create(btr_search_sys_t) catch {
        btr_search_sys = null;
        return;
    };
    sys.* = .{ .hash_size = hash_size };
    btr_search_sys = sys;
    btr_search_enabled = 1;
}

pub fn btr_search_sys_free() void {
    if (btr_search_sys) |sys| {
        std.heap.page_allocator.destroy(sys);
        btr_search_sys = null;
    }
}

pub fn btr_search_disable() void {
    btr_search_enabled = 0;
}

pub fn btr_search_enable() void {
    btr_search_enabled = 1;
}

pub fn btr_search_info_create(heap: *mem_heap_t) ?*btr_search_t {
    _ = heap;
    const info = std.heap.page_allocator.create(btr_search_t) catch return null;
    info.* = .{};
    return info;
}

pub fn btr_search_info_get_ref_count(info: *btr_search_t) ulint {
    return info.ref_count;
}

pub fn btr_search_guess_on_hash(
    index: *dict_index_t,
    info: *btr_search_t,
    tuple: *const dtuple_t,
    mode: ulint,
    latch_mode: ulint,
    cursor: *btr_cur_t,
    has_search_latch: ulint,
    mtr: *mtr_t,
) ibool {
    _ = index;
    _ = info;
    _ = tuple;
    _ = mode;
    _ = latch_mode;
    _ = cursor;
    _ = has_search_latch;
    _ = mtr;
    return compat.FALSE;
}

pub fn btr_search_move_or_delete_hash_entries(new_block: *buf_block_t, block: *buf_block_t, index: *dict_index_t) void {
    _ = new_block;
    _ = block;
    _ = index;
}

pub fn btr_search_drop_page_hash_index(block: *buf_block_t) void {
    _ = block;
}

pub fn btr_search_drop_page_hash_when_freed(space: ulint, zip_size: ulint, page_no: ulint) void {
    _ = space;
    _ = zip_size;
    _ = page_no;
}

pub fn btr_search_update_hash_node_on_insert(cursor: *btr_cur_t) void {
    _ = cursor;
}

pub fn btr_search_update_hash_on_insert(cursor: *btr_cur_t) void {
    _ = cursor;
}

pub fn btr_search_update_hash_on_delete(cursor: *btr_cur_t) void {
    _ = cursor;
}

pub fn btr_search_validate() ibool {
    return compat.TRUE;
}

pub fn btr_search_var_init() void {
    btr_search_enabled = 1;
    btr_search_this_is_zero = 0;
}

pub fn btr_search_sys_close() void {
    btr_search_sys_free();
    btr_search_enabled = 0;
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

test "btr cursor open and search stubs" {
    btr_cur_n_non_sea = 5;
    btr_cur_n_sea = 3;
    btr_cur_n_non_sea_old = 2;
    btr_cur_n_sea_old = 1;
    btr_cur_var_init();
    try std.testing.expectEqual(@as(ulint, 0), btr_cur_n_non_sea);
    try std.testing.expectEqual(@as(ulint, 0), btr_cur_n_sea);
    try std.testing.expectEqual(@as(ulint, 0), btr_cur_n_non_sea_old);
    try std.testing.expectEqual(@as(ulint, 0), btr_cur_n_sea_old);

    var index = dict_index_t{};
    var tuple = dtuple_t{};
    var mtr = mtr_t{};
    var cursor = btr_cur_t{};

    btr_cur_search_to_nth_level(&index, 0, &tuple, 0, 0, &cursor, 0, "file", 1, &mtr);
    try std.testing.expect(cursor.index == &index);
    try std.testing.expect(cursor.opened);
    try std.testing.expectEqual(@as(ulint, 1), btr_cur_n_non_sea);

    var cursor2 = btr_cur_t{};
    btr_cur_open_at_index_side_func(compat.TRUE, &index, 0, &cursor2, "file", 2, &mtr);
    try std.testing.expect(cursor2.index == &index);
    try std.testing.expect(cursor2.opened);
}

test "btr page create and empty base records" {
    var page_obj = page.page_t{};
    var block = page.buf_block_t{ .frame = &page_obj, .page_zip = null };
    var index = dict_index_t{};
    index.id = .{ .high = 1, .low = 2 };
    var mtr = mtr_t{};

    btr_page_create(&block, null, &index, 3, &mtr);
    try std.testing.expectEqual(@as(ulint, 3), btr_page_get_level(block.frame, &mtr));
    const idx_id = btr_page_get_index_id(block.frame);
    try std.testing.expectEqual(index.id.high, idx_id.high);
    try std.testing.expectEqual(index.id.low, idx_id.low);
    try std.testing.expectEqual(page.PAGE_HEAP_NO_USER_LOW, block.frame.header.n_heap);
    try std.testing.expectEqual(@as(ulint, 0), block.frame.header.n_recs);
    try std.testing.expect(block.frame.infimum.next == &block.frame.supremum);
    try std.testing.expect(block.frame.supremum.prev == &block.frame.infimum);

    var cursor = page.page_cur_t{};
    page.page_cur_set_before_first(&block, &cursor);
    var offsets: ulint = 0;
    var rec = page.rec_t{ .key = 42 };
    _ = page.page_cur_rec_insert(&cursor, &rec, &index, &offsets, &mtr);
    try std.testing.expectEqual(@as(ulint, 1), block.frame.header.n_recs);
    try std.testing.expect(block.frame.infimum.next == &rec);
    try std.testing.expect(rec.prev == &block.frame.infimum);

    btr_page_empty(&block, null, &index, 0, &mtr);
    try std.testing.expectEqual(@as(ulint, 0), block.frame.header.n_recs);
    try std.testing.expectEqual(page.PAGE_HEAP_NO_USER_LOW, block.frame.header.n_heap);
    try std.testing.expect(block.frame.infimum.next == &block.frame.supremum);
    try std.testing.expect(block.frame.supremum.prev == &block.frame.infimum);
}

test "btr node pointer and father lookup" {
    var parent_page = page.page_t{ .page_no = 10 };
    var child_page = page.page_t{ .page_no = 20 };
    var parent_block = page.buf_block_t{ .frame = &parent_page, .page_zip = null };
    var child_block = page.buf_block_t{ .frame = &child_page, .page_zip = null };
    child_page.parent_block = &parent_block;

    page.page_init(&parent_page);
    page.page_init(&child_page);

    var index = dict_index_t{};
    var mtr = mtr_t{};
    var node_ptr = page.rec_t{};
    btr_node_ptr_set_child_page_no(&node_ptr, child_page.page_no);
    node_ptr.child_block = &child_block;

    var cursor = page.page_cur_t{};
    page.page_cur_set_before_first(&parent_block, &cursor);
    var offsets: ulint = 0;
    _ = page.page_cur_rec_insert(&cursor, &node_ptr, &index, &offsets, &mtr);

    try std.testing.expectEqual(child_page.page_no, btr_node_ptr_get_child_page_no(&node_ptr));
    const child_out = btr_node_ptr_get_child(&node_ptr, &index, &offsets, &mtr);
    try std.testing.expect(child_out == &child_block);

    var btr_cursor = btr_cur_t{ .index = &index };
    _ = btr_page_get_father_block(null, null, btr_cursor.index.?, &child_block, &mtr, &btr_cursor);
    try std.testing.expect(btr_cursor.block == &parent_block);
    try std.testing.expect(btr_cursor.rec == &node_ptr);

    var btr_cursor2 = btr_cur_t{ .index = &index };
    btr_page_get_father(btr_cursor2.index.?, &child_block, &mtr, &btr_cursor2);
    try std.testing.expect(btr_cursor2.block == &parent_block);
    try std.testing.expect(btr_cursor2.rec == &node_ptr);
}

test "btr page alloc free and size" {
    const allocator = std.heap.page_allocator;
    const index = allocator.create(dict_index_t) catch return error.OutOfMemory;
    index.* = .{};
    defer allocator.destroy(index);

    var mtr = mtr_t{};
    const block1 = btr_page_alloc(index, 0, 0, 0, &mtr) orelse return error.OutOfMemory;
    try std.testing.expectEqual(@as(ulint, 1), block1.frame.page_no);
    try std.testing.expectEqual(@as(ulint, 1), btr_get_size(index, 0));

    const block2 = btr_page_alloc(index, 0, 0, 0, &mtr) orelse return error.OutOfMemory;
    try std.testing.expectEqual(@as(ulint, 2), block2.frame.page_no);
    try std.testing.expectEqual(@as(ulint, 2), btr_get_size(index, 0));

    btr_page_free(index, block1, &mtr);
    try std.testing.expectEqual(@as(ulint, 1), btr_get_size(index, 0));

    btr_page_free(index, block2, &mtr);
    try std.testing.expectEqual(@as(ulint, 0), btr_get_size(index, 0));

    index_state_remove(index);
}

test "btr external field prefix copy" {
    var buf = [_]byte{0} ** 5;
    const data = [_]byte{ 'a', 'b', 'c', 'd' };

    const copied = btr_copy_externally_stored_field_prefix(buf[0..].ptr, 5, 0, data[0..].ptr, 3);
    try std.testing.expectEqual(@as(ulint, 3), copied);
    try std.testing.expectEqualStrings("abc", buf[0..3]);
}

test "btr persistent cursor state" {
    const pcur = btr_pcur_create() orelse return error.OutOfMemory;
    defer btr_pcur_free(pcur);

    try std.testing.expect(!pcur.stored);

    var index = dict_index_t{};
    var tuple = dtuple_t{};
    var mtr = mtr_t{};

    btr_pcur_open_on_user_rec_func(&index, &tuple, 0, 0, pcur, "file", 1, &mtr);
    try std.testing.expect(pcur.btr_cur.opened);
    try std.testing.expectEqual(@as(ulint, BTR_PCUR_ON), pcur.rel_pos);

    btr_pcur_store_position(pcur, &mtr);
    try std.testing.expect(pcur.stored);

    const restored = btr_pcur_restore_position_func(0, pcur, "file", 2, &mtr);
    try std.testing.expectEqual(compat.TRUE, restored);

    btr_pcur_move_to_next_page(pcur, &mtr);
    try std.testing.expectEqual(@as(ulint, BTR_PCUR_AFTER), pcur.rel_pos);

    btr_pcur_move_backward_from_page(pcur, &mtr);
    try std.testing.expectEqual(@as(ulint, BTR_PCUR_BEFORE), pcur.rel_pos);

    var other = btr_pcur_t{};
    btr_pcur_copy_stored_position(&other, pcur);
    try std.testing.expectEqual(pcur.rel_pos, other.rel_pos);
    try std.testing.expect(other.stored);
}

test "btr search stubs" {
    btr_search_disable();
    try std.testing.expectEqual(@as(u8, 0), btr_search_enabled);
    btr_search_enable();
    try std.testing.expectEqual(@as(u8, 1), btr_search_enabled);

    btr_search_sys_create(128);
    try std.testing.expect(btr_search_sys != null);
    btr_search_sys_free();
    try std.testing.expect(btr_search_sys == null);

    var heap = mem_heap_t{};
    const info = btr_search_info_create(&heap) orelse return error.OutOfMemory;
    defer std.heap.page_allocator.destroy(info);
    try std.testing.expectEqual(@as(ulint, 0), btr_search_info_get_ref_count(info));

    var index = dict_index_t{};
    var tuple = dtuple_t{};
    var cursor = btr_cur_t{};
    var mtr = mtr_t{};
    try std.testing.expect(btr_search_guess_on_hash(&index, info, &tuple, 0, 0, &cursor, 0, &mtr) == compat.FALSE);

    btr_search_var_init();
    try std.testing.expectEqual(@as(u8, 1), btr_search_enabled);
}
