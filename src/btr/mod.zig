const std = @import("std");
const compat = @import("../ut/compat.zig");
const page = @import("../page/mod.zig");
const dict = @import("../dict/mod.zig");
const data = @import("../data/mod.zig");
const mem = @import("../mem/mod.zig");

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

const BTR_MAX_RECS_PER_PAGE: ulint = 4;

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
pub const upd_t = struct {
    new_key: i64 = 0,
    size_change: bool = false,
};
pub const que_thr_t = struct {};
pub const big_rec_field_t = data.big_rec_field_t;
pub const big_rec_t = data.big_rec_t;
pub const mem_heap_t = mem.mem_heap_t;
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
    stored_key: ?i64 = null,
    stored_block: ?*buf_block_t = null,
};

pub const btr_search_t = struct {
    ref_count: ulint = 0,
};

const HashKey = struct {
    index: *dict_index_t,
    key: i64,
};

const HashEntry = struct {
    rec: *rec_t,
    block: *buf_block_t,
};

pub const btr_search_sys_t = struct {
    hash_size: ulint = 0,
    entries: std.AutoHashMap(HashKey, HashEntry),
};

pub const dtuple_t = page.dtuple_t;

pub fn btr_root_get(index: *dict_index_t, mtr: *mtr_t) ?*page_t {
    const block = btr_root_block_get(index, mtr) orelse return null;
    return block.frame;
}

pub fn btr_root_block_get(index: *dict_index_t, mtr: *mtr_t) ?*buf_block_t {
    _ = mtr;
    const state = index_states.get(index) orelse return null;
    return state.pages.get(index.page);
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

fn page_last_user_rec(page_obj: *page_t) ?*rec_t {
    const last = page_obj.supremum.prev orelse return null;
    return if (last.is_infimum) null else last;
}

fn dtuple_to_key(entry: *const dtuple_t) i64 {
    if (entry.n_fields == 0) {
        return 0;
    }
    const field = entry.fields[0];
    const data_ptr = field.data orelse return 0;
    if (field.len == data.UNIV_SQL_NULL_U32) {
        return 0;
    }
    return switch (field.len) {
        @sizeOf(i32) => @as(i64, @intCast(@as(*const i32, @ptrCast(data_ptr)).*)),
        @sizeOf(u32) => @as(i64, @intCast(@as(*const u32, @ptrCast(data_ptr)).*)),
        @sizeOf(i64) => @as(*const i64, @ptrCast(data_ptr)).*,
        @sizeOf(u64) => @as(i64, @intCast(@as(*const u64, @ptrCast(data_ptr)).*)),
        else => 0,
    };
}

fn dtuple_to_child_block(entry: *const dtuple_t) ?*buf_block_t {
    if (entry.n_fields < 2) {
        return null;
    }
    const field = entry.fields[1];
    const data_ptr = field.data orelse return null;
    if (field.len != @sizeOf(*buf_block_t)) {
        return null;
    }
    return @as(*buf_block_t, @ptrCast(data_ptr));
}

fn page_find_insert_after(page_obj: *page_t, key: i64) *rec_t {
    var prev = &page_obj.infimum;
    var current = page_obj.infimum.next;
    while (current) |node| {
        if (node.is_supremum) {
            break;
        }
        if (key < node.key) {
            break;
        }
        prev = node;
        current = node.next;
    }
    return prev;
}

const SplitEntry = struct {
    key: i64,
    is_new: bool,
};

const SplitResult = struct {
    right: *buf_block_t,
    inserted: *rec_t,
    inserted_block: *buf_block_t,
};

fn insert_entries(block: *buf_block_t, entries: []const SplitEntry, index: *dict_index_t, mtr: *mtr_t) ?*rec_t {
    var cursor = page.page_cur_t{};
    page.page_cur_set_before_first(block, &cursor);
    var offsets: ulint = 0;
    var inserted: ?*rec_t = null;
    const allocator = std.heap.page_allocator;
    for (entries) |entry| {
        const rec = allocator.create(rec_t) catch return inserted;
        rec.* = .{ .key = entry.key };
        if (page.page_cur_rec_insert(&cursor, rec, index, &offsets, mtr) == null) {
            allocator.destroy(rec);
            return inserted;
        }
        cursor.rec = rec;
        if (entry.is_new and inserted == null) {
            inserted = rec;
        }
    }
    return inserted;
}

fn split_leaf_and_insert(index: *dict_index_t, block: *buf_block_t, tuple: *const dtuple_t, mtr: *mtr_t) ?SplitResult {
    const allocator = std.heap.page_allocator;
    var entries = std.ArrayList(SplitEntry).init(allocator);
    defer entries.deinit();

    const new_key = dtuple_to_key(tuple);
    var inserted = false;
    var current = block.frame.infimum.next;
    while (current) |node| {
        if (node.is_supremum) {
            break;
        }
        if (!inserted and new_key < node.key) {
            entries.append(.{ .key = new_key, .is_new = true }) catch return null;
            inserted = true;
        }
        entries.append(.{ .key = node.key, .is_new = false }) catch return null;
        current = node.next;
    }
    if (!inserted) {
        entries.append(.{ .key = new_key, .is_new = true }) catch return null;
    }

    if (entries.items.len < 2) {
        return null;
    }

    var split_index = entries.items.len / 2;
    if (split_index == 0) {
        split_index = 1;
    } else if (split_index >= entries.items.len) {
        split_index = entries.items.len - 1;
    }

    const level = block.frame.header.level;
    const right = btr_page_alloc(index, 0, 0, level, mtr) orelse return null;
    btr_page_create(right, right.page_zip, index, level, mtr);
    right.frame.parent_block = block.frame.parent_block;

    const old_next = block.frame.next_block;
    right.frame.prev_block = block;
    right.frame.next_block = old_next;
    block.frame.next_block = right;
    if (old_next) |next_blk| {
        next_blk.frame.prev_block = right;
    }

    btr_page_empty(block, block.page_zip, index, level, mtr);

    const left_inserted = insert_entries(block, entries.items[0..split_index], index, mtr);
    const right_inserted = insert_entries(right, entries.items[split_index..], index, mtr);

    if (left_inserted) |rec| {
        return .{ .right = right, .inserted = rec, .inserted_block = block };
    }
    if (right_inserted) |rec| {
        return .{ .right = right, .inserted = rec, .inserted_block = right };
    }
    return null;
}

fn insert_node_ptr_with_key(parent_block: *buf_block_t, child_block: *buf_block_t, key: i64, index: *dict_index_t, mtr: *mtr_t) void {
    const insert_after = page_find_insert_after(parent_block.frame, key);
    const allocator = std.heap.page_allocator;
    const node_ptr = allocator.create(rec_t) catch return;
    node_ptr.* = .{ .key = key };
    node_ptr.child_block = child_block;
    btr_node_ptr_set_child_page_no(node_ptr, child_block.frame.page_no);

    var page_cursor = page.page_cur_t{ .block = parent_block, .rec = insert_after };
    var offsets: ulint = 0;
    if (page.page_cur_rec_insert(&page_cursor, node_ptr, index, &offsets, mtr) == null) {
        allocator.destroy(node_ptr);
        return;
    }
    child_block.frame.parent_block = parent_block;
}

fn insert_node_ptr(parent_block: *buf_block_t, child_block: *buf_block_t, index: *dict_index_t, mtr: *mtr_t) void {
    const child_min = page_first_user_rec(child_block.frame);
    const key = if (child_min) |rec| rec.key else 0;
    insert_node_ptr_with_key(parent_block, child_block, key, index, mtr);
}

fn btr_find_rec_by_key(index: *dict_index_t, key: i64) ?*rec_t {
    const block = descend_to_level(index, 0, true) orelse return null;
    var rec_opt = btr_get_next_user_rec(page.page_get_infimum_rec(block.frame), null);
    while (rec_opt) |rec| {
        if (rec.key == key) {
            return rec;
        }
        rec_opt = btr_get_next_user_rec(rec, null);
    }
    return null;
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

fn descend_to_level(index: *dict_index_t, target_level: ulint, from_left: bool) ?*buf_block_t {
    var block = btr_root_block_get(index, null) orelse return null;
    var level = block.frame.header.level;
    while (level > target_level) {
        const node_ptr = if (from_left) page_first_user_rec(block.frame) else page_last_user_rec(block.frame);
        const rec = node_ptr orelse return block;
        const child = rec.child_block orelse return block;
        block = child;
        level = block.frame.header.level;
    }
    return block;
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

fn btr_get_prev_user_rec_raw(rec: ?*rec_t) ?*rec_t {
    const current = rec orelse return null;
    if (!current.is_infimum) {
        const prev = current.prev orelse return null;
        if (!prev.is_infimum) {
            return prev;
        }
    }
    const page_obj = current.page orelse return null;
    const prev_block = page_obj.prev_block orelse return null;
    const prev_page = prev_block.frame;
    const prev_rec = prev_page.supremum.prev orelse return null;
    return if (prev_rec.is_infimum) null else prev_rec;
}

fn btr_get_next_user_rec_raw(rec: ?*rec_t) ?*rec_t {
    const current = rec orelse return null;
    if (!current.is_supremum) {
        const next = current.next orelse return null;
        if (!next.is_supremum) {
            return next;
        }
    }
    const page_obj = current.page orelse return null;
    const next_block = page_obj.next_block orelse return null;
    const next_page = next_block.frame;
    const next_rec = next_page.infimum.next orelse return null;
    return if (next_rec.is_supremum) null else next_rec;
}

pub fn btr_get_prev_user_rec(rec: ?*rec_t, mtr: ?*mtr_t) ?*rec_t {
    _ = mtr;
    var candidate = btr_get_prev_user_rec_raw(rec);
    while (candidate) |cand| {
        if (!cand.deleted) {
            return cand;
        }
        candidate = btr_get_prev_user_rec_raw(cand);
    }
    return null;
}

pub fn btr_get_next_user_rec(rec: ?*rec_t, mtr: ?*mtr_t) ?*rec_t {
    _ = mtr;
    var candidate = btr_get_next_user_rec_raw(rec);
    while (candidate) |cand| {
        if (!cand.deleted) {
            return cand;
        }
        candidate = btr_get_next_user_rec_raw(cand);
    }
    return null;
}

pub fn btr_rec_set_deleted_flag(rec: *rec_t, deleted: ibool) void {
    rec.deleted = deleted != compat.FALSE;
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
    btr_search_drop_page_hash_index(block);
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
    index.space = space;
    index.zip_size = zip_size;
    index.id = index_id;
    const block = btr_page_alloc(index, 0, 0, 0, mtr) orelse return 0;
    btr_page_create(block, block.page_zip, index, 0, mtr);
    index.page = block.frame.page_no;
    index.root_level = 0;
    return index.page;
}

pub fn btr_free_but_not_root(space: ulint, zip_size: ulint, root_page_no: ulint) void {
    _ = space;
    _ = zip_size;
    var it = index_states.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.pages.get(root_page_no) == null) {
            continue;
        }
        const allocator = std.heap.page_allocator;
        var keys = std.ArrayList(ulint).init(allocator);
        defer keys.deinit();
        var page_it = entry.value_ptr.pages.iterator();
        while (page_it.next()) |page_entry| {
            if (page_entry.key_ptr.* != root_page_no) {
                keys.append(page_entry.key_ptr.*) catch {};
            }
        }
        for (keys.items) |page_no| {
            if (entry.value_ptr.pages.fetchRemove(page_no)) |removed| {
                allocator.destroy(removed.value.frame);
                allocator.destroy(removed.value);
            }
        }
        break;
    }
}

pub fn btr_free_root(space: ulint, zip_size: ulint, root_page_no: ulint, mtr: *mtr_t) void {
    _ = space;
    _ = zip_size;
    _ = mtr;
    var remove_index: ?*dict_index_t = null;
    var remove_state: ?*IndexState = null;
    var it = index_states.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.pages.fetchRemove(root_page_no)) |removed| {
            std.heap.page_allocator.destroy(removed.value.frame);
            std.heap.page_allocator.destroy(removed.value);
            entry.key_ptr.*.page = 0;
            entry.key_ptr.*.root_level = 0;
            if (entry.value_ptr.pages.count() == 0) {
                remove_index = entry.key_ptr.*;
                remove_state = entry.value_ptr.*;
            }
            break;
        }
    }
    if (remove_index) |idx| {
        if (remove_state) |state| {
            state.pages.deinit();
            std.heap.page_allocator.destroy(state);
        }
        _ = index_states.remove(idx);
    }
}

fn btr_page_reorganize_low(block: *buf_block_t, index: *dict_index_t, mtr: *mtr_t) ibool {
    var cursor = page.page_cur_t{};
    cursor.block = block;
    cursor.rec = page_first_user_rec(block.frame);
    var offsets: ulint = 0;
    var changed = false;
    while (cursor.rec) |rec| {
        if (rec.is_supremum) {
            break;
        }
        if (rec.deleted) {
            page.page_cur_delete_rec(&cursor, index, &offsets, mtr);
            changed = true;
            continue;
        }
        cursor.rec = rec.next;
    }
    return if (changed) compat.TRUE else compat.FALSE;
}

pub fn btr_page_reorganize(block: *buf_block_t, index: *dict_index_t, mtr: *mtr_t) ibool {
    return btr_page_reorganize_low(block, index, mtr);
}

pub fn btr_parse_page_reorganize(ptr: [*]byte, end_ptr: [*]byte, index: ?*dict_index_t, block: ?*buf_block_t, mtr: ?*mtr_t) [*]byte {
    _ = end_ptr;
    _ = index;
    _ = block;
    _ = mtr;
    return ptr;
}

pub fn btr_root_raise_and_insert(cursor: *btr_cur_t, tuple: *const dtuple_t, n_ext: ulint, mtr: *mtr_t) ?*rec_t {
    _ = n_ext;
    const block = cursor.block orelse return null;
    const index = cursor.index orelse return null;
    const split = split_leaf_and_insert(index, block, tuple, mtr) orelse return null;

    const old_level = block.frame.header.level;
    const new_root = btr_page_alloc(index, 0, 0, old_level + 1, mtr) orelse return null;
    btr_page_create(new_root, new_root.page_zip, index, old_level + 1, mtr);
    index.page = new_root.frame.page_no;
    index.root_level = old_level + 1;

    block.frame.parent_block = new_root;
    split.right.frame.parent_block = new_root;

    insert_node_ptr(new_root, block, index, mtr);
    insert_node_ptr(new_root, split.right, index, mtr);

    cursor.block = split.inserted_block;
    cursor.rec = split.inserted;
    cursor.opened = true;
    return split.inserted;
}

pub fn btr_page_get_split_rec_to_left(cursor: *btr_cur_t, split_rec: *?*rec_t) ibool {
    const block = cursor.block orelse {
        split_rec.* = null;
        return compat.FALSE;
    };
    split_rec.* = page.page_get_middle_rec(block.frame);
    return if (split_rec.* != null) compat.TRUE else compat.FALSE;
}

pub fn btr_page_get_split_rec_to_right(cursor: *btr_cur_t, split_rec: *?*rec_t) ibool {
    const block = cursor.block orelse {
        split_rec.* = null;
        return compat.FALSE;
    };
    split_rec.* = page.page_get_middle_rec(block.frame);
    return if (split_rec.* != null) compat.TRUE else compat.FALSE;
}

pub fn btr_insert_on_non_leaf_level_func(index: *dict_index_t, level: ulint, tuple: *dtuple_t, file: []const u8, line: ulint, mtr: *mtr_t) void {
    if (level == 0) {
        return;
    }
    const child_block = dtuple_to_child_block(tuple) orelse return;
    const key = dtuple_to_key(tuple);
    var cursor = btr_cur_t{};
    btr_cur_search_to_nth_level(index, level, tuple, page.PAGE_CUR_LE, 0, &cursor, 0, file, line, mtr);
    const parent = cursor.block orelse return;
    insert_node_ptr_with_key(parent, child_block, key, index, mtr);
}

pub fn btr_attach_half_pages(
    index: *dict_index_t,
    block: *buf_block_t,
    split_rec: *rec_t,
    new_block: *buf_block_t,
    direction: ulint,
    mtr: *mtr_t,
) void {
    _ = direction;
    const parent = block.frame.parent_block;
    new_block.frame.parent_block = parent;
    new_block.frame.prev_block = block;
    new_block.frame.next_block = block.frame.next_block;
    block.frame.next_block = new_block;
    if (new_block.frame.next_block) |next| {
        next.frame.prev_block = new_block;
    }

    if (parent) |parent_block| {
        insert_node_ptr_with_key(parent_block, new_block, split_rec.key, index, mtr);
        return;
    }

    const old_level = block.frame.header.level;
    const new_root = btr_page_alloc(index, 0, 0, old_level + 1, mtr) orelse return;
    btr_page_create(new_root, new_root.page_zip, index, old_level + 1, mtr);
    index.page = new_root.frame.page_no;
    index.root_level = old_level + 1;
    block.frame.parent_block = new_root;
    new_block.frame.parent_block = new_root;

    const left_min = page_first_user_rec(block.frame);
    const left_key = if (left_min) |rec| rec.key else 0;
    insert_node_ptr_with_key(new_root, block, left_key, index, mtr);
    insert_node_ptr_with_key(new_root, new_block, split_rec.key, index, mtr);
}

pub fn btr_page_split_and_insert(cursor: *btr_cur_t, tuple: *const dtuple_t, n_ext: ulint, mtr: *mtr_t) ?*rec_t {
    const block = cursor.block orelse return null;
    const index = cursor.index orelse return null;
    if (block.frame.parent_block == null) {
        return btr_root_raise_and_insert(cursor, tuple, n_ext, mtr);
    }
    const split = split_leaf_and_insert(index, block, tuple, mtr) orelse return null;
    const parent = block.frame.parent_block orelse return split.inserted;
    insert_node_ptr(parent, split.right, index, mtr);
    cursor.block = split.inserted_block;
    cursor.rec = split.inserted;
    cursor.opened = true;
    return split.inserted;
}

pub fn btr_parse_set_min_rec_mark(ptr: [*]byte, end_ptr: [*]byte, comp: ulint, page_ptr: ?*page_t, mtr: ?*mtr_t) [*]byte {
    _ = end_ptr;
    _ = comp;
    _ = page_ptr;
    _ = mtr;
    return ptr;
}

pub fn btr_set_min_rec_mark(rec: *rec_t, mtr: *mtr_t) void {
    _ = mtr;
    rec.min_rec_mark = true;
}

pub fn btr_node_ptr_delete(index: *dict_index_t, block: *buf_block_t, mtr: *mtr_t) void {
    const parent = block.frame.parent_block orelse return;
    const node_ptr = find_node_ptr(parent.frame, block) orelse return;
    var cursor = page.page_cur_t{ .block = parent, .rec = node_ptr };
    var offsets: ulint = 0;
    page.page_cur_delete_rec(&cursor, index, &offsets, mtr);
    block.frame.parent_block = null;
}

pub fn btr_lift_page_up(index: *dict_index_t, block: *buf_block_t, mtr: *mtr_t) void {
    _ = mtr;
    const parent = block.frame.parent_block orelse return;
    if (block.frame.prev_block != null or block.frame.next_block != null) {
        return;
    }
    index.page = block.frame.page_no;
    index.root_level = block.frame.header.level;
    block.frame.parent_block = null;
    if (index_states.get(index)) |state| {
        if (state.pages.fetchRemove(parent.frame.page_no)) |removed| {
            std.heap.page_allocator.destroy(removed.value.frame);
            std.heap.page_allocator.destroy(removed.value);
        }
    }
}

pub fn btr_compress(cursor: *btr_cur_t, mtr: *mtr_t) ibool {
    const block = cursor.block orelse return compat.FALSE;
    const index = cursor.index orelse return compat.FALSE;
    return btr_page_reorganize(block, index, mtr);
}

pub fn btr_discard_page(cursor: *btr_cur_t, mtr: *mtr_t) void {
    const block = cursor.block orelse return;
    const index = cursor.index orelse return;
    if (block.frame.parent_block == null) {
        return;
    }
    if (block.frame.header.n_recs != 0) {
        return;
    }
    btr_level_list_remove(block, mtr);
    btr_node_ptr_delete(index, block, mtr);
    if (index_states.get(index)) |state| {
        if (state.pages.fetchRemove(block.frame.page_no)) |removed| {
            std.heap.page_allocator.destroy(removed.value.frame);
            std.heap.page_allocator.destroy(removed.value);
        }
    }
}

pub fn btr_discard_only_page_on_level(index: *dict_index_t, block: *buf_block_t, mtr: *mtr_t) void {
    if (block.frame.prev_block != null or block.frame.next_block != null) {
        return;
    }
    if (block.frame.parent_block != null) {
        btr_lift_page_up(index, block, mtr);
    }
}

pub fn btr_level_list_remove(block: *buf_block_t, mtr: *mtr_t) void {
    _ = mtr;
    const prev = block.frame.prev_block;
    const next = block.frame.next_block;
    if (prev) |prev_block| {
        prev_block.frame.next_block = next;
    }
    if (next) |next_block| {
        next_block.frame.prev_block = prev;
    }
    block.frame.prev_block = null;
    block.frame.next_block = null;
}

pub fn btr_print_size(index: *dict_index_t) void {
    _ = index;
}

pub fn btr_print_index(index: *dict_index_t, width: ulint) void {
    _ = index;
    _ = width;
}

fn btr_validate_page_recs(page_obj: *page_t) bool {
    var count: ulint = 0;
    var last_key: ?i64 = null;
    var current = page_obj.infimum.next;
    while (current) |rec| {
        if (rec.is_supremum) {
            break;
        }
        if (rec.page != page_obj) {
            return false;
        }
        if (last_key) |prev_key| {
            if (rec.key < prev_key) {
                return false;
            }
        }
        last_key = rec.key;
        count += 1;
        current = rec.next;
    }
    return count == page_obj.header.n_recs;
}

fn btr_validate_leaf_chain(start: *buf_block_t) bool {
    var block_opt: ?*buf_block_t = start;
    var prev_block: ?*buf_block_t = null;
    var last_key: ?i64 = null;
    while (block_opt) |block| {
        if (block.frame.prev_block != prev_block) {
            return false;
        }
        var rec_opt = btr_get_next_user_rec(page.page_get_infimum_rec(block.frame), null);
        while (rec_opt) |rec| {
            if (last_key) |prev_key| {
                if (rec.key < prev_key) {
                    return false;
                }
            }
            last_key = rec.key;
            rec_opt = btr_get_next_user_rec(rec, null);
        }
        prev_block = block;
        block_opt = block.frame.next_block;
        if (block_opt) |next_block| {
            if (next_block.frame.prev_block != block) {
                return false;
            }
        }
    }
    return true;
}

fn btr_validate_subtree(index: *dict_index_t, block: *buf_block_t) bool {
    if (!btr_validate_page_recs(block.frame)) {
        return false;
    }
    if (block.frame.header.level == 0) {
        return true;
    }
    var rec_opt = btr_get_next_user_rec(page.page_get_infimum_rec(block.frame), null);
    while (rec_opt) |rec| {
        const child = rec.child_block orelse return false;
        if (child.frame.parent_block != block) {
            return false;
        }
        if (child.frame.header.level + 1 != block.frame.header.level) {
            return false;
        }
        var mtr = mtr_t{};
        if (btr_check_node_ptr(index, child, &mtr) == compat.FALSE) {
            return false;
        }
        if (!btr_validate_subtree(index, child)) {
            return false;
        }
        rec_opt = btr_get_next_user_rec(rec, null);
    }
    return true;
}

fn btr_find_leaf_for_key(index: *dict_index_t, key: i64) ?*buf_block_t {
    var block = descend_to_level(index, 0, true) orelse return null;
    while (true) {
        const last = page_last_user_rec(block.frame);
        if (last == null or key <= last.?.key) {
            return block;
        }
        block = block.frame.next_block orelse return block;
    }
}

fn btr_block_for_rec(index: *dict_index_t, rec: *rec_t) ?*buf_block_t {
    const page_ptr = rec.page orelse return null;
    const state = index_states.get(index) orelse return null;
    return state.pages.get(page_ptr.page_no);
}

pub fn btr_check_node_ptr(index: *dict_index_t, block: *buf_block_t, mtr: *mtr_t) ibool {
    _ = index;
    _ = mtr;
    const parent = block.frame.parent_block orelse return compat.TRUE;
    const node_ptr = find_node_ptr(parent.frame, block) orelse return compat.FALSE;
    const child_min = page_first_user_rec(block.frame) orelse return compat.TRUE;
    if (node_ptr.key != child_min.key) {
        return compat.FALSE;
    }
    return compat.TRUE;
}

pub fn btr_index_rec_validate(rec: *const rec_t, index: *const dict_index_t, dump_on_error: ibool) ibool {
    _ = rec;
    _ = index;
    _ = dump_on_error;
    return compat.TRUE;
}

pub fn btr_validate_index(index: *dict_index_t, trx: ?*trx_t) ibool {
    _ = trx;
    const root_block = btr_root_block_get(index, null) orelse return compat.TRUE;
    if (root_block.frame.page_no != index.page) {
        return compat.FALSE;
    }
    if (root_block.frame.header.level != index.root_level) {
        return compat.FALSE;
    }
    if (!btr_validate_subtree(index, root_block)) {
        return compat.FALSE;
    }
    const leftmost = descend_to_level(index, 0, true) orelse return compat.TRUE;
    return if (btr_validate_leaf_chain(leftmost)) compat.TRUE else compat.FALSE;
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
    _ = latch_mode;
    _ = has_search_latch;
    _ = file;
    _ = line;
    _ = mtr;
    cursor.index = index;
    cursor.opened = true;
    btr_cur_n_non_sea += 1;
    cursor.rec = null;
    cursor.block = null;

    const block = descend_to_level(index, level, true) orelse return;
    var page_cursor = page.page_cur_t{};
    _ = page.page_cur_search(block, index, tuple, mode, &page_cursor);
    cursor.block = page_cursor.block;
    cursor.rec = page_cursor.rec;
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
    _ = latch_mode;
    _ = file;
    _ = line;
    _ = mtr;
    cursor.index = index;
    cursor.opened = true;
    cursor.rec = null;
    cursor.block = null;

    const open_left = from_left != compat.FALSE;
    const block = descend_to_level(index, 0, open_left) orelse return;
    cursor.block = block;
    cursor.rec = if (open_left)
        page.page_get_infimum_rec(block.frame)
    else
        page.page_get_supremum_rec(block.frame);
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
    cursor.index = index;
    cursor.opened = true;
    cursor.rec = null;
    cursor.block = null;

    var block = btr_root_block_get(index, mtr) orelse return;
    var level = block.frame.header.level;
    while (level > 0) {
        var page_cursor = page.page_cur_t{};
        page.page_cur_open_on_rnd_user_rec(block, &page_cursor);
        const chosen = page_cursor.rec orelse break;
        if (chosen.is_infimum or chosen.is_supremum) {
            break;
        }
        const child = chosen.child_block orelse break;
        block = child;
        level = block.frame.header.level;
    }

    var leaf_cursor = page.page_cur_t{};
    page.page_cur_open_on_rnd_user_rec(block, &leaf_cursor);
    cursor.block = leaf_cursor.block;
    cursor.rec = leaf_cursor.rec;
}

fn btr_cur_insert_if_possible(cursor: *btr_cur_t, tuple: *const dtuple_t, n_ext: ulint, mtr: *mtr_t) ?*rec_t {
    _ = n_ext;
    const block = cursor.block orelse return null;
    const index = cursor.index orelse return null;
    if (block.frame.header.n_recs >= BTR_MAX_RECS_PER_PAGE) {
        return null;
    }
    const key = dtuple_to_key(tuple);
    const insert_after = page_find_insert_after(block.frame, key);
    const allocator = std.heap.page_allocator;
    const new_rec = allocator.create(rec_t) catch return null;
    new_rec.* = .{ .key = key };
    var page_cursor = page.page_cur_t{ .block = block, .rec = insert_after };
    var offsets: ulint = 0;
    if (page.page_cur_rec_insert(&page_cursor, new_rec, index, &offsets, mtr) == null) {
        allocator.destroy(new_rec);
        return null;
    }
    cursor.rec = new_rec;
    cursor.block = block;
    cursor.opened = true;
    return new_rec;
}

fn btr_cur_ins_lock_and_undo(
    flags: ulint,
    cursor: *btr_cur_t,
    entry: *const dtuple_t,
    thr: ?*que_thr_t,
    mtr: *mtr_t,
    inherit: *ibool,
) ulint {
    _ = flags;
    _ = cursor;
    _ = entry;
    _ = thr;
    _ = mtr;
    inherit.* = compat.FALSE;
    return 0;
}

fn btr_cur_trx_report(trx: ?*trx_t, index: *const dict_index_t, op: []const u8) void {
    _ = trx;
    _ = index;
    _ = op;
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
    big_rec.* = null;
    rec.* = null;

    var inherit: ibool = compat.FALSE;
    const lock_err = btr_cur_ins_lock_and_undo(flags, cursor, entry, thr, mtr, &inherit);
    if (lock_err != 0) {
        return lock_err;
    }

    if (btr_cur_insert_if_possible(cursor, entry, n_ext, mtr)) |inserted| {
        rec.* = inserted;
        btr_search_update_hash_on_insert(cursor);
        return 0;
    }

    const split_rec = btr_page_split_and_insert(cursor, entry, n_ext, mtr) orelse return 1;
    rec.* = split_rec;
    btr_search_update_hash_on_insert(cursor);
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

pub fn btr_cur_update_alloc_zip(
    page_zip: ?*page_zip_des_t,
    block: *buf_block_t,
    index: *dict_index_t,
    update: *const upd_t,
    mtr: *mtr_t,
) ibool {
    _ = page_zip;
    _ = block;
    _ = index;
    _ = update;
    _ = mtr;
    return compat.FALSE;
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
    _ = cmpl_info;
    _ = thr;
    _ = mtr;
    const rec = cursor.rec orelse return 1;
    if (rec.is_infimum or rec.is_supremum) {
        return 1;
    }
    if (update.size_change) {
        return 1;
    }
    if (rec.key != update.new_key) {
        btr_search_update_hash_on_delete(cursor);
        rec.key = update.new_key;
        btr_search_update_hash_on_insert(cursor);
        return 0;
    }
    rec.key = update.new_key;
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
    if (update.size_change) {
        return 1;
    }
    return btr_cur_update_in_place(flags, cursor, update, cmpl_info, thr, mtr);
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
    heap.* = null;
    big_rec.* = null;
    if (!update.size_change) {
        return btr_cur_update_in_place(flags, cursor, update, cmpl_info, thr, mtr);
    }

    const index = cursor.index orelse return 1;
    const block = cursor.block orelse return 1;
    const rec = cursor.rec orelse return 1;
    if (rec.is_infimum or rec.is_supremum) {
        return 1;
    }

    var page_cursor = page.page_cur_t{ .block = block, .rec = rec };
    var offsets: ulint = 0;
    page.page_cur_delete_rec(&page_cursor, index, &offsets, mtr);
    cursor.rec = page_cursor.rec;

    var new_key = update.new_key;
    var fields = [_]data.dfield_t{.{ .data = &new_key, .len = @intCast(@sizeOf(i64)) }};
    var tuple = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = fields[0..] };
    var rec_out: ?*rec_t = null;
    var big_out: ?*big_rec_t = null;
    if (btr_cur_optimistic_insert(0, cursor, &tuple, &rec_out, &big_out, 0, null, mtr) != 0 or rec_out == null) {
        return 1;
    }
    cursor.rec = rec_out;
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
    _ = thr;
    _ = mtr;
    const rec = cursor.rec orelse return 1;
    btr_rec_set_deleted_flag(rec, val);
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
    _ = thr;
    _ = mtr;
    const rec = cursor.rec orelse return 1;
    btr_rec_set_deleted_flag(rec, val);
    return 0;
}

pub fn btr_cur_del_unmark_for_ibuf(rec: *rec_t, page_zip: ?*page_zip_des_t, mtr: *mtr_t) void {
    _ = page_zip;
    _ = mtr;
    btr_rec_set_deleted_flag(rec, compat.FALSE);
}

pub fn btr_cur_compress_if_useful(cursor: *btr_cur_t, mtr: *mtr_t) ibool {
    _ = cursor;
    _ = mtr;
    return compat.FALSE;
}

pub fn btr_cur_optimistic_delete(cursor: *btr_cur_t, mtr: *mtr_t) ibool {
    const block = cursor.block orelse return compat.FALSE;
    const rec = cursor.rec orelse return compat.FALSE;
    if (rec.is_infimum or rec.is_supremum) {
        return compat.FALSE;
    }
    const index = cursor.index orelse return compat.FALSE;
    btr_search_update_hash_on_delete(cursor);
    var page_cursor = page.page_cur_t{ .block = block, .rec = rec };
    var offsets: ulint = 0;
    page.page_cur_delete_rec(&page_cursor, index, &offsets, mtr);
    cursor.rec = page_cursor.rec;
    cursor.block = block;
    return compat.TRUE;
}

pub fn btr_cur_pessimistic_delete(err: *ulint, has_reserved_extents: ibool, cursor: *btr_cur_t, rb_ctx: trx_rb_ctx, mtr: *mtr_t) ibool {
    _ = has_reserved_extents;
    _ = rb_ctx;
    const ok = btr_cur_optimistic_delete(cursor, mtr);
    err.* = if (ok == compat.TRUE) 0 else 1;
    return ok;
}

pub fn btr_estimate_n_rows_in_range(
    index: *dict_index_t,
    tuple1: *const dtuple_t,
    mode1: ulint,
    tuple2: *const dtuple_t,
    mode2: ulint,
) ib_int64_t {
    _ = mode1;
    _ = mode2;
    const key1 = dtuple_to_key(tuple1);
    const key2 = dtuple_to_key(tuple2);
    const min_key = if (key1 <= key2) key1 else key2;
    const max_key = if (key1 <= key2) key2 else key1;

    const block = descend_to_level(index, 0, true) orelse return 0;
    var count: ib_int64_t = 0;
    var rec_opt = btr_get_next_user_rec(page.page_get_infimum_rec(block.frame), null);
    while (rec_opt) |rec| {
        if (rec.key >= min_key and rec.key <= max_key) {
            count += 1;
        }
        rec_opt = btr_get_next_user_rec(rec, null);
    }
    return count;
}

pub fn btr_estimate_number_of_different_key_vals(index: *dict_index_t) ib_int64_t {
    const block = descend_to_level(index, 0, true) orelse return 0;
    var count: ib_int64_t = 0;
    var last_key: ?i64 = null;
    var rec_opt = btr_get_next_user_rec(page.page_get_infimum_rec(block.frame), null);
    while (rec_opt) |rec| {
        if (last_key == null or rec.key != last_key.?) {
            count += 1;
            last_key = rec.key;
        }
        rec_opt = btr_get_next_user_rec(rec, null);
    }
    return count;
}

fn rec_clear_extern_fields(rec: *rec_t) void {
    const allocator = std.heap.page_allocator;
    for (rec.extern_fields) |field| {
        if (field.data.len > 0) {
            allocator.free(field.data);
        }
    }
    if (rec.extern_fields.len > 0) {
        allocator.free(rec.extern_fields);
    }
    rec.extern_fields = &[_]page.extern_field_t{};
}

fn rec_find_extern_field_index(rec: *const rec_t, field_no: ulint) ?usize {
    for (rec.extern_fields, 0..) |field, idx| {
        if (field.field_no == field_no) {
            return idx;
        }
    }
    return null;
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
    _ = offsets;
    _ = local_mtr;
    if (big_rec_vec.n_fields == 0) {
        return 0;
    }

    rec_clear_extern_fields(rec);

    const allocator = std.heap.page_allocator;
    const field_count = @as(usize, @intCast(big_rec_vec.n_fields));
    var valid_count: usize = 0;
    for (big_rec_vec.fields[0..field_count]) |field| {
        if (field.len == 0 or field.data == null) {
            continue;
        }
        valid_count += 1;
    }

    if (valid_count == 0) {
        return 0;
    }

    var fields = allocator.alloc(page.extern_field_t, valid_count) catch return 1;
    var stored: usize = 0;

    for (big_rec_vec.fields[0..field_count]) |field| {
        if (field.len == 0 or field.data == null) {
            continue;
        }
        const len = @as(usize, @intCast(field.len));
        const src = @as([*]const u8, @ptrCast(field.data.?))[0..len];
        const buf = allocator.alloc(u8, len) catch {
            for (fields[0..stored]) |stored_field| {
                if (stored_field.data.len > 0) {
                    allocator.free(stored_field.data);
                }
            }
            allocator.free(fields);
            return 1;
        };
        std.mem.copyForwards(u8, buf, src);
        fields[stored] = .{ .field_no = field.field_no, .data = buf };
        stored += 1;
    }

    rec.extern_fields = fields[0..stored];
    if (rec_block.page_zip) |zip| {
        zip.n_blobs = @as(u16, @intCast(rec.extern_fields.len));
    }
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
    _ = offsets;
    _ = page_zip;
    _ = rb_ctx;
    _ = local_mtr;
    const rec_ptr = rec orelse return;
    const idx = rec_find_extern_field_index(rec_ptr, i) orelse return;
    const allocator = std.heap.page_allocator;
    var fields = rec_ptr.extern_fields;
    if (fields[idx].data.len > 0) {
        allocator.free(fields[idx].data);
    }
    const last = fields.len - 1;
    if (idx != last) {
        fields[idx] = fields[last];
    }
    if (last == 0) {
        allocator.free(fields);
        @constCast(rec_ptr).extern_fields = &[_]page.extern_field_t{};
    } else {
        @constCast(rec_ptr).extern_fields = fields[0..last];
    }
}

pub fn btr_copy_externally_stored_field_prefix(
    buf: [*]byte,
    len: ulint,
    zip_size: ulint,
    data_ptr: [*]const byte,
    local_len: ulint,
) ulint {
    _ = zip_size;
    const max = @min(len, local_len);
    const count = @as(usize, @intCast(max));
    if (count > 0) {
        std.mem.copyForwards(byte, buf[0..count], data_ptr[0..count]);
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
    _ = offsets;
    _ = zip_size;
    const idx = rec_find_extern_field_index(rec, no) orelse {
        len.* = 0;
        return null;
    };
    const field = rec.extern_fields[idx];
    if (field.data.len == 0) {
        len.* = 0;
        return null;
    }
    const buf = mem.mem_heap_alloc(heap, @as(ulint, @intCast(field.data.len))) orelse {
        len.* = 0;
        return null;
    };
    std.mem.copyForwards(u8, buf, field.data);
    len.* = @as(ulint, @intCast(field.data.len));
    return buf.ptr;
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
    pcur_receive.stored_key = pcur_donate.stored_key;
    pcur_receive.stored_block = pcur_donate.stored_block;
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
    btr_cur_search_to_nth_level(index, 0, tuple, mode, latch_mode, &cursor.btr_cur, 0, file, line, mtr);
    cursor.rel_pos = BTR_PCUR_ON;
    cursor.stored = false;
    cursor.stored_key = null;
    cursor.stored_block = null;
}

pub fn btr_pcur_store_position(cursor: *btr_pcur_t, mtr: *mtr_t) void {
    _ = mtr;
    cursor.rel_pos = BTR_PCUR_ON;
    cursor.stored = true;
    cursor.stored_block = cursor.btr_cur.block;
    if (cursor.btr_cur.rec) |rec| {
        if (rec.is_infimum or rec.is_supremum) {
            cursor.stored_key = null;
        } else {
            cursor.stored_key = rec.key;
        }
    } else {
        cursor.stored_key = null;
    }
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
    if (!cursor.stored) {
        return compat.FALSE;
    }
    const index = cursor.btr_cur.index orelse return compat.FALSE;
    const key = cursor.stored_key orelse return compat.FALSE;
    const rec = btr_find_rec_by_key(index, key) orelse {
        cursor.btr_cur.rec = null;
        cursor.btr_cur.block = null;
        cursor.rel_pos = BTR_PCUR_BEFORE;
        return compat.FALSE;
    };
    cursor.btr_cur.rec = rec;
    cursor.btr_cur.block = rec.page;
    cursor.btr_cur.opened = true;
    cursor.rel_pos = BTR_PCUR_ON;
    return compat.TRUE;
}

pub fn btr_pcur_release_leaf(cursor: *btr_pcur_t, mtr: *mtr_t) void {
    _ = cursor;
    _ = mtr;
}

pub fn btr_pcur_move_to_next_page(cursor: *btr_pcur_t, mtr: *mtr_t) void {
    _ = mtr;
    const block = cursor.btr_cur.block orelse {
        cursor.rel_pos = BTR_PCUR_AFTER;
        return;
    };
    const next_block = block.frame.next_block orelse {
        cursor.rel_pos = BTR_PCUR_AFTER;
        return;
    };
    cursor.btr_cur.block = next_block;
    cursor.btr_cur.rec = page.page_get_infimum_rec(next_block.frame);
    cursor.rel_pos = BTR_PCUR_AFTER;
}

pub fn btr_pcur_move_backward_from_page(cursor: *btr_pcur_t, mtr: *mtr_t) void {
    _ = mtr;
    const block = cursor.btr_cur.block orelse {
        cursor.rel_pos = BTR_PCUR_BEFORE;
        return;
    };
    const prev_block = block.frame.prev_block orelse {
        cursor.rel_pos = BTR_PCUR_BEFORE;
        return;
    };
    cursor.btr_cur.block = prev_block;
    cursor.btr_cur.rec = page.page_get_supremum_rec(prev_block.frame);
    cursor.rel_pos = BTR_PCUR_BEFORE;
}

pub fn btr_search_sys_create(hash_size: ulint) void {
    btr_search_sys_free();
    const sys = std.heap.page_allocator.create(btr_search_sys_t) catch {
        btr_search_sys = null;
        return;
    };
    sys.* = .{
        .hash_size = hash_size,
        .entries = std.AutoHashMap(HashKey, HashEntry).init(std.heap.page_allocator),
    };
    btr_search_sys = sys;
    btr_search_enabled = 1;
}

pub fn btr_search_sys_free() void {
    if (btr_search_sys) |sys| {
        sys.entries.deinit();
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

fn btr_search_put(index: *dict_index_t, block: *buf_block_t, rec: *rec_t) void {
    if (btr_search_enabled == 0) {
        return;
    }
    const sys = btr_search_sys orelse return;
    if (rec.is_infimum or rec.is_supremum or rec.deleted) {
        return;
    }
    _ = sys.entries.put(.{ .index = index, .key = rec.key }, .{ .rec = rec, .block = block }) catch {};
}

fn btr_search_remove(index: *dict_index_t, block: ?*buf_block_t, key: i64) void {
    if (btr_search_enabled == 0) {
        return;
    }
    const sys = btr_search_sys orelse return;
    const hash_key = HashKey{ .index = index, .key = key };
    if (block) |blk| {
        if (sys.entries.getEntry(hash_key)) |entry| {
            if (entry.value_ptr.block == blk) {
                _ = sys.entries.remove(hash_key);
            }
        }
        return;
    }
    _ = sys.entries.remove(hash_key);
}

pub fn btr_search_build_page_hash_index(index: *dict_index_t, block: *buf_block_t) void {
    if (btr_search_enabled == 0) {
        return;
    }
    _ = btr_search_sys orelse return;
    var rec_opt = btr_get_next_user_rec(page.page_get_infimum_rec(block.frame), null);
    while (rec_opt) |rec| {
        btr_search_put(index, block, rec);
        rec_opt = btr_get_next_user_rec(rec, null);
    }
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
    _ = info;
    _ = mode;
    _ = latch_mode;
    _ = has_search_latch;
    _ = mtr;
    if (btr_search_enabled == 0) {
        return compat.FALSE;
    }
    const sys = btr_search_sys orelse return compat.FALSE;
    const key = dtuple_to_key(tuple);
    const entry = sys.entries.getEntry(.{ .index = index, .key = key }) orelse return compat.FALSE;
    const rec = entry.value_ptr.rec;
    if (rec.deleted or rec.is_infimum or rec.is_supremum) {
        _ = sys.entries.remove(entry.key_ptr.*);
        return compat.FALSE;
    }
    cursor.index = index;
    cursor.block = entry.value_ptr.block;
    cursor.rec = rec;
    cursor.opened = true;
    btr_cur_n_sea += 1;
    return compat.TRUE;
}

pub fn btr_search_move_or_delete_hash_entries(new_block: *buf_block_t, block: *buf_block_t, index: *dict_index_t) void {
    _ = index;
    btr_search_drop_page_hash_index(block);
    btr_search_drop_page_hash_index(new_block);
}

pub fn btr_search_drop_page_hash_index(block: *buf_block_t) void {
    if (btr_search_enabled == 0) {
        return;
    }
    const sys = btr_search_sys orelse return;
    var remove_keys = std.ArrayList(HashKey).init(std.heap.page_allocator);
    defer remove_keys.deinit();
    var it = sys.entries.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.block == block) {
            remove_keys.append(entry.key_ptr.*) catch {};
        }
    }
    for (remove_keys.items) |key| {
        _ = sys.entries.remove(key);
    }
}

pub fn btr_search_drop_page_hash_when_freed(space: ulint, zip_size: ulint, page_no: ulint) void {
    _ = space;
    _ = zip_size;
    if (btr_search_enabled == 0) {
        return;
    }
    const sys = btr_search_sys orelse return;
    var remove_keys = std.ArrayList(HashKey).init(std.heap.page_allocator);
    defer remove_keys.deinit();
    var it = sys.entries.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.block.frame.page_no == page_no) {
            remove_keys.append(entry.key_ptr.*) catch {};
        }
    }
    for (remove_keys.items) |key| {
        _ = sys.entries.remove(key);
    }
}

pub fn btr_search_update_hash_node_on_insert(cursor: *btr_cur_t) void {
    btr_search_update_hash_on_insert(cursor);
}

pub fn btr_search_update_hash_on_insert(cursor: *btr_cur_t) void {
    const rec = cursor.rec orelse return;
    const block = cursor.block orelse return;
    const index = cursor.index orelse return;
    btr_search_put(index, block, rec);
}

pub fn btr_search_update_hash_on_delete(cursor: *btr_cur_t) void {
    const rec = cursor.rec orelse return;
    const block = cursor.block orelse return;
    const index = cursor.index orelse return;
    btr_search_remove(index, block, rec.key);
}

pub fn btr_search_validate() ibool {
    const sys = btr_search_sys orelse return compat.TRUE;
    var it = sys.entries.iterator();
    while (it.next()) |entry| {
        const rec = entry.value_ptr.rec;
        if (rec.deleted) {
            return compat.FALSE;
        }
        if (rec.page) |page_ptr| {
            if (page_ptr != entry.value_ptr.block.frame) {
                return compat.FALSE;
            }
        }
    }
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

test "btr user record navigation across pages" {
    var page1 = page.page_t{};
    var page2 = page.page_t{};
    var block1 = page.buf_block_t{ .frame = &page1, .page_zip = null };
    var block2 = page.buf_block_t{ .frame = &page2, .page_zip = null };

    page1.next_block = &block2;
    page2.prev_block = &block1;
    page.page_init(&page1);
    page.page_init(&page2);
    page1.next_block = &block2;
    page2.prev_block = &block1;

    var index = dict_index_t{};
    var mtr = mtr_t{};
    var offsets: ulint = 0;
    var cursor = page.page_cur_t{};

    page.page_cur_set_before_first(&block1, &cursor);
    var rec1 = page.rec_t{ .key = 1 };
    _ = page.page_cur_rec_insert(&cursor, &rec1, &index, &offsets, &mtr);
    cursor.rec = &rec1;
    var rec2 = page.rec_t{ .key = 2 };
    _ = page.page_cur_rec_insert(&cursor, &rec2, &index, &offsets, &mtr);

    page.page_cur_set_before_first(&block2, &cursor);
    var rec3 = page.rec_t{ .key = 3 };
    _ = page.page_cur_rec_insert(&cursor, &rec3, &index, &offsets, &mtr);

    try std.testing.expect(btr_get_next_user_rec(&rec2, null) == &rec3);
    try std.testing.expect(btr_get_prev_user_rec(&rec3, null) == &rec2);
    try std.testing.expect(btr_get_next_user_rec(&page1.supremum, null) == &rec3);
    try std.testing.expect(btr_get_prev_user_rec(&page2.infimum, null) == &rec2);
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

test "btr cursor counters reset" {
    btr_cur_n_non_sea = 5;
    btr_cur_n_sea = 3;
    btr_cur_n_non_sea_old = 2;
    btr_cur_n_sea_old = 1;
    btr_cur_var_init();
    try std.testing.expectEqual(@as(ulint, 0), btr_cur_n_non_sea);
    try std.testing.expectEqual(@as(ulint, 0), btr_cur_n_sea);
    try std.testing.expectEqual(@as(ulint, 0), btr_cur_n_non_sea_old);
    try std.testing.expectEqual(@as(ulint, 0), btr_cur_n_sea_old);
}

test "btr cursor search and open at index sides" {
    const allocator = std.heap.page_allocator;
    const index = allocator.create(dict_index_t) catch return error.OutOfMemory;
    index.* = .{};
    defer allocator.destroy(index);

    var mtr = mtr_t{};
    _ = btr_create(0, 1, 0, .{ .high = 0, .low = 200 }, index, &mtr);
    const root_block = btr_root_block_get(index, &mtr) orelse return error.OutOfMemory;
    root_block.frame.header.level = 1;
    index.root_level = 1;

    const left = btr_page_alloc(index, 0, 0, 0, &mtr) orelse return error.OutOfMemory;
    const right = btr_page_alloc(index, 0, 0, 0, &mtr) orelse return error.OutOfMemory;
    left.frame.parent_block = root_block;
    right.frame.parent_block = root_block;

    var offsets: ulint = 0;
    var root_cursor = page.page_cur_t{};
    page.page_cur_set_before_first(root_block, &root_cursor);

    var left_ptr = page.rec_t{};
    btr_node_ptr_set_child_page_no(&left_ptr, left.frame.page_no);
    left_ptr.child_block = left;
    _ = page.page_cur_rec_insert(&root_cursor, &left_ptr, index, &offsets, &mtr);

    root_cursor.rec = &left_ptr;
    var right_ptr = page.rec_t{};
    btr_node_ptr_set_child_page_no(&right_ptr, right.frame.page_no);
    right_ptr.child_block = right;
    _ = page.page_cur_rec_insert(&root_cursor, &right_ptr, index, &offsets, &mtr);

    var leaf_cursor = page.page_cur_t{};
    page.page_cur_set_before_first(left, &leaf_cursor);
    var lrec = page.rec_t{ .key = 10 };
    _ = page.page_cur_rec_insert(&leaf_cursor, &lrec, index, &offsets, &mtr);

    page.page_cur_set_before_first(right, &leaf_cursor);
    var rrec1 = page.rec_t{ .key = 20 };
    _ = page.page_cur_rec_insert(&leaf_cursor, &rrec1, index, &offsets, &mtr);
    leaf_cursor.rec = &rrec1;
    var rrec2 = page.rec_t{ .key = 30 };
    _ = page.page_cur_rec_insert(&leaf_cursor, &rrec2, index, &offsets, &mtr);

    btr_cur_n_non_sea = 0;
    var tuple = dtuple_t{};
    var cursor = btr_cur_t{};
    btr_cur_search_to_nth_level(index, 0, &tuple, 0, 0, &cursor, 0, "file", 1, &mtr);
    try std.testing.expectEqual(@as(ulint, 1), btr_cur_n_non_sea);
    try std.testing.expect(cursor.block != null);
    try std.testing.expect(cursor.rec != null);
    try std.testing.expect(cursor.block.? == left);
    try std.testing.expect(cursor.rec.? == &lrec);

    var left_cur = btr_cur_t{};
    btr_cur_open_at_index_side_func(compat.TRUE, index, 0, &left_cur, "file", 2, &mtr);
    try std.testing.expect(left_cur.block != null);
    try std.testing.expect(left_cur.rec != null);
    try std.testing.expect(left_cur.block.? == left);
    try std.testing.expect(left_cur.rec.? == page.page_get_infimum_rec(left.frame));
    try std.testing.expect(btr_get_next_user_rec(left_cur.rec, null) == &lrec);

    var right_cur = btr_cur_t{};
    btr_cur_open_at_index_side_func(compat.FALSE, index, 0, &right_cur, "file", 3, &mtr);
    try std.testing.expect(right_cur.block != null);
    try std.testing.expect(right_cur.rec != null);
    try std.testing.expect(right_cur.block.? == right);
    try std.testing.expect(right_cur.rec.? == page.page_get_supremum_rec(right.frame));
    try std.testing.expect(btr_get_prev_user_rec(right_cur.rec, null) == &rrec2);

    var rnd_cur = btr_cur_t{};
    btr_cur_open_at_rnd_pos_func(index, 0, &rnd_cur, "file", 4, &mtr);
    try std.testing.expect(rnd_cur.block != null);
    try std.testing.expect(rnd_cur.rec != null);
    const rnd_block = rnd_cur.block.?;
    const rnd_rec = rnd_cur.rec.?;
    try std.testing.expect(rnd_block == left or rnd_block == right);
    try std.testing.expect(!rnd_rec.is_infimum and !rnd_rec.is_supremum);

    index_state_remove(index);
}

test "btr optimistic insert leaf order and duplicates" {
    const allocator = std.heap.page_allocator;
    const index = allocator.create(dict_index_t) catch return error.OutOfMemory;
    index.* = .{};
    defer allocator.destroy(index);

    var mtr = mtr_t{};
    _ = btr_create(0, 1, 0, .{ .high = 0, .low = 300 }, index, &mtr);
    const root_block = btr_root_block_get(index, &mtr) orelse return error.OutOfMemory;

    var cursor = btr_cur_t{
        .index = index,
        .block = root_block,
        .rec = page.page_get_infimum_rec(root_block.frame),
        .opened = true,
    };

    var key_a: i64 = 2;
    var fields_a = [_]data.dfield_t{.{ .data = &key_a, .len = @intCast(@sizeOf(i64)) }};
    var tuple_a = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = fields_a[0..] };

    var rec_out: ?*rec_t = null;
    var big_rec: ?*big_rec_t = null;
    try std.testing.expectEqual(@as(ulint, 0), btr_cur_optimistic_insert(0, &cursor, &tuple_a, &rec_out, &big_rec, 0, null, &mtr));
    try std.testing.expect(rec_out != null);
    try std.testing.expect(big_rec == null);

    var key_b: i64 = 1;
    var fields_b = [_]data.dfield_t{.{ .data = &key_b, .len = @intCast(@sizeOf(i64)) }};
    var tuple_b = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = fields_b[0..] };
    try std.testing.expectEqual(@as(ulint, 0), btr_cur_optimistic_insert(0, &cursor, &tuple_b, &rec_out, &big_rec, 0, null, &mtr));

    var key_c: i64 = 2;
    var fields_c = [_]data.dfield_t{.{ .data = &key_c, .len = @intCast(@sizeOf(i64)) }};
    var tuple_c = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = fields_c[0..] };
    try std.testing.expectEqual(@as(ulint, 0), btr_cur_optimistic_insert(0, &cursor, &tuple_c, &rec_out, &big_rec, 0, null, &mtr));

    const first = page_first_user_rec(root_block.frame) orelse return error.OutOfMemory;
    const second = btr_get_next_user_rec(first, null) orelse return error.OutOfMemory;
    const third = btr_get_next_user_rec(second, null) orelse return error.OutOfMemory;

    try std.testing.expectEqual(@as(i64, 1), first.key);
    try std.testing.expectEqual(@as(i64, 2), second.key);
    try std.testing.expectEqual(@as(i64, 2), third.key);
    try std.testing.expect(btr_get_next_user_rec(third, null) == null);
    try std.testing.expectEqual(@as(ulint, 3), root_block.frame.header.n_recs);

    index_state_remove(index);
}

test "btr leaf split raises root" {
    const allocator = std.heap.page_allocator;
    const index = allocator.create(dict_index_t) catch return error.OutOfMemory;
    index.* = .{};
    defer allocator.destroy(index);

    var mtr = mtr_t{};
    _ = btr_create(0, 1, 0, .{ .high = 0, .low = 400 }, index, &mtr);
    const root_block = btr_root_block_get(index, &mtr) orelse return error.OutOfMemory;

    var cursor = btr_cur_t{
        .index = index,
        .block = root_block,
        .rec = page.page_get_infimum_rec(root_block.frame),
        .opened = true,
    };

    var rec_out: ?*rec_t = null;
    var big_rec: ?*big_rec_t = null;
    var key_val: i64 = 1;
    while (key_val <= @as(i64, BTR_MAX_RECS_PER_PAGE + 1)) : (key_val += 1) {
        var fields = [_]data.dfield_t{.{ .data = &key_val, .len = @intCast(@sizeOf(i64)) }};
        var tuple = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = fields[0..] };
        try std.testing.expectEqual(@as(ulint, 0), btr_cur_optimistic_insert(0, &cursor, &tuple, &rec_out, &big_rec, 0, null, &mtr));
    }

    try std.testing.expectEqual(@as(ulint, 1), index.root_level);
    const new_root = btr_root_block_get(index, &mtr) orelse return error.OutOfMemory;
    try std.testing.expectEqual(@as(ulint, 1), new_root.frame.header.level);
    try std.testing.expectEqual(@as(ulint, 3), btr_get_size(index, 0));
    try std.testing.expectEqual(@as(ulint, 2), new_root.frame.header.n_recs);

    const left_ptr = page_first_user_rec(new_root.frame) orelse return error.OutOfMemory;
    const right_ptr = btr_get_next_user_rec(left_ptr, null) orelse return error.OutOfMemory;
    const left_block = left_ptr.child_block orelse return error.OutOfMemory;
    const right_block = right_ptr.child_block orelse return error.OutOfMemory;

    try std.testing.expect(left_block.frame.next_block == right_block);
    try std.testing.expect(right_block.frame.prev_block == left_block);
    try std.testing.expect(left_block.frame.parent_block == new_root);
    try std.testing.expect(right_block.frame.parent_block == new_root);

    var values = std.ArrayList(i64).init(allocator);
    defer values.deinit();
    var rec = page_first_user_rec(left_block.frame) orelse return error.OutOfMemory;
    while (true) {
        values.append(rec.key) catch return error.OutOfMemory;
        const next = btr_get_next_user_rec(rec, null) orelse break;
        rec = next;
    }

    try std.testing.expectEqual(@as(usize, BTR_MAX_RECS_PER_PAGE + 1), values.items.len);
    var expected: i64 = 1;
    for (values.items) |val| {
        try std.testing.expectEqual(expected, val);
        expected += 1;
    }

    index_state_remove(index);
}

test "btr non-leaf insert updates node pointers" {
    const allocator = std.heap.page_allocator;
    const index = allocator.create(dict_index_t) catch return error.OutOfMemory;
    index.* = .{};
    defer allocator.destroy(index);

    var mtr = mtr_t{};
    _ = btr_create(0, 1, 0, .{ .high = 0, .low = 500 }, index, &mtr);
    const root_block = btr_root_block_get(index, &mtr) orelse return error.OutOfMemory;

    var cursor = btr_cur_t{
        .index = index,
        .block = root_block,
        .rec = page.page_get_infimum_rec(root_block.frame),
        .opened = true,
    };
    var rec_out: ?*rec_t = null;
    var big_rec: ?*big_rec_t = null;
    var key_val: i64 = 1;
    while (key_val <= @as(i64, BTR_MAX_RECS_PER_PAGE + 1)) : (key_val += 1) {
        var fields = [_]data.dfield_t{.{ .data = &key_val, .len = @intCast(@sizeOf(i64)) }};
        var tuple = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = fields[0..] };
        try std.testing.expectEqual(@as(ulint, 0), btr_cur_optimistic_insert(0, &cursor, &tuple, &rec_out, &big_rec, 0, null, &mtr));
    }

    const new_root = btr_root_block_get(index, &mtr) orelse return error.OutOfMemory;
    try std.testing.expectEqual(@as(ulint, 2), new_root.frame.header.n_recs);

    const extra = btr_page_alloc(index, 0, 0, 0, &mtr) orelse return error.OutOfMemory;
    btr_page_create(extra, extra.page_zip, index, 0, &mtr);
    var extra_cursor = page.page_cur_t{};
    page.page_cur_set_before_first(extra, &extra_cursor);
    var extra_rec = page.rec_t{ .key = 99 };
    var offsets: ulint = 0;
    _ = page.page_cur_rec_insert(&extra_cursor, &extra_rec, index, &offsets, &mtr);

    var fields_np = [_]data.dfield_t{
        .{ .data = &extra_rec.key, .len = @intCast(@sizeOf(i64)) },
        .{ .data = @ptrCast(extra), .len = @intCast(@sizeOf(*buf_block_t)) },
    };
    var tuple_np = data.dtuple_t{ .n_fields = 2, .n_fields_cmp = 1, .fields = fields_np[0..] };
    btr_insert_on_non_leaf_level_func(index, index.root_level, &tuple_np, "file", 1, &mtr);

    try std.testing.expectEqual(@as(ulint, 3), new_root.frame.header.n_recs);
    var found = false;
    var node = page_first_user_rec(new_root.frame) orelse return error.OutOfMemory;
    while (true) {
        if (node.child_block == extra) {
            found = true;
            break;
        }
        node = btr_get_next_user_rec(node, null) orelse break;
    }
    try std.testing.expect(found);

    index_state_remove(index);
}

test "btr attach pages and delete node pointer" {
    const allocator = std.heap.page_allocator;
    const index = allocator.create(dict_index_t) catch return error.OutOfMemory;
    index.* = .{};
    defer allocator.destroy(index);

    var mtr = mtr_t{};
    _ = btr_create(0, 1, 0, .{ .high = 0, .low = 600 }, index, &mtr);
    const root_block = btr_root_block_get(index, &mtr) orelse return error.OutOfMemory;

    var cursor = btr_cur_t{
        .index = index,
        .block = root_block,
        .rec = page.page_get_infimum_rec(root_block.frame),
        .opened = true,
    };
    var rec_out: ?*rec_t = null;
    var big_rec: ?*big_rec_t = null;
    var key_val: i64 = 1;
    while (key_val <= @as(i64, BTR_MAX_RECS_PER_PAGE + 1)) : (key_val += 1) {
        var fields = [_]data.dfield_t{.{ .data = &key_val, .len = @intCast(@sizeOf(i64)) }};
        var tuple = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = fields[0..] };
        try std.testing.expectEqual(@as(ulint, 0), btr_cur_optimistic_insert(0, &cursor, &tuple, &rec_out, &big_rec, 0, null, &mtr));
    }

    const new_root = btr_root_block_get(index, &mtr) orelse return error.OutOfMemory;
    const left_ptr = page_first_user_rec(new_root.frame) orelse return error.OutOfMemory;
    const right_ptr = btr_get_next_user_rec(left_ptr, null) orelse return error.OutOfMemory;
    const right_block = right_ptr.child_block orelse return error.OutOfMemory;

    const extra = btr_page_alloc(index, 0, 0, 0, &mtr) orelse return error.OutOfMemory;
    btr_page_create(extra, extra.page_zip, index, 0, &mtr);
    var extra_cursor = page.page_cur_t{};
    page.page_cur_set_before_first(extra, &extra_cursor);
    var extra_rec = page.rec_t{ .key = 150 };
    var offsets: ulint = 0;
    _ = page.page_cur_rec_insert(&extra_cursor, &extra_rec, index, &offsets, &mtr);

    btr_attach_half_pages(index, right_block, &extra_rec, extra, 0, &mtr);
    try std.testing.expectEqual(@as(ulint, 3), new_root.frame.header.n_recs);
    try std.testing.expect(right_block.frame.next_block == extra);
    try std.testing.expect(extra.frame.prev_block == right_block);
    try std.testing.expect(extra.frame.parent_block == new_root);

    btr_node_ptr_delete(index, extra, &mtr);
    try std.testing.expectEqual(@as(ulint, 2), new_root.frame.header.n_recs);
    try std.testing.expect(extra.frame.parent_block == null);

    index_state_remove(index);
}

test "btr delete mark and delete visibility" {
    const allocator = std.heap.page_allocator;
    const index = allocator.create(dict_index_t) catch return error.OutOfMemory;
    index.* = .{};
    defer allocator.destroy(index);

    var mtr = mtr_t{};
    _ = btr_create(0, 1, 0, .{ .high = 0, .low = 700 }, index, &mtr);
    const root_block = btr_root_block_get(index, &mtr) orelse return error.OutOfMemory;

    var cursor = btr_cur_t{
        .index = index,
        .block = root_block,
        .rec = page.page_get_infimum_rec(root_block.frame),
        .opened = true,
    };
    var rec_out: ?*rec_t = null;
    var big_rec: ?*big_rec_t = null;

    var key_val: i64 = 1;
    while (key_val <= 3) : (key_val += 1) {
        var fields = [_]data.dfield_t{.{ .data = &key_val, .len = @intCast(@sizeOf(i64)) }};
        var tuple = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = fields[0..] };
        try std.testing.expectEqual(@as(ulint, 0), btr_cur_optimistic_insert(0, &cursor, &tuple, &rec_out, &big_rec, 0, null, &mtr));
    }

    const rec1 = page_first_user_rec(root_block.frame) orelse return error.OutOfMemory;
    const rec2 = btr_get_next_user_rec(rec1, null) orelse return error.OutOfMemory;
    const rec3 = btr_get_next_user_rec(rec2, null) orelse return error.OutOfMemory;

    var thr = que_thr_t{};
    cursor.rec = rec2;
    try std.testing.expectEqual(@as(ulint, 0), btr_cur_del_mark_set_sec_rec(0, &cursor, compat.TRUE, &thr, &mtr));
    try std.testing.expect(rec2.deleted);

    var values = std.ArrayList(i64).init(allocator);
    defer values.deinit();
    var rec_opt = btr_get_next_user_rec(page.page_get_infimum_rec(root_block.frame), null);
    while (rec_opt) |rec| {
        values.append(rec.key) catch return error.OutOfMemory;
        rec_opt = btr_get_next_user_rec(rec, null);
    }
    const expected_marked = [_]i64{ 1, 3 };
    try std.testing.expectEqualSlices(i64, expected_marked[0..], values.items);

    btr_cur_del_unmark_for_ibuf(rec2, null, &mtr);
    try std.testing.expect(!rec2.deleted);
    values.clearRetainingCapacity();
    rec_opt = btr_get_next_user_rec(page.page_get_infimum_rec(root_block.frame), null);
    while (rec_opt) |rec| {
        values.append(rec.key) catch return error.OutOfMemory;
        rec_opt = btr_get_next_user_rec(rec, null);
    }
    const expected_unmarked = [_]i64{ 1, 2, 3 };
    try std.testing.expectEqualSlices(i64, expected_unmarked[0..], values.items);

    cursor.rec = rec2;
    try std.testing.expectEqual(compat.TRUE, btr_cur_optimistic_delete(&cursor, &mtr));
    values.clearRetainingCapacity();
    rec_opt = btr_get_next_user_rec(page.page_get_infimum_rec(root_block.frame), null);
    while (rec_opt) |rec| {
        values.append(rec.key) catch return error.OutOfMemory;
        rec_opt = btr_get_next_user_rec(rec, null);
    }
    const expected_deleted = [_]i64{ 1, 3 };
    try std.testing.expectEqualSlices(i64, expected_deleted[0..], values.items);

    cursor.rec = rec3;
    var err: ulint = 0;
    try std.testing.expectEqual(compat.TRUE, btr_cur_pessimistic_delete(&err, compat.FALSE, &cursor, .TRX_RB_NONE, &mtr));
    try std.testing.expectEqual(@as(ulint, 0), err);
    values.clearRetainingCapacity();
    rec_opt = btr_get_next_user_rec(page.page_get_infimum_rec(root_block.frame), null);
    while (rec_opt) |rec| {
        values.append(rec.key) catch return error.OutOfMemory;
        rec_opt = btr_get_next_user_rec(rec, null);
    }
    const expected_final = [_]i64{1};
    try std.testing.expectEqualSlices(i64, expected_final[0..], values.items);

    index_state_remove(index);
}

test "btr update paths" {
    const allocator = std.heap.page_allocator;
    const index = allocator.create(dict_index_t) catch return error.OutOfMemory;
    index.* = .{};
    defer allocator.destroy(index);

    var mtr = mtr_t{};
    _ = btr_create(0, 1, 0, .{ .high = 0, .low = 800 }, index, &mtr);
    const root_block = btr_root_block_get(index, &mtr) orelse return error.OutOfMemory;

    var cursor = btr_cur_t{
        .index = index,
        .block = root_block,
        .rec = page.page_get_infimum_rec(root_block.frame),
        .opened = true,
    };
    var rec_out: ?*rec_t = null;
    var big_rec: ?*big_rec_t = null;
    var key_val: i64 = 1;
    while (key_val <= 3) : (key_val += 1) {
        var fields = [_]data.dfield_t{.{ .data = &key_val, .len = @intCast(@sizeOf(i64)) }};
        var tuple = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = fields[0..] };
        try std.testing.expectEqual(@as(ulint, 0), btr_cur_optimistic_insert(0, &cursor, &tuple, &rec_out, &big_rec, 0, null, &mtr));
    }

    const rec1 = page_first_user_rec(root_block.frame) orelse return error.OutOfMemory;
    const rec2 = btr_get_next_user_rec(rec1, null) orelse return error.OutOfMemory;
    const rec3 = btr_get_next_user_rec(rec2, null) orelse return error.OutOfMemory;

    var update_in_place = upd_t{ .new_key = 4, .size_change = false };
    var thr = que_thr_t{};
    cursor.rec = rec2;
    try std.testing.expectEqual(@as(ulint, 0), btr_cur_optimistic_update(0, &cursor, &update_in_place, 0, &thr, &mtr));
    try std.testing.expectEqual(@as(i64, 4), rec2.key);

    var update_size = upd_t{ .new_key = 0, .size_change = true };
    var heap: ?*mem_heap_t = null;
    var big: ?*big_rec_t = null;
    cursor.rec = rec3;
    try std.testing.expectEqual(@as(ulint, 0), btr_cur_pessimistic_update(0, &cursor, &heap, &big, &update_size, 0, &thr, &mtr));

    var values = std.ArrayList(i64).init(allocator);
    defer values.deinit();
    var rec_opt = btr_get_next_user_rec(page.page_get_infimum_rec(root_block.frame), null);
    while (rec_opt) |rec| {
        values.append(rec.key) catch return error.OutOfMemory;
        rec_opt = btr_get_next_user_rec(rec, null);
    }
    const expected = [_]i64{ 0, 1, 4 };
    try std.testing.expectEqualSlices(i64, expected[0..], values.items);

    index_state_remove(index);
}

test "btr page reorganize removes deleted records" {
    const allocator = std.heap.page_allocator;
    const index = allocator.create(dict_index_t) catch return error.OutOfMemory;
    index.* = .{};
    defer allocator.destroy(index);

    var mtr = mtr_t{};
    _ = btr_create(0, 1, 0, .{ .high = 0, .low = 900 }, index, &mtr);
    const root_block = btr_root_block_get(index, &mtr) orelse return error.OutOfMemory;

    var cursor = btr_cur_t{
        .index = index,
        .block = root_block,
        .rec = page.page_get_infimum_rec(root_block.frame),
        .opened = true,
    };
    var rec_out: ?*rec_t = null;
    var big_rec: ?*big_rec_t = null;
    var key_val: i64 = 1;
    while (key_val <= 4) : (key_val += 1) {
        var fields = [_]data.dfield_t{.{ .data = &key_val, .len = @intCast(@sizeOf(i64)) }};
        var tuple = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = fields[0..] };
        try std.testing.expectEqual(@as(ulint, 0), btr_cur_optimistic_insert(0, &cursor, &tuple, &rec_out, &big_rec, 0, null, &mtr));
    }

    const rec1 = page_first_user_rec(root_block.frame) orelse return error.OutOfMemory;
    const rec2 = btr_get_next_user_rec(rec1, null) orelse return error.OutOfMemory;
    const rec3 = btr_get_next_user_rec(rec2, null) orelse return error.OutOfMemory;
    btr_rec_set_deleted_flag(rec2, compat.TRUE);
    btr_rec_set_deleted_flag(rec3, compat.TRUE);

    try std.testing.expectEqual(compat.TRUE, btr_page_reorganize(root_block, index, &mtr));
    try std.testing.expectEqual(@as(ulint, 2), root_block.frame.header.n_recs);

    var values = std.ArrayList(i64).init(allocator);
    defer values.deinit();
    var rec_opt = btr_get_next_user_rec(page.page_get_infimum_rec(root_block.frame), null);
    while (rec_opt) |rec| {
        values.append(rec.key) catch return error.OutOfMemory;
        rec_opt = btr_get_next_user_rec(rec, null);
    }
    const expected = [_]i64{ 1, 4 };
    try std.testing.expectEqualSlices(i64, expected[0..], values.items);

    index_state_remove(index);
}

test "btr discard empty page updates links" {
    const allocator = std.heap.page_allocator;
    const index = allocator.create(dict_index_t) catch return error.OutOfMemory;
    index.* = .{};
    defer allocator.destroy(index);

    var mtr = mtr_t{};
    _ = btr_create(0, 1, 0, .{ .high = 0, .low = 1000 }, index, &mtr);
    const root_block = btr_root_block_get(index, &mtr) orelse return error.OutOfMemory;

    var cursor = btr_cur_t{
        .index = index,
        .block = root_block,
        .rec = page.page_get_infimum_rec(root_block.frame),
        .opened = true,
    };
    var rec_out: ?*rec_t = null;
    var big_rec: ?*big_rec_t = null;
    var key_val: i64 = 1;
    while (key_val <= @as(i64, BTR_MAX_RECS_PER_PAGE + 1)) : (key_val += 1) {
        var fields = [_]data.dfield_t{.{ .data = &key_val, .len = @intCast(@sizeOf(i64)) }};
        var tuple = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = fields[0..] };
        try std.testing.expectEqual(@as(ulint, 0), btr_cur_optimistic_insert(0, &cursor, &tuple, &rec_out, &big_rec, 0, null, &mtr));
    }

    const new_root = btr_root_block_get(index, &mtr) orelse return error.OutOfMemory;
    const left_ptr = page_first_user_rec(new_root.frame) orelse return error.OutOfMemory;
    const right_ptr = btr_get_next_user_rec(left_ptr, null) orelse return error.OutOfMemory;
    const left_block = left_ptr.child_block orelse return error.OutOfMemory;
    const right_block = right_ptr.child_block orelse return error.OutOfMemory;

    var rec_opt = page_first_user_rec(right_block.frame);
    while (rec_opt) |rec| {
        if (rec.is_supremum) {
            break;
        }
        rec.deleted = true;
        rec_opt = rec.next;
    }
    _ = btr_page_reorganize(right_block, index, &mtr);

    var discard_cursor = btr_cur_t{ .index = index, .block = right_block, .rec = page.page_get_infimum_rec(right_block.frame), .opened = true };
    btr_discard_page(&discard_cursor, &mtr);

    try std.testing.expectEqual(@as(ulint, 2), btr_get_size(index, 0));
    try std.testing.expectEqual(@as(ulint, 1), new_root.frame.header.n_recs);
    try std.testing.expect(left_block.frame.next_block == null);

    index_state_remove(index);
}

test "btr min rec mark" {
    var rec = rec_t{};
    var mtr = mtr_t{};
    btr_set_min_rec_mark(&rec, &mtr);
    try std.testing.expect(rec.min_rec_mark);
}

test "btr estimate helpers" {
    const allocator = std.heap.page_allocator;
    const index = allocator.create(dict_index_t) catch return error.OutOfMemory;
    index.* = .{};
    defer allocator.destroy(index);

    var mtr = mtr_t{};
    _ = btr_create(0, 1, 0, .{ .high = 0, .low = 1300 }, index, &mtr);
    const root_block = btr_root_block_get(index, &mtr) orelse return error.OutOfMemory;

    var cursor = btr_cur_t{
        .index = index,
        .block = root_block,
        .rec = page.page_get_infimum_rec(root_block.frame),
        .opened = true,
    };
    var rec_out: ?*rec_t = null;
    var big_rec: ?*big_rec_t = null;
    const keys = [_]i64{ 1, 2, 2, 4, 5 };
    for (keys) |key_val| {
        var key = key_val;
        var fields = [_]data.dfield_t{.{ .data = &key, .len = @intCast(@sizeOf(i64)) }};
        var tuple = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = fields[0..] };
        try std.testing.expectEqual(@as(ulint, 0), btr_cur_optimistic_insert(0, &cursor, &tuple, &rec_out, &big_rec, 0, null, &mtr));
    }

    var low_key: i64 = 2;
    var high_key: i64 = 4;
    var low_fields = [_]data.dfield_t{.{ .data = &low_key, .len = @intCast(@sizeOf(i64)) }};
    var high_fields = [_]data.dfield_t{.{ .data = &high_key, .len = @intCast(@sizeOf(i64)) }};
    var low_tuple = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = low_fields[0..] };
    var high_tuple = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = high_fields[0..] };

    const count = btr_estimate_n_rows_in_range(index, &low_tuple, 0, &high_tuple, 0);
    try std.testing.expectEqual(@as(ib_int64_t, 3), count);

    const distinct = btr_estimate_number_of_different_key_vals(index);
    try std.testing.expectEqual(@as(ib_int64_t, 4), distinct);

    index_state_remove(index);
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

test "btr create root and free" {
    const allocator = std.heap.page_allocator;
    const index = allocator.create(dict_index_t) catch return error.OutOfMemory;
    index.* = .{};
    defer allocator.destroy(index);

    var mtr = mtr_t{};
    const root_no = btr_create(0, 1, 0, .{ .high = 0, .low = 100 }, index, &mtr);
    try std.testing.expect(root_no != 0);
    try std.testing.expectEqual(root_no, index.page);
    try std.testing.expectEqual(@as(ulint, 0), index.root_level);
    try std.testing.expect(btr_root_get(index, &mtr) != null);
    try std.testing.expectEqual(@as(ulint, 1), btr_get_size(index, 0));

    const extra = btr_page_alloc(index, 0, 0, 0, &mtr) orelse return error.OutOfMemory;
    try std.testing.expect(extra.frame.page_no != root_no);
    try std.testing.expectEqual(@as(ulint, 2), btr_get_size(index, 0));

    btr_free_but_not_root(index.space, index.zip_size, root_no);
    try std.testing.expectEqual(@as(ulint, 1), btr_get_size(index, 0));
    try std.testing.expect(btr_root_get(index, &mtr) != null);

    btr_free_root(index.space, index.zip_size, root_no, &mtr);
    try std.testing.expectEqual(@as(ulint, 0), btr_get_size(index, 0));
    try std.testing.expect(btr_root_get(index, &mtr) == null);

    index_state_remove(index);
}

test "btr external field prefix copy" {
    var buf = [_]byte{0} ** 5;
    const payload = [_]byte{ 'a', 'b', 'c', 'd' };

    const copied = btr_copy_externally_stored_field_prefix(buf[0..].ptr, 5, 0, payload[0..].ptr, 3);
    try std.testing.expectEqual(@as(ulint, 3), copied);
    try std.testing.expectEqualStrings("abc", buf[0..3]);
}

test "btr external field store copy free" {
    const allocator = std.heap.page_allocator;
    const index = allocator.create(dict_index_t) catch return error.OutOfMemory;
    index.* = .{};
    defer allocator.destroy(index);

    var page_obj = page_t{};
    page.page_init(&page_obj);
    var block = buf_block_t{ .frame = &page_obj };

    var rec = rec_t{};
    const blob1 = "blob-one";
    const blob2 = "longer-blob-two";
    var big_fields = [_]big_rec_field_t{
        .{ .field_no = 1, .len = @intCast(blob1.len), .data = blob1.ptr },
        .{ .field_no = 3, .len = @intCast(blob2.len), .data = blob2.ptr },
    };
    var big_rec = big_rec_t{ .heap = null, .n_fields = big_fields.len, .fields = big_fields[0..] };

    var offsets: ulint = 0;
    try std.testing.expectEqual(@as(ulint, 0), btr_store_big_rec_extern_fields(index, &block, &rec, &offsets, &big_rec, null));
    try std.testing.expectEqual(@as(usize, 2), rec.extern_fields.len);

    const heap = mem.mem_heap_create_func(0, mem.MEM_HEAP_DYNAMIC) orelse return error.OutOfMemory;
    defer mem.mem_heap_free_func(heap);

    var len: ulint = 0;
    const out_ptr = btr_rec_copy_externally_stored_field(&rec, &offsets, 0, 3, &len, heap) orelse return error.OutOfMemory;
    try std.testing.expectEqual(@as(ulint, blob2.len), len);
    try std.testing.expect(std.mem.eql(u8, out_ptr[0..@as(usize, @intCast(len))], blob2));

    var ref_buf = [_]byte{0};
    btr_free_externally_stored_field(index, ref_buf[0..].ptr, &rec, &offsets, null, 1, .TRX_RB_NONE, null);
    try std.testing.expectEqual(@as(usize, 1), rec.extern_fields.len);

    btr_free_externally_stored_field(index, ref_buf[0..].ptr, &rec, &offsets, null, 3, .TRX_RB_NONE, null);
    try std.testing.expectEqual(@as(usize, 0), rec.extern_fields.len);
}

test "btr persistent cursor store restore" {
    const pcur = btr_pcur_create() orelse return error.OutOfMemory;
    defer btr_pcur_free(pcur);

    try std.testing.expect(!pcur.stored);

    const allocator = std.heap.page_allocator;
    const index = allocator.create(dict_index_t) catch return error.OutOfMemory;
    index.* = .{};
    defer allocator.destroy(index);

    var mtr = mtr_t{};
    _ = btr_create(0, 1, 0, .{ .high = 0, .low = 1100 }, index, &mtr);
    const root_block = btr_root_block_get(index, &mtr) orelse return error.OutOfMemory;

    var cursor = btr_cur_t{
        .index = index,
        .block = root_block,
        .rec = page.page_get_infimum_rec(root_block.frame),
        .opened = true,
    };
    var rec_out: ?*rec_t = null;
    var big_rec: ?*big_rec_t = null;
    var key_val: i64 = 1;
    while (key_val <= 3) : (key_val += 1) {
        var fields = [_]data.dfield_t{.{ .data = &key_val, .len = @intCast(@sizeOf(i64)) }};
        var tuple = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = fields[0..] };
        try std.testing.expectEqual(@as(ulint, 0), btr_cur_optimistic_insert(0, &cursor, &tuple, &rec_out, &big_rec, 0, null, &mtr));
    }

    const rec1 = page_first_user_rec(root_block.frame) orelse return error.OutOfMemory;
    const rec2 = btr_get_next_user_rec(rec1, null) orelse return error.OutOfMemory;

    pcur.btr_cur.index = index;
    pcur.btr_cur.block = root_block;
    pcur.btr_cur.rec = rec2;
    pcur.btr_cur.opened = true;
    pcur.rel_pos = BTR_PCUR_ON;

    btr_pcur_store_position(pcur, &mtr);
    try std.testing.expect(pcur.stored);

    var insert_key: i64 = 4;
    var insert_fields = [_]data.dfield_t{.{ .data = &insert_key, .len = @intCast(@sizeOf(i64)) }};
    var insert_tuple = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = insert_fields[0..] };
    try std.testing.expectEqual(@as(ulint, 0), btr_cur_optimistic_insert(0, &cursor, &insert_tuple, &rec_out, &big_rec, 0, null, &mtr));

    const restored = btr_pcur_restore_position_func(0, pcur, "file", 2, &mtr);
    try std.testing.expectEqual(compat.TRUE, restored);
    try std.testing.expectEqual(@as(i64, 2), pcur.btr_cur.rec.?.key);

    cursor.rec = rec2;
    _ = btr_cur_optimistic_delete(&cursor, &mtr);
    const restored_after_delete = btr_pcur_restore_position_func(0, pcur, "file", 3, &mtr);
    try std.testing.expectEqual(compat.FALSE, restored_after_delete);

    var other = btr_pcur_t{};
    btr_pcur_copy_stored_position(&other, pcur);
    try std.testing.expectEqual(pcur.rel_pos, other.rel_pos);
    try std.testing.expect(other.stored);

    index_state_remove(index);
}

test "btr persistent cursor page moves" {
    const allocator = std.heap.page_allocator;
    const index = allocator.create(dict_index_t) catch return error.OutOfMemory;
    index.* = .{};
    defer allocator.destroy(index);

    var mtr = mtr_t{};
    _ = btr_create(0, 1, 0, .{ .high = 0, .low = 1200 }, index, &mtr);
    const root_block = btr_root_block_get(index, &mtr) orelse return error.OutOfMemory;

    var cursor = btr_cur_t{
        .index = index,
        .block = root_block,
        .rec = page.page_get_infimum_rec(root_block.frame),
        .opened = true,
    };
    var rec_out: ?*rec_t = null;
    var big_rec: ?*big_rec_t = null;
    var key_val: i64 = 1;
    while (key_val <= @as(i64, BTR_MAX_RECS_PER_PAGE + 1)) : (key_val += 1) {
        var fields = [_]data.dfield_t{.{ .data = &key_val, .len = @intCast(@sizeOf(i64)) }};
        var tuple = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = fields[0..] };
        try std.testing.expectEqual(@as(ulint, 0), btr_cur_optimistic_insert(0, &cursor, &tuple, &rec_out, &big_rec, 0, null, &mtr));
    }

    const new_root = btr_root_block_get(index, &mtr) orelse return error.OutOfMemory;
    const left_ptr = page_first_user_rec(new_root.frame) orelse return error.OutOfMemory;
    const right_ptr = btr_get_next_user_rec(left_ptr, null) orelse return error.OutOfMemory;
    const left_block = left_ptr.child_block orelse return error.OutOfMemory;
    const right_block = right_ptr.child_block orelse return error.OutOfMemory;

    var pcur = btr_pcur_t{};
    pcur.btr_cur.index = index;
    pcur.btr_cur.block = left_block;
    pcur.btr_cur.rec = page.page_get_infimum_rec(left_block.frame);
    pcur.btr_cur.opened = true;

    btr_pcur_move_to_next_page(&pcur, &mtr);
    try std.testing.expect(pcur.btr_cur.block == right_block);

    btr_pcur_move_backward_from_page(&pcur, &mtr);
    try std.testing.expect(pcur.btr_cur.block == left_block);

    index_state_remove(index);
}

test "btr search stubs" {
    btr_search_disable();
    try std.testing.expectEqual(@as(u8, 0), btr_search_enabled);
    btr_search_enable();
    try std.testing.expectEqual(@as(u8, 1), btr_search_enabled);

    btr_search_sys_create(128);
    try std.testing.expect(btr_search_sys != null);

    var heap = mem_heap_t{};
    const info = btr_search_info_create(&heap) orelse return error.OutOfMemory;
    defer std.heap.page_allocator.destroy(info);
    try std.testing.expectEqual(@as(ulint, 0), btr_search_info_get_ref_count(info));

    const allocator = std.heap.page_allocator;
    const index = allocator.create(dict_index_t) catch return error.OutOfMemory;
    index.* = .{};
    defer allocator.destroy(index);

    var mtr = mtr_t{};
    _ = btr_create(0, 1, 0, .{ .high = 0, .low = 1300 }, index, &mtr);
    const root_block = btr_root_block_get(index, &mtr) orelse return error.OutOfMemory;

    btr_search_disable();
    var cursor = btr_cur_t{
        .index = index,
        .block = root_block,
        .rec = page.page_get_infimum_rec(root_block.frame),
        .opened = true,
    };
    var rec_out: ?*rec_t = null;
    var big_rec: ?*big_rec_t = null;
    var key_val: i64 = 1;
    while (key_val <= 3) : (key_val += 1) {
        var fields = [_]data.dfield_t{.{ .data = &key_val, .len = @intCast(@sizeOf(i64)) }};
        var tuple = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = fields[0..] };
        try std.testing.expectEqual(@as(ulint, 0), btr_cur_optimistic_insert(0, &cursor, &tuple, &rec_out, &big_rec, 0, null, &mtr));
    }

    var tuple = dtuple_t{};
    var search_key: i64 = 2;
    var search_fields = [_]data.dfield_t{.{ .data = &search_key, .len = @intCast(@sizeOf(i64)) }};
    tuple = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = search_fields[0..] };

    btr_search_enable();
    try std.testing.expect(btr_search_guess_on_hash(index, info, &tuple, 0, 0, &cursor, 0, &mtr) == compat.FALSE);

    btr_search_build_page_hash_index(index, root_block);
    try std.testing.expect(btr_search_guess_on_hash(index, info, &tuple, 0, 0, &cursor, 0, &mtr) == compat.TRUE);
    try std.testing.expectEqual(@as(i64, 2), cursor.rec.?.key);

    const found = btr_find_rec_by_key(index, 2) orelse return error.OutOfMemory;
    try std.testing.expect(found == cursor.rec.?);

    _ = btr_cur_optimistic_delete(&cursor, &mtr);
    try std.testing.expect(btr_search_guess_on_hash(index, info, &tuple, 0, 0, &cursor, 0, &mtr) == compat.FALSE);

    index_state_remove(index);

    btr_search_var_init();
    try std.testing.expectEqual(@as(u8, 1), btr_search_enabled);

    btr_search_sys_free();
    try std.testing.expect(btr_search_sys == null);
}

fn btr_collect_keys(index: *dict_index_t, list: *std.ArrayList(i64)) !void {
    list.clearRetainingCapacity();
    const leftmost = descend_to_level(index, 0, true) orelse return;
    var block_opt: ?*buf_block_t = leftmost;
    while (block_opt) |block| {
        var rec_opt = btr_get_next_user_rec(page.page_get_infimum_rec(block.frame), null);
        while (rec_opt) |rec| {
            try list.append(rec.key);
            rec_opt = btr_get_next_user_rec(rec, null);
        }
        block_opt = block.frame.next_block;
    }
}

test "btr validate index random ops" {
    const allocator = std.heap.page_allocator;
    const index = allocator.create(dict_index_t) catch return error.OutOfMemory;
    index.* = .{};
    defer allocator.destroy(index);

    var mtr = mtr_t{};
    _ = btr_create(0, 1, 0, .{ .high = 0, .low = 1400 }, index, &mtr);

    var keys = std.ArrayList(i64).init(allocator);
    defer keys.deinit();
    var key_set = std.AutoHashMap(i64, void).init(allocator);
    defer key_set.deinit();

    var prng = std.rand.DefaultPrng.init(0x1357_9BDF);
    const rnd = prng.random();

    var op: usize = 0;
    while (op < 200) : (op += 1) {
        const action: u8 = if (keys.items.len == 0) 0 else rnd.uintLessThan(u8, 3);
        switch (action) {
            0 => {
                var key = rnd.intRangeAtMost(i64, 1, 1000);
                var tries: usize = 0;
                while (key_set.contains(key) and tries < 10) : (tries += 1) {
                    key = rnd.intRangeAtMost(i64, 1, 1000);
                }
                if (key_set.contains(key)) {
                    continue;
                }
                const block = btr_find_leaf_for_key(index, key) orelse return error.OutOfMemory;
                var cursor = btr_cur_t{
                    .index = index,
                    .block = block,
                    .rec = page.page_get_infimum_rec(block.frame),
                    .opened = true,
                };
                var rec_out: ?*rec_t = null;
                var big_rec: ?*big_rec_t = null;
                var fields = [_]data.dfield_t{.{ .data = &key, .len = @intCast(@sizeOf(i64)) }};
                var tuple = data.dtuple_t{ .n_fields = 1, .n_fields_cmp = 1, .fields = fields[0..] };
                try std.testing.expectEqual(@as(ulint, 0), btr_cur_optimistic_insert(0, &cursor, &tuple, &rec_out, &big_rec, 0, null, &mtr));
                try key_set.put(key, {});
                try keys.append(key);
            },
            1 => {
                const idx = rnd.uintLessThan(usize, keys.items.len);
                const key = keys.items[idx];
                const rec = btr_find_rec_by_key(index, key) orelse return error.OutOfMemory;
                const block = btr_block_for_rec(index, rec) orelse return error.OutOfMemory;
                var cursor = btr_cur_t{
                    .index = index,
                    .block = block,
                    .rec = rec,
                    .opened = true,
                };
                _ = btr_cur_optimistic_delete(&cursor, &mtr);
                _ = key_set.remove(key);
                keys.items[idx] = keys.items[keys.items.len - 1];
                keys.items.len -= 1;
            },
            else => {
                const idx = rnd.uintLessThan(usize, keys.items.len);
                const key = keys.items[idx];
                const rec = btr_find_rec_by_key(index, key) orelse return error.OutOfMemory;
                const block = btr_block_for_rec(index, rec) orelse return error.OutOfMemory;
                var cursor = btr_cur_t{
                    .index = index,
                    .block = block,
                    .rec = rec,
                    .opened = true,
                };
                var update = upd_t{ .new_key = key, .size_change = false };
                var thr = que_thr_t{};
                try std.testing.expectEqual(@as(ulint, 0), btr_cur_update_in_place(0, &cursor, &update, 0, &thr, &mtr));
            },
        }

        try std.testing.expectEqual(compat.TRUE, btr_validate_index(index, null));
    }

    var scanned = std.ArrayList(i64).init(allocator);
    defer scanned.deinit();
    try btr_collect_keys(index, &scanned);

    const expected = try allocator.dupe(i64, keys.items);
    defer allocator.free(expected);
    std.sort.sort(i64, expected, {}, comptime std.sort.asc(i64));

    try std.testing.expectEqual(expected.len, scanned.items.len);
    for (expected, scanned.items) |exp, got| {
        try std.testing.expectEqual(exp, got);
    }

    index_state_remove(index);
}
