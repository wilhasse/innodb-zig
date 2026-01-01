const std = @import("std");
const compat = @import("../ut/compat.zig");
const fil = @import("../fil/mod.zig");
const log_mod = @import("../log/mod.zig");

pub const module_name = "buf";

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;
pub const ib_uint64_t = compat.ib_uint64_t;
pub const ib_int64_t = compat.ib_int64_t;
pub const byte = compat.byte;

pub const BUF_BUDDY_LOW_SHIFT: u8 = if (@sizeOf(usize) <= 4) 6 else 7;
pub const BUF_BUDDY_LOW: ulint = @as(ulint, 1) << BUF_BUDDY_LOW_SHIFT;
pub const BUF_BUDDY_SIZES: ulint = compat.UNIV_PAGE_SIZE_SHIFT - BUF_BUDDY_LOW_SHIFT;
pub const BUF_BUDDY_HIGH: ulint = BUF_BUDDY_LOW << @as(usize, @intCast(BUF_BUDDY_SIZES));

pub const buf_buddy_stat_t = struct {
    used: ulint = 0,
    relocated: ib_uint64_t = 0,
    relocated_usec: ib_uint64_t = 0,
};

pub var buf_buddy_stat = [_]buf_buddy_stat_t{.{}} ** (BUF_BUDDY_SIZES + 1);

fn buddyBlockSize(slot: ulint) ulint {
    const capped = if (slot > BUF_BUDDY_SIZES) BUF_BUDDY_SIZES else slot;
    return BUF_BUDDY_LOW << @as(u6, @intCast(capped));
}

pub fn buf_buddy_get_slot(size: ulint) ulint {
    var i: ulint = 0;
    var s: ulint = BUF_BUDDY_LOW;
    while (s < size) : (i += 1) {
        s <<= 1;
    }
    return i;
}

pub fn buf_buddy_alloc_low(slot: ulint, lru: ?*ibool) ?*anyopaque {
    if (slot > BUF_BUDDY_SIZES) {
        return null;
    }
    if (lru) |flag| {
        flag.* = compat.FALSE;
    }
    const size = buddyBlockSize(slot);
    const bytes = std.heap.page_allocator.alloc(u8, @as(usize, @intCast(size))) catch return null;
    buf_buddy_stat[@as(usize, @intCast(slot))].used += 1;
    return bytes.ptr;
}

pub fn buf_buddy_free_low(buf: *anyopaque, slot: ulint) void {
    if (slot > BUF_BUDDY_SIZES) {
        return;
    }
    const size = buddyBlockSize(slot);
    const bytes = @as([*]u8, @ptrCast(buf))[0..@as(usize, @intCast(size))];
    std.heap.page_allocator.free(bytes);
    if (buf_buddy_stat[@as(usize, @intCast(slot))].used > 0) {
        buf_buddy_stat[@as(usize, @intCast(slot))].used -= 1;
    }
}

pub fn buf_buddy_alloc(size: ulint, lru: ?*ibool) ?*anyopaque {
    return buf_buddy_alloc_low(buf_buddy_get_slot(size), lru);
}

pub fn buf_buddy_free(buf: *anyopaque, size: ulint) void {
    buf_buddy_free_low(buf, buf_buddy_get_slot(size));
}

pub fn buf_buddy_var_init() void {
    for (&buf_buddy_stat) |*stat| {
        stat.* = .{};
    }
}

pub const BUF_GET: ulint = 10;
pub const BUF_GET_IF_IN_POOL: ulint = 11;
pub const BUF_GET_NO_LATCH: ulint = 14;

pub const BUF_MAKE_YOUNG: ulint = 51;
pub const BUF_KEEP_OLD: ulint = 52;

pub const BUF_NO_CHECKSUM_MAGIC: ulint = 0xDEADBEEF;

pub const buf_page_state = enum(u8) {
    BUF_BLOCK_ZIP_FREE = 0,
    BUF_BLOCK_ZIP_PAGE = 1,
    BUF_BLOCK_ZIP_DIRTY = 2,
    BUF_BLOCK_NOT_USED = 3,
    BUF_BLOCK_READY_FOR_USE = 4,
    BUF_BLOCK_FILE_PAGE = 5,
    BUF_BLOCK_MEMORY = 6,
    BUF_BLOCK_REMOVE_HASH = 7,
};

const BufPageKey = struct {
    space: ulint,
    page_no: ulint,
};

pub const buf_page_t = struct {
    state: buf_page_state = .BUF_BLOCK_NOT_USED,
    space: ulint = 0,
    page_no: ulint = 0,
    dirty: ibool = compat.FALSE,
    modification: ib_uint64_t = 0,
    lru_old: ibool = compat.FALSE,
    last_access: ib_uint64_t = 0,
};

pub const buf_block_t = struct {
    frame: []byte,
    page: buf_page_t = .{},
    zip_size: ulint = 0,
};

pub const buf_pool_t = struct {
    curr_size: ulint = 0,
    oldest_modification: ib_uint64_t = 0,
    free_list_len: ulint = 0,
    dirty_pages: ulint = 0,
    next_modification: ib_uint64_t = 1,
    next_access: ib_uint64_t = 1,
    pages_created: ulint = 0,
    pages_read: ulint = 0,
    read_requests: ulint = 0,
    pages_flushed: ulint = 0,
    write_requests: ulint = 0,
    flush_list: std.ArrayListUnmanaged(*buf_page_t) = .{},
    pages: std.AutoHashMap(BufPageKey, *buf_block_t) = undefined,
};

pub const buf_frame_t = byte;
pub const mtr_t = struct {};
pub const ib_stream_t = @import("../ut/log.zig").Stream;

pub var buf_pool: ?*buf_pool_t = null;
pub var buf_debug_prints: ibool = compat.FALSE;
pub var srv_buf_pool_write_requests: ulint = 0;

pub fn buf_var_init() void {
    srv_buf_pool_write_requests = 0;
}

pub fn buf_calc_page_new_checksum(page: [*]const byte) ulint {
    const page_slice = page[0..compat.UNIV_PAGE_SIZE];
    var crc = std.hash.Crc32.init();
    const end_off = compat.UNIV_PAGE_SIZE - fil.FIL_PAGE_END_LSN_OLD_CHKSUM;
    const head = page_slice[0..fil.FIL_PAGE_SPACE_OR_CHKSUM];
    const mid = page_slice[fil.FIL_PAGE_SPACE_OR_CHKSUM + 4 .. end_off];
    const tail = page_slice[end_off + 4 ..];
    crc.update(head);
    crc.update(mid);
    crc.update(tail);
    return @as(ulint, @intCast(crc.final()));
}

pub fn buf_calc_page_old_checksum(page: [*]const byte) ulint {
    const page_slice = page[0..compat.UNIV_PAGE_SIZE];
    var sum: u32 = 0;
    const end_off = compat.UNIV_PAGE_SIZE - fil.FIL_PAGE_END_LSN_OLD_CHKSUM;
    for (page_slice, 0..) |b, idx| {
        if (idx >= fil.FIL_PAGE_SPACE_OR_CHKSUM and idx < fil.FIL_PAGE_SPACE_OR_CHKSUM + 4) {
            continue;
        }
        if (idx >= end_off and idx < end_off + 4) {
            continue;
        }
        sum +%= b;
    }
    return @as(ulint, sum);
}

pub fn buf_page_is_corrupted(read_buf: [*]const byte, zip_size: ulint) ibool {
    _ = read_buf;
    _ = zip_size;
    return compat.FALSE;
}

pub fn buf_page_print(read_buf: [*]const byte, zip_size: ulint) void {
    _ = read_buf;
    _ = zip_size;
}

pub fn buf_pool_contains_zip(data: ?*const anyopaque) ?*buf_block_t {
    _ = data;
    return null;
}

fn buf_pool_clear_pages(pool: *buf_pool_t) void {
    var it = pool.pages.valueIterator();
    while (it.next()) |block| {
        buf_block_free(block.*);
    }
    pool.pages.clearRetainingCapacity();
    pool.flush_list.deinit(std.heap.page_allocator);
    pool.flush_list = .{};
    pool.curr_size = 0;
    pool.dirty_pages = 0;
    pool.oldest_modification = 0;
    pool.next_modification = 1;
    pool.next_access = 1;
    pool.pages_created = 0;
    pool.pages_read = 0;
    pool.read_requests = 0;
    pool.pages_flushed = 0;
    pool.write_requests = 0;
    pool.free_list_len = 0;
}

fn buf_pool_recompute_oldest(pool: *buf_pool_t) void {
    var oldest: ib_uint64_t = 0;
    var it = pool.pages.valueIterator();
    while (it.next()) |block| {
        const page = &block.*.page;
        if (page.dirty == compat.TRUE) {
            if (oldest == 0 or page.modification < oldest) {
                oldest = page.modification;
            }
        }
    }
    pool.oldest_modification = oldest;
}

fn buf_pool_touch_page(pool: *buf_pool_t, bpage: *buf_page_t, old: ?ibool) void {
    bpage.last_access = pool.next_access;
    pool.next_access += 1;
    if (old) |flag| {
        bpage.lru_old = flag;
    }
}

pub fn buf_pool_init() ?*buf_pool_t {
    if (buf_pool != null) {
        return buf_pool;
    }
    const pool = std.heap.page_allocator.create(buf_pool_t) catch return null;
    pool.* = .{
        .pages = std.AutoHashMap(BufPageKey, *buf_block_t).init(std.heap.page_allocator),
    };
    buf_pool = pool;
    return pool;
}

pub fn buf_close() void {}

pub fn buf_mem_free() void {
    if (buf_pool) |pool| {
        buf_pool_clear_pages(pool);
        pool.pages.deinit();
        std.heap.page_allocator.destroy(pool);
        buf_pool = null;
    }
}

pub fn buf_pool_drop_hash_index() void {
    if (buf_pool) |pool| {
        buf_pool_clear_pages(pool);
    }
}

pub fn buf_relocate(bpage: *buf_page_t, dpage: *buf_page_t) void {
    dpage.* = bpage.*;
}

pub fn buf_pool_resize() void {}

pub fn buf_pool_get_curr_size() ulint {
    return if (buf_pool) |pool| pool.curr_size else 0;
}

pub fn buf_pool_get_oldest_modification() ib_uint64_t {
    return if (buf_pool) |pool| pool.oldest_modification else 0;
}

pub fn buf_block_alloc(zip_size: ulint) ?*buf_block_t {
    const block = std.heap.page_allocator.create(buf_block_t) catch return null;
    const bytes = std.heap.page_allocator.alloc(byte, compat.UNIV_PAGE_SIZE) catch {
        std.heap.page_allocator.destroy(block);
        return null;
    };
    block.* = .{
        .frame = bytes,
        .zip_size = zip_size,
    };
    return block;
}

pub fn buf_block_free(block: *buf_block_t) void {
    std.heap.page_allocator.free(block.frame);
    std.heap.page_allocator.destroy(block);
}

pub fn buf_frame_copy(buf: [*]byte, frame: [*]const buf_frame_t) [*]byte {
    std.mem.copyForwards(byte, buf[0..compat.UNIV_PAGE_SIZE], frame[0..compat.UNIV_PAGE_SIZE]);
    return buf;
}

pub fn buf_page_make_young(bpage: *buf_page_t) void {
    buf_LRU_make_block_young(bpage);
}

pub fn buf_page_set_dirty(bpage: *buf_page_t) void {
    if (buf_pool) |pool| {
        if (bpage.dirty == compat.FALSE) {
            bpage.dirty = compat.TRUE;
            bpage.modification = pool.next_modification;
            pool.next_modification += 1;
            pool.dirty_pages += 1;
            if (pool.oldest_modification == 0 or bpage.modification < pool.oldest_modification) {
                pool.oldest_modification = bpage.modification;
            }
            buf_flush_list_add(pool, bpage);
        }
    }
}

fn buf_page_clear_dirty(bpage: *buf_page_t) void {
    if (bpage.dirty == compat.TRUE) {
        bpage.dirty = compat.FALSE;
        bpage.modification = 0;
        if (buf_pool) |pool| {
            if (pool.dirty_pages > 0) {
                pool.dirty_pages -= 1;
            }
            buf_flush_list_remove(pool, bpage);
            buf_pool_recompute_oldest(pool);
        }
    }
}

fn buf_flush_list_add(pool: *buf_pool_t, bpage: *buf_page_t) void {
    pool.flush_list.append(std.heap.page_allocator, bpage) catch {};
}

fn buf_flush_list_remove(pool: *buf_pool_t, bpage: *buf_page_t) void {
    var idx: usize = 0;
    while (idx < pool.flush_list.items.len) : (idx += 1) {
        if (pool.flush_list.items[idx] == bpage) {
            _ = pool.flush_list.orderedRemove(idx);
            return;
        }
    }
}

pub fn buf_reset_check_index_page_at_flush(space: ulint, offset: ulint) void {
    _ = space;
    _ = offset;
}

pub fn buf_page_peek_if_search_hashed(space: ulint, offset: ulint) ibool {
    _ = space;
    _ = offset;
    return compat.FALSE;
}

pub fn buf_page_set_file_page_was_freed(space: ulint, offset: ulint) ?*buf_page_t {
    if (buf_pool) |pool| {
        const key = BufPageKey{ .space = space, .page_no = offset };
        if (pool.pages.get(key)) |block| {
            return &block.page;
        }
    }
    return null;
}

pub fn buf_page_reset_file_page_was_freed(space: ulint, offset: ulint) ?*buf_page_t {
    if (buf_pool) |pool| {
        const key = BufPageKey{ .space = space, .page_no = offset };
        if (pool.pages.get(key)) |block| {
            return &block.page;
        }
    }
    return null;
}

pub fn buf_page_get_zip(space: ulint, zip_size: ulint, offset: ulint) ?*buf_page_t {
    _ = zip_size;
    if (buf_pool) |pool| {
        const key = BufPageKey{ .space = space, .page_no = offset };
        if (pool.pages.get(key)) |block| {
            return &block.page;
        }
    }
    return null;
}

pub fn buf_zip_decompress(block: *buf_block_t, check: ibool) ibool {
    _ = block;
    _ = check;
    return compat.TRUE;
}

pub fn buf_block_align(ptr: [*]const byte) ?*buf_block_t {
    _ = ptr;
    return null;
}

pub fn buf_pointer_is_block_field(ptr: ?*const anyopaque) ibool {
    _ = ptr;
    return compat.FALSE;
}

pub fn buf_page_get_gen(
    space: ulint,
    zip_size: ulint,
    offset: ulint,
    rw_latch: ulint,
    guess: ?*buf_block_t,
    mode: ulint,
    file: []const u8,
    line: ulint,
    mtr: *mtr_t,
) ?*buf_block_t {
    _ = rw_latch;
    _ = guess;
    _ = file;
    _ = line;
    _ = mtr;
    const pool = buf_pool_init() orelse return null;
    const key = BufPageKey{ .space = space, .page_no = offset };
    if (pool.pages.get(key)) |block| {
        buf_pool_touch_page(pool, &block.page, null);
        return block;
    }
    if (mode == BUF_GET_IF_IN_POOL) {
        return null;
    }
    const block = buf_block_alloc(zip_size) orelse return null;
    block.page = .{
        .state = .BUF_BLOCK_FILE_PAGE,
        .space = space,
        .page_no = offset,
    };
    pool.pages.put(key, block) catch {
        buf_block_free(block);
        return null;
    };
    pool.curr_size += 1;
    pool.pages_created += 1;
    buf_pool_touch_page(pool, &block.page, compat.FALSE);
    return block;
}

pub fn buf_page_optimistic_get(
    rw_latch: ulint,
    block: *buf_block_t,
    modify_clock: ib_uint64_t,
    file: []const u8,
    line: ulint,
    mtr: *mtr_t,
) ibool {
    _ = rw_latch;
    _ = modify_clock;
    _ = file;
    _ = line;
    _ = mtr;
    return if (block.page.state == .BUF_BLOCK_FILE_PAGE) compat.TRUE else compat.FALSE;
}

pub fn buf_page_get_known_nowait(
    rw_latch: ulint,
    block: *buf_block_t,
    mode: ulint,
    file: []const u8,
    line: ulint,
    mtr: *mtr_t,
) ibool {
    _ = rw_latch;
    _ = mode;
    _ = file;
    _ = line;
    _ = mtr;
    return if (block.page.state == .BUF_BLOCK_FILE_PAGE) compat.TRUE else compat.FALSE;
}

pub fn buf_page_try_get_func(
    space_id: ulint,
    page_no: ulint,
    file: []const u8,
    line: ulint,
    mtr: *mtr_t,
) ?*const buf_block_t {
    _ = file;
    _ = line;
    _ = mtr;
    if (buf_pool) |pool| {
        const key = BufPageKey{ .space = space_id, .page_no = page_no };
        if (pool.pages.get(key)) |block| {
            return block;
        }
    }
    return null;
}

pub fn buf_page_init_for_read(
    err: *ulint,
    mode: ulint,
    space: ulint,
    zip_size: ulint,
    unzip: ibool,
    tablespace_version: ib_int64_t,
    offset: ulint,
) ?*buf_page_t {
    _ = mode;
    _ = space;
    _ = zip_size;
    _ = unzip;
    _ = tablespace_version;
    _ = offset;
    err.* = 0;
    return null;
}

pub fn buf_page_create(space: ulint, offset: ulint, zip_size: ulint, mtr: *mtr_t) ?*buf_block_t {
    const block = buf_page_get_gen(space, zip_size, offset, 0, null, BUF_GET, "", 0, mtr) orelse return null;
    buf_page_set_dirty(&block.page);
    return block;
}

pub fn buf_page_io_complete(bpage: *buf_page_t) void {
    _ = bpage;
}

pub fn buf_pool_invalidate() void {
    if (buf_pool) |pool| {
        buf_pool_clear_pages(pool);
    }
}

pub fn buf_validate() ibool {
    return compat.TRUE;
}

pub fn buf_print() void {}

pub fn buf_get_latched_pages_number() ulint {
    return 0;
}

pub fn buf_get_n_pending_ios() ulint {
    return 0;
}

pub fn buf_get_modified_ratio_pct() ulint {
    return 0;
}

pub fn buf_print_io(ib_stream: ib_stream_t) void {
    _ = ib_stream;
}

pub fn buf_refresh_io_stats() void {}

pub fn buf_all_freed() ibool {
    return if (buf_pool == null) compat.TRUE else compat.FALSE;
}

pub fn buf_pool_check_no_pending_io() ibool {
    return compat.TRUE;
}

pub fn buf_get_free_list_len() ulint {
    return if (buf_pool) |pool| pool.free_list_len else 0;
}

pub fn buf_page_init_for_backup_restore(space: ulint, offset: ulint, zip_size: ulint, block: *buf_block_t) void {
    _ = space;
    _ = offset;
    _ = zip_size;
    _ = block;
}

pub const buf_flush = enum(u8) {
    BUF_FLUSH_LRU = 0,
    BUF_FLUSH_SINGLE_PAGE = 1,
    BUF_FLUSH_LIST = 2,
    BUF_FLUSH_N_TYPES = 3,
};

pub const buf_flush_stat_t = struct {
    redo: ib_uint64_t = 0,
    n_flushed: ulint = 0,
};

pub const BUF_READ_AHEAD_AREA: ulint = 0;
pub const BUF_FLUSH_FREE_BLOCK_MARGIN: ulint = 5 + BUF_READ_AHEAD_AREA;
pub const BUF_FLUSH_EXTRA_MARGIN: ulint = BUF_FLUSH_FREE_BLOCK_MARGIN / 4 + 100;
pub const BUF_READ_IBUF_PAGES_ONLY: ulint = 131;
pub const BUF_READ_ANY_PAGE: ulint = 132;

pub fn buf_flush_remove(bpage: *buf_page_t) void {
    buf_page_clear_dirty(bpage);
    if (buf_pool) |pool| {
        buf_pool_recompute_oldest(pool);
    }
}

pub fn buf_flush_write_complete(bpage: *buf_page_t) void {
    buf_page_clear_dirty(bpage);
    if (buf_pool) |pool| {
        buf_pool_recompute_oldest(pool);
    }
}

pub fn buf_flush_free_margin() void {
    if (buf_pool) |pool| {
        if (pool.free_list_len >= BUF_FLUSH_FREE_BLOCK_MARGIN) {
            return;
        }
        var attempts: ulint = 0;
        while (pool.free_list_len < BUF_FLUSH_FREE_BLOCK_MARGIN) : (attempts += 1) {
            if (attempts > pool.curr_size) {
                break;
            }
            if (buf_LRU_search_and_free_block(0) == compat.FALSE) {
                break;
            }
        }
    }
}

pub fn buf_flush_init_for_writing(page: [*]byte, page_zip_: ?*anyopaque, newest_lsn: ib_uint64_t) void {
    _ = page_zip_;
    const page_slice = page[0..compat.UNIV_PAGE_SIZE];
    std.mem.writeInt(u64, page_slice[fil.FIL_PAGE_LSN .. fil.FIL_PAGE_LSN + 8], newest_lsn, .big);
    const new_checksum = buf_calc_page_new_checksum(page);
    std.mem.writeInt(u32, page_slice[fil.FIL_PAGE_SPACE_OR_CHKSUM .. fil.FIL_PAGE_SPACE_OR_CHKSUM + 4], @as(u32, @intCast(new_checksum)), .big);
    const old_checksum = buf_calc_page_old_checksum(page);
    const end_off = compat.UNIV_PAGE_SIZE - fil.FIL_PAGE_END_LSN_OLD_CHKSUM;
    std.mem.writeInt(u32, page_slice[end_off .. end_off + 4], @as(u32, @intCast(old_checksum)), .big);
}

pub fn buf_flush_batch(flush_type: buf_flush, min_n: ulint, lsn_limit: ib_uint64_t) ulint {
    _ = lsn_limit;
    if (buf_pool) |pool| {
        var flushed: ulint = 0;
        if (flush_type == .BUF_FLUSH_LIST and pool.flush_list.items.len != 0) {
            var idx: usize = 0;
            while (idx < pool.flush_list.items.len) {
                const bpage = pool.flush_list.items[idx];
                if (bpage.dirty == compat.TRUE) {
                    buf_page_clear_dirty(bpage);
                    flushed += 1;
                    if (min_n != 0 and flushed >= min_n) {
                        break;
                    }
                } else {
                    idx += 1;
                }
            }
        } else {
            var it = pool.pages.valueIterator();
            while (it.next()) |block| {
                if (block.*.page.dirty == compat.TRUE) {
                    buf_page_clear_dirty(&block.*.page);
                    flushed += 1;
                    if (min_n != 0 and flushed >= min_n) {
                        break;
                    }
                }
            }
        }
        if (flushed > 0) {
            buf_pool_recompute_oldest(pool);
        }
        pool.pages_flushed += flushed;
        pool.write_requests += flushed;
        srv_buf_pool_write_requests = pool.write_requests;
        return flushed;
    }
    return 0;
}

pub fn buf_flush_wait_batch_end(flush_type: buf_flush) void {
    _ = flush_type;
}

pub fn buf_flush_ready_for_replace(bpage: *buf_page_t) ibool {
    return if (bpage.dirty == compat.TRUE) compat.FALSE else compat.TRUE;
}

pub fn buf_flush_stat_update() void {
    if (buf_pool) |pool| {
        srv_buf_pool_write_requests = pool.write_requests;
    } else {
        srv_buf_pool_write_requests = 0;
    }
}

pub fn buf_flush_get_desired_flush_rate() ulint {
    if (buf_pool) |pool| {
        if (pool.curr_size == 0) {
            return 0;
        }
        return @as(ulint, @intCast((pool.dirty_pages * 100) / pool.curr_size));
    }
    return 0;
}

pub fn buf_flush_validate() ibool {
    return compat.TRUE;
}

pub const buf_lru_free_block_status = enum(u8) {
    BUF_LRU_FREED = 0,
    BUF_LRU_CANNOT_RELOCATE = 1,
    BUF_LRU_NOT_FREED = 2,
};

pub const BUF_LRU_OLD_MIN_LEN: ulint = 512;
pub const BUF_LRU_FREE_SEARCH_LEN: ulint = 5 + 2 * BUF_READ_AHEAD_AREA;
pub var buf_LRU_old_ratio: ulint = 0;

pub fn buf_LRU_try_free_flushed_blocks() void {
    if (buf_pool) |pool| {
        var iterations: ulint = 0;
        while (pool.free_list_len < BUF_FLUSH_FREE_BLOCK_MARGIN) : (iterations += 1) {
            if (iterations > pool.curr_size) {
                break;
            }
            if (buf_LRU_search_and_free_block(BUF_LRU_FREE_SEARCH_LEN) == compat.FALSE) {
                break;
            }
        }
    }
}

pub fn buf_LRU_buf_pool_running_out() ibool {
    return compat.FALSE;
}

pub fn buf_LRU_invalidate_tablespace(id: ulint) void {
    if (buf_pool) |pool| {
        var keys: std.ArrayList(BufPageKey) = .{};
        defer keys.deinit(std.heap.page_allocator);
        var it = pool.pages.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.*.page.space == id) {
                keys.append(std.heap.page_allocator, entry.key_ptr.*) catch {};
            }
        }
        for (keys.items) |key| {
            buf_pool_remove_block(pool, key);
        }
    }
}

pub fn buf_LRU_insert_zip_clean(bpage: *buf_page_t) void {
    buf_LRU_add_block(bpage, compat.FALSE);
}

fn buf_pool_find_block_key(pool: *buf_pool_t, bpage: *buf_page_t) ?BufPageKey {
    var it = pool.pages.iterator();
    while (it.next()) |entry| {
        if (&entry.value_ptr.*.page == bpage) {
            return entry.key_ptr.*;
        }
    }
    return null;
}

fn buf_pool_remove_block(pool: *buf_pool_t, key: BufPageKey) void {
    if (pool.pages.fetchRemove(key)) |entry| {
        if (pool.curr_size > 0) {
            pool.curr_size -= 1;
        }
        pool.free_list_len += 1;
        buf_block_free(entry.value);
        buf_pool_recompute_oldest(pool);
    }
}

pub fn buf_LRU_free_block(bpage: *buf_page_t, zip: ibool, buf_pool_mutex_released: ?*ibool) buf_lru_free_block_status {
    _ = zip;
    if (buf_pool_mutex_released) |flag| {
        flag.* = compat.FALSE;
    }
    if (bpage.dirty == compat.TRUE) {
        return .BUF_LRU_NOT_FREED;
    }
    const pool = buf_pool orelse return .BUF_LRU_NOT_FREED;
    const key = buf_pool_find_block_key(pool, bpage) orelse return .BUF_LRU_NOT_FREED;
    buf_pool_remove_block(pool, key);
    return .BUF_LRU_FREED;
}

pub fn buf_LRU_search_and_free_block(n_iterations: ulint) ibool {
    const pool = buf_pool orelse return compat.FALSE;
    var candidate: ?BufPageKey = null;
    var candidate_access: ib_uint64_t = 0;
    var scanned: ulint = 0;
    var it = pool.pages.iterator();
    while (it.next()) |entry| {
        if (n_iterations != 0 and scanned >= n_iterations) {
            break;
        }
        scanned += 1;
        const page = &entry.value_ptr.*.page;
        if (page.dirty == compat.TRUE) {
            continue;
        }
        if (candidate == null or page.last_access < candidate_access) {
            candidate = entry.key_ptr.*;
            candidate_access = page.last_access;
        }
    }
    if (candidate) |key| {
        buf_pool_remove_block(pool, key);
        return compat.TRUE;
    }
    return compat.FALSE;
}

pub fn buf_LRU_get_free_only() ?*buf_block_t {
    return null;
}

pub fn buf_LRU_get_free_block(zip_size: ulint) ?*buf_block_t {
    return buf_block_alloc(zip_size);
}

pub fn buf_LRU_block_free_non_file_page(block: *buf_block_t) void {
    buf_block_free(block);
}

pub fn buf_LRU_add_block(bpage: *buf_page_t, old: ibool) void {
    const pool = buf_pool orelse return;
    buf_pool_touch_page(pool, bpage, old);
}

pub fn buf_unzip_LRU_add_block(block: *buf_block_t, old: ibool) void {
    _ = block;
    _ = old;
}

pub fn buf_LRU_make_block_young(bpage: *buf_page_t) void {
    if (buf_pool) |pool| {
        buf_pool_touch_page(pool, bpage, compat.FALSE);
    } else {
        bpage.lru_old = compat.FALSE;
    }
}

pub fn buf_LRU_make_block_old(bpage: *buf_page_t) void {
    if (buf_pool) |pool| {
        buf_pool_touch_page(pool, bpage, compat.TRUE);
    } else {
        bpage.lru_old = compat.TRUE;
    }
}

pub fn buf_LRU_old_ratio_update(old_pct: ulint, adjust: ibool) ulint {
    _ = adjust;
    buf_LRU_old_ratio = old_pct;
    return old_pct;
}

pub fn buf_LRU_stat_update() void {}

pub fn buf_LRU_var_init() void {
    buf_LRU_old_ratio = 0;
}

pub fn buf_LRU_validate() ibool {
    return compat.TRUE;
}

pub fn buf_LRU_print() void {}

pub fn buf_read_page(space: ulint, zip_size: ulint, offset: ulint) ibool {
    const pool = buf_pool_init() orelse return compat.FALSE;
    pool.read_requests += 1;
    const key = BufPageKey{ .space = space, .page_no = offset };
    var block: *buf_block_t = undefined;
    if (pool.pages.get(key)) |existing| {
        block = existing;
    } else {
        block = buf_block_alloc(zip_size) orelse return compat.FALSE;
        block.page = .{
            .state = .BUF_BLOCK_FILE_PAGE,
            .space = space,
            .page_no = offset,
        };
        pool.pages.put(key, block) catch {
            buf_block_free(block);
            return compat.FALSE;
        };
        pool.curr_size += 1;
        pool.pages_created += 1;
    }

    if (fil.fil_tablespace_exists_in_mem(space) == compat.FALSE) {
        return compat.FALSE;
    }
    if (fil.fil_read_page(space, offset, block.frame.ptr) != fil.DB_SUCCESS) {
        return compat.FALSE;
    }
    block.page.dirty = compat.FALSE;
    _ = log_mod.recv_apply_log_recs(space, offset, block.frame.ptr);
    pool.pages_read += 1;
    buf_pool_touch_page(pool, &block.page, compat.FALSE);
    return compat.TRUE;
}

pub fn buf_read_ahead_linear(space: ulint, zip_size: ulint, offset: ulint) ulint {
    if (fil.fil_tablespace_exists_in_mem(space) == compat.FALSE) {
        return 0;
    }
    const size = fil.fil_space_get_size(space);
    if (size == 0 or offset + 1 >= size) {
        return 0;
    }
    const next_page = offset + 1;
    if (buf_read_page(space, zip_size, next_page) == compat.TRUE) {
        return 1;
    }
    return 0;
}

pub fn buf_read_ibuf_merge_pages(sync: ibool, space_ids: []const ulint, space_versions: []const ib_int64_t, page_nos: []const ulint, n_stored: ulint) void {
    _ = sync;
    _ = space_ids;
    _ = space_versions;
    _ = page_nos;
    _ = n_stored;
}

pub fn buf_read_recv_pages(sync: ibool, space: ulint, zip_size: ulint, page_nos: []const ulint, n_stored: ulint) void {
    _ = sync;
    _ = space;
    _ = zip_size;
    _ = page_nos;
    _ = n_stored;
}

test "buf buddy slot and alloc/free" {
    buf_buddy_var_init();
    try std.testing.expectEqual(@as(ulint, 0), buf_buddy_get_slot(BUF_BUDDY_LOW));
    try std.testing.expectEqual(@as(ulint, 1), buf_buddy_get_slot(BUF_BUDDY_LOW << 1));

    var lru_flag: ibool = compat.TRUE;
    const ptr = buf_buddy_alloc(BUF_BUDDY_LOW, &lru_flag) orelse return error.OutOfMemory;
    try std.testing.expectEqual(@as(ibool, compat.FALSE), lru_flag);
    try std.testing.expect(buf_buddy_stat[0].used == 1);
    buf_buddy_free(ptr, BUF_BUDDY_LOW);
    try std.testing.expect(buf_buddy_stat[0].used == 0);
}

test "buf pool basics" {
    defer buf_mem_free();
    const pool = buf_pool_init() orelse return error.OutOfMemory;
    try std.testing.expect(pool == buf_pool.?);
    try std.testing.expectEqual(@as(ulint, 0), buf_pool_get_curr_size());
    try std.testing.expectEqual(@as(ib_uint64_t, 0), buf_pool_get_oldest_modification());
    try std.testing.expectEqual(compat.FALSE, buf_all_freed());
    buf_mem_free();
    try std.testing.expectEqual(compat.TRUE, buf_all_freed());
}

test "buf dirty tracking and flush" {
    defer buf_mem_free();
    _ = buf_pool_init() orelse return error.OutOfMemory;

    var mtr = mtr_t{};
    const block = buf_page_get_gen(1, 0, 7, 0, null, BUF_GET, "test", 0, &mtr) orelse return error.OutOfMemory;
    try std.testing.expectEqual(@as(ulint, 1), buf_pool_get_curr_size());
    try std.testing.expectEqual(compat.FALSE, block.page.dirty);

    buf_page_set_dirty(&block.page);
    try std.testing.expectEqual(compat.TRUE, block.page.dirty);
    try std.testing.expect(buf_pool_get_oldest_modification() != 0);

    const flushed = buf_flush_batch(.BUF_FLUSH_SINGLE_PAGE, 0, 0);
    try std.testing.expectEqual(@as(ulint, 1), flushed);
    try std.testing.expectEqual(compat.FALSE, block.page.dirty);
    try std.testing.expectEqual(@as(ib_uint64_t, 0), buf_pool_get_oldest_modification());
}

test "buf block alloc and frame copy" {
    const block = buf_block_alloc(0) orelse return error.OutOfMemory;
    defer buf_block_free(block);

    try std.testing.expectEqual(@as(usize, compat.UNIV_PAGE_SIZE), block.frame.len);

    var src = [_]byte{0} ** compat.UNIV_PAGE_SIZE;
    var dst = [_]byte{0} ** compat.UNIV_PAGE_SIZE;
    src[0] = 1;
    src[1] = 2;
    src[2] = 3;
    src[3] = 4;

    _ = buf_frame_copy(dst[0..].ptr, src[0..].ptr);
    try std.testing.expectEqual(@as(byte, 1), dst[0]);
    try std.testing.expectEqual(@as(byte, 4), dst[3]);
}

test "buf flush helpers" {
    defer buf_mem_free();
    var page = buf_page_t{};
    try std.testing.expectEqual(compat.TRUE, buf_flush_ready_for_replace(&page));
    try std.testing.expectEqual(@as(ulint, 0), buf_flush_get_desired_flush_rate());

    _ = buf_pool_init() orelse return error.OutOfMemory;
    var mtr = mtr_t{};
    const block = buf_page_get_gen(1, 0, 1, 0, null, BUF_GET, "", 0, &mtr) orelse return error.OutOfMemory;
    buf_page_set_dirty(&block.page);
    try std.testing.expect(buf_flush_get_desired_flush_rate() > 0);
}

test "buf LRU helpers" {
    defer buf_mem_free();
    buf_LRU_var_init();
    try std.testing.expectEqual(@as(ulint, 0), buf_LRU_old_ratio);

    const updated = buf_LRU_old_ratio_update(37, compat.FALSE);
    try std.testing.expectEqual(@as(ulint, 37), updated);
    try std.testing.expectEqual(@as(ulint, 37), buf_LRU_old_ratio);

    var mtr = mtr_t{};
    const block = buf_page_get_gen(1, 0, 1, 0, null, BUF_GET, "", 0, &mtr) orelse return error.OutOfMemory;
    try std.testing.expect(buf_page_get_zip(1, 0, 1) != null);

    var released: ibool = compat.TRUE;
    try std.testing.expectEqual(.BUF_LRU_FREED, buf_LRU_free_block(&block.page, compat.FALSE, &released));
    try std.testing.expectEqual(@as(ibool, compat.FALSE), released);
    try std.testing.expect(buf_page_get_zip(1, 0, 1) == null);

    const block2 = buf_page_get_gen(2, 0, 2, 0, null, BUF_GET, "", 0, &mtr) orelse return error.OutOfMemory;
    const block3 = buf_page_get_gen(3, 0, 3, 0, null, BUF_GET, "", 0, &mtr) orelse return error.OutOfMemory;
    try std.testing.expect(block2.page.space == 2);
    try std.testing.expect(block3.page.space == 3);

    buf_LRU_invalidate_tablespace(2);
    try std.testing.expect(buf_page_get_zip(2, 0, 2) == null);
    try std.testing.expect(buf_page_get_zip(3, 0, 3) != null);

    try std.testing.expectEqual(compat.TRUE, buf_LRU_search_and_free_block(0));
}

test "buf read stubs" {
    try std.testing.expectEqual(compat.FALSE, buf_read_page(0, 0, 0));
    try std.testing.expectEqual(@as(ulint, 0), buf_read_ahead_linear(0, 0, 0));
}

test "buf flush list tracks dirty pages" {
    var mtr = mtr_t{};
    const block = buf_page_get_gen(1, 0, 42, 0, null, BUF_GET, "flush", 0, &mtr) orelse return error.OutOfMemory;
    const pool = buf_pool orelse return error.UnexpectedNull;
    try std.testing.expectEqual(@as(usize, 0), pool.flush_list.items.len);
    buf_page_set_dirty(&block.page);
    try std.testing.expectEqual(@as(usize, 1), pool.flush_list.items.len);
    buf_flush_remove(&block.page);
    try std.testing.expectEqual(@as(usize, 0), pool.flush_list.items.len);
    buf_mem_free();
}

test "buf read applies recv log lsn" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    const prev_dir = fil.fil_path_to_client_datadir;
    fil.fil_path_to_client_datadir = base;
    defer fil.fil_path_to_client_datadir = prev_dir;

    log_mod.log_var_init();
    log_mod.recv_sys_var_init();
    defer log_mod.recv_sys_var_init();
    try std.testing.expectEqual(compat.TRUE, log_mod.log_sys_init(base, 1, 4096, 0));
    defer log_mod.log_sys_close();

    fil.fil_init(0, 32);
    defer fil.fil_close();

    var space_id: ulint = 0;
    const create_err = fil.fil_create_new_single_table_tablespace(&space_id, "redo_apply", compat.FALSE, 0, 4);
    try std.testing.expectEqual(fil.DB_SUCCESS, create_err);

    var page_buf: [compat.UNIV_PAGE_SIZE]u8 = [_]u8{0} ** compat.UNIV_PAGE_SIZE;
    try std.testing.expectEqual(fil.DB_SUCCESS, fil.fil_write_page(space_id, 0, page_buf[0..].ptr));

    const lsn: u64 = 0x0102030405060708;
    var payload: [8]u8 = undefined;
    std.mem.writeInt(u64, payload[0..8], lsn, .big);
    const rec = log_mod.RedoRecord{
        .type_ = log_mod.LOG_REC_PAGE_LSN,
        .space = @as(u32, @intCast(space_id)),
        .page_no = 0,
        .payload = payload[0..],
    };
    var rec_buf: [64]u8 = undefined;
    const rec_len = try log_mod.redo_record_encode(rec_buf[0..], rec);
    _ = log_mod.log_append_bytes(rec_buf[0..rec_len]) orelse return error.UnexpectedNull;

    log_mod.recv_sys_create();
    log_mod.recv_sys_init(1024);
    defer log_mod.recv_sys_mem_free();
    try std.testing.expectEqual(compat.TRUE, log_mod.recv_scan_log_recs(1024));

    try std.testing.expectEqual(compat.TRUE, buf_read_page(space_id, 0, 0));
    var mtr = mtr_t{};
    const block = buf_page_get_gen(space_id, 0, 0, 0, null, BUF_GET_IF_IN_POOL, "", 0, &mtr) orelse return error.UnexpectedNull;
    const page_lsn = std.mem.readInt(u64, block.frame[fil.FIL_PAGE_LSN .. fil.FIL_PAGE_LSN + 8], .big);
    try std.testing.expectEqual(lsn, page_lsn);
    try std.testing.expectEqual(@as(ulint, 0), log_mod.recv_sys.?.n_addrs);

    buf_mem_free();
}

test "buf flush init writes lsn and checksums" {
    var page: [compat.UNIV_PAGE_SIZE]byte = undefined;
    @memset(page[0..], 0x5A);
    const lsn: u64 = 0x1122334455667788;
    buf_flush_init_for_writing(page[0..].ptr, null, lsn);

    const got_lsn = std.mem.readInt(u64, page[fil.FIL_PAGE_LSN .. fil.FIL_PAGE_LSN + 8], .big);
    try std.testing.expectEqual(lsn, got_lsn);

    const stored = std.mem.readInt(u32, page[fil.FIL_PAGE_SPACE_OR_CHKSUM .. fil.FIL_PAGE_SPACE_OR_CHKSUM + 4], .big);
    const computed = @as(u32, @intCast(buf_calc_page_new_checksum(page[0..].ptr)));
    try std.testing.expectEqual(computed, stored);

    const end_off = compat.UNIV_PAGE_SIZE - fil.FIL_PAGE_END_LSN_OLD_CHKSUM;
    const stored_old = std.mem.readInt(u32, page[end_off .. end_off + 4], .big);
    const computed_old = @as(u32, @intCast(buf_calc_page_old_checksum(page[0..].ptr)));
    try std.testing.expectEqual(computed_old, stored_old);
}
