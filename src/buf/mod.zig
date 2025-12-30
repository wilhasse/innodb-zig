const std = @import("std");
const compat = @import("../ut/compat.zig");

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
    return BUF_BUDDY_LOW << @as(usize, @intCast(capped));
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

pub const buf_page_t = struct {
    state: buf_page_state = .BUF_BLOCK_NOT_USED,
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
    _ = page;
    return 0;
}

pub fn buf_calc_page_old_checksum(page: [*]const byte) ulint {
    _ = page;
    return 0;
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

pub fn buf_pool_init() ?*buf_pool_t {
    if (buf_pool != null) {
        return buf_pool;
    }
    const pool = std.heap.page_allocator.create(buf_pool_t) catch return null;
    pool.* = .{};
    buf_pool = pool;
    return pool;
}

pub fn buf_close() void {}

pub fn buf_mem_free() void {
    if (buf_pool) |pool| {
        std.heap.page_allocator.destroy(pool);
        buf_pool = null;
    }
}

pub fn buf_pool_drop_hash_index() void {}

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
    _ = bpage;
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
    _ = space;
    _ = offset;
    return null;
}

pub fn buf_page_reset_file_page_was_freed(space: ulint, offset: ulint) ?*buf_page_t {
    _ = space;
    _ = offset;
    return null;
}

pub fn buf_page_get_zip(space: ulint, zip_size: ulint, offset: ulint) ?*buf_page_t {
    _ = space;
    _ = zip_size;
    _ = offset;
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
    _ = space;
    _ = zip_size;
    _ = offset;
    _ = rw_latch;
    _ = guess;
    _ = mode;
    _ = file;
    _ = line;
    _ = mtr;
    return null;
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
    _ = block;
    _ = modify_clock;
    _ = file;
    _ = line;
    _ = mtr;
    return compat.FALSE;
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
    _ = block;
    _ = mode;
    _ = file;
    _ = line;
    _ = mtr;
    return compat.FALSE;
}

pub fn buf_page_try_get_func(
    space_id: ulint,
    page_no: ulint,
    file: []const u8,
    line: ulint,
    mtr: *mtr_t,
) ?*const buf_block_t {
    _ = space_id;
    _ = page_no;
    _ = file;
    _ = line;
    _ = mtr;
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
    _ = space;
    _ = offset;
    _ = mtr;
    return buf_block_alloc(zip_size);
}

pub fn buf_page_io_complete(bpage: *buf_page_t) void {
    _ = bpage;
}

pub fn buf_pool_invalidate() void {}

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

pub fn buf_flush_remove(bpage: *buf_page_t) void {
    _ = bpage;
}

pub fn buf_flush_write_complete(bpage: *buf_page_t) void {
    _ = bpage;
}

pub fn buf_flush_free_margin() void {}

pub fn buf_flush_init_for_writing(page: [*]byte, page_zip_: ?*anyopaque, newest_lsn: ib_uint64_t) void {
    _ = page;
    _ = page_zip_;
    _ = newest_lsn;
}

pub fn buf_flush_batch(flush_type: buf_flush, min_n: ulint, lsn_limit: ib_uint64_t) ulint {
    _ = flush_type;
    _ = min_n;
    _ = lsn_limit;
    return 0;
}

pub fn buf_flush_wait_batch_end(flush_type: buf_flush) void {
    _ = flush_type;
}

pub fn buf_flush_ready_for_replace(bpage: *buf_page_t) ibool {
    _ = bpage;
    return compat.TRUE;
}

pub fn buf_flush_stat_update() void {}

pub fn buf_flush_get_desired_flush_rate() ulint {
    return 0;
}

pub fn buf_flush_validate() ibool {
    return compat.TRUE;
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

test "buf flush stubs" {
    var page = buf_page_t{};
    try std.testing.expectEqual(compat.TRUE, buf_flush_ready_for_replace(&page));
    try std.testing.expectEqual(@as(ulint, 0), buf_flush_get_desired_flush_rate());
    try std.testing.expectEqual(@as(ulint, 0), buf_flush_batch(.BUF_FLUSH_LRU, 0, 0));
}
