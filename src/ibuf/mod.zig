const std = @import("std");
const compat = @import("../ut/compat.zig");
const buf = @import("../buf/mod.zig");
const dict = @import("../dict/mod.zig");

pub const module_name = "ibuf";

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;
pub const byte = compat.byte;

pub const buf_block_t = buf.buf_block_t;
pub const mtr_t = struct {};

pub const IBUF_BITS_PER_PAGE: ulint = 4;
pub const IBUF_BITMAP: ulint = 0;
pub const IBUF_POOL_SIZE_PER_MAX_SIZE: ulint = 2;
pub const IBUF_TABLE_NAME: []const u8 = "SYS_IBUF_TABLE";
pub const IBUF_PAGE_SIZE_PER_FREE_SPACE: ulint = 32;
pub const FSP_IBUF_BITMAP_OFFSET: ulint = 1;

pub const ibuf_use_t = enum(u8) {
    IBUF_USE_NONE = 0,
    IBUF_USE_INSERT = 1,
    IBUF_USE_COUNT = 2,
};

pub const ibuf_t = struct {
    size: ulint = 0,
    max_size: ulint = 0,
    seg_size: ulint = 0,
    empty: ibool = compat.TRUE,
    free_list_len: ulint = 0,
    height: ulint = 0,
    index: ?*dict.dict_index_t = null,
    n_inserts: ulint = 0,
    n_merges: ulint = 0,
    n_merged_recs: ulint = 0,
};

pub var ibuf_use: ibuf_use_t = .IBUF_USE_INSERT;
pub var ibuf: ?*ibuf_t = null;
pub var ibuf_flush_count: ulint = 0;

const FreeBitsKey = struct {
    space: ulint,
    page_no: ulint,
};

var ibuf_free_bits: std.AutoHashMap(FreeBitsKey, u8) = undefined;
var ibuf_free_bits_inited = false;

fn ibuf_free_bits_map() *std.AutoHashMap(FreeBitsKey, u8) {
    if (!ibuf_free_bits_inited) {
        ibuf_free_bits = std.AutoHashMap(FreeBitsKey, u8).init(std.heap.page_allocator);
        ibuf_free_bits_inited = true;
    }
    return &ibuf_free_bits;
}

fn ibuf_free_bits_get(space: ulint, page_no: ulint) u8 {
    if (!ibuf_free_bits_inited) {
        return 0;
    }
    return ibuf_free_bits.get(.{ .space = space, .page_no = page_no }) orelse 0;
}

fn ibuf_free_bits_set(space: ulint, page_no: ulint, bits: u8) void {
    const map = ibuf_free_bits_map();
    _ = map.put(.{ .space = space, .page_no = page_no }, bits) catch {};
}

fn ibuf_free_bits_clear() void {
    if (ibuf_free_bits_inited) {
        ibuf_free_bits.clearAndFree();
    }
}

fn ibuf_ensure() *ibuf_t {
    if (ibuf == null) {
        const ptr = std.heap.page_allocator.create(ibuf_t) catch @panic("ibuf_ensure");
        ptr.* = .{};
        ibuf = ptr;
    }
    return ibuf.?;
}

pub fn ibuf_init_at_db_start() void {
    _ = ibuf_free_bits_map();
    _ = ibuf_ensure();
}

pub fn ibuf_update_max_tablespace_id() void {}

pub fn ibuf_bitmap_page_init(block: *buf_block_t, mtr: *mtr_t) void {
    ibuf_free_bits_set(block.page.space, block.page.page_no, 0);
    _ = mtr;
}

pub fn ibuf_reset_free_bits(block: *buf_block_t) void {
    ibuf_free_bits_set(block.page.space, block.page.page_no, 0);
}

pub fn ibuf_update_free_bits_if_full(
    block: *buf_block_t,
    max_ins_size: ulint,
    increase: ulint,
) void {
    const current = ibuf_free_bits_get(block.page.space, block.page.page_no);
    if (current == 0 and increase > 0) {
        const bits = ibuf_index_page_calc_free_bits(block.zip_size, max_ins_size + increase);
        ibuf_free_bits_set(block.page.space, block.page.page_no, @intCast(bits));
    }
}

pub fn ibuf_update_free_bits_low(block: *const buf_block_t, max_ins_size: ulint, mtr: *mtr_t) void {
    const bits = ibuf_index_page_calc_free_bits(block.zip_size, max_ins_size);
    ibuf_free_bits_set(block.page.space, block.page.page_no, @intCast(bits));
    _ = mtr;
}

pub fn ibuf_update_free_bits_zip(block: *buf_block_t, mtr: *mtr_t) void {
    ibuf_update_free_bits_low(block, compat.UNIV_PAGE_SIZE, mtr);
}

pub fn ibuf_update_free_bits_for_two_pages_low(
    zip_size: ulint,
    block1: *buf_block_t,
    block2: *buf_block_t,
    mtr: *mtr_t,
) void {
    _ = zip_size;
    ibuf_update_free_bits_low(block1, compat.UNIV_PAGE_SIZE, mtr);
    ibuf_update_free_bits_low(block2, compat.UNIV_PAGE_SIZE, mtr);
}

pub fn ibuf_should_try(index: *dict.dict_index_t, ignore_sec_unique: ulint) ibool {
    if (ibuf_use != .IBUF_USE_NONE and dict.dict_index_is_clust(index) == 0 and
        (ignore_sec_unique != 0 or dict.dict_index_is_unique(index) == 0))
    {
        ibuf_flush_count += 1;
        if (ibuf_flush_count % 4 == 0) {
            buf.buf_LRU_try_free_flushed_blocks();
        }
        return compat.TRUE;
    }
    return compat.FALSE;
}

pub fn ibuf_inside() ibool {
    return compat.FALSE;
}

pub fn ibuf_merge_or_delete_for_page(space: ulint, page_no: ulint, merged_recs: ulint) ibool {
    const state = ibuf_ensure();
    state.n_merges += 1;
    state.n_merged_recs += merged_recs;
    ibuf_free_bits_set(space, page_no, 0);
    return compat.TRUE;
}

pub fn ibuf_bitmap_page(zip_size: ulint, page_no: ulint) ibool {
    std.debug.assert(compat.ut_is_2pow(zip_size));
    if (zip_size == 0) {
        return if ((page_no & (compat.UNIV_PAGE_SIZE - 1)) == FSP_IBUF_BITMAP_OFFSET)
            compat.TRUE
        else
            compat.FALSE;
    }
    return if ((page_no & (zip_size - 1)) == FSP_IBUF_BITMAP_OFFSET) compat.TRUE else compat.FALSE;
}

pub fn ibuf_index_page_calc_free_bits(zip_size: ulint, max_ins_size: ulint) ulint {
    std.debug.assert(compat.ut_is_2pow(zip_size));
    if (zip_size != 0) {
        std.debug.assert(zip_size > IBUF_PAGE_SIZE_PER_FREE_SPACE);
    }
    std.debug.assert(zip_size <= compat.UNIV_PAGE_SIZE);

    const denom = if (zip_size != 0)
        (zip_size / IBUF_PAGE_SIZE_PER_FREE_SPACE)
    else
        (compat.UNIV_PAGE_SIZE / IBUF_PAGE_SIZE_PER_FREE_SPACE);
    var n = max_ins_size / denom;
    if (n == 3) {
        n = 2;
    }
    if (n > 3) {
        n = 3;
    }
    return n;
}

pub fn ibuf_index_page_calc_free_from_bits(zip_size: ulint, bits: ulint) ulint {
    std.debug.assert(bits < 4);
    std.debug.assert(compat.ut_is_2pow(zip_size));
    if (zip_size != 0) {
        std.debug.assert(zip_size > IBUF_PAGE_SIZE_PER_FREE_SPACE);
    }
    std.debug.assert(zip_size <= compat.UNIV_PAGE_SIZE);

    if (zip_size != 0) {
        if (bits == 3) {
            return 4 * zip_size / IBUF_PAGE_SIZE_PER_FREE_SPACE;
        }
        return bits * zip_size / IBUF_PAGE_SIZE_PER_FREE_SPACE;
    }

    if (bits == 3) {
        return 4 * compat.UNIV_PAGE_SIZE / IBUF_PAGE_SIZE_PER_FREE_SPACE;
    }
    return bits * compat.UNIV_PAGE_SIZE / IBUF_PAGE_SIZE_PER_FREE_SPACE;
}

test "ibuf_should_try respects index flags and ibuf_use" {
    const saved_use = ibuf_use;
    const saved_count = ibuf_flush_count;
    defer {
        ibuf_use = saved_use;
        ibuf_flush_count = saved_count;
    }

    var index = dict.dict_index_t{};
    ibuf_use = .IBUF_USE_INSERT;
    ibuf_flush_count = 0;
    try std.testing.expect(ibuf_should_try(&index, 0) == compat.TRUE);
    try std.testing.expect(ibuf_flush_count == 1);

    ibuf_use = .IBUF_USE_NONE;
    try std.testing.expect(ibuf_should_try(&index, 0) == compat.FALSE);
    try std.testing.expect(ibuf_flush_count == 1);

    ibuf_use = .IBUF_USE_INSERT;
    index.type = dict.DICT_CLUSTERED;
    try std.testing.expect(ibuf_should_try(&index, 0) == compat.FALSE);

    index.type = dict.DICT_UNIQUE;
    try std.testing.expect(ibuf_should_try(&index, 0) == compat.FALSE);
    try std.testing.expect(ibuf_should_try(&index, 1) == compat.TRUE);
}

test "ibuf bitmap and free space helpers" {
    try std.testing.expect(ibuf_bitmap_page(0, 1) == compat.TRUE);
    try std.testing.expect(ibuf_bitmap_page(0, 2) == compat.FALSE);
    try std.testing.expect(ibuf_bitmap_page(8192, 1) == compat.TRUE);

    const denom = compat.UNIV_PAGE_SIZE / IBUF_PAGE_SIZE_PER_FREE_SPACE;
    try std.testing.expect(ibuf_index_page_calc_free_bits(0, denom * 3) == 2);
    try std.testing.expect(ibuf_index_page_calc_free_bits(0, denom * 4) == 3);
    try std.testing.expect(ibuf_index_page_calc_free_from_bits(0, 2) == 2 * denom);
    try std.testing.expect(ibuf_index_page_calc_free_from_bits(0, 3) == 4 * denom);
}

test "ibuf free bits tracking and merge" {
    ibuf_free_bits_clear();
    ibuf_free_bits_inited = false;
    ibuf = null;

    const allocator = std.testing.allocator;
    const frame = try allocator.alloc(byte, compat.UNIV_PAGE_SIZE);
    defer allocator.free(frame);

    var block = buf_block_t{
        .frame = frame,
        .page = .{ .space = 1, .page_no = 5 },
        .zip_size = 0,
    };
    var mtr = mtr_t{};

    ibuf_bitmap_page_init(&block, &mtr);
    try std.testing.expectEqual(@as(u8, 0), ibuf_free_bits_get(1, 5));

    const expected = ibuf_index_page_calc_free_bits(0, compat.UNIV_PAGE_SIZE / 2);
    ibuf_update_free_bits_low(&block, compat.UNIV_PAGE_SIZE / 2, &mtr);
    try std.testing.expectEqual(@as(u8, @intCast(expected)), ibuf_free_bits_get(1, 5));

    ibuf_reset_free_bits(&block);
    try std.testing.expectEqual(@as(u8, 0), ibuf_free_bits_get(1, 5));
    ibuf_update_free_bits_if_full(&block, 0, compat.UNIV_PAGE_SIZE);
    try std.testing.expect(ibuf_free_bits_get(1, 5) > 0);

    const state = ibuf_ensure();
    state.n_merges = 0;
    state.n_merged_recs = 0;
    try std.testing.expectEqual(compat.TRUE, ibuf_merge_or_delete_for_page(1, 5, 7));
    try std.testing.expectEqual(@as(ulint, 1), state.n_merges);
    try std.testing.expectEqual(@as(ulint, 7), state.n_merged_recs);
    try std.testing.expectEqual(@as(u8, 0), ibuf_free_bits_get(1, 5));
}
