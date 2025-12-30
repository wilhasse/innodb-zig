const std = @import("std");
const compat = @import("../ut/compat.zig");

pub const module_name = "buf";

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;
pub const ib_uint64_t = compat.ib_uint64_t;
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
