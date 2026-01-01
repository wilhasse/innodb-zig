const std = @import("std");
const compat = @import("../ut/compat.zig");

pub const module_name = "lock";

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;

pub const lock_mode = enum(u8) {
    LOCK_IS = 0,
    LOCK_IX,
    LOCK_S,
    LOCK_X,
    LOCK_AUTO_INC,
    LOCK_NONE,
    LOCK_NUM = LOCK_NONE,
};

pub const LOCK_IS: ulint = @intFromEnum(lock_mode.LOCK_IS);
pub const LOCK_IX: ulint = @intFromEnum(lock_mode.LOCK_IX);
pub const LOCK_S: ulint = @intFromEnum(lock_mode.LOCK_S);
pub const LOCK_X: ulint = @intFromEnum(lock_mode.LOCK_X);
pub const LOCK_AUTO_INC: ulint = @intFromEnum(lock_mode.LOCK_AUTO_INC);
pub const LOCK_NONE: ulint = @intFromEnum(lock_mode.LOCK_NONE);
pub const LOCK_NUM: ulint = @intFromEnum(lock_mode.LOCK_NUM);

pub const LOCK_MODE_MASK: ulint = 0x0F;
pub const LOCK_TABLE: ulint = 16;
pub const LOCK_REC: ulint = 32;
pub const LOCK_TYPE_MASK: ulint = 0xF0;
pub const LOCK_WAIT: ulint = 256;
pub const LOCK_ORDINARY: ulint = 0;
pub const LOCK_GAP: ulint = 512;
pub const LOCK_REC_NOT_GAP: ulint = 1024;
pub const LOCK_INSERT_INTENTION: ulint = 2048;

fn lock_mode_bit(mode1: ulint, mode2: ulint) ulint {
    const shift: u6 = @intCast(mode1 * LOCK_NUM + mode2);
    return @as(ulint, 1) << shift;
}

pub const LOCK_MODE_COMPATIBILITY: ulint = 0 |
    lock_mode_bit(LOCK_IS, LOCK_IS) | lock_mode_bit(LOCK_IX, LOCK_IX) | lock_mode_bit(LOCK_S, LOCK_S) |
    lock_mode_bit(LOCK_IX, LOCK_IS) | lock_mode_bit(LOCK_IS, LOCK_IX) |
    lock_mode_bit(LOCK_IS, LOCK_AUTO_INC) | lock_mode_bit(LOCK_AUTO_INC, LOCK_IS) |
    lock_mode_bit(LOCK_S, LOCK_IS) | lock_mode_bit(LOCK_IS, LOCK_S) |
    lock_mode_bit(LOCK_AUTO_INC, LOCK_IX) | lock_mode_bit(LOCK_IX, LOCK_AUTO_INC);

pub const LOCK_MODE_STRONGER_OR_EQ: ulint = 0 |
    lock_mode_bit(LOCK_IS, LOCK_IS) |
    lock_mode_bit(LOCK_IX, LOCK_IS) | lock_mode_bit(LOCK_IX, LOCK_IX) |
    lock_mode_bit(LOCK_S, LOCK_IS) | lock_mode_bit(LOCK_S, LOCK_S) |
    lock_mode_bit(LOCK_AUTO_INC, LOCK_AUTO_INC) |
    lock_mode_bit(LOCK_X, LOCK_IS) | lock_mode_bit(LOCK_X, LOCK_IX) | lock_mode_bit(LOCK_X, LOCK_S) |
    lock_mode_bit(LOCK_X, LOCK_AUTO_INC) | lock_mode_bit(LOCK_X, LOCK_X);

pub fn lock_mode_compatible(mode1: ulint, mode2: ulint) ibool {
    if (mode1 >= LOCK_NUM or mode2 >= LOCK_NUM) {
        return compat.FALSE;
    }
    return if ((LOCK_MODE_COMPATIBILITY & lock_mode_bit(mode1, mode2)) != 0) compat.TRUE else compat.FALSE;
}

pub fn lock_mode_stronger_or_eq(mode1: ulint, mode2: ulint) ibool {
    if (mode1 >= LOCK_NUM or mode2 >= LOCK_NUM) {
        return compat.FALSE;
    }
    return if ((LOCK_MODE_STRONGER_OR_EQ & lock_mode_bit(mode1, mode2)) != 0) compat.TRUE else compat.FALSE;
}

pub const lock_t = struct {
    type_mode: ulint = 0,
    prev: ?*lock_t = null,
    rec_bit: ulint = compat.ULINT_UNDEFINED,
};

pub const lock_queue_iterator_t = struct {
    current_lock: *const lock_t,
    bit_no: ulint = compat.ULINT_UNDEFINED,
};

pub const lock_sys_t = struct {
    rec_hash: ?*anyopaque = null,
};

pub var lock_sys: ?*lock_sys_t = null;

pub fn lock_get_type_low(lock: *const lock_t) ulint {
    return lock.type_mode & LOCK_TYPE_MASK;
}

pub fn lock_get_mode(lock: *const lock_t) ulint {
    return lock.type_mode & LOCK_MODE_MASK;
}

pub fn lock_is_wait(lock: *const lock_t) ibool {
    return if ((lock.type_mode & LOCK_WAIT) != 0) compat.TRUE else compat.FALSE;
}

pub fn lock_rec_find_set_bit(lock: *const lock_t) ulint {
    return lock.rec_bit;
}

pub fn lock_rec_get_prev(lock: *const lock_t, heap_no: ulint) ?*const lock_t {
    _ = heap_no;
    return lock.prev;
}

pub fn lock_var_init() void {
    if (lock_sys != null) {
        return;
    }
    const sys = std.heap.page_allocator.create(lock_sys_t) catch return;
    sys.* = .{};
    lock_sys = sys;
}

pub fn lock_sys_close() void {
    if (lock_sys) |sys| {
        std.heap.page_allocator.destroy(sys);
        lock_sys = null;
    }
}

pub fn lock_queue_iterator_reset(iter: *lock_queue_iterator_t, lock: *const lock_t, bit_no: ulint) void {
    iter.current_lock = lock;

    if (bit_no != compat.ULINT_UNDEFINED) {
        iter.bit_no = bit_no;
        return;
    }

    switch (lock_get_type_low(lock)) {
        LOCK_TABLE => iter.bit_no = compat.ULINT_UNDEFINED,
        LOCK_REC => {
            iter.bit_no = lock_rec_find_set_bit(lock);
            std.debug.assert(iter.bit_no != compat.ULINT_UNDEFINED);
        },
        else => @panic("invalid lock type"),
    }
}

pub fn lock_queue_iterator_get_prev(iter: *lock_queue_iterator_t) ?*const lock_t {
    const prev_lock: ?*const lock_t = switch (lock_get_type_low(iter.current_lock)) {
        LOCK_REC => lock_rec_get_prev(iter.current_lock, iter.bit_no),
        LOCK_TABLE => iter.current_lock.prev,
        else => @panic("invalid lock type"),
    };

    if (prev_lock) |prev| {
        iter.current_lock = prev;
    }

    return prev_lock;
}

test "lock iterator over record locks" {
    var lock0 = lock_t{ .type_mode = LOCK_REC, .rec_bit = 5 };
    var lock1 = lock_t{ .type_mode = LOCK_REC, .rec_bit = 5, .prev = &lock0 };

    var iter = lock_queue_iterator_t{ .current_lock = &lock1 };
    lock_queue_iterator_reset(&iter, &lock1, compat.ULINT_UNDEFINED);
    try std.testing.expect(iter.bit_no == 5);

    const prev = lock_queue_iterator_get_prev(&iter);
    try std.testing.expect(prev == &lock0);
    try std.testing.expect(iter.current_lock == &lock0);
    try std.testing.expect(lock_queue_iterator_get_prev(&iter) == null);
}

test "lock iterator over table locks" {
    var lock0 = lock_t{ .type_mode = LOCK_TABLE };
    var lock1 = lock_t{ .type_mode = LOCK_TABLE, .prev = &lock0 };

    var iter = lock_queue_iterator_t{ .current_lock = &lock1, .bit_no = 7 };
    lock_queue_iterator_reset(&iter, &lock1, compat.ULINT_UNDEFINED);
    try std.testing.expect(iter.bit_no == compat.ULINT_UNDEFINED);

    const prev = lock_queue_iterator_get_prev(&iter);
    try std.testing.expect(prev == &lock0);
    try std.testing.expect(iter.current_lock == &lock0);

    lock_queue_iterator_reset(&iter, &lock1, 9);
    try std.testing.expect(iter.bit_no == 9);
}

test "lock mode helpers and sys init" {
    var lock = lock_t{ .type_mode = LOCK_TABLE | LOCK_WAIT | 3 };
    try std.testing.expect(lock_get_type_low(&lock) == LOCK_TABLE);
    try std.testing.expect(lock_get_mode(&lock) == 3);
    try std.testing.expect(lock_is_wait(&lock) == compat.TRUE);

    lock_sys_close();
    try std.testing.expect(lock_sys == null);
    lock_var_init();
    try std.testing.expect(lock_sys != null);
    lock_sys_close();
    try std.testing.expect(lock_sys == null);
}

test "lock mode compatibility matrix" {
    try std.testing.expect(lock_mode_compatible(LOCK_IS, LOCK_IS) == compat.TRUE);
    try std.testing.expect(lock_mode_compatible(LOCK_IS, LOCK_IX) == compat.TRUE);
    try std.testing.expect(lock_mode_compatible(LOCK_S, LOCK_IS) == compat.TRUE);
    try std.testing.expect(lock_mode_compatible(LOCK_S, LOCK_X) == compat.FALSE);
    try std.testing.expect(lock_mode_compatible(LOCK_X, LOCK_S) == compat.FALSE);
    try std.testing.expect(lock_mode_compatible(LOCK_AUTO_INC, LOCK_IX) == compat.TRUE);
    try std.testing.expect(lock_mode_compatible(LOCK_AUTO_INC, LOCK_AUTO_INC) == compat.FALSE);
}

test "lock mode stronger or equal matrix" {
    try std.testing.expect(lock_mode_stronger_or_eq(LOCK_X, LOCK_S) == compat.TRUE);
    try std.testing.expect(lock_mode_stronger_or_eq(LOCK_IX, LOCK_IS) == compat.TRUE);
    try std.testing.expect(lock_mode_stronger_or_eq(LOCK_S, LOCK_IX) == compat.FALSE);
    try std.testing.expect(lock_mode_stronger_or_eq(LOCK_AUTO_INC, LOCK_IS) == compat.FALSE);
}
