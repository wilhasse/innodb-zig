const std = @import("std");
const compat = @import("../ut/compat.zig");

pub const module_name = "lock";

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;

pub const LOCK_TABLE: ulint = 16;
pub const LOCK_REC: ulint = 32;
pub const LOCK_TYPE_MASK: ulint = 0xF0;

pub const lock_t = struct {
    type_mode: ulint = 0,
    prev: ?*lock_t = null,
    rec_bit: ulint = compat.ULINT_UNDEFINED,
};

pub const lock_queue_iterator_t = struct {
    current_lock: *const lock_t,
    bit_no: ulint = compat.ULINT_UNDEFINED,
};

pub fn lock_get_type_low(lock: *const lock_t) ulint {
    return lock.type_mode & LOCK_TYPE_MASK;
}

pub fn lock_rec_find_set_bit(lock: *const lock_t) ulint {
    return lock.rec_bit;
}

pub fn lock_rec_get_prev(lock: *const lock_t, heap_no: ulint) ?*const lock_t {
    _ = heap_no;
    return lock.prev;
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
