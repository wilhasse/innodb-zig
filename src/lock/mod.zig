const std = @import("std");
const compat = @import("../ut/compat.zig");
const dict = @import("../dict/mod.zig");
const page = @import("../page/mod.zig");
const trx_types = @import("../trx/types.zig");
const errors = @import("../ut/errors.zig");

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
};

pub const LOCK_IS: ulint = @intFromEnum(lock_mode.LOCK_IS);
pub const LOCK_IX: ulint = @intFromEnum(lock_mode.LOCK_IX);
pub const LOCK_S: ulint = @intFromEnum(lock_mode.LOCK_S);
pub const LOCK_X: ulint = @intFromEnum(lock_mode.LOCK_X);
pub const LOCK_AUTO_INC: ulint = @intFromEnum(lock_mode.LOCK_AUTO_INC);
pub const LOCK_NONE: ulint = @intFromEnum(lock_mode.LOCK_NONE);
pub const LOCK_NUM: ulint = @intFromEnum(lock_mode.LOCK_NONE);

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
    trx: ?*trx_types.trx_t = null,
    type_mode: ulint = 0,
    prev: ?*lock_t = null,
    next: ?*lock_t = null,
    index: ?*dict.dict_index_t = null,
    table_id: u64 = 0,
    space: ulint = 0,
    page_no: ulint = 0,
    rec_bit: ulint = compat.ULINT_UNDEFINED,
    wait_for: ?*lock_t = null,
};

pub const lock_queue_iterator_t = struct {
    current_lock: *const lock_t,
    bit_no: ulint = compat.ULINT_UNDEFINED,
};

pub const lock_queue_t = struct {
    head: ?*lock_t = null,
    tail: ?*lock_t = null,
};

const LockRecKey = struct {
    space: ulint,
    page_no: ulint,
    rec_offset: ulint,
};

pub const lock_sys_t = struct {
    rec_hash: std.AutoHashMap(LockRecKey, lock_queue_t),
    table_hash: std.AutoHashMap(u64, lock_queue_t),
    trx_hash: std.AutoHashMap(*trx_types.trx_t, std.ArrayListUnmanaged(*lock_t)),
    waits_for: std.AutoHashMap(*trx_types.trx_t, *trx_types.trx_t),
    allocator: std.mem.Allocator,
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

fn lock_queue_append(queue: *lock_queue_t, lock: *lock_t) void {
    lock.prev = queue.tail;
    lock.next = null;
    if (queue.tail) |tail| {
        tail.next = lock;
    } else {
        queue.head = lock;
    }
    queue.tail = lock;
}

fn lock_queue_remove(queue: *lock_queue_t, lock: *lock_t) void {
    if (lock.prev) |prev| {
        prev.next = lock.next;
    } else {
        queue.head = lock.next;
    }
    if (lock.next) |next| {
        next.prev = lock.prev;
    } else {
        queue.tail = lock.prev;
    }
    lock.prev = null;
    lock.next = null;
}

fn lock_rec_queue_add(sys: *lock_sys_t, lock: *lock_t) void {
    const key = LockRecKey{
        .space = lock.space,
        .page_no = lock.page_no,
        .rec_offset = lock.rec_bit,
    };
    const entry = sys.rec_hash.getOrPut(key) catch return;
    if (!entry.found_existing) {
        entry.value_ptr.* = .{};
    }
    lock_queue_append(entry.value_ptr, lock);
}

fn lock_rec_queue_remove(sys: *lock_sys_t, lock: *lock_t) void {
    const key = LockRecKey{
        .space = lock.space,
        .page_no = lock.page_no,
        .rec_offset = lock.rec_bit,
    };
    if (sys.rec_hash.getPtr(key)) |queue| {
        lock_queue_remove(queue, lock);
        if (queue.head == null) {
            _ = sys.rec_hash.remove(key);
        }
    }
}

fn lock_rec_key_from(index: *dict.dict_index_t, rec: *page.rec_t) ?LockRecKey {
    const page_ptr = rec.page orelse return null;
    return .{
        .space = index.space,
        .page_no = page_ptr.page_no,
        .rec_offset = rec.rec_page_offset,
    };
}

pub fn lock_var_init() void {
    if (lock_sys != null) {
        return;
    }
    lock_sys_create(0);
}

pub fn lock_sys_close() void {
    if (lock_sys) |sys| {
        var it = sys.trx_hash.valueIterator();
        while (it.next()) |list| {
            list.deinit(sys.allocator);
        }
        sys.rec_hash.deinit();
        sys.table_hash.deinit();
        sys.trx_hash.deinit();
        sys.waits_for.deinit();
        std.heap.page_allocator.destroy(sys);
        lock_sys = null;
    }
}

pub fn lock_sys_create(n_cells: ulint) void {
    _ = n_cells;
    if (lock_sys != null) {
        return;
    }
    const allocator = std.heap.page_allocator;
    const sys = allocator.create(lock_sys_t) catch return;
    sys.* = .{
        .rec_hash = std.AutoHashMap(LockRecKey, lock_queue_t).init(allocator),
        .table_hash = std.AutoHashMap(u64, lock_queue_t).init(allocator),
        .trx_hash = std.AutoHashMap(*trx_types.trx_t, std.ArrayListUnmanaged(*lock_t)).init(allocator),
        .waits_for = std.AutoHashMap(*trx_types.trx_t, *trx_types.trx_t).init(allocator),
        .allocator = allocator,
    };
    lock_sys = sys;
}

fn lock_waits_for_set(sys: *lock_sys_t, waiter: *trx_types.trx_t, blocker: *trx_types.trx_t) void {
    _ = sys.waits_for.put(waiter, blocker) catch {};
}

fn lock_waits_for_clear(sys: *lock_sys_t, waiter: *trx_types.trx_t) void {
    _ = sys.waits_for.remove(waiter);
}

fn lock_deadlock_detect(sys: *lock_sys_t, start: *trx_types.trx_t, blocking: *trx_types.trx_t) bool {
    var current = blocking;
    var steps: usize = 0;
    while (steps < 1024) : (steps += 1) {
        if (current == start) {
            return true;
        }
        const next = sys.waits_for.get(current) orelse return false;
        current = next;
    }
    return false;
}

fn lock_trx_list_add(sys: *lock_sys_t, trx: *trx_types.trx_t, lock: *lock_t) void {
    const entry = sys.trx_hash.getOrPut(trx) catch return;
    if (!entry.found_existing) {
        entry.value_ptr.* = .{};
    }
    entry.value_ptr.append(sys.allocator, lock) catch {};
}

fn lock_trx_list_remove(sys: *lock_sys_t, trx: *trx_types.trx_t, lock: *lock_t) void {
    if (sys.trx_hash.getPtr(trx)) |list| {
        var idx: usize = 0;
        while (idx < list.items.len) {
            if (list.items[idx] == lock) {
                _ = list.orderedRemove(idx);
                break;
            }
            idx += 1;
        }
        if (list.items.len == 0) {
            list.deinit(sys.allocator);
            _ = sys.trx_hash.remove(trx);
        }
    }
}

fn lock_table_queue_add(sys: *lock_sys_t, lock: *lock_t) void {
    const entry = sys.table_hash.getOrPut(lock.table_id) catch return;
    if (!entry.found_existing) {
        entry.value_ptr.* = .{};
    }
    lock_queue_append(entry.value_ptr, lock);
}

fn lock_table_queue_remove(sys: *lock_sys_t, lock: *lock_t) void {
    if (sys.table_hash.getPtr(lock.table_id)) |queue| {
        lock_queue_remove(queue, lock);
        if (queue.head == null) {
            _ = sys.table_hash.remove(lock.table_id);
        }
    }
}

fn lock_table_create(sys: *lock_sys_t, trx: *trx_types.trx_t, table_id: u64, type_mode: ulint) ?*lock_t {
    const lock = sys.allocator.create(lock_t) catch return null;
    lock.* = .{
        .trx = trx,
        .type_mode = type_mode | LOCK_TABLE,
        .table_id = table_id,
    };
    lock_table_queue_add(sys, lock);
    lock_trx_list_add(sys, trx, lock);
    return lock;
}

fn lock_table_remove(sys: *lock_sys_t, lock: *lock_t) void {
    if (lock_is_wait(lock) == compat.TRUE) {
        if (lock.trx) |trx| {
            lock_waits_for_clear(sys, trx);
        }
    }
    lock_table_queue_remove(sys, lock);
    if (lock.trx) |trx| {
        lock_trx_list_remove(sys, trx, lock);
    }
    sys.allocator.destroy(lock);
}

fn lock_table_has(sys: *lock_sys_t, trx: *trx_types.trx_t, table_id: u64, mode: ulint) bool {
    const queue = sys.table_hash.getPtr(table_id) orelse return false;
    var current = queue.tail;
    while (current) |lock| {
        if (lock.trx == trx and lock_mode_stronger_or_eq(lock_get_mode(lock), mode) == compat.TRUE) {
            return true;
        }
        current = lock.prev;
    }
    return false;
}

fn lock_table_other_has_incompatible(
    sys: *lock_sys_t,
    trx: *trx_types.trx_t,
    table_id: u64,
    mode: ulint,
    include_wait: bool,
) ?*lock_t {
    const queue = sys.table_hash.getPtr(table_id) orelse return null;
    var current = queue.tail;
    while (current) |lock| {
        if (lock.trx != trx and lock_mode_compatible(lock_get_mode(lock), mode) == compat.FALSE) {
            if (include_wait or lock_is_wait(lock) == compat.FALSE) {
                return lock;
            }
        }
        current = lock.prev;
    }
    return null;
}

pub fn lock_table(trx: *trx_types.trx_t, table_id: u64, mode: ulint) errors.DbErr {
    if (mode >= LOCK_NUM) {
        return .DB_ERROR;
    }
    lock_sys_create(0);
    const sys = lock_sys orelse return .DB_ERROR;

    if (lock_table_has(sys, trx, table_id, mode)) {
        return .DB_SUCCESS;
    }

    if (lock_table_other_has_incompatible(sys, trx, table_id, mode, true)) |conflict| {
        const lock = lock_table_create(sys, trx, table_id, mode | LOCK_WAIT) orelse return .DB_OUT_OF_MEMORY;
        lock.wait_for = conflict;
        if (conflict.trx) |blocking| {
            lock_waits_for_set(sys, trx, blocking);
            if (lock_deadlock_detect(sys, trx, blocking)) {
                lock_table_remove(sys, lock);
                return .DB_DEADLOCK;
            }
        }
        return .DB_LOCK_WAIT;
    }

    _ = lock_table_create(sys, trx, table_id, mode) orelse return .DB_OUT_OF_MEMORY;
    return .DB_SUCCESS;
}

pub fn lock_table_release(trx: *trx_types.trx_t, table_id: u64) void {
    const sys = lock_sys orelse return;
    const queue = sys.table_hash.getPtr(table_id) orelse return;
    var current = queue.head;
    while (current) |lock| {
        const next = lock.next;
        if (lock.trx == trx) {
            lock_table_remove(sys, lock);
        }
        current = next;
    }
}

fn lock_rec_create(sys: *lock_sys_t, trx: *trx_types.trx_t, index: *dict.dict_index_t, rec: *page.rec_t, type_mode: ulint) ?*lock_t {
    const key = lock_rec_key_from(index, rec) orelse return null;
    const lock = sys.allocator.create(lock_t) catch return null;
    lock.* = .{
        .trx = trx,
        .type_mode = type_mode | LOCK_REC,
        .index = index,
        .space = key.space,
        .page_no = key.page_no,
        .rec_bit = key.rec_offset,
    };
    lock_rec_queue_add(sys, lock);
    lock_trx_list_add(sys, trx, lock);
    return lock;
}

fn lock_rec_remove(sys: *lock_sys_t, lock: *lock_t) void {
    if (lock_is_wait(lock) == compat.TRUE) {
        if (lock.trx) |trx| {
            lock_waits_for_clear(sys, trx);
        }
    }
    lock_rec_queue_remove(sys, lock);
    if (lock.trx) |trx| {
        lock_trx_list_remove(sys, trx, lock);
    }
    sys.allocator.destroy(lock);
}

fn lock_rec_has(sys: *lock_sys_t, trx: *trx_types.trx_t, key: LockRecKey, mode: ulint) bool {
    const queue = sys.rec_hash.getPtr(key) orelse return false;
    var current = queue.tail;
    while (current) |lock| {
        if (lock.trx == trx and lock_mode_stronger_or_eq(lock_get_mode(lock), mode) == compat.TRUE) {
            return true;
        }
        current = lock.prev;
    }
    return false;
}

fn lock_rec_other_has_incompatible(
    sys: *lock_sys_t,
    trx: *trx_types.trx_t,
    key: LockRecKey,
    mode: ulint,
    include_wait: bool,
) ?*lock_t {
    const queue = sys.rec_hash.getPtr(key) orelse return null;
    var current = queue.tail;
    while (current) |lock| {
        if (lock.trx != trx and lock_mode_compatible(lock_get_mode(lock), mode) == compat.FALSE) {
            if (include_wait or lock_is_wait(lock) == compat.FALSE) {
                return lock;
            }
        }
        current = lock.prev;
    }
    return null;
}

pub fn lock_rec(trx: *trx_types.trx_t, index: *dict.dict_index_t, rec: *page.rec_t, mode: ulint) errors.DbErr {
    if (mode >= LOCK_NUM) {
        return .DB_ERROR;
    }
    const key = lock_rec_key_from(index, rec) orelse return .DB_ERROR;
    lock_sys_create(0);
    const sys = lock_sys orelse return .DB_ERROR;

    if (lock_rec_has(sys, trx, key, mode)) {
        return .DB_SUCCESS;
    }

    if (lock_rec_other_has_incompatible(sys, trx, key, mode, true)) |conflict| {
        const lock = lock_rec_create(sys, trx, index, rec, mode | LOCK_WAIT) orelse return .DB_OUT_OF_MEMORY;
        lock.wait_for = conflict;
        if (conflict.trx) |blocking| {
            lock_waits_for_set(sys, trx, blocking);
            if (lock_deadlock_detect(sys, trx, blocking)) {
                lock_rec_remove(sys, lock);
                return .DB_DEADLOCK;
            }
        }
        return .DB_LOCK_WAIT;
    }

    _ = lock_rec_create(sys, trx, index, rec, mode) orelse return .DB_OUT_OF_MEMORY;
    return .DB_SUCCESS;
}

pub fn lock_rec_release(trx: *trx_types.trx_t, index: *dict.dict_index_t, rec: *page.rec_t) void {
    const key = lock_rec_key_from(index, rec) orelse return;
    const sys = lock_sys orelse return;
    const queue = sys.rec_hash.getPtr(key) orelse return;
    var current = queue.head;
    while (current) |lock| {
        const next = lock.next;
        if (lock.trx == trx) {
            lock_rec_remove(sys, lock);
        }
        current = next;
    }
}

pub fn lock_trx_release(trx: *trx_types.trx_t) void {
    const sys = lock_sys orelse return;
    while (sys.trx_hash.getPtr(trx)) |list| {
        if (list.items.len == 0) {
            list.deinit(sys.allocator);
            _ = sys.trx_hash.remove(trx);
            break;
        }
        const lock = list.items[0];
        switch (lock_get_type_low(lock)) {
            LOCK_TABLE => lock_table_remove(sys, lock),
            LOCK_REC => lock_rec_remove(sys, lock),
            else => {
                lock_trx_list_remove(sys, trx, lock);
                sys.allocator.destroy(lock);
            },
        }
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

test "lock sys hash tables init" {
    lock_sys_close();
    lock_sys_create(0);
    const sys = lock_sys orelse return error.OutOfMemory;
    try std.testing.expect(sys.rec_hash.count() == 0);
    try std.testing.expect(sys.table_hash.count() == 0);

    const key = LockRecKey{ .space = 1, .page_no = 2, .rec_offset = 3 };
    try sys.rec_hash.put(key, .{});
    try sys.table_hash.put(42, .{});
    try std.testing.expect(sys.rec_hash.count() == 1);
    try std.testing.expect(sys.table_hash.count() == 1);

    _ = sys.rec_hash.remove(key);
    _ = sys.table_hash.remove(42);
    lock_sys_close();
}

test "record lock queue append/remove" {
    lock_sys_close();
    lock_sys_create(0);
    const sys = lock_sys orelse return error.OutOfMemory;

    var lock1 = lock_t{ .type_mode = LOCK_REC, .space = 1, .page_no = 2, .rec_bit = 7 };
    var lock2 = lock_t{ .type_mode = LOCK_REC, .space = 1, .page_no = 2, .rec_bit = 7 };
    lock_rec_queue_add(sys, &lock1);
    lock_rec_queue_add(sys, &lock2);

    const key = LockRecKey{ .space = 1, .page_no = 2, .rec_offset = 7 };
    const queue = sys.rec_hash.getPtr(key) orelse return error.OutOfMemory;
    try std.testing.expect(queue.head == &lock1);
    try std.testing.expect(queue.tail == &lock2);
    try std.testing.expect(lock_rec_get_prev(&lock2, 0) == &lock1);

    lock_rec_queue_remove(sys, &lock1);
    try std.testing.expect(queue.head == &lock2);
    try std.testing.expect(queue.tail == &lock2);

    lock_rec_queue_remove(sys, &lock2);
    try std.testing.expect(sys.rec_hash.count() == 0);
    lock_sys_close();
}

test "table lock request and release" {
    lock_sys_close();
    lock_sys_create(0);

    var trx1 = trx_types.trx_t{};
    var trx2 = trx_types.trx_t{};

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, lock_table(&trx1, 11, LOCK_IX));
    try std.testing.expectEqual(errors.DbErr.DB_LOCK_WAIT, lock_table(&trx2, 11, LOCK_S));

    lock_table_release(&trx1, 11);
    lock_table_release(&trx2, 11);

    const sys = lock_sys orelse return error.OutOfMemory;
    try std.testing.expect(sys.table_hash.count() == 0);
    lock_sys_close();
}

test "deadlock detection for table locks" {
    lock_sys_close();
    lock_sys_create(0);

    var trx1 = trx_types.trx_t{};
    var trx2 = trx_types.trx_t{};

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, lock_table(&trx1, 1, LOCK_X));
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, lock_table(&trx2, 2, LOCK_X));
    try std.testing.expectEqual(errors.DbErr.DB_LOCK_WAIT, lock_table(&trx1, 2, LOCK_X));
    try std.testing.expectEqual(errors.DbErr.DB_DEADLOCK, lock_table(&trx2, 1, LOCK_X));

    lock_table_release(&trx1, 1);
    lock_table_release(&trx1, 2);
    lock_table_release(&trx2, 2);
    lock_sys_close();
}

test "record lock request and release" {
    lock_sys_close();
    lock_sys_create(0);

    var trx1 = trx_types.trx_t{};
    var trx2 = trx_types.trx_t{};
    var index = dict.dict_index_t{ .space = 3 };
    var page_obj = page.page_t{ .page_no = 7 };
    var rec = page.rec_t{ .page = &page_obj, .rec_page_offset = 128 };

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, lock_rec(&trx1, &index, &rec, LOCK_X));
    try std.testing.expectEqual(errors.DbErr.DB_LOCK_WAIT, lock_rec(&trx2, &index, &rec, LOCK_S));

    lock_rec_release(&trx1, &index, &rec);
    lock_rec_release(&trx2, &index, &rec);

    const sys = lock_sys orelse return error.OutOfMemory;
    try std.testing.expect(sys.rec_hash.count() == 0);
    lock_sys_close();
}
