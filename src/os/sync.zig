const std = @import("std");
const compat = @import("../ut/compat.zig");

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;
pub const ib_int64_t = compat.ib_int64_t;

pub const OS_SYNC_INFINITE_TIME: ulint = std.math.maxInt(ulint);
pub const OS_SYNC_TIME_EXCEEDED: ulint = 1;

pub const os_fast_mutex_t = std.Thread.Mutex;
pub const os_event_t = *OsEvent;
pub const os_mutex_t = *OsMutex;

pub var os_sync_mutex: ?os_mutex_t = null;
pub var os_thread_count: ulint = 0;
pub var os_event_count: ulint = 0;
pub var os_mutex_count: ulint = 0;
pub var os_fast_mutex_count: ulint = 0;

var os_sync_mutex_inited: bool = false;
var os_sync_free_called: bool = false;

var event_list: std.ArrayListUnmanaged(os_event_t) = .{};
var mutex_list: std.ArrayListUnmanaged(os_mutex_t) = .{};
var list_mutex: std.Thread.Mutex = .{};

const OsEvent = struct {
    mutex: std.Thread.Mutex = .{},
    cond: std.Thread.Condition = .{},
    is_set: bool = false,
    signal_count: ib_int64_t = 1,
};

const OsMutex = struct {
    mutex: std.Thread.Mutex = .{},
    owner: ?std.Thread.Id = null,
    count: ulint = 0,
    event: ?os_event_t = null,
};

fn addEvent(event: os_event_t) void {
    list_mutex.lock();
    defer list_mutex.unlock();
    event_list.append(std.heap.page_allocator, event) catch return;
    os_event_count += 1;
}

fn removeEvent(event: os_event_t) void {
    list_mutex.lock();
    defer list_mutex.unlock();
    for (event_list.items, 0..) |item, idx| {
        if (item == event) {
            _ = event_list.orderedRemove(idx);
            os_event_count -= 1;
            break;
        }
    }
}

fn addMutex(mutex: os_mutex_t) void {
    list_mutex.lock();
    defer list_mutex.unlock();
    mutex_list.append(std.heap.page_allocator, mutex) catch return;
    os_mutex_count += 1;
}

fn removeMutex(mutex: os_mutex_t) void {
    list_mutex.lock();
    defer list_mutex.unlock();
    for (mutex_list.items, 0..) |item, idx| {
        if (item == mutex) {
            _ = mutex_list.orderedRemove(idx);
            os_mutex_count -= 1;
            break;
        }
    }
}

pub fn os_sync_var_init() void {
    os_sync_mutex = null;
    os_sync_mutex_inited = false;
    os_sync_free_called = false;
    os_thread_count = 0;

    list_mutex.lock();
    event_list.deinit(std.heap.page_allocator);
    mutex_list.deinit(std.heap.page_allocator);
    event_list = .{};
    mutex_list = .{};
    os_event_count = 0;
    os_mutex_count = 0;
    os_fast_mutex_count = 0;
    list_mutex.unlock();
}

pub fn os_sync_init() void {
    os_sync_var_init();
    os_sync_mutex = os_mutex_create(null);
    os_sync_mutex_inited = os_sync_mutex != null;
}

pub fn os_sync_free() void {
    os_sync_free_called = true;

    list_mutex.lock();
    const events = event_list;
    const mutexes = mutex_list;
    event_list = .{};
    mutex_list = .{};
    os_event_count = 0;
    os_mutex_count = 0;
    list_mutex.unlock();

    for (events.items) |event| {
        std.heap.page_allocator.destroy(event);
    }
    events.deinit(std.heap.page_allocator);

    for (mutexes.items) |mutex| {
        std.heap.page_allocator.destroy(mutex);
    }
    mutexes.deinit(std.heap.page_allocator);

    os_sync_mutex = null;
    os_sync_mutex_inited = false;
    os_sync_free_called = false;
}

pub fn os_event_create(name: ?[]const u8) os_event_t {
    _ = name;
    const event = std.heap.page_allocator.create(OsEvent) catch @panic("os_event_create");
    event.* = .{};
    addEvent(event);
    return event;
}

pub fn os_event_set(event: os_event_t) void {
    if (@intFromPtr(event) == 0) return;
    event.mutex.lock();
    defer event.mutex.unlock();
    if (!event.is_set) {
        event.is_set = true;
        event.signal_count += 1;
        event.cond.broadcast();
    }
}

pub fn os_event_reset(event: os_event_t) ib_int64_t {
    if (@intFromPtr(event) == 0) return 0;
    event.mutex.lock();
    defer event.mutex.unlock();
    event.is_set = false;
    return event.signal_count;
}

pub fn os_event_free(event: os_event_t) void {
    if (@intFromPtr(event) == 0) return;
    if (!os_sync_free_called) {
        removeEvent(event);
    }
    std.heap.page_allocator.destroy(event);
}

pub fn os_event_wait_low(event: os_event_t, reset_sig_count: ib_int64_t) void {
    if (@intFromPtr(event) == 0) return;
    event.mutex.lock();
    defer event.mutex.unlock();
    while (!event.is_set and (reset_sig_count == 0 or event.signal_count == reset_sig_count)) {
        event.cond.wait(&event.mutex);
    }
}

pub inline fn os_event_wait(event: os_event_t) void {
    os_event_wait_low(event, 0);
}

pub fn os_event_wait_time(event: os_event_t, time: ulint) ulint {
    if (@intFromPtr(event) == 0) return 0;
    if (time == OS_SYNC_INFINITE_TIME) {
        os_event_wait_low(event, 0);
        return 0;
    }

    event.mutex.lock();
    defer event.mutex.unlock();

    if (event.is_set) {
        return 0;
    }

    const timeout_ns = @as(u64, @intCast(time)) * std.time.ns_per_us;
    event.cond.timedWait(&event.mutex, timeout_ns) catch return OS_SYNC_TIME_EXCEEDED;
    return if (event.is_set) 0 else OS_SYNC_TIME_EXCEEDED;
}

pub fn os_mutex_create(name: ?[]const u8) os_mutex_t {
    _ = name;
    const mutex = std.heap.page_allocator.create(OsMutex) catch @panic("os_mutex_create");
    mutex.* = .{};
    addMutex(mutex);
    return mutex;
}

pub fn os_mutex_enter(mutex: os_mutex_t) void {
    if (@intFromPtr(mutex) == 0) return;
    std.debug.assert(mutex.owner == null or mutex.owner.? != std.Thread.getCurrentId());
    mutex.mutex.lock();
    mutex.owner = std.Thread.getCurrentId();
    mutex.count += 1;
}

pub fn os_mutex_exit(mutex: os_mutex_t) void {
    if (@intFromPtr(mutex) == 0) return;
    std.debug.assert(mutex.owner != null);
    std.debug.assert(mutex.owner.? == std.Thread.getCurrentId());
    if (mutex.count > 0) {
        mutex.count -= 1;
    }
    if (mutex.count == 0) {
        mutex.owner = null;
    }
    mutex.mutex.unlock();
}

pub fn os_mutex_free(mutex: os_mutex_t) void {
    if (@intFromPtr(mutex) == 0) return;
    if (!os_sync_free_called) {
        removeMutex(mutex);
    }
    std.heap.page_allocator.destroy(mutex);
}

pub fn os_fast_mutex_trylock(fast_mutex: *os_fast_mutex_t) ulint {
    return if (fast_mutex.tryLock()) 0 else 1;
}

pub fn os_fast_mutex_unlock(fast_mutex: *os_fast_mutex_t) void {
    fast_mutex.unlock();
}

pub fn os_fast_mutex_init(fast_mutex: *os_fast_mutex_t) void {
    fast_mutex.* = .{};
    os_fast_mutex_count += 1;
}

pub fn os_fast_mutex_lock(fast_mutex: *os_fast_mutex_t) void {
    fast_mutex.lock();
}

pub fn os_fast_mutex_free(fast_mutex: *os_fast_mutex_t) void {
    _ = fast_mutex;
    if (os_fast_mutex_count > 0) {
        os_fast_mutex_count -= 1;
    }
}

test "os sync event wait" {
    os_sync_var_init();
    const event = os_event_create(null);
    defer os_event_free(event);

    _ = os_event_reset(event);

    const worker = struct {
        fn run(ev: os_event_t) void {
            std.Thread.sleep(50 * std.time.ns_per_ms);
            os_event_set(ev);
        }
    };

    const thread = try std.Thread.spawn(.{}, worker.run, .{event});
    const res = os_event_wait_time(event, 200_000);
    thread.join();
    try std.testing.expectEqual(@as(ulint, 0), res);
}

test "os sync mutex and fast mutex" {
    os_sync_var_init();
    const mutex = os_mutex_create(null);
    defer os_mutex_free(mutex);

    os_mutex_enter(mutex);
    os_mutex_exit(mutex);

    var fast: os_fast_mutex_t = .{};
    os_fast_mutex_init(&fast);
    try std.testing.expectEqual(@as(ulint, 0), os_fast_mutex_trylock(&fast));
    try std.testing.expect(os_fast_mutex_trylock(&fast) != 0);
    os_fast_mutex_unlock(&fast);
    os_fast_mutex_free(&fast);
}
