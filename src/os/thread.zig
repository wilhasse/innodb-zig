const std = @import("std");
const builtin = @import("builtin");
const compat = @import("../ut/compat.zig");
const os_sync = @import("sync.zig");

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;

pub const ThreadId = std.Thread.Id;
pub const os_thread_id_t = ThreadId;
pub const os_thread_t = ThreadId;
pub const os_posix_f_t = *const fn (?*anyopaque) callconv(.c) ?*anyopaque;

pub const Thread = struct {
    handle: std.Thread,

    pub fn join(self: Thread) void {
        self.handle.join();
    }
};

pub fn spawn(comptime start: anytype, args: anytype) !Thread {
    return .{ .handle = try std.Thread.spawn(.{}, start, args) };
}

pub fn currentId() ThreadId {
    return std.Thread.getCurrentId();
}

pub fn yield() void {
    std.Thread.yield();
}

pub fn sleepMicros(us: u64) void {
    std.Thread.sleep(us * std.time.ns_per_us);
}

pub fn os_thread_eq(a: os_thread_id_t, b: os_thread_id_t) ibool {
    return if (a == b) compat.TRUE else compat.FALSE;
}

pub fn os_thread_pf(a: os_thread_id_t) ulint {
    return @as(ulint, @intCast(a));
}

pub fn os_thread_get_curr_id() os_thread_id_t {
    return std.Thread.getCurrentId();
}

pub fn os_thread_get_curr() os_thread_t {
    return os_thread_get_curr_id();
}

pub fn os_thread_create(start_f: os_posix_f_t, arg: ?*anyopaque, thread_id: ?*os_thread_id_t) os_thread_t {
    var id_store = std.atomic.Value(os_thread_id_t).init(0);
    var ready = std.Thread.ResetEvent{};

    const Context = struct {
        start: os_posix_f_t,
        arg: ?*anyopaque,
        id_store: *std.atomic.Value(os_thread_id_t),
        ready: *std.Thread.ResetEvent,
    };

    const Wrapper = struct {
        fn run(ctx: Context) void {
            ctx.id_store.store(std.Thread.getCurrentId(), .seq_cst);
            ctx.ready.set();
            _ = ctx.start(ctx.arg);
            os_thread_exit(null);
        }
    };

    if (os_sync.os_sync_mutex) |mutex| {
        os_sync.os_mutex_enter(mutex);
        os_sync.os_thread_count += 1;
        os_sync.os_mutex_exit(mutex);
    } else {
        os_sync.os_thread_count += 1;
    }

    const ctx = Context{
        .start = start_f,
        .arg = arg,
        .id_store = &id_store,
        .ready = &ready,
    };

    const thread = std.Thread.spawn(.{}, Wrapper.run, .{ctx}) catch @panic("os_thread_create failed");
    ready.wait();
    const id = id_store.load(.seq_cst);
    if (thread_id) |out| {
        out.* = id;
    }
    thread.detach();
    return id;
}

pub fn os_thread_exit(exit_value: ?*anyopaque) noreturn {
    if (os_sync.os_sync_mutex) |mutex| {
        os_sync.os_mutex_enter(mutex);
        if (os_sync.os_thread_count > 0) {
            os_sync.os_thread_count -= 1;
        }
        os_sync.os_mutex_exit(mutex);
    } else if (os_sync.os_thread_count > 0) {
        os_sync.os_thread_count -= 1;
    }

    if (builtin.os.tag == .windows) {
        const code: u32 = if (exit_value) |ptr| @as(u32, @intFromPtr(ptr)) else 0;
        std.os.windows.kernel32.ExitThread(code);
        unreachable;
    }
    if (builtin.os.tag == .linux) {
        const code: i32 = if (exit_value) |ptr| @as(i32, @intCast(@intFromPtr(ptr) & 0xff)) else 0;
        std.os.linux.exit(code);
    }
    @panic("os_thread_exit unsupported on this OS");
}

pub fn os_thread_yield() void {
    std.Thread.yield();
}

pub fn os_thread_sleep(tm: ulint) void {
    std.Thread.sleep(@as(u64, tm) * std.time.ns_per_us);
}

pub fn os_thread_set_priority(handle: os_thread_t, pri: ulint) void {
    _ = handle;
    _ = pri;
}

pub fn os_thread_get_priority(handle: os_thread_t) ulint {
    _ = handle;
    return 0;
}

pub fn os_thread_get_last_error() ulint {
    return 0;
}

test "thread spawn/join updates flag" {
    var flag = std.atomic.Value(u32).init(0);

    const worker = struct {
        fn run(ptr: *std.atomic.Value(u32)) void {
            ptr.store(1, .seq_cst);
        }
    };

    const thread = try spawn(worker.run, .{&flag});
    thread.join();

    try std.testing.expectEqual(@as(u32, 1), flag.load(.seq_cst));
}

test "os thread create sets id and runs" {
    var flag = std.atomic.Value(u32).init(0);
    var done = std.Thread.ResetEvent{};

    const Context = struct {
        flag: *std.atomic.Value(u32),
        done: *std.Thread.ResetEvent,
    };

    const worker = struct {
        fn run(arg: ?*anyopaque) callconv(.c) ?*anyopaque {
            const ctx = @as(*Context, @ptrCast(@alignCast(arg.?)));
            ctx.flag.store(1, .seq_cst);
            ctx.done.set();
            return null;
        }
    };

    var ctx = Context{ .flag = &flag, .done = &done };
    var tid: os_thread_id_t = 0;
    _ = os_thread_create(worker.run, @ptrCast(&ctx), &tid);

    done.wait();
    try std.testing.expectEqual(@as(u32, 1), flag.load(.seq_cst));
    try std.testing.expect(tid != 0);
}

test "os thread eq and pf" {
    const id = os_thread_get_curr_id();
    try std.testing.expectEqual(compat.TRUE, os_thread_eq(id, id));
    try std.testing.expect(os_thread_pf(id) != 0);
}
