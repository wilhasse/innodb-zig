const std = @import("std");
const os_thread = @import("../os/thread.zig");

pub const module_name = "srv.master";

pub const SrvTask = struct {
    func: *const fn (?*anyopaque) void,
    ctx: ?*anyopaque = null,
};

pub const DEFAULT_TICK_US: u64 = 100_000;

pub const SrvMaster = struct {
    allocator: std.mem.Allocator,
    mutex: std.Thread.Mutex = .{},
    tasks: std.ArrayList(SrvTask),
    stop_flag: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    thread: ?os_thread.Thread = null,
    tick_us: u64 = DEFAULT_TICK_US,

    pub fn init(allocator: std.mem.Allocator, tick_us: u64) SrvMaster {
        return .{
            .allocator = allocator,
            .tasks = std.ArrayList(SrvTask).init(allocator),
            .tick_us = tick_us,
        };
    }

    pub fn deinit(self: *SrvMaster) void {
        self.stop();
        self.tasks.deinit();
    }

    pub fn start(self: *SrvMaster) !void {
        if (self.thread != null) {
            return;
        }
        self.stop_flag.store(false, .seq_cst);
        self.thread = try os_thread.spawn(run, .{self});
    }

    pub fn stop(self: *SrvMaster) void {
        if (self.thread) |thread| {
            self.stop_flag.store(true, .seq_cst);
            thread.join();
            self.thread = null;
        }
    }

    pub fn enqueue(self: *SrvMaster, task: SrvTask) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.tasks.append(task) catch @panic("SrvMaster.enqueue");
    }

    pub fn pendingCount(self: *SrvMaster) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.tasks.items.len;
    }

    fn run(self: *SrvMaster) void {
        while (!self.stop_flag.load(.seq_cst)) {
            while (true) {
                var task_opt: ?SrvTask = null;
                self.mutex.lock();
                if (self.tasks.items.len > 0) {
                    task_opt = self.tasks.orderedRemove(0);
                }
                self.mutex.unlock();

                if (task_opt) |task| {
                    task.func(task.ctx);
                } else {
                    break;
                }
            }
            os_thread.sleepMicros(self.tick_us);
        }
    }
};

test "srv master executes tasks" {
    var master = SrvMaster.init(std.testing.allocator, 1_000);
    defer master.deinit();
    try master.start();
    defer master.stop();

    var counter = std.atomic.Value(u32).init(0);

    const Task = struct {
        fn run(ctx: ?*anyopaque) void {
            const ptr = @as(*std.atomic.Value(u32), @ptrCast(@alignCast(ctx.?)));
            _ = ptr.fetchAdd(1, .seq_cst);
        }
    };

    master.enqueue(.{ .func = Task.run, .ctx = &counter });

    var attempts: usize = 0;
    while (attempts < 100 and counter.load(.seq_cst) == 0) : (attempts += 1) {
        std.Thread.sleep(1 * std.time.ns_per_ms);
    }

    try std.testing.expectEqual(@as(u32, 1), counter.load(.seq_cst));
    try std.testing.expectEqual(@as(usize, 0), master.pendingCount());
}
