const std = @import("std");
const os_thread = @import("../os/thread.zig");

pub const module_name = "sync";

pub const Mutex = struct {
    inner: std.Thread.Mutex = .{},

    pub fn init() Mutex {
        return .{};
    }

    pub fn lock(self: *Mutex) void {
        self.inner.lock();
    }

    pub fn unlock(self: *Mutex) void {
        self.inner.unlock();
    }

    pub fn tryLock(self: *Mutex) bool {
        return self.inner.tryLock();
    }
};

pub const RwLock = struct {
    inner: std.Thread.RwLock = .{},

    pub fn init() RwLock {
        return .{};
    }

    pub fn lockShared(self: *RwLock) void {
        self.inner.lockShared();
    }

    pub fn unlockShared(self: *RwLock) void {
        self.inner.unlockShared();
    }

    pub fn lock(self: *RwLock) void {
        self.inner.lock();
    }

    pub fn unlock(self: *RwLock) void {
        self.inner.unlock();
    }
};

pub const CondVar = struct {
    inner: std.Thread.Condition = .{},

    pub fn init() CondVar {
        return .{};
    }

    pub fn wait(self: *CondVar, mutex: *Mutex) void {
        self.inner.wait(&mutex.inner);
    }

    pub fn signal(self: *CondVar) void {
        self.inner.signal();
    }

    pub fn broadcast(self: *CondVar) void {
        self.inner.broadcast();
    }
};

test "mutex lock/unlock" {
    var mutex = Mutex.init();
    mutex.lock();
    try std.testing.expect(!mutex.tryLock());
    mutex.unlock();
    try std.testing.expect(mutex.tryLock());
    mutex.unlock();
}

test "rwlock shared/exclusive" {
    var rw = RwLock.init();
    rw.lockShared();
    rw.unlockShared();
    rw.lock();
    rw.unlock();
}

test "condvar wait/signal" {
    var mutex = Mutex.init();
    var cond = CondVar.init();
    var ready = false;

    const worker = struct {
        fn run(m: *Mutex, c: *CondVar, flag: *bool) void {
            os_thread.sleepMicros(500);
            m.lock();
            flag.* = true;
            c.signal();
            m.unlock();
        }
    };

    const thread = try os_thread.spawn(worker.run, .{ &mutex, &cond, &ready });

    mutex.lock();
    while (!ready) {
        cond.wait(&mutex);
    }
    mutex.unlock();

    thread.join();
    try std.testing.expect(ready);
}
