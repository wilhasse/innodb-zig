const std = @import("std");
const sync = @import("mod.zig");

var mutex_count: usize = 0;

pub fn sync_var_init() void {
    mutex_count = 0;
}

pub fn sync_create_mutex() sync.Mutex {
    mutex_count += 1;
    return sync.Mutex.init();
}

pub fn sync_get_mutex_count() usize {
    return mutex_count;
}

test "sync core mutex count" {
    sync_var_init();
    _ = sync_create_mutex();
    _ = sync_create_mutex();
    try std.testing.expectEqual(@as(usize, 2), sync_get_mutex_count());
}
