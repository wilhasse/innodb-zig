const std = @import("std");

threadlocal var tls_value: ?usize = null;

pub fn thr_local_set(val: usize) void {
    tls_value = val;
}

pub fn thr_local_get() ?usize {
    return tls_value;
}

test "thr local set/get" {
    thr_local_set(123);
    try std.testing.expectEqual(@as(usize, 123), thr_local_get().?);
}
