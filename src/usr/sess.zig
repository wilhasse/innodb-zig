const std = @import("std");
const compat = @import("../ut/compat.zig");
const que = @import("../que/mod.zig");
const trx = @import("../trx/mod.zig");

pub const module_name = "usr.sess";

pub const ulint = compat.ulint;

pub const SESS_ACTIVE: ulint = 1;
pub const SESS_ERROR: ulint = 2;

pub fn sess_open(allocator: std.mem.Allocator) *que.sess_t {
    const sess = allocator.create(que.sess_t) catch @panic("sess_open");
    sess.* = .{};
    sess.state = SESS_ACTIVE;
    const trx_ptr = trx.core.trx_create(sess, allocator);
    sess.trx = @ptrCast(trx_ptr);
    return sess;
}

pub fn sess_close(sess: *que.sess_t, allocator: std.mem.Allocator) void {
    if (sess.trx) |ptr| {
        const trx_ptr = @as(*trx.trx_t, @ptrCast(@alignCast(ptr)));
        trx.core.trx_free(trx_ptr);
    }
    allocator.destroy(sess);
}

test "sess open/close lifecycle" {
    var sess = sess_open(std.testing.allocator);
    defer sess_close(sess, std.testing.allocator);
    try std.testing.expectEqual(SESS_ACTIVE, sess.state);
    try std.testing.expect(sess.trx != null);
}
