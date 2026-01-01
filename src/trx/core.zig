const std = @import("std");
const compat = @import("../ut/compat.zig");
const types = @import("types.zig");
const sys = @import("sys.zig");
const roll = @import("roll.zig");
const que = @import("../que/mod.zig");

pub const module_name = "trx.core";

pub const ulint = compat.ulint;

pub var trx_dummy_sess: ?*que.sess_t = null;
pub var trx_n_transactions: ulint = 0;

pub fn trx_var_init() void {
    trx_dummy_sess = null;
    trx_n_transactions = 0;
}

pub fn trx_is_strict(trx: *types.trx_t) bool {
    _ = trx;
    return false;
}

pub fn trx_set_detailed_error(trx: *types.trx_t, msg: []const u8) void {
    const max_len = trx.detailed_error.len;
    const copy_len = @min(max_len - 1, msg.len);
    std.mem.copyForwards(u8, trx.detailed_error[0..copy_len], msg[0..copy_len]);
    trx.detailed_error[copy_len] = 0;
}

pub fn trx_create(sess: *que.sess_t, allocator: std.mem.Allocator) *types.trx_t {
    const trx = allocator.create(types.trx_t) catch @panic("trx_create");
    trx.* = .{};
    trx.allocator = allocator;
    trx.sess = sess;
    trx.conc_state = .not_started;
    trx.isolation_level = types.TRX_ISO_REPEATABLE_READ;
    trx.start_time = std.time.timestamp();
    trx_n_transactions += 1;
    return trx;
}

pub fn trx_allocate_for_client(sess: *que.sess_t, allocator: std.mem.Allocator) *types.trx_t {
    return trx_create(sess, allocator);
}

pub fn trx_start(trx: *types.trx_t) void {
    const sys_ptr = sys.trx_sys_init_at_db_start(trx.allocator);
    trx.id = sys_ptr.max_trx_id;
    sys_ptr.max_trx_id += 1;
    trx.conc_state = .active;
    sys_ptr.trx_list.append(trx.allocator, trx) catch @panic("trx_start");
}

pub fn trx_commit(trx: *types.trx_t) void {
    if (sys.trx_sys) |sys_ptr| {
        var idx: usize = 0;
        while (idx < sys_ptr.trx_list.items.len) {
            if (sys_ptr.trx_list.items[idx] == trx) {
                _ = sys_ptr.trx_list.orderedRemove(sys_ptr.allocator, idx);
                break;
            }
            idx += 1;
        }
    }
    trx.conc_state = .committed_in_memory;
}

pub fn trx_free(trx: *types.trx_t) void {
    trx_commit(trx);
    roll.trx_deinit(trx);
    trx.allocator.destroy(trx);
    if (trx_n_transactions > 0) {
        trx_n_transactions -= 1;
    }
}

test "trx create/start/commit lifecycle" {
    trx_var_init();
    var sess = que.sess_t{};
    const trx = trx_allocate_for_client(&sess, std.testing.allocator);
    defer trx_free(trx);

    try std.testing.expect(trx.conc_state == .not_started);
    trx_start(trx);
    try std.testing.expect(trx.conc_state == .active);
    try std.testing.expect(sys.trx_in_trx_list(trx));

    trx_commit(trx);
    try std.testing.expect(trx.conc_state == .committed_in_memory);
    try std.testing.expect(!sys.trx_in_trx_list(trx));
}

test "trx detailed error set" {
    var sess = que.sess_t{};
    const trx = trx_allocate_for_client(&sess, std.testing.allocator);
    defer trx_free(trx);
    trx_set_detailed_error(trx, "bad");
    try std.testing.expectEqual(@as(u8, 'b'), trx.detailed_error[0]);
}
