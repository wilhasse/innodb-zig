const std = @import("std");
const errors = @import("../ut/errors.zig");
const que = @import("../que/mod.zig");
const core = @import("core.zig");
const types = @import("types.zig");

pub const module_name = "trx.xa";

pub const trx_t = types.trx_t;

pub const Xid = struct {
    format_id: u32 = 0,
    gtrid: []const u8 = "",
    bqual: []const u8 = "",
};

pub fn trx_xa_prepare(trx: *trx_t, xid: ?Xid) errors.DbErr {
    _ = xid;
    trx.conc_state = .prepared;
    return .DB_SUCCESS;
}

pub fn trx_xa_commit(trx: *trx_t, xid: ?Xid, one_phase: bool) errors.DbErr {
    _ = xid;
    _ = one_phase;
    trx.conc_state = .committed_in_memory;
    return .DB_SUCCESS;
}

pub fn trx_xa_rollback(trx: *trx_t, xid: ?Xid) errors.DbErr {
    _ = xid;
    trx.conc_state = .committed_in_memory;
    return .DB_SUCCESS;
}

pub fn trx_xa_recover(_: std.mem.Allocator) []const Xid {
    return &[_]Xid{};
}

test "trx xa prepare/commit stubs" {
    core.trx_var_init();
    var sess = que.sess_t{};
    const trx = core.trx_allocate_for_client(&sess, std.testing.allocator);
    defer core.trx_free(trx);

    core.trx_start(trx);
    try std.testing.expect(trx.conc_state == .active);

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, trx_xa_prepare(trx, null));
    try std.testing.expect(trx.conc_state == .prepared);

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, trx_xa_commit(trx, null, true));
    try std.testing.expect(trx.conc_state == .committed_in_memory);
}
