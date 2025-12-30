const std = @import("std");
const api = @import("../api/api.zig");
const log = @import("../ut/log.zig");

fn expectOk(err: api.ib_err_t) !void {
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
}

test "ib logger harness" {
    try expectOk(api.ib_init());
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);

    api.ib_logger_set(log.nullLogger, null);
    try expectOk(api.ib_startup("barracuda"));
}
