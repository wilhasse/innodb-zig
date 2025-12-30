const std = @import("std");
const api = @import("../api/api.zig");

const Loops: usize = 10;

fn expectOk(err: api.ib_err_t) !void {
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
}

test "ib shutdown harness" {
    _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);

    var i: usize = 0;
    while (i < Loops) : (i += 1) {
        try expectOk(api.ib_init());

        const enable = (i % 2) == 0;
        try expectOk(api.ib_cfg_set("use_sys_malloc", enable));

        var read_back: bool = false;
        try expectOk(api.ib_cfg_get("use_sys_malloc", &read_back));
        try std.testing.expectEqual(enable, read_back);

        try expectOk(api.ib_shutdown(.IB_SHUTDOWN_NORMAL));
    }
}
