const std = @import("std");
const api = @import("../api/api.zig");
const dict = @import("../dict/mod.zig");
const compat = @import("../ut/compat.zig");

test "sys tables bootstrapped on startup" {
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_init());
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_startup("barracuda"));

    try std.testing.expect(dict.dict_table_get("SYS_TABLES", compat.FALSE) != null);
    try std.testing.expect(dict.dict_table_get("SYS_COLUMNS", compat.FALSE) != null);
    try std.testing.expect(dict.dict_table_get("SYS_INDEXES", compat.FALSE) != null);
}
