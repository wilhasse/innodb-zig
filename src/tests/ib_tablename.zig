const std = @import("std");
const api = @import("../api/api.zig");

const invalid_names = [_][]const u8{
    "",
    "a",
    "ab",
    ".",
    "./",
    "../",
    "/",
    "/aaaaa",
    "/a/a",
    "abcdef/",
};

fn expectOk(err: api.ib_err_t) !void {
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
}

test "ib tablename validation" {
    _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
    try expectOk(api.ib_init());
    try expectOk(api.ib_startup("barracuda"));
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);

    var tbl_sch: api.ib_tbl_sch_t = null;
    for (invalid_names) |name| {
        try std.testing.expectEqual(api.ib_err_t.DB_DATA_MISMATCH, api.ib_table_schema_create(name, &tbl_sch, .IB_TBL_COMPACT, 0));
        try std.testing.expectEqual(@as(api.ib_tbl_sch_t, null), tbl_sch);
    }

    try expectOk(api.ib_table_schema_create("a/b", &tbl_sch, .IB_TBL_COMPACT, 0));
    api.ib_table_schema_delete(tbl_sch);
}
