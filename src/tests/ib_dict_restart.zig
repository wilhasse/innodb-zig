const std = @import("std");
const api = @import("../api/api.zig");
const dict = @import("../dict/mod.zig");
const compat = @import("../ut/compat.zig");

fn expectOk(err: api.ib_err_t) !void {
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
}

fn createTable() !void {
    var tbl_sch: api.ib_tbl_sch_t = null;
    try expectOk(api.ib_table_schema_create("db/restart1", &tbl_sch, .IB_TBL_COMPACT, 0));
    defer api.ib_table_schema_delete(tbl_sch);

    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c1", .IB_INT, .IB_COL_NONE, 0, 4));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c2", .IB_VARCHAR, .IB_COL_NONE, 0, 10));

    var idx_sch: api.ib_idx_sch_t = null;
    try expectOk(api.ib_table_schema_add_index(tbl_sch, "PRIMARY", &idx_sch));
    try expectOk(api.ib_index_schema_add_col(idx_sch, "c1", 0));
    try expectOk(api.ib_index_schema_set_clustered(idx_sch));

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);
    try expectOk(api.ib_schema_lock_exclusive(trx));
    var table_id: api.ib_id_t = 0;
    try expectOk(api.ib_table_create(trx, tbl_sch, &table_id));
    try expectOk(api.ib_trx_commit(trx));
}

test "dict cache reloads from sys metadata" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);
    const data_home = try std.fmt.allocPrint(std.testing.allocator, "{s}/", .{base});
    defer std.testing.allocator.free(data_home);

    try expectOk(api.ib_init());
    try expectOk(api.ib_cfg_set("data_home_dir", data_home));
    try expectOk(api.ib_startup("barracuda"));

    try createTable();

    _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);

    try expectOk(api.ib_init());
    try expectOk(api.ib_cfg_set("data_home_dir", data_home));
    try expectOk(api.ib_startup("barracuda"));

    try std.testing.expect(dict.dict_sys_table_find_by_name("db/restart1") != null);
    try std.testing.expectEqual(@as(dict.ulint, 2), dict.dict_sys_column_count_for_table_name("db/restart1"));
    try std.testing.expectEqual(@as(dict.ulint, 1), dict.dict_sys_index_count_for_table_name("db/restart1"));

    const cached = dict.dict_table_get("db/restart1", compat.FALSE) orelse return error.TestExpectedEqual;
    try std.testing.expect(dict.dict_table_get_first_index(cached) != null);

    _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
}
