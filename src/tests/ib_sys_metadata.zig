const std = @import("std");
const api = @import("../api/api.zig");
const dict = @import("../dict/mod.zig");

fn expectOk(err: api.ib_err_t) !void {
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
}

test "sys metadata tracks create/drop" {
    try expectOk(api.ib_init());
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
    try expectOk(api.ib_startup("barracuda"));

    var tbl_sch: api.ib_tbl_sch_t = null;
    try expectOk(api.ib_table_schema_create("db/sysmeta1", &tbl_sch, .IB_TBL_COMPACT, 0));
    defer api.ib_table_schema_delete(tbl_sch);

    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c1", .IB_INT, .IB_COL_NONE, 0, 4));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c2", .IB_VARCHAR, .IB_COL_NONE, 0, 10));

    var idx_sch: api.ib_idx_sch_t = null;
    try expectOk(api.ib_table_schema_add_index(tbl_sch, "PRIMARY", &idx_sch));
    try expectOk(api.ib_index_schema_add_col(idx_sch, "c1", 0));
    try expectOk(api.ib_index_schema_set_clustered(idx_sch));

    var sec_idx: api.ib_idx_sch_t = null;
    try expectOk(api.ib_table_schema_add_index(tbl_sch, "idx_c2", &sec_idx));
    try expectOk(api.ib_index_schema_add_col(sec_idx, "c2", 0));
    try expectOk(api.ib_index_schema_set_unique(sec_idx));

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);
    try expectOk(api.ib_schema_lock_exclusive(trx));
    var table_id: api.ib_id_t = 0;
    try expectOk(api.ib_table_create(trx, tbl_sch, &table_id));
    try expectOk(api.ib_trx_commit(trx));

    try std.testing.expect(dict.dict_sys_table_find_by_name("db/sysmeta1") != null);
    try std.testing.expectEqual(@as(dict.ulint, 2), dict.dict_sys_column_count_for_table_name("db/sysmeta1"));
    try std.testing.expectEqual(@as(dict.ulint, 2), dict.dict_sys_index_count_for_table_name("db/sysmeta1"));
    try std.testing.expect(dict.dict_sys_index_find_by_name("db/sysmeta1", "PRIMARY") != null);
    try std.testing.expect(dict.dict_sys_index_find_by_name("db/sysmeta1", "idx_c2") != null);

    var idx_id: api.ib_id_t = 0;
    try expectOk(api.ib_index_get_id("db/sysmeta1", "idx_c2", &idx_id));

    const trx_drop_idx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx_drop_idx);
    try expectOk(api.ib_schema_lock_exclusive(trx_drop_idx));
    try expectOk(api.ib_index_drop(trx_drop_idx, idx_id));
    try expectOk(api.ib_trx_commit(trx_drop_idx));

    try std.testing.expect(dict.dict_sys_index_find_by_name("db/sysmeta1", "idx_c2") == null);
    try std.testing.expectEqual(@as(dict.ulint, 1), dict.dict_sys_index_count_for_table_name("db/sysmeta1"));

    const trx_drop_tbl = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx_drop_tbl);
    try expectOk(api.ib_schema_lock_exclusive(trx_drop_tbl));
    try expectOk(api.ib_table_drop(trx_drop_tbl, "db/sysmeta1"));
    try expectOk(api.ib_trx_commit(trx_drop_tbl));

    try std.testing.expect(dict.dict_sys_table_find_by_name("db/sysmeta1") == null);
    try std.testing.expectEqual(@as(dict.ulint, 0), dict.dict_sys_column_count_for_table_name("db/sysmeta1"));
    try std.testing.expectEqual(@as(dict.ulint, 0), dict.dict_sys_index_count_for_table_name("db/sysmeta1"));
}
