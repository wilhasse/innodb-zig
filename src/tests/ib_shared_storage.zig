const std = @import("std");
const api = @import("../api/api.zig");
const compat = @import("../ut/compat.zig");

const DATABASE: []const u8 = "shared_db";
const TABLE_NAME: []const u8 = DATABASE ++ "/t1";

fn expectOk(err: api.ib_err_t) !void {
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
}

fn createTable() !void {
    var tbl_sch: api.ib_tbl_sch_t = null;
    var idx_sch: api.ib_idx_sch_t = null;

    try std.testing.expectEqual(compat.IB_TRUE, api.ib_database_create(DATABASE));
    try expectOk(api.ib_table_schema_create(TABLE_NAME, &tbl_sch, .IB_TBL_COMPACT, 0));
    defer api.ib_table_schema_delete(tbl_sch);

    try expectOk(api.ib_table_schema_add_col(
        tbl_sch,
        "id",
        .IB_INT,
        .IB_COL_UNSIGNED,
        0,
        @sizeOf(api.ib_u32_t),
    ));

    try expectOk(api.ib_table_schema_add_index(tbl_sch, "PRIMARY", &idx_sch));
    try expectOk(api.ib_index_schema_add_col(idx_sch, "id", 0));
    try expectOk(api.ib_index_schema_set_clustered(idx_sch));

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);
    try expectOk(api.ib_schema_lock_exclusive(trx));
    var table_id: api.ib_id_t = 0;
    try expectOk(api.ib_table_create(trx, tbl_sch, &table_id));
    try expectOk(api.ib_trx_commit(trx));
}

fn dropTable() !void {
    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);
    try expectOk(api.ib_schema_lock_exclusive(trx));
    try expectOk(api.ib_table_drop(trx, TABLE_NAME));
    try expectOk(api.ib_trx_commit(trx));
    try expectOk(api.ib_database_drop(DATABASE));
}

test "shared table storage across cursors" {
    try expectOk(api.ib_init());
    try expectOk(api.ib_startup("barracuda"));
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);

    try createTable();
    defer dropTable() catch {};

    var crsr1: api.ib_crsr_t = null;
    var crsr2: api.ib_crsr_t = null;
    try expectOk(api.ib_cursor_open_table(TABLE_NAME, null, &crsr1));
    try expectOk(api.ib_cursor_open_table(TABLE_NAME, null, &crsr2));

    const insert_tpl = api.ib_clust_read_tuple_create(crsr1) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(insert_tpl);

    var id: api.ib_u32_t = 1;
    try expectOk(api.ib_col_set_value(insert_tpl, 0, &id, @sizeOf(api.ib_u32_t)));
    try expectOk(api.ib_cursor_insert_row(crsr1, insert_tpl));

    try expectOk(api.ib_cursor_close(crsr1));
    crsr1 = null;

    try expectOk(api.ib_cursor_first(crsr2));
    const read_tpl = api.ib_clust_read_tuple_create(crsr2) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(read_tpl);
    try expectOk(api.ib_cursor_read_row(crsr2, read_tpl));

    var read_id: api.ib_u32_t = 0;
    try expectOk(api.ib_tuple_read_u32(read_tpl, 0, &read_id));
    try std.testing.expectEqual(id, read_id);

    try expectOk(api.ib_cursor_close(crsr2));
}
