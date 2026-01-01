const std = @import("std");
const api = @import("../api/api.zig");
const compat = @import("../ut/compat.zig");

const DATABASE: []const u8 = "lockapi";
const TABLE: []const u8 = "t1";
const TABLE_NAME: []const u8 = DATABASE ++ "/" ++ TABLE;

fn expectOk(err: api.ib_err_t) !void {
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
}

fn createDatabase() !void {
    try std.testing.expectEqual(compat.IB_TRUE, api.ib_database_create(DATABASE));
}

fn dropDatabase() !void {
    try expectOk(api.ib_database_drop(DATABASE));
}

fn createTable() !void {
    var tbl_sch: api.ib_tbl_sch_t = null;
    var idx_sch: api.ib_idx_sch_t = null;

    try expectOk(api.ib_table_schema_create(TABLE_NAME, &tbl_sch, .IB_TBL_COMPACT, 0));
    defer api.ib_table_schema_delete(tbl_sch);

    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c1", .IB_INT, .IB_COL_NONE, 0, 4));
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

fn dropTable() !void {
    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);
    try expectOk(api.ib_schema_lock_exclusive(trx));
    try expectOk(api.ib_table_drop(trx, TABLE_NAME));
    try expectOk(api.ib_trx_commit(trx));
}

test "record lock conflict via api" {
    try expectOk(api.ib_init());
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
    try expectOk(api.ib_startup("barracuda"));

    try createDatabase();
    defer dropDatabase() catch {};
    try createTable();
    defer dropTable() catch {};

    const trx1 = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    defer _ = api.ib_trx_rollback(trx1);
    const trx2 = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    defer _ = api.ib_trx_rollback(trx2);

    var crsr1: api.ib_crsr_t = null;
    var crsr2: api.ib_crsr_t = null;
    try expectOk(api.ib_cursor_open_table(TABLE_NAME, trx1, &crsr1));
    defer _ = api.ib_cursor_close(crsr1);
    try expectOk(api.ib_cursor_open_table(TABLE_NAME, trx2, &crsr2));
    defer _ = api.ib_cursor_close(crsr2);

    try expectOk(api.ib_cursor_lock(crsr1, .IB_LOCK_X));
    try expectOk(api.ib_cursor_lock(crsr2, .IB_LOCK_X));

    const insert_tpl = api.ib_clust_read_tuple_create(crsr1) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(insert_tpl);
    try expectOk(api.ib_tuple_write_i32(insert_tpl, 0, 1));
    try expectOk(api.ib_cursor_insert_row(crsr1, insert_tpl));

    try expectOk(api.ib_cursor_first(crsr1));
    const old_tpl = api.ib_clust_read_tuple_create(crsr1) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(old_tpl);
    const new_tpl = api.ib_clust_read_tuple_create(crsr1) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(new_tpl);
    try expectOk(api.ib_cursor_read_row(crsr1, old_tpl));
    try expectOk(api.ib_tuple_copy(new_tpl, old_tpl));
    try expectOk(api.ib_tuple_write_i32(new_tpl, 0, 2));
    try expectOk(api.ib_cursor_update_row(crsr1, old_tpl, new_tpl));

    try expectOk(api.ib_cursor_first(crsr2));
    const read_tpl = api.ib_clust_read_tuple_create(crsr2) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(read_tpl);
    try std.testing.expectEqual(api.ib_err_t.DB_LOCK_WAIT, api.ib_cursor_read_row(crsr2, read_tpl));
}
