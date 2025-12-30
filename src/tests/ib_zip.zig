const std = @import("std");
const api = @import("../api/api.zig");
const compat = @import("../ut/compat.zig");

const DATABASE: []const u8 = "test";
const TABLE: []const u8 = "t";
const TABLE_NAME: []const u8 = DATABASE ++ "/" ++ TABLE;

fn expectOk(err: api.ib_err_t) !void {
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
}

fn createDatabase() !void {
    try std.testing.expectEqual(compat.IB_TRUE, api.ib_database_create(DATABASE));
}

fn createTable(page_size: api.ib_ulint_t) !api.ib_err_t {
    var tbl_sch: api.ib_tbl_sch_t = null;
    var idx_sch: api.ib_idx_sch_t = null;
    const fmt: api.ib_tbl_fmt_t = if (page_size > 0) .IB_TBL_COMPRESSED else .IB_TBL_COMPACT;

    const err = api.ib_table_schema_create(TABLE_NAME, &tbl_sch, fmt, page_size);
    if (err == .DB_UNSUPPORTED) {
        return err;
    }
    try expectOk(err);
    defer api.ib_table_schema_delete(tbl_sch);

    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c1", .IB_VARCHAR, .IB_COL_NONE, 0, 10));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c2", .IB_INT, .IB_COL_NONE, 0, @sizeOf(api.ib_i32_t)));

    try expectOk(api.ib_table_schema_add_index(tbl_sch, "c1", &idx_sch));
    try expectOk(api.ib_index_schema_add_col(idx_sch, "c1", 0));
    try expectOk(api.ib_index_schema_set_clustered(idx_sch));

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);
    try expectOk(api.ib_schema_lock_exclusive(trx));
    var table_id: api.ib_id_t = 0;
    const create_err = api.ib_table_create(trx, tbl_sch, &table_id);
    if (create_err != .DB_SUCCESS and create_err != .DB_TABLE_IS_BEING_USED) {
        try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, create_err);
    }
    try expectOk(api.ib_trx_commit(trx));
    return .DB_SUCCESS;
}

fn insertRow(crsr: api.ib_crsr_t) !void {
    const tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(tpl);

    try expectOk(api.ib_col_set_value(tpl, 0, "x", 1));
    try expectOk(api.ib_tuple_write_i32(tpl, 1, 1));
    const err = api.ib_cursor_insert_row(crsr, tpl);
    if (err != .DB_SUCCESS and err != .DB_DUPLICATE_KEY) {
        try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
    }
}

test "ib zip harness" {
    _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
    try expectOk(api.ib_init());
    try expectOk(api.ib_startup("barracuda"));
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);

    try createDatabase();

    const create_err = try createTable(4);
    if (create_err == .DB_UNSUPPORTED) {
        return;
    }

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);

    var crsr: api.ib_crsr_t = null;
    try expectOk(api.ib_cursor_open_table(TABLE_NAME, trx, &crsr));
    defer _ = api.ib_cursor_close(crsr);

    try expectOk(api.ib_cursor_lock(crsr, .IB_LOCK_IX));
    try insertRow(crsr);

    try expectOk(api.ib_trx_commit(trx));
}
