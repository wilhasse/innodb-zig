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

fn createTable(page_size: api.ib_ulint_t) !void {
    var tbl_sch: api.ib_tbl_sch_t = null;
    var idx_sch: api.ib_idx_sch_t = null;
    var tbl_fmt: api.ib_tbl_fmt_t = .IB_TBL_COMPACT;

    if (page_size > 0) {
        tbl_fmt = .IB_TBL_COMPRESSED;
    }

    try expectOk(api.ib_table_schema_create(TABLE_NAME, &tbl_sch, tbl_fmt, page_size));
    defer api.ib_table_schema_delete(tbl_sch);

    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c1", .IB_VARCHAR, .IB_COL_NONE, 0, 10));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c2", .IB_INT, .IB_COL_NONE, 0, @sizeOf(api.ib_i32_t)));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c3", .IB_FLOAT, .IB_COL_NONE, 0, @sizeOf(f32)));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c4", .IB_DOUBLE, .IB_COL_NONE, 0, @sizeOf(f64)));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c5", .IB_DECIMAL, .IB_COL_NONE, 0, 0));

    try expectOk(api.ib_table_schema_add_index(tbl_sch, "c1", &idx_sch));
    try expectOk(api.ib_index_schema_add_col(idx_sch, "c1", 4));

    try std.testing.expectEqual(api.ib_err_t.DB_SCHEMA_ERROR, api.ib_index_schema_add_col(idx_sch, "c2", 2));
    try std.testing.expectEqual(api.ib_err_t.DB_SCHEMA_ERROR, api.ib_index_schema_add_col(idx_sch, "c3", 2));
    try std.testing.expectEqual(api.ib_err_t.DB_SCHEMA_ERROR, api.ib_index_schema_add_col(idx_sch, "c4", 2));
    try std.testing.expectEqual(api.ib_err_t.DB_SCHEMA_ERROR, api.ib_index_schema_add_col(idx_sch, "c5", 2));

    try expectOk(api.ib_index_schema_set_clustered(idx_sch));

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);
    try expectOk(api.ib_schema_lock_exclusive(trx));
    var table_id: api.ib_id_t = 0;
    try expectOk(api.ib_table_create(trx, tbl_sch, &table_id));
    try expectOk(api.ib_trx_commit(trx));
}

fn openTable(trx: api.ib_trx_t) !api.ib_crsr_t {
    var crsr: api.ib_crsr_t = null;
    try expectOk(api.ib_cursor_open_table(TABLE_NAME, trx, &crsr));
    return crsr;
}

fn insertRows(crsr: api.ib_crsr_t) !void {
    const tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(tpl);

    try expectOk(api.ib_col_set_value(tpl, 0, "xxxxaaaa", 8));
    try expectOk(api.ib_tuple_write_i32(tpl, 1, 2));
    try expectOk(api.ib_cursor_insert_row(crsr, tpl));

    try expectOk(api.ib_col_set_value(tpl, 0, "xxxxbbbb", 8));
    try expectOk(api.ib_tuple_write_i32(tpl, 1, 2));
    try std.testing.expectEqual(api.ib_err_t.DB_DUPLICATE_KEY, api.ib_cursor_insert_row(crsr, tpl));
}

fn dropTable() !void {
    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);
    try expectOk(api.ib_schema_lock_exclusive(trx));
    try expectOk(api.ib_table_drop(trx, TABLE_NAME));
    try expectOk(api.ib_trx_commit(trx));
}

test "ib index harness" {
    const version = api.ib_api_version();
    try std.testing.expect(version != 0);

    try expectOk(api.ib_init());
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
    try expectOk(api.ib_startup("barracuda"));

    try createDatabase();
    try createTable(0);

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);

    const crsr = try openTable(trx);
    try expectOk(api.ib_cursor_lock(crsr, .IB_LOCK_IX));
    try insertRows(crsr);

    try expectOk(api.ib_cursor_close(crsr));
    try expectOk(api.ib_trx_commit(trx));
    try dropTable();
}
