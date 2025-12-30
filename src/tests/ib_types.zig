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

fn dropDatabase() !void {
    try expectOk(api.ib_database_drop(DATABASE));
}

fn createTable() !void {
    var tbl_sch: api.ib_tbl_sch_t = null;
    var idx_sch: api.ib_idx_sch_t = null;

    try expectOk(api.ib_table_schema_create(TABLE_NAME, &tbl_sch, .IB_TBL_COMPACT, 0));
    defer api.ib_table_schema_delete(tbl_sch);

    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c1", .IB_VARCHAR, .IB_COL_NONE, 0, 10));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c2", .IB_INT, .IB_COL_NOT_NULL, 0, @sizeOf(api.ib_u32_t)));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c3", .IB_FLOAT, .IB_COL_NONE, 0, @sizeOf(f32)));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c4", .IB_DOUBLE, .IB_COL_NONE, 0, @sizeOf(f64)));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c5", .IB_BLOB, .IB_COL_NONE, 0, 0));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c6", .IB_DECIMAL, .IB_COL_NONE, 0, 0));

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

fn colSlice(ib_tpl: api.ib_tpl_t, col: api.ib_ulint_t) ?[]const u8 {
    const ptr = api.ib_col_get_value(ib_tpl, col) orelse return null;
    const len = api.ib_col_get_len(ib_tpl, col);
    if (len == 0 or len == api.IB_SQL_NULL) {
        return null;
    }
    return @as([*]const u8, @ptrCast(ptr))[0..@intCast(len)];
}

fn insertRows(crsr: api.ib_crsr_t) !void {
    const tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(tpl);

    try expectOk(api.ib_col_set_value(tpl, 0, "abcdefghij", 10));
    try expectOk(api.ib_tuple_write_float(tpl, 2, 2.0));
    try expectOk(api.ib_tuple_write_double(tpl, 3, 3.0));
    try expectOk(api.ib_col_set_value(tpl, 4, "BLOB", 4));
    try expectOk(api.ib_col_set_value(tpl, 5, "1.23", 4));

    try std.testing.expectEqual(api.ib_err_t.DB_DATA_MISMATCH, api.ib_cursor_insert_row(crsr, tpl));

    try expectOk(api.ib_tuple_write_u32(tpl, 1, 1));
    try expectOk(api.ib_cursor_insert_row(crsr, tpl));
}

fn readRows(crsr: api.ib_crsr_t) !void {
    const tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(tpl);

    try expectOk(api.ib_cursor_first(crsr));
    try expectOk(api.ib_cursor_read_row(crsr, tpl));

    const c1 = colSlice(tpl, 0) orelse return error.MissingValue;
    try std.testing.expectEqualStrings("abcdefghij", c1);

    var c2: api.ib_u32_t = 0;
    try expectOk(api.ib_tuple_read_u32(tpl, 1, &c2));
    try std.testing.expectEqual(@as(api.ib_u32_t, 1), c2);

    var c3: f32 = 0;
    try expectOk(api.ib_tuple_read_float(tpl, 2, &c3));
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), c3, 0.0001);

    var c4: f64 = 0;
    try expectOk(api.ib_tuple_read_double(tpl, 3, &c4));
    try std.testing.expectApproxEqAbs(@as(f64, 3.0), c4, 0.0001);

    const c5 = colSlice(tpl, 4) orelse return error.MissingValue;
    try std.testing.expectEqualStrings("BLOB", c5);

    const c6 = colSlice(tpl, 5) orelse return error.MissingValue;
    try std.testing.expectEqualStrings("1.23", c6);

    try expectOk(api.ib_cursor_reset(crsr));
}

test "ib types harness" {
    _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
    try expectOk(api.ib_init());
    try expectOk(api.ib_startup("barracuda"));
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);

    try createDatabase();
    try createTable();

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);

    var crsr: api.ib_crsr_t = null;
    try expectOk(api.ib_cursor_open_table(TABLE_NAME, trx, &crsr));
    defer _ = api.ib_cursor_close(crsr);

    try expectOk(api.ib_cursor_lock(crsr, .IB_LOCK_IX));
    try insertRows(crsr);
    try readRows(crsr);

    try expectOk(api.ib_trx_commit(trx));
    try dropTable();
    try dropDatabase();
}
