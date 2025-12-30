const std = @import("std");
const api = @import("../api/api.zig");
const compat = @import("../ut/compat.zig");

const DATABASE: []const u8 = "simple_test";
const TABLE: []const u8 = "data";
const TABLE_NAME: []const u8 = DATABASE ++ "/" ++ TABLE;

const TotalRows: usize = 200;
const BatchSize: usize = 50;

fn expectOk(err: api.ib_err_t) !void {
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
}

fn createDatabase() !void {
    _ = api.ib_database_create(DATABASE);
}

fn dropDatabase() !void {
    try expectOk(api.ib_database_drop(DATABASE));
}

fn createTable() !void {
    var tbl_sch: api.ib_tbl_sch_t = null;
    var idx_sch: api.ib_idx_sch_t = null;

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);
    try expectOk(api.ib_schema_lock_exclusive(trx));

    try expectOk(api.ib_table_schema_create(TABLE_NAME, &tbl_sch, .IB_TBL_COMPACT, 0));
    defer api.ib_table_schema_delete(tbl_sch);

    try expectOk(api.ib_table_schema_add_col(tbl_sch, "id", .IB_INT, .IB_COL_UNSIGNED, 0, 4));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "name", .IB_VARCHAR, .IB_COL_NONE, 0, 50));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "value", .IB_INT, .IB_COL_NONE, 0, 4));

    try expectOk(api.ib_table_schema_add_index(tbl_sch, "PRIMARY_KEY", &idx_sch));
    try expectOk(api.ib_index_schema_add_col(idx_sch, "id", 0));
    try expectOk(api.ib_index_schema_set_clustered(idx_sch));

    var table_id: api.ib_id_t = 0;
    const create_err = api.ib_table_create(trx, tbl_sch, &table_id);
    if (create_err != .DB_SUCCESS and create_err != .DB_TABLE_IS_BEING_USED) {
        try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, create_err);
    }

    try expectOk(api.ib_trx_commit(trx));
}

fn dropTable() !void {
    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);
    try expectOk(api.ib_schema_lock_exclusive(trx));
    try expectOk(api.ib_table_drop(trx, TABLE_NAME));
    try expectOk(api.ib_trx_commit(trx));
}

fn bulkInsert(total_rows: usize, batch_size: usize) !usize {
    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);

    var crsr: api.ib_crsr_t = null;
    try expectOk(api.ib_cursor_open_table(TABLE_NAME, trx, &crsr));
    defer _ = api.ib_cursor_close(crsr);

    try expectOk(api.ib_cursor_lock(crsr, .IB_LOCK_IX));

    const tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(tpl);

    var inserted: usize = 0;
    var batch_count: usize = 0;
    var name_buf: [64]u8 = undefined;

    var row: usize = 1;
    while (row <= total_rows) : (row += 1) {
        const id: api.ib_u32_t = @intCast(row);
        const value: api.ib_i32_t = @intCast(row % 1000);
        const name = try std.fmt.bufPrint(&name_buf, "User_{d}", .{row});

        try expectOk(api.ib_tuple_write_u32(tpl, 0, id));
        try expectOk(api.ib_col_set_value(tpl, 1, name.ptr, @intCast(name.len)));
        try expectOk(api.ib_tuple_write_i32(tpl, 2, value));

        try expectOk(api.ib_cursor_insert_row(crsr, tpl));
        inserted += 1;

        if (batch_size > 0 and row % batch_size == 0) {
            batch_count += 1;
        }

        _ = api.ib_tuple_clear(tpl);
    }

    if (batch_size == 0) {
        batch_count = 1;
    } else if (total_rows % batch_size != 0) {
        batch_count += 1;
    }

    try expectOk(api.ib_trx_commit(trx));
    try std.testing.expect(batch_count > 0);

    return inserted;
}

test "ib simple bulk harness" {
    _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
    try expectOk(api.ib_init());
    try expectOk(api.ib_startup("barracuda"));
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);

    try createDatabase();
    try createTable();

    const inserted = try bulkInsert(TotalRows, BatchSize);
    try std.testing.expectEqual(TotalRows, inserted);

    try dropTable();
    try dropDatabase();
}
