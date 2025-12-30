const std = @import("std");
const api = @import("../api/api.zig");
const compat = @import("../ut/compat.zig");

const DATABASE: []const u8 = "drop_test";
const TABLE_PREFIX: []const u8 = "t";
const TABLE_COUNT: usize = 10;

fn expectOk(err: api.ib_err_t) !void {
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
}

fn createDatabase() !void {
    try std.testing.expectEqual(compat.IB_TRUE, api.ib_database_create(DATABASE));
}

fn createTable(dbname: []const u8, name: []const u8, n: usize) !void {
    const max_len: usize = @intCast(api.IB_MAX_TABLE_NAME_LEN);
    var table_name_buf: [max_len]u8 = undefined;
    const table_name = try std.fmt.bufPrint(&table_name_buf, "{s}/{s}{d}", .{ dbname, name, n });

    var tbl_sch: api.ib_tbl_sch_t = null;
    try expectOk(api.ib_table_schema_create(table_name, &tbl_sch, .IB_TBL_COMPACT, 0));
    defer api.ib_table_schema_delete(tbl_sch);

    try expectOk(api.ib_table_schema_add_col(
        tbl_sch,
        "c1",
        .IB_INT,
        .IB_COL_UNSIGNED,
        0,
        @sizeOf(api.ib_u32_t),
    ));

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);
    try expectOk(api.ib_schema_lock_exclusive(trx));
    var table_id: api.ib_id_t = 0;
    try expectOk(api.ib_table_create(trx, tbl_sch, &table_id));
    try expectOk(api.ib_trx_commit(trx));
}

fn openTable(dbname: []const u8, name: []const u8, n: usize, trx: api.ib_trx_t) !void {
    const max_len: usize = @intCast(api.IB_MAX_TABLE_NAME_LEN);
    var table_name_buf: [max_len]u8 = undefined;
    const table_name = try std.fmt.bufPrint(&table_name_buf, "{s}/{s}{d}", .{ dbname, name, n });

    var crsr: api.ib_crsr_t = null;
    try expectOk(api.ib_cursor_open_table(table_name, trx, &crsr));
    try expectOk(api.ib_cursor_close(crsr));
}

test "ib drop harness" {
    try expectOk(api.ib_init());
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
    try expectOk(api.ib_startup("barracuda"));

    try createDatabase();

    for (0..TABLE_COUNT) |i| {
        try createTable(DATABASE, TABLE_PREFIX, i);
    }

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);

    for (0..TABLE_COUNT) |i| {
        try openTable(DATABASE, TABLE_PREFIX, i, trx);
    }

    try expectOk(api.ib_trx_commit(trx));
    try expectOk(api.ib_database_drop(DATABASE));
}
