const std = @import("std");
const api = @import("../api/api.zig");
const compat = @import("../ut/compat.zig");

const DATABASE: []const u8 = "test";
const TABLE: []const u8 = "ib_ddl";
const TABLE_NAME: []const u8 = DATABASE ++ "/" ++ TABLE;

fn expectOk(err: api.ib_err_t) !void {
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
}

fn createDatabase() !void {
    try std.testing.expectEqual(compat.IB_TRUE, api.ib_database_create(DATABASE));
}

fn createTable() !void {
    var tbl_sch: api.ib_tbl_sch_t = null;
    try expectOk(api.ib_table_schema_create(TABLE_NAME, &tbl_sch, .IB_TBL_COMPACT, 0));
    defer api.ib_table_schema_delete(tbl_sch);

    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c1", .IB_INT, .IB_COL_NONE, 0, @sizeOf(api.ib_i32_t)));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c2", .IB_VARCHAR, .IB_COL_NONE, 0, 10));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c3", .IB_BLOB, .IB_COL_NONE, 0, 0));

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);
    try expectOk(api.ib_schema_lock_exclusive(trx));
    var table_id: api.ib_id_t = 0;
    try expectOk(api.ib_table_create(trx, tbl_sch, &table_id));
    try expectOk(api.ib_trx_commit(trx));
}

fn insertRows(crsr: api.ib_crsr_t) !void {
    const tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(tpl);

    var i: api.ib_i32_t = 0;
    while (i < 10) : (i += 1) {
        const c2 = "row";
        const c3 = "blob";
        try expectOk(api.ib_tuple_write_i32(tpl, 0, @mod(i, 10)));
        try expectOk(api.ib_col_set_value(tpl, 1, @ptrCast(c2.ptr), @intCast(c2.len)));
        try expectOk(api.ib_col_set_value(tpl, 2, @ptrCast(c3.ptr), @intCast(c3.len)));
        try expectOk(api.ib_cursor_insert_row(crsr, tpl));
        _ = api.ib_tuple_clear(tpl);
    }
}

fn createSecIndex(table_name: []const u8, col_name: []const u8, prefix_len: api.ib_ulint_t) !void {
    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);
    try expectOk(api.ib_schema_lock_exclusive(trx));

    const index_name = try std.fmt.allocPrint(std.testing.allocator, "{s}_{s}", .{ table_name, col_name });
    defer std.testing.allocator.free(index_name);

    var idx_sch: api.ib_idx_sch_t = null;
    try expectOk(api.ib_index_schema_create(trx, index_name, table_name, &idx_sch));
    defer api.ib_index_schema_delete(idx_sch);
    try expectOk(api.ib_index_schema_add_col(idx_sch, col_name, prefix_len));
    var index_id: api.ib_id_t = 0;
    try expectOk(api.ib_index_create(idx_sch, &index_id));
    try expectOk(api.ib_trx_commit(trx));
}

fn createSecIndexes() !void {
    try createSecIndex(TABLE_NAME, "c1", 0);
    try createSecIndex(TABLE_NAME, "c2", 0);
    try createSecIndex(TABLE_NAME, "c3", 10);
}

fn openSecIndexes() !void {
    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);

    var crsr: api.ib_crsr_t = null;
    try expectOk(api.ib_cursor_open_table(TABLE_NAME, trx, &crsr));
    defer _ = api.ib_cursor_close(crsr);

    const names = [_][]const u8{ "c1", "c2", "c3" };
    for (names) |col_name| {
        const index_name = try std.fmt.allocPrint(std.testing.allocator, "{s}_{s}", .{ TABLE_NAME, col_name });
        defer std.testing.allocator.free(index_name);
        var idx_crsr: api.ib_crsr_t = null;
        try expectOk(api.ib_cursor_open_index_using_name(crsr, index_name, &idx_crsr));
        try expectOk(api.ib_cursor_close(idx_crsr));
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

test "ib ddl harness" {
    try expectOk(api.ib_init());
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
    try expectOk(api.ib_startup("barracuda"));

    try createDatabase();
    try createTable();

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);

    var crsr: api.ib_crsr_t = null;
    try expectOk(api.ib_cursor_open_table(TABLE_NAME, trx, &crsr));
    try expectOk(api.ib_cursor_lock(crsr, .IB_LOCK_IX));
    try insertRows(crsr);
    try expectOk(api.ib_cursor_close(crsr));
    try expectOk(api.ib_trx_commit(trx));

    try createSecIndexes();
    try openSecIndexes();
    try dropTable();
}
