const std = @import("std");
const api = @import("../api/api.zig");
const compat = @import("../ut/compat.zig");

const DATABASE: []const u8 = "test";

fn expectOk(err: api.ib_err_t) !void {
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
}

fn createDatabase() !void {
    try std.testing.expectEqual(compat.IB_TRUE, api.ib_database_create(DATABASE));
}

fn createTable(name: []const u8) !void {
    const table_name = try std.fmt.allocPrint(std.testing.allocator, "{s}/{s}", .{ DATABASE, name });
    defer std.testing.allocator.free(table_name);

    var tbl_sch: api.ib_tbl_sch_t = null;
    try expectOk(api.ib_table_schema_create(table_name, &tbl_sch, .IB_TBL_COMPACT, 0));
    defer api.ib_table_schema_delete(tbl_sch);

    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c1", .IB_INT, .IB_COL_UNSIGNED, 0, @sizeOf(api.ib_u32_t)));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c2", .IB_INT, .IB_COL_UNSIGNED, 0, @sizeOf(api.ib_u32_t)));

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

fn insertRows(crsr: api.ib_crsr_t, start: u32, n_values: u32, thread_id: u32) !void {
    const tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(tpl);

    var i: u32 = 0;
    while (i < n_values) : (i += 1) {
        const value = start + i;
        try expectOk(api.ib_tuple_write_u32(tpl, 0, value));
        try expectOk(api.ib_tuple_write_u32(tpl, 1, thread_id));
        try expectOk(api.ib_cursor_insert_row(crsr, tpl));
    }
}

fn workerSequence(thread_id: u32, start: u32, count: u32) !void {
    const t1_name = try std.fmt.allocPrint(std.testing.allocator, "{s}/T1", .{DATABASE});
    defer std.testing.allocator.free(t1_name);
    const t2_name = try std.fmt.allocPrint(std.testing.allocator, "{s}/T2", .{DATABASE});
    defer std.testing.allocator.free(t2_name);

    var crsr1: api.ib_crsr_t = null;
    var crsr2: api.ib_crsr_t = null;
    try expectOk(api.ib_cursor_open_table(t1_name, null, &crsr1));
    defer _ = api.ib_cursor_close(crsr1);
    try expectOk(api.ib_cursor_open_table(t2_name, null, &crsr2));
    defer _ = api.ib_cursor_close(crsr2);

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    defer _ = api.ib_trx_commit(trx);
    api.ib_cursor_attach_trx(crsr1, trx);
    api.ib_cursor_attach_trx(crsr2, trx);

    try expectOk(api.ib_cursor_lock(crsr1, .IB_LOCK_IX));
    try expectOk(api.ib_cursor_lock(crsr2, .IB_LOCK_IX));

    try insertRows(crsr1, start, count, thread_id);
    try insertRows(crsr2, start + 1000, count, thread_id);
}

fn dropTable(name: []const u8) !void {
    const table_name = try std.fmt.allocPrint(std.testing.allocator, "{s}/{s}", .{ DATABASE, name });
    defer std.testing.allocator.free(table_name);

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);
    try expectOk(api.ib_schema_lock_exclusive(trx));
    try expectOk(api.ib_table_drop(trx, table_name));
    try expectOk(api.ib_trx_commit(trx));
}

test "ib deadlock harness" {
    try expectOk(api.ib_init());
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
    try expectOk(api.ib_startup("barracuda"));

    try createDatabase();
    try createTable("T1");
    try createTable("T2");

    try workerSequence(1, 0, 5);
    try workerSequence(2, 10, 5);

    try dropTable("T1");
    try dropTable("T2");
}
