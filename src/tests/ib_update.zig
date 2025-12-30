const std = @import("std");
const api = @import("../api/api.zig");
const compat = @import("../ut/compat.zig");

const DATABASE: []const u8 = "test";
const TABLE: []const u8 = "t";
const TABLE_NAME: []const u8 = DATABASE ++ "/" ++ TABLE;

const ScanStats = struct {
    count: usize = 0,
    sum: i32 = 0,
};

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
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c2", .IB_VARCHAR, .IB_COL_NONE, 0, 10));

    try expectOk(api.ib_table_schema_add_index(tbl_sch, "c1", &idx_sch));
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

fn insertRows(crsr: api.ib_crsr_t) !void {
    const tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(tpl);

    var ch: u8 = 'a';
    var i: i32 = 0;
    while (i < 10) : (i += 2) {
        try expectOk(api.ib_tuple_write_i32(tpl, 0, i));
        const buf = [1]u8{ch};
        try expectOk(api.ib_col_set_value(tpl, 1, &buf, 1));
        try expectOk(api.ib_cursor_insert_row(crsr, tpl));
        ch += 1;
        _ = api.ib_tuple_clear(tpl);
    }
}

fn updateRows(crsr: api.ib_crsr_t) !void {
    try expectOk(api.ib_cursor_set_lock_mode(crsr, .IB_LOCK_X));
    try expectOk(api.ib_cursor_first(crsr));

    const old_tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(old_tpl);
    const new_tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(new_tpl);

    var err: api.ib_err_t = .DB_SUCCESS;
    while (err == .DB_SUCCESS) {
        var c1: api.ib_i32_t = 0;
        err = api.ib_cursor_read_row(crsr, old_tpl);
        if (err != .DB_SUCCESS) {
            break;
        }
        try expectOk(api.ib_tuple_copy(new_tpl, old_tpl));
        try expectOk(api.ib_tuple_read_i32(new_tpl, 0, &c1));
        c1 = @divTrunc(c1, 2);
        try expectOk(api.ib_tuple_write_i32(new_tpl, 0, c1));
        try expectOk(api.ib_cursor_update_row(crsr, old_tpl, new_tpl));
        err = api.ib_cursor_next(crsr);
        _ = api.ib_tuple_clear(old_tpl);
        _ = api.ib_tuple_clear(new_tpl);
    }
}

fn scanRows(crsr: api.ib_crsr_t) !ScanStats {
    var stats = ScanStats{};
    const tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(tpl);

    var err = api.ib_cursor_first(crsr);
    if (err != .DB_SUCCESS) {
        _ = api.ib_cursor_reset(crsr);
        return stats;
    }

    while (err == .DB_SUCCESS) {
        err = api.ib_cursor_read_row(crsr, tpl);
        if (err != .DB_SUCCESS) {
            break;
        }

        var c1: api.ib_i32_t = 0;
        try expectOk(api.ib_tuple_read_i32(tpl, 0, &c1));
        stats.count += 1;
        stats.sum += c1;

        err = api.ib_cursor_next(crsr);
        if (err != .DB_SUCCESS) {
            break;
        }
        _ = api.ib_tuple_clear(tpl);
    }

    try expectOk(api.ib_cursor_reset(crsr));
    return stats;
}

test "ib update harness" {
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

    var stats = try scanRows(crsr);
    try std.testing.expectEqual(@as(usize, 5), stats.count);
    try std.testing.expectEqual(@as(i32, 20), stats.sum);

    try updateRows(crsr);
    stats = try scanRows(crsr);
    try std.testing.expectEqual(@as(usize, 5), stats.count);
    try std.testing.expectEqual(@as(i32, 10), stats.sum);

    try expectOk(api.ib_trx_commit(trx));
    try dropTable();
    try dropDatabase();
}
