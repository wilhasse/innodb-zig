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

fn createTable() !void {
    var tbl_sch: api.ib_tbl_sch_t = null;
    try expectOk(api.ib_table_schema_create(TABLE_NAME, &tbl_sch, .IB_TBL_COMPACT, 0));
    defer api.ib_table_schema_delete(tbl_sch);

    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c1", .IB_INT, .IB_COL_NONE, 0, @sizeOf(i32)));

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

    var i: i32 = 0;
    while (i < 10) : (i += 1) {
        try expectOk(api.ib_tuple_write_i32(tpl, 0, i));
        try expectOk(api.ib_cursor_insert_row(crsr, tpl));
        _ = api.ib_tuple_clear(tpl);
    }
}

fn readRowValue(tpl: api.ib_tpl_t) !i32 {
    var val: api.ib_i32_t = 0;
    try expectOk(api.ib_tuple_read_i32(tpl, 0, &val));
    return val;
}

test "ib cursor harness" {
    try expectOk(api.ib_init());
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);

    try expectOk(api.ib_startup("barracuda"));

    try createDatabase();
    try createTable();

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    defer _ = api.ib_trx_commit(trx);

    var crsr: api.ib_crsr_t = null;
    try expectOk(api.ib_cursor_open_table(TABLE_NAME, trx, &crsr));
    defer _ = api.ib_cursor_close(crsr);

    try expectOk(api.ib_cursor_lock(crsr, .IB_LOCK_IX));
    try insertRows(crsr);

    // SELECT * FROM T;
    try expectOk(api.ib_cursor_first(crsr));
    const read_tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(read_tpl);
    var expected: i32 = 0;
    while (true) {
        const err = api.ib_cursor_read_row(crsr, read_tpl);
        if (err == .DB_RECORD_NOT_FOUND or err == .DB_END_OF_INDEX) {
            break;
        }
        try expectOk(err);
        const val = try readRowValue(read_tpl);
        try std.testing.expectEqual(expected, val);
        expected += 1;

        const next_err = api.ib_cursor_next(crsr);
        if (next_err == .DB_RECORD_NOT_FOUND or next_err == .DB_END_OF_INDEX) {
            break;
        }
        try expectOk(next_err);
        _ = api.ib_tuple_clear(read_tpl);
    }
    try std.testing.expectEqual(@as(i32, 10), expected);

    // SELECT * FROM T WHERE c1 = 5;
    const key_tpl = api.ib_clust_search_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(key_tpl);
    try expectOk(api.ib_tuple_write_i32(key_tpl, 0, 5));
    var ret: i32 = 0;
    try expectOk(api.ib_cursor_moveto(crsr, key_tpl, .IB_CUR_GE, &ret));
    try std.testing.expectEqual(@as(i32, 0), ret);
    try expectOk(api.ib_cursor_read_row(crsr, read_tpl));
    try std.testing.expectEqual(@as(i32, 5), try readRowValue(read_tpl));

    // SELECT * FROM T WHERE c1 > 5;
    try expectOk(api.ib_cursor_moveto(crsr, key_tpl, .IB_CUR_G, &ret));
    try std.testing.expect(ret < 0);
    var expected_gt: i32 = 6;
    while (true) {
        const err = api.ib_cursor_read_row(crsr, read_tpl);
        if (err == .DB_RECORD_NOT_FOUND or err == .DB_END_OF_INDEX) {
            break;
        }
        try expectOk(err);
        const val = try readRowValue(read_tpl);
        try std.testing.expectEqual(expected_gt, val);
        expected_gt += 1;

        const next_err = api.ib_cursor_next(crsr);
        if (next_err == .DB_RECORD_NOT_FOUND or next_err == .DB_END_OF_INDEX) {
            break;
        }
        try expectOk(next_err);
        _ = api.ib_tuple_clear(read_tpl);
    }
    try std.testing.expectEqual(@as(i32, 10), expected_gt);

    // SELECT * FROM T WHERE c1 < 5;
    try expectOk(api.ib_cursor_first(crsr));
    var expected_lt: i32 = 0;
    while (true) {
        const err = api.ib_cursor_read_row(crsr, read_tpl);
        if (err == .DB_RECORD_NOT_FOUND or err == .DB_END_OF_INDEX) {
            break;
        }
        try expectOk(err);
        const val = try readRowValue(read_tpl);
        if (val >= 5) {
            break;
        }
        try std.testing.expectEqual(expected_lt, val);
        expected_lt += 1;

        const next_err = api.ib_cursor_next(crsr);
        if (next_err == .DB_RECORD_NOT_FOUND or next_err == .DB_END_OF_INDEX) {
            break;
        }
        try expectOk(next_err);
        _ = api.ib_tuple_clear(read_tpl);
    }
    try std.testing.expectEqual(@as(i32, 5), expected_lt);

    // SELECT * FROM T WHERE c1 >= 1 AND c1 < 5;
    try expectOk(api.ib_tuple_write_i32(key_tpl, 0, 1));
    try expectOk(api.ib_cursor_moveto(crsr, key_tpl, .IB_CUR_GE, &ret));
    try std.testing.expectEqual(@as(i32, 0), ret);
    var expected_range: i32 = 1;
    while (true) {
        const err = api.ib_cursor_read_row(crsr, read_tpl);
        if (err == .DB_RECORD_NOT_FOUND or err == .DB_END_OF_INDEX) {
            break;
        }
        try expectOk(err);
        const val = try readRowValue(read_tpl);
        if (val >= 5) {
            break;
        }
        try std.testing.expectEqual(expected_range, val);
        expected_range += 1;

        const next_err = api.ib_cursor_next(crsr);
        if (next_err == .DB_RECORD_NOT_FOUND or next_err == .DB_END_OF_INDEX) {
            break;
        }
        try expectOk(next_err);
        _ = api.ib_tuple_clear(read_tpl);
    }
    try std.testing.expectEqual(@as(i32, 5), expected_range);

    try dropTable();
}
