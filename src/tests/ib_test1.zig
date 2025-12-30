const std = @import("std");
const api = @import("../api/api.zig");
const compat = @import("../ut/compat.zig");

const DATABASE: []const u8 = "test";
const TABLE: []const u8 = "t";
const TABLE_NAME: []const u8 = DATABASE ++ "/" ++ TABLE;

const Row = struct {
    c1: []const u8,
    c2: []const u8,
    c3: api.ib_u32_t,
};

const rows = [_]Row{
    .{ .c1 = "a", .c2 = "t", .c3 = 1 },
    .{ .c1 = "b", .c2 = "u", .c3 = 2 },
    .{ .c1 = "c", .c2 = "b", .c3 = 3 },
    .{ .c1 = "d", .c2 = "n", .c3 = 4 },
    .{ .c1 = "e", .c2 = "s", .c3 = 5 },
    .{ .c1 = "e", .c2 = "j", .c3 = 6 },
    .{ .c1 = "d", .c2 = "f", .c3 = 7 },
    .{ .c1 = "c", .c2 = "n", .c3 = 8 },
    .{ .c1 = "b", .c2 = "z", .c3 = 9 },
    .{ .c1 = "a", .c2 = "i", .c3 = 10 },
};

const ScanStats = struct {
    total: usize = 0,
    a_count: usize = 0,
    a_sum: api.ib_u32_t = 0,
    has_bz: bool = false,
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

    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c1", .IB_VARCHAR, .IB_COL_NONE, 0, 31));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c2", .IB_VARCHAR, .IB_COL_NONE, 0, 31));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c3", .IB_INT, .IB_COL_UNSIGNED, 0, @sizeOf(api.ib_u32_t)));

    try expectOk(api.ib_table_schema_add_index(tbl_sch, "c1_c2", &idx_sch));
    try expectOk(api.ib_index_schema_add_col(idx_sch, "c1", 0));
    try expectOk(api.ib_index_schema_add_col(idx_sch, "c2", 0));
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

fn openTable(trx: api.ib_trx_t) !api.ib_crsr_t {
    var crsr: api.ib_crsr_t = null;
    try expectOk(api.ib_cursor_open_table(TABLE_NAME, trx, &crsr));
    return crsr;
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

    for (rows) |row| {
        try expectOk(api.ib_col_set_value(tpl, 0, row.c1.ptr, @intCast(row.c1.len)));
        try expectOk(api.ib_col_set_value(tpl, 1, row.c2.ptr, @intCast(row.c2.len)));
        try expectOk(api.ib_col_set_value(tpl, 2, &row.c3, @sizeOf(api.ib_u32_t)));
        try expectOk(api.ib_cursor_insert_row(crsr, tpl));
        _ = api.ib_tuple_clear(tpl);
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

        const c1 = colSlice(tpl, 0) orelse break;
        const c2 = colSlice(tpl, 1) orelse break;
        var c3: api.ib_u32_t = 0;
        try expectOk(api.ib_tuple_read_u32(tpl, 2, &c3));

        stats.total += 1;
        if (std.mem.eql(u8, c1, "a")) {
            stats.a_count += 1;
            stats.a_sum += c3;
        }
        if (std.mem.eql(u8, c1, "b") and std.mem.eql(u8, c2, "z")) {
            stats.has_bz = true;
        }

        err = api.ib_cursor_next(crsr);
        if (err != .DB_SUCCESS) {
            break;
        }

        _ = api.ib_tuple_clear(tpl);
    }

    try expectOk(api.ib_cursor_reset(crsr));
    return stats;
}

fn updateARows(crsr: api.ib_crsr_t) !void {
    const key_tpl = api.ib_sec_search_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(key_tpl);

    try expectOk(api.ib_col_set_value(key_tpl, 0, "a", 1));

    var res: i32 = 0;
    try expectOk(api.ib_cursor_moveto(crsr, key_tpl, .IB_CUR_GE, &res));

    const old_tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(old_tpl);
    const new_tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(new_tpl);

    while (true) {
        var err = api.ib_cursor_read_row(crsr, old_tpl);
        if (err != .DB_SUCCESS) {
            break;
        }
        const c1 = colSlice(old_tpl, 0) orelse break;
        if (!std.mem.eql(u8, c1, "a")) {
            break;
        }

        try expectOk(api.ib_tuple_copy(new_tpl, old_tpl));
        var c3: api.ib_u32_t = 0;
        try expectOk(api.ib_tuple_read_u32(old_tpl, 2, &c3));
        c3 += 100;
        try expectOk(api.ib_tuple_write_u32(new_tpl, 2, c3));
        try expectOk(api.ib_cursor_update_row(crsr, old_tpl, new_tpl));

        err = api.ib_cursor_next(crsr);
        if (err != .DB_SUCCESS) {
            break;
        }

        _ = api.ib_tuple_clear(old_tpl);
        _ = api.ib_tuple_clear(new_tpl);
    }
}

fn deleteBzRow(crsr: api.ib_crsr_t) !void {
    const key_tpl = api.ib_sec_search_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(key_tpl);

    try expectOk(api.ib_col_set_value(key_tpl, 0, "b", 1));
    try expectOk(api.ib_col_set_value(key_tpl, 1, "z", 1));

    var res: i32 = 0;
    try expectOk(api.ib_cursor_moveto(crsr, key_tpl, .IB_CUR_GE, &res));

    const tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(tpl);

    while (true) {
        const err = api.ib_cursor_read_row(crsr, tpl);
        if (err != .DB_SUCCESS) {
            break;
        }
        const c1 = colSlice(tpl, 0) orelse break;
        const c2 = colSlice(tpl, 1) orelse break;
        if (std.mem.eql(u8, c1, "b") and std.mem.eql(u8, c2, "z")) {
            try expectOk(api.ib_cursor_delete_row(crsr));
            break;
        }
        const next_err = api.ib_cursor_next(crsr);
        if (next_err != .DB_SUCCESS) {
            break;
        }
        _ = api.ib_tuple_clear(tpl);
    }
}

test "ib test1 harness" {
    _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
    try expectOk(api.ib_init());
    try expectOk(api.ib_startup("barracuda"));
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);

    try createDatabase();
    try createTable();

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);

    const crsr = try openTable(trx);
    defer _ = api.ib_cursor_close(crsr);
    try expectOk(api.ib_cursor_lock(crsr, .IB_LOCK_IX));

    try insertRows(crsr);
    var stats = try scanRows(crsr);
    try std.testing.expectEqual(@as(usize, rows.len), stats.total);
    try std.testing.expectEqual(@as(usize, 2), stats.a_count);
    try std.testing.expect(stats.has_bz);

    try updateARows(crsr);
    stats = try scanRows(crsr);
    try std.testing.expectEqual(@as(usize, rows.len), stats.total);
    try std.testing.expectEqual(@as(usize, 2), stats.a_count);
    try std.testing.expectEqual(@as(api.ib_u32_t, 211), stats.a_sum);

    try deleteBzRow(crsr);
    stats = try scanRows(crsr);
    try std.testing.expectEqual(@as(usize, rows.len - 1), stats.total);
    try std.testing.expect(!stats.has_bz);

    try expectOk(api.ib_trx_commit(trx));
    try dropTable();
    try dropDatabase();
}
