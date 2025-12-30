const std = @import("std");
const api = @import("../api/api.zig");
const compat = @import("../ut/compat.zig");

const DATABASE: []const u8 = "test";
const TABLE: []const u8 = "t";

const Row = struct {
    c1: []const u8,
    c2: []const u8,
    c3: api.ib_u32_t,
};

const in_rows = [_]Row{
    .{ .c1 = "abc", .c2 = "def", .c3 = 1 },
    .{ .c1 = "abc", .c2 = "zzz", .c3 = 1 },
    .{ .c1 = "ghi", .c2 = "jkl", .c3 = 2 },
    .{ .c1 = "mno", .c2 = "pqr", .c3 = 3 },
    .{ .c1 = "mno", .c2 = "xxx", .c3 = 3 },
    .{ .c1 = "stu", .c2 = "vwx", .c3 = 4 },
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

fn createTable(dbname: []const u8, name: []const u8) !void {
    const max_len: usize = @intCast(api.IB_MAX_TABLE_NAME_LEN);
    var table_name_buf: [max_len]u8 = undefined;
    const table_name = try std.fmt.bufPrint(&table_name_buf, "{s}/{s}", .{ dbname, name });

    var tbl_sch: api.ib_tbl_sch_t = null;
    var idx_sch: api.ib_idx_sch_t = null;

    try expectOk(api.ib_table_schema_create(table_name, &tbl_sch, .IB_TBL_COMPACT, 0));
    defer api.ib_table_schema_delete(tbl_sch);

    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c1", .IB_VARCHAR, .IB_COL_NONE, 0, 31));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c2", .IB_VARCHAR, .IB_COL_NONE, 0, 31));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c3", .IB_INT, .IB_COL_UNSIGNED, 0, @sizeOf(api.ib_u32_t)));

    try expectOk(api.ib_table_schema_add_index(tbl_sch, "PRIMARY_KEY", &idx_sch));
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

fn dropTable(dbname: []const u8, name: []const u8) !void {
    const max_len: usize = @intCast(api.IB_MAX_TABLE_NAME_LEN);
    var table_name_buf: [max_len]u8 = undefined;
    const table_name = try std.fmt.bufPrint(&table_name_buf, "{s}/{s}", .{ dbname, name });

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);
    try expectOk(api.ib_schema_lock_exclusive(trx));
    try expectOk(api.ib_table_drop(trx, table_name));
    try expectOk(api.ib_trx_commit(trx));
}

fn openTable(dbname: []const u8, name: []const u8, trx: api.ib_trx_t) !api.ib_crsr_t {
    const max_len: usize = @intCast(api.IB_MAX_TABLE_NAME_LEN);
    var table_name_buf: [max_len]u8 = undefined;
    const table_name = try std.fmt.bufPrint(&table_name_buf, "{s}/{s}", .{ dbname, name });

    var crsr: api.ib_crsr_t = null;
    try expectOk(api.ib_cursor_open_table(table_name, trx, &crsr));
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

fn expectRow(tpl: api.ib_tpl_t, c1: []const u8, c2: []const u8, c3: api.ib_u32_t) !void {
    const c1_val = colSlice(tpl, 0) orelse return error.MissingValue;
    const c2_val = colSlice(tpl, 1) orelse return error.MissingValue;
    try std.testing.expectEqualStrings(c1, c1_val);
    try std.testing.expectEqualStrings(c2, c2_val);
    var c3_val: api.ib_u32_t = 0;
    try expectOk(api.ib_tuple_read_u32(tpl, 2, &c3_val));
    try std.testing.expectEqual(c3, c3_val);
}

fn insertRows(crsr: api.ib_crsr_t) !void {
    const tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(tpl);

    for (in_rows) |row| {
        try expectOk(api.ib_col_set_value(tpl, 0, row.c1.ptr, @intCast(row.c1.len)));
        try expectOk(api.ib_col_set_value(tpl, 1, row.c2.ptr, @intCast(row.c2.len)));
        try expectOk(api.ib_col_set_value(tpl, 2, &row.c3, @sizeOf(api.ib_u32_t)));
        try expectOk(api.ib_cursor_insert_row(crsr, tpl));
        _ = api.ib_tuple_clear(tpl);
    }
}

fn moveto(
    crsr: api.ib_crsr_t,
    c1: []const u8,
    c2: ?[]const u8,
    match_mode: api.ib_match_mode_t,
    res_out: *i32,
) !api.ib_err_t {
    const key_tpl = api.ib_sec_search_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(key_tpl);

    try expectOk(api.ib_col_set_value(key_tpl, 0, c1.ptr, @intCast(c1.len)));
    if (c2) |val| {
        try expectOk(api.ib_col_set_value(key_tpl, 1, val.ptr, @intCast(val.len)));
    }
    api.ib_cursor_set_match_mode(crsr, match_mode);

    var res: i32 = 0;
    const err = api.ib_cursor_moveto(crsr, key_tpl, .IB_CUR_GE, &res);
    res_out.* = res;
    return err;
}

fn queryExactC1C2(crsr: api.ib_crsr_t) !void {
    var res: i32 = 0;
    try expectOk(try moveto(crsr, "abc", "def", .IB_EXACT_MATCH, &res));
    const tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(tpl);

    try expectOk(api.ib_cursor_read_row(crsr, tpl));
    try expectRow(tpl, "abc", "def", 1);
    try expectOk(api.ib_cursor_reset(crsr));
}

fn queryC1Equals(crsr: api.ib_crsr_t, c1: []const u8) !usize {
    var res: i32 = 0;
    try expectOk(try moveto(crsr, c1, null, .IB_CLOSEST_MATCH, &res));

    const tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(tpl);

    var count: usize = 0;
    while (true) {
        const err = api.ib_cursor_read_row(crsr, tpl);
        if (err != .DB_SUCCESS) {
            break;
        }
        const c1_val = colSlice(tpl, 0) orelse break;
        if (!std.mem.eql(u8, c1_val, c1)) {
            break;
        }
        count += 1;
        const next_err = api.ib_cursor_next(crsr);
        if (next_err != .DB_SUCCESS) {
            break;
        }
        _ = api.ib_tuple_clear(tpl);
    }

    try expectOk(api.ib_cursor_reset(crsr));
    return count;
}

fn queryC1AtLeast(crsr: api.ib_crsr_t, prefix: []const u8) !usize {
    var res: i32 = 0;
    try expectOk(try moveto(crsr, prefix, null, .IB_CLOSEST_MATCH, &res));

    const tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(tpl);

    var count: usize = 0;
    while (true) {
        const err = api.ib_cursor_read_row(crsr, tpl);
        if (err != .DB_SUCCESS) {
            break;
        }
        const c1_val = colSlice(tpl, 0) orelse break;
        if (std.mem.order(u8, c1_val, prefix) == .lt) {
            break;
        }
        count += 1;
        const next_err = api.ib_cursor_next(crsr);
        if (next_err != .DB_SUCCESS) {
            break;
        }
        _ = api.ib_tuple_clear(tpl);
    }

    try expectOk(api.ib_cursor_reset(crsr));
    return count;
}

fn queryC1C2Prefix(crsr: api.ib_crsr_t, c1: []const u8, c2_prefix: []const u8) !usize {
    var res: i32 = 0;
    try expectOk(try moveto(crsr, c1, c2_prefix, .IB_EXACT_PREFIX, &res));

    const tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(tpl);

    var count: usize = 0;
    while (true) {
        const err = api.ib_cursor_read_row(crsr, tpl);
        if (err != .DB_SUCCESS) {
            break;
        }
        const c1_val = colSlice(tpl, 0) orelse break;
        const c2_val = colSlice(tpl, 1) orelse break;
        if (!std.mem.eql(u8, c1_val, c1)) {
            break;
        }
        if (std.mem.order(u8, c2_val, c2_prefix) != .lt) {
            count += 1;
        }
        const next_err = api.ib_cursor_next(crsr);
        if (next_err != .DB_SUCCESS) {
            break;
        }
        _ = api.ib_tuple_clear(tpl);
    }

    try expectOk(api.ib_cursor_reset(crsr));
    return count;
}

test "ib search harness" {
    _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
    try expectOk(api.ib_init());
    try expectOk(api.ib_startup("barracuda"));
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);

    try createDatabase();
    try createTable(DATABASE, TABLE);

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);

    const crsr = try openTable(DATABASE, TABLE, trx);
    defer _ = api.ib_cursor_close(crsr);

    try expectOk(api.ib_cursor_lock(crsr, .IB_LOCK_IX));
    try insertRows(crsr);

    try queryExactC1C2(crsr);
    const abc_rows = try queryC1Equals(crsr, "abc");
    try std.testing.expectEqual(@as(usize, 2), abc_rows);

    const g_rows = try queryC1AtLeast(crsr, "g");
    try std.testing.expectEqual(@as(usize, 4), g_rows);

    const mno_x_rows = try queryC1C2Prefix(crsr, "mno", "x");
    try std.testing.expectEqual(@as(usize, 1), mno_x_rows);

    const mno_z_rows = try queryC1C2Prefix(crsr, "mno", "z");
    try std.testing.expectEqual(@as(usize, 0), mno_z_rows);

    try expectOk(api.ib_trx_commit(trx));

    try dropTable(DATABASE, TABLE);
    try dropDatabase();
}
