const std = @import("std");
const api = @import("../api/api.zig");
const compat = @import("../ut/compat.zig");

const DATABASE: []const u8 = "bulk_test_mysql";
const TABLE: []const u8 = "massive_data";
const TABLE_NAME: []const u8 = DATABASE ++ "/" ++ TABLE;

const TotalRows: usize = 30;

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

    try expectOk(api.ib_table_schema_create(TABLE_NAME, &tbl_sch, .IB_TBL_COMPACT, 0));
    defer api.ib_table_schema_delete(tbl_sch);

    try expectOk(api.ib_table_schema_add_col(tbl_sch, "id", .IB_INT, .IB_COL_UNSIGNED, 0, 8));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "user_id", .IB_INT, .IB_COL_UNSIGNED, 0, 4));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "name", .IB_VARCHAR, .IB_COL_NONE, 0, 100));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "email", .IB_VARCHAR, .IB_COL_NONE, 0, 255));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "score", .IB_DOUBLE, .IB_COL_NONE, 0, 8));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "created_at", .IB_INT, .IB_COL_UNSIGNED, 0, 4));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "data_blob", .IB_BLOB, .IB_COL_NONE, 0, 0));

    try expectOk(api.ib_table_schema_add_index(tbl_sch, "PRIMARY", &idx_sch));
    try expectOk(api.ib_index_schema_add_col(idx_sch, "id", 0));
    try expectOk(api.ib_index_schema_set_clustered(idx_sch));

    try expectOk(api.ib_table_schema_add_index(tbl_sch, "IDX_USER_ID", &idx_sch));
    try expectOk(api.ib_index_schema_add_col(idx_sch, "user_id", 0));

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);
    try expectOk(api.ib_schema_lock_exclusive(trx));
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

fn generateRandomString(rng: *std.Random, buf: []u8, min_len: usize, max_len: usize) []u8 {
    std.debug.assert(min_len <= max_len);
    std.debug.assert(max_len <= buf.len);
    const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ";
    const len = min_len + rng.uintLessThan(usize, max_len - min_len + 1);
    for (buf[0..len]) |*ch| {
        const idx = rng.uintLessThan(usize, charset.len);
        ch.* = charset[idx];
    }
    return buf[0..len];
}

fn generateRandomEmail(rng: *std.Random, buf: []u8) []u8 {
    const domains = [_][]const u8{ "gmail.com", "yahoo.com", "hotmail.com", "company.com", "test.org" };
    var username_buf: [64]u8 = undefined;
    const username = generateRandomString(rng, &username_buf, 5, 15);
    return std.fmt.bufPrint(buf, "{s}@{s}", .{ username, domains[rng.uintLessThan(usize, domains.len)] }) catch buf[0..0];
}

fn insertRows(crsr: api.ib_crsr_t, rng: *std.Random) !usize {
    const tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(tpl);

    var name_buf: [101]u8 = undefined;
    var email_buf: [256]u8 = undefined;
    var blob_buf: [256]u8 = undefined;

    var inserted: usize = 0;
    var row: usize = 1;
    while (row <= TotalRows) : (row += 1) {
        const name = generateRandomString(rng, &name_buf, 10, 50);
        const email = generateRandomEmail(rng, &email_buf);
        const blob = generateRandomString(rng, &blob_buf, 50, 150);

        const id: u64 = @intCast(row);
        const user_id: u32 = @intCast((row % 1000) + 1);
        const score: f64 = @as(f64, @floatFromInt(rng.uintLessThan(u32, 10000))) / 100.0;
        const created_at: u32 = 1_700_000_000;

        try expectOk(api.ib_col_set_value(tpl, 0, @ptrCast(&id), @sizeOf(u64)));
        try expectOk(api.ib_col_set_value(tpl, 1, @ptrCast(&user_id), @sizeOf(u32)));
        try expectOk(api.ib_col_set_value(tpl, 2, name.ptr, @intCast(name.len)));
        try expectOk(api.ib_col_set_value(tpl, 3, email.ptr, @intCast(email.len)));
        try expectOk(api.ib_col_set_value(tpl, 4, @ptrCast(&score), @sizeOf(f64)));
        try expectOk(api.ib_col_set_value(tpl, 5, @ptrCast(&created_at), @sizeOf(u32)));
        try expectOk(api.ib_col_set_value(tpl, 6, blob.ptr, @intCast(blob.len)));

        try expectOk(api.ib_cursor_insert_row(crsr, tpl));
        inserted += 1;
        _ = api.ib_tuple_clear(tpl);
    }

    return inserted;
}

test "mysql bulk insert harness (embedded)" {
    _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
    try expectOk(api.ib_init());
    try expectOk(api.ib_startup("barracuda"));
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);

    try createDatabase();
    try createTable();

    var prng = std.Random.DefaultPrng.init(0x5eed);
    var rng = prng.random();

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);

    var crsr: api.ib_crsr_t = null;
    try expectOk(api.ib_cursor_open_table(TABLE_NAME, trx, &crsr));
    defer _ = api.ib_cursor_close(crsr);

    try expectOk(api.ib_cursor_lock(crsr, .IB_LOCK_IX));
    const inserted = try insertRows(crsr, &rng);
    try std.testing.expectEqual(TotalRows, inserted);

    try expectOk(api.ib_trx_commit(trx));
    try dropTable();
    try dropDatabase();
}
