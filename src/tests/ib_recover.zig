const std = @import("std");
const api = @import("../api/api.zig");
const compat = @import("../ut/compat.zig");

const DATABASE: []const u8 = "test";
const TABLE: []const u8 = "t";

const N_TRX: usize = 10;
const N_RECS: usize = 100;

const C2_MAX_LEN: usize = 256;
const C3_MAX_LEN: usize = 8192;

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

    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c1", .IB_INT, .IB_COL_NONE, 0, 4));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c2", .IB_VARCHAR, .IB_COL_NONE, 0, @intCast(C2_MAX_LEN)));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c3", .IB_BLOB, .IB_COL_NONE, 0, 0));

    try expectOk(api.ib_table_schema_add_index(tbl_sch, "c1", &idx_sch));
    try expectOk(api.ib_index_schema_add_col(idx_sch, "c1", 0));
    try expectOk(api.ib_index_schema_set_clustered(idx_sch));

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

fn genRandText(rng: *std.Random, buf: []u8, max_len: usize) []u8 {
    std.debug.assert(buf.len >= max_len);
    std.debug.assert(max_len > 1);

    const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    const len = 1 + rng.uintLessThan(usize, max_len - 1);
    for (buf[0..len]) |*ch| {
        const idx = rng.uintLessThan(usize, charset.len);
        ch.* = charset[idx];
    }
    return buf[0..len];
}

fn insertRows(crsr: api.ib_crsr_t, start: i32, count: usize, rng: *std.Random) !api.ib_err_t {
    var dups: usize = 0;

    const tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(tpl);

    var scratch: [C3_MAX_LEN]u8 = undefined;

    var i: usize = 0;
    while (i < count) : (i += 1) {
        const key = start + @as(i32, @intCast(i));
        try expectOk(api.ib_tuple_write_i32(tpl, 0, key));

        const c2 = genRandText(rng, scratch[0..C2_MAX_LEN], C2_MAX_LEN);
        try expectOk(api.ib_col_set_value(tpl, 1, c2.ptr, @intCast(c2.len)));

        const c3 = genRandText(rng, scratch[0..C3_MAX_LEN], C3_MAX_LEN);
        try expectOk(api.ib_col_set_value(tpl, 2, c3.ptr, @intCast(c3.len)));

        const err = api.ib_cursor_insert_row(crsr, tpl);
        if (err == .DB_DUPLICATE_KEY) {
            dups += 1;
        } else {
            try expectOk(err);
        }

        _ = api.ib_tuple_clear(tpl);
    }

    try std.testing.expect(dups == 0 or dups == count);
    return if (dups == count) .DB_DUPLICATE_KEY else .DB_SUCCESS;
}

fn runPhase(crsr: api.ib_crsr_t, rng: *std.Random) !api.ib_err_t {
    var dup_trx: usize = 0;
    var i: usize = 0;
    while (i < N_TRX) : (i += 1) {
        const err = try insertRows(crsr, @intCast(i * N_RECS), N_RECS, rng);
        if (err == .DB_DUPLICATE_KEY) {
            dup_trx += 1;
        } else {
            try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
        }
    }
    try std.testing.expect(dup_trx == 0 or dup_trx == N_TRX);
    return if (dup_trx == N_TRX) .DB_DUPLICATE_KEY else .DB_SUCCESS;
}

test "ib recover harness" {
    _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
    try expectOk(api.ib_init());
    try expectOk(api.ib_startup("barracuda"));
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);

    try createDatabase();
    try createTable(DATABASE, TABLE);

    var prng = std.Random.DefaultPrng.init(0x1badbeef);
    var rng = prng.random();

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);

    const crsr = try openTable(DATABASE, TABLE, trx);
    defer _ = api.ib_cursor_close(crsr);

    try expectOk(api.ib_cursor_lock(crsr, .IB_LOCK_IX));

    var err = try runPhase(crsr, &rng);
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);

    err = try runPhase(crsr, &rng);
    try std.testing.expectEqual(api.ib_err_t.DB_DUPLICATE_KEY, err);

    try expectOk(api.ib_trx_commit(trx));

    try dropTable(DATABASE, TABLE);
    try dropDatabase();
}
