const std = @import("std");
const api = @import("../api/api.zig");
const compat = @import("../ut/compat.zig");

const DATABASE: []const u8 = "test";
const TABLE: []const u8 = "ib_test2";
const TABLE_NAME: []const u8 = DATABASE ++ "/" ++ TABLE;

const TablesCount: usize = 3;
const TrxPerTable: usize = 2;
const RowsPerInsert: usize = 20;

const VcharMax: usize = 128;
const BlobMax: usize = 256;

fn expectOk(err: api.ib_err_t) !void {
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
}

fn createDatabase() !void {
    try std.testing.expectEqual(compat.IB_TRUE, api.ib_database_create(DATABASE));
}

fn createTable() !void {
    var tbl_sch: api.ib_tbl_sch_t = null;
    var idx_sch: api.ib_idx_sch_t = null;

    try expectOk(api.ib_table_schema_create(TABLE_NAME, &tbl_sch, .IB_TBL_COMPACT, 0));
    defer api.ib_table_schema_delete(tbl_sch);

    try expectOk(api.ib_table_schema_add_col(tbl_sch, "vchar", .IB_VARCHAR, .IB_COL_NONE, 0, @intCast(VcharMax)));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "blob", .IB_BLOB, .IB_COL_NONE, 0, 0));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "count", .IB_INT, .IB_COL_UNSIGNED, 0, 4));

    try expectOk(api.ib_table_schema_add_index(tbl_sch, "PRIMARY", &idx_sch));
    try expectOk(api.ib_index_schema_add_col(idx_sch, "vchar", 0));
    try expectOk(api.ib_index_schema_set_clustered(idx_sch));
    try expectOk(api.ib_index_schema_set_unique(idx_sch));

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

fn genRandText(rng: *std.Random, buf: []u8, max_len: usize) []u8 {
    std.debug.assert(max_len > 1);
    std.debug.assert(max_len <= buf.len);
    const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    const len = 1 + rng.uintLessThan(usize, max_len - 1);
    for (buf[0..len]) |*ch| {
        const idx = rng.uintLessThan(usize, charset.len);
        ch.* = charset[idx];
    }
    return buf[0..len];
}

fn insertRandomRows(crsr: api.ib_crsr_t, rng: *std.Random) !void {
    const tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(tpl);

    var vchar_buf: [VcharMax]u8 = undefined;
    var blob_buf: [BlobMax]u8 = undefined;

    var i: usize = 0;
    while (i < RowsPerInsert) : (i += 1) {
        const vchar = genRandText(rng, &vchar_buf, VcharMax);
        const blob = genRandText(rng, &blob_buf, BlobMax);
        try expectOk(api.ib_col_set_value(tpl, 0, vchar.ptr, @intCast(vchar.len)));
        try expectOk(api.ib_col_set_value(tpl, 1, blob.ptr, @intCast(blob.len)));
        try expectOk(api.ib_tuple_write_u32(tpl, 2, 0));
        const err = api.ib_cursor_insert_row(crsr, tpl);
        if (err != .DB_SUCCESS and err != .DB_DUPLICATE_KEY) {
            try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
        }
        _ = api.ib_tuple_clear(tpl);
    }
}

fn updateRandomRow(crsr: api.ib_crsr_t, rng: *std.Random) !void {
    const key_tpl = api.ib_sec_search_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(key_tpl);

    var vchar_buf: [VcharMax]u8 = undefined;
    var blob_buf: [BlobMax]u8 = undefined;
    const key = genRandText(rng, &vchar_buf, VcharMax);
    try expectOk(api.ib_col_set_value(key_tpl, 0, key.ptr, @intCast(key.len)));

    var res: i32 = 0;
    const move_err = api.ib_cursor_moveto(crsr, key_tpl, .IB_CUR_GE, &res);
    if (move_err != .DB_SUCCESS and move_err != .DB_END_OF_INDEX and move_err != .DB_RECORD_NOT_FOUND) {
        try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, move_err);
    }

    const old_tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(old_tpl);
    const new_tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(new_tpl);

    const read_err = api.ib_cursor_read_row(crsr, old_tpl);
    if (read_err != .DB_SUCCESS) {
        return;
    }

    const c1_ptr = api.ib_col_get_value(old_tpl, 0) orelse return;
    const c1_len = api.ib_col_get_len(old_tpl, 0);
    const c1 = @as([*]const u8, @ptrCast(c1_ptr))[0..@intCast(c1_len)];
    if (!std.mem.eql(u8, c1, key)) {
        return;
    }

    try expectOk(api.ib_tuple_copy(new_tpl, old_tpl));
    var count: api.ib_u32_t = 0;
    try expectOk(api.ib_tuple_read_u32(old_tpl, 2, &count));
    count += 1;

    const new_vchar = genRandText(rng, &vchar_buf, VcharMax);
    try expectOk(api.ib_col_set_value(new_tpl, 0, new_vchar.ptr, @intCast(new_vchar.len)));
    const new_blob = genRandText(rng, &blob_buf, BlobMax);
    try expectOk(api.ib_col_set_value(new_tpl, 1, new_blob.ptr, @intCast(new_blob.len)));
    try expectOk(api.ib_tuple_write_u32(new_tpl, 2, count));

    const err = api.ib_cursor_update_row(crsr, old_tpl, new_tpl);
    if (err != .DB_SUCCESS and err != .DB_DUPLICATE_KEY) {
        try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
    }
}

test "ib test2 harness" {
    _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
    try expectOk(api.ib_init());
    try expectOk(api.ib_startup("barracuda"));
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);

    try createDatabase();

    var prng = std.Random.DefaultPrng.init(0x5eed);
    var rng = prng.random();

    var t: usize = 0;
    while (t < TablesCount) : (t += 1) {
        try createTable();
        var j: usize = 0;
        while (j < TrxPerTable) : (j += 1) {
            const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
            errdefer _ = api.ib_trx_rollback(trx);

            var crsr: api.ib_crsr_t = null;
            try expectOk(api.ib_cursor_open_table(TABLE_NAME, trx, &crsr));
            try expectOk(api.ib_cursor_lock(crsr, .IB_LOCK_IX));

            try insertRandomRows(crsr, &rng);
            try updateRandomRow(crsr, &rng);

            try expectOk(api.ib_cursor_close(crsr));
            try expectOk(api.ib_trx_commit(trx));
        }
        try dropTable();
    }
}
