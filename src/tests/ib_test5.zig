const std = @import("std");
const api = @import("../api/api.zig");
const compat = @import("../ut/compat.zig");

const DATABASE: []const u8 = "test";
const TABLE: []const u8 = "t";
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

    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c1", .IB_VARCHAR, .IB_COL_NONE, 0, @intCast(VcharMax)));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c2", .IB_BLOB, .IB_COL_NONE, 0, 0));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c3", .IB_INT, .IB_COL_NONE, 0, 4));

    try expectOk(api.ib_table_schema_add_index(tbl_sch, "PRIMARY", &idx_sch));
    try expectOk(api.ib_index_schema_add_col(idx_sch, "c1", 0));
    try expectOk(api.ib_index_schema_set_clustered(idx_sch));
    try expectOk(api.ib_index_schema_set_unique(idx_sch));

    try expectOk(api.ib_table_schema_add_index(tbl_sch, "c3", &idx_sch));
    try expectOk(api.ib_index_schema_add_col(idx_sch, "c3", 0));

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
        const c3: api.ib_i32_t = @intCast(i % 10);

        try expectOk(api.ib_col_set_value(tpl, 0, vchar.ptr, @intCast(vchar.len)));
        try expectOk(api.ib_col_set_value(tpl, 1, blob.ptr, @intCast(blob.len)));
        try expectOk(api.ib_tuple_write_i32(tpl, 2, c3));
        const err = api.ib_cursor_insert_row(crsr, tpl);
        if (err != .DB_SUCCESS and err != .DB_DUPLICATE_KEY) {
            try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
        }
        _ = api.ib_tuple_clear(tpl);
    }
}

fn updateRandomRow(crsr: api.ib_crsr_t, rng: *std.Random) !void {
    const key: api.ib_i32_t = @intCast(rng.uintLessThan(u32, 10));

    const old_tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(old_tpl);
    const new_tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(new_tpl);

    var err = api.ib_cursor_first(crsr);
    if (err != .DB_SUCCESS) {
        return;
    }

    var vchar_buf: [VcharMax]u8 = undefined;
    var blob_buf: [BlobMax]u8 = undefined;

    while (err == .DB_SUCCESS) {
        err = api.ib_cursor_read_row(crsr, old_tpl);
        if (err != .DB_SUCCESS) {
            break;
        }

        var c3: api.ib_i32_t = 0;
        try expectOk(api.ib_tuple_read_i32(old_tpl, 2, &c3));
        if (c3 == key) {
            try expectOk(api.ib_tuple_copy(new_tpl, old_tpl));
            const new_vchar = genRandText(rng, &vchar_buf, VcharMax);
            const new_blob = genRandText(rng, &blob_buf, BlobMax);
            const new_c3: api.ib_i32_t = @mod(c3 + 1, @as(api.ib_i32_t, 10));

            try expectOk(api.ib_col_set_value(new_tpl, 0, new_vchar.ptr, @intCast(new_vchar.len)));
            try expectOk(api.ib_col_set_value(new_tpl, 1, new_blob.ptr, @intCast(new_blob.len)));
            try expectOk(api.ib_tuple_write_i32(new_tpl, 2, new_c3));

            const upd_err = api.ib_cursor_update_row(crsr, old_tpl, new_tpl);
            if (upd_err != .DB_SUCCESS and upd_err != .DB_DUPLICATE_KEY) {
                try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, upd_err);
            }
            break;
        }

        err = api.ib_cursor_next(crsr);
        if (err != .DB_SUCCESS) {
            break;
        }
        _ = api.ib_tuple_clear(old_tpl);
        _ = api.ib_tuple_clear(new_tpl);
    }
}

test "ib test5 harness" {
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
