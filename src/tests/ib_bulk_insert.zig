const std = @import("std");
const api = @import("../api/api.zig");
const compat = @import("../ut/compat.zig");

const DATABASE: []const u8 = "bulk_test";
const TABLE: []const u8 = "massive_data";
const TABLE_NAME: []const u8 = DATABASE ++ "/" ++ TABLE;

const PerfStats = struct {
    start_us: u64 = 0,
    end_us: u64 = 0,
    rows_inserted: u64 = 0,
    batches_completed: u64 = 0,
    total_bytes: u64 = 0,
};

const BatchContext = struct {
    trx: api.ib_trx_t,
    cursor: api.ib_crsr_t,
};

fn nowUs() u64 {
    const now = std.time.microTimestamp();
    return if (now < 0) 0 else @as(u64, @intCast(now));
}

fn expectOk(err: api.ib_err_t) !void {
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
}

fn createDatabase(dbname: []const u8) !void {
    try std.testing.expectEqual(compat.IB_TRUE, api.ib_database_create(dbname));
}

fn createBulkTable() !void {
    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);

    try expectOk(api.ib_schema_lock_exclusive(trx));

    var tbl_sch: api.ib_tbl_sch_t = null;
    try expectOk(api.ib_table_schema_create(TABLE_NAME, &tbl_sch, .IB_TBL_COMPACT, 0));
    defer api.ib_table_schema_delete(tbl_sch);

    try expectOk(api.ib_table_schema_add_col(tbl_sch, "id", .IB_INT, .IB_COL_UNSIGNED, 0, 8));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "user_id", .IB_INT, .IB_COL_UNSIGNED, 0, 4));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "name", .IB_VARCHAR, .IB_COL_NONE, 0, 100));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "email", .IB_VARCHAR, .IB_COL_NONE, 0, 255));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "score", .IB_DOUBLE, .IB_COL_NONE, 0, 8));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "created_at", .IB_INT, .IB_COL_UNSIGNED, 0, 4));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "data_blob", .IB_BLOB, .IB_COL_NONE, 0, 0));

    var primary_idx: api.ib_idx_sch_t = null;
    try expectOk(api.ib_table_schema_add_index(tbl_sch, "PRIMARY_KEY", &primary_idx));
    try expectOk(api.ib_index_schema_add_col(primary_idx, "id", 0));
    try expectOk(api.ib_index_schema_set_clustered(primary_idx));

    var user_idx: api.ib_idx_sch_t = null;
    try expectOk(api.ib_table_schema_add_index(tbl_sch, "IDX_USER_ID", &user_idx));
    try expectOk(api.ib_index_schema_add_col(user_idx, "user_id", 0));

    var table_id: api.ib_id_t = 0;
    const create_err = api.ib_table_create(trx, tbl_sch, &table_id);
    if (create_err != .DB_SUCCESS and create_err != .DB_TABLE_IS_BEING_USED) {
        try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, create_err);
    }

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
    const domain = domains[rng.uintLessThan(usize, domains.len)];
    const out = std.fmt.bufPrint(buf, "{s}@{s}", .{ username, domain }) catch return buf[0..0];
    return out;
}

fn openBatch(table_name: []const u8) !BatchContext {
    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);

    var cursor: api.ib_crsr_t = null;
    try expectOk(api.ib_cursor_open_table(table_name, trx, &cursor));
    errdefer _ = api.ib_cursor_close(cursor);

    try expectOk(api.ib_cursor_lock(cursor, .IB_LOCK_IX));
    return .{ .trx = trx, .cursor = cursor };
}

fn closeBatch(ctx: *BatchContext) !void {
    try expectOk(api.ib_trx_commit(ctx.trx));
    try expectOk(api.ib_cursor_close(ctx.cursor));
}

fn bulkInsertRange(start_row: u64, end_row: u64, batch_size: u64, rng: *std.Random) !PerfStats {
    std.debug.assert(start_row <= end_row);
    var stats = PerfStats{ .start_us = nowUs() };

    var ctx = try openBatch(TABLE_NAME);
    const tpl = api.ib_clust_read_tuple_create(ctx.cursor) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(tpl);

    var name_buf: [101]u8 = undefined;
    var email_buf: [256]u8 = undefined;
    var blob_buf: [1024]u8 = undefined;
    const timestamp: u32 = @intCast(@as(u64, @intCast(nowUs() / 1_000_000)));

    var row = start_row;
    while (row < end_row) : (row += 1) {
        const name = generateRandomString(rng, &name_buf, 10, 50);
        const email = generateRandomEmail(rng, &email_buf);
        const blob = generateRandomString(rng, &blob_buf, 100, 200);

        const row_id = row;
        const user_id: u32 = @intCast((row % 100000) + 1);
        const score: f64 = @as(f64, @floatFromInt(rng.uintLessThan(u32, 10000))) / 100.0;

        try expectOk(api.ib_col_set_value(tpl, 0, @ptrCast(&row_id), @sizeOf(u64)));
        try expectOk(api.ib_col_set_value(tpl, 1, @ptrCast(&user_id), @sizeOf(u32)));
        try expectOk(api.ib_col_set_value(tpl, 2, @ptrCast(name.ptr), @intCast(name.len)));
        try expectOk(api.ib_col_set_value(tpl, 3, @ptrCast(email.ptr), @intCast(email.len)));
        try expectOk(api.ib_col_set_value(tpl, 4, @ptrCast(&score), @sizeOf(f64)));
        try expectOk(api.ib_col_set_value(tpl, 5, @ptrCast(&timestamp), @sizeOf(u32)));
        try expectOk(api.ib_col_set_value(tpl, 6, @ptrCast(blob.ptr), @intCast(blob.len)));

        const insert_err = api.ib_cursor_insert_row(ctx.cursor, tpl);
        if (insert_err != .DB_SUCCESS and insert_err != .DB_DUPLICATE_KEY) {
            try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, insert_err);
        }

        stats.rows_inserted += 1;
        stats.total_bytes += @as(u64, @intCast(name.len + email.len + blob.len + 24));

        const batch_pos = row - start_row + 1;
        if (batch_size > 0 and batch_pos % batch_size == 0) {
            try closeBatch(&ctx);
            stats.batches_completed += 1;
            ctx = try openBatch(TABLE_NAME);
        }

        _ = api.ib_tuple_clear(tpl);
    }

    if (batch_size > 0 and (stats.rows_inserted % batch_size) != 0) {
        try closeBatch(&ctx);
        stats.batches_completed += 1;
    } else {
        try closeBatch(&ctx);
    }
    stats.end_us = nowUs();
    return stats;
}

test "ib bulk insert harness" {
    _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
    try expectOk(api.ib_init());
    try expectOk(api.ib_startup("barracuda"));
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);

    try createDatabase(DATABASE);
    try createBulkTable();

    const total_rows: u64 = 50;
    const batch_size: u64 = 10;
    const num_workers: u64 = 2;

    var prng = std.Random.DefaultPrng.init(0x5eed);
    var rng = prng.random();

    var workers_stats: [num_workers]PerfStats = undefined;
    const rows_per_worker = total_rows / num_workers;
    const remaining = total_rows % num_workers;

    var i: u64 = 0;
    while (i < num_workers) : (i += 1) {
        const start_row = i * rows_per_worker + 1;
        var end_row = (i + 1) * rows_per_worker;
        if (i == num_workers - 1) {
            end_row += remaining;
        }
        end_row += 1;
        workers_stats[@as(usize, @intCast(i))] = try bulkInsertRange(start_row, end_row, batch_size, &rng);
    }

    var total_inserted: u64 = 0;
    var total_batches: u64 = 0;
    var total_bytes: u64 = 0;
    for (workers_stats) |stats| {
        try std.testing.expect(stats.end_us >= stats.start_us);
        total_inserted += stats.rows_inserted;
        total_batches += stats.batches_completed;
        total_bytes += stats.total_bytes;
    }

    var expected_batches: u64 = 0;
    i = 0;
    while (i < num_workers) : (i += 1) {
        const rows = workers_stats[@as(usize, @intCast(i))].rows_inserted;
        expected_batches += if (batch_size == 0) 0 else (rows + batch_size - 1) / batch_size;
    }
    try std.testing.expectEqual(total_rows, total_inserted);
    try std.testing.expect(total_bytes > 0);
    try std.testing.expectEqual(expected_batches, total_batches);
}
