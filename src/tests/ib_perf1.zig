const std = @import("std");
const api = @import("../api/api.zig");
const compat = @import("../ut/compat.zig");

const DATABASE: []const u8 = "test";

const NumThreads: usize = 2;
const NumRows: usize = 64;
const BatchSize: usize = 16;
const PageSize: usize = 0;

const OpTimes = struct {
    times: [NumThreads]u64 = [_]u64{0} ** NumThreads,
    count: usize = 0,

    fn add(self: *OpTimes, elapsed_us: u64) void {
        std.debug.assert(self.count < self.times.len);
        self.times[self.count] = elapsed_us;
        self.count += 1;
    }

    fn slice(self: *const OpTimes) []const u64 {
        return self.times[0..self.count];
    }
};

const OpStats = struct {
    insert: OpTimes = .{},
    copy: OpTimes = .{},
    join: OpTimes = .{},
};

fn nowUs() u64 {
    const now = std.time.microTimestamp();
    return if (now < 0) 0 else @as(u64, @intCast(now));
}

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
    const fmt: api.ib_tbl_fmt_t = if (PageSize > 0) .IB_TBL_COMPRESSED else .IB_TBL_COMPACT;

    try expectOk(api.ib_table_schema_create(table_name, &tbl_sch, fmt, @intCast(PageSize)));
    defer api.ib_table_schema_delete(tbl_sch);

    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c1", .IB_INT, .IB_COL_UNSIGNED, 0, @sizeOf(api.ib_u32_t)));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c2", .IB_INT, .IB_COL_UNSIGNED, 0, @sizeOf(api.ib_u32_t)));

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

fn insertRows(crsr: api.ib_crsr_t, start: api.ib_u32_t, count: usize) !void {
    const tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(tpl);

    var i: usize = 0;
    while (i < count) : (i += 1) {
        const v: api.ib_u32_t = start + @as(api.ib_u32_t, @intCast(i));
        try expectOk(api.ib_tuple_write_u32(tpl, 0, v));
        try expectOk(api.ib_tuple_write_u32(tpl, 1, v));
        try expectOk(api.ib_cursor_insert_row(crsr, tpl));
        _ = api.ib_tuple_clear(tpl);
    }
}

fn copyTableBatch(dst_crsr: api.ib_crsr_t, src_crsr: api.ib_crsr_t, count: usize, positioned: *bool) !usize {
    if (!positioned.*) {
        try expectOk(api.ib_cursor_first(src_crsr));
        positioned.* = true;
    }

    const src_tpl = api.ib_clust_read_tuple_create(src_crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(src_tpl);
    const dst_tpl = api.ib_clust_read_tuple_create(dst_crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(dst_tpl);

    var copied: usize = 0;
    while (copied < count) {
        var v: api.ib_u32_t = 0;
        var err = api.ib_cursor_read_row(src_crsr, src_tpl);
        if (err != .DB_SUCCESS) {
            break;
        }
        try expectOk(api.ib_tuple_read_u32(src_tpl, 0, &v));
        try expectOk(api.ib_tuple_write_u32(dst_tpl, 0, v));
        try expectOk(api.ib_tuple_read_u32(src_tpl, 1, &v));
        try expectOk(api.ib_tuple_write_u32(dst_tpl, 1, v));
        try expectOk(api.ib_cursor_insert_row(dst_crsr, dst_tpl));

        copied += 1;
        _ = api.ib_tuple_clear(src_tpl);
        _ = api.ib_tuple_clear(dst_tpl);

        err = api.ib_cursor_next(src_crsr);
        if (err == .DB_END_OF_INDEX) {
            positioned.* = false;
            break;
        }
        try expectOk(err);
    }

    return copied;
}

fn joinOnC1(t1_crsr: api.ib_crsr_t, t2_crsr: api.ib_crsr_t) !usize {
    const t1_tpl = api.ib_clust_read_tuple_create(t1_crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(t1_tpl);
    const t2_tpl = api.ib_clust_read_tuple_create(t2_crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(t2_tpl);

    var t1_err = api.ib_cursor_first(t1_crsr);
    var t2_err = api.ib_cursor_first(t2_crsr);
    if (t1_err != .DB_SUCCESS or t2_err != .DB_SUCCESS) {
        return 0;
    }

    var count: usize = 0;
    while (t1_err == .DB_SUCCESS and t2_err == .DB_SUCCESS) {
        var t1_c1: api.ib_u32_t = 0;
        var t2_c1: api.ib_u32_t = 0;

        try expectOk(api.ib_cursor_read_row(t1_crsr, t1_tpl));
        try expectOk(api.ib_cursor_read_row(t2_crsr, t2_tpl));
        try expectOk(api.ib_tuple_read_u32(t1_tpl, 0, &t1_c1));
        try expectOk(api.ib_tuple_read_u32(t2_tpl, 0, &t2_c1));

        if (t1_c1 == t2_c1) {
            count += 1;
        }

        t1_err = api.ib_cursor_next(t1_crsr);
        t2_err = api.ib_cursor_next(t2_crsr);
        if (t1_err == .DB_END_OF_INDEX or t2_err == .DB_END_OF_INDEX) {
            break;
        }
    }

    try expectOk(api.ib_cursor_reset(t1_crsr));
    try expectOk(api.ib_cursor_reset(t2_crsr));

    return count;
}

fn runWorker(table_id: usize, stats: *OpStats) !void {
    var table1_buf: [32]u8 = undefined;
    var table2_buf: [32]u8 = undefined;
    const table1 = try std.fmt.bufPrint(&table1_buf, "T{d}", .{table_id});
    const table2 = try std.fmt.bufPrint(&table2_buf, "T{d}", .{table_id + 1});

    try createTable(DATABASE, table1);
    try createTable(DATABASE, table2);

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);

    const src_crsr = try openTable(DATABASE, table1, trx);
    defer _ = api.ib_cursor_close(src_crsr);
    const dst_crsr = try openTable(DATABASE, table2, trx);
    defer _ = api.ib_cursor_close(dst_crsr);

    try expectOk(api.ib_cursor_lock(src_crsr, .IB_LOCK_IX));

    var start = nowUs();
    var row: usize = 0;
    while (row < NumRows) : (row += BatchSize) {
        const batch = @min(BatchSize, NumRows - row);
        try insertRows(src_crsr, @intCast(row), batch);
        try expectOk(api.ib_cursor_reset(src_crsr));
    }
    stats.insert.add(nowUs() - start);

    try expectOk(api.ib_cursor_lock(src_crsr, .IB_LOCK_IS));
    try expectOk(api.ib_cursor_lock(dst_crsr, .IB_LOCK_IX));
    try expectOk(api.ib_cursor_reset(src_crsr));
    try expectOk(api.ib_cursor_reset(dst_crsr));

    start = nowUs();
    var positioned = false;
    var copied: usize = 0;
    while (copied < NumRows) {
        const batch = @min(BatchSize, NumRows - copied);
        const added = try copyTableBatch(dst_crsr, src_crsr, batch, &positioned);
        copied += added;
        if (added < batch) {
            break;
        }
    }
    stats.copy.add(nowUs() - start);
    try std.testing.expectEqual(NumRows, copied);

    try expectOk(api.ib_cursor_reset(src_crsr));
    try expectOk(api.ib_cursor_reset(dst_crsr));

    start = nowUs();
    const matched = try joinOnC1(src_crsr, dst_crsr);
    stats.join.add(nowUs() - start);
    try std.testing.expectEqual(NumRows, matched);

    try expectOk(api.ib_trx_commit(trx));

    try dropTable(DATABASE, table1);
    try dropTable(DATABASE, table2);
}

test "ib perf1 harness" {
    _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
    try expectOk(api.ib_init());
    try expectOk(api.ib_startup("barracuda"));
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);

    try createDatabase();

    var stats = OpStats{};
    for (0..NumThreads) |i| {
        try runWorker(i * 2, &stats);
    }

    try dropDatabase();

    try std.testing.expectEqual(NumThreads, stats.insert.count);
    try std.testing.expectEqual(NumThreads, stats.copy.count);
    try std.testing.expectEqual(NumThreads, stats.join.count);
    _ = stats.insert.slice();
    _ = stats.copy.slice();
    _ = stats.join.slice();
}
