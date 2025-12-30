const std = @import("std");
const api = @import("../api/api.zig");
const compat = @import("../ut/compat.zig");

const DATABASE: []const u8 = "test";
const TABLE: []const u8 = "blobt3";
const TABLE_NAME: []const u8 = DATABASE ++ "/" ++ TABLE;

const num_rows: usize = 32;
const batch_size: usize = 4;
const blob_len: usize = 128;

const ErrSlots: usize = @intFromEnum(api.ib_err_t.DB_DATA_MISMATCH);

const DmlStats = struct {
    n_ops: usize = 0,
    n_errs: usize = 0,
    errs: [ErrSlots]usize = [_]usize{0} ** ErrSlots,
    mutex: std.Thread.Mutex = .{},
};

const RandomData = struct {
    allocator: std.mem.Allocator,
    strings: [][]u8,
    lengths: []usize,
    prng: std.Random.DefaultPrng,

    fn init(allocator: std.mem.Allocator, count: usize, max_len: usize, seed: u64) !RandomData {
        var prng = std.Random.DefaultPrng.init(seed);
        const strings = try allocator.alloc([]u8, count);
        errdefer allocator.free(strings);
        const lengths = try allocator.alloc(usize, count);
        errdefer allocator.free(lengths);

        const prefixes = [_][]const u8{
            "kjgclgrtfuylfluyfyufyulfulfyyulofuyolfyufyufuyfyufyufyufyui",
            "khd",
            "kh",
        };

        for (strings, 0..) |_, i| {
            const prefix = prefixes[i % prefixes.len];
            const extra_max = if (max_len > prefix.len) max_len - prefix.len else 0;
            const extra = if (extra_max == 0)
                0
            else
                prng.random().intRangeAtMost(usize, 0, extra_max);
            const total = prefix.len + extra;
            const buf = try allocator.alloc(u8, total);
            std.mem.copyForwards(u8, buf[0..prefix.len], prefix);
            if (extra > 0) {
                var j: usize = 0;
                while (j < extra) : (j += 1) {
                    buf[prefix.len + j] = @as(u8, 'a') + @as(u8, @intCast(prng.random().intRangeAtMost(u8, 0, 25)));
                }
            }
            strings[i] = buf;
            lengths[i] = total;
        }

        return .{
            .allocator = allocator,
            .strings = strings,
            .lengths = lengths,
            .prng = prng,
        };
    }

    fn deinit(self: *RandomData) void {
        for (self.strings) |buf| {
            self.allocator.free(buf);
        }
        self.allocator.free(self.strings);
        self.allocator.free(self.lengths);
    }

    fn randIndex(self: *RandomData) usize {
        return self.prng.random().intRangeAtMost(usize, 0, self.strings.len - 1);
    }
};

fn expectOk(err: api.ib_err_t) !void {
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
}

fn updateErrStats(stats: *DmlStats, err: api.ib_err_t) void {
    stats.mutex.lock();
    defer stats.mutex.unlock();
    stats.n_ops += 1;
    if (err != .DB_SUCCESS) {
        stats.n_errs += 1;
        const idx: usize = @intCast(@intFromEnum(err));
        if (idx < stats.errs.len) {
            stats.errs[idx] += 1;
        }
    }
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

    try expectOk(api.ib_table_schema_add_col(tbl_sch, "A", .IB_INT, .IB_COL_UNSIGNED, 0, @sizeOf(api.ib_u32_t)));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "D", .IB_INT, .IB_COL_UNSIGNED, 0, @sizeOf(api.ib_u32_t)));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "B", .IB_BLOB, .IB_COL_NONE, 0, 0));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "C", .IB_BLOB, .IB_COL_NONE, 0, 0));

    try expectOk(api.ib_table_schema_add_index(tbl_sch, "PRIMARY", &idx_sch));
    try expectOk(api.ib_index_schema_add_col(idx_sch, "B", 10));
    try expectOk(api.ib_index_schema_add_col(idx_sch, "A", 0));
    try expectOk(api.ib_index_schema_add_col(idx_sch, "D", 0));
    try expectOk(api.ib_index_schema_set_clustered(idx_sch));
    try expectOk(api.ib_index_schema_set_unique(idx_sch));

    try expectOk(api.ib_table_schema_add_index(tbl_sch, "SEC_0", &idx_sch));
    try expectOk(api.ib_index_schema_add_col(idx_sch, "D", 0));

    try expectOk(api.ib_table_schema_add_index(tbl_sch, "SEC_1", &idx_sch));
    try expectOk(api.ib_index_schema_add_col(idx_sch, "A", 0));

    try expectOk(api.ib_table_schema_add_index(tbl_sch, "SEC_2", &idx_sch));
    try expectOk(api.ib_index_schema_add_col(idx_sch, "C", 255));
    try expectOk(api.ib_index_schema_add_col(idx_sch, "B", 255));

    try expectOk(api.ib_table_schema_add_index(tbl_sch, "SEC_3", &idx_sch));
    try expectOk(api.ib_index_schema_add_col(idx_sch, "B", 5));
    try expectOk(api.ib_index_schema_add_col(idx_sch, "C", 10));
    try expectOk(api.ib_index_schema_add_col(idx_sch, "A", 0));

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

fn insertOneRow(crsr: api.ib_crsr_t, tpl: api.ib_tpl_t, data: *RandomData) api.ib_err_t {
    const a_val: api.ib_u32_t = @intCast(data.randIndex());
    var err = api.ib_tuple_write_u32(tpl, 0, a_val);
    if (err != .DB_SUCCESS) return err;
    err = api.ib_tuple_write_u32(tpl, 1, 5);
    if (err != .DB_SUCCESS) return err;

    const b_idx = data.randIndex();
    err = api.ib_col_set_value(tpl, 2, data.strings[b_idx].ptr, @intCast(data.lengths[b_idx]));
    if (err != .DB_SUCCESS) return err;

    const c_idx = data.randIndex();
    err = api.ib_col_set_value(tpl, 3, data.strings[c_idx].ptr, @intCast(data.lengths[c_idx]));
    if (err != .DB_SUCCESS) return err;

    return api.ib_cursor_insert_row(crsr, tpl);
}

fn insertRowBatch(crsr: api.ib_crsr_t, count: usize, data: *RandomData, stats: *DmlStats) usize {
    const tpl = api.ib_clust_read_tuple_create(crsr) orelse return 0;
    defer api.ib_tuple_delete(tpl);

    var inserted: usize = 0;
    var i: usize = 0;
    while (i < count) : (i += 1) {
        const err = insertOneRow(crsr, tpl, data);
        updateErrStats(stats, err);
        if (err == .DB_SUCCESS) {
            inserted += 1;
            _ = api.ib_tuple_clear(tpl);
        } else {
            break;
        }
    }

    return inserted;
}

fn updateRowBatch(crsr: api.ib_crsr_t, count: usize, data: *RandomData, stats: *DmlStats) void {
    if (api.ib_cursor_first(crsr) != .DB_SUCCESS) {
        return;
    }

    const old_tpl = api.ib_clust_read_tuple_create(crsr) orelse return;
    defer api.ib_tuple_delete(old_tpl);
    const new_tpl = api.ib_clust_read_tuple_create(crsr) orelse return;
    defer api.ib_tuple_delete(new_tpl);

    var i: usize = 0;
    while (i < count) : (i += 1) {
        var err = api.ib_cursor_read_row(crsr, old_tpl);
        if (err != .DB_SUCCESS) {
            updateErrStats(stats, err);
            break;
        }

        err = api.ib_tuple_copy(new_tpl, old_tpl);
        if (err != .DB_SUCCESS) {
            updateErrStats(stats, err);
            break;
        }

        const b_idx = data.randIndex();
        err = api.ib_col_set_value(new_tpl, 2, data.strings[b_idx].ptr, @intCast(data.lengths[b_idx]));
        if (err != .DB_SUCCESS) {
            updateErrStats(stats, err);
            break;
        }

        err = api.ib_cursor_update_row(crsr, old_tpl, new_tpl);
        updateErrStats(stats, err);
        if (err != .DB_SUCCESS) {
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

fn deleteRowBatch(crsr: api.ib_crsr_t, count: usize, stats: *DmlStats) void {
    if (api.ib_cursor_first(crsr) != .DB_SUCCESS) {
        return;
    }

    var i: usize = 0;
    while (i < count) : (i += 1) {
        const err = api.ib_cursor_delete_row(crsr);
        updateErrStats(stats, err);
        if (err != .DB_SUCCESS) {
            break;
        }
    }
}

fn doQuery(crsr: api.ib_crsr_t, stats: *DmlStats) void {
    const tpl = api.ib_clust_read_tuple_create(crsr) orelse return;
    defer api.ib_tuple_delete(tpl);

    var err = api.ib_cursor_first(crsr);
    if (err != .DB_SUCCESS and err != .DB_END_OF_INDEX) {
        updateErrStats(stats, err);
        return;
    }

    while (err == .DB_SUCCESS) {
        err = api.ib_cursor_read_row(crsr, tpl);
        if (err == .DB_END_OF_INDEX or err == .DB_RECORD_NOT_FOUND) {
            break;
        }
        if (err != .DB_SUCCESS) {
            updateErrStats(stats, err);
            break;
        }

        err = api.ib_cursor_next(crsr);
        if (err == .DB_END_OF_INDEX or err == .DB_RECORD_NOT_FOUND) {
            break;
        }
        if (err != .DB_SUCCESS) {
            updateErrStats(stats, err);
            break;
        }

        _ = api.ib_tuple_clear(tpl);
    }
}

test "ib mt stress harness" {
    try expectOk(api.ib_init());
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
    try expectOk(api.ib_startup("barracuda"));

    try createDatabase();
    try createTable();

    var data = try RandomData.init(std.testing.allocator, num_rows, blob_len, 0x1234_5678);
    defer data.deinit();

    var ins_stats = DmlStats{};
    var upd_stats = DmlStats{};
    var del_stats = DmlStats{};
    var sel_stats = DmlStats{};

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);
    var crsr: api.ib_crsr_t = null;
    try expectOk(api.ib_cursor_open_table(TABLE_NAME, trx, &crsr));
    defer _ = api.ib_cursor_close(crsr);
    try expectOk(api.ib_cursor_lock(crsr, .IB_LOCK_IX));

    _ = insertRowBatch(crsr, batch_size, &data, &ins_stats);
    updateRowBatch(crsr, batch_size, &data, &upd_stats);
    deleteRowBatch(crsr, batch_size / 2, &del_stats);
    doQuery(crsr, &sel_stats);
    try expectOk(api.ib_trx_commit(trx));

    try dropTable();
    try dropDatabase();

    try std.testing.expect(ins_stats.n_ops > 0);
    try std.testing.expect(upd_stats.n_ops > 0);
}
