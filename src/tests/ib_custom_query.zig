const std = @import("std");
const api = @import("../api/api.zig");
const compat = @import("../ut/compat.zig");

const DATABASE: []const u8 = "bulk_test";
const TABLE: []const u8 = "massive_data";
const TABLE_NAME: []const u8 = DATABASE ++ "/" ++ TABLE;

const QueryParams = struct {
    specific_id: u64 = 0,
    specific_user_id: u32 = 0,
    range_start: u64 = 0,
    range_end: u64 = 0,
    limit: u64 = 20,
    offset: u64 = 0,
    score_min: f64 = 0.0,
    score_max: f64 = 0.0,
    name_like: []const u8 = "",
    email_domain: []const u8 = "",
    count_only: bool = false,
    sample_mode: bool = false,
    use_specific_id: bool = false,
    use_specific_user_id: bool = false,
    use_range: bool = false,
    use_score_filter: bool = false,
    use_name_filter: bool = false,
    use_email_filter: bool = false,
};

const QueryResult = struct {
    processed: u64 = 0,
    matched: u64 = 0,
    displayed: u64 = 0,
};

fn expectOk(err: api.ib_err_t) !void {
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
}

fn createDatabase() !void {
    try std.testing.expectEqual(compat.IB_TRUE, api.ib_database_create(DATABASE));
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

    var table_id: api.ib_id_t = 0;
    const create_err = api.ib_table_create(trx, tbl_sch, &table_id);
    if (create_err != .DB_SUCCESS and create_err != .DB_TABLE_IS_BEING_USED) {
        try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, create_err);
    }

    try expectOk(api.ib_trx_commit(trx));
}

const SampleRow = struct {
    id: u64,
    user_id: u32,
    name: []const u8,
    email: []const u8,
    score: f64,
    created_at: u32,
    blob: []const u8,
};

fn insertSampleRows(crsr: api.ib_crsr_t, rows: []const SampleRow) !void {
    const tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(tpl);

    for (rows) |row| {
        try expectOk(api.ib_col_set_value(tpl, 0, @ptrCast(&row.id), @sizeOf(u64)));
        try expectOk(api.ib_col_set_value(tpl, 1, @ptrCast(&row.user_id), @sizeOf(u32)));
        try expectOk(api.ib_col_set_value(tpl, 2, @ptrCast(row.name.ptr), @intCast(row.name.len)));
        try expectOk(api.ib_col_set_value(tpl, 3, @ptrCast(row.email.ptr), @intCast(row.email.len)));
        try expectOk(api.ib_col_set_value(tpl, 4, @ptrCast(&row.score), @sizeOf(f64)));
        try expectOk(api.ib_col_set_value(tpl, 5, @ptrCast(&row.created_at), @sizeOf(u32)));
        try expectOk(api.ib_col_set_value(tpl, 6, @ptrCast(row.blob.ptr), @intCast(row.blob.len)));
        try expectOk(api.ib_cursor_insert_row(crsr, tpl));
        _ = api.ib_tuple_clear(tpl);
    }
}

fn colSlice(ib_tpl: api.ib_tpl_t, col: api.ib_ulint_t) ?[]const u8 {
    const ptr = api.ib_col_get_value(ib_tpl, col) orelse return null;
    const len = api.ib_col_get_len(ib_tpl, col);
    if (len == 0 or len == api.IB_SQL_NULL) {
        return null;
    }
    return @as([*]const u8, @ptrCast(ptr))[0..@intCast(len)];
}

fn rowMatchesFilters(ib_tpl: api.ib_tpl_t, params: *const QueryParams) bool {
    var id: u64 = 0;
    if (api.ib_tuple_read_u64(ib_tpl, 0, &id) != .DB_SUCCESS) {
        return false;
    }
    if (params.use_specific_id and id != params.specific_id) {
        return false;
    }
    if (params.use_range and (id < params.range_start or id > params.range_end)) {
        return false;
    }

    if (params.use_specific_user_id) {
        var user_id: u32 = 0;
        if (api.ib_tuple_read_u32(ib_tpl, 1, &user_id) != .DB_SUCCESS or user_id != params.specific_user_id) {
            return false;
        }
    }

    if (params.use_score_filter) {
        var score: f64 = 0;
        if (api.ib_tuple_read_double(ib_tpl, 4, &score) != .DB_SUCCESS) {
            return false;
        }
        if (score < params.score_min or score > params.score_max) {
            return false;
        }
    }

    if (params.use_name_filter) {
        const name = colSlice(ib_tpl, 2) orelse return false;
        if (std.mem.indexOf(u8, name, params.name_like) == null) {
            return false;
        }
    }

    if (params.use_email_filter) {
        const email = colSlice(ib_tpl, 3) orelse return false;
        if (std.mem.indexOf(u8, email, params.email_domain) == null) {
            return false;
        }
    }

    return true;
}

fn executeCustomQuery(crsr: api.ib_crsr_t, params: *const QueryParams) !QueryResult {
    var result = QueryResult{};
    const cursor = crsr orelse return error.OutOfMemory;
    const tpl = api.ib_clust_read_tuple_create(cursor) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(tpl);

    var err = api.ib_cursor_first(cursor);
    if (err != .DB_SUCCESS) {
        _ = api.ib_cursor_reset(cursor);
        return result;
    }

    while (err == .DB_SUCCESS) {
        err = api.ib_cursor_read_row(cursor, tpl);
        if (err != .DB_SUCCESS) {
            break;
        }
        result.processed += 1;
        if (rowMatchesFilters(tpl, params)) {
            if (result.matched < params.offset) {
                result.matched += 1;
            } else {
                result.displayed += 1;
                result.matched += 1;
                if (result.displayed >= params.limit) {
                    break;
                }
            }
        }

        err = api.ib_cursor_next(cursor);
        if (err != .DB_SUCCESS) {
            break;
        }

        _ = api.ib_tuple_clear(tpl);
    }

    _ = api.ib_cursor_reset(cursor);

    return result;
}

test "ib custom query harness" {
    try expectOk(api.ib_init());
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
    try expectOk(api.ib_startup("barracuda"));

    try createDatabase();
    try createBulkTable();

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    defer _ = api.ib_trx_commit(trx);

    var crsr: api.ib_crsr_t = null;
    try expectOk(api.ib_cursor_open_table(TABLE_NAME, trx, &crsr));
    defer _ = api.ib_cursor_close(crsr);
    try expectOk(api.ib_cursor_lock(crsr, .IB_LOCK_IX));

    const rows = [_]SampleRow{
        .{
            .id = 1,
            .user_id = 100,
            .name = "Alice",
            .email = "alice@gmail.com",
            .score = 10.5,
            .created_at = 1_700_000_000,
            .blob = "data1",
        },
        .{
            .id = 2,
            .user_id = 200,
            .name = "Bob",
            .email = "bob@yahoo.com",
            .score = 50.0,
            .created_at = 1_700_000_100,
            .blob = "data2",
        },
        .{
            .id = 3,
            .user_id = 100,
            .name = "Carol",
            .email = "carol@gmail.com",
            .score = 70.0,
            .created_at = 1_700_000_200,
            .blob = "data3",
        },
    };
    try insertSampleRows(crsr, &rows);

    var params = QueryParams{
        .limit = 10,
        .offset = 0,
        .use_email_filter = true,
        .email_domain = "gmail.com",
    };
    var result = try executeCustomQuery(crsr, &params);
    try std.testing.expectEqual(@as(u64, 3), result.processed);
    try std.testing.expectEqual(@as(u64, 2), result.matched);
    try std.testing.expectEqual(@as(u64, 2), result.displayed);

    params = QueryParams{
        .limit = 10,
        .offset = 0,
        .use_specific_id = true,
        .specific_id = 2,
    };
    result = try executeCustomQuery(crsr, &params);
    try std.testing.expectEqual(@as(u64, 3), result.processed);
    try std.testing.expectEqual(@as(u64, 1), result.matched);
    try std.testing.expectEqual(@as(u64, 1), result.displayed);

    params = QueryParams{
        .limit = 1,
        .offset = 1,
        .use_range = true,
        .range_start = 1,
        .range_end = 3,
    };
    result = try executeCustomQuery(crsr, &params);
    try std.testing.expectEqual(@as(u64, 2), result.processed);
    try std.testing.expectEqual(@as(u64, 2), result.matched);
    try std.testing.expectEqual(@as(u64, 1), result.displayed);
}
