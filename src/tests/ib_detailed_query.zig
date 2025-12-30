const std = @import("std");
const api = @import("../api/api.zig");
const compat = @import("../ut/compat.zig");

const DATABASE: []const u8 = "bulk_test";
const TABLE: []const u8 = "massive_data";
const TABLE_NAME: []const u8 = DATABASE ++ "/" ++ TABLE;

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

fn colSlice(ib_tpl: api.ib_tpl_t, col: api.ib_ulint_t) ?[]const u8 {
    const ptr = api.ib_col_get_value(ib_tpl, col) orelse return null;
    const len = api.ib_col_get_len(ib_tpl, col);
    if (len == 0 or len == api.IB_SQL_NULL) {
        return null;
    }
    return @as([*]const u8, @ptrCast(ptr))[0..@intCast(len)];
}

test "ib detailed query harness" {
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

    const tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(tpl);

    const id: u64 = 42;
    const user_id: u32 = 7;
    const name = "Alice";
    const email = "alice@example.com";
    const score: f64 = 12.34;
    const created_at: u32 = 1_700_000_000;
    const blob = "blobdata";

    try expectOk(api.ib_col_set_value(tpl, 0, @ptrCast(&id), @sizeOf(u64)));
    try expectOk(api.ib_col_set_value(tpl, 1, @ptrCast(&user_id), @sizeOf(u32)));
    try expectOk(api.ib_col_set_value(tpl, 2, @ptrCast(name.ptr), @intCast(name.len)));
    try expectOk(api.ib_col_set_value(tpl, 3, @ptrCast(email.ptr), @intCast(email.len)));
    try expectOk(api.ib_col_set_value(tpl, 4, @ptrCast(&score), @sizeOf(f64)));
    try expectOk(api.ib_col_set_value(tpl, 5, @ptrCast(&created_at), @sizeOf(u32)));
    try expectOk(api.ib_col_set_value(tpl, 6, @ptrCast(blob.ptr), @intCast(blob.len)));
    try expectOk(api.ib_cursor_insert_row(crsr, tpl));

    try expectOk(api.ib_cursor_first(crsr));
    const read_tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(read_tpl);
    try expectOk(api.ib_cursor_read_row(crsr, read_tpl));

    var out_id: u64 = 0;
    try expectOk(api.ib_tuple_read_u64(read_tpl, 0, &out_id));
    try std.testing.expectEqual(id, out_id);

    var out_user: u32 = 0;
    try expectOk(api.ib_tuple_read_u32(read_tpl, 1, &out_user));
    try std.testing.expectEqual(user_id, out_user);

    const name_slice = colSlice(read_tpl, 2) orelse return error.OutOfMemory;
    try std.testing.expect(std.mem.eql(u8, name_slice, name));
    try std.testing.expectEqual(@as(usize, name.len), @as(usize, @intCast(api.ib_col_get_len(read_tpl, 2))));

    const email_slice = colSlice(read_tpl, 3) orelse return error.OutOfMemory;
    try std.testing.expect(std.mem.eql(u8, email_slice, email));
    try std.testing.expectEqual(@as(usize, email.len), @as(usize, @intCast(api.ib_col_get_len(read_tpl, 3))));

    var out_score: f64 = 0;
    try expectOk(api.ib_tuple_read_double(read_tpl, 4, &out_score));
    try std.testing.expectApproxEqAbs(score, out_score, 0.0001);

    var out_created: u32 = 0;
    try expectOk(api.ib_tuple_read_u32(read_tpl, 5, &out_created));
    try std.testing.expectEqual(created_at, out_created);

    const blob_slice = colSlice(read_tpl, 6) orelse return error.OutOfMemory;
    try std.testing.expect(std.mem.eql(u8, blob_slice, blob));

    var meta: api.ib_col_meta_t = undefined;
    _ = api.ib_col_get_meta(read_tpl, 2, &meta);
    try std.testing.expectEqual(api.ib_col_type_t.IB_VARCHAR, meta.type);
    try std.testing.expectEqual(@as(api.ib_u32_t, 100), meta.type_len);

    _ = api.ib_col_get_meta(read_tpl, 3, &meta);
    try std.testing.expectEqual(api.ib_col_type_t.IB_VARCHAR, meta.type);
    try std.testing.expectEqual(@as(api.ib_u32_t, 255), meta.type_len);

    _ = api.ib_col_get_meta(read_tpl, 6, &meta);
    try std.testing.expectEqual(api.ib_col_type_t.IB_BLOB, meta.type);
    try std.testing.expectEqual(@as(api.ib_u32_t, 0), meta.type_len);
}
