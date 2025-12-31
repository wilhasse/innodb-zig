const std = @import("std");
const api = @import("../api/api.zig");
const compat = @import("../ut/compat.zig");

fn expectOk(err: api.ib_err_t) !void {
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
}

test "tablespace file create and drop" {
    _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
    try expectOk(api.ib_init());

    const data_home = "tmp/ibd-test/";
    std.fs.cwd().makePath(data_home) catch {};
    defer std.fs.cwd().deleteTree(data_home) catch {};

    try expectOk(api.ib_cfg_set("data_home_dir", data_home));
    try expectOk(api.ib_cfg_set("file_per_table", compat.IB_TRUE));
    try expectOk(api.ib_startup("barracuda"));
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);

    const db = "test";
    const table = "io";
    const table_name = db ++ "/" ++ table;

    try std.testing.expectEqual(compat.IB_TRUE, api.ib_database_create(db));

    var tbl_sch: api.ib_tbl_sch_t = null;
    var idx_sch: api.ib_idx_sch_t = null;
    try expectOk(api.ib_table_schema_create(table_name, &tbl_sch, .IB_TBL_COMPACT, 0));
    defer api.ib_table_schema_delete(tbl_sch);

    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c1", .IB_INT, .IB_COL_NONE, 0, @sizeOf(api.ib_i32_t)));
    try expectOk(api.ib_table_schema_add_index(tbl_sch, "PRIMARY", &idx_sch));
    try expectOk(api.ib_index_schema_add_col(idx_sch, "c1", 0));
    try expectOk(api.ib_index_schema_set_clustered(idx_sch));

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);
    try expectOk(api.ib_schema_lock_exclusive(trx));
    var table_id: api.ib_id_t = 0;
    try expectOk(api.ib_table_create(trx, tbl_sch, &table_id));
    try expectOk(api.ib_trx_commit(trx));

    const ibd_path = data_home ++ "test/io.ibd";
    try std.fs.cwd().access(ibd_path, .{});

    const drop_trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(drop_trx);
    try expectOk(api.ib_schema_lock_exclusive(drop_trx));
    try expectOk(api.ib_table_drop(drop_trx, table_name));
    try expectOk(api.ib_trx_commit(drop_trx));

    if (std.fs.cwd().access(ibd_path, .{})) |_| {
        try std.testing.expect(false);
    } else |err| {
        if (err != error.FileNotFound and err != error.PathNotFound) {
            return err;
        }
    }
}
