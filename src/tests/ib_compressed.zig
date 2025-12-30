const std = @import("std");
const api = @import("../api/api.zig");
const compat = @import("../ut/compat.zig");
const build_options = @import("build_options");

const DBNAME: []const u8 = "test";
const TABLENAME: []const u8 = DBNAME ++ "/t_compressed";

fn expectOk(err: api.ib_err_t) !void {
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
}

test "ib compressed table page sizes" {
    if (!build_options.enable_compression) {
        return;
    }

    try expectOk(api.ib_init());
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);

    try expectOk(api.ib_startup("barracuda"));
    try expectOk(api.ib_cfg_set("file_per_table", compat.IB_TRUE));

    try std.testing.expectEqual(compat.IB_TRUE, api.ib_database_create(DBNAME));

    const valid_page_sizes = [_]api.ib_ulint_t{ 0, 1, 2, 4, 8, 16 };
    for (valid_page_sizes) |page_size| {
        var tbl_sch: api.ib_tbl_sch_t = null;
        try expectOk(api.ib_table_schema_create(TABLENAME, &tbl_sch, .IB_TBL_COMPRESSED, page_size));
        defer api.ib_table_schema_delete(tbl_sch);
        try expectOk(api.ib_table_schema_add_col(
            tbl_sch,
            "c1",
            .IB_INT,
            .IB_COL_UNSIGNED,
            0,
            @sizeOf(i32),
        ));

        const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
        try expectOk(api.ib_schema_lock_exclusive(trx));
        var table_id: api.ib_id_t = 0;
        try expectOk(api.ib_table_create(trx, tbl_sch, &table_id));
        try expectOk(api.ib_trx_commit(trx));

        const drop_trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
        try expectOk(api.ib_schema_lock_exclusive(drop_trx));
        try expectOk(api.ib_table_drop(drop_trx, TABLENAME));
        try expectOk(api.ib_trx_commit(drop_trx));
    }

    const invalid_page_sizes = [_]api.ib_ulint_t{ 3, 5, 6, 14, 17, 32, 128, 301 };
    for (invalid_page_sizes) |page_size| {
        var tbl_sch: api.ib_tbl_sch_t = null;
        const err = api.ib_table_schema_create(TABLENAME, &tbl_sch, .IB_TBL_COMPRESSED, page_size);
        try std.testing.expect(err != api.ib_err_t.DB_SUCCESS);
        if (tbl_sch) |schema| {
            api.ib_table_schema_delete(schema);
        }
    }

    _ = api.ib_database_drop(DBNAME);
}
