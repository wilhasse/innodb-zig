const std = @import("std");
const api = @import("../api/api.zig");
const compat = @import("../ut/compat.zig");
const mt = @import("ib_mt_common.zig");

fn expectOk(err: api.ib_err_t) !void {
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
}

fn selectStub(_: *mt.CbArgs) api.ib_err_t {
    return .DB_SUCCESS;
}

fn dropBase(arg: *mt.CbArgs) api.ib_err_t {
    const tbl = arg.tbl;
    const dbname = mt.sliceName(&tbl.db_name);
    const name = mt.sliceName(&tbl.name);

    const max_len: usize = @intCast(api.IB_MAX_TABLE_NAME_LEN);
    var table_name_buf: [max_len]u8 = undefined;
    const table_name = std.fmt.bufPrint(&table_name_buf, "{s}/{s}", .{ dbname, name }) catch {
        mt.updateErrStats(arg.err_st, .DB_INVALID_INPUT);
        return .DB_INVALID_INPUT;
    };

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return .DB_OUT_OF_MEMORY;

    var err = api.ib_schema_lock_exclusive(trx);
    if (err != .DB_SUCCESS) {
        _ = api.ib_trx_rollback(trx);
        mt.updateErrStats(arg.err_st, err);
        return err;
    }

    err = api.ib_table_drop(trx, table_name);
    if (err != .DB_SUCCESS and err != .DB_TABLE_NOT_FOUND and err != .DB_TABLESPACE_DELETED) {
        _ = api.ib_trx_rollback(trx);
        mt.updateErrStats(arg.err_st, err);
        return err;
    }

    err = api.ib_trx_commit(trx);
    mt.updateErrStats(arg.err_st, err);
    return err;
}

fn truncateBase(arg: *mt.CbArgs) api.ib_err_t {
    const tbl = arg.tbl;
    const dbname = mt.sliceName(&tbl.db_name);
    const name = mt.sliceName(&tbl.name);

    const max_len: usize = @intCast(api.IB_MAX_TABLE_NAME_LEN);
    var table_name_buf: [max_len]u8 = undefined;
    const table_name = std.fmt.bufPrint(&table_name_buf, "{s}/{s}", .{ dbname, name }) catch {
        mt.updateErrStats(arg.err_st, .DB_INVALID_INPUT);
        return .DB_INVALID_INPUT;
    };

    var table_id: api.ib_id_t = 0;
    const err = api.ib_table_truncate(table_name, &table_id);
    mt.updateErrStats(arg.err_st, err);
    return err;
}

fn updateBase(_: *mt.CbArgs) api.ib_err_t {
    return .DB_SUCCESS;
}

fn insertBase(_: *mt.CbArgs) api.ib_err_t {
    return .DB_SUCCESS;
}

fn deleteBase(_: *mt.CbArgs) api.ib_err_t {
    return .DB_SUCCESS;
}

fn createBase(_: *mt.CbArgs) api.ib_err_t {
    return .DB_SUCCESS;
}

fn alterBase(_: *mt.CbArgs) api.ib_err_t {
    return .DB_SUCCESS;
}

pub fn registerBaseTable(tbl: *mt.TblClass) void {
    mt.setName(&tbl.name, "dummy");

    tbl.dml_fn[@intFromEnum(mt.DmlOpType.select)] = selectStub;
    tbl.dml_fn[@intFromEnum(mt.DmlOpType.update)] = updateBase;
    tbl.dml_fn[@intFromEnum(mt.DmlOpType.insert)] = insertBase;
    tbl.dml_fn[@intFromEnum(mt.DmlOpType.delete)] = deleteBase;

    tbl.ddl_fn[@intFromEnum(mt.DdlOpType.create)] = createBase;
    tbl.ddl_fn[@intFromEnum(mt.DdlOpType.drop)] = dropBase;
    tbl.ddl_fn[@intFromEnum(mt.DdlOpType.alter)] = alterBase;
    tbl.ddl_fn[@intFromEnum(mt.DdlOpType.truncate)] = truncateBase;
}

fn createBaseTable(tbl: *const mt.TblClass) !void {
    const dbname = mt.sliceName(&tbl.db_name);
    const name = mt.sliceName(&tbl.name);

    const max_len: usize = @intCast(api.IB_MAX_TABLE_NAME_LEN);
    var table_name_buf: [max_len]u8 = undefined;
    const table_name = try std.fmt.bufPrint(&table_name_buf, "{s}/{s}", .{ dbname, name });

    var tbl_sch: api.ib_tbl_sch_t = null;
    try expectOk(api.ib_table_schema_create(table_name, &tbl_sch, tbl.format, tbl.page_size));
    defer api.ib_table_schema_delete(tbl_sch);

    try expectOk(api.ib_table_schema_add_col(
        tbl_sch,
        "id",
        .IB_INT,
        .IB_COL_UNSIGNED,
        0,
        @sizeOf(api.ib_u32_t),
    ));

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);
    try expectOk(api.ib_schema_lock_exclusive(trx));
    var table_id: api.ib_id_t = 0;
    try expectOk(api.ib_table_create(trx, tbl_sch, &table_id));
    try expectOk(api.ib_trx_commit(trx));
}

test "ib mt base harness" {
    try expectOk(api.ib_init());
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
    try expectOk(api.ib_startup("barracuda"));

    try std.testing.expectEqual(compat.IB_TRUE, api.ib_database_create("mt_db"));

    var tbl = mt.TblClass{};
    mt.setName(&tbl.db_name, "mt_db");
    registerBaseTable(&tbl);

    try createBaseTable(&tbl);

    var stats = mt.OpErr{};
    var args = mt.CbArgs{
        .err_st = &stats,
        .tbl = &tbl,
    };

    const truncate_fn = tbl.ddl_fn[@intFromEnum(mt.DdlOpType.truncate)] orelse return error.MissingFn;
    try expectOk(truncate_fn(&args));

    const select_fn = tbl.dml_fn[@intFromEnum(mt.DmlOpType.select)] orelse return error.MissingFn;
    try expectOk(select_fn(&args));

    const drop_fn = tbl.ddl_fn[@intFromEnum(mt.DdlOpType.drop)] orelse return error.MissingFn;
    try expectOk(drop_fn(&args));

    try std.testing.expect(stats.n_ops >= 2);
}
