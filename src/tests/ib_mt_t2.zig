const std = @import("std");
const api = @import("../api/api.zig");
const compat = @import("../ut/compat.zig");
const mt = @import("ib_mt_common.zig");
const mt_base = @import("ib_mt_base.zig");

const DATABASE: []const u8 = "test";

var next_id: api.ib_u32_t = 1;

fn expectOk(err: api.ib_err_t) !void {
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
}

fn create_t2(arg: *mt.CbArgs) api.ib_err_t {
    var tbl_sch: api.ib_tbl_sch_t = null;
    var idx_sch: api.ib_idx_sch_t = null;

    const dbname = mt.sliceName(&arg.tbl.db_name);
    const name = mt.sliceName(&arg.tbl.name);
    const max_len: usize = @intCast(api.IB_MAX_TABLE_NAME_LEN);
    var table_name_buf: [max_len]u8 = undefined;
    const table_name = std.fmt.bufPrint(&table_name_buf, "{s}/{s}", .{ dbname, name }) catch {
        mt.updateErrStats(arg.err_st, .DB_INVALID_INPUT);
        return .DB_INVALID_INPUT;
    };

    var err = api.ib_table_schema_create(table_name, &tbl_sch, arg.tbl.format, arg.tbl.page_size);
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }
    defer api.ib_table_schema_delete(tbl_sch);

    err = api.ib_table_schema_add_col(tbl_sch, "c1", .IB_INT, .IB_COL_UNSIGNED, 0, @sizeOf(api.ib_u32_t));
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }
    err = api.ib_table_schema_add_col(tbl_sch, "score", .IB_INT, .IB_COL_UNSIGNED, 0, @sizeOf(api.ib_u32_t));
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }
    err = api.ib_table_schema_add_col(tbl_sch, "ins_run", .IB_INT, .IB_COL_UNSIGNED, 0, @sizeOf(api.ib_u32_t));
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }
    err = api.ib_table_schema_add_col(tbl_sch, "upd_run", .IB_INT, .IB_COL_UNSIGNED, 0, @sizeOf(api.ib_u32_t));
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }

    err = api.ib_table_schema_add_index(tbl_sch, "PK_index", &idx_sch);
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }
    err = api.ib_index_schema_add_col(idx_sch, "c1", 0);
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }
    err = api.ib_index_schema_set_clustered(idx_sch);
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }

    const trx = api.ib_trx_begin(arg.isolation_level) orelse return .DB_OUT_OF_MEMORY;
    err = api.ib_schema_lock_exclusive(trx);
    if (err == .DB_SUCCESS) {
        var table_id: api.ib_id_t = 0;
        err = api.ib_table_create(trx, tbl_sch, &table_id);
    }
    if (err == .DB_SUCCESS) {
        err = api.ib_trx_commit(trx);
    } else {
        _ = api.ib_trx_rollback(trx);
    }
    mt.updateErrStats(arg.err_st, err);
    return err;
}

fn alter_t2(_: *mt.CbArgs) api.ib_err_t {
    return .DB_SUCCESS;
}

fn insert_t2(arg: *mt.CbArgs) api.ib_err_t {
    var crsr: api.ib_crsr_t = null;
    var err = mt.openTable(mt.sliceName(&arg.tbl.db_name), mt.sliceName(&arg.tbl.name), arg.trx, &crsr);
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }
    defer _ = api.ib_cursor_close(crsr);

    err = api.ib_cursor_lock(crsr, .IB_LOCK_IX);
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }
    err = api.ib_cursor_set_lock_mode(crsr, .IB_LOCK_X);
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }

    const tpl = api.ib_clust_read_tuple_create(crsr) orelse return .DB_OUT_OF_MEMORY;
    defer api.ib_tuple_delete(tpl);

    const batch = if (arg.batch_size <= 0) 1 else @as(usize, @intCast(arg.batch_size));
    var i: usize = 0;
    while (i < batch) : (i += 1) {
        const id_val: api.ib_u32_t = next_id;
        next_id += 1;

        err = api.ib_tuple_write_u32(tpl, 0, id_val);
        if (err != .DB_SUCCESS) break;
        err = api.ib_tuple_write_u32(tpl, 1, 0);
        if (err != .DB_SUCCESS) break;
        err = api.ib_tuple_write_u32(tpl, 2, @intCast(arg.run_number));
        if (err != .DB_SUCCESS) break;
        err = api.ib_tuple_write_u32(tpl, 3, 0);
        if (err != .DB_SUCCESS) break;

        err = api.ib_cursor_insert_row(crsr, tpl);
        mt.updateErrStats(arg.err_st, err);
        if (err != .DB_SUCCESS) break;
        _ = api.ib_tuple_clear(tpl);
    }

    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
    }

    return err;
}

fn update_t2(arg: *mt.CbArgs) api.ib_err_t {
    var crsr: api.ib_crsr_t = null;
    var err = mt.openTable(mt.sliceName(&arg.tbl.db_name), mt.sliceName(&arg.tbl.name), arg.trx, &crsr);
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }
    defer _ = api.ib_cursor_close(crsr);

    err = api.ib_cursor_lock(crsr, .IB_LOCK_IX);
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }
    err = api.ib_cursor_set_lock_mode(crsr, .IB_LOCK_X);
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }

    const key_tpl = api.ib_sec_search_tuple_create(crsr) orelse return .DB_OUT_OF_MEMORY;
    defer api.ib_tuple_delete(key_tpl);

    const five: api.ib_u32_t = 5;
    err = api.ib_tuple_write_u32(key_tpl, 0, five);
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }

    var res: i32 = 0;
    err = api.ib_cursor_moveto(crsr, key_tpl, .IB_CUR_GE, &res);
    if (err == .DB_RECORD_NOT_FOUND or res != 0) {
        return .DB_SUCCESS;
    }
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }

    const old_tpl = api.ib_clust_read_tuple_create(crsr) orelse return .DB_OUT_OF_MEMORY;
    defer api.ib_tuple_delete(old_tpl);
    const new_tpl = api.ib_clust_read_tuple_create(crsr) orelse return .DB_OUT_OF_MEMORY;
    defer api.ib_tuple_delete(new_tpl);

    err = api.ib_cursor_read_row(crsr, old_tpl);
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }
    err = api.ib_tuple_copy(new_tpl, old_tpl);
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }

    var score: api.ib_u32_t = 0;
    err = api.ib_tuple_read_u32(old_tpl, 1, &score);
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }
    score += 100;
    err = api.ib_tuple_write_u32(new_tpl, 1, score);
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }
    err = api.ib_tuple_write_u32(new_tpl, 3, @intCast(arg.run_number));
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }

    err = api.ib_cursor_update_row(crsr, old_tpl, new_tpl);
    mt.updateErrStats(arg.err_st, err);
    return err;
}

fn delete_t2(arg: *mt.CbArgs) api.ib_err_t {
    var crsr: api.ib_crsr_t = null;
    var err = mt.openTable(mt.sliceName(&arg.tbl.db_name), mt.sliceName(&arg.tbl.name), arg.trx, &crsr);
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }
    defer _ = api.ib_cursor_close(crsr);

    err = api.ib_cursor_lock(crsr, .IB_LOCK_IX);
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }
    err = api.ib_cursor_set_lock_mode(crsr, .IB_LOCK_X);
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }

    const key_tpl = api.ib_sec_search_tuple_create(crsr) orelse return .DB_OUT_OF_MEMORY;
    defer api.ib_tuple_delete(key_tpl);

    const nine: api.ib_u32_t = 9;
    err = api.ib_tuple_write_u32(key_tpl, 0, nine);
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }

    var res: i32 = 0;
    err = api.ib_cursor_moveto(crsr, key_tpl, .IB_CUR_GE, &res);
    if (err == .DB_RECORD_NOT_FOUND or res != 0) {
        return .DB_SUCCESS;
    }
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }

    err = api.ib_cursor_delete_row(crsr);
    mt.updateErrStats(arg.err_st, err);
    return err;
}

pub fn register_t2_table(tbl: *mt.TblClass) void {
    mt_base.registerBaseTable(tbl);
    mt.setName(&tbl.name, "t2");

    tbl.dml_fn[@intFromEnum(mt.DmlOpType.insert)] = insert_t2;
    tbl.dml_fn[@intFromEnum(mt.DmlOpType.update)] = update_t2;
    tbl.dml_fn[@intFromEnum(mt.DmlOpType.delete)] = delete_t2;

    tbl.ddl_fn[@intFromEnum(mt.DdlOpType.create)] = create_t2;
    tbl.ddl_fn[@intFromEnum(mt.DdlOpType.alter)] = alter_t2;
}

test "ib mt t2 harness" {
    try expectOk(api.ib_init());
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
    try expectOk(api.ib_startup("barracuda"));

    try std.testing.expectEqual(compat.IB_TRUE, api.ib_database_create(DATABASE));

    var tbl = mt.TblClass{};
    mt.setName(&tbl.db_name, DATABASE);
    register_t2_table(&tbl);

    var ddl_stats = mt.OpErr{};
    var dml_stats = mt.OpErr{};
    var args = mt.CbArgs{
        .tbl = &tbl,
        .err_st = &ddl_stats,
        .isolation_level = .IB_TRX_REPEATABLE_READ,
        .run_number = 1,
    };

    const create_fn = tbl.ddl_fn[@intFromEnum(mt.DdlOpType.create)] orelse return error.MissingFn;
    try expectOk(create_fn(&args));

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);
    args.trx = trx;
    args.batch_size = 4;
    args.err_st = &dml_stats;

    const insert_fn = tbl.dml_fn[@intFromEnum(mt.DmlOpType.insert)] orelse return error.MissingFn;
    _ = insert_fn(&args);
    const update_fn = tbl.dml_fn[@intFromEnum(mt.DmlOpType.update)] orelse return error.MissingFn;
    _ = update_fn(&args);
    const delete_fn = tbl.dml_fn[@intFromEnum(mt.DmlOpType.delete)] orelse return error.MissingFn;
    _ = delete_fn(&args);

    try expectOk(api.ib_trx_commit(trx));

    args.trx = null;
    args.err_st = &ddl_stats;
    const drop_fn = tbl.ddl_fn[@intFromEnum(mt.DdlOpType.drop)] orelse return error.MissingFn;
    _ = drop_fn(&args);
}
