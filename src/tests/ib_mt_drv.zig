const std = @import("std");
const api = @import("../api/api.zig");
const compat = @import("../ut/compat.zig");
const mt = @import("ib_mt_common.zig");
const mt_base = @import("ib_mt_base.zig");
const mt_t1 = @import("ib_mt_t1.zig");

const DATABASE: []const u8 = "test";
const NUM_TBLS: usize = 8;
const MaxNameLen: usize = @intCast(api.IB_MAX_TABLE_NAME_LEN);

const TableEntry = struct {
    tbl: mt.TblClass = .{},
    next_id: u32 = 1,
};

var tbl_array: [NUM_TBLS]TableEntry = [_]TableEntry{.{}} ** NUM_TBLS;
var num_tables: usize = 0;

var dml_op_errs: [mt.DmlOpCount]mt.OpErr = [_]mt.OpErr{.{}} ** mt.DmlOpCount;
var ddl_op_errs: [mt.DdlOpCount]mt.OpErr = [_]mt.OpErr{.{}} ** mt.DdlOpCount;

var run_number: i32 = 1;

fn expectOk(err: api.ib_err_t) !void {
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
}

fn entryFromTbl(tbl: *mt.TblClass) *TableEntry {
    return @as(*TableEntry, @fieldParentPtr("tbl", tbl));
}

fn tableName(tbl: *mt.TblClass, buf: *[MaxNameLen]u8) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}/{s}", .{ mt.sliceName(&tbl.db_name), mt.sliceName(&tbl.name) });
}

fn createSimpleTable(arg: *mt.CbArgs) api.ib_err_t {
    var name_buf: [MaxNameLen]u8 = undefined;
    const table_name = tableName(arg.tbl, &name_buf) catch {
        mt.updateErrStats(arg.err_st, .DB_INVALID_INPUT);
        return .DB_INVALID_INPUT;
    };

    var tbl_sch: api.ib_tbl_sch_t = null;
    var idx_sch: api.ib_idx_sch_t = null;
    var err = api.ib_table_schema_create(table_name, &tbl_sch, arg.tbl.format, arg.tbl.page_size);
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }
    defer api.ib_table_schema_delete(tbl_sch);

    err = api.ib_table_schema_add_col(tbl_sch, "id", .IB_INT, .IB_COL_UNSIGNED, 0, @sizeOf(api.ib_u32_t));
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

    err = api.ib_table_schema_add_index(tbl_sch, "pk_id", &idx_sch);
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }
    err = api.ib_index_schema_add_col(idx_sch, "id", 0);
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

fn alterSimple(_: *mt.CbArgs) api.ib_err_t {
    return .DB_SUCCESS;
}

fn insertSimple(arg: *mt.CbArgs) api.ib_err_t {
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

    const entry = entryFromTbl(arg.tbl);
    const id: api.ib_u32_t = entry.next_id;
    entry.next_id += 1;

    err = api.ib_tuple_write_u32(tpl, 0, id);
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }
    err = api.ib_tuple_write_u32(tpl, 1, 0);
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }
    err = api.ib_tuple_write_u32(tpl, 2, @intCast(arg.run_number));
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }
    err = api.ib_tuple_write_u32(tpl, 3, 0);
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }

    err = api.ib_cursor_insert_row(crsr, tpl);
    mt.updateErrStats(arg.err_st, err);
    return err;
}

fn updateSimple(arg: *mt.CbArgs) api.ib_err_t {
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

    const id: api.ib_u32_t = 1;
    err = api.ib_tuple_write_u32(key_tpl, 0, id);
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }

    var res: i32 = 0;
    err = api.ib_cursor_moveto(crsr, key_tpl, .IB_CUR_GE, &res);
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }
    if (res != 0) {
        return .DB_SUCCESS;
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
    score += 1;
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

fn deleteSimple(arg: *mt.CbArgs) api.ib_err_t {
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

    const id: api.ib_u32_t = 1;
    err = api.ib_tuple_write_u32(key_tpl, 0, id);
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }

    var res: i32 = 0;
    err = api.ib_cursor_moveto(crsr, key_tpl, .IB_CUR_GE, &res);
    if (err != .DB_SUCCESS) {
        mt.updateErrStats(arg.err_st, err);
        return err;
    }
    if (res != 0) {
        return .DB_SUCCESS;
    }

    err = api.ib_cursor_delete_row(crsr);
    mt.updateErrStats(arg.err_st, err);
    return err;
}

fn register_t2_table(entry: *TableEntry) void {
    mt_base.registerBaseTable(&entry.tbl);
    mt.setName(&entry.tbl.name, "t2");
    entry.tbl.dml_fn[@intFromEnum(mt.DmlOpType.insert)] = insertSimple;
    entry.tbl.dml_fn[@intFromEnum(mt.DmlOpType.update)] = updateSimple;
    entry.tbl.dml_fn[@intFromEnum(mt.DmlOpType.delete)] = deleteSimple;
    entry.tbl.ddl_fn[@intFromEnum(mt.DdlOpType.create)] = createSimpleTable;
    entry.tbl.ddl_fn[@intFromEnum(mt.DdlOpType.alter)] = alterSimple;
}

fn register_test_tables() void {
    for (&tbl_array) |*entry| {
        mt_base.registerBaseTable(&entry.tbl);
        mt.setName(&entry.tbl.db_name, DATABASE);
        entry.tbl.format = .IB_TBL_COMPACT;
        entry.tbl.page_size = 0;
        entry.next_id = 1;
    }

    mt_t1.register_t1_table(&tbl_array[num_tables].tbl);
    num_tables += 1;
    register_t2_table(&tbl_array[num_tables]);
    num_tables += 1;
}

fn create_test_table(tbl: *mt.TblClass) void {
    var args = mt.CbArgs{
        .tbl = tbl,
        .isolation_level = .IB_TRX_REPEATABLE_READ,
        .run_number = run_number,
        .err_st = &ddl_op_errs[@intFromEnum(mt.DdlOpType.create)],
    };
    const create_fn = tbl.ddl_fn[@intFromEnum(mt.DdlOpType.create)] orelse return;
    _ = create_fn(&args);
}

fn seed_test_table(tbl: *mt.TblClass, rows: usize) !void {
    var i: usize = 0;
    while (i < rows) : (i += 1) {
        const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
        var args = mt.CbArgs{
            .tbl = tbl,
            .trx = trx,
            .isolation_level = .IB_TRX_REPEATABLE_READ,
            .run_number = run_number,
            .batch_size = 1,
            .err_st = &dml_op_errs[@intFromEnum(mt.DmlOpType.insert)],
        };
        const insert_fn = tbl.dml_fn[@intFromEnum(mt.DmlOpType.insert)] orelse return error.MissingFn;
        const err = insert_fn(&args);
        if (err == .DB_SUCCESS) {
            _ = api.ib_trx_commit(trx);
        } else {
            _ = api.ib_trx_rollback(trx);
        }
    }
}

fn init_test_tables() !void {
    for (tbl_array[0..num_tables]) |*entry| {
        create_test_table(&entry.tbl);
    }
    for (tbl_array[0..num_tables]) |*entry| {
        try seed_test_table(&entry.tbl, 2);
    }
}

fn drop_test_tables() void {
    for (tbl_array[0..num_tables]) |*entry| {
        var args = mt.CbArgs{
            .tbl = &entry.tbl,
            .err_st = &ddl_op_errs[@intFromEnum(mt.DdlOpType.drop)],
            .isolation_level = .IB_TRX_REPEATABLE_READ,
        };
        const drop_fn = entry.tbl.ddl_fn[@intFromEnum(mt.DdlOpType.drop)] orelse continue;
        _ = drop_fn(&args);
    }
}

test "ib mt drv harness" {
    try expectOk(api.ib_init());
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
    try expectOk(api.ib_startup("barracuda"));

    try std.testing.expectEqual(compat.IB_TRUE, api.ib_database_create(DATABASE));

    register_test_tables();
    try init_test_tables();

    for (tbl_array[0..num_tables]) |*entry| {
        const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
        var args = mt.CbArgs{
            .tbl = &entry.tbl,
            .trx = trx,
            .isolation_level = .IB_TRX_REPEATABLE_READ,
            .run_number = run_number,
            .batch_size = 1,
            .err_st = &dml_op_errs[@intFromEnum(mt.DmlOpType.insert)],
        };

        const insert_fn = entry.tbl.dml_fn[@intFromEnum(mt.DmlOpType.insert)] orelse return error.MissingFn;
        _ = insert_fn(&args);
        const update_fn = entry.tbl.dml_fn[@intFromEnum(mt.DmlOpType.update)] orelse return error.MissingFn;
        _ = update_fn(&args);
        const delete_fn = entry.tbl.dml_fn[@intFromEnum(mt.DmlOpType.delete)] orelse return error.MissingFn;
        _ = delete_fn(&args);

        _ = api.ib_trx_commit(trx);
    }

    for (tbl_array[0..num_tables]) |*entry| {
        run_number += 1;
        var args = mt.CbArgs{
            .tbl = &entry.tbl,
            .err_st = &ddl_op_errs[@intFromEnum(mt.DdlOpType.truncate)],
            .isolation_level = .IB_TRX_REPEATABLE_READ,
            .run_number = run_number,
        };
        const truncate_fn = entry.tbl.ddl_fn[@intFromEnum(mt.DdlOpType.truncate)] orelse return error.MissingFn;
        _ = truncate_fn(&args);

        run_number += 1;
        args.err_st = &ddl_op_errs[@intFromEnum(mt.DdlOpType.drop)];
        const drop_fn = entry.tbl.ddl_fn[@intFromEnum(mt.DdlOpType.drop)] orelse return error.MissingFn;
        const drop_err = drop_fn(&args);

        if (drop_err == .DB_SUCCESS or drop_err == .DB_TABLE_NOT_FOUND) {
            args.err_st = &ddl_op_errs[@intFromEnum(mt.DdlOpType.create)];
            const create_fn = entry.tbl.ddl_fn[@intFromEnum(mt.DdlOpType.create)] orelse return error.MissingFn;
            _ = create_fn(&args);
        }
    }

    drop_test_tables();
    try expectOk(api.ib_database_drop(DATABASE));

    try std.testing.expect(dml_op_errs[@intFromEnum(mt.DmlOpType.insert)].n_ops > 0);
    try std.testing.expect(ddl_op_errs[@intFromEnum(mt.DdlOpType.create)].n_ops > 0);
}
