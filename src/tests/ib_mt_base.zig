const std = @import("std");
const api = @import("../api/api.zig");
const compat = @import("../ut/compat.zig");

const DmlOpType = enum(u8) { select, insert, update, delete, max };
const DdlOpType = enum(u8) { create, drop, alter, truncate, max };

const DmlOpCount: usize = @intFromEnum(DmlOpType.max);
const DdlOpCount: usize = @intFromEnum(DdlOpType.max);
const ErrSlots: usize = @intFromEnum(api.ib_err_t.DB_SCHEMA_NOT_LOCKED) + 1;

const OpErr = struct {
    n_ops: usize = 0,
    n_errs: usize = 0,
    errs: [ErrSlots]usize = [_]usize{0} ** ErrSlots,
    mutex: std.Thread.Mutex = .{},
};

const CbArgs = struct {
    trx: api.ib_trx_t = null,
    isolation_level: i32 = 0,
    run_number: i32 = 0,
    batch_size: i32 = 0,
    print_res: api.ib_bool_t = compat.IB_FALSE,
    err_st: *OpErr,
    tbl: *TblClass,
};

const OpFn = *const fn (*CbArgs) api.ib_err_t;

const TblClass = struct {
    name: [32]u8 = [_]u8{0} ** 32,
    db_name: [32]u8 = [_]u8{0} ** 32,
    format: api.ib_tbl_fmt_t = .IB_TBL_COMPACT,
    page_size: api.ib_ulint_t = 0,
    dml_fn: [DmlOpCount]?OpFn = [_]?OpFn{null} ** DmlOpCount,
    ddl_fn: [DdlOpCount]?OpFn = [_]?OpFn{null} ** DdlOpCount,
};

fn expectOk(err: api.ib_err_t) !void {
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
}

fn setName(buf: *[32]u8, name: []const u8) void {
    @memset(buf, 0);
    const len = @min(name.len, buf.len - 1);
    std.mem.copyForwards(u8, buf[0..len], name[0..len]);
}

fn sliceName(buf: *const [32]u8) []const u8 {
    return std.mem.sliceTo(buf, 0);
}

fn updateErrStats(e: *OpErr, err: api.ib_err_t) void {
    e.mutex.lock();
    defer e.mutex.unlock();
    e.n_ops += 1;
    if (err != .DB_SUCCESS) {
        e.n_errs += 1;
        const idx: usize = @intCast(@intFromEnum(err));
        if (idx < e.errs.len) {
            e.errs[idx] += 1;
        }
    }
}

fn openTable(dbname: []const u8, name: []const u8, trx: api.ib_trx_t, crsr: *api.ib_crsr_t) api.ib_err_t {
    const max_len: usize = @intCast(api.IB_MAX_TABLE_NAME_LEN);
    var table_name_buf: [max_len]u8 = undefined;
    const table_name = std.fmt.bufPrint(&table_name_buf, "{s}/{s}", .{ dbname, name }) catch {
        return .DB_INVALID_INPUT;
    };
    return api.ib_cursor_open_table(table_name, trx, crsr);
}

fn selectStub(_: *CbArgs) api.ib_err_t {
    return .DB_SUCCESS;
}

fn dropBase(arg: *CbArgs) api.ib_err_t {
    const tbl = arg.tbl;
    const dbname = sliceName(&tbl.db_name);
    const name = sliceName(&tbl.name);

    const max_len: usize = @intCast(api.IB_MAX_TABLE_NAME_LEN);
    var table_name_buf: [max_len]u8 = undefined;
    const table_name = std.fmt.bufPrint(&table_name_buf, "{s}/{s}", .{ dbname, name }) catch {
        updateErrStats(arg.err_st, .DB_INVALID_INPUT);
        return .DB_INVALID_INPUT;
    };

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return .DB_OUT_OF_MEMORY;

    var err = api.ib_schema_lock_exclusive(trx);
    if (err != .DB_SUCCESS) {
        _ = api.ib_trx_rollback(trx);
        updateErrStats(arg.err_st, err);
        return err;
    }

    err = api.ib_table_drop(trx, table_name);
    if (err != .DB_SUCCESS and err != .DB_TABLE_NOT_FOUND and err != .DB_TABLESPACE_DELETED) {
        _ = api.ib_trx_rollback(trx);
        updateErrStats(arg.err_st, err);
        return err;
    }

    err = api.ib_trx_commit(trx);
    updateErrStats(arg.err_st, err);
    return err;
}

fn truncateBase(arg: *CbArgs) api.ib_err_t {
    const tbl = arg.tbl;
    const dbname = sliceName(&tbl.db_name);
    const name = sliceName(&tbl.name);

    const max_len: usize = @intCast(api.IB_MAX_TABLE_NAME_LEN);
    var table_name_buf: [max_len]u8 = undefined;
    const table_name = std.fmt.bufPrint(&table_name_buf, "{s}/{s}", .{ dbname, name }) catch {
        updateErrStats(arg.err_st, .DB_INVALID_INPUT);
        return .DB_INVALID_INPUT;
    };

    var table_id: api.ib_id_t = 0;
    const err = api.ib_table_truncate(table_name, &table_id);
    updateErrStats(arg.err_st, err);
    return err;
}

fn updateBase(_: *CbArgs) api.ib_err_t {
    return .DB_SUCCESS;
}

fn insertBase(_: *CbArgs) api.ib_err_t {
    return .DB_SUCCESS;
}

fn deleteBase(_: *CbArgs) api.ib_err_t {
    return .DB_SUCCESS;
}

fn createBase(_: *CbArgs) api.ib_err_t {
    return .DB_SUCCESS;
}

fn alterBase(_: *CbArgs) api.ib_err_t {
    return .DB_SUCCESS;
}

fn registerBaseTable(tbl: *TblClass) void {
    setName(&tbl.name, "dummy");

    tbl.dml_fn[@intFromEnum(DmlOpType.select)] = selectStub;
    tbl.dml_fn[@intFromEnum(DmlOpType.update)] = updateBase;
    tbl.dml_fn[@intFromEnum(DmlOpType.insert)] = insertBase;
    tbl.dml_fn[@intFromEnum(DmlOpType.delete)] = deleteBase;

    tbl.ddl_fn[@intFromEnum(DdlOpType.create)] = createBase;
    tbl.ddl_fn[@intFromEnum(DdlOpType.drop)] = dropBase;
    tbl.ddl_fn[@intFromEnum(DdlOpType.alter)] = alterBase;
    tbl.ddl_fn[@intFromEnum(DdlOpType.truncate)] = truncateBase;
}

fn createBaseTable(tbl: *const TblClass) !void {
    const dbname = sliceName(&tbl.db_name);
    const name = sliceName(&tbl.name);

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

    var tbl = TblClass{};
    setName(&tbl.db_name, "mt_db");
    registerBaseTable(&tbl);

    try createBaseTable(&tbl);

    var stats = OpErr{};
    var args = CbArgs{
        .err_st = &stats,
        .tbl = &tbl,
    };

    const truncate_fn = tbl.ddl_fn[@intFromEnum(DdlOpType.truncate)] orelse return error.MissingFn;
    try expectOk(truncate_fn(&args));

    const select_fn = tbl.dml_fn[@intFromEnum(DmlOpType.select)] orelse return error.MissingFn;
    try expectOk(select_fn(&args));

    const drop_fn = tbl.ddl_fn[@intFromEnum(DdlOpType.drop)] orelse return error.MissingFn;
    try expectOk(drop_fn(&args));

    try std.testing.expect(stats.n_ops >= 2);
}
