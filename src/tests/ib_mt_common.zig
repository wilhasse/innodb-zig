const std = @import("std");
const api = @import("../api/api.zig");
const compat = @import("../ut/compat.zig");

pub const DmlOpType = enum(u8) { select, insert, update, delete, max };
pub const DdlOpType = enum(u8) { create, drop, alter, truncate, max };

pub const DmlOpCount: usize = @intFromEnum(DmlOpType.max);
pub const DdlOpCount: usize = @intFromEnum(DdlOpType.max);
pub const ErrSlots: usize = @intFromEnum(api.ib_err_t.DB_SCHEMA_NOT_LOCKED) + 1;

pub const OpErr = struct {
    n_ops: usize = 0,
    n_errs: usize = 0,
    errs: [ErrSlots]usize = [_]usize{0} ** ErrSlots,
    mutex: std.Thread.Mutex = .{},
};

pub const CbArgs = struct {
    trx: api.ib_trx_t = null,
    isolation_level: api.ib_trx_level_t = .IB_TRX_REPEATABLE_READ,
    run_number: i32 = 0,
    batch_size: i32 = 0,
    print_res: api.ib_bool_t = compat.IB_FALSE,
    err_st: *OpErr,
    tbl: *TblClass,
};

pub const OpFn = *const fn (*CbArgs) api.ib_err_t;

pub const TblClass = struct {
    name: [32]u8 = [_]u8{0} ** 32,
    db_name: [32]u8 = [_]u8{0} ** 32,
    format: api.ib_tbl_fmt_t = .IB_TBL_COMPACT,
    page_size: api.ib_ulint_t = 0,
    dml_fn: [DmlOpCount]?OpFn = [_]?OpFn{null} ** DmlOpCount,
    ddl_fn: [DdlOpCount]?OpFn = [_]?OpFn{null} ** DdlOpCount,
};

pub fn setName(buf: *[32]u8, name: []const u8) void {
    @memset(buf, 0);
    const len = @min(name.len, buf.len - 1);
    std.mem.copyForwards(u8, buf[0..len], name[0..len]);
}

pub fn sliceName(buf: *const [32]u8) []const u8 {
    return std.mem.sliceTo(buf, 0);
}

pub fn updateErrStats(e: *OpErr, err: api.ib_err_t) void {
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

pub fn openTable(dbname: []const u8, name: []const u8, trx: api.ib_trx_t, crsr: *api.ib_crsr_t) api.ib_err_t {
    const max_len: usize = @intCast(api.IB_MAX_TABLE_NAME_LEN);
    var table_name_buf: [max_len]u8 = undefined;
    const table_name = std.fmt.bufPrint(&table_name_buf, "{s}/{s}", .{ dbname, name }) catch {
        return .DB_INVALID_INPUT;
    };
    return api.ib_cursor_open_table(table_name, trx, crsr);
}
