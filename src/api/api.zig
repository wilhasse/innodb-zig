const std = @import("std");
const compat = @import("../ut/compat.zig");
const errors = @import("../ut/errors.zig");
const log = @import("../ut/log.zig");
const os_thread = @import("../os/thread.zig");

const ib_tuple_type_t = enum(u32) {
    TPL_ROW = 0,
    TPL_KEY = 1,
};

pub const ib_err_t = errors.DbErr;
pub const ib_bool_t = compat.ib_bool_t;
pub const ib_ulint_t = compat.ib_ulint_t;
pub const ib_u64_t = compat.ib_uint64_t;
pub const ib_u32_t = u32;
pub const ib_u16_t = u16;
pub const ib_byte_t = u8;
pub const ib_opaque_t = ?*anyopaque;
pub const ib_charset_t = ib_opaque_t;

pub const ib_logger_t = log.LoggerFn;
pub const ib_stream_t = log.Stream;
pub const ib_msg_log_t = log.LoggerFn;
pub const ib_msg_stream_t = log.Stream;

pub const IB_API_VERSION_CURRENT: u64 = 3;
pub const IB_API_VERSION_REVISION: u64 = 0;
pub const IB_API_VERSION_AGE: u64 = 0;

const dict_tf_format_max: u32 = 1;
const file_format_names = [_][]const u8{
    "Antelope",
    "Barracuda",
};

pub const Trx = struct {
    state: ib_trx_state_t,
    isolation_level: ib_trx_level_t,
    client_thread_id: os_thread.ThreadId,
};
pub const Cursor = struct {
    allocator: std.mem.Allocator,
    trx: ib_trx_t,
    table_name: ?[]u8,
    table_id: ?ib_id_t,
    index_name: ?[]u8,
    index_id: ?ib_id_t,
    match_mode: ib_match_mode_t,
    lock_mode: ib_lck_mode_t,
    positioned: bool,
    cluster_access: bool,
    simple_select: bool,
};
const Tuple = struct {
    tuple_type: ib_tuple_type_t,
    cols: []Column,
    allocator: std.mem.Allocator,
};

const Column = struct {
    meta: ib_col_meta_t,
    data: ?[]u8,
};
pub const TableSchema = opaque {};
pub const IndexSchema = opaque {};

pub const ib_trx_t = ?*Trx;
pub const ib_crsr_t = ?*Cursor;
pub const ib_tpl_t = ?*Tuple;
pub const ib_tbl_sch_t = ?*TableSchema;
pub const ib_idx_sch_t = ?*IndexSchema;

pub const ib_id_t = ib_u64_t;

pub const ib_i8_t = i8;
pub const ib_u8_t = u8;
pub const ib_i16_t = i16;
pub const ib_i32_t = i32;
pub const ib_i64_t = i64;

pub const IB_SQL_NULL: ib_u32_t = 0xFFFF_FFFF;
pub const IB_N_SYS_COLS: ib_u32_t = 3;
pub const MAX_TEXT_LEN: ib_u32_t = 4096;
pub const IB_MAX_COL_NAME_LEN: ib_u32_t = 64 * 3;
pub const IB_MAX_TABLE_NAME_LEN: ib_u32_t = 64 * 3;

pub const ib_trx_level_t = enum(u32) {
    IB_TRX_READ_UNCOMMITTED = 0,
    IB_TRX_READ_COMMITTED = 1,
    IB_TRX_REPEATABLE_READ = 2,
    IB_TRX_SERIALIZABLE = 3,
};

pub const ib_trx_state_t = enum(u32) {
    IB_TRX_NOT_STARTED = 0,
    IB_TRX_ACTIVE = 1,
    IB_TRX_COMMITTED_IN_MEMORY = 2,
    IB_TRX_PREPARED = 3,
};

pub const ib_shutdown_t = enum(u32) {
    IB_SHUTDOWN_NORMAL = 0,
    IB_SHUTDOWN_NO_IBUFMERGE_PURGE = 1,
    IB_SHUTDOWN_NO_BUFPOOL_FLUSH = 2,
};

pub const ib_col_type_t = enum(u32) {
    IB_VARCHAR = 1,
    IB_CHAR = 2,
    IB_BINARY = 3,
    IB_VARBINARY = 4,
    IB_BLOB = 5,
    IB_INT = 6,
    IB_SYS = 8,
    IB_FLOAT = 9,
    IB_DOUBLE = 10,
    IB_DECIMAL = 11,
    IB_VARCHAR_ANYCHARSET = 12,
    IB_CHAR_ANYCHARSET = 13,
};

pub const ib_col_attr_t = enum(u32) {
    IB_COL_NONE = 0,
    IB_COL_NOT_NULL = 1,
    IB_COL_UNSIGNED = 2,
    IB_COL_NOT_USED = 4,
    IB_COL_CUSTOM1 = 8,
    IB_COL_CUSTOM2 = 16,
    IB_COL_CUSTOM3 = 32,
};

pub const ib_lck_mode_t = enum(u32) {
    IB_LOCK_IS = 0,
    IB_LOCK_IX = 1,
    IB_LOCK_S = 2,
    IB_LOCK_X = 3,
    IB_LOCK_NOT_USED = 4,
    IB_LOCK_NONE = 5,
    IB_LOCK_NUM = 5,
};

pub const ib_srch_mode_t = enum(u32) {
    IB_CUR_G = 1,
    IB_CUR_GE = 2,
    IB_CUR_L = 3,
    IB_CUR_LE = 4,
};

pub const ib_match_mode_t = enum(u32) {
    IB_CLOSEST_MATCH = 0,
    IB_EXACT_MATCH = 1,
    IB_EXACT_PREFIX = 2,
};

pub const ib_tbl_fmt_t = enum(u32) {
    IB_TBL_REDUNDANT = 0,
    IB_TBL_COMPACT = 1,
    IB_TBL_DYNAMIC = 2,
    IB_TBL_COMPRESSED = 3,
};

pub const ib_col_meta_t = struct {
    type: ib_col_type_t,
    attr: ib_col_attr_t,
    type_len: ib_u32_t,
    client_type: ib_u16_t,
    charset: ?*ib_charset_t,
};

pub const ib_client_cmp_t = *const fn (
    col_meta: *const ib_col_meta_t,
    p1: [*]const ib_byte_t,
    p1_len: ib_ulint_t,
    p2: [*]const ib_byte_t,
    p2_len: ib_ulint_t,
) i32;

fn ib_default_compare(
    col_meta: *const ib_col_meta_t,
    p1: [*]const ib_byte_t,
    p1_len: ib_ulint_t,
    p2: [*]const ib_byte_t,
    p2_len: ib_ulint_t,
) i32 {
    _ = col_meta;
    const len = @min(p1_len, p2_len);
    const a = p1[0..len];
    const b = p2[0..len];
    const ord = std.mem.order(u8, a, b);
    if (ord == .eq) {
        if (p1_len == p2_len) {
            return 0;
        }
        return if (p1_len < p2_len) -1 else 1;
    }
    return if (ord == .lt) -1 else 1;
}

pub var ib_client_compare: ib_client_cmp_t = ib_default_compare;

pub fn ib_set_client_compare(func: ib_client_cmp_t) void {
    ib_client_compare = func;
}

pub fn ib_api_version() ib_u64_t {
    return (@as(ib_u64_t, IB_API_VERSION_CURRENT) << 32) |
        (@as(ib_u64_t, IB_API_VERSION_REVISION) << 16) |
        @as(ib_u64_t, IB_API_VERSION_AGE);
}

pub fn ib_logger_set(ib_msg_log: ib_msg_log_t, ib_msg_stream: ib_msg_stream_t) void {
    log.setLogger(ib_msg_log, ib_msg_stream);
}

pub fn ib_strerror(err: ib_err_t) []const u8 {
    return errors.strerror(err);
}

const DbFormat = struct {
    id: u32,
    name: ?[]const u8,
};

var db_format: DbFormat = .{ .id = 0, .name = null };

pub fn ib_init() ib_err_t {
    return .DB_SUCCESS;
}

pub fn ib_startup(format: ?[]const u8) ib_err_t {
    db_format.id = 0;
    db_format.name = null;

    if (format) |fmt| {
        if (parseFileFormat(fmt)) |id| {
            db_format.id = id;
        } else {
            db_format.id = dict_tf_format_max + 1;
            log.logf("InnoDB: format '{s}' unknown.", .{fmt});
            return .DB_UNSUPPORTED;
        }
    }

    if (db_format.id <= dict_tf_format_max) {
        db_format.name = file_format_names[db_format.id];
    }

    return .DB_SUCCESS;
}

pub fn ib_shutdown(flag: ib_shutdown_t) ib_err_t {
    _ = flag;
    db_format.id = 0;
    db_format.name = null;
    return .DB_SUCCESS;
}

fn parseFileFormat(format: []const u8) ?u32 {
    if (format.len == 0) {
        return null;
    }
    if (std.fmt.parseInt(u32, format, 10)) |id| {
        if (id <= dict_tf_format_max) {
            return id;
        }
        return null;
    } else |_| {}

    for (file_format_names, 0..) |name, id| {
        if (std.ascii.eqlIgnoreCase(format, name)) {
            return @intCast(id);
        }
    }

    return null;
}

fn tupleCreate(allocator: std.mem.Allocator, tuple_type: ib_tuple_type_t, metas: []const ib_col_meta_t) !*Tuple {
    const tuple = try allocator.create(Tuple);
    const cols = try allocator.alloc(Column, metas.len);
    for (cols, 0..) |*col, i| {
        col.* = .{ .meta = metas[i], .data = null };
    }
    tuple.* = .{
        .tuple_type = tuple_type,
        .cols = cols,
        .allocator = allocator,
    };
    return tuple;
}

fn tupleDestroy(tuple: *Tuple) void {
    for (tuple.cols) |col| {
        if (col.data) |buf| {
            tuple.allocator.free(buf);
        }
    }
    tuple.allocator.free(tuple.cols);
    tuple.allocator.destroy(tuple);
}

fn tupleColumn(tuple: *Tuple, col_no: ib_ulint_t) ?*Column {
    if (col_no >= tuple.cols.len) {
        return null;
    }
    return &tuple.cols[@intCast(col_no)];
}

pub fn ib_col_set_value(ib_tpl: ib_tpl_t, col_no: ib_ulint_t, src: ?*const anyopaque, len: ib_ulint_t) ib_err_t {
    const tuple = ib_tpl orelse return .DB_ERROR;
    const col = tupleColumn(tuple, col_no) orelse return .DB_ERROR;

    if (len == @as(ib_ulint_t, IB_SQL_NULL)) {
        if (col.data) |buf| {
            tuple.allocator.free(buf);
        }
        col.data = null;
        return .DB_SUCCESS;
    }

    const ptr = src orelse return .DB_ERROR;
    const data_len: usize = @intCast(len);
    const buf = tuple.allocator.alloc(u8, data_len) catch return .DB_OUT_OF_MEMORY;
    const src_bytes = @as([*]const u8, @ptrCast(ptr))[0..data_len];
    std.mem.copyForwards(u8, buf, src_bytes);

    if (col.data) |old| {
        tuple.allocator.free(old);
    }
    col.data = buf;

    if (col.meta.type_len == 0 or col.meta.type_len == IB_SQL_NULL) {
        col.meta.type_len = @intCast(data_len);
    }

    return .DB_SUCCESS;
}

pub fn ib_col_get_len(ib_tpl: ib_tpl_t, i: ib_ulint_t) ib_ulint_t {
    const tuple = ib_tpl orelse return 0;
    const col = tupleColumn(tuple, i) orelse return 0;
    return if (col.data == null) @as(ib_ulint_t, IB_SQL_NULL) else @intCast(col.data.?.len);
}

pub fn ib_col_copy_value_low(ib_tpl: ib_tpl_t, i: ib_ulint_t, dst: *anyopaque, len: ib_ulint_t) ib_ulint_t {
    const tuple = ib_tpl orelse return 0;
    const col = tupleColumn(tuple, i) orelse return 0;
    const data = col.data orelse return @as(ib_ulint_t, IB_SQL_NULL);

    const data_len: ib_ulint_t = @intCast(data.len);
    const max_len: ib_ulint_t = if (len < data_len) len else data_len;

    switch (col.meta.type) {
        .IB_INT, .IB_FLOAT, .IB_DOUBLE => {
            if (len != data_len) {
                return 0;
            }
        },
        else => {},
    }

    const dst_bytes = @as([*]u8, @ptrCast(dst))[0..@intCast(max_len)];
    std.mem.copyForwards(u8, dst_bytes, data[0..@intCast(max_len)]);
    return max_len;
}

pub fn ib_col_copy_value(ib_tpl: ib_tpl_t, i: ib_ulint_t, dst: *anyopaque, len: ib_ulint_t) ib_ulint_t {
    return ib_col_copy_value_low(ib_tpl, i, dst, len);
}

pub fn ib_col_get_value(ib_tpl: ib_tpl_t, i: ib_ulint_t) ?*const anyopaque {
    const tuple = ib_tpl orelse return null;
    const col = tupleColumn(tuple, i) orelse return null;
    const data = col.data orelse return null;
    return data.ptr;
}

pub fn ib_col_get_meta_low(ib_tpl: ib_tpl_t, i: ib_ulint_t, meta: *ib_col_meta_t) ib_ulint_t {
    const tuple = ib_tpl orelse return 0;
    const col = tupleColumn(tuple, i) orelse return 0;
    meta.* = col.meta;
    return if (col.data == null) @as(ib_ulint_t, IB_SQL_NULL) else @intCast(col.data.?.len);
}

pub fn ib_col_get_meta(ib_tpl: ib_tpl_t, i: ib_ulint_t, meta: *ib_col_meta_t) ib_ulint_t {
    return ib_col_get_meta_low(ib_tpl, i, meta);
}

fn ib_tuple_check_int(ib_tpl: ib_tpl_t, i: ib_ulint_t, usign: ib_bool_t, size: usize) ib_err_t {
    var meta: ib_col_meta_t = undefined;
    _ = ib_col_get_meta_low(ib_tpl, i, &meta);

    if (meta.type != .IB_INT) {
        return .DB_DATA_MISMATCH;
    } else if (meta.type_len == IB_SQL_NULL) {
        return .DB_UNDERFLOW;
    } else if (meta.type_len != size) {
        return .DB_DATA_MISMATCH;
    }

    const attr_val = @intFromEnum(meta.attr);
    if ((attr_val & @intFromEnum(ib_col_attr_t.IB_COL_UNSIGNED)) != 0 and usign == compat.IB_FALSE) {
        return .DB_DATA_MISMATCH;
    }

    return .DB_SUCCESS;
}

pub fn ib_tuple_read_i8(ib_tpl: ib_tpl_t, i: ib_ulint_t, ival: *ib_i8_t) ib_err_t {
    const err = ib_tuple_check_int(ib_tpl, i, compat.IB_FALSE, @sizeOf(ib_i8_t));
    if (err == .DB_SUCCESS) {
        _ = ib_col_copy_value_low(ib_tpl, i, ival, @sizeOf(ib_i8_t));
    }
    return err;
}

pub fn ib_tuple_read_u8(ib_tpl: ib_tpl_t, i: ib_ulint_t, ival: *ib_u8_t) ib_err_t {
    const err = ib_tuple_check_int(ib_tpl, i, compat.IB_TRUE, @sizeOf(ib_u8_t));
    if (err == .DB_SUCCESS) {
        _ = ib_col_copy_value_low(ib_tpl, i, ival, @sizeOf(ib_u8_t));
    }
    return err;
}

pub fn ib_tuple_read_i16(ib_tpl: ib_tpl_t, i: ib_ulint_t, ival: *ib_i16_t) ib_err_t {
    const err = ib_tuple_check_int(ib_tpl, i, compat.IB_FALSE, @sizeOf(ib_i16_t));
    if (err == .DB_SUCCESS) {
        _ = ib_col_copy_value_low(ib_tpl, i, ival, @sizeOf(ib_i16_t));
    }
    return err;
}

pub fn ib_tuple_read_u16(ib_tpl: ib_tpl_t, i: ib_ulint_t, ival: *ib_u16_t) ib_err_t {
    const err = ib_tuple_check_int(ib_tpl, i, compat.IB_TRUE, @sizeOf(ib_u16_t));
    if (err == .DB_SUCCESS) {
        _ = ib_col_copy_value_low(ib_tpl, i, ival, @sizeOf(ib_u16_t));
    }
    return err;
}

pub fn ib_tuple_read_i32(ib_tpl: ib_tpl_t, i: ib_ulint_t, ival: *ib_i32_t) ib_err_t {
    const err = ib_tuple_check_int(ib_tpl, i, compat.IB_FALSE, @sizeOf(ib_i32_t));
    if (err == .DB_SUCCESS) {
        _ = ib_col_copy_value_low(ib_tpl, i, ival, @sizeOf(ib_i32_t));
    }
    return err;
}

pub fn ib_tuple_read_u32(ib_tpl: ib_tpl_t, i: ib_ulint_t, ival: *ib_u32_t) ib_err_t {
    const err = ib_tuple_check_int(ib_tpl, i, compat.IB_TRUE, @sizeOf(ib_u32_t));
    if (err == .DB_SUCCESS) {
        _ = ib_col_copy_value_low(ib_tpl, i, ival, @sizeOf(ib_u32_t));
    }
    return err;
}

pub fn ib_tuple_read_i64(ib_tpl: ib_tpl_t, i: ib_ulint_t, ival: *ib_i64_t) ib_err_t {
    const err = ib_tuple_check_int(ib_tpl, i, compat.IB_FALSE, @sizeOf(ib_i64_t));
    if (err == .DB_SUCCESS) {
        _ = ib_col_copy_value_low(ib_tpl, i, ival, @sizeOf(ib_i64_t));
    }
    return err;
}

pub fn ib_tuple_read_u64(ib_tpl: ib_tpl_t, i: ib_ulint_t, ival: *ib_u64_t) ib_err_t {
    const err = ib_tuple_check_int(ib_tpl, i, compat.IB_TRUE, @sizeOf(ib_u64_t));
    if (err == .DB_SUCCESS) {
        _ = ib_col_copy_value_low(ib_tpl, i, ival, @sizeOf(ib_u64_t));
    }
    return err;
}

fn ib_tuple_write_int(ib_tpl: ib_tpl_t, col_no: ib_ulint_t, value: *const anyopaque, value_len: usize) ib_err_t {
    const tuple = ib_tpl orelse return .DB_ERROR;
    const col = tupleColumn(tuple, col_no) orelse return .DB_ERROR;

    if (col.meta.type != .IB_INT) {
        return .DB_DATA_MISMATCH;
    }

    if (col.meta.type_len != 0 and col.meta.type_len != IB_SQL_NULL and col.meta.type_len != value_len) {
        return .DB_DATA_MISMATCH;
    }

    return ib_col_set_value(ib_tpl, col_no, value, @intCast(value_len));
}

pub fn ib_tuple_write_i8(ib_tpl: ib_tpl_t, col_no: ib_ulint_t, val: ib_i8_t) ib_err_t {
    return ib_tuple_write_int(ib_tpl, col_no, &val, @sizeOf(ib_i8_t));
}

pub fn ib_tuple_write_u8(ib_tpl: ib_tpl_t, col_no: ib_ulint_t, val: ib_u8_t) ib_err_t {
    return ib_tuple_write_int(ib_tpl, col_no, &val, @sizeOf(ib_u8_t));
}

pub fn ib_tuple_write_i16(ib_tpl: ib_tpl_t, col_no: ib_ulint_t, val: ib_i16_t) ib_err_t {
    return ib_tuple_write_int(ib_tpl, col_no, &val, @sizeOf(ib_i16_t));
}

pub fn ib_tuple_write_u16(ib_tpl: ib_tpl_t, col_no: ib_ulint_t, val: ib_u16_t) ib_err_t {
    return ib_tuple_write_int(ib_tpl, col_no, &val, @sizeOf(ib_u16_t));
}

pub fn ib_tuple_write_i32(ib_tpl: ib_tpl_t, col_no: ib_ulint_t, val: ib_i32_t) ib_err_t {
    return ib_tuple_write_int(ib_tpl, col_no, &val, @sizeOf(ib_i32_t));
}

pub fn ib_tuple_write_u32(ib_tpl: ib_tpl_t, col_no: ib_ulint_t, val: ib_u32_t) ib_err_t {
    return ib_tuple_write_int(ib_tpl, col_no, &val, @sizeOf(ib_u32_t));
}

pub fn ib_tuple_write_i64(ib_tpl: ib_tpl_t, col_no: ib_ulint_t, val: ib_i64_t) ib_err_t {
    return ib_tuple_write_int(ib_tpl, col_no, &val, @sizeOf(ib_i64_t));
}

pub fn ib_tuple_write_u64(ib_tpl: ib_tpl_t, col_no: ib_ulint_t, val: ib_u64_t) ib_err_t {
    return ib_tuple_write_int(ib_tpl, col_no, &val, @sizeOf(ib_u64_t));
}

pub fn ib_tuple_write_double(ib_tpl: ib_tpl_t, col_no: ib_ulint_t, val: f64) ib_err_t {
    const tuple = ib_tpl orelse return .DB_ERROR;
    const col = tupleColumn(tuple, col_no) orelse return .DB_ERROR;
    if (col.meta.type != .IB_DOUBLE) {
        return .DB_DATA_MISMATCH;
    }
    return ib_col_set_value(ib_tpl, col_no, &val, @sizeOf(f64));
}

pub fn ib_tuple_read_double(ib_tpl: ib_tpl_t, col_no: ib_ulint_t, dval: *f64) ib_err_t {
    const tuple = ib_tpl orelse return .DB_ERROR;
    const col = tupleColumn(tuple, col_no) orelse return .DB_ERROR;
    if (col.meta.type != .IB_DOUBLE) {
        return .DB_DATA_MISMATCH;
    }
    _ = ib_col_copy_value_low(ib_tpl, col_no, dval, @sizeOf(f64));
    return .DB_SUCCESS;
}

pub fn ib_tuple_write_float(ib_tpl: ib_tpl_t, col_no: ib_ulint_t, val: f32) ib_err_t {
    const tuple = ib_tpl orelse return .DB_ERROR;
    const col = tupleColumn(tuple, col_no) orelse return .DB_ERROR;
    if (col.meta.type != .IB_FLOAT) {
        return .DB_DATA_MISMATCH;
    }
    return ib_col_set_value(ib_tpl, col_no, &val, @sizeOf(f32));
}

pub fn ib_tuple_read_float(ib_tpl: ib_tpl_t, col_no: ib_ulint_t, fval: *f32) ib_err_t {
    const tuple = ib_tpl orelse return .DB_ERROR;
    const col = tupleColumn(tuple, col_no) orelse return .DB_ERROR;
    if (col.meta.type != .IB_FLOAT) {
        return .DB_DATA_MISMATCH;
    }
    _ = ib_col_copy_value_low(ib_tpl, col_no, fval, @sizeOf(f32));
    return .DB_SUCCESS;
}

pub fn ib_tuple_get_n_cols(ib_tpl: ib_tpl_t) ib_ulint_t {
    const tuple = ib_tpl orelse return 0;
    return @intCast(tuple.cols.len);
}

pub fn ib_tuple_get_n_user_cols(ib_tpl: ib_tpl_t) ib_ulint_t {
    const tuple = ib_tpl orelse return 0;
    return @intCast(tuple.cols.len);
}

pub fn ib_tuple_clear(ib_tpl: ib_tpl_t) ib_tpl_t {
    const tuple = ib_tpl orelse return null;
    for (tuple.cols) |*col| {
        if (col.data) |buf| {
            tuple.allocator.free(buf);
        }
        col.data = null;
    }
    return tuple;
}

pub fn ib_tuple_copy(ib_dst_tpl: ib_tpl_t, ib_src_tpl: ib_tpl_t) ib_err_t {
    const src = ib_src_tpl orelse return .DB_ERROR;
    const dst = ib_dst_tpl orelse return .DB_ERROR;

    if (src == dst) {
        return .DB_DATA_MISMATCH;
    }
    if (src.tuple_type != dst.tuple_type or src.cols.len != dst.cols.len) {
        return .DB_DATA_MISMATCH;
    }

    for (src.cols, 0..) |src_col, idx| {
        const dst_col = &dst.cols[idx];
        if (src_col.meta.type != dst_col.meta.type or src_col.meta.type_len != dst_col.meta.type_len) {
            return .DB_DATA_MISMATCH;
        }

        if (src_col.data) |data| {
            const buf = dst.allocator.alloc(u8, data.len) catch return .DB_OUT_OF_MEMORY;
            std.mem.copyForwards(u8, buf, data);
            if (dst_col.data) |old| {
                dst.allocator.free(old);
            }
            dst_col.data = buf;
        } else {
            if (dst_col.data) |old| {
                dst.allocator.free(old);
            }
            dst_col.data = null;
        }
    }

    return .DB_SUCCESS;
}

pub fn ib_tuple_delete(ib_tpl: ib_tpl_t) void {
    const tuple = ib_tpl orelse return;
    tupleDestroy(tuple);
}

pub fn ib_trx_start(ib_trx: ib_trx_t, ib_trx_level: ib_trx_level_t) ib_err_t {
    const trx = ib_trx orelse return .DB_ERROR;
    if (trx.client_thread_id != os_thread.currentId()) {
        return .DB_ERROR;
    }

    switch (ib_trx_level) {
        .IB_TRX_READ_UNCOMMITTED,
        .IB_TRX_READ_COMMITTED,
        .IB_TRX_REPEATABLE_READ,
        .IB_TRX_SERIALIZABLE,
        => {},
    }

    if (trx.state == .IB_TRX_NOT_STARTED) {
        trx.state = .IB_TRX_ACTIVE;
        trx.isolation_level = ib_trx_level;
        return .DB_SUCCESS;
    }

    return .DB_ERROR;
}

pub fn ib_trx_begin(ib_trx_level: ib_trx_level_t) ib_trx_t {
    const trx = std.heap.page_allocator.create(Trx) catch return null;
    trx.* = .{
        .state = .IB_TRX_NOT_STARTED,
        .isolation_level = ib_trx_level,
        .client_thread_id = os_thread.currentId(),
    };

    if (ib_trx_start(trx, ib_trx_level) != .DB_SUCCESS) {
        std.heap.page_allocator.destroy(trx);
        return null;
    }

    return trx;
}

pub fn ib_trx_state(ib_trx: ib_trx_t) ib_trx_state_t {
    const trx = ib_trx orelse return .IB_TRX_NOT_STARTED;
    return trx.state;
}

pub fn ib_trx_release(ib_trx: ib_trx_t) ib_err_t {
    const trx = ib_trx orelse return .DB_ERROR;
    std.heap.page_allocator.destroy(trx);
    return .DB_SUCCESS;
}

pub fn ib_trx_commit(ib_trx: ib_trx_t) ib_err_t {
    const trx = ib_trx orelse return .DB_ERROR;
    trx.state = .IB_TRX_COMMITTED_IN_MEMORY;
    return ib_trx_release(trx);
}

pub fn ib_trx_rollback(ib_trx: ib_trx_t) ib_err_t {
    const trx = ib_trx orelse return .DB_ERROR;
    trx.state = .IB_TRX_NOT_STARTED;
    return ib_trx_release(trx);
}

fn dupName(allocator: std.mem.Allocator, name: []const u8) ![]u8 {
    const buf = try allocator.alloc(u8, name.len);
    std.mem.copyForwards(u8, buf, name);
    return buf;
}

fn cursorCreate(
    allocator: std.mem.Allocator,
    table_name: ?[]const u8,
    table_id: ?ib_id_t,
    index_name: ?[]const u8,
    index_id: ?ib_id_t,
    trx: ib_trx_t,
) !*Cursor {
    const cursor = try allocator.create(Cursor);
    cursor.* = .{
        .allocator = allocator,
        .trx = trx,
        .table_name = null,
        .table_id = table_id,
        .index_name = null,
        .index_id = index_id,
        .match_mode = .IB_CLOSEST_MATCH,
        .lock_mode = .IB_LOCK_NONE,
        .positioned = false,
        .cluster_access = false,
        .simple_select = false,
    };

    if (table_name) |name| {
        if (name.len > 0) {
            cursor.table_name = try dupName(allocator, name);
        }
    }

    if (index_name) |name| {
        if (name.len > 0) {
            cursor.index_name = try dupName(allocator, name);
        }
    }

    return cursor;
}

fn cursorDestroy(cursor: *Cursor) void {
    if (cursor.table_name) |buf| {
        cursor.allocator.free(buf);
    }
    if (cursor.index_name) |buf| {
        cursor.allocator.free(buf);
    }
    cursor.allocator.destroy(cursor);
}

fn cursorLockModeValid(mode: ib_lck_mode_t) bool {
    return @intFromEnum(mode) <= @intFromEnum(ib_lck_mode_t.IB_LOCK_NUM);
}

pub fn ib_cursor_open_table_using_id(table_id: ib_id_t, ib_trx: ib_trx_t, ib_crsr: *ib_crsr_t) ib_err_t {
    if (table_id == 0) {
        ib_crsr.* = null;
        return .DB_TABLE_NOT_FOUND;
    }

    const cursor = cursorCreate(std.heap.page_allocator, null, table_id, null, null, ib_trx) catch {
        ib_crsr.* = null;
        return .DB_OUT_OF_MEMORY;
    };
    ib_crsr.* = cursor;
    return .DB_SUCCESS;
}

pub fn ib_cursor_open_index_using_id(index_id: ib_id_t, ib_trx: ib_trx_t, ib_crsr: *ib_crsr_t) ib_err_t {
    if (index_id == 0) {
        ib_crsr.* = null;
        return .DB_TABLE_NOT_FOUND;
    }

    const table_id: ib_id_t = index_id >> 32;
    const cursor = cursorCreate(
        std.heap.page_allocator,
        null,
        if (table_id == 0) null else table_id,
        null,
        index_id,
        ib_trx,
    ) catch {
        ib_crsr.* = null;
        return .DB_OUT_OF_MEMORY;
    };
    ib_crsr.* = cursor;
    return .DB_SUCCESS;
}

pub fn ib_cursor_open_index_using_name(
    ib_open_crsr: ib_crsr_t,
    index_name: []const u8,
    ib_crsr: *ib_crsr_t,
) ib_err_t {
    const base = ib_open_crsr orelse {
        ib_crsr.* = null;
        return .DB_TABLE_NOT_FOUND;
    };
    if (index_name.len == 0) {
        ib_crsr.* = null;
        return .DB_TABLE_NOT_FOUND;
    }

    const cursor = cursorCreate(
        std.heap.page_allocator,
        if (base.table_name) |name| name else null,
        base.table_id,
        index_name,
        null,
        base.trx,
    ) catch {
        ib_crsr.* = null;
        return .DB_OUT_OF_MEMORY;
    };
    ib_crsr.* = cursor;
    return .DB_SUCCESS;
}

pub fn ib_cursor_open_table(name: []const u8, ib_trx: ib_trx_t, ib_crsr: *ib_crsr_t) ib_err_t {
    if (name.len == 0) {
        ib_crsr.* = null;
        return .DB_TABLE_NOT_FOUND;
    }

    const cursor = cursorCreate(std.heap.page_allocator, name, null, null, null, ib_trx) catch {
        ib_crsr.* = null;
        return .DB_OUT_OF_MEMORY;
    };
    ib_crsr.* = cursor;
    return .DB_SUCCESS;
}

pub fn ib_cursor_reset(ib_crsr: ib_crsr_t) ib_err_t {
    const cursor = ib_crsr orelse return .DB_ERROR;
    cursor.positioned = false;
    cursor.cluster_access = false;
    cursor.simple_select = false;
    return .DB_SUCCESS;
}

pub fn ib_cursor_close(ib_crsr: ib_crsr_t) ib_err_t {
    const cursor = ib_crsr orelse return .DB_ERROR;
    cursorDestroy(cursor);
    return .DB_SUCCESS;
}

pub fn ib_cursor_read_row(ib_crsr: ib_crsr_t, ib_tpl: ib_tpl_t) ib_err_t {
    const cursor = ib_crsr orelse return .DB_ERROR;
    _ = ib_tpl orelse return .DB_ERROR;
    if (!cursor.positioned) {
        return .DB_RECORD_NOT_FOUND;
    }
    return .DB_SUCCESS;
}

pub fn ib_cursor_prev(ib_crsr: ib_crsr_t) ib_err_t {
    const cursor = ib_crsr orelse return .DB_ERROR;
    if (!cursor.positioned) {
        return .DB_RECORD_NOT_FOUND;
    }
    return .DB_SUCCESS;
}

pub fn ib_cursor_next(ib_crsr: ib_crsr_t) ib_err_t {
    const cursor = ib_crsr orelse return .DB_ERROR;
    if (!cursor.positioned) {
        return .DB_RECORD_NOT_FOUND;
    }
    return .DB_SUCCESS;
}

pub fn ib_cursor_first(ib_crsr: ib_crsr_t) ib_err_t {
    const cursor = ib_crsr orelse return .DB_ERROR;
    cursor.positioned = true;
    return .DB_SUCCESS;
}

pub fn ib_cursor_last(ib_crsr: ib_crsr_t) ib_err_t {
    const cursor = ib_crsr orelse return .DB_ERROR;
    cursor.positioned = true;
    return .DB_SUCCESS;
}

pub fn ib_cursor_moveto(
    ib_crsr: ib_crsr_t,
    ib_tpl: ib_tpl_t,
    ib_srch_mode: ib_srch_mode_t,
    result: *i32,
) ib_err_t {
    const cursor = ib_crsr orelse return .DB_ERROR;
    _ = ib_srch_mode;
    const tuple = ib_tpl orelse return .DB_ERROR;
    if (tuple.tuple_type != .TPL_KEY) {
        return .DB_DATA_MISMATCH;
    }
    cursor.positioned = true;
    result.* = 0;
    return .DB_SUCCESS;
}

pub fn ib_cursor_attach_trx(ib_crsr: ib_crsr_t, ib_trx: ib_trx_t) void {
    const cursor = ib_crsr orelse return;
    const trx = ib_trx orelse return;
    if (cursor.trx == null) {
        cursor.trx = trx;
    }
}

pub fn ib_cursor_set_match_mode(ib_crsr: ib_crsr_t, match_mode: ib_match_mode_t) void {
    const cursor = ib_crsr orelse return;
    cursor.match_mode = match_mode;
}

pub fn ib_cursor_is_positioned(ib_crsr: ib_crsr_t) ib_bool_t {
    const cursor = ib_crsr orelse return compat.IB_FALSE;
    return if (cursor.positioned) compat.IB_TRUE else compat.IB_FALSE;
}

pub fn ib_cursor_lock(ib_crsr: ib_crsr_t, ib_lck_mode: ib_lck_mode_t) ib_err_t {
    const cursor = ib_crsr orelse return .DB_ERROR;
    if (!cursorLockModeValid(ib_lck_mode)) {
        return .DB_ERROR;
    }
    cursor.lock_mode = ib_lck_mode;
    return .DB_SUCCESS;
}

pub fn ib_cursor_unlock(ib_crsr: ib_crsr_t) ib_err_t {
    const cursor = ib_crsr orelse return .DB_ERROR;
    cursor.lock_mode = .IB_LOCK_NONE;
    return .DB_SUCCESS;
}

pub fn ib_cursor_set_lock_mode(ib_crsr: ib_crsr_t, ib_lck_mode: ib_lck_mode_t) ib_err_t {
    return ib_cursor_lock(ib_crsr, ib_lck_mode);
}

pub fn ib_cursor_set_cluster_access(ib_crsr: ib_crsr_t) void {
    const cursor = ib_crsr orelse return;
    cursor.cluster_access = true;
}

pub fn ib_cursor_set_simple_select(ib_crsr: ib_crsr_t) void {
    const cursor = ib_crsr orelse return;
    cursor.simple_select = true;
}

test "ib_api_version matches C constants" {
    const expected = (@as(ib_u64_t, 3) << 32) | (@as(ib_u64_t, 0) << 16) | @as(ib_u64_t, 0);
    try std.testing.expectEqual(expected, ib_api_version());
}

test "ib_default_compare orders by content then length" {
    var meta = ib_col_meta_t{
        .type = .IB_VARCHAR,
        .attr = .IB_COL_NONE,
        .type_len = 0,
        .client_type = 0,
        .charset = null,
    };

    const a = "abc";
    const b = "abd";
    const c = "abcx";

    try std.testing.expect(ib_client_compare(&meta, a.ptr, a.len, b.ptr, b.len) < 0);
    try std.testing.expect(ib_client_compare(&meta, b.ptr, b.len, a.ptr, a.len) > 0);
    try std.testing.expect(ib_client_compare(&meta, a.ptr, a.len, c.ptr, c.len) < 0);
    try std.testing.expect(ib_client_compare(&meta, a.ptr, a.len, a.ptr, a.len) == 0);
}

test "ib_set_client_compare updates comparator" {
    var meta = ib_col_meta_t{
        .type = .IB_VARCHAR,
        .attr = .IB_COL_NONE,
        .type_len = 0,
        .client_type = 0,
        .charset = null,
    };

    const prev = ib_client_compare;
    defer ib_set_client_compare(prev);

    const always_equal = struct {
        fn cmp(
            _: *const ib_col_meta_t,
            _: [*]const ib_byte_t,
            _: ib_ulint_t,
            _: [*]const ib_byte_t,
            _: ib_ulint_t,
        ) i32 {
            return 0;
        }
    }.cmp;

    ib_set_client_compare(always_equal);
    try std.testing.expect(ib_client_compare(&meta, "a".ptr, 1, "b".ptr, 1) == 0);
}

test "ib_logger_set wires logger" {
    const prev_logger = log.getLogger();
    const prev_stream = log.getStream();
    defer log.setLogger(prev_logger, prev_stream);

    ib_logger_set(log.nullLogger, null);
    try std.testing.expect(log.getLogger() == log.nullLogger);
}

test "ib_startup validates format" {
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_startup(null));
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_startup("Antelope"));
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_startup("barracuda"));
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_startup("1"));
    try std.testing.expectEqual(errors.DbErr.DB_UNSUPPORTED, ib_startup("Zebra"));
}

test "ib_init and shutdown succeed" {
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_init());
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_shutdown(.IB_SHUTDOWN_NORMAL));
}

test "ib_trx lifecycle stubs" {
    const trx = ib_trx_begin(.IB_TRX_READ_COMMITTED);
    try std.testing.expect(trx != null);
    try std.testing.expectEqual(ib_trx_state_t.IB_TRX_ACTIVE, ib_trx_state(trx));
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_trx_commit(trx));
}

test "ib_trx_start rejects already started" {
    const trx = ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    defer _ = ib_trx_release(trx);
    try std.testing.expectEqual(errors.DbErr.DB_ERROR, ib_trx_start(trx, .IB_TRX_REPEATABLE_READ));
}

test "tuple read/write int float double" {
    const metas = [_]ib_col_meta_t{
        .{
            .type = .IB_INT,
            .attr = .IB_COL_NONE,
            .type_len = @intCast(@sizeOf(ib_i32_t)),
            .client_type = 0,
            .charset = null,
        },
        .{
            .type = .IB_FLOAT,
            .attr = .IB_COL_NONE,
            .type_len = @intCast(@sizeOf(f32)),
            .client_type = 0,
            .charset = null,
        },
        .{
            .type = .IB_DOUBLE,
            .attr = .IB_COL_NONE,
            .type_len = @intCast(@sizeOf(f64)),
            .client_type = 0,
            .charset = null,
        },
    };

    const tuple = try tupleCreate(std.heap.page_allocator, .TPL_ROW, &metas);
    defer ib_tuple_delete(tuple);

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_tuple_write_i32(tuple, 0, 42));
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_tuple_write_float(tuple, 1, 1.25));
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_tuple_write_double(tuple, 2, 2.5));

    var iv: ib_i32_t = 0;
    var fv: f32 = 0;
    var dv: f64 = 0;

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_tuple_read_i32(tuple, 0, &iv));
    try std.testing.expectEqual(@as(ib_i32_t, 42), iv);
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_tuple_read_float(tuple, 1, &fv));
    try std.testing.expectEqual(@as(f32, 1.25), fv);
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_tuple_read_double(tuple, 2, &dv));
    try std.testing.expectEqual(@as(f64, 2.5), dv);
}

test "tuple copy and clear" {
    const metas = [_]ib_col_meta_t{
        .{
            .type = .IB_INT,
            .attr = .IB_COL_NONE,
            .type_len = @intCast(@sizeOf(ib_u32_t)),
            .client_type = 0,
            .charset = null,
        },
    };

    const src = try tupleCreate(std.heap.page_allocator, .TPL_ROW, &metas);
    defer ib_tuple_delete(src);
    const dst = try tupleCreate(std.heap.page_allocator, .TPL_ROW, &metas);
    defer ib_tuple_delete(dst);

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_tuple_write_u32(src, 0, 7));
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_tuple_copy(dst, src));

    var out: ib_u32_t = 0;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_tuple_read_u32(dst, 0, &out));
    try std.testing.expectEqual(@as(ib_u32_t, 7), out);

    _ = ib_tuple_clear(dst);
    try std.testing.expectEqual(@as(ib_ulint_t, IB_SQL_NULL), ib_col_get_len(dst, 0));
}

test "cursor open/close and flags" {
    var crsr: ib_crsr_t = null;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cursor_open_table("db/t1", null, &crsr));
    try std.testing.expect(crsr != null);

    const cursor = crsr.?;
    try std.testing.expectEqual(ib_match_mode_t.IB_CLOSEST_MATCH, cursor.match_mode);

    ib_cursor_set_match_mode(crsr, .IB_EXACT_MATCH);
    try std.testing.expectEqual(ib_match_mode_t.IB_EXACT_MATCH, cursor.match_mode);

    try std.testing.expectEqual(errors.DbErr.DB_RECORD_NOT_FOUND, ib_cursor_next(crsr));
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cursor_first(crsr));
    try std.testing.expectEqual(compat.IB_TRUE, ib_cursor_is_positioned(crsr));

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cursor_reset(crsr));
    try std.testing.expectEqual(compat.IB_FALSE, ib_cursor_is_positioned(crsr));

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cursor_set_lock_mode(crsr, .IB_LOCK_S));
    try std.testing.expectEqual(ib_lck_mode_t.IB_LOCK_S, cursor.lock_mode);
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cursor_unlock(crsr));
    try std.testing.expectEqual(ib_lck_mode_t.IB_LOCK_NONE, cursor.lock_mode);

    ib_cursor_set_cluster_access(crsr);
    ib_cursor_set_simple_select(crsr);
    try std.testing.expect(cursor.cluster_access);
    try std.testing.expect(cursor.simple_select);

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cursor_close(crsr));
}

test "cursor moveto positions and read row" {
    var crsr: ib_crsr_t = null;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cursor_open_table("db/t2", null, &crsr));
    defer _ = ib_cursor_close(crsr);

    const metas = [_]ib_col_meta_t{
        .{
            .type = .IB_INT,
            .attr = .IB_COL_NONE,
            .type_len = @intCast(@sizeOf(ib_i32_t)),
            .client_type = 0,
            .charset = null,
        },
    };

    const key_tpl = try tupleCreate(std.heap.page_allocator, .TPL_KEY, &metas);
    defer ib_tuple_delete(key_tpl);
    const row_tpl = try tupleCreate(std.heap.page_allocator, .TPL_ROW, &metas);
    defer ib_tuple_delete(row_tpl);

    var result: i32 = -1;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cursor_moveto(crsr, key_tpl, .IB_CUR_GE, &result));
    try std.testing.expectEqual(@as(i32, 0), result);
    try std.testing.expectEqual(compat.IB_TRUE, ib_cursor_is_positioned(crsr));

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cursor_read_row(crsr, row_tpl));
}

test "cursor open index variants" {
    var table_crsr: ib_crsr_t = null;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cursor_open_table("db/t3", null, &table_crsr));
    defer _ = ib_cursor_close(table_crsr);

    var idx_by_name: ib_crsr_t = null;
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_cursor_open_index_using_name(table_crsr, "idx_name", &idx_by_name),
    );
    const named_cursor = idx_by_name.?;
    try std.testing.expect(named_cursor.index_name != null);
    try std.testing.expect(std.mem.eql(u8, named_cursor.index_name.?, "idx_name"));
    try std.testing.expect(named_cursor.table_name != null);
    try std.testing.expect(std.mem.eql(u8, named_cursor.table_name.?, "db/t3"));
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cursor_close(idx_by_name));

    var idx_by_id: ib_crsr_t = null;
    const idx_id: ib_id_t = (@as(ib_id_t, 1) << 32) | 2;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cursor_open_index_using_id(idx_id, null, &idx_by_id));
    const id_cursor = idx_by_id.?;
    try std.testing.expectEqual(@as(ib_id_t, 1), id_cursor.table_id.?);
    try std.testing.expectEqual(idx_id, id_cursor.index_id.?);
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cursor_close(idx_by_id));
}
