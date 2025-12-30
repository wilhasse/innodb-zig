const std = @import("std");
const ArrayList = std.array_list.Managed;
const build_options = @import("build_options");
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
pub const ib_cb_t = *const fn () callconv(.c) void;

const charset_t = opaque {};

pub const ib_logger_t = log.LoggerFn;
pub const ib_stream_t = log.Stream;
pub const ib_msg_log_t = log.LoggerFn;
pub const ib_msg_stream_t = log.Stream;

pub const IB_API_VERSION_CURRENT: u64 = 3;
pub const IB_API_VERSION_REVISION: u64 = 0;
pub const IB_API_VERSION_AGE: u64 = 0;

const dict_tf_format_max: u32 = 1;
const dict_tf_format_zip: u32 = 1;
const file_format_names = [_][]const u8{
    "Antelope",
    "Barracuda",
};

const SchemaLock = enum(u8) {
    none,
    shared,
    exclusive,
};

const Savepoint = struct {
    name: []u8,
};

pub const Trx = struct {
    state: ib_trx_state_t,
    isolation_level: ib_trx_level_t,
    client_thread_id: os_thread.ThreadId,
    schema_lock: SchemaLock,
    savepoints: ArrayList(Savepoint),
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
    rows: ArrayList(*Tuple),
    position: ?usize,
    sql_stat_start: bool,
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

const SchemaColumn = struct {
    name: []u8,
    col_type: ib_col_type_t,
    attr: ib_col_attr_t,
    client_type: ib_u16_t,
    len: ib_ulint_t,
};

const IndexColumn = struct {
    name: []u8,
    prefix_len: ib_ulint_t,
};

pub const TableSchema = struct {
    allocator: std.mem.Allocator,
    name: []u8,
    format: ib_tbl_fmt_t,
    page_size: ib_ulint_t,
    columns: ArrayList(SchemaColumn),
    indexes: ArrayList(*IndexSchema),
    table_id: ?ib_id_t,
};

pub const IndexSchema = struct {
    allocator: std.mem.Allocator,
    name: []u8,
    table_name: []u8,
    columns: ArrayList(IndexColumn),
    clustered: bool,
    unique: bool,
    schema_owner: ?*TableSchema,
    trx: ib_trx_t,
};

const CatalogColumn = struct {
    name: []u8,
    col_type: ib_col_type_t,
    attr: ib_col_attr_t,
    len: ib_ulint_t,
};

const CatalogIndexColumn = struct {
    name: []u8,
    prefix_len: ib_ulint_t,
};

const CatalogIndex = struct {
    name: []u8,
    clustered: bool,
    unique: bool,
    id: ib_id_t,
    columns: ArrayList(CatalogIndexColumn),
};

const CatalogTable = struct {
    allocator: std.mem.Allocator,
    name: []u8,
    format: ib_tbl_fmt_t,
    page_size: ib_ulint_t,
    id: ib_id_t,
    stat_modified_counter: ib_ulint_t,
    stat_n_rows: ib_ulint_t,
    stats_updated: bool,
    columns: ArrayList(CatalogColumn),
    indexes: ArrayList(CatalogIndex),
};

pub const ib_trx_t = ?*Trx;
pub const ib_crsr_t = ?*Cursor;
pub const ib_tpl_t = ?*Tuple;
pub const ib_tbl_sch_t = ?*TableSchema;
pub const ib_idx_sch_t = ?*IndexSchema;

pub const ib_id_t = ib_u64_t;
pub const ib_cfg_type_t = enum(u32) {
    IB_CFG_IBOOL = 0,
    IB_CFG_ULINT = 1,
    IB_CFG_ULONG = 2,
    IB_CFG_TEXT = 3,
    IB_CFG_CB = 4,
};
pub const ib_cfg_flag_t = enum(u32) {
    IB_CFG_FLAG_NONE = 0x1,
    IB_CFG_FLAG_READONLY_AFTER_STARTUP = 0x2,
    IB_CFG_FLAG_READONLY = 0x4,
};

pub const ib_i8_t = i8;
pub const ib_u8_t = u8;
pub const ib_i16_t = i16;
pub const ib_i32_t = i32;
pub const ib_i64_t = i64;

const ib_status_type_t = enum(u8) {
    IB_STATUS_IBOOL = 0,
    IB_STATUS_I64 = 1,
    IB_STATUS_ULINT = 2,
};

const StatusVar = struct {
    name: []const u8,
    status_type: ib_status_type_t,
    value_ptr: *const anyopaque,
};

const ExportVars = struct {
    innodb_data_pending_reads: ib_ulint_t = 0,
    innodb_data_pending_writes: ib_ulint_t = 0,
    innodb_data_pending_fsyncs: ib_ulint_t = 0,
    innodb_data_fsyncs: ib_ulint_t = 0,
    innodb_data_read: ib_ulint_t = 0,
    innodb_data_writes: ib_ulint_t = 0,
    innodb_data_written: ib_ulint_t = 0,
    innodb_data_reads: ib_ulint_t = 0,
    innodb_buffer_pool_pages_total: ib_ulint_t = 0,
    innodb_buffer_pool_pages_data: ib_ulint_t = 0,
    innodb_buffer_pool_pages_dirty: ib_ulint_t = 0,
    innodb_buffer_pool_pages_misc: ib_ulint_t = 0,
    innodb_buffer_pool_pages_free: ib_ulint_t = 0,
    innodb_buffer_pool_read_requests: ib_ulint_t = 0,
    innodb_buffer_pool_reads: ib_ulint_t = 0,
    innodb_buffer_pool_wait_free: ib_ulint_t = 0,
    innodb_buffer_pool_pages_flushed: ib_ulint_t = 0,
    innodb_buffer_pool_write_requests: ib_ulint_t = 0,
    innodb_buffer_pool_read_ahead: ib_ulint_t = 0,
    innodb_buffer_pool_read_ahead_evicted: ib_ulint_t = 0,
    innodb_dblwr_pages_written: ib_ulint_t = 0,
    innodb_dblwr_writes: ib_ulint_t = 0,
    innodb_have_atomic_builtins: ib_bool_t = 0,
    innodb_log_waits: ib_ulint_t = 0,
    innodb_log_write_requests: ib_ulint_t = 0,
    innodb_log_writes: ib_ulint_t = 0,
    innodb_os_log_written: ib_ulint_t = 0,
    innodb_os_log_fsyncs: ib_ulint_t = 0,
    innodb_os_log_pending_writes: ib_ulint_t = 0,
    innodb_os_log_pending_fsyncs: ib_ulint_t = 0,
    innodb_page_size: ib_ulint_t = 0,
    innodb_pages_created: ib_ulint_t = 0,
    innodb_pages_read: ib_ulint_t = 0,
    innodb_pages_written: ib_ulint_t = 0,
    innodb_row_lock_waits: ib_ulint_t = 0,
    innodb_row_lock_current_waits: ib_ulint_t = 0,
    innodb_row_lock_time: ib_i64_t = 0,
    innodb_row_lock_time_avg: ib_ulint_t = 0,
    innodb_row_lock_time_max: ib_ulint_t = 0,
    innodb_rows_read: ib_ulint_t = 0,
    innodb_rows_inserted: ib_ulint_t = 0,
    innodb_rows_updated: ib_ulint_t = 0,
    innodb_rows_deleted: ib_ulint_t = 0,
};

pub const IB_SQL_NULL: ib_u32_t = 0xFFFF_FFFF;
pub const IB_N_SYS_COLS: ib_u32_t = 3;
pub const MAX_TEXT_LEN: ib_u32_t = 4096;
pub const IB_MAX_COL_NAME_LEN: ib_u32_t = 64 * 3;
pub const IB_MAX_TABLE_NAME_LEN: ib_u32_t = 64 * 3;

pub const ib_schema_visitor_version_t = enum(u32) {
    IB_SCHEMA_VISITOR_TABLE = 1,
    IB_SCHEMA_VISITOR_TABLE_COL = 2,
    IB_SCHEMA_VISITOR_TABLE_AND_INDEX = 3,
    IB_SCHEMA_VISITOR_TABLE_AND_INDEX_COL = 4,
};

pub const ib_schema_visitor_table_all_t = *const fn (
    arg: ib_opaque_t,
    name: [*]const u8,
    name_len: i32,
) callconv(.c) i32;

pub const ib_schema_visitor_table_t = *const fn (
    arg: ib_opaque_t,
    name: [*]const u8,
    tbl_fmt: ib_tbl_fmt_t,
    page_size: ib_ulint_t,
    n_cols: i32,
    n_indexes: i32,
) callconv(.c) i32;

pub const ib_schema_visitor_table_col_t = *const fn (
    arg: ib_opaque_t,
    name: [*]const u8,
    col_type: ib_col_type_t,
    len: ib_ulint_t,
    attr: ib_col_attr_t,
) callconv(.c) i32;

pub const ib_schema_visitor_index_t = *const fn (
    arg: ib_opaque_t,
    name: [*]const u8,
    clustered: ib_bool_t,
    unique: ib_bool_t,
    n_cols: i32,
) callconv(.c) i32;

pub const ib_schema_visitor_index_col_t = *const fn (
    arg: ib_opaque_t,
    name: [*]const u8,
    prefix_len: ib_ulint_t,
) callconv(.c) i32;

pub const ib_schema_visitor_t = struct {
    version: ib_schema_visitor_version_t,
    table: ?ib_schema_visitor_table_t,
    table_col: ?ib_schema_visitor_table_col_t,
    index: ?ib_schema_visitor_index_t,
    index_col: ?ib_schema_visitor_index_col_t,
};

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
};
pub const IB_LOCK_NUM: u32 = @intFromEnum(ib_lck_mode_t.IB_LOCK_NONE);

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

pub fn ib_ucode_get_connection_charset() ?*const charset_t {
    return null;
}

pub fn ib_ucode_get_charset(id: ib_ulint_t) ?*const charset_t {
    _ = id;
    return null;
}

pub fn ib_ucode_get_charset_width(cs: ?*const charset_t, mbminlen: *ib_ulint_t, mbmaxlen: *ib_ulint_t) void {
    _ = cs;
    mbminlen.* = 0;
    mbmaxlen.* = 0;
}

fn compareIgnoreCase(a: []const u8, b: []const u8) i32 {
    return switch (std.ascii.orderIgnoreCase(a, b)) {
        .lt => -1,
        .eq => 0,
        .gt => 1,
    };
}

pub fn ib_utf8_strcasecmp(p1: []const u8, p2: []const u8) i32 {
    return compareIgnoreCase(p1, p2);
}

pub fn ib_utf8_strncasecmp(p1: []const u8, p2: []const u8, len: ib_ulint_t) i32 {
    const n = @as(usize, @intCast(len));
    const a = p1[0..@min(p1.len, n)];
    const b = p2[0..@min(p2.len, n)];
    return compareIgnoreCase(a, b);
}

pub fn ib_utf8_casedown(a: []u8) void {
    for (a) |*ch| {
        ch.* = std.ascii.toLower(ch.*);
    }
}

fn copyWithLimit(to: []u8, from: []const u8, len: ib_ulint_t) void {
    const max_len = @min(@as(usize, @intCast(len)), to.len);
    const copy_len = @min(from.len, max_len);
    if (copy_len > 0) {
        std.mem.copyForwards(u8, to[0..copy_len], from[0..copy_len]);
    }
    if (copy_len < max_len) {
        for (to[copy_len..max_len]) |*byte| {
            byte.* = 0;
        }
    }
}

pub fn ib_utf8_convert_from_table_id(cs: ?*const charset_t, to: []u8, from: []const u8, len: ib_ulint_t) void {
    _ = cs;
    copyWithLimit(to, from, len);
}

pub fn ib_utf8_convert_from_id(cs: ?*const charset_t, to: []u8, from: []const u8, len: ib_ulint_t) void {
    _ = cs;
    copyWithLimit(to, from, len);
}

pub fn ib_utf8_isspace(cs: ?*const charset_t, c: u8) i32 {
    _ = cs;
    return if (std.ascii.isWhitespace(c)) 1 else 0;
}

pub fn ib_ucode_get_storage_size(cs: ?*const charset_t, prefix_len: ib_ulint_t, str_len: ib_ulint_t, str: []const u8) ib_ulint_t {
    _ = cs;
    _ = str;
    return @min(prefix_len, str_len);
}

const DbFormat = struct {
    id: u32,
    name: ?[]const u8,
};

var db_format: DbFormat = .{ .id = 0, .name = null };
var next_table_id: ib_id_t = 1;
var next_index_id: ib_id_t = 1;
var table_registry = ArrayList(*CatalogTable).init(std.heap.page_allocator);
const CfgValue = union(ib_cfg_type_t) {
    IB_CFG_IBOOL: ib_bool_t,
    IB_CFG_ULINT: ib_ulint_t,
    IB_CFG_ULONG: ib_ulint_t,
    IB_CFG_TEXT: []const u8,
    IB_CFG_CB: ?ib_cb_t,
};

const CfgVar = struct {
    name: []const u8,
    cfg_type: ib_cfg_type_t,
    flag: ib_cfg_flag_t,
    min_val: ib_u64_t,
    max_val: ib_u64_t,
    value: CfgValue,
};

const cfg_vars_defaults = [_]CfgVar{
    .{
        .name = "adaptive_hash_index",
        .cfg_type = .IB_CFG_IBOOL,
        .flag = .IB_CFG_FLAG_READONLY_AFTER_STARTUP,
        .min_val = 0,
        .max_val = 0,
        .value = .{ .IB_CFG_IBOOL = compat.IB_TRUE },
    },
    .{
        .name = "adaptive_flushing",
        .cfg_type = .IB_CFG_IBOOL,
        .flag = .IB_CFG_FLAG_READONLY_AFTER_STARTUP,
        .min_val = 0,
        .max_val = 0,
        .value = .{ .IB_CFG_IBOOL = compat.IB_TRUE },
    },
    .{
        .name = "additional_mem_pool_size",
        .cfg_type = .IB_CFG_ULINT,
        .flag = .IB_CFG_FLAG_READONLY_AFTER_STARTUP,
        .min_val = 512 * 1024,
        .max_val = @as(ib_u64_t, compat.IB_UINT64_T_MAX),
        .value = .{ .IB_CFG_ULINT = @as(ib_ulint_t, 4 * 1024 * 1024) },
    },
    .{
        .name = "autoextend_increment",
        .cfg_type = .IB_CFG_ULONG,
        .flag = .IB_CFG_FLAG_NONE,
        .min_val = 1,
        .max_val = 1000,
        .value = .{ .IB_CFG_ULONG = 1 },
    },
    .{
        .name = "buffer_pool_size",
        .cfg_type = .IB_CFG_ULINT,
        .flag = .IB_CFG_FLAG_READONLY_AFTER_STARTUP,
        .min_val = 5 * 1024 * 1024,
        .max_val = @as(ib_u64_t, @intCast(compat.ULINT_MAX)),
        .value = .{ .IB_CFG_ULINT = @as(ib_ulint_t, 8 * 1024 * 1024) },
    },
    .{
        .name = "checksums",
        .cfg_type = .IB_CFG_IBOOL,
        .flag = .IB_CFG_FLAG_READONLY_AFTER_STARTUP,
        .min_val = 0,
        .max_val = 0,
        .value = .{ .IB_CFG_IBOOL = compat.IB_TRUE },
    },
    .{
        .name = "data_file_path",
        .cfg_type = .IB_CFG_TEXT,
        .flag = .IB_CFG_FLAG_READONLY_AFTER_STARTUP,
        .min_val = 0,
        .max_val = 0,
        .value = .{ .IB_CFG_TEXT = "ibdata1:32M:autoextend" },
    },
    .{
        .name = "data_home_dir",
        .cfg_type = .IB_CFG_TEXT,
        .flag = .IB_CFG_FLAG_READONLY_AFTER_STARTUP,
        .min_val = 0,
        .max_val = 0,
        .value = .{ .IB_CFG_TEXT = "./" },
    },
    .{
        .name = "doublewrite",
        .cfg_type = .IB_CFG_IBOOL,
        .flag = .IB_CFG_FLAG_READONLY_AFTER_STARTUP,
        .min_val = 0,
        .max_val = 0,
        .value = .{ .IB_CFG_IBOOL = compat.IB_TRUE },
    },
    .{
        .name = "file_format",
        .cfg_type = .IB_CFG_TEXT,
        .flag = .IB_CFG_FLAG_NONE,
        .min_val = 0,
        .max_val = 0,
        .value = .{ .IB_CFG_TEXT = "Antelope" },
    },
    .{
        .name = "file_io_threads",
        .cfg_type = .IB_CFG_ULINT,
        .flag = .IB_CFG_FLAG_READONLY_AFTER_STARTUP,
        .min_val = 4,
        .max_val = 64,
        .value = .{ .IB_CFG_ULINT = 4 },
    },
    .{
        .name = "file_per_table",
        .cfg_type = .IB_CFG_IBOOL,
        .flag = .IB_CFG_FLAG_NONE,
        .min_val = 0,
        .max_val = 0,
        .value = .{ .IB_CFG_IBOOL = compat.IB_TRUE },
    },
    .{
        .name = "flush_log_at_trx_commit",
        .cfg_type = .IB_CFG_ULONG,
        .flag = .IB_CFG_FLAG_NONE,
        .min_val = 0,
        .max_val = 2,
        .value = .{ .IB_CFG_ULONG = 1 },
    },
    .{
        .name = "flush_method",
        .cfg_type = .IB_CFG_TEXT,
        .flag = .IB_CFG_FLAG_READONLY_AFTER_STARTUP,
        .min_val = 0,
        .max_val = 0,
        .value = .{ .IB_CFG_TEXT = "fsync" },
    },
    .{
        .name = "force_recovery",
        .cfg_type = .IB_CFG_ULINT,
        .flag = .IB_CFG_FLAG_READONLY_AFTER_STARTUP,
        .min_val = 0,
        .max_val = 6,
        .value = .{ .IB_CFG_ULINT = 0 },
    },
    .{
        .name = "io_capacity",
        .cfg_type = .IB_CFG_ULONG,
        .flag = .IB_CFG_FLAG_NONE,
        .min_val = 100,
        .max_val = 1_000_000,
        .value = .{ .IB_CFG_ULONG = 200 },
    },
    .{
        .name = "lock_wait_timeout",
        .cfg_type = .IB_CFG_ULINT,
        .flag = .IB_CFG_FLAG_NONE,
        .min_val = 1,
        .max_val = 1_073_741_824,
        .value = .{ .IB_CFG_ULINT = 60 },
    },
    .{
        .name = "log_buffer_size",
        .cfg_type = .IB_CFG_ULINT,
        .flag = .IB_CFG_FLAG_READONLY_AFTER_STARTUP,
        .min_val = 256 * 1024,
        .max_val = @as(ib_u64_t, compat.IB_UINT64_T_MAX),
        .value = .{ .IB_CFG_ULINT = @as(ib_ulint_t, 384 * 1024) },
    },
    .{
        .name = "log_file_size",
        .cfg_type = .IB_CFG_ULINT,
        .flag = .IB_CFG_FLAG_READONLY_AFTER_STARTUP,
        .min_val = 1024 * 1024,
        .max_val = @as(ib_u64_t, @intCast(compat.ULINT_MAX)),
        .value = .{ .IB_CFG_ULINT = @as(ib_ulint_t, 16 * 1024 * 1024) },
    },
    .{
        .name = "log_files_in_group",
        .cfg_type = .IB_CFG_ULINT,
        .flag = .IB_CFG_FLAG_READONLY_AFTER_STARTUP,
        .min_val = 2,
        .max_val = 100,
        .value = .{ .IB_CFG_ULINT = 2 },
    },
    .{
        .name = "log_group_home_dir",
        .cfg_type = .IB_CFG_TEXT,
        .flag = .IB_CFG_FLAG_READONLY_AFTER_STARTUP,
        .min_val = 0,
        .max_val = 0,
        .value = .{ .IB_CFG_TEXT = "." },
    },
    .{
        .name = "max_dirty_pages_pct",
        .cfg_type = .IB_CFG_ULONG,
        .flag = .IB_CFG_FLAG_NONE,
        .min_val = 0,
        .max_val = 100,
        .value = .{ .IB_CFG_ULONG = 75 },
    },
    .{
        .name = "max_purge_lag",
        .cfg_type = .IB_CFG_ULONG,
        .flag = .IB_CFG_FLAG_NONE,
        .min_val = 0,
        .max_val = @as(ib_u64_t, compat.IB_UINT64_T_MAX),
        .value = .{ .IB_CFG_ULONG = 0 },
    },
    .{
        .name = "lru_old_blocks_pct",
        .cfg_type = .IB_CFG_ULINT,
        .flag = .IB_CFG_FLAG_NONE,
        .min_val = 5,
        .max_val = 95,
        .value = .{ .IB_CFG_ULINT = @as(ib_ulint_t, 3 * 100 / 8) },
    },
    .{
        .name = "lru_block_access_recency",
        .cfg_type = .IB_CFG_ULINT,
        .flag = .IB_CFG_FLAG_NONE,
        .min_val = 0,
        .max_val = 0xFFFF_FFFF,
        .value = .{ .IB_CFG_ULINT = 0 },
    },
    .{
        .name = "open_files",
        .cfg_type = .IB_CFG_ULINT,
        .flag = .IB_CFG_FLAG_READONLY_AFTER_STARTUP,
        .min_val = 10,
        .max_val = @as(ib_u64_t, compat.IB_UINT64_T_MAX),
        .value = .{ .IB_CFG_ULINT = 10 },
    },
    .{
        .name = "read_io_threads",
        .cfg_type = .IB_CFG_ULINT,
        .flag = .IB_CFG_FLAG_READONLY_AFTER_STARTUP,
        .min_val = 1,
        .max_val = 64,
        .value = .{ .IB_CFG_ULINT = 4 },
    },
    .{
        .name = "write_io_threads",
        .cfg_type = .IB_CFG_ULINT,
        .flag = .IB_CFG_FLAG_READONLY_AFTER_STARTUP,
        .min_val = 1,
        .max_val = 64,
        .value = .{ .IB_CFG_ULINT = 4 },
    },
    .{
        .name = "pre_rollback_hook",
        .cfg_type = .IB_CFG_CB,
        .flag = .IB_CFG_FLAG_NONE,
        .min_val = 0,
        .max_val = 0,
        .value = .{ .IB_CFG_CB = null },
    },
    .{
        .name = "print_verbose_log",
        .cfg_type = .IB_CFG_IBOOL,
        .flag = .IB_CFG_FLAG_NONE,
        .min_val = 0,
        .max_val = 0,
        .value = .{ .IB_CFG_IBOOL = compat.IB_FALSE },
    },
    .{
        .name = "rollback_on_timeout",
        .cfg_type = .IB_CFG_IBOOL,
        .flag = .IB_CFG_FLAG_NONE,
        .min_val = 0,
        .max_val = 0,
        .value = .{ .IB_CFG_IBOOL = compat.IB_TRUE },
    },
    .{
        .name = "stats_sample_pages",
        .cfg_type = .IB_CFG_ULINT,
        .flag = .IB_CFG_FLAG_NONE,
        .min_val = 1,
        .max_val = @as(ib_u64_t, @intCast(compat.ULINT_MAX)),
        .value = .{ .IB_CFG_ULINT = 8 },
    },
    .{
        .name = "status_file",
        .cfg_type = .IB_CFG_IBOOL,
        .flag = .IB_CFG_FLAG_NONE,
        .min_val = 0,
        .max_val = 0,
        .value = .{ .IB_CFG_IBOOL = compat.IB_FALSE },
    },
    .{
        .name = "sync_spin_loops",
        .cfg_type = .IB_CFG_ULONG,
        .flag = .IB_CFG_FLAG_NONE,
        .min_val = 0,
        .max_val = @as(ib_u64_t, compat.IB_UINT64_T_MAX),
        .value = .{ .IB_CFG_ULONG = 0 },
    },
    .{
        .name = "use_sys_malloc",
        .cfg_type = .IB_CFG_IBOOL,
        .flag = .IB_CFG_FLAG_NONE,
        .min_val = 0,
        .max_val = 0,
        .value = .{ .IB_CFG_IBOOL = compat.IB_FALSE },
    },
    .{
        .name = "version",
        .cfg_type = .IB_CFG_TEXT,
        .flag = .IB_CFG_FLAG_READONLY,
        .min_val = 0,
        .max_val = 0,
        .value = .{ .IB_CFG_TEXT = "zig-port" },
    },
};

var cfg_vars = cfg_vars_defaults;
var cfg_initialized = false;
var cfg_started = false;
var cfg_mutex = std.Thread.Mutex{};
var ses_rollback_on_timeout: ib_bool_t = compat.IB_FALSE;
var export_vars = ExportVars{
    .innodb_page_size = compat.UNIV_PAGE_SIZE,
    .innodb_have_atomic_builtins = compat.IB_TRUE,
};

fn statusPtr(ptr: anytype) *const anyopaque {
    return @as(*const anyopaque, @ptrCast(ptr));
}

const status_vars = [_]StatusVar{
    .{ .name = "read_req_pending", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_data_pending_reads) },
    .{ .name = "write_req_pending", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_data_pending_writes) },
    .{ .name = "fsync_req_pending", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_data_pending_fsyncs) },
    .{ .name = "write_req_done", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_data_writes) },
    .{ .name = "read_req_done", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_data_reads) },
    .{ .name = "fsync_req_done", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_data_fsyncs) },
    .{ .name = "bytes_total_written", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_data_written) },
    .{ .name = "bytes_total_read", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_data_read) },
    .{ .name = "buffer_pool_current_size", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_buffer_pool_pages_total) },
    .{ .name = "buffer_pool_data_pages", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_buffer_pool_pages_data) },
    .{ .name = "buffer_pool_dirty_pages", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_buffer_pool_pages_dirty) },
    .{ .name = "buffer_pool_misc_pages", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_buffer_pool_pages_misc) },
    .{ .name = "buffer_pool_free_pages", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_buffer_pool_pages_free) },
    .{ .name = "buffer_pool_read_reqs", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_buffer_pool_read_requests) },
    .{ .name = "buffer_pool_reads", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_buffer_pool_reads) },
    .{ .name = "buffer_pool_waited_for_free", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_buffer_pool_wait_free) },
    .{ .name = "buffer_pool_pages_flushed", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_buffer_pool_pages_flushed) },
    .{ .name = "buffer_pool_write_reqs", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_buffer_pool_write_requests) },
    .{ .name = "buffer_pool_total_pages", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_pages_created) },
    .{ .name = "buffer_pool_pages_read", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_pages_read) },
    .{ .name = "buffer_pool_pages_written", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_pages_written) },
    .{ .name = "double_write_pages_written", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_dblwr_pages_written) },
    .{ .name = "double_write_invoked", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_dblwr_writes) },
    .{ .name = "log_buffer_slot_waits", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_log_waits) },
    .{ .name = "log_write_reqs", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_log_write_requests) },
    .{ .name = "log_write_flush_count", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_log_writes) },
    .{ .name = "log_bytes_written", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_os_log_written) },
    .{ .name = "log_fsync_req_done", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_os_log_fsyncs) },
    .{ .name = "log_write_req_pending", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_os_log_pending_writes) },
    .{ .name = "log_fsync_req_pending", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_os_log_pending_fsyncs) },
    .{ .name = "lock_row_waits", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_row_lock_waits) },
    .{ .name = "lock_row_waiting", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_row_lock_current_waits) },
    .{ .name = "lock_total_wait_time_in_secs", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_row_lock_time) },
    .{ .name = "lock_wait_time_avg_in_secs", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_row_lock_time_avg) },
    .{ .name = "lock_max_wait_time_in_secs", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_row_lock_time_max) },
    .{ .name = "row_total_read", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_rows_read) },
    .{ .name = "row_total_inserted", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_rows_inserted) },
    .{ .name = "row_total_updated", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_rows_updated) },
    .{ .name = "row_total_deleted", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_rows_deleted) },
    .{ .name = "page_size", .status_type = .IB_STATUS_ULINT, .value_ptr = statusPtr(&export_vars.innodb_page_size) },
    .{ .name = "have_atomic_builtins", .status_type = .IB_STATUS_IBOOL, .value_ptr = statusPtr(&export_vars.innodb_have_atomic_builtins) },
};

pub fn ib_init() ib_err_t {
    return ib_cfg_init();
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

    cfg_started = true;
    return .DB_SUCCESS;
}

pub fn ib_shutdown(flag: ib_shutdown_t) ib_err_t {
    _ = flag;
    _ = ib_cfg_shutdown();
    db_format.id = 0;
    db_format.name = null;
    catalogClear();
    next_table_id = 1;
    next_index_id = 1;
    return .DB_SUCCESS;
}

fn cfgFind(name: []const u8) ?*CfgVar {
    for (&cfg_vars) |*cfg_var| {
        if (std.ascii.eqlIgnoreCase(name, cfg_var.name)) {
            return cfg_var;
        }
    }
    return null;
}

fn cfgBoolFromAny(value: anytype) ?ib_bool_t {
    const T = @TypeOf(value);
    if (T == bool) {
        return if (value) compat.IB_TRUE else compat.IB_FALSE;
    }
    switch (@typeInfo(T)) {
        .comptime_int => {
            if (value != 0 and value != 1) {
                return null;
            }
            return @intCast(value);
        },
        .int => |_| {
            if (T == ib_bool_t) {
                return value;
            }
            return null;
        },
        else => return null,
    }
}

fn cfgUlintFromAny(value: anytype) ?ib_ulint_t {
    const T = @TypeOf(value);
    if (T == ib_ulint_t) {
        return value;
    }
    switch (@typeInfo(T)) {
        .comptime_int => {
            if (value < 0) {
                return null;
            }
            return @intCast(value);
        },
        else => return null,
    }
}

fn cfgTextFromAny(value: anytype) ?[]const u8 {
    const T = @TypeOf(value);
    switch (@typeInfo(T)) {
        .pointer => |ptr| {
            if (ptr.size == .slice and ptr.child == u8) {
                return value[0..];
            }
            if ((ptr.size == .many or ptr.size == .c) and ptr.sentinel_ptr != null) {
                if (ptr.child == u8) {
                    const c_ptr: [*:0]const u8 = @ptrCast(value);
                    return std.mem.span(c_ptr);
                }
            }
            if (ptr.size == .one) {
                const child_info = @typeInfo(ptr.child);
                if (child_info == .array and child_info.array.child == u8) {
                    if (child_info.array.sentinel_ptr != null) {
                        const c_ptr: [*:0]const u8 = @ptrCast(value);
                        return std.mem.span(c_ptr);
                    }
                    return value.*[0..child_info.array.len];
                }
            }
        },
        .array => |arr| {
            if (arr.child == u8) {
                return value[0..];
            }
        },
        else => {},
    }
    return null;
}

fn cfgIsReadOnly(cfg_var: *const CfgVar) bool {
    const flag_val = @intFromEnum(cfg_var.flag);
    if ((flag_val & @intFromEnum(ib_cfg_flag_t.IB_CFG_FLAG_READONLY)) != 0) {
        return true;
    }
    if (cfg_started and (flag_val & @intFromEnum(ib_cfg_flag_t.IB_CFG_FLAG_READONLY_AFTER_STARTUP)) != 0) {
        return true;
    }
    return false;
}

fn cfgValidateNumeric(cfg_var: *const CfgVar, value: ib_u64_t) ib_err_t {
    if (cfg_var.min_val != 0 or cfg_var.max_val != 0) {
        if (value < cfg_var.min_val or value > cfg_var.max_val) {
            return .DB_INVALID_INPUT;
        }
    }
    return .DB_SUCCESS;
}

fn cfgFlushMethodValid(value: []const u8) bool {
    const allowed = [_][]const u8{
        "fsync",
        "O_DSYNC",
        "O_DIRECT",
        "littlesync",
        "nosync",
        "normal",
        "unbuffered",
        "async_unbuffered",
    };

    for (allowed) |name| {
        if (std.mem.eql(u8, value, name)) {
            return true;
        }
    }
    return false;
}

fn cfgValidateText(cfg_var: *const CfgVar, value: []const u8) ib_err_t {
    if (schemaNameEq(cfg_var.name, "data_home_dir")) {
        if (value.len == 0) {
            return .DB_INVALID_INPUT;
        }
        const last = value[value.len - 1];
        if (last != '/' and last != '\\') {
            return .DB_INVALID_INPUT;
        }
    } else if (schemaNameEq(cfg_var.name, "data_file_path")) {
        if (value.len == 0) {
            return .DB_INVALID_INPUT;
        }
    } else if (schemaNameEq(cfg_var.name, "file_format")) {
        if (parseFileFormat(value) == null) {
            return .DB_INVALID_INPUT;
        }
    } else if (schemaNameEq(cfg_var.name, "flush_method")) {
        if (!cfgFlushMethodValid(value)) {
            return .DB_INVALID_INPUT;
        }
    } else if (schemaNameEq(cfg_var.name, "log_group_home_dir")) {
        if (value.len == 0) {
            return .DB_INVALID_INPUT;
        }
    }

    return .DB_SUCCESS;
}

fn cfgSetValue(cfg_var: *CfgVar, value: anytype) ib_err_t {
    if (cfgIsReadOnly(cfg_var)) {
        return .DB_READONLY;
    }

    switch (cfg_var.cfg_type) {
        .IB_CFG_IBOOL => {
            const val = cfgBoolFromAny(value) orelse return .DB_INVALID_INPUT;
            cfg_var.value = .{ .IB_CFG_IBOOL = val };
            if (schemaNameEq(cfg_var.name, "rollback_on_timeout")) {
                ses_rollback_on_timeout = val;
            }
            return .DB_SUCCESS;
        },
        .IB_CFG_ULINT => {
            const val = cfgUlintFromAny(value) orelse return .DB_INVALID_INPUT;
            const err = cfgValidateNumeric(cfg_var, @as(ib_u64_t, @intCast(val)));
            if (err != .DB_SUCCESS) {
                return err;
            }
            cfg_var.value = .{ .IB_CFG_ULINT = val };
            return .DB_SUCCESS;
        },
        .IB_CFG_ULONG => {
            const val = cfgUlintFromAny(value) orelse return .DB_INVALID_INPUT;
            const err = cfgValidateNumeric(cfg_var, @as(ib_u64_t, @intCast(val)));
            if (err != .DB_SUCCESS) {
                return err;
            }
            cfg_var.value = .{ .IB_CFG_ULONG = val };
            return .DB_SUCCESS;
        },
        .IB_CFG_TEXT => {
            const val = cfgTextFromAny(value) orelse return .DB_INVALID_INPUT;
            const err = cfgValidateText(cfg_var, val);
            if (err != .DB_SUCCESS) {
                return err;
            }
            if (schemaNameEq(cfg_var.name, "file_format")) {
                const id = parseFileFormat(val) orelse return .DB_INVALID_INPUT;
                cfg_var.value = .{ .IB_CFG_TEXT = file_format_names[id] };
                return .DB_SUCCESS;
            }
            cfg_var.value = .{ .IB_CFG_TEXT = val };
            return .DB_SUCCESS;
        },
        .IB_CFG_CB => {
            if (@TypeOf(value) == ib_cb_t) {
                cfg_var.value = .{ .IB_CFG_CB = value };
                return .DB_SUCCESS;
            }
            if (@TypeOf(value) == ?ib_cb_t) {
                cfg_var.value = .{ .IB_CFG_CB = value };
                return .DB_SUCCESS;
            }
            return .DB_INVALID_INPUT;
        },
    }
}

fn cfgGetValue(cfg_var: *const CfgVar, out: anytype) ib_err_t {
    const OutType = @TypeOf(out);
    const out_info = @typeInfo(OutType);
    if (out_info != .pointer) {
        return .DB_INVALID_INPUT;
    }
    const Child = out_info.pointer.child;

    switch (cfg_var.cfg_type) {
        .IB_CFG_IBOOL => {
            if (Child == ib_bool_t) {
                out.* = cfg_var.value.IB_CFG_IBOOL;
                return .DB_SUCCESS;
            }
            if (Child == bool) {
                out.* = cfg_var.value.IB_CFG_IBOOL != 0;
                return .DB_SUCCESS;
            }
            return .DB_INVALID_INPUT;
        },
        .IB_CFG_ULINT => {
            if (Child == ib_ulint_t) {
                out.* = cfg_var.value.IB_CFG_ULINT;
                return .DB_SUCCESS;
            }
            return .DB_INVALID_INPUT;
        },
        .IB_CFG_ULONG => {
            if (Child == ib_ulint_t) {
                out.* = cfg_var.value.IB_CFG_ULONG;
                return .DB_SUCCESS;
            }
            return .DB_INVALID_INPUT;
        },
        .IB_CFG_TEXT => {
            if (Child == []const u8) {
                out.* = cfg_var.value.IB_CFG_TEXT;
                return .DB_SUCCESS;
            }
            return .DB_INVALID_INPUT;
        },
        .IB_CFG_CB => {
            if (Child == ?ib_cb_t) {
                out.* = cfg_var.value.IB_CFG_CB;
                return .DB_SUCCESS;
            }
            if (Child == ib_cb_t) {
                if (cfg_var.value.IB_CFG_CB) |cb| {
                    out.* = cb;
                    return .DB_SUCCESS;
                }
                return .DB_NOT_FOUND;
            }
            return .DB_INVALID_INPUT;
        },
    }
}

pub fn ib_cfg_var_get_type(name: []const u8, cfg_type: *ib_cfg_type_t) ib_err_t {
    cfg_mutex.lock();
    defer cfg_mutex.unlock();

    if (!cfg_initialized) {
        return .DB_ERROR;
    }

    const cfg_var = cfgFind(name) orelse return .DB_NOT_FOUND;
    cfg_type.* = cfg_var.cfg_type;
    return .DB_SUCCESS;
}

pub fn ib_cfg_set(name: []const u8, value: anytype) ib_err_t {
    cfg_mutex.lock();
    defer cfg_mutex.unlock();

    if (!cfg_initialized) {
        return .DB_ERROR;
    }

    const cfg_var = cfgFind(name) orelse return .DB_NOT_FOUND;
    return cfgSetValue(cfg_var, value);
}

pub fn ib_cfg_get(name: []const u8, out: anytype) ib_err_t {
    cfg_mutex.lock();
    defer cfg_mutex.unlock();

    if (!cfg_initialized) {
        return .DB_ERROR;
    }

    const cfg_var = cfgFind(name) orelse return .DB_NOT_FOUND;
    return cfgGetValue(cfg_var, out);
}

pub fn ib_cfg_get_all(names: *[][]const u8, names_num: *ib_u32_t) ib_err_t {
    cfg_mutex.lock();
    defer cfg_mutex.unlock();

    if (!cfg_initialized) {
        return .DB_ERROR;
    }

    const out = std.heap.page_allocator.alloc([]const u8, cfg_vars.len) catch {
        return .DB_OUT_OF_MEMORY;
    };

    for (cfg_vars, 0..) |cfg_var, idx| {
        out[idx] = cfg_var.name;
    }

    names.* = out;
    names_num.* = @intCast(cfg_vars.len);
    return .DB_SUCCESS;
}

pub fn ib_cfg_init() ib_err_t {
    cfg_mutex.lock();
    defer cfg_mutex.unlock();
    cfg_vars = cfg_vars_defaults;
    cfg_initialized = true;
    cfg_started = false;
    if (cfgFind("rollback_on_timeout")) |cfg_var| {
        ses_rollback_on_timeout = cfg_var.value.IB_CFG_IBOOL;
    } else {
        ses_rollback_on_timeout = compat.IB_FALSE;
    }
    return .DB_SUCCESS;
}

pub fn ib_cfg_shutdown() ib_err_t {
    cfg_mutex.lock();
    defer cfg_mutex.unlock();
    cfg_vars = cfg_vars_defaults;
    cfg_initialized = false;
    cfg_started = false;
    ses_rollback_on_timeout = compat.IB_FALSE;
    return .DB_SUCCESS;
}

fn srv_export_innodb_status() void {
    export_vars.innodb_page_size = compat.UNIV_PAGE_SIZE;
    export_vars.innodb_have_atomic_builtins = compat.IB_TRUE;
}

fn statusLookup(name: []const u8) ?*const StatusVar {
    for (status_vars[0..]) |*entry| {
        if (schemaNameEq(name, entry.name)) {
            return entry;
        }
    }
    return null;
}

pub fn ib_status_get_i64(name: []const u8, dst: *ib_i64_t) ib_err_t {
    if (name.len == 0) {
        return .DB_NOT_FOUND;
    }

    const entry = statusLookup(name) orelse return .DB_NOT_FOUND;

    srv_export_innodb_status();

    switch (entry.status_type) {
        .IB_STATUS_ULINT => {
            const ptr = @as(*const ib_ulint_t, @ptrCast(@alignCast(entry.value_ptr)));
            dst.* = @as(ib_i64_t, @intCast(ptr.*));
            return .DB_SUCCESS;
        },
        .IB_STATUS_IBOOL => {
            const ptr = @as(*const ib_bool_t, @ptrCast(@alignCast(entry.value_ptr)));
            dst.* = if (ptr.* != 0) 1 else 0;
            return .DB_SUCCESS;
        },
        .IB_STATUS_I64 => {
            const ptr = @as(*const ib_i64_t, @ptrCast(@alignCast(entry.value_ptr)));
            dst.* = ptr.*;
            return .DB_SUCCESS;
        },
    }
}

pub fn ib_create_tempfile(prefix: []const u8) i32 {
    const allocator = std.heap.page_allocator;
    const stamp = std.time.nanoTimestamp();
    const name = std.fmt.allocPrint(allocator, "{s}{d}", .{ prefix, stamp }) catch return -1;
    defer allocator.free(name);

    const file = std.fs.cwd().createFile(name, .{ .read = true, .truncate = true, .exclusive = true }) catch return -1;
    _ = std.fs.cwd().deleteFile(name) catch {};
    return @intCast(file.handle);
}

pub fn trx_is_interrupted(trx: ib_trx_t) ib_bool_t {
    _ = trx;
    return compat.IB_FALSE;
}

pub fn ib_handle_errors(
    new_err: *ib_err_t,
    ib_trx: ib_trx_t,
    ib_thr: ?*anyopaque,
    ib_savept: ?*anyopaque,
) ib_bool_t {
    _ = ib_thr;
    _ = ib_savept;

    if (new_err.* == .DB_LOCK_WAIT) {
        return compat.IB_TRUE;
    }
    if (new_err.* == .DB_LOCK_WAIT_TIMEOUT and ses_rollback_on_timeout == compat.IB_TRUE) {
        if (ib_trx != null) {
            _ = ib_trx_rollback(ib_trx);
        }
    }
    return compat.IB_FALSE;
}

pub fn ib_trx_lock_table_with_retry(ib_trx: ib_trx_t, table: *CatalogTable, mode: ib_lck_mode_t) ib_err_t {
    _ = ib_trx orelse return .DB_ERROR;
    if (mode != .IB_LOCK_S and mode != .IB_LOCK_X) {
        return .DB_ERROR;
    }
    if (table.id == 0) {
        return .DB_TABLE_NOT_FOUND;
    }
    return .DB_SUCCESS;
}

pub fn ib_update_statistics_if_needed(table: *CatalogTable) void {
    const counter = table.stat_modified_counter;
    table.stat_modified_counter += 1;

    const counter_i64 = @as(i64, @intCast(counter));
    const rows_i64 = @as(i64, @intCast(table.stat_n_rows));
    if (counter > 2_000_000_000 or counter_i64 > 16 + @divTrunc(rows_i64, 16)) {
        table.stats_updated = true;
    }
}

pub const ib_sql_arg_t = union(enum) {
    text: struct {
        name: []const u8,
        value: []const u8,
    },
    int: struct {
        name: []const u8,
        value: ib_i64_t,
        unsigned: bool,
        len: u8,
    },
    func: struct {
        name: []const u8,
        callback: ?ib_cb_t,
        arg: ?*anyopaque,
    },
};

fn sqlArgNameValid(name: []const u8) bool {
    if (name.len < 2) {
        return false;
    }
    return name[0] == ':' or name[0] == '$';
}

fn sqlArgsValidate(args: []const ib_sql_arg_t) ib_err_t {
    for (args) |arg| {
        switch (arg) {
            .text => |payload| {
                if (!sqlArgNameValid(payload.name)) {
                    return .DB_INVALID_INPUT;
                }
            },
            .int => |payload| {
                if (!sqlArgNameValid(payload.name)) {
                    return .DB_INVALID_INPUT;
                }
                switch (payload.len) {
                    1, 2, 4, 8 => {},
                    else => return .DB_INVALID_INPUT,
                }
            },
            .func => |payload| {
                if (!sqlArgNameValid(payload.name)) {
                    return .DB_INVALID_INPUT;
                }
                if (payload.callback == null) {
                    return .DB_INVALID_INPUT;
                }
            },
        }
    }
    return .DB_SUCCESS;
}

fn execSqlCommon(sql: []const u8, args: []const ib_sql_arg_t, ddl: bool) ib_err_t {
    if (sql.len == 0) {
        return .DB_INVALID_INPUT;
    }
    const arg_err = sqlArgsValidate(args);
    if (arg_err != .DB_SUCCESS) {
        return arg_err;
    }

    const trx = ib_trx_begin(.IB_TRX_READ_COMMITTED) orelse return .DB_OUT_OF_MEMORY;
    if (ddl) {
        const lock_err = ib_schema_lock_exclusive(trx);
        if (lock_err != .DB_SUCCESS) {
            _ = ib_trx_rollback(trx);
            return lock_err;
        }

        const unlock_err = ib_schema_unlock(trx);
        if (unlock_err != .DB_SUCCESS) {
            _ = ib_trx_rollback(trx);
            return unlock_err;
        }
    }

    return ib_trx_commit(trx);
}

pub fn ib_exec_sql(sql: []const u8, n_args: ib_ulint_t) ib_err_t {
    if (n_args != 0) {
        return .DB_INVALID_INPUT;
    }
    return execSqlCommon(sql, &.{}, false);
}

pub fn ib_exec_sql_args(sql: []const u8, args: []const ib_sql_arg_t) ib_err_t {
    return execSqlCommon(sql, args, false);
}

pub fn ib_exec_ddl_sql(sql: []const u8, n_args: ib_ulint_t) ib_err_t {
    if (n_args != 0) {
        return .DB_INVALID_INPUT;
    }
    return execSqlCommon(sql, &.{}, true);
}

pub fn ib_exec_ddl_sql_args(sql: []const u8, args: []const ib_sql_arg_t) ib_err_t {
    return execSqlCommon(sql, args, true);
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

fn tupleClone(allocator: std.mem.Allocator, src: *const Tuple) !*Tuple {
    const tuple = try allocator.create(Tuple);
    const cols = try allocator.alloc(Column, src.cols.len);
    errdefer {
        for (cols) |col| {
            if (col.data) |buf| {
                allocator.free(buf);
            }
        }
        allocator.free(cols);
        allocator.destroy(tuple);
    }

    for (src.cols, 0..) |src_col, i| {
        var data_copy: ?[]u8 = null;
        if (src_col.data) |data| {
            const buf = try allocator.alloc(u8, data.len);
            std.mem.copyForwards(u8, buf, data);
            data_copy = buf;
        }
        cols[i] = .{
            .meta = src_col.meta,
            .data = data_copy,
        };
    }

    tuple.* = .{
        .tuple_type = src.tuple_type,
        .cols = cols,
        .allocator = allocator,
    };

    return tuple;
}

fn tupleColumn(tuple: *Tuple, col_no: ib_ulint_t) ?*Column {
    if (col_no >= tuple.cols.len) {
        return null;
    }
    return &tuple.cols[@intCast(col_no)];
}

fn columnIntValue(col: *const Column) ?i64 {
    const data = col.data orelse return null;
    const attr_val = @intFromEnum(col.meta.attr);
    const is_unsigned = (attr_val & @intFromEnum(ib_col_attr_t.IB_COL_UNSIGNED)) != 0;

    switch (data.len) {
        1 => {
            const slice = data[0..1];
            if (is_unsigned) {
                const v = std.mem.bytesToValue(u8, slice);
                return @as(i64, @intCast(v));
            }
            const v = std.mem.bytesToValue(i8, slice);
            return @as(i64, v);
        },
        2 => {
            const slice = data[0..2];
            if (is_unsigned) {
                const v = std.mem.bytesToValue(u16, slice);
                return @as(i64, @intCast(v));
            }
            const v = std.mem.bytesToValue(i16, slice);
            return @as(i64, v);
        },
        4 => {
            const slice = data[0..4];
            if (is_unsigned) {
                const v = std.mem.bytesToValue(u32, slice);
                return @as(i64, @intCast(v));
            }
            const v = std.mem.bytesToValue(i32, slice);
            return @as(i64, v);
        },
        8 => {
            const slice = data[0..8];
            if (is_unsigned) {
                const v = std.mem.bytesToValue(u64, slice);
                return @as(i64, @intCast(v));
            }
            const v = std.mem.bytesToValue(i64, slice);
            return @as(i64, v);
        },
        else => return null,
    }
}

fn columnCompare(a: *const Column, b: *const Column) i32 {
    if (a.meta.type == .IB_INT and b.meta.type == .IB_INT) {
        if (columnIntValue(a)) |av| {
            if (columnIntValue(b)) |bv| {
                if (av < bv) {
                    return -1;
                }
                if (av > bv) {
                    return 1;
                }
                return 0;
            }
        }
    }

    const a_data = a.data orelse return if (b.data == null) 0 else -1;
    const b_data = b.data orelse return 1;
    return ib_client_compare(&a.meta, a_data.ptr, @intCast(a_data.len), b_data.ptr, @intCast(b_data.len));
}

fn tupleKeyCompare(a: *Tuple, b: *Tuple) i32 {
    const col_a = tupleColumn(a, 0) orelse return 0;
    const col_b = tupleColumn(b, 0) orelse return 0;
    return columnCompare(col_a, col_b);
}

fn cursorCurrentRow(cursor: *Cursor) ?*Tuple {
    const pos = cursor.position orelse return null;
    if (pos >= cursor.rows.items.len) {
        return null;
    }
    return cursor.rows.items[pos];
}

fn cursorInsertIndex(cursor: *Cursor, row: *Tuple) usize {
    var i: usize = 0;
    while (i < cursor.rows.items.len) : (i += 1) {
        if (tupleKeyCompare(row, cursor.rows.items[i]) < 0) {
            return i;
        }
    }
    return cursor.rows.items.len;
}

fn cursorFindRow(cursor: *Cursor, key: *Tuple, mode: ib_srch_mode_t) ?usize {
    if (cursor.rows.items.len == 0) {
        return null;
    }

    switch (mode) {
        .IB_CUR_GE => {
            for (cursor.rows.items, 0..) |row, idx| {
                if (tupleKeyCompare(key, row) <= 0) {
                    return idx;
                }
            }
        },
        .IB_CUR_G => {
            for (cursor.rows.items, 0..) |row, idx| {
                if (tupleKeyCompare(key, row) < 0) {
                    return idx;
                }
            }
        },
        .IB_CUR_LE => {
            var idx: usize = cursor.rows.items.len;
            while (idx > 0) {
                idx -= 1;
                const row = cursor.rows.items[idx];
                if (tupleKeyCompare(key, row) >= 0) {
                    return idx;
                }
            }
        },
        .IB_CUR_L => {
            var idx: usize = cursor.rows.items.len;
            while (idx > 0) {
                idx -= 1;
                const row = cursor.rows.items[idx];
                if (tupleKeyCompare(key, row) > 0) {
                    return idx;
                }
            }
        },
    }

    return null;
}

fn catalogColumnMeta(col: CatalogColumn) ib_col_meta_t {
    return .{
        .type = col.col_type,
        .attr = col.attr,
        .type_len = @intCast(col.len),
        .client_type = 0,
        .charset = null,
    };
}

fn tupleCreateFromCatalogColumns(table: *CatalogTable, tuple_type: ib_tuple_type_t) ?*Tuple {
    const allocator = std.heap.page_allocator;
    if (table.columns.items.len == 0) {
        return null;
    }
    const metas = allocator.alloc(ib_col_meta_t, table.columns.items.len) catch return null;
    defer allocator.free(metas);

    for (table.columns.items, 0..) |col, idx| {
        metas[idx] = catalogColumnMeta(col);
    }

    return tupleCreate(allocator, tuple_type, metas) catch null;
}

fn tupleCreateFromCatalogIndex(
    table: *CatalogTable,
    index: *CatalogIndex,
    tuple_type: ib_tuple_type_t,
) ?*Tuple {
    const allocator = std.heap.page_allocator;
    if (index.columns.items.len == 0) {
        return null;
    }
    const metas = allocator.alloc(ib_col_meta_t, index.columns.items.len) catch return null;
    defer allocator.free(metas);

    for (index.columns.items, 0..) |icol, idx| {
        if (catalogFindColumnByName(table, icol.name)) |col| {
            metas[idx] = catalogColumnMeta(col.*);
        } else {
            metas[idx] = .{
                .type = .IB_INT,
                .attr = .IB_COL_NONE,
                .type_len = 0,
                .client_type = 0,
                .charset = null,
            };
        }
    }

    return tupleCreate(allocator, tuple_type, metas) catch null;
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

pub fn ib_tuple_get_cluster_key(ib_crsr: ib_crsr_t, ib_dst_tpl: *ib_tpl_t, ib_src_tpl: ib_tpl_t) ib_err_t {
    const cursor = ib_crsr orelse return .DB_ERROR;
    const src = ib_src_tpl orelse return .DB_ERROR;
    if (src.tuple_type != .TPL_KEY) {
        return .DB_ERROR;
    }

    const dst_tuple = ib_clust_search_tuple_create(cursor) orelse return .DB_OUT_OF_MEMORY;
    ib_dst_tpl.* = dst_tuple;

    const dst = dst_tuple;
    const n = @min(src.cols.len, dst.cols.len);
    for (src.cols[0..n], 0..) |src_col, idx| {
        const dst_col = &dst.cols[idx];
        if (src_col.data) |data| {
            const buf = dst.allocator.alloc(u8, data.len) catch return .DB_OUT_OF_MEMORY;
            std.mem.copyForwards(u8, buf, data);
            dst_col.data = buf;
        } else {
            dst_col.data = null;
        }
    }

    return .DB_SUCCESS;
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

pub fn ib_sec_search_tuple_create(ib_crsr: ib_crsr_t) ib_tpl_t {
    const cursor = ib_crsr orelse return null;
    const table = cursorCatalogTable(cursor) orelse return null;
    if (cursorCatalogIndex(cursor)) |index| {
        return tupleCreateFromCatalogIndex(table, index, .TPL_KEY);
    }
    return tupleCreateFromCatalogColumns(table, .TPL_KEY);
}

pub fn ib_sec_read_tuple_create(ib_crsr: ib_crsr_t) ib_tpl_t {
    const cursor = ib_crsr orelse return null;
    const table = cursorCatalogTable(cursor) orelse return null;
    if (cursorCatalogIndex(cursor)) |index| {
        return tupleCreateFromCatalogIndex(table, index, .TPL_ROW);
    }
    return tupleCreateFromCatalogColumns(table, .TPL_ROW);
}

pub fn ib_clust_search_tuple_create(ib_crsr: ib_crsr_t) ib_tpl_t {
    const cursor = ib_crsr orelse return null;
    const table = cursorCatalogTable(cursor) orelse return null;
    const clustered = catalogFindIndexByName(table, "PRIMARY") orelse {
        for (table.indexes.items) |*idx| {
            if (idx.clustered) {
                return tupleCreateFromCatalogIndex(table, idx, .TPL_KEY);
            }
        }
        return tupleCreateFromCatalogColumns(table, .TPL_KEY);
    };
    return tupleCreateFromCatalogIndex(table, clustered, .TPL_KEY);
}

pub fn ib_clust_read_tuple_create(ib_crsr: ib_crsr_t) ib_tpl_t {
    const cursor = ib_crsr orelse return null;
    const table = cursorCatalogTable(cursor) orelse return null;
    return tupleCreateFromCatalogColumns(table, .TPL_ROW);
}

pub fn ib_tuple_delete(ib_tpl: ib_tpl_t) void {
    const tuple = ib_tpl orelse return;
    tupleDestroy(tuple);
}

fn savepointsClear(trx: *Trx) void {
    for (trx.savepoints.items) |sp| {
        std.heap.page_allocator.free(sp.name);
    }
    trx.savepoints.clearAndFree();
}

fn savepointNameSlice(name: ?*const anyopaque, name_len: ib_ulint_t) ?[]const u8 {
    const ptr = name orelse return null;
    if (name_len == 0) {
        return null;
    }
    const len: usize = @intCast(name_len);
    return @as([*]const u8, @ptrCast(ptr))[0..len];
}

fn savepointFindIndex(trx: *Trx, name: []const u8) ?usize {
    for (trx.savepoints.items, 0..) |sp, idx| {
        if (std.mem.eql(u8, sp.name, name)) {
            return idx;
        }
    }
    return null;
}

fn savepointRemoveAt(trx: *Trx, index: usize) void {
    std.heap.page_allocator.free(trx.savepoints.items[index].name);
    _ = trx.savepoints.orderedRemove(index);
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
        savepointsClear(trx);
        trx.state = .IB_TRX_ACTIVE;
        trx.isolation_level = ib_trx_level;
        trx.schema_lock = .none;
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
        .schema_lock = .none,
        .savepoints = ArrayList(Savepoint).init(std.heap.page_allocator),
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
    savepointsClear(trx);
    std.heap.page_allocator.destroy(trx);
    return .DB_SUCCESS;
}

pub fn ib_trx_commit(ib_trx: ib_trx_t) ib_err_t {
    const trx = ib_trx orelse return .DB_ERROR;
    trx.schema_lock = .none;
    trx.state = .IB_TRX_COMMITTED_IN_MEMORY;
    return ib_trx_release(trx);
}

pub fn ib_trx_rollback(ib_trx: ib_trx_t) ib_err_t {
    const trx = ib_trx orelse return .DB_ERROR;
    trx.schema_lock = .none;
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
        .rows = ArrayList(*Tuple).init(allocator),
        .position = null,
        .sql_stat_start = false,
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
    for (cursor.rows.items) |row| {
        tupleDestroy(row);
    }
    cursor.rows.deinit();
    cursor.allocator.destroy(cursor);
}

fn cursorCatalogTable(cursor: *Cursor) ?*CatalogTable {
    if (cursor.table_id) |tid| {
        return catalogFindById(tid);
    }
    if (cursor.table_name) |name| {
        return catalogFindByName(name);
    }
    return null;
}

fn cursorCatalogIndex(cursor: *Cursor) ?*CatalogIndex {
    const table = cursorCatalogTable(cursor) orelse return null;
    if (cursor.index_id) |iid| {
        return catalogFindIndexById(table, iid);
    }
    if (cursor.index_name) |name| {
        return catalogFindIndexByName(table, name);
    }
    return null;
}

fn cursorLockModeValid(mode: ib_lck_mode_t) bool {
    return @intFromEnum(mode) <= IB_LOCK_NUM;
}

fn tableLockModeValid(mode: ib_lck_mode_t) bool {
    return mode == .IB_LOCK_IS or mode == .IB_LOCK_IX;
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
    cursor.position = null;
    cursor.cluster_access = false;
    cursor.simple_select = false;
    cursor.sql_stat_start = false;
    return .DB_SUCCESS;
}

pub fn ib_cursor_close(ib_crsr: ib_crsr_t) ib_err_t {
    const cursor = ib_crsr orelse return .DB_ERROR;
    cursorDestroy(cursor);
    return .DB_SUCCESS;
}

pub fn ib_cursor_read_row(ib_crsr: ib_crsr_t, ib_tpl: ib_tpl_t) ib_err_t {
    const cursor = ib_crsr orelse return .DB_ERROR;
    const tuple = ib_tpl orelse return .DB_ERROR;
    if (!cursor.positioned) {
        return .DB_RECORD_NOT_FOUND;
    }
    const row = cursorCurrentRow(cursor) orelse return .DB_RECORD_NOT_FOUND;
    return ib_tuple_copy(tuple, row);
}

pub fn ib_cursor_insert_row(ib_crsr: ib_crsr_t, ib_tpl: ib_tpl_t) ib_err_t {
    const cursor = ib_crsr orelse return .DB_ERROR;
    const tuple = ib_tpl orelse return .DB_ERROR;
    if (tuple.tuple_type != .TPL_ROW) {
        return .DB_DATA_MISMATCH;
    }

    const clone = tupleClone(cursor.allocator, tuple) catch return .DB_OUT_OF_MEMORY;
    const insert_idx = cursorInsertIndex(cursor, clone);
    cursor.rows.insert(insert_idx, clone) catch return .DB_OUT_OF_MEMORY;
    cursor.position = insert_idx;
    cursor.positioned = true;
    return .DB_SUCCESS;
}

pub fn ib_cursor_update_row(ib_crsr: ib_crsr_t, ib_old_tpl: ib_tpl_t, ib_new_tpl: ib_tpl_t) ib_err_t {
    const cursor = ib_crsr orelse return .DB_ERROR;
    const old_tuple = ib_old_tpl orelse return .DB_ERROR;
    const new_tuple = ib_new_tpl orelse return .DB_ERROR;
    if (old_tuple.tuple_type != .TPL_ROW or new_tuple.tuple_type != .TPL_ROW) {
        return .DB_DATA_MISMATCH;
    }
    const pos = cursor.position orelse return .DB_RECORD_NOT_FOUND;
    if (pos >= cursor.rows.items.len) {
        return .DB_RECORD_NOT_FOUND;
    }

    const clone = tupleClone(cursor.allocator, new_tuple) catch return .DB_OUT_OF_MEMORY;
    tupleDestroy(cursor.rows.items[pos]);
    cursor.rows.items[pos] = clone;
    cursor.positioned = true;
    return .DB_SUCCESS;
}

pub fn ib_cursor_delete_row(ib_crsr: ib_crsr_t) ib_err_t {
    const cursor = ib_crsr orelse return .DB_ERROR;
    const pos = cursor.position orelse return .DB_RECORD_NOT_FOUND;
    if (pos >= cursor.rows.items.len) {
        return .DB_RECORD_NOT_FOUND;
    }
    const removed = cursor.rows.orderedRemove(pos);
    tupleDestroy(removed);
    if (cursor.rows.items.len == 0) {
        cursor.position = null;
        cursor.positioned = false;
    } else {
        const next_pos = if (pos >= cursor.rows.items.len) cursor.rows.items.len - 1 else pos;
        cursor.position = next_pos;
        cursor.positioned = true;
    }
    return .DB_SUCCESS;
}

pub fn ib_cursor_prev(ib_crsr: ib_crsr_t) ib_err_t {
    const cursor = ib_crsr orelse return .DB_ERROR;
    if (!cursor.positioned) {
        return .DB_RECORD_NOT_FOUND;
    }
    const pos = cursor.position orelse return .DB_RECORD_NOT_FOUND;
    if (pos == 0) {
        cursor.position = null;
        cursor.positioned = false;
        return .DB_END_OF_INDEX;
    }
    cursor.position = pos - 1;
    cursor.positioned = true;
    return .DB_SUCCESS;
}

pub fn ib_cursor_next(ib_crsr: ib_crsr_t) ib_err_t {
    const cursor = ib_crsr orelse return .DB_ERROR;
    if (!cursor.positioned) {
        return .DB_RECORD_NOT_FOUND;
    }
    const pos = cursor.position orelse return .DB_RECORD_NOT_FOUND;
    if (pos + 1 >= cursor.rows.items.len) {
        cursor.position = null;
        cursor.positioned = false;
        return .DB_END_OF_INDEX;
    }
    cursor.position = pos + 1;
    cursor.positioned = true;
    return .DB_SUCCESS;
}

pub fn ib_cursor_first(ib_crsr: ib_crsr_t) ib_err_t {
    const cursor = ib_crsr orelse return .DB_ERROR;
    if (cursor.rows.items.len == 0) {
        cursor.position = null;
        cursor.positioned = false;
        return .DB_RECORD_NOT_FOUND;
    }
    cursor.position = 0;
    cursor.positioned = true;
    return .DB_SUCCESS;
}

pub fn ib_cursor_last(ib_crsr: ib_crsr_t) ib_err_t {
    const cursor = ib_crsr orelse return .DB_ERROR;
    if (cursor.rows.items.len == 0) {
        cursor.position = null;
        cursor.positioned = false;
        return .DB_RECORD_NOT_FOUND;
    }
    cursor.position = cursor.rows.items.len - 1;
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
    const tuple = ib_tpl orelse return .DB_ERROR;
    if (tuple.tuple_type != .TPL_KEY) {
        return .DB_DATA_MISMATCH;
    }
    const pos = cursorFindRow(cursor, tuple, ib_srch_mode) orelse {
        cursor.position = null;
        cursor.positioned = false;
        result.* = -1;
        return .DB_RECORD_NOT_FOUND;
    };
    cursor.position = pos;
    cursor.positioned = true;
    const row = cursor.rows.items[pos];
    result.* = tupleKeyCompare(tuple, row);
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

pub fn ib_cursor_stmt_begin(ib_crsr: ib_crsr_t) void {
    const cursor = ib_crsr orelse return;
    cursor.sql_stat_start = true;
}

pub fn ib_cursor_truncate(ib_crsr: *ib_crsr_t, table_id: *ib_id_t) ib_err_t {
    table_id.* = 0;
    const cursor = ib_crsr.* orelse return .DB_ERROR;
    if (ib_schema_lock_is_exclusive(cursor.trx) != compat.IB_TRUE) {
        return .DB_SCHEMA_NOT_LOCKED;
    }

    const table = cursorCatalogTable(cursor) orelse return .DB_TABLE_NOT_FOUND;
    const lock_err = ib_cursor_lock(cursor, .IB_LOCK_X);
    if (lock_err != .DB_SUCCESS) {
        return lock_err;
    }

    table_id.* = catalogTruncateTable(table);
    _ = ib_cursor_close(cursor);
    ib_crsr.* = null;
    return .DB_SUCCESS;
}

pub fn ib_table_lock(ib_trx: ib_trx_t, table_id: ib_id_t, ib_lck_mode: ib_lck_mode_t) ib_err_t {
    _ = ib_trx orelse return .DB_ERROR;
    if (!tableLockModeValid(ib_lck_mode)) {
        return .DB_ERROR;
    }
    if (table_id == 0) {
        return .DB_TABLE_NOT_FOUND;
    }
    for (table_registry.items) |table| {
        if (table.id == table_id) {
            return .DB_SUCCESS;
        }
    }
    return .DB_TABLE_NOT_FOUND;
}

pub fn ib_savepoint_take(ib_trx: ib_trx_t, name: ?*const anyopaque, name_len: ib_ulint_t) void {
    const trx = ib_trx orelse return;
    const slice = savepointNameSlice(name, name_len) orelse return;
    if (trx.state == .IB_TRX_NOT_STARTED) {
        return;
    }

    if (savepointFindIndex(trx, slice)) |idx| {
        savepointRemoveAt(trx, idx);
    }

    const copy = std.heap.page_allocator.alloc(u8, slice.len) catch return;
    std.mem.copyForwards(u8, copy, slice);
    trx.savepoints.append(.{ .name = copy }) catch {
        std.heap.page_allocator.free(copy);
    };
}

pub fn ib_savepoint_release(ib_trx: ib_trx_t, name: ?*const anyopaque, name_len: ib_ulint_t) ib_err_t {
    const trx = ib_trx orelse return .DB_ERROR;
    const slice = savepointNameSlice(name, name_len) orelse return .DB_NO_SAVEPOINT;

    if (savepointFindIndex(trx, slice)) |idx| {
        savepointRemoveAt(trx, idx);
        return .DB_SUCCESS;
    }

    return .DB_NO_SAVEPOINT;
}

pub fn ib_savepoint_rollback(ib_trx: ib_trx_t, name: ?*const anyopaque, name_len: ib_ulint_t) ib_err_t {
    const trx = ib_trx orelse return .DB_ERROR;
    if (trx.state == .IB_TRX_NOT_STARTED) {
        return .DB_ERROR;
    }

    const slice = savepointNameSlice(name, name_len);
    if (slice == null) {
        if (trx.savepoints.items.len == 0) {
            return .DB_NO_SAVEPOINT;
        }
        savepointsClear(trx);
        return .DB_SUCCESS;
    }

    const idx = savepointFindIndex(trx, slice.?) orelse return .DB_NO_SAVEPOINT;
    while (trx.savepoints.items.len > idx + 1) {
        savepointRemoveAt(trx, trx.savepoints.items.len - 1);
    }

    return .DB_SUCCESS;
}

fn schemaNameEq(a: []const u8, b: []const u8) bool {
    return std.ascii.eqlIgnoreCase(a, b);
}

fn schemaNameHasPrefix(name: []const u8, prefix: []const u8) bool {
    if (name.len < prefix.len) {
        return false;
    }
    return std.ascii.eqlIgnoreCase(name[0..prefix.len], prefix);
}

fn tableSchemaFindColumn(schema: *const TableSchema, name: []const u8) bool {
    for (schema.columns.items) |col| {
        if (schemaNameEq(col.name, name)) {
            return true;
        }
    }
    return false;
}

fn tableSchemaFindIndex(schema: *const TableSchema, name: []const u8) ?*IndexSchema {
    for (schema.indexes.items) |idx| {
        if (schemaNameEq(idx.name, name)) {
            return idx;
        }
    }
    return null;
}

fn indexSchemaHasColumn(index: *const IndexSchema, name: []const u8) bool {
    for (index.columns.items) |col| {
        if (schemaNameEq(col.name, name)) {
            return true;
        }
    }
    return false;
}

fn indexSchemaDestroy(index: *IndexSchema) void {
    for (index.columns.items) |col| {
        index.allocator.free(col.name);
    }
    index.columns.deinit();
    index.allocator.free(index.name);
    index.allocator.free(index.table_name);
    index.allocator.destroy(index);
}

fn tableSchemaDestroy(schema: *TableSchema) void {
    for (schema.columns.items) |col| {
        schema.allocator.free(col.name);
    }
    schema.columns.deinit();

    for (schema.indexes.items) |idx| {
        indexSchemaDestroy(idx);
    }
    schema.indexes.deinit();

    schema.allocator.free(schema.name);
    schema.allocator.destroy(schema);
}

fn catalogIndexDestroy(index: *CatalogIndex, allocator: std.mem.Allocator) void {
    for (index.columns.items) |col| {
        allocator.free(col.name);
    }
    index.columns.deinit();
    allocator.free(index.name);
}

fn catalogDestroy(table: *CatalogTable) void {
    for (table.columns.items) |col| {
        table.allocator.free(col.name);
    }
    table.columns.deinit();

    for (table.indexes.items) |*idx| {
        catalogIndexDestroy(idx, table.allocator);
    }
    table.indexes.deinit();

    table.allocator.free(table.name);
    table.allocator.destroy(table);
}

fn catalogClear() void {
    for (table_registry.items) |table| {
        catalogDestroy(table);
    }
    table_registry.clearAndFree();
}

fn catalogFindByName(name: []const u8) ?*CatalogTable {
    for (table_registry.items) |table| {
        if (schemaNameEq(table.name, name)) {
            return table;
        }
    }
    return null;
}

fn catalogFindById(id: ib_id_t) ?*CatalogTable {
    for (table_registry.items) |table| {
        if (table.id == id) {
            return table;
        }
    }
    return null;
}

fn catalogFindColumnByName(table: *CatalogTable, name: []const u8) ?*CatalogColumn {
    for (table.columns.items) |*col| {
        if (schemaNameEq(col.name, name)) {
            return col;
        }
    }
    return null;
}

fn catalogFindIndexByName(table: *CatalogTable, name: []const u8) ?*CatalogIndex {
    for (table.indexes.items) |*idx| {
        if (schemaNameEq(idx.name, name)) {
            return idx;
        }
    }
    return null;
}

fn catalogFindIndexById(table: *CatalogTable, id: ib_id_t) ?*CatalogIndex {
    for (table.indexes.items) |*idx| {
        if (idx.id == id) {
            return idx;
        }
    }
    return null;
}

fn catalogTruncateTable(table: *CatalogTable) ib_id_t {
    const new_id = next_table_id;
    next_table_id += 1;
    table.id = new_id;

    for (table.indexes.items) |*idx| {
        const low = idx.id & 0xFFFF_FFFF;
        idx.id = (new_id << 32) | low;
    }

    return new_id;
}

fn catalogRemoveByName(name: []const u8) bool {
    for (table_registry.items, 0..) |table, idx| {
        if (schemaNameEq(table.name, name)) {
            catalogDestroy(table);
            _ = table_registry.orderedRemove(idx);
            return true;
        }
    }
    return false;
}

fn catalogCreateFromSchema(schema: *const TableSchema, id: ib_id_t) !*CatalogTable {
    const allocator = std.heap.page_allocator;
    const table = try allocator.create(CatalogTable);
    const name_copy = try dupName(allocator, schema.name);

    table.* = .{
        .allocator = allocator,
        .name = name_copy,
        .format = schema.format,
        .page_size = schema.page_size,
        .id = id,
        .stat_modified_counter = 0,
        .stat_n_rows = 0,
        .stats_updated = false,
        .columns = ArrayList(CatalogColumn).init(allocator),
        .indexes = ArrayList(CatalogIndex).init(allocator),
    };

    errdefer catalogDestroy(table);

    for (schema.columns.items) |col| {
        const col_name = try dupName(allocator, col.name);
        table.columns.append(.{
            .name = col_name,
            .col_type = col.col_type,
            .attr = col.attr,
            .len = col.len,
        }) catch {
            allocator.free(col_name);
            return error.OutOfMemory;
        };
    }

    for (schema.indexes.items) |idx| {
        var catalog_index = CatalogIndex{
            .name = try dupName(allocator, idx.name),
            .clustered = idx.clustered,
            .unique = idx.unique,
            .id = 0,
            .columns = ArrayList(CatalogIndexColumn).init(allocator),
        };

        var appended = false;
        defer {
            if (!appended) {
                catalogIndexDestroy(&catalog_index, allocator);
            }
        }

        for (idx.columns.items) |icol| {
            const icol_name = try dupName(allocator, icol.name);
            catalog_index.columns.append(.{
                .name = icol_name,
                .prefix_len = icol.prefix_len,
            }) catch {
                allocator.free(icol_name);
                return error.OutOfMemory;
            };
        }

        try table.indexes.append(catalog_index);
        appended = true;
    }

    return table;
}

fn catalogAppendIndex(table: *CatalogTable, index: *const IndexSchema, id: ib_id_t) !void {
    const allocator = table.allocator;
    var catalog_index = CatalogIndex{
        .name = try dupName(allocator, index.name),
        .clustered = index.clustered,
        .unique = index.unique,
        .id = id,
        .columns = ArrayList(CatalogIndexColumn).init(allocator),
    };

    var appended = false;
    defer {
        if (!appended) {
            catalogIndexDestroy(&catalog_index, allocator);
        }
    }

    for (index.columns.items) |icol| {
        const icol_name = try dupName(allocator, icol.name);
        catalog_index.columns.append(.{
            .name = icol_name,
            .prefix_len = icol.prefix_len,
        }) catch {
            allocator.free(icol_name);
            return error.OutOfMemory;
        };
    }

    try table.indexes.append(catalog_index);
    appended = true;
}

pub fn ib_schema_lock_shared(ib_trx: ib_trx_t) ib_err_t {
    const trx = ib_trx orelse return .DB_ERROR;
    switch (trx.schema_lock) {
        .none, .shared => trx.schema_lock = .shared,
        .exclusive => {},
    }
    return .DB_SUCCESS;
}

pub fn ib_schema_lock_exclusive(ib_trx: ib_trx_t) ib_err_t {
    const trx = ib_trx orelse return .DB_ERROR;
    switch (trx.schema_lock) {
        .none, .exclusive => {
            trx.schema_lock = .exclusive;
            return .DB_SUCCESS;
        },
        .shared => return .DB_SCHEMA_NOT_LOCKED,
    }
}

pub fn ib_schema_lock_is_exclusive(ib_trx: ib_trx_t) ib_bool_t {
    const trx = ib_trx orelse return compat.IB_FALSE;
    return if (trx.schema_lock == .exclusive) compat.IB_TRUE else compat.IB_FALSE;
}

pub fn ib_schema_lock_is_shared(ib_trx: ib_trx_t) ib_bool_t {
    const trx = ib_trx orelse return compat.IB_FALSE;
    return if (trx.schema_lock == .shared) compat.IB_TRUE else compat.IB_FALSE;
}

pub fn ib_schema_unlock(ib_trx: ib_trx_t) ib_err_t {
    const trx = ib_trx orelse return .DB_ERROR;
    if (trx.schema_lock == .none) {
        return .DB_SCHEMA_NOT_LOCKED;
    }
    trx.schema_lock = .none;
    return .DB_SUCCESS;
}

fn cfgFilePerTableEnabled() bool {
    cfg_mutex.lock();
    defer cfg_mutex.unlock();

    if (cfgFind("file_per_table")) |cfg_var| {
        return cfg_var.value.IB_CFG_IBOOL != 0;
    }
    return true;
}

fn ibTableSchemaCheck(ib_tbl_fmt: ib_tbl_fmt_t, page_size: *ib_ulint_t) ib_err_t {
    if (!build_options.enable_compression and ib_tbl_fmt == .IB_TBL_COMPRESSED) {
        return .DB_UNSUPPORTED;
    }

    if (ib_tbl_fmt != .IB_TBL_COMPRESSED) {
        page_size.* = 0;
        return .DB_SUCCESS;
    }

    if (page_size.* == 0) {
        page_size.* = 8;
    }

    switch (page_size.*) {
        1, 2, 4, 8, 16 => {},
        else => return .DB_UNSUPPORTED,
    }

    if (!cfgFilePerTableEnabled()) {
        return .DB_UNSUPPORTED;
    }
    if (db_format.id < dict_tf_format_zip) {
        return .DB_UNSUPPORTED;
    }

    return .DB_SUCCESS;
}

pub fn ib_table_schema_create(
    name: []const u8,
    ib_tbl_sch: *ib_tbl_sch_t,
    ib_tbl_fmt: ib_tbl_fmt_t,
    page_size: ib_ulint_t,
) ib_err_t {
    if (name.len == 0 or name.len > IB_MAX_TABLE_NAME_LEN) {
        ib_tbl_sch.* = null;
        return .DB_INVALID_INPUT;
    }
    if (@intFromEnum(ib_tbl_fmt) > @intFromEnum(ib_tbl_fmt_t.IB_TBL_COMPRESSED)) {
        ib_tbl_sch.* = null;
        return .DB_INVALID_INPUT;
    }

    var final_page_size = page_size;
    const schema_err = ibTableSchemaCheck(ib_tbl_fmt, &final_page_size);
    if (schema_err != .DB_SUCCESS) {
        ib_tbl_sch.* = null;
        return schema_err;
    }

    const allocator = std.heap.page_allocator;
    const schema = allocator.create(TableSchema) catch {
        ib_tbl_sch.* = null;
        return .DB_OUT_OF_MEMORY;
    };

    const name_copy = dupName(allocator, name) catch {
        allocator.destroy(schema);
        ib_tbl_sch.* = null;
        return .DB_OUT_OF_MEMORY;
    };

    schema.* = .{
        .allocator = allocator,
        .name = name_copy,
        .format = ib_tbl_fmt,
        .page_size = final_page_size,
        .columns = ArrayList(SchemaColumn).init(allocator),
        .indexes = ArrayList(*IndexSchema).init(allocator),
        .table_id = null,
    };

    ib_tbl_sch.* = schema;
    return .DB_SUCCESS;
}

pub fn ib_table_schema_add_col(
    ib_tbl_sch: ib_tbl_sch_t,
    name: []const u8,
    ib_col_type: ib_col_type_t,
    ib_col_attr: ib_col_attr_t,
    client_type: ib_u16_t,
    len: ib_ulint_t,
) ib_err_t {
    const schema = ib_tbl_sch orelse return .DB_ERROR;
    if (schema.table_id != null) {
        return .DB_ERROR;
    }
    if (name.len == 0 or name.len > IB_MAX_COL_NAME_LEN) {
        return .DB_INVALID_INPUT;
    }
    if (tableSchemaFindColumn(schema, name)) {
        return .DB_DUPLICATE_KEY;
    }

    const name_copy = dupName(schema.allocator, name) catch return .DB_OUT_OF_MEMORY;
    schema.columns.append(.{
        .name = name_copy,
        .col_type = ib_col_type,
        .attr = ib_col_attr,
        .client_type = client_type,
        .len = len,
    }) catch {
        schema.allocator.free(name_copy);
        return .DB_OUT_OF_MEMORY;
    };

    return .DB_SUCCESS;
}

pub fn ib_table_schema_add_index(
    ib_tbl_sch: ib_tbl_sch_t,
    name: []const u8,
    ib_idx_sch: *ib_idx_sch_t,
) ib_err_t {
    const schema = ib_tbl_sch orelse return .DB_ERROR;
    if (schema.table_id != null) {
        ib_idx_sch.* = null;
        return .DB_ERROR;
    }
    if (name.len == 0 or schemaNameEq(name, "GEN_CLUST_INDEX")) {
        ib_idx_sch.* = null;
        return .DB_INVALID_INPUT;
    }
    if (tableSchemaFindIndex(schema, name) != null) {
        ib_idx_sch.* = null;
        return .DB_DUPLICATE_KEY;
    }

    const allocator = schema.allocator;
    const index = allocator.create(IndexSchema) catch {
        ib_idx_sch.* = null;
        return .DB_OUT_OF_MEMORY;
    };

    const name_copy = dupName(allocator, name) catch {
        allocator.destroy(index);
        ib_idx_sch.* = null;
        return .DB_OUT_OF_MEMORY;
    };
    const table_name_copy = dupName(allocator, schema.name) catch {
        allocator.free(name_copy);
        allocator.destroy(index);
        ib_idx_sch.* = null;
        return .DB_OUT_OF_MEMORY;
    };

    index.* = .{
        .allocator = allocator,
        .name = name_copy,
        .table_name = table_name_copy,
        .columns = ArrayList(IndexColumn).init(allocator),
        .clustered = false,
        .unique = false,
        .schema_owner = schema,
        .trx = null,
    };

    schema.indexes.append(index) catch {
        indexSchemaDestroy(index);
        ib_idx_sch.* = null;
        return .DB_OUT_OF_MEMORY;
    };

    ib_idx_sch.* = index;
    return .DB_SUCCESS;
}

pub fn ib_table_schema_delete(ib_tbl_sch: ib_tbl_sch_t) void {
    const schema = ib_tbl_sch orelse return;
    tableSchemaDestroy(schema);
}

pub fn ib_index_schema_add_col(ib_idx_sch: ib_idx_sch_t, name: []const u8, prefix_len: ib_ulint_t) ib_err_t {
    const index = ib_idx_sch orelse return .DB_ERROR;
    if (name.len == 0) {
        return .DB_INVALID_INPUT;
    }
    if (indexSchemaHasColumn(index, name)) {
        return .DB_COL_APPEARS_TWICE_IN_INDEX;
    }
    if (index.schema_owner) |owner| {
        if (!tableSchemaFindColumn(owner, name)) {
            return .DB_NOT_FOUND;
        }
    }

    const name_copy = dupName(index.allocator, name) catch return .DB_OUT_OF_MEMORY;
    index.columns.append(.{
        .name = name_copy,
        .prefix_len = prefix_len,
    }) catch {
        index.allocator.free(name_copy);
        return .DB_OUT_OF_MEMORY;
    };

    return .DB_SUCCESS;
}

pub fn ib_index_schema_create(
    ib_usr_trx: ib_trx_t,
    name: []const u8,
    table_name: []const u8,
    ib_idx_sch: *ib_idx_sch_t,
) ib_err_t {
    if (ib_schema_lock_is_exclusive(ib_usr_trx) != compat.IB_TRUE) {
        ib_idx_sch.* = null;
        return .DB_SCHEMA_NOT_LOCKED;
    }
    if (name.len == 0 or schemaNameEq(name, "GEN_CLUST_INDEX")) {
        ib_idx_sch.* = null;
        return .DB_INVALID_INPUT;
    }
    if (table_name.len == 0) {
        ib_idx_sch.* = null;
        return .DB_TABLE_NOT_FOUND;
    }
    if (catalogFindByName(table_name) == null) {
        ib_idx_sch.* = null;
        return .DB_TABLE_NOT_FOUND;
    }

    const allocator = std.heap.page_allocator;
    const index = allocator.create(IndexSchema) catch {
        ib_idx_sch.* = null;
        return .DB_OUT_OF_MEMORY;
    };

    const name_copy = dupName(allocator, name) catch {
        allocator.destroy(index);
        ib_idx_sch.* = null;
        return .DB_OUT_OF_MEMORY;
    };
    const table_name_copy = dupName(allocator, table_name) catch {
        allocator.free(name_copy);
        allocator.destroy(index);
        ib_idx_sch.* = null;
        return .DB_OUT_OF_MEMORY;
    };

    index.* = .{
        .allocator = allocator,
        .name = name_copy,
        .table_name = table_name_copy,
        .columns = ArrayList(IndexColumn).init(allocator),
        .clustered = false,
        .unique = false,
        .schema_owner = null,
        .trx = ib_usr_trx,
    };

    ib_idx_sch.* = index;
    return .DB_SUCCESS;
}

pub fn ib_index_schema_set_clustered(ib_idx_sch: ib_idx_sch_t) ib_err_t {
    const index = ib_idx_sch orelse return .DB_ERROR;
    if (index.schema_owner) |owner| {
        for (owner.indexes.items) |idx| {
            idx.clustered = false;
        }
    }
    index.unique = true;
    index.clustered = true;
    return .DB_SUCCESS;
}

pub fn ib_index_schema_set_unique(ib_idx_sch: ib_idx_sch_t) ib_err_t {
    const index = ib_idx_sch orelse return .DB_ERROR;
    index.unique = true;
    return .DB_SUCCESS;
}

pub fn ib_index_schema_delete(ib_idx_sch: ib_idx_sch_t) void {
    const index = ib_idx_sch orelse return;
    if (index.schema_owner != null) {
        return;
    }
    indexSchemaDestroy(index);
}

pub fn ib_table_create(ib_trx: ib_trx_t, ib_tbl_sch: ib_tbl_sch_t, id: *ib_id_t) ib_err_t {
    const trx = ib_trx orelse return .DB_SCHEMA_NOT_LOCKED;
    if (ib_schema_lock_is_exclusive(trx) != compat.IB_TRUE) {
        return .DB_SCHEMA_NOT_LOCKED;
    }

    const schema = ib_tbl_sch orelse return .DB_ERROR;
    if (catalogFindByName(schema.name)) |existing| {
        id.* = existing.id;
        return .DB_TABLE_IS_BEING_USED;
    }
    if (schema.table_id) |existing| {
        id.* = existing;
        return .DB_TABLE_IS_BEING_USED;
    }
    if (schema.columns.items.len == 0) {
        return .DB_SCHEMA_ERROR;
    }

    var clustered_count: usize = 0;
    for (schema.indexes.items) |idx| {
        if (idx.columns.items.len == 0) {
            return .DB_SCHEMA_ERROR;
        }
        if (idx.clustered) {
            clustered_count += 1;
            if (clustered_count > 1) {
                return .DB_SCHEMA_ERROR;
            }
        }
    }

    const new_id = next_table_id;
    const catalog = catalogCreateFromSchema(schema, new_id) catch return .DB_OUT_OF_MEMORY;
    table_registry.append(catalog) catch {
        catalogDestroy(catalog);
        return .DB_OUT_OF_MEMORY;
    };

    next_table_id += 1;
    schema.table_id = new_id;
    id.* = new_id;
    return .DB_SUCCESS;
}

pub fn ib_table_rename(ib_trx: ib_trx_t, old_name: []const u8, new_name: []const u8) ib_err_t {
    if (ib_schema_lock_is_exclusive(ib_trx) != compat.IB_TRUE) {
        const err = ib_schema_lock_exclusive(ib_trx);
        if (err != .DB_SUCCESS) {
            return err;
        }
    }
    if (old_name.len == 0 or new_name.len == 0) {
        return .DB_INVALID_INPUT;
    }
    if (catalogFindByName(new_name) != null and !schemaNameEq(old_name, new_name)) {
        return .DB_DUPLICATE_KEY;
    }

    if (catalogFindByName(old_name)) |table| {
        const name_copy = dupName(table.allocator, new_name) catch return .DB_OUT_OF_MEMORY;
        table.allocator.free(table.name);
        table.name = name_copy;
        return .DB_SUCCESS;
    }

    return .DB_TABLE_NOT_FOUND;
}

pub fn ib_index_create(ib_idx_sch: ib_idx_sch_t, index_id: *ib_id_t) ib_err_t {
    const index = ib_idx_sch orelse return .DB_ERROR;
    if (ib_schema_lock_is_exclusive(index.trx) != compat.IB_TRUE) {
        return .DB_SCHEMA_NOT_LOCKED;
    }
    if (index.columns.items.len == 0) {
        return .DB_SCHEMA_ERROR;
    }

    const table_id = if (index.schema_owner) |owner| owner.table_id else null;
    const low_bits = next_index_id & 0xFFFF_FFFF;
    next_index_id += 1;
    index_id.* = if (table_id) |tid| (tid << 32) | low_bits else low_bits;

    if (index.schema_owner == null) {
        const table = catalogFindByName(index.table_name) orelse return .DB_TABLE_NOT_FOUND;
        catalogAppendIndex(table, index, index_id.*) catch return .DB_OUT_OF_MEMORY;
    }

    return .DB_SUCCESS;
}

pub fn ib_table_drop(ib_trx: ib_trx_t, name: []const u8) ib_err_t {
    if (ib_schema_lock_is_exclusive(ib_trx) != compat.IB_TRUE) {
        return .DB_SCHEMA_NOT_LOCKED;
    }
    if (name.len == 0) {
        return .DB_INVALID_INPUT;
    }
    return if (catalogRemoveByName(name)) .DB_SUCCESS else .DB_TABLE_NOT_FOUND;
}

pub fn ib_index_drop(ib_trx: ib_trx_t, index_id: ib_id_t) ib_err_t {
    if (ib_schema_lock_is_exclusive(ib_trx) != compat.IB_TRUE) {
        return .DB_SCHEMA_NOT_LOCKED;
    }
    if (index_id == 0) {
        return .DB_TABLE_NOT_FOUND;
    }
    for (table_registry.items) |table| {
        for (table.indexes.items, 0..) |idx, idx_pos| {
            if (idx.id == index_id) {
                catalogIndexDestroy(&table.indexes.items[idx_pos], table.allocator);
                _ = table.indexes.orderedRemove(idx_pos);
                return .DB_SUCCESS;
            }
        }
    }
    return .DB_TABLE_NOT_FOUND;
}

pub fn ib_table_truncate(name: []const u8, table_id: *ib_id_t) ib_err_t {
    table_id.* = 0;
    if (name.len == 0) {
        return .DB_INVALID_INPUT;
    }
    const table = catalogFindByName(name) orelse return .DB_TABLE_NOT_FOUND;
    table_id.* = catalogTruncateTable(table);
    return .DB_SUCCESS;
}

pub fn ib_table_get_id(name: []const u8, table_id: *ib_id_t) ib_err_t {
    table_id.* = 0;
    const table = catalogFindByName(name) orelse return .DB_TABLE_NOT_FOUND;
    table_id.* = table.id;
    return .DB_SUCCESS;
}

pub fn ib_index_get_id(table_name: []const u8, index_name: []const u8, index_id: *ib_id_t) ib_err_t {
    index_id.* = 0;
    const table = catalogFindByName(table_name) orelse return .DB_TABLE_NOT_FOUND;
    const index = catalogFindIndexByName(table, index_name) orelse return .DB_TABLE_NOT_FOUND;
    index_id.* = index.id;
    return .DB_SUCCESS;
}

pub fn ib_database_create(dbname: []const u8) ib_bool_t {
    if (dbname.len == 0) {
        return compat.IB_FALSE;
    }
    for (dbname) |ch| {
        if (ch == '/' or ch == '\\') {
            return compat.IB_FALSE;
        }
    }
    return compat.IB_TRUE;
}

pub fn ib_database_drop(dbname: []const u8) ib_err_t {
    if (dbname.len == 0) {
        return .DB_INVALID_INPUT;
    }

    const allocator = std.heap.page_allocator;
    var prefix = dbname;
    var buf: ?[]u8 = null;
    if (prefix[prefix.len - 1] != '/') {
        const tmp = allocator.alloc(u8, prefix.len + 1) catch return .DB_OUT_OF_MEMORY;
        std.mem.copyForwards(u8, tmp[0..prefix.len], prefix);
        tmp[prefix.len] = '/';
        prefix = tmp;
        buf = tmp;
    }
    defer if (buf) |tmp| allocator.free(tmp);

    var idx: usize = 0;
    while (idx < table_registry.items.len) {
        const table = table_registry.items[idx];
        if (schemaNameHasPrefix(table.name, prefix)) {
            catalogDestroy(table);
            _ = table_registry.orderedRemove(idx);
            continue;
        }
        idx += 1;
    }

    return .DB_SUCCESS;
}

pub fn ib_table_schema_visit(
    ib_trx: ib_trx_t,
    name: []const u8,
    visitor: *const ib_schema_visitor_t,
    arg: ib_opaque_t,
) ib_err_t {
    if (ib_schema_lock_is_exclusive(ib_trx) != compat.IB_TRUE) {
        return .DB_SCHEMA_NOT_LOCKED;
    }
    if (name.len == 0) {
        return .DB_TABLE_NOT_FOUND;
    }
    const table = catalogFindByName(name) orelse return .DB_TABLE_NOT_FOUND;

    if (@intFromEnum(visitor.version) < @intFromEnum(ib_schema_visitor_version_t.IB_SCHEMA_VISITOR_TABLE)) {
        return .DB_SUCCESS;
    }

    if (visitor.table) |func| {
        const user_err = func(
            arg,
            table.name.ptr,
            table.format,
            table.page_size,
            @intCast(table.columns.items.len),
            @intCast(table.indexes.items.len),
        );
        if (user_err != 0) {
            return .DB_ERROR;
        }
    }

    if (@intFromEnum(visitor.version) < @intFromEnum(ib_schema_visitor_version_t.IB_SCHEMA_VISITOR_TABLE_COL)) {
        return .DB_SUCCESS;
    }

    if (visitor.table_col) |func| {
        for (table.columns.items) |col| {
            const user_err = func(arg, col.name.ptr, col.col_type, col.len, col.attr);
            if (user_err != 0) {
                return .DB_ERROR;
            }
        }
    }

    if (visitor.index == null or @intFromEnum(visitor.version) < @intFromEnum(ib_schema_visitor_version_t.IB_SCHEMA_VISITOR_TABLE_AND_INDEX)) {
        return .DB_SUCCESS;
    }

    for (table.indexes.items) |idx| {
        if (idx.columns.items.len == 0) {
            continue;
        }
        const user_err = visitor.index.?(
            arg,
            idx.name.ptr,
            if (idx.clustered) compat.IB_TRUE else compat.IB_FALSE,
            if (idx.unique) compat.IB_TRUE else compat.IB_FALSE,
            @intCast(idx.columns.items.len),
        );
        if (user_err != 0) {
            return .DB_ERROR;
        }

        if (@intFromEnum(visitor.version) >= @intFromEnum(ib_schema_visitor_version_t.IB_SCHEMA_VISITOR_TABLE_AND_INDEX_COL)) {
            if (visitor.index_col) |icol_func| {
                for (idx.columns.items) |icol| {
                    const col_err = icol_func(arg, icol.name.ptr, icol.prefix_len);
                    if (col_err != 0) {
                        return .DB_ERROR;
                    }
                }
            }
        }
    }

    return .DB_SUCCESS;
}

pub fn ib_schema_tables_iterate(
    ib_trx: ib_trx_t,
    visitor: ib_schema_visitor_table_all_t,
    arg: ib_opaque_t,
) ib_err_t {
    if (ib_schema_lock_is_exclusive(ib_trx) != compat.IB_TRUE) {
        return .DB_SCHEMA_NOT_LOCKED;
    }
    if (table_registry.items.len == 0) {
        return .DB_TABLE_NOT_FOUND;
    }

    for (table_registry.items) |table| {
        const user_err = visitor(arg, table.name.ptr, @intCast(table.name.len));
        if (user_err != 0) {
            break;
        }
    }

    return .DB_SUCCESS;
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

    const metas = [_]ib_col_meta_t{
        .{
            .type = .IB_INT,
            .attr = .IB_COL_NONE,
            .type_len = @intCast(@sizeOf(ib_i32_t)),
            .client_type = 0,
            .charset = null,
        },
    };
    const insert_tpl = try tupleCreate(std.heap.page_allocator, .TPL_ROW, &metas);
    defer ib_tuple_delete(insert_tpl);
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_tuple_write_i32(insert_tpl, 0, 7));
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cursor_insert_row(crsr, insert_tpl));

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

    ib_cursor_stmt_begin(crsr);
    try std.testing.expect(cursor.sql_stat_start);

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

    const insert_tpl = try tupleCreate(std.heap.page_allocator, .TPL_ROW, &metas);
    defer ib_tuple_delete(insert_tpl);
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_tuple_write_i32(insert_tpl, 0, 42));
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cursor_insert_row(crsr, insert_tpl));

    const key_tpl = try tupleCreate(std.heap.page_allocator, .TPL_KEY, &metas);
    defer ib_tuple_delete(key_tpl);
    const row_tpl = try tupleCreate(std.heap.page_allocator, .TPL_ROW, &metas);
    defer ib_tuple_delete(row_tpl);

    var result: i32 = -1;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_tuple_write_i32(key_tpl, 0, 42));
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cursor_moveto(crsr, key_tpl, .IB_CUR_GE, &result));
    try std.testing.expectEqual(@as(i32, 0), result);
    try std.testing.expectEqual(compat.IB_TRUE, ib_cursor_is_positioned(crsr));

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cursor_read_row(crsr, row_tpl));
    var out: ib_i32_t = 0;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_tuple_read_i32(row_tpl, 0, &out));
    try std.testing.expectEqual(@as(ib_i32_t, 42), out);
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

test "cursor insert update delete" {
    var crsr: ib_crsr_t = null;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cursor_open_table("db/t6", null, &crsr));
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

    const insert_tpl = try tupleCreate(std.heap.page_allocator, .TPL_ROW, &metas);
    defer ib_tuple_delete(insert_tpl);
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_tuple_write_i32(insert_tpl, 0, 10));
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cursor_insert_row(crsr, insert_tpl));

    const read_tpl = try tupleCreate(std.heap.page_allocator, .TPL_ROW, &metas);
    defer ib_tuple_delete(read_tpl);
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cursor_read_row(crsr, read_tpl));

    var out: ib_i32_t = 0;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_tuple_read_i32(read_tpl, 0, &out));
    try std.testing.expectEqual(@as(ib_i32_t, 10), out);

    const new_tpl = try tupleCreate(std.heap.page_allocator, .TPL_ROW, &metas);
    defer ib_tuple_delete(new_tpl);
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_tuple_write_i32(new_tpl, 0, 20));
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cursor_update_row(crsr, insert_tpl, new_tpl));

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cursor_read_row(crsr, read_tpl));
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_tuple_read_i32(read_tpl, 0, &out));
    try std.testing.expectEqual(@as(ib_i32_t, 20), out);

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cursor_delete_row(crsr));
    try std.testing.expectEqual(errors.DbErr.DB_RECORD_NOT_FOUND, ib_cursor_read_row(crsr, read_tpl));
}

test "schema lock state transitions" {
    const trx = ib_trx_begin(.IB_TRX_READ_COMMITTED);
    defer _ = ib_trx_rollback(trx);

    try std.testing.expectEqual(compat.IB_FALSE, ib_schema_lock_is_shared(trx));
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_schema_lock_shared(trx));
    try std.testing.expectEqual(compat.IB_TRUE, ib_schema_lock_is_shared(trx));
    try std.testing.expectEqual(errors.DbErr.DB_SCHEMA_NOT_LOCKED, ib_schema_lock_exclusive(trx));
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_schema_unlock(trx));
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_schema_lock_exclusive(trx));
    try std.testing.expectEqual(compat.IB_TRUE, ib_schema_lock_is_exclusive(trx));
}

test "savepoint lifecycle" {
    const trx = ib_trx_begin(.IB_TRX_READ_COMMITTED);
    defer _ = ib_trx_rollback(trx);

    const sp1 = "sp1";
    const sp2 = "sp2";

    ib_savepoint_take(trx, @as(*const anyopaque, @ptrCast(sp1.ptr)), sp1.len);
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_savepoint_release(trx, @as(*const anyopaque, @ptrCast(sp1.ptr)), sp1.len),
    );
    try std.testing.expectEqual(
        errors.DbErr.DB_NO_SAVEPOINT,
        ib_savepoint_release(trx, @as(*const anyopaque, @ptrCast(sp1.ptr)), sp1.len),
    );

    ib_savepoint_take(trx, @as(*const anyopaque, @ptrCast(sp1.ptr)), sp1.len);
    ib_savepoint_take(trx, @as(*const anyopaque, @ptrCast(sp2.ptr)), sp2.len);
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_savepoint_rollback(trx, @as(*const anyopaque, @ptrCast(sp1.ptr)), sp1.len),
    );
    try std.testing.expectEqual(
        errors.DbErr.DB_NO_SAVEPOINT,
        ib_savepoint_release(trx, @as(*const anyopaque, @ptrCast(sp2.ptr)), sp2.len),
    );

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_savepoint_rollback(trx, null, 0));
    try std.testing.expectEqual(errors.DbErr.DB_NO_SAVEPOINT, ib_savepoint_rollback(trx, null, 0));
}

test "table schema create and create table" {
    const trx = ib_trx_begin(.IB_TRX_REPEATABLE_READ);
    defer _ = ib_trx_rollback(trx);

    var tbl_sch: ib_tbl_sch_t = null;
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_table_schema_create("db/t4", &tbl_sch, .IB_TBL_COMPACT, 0),
    );
    try std.testing.expect(tbl_sch != null);

    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_table_schema_add_col(tbl_sch, "id", .IB_INT, .IB_COL_UNSIGNED, 0, 4),
    );

    var idx_sch: ib_idx_sch_t = null;
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_table_schema_add_index(tbl_sch, "PRIMARY", &idx_sch),
    );
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_index_schema_add_col(idx_sch, "id", 0));
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_index_schema_set_clustered(idx_sch));

    var table_id: ib_id_t = 0;
    try std.testing.expectEqual(errors.DbErr.DB_SCHEMA_NOT_LOCKED, ib_table_create(trx, tbl_sch, &table_id));

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_schema_lock_exclusive(trx));
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_table_create(trx, tbl_sch, &table_id));
    try std.testing.expect(table_id != 0);

    var again_id: ib_id_t = 0;
    try std.testing.expectEqual(errors.DbErr.DB_TABLE_IS_BEING_USED, ib_table_create(trx, tbl_sch, &again_id));
    try std.testing.expectEqual(table_id, again_id);

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_table_drop(trx, "db/t4"));
    ib_table_schema_delete(tbl_sch);
}

test "table lock uses catalog" {
    const trx = ib_trx_begin(.IB_TRX_READ_COMMITTED);
    defer _ = ib_trx_rollback(trx);

    var tbl_sch: ib_tbl_sch_t = null;
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_table_schema_create("db/lock1", &tbl_sch, .IB_TBL_COMPACT, 0),
    );
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_table_schema_add_col(tbl_sch, "id", .IB_INT, .IB_COL_UNSIGNED, 0, 4),
    );

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_schema_lock_exclusive(trx));
    var table_id: ib_id_t = 0;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_table_create(trx, tbl_sch, &table_id));

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_table_lock(trx, table_id, .IB_LOCK_IX));
    try std.testing.expectEqual(errors.DbErr.DB_TABLE_NOT_FOUND, ib_table_lock(trx, 0, .IB_LOCK_IX));
    try std.testing.expectEqual(errors.DbErr.DB_ERROR, ib_table_lock(trx, table_id, .IB_LOCK_S));

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_table_drop(trx, "db/lock1"));
    ib_table_schema_delete(tbl_sch);
}

test "index schema create and index create" {
    const trx = ib_trx_begin(.IB_TRX_READ_COMMITTED);
    defer _ = ib_trx_rollback(trx);

    var idx_sch: ib_idx_sch_t = null;
    try std.testing.expectEqual(
        errors.DbErr.DB_SCHEMA_NOT_LOCKED,
        ib_index_schema_create(trx, "idx1", "db/t5", &idx_sch),
    );

    var tbl_sch: ib_tbl_sch_t = null;
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_table_schema_create("db/t5", &tbl_sch, .IB_TBL_COMPACT, 0),
    );
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_table_schema_add_col(tbl_sch, "col1", .IB_INT, .IB_COL_NONE, 0, 4),
    );

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_schema_lock_exclusive(trx));

    var table_id: ib_id_t = 0;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_table_create(trx, tbl_sch, &table_id));

    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_index_schema_create(trx, "idx1", "db/t5", &idx_sch),
    );
    try std.testing.expect(idx_sch != null);

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_index_schema_add_col(idx_sch, "col1", 0));
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_index_schema_set_unique(idx_sch));

    var index_id: ib_id_t = 0;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_index_create(idx_sch, &index_id));
    try std.testing.expect(index_id != 0);

    ib_index_schema_delete(idx_sch);
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_table_drop(trx, "db/t5"));
    ib_table_schema_delete(tbl_sch);
}

test "schema visitor callbacks" {
    const trx = ib_trx_begin(.IB_TRX_READ_COMMITTED);
    defer _ = ib_trx_rollback(trx);

    var tbl_sch: ib_tbl_sch_t = null;
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_table_schema_create("db/visit1", &tbl_sch, .IB_TBL_COMPACT, 0),
    );
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_table_schema_add_col(tbl_sch, "id", .IB_INT, .IB_COL_UNSIGNED, 0, 4),
    );
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_table_schema_add_col(tbl_sch, "name", .IB_VARCHAR, .IB_COL_NONE, 0, 8),
    );

    var primary_idx: ib_idx_sch_t = null;
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_table_schema_add_index(tbl_sch, "PRIMARY", &primary_idx),
    );
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_index_schema_add_col(primary_idx, "id", 0));
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_index_schema_set_clustered(primary_idx));

    var name_idx: ib_idx_sch_t = null;
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_table_schema_add_index(tbl_sch, "idx_name", &name_idx),
    );
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_index_schema_add_col(name_idx, "name", 0));

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_schema_lock_exclusive(trx));
    var table_id: ib_id_t = 0;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_table_create(trx, tbl_sch, &table_id));

    const VisitCtx = struct {
        table_calls: usize,
        col_calls: usize,
        index_calls: usize,
        index_col_calls: usize,
        last_n_cols: i32,
        last_n_indexes: i32,
    };

    const Visitor = struct {
        fn table(
            arg: ib_opaque_t,
            _: [*]const u8,
            _: ib_tbl_fmt_t,
            _: ib_ulint_t,
            n_cols: i32,
            n_indexes: i32,
        ) callconv(.c) i32 {
            const ctx_ptr = arg orelse return 1;
            const ctx = @as(*VisitCtx, @ptrCast(@alignCast(ctx_ptr)));
            ctx.table_calls += 1;
            ctx.last_n_cols = n_cols;
            ctx.last_n_indexes = n_indexes;
            return 0;
        }

        fn tableCol(
            arg: ib_opaque_t,
            _: [*]const u8,
            _: ib_col_type_t,
            _: ib_ulint_t,
            _: ib_col_attr_t,
        ) callconv(.c) i32 {
            const ctx_ptr = arg orelse return 1;
            const ctx = @as(*VisitCtx, @ptrCast(@alignCast(ctx_ptr)));
            ctx.col_calls += 1;
            return 0;
        }

        fn index(
            arg: ib_opaque_t,
            _: [*]const u8,
            _: ib_bool_t,
            _: ib_bool_t,
            _: i32,
        ) callconv(.c) i32 {
            const ctx_ptr = arg orelse return 1;
            const ctx = @as(*VisitCtx, @ptrCast(@alignCast(ctx_ptr)));
            ctx.index_calls += 1;
            return 0;
        }

        fn indexCol(
            arg: ib_opaque_t,
            _: [*]const u8,
            _: ib_ulint_t,
        ) callconv(.c) i32 {
            const ctx_ptr = arg orelse return 1;
            const ctx = @as(*VisitCtx, @ptrCast(@alignCast(ctx_ptr)));
            ctx.index_col_calls += 1;
            return 0;
        }
    };

    var ctx = VisitCtx{
        .table_calls = 0,
        .col_calls = 0,
        .index_calls = 0,
        .index_col_calls = 0,
        .last_n_cols = 0,
        .last_n_indexes = 0,
    };
    const visitor = ib_schema_visitor_t{
        .version = .IB_SCHEMA_VISITOR_TABLE_AND_INDEX_COL,
        .table = Visitor.table,
        .table_col = Visitor.tableCol,
        .index = Visitor.index,
        .index_col = Visitor.indexCol,
    };
    const arg: ib_opaque_t = @as(*anyopaque, @ptrCast(&ctx));

    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_table_schema_visit(trx, "db/visit1", &visitor, arg),
    );
    try std.testing.expectEqual(@as(usize, 1), ctx.table_calls);
    try std.testing.expectEqual(@as(usize, 2), ctx.col_calls);
    try std.testing.expectEqual(@as(usize, 2), ctx.index_calls);
    try std.testing.expectEqual(@as(usize, 2), ctx.index_col_calls);
    try std.testing.expectEqual(@as(i32, 2), ctx.last_n_cols);
    try std.testing.expectEqual(@as(i32, 2), ctx.last_n_indexes);

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_table_drop(trx, "db/visit1"));
    ib_table_schema_delete(tbl_sch);
}

test "schema tables iterate" {
    const trx = ib_trx_begin(.IB_TRX_READ_COMMITTED);
    defer _ = ib_trx_rollback(trx);

    var tbl_sch: ib_tbl_sch_t = null;
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_table_schema_create("db/iter1", &tbl_sch, .IB_TBL_COMPACT, 0),
    );
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_table_schema_add_col(tbl_sch, "id", .IB_INT, .IB_COL_UNSIGNED, 0, 4),
    );

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_schema_lock_exclusive(trx));
    var table_id: ib_id_t = 0;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_table_create(trx, tbl_sch, &table_id));

    const IterCtx = struct {
        calls: usize,
        saw_target: bool,
        target: []const u8,
    };

    const Iterator = struct {
        fn table(arg: ib_opaque_t, name: [*]const u8, name_len: i32) callconv(.c) i32 {
            const ctx_ptr = arg orelse return 1;
            const ctx = @as(*IterCtx, @ptrCast(@alignCast(ctx_ptr)));
            ctx.calls += 1;
            const slice = name[0..@intCast(name_len)];
            if (std.mem.eql(u8, slice, ctx.target)) {
                ctx.saw_target = true;
            }
            return 0;
        }
    };

    var ctx = IterCtx{ .calls = 0, .saw_target = false, .target = "db/iter1" };
    const arg: ib_opaque_t = @as(*anyopaque, @ptrCast(&ctx));

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_schema_tables_iterate(trx, Iterator.table, arg));
    try std.testing.expect(ctx.calls > 0);
    try std.testing.expect(ctx.saw_target);

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_table_drop(trx, "db/iter1"));
    ib_table_schema_delete(tbl_sch);
}

test "config set get and list" {
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cfg_init());

    var cfg_type: ib_cfg_type_t = undefined;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cfg_var_get_type("buffer_pool_size", &cfg_type));
    try std.testing.expectEqual(ib_cfg_type_t.IB_CFG_ULINT, cfg_type);

    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_cfg_set("buffer_pool_size", @as(ib_ulint_t, 16 * 1024 * 1024)),
    );
    var pool_size: ib_ulint_t = 0;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cfg_get("buffer_pool_size", &pool_size));
    try std.testing.expectEqual(@as(ib_ulint_t, 16 * 1024 * 1024), pool_size);

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cfg_set("rollback_on_timeout", compat.IB_FALSE));
    var rollback: ib_bool_t = compat.IB_TRUE;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cfg_get("rollback_on_timeout", &rollback));
    try std.testing.expectEqual(compat.IB_FALSE, rollback);

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cfg_set("data_home_dir", "data/"));
    var data_home: []const u8 = "";
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cfg_get("data_home_dir", &data_home));
    try std.testing.expect(std.mem.eql(u8, data_home, "data/"));
    try std.testing.expectEqual(errors.DbErr.DB_INVALID_INPUT, ib_cfg_set("data_home_dir", "data"));

    var names: [][]const u8 = undefined;
    var names_num: ib_u32_t = 0;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cfg_get_all(&names, &names_num));
    defer std.heap.page_allocator.free(names);
    try std.testing.expect(names_num > 0);
    var found = false;
    for (names) |name| {
        if (std.ascii.eqlIgnoreCase(name, "buffer_pool_size")) {
            found = true;
            break;
        }
    }
    try std.testing.expect(found);

    try std.testing.expectEqual(errors.DbErr.DB_NOT_FOUND, ib_cfg_var_get_type("unknown", &cfg_type));
    try std.testing.expectEqual(errors.DbErr.DB_NOT_FOUND, ib_cfg_set("unknown", @as(ib_ulint_t, 1)));
    try std.testing.expectEqual(errors.DbErr.DB_INVALID_INPUT, ib_cfg_set("buffer_pool_size", @as(ib_ulint_t, 1)));
    try std.testing.expectEqual(errors.DbErr.DB_INVALID_INPUT, ib_cfg_set("file_format", "Zebra"));

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cfg_shutdown());
}

test "config readonly after startup" {
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cfg_init());
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_startup(null));

    try std.testing.expectEqual(
        errors.DbErr.DB_READONLY,
        ib_cfg_set("buffer_pool_size", @as(ib_ulint_t, 16 * 1024 * 1024)),
    );
    try std.testing.expectEqual(errors.DbErr.DB_READONLY, ib_cfg_set("adaptive_hash_index", compat.IB_FALSE));

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_shutdown(.IB_SHUTDOWN_NORMAL));
}

test "exec sql stubs validate input" {
    try std.testing.expectEqual(errors.DbErr.DB_INVALID_INPUT, ib_exec_sql("", 0));
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_exec_sql("select 1", 0));
    try std.testing.expectEqual(errors.DbErr.DB_INVALID_INPUT, ib_exec_sql("select 1", 1));

    try std.testing.expectEqual(errors.DbErr.DB_INVALID_INPUT, ib_exec_ddl_sql("", 0));
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_exec_ddl_sql("create table t(a int)", 0));
    try std.testing.expectEqual(errors.DbErr.DB_INVALID_INPUT, ib_exec_ddl_sql("create table t(a int)", 2));

    const args = [_]ib_sql_arg_t{
        .{ .text = .{ .name = ":name", .value = "alice" } },
        .{ .int = .{ .name = ":id", .value = 42, .unsigned = true, .len = 4 } },
    };
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_exec_sql_args("select :id", &args));
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_exec_ddl_sql_args("create table t(a int)", &args));

    const bad_args = [_]ib_sql_arg_t{
        .{ .int = .{ .name = "id", .value = 1, .unsigned = false, .len = 3 } },
    };
    try std.testing.expectEqual(errors.DbErr.DB_INVALID_INPUT, ib_exec_sql_args("select 1", &bad_args));
}

test "tuple create helpers and cluster key" {
    const trx = ib_trx_begin(.IB_TRX_READ_COMMITTED);
    defer _ = ib_trx_rollback(trx);

    var tbl_sch: ib_tbl_sch_t = null;
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_table_schema_create("db/tpl1", &tbl_sch, .IB_TBL_COMPACT, 0),
    );
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_table_schema_add_col(tbl_sch, "id", .IB_INT, .IB_COL_UNSIGNED, 0, 4),
    );
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_table_schema_add_col(tbl_sch, "name", .IB_VARCHAR, .IB_COL_NONE, 0, 10),
    );

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_schema_lock_exclusive(trx));
    var table_id: ib_id_t = 0;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_table_create(trx, tbl_sch, &table_id));

    var primary_idx: ib_idx_sch_t = null;
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_index_schema_create(trx, "PRIMARY", "db/tpl1", &primary_idx),
    );
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_index_schema_add_col(primary_idx, "id", 0));
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_index_schema_set_clustered(primary_idx));
    var primary_id: ib_id_t = 0;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_index_create(primary_idx, &primary_id));

    var sec_idx: ib_idx_sch_t = null;
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_index_schema_create(trx, "idx_id", "db/tpl1", &sec_idx),
    );
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_index_schema_add_col(sec_idx, "id", 0));
    var sec_id: ib_id_t = 0;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_index_create(sec_idx, &sec_id));

    var table_crsr: ib_crsr_t = null;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cursor_open_table("db/tpl1", null, &table_crsr));
    var index_crsr: ib_crsr_t = null;
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_cursor_open_index_using_name(table_crsr, "idx_id", &index_crsr),
    );

    const sec_key = ib_sec_search_tuple_create(index_crsr);
    try std.testing.expect(sec_key != null);
    try std.testing.expectEqual(ib_tuple_type_t.TPL_KEY, sec_key.?.tuple_type);
    try std.testing.expectEqual(@as(ib_ulint_t, 1), ib_tuple_get_n_cols(sec_key));

    const sec_row = ib_sec_read_tuple_create(index_crsr);
    try std.testing.expect(sec_row != null);
    try std.testing.expectEqual(ib_tuple_type_t.TPL_ROW, sec_row.?.tuple_type);

    const clust_key = ib_clust_search_tuple_create(table_crsr);
    try std.testing.expect(clust_key != null);
    try std.testing.expectEqual(ib_tuple_type_t.TPL_KEY, clust_key.?.tuple_type);

    const clust_row = ib_clust_read_tuple_create(table_crsr);
    try std.testing.expect(clust_row != null);
    try std.testing.expectEqual(ib_tuple_type_t.TPL_ROW, clust_row.?.tuple_type);
    try std.testing.expectEqual(@as(ib_ulint_t, 2), ib_tuple_get_n_cols(clust_row));

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_tuple_write_u32(sec_key, 0, 42));
    var dst_key: ib_tpl_t = null;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_tuple_get_cluster_key(index_crsr, &dst_key, sec_key));
    var out: ib_u32_t = 0;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_tuple_read_u32(dst_key, 0, &out));
    try std.testing.expectEqual(@as(ib_u32_t, 42), out);

    ib_tuple_delete(sec_key);
    ib_tuple_delete(sec_row);
    ib_tuple_delete(clust_key);
    ib_tuple_delete(clust_row);
    ib_tuple_delete(dst_key);

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cursor_close(index_crsr));
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cursor_close(table_crsr));

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_table_drop(trx, "db/tpl1"));
    ib_table_schema_delete(tbl_sch);
    ib_index_schema_delete(primary_idx);
    ib_index_schema_delete(sec_idx);
}

test "table id and index id lookups" {
    const trx = ib_trx_begin(.IB_TRX_READ_COMMITTED);
    defer _ = ib_trx_rollback(trx);

    var tbl_sch: ib_tbl_sch_t = null;
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_table_schema_create("db/id1", &tbl_sch, .IB_TBL_COMPACT, 0),
    );
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_table_schema_add_col(tbl_sch, "id", .IB_INT, .IB_COL_UNSIGNED, 0, 4),
    );

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_schema_lock_exclusive(trx));
    var table_id: ib_id_t = 0;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_table_create(trx, tbl_sch, &table_id));

    var idx_sch: ib_idx_sch_t = null;
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_index_schema_create(trx, "PRIMARY", "db/id1", &idx_sch),
    );
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_index_schema_add_col(idx_sch, "id", 0));
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_index_schema_set_clustered(idx_sch));
    var index_id: ib_id_t = 0;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_index_create(idx_sch, &index_id));

    var lookup_table: ib_id_t = 0;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_table_get_id("db/id1", &lookup_table));
    try std.testing.expectEqual(table_id, lookup_table);

    var lookup_index: ib_id_t = 0;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_index_get_id("db/id1", "PRIMARY", &lookup_index));
    try std.testing.expectEqual(index_id, lookup_index);

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_table_drop(trx, "db/id1"));
    ib_table_schema_delete(tbl_sch);
    ib_index_schema_delete(idx_sch);
}

test "table and cursor truncate" {
    const trx = ib_trx_begin(.IB_TRX_READ_COMMITTED);
    defer _ = ib_trx_rollback(trx);

    var tbl_sch: ib_tbl_sch_t = null;
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_table_schema_create("db/trunc1", &tbl_sch, .IB_TBL_COMPACT, 0),
    );
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_table_schema_add_col(tbl_sch, "id", .IB_INT, .IB_COL_UNSIGNED, 0, 4),
    );

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_schema_lock_exclusive(trx));
    var table_id: ib_id_t = 0;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_table_create(trx, tbl_sch, &table_id));

    var trunc_id: ib_id_t = 0;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_table_truncate("db/trunc1", &trunc_id));
    try std.testing.expect(trunc_id != 0);
    try std.testing.expect(trunc_id != table_id);

    var lookup: ib_id_t = 0;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_table_get_id("db/trunc1", &lookup));
    try std.testing.expectEqual(trunc_id, lookup);

    var crsr: ib_crsr_t = null;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cursor_open_table("db/trunc1", trx, &crsr));
    var trunc_id2: ib_id_t = 0;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_cursor_truncate(&crsr, &trunc_id2));
    try std.testing.expect(crsr == null);
    try std.testing.expect(trunc_id2 != 0);

    var lookup2: ib_id_t = 0;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_table_get_id("db/trunc1", &lookup2));
    try std.testing.expectEqual(trunc_id2, lookup2);

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_table_drop(trx, "db/trunc1"));
    ib_table_schema_delete(tbl_sch);
}

test "database create and drop" {
    try std.testing.expectEqual(compat.IB_FALSE, ib_database_create("bad/name"));
    try std.testing.expectEqual(compat.IB_TRUE, ib_database_create("okdb"));

    const trx = ib_trx_begin(.IB_TRX_READ_COMMITTED);
    defer _ = ib_trx_rollback(trx);

    var tbl_sch: ib_tbl_sch_t = null;
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_table_schema_create("dbdrop/t1", &tbl_sch, .IB_TBL_COMPACT, 0),
    );
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_table_schema_add_col(tbl_sch, "id", .IB_INT, .IB_COL_UNSIGNED, 0, 4),
    );

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_schema_lock_exclusive(trx));
    var table_id: ib_id_t = 0;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_table_create(trx, tbl_sch, &table_id));

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_database_drop("dbdrop"));

    var lookup: ib_id_t = 0;
    try std.testing.expectEqual(errors.DbErr.DB_TABLE_NOT_FOUND, ib_table_get_id("dbdrop/t1", &lookup));

    ib_table_schema_delete(tbl_sch);
}

test "status get i64" {
    export_vars = ExportVars{
        .innodb_page_size = 0,
        .innodb_have_atomic_builtins = compat.IB_FALSE,
    };
    export_vars.innodb_data_pending_reads = 7;
    export_vars.innodb_row_lock_time = 123;

    var val: ib_i64_t = 0;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_status_get_i64("READ_REQ_PENDING", &val));
    try std.testing.expectEqual(@as(ib_i64_t, 7), val);

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_status_get_i64("lock_total_wait_time_in_secs", &val));
    try std.testing.expectEqual(@as(ib_i64_t, 123), val);

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_status_get_i64("have_atomic_builtins", &val));
    try std.testing.expectEqual(@as(ib_i64_t, 1), val);

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_status_get_i64("page_size", &val));
    try std.testing.expectEqual(@as(ib_i64_t, @intCast(compat.UNIV_PAGE_SIZE)), val);

    try std.testing.expectEqual(errors.DbErr.DB_NOT_FOUND, ib_status_get_i64("missing_status", &val));
}

test "ucode helpers" {
    try std.testing.expect(ib_ucode_get_connection_charset() == null);
    try std.testing.expect(ib_ucode_get_charset(42) == null);

    var min_len: ib_ulint_t = 1;
    var max_len: ib_ulint_t = 1;
    ib_ucode_get_charset_width(null, &min_len, &max_len);
    try std.testing.expectEqual(@as(ib_ulint_t, 0), min_len);
    try std.testing.expectEqual(@as(ib_ulint_t, 0), max_len);

    try std.testing.expectEqual(@as(i32, 0), ib_utf8_strcasecmp("AbC", "aBc"));
    try std.testing.expect(ib_utf8_strcasecmp("a", "b") < 0);
    try std.testing.expect(ib_utf8_strcasecmp("c", "b") > 0);

    try std.testing.expectEqual(@as(i32, 0), ib_utf8_strncasecmp("AbCd", "aBcZ", 3));
    try std.testing.expect(ib_utf8_strncasecmp("ab", "ac", 2) < 0);

    var down = [_]u8{ 'A', 'B', 'C' };
    ib_utf8_casedown(down[0..]);
    try std.testing.expectEqualStrings("abc", down[0..]);

    var out = [_]u8{0} ** 8;
    ib_utf8_convert_from_table_id(null, out[0..], "tab", out.len);
    try std.testing.expectEqualStrings("tab", out[0..3]);
    try std.testing.expectEqual(@as(u8, 0), out[3]);

    ib_utf8_convert_from_id(null, out[0..], "name", 4);
    try std.testing.expectEqualStrings("name", out[0..4]);

    try std.testing.expectEqual(@as(i32, 1), ib_utf8_isspace(null, @as(u8, ' ')));
    try std.testing.expectEqual(@as(i32, 0), ib_utf8_isspace(null, @as(u8, 'a')));

    try std.testing.expectEqual(@as(ib_ulint_t, 3), ib_ucode_get_storage_size(null, 5, 3, "hello"));
    try std.testing.expectEqual(@as(ib_ulint_t, 2), ib_ucode_get_storage_size(null, 2, 10, "hello"));
}

test "misc tempfile and error handling" {
    const fd = ib_create_tempfile("ibtmp_");
    try std.testing.expect(fd >= 0);
    if (fd >= 0) {
        std.posix.close(@as(std.posix.fd_t, @intCast(fd)));
    }

    try std.testing.expectEqual(compat.IB_FALSE, trx_is_interrupted(null));

    var err: ib_err_t = .DB_LOCK_WAIT;
    try std.testing.expectEqual(compat.IB_TRUE, ib_handle_errors(&err, null, null, null));
    err = .DB_DUPLICATE_KEY;
    try std.testing.expectEqual(compat.IB_FALSE, ib_handle_errors(&err, null, null, null));
}

test "misc lock and stats stubs" {
    const trx = ib_trx_begin(.IB_TRX_READ_COMMITTED);
    defer _ = ib_trx_rollback(trx);

    var tbl_sch: ib_tbl_sch_t = null;
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_table_schema_create("db/misc1", &tbl_sch, .IB_TBL_COMPACT, 0),
    );
    try std.testing.expectEqual(
        errors.DbErr.DB_SUCCESS,
        ib_table_schema_add_col(tbl_sch, "id", .IB_INT, .IB_COL_UNSIGNED, 0, 4),
    );

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_schema_lock_exclusive(trx));
    var table_id: ib_id_t = 0;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_table_create(trx, tbl_sch, &table_id));

    const table = catalogFindByName("db/misc1") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_trx_lock_table_with_retry(trx, table, .IB_LOCK_S));

    table.stat_modified_counter = 2_000_000_001;
    table.stat_n_rows = 0;
    table.stats_updated = false;
    ib_update_statistics_if_needed(table);
    try std.testing.expect(table.stats_updated);

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, ib_table_drop(trx, "db/misc1"));
    ib_table_schema_delete(tbl_sch);
}
