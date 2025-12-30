const std = @import("std");
const compat = @import("../ut/compat.zig");
const errors = @import("../ut/errors.zig");
const log = @import("../ut/log.zig");
const os_thread = @import("../os/thread.zig");

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
pub const Cursor = opaque {};
pub const Tuple = opaque {};
pub const TableSchema = opaque {};
pub const IndexSchema = opaque {};

pub const ib_trx_t = ?*Trx;
pub const ib_crsr_t = ?*Cursor;
pub const ib_tpl_t = ?*Tuple;
pub const ib_tbl_sch_t = ?*TableSchema;
pub const ib_idx_sch_t = ?*IndexSchema;

pub const ib_id_t = ib_u64_t;

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
