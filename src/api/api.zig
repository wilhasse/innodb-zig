const std = @import("std");
const compat = @import("../ut/compat.zig");
const errors = @import("../ut/errors.zig");
const log = @import("../ut/log.zig");

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
