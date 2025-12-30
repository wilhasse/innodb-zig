const std = @import("std");
const compat = @import("../ut/compat.zig");

pub const module_name = "log";

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;
pub const ib_int64_t = compat.ib_int64_t;
pub const ib_uint64_t = compat.ib_uint64_t;

pub const OS_FILE_LOG_BLOCK_SIZE: ulint = 512;
pub const LOG_FILE_HDR_SIZE: ib_int64_t = @as(ib_int64_t, @intCast(4 * OS_FILE_LOG_BLOCK_SIZE));

pub const LOG_BUF_WRITE_MARGIN: ulint = 4 * OS_FILE_LOG_BLOCK_SIZE;
pub const LOG_BUF_FLUSH_RATIO: ulint = 2;
pub const LOG_BUF_FLUSH_MARGIN: ulint = LOG_BUF_WRITE_MARGIN + 4 * compat.UNIV_PAGE_SIZE;
pub const LOG_CHECKPOINT_FREE_PER_THREAD: ulint = 4 * compat.UNIV_PAGE_SIZE;
pub const LOG_CHECKPOINT_EXTRA_FREE: ulint = 8 * compat.UNIV_PAGE_SIZE;

pub const log_t = struct {
    dummy: u8 = 0,
};

pub var log_fsp_current_free_limit: ulint = 0;
pub var log_sys: ?*log_t = null;
pub var log_do_write: ibool = compat.TRUE;
pub var log_debug_writes: ibool = compat.FALSE;
pub var log_has_printed_chkp_warning: ibool = compat.FALSE;
pub var log_last_warning_time: i64 = 0;

pub fn log_var_init() void {
    log_fsp_current_free_limit = 0;
    log_sys = null;
    log_do_write = compat.TRUE;
    log_debug_writes = compat.FALSE;
    log_has_printed_chkp_warning = compat.FALSE;
    log_last_warning_time = 0;
}

pub fn log_fsp_current_free_limit_set_and_checkpoint(limit: ulint) void {
    log_fsp_current_free_limit = limit;
}

pub fn log_calc_where_lsn_is(
    log_file_offset: *ib_int64_t,
    first_header_lsn: ib_uint64_t,
    lsn_in: ib_uint64_t,
    n_log_files: ulint,
    log_file_size: ib_int64_t,
) ulint {
    std.debug.assert(log_file_size > LOG_FILE_HDR_SIZE);
    const capacity: ib_uint64_t = @as(ib_uint64_t, @intCast(log_file_size - LOG_FILE_HDR_SIZE));
    var lsn = lsn_in;

    if (lsn < first_header_lsn) {
        const span = capacity * @as(ib_uint64_t, @intCast(n_log_files));
        const add_this_many = 1 + (first_header_lsn - lsn) / span;
        lsn += add_this_many * span;
    }

    std.debug.assert(lsn >= first_header_lsn);

    const file_no = @as(ulint, @intCast(((lsn - first_header_lsn) / capacity) % @as(ib_uint64_t, @intCast(n_log_files))));
    const offset = (lsn - first_header_lsn) % capacity + @as(ib_uint64_t, @intCast(LOG_FILE_HDR_SIZE));
    log_file_offset.* = @as(ib_int64_t, @intCast(offset));
    return file_no;
}

test "log_var_init resets globals" {
    var temp = log_t{};
    log_sys = &temp;
    log_fsp_current_free_limit = 55;
    log_do_write = compat.FALSE;
    log_debug_writes = compat.TRUE;
    log_has_printed_chkp_warning = compat.TRUE;
    log_last_warning_time = 42;

    log_var_init();

    try std.testing.expect(log_sys == null);
    try std.testing.expect(log_fsp_current_free_limit == 0);
    try std.testing.expect(log_do_write == compat.TRUE);
    try std.testing.expect(log_debug_writes == compat.FALSE);
    try std.testing.expect(log_has_printed_chkp_warning == compat.FALSE);
    try std.testing.expect(log_last_warning_time == 0);
}

test "log_calc_where_lsn_is positions lsn" {
    var offset: ib_int64_t = 0;
    const file_no0 = log_calc_where_lsn_is(&offset, 1000, 1000, 2, 8192);
    try std.testing.expect(file_no0 == 0);
    try std.testing.expect(offset == 2048);

    const file_no1 = log_calc_where_lsn_is(&offset, 1000, 8000, 2, 8192);
    try std.testing.expect(file_no1 == 1);
    try std.testing.expect(offset == 2904);

    const file_no2 = log_calc_where_lsn_is(&offset, 1000, 500, 2, 8192);
    try std.testing.expect(file_no2 == 1);
    try std.testing.expect(offset == 7692);
}
