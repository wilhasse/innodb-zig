const std = @import("std");
const compat = @import("../ut/compat.zig");
const ha = @import("../ha/mod.zig");

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

pub const RECV_PARSING_BUF_SIZE: ulint = 2 * 1024 * 1024;
pub const RECV_SCAN_SIZE: ulint = 4 * compat.UNIV_PAGE_SIZE;

pub const recv_data_t = struct {
    next: ?*recv_data_t = null,
};

pub const recv_t = struct {
    type_: u8 = 0,
    len: ulint = 0,
    data: ?*recv_data_t = null,
    start_lsn: ib_uint64_t = 0,
    end_lsn: ib_uint64_t = 0,
    next: ?*recv_t = null,
};

pub const recv_addr_state = enum(u8) {
    RECV_NOT_PROCESSED = 0,
    RECV_BEING_READ = 1,
    RECV_BEING_PROCESSED = 2,
    RECV_PROCESSED = 3,
};

pub const recv_addr_t = struct {
    state: recv_addr_state = .RECV_NOT_PROCESSED,
    space: ulint = 0,
    page_no: ulint = 0,
    rec_list_head: ?*recv_t = null,
    addr_hash_next: ?*recv_addr_t = null,
};

pub const recv_sys_t = struct {
    mutex: std.Thread.Mutex = .{},
    apply_log_recs: ibool = compat.FALSE,
    apply_batch_on: ibool = compat.FALSE,
    lsn: ib_uint64_t = 0,
    last_log_buf_size: ulint = 0,
    last_block: ?[]u8 = null,
    last_block_buf_start: ?[]u8 = null,
    buf: ?[]u8 = null,
    len: ulint = 0,
    parse_start_lsn: ib_uint64_t = 0,
    scanned_lsn: ib_uint64_t = 0,
    scanned_checkpoint_no: ulint = 0,
    recovered_offset: ulint = 0,
    recovered_lsn: ib_uint64_t = 0,
    limit_lsn: ib_uint64_t = 0,
    found_corrupt_log: ibool = compat.FALSE,
    heap: ?*anyopaque = null,
    addr_hash: ?*ha.hash_table_t = null,
    n_addrs: ulint = 0,
};

pub var recv_replay_file_ops: ibool = compat.TRUE;
pub var recv_sys: ?*recv_sys_t = null;
pub var recv_recovery_on: ibool = compat.FALSE;
pub var recv_recovery_from_backup_on: ibool = compat.FALSE;
pub var recv_needed_recovery: ibool = compat.FALSE;
pub var recv_no_log_write: ibool = compat.FALSE;
pub var recv_lsn_checks_on: ibool = compat.FALSE;
pub var recv_pre_rollback_hook: ?*const fn () void = null;
pub var recv_log_scan_is_startup_type: ibool = compat.FALSE;
pub var recv_no_ibuf_operations: ibool = compat.FALSE;
pub var recv_is_making_a_backup: ibool = compat.FALSE;
pub var recv_is_from_backup: ibool = compat.FALSE;
pub var recv_scan_print_counter: ulint = 0;
pub var recv_previous_parsed_rec_type: ulint = 0;
pub var recv_previous_parsed_rec_offset: ulint = 0;
pub var recv_previous_parsed_rec_is_multi: ulint = 0;
pub var recv_max_parsed_page_no: ulint = 0;
pub var recv_n_pool_free_frames: ulint = 256;
pub var recv_max_page_lsn: ib_uint64_t = 0;

pub fn recv_sys_var_init() void {
    recv_sys = null;
    recv_lsn_checks_on = compat.FALSE;
    recv_n_pool_free_frames = 256;
    recv_recovery_on = compat.FALSE;
    recv_recovery_from_backup_on = compat.FALSE;
    recv_is_from_backup = compat.FALSE;
    recv_needed_recovery = compat.FALSE;
    recv_log_scan_is_startup_type = compat.FALSE;
    recv_no_ibuf_operations = compat.FALSE;
    recv_scan_print_counter = 0;
    recv_previous_parsed_rec_type = 999_999;
    recv_previous_parsed_rec_offset = 0;
    recv_previous_parsed_rec_is_multi = 0;
    recv_max_parsed_page_no = 0;
    recv_n_pool_free_frames = 256;
    recv_max_page_lsn = 0;
}

pub fn recv_sys_create() void {
    if (recv_sys != null) {
        return;
    }
    const sys = std.heap.page_allocator.create(recv_sys_t) catch return;
    sys.* = .{};
    recv_sys = sys;
}

pub fn recv_sys_close() void {
    if (recv_sys) |sys| {
        sys.mutex = .{};
    }
}

pub fn recv_sys_mem_free() void {
    if (recv_sys) |sys| {
        if (sys.addr_hash) |hash| {
            ha.hash_table_free(hash);
            sys.addr_hash = null;
        }
        if (sys.buf) |buf| {
            std.heap.page_allocator.free(buf);
            sys.buf = null;
        }
        if (sys.last_block_buf_start) |buf| {
            std.heap.page_allocator.free(buf);
            sys.last_block_buf_start = null;
            sys.last_block = null;
        }
        std.heap.page_allocator.destroy(sys);
        recv_sys = null;
    }
}

pub fn recv_sys_init(available_memory: ulint) void {
    const sys = recv_sys orelse return;
    if (sys.buf != null) {
        return;
    }

    sys.buf = std.heap.page_allocator.alloc(u8, @as(usize, @intCast(RECV_PARSING_BUF_SIZE))) catch return;
    sys.len = 0;
    sys.recovered_offset = 0;

    const hash_cells = if (available_memory / 64 > 0) available_memory / 64 else 1;
    sys.addr_hash = ha.hash_create(hash_cells);
    sys.n_addrs = 0;

    sys.apply_log_recs = compat.FALSE;
    sys.apply_batch_on = compat.FALSE;

    sys.last_block_buf_start =
        std.heap.page_allocator.alloc(u8, @as(usize, @intCast(2 * OS_FILE_LOG_BLOCK_SIZE))) catch return;
    if (sys.last_block_buf_start) |buf| {
        const addr = @intFromPtr(buf.ptr);
        const aligned = compat.alignUp(addr, OS_FILE_LOG_BLOCK_SIZE);
        const offset = aligned - addr;
        sys.last_block = buf[@as(usize, @intCast(offset)) .. @as(usize, @intCast(offset + OS_FILE_LOG_BLOCK_SIZE))];
    }

    sys.found_corrupt_log = compat.FALSE;
    recv_max_page_lsn = 0;
}

pub fn recv_recovery_is_on() ibool {
    return recv_recovery_on;
}

test "recv_sys_var_init resets globals" {
    var temp = recv_sys_t{};
    recv_sys = &temp;
    recv_lsn_checks_on = compat.TRUE;
    recv_recovery_on = compat.TRUE;
    recv_recovery_from_backup_on = compat.TRUE;
    recv_is_from_backup = compat.TRUE;
    recv_needed_recovery = compat.TRUE;
    recv_log_scan_is_startup_type = compat.TRUE;
    recv_no_ibuf_operations = compat.TRUE;
    recv_scan_print_counter = 7;
    recv_previous_parsed_rec_type = 1;
    recv_previous_parsed_rec_offset = 2;
    recv_previous_parsed_rec_is_multi = 3;
    recv_max_parsed_page_no = 4;
    recv_n_pool_free_frames = 5;
    recv_max_page_lsn = 6;

    recv_sys_var_init();

    try std.testing.expect(recv_sys == null);
    try std.testing.expect(recv_lsn_checks_on == compat.FALSE);
    try std.testing.expect(recv_recovery_on == compat.FALSE);
    try std.testing.expect(recv_recovery_from_backup_on == compat.FALSE);
    try std.testing.expect(recv_is_from_backup == compat.FALSE);
    try std.testing.expect(recv_needed_recovery == compat.FALSE);
    try std.testing.expect(recv_log_scan_is_startup_type == compat.FALSE);
    try std.testing.expect(recv_no_ibuf_operations == compat.FALSE);
    try std.testing.expect(recv_scan_print_counter == 0);
    try std.testing.expect(recv_previous_parsed_rec_type == 999_999);
    try std.testing.expect(recv_previous_parsed_rec_offset == 0);
    try std.testing.expect(recv_previous_parsed_rec_is_multi == 0);
    try std.testing.expect(recv_max_parsed_page_no == 0);
    try std.testing.expect(recv_n_pool_free_frames == 256);
    try std.testing.expect(recv_max_page_lsn == 0);
}

test "recv_sys_create_init_mem_free" {
    recv_sys_var_init();
    recv_sys_create();
    try std.testing.expect(recv_sys != null);

    recv_sys_init(1024);
    try std.testing.expect(recv_sys.?.buf != null);
    try std.testing.expect(recv_sys.?.addr_hash != null);
    try std.testing.expect(recv_sys.?.last_block != null);

    recv_sys_mem_free();
    try std.testing.expect(recv_sys == null);
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
