const std = @import("std");
const compat = @import("../ut/compat.zig");
const ha = @import("../ha/mod.zig");
const os_file = @import("../os/file.zig");

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

pub const LOG_FILE_MAGIC: u32 = 0x49424C47; // "IBLG"
pub const LOG_FILE_VERSION: u32 = 1;
pub const LOG_FILE_HDR_BYTES: usize = @as(usize, @intCast(LOG_FILE_HDR_SIZE));

const LOG_HDR_MAGIC_OFF: usize = 0;
const LOG_HDR_VERSION_OFF: usize = 4;
const LOG_HDR_START_LSN_OFF: usize = 8;
const LOG_HDR_CHECKPOINT_LSN_OFF: usize = 16;
const LOG_HDR_FLUSHED_LSN_OFF: usize = 24;
const LOG_HDR_CLEAN_SHUTDOWN_OFF: usize = 32;

const LogError = error{
    InvalidHeader,
    InvalidLogFileSize,
    ShortRead,
    ShortWrite,
};

const LogHeader = struct {
    magic: u32 = LOG_FILE_MAGIC,
    version: u32 = LOG_FILE_VERSION,
    start_lsn: ib_uint64_t = 0,
    checkpoint_lsn: ib_uint64_t = 0,
    flushed_lsn: ib_uint64_t = 0,
    clean_shutdown: bool = true,
};

const LogFile = struct {
    path: []u8,
    handle: os_file.FileHandle,
};

pub const log_t = struct {
    allocator: std.mem.Allocator = std.heap.page_allocator,
    mutex: std.Thread.Mutex = .{},
    log_dir: []u8 = &[_]u8{},
    log_dir_owned: bool = false,
    n_files: ulint = 0,
    file_size: ib_int64_t = 0,
    files: std.ArrayList(LogFile) = std.ArrayList(LogFile).init(std.heap.page_allocator),
    log_buf: ?[]u8 = null,
    log_buf_used: ulint = 0,
    log_buf_lsn: ib_uint64_t = 0,
    current_lsn: ib_uint64_t = 0,
    flushed_lsn: ib_uint64_t = 0,
    checkpoint_lsn: ib_uint64_t = 0,
    first_header_lsn: ib_uint64_t = 0,
    was_clean_shutdown: bool = true,
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

fn log_write_header(file: *os_file.FileHandle, header: LogHeader) !void {
    var buf: [LOG_FILE_HDR_BYTES]u8 = [_]u8{0} ** LOG_FILE_HDR_BYTES;
    std.mem.writeInt(u32, buf[LOG_HDR_MAGIC_OFF .. LOG_HDR_MAGIC_OFF + 4], header.magic, .big);
    std.mem.writeInt(u32, buf[LOG_HDR_VERSION_OFF .. LOG_HDR_VERSION_OFF + 4], header.version, .big);
    std.mem.writeInt(u64, buf[LOG_HDR_START_LSN_OFF .. LOG_HDR_START_LSN_OFF + 8], header.start_lsn, .big);
    std.mem.writeInt(u64, buf[LOG_HDR_CHECKPOINT_LSN_OFF .. LOG_HDR_CHECKPOINT_LSN_OFF + 8], header.checkpoint_lsn, .big);
    std.mem.writeInt(u64, buf[LOG_HDR_FLUSHED_LSN_OFF .. LOG_HDR_FLUSHED_LSN_OFF + 8], header.flushed_lsn, .big);
    const clean: u32 = if (header.clean_shutdown) 1 else 0;
    std.mem.writeInt(u32, buf[LOG_HDR_CLEAN_SHUTDOWN_OFF .. LOG_HDR_CLEAN_SHUTDOWN_OFF + 4], clean, .big);

    const written = try file.writeAt(buf[0..], 0);
    if (written != buf.len) {
        return LogError.ShortWrite;
    }
}

fn log_read_header(file: *os_file.FileHandle) !LogHeader {
    var buf: [LOG_FILE_HDR_BYTES]u8 = undefined;
    const read = try file.readAt(buf[0..], 0);
    if (read != buf.len) {
        return LogError.ShortRead;
    }
    const magic = std.mem.readInt(u32, buf[LOG_HDR_MAGIC_OFF .. LOG_HDR_MAGIC_OFF + 4], .big);
    const version = std.mem.readInt(u32, buf[LOG_HDR_VERSION_OFF .. LOG_HDR_VERSION_OFF + 4], .big);
    if (magic != LOG_FILE_MAGIC or version != LOG_FILE_VERSION) {
        return LogError.InvalidHeader;
    }
    const start_lsn = std.mem.readInt(u64, buf[LOG_HDR_START_LSN_OFF .. LOG_HDR_START_LSN_OFF + 8], .big);
    const checkpoint_lsn = std.mem.readInt(u64, buf[LOG_HDR_CHECKPOINT_LSN_OFF .. LOG_HDR_CHECKPOINT_LSN_OFF + 8], .big);
    const flushed_lsn = std.mem.readInt(u64, buf[LOG_HDR_FLUSHED_LSN_OFF .. LOG_HDR_FLUSHED_LSN_OFF + 8], .big);
    const clean_val = std.mem.readInt(u32, buf[LOG_HDR_CLEAN_SHUTDOWN_OFF .. LOG_HDR_CLEAN_SHUTDOWN_OFF + 4], .big);
    return .{
        .magic = magic,
        .version = version,
        .start_lsn = start_lsn,
        .checkpoint_lsn = checkpoint_lsn,
        .flushed_lsn = flushed_lsn,
        .clean_shutdown = clean_val != 0,
    };
}

fn log_make_file_name(allocator: std.mem.Allocator, log_dir: []const u8, file_no: ulint) ![]u8 {
    const filename = try std.fmt.allocPrint(allocator, "ib_logfile{d}", .{file_no});
    defer allocator.free(filename);
    if (log_dir.len == 0 or std.mem.eql(u8, log_dir, ".")) {
        return try allocator.dupe(u8, filename);
    }
    return std.fs.path.join(allocator, &.{ log_dir, filename });
}

pub fn log_sys_init(log_dir: []const u8, n_files: ulint, log_file_size: ib_int64_t, log_buffer_size: ulint) ibool {
    if (log_sys != null) {
        return compat.TRUE;
    }
    if (log_file_size <= LOG_FILE_HDR_SIZE) {
        return compat.FALSE;
    }

    const allocator = std.heap.page_allocator;
    const sys = allocator.create(log_t) catch return compat.FALSE;
    sys.* = .{
        .allocator = allocator,
        .log_dir = &[_]u8{},
        .log_dir_owned = false,
        .n_files = n_files,
        .file_size = log_file_size,
        .files = std.ArrayList(LogFile).init(allocator),
        .log_buf = null,
        .log_buf_used = 0,
        .log_buf_lsn = 0,
        .current_lsn = 0,
        .flushed_lsn = 0,
        .checkpoint_lsn = 0,
        .first_header_lsn = 0,
        .was_clean_shutdown = true,
    };
    errdefer {
        log_sys = sys;
        log_sys_close();
    }

    if (!(log_dir.len == 0 or std.mem.eql(u8, log_dir, "."))) {
        std.fs.cwd().makePath(log_dir) catch return compat.FALSE;
    }

    sys.log_dir = allocator.dupe(u8, log_dir) catch return compat.FALSE;
    sys.log_dir_owned = true;

    var header0: LogHeader = .{};
    var header0_valid = false;

    var i: ulint = 0;
    while (i < n_files) : (i += 1) {
        const path = log_make_file_name(allocator, log_dir, i) catch return compat.FALSE;
        var keep_path = false;
        defer if (!keep_path) allocator.free(path);
        const exists = os_file.exists(path);
        const handle = os_file.open(path, if (exists) .open else .create, .read_write) catch return compat.FALSE;
        var keep_handle = false;
        defer if (!keep_handle) handle.close();

        if (!exists) {
            handle.file.setEndPos(@as(u64, @intCast(log_file_size))) catch return compat.FALSE;
            const header = LogHeader{};
            log_write_header(&handle, header) catch return compat.FALSE;
            if (!header0_valid) {
                header0 = header;
                header0_valid = true;
            }
        } else {
            const header = log_read_header(&handle) catch return compat.FALSE;
            if (!header0_valid) {
                header0 = header;
                header0_valid = true;
            }
        }

        sys.files.append(.{ .path = path, .handle = handle }) catch return compat.FALSE;
        keep_handle = true;
        keep_path = true;
    }

    if (!header0_valid) {
        return compat.FALSE;
    }

    sys.first_header_lsn = header0.start_lsn;
    sys.checkpoint_lsn = header0.checkpoint_lsn;
    sys.flushed_lsn = header0.flushed_lsn;
    sys.current_lsn = header0.flushed_lsn;
    sys.was_clean_shutdown = header0.clean_shutdown;

    if (log_buffer_size > 0) {
        sys.log_buf = allocator.alloc(u8, @as(usize, @intCast(log_buffer_size))) catch return compat.FALSE;
    }

    log_sys = sys;
    return compat.TRUE;
}

pub fn log_sys_close() void {
    if (log_sys) |sys| {
        for (sys.files.items) |*file| {
            file.handle.close();
            if (file.path.len > 0) {
                sys.allocator.free(file.path);
            }
        }
        sys.files.deinit();
        if (sys.log_buf) |buf| {
            sys.allocator.free(buf);
        }
        if (sys.log_dir_owned and sys.log_dir.len > 0) {
            sys.allocator.free(sys.log_dir);
        }
        sys.allocator.destroy(sys);
        log_sys = null;
    }
}

fn log_write_bytes(sys: *log_t, lsn: ib_uint64_t, data: []const u8) !void {
    if (data.len == 0) {
        return;
    }
    const capacity = @as(u64, @intCast(sys.file_size - LOG_FILE_HDR_SIZE));
    var remaining = data;
    var cur_lsn = lsn;

    while (remaining.len > 0) {
        var file_offset: ib_int64_t = 0;
        const file_no = log_calc_where_lsn_is(
            &file_offset,
            sys.first_header_lsn,
            cur_lsn,
            sys.n_files,
            sys.file_size,
        );
        if (file_no >= sys.files.items.len) {
            return LogError.InvalidLogFileSize;
        }
        const file = &sys.files.items[@as(usize, @intCast(file_no))].handle;
        const offset = @as(u64, @intCast(file_offset));
        const used = @as(u64, @intCast(file_offset - LOG_FILE_HDR_SIZE));
        const available = capacity - used;
        const chunk_len = @min(@as(u64, @intCast(remaining.len)), available);
        const chunk = remaining[0..@as(usize, @intCast(chunk_len))];
        const written = try file.writeAt(chunk, offset);
        if (written != chunk.len) {
            return LogError.ShortWrite;
        }
        remaining = remaining[chunk.len..];
        cur_lsn += chunk_len;
    }
}

fn log_persist_header(sys: *log_t, clean_shutdown: bool) !void {
    const header = LogHeader{
        .start_lsn = sys.first_header_lsn,
        .checkpoint_lsn = sys.checkpoint_lsn,
        .flushed_lsn = sys.flushed_lsn,
        .clean_shutdown = clean_shutdown,
    };
    for (sys.files.items) |*file| {
        try log_write_header(&file.handle, header);
    }
}

fn log_flush_sys(sys: *log_t) !void {
    if (sys.log_buf == null or sys.log_buf_used == 0) {
        return;
    }
    const buf = sys.log_buf.?;
    try log_write_bytes(sys, sys.log_buf_lsn, buf[0..sys.log_buf_used]);
    sys.flushed_lsn = sys.log_buf_lsn + @as(ib_uint64_t, @intCast(sys.log_buf_used));
    sys.log_buf_used = 0;
    sys.log_buf_lsn = sys.flushed_lsn;
    try log_persist_header(sys, false);
}

pub fn log_flush() ibool {
    const sys = log_sys orelse return compat.FALSE;
    log_flush_sys(sys) catch return compat.FALSE;
    return compat.TRUE;
}

pub fn log_append_bytes(data: []const u8) ?ib_uint64_t {
    const sys = log_sys orelse return null;
    const start_lsn = sys.current_lsn;
    if (sys.log_buf == null or sys.log_buf.?.len == 0) {
        log_write_bytes(sys, start_lsn, data) catch return null;
        sys.current_lsn = start_lsn + @as(ib_uint64_t, @intCast(data.len));
        sys.flushed_lsn = sys.current_lsn;
        log_persist_header(sys, false) catch return null;
        return start_lsn;
    }

    const buf = sys.log_buf.?;
    if (data.len > buf.len) {
        log_flush_sys(sys) catch return null;
        log_write_bytes(sys, start_lsn, data) catch return null;
        sys.current_lsn = start_lsn + @as(ib_uint64_t, @intCast(data.len));
        sys.flushed_lsn = sys.current_lsn;
        log_persist_header(sys, false) catch return null;
        return start_lsn;
    }

    if (sys.log_buf_used == 0) {
        sys.log_buf_lsn = start_lsn;
    }
    if (sys.log_buf_used + data.len > buf.len) {
        log_flush_sys(sys) catch return null;
        sys.log_buf_lsn = sys.current_lsn;
    }

    std.mem.copyForwards(u8, buf[sys.log_buf_used .. sys.log_buf_used + data.len], data);
    sys.log_buf_used += data.len;
    sys.current_lsn += @as(ib_uint64_t, @intCast(data.len));

    if (sys.log_buf_used >= buf.len / LOG_BUF_FLUSH_RATIO) {
        log_flush_sys(sys) catch return null;
    }
    return start_lsn;
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

test "log sys init creates and reopens log headers" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    log_var_init();
    try std.testing.expectEqual(compat.TRUE, log_sys_init(base, 2, 1024 * 1024, 0));
    try std.testing.expect(log_sys != null);

    const path = try log_make_file_name(std.testing.allocator, base, 0);
    defer std.testing.allocator.free(path);
    const handle = try os_file.open(path, .open, .read_write);
    defer handle.close();
    const header = try log_read_header(&handle);
    try std.testing.expectEqual(LOG_FILE_MAGIC, header.magic);
    try std.testing.expectEqual(LOG_FILE_VERSION, header.version);

    const prev_flushed = log_sys.?.flushed_lsn;
    log_sys_close();

    log_var_init();
    try std.testing.expectEqual(compat.TRUE, log_sys_init(base, 2, 1024 * 1024, 0));
    try std.testing.expectEqual(prev_flushed, log_sys.?.flushed_lsn);
    log_sys_close();
}

test "log append writes at header offset" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    log_var_init();
    try std.testing.expectEqual(compat.TRUE, log_sys_init(base, 1, 4096, 0));
    defer log_sys_close();

    const payload = "hello";
    const start = log_append_bytes(payload) orelse return error.UnexpectedNull;
    try std.testing.expectEqual(@as(ib_uint64_t, 0), start);

    const path = try log_make_file_name(std.testing.allocator, base, 0);
    defer std.testing.allocator.free(path);
    const handle = try os_file.open(path, .open, .read_write);
    defer handle.close();
    var buf: [5]u8 = undefined;
    const read = try handle.readAt(buf[0..], @as(u64, @intCast(LOG_FILE_HDR_SIZE)));
    try std.testing.expectEqual(@as(usize, payload.len), read);
    try std.testing.expect(std.mem.eql(u8, buf[0..], payload));
}

test "log append wraps across files" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    log_var_init();
    try std.testing.expectEqual(compat.TRUE, log_sys_init(base, 2, LOG_FILE_HDR_SIZE + 16, 0));
    defer log_sys_close();

    var payload: [24]u8 = undefined;
    for (payload, 0..) |*b, idx| {
        b.* = @as(u8, @intCast(idx));
    }
    _ = log_append_bytes(payload[0..]) orelse return error.UnexpectedNull;

    const path0 = try log_make_file_name(std.testing.allocator, base, 0);
    defer std.testing.allocator.free(path0);
    const handle0 = try os_file.open(path0, .open, .read_write);
    defer handle0.close();
    var buf0: [16]u8 = undefined;
    const read0 = try handle0.readAt(buf0[0..], @as(u64, @intCast(LOG_FILE_HDR_SIZE)));
    try std.testing.expectEqual(@as(usize, 16), read0);
    try std.testing.expect(std.mem.eql(u8, buf0[0..], payload[0..16]));

    const path1 = try log_make_file_name(std.testing.allocator, base, 1);
    defer std.testing.allocator.free(path1);
    const handle1 = try os_file.open(path1, .open, .read_write);
    defer handle1.close();
    var buf1: [8]u8 = undefined;
    const read1 = try handle1.readAt(buf1[0..], @as(u64, @intCast(LOG_FILE_HDR_SIZE)));
    try std.testing.expectEqual(@as(usize, 8), read1);
    try std.testing.expect(std.mem.eql(u8, buf1[0..], payload[16..24]));
}

test "log buffer flush persists flushed lsn" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    log_var_init();
    try std.testing.expectEqual(compat.TRUE, log_sys_init(base, 1, 4096, 64));
    try std.testing.expect(log_sys.?.log_buf != null);

    const payload = "buffered";
    _ = log_append_bytes(payload) orelse return error.UnexpectedNull;
    try std.testing.expectEqual(@as(ib_uint64_t, @intCast(payload.len)), log_sys.?.current_lsn);

    try std.testing.expectEqual(compat.TRUE, log_flush());
    const flushed = log_sys.?.flushed_lsn;
    log_sys_close();

    log_var_init();
    try std.testing.expectEqual(compat.TRUE, log_sys_init(base, 1, 4096, 64));
    try std.testing.expectEqual(flushed, log_sys.?.flushed_lsn);
    log_sys_close();
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
