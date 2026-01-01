const std = @import("std");
const compat = @import("../ut/compat.zig");
const dyn = @import("../dyn/mod.zig");
const mach = @import("../mach/mod.zig");
const log = @import("../log/mod.zig");
const fil = @import("../fil/mod.zig");

pub const module_name = "mtr";

pub const byte = compat.byte;
pub const ulint = compat.ulint;
pub const dulint = compat.Dulint;
pub const ibool = compat.ibool;

pub const MTR_LOG_ALL: ulint = 21;
pub const MTR_LOG_NONE: ulint = 22;
pub const MTR_LOG_SHORT_INSERTS: ulint = 24;

pub const MTR_ACTIVE: ulint = 12231;
pub const MTR_COMMITTING: ulint = 56456;
pub const MTR_COMMITTED: ulint = 34676;
pub const MTR_MAGIC_N: ulint = 54551;

pub const mtr_memo_slot_t = struct {
    type_: ulint,
    object: *anyopaque,
};

pub const MLOG_SINGLE_REC_FLAG: byte = 128;
pub const MLOG_1BYTE: ulint = 1;
pub const MLOG_2BYTES: ulint = 2;
pub const MLOG_4BYTES: ulint = 4;
pub const MLOG_8BYTES: ulint = 8;
pub const MLOG_MULTI_REC_END: byte = 31;
pub const MLOG_WRITE_STRING: byte = 30;
pub const MLOG_FILE_CREATE: byte = 33;
pub const MLOG_FILE_RENAME: byte = 34;
pub const MLOG_FILE_DELETE: byte = 35;
pub const MLOG_BIGGEST_TYPE: byte = 51;

pub const mtr_t = struct {
    state: ulint = MTR_ACTIVE,
    memo: dyn.dyn_array_t = undefined,
    log_mode: ulint = MTR_LOG_ALL,
    log: dyn.dyn_array_t = undefined,
    modifications: ibool = compat.FALSE,
    n_log_recs: ulint = 0,
    start_lsn: u64 = 0,
    end_lsn: u64 = 0,
    magic_n: ulint = MTR_MAGIC_N,
};

pub fn mtr_init(mtr: *mtr_t) void {
    _ = mtr_start(mtr);
}

pub fn mtr_start(mtr: *mtr_t) *mtr_t {
    _ = dyn.dyn_array_create(&mtr.memo);
    _ = dyn.dyn_array_create(&mtr.log);
    mtr.log_mode = MTR_LOG_ALL;
    mtr.modifications = compat.FALSE;
    mtr.n_log_recs = 0;
    mtr.start_lsn = 0;
    mtr.end_lsn = 0;
    mtr.state = MTR_ACTIVE;
    mtr.magic_n = MTR_MAGIC_N;
    return mtr;
}

fn mtr_log_reserve_and_write(mtr: *mtr_t) void {
    const mlog = &mtr.log;
    if (mtr.n_log_recs > 1) {
        mlog_catenate_ulint(mtr, MLOG_MULTI_REC_END, MLOG_1BYTE);
    } else {
        const first = dyn.dyn_block_get_data(mlog);
        first[0] = @as(byte, @intCast(@as(ulint, first[0]) | MLOG_SINGLE_REC_FLAG));
    }

    var total: u64 = 0;
    var start_lsn: ?u64 = null;
    var block: ?*dyn.dyn_block_t = dyn.dyn_array_get_first_block(mlog);
    while (block) |cur| {
        const used = dyn.dyn_block_get_used(cur);
        if (used > 0) {
            const data = dyn.dyn_block_get_data(cur)[0..@as(usize, @intCast(used))];
            const lsn = log.log_append_bytes(data) orelse return;
            if (start_lsn == null) {
                start_lsn = lsn;
            }
            total += used;
        }
        block = dyn.dyn_array_get_next_block(mlog, cur);
    }
    if (start_lsn) |lsn| {
        mtr.start_lsn = lsn;
        mtr.end_lsn = lsn + total;
    }
}

pub fn mtr_commit(mtr: *mtr_t) void {
    std.debug.assert(mtr.magic_n == MTR_MAGIC_N);
    std.debug.assert(mtr.state == MTR_ACTIVE);
    mtr.state = MTR_COMMITTING;
    if (mtr.modifications == compat.TRUE and mtr.n_log_recs > 0 and mtr.log_mode != MTR_LOG_NONE) {
        if (log.recv_no_log_write == compat.FALSE and log.log_sys != null) {
            if (mtr.log.magic_n == dyn.DYN_BLOCK_MAGIC_N and dyn.dyn_array_get_data_size(&mtr.log) > 0) {
                mtr_log_reserve_and_write(mtr);
            }
        }
    }
    if (mtr.memo.magic_n == dyn.DYN_BLOCK_MAGIC_N) {
        dyn.dyn_array_free(&mtr.memo);
    }
    if (mtr.log.magic_n == dyn.DYN_BLOCK_MAGIC_N) {
        dyn.dyn_array_free(&mtr.log);
    }
    mtr.state = MTR_COMMITTED;
}

pub fn mtr_get_log_mode(mtr: *const mtr_t) ulint {
    return mtr.log_mode;
}

pub fn mtr_set_log_mode(mtr: *mtr_t, mode: ulint) ulint {
    const old = mtr.log_mode;
    mtr.log_mode = mode;
    return old;
}

pub fn mtr_memo_push(mtr: *mtr_t, object: *anyopaque, type_: ulint) void {
    std.debug.assert(mtr.magic_n == MTR_MAGIC_N);
    const slot_buf = dyn.dyn_array_push(&mtr.memo, @sizeOf(mtr_memo_slot_t));
    const slot = @as(*mtr_memo_slot_t, @ptrCast(@alignCast(slot_buf)));
    slot.* = .{ .type_ = type_, .object = object };
}

pub fn mtr_set_savepoint(mtr: *mtr_t) ulint {
    std.debug.assert(mtr.magic_n == MTR_MAGIC_N);
    return dyn.dyn_array_get_data_size(&mtr.memo);
}

fn page_base_ptr(ptr: [*]const byte) [*]const byte {
    const addr = @intFromPtr(ptr);
    const mask = @as(usize, @intCast(compat.UNIV_PAGE_SIZE - 1));
    return @as([*]const byte, @ptrFromInt(addr & ~mask));
}

fn page_offset(ptr: [*]const byte) ulint {
    const addr = @intFromPtr(ptr);
    const mask = @as(usize, @intCast(compat.UNIV_PAGE_SIZE - 1));
    return @as(ulint, @intCast(addr & mask));
}

pub fn mlog_open(mtr: *mtr_t, size: ulint) ?[*]byte {
    std.debug.assert(mtr.magic_n == MTR_MAGIC_N);
    std.debug.assert(size > 0);
    std.debug.assert(size < dyn.DYN_ARRAY_DATA_SIZE);
    mtr.modifications = compat.TRUE;
    if (mtr_get_log_mode(mtr) == MTR_LOG_NONE) {
        return null;
    }
    if (mtr.log.magic_n != dyn.DYN_BLOCK_MAGIC_N) {
        _ = dyn.dyn_array_create(&mtr.log);
    }
    return dyn.dyn_array_open(&mtr.log, size);
}

pub fn mlog_close(mtr: *mtr_t, ptr: [*]byte) void {
    std.debug.assert(mtr.magic_n == MTR_MAGIC_N);
    std.debug.assert(mtr_get_log_mode(mtr) != MTR_LOG_NONE);
    dyn.dyn_array_close(&mtr.log, ptr);
}

pub fn mlog_catenate_ulint(mtr: *mtr_t, val: ulint, type_: ulint) void {
    if (mtr_get_log_mode(mtr) == MTR_LOG_NONE) {
        return;
    }
    if (mtr.log.magic_n != dyn.DYN_BLOCK_MAGIC_N) {
        _ = dyn.dyn_array_create(&mtr.log);
    }
    const ptr = dyn.dyn_array_push(&mtr.log, type_);
    switch (type_) {
        MLOG_1BYTE => mach.mach_write_to_1(ptr, val),
        MLOG_2BYTES => mach.mach_write_to_2(ptr, val),
        MLOG_4BYTES => mach.mach_write_to_4(ptr, val),
        else => std.debug.panic("mlog_catenate_ulint: invalid type {d}", .{type_}),
    }
}

pub fn mlog_catenate_ulint_compressed(mtr: *mtr_t, val: ulint) void {
    const log_ptr = mlog_open(mtr, 10) orelse return;
    const next = log_ptr + mach.mach_write_compressed(log_ptr, val);
    mlog_close(mtr, next);
}

pub fn mlog_catenate_dulint_compressed(mtr: *mtr_t, val: dulint) void {
    const log_ptr = mlog_open(mtr, 15) orelse return;
    const next = log_ptr + mach.mach_dulint_write_compressed(log_ptr, val);
    mlog_close(mtr, next);
}

pub fn mlog_catenate_string(mtr: *mtr_t, str: [*]const byte, len: ulint) void {
    if (mtr_get_log_mode(mtr) == MTR_LOG_NONE) {
        return;
    }
    if (mtr.log.magic_n != dyn.DYN_BLOCK_MAGIC_N) {
        _ = dyn.dyn_array_create(&mtr.log);
    }
    dyn.dyn_push_string(&mtr.log, str, len);
}

pub fn mlog_write_initial_log_record_fast(
    ptr: [*]const byte,
    type_: byte,
    log_ptr: [*]byte,
    mtr: *mtr_t,
) [*]byte {
    std.debug.assert(mtr.magic_n == MTR_MAGIC_N);
    std.debug.assert(type_ <= MLOG_BIGGEST_TYPE);
    const page = page_base_ptr(ptr);
    const space = mach.mach_read_from_4(page + fil.FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID);
    const page_no = mach.mach_read_from_4(page + fil.FIL_PAGE_OFFSET);
    mach.mach_write_to_1(log_ptr, type_);
    var next = log_ptr + 1;
    next += mach.mach_write_compressed(next, space);
    next += mach.mach_write_compressed(next, page_no);
    mtr.n_log_recs += 1;
    return next;
}

pub fn mlog_write_initial_log_record(ptr: [*]const byte, type_: byte, mtr: *mtr_t) void {
    std.debug.assert(type_ <= MLOG_BIGGEST_TYPE);
    std.debug.assert(type_ > MLOG_8BYTES);
    const log_ptr = mlog_open(mtr, 11) orelse return;
    const next = mlog_write_initial_log_record_fast(ptr, type_, log_ptr, mtr);
    mlog_close(mtr, next);
}

pub fn mlog_write_initial_log_record_for_file_op(
    type_: ulint,
    space_id: ulint,
    page_no: ulint,
    log_ptr: [*]byte,
    mtr: *mtr_t,
) [*]byte {
    std.debug.assert(mtr.magic_n == MTR_MAGIC_N);
    mach.mach_write_to_1(log_ptr, @as(byte, @intCast(type_)));
    var next = log_ptr + 1;
    next += mach.mach_write_compressed(next, space_id);
    next += mach.mach_write_compressed(next, page_no);
    mtr.n_log_recs += 1;
    return next;
}

pub fn mlog_write_ulint(ptr: [*]byte, val: ulint, type_: byte, mtr: *mtr_t) void {
    switch (type_) {
        MLOG_1BYTE => mach.mach_write_to_1(ptr, val),
        MLOG_2BYTES => mach.mach_write_to_2(ptr, val),
        MLOG_4BYTES => mach.mach_write_to_4(ptr, val),
        else => std.debug.panic("mlog_write_ulint: invalid type {d}", .{type_}),
    }

    const log_ptr = mlog_open(mtr, 11 + 2 + 5) orelse return;
    var next = mlog_write_initial_log_record_fast(ptr, type_, log_ptr, mtr);
    mach.mach_write_to_2(next, page_offset(ptr));
    next += 2;
    next += mach.mach_write_compressed(next, val);
    mlog_close(mtr, next);
}

pub fn mlog_write_dulint(ptr: [*]byte, val: dulint, mtr: *mtr_t) void {
    mach.mach_write_to_8(ptr, val);
    const log_ptr = mlog_open(mtr, 11 + 2 + 9) orelse return;
    var next = mlog_write_initial_log_record_fast(ptr, MLOG_8BYTES, log_ptr, mtr);
    mach.mach_write_to_2(next, page_offset(ptr));
    next += 2;
    next += mach.mach_dulint_write_compressed(next, val);
    mlog_close(mtr, next);
}

pub fn mlog_write_string(ptr: [*]byte, str: [*]const byte, len: ulint, mtr: *mtr_t) void {
    std.debug.assert(len <= compat.UNIV_PAGE_SIZE);
    const dst = ptr[0..@as(usize, @intCast(len))];
    const src = str[0..@as(usize, @intCast(len))];
    std.mem.copyForwards(byte, dst, src);
    mlog_log_string(ptr, len, mtr);
}

pub fn mlog_log_string(ptr: [*]byte, len: ulint, mtr: *mtr_t) void {
    std.debug.assert(len <= compat.UNIV_PAGE_SIZE);
    const log_ptr = mlog_open(mtr, 30) orelse return;
    var next = mlog_write_initial_log_record_fast(ptr, MLOG_WRITE_STRING, log_ptr, mtr);
    mach.mach_write_to_2(next, page_offset(ptr));
    next += 2;
    mach.mach_write_to_2(next, len);
    next += 2;
    mlog_close(mtr, next);
    mlog_catenate_string(mtr, ptr, len);
}

pub fn mlog_parse_initial_log_record(
    ptr: [*]byte,
    end_ptr: [*]byte,
    type_out: *byte,
    space: *ulint,
    page_no: *ulint,
) ?[*]byte {
    if (@intFromPtr(end_ptr) < @intFromPtr(ptr + 1)) {
        return null;
    }

    type_out.* = @as(byte, @intCast(@as(ulint, ptr[0]) & ~@as(ulint, MLOG_SINGLE_REC_FLAG)));
    std.debug.assert(type_out.* <= MLOG_BIGGEST_TYPE);
    var next = ptr + 1;

    if (@intFromPtr(end_ptr) < @intFromPtr(next + 2)) {
        return null;
    }

    next = mach.mach_parse_compressed(next, end_ptr, space) orelse return null;
    next = mach.mach_parse_compressed(next, end_ptr, page_no) orelse return null;
    return next;
}

pub fn mlog_parse_nbytes(
    type_: ulint,
    ptr: [*]byte,
    end_ptr: [*]byte,
    page: ?[*]byte,
    page_zip: ?*anyopaque,
) ?[*]byte {
    _ = page_zip;
    std.debug.assert(type_ <= MLOG_8BYTES);

    if (@intFromPtr(end_ptr) < @intFromPtr(ptr + 2)) {
        return null;
    }

    const offset = mach.mach_read_from_2(ptr);
    var next = ptr + 2;

    if (offset >= compat.UNIV_PAGE_SIZE) {
        if (log.recv_sys) |sys| {
            sys.found_corrupt_log = compat.TRUE;
        }
        return null;
    }

    if (type_ == MLOG_8BYTES) {
        var dval: dulint = .{ .high = 0, .low = 0 };
        next = mach.mach_dulint_parse_compressed(next, end_ptr, &dval) orelse return null;
        if (page) |buf| {
            mach.mach_write_to_8(buf + offset, dval);
        }
        return next;
    }

    var val: ulint = 0;
    next = mach.mach_parse_compressed(next, end_ptr, &val) orelse return null;

    switch (type_) {
        MLOG_1BYTE => {
            if (val > 0xFF) {
                if (log.recv_sys) |sys| {
                    sys.found_corrupt_log = compat.TRUE;
                }
                return null;
            }
            if (page) |buf| {
                mach.mach_write_to_1(buf + offset, val);
            }
        },
        MLOG_2BYTES => {
            if (val > 0xFFFF) {
                if (log.recv_sys) |sys| {
                    sys.found_corrupt_log = compat.TRUE;
                }
                return null;
            }
            if (page) |buf| {
                mach.mach_write_to_2(buf + offset, val);
            }
        },
        MLOG_4BYTES => {
            if (page) |buf| {
                mach.mach_write_to_4(buf + offset, val);
            }
        },
        else => return null,
    }

    return next;
}

pub fn mlog_parse_string(
    ptr: [*]byte,
    end_ptr: [*]byte,
    page: ?[*]byte,
    page_zip: ?*anyopaque,
) ?[*]byte {
    _ = page_zip;
    if (@intFromPtr(end_ptr) < @intFromPtr(ptr + 4)) {
        return null;
    }
    const offset = mach.mach_read_from_2(ptr);
    const len = mach.mach_read_from_2(ptr + 2);
    const next = ptr + 4;
    if (offset >= compat.UNIV_PAGE_SIZE or len + offset > compat.UNIV_PAGE_SIZE) {
        if (log.recv_sys) |sys| {
            sys.found_corrupt_log = compat.TRUE;
        }
        return null;
    }
    if (@intFromPtr(end_ptr) < @intFromPtr(next + len)) {
        return null;
    }
    if (page) |buf| {
        const dst = buf[offset..offset + len];
        const src = next[0..len];
        std.mem.copyForwards(byte, dst, src);
    }
    return next + len;
}

test "mlog_parse_initial_log_record reads header fields" {
    var buf: [16]byte = .{0} ** 16;
    buf[0] = MLOG_1BYTE;
    var pos = mach.mach_write_compressed(buf[1..].ptr, 3);
    pos += mach.mach_write_compressed(buf[1 + pos ..].ptr, 7);

    var out_type: byte = 0;
    var space: ulint = 0;
    var page_no: ulint = 0;
    const end_ptr = buf[1 + pos ..].ptr;
    const res = mlog_parse_initial_log_record(buf[0..].ptr, end_ptr, &out_type, &space, &page_no);
    try std.testing.expect(res == end_ptr);
    try std.testing.expect(out_type == MLOG_1BYTE);
    try std.testing.expect(space == 3);
    try std.testing.expect(page_no == 7);
}

test "mlog_parse_nbytes writes page data" {
    var page = [_]byte{0} ** compat.UNIV_PAGE_SIZE;
    var buf: [16]byte = .{0} ** 16;
    mach.mach_write_to_2(buf[0..].ptr, 4);
    _ = mach.mach_write_compressed(buf[2..].ptr, 0xAA);

    const end_ptr = buf[3..].ptr;
    const res = mlog_parse_nbytes(MLOG_1BYTE, buf[0..].ptr, end_ptr, page[0..].ptr, null);
    try std.testing.expect(res != null);
    try std.testing.expect(page[4] == 0xAA);
}

test "mlog_parse_nbytes marks corrupt on bad offset" {
    var recv_sys_local = log.recv_sys_t{};
    log.recv_sys = &recv_sys_local;
    defer log.recv_sys = null;

    var buf: [8]byte = .{0} ** 8;
    mach.mach_write_to_2(buf[0..].ptr, compat.UNIV_PAGE_SIZE);
    _ = mach.mach_write_compressed(buf[2..].ptr, 1);

    recv_sys_local.found_corrupt_log = compat.FALSE;
    const res = mlog_parse_nbytes(MLOG_1BYTE, buf[0..].ptr, buf[0..].ptr + buf.len, null, null);
    try std.testing.expect(res == null);
    try std.testing.expect(recv_sys_local.found_corrupt_log == compat.TRUE);
}

test "mtr start/commit and memo push" {
    var mtr = mtr_t{};
    _ = mtr_start(&mtr);
    try std.testing.expect(mtr.state == MTR_ACTIVE);
    try std.testing.expect(mtr_get_log_mode(&mtr) == MTR_LOG_ALL);

    var dummy: u32 = 0;
    mtr_memo_push(&mtr, @ptrCast(&dummy), 1);
    const savepoint = mtr_set_savepoint(&mtr);
    try std.testing.expect(savepoint == @sizeOf(mtr_memo_slot_t));

    _ = mtr_set_log_mode(&mtr, MTR_LOG_NONE);
    try std.testing.expect(mtr_get_log_mode(&mtr) == MTR_LOG_NONE);

    mtr_commit(&mtr);
    try std.testing.expect(mtr.state == MTR_COMMITTED);
}

test "mtr commit writes log bytes to buffer" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    log.log_var_init();
    defer log.log_var_init();

    try std.testing.expectEqual(compat.TRUE, log.log_sys_init(base, 1, 1024 * 1024, 256));
    defer log.log_sys_close();

    const allocator = std.testing.allocator;
    const page_mem = try allocator.alignedAlloc(u8, compat.UNIV_PAGE_SIZE, compat.UNIV_PAGE_SIZE);
    defer allocator.free(page_mem);
    @memset(page_mem, 0);

    mach.mach_write_to_4(page_mem.ptr + fil.FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID, 5);
    mach.mach_write_to_4(page_mem.ptr + fil.FIL_PAGE_OFFSET, 17);

    var mtr = mtr_t{};
    _ = mtr_start(&mtr);
    const target = page_mem.ptr + 128;
    mlog_write_ulint(target, 0xCC, MLOG_1BYTE, &mtr);
    mtr_commit(&mtr);

    const sys = log.log_sys orelse return error.UnexpectedNull;
    try std.testing.expect(sys.log_buf_used > 0);
    try std.testing.expect(mtr.start_lsn != 0);
    try std.testing.expect(mtr.end_lsn >= mtr.start_lsn);

    const buf = sys.log_buf.?[0..@as(usize, @intCast(sys.log_buf_used))];
    var expected: [32]u8 = undefined;
    var pos: usize = 0;
    expected[pos] = @as(byte, @intCast(MLOG_SINGLE_REC_FLAG | MLOG_1BYTE));
    pos += 1;
    pos += @as(usize, @intCast(mach.mach_write_compressed(expected[pos..].ptr, 5)));
    pos += @as(usize, @intCast(mach.mach_write_compressed(expected[pos..].ptr, 17)));
    mach.mach_write_to_2(expected[pos..].ptr, 128);
    pos += 2;
    pos += @as(usize, @intCast(mach.mach_write_compressed(expected[pos..].ptr, 0xCC)));

    try std.testing.expectEqualSlices(u8, expected[0..pos], buf[0..pos]);
}

fn mlog_collect_bytes(allocator: std.mem.Allocator, arr: *dyn.dyn_array_t) ![]u8 {
    const size = dyn.dyn_array_get_data_size(arr);
    const buf = try allocator.alloc(u8, @as(usize, @intCast(size)));
    var i: ulint = 0;
    while (i < size) : (i += 1) {
        buf[@as(usize, @intCast(i))] = dyn.dyn_array_get_element(arr, i)[0];
    }
    return buf;
}

test "mlog_write_ulint logs page update" {
    const allocator = std.testing.allocator;
    const page_mem = try allocator.alignedAlloc(u8, compat.UNIV_PAGE_SIZE, compat.UNIV_PAGE_SIZE);
    defer allocator.free(page_mem);
    @memset(page_mem, 0);

    mach.mach_write_to_4(page_mem.ptr + fil.FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID, 7);
    mach.mach_write_to_4(page_mem.ptr + fil.FIL_PAGE_OFFSET, 42);

    var mtr = mtr_t{};
    _ = mtr_start(&mtr);
    defer mtr_commit(&mtr);

    const target = page_mem.ptr + 128;
    mlog_write_ulint(target, 0xAB, MLOG_1BYTE, &mtr);
    try std.testing.expectEqual(@as(byte, 0xAB), target[0]);

    const log_bytes = try mlog_collect_bytes(allocator, &mtr.log);
    defer allocator.free(log_bytes);

    var out_type: byte = 0;
    var out_space: ulint = 0;
    var out_page: ulint = 0;
    const end_ptr = log_bytes.ptr + log_bytes.len;
    const after_hdr = mlog_parse_initial_log_record(log_bytes.ptr, end_ptr, &out_type, &out_space, &out_page) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(byte, MLOG_1BYTE), out_type);
    try std.testing.expectEqual(@as(ulint, 7), out_space);
    try std.testing.expectEqual(@as(ulint, 42), out_page);

    var apply_page = [_]byte{0} ** compat.UNIV_PAGE_SIZE;
    const after = mlog_parse_nbytes(out_type, after_hdr, end_ptr, apply_page[0..].ptr, null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(after == end_ptr);
    try std.testing.expectEqual(@as(byte, 0xAB), apply_page[128]);
}

test "mlog_write_dulint logs 8-byte update" {
    const allocator = std.testing.allocator;
    const page_mem = try allocator.alignedAlloc(u8, compat.UNIV_PAGE_SIZE, compat.UNIV_PAGE_SIZE);
    defer allocator.free(page_mem);
    @memset(page_mem, 0);

    mach.mach_write_to_4(page_mem.ptr + fil.FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID, 3);
    mach.mach_write_to_4(page_mem.ptr + fil.FIL_PAGE_OFFSET, 9);

    var mtr = mtr_t{};
    _ = mtr_start(&mtr);
    defer mtr_commit(&mtr);

    const target = page_mem.ptr + 256;
    const value = dulint{ .high = 0x1, .low = 0x22334455 };
    mlog_write_dulint(target, value, &mtr);
    const stored = mach.mach_read_from_8(target);
    try std.testing.expectEqual(value.high, stored.high);
    try std.testing.expectEqual(value.low, stored.low);

    const log_bytes = try mlog_collect_bytes(allocator, &mtr.log);
    defer allocator.free(log_bytes);

    var out_type: byte = 0;
    var out_space: ulint = 0;
    var out_page: ulint = 0;
    const end_ptr = log_bytes.ptr + log_bytes.len;
    const after_hdr = mlog_parse_initial_log_record(log_bytes.ptr, end_ptr, &out_type, &out_space, &out_page) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(byte, MLOG_8BYTES), out_type);
    try std.testing.expectEqual(@as(ulint, 3), out_space);
    try std.testing.expectEqual(@as(ulint, 9), out_page);

    var apply_page = [_]byte{0} ** compat.UNIV_PAGE_SIZE;
    const after = mlog_parse_nbytes(out_type, after_hdr, end_ptr, apply_page[0..].ptr, null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(after == end_ptr);
    const out = mach.mach_read_from_8(apply_page[0..].ptr + 256);
    try std.testing.expectEqual(value.high, out.high);
    try std.testing.expectEqual(value.low, out.low);
}

test "mlog_write_string logs payload" {
    const allocator = std.testing.allocator;
    const page_mem = try allocator.alignedAlloc(u8, compat.UNIV_PAGE_SIZE, compat.UNIV_PAGE_SIZE);
    defer allocator.free(page_mem);
    @memset(page_mem, 0);

    mach.mach_write_to_4(page_mem.ptr + fil.FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID, 2);
    mach.mach_write_to_4(page_mem.ptr + fil.FIL_PAGE_OFFSET, 7);

    var mtr = mtr_t{};
    _ = mtr_start(&mtr);
    defer mtr_commit(&mtr);

    const msg = "redo";
    const target = page_mem.ptr + 64;
    mlog_write_string(target, msg.ptr, msg.len, &mtr);
    try std.testing.expectEqualStrings(msg, target[0..msg.len]);

    const log_bytes = try mlog_collect_bytes(allocator, &mtr.log);
    defer allocator.free(log_bytes);

    var out_type: byte = 0;
    var out_space: ulint = 0;
    var out_page: ulint = 0;
    const end_ptr = log_bytes.ptr + log_bytes.len;
    const after_hdr = mlog_parse_initial_log_record(log_bytes.ptr, end_ptr, &out_type, &out_space, &out_page) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(byte, MLOG_WRITE_STRING), out_type);
    try std.testing.expectEqual(@as(ulint, 2), out_space);
    try std.testing.expectEqual(@as(ulint, 7), out_page);

    var apply_page = [_]byte{0} ** compat.UNIV_PAGE_SIZE;
    const after = mlog_parse_string(after_hdr, end_ptr, apply_page[0..].ptr, null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(after == end_ptr);
    try std.testing.expectEqualStrings(msg, apply_page[64 .. 64 + msg.len]);
}
