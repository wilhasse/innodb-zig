const std = @import("std");
const compat = @import("../ut/compat.zig");
const dyn = @import("../dyn/mod.zig");
const mach = @import("../mach/mod.zig");
const log = @import("../log/mod.zig");

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
    mtr.state = MTR_ACTIVE;
    mtr.magic_n = MTR_MAGIC_N;
    return mtr;
}

pub fn mtr_commit(mtr: *mtr_t) void {
    std.debug.assert(mtr.magic_n == MTR_MAGIC_N);
    std.debug.assert(mtr.state == MTR_ACTIVE);
    mtr.state = MTR_COMMITTING;
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

pub fn mlog_catenate_string(mtr: *mtr_t, str: [*]const byte, len: ulint) void {
    if (mtr_get_log_mode(mtr) == MTR_LOG_NONE) {
        return;
    }
    if (mtr.log.magic_n != dyn.DYN_BLOCK_MAGIC_N) {
        _ = dyn.dyn_array_create(&mtr.log);
    }
    dyn.dyn_push_string(&mtr.log, str, len);
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
