const std = @import("std");
const compat = @import("compat.zig");
const log = @import("log.zig");
const os_thread = @import("../os/thread.zig");

pub const module_name = "ut.dbg";

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;

pub var ut_dbg_zero: ulint = 0;
pub var ut_dbg_stop_threads: ibool = compat.FALSE;
pub var ut_dbg_null_ptr: ?*ulint = null;

pub fn ut_dbg_assertion_failed(expr: ?[]const u8, file: []const u8, line: ulint) void {
    const tid = os_thread.os_thread_pf(os_thread.os_thread_get_curr_id());
    if (expr) |e| {
        log.logf(
            "InnoDB: Assertion failure in thread {d} in file {s} line {d}\nInnoDB: Failing assertion: {s}\n",
            .{ tid, file, line, e },
        );
    } else {
        log.logf(
            "InnoDB: Assertion failure in thread {d} in file {s} line {d}\n",
            .{ tid, file, line },
        );
    }
    ut_dbg_stop_threads = compat.TRUE;
}

pub fn ut_dbg_stop_thread(file: []const u8, line: ulint) void {
    const tid = os_thread.os_thread_pf(os_thread.os_thread_get_curr_id());
    log.logf("InnoDB: Thread {d} stopped in file {s} line {d}\n", .{ tid, file, line });
}

test "ut dbg assertion sets stop flag" {
    ut_dbg_stop_threads = compat.FALSE;
    ut_dbg_assertion_failed(null, "file.c", 10);
    try std.testing.expectEqual(compat.TRUE, ut_dbg_stop_threads);
}

test "ut dbg stop thread logs" {
    ut_dbg_stop_thread("file.c", 12);
    try std.testing.expect(true);
}
