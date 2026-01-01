const std = @import("std");
const log_mod = @import("../log/mod.zig");
const mtr_mod = @import("../mtr/mod.zig");
const mach = @import("../mach/mod.zig");
const fil = @import("../fil/mod.zig");
const compat = @import("../ut/compat.zig");

test "redo recovery applies MLOG updates to page" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    log_mod.log_var_init();
    log_mod.recv_sys_var_init();
    defer log_mod.recv_sys_mem_free();

    try std.testing.expectEqual(compat.TRUE, log_mod.log_sys_init(base, 1, 4096, 0));

    const allocator = std.testing.allocator;
    const page_mem = try allocator.alignedAlloc(
        u8,
        std.mem.Alignment.fromByteUnits(compat.UNIV_PAGE_SIZE),
        compat.UNIV_PAGE_SIZE,
    );
    defer allocator.free(page_mem);
    @memset(page_mem, 0);
    mach.mach_write_to_4(page_mem.ptr + fil.FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID, 1);
    mach.mach_write_to_4(page_mem.ptr + fil.FIL_PAGE_OFFSET, 7);

    var mtr = mtr_mod.mtr_t{};
    _ = mtr_mod.mtr_start(&mtr);
    mtr_mod.mlog_write_ulint(page_mem.ptr + 200, 0x5A, mtr_mod.MLOG_1BYTE, &mtr);
    mtr_mod.mtr_commit(&mtr);

    try std.testing.expectEqual(compat.TRUE, log_mod.log_flush());
    log_mod.log_sys_close();

    log_mod.log_var_init();
    try std.testing.expectEqual(compat.TRUE, log_mod.log_sys_init(base, 1, 4096, 0));
    try std.testing.expectEqual(compat.TRUE, log_mod.recv_scan_log_recs(1024));

    var apply_page = [_]u8{0} ** compat.UNIV_PAGE_SIZE;
    try std.testing.expectEqual(compat.TRUE, log_mod.recv_apply_log_recs(1, 7, apply_page[0..].ptr));
    try std.testing.expectEqual(@as(u8, 0x5A), apply_page[200]);

    log_mod.log_sys_close();
}
