const std = @import("std");
const buf_mod = @import("../buf/mod.zig");
const log_mod = @import("../log/mod.zig");
const compat = @import("../ut/compat.zig");

test "adaptive flush tick clears dirty pages and checkpoints" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    log_mod.log_var_init();
    defer log_mod.log_var_init();

    try std.testing.expectEqual(compat.TRUE, log_mod.log_sys_init(base, 1, 4096, 128));
    defer log_mod.log_sys_close();

    buf_mod.buf_buddy_var_init();
    buf_mod.buf_LRU_var_init();
    buf_mod.buf_var_init();
    _ = buf_mod.buf_pool_init();
    defer buf_mod.buf_mem_free();

    var mtr = buf_mod.mtr_t{};
    const block = buf_mod.buf_page_get_gen(1, 0, 1, 0, null, buf_mod.BUF_GET, "adaptive", 0, &mtr) orelse return error.OutOfMemory;
    buf_mod.buf_page_set_dirty(&block.page);
    try std.testing.expect(buf_mod.buf_pool.?.dirty_pages > 0);

    log_mod.log_set_adaptive_flushing(compat.TRUE);
    log_mod.log_set_adaptive_flush_callback(buf_mod.buf_adaptive_flush);

    _ = log_mod.log_append_bytes("tick") orelse return error.UnexpectedNull;
    log_mod.log_writer_tick();

    try std.testing.expectEqual(@as(compat.ulint, 0), buf_mod.buf_pool.?.dirty_pages);
    const sys = log_mod.log_sys orelse return error.UnexpectedNull;
    try std.testing.expectEqual(sys.flushed_lsn, sys.checkpoint_lsn);
}
