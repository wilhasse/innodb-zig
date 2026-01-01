const std = @import("std");
const api = @import("../api/api.zig");
const log_mod = @import("../log/mod.zig");
const compat = @import("../ut/compat.zig");

fn expectOk(err: api.ib_err_t) !void {
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
}

test "startup triggers recovery on dirty log header" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    log_mod.log_var_init();
    log_mod.recv_sys_var_init();
    defer log_mod.recv_sys_var_init();

    try std.testing.expectEqual(compat.TRUE, log_mod.log_sys_init(base, 2, 1024 * 1024, 0));
    const payload = [_]u8{ 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00, 0x11 };
    const rec = log_mod.RedoRecord{
        .type_ = log_mod.LOG_REC_PAGE_LSN,
        .space = 1,
        .page_no = 1,
        .payload = payload[0..],
    };
    var rec_buf: [64]u8 = undefined;
    const rec_len = try log_mod.redo_record_encode(rec_buf[0..], rec);
    _ = log_mod.log_append_bytes(rec_buf[0..rec_len]) orelse return error.UnexpectedNull;
    try std.testing.expectEqual(compat.TRUE, log_mod.log_flush());
    log_mod.log_sys_close();

    try expectOk(api.ib_init());
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
    defer log_mod.recv_sys_mem_free();

    const data_dir = try std.fmt.allocPrint(std.testing.allocator, "{s}/", .{base});
    defer std.testing.allocator.free(data_dir);
    try expectOk(api.ib_cfg_set("data_home_dir", data_dir));
    try expectOk(api.ib_cfg_set("log_group_home_dir", base));
    try expectOk(api.ib_cfg_set("log_file_size", @as(api.ib_ulint_t, 1024 * 1024)));
    try expectOk(api.ib_cfg_set("log_files_in_group", @as(api.ib_ulint_t, 2)));
    try expectOk(api.ib_cfg_set("log_buffer_size", @as(api.ib_ulint_t, 256 * 1024)));

    try expectOk(api.ib_startup(null));

    try std.testing.expectEqual(compat.TRUE, log_mod.recv_needed_recovery);
}
