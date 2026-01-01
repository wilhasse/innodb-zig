const std = @import("std");
const compat = @import("../ut/compat.zig");
const fil = @import("../fil/mod.zig");
const fil_sys = @import("../fil/sys.zig");
const fsp = @import("../fsp/mod.zig");
const trx_sys = @import("../trx/sys.zig");

test "doublewrite writes page into system buffer" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    const prev_path = fil.fil_path_to_client_datadir;
    fil.fil_path_to_client_datadir = base;
    defer fil.fil_path_to_client_datadir = prev_path;

    fil.fil_var_init();
    fil.fil_init(0, 16);
    defer fil.fil_close();

    fsp.fsp_init();
    try std.testing.expectEqual(compat.TRUE, fil_sys.openOrCreateSystemTablespace(base, "ibdata1:4M:autoextend"));

    var space_id: fil.ulint = 0;
    const create_err = fil.fil_create_new_single_table_tablespace(&space_id, "dw_table", compat.FALSE, 0, 4);
    try std.testing.expectEqual(fil.DB_SUCCESS, create_err);

    trx_sys.trx_doublewrite_set_enabled(compat.TRUE);
    trx_sys.trx_doublewrite_init_default(std.testing.allocator);
    defer {
        if (trx_sys.trx_doublewrite) |dw| std.testing.allocator.destroy(dw);
        trx_sys.trx_doublewrite = null;
    }
    fil.fil_set_doublewrite_handler(trx_sys.trx_doublewrite_write_page);
    defer fil.fil_set_doublewrite_handler(null);

    var page = [_]u8{0xAB} ** compat.UNIV_PAGE_SIZE;
    try std.testing.expectEqual(fil.DB_SUCCESS, fil.fil_write_page(space_id, 0, page[0..].ptr));

    const dw = trx_sys.trx_doublewrite orelse return error.UnexpectedNull;
    var read_buf = [_]u8{0} ** compat.UNIV_PAGE_SIZE;
    try std.testing.expectEqual(fil.DB_SUCCESS, fil.fil_read_page(0, dw.block1, read_buf[0..].ptr));
    try std.testing.expectEqual(@as(u8, 0xAB), read_buf[0]);
}
