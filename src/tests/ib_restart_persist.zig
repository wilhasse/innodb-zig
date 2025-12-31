const std = @import("std");
const btr = @import("../btr/mod.zig");
const dict = @import("../dict/mod.zig");
const fil = @import("../fil/mod.zig");
const buf = @import("../buf/mod.zig");
const compat = @import("../ut/compat.zig");

test "restart persistence via btr/fil" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);
    const prev_path = fil.fil_path_to_client_datadir;
    fil.fil_path_to_client_datadir = base;
    defer fil.fil_path_to_client_datadir = prev_path;

    fil.fil_init(0, 32);
    defer fil.fil_close();

    var space_id: btr.ulint = 0;
    const create_err = fil.fil_create_new_single_table_tablespace(&space_id, "restart", compat.FALSE, 0, 4);
    try std.testing.expectEqual(fil.DB_SUCCESS, create_err);

    var page_no: btr.ulint = 0;

    {
        const allocator = std.heap.page_allocator;
        const index = allocator.create(dict.dict_index_t) catch return error.OutOfMemory;
        index.* = .{};
        index.space = space_id;
        defer allocator.destroy(index);

        var mtr = btr.mtr_t{};
        const block = btr.btr_page_alloc(index, 0, 0, 0, &mtr) orelse return error.OutOfMemory;
        defer btr.btr_page_free(index, block, &mtr);

        page_no = block.frame.page_no;
        index.page = page_no;

        try std.testing.expectEqual(@as(usize, compat.UNIV_PAGE_SIZE), block.bytes.len);
        block.bytes[0] = 0x5A;
        block.bytes[1] = 0xA5;
        try std.testing.expectEqual(fil.DB_SUCCESS, fil.fil_write_page(space_id, page_no, block.bytes.ptr));

        btr.btr_free_index(index);
    }

    buf.buf_mem_free();

    {
        const allocator = std.heap.page_allocator;
        const index = allocator.create(dict.dict_index_t) catch return error.OutOfMemory;
        index.* = .{};
        index.space = space_id;
        index.page = page_no;
        defer allocator.destroy(index);

        var mtr = btr.mtr_t{};
        const loaded = btr.btr_root_block_get(index, &mtr) orelse return error.OutOfMemory;
        defer btr.btr_page_free(index, loaded, &mtr);

        try std.testing.expectEqual(@as(usize, compat.UNIV_PAGE_SIZE), loaded.bytes.len);
        try std.testing.expectEqual(@as(u8, 0x5A), loaded.bytes[0]);
        try std.testing.expectEqual(@as(u8, 0xA5), loaded.bytes[1]);

        btr.btr_free_index(index);
    }
}
