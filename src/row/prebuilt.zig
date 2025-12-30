const std = @import("std");
const dict = @import("../dict/mod.zig");
const row = @import("mod.zig");

pub fn row_prebuilt_create(table: *dict.dict_table_t, allocator: std.mem.Allocator) *row.row_prebuilt_t {
    const prebuilt = allocator.create(row.row_prebuilt_t) catch @panic("row_prebuilt_create");
    prebuilt.* = .{};
    prebuilt.magic_n = row.ROW_PREBUILT_ALLOCATED;
    prebuilt.magic_n2 = row.ROW_PREBUILT_ALLOCATED;
    prebuilt.table = table;
    prebuilt.allocator = allocator;
    prebuilt.row_cache.n_max = row.FETCH_CACHE_SIZE;
    prebuilt.row_cache.n_size = row.FETCH_CACHE_SIZE;
    prebuilt.row_cache.ptr = allocator.alloc(row.ib_cached_row_t, row.FETCH_CACHE_SIZE) catch @panic("row_prebuilt_create");
    return prebuilt;
}

pub fn row_prebuilt_free(prebuilt: *row.row_prebuilt_t) void {
    if (prebuilt.magic_n != row.ROW_PREBUILT_ALLOCATED or prebuilt.magic_n2 != row.ROW_PREBUILT_ALLOCATED) {
        @panic("row_prebuilt_free");
    }
    prebuilt.magic_n = row.ROW_PREBUILT_FREED;
    prebuilt.magic_n2 = row.ROW_PREBUILT_FREED;
    prebuilt.allocator.free(prebuilt.row_cache.ptr);
    prebuilt.allocator.destroy(prebuilt);
}

pub fn row_prebuilt_reset(prebuilt: *row.row_prebuilt_t) void {
    prebuilt.row_cache.n_size = prebuilt.row_cache.n_max;
}

pub fn row_prebuilt_update_trx(prebuilt: *row.row_prebuilt_t, trx_id: u64) void {
    _ = prebuilt;
    _ = trx_id;
}

test "row prebuilt create/free" {
    const allocator = std.testing.allocator;
    var table = dict.dict_table_t{};
    const prebuilt = row_prebuilt_create(&table, allocator);
    try std.testing.expectEqual(row.ROW_PREBUILT_ALLOCATED, prebuilt.magic_n);
    try std.testing.expectEqual(row.FETCH_CACHE_SIZE, prebuilt.row_cache.n_max);
    row_prebuilt_free(prebuilt);
}
