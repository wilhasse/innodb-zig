const std = @import("std");
const data = @import("../data/mod.zig");
const page = @import("../page/mod.zig");

pub fn row_build_index_entry_simple(rec: *page.rec_t, allocator: std.mem.Allocator) *data.dtuple_t {
    const tuple = allocator.create(data.dtuple_t) catch @panic("row_build_index_entry_simple");
    const fields = allocator.alloc(data.dfield_t, 1) catch @panic("row_build_index_entry_simple");
    tuple.* = .{ .n_fields = 1, .fields = fields };
    fields[0] = .{};
    data.dtype_set(&fields[0].type, data.DATA_INT, 0, 8);
    const buf = allocator.alloc(u8, 8) catch @panic("row_build_index_entry_simple");
    std.mem.writeInt(i64, buf[0..8], rec.key, .big);
    data.dfield_set_data(&fields[0], buf.ptr, 8);
    return tuple;
}

pub fn row_tuple_to_key(tuple: *const data.dtuple_t) i64 {
    if (tuple.n_fields == 0) {
        return 0;
    }
    const field = &tuple.fields[0];
    const ptr = data.dfield_get_data(field) orelse return 0;
    const bytes = @as([*]const u8, @ptrCast(ptr))[0..8];
    return std.mem.readInt(i64, bytes, .big);
}

pub fn row_free_tuple_simple(tuple: *data.dtuple_t, allocator: std.mem.Allocator) void {
    if (tuple.n_fields > 0) {
        const field = &tuple.fields[0];
        if (data.dfield_get_data(field)) |ptr| {
            allocator.free(@as([*]u8, @ptrCast(ptr))[0..8]);
        }
    }
    allocator.free(tuple.fields);
    allocator.destroy(tuple);
}

test "row build index entry simple" {
    const allocator = std.testing.allocator;
    var rec = page.rec_t{ .key = 42 };
    const tuple = row_build_index_entry_simple(&rec, allocator);
    defer row_free_tuple_simple(tuple, allocator);
    try std.testing.expectEqual(@as(i64, 42), row_tuple_to_key(tuple));
}
