const std = @import("std");
const compat = @import("../ut/compat.zig");
const data = @import("../data/mod.zig");
const rem = @import("../rem/mod.zig");
const row = @import("mod.zig");

pub const ulint = compat.ulint;

fn row_ext_cache_fill(ext_cache: *row.row_ext_t, i: ulint, dfield: *const data.dfield_t) void {
    if (i >= ext_cache.n_ext) {
        return;
    }
    if (data.dfield_is_ext(dfield) == compat.FALSE) {
        ext_cache.len[i] = 0;
        return;
    }
    const f_len = data.dfield_get_len(dfield);
    const ptr = data.dfield_get_data(dfield) orelse {
        ext_cache.len[i] = 0;
        return;
    };
    const copy_len = @min(f_len, rem.REC_MAX_INDEX_COL_LEN);
    if (copy_len == 0) {
        ext_cache.len[i] = 0;
        return;
    }
    const start = @as(usize, @intCast(i * rem.REC_MAX_INDEX_COL_LEN));
    std.mem.copyForwards(u8, ext_cache.buf[start .. start + copy_len], @as([*]const u8, @ptrCast(ptr))[0..copy_len]);
    ext_cache.len[i] = copy_len;
}

pub fn row_ext_create(n_ext: ulint, ext_cols: []const ulint, tuple: *const data.dtuple_t, zip_size: ulint, allocator: std.mem.Allocator) *row.row_ext_t {
    _ = zip_size;
    const ext_cache = allocator.create(row.row_ext_t) catch @panic("row_ext_create");
    ext_cache.* = .{};
    ext_cache.n_ext = n_ext;
    ext_cache.ext = allocator.alloc(ulint, n_ext) catch @panic("row_ext_create");
    std.mem.copyForwards(ulint, @constCast(ext_cache.ext), ext_cols[0..n_ext]);
    ext_cache.buf = allocator.alloc(u8, n_ext * rem.REC_MAX_INDEX_COL_LEN) catch @panic("row_ext_create");
    ext_cache.len = allocator.alloc(ulint, n_ext) catch @panic("row_ext_create");
    std.mem.set(ulint, ext_cache.len, 0);

    for (ext_cols[0..n_ext], 0..) |col_no, i| {
        const dfield = data.dtuple_get_nth_field(tuple, col_no);
        row_ext_cache_fill(ext_cache, @as(ulint, @intCast(i)), dfield);
    }

    return ext_cache;
}

pub fn row_ext_lookup_ith(ext_cache: *const row.row_ext_t, i: ulint, len_out: *ulint) ?[]const u8 {
    if (i >= ext_cache.n_ext) {
        len_out.* = 0;
        return null;
    }
    const len = ext_cache.len[i];
    len_out.* = len;
    if (len == 0) {
        return null;
    }
    const start = @as(usize, @intCast(i * rem.REC_MAX_INDEX_COL_LEN));
    return ext_cache.buf[start .. start + len];
}

pub fn row_ext_lookup(ext_cache: *const row.row_ext_t, col: ulint, len_out: *ulint) ?[]const u8 {
    for (ext_cache.ext, 0..) |col_no, i| {
        if (col_no == col) {
            return row_ext_lookup_ith(ext_cache, @as(ulint, @intCast(i)), len_out);
        }
    }
    len_out.* = 0;
    return null;
}

pub fn row_ext_free(ext_cache: *row.row_ext_t, allocator: std.mem.Allocator) void {
    allocator.free(@constCast(ext_cache.ext));
    allocator.free(ext_cache.buf);
    allocator.free(ext_cache.len);
    allocator.destroy(ext_cache);
}

test "row ext cache fill and lookup" {
    const allocator = std.testing.allocator;
    var field = data.dfield_t{};
    data.dfield_set_data(&field, "blob".ptr, 4);
    field.ext = true;

    var fields = [_]data.dfield_t{field};
    var tuple = data.dtuple_t{ .n_fields = 1, .fields = fields[0..] };

    const ext_cols = [_]ulint{0};
    const ext_cache = row_ext_create(1, ext_cols[0..], &tuple, 0, allocator);
    defer row_ext_free(ext_cache, allocator);

    var len: ulint = 0;
    const prefix = row_ext_lookup(ext_cache, 0, &len) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(ulint, 4), len);
    try std.testing.expectEqualStrings("blob", prefix);
}
