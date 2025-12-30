const std = @import("std");
const compat = @import("../ut/compat.zig");
const data = @import("../data/mod.zig");
const dict = @import("../dict/mod.zig");

pub const module_name = "rem";

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;
pub const byte = compat.byte;

fn cmp_collate(code: ulint) ulint {
    return code;
}

pub fn cmp_cols_are_equal(col1: *const dict.dict_col_t, col2: *const dict.dict_col_t, check_charsets: ibool) ibool {
    if (data.dtype_is_non_binary_string_type(col1.mtype, col1.prtype) == compat.TRUE and
        data.dtype_is_non_binary_string_type(col2.mtype, col2.prtype) == compat.TRUE)
    {
        if (check_charsets != 0) {
            return if (data.dtype_get_charset_coll(col1.prtype) == data.dtype_get_charset_coll(col2.prtype)) compat.TRUE else compat.FALSE;
        }
        return compat.TRUE;
    }

    if (data.dtype_is_binary_string_type(col1.mtype, col1.prtype) == compat.TRUE and
        data.dtype_is_binary_string_type(col2.mtype, col2.prtype) == compat.TRUE)
    {
        return compat.TRUE;
    }

    if (col1.mtype != col2.mtype) {
        return compat.FALSE;
    }

    if (col1.mtype == data.DATA_INT and
        ((col1.prtype & data.DATA_UNSIGNED) != (col2.prtype & data.DATA_UNSIGNED)))
    {
        return compat.FALSE;
    }

    return if (col1.mtype != data.DATA_INT or col1.len == col2.len) compat.TRUE else compat.FALSE;
}

pub fn cmp_data_data(cmp_ctx: ?*anyopaque, mtype: ulint, prtype: ulint, data1: ?[*]const byte, len1: ulint, data2: ?[*]const byte, len2: ulint) i32 {
    _ = cmp_ctx;
    return cmp_data_data_slow(null, mtype, prtype, data1, len1, data2, len2);
}

pub fn cmp_data_data_slow(cmp_ctx: ?*anyopaque, mtype: ulint, prtype: ulint, data1: ?[*]const byte, len1: ulint, data2: ?[*]const byte, len2: ulint) i32 {
    _ = cmp_ctx;
    _ = mtype;
    _ = prtype;

    if (len1 == compat.UNIV_SQL_NULL and len2 == compat.UNIV_SQL_NULL) {
        return 0;
    }
    if (len1 == compat.UNIV_SQL_NULL) {
        return 1;
    }
    if (len2 == compat.UNIV_SQL_NULL) {
        return -1;
    }

    const slice1 = if (data1) |ptr| ptr[0..len1] else &[_]byte{};
    const slice2 = if (data2) |ptr| ptr[0..len2] else &[_]byte{};
    const min_len = @min(slice1.len, slice2.len);
    var i: usize = 0;
    while (i < min_len) : (i += 1) {
        const a = cmp_collate(slice1[i]);
        const b = cmp_collate(slice2[i]);
        if (a != b) {
            return if (a > b) 1 else -1;
        }
    }
    if (slice1.len == slice2.len) {
        return 0;
    }
    return if (slice1.len > slice2.len) 1 else -1;
}

pub fn cmp_dfield_dfield(cmp_ctx: ?*anyopaque, dfield1: *const data.dfield_t, dfield2: *const data.dfield_t) i32 {
    const ptr1 = data.dfield_get_data(dfield1);
    const ptr2 = data.dfield_get_data(dfield2);
    const len1 = data.dfield_get_len(dfield1);
    const len2 = data.dfield_get_len(dfield2);
    const type1 = dfield1.type;
    return cmp_data_data(cmp_ctx, type1.mtype, type1.prtype, if (ptr1) |p| @as([*]const byte, @ptrCast(p)) else null, len1, if (ptr2) |p| @as([*]const byte, @ptrCast(p)) else null, len2);
}

test "cmp cols are equal basic" {
    var col1 = dict.dict_col_t{ .mtype = data.DATA_INT, .prtype = 0, .len = 4 };
    var col2 = dict.dict_col_t{ .mtype = data.DATA_INT, .prtype = 0, .len = 4 };
    try std.testing.expectEqual(compat.TRUE, cmp_cols_are_equal(&col1, &col2, compat.TRUE));

    col2.prtype = data.DATA_UNSIGNED;
    try std.testing.expectEqual(compat.FALSE, cmp_cols_are_equal(&col1, &col2, compat.TRUE));
}

test "cmp data data lexicographic" {
    const a = "abc";
    const b = "abd";
    const res = cmp_data_data(null, data.DATA_VARCHAR, 0, a.ptr, a.len, b.ptr, b.len);
    try std.testing.expect(res < 0);
}
