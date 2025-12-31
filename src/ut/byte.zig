const std = @import("std");
const compat = @import("compat.zig");

pub const module_name = "ut.byte";

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;
pub const ib_int64_t = compat.ib_int64_t;
pub const ib_uint64_t = compat.ib_uint64_t;
pub const dulint = compat.Dulint;

pub const ut_dulint_zero: dulint = .{ .high = 0, .low = 0 };
pub const ut_dulint_max: dulint = .{ .high = 0xFFFF_FFFF, .low = 0xFFFF_FFFF };

pub fn ut_dulint_create(high: ulint, low: ulint) dulint {
    std.debug.assert(high <= 0xFFFF_FFFF);
    std.debug.assert(low <= 0xFFFF_FFFF);
    return .{ .high = high, .low = low };
}

pub fn ut_dulint_get_high(d: dulint) ulint {
    return d.high;
}

pub fn ut_dulint_get_low(d: dulint) ulint {
    return d.low;
}

pub fn ut_conv_dulint_to_longlong(d: dulint) ib_int64_t {
    return @as(ib_int64_t, @intCast(d.low)) + (@as(ib_int64_t, @intCast(d.high)) << 32);
}

pub fn ut_dulint_is_zero(a: dulint) ibool {
    return if (a.low == 0 and a.high == 0) compat.TRUE else compat.FALSE;
}

pub fn ut_dulint_cmp(a: dulint, b: dulint) i32 {
    if (a.high > b.high) {
        return 1;
    } else if (a.high < b.high) {
        return -1;
    } else if (a.low > b.low) {
        return 1;
    } else if (a.low < b.low) {
        return -1;
    }
    return 0;
}

pub fn ut_dulint_add(a: dulint, b: ulint) dulint {
    std.debug.assert(b <= 0xFFFF_FFFF);
    var res = a;
    if (0xFFFF_FFFF - b >= res.low) {
        res.low += b;
        return res;
    }
    res.low = res.low - (0xFFFF_FFFF - b) - 1;
    res.high += 1;
    return res;
}

pub fn ut_dulint_subtract(a: dulint, b: ulint) dulint {
    std.debug.assert(b <= 0xFFFF_FFFF);
    var res = a;
    if (res.low >= b) {
        res.low -= b;
        return res;
    }
    var borrow = b;
    borrow -= res.low + 1;
    res.low = 0xFFFF_FFFF - borrow;
    std.debug.assert(res.high > 0);
    res.high -= 1;
    return res;
}

pub fn ut_dulint_align_down(n: dulint, align_no: ulint) dulint {
    std.debug.assert(align_no > 0);
    std.debug.assert(compat.ut_is_2pow(align_no));
    const low = n.low & ~(align_no - 1);
    return ut_dulint_create(n.high, low);
}

pub fn ut_dulint_align_up(n: dulint, align_no: ulint) dulint {
    return ut_dulint_align_down(ut_dulint_add(n, align_no - 1), align_no);
}

pub fn ut_uint64_align_down(n: ib_uint64_t, align_no: ulint) ib_uint64_t {
    std.debug.assert(align_no > 0);
    std.debug.assert(compat.ut_is_2pow(align_no));
    return n & ~(@as(ib_uint64_t, align_no) - 1);
}

pub fn ut_uint64_align_up(n: ib_uint64_t, align_no: ulint) ib_uint64_t {
    std.debug.assert(align_no > 0);
    std.debug.assert(compat.ut_is_2pow(align_no));
    const align_1 = @as(ib_uint64_t, align_no) - 1;
    return (n + align_1) & ~align_1;
}

pub fn ut_align(ptr: anytype, align_no: ulint) @TypeOf(ptr) {
    std.debug.assert(align_no > 0);
    std.debug.assert(compat.ut_is_2pow(align_no));
    const addr = @intFromPtr(ptr);
    const aligned = (addr + align_no - 1) & ~(align_no - 1);
    return @as(@TypeOf(ptr), @ptrFromInt(aligned));
}

pub fn ut_align_down(ptr: anytype, align_no: ulint) @TypeOf(ptr) {
    std.debug.assert(align_no > 0);
    std.debug.assert(compat.ut_is_2pow(align_no));
    const addr = @intFromPtr(ptr);
    const aligned = addr & ~(align_no - 1);
    return @as(@TypeOf(ptr), @ptrFromInt(aligned));
}

pub fn ut_align_offset(ptr: anytype, align_no: ulint) ulint {
    std.debug.assert(align_no > 0);
    std.debug.assert(compat.ut_is_2pow(align_no));
    const addr = @intFromPtr(ptr);
    return addr & (align_no - 1);
}

pub fn ut_bit_get_nth(a: ulint, n: ulint) ibool {
    std.debug.assert(n < 8 * @sizeOf(ulint));
    const shift = @as(u6, @intCast(n));
    return @as(ibool, @intCast((a >> shift) & 1));
}

pub fn ut_bit_set_nth(a: ulint, n: ulint, val: ibool) ulint {
    std.debug.assert(n < 8 * @sizeOf(ulint));
    const shift = @as(u6, @intCast(n));
    if (val != 0) {
        return (@as(ulint, 1) << shift) | a;
    }
    return ~(@as(ulint, 1) << shift) & a;
}

test "ut dulint basics" {
    const d = ut_dulint_create(1, 2);
    try std.testing.expectEqual(@as(ulint, 1), ut_dulint_get_high(d));
    try std.testing.expectEqual(@as(ulint, 2), ut_dulint_get_low(d));
    try std.testing.expectEqual(@as(ib_int64_t, (1 << 32) + 2), ut_conv_dulint_to_longlong(d));
    try std.testing.expectEqual(compat.FALSE, ut_dulint_is_zero(d));
    try std.testing.expectEqual(compat.TRUE, ut_dulint_is_zero(ut_dulint_zero));
}

test "ut dulint compare add subtract" {
    const a = ut_dulint_create(0, 10);
    const b = ut_dulint_create(0, 12);
    try std.testing.expect(ut_dulint_cmp(a, b) < 0);
    try std.testing.expect(ut_dulint_cmp(b, a) > 0);
    try std.testing.expect(ut_dulint_cmp(a, a) == 0);

    var c = ut_dulint_create(0, 0xFFFF_FFFF);
    c = ut_dulint_add(c, 1);
    try std.testing.expectEqual(@as(ulint, 1), c.high);
    try std.testing.expectEqual(@as(ulint, 0), c.low);

    const d = ut_dulint_subtract(c, 1);
    try std.testing.expectEqual(@as(ulint, 0), d.high);
    try std.testing.expectEqual(@as(ulint, 0xFFFF_FFFF), d.low);
}

test "ut dulint align helpers" {
    const d = ut_dulint_create(1, 13);
    const down = ut_dulint_align_down(d, 8);
    const up = ut_dulint_align_up(d, 8);
    try std.testing.expectEqual(@as(ulint, 8), down.low);
    try std.testing.expectEqual(@as(ulint, 16), up.low);
}

test "ut uint64 align helpers" {
    try std.testing.expectEqual(@as(ib_uint64_t, 16), ut_uint64_align_down(19, 8));
    try std.testing.expectEqual(@as(ib_uint64_t, 24), ut_uint64_align_up(19, 8));
}

test "ut pointer align helpers" {
    var buf: [32]u8 align(8) = [_]u8{0} ** 32;
    const ptr = &buf[3];
    const up = ut_align(ptr, 8);
    const down = ut_align_down(ptr, 8);
    try std.testing.expectEqual(@as(ulint, 3), ut_align_offset(ptr, 8));
    try std.testing.expect(@intFromPtr(down) <= @intFromPtr(ptr));
    try std.testing.expect(@intFromPtr(up) >= @intFromPtr(ptr));
}

test "ut bit get/set" {
    const value: ulint = 0b1010;
    try std.testing.expectEqual(@as(ibool, 1), ut_bit_get_nth(value, 1));
    try std.testing.expectEqual(@as(ibool, 0), ut_bit_get_nth(value, 0));
    try std.testing.expectEqual(@as(ulint, 0b1110), ut_bit_set_nth(value, 2, compat.TRUE));
    try std.testing.expectEqual(@as(ulint, 0b1000), ut_bit_set_nth(value, 1, compat.FALSE));
}
