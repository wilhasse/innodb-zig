const std = @import("std");
const compat = @import("../ut/compat.zig");

pub const module_name = "mach";

pub const byte = compat.byte;
pub const ulint = compat.ulint;
pub const ib_uint16_t = compat.ib_uint16_t;
pub const dulint = compat.Dulint;

pub fn mach_write_to_1(b: [*]byte, n: ulint) void {
    std.debug.assert(n < 256);
    b[0] = @as(byte, @intCast(n));
}

pub fn mach_read_from_1(b: [*]const byte) ulint {
    return b[0];
}

pub fn mach_write_to_4(b: [*]byte, n: ulint) void {
    const val = @as(u32, @intCast(n));
    std.mem.writeInt(u32, b[0..4], val, .big);
}

pub fn mach_read_from_4(b: [*]const byte) ulint {
    const val = std.mem.readInt(u32, b[0..4], .big);
    return val;
}

pub fn mach_write_to_2(b: [*]byte, n: ulint) void {
    std.debug.assert(n <= 0xFFFF);
    b[0] = @as(byte, @intCast((n >> 8) & 0xFF));
    b[1] = @as(byte, @intCast(n & 0xFF));
}

pub fn mach_read_from_2(b: [*]const byte) ulint {
    return (@as(ulint, b[0]) << 8) | @as(ulint, b[1]);
}

pub fn mach_encode_2(n: ulint) ib_uint16_t {
    var out: ib_uint16_t = 0;
    mach_write_to_2(@ptrCast(&out), n);
    return out;
}

pub fn mach_decode_2(n: ib_uint16_t) ulint {
    return mach_read_from_2(@ptrCast(&n));
}

pub fn mach_write_to_3(b: [*]byte, n: ulint) void {
    std.debug.assert(n <= 0xFFFFFF);
    b[0] = @as(byte, @intCast((n >> 16) & 0xFF));
    b[1] = @as(byte, @intCast((n >> 8) & 0xFF));
    b[2] = @as(byte, @intCast(n & 0xFF));
}

pub fn mach_read_from_3(b: [*]const byte) ulint {
    return (@as(ulint, b[0]) << 16) | (@as(ulint, b[1]) << 8) | @as(ulint, b[2]);
}

pub fn mach_write_to_8(b: [*]byte, n: dulint) void {
    mach_write_to_4(b, n.high);
    mach_write_to_4(b + 4, n.low);
}

pub fn mach_read_from_8(b: [*]const byte) dulint {
    return .{ .high = mach_read_from_4(b), .low = mach_read_from_4(b + 4) };
}

pub fn mach_write_compressed(b: [*]byte, n: ulint) ulint {
    if (n < 0x80) {
        mach_write_to_1(b, n);
        return 1;
    } else if (n < 0x4000) {
        mach_write_to_2(b, n | 0x8000);
        return 2;
    } else if (n < 0x200000) {
        mach_write_to_3(b, n | 0xC00000);
        return 3;
    } else if (n < 0x10000000) {
        mach_write_to_4(b, n | 0xE0000000);
        return 4;
    } else {
        mach_write_to_1(b, 0xF0);
        mach_write_to_4(b + 1, n);
        return 5;
    }
}

pub fn mach_get_compressed_size(n: ulint) ulint {
    if (n < 0x80) {
        return 1;
    } else if (n < 0x4000) {
        return 2;
    } else if (n < 0x200000) {
        return 3;
    } else if (n < 0x10000000) {
        return 4;
    } else {
        return 5;
    }
}

pub fn mach_read_compressed(b: [*]const byte) ulint {
    const flag = mach_read_from_1(b);
    if (flag < 0x80) {
        return flag;
    } else if (flag < 0xC0) {
        return mach_read_from_2(b) & 0x7FFF;
    } else if (flag < 0xE0) {
        return mach_read_from_3(b) & 0x3FFFFF;
    } else if (flag < 0xF0) {
        return mach_read_from_4(b) & 0x1FFFFFFF;
    } else {
        std.debug.assert(flag == 0xF0);
        return mach_read_from_4(b + 1);
    }
}

pub fn mach_dulint_write_compressed(b: [*]byte, n: dulint) ulint {
    const size = mach_write_compressed(b, n.high);
    mach_write_to_4(b + size, n.low);
    return size + 4;
}

pub fn mach_dulint_get_compressed_size(n: dulint) ulint {
    return 4 + mach_get_compressed_size(n.high);
}

pub fn mach_dulint_read_compressed(b: [*]const byte) dulint {
    const high = mach_read_compressed(b);
    const size = mach_get_compressed_size(high);
    const low = mach_read_from_4(b + size);
    return .{ .high = high, .low = low };
}

pub fn mach_dulint_write_much_compressed(b: [*]byte, n: dulint) ulint {
    if (n.high == 0) {
        return mach_write_compressed(b, n.low);
    }

    b[0] = 0xFF;
    var size = 1 + mach_write_compressed(b + 1, n.high);
    size += mach_write_compressed(b + size, n.low);
    return size;
}

pub fn mach_dulint_get_much_compressed_size(n: dulint) ulint {
    if (n.high == 0) {
        return mach_get_compressed_size(n.low);
    }
    return 1 + mach_get_compressed_size(n.high) + mach_get_compressed_size(n.low);
}

pub fn mach_dulint_read_much_compressed(b: [*]const byte) dulint {
    if (b[0] != 0xFF) {
        const low = mach_read_compressed(b);
        return .{ .high = 0, .low = low };
    }

    const high = mach_read_compressed(b + 1);
    const size = mach_get_compressed_size(high);
    const low = mach_read_compressed(b + 1 + size);
    return .{ .high = high, .low = low };
}

pub fn mach_parse_compressed(ptr: [*]byte, end_ptr: [*]byte, val: *ulint) ?[*]byte {
    if (@intFromPtr(ptr) >= @intFromPtr(end_ptr)) {
        return null;
    }
    const flag = mach_read_from_1(ptr);
    if (flag < 0x80) {
        val.* = flag;
        return ptr + 1;
    } else if (flag < 0xC0) {
        if (@intFromPtr(end_ptr) < @intFromPtr(ptr + 2)) {
            return null;
        }
        val.* = mach_read_from_2(ptr) & 0x7FFF;
        return ptr + 2;
    } else if (flag < 0xE0) {
        if (@intFromPtr(end_ptr) < @intFromPtr(ptr + 3)) {
            return null;
        }
        val.* = mach_read_from_3(ptr) & 0x3FFFFF;
        return ptr + 3;
    } else if (flag < 0xF0) {
        if (@intFromPtr(end_ptr) < @intFromPtr(ptr + 4)) {
            return null;
        }
        val.* = mach_read_from_4(ptr) & 0x1FFFFFFF;
        return ptr + 4;
    } else {
        std.debug.assert(flag == 0xF0);
        if (@intFromPtr(end_ptr) < @intFromPtr(ptr + 5)) {
            return null;
        }
        val.* = mach_read_from_4(ptr + 1);
        return ptr + 5;
    }
}

pub fn mach_dulint_parse_compressed(ptr: [*]byte, end_ptr: [*]byte, val: *dulint) ?[*]byte {
    if (@intFromPtr(end_ptr) < @intFromPtr(ptr + 5)) {
        return null;
    }

    const high = mach_read_compressed(ptr);
    const size = mach_get_compressed_size(high);
    const next = ptr + size;

    if (@intFromPtr(end_ptr) < @intFromPtr(next + 4)) {
        return null;
    }

    const low = mach_read_from_4(next);
    val.* = .{ .high = high, .low = low };
    return next + 4;
}

test "mach compressed roundtrip and parse" {
    var buf = [_]byte{0} ** 8;
    var out: ulint = 0;
    const ptr = buf[0..].ptr;

    _ = mach_write_compressed(ptr, 0x7F);
    try std.testing.expect(mach_parse_compressed(ptr, ptr + buf.len, &out) == ptr + 1);
    try std.testing.expect(out == 0x7F);

    _ = mach_write_compressed(ptr, 0x4000);
    try std.testing.expect(mach_parse_compressed(ptr, ptr + buf.len, &out) == ptr + 3);
    try std.testing.expect(out == 0x4000);

    _ = mach_write_compressed(ptr, 0x10000000);
    try std.testing.expect(mach_parse_compressed(ptr, ptr + buf.len, &out) == ptr + 5);
    try std.testing.expect(out == 0x10000000);
}

test "mach dulint parse compressed" {
    var buf = [_]byte{0} ** 16;
    const high: ulint = 0x1234;
    const low: ulint = 0x89ABCDEF;
    const size = mach_write_compressed(buf[0..].ptr, high);
    mach_write_to_4(buf[size..].ptr, low);

    var value: dulint = .{ .high = 0, .low = 0 };
    const end_ptr = mach_dulint_parse_compressed(buf[0..].ptr, buf[0..].ptr + buf.len, &value);
    try std.testing.expect(end_ptr != null);
    try std.testing.expect(value.high == high);
    try std.testing.expect(value.low == low);
}

test "mach dulint compressed roundtrip" {
    var buf = [_]byte{0} ** 16;
    const input: dulint = .{ .high = 0x2, .low = 0x11223344 };
    const size = mach_dulint_write_compressed(buf[0..].ptr, input);
    const out = mach_dulint_read_compressed(buf[0..].ptr);
    try std.testing.expectEqual(input.high, out.high);
    try std.testing.expectEqual(input.low, out.low);
    try std.testing.expectEqual(size, mach_dulint_get_compressed_size(input));
}

test "mach dulint much compressed roundtrip" {
    var buf = [_]byte{0} ** 16;
    const input: dulint = .{ .high = 0x1, .low = 0x22334455 };
    const size = mach_dulint_write_much_compressed(&buf, input);
    const out = mach_dulint_read_much_compressed(&buf);
    try std.testing.expectEqual(input.high, out.high);
    try std.testing.expectEqual(input.low, out.low);
    try std.testing.expectEqual(size, mach_dulint_get_much_compressed_size(input));

    const low_only: dulint = .{ .high = 0, .low = 0x77 };
    const size_low = mach_dulint_write_much_compressed(&buf, low_only);
    const out_low = mach_dulint_read_much_compressed(&buf);
    try std.testing.expectEqual(low_only.high, out_low.high);
    try std.testing.expectEqual(low_only.low, out_low.low);
    try std.testing.expectEqual(size_low, mach_dulint_get_much_compressed_size(low_only));
}
