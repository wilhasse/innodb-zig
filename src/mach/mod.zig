const std = @import("std");
const compat = @import("../ut/compat.zig");

pub const module_name = "mach";

pub const byte = compat.byte;
pub const ulint = compat.ulint;

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
