const std = @import("std");
const compat = @import("../ut/compat.zig");
const fil = @import("../fil/mod.zig");

pub const module_name = "fut";

pub const ulint = compat.ulint;
pub const byte = compat.byte;
pub const fil_addr_t = fil.fil_addr_t;
pub const mtr_t = struct {};

pub const RW_S_LATCH: ulint = 1;
pub const RW_X_LATCH: ulint = 2;

var fut_page: [compat.UNIV_PAGE_SIZE]byte = undefined;

pub fn fut_get_ptr(space: ulint, zip_size: ulint, addr: fil_addr_t, rw_latch: ulint, mtr: *mtr_t) [*]byte {
    _ = space;
    _ = zip_size;
    _ = mtr;
    std.debug.assert(addr.boffset < compat.UNIV_PAGE_SIZE);
    std.debug.assert(rw_latch == RW_S_LATCH or rw_latch == RW_X_LATCH);
    return fut_page[0..].ptr + addr.boffset;
}

test "fut_get_ptr returns frame pointer" {
    var mtr = mtr_t{};
    const addr = fil_addr_t{ .page = 0, .boffset = 12 };
    const ptr = fut_get_ptr(0, 0, addr, RW_X_LATCH, &mtr);
    ptr[0] = 0xAB;
    try std.testing.expect(fut_page[12] == 0xAB);
}
