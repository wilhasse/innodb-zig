const std = @import("std");
const compat = @import("../ut/compat.zig");
const fil = @import("../fil/mod.zig");

pub const module_name = "fut";

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;
pub const byte = compat.byte;
pub const fil_addr_t = fil.fil_addr_t;
pub const fil_faddr_t = fil.fil_faddr_t;
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

pub const flst_base_node_t = struct {
    len: ulint = 0,
    first: ?*flst_node_t = null,
    last: ?*flst_node_t = null,
};

pub const flst_node_t = struct {
    addr: fil_addr_t = fil.fil_addr_null,
    prev: ?*flst_node_t = null,
    next: ?*flst_node_t = null,
};

pub const FLST_BASE_NODE_SIZE: ulint = 4 + 2 * fil.FIL_ADDR_SIZE;
pub const FLST_NODE_SIZE: ulint = 2 * fil.FIL_ADDR_SIZE;

pub fn flst_init(base: *flst_base_node_t, mtr: *mtr_t) void {
    _ = mtr;
    base.* = .{};
}

fn flst_add_to_empty(base: *flst_base_node_t, node: *flst_node_t) void {
    base.first = node;
    base.last = node;
    base.len = 1;
    node.prev = null;
    node.next = null;
}

pub fn flst_add_last(base: *flst_base_node_t, node: *flst_node_t, mtr: *mtr_t) void {
    _ = mtr;
    if (base.len == 0) {
        flst_add_to_empty(base, node);
        return;
    }
    node.prev = base.last;
    node.next = null;
    base.last.?.next = node;
    base.last = node;
    base.len += 1;
}

pub fn flst_add_first(base: *flst_base_node_t, node: *flst_node_t, mtr: *mtr_t) void {
    _ = mtr;
    if (base.len == 0) {
        flst_add_to_empty(base, node);
        return;
    }
    node.next = base.first;
    node.prev = null;
    base.first.?.prev = node;
    base.first = node;
    base.len += 1;
}

pub fn flst_insert_after(base: *flst_base_node_t, node1: *flst_node_t, node2: *flst_node_t, mtr: *mtr_t) void {
    _ = mtr;
    node2.prev = node1;
    node2.next = node1.next;
    if (node1.next) |next| {
        next.prev = node2;
    } else {
        base.last = node2;
    }
    node1.next = node2;
    base.len += 1;
}

pub fn flst_insert_before(base: *flst_base_node_t, node2: *flst_node_t, node3: *flst_node_t, mtr: *mtr_t) void {
    _ = mtr;
    node2.next = node3;
    node2.prev = node3.prev;
    if (node3.prev) |prev| {
        prev.next = node2;
    } else {
        base.first = node2;
    }
    node3.prev = node2;
    base.len += 1;
}

pub fn flst_remove(base: *flst_base_node_t, node2: *flst_node_t, mtr: *mtr_t) void {
    _ = mtr;
    if (node2.prev) |prev| {
        prev.next = node2.next;
    } else {
        base.first = node2.next;
    }
    if (node2.next) |next| {
        next.prev = node2.prev;
    } else {
        base.last = node2.prev;
    }
    node2.prev = null;
    node2.next = null;
    if (base.len > 0) {
        base.len -= 1;
    }
}

pub fn flst_cut_end(base: *flst_base_node_t, node2: *flst_node_t, n_nodes: ulint, mtr: *mtr_t) void {
    _ = mtr;
    if (n_nodes == 0 or base.len == 0) {
        return;
    }
    if (node2.prev) |prev| {
        prev.next = null;
        base.last = prev;
    } else {
        base.first = null;
        base.last = null;
    }
    node2.prev = null;
    if (base.len >= n_nodes) {
        base.len -= n_nodes;
    } else {
        base.len = 0;
    }
}

pub fn flst_truncate_end(base: *flst_base_node_t, node2: *flst_node_t, n_nodes: ulint, mtr: *mtr_t) void {
    _ = mtr;
    node2.next = null;
    base.last = node2;
    if (base.len > 0 and base.len >= n_nodes) {
        base.len -= n_nodes;
    }
}

pub fn flst_get_len(base: *const flst_base_node_t, mtr: *mtr_t) ulint {
    _ = mtr;
    return base.len;
}

pub fn flst_get_first(base: *const flst_base_node_t, mtr: *mtr_t) fil_addr_t {
    _ = mtr;
    return if (base.first) |node| node.addr else fil.fil_addr_null;
}

pub fn flst_get_last(base: *const flst_base_node_t, mtr: *mtr_t) fil_addr_t {
    _ = mtr;
    return if (base.last) |node| node.addr else fil.fil_addr_null;
}

pub fn flst_get_next_addr(node: *const flst_node_t, mtr: *mtr_t) fil_addr_t {
    _ = mtr;
    return if (node.next) |next| next.addr else fil.fil_addr_null;
}

pub fn flst_get_prev_addr(node: *const flst_node_t, mtr: *mtr_t) fil_addr_t {
    _ = mtr;
    return if (node.prev) |prev| prev.addr else fil.fil_addr_null;
}

pub fn flst_write_addr(faddr: *fil_addr_t, addr: fil_addr_t, mtr: *mtr_t) void {
    _ = mtr;
    faddr.* = addr;
}

pub fn flst_read_addr(faddr: *const fil_addr_t, mtr: *mtr_t) fil_addr_t {
    _ = mtr;
    return faddr.*;
}

pub fn flst_validate(base: *const flst_base_node_t, mtr: *mtr_t) ibool {
    _ = mtr;
    var count: ulint = 0;
    var cur = base.first;
    var last: ?*flst_node_t = null;
    while (cur) |node| {
        count += 1;
        last = node;
        cur = node.next;
    }
    if (count != base.len) {
        return compat.FALSE;
    }
    if ((last == null and base.last != null) or (last != null and base.last != last)) {
        return compat.FALSE;
    }
    return compat.TRUE;
}

test "fut_get_ptr returns frame pointer" {
    var mtr = mtr_t{};
    const addr = fil_addr_t{ .page = 0, .boffset = 12 };
    const ptr = fut_get_ptr(0, 0, addr, RW_X_LATCH, &mtr);
    ptr[0] = 0xAB;
    try std.testing.expect(fut_page[12] == 0xAB);
}

test "flst list operations" {
    var mtr = mtr_t{};
    var base = flst_base_node_t{};
    flst_init(&base, &mtr);

    var n1 = flst_node_t{ .addr = .{ .page = 1, .boffset = 0 } };
    var n2 = flst_node_t{ .addr = .{ .page = 2, .boffset = 0 } };
    var n3 = flst_node_t{ .addr = .{ .page = 3, .boffset = 0 } };

    flst_add_last(&base, &n1, &mtr);
    flst_add_last(&base, &n2, &mtr);
    try std.testing.expect(flst_get_len(&base, &mtr) == 2);
    try std.testing.expect(flst_get_first(&base, &mtr).page == 1);
    try std.testing.expect(flst_get_last(&base, &mtr).page == 2);

    flst_insert_before(&base, &n3, &n2, &mtr);
    try std.testing.expect(flst_get_next_addr(&n1, &mtr).page == 3);
    try std.testing.expect(flst_get_prev_addr(&n2, &mtr).page == 3);

    flst_remove(&base, &n3, &mtr);
    try std.testing.expect(flst_get_len(&base, &mtr) == 2);
    try std.testing.expect(flst_validate(&base, &mtr) == compat.TRUE);
}
