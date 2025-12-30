const std = @import("std");
const compat = @import("compat.zig");
const mem = @import("../mem/mod.zig");

pub const module_name = "ut.list";

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;
pub const mem_heap_t = mem.mem_heap_t;

pub const ib_list_node_t = struct {
    prev: ?*ib_list_node_t = null,
    next: ?*ib_list_node_t = null,
    data: ?*anyopaque = null,
};

pub const ib_list_t = struct {
    first: ?*ib_list_node_t = null,
    last: ?*ib_list_node_t = null,
    is_heap_list: ibool = compat.FALSE,
};

pub const ib_list_helper_t = struct {
    heap: *mem_heap_t,
    data: ?*anyopaque = null,
};

pub fn ib_list_create() *ib_list_t {
    const list = std.heap.page_allocator.create(ib_list_t) catch @panic("ib_list_create");
    list.* = .{};
    return list;
}

pub fn ib_list_free(list: *ib_list_t) void {
    std.debug.assert(list.is_heap_list == compat.FALSE);
    std.heap.page_allocator.destroy(list);
}

pub fn ib_list_get_first(list: *ib_list_t) ?*ib_list_node_t {
    return list.first;
}

pub fn ib_list_get_last(list: *ib_list_t) ?*ib_list_node_t {
    return list.last;
}

pub fn ib_list_add_last(list: *ib_list_t, data: ?*anyopaque, heap: *mem_heap_t) *ib_list_node_t {
    return ib_list_add_after(list, ib_list_get_last(list), data, heap);
}

pub fn ib_list_add_after(
    list: *ib_list_t,
    prev_node: ?*ib_list_node_t,
    data: ?*anyopaque,
    heap: *mem_heap_t,
) *ib_list_node_t {
    const buf = mem.mem_heap_alloc(heap, @sizeOf(ib_list_node_t)) orelse @panic("ib_list_add_after");
    const node = @as(*ib_list_node_t, @ptrCast(@alignCast(buf.ptr)));
    node.* = .{ .data = data };

    if (list.first == null) {
        std.debug.assert(prev_node == null);
        list.first = node;
        list.last = node;
        return node;
    }

    if (prev_node == null) {
        node.next = list.first;
        if (list.first) |first| {
            first.prev = node;
        }
        list.first = node;
        return node;
    }

    node.prev = prev_node;
    node.next = prev_node.?.next;
    prev_node.?.next = node;
    if (node.next) |next| {
        next.prev = node;
    } else {
        list.last = node;
    }
    return node;
}

pub fn ib_list_remove(list: *ib_list_t, node: *ib_list_node_t) void {
    if (node.prev) |prev| {
        prev.next = node.next;
    } else {
        std.debug.assert(list.first == node);
        list.first = node.next;
    }

    if (node.next) |next| {
        next.prev = node.prev;
    } else {
        std.debug.assert(list.last == node);
        list.last = node.prev;
    }
}

test "ib list add/remove" {
    var heap = mem.heap.MemHeap.init(std.testing.allocator, 128, .dynamic) catch @panic("heap");
    defer heap.deinit();

    const list = ib_list_create();
    defer ib_list_free(list);

    var a: u32 = 1;
    var b: u32 = 2;
    const node1 = ib_list_add_last(list, @ptrCast(&a), &heap);
    const node2 = ib_list_add_last(list, @ptrCast(&b), &heap);

    try std.testing.expect(list.first == node1);
    try std.testing.expect(list.last == node2);
    try std.testing.expect(node1.next == node2);
    try std.testing.expect(node2.prev == node1);

    ib_list_remove(list, node1);
    try std.testing.expect(list.first == node2);
    try std.testing.expect(node2.prev == null);

    ib_list_remove(list, node2);
    try std.testing.expect(list.first == null);
    try std.testing.expect(list.last == null);
}
