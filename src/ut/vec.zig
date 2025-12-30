const std = @import("std");
const compat = @import("compat.zig");
const mem = @import("../mem/mod.zig");

pub const module_name = "ut.vec";

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;
pub const mem_heap_t = mem.mem_heap_t;

pub const ib_vector_t = struct {
    heap: *mem_heap_t,
    data: [*]?*anyopaque,
    used: ulint,
    total: ulint,
};

pub fn ib_vector_create(heap: *mem_heap_t, size: ulint) *ib_vector_t {
    std.debug.assert(size > 0);
    const vec_buf = mem.mem_heap_alloc(heap, @as(ulint, @intCast(@sizeOf(ib_vector_t)))) orelse @panic("ib_vector_create");
    const vec = @as(*ib_vector_t, @ptrCast(@alignCast(vec_buf.ptr)));
    const data_buf = mem.mem_heap_alloc(
        heap,
        @as(ulint, @intCast(@sizeOf(?*anyopaque) * @as(usize, @intCast(size)))),
    ) orelse @panic("ib_vector_create data");
    vec.* = .{
        .heap = heap,
        .data = @as([*]?*anyopaque, @ptrCast(@alignCast(data_buf.ptr))),
        .used = 0,
        .total = size,
    };
    return vec;
}

pub fn ib_vector_push(vec: *ib_vector_t, elem: ?*anyopaque) void {
    if (vec.used >= vec.total) {
        const new_total = vec.total * 2;
        const data_buf = mem.mem_heap_alloc(
            vec.heap,
            @as(ulint, @intCast(@sizeOf(?*anyopaque) * @as(usize, @intCast(new_total)))),
        ) orelse @panic("ib_vector_push grow");
        const new_data = @as([*]?*anyopaque, @ptrCast(@alignCast(data_buf.ptr)));
        const old_slice = vec.data[0..@as(usize, @intCast(vec.total))];
        const new_slice = new_data[0..@as(usize, @intCast(vec.total))];
        std.mem.copyForwards(?*anyopaque, new_slice, old_slice);
        vec.data = new_data;
        vec.total = new_total;
    }
    vec.data[@as(usize, @intCast(vec.used))] = elem;
    vec.used += 1;
}

pub fn ib_vector_size(vec: *const ib_vector_t) ulint {
    return vec.used;
}

pub fn ib_vector_is_empty(vec: *const ib_vector_t) ibool {
    return if (ib_vector_size(vec) == 0) compat.TRUE else compat.FALSE;
}

pub fn ib_vector_get(vec: *ib_vector_t, n: ulint) ?*anyopaque {
    std.debug.assert(n < ib_vector_size(vec));
    return vec.data[@as(usize, @intCast(n))];
}

pub fn ib_vector_get_const(vec: *const ib_vector_t, n: ulint) ?*const anyopaque {
    std.debug.assert(n < ib_vector_size(vec));
    return @as(?*const anyopaque, @ptrCast(vec.data[@as(usize, @intCast(n))]));
}

pub fn ib_vector_set(vec: *ib_vector_t, n: ulint, p: ?*anyopaque) ?*anyopaque {
    std.debug.assert(n < ib_vector_size(vec));
    const idx = @as(usize, @intCast(n));
    const prev = vec.data[idx];
    vec.data[idx] = p;
    return prev;
}

pub fn ib_vector_pop(vec: *ib_vector_t) ?*anyopaque {
    std.debug.assert(vec.used > 0);
    vec.used -= 1;
    const idx = @as(usize, @intCast(vec.used));
    const elem = vec.data[idx];
    vec.data[idx] = null;
    return elem;
}

pub fn ib_vector_free(vec: *ib_vector_t) void {
    mem.mem_heap_free_func(vec.heap);
}

test "ib vector push/get/set/pop" {
    const heap = mem.mem_heap_create_func(128, mem.MEM_HEAP_DYNAMIC) orelse return error.OutOfMemory;
    const vec = ib_vector_create(heap, 2);

    var a: u32 = 1;
    var b: u32 = 2;
    var c: u32 = 3;

    try std.testing.expectEqual(@as(ulint, 0), ib_vector_size(vec));
    try std.testing.expectEqual(compat.TRUE, ib_vector_is_empty(vec));

    ib_vector_push(vec, @ptrCast(&a));
    ib_vector_push(vec, @ptrCast(&b));
    ib_vector_push(vec, @ptrCast(&c));

    try std.testing.expectEqual(@as(ulint, 3), ib_vector_size(vec));
    try std.testing.expectEqual(@as(?*anyopaque, @ptrCast(&a)), ib_vector_get(vec, 0));
    try std.testing.expectEqual(@as(?*anyopaque, @ptrCast(&b)), ib_vector_get(vec, 1));
    try std.testing.expectEqual(@as(?*anyopaque, @ptrCast(&c)), ib_vector_get(vec, 2));

    const prev = ib_vector_set(vec, 1, @ptrCast(&c));
    try std.testing.expectEqual(@as(?*anyopaque, @ptrCast(&b)), prev);

    const popped = ib_vector_pop(vec);
    try std.testing.expectEqual(@as(?*anyopaque, @ptrCast(&c)), popped);
    try std.testing.expectEqual(@as(ulint, 2), ib_vector_size(vec));

    ib_vector_free(vec);
}
