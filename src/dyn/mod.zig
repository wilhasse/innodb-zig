const std = @import("std");
const compat = @import("../ut/compat.zig");
const mem = @import("../mem/mod.zig");

pub const module_name = "dyn";

pub const ulint = compat.ulint;
pub const byte = compat.byte;
pub const mem_heap_t = mem.heap.MemHeap;

pub const DYN_ARRAY_DATA_SIZE: ulint = 512;
pub const DYN_BLOCK_MAGIC_N: ulint = 375767;
pub const DYN_BLOCK_FULL_FLAG: ulint = 0x1000000;

comptime {
    if (DYN_ARRAY_DATA_SIZE >= DYN_BLOCK_FULL_FLAG) {
        @compileError("DYN_ARRAY_DATA_SIZE must be smaller than DYN_BLOCK_FULL_FLAG");
    }
}

const DynList = struct {
    first: ?*dyn_block_t = null,
    last: ?*dyn_block_t = null,
};

pub const dyn_block_t = struct {
    heap: ?*mem_heap_t = null,
    used: ulint = 0,
    data: [DYN_ARRAY_DATA_SIZE]byte = undefined,
    base: DynList = .{},
    list_prev: ?*dyn_block_t = null,
    list_next: ?*dyn_block_t = null,
    buf_end: ulint = 0,
    magic_n: ulint = DYN_BLOCK_MAGIC_N,
};

pub const dyn_array_t = dyn_block_t;

fn listInit(list: *DynList) void {
    list.first = null;
    list.last = null;
}

fn listAddFirst(list: *DynList, node: *dyn_block_t) void {
    node.list_prev = null;
    node.list_next = list.first;
    if (list.first) |first| {
        first.list_prev = node;
    } else {
        list.last = node;
    }
    list.first = node;
}

fn listAddLast(list: *DynList, node: *dyn_block_t) void {
    node.list_next = null;
    node.list_prev = list.last;
    if (list.last) |last| {
        last.list_next = node;
    } else {
        list.first = node;
    }
    list.last = node;
}

fn heapCreate(start_size: usize) *mem_heap_t {
    const heap = std.heap.page_allocator.create(mem_heap_t) catch @panic("dyn heap alloc failed");
    heap.* = mem_heap_t.init(std.heap.page_allocator, start_size, .dynamic) catch {
        std.heap.page_allocator.destroy(heap);
        @panic("dyn heap init failed");
    };
    return heap;
}

fn heapDestroy(heap: *mem_heap_t) void {
    heap.deinit();
    std.heap.page_allocator.destroy(heap);
}

pub fn dyn_array_add_block(arr: *dyn_array_t) *dyn_block_t {
    std.debug.assert(arr.magic_n == DYN_BLOCK_MAGIC_N);

    if (arr.heap == null) {
        listInit(&arr.base);
        listAddFirst(&arr.base, arr);
        arr.heap = heapCreate(@sizeOf(dyn_block_t));
    }

    var block = dyn_array_get_last_block(arr);
    block.used |= DYN_BLOCK_FULL_FLAG;

    const heap = arr.heap.?;
    const buf = heap.alloc(@sizeOf(dyn_block_t)) catch @panic("dyn block alloc failed");
    const new_block = @as(*dyn_block_t, @ptrCast(@alignCast(buf.ptr)));
    new_block.* = dyn_block_t{
        .heap = null,
        .used = 0,
        .data = undefined,
        .base = .{},
        .list_prev = null,
        .list_next = null,
        .buf_end = 0,
        .magic_n = DYN_BLOCK_MAGIC_N,
    };
    listAddLast(&arr.base, new_block);
    return new_block;
}

pub fn dyn_array_get_first_block(arr: *dyn_array_t) *dyn_block_t {
    return arr;
}

pub fn dyn_array_get_last_block(arr: *dyn_array_t) *dyn_block_t {
    if (arr.heap == null) {
        return arr;
    }
    std.debug.assert(arr.base.last != null);
    return arr.base.last.?;
}

pub fn dyn_array_get_next_block(arr: *dyn_array_t, block: *dyn_block_t) ?*dyn_block_t {
    std.debug.assert(arr.heap == null or arr.base.first != null);
    if (arr.heap == null) {
        std.debug.assert(arr == block);
        return null;
    }
    return block.list_next;
}

pub fn dyn_block_get_used(block: *dyn_block_t) ulint {
    return block.used & ~DYN_BLOCK_FULL_FLAG;
}

pub fn dyn_block_get_data(block: *dyn_block_t) [*]byte {
    return block.data[0..].ptr;
}

pub fn dyn_array_create(arr: *dyn_array_t) *dyn_array_t {
    arr.heap = null;
    arr.used = 0;
    arr.base = .{};
    arr.list_prev = null;
    arr.list_next = null;
    arr.buf_end = 0;
    arr.magic_n = DYN_BLOCK_MAGIC_N;
    return arr;
}

pub fn dyn_array_free(arr: *dyn_array_t) void {
    if (arr.heap) |heap| {
        heapDestroy(heap);
    }
    arr.heap = null;
    arr.magic_n = 0;
}

pub fn dyn_array_push(arr: *dyn_array_t, size: ulint) [*]byte {
    std.debug.assert(arr.magic_n == DYN_BLOCK_MAGIC_N);
    std.debug.assert(size <= DYN_ARRAY_DATA_SIZE);
    std.debug.assert(size > 0);

    var block: *dyn_block_t = arr;
    var used = block.used;

    if (used + size > DYN_ARRAY_DATA_SIZE) {
        block = dyn_array_get_last_block(arr);
        used = block.used;

        if (used + size > DYN_ARRAY_DATA_SIZE) {
            block = dyn_array_add_block(arr);
            used = block.used;
        }
    }

    block.used = used + size;
    std.debug.assert(block.used <= DYN_ARRAY_DATA_SIZE);
    return block.data[0..].ptr + used;
}

pub fn dyn_array_open(arr: *dyn_array_t, size: ulint) [*]byte {
    std.debug.assert(arr.magic_n == DYN_BLOCK_MAGIC_N);
    std.debug.assert(size <= DYN_ARRAY_DATA_SIZE);
    std.debug.assert(size > 0);

    var block: *dyn_block_t = arr;
    var used = block.used;

    if (used + size > DYN_ARRAY_DATA_SIZE) {
        block = dyn_array_get_last_block(arr);
        used = block.used;

        if (used + size > DYN_ARRAY_DATA_SIZE) {
            block = dyn_array_add_block(arr);
            used = block.used;
            std.debug.assert(size <= DYN_ARRAY_DATA_SIZE);
        }
    }

    std.debug.assert(block.used <= DYN_ARRAY_DATA_SIZE);
    std.debug.assert(arr.buf_end == 0);
    arr.buf_end = used + size;
    return block.data[0..].ptr + used;
}

pub fn dyn_array_close(arr: *dyn_array_t, ptr: [*]byte) void {
    std.debug.assert(arr.magic_n == DYN_BLOCK_MAGIC_N);

    const block = dyn_array_get_last_block(arr);
    const base = @intFromPtr(block.data[0..].ptr);
    const end_ptr = @intFromPtr(ptr);
    std.debug.assert(end_ptr >= base);

    block.used = @as(ulint, @intCast(end_ptr - base));
    std.debug.assert(block.used <= DYN_ARRAY_DATA_SIZE);
    arr.buf_end = 0;
}

pub fn dyn_array_get_element(arr: *dyn_array_t, pos: ulint) [*]byte {
    std.debug.assert(arr.magic_n == DYN_BLOCK_MAGIC_N);

    var block = dyn_array_get_first_block(arr);
    if (arr.heap != null) {
        var used = dyn_block_get_used(block);
        var remaining = pos;
        while (remaining >= used) {
            remaining -= used;
            block = block.list_next.?;
            used = dyn_block_get_used(block);
        }
        return block.data[0..].ptr + remaining;
    }

    return block.data[0..].ptr + pos;
}

pub fn dyn_array_get_data_size(arr: *dyn_array_t) ulint {
    std.debug.assert(arr.magic_n == DYN_BLOCK_MAGIC_N);
    if (arr.heap == null) {
        return arr.used;
    }

    var sum: ulint = 0;
    var block: ?*dyn_block_t = dyn_array_get_first_block(arr);
    while (block) |cur| {
        sum += dyn_block_get_used(cur);
        block = dyn_array_get_next_block(arr, cur);
    }
    return sum;
}

pub fn dyn_push_string(arr: *dyn_array_t, str: [*]const byte, len: ulint) void {
    var remaining = len;
    var src = str;
    while (remaining > 0) {
        const n_copied: ulint = if (remaining > DYN_ARRAY_DATA_SIZE) DYN_ARRAY_DATA_SIZE else remaining;
        const buf = dyn_array_push(arr, n_copied);
        const dst_slice = buf[0..n_copied];
        const src_slice = src[0..n_copied];
        std.mem.copyForwards(byte, dst_slice, src_slice);
        src += n_copied;
        remaining -= n_copied;
    }
}

test "dyn array create and push within one block" {
    var arr: dyn_array_t = undefined;
    _ = dyn_array_create(&arr);
    defer dyn_array_free(&arr);

    const buf = dyn_array_push(&arr, 4);
    buf[0] = 0x1;
    buf[1] = 0x2;
    buf[2] = 0x3;
    buf[3] = 0x4;

    try std.testing.expect(dyn_array_get_data_size(&arr) == 4);

    const elem = dyn_array_get_element(&arr, 0);
    try std.testing.expect(elem[0] == 0x1);
    try std.testing.expect(elem[3] == 0x4);
}

test "dyn array spans multiple blocks" {
    var arr: dyn_array_t = undefined;
    _ = dyn_array_create(&arr);
    defer dyn_array_free(&arr);

    const first = dyn_array_push(&arr, 400);
    std.mem.set(byte, first[0..400], 0xAA);

    const second = dyn_array_push(&arr, 200);
    std.mem.set(byte, second[0..200], 0xBB);

    try std.testing.expect(dyn_array_get_data_size(&arr) == 600);
    try std.testing.expect(dyn_array_get_next_block(&arr, &arr) != null);
    const cross = dyn_array_get_element(&arr, 399);
    try std.testing.expect(cross[0] == 0xAA);
    const after = dyn_array_get_element(&arr, 400);
    try std.testing.expect(after[0] == 0xBB);
}

test "dyn array open and close" {
    var arr: dyn_array_t = undefined;
    _ = dyn_array_create(&arr);
    defer dyn_array_free(&arr);

    const buf = dyn_array_open(&arr, 128);
    std.mem.set(byte, buf[0..100], 0xCC);
    dyn_array_close(&arr, buf + 100);

    try std.testing.expect(dyn_array_get_data_size(&arr) == 100);
    const elem = dyn_array_get_element(&arr, 99);
    try std.testing.expect(elem[0] == 0xCC);
}

test "dyn push string spans blocks" {
    var arr: dyn_array_t = undefined;
    _ = dyn_array_create(&arr);
    defer dyn_array_free(&arr);

    var payload: [DYN_ARRAY_DATA_SIZE * 2 + 10]byte = undefined;
    for (payload, 0..) |*b, idx| {
        b.* = @as(byte, @intCast(idx % 251));
    }

    dyn_push_string(&arr, payload[0..].ptr, payload.len);
    try std.testing.expect(dyn_array_get_data_size(&arr) == payload.len);

    const first = dyn_array_get_element(&arr, 0);
    const middle = dyn_array_get_element(&arr, DYN_ARRAY_DATA_SIZE);
    const last = dyn_array_get_element(&arr, payload.len - 1);
    try std.testing.expect(first[0] == payload[0]);
    try std.testing.expect(middle[0] == payload[DYN_ARRAY_DATA_SIZE]);
    try std.testing.expect(last[0] == payload[payload.len - 1]);
}
