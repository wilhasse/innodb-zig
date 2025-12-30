const std = @import("std");
const builtin = @import("builtin");
const compat = @import("../ut/compat.zig");

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;

pub const os_process_t = ?*anyopaque;
pub const os_process_id_t = ulint;

pub var os_use_large_pages: ibool = compat.FALSE;
pub var os_large_page_size: ulint = 0;

pub fn os_proc_var_init() void {
    os_use_large_pages = 0;
    os_large_page_size = 0;
}

pub fn os_proc_get_number() ulint {
    if (builtin.os.tag == .windows) {
        return @as(ulint, @intCast(std.os.windows.kernel32.GetCurrentProcessId()));
    }
    return @as(ulint, @intCast(std.c.getpid()));
}

pub fn os_mem_alloc_large(n: *ulint) ?*anyopaque {
    if (n.* == 0) {
        return null;
    }

    var alignment = std.mem.page_size;
    if (os_use_large_pages == compat.TRUE and os_large_page_size > 0 and compat.ut_is_2pow(os_large_page_size)) {
        alignment = @as(usize, @intCast(os_large_page_size));
    }

    const size = compat.alignUp(@as(usize, n.*), alignment);
    n.* = size;

    const buf = std.heap.page_allocator.alignedAlloc(u8, alignment, size) catch return null;
    std.mem.set(u8, buf, 0);
    return buf.ptr;
}

pub fn os_mem_free_large(ptr: *anyopaque, size: ulint) void {
    if (size == 0) {
        return;
    }
    const slice = @as([*]u8, @ptrCast(ptr))[0..@as(usize, size)];
    std.heap.page_allocator.free(slice);
}

test "os proc var init resets flags" {
    os_use_large_pages = compat.TRUE;
    os_large_page_size = 4096;
    os_proc_var_init();
    try std.testing.expectEqual(@as(ibool, 0), os_use_large_pages);
    try std.testing.expectEqual(@as(ulint, 0), os_large_page_size);
}

test "os proc get number non-zero" {
    try std.testing.expect(os_proc_get_number() != 0);
}

test "os mem alloc large alignment" {
    os_proc_var_init();
    var size: ulint = 1000;
    const ptr = os_mem_alloc_large(&size) orelse return error.OutOfMemory;
    defer os_mem_free_large(ptr, size);

    const alignment = std.mem.page_size;
    try std.testing.expect(size >= 1000);
    try std.testing.expect(@intFromPtr(ptr) % alignment == 0);
}
