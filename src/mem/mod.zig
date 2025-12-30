const std = @import("std");
const compat = @import("../ut/compat.zig");

pub const module_name = "mem";
pub const heap = @import("heap.zig");

pub const byte = compat.byte;
pub const ulint = compat.ulint;
pub const mem_heap_t = heap.MemHeap;

pub const MEM_HEAP_DYNAMIC: ulint = 0;
pub const MEM_HEAP_BUFFER: ulint = 1;
pub const MEM_HEAP_BTR_SEARCH: ulint = 2;

fn heapTypeFromFlags(flags: ulint) heap.HeapType {
    if ((flags & MEM_HEAP_BTR_SEARCH) != 0) {
        return .btr_search;
    }
    if ((flags & MEM_HEAP_BUFFER) != 0) {
        return .buffer;
    }
    return .dynamic;
}

pub fn mem_init(size: ulint) void {
    _ = size;
}

pub fn mem_close() void {}

pub fn mem_heap_create_func(n: ulint, flags: ulint) ?*mem_heap_t {
    const heap_type = heapTypeFromFlags(flags);
    const heap_ptr = std.heap.page_allocator.create(mem_heap_t) catch return null;
    heap_ptr.* = mem_heap_t.init(std.heap.page_allocator, @as(usize, @intCast(n)), heap_type) catch {
        std.heap.page_allocator.destroy(heap_ptr);
        return null;
    };
    return heap_ptr;
}

pub fn mem_heap_free_func(heap_ptr: *mem_heap_t) void {
    heap_ptr.deinit();
    std.heap.page_allocator.destroy(heap_ptr);
}

pub fn mem_heap_alloc(heap_ptr: *mem_heap_t, n: ulint) ?[]u8 {
    return heap_ptr.alloc(@as(usize, @intCast(n))) catch null;
}

pub fn mem_heap_zalloc(heap_ptr: *mem_heap_t, n: ulint) ?[]u8 {
    return heap_ptr.zalloc(@as(usize, @intCast(n))) catch null;
}

pub fn mem_heap_get_top(heap_ptr: *mem_heap_t, n: ulint) []u8 {
    return heap_ptr.getTop(@as(usize, @intCast(n)));
}

pub fn mem_heap_free_top(heap_ptr: *mem_heap_t, n: ulint) void {
    heap_ptr.freeTop(@as(usize, @intCast(n)));
}

pub fn mem_heap_empty(heap_ptr: *mem_heap_t) void {
    heap_ptr.release(.{ .block_index = 0, .used = 0 });
}

pub fn mem_heap_get_size(heap_ptr: *mem_heap_t) ulint {
    return @as(ulint, @intCast(heap_ptr.totalSize()));
}

pub fn mem_heap_dup(heap_ptr: *mem_heap_t, data: []const u8) ?[]u8 {
    const buf = mem_heap_alloc(heap_ptr, data.len) orelse return null;
    std.mem.copyForwards(u8, buf, data);
    return buf;
}

pub fn mem_heap_strdup(heap_ptr: *mem_heap_t, str: []const u8) ?[]u8 {
    const buf = mem_heap_alloc(heap_ptr, str.len + 1) orelse return null;
    std.mem.copyForwards(u8, buf[0..str.len], str);
    buf[str.len] = 0;
    return buf;
}

pub fn mem_heap_strcat(heap_ptr: *mem_heap_t, s1: []const u8, s2: []const u8) ?[]u8 {
    const buf = mem_heap_alloc(heap_ptr, s1.len + s2.len + 1) orelse return null;
    std.mem.copyForwards(u8, buf[0..s1.len], s1);
    std.mem.copyForwards(u8, buf[s1.len .. s1.len + s2.len], s2);
    buf[s1.len + s2.len] = 0;
    return buf;
}

pub const MemPrintfArg = union(enum) {
    str: []const u8,
    ulong: ulint,
};

fn mem_heap_printf_low(buf: ?[]u8, format: []const u8, args: []const MemPrintfArg) usize {
    var out_len: usize = 0;
    var arg_idx: usize = 0;
    var i: usize = 0;

    while (i < format.len) : (i += 1) {
        if (format[i] != '%') {
            if (buf) |out| {
                out[out_len] = format[i];
            }
            out_len += 1;
            continue;
        }

        i += 1;
        var is_long = false;
        if (i < format.len and format[i] == 'l') {
            is_long = true;
            i += 1;
        }
        std.debug.assert(i < format.len);

        switch (format[i]) {
            's' => {
                std.debug.assert(!is_long);
                std.debug.assert(arg_idx < args.len);
                const val = args[arg_idx];
                arg_idx += 1;
                const s = switch (val) {
                    .str => |v| v,
                    else => @panic("mem_heap_printf expected string"),
                };
                if (buf) |out| {
                    std.mem.copyForwards(u8, out[out_len .. out_len + s.len], s);
                }
                out_len += s.len;
            },
            'u' => {
                std.debug.assert(is_long);
                std.debug.assert(arg_idx < args.len);
                const val = args[arg_idx];
                arg_idx += 1;
                const num = switch (val) {
                    .ulong => |v| v,
                    else => @panic("mem_heap_printf expected ulong"),
                };
                var tmp: [32]u8 = undefined;
                const len = std.fmt.formatIntBuf(&tmp, num, 10, .lower, .{});
                if (buf) |out| {
                    std.mem.copyForwards(u8, out[out_len .. out_len + len], tmp[0..len]);
                }
                out_len += len;
            },
            '%' => {
                std.debug.assert(!is_long);
                if (buf) |out| {
                    out[out_len] = '%';
                }
                out_len += 1;
            },
            else => @panic("mem_heap_printf unsupported format"),
        }
    }

    if (buf) |out| {
        out[out_len] = 0;
    }
    return out_len + 1;
}

pub fn mem_heap_printf(heap_ptr: *mem_heap_t, format: []const u8, args: []const MemPrintfArg) ?[]u8 {
    const len = mem_heap_printf_low(null, format, args);
    const buf = mem_heap_alloc(heap_ptr, @as(ulint, @intCast(len))) orelse return null;
    _ = mem_heap_printf_low(buf, format, args);
    return buf;
}

test "mem heap helpers" {
    const heap_ptr = mem_heap_create_func(0, MEM_HEAP_DYNAMIC) orelse return error.OutOfMemory;
    defer mem_heap_free_func(heap_ptr);

    const dup = mem_heap_dup(heap_ptr, "abc") orelse return error.OutOfMemory;
    try std.testing.expect(std.mem.eql(u8, dup, "abc"));

    const s1 = mem_heap_strdup(heap_ptr, "foo") orelse return error.OutOfMemory;
    try std.testing.expect(s1[s1.len - 1] == 0);

    const cat = mem_heap_strcat(heap_ptr, "foo", "bar") orelse return error.OutOfMemory;
    try std.testing.expect(std.mem.eql(u8, cat[0 .. cat.len - 1], "foobar"));

    const fmt = mem_heap_printf(heap_ptr, "x=%lu", &[_]MemPrintfArg{.{ .ulong = 42 }}) orelse return error.OutOfMemory;
    try std.testing.expect(std.mem.eql(u8, fmt[0 .. fmt.len - 1], "x=42"));

    const zeroed = mem_heap_zalloc(heap_ptr, 4) orelse return error.OutOfMemory;
    try std.testing.expect(std.mem.allEqual(u8, zeroed, 0));
}
