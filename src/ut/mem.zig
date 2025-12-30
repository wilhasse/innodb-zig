const std = @import("std");
const compat = @import("compat.zig");
const log = @import("log.zig");
const os_sync = @import("../os/sync.zig");

pub const module_name = "ut.mem";

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;

pub var ut_total_allocated_memory: ulint = 0;
pub var ut_list_mutex: os_sync.os_fast_mutex_t = .{};

const UT_MEM_MAGIC_N: ulint = 1601650166;

const ut_mem_block_t = struct {
    prev: ?*ut_mem_block_t = null,
    next: ?*ut_mem_block_t = null,
    size: ulint = 0,
    magic_n: ulint = UT_MEM_MAGIC_N,
};

const MemBlockList = struct {
    first: ?*ut_mem_block_t = null,
    last: ?*ut_mem_block_t = null,
};

var ut_mem_block_list: MemBlockList = .{};
var ut_mem_block_list_inited: bool = false;
var ut_mem_null_ptr: ?*ulint = null;

fn listAddFirst(block: *ut_mem_block_t) void {
    block.prev = null;
    block.next = ut_mem_block_list.first;
    if (ut_mem_block_list.first) |first| {
        first.prev = block;
    } else {
        ut_mem_block_list.last = block;
    }
    ut_mem_block_list.first = block;
}

fn listRemove(block: *ut_mem_block_t) void {
    if (block.prev) |prev| {
        prev.next = block.next;
    } else {
        ut_mem_block_list.first = block.next;
    }

    if (block.next) |next| {
        next.prev = block.prev;
    } else {
        ut_mem_block_list.last = block.prev;
    }
    block.prev = null;
    block.next = null;
}

fn headerFromPayload(ptr: *anyopaque) *ut_mem_block_t {
    const addr = @intFromPtr(ptr);
    return @as(*ut_mem_block_t, @ptrFromInt(addr - @sizeOf(ut_mem_block_t)));
}

fn cStrLen(str: [*]const u8) ulint {
    var i: ulint = 0;
    while (str[i] != 0) : (i += 1) {}
    return i;
}

fn cSlice(str: [*]const u8) []const u8 {
    const len = cStrLen(str);
    return str[0..@as(usize, @intCast(len))];
}

pub fn ut_mem_var_init() void {
    ut_total_allocated_memory = 0;
    ut_mem_block_list = .{};
    ut_list_mutex = .{};
    ut_mem_block_list_inited = false;
    ut_mem_null_ptr = null;
}

pub fn ut_mem_init() void {
    if (!ut_mem_block_list_inited) {
        os_sync.os_fast_mutex_init(&ut_list_mutex);
        ut_mem_block_list = .{};
        ut_mem_block_list_inited = true;
    }
}

pub fn ut_malloc_low(n: ulint, set_to_zero: ibool, assert_on_error: ibool) ?*anyopaque {
    std.debug.assert(@sizeOf(ut_mem_block_t) % 8 == 0);
    std.debug.assert(ut_mem_block_list_inited);

    const total = @as(usize, @intCast(n)) + @sizeOf(ut_mem_block_t);
    const buf = std.heap.page_allocator.alloc(u8, total) catch {
        if (assert_on_error != 0) {
            @panic("ut_malloc_low: out of memory");
        }
        return null;
    };

    if (set_to_zero != 0) {
        std.mem.set(u8, buf, 0);
    }

    const block = @as(*ut_mem_block_t, @ptrCast(@alignCast(buf.ptr)));
    block.* = .{
        .size = @as(ulint, @intCast(total)),
        .magic_n = UT_MEM_MAGIC_N,
    };

    os_sync.os_fast_mutex_lock(&ut_list_mutex);
    ut_total_allocated_memory += block.size;
    listAddFirst(block);
    os_sync.os_fast_mutex_unlock(&ut_list_mutex);

    return @as(*anyopaque, @ptrCast(buf.ptr + @sizeOf(ut_mem_block_t)));
}

pub fn ut_malloc(n: ulint) ?*anyopaque {
    return ut_malloc_low(n, compat.TRUE, compat.TRUE);
}

pub fn ut_test_malloc(n: ulint) ibool {
    const buf = std.heap.page_allocator.alloc(u8, @as(usize, @intCast(n))) catch {
        log.logf("InnoDB: Error: cannot allocate {d} bytes of memory\n", .{n});
        return compat.FALSE;
    };
    std.heap.page_allocator.free(buf);
    return compat.TRUE;
}

pub fn ut_free(ptr: ?*anyopaque) void {
    if (ptr == null) {
        return;
    }

    const block = headerFromPayload(ptr.?);
    os_sync.os_fast_mutex_lock(&ut_list_mutex);
    std.debug.assert(block.magic_n == UT_MEM_MAGIC_N);
    std.debug.assert(ut_total_allocated_memory >= block.size);
    ut_total_allocated_memory -= block.size;
    listRemove(block);
    os_sync.os_fast_mutex_unlock(&ut_list_mutex);

    const slice = @as([*]u8, @ptrCast(block))[0..@as(usize, @intCast(block.size))];
    std.heap.page_allocator.free(slice);
}

pub fn ut_realloc(ptr: ?*anyopaque, size: ulint) ?*anyopaque {
    if (ptr == null) {
        return ut_malloc(size);
    }
    if (size == 0) {
        ut_free(ptr);
        return null;
    }

    const block = headerFromPayload(ptr.?);
    std.debug.assert(block.magic_n == UT_MEM_MAGIC_N);

    const old_size = block.size - @as(ulint, @intCast(@sizeOf(ut_mem_block_t)));
    const min_size = if (size < old_size) size else old_size;
    const new_ptr = ut_malloc(size) orelse return null;

    const dst = @as([*]u8, @ptrCast(new_ptr))[0..@as(usize, @intCast(min_size))];
    const src = @as([*]const u8, @ptrCast(ptr.?))[0..@as(usize, @intCast(min_size))];
    std.mem.copyForwards(u8, dst, src);
    ut_free(ptr);
    return new_ptr;
}

pub fn ut_free_all_mem() void {
    if (!ut_mem_block_list_inited) {
        return;
    }

    os_sync.os_fast_mutex_free(&ut_list_mutex);

    while (ut_mem_block_list.first) |block| {
        std.debug.assert(block.magic_n == UT_MEM_MAGIC_N);
        std.debug.assert(ut_total_allocated_memory >= block.size);
        ut_total_allocated_memory -= block.size;
        listRemove(block);
        const slice = @as([*]u8, @ptrCast(block))[0..@as(usize, @intCast(block.size))];
        std.heap.page_allocator.free(slice);
    }

    if (ut_total_allocated_memory != 0) {
        log.logf("InnoDB: Warning: after shutdown total allocated memory is {d}\n", .{ut_total_allocated_memory});
    }

    ut_mem_block_list_inited = false;
}

pub fn ut_memcpy(dest: *anyopaque, sour: *const anyopaque, n: ulint) *anyopaque {
    const dst = @as([*]u8, @ptrCast(dest))[0..@as(usize, @intCast(n))];
    const src = @as([*]const u8, @ptrCast(sour))[0..@as(usize, @intCast(n))];
    std.mem.copyForwards(u8, dst, src);
    return dest;
}

pub fn ut_memmove(dest: *anyopaque, sour: *const anyopaque, n: ulint) *anyopaque {
    const dst = @as([*]u8, @ptrCast(dest))[0..@as(usize, @intCast(n))];
    const src = @as([*]const u8, @ptrCast(sour))[0..@as(usize, @intCast(n))];
    if (@intFromPtr(dst.ptr) <= @intFromPtr(src.ptr)) {
        std.mem.copyForwards(u8, dst, src);
    } else {
        std.mem.copyBackwards(u8, dst, src);
    }
    return dest;
}

pub fn ut_memcmp(str1: *const anyopaque, str2: *const anyopaque, n: ulint) i32 {
    const s1 = @as([*]const u8, @ptrCast(str1))[0..@as(usize, @intCast(n))];
    const s2 = @as([*]const u8, @ptrCast(str2))[0..@as(usize, @intCast(n))];
    var i: usize = 0;
    while (i < s1.len) : (i += 1) {
        if (s1[i] != s2[i]) {
            return if (s1[i] < s2[i]) -1 else 1;
        }
    }
    return 0;
}

pub fn ut_strcpy(dest: [*]u8, sour: [*]const u8) [*]u8 {
    var i: usize = 0;
    while (true) : (i += 1) {
        dest[i] = sour[i];
        if (sour[i] == 0) {
            break;
        }
    }
    return dest;
}

pub fn ut_strlen(str: [*]const u8) ulint {
    return cStrLen(str);
}

pub fn ut_strcmp(str1: [*]const u8, str2: [*]const u8) i32 {
    var i: usize = 0;
    while (true) : (i += 1) {
        const a = str1[i];
        const b = str2[i];
        if (a != b) {
            return if (a < b) -1 else 1;
        }
        if (a == 0) {
            return 0;
        }
    }
}

pub fn ut_strlcpy(dst: [*]u8, src: [*]const u8, size: ulint) ulint {
    const src_size = cStrLen(src);
    if (size != 0) {
        const max_copy = size - 1;
        const n = if (src_size < max_copy) src_size else max_copy;
        const dst_slice = dst[0..@as(usize, @intCast(n))];
        const src_slice = src[0..@as(usize, @intCast(n))];
        std.mem.copyForwards(u8, dst_slice, src_slice);
        dst[@as(usize, @intCast(n))] = 0;
    }
    return src_size;
}

pub fn ut_strlcpy_rev(dst: [*]u8, src: [*]const u8, size: ulint) ulint {
    const src_size = cStrLen(src);
    if (size != 0) {
        const max_copy = size - 1;
        const n = if (src_size < max_copy) src_size else max_copy;
        const start = src_size - n;
        const dst_slice = dst[0..@as(usize, @intCast(n))];
        const src_slice = src[@as(usize, @intCast(start))..@as(usize, @intCast(start + n))];
        std.mem.copyForwards(u8, dst_slice, src_slice);
        dst[@as(usize, @intCast(n))] = 0;
    }
    return src_size;
}

pub fn ut_strlenq(str: [*]const u8, q: u8) ulint {
    var len: ulint = 0;
    var i: ulint = 0;
    while (str[i] != 0) : (i += 1) {
        len += 1;
        if (str[i] == q) {
            len += 1;
        }
    }
    return len;
}

pub fn ut_strcpyq(dest: [*]u8, q: u8, src: [*]const u8) [*]u8 {
    var di: usize = 0;
    var si: usize = 0;
    while (src[si] != 0) : (si += 1) {
        dest[di] = src[si];
        di += 1;
        if (src[si] == q) {
            dest[di] = q;
            di += 1;
        }
    }
    return dest + di;
}

pub fn ut_memcpyq(dest: [*]u8, q: u8, src: [*]const u8, len: ulint) [*]u8 {
    var di: usize = 0;
    var si: usize = 0;
    while (si < @as(usize, @intCast(len))) : (si += 1) {
        dest[di] = src[si];
        di += 1;
        if (src[si] == q) {
            dest[di] = q;
            di += 1;
        }
    }
    return dest + di;
}

pub fn ut_strcount(s1: [*]const u8, s2: [*]const u8) ulint {
    const haystack = cSlice(s1);
    const needle = cSlice(s2);
    if (needle.len == 0) {
        return 0;
    }
    var count: ulint = 0;
    var pos: usize = 0;
    while (pos <= haystack.len) {
        const idx = std.mem.indexOfPos(u8, haystack, pos, needle) orelse break;
        count += 1;
        pos = idx + needle.len;
    }
    return count;
}

pub fn ut_strreplace(str: [*]const u8, s1: [*]const u8, s2: [*]const u8) ?[*]u8 {
    const str_slice = cSlice(str);
    const s1_slice = cSlice(s1);
    const s2_slice = cSlice(s2);
    if (s1_slice.len == 0) {
        const out = ut_malloc(@as(ulint, @intCast(str_slice.len + 1))) orelse return null;
        const dst = @as([*]u8, @ptrCast(out))[0..str_slice.len];
        std.mem.copyForwards(u8, dst, str_slice);
        dst[str_slice.len] = 0;
        return @as([*]u8, @ptrCast(out));
    }

    const len_delta_signed: isize = @as(isize, @intCast(s2_slice.len)) - @as(isize, @intCast(s1_slice.len));
    const extra = if (len_delta_signed > 0) ut_strcount(str, s1) * @as(ulint, @intCast(len_delta_signed)) else 0;
    const total_len = str_slice.len + @as(usize, @intCast(extra)) + 1;

    const out = ut_malloc(@as(ulint, @intCast(total_len))) orelse return null;
    const dst_full = @as([*]u8, @ptrCast(out))[0..total_len];
    var dst_idx: usize = 0;
    var src_pos: usize = 0;

    while (src_pos <= str_slice.len) {
        const next = std.mem.indexOfPos(u8, str_slice, src_pos, s1_slice) orelse str_slice.len;
        const chunk_len = next - src_pos;
        if (chunk_len > 0) {
            std.mem.copyForwards(u8, dst_full[dst_idx .. dst_idx + chunk_len], str_slice[src_pos..next]);
            dst_idx += chunk_len;
        }
        if (next == str_slice.len) {
            break;
        }
        std.mem.copyForwards(u8, dst_full[dst_idx .. dst_idx + s2_slice.len], s2_slice);
        dst_idx += s2_slice.len;
        src_pos = next + s1_slice.len;
    }

    dst_full[dst_idx] = 0;
    return @as([*]u8, @ptrCast(out));
}

pub fn ut_raw_to_hex(raw: *const anyopaque, raw_size: ulint, hex: [*]u8, hex_size: ulint) ulint {
    if (hex_size == 0) {
        return 0;
    }

    const raw_slice = @as([*]const u8, @ptrCast(raw))[0..@as(usize, @intCast(raw_size))];
    const out_size = @as(usize, @intCast(hex_size));
    const read_bytes: usize = if (hex_size <= 2 * raw_size) @as(usize, @intCast(hex_size / 2)) else raw_slice.len;
    const write_bytes: ulint = if (hex_size <= 2 * raw_size) hex_size else 2 * raw_size + 1;

    const hex_chars = "0123456789ABCDEF";
    var i: usize = 0;
    while (i < read_bytes) : (i += 1) {
        const byte_val = raw_slice[i];
        const high = hex_chars[@as(usize, byte_val >> 4)];
        const low = hex_chars[@as(usize, byte_val & 0x0F)];
        hex[2 * i] = high;
        hex[2 * i + 1] = low;
    }

    var term_index: usize = 2 * read_bytes;
    if (hex_size <= 2 * raw_size and hex_size % 2 == 0) {
        if (term_index > 0) {
            term_index -= 1;
        }
    }
    if (term_index < out_size) {
        hex[term_index] = 0;
    }

    return write_bytes;
}

pub fn ut_str_sql_format(str: [*]const u8, str_len: ulint, buf: [*]u8, buf_size: ulint) ulint {
    var buf_i: ulint = 0;

    switch (buf_size) {
        3 => {
            if (str_len == 0) {
                buf[buf_i] = '\'';
                buf_i += 1;
                buf[buf_i] = '\'';
                buf_i += 1;
            }
            buf[buf_i] = 0;
            buf_i += 1;
            return buf_i;
        },
        2, 1 => {
            buf[buf_i] = 0;
            buf_i += 1;
            return buf_i;
        },
        0 => return 0,
        else => {},
    }

    buf[0] = '\'';
    buf_i = 1;
    var str_i: ulint = 0;

    loop: while (str_i < str_len) : (str_i += 1) {
        if (buf_size - buf_i == 2) {
            break :loop;
        }

        const ch = str[@as(usize, @intCast(str_i))];
        switch (ch) {
            0 => {
                if (buf_size - buf_i < 4) {
                    break :loop;
                }
                buf[@as(usize, @intCast(buf_i))] = '\\';
                buf_i += 1;
                buf[@as(usize, @intCast(buf_i))] = '0';
                buf_i += 1;
            },
            '\'', '\\' => {
                if (buf_size - buf_i < 4) {
                    break :loop;
                }
                buf[@as(usize, @intCast(buf_i))] = ch;
                buf_i += 1;
                buf[@as(usize, @intCast(buf_i))] = ch;
                buf_i += 1;
            },
            else => {
                buf[@as(usize, @intCast(buf_i))] = ch;
                buf_i += 1;
            },
        }
    }

    buf[@as(usize, @intCast(buf_i))] = '\'';
    buf_i += 1;
    buf[@as(usize, @intCast(buf_i))] = 0;
    buf_i += 1;
    return buf_i;
}

test "ut malloc free and realloc" {
    ut_mem_var_init();
    ut_mem_init();
    defer ut_free_all_mem();

    const before = ut_total_allocated_memory;
    const ptr = ut_malloc(16) orelse return error.OutOfMemory;
    try std.testing.expect(ut_total_allocated_memory > before);
    const buf = @as([*]u8, @ptrCast(ptr))[0..16];
    for (buf, 0..) |*b, i| {
        b.* = @as(u8, @intCast(i));
    }

    const ptr2 = ut_realloc(ptr, 32) orelse return error.OutOfMemory;
    const buf2 = @as([*]const u8, @ptrCast(ptr2))[0..16];
    try std.testing.expectEqual(@as(u8, 0), buf2[0]);
    try std.testing.expectEqual(@as(u8, 15), buf2[15]);
    ut_free(ptr2);
    try std.testing.expectEqual(before, ut_total_allocated_memory);
}

test "ut mem copy and compare" {
    var src = [_]u8{ 1, 2, 3, 4 };
    var dst = [_]u8{ 0, 0, 0, 0 };
    _ = ut_memcpy(@ptrCast(dst.ptr), @ptrCast(src.ptr), 4);
    try std.testing.expect(std.mem.eql(u8, dst[0..], src[0..]));

    var overlap = [_]u8{ 1, 2, 3, 4, 5 };
    _ = ut_memmove(@ptrCast(overlap.ptr + 1), @ptrCast(overlap.ptr), 4);
    const expected = [_]u8{ 1, 1, 2, 3, 4 };
    try std.testing.expectEqualSlices(u8, expected[0..], overlap[0..5]);

    const cmp = ut_memcmp(@ptrCast(src.ptr), @ptrCast(dst.ptr), 4);
    try std.testing.expectEqual(@as(i32, 0), cmp);
}

test "ut string helpers" {
    var buf = [_]u8{0} ** 16;
    const src: [:0]const u8 = "hello";
    _ = ut_strlcpy(buf[0..].ptr, src.ptr, 5);
    try std.testing.expectEqualStrings("hell", std.mem.span(@as([*:0]const u8, @ptrCast(&buf))));

    _ = ut_strlcpy(buf[0..].ptr, src.ptr, 6);
    try std.testing.expectEqualStrings("hello", std.mem.span(@as([*:0]const u8, @ptrCast(&buf))));

    const src_rev: [:0]const u8 = "abcdef";
    _ = ut_strlcpy_rev(buf[0..].ptr, src_rev.ptr, 4);
    try std.testing.expectEqualStrings("def", std.mem.span(@as([*:0]const u8, @ptrCast(&buf))));

    const s1: [:0]const u8 = "abc''";
    const s2: [:0]const u8 = "a'b'c";
    try std.testing.expectEqual(@as(ulint, 5), ut_strlen(s1.ptr));
    try std.testing.expectEqual(@as(ulint, 7), ut_strlenq(s2.ptr, '\''));

    var quote_buf = [_]u8{0} ** 16;
    const s3: [:0]const u8 = "a'b";
    const end_ptr = ut_strcpyq(quote_buf[0..].ptr, '\'', s3.ptr);
    end_ptr[0] = 0;
    try std.testing.expectEqualStrings("a''b", std.mem.span(@as([*:0]const u8, @ptrCast(&quote_buf))));

    var qmem = [_]u8{0} ** 16;
    const end_ptr2 = ut_memcpyq(qmem[0..].ptr, '\'', s3.ptr, 3);
    end_ptr2[0] = 0;
    try std.testing.expectEqualStrings("a''b", std.mem.span(@as([*:0]const u8, @ptrCast(&qmem))));
}

test "ut strcount and replace" {
    ut_mem_var_init();
    ut_mem_init();
    defer ut_free_all_mem();

    const s1: [:0]const u8 = "ababab";
    const s2: [:0]const u8 = "ab";
    const s3: [:0]const u8 = "a_b_a";
    const s4: [:0]const u8 = "a";
    const s5: [:0]const u8 = "xy";
    try std.testing.expectEqual(@as(ulint, 2), ut_strcount(s1.ptr, s2.ptr));
    const out_ptr = ut_strreplace(s3.ptr, s4.ptr, s5.ptr) orelse return error.OutOfMemory;
    defer ut_free(@ptrCast(out_ptr));
    const out = std.mem.span(@as([*:0]const u8, @ptrCast(out_ptr)));
    try std.testing.expectEqualStrings("xy_b_xy", out);
}

test "ut raw to hex" {
    var hex = [_]u8{0} ** 16;
    const raw = [_]u8{ 0xAB, 0xCD, 0x00 };
    const written = ut_raw_to_hex(raw[0..].ptr, 3, hex[0..].ptr, 7);
    try std.testing.expectEqual(@as(ulint, 7), written);
    try std.testing.expectEqualStrings("ABCD00", std.mem.span(@as([*:0]const u8, @ptrCast(&hex))));

    var small = [_]u8{0} ** 8;
    const written_small = ut_raw_to_hex(raw[0..].ptr, 3, small[0..].ptr, 4);
    try std.testing.expectEqual(@as(ulint, 4), written_small);
    try std.testing.expectEqualStrings("ABC", std.mem.span(@as([*:0]const u8, @ptrCast(&small))));
}

test "ut str sql format cases" {
    const Case = struct {
        str: []const u8,
        len: ulint,
        buf_size: ulint,
        ret: ulint,
        expected: []const u8,
    };

    const cases = [_]Case{
        .{ .str = "abcd", .len = 4, .buf_size = 0, .ret = 0, .expected = "xxxxxxxxxx" },
        .{ .str = "abcd", .len = 4, .buf_size = 1, .ret = 1, .expected = "" },
        .{ .str = "abcd", .len = 4, .buf_size = 2, .ret = 1, .expected = "" },
        .{ .str = "abcd", .len = 0, .buf_size = 3, .ret = 3, .expected = "''" },
        .{ .str = "abcd", .len = 1, .buf_size = 3, .ret = 1, .expected = "" },
        .{ .str = "abcd", .len = 0, .buf_size = 4, .ret = 3, .expected = "''" },
        .{ .str = "abcd", .len = 1, .buf_size = 4, .ret = 4, .expected = "'a'" },
        .{ .str = "'", .len = 1, .buf_size = 4, .ret = 3, .expected = "''" },
        .{ .str = "a'b'c", .len = 5, .buf_size = 32, .ret = 10, .expected = "'a''b''c'" },
        .{ .str = "a'b'c'", .len = 6, .buf_size = 32, .ret = 12, .expected = "'a''b''c'''" },
    };

    var buf = [_]u8{0} ** 128;
    for (cases) |case| {
        std.mem.set(u8, buf[0..10], 'x');
        buf[10] = 0;
        const ret = ut_str_sql_format(case.str.ptr, case.len, buf[0..].ptr, case.buf_size);
        try std.testing.expectEqual(case.ret, ret);
        const out = std.mem.span(@as([*:0]const u8, @ptrCast(&buf)));
        try std.testing.expectEqualStrings(case.expected, out);
    }
}
