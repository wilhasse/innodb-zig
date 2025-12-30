const std = @import("std");
const compat = @import("../ut/compat.zig");
const data = @import("../data/mod.zig");
const mach = @import("../mach/mod.zig");
const pars = @import("mod.zig");
const que = @import("../que/mod.zig");

fn ut_error() noreturn {
    @panic("ut_error");
}

pub fn sym_tab_create(allocator: std.mem.Allocator) *pars.sym_tab_t {
    const sym_tab = allocator.create(pars.sym_tab_t) catch ut_error();
    sym_tab.* = .{};
    sym_tab.allocator = allocator;
    return sym_tab;
}

pub fn sym_tab_free_private(sym_tab: *pars.sym_tab_t) void {
    _ = sym_tab;
}

pub fn sym_tab_add_int_lit(sym_tab: *pars.sym_tab_t, val: compat.ulint) *pars.sym_node_t {
    const allocator = sym_tab.allocator;
    const node = allocator.create(pars.sym_node_t) catch ut_error();
    node.* = .{};
    node.common.type = que.QUE_NODE_SYMBOL;
    node.resolved = compat.TRUE;
    node.token_type = .SYM_LIT;
    node.indirection = null;
    data.dtype_set(&node.common.val.type, data.DATA_INT, 0, 4);
    const buf = allocator.alloc(compat.byte, 4) catch ut_error();
    mach.mach_write_to_4(buf.ptr, val);
    data.dfield_set_data(&node.common.val, buf.ptr, 4);
    node.common.val_buf_size = 0;
    node.prefetch_buf = null;
    node.cursor_def = null;
    node.sym_table = sym_tab;
    return node;
}

pub fn sym_tab_add_str_lit(sym_tab: *pars.sym_tab_t, str: []const u8, len: compat.ulint) *pars.sym_node_t {
    const allocator = sym_tab.allocator;
    const node = allocator.create(pars.sym_node_t) catch ut_error();
    node.* = .{};
    node.common.type = que.QUE_NODE_SYMBOL;
    node.resolved = compat.TRUE;
    node.token_type = .SYM_LIT;
    node.indirection = null;
    data.dtype_set(&node.common.val.type, data.DATA_VARCHAR, data.DATA_ENGLISH, 0);
    const use_len = if (len > str.len) str.len else @as(usize, @intCast(len));
    if (use_len > 0) {
        const buf = allocator.alloc(compat.byte, use_len) catch ut_error();
        std.mem.copyForwards(u8, buf, str[0..use_len]);
        data.dfield_set_data(&node.common.val, buf.ptr, use_len);
    } else {
        data.dfield_set_data(&node.common.val, null, 0);
    }
    node.common.val_buf_size = 0;
    node.prefetch_buf = null;
    node.cursor_def = null;
    node.sym_table = sym_tab;
    return node;
}

pub fn sym_tab_add_bound_lit(sym_tab: *pars.sym_tab_t, name: []const u8, lit_type: *compat.ulint) *pars.sym_node_t {
    _ = sym_tab;
    _ = name;
    _ = lit_type;
    ut_error();
}

pub fn sym_tab_add_null_lit(sym_tab: *pars.sym_tab_t) *pars.sym_node_t {
    const allocator = sym_tab.allocator;
    const node = allocator.create(pars.sym_node_t) catch ut_error();
    node.* = .{};
    node.common.type = que.QUE_NODE_SYMBOL;
    node.resolved = compat.TRUE;
    node.token_type = .SYM_LIT;
    node.indirection = null;
    data.dtype_set(&node.common.val.type, data.DATA_ERROR, 0, 0);
    data.dfield_set_null(&node.common.val);
    node.common.val_buf_size = 0;
    node.prefetch_buf = null;
    node.cursor_def = null;
    node.sym_table = sym_tab;
    return node;
}

pub fn sym_tab_add_id(sym_tab: *pars.sym_tab_t, name: []const u8, len: compat.ulint) *pars.sym_node_t {
    const allocator = sym_tab.allocator;
    const node = allocator.create(pars.sym_node_t) catch ut_error();
    node.* = .{};
    node.common.type = que.QUE_NODE_SYMBOL;
    node.resolved = compat.FALSE;
    node.indirection = null;
    const use_len = if (len > name.len) name.len else @as(usize, @intCast(len));
    const buf = allocator.alloc(u8, use_len) catch ut_error();
    std.mem.copyForwards(u8, buf, name[0..use_len]);
    node.name = buf;
    node.name_len = @as(compat.ulint, @intCast(use_len));
    data.dfield_set_null(&node.common.val);
    node.common.val_buf_size = 0;
    node.prefetch_buf = null;
    node.cursor_def = null;
    node.sym_table = sym_tab;
    return node;
}

pub fn sym_tab_add_bound_id(sym_tab: *pars.sym_tab_t, name: []const u8) *pars.sym_node_t {
    _ = sym_tab;
    _ = name;
    ut_error();
}

test "sym add int literal" {
    const allocator = std.testing.allocator;
    const sym_tab = sym_tab_create(allocator);
    defer allocator.destroy(sym_tab);
    const node = sym_tab_add_int_lit(sym_tab, 42);
    defer {
        const ptr = data.dfield_get_data(&node.common.val).?;
        const len = data.dfield_get_len(&node.common.val);
        allocator.free(@as([*]u8, @ptrCast(ptr))[0..len]);
        allocator.destroy(node);
    }
    try std.testing.expectEqual(compat.TRUE, node.resolved);
    try std.testing.expect(node.token_type == .SYM_LIT);
    const ptr = data.dfield_get_data(&node.common.val).?;
    const stored = mach.mach_read_from_4(@as([*]const compat.byte, @ptrCast(ptr)));
    try std.testing.expectEqual(@as(compat.ulint, 42), stored);
}

test "sym add string literal" {
    const allocator = std.testing.allocator;
    const sym_tab = sym_tab_create(allocator);
    defer allocator.destroy(sym_tab);
    const node = sym_tab_add_str_lit(sym_tab, "hi", 2);
    defer {
        const ptr = data.dfield_get_data(&node.common.val);
        const len = data.dfield_get_len(&node.common.val);
        if (ptr) |p| {
            if (len > 0) {
                allocator.free(@as([*]u8, @ptrCast(p))[0..len]);
            }
        }
        allocator.destroy(node);
    }
    try std.testing.expectEqual(compat.TRUE, node.resolved);
    try std.testing.expectEqual(@as(compat.ulint, 2), data.dfield_get_len(&node.common.val));
}

test "sym add null literal" {
    const allocator = std.testing.allocator;
    const sym_tab = sym_tab_create(allocator);
    defer allocator.destroy(sym_tab);
    const node = sym_tab_add_null_lit(sym_tab);
    defer allocator.destroy(node);
    try std.testing.expectEqual(compat.TRUE, data.dfield_is_null(&node.common.val));
}

test "sym add id" {
    const allocator = std.testing.allocator;
    const sym_tab = sym_tab_create(allocator);
    defer allocator.destroy(sym_tab);
    const node = sym_tab_add_id(sym_tab, "col", 3);
    defer {
        allocator.free(@constCast(node.name));
        allocator.destroy(node);
    }
    try std.testing.expectEqual(compat.FALSE, node.resolved);
    try std.testing.expectEqualStrings("col", node.name);
    try std.testing.expectEqual(@as(compat.ulint, 3), node.name_len);
}
