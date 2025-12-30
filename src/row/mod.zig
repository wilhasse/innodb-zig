const std = @import("std");
const compat = @import("../ut/compat.zig");
const dict = @import("../dict/mod.zig");

pub const module_name = "row";

pub const ulint = compat.ulint;

pub const sel_node_state = enum(u32) {
    SEL_NODE_CLOSED = 0,
    SEL_NODE_OPEN = 1,
    SEL_NODE_FETCH = 2,
    SEL_NODE_NO_MORE_ROWS = 3,
};

pub const plan_t = struct {};
pub const sel_buf_t = struct {};
pub const row_ext_t = struct {
    n_ext: ulint = 0,
    ext: []const ulint = &[_]ulint{},
    buf: []u8 = &[_]u8{},
    len: []ulint = &[_]ulint{},
};

pub const ext = @import("ext.zig");
pub const ins = @import("ins.zig");
pub const merge = @import("merge.zig");
pub const prebuilt = @import("prebuilt.zig");
pub const purge = @import("purge.zig");
pub const format = @import("format.zig");

pub const FETCH_CACHE_SIZE: ulint = 16;
pub const ROW_PREBUILT_ALLOCATED: ulint = 78540783;
pub const ROW_PREBUILT_FREED: ulint = 26423527;

pub const ib_cached_row_t = struct {
    ptr: ?[]u8 = null,
};

pub const ib_row_cache_t = struct {
    n_max: ulint = 0,
    n_size: ulint = 0,
    ptr: []ib_cached_row_t = &[_]ib_cached_row_t{},
};

pub const row_prebuilt_t = struct {
    magic_n: ulint = 0,
    magic_n2: ulint = 0,
    table: ?*dict.dict_table_t = null,
    row_cache: ib_row_cache_t = .{},
    allocator: std.mem.Allocator = std.heap.page_allocator,
};

pub const sel_node_t = struct {
    state: sel_node_state = .SEL_NODE_CLOSED,
};
