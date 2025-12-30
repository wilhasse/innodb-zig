const compat = @import("../ut/compat.zig");

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

pub const sel_node_t = struct {
    state: sel_node_state = .SEL_NODE_CLOSED,
};
