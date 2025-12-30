const compat = @import("../ut/compat.zig");
const data = @import("../data/mod.zig");
const row = @import("../row/mod.zig");

pub const module_name = "que";

pub const ulint = compat.ulint;

pub const QUE_NODE_SYMBOL: ulint = 16;
pub const QUE_NODE_FUNC: ulint = 18;
pub const QUE_NODE_FORK: ulint = 8;

pub const que_common_t = struct {
    type: ulint = 0,
    parent: ?*que_node_t = null,
    brother: ?*que_node_t = null,
    val: data.dfield_t = .{},
    val_buf_size: ulint = 0,
};

pub const que_node_t = que_common_t;

pub const que_fork_t = struct {
    common: que_common_t = .{ .type = QUE_NODE_FORK },
    last_sel_node: ?*row.sel_node_t = null,
};

pub const que_t = que_fork_t;

pub fn que_node_get_type(node: *que_node_t) ulint {
    return node.type;
}

pub fn que_node_get_data_type(node: *que_node_t) *data.dtype_t {
    return &node.val.type;
}

pub fn que_node_get_val(node: *que_node_t) *data.dfield_t {
    return &node.val;
}

pub fn que_node_get_val_buf_size(node: *que_node_t) ulint {
    return node.val_buf_size;
}

pub fn que_node_set_val_buf_size(node: *que_node_t, size: ulint) void {
    node.val_buf_size = size;
}

pub fn que_node_get_next(node: *que_node_t) ?*que_node_t {
    return node.brother;
}
