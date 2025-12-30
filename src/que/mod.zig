const compat = @import("../ut/compat.zig");
const data = @import("../data/mod.zig");
const row = @import("../row/mod.zig");

pub const module_name = "que";

pub const ulint = compat.ulint;

pub const QUE_NODE_SYMBOL: ulint = 16;
pub const QUE_NODE_FUNC: ulint = 18;
pub const QUE_NODE_ORDER: ulint = 19;
pub const QUE_NODE_FORK: ulint = 8;
pub const QUE_NODE_ASSIGNMENT: ulint = 23;
pub const QUE_NODE_RETURN: ulint = 28;
pub const QUE_NODE_EXIT: ulint = 32;
pub const QUE_NODE_ELSIF: ulint = 30;
pub const QUE_NODE_CONTROL_STAT: ulint = 1024;
pub const QUE_NODE_PROC: ulint = 20 + QUE_NODE_CONTROL_STAT;
pub const QUE_NODE_IF: ulint = 21 + QUE_NODE_CONTROL_STAT;
pub const QUE_NODE_WHILE: ulint = 22 + QUE_NODE_CONTROL_STAT;
pub const QUE_NODE_FOR: ulint = 27 + QUE_NODE_CONTROL_STAT;

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

pub const que_thr_t = struct {
    run_node: ?*que_node_t = null,
    prev_node: ?*que_node_t = null,
};

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

pub fn que_node_get_parent(node: *que_node_t) ?*que_node_t {
    return node.parent;
}

pub fn que_node_get_containing_loop_node(node: *que_node_t) ?*que_node_t {
    var cur = node.parent;
    while (cur) |ptr| {
        const t = ptr.type;
        if (t == QUE_NODE_WHILE or t == QUE_NODE_FOR) {
            return ptr;
        }
        cur = ptr.parent;
    }
    return null;
}
