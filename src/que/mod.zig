const std = @import("std");
const compat = @import("../ut/compat.zig");
const data = @import("../data/mod.zig");
const row = @import("../row/mod.zig");

pub const module_name = "que";

pub const ulint = compat.ulint;

pub const QUE_NODE_SYMBOL: ulint = 16;
pub const QUE_NODE_FUNC: ulint = 18;
pub const QUE_NODE_ORDER: ulint = 19;
pub const QUE_NODE_FORK: ulint = 8;
pub const QUE_NODE_ROLLBACK: ulint = 12;
pub const QUE_NODE_ASSIGNMENT: ulint = 23;
pub const QUE_NODE_RETURN: ulint = 28;
pub const QUE_NODE_EXIT: ulint = 32;
pub const QUE_NODE_ELSIF: ulint = 30;
pub const QUE_NODE_CONTROL_STAT: ulint = 1024;
pub const QUE_NODE_PROC: ulint = 20 + QUE_NODE_CONTROL_STAT;
pub const QUE_NODE_IF: ulint = 21 + QUE_NODE_CONTROL_STAT;
pub const QUE_NODE_WHILE: ulint = 22 + QUE_NODE_CONTROL_STAT;
pub const QUE_NODE_FOR: ulint = 27 + QUE_NODE_CONTROL_STAT;

pub const QUE_FORK_ROLLBACK: ulint = 5;
pub const QUE_FORK_RECOVERY: ulint = 11;

pub const QUE_THR_RUNNING: ulint = 1;
pub const QUE_THR_SIG_REPLY_WAIT: ulint = 6;

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
    graph: ?*que_t = null,
    fork_type: ulint = 0,
    n_active_thrs: ulint = 0,
    thrs_head: ?*que_thr_t = null,
    last_sel_node: ?*row.sel_node_t = null,
    trx: ?*anyopaque = null,
};

pub const que_t = que_fork_t;

pub const que_thr_t = struct {
    run_node: ?*que_node_t = null,
    prev_node: ?*que_node_t = null,
    child: ?*que_node_t = null,
    parent: ?*que_fork_t = null,
    next: ?*que_thr_t = null,
    state: ulint = QUE_THR_RUNNING,
};

pub const sess_t = struct {
    graphs_head: ?*que_t = null,
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

pub fn que_var_init() void {}

pub fn que_graph_publish(graph: *que_t, sess: *sess_t) void {
    graph.common.brother = sess.graphs_head;
    sess.graphs_head = graph;
}

pub fn que_fork_create(graph: ?*que_t, parent: ?*que_node_t, fork_type: ulint, allocator: std.mem.Allocator) *que_fork_t {
    const fork = allocator.create(que_fork_t) catch @panic("que_fork_create");
    fork.* = .{};
    fork.common.type = QUE_NODE_FORK;
    fork.common.parent = parent;
    fork.fork_type = fork_type;
    fork.n_active_thrs = 0;
    fork.graph = graph orelse fork;
    return fork;
}

pub fn que_fork_get_first_thr(fork: *que_fork_t) ?*que_thr_t {
    return fork.thrs_head;
}

pub fn que_fork_get_child(fork: *que_fork_t) ?*que_node_t {
    return if (fork.thrs_head) |thr| thr.run_node else null;
}

pub fn que_node_set_parent(node: *que_node_t, parent: ?*que_node_t) void {
    node.parent = parent;
}

pub fn que_thr_create(parent: *que_fork_t, allocator: std.mem.Allocator) *que_thr_t {
    const thr = allocator.create(que_thr_t) catch @panic("que_thr_create");
    thr.* = .{};
    thr.parent = parent;
    if (parent.thrs_head == null) {
        parent.thrs_head = thr;
    } else {
        var cur = parent.thrs_head.?;
        while (cur.next) |next| {
            cur = next;
        }
        cur.next = thr;
    }
    return thr;
}

pub fn que_fork_start_command(fork: *que_fork_t) ?*que_thr_t {
    if (fork.thrs_head) |thr| {
        if (thr.run_node == null and thr.child != null) {
            thr.run_node = thr.child;
        }
        return thr;
    }
    return null;
}

pub fn que_run_threads(thr: *que_thr_t) void {
    _ = thr;
}

pub fn que_graph_free_recursive(node: *que_node_t) void {
    _ = node;
}

pub fn que_graph_free(graph: *que_t) void {
    _ = graph;
}

test "que fork create defaults graph" {
    const allocator = std.testing.allocator;
    const fork = que_fork_create(null, null, 7, allocator);
    defer allocator.destroy(fork);
    try std.testing.expect(fork.graph == fork);
    try std.testing.expectEqual(@as(ulint, 7), fork.fork_type);
}

test "que thr create links into fork" {
    const allocator = std.testing.allocator;
    const fork = que_fork_create(null, null, 0, allocator);
    defer allocator.destroy(fork);
    const thr1 = que_thr_create(fork, allocator);
    const thr2 = que_thr_create(fork, allocator);
    defer {
        allocator.destroy(thr2);
        allocator.destroy(thr1);
    }
    try std.testing.expect(fork.thrs_head == thr1);
    try std.testing.expect(thr1.next == thr2);
    try std.testing.expect(thr2.next == null);
    try std.testing.expect(thr1.parent == fork);
    try std.testing.expect(thr2.parent == fork);
}

test "que node set parent" {
    var parent_node = que_node_t{};
    var child_node = que_node_t{};
    que_node_set_parent(&child_node, &parent_node);
    try std.testing.expect(child_node.parent == &parent_node);
}
