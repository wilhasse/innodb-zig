const std = @import("std");
const compat = @import("../ut/compat.zig");
const errors = @import("../ut/errors.zig");
const mem = @import("../mem/mod.zig");
const que = @import("../que/mod.zig");
const types = @import("types.zig");

pub const module_name = "trx.roll";

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;
pub const dulint = types.dulint;
pub const undo_no_t = types.undo_no_t;
pub const roll_ptr_t = types.roll_ptr_t;
pub const trx_t = types.trx_t;
pub const trx_savept_t = types.trx_savept_t;
pub const trx_named_savept_t = types.trx_named_savept_t;
pub const trx_sig_t = types.trx_sig_t;
pub const trx_undo_inf_t = types.trx_undo_inf_t;
pub const trx_undo_arr_t = types.trx_undo_arr_t;
pub const trx_undo_record_t = types.trx_undo_record_t;

pub const TRX_ROLL_TRUNC_THRESHOLD: ulint = 1;

pub const roll_node_state = enum(u8) {
    send = 1,
    wait = 2,
};

pub const roll_node_t = struct {
    common: que.que_common_t = .{},
    state: roll_node_state = .send,
    partial: bool = false,
    savept: trx_savept_t = .{},
};

var trx_roll_crash_recv_trx: ?*trx_t = null;
var trx_roll_max_undo_no: i64 = 0;
var trx_roll_progress_printed_pct: ulint = 0;

fn dulintCmp(a: dulint, b: dulint) i32 {
    if (a.high < b.high) return -1;
    if (a.high > b.high) return 1;
    if (a.low < b.low) return -1;
    if (a.low > b.low) return 1;
    return 0;
}

fn dulintAdd(a: dulint, inc: ulint) dulint {
    const low = a.low + inc;
    const high = if (low < a.low) a.high + 1 else a.high;
    return .{ .high = high, .low = low };
}

fn dulintIsZero(a: dulint) bool {
    return a.high == 0 and a.low == 0;
}

pub fn trx_is_recv(trx: *const trx_t) bool {
    return trx == trx_roll_crash_recv_trx;
}

pub fn trx_savept_take(trx: *trx_t) trx_savept_t {
    return .{ .least_undo_no = trx.undo_no };
}

pub fn trx_roll_savepoints_free(trx: *trx_t, savep: ?*trx_named_savept_t) void {
    if (trx.savepoints.items.len == 0) {
        return;
    }

    if (savep == null) {
        trx.savepoints.clearRetainingCapacity();
        return;
    }

    var idx: ?usize = null;
    for (trx.savepoints.items, 0..) |*item, i| {
        if (item == savep.?) {
            idx = i;
            break;
        }
    }
    if (idx) |pos| {
        trx.savepoints.items.len = @min(trx.savepoints.items.len, pos + 1);
    }
}

pub fn trx_undo_arr_get_nth_info(arr: *trx_undo_arr_t, n: ulint) *trx_undo_inf_t {
    return &arr.infos[@as(usize, @intCast(n))];
}

pub fn trx_undo_arr_create(allocator: std.mem.Allocator) *trx_undo_arr_t {
    const arr = allocator.create(trx_undo_arr_t) catch @panic("trx_undo_arr_create");
    const infos = allocator.alloc(trx_undo_inf_t, compat.UNIV_MAX_PARALLELISM) catch @panic("trx_undo_arr_create");
    for (infos) |*info| {
        info.* = .{};
    }
    arr.* = .{
        .infos = infos,
        .n_cells = compat.UNIV_MAX_PARALLELISM,
        .n_used = 0,
        .allocator = allocator,
    };
    return arr;
}

pub fn trx_undo_arr_free(arr: *trx_undo_arr_t) void {
    std.debug.assert(arr.n_used == 0);
    arr.allocator.free(arr.infos);
    arr.allocator.destroy(arr);
}

fn trx_undo_arr_store_info(trx: *trx_t, undo_no: undo_no_t) bool {
    const arr = trx.undo_no_arr orelse return false;
    const n_used = arr.n_used;
    var stored_here: ?*trx_undo_inf_t = null;
    var n: ulint = 0;
    var i: ulint = 0;
    while (i < arr.infos.len) : (i += 1) {
        const cell = trx_undo_arr_get_nth_info(arr, i);
        if (!cell.in_use) {
            if (stored_here == null) {
                cell.undo_no = undo_no;
                cell.in_use = true;
                arr.n_used += 1;
                stored_here = cell;
            }
        } else {
            n += 1;
            if (dulintCmp(cell.undo_no, undo_no) == 0) {
                if (stored_here) |slot| {
                    slot.in_use = false;
                    std.debug.assert(arr.n_used > 0);
                    arr.n_used -= 1;
                }
                std.debug.assert(arr.n_used == n_used);
                return false;
            }
        }

        if (n == n_used and stored_here != null) {
            std.debug.assert(arr.n_used == 1 + n_used);
            return true;
        }
    }
    return stored_here != null;
}

fn trx_undo_arr_remove_info(arr: *trx_undo_arr_t, undo_no: undo_no_t) void {
    for (arr.infos) |*cell| {
        if (cell.in_use and dulintCmp(cell.undo_no, undo_no) == 0) {
            cell.in_use = false;
            std.debug.assert(arr.n_used > 0);
            arr.n_used -= 1;
            return;
        }
    }
}

fn trx_undo_arr_get_biggest(arr: *trx_undo_arr_t) undo_no_t {
    if (arr.n_used == 0) {
        return types.dulintZero();
    }

    var biggest = types.dulintZero();
    var n: ulint = 0;
    for (arr.infos) |cell| {
        if (cell.in_use) {
            n += 1;
            if (dulintCmp(cell.undo_no, biggest) > 0) {
                biggest = cell.undo_no;
            }
            if (n == arr.n_used) {
                break;
            }
        }
    }
    return biggest;
}

pub fn trx_roll_try_truncate(trx: *trx_t) void {
    trx.pages_undone = 0;
    const arr = trx.undo_no_arr orelse return;
    var limit = trx.undo_no;
    if (arr.n_used > 0) {
        const biggest = trx_undo_arr_get_biggest(arr);
        if (dulintCmp(biggest, limit) >= 0) {
            limit = dulintAdd(biggest, 1);
        }
    }
    if (trx.undo_stack.items.len == 0) {
        return;
    }
    var write: usize = 0;
    for (trx.undo_stack.items) |rec| {
        if (dulintCmp(rec.undo_no, limit) < 0) {
            if (rec.data.len > 0) {
                trx.allocator.free(rec.data);
            }
            continue;
        }
        trx.undo_stack.items[write] = rec;
        write += 1;
    }
    trx.undo_stack.items.len = write;
}

pub fn trx_undo_rec_reserve(trx: *trx_t, undo_no: undo_no_t) bool {
    return trx_undo_arr_store_info(trx, undo_no);
}

pub fn trx_undo_rec_release(trx: *trx_t, undo_no: undo_no_t) void {
    const arr = trx.undo_no_arr orelse return;
    trx_undo_arr_remove_info(arr, undo_no);
}

pub fn trx_undo_record_push(trx: *trx_t, record: trx_undo_record_t) void {
    const allocator = trx.allocator;
    const data = if (record.data.len > 0)
        allocator.alloc(u8, record.data.len) catch @panic("trx_undo_record_push")
    else
        &[_]u8{};
    if (record.data.len > 0) {
        std.mem.copyForwards(u8, data, record.data);
    }
    var copy = record;
    copy.data = data;
    trx.undo_stack.append(allocator, copy) catch @panic("trx_undo_record_push");
}

pub fn trx_undo_stack_clear(trx: *trx_t) void {
    for (trx.undo_stack.items) |rec| {
        if (rec.data.len > 0) {
            trx.allocator.free(rec.data);
        }
    }
    trx.undo_stack.deinit(trx.allocator);
}

pub fn trx_roll_pop_top_rec_of_trx(
    trx: *trx_t,
    limit: undo_no_t,
    roll_ptr: *roll_ptr_t,
    allocator: std.mem.Allocator,
) ?trx_undo_record_t {
    while (trx.undo_stack.items.len > 0) {
        const idx = trx.undo_stack.items.len - 1;
        const rec = trx.undo_stack.items[idx];

        if (dulintCmp(limit, rec.undo_no) > 0) {
            return null;
        }

        trx.undo_stack.items.len -= 1;
        if (!trx_undo_arr_store_info(trx, rec.undo_no)) {
            if (rec.data.len > 0) {
                trx.allocator.free(rec.data);
            }
            continue;
        }
        trx.undo_no = rec.undo_no;
        roll_ptr.* = rec.roll_ptr;

        const data_copy = if (rec.data.len > 0)
            allocator.alloc(u8, rec.data.len) catch @panic("trx_roll_pop_top_rec_of_trx")
        else
            &[_]u8{};
        if (rec.data.len > 0) {
            std.mem.copyForwards(u8, data_copy, rec.data);
            trx.allocator.free(rec.data);
        }

        return .{
            .undo_no = rec.undo_no,
            .roll_ptr = rec.roll_ptr,
            .is_insert = rec.is_insert,
            .data = data_copy,
        };
    }
    return null;
}

pub fn roll_node_create(heap: *mem.mem_heap_t) *roll_node_t {
    const buf = mem.mem_heap_alloc(heap, @sizeOf(roll_node_t)) orelse @panic("roll_node_create");
    const node = @as(*roll_node_t, @ptrCast(@alignCast(buf.ptr)));
    node.* = .{};
    node.common.type = que.QUE_NODE_ROLLBACK;
    node.state = .send;
    node.partial = false;
    return node;
}

pub fn trx_sig_send(
    trx: *trx_t,
    type_: ulint,
    sender: ulint,
    receiver_thr: ?*que.que_thr_t,
    savept: ?*trx_savept_t,
    next_thr: ?*?*que.que_thr_t,
) void {
    _ = receiver_thr;
    _ = next_thr;
    const sig = trx_sig_t{
        .type = type_,
        .sender = sender,
        .savept = if (savept) |sp| sp.* else null,
    };
    trx.signals.append(trx.allocator, sig) catch @panic("trx_sig_send");
}

pub fn trx_sig_reply(sig: *trx_sig_t, next_thr: ?*?*que.que_thr_t) void {
    _ = next_thr;
    sig.replied = true;
}

pub fn trx_sig_remove(trx: *trx_t, sig: *trx_sig_t) void {
    for (trx.signals.items, 0..) |*item, idx| {
        if (item == sig) {
            _ = trx.signals.orderedRemove(trx.allocator, idx);
            return;
        }
    }
}

fn thr_get_trx(thr: *que.que_thr_t) ?*trx_t {
    const fork = thr.parent orelse return null;
    if (fork.trx == null) {
        return null;
    }
    return @as(*trx_t, @ptrCast(@alignCast(fork.trx.?)));
}

pub fn trx_rollback_step(thr: *que.que_thr_t) ?*que.que_thr_t {
    const node_ptr = thr.run_node orelse return null;
    const node = @as(*roll_node_t, @fieldParentPtr("common", node_ptr));

    if (thr.prev_node == que.que_node_get_parent(node_ptr)) {
        node.state = .send;
    }

    if (node.state == .send) {
        node.state = .wait;

        const sig_no: ulint = if (node.partial)
            types.TRX_SIG_ROLLBACK_TO_SAVEPT
        else
            types.TRX_SIG_TOTAL_ROLLBACK;

        const savept_ptr: ?*trx_savept_t = if (node.partial) &node.savept else null;

        if (thr_get_trx(thr)) |trx| {
            trx_sig_send(trx, sig_no, types.TRX_SIG_SELF, thr, savept_ptr, null);
        }

        thr.state = que.QUE_THR_SIG_REPLY_WAIT;
        return null;
    }

    thr.run_node = que.que_node_get_parent(node_ptr);
    return thr;
}

pub fn trx_roll_graph_build(trx: *trx_t) *que.que_t {
    const heap = mem.mem_heap_create_func(512, mem.MEM_HEAP_DYNAMIC) orelse @panic("trx_roll_graph_build");
    const fork = que.que_fork_create(null, null, que.QUE_FORK_ROLLBACK, heap.allocator);
    fork.trx = trx;
    _ = que.que_thr_create(fork, heap.allocator);
    return fork;
}

pub fn trx_rollback(trx: *trx_t, sig: *trx_sig_t, next_thr: ?*?*que.que_thr_t) void {
    if (sig.type == types.TRX_SIG_TOTAL_ROLLBACK) {
        trx.roll_limit = types.dulintZero();
    } else if (sig.type == types.TRX_SIG_ROLLBACK_TO_SAVEPT) {
        if (sig.savept) |sp| {
            trx.roll_limit = sp.least_undo_no;
        } else {
            trx.roll_limit = types.dulintZero();
        }
    } else if (sig.type == types.TRX_SIG_ERROR_OCCURRED) {
        trx.roll_limit = trx.last_sql_stat_start.least_undo_no;
    } else {
        trx.roll_limit = types.dulintZero();
    }

    trx.pages_undone = 0;

    if (trx.undo_no_arr == null) {
        trx.undo_no_arr = trx_undo_arr_create(trx.allocator);
    }

    const graph = trx_roll_graph_build(trx);
    trx.graph = graph;
    trx.que_state = .rolling_back;

    const thr = que.que_fork_start_command(graph);
    if (next_thr) |out| {
        if (out.* == null) {
            out.* = thr;
        }
    }
}

pub fn trx_general_rollback(trx: *trx_t, partial: bool, savept: ?*trx_savept_t) errors.DbErr {
    trx.error_state = .DB_SUCCESS;
    if (partial) {
        if (savept) |sp| {
            trx.roll_limit = sp.least_undo_no;
        }
    } else {
        trx.roll_limit = types.dulintZero();
    }
    return .DB_SUCCESS;
}

pub fn trx_finish_rollback_off_kernel(trx: *trx_t) void {
    trx.que_state = .running;
    var idx: usize = 0;
    while (idx < trx.signals.items.len) {
        if (trx.signals.items[idx].type == types.TRX_SIG_TOTAL_ROLLBACK) {
            _ = trx.signals.orderedRemove(trx.allocator, idx);
        } else {
            idx += 1;
        }
    }
}

pub fn trx_finish_partial_rollback_off_kernel(trx: *trx_t, sig: *trx_sig_t) void {
    trx_sig_reply(sig, null);
    trx_sig_remove(trx, sig);
    trx.que_state = .running;
}

pub fn trx_finish_error_processing(trx: *trx_t) void {
    var idx: usize = 0;
    while (idx < trx.signals.items.len) {
        if (trx.signals.items[idx].type == types.TRX_SIG_ERROR_OCCURRED) {
            _ = trx.signals.orderedRemove(trx.allocator, idx);
        } else {
            idx += 1;
        }
    }
    trx.que_state = .running;
}

pub fn trx_deinit(trx: *trx_t) void {
    trx_undo_stack_clear(trx);
    trx.signals.deinit(trx.allocator);
    trx.savepoints.deinit(trx.allocator);
    if (trx.undo_no_arr) |arr| {
        trx_undo_arr_free(arr);
        trx.undo_no_arr = null;
    }
}

test "trx savepoint take and free" {
    var trx = trx_t{ .allocator = std.testing.allocator };
    defer trx_deinit(&trx);
    trx.undo_no = .{ .high = 0, .low = 7 };
    const savept = trx_savept_take(&trx);
    try std.testing.expectEqual(@as(ulint, 7), savept.least_undo_no.low);

    try trx.savepoints.append(std.testing.allocator, .{ .name = "a", .savept = savept });
    try trx.savepoints.append(std.testing.allocator, .{ .name = "b", .savept = savept });
    try std.testing.expectEqual(@as(usize, 2), trx.savepoints.items.len);
    const first = &trx.savepoints.items[0];
    trx_roll_savepoints_free(&trx, first);
    try std.testing.expectEqual(@as(usize, 1), trx.savepoints.items.len);
}

test "trx undo array store and biggest" {
    var trx = trx_t{ .allocator = std.testing.allocator };
    defer trx_deinit(&trx);
    trx.undo_no_arr = trx_undo_arr_create(std.testing.allocator);
    const arr = trx.undo_no_arr.?;

    const a: undo_no_t = .{ .high = 0, .low = 5 };
    const b: undo_no_t = .{ .high = 0, .low = 9 };
    try std.testing.expect(trx_undo_rec_reserve(&trx, a));
    try std.testing.expect(trx_undo_rec_reserve(&trx, b));
    try std.testing.expect(!trx_undo_rec_reserve(&trx, a));

    const biggest = trx_undo_arr_get_biggest(arr);
    try std.testing.expectEqual(@as(ulint, 9), biggest.low);

    trx_undo_rec_release(&trx, b);
    try std.testing.expectEqual(@as(ulint, 1), arr.n_used);
}

test "trx rollback sets roll limit" {
    var trx = trx_t{ .allocator = std.testing.allocator };
    defer trx_deinit(&trx);
    trx.undo_no = .{ .high = 0, .low = 10 };
    var sig = trx_sig_t{ .type = types.TRX_SIG_TOTAL_ROLLBACK };
    trx_rollback(&trx, &sig, null);
    try std.testing.expectEqual(@as(ulint, 0), trx.roll_limit.low);

    var sig2 = trx_sig_t{
        .type = types.TRX_SIG_ROLLBACK_TO_SAVEPT,
        .savept = .{ .least_undo_no = .{ .high = 0, .low = 4 } },
    };
    trx_rollback(&trx, &sig2, null);
    try std.testing.expectEqual(@as(ulint, 4), trx.roll_limit.low);
}

test "trx rollback step sends signal" {
    var trx = trx_t{ .allocator = std.testing.allocator };
    defer trx_deinit(&trx);
    var fork = que.que_fork_t{};
    fork.trx = &trx;
    var thr = que.que_thr_t{ .parent = &fork };

    var node = roll_node_t{};
    node.common.type = que.QUE_NODE_ROLLBACK;
    node.partial = true;
    node.savept.least_undo_no = .{ .high = 0, .low = 3 };
    thr.run_node = &node.common;

    try std.testing.expect(trx_rollback_step(&thr) == null);
    try std.testing.expectEqual(@as(usize, 1), trx.signals.items.len);
    try std.testing.expectEqual(types.TRX_SIG_ROLLBACK_TO_SAVEPT, trx.signals.items[0].type);
    try std.testing.expectEqual(que.QUE_THR_SIG_REPLY_WAIT, thr.state);
}

test "trx roll pop top record honors limit" {
    var trx = trx_t{ .allocator = std.testing.allocator };
    defer trx_deinit(&trx);
    trx.undo_no_arr = trx_undo_arr_create(std.testing.allocator);
    trx_undo_record_push(&trx, .{ .undo_no = .{ .high = 0, .low = 1 }, .data = "a" });
    trx_undo_record_push(&trx, .{ .undo_no = .{ .high = 0, .low = 2 }, .data = "b" });

    var roll_ptr: roll_ptr_t = types.dulintZero();
    const rec = trx_roll_pop_top_rec_of_trx(&trx, types.dulintZero(), &roll_ptr, std.testing.allocator) orelse return error.TestExpectedEqual;
    defer if (rec.data.len > 0) std.testing.allocator.free(rec.data);
    try std.testing.expectEqual(@as(ulint, 2), rec.undo_no.low);
    try std.testing.expectEqualStrings("b", rec.data);
}

test "trx roll try truncate drops old undo records" {
    var trx = trx_t{ .allocator = std.testing.allocator };
    defer trx_deinit(&trx);
    trx.undo_no_arr = trx_undo_arr_create(std.testing.allocator);
    trx.undo_no = .{ .high = 0, .low = 2 };

    trx_undo_record_push(&trx, .{ .undo_no = .{ .high = 0, .low = 1 }, .data = "a" });
    trx_undo_record_push(&trx, .{ .undo_no = .{ .high = 0, .low = 2 }, .data = "b" });
    trx_undo_record_push(&trx, .{ .undo_no = .{ .high = 0, .low = 3 }, .data = "c" });

    trx_roll_try_truncate(&trx);
    try std.testing.expectEqual(@as(usize, 2), trx.undo_stack.items.len);
    try std.testing.expectEqual(@as(ulint, 2), trx.undo_stack.items[0].undo_no.low);
    try std.testing.expectEqual(@as(ulint, 3), trx.undo_stack.items[1].undo_no.low);
}
