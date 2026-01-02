const std = @import("std");
const errors = @import("../ut/errors.zig");
const que = @import("../que/mod.zig");
const core = @import("core.zig");
const types = @import("types.zig");

pub const module_name = "trx.xa";

pub const trx_t = types.trx_t;

pub const Xid = struct {
    format_id: u32 = 0,
    gtrid: []const u8 = "",
    bqual: []const u8 = "",
};

const XidEntry = struct {
    trx: *trx_t,
    xid: Xid,
};

var xa_entries: std.ArrayList(XidEntry) = undefined;
var xa_entries_inited: bool = false;
var xa_recover_cache: ?struct {
    allocator: std.mem.Allocator,
    items: []Xid,
} = null;

fn xa_list() *std.ArrayList(XidEntry) {
    if (!xa_entries_inited) {
        xa_entries = std.ArrayList(XidEntry).init(std.heap.page_allocator);
        xa_entries_inited = true;
    }
    return &xa_entries;
}

fn xa_cache_clear() void {
    if (xa_recover_cache) |cache| {
        cache.allocator.free(cache.items);
        xa_recover_cache = null;
    }
}

fn xidCopy(allocator: std.mem.Allocator, xid: Xid) !Xid {
    const gtrid = if (xid.gtrid.len > 0) try allocator.alloc(u8, xid.gtrid.len) else &[_]u8{};
    const bqual = if (xid.bqual.len > 0) try allocator.alloc(u8, xid.bqual.len) else &[_]u8{};
    if (xid.gtrid.len > 0) {
        std.mem.copyForwards(u8, gtrid, xid.gtrid);
    }
    if (xid.bqual.len > 0) {
        std.mem.copyForwards(u8, bqual, xid.bqual);
    }
    return .{ .format_id = xid.format_id, .gtrid = gtrid, .bqual = bqual };
}

fn xidFree(allocator: std.mem.Allocator, xid: Xid) void {
    if (xid.gtrid.len > 0) {
        allocator.free(@constCast(xid.gtrid));
    }
    if (xid.bqual.len > 0) {
        allocator.free(@constCast(xid.bqual));
    }
}

fn xidEqual(a: Xid, b: Xid) bool {
    return a.format_id == b.format_id and std.mem.eql(u8, a.gtrid, b.gtrid) and std.mem.eql(u8, a.bqual, b.bqual);
}

fn xa_find_by_trx(trx: *trx_t) ?usize {
    const list = xa_list();
    for (list.items, 0..) |entry, idx| {
        if (entry.trx == trx) {
            return idx;
        }
    }
    return null;
}

pub fn trx_xa_prepare(trx: *trx_t, xid: ?Xid) errors.DbErr {
    const xid_val = xid orelse return .DB_INVALID_INPUT;
    const allocator = std.heap.page_allocator;
    const list = xa_list();
    const copy = xidCopy(allocator, xid_val) catch return .DB_OUT_OF_MEMORY;

    if (xa_find_by_trx(trx)) |idx| {
        xidFree(allocator, list.items[idx].xid);
        list.items[idx].xid = copy;
    } else {
        list.append(.{ .trx = trx, .xid = copy }) catch {
            xidFree(allocator, copy);
            return .DB_OUT_OF_MEMORY;
        };
    }
    xa_cache_clear();
    trx.conc_state = .prepared;
    return .DB_SUCCESS;
}

pub fn trx_xa_commit(trx: *trx_t, xid: ?Xid, one_phase: bool) errors.DbErr {
    const list = xa_list();
    if (xa_find_by_trx(trx)) |idx| {
        if (xid) |xid_val| {
            if (!xidEqual(list.items[idx].xid, xid_val)) {
                return .DB_ERROR;
            }
        }
        xidFree(std.heap.page_allocator, list.items[idx].xid);
        _ = list.orderedRemove(idx);
        xa_cache_clear();
    } else if (!one_phase) {
        return .DB_NOT_FOUND;
    }
    trx.conc_state = .committed_in_memory;
    return .DB_SUCCESS;
}

pub fn trx_xa_rollback(trx: *trx_t, xid: ?Xid) errors.DbErr {
    const list = xa_list();
    if (xa_find_by_trx(trx)) |idx| {
        if (xid) |xid_val| {
            if (!xidEqual(list.items[idx].xid, xid_val)) {
                return .DB_ERROR;
            }
        }
        xidFree(std.heap.page_allocator, list.items[idx].xid);
        _ = list.orderedRemove(idx);
        xa_cache_clear();
    } else if (xid != null) {
        return .DB_NOT_FOUND;
    }
    trx.conc_state = .committed_in_memory;
    return .DB_SUCCESS;
}

pub fn trx_xa_recover(_: std.mem.Allocator) []const Xid {
    const list = xa_list();
    xa_cache_clear();
    if (list.items.len == 0) {
        return &[_]Xid{};
    }
    const allocator = std.heap.page_allocator;
    const buf = allocator.alloc(Xid, list.items.len) catch return &[_]Xid{};
    for (list.items, 0..) |entry, idx| {
        buf[idx] = entry.xid;
    }
    xa_recover_cache = .{ .allocator = allocator, .items = buf };
    return buf;
}

test "trx xa prepare/commit and recover" {
    core.trx_var_init();
    var sess = que.sess_t{};
    const trx = core.trx_allocate_for_client(&sess, std.testing.allocator);
    defer core.trx_free(trx);

    core.trx_start(trx);
    try std.testing.expect(trx.conc_state == .active);

    const xid = Xid{ .format_id = 1, .gtrid = "g1", .bqual = "b1" };
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, trx_xa_prepare(trx, xid));
    try std.testing.expect(trx.conc_state == .prepared);

    const recovered = trx_xa_recover(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), recovered.len);
    try std.testing.expectEqual(@as(u32, 1), recovered[0].format_id);
    try std.testing.expectEqualStrings("g1", recovered[0].gtrid);

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, trx_xa_commit(trx, xid, false));
    try std.testing.expect(trx.conc_state == .committed_in_memory);
}
