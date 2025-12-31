const std = @import("std");
const compat = @import("../ut/compat.zig");
const types = @import("types.zig");
const undo_mod = @import("undo.zig");

pub const module_name = "trx.rseg";

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;
pub const trx_id_t = types.trx_id_t;

pub const FIL_NULL: ulint = compat.ULINT32_UNDEFINED;

pub const TRX_SYS_N_RSEGS: ulint = 256;
pub const TRX_RSEG_N_SLOTS: ulint = compat.UNIV_PAGE_SIZE / 16;
pub const TRX_RSEG_MAX_N_TRXS: ulint = TRX_RSEG_N_SLOTS / 2;

pub const trx_rseg_t = struct {
    id: ulint = 0,
    space: ulint = 0,
    zip_size: ulint = 0,
    page_no: ulint = 0,
    max_size: ulint = 0,
    curr_size: ulint = 0,
    update_undo_list: std.ArrayListUnmanaged(*undo_mod.trx_undo_t) = .{},
    update_undo_cached: std.ArrayListUnmanaged(*undo_mod.trx_undo_t) = .{},
    insert_undo_list: std.ArrayListUnmanaged(*undo_mod.trx_undo_t) = .{},
    insert_undo_cached: std.ArrayListUnmanaged(*undo_mod.trx_undo_t) = .{},
    last_page_no: ulint = FIL_NULL,
    last_offset: ulint = 0,
    last_trx_no: trx_id_t = 0,
    last_del_marks: ibool = compat.FALSE,
};

pub const trx_sys_t = struct {
    rseg_list: std.ArrayListUnmanaged(*trx_rseg_t) = .{},
    rsegs: []?*trx_rseg_t = &[_]?*trx_rseg_t{},
    rseg_history_len: ulint = 0,
    next_rseg_page_no: ulint = 1,
    allocator: std.mem.Allocator = std.heap.page_allocator,
};

pub var trx_sys: trx_sys_t = .{};

pub fn trx_sys_init(allocator: std.mem.Allocator) void {
    if (trx_sys.rsegs.len == 0) {
        trx_sys.rsegs = allocator.alloc(?*trx_rseg_t, TRX_SYS_N_RSEGS) catch @panic("trx_sys_init");
        for (trx_sys.rsegs) |*slot| {
            slot.* = null;
        }
        trx_sys.allocator = allocator;
    }
    trx_sys.rseg_list.clearRetainingCapacity();
    trx_sys.rseg_history_len = 0;
}

pub fn trx_sys_deinit() void {
    if (trx_sys.rsegs.len == 0) {
        return;
    }
    while (trx_sys.rseg_list.items.len > 0) {
        const last = trx_sys.rseg_list.items[trx_sys.rseg_list.items.len - 1];
        trx_rseg_mem_free(last);
    }
    trx_sys.allocator.free(trx_sys.rsegs);
    trx_sys.rsegs = &[_]?*trx_rseg_t{};
    trx_sys.rseg_list.deinit(trx_sys.allocator);
    trx_sys.rseg_list = .{};
}

pub fn trx_sys_set_nth_rseg(id: ulint, rseg: ?*trx_rseg_t) void {
    if (id >= trx_sys.rsegs.len) {
        return;
    }
    trx_sys.rsegs[@as(usize, @intCast(id))] = rseg;
}


pub fn trx_rseg_get_on_id(id: ulint) ?*trx_rseg_t {
    for (trx_sys.rseg_list.items) |rseg| {
        if (rseg.id == id) {
            return rseg;
        }
    }
    return null;
}

pub fn trx_rseg_header_create(
    space: ulint,
    zip_size: ulint,
    max_size: ulint,
    slot_no: *ulint,
) ulint {
    if (trx_sys.rsegs.len == 0) {
        trx_sys_init(std.heap.page_allocator);
    }

    var idx: ?ulint = null;
    for (trx_sys.rsegs, 0..) |slot, i| {
        if (slot == null) {
            idx = @as(ulint, @intCast(i));
            break;
        }
    }
    if (idx == null) {
        slot_no.* = compat.ULINT_UNDEFINED;
        return FIL_NULL;
    }

    const page_no = trx_sys.next_rseg_page_no;
    trx_sys.next_rseg_page_no += 1;
    slot_no.* = idx.?;

    const rseg = trx_rseg_mem_create(idx.?, space, zip_size, page_no);
    rseg.max_size = max_size;
    return page_no;
}

pub fn trx_rseg_mem_free(rseg: *trx_rseg_t) void {
    for (trx_sys.rseg_list.items, 0..) |item, idx| {
        if (item == rseg) {
            _ = trx_sys.rseg_list.orderedRemove(idx);
            break;
        }
    }

    std.debug.assert(rseg.update_undo_list.items.len == 0);
    std.debug.assert(rseg.insert_undo_list.items.len == 0);

    while (rseg.update_undo_cached.items.len > 0) {
        const undo = rseg.update_undo_cached.items[rseg.update_undo_cached.items.len - 1];
        rseg.update_undo_cached.items.len -= 1;
        undo_mod.trx_undo_mem_free(undo);
    }
    rseg.update_undo_cached.deinit(trx_sys.allocator);

    while (rseg.insert_undo_cached.items.len > 0) {
        const undo = rseg.insert_undo_cached.items[rseg.insert_undo_cached.items.len - 1];
        rseg.insert_undo_cached.items.len -= 1;
        undo_mod.trx_undo_mem_free(undo);
    }
    rseg.insert_undo_cached.deinit(trx_sys.allocator);

    rseg.update_undo_list.deinit(trx_sys.allocator);
    rseg.insert_undo_list.deinit(trx_sys.allocator);

    trx_sys_set_nth_rseg(rseg.id, null);
    trx_sys.allocator.destroy(rseg);
}

fn trx_rseg_mem_create(id: ulint, space: ulint, zip_size: ulint, page_no: ulint) *trx_rseg_t {
    const rseg = trx_sys.allocator.create(trx_rseg_t) catch @panic("trx_rseg_mem_create");
    rseg.* = .{
        .id = id,
        .space = space,
        .zip_size = zip_size,
        .page_no = page_no,
        .curr_size = 1,
    };
    trx_sys.rseg_list.append(trx_sys.allocator, rseg) catch @panic("trx_rseg_mem_create");
    trx_sys_set_nth_rseg(id, rseg);
    return rseg;
}

pub fn trx_rseg_list_and_array_init() void {
    if (trx_sys.rsegs.len == 0) {
        trx_sys_init(std.heap.page_allocator);
    }
    trx_sys.rseg_list.clearRetainingCapacity();
    trx_sys.rseg_history_len = 0;
    for (trx_sys.rsegs) |slot| {
        if (slot) |rseg| {
            trx_sys.rseg_list.append(trx_sys.allocator, rseg) catch @panic("trx_rseg_list_and_array_init");
        }
    }
}

test "trx rseg create and lookup" {
    trx_sys_init(std.testing.allocator);
    defer trx_sys_deinit();

    var slot: ulint = 0;
    const page_no = trx_rseg_header_create(1, 0, 128, &slot);
    try std.testing.expect(page_no != FIL_NULL);
    try std.testing.expectEqual(@as(ulint, 0), slot);

    const rseg = trx_rseg_get_on_id(slot) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(ulint, 1), rseg.space);
    try std.testing.expectEqual(page_no, rseg.page_no);

    trx_rseg_mem_free(rseg);
}

test "trx rseg list init reuses array" {
    trx_sys_init(std.testing.allocator);
    defer trx_sys_deinit();

    var slot: ulint = 0;
    _ = trx_rseg_header_create(2, 0, 64, &slot);
    trx_rseg_list_and_array_init();
    try std.testing.expect(trx_sys.rseg_list.items.len >= 1);
}
