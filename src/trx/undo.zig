const std = @import("std");
const compat = @import("../ut/compat.zig");
const types = @import("types.zig");

pub const module_name = "trx.undo";

pub const ulint = compat.ulint;
pub const undo_no_t = types.undo_no_t;
pub const trx_id_t = types.trx_id_t;

pub const TRX_UNDO_INSERT: ulint = 1;
pub const TRX_UNDO_UPDATE: ulint = 2;

pub const trx_undo_t = struct {
    id: ulint = 0,
    type: ulint = TRX_UNDO_INSERT,
    trx_id: trx_id_t = 0,
    top_undo_no: undo_no_t = types.dulintZero(),
    empty: bool = true,
    records: std.ArrayListUnmanaged(types.trx_undo_record_t) = .{},
    allocator: std.mem.Allocator = std.heap.page_allocator,
};

pub fn trx_undo_mem_create(
    id: ulint,
    type_: ulint,
    trx_id: trx_id_t,
    allocator: std.mem.Allocator,
) *trx_undo_t {
    const undo = allocator.create(trx_undo_t) catch @panic("trx_undo_mem_create");
    undo.* = .{
        .id = id,
        .type = type_,
        .trx_id = trx_id,
        .allocator = allocator,
    };
    return undo;
}

pub fn trx_undo_mem_free(undo: *trx_undo_t) void {
    for (undo.records.items) |rec| {
        if (rec.data.len > 0) {
            undo.allocator.free(rec.data);
        }
    }
    undo.records.deinit(undo.allocator);
    undo.allocator.destroy(undo);
}

pub fn trx_undo_append_record(undo: *trx_undo_t, record: types.trx_undo_record_t) void {
    const data_copy = if (record.data.len > 0)
        undo.allocator.alloc(u8, record.data.len) catch @panic("trx_undo_append_record")
    else
        &[_]u8{};
    if (record.data.len > 0) {
        std.mem.copyForwards(u8, data_copy, record.data);
    }
    var copy = record;
    copy.data = data_copy;
    undo.records.append(undo.allocator, copy) catch @panic("trx_undo_append_record");
    undo.top_undo_no = record.undo_no;
    undo.empty = false;
}

pub fn trx_undo_get_prev_rec(undo: *trx_undo_t) ?*types.trx_undo_record_t {
    if (undo.records.items.len < 2) {
        return null;
    }
    return &undo.records.items[undo.records.items.len - 2];
}

pub fn trx_undo_pop_record(undo: *trx_undo_t) ?types.trx_undo_record_t {
    if (undo.records.items.len == 0) {
        undo.empty = true;
        return null;
    }
    const idx = undo.records.items.len - 1;
    const rec = undo.records.items[idx];
    undo.records.items.len -= 1;
    undo.empty = undo.records.items.len == 0;
    if (!undo.empty) {
        undo.top_undo_no = undo.records.items[undo.records.items.len - 1].undo_no;
    }
    return rec;
}

pub fn trx_undo_truncate_end(undo: *trx_undo_t, limit: undo_no_t) void {
    while (undo.records.items.len > 0) {
        const idx = undo.records.items.len - 1;
        const rec = undo.records.items[idx];
        if (rec.undo_no.high < limit.high or (rec.undo_no.high == limit.high and rec.undo_no.low < limit.low)) {
            break;
        }
        undo.records.items.len -= 1;
        if (rec.data.len > 0) {
            undo.allocator.free(rec.data);
        }
    }
    undo.empty = undo.records.items.len == 0;
    if (!undo.empty) {
        undo.top_undo_no = undo.records.items[undo.records.items.len - 1].undo_no;
    }
}

test "trx undo append, prev, pop" {
    var undo = trx_undo_mem_create(1, TRX_UNDO_UPDATE, 11, std.testing.allocator);
    defer trx_undo_mem_free(undo);

    trx_undo_append_record(undo, .{ .undo_no = .{ .high = 0, .low = 1 }, .data = "a" });
    trx_undo_append_record(undo, .{ .undo_no = .{ .high = 0, .low = 2 }, .data = "b" });
    const prev = trx_undo_get_prev_rec(undo) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(ulint, 1), prev.undo_no.low);

    const popped = trx_undo_pop_record(undo) orelse return error.TestExpectedEqual;
    defer if (popped.data.len > 0) std.testing.allocator.free(popped.data);
    try std.testing.expectEqualStrings("b", popped.data);
}

test "trx undo truncate end" {
    var undo = trx_undo_mem_create(2, TRX_UNDO_INSERT, 12, std.testing.allocator);
    defer trx_undo_mem_free(undo);

    trx_undo_append_record(undo, .{ .undo_no = .{ .high = 0, .low = 3 }, .data = "c" });
    trx_undo_append_record(undo, .{ .undo_no = .{ .high = 0, .low = 5 }, .data = "d" });
    trx_undo_truncate_end(undo, .{ .high = 0, .low = 5 });
    try std.testing.expectEqual(@as(usize, 1), undo.records.items.len);
    try std.testing.expectEqual(@as(ulint, 3), undo.top_undo_no.low);
}
