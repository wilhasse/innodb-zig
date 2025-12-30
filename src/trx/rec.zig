const std = @import("std");
const compat = @import("../ut/compat.zig");
const mach = @import("../mach/mod.zig");
const dict = @import("../dict/mod.zig");
const data = @import("../data/mod.zig");
const mem = @import("../mem/mod.zig");

pub const module_name = "trx.rec";

pub const byte = compat.byte;
pub const ulint = compat.ulint;
pub const dulint = compat.Dulint;

pub const undo_no_t = dulint;
pub const table_id_t = dulint;
pub const trx_id_t = dulint;
pub const roll_ptr_t = dulint;

pub const TRX_UNDO_INSERT_REC: u8 = 11;
pub const TRX_UNDO_UPD_EXIST_REC: u8 = 12;
pub const TRX_UNDO_UPD_DEL_REC: u8 = 13;
pub const TRX_UNDO_DEL_MARK_REC: u8 = 14;
pub const TRX_UNDO_CMPL_INFO_MULT: u8 = 16;
pub const TRX_UNDO_UPD_EXTERN: u8 = 128;

pub const TRX_UNDO_PAGE_HDR: ulint = 0;
pub const TRX_UNDO_PAGE_FREE: ulint = 4;

const sql_null: ulint = @as(ulint, @intCast(compat.UNIV_SQL_NULL));
const extern_marker_const: ulint = @as(ulint, @intCast(compat.UNIV_EXTERN_STORAGE_FIELD));

pub const UndoHeader = struct {
    type: ulint = 0,
    cmpl_info: ulint = 0,
    updated_extern: bool = false,
    undo_no: undo_no_t = .{ .high = 0, .low = 0 },
    table_id: table_id_t = .{ .high = 0, .low = 0 },
};

pub const UpdateField = struct {
    field_no: ulint = 0,
    data: ?[]const byte = null,
    len: ulint = sql_null,
    orig_len: ulint = 0,
    extern_storage: bool = false,
};

pub const UpdateVector = struct {
    info_bits: ulint = 0,
    trx_id: trx_id_t = .{ .high = 0, .low = 0 },
    roll_ptr: roll_ptr_t = .{ .high = 0, .low = 0 },
    fields: []UpdateField = &[_]UpdateField{},
    allocator: std.mem.Allocator = std.heap.page_allocator,

    pub fn deinit(self: *UpdateVector) void {
        if (self.fields.len > 0) {
            self.allocator.free(self.fields);
        }
        self.fields = &[_]UpdateField{};
    }
};

pub fn undo_rec_get_type(undo_rec: []const byte) ulint {
    std.debug.assert(undo_rec.len >= 3);
    return undo_rec[2] & (TRX_UNDO_CMPL_INFO_MULT - 1);
}

pub fn undo_rec_get_cmpl_info(undo_rec: []const byte) ulint {
    std.debug.assert(undo_rec.len >= 3);
    return undo_rec[2] / TRX_UNDO_CMPL_INFO_MULT;
}

pub fn undo_rec_get_extern_storage(undo_rec: []const byte) bool {
    std.debug.assert(undo_rec.len >= 3);
    return (undo_rec[2] & TRX_UNDO_UPD_EXTERN) != 0;
}

pub fn undo_rec_get_undo_no(undo_rec: []const byte) undo_no_t {
    std.debug.assert(undo_rec.len >= 3);
    return mach.mach_dulint_read_much_compressed(undo_rec[3..].ptr);
}

pub fn undo_rec_get_offset(undo_no: undo_no_t) ulint {
    return 3 + mach.mach_dulint_get_much_compressed_size(undo_no);
}

pub fn undo_rec_copy(allocator: std.mem.Allocator, undo_rec: []const byte) []byte {
    std.debug.assert(undo_rec.len >= 2);
    const len = mach.mach_read_from_2(undo_rec.ptr);
    const copy_len = @min(len, undo_rec.len);
    const out = allocator.alloc(byte, copy_len) catch @panic("undo_rec_copy");
    std.mem.copyForwards(byte, out, undo_rec[0..copy_len]);
    return out;
}

pub fn undo_rec_get_pars(undo_rec: []const byte, header: *UndoHeader) ?usize {
    if (undo_rec.len < 3) {
        return null;
    }

    var ptr: usize = 2;
    var type_cmpl: u8 = undo_rec[ptr];
    ptr += 1;

    header.updated_extern = (type_cmpl & TRX_UNDO_UPD_EXTERN) != 0;
    if (header.updated_extern) {
        type_cmpl &= ~TRX_UNDO_UPD_EXTERN;
    }

    header.type = type_cmpl & (TRX_UNDO_CMPL_INFO_MULT - 1);
    header.cmpl_info = type_cmpl / TRX_UNDO_CMPL_INFO_MULT;

    if (ptr >= undo_rec.len) {
        return null;
    }
    header.undo_no = mach.mach_dulint_read_much_compressed(undo_rec[ptr..].ptr);
    const undo_size = mach.mach_dulint_get_much_compressed_size(header.undo_no);
    if (ptr + undo_size > undo_rec.len) {
        return null;
    }
    ptr += undo_size;

    if (ptr >= undo_rec.len) {
        return null;
    }
    header.table_id = mach.mach_dulint_read_much_compressed(undo_rec[ptr..].ptr);
    const table_size = mach.mach_dulint_get_much_compressed_size(header.table_id);
    if (ptr + table_size > undo_rec.len) {
        return null;
    }
    ptr += table_size;

    return ptr;
}

pub fn undo_rec_get_col_val(
    buf: []const byte,
    start: usize,
    field: *?[]const byte,
    len: *ulint,
    orig_len: *ulint,
) ?usize {
    if (start >= buf.len) {
        return null;
    }

    const extern_marker = extern_marker_const;
    const stored_len = mach.mach_read_compressed(buf[start..].ptr);
    const stored_size = mach.mach_get_compressed_size(stored_len);
    if (start + stored_size > buf.len) {
        return null;
    }
    var ptr = start + stored_size;

    orig_len.* = 0;
    len.* = stored_len;

    if (stored_len == sql_null) {
        field.* = null;
        return ptr;
    }

    if (stored_len == extern_marker) {
        const orig = mach.mach_read_compressed(buf[ptr..].ptr);
        const orig_size = mach.mach_get_compressed_size(orig);
        if (ptr + orig_size > buf.len) {
            return null;
        }
        ptr += orig_size;

        const real_len = mach.mach_read_compressed(buf[ptr..].ptr);
        const real_size = mach.mach_get_compressed_size(real_len);
        if (ptr + real_size > buf.len) {
            return null;
        }
        ptr += real_size;

        if (ptr + real_len > buf.len) {
            return null;
        }

        field.* = buf[ptr .. ptr + real_len];
        ptr += real_len;
        orig_len.* = orig;
        len.* = real_len + extern_marker;
        return ptr;
    }

    const data_len: ulint = if (stored_len >= extern_marker)
        stored_len - extern_marker
    else
        stored_len;
    if (ptr + data_len > buf.len) {
        return null;
    }
    field.* = buf[ptr .. ptr + data_len];
    ptr += data_len;
    return ptr;
}

pub fn undo_rec_get_row_ref(
    buf: []const byte,
    start: usize,
    index: *const dict.dict_index_t,
    heap: *mem.heap.MemHeap,
    ref: **data.dtuple_t,
) ?usize {
    const ref_len = dict.dict_index_get_n_unique(index);
    const tuple = data.dtuple_create(heap, ref_len) orelse return null;
    dict.dict_index_copy_types(tuple, index, ref_len);

    var ptr = start;
    var i: ulint = 0;
    while (i < ref_len) : (i += 1) {
        var field_ptr: ?[]const byte = null;
        var field_len: ulint = 0;
        var orig_len: ulint = 0;
        ptr = undo_rec_get_col_val(buf, ptr, &field_ptr, &field_len, &orig_len) orelse return null;
        const dfield = data.dtuple_get_nth_field(tuple, i);
        if (field_len == sql_null) {
            data.dfield_set_null(dfield);
        } else {
            const ptr_any: ?*const anyopaque = if (field_ptr) |slice|
                @as(*const anyopaque, @ptrCast(slice.ptr))
            else
                null;
            data.dfield_set_data(dfield, ptr_any, field_len);
        }
    }

    ref.* = tuple;
    return ptr;
}

pub fn undo_rec_skip_row_ref(
    buf: []const byte,
    start: usize,
    index: *const dict.dict_index_t,
) ?usize {
    const ref_len = dict.dict_index_get_n_unique(index);
    var ptr = start;
    var i: ulint = 0;
    while (i < ref_len) : (i += 1) {
        var field_ptr: ?[]const byte = null;
        var field_len: ulint = 0;
        var orig_len: ulint = 0;
        ptr = undo_rec_get_col_val(buf, ptr, &field_ptr, &field_len, &orig_len) orelse return null;
    }
    return ptr;
}

pub fn undo_update_rec_get_sys_cols(
    buf: []const byte,
    start: usize,
    trx_id: *trx_id_t,
    roll_ptr: *roll_ptr_t,
    info_bits: *ulint,
) ?usize {
    if (start >= buf.len) {
        return null;
    }

    info_bits.* = buf[start];
    var ptr = start + 1;

    if (ptr >= buf.len) {
        return null;
    }
    trx_id.* = mach.mach_dulint_read_compressed(buf[ptr..].ptr);
    const trx_size = mach.mach_dulint_get_compressed_size(trx_id.*);
    if (ptr + trx_size > buf.len) {
        return null;
    }
    ptr += trx_size;

    if (ptr >= buf.len) {
        return null;
    }
    roll_ptr.* = mach.mach_dulint_read_compressed(buf[ptr..].ptr);
    const roll_size = mach.mach_dulint_get_compressed_size(roll_ptr.*);
    if (ptr + roll_size > buf.len) {
        return null;
    }
    ptr += roll_size;

    return ptr;
}

pub fn undo_update_rec_get_n_upd_fields(buf: []const byte, start: usize, n: *ulint) ?usize {
    if (start >= buf.len) {
        return null;
    }
    const val = mach.mach_read_compressed(buf[start..].ptr);
    const size = mach.mach_get_compressed_size(val);
    if (start + size > buf.len) {
        return null;
    }
    n.* = val;
    return start + size;
}

pub fn undo_update_rec_get_field_no(buf: []const byte, start: usize, field_no: *ulint) ?usize {
    if (start >= buf.len) {
        return null;
    }
    const val = mach.mach_read_compressed(buf[start..].ptr);
    const size = mach.mach_get_compressed_size(val);
    if (start + size > buf.len) {
        return null;
    }
    field_no.* = val;
    return start + size;
}

pub fn undo_update_rec_get_update(
    buf: []const byte,
    start: usize,
    type_: ulint,
    trx_id: trx_id_t,
    roll_ptr: roll_ptr_t,
    info_bits: ulint,
    allocator: std.mem.Allocator,
    update: *UpdateVector,
) ?usize {
    const extern_marker = extern_marker_const;
    var ptr = start;
    var n_fields: ulint = 0;
    if (type_ != TRX_UNDO_DEL_MARK_REC) {
        ptr = undo_update_rec_get_n_upd_fields(buf, ptr, &n_fields) orelse return null;
    }

    var fields = if (n_fields > 0)
        allocator.alloc(UpdateField, @as(usize, @intCast(n_fields))) catch return null
    else
        &[_]UpdateField{};
    const needs_free = n_fields > 0;

    var i: ulint = 0;
    while (i < n_fields) : (i += 1) {
        var field_no: ulint = 0;
        ptr = undo_update_rec_get_field_no(buf, ptr, &field_no) orelse {
            if (needs_free) {
                allocator.free(fields);
            }
            return null;
        };

        var field_ptr: ?[]const byte = null;
        var field_len: ulint = 0;
        var orig_len: ulint = 0;
        ptr = undo_rec_get_col_val(buf, ptr, &field_ptr, &field_len, &orig_len) orelse {
            if (needs_free) {
                allocator.free(fields);
            }
            return null;
        };

        var extern_storage = false;
        var stored_len = field_len;
        if (field_len != sql_null and field_len >= extern_marker) {
            extern_storage = true;
            stored_len = field_len - extern_marker;
        }

        fields[@as(usize, @intCast(i))] = .{
            .field_no = field_no,
            .data = field_ptr,
            .len = stored_len,
            .orig_len = orig_len,
            .extern_storage = extern_storage,
        };
    }

    update.* = .{
        .info_bits = info_bits,
        .trx_id = trx_id,
        .roll_ptr = roll_ptr,
        .fields = fields,
        .allocator = allocator,
    };

    return ptr;
}

pub fn undo_parse_add_undo_rec(buf: []const byte, start: usize, page: ?[]byte) ?usize {
    if (start + 2 > buf.len) {
        return null;
    }

    const len = mach.mach_read_from_2(buf[start..].ptr);
    const ptr = start + 2;
    if (ptr + len > buf.len) {
        return null;
    }

    if (page) |p| {
        const hdr = TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FREE;
        if (hdr + 2 > p.len) {
            return null;
        }
        const first_free = mach.mach_read_from_2(p[hdr..].ptr);
        const end = first_free + 4 + len;
        if (end + 2 > p.len) {
            return null;
        }

        mach.mach_write_to_2(p[first_free..].ptr, end);
        mach.mach_write_to_2(p[first_free + 2 + len ..].ptr, first_free);
        mach.mach_write_to_2(p[hdr..].ptr, end);
        std.mem.copyForwards(byte, p[first_free + 2 .. first_free + 2 + len], buf[ptr .. ptr + len]);
    }

    return ptr + len;
}

test "trx undo header parse" {
    var buf = [_]byte{0} ** 64;
    var ptr: usize = 2;
    buf[ptr] = TRX_UNDO_UPD_EXTERN | (TRX_UNDO_UPD_EXIST_REC + 3 * TRX_UNDO_CMPL_INFO_MULT);
    ptr += 1;
    const undo_no: undo_no_t = .{ .high = 0, .low = 9 };
    ptr += mach.mach_dulint_write_much_compressed(buf[ptr..].ptr, undo_no);
    const table_id: table_id_t = .{ .high = 1, .low = 5 };
    ptr += mach.mach_dulint_write_much_compressed(buf[ptr..].ptr, table_id);

    var header: UndoHeader = .{};
    const next = undo_rec_get_pars(buf[0..ptr], &header) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(ulint, TRX_UNDO_UPD_EXIST_REC), header.type);
    try std.testing.expectEqual(@as(ulint, 3), header.cmpl_info);
    try std.testing.expect(header.updated_extern);
    try std.testing.expectEqual(undo_no.high, header.undo_no.high);
    try std.testing.expectEqual(undo_no.low, header.undo_no.low);
    try std.testing.expectEqual(table_id.high, header.table_id.high);
    try std.testing.expectEqual(table_id.low, header.table_id.low);
    try std.testing.expectEqual(ptr, next);
}

test "trx undo col val parsing" {
    var buf = [_]byte{0} ** 64;
    var ptr: usize = 0;
    ptr += mach.mach_write_compressed(buf[ptr..].ptr, sql_null);
    var field: ?[]const byte = null;
    var len: ulint = 0;
    var orig_len: ulint = 0;
    const next = undo_rec_get_col_val(buf[0..ptr], 0, &field, &len, &orig_len) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(sql_null, len);
    try std.testing.expect(field == null);
    try std.testing.expectEqual(ptr, next);

    ptr = 0;
    ptr += mach.mach_write_compressed(buf[ptr..].ptr, extern_marker_const);
    ptr += mach.mach_write_compressed(buf[ptr..].ptr, 4);
    ptr += mach.mach_write_compressed(buf[ptr..].ptr, 6);
    std.mem.copyForwards(byte, buf[ptr .. ptr + 6], "EXTVAL");
    ptr += 6;
    const next2 = undo_rec_get_col_val(buf[0..ptr], 0, &field, &len, &orig_len) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(ulint, 4), orig_len);
    try std.testing.expectEqual(@as(ulint, 6) + extern_marker_const, len);
    try std.testing.expect(field != null);
    try std.testing.expectEqual(true, std.mem.eql(u8, "EXTVAL", field.?));
    try std.testing.expectEqual(ptr, next2);
}

test "trx undo row ref parse and skip" {
    var buf = [_]byte{0} ** 64;
    var ptr: usize = 0;
    ptr += mach.mach_write_compressed(buf[ptr..].ptr, 2);
    std.mem.copyForwards(byte, buf[ptr .. ptr + 2], "aa");
    ptr += 2;
    ptr += mach.mach_write_compressed(buf[ptr..].ptr, 3);
    std.mem.copyForwards(byte, buf[ptr .. ptr + 3], "bbb");
    ptr += 3;

    var table = dict.dict_table_t{};
    var col1 = dict.dict_col_t{ .mtype = data.DATA_INT, .len = 4 };
    var col2 = dict.dict_col_t{ .mtype = data.DATA_INT, .len = 4 };
    var index = dict.dict_index_t{};
    dict.dict_index_add_col(&index, &table, &col1, 0);
    dict.dict_index_add_col(&index, &table, &col2, 0);

    var heap = try mem.heap.MemHeap.init(std.testing.allocator, 128, .dynamic);
    defer heap.deinit();
    var ref: *data.dtuple_t = undefined;
    const next = undo_rec_get_row_ref(buf[0..ptr], 0, &index, &heap, &ref) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(ptr, next);
    try std.testing.expectEqual(@as(ulint, 2), data.dfield_get_len(data.dtuple_get_nth_field(ref, 0)));
    try std.testing.expectEqual(@as(ulint, 3), data.dfield_get_len(data.dtuple_get_nth_field(ref, 1)));

    const f0_ptr = data.dfield_get_data(data.dtuple_get_nth_field(ref, 0)).?;
    const f1_ptr = data.dfield_get_data(data.dtuple_get_nth_field(ref, 1)).?;
    try std.testing.expectEqual(true, std.mem.eql(u8, "aa", @as([*]const u8, @ptrCast(f0_ptr))[0..2]));
    try std.testing.expectEqual(true, std.mem.eql(u8, "bbb", @as([*]const u8, @ptrCast(f1_ptr))[0..3]));

    const skip_next = undo_rec_skip_row_ref(buf[0..ptr], 0, &index) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(ptr, skip_next);

    index.fields.deinit(std.heap.page_allocator);
}

test "trx undo update sys cols and update vector" {
    var buf = [_]byte{0} ** 128;
    var ptr: usize = 0;
    buf[ptr] = 0x7;
    ptr += 1;

    const trx_id: trx_id_t = .{ .high = 0, .low = 0x10 };
    ptr += mach.mach_dulint_write_compressed(buf[ptr..].ptr, trx_id);
    const roll_ptr: roll_ptr_t = .{ .high = 0, .low = 0x20 };
    ptr += mach.mach_dulint_write_compressed(buf[ptr..].ptr, roll_ptr);

    ptr += mach.mach_write_compressed(buf[ptr..].ptr, 1);
    ptr += mach.mach_write_compressed(buf[ptr..].ptr, 3);
    ptr += mach.mach_write_compressed(buf[ptr..].ptr, 2);
    std.mem.copyForwards(byte, buf[ptr .. ptr + 2], "xy");
    ptr += 2;

    var out_trx: trx_id_t = .{ .high = 0, .low = 0 };
    var out_roll: roll_ptr_t = .{ .high = 0, .low = 0 };
    var info_bits: ulint = 0;
    const next = undo_update_rec_get_sys_cols(buf[0..ptr], 0, &out_trx, &out_roll, &info_bits) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(ulint, 0x7), info_bits);
    try std.testing.expectEqual(trx_id.low, out_trx.low);
    try std.testing.expectEqual(roll_ptr.low, out_roll.low);

    var update: UpdateVector = .{};
    defer update.deinit();
    const after = undo_update_rec_get_update(
        buf[0..ptr],
        next,
        TRX_UNDO_UPD_EXIST_REC,
        out_trx,
        out_roll,
        info_bits,
        std.testing.allocator,
        &update,
    ) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(ptr, after);
    try std.testing.expectEqual(@as(usize, 1), update.fields.len);
    try std.testing.expectEqual(@as(ulint, 3), update.fields[0].field_no);
    try std.testing.expectEqual(@as(ulint, 2), update.fields[0].len);
    try std.testing.expectEqual(true, std.mem.eql(u8, "xy", update.fields[0].data.?));
}

test "trx undo parse add undo rec" {
    var page = [_]byte{0} ** 64;
    mach.mach_write_to_2(page[TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FREE ..].ptr, 8);

    var log = [_]byte{0} ** 16;
    mach.mach_write_to_2(&log, 3);
    log[2] = 'a';
    log[3] = 'b';
    log[4] = 'c';

    const next = undo_parse_add_undo_rec(log[0..5], 0, &page) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 5), next);
    const free = mach.mach_read_from_2(page[TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FREE ..].ptr);
    try std.testing.expectEqual(@as(ulint, 15), free);
    try std.testing.expectEqual(@as(ulint, 15), mach.mach_read_from_2(page[8..].ptr));
    try std.testing.expectEqual(@as(ulint, 8), mach.mach_read_from_2(page[8 + 2 + 3 ..].ptr));
    try std.testing.expectEqual(true, std.mem.eql(u8, "abc", page[10..13]));
}
