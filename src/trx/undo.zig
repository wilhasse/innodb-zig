const std = @import("std");
const compat = @import("../ut/compat.zig");
const mach = @import("../mach/mod.zig");
const fil = @import("../fil/mod.zig");
const buf_mod = @import("../buf/mod.zig");
const types = @import("types.zig");
const rseg = @import("rseg.zig");

pub const module_name = "trx.undo";

pub const ulint = compat.ulint;
pub const byte = compat.byte;
pub const dulint = compat.Dulint;
pub const undo_no_t = types.undo_no_t;
pub const trx_id_t = types.trx_id_t;

pub const TRX_UNDO_INSERT: ulint = 1;
pub const TRX_UNDO_UPDATE: ulint = 2;

// Undo record types (from C trx0rec.h)
pub const TRX_UNDO_INSERT_REC: ulint = 11; // fresh insert into clustered index
pub const TRX_UNDO_UPD_EXIST_REC: ulint = 12; // update of a non-delete-marked record
pub const TRX_UNDO_UPD_DEL_REC: ulint = 13; // update of a delete marked to not delete marked
pub const TRX_UNDO_DEL_MARK_REC: ulint = 14; // delete marking of a record
pub const TRX_UNDO_CMPL_INFO_MULT: ulint = 16; // compilation info multiplier
pub const TRX_UNDO_UPD_EXTERN: ulint = 128; // updated external storage fields

// Operation type flags
pub const TRX_UNDO_INSERT_OP: ulint = 1;
pub const TRX_UNDO_MODIFY_OP: ulint = 2;

pub const TRX_UNDO_PAGE_HDR: ulint = fil.FIL_PAGE_DATA;
pub const TRX_UNDO_PAGE_HDR_SIZE: ulint = 32;
pub const TRX_UNDO_PAGE_TYPE: ulint = 0;
pub const TRX_UNDO_PAGE_RSEG_ID: ulint = 2;
pub const TRX_UNDO_PAGE_TRX_ID: ulint = 4;
pub const TRX_UNDO_PAGE_FREE: ulint = 12;
pub const TRX_UNDO_PAGE_FIRST: ulint = 14;
pub const TRX_UNDO_PAGE_LAST: ulint = 16;
pub const TRX_UNDO_PAGE_N_RECS: ulint = 18;
pub const TRX_UNDO_PAGE_DATA_START: ulint = TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_HDR_SIZE;
const TRX_UNDO_PAGE_DATA_END: ulint = compat.UNIV_PAGE_SIZE - fil.FIL_PAGE_END_LSN_OLD_CHKSUM;

/// Undo record type enum for Zig code
pub const UndoRecType = enum(u8) {
    insert = 11, // TRX_UNDO_INSERT_REC
    update_exist = 12, // TRX_UNDO_UPD_EXIST_REC
    update_del = 13, // TRX_UNDO_UPD_DEL_REC
    del_mark = 14, // TRX_UNDO_DEL_MARK_REC

    pub fn toUlint(self: UndoRecType) ulint {
        return @intFromEnum(self);
    }

    pub fn fromUlint(v: ulint) ?UndoRecType {
        return switch (v) {
            11 => .insert,
            12 => .update_exist,
            13 => .update_del,
            14 => .del_mark,
            else => null,
        };
    }
};

const UndoPageHeader = struct {
    undo_type: ulint = TRX_UNDO_INSERT,
    rseg_id: ulint = 0,
    trx_id: trx_id_t = 0,
    free_offset: ulint = TRX_UNDO_PAGE_DATA_START,
    first_rec: ulint = 0,
    last_rec: ulint = 0,
    n_recs: ulint = 0,
};

fn undo_page_write_header(page: []byte, header: *const UndoPageHeader) void {
    mach.mach_write_to_2(page.ptr + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE, header.undo_type);
    mach.mach_write_to_2(page.ptr + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_RSEG_ID, header.rseg_id);
    mach.mach_write_to_8(
        page.ptr + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TRX_ID,
        .{ .high = @intCast(header.trx_id >> 32), .low = @intCast(header.trx_id & 0xFFFFFFFF) },
    );
    mach.mach_write_to_2(page.ptr + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FREE, header.free_offset);
    mach.mach_write_to_2(page.ptr + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FIRST, header.first_rec);
    mach.mach_write_to_2(page.ptr + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_LAST, header.last_rec);
    mach.mach_write_to_2(page.ptr + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_N_RECS, header.n_recs);
}

fn undo_page_read_header(page: []const byte) UndoPageHeader {
    const trx_id_dul = mach.mach_read_from_8(page.ptr + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TRX_ID);
    const trx_id: trx_id_t = (@as(u64, trx_id_dul.high) << 32) | trx_id_dul.low;
    return .{
        .undo_type = mach.mach_read_from_2(page.ptr + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE),
        .rseg_id = mach.mach_read_from_2(page.ptr + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_RSEG_ID),
        .trx_id = trx_id,
        .free_offset = mach.mach_read_from_2(page.ptr + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FREE),
        .first_rec = mach.mach_read_from_2(page.ptr + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FIRST),
        .last_rec = mach.mach_read_from_2(page.ptr + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_LAST),
        .n_recs = mach.mach_read_from_2(page.ptr + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_N_RECS),
    };
}

fn undo_page_init(
    page: []byte,
    space_id: ulint,
    page_no: ulint,
    undo_type: ulint,
    rseg_id: ulint,
    trx_id: trx_id_t,
    prev_page_no: ulint,
) UndoPageHeader {
    @memset(page, 0);
    mach.mach_write_to_4(page.ptr + fil.FIL_PAGE_OFFSET, page_no);
    mach.mach_write_to_4(page.ptr + fil.FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID, space_id);
    mach.mach_write_to_4(page.ptr + fil.FIL_PAGE_PREV, prev_page_no);
    mach.mach_write_to_4(page.ptr + fil.FIL_PAGE_NEXT, fil.FIL_NULL);
    fil.fil_page_set_type(page.ptr, fil.FIL_PAGE_UNDO_LOG);

    var header = UndoPageHeader{
        .undo_type = undo_type,
        .rseg_id = rseg_id,
        .trx_id = trx_id,
        .free_offset = TRX_UNDO_PAGE_DATA_START,
        .first_rec = 0,
        .last_rec = 0,
        .n_recs = 0,
    };
    undo_page_write_header(page, &header);
    return header;
}

fn undo_page_append(page: []byte, header: *UndoPageHeader, rec: *const UndoRec, data_len: usize) bool {
    if (data_len > std.math.maxInt(u16)) {
        return false;
    }
    const encoded_size = rec.encodedSize();
    const total = @as(ulint, @intCast(encoded_size + 2));
    if (header.free_offset + total > TRX_UNDO_PAGE_DATA_END) {
        return false;
    }
    const offset = header.free_offset;
    mach.mach_write_to_2(page.ptr + offset, @as(ulint, @intCast(data_len)));
    const written = rec.encode(page[offset + 2 ..]);
    if (written != encoded_size) {
        return false;
    }
    header.free_offset += total;
    if (header.n_recs == 0) {
        header.first_rec = offset;
    }
    header.last_rec = offset;
    header.n_recs += 1;
    undo_page_write_header(page, header);
    return true;
}

/// Undo record header - minimal header for each undo record
/// Layout: [type_cmpl:1][undo_no:compressed][table_id:8][data...]
pub const UndoRecHeader = struct {
    rec_type: UndoRecType = .insert,
    cmpl_info: u4 = 0, // compiler info (0-15)
    updated_extern: bool = false,
    undo_no: undo_no_t = types.dulintZero(),
    table_id: dulint = types.dulintZero(),

    /// Maximum encoded header size (type_cmpl:1 + undo_no:9 + table_id:8)
    pub const MAX_SIZE: usize = 18;

    /// Encode header into buffer, returns bytes written
    pub fn encode(self: *const UndoRecHeader, buf: []byte) usize {
        if (buf.len < MAX_SIZE) return 0;

        var pos: usize = 0;

        // Encode type_cmpl byte: type | (cmpl_info * 16) | extern_flag
        var type_cmpl: byte = @intFromEnum(self.rec_type);
        type_cmpl |= (@as(byte, self.cmpl_info) << 4); // cmpl_info * 16
        if (self.updated_extern) {
            type_cmpl |= TRX_UNDO_UPD_EXTERN;
        }
        buf[pos] = type_cmpl;
        pos += 1;

        // Encode undo_no using much_compressed format
        pos += mach.mach_dulint_write_much_compressed(buf[pos..].ptr, self.undo_no);

        // Encode table_id as 8 bytes
        mach.mach_write_to_8(buf[pos..].ptr, self.table_id);
        pos += 8;

        return pos;
    }

    /// Decode header from buffer, returns bytes consumed (0 on error)
    pub fn decode(buf: []const byte) ?struct { header: UndoRecHeader, size: usize } {
        if (buf.len < 2) return null; // minimum: 1 byte type + 1 byte undo_no

        var pos: usize = 0;

        // Decode type_cmpl byte
        const type_cmpl = buf[pos];
        pos += 1;

        const rec_type_val = type_cmpl & 0x0F;
        const rec_type = UndoRecType.fromUlint(rec_type_val) orelse return null;
        const cmpl_info: u4 = @intCast((type_cmpl >> 4) & 0x07);
        const updated_extern = (type_cmpl & TRX_UNDO_UPD_EXTERN) != 0;

        // Decode undo_no
        const undo_no = mach.mach_dulint_read_much_compressed(buf[pos..].ptr);
        pos += mach.mach_dulint_get_much_compressed_size(undo_no);

        // Need at least 8 more bytes for table_id
        if (buf.len < pos + 8) return null;

        // Decode table_id
        const table_id = mach.mach_read_from_8(buf[pos..].ptr);
        pos += 8;

        return .{
            .header = .{
                .rec_type = rec_type,
                .cmpl_info = cmpl_info,
                .updated_extern = updated_extern,
                .undo_no = undo_no,
                .table_id = table_id,
            },
            .size = pos,
        };
    }

    /// Get encoded size without writing
    pub fn encodedSize(self: *const UndoRecHeader) usize {
        return 1 + // type_cmpl
            mach.mach_dulint_get_much_compressed_size(self.undo_no) +
            8; // table_id
    }
};

/// Full undo record with header and data payload
pub const UndoRec = struct {
    header: UndoRecHeader = .{},
    trx_id: trx_id_t = 0, // transaction that created this record
    roll_ptr: types.roll_ptr_t = types.dulintZero(), // for update records
    data: []const u8 = &[_]u8{}, // primary key + before-image

    /// Maximum header overhead (UndoRecHeader + trx_id:8 + roll_ptr:8)
    pub const MAX_HEADER_SIZE: usize = UndoRecHeader.MAX_SIZE + 16;

    /// Encode full record to buffer, returns bytes written
    pub fn encode(self: *const UndoRec, buf: []byte) usize {
        if (buf.len < MAX_HEADER_SIZE + self.data.len) return 0;

        var pos: usize = 0;

        // Encode header
        pos += self.header.encode(buf[pos..]);

        // Encode trx_id as 8 bytes
        mach.mach_write_to_8(buf[pos..].ptr, .{ .high = @intCast(self.trx_id >> 32), .low = @intCast(self.trx_id & 0xFFFFFFFF) });
        pos += 8;

        // Encode roll_ptr for update records
        if (self.header.rec_type != .insert) {
            mach.mach_write_to_8(buf[pos..].ptr, self.roll_ptr);
            pos += 8;
        }

        // Copy data payload
        if (self.data.len > 0) {
            @memcpy(buf[pos..][0..self.data.len], self.data);
            pos += self.data.len;
        }

        return pos;
    }

    /// Decode record from buffer (data slice points into original buffer)
    pub fn decode(buf: []const byte, data_len: usize) ?struct { rec: UndoRec, size: usize } {
        const hdr_result = UndoRecHeader.decode(buf) orelse return null;
        var pos = hdr_result.size;

        // Decode trx_id
        if (buf.len < pos + 8) return null;
        const trx_id_dul = mach.mach_read_from_8(buf[pos..].ptr);
        const trx_id: trx_id_t = (@as(u64, trx_id_dul.high) << 32) | trx_id_dul.low;
        pos += 8;

        // Decode roll_ptr for update records
        var roll_ptr = types.dulintZero();
        if (hdr_result.header.rec_type != .insert) {
            if (buf.len < pos + 8) return null;
            roll_ptr = mach.mach_read_from_8(buf[pos..].ptr);
            pos += 8;
        }

        // Extract data slice
        if (buf.len < pos + data_len) return null;
        const data = if (data_len > 0) buf[pos..][0..data_len] else &[_]u8{};
        pos += data_len;

        return .{
            .rec = .{
                .header = hdr_result.header,
                .trx_id = trx_id,
                .roll_ptr = roll_ptr,
                .data = data,
            },
            .size = pos,
        };
    }

    /// Get encoded size without writing
    pub fn encodedSize(self: *const UndoRec) usize {
        var size = self.header.encodedSize() + 8; // + trx_id
        if (self.header.rec_type != .insert) {
            size += 8; // roll_ptr
        }
        size += self.data.len;
        return size;
    }
};

pub const trx_undo_t = struct {
    id: ulint = 0,
    type: ulint = TRX_UNDO_INSERT,
    trx_id: trx_id_t = 0,
    top_undo_no: undo_no_t = types.dulintZero(),
    empty: bool = true,
    records: std.ArrayListUnmanaged(types.trx_undo_record_t) = .{},
    allocator: std.mem.Allocator = std.heap.page_allocator,
    space_id: ulint = fil.FIL_NULL,
    rseg_id: ulint = 0,
    first_page_no: ulint = fil.FIL_NULL,
    last_page_no: ulint = fil.FIL_NULL,
    last_offset: ulint = 0,

    /// Get the undo log as a typed pointer from trx_t's insert_undo/update_undo
    pub fn fromOpaquePtr(ptr: ?*anyopaque) ?*trx_undo_t {
        if (ptr) |p| {
            return @ptrCast(@alignCast(p));
        }
        return null;
    }

    /// Convert to opaque pointer for storage in trx_t
    pub fn toOpaquePtr(self: *trx_undo_t) *anyopaque {
        return @ptrCast(self);
    }
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
    trx_undo_detach_from_rseg(undo);
    undo.allocator.destroy(undo);
}

fn trx_undo_mem_free_opaque(ptr: *types.trx_undo_t) void {
    const undo: *trx_undo_t = @ptrCast(@alignCast(ptr));
    trx_undo_mem_free(undo);
}

fn trx_undo_list_remove(list: *std.ArrayListUnmanaged(*types.trx_undo_t), undo_ptr: *types.trx_undo_t) void {
    var idx: usize = 0;
    while (idx < list.items.len) : (idx += 1) {
        if (list.items[idx] == undo_ptr) {
            _ = list.orderedRemove(idx);
            break;
        }
    }
}

fn trx_undo_detach_from_rseg(undo: *trx_undo_t) void {
    if (undo.space_id == fil.FIL_NULL) {
        return;
    }
    const rseg_ptr = rseg.trx_rseg_get_on_id(undo.rseg_id) orelse return;
    const undo_ptr: *types.trx_undo_t = @ptrCast(undo);
    trx_undo_list_remove(&rseg_ptr.insert_undo_list, undo_ptr);
    trx_undo_list_remove(&rseg_ptr.update_undo_list, undo_ptr);
}

pub fn trx_undo_append_record(undo: *trx_undo_t, record: types.trx_undo_record_t) void {
    var data_copy: []u8 = &[_]u8{};
    if (record.data.len > 0) {
        data_copy = undo.allocator.alloc(u8, record.data.len) catch @panic("trx_undo_append_record");
    }
    if (record.data.len > 0) {
        std.mem.copyForwards(u8, data_copy, record.data);
    }
    var copy = record;
    copy.data = data_copy;
    undo.records.append(undo.allocator, copy) catch @panic("trx_undo_append_record");
    undo.top_undo_no = record.undo_no;
    undo.empty = false;

    if (undo.space_id != fil.FIL_NULL) {
        trx_undo_append_record_persistent(undo, copy);
    }
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

fn trx_undo_pick_rseg(allocator: std.mem.Allocator) ?*rseg.trx_rseg_t {
    if (rseg.trx_sys.rsegs.len == 0) {
        rseg.trx_sys_init(allocator);
    }
    if (rseg.trx_sys.rseg_list.items.len == 0) {
        var slot: ulint = 0;
        _ = rseg.trx_rseg_header_create(0, 0, 128, &slot);
    }
    if (rseg.trx_sys.rseg_list.items.len == 0) {
        return null;
    }
    return rseg.trx_sys.rseg_list.items[0];
}

fn trx_undo_attach_rseg(undo: *trx_undo_t, is_insert: bool, allocator: std.mem.Allocator) void {
    if (undo.space_id != fil.FIL_NULL) {
        return;
    }
    if (fil.fil_tablespace_exists_in_mem(0) != compat.TRUE) {
        return;
    }
    const rseg_ptr = trx_undo_pick_rseg(allocator) orelse return;
    rseg.trx_rseg_set_undo_free(trx_undo_mem_free_opaque);
    undo.space_id = rseg_ptr.space;
    undo.rseg_id = rseg_ptr.id;
    const undo_ptr: *types.trx_undo_t = @ptrCast(undo);
    const rseg_alloc = rseg.trx_sys.allocator;
    if (is_insert) {
        rseg_ptr.insert_undo_list.append(rseg_alloc, undo_ptr) catch {};
    } else {
        rseg_ptr.update_undo_list.append(rseg_alloc, undo_ptr) catch {};
    }
}

fn trx_undo_update_prev_page_next(undo: *trx_undo_t, prev_page_no: ulint, next_page_no: ulint) void {
    var page: [compat.UNIV_PAGE_SIZE]byte = undefined;
    if (fil.fil_read_page(undo.space_id, prev_page_no, page[0..].ptr) != fil.DB_SUCCESS) {
        return;
    }
    mach.mach_write_to_4(page[0..].ptr + fil.FIL_PAGE_NEXT, next_page_no);
    buf_mod.buf_flush_init_for_writing(page[0..].ptr, null, 0);
    _ = fil.fil_write_page(undo.space_id, prev_page_no, page[0..].ptr);
}

fn trx_undo_alloc_page_no(undo: *trx_undo_t) ?ulint {
    const rseg_ptr = rseg.trx_rseg_get_on_id(undo.rseg_id) orelse return null;
    return rseg.trx_rseg_alloc_undo_page(rseg_ptr);
}

fn trx_undo_append_record_persistent(undo: *trx_undo_t, record: types.trx_undo_record_t) void {
    const rec_type = if (record.is_insert) UndoRecType.insert else UndoRecType.update_exist;
    var undo_rec = UndoRec{
        .header = .{
            .rec_type = rec_type,
            .undo_no = record.undo_no,
            .table_id = types.dulintZero(),
        },
        .trx_id = undo.trx_id,
        .roll_ptr = record.roll_ptr,
        .data = record.data,
    };

    var page_buf: [compat.UNIV_PAGE_SIZE]byte = undefined;
    var page_no = undo.last_page_no;
    var header: UndoPageHeader = undefined;

    if (page_no == fil.FIL_NULL) {
        const new_page = trx_undo_alloc_page_no(undo) orelse return;
        header = undo_page_init(page_buf[0..], undo.space_id, new_page, undo.type, undo.rseg_id, undo.trx_id, fil.FIL_NULL);
        page_no = new_page;
        if (!undo_page_append(page_buf[0..], &header, &undo_rec, record.data.len)) {
            return;
        }
    } else {
        if (fil.fil_read_page(undo.space_id, page_no, page_buf[0..].ptr) != fil.DB_SUCCESS) {
            return;
        }
        header = undo_page_read_header(page_buf[0..]);
        if (!undo_page_append(page_buf[0..], &header, &undo_rec, record.data.len)) {
            const new_page = trx_undo_alloc_page_no(undo) orelse return;
            trx_undo_update_prev_page_next(undo, page_no, new_page);
            header = undo_page_init(page_buf[0..], undo.space_id, new_page, undo.type, undo.rseg_id, undo.trx_id, page_no);
            page_no = new_page;
            if (!undo_page_append(page_buf[0..], &header, &undo_rec, record.data.len)) {
                return;
            }
        }
    }

    buf_mod.buf_flush_init_for_writing(page_buf[0..].ptr, null, 0);
    _ = fil.fil_write_page(undo.space_id, page_no, page_buf[0..].ptr);
    if (undo.first_page_no == fil.FIL_NULL) {
        undo.first_page_no = page_no;
    }
    undo.last_page_no = page_no;
    undo.last_offset = header.free_offset;
}

test "trx undo append, prev, pop" {
    const undo = trx_undo_mem_create(1, TRX_UNDO_UPDATE, 11, std.testing.allocator);
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
    const undo = trx_undo_mem_create(2, TRX_UNDO_INSERT, 12, std.testing.allocator);
    defer trx_undo_mem_free(undo);

    trx_undo_append_record(undo, .{ .undo_no = .{ .high = 0, .low = 3 }, .data = "c" });
    trx_undo_append_record(undo, .{ .undo_no = .{ .high = 0, .low = 5 }, .data = "d" });
    trx_undo_truncate_end(undo, .{ .high = 0, .low = 5 });
    try std.testing.expectEqual(@as(usize, 1), undo.records.items.len);
    try std.testing.expectEqual(@as(ulint, 3), undo.top_undo_no.low);
}

test "UndoRecType conversion" {
    try std.testing.expectEqual(@as(ulint, 11), UndoRecType.insert.toUlint());
    try std.testing.expectEqual(@as(ulint, 12), UndoRecType.update_exist.toUlint());
    try std.testing.expectEqual(@as(ulint, 13), UndoRecType.update_del.toUlint());
    try std.testing.expectEqual(@as(ulint, 14), UndoRecType.del_mark.toUlint());

    try std.testing.expectEqual(UndoRecType.insert, UndoRecType.fromUlint(11).?);
    try std.testing.expectEqual(UndoRecType.update_exist, UndoRecType.fromUlint(12).?);
    try std.testing.expect(UndoRecType.fromUlint(0) == null);
    try std.testing.expect(UndoRecType.fromUlint(255) == null);
}

test "UndoRecHeader encode/decode roundtrip" {
    var buf: [UndoRecHeader.MAX_SIZE]byte = undefined;

    // Test insert record header
    const insert_hdr = UndoRecHeader{
        .rec_type = .insert,
        .cmpl_info = 0,
        .updated_extern = false,
        .undo_no = .{ .high = 0, .low = 42 },
        .table_id = .{ .high = 0, .low = 100 },
    };
    const insert_size = insert_hdr.encode(&buf);
    try std.testing.expect(insert_size > 0);

    const insert_result = UndoRecHeader.decode(&buf) orelse return error.DecodeFailed;
    try std.testing.expectEqual(UndoRecType.insert, insert_result.header.rec_type);
    try std.testing.expectEqual(@as(u4, 0), insert_result.header.cmpl_info);
    try std.testing.expectEqual(false, insert_result.header.updated_extern);
    try std.testing.expectEqual(@as(ulint, 42), insert_result.header.undo_no.low);
    try std.testing.expectEqual(@as(ulint, 100), insert_result.header.table_id.low);

    // Test update record with extern flag
    const update_hdr = UndoRecHeader{
        .rec_type = .update_exist,
        .cmpl_info = 3,
        .updated_extern = true,
        .undo_no = .{ .high = 1, .low = 0x12345678 },
        .table_id = .{ .high = 2, .low = 0xABCDEF00 },
    };
    const update_size = update_hdr.encode(&buf);
    try std.testing.expect(update_size > 0);

    const update_result = UndoRecHeader.decode(&buf) orelse return error.DecodeFailed;
    try std.testing.expectEqual(UndoRecType.update_exist, update_result.header.rec_type);
    try std.testing.expectEqual(@as(u4, 3), update_result.header.cmpl_info);
    try std.testing.expectEqual(true, update_result.header.updated_extern);
    try std.testing.expectEqual(@as(ulint, 1), update_result.header.undo_no.high);
    try std.testing.expectEqual(@as(ulint, 0x12345678), update_result.header.undo_no.low);
    try std.testing.expectEqual(@as(ulint, 2), update_result.header.table_id.high);
}

test "UndoRec encode/decode roundtrip insert" {
    var buf: [64]byte = undefined;
    const payload = "test_pk_data";

    const rec = UndoRec{
        .header = .{
            .rec_type = .insert,
            .cmpl_info = 0,
            .undo_no = .{ .high = 0, .low = 1 },
            .table_id = .{ .high = 0, .low = 50 },
        },
        .trx_id = 0x123456789ABC,
        .data = payload,
    };

    const encoded_size = rec.encode(&buf);
    try std.testing.expect(encoded_size > 0);
    try std.testing.expectEqual(rec.encodedSize(), encoded_size);

    const result = UndoRec.decode(&buf, payload.len) orelse return error.DecodeFailed;
    try std.testing.expectEqual(UndoRecType.insert, result.rec.header.rec_type);
    try std.testing.expectEqual(@as(trx_id_t, 0x123456789ABC), result.rec.trx_id);
    try std.testing.expectEqual(@as(ulint, 1), result.rec.header.undo_no.low);
    try std.testing.expectEqual(@as(ulint, 50), result.rec.header.table_id.low);
    try std.testing.expectEqualStrings(payload, result.rec.data);
}

test "UndoRec encode/decode roundtrip update" {
    var buf: [64]byte = undefined;
    const payload = "old_value";

    const rec = UndoRec{
        .header = .{
            .rec_type = .update_exist,
            .cmpl_info = 2,
            .updated_extern = false,
            .undo_no = .{ .high = 0, .low = 10 },
            .table_id = .{ .high = 0, .low = 200 },
        },
        .trx_id = 0xFEDCBA987654,
        .roll_ptr = .{ .high = 5, .low = 999 },
        .data = payload,
    };

    const encoded_size = rec.encode(&buf);
    try std.testing.expect(encoded_size > 0);
    try std.testing.expectEqual(rec.encodedSize(), encoded_size);

    const result = UndoRec.decode(&buf, payload.len) orelse return error.DecodeFailed;
    try std.testing.expectEqual(UndoRecType.update_exist, result.rec.header.rec_type);
    try std.testing.expectEqual(@as(u4, 2), result.rec.header.cmpl_info);
    try std.testing.expectEqual(@as(trx_id_t, 0xFEDCBA987654), result.rec.trx_id);
    try std.testing.expectEqual(@as(ulint, 5), result.rec.roll_ptr.high);
    try std.testing.expectEqual(@as(ulint, 999), result.rec.roll_ptr.low);
    try std.testing.expectEqualStrings(payload, result.rec.data);
}

test "UndoRecHeader encodedSize matches encode" {
    const headers = [_]UndoRecHeader{
        .{ .rec_type = .insert, .undo_no = .{ .high = 0, .low = 0 }, .table_id = .{ .high = 0, .low = 0 } },
        .{ .rec_type = .del_mark, .undo_no = .{ .high = 0, .low = 127 }, .table_id = .{ .high = 0, .low = 1 } },
        .{ .rec_type = .update_del, .undo_no = .{ .high = 100, .low = 200 }, .table_id = .{ .high = 300, .low = 400 } },
    };

    var buf: [UndoRecHeader.MAX_SIZE]byte = undefined;
    for (headers) |hdr| {
        const actual_size = hdr.encode(&buf);
        try std.testing.expectEqual(hdr.encodedSize(), actual_size);
    }
}

// ============================================================================
// Transaction Undo Log Management (IBD-209)
// ============================================================================

/// Get or create the insert undo log for a transaction
pub fn trx_undo_assign_insert(trx: *types.trx_t) *trx_undo_t {
    if (trx_undo_t.fromOpaquePtr(trx.insert_undo)) |undo| {
        return undo;
    }
    // Create new insert undo log
    const undo = trx_undo_mem_create(0, TRX_UNDO_INSERT, trx.id, trx.allocator);
    trx_undo_attach_rseg(undo, true, trx.allocator);
    trx.insert_undo = undo.toOpaquePtr();
    return undo;
}

/// Get or create the update undo log for a transaction
pub fn trx_undo_assign_update(trx: *types.trx_t) *trx_undo_t {
    if (trx_undo_t.fromOpaquePtr(trx.update_undo)) |undo| {
        return undo;
    }
    // Create new update undo log
    const undo = trx_undo_mem_create(0, TRX_UNDO_UPDATE, trx.id, trx.allocator);
    trx_undo_attach_rseg(undo, false, trx.allocator);
    trx.update_undo = undo.toOpaquePtr();
    return undo;
}

/// Report a row operation - creates undo record and appends to appropriate log
/// Returns the undo_no assigned to this operation
/// table_id is for future use in full UndoRec serialization
pub fn trx_undo_report_row_operation(
    trx: *types.trx_t,
    rec_type: UndoRecType,
    _: dulint, // table_id - reserved for full undo record encoding
    pk_data: []const u8,
) undo_no_t {
    const undo_no = trx.nextUndoNo();
    const is_insert = (rec_type == .insert);

    // Get appropriate undo log
    const undo = if (is_insert)
        trx_undo_assign_insert(trx)
    else
        trx_undo_assign_update(trx);

    // Create and append the undo record
    const rec = types.trx_undo_record_t{
        .undo_no = undo_no,
        .roll_ptr = trx.makeRollPtr(is_insert, 0, 0), // simplified: page/offset from caller in real impl
        .is_insert = is_insert,
        .data = pk_data,
    };

    trx_undo_append_record(undo, rec);

    return undo_no;
}

/// Free all undo logs for a transaction (called during trx cleanup)
pub fn trx_undo_free_logs(trx: *types.trx_t) void {
    if (trx_undo_t.fromOpaquePtr(trx.insert_undo)) |undo| {
        trx_undo_mem_free(undo);
        trx.insert_undo = null;
    }
    if (trx_undo_t.fromOpaquePtr(trx.update_undo)) |undo| {
        trx_undo_mem_free(undo);
        trx.update_undo = null;
    }
}

/// Check if transaction has any undo logs with records
pub fn trx_undo_has_records(trx: *const types.trx_t) bool {
    if (trx_undo_t.fromOpaquePtr(trx.insert_undo)) |undo| {
        if (!undo.empty) return true;
    }
    if (trx_undo_t.fromOpaquePtr(trx.update_undo)) |undo| {
        if (!undo.empty) return true;
    }
    return false;
}

/// Get total number of undo records across all logs
pub fn trx_undo_record_count(trx: *const types.trx_t) usize {
    var count: usize = 0;
    if (trx_undo_t.fromOpaquePtr(trx.insert_undo)) |undo| {
        count += undo.records.items.len;
    }
    if (trx_undo_t.fromOpaquePtr(trx.update_undo)) |undo| {
        count += undo.records.items.len;
    }
    return count;
}

test "trx_undo_assign_insert creates undo log" {
    var trx = types.trx_t{
        .id = 100,
        .allocator = std.testing.allocator,
    };
    defer trx_undo_free_logs(&trx);

    try std.testing.expect(trx.insert_undo == null);

    const undo = trx_undo_assign_insert(&trx);
    try std.testing.expect(trx.insert_undo != null);
    try std.testing.expectEqual(@as(ulint, TRX_UNDO_INSERT), undo.type);
    try std.testing.expectEqual(@as(trx_id_t, 100), undo.trx_id);

    // Second call returns same undo log
    const undo2 = trx_undo_assign_insert(&trx);
    try std.testing.expectEqual(undo, undo2);
}

test "trx_undo_assign_update creates separate undo log" {
    var trx = types.trx_t{
        .id = 200,
        .allocator = std.testing.allocator,
    };
    defer trx_undo_free_logs(&trx);

    const insert_undo = trx_undo_assign_insert(&trx);
    const update_undo = trx_undo_assign_update(&trx);

    try std.testing.expect(trx.insert_undo != null);
    try std.testing.expect(trx.update_undo != null);
    try std.testing.expect(insert_undo != update_undo);
    try std.testing.expectEqual(@as(ulint, TRX_UNDO_INSERT), insert_undo.type);
    try std.testing.expectEqual(@as(ulint, TRX_UNDO_UPDATE), update_undo.type);
}

test "trx_undo_report_row_operation insert" {
    var trx = types.trx_t{
        .id = 300,
        .allocator = std.testing.allocator,
    };
    defer trx_undo_free_logs(&trx);

    const pk = "pk_value";
    const undo_no = trx_undo_report_row_operation(&trx, .insert, .{ .high = 0, .low = 1 }, pk);

    try std.testing.expectEqual(@as(ulint, 0), undo_no.low);

    const insert_undo = trx_undo_t.fromOpaquePtr(trx.insert_undo).?;
    try std.testing.expectEqual(@as(usize, 1), insert_undo.records.items.len);
    try std.testing.expectEqual(true, insert_undo.records.items[0].is_insert);

    // update_undo should still be null
    try std.testing.expect(trx.update_undo == null);
}

test "trx_undo_report_row_operation update goes to update log" {
    var trx = types.trx_t{
        .id = 400,
        .allocator = std.testing.allocator,
    };
    defer trx_undo_free_logs(&trx);

    const pk = "updated_pk";
    _ = trx_undo_report_row_operation(&trx, .update_exist, .{ .high = 0, .low = 2 }, pk);

    try std.testing.expect(trx.insert_undo == null); // no insert log created
    const update_undo = trx_undo_t.fromOpaquePtr(trx.update_undo).?;
    try std.testing.expectEqual(@as(usize, 1), update_undo.records.items.len);
    try std.testing.expectEqual(false, update_undo.records.items[0].is_insert);
}

test "trx_undo_report_row_operation increments undo_no" {
    var trx = types.trx_t{
        .id = 500,
        .allocator = std.testing.allocator,
    };
    defer trx_undo_free_logs(&trx);

    const undo1 = trx_undo_report_row_operation(&trx, .insert, .{ .high = 0, .low = 1 }, "a");
    const undo2 = trx_undo_report_row_operation(&trx, .update_exist, .{ .high = 0, .low = 1 }, "b");
    const undo3 = trx_undo_report_row_operation(&trx, .del_mark, .{ .high = 0, .low = 1 }, "c");

    try std.testing.expectEqual(@as(ulint, 0), undo1.low);
    try std.testing.expectEqual(@as(ulint, 1), undo2.low);
    try std.testing.expectEqual(@as(ulint, 2), undo3.low);
}

test "trx_undo_has_records and record_count" {
    var trx = types.trx_t{
        .id = 600,
        .allocator = std.testing.allocator,
    };
    defer trx_undo_free_logs(&trx);

    try std.testing.expect(!trx_undo_has_records(&trx));
    try std.testing.expectEqual(@as(usize, 0), trx_undo_record_count(&trx));

    _ = trx_undo_report_row_operation(&trx, .insert, .{ .high = 0, .low = 1 }, "x");
    try std.testing.expect(trx_undo_has_records(&trx));
    try std.testing.expectEqual(@as(usize, 1), trx_undo_record_count(&trx));

    _ = trx_undo_report_row_operation(&trx, .update_exist, .{ .high = 0, .low = 1 }, "y");
    _ = trx_undo_report_row_operation(&trx, .del_mark, .{ .high = 0, .low = 1 }, "z");
    try std.testing.expectEqual(@as(usize, 3), trx_undo_record_count(&trx));
}

test "trx_undo_free_logs cleans up" {
    var trx = types.trx_t{
        .id = 700,
        .allocator = std.testing.allocator,
    };

    _ = trx_undo_report_row_operation(&trx, .insert, .{ .high = 0, .low = 1 }, "data1");
    _ = trx_undo_report_row_operation(&trx, .update_exist, .{ .high = 0, .low = 1 }, "data2");

    try std.testing.expect(trx.insert_undo != null);
    try std.testing.expect(trx.update_undo != null);

    trx_undo_free_logs(&trx);

    try std.testing.expect(trx.insert_undo == null);
    try std.testing.expect(trx.update_undo == null);
}

test "trx undo persists records to undo log page" {
    const fil_sys = @import("../fil/sys.zig");

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);
    const data_home = try std.fmt.allocPrint(std.testing.allocator, "{s}/", .{base});
    defer std.testing.allocator.free(data_home);

    fil.fil_init(0, 32);
    defer fil.fil_close();

    try std.testing.expect(fil_sys.openOrCreateSystemTablespace(data_home, "ibdata1:1M:autoextend") == compat.TRUE);

    rseg.trx_sys_init(std.testing.allocator);
    defer rseg.trx_sys_deinit();

    var slot: ulint = 0;
    _ = rseg.trx_rseg_header_create(0, 0, 128, &slot);

    var trx = types.trx_t{
        .id = 800,
        .allocator = std.testing.allocator,
    };
    defer trx_undo_free_logs(&trx);

    _ = trx_undo_report_row_operation(&trx, .insert, .{ .high = 0, .low = 1 }, "pk1");
    _ = trx_undo_report_row_operation(&trx, .insert, .{ .high = 0, .low = 1 }, "pk2");

    const insert_undo = trx_undo_t.fromOpaquePtr(trx.insert_undo).?;
    try std.testing.expect(insert_undo.first_page_no != fil.FIL_NULL);

    var page: [compat.UNIV_PAGE_SIZE]byte = undefined;
    try std.testing.expectEqual(
        fil.DB_SUCCESS,
        fil.fil_read_page(insert_undo.space_id, insert_undo.first_page_no, page[0..].ptr),
    );

    const header = undo_page_read_header(page[0..]);
    try std.testing.expectEqual(@as(ulint, 2), header.n_recs);

    const first_off = header.first_rec;
    const data_len = mach.mach_read_from_2(page[0..].ptr + first_off);
    const rec_buf = page[first_off + 2 ..];
    const rec_result = UndoRec.decode(rec_buf, data_len) orelse return error.DecodeFailed;
    try std.testing.expectEqual(UndoRecType.insert, rec_result.rec.header.rec_type);
    try std.testing.expectEqualStrings("pk1", rec_result.rec.data);
}
