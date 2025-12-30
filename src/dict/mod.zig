const std = @import("std");
const compat = @import("../ut/compat.zig");
const errors = @import("../ut/errors.zig");

pub const module_name = "dict";

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;
pub const byte = compat.byte;
pub const dulint = compat.Dulint;

pub const dict_hdr_t = byte;

pub const dict_table_t = struct {
    name: []const u8 = "",
    id: dulint = .{ .high = 0, .low = 0 },
};

pub const dict_index_t = struct {
    name: []const u8 = "",
    id: dulint = .{ .high = 0, .low = 0 },
};

pub const dict_sys_t = struct {
    row_id: dulint = .{ .high = 0, .low = DICT_HDR_FIRST_ID },
    booted: bool = false,
};

pub var dict_sys: dict_sys_t = .{};

pub const DICT_HDR_SPACE: ulint = 0;
pub const DICT_HDR_PAGE_NO: ulint = 7;

pub const DICT_HDR_FIRST_ID: ulint = 10;
pub const DICT_HDR_ROW_ID_WRITE_MARGIN: ulint = 256;

pub const DICT_HDR_ROW_ID: ulint = 0;
pub const DICT_HDR_TABLE_ID: ulint = 8;
pub const DICT_HDR_INDEX_ID: ulint = 16;
pub const DICT_HDR_MIX_ID: ulint = 24;
pub const DICT_HDR_TABLES: ulint = 32;
pub const DICT_HDR_TABLE_IDS: ulint = 36;
pub const DICT_HDR_COLUMNS: ulint = 40;
pub const DICT_HDR_INDEXES: ulint = 44;
pub const DICT_HDR_FIELDS: ulint = 48;
pub const DICT_HDR_FSEG_HEADER: ulint = 56;

pub const DICT_TABLES_ID = dulintCreate(0, 1);
pub const DICT_COLUMNS_ID = dulintCreate(0, 2);
pub const DICT_INDEXES_ID = dulintCreate(0, 3);
pub const DICT_FIELDS_ID = dulintCreate(0, 4);
pub const DICT_TABLE_IDS_ID = dulintCreate(0, 5);
pub const DICT_IBUF_ID_MIN = dulintCreate(0xFFFF_FFFF, 0);

pub const DICT_SYS_INDEXES_PAGE_NO_FIELD: ulint = 8;
pub const DICT_SYS_INDEXES_SPACE_NO_FIELD: ulint = 7;
pub const DICT_SYS_INDEXES_TYPE_FIELD: ulint = 6;
pub const DICT_SYS_INDEXES_NAME_FIELD: ulint = 3;

const DB_SUCCESS_ULINT: ulint = @intFromEnum(errors.DbErr.DB_SUCCESS);

var dict_hdr_buf: [128]byte = [_]byte{0} ** 128;
var dict_hdr_row_id: dulint = dulintCreate(0, DICT_HDR_FIRST_ID);
var dict_hdr_table_id: dulint = dulintCreate(0, DICT_HDR_FIRST_ID);
var dict_hdr_index_id: dulint = dulintCreate(0, DICT_HDR_FIRST_ID);

pub fn dict_hdr_get(mtr: ?*anyopaque) *dict_hdr_t {
    _ = mtr;
    return &dict_hdr_buf[0];
}

pub fn dict_hdr_get_new_id(type_: ulint) dulint {
    if (type_ == DICT_HDR_TABLE_ID) {
        dict_hdr_table_id = dulintAdd(dict_hdr_table_id, 1);
        return dict_hdr_table_id;
    }
    if (type_ == DICT_HDR_INDEX_ID) {
        dict_hdr_index_id = dulintAdd(dict_hdr_index_id, 1);
        return dict_hdr_index_id;
    }
    dict_hdr_row_id = dulintAdd(dict_hdr_row_id, 1);
    return dict_hdr_row_id;
}

pub fn dict_hdr_flush_row_id() void {
    dict_hdr_row_id = dict_sys.row_id;
}

pub fn dict_sys_get_new_row_id() dulint {
    const id = dict_sys.row_id;
    if (dulintGetLow(id) % DICT_HDR_ROW_ID_WRITE_MARGIN == 0) {
        dict_hdr_flush_row_id();
    }
    dict_sys.row_id = dulintAdd(dict_sys.row_id, 1);
    return id;
}

pub fn dict_sys_read_row_id(field: [*]const byte) dulint {
    const high = readU16Be(field);
    const low = readU32Be(field + 2);
    return dulintCreate(high, low);
}

pub fn dict_sys_write_row_id(field: [*]byte, row_id: dulint) void {
    writeU16Be(field, dulintGetHigh(row_id));
    writeU32Be(field + 2, dulintGetLow(row_id));
}

pub fn dict_boot() void {
    dict_sys.row_id = dulintAlignUp(dict_hdr_row_id, DICT_HDR_ROW_ID_WRITE_MARGIN);
    dict_sys.row_id = dulintAdd(dict_sys.row_id, DICT_HDR_ROW_ID_WRITE_MARGIN);
    dict_sys.booted = true;
}

pub fn dict_create() void {
    dict_hdr_create();
    dict_boot();
    dict_insert_initial_data();
}

fn dict_hdr_create() ibool {
    dict_hdr_row_id = dulintCreate(0, DICT_HDR_FIRST_ID);
    dict_hdr_table_id = dulintCreate(0, DICT_HDR_FIRST_ID);
    dict_hdr_index_id = dulintCreate(0, DICT_HDR_FIRST_ID);
    return compat.TRUE;
}

fn dict_insert_initial_data() void {}

fn dulintCreate(high: ulint, low: ulint) dulint {
    return .{ .high = high, .low = low };
}

fn dulintGetHigh(id: dulint) ulint {
    return id.high;
}

fn dulintGetLow(id: dulint) ulint {
    return id.low;
}

fn dulintAdd(id: dulint, add: ulint) dulint {
    const low = id.low + add;
    const carry = if (low < id.low) 1 else 0;
    return .{ .high = id.high + carry, .low = low };
}

fn dulintAlignUp(id: dulint, alignment: ulint) dulint {
    if (alignment == 0) {
        return id;
    }
    const mask = alignment - 1;
    const low = (id.low + mask) & ~mask;
    var high = id.high;
    if (low < id.low) {
        high += 1;
    }
    return .{ .high = high, .low = low };
}

fn writeU16Be(ptr: [*]byte, value: ulint) void {
    ptr[0] = @as(byte, @intCast((value >> 8) & 0xFF));
    ptr[1] = @as(byte, @intCast(value & 0xFF));
}

fn writeU32Be(ptr: [*]byte, value: ulint) void {
    ptr[0] = @as(byte, @intCast((value >> 24) & 0xFF));
    ptr[1] = @as(byte, @intCast((value >> 16) & 0xFF));
    ptr[2] = @as(byte, @intCast((value >> 8) & 0xFF));
    ptr[3] = @as(byte, @intCast(value & 0xFF));
}

fn readU16Be(ptr: [*]const byte) ulint {
    return (@as(ulint, ptr[0]) << 8) | @as(ulint, ptr[1]);
}

fn readU32Be(ptr: [*]const byte) ulint {
    return (@as(ulint, ptr[0]) << 24) |
        (@as(ulint, ptr[1]) << 16) |
        (@as(ulint, ptr[2]) << 8) |
        @as(ulint, ptr[3]);
}

test "dict header id allocation" {
    dict_hdr_create();
    const id1 = dict_hdr_get_new_id(DICT_HDR_TABLE_ID);
    const id2 = dict_hdr_get_new_id(DICT_HDR_TABLE_ID);
    try std.testing.expect(dulintGetLow(id2) == dulintGetLow(id1) + 1);

    const idx1 = dict_hdr_get_new_id(DICT_HDR_INDEX_ID);
    try std.testing.expect(dulintGetLow(idx1) >= DICT_HDR_FIRST_ID);
}

test "dict row id read/write" {
    var buf: [6]byte = undefined;
    const id = dulintCreate(0x12, 0x3456789a);
    dict_sys_write_row_id(&buf, id);
    const out = dict_sys_read_row_id(&buf);
    try std.testing.expectEqual(id.high, out.high);
    try std.testing.expectEqual(id.low, out.low);
}

test "dict boot updates row id" {
    dict_hdr_create();
    dict_sys.row_id = dulintCreate(0, 1);
    dict_boot();
    try std.testing.expect(dict_sys.booted);
    try std.testing.expect(dulintGetLow(dict_sys.row_id) > 0);
}
