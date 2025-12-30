const std = @import("std");
const compat = @import("../ut/compat.zig");
const mem = @import("../mem/mod.zig");
const api = @import("../api/mod.zig").impl;

pub const module_name = "data";

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;
pub const byte = compat.byte;
pub const dulint = compat.Dulint;

pub const UNIV_SQL_NULL_U32: u32 = compat.UNIV_SQL_NULL;

pub const mem_heap_t = mem.heap.MemHeap;

pub var data_error: byte = 0;
pub var data_client_default_charset_coll: ulint = 0;

pub const DATA_CLIENT_LATIN1_SWEDISH_CHARSET_COLL: ulint = 8;
pub const DATA_CLIENT_BINARY_CHARSET_COLL: ulint = 63;

pub const DATA_VARCHAR: ulint = 1;
pub const DATA_CHAR: ulint = 2;
pub const DATA_FIXBINARY: ulint = 3;
pub const DATA_BINARY: ulint = 4;
pub const DATA_BLOB: ulint = 5;
pub const DATA_INT: ulint = 6;
pub const DATA_SYS_CHILD: ulint = 7;
pub const DATA_SYS: ulint = 8;
pub const DATA_FLOAT: ulint = 9;
pub const DATA_DOUBLE: ulint = 10;
pub const DATA_DECIMAL: ulint = 11;
pub const DATA_VARCLIENT: ulint = 12;
pub const DATA_CLIENT: ulint = 13;
pub const DATA_MTYPE_MAX: ulint = 63;

pub const DATA_ENGLISH: ulint = 4;
pub const DATA_ERROR: ulint = 111;
pub const DATA_CLIENT_TYPE_MASK: ulint = 0xFF;

pub const DATA_ROW_ID: ulint = 0;
pub const DATA_ROW_ID_LEN: ulint = 6;
pub const DATA_TRX_ID: ulint = 1;
pub const DATA_TRX_ID_LEN: ulint = 6;
pub const DATA_ROLL_PTR: ulint = 2;
pub const DATA_ROLL_PTR_LEN: ulint = 7;
pub const DATA_N_SYS_COLS: ulint = 3;

pub const DATA_SYS_PRTYPE_MASK: ulint = 0xF;

pub const DATA_NOT_NULL: ulint = 256;
pub const DATA_UNSIGNED: ulint = 512;
pub const DATA_BINARY_TYPE: ulint = 1024;
pub const DATA_CUSTOM_TYPE: ulint = 2048;

pub const DATA_ORDER_NULL_TYPE_BUF_SIZE: ulint = 4;
pub const DATA_NEW_ORDER_NULL_TYPE_BUF_SIZE: ulint = 6;

pub const dtype_t = struct {
    mtype: ulint = 0,
    prtype: ulint = 0,
    len: ulint = 0,
    mbminlen: ulint = 0,
    mbmaxlen: ulint = 0,
};

pub const dfield_t = struct {
    data: ?*anyopaque = null,
    ext: bool = false,
    len: u32 = UNIV_SQL_NULL_U32,
    type: dtype_t = .{},
};

pub const dtuple_t = struct {
    info_bits: ulint = 0,
    n_fields: ulint = 0,
    n_fields_cmp: ulint = 0,
    fields: []dfield_t = undefined,
    magic_n: ulint = 0,
};

pub const big_rec_field_t = struct {
    field_no: ulint = 0,
    len: ulint = 0,
    data: ?*const anyopaque = null,
};

pub const big_rec_t = struct {
    heap: ?*mem_heap_t = null,
    n_fields: ulint = 0,
    fields: []big_rec_field_t = undefined,
};

pub fn dfield_var_init() void {
    data_error = 0;
}

pub fn dfield_get_type(field: *const dfield_t) *const dtype_t {
    return &field.type;
}

pub fn dfield_get_data(field: *const dfield_t) ?*const anyopaque {
    return field.data;
}

pub fn dfield_set_type(field: *dfield_t, type_: *const dtype_t) void {
    field.type = type_.*;
}

pub fn dfield_get_len(field: *const dfield_t) ulint {
    return @as(ulint, field.len);
}

pub fn dfield_set_len(field: *dfield_t, len: ulint) void {
    field.ext = false;
    field.len = lenToU32(len);
}

pub fn dfield_is_null(field: *const dfield_t) ibool {
    return if (field.len == UNIV_SQL_NULL_U32) compat.TRUE else compat.FALSE;
}

pub fn dfield_is_ext(field: *const dfield_t) ibool {
    return if (field.ext) compat.TRUE else compat.FALSE;
}

pub fn dfield_set_ext(field: *dfield_t) void {
    field.ext = true;
}

pub fn dfield_set_data(field: *dfield_t, data: ?*const anyopaque, len: ulint) void {
    field.data = if (data) |ptr| @constCast(ptr) else null;
    field.ext = false;
    field.len = lenToU32(len);
}

pub fn dfield_set_null(field: *dfield_t) void {
    dfield_set_data(field, null, compat.UNIV_SQL_NULL);
}

pub fn dfield_copy_data(field1: *dfield_t, field2: *const dfield_t) void {
    field1.data = field2.data;
    field1.len = field2.len;
    field1.ext = field2.ext;
}

pub fn dfield_copy(field1: *dfield_t, field2: *const dfield_t) void {
    field1.* = field2.*;
}

pub fn dfield_dup(field: *dfield_t, heap: *mem_heap_t) void {
    if (dfield_is_null(field) == compat.TRUE) {
        return;
    }
    const len = @as(usize, @intCast(dfield_get_len(field)));
    if (len == 0) {
        return;
    }
    const src = fieldDataSlice(field, len);
    const buf = heap.alloc(len) catch return;
    std.mem.copyForwards(u8, buf, src);
    field.data = @ptrCast(buf.ptr);
}

pub fn dfield_datas_are_binary_equal(field1: *const dfield_t, field2: *const dfield_t) ibool {
    const len1 = dfield_get_len(field1);
    if (len1 != dfield_get_len(field2)) {
        return compat.FALSE;
    }
    if (len1 == compat.UNIV_SQL_NULL) {
        return compat.TRUE;
    }
    const len = @as(usize, @intCast(len1));
    const data1 = fieldDataSlice(field1, len);
    const data2 = fieldDataSlice(field2, len);
    return if (std.mem.eql(u8, data1, data2)) compat.TRUE else compat.FALSE;
}

pub fn dfield_data_is_binary_equal(field: *const dfield_t, len: ulint, data: [*]const byte) ibool {
    if (len != dfield_get_len(field)) {
        return compat.FALSE;
    }
    if (len == compat.UNIV_SQL_NULL) {
        return compat.TRUE;
    }
    const size = @as(usize, @intCast(len));
    const field_data = fieldDataSlice(field, size);
    const input = data[0..size];
    return if (std.mem.eql(u8, field_data, input)) compat.TRUE else compat.FALSE;
}

pub fn dtuple_get_n_fields(tuple: *const dtuple_t) ulint {
    return tuple.n_fields;
}

pub fn dtuple_get_nth_field(tuple: *const dtuple_t, n: ulint) *dfield_t {
    std.debug.assert(n < tuple.n_fields);
    return &tuple.fields[@as(usize, @intCast(n))];
}

pub fn dtuple_get_info_bits(tuple: *const dtuple_t) ulint {
    return tuple.info_bits;
}

pub fn dtuple_set_info_bits(tuple: *dtuple_t, info_bits: ulint) void {
    tuple.info_bits = info_bits;
}

pub fn dtuple_get_n_fields_cmp(tuple: *const dtuple_t) ulint {
    return tuple.n_fields_cmp;
}

pub fn dtuple_set_n_fields_cmp(tuple: *dtuple_t, n_fields_cmp: ulint) void {
    tuple.n_fields_cmp = n_fields_cmp;
}

pub fn dtuple_set_n_fields(tuple: *dtuple_t, n_fields: ulint) void {
    tuple.n_fields = n_fields;
    tuple.n_fields_cmp = n_fields;
}

pub fn dtuple_create(heap: *mem_heap_t, n_fields: ulint) ?*dtuple_t {
    const tuple_buf = heap.alloc(@sizeOf(dtuple_t)) catch return null;
    const tuple = @as(*dtuple_t, @ptrCast(@alignCast(tuple_buf.ptr)));
    tuple.info_bits = 0;
    tuple.n_fields = n_fields;
    tuple.n_fields_cmp = n_fields;
    tuple.magic_n = 0;

    if (n_fields == 0) {
        tuple.fields = &[_]dfield_t{};
        return tuple;
    }

    const fields_len = @as(usize, @intCast(n_fields)) * @sizeOf(dfield_t);
    const fields_buf = heap.alloc(fields_len) catch return null;
    const fields_ptr = @as([*]dfield_t, @ptrCast(@alignCast(fields_buf.ptr)));
    tuple.fields = fields_ptr[0..@as(usize, @intCast(n_fields))];
    for (tuple.fields) |*field| {
        field.* = dfield_t{};
    }
    return tuple;
}

pub fn dtuple_from_fields(tuple: *dtuple_t, fields: [*]const dfield_t, n_fields: ulint) *const dtuple_t {
    tuple.info_bits = 0;
    tuple.n_fields = n_fields;
    tuple.n_fields_cmp = n_fields;
    tuple.fields = @constCast(fields[0..@as(usize, @intCast(n_fields))]);
    tuple.magic_n = 0;
    return tuple;
}

pub fn dtuple_copy(tuple: *const dtuple_t, heap: *mem_heap_t) ?*dtuple_t {
    const n_fields = dtuple_get_n_fields(tuple);
    const copy = dtuple_create(heap, n_fields) orelse return null;
    for (tuple.fields, 0..) |*field, idx| {
        dfield_copy(&copy.fields[idx], field);
    }
    return copy;
}

pub fn dtuple_get_data_size(tuple: *const dtuple_t, comp: ulint) ulint {
    var sum: ulint = 0;
    for (tuple.fields) |*field| {
        var len = dfield_get_len(field);
        if (len == compat.UNIV_SQL_NULL) {
            len = dtype_get_sql_null_size(&field.type, comp);
        }
        sum += len;
    }
    return sum;
}

pub fn dtuple_get_n_ext(tuple: *const dtuple_t) ulint {
    var n_ext: ulint = 0;
    for (tuple.fields) |*field| {
        if (field.ext) {
            n_ext += 1;
        }
    }
    return n_ext;
}

pub fn dtuple_coll_cmp(cmp_ctx: ?*anyopaque, tuple1: *const dtuple_t, tuple2: *const dtuple_t) i32 {
    _ = cmp_ctx;
    const n1 = dtuple_get_n_fields(tuple1);
    const n2 = dtuple_get_n_fields(tuple2);
    if (n1 != n2) {
        return if (n1 < n2) -1 else 1;
    }
    for (tuple1.fields, 0..) |*field1, idx| {
        const field2 = &tuple2.fields[idx];
        const cmp = dfield_compare_binary(field1, field2);
        if (cmp != 0) {
            return cmp;
        }
    }
    return 0;
}

pub fn dtuple_fold(tuple: *const dtuple_t, n_fields: ulint, n_bytes: ulint, tree_id: dulint) ulint {
    var fold = foldDulint(tree_id);
    const max_fields = @min(n_fields, tuple.fields.len);
    for (tuple.fields[0..max_fields]) |*field| {
        const len = dfield_get_len(field);
        if (len == compat.UNIV_SQL_NULL) {
            continue;
        }
        const size = @as(usize, @intCast(len));
        const data = fieldDataSlice(field, size);
        fold = foldUlintPair(fold, foldBinary(data));
    }

    if (n_bytes > 0 and max_fields < tuple.fields.len) {
        const field = &tuple.fields[max_fields];
        const len = dfield_get_len(field);
        if (len != compat.UNIV_SQL_NULL) {
            var size = @as(usize, @intCast(len));
            const max_bytes = @as(usize, @intCast(n_bytes));
            if (size > max_bytes) {
                size = max_bytes;
            }
            const data = fieldDataSlice(field, size);
            fold = foldUlintPair(fold, foldBinary(data));
        }
    }
    return fold;
}

pub fn dtuple_set_types_binary(tuple: *dtuple_t, n: ulint) void {
    const limit = @min(@as(usize, @intCast(n)), tuple.fields.len);
    for (tuple.fields[0..limit]) |*field| {
        dtype_set(&field.type, DATA_BINARY, 0, 0);
    }
}

pub fn dtuple_contains_null(tuple: *const dtuple_t) ibool {
    for (tuple.fields) |*field| {
        if (dfield_is_null(field) == compat.TRUE) {
            return compat.TRUE;
        }
    }
    return compat.FALSE;
}

pub fn dfield_check_typed(field: *const dfield_t) ibool {
    const mtype = field.type.mtype;
    if (mtype < DATA_VARCHAR or mtype > DATA_CLIENT) {
        return compat.FALSE;
    }
    return compat.TRUE;
}

pub fn dtuple_check_typed(tuple: *const dtuple_t) ibool {
    for (tuple.fields) |*field| {
        if (dfield_check_typed(field) == compat.FALSE) {
            return compat.FALSE;
        }
    }
    return compat.TRUE;
}

pub fn dtuple_check_typed_no_assert(tuple: *const dtuple_t) ibool {
    return dtuple_check_typed(tuple);
}

pub fn dtuple_validate(tuple: *const dtuple_t) ibool {
    for (tuple.fields) |*field| {
        if (dfield_is_null(field) == compat.FALSE and field.data == null) {
            return compat.FALSE;
        }
    }
    return compat.TRUE;
}

pub fn dfield_print(dfield: *const dfield_t) void {
    _ = dfield;
}

pub fn dfield_print_also_hex(dfield: *const dfield_t) void {
    _ = dfield;
}

pub fn dtuple_print(tuple: *const dtuple_t) void {
    _ = tuple;
}

pub fn dtuple_convert_big_rec(index: ?*anyopaque, entry: *dtuple_t, n_ext: *ulint) ?*big_rec_t {
    _ = index;
    _ = entry;
    _ = n_ext;
    return null;
}

pub fn dtuple_convert_back_big_rec(index: ?*anyopaque, entry: *dtuple_t, vector: *big_rec_t) void {
    _ = index;
    _ = entry;
    _ = vector;
}

pub fn dtuple_big_rec_free(vector: *big_rec_t) void {
    _ = vector;
}

pub fn data_write_sql_null(data: []byte) void {
    std.mem.set(u8, data, 0);
}

pub fn dtype_var_init() void {
    data_client_default_charset_coll = 0;
}

pub fn dtype_get_at_most_n_mbchars(
    prtype: ulint,
    mbminlen: ulint,
    mbmaxlen: ulint,
    prefix_len: ulint,
    data_len: ulint,
    str: [*]const u8,
) ulint {
    if (mbminlen != mbmaxlen) {
        const cs = api.ib_ucode_get_charset(dtype_get_charset_coll(prtype));
        const slice = str[0..@as(usize, @intCast(data_len))];
        return api.ib_ucode_get_storage_size(cs, prefix_len, data_len, slice);
    }
    if (prefix_len < data_len) {
        return prefix_len;
    }
    return data_len;
}

pub fn dtype_is_string_type(mtype: ulint) ibool {
    if (mtype <= DATA_BLOB or mtype == DATA_CLIENT or mtype == DATA_VARCLIENT) {
        return compat.TRUE;
    }
    return compat.FALSE;
}

pub fn dtype_is_binary_string_type(mtype: ulint, prtype: ulint) ibool {
    if (mtype == DATA_FIXBINARY or mtype == DATA_BINARY) {
        return compat.TRUE;
    }
    if (mtype == DATA_BLOB and (prtype & DATA_BINARY_TYPE) != 0) {
        return compat.TRUE;
    }
    return compat.FALSE;
}

pub fn dtype_is_non_binary_string_type(mtype: ulint, prtype: ulint) ibool {
    if (dtype_is_string_type(mtype) == compat.TRUE and dtype_is_binary_string_type(mtype, prtype) == compat.FALSE) {
        return compat.TRUE;
    }
    return compat.FALSE;
}

pub fn dtype_set(type_: *dtype_t, mtype: ulint, prtype: ulint, len: ulint) void {
    type_.mtype = mtype;
    type_.prtype = prtype;
    type_.len = len;
    dtype_set_mblen(type_);
}

pub fn dtype_copy(type1: *dtype_t, type2: *const dtype_t) void {
    type1.* = type2.*;
}

pub fn dtype_get_mtype(type_: *const dtype_t) ulint {
    return type_.mtype;
}

pub fn dtype_get_prtype(type_: *const dtype_t) ulint {
    return type_.prtype;
}

pub fn dtype_get_mblen(mtype: ulint, prtype: ulint, mbminlen: *ulint, mbmaxlen: *ulint) void {
    if (dtype_is_string_type(mtype) == compat.TRUE) {
        const cs = api.ib_ucode_get_charset(dtype_get_charset_coll(prtype));
        api.ib_ucode_get_charset_width(cs, mbminlen, mbmaxlen);
    } else {
        mbminlen.* = 0;
        mbmaxlen.* = 0;
    }
}

pub fn dtype_get_charset_coll(prtype: ulint) ulint {
    return (prtype >> 16) & 0xFF;
}

pub fn dtype_form_prtype(old_prtype: ulint, charset_coll: ulint) ulint {
    return old_prtype + (charset_coll << 16);
}

pub fn dtype_get_len(type_: *const dtype_t) ulint {
    return type_.len;
}

pub fn dtype_get_mbminlen(type_: *const dtype_t) ulint {
    return type_.mbminlen;
}

pub fn dtype_get_mbmaxlen(type_: *const dtype_t) ulint {
    return type_.mbmaxlen;
}

pub fn dtype_get_pad_char(mtype: ulint, prtype: ulint) ulint {
    switch (mtype) {
        DATA_FIXBINARY, DATA_BINARY => {
            if (dtype_get_charset_coll(prtype) == DATA_CLIENT_BINARY_CHARSET_COLL) {
                return compat.ULINT_UNDEFINED;
            }
        },
        DATA_CHAR, DATA_VARCHAR, DATA_CLIENT, DATA_VARCLIENT => {
            return 0x20;
        },
        DATA_BLOB => {
            if ((prtype & DATA_BINARY_TYPE) == 0) {
                return 0x20;
            }
        },
        else => {},
    }
    return compat.ULINT_UNDEFINED;
}

pub fn dtype_get_fixed_size_low(
    mtype: ulint,
    prtype: ulint,
    len: ulint,
    mbminlen: ulint,
    mbmaxlen: ulint,
    comp: ulint,
) ulint {
    _ = comp;
    switch (mtype) {
        DATA_SYS => {
            const sys_prtype = prtype & DATA_CLIENT_TYPE_MASK;
            if (sys_prtype == DATA_ROW_ID or sys_prtype == DATA_TRX_ID or sys_prtype == DATA_ROLL_PTR) {
                return len;
            }
            return 0;
        },
        DATA_CHAR, DATA_FIXBINARY, DATA_INT, DATA_FLOAT, DATA_DOUBLE => return len,
        DATA_CLIENT => {
            if ((prtype & DATA_BINARY_TYPE) != 0 or mbminlen == mbmaxlen) {
                return len;
            }
            return 0;
        },
        DATA_VARCHAR, DATA_BINARY, DATA_DECIMAL, DATA_VARCLIENT, DATA_BLOB => return 0,
        else => return 0,
    }
}

pub fn dtype_get_min_size_low(
    mtype: ulint,
    prtype: ulint,
    len: ulint,
    mbminlen: ulint,
    mbmaxlen: ulint,
) ulint {
    switch (mtype) {
        DATA_SYS => {
            const sys_prtype = prtype & DATA_CLIENT_TYPE_MASK;
            if (sys_prtype == DATA_ROW_ID or sys_prtype == DATA_TRX_ID or sys_prtype == DATA_ROLL_PTR) {
                return len;
            }
            return 0;
        },
        DATA_CHAR, DATA_FIXBINARY, DATA_INT, DATA_FLOAT, DATA_DOUBLE => return len,
        DATA_CLIENT => {
            if ((prtype & DATA_BINARY_TYPE) != 0 or mbminlen == mbmaxlen) {
                return len;
            }
            if (mbmaxlen == 0) {
                return 0;
            }
            return len * mbminlen / mbmaxlen;
        },
        DATA_VARCHAR, DATA_BINARY, DATA_DECIMAL, DATA_VARCLIENT, DATA_BLOB => return 0,
        else => return 0,
    }
}

pub fn dtype_get_max_size_low(mtype: ulint, len: ulint) ulint {
    switch (mtype) {
        DATA_SYS,
        DATA_CHAR,
        DATA_FIXBINARY,
        DATA_INT,
        DATA_FLOAT,
        DATA_DOUBLE,
        DATA_CLIENT,
        DATA_VARCHAR,
        DATA_BINARY,
        DATA_DECIMAL,
        DATA_VARCLIENT,
        => return len,
        DATA_BLOB => return compat.ULINT_MAX,
        else => return compat.ULINT_MAX,
    }
}

pub fn dtype_get_sql_null_size(type_: *const dtype_t, comp: ulint) ulint {
    return dtype_get_fixed_size_low(type_.mtype, type_.prtype, type_.len, type_.mbminlen, type_.mbmaxlen, comp);
}

pub fn dtype_read_for_order_and_null_size(type_: *dtype_t, buf: []const byte) void {
    type_.mtype = buf[0] & 63;
    type_.prtype = buf[1];
    if ((buf[0] & 128) != 0) {
        type_.prtype |= DATA_BINARY_TYPE;
    }
    type_.len = readU16Be(buf[2..].ptr);
    type_.prtype = dtype_form_prtype(type_.prtype, data_client_default_charset_coll);
    dtype_set_mblen(type_);
}

pub fn dtype_new_store_for_order_and_null_size(buf: []byte, type_: *const dtype_t, prefix_len: ulint) void {
    var flags: byte = @as(byte, @intCast(type_.mtype & 0xFF));
    if ((type_.prtype & DATA_BINARY_TYPE) != 0) {
        flags |= 0x80;
    }
    buf[0] = flags;
    buf[1] = @as(byte, @intCast(type_.prtype & 0xFF));

    const len = if (prefix_len != 0) prefix_len else type_.len;
    writeU16Be(buf[2..].ptr, len & 0xFFFF);

    const charset_coll = dtype_get_charset_coll(type_.prtype) & 0xFF;
    writeU16Be(buf[4..].ptr, charset_coll);
    if ((type_.prtype & DATA_NOT_NULL) != 0) {
        buf[4] |= 0x80;
    }
}

pub fn dtype_new_read_for_order_and_null_size(type_: *dtype_t, buf: []const byte) void {
    type_.mtype = buf[0] & 63;
    type_.prtype = buf[1];
    if ((buf[0] & 128) != 0) {
        type_.prtype |= DATA_BINARY_TYPE;
    }
    if ((buf[4] & 128) != 0) {
        type_.prtype |= DATA_NOT_NULL;
    }
    type_.len = readU16Be(buf[2..].ptr);

    const charset_coll = readU16Be(buf[4..].ptr) & 0x7FFF;
    if (dtype_is_string_type(type_.mtype) == compat.TRUE) {
        type_.prtype = dtype_form_prtype(type_.prtype, charset_coll);
    }
    dtype_set_mblen(type_);
}

pub fn dtype_validate(type_: *const dtype_t) ibool {
    if (type_.mtype < DATA_VARCHAR or type_.mtype > DATA_CLIENT) {
        return compat.FALSE;
    }
    if (type_.mtype == DATA_SYS and (type_.prtype & DATA_CLIENT_TYPE_MASK) >= DATA_N_SYS_COLS) {
        return compat.FALSE;
    }
    if (type_.mbminlen > type_.mbmaxlen) {
        return compat.FALSE;
    }
    return compat.TRUE;
}

pub fn dtype_print(type_: *const dtype_t) void {
    _ = type_;
}

pub fn dtype_get_attrib(type_: *const dtype_t) ulint {
    return type_.prtype & DATA_CLIENT_TYPE_MASK;
}

fn lenToU32(len: ulint) u32 {
    std.debug.assert(len <= std.math.maxInt(u32));
    return @as(u32, @intCast(len));
}

fn dtype_set_mblen(type_: *dtype_t) void {
    var mbmin: ulint = 0;
    var mbmax: ulint = 0;
    dtype_get_mblen(type_.mtype, type_.prtype, &mbmin, &mbmax);
    type_.mbminlen = mbmin;
    type_.mbmaxlen = mbmax;
}

fn writeU16Be(ptr: [*]byte, value: ulint) void {
    std.debug.assert(value <= 0xFFFF);
    ptr[0] = @as(byte, @intCast((value >> 8) & 0xFF));
    ptr[1] = @as(byte, @intCast(value & 0xFF));
}

fn readU16Be(ptr: [*]const byte) ulint {
    return (@as(ulint, ptr[0]) << 8) | @as(ulint, ptr[1]);
}

fn fieldDataSlice(field: *const dfield_t, len: usize) []const u8 {
    if (field.data) |ptr| {
        return @as([*]const u8, @ptrCast(ptr))[0..len];
    }
    return &[_]u8{};
}

fn dfield_compare_binary(field1: *const dfield_t, field2: *const dfield_t) i32 {
    const len1 = dfield_get_len(field1);
    const len2 = dfield_get_len(field2);
    if (len1 == compat.UNIV_SQL_NULL and len2 == compat.UNIV_SQL_NULL) {
        return 0;
    }
    if (len1 == compat.UNIV_SQL_NULL) {
        return -1;
    }
    if (len2 == compat.UNIV_SQL_NULL) {
        return 1;
    }
    const n1 = @as(usize, @intCast(len1));
    const n2 = @as(usize, @intCast(len2));
    const min_len = @min(n1, n2);
    const data1 = fieldDataSlice(field1, min_len);
    const data2 = fieldDataSlice(field2, min_len);
    switch (std.mem.order(u8, data1, data2)) {
        .lt => return -1,
        .gt => return 1,
        .eq => {},
    }
    if (n1 < n2) {
        return -1;
    }
    if (n1 > n2) {
        return 1;
    }
    return 0;
}

fn foldDulint(id: dulint) ulint {
    return id.high ^ id.low;
}

fn foldUlintPair(a: ulint, b: ulint) ulint {
    return a ^ (b +% 0x9e3779b97f4a7c15);
}

fn foldBinary(data: []const u8) ulint {
    var hash: u64 = 14695981039346656037;
    for (data) |value| {
        hash = (hash ^ value) *% 1099511628211;
    }
    return @as(ulint, @intCast(hash));
}

test "data dfield basics" {
    var field = dfield_t{};
    const value = [_]byte{ 1, 2, 3 };

    dfield_set_data(&field, value[0..].ptr, value.len);
    try std.testing.expectEqual(@as(ulint, 3), dfield_get_len(&field));
    try std.testing.expectEqual(compat.FALSE, dfield_is_null(&field));

    var other = dfield_t{};
    dfield_copy_data(&other, &field);
    try std.testing.expectEqual(compat.TRUE, dfield_datas_are_binary_equal(&field, &other));

    dfield_set_null(&other);
    try std.testing.expectEqual(compat.TRUE, dfield_is_null(&other));
}

test "data dtuple create and copy" {
    var heap = try mem.heap.MemHeap.init(std.testing.allocator, 0, .dynamic);
    defer heap.deinit();

    const tuple = dtuple_create(&heap, 2) orelse return error.OutOfMemory;
    const values = [_]byte{ 'a', 'b' };
    dfield_set_data(&tuple.fields[0], values[0..1].ptr, 1);
    dfield_set_data(&tuple.fields[1], values[1..2].ptr, 1);

    try std.testing.expectEqual(@as(ulint, 2), dtuple_get_n_fields(tuple));
    try std.testing.expectEqual(@as(ulint, 0), dtuple_get_n_ext(tuple));
    try std.testing.expectEqual(@as(ulint, 2), dtuple_get_data_size(tuple, 0));

    const copy = dtuple_copy(tuple, &heap) orelse return error.OutOfMemory;
    try std.testing.expectEqual(@as(ulint, 2), dtuple_get_n_fields(copy));
    try std.testing.expectEqual(compat.TRUE, dfield_datas_are_binary_equal(&tuple.fields[0], &copy.fields[0]));
}

test "data dtuple compare and fold" {
    var heap = try mem.heap.MemHeap.init(std.testing.allocator, 0, .dynamic);
    defer heap.deinit();

    const tuple1 = dtuple_create(&heap, 1) orelse return error.OutOfMemory;
    const tuple2 = dtuple_create(&heap, 1) orelse return error.OutOfMemory;

    const a = [_]byte{'a'};
    const b = [_]byte{'b'};
    dfield_set_data(&tuple1.fields[0], a[0..].ptr, 1);
    dfield_set_data(&tuple2.fields[0], b[0..].ptr, 1);

    try std.testing.expectEqual(@as(i32, -1), dtuple_coll_cmp(null, tuple1, tuple2));

    const tree_id = dulint{ .high = 1, .low = 2 };
    const fold = dtuple_fold(tuple1, 0, 0, tree_id);
    try std.testing.expectEqual(foldDulint(tree_id), fold);
}

test "data sql null write" {
    var buf = [_]byte{ 1, 2, 3 };
    data_write_sql_null(buf[0..]);
    try std.testing.expectEqual(@as(byte, 0), buf[0]);
    try std.testing.expectEqual(@as(byte, 0), buf[1]);
    try std.testing.expectEqual(@as(byte, 0), buf[2]);
}

test "data dtype basics" {
    var dtype = dtype_t{};
    dtype_set(&dtype, DATA_INT, DATA_UNSIGNED, 4);
    try std.testing.expectEqual(@as(ulint, DATA_INT), dtype_get_mtype(&dtype));
    try std.testing.expectEqual(@as(ulint, DATA_UNSIGNED), dtype_get_prtype(&dtype));
    try std.testing.expectEqual(@as(ulint, 4), dtype_get_len(&dtype));
    try std.testing.expectEqual(compat.TRUE, dtype_is_string_type(DATA_VARCHAR));
    try std.testing.expectEqual(compat.TRUE, dtype_is_binary_string_type(DATA_BLOB, DATA_BINARY_TYPE));
    try std.testing.expectEqual(compat.TRUE, dtype_is_non_binary_string_type(DATA_BLOB, 0));

    var field = dfield_t{};
    dtype_set(&field.type, DATA_INT, 0, 4);
    try std.testing.expectEqual(compat.TRUE, dfield_check_typed(&field));
    try std.testing.expectEqual(@as(ulint, 4), dtype_get_sql_null_size(&field.type, 0));
}

test "data dtype order buffer roundtrip" {
    var dtype = dtype_t{};
    const prtype = dtype_form_prtype(DATA_NOT_NULL, 33);
    dtype_set(&dtype, DATA_VARCHAR, prtype, 12);

    var buf: [DATA_NEW_ORDER_NULL_TYPE_BUF_SIZE]byte = undefined;
    dtype_new_store_for_order_and_null_size(buf[0..], &dtype, 0);

    var read = dtype_t{};
    dtype_new_read_for_order_and_null_size(&read, buf[0..]);
    try std.testing.expectEqual(dtype.mtype, read.mtype);
    try std.testing.expectEqual(dtype.len, read.len);
    try std.testing.expectEqual(@as(ulint, 33), dtype_get_charset_coll(read.prtype));
}

test "data dtype old order buffer" {
    data_client_default_charset_coll = 8;
    var buf: [DATA_ORDER_NULL_TYPE_BUF_SIZE]byte = undefined;
    buf[0] = @as(byte, @intCast(DATA_CHAR));
    buf[1] = 0;
    writeU16Be(buf[2..].ptr, 5);

    var dtype = dtype_t{};
    dtype_read_for_order_and_null_size(&dtype, buf[0..]);
    try std.testing.expectEqual(@as(ulint, DATA_CHAR), dtype.mtype);
    try std.testing.expectEqual(@as(ulint, 5), dtype.len);
    try std.testing.expectEqual(@as(ulint, 8), dtype_get_charset_coll(dtype.prtype));
}
