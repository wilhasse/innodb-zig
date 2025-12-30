const std = @import("std");
const compat = @import("../ut/compat.zig");
const mem = @import("../mem/mod.zig");

pub const module_name = "data";

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;
pub const byte = compat.byte;
pub const dulint = compat.Dulint;

pub const UNIV_SQL_NULL_U32: u32 = compat.UNIV_SQL_NULL;

pub const mem_heap_t = mem.heap.MemHeap;

pub var data_error: byte = 0;

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
    _ = tuple;
    _ = n;
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
    _ = field;
    return compat.TRUE;
}

pub fn dtuple_check_typed(tuple: *const dtuple_t) ibool {
    _ = tuple;
    return compat.TRUE;
}

pub fn dtuple_check_typed_no_assert(tuple: *const dtuple_t) ibool {
    _ = tuple;
    return compat.TRUE;
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

pub fn dtype_get_sql_null_size(type_: *const dtype_t, comp: ulint) ulint {
    _ = comp;
    return type_.len;
}

fn lenToU32(len: ulint) u32 {
    std.debug.assert(len <= std.math.maxInt(u32));
    return @as(u32, @intCast(len));
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
