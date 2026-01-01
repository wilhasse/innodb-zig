const std = @import("std");
const compat = @import("../ut/compat.zig");
const data = @import("../data/mod.zig");
const dict = @import("mod.zig");
const btr = @import("../btr/mod.zig");
const fil = @import("../fil/mod.zig");

pub const module_name = "dict.sys_btr";

const ColumnSpec = struct {
    name: []const u8,
    mtype: dict.ulint,
    prtype: dict.ulint,
    len: dict.ulint,
};

const SysBtrState = struct {
    tables_table: *dict.dict_table_t,
    tables_index: *dict.dict_index_t,
    columns_table: *dict.dict_table_t,
    columns_index: *dict.dict_index_t,
    indexes_table: *dict.dict_table_t,
    indexes_index: *dict.dict_index_t,
    allocator: std.mem.Allocator,
};

var sys_btr: ?*SysBtrState = null;

const SYS_TABLES_FIXED_LEN: usize = 8 + 4 + 4 + 4;
const SYS_COLUMNS_FIXED_LEN: usize = 8 + 8 + 4 + 4 + 4 + 4;
const SYS_INDEXES_FIXED_LEN: usize = 8 + 8 + 4 + 4;

fn dulintToU64(id: dict.dulint) u64 {
    return (@as(u64, id.high) << 32) | id.low;
}

fn dulintFromU64(val: u64) dict.dulint {
    return .{
        .high = @as(dict.ulint, @intCast(val >> 32)),
        .low = @as(dict.ulint, @intCast(val & 0xFFFF_FFFF)),
    };
}

fn recDataSlice(rec: *btr.rec_t) ?[]const u8 {
    const rec_bytes = rec.rec_bytes orelse return null;
    if (rec.rec_offset == 0 or rec.rec_offset > rec_bytes.len) {
        return null;
    }
    return rec_bytes[@as(usize, @intCast(rec.rec_offset))..];
}

fn readU64Slice(bytes: []const u8) u64 {
    return std.mem.readInt(u64, @as(*const [8]u8, @ptrCast(bytes.ptr)), .little);
}

fn readU32Slice(bytes: []const u8) u32 {
    return std.mem.readInt(u32, @as(*const [4]u8, @ptrCast(bytes.ptr)), .little);
}

fn dtupleKey(tuple: *const data.dtuple_t) i64 {
    if (tuple.n_fields == 0) {
        return 0;
    }
    const field = &tuple.fields[0];
    const ptr = data.dfield_get_data(field) orelse return 0;
    const len = data.dfield_get_len(field);
    const bytes = @as([*]const u8, @ptrCast(ptr));
    return switch (len) {
        4 => @as(i64, @intCast(std.mem.readInt(i32, bytes[0..4], .little))),
        8 => std.mem.readInt(i64, bytes[0..8], .little),
        else => 0,
    };
}

fn btrInsert(index: *dict.dict_index_t, tuple: *data.dtuple_t) void {
    const key = dtupleKey(tuple);
    const block = btr.btr_find_leaf_for_key(index, key) orelse return;
    var cursor = btr.btr_cur_t{ .index = index, .block = block, .rec = null, .opened = true };
    var rec_out: ?*btr.rec_t = null;
    var big_out: ?*data.big_rec_t = null;
    var mtr = btr.mtr_t{};
    _ = btr.btr_cur_optimistic_insert(0, &cursor, tuple, &rec_out, &big_out, 0, null, &mtr);
}

fn createSysTable(
    allocator: std.mem.Allocator,
    table_name: []const u8,
    table_id: dict.dulint,
    cols: []const ColumnSpec,
) struct { table: *dict.dict_table_t, index: *dict.dict_index_t } {
    _ = allocator;
    const table = dict.dict_mem_table_create(table_name, dict.DICT_HDR_SPACE, @as(dict.ulint, @intCast(cols.len)), 0) orelse {
        @panic("dict_sys_btr table");
    };
    for (cols) |col| {
        dict.dict_mem_table_add_col(table, null, col.name, col.mtype, col.prtype, col.len);
    }
    table.id = table_id;
    table.n_cols = @as(dict.ulint, @intCast(table.cols.items.len));
    table.n_def = table.n_cols;

    const index = dict.dict_mem_index_create(table_name, "PRIMARY", dict.DICT_HDR_SPACE, dict.DICT_UNIQUE | dict.DICT_CLUSTERED, @as(dict.ulint, @intCast(cols.len))) orelse {
        @panic("dict_sys_btr index");
    };
    index.table = table;
    for (table.cols.items, 0..) |*col, i| {
        dict.dict_index_add_col(index, table, col, @as(dict.ulint, @intCast(i)));
    }
    index.id = table_id;
    var mtr = btr.mtr_t{};
    _ = btr.btr_create(index.type, dict.DICT_HDR_SPACE, 0, index.id, index, &mtr);

    return .{ .table = table, .index = index };
}

fn ensureSysBtr(allocator: std.mem.Allocator) *SysBtrState {
    if (sys_btr) |state| {
        return state;
    }

    const tables_cols = [_]ColumnSpec{
        .{ .name = "ID", .mtype = data.DATA_INT, .prtype = data.DATA_UNSIGNED, .len = 8 },
        .{ .name = "NAME", .mtype = data.DATA_VARCHAR, .prtype = data.DATA_BINARY, .len = 255 },
        .{ .name = "SPACE", .mtype = data.DATA_INT, .prtype = data.DATA_UNSIGNED, .len = 4 },
        .{ .name = "N_COLS", .mtype = data.DATA_INT, .prtype = data.DATA_UNSIGNED, .len = 4 },
        .{ .name = "FLAGS", .mtype = data.DATA_INT, .prtype = data.DATA_UNSIGNED, .len = 4 },
    };
    const cols_cols = [_]ColumnSpec{
        .{ .name = "ROW_ID", .mtype = data.DATA_INT, .prtype = data.DATA_UNSIGNED, .len = 8 },
        .{ .name = "TABLE_ID", .mtype = data.DATA_INT, .prtype = data.DATA_UNSIGNED, .len = 8 },
        .{ .name = "NAME", .mtype = data.DATA_VARCHAR, .prtype = data.DATA_BINARY, .len = 255 },
        .{ .name = "POS", .mtype = data.DATA_INT, .prtype = data.DATA_UNSIGNED, .len = 4 },
        .{ .name = "MTYPE", .mtype = data.DATA_INT, .prtype = data.DATA_UNSIGNED, .len = 4 },
        .{ .name = "PRTYPE", .mtype = data.DATA_INT, .prtype = data.DATA_UNSIGNED, .len = 4 },
        .{ .name = "LEN", .mtype = data.DATA_INT, .prtype = data.DATA_UNSIGNED, .len = 4 },
    };
    const indexes_cols = [_]ColumnSpec{
        .{ .name = "ID", .mtype = data.DATA_INT, .prtype = data.DATA_UNSIGNED, .len = 8 },
        .{ .name = "TABLE_ID", .mtype = data.DATA_INT, .prtype = data.DATA_UNSIGNED, .len = 8 },
        .{ .name = "NAME", .mtype = data.DATA_VARCHAR, .prtype = data.DATA_BINARY, .len = 255 },
        .{ .name = "TYPE", .mtype = data.DATA_INT, .prtype = data.DATA_UNSIGNED, .len = 4 },
        .{ .name = "SPACE", .mtype = data.DATA_INT, .prtype = data.DATA_UNSIGNED, .len = 4 },
    };

    const state = allocator.create(SysBtrState) catch @panic("dict_sys_btr state");
    const tables = createSysTable(allocator, "SYS_TABLES", dict.DICT_TABLES_ID, tables_cols[0..]);
    const columns = createSysTable(allocator, "SYS_COLUMNS", dict.DICT_COLUMNS_ID, cols_cols[0..]);
    const indexes = createSysTable(allocator, "SYS_INDEXES", dict.DICT_INDEXES_ID, indexes_cols[0..]);
    state.* = .{
        .tables_table = tables.table,
        .tables_index = tables.index,
        .columns_table = columns.table,
        .columns_index = columns.index,
        .indexes_table = indexes.table,
        .indexes_index = indexes.index,
        .allocator = allocator,
    };
    sys_btr = state;
    return state;
}

fn dictSysBtrClear() void {
    if (sys_btr) |state| {
        btr.btr_free_index(state.tables_index);
        btr.btr_free_index(state.columns_index);
        btr.btr_free_index(state.indexes_index);
        dict.dict_mem_index_free(state.tables_index);
        dict.dict_mem_index_free(state.columns_index);
        dict.dict_mem_index_free(state.indexes_index);
        dict.dict_mem_table_free(state.tables_table);
        dict.dict_mem_table_free(state.columns_table);
        dict.dict_mem_table_free(state.indexes_table);
        state.allocator.destroy(state);
        sys_btr = null;
    }
}

fn dictSysBtrInsertTable(row: dict.dict_sys_table_row_t) void {
    const state = ensureSysBtr(std.heap.page_allocator);
    var id_val = dulintToU64(row.id);
    var space_val: u32 = @intCast(row.space);
    var cols_val: u32 = @intCast(row.n_cols);
    var flags_val: u32 = @intCast(row.flags);
    var fields = [_]data.dfield_t{
        .{ .data = @ptrCast(&id_val), .len = 8 },
        .{ .data = @ptrCast(@constCast(row.name.ptr)), .len = @intCast(row.name.len) },
        .{ .data = @ptrCast(&space_val), .len = 4 },
        .{ .data = @ptrCast(&cols_val), .len = 4 },
        .{ .data = @ptrCast(&flags_val), .len = 4 },
    };
    var tuple = data.dtuple_t{
        .n_fields = fields.len,
        .n_fields_cmp = 1,
        .fields = fields[0..],
    };
    btrInsert(state.tables_index, &tuple);
}

fn dictSysBtrInsertColumn(row: dict.dict_sys_column_row_t) void {
    const state = ensureSysBtr(std.heap.page_allocator);
    const row_id = dict.dict_sys_get_new_row_id();
    var row_id_val = dulintToU64(row_id);
    var table_id_val = dulintToU64(row.table_id);
    var pos_val: u32 = @intCast(row.pos);
    var mtype_val: u32 = @intCast(row.mtype);
    var prtype_val: u32 = @intCast(row.prtype);
    var len_val: u32 = @intCast(row.len);
    var fields = [_]data.dfield_t{
        .{ .data = @ptrCast(&row_id_val), .len = 8 },
        .{ .data = @ptrCast(&table_id_val), .len = 8 },
        .{ .data = @ptrCast(@constCast(row.name.ptr)), .len = @intCast(row.name.len) },
        .{ .data = @ptrCast(&pos_val), .len = 4 },
        .{ .data = @ptrCast(&mtype_val), .len = 4 },
        .{ .data = @ptrCast(&prtype_val), .len = 4 },
        .{ .data = @ptrCast(&len_val), .len = 4 },
    };
    var tuple = data.dtuple_t{
        .n_fields = fields.len,
        .n_fields_cmp = 1,
        .fields = fields[0..],
    };
    btrInsert(state.columns_index, &tuple);
}

fn dictSysBtrInsertIndex(row: dict.dict_sys_index_row_t) void {
    const state = ensureSysBtr(std.heap.page_allocator);
    var id_val = dulintToU64(row.id);
    var table_id_val = dulintToU64(row.table_id);
    var type_val: u32 = @intCast(row.type);
    var space_val: u32 = @intCast(row.space);
    var fields = [_]data.dfield_t{
        .{ .data = @ptrCast(&id_val), .len = 8 },
        .{ .data = @ptrCast(&table_id_val), .len = 8 },
        .{ .data = @ptrCast(@constCast(row.name.ptr)), .len = @intCast(row.name.len) },
        .{ .data = @ptrCast(&type_val), .len = 4 },
        .{ .data = @ptrCast(&space_val), .len = 4 },
    };
    var tuple = data.dtuple_t{
        .n_fields = fields.len,
        .n_fields_cmp = 1,
        .fields = fields[0..],
    };
    btrInsert(state.indexes_index, &tuple);
}

fn loadSysTables(state: *SysBtrState) bool {
    var cursor = btr.btr_cur_t{};
    var mtr = btr.mtr_t{};
    btr.btr_cur_open_at_index_side_func(compat.TRUE, state.tables_index, 0, &cursor, module_name, 0, &mtr);
    var rec_opt = btr.btr_get_next_user_rec(cursor.rec, null);
    while (rec_opt) |rec| {
        const data_bytes = recDataSlice(rec) orelse return false;
        if (data_bytes.len < SYS_TABLES_FIXED_LEN) {
            return false;
        }
        const name_len = data_bytes.len - SYS_TABLES_FIXED_LEN;
        const id_val = readU64Slice(data_bytes[0..8]);
        const name_bytes = data_bytes[8 .. 8 + name_len];
        const space_val = readU32Slice(data_bytes[8 + name_len .. 12 + name_len]);
        const n_cols_val = readU32Slice(data_bytes[12 + name_len .. 16 + name_len]);
        const flags_val = readU32Slice(data_bytes[16 + name_len .. 20 + name_len]);
        if (dict.dict_sys_table_insert(
            name_bytes,
            dulintFromU64(id_val),
            @as(dict.ulint, @intCast(space_val)),
            @as(dict.ulint, @intCast(n_cols_val)),
            @as(dict.ulint, @intCast(flags_val)),
        ) == compat.FALSE) {
            return false;
        }
        rec_opt = btr.btr_get_next_user_rec(rec, null);
    }
    return true;
}

fn loadSysColumns(state: *SysBtrState, max_row_id: *u64) bool {
    var cursor = btr.btr_cur_t{};
    var mtr = btr.mtr_t{};
    btr.btr_cur_open_at_index_side_func(compat.TRUE, state.columns_index, 0, &cursor, module_name, 0, &mtr);
    var rec_opt = btr.btr_get_next_user_rec(cursor.rec, null);
    while (rec_opt) |rec| {
        const data_bytes = recDataSlice(rec) orelse return false;
        if (data_bytes.len < SYS_COLUMNS_FIXED_LEN) {
            return false;
        }
        const name_len = data_bytes.len - SYS_COLUMNS_FIXED_LEN;
        const row_id_val = readU64Slice(data_bytes[0..8]);
        if (row_id_val > max_row_id.*) {
            max_row_id.* = row_id_val;
        }
        const table_id_val = readU64Slice(data_bytes[8..16]);
        const name_bytes = data_bytes[16 .. 16 + name_len];
        const pos_val = readU32Slice(data_bytes[16 + name_len .. 20 + name_len]);
        const mtype_val = readU32Slice(data_bytes[20 + name_len .. 24 + name_len]);
        const prtype_val = readU32Slice(data_bytes[24 + name_len .. 28 + name_len]);
        const len_val = readU32Slice(data_bytes[28 + name_len .. 32 + name_len]);
        if (dict.dict_sys_column_insert(
            dulintFromU64(table_id_val),
            name_bytes,
            @as(dict.ulint, @intCast(pos_val)),
            @as(dict.ulint, @intCast(mtype_val)),
            @as(dict.ulint, @intCast(prtype_val)),
            @as(dict.ulint, @intCast(len_val)),
        ) == compat.FALSE) {
            return false;
        }
        rec_opt = btr.btr_get_next_user_rec(rec, null);
    }
    return true;
}

fn loadSysIndexes(state: *SysBtrState) bool {
    var cursor = btr.btr_cur_t{};
    var mtr = btr.mtr_t{};
    btr.btr_cur_open_at_index_side_func(compat.TRUE, state.indexes_index, 0, &cursor, module_name, 0, &mtr);
    var rec_opt = btr.btr_get_next_user_rec(cursor.rec, null);
    while (rec_opt) |rec| {
        const data_bytes = recDataSlice(rec) orelse return false;
        if (data_bytes.len < SYS_INDEXES_FIXED_LEN) {
            return false;
        }
        const name_len = data_bytes.len - SYS_INDEXES_FIXED_LEN;
        const id_val = readU64Slice(data_bytes[0..8]);
        const table_id_val = readU64Slice(data_bytes[8..16]);
        const name_bytes = data_bytes[16 .. 16 + name_len];
        const type_val = readU32Slice(data_bytes[16 + name_len .. 20 + name_len]);
        const space_val = readU32Slice(data_bytes[20 + name_len .. 24 + name_len]);
        if (dict.dict_sys_index_insert(
            dulintFromU64(table_id_val),
            dulintFromU64(id_val),
            name_bytes,
            @as(dict.ulint, @intCast(type_val)),
            @as(dict.ulint, @intCast(space_val)),
        ) == compat.FALSE) {
            return false;
        }
        rec_opt = btr.btr_get_next_user_rec(rec, null);
    }
    return true;
}

pub fn dict_sys_btr_load_cache() compat.ibool {
    const state = ensureSysBtr(std.heap.page_allocator);
    const prev_hooks = dict.dict_sys_btr_hooks;
    dict.dict_sys_btr_set_hooks(.{});
    defer dict.dict_sys_btr_set_hooks(prev_hooks);

    dict.dict_sys_clear_in_memory();

    var max_row_id: u64 = 0;
    if (!loadSysTables(state)) {
        return compat.FALSE;
    }
    if (!loadSysColumns(state, &max_row_id)) {
        return compat.FALSE;
    }
    if (!loadSysIndexes(state)) {
        return compat.FALSE;
    }
    if (max_row_id != 0) {
        dict.dict_sys.row_id = dulintFromU64(max_row_id + 1);
    }
    dict.dict_sys_load_cache();
    return compat.TRUE;
}

pub fn dict_sys_btr_init(allocator: std.mem.Allocator) void {
    dictSysBtrClear();
    _ = ensureSysBtr(allocator);
    dict.dict_sys_btr_set_hooks(.{
        .insert_table = dictSysBtrInsertTable,
        .insert_column = dictSysBtrInsertColumn,
        .insert_index = dictSysBtrInsertIndex,
        .clear = dictSysBtrClear,
    });

    if (fil.fil_tablespace_exists_in_mem(dict.DICT_HDR_SPACE) != compat.TRUE) {
        return;
    }

    for (dict.dict_sys.sys_tables.items) |row| {
        dictSysBtrInsertTable(row);
    }
    for (dict.dict_sys.sys_columns.items) |row| {
        dictSysBtrInsertColumn(row);
    }
    for (dict.dict_sys.sys_indexes.items) |row| {
        dictSysBtrInsertIndex(row);
    }
}

pub fn dict_sys_btr_clear() void {
    dictSysBtrClear();
}

pub fn dict_sys_btr_find_table(id: dict.dulint) ?*btr.rec_t {
    const state = sys_btr orelse return null;
    const key = @as(i64, @intCast(dulintToU64(id)));
    return btr.btr_find_rec_by_key(state.tables_index, key);
}

test "dict sys tables stored in BTR index" {
    dict.dict_var_init();
    dict.dict_init();
    dict.dict_create();
    dict_sys_btr_init(std.testing.allocator);
    defer dict_sys_btr_clear();

    _ = dict.dict_sys_table_insert("db/sys_test", .{ .high = 0, .low = 42 }, 0, 5, 0);

    const found = dict_sys_btr_find_table(.{ .high = 0, .low = 42 });
    try std.testing.expect(found != null);
}

test "dict sys btr load cache rebuilds dict tables" {
    dict.dict_var_init();
    dict.dict_init();
    dict.dict_create();
    dict_sys_btr_init(std.testing.allocator);
    defer dict_sys_btr_clear();

    const table_id: dict.dulint = .{ .high = 0, .low = 42 };
    const index_id: dict.dulint = .{ .high = 0, .low = 43 };
    _ = dict.dict_sys_table_insert("db/sys_test", table_id, 0, 1, 0);
    _ = dict.dict_sys_column_insert(table_id, "id", 0, data.DATA_INT, data.DATA_UNSIGNED, 4);
    _ = dict.dict_sys_index_insert(table_id, index_id, "PRIMARY", dict.DICT_CLUSTERED | dict.DICT_UNIQUE, 0);

    try std.testing.expectEqual(compat.TRUE, dict_sys_btr_load_cache());
    try std.testing.expect(dict.dict_table_get_low("db/sys_test") != null);
    try std.testing.expect(dict.dict_index_find_on_id_low(index_id) != null);
}
