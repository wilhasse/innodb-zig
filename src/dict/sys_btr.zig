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

fn dulintToU64(id: dict.dulint) u64 {
    return (@as(u64, id.high) << 32) | id.low;
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
