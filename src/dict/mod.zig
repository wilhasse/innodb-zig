const std = @import("std");
const compat = @import("../ut/compat.zig");
const errors = @import("../ut/errors.zig");
const data = @import("../data/mod.zig");
const api = @import("../api/mod.zig").impl;

pub const module_name = "dict";

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;
pub const byte = compat.byte;
pub const dulint = compat.Dulint;
pub const ULINT_UNDEFINED = compat.ULINT_UNDEFINED;

pub const dtype_t = data.dtype_t;
pub const dtuple_t = data.dtuple_t;
pub const dfield_t = data.dfield_t;

pub const DATA_N_SYS_COLS = data.DATA_N_SYS_COLS;
pub const DATA_ROW_ID = data.DATA_ROW_ID;
pub const DATA_TRX_ID = data.DATA_TRX_ID;
pub const DATA_ROLL_PTR = data.DATA_ROLL_PTR;
pub const DATA_NOT_NULL = data.DATA_NOT_NULL;
pub const DATA_SYS = data.DATA_SYS;
pub const DATA_BINARY = data.DATA_BINARY;
pub const DATA_INT = data.DATA_INT;

pub const dict_hdr_t = byte;

pub const dict_col_t = struct {
    name: []const u8 = "",
    mtype: ulint = 0,
    prtype: ulint = 0,
    len: ulint = 0,
    mbminlen: ulint = 0,
    mbmaxlen: ulint = 0,
    ind: ulint = 0,
};

pub const dict_field_t = struct {
    col: ?*dict_col_t = null,
    prefix_len: ulint = 0,
};

pub const dict_table_t = struct {
    name: []const u8 = "",
    id: dulint = .{ .high = 0, .low = 0 },
    flags: ulint = 0,
    n_def: ulint = 0,
    n_cols: ulint = 0,
    cols: std.ArrayListUnmanaged(dict_col_t) = .{},
    col_names: []const u8 = "",
    indexes: std.ArrayListUnmanaged(*dict_index_t) = .{},
    n_handles_opened: ulint = 0,
    cached: bool = false,
    magic_n: ulint = DICT_TABLE_MAGIC_N,
};

pub const dict_index_t = struct {
    name: []const u8 = "",
    id: dulint = .{ .high = 0, .low = 0 },
    type: ulint = 0,
    table: ?*dict_table_t = null,
    n_def: ulint = 0,
    n_fields: ulint = 0,
    n_uniq: ulint = 0,
    n_uniq_in_tree: ulint = 0,
    fields: std.ArrayListUnmanaged(dict_field_t) = .{},
    magic_n: ulint = DICT_INDEX_MAGIC_N,
};

pub const dict_foreign_t = struct {
    id: []const u8 = "",
    foreign_table: ?*dict_table_t = null,
    referenced_table: ?*dict_table_t = null,
    foreign_index: ?*dict_index_t = null,
    referenced_index: ?*dict_index_t = null,
};

pub const dict_sys_t = struct {
    row_id: dulint = .{ .high = 0, .low = DICT_HDR_FIRST_ID },
    booted: bool = false,
    tables_by_name: std.StringHashMapUnmanaged(*dict_table_t) = .{},
    tables_by_id: std.AutoHashMapUnmanaged(u128, *dict_table_t) = .{},
};

pub var dict_sys: dict_sys_t = .{};
pub var dict_ind_redundant: ?*dict_index_t = null;
pub var dict_ind_compact: ?*dict_index_t = null;

pub const rw_lock_t = struct {};
pub const mutex_t = struct {};
pub var dict_operation_lock: rw_lock_t = .{};
pub var dict_foreign_err_mutex: mutex_t = .{};

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

pub const DICT_TABLE_MAGIC_N: ulint = 76333786;
pub const DICT_INDEX_MAGIC_N: ulint = 30505196;

pub const DICT_CLUSTERED: ulint = 1;
pub const DICT_UNIQUE: ulint = 2;
pub const DICT_IBUF: ulint = 4;

pub const DICT_TF_COMPACT: ulint = 1;
pub const DICT_TF_FORMAT_MASK: ulint = 0xF;

const DB_SUCCESS_ULINT: ulint = @intFromEnum(errors.DbErr.DB_SUCCESS);

pub const mem_heap_t = struct {};
pub const que_thr_t = struct {
    node: ?*anyopaque = null,
};
pub const tab_node_t = struct {
    table: ?*dict_table_t = null,
    state: ulint = TABLE_BUILD_TABLE_DEF,
    commit: ibool = compat.FALSE,
    heap: ?*mem_heap_t = null,
};
pub const ind_node_t = struct {
    index: ?*dict_index_t = null,
    state: ulint = INDEX_BUILD_INDEX_DEF,
    table: ?*dict_table_t = null,
    heap: ?*mem_heap_t = null,
};
pub const btr_pcur_t = struct {};
pub const mtr_t = struct {};
pub const rec_t = struct {};
pub const trx_t = struct {};

pub const TABLE_BUILD_TABLE_DEF: ulint = 1;
pub const TABLE_BUILD_COL_DEF: ulint = 2;
pub const TABLE_COMMIT_WORK: ulint = 3;
pub const TABLE_ADD_TO_CACHE: ulint = 4;
pub const TABLE_COMPLETED: ulint = 5;

pub const INDEX_BUILD_INDEX_DEF: ulint = 1;
pub const INDEX_BUILD_FIELD_DEF: ulint = 2;
pub const INDEX_CREATE_INDEX_TREE: ulint = 3;
pub const INDEX_COMMIT_WORK: ulint = 4;
pub const INDEX_ADD_TO_CACHE: ulint = 5;

var dict_hdr_buf: [128]byte = [_]byte{0} ** 128;
var dict_hdr_row_id: dulint = dulintCreate(0, DICT_HDR_FIRST_ID);
var dict_hdr_table_id: dulint = dulintCreate(0, DICT_HDR_FIRST_ID);
var dict_hdr_index_id: dulint = dulintCreate(0, DICT_HDR_FIRST_ID);

pub fn dict_var_init() void {
    dict_sys = .{};
    dict_ind_redundant = null;
    dict_ind_compact = null;
    dict_operation_lock = .{};
    dict_foreign_err_mutex = .{};
}

pub fn dict_init() void {
    dict_sys.tables_by_name = .{};
    dict_sys.tables_by_id = .{};
}

pub fn dict_close() void {
    dict_sys.tables_by_name.deinit(std.heap.page_allocator);
    dict_sys.tables_by_id.deinit(std.heap.page_allocator);
}

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

pub fn tab_create_graph_create(table: *dict_table_t, heap: *mem_heap_t, commit: ibool) ?*tab_node_t {
    const node = std.heap.page_allocator.create(tab_node_t) catch return null;
    node.* = .{ .table = table, .state = TABLE_BUILD_TABLE_DEF, .commit = commit, .heap = heap };
    return node;
}

pub fn ind_create_graph_create(index: *dict_index_t, heap: *mem_heap_t, commit: ibool) ?*ind_node_t {
    const node = std.heap.page_allocator.create(ind_node_t) catch return null;
    node.* = .{ .index = index, .state = INDEX_BUILD_INDEX_DEF, .heap = heap };
    _ = commit;
    return node;
}

pub fn dict_create_table_step(thr: *que_thr_t) ?*que_thr_t {
    return thr;
}

pub fn dict_create_index_step(thr: *que_thr_t) ?*que_thr_t {
    return thr;
}

pub fn dict_truncate_index_tree(table: *dict_table_t, space: ulint, pcur: *btr_pcur_t, mtr: *mtr_t) ulint {
    _ = table;
    _ = space;
    _ = pcur;
    _ = mtr;
    return 0;
}

pub fn dict_drop_index_tree(rec: *rec_t, mtr: *mtr_t) void {
    _ = rec;
    _ = mtr;
}

pub fn dict_create_or_check_foreign_constraint_tables() ulint {
    return DB_SUCCESS_ULINT;
}

pub fn dict_create_add_foreigns_to_dictionary(start_id: ulint, table: *dict_table_t, trx: *trx_t) ulint {
    _ = start_id;
    _ = table;
    _ = trx;
    return DB_SUCCESS_ULINT;
}

pub fn dict_casedn_str(a: []u8) void {
    api.ib_utf8_casedown(a);
}

pub fn dict_tables_have_same_db(name1: []const u8, name2: []const u8) ibool {
    var i: usize = 0;
    while (i < name1.len and i < name2.len and name1[i] == name2[i]) : (i += 1) {
        if (name1[i] == '/') {
            return compat.TRUE;
        }
    }
    return compat.FALSE;
}

pub fn dict_remove_db_name(name: []const u8) []const u8 {
    if (std.mem.indexOfScalar(u8, name, '/')) |pos| {
        return name[pos + 1 ..];
    }
    return name;
}

pub fn dict_get_db_name_len(name: []const u8) ulint {
    if (std.mem.indexOfScalar(u8, name, '/')) |pos| {
        return @as(ulint, pos);
    }
    return 0;
}

pub fn dict_mutex_enter() void {}

pub fn dict_mutex_exit() void {}

pub fn dict_table_decrement_handle_count(table: *dict_table_t, dict_locked: ibool) void {
    _ = dict_locked;
    if (table.n_handles_opened > 0) {
        table.n_handles_opened -= 1;
    }
}

pub fn dict_table_increment_handle_count(table: *dict_table_t, dict_locked: ibool) void {
    _ = dict_locked;
    table.n_handles_opened += 1;
}

pub fn dict_table_get_col_name(table: *const dict_table_t, col_nr: ulint) []const u8 {
    if (col_nr < table.cols.items.len) {
        return table.cols.items[@as(usize, @intCast(col_nr))].name;
    }
    return "";
}

pub fn dict_table_get_col_no(table: *const dict_table_t, name: []const u8) i32 {
    for (table.cols.items, 0..) |col, idx| {
        if (std.mem.eql(u8, col.name, name)) {
            return @as(i32, @intCast(idx));
        }
    }
    return -1;
}

pub fn dict_index_get_on_id_low(table: *dict_table_t, id: dulint) ?*dict_index_t {
    for (table.indexes.items) |index| {
        if (dulintEqual(index.id, id)) {
            return index;
        }
    }
    return null;
}

pub fn dict_index_get_nth_col_pos(index: *const dict_index_t, n: ulint) ulint {
    if (index.table == null) {
        return ULINT_UNDEFINED;
    }
    const table = index.table.?;
    if (n >= table.cols.items.len) {
        return ULINT_UNDEFINED;
    }
    const col = &table.cols.items[@as(usize, @intCast(n))];

    if (dict_index_is_clust(index) != 0) {
        return dict_col_get_clust_pos(col, index);
    }

    for (index.fields.items, 0..) |field, pos| {
        if (field.col == col and field.prefix_len == 0) {
            return @as(ulint, @intCast(pos));
        }
    }

    return ULINT_UNDEFINED;
}

pub fn dict_index_contains_col_or_prefix(index: *const dict_index_t, n: ulint) ibool {
    if (index.table == null) {
        return compat.FALSE;
    }
    if (dict_index_is_clust(index) != 0) {
        return compat.TRUE;
    }
    const table = index.table.?;
    if (n >= table.cols.items.len) {
        return compat.FALSE;
    }
    const col = &table.cols.items[@as(usize, @intCast(n))];
    for (index.fields.items) |field| {
        if (field.col == col) {
            return compat.TRUE;
        }
    }
    return compat.FALSE;
}

pub fn dict_index_get_nth_field_pos(index: *const dict_index_t, index2: *const dict_index_t, n: ulint) ulint {
    if (n >= index2.fields.items.len) {
        return ULINT_UNDEFINED;
    }
    const field2 = index2.fields.items[@as(usize, @intCast(n))];
    for (index.fields.items, 0..) |field, pos| {
        if (field.col == field2.col and (field.prefix_len == 0 or (field.prefix_len >= field2.prefix_len and field2.prefix_len != 0))) {
            return @as(ulint, @intCast(pos));
        }
    }
    return ULINT_UNDEFINED;
}

pub fn dict_table_get_on_id(recovery: u8, table_id: dulint, trx: *trx_t) ?*dict_table_t {
    _ = recovery;
    _ = trx;
    const key = dulintKey(table_id);
    if (dict_sys.tables_by_id.get(key)) |table| {
        return table;
    }
    return null;
}

pub fn dict_table_get(table_name: []const u8, inc_count: ibool) ?*dict_table_t {
    if (dict_sys.tables_by_name.get(table_name)) |table| {
        if (inc_count == compat.TRUE) {
            dict_table_increment_handle_count(table, compat.TRUE);
        }
        return table;
    }
    return null;
}

pub fn dict_table_get_using_id(recovery: u8, table_id: dulint, ref_count: ibool) ?*dict_table_t {
    _ = recovery;
    const table = dict_sys.tables_by_id.get(dulintKey(table_id));
    if (table != null and ref_count == compat.TRUE) {
        dict_table_increment_handle_count(table.?, compat.TRUE);
    }
    return table;
}

pub fn dict_table_check_if_in_cache_low(table_name: []const u8) ?*dict_table_t {
    return dict_sys.tables_by_name.get(table_name);
}

pub fn dict_table_get_low(table_name: []const u8) ?*dict_table_t {
    return dict_table_check_if_in_cache_low(table_name);
}

pub fn dict_table_get_on_id_low(recovery: u8, table_id: dulint) ?*dict_table_t {
    _ = recovery;
    return dict_sys.tables_by_id.get(dulintKey(table_id));
}

pub fn dict_foreign_find_equiv_index(foreign: *dict_foreign_t) ?*dict_index_t {
    _ = foreign;
    return null;
}

pub fn dict_table_get_index_by_max_id(table: *dict_table_t, name: []const u8, columns: []const []const u8, n_cols: ulint) ?*dict_index_t {
    _ = columns;
    _ = n_cols;
    var best: ?*dict_index_t = null;
    for (table.indexes.items) |index| {
        if (!std.mem.eql(u8, index.name, name)) {
            continue;
        }
        if (best == null or dulintLess(best.?.id, index.id)) {
            best = index;
        }
    }
    return best;
}

pub fn dict_table_print(table: *dict_table_t) void {
    _ = table;
}

pub fn dict_table_print_low(table: *dict_table_t) void {
    _ = table;
}

pub fn dict_table_print_by_name(name: []const u8) void {
    _ = name;
}

pub fn dict_print_info_on_foreign_keys(create_table_format: ibool, stream: ?*anyopaque, trx: *trx_t, table: *dict_table_t) void {
    _ = create_table_format;
    _ = stream;
    _ = trx;
    _ = table;
}

pub fn dict_print_info_on_foreign_key_in_create_format(stream: ?*anyopaque, trx: *trx_t, foreign: *dict_foreign_t, add_newline: ibool) void {
    _ = stream;
    _ = trx;
    _ = foreign;
    _ = add_newline;
}

pub fn dict_index_name_print(stream: ?*anyopaque, trx: *trx_t, index: *const dict_index_t) void {
    _ = stream;
    _ = trx;
    _ = index;
}

pub fn dict_table_get_first_index(table: *const dict_table_t) ?*dict_index_t {
    if (table.indexes.items.len == 0) {
        return null;
    }
    return table.indexes.items[0];
}

pub fn dict_table_get_next_index(index: *const dict_index_t) ?*dict_index_t {
    if (index.table == null) {
        return null;
    }
    const table = index.table.?;
    for (table.indexes.items, 0..) |item, idx| {
        if (item == index) {
            const next = idx + 1;
            if (next < table.indexes.items.len) {
                return table.indexes.items[next];
            }
            break;
        }
    }
    return null;
}

pub fn dict_index_is_clust(index: *const dict_index_t) ulint {
    return if ((index.type & DICT_CLUSTERED) != 0) 1 else 0;
}

pub fn dict_index_is_unique(index: *const dict_index_t) ulint {
    return if ((index.type & DICT_UNIQUE) != 0) 1 else 0;
}

pub fn dict_index_is_ibuf(index: *const dict_index_t) ulint {
    return if ((index.type & DICT_IBUF) != 0) 1 else 0;
}

pub fn dict_index_is_sec_or_ibuf(index: *const dict_index_t) ulint {
    if ((index.type & DICT_CLUSTERED) == 0 or (index.type & DICT_IBUF) != 0) {
        return 1;
    }
    return 0;
}

pub fn dict_table_get_n_user_cols(table: *const dict_table_t) ulint {
    if (table.n_cols < DATA_N_SYS_COLS) {
        return 0;
    }
    return table.n_cols - DATA_N_SYS_COLS;
}

pub fn dict_table_get_n_sys_cols(table: *const dict_table_t) ulint {
    _ = table;
    return DATA_N_SYS_COLS;
}

pub fn dict_table_get_n_cols(table: *const dict_table_t) ulint {
    return table.n_cols;
}

pub fn dict_table_get_nth_col(table: *const dict_table_t, pos: ulint) ?*dict_col_t {
    if (pos >= table.cols.items.len) {
        return null;
    }
    return &table.cols.items[@as(usize, @intCast(pos))];
}

pub fn dict_table_get_sys_col(table: *const dict_table_t, sys: ulint) ?*dict_col_t {
    if (sys >= DATA_N_SYS_COLS) {
        return null;
    }
    if (table.n_cols < DATA_N_SYS_COLS) {
        return null;
    }
    const pos = table.n_cols - DATA_N_SYS_COLS + sys;
    return dict_table_get_nth_col(table, pos);
}

pub fn dict_table_get_sys_col_no(table: *const dict_table_t, sys: ulint) ulint {
    if (table.n_cols < DATA_N_SYS_COLS) {
        return ULINT_UNDEFINED;
    }
    return table.n_cols - DATA_N_SYS_COLS + sys;
}

pub fn dict_table_is_comp(table: *const dict_table_t) ibool {
    return if ((table.flags & DICT_TF_COMPACT) != 0) compat.TRUE else compat.FALSE;
}

pub fn dict_table_get_format(table: *const dict_table_t) ulint {
    return table.flags & DICT_TF_FORMAT_MASK;
}

pub fn dict_table_set_format(table: *dict_table_t, format: ulint) void {
    table.flags = (table.flags & ~DICT_TF_FORMAT_MASK) | (format & DICT_TF_FORMAT_MASK);
}

pub fn dict_table_flags_to_zip_size(flags: ulint) ulint {
    _ = flags;
    return 0;
}

pub fn dict_table_zip_size(table: *const dict_table_t) ulint {
    return dict_table_flags_to_zip_size(table.flags);
}

pub fn dict_table_col_in_clustered_key(table: *const dict_table_t, n: ulint) ibool {
    const clust = dict_table_get_first_index(table) orelse return compat.FALSE;
    return dict_index_contains_col_or_prefix(clust, n);
}

pub fn dict_table_copy_types(tuple: *dtuple_t, table: *const dict_table_t) void {
    const count = @min(tuple.fields.len, table.cols.items.len);
    var i: usize = 0;
    while (i < count) : (i += 1) {
        data.dfield_set_type(&tuple.fields[i], &dtypeFromCol(&table.cols.items[i]));
        data.dfield_set_null(&tuple.fields[i]);
    }
}

pub fn dict_index_find_on_id_low(id: dulint) ?*dict_index_t {
    var iter = dict_sys.tables_by_name.iterator();
    while (iter.next()) |entry| {
        for (entry.value_ptr.*.indexes.items) |index| {
            if (dulintEqual(index.id, id)) {
                return index;
            }
        }
    }
    return null;
}

pub fn dict_index_add_to_cache(table: *dict_table_t, index: *dict_index_t, page_no: ulint, strict: ibool) ulint {
    _ = page_no;
    _ = strict;
    index.table = table;
    index.n_fields = @as(ulint, @intCast(index.fields.items.len));
    if (index.n_uniq == 0) {
        index.n_uniq = index.n_fields;
    }
    if (index.n_uniq_in_tree == 0) {
        index.n_uniq_in_tree = index.n_fields;
    }
    table.indexes.append(std.heap.page_allocator, index) catch return @intFromEnum(errors.DbErr.DB_OUT_OF_MEMORY);
    return DB_SUCCESS_ULINT;
}

pub fn dict_index_remove_from_cache(table: *dict_table_t, index: *dict_index_t) void {
    for (table.indexes.items, 0..) |item, idx| {
        if (item == index) {
            table.indexes.orderedRemove(idx);
            break;
        }
    }
}

pub fn dict_index_get_n_fields(index: *const dict_index_t) ulint {
    return @as(ulint, @intCast(index.fields.items.len));
}

pub fn dict_index_get_n_unique(index: *const dict_index_t) ulint {
    return if (index.n_uniq == 0) dict_index_get_n_fields(index) else index.n_uniq;
}

pub fn dict_index_get_n_unique_in_tree(index: *const dict_index_t) ulint {
    return if (index.n_uniq_in_tree == 0) dict_index_get_n_fields(index) else index.n_uniq_in_tree;
}

pub fn dict_index_get_n_ordering_defined_by_user(index: *const dict_index_t) ulint {
    return dict_index_get_n_fields(index);
}

pub fn dict_index_get_nth_field(index: *const dict_index_t, pos: ulint) ?*dict_field_t {
    if (pos >= index.fields.items.len) {
        return null;
    }
    return &index.fields.items[@as(usize, @intCast(pos))];
}

pub fn dict_index_get_nth_col(index: *const dict_index_t, pos: ulint) ?*const dict_col_t {
    const field = dict_index_get_nth_field(index, pos) orelse return null;
    return field.col;
}

pub fn dict_index_get_nth_col_no(index: *const dict_index_t, pos: ulint) ulint {
    const col = dict_index_get_nth_col(index, pos) orelse return ULINT_UNDEFINED;
    return col.ind;
}

pub fn dict_table_get_nth_col_pos(table: *const dict_table_t, n: ulint) ulint {
    const clust = dict_table_get_first_index(table) orelse return ULINT_UNDEFINED;
    return dict_index_get_nth_col_pos(clust, n);
}

pub fn dict_index_get_sys_col_pos(index: *const dict_index_t, type_: ulint) ulint {
    for (index.fields.items, 0..) |field, pos| {
        if (field.col) |col| {
            if (col.mtype == DATA_SYS and (col.prtype & 0xFF) == type_) {
                return @as(ulint, @intCast(pos));
            }
        }
    }
    return ULINT_UNDEFINED;
}

pub fn dict_index_add_col(index: *dict_index_t, table: *const dict_table_t, col: *dict_col_t, prefix_len: ulint) void {
    _ = table;
    index.fields.append(std.heap.page_allocator, .{ .col = col, .prefix_len = prefix_len }) catch return;
    index.n_fields = @as(ulint, @intCast(index.fields.items.len));
}

pub fn dict_index_copy_types(tuple: *dtuple_t, index: *const dict_index_t, n_fields: ulint) void {
    const count = @min(@as(usize, @intCast(n_fields)), index.fields.items.len);
    var i: usize = 0;
    while (i < count and i < tuple.fields.len) : (i += 1) {
        const col = index.fields.items[i].col orelse continue;
        data.dfield_set_type(&tuple.fields[i], &dtypeFromCol(col));
    }
}

pub fn dict_field_get_col(field: *const dict_field_t) ?*const dict_col_t {
    return field.col;
}

pub fn dict_col_copy_type(col: *const dict_col_t, type_: *dtype_t) void {
    type_.mtype = col.mtype;
    type_.prtype = col.prtype;
    type_.len = col.len;
    type_.mbminlen = col.mbminlen;
    type_.mbmaxlen = col.mbmaxlen;
}

pub fn dict_col_type_assert_equal(col: *const dict_col_t, type_: *const dtype_t) ibool {
    if (col.mtype != type_.mtype or col.prtype != type_.prtype or col.len != type_.len) {
        return compat.FALSE;
    }
    if (col.mbminlen != type_.mbminlen or col.mbmaxlen != type_.mbmaxlen) {
        return compat.FALSE;
    }
    return compat.TRUE;
}

pub fn dict_col_get_min_size(col: *const dict_col_t) ulint {
    return data.dtype_get_min_size_low(col.mtype, col.prtype, col.len, col.mbminlen, col.mbmaxlen);
}

pub fn dict_col_get_max_size(col: *const dict_col_t) ulint {
    return data.dtype_get_max_size_low(col.mtype, col.len);
}

pub fn dict_col_get_fixed_size(col: *const dict_col_t, comp: ulint) ulint {
    return data.dtype_get_fixed_size_low(col.mtype, col.prtype, col.len, col.mbminlen, col.mbmaxlen, comp);
}

pub fn dict_col_get_sql_null_size(col: *const dict_col_t, comp: ulint) ulint {
    return dict_col_get_fixed_size(col, comp);
}

pub fn dict_col_get_no(col: *const dict_col_t) ulint {
    return col.ind;
}

pub fn dict_col_get_clust_pos(col: *const dict_col_t, clust_index: *const dict_index_t) ulint {
    for (clust_index.fields.items, 0..) |field, idx| {
        if (field.col == col and field.prefix_len == 0) {
            return @as(ulint, @intCast(idx));
        }
    }
    return ULINT_UNDEFINED;
}

pub fn dict_col_name_is_reserved(name: []const u8) ibool {
    const reserved = [_][]const u8{ "DB_ROW_ID", "DB_TRX_ID", "DB_ROLL_PTR" };
    for (reserved) |value| {
        if (std.ascii.eqlIgnoreCase(name, value)) {
            return compat.TRUE;
        }
    }
    return compat.FALSE;
}

pub fn dict_table_add_system_columns(table: *dict_table_t, heap: *mem_heap_t) void {
    _ = heap;
    if (table.n_cols >= DATA_N_SYS_COLS and table.n_cols == table.cols.items.len) {
        return;
    }
    const sys_names = [_][]const u8{ "DB_ROW_ID", "DB_TRX_ID", "DB_ROLL_PTR" };
    const sys_types = [_]ulint{ DATA_ROW_ID, DATA_TRX_ID, DATA_ROLL_PTR };
    for (sys_names, 0..) |name, i| {
        const col = dict_col_t{
            .name = name,
            .mtype = DATA_SYS,
            .prtype = sys_types[i] | DATA_NOT_NULL,
            .len = 0,
            .mbminlen = 0,
            .mbmaxlen = 0,
            .ind = table.n_cols,
        };
        table.cols.append(std.heap.page_allocator, col) catch return;
        table.n_cols += 1;
    }
    if (table.n_def == 0) {
        table.n_def = table.n_cols;
    }
}

pub fn dict_table_add_to_cache(table: *dict_table_t, heap: *mem_heap_t) void {
    _ = heap;
    if (table.n_cols == 0) {
        table.n_cols = @as(ulint, @intCast(table.cols.items.len));
    }
    if (table.n_def == 0) {
        table.n_def = table.n_cols;
    }
    table.cached = true;
    const key = dulintKey(table.id);
    _ = dict_sys.tables_by_name.put(std.heap.page_allocator, table.name, table);
    _ = dict_sys.tables_by_id.put(std.heap.page_allocator, key, table);
}

pub fn dict_table_remove_from_cache(table: *dict_table_t) void {
    _ = dict_sys.tables_by_name.remove(table.name);
    _ = dict_sys.tables_by_id.remove(dulintKey(table.id));
    table.cached = false;
}

pub fn dict_table_rename_in_cache(table: *dict_table_t, new_name: []const u8, rename_also_foreigns: ibool) ibool {
    _ = rename_also_foreigns;
    dict_table_remove_from_cache(table);
    table.name = new_name;
    dict_table_add_to_cache(table, &mem_heap_t{});
    return compat.TRUE;
}

pub fn dict_table_change_id_in_cache(table: *dict_table_t, new_id: dulint) void {
    _ = dict_sys.tables_by_id.remove(dulintKey(table.id));
    table.id = new_id;
    _ = dict_sys.tables_by_id.put(std.heap.page_allocator, dulintKey(new_id), table);
}

pub fn dict_foreign_add_to_cache(foreign: *dict_foreign_t, check_charsets: ibool) ulint {
    _ = foreign;
    _ = check_charsets;
    return DB_SUCCESS_ULINT;
}

pub fn dict_table_get_referenced_constraint(table: *dict_table_t, index: *dict_index_t) ?*dict_foreign_t {
    _ = table;
    _ = index;
    return null;
}

pub fn dict_table_is_referenced_by_foreign_key(table: *const dict_table_t) ibool {
    _ = table;
    return compat.FALSE;
}

pub fn dict_table_replace_index_in_foreign_list(table: *dict_table_t, index: *dict_index_t) void {
    _ = table;
    _ = index;
}

pub fn dict_table_get_foreign_constraint(table: *dict_table_t, index: *dict_index_t) ?*dict_foreign_t {
    _ = table;
    _ = index;
    return null;
}

pub fn dict_create_foreign_constraints(trx: *trx_t, sql_string: []const u8, name: []const u8, reject_fks: ibool) ulint {
    _ = trx;
    _ = sql_string;
    _ = name;
    _ = reject_fks;
    return DB_SUCCESS_ULINT;
}

pub fn dict_foreign_parse_drop_constraints(heap: *mem_heap_t, trx: *trx_t, table: *dict_table_t, n: *ulint, constraints_to_drop: *[][]const u8) ulint {
    _ = heap;
    _ = trx;
    _ = table;
    n.* = 0;
    constraints_to_drop.* = &.{};
    return DB_SUCCESS_ULINT;
}

pub fn dict_check_tablespaces_and_store_max_id(in_crash_recovery: ibool) void {
    _ = in_crash_recovery;
}

pub fn dict_get_first_table_name_in_db(name: []const u8) ?[]u8 {
    var iter = dict_sys.tables_by_name.iterator();
    while (iter.next()) |entry| {
        const key = entry.key_ptr.*;
        if (std.mem.startsWith(u8, key, name)) {
            const buf = std.heap.page_allocator.alloc(u8, key.len) catch return null;
            std.mem.copyForwards(u8, buf, key);
            return buf;
        }
    }
    return null;
}

pub fn dict_load_table(recovery: u8, name: []const u8) ?*dict_table_t {
    _ = recovery;
    return dict_table_get_low(name);
}

pub fn dict_load_table_on_id(recovery: u8, table_id: dulint) ?*dict_table_t {
    return dict_table_get_on_id_low(recovery, table_id);
}

pub fn dict_load_sys_table(table: *dict_table_t) void {
    dict_table_add_to_cache(table, &mem_heap_t{});
}

pub fn dict_load_foreigns(table_name: []const u8, check_charsets: ibool) ulint {
    _ = table_name;
    _ = check_charsets;
    return DB_SUCCESS_ULINT;
}

pub fn dict_print() void {}

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

fn dulintEqual(a: dulint, b: dulint) bool {
    return a.high == b.high and a.low == b.low;
}

fn dulintLess(a: dulint, b: dulint) bool {
    if (a.high != b.high) {
        return a.high < b.high;
    }
    return a.low < b.low;
}

fn dulintKey(id: dulint) u128 {
    const shift = @bitSizeOf(ulint);
    return (@as(u128, id.high) << shift) | @as(u128, id.low);
}

fn dtypeFromCol(col: *const dict_col_t) dtype_t {
    var t: dtype_t = .{};
    dict_col_copy_type(col, &t);
    return t;
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

test "dict create graph stubs" {
    var table = dict_table_t{ .name = "t1" };
    var index = dict_index_t{ .name = "idx1" };
    var heap = mem_heap_t{};

    const tab_node = tab_create_graph_create(&table, &heap, compat.TRUE) orelse return error.OutOfMemory;
    defer std.heap.page_allocator.destroy(tab_node);
    try std.testing.expect(tab_node.table == &table);
    try std.testing.expectEqual(@as(ulint, TABLE_BUILD_TABLE_DEF), tab_node.state);

    const ind_node = ind_create_graph_create(&index, &heap, compat.FALSE) orelse return error.OutOfMemory;
    defer std.heap.page_allocator.destroy(ind_node);
    try std.testing.expect(ind_node.index == &index);
    try std.testing.expectEqual(@as(ulint, INDEX_BUILD_INDEX_DEF), ind_node.state);

    var thr = que_thr_t{};
    try std.testing.expect(dict_create_table_step(&thr) == &thr);
    try std.testing.expect(dict_create_index_step(&thr) == &thr);
    try std.testing.expectEqual(DB_SUCCESS_ULINT, dict_create_or_check_foreign_constraint_tables());
}

test "dict string helpers" {
    var buf = [_]u8{ 'A', 'B', 'C' };
    dict_casedn_str(buf[0..]);
    try std.testing.expectEqualStrings("abc", buf[0..]);

    try std.testing.expectEqual(compat.TRUE, dict_tables_have_same_db("db/t1", "db/t2"));
    try std.testing.expectEqual(compat.FALSE, dict_tables_have_same_db("db1/t1", "db2/t2"));

    try std.testing.expectEqual(@as(ulint, 2), dict_get_db_name_len("db/t"));
    try std.testing.expectEqualStrings("t1", dict_remove_db_name("db/t1"));
}

test "dict table cache and index helpers" {
    dict_var_init();
    dict_init();

    var table = dict_table_t{ .name = "db/t1", .id = dulintCreate(0, 100) };
    try table.cols.append(std.heap.page_allocator, .{
        .name = "c1",
        .mtype = DATA_INT,
        .prtype = 0,
        .len = 4,
        .ind = 0,
    });
    table.n_cols = 1;
    dict_table_add_to_cache(&table, &mem_heap_t{});
    defer dict_table_remove_from_cache(&table);

    const fetched = dict_table_get("db/t1", compat.TRUE);
    try std.testing.expect(fetched != null);
    try std.testing.expect(fetched.? == &table);

    try std.testing.expectEqual(@as(i32, 0), dict_table_get_col_no(&table, "c1"));
    try std.testing.expectEqualStrings("c1", dict_table_get_col_name(&table, 0));

    var index = dict_index_t{ .name = "PRIMARY", .type = DICT_CLUSTERED | DICT_UNIQUE };
    dict_index_add_col(&index, &table, &table.cols.items[0], 0);
    _ = dict_index_add_to_cache(&table, &index, 0, compat.FALSE);

    try std.testing.expectEqual(@as(ulint, 0), dict_index_get_nth_col_pos(&index, 0));
    try std.testing.expectEqual(compat.TRUE, dict_index_contains_col_or_prefix(&index, 0));
    try std.testing.expectEqual(@as(ulint, 1), dict_index_get_n_fields(&index));
    try std.testing.expectEqual(@as(ulint, 1), dict_index_get_n_unique(&index));

    dict_table_change_id_in_cache(&table, dulintCreate(0, 200));
    const renamed = dict_table_rename_in_cache(&table, "db/t2", compat.FALSE);
    try std.testing.expectEqual(compat.TRUE, renamed);
    try std.testing.expect(dict_table_get("db/t2", compat.FALSE) != null);
}

test "dict reserved column names" {
    try std.testing.expectEqual(compat.TRUE, dict_col_name_is_reserved("db_row_id"));
    try std.testing.expectEqual(compat.TRUE, dict_col_name_is_reserved("DB_TRX_ID"));
    try std.testing.expectEqual(compat.FALSE, dict_col_name_is_reserved("user_col"));
}

test "dict load helpers" {
    dict_var_init();
    dict_init();

    var table = dict_table_t{ .name = "db/load1", .id = dulintCreate(0, 300) };
    dict_table_add_to_cache(&table, &mem_heap_t{});
    defer dict_table_remove_from_cache(&table);

    const first = dict_get_first_table_name_in_db("db/") orelse return error.OutOfMemory;
    defer std.heap.page_allocator.free(first);
    try std.testing.expect(std.mem.startsWith(u8, first, "db/"));

    try std.testing.expect(dict_load_table(0, "db/load1") == &table);
    try std.testing.expect(dict_load_table_on_id(0, table.id) == &table);
}
