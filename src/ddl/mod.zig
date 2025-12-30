const std = @import("std");
const compat = @import("../ut/compat.zig");
const errors = @import("../ut/errors.zig");

pub const module_name = "ddl";

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;
pub const db_err = errors.DbErr;

pub const dict_table_t = struct {
    name: []const u8 = "",
};

pub const dict_index_t = struct {};
pub const trx_t = struct {};

pub const ib_recovery_t = enum(u8) {
    IB_RECOVERY_DEFAULT = 0,
};

const DB_SUCCESS_ULINT: ulint = @intFromEnum(errors.DbErr.DB_SUCCESS);

const S_innodb_monitor = "innodb_monitor";
const S_innodb_lock_monitor = "innodb_lock_monitor";
const S_innodb_tablespace_monitor = "innodb_tablespace_monitor";
const S_innodb_table_monitor = "innodb_table_monitor";
const S_innodb_mem_validate = "innodb_mem_validate";

var drop_list = std.ArrayListUnmanaged([]u8){};
var drop_list_inited: bool = false;

fn ensureDropList() void {
    if (!drop_list_inited) {
        drop_list_inited = true;
    }
}

fn dropListAdd(name: []const u8) ibool {
    ensureDropList();
    for (drop_list.items) |item| {
        if (std.mem.eql(u8, item, name)) {
            return compat.FALSE;
        }
    }

    const buf = std.heap.page_allocator.alloc(u8, name.len) catch return compat.FALSE;
    std.mem.copyForwards(u8, buf, name);
    drop_list.append(std.heap.page_allocator, buf) catch {
        std.heap.page_allocator.free(buf);
        return compat.FALSE;
    };
    return compat.TRUE;
}

pub fn ddl_get_background_drop_list_len_low() ulint {
    ensureDropList();
    return @as(ulint, drop_list.items.len);
}

pub fn ddl_drop_tables_in_background() ulint {
    ensureDropList();
    var dropped: ulint = 0;
    while (drop_list.items.len > 0) {
        const idx = drop_list.items.len - 1;
        const name = drop_list.items[idx];
        std.heap.page_allocator.free(name);
        drop_list.items.len = idx;
        dropped += 1;
    }
    return dropped;
}

pub fn ddl_create_table(table: *dict_table_t, trx: *trx_t) ulint {
    _ = trx;
    _ = table;
    return DB_SUCCESS_ULINT;
}

pub fn ddl_create_index(index: *dict_index_t, trx: *trx_t) ulint {
    _ = index;
    _ = trx;
    return DB_SUCCESS_ULINT;
}

pub fn ddl_drop_table(name: []const u8, trx: *trx_t, drop_db: ibool) ulint {
    _ = trx;
    _ = drop_db;
    _ = name;
    return DB_SUCCESS_ULINT;
}

pub fn ddl_drop_index(table: *dict_table_t, index: *dict_index_t, trx: *trx_t) ulint {
    _ = table;
    _ = index;
    _ = trx;
    return DB_SUCCESS_ULINT;
}

pub fn ddl_truncate_table(table: *dict_table_t, trx: *trx_t) db_err {
    _ = table;
    _ = trx;
    return .DB_SUCCESS;
}

pub fn ddl_rename_table(old_name: []const u8, new_name: []const u8, trx: *trx_t) ulint {
    _ = old_name;
    _ = new_name;
    _ = trx;
    return DB_SUCCESS_ULINT;
}

pub fn ddl_rename_index(table_name: []const u8, old_name: []const u8, new_name: []const u8, trx: *trx_t) ulint {
    _ = table_name;
    _ = old_name;
    _ = new_name;
    _ = trx;
    return DB_SUCCESS_ULINT;
}

pub fn ddl_drop_database(name: []const u8, trx: *trx_t) db_err {
    _ = name;
    _ = trx;
    return .DB_SUCCESS;
}

pub fn ddl_drop_all_temp_indexes(recovery: ib_recovery_t) void {
    _ = recovery;
}

pub fn ddl_drop_all_temp_tables(recovery: ib_recovery_t) void {
    _ = recovery;
}

fn ddl_name_is_monitor(name: []const u8) bool {
    return std.mem.endsWith(u8, name, S_innodb_monitor) or
        std.mem.endsWith(u8, name, S_innodb_lock_monitor) or
        std.mem.endsWith(u8, name, S_innodb_tablespace_monitor) or
        std.mem.endsWith(u8, name, S_innodb_table_monitor) or
        std.mem.endsWith(u8, name, S_innodb_mem_validate);
}

test "ddl background drop list basics" {
    const len0 = ddl_get_background_drop_list_len_low();
    try std.testing.expectEqual(@as(ulint, 0), len0);

    try std.testing.expectEqual(compat.TRUE, dropListAdd("db/t1"));
    try std.testing.expectEqual(compat.FALSE, dropListAdd("db/t1"));
    try std.testing.expectEqual(@as(ulint, 1), ddl_get_background_drop_list_len_low());

    const dropped = ddl_drop_tables_in_background();
    try std.testing.expectEqual(@as(ulint, 1), dropped);
    try std.testing.expectEqual(@as(ulint, 0), ddl_get_background_drop_list_len_low());
}

test "ddl name monitor detection" {
    try std.testing.expect(ddl_name_is_monitor("db/innodb_monitor"));
    try std.testing.expect(ddl_name_is_monitor("innodb_lock_monitor"));
    try std.testing.expect(!ddl_name_is_monitor("db/table1"));
}
