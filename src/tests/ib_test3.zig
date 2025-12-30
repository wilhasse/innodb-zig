const std = @import("std");
const api = @import("../api/api.zig");
const compat = @import("../ut/compat.zig");

const DATABASE: []const u8 = "test";
const TABLE: []const u8 = "t";

const sizes = [_]u8{ 8, 16, 32, 64 };

fn expectOk(err: api.ib_err_t) !void {
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
}

fn createDatabase() !void {
    try std.testing.expectEqual(compat.IB_TRUE, api.ib_database_create(DATABASE));
}

fn tableName(buf: []u8, size: u8) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}/{s}{d}", .{ DATABASE, TABLE, size });
}

fn createTable(size: u8) !void {
    var buf: [api.IB_MAX_TABLE_NAME_LEN]u8 = undefined;
    const name = try tableName(&buf, size);

    var tbl_sch: api.ib_tbl_sch_t = null;
    try expectOk(api.ib_table_schema_create(name, &tbl_sch, .IB_TBL_COMPACT, 0));
    defer api.ib_table_schema_delete(tbl_sch);

    const byte_len: api.ib_ulint_t = @intCast(size / 8);
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c1", .IB_INT, .IB_COL_NONE, 0, byte_len));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "c2", .IB_INT, .IB_COL_UNSIGNED, 0, byte_len));

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);
    try expectOk(api.ib_schema_lock_exclusive(trx));
    var table_id: api.ib_id_t = 0;
    try expectOk(api.ib_table_create(trx, tbl_sch, &table_id));
    try expectOk(api.ib_trx_commit(trx));
}

fn dropTable(size: u8) !void {
    var buf: [api.IB_MAX_TABLE_NAME_LEN]u8 = undefined;
    const name = try tableName(&buf, size);

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);
    try expectOk(api.ib_schema_lock_exclusive(trx));
    try expectOk(api.ib_table_drop(trx, name));
    try expectOk(api.ib_trx_commit(trx));
}

fn openTable(size: u8, trx: api.ib_trx_t) !api.ib_crsr_t {
    var buf: [api.IB_MAX_TABLE_NAME_LEN]u8 = undefined;
    const name = try tableName(&buf, size);

    var crsr: api.ib_crsr_t = null;
    try expectOk(api.ib_cursor_open_table(name, trx, &crsr));
    return crsr;
}

fn insertRows(crsr: api.ib_crsr_t, size: u8) !void {
    const tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(tpl);

    var i: usize = 0;
    while (i < 100) : (i += 1) {
        switch (size) {
            8 => {
                const u8_val: api.ib_u8_t = @intCast(i + 1);
                const i8_val: api.ib_i8_t = @intCast(-@as(i32, @intCast(i + 1)));
                try expectOk(api.ib_tuple_write_i8(tpl, 0, i8_val));
                try expectOk(api.ib_tuple_write_u8(tpl, 1, u8_val));
            },
            16 => {
                const u16_val: api.ib_u16_t = @intCast(i + 1);
                const i16_val: api.ib_i16_t = @intCast(-@as(i32, @intCast(i + 1)));
                try expectOk(api.ib_tuple_write_i16(tpl, 0, i16_val));
                try expectOk(api.ib_tuple_write_u16(tpl, 1, u16_val));
            },
            32 => {
                const u32_val: api.ib_u32_t = @intCast(i + 1);
                const i32_val: api.ib_i32_t = @intCast(-@as(i32, @intCast(i + 1)));
                try expectOk(api.ib_tuple_write_i32(tpl, 0, i32_val));
                try expectOk(api.ib_tuple_write_u32(tpl, 1, u32_val));
            },
            64 => {
                const u64_val: api.ib_u64_t = @intCast(i + 1);
                const i64_val: api.ib_i64_t = @intCast(-@as(i64, @intCast(i + 1)));
                try expectOk(api.ib_tuple_write_i64(tpl, 0, i64_val));
                try expectOk(api.ib_tuple_write_u64(tpl, 1, u64_val));
            },
            else => return,
        }

        try expectOk(api.ib_cursor_insert_row(crsr, tpl));
        _ = api.ib_tuple_clear(tpl);
    }
}

fn readCol(tpl: api.ib_tpl_t, col: api.ib_ulint_t, meta: api.ib_col_meta_t, sum: *i64) !void {
    const unsigned = (@intFromEnum(meta.attr) & @intFromEnum(api.ib_col_attr_t.IB_COL_UNSIGNED)) != 0;
    switch (meta.type_len) {
        1 => {
            if (unsigned) {
                var v: api.ib_u8_t = 0;
                try expectOk(api.ib_tuple_read_u8(tpl, col, &v));
                sum.* += @as(i64, v);
            } else {
                var v: api.ib_i8_t = 0;
                try expectOk(api.ib_tuple_read_i8(tpl, col, &v));
                sum.* -= @as(i64, v);
            }
        },
        2 => {
            if (unsigned) {
                var v: api.ib_u16_t = 0;
                try expectOk(api.ib_tuple_read_u16(tpl, col, &v));
                sum.* += @as(i64, v);
            } else {
                var v: api.ib_i16_t = 0;
                try expectOk(api.ib_tuple_read_i16(tpl, col, &v));
                sum.* -= @as(i64, v);
            }
        },
        4 => {
            if (unsigned) {
                var v: api.ib_u32_t = 0;
                try expectOk(api.ib_tuple_read_u32(tpl, col, &v));
                sum.* += @as(i64, v);
            } else {
                var v: api.ib_i32_t = 0;
                try expectOk(api.ib_tuple_read_i32(tpl, col, &v));
                sum.* -= @as(i64, v);
            }
        },
        8 => {
            if (unsigned) {
                var v: api.ib_u64_t = 0;
                try expectOk(api.ib_tuple_read_u64(tpl, col, &v));
                sum.* += @as(i64, @intCast(v));
            } else {
                var v: api.ib_i64_t = 0;
                try expectOk(api.ib_tuple_read_i64(tpl, col, &v));
                sum.* -= @as(i64, v);
            }
        },
        else => return,
    }
}

fn readRows(crsr: api.ib_crsr_t) !void {
    const tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.OutOfMemory;
    defer api.ib_tuple_delete(tpl);

    var err = api.ib_cursor_first(crsr);
    try expectOk(err);

    var c1_sum: i64 = 0;
    var c2_sum: i64 = 0;
    while (err == .DB_SUCCESS) {
        err = api.ib_cursor_read_row(crsr, tpl);
        if (err != .DB_SUCCESS) {
            break;
        }

        var meta: api.ib_col_meta_t = undefined;
        _ = api.ib_col_get_meta(tpl, 0, &meta);
        try readCol(tpl, 0, meta, &c1_sum);
        _ = api.ib_col_get_meta(tpl, 1, &meta);
        try readCol(tpl, 1, meta, &c2_sum);

        err = api.ib_cursor_next(crsr);
        if (err != .DB_SUCCESS) {
            break;
        }
    }

    try std.testing.expectEqual(c1_sum, c2_sum);
    try expectOk(api.ib_cursor_reset(crsr));
}

test "ib test3 harness" {
    _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
    try expectOk(api.ib_init());
    try expectOk(api.ib_startup("barracuda"));
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);

    try createDatabase();

    for (sizes) |size| {
        try createTable(size);

        const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
        errdefer _ = api.ib_trx_rollback(trx);

        const crsr = try openTable(size, trx);
        try expectOk(api.ib_cursor_lock(crsr, .IB_LOCK_IX));

        try insertRows(crsr, size);
        try readRows(crsr);

        try expectOk(api.ib_cursor_close(crsr));
        try expectOk(api.ib_trx_commit(trx));

        try dropTable(size);
    }
}
