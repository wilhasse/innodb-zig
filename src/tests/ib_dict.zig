const std = @import("std");
const api = @import("../api/api.zig");
const compat = @import("../ut/compat.zig");
const ArrayList = std.array_list.Managed;

const DATABASE: []const u8 = "dict_test";
const TABLE_PREFIX: []const u8 = "t";
const TABLE_COUNT: usize = 10;
const SYSTEM_TABLES = [_][]const u8{ "SYS_TABLES", "SYS_COLUMNS", "SYS_INDEXES" };

fn expectOk(err: api.ib_err_t) !void {
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
}

fn hasAttr(attr: api.ib_col_attr_t, flag: api.ib_col_attr_t) bool {
    return (@intFromEnum(attr) & @intFromEnum(flag)) != 0;
}

fn tableShortName(name: []const u8) []const u8 {
    if (std.mem.indexOfScalar(u8, name, '/')) |pos| {
        return name[pos + 1 ..];
    }
    return name;
}

fn argFromOpaque(arg: api.ib_opaque_t) ?*VisitorArg {
    const ptr = arg orelse return null;
    return @as(*VisitorArg, @ptrCast(@alignCast(ptr)));
}

const VisitorArg = struct {
    output: ArrayList(u8),
    table_name: []const u8 = "",
    cur_col: i32 = 0,
    n_indexes: i32 = 0,
    n_table_cols: i32 = 0,
    n_index_cols: i32 = 0,
    table_list_calls: usize = 0,
    table_calls: usize = 0,
    table_col_calls: usize = 0,
    index_calls: usize = 0,
    index_col_calls: usize = 0,
    had_error: bool = false,
    trx: api.ib_trx_t = null,

    fn init(allocator: std.mem.Allocator) VisitorArg {
        return .{
            .output = ArrayList(u8).init(allocator),
        };
    }

    fn deinit(self: *VisitorArg) void {
        self.output.deinit();
    }
};

fn appendFmt(arg: *VisitorArg, comptime fmt: []const u8, args: anytype) bool {
    arg.output.writer().print(fmt, args) catch {
        arg.had_error = true;
        return false;
    };
    return true;
}

fn isUserTable(name: []const u8) bool {
    return std.mem.indexOfScalar(u8, name, '/') != null;
}

fn columnDisplayName(arg: *const VisitorArg) []const u8 {
    if (isUserTable(arg.table_name)) {
        return switch (arg.cur_col) {
            1 => "C1",
            2 => "C2",
            3 => "C3",
            else => "COL",
        };
    }
    return "ID";
}

fn indexColumnDisplayName(arg: *const VisitorArg) []const u8 {
    if (isUserTable(arg.table_name)) {
        return switch (arg.cur_col) {
            1 => "C1",
            2 => "C2",
            else => "COL",
        };
    }
    return "ID";
}

fn visit_table_list(
    arg: api.ib_opaque_t,
    _: [*]const u8,
    _: api.ib_tbl_fmt_t,
    _: api.ib_ulint_t,
    _: i32,
    _: i32,
) callconv(.c) i32 {
    const ctx = argFromOpaque(arg) orelse return 1;
    ctx.table_list_calls += 1;
    return 0;
}

fn visit_table(
    arg: api.ib_opaque_t,
    _: [*]const u8,
    _: api.ib_tbl_fmt_t,
    _: api.ib_ulint_t,
    n_cols: i32,
    n_indexes: i32,
) callconv(.c) i32 {
    const ctx = argFromOpaque(arg) orelse return 1;
    ctx.table_calls += 1;
    ctx.cur_col = 0;
    ctx.n_table_cols = n_cols;
    ctx.n_indexes = n_indexes;

    const short_name = tableShortName(ctx.table_name);
    return if (appendFmt(ctx, "CREATE TABLE {s}(\n", .{short_name})) 0 else 1;
}

fn visit_table_col(
    arg: api.ib_opaque_t,
    _: [*]const u8,
    col_type: api.ib_col_type_t,
    len: api.ib_ulint_t,
    attr: api.ib_col_attr_t,
) callconv(.c) i32 {
    const ctx = argFromOpaque(arg) orelse return 1;

    if (ctx.cur_col > 0 and col_type != .IB_SYS) {
        if (!appendFmt(ctx, ",\n", .{})) {
            return 1;
        }
    }

    ctx.cur_col += 1;

    if (col_type == .IB_SYS) {
        if (ctx.cur_col == ctx.n_table_cols) {
            if (!appendFmt(ctx, ");\n", .{})) {
                return 1;
            }
        }
        return 0;
    }

    ctx.table_col_calls += 1;
    const col_name = columnDisplayName(ctx);
    if (!appendFmt(ctx, "\t{s:<16}\t", .{col_name})) {
        return 1;
    }

    switch (col_type) {
        .IB_VARCHAR, .IB_VARCHAR_ANYCHARSET => {
            if (len > 0) {
                if (!appendFmt(ctx, "VARCHAR({d})", .{len})) {
                    return 1;
                }
            } else {
                if (!appendFmt(ctx, "TEXT", .{})) {
                    return 1;
                }
            }
        },
        .IB_CHAR, .IB_CHAR_ANYCHARSET => {
            if (!appendFmt(ctx, "CHAR({d})", .{len})) {
                return 1;
            }
        },
        .IB_BINARY => {
            if (!appendFmt(ctx, "BINARY({d})", .{len})) {
                return 1;
            }
        },
        .IB_VARBINARY => {
            if (len > 0) {
                if (!appendFmt(ctx, "VARBINARY({d})", .{len})) {
                    return 1;
                }
            } else {
                if (!appendFmt(ctx, "VARBINARY", .{})) {
                    return 1;
                }
            }
        },
        .IB_BLOB => {
            if (!appendFmt(ctx, "BLOB", .{})) {
                return 1;
            }
        },
        .IB_INT => {
            if (hasAttr(attr, .IB_COL_UNSIGNED)) {
                if (!appendFmt(ctx, "UNSIGNED ", .{})) {
                    return 1;
                }
            }
            switch (len) {
                1 => if (!appendFmt(ctx, "TINYINT", .{})) return 1,
                2 => if (!appendFmt(ctx, "SMALLINT", .{})) return 1,
                4 => if (!appendFmt(ctx, "INT", .{})) return 1,
                8 => if (!appendFmt(ctx, "BIGINT", .{})) return 1,
                else => return 1,
            }
        },
        .IB_FLOAT => if (!appendFmt(ctx, "FLOAT", .{})) return 1,
        .IB_DOUBLE => if (!appendFmt(ctx, "DOUBLE", .{})) return 1,
        .IB_DECIMAL => if (!appendFmt(ctx, "DECIMAL", .{})) return 1,
        else => if (!appendFmt(ctx, "UNKNOWN", .{})) return 1,
    }

    if (hasAttr(attr, .IB_COL_NOT_NULL)) {
        if (!appendFmt(ctx, " NOT NULL", .{})) {
            return 1;
        }
    }

    return 0;
}

fn visit_index(
    arg: api.ib_opaque_t,
    _: [*]const u8,
    _: api.ib_bool_t,
    unique: api.ib_bool_t,
    n_cols: i32,
) callconv(.c) i32 {
    const ctx = argFromOpaque(arg) orelse return 1;
    ctx.index_calls += 1;
    ctx.cur_col = 0;
    ctx.n_index_cols = n_cols;

    const short_name = tableShortName(ctx.table_name);
    const unique_tag = if (unique == compat.IB_TRUE) "UNIQUE" else "";
    if (!appendFmt(ctx, "CREATE {s} INDEX PRIMARY ON {s}(", .{ unique_tag, short_name })) {
        return 1;
    }
    return 0;
}

fn visit_index_col(
    arg: api.ib_opaque_t,
    _: [*]const u8,
    _: api.ib_ulint_t,
) callconv(.c) i32 {
    const ctx = argFromOpaque(arg) orelse return 1;
    ctx.cur_col += 1;
    ctx.index_col_calls += 1;

    const col_name = indexColumnDisplayName(ctx);
    if (!appendFmt(ctx, "{s}", .{col_name})) {
        return 1;
    }

    if (ctx.cur_col < ctx.n_index_cols) {
        if (!appendFmt(ctx, ",", .{})) {
            return 1;
        }
    } else {
        if (!appendFmt(ctx, ");\n", .{})) {
            return 1;
        }
    }

    return 0;
}

const table_visitor = api.ib_schema_visitor_t{
    .version = .IB_SCHEMA_VISITOR_TABLE_AND_INDEX_COL,
    .table = visit_table_list,
    .table_col = null,
    .index = null,
    .index_col = null,
};

const visitor = api.ib_schema_visitor_t{
    .version = .IB_SCHEMA_VISITOR_TABLE_AND_INDEX_COL,
    .table = visit_table,
    .table_col = visit_table_col,
    .index = visit_index,
    .index_col = visit_index_col,
};

fn visit_tables_list(arg: api.ib_opaque_t, name: [*]const u8, name_len: i32) callconv(.c) i32 {
    const ctx = argFromOpaque(arg) orelse return 1;
    const slice = name[0..@intCast(name_len)];
    ctx.table_name = slice;
    const err = api.ib_table_schema_visit(ctx.trx, slice, &table_visitor, arg);
    return if (err == .DB_SUCCESS) 0 else -1;
}

fn visit_tables(arg: api.ib_opaque_t, name: [*]const u8, name_len: i32) callconv(.c) i32 {
    const ctx = argFromOpaque(arg) orelse return 1;
    const slice = name[0..@intCast(name_len)];
    ctx.table_name = slice;
    ctx.had_error = false;
    const err = api.ib_table_schema_visit(ctx.trx, slice, &visitor, arg);
    if (err != .DB_SUCCESS or ctx.had_error) {
        return -1;
    }
    return 0;
}

fn createDatabase() !void {
    try std.testing.expectEqual(compat.IB_TRUE, api.ib_database_create(DATABASE));
}

fn createSystemTable(name: []const u8) !void {
    var tbl_sch: api.ib_tbl_sch_t = null;
    try expectOk(api.ib_table_schema_create(name, &tbl_sch, .IB_TBL_COMPACT, 0));
    defer api.ib_table_schema_delete(tbl_sch);

    try expectOk(api.ib_table_schema_add_col(
        tbl_sch,
        "ID",
        .IB_INT,
        .IB_COL_UNSIGNED,
        0,
        @sizeOf(api.ib_u32_t),
    ));

    var idx_sch: api.ib_idx_sch_t = null;
    try expectOk(api.ib_table_schema_add_index(tbl_sch, "PRIMARY", &idx_sch));
    try expectOk(api.ib_index_schema_add_col(idx_sch, "ID", 0));
    try expectOk(api.ib_index_schema_set_clustered(idx_sch));

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);
    try expectOk(api.ib_schema_lock_exclusive(trx));
    var table_id: api.ib_id_t = 0;
    try expectOk(api.ib_table_create(trx, tbl_sch, &table_id));
    try expectOk(api.ib_trx_commit(trx));
}

fn createSystemTables() !void {
    for (SYSTEM_TABLES) |name| {
        try createSystemTable(name);
    }
}

fn createTable(dbname: []const u8, name: []const u8, n: usize) !void {
    const max_len: usize = @intCast(api.IB_MAX_TABLE_NAME_LEN);
    var table_name_buf: [max_len]u8 = undefined;
    const table_name = try std.fmt.bufPrint(&table_name_buf, "{s}/{s}{d}", .{ dbname, name, n });

    var tbl_sch: api.ib_tbl_sch_t = null;
    try expectOk(api.ib_table_schema_create(table_name, &tbl_sch, .IB_TBL_COMPACT, 0));
    defer api.ib_table_schema_delete(tbl_sch);

    try expectOk(api.ib_table_schema_add_col(tbl_sch, "C1", .IB_VARCHAR, .IB_COL_NONE, 0, 10));
    try expectOk(api.ib_table_schema_add_col(tbl_sch, "C2", .IB_VARCHAR, .IB_COL_NONE, 0, 10));
    try expectOk(api.ib_table_schema_add_col(
        tbl_sch,
        "C3",
        .IB_INT,
        .IB_COL_UNSIGNED,
        0,
        @sizeOf(api.ib_u32_t),
    ));

    var idx_sch: api.ib_idx_sch_t = null;
    try expectOk(api.ib_table_schema_add_index(tbl_sch, "PRIMARY", &idx_sch));
    try expectOk(api.ib_index_schema_add_col(idx_sch, "C1", 0));
    try expectOk(api.ib_index_schema_add_col(idx_sch, "C2", 0));
    try expectOk(api.ib_index_schema_set_clustered(idx_sch));

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);
    try expectOk(api.ib_schema_lock_exclusive(trx));
    var table_id: api.ib_id_t = 0;
    try expectOk(api.ib_table_create(trx, tbl_sch, &table_id));
    try expectOk(api.ib_trx_commit(trx));
}

fn visitTableSchema(trx: api.ib_trx_t, name: []const u8, arg: *VisitorArg) api.ib_err_t {
    arg.table_name = name;
    const opaque_arg: api.ib_opaque_t = @as(*anyopaque, @ptrCast(arg));
    return api.ib_table_schema_visit(trx, name, &visitor, opaque_arg);
}

fn visitSysTables() !void {
    var arg = VisitorArg.init(std.testing.allocator);
    defer arg.deinit();

    const trx = api.ib_trx_begin(.IB_TRX_SERIALIZABLE) orelse return error.OutOfMemory;
    arg.trx = trx;

    const opaque_arg: api.ib_opaque_t = @as(*anyopaque, @ptrCast(&arg));

    try std.testing.expectEqual(
        api.ib_err_t.DB_SCHEMA_NOT_LOCKED,
        api.ib_table_schema_visit(trx, "SYS_TABLES", &visitor, opaque_arg),
    );

    try expectOk(api.ib_schema_lock_exclusive(trx));
    try expectOk(visitTableSchema(trx, "SYS_TABLES", &arg));
    try expectOk(visitTableSchema(trx, "SYS_COLUMNS", &arg));
    try expectOk(visitTableSchema(trx, "SYS_TABLES", &arg));
    try expectOk(visitTableSchema(trx, "SYS_INDEXES", &arg));
    try expectOk(api.ib_trx_commit(trx));
}

fn printEntireSchema() !VisitorArg {
    var arg = VisitorArg.init(std.testing.allocator);

    const trx = api.ib_trx_begin(.IB_TRX_SERIALIZABLE) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);
    arg.trx = trx;

    try expectOk(api.ib_schema_lock_exclusive(trx));

    const opaque_arg: api.ib_opaque_t = @as(*anyopaque, @ptrCast(&arg));
    try expectOk(api.ib_schema_tables_iterate(trx, visit_tables_list, opaque_arg));
    try expectOk(api.ib_schema_tables_iterate(trx, visit_tables, opaque_arg));
    try expectOk(api.ib_trx_commit(trx));

    return arg;
}

fn dropTable(dbname: []const u8, name: []const u8, n: usize) !void {
    const max_len: usize = @intCast(api.IB_MAX_TABLE_NAME_LEN);
    var table_name_buf: [max_len]u8 = undefined;
    const table_name = try std.fmt.bufPrint(&table_name_buf, "{s}/{s}{d}", .{ dbname, name, n });

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.OutOfMemory;
    errdefer _ = api.ib_trx_rollback(trx);
    try expectOk(api.ib_schema_lock_exclusive(trx));
    try expectOk(api.ib_table_drop(trx, table_name));
    try expectOk(api.ib_trx_commit(trx));
}

test "ib dict harness" {
    try expectOk(api.ib_init());
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
    try expectOk(api.ib_startup("barracuda"));

    try createSystemTables();
    try visitSysTables();
    try createDatabase();

    for (0..TABLE_COUNT) |i| {
        try createTable(DATABASE, TABLE_PREFIX, i);
    }

    var schema = try printEntireSchema();
    defer schema.deinit();

    const sys_tables = SYSTEM_TABLES.len;
    const user_tables = TABLE_COUNT;
    const total_tables = sys_tables + user_tables;
    const total_cols = sys_tables * 1 + user_tables * 3;
    const total_indexes = sys_tables * 1 + user_tables * 1;
    const total_index_cols = sys_tables * 1 + user_tables * 2;

    try std.testing.expectEqual(@as(usize, total_tables), schema.table_list_calls);
    try std.testing.expectEqual(@as(usize, total_tables), schema.table_calls);
    try std.testing.expectEqual(@as(usize, total_cols), schema.table_col_calls);
    try std.testing.expectEqual(@as(usize, total_indexes), schema.index_calls);
    try std.testing.expectEqual(@as(usize, total_index_cols), schema.index_col_calls);

    try std.testing.expect(std.mem.indexOf(u8, schema.output.items, "CREATE TABLE t0") != null);
    try std.testing.expect(std.mem.indexOf(u8, schema.output.items, "INDEX PRIMARY ON t0") != null);

    for (0..TABLE_COUNT) |i| {
        try dropTable(DATABASE, TABLE_PREFIX, i);
    }
}
