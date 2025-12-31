const std = @import("std");
const api = @import("../api/api.zig");
const compat = @import("../ut/compat.zig");

fn print(comptime fmt: []const u8, args: anytype) void {
    std.debug.print(fmt, args);
}

test "demo: full InnoDB workflow" {
    print("\n\n=== InnoDB-Zig Test Harness Demo ===\n\n", .{});

    // Initialize InnoDB
    print("1. Initializing InnoDB engine...\n", .{});
    _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_init());
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_startup("barracuda"));
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
    print("   InnoDB started with 'barracuda' format\n", .{});

    // Create database
    print("\n2. Creating database 'demo'...\n", .{});
    try std.testing.expectEqual(compat.IB_TRUE, api.ib_database_create("demo"));
    print("   Database 'demo' created\n", .{});
    defer _ = api.ib_database_drop("demo");

    // Create table schema
    print("\n3. Creating table schema 'demo/users'...\n", .{});
    var tbl_sch: api.ib_tbl_sch_t = null;
    var idx_sch: api.ib_idx_sch_t = null;

    try std.testing.expectEqual(
        api.ib_err_t.DB_SUCCESS,
        api.ib_table_schema_create("demo/users", &tbl_sch, .IB_TBL_COMPACT, 0),
    );
    defer api.ib_table_schema_delete(tbl_sch);

    // Add columns
    print("   Adding columns:\n", .{});
    try std.testing.expectEqual(
        api.ib_err_t.DB_SUCCESS,
        api.ib_table_schema_add_col(tbl_sch, "id", .IB_INT, .IB_COL_UNSIGNED, 0, 4),
    );
    print("     - id (INT UNSIGNED)\n", .{});
    try std.testing.expectEqual(
        api.ib_err_t.DB_SUCCESS,
        api.ib_table_schema_add_col(tbl_sch, "name", .IB_VARCHAR, .IB_COL_NONE, 0, 64),
    );
    print("     - name (VARCHAR 64)\n", .{});
    try std.testing.expectEqual(
        api.ib_err_t.DB_SUCCESS,
        api.ib_table_schema_add_col(tbl_sch, "age", .IB_INT, .IB_COL_UNSIGNED, 0, 4),
    );
    print("     - age (INT UNSIGNED)\n", .{});

    // Add clustered index
    try std.testing.expectEqual(
        api.ib_err_t.DB_SUCCESS,
        api.ib_table_schema_add_index(tbl_sch, "PRIMARY", &idx_sch),
    );
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_index_schema_add_col(idx_sch, "id", 0));
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_index_schema_set_clustered(idx_sch));
    print("   Primary key on 'id' (clustered)\n", .{});

    // Create table in transaction
    print("\n4. Creating table in transaction...\n", .{});
    const create_trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.TrxFailed;
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_schema_lock_exclusive(create_trx));
    var table_id: api.ib_id_t = 0;
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_table_create(create_trx, tbl_sch, &table_id));
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_trx_commit(create_trx));
    print("   Table created with ID: {d}\n", .{table_id});

    // Insert rows
    print("\n5. Inserting rows...\n", .{});
    const insert_trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.TrxFailed;
    errdefer _ = api.ib_trx_rollback(insert_trx);

    var crsr: api.ib_crsr_t = null;
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_cursor_open_table("demo/users", insert_trx, &crsr));
    defer _ = api.ib_cursor_close(crsr);
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_cursor_lock(crsr, .IB_LOCK_IX));

    const tpl = api.ib_clust_read_tuple_create(crsr) orelse return error.TupleFailed;
    defer api.ib_tuple_delete(tpl);

    const users = [_]struct { id: u32, name: []const u8, age: u32 }{
        .{ .id = 1, .name = "Alice", .age = 30 },
        .{ .id = 2, .name = "Bob", .age = 25 },
        .{ .id = 3, .name = "Charlie", .age = 35 },
    };

    for (users) |user| {
        try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_col_set_value(tpl, 0, &user.id, 4));
        try std.testing.expectEqual(
            api.ib_err_t.DB_SUCCESS,
            api.ib_col_set_value(tpl, 1, user.name.ptr, @intCast(user.name.len)),
        );
        try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_col_set_value(tpl, 2, &user.age, 4));
        try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_cursor_insert_row(crsr, tpl));
        print("   Inserted: id={d}, name=\"{s}\", age={d}\n", .{ user.id, user.name, user.age });
        _ = api.ib_tuple_clear(tpl);
    }

    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_trx_commit(insert_trx));
    print("   Transaction committed\n", .{});

    // Scan and read rows
    print("\n6. Scanning table...\n", .{});
    const read_trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.TrxFailed;
    defer _ = api.ib_trx_rollback(read_trx);

    var read_crsr: api.ib_crsr_t = null;
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_cursor_open_table("demo/users", read_trx, &read_crsr));
    defer _ = api.ib_cursor_close(read_crsr);

    const read_tpl = api.ib_clust_read_tuple_create(read_crsr) orelse return error.TupleFailed;
    defer api.ib_tuple_delete(read_tpl);

    var row_count: usize = 0;
    var err = api.ib_cursor_first(read_crsr);
    while (err == .DB_SUCCESS) {
        err = api.ib_cursor_read_row(read_crsr, read_tpl);
        if (err != .DB_SUCCESS) break;

        var id: api.ib_u32_t = 0;
        var age: api.ib_u32_t = 0;
        try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_tuple_read_u32(read_tpl, 0, &id));
        try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_tuple_read_u32(read_tpl, 2, &age));

        const name_ptr = api.ib_col_get_value(read_tpl, 1);
        const name_len = api.ib_col_get_len(read_tpl, 1);
        if (name_ptr != null and name_len > 0 and name_len != api.IB_SQL_NULL) {
            const name = @as([*]const u8, @ptrCast(name_ptr.?))[0..@intCast(name_len)];
            print("   Row {d}: id={d}, name=\"{s}\", age={d}\n", .{ row_count + 1, id, name, age });
        }

        row_count += 1;
        err = api.ib_cursor_next(read_crsr);
        _ = api.ib_tuple_clear(read_tpl);
    }
    print("   Total rows: {d}\n", .{row_count});
    try std.testing.expectEqual(@as(usize, 3), row_count);

    // Cleanup
    print("\n7. Dropping table...\n", .{});
    const drop_trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return error.TrxFailed;
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_schema_lock_exclusive(drop_trx));
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_table_drop(drop_trx, "demo/users"));
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_trx_commit(drop_trx));
    print("   Table dropped\n", .{});

    print("\n=== Demo Complete ===\n\n", .{});
}
