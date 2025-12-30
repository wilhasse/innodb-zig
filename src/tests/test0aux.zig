const std = @import("std");
const api = @import("../api/api.zig");
const compat = @import("../ut/compat.zig");

pub const String = struct {
    ptr: []u8,
};

pub const Var = struct {
    name: []u8,
    value: []u8,
};

pub const Config = struct {
    allocator: std.mem.Allocator,
    vars: std.ArrayList(Var),

    pub fn init(allocator: std.mem.Allocator) Config {
        return .{ .allocator = allocator, .vars = std.ArrayList(Var).init(allocator) };
    }

    pub fn deinit(self: *Config) void {
        for (self.vars.items) |var_item| {
            self.allocator.free(var_item.name);
            self.allocator.free(var_item.value);
        }
        self.vars.deinit();
    }
};

fn expectOk(err: api.ib_err_t) !void {
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
}

pub fn readIntFromTuple(tpl: api.ib_tpl_t, meta: *const api.ib_col_meta_t, col: api.ib_ulint_t) api.ib_u64_t {
    var out: api.ib_u64_t = 0;
    switch (meta.type_len) {
        1 => {
            var v: api.ib_u8_t = 0;
            _ = api.ib_col_copy_value(tpl, col, &v, @sizeOf(api.ib_u8_t));
            if ((@intFromEnum(meta.attr) & @intFromEnum(api.ib_col_attr_t.IB_COL_UNSIGNED)) != 0) {
                out = v;
            } else {
                const signed: i64 = @as(i64, @as(api.ib_i8_t, @bitCast(v)));
                out = @as(api.ib_u64_t, @bitCast(signed));
            }
        },
        2 => {
            var v: api.ib_u16_t = 0;
            _ = api.ib_col_copy_value(tpl, col, &v, @sizeOf(api.ib_u16_t));
            if ((@intFromEnum(meta.attr) & @intFromEnum(api.ib_col_attr_t.IB_COL_UNSIGNED)) != 0) {
                out = v;
            } else {
                const signed: i64 = @as(i64, @as(api.ib_i16_t, @bitCast(v)));
                out = @as(api.ib_u64_t, @bitCast(signed));
            }
        },
        4 => {
            var v: api.ib_u32_t = 0;
            _ = api.ib_col_copy_value(tpl, col, &v, @sizeOf(api.ib_u32_t));
            if ((@intFromEnum(meta.attr) & @intFromEnum(api.ib_col_attr_t.IB_COL_UNSIGNED)) != 0) {
                out = v;
            } else {
                const signed: i64 = @as(i64, @as(api.ib_i32_t, @bitCast(v)));
                out = @as(api.ib_u64_t, @bitCast(signed));
            }
        },
        8 => {
            _ = api.ib_col_copy_value(tpl, col, &out, @sizeOf(api.ib_u64_t));
        },
        else => {},
    }
    return out;
}

pub fn printTuple(writer: anytype, tpl: api.ib_tpl_t) !void {
    const n_cols: usize = @intCast(api.ib_tuple_get_n_cols(tpl));
    var i: usize = 0;
    while (i < n_cols) : (i += 1) {
        var meta: api.ib_col_meta_t = undefined;
        const data_len = api.ib_col_get_meta(tpl, @intCast(i), &meta);
        if (meta.type == .IB_SYS) {
            continue;
        } else if (data_len == api.IB_SQL_NULL) {
            try writer.writeAll("|");
            continue;
        }

        switch (meta.type) {
            .IB_INT => try printIntCol(writer, tpl, @intCast(i), &meta),
            .IB_FLOAT => {
                var v: f32 = 0;
                try expectOk(api.ib_tuple_read_float(tpl, @intCast(i), &v));
                try writer.print("{d}", .{v});
            },
            .IB_DOUBLE => {
                var v: f64 = 0;
                try expectOk(api.ib_tuple_read_double(tpl, @intCast(i), &v));
                try writer.print("{d}", .{v});
            },
            .IB_CHAR, .IB_BLOB, .IB_DECIMAL, .IB_VARCHAR => {
                const ptr = api.ib_col_get_value(tpl, @intCast(i)) orelse {
                    try writer.writeAll("|");
                    continue;
                };
                const slice = @as([*]const u8, @ptrCast(ptr))[0..@intCast(data_len)];
                try writer.print("{d}:{s}", .{ data_len, slice });
            },
            else => {},
        }
        try writer.writeAll("|");
    }
    try writer.writeAll("\n");
}

fn printIntCol(writer: anytype, tpl: api.ib_tpl_t, col: api.ib_ulint_t, meta: *const api.ib_col_meta_t) !void {
    const unsigned = (@intFromEnum(meta.attr) & @intFromEnum(api.ib_col_attr_t.IB_COL_UNSIGNED)) != 0;
    switch (meta.type_len) {
        1 => {
            if (unsigned) {
                var v: api.ib_u8_t = 0;
                try expectOk(api.ib_tuple_read_u8(tpl, col, &v));
                try writer.print("{d}", .{v});
            } else {
                var v: api.ib_i8_t = 0;
                try expectOk(api.ib_tuple_read_i8(tpl, col, &v));
                try writer.print("{d}", .{v});
            }
        },
        2 => {
            if (unsigned) {
                var v: api.ib_u16_t = 0;
                try expectOk(api.ib_tuple_read_u16(tpl, col, &v));
                try writer.print("{d}", .{v});
            } else {
                var v: api.ib_i16_t = 0;
                try expectOk(api.ib_tuple_read_i16(tpl, col, &v));
                try writer.print("{d}", .{v});
            }
        },
        4 => {
            if (unsigned) {
                var v: api.ib_u32_t = 0;
                try expectOk(api.ib_tuple_read_u32(tpl, col, &v));
                try writer.print("{d}", .{v});
            } else {
                var v: api.ib_i32_t = 0;
                try expectOk(api.ib_tuple_read_i32(tpl, col, &v));
                try writer.print("{d}", .{v});
            }
        },
        8 => {
            if (unsigned) {
                var v: api.ib_u64_t = 0;
                try expectOk(api.ib_tuple_read_u64(tpl, col, &v));
                try writer.print("{d}", .{v});
            } else {
                var v: api.ib_i64_t = 0;
                try expectOk(api.ib_tuple_read_i64(tpl, col, &v));
                try writer.print("{d}", .{v});
            }
        },
        else => {},
    }
}

pub fn testConfigure() !void {
    const log_dir = "log";
    std.fs.cwd().makePath(log_dir) catch {};

    try expectOk(api.ib_cfg_set("flush_method", "O_DIRECT"));
    try expectOk(api.ib_cfg_set("log_files_in_group", @as(api.ib_ulint_t, 2)));
    try expectOk(api.ib_cfg_set("log_file_size", @as(api.ib_ulint_t, 32 * 1024 * 1024)));
    try expectOk(api.ib_cfg_set("log_buffer_size", @as(api.ib_ulint_t, 24 * 16384)));
    try expectOk(api.ib_cfg_set("buffer_pool_size", @as(api.ib_ulint_t, 5 * 1024 * 1024)));
    try expectOk(api.ib_cfg_set("additional_mem_pool_size", @as(api.ib_ulint_t, 4 * 1024 * 1024)));
    try expectOk(api.ib_cfg_set("flush_log_at_trx_commit", @as(api.ib_ulint_t, 1)));
    try expectOk(api.ib_cfg_set("file_io_threads", @as(api.ib_ulint_t, 4)));
    try expectOk(api.ib_cfg_set("lock_wait_timeout", @as(api.ib_ulint_t, 60)));
    try expectOk(api.ib_cfg_set("open_files", @as(api.ib_ulint_t, 300)));
    try expectOk(api.ib_cfg_set("doublewrite", compat.IB_TRUE));
    try expectOk(api.ib_cfg_set("checksums", compat.IB_TRUE));
    try expectOk(api.ib_cfg_set("rollback_on_timeout", compat.IB_TRUE));
    try expectOk(api.ib_cfg_set("print_verbose_log", compat.IB_TRUE));
    try expectOk(api.ib_cfg_set("file_per_table", compat.IB_TRUE));
    try expectOk(api.ib_cfg_set("data_home_dir", "./"));
    try expectOk(api.ib_cfg_set("log_group_home_dir", log_dir));
    try expectOk(api.ib_cfg_set("data_file_path", "ibdata1:32M:autoextend"));
}

pub fn genRandText(buf: []u8, max_size: usize) []u8 {
    std.debug.assert(max_size > 1);
    std.debug.assert(max_size <= buf.len);
    const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    const len = 1 + std.crypto.random.intRangeAtMost(usize, 0, max_size - 2);
    for (buf[0..len]) |*ch| {
        ch.* = charset[std.crypto.random.intRangeAtMost(usize, 0, charset.len - 1)];
    }
    return buf[0..len];
}

pub fn setGlobalOption(opt: u32, arg: []const u8) api.ib_err_t {
    switch (opt) {
        1 => {
            const size = std.fmt.parseInt(api.ib_ulint_t, arg, 10) catch return .DB_INVALID_INPUT;
            return api.ib_cfg_set("buffer_pool_size", size * 1024 * 1024);
        },
        2 => {
            const size = std.fmt.parseInt(api.ib_ulint_t, arg, 10) catch return .DB_INVALID_INPUT;
            return api.ib_cfg_set("log_file_size", size * 1024 * 1024);
        },
        3 => return api.ib_cfg_set("adaptive_hash_index", compat.IB_FALSE),
        4 => {
            const size = std.fmt.parseInt(api.ib_ulint_t, arg, 10) catch return .DB_INVALID_INPUT;
            return api.ib_cfg_set("io_capacity", size);
        },
        5 => return api.ib_cfg_set("use_sys_malloc", compat.IB_TRUE),
        6 => {
            const pct = std.fmt.parseInt(api.ib_ulint_t, arg, 10) catch return .DB_INVALID_INPUT;
            return api.ib_cfg_set("lru_old_blocks_pct", pct);
        },
        7 => {
            const pct = std.fmt.parseInt(api.ib_ulint_t, arg, 10) catch return .DB_INVALID_INPUT;
            return api.ib_cfg_set("lru_block_access_recency", pct);
        },
        8 => {
            const level = std.fmt.parseInt(api.ib_ulint_t, arg, 10) catch return .DB_INVALID_INPUT;
            return api.ib_cfg_set("force_recovery", level);
        },
        9 => return api.ib_cfg_set("log_group_home_dir", arg),
        10 => return api.ib_cfg_set("data_home_dir", arg),
        11 => return api.ib_cfg_set("data_file_path", arg),
        12 => return api.ib_cfg_set("doublewrite", compat.IB_FALSE),
        13 => return api.ib_cfg_set("checksums", compat.IB_FALSE),
        14 => return api.ib_cfg_set("file_per_table", compat.IB_FALSE),
        15 => {
            const level = std.fmt.parseInt(api.ib_ulint_t, arg, 10) catch return .DB_INVALID_INPUT;
            return api.ib_cfg_set("flush_log_at_trx_commit", level);
        },
        16 => return api.ib_cfg_set("flush_method", arg),
        17 => {
            const threads = std.fmt.parseInt(api.ib_ulint_t, arg, 10) catch return .DB_INVALID_INPUT;
            return api.ib_cfg_set("read_io_threads", threads);
        },
        18 => {
            const threads = std.fmt.parseInt(api.ib_ulint_t, arg, 10) catch return .DB_INVALID_INPUT;
            return api.ib_cfg_set("write_io_threads", threads);
        },
        19 => {
            const count = std.fmt.parseInt(api.ib_ulint_t, arg, 10) catch return .DB_INVALID_INPUT;
            return api.ib_cfg_set("open_files", count);
        },
        20 => {
            const secs = std.fmt.parseInt(api.ib_ulint_t, arg, 10) catch return .DB_INVALID_INPUT;
            return api.ib_cfg_set("lock_wait_timeout", secs);
        },
        else => return .DB_ERROR,
    }
}

pub fn printUsage(writer: anytype, progname: []const u8) !void {
    try writer.print(
        "usage: {s} [--ib-buffer-pool-size size in mb] [--ib-log-file-size size in mb]\n",
        .{progname},
    );
}

pub fn printVersion(writer: anytype) !void {
    const version = api.ib_api_version();
    try writer.print(
        "API: {d}.{d}.{d}\n",
        .{
            @as(u32, @intCast(version >> 32)),
            @as(u32, @intCast((version >> 16) & 0xffff)),
            @as(u32, @intCast(version & 0xffff)),
        },
    );
}

pub fn configParseFile(allocator: std.mem.Allocator, filename: []const u8, config: *Config) !void {
    var file = try std.fs.cwd().openFile(filename, .{});
    defer file.close();

    const contents = try file.readToEndAlloc(allocator, 1024 * 1024);
    defer allocator.free(contents);

    var it = std.mem.splitScalar(u8, contents, '\n');
    while (it.next()) |line| {
        var trimmed = std.mem.trim(u8, line, " \t\r");
        if (trimmed.len == 0) continue;
        if (trimmed[0] == '#') continue;
        if (std.mem.indexOfScalar(u8, trimmed, '#')) |idx| {
            trimmed = std.mem.trim(u8, trimmed[0..idx], " \t\r");
        }
        if (trimmed.len == 0) continue;
        const eq = std.mem.indexOfScalar(u8, trimmed, '=') orelse continue;
        const name = std.mem.trim(u8, trimmed[0..eq], " \t");
        const value = std.mem.trim(u8, trimmed[eq + 1 ..], " \t");
        if (name.len == 0) continue;

        const name_copy = try allocator.alloc(u8, name.len);
        std.mem.copyForwards(u8, name_copy, name);
        const value_copy = try allocator.alloc(u8, value.len);
        std.mem.copyForwards(u8, value_copy, value);
        try config.vars.append(.{ .name = name_copy, .value = value_copy });
    }
}

pub fn configPrint(writer: anytype, config: *const Config) !void {
    for (config.vars.items) |var_item| {
        try writer.print("{s}={s}\n", .{ var_item.name, var_item.value });
    }
}

pub fn dropTable(dbname: []const u8, name: []const u8) api.ib_err_t {
    const max_len: usize = @intCast(api.IB_MAX_TABLE_NAME_LEN);
    var buf: [max_len]u8 = undefined;
    const table_name = std.fmt.bufPrint(&buf, "{s}/{s}", .{ dbname, name }) catch return .DB_ERROR;

    const trx = api.ib_trx_begin(.IB_TRX_REPEATABLE_READ) orelse return .DB_OUT_OF_MEMORY;
    errdefer _ = api.ib_trx_rollback(trx);
    if (api.ib_schema_lock_exclusive(trx) != .DB_SUCCESS) {
        _ = api.ib_trx_rollback(trx);
        return .DB_ERROR;
    }
    if (api.ib_table_drop(trx, table_name) != .DB_SUCCESS) {
        _ = api.ib_trx_rollback(trx);
        return .DB_ERROR;
    }
    return api.ib_trx_commit(trx);
}

test "test0aux helpers" {
    _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);
    try expectOk(api.ib_init());
    try testConfigure();
    try expectOk(api.ib_shutdown(.IB_SHUTDOWN_NORMAL));

    var buf: [32]u8 = undefined;
    const out = genRandText(&buf, buf.len);
    try std.testing.expect(out.len > 0);
    try std.testing.expect(out.len < buf.len);
}
