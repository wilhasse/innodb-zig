const std = @import("std");
const api = @import("../api/api.zig");

const ArrayList = std.array_list.Managed;

const ConfigVar = struct {
    name: []u8,
    value: ?[]u8,
};

const Config = struct {
    allocator: std.mem.Allocator,
    vars: ArrayList(ConfigVar),

    fn init(allocator: std.mem.Allocator) Config {
        return .{ .allocator = allocator, .vars = ArrayList(ConfigVar).init(allocator) };
    }

    fn deinit(self: *Config) void {
        for (self.vars.items) |item| {
            self.allocator.free(item.name);
            if (item.value) |value| {
                self.allocator.free(value);
            }
        }
        self.vars.deinit();
    }
};

fn configAdd(cfg: *Config, key: []const u8, val: []const u8) !void {
    if (key.len == 0) {
        return;
    }
    const name = try cfg.allocator.alloc(u8, key.len);
    std.mem.copyForwards(u8, name, key);

    var value_copy: ?[]u8 = null;
    if (val.len > 0) {
        const value = try cfg.allocator.alloc(u8, val.len);
        std.mem.copyForwards(u8, value, val);
        value_copy = value;
    }

    try cfg.vars.append(.{ .name = name, .value = value_copy });
}

fn configParseBytes(allocator: std.mem.Allocator, bytes: []const u8) !Config {
    var cfg = Config.init(allocator);
    errdefer cfg.deinit();

    var key = ArrayList(u8).init(allocator);
    defer key.deinit();
    var val = ArrayList(u8).init(allocator);
    defer val.deinit();

    var in_value = false;
    var i: usize = 0;
    while (i < bytes.len) : (i += 1) {
        const ch = bytes[i];
        if (ch == '#') {
            while (i < bytes.len and bytes[i] != '\n') : (i += 1) {}
            if (i < bytes.len and bytes[i] == '\n') {
                try configAdd(&cfg, key.items, val.items);
                key.clearRetainingCapacity();
                val.clearRetainingCapacity();
                in_value = false;
            }
            continue;
        }

        if (ch != '\n' and std.ascii.isWhitespace(ch)) {
            continue;
        }

        switch (ch) {
            '\r' => {},
            '\n' => {
                try configAdd(&cfg, key.items, val.items);
                key.clearRetainingCapacity();
                val.clearRetainingCapacity();
                in_value = false;
            },
            '=' => {
                in_value = true;
            },
            else => {
                if (in_value) {
                    try val.append(ch);
                } else {
                    try key.append(ch);
                }
            },
        }
    }

    if (key.items.len > 0) {
        try configAdd(&cfg, key.items, val.items);
    }

    return cfg;
}

fn configParseFile(allocator: std.mem.Allocator, path: []const u8) !Config {
    const data = try std.fs.cwd().readFileAlloc(allocator, path, 1024 * 1024);
    defer allocator.free(data);
    return configParseBytes(allocator, data);
}

fn configPrint(cfg: *const Config, writer: anytype) !void {
    for (cfg.vars.items) |item| {
        if (item.value) |value| {
            try writer.print("{s}={s}\n", .{ item.name, value });
        } else {
            try writer.print("{s}\n", .{ item.name });
        }
    }
}

fn configParserSmoke() !void {
    const content =
        "# comment\n" ++
        "buffer_pool_size = 64M\n" ++
        "log_files_in_group = 2\n" ++
        "data_home_dir = ./\n";

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(.{ .sub_path = "test.conf", .data = content });
    const path = try tmp.dir.realpathAlloc(std.testing.allocator, "test.conf");
    defer std.testing.allocator.free(path);

    var cfg = try configParseFile(std.testing.allocator, path);
    defer cfg.deinit();

    try configPrint(&cfg, std.Io.null_writer);

    var saw_pool = false;
    var saw_group = false;
    var saw_home = false;
    for (cfg.vars.items) |item| {
        if (std.mem.eql(u8, item.name, "buffer_pool_size")) {
            saw_pool = true;
            try std.testing.expect(item.value != null);
            try std.testing.expect(std.mem.eql(u8, item.value.?, "64M"));
        } else if (std.mem.eql(u8, item.name, "log_files_in_group")) {
            saw_group = true;
            try std.testing.expect(item.value != null);
            try std.testing.expect(std.mem.eql(u8, item.value.?, "2"));
        } else if (std.mem.eql(u8, item.name, "data_home_dir")) {
            saw_home = true;
            try std.testing.expect(item.value != null);
            try std.testing.expect(std.mem.eql(u8, item.value.?, "./"));
        }
    }
    try std.testing.expect(saw_pool and saw_group and saw_home);
}

fn getAllCfg() !void {
    const names = [_][]const u8{
        "adaptive_hash_index",
        "additional_mem_pool_size",
        "autoextend_increment",
        "buffer_pool_size",
        "checksums",
        "data_file_path",
        "data_home_dir",
        "doublewrite",
        "file_format",
        "file_io_threads",
        "file_per_table",
        "flush_log_at_trx_commit",
        "flush_method",
        "force_recovery",
        "lock_wait_timeout",
        "log_buffer_size",
        "log_file_size",
        "log_files_in_group",
        "log_group_home_dir",
        "max_dirty_pages_pct",
        "max_purge_lag",
        "lru_old_blocks_pct",
        "lru_block_access_recency",
        "open_files",
        "pre_rollback_hook",
        "print_verbose_log",
        "rollback_on_timeout",
        "stats_sample_pages",
        "status_file",
        "sync_spin_loops",
        "version",
    };

    for (names) |name| {
        try cfgGetByType(name);
    }
}

fn cfgGetByType(name: []const u8) !void {
    var cfg_type: api.ib_cfg_type_t = undefined;
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_cfg_var_get_type(name, &cfg_type));

    switch (cfg_type) {
        .IB_CFG_IBOOL => {
            var val: api.ib_bool_t = 0;
            try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_cfg_get(name, &val));
        },
        .IB_CFG_ULINT, .IB_CFG_ULONG => {
            var val: api.ib_ulint_t = 0;
            try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_cfg_get(name, &val));
        },
        .IB_CFG_TEXT => {
            var val: []const u8 = "";
            try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_cfg_get(name, &val));
        },
        .IB_CFG_CB => {
            var val: ?api.ib_cb_t = null;
            try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_cfg_get(name, &val));
        },
    }
}

fn testIbCfgGetAll() !void {
    var names: [][]const u8 = undefined;
    var names_num: api.ib_u32_t = 0;
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_cfg_get_all(&names, &names_num));
    defer std.heap.page_allocator.free(names);

    var i: api.ib_u32_t = 0;
    while (i < names_num) : (i += 1) {
        try cfgGetByType(names[@intCast(i)]);
    }
}

test "ib cfg harness" {
    try configParserSmoke();

    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_init());
    defer _ = api.ib_shutdown(.IB_SHUTDOWN_NORMAL);

    try testIbCfgGetAll();
    try getAllCfg();

    try std.testing.expectEqual(api.ib_err_t.DB_INVALID_INPUT, api.ib_cfg_set("data_home_dir", "/some/path"));
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_cfg_set("data_home_dir", "/some/path/"));

    var ptr: []const u8 = "";
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_cfg_get("data_home_dir", &ptr));
    try std.testing.expect(std.mem.eql(u8, ptr, "/some/path/"));

    try std.testing.expectEqual(
        api.ib_err_t.DB_SUCCESS,
        api.ib_cfg_set("buffer_pool_size", @as(api.ib_ulint_t, 0xFFFF_FFFF) - 5),
    );

    try std.testing.expectEqual(api.ib_err_t.DB_INVALID_INPUT, api.ib_cfg_set("flush_method", "fdatasync"));

    var i: api.ib_ulint_t = 0;
    while (i <= 100) : (i += 1) {
        const err = api.ib_cfg_set("lru_old_blocks_pct", i);
        if (i >= 5 and i <= 95) {
            try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, err);
            var val: api.ib_ulint_t = 0;
            try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_cfg_get("lru_old_blocks_pct", &val));
            try std.testing.expectEqual(i, val);
        } else {
            try std.testing.expectEqual(api.ib_err_t.DB_INVALID_INPUT, err);
        }
    }

    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_cfg_set("lru_block_access_recency", 123));
    var val: api.ib_ulint_t = 0;
    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_cfg_get("lru_block_access_recency", &val));
    try std.testing.expectEqual(@as(api.ib_ulint_t, 123), val);

    try std.testing.expectEqual(api.ib_err_t.DB_SUCCESS, api.ib_cfg_set("open_files", 123));
    try getAllCfg();
}
