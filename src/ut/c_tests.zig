const std = @import("std");

pub const default_tests = [_][]const u8{
    "ib_cfg",
    "ib_cursor",
    "ib_ddl",
    "ib_dict",
    "ib_drop",
    "ib_logger",
    "ib_mt_drv",
    "ib_shutdown",
    "ib_status",
    "ib_tablename",
    "ib_test1",
    "ib_test2",
    "ib_test3",
    "ib_test5",
    "ib_types",
    "ib_update",
};

pub const stress_tests = [_][]const u8{
    "ib_mt_stress",
    "ib_perf1",
};

pub fn contains(list: []const []const u8, name: []const u8) bool {
    for (list) |item| {
        if (std.mem.eql(u8, item, name)) {
            return true;
        }
    }
    return false;
}

pub fn matchesFilter(name: []const u8, filter: ?[]const u8) bool {
    if (filter) |f| {
        return std.mem.indexOf(u8, name, f) != null;
    }
    return true;
}

test "c test lists are unique and non-empty" {
    try std.testing.expect(default_tests.len > 0);
    try std.testing.expect(stress_tests.len > 0);

    for (default_tests, 0..) |name, i| {
        for (default_tests[0..i]) |prev| {
            try std.testing.expect(!std.mem.eql(u8, name, prev));
        }
    }

    for (stress_tests, 0..) |name, i| {
        for (stress_tests[0..i]) |prev| {
            try std.testing.expect(!std.mem.eql(u8, name, prev));
        }
    }
}

test "c test filter match and contains" {
    try std.testing.expect(contains(&default_tests, "ib_test1"));
    try std.testing.expect(!contains(&default_tests, "ib_missing"));
    try std.testing.expect(matchesFilter("ib_test1", "test"));
    try std.testing.expect(!matchesFilter("ib_cfg", "test"));
    try std.testing.expect(matchesFilter("ib_cfg", null));
}
