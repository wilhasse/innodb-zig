const std = @import("std");
const compat = @import("../ut/compat.zig");
const fil = @import("../fil/mod.zig");

pub const module_name = "log.ddl";

pub const ibool = compat.ibool;
pub const ulint = compat.ulint;

pub const DdlOpType = enum(u8) {
    create = 'C',
    drop = 'D',
    rename = 'R',
};

pub const DdlOpState = enum(u8) {
    begin = 'B',
    end = 'E',
};

const DDL_LOG_FILE = "ddl_log.txt";

const PendingOp = struct {
    op: DdlOpType,
    name: []u8,
    new_name: ?[]u8 = null,
};

var ddl_log_path: []u8 = &[_]u8{};
var ddl_log_inited: bool = false;
var pending_ops = std.ArrayListUnmanaged(PendingOp){};

fn ddlLogPath(allocator: std.mem.Allocator) ?[]u8 {
    const base = fil.fil_path_to_client_datadir;
    const needs_sep = base.len > 0 and
        !std.mem.endsWith(u8, base, "/") and
        !std.mem.endsWith(u8, base, "\\");
    const sep = if (needs_sep) "/" else "";
    return std.fmt.allocPrint(allocator, "{s}{s}{s}", .{ base, sep, DDL_LOG_FILE }) catch null;
}

fn dupName(allocator: std.mem.Allocator, name: []const u8) ?[]u8 {
    if (name.len == 0) {
        return "";
    }
    const buf = allocator.alloc(u8, name.len) catch return null;
    std.mem.copyForwards(u8, buf, name);
    return buf;
}

fn freeName(name: []u8) void {
    if (name.len == 0) {
        return;
    }
    std.heap.page_allocator.free(name);
}

fn pendingClear() void {
    for (pending_ops.items) |item| {
        freeName(item.name);
        if (item.new_name) |new_name| {
            freeName(new_name);
        }
    }
    pending_ops.clearAndFree(std.heap.page_allocator);
}

pub fn ddl_log_init() ibool {
    if (ddl_log_inited) {
        return compat.TRUE;
    }
    ddl_log_inited = true;
    const allocator = std.heap.page_allocator;
    const path = ddlLogPath(allocator) orelse return compat.FALSE;
    ddl_log_path = path;
    if (fil.fil_path_to_client_datadir.len > 0) {
        std.fs.cwd().makePath(fil.fil_path_to_client_datadir) catch {};
    }
    return compat.TRUE;
}

pub fn ddl_log_shutdown() void {
    pendingClear();
    if (ddl_log_path.len != 0) {
        std.heap.page_allocator.free(ddl_log_path);
        ddl_log_path = &[_]u8{};
    }
    ddl_log_inited = false;
}

pub fn ddl_log_clear() void {
    pendingClear();
    if (!ddl_log_inited) {
        _ = ddl_log_init();
    }
    if (ddl_log_path.len == 0) {
        return;
    }
    std.fs.cwd().deleteFile(ddl_log_path) catch |err| {
        if (err != error.FileNotFound) {}
    };
}

fn ddlLogAppend(op: DdlOpType, state: DdlOpState, name: []const u8, new_name: ?[]const u8) ibool {
    if (!ddl_log_inited and ddl_log_init() == compat.FALSE) {
        return compat.FALSE;
    }
    if (ddl_log_path.len == 0) {
        return compat.FALSE;
    }
    if (op == .rename and new_name == null) {
        return compat.FALSE;
    }
    var file = std.fs.cwd().createFile(ddl_log_path, .{ .truncate = false }) catch return compat.FALSE;
    defer file.close();
    file.seekFromEnd(0) catch return compat.FALSE;

    const allocator = std.heap.page_allocator;
    const line = if (op == .rename)
        std.fmt.allocPrint(allocator, "{c} {c} {s} {s}\n", .{ @intFromEnum(op), @intFromEnum(state), name, new_name.? }) catch return compat.FALSE
    else
        std.fmt.allocPrint(allocator, "{c} {c} {s}\n", .{ @intFromEnum(op), @intFromEnum(state), name }) catch return compat.FALSE;
    defer allocator.free(line);
    file.writeAll(line) catch return compat.FALSE;
    return compat.TRUE;
}

pub fn ddl_log_begin(op: DdlOpType, name: []const u8, new_name: ?[]const u8) ibool {
    return ddlLogAppend(op, .begin, name, new_name);
}

pub fn ddl_log_end(op: DdlOpType, name: []const u8, new_name: ?[]const u8) ibool {
    return ddlLogAppend(op, .end, name, new_name);
}

fn parseOp(ch: u8) ?DdlOpType {
    return switch (ch) {
        'C' => .create,
        'D' => .drop,
        'R' => .rename,
        else => null,
    };
}

fn parseState(ch: u8) ?DdlOpState {
    return switch (ch) {
        'B' => .begin,
        'E' => .end,
        else => null,
    };
}

fn pendingFind(op: DdlOpType, name: []const u8, new_name: ?[]const u8) ?usize {
    for (pending_ops.items, 0..) |item, idx| {
        if (item.op != op) {
            continue;
        }
        if (!std.mem.eql(u8, item.name, name)) {
            continue;
        }
        if (op == .rename) {
            const item_new = item.new_name orelse continue;
            const desired = new_name orelse continue;
            if (!std.mem.eql(u8, item_new, desired)) {
                continue;
            }
        }
        return idx;
    }
    return null;
}

pub fn ddl_log_recover() ulint {
    if (!ddl_log_inited and ddl_log_init() == compat.FALSE) {
        return 0;
    }
    pendingClear();
    if (ddl_log_path.len == 0) {
        return 0;
    }

    var file = std.fs.cwd().openFile(ddl_log_path, .{}) catch |err| {
        return if (err == error.FileNotFound) 0 else 0;
    };
    defer file.close();

    const allocator = std.heap.page_allocator;
    const data = file.readToEndAlloc(allocator, 1024 * 1024) catch return 0;
    defer allocator.free(data);

    var lines = std.mem.splitScalar(u8, data, '\n');
    while (lines.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, " \r\t");
        if (line.len == 0) {
            continue;
        }
        var tok = std.mem.tokenizeScalar(u8, line, ' ');
        const op_txt = tok.next() orelse continue;
        const state_txt = tok.next() orelse continue;
        const name_txt = tok.next() orelse continue;
        const new_txt = tok.next();
        const op = parseOp(op_txt[0]) orelse continue;
        const state = parseState(state_txt[0]) orelse continue;

        if (op == .rename and new_txt == null) {
            continue;
        }

        if (state == .begin) {
            const name_copy = dupName(allocator, name_txt) orelse continue;
            var new_copy: ?[]u8 = null;
            if (op == .rename) {
                const new_name = new_txt orelse {
                    freeName(name_copy);
                    continue;
                };
                new_copy = dupName(allocator, new_name) orelse {
                    freeName(name_copy);
                    continue;
                };
            }
            pending_ops.append(allocator, .{ .op = op, .name = name_copy, .new_name = new_copy }) catch {
                freeName(name_copy);
                if (new_copy) |new_name| {
                    freeName(new_name);
                }
            };
        } else {
            if (pendingFind(op, name_txt, new_txt) ) |idx| {
                const removed = pending_ops.orderedRemove(idx);
                freeName(removed.name);
                if (removed.new_name) |new_name| {
                    freeName(new_name);
                }
            }
        }
    }

    const pending_count: ulint = @as(ulint, @intCast(pending_ops.items.len));
    var trunc_file = std.fs.cwd().createFile(ddl_log_path, .{ .truncate = true }) catch return pending_count;
    trunc_file.close();
    for (pending_ops.items) |entry| {
        _ = ddlLogAppend(entry.op, .begin, entry.name, entry.new_name);
    }

    return pending_count;
}

test "ddl log begin/end recovery" {
    ddl_log_shutdown();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);
    const prev = fil.fil_path_to_client_datadir;
    fil.fil_path_to_client_datadir = base;
    defer fil.fil_path_to_client_datadir = prev;

    try std.testing.expectEqual(compat.TRUE, ddl_log_init());
    defer ddl_log_shutdown();
    ddl_log_clear();

    try std.testing.expectEqual(compat.TRUE, ddl_log_begin(.create, "db/t1", null));
    try std.testing.expectEqual(compat.TRUE, ddl_log_end(.create, "db/t1", null));
    try std.testing.expectEqual(@as(ulint, 0), ddl_log_recover());

    ddl_log_clear();
    try std.testing.expectEqual(compat.TRUE, ddl_log_begin(.drop, "db/t2", null));
    try std.testing.expectEqual(@as(ulint, 1), ddl_log_recover());
}
