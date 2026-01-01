const std = @import("std");
const compat = @import("../ut/compat.zig");
const types = @import("types.zig");
const rseg = @import("rseg.zig");
const fil = @import("../fil/mod.zig");

pub const module_name = "trx.sys";

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;
pub const byte = compat.byte;

pub const TRX_SYS_DOUBLEWRITE_BLOCK_SIZE: ulint = @as(ulint, 1) << (20 - compat.UNIV_PAGE_SIZE_SHIFT);
pub const DICT_TF_FORMAT_51: ulint = 0;

pub const file_format_t = struct {
    id: ulint = 0,
    name: []const u8 = "",
};

pub const trx_doublewrite_t = struct {
    first_free: ulint = 0,
    block1: ulint = 0,
    block2: ulint = 0,
};

pub const trx_sys_t = struct {
    max_trx_id: types.trx_id_t = 0,
    trx_list: std.ArrayListUnmanaged(*types.trx_t) = .{},
    allocator: std.mem.Allocator = std.heap.page_allocator,
};

pub var trx_sys: ?*trx_sys_t = null;
pub var trx_doublewrite: ?*trx_doublewrite_t = null;
pub var trx_doublewrite_must_reset_space_ids: ibool = compat.FALSE;
pub var trx_doublewrite_buf_is_being_created: ibool = compat.FALSE;
pub var trx_sys_multiple_tablespace_format: ibool = compat.FALSE;
pub var trx_doublewrite_enabled: ibool = compat.TRUE;

const file_format_name_map = [_][]const u8{
    "Antelope",
    "Barracuda",
    "Cheetah",
    "Dragon",
    "Elk",
    "Fox",
    "Gazelle",
    "Hornet",
    "Impala",
    "Jaguar",
    "Kangaroo",
    "Leopard",
    "Moose",
    "Nautilus",
    "Ocelot",
    "Porpoise",
    "Quail",
    "Rabbit",
    "Shark",
    "Tiger",
    "Urchin",
    "Viper",
    "Whale",
    "Xenops",
    "Yak",
    "Zebra",
};

var file_format_max: file_format_t = .{};

pub fn trx_sys_var_init() void {
    if (trx_sys) |sys| {
        sys.trx_list.deinit(sys.allocator);
        sys.allocator.destroy(sys);
    }
    trx_sys = null;

    if (trx_doublewrite) |dw| {
        std.heap.page_allocator.destroy(dw);
    }
    trx_doublewrite = null;

    trx_doublewrite_must_reset_space_ids = compat.FALSE;
    trx_sys_multiple_tablespace_format = compat.FALSE;
    trx_doublewrite_buf_is_being_created = compat.FALSE;
    trx_doublewrite_enabled = compat.TRUE;
    file_format_max = .{};
}

pub fn trx_doublewrite_init(block1: ulint, block2: ulint, allocator: std.mem.Allocator) void {
    const dw = allocator.create(trx_doublewrite_t) catch @panic("trx_doublewrite_init");
    dw.* = .{ .first_free = 0, .block1 = block1, .block2 = block2 };
    trx_doublewrite = dw;
}

pub fn trx_doublewrite_init_default(allocator: std.mem.Allocator) void {
    const block1 = TRX_SYS_DOUBLEWRITE_BLOCK_SIZE;
    const block2 = TRX_SYS_DOUBLEWRITE_BLOCK_SIZE * 2;
    trx_doublewrite_init(block1, block2, allocator);
}

pub fn trx_doublewrite_set_enabled(enabled: ibool) void {
    trx_doublewrite_enabled = enabled;
}

pub fn trx_doublewrite_page_inside(page_no: ulint) bool {
    const dw = trx_doublewrite orelse return false;
    if (page_no >= dw.block1 and page_no < dw.block1 + TRX_SYS_DOUBLEWRITE_BLOCK_SIZE) {
        return true;
    }
    if (page_no >= dw.block2 and page_no < dw.block2 + TRX_SYS_DOUBLEWRITE_BLOCK_SIZE) {
        return true;
    }
    return false;
}

pub fn trx_doublewrite_write_page(space_id: ulint, page_no: ulint, buf: [*]const byte) ulint {
    if (trx_doublewrite_enabled == compat.FALSE) {
        return fil.fil_write_page_raw(space_id, page_no, buf);
    }
    const dw = trx_doublewrite orelse return fil.fil_write_page_raw(space_id, page_no, buf);
    if (space_id == 0 and trx_doublewrite_page_inside(page_no)) {
        return fil.fil_write_page_raw(space_id, page_no, buf);
    }
    const max_slots = TRX_SYS_DOUBLEWRITE_BLOCK_SIZE * 2;
    var slot = dw.first_free;
    if (slot >= max_slots) {
        slot = 0;
    }
    const target_page = if (slot < TRX_SYS_DOUBLEWRITE_BLOCK_SIZE)
        dw.block1 + slot
    else
        dw.block2 + (slot - TRX_SYS_DOUBLEWRITE_BLOCK_SIZE);
    if (fil.fil_write_page_raw(0, target_page, buf) != fil.DB_SUCCESS) {
        return fil.DB_ERROR;
    }
    dw.first_free = (slot + 1) % max_slots;
    return fil.fil_write_page_raw(space_id, page_no, buf);
}

pub fn trx_sys_mark_upgraded_to_multiple_tablespaces() void {
    trx_sys_multiple_tablespace_format = compat.TRUE;
}

pub fn trx_sys_init_at_db_start(allocator: std.mem.Allocator) *trx_sys_t {
    if (trx_sys) |sys| {
        return sys;
    }
    const sys = allocator.create(trx_sys_t) catch @panic("trx_sys_init_at_db_start");
    sys.* = .{ .max_trx_id = 1, .allocator = allocator };
    trx_sys = sys;
    rseg.trx_sys_init(allocator);
    rseg.trx_rseg_list_and_array_init();
    return sys;
}

pub fn trx_sys_close() void {
    trx_sys_var_init();
}

pub fn trx_in_trx_list(trx: *types.trx_t) bool {
    const sys = trx_sys orelse return false;
    for (sys.trx_list.items) |item| {
        if (item == trx) {
            return true;
        }
    }
    return false;
}

pub fn trx_sys_file_format_id_to_name(id: ulint) ?[]const u8 {
    if (id >= file_format_name_map.len) {
        return null;
    }
    return file_format_name_map[@as(usize, @intCast(id))];
}

pub fn trx_sys_file_format_name_to_id(name: []const u8) ?ulint {
    for (file_format_name_map, 0..) |item, i| {
        if (std.mem.eql(u8, item, name)) {
            return @as(ulint, @intCast(i));
        }
    }
    return null;
}

pub fn trx_sys_file_format_max_set(format_id: ulint, name: ?[]const u8) void {
    file_format_max.id = format_id;
    file_format_max.name = if (name) |n| n else trx_sys_file_format_id_to_name(format_id) orelse "";
}

pub fn trx_sys_file_format_max_get() []const u8 {
    return file_format_max.name;
}

pub fn trx_sys_file_format_init() void {
    trx_sys_file_format_max_set(DICT_TF_FORMAT_51, null);
}

pub fn trx_sys_file_format_close() void {
    file_format_max = .{};
}

test "trx sys var init and doublewrite" {
    trx_sys_var_init();
    try std.testing.expect(trx_sys == null);

    trx_doublewrite_init(10, 100, std.testing.allocator);
    defer {
        if (trx_doublewrite) |dw| std.testing.allocator.destroy(dw);
        trx_doublewrite = null;
    }
    try std.testing.expect(trx_doublewrite_page_inside(10));
    try std.testing.expect(trx_doublewrite_page_inside(10 + TRX_SYS_DOUBLEWRITE_BLOCK_SIZE - 1));
    try std.testing.expect(!trx_doublewrite_page_inside(10 + TRX_SYS_DOUBLEWRITE_BLOCK_SIZE));
}

test "trx sys init at db start" {
    trx_sys_var_init();
    const sys = trx_sys_init_at_db_start(std.testing.allocator);
    defer trx_sys_close();
    try std.testing.expect(sys.max_trx_id == 1);
}

test "trx sys file format helpers" {
    trx_sys_file_format_init();
    try std.testing.expectEqualStrings("Antelope", trx_sys_file_format_max_get());
    try std.testing.expectEqual(@as(?ulint, 0), trx_sys_file_format_name_to_id("Antelope"));
    try std.testing.expect(trx_sys_file_format_id_to_name(100) == null);
    trx_sys_file_format_close();
}
