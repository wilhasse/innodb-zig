const std = @import("std");
const compat = @import("../ut/compat.zig");
const log = @import("../ut/log.zig");

pub const module_name = "ha";

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;

pub const ib_stream_t = log.ib_stream_t;

pub const ha_node_t = struct {
    next: ?*ha_node_t = null,
    data: ?*anyopaque = null,
    fold: ulint = 0,
};

pub const hash_table_t = struct {
    map: std.AutoHashMap(ulint, *ha_node_t),
    n_cells: ulint = 0,
    n_mutexes: ulint = 0,
};

pub fn ha_search_and_get_data(table: *hash_table_t, fold: ulint) ?*anyopaque {
    if (table.map.get(fold)) |node| {
        return node.data;
    }
    return null;
}

pub fn ha_search_and_update_if_found_func(
    table: *hash_table_t,
    fold: ulint,
    data: *anyopaque,
    new_data: *anyopaque,
) void {
    if (table.map.get(fold)) |node| {
        if (node.data == data) {
            node.data = new_data;
        }
    }
}

pub fn ha_create_func(n: ulint, n_mutexes: ulint) ?*hash_table_t {
    const table = std.heap.page_allocator.create(hash_table_t) catch return null;
    table.* = .{
        .map = std.AutoHashMap(ulint, *ha_node_t).init(std.heap.page_allocator),
        .n_cells = n,
        .n_mutexes = n_mutexes,
    };
    return table;
}

pub fn ha_clear(table: *hash_table_t) void {
    var it = table.map.valueIterator();
    while (it.next()) |node| {
        std.heap.page_allocator.destroy(node.*);
    }
    table.map.clearAndFree();
}

pub fn ha_insert_for_fold_func(table: *hash_table_t, fold: ulint, data: *anyopaque) ibool {
    if (table.map.get(fold)) |node| {
        node.data = data;
        return compat.TRUE;
    }
    const node = std.heap.page_allocator.create(ha_node_t) catch return compat.FALSE;
    node.* = .{
        .data = data,
        .fold = fold,
    };
    table.map.put(fold, node) catch {
        std.heap.page_allocator.destroy(node);
        return compat.FALSE;
    };
    return compat.TRUE;
}

pub fn ha_search_and_delete_if_found(table: *hash_table_t, fold: ulint, data: *anyopaque) ibool {
    if (table.map.get(fold)) |node| {
        if (node.data == data) {
            _ = table.map.remove(fold);
            std.heap.page_allocator.destroy(node);
            return compat.TRUE;
        }
    }
    return compat.FALSE;
}

pub fn ha_remove_all_nodes_to_page(table: *hash_table_t, fold: ulint, page: *const anyopaque) void {
    _ = page;
    if (table.map.get(fold)) |node| {
        _ = table.map.remove(fold);
        std.heap.page_allocator.destroy(node);
    }
}

pub fn ha_validate(table: *hash_table_t, start_index: ulint, end_index: ulint) ibool {
    _ = start_index;
    _ = end_index;
    _ = table;
    return compat.TRUE;
}

pub fn ha_print_info(ib_stream: ib_stream_t, table: *hash_table_t) void {
    _ = ib_stream;
    const count = table.map.count();
    log.logf("ha: entries={d}\n", .{count});
}

pub const HA_STORAGE_DEFAULT_HEAP_BYTES: ulint = 1024;
pub const HA_STORAGE_DEFAULT_HASH_CELLS: ulint = 4096;

pub const ha_storage_node_t = struct {
    data: []u8,
    len: ulint,
};

pub const ha_storage_t = struct {
    nodes: std.ArrayListUnmanaged(ha_storage_node_t) = .{},
    size_bytes: ulint = 0,
};

fn ha_storage_get(storage: *const ha_storage_t, data: [*]const u8, data_len: ulint) ?*const anyopaque {
    const n = @as(usize, @intCast(data_len));
    for (storage.nodes.items) |node| {
        if (node.len == data_len and std.mem.eql(u8, node.data, data[0..n])) {
            return @ptrCast(node.data.ptr);
        }
    }
    return null;
}

pub fn ha_storage_create(initial_heap_bytes: ulint, initial_hash_cells: ulint) ?*ha_storage_t {
    _ = initial_heap_bytes;
    _ = initial_hash_cells;
    const storage = std.heap.page_allocator.create(ha_storage_t) catch return null;
    storage.* = .{};
    return storage;
}

pub fn ha_storage_put_memlim(
    storage: *ha_storage_t,
    data: *const anyopaque,
    data_len: ulint,
    memlim: ulint,
) ?*const anyopaque {
    const bytes = @as([*]const u8, @ptrCast(data));
    if (ha_storage_get(storage, bytes, data_len)) |found| {
        return found;
    }
    if (memlim > 0 and storage.size_bytes + data_len > memlim) {
        return null;
    }
    const n = @as(usize, @intCast(data_len));
    const buf = std.heap.page_allocator.alloc(u8, n) catch return null;
    std.mem.copyForwards(u8, buf, bytes[0..n]);
    storage.nodes.append(std.heap.page_allocator, .{ .data = buf, .len = data_len }) catch {
        std.heap.page_allocator.free(buf);
        return null;
    };
    storage.size_bytes += data_len;
    return @ptrCast(buf.ptr);
}

pub fn ha_storage_put(storage: *ha_storage_t, data: *const anyopaque, data_len: ulint) ?*const anyopaque {
    return ha_storage_put_memlim(storage, data, data_len, 0);
}

pub fn ha_storage_put_str(storage: *ha_storage_t, str: []const u8) ?*const u8 {
    var buf = std.heap.page_allocator.alloc(u8, str.len + 1) catch return null;
    std.mem.copyForwards(u8, buf[0..str.len], str);
    buf[str.len] = 0;
    const out = ha_storage_put_memlim(storage, buf.ptr, str.len + 1, 0);
    std.heap.page_allocator.free(buf);
    return if (out) |ptr| @as([*]const u8, @ptrCast(ptr)) else null;
}

pub fn ha_storage_put_str_memlim(storage: *ha_storage_t, str: []const u8, memlim: ulint) ?*const u8 {
    var buf = std.heap.page_allocator.alloc(u8, str.len + 1) catch return null;
    std.mem.copyForwards(u8, buf[0..str.len], str);
    buf[str.len] = 0;
    const out = ha_storage_put_memlim(storage, buf.ptr, str.len + 1, memlim);
    std.heap.page_allocator.free(buf);
    return if (out) |ptr| @as([*]const u8, @ptrCast(ptr)) else null;
}

pub fn ha_storage_empty(storage: *ha_storage_t) void {
    for (storage.nodes.items) |node| {
        std.heap.page_allocator.free(node.data);
    }
    storage.nodes.clearAndFree(std.heap.page_allocator);
    storage.size_bytes = 0;
}

pub fn ha_storage_free(storage: *ha_storage_t) void {
    ha_storage_empty(storage);
    std.heap.page_allocator.destroy(storage);
}

pub fn ha_storage_get_size(storage: *const ha_storage_t) ulint {
    return storage.size_bytes;
}

test "ha insert/search/update/delete" {
    const table = ha_create_func(8, 0) orelse return error.OutOfMemory;
    defer {
        ha_clear(table);
        std.heap.page_allocator.destroy(table);
    }

    var value1: u32 = 10;
    var value2: u32 = 20;
    const ptr1: *anyopaque = @ptrCast(&value1);
    const ptr2: *anyopaque = @ptrCast(&value2);

    try std.testing.expect(ha_insert_for_fold_func(table, 5, ptr1) == compat.TRUE);
    try std.testing.expect(ha_search_and_get_data(table, 5) == ptr1);

    ha_search_and_update_if_found_func(table, 5, ptr1, ptr2);
    try std.testing.expect(ha_search_and_get_data(table, 5) == ptr2);

    try std.testing.expect(ha_search_and_delete_if_found(table, 5, ptr2) == compat.TRUE);
    try std.testing.expect(ha_search_and_get_data(table, 5) == null);
}

test "ha storage deduplicates data" {
    const storage = ha_storage_create(0, 0) orelse return error.OutOfMemory;
    defer ha_storage_free(storage);

    const data = "abc";
    const p1 = ha_storage_put(storage, data.ptr, data.len) orelse return error.OutOfMemory;
    const p2 = ha_storage_put(storage, data.ptr, data.len) orelse return error.OutOfMemory;
    try std.testing.expect(p1 == p2);
    try std.testing.expect(ha_storage_get_size(storage) == data.len);

    const limited = ha_storage_put_memlim(storage, data.ptr, data.len, data.len - 1);
    try std.testing.expect(limited == null);
}
