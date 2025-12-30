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
