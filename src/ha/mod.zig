const std = @import("std");
const compat = @import("../ut/compat.zig");
const log = @import("../ut/log.zig");
const rnd = @import("../ut/rnd.zig");

pub const module_name = "ha";

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;

pub const ib_stream_t = log.ib_stream_t;

pub const ha_node_t = struct {
    next: ?*ha_node_t = null,
    data: ?*anyopaque = null,
    fold: ulint = 0,
};

pub const hash_cell_t = struct {
    node: ?*anyopaque = null,
};

pub const HASH_TABLE_MAGIC_N: ulint = 76561114;

pub const hash_table_t = struct {
    n_cells: ulint,
    array: []hash_cell_t,
    n_mutexes: ulint = 0,
    mutexes: ?[]std.Thread.Mutex = null,
    heaps: ?[]*anyopaque = null,
    heap: ?*anyopaque = null,
    magic_n: ulint = HASH_TABLE_MAGIC_N,
};

pub fn hash_get_nth_cell(table: *hash_table_t, n: ulint) *hash_cell_t {
    std.debug.assert(n < table.n_cells);
    return &table.array[@as(usize, @intCast(n))];
}

pub fn hash_table_clear(table: *hash_table_t) void {
    for (table.array) |*cell| {
        cell.node = null;
    }
}

pub fn hash_get_n_cells(table: *hash_table_t) ulint {
    return table.n_cells;
}

pub fn hash_calc_hash(fold: ulint, table: *hash_table_t) ulint {
    return rnd.ut_hash_ulint(fold, table.n_cells);
}

pub fn hash_create(n: ulint) ?*hash_table_t {
    const prime = rnd.ut_find_prime(n);
    const table = std.heap.page_allocator.create(hash_table_t) catch return null;
    const array = std.heap.page_allocator.alloc(hash_cell_t, @as(usize, @intCast(prime))) catch {
        std.heap.page_allocator.destroy(table);
        return null;
    };
    for (array) |*cell| {
        cell.* = .{};
    }
    table.* = .{
        .n_cells = prime,
        .array = array,
        .magic_n = HASH_TABLE_MAGIC_N,
    };
    return table;
}

pub fn hash_table_free(table: *hash_table_t) void {
    hash_free_mutexes_func(table);
    if (table.heaps) |heaps| {
        std.heap.page_allocator.free(heaps);
        table.heaps = null;
    }
    std.heap.page_allocator.free(table.array);
    std.heap.page_allocator.destroy(table);
}

pub fn hash_create_mutexes_func(table: *hash_table_t, n_mutexes: ulint) ibool {
    std.debug.assert(n_mutexes > 0);
    std.debug.assert(compat.ut_is_2pow(n_mutexes));

    const mutexes = std.heap.page_allocator.alloc(
        std.Thread.Mutex,
        @as(usize, @intCast(n_mutexes)),
    ) catch return compat.FALSE;
    for (mutexes) |*mutex| {
        mutex.* = .{};
    }
    table.mutexes = mutexes;
    table.n_mutexes = n_mutexes;
    return compat.TRUE;
}

pub fn hash_free_mutexes_func(table: *hash_table_t) void {
    if (table.mutexes) |mutexes| {
        std.heap.page_allocator.free(mutexes);
        table.mutexes = null;
    }
    table.n_mutexes = 0;
}

pub fn hash_get_mutex_no(table: *hash_table_t, fold: ulint) ulint {
    std.debug.assert(compat.ut_is_2pow(table.n_mutexes));
    return compat.ut_2pow_remainder(hash_calc_hash(fold, table), table.n_mutexes);
}

pub fn hash_get_nth_mutex(table: *hash_table_t, i: ulint) *std.Thread.Mutex {
    std.debug.assert(i < table.n_mutexes);
    return &table.mutexes.?[@as(usize, @intCast(i))];
}

pub fn hash_get_mutex(table: *hash_table_t, fold: ulint) *std.Thread.Mutex {
    const i = hash_get_mutex_no(table, fold);
    return hash_get_nth_mutex(table, i);
}

pub fn hash_mutex_enter(table: *hash_table_t, fold: ulint) void {
    if (table.mutexes != null) {
        hash_get_mutex(table, fold).lock();
    }
}

pub fn hash_mutex_exit(table: *hash_table_t, fold: ulint) void {
    if (table.mutexes != null) {
        hash_get_mutex(table, fold).unlock();
    }
}

pub fn hash_mutex_enter_all(table: *hash_table_t) void {
    if (table.mutexes) |mutexes| {
        for (mutexes) |*mutex| {
            mutex.lock();
        }
    }
}

pub fn hash_mutex_exit_all(table: *hash_table_t) void {
    if (table.mutexes) |mutexes| {
        for (mutexes) |*mutex| {
            mutex.unlock();
        }
    }
}

pub fn hash_insert(
    comptime T: type,
    comptime next_field: []const u8,
    table: *hash_table_t,
    fold: ulint,
    node: *T,
) void {
    @field(node, next_field) = null;
    const cell = hash_get_nth_cell(table, hash_calc_hash(fold, table));
    if (cell.node == null) {
        cell.node = @ptrCast(node);
        return;
    }
    var cur: *T = @ptrCast(@alignCast(cell.node.?));
    while (true) {
        const next_opt = @field(cur, next_field);
        if (next_opt) |next| {
            cur = next;
        } else {
            @field(cur, next_field) = node;
            return;
        }
    }
}

pub fn hash_delete(
    comptime T: type,
    comptime next_field: []const u8,
    table: *hash_table_t,
    fold: ulint,
    node: *T,
) void {
    const cell = hash_get_nth_cell(table, hash_calc_hash(fold, table));
    if (cell.node == null) {
        return;
    }
    const head: *T = @ptrCast(@alignCast(cell.node.?));
    if (head == node) {
        cell.node = if (@field(node, next_field)) |next| @ptrCast(next) else null;
        return;
    }
    var cur: *T = head;
    while (true) {
        const next_opt = @field(cur, next_field) orelse return;
        if (next_opt == node) {
            @field(cur, next_field) = @field(node, next_field);
            return;
        }
        cur = next_opt;
    }
}

pub fn hash_search(
    comptime T: type,
    comptime next_field: []const u8,
    table: *hash_table_t,
    fold: ulint,
    test_fn: anytype,
) ?*T {
    const cell = hash_get_nth_cell(table, hash_calc_hash(fold, table));
    var cur_opt: ?*anyopaque = cell.node;
    while (cur_opt) |ptr| {
        const cur: *T = @ptrCast(@alignCast(ptr));
        if (test_fn(cur)) {
            return cur;
        }
        const next_opt = @field(cur, next_field);
        cur_opt = if (next_opt) |next| @ptrCast(next) else null;
    }
    return null;
}

pub fn hash_search_all(
    comptime T: type,
    comptime next_field: []const u8,
    table: *hash_table_t,
    test_fn: anytype,
) ?*T {
    var i: ulint = 0;
    while (i < table.n_cells) : (i += 1) {
        var cur_opt: ?*anyopaque = table.array[@as(usize, @intCast(i))].node;
        while (cur_opt) |ptr| {
            const cur: *T = @ptrCast(@alignCast(ptr));
            if (test_fn(cur)) {
                return cur;
            }
            const next_opt = @field(cur, next_field);
            cur_opt = if (next_opt) |next| @ptrCast(next) else null;
        }
    }
    return null;
}

pub fn ha_search_and_get_data(table: *hash_table_t, fold: ulint) ?*anyopaque {
    const cell = hash_get_nth_cell(table, hash_calc_hash(fold, table));
    var node_opt: ?*ha_node_t = if (cell.node) |ptr| @ptrCast(@alignCast(ptr)) else null;
    while (node_opt) |node| {
        if (node.fold == fold) {
            return node.data;
        }
        node_opt = node.next;
    }
    return null;
}

pub fn ha_search_and_update_if_found_func(
    table: *hash_table_t,
    fold: ulint,
    data: *anyopaque,
    new_data: *anyopaque,
) void {
    const cell = hash_get_nth_cell(table, hash_calc_hash(fold, table));
    var node_opt: ?*ha_node_t = if (cell.node) |ptr| @ptrCast(@alignCast(ptr)) else null;
    while (node_opt) |node| {
        if (node.fold == fold and node.data == data) {
            node.data = new_data;
            return;
        }
        node_opt = node.next;
    }
}

pub fn ha_create_func(n: ulint, n_mutexes: ulint) ?*hash_table_t {
    const table = hash_create(n) orelse return null;
    if (n_mutexes > 0) {
        if (hash_create_mutexes_func(table, n_mutexes) == compat.FALSE) {
            hash_table_free(table);
            return null;
        }
    }
    return table;
}

pub fn ha_clear(table: *hash_table_t) void {
    for (table.array) |*cell| {
        var node_opt: ?*ha_node_t = if (cell.node) |ptr| @ptrCast(@alignCast(ptr)) else null;
        while (node_opt) |node| {
            const next = node.next;
            std.heap.page_allocator.destroy(node);
            node_opt = next;
        }
        cell.node = null;
    }
}

pub fn ha_insert_for_fold_func(table: *hash_table_t, fold: ulint, data: *anyopaque) ibool {
    const cell = hash_get_nth_cell(table, hash_calc_hash(fold, table));
    var node_opt: ?*ha_node_t = if (cell.node) |ptr| @ptrCast(@alignCast(ptr)) else null;
    while (node_opt) |node| {
        if (node.fold == fold) {
            node.data = data;
            return compat.TRUE;
        }
        node_opt = node.next;
    }

    const node = std.heap.page_allocator.create(ha_node_t) catch return compat.FALSE;
    node.* = .{
        .data = data,
        .fold = fold,
    };
    hash_insert(ha_node_t, "next", table, fold, node);
    return compat.TRUE;
}

pub fn ha_search_and_delete_if_found(table: *hash_table_t, fold: ulint, data: *anyopaque) ibool {
    const cell = hash_get_nth_cell(table, hash_calc_hash(fold, table));
    if (cell.node == null) {
        return compat.FALSE;
    }
    var node_opt: ?*ha_node_t = @ptrCast(@alignCast(cell.node.?));
    var prev: ?*ha_node_t = null;
    while (node_opt) |node| {
        if (node.fold == fold and node.data == data) {
            if (prev) |prev_node| {
                prev_node.next = node.next;
            } else {
                cell.node = if (node.next) |next| @ptrCast(next) else null;
            }
            std.heap.page_allocator.destroy(node);
            return compat.TRUE;
        }
        prev = node;
        node_opt = node.next;
    }
    return compat.FALSE;
}

pub fn ha_remove_all_nodes_to_page(table: *hash_table_t, fold: ulint, page: *const anyopaque) void {
    _ = page;
    const cell = hash_get_nth_cell(table, hash_calc_hash(fold, table));
    var node_opt: ?*ha_node_t = if (cell.node) |ptr| @ptrCast(@alignCast(ptr)) else null;
    while (node_opt) |node| {
        const next = node.next;
        std.heap.page_allocator.destroy(node);
        node_opt = next;
    }
    cell.node = null;
}

pub fn ha_validate(table: *hash_table_t, start_index: ulint, end_index: ulint) ibool {
    _ = start_index;
    _ = end_index;
    _ = table;
    return compat.TRUE;
}

pub fn ha_print_info(ib_stream: ib_stream_t, table: *hash_table_t) void {
    _ = ib_stream;
    var count: usize = 0;
    for (table.array) |cell| {
        var node_opt: ?*ha_node_t = if (cell.node) |ptr| @ptrCast(@alignCast(ptr)) else null;
        while (node_opt) |node| {
            count += 1;
            node_opt = node.next;
        }
    }
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
    if (memlim > 0 and storage.size_bytes + data_len > memlim) {
        return null;
    }
    if (ha_storage_get(storage, bytes, data_len)) |found| {
        return found;
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
        hash_table_free(table);
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

test "hash insert/search/delete" {
    const table = hash_create(7) orelse return error.OutOfMemory;
    defer hash_table_free(table);

    var node1 = ha_node_t{ .fold = 11 };
    var node2 = ha_node_t{ .fold = 11 };
    var node3 = ha_node_t{ .fold = 13 };

    hash_insert(ha_node_t, "next", table, node1.fold, &node1);
    hash_insert(ha_node_t, "next", table, node2.fold, &node2);
    hash_insert(ha_node_t, "next", table, node3.fold, &node3);

    try std.testing.expect(node1.next == &node2);

    const found = hash_search(
        ha_node_t,
        "next",
        table,
        13,
        struct {
            fn predicate(node: *ha_node_t) bool {
                return node.fold == 13;
            }
        }.predicate,
    );
    try std.testing.expect(found == &node3);

    hash_delete(ha_node_t, "next", table, 13, &node3);
    const missing = hash_search(
        ha_node_t,
        "next",
        table,
        13,
        struct {
            fn predicate(node: *ha_node_t) bool {
                return node.fold == 13;
            }
        }.predicate,
    );
    try std.testing.expect(missing == null);

    hash_table_clear(table);
    const after_clear = hash_search_all(
        ha_node_t,
        "next",
        table,
        struct {
            fn predicate(node: *ha_node_t) bool {
                _ = node;
                return true;
            }
        }.predicate,
    );
    try std.testing.expect(after_clear == null);
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
