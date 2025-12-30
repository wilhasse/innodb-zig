const std = @import("std");
const compat = @import("../ut/compat.zig");
const mach = @import("../mach/mod.zig");
const sync = @import("../sync/mod.zig");

pub const module_name = "fil";

pub const ulint = compat.ulint;
pub const lint = compat.lint;
pub const ibool = compat.ibool;
pub const byte = compat.byte;
pub const ib_int64_t = compat.ib_int64_t;
pub const ib_uint64_t = compat.ib_uint64_t;

pub const FIL_IBD_FILE_INITIAL_SIZE: ulint = 4;
pub const FIL_NULL: ulint = @as(ulint, compat.ULINT32_UNDEFINED);

pub const fil_faddr_t = byte;
pub const FIL_ADDR_PAGE: ulint = 0;
pub const FIL_ADDR_BYTE: ulint = 4;
pub const FIL_ADDR_SIZE: ulint = 6;

pub const fil_addr_t = struct {
    page: ulint,
    boffset: ulint,
};

pub const fil_addr_null = fil_addr_t{
    .page = FIL_NULL,
    .boffset = 0,
};

pub const FIL_PAGE_SPACE_OR_CHKSUM: ulint = 0;
pub const FIL_PAGE_OFFSET: ulint = 4;
pub const FIL_PAGE_PREV: ulint = 8;
pub const FIL_PAGE_NEXT: ulint = 12;
pub const FIL_PAGE_LSN: ulint = 16;
pub const FIL_PAGE_TYPE: ulint = 24;
pub const FIL_PAGE_FILE_FLUSH_LSN: ulint = 26;
pub const FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID: ulint = 34;
pub const FIL_PAGE_DATA: ulint = 38;
pub const FIL_PAGE_END_LSN_OLD_CHKSUM: ulint = 8;
pub const FIL_PAGE_DATA_END: ulint = 8;

pub const FIL_PAGE_INDEX: ulint = 17855;
pub const FIL_PAGE_UNDO_LOG: ulint = 2;
pub const FIL_PAGE_INODE: ulint = 3;
pub const FIL_PAGE_IBUF_FREE_LIST: ulint = 4;
pub const FIL_PAGE_TYPE_ALLOCATED: ulint = 0;
pub const FIL_PAGE_IBUF_BITMAP: ulint = 5;
pub const FIL_PAGE_TYPE_SYS: ulint = 6;
pub const FIL_PAGE_TYPE_TRX_SYS: ulint = 7;
pub const FIL_PAGE_TYPE_FSP_HDR: ulint = 8;
pub const FIL_PAGE_TYPE_XDES: ulint = 9;
pub const FIL_PAGE_TYPE_BLOB: ulint = 10;
pub const FIL_PAGE_TYPE_ZBLOB: ulint = 11;
pub const FIL_PAGE_TYPE_ZBLOB2: ulint = 12;

pub const FIL_TABLESPACE: ulint = 501;
pub const FIL_LOG: ulint = 502;

pub const DB_SUCCESS: ulint = 0;
pub const DB_TABLESPACE_DELETED: ulint = 1;

pub var fil_n_log_flushes: ulint = 0;
pub var fil_n_pending_log_flushes: ulint = 0;
pub var fil_n_pending_tablespace_flushes: ulint = 0;

pub var fil_path_to_client_datadir: []const u8 = ".";

pub const rw_lock_t = sync.RwLock;
pub const os_file_t = ?*anyopaque;
pub const ib_recovery_t = ulint;

pub const fil_node_t = struct {
    name: []u8,
    size: ulint,
    is_raw: ibool,
};

pub const fil_space_t = struct {
    id: ulint,
    name: []u8,
    purpose: ulint,
    zip_size: ulint,
    flags: ulint,
    size: ulint,
    version: ib_int64_t,
    latch: rw_lock_t,
    nodes: std.ArrayListUnmanaged(fil_node_t) = .{},
    reserved_extents: ulint = 0,
    pending_ibuf_merges: ulint = 0,
    deleted: bool = false,
};

const FilSystem = struct {
    spaces: std.AutoHashMap(ulint, *fil_space_t),
    spaces_by_name: std.StringHashMap(*fil_space_t),
    max_space_id: ulint,
    max_n_open: ulint,
};

var fil_system: FilSystem = undefined;
var fil_system_inited: bool = false;

fn ensureSystem() *FilSystem {
    if (!fil_system_inited) {
        fil_system = .{
            .spaces = std.AutoHashMap(ulint, *fil_space_t).init(std.heap.page_allocator),
            .spaces_by_name = std.StringHashMap(*fil_space_t).init(std.heap.page_allocator),
            .max_space_id = 0,
            .max_n_open = 0,
        };
        fil_system_inited = true;
    }
    return &fil_system;
}

fn findSpace(id: ulint) ?*fil_space_t {
    const sys = ensureSystem();
    return sys.spaces.get(id);
}

fn freeSpace(space: *fil_space_t) void {
    for (space.nodes.items) |node| {
        std.heap.page_allocator.free(node.name);
    }
    space.nodes.deinit(std.heap.page_allocator);
    std.heap.page_allocator.free(space.name);
    std.heap.page_allocator.destroy(space);
}

pub fn fil_space_get_version(id: ulint) ib_int64_t {
    if (findSpace(id)) |space| {
        return space.version;
    }
    return -1;
}

pub fn fil_space_get_latch(id: ulint, zip_size: *ulint) ?*rw_lock_t {
    if (findSpace(id)) |space| {
        zip_size.* = space.zip_size;
        return &space.latch;
    }
    zip_size.* = 0;
    return null;
}

pub fn fil_space_get_type(id: ulint) ulint {
    if (findSpace(id)) |space| {
        return space.purpose;
    }
    return 0;
}

pub fn fil_node_create(name: []const u8, size: ulint, id: ulint, is_raw: ibool) void {
    if (findSpace(id)) |space| {
        const buf = std.heap.page_allocator.alloc(u8, name.len) catch return;
        std.mem.copyForwards(u8, buf, name);
        space.nodes.append(std.heap.page_allocator, .{
            .name = buf,
            .size = size,
            .is_raw = is_raw,
        }) catch {
            std.heap.page_allocator.free(buf);
            return;
        };
        space.size += size;
    }
}

pub fn fil_space_create(name: []const u8, id: ulint, zip_size: ulint, purpose: ulint) ibool {
    const sys = ensureSystem();
    if (sys.spaces.contains(id) or sys.spaces_by_name.contains(name)) {
        return compat.FALSE;
    }

    const name_buf = std.heap.page_allocator.alloc(u8, name.len) catch return compat.FALSE;
    std.mem.copyForwards(u8, name_buf, name);
    const space = std.heap.page_allocator.create(fil_space_t) catch {
        std.heap.page_allocator.free(name_buf);
        return compat.FALSE;
    };
    space.* = .{
        .id = id,
        .name = name_buf,
        .purpose = purpose,
        .zip_size = zip_size,
        .flags = 0,
        .size = 0,
        .version = 0,
        .latch = rw_lock_t.init(),
    };
    sys.spaces.put(id, space) catch {
        freeSpace(space);
        return compat.FALSE;
    };
    sys.spaces_by_name.put(space.name, space) catch {
        _ = sys.spaces.remove(id);
        freeSpace(space);
        return compat.FALSE;
    };

    if (id > sys.max_space_id) {
        sys.max_space_id = id;
    }
    return compat.TRUE;
}

pub fn fil_space_get_size(id: ulint) ulint {
    if (findSpace(id)) |space| {
        return space.size;
    }
    return 0;
}

pub fn fil_space_get_flags(id: ulint) ulint {
    if (findSpace(id)) |space| {
        return space.flags;
    }
    return compat.ULINT_UNDEFINED;
}

pub fn fil_space_get_zip_size(id: ulint) ulint {
    if (findSpace(id)) |space| {
        return space.zip_size;
    }
    return compat.ULINT_UNDEFINED;
}

pub fn fil_check_adress_in_tablespace(id: ulint, page_no: ulint) ibool {
    if (findSpace(id)) |space| {
        return if (page_no < space.size) compat.TRUE else compat.FALSE;
    }
    return compat.FALSE;
}

pub fn fil_init(hash_size: ulint, max_n_open: ulint) void {
    _ = hash_size;
    if (fil_system_inited) {
        fil_close();
    }
    fil_system = .{
        .spaces = std.AutoHashMap(ulint, *fil_space_t).init(std.heap.page_allocator),
        .spaces_by_name = std.StringHashMap(*fil_space_t).init(std.heap.page_allocator),
        .max_space_id = 0,
        .max_n_open = max_n_open,
    };
    fil_system_inited = true;
}

pub fn fil_close() void {
    if (!fil_system_inited) {
        return;
    }
    var it = fil_system.spaces.valueIterator();
    while (it.next()) |space| {
        freeSpace(space.*);
    }
    fil_system.spaces.deinit();
    fil_system.spaces_by_name.deinit();
    fil_system_inited = false;
}

pub fn fil_open_log_and_system_tablespace_files() void {}
pub fn fil_close_all_files() void {}

pub fn fil_set_max_space_id_if_bigger(max_id: ulint) void {
    const sys = ensureSystem();
    if (max_id > sys.max_space_id) {
        sys.max_space_id = max_id;
    }
}

pub fn fil_write_flushed_lsn_to_data_files(lsn: ib_uint64_t, arch_log_no: ulint) ulint {
    _ = lsn;
    _ = arch_log_no;
    return DB_SUCCESS;
}

pub fn fil_read_flushed_lsn_and_arch_log_no(
    data_file: os_file_t,
    one_read_already: ibool,
    min_arch_log_no: ?*ulint,
    max_arch_log_no: ?*ulint,
    min_flushed_lsn: *ib_uint64_t,
    max_flushed_lsn: *ib_uint64_t,
) void {
    _ = data_file;
    _ = one_read_already;
    if (min_arch_log_no) |ptr| {
        ptr.* = 0;
    }
    if (max_arch_log_no) |ptr| {
        ptr.* = 0;
    }
    min_flushed_lsn.* = 0;
    max_flushed_lsn.* = 0;
}

pub fn fil_inc_pending_ibuf_merges(id: ulint) ibool {
    if (findSpace(id)) |space| {
        if (space.deleted) {
            return compat.TRUE;
        }
        space.pending_ibuf_merges += 1;
        return compat.FALSE;
    }
    return compat.TRUE;
}

pub fn fil_decr_pending_ibuf_merges(id: ulint) void {
    if (findSpace(id)) |space| {
        if (space.pending_ibuf_merges > 0) {
            space.pending_ibuf_merges -= 1;
        }
    }
}

pub fn fil_op_log_parse_or_replay(
    ptr: [*]byte,
    end_ptr: [*]byte,
    type_: ulint,
    space_id: ulint,
    log_flags: ulint,
) ?[*]byte {
    _ = type_;
    _ = space_id;
    _ = log_flags;
    if (@intFromPtr(ptr) > @intFromPtr(end_ptr)) {
        return null;
    }
    return end_ptr;
}

pub fn fil_delete_tablespace(id: ulint) ibool {
    const sys = ensureSystem();
    if (sys.spaces.fetchRemove(id)) |entry| {
        _ = sys.spaces_by_name.remove(entry.value.name);
        freeSpace(entry.value);
        return compat.TRUE;
    }
    return compat.FALSE;
}

pub fn fil_discard_tablespace(id: ulint) ibool {
    return fil_delete_tablespace(id);
}

pub fn fil_rename_tablespace(old_name: ?[]const u8, id: ulint, new_name: []const u8) ibool {
    _ = old_name;
    const sys = ensureSystem();
    if (findSpace(id)) |space| {
        if (sys.spaces_by_name.contains(new_name)) {
            return compat.FALSE;
        }
        _ = sys.spaces_by_name.remove(space.name);
        const buf = std.heap.page_allocator.alloc(u8, new_name.len) catch return compat.FALSE;
        std.mem.copyForwards(u8, buf, new_name);
        std.heap.page_allocator.free(space.name);
        space.name = buf;
        _ = sys.spaces_by_name.put(space.name, space);
        return compat.TRUE;
    }
    return compat.FALSE;
}

pub fn fil_create_new_single_table_tablespace(
    space_id: *ulint,
    tablename: []const u8,
    is_temp: ibool,
    flags: ulint,
    size: ulint,
) ulint {
    _ = is_temp;
    const sys = ensureSystem();
    if (size < FIL_IBD_FILE_INITIAL_SIZE) {
        return 1;
    }
    if (space_id.* == 0) {
        sys.max_space_id += 1;
        space_id.* = sys.max_space_id;
    }
    if (fil_space_create(tablename, space_id.*, 0, FIL_TABLESPACE) == compat.FALSE) {
        return 1;
    }
    if (findSpace(space_id.*)) |space| {
        space.flags = flags;
    }
    fil_node_create(tablename, size, space_id.*, compat.FALSE);
    return DB_SUCCESS;
}

pub fn fil_open_single_table_tablespace(
    check_space_id: ibool,
    id: ulint,
    flags: ulint,
    name: []const u8,
) ibool {
    _ = check_space_id;
    _ = flags;
    _ = name;
    return if (findSpace(id) != null) compat.TRUE else compat.FALSE;
}

pub fn fil_reset_too_high_lsns(name: []const u8, current_lsn: ib_uint64_t) ibool {
    _ = name;
    _ = current_lsn;
    return compat.TRUE;
}

pub fn fil_load_single_table_tablespaces(recovery: ib_recovery_t) ulint {
    _ = recovery;
    return DB_SUCCESS;
}

pub fn fil_print_orphaned_tablespaces() void {}

pub fn fil_tablespace_deleted_or_being_deleted_in_mem(id: ulint, version: ib_int64_t) ibool {
    if (findSpace(id)) |space| {
        if (space.deleted) {
            return compat.TRUE;
        }
        if (version != -1 and space.version != version) {
            return compat.TRUE;
        }
        return compat.FALSE;
    }
    return compat.TRUE;
}

pub fn fil_tablespace_exists_in_mem(id: ulint) ibool {
    return if (findSpace(id) != null) compat.TRUE else compat.FALSE;
}

pub fn fil_space_for_table_exists_in_mem(
    id: ulint,
    name: []const u8,
    is_temp: ibool,
    mark_space: ibool,
    print_error_if_does_not_exist: ibool,
) ibool {
    _ = is_temp;
    _ = mark_space;
    _ = print_error_if_does_not_exist;
    if (findSpace(id)) |space| {
        if (std.mem.eql(u8, space.name, name)) {
            return compat.TRUE;
        }
    }
    return compat.FALSE;
}

pub fn fil_extend_tablespaces_to_stored_len() void {}

pub fn fil_extend_space_to_desired_size(actual_size: *ulint, space_id: ulint, size_after_extend: ulint) ibool {
    if (findSpace(space_id)) |space| {
        if (space.size < size_after_extend) {
            space.size = size_after_extend;
        }
        actual_size.* = space.size;
        return compat.TRUE;
    }
    actual_size.* = 0;
    return compat.FALSE;
}

pub fn fil_space_reserve_free_extents(id: ulint, n_free_now: ulint, n_to_reserve: ulint) ibool {
    if (findSpace(id)) |space| {
        if (n_free_now < n_to_reserve) {
            return compat.FALSE;
        }
        space.reserved_extents += n_to_reserve;
        return compat.TRUE;
    }
    return compat.FALSE;
}

pub fn fil_space_release_free_extents(id: ulint, n_reserved: ulint) void {
    if (findSpace(id)) |space| {
        if (space.reserved_extents >= n_reserved) {
            space.reserved_extents -= n_reserved;
        } else {
            space.reserved_extents = 0;
        }
    }
}

pub fn fil_space_get_n_reserved_extents(id: ulint) ulint {
    if (findSpace(id)) |space| {
        return space.reserved_extents;
    }
    return 0;
}

pub fn fil_io(
    type_: ulint,
    sync_: ibool,
    space_id: ulint,
    zip_size: ulint,
    block_offset: ulint,
    byte_offset: ulint,
    len: ulint,
    buf: ?*anyopaque,
    message: ?*anyopaque,
) ulint {
    _ = type_;
    _ = sync_;
    _ = zip_size;
    _ = block_offset;
    _ = byte_offset;
    _ = len;
    _ = buf;
    _ = message;
    if (findSpace(space_id) == null) {
        return DB_TABLESPACE_DELETED;
    }
    return DB_SUCCESS;
}

pub fn fil_aio_wait(segment: ulint) void {
    _ = segment;
}

pub fn fil_flush(space_id: ulint) void {
    _ = space_id;
}

pub fn fil_flush_file_spaces(purpose: ulint) void {
    _ = purpose;
}

pub fn fil_validate() ibool {
    return compat.TRUE;
}

pub fn fil_addr_is_null(addr: fil_addr_t) ibool {
    return if (addr.page == FIL_NULL) compat.TRUE else compat.FALSE;
}

pub fn fil_page_get_prev(page: [*]const byte) ulint {
    return mach.mach_read_from_4(page + FIL_PAGE_PREV);
}

pub fn fil_page_get_next(page: [*]const byte) ulint {
    return mach.mach_read_from_4(page + FIL_PAGE_NEXT);
}

fn write_u16_be(ptr: [*]byte, value: ulint) void {
    std.debug.assert(value <= 0xFFFF);
    ptr[0] = @as(byte, @intCast((value >> 8) & 0xFF));
    ptr[1] = @as(byte, @intCast(value & 0xFF));
}

fn read_u16_be(ptr: [*]const byte) ulint {
    return (@as(ulint, ptr[0]) << 8) | @as(ulint, ptr[1]);
}

pub fn fil_page_set_type(page: [*]byte, type_: ulint) void {
    write_u16_be(page + FIL_PAGE_TYPE, type_);
}

pub fn fil_page_get_type(page: [*]const byte) ulint {
    return read_u16_be(page + FIL_PAGE_TYPE);
}

pub fn fil_var_init() void {
    fil_n_log_flushes = 0;
    fil_n_pending_log_flushes = 0;
    fil_n_pending_tablespace_flushes = 0;
}

pub fn fil_rmdir(dbname: []const u8) ibool {
    std.fs.cwd().deleteTree(dbname) catch return compat.FALSE;
    return compat.TRUE;
}

pub fn fil_mkdir(dbname: []const u8) ibool {
    std.fs.cwd().makePath(dbname) catch return compat.FALSE;
    return compat.TRUE;
}

test "fil space create and node add" {
    fil_init(0, 32);
    defer fil_close();

    try std.testing.expect(fil_space_create("test", 1, 0, FIL_TABLESPACE) == compat.TRUE);
    fil_node_create("test.ibd", 10, 1, compat.FALSE);

    try std.testing.expect(fil_space_get_size(1) == 10);
    try std.testing.expect(fil_space_get_type(1) == FIL_TABLESPACE);
    try std.testing.expect(fil_check_adress_in_tablespace(1, 9) == compat.TRUE);
    try std.testing.expect(fil_check_adress_in_tablespace(1, 10) == compat.FALSE);
    try std.testing.expect(fil_tablespace_exists_in_mem(1) == compat.TRUE);

    try std.testing.expect(fil_delete_tablespace(1) == compat.TRUE);
    try std.testing.expect(fil_tablespace_exists_in_mem(1) == compat.FALSE);
}

test "fil page header helpers" {
    var page: [64]byte = undefined;
    fil_page_set_type(&page, FIL_PAGE_INDEX);
    try std.testing.expect(fil_page_get_type(&page) == FIL_PAGE_INDEX);
}

test "fil addr null" {
    try std.testing.expect(fil_addr_is_null(fil_addr_null) == compat.TRUE);
    try std.testing.expect(fil_addr_is_null(.{ .page = 1, .boffset = 0 }) == compat.FALSE);
}
