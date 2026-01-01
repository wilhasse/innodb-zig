const std = @import("std");
const compat = @import("../ut/compat.zig");
const mach = @import("../mach/mod.zig");
const fil = @import("../fil/mod.zig");
const buf = @import("../buf/mod.zig");

pub const module_name = "fsp";

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;
pub const byte = compat.byte;
pub const ib_uint64_t = compat.ib_uint64_t;

pub const page_t = [*]byte;
pub const mtr_t = struct {};
pub const buf_block_t = struct {};
pub const fseg_header_t = byte;

pub const FSP_UP: byte = 111;
pub const FSP_DOWN: byte = 112;
pub const FSP_NO_DIR: byte = 113;

pub const FSP_EXTENT_SIZE: ulint = @as(ulint, 1) << (20 - compat.UNIV_PAGE_SIZE_SHIFT);
pub const FSEG_PAGE_DATA: ulint = fil.FIL_PAGE_DATA;

pub const FSEG_HDR_SPACE: ulint = 0;
pub const FSEG_HDR_PAGE_NO: ulint = 4;
pub const FSEG_HDR_OFFSET: ulint = 8;
pub const FSEG_HEADER_SIZE: ulint = 10;

pub const FSP_NORMAL: ulint = 1_000_000;
pub const FSP_UNDO: ulint = 2_000_000;
pub const FSP_CLEANING: ulint = 3_000_000;

pub const FSP_HEADER_OFFSET: ulint = fil.FIL_PAGE_DATA;
pub const FSP_SPACE_ID: ulint = 0;
pub const FSP_NOT_USED: ulint = 4;
pub const FSP_SIZE: ulint = 8;
pub const FSP_FREE_LIMIT: ulint = 12;
pub const FSP_SPACE_FLAGS: ulint = 16;

var log_fsp_current_free_limit: ulint = 0;
var fsp_current_size: ulint = 0;
var fsp_current_flags: ulint = 0;
var fsp_extent_map: []u8 = &[_]u8{};
var fsp_extent_map_space: ulint = fil.FIL_NULL;

pub fn fsp_init() void {
    log_fsp_current_free_limit = 0;
    fsp_current_size = 0;
    fsp_current_flags = 0;
    if (fsp_extent_map.len != 0) {
        std.heap.page_allocator.free(fsp_extent_map);
    }
    fsp_extent_map = &[_]u8{};
    fsp_extent_map_space = fil.FIL_NULL;
}

pub fn fsp_header_get_free_limit() ulint {
    return log_fsp_current_free_limit;
}

pub fn fsp_header_get_tablespace_size() ulint {
    return fsp_current_size;
}

pub fn fsp_get_size_low(page: [*]const byte) ulint {
    return mach.mach_read_from_4(page + FSP_HEADER_OFFSET + FSP_SIZE);
}

pub fn fsp_header_get_space_id(page: [*]const byte) ulint {
    return mach.mach_read_from_4(page + FSP_HEADER_OFFSET + FSP_SPACE_ID);
}

pub fn fsp_header_get_flags(page: [*]const byte) ulint {
    return mach.mach_read_from_4(page + FSP_HEADER_OFFSET + FSP_SPACE_FLAGS);
}

pub fn fsp_header_get_zip_size(page: [*]const byte) ulint {
    const flags = fsp_header_get_flags(page);
    return flags & 0xFFFF;
}

pub fn fsp_header_init_fields(page: [*]byte, space_id: ulint, flags: ulint) void {
    mach.mach_write_to_4(page + FSP_HEADER_OFFSET + FSP_SPACE_ID, space_id);
    mach.mach_write_to_4(page + FSP_HEADER_OFFSET + FSP_SPACE_FLAGS, flags);
}

pub fn fsp_header_init(space: ulint, size: ulint, mtr: *mtr_t) void {
    _ = mtr;
    fsp_current_size = size;
    log_fsp_current_free_limit = size;
    fsp_current_flags = 0;
    fsp_extent_map_init(space, size);
    fsp_header_write(space);
    fsp_extent_map_write(space);
}

pub fn fsp_header_inc_size(space: ulint, size_inc: ulint, mtr: *mtr_t) void {
    _ = mtr;
    fsp_current_size += size_inc;
    log_fsp_current_free_limit += size_inc;
    fsp_extent_map_extend(space, fsp_current_size);
    fsp_header_write(space);
    fsp_extent_map_write(space);
}

pub fn fseg_create(space: ulint, page: ulint, byte_offset: ulint, mtr: *mtr_t) ?*buf_block_t {
    _ = space;
    _ = page;
    _ = byte_offset;
    _ = mtr;
    return null;
}

pub fn fseg_create_general(
    space: ulint,
    page: ulint,
    byte_offset: ulint,
    has_done_reservation: ibool,
    mtr: *mtr_t,
) ?*buf_block_t {
    _ = space;
    _ = page;
    _ = byte_offset;
    _ = has_done_reservation;
    _ = mtr;
    return null;
}

pub fn fseg_n_reserved_pages(header: *fseg_header_t, used: *ulint, mtr: *mtr_t) ulint {
    _ = header;
    _ = mtr;
    used.* = 0;
    return 0;
}

pub fn fseg_alloc_free_page(seg_header: *fseg_header_t, hint: ulint, direction: byte, mtr: *mtr_t) ulint {
    _ = seg_header;
    _ = hint;
    _ = direction;
    _ = mtr;
    return fil.FIL_NULL;
}

pub fn fseg_alloc_free_page_general(
    seg_header: *fseg_header_t,
    hint: ulint,
    direction: byte,
    has_done_reservation: ibool,
    mtr: *mtr_t,
) ulint {
    _ = seg_header;
    _ = hint;
    _ = direction;
    _ = has_done_reservation;
    _ = mtr;
    return fil.FIL_NULL;
}

pub fn fsp_reserve_free_extents(
    n_reserved: *ulint,
    space: ulint,
    n_ext: ulint,
    alloc_type: ulint,
    mtr: *mtr_t,
) ibool {
    _ = alloc_type;
    _ = mtr;
    const map = fsp_extent_map_for(space) orelse {
        n_reserved.* = 0;
        return compat.FALSE;
    };
    var free_count: ulint = 0;
    for (map) |state| {
        if (state == 0) {
            free_count += 1;
        }
    }
    if (free_count < n_ext) {
        n_reserved.* = 0;
        return compat.FALSE;
    }
    var reserved: ulint = 0;
    for (map, 0..) |state, idx| {
        if (state == 0) {
            fsp_extent_map[idx] = 1;
            reserved += 1;
            if (reserved == n_ext) {
                break;
            }
        }
    }
    n_reserved.* = reserved;
    fsp_extent_map_write(space);
    return compat.TRUE;
}

pub fn fsp_get_available_space_in_free_extents(space: ulint) ib_uint64_t {
    const map = fsp_extent_map_for(space) orelse return 0;
    var free_count: ib_uint64_t = 0;
    for (map) |state| {
        if (state == 0) {
            free_count += 1;
        }
    }
    const pages = free_count * @as(ib_uint64_t, FSP_EXTENT_SIZE);
    return pages * @as(ib_uint64_t, compat.UNIV_PAGE_SIZE);
}

pub fn fsp_header_load(space: ulint) ibool {
    var page: [compat.UNIV_PAGE_SIZE]byte = undefined;
    if (fil.fil_read_page(space, 0, page[0..].ptr) != fil.DB_SUCCESS) {
        return compat.FALSE;
    }
    fsp_current_size = mach.mach_read_from_4(page[0..].ptr + FSP_HEADER_OFFSET + FSP_SIZE);
    log_fsp_current_free_limit = mach.mach_read_from_4(page[0..].ptr + FSP_HEADER_OFFSET + FSP_FREE_LIMIT);
    fsp_current_flags = mach.mach_read_from_4(page[0..].ptr + FSP_HEADER_OFFSET + FSP_SPACE_FLAGS);
    fsp_extent_map_load(space);
    return compat.TRUE;
}

pub fn fseg_free_page(seg_header: *fseg_header_t, space: ulint, page: ulint, mtr: *mtr_t) void {
    _ = seg_header;
    _ = space;
    _ = page;
    _ = mtr;
}

pub fn fseg_free_step(header: *fseg_header_t, mtr: *mtr_t) ibool {
    _ = header;
    _ = mtr;
    return compat.TRUE;
}

pub fn fseg_free_step_not_header(header: *fseg_header_t, mtr: *mtr_t) ibool {
    _ = header;
    _ = mtr;
    return compat.TRUE;
}

pub fn fsp_descr_page(zip_size: ulint, page_no: ulint) ibool {
    const descr_per_page = if (zip_size == 0) compat.UNIV_PAGE_SIZE else zip_size;
    return if (page_no % descr_per_page == 0) compat.TRUE else compat.FALSE;
}

pub fn fsp_parse_init_file_page(ptr: [*]byte, end_ptr: [*]byte, block: ?*buf_block_t) ?[*]byte {
    _ = block;
    if (@intFromPtr(ptr) > @intFromPtr(end_ptr)) {
        return null;
    }
    return end_ptr;
}

pub fn fsp_validate(space: ulint) ibool {
    _ = space;
    return compat.TRUE;
}

pub fn fsp_print(space: ulint) void {
    _ = space;
}

fn fsp_extent_map_init(space: ulint, size_pages: ulint) void {
    if (fsp_extent_map.len != 0) {
        std.heap.page_allocator.free(fsp_extent_map);
    }
    const extents = if (size_pages == 0) 0 else (size_pages + FSP_EXTENT_SIZE - 1) / FSP_EXTENT_SIZE;
    fsp_extent_map = if (extents == 0) &[_]u8{} else std.heap.page_allocator.alloc(u8, extents) catch &[_]u8{};
    if (fsp_extent_map.len != 0) {
        @memset(fsp_extent_map, 0);
        fsp_extent_map_space = space;
    } else {
        fsp_extent_map_space = fil.FIL_NULL;
    }
}

fn fsp_extent_map_extend(space: ulint, size_pages: ulint) void {
    if (space != fsp_extent_map_space) {
        return;
    }
    const extents = if (size_pages == 0) 0 else (size_pages + FSP_EXTENT_SIZE - 1) / FSP_EXTENT_SIZE;
    if (extents <= fsp_extent_map.len) {
        return;
    }
    const new_map = std.heap.page_allocator.alloc(u8, extents) catch return;
    @memset(new_map, 0);
    if (fsp_extent_map.len != 0) {
        std.mem.copyForwards(u8, new_map[0..fsp_extent_map.len], fsp_extent_map);
        std.heap.page_allocator.free(fsp_extent_map);
    }
    fsp_extent_map = new_map;
    fsp_extent_map_space = space;
}

fn fsp_extent_map_for(space: ulint) ?[]u8 {
    if (space != fsp_extent_map_space) {
        return null;
    }
    return fsp_extent_map;
}

fn fsp_header_write(space: ulint) void {
    var page: [compat.UNIV_PAGE_SIZE]byte = undefined;
    @memset(page[0..], 0);
    fsp_header_init_fields(page[0..].ptr, space, fsp_current_flags);
    mach.mach_write_to_4(page[0..].ptr + FSP_HEADER_OFFSET + FSP_SIZE, fsp_current_size);
    mach.mach_write_to_4(page[0..].ptr + FSP_HEADER_OFFSET + FSP_FREE_LIMIT, log_fsp_current_free_limit);
    mach.mach_write_to_4(page[0..].ptr + fil.FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID, space);
    fil.fil_page_set_type(page[0..].ptr, fil.FIL_PAGE_TYPE_FSP_HDR);
    buf.buf_flush_init_for_writing(page[0..].ptr, null, 0);
    _ = fil.fil_write_page(space, 0, page[0..].ptr);
}

fn fsp_extent_map_write(space: ulint) void {
    const map = fsp_extent_map_for(space) orelse return;
    if (map.len == 0) {
        return;
    }
    var page: [compat.UNIV_PAGE_SIZE]byte = undefined;
    @memset(page[0..], 0);
    mach.mach_write_to_4(page[0..].ptr + fil.FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID, space);
    fil.fil_page_set_type(page[0..].ptr, fil.FIL_PAGE_TYPE_XDES);
    const base = fil.FIL_PAGE_DATA;
    mach.mach_write_to_4(page[0..].ptr + base, @as(ulint, @intCast(map.len)));
    const max_copy = @min(map.len, compat.UNIV_PAGE_SIZE - base - 4);
    std.mem.copyForwards(u8, page[base + 4 .. base + 4 + max_copy], map[0..max_copy]);
    buf.buf_flush_init_for_writing(page[0..].ptr, null, 0);
    _ = fil.fil_write_page(space, 1, page[0..].ptr);
}

fn fsp_extent_map_load(space: ulint) void {
    var page: [compat.UNIV_PAGE_SIZE]byte = undefined;
    if (fil.fil_read_page(space, 1, page[0..].ptr) != fil.DB_SUCCESS) {
        fsp_extent_map_init(space, fsp_current_size);
        return;
    }
    const base = fil.FIL_PAGE_DATA;
    const count = mach.mach_read_from_4(page[0..].ptr + base);
    if (count == 0) {
        fsp_extent_map_init(space, fsp_current_size);
        return;
    }
    fsp_extent_map_init(space, count * FSP_EXTENT_SIZE);
    const max_copy = @min(fsp_extent_map.len, compat.UNIV_PAGE_SIZE - base - 4);
    if (max_copy != 0) {
        std.mem.copyForwards(u8, fsp_extent_map[0..max_copy], page[base + 4 .. base + 4 + max_copy]);
    }
}

test "fsp header fields" {
    var page: [128]byte = undefined;
    fsp_header_init_fields(&page, 7, 0x12);
    try std.testing.expect(fsp_header_get_space_id(&page) == 7);
    try std.testing.expect(fsp_header_get_flags(&page) == 0x12);

    mach.mach_write_to_4(page[0..].ptr + FSP_HEADER_OFFSET + FSP_SIZE, 99);
    try std.testing.expect(fsp_get_size_low(&page) == 99);
}

test "fsp header size tracking" {
    var mtr = mtr_t{};
    fsp_init();
    fsp_header_init(0, 5, &mtr);
    try std.testing.expect(fsp_header_get_tablespace_size() == 5);
    fsp_header_inc_size(0, 3, &mtr);
    try std.testing.expect(fsp_header_get_tablespace_size() == 8);
}

test "fsp header persists to disk" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);
    const prev_path = fil.fil_path_to_client_datadir;
    fil.fil_path_to_client_datadir = base;
    defer fil.fil_path_to_client_datadir = prev_path;

    fil.fil_init(0, 32);
    defer fil.fil_close();

    var space_id: ulint = 0;
    const err = fil.fil_create_new_single_table_tablespace(&space_id, "fspdisk", compat.FALSE, 0, 4);
    try std.testing.expectEqual(fil.DB_SUCCESS, err);

    var mtr = mtr_t{};
    fsp_header_init(space_id, 4, &mtr);

    var page: [compat.UNIV_PAGE_SIZE]byte = undefined;
    try std.testing.expectEqual(fil.DB_SUCCESS, fil.fil_read_page(space_id, 0, page[0..].ptr));
    const size_on_disk = mach.mach_read_from_4(page[0..].ptr + FSP_HEADER_OFFSET + FSP_SIZE);
    try std.testing.expectEqual(@as(ulint, 4), size_on_disk);
}

test "fsp descriptor page check" {
    try std.testing.expect(fsp_descr_page(0, 0) == compat.TRUE);
    try std.testing.expect(fsp_descr_page(0, 1) == compat.FALSE);
}
