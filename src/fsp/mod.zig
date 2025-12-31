const std = @import("std");
const compat = @import("../ut/compat.zig");
const mach = @import("../mach/mod.zig");
const fil = @import("../fil/mod.zig");

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

pub fn fsp_init() void {
    log_fsp_current_free_limit = 0;
    fsp_current_size = 0;
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
    _ = space;
    _ = mtr;
    fsp_current_size = size;
    log_fsp_current_free_limit = size;
}

pub fn fsp_header_inc_size(space: ulint, size_inc: ulint, mtr: *mtr_t) void {
    _ = space;
    _ = mtr;
    fsp_current_size += size_inc;
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
    _ = space;
    _ = alloc_type;
    _ = mtr;
    n_reserved.* = n_ext;
    return compat.TRUE;
}

pub fn fsp_get_available_space_in_free_extents(space: ulint) ib_uint64_t {
    _ = space;
    return 0;
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

test "fsp descriptor page check" {
    try std.testing.expect(fsp_descr_page(0, 0) == compat.TRUE);
    try std.testing.expect(fsp_descr_page(0, 1) == compat.FALSE);
}
