const std = @import("std");
const builtin = @import("builtin");
const compat = @import("../ut/compat.zig");
const mach = @import("../mach/mod.zig");

pub const module_name = "rec";

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;
pub const byte = compat.byte;

// Physical record bytes.
pub const rec_t = byte;

// Maximum values for various fields (for non-blob tuples).
pub const REC_MAX_N_FIELDS: ulint = 1024 - 1;
pub const REC_MAX_HEAP_NO: ulint = 2 * 8192 - 1;
pub const REC_MAX_N_OWNED: ulint = 16 - 1;

// Maximum indexed column length (or prefix length), in bytes.
pub const REC_MAX_INDEX_COL_LEN: ulint = 768;

// Info bits.
pub const REC_INFO_MIN_REC_FLAG: ulint = 0x10;
pub const REC_INFO_DELETED_FLAG: ulint = 0x20;

// Extra bytes in record headers.
pub const REC_N_OLD_EXTRA_BYTES: ulint = 6;
pub const REC_N_NEW_EXTRA_BYTES: ulint = 5;

// Record status values.
pub const REC_STATUS_ORDINARY: ulint = 0;
pub const REC_STATUS_NODE_PTR: ulint = 1;
pub const REC_STATUS_INFIMUM: ulint = 2;
pub const REC_STATUS_SUPREMUM: ulint = 3;

// Compact record header layout.
pub const REC_NEW_HEAP_NO: ulint = 4;
pub const REC_HEAP_NO_SHIFT: ulint = 3;

// Length of a B-tree node pointer (child page number), in bytes.
pub const REC_NODE_PTR_SIZE: ulint = 4;

// Offsets array header size.
pub const REC_OFFS_HEADER_SIZE: ulint = if (builtin.mode == .Debug) 4 else 2;
pub const REC_OFFS_NORMAL_SIZE: ulint = 100;
pub const REC_OFFS_SMALL_SIZE: ulint = 10;

// Bit-field offsets and masks.
pub const REC_NEXT: ulint = 2;
pub const REC_NEXT_MASK: ulint = 0xFFFF;
pub const REC_NEXT_SHIFT: ulint = 0;

pub const REC_OLD_SHORT: ulint = 3;
pub const REC_OLD_SHORT_MASK: ulint = 0x1;
pub const REC_OLD_SHORT_SHIFT: ulint = 0;

pub const REC_OLD_N_FIELDS: ulint = 4;
pub const REC_OLD_N_FIELDS_MASK: ulint = 0x7FE;
pub const REC_OLD_N_FIELDS_SHIFT: ulint = 1;

pub const REC_NEW_STATUS: ulint = 3;
pub const REC_NEW_STATUS_MASK: ulint = 0x7;
pub const REC_NEW_STATUS_SHIFT: ulint = 0;

pub const REC_OLD_HEAP_NO: ulint = 5;
pub const REC_HEAP_NO_MASK: ulint = 0xFFF8;

pub const REC_OLD_N_OWNED: ulint = 6;
pub const REC_NEW_N_OWNED: ulint = 5;
pub const REC_N_OWNED_MASK: ulint = 0xF;
pub const REC_N_OWNED_SHIFT: ulint = 0;

pub const REC_OLD_INFO_BITS: ulint = 6;
pub const REC_NEW_INFO_BITS: ulint = 5;
pub const REC_INFO_BITS_MASK: ulint = 0xF0;
pub const REC_INFO_BITS_SHIFT: ulint = 0;

fn rec_ptr(rec: [*]const byte, offs: ulint) [*]const byte {
    return @as([*]const byte, @ptrFromInt(@intFromPtr(rec) - @as(usize, @intCast(offs))));
}

fn rec_ptr_mut(rec: [*]byte, offs: ulint) [*]byte {
    return @as([*]byte, @ptrFromInt(@intFromPtr(rec) - @as(usize, @intCast(offs))));
}

pub fn rec_get_bit_field_1(rec: [*]const byte, offs: ulint, mask: ulint, shift: ulint) ulint {
    const ptr = rec_ptr(rec, offs);
    return (mach.mach_read_from_1(ptr) & mask) >> shift;
}

pub fn rec_set_bit_field_1(rec: [*]byte, val: ulint, offs: ulint, mask: ulint, shift: ulint) void {
    const ptr = rec_ptr_mut(rec, offs);
    const current = mach.mach_read_from_1(ptr);
    mach.mach_write_to_1(ptr, (current & ~mask) | (val << shift));
}

pub fn rec_get_bit_field_2(rec: [*]const byte, offs: ulint, mask: ulint, shift: ulint) ulint {
    const ptr = rec_ptr(rec, offs);
    return (mach.mach_read_from_2(ptr) & mask) >> shift;
}

pub fn rec_set_bit_field_2(rec: [*]byte, val: ulint, offs: ulint, mask: ulint, shift: ulint) void {
    const ptr = rec_ptr_mut(rec, offs);
    const current = mach.mach_read_from_2(ptr);
    mach.mach_write_to_2(ptr, (current & ~mask) | (val << shift));
}

pub fn rec_get_n_owned_old(rec: [*]const byte) ulint {
    return rec_get_bit_field_1(rec, REC_OLD_N_OWNED, REC_N_OWNED_MASK, REC_N_OWNED_SHIFT);
}

pub fn rec_set_n_owned_old(rec: [*]byte, n_owned: ulint) void {
    rec_set_bit_field_1(rec, n_owned, REC_OLD_N_OWNED, REC_N_OWNED_MASK, REC_N_OWNED_SHIFT);
}

pub fn rec_get_n_owned_new(rec: [*]const byte) ulint {
    return rec_get_bit_field_1(rec, REC_NEW_N_OWNED, REC_N_OWNED_MASK, REC_N_OWNED_SHIFT);
}

pub fn rec_set_n_owned_new(rec: [*]byte, n_owned: ulint) void {
    rec_set_bit_field_1(rec, n_owned, REC_NEW_N_OWNED, REC_N_OWNED_MASK, REC_N_OWNED_SHIFT);
}

pub fn rec_get_info_bits(rec: [*]const byte, compact: bool) ulint {
    return rec_get_bit_field_1(
        rec,
        if (compact) REC_NEW_INFO_BITS else REC_OLD_INFO_BITS,
        REC_INFO_BITS_MASK,
        REC_INFO_BITS_SHIFT,
    );
}

pub fn rec_set_info_bits_old(rec: [*]byte, bits: ulint) void {
    rec_set_bit_field_1(rec, bits, REC_OLD_INFO_BITS, REC_INFO_BITS_MASK, REC_INFO_BITS_SHIFT);
}

pub fn rec_set_info_bits_new(rec: [*]byte, bits: ulint) void {
    rec_set_bit_field_1(rec, bits, REC_NEW_INFO_BITS, REC_INFO_BITS_MASK, REC_INFO_BITS_SHIFT);
}

pub fn rec_get_deleted_flag(rec: [*]const byte, compact: bool) ibool {
    const bits = rec_get_info_bits(rec, compact);
    return if ((bits & REC_INFO_DELETED_FLAG) != 0) 1 else 0;
}

pub fn rec_set_deleted_flag_old(rec: [*]byte, flag: ibool) void {
    var bits = rec_get_info_bits(rec, false);
    if (flag != 0) {
        bits |= REC_INFO_DELETED_FLAG;
    } else {
        bits &= ~REC_INFO_DELETED_FLAG;
    }
    rec_set_info_bits_old(rec, bits);
}

pub fn rec_set_deleted_flag_new(rec: [*]byte, flag: ibool) void {
    var bits = rec_get_info_bits(rec, true);
    if (flag != 0) {
        bits |= REC_INFO_DELETED_FLAG;
    } else {
        bits &= ~REC_INFO_DELETED_FLAG;
    }
    rec_set_info_bits_new(rec, bits);
}

pub fn rec_get_min_rec_flag(rec: [*]const byte, compact: bool) ibool {
    const bits = rec_get_info_bits(rec, compact);
    return if ((bits & REC_INFO_MIN_REC_FLAG) != 0) 1 else 0;
}

pub fn rec_set_min_rec_flag_old(rec: [*]byte, flag: ibool) void {
    var bits = rec_get_info_bits(rec, false);
    if (flag != 0) {
        bits |= REC_INFO_MIN_REC_FLAG;
    } else {
        bits &= ~REC_INFO_MIN_REC_FLAG;
    }
    rec_set_info_bits_old(rec, bits);
}

pub fn rec_set_min_rec_flag_new(rec: [*]byte, flag: ibool) void {
    var bits = rec_get_info_bits(rec, true);
    if (flag != 0) {
        bits |= REC_INFO_MIN_REC_FLAG;
    } else {
        bits &= ~REC_INFO_MIN_REC_FLAG;
    }
    rec_set_info_bits_new(rec, bits);
}

pub fn rec_get_heap_no_old(rec: [*]const byte) ulint {
    return rec_get_bit_field_2(rec, REC_OLD_HEAP_NO, REC_HEAP_NO_MASK, REC_HEAP_NO_SHIFT);
}

pub fn rec_set_heap_no_old(rec: [*]byte, heap_no: ulint) void {
    rec_set_bit_field_2(rec, heap_no, REC_OLD_HEAP_NO, REC_HEAP_NO_MASK, REC_HEAP_NO_SHIFT);
}

pub fn rec_get_heap_no_new(rec: [*]const byte) ulint {
    return rec_get_bit_field_2(rec, REC_NEW_HEAP_NO, REC_HEAP_NO_MASK, REC_HEAP_NO_SHIFT);
}

pub fn rec_set_heap_no_new(rec: [*]byte, heap_no: ulint) void {
    rec_set_bit_field_2(rec, heap_no, REC_NEW_HEAP_NO, REC_HEAP_NO_MASK, REC_HEAP_NO_SHIFT);
}

test "rec constants match C defaults" {
    try std.testing.expectEqual(@as(ulint, 1023), REC_MAX_N_FIELDS);
    try std.testing.expectEqual(@as(ulint, 16383), REC_MAX_HEAP_NO);
    try std.testing.expectEqual(@as(ulint, 15), REC_MAX_N_OWNED);
    try std.testing.expectEqual(@as(ulint, 768), REC_MAX_INDEX_COL_LEN);
    try std.testing.expectEqual(@as(ulint, 6), REC_N_OLD_EXTRA_BYTES);
    try std.testing.expectEqual(@as(ulint, 5), REC_N_NEW_EXTRA_BYTES);
    try std.testing.expectEqual(@as(ulint, 0), REC_STATUS_ORDINARY);
    try std.testing.expectEqual(@as(ulint, 3), REC_STATUS_SUPREMUM);
    try std.testing.expectEqual(@as(ulint, 4), REC_NEW_HEAP_NO);
    try std.testing.expectEqual(@as(ulint, 3), REC_HEAP_NO_SHIFT);
    try std.testing.expectEqual(@as(ulint, 4), REC_NODE_PTR_SIZE);
}

test "rec header bit helpers (compact)" {
    var buf = [_]byte{0} ** 32;
    const rec = @as([*]byte, @ptrCast(&buf[16]));

    rec_set_n_owned_new(rec, 5);
    try std.testing.expectEqual(@as(ulint, 5), rec_get_n_owned_new(rec));

    rec_set_heap_no_new(rec, 1234);
    try std.testing.expectEqual(@as(ulint, 1234), rec_get_heap_no_new(rec));

    rec_set_deleted_flag_new(rec, 1);
    try std.testing.expectEqual(@as(ibool, 1), rec_get_deleted_flag(rec, true));
    rec_set_deleted_flag_new(rec, 0);
    try std.testing.expectEqual(@as(ibool, 0), rec_get_deleted_flag(rec, true));

    rec_set_min_rec_flag_new(rec, 1);
    try std.testing.expectEqual(@as(ibool, 1), rec_get_min_rec_flag(rec, true));
    rec_set_min_rec_flag_new(rec, 0);
    try std.testing.expectEqual(@as(ibool, 0), rec_get_min_rec_flag(rec, true));
}
