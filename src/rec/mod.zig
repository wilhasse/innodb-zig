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

// Offsets flags.
pub const REC_OFFS_COMPACT: ulint = 1 << 31;
pub const REC_OFFS_SQL_NULL: ulint = 1 << 31;
pub const REC_OFFS_EXTERNAL: ulint = 1 << 30;
pub const REC_OFFS_MASK: ulint = REC_OFFS_EXTERNAL - 1;

pub const FieldMeta = struct {
    fixed_len: ulint = 0,
    max_len: ulint = 0,
    nullable: bool = false,
    is_blob: bool = false,
};

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

pub fn rec_offs_get_n_alloc(offsets: []const ulint) ulint {
    return offsets[0];
}

pub fn rec_offs_set_n_alloc(offsets: []ulint, n_alloc: ulint) void {
    offsets[0] = n_alloc;
}

pub fn rec_offs_n_fields(offsets: []const ulint) ulint {
    return offsets[1];
}

pub fn rec_offs_set_n_fields(offsets: []ulint, n_fields: ulint) void {
    offsets[1] = n_fields;
}

fn rec_offs_base(offsets: []ulint) []ulint {
    return offsets[REC_OFFS_HEADER_SIZE..];
}

pub fn rec_init_offsets_fixed(field_lens: []const ulint, offsets: []ulint, compact: bool) void {
    const needed = @as(usize, @intCast(REC_OFFS_HEADER_SIZE + 1 + field_lens.len));
    std.debug.assert(offsets.len >= needed);
    rec_offs_set_n_fields(offsets, @intCast(field_lens.len));
    rec_offs_set_n_alloc(offsets, @intCast(offsets.len));

    var total: ulint = 0;
    const base = rec_offs_base(offsets);
    base[0] = if (compact)
        (REC_N_NEW_EXTRA_BYTES | REC_OFFS_COMPACT)
    else
        REC_N_OLD_EXTRA_BYTES;

    for (field_lens, 0..) |len, i| {
        total += len;
        base[1 + i] = total;
    }
}

fn bitsInBytes(n: ulint) ulint {
    return (n + 7) / 8;
}

pub fn rec_init_offsets_compact(rec: [*]const byte, extra: ulint, fields: []const FieldMeta, offsets: []ulint) void {
    const needed = @as(usize, @intCast(REC_OFFS_HEADER_SIZE + 1 + fields.len));
    std.debug.assert(offsets.len >= needed);
    rec_offs_set_n_fields(offsets, @intCast(fields.len));
    rec_offs_set_n_alloc(offsets, @intCast(offsets.len));

    var n_nullable: ulint = 0;
    for (fields) |field| {
        if (field.nullable) {
            n_nullable += 1;
        }
    }

    var offs: ulint = 0;
    var any_ext: ulint = 0;
    var null_mask: u8 = 1;
    var nulls = rec_ptr(rec, extra + 1);
    var lens = rec_ptr(nulls, bitsInBytes(n_nullable));

    const base = rec_offs_base(offsets);
    for (fields, 0..) |field, i| {
        var len: ulint = 0;
        if (field.nullable) {
            if (null_mask == 0) {
                nulls = rec_ptr(nulls, 1);
                null_mask = 1;
            }
            if ((nulls[0] & null_mask) != 0) {
                null_mask <<= 1;
                len = offs | REC_OFFS_SQL_NULL;
                base[1 + i] = len;
                continue;
            }
            null_mask <<= 1;
        }

        if (field.fixed_len != 0) {
            offs += field.fixed_len;
            len = offs;
            base[1 + i] = len;
            continue;
        }

        const len_byte: ulint = lens[0];
        lens = rec_ptr(lens, 1);
        if (field.max_len > 255 or field.is_blob) {
            if ((len_byte & 0x80) != 0) {
                len = (len_byte << 8) | lens[0];
                lens = rec_ptr(lens, 1);
                offs += len & 0x3fff;
                if ((len & 0x4000) != 0) {
                    any_ext = REC_OFFS_EXTERNAL;
                    len = offs | REC_OFFS_EXTERNAL;
                } else {
                    len = offs;
                }
                base[1 + i] = len;
                continue;
            }
        }

        offs += len_byte;
        len = offs;
        base[1 + i] = len;
    }

    const extra_size = @as(ulint, @intCast(@intFromPtr(rec) - (@intFromPtr(lens) + 1)));
    base[0] = extra_size | REC_OFFS_COMPACT | any_ext;
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

test "rec offsets fixed-length" {
    var offsets = [_]ulint{0} ** 16;
    const lens = [_]ulint{ 3, 4, 2 };

    rec_init_offsets_fixed(&lens, offsets[0..], true);
    const base = rec_offs_base(offsets[0..]);
    try std.testing.expectEqual(@as(ulint, REC_N_NEW_EXTRA_BYTES | REC_OFFS_COMPACT), base[0]);
    try std.testing.expectEqual(@as(ulint, 3), base[1]);
    try std.testing.expectEqual(@as(ulint, 7), base[2]);
    try std.testing.expectEqual(@as(ulint, 9), base[3]);
}

test "rec offsets varlen + nulls (compact)" {
    var buf = [_]byte{0} ** 64;
    const rec = @as([*]const byte, @ptrCast(&buf[32]));

    // One nullable field (field 1), marked NULL in the bitmap.
    const extra = REC_N_NEW_EXTRA_BYTES;
    buf[32 - (extra + 1)] = 0x01;
    // One length byte for field 0 (3 bytes).
    buf[32 - (extra + 1) - 1] = 3;

    const fields = [_]FieldMeta{
        .{ .fixed_len = 0, .max_len = 10, .nullable = false },
        .{ .fixed_len = 0, .max_len = 10, .nullable = true },
        .{ .fixed_len = 2, .nullable = false },
    };

    var offsets = [_]ulint{0} ** 16;
    rec_init_offsets_compact(rec, extra, &fields, offsets[0..]);
    const base = rec_offs_base(offsets[0..]);

    try std.testing.expectEqual(@as(ulint, 3), base[1]);
    try std.testing.expect((base[2] & REC_OFFS_SQL_NULL) != 0);
    try std.testing.expectEqual(@as(ulint, 3), base[2] & REC_OFFS_MASK);
    try std.testing.expectEqual(@as(ulint, 5), base[3]);
}
