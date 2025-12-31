const std = @import("std");
const builtin = @import("builtin");
const compat = @import("../ut/compat.zig");

pub const module_name = "rec";

pub const ulint = compat.ulint;
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
