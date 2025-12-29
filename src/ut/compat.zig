const std = @import("std");
const builtin = @import("builtin");

// C-compat types and constants derived from include/univ.i and api0api.h.
// ABI guarantees:
// - ulint matches pointer size (compile-time assert)
// - Dulint uses extern layout to match C struct ordering
pub const byte = u8;

pub const ib_int16_t = i16;
pub const ib_uint16_t = u16;
pub const ib_int32_t = i32;
pub const ib_uint32_t = u32;
pub const ib_int64_t = i64;
pub const ib_uint64_t = u64;

pub const ulint = usize;
pub const lint = isize;

pub const ib_ulint_t = ulint;
pub const ib_bool_t = ib_ulint_t;
pub const ibool = ulint;

pub const IB_TRUE: ib_bool_t = 0x1;
pub const IB_FALSE: ib_bool_t = 0x0;
pub const TRUE: ibool = 1;
pub const FALSE: ibool = 0;

pub const UNIV_PAGE_SIZE_SHIFT: u8 = 14;
pub const UNIV_PAGE_SIZE: ulint = @as(ulint, 1) << UNIV_PAGE_SIZE_SHIFT;

pub const UNIV_MAX_PARALLELISM: ulint = 32;

pub const ULINT_UNDEFINED: ulint = std.math.maxInt(ulint);
pub const ULINT_MAX: ulint = std.math.maxInt(ulint) - 1;
pub const ULINT32_UNDEFINED: u32 = 0xFFFF_FFFF;

pub const IB_UINT64_T_MAX: ib_uint64_t = std.math.maxInt(ib_uint64_t);
pub const IB_ULONGLONG_MAX: ib_uint64_t = std.math.maxInt(ib_uint64_t);

pub const UNIV_SQL_NULL: u32 = ULINT32_UNDEFINED;
pub const UNIV_EXTERN_STORAGE_FIELD: u32 = UNIV_SQL_NULL - @as(u32, @intCast(UNIV_PAGE_SIZE));

pub const ULINTPF: []const u8 = if (builtin.os.tag == .windows and @sizeOf(ulint) == 8)
    "%I64u"
else
    "%lu";

pub const native_endian = builtin.cpu.arch.endian();
pub const is_little_endian: bool = native_endian == .little;

pub const Dulint = extern struct {
    high: ulint,
    low: ulint,
};

pub inline fn univExpect(expr: bool, expected: bool) bool {
    if (expr == expected) {
        @branchHint(.likely);
        return expr;
    } else {
        @branchHint(.unlikely);
        return expr;
    }
}

pub inline fn univLikely(cond: bool) bool {
    return univExpect(cond, true);
}

pub inline fn univUnlikely(cond: bool) bool {
    return univExpect(cond, false);
}

pub inline fn univLikelyNull(ptr: ?*anyopaque) bool {
    return univExpect(ptr == null, true);
}

pub inline fn alignUp(value: usize, alignment: usize) usize {
    std.debug.assert(alignment != 0);
    std.debug.assert((alignment & (alignment - 1)) == 0);
    return (value + alignment - 1) & ~(alignment - 1);
}

pub inline fn alignDown(value: usize, alignment: usize) usize {
    std.debug.assert(alignment != 0);
    std.debug.assert((alignment & (alignment - 1)) == 0);
    return value & ~(alignment - 1);
}

pub inline fn isAligned(value: usize, alignment: usize) bool {
    std.debug.assert(alignment != 0);
    return (value & (alignment - 1)) == 0;
}

pub inline fn arrSize(arr: anytype) comptime_int {
    return switch (@typeInfo(@TypeOf(arr))) {
        .array => |info| info.len,
        else => @compileError("arrSize expects an array"),
    };
}

comptime {
    if (@sizeOf(ulint) != @sizeOf(*anyopaque)) {
        @compileError("ulint must match pointer size");
    }
}

test "compat constants and sizes" {
    try std.testing.expect(@sizeOf(ulint) == @sizeOf(*anyopaque));
    try std.testing.expect(@sizeOf(Dulint) == 2 * @sizeOf(ulint));
    try std.testing.expect(@alignOf(Dulint) == @alignOf(ulint));

    try std.testing.expect(UNIV_PAGE_SIZE == (@as(ulint, 1) << UNIV_PAGE_SIZE_SHIFT));
    try std.testing.expect(UNIV_EXTERN_STORAGE_FIELD == UNIV_SQL_NULL - @as(u32, @intCast(UNIV_PAGE_SIZE)));
    try std.testing.expect(ULINT_UNDEFINED == std.math.maxInt(ulint));
    try std.testing.expect(ULINT_MAX == std.math.maxInt(ulint) - 1);
}

test "compat alignment helpers" {
    try std.testing.expect(alignUp(5, 4) == 8);
    try std.testing.expect(alignDown(5, 4) == 4);
    try std.testing.expect(isAligned(8, 4));
    try std.testing.expect(!isAligned(6, 4));
}

test "compat arrSize" {
    const data = [_]u8{ 1, 2, 3, 4 };
    try std.testing.expect(arrSize(data) == 4);
}
