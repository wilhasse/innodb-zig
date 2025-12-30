const std = @import("std");
const compat = @import("compat.zig");
const byte = @import("byte.zig");

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;
pub const dulint = compat.Dulint;

pub const UT_END_OF_FIELD: ulint = 257;
pub const UT_HASH_RANDOM_MASK: ulint = 1463735687;
pub const UT_HASH_RANDOM_MASK2: ulint = 1653893711;

const UT_RND1: ulint = 151117737;
const UT_RND2: ulint = 119785373;
const UT_RND3: ulint = 85689495;
const UT_RND4: ulint = 76595339;
const UT_SUM_RND2: ulint = 98781234;
const UT_SUM_RND3: ulint = 126792457;
const UT_SUM_RND4: ulint = 63498502;
const UT_XOR_RND1: ulint = 187678878;
const UT_XOR_RND2: ulint = 143537923;

const UT_RANDOM_1: f64 = 1.0412321;
const UT_RANDOM_2: f64 = 1.1131347;
const UT_RANDOM_3: f64 = 1.0132677;

pub var ut_rnd_ulint_counter: ulint = 65654363;

pub inline fn ut_hash_ulint(key: ulint, table_size: ulint) ulint {
    return (key ^ UT_HASH_RANDOM_MASK2) % table_size;
}

pub fn ut_rnd_set_seed(seed: ulint) void {
    ut_rnd_ulint_counter = seed;
}

pub fn ut_rnd_gen_next_ulint(rnd: ulint) ulint {
    const n_bits: usize = @bitSizeOf(ulint);
    var val = rnd;
    val = UT_RND2 *% val +% UT_SUM_RND3;
    val = UT_XOR_RND1 ^ val;
    val = (val << 20) +% (val >> (n_bits - 20));
    val = UT_RND3 *% val +% UT_SUM_RND4;
    val = UT_XOR_RND2 ^ val;
    val = (val << 20) +% (val >> (n_bits - 20));
    val = UT_RND1 *% val +% UT_SUM_RND2;
    return val;
}

pub fn ut_rnd_gen_ulint() ulint {
    ut_rnd_ulint_counter = UT_RND1 *% ut_rnd_ulint_counter +% UT_RND2;
    return ut_rnd_gen_next_ulint(ut_rnd_ulint_counter);
}

pub fn ut_rnd_interval(low: ulint, high: ulint) ulint {
    std.debug.assert(high >= low);
    if (low == high) {
        return low;
    }
    const rnd = ut_rnd_gen_ulint();
    const range = (high - low) +% 1;
    return low + (rnd % range);
}

pub fn ut_rnd_gen_ibool() ibool {
    const x = ut_rnd_gen_ulint();
    if (((x >> 20) +% (x >> 15)) & 1 != 0) {
        return compat.TRUE;
    }
    return compat.FALSE;
}

pub fn ut_fold_ulint_pair(n1: ulint, n2: ulint) ulint {
    const mixed = (((n1 ^ n2 ^ UT_HASH_RANDOM_MASK2) << 8) +% n1) ^ UT_HASH_RANDOM_MASK;
    return mixed +% n2;
}

pub fn ut_fold_dulint(d: dulint) ulint {
    return ut_fold_ulint_pair(byte.ut_dulint_get_low(d), byte.ut_dulint_get_high(d));
}

pub fn ut_fold_string(str: [*]const u8) ulint {
    var fold: ulint = 0;
    var i: ulint = 0;
    while (str[i] != 0) : (i += 1) {
        fold = ut_fold_ulint_pair(fold, @as(ulint, str[i]));
    }
    return fold;
}

pub fn ut_fold_binary(str: [*]const u8, len: ulint) ulint {
    var fold: ulint = 0;
    var i: ulint = 0;
    while (i < len) : (i += 1) {
        fold = ut_fold_ulint_pair(fold, @as(ulint, str[i]));
    }
    return fold;
}

pub fn ut_find_prime(n_in: ulint) ulint {
    var n = n_in + 100;
    var pow2: ulint = 1;
    while (pow2 * 2 < n) {
        pow2 *= 2;
    }

    if (@as(f64, @floatFromInt(n)) < 1.05 * @as(f64, @floatFromInt(pow2))) {
        n = @as(ulint, @intFromFloat(@as(f64, @floatFromInt(n)) * UT_RANDOM_1));
    }

    pow2 *= 2;

    if (@as(f64, @floatFromInt(n)) > 0.95 * @as(f64, @floatFromInt(pow2))) {
        n = @as(ulint, @intFromFloat(@as(f64, @floatFromInt(n)) * UT_RANDOM_2));
    }

    if (n > pow2 - 20) {
        n += 30;
    }

    n = @as(ulint, @intFromFloat(@as(f64, @floatFromInt(n)) * UT_RANDOM_3));

    var candidate = n;
    while (true) : (candidate += 1) {
        var i: ulint = 2;
        while (i <= candidate / i) : (i += 1) {
            if (candidate % i == 0) {
                break;
            }
        }
        if (i > candidate / i) {
            return candidate;
        }
    }
}

test "ut_hash_ulint matches mask mod" {
    try std.testing.expectEqual(@as(ulint, 34), ut_hash_ulint(123, 97));
}

test "ut_find_prime returns expected prime for 100" {
    try std.testing.expectEqual(@as(ulint, 211), ut_find_prime(100));
}

test "ut rnd seed and interval" {
    ut_rnd_set_seed(1);
    const first = ut_rnd_gen_ulint();
    const second = ut_rnd_gen_ulint();
    try std.testing.expect(first != second);
    ut_rnd_set_seed(1);
    const repeat = ut_rnd_gen_ulint();
    try std.testing.expectEqual(first, repeat);

    const fixed = ut_rnd_interval(42, 42);
    try std.testing.expectEqual(@as(ulint, 42), fixed);
    const ranged = ut_rnd_interval(5, 10);
    try std.testing.expect(ranged >= 5 and ranged <= 10);
}

test "ut fold helpers" {
    const d = byte.ut_dulint_create(3, 7);
    const folded = ut_fold_dulint(d);
    try std.testing.expectEqual(ut_fold_ulint_pair(7, 3), folded);

    const s: [:0]const u8 = "abc";
    const fold_str = ut_fold_string(s.ptr);
    const fold_bin = ut_fold_binary(s.ptr, 3);
    try std.testing.expectEqual(fold_str, fold_bin);
}
