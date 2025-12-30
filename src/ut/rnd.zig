const std = @import("std");
const compat = @import("compat.zig");

pub const ulint = compat.ulint;

pub const UT_HASH_RANDOM_MASK: ulint = 1463735687;
pub const UT_HASH_RANDOM_MASK2: ulint = 1653893711;

const UT_RANDOM_1: f64 = 1.0412321;
const UT_RANDOM_2: f64 = 1.1131347;
const UT_RANDOM_3: f64 = 1.0132677;

pub inline fn ut_hash_ulint(key: ulint, table_size: ulint) ulint {
    return (key ^ UT_HASH_RANDOM_MASK2) % table_size;
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
