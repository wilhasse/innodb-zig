const std = @import("std");
const compat = @import("compat.zig");
const log = @import("log.zig");

pub const module_name = "ut.util";

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;
pub const ib_int64_t = compat.ib_int64_t;
pub const ib_uint64_t = compat.ib_uint64_t;
pub const ib_time_t = i64;
pub const ib_stream_t = log.ib_stream_t;
pub const ib_logger_t = log.ib_logger_t;

pub const TEMP_INDEX_PREFIX: u8 = 0xFF;
pub const TEMP_INDEX_PREFIX_STR: []const u8 = "\xFF";

const NAME_LEN: usize = 64 * 3;
var ut_always_false: ibool = compat.FALSE;

fn cStrLen(str: [*]const u8) ulint {
    var i: ulint = 0;
    while (str[i] != 0) : (i += 1) {}
    return i;
}

fn getUtcTimestamp() struct {
    year: u16,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
} {
    const now = std.time.timestamp();
    const secs = if (now < 0) 0 else @as(u64, @intCast(now));
    const epoch = std.time.epoch.EpochSeconds{ .secs = secs };
    const year_day = epoch.getEpochDay().calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    const day_seconds = epoch.getDaySeconds();

    return .{
        .year = year_day.year,
        .month = @as(u8, @intCast(month_day.month.numeric())),
        .day = @as(u8, @intCast(month_day.day_index + 1)),
        .hour = @as(u8, @intCast(day_seconds.getHoursIntoDay())),
        .minute = @as(u8, @intCast(day_seconds.getMinutesIntoHour())),
        .second = @as(u8, @intCast(day_seconds.getSecondsIntoMinute())),
    };
}

fn writeTimestamp(buf: []u8, with_spaces: bool) usize {
    const ts = getUtcTimestamp();
    const year = @as(u16, @intCast(ts.year % 100));
    const fmt = if (with_spaces)
        "{:0>2}{:0>2}{:0>2} {: >2}:{:0>2}:{:0>2}"
    else
        "{:0>2}{:0>2}{:0>2}_{: >2}_{:0>2}_{:0>2}";

    const out = std.fmt.bufPrint(
        buf,
        fmt,
        .{ year, ts.month, ts.day, ts.hour, ts.minute, ts.second },
    ) catch return 0;
    return out.len;
}

pub fn ut_get_high32(a: ulint) ulint {
    const i = @as(ib_int64_t, @intCast(a));
    return @as(ulint, @intCast(i >> 32));
}

pub fn ut_time() ib_time_t {
    return std.time.timestamp();
}

pub fn ut_usectime(sec: *ulint, us: *ulint) i32 {
    const now = std.time.microTimestamp();
    if (now < 0) {
        sec.* = 0;
        us.* = 0;
        return -1;
    }
    sec.* = @as(ulint, @intCast(@divTrunc(now, 1_000_000)));
    us.* = @as(ulint, @intCast(@mod(now, 1_000_000)));
    return 0;
}

pub fn ut_time_us(tloc: ?*ib_uint64_t) ib_uint64_t {
    const now = std.time.microTimestamp();
    const us = if (now < 0) 0 else @as(ib_uint64_t, @intCast(now));
    if (tloc) |out| {
        out.* = us;
    }
    return us;
}

pub fn ut_time_ms() ulint {
    const now = std.time.milliTimestamp();
    return if (now < 0) 0 else @as(ulint, @intCast(now));
}

pub fn ut_difftime(time2: ib_time_t, time1: ib_time_t) f64 {
    return @as(f64, @floatFromInt(time2 - time1));
}

pub fn ut_print_timestamp(ib_stream: ib_stream_t) void {
    var buf: [32]u8 = undefined;
    const len = writeTimestamp(&buf, true);
    if (len > 0) {
        log.logTo(ib_stream, buf[0..len]);
    }
}

pub fn ut_sprintf_timestamp(buf: [*]u8) void {
    var tmp: [32]u8 = undefined;
    const len = writeTimestamp(&tmp, true);
    if (len == 0) {
        buf[0] = 0;
        return;
    }
    std.mem.copyForwards(u8, buf[0..len], tmp[0..len]);
    buf[len] = 0;
}

pub fn ut_sprintf_timestamp_without_extra_chars(buf: [*]u8) void {
    var tmp: [32]u8 = undefined;
    const len = writeTimestamp(&tmp, false);
    if (len == 0) {
        buf[0] = 0;
        return;
    }
    std.mem.copyForwards(u8, buf[0..len], tmp[0..len]);
    buf[len] = 0;
}

pub fn ut_get_year_month_day(year: *ulint, month: *ulint, day: *ulint) void {
    const ts = getUtcTimestamp();
    year.* = @as(ulint, ts.year);
    month.* = @as(ulint, ts.month);
    day.* = @as(ulint, ts.day);
}

pub fn ut_delay(delay: ulint) ulint {
    var i: ulint = 0;
    var j: ulint = 0;
    const limit = delay *% 50;
    while (i < limit) : (i += 1) {
        j +%= i;
        std.atomic.spinLoopHint();
    }
    if (ut_always_false != 0) {
        ut_always_false = @as(ibool, @intCast(j));
    }
    return j;
}

pub fn ut_print_buf(ib_stream: ib_stream_t, buf: *const anyopaque, len: ulint) void {
    const data = @as([*]const u8, @ptrCast(buf));
    log.logfTo(ib_stream, " len {d}; hex ", .{len});
    var i: ulint = 0;
    while (i < len) : (i += 1) {
        log.logfTo(ib_stream, "{x:0>2}", .{data[i]});
    }
    log.logTo(ib_stream, "; asc ");
    i = 0;
    while (i < len) : (i += 1) {
        const c = data[i];
        const out = if (std.ascii.isPrint(c)) c else ' ';
        log.logfTo(ib_stream, "{c}", .{out});
    }
    log.logTo(ib_stream, ";");
}

pub fn ut_min(n1: ulint, n2: ulint) ulint {
    return if (n1 <= n2) n1 else n2;
}

pub fn ut_max(n1: ulint, n2: ulint) ulint {
    return if (n1 <= n2) n2 else n1;
}

pub fn ut_pair_min(a: *ulint, b: *ulint, a1: ulint, b1: ulint, a2: ulint, b2: ulint) void {
    if (a1 == a2) {
        a.* = a1;
        b.* = ut_min(b1, b2);
    } else if (a1 < a2) {
        a.* = a1;
        b.* = b1;
    } else {
        a.* = a2;
        b.* = b2;
    }
}

pub fn ut_ulint_cmp(a: ulint, b: ulint) i32 {
    if (a < b) return -1;
    if (a == b) return 0;
    return 1;
}

pub fn ut_pair_cmp(a1: ulint, a2: ulint, b1: ulint, b2: ulint) i32 {
    if (a1 > b1) return 1;
    if (a1 < b1) return -1;
    if (a2 > b2) return 1;
    if (a2 < b2) return -1;
    return 0;
}

pub fn ut_2_log(n_in: ulint) ulint {
    std.debug.assert(n_in > 0);
    var n = n_in - 1;
    var res: ulint = 0;
    while (true) {
        n /= 2;
        if (n == 0) break;
        res += 1;
    }
    return res + 1;
}

pub fn ut_2_exp(n: ulint) ulint {
    return @as(ulint, 1) << @as(usize, @intCast(n));
}

pub fn ut_2_power_up(n: ulint) ulint {
    std.debug.assert(n > 0);
    var res: ulint = 1;
    while (res < n) {
        res *%= 2;
    }
    return res;
}

pub fn ut_print_filename(ib_stream: ib_stream_t, name: [*]const u8) void {
    log.logTo(ib_stream, "'");
    var i: ulint = 0;
    while (true) : (i += 1) {
        const c = name[i];
        if (c == 0) break;
        if (c == '\'') {
            log.logfTo(ib_stream, "{c}", .{c});
        }
        log.logfTo(ib_stream, "{c}", .{c});
    }
    log.logTo(ib_stream, "'");
}

pub fn ut_print_name(ib_stream: ib_stream_t, trx: ?*anyopaque, table_id: ibool, name: [*]const u8) void {
    _ = trx;
    _ = table_id;
    ut_print_namel(ib_stream, name, cStrLen(name));
}

pub fn ut_print_namel(ib_stream: ib_stream_t, name: [*]const u8, namelen: ulint) void {
    var buf: [3 * NAME_LEN]u8 = undefined;
    const len = @min(@as(usize, @intCast(namelen)), buf.len);
    std.mem.copyForwards(u8, buf[0..len], name[0..len]);
    log.logfTo(ib_stream, "{s}", .{buf[0..len]});
}

pub inline fn ut_is_2pow(n: ulint) bool {
    return compat.ut_is_2pow(n);
}

pub inline fn ut_2pow_remainder(n: ulint, m: ulint) ulint {
    return compat.ut_2pow_remainder(n, m);
}

pub inline fn ut_2pow_round(n: ulint, m: ulint) ulint {
    return n & ~(@as(ulint, m) - 1);
}

pub inline fn ut_calc_align_down(n: ulint, m: ulint) ulint {
    return ut_2pow_round(n, m);
}

pub inline fn ut_calc_align(n: ulint, m: ulint) ulint {
    return (n + (m - 1)) & ~(m - 1);
}

pub inline fn ut_bits_in_bytes(b: ulint) ulint {
    return (b + 7) / 8;
}

test "ut min max and comparisons" {
    try std.testing.expectEqual(@as(ulint, 2), ut_min(2, 5));
    try std.testing.expectEqual(@as(ulint, 5), ut_max(2, 5));
    try std.testing.expectEqual(@as(i32, -1), ut_ulint_cmp(1, 2));
    try std.testing.expectEqual(@as(i32, 0), ut_ulint_cmp(2, 2));
    try std.testing.expectEqual(@as(i32, 1), ut_ulint_cmp(3, 2));

    var a: ulint = 0;
    var b: ulint = 0;
    ut_pair_min(&a, &b, 1, 9, 2, 3);
    try std.testing.expectEqual(@as(ulint, 1), a);
    try std.testing.expectEqual(@as(ulint, 9), b);

    try std.testing.expectEqual(@as(i32, 1), ut_pair_cmp(2, 1, 1, 9));
    try std.testing.expectEqual(@as(i32, 0), ut_pair_cmp(2, 1, 2, 1));
    try std.testing.expectEqual(@as(i32, -1), ut_pair_cmp(1, 5, 2, 0));
}

test "ut log and power helpers" {
    try std.testing.expectEqual(@as(ulint, 1), ut_2_exp(0));
    try std.testing.expectEqual(@as(ulint, 8), ut_2_exp(3));
    try std.testing.expectEqual(@as(ulint, 3), ut_2_log(5));
    try std.testing.expectEqual(@as(ulint, 16), ut_2_power_up(9));
    try std.testing.expectEqual(@as(ulint, 1), ut_bits_in_bytes(1));
    try std.testing.expectEqual(@as(ulint, 2), ut_bits_in_bytes(9));

    const diff = ut_difftime(10, 4);
    try std.testing.expect(diff == 6.0);
}

test "ut get high32 handles 64-bit values" {
    if (@sizeOf(ulint) >= 8) {
        const value: ulint = 0x1_0000_0000;
        try std.testing.expectEqual(@as(ulint, 1), ut_get_high32(value));
    } else {
        try std.testing.expectEqual(@as(ulint, 0), ut_get_high32(0xFFFF_FFFF));
    }
}

test "ut print filename quoting" {
    const prev_logger = log.getLogger();
    const prev_stream = log.getStream();
    defer log.setLogger(prev_logger, prev_stream);

    const Capture = struct {
        buf: [128]u8 = undefined,
        len: usize = 0,

        fn logger(stream: log.Stream, message: []const u8) void {
            const capture = @as(*@This(), @ptrCast(@alignCast(stream.?)));
            const avail = capture.buf.len - capture.len;
            const to_copy = @min(avail, message.len);
            std.mem.copyForwards(u8, capture.buf[capture.len .. capture.len + to_copy], message[0..to_copy]);
            capture.len += to_copy;
        }
    };

    var capture = Capture{};
    const stream = @as(log.Stream, @ptrCast(&capture));
    log.setLogger(Capture.logger, stream);

    const name: [:0]const u8 = "a'b";
    ut_print_filename(stream, name.ptr);
    try std.testing.expectEqualStrings("'a''b'", capture.buf[0..capture.len]);
}
