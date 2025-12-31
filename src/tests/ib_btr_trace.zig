const std = @import("std");
const btr = @import("../btr/mod.zig");

const ArrayList = std.array_list.Managed;

test "btr trace output hash" {
    var buf = ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();

    try btr.btr_debug_generate_trace(std.testing.allocator, 0xC0FFEE, 60, false, buf.writer());

    var hasher = std.hash.Fnv1a_64.init();
    hasher.update(buf.items);
    const digest = hasher.final();
    try std.testing.expectEqual(@as(u64, 0xe0eb_c16f_0500_addc), digest);
}
