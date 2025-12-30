const std = @import("std");

pub fn row_merge_sorted(a: []const i64, b: []const i64, allocator: std.mem.Allocator) []i64 {
    const out = allocator.alloc(i64, a.len + b.len) catch @panic("row_merge_sorted");
    var i: usize = 0;
    var j: usize = 0;
    var k: usize = 0;
    while (i < a.len and j < b.len) : (k += 1) {
        if (a[i] <= b[j]) {
            out[k] = a[i];
            i += 1;
        } else {
            out[k] = b[j];
            j += 1;
        }
    }
    while (i < a.len) : (i += 1) {
        out[k] = a[i];
        k += 1;
    }
    while (j < b.len) : (j += 1) {
        out[k] = b[j];
        k += 1;
    }
    return out;
}

test "row merge sorted arrays" {
    const allocator = std.testing.allocator;
    const a = [_]i64{ 1, 3, 5 };
    const b = [_]i64{ 2, 4, 6 };
    const merged = row_merge_sorted(a[0..], b[0..], allocator);
    defer allocator.free(merged);
    try std.testing.expectEqualSlices(i64, &[_]i64{ 1, 2, 3, 4, 5, 6 }, merged);
}
