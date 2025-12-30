const std = @import("std");

pub const RowVersion = struct {
    key: i64,
    deleted: bool,
    prev: ?*RowVersion = null,
};

pub fn row_version_add(head: ?*RowVersion, key: i64, deleted: bool, allocator: std.mem.Allocator) *RowVersion {
    const node = allocator.create(RowVersion) catch @panic("row_version_add");
    node.* = .{ .key = key, .deleted = deleted, .prev = head };
    return node;
}

pub fn row_version_get(head: ?*RowVersion, depth: usize) ?*RowVersion {
    var cur = head;
    var i: usize = 0;
    while (cur) |node| : (i += 1) {
        if (i == depth) {
            return node;
        }
        cur = node.prev;
    }
    return null;
}

pub fn row_version_free(head: ?*RowVersion, allocator: std.mem.Allocator) void {
    var cur = head;
    while (cur) |node| {
        const next = node.prev;
        allocator.destroy(node);
        cur = next;
    }
}

test "row version chain" {
    const allocator = std.testing.allocator;
    var head: ?*RowVersion = null;
    head = row_version_add(head, 1, false, allocator);
    head = row_version_add(head, 2, true, allocator);
    head = row_version_add(head, 3, false, allocator);
    defer row_version_free(head, allocator);

    const v0 = row_version_get(head, 0) orelse return error.TestExpectedEqual;
    const v1 = row_version_get(head, 1) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(i64, 3), v0.key);
    try std.testing.expect(v1.deleted);
}
