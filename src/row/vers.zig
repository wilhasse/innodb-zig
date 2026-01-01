const std = @import("std");
const read = @import("../read/mod.zig");
const trx_types = @import("../trx/types.zig");
const errors = @import("../ut/errors.zig");

// ============================================================================
// Row Version Chain (IBD-212)
// ============================================================================

/// A versioned row with transaction information for MVCC
pub const RowVersion = struct {
    key: i64,
    deleted: bool,
    /// Transaction that created this version
    trx_id: read.trx_id_t = 0,
    /// Roll pointer to undo log for building previous versions
    roll_ptr: trx_types.roll_ptr_t = trx_types.dulintZero(),
    /// Link to previous version (older)
    prev: ?*RowVersion = null,
};

/// Result of version visibility check
pub const VersionResult = enum {
    visible,        // Version is visible to the read view
    not_visible,    // Version is not visible (need older version)
    not_found,      // No visible version exists (freshly inserted after view)
};

pub fn row_version_add(head: ?*RowVersion, key: i64, deleted: bool, allocator: std.mem.Allocator) *RowVersion {
    const node = allocator.create(RowVersion) catch @panic("row_version_add");
    node.* = .{ .key = key, .deleted = deleted, .prev = head };
    return node;
}

/// Add a version with transaction information
pub fn row_version_add_with_trx(
    head: ?*RowVersion,
    key: i64,
    deleted: bool,
    trx_id: read.trx_id_t,
    allocator: std.mem.Allocator,
) *RowVersion {
    const node = allocator.create(RowVersion) catch @panic("row_version_add_with_trx");
    node.* = .{
        .key = key,
        .deleted = deleted,
        .trx_id = trx_id,
        .prev = head,
    };
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

// ============================================================================
// MVCC Consistent Read (IBD-212)
// ============================================================================

/// Check if a specific row version is visible to a read view
pub fn row_vers_is_visible(version: *const RowVersion, view: *const read.read_view_t) bool {
    return read.read_view_is_visible(view, version.trx_id);
}

/// Find the visible version of a row for a consistent read.
/// Traverses the version chain until a visible version is found.
///
/// Returns:
/// - The visible version if found
/// - null if no visible version exists (row was inserted after view was created)
pub fn row_vers_find_visible(
    head: ?*RowVersion,
    view: *const read.read_view_t,
) ?*RowVersion {
    var current = head;

    while (current) |version| {
        if (read.read_view_is_visible(view, version.trx_id)) {
            return version;
        }
        current = version.prev;
    }

    // No visible version found - row didn't exist when view was created
    return null;
}

/// Build the version of a row that a consistent read should see.
/// This is the main entry point for MVCC reads.
///
/// Returns:
/// - .visible with the version if found
/// - .not_found if no visible version exists
pub fn row_vers_build_for_consistent_read(
    head: ?*RowVersion,
    view: *const read.read_view_t,
) struct { result: VersionResult, version: ?*RowVersion } {
    const visible = row_vers_find_visible(head, view);

    if (visible) |v| {
        // Check if the visible version is delete-marked
        if (v.deleted) {
            // Row is deleted as of this view
            return .{ .result = .not_found, .version = null };
        }
        return .{ .result = .visible, .version = v };
    }

    return .{ .result = .not_found, .version = null };
}

/// Check if we need to go to an older version of a record.
/// This is called when the current record's trx_id is not visible.
pub fn row_vers_must_build_old_version(
    trx_id: read.trx_id_t,
    view: *const read.read_view_t,
) bool {
    return !read.read_view_is_visible(view, trx_id);
}

/// Counts how many versions in the chain are visible to the view
pub fn row_vers_count_visible(
    head: ?*RowVersion,
    view: *const read.read_view_t,
) usize {
    var count: usize = 0;
    var current = head;

    while (current) |version| {
        if (read.read_view_is_visible(view, version.trx_id)) {
            count += 1;
        }
        current = version.prev;
    }

    return count;
}

/// Get the length of the version chain
pub fn row_vers_chain_length(head: ?*RowVersion) usize {
    var count: usize = 0;
    var current = head;

    while (current) |_| {
        count += 1;
        current = current.?.prev;
    }

    return count;
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

// ============================================================================
// IBD-212 MVCC Tests
// ============================================================================

test "row_version_add_with_trx creates version with transaction info" {
    const allocator = std.testing.allocator;
    var head: ?*RowVersion = null;
    head = row_version_add_with_trx(head, 100, false, 42, allocator);
    defer row_version_free(head, allocator);

    try std.testing.expectEqual(@as(i64, 100), head.?.key);
    try std.testing.expectEqual(@as(read.trx_id_t, 42), head.?.trx_id);
    try std.testing.expectEqual(false, head.?.deleted);
}

test "row_vers_chain_length counts all versions" {
    const allocator = std.testing.allocator;
    var head: ?*RowVersion = null;
    head = row_version_add_with_trx(head, 1, false, 10, allocator);
    head = row_version_add_with_trx(head, 2, false, 20, allocator);
    head = row_version_add_with_trx(head, 3, false, 30, allocator);
    defer row_version_free(head, allocator);

    try std.testing.expectEqual(@as(usize, 3), row_vers_chain_length(head));
    try std.testing.expectEqual(@as(usize, 0), row_vers_chain_length(null));
}

test "row_vers_find_visible finds correct version" {
    const allocator = std.testing.allocator;

    // Create version chain: trx 30 -> trx 20 -> trx 10 (oldest)
    var head: ?*RowVersion = null;
    head = row_version_add_with_trx(head, 1, false, 10, allocator); // oldest
    head = row_version_add_with_trx(head, 2, false, 20, allocator);
    head = row_version_add_with_trx(head, 3, false, 30, allocator); // newest
    defer row_version_free(head, allocator);

    // Create view where trx 10 and 20 are committed, but 30 is active
    const active = [_]read.trx_id_t{30};
    const view = read.read_view_open_with_active(40, 50, &active, allocator);
    defer read.read_view_close(view);

    // Should find version with trx_id=20 (newest visible)
    const visible = row_vers_find_visible(head, view);
    try std.testing.expect(visible != null);
    try std.testing.expectEqual(@as(read.trx_id_t, 20), visible.?.trx_id);
}

test "row_vers_find_visible returns null when no version visible" {
    const allocator = std.testing.allocator;

    // Create version with future transaction
    var head: ?*RowVersion = null;
    head = row_version_add_with_trx(head, 1, false, 100, allocator);
    defer row_version_free(head, allocator);

    // Create view from trx 10, max_trx_id=50 - trx 100 is in the future
    const view = read.read_view_open_with_active(10, 50, &[_]read.trx_id_t{}, allocator);
    defer read.read_view_close(view);

    // No version should be visible
    try std.testing.expect(row_vers_find_visible(head, view) == null);
}

test "row_vers_build_for_consistent_read with deleted row" {
    const allocator = std.testing.allocator;

    // Create chain: deleted version (trx 10) -> original (trx 5)
    var head: ?*RowVersion = null;
    head = row_version_add_with_trx(head, 1, false, 5, allocator); // original
    head = row_version_add_with_trx(head, 1, true, 10, allocator); // delete-marked
    defer row_version_free(head, allocator);

    // View sees trx 10 as committed
    const view = read.read_view_open_with_active(20, 30, &[_]read.trx_id_t{}, allocator);
    defer read.read_view_close(view);

    // Should return not_found because visible version is deleted
    const result = row_vers_build_for_consistent_read(head, view);
    try std.testing.expectEqual(VersionResult.not_found, result.result);
    try std.testing.expect(result.version == null);
}

test "row_vers_build_for_consistent_read with visible version" {
    const allocator = std.testing.allocator;

    // Create simple version chain
    var head: ?*RowVersion = null;
    head = row_version_add_with_trx(head, 42, false, 5, allocator);
    defer row_version_free(head, allocator);

    // View sees all committed
    const view = read.read_view_open_with_active(10, 20, &[_]read.trx_id_t{}, allocator);
    defer read.read_view_close(view);

    const result = row_vers_build_for_consistent_read(head, view);
    try std.testing.expectEqual(VersionResult.visible, result.result);
    try std.testing.expect(result.version != null);
    try std.testing.expectEqual(@as(i64, 42), result.version.?.key);
}

test "row_vers_count_visible counts correctly" {
    const allocator = std.testing.allocator;

    // Create chain: trx 30, 20, 10, 5
    var head: ?*RowVersion = null;
    head = row_version_add_with_trx(head, 1, false, 5, allocator);
    head = row_version_add_with_trx(head, 2, false, 10, allocator);
    head = row_version_add_with_trx(head, 3, false, 20, allocator);
    head = row_version_add_with_trx(head, 4, false, 30, allocator);
    defer row_version_free(head, allocator);

    // View where 30 is active (not visible), others committed
    const active = [_]read.trx_id_t{30};
    const view = read.read_view_open_with_active(40, 50, &active, allocator);
    defer read.read_view_close(view);

    // Should count 3 visible versions (5, 10, 20)
    try std.testing.expectEqual(@as(usize, 3), row_vers_count_visible(head, view));
}

test "row_vers_must_build_old_version" {
    const allocator = std.testing.allocator;

    // View where trx 15 is active
    const active = [_]read.trx_id_t{15};
    const view = read.read_view_open_with_active(20, 30, &active, allocator);
    defer read.read_view_close(view);

    // Must build old if trx_id is not visible
    try std.testing.expect(row_vers_must_build_old_version(15, view)); // active - need old
    try std.testing.expect(row_vers_must_build_old_version(50, view)); // future - need old
    try std.testing.expect(!row_vers_must_build_old_version(5, view)); // old - visible
    try std.testing.expect(!row_vers_must_build_old_version(20, view)); // own - visible
}

test "row_vers_is_visible direct check" {
    const allocator = std.testing.allocator;

    // Create a version
    var head: ?*RowVersion = null;
    head = row_version_add_with_trx(head, 1, false, 15, allocator);
    defer row_version_free(head, allocator);

    // View where trx 15 is active
    const active = [_]read.trx_id_t{15};
    const view = read.read_view_open_with_active(20, 30, &active, allocator);
    defer read.read_view_close(view);

    // trx 15 is active, so not visible
    try std.testing.expect(!row_vers_is_visible(head.?, view));
}
