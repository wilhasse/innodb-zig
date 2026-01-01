const std = @import("std");
const compat = @import("../ut/compat.zig");
const trx = @import("../trx/mod.zig");

pub const module_name = "read";

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;
pub const trx_id_t = compat.ib_uint64_t;
pub const undo_no_t = compat.ib_uint64_t;

pub const VIEW_NORMAL: ulint = 1;
pub const VIEW_HIGH_GRANULARITY: ulint = 2;

// ============================================================================
// Read View (IBD-211)
// ============================================================================

/// Read view for MVCC consistent reads.
/// A read view determines which transaction versions are visible.
///
/// Visibility rules:
/// - trx_id < up_limit_id: visible (committed before view was created)
/// - trx_id >= low_limit_id: not visible (started after view was created)
/// - trx_id in trx_ids[]: not visible (was active when view was created)
/// - trx_id == creator_trx_id: visible (own changes)
pub const read_view_t = struct {
    type: ulint = VIEW_NORMAL,
    undo_no: undo_no_t = 0,
    /// Transactions with id < this are always visible
    low_limit_no: trx_id_t = 0,
    /// All trx ids >= this are not visible (next trx id to be assigned)
    low_limit_id: trx_id_t = 0,
    /// All trx ids < this are visible (smallest active trx id)
    up_limit_id: trx_id_t = 0,
    /// Number of active transactions when view was created
    n_trx_ids: ulint = 0,
    /// Array of active transaction ids (not visible in this view)
    trx_ids: []trx_id_t = &[_]trx_id_t{},
    /// Transaction that created this view (own changes visible)
    creator_trx_id: trx_id_t = 0,
    allocator: std.mem.Allocator = std.heap.page_allocator,
};

pub const cursor_view_t = struct {
    read_view: ?*read_view_t = null,
    n_client_tables_in_use: ulint = 0,
    allocator: std.mem.Allocator = std.heap.page_allocator,
};

fn read_view_create_low(n: ulint, allocator: std.mem.Allocator) *read_view_t {
    const view = allocator.create(read_view_t) catch @panic("read_view_create_low");
    view.* = .{};
    view.n_trx_ids = n;
    view.allocator = allocator;
    if (n > 0) {
        view.trx_ids = allocator.alloc(trx_id_t, n) catch @panic("read_view_create_low");
    } else {
        view.trx_ids = &[_]trx_id_t{};
    }
    return view;
}

/// Opens a read view at the current point in time.
/// Simple version without active transaction list - uses max_trx_id as limits.
pub fn read_view_open_now(cr_trx_id: trx_id_t, allocator: std.mem.Allocator) *read_view_t {
    const view = read_view_create_low(0, allocator);
    view.creator_trx_id = cr_trx_id;
    // Without transaction system, use creator_trx_id as both limits
    view.low_limit_id = cr_trx_id;
    view.up_limit_id = cr_trx_id;
    view.low_limit_no = cr_trx_id;
    view.type = VIEW_NORMAL;
    return view;
}

/// Opens a read view with explicit active transaction list.
/// This is the full MVCC version that properly handles concurrent transactions.
///
/// Parameters:
/// - cr_trx_id: The transaction creating the view
/// - max_trx_id: The next transaction ID to be assigned (low_limit)
/// - active_trx_ids: Slice of currently active transaction IDs (will be copied)
/// - allocator: Memory allocator
pub fn read_view_open_with_active(
    cr_trx_id: trx_id_t,
    max_trx_id: trx_id_t,
    active_trx_ids: []const trx_id_t,
    allocator: std.mem.Allocator,
) *read_view_t {
    // Count active transactions excluding creator
    var n_active: ulint = 0;
    for (active_trx_ids) |id| {
        if (id != cr_trx_id) {
            n_active += 1;
        }
    }

    const view = read_view_create_low(n_active, allocator);
    view.creator_trx_id = cr_trx_id;
    view.type = VIEW_NORMAL;
    view.undo_no = 0;

    // No future transactions should be visible
    view.low_limit_no = max_trx_id;
    view.low_limit_id = max_trx_id;

    // Copy active transaction IDs (excluding creator)
    var idx: usize = 0;
    for (active_trx_ids) |id| {
        if (id != cr_trx_id) {
            view.trx_ids[idx] = id;
            idx += 1;
        }
    }

    // Set up_limit_id to smallest active trx id
    if (n_active > 0) {
        var min_id = view.trx_ids[0];
        for (view.trx_ids) |id| {
            if (id < min_id) {
                min_id = id;
            }
        }
        view.up_limit_id = min_id;
    } else {
        view.up_limit_id = view.low_limit_id;
    }

    return view;
}

/// Makes a copy of the oldest existing read view, or opens a new one.
/// Used for purge to determine which undo records can be safely removed.
pub fn read_view_oldest_copy_or_open_new(cr_trx_id: trx_id_t, allocator: std.mem.Allocator) *read_view_t {
    // Without a global view list, just open a new view
    return read_view_open_now(cr_trx_id, allocator);
}

/// Clone an existing read view
pub fn read_view_clone(source: *const read_view_t, allocator: std.mem.Allocator) *read_view_t {
    const view = read_view_create_low(source.n_trx_ids, allocator);
    view.type = source.type;
    view.undo_no = source.undo_no;
    view.low_limit_no = source.low_limit_no;
    view.low_limit_id = source.low_limit_id;
    view.up_limit_id = source.up_limit_id;
    view.creator_trx_id = source.creator_trx_id;

    // Copy active transaction IDs
    if (source.n_trx_ids > 0) {
        @memcpy(view.trx_ids, source.trx_ids);
    }

    return view;
}

pub fn read_view_close(view: *read_view_t) void {
    if (view.trx_ids.len > 0) {
        view.allocator.free(view.trx_ids);
    }
    view.allocator.destroy(view);
}

pub fn read_view_close_for_read_committed(trx_: *trx.trx_t) void {
    _ = trx_;
}

/// Checks if a transaction's changes are visible to this read view.
///
/// Visibility rules:
/// 1. trx_id < up_limit_id: visible (committed before any active transaction)
/// 2. trx_id >= low_limit_id: not visible (started after view was created)
/// 3. trx_id in active list: not visible (was active when view was created)
/// 4. Otherwise: visible (was committed when view was created)
///
/// Note: The creator's own changes (creator_trx_id) are always visible,
/// but this function doesn't check that - caller should check separately.
pub fn read_view_sees_trx_id(view: *const read_view_t, trx_id: trx_id_t) ibool {
    // Transaction committed before any active transaction - visible
    if (trx_id < view.up_limit_id) {
        return compat.TRUE;
    }

    // Transaction started after view was created - not visible
    if (trx_id >= view.low_limit_id) {
        return compat.FALSE;
    }

    // Check if transaction was active when view was created
    for (view.trx_ids) |id| {
        if (id == trx_id) {
            return compat.FALSE;
        }
    }

    // Transaction was committed when view was created - visible
    return compat.TRUE;
}

/// Check if a row version is visible to this read view.
/// This is the main MVCC visibility check that also considers the creator.
pub fn read_view_is_visible(view: *const read_view_t, trx_id: trx_id_t) bool {
    // Own changes are always visible
    if (trx_id == view.creator_trx_id) {
        return true;
    }

    return read_view_sees_trx_id(view, trx_id) == compat.TRUE;
}

/// Check if a transaction is definitely visible (committed before view)
pub fn read_view_definitely_sees(view: *const read_view_t, trx_id: trx_id_t) bool {
    return trx_id < view.up_limit_id;
}

/// Check if a transaction is definitely not visible (started after view)
pub fn read_view_definitely_not_sees(view: *const read_view_t, trx_id: trx_id_t) bool {
    return trx_id >= view.low_limit_id;
}

pub fn read_view_print(view: *const read_view_t) void {
    std.debug.print("read_view: low_limit_id={d} up_limit_id={d} n_trx_ids={d}\n", .{
        view.low_limit_id,
        view.up_limit_id,
        view.n_trx_ids,
    });
}

pub fn read_cursor_view_create(trx_: *trx.trx_t, allocator: std.mem.Allocator) *cursor_view_t {
    const cur = allocator.create(cursor_view_t) catch @panic("read_cursor_view_create");
    cur.* = .{};
    cur.allocator = allocator;
    cur.read_view = read_view_open_now(trx_.id, allocator);
    return cur;
}

pub fn read_cursor_view_close(trx_: *trx.trx_t, cur: *cursor_view_t) void {
    _ = trx_;
    if (cur.read_view) |view| {
        read_view_close(view);
    }
    cur.allocator.destroy(cur);
}

pub fn read_cursor_set(trx_: *trx.trx_t, curview: ?*cursor_view_t) void {
    _ = trx_;
    _ = curview;
}

test "read view open now defaults" {
    const allocator = std.testing.allocator;
    const view = read_view_open_now(10, allocator);
    defer read_view_close(view);
    try std.testing.expectEqual(@as(ulint, 0), view.n_trx_ids);
    try std.testing.expectEqual(@as(trx_id_t, 10), view.low_limit_id);
    try std.testing.expectEqual(@as(trx_id_t, 10), view.up_limit_id);
}

test "read view sees trx id logic" {
    const allocator = std.testing.allocator;
    const view = read_view_create_low(1, allocator);
    defer read_view_close(view);
    view.up_limit_id = 5;
    view.low_limit_id = 10;
    view.trx_ids[0] = 8;
    try std.testing.expectEqual(compat.TRUE, read_view_sees_trx_id(view, 4));
    try std.testing.expectEqual(compat.FALSE, read_view_sees_trx_id(view, 8));
    try std.testing.expectEqual(compat.TRUE, read_view_sees_trx_id(view, 9));
    try std.testing.expectEqual(compat.FALSE, read_view_sees_trx_id(view, 10));
}

// ============================================================================
// IBD-211 Enhanced Tests
// ============================================================================

test "read_view_open_with_active creates proper view" {
    const allocator = std.testing.allocator;
    const active = [_]trx_id_t{ 5, 7, 9 };

    // Transaction 10 creates view, max_trx_id = 15
    const view = read_view_open_with_active(10, 15, &active, allocator);
    defer read_view_close(view);

    try std.testing.expectEqual(@as(trx_id_t, 10), view.creator_trx_id);
    try std.testing.expectEqual(@as(trx_id_t, 15), view.low_limit_id);
    try std.testing.expectEqual(@as(ulint, 3), view.n_trx_ids); // All active (10 not in list)
    try std.testing.expectEqual(@as(trx_id_t, 5), view.up_limit_id); // Smallest active
}

test "read_view_open_with_active excludes creator" {
    const allocator = std.testing.allocator;
    const active = [_]trx_id_t{ 5, 10, 12 }; // 10 is the creator

    const view = read_view_open_with_active(10, 20, &active, allocator);
    defer read_view_close(view);

    // Creator (10) should be excluded from active list
    try std.testing.expectEqual(@as(ulint, 2), view.n_trx_ids);
    try std.testing.expectEqual(@as(trx_id_t, 5), view.up_limit_id);
}

test "read_view_is_visible with MVCC scenario" {
    const allocator = std.testing.allocator;
    // Scenario: trx 10 creates view, active transactions: 7, 8, 9
    // Committed transactions: 1-6
    // Next trx id: 15
    const active = [_]trx_id_t{ 7, 8, 9 };
    const view = read_view_open_with_active(10, 15, &active, allocator);
    defer read_view_close(view);

    // Committed before any active (< 7) - visible
    try std.testing.expect(read_view_is_visible(view, 5));
    try std.testing.expect(read_view_is_visible(view, 6));

    // Active when view created (7, 8, 9) - not visible
    try std.testing.expect(!read_view_is_visible(view, 7));
    try std.testing.expect(!read_view_is_visible(view, 8));
    try std.testing.expect(!read_view_is_visible(view, 9));

    // Own changes (10) - visible
    try std.testing.expect(read_view_is_visible(view, 10));

    // Future transactions (>= 15) - not visible
    try std.testing.expect(!read_view_is_visible(view, 15));
    try std.testing.expect(!read_view_is_visible(view, 20));
}

test "read_view_clone creates independent copy" {
    const allocator = std.testing.allocator;
    const active = [_]trx_id_t{ 5, 7 };
    const original = read_view_open_with_active(10, 15, &active, allocator);
    defer read_view_close(original);

    const cloned = read_view_clone(original, allocator);
    defer read_view_close(cloned);

    try std.testing.expectEqual(original.creator_trx_id, cloned.creator_trx_id);
    try std.testing.expectEqual(original.low_limit_id, cloned.low_limit_id);
    try std.testing.expectEqual(original.up_limit_id, cloned.up_limit_id);
    try std.testing.expectEqual(original.n_trx_ids, cloned.n_trx_ids);

    // Should have same visibility
    try std.testing.expectEqual(
        read_view_is_visible(original, 5),
        read_view_is_visible(cloned, 5),
    );
}

test "read_view with no active transactions" {
    const allocator = std.testing.allocator;
    const active = [_]trx_id_t{};

    const view = read_view_open_with_active(10, 15, &active, allocator);
    defer read_view_close(view);

    try std.testing.expectEqual(@as(ulint, 0), view.n_trx_ids);
    // When no active transactions, up_limit = low_limit
    try std.testing.expectEqual(view.low_limit_id, view.up_limit_id);

    // All committed (< 15) should be visible
    try std.testing.expect(read_view_is_visible(view, 5));
    try std.testing.expect(read_view_is_visible(view, 14));
    // Future (>= 15) not visible
    try std.testing.expect(!read_view_is_visible(view, 15));
}

test "read_view_definitely helper functions" {
    const allocator = std.testing.allocator;
    const view = read_view_create_low(1, allocator);
    defer read_view_close(view);

    view.up_limit_id = 10;
    view.low_limit_id = 20;
    view.trx_ids[0] = 15;

    // Definitely sees (< up_limit_id)
    try std.testing.expect(read_view_definitely_sees(view, 5));
    try std.testing.expect(!read_view_definitely_sees(view, 15));

    // Definitely not sees (>= low_limit_id)
    try std.testing.expect(read_view_definitely_not_sees(view, 20));
    try std.testing.expect(read_view_definitely_not_sees(view, 25));
    try std.testing.expect(!read_view_definitely_not_sees(view, 15));
}
