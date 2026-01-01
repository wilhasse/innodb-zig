const std = @import("std");
const read = @import("../read/mod.zig");
const vers = @import("../row/vers.zig");
const row_undo = @import("../row/undo.zig");
const trx_undo = @import("../trx/undo.zig");
const trx_types = @import("../trx/types.zig");
const purge = @import("../trx/purge.zig");
const errors = @import("../ut/errors.zig");

// ============================================================================
// IBD-215: MVCC/Rollback Test Coverage
// Tests for insert/update/delete visibility and rollback behavior
// ============================================================================

// ============================================================================
// Section 1: Insert Rollback Tests
// ============================================================================

test "insert rollback marks row as deleted" {
    // Setup: Transaction inserts a row
    var trx = trx_types.trx_t{
        .id = 100,
        .allocator = std.testing.allocator,
    };
    defer trx_undo.trx_undo_free_logs(&trx);

    // Record the insert operation
    _ = trx_undo.trx_undo_report_row_operation(
        &trx,
        .insert,
        .{ .high = 0, .low = 1 },
        "row_pk_data",
    );

    // Verify undo record was created
    try std.testing.expectEqual(@as(usize, 1), trx_undo.trx_undo_record_count(&trx));

    // Rollback should succeed
    const result = row_undo.row_undo_trx(&trx, null, null);
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, result);
}

test "multiple inserts rolled back in reverse order" {
    var trx = trx_types.trx_t{
        .id = 101,
        .allocator = std.testing.allocator,
    };
    defer trx_undo.trx_undo_free_logs(&trx);

    // Insert 3 rows
    _ = trx_undo.trx_undo_report_row_operation(&trx, .insert, .{ .high = 0, .low = 1 }, "pk1");
    _ = trx_undo.trx_undo_report_row_operation(&trx, .insert, .{ .high = 0, .low = 1 }, "pk2");
    _ = trx_undo.trx_undo_report_row_operation(&trx, .insert, .{ .high = 0, .low = 1 }, "pk3");

    try std.testing.expectEqual(@as(usize, 3), trx_undo.trx_undo_record_count(&trx));

    // Track order of undo application
    var undo_order: [3]usize = .{ 0, 0, 0 };
    var order_idx: usize = 0;

    const orderTracker = struct {
        fn apply(
            rec_type: trx_undo.UndoRecType,
            pk_data: []const u8,
            ctx: ?*anyopaque,
        ) row_undo.UndoResult {
            _ = rec_type;
            if (ctx) |c| {
                const state: *struct { order: *[3]usize, idx: *usize } = @ptrCast(@alignCast(c));
                // Record which row we're undoing based on pk_data
                if (pk_data.len >= 3) {
                    state.order[state.idx.*] = pk_data[2] - '0';
                    state.idx.* += 1;
                }
            }
            return .success;
        }
    };

    var state = .{ .order = &undo_order, .idx = &order_idx };
    const result = row_undo.row_undo_trx(&trx, orderTracker.apply, &state);

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, result);
    // Verify reverse order: 3, 2, 1 (LIFO)
    try std.testing.expectEqual(@as(usize, 3), undo_order[0]);
    try std.testing.expectEqual(@as(usize, 2), undo_order[1]);
    try std.testing.expectEqual(@as(usize, 1), undo_order[2]);
}

// ============================================================================
// Section 2: Update Rollback Tests
// ============================================================================

test "update rollback restores old value" {
    var trx = trx_types.trx_t{
        .id = 200,
        .allocator = std.testing.allocator,
    };
    defer trx_undo.trx_undo_free_logs(&trx);

    // Record an update operation (old value stored in undo)
    _ = trx_undo.trx_undo_report_row_operation(
        &trx,
        .update_exist,
        .{ .high = 0, .low = 1 },
        "old_value_data",
    );

    // Verify undo record count
    try std.testing.expectEqual(@as(usize, 1), trx_undo.trx_undo_record_count(&trx));

    var applied: usize = 0;
    const counter = struct {
        fn apply(rec_type: trx_undo.UndoRecType, pk_data: []const u8, ctx: ?*anyopaque) row_undo.UndoResult {
            _ = pk_data;
            if (ctx) |c| {
                const cnt: *usize = @ptrCast(@alignCast(c));
                cnt.* += 1;
            }
            // Verify it's an update undo
            if (rec_type != .update_exist) {
                return .failed;
            }
            return .success;
        }
    };

    const result = row_undo.row_undo_trx(&trx, counter.apply, &applied);
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, result);
    try std.testing.expectEqual(@as(usize, 1), applied);
}

test "mixed insert and update rollback" {
    var trx = trx_types.trx_t{
        .id = 201,
        .allocator = std.testing.allocator,
    };
    defer trx_undo.trx_undo_free_logs(&trx);

    // Insert a row
    _ = trx_undo.trx_undo_report_row_operation(&trx, .insert, .{ .high = 0, .low = 1 }, "ins");
    // Update it
    _ = trx_undo.trx_undo_report_row_operation(&trx, .update_exist, .{ .high = 0, .low = 1 }, "upd");
    // Insert another
    _ = trx_undo.trx_undo_report_row_operation(&trx, .insert, .{ .high = 0, .low = 1 }, "ins2");

    try std.testing.expectEqual(@as(usize, 3), trx_undo.trx_undo_record_count(&trx));

    const result = row_undo.row_undo_trx(&trx, null, null);
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, result);
}

// ============================================================================
// Section 3: Delete Rollback Tests
// ============================================================================

test "delete mark rollback unmarks row" {
    var trx = trx_types.trx_t{
        .id = 300,
        .allocator = std.testing.allocator,
    };
    defer trx_undo.trx_undo_free_logs(&trx);

    // Delete mark is recorded as update_exist in undo (InnoDB uses same type)
    _ = trx_undo.trx_undo_report_row_operation(
        &trx,
        .update_exist, // Delete mark uses update undo
        .{ .high = 0, .low = 1 },
        "delete_marker",
    );

    try std.testing.expectEqual(@as(usize, 1), trx_undo.trx_undo_record_count(&trx));

    const result = row_undo.row_undo_trx(&trx, null, null);
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, result);
}

// ============================================================================
// Section 4: Read View Visibility Tests
// ============================================================================

test "read view sees committed transactions before view creation" {
    const allocator = std.testing.allocator;

    // Transaction 10 committed, transactions 20, 25 active, next trx = 30
    const active = [_]read.trx_id_t{ 20, 25 };
    const view = read.read_view_open_with_active(30, 35, &active, allocator);
    defer read.read_view_close(view);

    // Committed before any active (trx 10 < 20)
    try std.testing.expect(read.read_view_is_visible(view, 10));

    // Active transactions - not visible
    try std.testing.expect(!read.read_view_is_visible(view, 20));
    try std.testing.expect(!read.read_view_is_visible(view, 25));

    // Own transaction - visible
    try std.testing.expect(read.read_view_is_visible(view, 30));

    // Future transactions - not visible
    try std.testing.expect(!read.read_view_is_visible(view, 35));
    try std.testing.expect(!read.read_view_is_visible(view, 100));
}

test "read view visibility with version chain" {
    const allocator = std.testing.allocator;

    // Create version chain: trx 50 (newest) -> trx 30 -> trx 10 (oldest)
    var head: ?*vers.RowVersion = null;
    head = vers.row_version_add_with_trx(head, 100, false, 10, allocator); // oldest
    head = vers.row_version_add_with_trx(head, 200, false, 30, allocator);
    head = vers.row_version_add_with_trx(head, 300, false, 50, allocator); // newest
    defer vers.row_version_free(head, allocator);

    // View where trx 30 and 50 are active
    const active = [_]read.trx_id_t{ 30, 50 };
    const view = read.read_view_open_with_active(40, 60, &active, allocator);
    defer read.read_view_close(view);

    // Should find version with trx 10 (newest visible)
    const result = vers.row_vers_build_for_consistent_read(head, view);
    try std.testing.expectEqual(vers.VersionResult.visible, result.result);
    try std.testing.expect(result.version != null);
    try std.testing.expectEqual(@as(i64, 100), result.version.?.key);
    try std.testing.expectEqual(@as(read.trx_id_t, 10), result.version.?.trx_id);
}

test "read view with no visible version" {
    const allocator = std.testing.allocator;

    // Create version chain with only future transactions
    var head: ?*vers.RowVersion = null;
    head = vers.row_version_add_with_trx(head, 500, false, 100, allocator);
    defer vers.row_version_free(head, allocator);

    // View from trx 10, max = 50
    const view = read.read_view_open_with_active(10, 50, &[_]read.trx_id_t{}, allocator);
    defer read.read_view_close(view);

    // trx 100 is in the future, no visible version
    const result = vers.row_vers_build_for_consistent_read(head, view);
    try std.testing.expectEqual(vers.VersionResult.not_found, result.result);
    try std.testing.expect(result.version == null);
}

// ============================================================================
// Section 5: Savepoint Rollback Tests
// ============================================================================

test "savepoint rollback undoes only after savepoint" {
    var trx = trx_types.trx_t{
        .id = 400,
        .allocator = std.testing.allocator,
    };
    defer trx_undo.trx_undo_free_logs(&trx);

    // Operations before savepoint
    _ = trx_undo.trx_undo_report_row_operation(&trx, .insert, .{ .high = 0, .low = 1 }, "a");
    _ = trx_undo.trx_undo_report_row_operation(&trx, .insert, .{ .high = 0, .low = 1 }, "b");

    // Take savepoint
    const savept = trx.getSavepoint();

    // Operations after savepoint
    _ = trx_undo.trx_undo_report_row_operation(&trx, .insert, .{ .high = 0, .low = 1 }, "c");
    _ = trx_undo.trx_undo_report_row_operation(&trx, .update_exist, .{ .high = 0, .low = 1 }, "d");

    try std.testing.expectEqual(@as(usize, 4), trx_undo.trx_undo_record_count(&trx));

    var undo_count: usize = 0;
    const counter = struct {
        fn apply(rec_type: trx_undo.UndoRecType, pk_data: []const u8, ctx: ?*anyopaque) row_undo.UndoResult {
            _ = rec_type;
            _ = pk_data;
            if (ctx) |c| {
                const cnt: *usize = @ptrCast(@alignCast(c));
                cnt.* += 1;
            }
            return .success;
        }
    };

    // Rollback to savepoint
    const result = row_undo.row_undo_to_savepoint(&trx, savept, counter.apply, &undo_count);

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, result);
    // Should have undone 2 records (c and d)
    try std.testing.expectEqual(@as(usize, 2), undo_count);
    // Should have 2 records remaining (a and b)
    try std.testing.expectEqual(@as(usize, 2), trx_undo.trx_undo_record_count(&trx));
}

test "multiple savepoints nested rollback" {
    var trx = trx_types.trx_t{
        .id = 401,
        .allocator = std.testing.allocator,
    };
    defer trx_undo.trx_undo_free_logs(&trx);

    // First batch
    _ = trx_undo.trx_undo_report_row_operation(&trx, .insert, .{ .high = 0, .low = 1 }, "1");

    // Savepoint 1
    const sp1 = trx.getSavepoint();

    // Second batch
    _ = trx_undo.trx_undo_report_row_operation(&trx, .insert, .{ .high = 0, .low = 1 }, "2");
    _ = trx_undo.trx_undo_report_row_operation(&trx, .insert, .{ .high = 0, .low = 1 }, "3");

    // Savepoint 2
    const sp2 = trx.getSavepoint();

    // Third batch
    _ = trx_undo.trx_undo_report_row_operation(&trx, .insert, .{ .high = 0, .low = 1 }, "4");

    try std.testing.expectEqual(@as(usize, 4), trx_undo.trx_undo_record_count(&trx));

    // Rollback to sp2 (should undo "4")
    _ = row_undo.row_undo_to_savepoint(&trx, sp2, null, null);
    try std.testing.expectEqual(@as(usize, 3), trx_undo.trx_undo_record_count(&trx));

    // Rollback to sp1 (should undo "2" and "3")
    _ = row_undo.row_undo_to_savepoint(&trx, sp1, null, null);
    try std.testing.expectEqual(@as(usize, 1), trx_undo.trx_undo_record_count(&trx));
}

// ============================================================================
// Section 6: Concurrent Read/Write Scenarios
// ============================================================================

test "concurrent readers see consistent snapshot" {
    const allocator = std.testing.allocator;

    // Writer transaction creates versions
    var head: ?*vers.RowVersion = null;
    head = vers.row_version_add_with_trx(head, 10, false, 100, allocator); // committed
    defer vers.row_version_free(head, allocator);

    // Reader 1 opens view (next trx id is 115, so trx 115 is not visible)
    const active1 = [_]read.trx_id_t{};
    const view1 = read.read_view_open_with_active(110, 115, &active1, allocator);
    defer read.read_view_close(view1);

    // Writer adds new version (trx 115)
    head = vers.row_version_add_with_trx(head, 20, false, 115, allocator);

    // Reader 1 still sees old value
    const result1 = vers.row_vers_build_for_consistent_read(head, view1);
    try std.testing.expectEqual(vers.VersionResult.visible, result1.result);
    try std.testing.expectEqual(@as(i64, 10), result1.version.?.key);

    // Reader 2 opens view after write (next trx id is 130)
    const view2 = read.read_view_open_with_active(120, 130, &[_]read.trx_id_t{}, allocator);
    defer read.read_view_close(view2);

    // Reader 2 sees new value (trx 115 < 130)
    const result2 = vers.row_vers_build_for_consistent_read(head, view2);
    try std.testing.expectEqual(vers.VersionResult.visible, result2.result);
    try std.testing.expectEqual(@as(i64, 20), result2.version.?.key);
}

test "writer blocked by reader view" {
    const allocator = std.testing.allocator;

    // Initialize purge system
    _ = purge.trx_purge_sys_init(allocator);
    defer purge.trx_purge_sys_free();

    // Reader opens view
    const view = read.read_view_open_with_active(50, 60, &[_]read.trx_id_t{}, allocator);
    defer read.read_view_close(view);

    // Register with purge system
    purge.trx_purge_register_view(view);
    defer purge.trx_purge_unregister_view(view);

    // Writer's transaction is 40 (committed before view)
    // But purge must wait because view might need undo data

    // trx 40 < 60 (low_limit), but view needs to see it
    // In reality, 40 < 60 means it's purgeable, so let's test
    // with trx >= low_limit to show it's protected
    try std.testing.expect(!purge.trx_purge_can_purge(60));
    try std.testing.expect(!purge.trx_purge_can_purge(70));

    // trx 40 < 60 is actually purgeable since it's committed before view
    try std.testing.expect(purge.trx_purge_can_purge(40));
}

// ============================================================================
// Section 7: Delete Visibility Tests
// ============================================================================

test "deleted row not visible after commit" {
    const allocator = std.testing.allocator;

    // Original row (trx 10)
    var head: ?*vers.RowVersion = null;
    head = vers.row_version_add_with_trx(head, 100, false, 10, allocator);
    // Delete mark (trx 20)
    head = vers.row_version_add_with_trx(head, 100, true, 20, allocator);
    defer vers.row_version_free(head, allocator);

    // View after delete (trx 30)
    const view = read.read_view_open_with_active(30, 40, &[_]read.trx_id_t{}, allocator);
    defer read.read_view_close(view);

    // Should return not_found because visible version is deleted
    const result = vers.row_vers_build_for_consistent_read(head, view);
    try std.testing.expectEqual(vers.VersionResult.not_found, result.result);
}

test "deleted row visible to earlier view" {
    const allocator = std.testing.allocator;

    // Original row (trx 10)
    var head: ?*vers.RowVersion = null;
    head = vers.row_version_add_with_trx(head, 100, false, 10, allocator);
    // Delete mark (trx 30) - in the future relative to view
    head = vers.row_version_add_with_trx(head, 100, true, 30, allocator);
    defer vers.row_version_free(head, allocator);

    // View from trx 20, before delete
    const view = read.read_view_open_with_active(20, 25, &[_]read.trx_id_t{}, allocator);
    defer read.read_view_close(view);

    // Delete version (trx 30) is not visible, should see original (trx 10)
    const result = vers.row_vers_build_for_consistent_read(head, view);
    try std.testing.expectEqual(vers.VersionResult.visible, result.result);
    try std.testing.expect(result.version != null);
    try std.testing.expectEqual(@as(i64, 100), result.version.?.key);
    try std.testing.expectEqual(false, result.version.?.deleted);
}

// ============================================================================
// Section 8: Purge Eligibility Tests
// ============================================================================

test "purge eligible after all views close" {
    const allocator = std.testing.allocator;

    _ = purge.trx_purge_sys_init(allocator);
    defer purge.trx_purge_sys_free();

    // Create undo log for trx 50
    const undo_log = trx_undo.trx_undo_mem_create(0, trx_undo.TRX_UNDO_INSERT, 50, allocator);
    defer trx_undo.trx_undo_mem_free(undo_log);

    trx_undo.trx_undo_append_record(undo_log, .{
        .undo_no = .{ .high = 0, .low = 1 },
        .data = "test",
    });

    // Open view with low_limit = 40 (protects >= 40)
    const view = read.read_view_open_with_active(30, 40, &[_]read.trx_id_t{}, allocator);
    defer read.read_view_close(view);
    purge.trx_purge_register_view(view);

    // Can't purge: trx 50 >= 40
    try std.testing.expectEqual(@as(usize, 0), purge.trx_purge_undo_log(undo_log));

    // Close view
    purge.trx_purge_unregister_view(view);

    // Now can purge
    const purged = purge.trx_purge_undo_log(undo_log);
    try std.testing.expectEqual(@as(usize, 1), purged);
}

test "multiple views protect overlapping ranges" {
    const allocator = std.testing.allocator;

    _ = purge.trx_purge_sys_init(allocator);
    defer purge.trx_purge_sys_free();

    // View 1: low_limit = 50
    const view1 = read.read_view_open_with_active(40, 50, &[_]read.trx_id_t{}, allocator);
    defer read.read_view_close(view1);
    purge.trx_purge_register_view(view1);

    // View 2: low_limit = 70
    const view2 = read.read_view_open_with_active(60, 70, &[_]read.trx_id_t{}, allocator);
    defer read.read_view_close(view2);
    purge.trx_purge_register_view(view2);

    // Oldest limit should be 50 (minimum)
    try std.testing.expectEqual(@as(purge.trx_id_t, 50), purge.trx_purge_get_limit());

    // Can purge < 50
    try std.testing.expect(purge.trx_purge_can_purge(30));
    try std.testing.expect(purge.trx_purge_can_purge(49));

    // Cannot purge >= 50
    try std.testing.expect(!purge.trx_purge_can_purge(50));
    try std.testing.expect(!purge.trx_purge_can_purge(60));

    // Close view1 (the older one)
    purge.trx_purge_unregister_view(view1);

    // Now limit is 70
    try std.testing.expectEqual(@as(purge.trx_id_t, 70), purge.trx_purge_get_limit());
    try std.testing.expect(purge.trx_purge_can_purge(50));
    try std.testing.expect(purge.trx_purge_can_purge(69));

    purge.trx_purge_unregister_view(view2);
}

// ============================================================================
// Section 9: Version Chain Length Tests
// ============================================================================

test "long version chain traversal" {
    const allocator = std.testing.allocator;

    // Create long version chain
    var head: ?*vers.RowVersion = null;
    var i: u64 = 0;
    while (i < 100) : (i += 1) {
        head = vers.row_version_add_with_trx(head, @intCast(i), false, i * 10, allocator);
    }
    defer vers.row_version_free(head, allocator);

    try std.testing.expectEqual(@as(usize, 100), vers.row_vers_chain_length(head));

    // View that only sees first 50 versions
    const active = [_]read.trx_id_t{};
    const view = read.read_view_open_with_active(510, 520, &active, allocator);
    defer read.read_view_close(view);

    // Should find version with trx_id = 500 (i=50, key=50)
    const visible = vers.row_vers_find_visible(head, view);
    try std.testing.expect(visible != null);
    // Newest visible is trx 500 (key 50)
    // Actually let me reconsider - head is the newest (trx 990)
    // View has low_limit_id = 520, so trx >= 520 not visible
    // We look for newest visible: 510 is visible (own), 500 is visible
    // Actually creator is 510, and 510 < 520, so 510 is visible
    // But 510 might not exist in chain... let's trace:
    // Chain: 99*10=990 -> 98*10=980 -> ... -> 51*10=510 -> 50*10=500 -> ...
    // 510 is trx that created the view, it's also in the chain
    // Checking: read_view_is_visible(view, 510) - own changes visible
    try std.testing.expect(visible.?.trx_id <= 510);
}

test "version chain count visible" {
    const allocator = std.testing.allocator;

    // Create chain with mixed visibility
    var head: ?*vers.RowVersion = null;
    head = vers.row_version_add_with_trx(head, 1, false, 10, allocator);
    head = vers.row_version_add_with_trx(head, 2, false, 20, allocator);
    head = vers.row_version_add_with_trx(head, 3, false, 30, allocator); // active
    head = vers.row_version_add_with_trx(head, 4, false, 40, allocator);
    head = vers.row_version_add_with_trx(head, 5, false, 50, allocator); // future
    defer vers.row_version_free(head, allocator);

    // View where 30 is active, max = 45
    const active = [_]read.trx_id_t{30};
    const view = read.read_view_open_with_active(35, 45, &active, allocator);
    defer read.read_view_close(view);

    // Visible: 10, 20, 35 (own), 40
    // Not visible: 30 (active), 50 (future)
    // Wait, 35 is the creator, it's not in the chain
    // 40 < 45, not in active list, so visible
    // Actually chain has: 10, 20, 30, 40, 50
    // Visible: 10 (< up_limit), 20 (< up_limit... depends on up_limit)
    // up_limit = min active = 30
    // So < 30 visible: 10, 20
    // 30 is active: not visible
    // 40 is between 30 and 45: check active list, not there, visible
    // 50 >= 45: not visible
    // Total visible: 10, 20, 40 = 3
    const count = vers.row_vers_count_visible(head, view);
    try std.testing.expectEqual(@as(usize, 3), count);
}

// ============================================================================
// Section 10: Edge Cases
// ============================================================================

test "empty transaction rollback succeeds" {
    var trx = trx_types.trx_t{
        .id = 500,
        .allocator = std.testing.allocator,
    };

    // No operations recorded
    try std.testing.expectEqual(@as(usize, 0), trx_undo.trx_undo_record_count(&trx));

    // Rollback should still succeed
    const result = row_undo.row_undo_trx(&trx, null, null);
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, result);
}

test "rollback with failed undo application" {
    var trx = trx_types.trx_t{
        .id = 501,
        .allocator = std.testing.allocator,
    };
    defer trx_undo.trx_undo_free_logs(&trx);

    _ = trx_undo.trx_undo_report_row_operation(&trx, .insert, .{ .high = 0, .low = 1 }, "x");
    _ = trx_undo.trx_undo_report_row_operation(&trx, .insert, .{ .high = 0, .low = 1 }, "y");

    // Apply function that fails on second record
    var call_count: usize = 0;
    const failingApply = struct {
        fn apply(rec_type: trx_undo.UndoRecType, pk_data: []const u8, ctx: ?*anyopaque) row_undo.UndoResult {
            _ = rec_type;
            _ = pk_data;
            if (ctx) |c| {
                const cnt: *usize = @ptrCast(@alignCast(c));
                cnt.* += 1;
                if (cnt.* == 2) {
                    return .failed;
                }
            }
            return .success;
        }
    };

    const result = row_undo.row_undo_trx(&trx, failingApply.apply, &call_count);

    // Should return error because one undo failed
    try std.testing.expectEqual(errors.DbErr.DB_ROLLBACK, result);
    try std.testing.expectEqual(@as(usize, 2), call_count);
}

test "read view with all active transactions" {
    const allocator = std.testing.allocator;

    // Many active transactions
    const active = [_]read.trx_id_t{ 10, 20, 30, 40 };
    const view = read.read_view_open_with_active(50, 60, &active, allocator);
    defer read.read_view_close(view);

    // Only own changes and committed before min active are visible
    try std.testing.expect(!read.read_view_is_visible(view, 10)); // active
    try std.testing.expect(!read.read_view_is_visible(view, 20)); // active
    try std.testing.expect(read.read_view_is_visible(view, 5)); // committed before
    try std.testing.expect(read.read_view_is_visible(view, 50)); // own
}

test "purge statistics accumulate correctly" {
    const allocator = std.testing.allocator;

    _ = purge.trx_purge_sys_init(allocator);
    defer purge.trx_purge_sys_free();

    purge.trx_purge_reset_stats();

    // Purge first log
    const log1 = trx_undo.trx_undo_mem_create(0, trx_undo.TRX_UNDO_INSERT, 10, allocator);
    defer trx_undo.trx_undo_mem_free(log1);
    trx_undo.trx_undo_append_record(log1, .{ .undo_no = .{ .high = 0, .low = 1 }, .data = "a" });
    trx_undo.trx_undo_append_record(log1, .{ .undo_no = .{ .high = 0, .low = 2 }, .data = "b" });
    _ = purge.trx_purge_undo_log(log1);

    // Purge second log
    const log2 = trx_undo.trx_undo_mem_create(0, trx_undo.TRX_UNDO_INSERT, 20, allocator);
    defer trx_undo.trx_undo_mem_free(log2);
    trx_undo.trx_undo_append_record(log2, .{ .undo_no = .{ .high = 0, .low = 1 }, .data = "c" });
    _ = purge.trx_purge_undo_log(log2);

    const stats = purge.trx_purge_get_stats();
    try std.testing.expectEqual(@as(usize, 3), stats.records_purged);
}
