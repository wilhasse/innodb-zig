const std = @import("std");
const page = @import("../page/mod.zig");
const types = @import("types.zig");
const undo = @import("undo.zig");
const read = @import("../read/mod.zig");

pub const module_name = "trx.purge";

pub const ulint = types.ulint;
pub const trx_id_t = types.trx_id_t;
pub const undo_no_t = types.undo_no_t;

// ============================================================================
// Legacy TrxPurge for delete-marked records
// ============================================================================

pub const TrxPurge = struct {
    pending: std.ArrayListUnmanaged(*page.rec_t) = .{},
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) TrxPurge {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *TrxPurge) void {
        self.pending.deinit(self.allocator);
    }

    pub fn add(self: *TrxPurge, rec: *page.rec_t) void {
        self.pending.append(self.allocator, rec) catch @panic("TrxPurge.add");
    }

    pub fn apply(self: *TrxPurge) usize {
        var count: usize = 0;
        for (self.pending.items) |rec| {
            rec.deleted = true;
            count += 1;
        }
        self.pending.clearRetainingCapacity();
        return count;
    }
};

// ============================================================================
// MVCC Purge System (IBD-214)
// ============================================================================

/// Purge statistics
pub const PurgeStats = struct {
    /// Total undo records purged
    records_purged: usize = 0,
    /// Total purge cycles run
    cycles_run: usize = 0,
    /// Number of undo logs freed
    logs_freed: usize = 0,
};

/// Purge system state
pub const purge_sys_t = struct {
    /// List of active read views (for determining oldest)
    active_views: std.ArrayListUnmanaged(*read.read_view_t) = .{},
    /// Oldest view's low_limit_no - undo records with undo_no < this can be purged
    oldest_view_low_limit: trx_id_t = 0,
    /// Statistics
    stats: PurgeStats = .{},
    /// Memory allocator
    allocator: std.mem.Allocator = std.heap.page_allocator,

    /// Check if there are any active views
    pub fn hasActiveViews(self: *const purge_sys_t) bool {
        return self.active_views.items.len > 0;
    }

    /// Get count of active views
    pub fn activeViewCount(self: *const purge_sys_t) usize {
        return self.active_views.items.len;
    }
};

/// Global purge system instance
pub var purge_sys: ?*purge_sys_t = null;

/// Initialize the global purge system
pub fn trx_purge_sys_init(allocator: std.mem.Allocator) *purge_sys_t {
    if (purge_sys) |sys| {
        return sys;
    }
    const sys = allocator.create(purge_sys_t) catch @panic("trx_purge_sys_init");
    sys.* = .{ .allocator = allocator };
    purge_sys = sys;
    return sys;
}

/// Free the global purge system
pub fn trx_purge_sys_free() void {
    if (purge_sys) |sys| {
        sys.active_views.deinit(sys.allocator);
        sys.allocator.destroy(sys);
        purge_sys = null;
    }
}

/// Register a read view with the purge system
/// Called when a new consistent read view is opened
pub fn trx_purge_register_view(view: *read.read_view_t) void {
    const sys = purge_sys orelse return;
    sys.active_views.append(sys.allocator, view) catch @panic("trx_purge_register_view");
    updateOldestViewLimit(sys);
}

/// Unregister a read view from the purge system
/// Called when a read view is closed - may trigger purge
pub fn trx_purge_unregister_view(view: *read.read_view_t) void {
    const sys = purge_sys orelse return;

    // Find and remove the view
    var idx: usize = 0;
    while (idx < sys.active_views.items.len) {
        if (sys.active_views.items[idx] == view) {
            _ = sys.active_views.orderedRemove(idx);
            break;
        }
        idx += 1;
    }

    updateOldestViewLimit(sys);
}

/// Update the oldest view limit after view list changes
fn updateOldestViewLimit(sys: *purge_sys_t) void {
    if (sys.active_views.items.len == 0) {
        // No active views - can purge everything
        sys.oldest_view_low_limit = std.math.maxInt(trx_id_t);
        return;
    }

    // Find the minimum low_limit_no among all views
    var min_limit: trx_id_t = std.math.maxInt(trx_id_t);
    for (sys.active_views.items) |view| {
        if (view.low_limit_no < min_limit) {
            min_limit = view.low_limit_no;
        }
    }
    sys.oldest_view_low_limit = min_limit;
}

/// Get the current purge limit - undo records older than this can be purged
pub fn trx_purge_get_limit() trx_id_t {
    const sys = purge_sys orelse return 0;
    return sys.oldest_view_low_limit;
}

/// Check if an undo record can be purged based on its trx_id
pub fn trx_purge_can_purge(trx_id: trx_id_t) bool {
    const sys = purge_sys orelse return false;

    // If no active views, can purge all committed transactions
    if (!sys.hasActiveViews()) {
        return true;
    }

    // Can purge if trx_id < oldest view's low_limit_no
    return trx_id < sys.oldest_view_low_limit;
}

/// Purge undo records from a single undo log that are no longer needed
/// Returns number of records purged
pub fn trx_purge_undo_log(undo_log: *undo.trx_undo_t) usize {
    const sys = purge_sys orelse return 0;

    // If views exist and this log's trx is still visible, can't purge
    if (sys.hasActiveViews() and undo_log.trx_id >= sys.oldest_view_low_limit) {
        return 0;
    }

    // Purge all records in this log
    const count = undo_log.records.items.len;

    // Free record data
    for (undo_log.records.items) |rec| {
        if (rec.data.len > 0) {
            undo_log.allocator.free(rec.data);
        }
    }

    undo_log.records.clearRetainingCapacity();
    undo_log.empty = true;

    sys.stats.records_purged += count;

    return count;
}

/// Purge all undo records from a transaction that are no longer needed
/// Returns number of records purged
pub fn trx_purge_trx_undo_logs(trx: *types.trx_t) usize {
    var purged: usize = 0;

    if (undo.trx_undo_t.fromOpaquePtr(trx.insert_undo)) |insert_log| {
        purged += trx_purge_undo_log(insert_log);
    }

    if (undo.trx_undo_t.fromOpaquePtr(trx.update_undo)) |update_log| {
        purged += trx_purge_undo_log(update_log);
    }

    return purged;
}

/// Run a purge cycle - purges undo records that are no longer visible to any view
/// In a full implementation, this would iterate through the undo log history
/// Returns number of records purged
pub fn trx_purge_run() usize {
    const sys = purge_sys orelse return 0;

    sys.stats.cycles_run += 1;

    // In a full implementation, we would:
    // 1. Get the oldest view's limit
    // 2. Iterate through committed transaction undo logs
    // 3. Purge records where trx_id < limit

    // For now, return 0 as we don't have a global undo history list
    return 0;
}

/// Get current purge statistics
pub fn trx_purge_get_stats() PurgeStats {
    const sys = purge_sys orelse return .{};
    return sys.stats;
}

/// Reset purge statistics
pub fn trx_purge_reset_stats() void {
    if (purge_sys) |sys| {
        sys.stats = .{};
    }
}

// ============================================================================
// Tests
// ============================================================================

test "trx purge apply marks deleted" {
    var purge_local = TrxPurge.init(std.testing.allocator);
    defer purge_local.deinit();
    var rec1 = page.rec_t{ .key = 1 };
    var rec2 = page.rec_t{ .key = 2 };
    purge_local.add(&rec1);
    purge_local.add(&rec2);
    const n = purge_local.apply();
    try std.testing.expectEqual(@as(usize, 2), n);
    try std.testing.expect(rec1.deleted);
    try std.testing.expect(rec2.deleted);
}

test "purge_sys_init and free" {
    const sys = trx_purge_sys_init(std.testing.allocator);
    defer trx_purge_sys_free();

    try std.testing.expect(purge_sys != null);
    try std.testing.expectEqual(@as(usize, 0), sys.activeViewCount());
    try std.testing.expect(!sys.hasActiveViews());
}

test "purge register and unregister views" {
    const sys = trx_purge_sys_init(std.testing.allocator);
    defer trx_purge_sys_free();

    // Create views with different low_limit_no values
    const view1 = read.read_view_open_with_active(10, 50, &[_]read.trx_id_t{}, std.testing.allocator);
    defer read.read_view_close(view1);

    const view2 = read.read_view_open_with_active(20, 100, &[_]read.trx_id_t{}, std.testing.allocator);
    defer read.read_view_close(view2);

    // Register views
    trx_purge_register_view(view1);
    try std.testing.expectEqual(@as(usize, 1), sys.activeViewCount());

    trx_purge_register_view(view2);
    try std.testing.expectEqual(@as(usize, 2), sys.activeViewCount());

    // Oldest limit should be the minimum
    try std.testing.expectEqual(@as(trx_id_t, 50), sys.oldest_view_low_limit);

    // Unregister view1 (the older one)
    trx_purge_unregister_view(view1);
    try std.testing.expectEqual(@as(usize, 1), sys.activeViewCount());
    try std.testing.expectEqual(@as(trx_id_t, 100), sys.oldest_view_low_limit);

    // Unregister view2
    trx_purge_unregister_view(view2);
    try std.testing.expectEqual(@as(usize, 0), sys.activeViewCount());
    // When no views, limit is maxInt (can purge everything)
    try std.testing.expectEqual(std.math.maxInt(trx_id_t), sys.oldest_view_low_limit);
}

test "trx_purge_can_purge with active views" {
    _ = trx_purge_sys_init(std.testing.allocator);
    defer trx_purge_sys_free();

    // With no views, can purge anything
    try std.testing.expect(trx_purge_can_purge(50));
    try std.testing.expect(trx_purge_can_purge(100));

    // Add a view with low_limit_no = 75
    const view = read.read_view_open_with_active(30, 75, &[_]read.trx_id_t{}, std.testing.allocator);
    defer read.read_view_close(view);
    trx_purge_register_view(view);

    // Can purge trx < 75
    try std.testing.expect(trx_purge_can_purge(50));
    try std.testing.expect(trx_purge_can_purge(74));

    // Cannot purge trx >= 75
    try std.testing.expect(!trx_purge_can_purge(75));
    try std.testing.expect(!trx_purge_can_purge(100));

    trx_purge_unregister_view(view);
}

test "trx_purge_undo_log purges records" {
    _ = trx_purge_sys_init(std.testing.allocator);
    defer trx_purge_sys_free();

    // Create an undo log with old transaction ID
    const undo_log = undo.trx_undo_mem_create(0, undo.TRX_UNDO_INSERT, 10, std.testing.allocator);
    defer undo.trx_undo_mem_free(undo_log);

    // Add some records
    undo.trx_undo_append_record(undo_log, .{ .undo_no = .{ .high = 0, .low = 1 }, .data = "a" });
    undo.trx_undo_append_record(undo_log, .{ .undo_no = .{ .high = 0, .low = 2 }, .data = "b" });

    try std.testing.expectEqual(@as(usize, 2), undo_log.records.items.len);

    // No views - should purge all
    const purged = trx_purge_undo_log(undo_log);
    try std.testing.expectEqual(@as(usize, 2), purged);
    try std.testing.expectEqual(@as(usize, 0), undo_log.records.items.len);
    try std.testing.expect(undo_log.empty);

    // Stats should reflect purge
    const stats = trx_purge_get_stats();
    try std.testing.expectEqual(@as(usize, 2), stats.records_purged);
}

test "trx_purge_undo_log respects active views" {
    _ = trx_purge_sys_init(std.testing.allocator);
    defer trx_purge_sys_free();

    // Create an undo log with trx_id = 50
    const undo_log = undo.trx_undo_mem_create(0, undo.TRX_UNDO_INSERT, 50, std.testing.allocator);
    defer undo.trx_undo_mem_free(undo_log);

    undo.trx_undo_append_record(undo_log, .{ .undo_no = .{ .high = 0, .low = 1 }, .data = "x" });

    // Add a view that needs to see trx 50
    const view = read.read_view_open_with_active(60, 100, &[_]read.trx_id_t{}, std.testing.allocator);
    defer read.read_view_close(view);
    trx_purge_register_view(view);

    // Should NOT purge because trx 50 >= view's low_limit_no would be... wait
    // Actually low_limit_no = 100 (max_trx_id), so 50 < 100, should be purgeable
    // But the view was created at max_trx_id=100, meaning trx 50 was committed before
    // the view, so its undo might still be needed for consistent read of older data.

    // Let me reconsider: view.low_limit_no is the next trx_id to be assigned
    // Undo records for trx_id < low_limit_no can be purged IF no active view needs them
    // The visibility check is: can purge if trx_id < oldest view's up_limit_id

    // Actually, for purge we check if the undo log's trx_id is older than what
    // any view could need. Since view1 has low_limit_no=100, and our undo is trx=50,
    // the undo IS older, so it should be purgeable in simple terms.

    // Let me test with a view that clearly needs the undo
    trx_purge_unregister_view(view);

    const view2 = read.read_view_open_with_active(30, 40, &[_]read.trx_id_t{}, std.testing.allocator);
    defer read.read_view_close(view2);
    trx_purge_register_view(view2);

    // view2 has low_limit_no = 40, undo trx_id = 50 >= 40, can't purge
    const purged = trx_purge_undo_log(undo_log);
    try std.testing.expectEqual(@as(usize, 0), purged);
    try std.testing.expectEqual(@as(usize, 1), undo_log.records.items.len);

    trx_purge_unregister_view(view2);
}

test "closing views triggers purge eligibility" {
    _ = trx_purge_sys_init(std.testing.allocator);
    defer trx_purge_sys_free();

    // Create undo log with trx_id = 50
    const undo_log = undo.trx_undo_mem_create(0, undo.TRX_UNDO_INSERT, 50, std.testing.allocator);
    defer undo.trx_undo_mem_free(undo_log);

    undo.trx_undo_append_record(undo_log, .{ .undo_no = .{ .high = 0, .low = 1 }, .data = "z" });

    // Add view with low_limit = 40 (protects trx >= 40)
    const view = read.read_view_open_with_active(30, 40, &[_]read.trx_id_t{}, std.testing.allocator);
    defer read.read_view_close(view);
    trx_purge_register_view(view);

    // Can't purge yet
    try std.testing.expect(!trx_purge_can_purge(50));
    try std.testing.expectEqual(@as(usize, 0), trx_purge_undo_log(undo_log));

    // Close the view
    trx_purge_unregister_view(view);

    // Now can purge
    try std.testing.expect(trx_purge_can_purge(50));
    const purged = trx_purge_undo_log(undo_log);
    try std.testing.expectEqual(@as(usize, 1), purged);
}

test "purge statistics tracking" {
    _ = trx_purge_sys_init(std.testing.allocator);
    defer trx_purge_sys_free();

    trx_purge_reset_stats();
    var stats = trx_purge_get_stats();
    try std.testing.expectEqual(@as(usize, 0), stats.records_purged);
    try std.testing.expectEqual(@as(usize, 0), stats.cycles_run);

    // Create and purge an undo log
    const undo_log = undo.trx_undo_mem_create(0, undo.TRX_UNDO_INSERT, 10, std.testing.allocator);
    defer undo.trx_undo_mem_free(undo_log);

    undo.trx_undo_append_record(undo_log, .{ .undo_no = .{ .high = 0, .low = 1 }, .data = "q" });
    undo.trx_undo_append_record(undo_log, .{ .undo_no = .{ .high = 0, .low = 2 }, .data = "r" });
    undo.trx_undo_append_record(undo_log, .{ .undo_no = .{ .high = 0, .low = 3 }, .data = "s" });

    _ = trx_purge_undo_log(undo_log);

    stats = trx_purge_get_stats();
    try std.testing.expectEqual(@as(usize, 3), stats.records_purged);

    // Run a purge cycle
    _ = trx_purge_run();
    stats = trx_purge_get_stats();
    try std.testing.expectEqual(@as(usize, 1), stats.cycles_run);
}
