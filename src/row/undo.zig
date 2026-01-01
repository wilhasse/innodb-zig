const std = @import("std");
const page = @import("../page/mod.zig");
const trx_types = @import("../trx/types.zig");
const trx_undo = @import("../trx/undo.zig");
const errors = @import("../ut/errors.zig");

pub const UndoOp = enum { insert, delete, modify };

pub const UndoEntry = struct {
    op: UndoOp,
    rec: *page.rec_t,
    old_key: i64 = 0,
};

pub const UndoLog = struct {
    entries: std.ArrayListUnmanaged(UndoEntry),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) UndoLog {
        return .{
            .entries = .{},
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *UndoLog) void {
        self.entries.deinit(self.allocator);
    }

    pub fn addInsert(self: *UndoLog, rec: *page.rec_t) void {
        self.entries.append(self.allocator, .{ .op = .insert, .rec = rec }) catch @panic("addInsert");
    }

    pub fn addDelete(self: *UndoLog, rec: *page.rec_t) void {
        self.entries.append(self.allocator, .{ .op = .delete, .rec = rec }) catch @panic("addDelete");
    }

    pub fn addModify(self: *UndoLog, rec: *page.rec_t, old_key: i64) void {
        self.entries.append(self.allocator, .{ .op = .modify, .rec = rec, .old_key = old_key }) catch @panic("addModify");
    }

    pub fn apply(self: *UndoLog) void {
        var i = self.entries.items.len;
        while (i > 0) {
            i -= 1;
            const entry = self.entries.items[i];
            switch (entry.op) {
                .insert => entry.rec.deleted = true,
                .delete => entry.rec.deleted = false,
                .modify => entry.rec.key = entry.old_key,
            }
        }
    }
};

// ============================================================================
// Transaction Undo Application (IBD-210)
// ============================================================================

/// Result of applying an undo record
pub const UndoResult = enum {
    success,
    row_not_found,
    already_undone,
    failed,
};

/// Callback function type for applying undo to actual storage
/// Returns true if undo was successfully applied
pub const UndoApplyFn = *const fn (
    rec_type: trx_undo.UndoRecType,
    pk_data: []const u8,
    ctx: ?*anyopaque,
) UndoResult;

/// Apply a single undo record for rollback
/// For insert: mark row as deleted (physically remove)
/// For update: restore old values from undo data
/// For delete mark: unmark the deletion
pub fn row_undo_rec(
    undo_rec: *const trx_types.trx_undo_record_t,
    apply_fn: ?UndoApplyFn,
    ctx: ?*anyopaque,
) UndoResult {
    // Determine record type from is_insert flag
    const rec_type: trx_undo.UndoRecType = if (undo_rec.is_insert) .insert else .update_exist;

    if (apply_fn) |f| {
        return f(rec_type, undo_rec.data, ctx);
    }

    // Default: just signal success (for testing without actual storage)
    return .success;
}

/// Apply all undo records from a transaction's insert undo log
/// Records are applied in reverse order (LIFO)
pub fn row_undo_insert_log(
    undo: *trx_undo.trx_undo_t,
    apply_fn: ?UndoApplyFn,
    ctx: ?*anyopaque,
) struct { applied: usize, failed: usize } {
    var applied: usize = 0;
    var failed: usize = 0;

    // Process records in reverse order
    var i = undo.records.items.len;
    while (i > 0) {
        i -= 1;
        const rec = &undo.records.items[i];
        const result = row_undo_rec(rec, apply_fn, ctx);
        switch (result) {
            .success, .already_undone => applied += 1,
            .row_not_found, .failed => failed += 1,
        }
    }

    return .{ .applied = applied, .failed = failed };
}

/// Apply all undo records from a transaction's update undo log
/// Records are applied in reverse order (LIFO)
pub fn row_undo_update_log(
    undo: *trx_undo.trx_undo_t,
    apply_fn: ?UndoApplyFn,
    ctx: ?*anyopaque,
) struct { applied: usize, failed: usize } {
    var applied: usize = 0;
    var failed: usize = 0;

    // Process records in reverse order
    var i = undo.records.items.len;
    while (i > 0) {
        i -= 1;
        const rec = &undo.records.items[i];
        const result = row_undo_rec(rec, apply_fn, ctx);
        switch (result) {
            .success, .already_undone => applied += 1,
            .row_not_found, .failed => failed += 1,
        }
    }

    return .{ .applied = applied, .failed = failed };
}

/// Rollback a transaction by applying all undo records
/// Processes update undo first (to restore modified rows),
/// then insert undo (to remove inserted rows)
pub fn row_undo_trx(
    trx: *trx_types.trx_t,
    apply_fn: ?UndoApplyFn,
    ctx: ?*anyopaque,
) errors.DbErr {
    var total_applied: usize = 0;
    var total_failed: usize = 0;

    // First: undo updates/deletes (restore old versions)
    if (trx_undo.trx_undo_t.fromOpaquePtr(trx.update_undo)) |undo| {
        const result = row_undo_update_log(undo, apply_fn, ctx);
        total_applied += result.applied;
        total_failed += result.failed;
    }

    // Second: undo inserts (remove inserted rows)
    if (trx_undo.trx_undo_t.fromOpaquePtr(trx.insert_undo)) |undo| {
        const result = row_undo_insert_log(undo, apply_fn, ctx);
        total_applied += result.applied;
        total_failed += result.failed;
    }

    // Also process undo_stack if present (legacy support)
    var i = trx.undo_stack.items.len;
    while (i > 0) {
        i -= 1;
        const rec = &trx.undo_stack.items[i];
        const result = row_undo_rec(rec, apply_fn, ctx);
        switch (result) {
            .success, .already_undone => total_applied += 1,
            .row_not_found, .failed => total_failed += 1,
        }
    }

    if (total_failed > 0) {
        return .DB_ROLLBACK;
    }

    return .DB_SUCCESS;
}

/// Rollback to a savepoint by applying undo records down to the savepoint's undo_no
pub fn row_undo_to_savepoint(
    trx: *trx_types.trx_t,
    savept: trx_types.trx_savept_t,
    apply_fn: ?UndoApplyFn,
    ctx: ?*anyopaque,
) errors.DbErr {
    const limit = savept.least_undo_no;

    // Undo update log records with undo_no >= limit
    if (trx_undo.trx_undo_t.fromOpaquePtr(trx.update_undo)) |undo| {
        while (undo.records.items.len > 0) {
            const idx = undo.records.items.len - 1;
            const rec = &undo.records.items[idx];

            // Check if we've reached the savepoint limit
            if (rec.undo_no.high < limit.high or
                (rec.undo_no.high == limit.high and rec.undo_no.low < limit.low))
            {
                break;
            }

            // Apply undo and remove record
            _ = row_undo_rec(rec, apply_fn, ctx);
            const popped = trx_undo.trx_undo_pop_record(undo);
            if (popped) |p| {
                if (p.data.len > 0) {
                    undo.allocator.free(p.data);
                }
            }
        }
    }

    // Undo insert log records with undo_no >= limit
    if (trx_undo.trx_undo_t.fromOpaquePtr(trx.insert_undo)) |undo| {
        while (undo.records.items.len > 0) {
            const idx = undo.records.items.len - 1;
            const rec = &undo.records.items[idx];

            if (rec.undo_no.high < limit.high or
                (rec.undo_no.high == limit.high and rec.undo_no.low < limit.low))
            {
                break;
            }

            _ = row_undo_rec(rec, apply_fn, ctx);
            const popped = trx_undo.trx_undo_pop_record(undo);
            if (popped) |p| {
                if (p.data.len > 0) {
                    undo.allocator.free(p.data);
                }
            }
        }
    }

    // Update transaction's undo_no to savepoint level
    trx.undo_no = limit;

    return .DB_SUCCESS;
}

test "undo log apply restores state" {
    var rec = page.rec_t{ .key = 10, .deleted = true };
    var log = UndoLog.init(std.testing.allocator);
    defer log.deinit();
    log.addModify(&rec, 3);
    log.addDelete(&rec);
    log.apply();
    try std.testing.expectEqual(@as(i64, 3), rec.key);
    try std.testing.expectEqual(false, rec.deleted);
}

test "undo log insert marks deleted" {
    var rec = page.rec_t{ .key = 1 };
    var log = UndoLog.init(std.testing.allocator);
    defer log.deinit();
    log.addInsert(&rec);
    log.apply();
    try std.testing.expect(rec.deleted);
}

// ============================================================================
// IBD-210 Tests
// ============================================================================

test "row_undo_rec with no callback returns success" {
    const rec = trx_types.trx_undo_record_t{
        .undo_no = .{ .high = 0, .low = 1 },
        .is_insert = true,
        .data = "pk",
    };
    const result = row_undo_rec(&rec, null, null);
    try std.testing.expectEqual(UndoResult.success, result);
}

fn testApplyFn(rec_type: trx_undo.UndoRecType, pk_data: []const u8, ctx: ?*anyopaque) UndoResult {
    if (ctx) |c| {
        const counter: *usize = @ptrCast(@alignCast(c));
        counter.* += 1;
    }
    _ = rec_type;
    _ = pk_data;
    return .success;
}

test "row_undo_insert_log applies records in reverse" {
    const undo = trx_undo.trx_undo_mem_create(0, trx_undo.TRX_UNDO_INSERT, 100, std.testing.allocator);
    defer trx_undo.trx_undo_mem_free(undo);

    trx_undo.trx_undo_append_record(undo, .{ .undo_no = .{ .high = 0, .low = 1 }, .is_insert = true, .data = "a" });
    trx_undo.trx_undo_append_record(undo, .{ .undo_no = .{ .high = 0, .low = 2 }, .is_insert = true, .data = "b" });

    var counter: usize = 0;
    const result = row_undo_insert_log(undo, testApplyFn, &counter);

    try std.testing.expectEqual(@as(usize, 2), result.applied);
    try std.testing.expectEqual(@as(usize, 0), result.failed);
    try std.testing.expectEqual(@as(usize, 2), counter);
}

test "row_undo_trx applies both insert and update logs" {
    var trx = trx_types.trx_t{
        .id = 200,
        .allocator = std.testing.allocator,
    };
    defer trx_undo.trx_undo_free_logs(&trx);

    // Add some undo records
    _ = trx_undo.trx_undo_report_row_operation(&trx, .insert, .{ .high = 0, .low = 1 }, "ins1");
    _ = trx_undo.trx_undo_report_row_operation(&trx, .update_exist, .{ .high = 0, .low = 1 }, "upd1");
    _ = trx_undo.trx_undo_report_row_operation(&trx, .insert, .{ .high = 0, .low = 1 }, "ins2");

    var counter: usize = 0;
    const result = row_undo_trx(&trx, testApplyFn, &counter);

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, result);
    try std.testing.expectEqual(@as(usize, 3), counter);
}

test "row_undo_to_savepoint only undos after savepoint" {
    var trx = trx_types.trx_t{
        .id = 300,
        .allocator = std.testing.allocator,
    };
    defer trx_undo.trx_undo_free_logs(&trx);

    // Record some operations
    _ = trx_undo.trx_undo_report_row_operation(&trx, .insert, .{ .high = 0, .low = 1 }, "r1"); // undo_no = 0
    _ = trx_undo.trx_undo_report_row_operation(&trx, .insert, .{ .high = 0, .low = 1 }, "r2"); // undo_no = 1

    // Take savepoint here (undo_no = 2)
    const savept = trx.getSavepoint();

    _ = trx_undo.trx_undo_report_row_operation(&trx, .insert, .{ .high = 0, .low = 1 }, "r3"); // undo_no = 2
    _ = trx_undo.trx_undo_report_row_operation(&trx, .update_exist, .{ .high = 0, .low = 1 }, "r4"); // undo_no = 3

    // Before rollback: 3 insert records, 1 update record
    try std.testing.expectEqual(@as(usize, 4), trx_undo.trx_undo_record_count(&trx));

    var counter: usize = 0;
    const result = row_undo_to_savepoint(&trx, savept, testApplyFn, &counter);

    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, result);
    // Should have undone 2 records (undo_no 2 and 3)
    try std.testing.expectEqual(@as(usize, 2), counter);
    // Should have 2 records left (undo_no 0 and 1)
    try std.testing.expectEqual(@as(usize, 2), trx_undo.trx_undo_record_count(&trx));
}

test "row_undo_trx with empty transaction succeeds" {
    var trx = trx_types.trx_t{
        .id = 400,
        .allocator = std.testing.allocator,
    };

    const result = row_undo_trx(&trx, null, null);
    try std.testing.expectEqual(errors.DbErr.DB_SUCCESS, result);
}
