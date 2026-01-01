const std = @import("std");
const compat = @import("../ut/compat.zig");
const errors = @import("../ut/errors.zig");
const que = @import("../que/mod.zig");

pub const ulint = compat.ulint;
pub const dulint = compat.Dulint;

pub const TRX_ISO_READ_UNCOMMITTED: ulint = 0;
pub const TRX_ISO_READ_COMMITTED: ulint = 1;
pub const TRX_ISO_REPEATABLE_READ: ulint = 2;
pub const TRX_ISO_SERIALIZABLE: ulint = 3;

pub const trx_id_t = compat.ib_uint64_t;
pub const undo_no_t = dulint;
pub const roll_ptr_t = dulint;

pub const TRX_SIG_NO_SIGNAL: ulint = 0;
pub const TRX_SIG_TOTAL_ROLLBACK: ulint = 1;
pub const TRX_SIG_ROLLBACK_TO_SAVEPT: ulint = 2;
pub const TRX_SIG_COMMIT: ulint = 3;
pub const TRX_SIG_ERROR_OCCURRED: ulint = 4;
pub const TRX_SIG_BREAK_EXECUTION: ulint = 5;

pub const TRX_SIG_SELF: ulint = 0;
pub const TRX_SIG_OTHER_SESS: ulint = 1;

pub const TrxQueState = enum(u8) {
    running = 1,
    rolling_back = 2,
    errored = 3,
};

pub const TrxConcState = enum(u8) {
    not_started = 0,
    active = 1,
    committed_in_memory = 2,
    prepared = 3,
};

pub fn dulintZero() dulint {
    return .{ .high = 0, .low = 0 };
}

pub const trx_savept_t = struct {
    least_undo_no: undo_no_t = dulintZero(),
};

pub const trx_named_savept_t = struct {
    name: []const u8 = "",
    savept: trx_savept_t = .{},
};

pub const trx_sig_t = struct {
    type: ulint = TRX_SIG_NO_SIGNAL,
    sender: ulint = TRX_SIG_SELF,
    savept: ?trx_savept_t = null,
    replied: bool = false,
};

pub const trx_undo_inf_t = struct {
    undo_no: undo_no_t = dulintZero(),
    in_use: bool = false,
};

pub const trx_undo_arr_t = struct {
    infos: []trx_undo_inf_t = &[_]trx_undo_inf_t{},
    n_cells: ulint = 0,
    n_used: ulint = 0,
    allocator: std.mem.Allocator = std.heap.page_allocator,
};

pub const trx_undo_record_t = struct {
    undo_no: undo_no_t = dulintZero(),
    roll_ptr: roll_ptr_t = dulintZero(),
    is_insert: bool = false,
    data: []const u8 = &[_]u8{},
};

// Forward declaration for trx_undo_t (defined in undo.zig)
pub const trx_undo_t = opaque {};

pub const trx_t = struct {
    id: trx_id_t = 0,
    sess: ?*que.sess_t = null,
    conc_state: TrxConcState = .not_started,
    is_recovered: bool = false,
    isolation_level: ulint = 0,
    start_time: i64 = 0,
    undo_no: undo_no_t = dulintZero(),
    roll_limit: undo_no_t = dulintZero(),
    last_sql_stat_start: trx_savept_t = .{},
    undo_no_arr: ?*trx_undo_arr_t = null,
    pages_undone: ulint = 0,
    graph: ?*que.que_t = null,
    que_state: TrxQueState = .running,
    signals: std.ArrayListUnmanaged(trx_sig_t) = .{},
    savepoints: std.ArrayListUnmanaged(trx_named_savept_t) = .{},
    undo_stack: std.ArrayListUnmanaged(trx_undo_record_t) = .{},
    allocator: std.mem.Allocator = std.heap.page_allocator,
    error_state: errors.DbErr = .DB_SUCCESS,
    error_key_num: ulint = 0,
    detailed_error: [128]u8 = [_]u8{0} ** 128,

    // Undo log pointers - separate logs for inserts vs updates/deletes
    insert_undo: ?*anyopaque = null, // trx_undo_t for insert operations
    update_undo: ?*anyopaque = null, // trx_undo_t for update/delete operations

    /// Get the next undo number and increment
    pub fn nextUndoNo(self: *trx_t) undo_no_t {
        const result = self.undo_no;
        // Increment undo_no (handle overflow from low to high)
        if (self.undo_no.low == 0xFFFFFFFF) {
            self.undo_no.high += 1;
            self.undo_no.low = 0;
        } else {
            self.undo_no.low += 1;
        }
        return result;
    }

    /// Create a roll pointer from page/offset (simplified)
    pub fn makeRollPtr(self: *const trx_t, is_insert: bool, page_no: ulint, offset: ulint) roll_ptr_t {
        _ = self;
        // Roll pointer format: [is_insert:1][rseg_id:7][page_no:24][offset:16]
        // Simplified version using dulint
        const high: ulint = if (is_insert) 0x80000000 else 0;
        const low: ulint = (page_no << 16) | (offset & 0xFFFF);
        return .{ .high = high, .low = low };
    }

    /// Check if transaction has any undo logs
    pub fn hasUndoLogs(self: *const trx_t) bool {
        return self.insert_undo != null or self.update_undo != null or
            self.undo_stack.items.len > 0;
    }

    /// Get current savepoint
    pub fn getSavepoint(self: *const trx_t) trx_savept_t {
        return .{ .least_undo_no = self.undo_no };
    }
};
