const std = @import("std");

pub const DbErr = enum(i32) {
    DB_SUCCESS = 10,
    DB_ERROR,
    DB_INTERRUPTED,
    DB_OUT_OF_MEMORY,
    DB_OUT_OF_FILE_SPACE,
    DB_LOCK_WAIT,
    DB_DEADLOCK,
    DB_ROLLBACK,
    DB_DUPLICATE_KEY,
    DB_QUE_THR_SUSPENDED,
    DB_MISSING_HISTORY,
    DB_CLUSTER_NOT_FOUND = 30,
    DB_TABLE_NOT_FOUND,
    DB_MUST_GET_MORE_FILE_SPACE,
    DB_TABLE_IS_BEING_USED,
    DB_TOO_BIG_RECORD,
    DB_LOCK_WAIT_TIMEOUT,
    DB_NO_REFERENCED_ROW,
    DB_ROW_IS_REFERENCED,
    DB_CANNOT_ADD_CONSTRAINT,
    DB_CORRUPTION,
    DB_COL_APPEARS_TWICE_IN_INDEX,
    DB_CANNOT_DROP_CONSTRAINT,
    DB_NO_SAVEPOINT,
    DB_TABLESPACE_ALREADY_EXISTS,
    DB_TABLESPACE_DELETED,
    DB_LOCK_TABLE_FULL,
    DB_FOREIGN_DUPLICATE_KEY,
    DB_TOO_MANY_CONCURRENT_TRXS,
    DB_UNSUPPORTED,
    DB_PRIMARY_KEY_IS_NULL,
    DB_FATAL,
    DB_FAIL = 1000,
    DB_OVERFLOW,
    DB_UNDERFLOW,
    DB_STRONG_FAIL,
    DB_ZIP_OVERFLOW,
    DB_RECORD_NOT_FOUND = 1500,
    DB_END_OF_INDEX,
    DB_SCHEMA_ERROR = 2000,
    DB_DATA_MISMATCH,
    DB_SCHEMA_NOT_LOCKED,
    DB_NOT_FOUND,
    DB_READONLY,
    DB_INVALID_INPUT,
};

pub const ib_err_t = DbErr;

pub fn strerror(err: DbErr) []const u8 {
    return strerrorCode(@intFromEnum(err));
}

pub fn strerrorCode(code: i32) []const u8 {
    return switch (code) {
        @intFromEnum(DbErr.DB_SUCCESS) => "Success",
        @intFromEnum(DbErr.DB_ERROR) => "Generic error",
        @intFromEnum(DbErr.DB_OUT_OF_MEMORY) => "Cannot allocate memory",
        @intFromEnum(DbErr.DB_OUT_OF_FILE_SPACE) => "Out of disk space",
        @intFromEnum(DbErr.DB_LOCK_WAIT) => "Lock wait",
        @intFromEnum(DbErr.DB_DEADLOCK) => "Deadlock",
        @intFromEnum(DbErr.DB_ROLLBACK) => "Rollback",
        @intFromEnum(DbErr.DB_DUPLICATE_KEY) => "Duplicate key",
        @intFromEnum(DbErr.DB_QUE_THR_SUSPENDED) => "The queue thread has been suspended",
        @intFromEnum(DbErr.DB_MISSING_HISTORY) => "Required history data has been deleted",
        @intFromEnum(DbErr.DB_CLUSTER_NOT_FOUND) => "Cluster not found",
        @intFromEnum(DbErr.DB_TABLE_NOT_FOUND) => "Table not found",
        @intFromEnum(DbErr.DB_MUST_GET_MORE_FILE_SPACE) => "More file space needed",
        @intFromEnum(DbErr.DB_TABLE_IS_BEING_USED) => "Table is being used",
        @intFromEnum(DbErr.DB_TOO_BIG_RECORD) => "Record too big",
        @intFromEnum(DbErr.DB_LOCK_WAIT_TIMEOUT) => "Lock wait timeout",
        @intFromEnum(DbErr.DB_NO_REFERENCED_ROW) => "Referenced key value not found",
        @intFromEnum(DbErr.DB_ROW_IS_REFERENCED) => "Row is referenced",
        @intFromEnum(DbErr.DB_CANNOT_ADD_CONSTRAINT) => "Cannot add constraint",
        @intFromEnum(DbErr.DB_CORRUPTION) => "Data structure corruption",
        @intFromEnum(DbErr.DB_COL_APPEARS_TWICE_IN_INDEX) => "Column appears twice in index",
        @intFromEnum(DbErr.DB_CANNOT_DROP_CONSTRAINT) => "Cannot drop constraint",
        @intFromEnum(DbErr.DB_NO_SAVEPOINT) => "No such savepoint",
        @intFromEnum(DbErr.DB_TABLESPACE_ALREADY_EXISTS) => "Tablespace already exists",
        @intFromEnum(DbErr.DB_TABLESPACE_DELETED) => "No such tablespace",
        @intFromEnum(DbErr.DB_LOCK_TABLE_FULL) => "Lock structs have exhausted the buffer pool",
        @intFromEnum(DbErr.DB_FOREIGN_DUPLICATE_KEY) => "Foreign key activated with duplicate keys",
        @intFromEnum(DbErr.DB_TOO_MANY_CONCURRENT_TRXS) => "Too many concurrent transactions",
        @intFromEnum(DbErr.DB_UNSUPPORTED) => "Unsupported",
        @intFromEnum(DbErr.DB_PRIMARY_KEY_IS_NULL) => "Primary key is NULL",
        @intFromEnum(DbErr.DB_FAIL) => "Failed, retry may succeed",
        @intFromEnum(DbErr.DB_OVERFLOW) => "Overflow",
        @intFromEnum(DbErr.DB_UNDERFLOW) => "Underflow",
        @intFromEnum(DbErr.DB_STRONG_FAIL) => "Failed, retry will not succeed",
        @intFromEnum(DbErr.DB_ZIP_OVERFLOW) => "Zip overflow",
        @intFromEnum(DbErr.DB_RECORD_NOT_FOUND) => "Record not found",
        @intFromEnum(DbErr.DB_END_OF_INDEX) => "End of index",
        @intFromEnum(DbErr.DB_SCHEMA_ERROR) => "Error while validating a table or index schema",
        @intFromEnum(DbErr.DB_DATA_MISMATCH) => "Type mismatch",
        @intFromEnum(DbErr.DB_SCHEMA_NOT_LOCKED) => "Schema not locked",
        @intFromEnum(DbErr.DB_NOT_FOUND) => "Not found",
        @intFromEnum(DbErr.DB_READONLY) => "Readonly",
        @intFromEnum(DbErr.DB_INVALID_INPUT) => "Invalid input",
        @intFromEnum(DbErr.DB_FATAL) => "InnoDB fatal error",
        @intFromEnum(DbErr.DB_INTERRUPTED) => "Operation interrupted",
        else => "Unknown error",
    };
}

test "db_err values and strerror" {
    try std.testing.expectEqual(@as(i32, 10), @intFromEnum(DbErr.DB_SUCCESS));
    try std.testing.expectEqual(@as(i32, 30), @intFromEnum(DbErr.DB_CLUSTER_NOT_FOUND));
    try std.testing.expectEqual(@as(i32, 50), @intFromEnum(DbErr.DB_FATAL));
    try std.testing.expectEqual(@as(i32, 1000), @intFromEnum(DbErr.DB_FAIL));
    try std.testing.expectEqual(@as(i32, 1500), @intFromEnum(DbErr.DB_RECORD_NOT_FOUND));
    try std.testing.expectEqual(@as(i32, 2000), @intFromEnum(DbErr.DB_SCHEMA_ERROR));
    try std.testing.expectEqual(@as(i32, 2005), @intFromEnum(DbErr.DB_INVALID_INPUT));

    const cases = [_]struct { code: DbErr, expected: []const u8 }{
        .{ .code = .DB_SUCCESS, .expected = "Success" },
        .{ .code = .DB_ERROR, .expected = "Generic error" },
        .{ .code = .DB_OUT_OF_MEMORY, .expected = "Cannot allocate memory" },
        .{ .code = .DB_OUT_OF_FILE_SPACE, .expected = "Out of disk space" },
        .{ .code = .DB_LOCK_WAIT, .expected = "Lock wait" },
        .{ .code = .DB_DEADLOCK, .expected = "Deadlock" },
        .{ .code = .DB_ROLLBACK, .expected = "Rollback" },
        .{ .code = .DB_DUPLICATE_KEY, .expected = "Duplicate key" },
        .{ .code = .DB_QUE_THR_SUSPENDED, .expected = "The queue thread has been suspended" },
        .{ .code = .DB_MISSING_HISTORY, .expected = "Required history data has been deleted" },
        .{ .code = .DB_CLUSTER_NOT_FOUND, .expected = "Cluster not found" },
        .{ .code = .DB_TABLE_NOT_FOUND, .expected = "Table not found" },
        .{ .code = .DB_MUST_GET_MORE_FILE_SPACE, .expected = "More file space needed" },
        .{ .code = .DB_TABLE_IS_BEING_USED, .expected = "Table is being used" },
        .{ .code = .DB_TOO_BIG_RECORD, .expected = "Record too big" },
        .{ .code = .DB_LOCK_WAIT_TIMEOUT, .expected = "Lock wait timeout" },
        .{ .code = .DB_NO_REFERENCED_ROW, .expected = "Referenced key value not found" },
        .{ .code = .DB_ROW_IS_REFERENCED, .expected = "Row is referenced" },
        .{ .code = .DB_CANNOT_ADD_CONSTRAINT, .expected = "Cannot add constraint" },
        .{ .code = .DB_CORRUPTION, .expected = "Data structure corruption" },
        .{ .code = .DB_COL_APPEARS_TWICE_IN_INDEX, .expected = "Column appears twice in index" },
        .{ .code = .DB_CANNOT_DROP_CONSTRAINT, .expected = "Cannot drop constraint" },
        .{ .code = .DB_NO_SAVEPOINT, .expected = "No such savepoint" },
        .{ .code = .DB_TABLESPACE_ALREADY_EXISTS, .expected = "Tablespace already exists" },
        .{ .code = .DB_TABLESPACE_DELETED, .expected = "No such tablespace" },
        .{ .code = .DB_LOCK_TABLE_FULL, .expected = "Lock structs have exhausted the buffer pool" },
        .{ .code = .DB_FOREIGN_DUPLICATE_KEY, .expected = "Foreign key activated with duplicate keys" },
        .{ .code = .DB_TOO_MANY_CONCURRENT_TRXS, .expected = "Too many concurrent transactions" },
        .{ .code = .DB_UNSUPPORTED, .expected = "Unsupported" },
        .{ .code = .DB_PRIMARY_KEY_IS_NULL, .expected = "Primary key is NULL" },
        .{ .code = .DB_FAIL, .expected = "Failed, retry may succeed" },
        .{ .code = .DB_OVERFLOW, .expected = "Overflow" },
        .{ .code = .DB_UNDERFLOW, .expected = "Underflow" },
        .{ .code = .DB_STRONG_FAIL, .expected = "Failed, retry will not succeed" },
        .{ .code = .DB_ZIP_OVERFLOW, .expected = "Zip overflow" },
        .{ .code = .DB_RECORD_NOT_FOUND, .expected = "Record not found" },
        .{ .code = .DB_END_OF_INDEX, .expected = "End of index" },
        .{ .code = .DB_SCHEMA_ERROR, .expected = "Error while validating a table or index schema" },
        .{ .code = .DB_DATA_MISMATCH, .expected = "Type mismatch" },
        .{ .code = .DB_SCHEMA_NOT_LOCKED, .expected = "Schema not locked" },
        .{ .code = .DB_NOT_FOUND, .expected = "Not found" },
        .{ .code = .DB_READONLY, .expected = "Readonly" },
        .{ .code = .DB_INVALID_INPUT, .expected = "Invalid input" },
        .{ .code = .DB_FATAL, .expected = "InnoDB fatal error" },
        .{ .code = .DB_INTERRUPTED, .expected = "Operation interrupted" },
    };

    for (cases) |case| {
        try std.testing.expectEqualStrings(case.expected, strerror(case.code));
    }

    try std.testing.expectEqualStrings("Unknown error", strerrorCode(9999));
}
