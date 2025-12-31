const std = @import("std");
const compat = @import("../ut/compat.zig");

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;
pub const byte = compat.byte;
pub const ib_int64_t = compat.ib_int64_t;

pub const OS_FILE_LOG_BLOCK_SIZE: ulint = 512;

pub const OS_FILE_OPEN: ulint = 51;
pub const OS_FILE_CREATE: ulint = 52;
pub const OS_FILE_OVERWRITE: ulint = 53;
pub const OS_FILE_OPEN_RAW: ulint = 54;
pub const OS_FILE_CREATE_PATH: ulint = 55;
pub const OS_FILE_OPEN_RETRY: ulint = 56;

pub const OS_FILE_READ_ONLY: ulint = 333;
pub const OS_FILE_READ_WRITE: ulint = 444;
pub const OS_FILE_READ_ALLOW_DELETE: ulint = 555;

pub const OS_FILE_AIO: ulint = 61;
pub const OS_FILE_NORMAL: ulint = 62;

pub const OS_DATA_FILE: ulint = 100;
pub const OS_LOG_FILE: ulint = 101;

pub const OS_FILE_NOT_FOUND: ulint = 71;
pub const OS_FILE_DISK_FULL: ulint = 72;
pub const OS_FILE_ALREADY_EXISTS: ulint = 73;
pub const OS_FILE_PATH_ERROR: ulint = 74;
pub const OS_FILE_AIO_RESOURCES_RESERVED: ulint = 75;
pub const OS_FILE_SHARING_VIOLATION: ulint = 76;
pub const OS_FILE_ERROR_NOT_SPECIFIED: ulint = 77;
pub const OS_FILE_INSUFFICIENT_RESOURCE: ulint = 78;
pub const OS_FILE_OPERATION_ABORTED: ulint = 79;

pub const OS_FILE_READ: ulint = 10;
pub const OS_FILE_WRITE: ulint = 11;
pub const OS_FILE_LOG: ulint = 256;

pub const OS_AIO_N_PENDING_IOS_PER_THREAD: ulint = 32;

pub const OS_AIO_NORMAL: ulint = 21;
pub const OS_AIO_IBUF: ulint = 22;
pub const OS_AIO_LOG: ulint = 23;
pub const OS_AIO_SYNC: ulint = 24;
pub const OS_AIO_SIMULATED_WAKE_LATER: ulint = 512;

pub const OS_WIN31: ulint = 1;
pub const OS_WIN95: ulint = 2;
pub const OS_WINNT: ulint = 3;
pub const OS_WIN2000: ulint = 4;

pub const OS_FILE_MAX_PATH: usize = 4000;

pub const os_file_type_t = enum(u8) {
    OS_FILE_TYPE_UNKNOWN = 0,
    OS_FILE_TYPE_FILE = 1,
    OS_FILE_TYPE_DIR = 2,
    OS_FILE_TYPE_LINK = 3,
};

pub const os_file_stat_t = struct {
    name: [OS_FILE_MAX_PATH]u8 = [_]u8{0} ** OS_FILE_MAX_PATH,
    type: os_file_type_t = .OS_FILE_TYPE_UNKNOWN,
    size: ib_int64_t = 0,
    ctime: i64 = 0,
    mtime: i64 = 0,
    atime: i64 = 0,
};

pub const Access = enum {
    read_only,
    read_write,
};

pub const Create = enum {
    open,
    create,
    overwrite,
};

pub const FileHandle = struct {
    file: std.fs.File,

    pub fn close(self: *FileHandle) void {
        self.file.close();
    }

    pub fn sync(self: *FileHandle) !void {
        try self.file.sync();
    }

    pub fn readAt(self: *FileHandle, buf: []u8, offset: u64) !usize {
        return self.file.pread(buf, offset);
    }

    pub fn writeAt(self: *FileHandle, buf: []const u8, offset: u64) !usize {
        return self.file.pwrite(buf, offset);
    }

    pub fn size(self: *FileHandle) !u64 {
        const stat = try self.file.stat();
        return stat.size;
    }
};

pub const os_file_t = ?FileHandle;

const DirHandle = struct {
    dir: std.fs.Dir,
    iter: std.fs.Dir.Iterator,
};

pub const os_file_dir_t = ?*DirHandle;

pub var os_innodb_umask: ulint = 0o660;
pub var os_aio_use_native_aio: ibool = compat.FALSE;
pub var os_aio_print_debug: ibool = compat.FALSE;
pub var os_do_not_call_flush_at_each_write: ibool = compat.FALSE;
pub var os_has_said_disk_full: ibool = compat.FALSE;

pub var os_n_file_reads: ulint = 0;
pub var os_bytes_read_since_printout: ulint = 0;
pub var os_n_file_writes: ulint = 0;
pub var os_n_fsyncs: ulint = 0;

pub var os_file_n_pending_preads: ulint = 0;
pub var os_file_n_pending_pwrites: ulint = 0;
pub var os_n_pending_reads: ulint = 0;
pub var os_n_pending_writes: ulint = 0;

var last_error_code: ulint = OS_FILE_ERROR_NOT_SPECIFIED;
var last_error: ?anyerror = null;
var file_count_mutex: std.Thread.Mutex = .{};

pub fn open(path: []const u8, create: Create, access: Access) !FileHandle {
    return openAt(std.fs.cwd(), path, create, access);
}

pub fn openAt(dir: std.fs.Dir, path: []const u8, create: Create, access: Access) !FileHandle {
    return switch (create) {
        .open => .{ .file = try dir.openFile(path, openFlags(access)) },
        .create => .{ .file = try dir.createFile(path, createFlags(access, false, true)) },
        .overwrite => .{ .file = try dir.createFile(path, createFlags(access, true, false)) },
    };
}

pub fn exists(path: []const u8) bool {
    return existsAt(std.fs.cwd(), path);
}

pub fn existsAt(dir: std.fs.Dir, path: []const u8) bool {
    dir.access(path, .{}) catch return false;
    return true;
}

fn openFlags(access: Access) std.fs.File.OpenFlags {
    return .{
        .mode = if (access == .read_write) .read_write else .read_only,
    };
}

fn createFlags(access: Access, truncate: bool, exclusive: bool) std.fs.File.CreateFlags {
    return .{
        .read = access == .read_write,
        .truncate = truncate,
        .exclusive = exclusive,
    };
}

fn setLastError(err: anyerror) void {
    last_error = err;
    last_error_code = mapError(err);
    if (last_error_code == OS_FILE_DISK_FULL) {
        os_has_said_disk_full = compat.TRUE;
    }
}

fn mapError(err: anyerror) ulint {
    return switch (err) {
        error.FileNotFound, error.PathNotFound => OS_FILE_NOT_FOUND,
        error.PathAlreadyExists => OS_FILE_ALREADY_EXISTS,
        error.AccessDenied => OS_FILE_SHARING_VIOLATION,
        error.NoSpaceLeft, error.DiskQuota => OS_FILE_DISK_FULL,
        error.NotDir => OS_FILE_PATH_ERROR,
        else => OS_FILE_ERROR_NOT_SPECIFIED,
    };
}

fn makeOffset(offset: ulint, offset_high: ulint) u64 {
    return (@as(u64, @intCast(offset_high)) << 32) | @as(u64, @intCast(offset));
}

fn fileKindToType(kind: std.fs.File.Kind) os_file_type_t {
    return switch (kind) {
        .file => .OS_FILE_TYPE_FILE,
        .directory => .OS_FILE_TYPE_DIR,
        .sym_link => .OS_FILE_TYPE_LINK,
        else => .OS_FILE_TYPE_UNKNOWN,
    };
}

fn statTimeSeconds(ns: i128) i64 {
    return @as(i64, @intCast(ns / std.time.ns_per_s));
}

fn setName(dest: *[OS_FILE_MAX_PATH]u8, src: []const u8) void {
    dest.* = [_]u8{0} ** OS_FILE_MAX_PATH;
    const len = @min(src.len, OS_FILE_MAX_PATH - 1);
    std.mem.copyForwards(u8, dest[0..len], src[0..len]);
}

pub fn os_get_os_version() ulint {
    return 0;
}

pub fn os_io_init_simple() void {
    os_file_var_init();
}

pub fn os_file_create_tmpfile() os_file_t {
    var name_buf: [64]u8 = undefined;
    var rand_bytes: [8]u8 = undefined;

    for (0..10) |_| {
        std.crypto.random.bytes(&rand_bytes);
        const suffix = std.fmt.fmtSliceHexLower(&rand_bytes);
        const name = std.fmt.bufPrint(&name_buf, "ibd-tmp-{s}.tmp", .{suffix}) catch return null;
        const file = std.fs.cwd().createFile(name, .{ .read = true, .exclusive = true }) catch |err| {
            if (err == error.PathAlreadyExists) {
                continue;
            }
            setLastError(err);
            return null;
        };
        return .{ .file = file };
    }

    return null;
}

pub fn os_file_opendir(dirname: []const u8, error_is_fatal: ibool) os_file_dir_t {
    _ = error_is_fatal;
    const dir = std.fs.cwd().openDir(dirname, .{ .iterate = true }) catch |err| {
        setLastError(err);
        return null;
    };

    const handle = std.heap.page_allocator.create(DirHandle) catch |err| {
        dir.close();
        setLastError(err);
        return null;
    };
    handle.* = .{ .dir = dir, .iter = undefined };
    handle.iter = handle.dir.iterate();
    return handle;
}

pub fn os_file_closedir(dir: os_file_dir_t) c_int {
    if (dir) |handle| {
        handle.dir.close();
        std.heap.page_allocator.destroy(handle);
        return 0;
    }
    return -1;
}

pub fn os_file_readdir_next_file(dirname: []const u8, dir: os_file_dir_t, info: *os_file_stat_t) c_int {
    const handle = dir orelse return -1;

    while (true) {
        const entry = handle.iter.next() catch |err| {
            setLastError(err);
            return -1;
        };
        if (entry == null) {
            return 1;
        }
        const ent = entry.?;
        if (std.mem.eql(u8, ent.name, ".") or std.mem.eql(u8, ent.name, "..")) {
            continue;
        }

        setName(&info.name, ent.name);
        info.type = fileKindToType(ent.kind);

        const full_path = std.fmt.allocPrint(std.heap.page_allocator, "{s}/{s}", .{ dirname, ent.name }) catch |err| {
            setLastError(err);
            return -1;
        };
        defer std.heap.page_allocator.free(full_path);

        const stat = std.fs.cwd().statFile(full_path) catch |err| {
            if (err == error.FileNotFound) {
                continue;
            }
            setLastError(err);
            return -1;
        };
        info.size = @as(ib_int64_t, @intCast(stat.size));
        info.ctime = statTimeSeconds(stat.ctime);
        info.atime = statTimeSeconds(stat.atime);
        info.mtime = statTimeSeconds(stat.mtime);
        return 0;
    }
}

pub fn os_file_create_directory(pathname: []const u8, fail_if_exists: ibool) ibool {
    std.fs.cwd().makeDir(pathname) catch |err| {
        if (!fail_if_exists and err == error.PathAlreadyExists) {
            return compat.TRUE;
        }
        setLastError(err);
        return compat.FALSE;
    };
    return compat.TRUE;
}

fn accessFromType(access_type: ulint) Access {
    return switch (access_type) {
        OS_FILE_READ_ONLY, OS_FILE_READ_ALLOW_DELETE => .read_only,
        OS_FILE_READ_WRITE => .read_write,
        else => .read_write,
    };
}

pub fn os_file_create_simple(name: []const u8, create_mode: ulint, access_type: ulint, success: *ibool) os_file_t {
    if (create_mode == OS_FILE_CREATE_PATH) {
        if (os_file_create_subdirs_if_needed(name) == compat.FALSE) {
            success.* = compat.FALSE;
            return null;
        }
    }
    const access = accessFromType(access_type);
    const create = switch (create_mode) {
        OS_FILE_OPEN, OS_FILE_OPEN_RAW, OS_FILE_OPEN_RETRY => Create.open,
        OS_FILE_CREATE, OS_FILE_CREATE_PATH => Create.create,
        OS_FILE_OVERWRITE => Create.overwrite,
        else => Create.open,
    };
    const handle = open(name, create, access) catch |err| {
        setLastError(err);
        success.* = compat.FALSE;
        return null;
    };
    success.* = compat.TRUE;
    return handle;
}

pub fn os_file_create_simple_no_error_handling(
    name: []const u8,
    create_mode: ulint,
    access_type: ulint,
    success: *ibool,
) os_file_t {
    return os_file_create_simple(name, create_mode, access_type, success);
}

pub fn os_file_set_nocache(fd: i32, file_name: []const u8, operation_name: []const u8) void {
    _ = fd;
    _ = file_name;
    _ = operation_name;
}

pub fn os_file_create(name: []const u8, create_mode: ulint, purpose: ulint, type_: ulint, success: *ibool) os_file_t {
    _ = purpose;
    _ = type_;
    return os_file_create_simple(name, create_mode, OS_FILE_READ_WRITE, success);
}

pub fn os_file_delete_if_exists(name: []const u8) ibool {
    std.fs.cwd().deleteFile(name) catch |err| {
        if (err == error.FileNotFound) {
            return compat.TRUE;
        }
        setLastError(err);
        return compat.FALSE;
    };
    return compat.TRUE;
}

pub fn os_file_delete(name: []const u8) ibool {
    std.fs.cwd().deleteFile(name) catch |err| {
        setLastError(err);
        return compat.FALSE;
    };
    return compat.TRUE;
}

pub fn os_file_rename(oldpath: []const u8, newpath: []const u8) ibool {
    std.fs.cwd().rename(oldpath, newpath) catch |err| {
        setLastError(err);
        return compat.FALSE;
    };
    return compat.TRUE;
}

pub fn os_file_close(file: os_file_t) ibool {
    if (file) |handle| {
        handle.file.close();
        return compat.TRUE;
    }
    return compat.FALSE;
}

pub fn os_file_close_no_error_handling(file: os_file_t) ibool {
    return os_file_close(file);
}

pub fn os_file_get_size(file: os_file_t, size: *ulint, size_high: *ulint) ibool {
    const handle = file orelse return compat.FALSE;
    const stat = handle.file.stat() catch |err| {
        setLastError(err);
        return compat.FALSE;
    };
    const value = stat.size;
    size.* = @as(ulint, @intCast(value & 0xFFFF_FFFF));
    size_high.* = @as(ulint, @intCast(value >> 32));
    return compat.TRUE;
}

pub fn os_file_get_size_as_iblonglong(file: os_file_t) ib_int64_t {
    var size_low: ulint = 0;
    var size_high: ulint = 0;
    if (os_file_get_size(file, &size_low, &size_high) == compat.FALSE) {
        return -1;
    }
    return (@as(ib_int64_t, @intCast(size_high)) << 32) + @as(ib_int64_t, @intCast(size_low));
}

pub fn os_file_set_size(name: []const u8, file: os_file_t, size: ulint, size_high: ulint) ibool {
    const handle = file orelse return compat.FALSE;
    const desired: u64 = makeOffset(size, size_high);
    if (desired == 0) {
        handle.file.setEndPos(0) catch |err| {
            setLastError(err);
            return compat.FALSE;
        };
        return compat.TRUE;
    }

    const page_size = compat.UNIV_PAGE_SIZE;
    const max_pages = @min(@as(u64, 64), desired / @as(u64, @intCast(page_size)));
    const buf_size = @as(usize, @intCast(@max(@as(u64, 1), max_pages) * @as(u64, @intCast(page_size))));
    const buf = std.heap.page_allocator.alloc(u8, buf_size) catch |err| {
        setLastError(err);
        return compat.FALSE;
    };
    defer std.heap.page_allocator.free(buf);
    @memset(buf, 0);

    var current: u64 = 0;
    while (current < desired) {
        const remaining = desired - current;
        const chunk = @min(@as(u64, @intCast(buf.len)), remaining);
        const ok = os_file_write(name, file, buf.ptr, @as(ulint, @intCast(current & 0xFFFF_FFFF)), @as(ulint, @intCast(current >> 32)), @as(ulint, @intCast(chunk)));
        if (ok == compat.FALSE) {
            return compat.FALSE;
        }
        current += chunk;
    }
    return os_file_flush(file);
}

pub fn os_file_set_eof(file: os_file_t) ibool {
    const handle = file orelse return compat.FALSE;
    const pos = handle.file.getPos() catch |err| {
        setLastError(err);
        return compat.FALSE;
    };
    handle.file.setEndPos(pos) catch |err| {
        setLastError(err);
        return compat.FALSE;
    };
    return compat.TRUE;
}

pub fn os_file_flush(file: os_file_t) ibool {
    const handle = file orelse return compat.FALSE;
    handle.file.sync() catch |err| {
        setLastError(err);
        return compat.FALSE;
    };
    os_n_fsyncs += 1;
    return compat.TRUE;
}

pub fn os_file_get_last_error(report_all_errors: ibool) ulint {
    _ = report_all_errors;
    _ = last_error;
    return last_error_code;
}

fn os_file_pread_internal(file: os_file_t, buf: [*]byte, n: ulint, offset: ulint, offset_high: ulint) bool {
    const handle = file orelse return false;
    const len = @as(usize, @intCast(n));
    const slice = buf[0..len];
    const offs = makeOffset(offset, offset_high);

    os_n_file_reads += 1;
    os_bytes_read_since_printout += n;

    file_count_mutex.lock();
    os_file_n_pending_preads += 1;
    os_n_pending_reads += 1;
    file_count_mutex.unlock();

    const read_bytes = handle.file.pread(slice, offs) catch |err| {
        file_count_mutex.lock();
        os_file_n_pending_preads -= 1;
        os_n_pending_reads -= 1;
        file_count_mutex.unlock();
        setLastError(err);
        return false;
    };

    file_count_mutex.lock();
    os_file_n_pending_preads -= 1;
    os_n_pending_reads -= 1;
    file_count_mutex.unlock();

    return read_bytes == len;
}

fn os_file_pwrite_internal(file: os_file_t, buf: [*]const byte, n: ulint, offset: ulint, offset_high: ulint) bool {
    const handle = file orelse return false;
    const len = @as(usize, @intCast(n));
    const slice = buf[0..len];
    const offs = makeOffset(offset, offset_high);

    os_n_file_writes += 1;

    file_count_mutex.lock();
    os_file_n_pending_pwrites += 1;
    os_n_pending_writes += 1;
    file_count_mutex.unlock();

    const written = handle.file.pwrite(slice, offs) catch |err| {
        file_count_mutex.lock();
        os_file_n_pending_pwrites -= 1;
        os_n_pending_writes -= 1;
        file_count_mutex.unlock();
        setLastError(err);
        return false;
    };

    file_count_mutex.lock();
    os_file_n_pending_pwrites -= 1;
    os_n_pending_writes -= 1;
    file_count_mutex.unlock();

    return written == len;
}

pub fn os_file_read(file: os_file_t, buf: [*]byte, offset: ulint, offset_high: ulint, n: ulint) ibool {
    return if (os_file_pread_internal(file, buf, n, offset, offset_high)) compat.TRUE else compat.FALSE;
}

pub fn os_file_read_no_error_handling(file: os_file_t, buf: [*]byte, offset: ulint, offset_high: ulint, n: ulint) ibool {
    return os_file_read(file, buf, offset, offset_high, n);
}

pub fn os_file_read_string(file: *FileHandle, buf: []u8) void {
    if (buf.len == 0) {
        return;
    }
    file.file.seekTo(0) catch return;
    const limit = buf.len - 1;
    const n = file.file.read(buf[0..limit]) catch return;
    buf[n] = 0;
}

pub fn os_file_write(name: []const u8, file: os_file_t, buf: [*]const byte, offset: ulint, offset_high: ulint, n: ulint) ibool {
    _ = name;
    if (!os_file_pwrite_internal(file, buf, n, offset, offset_high)) {
        return compat.FALSE;
    }
    if (os_do_not_call_flush_at_each_write == compat.FALSE) {
        return os_file_flush(file);
    }
    return compat.TRUE;
}

pub fn os_file_status(path: []const u8, exists_out: *ibool, type_out: *os_file_type_t) ibool {
    const stat = std.fs.cwd().statFile(path) catch |err| {
        if (err == error.FileNotFound or err == error.PathNotFound) {
            exists_out.* = compat.FALSE;
            type_out.* = .OS_FILE_TYPE_UNKNOWN;
            return compat.TRUE;
        }
        setLastError(err);
        return compat.FALSE;
    };
    exists_out.* = compat.TRUE;
    type_out.* = fileKindToType(stat.kind);
    return compat.TRUE;
}

pub fn os_file_get_status(path: []const u8, stat_info: *os_file_stat_t) ibool {
    const stat = std.fs.cwd().statFile(path) catch |err| {
        if (err == error.FileNotFound or err == error.PathNotFound) {
            return compat.FALSE;
        }
        setLastError(err);
        return compat.FALSE;
    };
    stat_info.type = fileKindToType(stat.kind);
    stat_info.size = @as(ib_int64_t, @intCast(stat.size));
    stat_info.ctime = statTimeSeconds(stat.ctime);
    stat_info.atime = statTimeSeconds(stat.atime);
    stat_info.mtime = statTimeSeconds(stat.mtime);
    return compat.TRUE;
}

pub fn os_file_dirname(path: []const u8) [:0]u8 {
    const sep = std.fs.path.sep;
    const last_slash = std.mem.lastIndexOfScalar(u8, path, sep) orelse {
        return std.heap.page_allocator.dupeZ(u8, ".") catch unreachable;
    };
    if (last_slash == 0) {
        return std.heap.page_allocator.dupeZ(u8, "/") catch unreachable;
    }
    return std.heap.page_allocator.dupeZ(u8, path[0..last_slash]) catch unreachable;
}

pub fn os_file_create_subdirs_if_needed(path: []const u8) ibool {
    const dir = os_file_dirname(path);
    defer std.heap.page_allocator.free(dir);

    if (dir.len == 1 and (dir[0] == std.fs.path.sep or dir[0] == '.')) {
        return compat.TRUE;
    }
    std.fs.cwd().makePath(dir[0..dir.len]) catch |err| {
        setLastError(err);
        return compat.FALSE;
    };
    return compat.TRUE;
}

pub fn os_aio_init(n_per_seg: ulint, n_read_segs: ulint, n_write_segs: ulint, n_slots_sync: ulint) void {
    _ = n_per_seg;
    _ = n_read_segs;
    _ = n_write_segs;
    _ = n_slots_sync;
}

pub fn os_aio_free() void {}

pub fn os_aio(type_: ulint, mode: ulint, name: []const u8, file: os_file_t, buf: *anyopaque, offset: ulint, offset_high: ulint, n: ulint, message1: ?*anyopaque, message2: ?*anyopaque) ibool {
    _ = type_;
    _ = mode;
    _ = name;
    _ = file;
    _ = buf;
    _ = offset;
    _ = offset_high;
    _ = n;
    _ = message1;
    _ = message2;
    return compat.FALSE;
}

pub fn os_aio_wake_all_threads_at_shutdown() void {}

pub fn os_aio_wait_until_no_pending_writes() void {}

pub fn os_aio_simulated_wake_handler_threads() void {}

pub fn os_aio_simulated_put_read_threads_to_sleep() void {}

pub fn os_aio_simulated_handle(segment: ulint, message1: *?*anyopaque, message2: *?*anyopaque, type_out: *ulint) ibool {
    _ = segment;
    message1.* = null;
    message2.* = null;
    type_out.* = 0;
    return compat.FALSE;
}

pub fn os_aio_validate() ibool {
    return compat.TRUE;
}

pub fn os_aio_print() void {}

pub fn os_aio_refresh_stats() void {}

pub fn os_file_var_init() void {
    os_has_said_disk_full = compat.FALSE;
    os_n_file_reads = 0;
    os_bytes_read_since_printout = 0;
    os_n_file_writes = 0;
    os_n_fsyncs = 0;
    os_file_n_pending_preads = 0;
    os_file_n_pending_pwrites = 0;
    os_n_pending_reads = 0;
    os_n_pending_writes = 0;
    last_error_code = OS_FILE_ERROR_NOT_SPECIFIED;
    last_error = null;
}

pub fn os_aio_close() void {}

pub fn os_set_io_thread_op_info(i: ulint, str: []const u8) void {
    _ = i;
    _ = str;
}

test "file open/create read/write" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = "io.dat";
    var handle = try openAt(tmp.dir, path, .create, .read_write);
    defer handle.close();

    const payload = "abc";
    try std.testing.expectEqual(@as(usize, 3), try handle.writeAt(payload, 0));
    try handle.sync();

    var buf: [3]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 3), try handle.readAt(&buf, 0));
    try std.testing.expect(std.mem.eql(u8, &buf, payload));
    try std.testing.expect(existsAt(tmp.dir, path));
    try std.testing.expect((try handle.size()) >= payload.len);
}

test "os file create simple and read/write" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);
    const path = try std.fmt.allocPrint(std.testing.allocator, "{s}/os-file.dat", .{base});
    defer std.testing.allocator.free(path);

    var success: ibool = compat.FALSE;
    const handle = os_file_create_simple(path, OS_FILE_CREATE, OS_FILE_READ_WRITE, &success);
    try std.testing.expectEqual(compat.TRUE, success);
    defer _ = os_file_close(handle);

    const payload = "hello";
    const ok_write = os_file_write(path, handle, payload.ptr, 0, 0, @as(ulint, payload.len));
    try std.testing.expectEqual(compat.TRUE, ok_write);

    var buf: [5]byte = undefined;
    const ok_read = os_file_read(handle, buf[0..].ptr, 0, 0, @as(ulint, buf.len));
    try std.testing.expectEqual(compat.TRUE, ok_read);
    try std.testing.expect(std.mem.eql(u8, &buf, payload));

    var size_low: ulint = 0;
    var size_high: ulint = 0;
    try std.testing.expectEqual(compat.TRUE, os_file_get_size(handle, &size_low, &size_high));
    try std.testing.expectEqual(@as(ulint, 0), size_high);
    try std.testing.expect(size_low >= payload.len);
}

test "os file status rename delete" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);
    const path = try std.fmt.allocPrint(std.testing.allocator, "{s}/rename-a.dat", .{base});
    defer std.testing.allocator.free(path);
    const renamed = try std.fmt.allocPrint(std.testing.allocator, "{s}/rename-b.dat", .{base});
    defer std.testing.allocator.free(renamed);

    var success: ibool = compat.FALSE;
    const handle = os_file_create_simple(path, OS_FILE_CREATE, OS_FILE_READ_WRITE, &success);
    try std.testing.expectEqual(compat.TRUE, success);
    defer _ = os_file_close(handle);

    var exists_out: ibool = compat.FALSE;
    var type_out: os_file_type_t = .OS_FILE_TYPE_UNKNOWN;
    try std.testing.expectEqual(compat.TRUE, os_file_status(path, &exists_out, &type_out));
    try std.testing.expectEqual(compat.TRUE, exists_out);
    try std.testing.expectEqual(os_file_type_t.OS_FILE_TYPE_FILE, type_out);

    try std.testing.expectEqual(compat.TRUE, os_file_rename(path, renamed));

    exists_out = compat.FALSE;
    type_out = .OS_FILE_TYPE_UNKNOWN;
    try std.testing.expectEqual(compat.TRUE, os_file_status(path, &exists_out, &type_out));
    try std.testing.expectEqual(compat.FALSE, exists_out);

    try std.testing.expectEqual(compat.TRUE, os_file_delete_if_exists(renamed));
    try std.testing.expectEqual(compat.TRUE, os_file_delete_if_exists(renamed));
}

test "os file dirname and create subdirs" {
    const dir = os_file_dirname("foo/bar/baz");
    defer std.heap.page_allocator.free(dir);
    try std.testing.expect(std.mem.eql(u8, dir[0..dir.len], "foo/bar"));

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);
    const path = try std.fmt.allocPrint(std.testing.allocator, "{s}/nested/a/b/c.dat", .{base});
    defer std.testing.allocator.free(path);

    try std.testing.expectEqual(compat.TRUE, os_file_create_subdirs_if_needed(path));
    const dir_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/nested/a/b", .{base});
    defer std.testing.allocator.free(dir_path);

    const stat = try std.fs.cwd().statFile(dir_path);
    try std.testing.expectEqual(std.fs.File.Kind.directory, stat.kind);
}

test "os file set size" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);
    const path = try std.fmt.allocPrint(std.testing.allocator, "{s}/size.dat", .{base});
    defer std.testing.allocator.free(path);

    var success: ibool = compat.FALSE;
    const handle = os_file_create_simple(path, OS_FILE_CREATE, OS_FILE_READ_WRITE, &success);
    try std.testing.expectEqual(compat.TRUE, success);
    defer _ = os_file_close(handle);

    const desired = compat.UNIV_PAGE_SIZE * 2;
    try std.testing.expectEqual(compat.TRUE, os_file_set_size(path, handle, @as(ulint, desired), 0));

    var size_low: ulint = 0;
    var size_high: ulint = 0;
    try std.testing.expectEqual(compat.TRUE, os_file_get_size(handle, &size_low, &size_high));
    try std.testing.expectEqual(@as(ulint, desired), size_low);
    try std.testing.expectEqual(@as(ulint, 0), size_high);
}
