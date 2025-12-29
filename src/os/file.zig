const std = @import("std");

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
        .read = true,
        .write = access == .read_write,
    };
}

fn createFlags(access: Access, truncate: bool, exclusive: bool) std.fs.File.CreateFlags {
    return .{
        .read = access == .read_write,
        .truncate = truncate,
        .exclusive = exclusive,
    };
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
