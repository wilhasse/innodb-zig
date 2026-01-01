const std = @import("std");
const compat = @import("../ut/compat.zig");
const os_file = @import("../os/file.zig");
const fsp = @import("../fsp/mod.zig");
const fil = @import("mod.zig");
const mach = @import("../mach/mod.zig");
const buf = @import("../buf/mod.zig");

pub const ulint = compat.ulint;
pub const ibool = compat.ibool;
pub const byte = compat.byte;

pub const DataFile = struct {
    path: []u8,
    size_mb: ulint,
    autoextend: bool,
    max_size_mb: ?ulint,
};

const ParseResult = struct {
    mb: ulint,
    rest: []const u8,
};

fn parseMegabytes(input: []const u8) ?ParseResult {
    var idx: usize = 0;
    var value: u64 = 0;
    while (idx < input.len and std.ascii.isDigit(input[idx])) : (idx += 1) {
        value = value * 10 + @as(u64, input[idx] - '0');
    }
    if (idx == 0) {
        return null;
    }
    var size = value;
    if (idx < input.len) {
        switch (input[idx]) {
            'G', 'g' => {
                size *= 1024;
                idx += 1;
            },
            'M', 'm' => {
                idx += 1;
            },
            else => {
                size /= 1024 * 1024;
            },
        }
    } else {
        size /= 1024 * 1024;
    }
    return .{ .mb = @as(ulint, @intCast(size)), .rest = input[idx..] };
}

fn findSizeDelimiter(spec: []const u8) ?usize {
    var i: usize = spec.len;
    while (i > 0) : (i -= 1) {
        const idx = i - 1;
        if (spec[idx] == ':' and idx + 1 < spec.len and std.ascii.isDigit(spec[idx + 1])) {
            return idx;
        }
    }
    return null;
}

fn parseDataFileSpec(allocator: std.mem.Allocator, spec: []const u8) !DataFile {
    const trimmed = std.mem.trim(u8, spec, " \t\r\n");
    if (trimmed.len == 0) {
        return error.InvalidInput;
    }
    const colon = findSizeDelimiter(trimmed) orelse return error.InvalidInput;
    const path_part = trimmed[0..colon];
    const size_part = trimmed[colon + 1 ..];
    const size_parse = parseMegabytes(size_part) orelse return error.InvalidInput;
    var rest = size_parse.rest;
    if (size_parse.mb == 0) {
        return error.InvalidInput;
    }
    var autoextend = false;
    var max_size: ?ulint = null;
    if (std.mem.startsWith(u8, rest, ":autoextend")) {
        rest = rest[":autoextend".len..];
        autoextend = true;
        if (std.mem.startsWith(u8, rest, ":max:")) {
            rest = rest[":max:".len..];
            const max_parse = parseMegabytes(rest) orelse return error.InvalidInput;
            max_size = max_parse.mb;
            rest = max_parse.rest;
        }
    }
    if (std.mem.startsWith(u8, rest, "new")) {
        rest = rest[3..];
    }
    if (std.mem.startsWith(u8, rest, "raw")) {
        rest = rest[3..];
    }
    if (rest.len != 0) {
        return error.InvalidInput;
    }
    const path_buf = try allocator.alloc(u8, path_part.len);
    std.mem.copyForwards(u8, path_buf, path_part);
    return .{
        .path = path_buf,
        .size_mb = size_parse.mb,
        .autoextend = autoextend,
        .max_size_mb = max_size,
    };
}

pub fn parseDataFilePaths(allocator: std.mem.Allocator, input: []const u8) ![]DataFile {
    var list = std.array_list.Managed(DataFile).init(allocator);
    errdefer {
        for (list.items) |item| {
            allocator.free(item.path);
        }
        list.deinit();
    }
    var iter = std.mem.splitScalar(u8, input, ';');
    while (iter.next()) |segment| {
        if (segment.len == 0) {
            return error.InvalidInput;
        }
        const df = try parseDataFileSpec(allocator, segment);
        try list.append(df);
    }
    return list.toOwnedSlice();
}

pub fn freeDataFilePaths(allocator: std.mem.Allocator, files: []DataFile) void {
    for (files) |file| {
        allocator.free(file.path);
    }
    allocator.free(files);
}

fn resolveDataPath(allocator: std.mem.Allocator, data_home_dir: []const u8, path: []const u8) ![]u8 {
    if (std.fs.path.isAbsolute(path)) {
        return allocator.dupe(u8, path);
    }
    return std.fmt.allocPrint(allocator, "{s}{s}", .{ data_home_dir, path });
}

fn mbToPages(mb: ulint) ulint {
    const bytes = @as(u64, mb) * 1024 * 1024;
    const page_size = @as(u64, compat.UNIV_PAGE_SIZE);
    const pages = (bytes + page_size - 1) / page_size;
    return @as(ulint, @intCast(pages));
}

fn initSystemHeader(page: []byte, space_id: ulint, flags: ulint, size_pages: ulint) void {
    @memset(page, 0);
    fsp.fsp_header_init_fields(page.ptr, space_id, flags);
    mach.mach_write_to_4(page.ptr + fsp.FSP_HEADER_OFFSET + fsp.FSP_SIZE, size_pages);
    mach.mach_write_to_4(page.ptr + fsp.FSP_HEADER_OFFSET + fsp.FSP_FREE_LIMIT, size_pages);
    mach.mach_write_to_4(page.ptr + fil.FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID, space_id);
    fil.fil_page_set_type(page.ptr, fil.FIL_PAGE_TYPE_FSP_HDR);
    buf.buf_flush_init_for_writing(page.ptr, null, 0);
}

pub fn openOrCreateSystemTablespace(data_home_dir: []const u8, data_file_path: []const u8) ibool {
    const allocator = std.heap.page_allocator;
    const files = parseDataFilePaths(allocator, data_file_path) catch return compat.FALSE;
    defer freeDataFilePaths(allocator, files);
    if (files.len == 0) {
        return compat.FALSE;
    }

    const space_id: ulint = 0;
    var total_pages: ulint = 0;
    const space_exists = fil.fil_tablespace_exists_in_mem(space_id) == compat.TRUE;
    var space_ready = space_exists;
    var created_any = false;

    for (files) |file_spec| {
        const full_path = resolveDataPath(allocator, data_home_dir, file_spec.path) catch return compat.FALSE;
        defer allocator.free(full_path);

        const desired_pages = mbToPages(file_spec.size_mb);
        var actual_pages = desired_pages;
        var file_handle: os_file.os_file_t = null;
        var success: ibool = compat.FALSE;
        var created = false;

        if (!os_file.exists(full_path)) {
            file_handle = os_file.os_file_create_simple(full_path, os_file.OS_FILE_CREATE_PATH, os_file.OS_FILE_READ_WRITE, &success);
            created = true;
        } else {
            file_handle = os_file.os_file_create_simple(full_path, os_file.OS_FILE_OPEN, os_file.OS_FILE_READ_WRITE, &success);
        }

        if (success == compat.FALSE or file_handle == null) {
            return compat.FALSE;
        }
        defer _ = os_file.os_file_close(file_handle);

        if (created) {
            const bytes = @as(u64, desired_pages) * @as(u64, compat.UNIV_PAGE_SIZE);
            if (os_file.os_file_set_size(full_path, file_handle, @as(ulint, @intCast(bytes & 0xFFFF_FFFF)), @as(ulint, @intCast(bytes >> 32))) == compat.FALSE) {
                return compat.FALSE;
            }
            const page_buf = allocator.alloc(byte, compat.UNIV_PAGE_SIZE) catch return compat.FALSE;
            defer allocator.free(page_buf);
            initSystemHeader(page_buf, space_id, 0, desired_pages);
            if (os_file.os_file_write(full_path, file_handle, page_buf.ptr, 0, 0, compat.UNIV_PAGE_SIZE) == compat.FALSE) {
                return compat.FALSE;
            }
            created_any = true;
        } else {
            var size_low: ulint = 0;
            var size_high: ulint = 0;
            if (os_file.os_file_get_size(file_handle, &size_low, &size_high) == compat.FALSE) {
                return compat.FALSE;
            }
            const bytes = (@as(u64, @intCast(size_high)) << 32) | @as(u64, @intCast(size_low));
            actual_pages = @as(ulint, @intCast(bytes / @as(u64, compat.UNIV_PAGE_SIZE)));
            if (actual_pages < desired_pages) {
                const new_bytes = @as(u64, desired_pages) * @as(u64, compat.UNIV_PAGE_SIZE);
                if (os_file.os_file_set_size(full_path, file_handle, @as(ulint, @intCast(new_bytes & 0xFFFF_FFFF)), @as(ulint, @intCast(new_bytes >> 32))) == compat.FALSE) {
                    return compat.FALSE;
                }
                actual_pages = desired_pages;
            }
        }

        total_pages += actual_pages;

        if (!space_ready) {
            if (fil.fil_space_create(full_path, space_id, 0, fil.FIL_TABLESPACE) == compat.FALSE) {
                if (fil.fil_tablespace_exists_in_mem(space_id) != compat.TRUE) {
                    return compat.FALSE;
                }
            }
            space_ready = true;
        }

        if (space_ready and !space_exists) {
            fil.fil_node_create(full_path, actual_pages, space_id, compat.FALSE);
        }
    }

    if (space_ready) {
        var mtr = fsp.mtr_t{};
        if (created_any) {
            fsp.fsp_header_init(space_id, total_pages, &mtr);
        } else {
            _ = fsp.fsp_header_load(space_id);
        }
    }

    return compat.TRUE;
}

test "parse data_file_path" {
    const allocator = std.testing.allocator;
    const files = try parseDataFilePaths(allocator, "ibdata1:32M:autoextend");
    defer freeDataFilePaths(allocator, files);
    try std.testing.expectEqual(@as(usize, 1), files.len);
    try std.testing.expect(std.mem.eql(u8, files[0].path, "ibdata1"));
    try std.testing.expectEqual(@as(ulint, 32), files[0].size_mb);
    try std.testing.expect(files[0].autoextend);
}

test "system tablespace create" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);
    const data_home = try std.fmt.allocPrint(std.testing.allocator, "{s}/", .{base});
    defer std.testing.allocator.free(data_home);

    fil.fil_init(0, 32);
    defer fil.fil_close();

    const ok = openOrCreateSystemTablespace(data_home, "ibdata1:1M:autoextend");
    try std.testing.expect(ok == compat.TRUE);

    const ibdata_path = try std.fmt.allocPrint(std.testing.allocator, "{s}ibdata1", .{data_home});
    defer std.testing.allocator.free(ibdata_path);
    try std.testing.expect(os_file.exists(ibdata_path));
    try std.testing.expect(fil.fil_tablespace_exists_in_mem(0) == compat.TRUE);
}
