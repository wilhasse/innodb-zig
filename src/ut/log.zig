const std = @import("std");

pub const Stream = ?*anyopaque;
pub const ib_stream_t = Stream;

pub const LoggerFn = *const fn (stream: Stream, message: []const u8) void;
pub const ib_logger_t = LoggerFn;

var logger_fn: LoggerFn = defaultLogger;
var logger_stream: Stream = null;

fn defaultLogger(stream: Stream, message: []const u8) void {
    if (stream) |ptr| {
        const file = @as(*std.fs.File, @ptrCast(@alignCast(ptr)));
        _ = file.writeAll(message) catch {};
    } else {
        var stderr = std.fs.File.stderr();
        _ = stderr.writeAll(message) catch {};
    }
}

pub fn nullLogger(stream: Stream, message: []const u8) void {
    _ = stream;
    _ = message;
}

pub fn setLogger(func: LoggerFn, stream: Stream) void {
    logger_fn = func;
    logger_stream = stream;
}

pub fn getLogger() LoggerFn {
    return logger_fn;
}

pub fn getStream() Stream {
    return logger_stream;
}

pub fn log(message: []const u8) void {
    logger_fn(logger_stream, message);
}

pub fn logTo(stream: Stream, message: []const u8) void {
    logger_fn(stream, message);
}

pub fn logf(comptime fmt: []const u8, args: anytype) void {
    logfTo(logger_stream, fmt, args);
}

pub fn logfTo(stream: Stream, comptime fmt: []const u8, args: anytype) void {
    var buf: [512]u8 = undefined;
    const msg = std.fmt.bufPrint(&buf, fmt, args) catch |err| {
        if (err == error.NoSpace) {
            const heap_msg = std.fmt.allocPrint(std.heap.page_allocator, fmt, args) catch {
                logger_fn(stream, "log: formatting failed\n");
                return;
            };
            defer std.heap.page_allocator.free(heap_msg);
            logger_fn(stream, heap_msg);
            return;
        }

        logger_fn(stream, "log: formatting failed\n");
        return;
    };

    logger_fn(stream, msg);
}

test "logf uses configured logger" {
    const prev_logger = getLogger();
    const prev_stream = getStream();
    defer setLogger(prev_logger, prev_stream);

    const Capture = struct {
        buf: [64]u8,
        len: usize,

        fn logger(stream: Stream, message: []const u8) void {
            const capture = @as(*@This(), @ptrCast(@alignCast(stream.?)));
            const len = @min(message.len, capture.buf.len);
            std.mem.copyForwards(u8, capture.buf[0..len], message[0..len]);
            capture.len = len;
        }
    };

    var capture = Capture{
        .buf = undefined,
        .len = 0,
    };

    const stream = @as(Stream, @ptrCast(&capture));
    setLogger(Capture.logger, stream);
    logf("Hello {d}", .{42});

    try std.testing.expect(std.mem.eql(u8, capture.buf[0..capture.len], "Hello 42"));
}
