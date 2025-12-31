const std = @import("std");
const btr = @import("innodb").btr;

fn printUsage(writer: anytype) !void {
    try writer.writeAll(
        "Usage: btr-trace [options]\n" ++
            "  --seed <u64>   RNG seed (default 0xC0FFEE)\n" ++
            "  --ops <usize>  Operation count (default 60)\n" ++
            "  --validate     Run btr_validate_index after each op\n" ++
            "  --help         Show this help\n" ++
            "\n" ++
            "Example:\n" ++
            "  btr-trace --seed 0xC0FFEE --ops 60\n",
    );
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var seed: u64 = 0xC0FFEE;
    var ops: usize = 60;
    var validate = false;

    var args = std.process.args();
    defer args.deinit();
    _ = args.next();

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help")) {
            try printUsage(std.fs.File.stdout().deprecatedWriter());
            return;
        }
        if (std.mem.eql(u8, arg, "--validate")) {
            validate = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--seed")) {
            const value = args.next() orelse {
                try printUsage(std.fs.File.stderr().deprecatedWriter());
                return error.InvalidArgument;
            };
            seed = try std.fmt.parseInt(u64, value, 0);
            continue;
        }
        if (std.mem.eql(u8, arg, "--ops")) {
            const value = args.next() orelse {
                try printUsage(std.fs.File.stderr().deprecatedWriter());
                return error.InvalidArgument;
            };
            ops = try std.fmt.parseInt(usize, value, 0);
            continue;
        }
        if (std.mem.startsWith(u8, arg, "--seed=")) {
            seed = try std.fmt.parseInt(u64, arg["--seed=".len..], 0);
            continue;
        }
        if (std.mem.startsWith(u8, arg, "--ops=")) {
            ops = try std.fmt.parseInt(usize, arg["--ops=".len..], 0);
            continue;
        }

        const stderr = std.fs.File.stderr().deprecatedWriter();
        try stderr.print("Unknown argument: {s}\n", .{arg});
        try printUsage(stderr);
        return error.InvalidArgument;
    }

    try btr.btr_debug_generate_trace(allocator, seed, ops, validate, std.fs.File.stdout().deprecatedWriter());
}
