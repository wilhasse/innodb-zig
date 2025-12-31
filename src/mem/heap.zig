const std = @import("std");
const compat = @import("../ut/compat.zig");

pub const HeapType = enum(u32) {
    dynamic = 0,
    buffer = 1,
    btr_search = 2,
};

const mem_block_start_size: usize = 64;
const mem_max_alloc_in_buf: usize = compat.UNIV_PAGE_SIZE - 200;
const mem_block_standard_size: usize = if (compat.UNIV_PAGE_SIZE >= 16384)
    8000
else
    mem_max_alloc_in_buf;
const heap_alignment: usize = @alignOf(usize);

const AllocationHeader = struct {
    prev_used: usize,
    size: usize,
};

fn headerSize() usize {
    return std.mem.alignForward(usize, @sizeOf(AllocationHeader), heap_alignment);
}

const Block = struct {
    buf: []u8,
    used: usize,
};

pub const Checkpoint = struct {
    block_index: usize,
    used: usize,
};

pub const MemHeap = struct {
    allocator: std.mem.Allocator,
    heap_type: HeapType,
    blocks: std.ArrayListUnmanaged(Block) = .{},
    total_size: usize = 0,
    next_block_size: usize = 0,

    pub fn init(
        allocator: std.mem.Allocator,
        start_size: usize,
        heap_type: HeapType,
    ) !MemHeap {
        var heap = MemHeap{
            .allocator = allocator,
            .heap_type = heap_type,
            .next_block_size = if (start_size == 0) mem_block_start_size else start_size,
        };
        try heap.addBlock(heap.next_block_size);
        return heap;
    }

    pub fn deinit(self: *MemHeap) void {
        while (self.blocks.items.len > 0) {
            self.popBlock();
        }
        self.blocks.deinit(self.allocator);
    }

    pub fn alloc(self: *MemHeap, n: usize) ![]u8 {
        std.debug.assert(n > 0);
        if (self.tryAlloc(n)) |buf| {
            return buf;
        }
        try self.addBlock(self.requiredBlockSize(n));
        return self.tryAlloc(n).?;
    }

    pub fn zalloc(self: *MemHeap, n: usize) ![]u8 {
        const buf = try self.alloc(n);
        @memset(buf, 0);
        return buf;
    }

    pub fn getTop(self: *MemHeap, n: usize) []u8 {
        std.debug.assert(n > 0);
        const block = self.lastBlock();
        const hsize = headerSize();
        std.debug.assert(block.used >= hsize + n);
        const data_start = block.used - n;
        const header_offset = data_start - hsize;
        const header = self.headerAt(block, header_offset);
        std.debug.assert(header.size == n);
        return block.buf[data_start .. data_start + n];
    }

    pub fn freeTop(self: *MemHeap, n: usize) void {
        std.debug.assert(n > 0);
        const block = self.lastBlock();
        const hsize = headerSize();
        std.debug.assert(block.used >= hsize + n);
        const data_start = block.used - n;
        const header_offset = data_start - hsize;
        const header = self.headerAt(block, header_offset);
        std.debug.assert(header.size == n);
        block.used = header.prev_used;
        if (block.used == 0 and self.blocks.items.len > 1) {
            self.popBlock();
        }
    }

    pub fn checkpoint(self: *const MemHeap) Checkpoint {
        const idx = self.blocks.items.len - 1;
        return .{ .block_index = idx, .used = self.blocks.items[idx].used };
    }

    pub fn release(self: *MemHeap, cp: Checkpoint) void {
        std.debug.assert(cp.block_index < self.blocks.items.len);
        while (self.blocks.items.len - 1 > cp.block_index) {
            self.popBlock();
        }
        self.blocks.items[cp.block_index].used = cp.used;
        if (cp.used == 0 and cp.block_index > 0) {
            self.popBlock();
        }
    }

    pub fn totalSize(self: *const MemHeap) usize {
        return self.total_size;
    }

    pub fn blockCount(self: *const MemHeap) usize {
        return self.blocks.items.len;
    }

    fn requiredBlockSize(self: *const MemHeap, n: usize) usize {
        _ = self;
        const hsize = headerSize();
        return n + hsize + (heap_alignment - 1);
    }

    fn tryAlloc(self: *MemHeap, n: usize) ?[]u8 {
        var block = self.lastBlock();
        const hsize = headerSize();
        const prev_used = block.used;
        const header_offset = std.mem.alignForward(usize, prev_used, heap_alignment);
        const data_start = header_offset + hsize;
        const data_end = data_start + n;
        if (data_end > block.buf.len) {
            return null;
        }
        self.writeHeader(block, header_offset, .{ .prev_used = prev_used, .size = n });
        block.used = data_end;
        return block.buf[data_start..data_end];
    }

    fn addBlock(self: *MemHeap, min_size: usize) !void {
        var size: usize = if (self.blocks.items.len == 0)
            self.next_block_size
        else
            self.next_block_size * 2;

        if (self.heap_type != .dynamic) {
            if (size > mem_max_alloc_in_buf) {
                size = mem_max_alloc_in_buf;
            }
        } else if (size > mem_block_standard_size) {
            size = mem_block_standard_size;
        }

        if (size < min_size) {
            size = min_size;
        }

        const buf = try self.allocator.alloc(u8, size);
        try self.blocks.append(self.allocator, .{ .buf = buf, .used = 0 });
        self.total_size += size;
        self.next_block_size = size;
    }

    fn lastBlock(self: *MemHeap) *Block {
        return &self.blocks.items[self.blocks.items.len - 1];
    }

    fn writeHeader(self: *MemHeap, block: *Block, offset: usize, header: AllocationHeader) void {
        _ = self;
        const header_ptr = @as(*AllocationHeader, @ptrCast(@alignCast(block.buf.ptr + offset)));
        header_ptr.* = header;
    }

    fn headerAt(self: *MemHeap, block: *Block, offset: usize) AllocationHeader {
        _ = self;
        const header_ptr = @as(*const AllocationHeader, @ptrCast(@alignCast(block.buf.ptr + offset)));
        return header_ptr.*;
    }

    fn popBlock(self: *MemHeap) void {
        const last = self.blocks.items.len - 1;
        const block = self.blocks.items[last];
        self.allocator.free(block.buf);
        self.blocks.items.len = last;
        self.total_size -= block.buf.len;
    }
};

test "mem heap alloc/free top" {
    var heap = try MemHeap.init(std.testing.allocator, 64, .dynamic);
    defer heap.deinit();

    const a = try heap.alloc(8);
    a[0] = 1;
    const b = try heap.alloc(8);
    b[0] = 2;

    const top = heap.getTop(8);
    try std.testing.expect(@intFromPtr(top.ptr) == @intFromPtr(b.ptr));

    heap.freeTop(8);
    const c = try heap.alloc(8);
    try std.testing.expect(@intFromPtr(c.ptr) == @intFromPtr(b.ptr));
}

test "mem heap checkpoint release" {
    var heap = try MemHeap.init(std.testing.allocator, 64, .dynamic);
    defer heap.deinit();

    _ = try heap.alloc(8);
    const cp = heap.checkpoint();
    const before = heap.blockCount();

    _ = try heap.alloc(2000);
    try std.testing.expect(heap.blockCount() > before);

    heap.release(cp);
    try std.testing.expectEqual(before, heap.blockCount());
}
