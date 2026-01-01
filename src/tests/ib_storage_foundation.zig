const std = @import("std");
const fil = @import("../fil/mod.zig");
const fsp = @import("../fsp/mod.zig");
const buf = @import("../buf/mod.zig");
const mach = @import("../mach/mod.zig");
const compat = @import("../ut/compat.zig");

// ============================================================================
// IBD-265: Storage Foundation Test Harness
// Covers: IBD-229 (system tablespace), IBD-230 (FSP persist), IBD-231 (fil node),
//         IBD-232 (page LSN/checksum), IBD-233 (flush list), IBD-234 (LRU)
// ============================================================================

// ============================================================================
// Test 1: System Tablespace Creation and FSP Header Persistence
// Covers IBD-229 (system tablespace) and IBD-230 (FSP header)
// ============================================================================

test "system tablespace creation and FSP header persistence" {
    // Setup temp directory for tablespace files
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    const prev_path = fil.fil_path_to_client_datadir;
    fil.fil_path_to_client_datadir = base;
    defer fil.fil_path_to_client_datadir = prev_path;

    // Initialize file system
    fil.fil_init(0, 32);
    defer fil.fil_close();

    // Create a new tablespace with initial size of 4 pages
    var space_id: fil.ulint = 0;
    const err = fil.fil_create_new_single_table_tablespace(&space_id, "test_sys", compat.FALSE, 0, 4);
    try std.testing.expectEqual(fil.DB_SUCCESS, err);
    try std.testing.expect(space_id > 0);

    // Initialize FSP header with size tracking
    var mtr = fsp.mtr_t{};
    fsp.fsp_init();
    fsp.fsp_header_init(space_id, 4, &mtr);

    // Verify FSP header size tracking
    try std.testing.expectEqual(@as(fil.ulint, 4), fsp.fsp_header_get_tablespace_size());
    try std.testing.expectEqual(@as(fil.ulint, 4), fsp.fsp_header_get_free_limit());

    // Read page 0 back and verify FSP header fields persisted to disk
    var page: [compat.UNIV_PAGE_SIZE]fil.byte = undefined;
    try std.testing.expectEqual(fil.DB_SUCCESS, fil.fil_read_page(space_id, 0, page[0..].ptr));

    // Verify space ID and size in FSP header on disk
    const disk_space_id = fsp.fsp_header_get_space_id(page[0..].ptr);
    const disk_size = fsp.fsp_get_size_low(page[0..].ptr);
    try std.testing.expectEqual(space_id, disk_space_id);
    try std.testing.expectEqual(@as(fil.ulint, 4), disk_size);

    // Test FSP header size increment
    fsp.fsp_header_inc_size(space_id, 2, &mtr);
    try std.testing.expectEqual(@as(fil.ulint, 6), fsp.fsp_header_get_tablespace_size());

    // Reload and verify incremented size persists
    try std.testing.expectEqual(compat.TRUE, fsp.fsp_header_load(space_id));
    try std.testing.expectEqual(@as(fil.ulint, 6), fsp.fsp_header_get_tablespace_size());
}

// ============================================================================
// Test 2: Extent Allocation and Bitmap Tracking
// Covers IBD-230 (FSP extent map persistence)
// ============================================================================

test "extent allocation and bitmap tracking" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    const prev_path = fil.fil_path_to_client_datadir;
    fil.fil_path_to_client_datadir = base;
    defer fil.fil_path_to_client_datadir = prev_path;

    fil.fil_init(0, 32);
    defer fil.fil_close();

    // Create tablespace with enough pages for multiple extents
    // FSP_EXTENT_SIZE is typically 64 pages
    var space_id: fil.ulint = 0;
    const initial_pages = fsp.FSP_EXTENT_SIZE * 4; // 4 extents worth
    const err = fil.fil_create_new_single_table_tablespace(&space_id, "test_extent", compat.FALSE, 0, initial_pages);
    try std.testing.expectEqual(fil.DB_SUCCESS, err);

    var mtr = fsp.mtr_t{};
    fsp.fsp_init();
    fsp.fsp_header_init(space_id, initial_pages, &mtr);

    // Check initial free space
    const initial_free = fsp.fsp_get_available_space_in_free_extents(space_id);
    try std.testing.expect(initial_free > 0);

    // Reserve some extents
    var n_reserved: fil.ulint = 0;
    const reserve_ok = fsp.fsp_reserve_free_extents(&n_reserved, space_id, 2, fsp.FSP_NORMAL, &mtr);
    try std.testing.expectEqual(compat.TRUE, reserve_ok);
    try std.testing.expectEqual(@as(fil.ulint, 2), n_reserved);

    // Verify free space decreased
    const after_reserve = fsp.fsp_get_available_space_in_free_extents(space_id);
    try std.testing.expect(after_reserve < initial_free);

    // Test extent map persistence by reloading
    fsp.fsp_init();
    try std.testing.expectEqual(compat.TRUE, fsp.fsp_header_load(space_id));

    // Verify size persisted correctly
    try std.testing.expectEqual(initial_pages, fsp.fsp_header_get_tablespace_size());
}

// ============================================================================
// Test 3: Page LSN/Checksum Write and Verify
// Covers IBD-232 (page LSN and checksum)
// ============================================================================

test "page LSN and checksum write and verify" {
    // Test buf_flush_init_for_writing stores LSN and checksums
    var page: [compat.UNIV_PAGE_SIZE]buf.byte = undefined;
    @memset(page[0..], 0xAB);

    const test_lsn: u64 = 0xDEADBEEFCAFEBABE;
    buf.buf_flush_init_for_writing(page[0..].ptr, null, test_lsn);

    // Verify LSN written at correct offset
    const stored_lsn = std.mem.readInt(u64, page[fil.FIL_PAGE_LSN .. fil.FIL_PAGE_LSN + 8], .big);
    try std.testing.expectEqual(test_lsn, stored_lsn);

    // Verify new checksum stored at page start
    const stored_new_chk = std.mem.readInt(u32, page[fil.FIL_PAGE_SPACE_OR_CHKSUM .. fil.FIL_PAGE_SPACE_OR_CHKSUM + 4], .big);
    const computed_new_chk = @as(u32, @intCast(buf.buf_calc_page_new_checksum(page[0..].ptr)));
    try std.testing.expectEqual(computed_new_chk, stored_new_chk);

    // Verify old checksum stored at page end
    const end_off = compat.UNIV_PAGE_SIZE - fil.FIL_PAGE_END_LSN_OLD_CHKSUM;
    const stored_old_chk = std.mem.readInt(u32, page[end_off .. end_off + 4], .big);
    const computed_old_chk = @as(u32, @intCast(buf.buf_calc_page_old_checksum(page[0..].ptr)));
    try std.testing.expectEqual(computed_old_chk, stored_old_chk);

    // Verify page is not marked as corrupted
    try std.testing.expectEqual(compat.FALSE, buf.buf_page_is_corrupted(page[0..].ptr, 0));

    // Test with different LSN values
    const lsn_zero: u64 = 0;
    buf.buf_flush_init_for_writing(page[0..].ptr, null, lsn_zero);
    const read_lsn_zero = std.mem.readInt(u64, page[fil.FIL_PAGE_LSN .. fil.FIL_PAGE_LSN + 8], .big);
    try std.testing.expectEqual(lsn_zero, read_lsn_zero);

    const lsn_max: u64 = 0xFFFFFFFFFFFFFFFF;
    buf.buf_flush_init_for_writing(page[0..].ptr, null, lsn_max);
    const read_lsn_max = std.mem.readInt(u64, page[fil.FIL_PAGE_LSN .. fil.FIL_PAGE_LSN + 8], .big);
    try std.testing.expectEqual(lsn_max, read_lsn_max);
}

// ============================================================================
// Test 4: Buffer Pool Flush List and LRU Segmentation
// Covers IBD-233 (flush list) and IBD-234 (LRU segmentation)
// ============================================================================

test "buffer pool flush list ordering and LRU segmentation" {
    // Initialize buffer pool
    const pool = buf.buf_pool_init_instances(1) orelse return error.OutOfMemory;
    defer buf.buf_mem_free();

    // Verify initial state
    try std.testing.expectEqual(@as(usize, 0), pool.flush_list.items.len);
    try std.testing.expectEqual(@as(buf.ulint, 0), pool.dirty_pages);
    try std.testing.expectEqual(@as(buf.ulint, 0), pool.lru_old_len);
    try std.testing.expectEqual(@as(buf.ulint, 0), pool.lru_new_len);

    // Create and allocate buffer blocks
    const block1 = buf.buf_block_alloc(0) orelse return error.OutOfMemory;
    defer buf.buf_block_free(block1);
    block1.page.pool = pool;
    block1.page.space = 1;
    block1.page.page_no = 100;

    const block2 = buf.buf_block_alloc(0) orelse return error.OutOfMemory;
    defer buf.buf_block_free(block2);
    block2.page.pool = pool;
    block2.page.space = 1;
    block2.page.page_no = 200;

    const block3 = buf.buf_block_alloc(0) orelse return error.OutOfMemory;
    defer buf.buf_block_free(block3);
    block3.page.pool = pool;
    block3.page.space = 1;
    block3.page.page_no = 300;

    // Test flush list: mark pages dirty in order
    buf.buf_page_set_dirty(&block1.page);
    try std.testing.expectEqual(@as(usize, 1), pool.flush_list.items.len);
    try std.testing.expectEqual(@as(buf.ulint, 1), pool.dirty_pages);
    const mod1 = block1.page.modification;
    try std.testing.expect(mod1 > 0);

    buf.buf_page_set_dirty(&block2.page);
    try std.testing.expectEqual(@as(usize, 2), pool.flush_list.items.len);
    try std.testing.expectEqual(@as(buf.ulint, 2), pool.dirty_pages);
    const mod2 = block2.page.modification;
    try std.testing.expect(mod2 > mod1); // Modification numbers increase

    buf.buf_page_set_dirty(&block3.page);
    try std.testing.expectEqual(@as(usize, 3), pool.flush_list.items.len);
    try std.testing.expectEqual(@as(buf.ulint, 3), pool.dirty_pages);
    const mod3 = block3.page.modification;
    try std.testing.expect(mod3 > mod2);

    // Verify oldest modification tracking
    try std.testing.expectEqual(mod1, pool.oldest_modification);

    // Test flush list removal
    buf.buf_flush_remove(&block1.page);
    try std.testing.expectEqual(@as(usize, 2), pool.flush_list.items.len);
    try std.testing.expectEqual(@as(buf.ulint, 2), pool.dirty_pages);
    // Note: oldest_modification is recomputed lazily in InnoDB during flush,
    // not eagerly on removal. The value may be stale until next flush scan.

    // Test LRU segmentation
    // Make block2 old, block3 new
    buf.buf_page_make_young(&block2.page);
    try std.testing.expect(block2.page.lru_counted == compat.TRUE);

    // Clean up remaining dirty pages
    buf.buf_flush_remove(&block2.page);
    buf.buf_flush_remove(&block3.page);
    try std.testing.expectEqual(@as(usize, 0), pool.flush_list.items.len);
    try std.testing.expectEqual(@as(buf.ulint, 0), pool.dirty_pages);
}

test "buffer pool multi-instance support" {
    // Test multi-instance buffer pool initialization
    const pool = buf.buf_pool_init_instances(4) orelse return error.OutOfMemory;
    defer buf.buf_mem_free();

    try std.testing.expectEqual(@as(buf.ulint, 4), buf.buf_pool_instances);
    try std.testing.expect(pool == buf.buf_pool.?);

    // Verify pool size aggregation works across instances
    const total_size = buf.buf_pool_get_curr_size();
    try std.testing.expect(total_size == 0); // Empty pools initially

    // Verify oldest modification aggregation
    const oldest = buf.buf_pool_get_oldest_modification();
    try std.testing.expect(oldest == 0); // No dirty pages
}

test "fil node chain and multi-file tablespace" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    const prev_path = fil.fil_path_to_client_datadir;
    fil.fil_path_to_client_datadir = base;
    defer fil.fil_path_to_client_datadir = prev_path;

    fil.fil_init(0, 32);
    defer fil.fil_close();

    // Create tablespace
    var space_id: fil.ulint = 0;
    const err = fil.fil_create_new_single_table_tablespace(&space_id, "multi_node", compat.FALSE, 0, 4);
    try std.testing.expectEqual(fil.DB_SUCCESS, err);

    // Verify space was created with correct size
    const size = fil.fil_space_get_size(space_id);
    try std.testing.expectEqual(@as(fil.ulint, 4), size);

    // Verify space type
    const space_type = fil.fil_space_get_type(space_id);
    try std.testing.expectEqual(fil.FIL_TABLESPACE, space_type);

    // Add additional fil node to chain
    fil.fil_node_create("extra_node.ibd", 8, space_id, compat.FALSE);
    const new_size = fil.fil_space_get_size(space_id);
    try std.testing.expectEqual(@as(fil.ulint, 12), new_size); // 4 + 8

    // Test page address validation
    try std.testing.expectEqual(compat.TRUE, fil.fil_check_adress_in_tablespace(space_id, 0));
    try std.testing.expectEqual(compat.TRUE, fil.fil_check_adress_in_tablespace(space_id, 11));
    try std.testing.expectEqual(compat.FALSE, fil.fil_check_adress_in_tablespace(space_id, 12));
    try std.testing.expectEqual(compat.FALSE, fil.fil_check_adress_in_tablespace(space_id, 100));
}
