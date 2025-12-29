# Memory Strategy (IBD-7)

This document captures how the early InnoDB memory system maps to Zig
allocators for the port.

## C overview (early InnoDB)
- `ut0mem.h` wraps malloc/free with bookkeeping and debug hooks.
- `mem0mem.h` defines `mem_heap_t`, a heap of linked blocks with bump
  allocation and limited LIFO free (`mem_heap_free_top` / `mem_heap_get_top`).
- `mem_alloc`/`mem_free` allocate a single buffer by creating a heap with
  one block and freeing the whole heap via pointer arithmetic.
- Growth strategy: block sizes double until a standard size, then stay
  constant unless a larger request arrives.

## Zig mapping
The Zig port uses a layered approach:

1. **System allocator**
   - Backed by `std.mem.Allocator` (likely GPA or page allocator).
   - Replaces `ut_malloc`/`ut_free` for global allocations.

2. **Heap allocator (mem heap)**
   - `MemHeap` in `src/mem/heap.zig` models `mem_heap_t`.
   - Bump allocation with per-allocation headers to support `getTop` and
     `freeTop`.
   - Growth mirrors C: double until `mem_block_standard_size`, cap for
     buffer-backed heaps.

3. **Convenience helpers**
   - `mem_heap_zalloc`, `mem_heap_strdup`, `mem_heap_strdupl`, etc. will be
     provided as thin wrappers around `MemHeap` once call sites are ported.

## Interface decisions
- Fixed alignment is based on pointer size (similar to `UNIV_MEM_ALIGNMENT`).
- `MemHeap` exposes:
  - `alloc`, `zalloc`
  - `getTop`, `freeTop`
  - `checkpoint`, `release` (Zig-friendly replacement for
    `mem_heap_free_heap_top`)
  - `totalSize`, `blockCount`
- Buffer-pool backed heaps and `MEM_HEAP_BTR_SEARCH` failure semantics are
  deferred; the interface is prepared to add those behaviors.

## Migration plan (C -> Zig)
- `mem_heap_create` -> `MemHeap.init(allocator, start_size, heap_type)`
- `mem_heap_free` -> `MemHeap.deinit()`
- `mem_heap_alloc` -> `MemHeap.alloc()`
- `mem_heap_zalloc` -> `MemHeap.zalloc()`
- `mem_heap_get_top` -> `MemHeap.getTop()`
- `mem_heap_free_top` -> `MemHeap.freeTop()`
- `mem_heap_free_heap_top` -> `MemHeap.release(checkpoint)`
- `mem_alloc`/`mem_free` -> small wrapper over the system allocator that
  stores size metadata (to be added when first call sites are ported).

## Notes
- We keep the heap API explicit to make allocation lifetimes visible in Zig.
- Debug hooks from `mem0dbg` are intentionally skipped until correctness
  comes first.
