# Zig Porting Conventions (IBD-10)

This document defines how we translate early InnoDB C code into Zig while
preserving behavior and keeping the port readable.

## Naming
- Module names mirror C directories (`src/ut`, `src/mem`, `src/os`, ...).
- Files use lower_snake_case. Types use PascalCase. Functions use camelCase.
- Keep C names when mapping external APIs or when the C identifier is a
  well-known InnoDB concept (`DbErr`, `ib_err_t`, `mem_heap_t`).
- Constants follow C names where meaningful (`IB_TRUE`, `UNIV_PAGE_SIZE`).

## Module boundaries
- Follow `src/module_map.zig` and `docs/module_boundaries.md`.
- Avoid cross-module imports unless the C dependency exists.
- Use `ut` for small helpers and compatibility types; do not grow it into a
  grab-bag of unrelated logic.

## C interop and layout
- Use `extern struct` to mirror C layout and verify sizes with `@sizeOf`
  and `@alignOf` asserts.
- Avoid `packed` unless the C structure is explicitly packed.
- Keep pointer-sized aliases (`ulint`, `ib_ulint_t`) consistent with
  `ut/compat.zig`.

## Error handling
- Map C error codes to `ut/errors.DbErr` and return those values when
  porting C APIs.
- Use Zig error sets only for internal plumbing (e.g., stdlib IO); convert
  to `DbErr` at module boundaries.
- Prefer explicit `DbErr` returns over `error{}` for ported entry points.

## Logging
- Use `ut/log` (`log`, `logf`, `setLogger`) instead of `std.debug.print`.
- Preserve C log messages and formatting where feasible.
- Logger state is global; keep it centralized to reduce hidden dependencies.

## Memory management
- Use `mem/heap` for C-style heap lifetimes and `std.mem.Allocator` for
  Zig-managed allocations.
- Avoid mixing heap lifetimes with allocator-owned buffers without clear
  ownership transfer.
- Keep lifetimes explicit in function signatures (heap pointer or allocator).

## Threads and sync
- Use `os/thread.zig` and `sync/mod.zig` wrappers instead of direct
  `std.Thread` usage in ported modules.
- Ported code should rely on the wrapper API even if it is a thin shim.

## Testing
- Every IBD ticket should add at least one Zig unit test.
- Tests must be deterministic and fast; avoid network access.
- Use `zig build test`, optionally with `-Dtest-filter` for focus.

## Documentation
- Update or add docs in `docs/` for each ticket when behavior or APIs change.
- Add Plane ticket comments summarizing C behavior and Zig mapping.
