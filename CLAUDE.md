# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Educational, non-production Zig port of early InnoDB C sources from `/home/cslog/oss-embedded-innodb`. The C code is the reference for behavior and layout; all Zig changes aim to mirror the original intent.

## Build Commands

```bash
zig build                           # Build static library
zig build test                      # Run all unit tests
zig build test -Dtest-filter=<sub>  # Run tests matching substring
zig build test -Dtest-verbose=true  # Show test stdout/stderr
zig build c-tests                   # Run original C test suite
zig build c-tests-stress            # Run C stress tests
```

Build options: `-Dcompression=true`, `-Dshared=false`, `-Datomic_ops=auto|gcc_builtins|solaris|innodb`.

## Architecture

### Module Hierarchy

The codebase mirrors InnoDB's C subsystem layout. Each `src/<module>/mod.zig` corresponds to a C directory. The canonical dependency graph is in `src/module_map.zig`.

**Layered dependency order (lower depends on nothing above):**
1. Foundation: `ut`, `mach`, `mem`
2. Platform: `os` → `sync` → `thr`
3. Storage core: `log`, `fil`, `fsp`
4. Update plumbing: `mtr`, `page`, `fut`
5. Cache: `buf`, `ibuf`
6. Structures: `rem`, `btr`, `dict`
7. Concurrency: `lock`, `trx`, `read`
8. Query: `row`, `que`, `pars`, `eval`
9. Server glue: `ddl`, `usr`, `api`, `ha`, `srv`

### Key Entry Points

- `src/lib.zig`: Main library root, exports all public modules
- `src/module_map.zig`: Machine-readable module dependency map
- `src/api/mod.zig`: External embedded InnoDB API
- `src/tests/*.zig`: Test harnesses ported from C

### Porting Conventions

- **Naming**: Files use lower_snake_case, types PascalCase, functions camelCase. Keep C names for well-known InnoDB concepts (`DbErr`, `mem_heap_t`, `UNIV_PAGE_SIZE`).
- **C layout**: Use `extern struct` to mirror C layout; verify with `@sizeOf`/`@alignOf` asserts.
- **Errors**: Map C error codes to `ut/errors.DbErr`. Use Zig error sets only internally; convert at module boundaries.
- **Logging**: Use `ut/log` (`log`, `logf`, `setLogger`) instead of `std.debug.print`.
- **Memory**: Use `mem/heap.zig` (`MemHeap`) for C-style heap lifetimes. See `docs/memory_strategy.md`.
- **Threading**: Use `os/thread.zig` and `sync/mod.zig` wrappers, not direct `std.Thread`.

### Cross-Module Rules

- Follow `src/module_map.zig` dependencies; avoid imports unless the C dependency exists.
- `ut` is for small helpers and C-compat types only; don't expand its scope.
- `dict`, `lock`, and `trx` have tight coupling in C; the Zig port splits interfaces to avoid hard cycles.

## Workflow

Work is tracked as Plane tickets in project INNODB (identifier IBD). For each ticket: study C implementation, summarize, implement Zig equivalent, add unit tests, commit.

## Documentation

- `docs/ROADMAP.md`: Porting order and test milestones
- `docs/module_boundaries.md`: Module scope definitions
- `docs/porting_conventions.md`: Full translation guidelines
- `docs/memory_strategy.md`: Memory system mapping
- `docs/c_tests.md`: Running original C tests
