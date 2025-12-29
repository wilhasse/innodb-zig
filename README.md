# innodb-zig

Educational, non-production Zig port of early InnoDB C sources.

This project is based on a local snapshot of the early InnoDB source tree
(oss-embedded-innodb). That C code is the reference for behavior and layout;
all Zig changes aim to mirror the original intent while keeping the port
readable for study.

## Layout
- src/  : Zig port modules (mirrors C subsystem layout)
- docs/ : roadmap and module boundary notes

## Build
- `zig build` builds the static library.
- `zig build test` runs unit tests.
- Build options (defaults): `-Dcompression=true`, `-Dshared=false`, `-Datomic_ops=auto` (auto|gcc_builtins|solaris|innodb).
- Test options: `-Dtest-filter=<substring>` to select tests, `-Dtest-verbose=true` to run `zig test` with visible output.
- C test runner (from the original C repo): `zig build c-tests` (see `docs/c_tests.md`).

## Workflow
- Work is tracked as Plane tickets in project INNODB (identifier IBD).
- For each ticket: study C implementation, summarize, implement Zig equivalent,
  add Zig unit tests, commit.

## Status
- IBD-1: roadmap and module map in place.
