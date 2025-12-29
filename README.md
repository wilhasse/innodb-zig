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

## Workflow
- Work is tracked as Plane tickets in project INNODB (identifier IBD).
- For each ticket: study C implementation, summarize, implement Zig equivalent,
  add Zig unit tests, commit.

## Status
- IBD-1: roadmap and module map in place.
