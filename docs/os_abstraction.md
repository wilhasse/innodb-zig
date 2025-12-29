# OS Abstraction Stubs (IBD-8)

This document describes the initial Zig shims for OS services.

## C overview (early InnoDB)
- `os0file.h` defines file handles, open flags, and sync-oriented IO APIs.
- `os0thread.h` wraps thread creation, ids, sleep, and priority hooks.
- `sync0sync.h` provides mutexes, rw-locks, and condition-style primitives.

## Zig stubs
The Zig port uses minimal wrappers around the standard library:

- `src/os/file.zig`
  - `open` / `openAt` with `Create` and `Access` enums.
  - `FileHandle` exposes `readAt`, `writeAt`, `sync`, `size`, `close`.
  - `exists` / `existsAt` for basic path checks.

- `src/os/thread.zig`
  - `spawn`, `Thread.join`, `currentId`, `yield`, `sleepMicros`.

- `src/sync/mod.zig`
  - `Mutex`, `RwLock`, `CondVar` wrappers for `std.Thread` primitives.

- `src/thr/mod.zig`
  - Re-exports `os.thread` for higher-level thread coordination later.

## Platform constraints
- Uses Zig stdlib only; no native async IO or direct IO yet.
- File IO is synchronous and uses `std.fs.Dir` and `std.fs.File`.
- Thread priority APIs from C are not mapped; stubs will be added if needed.
- Windows differences are not handled yet (this follows the host platform
  behavior of Zig stdlib).
