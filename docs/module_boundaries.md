# Module Boundaries

This document captures the intended scope of each Zig module, mirroring the C
subsystem layout.

- api: Embedded InnoDB API wrappers and external entry points.
- btr: B-tree structures, search, and modification logic.
- buf: Buffer pool, page caching, and LRU management.
- data: Data types, tuple fields, and type metadata.
- ddl: DDL operations and dictionary updates.
- dict: Data dictionary structures and caching.
- dyn: Dynamic arrays and buffer helpers.
- eval: Expression evaluation and simple SQL functions.
- fil: File layer and file space abstraction.
- fsp: Tablespace and segment space management.
- fut: File-based list utilities on pages.
- ha: Handler glue for MySQL integration.
- ibuf: Insert buffer and change buffer operations.
- lock: Locking system (table/record queues, wait graph, compatibility).
- log: Redo log records and logging subsystem.
- mach: Byte order, encoding, and integer packing.
- mem: Memory heaps and allocation helpers.
- mtr: Mini-transaction logging and latching.
- os: OS abstraction and file IO wrappers.
- page: Page formats, headers, and low-level access.
- pars: SQL parser and query graph construction.
- que: Query graph execution nodes and control flow.
- read: Cursor read views and MVCC visibility.
- rem: Record format parsing and creation.
- row: Row operations and row-level helpers.
- srv: Server startup, main loop, and background tasks.
- sync: Mutex and rw-lock primitives.
- thr: Thread helpers and scheduling wrappers.
- trx: Transaction system, rollback segments, and state.
- usr: Session/user state and transaction bindings.
- ut: Utilities, assertions, logging, and error helpers (C-compat types/macros live here).
