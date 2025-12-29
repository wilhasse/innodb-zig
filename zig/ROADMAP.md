# Zig Port Roadmap (IBD-1)
This project ports the early InnoDB C tree at /home/cslog/oss-embedded-innodb
into Zig for educational study only.

## Module Map (C dir -> Zig module)
- api  -> zig/api  (embedded InnoDB API)
- btr  -> zig/btr  (B-tree)
- buf  -> zig/buf  (buffer pool)
- data -> zig/data (data types and tuple fields)
- ddl  -> zig/ddl  (DDL operations)
- dict -> zig/dict (data dictionary)
- dyn  -> zig/dyn  (dynamic array and buffer helpers)
- eval -> zig/eval (expression evaluation)
- fil  -> zig/fil  (file layer)
- fsp  -> zig/fsp  (tablespace/segment space management)
- fut  -> zig/fut  (file-based list utilities)
- ha   -> zig/ha   (MySQL handler glue)
- ibuf -> zig/ibuf (insert buffer)
- lock -> zig/lock (lock system)
- log  -> zig/log  (redo log)
- mach -> zig/mach (byte order and encoding)
- mem  -> zig/mem  (heap allocators)
- mtr  -> zig/mtr  (mini-transaction)
- os   -> zig/os   (OS and file IO wrappers)
- page -> zig/page (page formats and headers)
- pars -> zig/pars (SQL parser)
- que  -> zig/que  (query graph)
- read -> zig/read (cursor read and read views)
- rem  -> zig/rem  (record format)
- row  -> zig/row  (row operations)
- srv  -> zig/srv  (server startup and main loop)
- sync -> zig/sync (mutex/rw-lock primitives)
- thr  -> zig/thr  (threads)
- trx  -> zig/trx  (transactions)
- usr  -> zig/usr  (session/user state)
- ut   -> zig/ut   (utilities, asserts, lists)

The machine-readable map lives in zig/module_map.zig.

## Dependency Sketch (high level)
- Foundation: ut, mach, mem
- Platform: os -> sync -> thr
- Storage core: fil, fsp, log
- Update plumbing: mtr, page, fut
- Cache: buf, ibuf
- Structures: rem, btr, dict
- Concurrency: lock, trx, read
- Query: row, que, pars, eval
- Server glue: ddl, usr, api, ha, srv

Note: dict, lock, and trx have tight coupling in C; the Zig port will split
interfaces to avoid hard cycles where possible.

## Incremental Porting Order
1) ut, mach, mem
2) os, sync, thr
3) log, fil, fsp
4) mtr, page, fut
5) buf, ibuf
6) rem, btr
7) dict, lock, trx, read
8) row, que, pars, eval
9) ddl, usr, api, ha, srv

## Test Milestones
- Step 1: unit tests for byte order, lists, and allocators
- Step 2: deterministic tests for mutex/rw-lock wrappers and thread helpers
- Step 3: redo log record encode/decode and file IO round-trip
- Step 4: page header parse/write and file-based list operations
- Step 5: buffer pool LRU and page fetch/evict behavior
- Step 6: B-tree insert/search on synthetic pages
- Step 7: transaction state and lock compatibility matrix
- Step 8: row selection and expression evaluation on in-memory data
- Step 9: server start/stop and API smoke tests
