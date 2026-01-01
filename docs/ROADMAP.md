# Zig Port Roadmap (IBD-1)
This project ports the early InnoDB C tree at /home/cslog/oss-embedded-innodb
into Zig for educational study only.

## Module Map (C dir -> Zig module)
- api  -> src/api  (embedded InnoDB API)
- btr  -> src/btr  (B-tree)
- buf  -> src/buf  (buffer pool)
- data -> src/data (data types and tuple fields)
- ddl  -> src/ddl  (DDL operations)
- dict -> src/dict (data dictionary)
- dyn  -> src/dyn  (dynamic array and buffer helpers)
- eval -> src/eval (expression evaluation)
- fil  -> src/fil  (file layer)
- fsp  -> src/fsp  (tablespace/segment space management)
- fut  -> src/fut  (file-based list utilities)
- ha   -> src/ha   (MySQL handler glue)
- ibuf -> src/ibuf (insert buffer)
- lock -> src/lock (lock system)
- log  -> src/log  (redo log)
- mach -> src/mach (byte order and encoding)
- mem  -> src/mem  (heap allocators)
- mtr  -> src/mtr  (mini-transaction)
- os   -> src/os   (OS and file IO wrappers)
- page -> src/page (page formats and headers)
- pars -> src/pars (SQL parser)
- que  -> src/que  (query graph)
- read -> src/read (cursor read and read views)
- rem  -> src/rem  (record format)
- row  -> src/row  (row operations)
- srv  -> src/srv  (server startup and main loop)
- sync -> src/sync (mutex/rw-lock primitives)
- thr  -> src/thr  (threads)
- trx  -> src/trx  (transactions)
- usr  -> src/usr  (session/user state)
- ut   -> src/ut   (utilities, asserts, lists)

The machine-readable map lives in src/module_map.zig.

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
- Step 3: redo log header/create + buffer flush, record codec, recovery scan/apply
- Step 4: page header parse/write and file-based list operations
- Step 5: buffer pool LRU and page fetch/evict behavior
- Step 6: B-tree insert/search on synthetic pages
- Step 7: transaction state and lock compatibility matrix
- Step 8: row selection and expression evaluation on in-memory data
- Step 9: server start/stop and API smoke tests
- Step 10: MVCC read view visibility and rollback coverage

## MVCC and Undo Log Implementation (IBD-208 to IBD-216)

The transaction system now includes multi-version concurrency control (MVCC)
support with undo logging for rollback and consistent reads.

### Undo Log Subsystem (`src/trx/undo.zig`)
- Undo record format with dulint-based undo_no
- Per-transaction insert and update undo logs
- Record append/pop operations with LIFO ordering
- Memory-efficient undo storage with configurable limits

### Read View (`src/read/mod.zig`)
- Snapshot isolation via `read_view_t` structure
- Visibility rules: up_limit_id, low_limit_id, active transaction list
- Creator transaction sees own changes
- Clone and lifecycle management for cursor views

### Row Version Chain (`src/row/vers.zig`)
- Linked list of row versions with transaction IDs
- Version traversal for consistent reads
- Delete-marked version handling
- Chain length and visibility counting

### Rollback (`src/row/undo.zig`)
- Apply undo records in reverse order (LIFO)
- Support for insert, update, and delete-mark undo
- Savepoint rollback to partial transaction state
- Callback-based undo application for storage integration

### Purge System (`src/trx/purge.zig`)
- Track active read views for purge eligibility
- Oldest view limit determines purgeable undo records
- Per-transaction and per-log purge operations
- Statistics tracking for monitoring

### Test Coverage (`src/tests/ib_mvcc.zig`)
- Insert/update/delete rollback tests
- Read view visibility with version chains
- Savepoint rollback (single and nested)
- Concurrent read/write scenarios
- Purge eligibility after view closure
- Edge cases: empty transactions, failed undo, long chains
