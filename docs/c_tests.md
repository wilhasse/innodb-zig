# Embedded InnoDB C Tests (IBD-9)

The original C test suite lives under
`/home/cslog/oss-embedded-innodb/tests`. The Zig build exposes a wrapper
to run those tests using the existing Makefile targets.

## Prerequisites
1. Configure and build the C project (autotools):
   - `cd /home/cslog/oss-embedded-innodb`
   - `./configure`
   - `make`

2. The tests write data/log files in the C repo. Delete old files if needed:
   - `make test-clean`

## Run from Zig
- Default test suite:
  - `zig build c-tests`
- Override the C repo path:
  - `zig build c-tests -Dc-tests-root=/path/to/oss-embedded-innodb`
- Run a subset (space-separated list):
  - `zig build c-tests -Dc-tests-list="ib_cfg ib_test1"`
- Stress tests:
  - `zig build c-tests-stress`
- Custom make binary:
  - `zig build c-tests -Dc-tests-make=gmake`

## Notes
- These steps invoke `make test` and `make test-stress` in the C repo.
- Failures usually mean the C tree is not configured or the libraries are
  missing (zlib/pthread).
- Zig tests now cover redo log header/flush and recovery scan/apply; run
  `zig build test` to exercise the log persistence path and startup recovery.
- Log files are created under `log_group_home_dir` (default `.`); remove
  `ib_logfile*` if you want a clean run.
