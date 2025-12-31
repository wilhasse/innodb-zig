# B-tree Trace Harness

This harness generates deterministic B-tree operations and prints a trace that can
be compared with an equivalent C harness.

## Usage

Run via the build step:

```
zig build btr-trace -- --seed 0xC0FFEE --ops 60
```

Or run the installed binary:

```
zig build install
./zig-out/bin/btr_trace --seed 0xC0FFEE --ops 60
```

Options:
- `--seed <u64>`: RNG seed (default `0xC0FFEE`).
- `--ops <usize>`: number of operations (default `60`).
- `--validate`: run `btr_validate_index` after each operation.

## Trace format

Each line is one operation:
- `I <key>`: insert key
- `D <key>`: delete key
- `S <key> <0|1>`: search for key, 1 if found
- `final <count> <k1> <k2> ...`: final sorted key list

## Tests

The unit test `btr trace output hash` locks the output via a stable FNV-1a hash.
If you intentionally change the trace behavior, update the expected hash in
`src/tests/ib_btr_trace.zig`.

## Zig vs C comparison

The script `scripts/compare_btr_trace.sh` builds/runs the Zig trace tool and a
small C harness against the embedded InnoDB source tree, then diffs the output.

Environment variables:
- `C_ROOT`: path to the embedded InnoDB C tree (default `/home/cslog/oss-embedded-innodb`)
- `SEED`: RNG seed (default `0xC0FFEE`)
- `OPS`: operation count (default `60`)
- `WORKDIR`: where to run the C harness (default: temp dir under `tmp/`)

Example:

```
scripts/compare_btr_trace.sh
```
