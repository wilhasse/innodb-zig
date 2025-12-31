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
