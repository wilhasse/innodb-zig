#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
C_ROOT=${C_ROOT:-/home/cslog/oss-embedded-innodb}
SEED=${SEED:-0xC0FFEE}
OPS=${OPS:-60}

TMP_BASE=${TMP_BASE:-"$ROOT_DIR/tmp"}
mkdir -p "$TMP_BASE"

if [ -z "${WORKDIR:-}" ]; then
  WORKDIR=$(mktemp -d "$TMP_BASE/btr_trace_XXXXXX")
else
  WORKDIR=${WORKDIR}
  mkdir -p "$WORKDIR"
fi

ZIG_TRACE="$WORKDIR/zig_trace.txt"
C_TRACE="$WORKDIR/c_trace.txt"
C_BIN="$WORKDIR/btr_trace_c"

zig build btr-trace -- --seed "$SEED" --ops "$OPS" > "$ZIG_TRACE"

if [ ! -f "$C_ROOT/.libs/libinnodb.so" ] && [ ! -f "$C_ROOT/.libs/libinnodb.a" ]; then
  if [ ! -f "$C_ROOT/config.h" ]; then
    (cd "$C_ROOT" && ./configure)
  fi
  (cd "$C_ROOT" && make)
fi

cc -O2 -I"$C_ROOT/include" -I"$C_ROOT/tests" -I"$C_ROOT" \
  "$ROOT_DIR/tools/c/btr_trace.c" "$C_ROOT/tests/test0aux.c" \
  -L"$C_ROOT/.libs" -linnodb -Wl,-rpath,"$C_ROOT/.libs" \
  -o "$C_BIN"

(
  cd "$WORKDIR"
  "$C_BIN" --seed "$SEED" --ops "$OPS" > "$C_TRACE"
)

diff -u "$ZIG_TRACE" "$C_TRACE"
