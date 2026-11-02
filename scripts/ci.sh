#!/usr/bin/env bash
# CI gate for oboe: build the server, run the default and stress suites, build
# the benchmark, and execute the embeddable library example.
set -euo pipefail

cd "$(dirname "$0")/.."

ODIN_BIN="${ODIN:-odin}"
OUT_DIR="${OUT_DIR:-/tmp/oboe_ci}"
TOTAL_START=$SECONDS

run() {
	local start=$SECONDS
	printf '\n==> %s\n' "$*"
	"$@"
	printf '<== ok: %s (%ds)\n' "$*" "$((SECONDS - start))"
}

if ! command -v "$ODIN_BIN" >/dev/null 2>&1; then
	printf 'error: %s not found in PATH (set ODIN to override)\n' "$ODIN_BIN" >&2
	exit 1
fi

mkdir -p "$OUT_DIR"

run "$ODIN_BIN" version
run "$ODIN_BIN" build cmd -out:"$OUT_DIR/oboe"
run "$ODIN_BIN" test tests -all-packages
run "$ODIN_BIN" test tests -all-packages -define:ODIN_STRESS=1
run "$ODIN_BIN" build bench -out:"$OUT_DIR/oboe_bench"
run "$ODIN_BIN" run examples/embed

printf '\nci: all steps passed in %ds\n' "$((SECONDS - TOTAL_START))"
