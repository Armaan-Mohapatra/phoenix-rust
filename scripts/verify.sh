#!/usr/bin/env bash
# Phoenix reproducible verification gate (Linux/macOS/WSL/Git-Bash).
#
# Runs every in-repo Rust check and prints one line per gate:
#   PASS | FAIL | BLOCKED | NOT_RUN
# plus the exact commit, toolchain, and per-gate test counts.
#
# Read-only with respect to source data: it never reads E:\PhoenixResearch,
# E:\TradingNextData, or any path outside the checkout and CARGO_TARGET_DIR.
#
# Usage:  scripts/verify.sh [--offline] [--with-hft] [--report FILE]
#   --offline   pass --offline to cargo (use only the local dependency cache)
#   --with-hft  also build the optional hftbacktest feature (needs git fetch
#               of a pinned upstream revision and a Rust >= 1.91.1 toolchain)
#   --report    also write the summary to FILE (default: stdout only)
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OFFLINE=""
WITH_HFT=0
REPORT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --offline) OFFLINE="--offline" ;;
    --with-hft) WITH_HFT=1 ;;
    --report) shift; REPORT="${1:-}" ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}"
export RUST_BACKTRACE=0
# Archive/index safety checks deliberately reject any symlinked ancestor path. macOS
# $TMPDIR lives under /var -> /private/var, so fixtures need the canonical path.
_t="${TMPDIR:-/tmp}"
export TMPDIR="$(cd "$_t" && pwd -P)/"
LOGDIR="$(mktemp -d "${TMPDIR:-/tmp}/phoenix-verify.XXXXXX")"
LINES=()
FAILS=0

commit="$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || echo UNKNOWN)"
dirty="$(git -C "$ROOT" status --porcelain 2>/dev/null | grep -v '^??' | wc -l | tr -d ' ')"
rustc_v="$(rustc --version 2>/dev/null || echo 'rustc MISSING')"
cargo_v="$(cargo --version 2>/dev/null || echo 'cargo MISSING')"

counts() { # sum "test result" lines in a log -> "passed=N failed=M ignored=K"
  awk '/^test result:/{p+=$4;f+=$6;i+=$8}END{printf "passed=%d failed=%d ignored=%d",p,f,i}' "$1"
}
record() { LINES+=("$(printf '%-8s %-44s %s' "$1" "$2" "$3")"); [ "$1" = FAIL ] && FAILS=$((FAILS+1)); }

# Run a gate. A network/dependency-resolution failure is BLOCKED, not FAIL.
gate() { # name dir cmd...
  local name="$1" dir="$2"; shift 2
  local log="$LOGDIR/$(echo "$name" | tr '/ ' '__').log"
  ( cd "$dir" && "$@" ) >"$log" 2>&1
  local rc=$?
  local detail=""
  grep -q '^test result:' "$log" && detail="$(counts "$log")"
  if [ $rc -eq 0 ]; then
    record PASS "$name" "$detail"
  elif grep -qiE 'failed to (download|fetch|get)|unsuccessful tunnel|Could not resolve host|no matching package|attempting to make an HTTP request, but --offline' "$log"; then
    record BLOCKED "$name" "dependency/network unavailable (log: $log)"
  else
    record FAIL "$name" "$detail rc=$rc (log: $log)"
  fi
}

if ! command -v cargo >/dev/null 2>&1; then
  record BLOCKED "toolchain" "cargo not on PATH"
else
  gate "root/fmt" "$ROOT" cargo fmt --all -- --check
  gate "root/check" "$ROOT" cargo check $OFFLINE --workspace --all-targets
  gate "root/test" "$ROOT" cargo test $OFFLINE --workspace --all-targets
  gate "root/doctest" "$ROOT" cargo test $OFFLINE --workspace --doc
  gate "root/clippy" "$ROOT" cargo clippy $OFFLINE --workspace --all-targets -- -D warnings
  gate "root/cli-smoke" "$ROOT" cargo run $OFFLINE -q -p phoenix-cli
  SMOKE_OUT="$LOGDIR/campaign-smoke-out"
  gate "root/campaign-smoke" "$ROOT" cargo run $OFFLINE -q --release -p phoenix-campaign -- \
    --scenario efficient --assets 2 --days 4 --seed 1 --threads 2 --out "$SMOKE_OUT"
  for d in "$ROOT"/integrations/*/; do
    n="$(basename "$d")"
    gate "int/$n/fmt" "$d" cargo fmt -- --check
    gate "int/$n/test" "$d" cargo test $OFFLINE --all-targets
    gate "int/$n/clippy" "$d" cargo clippy $OFFLINE --all-targets -- -D warnings
  done
  if [ $WITH_HFT -eq 1 ]; then
    gate "int/hftbacktest-adapter/upstream-hft" "$ROOT/integrations/hftbacktest-adapter" \
      cargo test $OFFLINE --features upstream-hft
  else
    record NOT_RUN "int/hftbacktest-adapter/upstream-hft" "opt-in: --with-hft"
  fi
  if command -v rustup >/dev/null 2>&1 && rustup run 1.78.0 rustc --version >/dev/null 2>&1; then
    CARGO_TARGET_DIR="$CARGO_TARGET_DIR/msrv" gate "root/msrv-1.78-check" "$ROOT" \
      cargo +1.78.0 check $OFFLINE --workspace --all-targets
  else
    record BLOCKED "root/msrv-1.78-check" "Rust 1.78.0 toolchain not installed"
  fi
fi
record NOT_RUN "historical/original-data-replay" "needs operator-mounted read-only archives"
record NOT_RUN "windows/runtime" "run scripts/validate-phoenix-locally.ps1 on Windows"

{
  echo "Phoenix verification"
  echo "commit:   $commit (tracked-modified files: $dirty)"
  echo "rustc:    $rustc_v"
  echo "cargo:    $cargo_v"
  echo "host:     $(uname -srm 2>/dev/null)"
  echo "logs:     $LOGDIR"
  echo
  printf '%s\n' "${LINES[@]}"
  echo
  for s in PASS FAIL BLOCKED NOT_RUN; do
    printf '%s=%s ' "$s" "$(printf '%s\n' "${LINES[@]}" | grep -c "^$s ")"
  done
  echo
} | tee ${REPORT:+"$REPORT"}
[ $FAILS -eq 0 ]
