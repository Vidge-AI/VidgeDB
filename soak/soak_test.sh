#!/bin/bash
# ---------------------------------------------------------------------------
# VidgeDB 2h SOAK test — churn + ingest + crashes + recovery, with integrity
# verification at every cycle. Wear-bug hunter: the correction tests are
# green, what remains to prove is USURE (wear).
#
# Usage: ./soak/soak_test.sh [--quick]        (quick = 5-minute smoke pass)
# Notes:
#   - build (release) is done HERE first, never inside the soak window;
#   - the runner is soak/soak_ingest.py (python3, stdlib only),
#     cycles: ingest ~2s / churn ~5s / retain ~90s / crash ~15s;
#   - everything lands in /tmp/soak/ (twin.vdg, expected.json, crashes.log,
#     divergences.log); the machine-validated JSON report goes to stdout;
#   - budget is HARD: the python runner enforces DURATION_S (7200s default,
#     300s with --quick) and never runs longer.
# ---------------------------------------------------------------------------
set -u
cd "$(dirname "$0")/.."

# Rustup env in a fresh shell
if [ -f "$HOME/.cargo/env" ]; then . "$HOME/.cargo/env"; fi

QUICK=0
[ "${1:-}" = "--quick" ] && QUICK=1

echo "[soak] building release binaries (vidgedb + bench) ..."
source "$HOME/.cargo/env" 2>/dev/null || true
cargo build --release --bin vidgedb --bin bench -q
BUILD_RC=$?
if [ "$BUILD_RC" -ne 0 ]; then
  echo "[soak] FATAL: cargo build failed (rc=$BUILD_RC)"
  exit 2
fi
echo "[soak] build ok."

mkdir -p /tmp/soak

if [ "$QUICK" -eq 1 ]; then
  echo "[soak] QUICK mode: SOAK_SECONDS=300"
  SOAK_SECONDS=300 exec python3 soak/soak_ingest.py
else
  echo "[soak] full run: SOAK_SECONDS=7200 (2h hard budget)"
  SOAK_SECONDS=7200 exec python3 soak/soak_ingest.py
fi