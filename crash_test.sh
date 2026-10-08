#!/bin/bash
# Crash-test loop (spec §18): writer killed with SIGKILL at arbitrary points,
# checker verifies no torn commit ever becomes visible.
source ~/.cargo/env
cd ~/vidgeDB
for delay_ms in 2 5 20 50 120 250 500; do
  DB=/tmp/crash_test_${delay_ms}.vdg
  rm -f "$DB" "$DB-wal"
  ./target/debug/crash_harness writer "$DB" 40 > /dev/null 2>&1 &
  WRITER=$!
  python3 -c "import time; time.sleep(${delay_ms}/1000)"
  kill -9 $WRITER 2>/dev/null
  wait $WRITER 2>/dev/null
  echo "--- delay=${delay_ms}ms ---"
  ./target/debug/crash_harness checker "$DB" 40
  echo "checker_exit=$?"
done