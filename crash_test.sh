#!/usr/bin/env bash
# 崩溃测试：多线程写到一半 kill -9，然后恢复并检查所有已确认的 key。
# memtable 调得很小，所以 kill 的时候后台多半正在 flush 或 compaction。
# 用法：./crash_test.sh [几秒后kill=2] [线程数=8] [memtable_kb=16]
set -euo pipefail
cd "$(dirname "$0")"

cmake --build build >/dev/null

MEM_KB="${3:-16}"
rm -rf test_db acked.txt

./build/kvstore write test_db 1000000 1 "${2:-8}" "$MEM_KB" > acked.txt &
pid=$!

sleep "${1:-2}"
kill -9 "$pid"
wait "$pid" 2>/dev/null || true

echo "killed after $(wc -l < acked.txt | tr -d ' ') acked puts; files: $(ls test_db | wc -l | tr -d ' ')"
./build/kvstore recover test_db acked.txt "$MEM_KB"
