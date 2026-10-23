#!/usr/bin/env bash
# Measures server and agent memory with N real agent processes.
# 用 N 个真实 Agent 进程测量中控与 Agent 的内存占用。
#
# Usage:
#   scripts/bench-100.sh [nodes] [settle_seconds]
#   SETTLE=60 scripts/bench-100.sh 100
#
# Requirements: release binaries built first:
#   cargo build --release -p sakuio-server
#   (cd ../Sakuio-agent && cargo build --release)
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVER_BIN="${SAKUIO_SERVER_BIN:-$ROOT/target/release/sakuio-server}"
AGENT_BIN="${SAKUIO_AGENT_BIN:-$ROOT/../Sakuio-agent/target/release/sakuio-agent}"
NODES="${1:-100}"
SETTLE="${2:-${SETTLE:-60}}"
PORT="${SAKUIO_BENCH_PORT:-18091}"
MEASURE="$ROOT/scripts/procmem.py"

for bin in "$SERVER_BIN" "$AGENT_BIN"; do
  [ -x "$bin" ] || {
    echo "missing binary: $bin (build release first)" >&2
    exit 1
  }
done

BASE="$(mktemp -d "${TMPDIR:-/tmp}/sakuio-bench-XXXXXX")"
DB="$BASE/data/sakuio.db"
mkdir -p "$BASE/data" "$BASE/logs"
cd "$BASE"

SERVER_PID=""
AGENT_PIDS=()
cleanup() {
  for pid in "${AGENT_PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null || true
  sleep 1
  lsof -ti "tcp:$PORT" 2>/dev/null | xargs -r kill -9 2>/dev/null || true
  rm -rf "$BASE"
}
trap cleanup EXIT

RUST_LOG=error SAKUIO_LISTEN="127.0.0.1:$PORT" "$SERVER_BIN" >"$BASE/logs/server.log" 2>&1 &
SERVER_PID=$!

ready=0
for _ in $(seq 1 100); do
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "http://127.0.0.1:$PORT/ping" || true)
  [ "$code" = 200 ] && ready=1 && break
  sleep 0.2
done
[ "$ready" = 1 ] || {
  echo "server not ready" >&2
  cat "$BASE/logs/server.log" >&2
  exit 1
}

curl -s -o /dev/null -w 'install=%{http_code}\n' -X POST -H 'Content-Type: application/json' \
  -d "{\"username\":\"admin\",\"password\":\"password123\",\"sitename\":\"Bench\",\"app_db_driver\":\"sqlite\",\"app_db_dsn\":\"sqlite://$DB?mode=rwc\",\"metric_colocation\":\"same\"}" \
  "http://127.0.0.1:$PORT/api/install/complete"

base_values=$(python3 "$MEASURE" "$SERVER_PID")
read -r SERVER_BASE_RSS SERVER_BASE_THR _ _ <<<"$base_values"
echo "baseline server: rss=${SERVER_BASE_RSS}MB threads=${SERVER_BASE_THR}"

python3 - "$DB" "$NODES" "$BASE/tokens.txt" <<'PY'
import datetime
import secrets
import sqlite3
import sys
import uuid

db, count, out = sys.argv[1], int(sys.argv[2]), sys.argv[3]
now = datetime.datetime.now(datetime.timezone.utc).isoformat()
rows = [
    (str(uuid.uuid4()), f"bench-{i:03d}", secrets.token_hex(32), now, now, float(i + 1))
    for i in range(count)
]
con = sqlite3.connect(db, timeout=30)
con.executemany(
    "INSERT INTO clients (uuid,name,token,created_at,updated_at,sort_order) VALUES (?,?,?,?,?,?)",
    rows,
)
con.commit()
with open(out, "w", encoding="utf-8") as fh:
    fh.write("\n".join(row[2] for row in rows) + "\n")
PY

i=0
while IFS= read -r token; do
  RUST_LOG=error "$AGENT_BIN" -e "ws://127.0.0.1:$PORT" -t "$token" \
    --state "$BASE/agent-$i.state" --disable-remote-control --disable-auto-update \
    -i 5 --info-report-interval 60 >"$BASE/logs/agent-$i.log" 2>&1 &
  AGENT_PIDS+=($!)
  i=$((i + 1))
done <"$BASE/tokens.txt"
echo "started $i agents"

online=0
for _ in $(seq 1 120); do
  online=$(python3 - "$DB" <<'PY'
import sqlite3
import sys
try:
    con = sqlite3.connect(sys.argv[1], timeout=5)
    print(con.execute("SELECT COUNT(*) FROM clients WHERE online = 1").fetchone()[0])
except Exception:
    print(0)
PY
)
  [ "$online" -ge "$NODES" ] && break
  sleep 1
done
echo "online=$online"

elapsed=0
while [ "$elapsed" -le "$SETTLE" ]; do
  values=$(python3 "$MEASURE" "$SERVER_PID")
  read -r rss threads _ _ <<<"$values"
  echo "sample t=${elapsed}s server_rss=${rss}MB threads=${threads}"
  [ "$elapsed" -eq "$SETTLE" ] && break
  sleep 30
  elapsed=$((elapsed + 30))
done

values=$(python3 "$MEASURE" "$SERVER_PID")
read -r SERVER_RSS SERVER_THR _ _ <<<"$values"
values=$(python3 "$MEASURE" "${AGENT_PIDS[@]}")
read -r AGENT_ONE_RSS AGENT_ONE_THR AGENT_TOTAL _ <<<"$values"
DB_MB=$(du -m "$DB" | awk '{print $1}')
echo "RESULT nodes=$NODES online=$online server_baseline_mb=$SERVER_BASE_RSS server_rss_mb=$SERVER_RSS server_threads=$SERVER_THR agent_one_mb=$AGENT_ONE_RSS agent_one_threads=$AGENT_ONE_THR agents_total_mb=$AGENT_TOTAL db_mb=$DB_MB"
