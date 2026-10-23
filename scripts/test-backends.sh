#!/usr/bin/env bash
# Runs the cross-backend integration suites against disposable PostgreSQL,
# MySQL and MariaDB containers (OrbStack / Docker Desktop alike).
# 在一次性 PostgreSQL / MySQL / MariaDB 容器上运行跨后端集成测试
# （OrbStack 与 Docker Desktop 均可）。
#
# Usage:
#   scripts/test-backends.sh          # start containers, test, tear down
#   KEEP=1 scripts/test-backends.sh   # keep containers after the run
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# OrbStack's docker CLI lives outside PATH for non-login shells.
# 非登录 shell 的 PATH 里通常没有 OrbStack 的 docker。
if ! command -v docker >/dev/null 2>&1; then
  for candidate in "$HOME/.orbstack/bin" "/Applications/OrbStack.app/Contents/MacOS/bin"; do
    if [ -x "$candidate/docker" ]; then
      export PATH="$candidate:$PATH"
      break
    fi
  done
fi
command -v docker >/dev/null 2>&1 || {
  echo "docker CLI not found; install OrbStack or Docker Desktop first" >&2
  exit 1
}

if command -v orb >/dev/null 2>&1 && ! orb status >/dev/null 2>&1; then
  echo "starting OrbStack..."
  orb start
fi

COMPOSE=(docker compose -f docker-compose.test.yml)
"${COMPOSE[@]}" up -d

echo "waiting for database containers..."
for service in postgres mysql mariadb; do
  container="$("${COMPOSE[@]}" ps -q "$service")"
  [ -n "$container" ] || {
    echo "container for $service not found" >&2
    exit 1
  }
  for _ in $(seq 1 90); do
    status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container" 2>/dev/null || true)"
    [ "$status" = "healthy" ] && break
    [ "$status" = "exited" ] && {
      docker logs "$container" >&2
      exit 1
    }
    sleep 1
  done
  [ "$status" = "healthy" ] || {
    echo "$service did not become healthy (last status: $status)" >&2
    docker logs "$container" >&2
    exit 1
  }
  echo "  $service: healthy"
done

export SAKUIO_TEST_POSTGRES_DSN="${SAKUIO_TEST_POSTGRES_DSN:-postgres://sakuio:sakuio@127.0.0.1:55432/sakuio_test}"
export SAKUIO_TEST_MYSQL_DSN="${SAKUIO_TEST_MYSQL_DSN:-mysql://sakuio:sakuio@127.0.0.1:53306/sakuio_test}"
export SAKUIO_TEST_MARIADB_DSN="${SAKUIO_TEST_MARIADB_DSN:-mysql://sakuio:sakuio@127.0.0.1:53307/sakuio_test}"

status=0
run() {
  echo "==> $*"
  "$@" || status=$?
}
run cargo test -p sakuio-db -- --test-threads=1
run cargo test -p sakuio-metric -- --test-threads=1
run cargo test -p sakuio-server --test acceptance -- --test-threads=1

if [ "${KEEP:-0}" != "1" ]; then
  "${COMPOSE[@]}" down -v
else
  echo "containers kept running (KEEP=1)"
fi
exit "$status"
