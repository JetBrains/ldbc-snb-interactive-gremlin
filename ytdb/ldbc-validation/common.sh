#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK_DIR="${SCRIPT_DIR}/work"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"

ensure_dirs() {
  mkdir -p \
    "${WORK_DIR}/data" \
    "${WORK_DIR}/data-backup" \
    "${WORK_DIR}/results" \
    "${WORK_DIR}/databases" \
    "${WORK_DIR}/conf" \
    "${WORK_DIR}/log" \
    "${WORK_DIR}/secrets"
  printf '%s' 'root' > "${WORK_DIR}/secrets/root_password"
}

compose() {
  docker compose -f "${COMPOSE_FILE}" "$@"
}

wait_for_ytdb_health() {
  local ytdb_id
  ytdb_id="$(compose ps -q ytdb)"
  if [ -z "${ytdb_id}" ]; then
    echo "Failed to find ytdb container. Is the service up?" >&2
    return 1
  fi

  local status
  for _ in $(seq 1 60); do
    status="$(docker inspect -f '{{.State.Health.Status}}' "${ytdb_id}" 2>/dev/null || true)"
    if [ "${status}" = "healthy" ]; then
      return 0
    fi
    sleep 1
  done

  echo "ytdb did not become healthy in time." >&2
  compose logs ytdb >&2 || true
  return 1
}

ensure_ytdb() {
  compose up -d ytdb
  wait_for_ytdb_health
}
