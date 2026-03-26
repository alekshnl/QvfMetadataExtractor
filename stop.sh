#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$ROOT_DIR"

PID_FILE="$ROOT_DIR/runtime/pids/web.pid"
ENGINE_CONTAINER_NAME=${ENGINE_CONTAINER_NAME:-qlik-engine}

log() {
  printf '[qvf-extractor] %s\n' "$*"
}

docker_cmd() {
  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    docker "$@"
  else
    sudo docker "$@"
  fi
}

if [[ -f "$PID_FILE" ]]; then
  pid=$(cat "$PID_FILE")
  if kill -0 "$pid" >/dev/null 2>&1; then
    log "Stopping web application PID $pid."
    kill "$pid"
  fi
  rm -f "$PID_FILE"
fi

if docker_cmd inspect "$ENGINE_CONTAINER_NAME" >/dev/null 2>&1; then
  log "Stopping engine container $ENGINE_CONTAINER_NAME."
  docker_cmd rm -f "$ENGINE_CONTAINER_NAME" >/dev/null
fi

log "Shutdown complete."
