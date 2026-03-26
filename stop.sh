#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$ROOT_DIR"

PID_FILE="$ROOT_DIR/runtime/pids/web.pid"

log() {
  printf '[qvf-extractor] %s\n' "$*"
}

if [[ -f "$PID_FILE" ]]; then
  pid=$(cat "$PID_FILE")
  if kill -0 "$pid" >/dev/null 2>&1; then
    log "Stopping web application PID $pid."
    kill "$pid"
  fi
  rm -f "$PID_FILE"
fi

log "Shutdown complete."
