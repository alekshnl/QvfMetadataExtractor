#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$ROOT_DIR"

PORT=${PORT:-5165}
HOST=${HOST:-0.0.0.0}
MAX_UPLOAD_MB=${MAX_UPLOAD_MB:-512}
TMP_ROOT=${TMP_ROOT:-./runtime/tmp}
JOB_TTL_MINUTES=${JOB_TTL_MINUTES:-30}
KEEP_FAILED_JOBS=${KEEP_FAILED_JOBS:-false}
PYTHON_BIN=${PYTHON_BIN:-python3}
EXTRACTOR_SCRIPT=${EXTRACTOR_SCRIPT:-./scripts/extract_qvf.py}
APP_PID_FILE="$ROOT_DIR/runtime/pids/web.pid"
APP_LOG_FILE="$ROOT_DIR/runtime/logs/web.log"

log() {
  printf '[qvf-extractor] %s\n' "$*"
}

run_sudo() {
  if [[ $EUID -eq 0 ]]; then
    "$@"
  else
    sudo "$@"
  fi
}

is_ubuntu() {
  [[ -f /etc/os-release ]] || return 1
  . /etc/os-release
  [[ "${ID:-}" == "ubuntu" ]]
}

install_base_packages_ubuntu() {
  log "Installing base packages."
  run_sudo apt-get update
  run_sudo apt-get install -y curl ca-certificates gnupg python3 python3-venv lsof
}

install_node_ubuntu() {
  if command -v node >/dev/null 2>&1; then
    local major
    major=$(node -p "process.versions.node.split('.')[0]")
    if [[ "$major" -ge 20 ]]; then
      log "Node.js $(node -v) already available."
      return
    fi
  fi

  log "Installing Node.js LTS from NodeSource."
  curl -fsSL https://deb.nodesource.com/setup_22.x | run_sudo bash -
  run_sudo apt-get install -y nodejs
}

verify_mac_dependencies() {
  command -v "$PYTHON_BIN" >/dev/null 2>&1 || {
    log "Python is required. Set PYTHON_BIN if needed."
    exit 1
  }
  command -v node >/dev/null 2>&1 || {
    log "Node.js 20 or newer is required."
    exit 1
  }
  command -v npm >/dev/null 2>&1 || {
    log "npm is required."
    exit 1
  }
}

prepare_runtime() {
  mkdir -p "$ROOT_DIR/runtime/logs" "$ROOT_DIR/runtime/pids" "$ROOT_DIR/runtime/tmp/incoming" "$ROOT_DIR/runtime/tmp/jobs"
}

stop_existing_app() {
  if [[ -f "$APP_PID_FILE" ]]; then
    local pid
    pid=$(cat "$APP_PID_FILE")
    if kill -0 "$pid" >/dev/null 2>&1; then
      log "Stopping existing web application PID $pid."
      kill "$pid"
      sleep 1
    fi
    rm -f "$APP_PID_FILE"
  fi
}

install_dependencies() {
  log "Installing npm dependencies."
  npm ci
}

verify_extractor() {
  command -v "$PYTHON_BIN" >/dev/null 2>&1 || {
    log "Python executable not found: $PYTHON_BIN"
    exit 1
  }
  [[ -f "$EXTRACTOR_SCRIPT" ]] || {
    log "Extractor script not found: $EXTRACTOR_SCRIPT"
    exit 1
  }
}

start_web_app() {
  log "Starting web application on ${HOST}:${PORT}."
  : > "$APP_LOG_FILE"
  PORT="$PORT" \
  HOST="$HOST" \
  MAX_UPLOAD_MB="$MAX_UPLOAD_MB" \
  TMP_ROOT="$TMP_ROOT" \
  JOB_TTL_MINUTES="$JOB_TTL_MINUTES" \
  KEEP_FAILED_JOBS="$KEEP_FAILED_JOBS" \
  PYTHON_BIN="$PYTHON_BIN" \
  EXTRACTOR_SCRIPT="$EXTRACTOR_SCRIPT" \
    nohup npm start >>"$APP_LOG_FILE" 2>&1 &
  echo $! > "$APP_PID_FILE"
}

if is_ubuntu; then
  install_base_packages_ubuntu
  install_node_ubuntu
else
  verify_mac_dependencies
fi

verify_extractor
prepare_runtime
stop_existing_app
install_dependencies
start_web_app

cat <<EOF

Service is running.

Web UI:  http://<server-ip>:${PORT}
Health:  http://<server-ip>:${PORT}/healthz
Logs:    ${APP_LOG_FILE}
PID:     $(cat "$APP_PID_FILE")

EOF
