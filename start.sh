#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$ROOT_DIR"

source "$ROOT_DIR/config/engine-image.env"

PORT=${PORT:-5165}
HOST=${HOST:-0.0.0.0}
ENGINE_URL=${ENGINE_URL:-127.0.0.1:9076}
MAX_UPLOAD_MB=${MAX_UPLOAD_MB:-512}
TMP_ROOT=${TMP_ROOT:-./runtime/tmp}
JOB_TTL_MINUTES=${JOB_TTL_MINUTES:-30}
QLIK_BIN=${QLIK_BIN:-./bin/qlik}
ENGINE_CONTAINER_NAME=${ENGINE_CONTAINER_NAME:-qlik-engine}
ENGINE_DOCS_DIR=${ENGINE_DOCS_DIR:-$ROOT_DIR/runtime/engine-docs}
APP_PID_FILE="$ROOT_DIR/runtime/pids/web.pid"
APP_LOG_FILE="$ROOT_DIR/runtime/logs/web.log"
INCOMING_DIR="$ROOT_DIR/runtime/tmp/incoming"

log() {
  printf '[qvf-extractor] %s\n' "$*"
}

require_ubuntu_2404() {
  if [[ ! -f /etc/os-release ]]; then
    log "Unable to determine operating system."
    exit 1
  fi

  . /etc/os-release
  if [[ "${ID:-}" != "ubuntu" || "${VERSION_ID:-}" != "24.04" ]]; then
    log "This installer supports Ubuntu 24.04 only. Detected: ${PRETTY_NAME:-unknown}."
    exit 1
  fi
}

require_sudo() {
  if ! command -v sudo >/dev/null 2>&1; then
    log "sudo is required."
    exit 1
  fi
}

sudo_run() {
  sudo "$@"
}

docker_cmd() {
  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    docker "$@"
  else
    sudo docker "$@"
  fi
}

install_base_packages() {
  log "Installing base packages."
  sudo_run apt-get update
  sudo_run apt-get install -y curl ca-certificates gnupg zip unzip tar jq lsof
}

install_docker() {
  if command -v docker >/dev/null 2>&1; then
    log "Docker already installed."
  else
    log "Installing Docker Engine."
    sudo_run install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo_run gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    sudo_run chmod a+r /etc/apt/keyrings/docker.gpg
    echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
      $(. /etc/os-release && echo \"$VERSION_CODENAME\") stable" | \
      sudo_run tee /etc/apt/sources.list.d/docker.list >/dev/null
    sudo_run apt-get update
    sudo_run apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
  fi

  log "Ensuring Docker service is enabled and running."
  sudo_run systemctl enable docker
  sudo_run systemctl restart docker
}

install_node() {
  if command -v node >/dev/null 2>&1; then
    local major
    major=$(node -p "process.versions.node.split('.')[0]")
    if [[ "$major" -ge 20 ]]; then
      log "Node.js $(node -v) already available."
      return
    fi
  fi

  log "Installing Node.js LTS from NodeSource."
  curl -fsSL https://deb.nodesource.com/setup_22.x | sudo_run bash -
  sudo_run apt-get install -y nodejs
}

install_npm_dependencies() {
  log "Installing npm dependencies."
  npm ci
}

install_qlik_cli() {
  mkdir -p "$ROOT_DIR/bin"

  if [[ -x "$QLIK_BIN" ]]; then
    local current_version
    current_version=$($QLIK_BIN version 2>/dev/null | awk 'NF {print $NF}' | tail -n 1 || true)
    if [[ "$current_version" == "$QLIK_CLI_VERSION" ]]; then
      log "Qlik CLI $QLIK_CLI_VERSION already installed."
      return
    fi
  fi

  log "Installing Qlik CLI $QLIK_CLI_VERSION."
  tmp_archive=$(mktemp)
  curl -fsSL "$QLIK_CLI_LINUX_URL" -o "$tmp_archive"
  tar -xzf "$tmp_archive" -C "$ROOT_DIR/bin"
  rm -f "$tmp_archive"
  chmod +x "$QLIK_BIN"
}

prepare_runtime() {
  mkdir -p "$ROOT_DIR/runtime/logs" "$ROOT_DIR/runtime/pids" "$TMP_ROOT" "$INCOMING_DIR" "$ENGINE_DOCS_DIR"
}

engine_running() {
  docker_cmd inspect "$ENGINE_CONTAINER_NAME" >/dev/null 2>&1 && \
    [[ "$(docker_cmd inspect -f '{{.State.Running}}' "$ENGINE_CONTAINER_NAME")" == "true" ]]
}

pull_engine_image() {
  log "Pulling Qlik Core engine image."
  docker_cmd pull "$ENGINE_IMAGE_REF"
}

start_engine() {
  if engine_running; then
    log "Qlik Core engine container already running."
    return
  fi

  if docker_cmd inspect "$ENGINE_CONTAINER_NAME" >/dev/null 2>&1; then
    log "Removing stopped engine container."
    docker_cmd rm -f "$ENGINE_CONTAINER_NAME" >/dev/null
  fi

  log "Starting Qlik Core engine container."
  docker_cmd run -d \
    --name "$ENGINE_CONTAINER_NAME" \
    -p 127.0.0.1:9076:9076 \
    -v "$ENGINE_DOCS_DIR:/engine-data" \
    "$ENGINE_IMAGE_REF" \
    -S AcceptEULA=yes -S DocumentDirectory=/engine-data >/dev/null
}

wait_for_engine() {
  local host=${ENGINE_URL%:*}
  local port=${ENGINE_URL##*:}
  local attempt

  log "Waiting for Qlik Core engine on $ENGINE_URL."
  for attempt in $(seq 1 40); do
    if bash -c "</dev/tcp/$host/$port" >/dev/null 2>&1; then
      log "Engine is reachable."
      return
    fi
    sleep 2
  done

  log "Qlik Core engine did not become reachable in time."
  exit 1
}

start_web_app() {
  if [[ -f "$APP_PID_FILE" ]]; then
    local old_pid
    old_pid=$(cat "$APP_PID_FILE")
    if kill -0 "$old_pid" >/dev/null 2>&1; then
      log "Web application already running on PID $old_pid."
      return
    fi
    rm -f "$APP_PID_FILE"
  fi

  log "Starting web application on ${HOST}:${PORT}."
  nohup env \
    PORT="$PORT" \
    HOST="$HOST" \
    ENGINE_URL="$ENGINE_URL" \
    MAX_UPLOAD_MB="$MAX_UPLOAD_MB" \
    TMP_ROOT="$TMP_ROOT" \
    JOB_TTL_MINUTES="$JOB_TTL_MINUTES" \
    QLIK_BIN="$QLIK_BIN" \
    ENGINE_CONTAINER_NAME="$ENGINE_CONTAINER_NAME" \
    node "$ROOT_DIR/server/index.js" >>"$APP_LOG_FILE" 2>&1 &
  echo $! > "$APP_PID_FILE"
}

show_summary() {
  cat <<SUMMARY

Service is running.

Web UI:  http://<server-ip>:${PORT}
Health:  http://<server-ip>:${PORT}/healthz
Logs:    ${APP_LOG_FILE}
PID:     $(cat "$APP_PID_FILE")

Make sure TCP port ${PORT} is allowed in the Hetzner firewall.
SUMMARY
}

require_ubuntu_2404
require_sudo
prepare_runtime
install_base_packages
install_docker
install_node
install_npm_dependencies
install_qlik_cli
pull_engine_image
start_engine
wait_for_engine
start_web_app
show_summary
