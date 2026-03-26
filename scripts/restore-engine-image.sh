#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT_DIR"
source "$ROOT_DIR/config/engine-image.env"

ARCHIVE_PATH=${1:-$ROOT_DIR/artifacts/qlikcore-engine-${ENGINE_IMAGE_TAG}.tar}

if [[ ! -f "$ARCHIVE_PATH" ]]; then
  printf 'Archive not found: %s\n' "$ARCHIVE_PATH" >&2
  exit 1
fi

if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  DOCKER_BIN=(docker)
else
  DOCKER_BIN=(sudo docker)
fi

"${DOCKER_BIN[@]}" load -i "$ARCHIVE_PATH"
printf 'Restored image from %s\n' "$ARCHIVE_PATH"
