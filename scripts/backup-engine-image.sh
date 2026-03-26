#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT_DIR"
source "$ROOT_DIR/config/engine-image.env"

OUTPUT_DIR=${1:-$ROOT_DIR/artifacts}
ARCHIVE_PATH="$OUTPUT_DIR/qlikcore-engine-${ENGINE_IMAGE_TAG}.tar"
MANIFEST_PATH="$OUTPUT_DIR/qlikcore-engine-${ENGINE_IMAGE_TAG}.txt"

mkdir -p "$OUTPUT_DIR"

if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  DOCKER_BIN=(docker)
else
  DOCKER_BIN=(sudo docker)
fi

"${DOCKER_BIN[@]}" pull "$ENGINE_IMAGE_REF"
"${DOCKER_BIN[@]}" save "$ENGINE_IMAGE_REF" -o "$ARCHIVE_PATH"
actual_digest=$("${DOCKER_BIN[@]}" image inspect "$ENGINE_IMAGE_REF" --format '{{index .RepoDigests 0}}' | awk -F'@' '{print $2}')

cat > "$MANIFEST_PATH" <<MANIFEST
image_name=$ENGINE_IMAGE_NAME
image_tag=$ENGINE_IMAGE_TAG
image_digest=$actual_digest
image_ref=${ENGINE_IMAGE_NAME}:${ENGINE_IMAGE_TAG}@${actual_digest}
archive_path=$ARCHIVE_PATH
MANIFEST

printf 'Saved archive to %s\n' "$ARCHIVE_PATH"
printf 'Wrote manifest to %s\n' "$MANIFEST_PATH"
