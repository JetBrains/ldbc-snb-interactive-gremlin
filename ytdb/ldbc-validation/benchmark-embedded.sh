#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export COMPOSE_FILE="${SCRIPT_DIR}/docker-compose-embedded.yml"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

ensure_dirs

if [ ! -d "${WORK_DIR}/data/social_network" ]; then
  echo "Missing dataset at ${WORK_DIR}/data/social_network" >&2
  echo "Run: ${SCRIPT_DIR}/datagen.sh" >&2
  exit 1
fi

compose run --rm loader
compose run --rm -v "${SCRIPT_DIR}/driver-benchmark-embedded.properties:/app/driver.properties:ro" driver
