#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

ensure_dirs

if [ ! -d "${WORK_DIR}/data/social_network" ]; then
  echo "Missing dataset at ${WORK_DIR}/data/social_network" >&2
  echo "Run: ${SCRIPT_DIR}/datagen.sh" >&2
  exit 1
fi

ensure_ytdb
compose run --rm --no-deps loader
