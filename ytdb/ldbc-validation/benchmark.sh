#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

ensure_dirs
mkdir -p "${WORK_DIR}/results-remote"
"${SCRIPT_DIR}/load.sh"
compose run --rm --no-deps \
  -v "${SCRIPT_DIR}/driver-benchmark.properties:/app/driver.properties:ro" \
  -v "${WORK_DIR}/results-remote:/results" \
  driver
