#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

ensure_dirs
"${SCRIPT_DIR}/load.sh"
compose run --rm --no-deps -v "${SCRIPT_DIR}/driver-create.properties:/app/driver.properties:ro" driver
