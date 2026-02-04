#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

ensure_dirs

rm -rf "${WORK_DIR}/data/social_network" "${WORK_DIR}/data/substitution_parameters"

compose run --rm datagen
