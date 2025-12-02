#!/bin/bash

set -eu
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"${SCRIPT_DIR}/cleanup.sh"
"${SCRIPT_DIR}/load-data.sh"
