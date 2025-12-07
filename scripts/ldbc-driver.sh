#!/bin/bash
# Run LDBC SNB Interactive driver using properties files.
# Loads defaults from repo root, then vendor overrides (if exist).
set -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RUNNER_JAR="${REPO_ROOT}/runner/target/runner-1.0-SNAPSHOT.jar"
DEFAULT_PROPS="${REPO_ROOT}/runner/ldbc-driver.properties"

# Validate required files
[[ -f "$RUNNER_JAR" ]] || { echo "Error: Runner JAR not found. Run 'mvn package' first."; exit 1; }
[[ -f "$DEFAULT_PROPS" ]] || { echo "Error: Default properties not found: $DEFAULT_PROPS"; exit 1; }

# Read vendor from default properties
VENDOR=$(grep -E '^tinkerpop\.vendor=' "$DEFAULT_PROPS" | cut -d= -f2 | tr -d '[:space:]')
[[ -n "$VENDOR" ]] || { echo "Error: tinkerpop.vendor not set in $DEFAULT_PROPS"; exit 1; }

VENDOR_DIR="${REPO_ROOT}/${VENDOR}"
VENDOR_PROPS="${VENDOR_DIR}/ldbc-driver.properties"

# Check if vendor directory exists
if [[ ! -d "$VENDOR_DIR" ]]; then
    echo "Warning: Vendor directory not found: $VENDOR_DIR"
fi

# Build -P argument: vendor props first (higher priority), then defaults
if [[ -f "$VENDOR_PROPS" ]]; then
    PROPS_ARG="${VENDOR_PROPS}|${DEFAULT_PROPS}"
    echo "Properties: ${VENDOR_PROPS} (overrides) + defaults"
else
    PROPS_ARG="${DEFAULT_PROPS}"
    echo "Properties: defaults only"
fi

# Change directory to vendor module if it exists
if [[ -d "$VENDOR_DIR" ]]; then
    cd "$VENDOR_DIR"
fi

# Java 9+ module access for LDBC driver's SBE library
exec java --add-exports java.base/sun.nio.ch=ALL-UNNAMED \
    -cp "$RUNNER_JAR" \
    org.ldbcouncil.snb.driver.Client \
    -P "$PROPS_ARG"
