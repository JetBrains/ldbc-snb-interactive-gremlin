#!/bin/bash

set -eu
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
YTDB_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$YTDB_DIR")"

cd "$YTDB_DIR"

# Load environment variables
. scripts/vars.sh

# Export for Maven exec to inherit
export YTDB_MODE
export YTDB_DATA_DIR
export YTDB_SERVER_HOST
export YTDB_SERVER_PORT
export YTDB_SERVER_USER
export YTDB_SERVER_PASSWORD
export YTDB_DATABASE_NAME
export YTDB_DATABASE_USER
export YTDB_DATABASE_PASSWORD
export YTDB_TEST_DATA_DIR

cd "$PROJECT_ROOT"
mvn clean install -DskipTests -q

# Run from ytdb directory so relative paths work
cd "$YTDB_DIR"
mvn exec:java \
    -Dexec.mainClass="com.youtrackdb.ldbc.ytdb.loader.Main" \
    -Dorg.slf4j.simpleLogger.defaultLogLevel=info
