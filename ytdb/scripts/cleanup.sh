#!/bin/bash

set -eu
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
YTDB_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$YTDB_DIR")"

cd "$YTDB_DIR"

. scripts/vars.sh

if [ "${YTDB_MODE}" = "embedded" ]; then
    echo "Cleaning up embedded database..."
    if [ -d "${YTDB_DATA_DIR}" ]; then
        rm -rf "${YTDB_DATA_DIR}"
        echo "Removed data directory: ${YTDB_DATA_DIR}"
    else
        echo "Data directory does not exist: ${YTDB_DATA_DIR}"
    fi
elif [ "${YTDB_MODE}" = "remote" ]; then
    echo "Cleaning up remote database..."

    # Export for Maven exec to inherit
    export YTDB_MODE
    export YTDB_SERVER_HOST
    export YTDB_SERVER_PORT
    export YTDB_SERVER_USER
    export YTDB_SERVER_PASSWORD
    export YTDB_DATABASE_NAME

    cd "$PROJECT_ROOT"
    mvn clean install -DskipTests -q

    cd "$YTDB_DIR"
    mvn exec:java \
        -Dexec.mainClass="com.youtrackdb.ldbc.ytdb.cleanup.Main" \
        -Dorg.slf4j.simpleLogger.defaultLogLevel=info
else
    echo "Unknown YTDB_MODE: ${YTDB_MODE}"
    exit 1
fi
