#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
YTDB_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$YTDB_DIR")"

YTDB_DATA_DIR=${YTDB_DATA_DIR:-"${YTDB_DIR}/scratch/data"}
YTDB_DATABASE_NAME=${YTDB_DATABASE_NAME:-ldbc_snb}
YTDB_USERNAME=${YTDB_USERNAME:-admin}
YTDB_PASSWORD=${YTDB_PASSWORD:-admin}
YTDB_TEST_DATA_DIR=${YTDB_TEST_DATA_DIR:-"${PROJECT_ROOT}/test-data/runtime/social-network/sf0.1"}
