#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
YTDB_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$YTDB_DIR")"

# Mode: embedded or remote
YTDB_MODE=${YTDB_MODE:-remote}

# Embedded mode configuration
YTDB_DATA_DIR=${YTDB_DATA_DIR:-"${YTDB_DIR}/scratch/data"}

# Remote mode configuration
YTDB_SERVER_HOST=${YTDB_SERVER_HOST:-localhost}
YTDB_SERVER_PORT=${YTDB_SERVER_PORT:-8182}
YTDB_SERVER_USER=${YTDB_SERVER_USER:-root}
YTDB_SERVER_PASSWORD=${YTDB_SERVER_PASSWORD:-root}

# Database configuration (used by both modes)
YTDB_DATABASE_NAME=${YTDB_DATABASE_NAME:-ldbc_snb}
YTDB_DATABASE_USER=${YTDB_DATABASE_USER:-admin}
YTDB_DATABASE_PASSWORD=${YTDB_DATABASE_PASSWORD:-admin}
YTDB_TEST_DATA_DIR=${YTDB_TEST_DATA_DIR:-"${PROJECT_ROOT}/test-data/runtime/social-network/sf0.1"}
