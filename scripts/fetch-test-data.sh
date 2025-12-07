#!/usr/bin/env bash

set -euo pipefail

# Default values
SCALE_FACTOR="0.1"
SERIALIZER="csv_composite-longdateformatter"
UPDATE_PARTITIONS="1"

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
TEST_DATA_DIR="${REPO_ROOT}/test-data/runtime"
TEMP_DIR="${TEST_DATA_DIR}/tmp"

# Colors for output (optional, but nice)
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# Function to print usage
print_usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

Download and unpack LDBC SNB Interactive test data.

OPTIONS:
  -s, --scale-factor FACTOR       Scale factor (default: 0.1)
  -f, --serializer SERIALIZER     Dataset format (default: csv_composite-longdateformatter)
  -p, --update-partitions COUNT   Number of update stream partitions (default: 1)
  -h, --help                      Show this help message and exit

EXAMPLES:
  $0                                 # Download SF 0.1 in CSV Composite format
  $0 --scale-factor 1                # Download SF 1.0
  $0 -s 3 -f csv_composite           # Download SF 3.0 in CSV Composite format
  $0 -s 0.1 -p 4                     # Download SF 0.1 with 4-partition update streams
EOF
}

# Function to print error and exit
error_exit() {
    echo -e "${RED}Error: $1${NC}" >&2
    print_usage
    exit 1
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -s|--scale-factor)
            if [[ $# -lt 2 ]]; then
                error_exit "Missing value for $1"
            fi
            SCALE_FACTOR="$2"
            shift 2
            ;;
        -f|--serializer)
            if [[ $# -lt 2 ]]; then
                error_exit "Missing value for $1"
            fi
            SERIALIZER="$2"
            shift 2
            ;;
        -p|--update-partitions)
            if [[ $# -lt 2 ]]; then
                error_exit "Missing value for $1"
            fi
            UPDATE_PARTITIONS="$2"
            shift 2
            ;;
        -h|--help)
            print_usage
            exit 0
            ;;
        *)
            error_exit "Unknown option: $1"
            ;;
    esac
done

# Create necessary directories
mkdir -p "$TEMP_DIR"
TARGET_DIR="${TEST_DATA_DIR}/social-network/sf${SCALE_FACTOR}"
mkdir -p "$TARGET_DIR"

echo "Downloading LDBC SNB Interactive test data..."
echo "  Scale Factor: $SCALE_FACTOR"
echo "  Serializer: $SERIALIZER"
echo "  Update Partitions: $UPDATE_PARTITIONS"
echo "  Target Directory: $TARGET_DIR"
echo ""

# Make the ldbc script executable (in case it isn't)
chmod +x "${SCRIPT_DIR}/ldbc-download-data-set.sh"

# Construct URLs and download main dataset
echo "Downloading main dataset..."
SERIALIZER_DIR="social_network-${SERIALIZER}"
ARCHIVE_NAME="${SERIALIZER_DIR}-sf${SCALE_FACTOR}.tar.zst"
DOWNLOAD_URL="https://repository.surfsara.nl/datasets/cwi/snb/files/${SERIALIZER_DIR}/${ARCHIVE_NAME}"

"${SCRIPT_DIR}/ldbc-download-data-set.sh" "${DOWNLOAD_URL}"

# Move archive to temp directory and unpack
mv "${ARCHIVE_NAME}" "${TEMP_DIR}/"
echo "Unpacking main dataset..."
cd "${TEMP_DIR}"
tar -xvf "${ARCHIVE_NAME}"
cd - > /dev/null

# Move unpacked data to target directory
# The tar archive extracts to a directory named after the archive (without .tar.zst)
EXTRACTED_DIR="${TEMP_DIR}/${SERIALIZER_DIR}-sf${SCALE_FACTOR}"
if [[ -d "${EXTRACTED_DIR}" ]]; then
    cp -r "${EXTRACTED_DIR}"/* "${TARGET_DIR}/"
    echo "Data moved to ${TARGET_DIR}"
elif [[ -d "${TEMP_DIR}/social_network" ]]; then
    # Fallback for different archive structures
    cp -r "${TEMP_DIR}/social_network"/* "${TARGET_DIR}/"
    echo "Data moved to ${TARGET_DIR}"
else
    # Fallback: assume files are in the temp directory root
    find "${TEMP_DIR}" -maxdepth 1 -type f ! -name "*.tar.zst" -exec cp {} "${TARGET_DIR}/" \;
fi

## Download and unpack substitution parameters
echo ""
echo "Downloading substitution parameters..."
SUBST_ARCHIVE_NAME="substitution_parameters-sf${SCALE_FACTOR}.tar.zst"
SUBST_DOWNLOAD_URL="https://repository.surfsara.nl/datasets/cwi/snb/files/substitution_parameters/${SUBST_ARCHIVE_NAME}"

"${SCRIPT_DIR}/ldbc-download-data-set.sh" "${SUBST_DOWNLOAD_URL}"

# Move and unpack substitution parameters
mv "${SUBST_ARCHIVE_NAME}" "${TEMP_DIR}/"
echo "Unpacking substitution parameters..."
cd "${TEMP_DIR}"
tar -xvf "${SUBST_ARCHIVE_NAME}"
cd - > /dev/null

# Move substitution parameters to target directory
SUBST_TARGET_DIR="${TARGET_DIR}/substitution_parameters"
mkdir -p "$SUBST_TARGET_DIR"
if [[ -d "${TEMP_DIR}/substitution_parameters" ]]; then
    cp -r "${TEMP_DIR}/substitution_parameters"/* "${SUBST_TARGET_DIR}/"
fi

# Download and unpack update streams
echo ""
echo "Downloading update streams..."
UPDATE_STREAMS_ARCHIVE_NAME="social_network-sf${SCALE_FACTOR}-numpart-${UPDATE_PARTITIONS}.tar.zst"
UPDATE_STREAMS_DOWNLOAD_URL="https://repository.surfsara.nl/datasets/cwi/ldbc-snb-interactive-v1-datagen-v100/files/update-streams/${UPDATE_STREAMS_ARCHIVE_NAME}"

"${SCRIPT_DIR}/ldbc-download-data-set.sh" "${UPDATE_STREAMS_DOWNLOAD_URL}"

# Move and unpack update streams
mv "${UPDATE_STREAMS_ARCHIVE_NAME}" "${TEMP_DIR}/"
echo "Unpacking update streams..."
cd "${TEMP_DIR}"
tar -xvf "${UPDATE_STREAMS_ARCHIVE_NAME}"
cd - > /dev/null

# Move update streams to target directory
UPDATE_STREAMS_TARGET_DIR="${TARGET_DIR}/update_streams"
mkdir -p "$UPDATE_STREAMS_TARGET_DIR"
UPDATE_STREAMS_EXTRACTED_DIR="${TEMP_DIR}/social_network-sf${SCALE_FACTOR}-numpart-${UPDATE_PARTITIONS}"
if [[ -d "${UPDATE_STREAMS_EXTRACTED_DIR}" ]]; then
    cp -r "${UPDATE_STREAMS_EXTRACTED_DIR}"/* "${UPDATE_STREAMS_TARGET_DIR}/"
fi

# Clean up temporary directory
echo ""
echo "Cleaning up temporary files..."
rm -rf "${TEMP_DIR}"

echo -e "${GREEN}Successfully downloaded and unpacked test data${NC}"
echo "Data location: $TARGET_DIR"
