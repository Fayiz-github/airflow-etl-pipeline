#!/usr/bin/env bash
# =============================================================
# setup.sh — Priority 7: Environment preparation script
#
# Run BEFORE the Python ETL pipeline starts.
# Handles:
#   1. Required env var validation
#   2. Disk space logging
#   3. Date-partitioned directory creation
#   4. Partial file cleanup from previous failed runs
#   5. Archiving raw files older than 7 days
#
# Usage:
#   bash setup.sh 2026-04-20
#   chmod +x setup.sh && ./setup.sh $(date +%F)
# =============================================================

set -euo pipefail  # exit on error, unset var, or pipe failure

# -------------------------------------------------------------
# 0. Argument / config
# -------------------------------------------------------------
RUN_DATE="${1:-$(date +%F)}"
BASE_PATH="${AIRFLOW_BASE_PATH:-/opt/airflow}"

YEAR=$(date -d "$RUN_DATE" +%Y 2>/dev/null || date -j -f "%Y-%m-%d" "$RUN_DATE" +%Y)
MONTH=$(date -d "$RUN_DATE" +%m 2>/dev/null || date -j -f "%Y-%m-%d" "$RUN_DATE" +%m)

LOG_FILE="${BASE_PATH}/logs/setup_${RUN_DATE}.log"
mkdir -p "$(dirname "$LOG_FILE")"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

log "========================================================"
log "  ETL Setup Script — RUN_DATE=${RUN_DATE}"
log "========================================================"

# -------------------------------------------------------------
# 1. Required environment variable check
# -------------------------------------------------------------
log "Checking required environment variables..."

REQUIRED_VARS=(
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
    "AIRFLOW__CORE__FERNET_KEY"
)

MISSING=0
for VAR in "${REQUIRED_VARS[@]}"; do
    if [ -z "${!VAR:-}" ]; then
        log "ERROR: Required env var '${VAR}' is not set."
        MISSING=1
    else
        log "  OK: ${VAR} is set."
    fi
done

if [ "$MISSING" -eq 1 ]; then
    log "FATAL: One or more required env vars missing. Exiting."
    exit 1
fi

# -------------------------------------------------------------
# 2. Disk space check
# -------------------------------------------------------------
log "Checking disk space..."
df -h "${BASE_PATH}" | tee -a "$LOG_FILE"
AVAILABLE_KB=$(df -k "${BASE_PATH}" | awk 'NR==2 {print $4}')
MIN_REQUIRED_KB=512000  # 500 MB minimum

if [ "$AVAILABLE_KB" -lt "$MIN_REQUIRED_KB" ]; then
    log "WARNING: Low disk space! Available: ${AVAILABLE_KB}KB, Required: ${MIN_REQUIRED_KB}KB"
fi

# -------------------------------------------------------------
# 3. Create date-partitioned directory structure
# -------------------------------------------------------------
log "Creating directory structure for ${YEAR}/${MONTH}..."

DIRS=(
    "${BASE_PATH}/data/raw/${YEAR}/${MONTH}"
    "${BASE_PATH}/data/processed/${YEAR}/${MONTH}"
    "${BASE_PATH}/data/rejected/${YEAR}/${MONTH}"
    "${BASE_PATH}/data/archive/${YEAR}/${MONTH}"
    "${BASE_PATH}/reports"
)

for DIR in "${DIRS[@]}"; do
    mkdir -p "$DIR"
    log "  Created: ${DIR}"
done

# -------------------------------------------------------------
# 4. Remove partial files from a previous failed run
#    (files with today's date that are 0 bytes or .tmp extension)
# -------------------------------------------------------------
log "Cleaning up partial files from previous failed runs..."

find "${BASE_PATH}/data/raw"       -name "*${RUN_DATE}*" -size 0 -delete 2>/dev/null && log "  Removed 0-byte raw files for ${RUN_DATE}" || true
find "${BASE_PATH}/data/processed" -name "*${RUN_DATE}*" -size 0 -delete 2>/dev/null && log "  Removed 0-byte processed files for ${RUN_DATE}" || true
find "${BASE_PATH}/data"           -name "*.tmp"         -delete 2>/dev/null && log "  Removed .tmp files" || true

# -------------------------------------------------------------
# 5. Archive raw files older than 7 days
# -------------------------------------------------------------
log "Archiving raw files older than 7 days..."

ARCHIVE_DIR="${BASE_PATH}/data/archive/${YEAR}/${MONTH}"
mkdir -p "$ARCHIVE_DIR"

find "${BASE_PATH}/data/raw" -maxdepth 3 -name "raw_*.csv" -mtime +7 | while read -r FILE; do
    DEST="${ARCHIVE_DIR}/$(basename "$FILE")"
    mv "$FILE" "$DEST"
    log "  Archived: ${FILE} → ${DEST}"
done

# -------------------------------------------------------------
# Done
# -------------------------------------------------------------
log "Setup complete. Pipeline is ready to run for ${RUN_DATE}."
log "========================================================"
exit 0
