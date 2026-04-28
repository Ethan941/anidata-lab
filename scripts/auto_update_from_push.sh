#!/usr/bin/env bash
set -euo pipefail

# Cron-friendly automation script:
# 1) Checks if remote branch has new commits
# 2) Fast-forward updates local repository
# 3) Redeploys Airflow stack from GHCR
# 4) Runs end-to-end Airflow verification
#
# Usage:
#   ./scripts/auto_update_from_push.sh
#
# Optional env vars:
#   TARGET_BRANCH (default: main)
#   REMOTE_NAME (default: origin)
#   LOCK_DIR (default: /tmp/anidata_auto_update.lock)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TARGET_BRANCH="${TARGET_BRANCH:-main}"
REMOTE_NAME="${REMOTE_NAME:-origin}"
LOCK_DIR="${LOCK_DIR:-/tmp/anidata_auto_update.lock}"

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

cleanup() {
  rm -rf "${LOCK_DIR}"
}

if ! mkdir "${LOCK_DIR}" 2>/dev/null; then
  log "Another run is already in progress, skipping."
  exit 0
fi
trap cleanup EXIT

cd "${REPO_ROOT}"

log "Checking working tree state"
if [[ -n "$(git status --porcelain)" ]]; then
  log "Working tree is not clean, skipping auto-update to avoid conflicts."
  exit 0
fi

log "Fetching latest refs from ${REMOTE_NAME}/${TARGET_BRANCH}"
git fetch "${REMOTE_NAME}" "${TARGET_BRANCH}"

LOCAL_SHA="$(git rev-parse HEAD)"
REMOTE_SHA="$(git rev-parse "${REMOTE_NAME}/${TARGET_BRANCH}")"

if [[ "${LOCAL_SHA}" == "${REMOTE_SHA}" ]]; then
  log "No new commit detected on ${REMOTE_NAME}/${TARGET_BRANCH}."
  exit 0
fi

log "New commit detected: ${LOCAL_SHA} -> ${REMOTE_SHA}"
log "Applying fast-forward update"
git pull --ff-only "${REMOTE_NAME}" "${TARGET_BRANCH}"

log "Deploying updated stack"
"${REPO_ROOT}/scripts/deploy_airflow_from_ghcr.sh"

log "Running end-to-end Airflow check"
"${REPO_ROOT}/scripts/run_e2e_airflow_check.sh"

log "Auto-update and verification completed successfully."
