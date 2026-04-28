#!/usr/bin/env bash
set -euo pipefail

# Cron-friendly automation script:
# 1) Checks GitHub API for latest commit on target branch
# 2) If new commit exists, fast-forward updates local repository
# 3) Redeploys Airflow stack from GHCR
# 4) Runs end-to-end Airflow verification
#
# Usage:
#   ./scripts/auto_update_from_push.sh
#
# Optional env vars:
#   TARGET_BRANCH (default: main)
#   GITHUB_OWNER (default: RomainR99)
#   GITHUB_REPO (default: anidata-lab)
#   GITHUB_TOKEN (default: empty, recommended to avoid rate limits)
#   WORKFLOW_FILE (default: ci-cd.yml)
#   REMOTE_NAME (default: origin)
#   LOCK_DIR (default: /tmp/anidata_auto_update.lock)
#   STATE_DIR (default: .state)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TARGET_BRANCH="${TARGET_BRANCH:-main}"
GITHUB_OWNER="${GITHUB_OWNER:-RomainR99}"
GITHUB_REPO="${GITHUB_REPO:-anidata-lab}"
GITHUB_TOKEN="${GITHUB_TOKEN:-}"
WORKFLOW_FILE="${WORKFLOW_FILE:-ci-cd.yml}"
REMOTE_NAME="${REMOTE_NAME:-origin}"
LOCK_DIR="${LOCK_DIR:-/tmp/anidata_auto_update.lock}"
STATE_DIR="${STATE_DIR:-.state}"
STATE_FILE="${REPO_ROOT}/${STATE_DIR}/last_seen_${TARGET_BRANCH}_sha.txt"

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

mkdir -p "${REPO_ROOT}/${STATE_DIR}"

API_URL="https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/commits/${TARGET_BRANCH}"
log "Checking latest commit on GitHub API: ${GITHUB_OWNER}/${GITHUB_REPO}@${TARGET_BRANCH}"

if [[ -n "${GITHUB_TOKEN}" ]]; then
  API_RESPONSE="$(curl -fsSL \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer ${GITHUB_TOKEN}" \
    "${API_URL}")"
else
  API_RESPONSE="$(curl -fsSL \
    -H "Accept: application/vnd.github+json" \
    "${API_URL}")"
fi

REMOTE_SHA="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["sha"])' <<< "${API_RESPONSE}")"
LAST_SEEN_SHA=""
if [[ -f "${STATE_FILE}" ]]; then
  LAST_SEEN_SHA="$(<"${STATE_FILE}")"
fi

if [[ "${LAST_SEEN_SHA}" == "${REMOTE_SHA}" ]]; then
  log "No new green commit to process on GitHub (${TARGET_BRANCH})."
  exit 0
fi

log "New commit detected on GitHub: ${LAST_SEEN_SHA:-<none>} -> ${REMOTE_SHA}"

RUNS_URL="https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/actions/workflows/${WORKFLOW_FILE}/runs?head_sha=${REMOTE_SHA}&branch=${TARGET_BRANCH}&event=push&per_page=1"
log "Checking CI workflow status: ${WORKFLOW_FILE} for ${REMOTE_SHA}"

if [[ -n "${GITHUB_TOKEN}" ]]; then
  RUNS_RESPONSE="$(curl -fsSL \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer ${GITHUB_TOKEN}" \
    "${RUNS_URL}")"
else
  RUNS_RESPONSE="$(curl -fsSL \
    -H "Accept: application/vnd.github+json" \
    "${RUNS_URL}")"
fi

RUN_STATUS="$(python3 -c 'import json,sys; d=json.load(sys.stdin); runs=d.get("workflow_runs",[]); print(runs[0].get("status","none") if runs else "none")' <<< "${RUNS_RESPONSE}")"
RUN_CONCLUSION="$(python3 -c 'import json,sys; d=json.load(sys.stdin); runs=d.get("workflow_runs",[]); print((runs[0].get("conclusion") or "none") if runs else "none")' <<< "${RUNS_RESPONSE}")"

if [[ "${RUN_STATUS}" != "completed" ]]; then
  log "CI not finished yet for ${REMOTE_SHA} (status=${RUN_STATUS}). Waiting next cron run."
  exit 0
fi

if [[ "${RUN_CONCLUSION}" != "success" ]]; then
  log "CI completed but not green for ${REMOTE_SHA} (conclusion=${RUN_CONCLUSION}). Skipping deploy."
  exit 0
fi

log "CI is green for ${REMOTE_SHA}. Starting deploy pipeline."

log "Checking working tree state"
if [[ -n "$(git status --porcelain)" ]]; then
  log "Working tree is not clean, skipping auto-update to avoid conflicts."
  exit 0
fi

log "Fetching latest refs from ${REMOTE_NAME}/${TARGET_BRANCH}"
git fetch "${REMOTE_NAME}" "${TARGET_BRANCH}"

log "Applying fast-forward update"
git pull --ff-only "${REMOTE_NAME}" "${TARGET_BRANCH}"

log "Deploying updated stack"
"${REPO_ROOT}/scripts/deploy_airflow_from_ghcr.sh"

log "Running end-to-end Airflow check"
"${REPO_ROOT}/scripts/run_e2e_airflow_check.sh"

printf '%s\n' "${REMOTE_SHA}" > "${STATE_FILE}"
log "Auto-update and verification completed successfully."
