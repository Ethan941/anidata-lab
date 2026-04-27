#!/usr/bin/env bash
set -euo pipefail

# End-to-end check:
# 1) Trigger scraper DAG
# 2) Wait for completion
# 3) Optionally verify downstream DAG run
# 4) Check Elasticsearch document count
#
# Usage:
#   ./scripts/run_e2e_airflow_check.sh
#
# Optional env vars:
#   AIRFLOW_BASE_URL (default: http://localhost:8080)
#   AIRFLOW_USER (default: admin)
#   AIRFLOW_PASSWORD (default: admin)
#   SCRAPER_DAG_ID (default: anidata_scraper_pipeline)
#   DOWNSTREAM_DAG_ID (default: empty, optional)
#   ES_BASE_URL (default: http://localhost:9200)
#   ES_INDEX (default: anime)
#   POLL_SECONDS (default: 10)
#   MAX_POLLS (default: 60)

AIRFLOW_BASE_URL="${AIRFLOW_BASE_URL:-http://localhost:8080}"
AIRFLOW_USER="${AIRFLOW_USER:-admin}"
AIRFLOW_PASSWORD="${AIRFLOW_PASSWORD:-admin}"
SCRAPER_DAG_ID="${SCRAPER_DAG_ID:-anidata_scraper_pipeline}"
DOWNSTREAM_DAG_ID="${DOWNSTREAM_DAG_ID:-}"
ES_BASE_URL="${ES_BASE_URL:-http://localhost:9200}"
ES_INDEX="${ES_INDEX:-anime}"
POLL_SECONDS="${POLL_SECONDS:-10}"
MAX_POLLS="${MAX_POLLS:-60}"

auth_args=(-u "${AIRFLOW_USER}:${AIRFLOW_PASSWORD}")

echo "==> Trigger DAG: ${SCRAPER_DAG_ID}"
TRIGGER_RESPONSE="$(curl -sS "${auth_args[@]}" -X POST \
  "${AIRFLOW_BASE_URL}/api/v1/dags/${SCRAPER_DAG_ID}/dagRuns" \
  -H "Content-Type: application/json" \
  -d '{"conf":{"source":"e2e-script"}}')"

DAG_RUN_ID="$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["dag_run_id"])' <<< "${TRIGGER_RESPONSE}")"
echo "Triggered run id: ${DAG_RUN_ID}"

echo "==> Waiting for scraper DAG completion"
SCRAPER_STATE="unknown"
for ((i=1; i<=MAX_POLLS; i++)); do
  RUN_RESPONSE="$(curl -sS "${auth_args[@]}" \
    "${AIRFLOW_BASE_URL}/api/v1/dags/${SCRAPER_DAG_ID}/dagRuns/${DAG_RUN_ID}")"
  SCRAPER_STATE="$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read()).get("state","unknown"))' <<< "${RUN_RESPONSE}")"
  echo "Poll ${i}/${MAX_POLLS}: state=${SCRAPER_STATE}"

  if [[ "${SCRAPER_STATE}" == "success" ]]; then
    break
  fi
  if [[ "${SCRAPER_STATE}" == "failed" ]]; then
    echo "Scraper DAG failed."
    exit 1
  fi
  sleep "${POLL_SECONDS}"
done

if [[ "${SCRAPER_STATE}" != "success" ]]; then
  echo "Timed out waiting for scraper DAG success."
  exit 1
fi

if [[ -n "${DOWNSTREAM_DAG_ID}" ]]; then
  echo "==> Checking optional downstream DAG trigger: ${DOWNSTREAM_DAG_ID}"
  DOWNSTREAM_RESPONSE="$(curl -sS "${auth_args[@]}" \
    "${AIRFLOW_BASE_URL}/api/v1/dags/${DOWNSTREAM_DAG_ID}/dagRuns?limit=1&order_by=-start_date")"
  DOWNSTREAM_LATEST_STATE="$(python3 -c 'import json,sys; d=json.loads(sys.stdin.read()); runs=d.get("dag_runs",[]); print(runs[0].get("state","none") if runs else "none")' <<< "${DOWNSTREAM_RESPONSE}")"
  echo "Latest downstream DAG state: ${DOWNSTREAM_LATEST_STATE}"
fi

echo "==> Checking Elasticsearch index document count"
ES_COUNT_RESPONSE="$(curl -sS "${ES_BASE_URL}/${ES_INDEX}/_count")"
DOC_COUNT="$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read()).get("count",0))' <<< "${ES_COUNT_RESPONSE}")"
echo "Index ${ES_INDEX} document count: ${DOC_COUNT}"

if [[ "${DOC_COUNT}" -le 0 ]]; then
  echo "Elasticsearch count is 0, expected > 0."
  exit 1
fi

echo "==> E2E check completed successfully."
echo "Grafana validation is visual: open http://localhost:3000 and confirm dashboard refresh."
