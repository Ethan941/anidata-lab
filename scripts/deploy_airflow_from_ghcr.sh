#!/usr/bin/env bash
set -euo pipefail

# Deploy the latest GHCR Airflow image to local docker-compose services.
# Usage:
#   ./scripts/deploy_airflow_from_ghcr.sh
#
# Optional env vars:
#   COMPOSE_FILE_PATH (default: docker-compose.yml)
#   ENV_FILE_PATH (default: .env)
#   AIRFLOW_IMAGE (default: ghcr.io/romainr99/anidata-lab-airflow:latest)

COMPOSE_FILE_PATH="${COMPOSE_FILE_PATH:-docker-compose.yml}"
ENV_FILE_PATH="${ENV_FILE_PATH:-.env}"
AIRFLOW_IMAGE="${AIRFLOW_IMAGE:-ghcr.io/romainr99/anidata-lab-airflow:latest}"

echo "==> Pull image from GHCR: ${AIRFLOW_IMAGE}"
docker pull "${AIRFLOW_IMAGE}"

echo "==> Restart Airflow services with docker compose"
docker compose -f "${COMPOSE_FILE_PATH}" --env-file "${ENV_FILE_PATH}" down
docker compose -f "${COMPOSE_FILE_PATH}" --env-file "${ENV_FILE_PATH}" up -d postgres airflow-init airflow-webserver airflow-scheduler

echo "==> Current service status"
docker compose -f "${COMPOSE_FILE_PATH}" --env-file "${ENV_FILE_PATH}" ps

echo "==> Waiting for Airflow webserver health"
for i in {1..30}; do
  status="$(docker compose -f "${COMPOSE_FILE_PATH}" --env-file "${ENV_FILE_PATH}" ps airflow-webserver --format json 2>/dev/null || true)"
  if [[ "${status}" == *"healthy"* ]]; then
    echo "Airflow webserver is healthy."
    exit 0
  fi
  sleep 5
done

echo "Airflow webserver did not become healthy within timeout."
exit 1
