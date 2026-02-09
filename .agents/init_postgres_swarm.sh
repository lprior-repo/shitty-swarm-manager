#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME="${SWARM_PG_CONTAINER:-shitty-swarm-manager-db}"
PG_PORT="${SWARM_PG_PORT:-5437}"
PG_USER="${SWARM_PG_USER:-shitty_swarm_manager}"
PG_PASSWORD="${SWARM_PG_PASSWORD:-shitty_swarm_manager}"
PG_DB="${SWARM_PG_DB:-shitty_swarm_manager_db}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SCHEMA_PATH="${SWARM_SCHEMA_PATH:-${REPO_ROOT}/crates/swarm-coordinator/schema.sql}"

echo "[swarm-init] ensuring container ${CONTAINER_NAME}"
if ! docker ps -a --format '{{.Names}}' | grep -qx "${CONTAINER_NAME}"; then
	docker run -d \
		--name "${CONTAINER_NAME}" \
		-p "${PG_PORT}:5432" \
		-e POSTGRES_USER="${PG_USER}" \
		-e POSTGRES_PASSWORD="${PG_PASSWORD}" \
		-e POSTGRES_DB="${PG_DB}" \
		--restart unless-stopped \
		postgres:16 >/dev/null
else
	docker start "${CONTAINER_NAME}" >/dev/null || true
fi

echo "[swarm-init] waiting for postgres readiness"
until docker exec "${CONTAINER_NAME}" pg_isready -U "${PG_USER}" -d "${PG_DB}" >/dev/null 2>&1; do
	sleep 1
done

echo "[swarm-init] applying schema ${SCHEMA_PATH}"
docker exec -i "${CONTAINER_NAME}" psql -U "${PG_USER}" -d "${PG_DB}" <"${SCHEMA_PATH}" >/dev/null

echo "[swarm-init] seeding 12 idle agents"
docker exec -i "${CONTAINER_NAME}" psql -U "${PG_USER}" -d "${PG_DB}" -c \
	"INSERT INTO agent_state (agent_id, status) SELECT g, 'idle' FROM generate_series(1, 12) AS g ON CONFLICT (agent_id) DO NOTHING;" >/dev/null

echo
echo "[swarm-init] ready"
echo "container: ${CONTAINER_NAME}"
echo "database_url: postgresql://${PG_USER}:${PG_PASSWORD}@localhost:${PG_PORT}/${PG_DB}"
