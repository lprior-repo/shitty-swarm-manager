#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
POSTGRES_IMAGE="${SWARM_EPHEMERAL_POSTGRES_IMAGE:-postgres:16}"
FLYWAY_IMAGE="${SWARM_FLYWAY_IMAGE:-flyway/flyway:10-alpine}"
DB_NAME="${SWARM_EPHEMERAL_DB_NAME:-shitty_swarm_manager_db}"
DB_USER="${SWARM_EPHEMERAL_DB_USER:-shitty_swarm_manager}"
CONTAINER_NAME="${SWARM_EPHEMERAL_DB_CONTAINER:-swarm-db-test-$(date +%s)-$RANDOM}"
READINESS_TIMEOUT_SECONDS="${SWARM_EPHEMERAL_DB_READY_TIMEOUT_SECONDS:-60}"

pick_port() {
	local candidate
	while true; do
		candidate="$(((RANDOM % 10000) + 20000))"
		if ! ss -ltn "( sport = :${candidate} )" | grep -q "LISTEN"; then
			printf '%s\n' "$candidate"
			return
		fi
	done
}

DB_PORT="${SWARM_EPHEMERAL_DB_PORT:-$(pick_port)}"
DB_URL="postgresql://${DB_USER}@127.0.0.1:${DB_PORT}/${DB_NAME}"

MIGRATIONS_DIR="$(mktemp -d)"

cleanup() {
	docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
	rm -rf "${MIGRATIONS_DIR}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

on_error() {
	local exit_code="$1"
	echo "[db-test] failure detected (exit ${exit_code})"
	if docker ps -a --format '{{.Names}}' | grep -qx "${CONTAINER_NAME}"; then
		echo "[db-test] container logs:"
		docker logs "${CONTAINER_NAME}" | tail -n 80 || true
	fi
	exit "${exit_code}"
}
trap 'on_error $?' ERR

if [[ ! -f "${ROOT_DIR}/crates/swarm-coordinator/schema.sql" ]]; then
	echo "[db-test] missing schema file: ${ROOT_DIR}/crates/swarm-coordinator/schema.sql"
	exit 2
fi

if [[ ! -f "${ROOT_DIR}/db/flyway/R__seed.sql" ]]; then
	echo "[db-test] missing seed file: ${ROOT_DIR}/db/flyway/R__seed.sql"
	exit 2
fi

echo "[db-test] starting ephemeral postgres ${CONTAINER_NAME} on port ${DB_PORT}"
docker run -d \
	--name "${CONTAINER_NAME}" \
	-e "POSTGRES_DB=${DB_NAME}" \
	-e "POSTGRES_USER=${DB_USER}" \
	-e POSTGRES_HOST_AUTH_METHOD=trust \
	-p "${DB_PORT}:5432" \
	"${POSTGRES_IMAGE}" >/dev/null

echo "[db-test] waiting for postgres readiness"
start_time="$(date +%s)"
until pg_isready -h 127.0.0.1 -p "${DB_PORT}" -U "${DB_USER}" >/dev/null 2>&1; do
	current_time="$(date +%s)"
	if ((current_time - start_time >= READINESS_TIMEOUT_SECONDS)); then
		echo "[db-test] postgres readiness timed out after ${READINESS_TIMEOUT_SECONDS}s"
		exit 3
	fi
	sleep 1
done

cp "${ROOT_DIR}/crates/swarm-coordinator/schema.sql" "${MIGRATIONS_DIR}/V1__schema.sql"
cp "${ROOT_DIR}/db/flyway/R__seed.sql" "${MIGRATIONS_DIR}/R__seed.sql"

echo "[db-test] applying schema and seeds with flyway"
docker run --rm \
	--network host \
	-v "${MIGRATIONS_DIR}:/flyway/sql" \
	"${FLYWAY_IMAGE}" \
	-url="jdbc:postgresql://127.0.0.1:${DB_PORT}/${DB_NAME}" \
	-user="${DB_USER}" \
	-connectRetries=20 \
	-baselineOnMigrate=true \
	migrate >/dev/null

echo "[db-test] running ignored DB integration tests"
DATABASE_URL="${DB_URL}" \
	SWARM_TEST_DATABASE_URL="${DB_URL}" \
	moon run :test -- db::write_ops::tests:: -- --ignored --nocapture --test-threads=1

echo "[db-test] complete"
