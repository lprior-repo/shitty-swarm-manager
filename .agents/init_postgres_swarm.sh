#!/usr/bin/env bash

set -euo pipefail

HOST="${SWARM_DB_HOST:-localhost}"
PORT="${SWARM_DB_PORT:-5432}"
DB="${SWARM_DB_NAME:-swarm_db}"
USER_NAME="${SWARM_DB_USER:-oya}"
CONTAINER="${SWARM_DB_CONTAINER:-oya-swarm-db}"
SCHEMA_PATH="${SWARM_SCHEMA_PATH:-crates/swarm-coordinator/schema.sql}"
SEED_AGENTS="${SWARM_SEED_AGENTS:-12}"

usage() {
	cat <<'EOF'
Usage: .agents/init_postgres_swarm.sh [--docker] [--host H] [--port P] [--db NAME] [--user USER] [--schema PATH] [--seed-agents N]

Defaults:
  host=localhost
  port=5432
  db=swarm_db
  user=oya
  schema=crates/swarm-coordinator/schema.sql
  seed-agents=12

Examples:
  .agents/init_postgres_swarm.sh --docker
  SWARM_DB_PORT=5437 SWARM_DB_USER=shitty_swarm_manager SWARM_DB_NAME=shitty_swarm_manager_db .agents/init_postgres_swarm.sh
EOF
}

USE_DOCKER=0
while [[ $# -gt 0 ]]; do
	case "$1" in
	--docker)
		USE_DOCKER=1
		shift
		;;
	--host)
		HOST="$2"
		shift 2
		;;
	--port)
		PORT="$2"
		shift 2
		;;
	--db)
		DB="$2"
		shift 2
		;;
	--user)
		USER_NAME="$2"
		shift 2
		;;
	--schema)
		SCHEMA_PATH="$2"
		shift 2
		;;
	--seed-agents)
		SEED_AGENTS="$2"
		shift 2
		;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		echo "Unknown argument: $1" >&2
		usage
		exit 1
		;;
	esac
done

if [[ ! -f "$SCHEMA_PATH" ]]; then
	echo "Schema not found: $SCHEMA_PATH" >&2
	exit 1
fi

if [[ "$USE_DOCKER" -eq 1 ]]; then
	if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
		if docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER"; then
			docker start "$CONTAINER" >/dev/null
		else
			docker run -d \
				--name "$CONTAINER" \
				-p "${PORT}:5432" \
				-e "POSTGRES_USER=${USER_NAME}" \
				-e POSTGRES_HOST_AUTH_METHOD=trust \
				-e "POSTGRES_DB=${DB}" \
				postgres:16 >/dev/null
		fi
	fi
fi

until pg_isready -h "$HOST" -p "$PORT" -U "$USER_NAME" >/dev/null 2>&1; do
	echo "Waiting for PostgreSQL at ${HOST}:${PORT}..."
	sleep 1
done

if ! psql -h "$HOST" -p "$PORT" -U "$USER_NAME" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='${DB}'" | grep -q 1; then
	psql -h "$HOST" -p "$PORT" -U "$USER_NAME" -d postgres -c "CREATE DATABASE \"${DB}\";" >/dev/null
fi

psql -h "$HOST" -p "$PORT" -U "$USER_NAME" -d "$DB" -v ON_ERROR_STOP=1 -f "$SCHEMA_PATH" >/dev/null

psql -h "$HOST" -p "$PORT" -U "$USER_NAME" -d "$DB" -v ON_ERROR_STOP=1 <<SQL >/dev/null
INSERT INTO agent_state (agent_id, status, current_stage, implementation_attempt)
SELECT id, 'idle', NULL, 0
FROM generate_series(1, ${SEED_AGENTS}) AS id
ON CONFLICT (agent_id) DO UPDATE
SET status = EXCLUDED.status,
    bead_id = NULL,
    current_stage = NULL,
    stage_started_at = NULL,
    implementation_attempt = 0,
    feedback = NULL,
    last_update = NOW();
SQL

cat <<EOF
Swarm PostgreSQL initialized.

Connection:
  host: $HOST
  port: $PORT
  db:   $DB
  user: $USER_NAME

Commands:
  psql -h $HOST -p $PORT -U $USER_NAME -d $DB
  ./target/release/swarm doctor
  ./target/release/swarm status
EOF
