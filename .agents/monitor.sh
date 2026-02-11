#!/usr/bin/env bash

set -euo pipefail

HOST="${SWARM_DB_HOST:-localhost}"
PORT="${SWARM_DB_PORT:-5432}"
DB="${SWARM_DB_NAME:-swarm_db}"
USER_NAME="${SWARM_DB_USER:-oya}"

watch -n 2 "psql -h ${HOST} -p ${PORT} -U ${USER_NAME} -d ${DB} -c \"SELECT * FROM v_active_agents ORDER BY agent_id;\""
