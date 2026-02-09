#!/usr/bin/env bash
set -euo pipefail

DB_URL="${DATABASE_URL:-postgresql://shitty_swarm_manager:shitty_swarm_manager@localhost:5437/shitty_swarm_manager_db}"

watch -n 2 "psql '${DB_URL}' -c \"SELECT agent_id, bead_id, current_stage, status, implementation_attempt, last_update FROM v_active_agents ORDER BY last_update DESC;\""
