#!/usr/bin/env bash
set -euo pipefail

DB_URL="${DATABASE_URL:-postgresql://shitty_swarm_manager:shitty_swarm_manager@localhost:5437/shitty_swarm_manager_db}"

psql "${DB_URL}" -c "SELECT * FROM v_feedback_required ORDER BY completed_at DESC NULLS LAST;"
