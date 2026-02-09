#!/usr/bin/env bash
set -euo pipefail

DB_URL="${DATABASE_URL:-postgresql://shitty_swarm_manager:shitty_swarm_manager@localhost:5437/shitty_swarm_manager_db}"

psql "${DB_URL}" -c "SELECT id, from_agent_id, to_agent_id, bead_id, message_type, subject, created_at FROM v_unread_messages ORDER BY created_at DESC;"
