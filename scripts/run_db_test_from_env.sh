#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_URL="${SWARM_TEST_DATABASE_URL:-${DATABASE_URL:-}}"

if [[ -z "${DB_URL}" && -f "${ROOT_DIR}/.env" ]]; then
	DB_URL="$(
		python - "${ROOT_DIR}/.env" <<'PY'
import pathlib
import sys

env_path = pathlib.Path(sys.argv[1])
for line in env_path.read_text(encoding="utf-8").splitlines():
    stripped = line.strip()
    if not stripped or stripped.startswith("#") or "=" not in stripped:
        continue
    key, value = stripped.split("=", 1)
    if key.strip() in {"SWARM_TEST_DATABASE_URL", "DATABASE_URL"}:
        print(value.strip().strip('"').strip("'"))
        break
PY
	)"
fi

if [[ -z "${DB_URL}" ]]; then
	printf "Skipping db-test: SWARM_TEST_DATABASE_URL or DATABASE_URL not set\n"
	exit 0
fi

if ! PGCONNECT_TIMEOUT=3 psql "${DB_URL}" -c "SELECT 1" >/dev/null 2>&1; then
	printf "db-test preflight failed: cannot reach database from configured URL\n"
	exit 3
fi

DATABASE_URL="${DB_URL}" \
	SWARM_TEST_DATABASE_URL="${DB_URL}" \
	cargo test db::write_ops::tests:: -- --ignored --nocapture --test-threads=1
