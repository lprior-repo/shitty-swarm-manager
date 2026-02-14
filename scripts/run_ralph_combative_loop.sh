#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
PROMPT_FILE="${RALPH_PROMPT_FILE:-${ROOT_DIR}/scripts/ralph_combative_loop_prompt.md}"

if ! command -v ralph >/dev/null 2>&1; then
	printf 'Error: ralph is not installed or not in PATH.\n' >&2
	exit 127
fi

if [[ ! -f "${PROMPT_FILE}" ]]; then
	printf 'Error: Ralph prompt file not found: %s\n' "${PROMPT_FILE}" >&2
	exit 1
fi

RALPH_AGENT_VALUE="${RALPH_AGENT:-opencode}"
RALPH_MODEL_VALUE="${RALPH_MODEL:-zai-coding-plan/glm-5}"
RALPH_MIN_ITERATIONS_VALUE="${RALPH_MIN_ITERATIONS:-30}"
RALPH_MAX_ITERATIONS_VALUE="${RALPH_MAX_ITERATIONS:-200}"
RALPH_COMPLETION_PROMISE_VALUE="${RALPH_COMPLETION_PROMISE:-COMBATIVE_LOOP_COMPLETE}"
RALPH_TASK_PROMISE_VALUE="${RALPH_TASK_PROMISE:-READY_FOR_NEXT_TASK}"
RALPH_ALLOW_ALL_VALUE="${RALPH_ALLOW_ALL:-1}"
RALPH_NO_COMMIT_VALUE="${RALPH_NO_COMMIT:-1}"

ralph_args=(
	--prompt-file "${PROMPT_FILE}"
	--agent "${RALPH_AGENT_VALUE}"
	--model "${RALPH_MODEL_VALUE}"
	--min-iterations "${RALPH_MIN_ITERATIONS_VALUE}"
	--max-iterations "${RALPH_MAX_ITERATIONS_VALUE}"
	--completion-promise "${RALPH_COMPLETION_PROMISE_VALUE}"
	--tasks
	--task-promise "${RALPH_TASK_PROMISE_VALUE}"
)

if [[ "${RALPH_ALLOW_ALL_VALUE}" == "1" || "${RALPH_ALLOW_ALL_VALUE}" == "true" ]]; then
	ralph_args+=(--allow-all)
else
	ralph_args+=(--no-allow-all)
fi

if [[ "${RALPH_NO_COMMIT_VALUE}" == "1" || "${RALPH_NO_COMMIT_VALUE}" == "true" ]]; then
	ralph_args+=(--no-commit)
fi

printf 'Starting Ralph combative loop in %s\n' "${ROOT_DIR}"
printf 'Agent=%s Model=%s MinIters=%s MaxIters=%s\n' \
	"${RALPH_AGENT_VALUE}" \
	"${RALPH_MODEL_VALUE}" \
	"${RALPH_MIN_ITERATIONS_VALUE}" \
	"${RALPH_MAX_ITERATIONS_VALUE}"

exec ralph "${ralph_args[@]}" "$@"
