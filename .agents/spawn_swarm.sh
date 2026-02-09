#!/usr/bin/env bash
set -euo pipefail

COUNT="${1:-12}"

swarm spawn-prompts --count "${COUNT}"

echo "Generated ${COUNT} prompts in .agents/generated"
echo "Launch each in parallel with your Task tool runner."
