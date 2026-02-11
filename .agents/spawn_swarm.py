#!/usr/bin/env python3

from pathlib import Path
import argparse


def render_prompt(template: str, agent_id: int) -> str:
    return template.replace("{N}", str(agent_id))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate per-agent prompts and Task invocations"
    )
    parser.add_argument(
        "--count", type=int, default=12, help="number of agents to generate"
    )
    parser.add_argument(
        "--template",
        default=".agents/agent_prompt.md",
        help="template path with {N} placeholder",
    )
    parser.add_argument(
        "--out-dir",
        default=".agents/generated",
        help="directory for rendered prompt files",
    )
    args = parser.parse_args()

    template_path = Path(args.template)
    out_dir = Path(args.out_dir)

    if not template_path.exists():
        raise SystemExit(f"Template not found: {template_path}")

    template = template_path.read_text(encoding="utf-8")
    out_dir.mkdir(parents=True, exist_ok=True)

    print("# Generated prompts")
    for agent_id in range(1, args.count + 1):
        out_file = out_dir / f"agent_{agent_id:02d}.md"
        out_file.write_text(render_prompt(template, agent_id), encoding="utf-8")
        print(out_file)

    print("\n# Task tool calls")
    for agent_id in range(1, args.count + 1):
        prompt_file = out_dir / f"agent_{agent_id:02d}.md"
        print(
            "Task("
            f'description="Agent {agent_id} process bead through pipeline", '
            f'prompt=Path("{prompt_file}").read_text(), '
            'subagent_type="general", '
            'command="swarm agent"'
            ")"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
