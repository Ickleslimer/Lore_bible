#!/usr/bin/env python3
"""
Start pipeline worker only (no quota capture, no watch MCP, no sentinel).

For full Antigravity handoff with watch/sentinel, use theriac-pipeline-ops/scripts/pipeline_handoff.py
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from pipeline.pipeline_launch import pipeline_handoff, resolve_repo_root


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Start Theriac pipeline worker only (Lore_bible).")
    parser.add_argument("--repo-root", type=Path, default=None)
    parser.add_argument("--run-root", type=str, default="")
    parser.add_argument("--new-run", action="store_true")
    parser.add_argument("--resume", action="store_true", default=True)
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--ignore-pending", action="store_true")
    parser.add_argument("--start-stage", type=int, default=None)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    repo_root = resolve_repo_root(args.repo_root)
    result = pipeline_handoff(
        repo_root,
        run_root=args.run_root or None,
        new_run=args.new_run,
        start_pipeline=True,
        resume=not args.full,
        ignore_pending=args.ignore_pending,
        start_stage=args.start_stage,
        start_sentinel_daemon=False,
        start_watch=False,
    )
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"run_root={result.get('run_root')}")
        print(result.get("pipeline", {}))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
