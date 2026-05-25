#!/usr/bin/env python3
"""
Autonomous pipeline + sentinel handoff (no Tauri UI required).

  python scripts/pipeline_handoff.py
  python scripts/pipeline_handoff.py --resume --ignore-pending
  python scripts/pipeline_handoff.py --pipeline-only
  python scripts/pipeline_handoff.py --sentinel-only
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from pipeline.pipeline_launch import pipeline_handoff, stop_sentinel
from pipeline.pipeline_watch import resolve_repo_root


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Start pipeline worker + watch sentinel + watch job (headless handoff).",
    )
    parser.add_argument("--repo-root", type=Path, default=None)
    parser.add_argument("--run-root", type=str, default="", help="Artifact run folder (default: last open).")
    parser.add_argument("--new-run", action="store_true", help="Create a new artifacts run folder.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--resume", action="store_true", help="Resume existing run (default).")
    mode.add_argument("--full", action="store_true", help="Full pipeline from stage 1 (no --resume).")
    parser.add_argument("--ignore-pending", action="store_true")
    parser.add_argument("--start-stage", type=int, default=None)
    parser.add_argument("--no-pipeline", action="store_true")
    parser.add_argument("--no-sentinel", action="store_true")
    parser.add_argument("--no-watch", action="store_true")
    parser.add_argument("--sentinel-interval", type=int, default=60)
    parser.add_argument("--poll-interval", type=int, default=300)
    parser.add_argument("--force-pipeline", action="store_true")
    parser.add_argument("--force-sentinel", action="store_true")
    parser.add_argument("--stop-sentinel", action="store_true", help="Stop background sentinel only.")
    parser.add_argument("--json", action="store_true", help="Print JSON result.")
    args = parser.parse_args(argv)

    repo_root = resolve_repo_root(args.repo_root)
    if args.stop_sentinel:
        result = stop_sentinel(repo_root)
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            print(result)
        return 0 if result.get("ok") else 1

    resume = not args.full
    result = pipeline_handoff(
        repo_root,
        run_root=args.run_root or None,
        new_run=args.new_run,
        start_pipeline=not args.no_pipeline,
        resume=resume,
        ignore_pending=args.ignore_pending,
        start_stage=args.start_stage,
        start_sentinel_daemon=not args.no_sentinel,
        sentinel_interval_seconds=args.sentinel_interval,
        start_watch=not args.no_watch,
        poll_interval_seconds=args.poll_interval,
        force_pipeline=args.force_pipeline,
        force_sentinel=args.force_sentinel,
    )
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"repo_root={result.get('repo_root')}")
        print(f"run_root={result.get('run_root')}")
        pipeline = result.get("pipeline") or {}
        if pipeline:
            print(
                f"pipeline: started={pipeline.get('started')} pid={pipeline.get('pid')} "
                f"reason={pipeline.get('reason', '')}"
            )
        sentinel = result.get("sentinel") or {}
        if sentinel:
            print(
                f"sentinel: started={sentinel.get('started')} pid={sentinel.get('pid')} "
                f"log={sentinel.get('log_path', '')}"
            )
        watch = result.get("watch_job") or {}
        if watch:
            print(f"watch_job_id={watch.get('job_id')}")
        for step in result.get("next_steps") or []:
            print(f"  -> {step}")
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
