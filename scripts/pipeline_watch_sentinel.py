#!/usr/bin/env python3
"""Deterministic pipeline watch sentinel (no LLM). Detects stale Antigravity polls."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from pipeline.pipeline_watch import check_stale_watchers, resolve_repo_root, watch_status_update


def run_once(repo_root: Path, *, apply_cancel: bool) -> int:
    alerts = check_stale_watchers(repo_root, apply_alerts=True, apply_cancel=apply_cancel)
    for job_file in (repo_root / "artifacts" / "pipeline_watches").glob("*.json"):
        try:
            import json

            job = json.loads(job_file.read_text(encoding="utf-8"))
        except Exception:
            continue
        if job.get("terminal_status") or job.get("cancelled"):
            continue
        job_id = str(job.get("job_id", "")).strip()
        if not job_id:
            continue
        try:
            watch_status_update(repo_root, job_id, checked_by="sentinel", stuck_threshold_polls=999)
        except Exception:
            pass
    print(f"sentinel: {len(alerts)} stale watcher alert(s)")
    for alert in alerts:
        print(f"  - job {alert.get('job_id')}: stale {int(alert.get('age_seconds', 0))}s")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Theriac pipeline watch sentinel.")
    parser.add_argument("--repo-root", type=Path, default=None)
    parser.add_argument("--loop", action="store_true", help="Run continuously.")
    parser.add_argument("--interval", type=int, default=60, help="Loop interval seconds.")
    parser.add_argument(
        "--apply-cancel",
        action="store_true",
        help="Honor on_watcher_lost=cancel_run for stale jobs.",
    )
    args = parser.parse_args(argv)
    repo_root = resolve_repo_root(args.repo_root)

    if not args.loop:
        return run_once(repo_root, apply_cancel=args.apply_cancel)

    while True:
        run_once(repo_root, apply_cancel=args.apply_cancel)
        time.sleep(max(15, int(args.interval)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
