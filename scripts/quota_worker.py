#!/usr/bin/env python3
"""
Quota capture worker (session B).

Polls artifacts/quota_snapshots/worker/capture.request.json on a shared repo folder
(e.g. D:\\Workplaces\\...\\Lore_bible), runs UI capture locally, writes capture.response.json.

Run inside the Windows session where Antigravity is visible (RDP session, VM, or second user):

  python scripts/quota_worker.py --loop

From Cursor / session A:

  set THERIAC_QUOTA_WORKER=1
  python scripts/check_quota.py --worker
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from pipeline.pipeline_watch import resolve_repo_root
from pipeline.quota_worker import (
    clear_shutdown_request,
    clear_worker_ready,
    process_capture_request,
    quota_worker_dir,
    shutdown_requested,
    write_worker_ready,
)


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Poll shared folder and capture Antigravity quota (session B).")
    parser.add_argument("--repo-root", type=Path, default=None)
    parser.add_argument("--loop", action="store_true", help="Poll until interrupted.")
    parser.add_argument("--interval", type=float, default=2.0, help="Poll interval seconds (default 2).")
    parser.add_argument("--once", action="store_true", help="Process one pending request, then exit.")
    args = parser.parse_args(argv)
    repo_root = resolve_repo_root(args.repo_root)
    worker_dir = quota_worker_dir(repo_root)
    print(f"Quota worker watching: {worker_dir}", flush=True)
    write_worker_ready(repo_root)

    def tick() -> bool:
        result = process_capture_request(repo_root)
        if result.get("processed"):
            status = "ok" if result.get("ok") else "failed"
            print(
                f"Processed request {result.get('request_id', '?')}: {status}",
                flush=True,
            )
            return True
        return False

    if args.once:
        try:
            tick()
        finally:
            clear_worker_ready(repo_root)
        return 0

    if not args.loop:
        parser.error("Use --loop or --once.")
        return 2

    try:
        while not shutdown_requested(repo_root):
            tick()
            time.sleep(max(0.5, args.interval))
    except KeyboardInterrupt:
        print("Stopped.", flush=True)
    finally:
        clear_shutdown_request(repo_root)
        clear_worker_ready(repo_root)
        print("Worker exited.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
