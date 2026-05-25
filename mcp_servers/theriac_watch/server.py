from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from mcp.server.fastmcp import FastMCP

from pipeline.pipeline_watch import (
    build_progress_payload,
    cancel_pipeline_run,
    cancel_watch_job,
    list_watch_jobs,
    quota_preflight,
    resolve_repo_root,
    resolve_run_root,
    start_watch_job,
    watch_status_update,
)
from pipeline.pipeline_launch import pipeline_handoff

mcp = FastMCP("theriac-watch")


def _repo() -> Path:
    return resolve_repo_root()


@mcp.tool()
def theriac_resolve_run(run_root: str = "") -> dict[str, Any]:
    """Resolve active or explicit pipeline artifact run root."""
    repo = _repo()
    root = resolve_run_root(repo, run_root or None)
    return {"repo_root": str(repo), "run_root": str(root)}


@mcp.tool()
def theriac_pipeline_progress(run_root: str = "") -> dict[str, Any]:
    """Return pipeline progress for the active or specified run."""
    repo = _repo()
    root = resolve_run_root(repo, run_root or None)
    return build_progress_payload(root)


@mcp.tool()
def theriac_quota_preflight(
    run_capture: bool = False,
    antigravity_assessment_json: str = "",
    check_openrouter: bool = True,
) -> dict[str, Any]:
    """
    Quota preflight: optional screenshot capture + Antigravity assessment + OpenRouter auth-only check.
    Pass antigravity_assessment_json after reading latest.png, e.g.
    {"gemini_bars_filled": 1, "gpt_pool_bars_filled": 4, "refresh_hint": "2h47m"}
    """
    assessment: dict[str, Any] | None = None
    raw = antigravity_assessment_json.strip()
    if raw:
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            raise ValueError("antigravity_assessment_json must be a JSON object.")
        assessment = parsed
    return quota_preflight(
        _repo(),
        run_capture=run_capture,
        antigravity_assessment=assessment,
        check_openrouter=check_openrouter,
    )


@mcp.tool()
def theriac_pipeline_handoff(
    run_root: str = "",
    new_run: bool = False,
    start_pipeline: bool = True,
    resume: bool = True,
    ignore_pending: bool = False,
    start_sentinel: bool = True,
    start_watch: bool = True,
    sentinel_interval_seconds: int = 60,
    poll_interval_seconds: int = 300,
) -> dict[str, Any]:
    """
    Headless handoff: start pipeline worker + sentinel + watch job (no Tauri UI).
    Skips quota preflight. Antigravity Flash should poll theriac_watch_status next.
    """
    return pipeline_handoff(
        _repo(),
        run_root=run_root or None,
        new_run=new_run,
        start_pipeline=start_pipeline,
        resume=resume,
        ignore_pending=ignore_pending,
        start_sentinel_daemon=start_sentinel,
        sentinel_interval_seconds=sentinel_interval_seconds,
        start_watch=start_watch,
        poll_interval_seconds=poll_interval_seconds,
    )


@mcp.tool()
def theriac_watch_start(
    run_root: str = "",
    watcher: str = "antigravity_flash",
    poll_interval_seconds: int = 300,
    on_watcher_lost: str = "alert",
    until_json: str = "",
    preflight_snapshot_at: str = "",
    failover_pool: str = "",
) -> dict[str, Any]:
    """Start a pipeline watch job."""
    repo = _repo()
    root = resolve_run_root(repo, run_root or None)
    until: tuple[str, ...] | None = None
    if until_json.strip():
        parsed = json.loads(until_json)
        if isinstance(parsed, list):
            until = tuple(str(item) for item in parsed)
    job = start_watch_job(
        repo,
        run_root=root,
        until=until,
        watcher=watcher,
        poll_interval_seconds=poll_interval_seconds,
        on_watcher_lost=on_watcher_lost,
        preflight_snapshot_at=preflight_snapshot_at or None,
        failover_pool=failover_pool or None,
    )
    return {
        "job": job,
        "message": (
            f"Watch job {job['job_id']} started. Run sentinel: "
            f"python scripts/pipeline_watch_sentinel.py --loop"
        ),
    }


@mcp.tool()
def theriac_watch_status(job_id: str, checked_by: str = "mcp") -> dict[str, Any]:
    """Poll watch job status; writes watch_report.md when terminal."""
    return watch_status_update(_repo(), job_id, checked_by=checked_by)


@mcp.tool()
def theriac_watch_list(include_terminal: bool = False) -> dict[str, Any]:
    """List watch jobs."""
    jobs = list_watch_jobs(_repo(), include_terminal=include_terminal)
    return {"jobs": jobs}


@mcp.tool()
def theriac_watch_cancel(job_id: str) -> dict[str, Any]:
    """Cancel a watch job (does not stop the pipeline worker)."""
    job = cancel_watch_job(_repo(), job_id)
    return {"job": job}


@mcp.tool()
def theriac_pipeline_cancel(run_root: str = "") -> dict[str, Any]:
    """Attempt to cancel a running pipeline worker by PID (best-effort)."""
    repo = _repo()
    root = resolve_run_root(repo, run_root or None)
    return cancel_pipeline_run(root)


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
